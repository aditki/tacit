"""Investigation history store.

Full request persistence for every pipeline run:
- prompts, intent, archetypes, selected metrics, generated queries
- per-step timings, failures, validation warnings
- dashboard URLs and UIDs

SQLite-backed. Complements the feedback store (which tracks post-hoc human ratings)
by capturing the full investigation lifecycle for debugging and audit.
"""

from __future__ import annotations

import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from tacit.config import DEFAULT_HISTORY_DB_PATH, Settings, settings
from tacit.investigation_contract import (
    CorrectionReference,
    DecisionLogEntry,
    InvestigationContract,
    InvestigationContractAssembler,
    InvestigationRunType,
    KnowledgeCandidate,
    KnowledgeCandidateStatus,
    ProvenanceRecord,
    RuntimeManifest,
    fingerprint,
    normalized_output_payload,
    stamp_fingerprints,
    utc_now,
)
from tacit.investigation_replay import (
    CounterfactualChanges,
    InvestigationReplaySnapshot,
    ReplayMode,
    apply_counterfactual,
    rebuild_contract,
)
from tacit.knowledge.authorization import KnowledgeAction, enforce_knowledge_action
from tacit.knowledge.enums import KnowledgeUsageDisposition
from tacit.knowledge.models import KnowledgeUsage
from tacit.pagination import KeysetPage, decode_cursor, encode_cursor
from tacit.tenancy import resolve_tenant_boundary

logger = structlog.get_logger()

_DEFAULT_DB_PATH = DEFAULT_HISTORY_DB_PATH
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_HISTORY_SCHEMA_MARKER = "history_tenant_assignments_v2"
_HISTORY_TENANT_MIGRATION = "history_tenant_assignment_backfill_v2"
_HISTORY_MIGRATION_BATCH_SIZE = 500
_CURRENT_ENGINE_REBUILT_STAGES = frozenset(
    {"candidate_exclusion", "candidate_generation", "ranking", "ranking_context"}
)


class StaleRevisionError(ValueError):
    """Raised when a revision-scoped operation no longer targets the current revision."""


class ReplayError(ValueError):
    """Base error for a replay request that cannot produce its promised result."""


class ReplayInputsUnavailableError(ReplayError):
    """Raised when an evaluative replay has no captured inputs to rebuild."""


class ExactReplayMismatchError(ReplayError):
    """Raised when captured inputs no longer rebuild the persisted exact output."""


def _decode_timestamp_cursor(cursor: str, *, label: str) -> tuple[float, str]:
    try:
        raw_timestamp, raw_id = decode_cursor(cursor, field_count=2)
        if isinstance(raw_timestamp, bool) or not isinstance(raw_timestamp, (int, float)):
            raise ValueError
        if not isinstance(raw_id, str) or not raw_id or len(raw_id) > 500:
            raise ValueError
        timestamp = float(raw_timestamp)
        if not math.isfinite(timestamp):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {label} cursor") from exc
    return timestamp, raw_id


def _decode_revision_cursor(cursor: str) -> int:
    try:
        (raw_revision,) = decode_cursor(cursor, field_count=1)
        if isinstance(raw_revision, bool) or not isinstance(raw_revision, int) or raw_revision < 1:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid revision cursor") from exc
    return raw_revision


def _decode_sequence_cursor(cursor: str) -> tuple[int, str]:
    try:
        raw_sequence, raw_event_id = decode_cursor(cursor, field_count=2)
        if isinstance(raw_sequence, bool) or not isinstance(raw_sequence, int) or raw_sequence < 0:
            raise ValueError
        if not isinstance(raw_event_id, str) or not raw_event_id or len(raw_event_id) > 500:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid event cursor") from exc
    return raw_sequence, raw_event_id


def _decode_event_cursor(cursor: str) -> tuple[float, int, str]:
    try:
        raw_created_at, raw_sequence, raw_event_id = decode_cursor(cursor, field_count=3)
        if isinstance(raw_created_at, bool) or not isinstance(raw_created_at, (int, float)):
            raise ValueError
        if isinstance(raw_sequence, bool) or not isinstance(raw_sequence, int) or raw_sequence < 0:
            raise ValueError
        if not isinstance(raw_event_id, str) or not raw_event_id or len(raw_event_id) > 500:
            raise ValueError
        created_at = float(raw_created_at)
        if not math.isfinite(created_at):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid event cursor") from exc
    return created_at, raw_sequence, raw_event_id


def _merge_current_engine_replay_usage(
    current_usage: list[KnowledgeUsage],
    historical_usage: list[KnowledgeUsage],
    *,
    tenant_id: str,
) -> list[KnowledgeUsage]:
    """Retain exact attribution for captured stages that current-engine replay does not rebuild."""
    merged = [item.model_copy(update={"tenant_id": tenant_id}) for item in current_usage]
    positions = {(item.knowledge_ref, item.knowledge_revision): index for index, item in enumerate(merged)}
    for historical in historical_usage:
        if historical.disposition != KnowledgeUsageDisposition.APPLIED:
            continue
        reused_stages = [stage for stage in historical.used_for if stage not in _CURRENT_ENGINE_REBUILT_STAGES]
        if not reused_stages:
            continue
        key = (historical.knowledge_ref, historical.knowledge_revision)
        reason_codes = list(historical.reason_codes)
        if "current_engine_reused_captured_stage_output" not in reason_codes:
            reason_codes.append("current_engine_reused_captured_stage_output")
        index = positions.get(key)
        if index is None:
            positions[key] = len(merged)
            merged.append(
                historical.model_copy(
                    update={
                        "tenant_id": tenant_id,
                        "used_for": reused_stages,
                        "score_delta": 0.0,
                        "reason_codes": reason_codes,
                    }
                )
            )
            continue
        current = merged[index]
        used_for = list(current.used_for)
        for stage in reused_stages:
            if stage not in used_for:
                used_for.append(stage)
        merged_reasons = list(current.reason_codes)
        for reason in reason_codes:
            if reason not in merged_reasons:
                merged_reasons.append(reason)
        merged[index] = current.model_copy(
            update={
                "disposition": historical.disposition,
                "used_for": used_for,
                "reason_codes": merged_reasons,
            }
        )
    return merged


def _normalize_legacy_snapshot_tenant(
    snapshot: InvestigationReplaySnapshot,
    *,
    selected_tenant: str,
    configured_tenant: str,
) -> InvestigationReplaySnapshot:
    snapshot_tenant = str(snapshot.request.tenant_id or "")
    if snapshot_tenant == selected_tenant:
        return snapshot
    legacy_placeholder = configured_tenant != "*" and snapshot_tenant in {"", "default"}
    if not legacy_placeholder:
        raise ReplayError(
            f"investigation tenant {snapshot_tenant!r} does not match configured tenant {selected_tenant!r}"
        )
    return snapshot.model_copy(
        update={
            "request": snapshot.request.model_copy(update={"tenant_id": selected_tenant}),
            "knowledge_usage": [
                item.model_copy(update={"tenant_id": selected_tenant}) for item in snapshot.knowledge_usage
            ],
        }
    )


def _normalize_legacy_contract_tenant(
    contract: InvestigationContract,
    *,
    selected_tenant: str,
    configured_tenant: str,
) -> InvestigationContract:
    contract_tenant = str(contract.request.scope.tenant_id or "")
    if contract_tenant == selected_tenant:
        return contract
    legacy_placeholder = configured_tenant != "*" and contract_tenant in {"", "default"}
    if not legacy_placeholder:
        raise StaleRevisionError(
            f"investigation tenant {contract_tenant!r} does not match configured tenant {selected_tenant!r}"
        )
    return contract.model_copy(
        update={
            "request": contract.request.model_copy(
                update={
                    "scope": contract.request.scope.model_copy(update={"tenant_id": selected_tenant}),
                }
            ),
            "knowledge_usage": [
                item.model_copy(update={"tenant_id": selected_tenant}) for item in contract.knowledge_usage
            ],
        }
    )


def _db_path(runtime_settings: Settings | None = None) -> Path:
    active_settings = runtime_settings or settings
    custom = active_settings.history_db_path
    path = Path(custom) if custom else _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS investigations (
    id              TEXT PRIMARY KEY,          -- UUID
    tenant_id       TEXT NOT NULL DEFAULT 'default',
    prompt          TEXT NOT NULL,
    user_id         TEXT NOT NULL DEFAULT '',
    channel_id      TEXT NOT NULL DEFAULT '',
    current_revision INTEGER NOT NULL DEFAULT 0,

    -- Intent
    intent_summary  TEXT NOT NULL DEFAULT '',
    intent_domain   TEXT NOT NULL DEFAULT '',
    intent_services TEXT NOT NULL DEFAULT '[]', -- JSON array
    intent_keywords TEXT NOT NULL DEFAULT '[]', -- JSON array
    intent_signals  TEXT NOT NULL DEFAULT '[]', -- JSON array
    problem_type    TEXT NOT NULL DEFAULT '',
    archetypes      TEXT NOT NULL DEFAULT '[]', -- JSON: [{type, confidence}]
    timerange       TEXT NOT NULL DEFAULT '',

    -- Metrics
    datasources_found    INTEGER NOT NULL DEFAULT 0,
    datasource_types     TEXT NOT NULL DEFAULT '[]',  -- JSON array
    metrics_catalog_size INTEGER NOT NULL DEFAULT 0,
    metrics_selected     TEXT NOT NULL DEFAULT '[]',  -- JSON array of metric names
    metrics_ranked_size  INTEGER NOT NULL DEFAULT 0,

    -- Queries
    generated_queries TEXT NOT NULL DEFAULT '[]', -- JSON: [{expr, panel_title}]
    panel_count       INTEGER NOT NULL DEFAULT 0,

    -- Routing
    path_used TEXT NOT NULL DEFAULT '',  -- 'archetype', 'freeform', 'failed'

    -- Validation
    validation_warnings TEXT NOT NULL DEFAULT '[]', -- JSON array
    panels_dropped      INTEGER NOT NULL DEFAULT 0,

    -- Diagnostic stage outcomes
    stage_outcomes TEXT NOT NULL DEFAULT '{}', -- JSON: {stage: {status, reason_code, details}}

    -- Result
    dashboard_uid TEXT NOT NULL DEFAULT '',
    dashboard_url TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'running', -- running, success, failed, timeout
    error         TEXT NOT NULL DEFAULT '',

    -- Timings
    timings     TEXT NOT NULL DEFAULT '{}', -- JSON: {step: seconds}
    total_time  REAL NOT NULL DEFAULT 0,

    -- Timestamps
    started_at  REAL NOT NULL,
    finished_at REAL
);

CREATE INDEX IF NOT EXISTS idx_inv_status ON investigations(status);
CREATE INDEX IF NOT EXISTS idx_inv_user ON investigations(user_id);
CREATE INDEX IF NOT EXISTS idx_inv_started ON investigations(started_at);
CREATE INDEX IF NOT EXISTS idx_inv_dashboard ON investigations(dashboard_uid);
CREATE TABLE IF NOT EXISTS investigation_tenant_assignments (
    investigation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    assignment_source TEXT NOT NULL,
    assigned_at REAL NOT NULL,
    FOREIGN KEY (investigation_id) REFERENCES investigations(id)
);

CREATE TABLE IF NOT EXISTS history_schema_metadata (
    migration_name TEXT PRIMARY KEY,
    completed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS history_migration_progress (
    migration_name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS investigation_revisions (
    investigation_id   TEXT NOT NULL,
    revision           INTEGER NOT NULL,
    parent_revision    INTEGER,
    schema_version     TEXT NOT NULL,
    contract_json      TEXT NOT NULL,
    input_fingerprint  TEXT NOT NULL,
    output_fingerprint TEXT NOT NULL,
    engine_version     TEXT NOT NULL,
    created_at         REAL NOT NULL,
    reason             TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (investigation_id, revision),
    FOREIGN KEY (investigation_id) REFERENCES investigations(id)
);

CREATE INDEX IF NOT EXISTS idx_inv_revisions_created
    ON investigation_revisions(investigation_id, created_at);

CREATE TABLE IF NOT EXISTS investigation_snapshots (
    investigation_id TEXT NOT NULL,
    revision         INTEGER NOT NULL,
    snapshot_version TEXT NOT NULL,
    snapshot_json    TEXT NOT NULL,
    created_at       REAL NOT NULL,
    PRIMARY KEY (investigation_id, revision),
    FOREIGN KEY (investigation_id, revision)
        REFERENCES investigation_revisions(investigation_id, revision)
);

CREATE TABLE IF NOT EXISTS investigation_runs (
    run_id                TEXT PRIMARY KEY,
    investigation_id      TEXT NOT NULL,
    base_revision         INTEGER,
    run_type              TEXT NOT NULL,
    status                TEXT NOT NULL,
    started_at            REAL NOT NULL,
    completed_at          REAL,
    error_code            TEXT NOT NULL DEFAULT '',
    error_detail          TEXT NOT NULL DEFAULT '',
    runtime_manifest_json TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY (investigation_id) REFERENCES investigations(id)
);

CREATE INDEX IF NOT EXISTS idx_inv_runs_investigation
    ON investigation_runs(investigation_id, started_at);
CREATE INDEX IF NOT EXISTS idx_inv_runs_page
    ON investigation_runs(investigation_id, started_at DESC, run_id DESC);

CREATE TABLE IF NOT EXISTS investigation_events (
    event_id         TEXT PRIMARY KEY,
    investigation_id TEXT NOT NULL,
    run_id           TEXT NOT NULL,
    sequence         INTEGER NOT NULL,
    event_type       TEXT NOT NULL,
    payload_json     TEXT NOT NULL DEFAULT '{}',
    created_at       REAL NOT NULL,
    FOREIGN KEY (investigation_id) REFERENCES investigations(id)
);

CREATE INDEX IF NOT EXISTS idx_inv_events_run
    ON investigation_events(run_id, sequence);
CREATE INDEX IF NOT EXISTS idx_inv_events_investigation_order
    ON investigation_events(investigation_id, created_at, sequence);
CREATE INDEX IF NOT EXISTS idx_inv_events_page
    ON investigation_events(investigation_id, created_at DESC, sequence DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_inv_run_events_page
    ON investigation_events(investigation_id, run_id, sequence DESC, event_id DESC);

CREATE TABLE IF NOT EXISTS knowledge_candidates (
    id               TEXT PRIMARY KEY,
    investigation_id TEXT NOT NULL,
    revision         INTEGER NOT NULL,
    correction_text  TEXT NOT NULL,
    target_ref       TEXT NOT NULL DEFAULT '',
    candidate_type   TEXT NOT NULL DEFAULT 'human_correction',
    status           TEXT NOT NULL DEFAULT 'pending_review',
    created_by       TEXT NOT NULL DEFAULT '',
    created_at       REAL NOT NULL,
    expires_at       REAL,
    provenance_json  TEXT NOT NULL,
    reviewed_by      TEXT NOT NULL DEFAULT '',
    reviewed_at      REAL,
    applied_revision INTEGER,
    FOREIGN KEY (investigation_id) REFERENCES investigations(id)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_candidates_investigation
    ON knowledge_candidates(investigation_id, revision);
"""


def _execute_schema_statements(conn: sqlite3.Connection, script: str) -> None:
    """Execute a schema script without sqlite3.executescript's implicit commit."""
    pending: list[str] = []
    for line in script.splitlines():
        pending.append(line)
        statement = "\n".join(pending).strip()
        if statement and sqlite3.complete_statement(statement):
            conn.execute(statement)
            pending.clear()
    if "\n".join(pending).strip():
        raise RuntimeError("History schema contains an incomplete SQL statement")


class InvestigationStore:
    """SQLite-backed investigation history."""

    def __init__(self, db_path: Path | None = None, *, runtime_settings: Settings | None = None):
        self._settings = runtime_settings or settings
        self._db_path = db_path or _db_path(self._settings)
        self._ensure_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self._db_path), timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self):
        with self._conn() as conn:
            investigations_existed = self._table_exists(conn, "investigations")
            if investigations_existed and self._history_schema_is_current(conn):
                self._require_confirmed_default_tenant_owner(conn)
                logger.info("investigation_store_init", db_path=str(self._db_path))
                return

            preflight_columns = (
                {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
                if investigations_existed
                else set()
            )
            preflight_tenant_column = "tenant_id" in preflight_columns
            assignments_existed = self._table_exists(conn, "investigation_tenant_assignments")
            pending_details = self._pending_history_tenant_migration(conn)
            original_tenant_column = (
                bool(pending_details["tenant_column_existed"])
                if pending_details is not None
                else preflight_tenant_column
            )
            preflight_tenant_migration = investigations_existed and (
                pending_details is not None or not preflight_tenant_column or not assignments_existed
            )
            if preflight_tenant_migration:
                # Owner discovery can parse substantial legacy history. Do it
                # before taking the write lock; each bounded migration batch still
                # rejects ownerless rows before persisting assignments.
                self._require_legacy_tenant_owner(
                    conn,
                    tenant_column_existed=original_tenant_column,
                )
            elif investigations_existed and preflight_tenant_column:
                self._require_confirmed_default_tenant_owner(conn)

            conn.execute("BEGIN IMMEDIATE")
            existing_columns = (
                {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
                if investigations_existed
                else set()
            )
            tenant_column_existed = "tenant_id" in existing_columns
            assignments_existed = self._table_exists(conn, "investigation_tenant_assignments")
            tenant_migration_required = investigations_existed and (
                pending_details is not None or not tenant_column_existed or not assignments_existed
            )
            _execute_schema_statements(conn, _SCHEMA_SQL)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
            if "stage_outcomes" not in columns:
                conn.execute("ALTER TABLE investigations ADD COLUMN stage_outcomes TEXT NOT NULL DEFAULT '{}'")
            if "current_revision" not in columns:
                conn.execute("ALTER TABLE investigations ADD COLUMN current_revision INTEGER NOT NULL DEFAULT 0")
            if "tenant_id" not in columns:
                conn.execute("ALTER TABLE investigations ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_inv_tenant_started
                   ON investigations(tenant_id, started_at DESC, id DESC)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_inv_tenant_status_started
                   ON investigations(tenant_id, status, started_at DESC, id DESC)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_inv_tenant_user_started
                   ON investigations(tenant_id, user_id, started_at DESC, id DESC)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_inv_tenant_dashboard
                   ON investigations(tenant_id, dashboard_uid)""")
            if tenant_migration_required:
                if pending_details is None and not tenant_column_existed:
                    conn.execute("DELETE FROM investigation_tenant_assignments")
                conn.execute(
                    "DELETE FROM history_schema_metadata WHERE migration_name=?",
                    (_HISTORY_SCHEMA_MARKER,),
                )
                details = pending_details or {
                    "tenant_column_existed": tenant_column_existed,
                    "configured_tenant": str(self._settings.knowledge_tenant_id or "default"),
                }
                conn.execute(
                    """INSERT INTO history_migration_progress (
                           migration_name, cursor, details_json, updated_at
                       ) VALUES (?, '', ?, ?)
                       ON CONFLICT(migration_name) DO NOTHING""",
                    (_HISTORY_TENANT_MIGRATION, json.dumps(details, sort_keys=True), time.time()),
                )
            candidate_columns = {row[1] for row in conn.execute("PRAGMA table_info(knowledge_candidates)")}
            if "reviewed_by" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN reviewed_by TEXT NOT NULL DEFAULT ''")
            if "reviewed_at" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN reviewed_at REAL")
            if "applied_revision" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN applied_revision INTEGER")
            if not tenant_migration_required:
                conn.execute(
                    """INSERT OR REPLACE INTO history_schema_metadata (migration_name, completed_at)
                       VALUES (?, ?)""",
                    (_HISTORY_SCHEMA_MARKER, time.time()),
                )
        if tenant_migration_required:
            self._run_history_tenant_migration()
        logger.info("investigation_store_init", db_path=str(self._db_path))

    def _history_schema_is_current(self, conn: sqlite3.Connection) -> bool:
        """Verify the marker still describes the physical schema."""
        if not self._table_exists(conn, "history_schema_metadata"):
            return False
        marker = conn.execute(
            "SELECT 1 FROM history_schema_metadata WHERE migration_name=?",
            (_HISTORY_SCHEMA_MARKER,),
        ).fetchone()
        if marker is None:
            return False
        if self._pending_history_tenant_migration(conn) is not None:
            return False
        investigation_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(investigations)")}
        candidate_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(knowledge_candidates)")}
        return (
            {"tenant_id", "stage_outcomes", "current_revision"}.issubset(investigation_columns)
            and {"reviewed_by", "reviewed_at", "applied_revision"}.issubset(candidate_columns)
            and self._table_exists(conn, "investigation_tenant_assignments")
            and self._table_exists(conn, "history_migration_progress")
            and self._index_exists(conn, "idx_inv_tenant_started")
            and self._index_exists(conn, "idx_inv_tenant_status_started")
            and self._index_exists(conn, "idx_inv_tenant_user_started")
            and self._index_exists(conn, "idx_inv_events_investigation_order")
        )

    def _pending_history_tenant_migration(self, conn: sqlite3.Connection) -> dict[str, Any] | None:
        if not self._table_exists(conn, "history_migration_progress"):
            return None
        row = conn.execute(
            "SELECT details_json FROM history_migration_progress WHERE migration_name=?",
            (_HISTORY_TENANT_MIGRATION,),
        ).fetchone()
        if row is None:
            return None
        try:
            details = json.loads(str(row["details_json"]))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("History tenant migration progress is invalid") from exc
        if not isinstance(details, dict) or not {
            "tenant_column_existed",
            "configured_tenant",
        }.issubset(details):
            raise RuntimeError("History tenant migration progress is incomplete")
        return details

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
                (index_name,),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _concrete_legacy_tenant(value: Any) -> str | None:
        tenant_id = str(value or "").strip()
        if tenant_id in {"", "default", "*"}:
            return None
        try:
            return resolve_tenant_boundary("*", tenant_id)
        except ValueError:
            return None

    def _legacy_contract_tenant(self, conn: sqlite3.Connection, investigation_id: str) -> str | None:
        if not self._table_exists(conn, "investigation_revisions"):
            return None
        contract_row = conn.execute(
            """SELECT contract_json FROM investigation_revisions
               WHERE investigation_id=? ORDER BY revision DESC LIMIT 1""",
            (investigation_id,),
        ).fetchone()
        if contract_row is None or not contract_row["contract_json"]:
            return None
        try:
            payload = json.loads(contract_row["contract_json"])
            return self._concrete_legacy_tenant(payload.get("request", {}).get("scope", {}).get("tenant_id"))
        except (AttributeError, TypeError, ValueError):
            return None

    def _legacy_row_tenant(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        tenant_column_existed: bool,
    ) -> str | None:
        if tenant_column_existed:
            row_tenant = self._concrete_legacy_tenant(row["tenant_id"])
            if row_tenant is not None:
                return row_tenant
        if "contract_json" in row.keys():
            if not row["contract_json"]:
                return None
            try:
                payload = json.loads(row["contract_json"])
                return self._concrete_legacy_tenant(payload.get("request", {}).get("scope", {}).get("tenant_id"))
            except (AttributeError, TypeError, ValueError):
                return None
        return self._legacy_contract_tenant(conn, str(row["id"]))

    def _legacy_investigation_batches(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_column_existed: bool,
        batch_size: int = 500,
    ) -> Iterator[list[sqlite3.Row]]:
        selected = "i.id, i.tenant_id" if tenant_column_existed else "i.id"
        revisions_exist = self._table_exists(conn, "investigation_revisions")
        after_id = ""
        while True:
            if not revisions_exist:
                rows = conn.execute(
                    f"""SELECT {selected}, NULL AS contract_json FROM investigations i
                        WHERE i.id>? ORDER BY i.id LIMIT ?""",
                    (after_id, batch_size),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""SELECT {selected}, revision.contract_json
                        FROM investigations i
                        LEFT JOIN investigation_revisions revision
                          ON revision.investigation_id=i.id
                         AND revision.revision=(
                           SELECT MAX(candidate.revision)
                           FROM investigation_revisions candidate
                           WHERE candidate.investigation_id=i.id
                         )
                        WHERE i.id>? ORDER BY i.id LIMIT ?""",
                    (after_id, batch_size),
                ).fetchall()
            if not rows:
                break
            yield rows
            after_id = str(rows[-1]["id"])

    def _require_legacy_tenant_owner(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_column_existed: bool,
    ) -> None:
        """Fail before schema mutation when wildcard history rows have no owner."""
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        if configured_tenant != "*":
            return
        ownerless_count = 0
        ownerless_ids: list[str] = []
        for batch in self._legacy_investigation_batches(
            conn,
            tenant_column_existed=tenant_column_existed,
        ):
            for row in batch:
                if (
                    self._legacy_row_tenant(
                        conn,
                        row,
                        tenant_column_existed=tenant_column_existed,
                    )
                    is None
                ):
                    ownerless_count += 1
                    if len(ownerless_ids) < 5:
                        ownerless_ids.append(str(row["id"]))
        if ownerless_count:
            logger.error(
                "legacy_history_owner_required",
                ownerless_count=ownerless_count,
                sample_investigation_ids=ownerless_ids,
            )
            raise RuntimeError(
                "Legacy investigation history has no tenant owner. Start once with knowledge_tenant_id pinned "
                "to its owner before enabling wildcard tenancy."
            )

    def _require_confirmed_default_tenant_owner(self, conn: sqlite3.Connection) -> None:
        """Reject synthetic default ownership left by the previous migration."""
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        if configured_tenant != "*":
            return
        if not self._table_exists(conn, "investigation_tenant_assignments"):
            ownerless_ids = [
                str(row["id"])
                for row in conn.execute("SELECT id FROM investigations WHERE tenant_id='default' LIMIT 5").fetchall()
            ]
        else:
            ownerless_ids = [str(row["id"]) for row in conn.execute("""SELECT i.id
                       FROM investigations i
                       LEFT JOIN investigation_tenant_assignments a
                         ON a.investigation_id=i.id AND a.tenant_id='default'
                       WHERE i.tenant_id='default' AND a.investigation_id IS NULL
                       LIMIT 5""").fetchall()]
        if ownerless_ids:
            logger.error(
                "legacy_history_default_owner_unconfirmed",
                ownerless_count=len(ownerless_ids),
                sample_investigation_ids=ownerless_ids[:5],
            )
            raise RuntimeError(
                "Previously migrated investigation history has unconfirmed default-tenant ownership. "
                "Start once with knowledge_tenant_id pinned to its owner before enabling wildcard tenancy."
            )

    def _run_history_tenant_migration(self) -> None:
        migrated = 0
        batch_count = 0
        while True:
            batch_size, completed = self._migrate_history_tenant_batch()
            if completed:
                break
            migrated += batch_size
            batch_count += 1
            logger.info(
                "legacy_history_tenant_backfill_batch",
                batch_size=batch_size,
                batch_count=batch_count,
            )
        if migrated:
            logger.info(
                "legacy_history_tenants_backfilled",
                investigation_count=migrated,
                batch_count=batch_count,
                batch_size=_HISTORY_MIGRATION_BATCH_SIZE,
            )

    def _migrate_history_tenant_batch(self) -> tuple[int, bool]:
        """Commit at most one bounded page of legacy tenant assignments."""
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            progress = conn.execute(
                """SELECT cursor, details_json FROM history_migration_progress
                   WHERE migration_name=?""",
                (_HISTORY_TENANT_MIGRATION,),
            ).fetchone()
            if progress is None:
                return 0, True
            try:
                details = json.loads(str(progress["details_json"]))
                tenant_column_existed = bool(details["tenant_column_existed"])
                recorded_tenant = str(details["configured_tenant"] or "default")
                after_id = str(progress["cursor"] or "")
            except (KeyError, TypeError, ValueError) as exc:
                raise RuntimeError("History tenant migration progress is invalid") from exc
            configured_tenant = str(self._settings.knowledge_tenant_id or "default")
            if recorded_tenant != configured_tenant:
                raise RuntimeError(
                    "History database tenant owner migration is already in progress for another tenant: "
                    f"recorded={recorded_tenant}, configured={configured_tenant}"
                )
            fallback_tenant = recorded_tenant if recorded_tenant != "*" else None
            revisions_exist = self._table_exists(conn, "investigation_revisions")
            revision_join = ""
            revision_column = "NULL AS contract_json"
            if revisions_exist:
                revision_join = """LEFT JOIN investigation_revisions revision
                          ON revision.investigation_id=i.id
                         AND revision.revision=(
                           SELECT MAX(candidate.revision)
                           FROM investigation_revisions candidate
                           WHERE candidate.investigation_id=i.id
                         )"""
                revision_column = "revision.contract_json"
            rows = conn.execute(
                f"""SELECT i.id, i.tenant_id, {revision_column}
                    FROM investigations i
                    LEFT JOIN investigation_tenant_assignments assignment
                      ON assignment.investigation_id=i.id
                    {revision_join}
                    WHERE assignment.investigation_id IS NULL AND i.id>?
                    ORDER BY i.id LIMIT ?""",
                (after_id, _HISTORY_MIGRATION_BATCH_SIZE),
            ).fetchall()
            if not rows:
                # A concurrent legacy writer can insert below the cursor. This
                # rare final sweep preserves correctness without making every
                # normal page rescan the completed prefix.
                remaining = conn.execute("""SELECT 1 FROM investigations i
                       LEFT JOIN investigation_tenant_assignments assignment
                         ON assignment.investigation_id=i.id
                       WHERE assignment.investigation_id IS NULL LIMIT 1""").fetchone()
                if remaining is not None:
                    conn.execute(
                        """UPDATE history_migration_progress
                           SET cursor='', updated_at=? WHERE migration_name=?""",
                        (time.time(), _HISTORY_TENANT_MIGRATION),
                    )
                    return 0, False
                conn.execute(
                    """INSERT OR REPLACE INTO history_schema_metadata (migration_name, completed_at)
                       VALUES (?, ?)""",
                    (_HISTORY_SCHEMA_MARKER, time.time()),
                )
                conn.execute(
                    "DELETE FROM history_migration_progress WHERE migration_name=?",
                    (_HISTORY_TENANT_MIGRATION,),
                )
                return 0, True

            assigned_at = time.time()
            updates: list[tuple[str, str]] = []
            assignments: list[tuple[str, str, float]] = []
            for row in rows:
                tenant_id = self._legacy_row_tenant(
                    conn,
                    row,
                    tenant_column_existed=tenant_column_existed,
                )
                if tenant_id is None:
                    tenant_id = fallback_tenant
                if tenant_id is None:
                    raise RuntimeError(
                        "Legacy investigation history has no tenant owner. Start once with knowledge_tenant_id pinned "
                        "to its owner before enabling wildcard tenancy."
                    )
                updates.append((tenant_id, str(row["id"])))
                assignments.append((str(row["id"]), tenant_id, assigned_at))
            conn.executemany("UPDATE investigations SET tenant_id=? WHERE id=?", updates)
            conn.executemany(
                """INSERT INTO investigation_tenant_assignments (
                       investigation_id, tenant_id, assignment_source, assigned_at
                   ) VALUES (?, ?, 'legacy_migration', ?)""",
                assignments,
            )
            conn.execute(
                """UPDATE history_migration_progress
                   SET cursor=?, updated_at=? WHERE migration_name=?""",
                (str(rows[-1]["id"]), assigned_at, _HISTORY_TENANT_MIGRATION),
            )
            return len(rows), False

    # ── Write operations ──────────────────────────────────────────────────

    def start(
        self,
        prompt: str,
        user_id: str = "",
        channel_id: str = "",
        tenant_id: str | None = None,
    ) -> str:
        """Record the start of a new investigation. Returns investigation ID."""
        tenant_id = self._resolve_tenant(tenant_id)
        inv_id = uuid.uuid4().hex[:16]
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO investigations (id, tenant_id, prompt, user_id, channel_id, started_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (inv_id, tenant_id or "default", prompt, user_id, channel_id, time.time()),
            )
            conn.execute(
                """INSERT INTO investigation_tenant_assignments (
                       investigation_id, tenant_id, assignment_source, assigned_at
                   ) VALUES (?, ?, 'runtime_boundary', ?)""",
                (inv_id, tenant_id or "default", time.time()),
            )
        return inv_id

    def record_intent(
        self,
        inv_id: str,
        *,
        summary: str = "",
        domain: str = "",
        services: list[str] | None = None,
        keywords: list[str] | None = None,
        signals: list[str] | None = None,
        problem_type: str = "",
        archetypes: list[dict] | None = None,
        timerange: str = "",
        tenant_id: str | None = None,
    ) -> None:
        """Record intent classification results."""
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   intent_summary=?, intent_domain=?, intent_services=?,
                   intent_keywords=?, intent_signals=?, problem_type=?,
                   archetypes=?, timerange=?
                   WHERE id=? AND tenant_id=?""",
                (
                    summary,
                    domain,
                    json.dumps(services or []),
                    json.dumps(keywords or []),
                    json.dumps(signals or []),
                    problem_type,
                    json.dumps(archetypes or []),
                    timerange,
                    inv_id,
                    selected_tenant,
                ),
            )

    def record_discovery(
        self,
        inv_id: str,
        *,
        datasources_found: int = 0,
        datasource_types: list[str] | None = None,
        metrics_catalog_size: int = 0,
        metrics_ranked_size: int = 0,
        tenant_id: str | None = None,
    ) -> None:
        """Record datasource & metric discovery results."""
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   datasources_found=?, datasource_types=?,
                   metrics_catalog_size=?, metrics_ranked_size=?
                   WHERE id=? AND tenant_id=?""",
                (
                    datasources_found,
                    json.dumps(datasource_types or []),
                    metrics_catalog_size,
                    metrics_ranked_size,
                    inv_id,
                    selected_tenant,
                ),
            )

    def record_queries(
        self,
        inv_id: str,
        *,
        metrics_selected: list[str] | None = None,
        generated_queries: list[dict] | None = None,
        panel_count: int = 0,
        path_used: str = "",
        tenant_id: str | None = None,
    ) -> None:
        """Record generated queries and panel info."""
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   metrics_selected=?, generated_queries=?,
                   panel_count=?, path_used=?
                   WHERE id=? AND tenant_id=?""",
                (
                    json.dumps(metrics_selected or []),
                    json.dumps(generated_queries or []),
                    panel_count,
                    path_used,
                    inv_id,
                    selected_tenant,
                ),
            )

    def record_validation(
        self,
        inv_id: str,
        *,
        warnings: list[str] | None = None,
        panels_dropped: int = 0,
        final_panel_count: int = 0,
        tenant_id: str | None = None,
    ) -> None:
        """Record query validation results."""
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   validation_warnings=?, panels_dropped=?, panel_count=?
                   WHERE id=? AND tenant_id=?""",
                (
                    json.dumps(warnings or []),
                    panels_dropped,
                    final_panel_count,
                    inv_id,
                    selected_tenant,
                ),
            )

    def record_stage(
        self,
        inv_id: str,
        stage: str,
        *,
        status: str,
        reason_code: str,
        details: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> None:
        """Persist one reason-coded diagnostic stage outcome."""
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT stage_outcomes FROM investigations WHERE id=? AND tenant_id=?",
                (inv_id, selected_tenant),
            ).fetchone()
            if row is None:
                return
            try:
                outcomes = json.loads(row[0] or "{}")
            except (json.JSONDecodeError, TypeError):
                outcomes = {}
            outcomes[stage] = {
                "status": status,
                "reason_code": reason_code,
                "details": details or {},
            }
            conn.execute(
                "UPDATE investigations SET stage_outcomes=? WHERE id=? AND tenant_id=?",
                (json.dumps(outcomes), inv_id, selected_tenant),
            )

    def finish(
        self,
        inv_id: str,
        *,
        status: str = "success",
        dashboard_uid: str = "",
        dashboard_url: str = "",
        error: str = "",
        timings: dict[str, float] | None = None,
        total_time: float = 0,
        tenant_id: str | None = None,
    ) -> None:
        """Record the final result of an investigation."""
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT stage_outcomes FROM investigations WHERE id=? AND tenant_id=?",
                (inv_id, selected_tenant),
            ).fetchone()
            if row is None:
                return
            try:
                outcomes = json.loads(row[0] or "{}")
            except (json.JSONDecodeError, TypeError):
                outcomes = {}
            outcomes.setdefault(
                "ranking",
                {
                    "status": "skipped",
                    "reason_code": "culprit_ranking_not_implemented",
                    "details": {},
                },
            )
            conn.execute(
                """UPDATE investigations SET
                   status=?, dashboard_uid=?, dashboard_url=?,
                   error=?, timings=?, total_time=?, finished_at=?, stage_outcomes=?
                   WHERE id=? AND tenant_id=?""",
                (
                    status,
                    dashboard_uid,
                    dashboard_url,
                    error,
                    json.dumps(timings or {}),
                    total_time,
                    time.time(),
                    json.dumps(outcomes),
                    inv_id,
                    selected_tenant,
                ),
            )

    # ── Contract revisions ───────────────────────────────────────────────

    def start_run(
        self,
        investigation_id: str,
        *,
        run_type: InvestigationRunType,
        base_revision: int | None = None,
        tenant_id: str | None = None,
    ) -> str:
        selected_tenant = self._resolve_tenant(tenant_id)
        run_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            inserted = conn.execute(
                """INSERT INTO investigation_runs (
                   run_id, investigation_id, base_revision, run_type, status, started_at
                )
                SELECT ?, i.id, ?, ?, ?, ?
                FROM investigations i
                WHERE i.id=? AND i.tenant_id=?""",
                (
                    run_id,
                    base_revision,
                    run_type.value,
                    "running",
                    time.time(),
                    investigation_id,
                    selected_tenant,
                ),
            )
            if inserted.rowcount != 1:
                raise ValueError("investigation not found for the selected tenant")
            event_written = self._append_event_in_transaction(
                conn,
                investigation_id,
                run_id,
                "run_started",
                {"run_type": run_type.value},
                tenant_id=selected_tenant,
            )
            if not event_written:
                raise RuntimeError("run start event could not be persisted")
        return run_id

    def _append_event_in_transaction(
        self,
        conn: sqlite3.Connection,
        investigation_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None,
        *,
        tenant_id: str,
    ) -> bool:
        """Append an event using the caller's active write transaction."""
        row = conn.execute(
            """SELECT COALESCE(MAX(e.sequence), 0) AS current
               FROM investigation_runs r
               JOIN investigations i ON i.id=r.investigation_id
               LEFT JOIN investigation_events e
                 ON e.run_id=r.run_id AND e.investigation_id=r.investigation_id
               WHERE r.run_id=? AND r.investigation_id=? AND i.tenant_id=?
               GROUP BY r.run_id, r.investigation_id""",
            (run_id, investigation_id, tenant_id),
        ).fetchone()
        if row is None:
            return False
        sequence = int(row["current"] or 0) + 1
        inserted = conn.execute(
            """INSERT INTO investigation_events (
               event_id, investigation_id, run_id, sequence, event_type, payload_json, created_at
            )
            SELECT ?, r.investigation_id, r.run_id, ?, ?, ?, ?
            FROM investigation_runs r
            JOIN investigations i ON i.id=r.investigation_id
            WHERE r.run_id=? AND r.investigation_id=? AND i.tenant_id=?""",
            (
                uuid.uuid4().hex,
                sequence,
                event_type,
                json.dumps(payload or {}, sort_keys=True),
                time.time(),
                run_id,
                investigation_id,
                tenant_id,
            ),
        )
        return inserted.rowcount == 1

    def append_event(
        self,
        investigation_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        tenant_id: str | None = None,
    ) -> None:
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._append_event_in_transaction(
                conn,
                investigation_id,
                run_id,
                event_type,
                payload,
                tenant_id=selected_tenant,
            )

    def complete_run(
        self,
        run_id: str,
        *,
        status: str,
        error_code: str = "",
        error_detail: str = "",
        runtime_manifest: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> None:
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT r.investigation_id FROM investigation_runs r
                   JOIN investigations i ON i.id=r.investigation_id
                   WHERE r.run_id=? AND i.tenant_id=?""",
                (run_id, selected_tenant),
            ).fetchone()
            if row is None:
                return
            investigation_id = str(row["investigation_id"])
            if runtime_manifest is None:
                updated = conn.execute(
                    """UPDATE investigation_runs SET status=?, completed_at=?, error_code=?,
                       error_detail=? WHERE run_id=? AND investigation_id=?
                       AND EXISTS (
                           SELECT 1 FROM investigations i
                           WHERE i.id=investigation_runs.investigation_id AND i.tenant_id=?
                       )""",
                    (
                        status,
                        time.time(),
                        error_code,
                        error_detail,
                        run_id,
                        investigation_id,
                        selected_tenant,
                    ),
                )
            else:
                updated = conn.execute(
                    """UPDATE investigation_runs SET status=?, completed_at=?, error_code=?,
                       error_detail=?, runtime_manifest_json=? WHERE run_id=? AND investigation_id=?
                       AND EXISTS (
                           SELECT 1 FROM investigations i
                           WHERE i.id=investigation_runs.investigation_id AND i.tenant_id=?
                       )""",
                    (
                        status,
                        time.time(),
                        error_code,
                        error_detail,
                        json.dumps(runtime_manifest, sort_keys=True),
                        run_id,
                        investigation_id,
                        selected_tenant,
                    ),
                )
            if updated.rowcount != 1:
                return
            event_type = (
                "run_completed" if status == "completed" else "run_cancelled" if status == "cancelled" else "run_failed"
            )
            event_written = self._append_event_in_transaction(
                conn,
                investigation_id,
                run_id,
                event_type,
                {"status": status, "error_code": error_code, "error_detail": error_detail},
                tenant_id=selected_tenant,
            )
            if not event_written:
                raise RuntimeError("run terminal event could not be persisted")

    def persist_contract_revision(
        self,
        contract: InvestigationContract,
        *,
        reason: str = "initial",
        run_type: InvestigationRunType = InvestigationRunType.INITIAL,
        snapshot: InvestigationReplaySnapshot | None = None,
        run_id: str | None = None,
        expected_parent_revision: int | None = None,
        applied_candidate_id: str | None = None,
    ) -> InvestigationContract:
        """Persist an immutable Investigation Contract revision.

        The store assigns the next revision number inside one transaction, then
        stamps fingerprints on the exact persisted document.
        """
        investigation_id = contract.investigation.id
        incoming_tenant = str(contract.request.scope.tenant_id or "default")
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        try:
            selected_tenant = self._resolve_tenant(incoming_tenant)
        except ValueError as exc:
            if configured_tenant != "*":
                raise StaleRevisionError("investigation tenant cannot change across revisions") from exc
            raise
        now = time.time()
        run_id = run_id or uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT i.tenant_id, COALESCE(MAX(r.revision), 0) AS current
                   FROM investigations i
                   LEFT JOIN investigation_revisions r ON r.investigation_id=i.id
                   WHERE i.id=? AND i.tenant_id=?
                   GROUP BY i.id, i.tenant_id""",
                (investigation_id, selected_tenant),
            ).fetchone()
            if row is None:
                raise StaleRevisionError("investigation not found for the selected tenant")
            current = int(row["current"] or 0)
            if expected_parent_revision is not None and current != expected_parent_revision:
                raise StaleRevisionError(
                    f"expected parent revision {expected_parent_revision}, current revision is {current}"
                )
            if str(row["tenant_id"] or "default") != incoming_tenant:
                raise StaleRevisionError("investigation tenant cannot change across revisions")
            candidate_row = None
            if applied_candidate_id is not None:
                candidate_row = conn.execute(
                    """SELECT c.* FROM knowledge_candidates c
                       JOIN investigations i ON i.id=c.investigation_id
                       WHERE c.id=? AND c.investigation_id=? AND c.revision=? AND c.status=?
                         AND i.tenant_id=?""",
                    (
                        applied_candidate_id,
                        investigation_id,
                        current,
                        KnowledgeCandidateStatus.APPROVED.value,
                        selected_tenant,
                    ),
                ).fetchone()
                if candidate_row is None:
                    raise StaleRevisionError(
                        f"knowledge candidate {applied_candidate_id} is no longer approved for revision {current}"
                    )
            revision = current + 1
            parent_revision = current or None
            investigation = contract.investigation.model_copy(
                update={"revision": revision, "parent_revision": parent_revision}
            )
            corrections = [
                (
                    correction.model_copy(update={"applied_in_revision": revision})
                    if correction.applied_in_revision is None
                    else correction
                )
                for correction in contract.corrections
            ]
            knowledge_usage = [
                usage.model_copy(
                    update={
                        "usage_id": f"usage_{investigation_id}_{revision}_{index:02d}",
                        "investigation_id": investigation_id,
                        "investigation_revision": revision,
                    }
                )
                for index, usage in enumerate(contract.knowledge_usage, start=1)
            ]
            renderings = contract.renderings.copy()
            dashboard_rendering = dict(renderings.get("dashboard", {}))
            references = dict(dashboard_rendering.get("references", {}))
            references.update({"investigation_id": investigation_id, "revision": revision})
            dashboard_rendering["references"] = references
            renderings["dashboard"] = dashboard_rendering
            stamped = stamp_fingerprints(
                contract.model_copy(
                    update={
                        "investigation": investigation,
                        "renderings": renderings,
                        "corrections": corrections,
                        "knowledge_usage": knowledge_usage,
                    }
                )
            )
            payload = stamped.model_dump(mode="json", by_alias=True)
            existing_run = conn.execute(
                """SELECT r.run_id FROM investigation_runs r
                   JOIN investigations i ON i.id=r.investigation_id
                   WHERE r.run_id=? AND r.investigation_id=? AND i.tenant_id=?""",
                (run_id, investigation_id, selected_tenant),
            ).fetchone()
            if existing_run is None:
                conflicting_run = conn.execute(
                    "SELECT 1 FROM investigation_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
                if conflicting_run is not None:
                    raise StaleRevisionError("investigation run does not belong to the selected investigation tenant")
                inserted_run = conn.execute(
                    """INSERT INTO investigation_runs (
                   run_id, investigation_id, base_revision, run_type, status,
                   started_at, completed_at, runtime_manifest_json
                )
                SELECT ?, i.id, ?, ?, ?, ?, ?, ?
                FROM investigations i
                WHERE i.id=? AND i.tenant_id=?""",
                    (
                        run_id,
                        parent_revision,
                        run_type.value,
                        "completed",
                        now,
                        now,
                        json.dumps(payload["runtime"], sort_keys=True),
                        investigation_id,
                        selected_tenant,
                    ),
                )
                if inserted_run.rowcount != 1:
                    raise StaleRevisionError("investigation not found for the selected tenant")
            else:
                updated_run = conn.execute(
                    """UPDATE investigation_runs SET runtime_manifest_json=?
                       WHERE run_id=? AND investigation_id=?
                         AND EXISTS (
                             SELECT 1 FROM investigations i
                             WHERE i.id=investigation_runs.investigation_id AND i.tenant_id=?
                         )""",
                    (
                        json.dumps(payload["runtime"], sort_keys=True),
                        run_id,
                        investigation_id,
                        selected_tenant,
                    ),
                )
                if updated_run.rowcount != 1:
                    raise StaleRevisionError("investigation run changed during revision persistence")
            inserted_revision = conn.execute(
                """INSERT INTO investigation_revisions (
                   investigation_id, revision, parent_revision, schema_version,
                   contract_json, input_fingerprint, output_fingerprint,
                   engine_version, created_at, reason
                )
                SELECT i.id, ?, ?, ?, ?, ?, ?, ?, ?, ?
                FROM investigations i
                WHERE i.id=? AND i.tenant_id=?""",
                (
                    revision,
                    parent_revision,
                    stamped.schema_.version,
                    json.dumps(payload, sort_keys=True),
                    stamped.runtime.input_fingerprint,
                    stamped.runtime.output_fingerprint,
                    stamped.runtime.engine_version,
                    now,
                    reason,
                    investigation_id,
                    selected_tenant,
                ),
            )
            if inserted_revision.rowcount != 1:
                raise StaleRevisionError("investigation tenant changed during revision persistence")
            updated_investigation = conn.execute(
                """UPDATE investigations SET current_revision=?
                   WHERE id=? AND tenant_id=?""",
                (revision, investigation_id, selected_tenant),
            )
            if updated_investigation.rowcount != 1:
                raise StaleRevisionError("investigation tenant changed during revision persistence")
            if candidate_row is not None:
                candidate_provenance = ProvenanceRecord.model_validate_json(
                    candidate_row["provenance_json"]
                ).model_copy(update={"review_state": KnowledgeCandidateStatus.APPLIED.value})
                updated = conn.execute(
                    """UPDATE knowledge_candidates
                       SET status=?, applied_revision=?, provenance_json=?
                       WHERE id=? AND investigation_id=? AND revision=? AND status=?
                         AND EXISTS (
                             SELECT 1 FROM investigations i
                             WHERE i.id=knowledge_candidates.investigation_id AND i.tenant_id=?
                         )""",
                    (
                        KnowledgeCandidateStatus.APPLIED.value,
                        revision,
                        candidate_provenance.model_dump_json(),
                        applied_candidate_id,
                        investigation_id,
                        current,
                        KnowledgeCandidateStatus.APPROVED.value,
                        selected_tenant,
                    ),
                )
                if updated.rowcount != 1:
                    raise StaleRevisionError(f"knowledge candidate {applied_candidate_id} changed during application")
            if snapshot is not None:
                persisted_snapshot = snapshot.model_copy(
                    update={
                        "investigation_id": investigation_id,
                        "revision": revision,
                        "runtime": stamped.runtime,
                        "corrections": corrections,
                        "knowledge_usage": knowledge_usage,
                    }
                )
                inserted_snapshot = conn.execute(
                    """INSERT INTO investigation_snapshots (
                       investigation_id, revision, snapshot_version, snapshot_json, created_at
                    )
                    SELECT r.investigation_id, r.revision, ?, ?, ?
                    FROM investigation_revisions r
                    JOIN investigations i ON i.id=r.investigation_id
                    WHERE r.investigation_id=? AND r.revision=? AND i.tenant_id=?""",
                    (
                        persisted_snapshot.snapshot_version,
                        persisted_snapshot.model_dump_json(),
                        now,
                        investigation_id,
                        revision,
                        selected_tenant,
                    ),
                )
                if inserted_snapshot.rowcount != 1:
                    raise StaleRevisionError("investigation snapshot target changed during revision persistence")
            event_row = conn.execute(
                """SELECT COALESCE(MAX(e.sequence), 0) AS current
                   FROM investigation_runs r
                   JOIN investigations i ON i.id=r.investigation_id
                   LEFT JOIN investigation_events e
                     ON e.run_id=r.run_id AND e.investigation_id=r.investigation_id
                   WHERE r.run_id=? AND r.investigation_id=? AND i.tenant_id=?
                   GROUP BY r.run_id, r.investigation_id""",
                (run_id, investigation_id, selected_tenant),
            ).fetchone()
            if event_row is None:
                raise StaleRevisionError("investigation run changed during revision persistence")
            event_sequence = int(event_row["current"] or 0) + 1
            inserted_event = conn.execute(
                """INSERT INTO investigation_events (
                   event_id, investigation_id, run_id, sequence, event_type, payload_json, created_at
                )
                SELECT ?, r.investigation_id, r.run_id, ?, ?, ?, ?
                FROM investigation_runs r
                JOIN investigations i ON i.id=r.investigation_id
                WHERE r.run_id=? AND r.investigation_id=? AND i.tenant_id=?""",
                (
                    uuid.uuid4().hex,
                    event_sequence,
                    "revision_persisted",
                    json.dumps(
                        {
                            "revision": revision,
                            "input_fingerprint": stamped.runtime.input_fingerprint,
                            "output_fingerprint": stamped.runtime.output_fingerprint,
                        },
                        sort_keys=True,
                    ),
                    now,
                    run_id,
                    investigation_id,
                    selected_tenant,
                ),
            )
            if inserted_event.rowcount != 1:
                raise StaleRevisionError("investigation event target changed during revision persistence")
            if existing_run is None:
                completed_event = conn.execute(
                    """INSERT INTO investigation_events (
                       event_id, investigation_id, run_id, sequence, event_type, payload_json, created_at
                    )
                    SELECT ?, r.investigation_id, r.run_id, ?, ?, ?, ?
                    FROM investigation_runs r
                    JOIN investigations i ON i.id=r.investigation_id
                    WHERE r.run_id=? AND r.investigation_id=? AND i.tenant_id=?""",
                    (
                        uuid.uuid4().hex,
                        event_sequence + 1,
                        "run_completed",
                        json.dumps({"status": "completed"}, sort_keys=True),
                        now,
                        run_id,
                        investigation_id,
                        selected_tenant,
                    ),
                )
                if completed_event.rowcount != 1:
                    raise StaleRevisionError("investigation event target changed during revision persistence")
        return stamped

    def get_snapshot(
        self,
        investigation_id: str,
        revision: int | None = None,
        *,
        tenant_id: str | None = None,
    ) -> InvestigationReplaySnapshot | None:
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            tenant_join = " JOIN investigations i ON i.id=s.investigation_id"
            tenant_clause = " AND i.tenant_id=?"
            if revision is None:
                row = conn.execute(
                    f"""SELECT s.snapshot_json FROM investigation_snapshots s{tenant_join}
                       WHERE s.investigation_id=?{tenant_clause} ORDER BY s.revision DESC LIMIT 1""",
                    (investigation_id, tenant_id),
                ).fetchone()
            else:
                row = conn.execute(
                    f"""SELECT s.snapshot_json FROM investigation_snapshots s{tenant_join}
                       WHERE s.investigation_id=? AND s.revision=?{tenant_clause}""",
                    (investigation_id, revision, tenant_id),
                ).fetchone()
        return InvestigationReplaySnapshot.model_validate_json(row["snapshot_json"]) if row else None

    def _resolve_tenant(self, tenant_id: str | None) -> str:
        return resolve_tenant_boundary(
            str(self._settings.knowledge_tenant_id or "default"),
            tenant_id,
        )

    def list_runs_page(
        self,
        investigation_id: str,
        *,
        tenant_id: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        offset: int = 0,
    ) -> KeysetPage[dict[str, Any]]:
        """Return newest runs first with a stable keyset cursor."""
        if limit < 1 or offset < 0:
            raise ValueError("invalid run page bounds")
        if cursor and offset:
            raise ValueError("run cursor and offset cannot be combined")
        selected_tenant = self._resolve_tenant(tenant_id)
        clauses = ["r.investigation_id=?", "i.tenant_id=?"]
        params: list[Any] = [investigation_id, selected_tenant]
        if cursor:
            started_at, run_id = _decode_timestamp_cursor(cursor, label="run")
            clauses.append("(r.started_at < ? OR (r.started_at = ? AND r.run_id < ?))")
            params.extend([started_at, started_at, run_id])
        params.extend([limit + 1, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT r.* FROM investigation_runs r
                    JOIN investigations i ON i.id=r.investigation_id
                    WHERE {' AND '.join(clauses)}
                    ORDER BY r.started_at DESC, r.run_id DESC
                    LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        items: list[dict[str, Any]] = []
        for row in visible:
            item = dict(row)
            item["runtime_manifest"] = json.loads(item.pop("runtime_manifest_json") or "{}")
            items.append(item)
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(float(last["started_at"]), str(last["run_id"]))
        return KeysetPage(items=items, has_more=has_more, next_cursor=next_cursor)

    def list_events_page(
        self,
        investigation_id: str,
        run_id: str | None = None,
        *,
        tenant_id: str | None = None,
        limit: int = 500,
        cursor: str | None = None,
        offset: int = 0,
    ) -> KeysetPage[dict[str, Any]]:
        """Return newest lifecycle events first with a stable keyset cursor."""
        if limit < 1 or offset < 0:
            raise ValueError("invalid event page bounds")
        if cursor and offset:
            raise ValueError("event cursor and offset cannot be combined")
        selected_tenant = self._resolve_tenant(tenant_id)
        clauses = ["e.investigation_id=?", "i.tenant_id=?"]
        params: list[Any] = [investigation_id, selected_tenant]
        if run_id is not None:
            clauses.append("e.run_id=?")
            params.append(run_id)
            if cursor:
                sequence, event_id = _decode_sequence_cursor(cursor)
                clauses.append("(e.sequence < ? OR (e.sequence = ? AND e.event_id < ?))")
                params.extend([sequence, sequence, event_id])
            order_by = "e.sequence DESC, e.event_id DESC"
        else:
            if cursor:
                created_at, sequence, event_id = _decode_event_cursor(cursor)
                clauses.append(
                    "(e.created_at < ? OR "
                    "(e.created_at = ? AND e.sequence < ?) OR "
                    "(e.created_at = ? AND e.sequence = ? AND e.event_id < ?))"
                )
                params.extend([created_at, created_at, sequence, created_at, sequence, event_id])
            order_by = "e.created_at DESC, e.sequence DESC, e.event_id DESC"
        params.extend([limit + 1, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT e.* FROM investigation_events e
                    JOIN investigations i ON i.id=e.investigation_id
                    WHERE {' AND '.join(clauses)}
                    ORDER BY {order_by}
                    LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        items: list[dict[str, Any]] = []
        for row in visible:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            items.append(item)
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            if run_id is not None:
                next_cursor = encode_cursor(int(last["sequence"]), str(last["event_id"]))
            else:
                next_cursor = encode_cursor(
                    float(last["created_at"]),
                    int(last["sequence"]),
                    str(last["event_id"]),
                )
        return KeysetPage(items=items, has_more=has_more, next_cursor=next_cursor)

    def list_revisions_page(
        self,
        investigation_id: str,
        *,
        tenant_id: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
        offset: int = 0,
    ) -> KeysetPage[dict[str, Any]]:
        """Return newest immutable revisions first with a stable keyset cursor."""
        if limit < 1 or offset < 0:
            raise ValueError("invalid revision page bounds")
        if cursor and offset:
            raise ValueError("revision cursor and offset cannot be combined")
        selected_tenant = self._resolve_tenant(tenant_id)
        clauses = ["r.investigation_id=?", "i.tenant_id=?"]
        params: list[Any] = [investigation_id, selected_tenant]
        if cursor:
            clauses.append("r.revision < ?")
            params.append(_decode_revision_cursor(cursor))
        params.extend([limit + 1, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT r.investigation_id, r.revision, r.parent_revision, r.schema_version,
                           r.input_fingerprint, r.output_fingerprint, r.engine_version, r.created_at, r.reason
                    FROM investigation_revisions r
                    JOIN investigations i ON i.id=r.investigation_id
                    WHERE {' AND '.join(clauses)}
                    ORDER BY r.revision DESC
                    LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = encode_cursor(int(visible[-1]["revision"])) if has_more and visible else None
        return KeysetPage(
            items=[dict(row) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_runs(
        self,
        investigation_id: str,
        *,
        tenant_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        selected_tenant = self._resolve_tenant(tenant_id)
        pagination = ""
        params: list[Any] = [investigation_id, selected_tenant]
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT r.* FROM investigation_runs r
                   JOIN investigations i ON i.id=r.investigation_id
                   WHERE r.investigation_id=? AND i.tenant_id=?
                   ORDER BY r.started_at ASC{pagination}""",
                params,
            ).fetchall()
        runs: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["runtime_manifest"] = json.loads(item.pop("runtime_manifest_json") or "{}")
            runs.append(item)
        return runs

    def list_events(
        self,
        investigation_id: str,
        run_id: str | None = None,
        *,
        tenant_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        selected_tenant = self._resolve_tenant(tenant_id)
        pagination = ""
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
        with self._conn() as conn:
            if run_id is None:
                params: list[Any] = [investigation_id, selected_tenant]
                if limit is not None:
                    params.extend([limit, offset])
                elif offset:
                    params.append(offset)
                rows = conn.execute(
                    f"""SELECT e.* FROM investigation_events e
                       JOIN investigations i ON i.id=e.investigation_id
                       WHERE e.investigation_id=? AND i.tenant_id=?
                       ORDER BY e.created_at ASC, e.sequence ASC{pagination}""",
                    params,
                ).fetchall()
            else:
                params = [investigation_id, run_id, selected_tenant]
                if limit is not None:
                    params.extend([limit, offset])
                elif offset:
                    params.append(offset)
                rows = conn.execute(
                    f"""SELECT e.* FROM investigation_events e
                       JOIN investigations i ON i.id=e.investigation_id
                       WHERE e.investigation_id=? AND e.run_id=? AND i.tenant_id=?
                       ORDER BY e.sequence ASC{pagination}""",
                    params,
                ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            events.append(item)
        return events

    def list_revisions(
        self,
        investigation_id: str,
        *,
        tenant_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List immutable contract revisions for one investigation."""
        tenant_id = self._resolve_tenant(tenant_id)
        pagination = ""
        params: list[Any] = [investigation_id, tenant_id]
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT r.investigation_id, r.revision, r.parent_revision, r.schema_version,
                           r.input_fingerprint, r.output_fingerprint, r.engine_version, r.created_at, r.reason
                    FROM investigation_revisions r
                    JOIN investigations i ON i.id=r.investigation_id
                    WHERE r.investigation_id=? AND i.tenant_id=?
                    ORDER BY r.revision ASC{pagination}""",
                params,
            ).fetchall()
            return [dict(row) for row in rows]

    def get_contract(
        self,
        investigation_id: str,
        revision: int | None = None,
        *,
        tenant_id: str | None = None,
    ) -> InvestigationContract | None:
        """Load a contract by revision, or the current revision when omitted."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            tenant_join = " JOIN investigations i ON i.id=r.investigation_id"
            tenant_clause = " AND i.tenant_id=?"
            if revision is None:
                row = conn.execute(
                    f"""SELECT r.contract_json FROM investigation_revisions r{tenant_join}
                       WHERE r.investigation_id=?{tenant_clause}
                       ORDER BY r.revision DESC LIMIT 1""",
                    (investigation_id, tenant_id),
                ).fetchone()
            else:
                row = conn.execute(
                    f"""SELECT r.contract_json FROM investigation_revisions r{tenant_join}
                       WHERE r.investigation_id=? AND r.revision=?{tenant_clause}""",
                    (investigation_id, revision, tenant_id),
                ).fetchone()
        if row is None:
            return None
        try:
            return InvestigationContract.model_validate_json(row["contract_json"])
        except Exception:
            logger.warning(
                "investigation_contract_deserialize_failed",
                investigation_id=investigation_id,
                exc_info=True,
            )
            return None

    def _uses_legacy_v1_fingerprint(
        self,
        investigation_id: str,
        revision: int,
        *,
        tenant_id: str | None = None,
    ) -> bool:
        with self._conn() as conn:
            tenant_join = " JOIN investigations i ON i.id=r.investigation_id" if tenant_id else ""
            tenant_clause = " AND i.tenant_id=?" if tenant_id else ""
            row = conn.execute(
                f"""SELECT r.contract_json FROM investigation_revisions r{tenant_join}
                   WHERE r.investigation_id=? AND r.revision=?{tenant_clause}""",
                (investigation_id, revision, tenant_id) if tenant_id else (investigation_id, revision),
            ).fetchone()
        if row is None:
            return False
        try:
            payload = json.loads(row["contract_json"])
        except (TypeError, json.JSONDecodeError):
            return False
        schema = payload.get("schema", {})
        return schema.get("version") == "1.0" and (
            "knowledge_snapshot_ref" not in payload or "knowledge_usage" not in payload
        )

    def replay_contract(
        self,
        investigation_id: str,
        revision: int | None = None,
        *,
        mode: ReplayMode = ReplayMode.EXACT,
        changes: CounterfactualChanges | None = None,
        runtime_settings: Settings | None = None,
        knowledge_service_factory: Callable[[], Any] | None = None,
        tenant_id: str | None = None,
    ) -> InvestigationContract | None:
        """Rebuild a contract from captured inputs without external refetch."""
        active_settings = runtime_settings or self._settings
        enforce_knowledge_action(active_settings, KnowledgeAction.READ)
        if mode != ReplayMode.EXACT:
            enforce_knowledge_action(active_settings, KnowledgeAction.APPLY)
        configured_tenant = str(getattr(active_settings, "knowledge_tenant_id", "default") or "default")
        try:
            selected_tenant = resolve_tenant_boundary(configured_tenant, tenant_id)
        except ValueError as exc:
            raise ReplayError(str(exc)) from exc
        contract = self.get_contract(investigation_id, revision, tenant_id=selected_tenant)
        if contract is None:
            return None
        legacy_v1_fingerprint = self._uses_legacy_v1_fingerprint(
            investigation_id,
            contract.investigation.revision,
            tenant_id=selected_tenant,
        )
        snapshot = self.get_snapshot(
            investigation_id,
            contract.investigation.revision,
            tenant_id=selected_tenant,
        )
        run_id = self.start_run(
            investigation_id,
            run_type=InvestigationRunType.REPLAY,
            base_revision=contract.investigation.revision,
            tenant_id=selected_tenant,
        )
        if mode != ReplayMode.EXACT:
            latest = self.get_contract(investigation_id, tenant_id=selected_tenant)
            if latest is not None and latest.investigation.revision != contract.investigation.revision:
                detail = (
                    f"expected parent revision {contract.investigation.revision}, "
                    f"current revision is {latest.investigation.revision}"
                )
                self.complete_run(
                    run_id,
                    status="failed",
                    error_code="stale_base_revision",
                    error_detail=detail,
                    tenant_id=selected_tenant,
                )
                raise StaleRevisionError(detail)
        if snapshot is None:
            if mode != ReplayMode.EXACT:
                detail = (
                    f"Captured replay inputs are unavailable for investigation {investigation_id} "
                    f"revision {contract.investigation.revision}; {mode.value} replay cannot be evaluated"
                )
                self.append_event(
                    investigation_id,
                    run_id,
                    "replay_inputs_unavailable",
                    {
                        "revision": contract.investigation.revision,
                        "mode": mode.value,
                        "captured_inputs_available": False,
                    },
                    tenant_id=selected_tenant,
                )
                self.complete_run(
                    run_id,
                    status="failed",
                    error_code="replay_inputs_unavailable",
                    error_detail=detail,
                    tenant_id=selected_tenant,
                )
                raise ReplayInputsUnavailableError(detail)
            self.append_event(
                investigation_id,
                run_id,
                "replay_legacy_contract_loaded",
                {"revision": contract.investigation.revision, "captured_inputs_available": False},
                tenant_id=selected_tenant,
            )
            self.complete_run(
                run_id,
                status="completed",
                runtime_manifest=contract.runtime.model_dump(mode="json"),
                tenant_id=selected_tenant,
            )
            return contract
        try:
            replay_snapshot = snapshot
            if mode != ReplayMode.EXACT:
                replay_snapshot = _normalize_legacy_snapshot_tenant(
                    replay_snapshot,
                    selected_tenant=selected_tenant,
                    configured_tenant=configured_tenant,
                )
            if mode == ReplayMode.CURRENT_ENGINE:
                from tacit.knowledge.scope import investigation_knowledge_scope

                current_runtime = RuntimeManifest()
                changed_runtime_components = [
                    field_name
                    for field_name in (
                        "engine_version",
                        "policy_version",
                        "ranking_version",
                        "vocabulary_version",
                    )
                    if getattr(replay_snapshot.runtime, field_name) != getattr(current_runtime, field_name)
                ]
                if changed_runtime_components:
                    raise ReplayInputsUnavailableError(
                        "Captured replay inputs cannot execute changed pipeline stages for current-engine replay "
                        f"({', '.join(changed_runtime_components)} changed); refresh the investigation instead"
                    )

                if knowledge_service_factory is not None:
                    knowledge_service = knowledge_service_factory()
                else:
                    from tacit.runtime_stores import RuntimeStores

                    knowledge_service = RuntimeStores(active_settings).knowledge()
                archetype_ids = {
                    *[match.type for match in replay_snapshot.intent.archetypes],
                    replay_snapshot.intent.problem_type,
                    *[
                        panel.source_archetype
                        for panel in replay_snapshot.dashboard_spec.panels
                        if panel.source_archetype
                    ],
                }
                _selected_knowledge_snapshot, knowledge_usage = knowledge_service.create_snapshot(
                    investigation_knowledge_scope(
                        tenant_id=selected_tenant,
                        prompt=replay_snapshot.request.prompt,
                        services=replay_snapshot.intent.services,
                        archetype_ids=archetype_ids,
                    )
                )
                knowledge_usage = knowledge_service.reconcile_live_observations(
                    knowledge_usage,
                    replay_snapshot.evidence_observations,
                )
                current_knowledge_snapshot = knowledge_service.snapshot_from_usage(
                    selected_tenant,
                    knowledge_usage,
                )
                blocker_check = getattr(knowledge_service, "captured_stage_replay_blockers", None)
                if not callable(blocker_check):
                    raise ReplayInputsUnavailableError(
                        "Current-engine replay requires an exact captured-stage knowledge verifier"
                    )
                replay_blockers = blocker_check(
                    tenant_id=selected_tenant,
                    historical_snapshot_ref=replay_snapshot.knowledge_snapshot_ref,
                    current_snapshot=current_knowledge_snapshot,
                    historical_usage=replay_snapshot.knowledge_usage,
                )
                if replay_blockers:
                    raise ReplayInputsUnavailableError(
                        "Current knowledge changed stages whose captured discovery and compilation inputs are "
                        f"unavailable for offline replay: {', '.join(replay_blockers)}"
                    )
                baseline_ranking = replay_snapshot.baseline_culprit_ranking or replay_snapshot.culprit_ranking
                replay_ranking, knowledge_usage = knowledge_service.apply_to_ranking(
                    baseline_ranking,
                    knowledge_usage,
                )
                knowledge_usage = _merge_current_engine_replay_usage(
                    knowledge_usage,
                    replay_snapshot.knowledge_usage,
                    tenant_id=selected_tenant,
                )
                knowledge_snapshot = knowledge_service.snapshot_from_usage(selected_tenant, knowledge_usage)
                replay_snapshot = replay_snapshot.model_copy(
                    update={
                        "request": replay_snapshot.request.model_copy(update={"tenant_id": selected_tenant}),
                        "culprit_ranking": replay_ranking,
                        "knowledge_snapshot_ref": knowledge_snapshot.id,
                        "knowledge_usage": knowledge_usage,
                    }
                )
            rebuilt = rebuild_contract(replay_snapshot, mode=mode, changes=changes)
            if mode == ReplayMode.EXACT and legacy_v1_fingerprint:
                rebuilt = stamp_fingerprints(rebuilt, include_knowledge_fields=False)
            self.append_event(
                investigation_id,
                run_id,
                "replay_rebuilt_captured_inputs",
                {
                    "revision": contract.investigation.revision,
                    "mode": mode.value,
                    "matched_output": rebuilt.runtime.output_fingerprint == contract.runtime.output_fingerprint,
                },
                tenant_id=selected_tenant,
            )
            if mode == ReplayMode.EXACT:
                if rebuilt.runtime.output_fingerprint != contract.runtime.output_fingerprint:
                    detail = (
                        f"Exact replay output fingerprint does not match investigation {investigation_id} "
                        f"revision {contract.investigation.revision}"
                    )
                    self.complete_run(
                        run_id,
                        status="failed",
                        error_code="exact_replay_output_mismatch",
                        error_detail=detail,
                        runtime_manifest=rebuilt.runtime.model_dump(mode="json"),
                        tenant_id=selected_tenant,
                    )
                    raise ExactReplayMismatchError(detail)
                self.complete_run(
                    run_id,
                    status="completed",
                    runtime_manifest=rebuilt.runtime.model_dump(mode="json"),
                    tenant_id=selected_tenant,
                )
                return rebuilt
            reason = "current-engine-replay" if mode == ReplayMode.CURRENT_ENGINE else "counterfactual-replay"
            persisted_snapshot = (
                apply_counterfactual(replay_snapshot, changes or CounterfactualChanges())
                if mode == ReplayMode.COUNTERFACTUAL
                else replay_snapshot
            )
            persisted = self.persist_contract_revision(
                rebuilt,
                reason=reason,
                run_type=InvestigationRunType.REPLAY,
                snapshot=persisted_snapshot,
                run_id=run_id,
                expected_parent_revision=contract.investigation.revision,
            )
            if mode == ReplayMode.CURRENT_ENGINE and persisted.knowledge_usage:
                try:
                    knowledge_service.persist_usage(
                        persisted.knowledge_usage,
                        investigation_id=persisted.investigation.id,
                        investigation_revision=persisted.investigation.revision,
                    )
                except Exception:
                    logger.warning(
                        "replay_knowledge_usage_persist_failed",
                        investigation_id=persisted.investigation.id,
                        investigation_revision=persisted.investigation.revision,
                        exc_info=True,
                    )
            self.complete_run(
                run_id,
                status="completed",
                runtime_manifest=persisted.runtime.model_dump(mode="json"),
                tenant_id=selected_tenant,
            )
            return persisted
        except ExactReplayMismatchError:
            raise
        except StaleRevisionError as exc:
            self.complete_run(
                run_id,
                status="failed",
                error_code="stale_base_revision",
                error_detail=str(exc),
                tenant_id=selected_tenant,
            )
            raise
        except Exception as exc:
            self.complete_run(
                run_id,
                status="failed",
                error_code="replay_failed",
                error_detail=f"{type(exc).__name__}: {exc}",
                tenant_id=selected_tenant,
            )
            raise

    def compare_revisions(
        self,
        investigation_id: str,
        left: int,
        right: int,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        left_contract = self.get_contract(investigation_id, left, tenant_id=tenant_id)
        right_contract = self.get_contract(investigation_id, right, tenant_id=tenant_id)
        if left_contract is None or right_contract is None:
            return None
        left_payload = normalized_output_payload(left_contract)
        right_payload = normalized_output_payload(right_contract)
        changed_sections = [
            key
            for key in left_payload
            if key not in {"investigation", "runtime"} and left_payload.get(key) != right_payload.get(key)
        ]
        return {
            "investigation_id": investigation_id,
            "left_revision": left,
            "right_revision": right,
            "same_input": left_contract.runtime.input_fingerprint == right_contract.runtime.input_fingerprint,
            "same_output": left_contract.runtime.output_fingerprint == right_contract.runtime.output_fingerprint,
            "left_output_fingerprint": left_contract.runtime.output_fingerprint,
            "right_output_fingerprint": right_contract.runtime.output_fingerprint,
            "changed_sections": changed_sections,
        }

    def create_knowledge_candidate(
        self,
        investigation_id: str,
        *,
        revision: int | None,
        correction_text: str,
        target_ref: str = "",
        created_by: str = "",
        expires_at: datetime | None = None,
        tenant_id: str | None = None,
    ) -> KnowledgeCandidate | None:
        """Store a human correction as a reviewable knowledge candidate."""
        enforce_knowledge_action(self._settings, KnowledgeAction.CORRECT)
        selected_tenant = self._resolve_tenant(tenant_id)
        contract = self.get_contract(investigation_id, revision, tenant_id=selected_tenant)
        if contract is None:
            return None
        now = utc_now()
        candidate_id = f"kc_{uuid.uuid4().hex[:16]}"
        provenance = ProvenanceRecord(
            id=f"prov_{candidate_id}",
            source_type="human_correction",
            source_ref=created_by or "anonymous",
            source_version=fingerprint({"correction_text": correction_text, "target_ref": target_ref}),
            ingested_at=now,
            observed_at=now,
            freshness={"status": "candidate", "last_verified_at": now.isoformat()},
            review_state="pending_review",
        )
        candidate = KnowledgeCandidate(
            id=candidate_id,
            investigation_id=investigation_id,
            revision=contract.investigation.revision,
            correction_text=correction_text,
            target_ref=target_ref,
            created_by=created_by,
            created_at=now,
            expires_at=expires_at,
            provenance=provenance,
        )
        with self._conn() as conn:
            inserted = conn.execute(
                """INSERT INTO knowledge_candidates (
                   id, investigation_id, revision, correction_text, target_ref,
                   candidate_type, status, created_by, created_at, expires_at, provenance_json
                )
                SELECT ?, i.id, ?, ?, ?, ?, ?, ?, ?, ?, ?
                FROM investigations i
                WHERE i.id=? AND i.tenant_id=?
                  AND EXISTS (
                      SELECT 1 FROM investigation_revisions r
                      WHERE r.investigation_id=i.id AND r.revision=?
                  )""",
                (
                    candidate.id,
                    candidate.revision,
                    candidate.correction_text,
                    candidate.target_ref,
                    candidate.candidate_type,
                    candidate.status,
                    candidate.created_by,
                    candidate.created_at.timestamp(),
                    candidate.expires_at.timestamp() if candidate.expires_at else None,
                    json.dumps(candidate.provenance.model_dump(mode="json"), sort_keys=True),
                    candidate.investigation_id,
                    selected_tenant,
                    candidate.revision,
                ),
            )
            if inserted.rowcount != 1:
                return None
        return candidate

    def list_knowledge_candidates(
        self,
        investigation_id: str,
        *,
        tenant_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeCandidate]:
        selected_tenant = self._resolve_tenant(tenant_id)
        pagination = ""
        params: list[Any] = [investigation_id, selected_tenant]
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT c.* FROM knowledge_candidates c
                   JOIN investigations i ON i.id=c.investigation_id
                   WHERE c.investigation_id=? AND i.tenant_id=?
                   ORDER BY c.created_at ASC{pagination}""",
                params,
            ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def review_knowledge_candidate(
        self,
        investigation_id: str,
        candidate_id: str,
        *,
        approved: bool,
        reviewed_by: str,
        tenant_id: str | None = None,
    ) -> KnowledgeCandidate | None:
        enforce_knowledge_action(
            self._settings,
            KnowledgeAction.APPROVE if approved else KnowledgeAction.REJECT,
        )
        selected_tenant = self._resolve_tenant(tenant_id)
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT c.* FROM knowledge_candidates c
                   JOIN investigations i ON i.id=c.investigation_id
                   WHERE c.id=? AND c.investigation_id=? AND i.tenant_id=?""",
                (candidate_id, investigation_id, selected_tenant),
            ).fetchone()
            if row is None:
                return None
            candidate = self._candidate_from_row(row)
            if candidate.status != KnowledgeCandidateStatus.PENDING_REVIEW:
                return candidate
            status = KnowledgeCandidateStatus.APPROVED if approved else KnowledgeCandidateStatus.REJECTED
            provenance = candidate.provenance.model_copy(update={"review_state": status.value})
            updated = conn.execute(
                """UPDATE knowledge_candidates
                   SET status=?, reviewed_by=?, reviewed_at=?, provenance_json=?
                   WHERE id=? AND investigation_id=? AND status=?
                     AND EXISTS (
                         SELECT 1 FROM investigations i
                         WHERE i.id=knowledge_candidates.investigation_id AND i.tenant_id=?
                     )""",
                (
                    status.value,
                    reviewed_by,
                    now.timestamp(),
                    provenance.model_dump_json(),
                    candidate_id,
                    investigation_id,
                    KnowledgeCandidateStatus.PENDING_REVIEW.value,
                    selected_tenant,
                ),
            )
            if updated.rowcount != 1:
                current_row = conn.execute(
                    """SELECT c.* FROM knowledge_candidates c
                       JOIN investigations i ON i.id=c.investigation_id
                       WHERE c.id=? AND c.investigation_id=? AND i.tenant_id=?""",
                    (candidate_id, investigation_id, selected_tenant),
                ).fetchone()
                return self._candidate_from_row(current_row) if current_row is not None else None
        return candidate.model_copy(
            update={
                "status": status,
                "reviewed_by": reviewed_by,
                "reviewed_at": now,
                "provenance": provenance,
            }
        )

    def apply_knowledge_candidate(
        self,
        investigation_id: str,
        candidate_id: str,
        *,
        tenant_id: str | None = None,
    ) -> InvestigationContract | None:
        enforce_knowledge_action(self._settings, KnowledgeAction.APPLY)
        selected_tenant = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                """SELECT c.* FROM knowledge_candidates c
                   JOIN investigations i ON i.id=c.investigation_id
                   WHERE c.id=? AND c.investigation_id=? AND i.tenant_id=?""",
                (candidate_id, investigation_id, selected_tenant),
            ).fetchone()
        if row is None:
            return None
        candidate = self._candidate_from_row(row)
        if candidate.status == KnowledgeCandidateStatus.APPLIED and candidate.applied_revision is not None:
            return self.get_contract(
                candidate.investigation_id,
                candidate.applied_revision,
                tenant_id=selected_tenant,
            )
        if candidate.status != KnowledgeCandidateStatus.APPROVED:
            return None
        if candidate.expires_at and candidate.expires_at <= utc_now():
            provenance = candidate.provenance.model_copy(
                update={"review_state": KnowledgeCandidateStatus.EXPIRED.value}
            )
            with self._conn() as conn:
                conn.execute(
                    """UPDATE knowledge_candidates SET status=?, provenance_json=?
                       WHERE id=? AND investigation_id=? AND status=?
                         AND EXISTS (
                             SELECT 1 FROM investigations i
                             WHERE i.id=knowledge_candidates.investigation_id AND i.tenant_id=?
                         )""",
                    (
                        KnowledgeCandidateStatus.EXPIRED.value,
                        provenance.model_dump_json(),
                        candidate_id,
                        investigation_id,
                        KnowledgeCandidateStatus.APPROVED.value,
                        selected_tenant,
                    ),
                )
            return None
        contract = self.get_contract(candidate.investigation_id, tenant_id=selected_tenant)
        if contract is None or contract.investigation.revision != candidate.revision:
            return None
        contract = _normalize_legacy_contract_tenant(
            contract,
            selected_tenant=selected_tenant,
            configured_tenant=str(self._settings.knowledge_tenant_id or "default"),
        )
        provenance = candidate.provenance.model_copy(update={"review_state": "approved"})
        decision = DecisionLogEntry(
            id=f"decision_{len(contract.decision_log) + 1:02d}",
            sequence=len(contract.decision_log) + 1,
            stage="correction",
            action="applied_human_correction",
            subject_ref=candidate.target_ref,
            reason_code="approved_knowledge_candidate",
            reason=candidate.correction_text,
            inputs=[candidate.id],
            output_ref=candidate.id,
            mechanism={"type": "human_review", "reviewed_by": candidate.reviewed_by},
            output_status="applied",
        )
        revised = contract.model_copy(
            update={
                "corrections": [*contract.corrections, CorrectionReference(correction_ref=candidate.id)],
                "provenance": [*contract.provenance, provenance],
                "decision_log": [*contract.decision_log, decision],
            }
        )
        snapshot = self.get_snapshot(
            candidate.investigation_id,
            contract.investigation.revision,
            tenant_id=selected_tenant,
        )
        if snapshot is not None:
            snapshot = _normalize_legacy_snapshot_tenant(
                snapshot,
                selected_tenant=selected_tenant,
                configured_tenant=str(self._settings.knowledge_tenant_id or "default"),
            )
            snapshot = snapshot.model_copy(
                update={
                    "corrections": [*snapshot.corrections, CorrectionReference(correction_ref=candidate.id)],
                    "additional_provenance": [*snapshot.additional_provenance, provenance],
                    "additional_decisions": [*snapshot.additional_decisions, decision],
                }
            )
        try:
            persisted = self.persist_contract_revision(
                revised,
                reason=f"correction:{candidate.id}",
                run_type=InvestigationRunType.CORRECTION_APPLICATION,
                snapshot=snapshot,
                expected_parent_revision=candidate.revision,
                applied_candidate_id=candidate.id,
            )
        except StaleRevisionError:
            with self._conn() as conn:
                current_row = conn.execute(
                    """SELECT c.* FROM knowledge_candidates c
                       JOIN investigations i ON i.id=c.investigation_id
                       WHERE c.id=? AND c.investigation_id=? AND i.tenant_id=?""",
                    (candidate_id, investigation_id, selected_tenant),
                ).fetchone()
            if current_row is not None:
                current_candidate = self._candidate_from_row(current_row)
                if (
                    current_candidate.status == KnowledgeCandidateStatus.APPLIED
                    and current_candidate.applied_revision is not None
                ):
                    return self.get_contract(
                        investigation_id,
                        current_candidate.applied_revision,
                        tenant_id=selected_tenant,
                    )
            return None
        return persisted

    def migrate_legacy_investigation(
        self,
        investigation_id: str,
        *,
        tenant_id: str | None = None,
    ) -> InvestigationContract | None:
        enforce_knowledge_action(self._settings, KnowledgeAction.READ)
        enforce_knowledge_action(self._settings, KnowledgeAction.APPLY)
        selected_tenant = self._resolve_tenant(tenant_id)
        existing = self.get_contract(investigation_id, tenant_id=selected_tenant)
        if existing is not None:
            return existing
        record = self.get(investigation_id, tenant_id=selected_tenant)
        if record is None:
            return None
        contract = InvestigationContractAssembler().from_legacy_history(record)
        return self.persist_contract_revision(
            contract,
            reason="legacy-history-migration",
            run_type=InvestigationRunType.MIGRATION,
        )

    @staticmethod
    def _candidate_from_row(row: sqlite3.Row) -> KnowledgeCandidate:
        expires_at = datetime.fromtimestamp(row["expires_at"], tz=utc_now().tzinfo) if row["expires_at"] else None
        reviewed_at = datetime.fromtimestamp(row["reviewed_at"], tz=utc_now().tzinfo) if row["reviewed_at"] else None
        return KnowledgeCandidate(
            id=row["id"],
            investigation_id=row["investigation_id"],
            revision=row["revision"],
            correction_text=row["correction_text"],
            target_ref=row["target_ref"],
            candidate_type=row["candidate_type"],
            status=row["status"],
            created_by=row["created_by"],
            created_at=datetime.fromtimestamp(row["created_at"], tz=utc_now().tzinfo),
            expires_at=expires_at,
            provenance=ProvenanceRecord.model_validate_json(row["provenance_json"]),
            reviewed_by=row["reviewed_by"],
            reviewed_at=reviewed_at,
            applied_revision=row["applied_revision"],
        )

    # ── Read operations ──────────────────────────────────────────────────

    def get(self, inv_id: str, *, tenant_id: str | None = None) -> dict[str, Any] | None:
        """Get a single investigation by ID."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM investigations WHERE id=? AND tenant_id=?",
                (inv_id, tenant_id),
            ).fetchone()
            return self._row_to_dict(row) if row else None

    def get_by_dashboard(
        self,
        dashboard_uid: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Get investigation by dashboard UID."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM investigations WHERE dashboard_uid=? AND tenant_id=?",
                (dashboard_uid, tenant_id),
            ).fetchone()
            return self._row_to_dict(row) if row else None

    def list_recent(
        self,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        user_id: str | None = None,
        tenant_id: str | None = None,
        before_started_at: float | None = None,
        before_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List recent investigations, newest first."""
        if limit < 1 or limit > 500:
            raise ValueError("limit must be between 1 and 500")
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if (before_started_at is None) != (before_id is None):
            raise ValueError("before_started_at and before_id must be supplied together")
        if before_started_at is not None and offset:
            raise ValueError("offset cannot be combined with a history cursor")
        tenant_id = self._resolve_tenant(tenant_id)
        query = "SELECT * FROM investigations"
        params: list[Any] = []
        conditions: list[str] = ["tenant_id=?"]
        params.append(tenant_id)

        if status:
            conditions.append("status=?")
            params.append(status)
        if user_id:
            conditions.append("user_id=?")
            params.append(user_id)
        if before_started_at is not None and before_id is not None:
            conditions.append("(started_at < ? OR (started_at = ? AND id < ?))")
            params.extend([before_started_at, before_started_at, before_id])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY started_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def stats(self, *, tenant_id: str | None = None) -> dict[str, Any]:
        """Aggregate one tenant's stats; wildcard runtimes must select a tenant."""
        tenant_id = resolve_tenant_boundary(
            str(self._settings.knowledge_tenant_id or "default"),
            tenant_id,
        )
        with self._conn() as conn:
            query = """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as succeeded,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status='timeout' THEN 1 ELSE 0 END) as timed_out,
                    SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END) as cancelled,
                    AVG(total_time) as avg_time,
                    AVG(panel_count) as avg_panels,
                    AVG(metrics_catalog_size) as avg_catalog_size,
                    SUM(CASE WHEN path_used='archetype' THEN 1 ELSE 0 END) as archetype_path,
                    SUM(CASE WHEN path_used='freeform' THEN 1 ELSE 0 END) as freeform_path
                FROM investigations
            """
            query += " WHERE tenant_id=?"
            params: tuple[Any, ...] = (tenant_id,)
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else {}

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        """Convert a Row to dict, parsing JSON fields."""
        d = dict(row)
        for key in (
            "intent_services",
            "intent_keywords",
            "intent_signals",
            "archetypes",
            "datasource_types",
            "metrics_selected",
            "generated_queries",
            "validation_warnings",
            "stage_outcomes",
            "timings",
        ):
            if key in d and isinstance(d[key], str):
                try:
                    d[key] = json.loads(d[key])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d


# ── Singleton ─────────────────────────────────────────────────────────────

_store: InvestigationStore | None = None


def get_investigation_store() -> InvestigationStore:
    global _store
    if _store is None:
        _store = InvestigationStore()
    return _store
