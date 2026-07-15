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
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from tacit.config import settings
from tacit.investigation_contract import (
    CorrectionReference,
    DecisionLogEntry,
    InvestigationContract,
    InvestigationContractAssembler,
    InvestigationRunType,
    KnowledgeCandidate,
    KnowledgeCandidateStatus,
    ProvenanceRecord,
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

logger = structlog.get_logger()

_DEFAULT_DB_PATH = Path("data/tacit_history.db")
_SQLITE_BUSY_TIMEOUT_MS = 30_000


class StaleRevisionError(ValueError):
    """Raised when a revision-scoped operation no longer targets the current revision."""


class ReplayError(ValueError):
    """Base error for a replay request that cannot produce its promised result."""


class ReplayInputsUnavailableError(ReplayError):
    """Raised when an evaluative replay has no captured inputs to rebuild."""


class ExactReplayMismatchError(ReplayError):
    """Raised when captured inputs no longer rebuild the persisted exact output."""


def _db_path() -> Path:
    custom = getattr(settings, "history_db_path", None)
    path = Path(custom) if custom else _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS investigations (
    id              TEXT PRIMARY KEY,          -- UUID
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


class InvestigationStore:
    """SQLite-backed investigation history."""

    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or _db_path()
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
            conn.executescript(_SCHEMA_SQL)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
            if "stage_outcomes" not in columns:
                conn.execute("ALTER TABLE investigations ADD COLUMN stage_outcomes TEXT NOT NULL DEFAULT '{}'")
            if "current_revision" not in columns:
                conn.execute("ALTER TABLE investigations ADD COLUMN current_revision INTEGER NOT NULL DEFAULT 0")
            candidate_columns = {row[1] for row in conn.execute("PRAGMA table_info(knowledge_candidates)")}
            if "reviewed_by" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN reviewed_by TEXT NOT NULL DEFAULT ''")
            if "reviewed_at" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN reviewed_at REAL")
            if "applied_revision" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN applied_revision INTEGER")
        logger.info("investigation_store_init", db_path=str(self._db_path))

    # ── Write operations ──────────────────────────────────────────────────

    def start(self, prompt: str, user_id: str = "", channel_id: str = "") -> str:
        """Record the start of a new investigation. Returns investigation ID."""
        inv_id = uuid.uuid4().hex[:16]
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO investigations (id, prompt, user_id, channel_id, started_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (inv_id, prompt, user_id, channel_id, time.time()),
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
    ) -> None:
        """Record intent classification results."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   intent_summary=?, intent_domain=?, intent_services=?,
                   intent_keywords=?, intent_signals=?, problem_type=?,
                   archetypes=?, timerange=?
                   WHERE id=?""",
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
    ) -> None:
        """Record datasource & metric discovery results."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   datasources_found=?, datasource_types=?,
                   metrics_catalog_size=?, metrics_ranked_size=?
                   WHERE id=?""",
                (
                    datasources_found,
                    json.dumps(datasource_types or []),
                    metrics_catalog_size,
                    metrics_ranked_size,
                    inv_id,
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
    ) -> None:
        """Record generated queries and panel info."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   metrics_selected=?, generated_queries=?,
                   panel_count=?, path_used=?
                   WHERE id=?""",
                (
                    json.dumps(metrics_selected or []),
                    json.dumps(generated_queries or []),
                    panel_count,
                    path_used,
                    inv_id,
                ),
            )

    def record_validation(
        self,
        inv_id: str,
        *,
        warnings: list[str] | None = None,
        panels_dropped: int = 0,
        final_panel_count: int = 0,
    ) -> None:
        """Record query validation results."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   validation_warnings=?, panels_dropped=?, panel_count=?
                   WHERE id=?""",
                (
                    json.dumps(warnings or []),
                    panels_dropped,
                    final_panel_count,
                    inv_id,
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
    ) -> None:
        """Persist one reason-coded diagnostic stage outcome."""
        with self._conn() as conn:
            row = conn.execute("SELECT stage_outcomes FROM investigations WHERE id=?", (inv_id,)).fetchone()
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
                "UPDATE investigations SET stage_outcomes=? WHERE id=?",
                (json.dumps(outcomes), inv_id),
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
    ) -> None:
        """Record the final result of an investigation."""
        with self._conn() as conn:
            row = conn.execute("SELECT stage_outcomes FROM investigations WHERE id=?", (inv_id,)).fetchone()
            try:
                outcomes = json.loads(row[0] or "{}") if row else {}
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
                   WHERE id=?""",
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
                ),
            )

    # ── Contract revisions ───────────────────────────────────────────────

    def start_run(
        self,
        investigation_id: str,
        *,
        run_type: InvestigationRunType,
        base_revision: int | None = None,
    ) -> str:
        run_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO investigation_runs (
                   run_id, investigation_id, base_revision, run_type, status, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (run_id, investigation_id, base_revision, run_type.value, "running", time.time()),
            )
        self.append_event(investigation_id, run_id, "run_started", {"run_type": run_type.value})
        return run_id

    def append_event(
        self,
        investigation_id: str,
        run_id: str,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS current FROM investigation_events WHERE run_id=?",
                (run_id,),
            ).fetchone()
            sequence = int(row["current"] or 0) + 1
            conn.execute(
                """INSERT INTO investigation_events (
                   event_id, investigation_id, run_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    uuid.uuid4().hex,
                    investigation_id,
                    run_id,
                    sequence,
                    event_type,
                    json.dumps(payload or {}, sort_keys=True),
                    time.time(),
                ),
            )

    def complete_run(
        self,
        run_id: str,
        *,
        status: str,
        error_code: str = "",
        error_detail: str = "",
        runtime_manifest: dict[str, Any] | None = None,
    ) -> None:
        with self._conn() as conn:
            row = conn.execute("SELECT investigation_id FROM investigation_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                return
            if runtime_manifest is None:
                conn.execute(
                    """UPDATE investigation_runs SET status=?, completed_at=?, error_code=?,
                       error_detail=? WHERE run_id=?""",
                    (status, time.time(), error_code, error_detail, run_id),
                )
            else:
                conn.execute(
                    """UPDATE investigation_runs SET status=?, completed_at=?, error_code=?,
                       error_detail=?, runtime_manifest_json=? WHERE run_id=?""",
                    (
                        status,
                        time.time(),
                        error_code,
                        error_detail,
                        json.dumps(runtime_manifest, sort_keys=True),
                        run_id,
                    ),
                )
            investigation_id = str(row["investigation_id"])
        event_type = (
            "run_completed" if status == "completed" else "run_cancelled" if status == "cancelled" else "run_failed"
        )
        self.append_event(
            investigation_id,
            run_id,
            event_type,
            {"status": status, "error_code": error_code, "error_detail": error_detail},
        )

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
        now = time.time()
        run_id = run_id or uuid.uuid4().hex
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(revision), 0) AS current FROM investigation_revisions WHERE investigation_id=?",
                (investigation_id,),
            ).fetchone()
            current = int(row["current"] or 0)
            if expected_parent_revision is not None and current != expected_parent_revision:
                raise StaleRevisionError(
                    f"expected parent revision {expected_parent_revision}, current revision is {current}"
                )
            candidate_row = None
            if applied_candidate_id is not None:
                candidate_row = conn.execute(
                    """SELECT * FROM knowledge_candidates
                       WHERE id=? AND investigation_id=? AND revision=? AND status=?""",
                    (
                        applied_candidate_id,
                        investigation_id,
                        current,
                        KnowledgeCandidateStatus.APPROVED.value,
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
                    }
                )
            )
            payload = stamped.model_dump(mode="json", by_alias=True)
            existing_run = conn.execute("SELECT run_id FROM investigation_runs WHERE run_id=?", (run_id,)).fetchone()
            if existing_run is None:
                conn.execute(
                    """INSERT INTO investigation_runs (
                   run_id, investigation_id, base_revision, run_type, status,
                   started_at, completed_at, runtime_manifest_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        investigation_id,
                        parent_revision,
                        run_type.value,
                        "completed",
                        now,
                        now,
                        json.dumps(payload["runtime"], sort_keys=True),
                    ),
                )
            else:
                conn.execute(
                    "UPDATE investigation_runs SET runtime_manifest_json=? WHERE run_id=?",
                    (json.dumps(payload["runtime"], sort_keys=True), run_id),
                )
            conn.execute(
                """INSERT INTO investigation_revisions (
                   investigation_id, revision, parent_revision, schema_version,
                   contract_json, input_fingerprint, output_fingerprint,
                   engine_version, created_at, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    investigation_id,
                    revision,
                    parent_revision,
                    stamped.schema_.version,
                    json.dumps(payload, sort_keys=True),
                    stamped.runtime.input_fingerprint,
                    stamped.runtime.output_fingerprint,
                    stamped.runtime.engine_version,
                    now,
                    reason,
                ),
            )
            conn.execute(
                "UPDATE investigations SET current_revision=? WHERE id=?",
                (revision, investigation_id),
            )
            if candidate_row is not None:
                candidate_provenance = ProvenanceRecord.model_validate_json(
                    candidate_row["provenance_json"]
                ).model_copy(update={"review_state": KnowledgeCandidateStatus.APPLIED.value})
                updated = conn.execute(
                    """UPDATE knowledge_candidates
                       SET status=?, applied_revision=?, provenance_json=?
                       WHERE id=? AND investigation_id=? AND revision=? AND status=?""",
                    (
                        KnowledgeCandidateStatus.APPLIED.value,
                        revision,
                        candidate_provenance.model_dump_json(),
                        applied_candidate_id,
                        investigation_id,
                        current,
                        KnowledgeCandidateStatus.APPROVED.value,
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
                    }
                )
                conn.execute(
                    """INSERT INTO investigation_snapshots (
                       investigation_id, revision, snapshot_version, snapshot_json, created_at
                    ) VALUES (?, ?, ?, ?, ?)""",
                    (
                        investigation_id,
                        revision,
                        persisted_snapshot.snapshot_version,
                        persisted_snapshot.model_dump_json(),
                        now,
                    ),
                )
            event_row = conn.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS current FROM investigation_events WHERE run_id=?",
                (run_id,),
            ).fetchone()
            event_sequence = int(event_row["current"] or 0) + 1
            conn.execute(
                """INSERT INTO investigation_events (
                   event_id, investigation_id, run_id, sequence, event_type, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    uuid.uuid4().hex,
                    investigation_id,
                    run_id,
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
                ),
            )
            if existing_run is None:
                conn.execute(
                    """INSERT INTO investigation_events (
                       event_id, investigation_id, run_id, sequence, event_type, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        uuid.uuid4().hex,
                        investigation_id,
                        run_id,
                        event_sequence + 1,
                        "run_completed",
                        json.dumps({"status": "completed"}, sort_keys=True),
                        now,
                    ),
                )
        return stamped

    def get_snapshot(self, investigation_id: str, revision: int | None = None) -> InvestigationReplaySnapshot | None:
        with self._conn() as conn:
            if revision is None:
                row = conn.execute(
                    """SELECT snapshot_json FROM investigation_snapshots
                       WHERE investigation_id=? ORDER BY revision DESC LIMIT 1""",
                    (investigation_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT snapshot_json FROM investigation_snapshots
                       WHERE investigation_id=? AND revision=?""",
                    (investigation_id, revision),
                ).fetchone()
        return InvestigationReplaySnapshot.model_validate_json(row["snapshot_json"]) if row else None

    def list_runs(self, investigation_id: str) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM investigation_runs WHERE investigation_id=? ORDER BY started_at ASC",
                (investigation_id,),
            ).fetchall()
        runs: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["runtime_manifest"] = json.loads(item.pop("runtime_manifest_json") or "{}")
            runs.append(item)
        return runs

    def list_events(self, investigation_id: str, run_id: str | None = None) -> list[dict[str, Any]]:
        with self._conn() as conn:
            if run_id is None:
                rows = conn.execute(
                    """SELECT * FROM investigation_events WHERE investigation_id=?
                       ORDER BY created_at ASC, sequence ASC""",
                    (investigation_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM investigation_events WHERE investigation_id=? AND run_id=?
                       ORDER BY sequence ASC""",
                    (investigation_id, run_id),
                ).fetchall()
        events: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            events.append(item)
        return events

    def list_revisions(self, investigation_id: str) -> list[dict[str, Any]]:
        """List immutable contract revisions for one investigation."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT investigation_id, revision, parent_revision, schema_version,
                          input_fingerprint, output_fingerprint, engine_version, created_at, reason
                   FROM investigation_revisions
                   WHERE investigation_id=?
                   ORDER BY revision ASC""",
                (investigation_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_contract(self, investigation_id: str, revision: int | None = None) -> InvestigationContract | None:
        """Load a contract by revision, or the current revision when omitted."""
        with self._conn() as conn:
            if revision is None:
                row = conn.execute(
                    """SELECT contract_json FROM investigation_revisions
                       WHERE investigation_id=?
                       ORDER BY revision DESC LIMIT 1""",
                    (investigation_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT contract_json FROM investigation_revisions
                       WHERE investigation_id=? AND revision=?""",
                    (investigation_id, revision),
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

    def replay_contract(
        self,
        investigation_id: str,
        revision: int | None = None,
        *,
        mode: ReplayMode = ReplayMode.EXACT,
        changes: CounterfactualChanges | None = None,
    ) -> InvestigationContract | None:
        """Rebuild a contract from captured inputs without external refetch."""
        contract = self.get_contract(investigation_id, revision)
        if contract is None:
            return None
        snapshot = self.get_snapshot(investigation_id, contract.investigation.revision)
        run_id = self.start_run(
            investigation_id,
            run_type=InvestigationRunType.REPLAY,
            base_revision=contract.investigation.revision,
        )
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
                )
                self.complete_run(
                    run_id,
                    status="failed",
                    error_code="replay_inputs_unavailable",
                    error_detail=detail,
                )
                raise ReplayInputsUnavailableError(detail)
            self.append_event(
                investigation_id,
                run_id,
                "replay_legacy_contract_loaded",
                {"revision": contract.investigation.revision, "captured_inputs_available": False},
            )
            self.complete_run(
                run_id,
                status="completed",
                runtime_manifest=contract.runtime.model_dump(mode="json"),
            )
            return contract
        try:
            rebuilt = rebuild_contract(snapshot, mode=mode, changes=changes)
            self.append_event(
                investigation_id,
                run_id,
                "replay_rebuilt_captured_inputs",
                {
                    "revision": contract.investigation.revision,
                    "mode": mode.value,
                    "matched_output": rebuilt.runtime.output_fingerprint == contract.runtime.output_fingerprint,
                },
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
                    )
                    raise ExactReplayMismatchError(detail)
                self.complete_run(
                    run_id,
                    status="completed",
                    runtime_manifest=rebuilt.runtime.model_dump(mode="json"),
                )
                return rebuilt
            reason = "current-engine-replay" if mode == ReplayMode.CURRENT_ENGINE else "counterfactual-replay"
            persisted_snapshot = (
                apply_counterfactual(snapshot, changes or CounterfactualChanges())
                if mode == ReplayMode.COUNTERFACTUAL
                else snapshot
            )
            persisted = self.persist_contract_revision(
                rebuilt,
                reason=reason,
                run_type=InvestigationRunType.REPLAY,
                snapshot=persisted_snapshot,
                run_id=run_id,
                expected_parent_revision=contract.investigation.revision,
            )
            self.complete_run(run_id, status="completed", runtime_manifest=persisted.runtime.model_dump(mode="json"))
            return persisted
        except ExactReplayMismatchError:
            raise
        except StaleRevisionError as exc:
            self.complete_run(
                run_id,
                status="failed",
                error_code="stale_base_revision",
                error_detail=str(exc),
            )
            raise
        except Exception as exc:
            self.complete_run(
                run_id,
                status="failed",
                error_code="replay_failed",
                error_detail=f"{type(exc).__name__}: {exc}",
            )
            raise

    def compare_revisions(self, investigation_id: str, left: int, right: int) -> dict[str, Any] | None:
        left_contract = self.get_contract(investigation_id, left)
        right_contract = self.get_contract(investigation_id, right)
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
    ) -> KnowledgeCandidate | None:
        """Store a human correction as a reviewable knowledge candidate."""
        contract = self.get_contract(investigation_id, revision)
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
            conn.execute(
                """INSERT INTO knowledge_candidates (
                   id, investigation_id, revision, correction_text, target_ref,
                   candidate_type, status, created_by, created_at, expires_at, provenance_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    candidate.id,
                    candidate.investigation_id,
                    candidate.revision,
                    candidate.correction_text,
                    candidate.target_ref,
                    candidate.candidate_type,
                    candidate.status,
                    candidate.created_by,
                    candidate.created_at.timestamp(),
                    candidate.expires_at.timestamp() if candidate.expires_at else None,
                    json.dumps(candidate.provenance.model_dump(mode="json"), sort_keys=True),
                ),
            )
        return candidate

    def list_knowledge_candidates(self, investigation_id: str) -> list[KnowledgeCandidate]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM knowledge_candidates WHERE investigation_id=? ORDER BY created_at ASC",
                (investigation_id,),
            ).fetchall()
        return [self._candidate_from_row(row) for row in rows]

    def review_knowledge_candidate(
        self,
        investigation_id: str,
        candidate_id: str,
        *,
        approved: bool,
        reviewed_by: str,
    ) -> KnowledgeCandidate | None:
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM knowledge_candidates WHERE id=? AND investigation_id=?",
                (candidate_id, investigation_id),
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
                   WHERE id=? AND investigation_id=? AND status=?""",
                (
                    status.value,
                    reviewed_by,
                    now.timestamp(),
                    provenance.model_dump_json(),
                    candidate_id,
                    investigation_id,
                    KnowledgeCandidateStatus.PENDING_REVIEW.value,
                ),
            )
            if updated.rowcount != 1:
                current_row = conn.execute(
                    "SELECT * FROM knowledge_candidates WHERE id=? AND investigation_id=?",
                    (candidate_id, investigation_id),
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
    ) -> InvestigationContract | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge_candidates WHERE id=? AND investigation_id=?",
                (candidate_id, investigation_id),
            ).fetchone()
        if row is None:
            return None
        candidate = self._candidate_from_row(row)
        if candidate.status == KnowledgeCandidateStatus.APPLIED and candidate.applied_revision is not None:
            return self.get_contract(candidate.investigation_id, candidate.applied_revision)
        if candidate.status != KnowledgeCandidateStatus.APPROVED:
            return None
        if candidate.expires_at and candidate.expires_at <= utc_now():
            provenance = candidate.provenance.model_copy(
                update={"review_state": KnowledgeCandidateStatus.EXPIRED.value}
            )
            with self._conn() as conn:
                conn.execute(
                    """UPDATE knowledge_candidates SET status=?, provenance_json=?
                       WHERE id=? AND investigation_id=? AND status=?""",
                    (
                        KnowledgeCandidateStatus.EXPIRED.value,
                        provenance.model_dump_json(),
                        candidate_id,
                        investigation_id,
                        KnowledgeCandidateStatus.APPROVED.value,
                    ),
                )
            return None
        contract = self.get_contract(candidate.investigation_id)
        if contract is None or contract.investigation.revision != candidate.revision:
            return None
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
        snapshot = self.get_snapshot(candidate.investigation_id, contract.investigation.revision)
        if snapshot is not None:
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
                    "SELECT * FROM knowledge_candidates WHERE id=? AND investigation_id=?",
                    (candidate_id, investigation_id),
                ).fetchone()
            if current_row is not None:
                current_candidate = self._candidate_from_row(current_row)
                if (
                    current_candidate.status == KnowledgeCandidateStatus.APPLIED
                    and current_candidate.applied_revision is not None
                ):
                    return self.get_contract(investigation_id, current_candidate.applied_revision)
            return None
        return persisted

    def migrate_legacy_investigation(self, investigation_id: str) -> InvestigationContract | None:
        existing = self.get_contract(investigation_id)
        if existing is not None:
            return existing
        record = self.get(investigation_id)
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

    def get(self, inv_id: str) -> dict[str, Any] | None:
        """Get a single investigation by ID."""
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM investigations WHERE id=?", (inv_id,)).fetchone()
            return self._row_to_dict(row) if row else None

    def get_by_dashboard(self, dashboard_uid: str) -> dict[str, Any] | None:
        """Get investigation by dashboard UID."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM investigations WHERE dashboard_uid=?",
                (dashboard_uid,),
            ).fetchone()
            return self._row_to_dict(row) if row else None

    def list_recent(
        self,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List recent investigations, newest first."""
        query = "SELECT * FROM investigations"
        params: list[Any] = []
        conditions: list[str] = []

        if status:
            conditions.append("status=?")
            params.append(status)
        if user_id:
            conditions.append("user_id=?")
            params.append(user_id)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def stats(self) -> dict[str, Any]:
        """Aggregate stats across all investigations."""
        with self._conn() as conn:
            row = conn.execute("""
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
            """).fetchone()
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
