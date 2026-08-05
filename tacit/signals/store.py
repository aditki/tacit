"""Semantic signal mapping store and resolution engine.

Decouples archetypes from raw metric names by introducing a semantic signal
layer.  Instead of ``required_metrics: [http_request_duration_seconds]``,
archetypes declare ``required_signals: [request_latency]``.  The resolution
engine maps signals to actual metrics at compile time using:

- Metric name pattern matching
- Context filters (service, datasource type, archetype, environment)
- Confidence scores with feedback-driven adjustment
- Provenance tracking for every learned mapping

Storage: SQLite (same DB as feedback store).

Many-to-many relationship: one metric can imply multiple signals (e.g.
``queue_depth`` → saturation, throughput_mismatch, downstream_outage);
one signal can map to many metrics across environments.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar, Token
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

import structlog

from tacit.config import Settings, settings
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.knowledge.versioning import version_scope_applies
from tacit.models.schemas import MetricEntry
from tacit.pagination import KeysetPage, decode_cursor, encode_cursor
from tacit.signals.confidence import TRUST_THRESHOLD, stronger_review_state
from tacit.signals.learning_index import (
    build_alert_context_rows,
    build_learning_context_rows,
)
from tacit.signals.learning_index import (
    eligible_pairs_from_ingested_signals as _eligible_pairs_from_ingested_signals,
)
from tacit.signals.learning_index import (
    fts_query as _fts_query,
)
from tacit.signals.migrations import (
    CURRENT_SIGNAL_SCHEMA_MARKER,
    GOVERNED_PROJECTION_AUDIT_MARKER,
    ensure_governed_projection_audit_triggers,
    ensure_ingested_alert_columns,
    ensure_ingested_dashboard_backend_scope,
    ensure_learning_index,
    ensure_mapping_columns,
    ensure_schema,
    governed_projection_audit_is_current,
    mark_governed_projection_audit_current,
    projection_matches_authority,
    rebuild_ingested_dashboards_table,
    reconcile_default_tenant_owner_batch,
    require_confirmed_default_tenant_owner,
    signal_schema_is_current,
    signal_tenant_owner_is_current,
)
from tacit.signals.resolution import (
    context_matches as _context_matches,
)
from tacit.signals.resolution import (
    datasource_type_matches as _datasource_type_matches,
)
from tacit.signals.resolution import (
    effective_confidence as _effective_confidence,
)
from tacit.signals.resolution import (
    metric_matches_pattern as _metric_matches_pattern,
)
from tacit.signals.resolution import (
    metric_metadata_compatibility as _metric_metadata_compatibility,
)
from tacit.signals.resolution import (
    missing_context_multiplier as _missing_context_multiplier,
)
from tacit.signals.resolution import (
    unit_class as _unit_class,
)
from tacit.signals.resolution import (
    unit_compatibility as _unit_compatibility,
)
from tacit.signals.schema import (
    DEFAULT_DB_PATH,
    GLOBAL_BOOTSTRAP_TENANT_ID,
    SQLITE_BUSY_TIMEOUT_MS,
)
from tacit.tenancy import resolve_tenant_boundary

logger = structlog.get_logger()
_DEFAULT_OWNER_MIGRATION_BATCH_SIZE = 500
_PROJECTION_AUDIT_BATCH_SIZE = 500
_PROJECTION_AUTHORITY_VALIDATION_BATCH_SIZE = 100
_PROJECTION_AUDIT_MAX_RETRIES = 3
_SIGNAL_RESOLUTION_PAGE_SIZE = 500
_STALE_SOURCE_PAGE_SIZE = 500
_SIGNAL_RESOLUTION_MIN_SCAN_LIMIT = 10_000
_SIGNAL_RESOLUTION_MAX_SCAN_LIMIT = 100_000
_SIGNAL_RESOLUTION_SCAN_MULTIPLIER = 100
_ARTIFACT_COUNT_BATCH_SIZE = 200
_ARTIFACT_EXTRACTION_TABLES = {
    "evidence_requirements": "evidence_requirements",
    "ownership_hints": "ownership_hints",
    "dependency_hints": "dependency_hints",
    "signal_mapping_candidates": "signal_mapping_candidates",
}


def _decode_artifact_cursor(cursor: str) -> tuple[float, int]:
    try:
        raw_updated_at, raw_id = decode_cursor(cursor, field_count=2)
        if isinstance(raw_updated_at, bool) or not isinstance(raw_updated_at, (int, float)):
            raise ValueError
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id < 1:
            raise ValueError
        updated_at = float(raw_updated_at)
        if not math.isfinite(updated_at):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid artifact cursor") from exc
    return updated_at, raw_id


def _decode_extraction_cursor(cursor: str) -> tuple[str, str]:
    try:
        raw_generation, raw_id = decode_cursor(cursor, field_count=2)
        if not isinstance(raw_generation, str) or len(raw_generation) > 128:
            raise ValueError
        if not isinstance(raw_id, str) or not raw_id or len(raw_id) > 500:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid extraction cursor") from exc
    return raw_generation, raw_id


def _decode_signal_mapping_cursor(cursor: str) -> tuple[int, float, int]:
    try:
        raw_priority, raw_confidence, raw_id = decode_cursor(cursor, field_count=3)
        if isinstance(raw_priority, bool) or not isinstance(raw_priority, int) or raw_priority not in {0, 1}:
            raise ValueError
        if isinstance(raw_confidence, bool) or not isinstance(raw_confidence, (int, float)):
            raise ValueError
        if isinstance(raw_id, bool) or not isinstance(raw_id, int) or raw_id < 1:
            raise ValueError
        confidence = float(raw_confidence)
        if not math.isfinite(confidence):
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid signal mapping cursor") from exc
    return raw_priority, confidence, raw_id


class _ProjectionAuditChanged(RuntimeError):
    pass


_LEARNING_LIST_MAX_LIMIT = 10_000

__all__ = [
    "ArtifactGenerationConflictError",
    "LearningIndexUnavailable",
    "ResolvedSignal",
    "SignalStore",
    "_effective_confidence",
    "_metric_matches_pattern",
    "_missing_context_multiplier",
    "_unit_class",
    "_unit_compatibility",
    "get_signal_store",
]

_DEFAULT_DB_PATH = DEFAULT_DB_PATH
_BOOTSTRAP_FINGERPRINT_KEY = "bootstrap_signal_catalog_fingerprint"
_BOOTSTRAP_WRITE_TOKEN = object()


class LearningIndexUnavailable(RuntimeError):
    """Raised when SQLite FTS5-backed learning retrieval is unavailable."""


class ArtifactGenerationConflictError(ValueError):
    """Raised when an extraction page cursor targets a replaced source generation."""


@dataclass(frozen=True)
class ResolvedSignal:
    entry: MetricEntry
    confidence: float
    governance_ref: str = ""
    governance_revision: int = 0

    @property
    def knowledge_revision_ref(self) -> KnowledgeRevisionRef | None:
        if not self.governance_ref or self.governance_revision < 1:
            return None
        return KnowledgeRevisionRef(self.governance_ref, self.governance_revision)


@dataclass(frozen=True)
class _PinnedGovernedMappings:
    tenant_id: str
    mappings: tuple[dict[str, Any], ...]


def _escape_like_prefix(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _projection_scope_values(refs: list[str], prefix: str) -> list[str]:
    return sorted(
        {value.removeprefix(prefix) for value in refs if value and (value.startswith(prefix) or ":" not in value)}
    )


def _stronger_review_state(existing: str, incoming: str) -> str:
    """Return the higher-trust review state without allowing downgrades."""
    return stronger_review_state(existing, incoming)


def _db_path(runtime_settings: Settings | None = None) -> Path:
    active_settings = runtime_settings or settings
    custom = active_settings.signals_db_path
    path = Path(custom) if custom else _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class SignalStore:
    """SQLite-backed semantic signal mapping store."""

    def __init__(self, db_path: Path | None = None, *, runtime_settings: Settings | None = None):
        self._settings = runtime_settings or settings
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        self._legacy_tenant = configured_tenant if configured_tenant != "*" else None
        self._db_path = db_path or _db_path(self._settings)
        self._transaction_connection: ContextVar[sqlite3.Connection | None] = ContextVar(
            f"signal_transaction_{id(self)}",
            default=None,
        )
        self._pinned_governed_mappings: ContextVar[_PinnedGovernedMappings | None] = ContextVar(
            f"signal_knowledge_pin_{id(self)}",
            default=None,
        )
        self._ensure_schema()

    def activate_pinned_governed_mappings(
        self,
        *,
        tenant_id: str,
        mappings: list[dict[str, Any]],
    ) -> Token[_PinnedGovernedMappings | None]:
        """Use one immutable governed mapping set for the current execution context."""
        pinned = self._validated_pinned_governed_mappings(tenant_id=tenant_id, mappings=mappings)
        return self._pinned_governed_mappings.set(pinned)

    def replace_pinned_governed_mappings(
        self,
        token: Token[_PinnedGovernedMappings | None],
        *,
        tenant_id: str,
        mappings: list[dict[str, Any]],
    ) -> Token[_PinnedGovernedMappings | None]:
        """Replace a context pin after validating the complete staged mapping set."""
        pinned = self._validated_pinned_governed_mappings(tenant_id=tenant_id, mappings=mappings)
        self._pinned_governed_mappings.reset(token)
        return self._pinned_governed_mappings.set(pinned)

    def _validated_pinned_governed_mappings(
        self,
        *,
        tenant_id: str,
        mappings: list[dict[str, Any]],
    ) -> _PinnedGovernedMappings:
        tenant_id = self._resolve_tenant(tenant_id)
        pinned: list[dict[str, Any]] = []
        for mapping in mappings:
            mapping_tenant = str(mapping.get("tenant_id") or "")
            if mapping_tenant != tenant_id:
                raise ValueError("pinned governed mappings cannot cross tenants")
            if not mapping.get("governance_ref") or int(mapping.get("governance_revision") or 0) < 1:
                raise ValueError("pinned governed mappings require an exact knowledge revision")
            pinned.append(dict(mapping))
        return _PinnedGovernedMappings(tenant_id=tenant_id, mappings=tuple(pinned))

    def reset_pinned_governed_mappings(self, token: Token[_PinnedGovernedMappings | None]) -> None:
        """Restore the previous resolver pin for the current execution context."""
        self._pinned_governed_mappings.reset(token)

    @contextmanager
    def _conn(self):
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        conn = sqlite3.connect(str(self._db_path), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        try:
            if str(conn.execute("PRAGMA journal_mode").fetchone()[0]).casefold() != "wal":
                conn.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).casefold():
                conn.close()
                raise
            # Another startup can be switching the database to WAL. The
            # subsequent BEGIN IMMEDIATE is the authoritative serialization.
            logger.info("signal_store_journal_mode_deferred", db_path=str(self._db_path))
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        """Run signal-store writes in one immediate, nestable transaction."""
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            token = self._transaction_connection.set(conn)
            try:
                yield conn
            finally:
                self._transaction_connection.reset(token)

    @contextmanager
    def read_transaction(self):
        """Keep related reads on one SQLite snapshot without taking a writer lock."""
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        with self._conn() as conn:
            conn.execute("BEGIN")
            token = self._transaction_connection.set(conn)
            try:
                yield conn
            finally:
                self._transaction_connection.reset(token)

    def _ensure_schema(self):
        with self._conn() as conn:
            schema_current = signal_schema_is_current(conn)
            audit_current = governed_projection_audit_is_current(conn)
            owner_current = signal_tenant_owner_is_current(conn, legacy_tenant=self._legacy_tenant)
            if schema_current and audit_current and owner_current:
                require_confirmed_default_tenant_owner(conn, legacy_tenant=self._legacy_tenant)
                logger.info(
                    "signal_store_init",
                    db_path=str(self._db_path),
                    schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
                    migration_required=False,
                )
                return
            projection_repair_only = schema_current and owner_current

        if projection_repair_only:
            self._reconcile_governed_projection_audit_batched()
            logger.info(
                "signal_store_init",
                db_path=str(self._db_path),
                schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
                migration_required=True,
                projection_repair_batched=True,
            )
            return

        bootstrap_signal_definitions: dict[str, dict[str, Any]] | None = None
        try:
            import yaml

            resource = files("tacit.data").joinpath("signals.yaml")
            if resource.is_file():
                with resource.open() as stream:
                    bootstrap_data = yaml.safe_load(stream) or {}
                bootstrap_signal_definitions = dict(bootstrap_data.get("signals", {}))
        except Exception as exc:
            logger.warning("signals_bootstrap_taxonomy_unavailable", error=str(exc))
        with self._conn() as conn:
            # Structural rebuilds remain atomic. Potentially large owner and
            # projection reconciliation runs in restartable batches below.
            conn.execute("BEGIN IMMEDIATE")
            if (
                signal_schema_is_current(conn)
                and governed_projection_audit_is_current(conn)
                and signal_tenant_owner_is_current(conn, legacy_tenant=self._legacy_tenant)
            ):
                require_confirmed_default_tenant_owner(conn, legacy_tenant=self._legacy_tenant)
                logger.info(
                    "signal_store_init",
                    db_path=str(self._db_path),
                    schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
                    migration_required=False,
                )
                return
            ensure_schema(
                conn,
                legacy_tenant=self._legacy_tenant,
                bootstrap_signal_definitions=bootstrap_signal_definitions,
            )
        self._reconcile_default_tenant_owner_batched()
        self._reconcile_governed_projection_audit_batched()
        logger.info(
            "signal_store_init",
            db_path=str(self._db_path),
            schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
            migration_required=True,
            projection_repair_batched=True,
        )

    def _reconcile_default_tenant_owner_batched(self) -> None:
        migrated_rows = 0
        batches = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                complete, operation, row_count = reconcile_default_tenant_owner_batch(
                    conn,
                    legacy_tenant=self._legacy_tenant,
                    batch_size=_DEFAULT_OWNER_MIGRATION_BATCH_SIZE,
                )
            migrated_rows += row_count
            if row_count:
                batches += 1
                logger.info(
                    "signal_tenant_owner_migration_batch",
                    operation=operation,
                    rows=row_count,
                    batch=batches,
                )
            if complete:
                if migrated_rows:
                    logger.warning(
                        "signal_tenant_owner_migration_complete",
                        rows=migrated_rows,
                        batches=batches,
                        tenant_id=self._legacy_tenant,
                    )
                return

    @staticmethod
    def _projection_audit_marker_value(connection: sqlite3.Connection) -> str:
        row = connection.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (GOVERNED_PROJECTION_AUDIT_MARKER,),
        ).fetchone()
        return str(row["value"]) if row is not None else ""

    def _repair_projection_authority_batches(self) -> int:
        repaired = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                authority_tables = {str(row[0]) for row in conn.execute("""SELECT name FROM sqlite_master
                           WHERE type='table'
                             AND name IN ('operational_knowledge', 'operational_knowledge_revisions')""")}
                if authority_tables != {"operational_knowledge", "operational_knowledge_revisions"}:
                    return repaired
                rows = conn.execute(
                    """SELECT revision.tenant_id, revision.knowledge_id, revision.revision,
                              revision.content_json
                       FROM operational_knowledge current
                       JOIN operational_knowledge_revisions revision
                         ON revision.tenant_id=current.tenant_id
                        AND revision.knowledge_id=current.knowledge_id
                        AND revision.revision=current.current_revision
                       WHERE current.kind='signal_mapping'
                         AND current.status='active'
                         AND revision.lifecycle_status='active'
                         AND revision.eligibility!='ineligible'
                         AND (current.tenant_id>?
                              OR (current.tenant_id=? AND current.knowledge_id>?))
                       ORDER BY current.tenant_id, current.knowledge_id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _PROJECTION_AUDIT_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    return repaired
                logger.info(
                    "governed_signal_projection_repair_batch",
                    batch_size=len(rows),
                    after_tenant=after_tenant,
                    after_knowledge_id=after_id,
                )
                from tacit.knowledge.models import KnowledgeRevision

                for row in rows:
                    try:
                        revision = KnowledgeRevision.model_validate_json(row["content_json"])
                        result = self.sync_governed_revision(
                            revision,
                            connection=conn,
                            allow_dirty=True,
                        )
                        repaired += int(result["projected"])
                    except ValueError as exc:
                        logger.error(
                            "governed_signal_projection_authority_unrepairable",
                            tenant_id=row["tenant_id"],
                            knowledge_id=row["knowledge_id"],
                            knowledge_revision=row["revision"],
                            error=str(exc),
                        )
                        raise RuntimeError(
                            "active governed signal authority cannot be projected exactly: "
                            f"{row['knowledge_id']}@{row['revision']}"
                        ) from exc
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["knowledge_id"])

    def _quarantine_projection_batches(self) -> tuple[int, int]:
        quarantined = 0
        legacy_quarantined = 0
        quarantine_reasons: dict[str, int] = {}
        quarantine_patterns: set[str] = set()
        after_rowid = 0
        while True:
            with self.transaction() as conn:
                authority_tables = {str(row[0]) for row in conn.execute("""SELECT name FROM sqlite_master
                           WHERE type='table'
                             AND name IN ('operational_knowledge', 'operational_knowledge_revisions')""")}
                if authority_tables == {"operational_knowledge", "operational_knowledge_revisions"}:
                    rows = conn.execute(
                        """SELECT mapping.rowid AS mapping_rowid, mapping.*,
                                  revision.content_json AS authority_content_json,
                                  revision.review_state AS authority_review_state,
                                  revision.lifecycle_status AS authority_lifecycle_status,
                                  revision.eligibility AS authority_eligibility
                           FROM signal_metric_mappings mapping
                           LEFT JOIN operational_knowledge item
                             ON item.tenant_id=mapping.tenant_id
                            AND item.knowledge_id=mapping.governance_ref
                            AND item.current_revision=mapping.governance_revision
                            AND item.status='active'
                           LEFT JOIN operational_knowledge_revisions revision
                             ON revision.tenant_id=item.tenant_id
                            AND revision.knowledge_id=item.knowledge_id
                            AND revision.revision=item.current_revision
                           WHERE mapping.governance_ref!=''
                             AND mapping.review_state IN ('approved', 'trusted')
                             AND mapping.rowid>?
                           ORDER BY mapping.rowid LIMIT ?""",
                        (after_rowid, _PROJECTION_AUDIT_BATCH_SIZE),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """SELECT rowid AS mapping_rowid, *,
                                  NULL AS authority_content_json,
                                  NULL AS authority_review_state,
                                  NULL AS authority_lifecycle_status,
                                  NULL AS authority_eligibility
                           FROM signal_metric_mappings
                           WHERE governance_ref!='' AND review_state IN ('approved', 'trusted')
                             AND rowid>?
                           ORDER BY rowid LIMIT ?""",
                        (after_rowid, _PROJECTION_AUDIT_BATCH_SIZE),
                    ).fetchall()
                if not rows:
                    break
                invalid_ids: list[str] = []
                for row in rows:
                    authority = None
                    if row["authority_content_json"] is not None:
                        authority = {
                            "content_json": row["authority_content_json"],
                            "review_state": row["authority_review_state"],
                            "lifecycle_status": row["authority_lifecycle_status"],
                            "eligibility": row["authority_eligibility"],
                        }
                    valid, reason = projection_matches_authority(row, authority)
                    if valid:
                        continue
                    invalid_ids.append(str(row["id"]))
                    quarantine_reasons[reason] = quarantine_reasons.get(reason, 0) + 1
                    quarantine_patterns.add(str(row["metric_pattern"]))
                if invalid_ids:
                    conn.executemany(
                        "UPDATE signal_metric_mappings SET review_state='candidate' WHERE id=?",
                        [(mapping_id,) for mapping_id in invalid_ids],
                    )
                    quarantined += len(invalid_ids)
                after_rowid = int(rows[-1]["mapping_rowid"])

        after_rowid = 0
        while True:
            with self.transaction() as conn:
                rows = conn.execute(
                    """SELECT rowid AS mapping_rowid, id FROM signal_metric_mappings
                       WHERE source_type!='bootstrap' AND governance_ref=''
                         AND review_state IN ('approved', 'trusted') AND rowid>?
                       ORDER BY rowid LIMIT ?""",
                    (after_rowid, _PROJECTION_AUDIT_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    break
                conn.executemany(
                    "UPDATE signal_metric_mappings SET review_state='candidate' WHERE id=?",
                    [(str(row["id"]),) for row in rows],
                )
                legacy_quarantined += len(rows)
                after_rowid = int(rows[-1]["mapping_rowid"])
        if quarantined:
            logger.warning(
                "governed_signal_mapping_revision_unknown",
                mappings=quarantined,
                quarantined=quarantined,
                reasons=quarantine_reasons,
                sample_patterns=sorted(quarantine_patterns)[:5],
            )
        return quarantined, legacy_quarantined

    def _validated_projection_audit_token(self) -> str | None:
        with self._conn() as conn:
            conn.execute("BEGIN")
            token = self._projection_audit_marker_value(conn)
            if token == "clean":
                return None
            if not token:
                raise RuntimeError("governed signal projection audit was not dirty during repair")
            authority_tables = {str(row[0]) for row in conn.execute("""SELECT name FROM sqlite_master
                       WHERE type='table'
                         AND name IN ('operational_knowledge', 'operational_knowledge_revisions')""")}

        def require_same_token(connection: sqlite3.Connection) -> bool:
            current = self._projection_audit_marker_value(connection)
            if current == "clean":
                return False
            if not current:
                raise RuntimeError("governed signal projection audit marker disappeared during repair")
            if current != token:
                raise _ProjectionAuditChanged
            return True

        if authority_tables == {"operational_knowledge", "operational_knowledge_revisions"}:
            after_tenant = ""
            after_knowledge_id = ""
            while True:
                with self._conn() as conn:
                    conn.execute("BEGIN")
                    if not require_same_token(conn):
                        return None
                    authorities = conn.execute(
                        """SELECT current.tenant_id, current.knowledge_id, current.current_revision,
                                  revision.content_json
                           FROM operational_knowledge current
                           JOIN operational_knowledge_revisions revision
                             ON revision.tenant_id=current.tenant_id
                            AND revision.knowledge_id=current.knowledge_id
                            AND revision.revision=current.current_revision
                           WHERE current.kind='signal_mapping' AND current.status='active'
                             AND revision.lifecycle_status='active'
                             AND revision.eligibility!='ineligible'
                             AND (current.tenant_id>?
                                  OR (current.tenant_id=? AND current.knowledge_id>?))
                           ORDER BY current.tenant_id, current.knowledge_id LIMIT ?""",
                        (
                            after_tenant,
                            after_tenant,
                            after_knowledge_id,
                            _PROJECTION_AUTHORITY_VALIDATION_BATCH_SIZE,
                        ),
                    ).fetchall()
                    if not authorities:
                        break

                    expected_by_authority: dict[tuple[str, str, int], set[tuple[str, str]]] = {}
                    for authority in authorities:
                        key = (
                            str(authority["tenant_id"]),
                            str(authority["knowledge_id"]),
                            int(authority["current_revision"]),
                        )
                        try:
                            content = json.loads(str(authority["content_json"]))
                            signal_type = str(content.get("proposition", {}).get("concept_ref") or "").removeprefix(
                                "signal:"
                            )
                            resolver_mappings = content.get("resolver_payload", {}).get("mappings", [])
                            expected = {
                                (signal_type, str(mapping.get("metric_pattern") or "").strip())
                                for mapping in resolver_mappings
                                if isinstance(mapping, dict) and str(mapping.get("metric_pattern") or "").strip()
                            }
                        except (AttributeError, TypeError, json.JSONDecodeError) as exc:
                            raise RuntimeError(
                                "active governed signal authority has invalid resolver payload: " f"{key[1]}@{key[2]}"
                            ) from exc
                        if not signal_type or not expected:
                            raise RuntimeError(
                                "active governed signal authority has no exact resolver payload: " f"{key[1]}@{key[2]}"
                            )
                        expected_by_authority[key] = expected

                    projected_by_authority: dict[tuple[str, str, int], set[tuple[str, str]]] = {
                        key: set() for key in expected_by_authority
                    }
                    key_clause = " OR ".join(
                        "(tenant_id=? AND governance_ref=? AND governance_revision=?)" for _ in expected_by_authority
                    )
                    key_parameters = [value for key in expected_by_authority for value in key]
                    after_mapping_rowid = 0
                    while True:
                        projections = conn.execute(
                            f"""SELECT rowid AS mapping_rowid, tenant_id, governance_ref,
                                       governance_revision, signal_type, metric_pattern
                                FROM signal_metric_mappings
                                WHERE rowid>? AND review_state IN ('approved', 'trusted')
                                  AND ({key_clause})
                                ORDER BY rowid LIMIT ?""",
                            (after_mapping_rowid, *key_parameters, _PROJECTION_AUDIT_BATCH_SIZE),
                        ).fetchall()
                        if not projections:
                            break
                        for projection in projections:
                            key = (
                                str(projection["tenant_id"]),
                                str(projection["governance_ref"]),
                                int(projection["governance_revision"]),
                            )
                            projected_by_authority[key].add(
                                (str(projection["signal_type"]), str(projection["metric_pattern"]))
                            )
                        after_mapping_rowid = int(projections[-1]["mapping_rowid"])

                    for key, expected in expected_by_authority.items():
                        projected = projected_by_authority[key]
                        if projected != expected:
                            raise RuntimeError(
                                "active governed signal authority has an incomplete resolver projection: "
                                f"{key[1]}@{key[2]} "
                                f"(expected={len(expected)}, projected={len(projected)})"
                            )
                    after_tenant = str(authorities[-1]["tenant_id"])
                    after_knowledge_id = str(authorities[-1]["knowledge_id"])

        after_rowid = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN")
                if not require_same_token(conn):
                    return None
                if authority_tables == {"operational_knowledge", "operational_knowledge_revisions"}:
                    mappings = conn.execute(
                        """SELECT mapping.rowid AS mapping_rowid, mapping.*,
                                  revision.content_json AS authority_content_json,
                                  revision.review_state AS authority_review_state,
                                  revision.lifecycle_status AS authority_lifecycle_status,
                                  revision.eligibility AS authority_eligibility
                           FROM signal_metric_mappings mapping
                           LEFT JOIN operational_knowledge item
                             ON item.tenant_id=mapping.tenant_id
                            AND item.knowledge_id=mapping.governance_ref
                            AND item.current_revision=mapping.governance_revision
                            AND item.status='active'
                           LEFT JOIN operational_knowledge_revisions revision
                             ON revision.tenant_id=item.tenant_id
                            AND revision.knowledge_id=item.knowledge_id
                            AND revision.revision=item.current_revision
                           WHERE mapping.rowid>? AND mapping.review_state IN ('approved', 'trusted')
                             AND (mapping.governance_ref!='' OR mapping.source_type!='bootstrap')
                           ORDER BY mapping.rowid LIMIT ?""",
                        (after_rowid, _PROJECTION_AUDIT_BATCH_SIZE),
                    ).fetchall()
                else:
                    mappings = conn.execute(
                        """SELECT rowid AS mapping_rowid, *,
                                  NULL AS authority_content_json,
                                  NULL AS authority_review_state,
                                  NULL AS authority_lifecycle_status,
                                  NULL AS authority_eligibility
                           FROM signal_metric_mappings
                           WHERE rowid>? AND review_state IN ('approved', 'trusted')
                             AND (governance_ref!='' OR source_type!='bootstrap')
                           ORDER BY rowid LIMIT ?""",
                        (after_rowid, _PROJECTION_AUDIT_BATCH_SIZE),
                    ).fetchall()
                if not mappings:
                    break
                for mapping in mappings:
                    if not mapping["governance_ref"]:
                        raise RuntimeError("active ungoverned signal mapping remains after projection repair")
                    authority = None
                    if mapping["authority_content_json"] is not None:
                        authority = {
                            "content_json": mapping["authority_content_json"],
                            "review_state": mapping["authority_review_state"],
                            "lifecycle_status": mapping["authority_lifecycle_status"],
                            "eligibility": mapping["authority_eligibility"],
                        }
                    valid, reason = projection_matches_authority(mapping, authority)
                    if not valid:
                        raise RuntimeError(
                            f"governed signal projection remains invalid after repair: " f"{mapping['id']} ({reason})"
                        )
                after_rowid = int(mappings[-1]["mapping_rowid"])
        return token

    def _reconcile_governed_projection_audit_batched(self) -> None:
        """Repair a dirty projection audit without one database-wide write lock."""
        with self.transaction() as conn:
            require_confirmed_default_tenant_owner(conn, legacy_tenant=self._legacy_tenant)
            ensure_governed_projection_audit_triggers(conn)
        for attempt in range(1, _PROJECTION_AUDIT_MAX_RETRIES + 1):
            repaired = self._repair_projection_authority_batches()
            quarantined, legacy_quarantined = self._quarantine_projection_batches()
            try:
                token = self._validated_projection_audit_token()
            except _ProjectionAuditChanged:
                logger.warning(
                    "governed_signal_projection_audit_raced",
                    attempt=attempt,
                )
                continue
            if token is None:
                return
            with self.transaction() as conn:
                if self._projection_audit_marker_value(conn) != token:
                    logger.warning(
                        "governed_signal_projection_audit_raced",
                        attempt=attempt,
                    )
                    continue
                mark_governed_projection_audit_current(conn)
            if repaired or quarantined or legacy_quarantined:
                logger.warning(
                    "governed_signal_projection_authority_rebuilt",
                    mappings=repaired,
                    quarantined=quarantined,
                    legacy_quarantined=legacy_quarantined,
                )
            return
        raise RuntimeError("governed signal projection audit changed repeatedly during repair")

    def ensure_governed_projection_audit_current(self) -> None:
        """Repair a dirty authority projection in bounded transactions."""
        with self._conn() as conn:
            if governed_projection_audit_is_current(conn):
                return
        self._reconcile_governed_projection_audit_batched()

    @staticmethod
    def mark_governed_projection_audit_current(connection: sqlite3.Connection) -> None:
        """Mark an authority/projection transaction internally consistent."""
        mark_governed_projection_audit_current(connection)

    @staticmethod
    def governed_projection_audit_is_current(connection: sqlite3.Connection) -> bool:
        """Return whether no unresolved projection mutation predates this transaction."""
        return governed_projection_audit_is_current(connection)

    def reconcile_governed_projection_audit(self, _connection: sqlite3.Connection) -> None:
        """Reject the removed single-transaction projection repair path."""
        raise RuntimeError(
            "single-transaction projection repair is unsupported; " "call ensure_governed_projection_audit_current()"
        )

    def _ensure_learning_index(self, conn: sqlite3.Connection) -> None:
        """Create the FTS5 operational knowledge index when available."""
        ensure_learning_index(conn, legacy_tenant=self._legacy_tenant or "default")

    def _ensure_mapping_columns(self, conn: sqlite3.Connection) -> None:
        """Add newer columns to signal_metric_mappings on pre-existing DBs."""
        ensure_mapping_columns(conn)

    def _ensure_ingested_dashboard_backend_scope(self, conn: sqlite3.Connection) -> None:
        ensure_ingested_dashboard_backend_scope(conn, legacy_tenant=self._legacy_tenant or "default")

    def _ensure_ingested_alert_columns(self, conn: sqlite3.Connection) -> None:
        ensure_ingested_alert_columns(conn)

    def _rebuild_ingested_dashboards_table(self, conn: sqlite3.Connection) -> None:
        rebuild_ingested_dashboards_table(conn, legacy_tenant=self._legacy_tenant or "default")

    def _resolve_tenant(self, tenant_id: str | None) -> str:
        return resolve_tenant_boundary(
            str(self._settings.knowledge_tenant_id or "default"),
            tenant_id,
        )

    # ── Signal type CRUD ─────────────────────────────────────────────────

    def register_signal_type(
        self,
        signal_type: str,
        description: str = "",
        category: str = "",
        unit: str = "",
        *,
        tenant_id: str | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> None:
        """Register a global taxonomy definition or a tenant-specific override."""
        if tenant_id is not None:
            tenant_id = self._resolve_tenant(tenant_id)
        now = time.time()
        connection_context = nullcontext(connection) if connection is not None else self._conn()
        with connection_context as conn:
            assert conn is not None
            if tenant_id is None:
                conn.execute(
                    """INSERT INTO signal_types (signal_type, description, category, unit, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)
                       ON CONFLICT(signal_type) DO UPDATE SET
                           description = CASE WHEN excluded.description != '' THEN excluded.description
                                             ELSE signal_types.description END,
                           category = CASE WHEN excluded.category != '' THEN excluded.category
                                           ELSE signal_types.category END,
                           unit = CASE WHEN excluded.unit != '' THEN excluded.unit ELSE signal_types.unit END,
                           updated_at = excluded.updated_at""",
                    (signal_type, description, category, unit, now, now),
                )
            else:
                conn.execute(
                    """INSERT INTO tenant_signal_types
                       (tenant_id, signal_type, description, category, unit, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(tenant_id, signal_type) DO UPDATE SET
                           description = CASE WHEN excluded.description != '' THEN excluded.description
                                             ELSE tenant_signal_types.description END,
                           category = CASE WHEN excluded.category != '' THEN excluded.category
                                           ELSE tenant_signal_types.category END,
                           unit = CASE WHEN excluded.unit != '' THEN excluded.unit ELSE tenant_signal_types.unit END,
                           updated_at = excluded.updated_at""",
                    (tenant_id, signal_type, description, category, unit, now, now),
                )

    def list_signal_types(self, *, tenant_id: str | None = None) -> list[dict[str, Any]]:
        """List all registered signal types with mapping counts."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            global_rows = conn.execute("SELECT * FROM signal_types").fetchall()
            tenant_rows = conn.execute(
                "SELECT * FROM tenant_signal_types WHERE tenant_id = ?",
                (tenant_id,),
            ).fetchall()
            mapping_rows = conn.execute(
                """SELECT signal_type, COUNT(*) AS mapping_count
                   FROM signal_metric_mappings
                   WHERE tenant_id IN (?, ?)
                   GROUP BY signal_type""",
                (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
            ).fetchall()
        definitions = {row["signal_type"]: dict(row) for row in global_rows}
        for row in tenant_rows:
            signal_type = row["signal_type"]
            merged = _merge_signal_definition(definitions.get(signal_type), dict(row))
            assert merged is not None
            definitions[signal_type] = merged
        counts = {row["signal_type"]: row["mapping_count"] for row in mapping_rows}
        result = []
        for signal_type, definition in definitions.items():
            definition.pop("tenant_id", None)
            definition["mapping_count"] = counts.get(signal_type, 0)
            result.append(definition)
        return sorted(result, key=lambda row: (row["category"], row["signal_type"]))

    def get_signal_type(self, signal_type: str, *, tenant_id: str | None = None) -> dict[str, Any] | None:
        """Get a signal type with all its metric mappings."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            tenant_definition = conn.execute(
                "SELECT * FROM tenant_signal_types WHERE tenant_id = ? AND signal_type = ?",
                (tenant_id, signal_type),
            ).fetchone()
            global_definition = conn.execute(
                "SELECT * FROM signal_types WHERE signal_type = ?",
                (signal_type,),
            ).fetchone()
            st = _merge_signal_definition(
                dict(global_definition) if global_definition is not None else None,
                dict(tenant_definition) if tenant_definition is not None else None,
            )
            if st is None:
                return None

            mappings = conn.execute(
                """SELECT * FROM signal_metric_mappings
                   WHERE signal_type = ?
                     AND tenant_id IN (?, ?)
                   ORDER BY CASE WHEN tenant_id = ? THEN 0 ELSE 1 END, confidence DESC""",
                (signal_type, tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID, tenant_id),
            ).fetchall()

        result = dict(st)
        result.pop("tenant_id", None)
        result["mappings"] = [_deserialize_mapping(r) for r in mappings]
        return result

    def get_signal_type_page(
        self,
        signal_type: str,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any] | None:
        """Return signal metadata plus one tenant-prioritized mapping page."""
        if limit < 1:
            raise ValueError("signal mapping page limit must be positive")
        tenant_id = self._resolve_tenant(tenant_id)
        cursor_clause = ""
        cursor_params: list[Any] = []
        if cursor:
            priority, confidence, mapping_id = _decode_signal_mapping_cursor(cursor)
            cursor_clause = """WHERE (_tenant_priority > ?
                OR (_tenant_priority = ? AND confidence < ?)
                OR (_tenant_priority = ? AND confidence = ? AND id < ?))"""
            cursor_params = [priority, priority, confidence, priority, confidence, mapping_id]
        with self.read_transaction() as conn:
            tenant_definition = conn.execute(
                "SELECT * FROM tenant_signal_types WHERE tenant_id = ? AND signal_type = ?",
                (tenant_id, signal_type),
            ).fetchone()
            global_definition = conn.execute(
                "SELECT * FROM signal_types WHERE signal_type = ?",
                (signal_type,),
            ).fetchone()
            definition = _merge_signal_definition(
                dict(global_definition) if global_definition is not None else None,
                dict(tenant_definition) if tenant_definition is not None else None,
            )
            if definition is None:
                return None
            mapping_count = int(
                conn.execute(
                    """SELECT COUNT(*) FROM signal_metric_mappings
                       WHERE signal_type=? AND tenant_id IN (?, ?)""",
                    (signal_type, tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
                ).fetchone()[0]
            )
            rows = conn.execute(
                f"""WITH scoped AS (
                        SELECT mapping.*, 0 AS _tenant_priority
                        FROM signal_metric_mappings mapping
                        WHERE mapping.tenant_id=? AND mapping.signal_type=?
                        UNION ALL
                        SELECT mapping.*, 1 AS _tenant_priority
                        FROM signal_metric_mappings mapping
                        WHERE mapping.tenant_id=? AND mapping.signal_type=?
                    )
                    SELECT * FROM scoped
                    {cursor_clause}
                    ORDER BY _tenant_priority ASC, confidence DESC, id DESC
                    LIMIT ?""",
                (
                    tenant_id,
                    signal_type,
                    GLOBAL_BOOTSTRAP_TENANT_ID,
                    signal_type,
                    *cursor_params,
                    limit + 1,
                ),
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        mappings = []
        for row in visible:
            mapping = _deserialize_mapping(row)
            mapping.pop("_tenant_priority", None)
            mappings.append(mapping)
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(
                int(last["_tenant_priority"]),
                float(last["confidence"]),
                int(last["id"]),
            )
        result = dict(definition)
        result.pop("tenant_id", None)
        result.update(
            {
                "mapping_count": mapping_count,
                "mappings": mappings,
                "has_more": has_more,
                "next_cursor": next_cursor,
            }
        )
        return result

    # ── Signal ↔ metric mappings ─────────────────────────────────────────

    def add_mapping(
        self,
        signal_type: str,
        metric_pattern: str,
        confidence: float = 0.5,
        *,
        context_services: list[str] | None = None,
        context_datasource_types: list[str] | None = None,
        context_environments: list[str] | None = None,
        context_archetypes: list[str] | None = None,
        context_regions: list[str] | None = None,
        context_clusters: list[str] | None = None,
        context_namespaces: list[str] | None = None,
        context_versions: list[str] | None = None,
        valid_from: float | None = None,
        valid_until: float | None = None,
        source_type: str = "teach",
        source_refs: list[str] | None = None,
        governance_ref: str = "",
        governance_revision: int = 0,
        inference_version: str = "",
        review_state: str = "trusted",
        tenant_id: str | None = None,
        connection: sqlite3.Connection | None = None,
        replace_existing: bool = False,
        increment_use_count: bool = True,
        _bootstrap_write_token: object | None = None,
    ) -> int:
        """Add or update a signal-to-metric mapping. Returns mapping ID.

        ``confidence`` is a 0.0–1.0 score; out-of-range values (e.g. ``90``
        instead of ``0.9``) are rejected here so a single bad write cannot
        dominate resolution / effective-confidence sorting.

        ``inference_version`` records which ruleset produced a heuristic mapping
        (for later invalidate/replay). ``review_state`` is the lifecycle state
        ('candidate' → 'approved' → 'trusted'); on conflict it is preserved
        (re-teaching never downgrades trust).
        """
        if source_type == "bootstrap" and _bootstrap_write_token is not _BOOTSTRAP_WRITE_TOKEN:
            raise PermissionError("global bootstrap mappings may only be written by the packaged catalog loader")
        if source_type != "bootstrap":
            tenant_id = self._resolve_tenant(tenant_id)
        else:
            tenant_id = tenant_id or "default"
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"confidence must be within [0.0, 1.0], got {confidence!r}")
        if governance_ref and governance_revision < 1:
            raise ValueError("governed mappings require a positive governance_revision")
        if not governance_ref and governance_revision:
            raise ValueError("governance_revision requires governance_ref")
        if replace_existing and not governance_ref:
            raise ValueError("replace_existing is reserved for governed mappings")
        now = time.time()
        storage_tenant = GLOBAL_BOOTSTRAP_TENANT_ID if source_type == "bootstrap" else tenant_id
        connection_context = nullcontext(connection) if connection is not None else self._conn()
        with connection_context as conn:
            assert conn is not None
            # Ensure signal type exists
            if source_type == "bootstrap":
                existing = conn.execute(
                    "SELECT 1 FROM signal_types WHERE signal_type = ?",
                    (signal_type,),
                ).fetchone()
            else:
                existing = conn.execute(
                    """SELECT 1 FROM signal_types WHERE signal_type = ?
                       UNION ALL
                       SELECT 1 FROM tenant_signal_types WHERE tenant_id = ? AND signal_type = ?
                       LIMIT 1""",
                    (signal_type, tenant_id, signal_type),
                ).fetchone()
            if existing is None:
                if source_type == "bootstrap":
                    conn.execute(
                        """INSERT INTO signal_types
                           (signal_type, description, category, unit, created_at, updated_at)
                           VALUES (?, '', '', '', ?, ?)""",
                        (signal_type, now, now),
                    )
                else:
                    conn.execute(
                        """INSERT INTO tenant_signal_types
                           (tenant_id, signal_type, description, category, unit, created_at, updated_at)
                           VALUES (?, ?, '', '', '', ?, ?)""",
                        (tenant_id, signal_type, now, now),
                    )

            # Merge context scopes with any existing mapping so re-teaching the
            # same signal/metric for a second service unions rather than
            # replaces. Semantics per dimension:
            #   None  → leave existing unchanged
            #   []    → explicitly clear (make global)
            #   [...] → union with existing
            prior = conn.execute(
                """SELECT context_services, context_datasource_types,
                          context_environments, context_archetypes, context_regions,
                          context_clusters, context_namespaces, context_versions,
                          valid_from, valid_until, source_refs, inference_version, review_state
                    FROM signal_metric_mappings
                    WHERE tenant_id = ? AND signal_type = ? AND metric_pattern = ?
                      AND governance_ref = ?""",
                (storage_tenant, signal_type, metric_pattern, governance_ref),
            ).fetchone()

            def _merge(provided: list[str] | None, existing_json: str | None) -> list[str]:
                if replace_existing:
                    return list(dict.fromkeys(provided or []))
                existing_list = json.loads(existing_json) if existing_json else []
                if provided is None:
                    return existing_list
                if not provided:  # explicit empty list clears the scope
                    return []
                if prior is not None and not existing_list:
                    return []
                merged = list(existing_list)
                for value in provided:
                    if value not in merged:
                        merged.append(value)
                return merged

            services = _merge(context_services, prior["context_services"] if prior else None)
            ds_types = _merge(context_datasource_types, prior["context_datasource_types"] if prior else None)
            environments = _merge(context_environments, prior["context_environments"] if prior else None)
            archetypes = _merge(context_archetypes, prior["context_archetypes"] if prior else None)
            regions = _merge(context_regions, prior["context_regions"] if prior else None)
            clusters = _merge(context_clusters, prior["context_clusters"] if prior else None)
            namespaces = _merge(context_namespaces, prior["context_namespaces"] if prior else None)
            versions = _merge(context_versions, prior["context_versions"] if prior else None)
            merged_valid_from = (
                valid_from if replace_existing or valid_from is not None else (prior["valid_from"] if prior else None)
            )
            merged_valid_until = (
                valid_until
                if replace_existing or valid_until is not None
                else (prior["valid_until"] if prior else None)
            )
            existing_refs = (
                [] if replace_existing else json.loads(prior["source_refs"]) if prior and prior["source_refs"] else []
            )
            refs = list(existing_refs)
            for ref in source_refs or []:
                if ref not in refs:
                    refs.append(ref)
            merged_inference_version = (
                inference_version
                if replace_existing
                else inference_version or (prior["inference_version"] if prior else "")
            )
            merged_review_state = (
                review_state
                if replace_existing
                else _stronger_review_state(prior["review_state"], review_state) if prior else review_state
            )

            cursor = conn.execute(
                """INSERT INTO signal_metric_mappings
                   (tenant_id, signal_type, metric_pattern, confidence,
                    context_services, context_datasource_types,
                    context_environments, context_archetypes, context_regions,
                    context_clusters, context_namespaces, context_versions,
                    valid_from, valid_until,
                    source_type, source_refs, governance_ref, governance_revision, inference_version,
                    review_state, created_at, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant_id, signal_type, metric_pattern, governance_ref) DO UPDATE SET
                       confidence = CASE WHEN ? THEN excluded.confidence
                                         ELSE MAX(excluded.confidence, signal_metric_mappings.confidence) END,
                       governance_revision = excluded.governance_revision,
                       inference_version = excluded.inference_version,
                       review_state = excluded.review_state,
                       -- excluded.context_* already holds the merged scopes.
                       context_services = excluded.context_services,
                       context_datasource_types = excluded.context_datasource_types,
                       context_environments = excluded.context_environments,
                       context_archetypes = excluded.context_archetypes,
                       context_regions = excluded.context_regions,
                       context_clusters = excluded.context_clusters,
                       context_namespaces = excluded.context_namespaces,
                       context_versions = excluded.context_versions,
                       valid_from = excluded.valid_from,
                       valid_until = excluded.valid_until,
                       source_type = CASE
                           WHEN excluded.source_type = 'bootstrap'
                                AND signal_metric_mappings.source_type <> 'bootstrap'
                           THEN signal_metric_mappings.source_type
                           ELSE excluded.source_type
                       END,
                       source_refs = CASE
                           WHEN excluded.source_type = 'bootstrap'
                                AND signal_metric_mappings.source_type <> 'bootstrap'
                           THEN signal_metric_mappings.source_refs
                           ELSE excluded.source_refs
                       END,
                       last_seen = excluded.last_seen,
                       use_count = signal_metric_mappings.use_count + ?""",
                (
                    storage_tenant,
                    signal_type,
                    metric_pattern,
                    confidence,
                    json.dumps(services),
                    json.dumps(ds_types),
                    json.dumps(environments),
                    json.dumps(archetypes),
                    json.dumps(regions),
                    json.dumps(clusters),
                    json.dumps(namespaces),
                    json.dumps(versions),
                    merged_valid_from,
                    merged_valid_until,
                    source_type,
                    json.dumps(refs),
                    governance_ref,
                    governance_revision,
                    merged_inference_version,
                    merged_review_state,
                    now,
                    now,
                    int(replace_existing),
                    int(increment_use_count),
                ),
            )
            return cursor.lastrowid or 0

    def _add_bootstrap_mapping(
        self,
        signal_type: str,
        metric_pattern: str,
        confidence: float = 0.5,
        **kwargs: Any,
    ) -> int:
        """Write one packaged bootstrap mapping through the reserved internal path."""
        kwargs.pop("source_type", None)
        return self.add_mapping(
            signal_type,
            metric_pattern,
            confidence,
            source_type="bootstrap",
            _bootstrap_write_token=_BOOTSTRAP_WRITE_TOKEN,
            **kwargs,
        )

    def record_rejected_candidate(
        self,
        metric: str,
        *,
        signal_family: str = "",
        signal_name: str = "",
        score: float = 0.0,
        margin: float = 0.0,
        why_not: str = "",
        evidence: list[str] | None = None,
        inference_version: str = "",
        dashboard_uid: str = "",
        backend_name: str = "",
        tenant_id: str | None = None,
    ) -> int:
        """Persist an inferred candidate that was NOT auto-taught.

        Rejections are negative training data — they record what the heuristic
        proposed and why it was held back ('low_score'|'low_margin'|
        'single_source_only'), so the ruleset can be tuned/replayed later.
        """
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            cursor = conn.execute(
                """INSERT INTO rejected_signal_candidates
                   (tenant_id, dashboard_uid, backend_name, metric, signal_family, signal_name,
                    score, margin, why_not, evidence, inference_version, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    tenant_id,
                    dashboard_uid,
                    backend_name,
                    metric,
                    signal_family,
                    signal_name,
                    score,
                    margin,
                    why_not,
                    json.dumps(evidence or []),
                    inference_version,
                    time.time(),
                ),
            )
            return cursor.lastrowid or 0

    def list_rejected_candidates(
        self,
        limit: int = 100,
        *,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return recorded rejected candidates (newest first)."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM rejected_signal_candidates
                   WHERE tenant_id=? ORDER BY created_at DESC LIMIT ?""",
                (tenant_id, limit),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("evidence"), str):
                d["evidence"] = json.loads(d["evidence"])
            out.append(d)
        return out

    def record_feedback(
        self,
        signal_type: str,
        metric_pattern: str,
        positive: bool,
        *,
        tenant_id: str | None = None,
    ) -> None:
        """Record positive/negative feedback for a mapping (anti-drift)."""
        tenant_id = self._resolve_tenant(tenant_id)
        col = "positive_feedback" if positive else "negative_feedback"
        with self._conn() as conn:
            conn.execute(
                f"""UPDATE signal_metric_mappings
                    SET {col} = {col} + 1, last_seen = ?
                    WHERE tenant_id = ? AND signal_type = ? AND metric_pattern = ?""",
                (time.time(), tenant_id, signal_type, metric_pattern),
            )

    def set_mapping_review_state(
        self,
        signal_type: str,
        metric_pattern: str,
        review_state: str,
        *,
        tenant_id: str | None = None,
        governance_ref: str = "",
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        """Activate or deactivate a tenant mapping after governance evaluation."""
        tenant_id = self._resolve_tenant(tenant_id)
        connection_context = nullcontext(connection) if connection is not None else self._conn()
        with connection_context as conn:
            assert conn is not None
            updated = conn.execute(
                """UPDATE signal_metric_mappings SET review_state=?, last_seen=?
                   WHERE tenant_id=? AND signal_type=? AND metric_pattern=? AND governance_ref=?""",
                (review_state, time.time(), tenant_id, signal_type, metric_pattern, governance_ref),
            )
            return updated.rowcount == 1

    def deactivate_governed_mappings(
        self,
        *,
        tenant_id: str,
        governance_ref: str,
        connection: sqlite3.Connection | None = None,
    ) -> int:
        """Deactivate every resolver row owned by one governed knowledge item."""
        tenant_id = self._resolve_tenant(tenant_id)
        if not governance_ref:
            raise ValueError("governance_ref is required")
        connection_context = nullcontext(connection) if connection is not None else self._conn()
        with connection_context as conn:
            assert conn is not None
            updated = conn.execute(
                """UPDATE signal_metric_mappings SET review_state='candidate', last_seen=?
                   WHERE tenant_id=? AND governance_ref=?""",
                (time.time(), tenant_id, governance_ref),
            )
            return updated.rowcount

    def sync_governed_revision(
        self,
        revision: Any,
        *,
        connection: sqlite3.Connection,
        allow_dirty: bool = False,
    ) -> dict[str, int | bool]:
        """Project one immutable signal-mapping revision into the resolver index."""
        if revision.proposition.kind.value != "signal_mapping":
            return {"active": False, "deactivated": 0, "projected": 0}
        if not allow_dirty and not self.governed_projection_audit_is_current(connection):
            raise RuntimeError("governed signal projection audit is dirty; reopen the signal store to reconcile it")

        deactivated = self.deactivate_governed_mappings(
            tenant_id=revision.tenant_id,
            governance_ref=revision.knowledge_id,
            connection=connection,
        )
        active = revision.state.lifecycle_status.value == "active" and revision.state.eligibility.value != "ineligible"
        if not active:
            return {"active": False, "deactivated": deactivated, "projected": 0}

        signal_type = revision.proposition.concept_ref.removeprefix("signal:")
        resolver_mappings = revision.resolver_payload.get("mappings", [])
        metric_patterns = sorted(
            {
                str(mapping.get("metric_pattern") or "").strip()
                for mapping in resolver_mappings
                if isinstance(mapping, dict) and str(mapping.get("metric_pattern") or "").strip()
            }
        )
        if not signal_type or not metric_patterns:
            raise ValueError("active governed signal mapping requires a signal and exact metric pattern")

        datasource_types = sorted(
            {
                str(value).strip()
                for mapping in resolver_mappings
                if isinstance(mapping, dict) and isinstance(mapping.get("context_datasource_types", []), list)
                for value in mapping.get("context_datasource_types", [])
                if str(value).strip()
            }
        )
        review_state = (
            revision.state.review_state.value
            if revision.state.review_state.value in {"approved", "trusted"}
            else "candidate"
        )
        for metric_pattern in metric_patterns:
            confidences: list[float] = []
            for mapping in resolver_mappings:
                if not isinstance(mapping, dict) or str(mapping.get("metric_pattern") or "").strip() != metric_pattern:
                    continue
                try:
                    confidence = float(mapping.get("confidence", 0.5))
                except (TypeError, ValueError):
                    confidence = 0.5
                confidences.append(max(0.0, min(1.0, confidence)))
            self.add_mapping(
                signal_type,
                metric_pattern,
                confidence=max(confidences, default=0.5),
                context_services=_projection_scope_values(revision.scope.service_refs, "entity:service:"),
                context_environments=_projection_scope_values(revision.scope.environment_refs, "environment:"),
                context_datasource_types=datasource_types,
                context_archetypes=_projection_scope_values(revision.scope.archetype_refs, "archetype:"),
                context_regions=_projection_scope_values(revision.scope.region_refs, "region:"),
                context_clusters=_projection_scope_values(revision.scope.cluster_refs, "cluster:"),
                context_namespaces=_projection_scope_values(revision.scope.namespace_refs, "namespace:"),
                context_versions=_projection_scope_values(revision.scope.version_constraints, "version:"),
                valid_from=revision.scope.valid_from.timestamp() if revision.scope.valid_from else None,
                valid_until=revision.scope.valid_until.timestamp() if revision.scope.valid_until else None,
                source_type="operational_knowledge",
                source_refs=[f"{revision.knowledge_id}@{revision.revision}", *revision.provenance_refs],
                governance_ref=revision.knowledge_id,
                governance_revision=revision.revision,
                inference_version=f"{revision.policy_id}:{revision.policy_version}",
                review_state=review_state,
                tenant_id=revision.tenant_id,
                connection=connection,
                replace_existing=True,
                increment_use_count=False,
            )
        return {"active": True, "deactivated": deactivated, "projected": len(metric_patterns)}

    def get_mappings_for_signal(
        self,
        signal_type: str,
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        context_environment: str = "",
        include_decayed: bool = False,
        tenant_id: str | None = None,
        knowledge_scope: Any | None = None,
        excluded_knowledge_refs: set[KnowledgeRevisionRef] | None = None,
        resolution_limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Get all metric mappings for a signal, optionally filtered by context.

        Returns mappings sorted by effective confidence (adjusted for decay
        and feedback).
        """
        tenant_id = self._resolve_tenant(tenant_id)
        pinned = self._pinned_governed_mappings.get()
        if pinned is not None and pinned.tenant_id != tenant_id:
            raise ValueError("pinned governed mappings belong to another tenant")
        now = time.time()
        applicable: list[dict[str, Any]] = []

        def consider(mapping: dict[str, Any], *, priority: int) -> None:
            if excluded_knowledge_refs:
                governance_ref = str(mapping.get("governance_ref") or "")
                governance_revision = int(mapping.get("governance_revision") or 0)
                if (
                    governance_ref
                    and governance_revision > 0
                    and KnowledgeRevisionRef(governance_ref, governance_revision) in excluded_knowledge_refs
                ):
                    return

            if not _context_matches(
                mapping,
                context_service,
                context_datasource_type,
                context_archetype,
                context_environment,
            ):
                return
            if mapping.get("governance_ref") and not _governed_scope_matches(
                mapping,
                knowledge_scope=knowledge_scope,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                now=now,
            ):
                return

            effective = _effective_confidence(
                mapping,
                now,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
            )
            trust_effective = _effective_confidence(
                mapping,
                now,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                apply_context_penalty=False,
            )
            if not include_decayed and trust_effective < TRUST_THRESHOLD:
                return

            candidate = dict(mapping)
            candidate["effective_confidence"] = round(effective, 4)
            candidate["_resolution_priority"] = priority
            applicable.append(candidate)
            if resolution_limit is not None and len(applicable) > resolution_limit:
                logger.error(
                    "signal_resolution_mapping_limit_exceeded",
                    tenant_id=tenant_id,
                    signal_type=signal_type,
                    mapping_count=len(applicable),
                    mapping_limit=resolution_limit,
                )
                raise RuntimeError(f"Signal '{signal_type}' has more than {resolution_limit} active mapping candidates")

        if pinned is not None:
            pinned_for_signal = [
                dict(mapping) for mapping in pinned.mappings if str(mapping.get("signal_type") or "") == signal_type
            ]
            for mapping in pinned_for_signal:
                consider(mapping, priority=0)

        if resolution_limit is None:
            scan_limit = None
        else:
            scan_limit = min(
                _SIGNAL_RESOLUTION_MAX_SCAN_LIMIT,
                max(_SIGNAL_RESOLUTION_MIN_SCAN_LIMIT, resolution_limit * _SIGNAL_RESOLUTION_SCAN_MULTIPLIER),
            )
        scanned = len(pinned_for_signal) if pinned is not None else 0
        last_id = 0
        exhausted = False
        with self._conn() as conn:
            while scan_limit is None or scanned < scan_limit:
                page_limit = _SIGNAL_RESOLUTION_PAGE_SIZE
                if scan_limit is not None:
                    page_limit = min(page_limit, scan_limit - scanned)
                if pinned is None:
                    rows = conn.execute(
                        """SELECT * FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id IN (?, ?)
                             AND review_state IN ('approved', 'trusted')
                             AND id > ?
                           ORDER BY id LIMIT ?""",
                        (signal_type, tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID, last_id, page_limit),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        """SELECT * FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id = ?
                             AND source_type = 'bootstrap'
                             AND review_state IN ('approved', 'trusted')
                             AND id > ?
                           ORDER BY id LIMIT ?""",
                        (signal_type, GLOBAL_BOOTSTRAP_TENANT_ID, last_id, page_limit),
                    ).fetchall()
                if not rows:
                    exhausted = True
                    break
                scanned += len(rows)
                last_id = int(rows[-1]["id"])
                for row in rows:
                    mapping = _deserialize_mapping(row)
                    priority = 0 if mapping["tenant_id"] == tenant_id else 1
                    consider(mapping, priority=priority)
                if len(rows) < page_limit:
                    exhausted = True
                    break

            if not exhausted and scan_limit is not None:
                if pinned is None:
                    more = conn.execute(
                        """SELECT 1 FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id IN (?, ?)
                             AND review_state IN ('approved', 'trusted')
                             AND id > ? LIMIT 1""",
                        (signal_type, tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID, last_id),
                    ).fetchone()
                else:
                    more = conn.execute(
                        """SELECT 1 FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id = ?
                             AND source_type = 'bootstrap'
                             AND review_state IN ('approved', 'trusted')
                             AND id > ? LIMIT 1""",
                        (signal_type, GLOBAL_BOOTSTRAP_TENANT_ID, last_id),
                    ).fetchone()
                if more is not None:
                    logger.error(
                        "signal_resolution_mapping_scan_limit_exceeded",
                        tenant_id=tenant_id,
                        signal_type=signal_type,
                        mapping_scan_count=scanned,
                        mapping_scan_limit=scan_limit,
                    )
                    raise RuntimeError(
                        f"Signal '{signal_type}' mapping scan exceeded the {scan_limit}-row safety limit"
                    )

        applicable.sort(
            key=lambda mapping: (
                int(mapping.pop("_resolution_priority")),
                -float(mapping["effective_confidence"]),
                int(mapping.get("id") or 0),
            )
        )
        results: list[dict[str, Any]] = []
        seen_patterns: set[str] = set()
        for mapping in applicable:
            if mapping["metric_pattern"] in seen_patterns:
                continue
            seen_patterns.add(mapping["metric_pattern"])
            results.append(mapping)

        results.sort(key=lambda mapping: mapping["effective_confidence"], reverse=True)
        return results

    # ── Resolution engine ────────────────────────────────────────────────

    def resolve_signal(
        self,
        signal_type: str,
        catalog: list[MetricEntry],
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        context_environment: str = "",
        target_query_language: str = "",
        tenant_id: str | None = None,
        knowledge_scope: Any | None = None,
    ) -> list[tuple[MetricEntry, float]]:
        """Resolve a semantic signal while preserving the established public result shape."""
        return [
            (match.entry, match.confidence)
            for match in self.resolve_signal_details(
                signal_type,
                catalog,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                target_query_language=target_query_language,
                tenant_id=tenant_id,
                knowledge_scope=knowledge_scope,
            )
        ]

    def resolve_signal_details(
        self,
        signal_type: str,
        catalog: list[MetricEntry],
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        context_environment: str = "",
        target_query_language: str = "",
        tenant_id: str | None = None,
        knowledge_scope: Any | None = None,
        excluded_knowledge_refs: set[KnowledgeRevisionRef] | None = None,
    ) -> list[ResolvedSignal]:
        """Resolve a semantic signal to actual metrics from the live catalog.

        Returns a list of (MetricEntry, effective_confidence) sorted by
        confidence, considering:
        - Pattern matching against catalog metric names
        - Context filters (service, datasource, archetype, environment)
        - Confidence decay and feedback adjustment

        ``target_query_language`` restricts matching to catalog entries of that
        query language (e.g. ``promql``). This prevents a learned SignalFx metric
        from being substituted into a PromQL template (or vice versa) when the
        catalog spans multiple backends.

        This is the core algorithm that bridges semantic signals to real metrics.
        """
        tenant_id = self._resolve_tenant(tenant_id)
        mappings = self.get_mappings_for_signal(
            signal_type,
            context_service=context_service,
            context_datasource_type=context_datasource_type,
            context_archetype=context_archetype,
            context_environment=context_environment,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            excluded_knowledge_refs=excluded_knowledge_refs,
            resolution_limit=int(self._settings.signal_resolution_mapping_limit),
        )

        if not mappings:
            return []

        target_lang = target_query_language.lower()
        target_ds = context_datasource_type.lower()
        eligible_catalog = [
            entry
            for entry in catalog
            if (not target_lang or (entry.query_language or "").lower() == target_lang)
            and (not target_ds or _datasource_type_matches(entry.datasource_type, target_ds))
        ]
        catalog_limit = int(self._settings.signal_resolution_catalog_limit)
        if len(eligible_catalog) > catalog_limit:
            logger.error(
                "signal_resolution_catalog_limit_exceeded",
                tenant_id=tenant_id,
                signal_type=signal_type,
                catalog_count=len(eligible_catalog),
                catalog_limit=catalog_limit,
            )
            raise RuntimeError(f"Signal resolution catalog has more than {catalog_limit} eligible metrics")
        matched: list[ResolvedSignal] = []
        seen_metrics: set[tuple[str, str]] = set()

        sig_type = self.get_signal_type(signal_type, tenant_id=tenant_id)

        for mapping in mappings:
            pattern = mapping["metric_pattern"]
            eff_conf = mapping["effective_confidence"]

            for entry in eligible_catalog:
                metric_key = (entry.datasource_uid, entry.name)
                if metric_key in seen_metrics:
                    continue
                if _metric_matches_pattern(entry.name, pattern):
                    adjusted = eff_conf * _metric_metadata_compatibility(signal_type, sig_type or {}, entry)
                    matched.append(
                        ResolvedSignal(
                            entry=entry,
                            confidence=round(adjusted, 4),
                            governance_ref=str(mapping.get("governance_ref") or ""),
                            governance_revision=int(mapping.get("governance_revision") or 0),
                        )
                    )
                    seen_metrics.add(metric_key)

        matched.sort(key=lambda item: item.confidence, reverse=True)
        return matched

    def resolve_signals_for_archetype(
        self,
        signal_bindings: dict[str, str],
        catalog: list[MetricEntry],
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        context_environment: str = "",
        target_query_language: str = "",
        tenant_id: str | None = None,
        knowledge_scope: Any | None = None,
        applied_governance_refs: set[str] | None = None,
        governance_refs_by_default_metric: dict[str, set[str]] | None = None,
        applied_governance_revision_refs: set[KnowledgeRevisionRef] | None = None,
        governance_revision_refs_by_default_metric: dict[str, set[KnowledgeRevisionRef]] | None = None,
    ) -> dict[str, str]:
        """Resolve signal bindings to metric substitutions for archetype compile.

        Parameters
        ----------
        signal_bindings : dict[str, str]
            Maps signal_type → default_metric_name (from archetype YAML).
        catalog : list[MetricEntry]
            Live metric catalog from datasource discovery.
        target_query_language : str
            When set, only catalog metrics of this query language are eligible,
            so substitutions stay within the backend being compiled for.

        Returns
        -------
        dict[str, str]
            Maps default_metric_name → resolved_actual_metric_name.
            Only contains entries where the default metric was NOT found in
            the catalog and a signal-based resolution succeeded.
        """
        tenant_id = self._resolve_tenant(tenant_id)
        target_lang = target_query_language.lower()
        target_ds = context_datasource_type.lower()
        catalog_names = {
            e.name
            for e in catalog
            if (not target_lang or (e.query_language or "").lower() == target_lang)
            and (not target_ds or _datasource_type_matches(e.datasource_type, target_ds))
        }

        substitutions: dict[str, str] = {}

        for signal_type, default_metric in signal_bindings.items():
            # If the default metric exists in the catalog (filtered by target language), no substitution needed
            if default_metric in catalog_names:
                continue

            # Try signal-based resolution
            resolved = self.resolve_signal_details(
                signal_type,
                catalog,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                target_query_language=target_query_language,
                tenant_id=tenant_id,
                knowledge_scope=knowledge_scope,
            )

            if resolved:
                best = resolved[0]
                best_entry = best.entry
                substitutions[default_metric] = best_entry.name
                if applied_governance_refs is not None and best.governance_ref:
                    applied_governance_refs.add(best.governance_ref)
                if governance_refs_by_default_metric is not None and best.governance_ref:
                    governance_refs_by_default_metric.setdefault(default_metric, set()).add(best.governance_ref)
                revision_ref = best.knowledge_revision_ref
                if applied_governance_revision_refs is not None and revision_ref is not None:
                    applied_governance_revision_refs.add(revision_ref)
                if governance_revision_refs_by_default_metric is not None and revision_ref is not None:
                    governance_revision_refs_by_default_metric.setdefault(default_metric, set()).add(revision_ref)
                logger.info(
                    "signal_resolved",
                    signal=signal_type,
                    default_metric=default_metric,
                    resolved_to=best_entry.name,
                    confidence=best.confidence,
                    governance_ref=best.governance_ref,
                    governance_revision=best.governance_revision,
                )

        return substitutions

    # ── Bulk operations ──────────────────────────────────────────────────

    def _load_yaml_data(self, path: Path | None = None) -> tuple[dict[str, Any], str] | tuple[None, None]:
        """Load signal taxonomy from an explicit path or packaged data."""
        import yaml

        if path is not None:
            if not path.is_file():
                return None, None
            with open(path) as f:
                return yaml.safe_load(f) or {}, str(path)

        env_path = os.environ.get("TACIT_SIGNALS_PATH")
        if env_path:
            candidate = Path(env_path)
            if candidate.is_file():
                with open(candidate) as f:
                    return yaml.safe_load(f) or {}, str(candidate)

        candidates = [
            # Local editable overrides for source checkouts and container mounts.
            Path("signals.yaml"),
            Path(__file__).resolve().parent.parent / "signals.yaml",
            # Backward-compatible fallback for older wheel/PyInstaller layouts.
            Path(__file__).resolve().parent / "signals.yaml",
        ]
        for p in candidates:
            if p.is_file():
                with open(p) as f:
                    return yaml.safe_load(f) or {}, str(p)

        resource = files("tacit.data").joinpath("signals.yaml")
        if resource.is_file():
            with resource.open() as f:
                return yaml.safe_load(f) or {}, "package:tacit.data/signals.yaml"
        return None, None

    def load_from_yaml(self, path: Path | None = None, *, only_if_changed: bool = False) -> int:
        """Load bootstrap signal definitions from signals.yaml.

        Returns the number of mappings loaded.
        """
        data, source = self._load_yaml_data(path)
        if data is None:
            logger.info("signals_yaml_not_found")
            return 0

        fingerprint = hashlib.sha256(
            json.dumps(data, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        count = 0
        with self.transaction() as conn:
            if only_if_changed:
                existing = conn.execute(
                    "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
                    (_BOOTSTRAP_FINGERPRINT_KEY,),
                ).fetchone()
                if existing is not None and str(existing["value"]) == fingerprint:
                    logger.info("signals_yaml_unchanged", path=source, fingerprint=fingerprint)
                    return 0

            for sig_type, sig_def in data.get("signals", {}).items():
                self.register_signal_type(
                    signal_type=sig_type,
                    description=sig_def.get("description", ""),
                    category=sig_def.get("category", ""),
                    unit=sig_def.get("unit", ""),
                )
                for mp in sig_def.get("metric_patterns", []):
                    self._add_bootstrap_mapping(
                        signal_type=sig_type,
                        metric_pattern=mp["pattern"],
                        confidence=mp.get("confidence", 0.5),
                        context_datasource_types=mp["datasource_types"] if "datasource_types" in mp else None,
                    )
                    count += 1
            conn.execute(
                """INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (_BOOTSTRAP_FINGERPRINT_KEY, fingerprint, time.time()),
            )

        logger.info("signals_loaded_from_yaml", path=source, mappings=count, fingerprint=fingerprint)
        return count

    # ── Ingested dashboard records ───────────────────────────────────────

    def record_ingested_dashboard(
        self,
        dashboard_uid: str,
        *,
        tenant_id: str | None = None,
        backend_name: str = "",
        dashboard_title: str = "",
        dashboard_tags: list[str] | None = None,
        metrics_found: list[str] | None = None,
        panel_count: int = 0,
        row_groups: list[dict] | None = None,
        metric_cooccurrence: dict[str, list[str]] | None = None,
        aggregation_patterns: list[dict] | None = None,
        query_transformations: list[str] | None = None,
        panel_titles: list[str] | None = None,
        alert_links: list[str] | None = None,
        drilldown_links: list[str] | None = None,
        signals_inferred: list[str] | list[dict] | None = None,
        archetype_generated: str = "",
        status: str = "pending",
    ) -> str:
        """Record features while preserving terminal review for unchanged content."""
        tenant_id = self._resolve_tenant(tenant_id)
        now = time.time()
        values = {
            "dashboard_title": dashboard_title,
            "dashboard_tags": dashboard_tags or [],
            "metrics_found": metrics_found or [],
            "panel_count": panel_count,
            "row_groups": row_groups or [],
            "metric_cooccurrence": metric_cooccurrence or {},
            "aggregation_patterns": aggregation_patterns or [],
            "query_transformations": query_transformations or [],
            "panel_titles": panel_titles or [],
            "alert_links": alert_links or [],
            "drilldown_links": drilldown_links or [],
            "signals_inferred": signals_inferred or [],
            "archetype_generated": archetype_generated,
        }
        source_content_fields = (
            "dashboard_tags",
            "metrics_found",
            "panel_count",
            "row_groups",
            "metric_cooccurrence",
            "aggregation_patterns",
            "query_transformations",
            "panel_titles",
            "alert_links",
            "drilldown_links",
        )
        generation_fields = ("dashboard_title", *source_content_fields, "signals_inferred", "archetype_generated")
        with self.transaction() as conn:
            existing_row = conn.execute(
                """SELECT * FROM ingested_dashboards
                   WHERE tenant_id = ? AND dashboard_uid = ? AND backend_name = ?""",
                (tenant_id, dashboard_uid, backend_name),
            ).fetchone()
            if existing_row is None:
                conn.execute(
                    """INSERT INTO ingested_dashboards
                       (tenant_id, dashboard_uid, backend_name, dashboard_title, dashboard_tags,
                        metrics_found, panel_count, row_groups, metric_cooccurrence,
                        aggregation_patterns, query_transformations, panel_titles,
                        alert_links, drilldown_links, status, signals_inferred,
                        archetype_generated, stale, missing_since, knowledge_reconciled_at,
                        last_seen_at, created_at, reviewed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, NULL)""",
                    (
                        tenant_id,
                        dashboard_uid,
                        backend_name,
                        dashboard_title,
                        json.dumps(values["dashboard_tags"]),
                        json.dumps(values["metrics_found"]),
                        panel_count,
                        json.dumps(values["row_groups"]),
                        json.dumps(values["metric_cooccurrence"]),
                        json.dumps(values["aggregation_patterns"]),
                        json.dumps(values["query_transformations"]),
                        json.dumps(values["panel_titles"]),
                        json.dumps(values["alert_links"]),
                        json.dumps(values["drilldown_links"]),
                        status,
                        json.dumps(values["signals_inferred"]),
                        archetype_generated,
                        now,
                        now,
                    ),
                )
                return "created"

            existing = _deserialize_ingested(existing_row)
            source_unchanged = not existing.get("stale") and all(
                existing.get(field) == values[field] for field in source_content_fields
            )
            generation_unchanged = source_unchanged and all(
                existing.get(field) == values[field] for field in generation_fields
            )
            existing_status = str(existing.get("status") or "pending")
            if existing_status == "approving" and not generation_unchanged:
                raise RuntimeError("dashboard approval is in progress; retry ingestion after it completes")
            if generation_unchanged and existing_status in {"approved", "rejected", "ignored"}:
                effective_status = existing_status
            elif generation_unchanged and existing_status == "approving":
                effective_status = existing_status
            else:
                effective_status = status
            previous_generation = float(existing["created_at"])
            generation = previous_generation if generation_unchanged else max(now, previous_generation + 0.000001)
            reviewed_at = existing.get("reviewed_at") if effective_status == existing_status else None
            conn.execute(
                """UPDATE ingested_dashboards
                   SET dashboard_title = ?, dashboard_tags = ?, metrics_found = ?,
                       panel_count = ?, row_groups = ?, metric_cooccurrence = ?,
                       aggregation_patterns = ?, query_transformations = ?, panel_titles = ?,
                       alert_links = ?, drilldown_links = ?, status = ?, signals_inferred = ?,
                       archetype_generated = ?, stale = 0, missing_since = NULL,
                       knowledge_reconciled_at = NULL, last_seen_at = ?,
                       created_at = ?, reviewed_at = ?
                   WHERE id = ?""",
                (
                    dashboard_title,
                    json.dumps(values["dashboard_tags"]),
                    json.dumps(values["metrics_found"]),
                    panel_count,
                    json.dumps(values["row_groups"]),
                    json.dumps(values["metric_cooccurrence"]),
                    json.dumps(values["aggregation_patterns"]),
                    json.dumps(values["query_transformations"]),
                    json.dumps(values["panel_titles"]),
                    json.dumps(values["alert_links"]),
                    json.dumps(values["drilldown_links"]),
                    effective_status,
                    json.dumps(values["signals_inferred"]),
                    archetype_generated,
                    now,
                    generation,
                    reviewed_at,
                    existing["id"],
                ),
            )
        return "skipped" if generation_unchanged else "updated"

    def index_dashboard_context(
        self,
        *,
        tenant_id: str | None = None,
        dashboard_uid: str,
        backend_name: str = "",
        dashboard_title: str = "",
        dashboard_tags: list[str] | None = None,
        panels: list[dict[str, Any]] | None = None,
        metrics_found: list[str] | None = None,
        signals_inferred: list[dict[str, Any]] | list[str] | None = None,
        status: str = "pending",
        activated_pairs: set[tuple[str, str]] | None = None,
    ) -> int:
        """Index learned dashboard context for fast operational-language retrieval.

        The index is intentionally a retrieval aid, not the trust source of
        truth. Mapping approval still lives in ``signal_metric_mappings`` and
        dashboard review state still lives in ``ingested_dashboards``.
        """
        tenant_id = self._resolve_tenant(tenant_id)
        if not self._learning_index_available():
            return 0

        rows = build_learning_context_rows(
            dashboard_uid=dashboard_uid,
            backend_name=backend_name,
            dashboard_title=dashboard_title,
            dashboard_tags=dashboard_tags or [],
            panels=panels or [],
            metrics_found=metrics_found or [],
            signals_inferred=signals_inferred or [],
            status=status,
            activated_pairs=activated_pairs,
        )

        try:
            with self._conn() as conn:
                conn.execute(
                    """DELETE FROM learning_context_fts
                       WHERE tenant_id = ? AND source_kind = 'dashboard_panel'
                         AND dashboard_uid = ? AND backend_name = ?""",
                    (tenant_id, dashboard_uid, backend_name),
                )
                conn.executemany(
                    """INSERT INTO learning_context_fts
                       (tenant_id, source_kind, source_id, backend_name, dashboard_uid,
                        dashboard_title, dashboard_tags, panel_title, metric_name,
                        query_text, service, signal_type, review_state, reason,
                        provenance, indexed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(tenant_id, *row) for row in rows],
                )
        except sqlite3.OperationalError as exc:
            logger.warning("learning_context_index_failed", error=str(exc))
            return 0
        return len(rows)

    def record_ingested_alert(
        self,
        alert_uid: str,
        *,
        tenant_id: str | None = None,
        backend_name: str = "",
        source_vendor: str = "",
        source_instance: str = "",
        external_id: str = "",
        fingerprint: str = "",
        alert_title: str = "",
        alert_tags: list[str] | None = None,
        condition: str = "",
        severity: str = "",
        enabled: bool = True,
        labels: dict[str, str] | None = None,
        annotations: dict[str, str] | None = None,
        metrics_found: list[str] | None = None,
        query_transformations: list[str] | None = None,
        service_hints: list[str] | None = None,
        dashboard_uid: str = "",
        panel_title: str = "",
        source_url: str = "",
        provenance_url: str = "",
        confidence: float = 0.0,
        signals_inferred: list[str] | list[dict] | None = None,
        status: str = "pending",
    ) -> str:
        """Record features extracted from an ingested alert rule/detector.

        Returns ``created``, ``updated``, or ``skipped``.
        """
        tenant_id = self._resolve_tenant(tenant_id)
        now = time.time()
        serialized_signals = json.dumps(signals_inferred or [], sort_keys=True)
        generation_fingerprint = hashlib.sha256(
            json.dumps(
                {"source_fingerprint": fingerprint, "signals_inferred": json.loads(serialized_signals)},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        # The approval claim and source generation share this row. Acquire the
        # writer lock before reading so a concurrent claim cannot be overwritten
        # by a stale re-ingestion decision.
        with self.transaction() as conn:
            existing = conn.execute(
                """SELECT id, fingerprint, generation_fingerprint, first_seen_at, status, stale
                   FROM ingested_alerts
                   WHERE tenant_id = ? AND alert_uid = ? AND backend_name = ?""",
                (tenant_id, alert_uid, backend_name),
            ).fetchone()
            first_seen = existing["first_seen_at"] if existing and existing["first_seen_at"] else now
            change_state = "created"
            if existing is not None:
                change_state = "skipped" if existing["generation_fingerprint"] == generation_fingerprint else "updated"
                if existing["status"] == "approving" and change_state != "skipped":
                    raise RuntimeError("alert approval is in progress; retry ingestion after it completes")
            conn.execute(
                """INSERT INTO ingested_alerts
                   (tenant_id, alert_uid, backend_name, source_vendor, source_instance,
                    external_id, fingerprint, generation_fingerprint, alert_title, alert_tags,
                    condition, severity, enabled, labels, annotations,
                    metrics_found, query_transformations, service_hints,
                    dashboard_uid, panel_title, source_url, provenance_url,
                    confidence, stale, missing_since, knowledge_reconciled_at,
                    status, signals_inferred, first_seen_at,
                    last_seen_at, updated_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant_id, alert_uid, backend_name) DO UPDATE SET
                       source_vendor = excluded.source_vendor,
                       source_instance = excluded.source_instance,
                       external_id = excluded.external_id,
                       fingerprint = excluded.fingerprint,
                       generation_fingerprint = excluded.generation_fingerprint,
                       alert_title = excluded.alert_title,
                       alert_tags = excluded.alert_tags,
                       condition = excluded.condition,
                       severity = excluded.severity,
                       enabled = excluded.enabled,
                       labels = excluded.labels,
                       annotations = excluded.annotations,
                       metrics_found = excluded.metrics_found,
                       query_transformations = excluded.query_transformations,
                       service_hints = excluded.service_hints,
                       dashboard_uid = excluded.dashboard_uid,
                       panel_title = excluded.panel_title,
                       source_url = excluded.source_url,
                       provenance_url = excluded.provenance_url,
                       confidence = excluded.confidence,
                       stale = 0,
                       missing_since = NULL,
                       knowledge_reconciled_at = NULL,
                       status = CASE
                           WHEN ingested_alerts.generation_fingerprint = excluded.generation_fingerprint
                                AND ingested_alerts.stale = 0
                                AND excluded.status != 'approved' THEN ingested_alerts.status
                           ELSE excluded.status
                       END,
                       signals_inferred = excluded.signals_inferred,
                       first_seen_at = ingested_alerts.first_seen_at,
                       last_seen_at = excluded.last_seen_at,
                       updated_at = CASE
                           WHEN ingested_alerts.generation_fingerprint = excluded.generation_fingerprint
                               THEN ingested_alerts.updated_at
                           ELSE excluded.updated_at
                       END,
                       created_at = ingested_alerts.created_at""",
                (
                    tenant_id,
                    alert_uid,
                    backend_name,
                    source_vendor or backend_name,
                    source_instance,
                    external_id or alert_uid,
                    fingerprint,
                    generation_fingerprint,
                    alert_title,
                    json.dumps(alert_tags or []),
                    condition,
                    severity,
                    1 if enabled else 0,
                    json.dumps(labels or {}),
                    json.dumps(annotations or {}),
                    json.dumps(metrics_found or []),
                    json.dumps(query_transformations or []),
                    json.dumps(service_hints or []),
                    dashboard_uid,
                    panel_title,
                    source_url,
                    provenance_url or source_url,
                    confidence,
                    0,
                    None,
                    None,
                    status,
                    serialized_signals,
                    first_seen,
                    now,
                    now,
                    now,
                ),
            )
        return change_state

    def record_learned_artifact(
        self,
        *,
        tenant_id: str | None = None,
        artifact_id: str,
        artifact_type: str,
        source_vendor: str = "",
        source_instance: str = "",
        external_id: str = "",
        title: str = "",
        body_text: str = "",
        provenance_url: str = "",
        fingerprint: str = "",
    ) -> str:
        """Record a learned operational artifact lifecycle row.

        Returns ``created``, ``updated``, ``skipped``, or ``restored``.
        """
        tenant_id = self._resolve_tenant(tenant_id)
        now = time.time()
        with self._conn() as conn:
            existing = conn.execute(
                """SELECT fingerprint, first_seen_at, stale FROM learned_artifacts
                   WHERE tenant_id = ? AND artifact_id = ?""",
                (tenant_id, artifact_id),
            ).fetchone()
            first_seen = existing["first_seen_at"] if existing and existing["first_seen_at"] else now
            change_state = "created"
            if existing is not None:
                same_fingerprint = bool(fingerprint and existing["fingerprint"] == fingerprint)
                if same_fingerprint and existing["stale"]:
                    change_state = "restored"
                else:
                    change_state = "skipped" if same_fingerprint else "updated"
            conn.execute(
                """INSERT INTO learned_artifacts
                   (tenant_id, artifact_id, artifact_type, source_vendor, source_instance,
                    external_id, title, body_text, provenance_url, fingerprint,
                    stale, missing_since, knowledge_reconciled_at,
                    first_seen_at, last_seen_at, updated_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, NULL, ?, ?, ?, ?)
                   ON CONFLICT(tenant_id, artifact_id) DO UPDATE SET
                       artifact_type = excluded.artifact_type,
                       source_vendor = excluded.source_vendor,
                       source_instance = excluded.source_instance,
                       external_id = excluded.external_id,
                       title = excluded.title,
                       body_text = excluded.body_text,
                       provenance_url = excluded.provenance_url,
                       fingerprint = excluded.fingerprint,
                       stale = 0,
                       missing_since = NULL,
                       knowledge_reconciled_at = NULL,
                       first_seen_at = learned_artifacts.first_seen_at,
                       last_seen_at = excluded.last_seen_at,
                       updated_at = CASE
                           WHEN learned_artifacts.fingerprint = excluded.fingerprint THEN learned_artifacts.updated_at
                           ELSE excluded.updated_at
                       END,
                       created_at = learned_artifacts.created_at""",
                (
                    tenant_id,
                    artifact_id,
                    artifact_type,
                    source_vendor,
                    source_instance,
                    external_id,
                    title,
                    body_text,
                    provenance_url,
                    fingerprint,
                    first_seen,
                    now,
                    now,
                    now,
                ),
            )
        return change_state

    def replace_artifact_extractions(
        self,
        *,
        tenant_id: str | None = None,
        artifact_id: str,
        evidence_requirements: list[dict[str, Any]] | None = None,
        ownership_hints: list[dict[str, Any]] | None = None,
        dependency_hints: list[dict[str, Any]] | None = None,
        signal_mapping_candidates: list[dict[str, Any]] | None = None,
    ) -> dict[str, int]:
        """Replace extracted IR rows for one artifact."""
        tenant_id = self._resolve_tenant(tenant_id)
        now = time.time()
        extraction_generation = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM evidence_requirements WHERE tenant_id = ? AND artifact_id = ?",
                (tenant_id, artifact_id),
            )
            conn.execute(
                "DELETE FROM ownership_hints WHERE tenant_id = ? AND artifact_id = ?",
                (tenant_id, artifact_id),
            )
            conn.execute(
                "DELETE FROM dependency_hints WHERE tenant_id = ? AND artifact_id = ?",
                (tenant_id, artifact_id),
            )
            conn.execute(
                "DELETE FROM signal_mapping_candidates WHERE tenant_id = ? AND artifact_id = ?",
                (tenant_id, artifact_id),
            )
            for row in evidence_requirements or []:
                conn.execute(
                    """INSERT INTO evidence_requirements
                       (tenant_id, id, artifact_id, subject, evidence_kind, target_entity,
                        signal_hint, query_hint, priority, source_artifact_id,
                        source_excerpt, source_type, confidence_prior, review_state,
                        observation_state, extraction_hash, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        tenant_id,
                        row["id"],
                        artifact_id,
                        row.get("subject", ""),
                        row.get("evidence_kind", ""),
                        row.get("target_entity"),
                        row.get("signal_hint"),
                        row.get("query_hint"),
                        row.get("priority"),
                        row.get("source_artifact_id", artifact_id),
                        row.get("source_excerpt", ""),
                        row.get("source_type", ""),
                        row.get("confidence_prior", 0.5),
                        row.get("review_state", "candidate"),
                        row.get("observation_state", "indeterminate"),
                        row.get("extraction_hash", ""),
                        now,
                    ),
                )
            for row in ownership_hints or []:
                conn.execute(
                    """INSERT INTO ownership_hints
                       (tenant_id, id, artifact_id, entity, owner, hint_kind, source_artifact_id,
                        source_excerpt, source_type, confidence_prior, review_state,
                        extraction_hash, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        tenant_id,
                        row["id"],
                        artifact_id,
                        row.get("entity", ""),
                        row.get("owner", ""),
                        row.get("hint_kind", ""),
                        row.get("source_artifact_id", artifact_id),
                        row.get("source_excerpt", ""),
                        row.get("source_type", ""),
                        row.get("confidence_prior", 0.5),
                        row.get("review_state", "candidate"),
                        row.get("extraction_hash", ""),
                        now,
                    ),
                )
            for row in dependency_hints or []:
                conn.execute(
                    """INSERT INTO dependency_hints
                       (tenant_id, id, artifact_id, source_entity, target_entity, direction,
                        source_artifact_id, source_excerpt, source_type, confidence_prior,
                        review_state, extraction_hash, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        tenant_id,
                        row["id"],
                        artifact_id,
                        row.get("source_entity", ""),
                        row.get("target_entity", ""),
                        row.get("direction", "unknown"),
                        row.get("source_artifact_id", artifact_id),
                        row.get("source_excerpt", ""),
                        row.get("source_type", ""),
                        row.get("confidence_prior", 0.5),
                        row.get("review_state", "candidate"),
                        row.get("extraction_hash", ""),
                        now,
                    ),
                )
            for row in signal_mapping_candidates or []:
                conn.execute(
                    """INSERT INTO signal_mapping_candidates
                       (tenant_id, id, artifact_id, source, candidate_metric, symptom, signal_type,
                        source_artifact_id, source_excerpt, query_hint, confidence_prior,
                        review_state, extraction_hash, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        tenant_id,
                        row["id"],
                        artifact_id,
                        row.get("source", ""),
                        row.get("candidate_metric", ""),
                        row.get("symptom", ""),
                        row.get("signal_type", ""),
                        row.get("source_artifact_id", artifact_id),
                        row.get("source_excerpt", ""),
                        row.get("query_hint"),
                        row.get("confidence_prior", 0.5),
                        row.get("review_state", "candidate"),
                        row.get("extraction_hash", ""),
                        now,
                    ),
                )
            generation_update = conn.execute(
                """UPDATE learned_artifacts SET extraction_generation=?
                   WHERE tenant_id=? AND artifact_id=?""",
                (extraction_generation, tenant_id, artifact_id),
            )
            if generation_update.rowcount != 1:
                raise ValueError("artifact extraction replacement requires a persisted source")
        return {
            "evidence_requirements": len(evidence_requirements or []),
            "ownership_hints": len(ownership_hints or []),
            "dependency_hints": len(dependency_hints or []),
            "signal_mapping_candidates": len(signal_mapping_candidates or []),
        }

    def index_artifact_context(
        self,
        *,
        tenant_id: str | None = None,
        artifact_id: str,
        artifact_type: str,
        title: str = "",
        body_text: str = "",
        evidence_requirements: list[dict[str, Any]] | None = None,
        ownership_hints: list[dict[str, Any]] | None = None,
        dependency_hints: list[dict[str, Any]] | None = None,
        signal_mapping_candidates: list[dict[str, Any]] | None = None,
    ) -> int:
        """Index learned artifact context for retrieval when FTS5 is available."""
        tenant_id = self._resolve_tenant(tenant_id)
        if not self._learning_index_available():
            return 0
        indexed_at = time.time()
        rows: list[tuple[Any, ...]] = []
        for req in evidence_requirements or []:
            rows.append(
                (
                    artifact_type,
                    artifact_id,
                    artifact_type,
                    artifact_id,
                    title,
                    artifact_type,
                    req.get("evidence_kind", ""),
                    req.get("signal_hint", ""),
                    req.get("query_hint", "") or req.get("source_excerpt", ""),
                    req.get("target_entity") or req.get("subject", ""),
                    req.get("evidence_kind", ""),
                    req.get("review_state", "candidate"),
                    req.get("source_excerpt", ""),
                    f"artifact:{artifact_id} type:evidence_requirement",
                    indexed_at,
                )
            )
        for hint in ownership_hints or []:
            rows.append(
                (
                    artifact_type,
                    artifact_id,
                    artifact_type,
                    artifact_id,
                    title,
                    artifact_type,
                    hint.get("hint_kind", ""),
                    "",
                    hint.get("source_excerpt", ""),
                    hint.get("entity", ""),
                    "ownership",
                    hint.get("review_state", "candidate"),
                    hint.get("source_excerpt", ""),
                    f"artifact:{artifact_id} type:ownership_hint owner:{hint.get('owner', '')}",
                    indexed_at,
                )
            )
        for hint in dependency_hints or []:
            service_key = " ".join(
                part for part in [hint.get("source_entity", ""), hint.get("target_entity", "")] if part
            )
            rows.append(
                (
                    artifact_type,
                    artifact_id,
                    artifact_type,
                    artifact_id,
                    title,
                    artifact_type,
                    hint.get("direction", ""),
                    "",
                    hint.get("source_excerpt", ""),
                    service_key,
                    "dependency",
                    hint.get("review_state", "candidate"),
                    hint.get("source_excerpt", ""),
                    f"artifact:{artifact_id} type:dependency_hint target:{hint.get('target_entity', '')}",
                    indexed_at,
                )
            )
        for candidate in signal_mapping_candidates or []:
            rows.append(
                (
                    artifact_type,
                    artifact_id,
                    artifact_type,
                    artifact_id,
                    title,
                    artifact_type,
                    "signal_mapping_candidate",
                    candidate.get("candidate_metric", ""),
                    candidate.get("query_hint", "") or candidate.get("source_excerpt", ""),
                    candidate.get("symptom", ""),
                    candidate.get("signal_type", ""),
                    candidate.get("review_state", "candidate"),
                    candidate.get("source_excerpt", ""),
                    f"artifact:{artifact_id} type:signal_mapping_candidate",
                    indexed_at,
                )
            )
        if body_text:
            rows.append(
                (
                    artifact_type,
                    artifact_id,
                    artifact_type,
                    artifact_id,
                    title,
                    artifact_type,
                    "artifact_text",
                    "",
                    body_text[:2000],
                    "",
                    "",
                    "candidate",
                    body_text[:500],
                    f"artifact:{artifact_id} type:text",
                    indexed_at,
                )
            )
        try:
            with self._conn() as conn:
                conn.execute(
                    """DELETE FROM learning_context_fts
                       WHERE tenant_id = ? AND source_kind = ? AND source_id = ?""",
                    (tenant_id, artifact_type, artifact_id),
                )
                conn.executemany(
                    """INSERT INTO learning_context_fts
                       (tenant_id, source_kind, source_id, backend_name, dashboard_uid,
                        dashboard_title, dashboard_tags, panel_title, metric_name,
                        query_text, service, signal_type, review_state, reason,
                        provenance, indexed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(tenant_id, *row) for row in rows],
                )
        except sqlite3.OperationalError as exc:
            logger.warning("artifact_context_index_failed", error=str(exc))
            return 0
        return len(rows)

    def mark_missing_artifacts_stale(
        self,
        *,
        tenant_id: str | None = None,
        artifact_type: str,
        seen_artifact_ids: set[str],
        source_vendor: str | None = None,
        source_instance: str | None = None,
        external_id_prefix: str | None = None,
        crawl_started_at: float | None = None,
    ) -> int:
        """Mark previously learned artifacts stale when absent from a complete crawl."""
        tenant_id = self._resolve_tenant(tenant_id)
        crawl_started_at = crawl_started_at if crawl_started_at is not None else time.time()
        marked_at = time.time()
        after_id = 0
        total = 0
        while True:
            with self.transaction() as conn:
                clauses = [
                    "tenant_id = ?",
                    "artifact_type = ?",
                    "stale = 0",
                    "id > ?",
                    "last_seen_at <= ?",
                ]
                params: list[Any] = [tenant_id, artifact_type, after_id, crawl_started_at]
                if source_vendor is not None:
                    clauses.append("source_vendor = ?")
                    params.append(source_vendor)
                if source_instance is not None:
                    clauses.append("source_instance = ?")
                    params.append(source_instance)
                if external_id_prefix is not None:
                    clauses.append("external_id LIKE ? ESCAPE '\\'")
                    params.append(f"{_escape_like_prefix(external_id_prefix)}%")
                rows = conn.execute(
                    f"""SELECT id, artifact_id, last_seen_at FROM learned_artifacts
                        WHERE {" AND ".join(clauses)}
                        ORDER BY id LIMIT ?""",
                    (*params, _STALE_SOURCE_PAGE_SIZE),
                ).fetchall()
                if not rows:
                    break
                after_id = int(rows[-1]["id"])
                stale_refs: set[str] = set()
                for row in rows:
                    artifact_id = str(row["artifact_id"])
                    if artifact_id in seen_artifact_ids:
                        continue
                    cursor = conn.execute(
                        """UPDATE learned_artifacts
                           SET stale=1, missing_since=COALESCE(missing_since, ?),
                               knowledge_reconciled_at=NULL, updated_at=?
                           WHERE tenant_id=? AND id=? AND stale=0 AND last_seen_at=?""",
                        (marked_at, marked_at, tenant_id, row["id"], row["last_seen_at"]),
                    )
                    if cursor.rowcount == 1:
                        stale_refs.add(artifact_id)
                if not stale_refs:
                    continue
                total += len(stale_refs)
                placeholders = ", ".join("?" for _ in stale_refs)
                if self._learning_index_available():
                    try:
                        conn.execute(
                            f"""DELETE FROM learning_context_fts
                                WHERE tenant_id=? AND source_kind=?
                                  AND source_id IN ({placeholders})""",
                            (tenant_id, artifact_type, *sorted(stale_refs)),
                        )
                    except sqlite3.OperationalError as exc:
                        logger.warning("stale_artifact_context_update_failed", error=str(exc))
                self._remove_mapping_source_refs(
                    conn,
                    tenant_id=tenant_id,
                    source_type=artifact_type,
                    stale_refs=stale_refs,
                )
        return total

    def list_learned_artifacts(
        self,
        *,
        tenant_id: str | None = None,
        artifact_type: str | None = None,
        stale: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List learned operational artifacts."""
        tenant_id = self._resolve_tenant(tenant_id)
        conditions = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if artifact_type:
            conditions.append("artifact_type = ?")
            params.append(artifact_type)
        if stale is not None:
            conditions.append("stale = ?")
            params.append(int(stale))
        params.extend([limit, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT id, tenant_id, artifact_id, artifact_type, source_vendor, source_instance,
                           external_id, title, provenance_url, fingerprint, extraction_generation,
                           stale, missing_since, first_seen_at, last_seen_at,
                           updated_at, created_at
                    FROM learned_artifacts
                    WHERE {" AND ".join(conditions)}
                    ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        return [_deserialize_learned_artifact(row) for row in rows]

    def list_learned_artifacts_page(
        self,
        *,
        tenant_id: str | None = None,
        artifact_type: str | None = None,
        stale: bool | None = None,
        limit: int = 50,
        cursor: str | None = None,
        offset: int = 0,
    ) -> KeysetPage[dict[str, Any]]:
        """Return newest artifacts with stable pagination for long-lived stores."""
        if limit < 1 or offset < 0:
            raise ValueError("invalid artifact page bounds")
        if cursor and offset:
            raise ValueError("artifact cursor and offset cannot be combined")
        tenant_id = self._resolve_tenant(tenant_id)
        conditions = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if artifact_type:
            conditions.append("artifact_type = ?")
            params.append(artifact_type)
        if stale is not None:
            conditions.append("stale = ?")
            params.append(int(stale))
        if cursor:
            updated_at, row_id = _decode_artifact_cursor(cursor)
            conditions.append("(updated_at < ? OR (updated_at = ? AND id < ?))")
            params.extend([updated_at, updated_at, row_id])
        params.extend([limit + 1, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT id, tenant_id, artifact_id, artifact_type, source_vendor, source_instance,
                           external_id, title, provenance_url, fingerprint, extraction_generation,
                           stale, missing_since, first_seen_at, last_seen_at,
                           updated_at, created_at
                    FROM learned_artifacts
                    WHERE {' AND '.join(conditions)}
                    ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(float(last["updated_at"]), int(last["id"]))
        return KeysetPage(
            items=[_deserialize_learned_artifact(row) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_unreconciled_stale_artifacts(
        self,
        *,
        tenant_id: str | None = None,
        artifact_type: str,
        limit: int = 1_000,
        after_id: int = 0,
    ) -> list[dict[str, Any]]:
        """Return a keyset page of stale artifacts whose knowledge transition is pending."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, artifact_id, missing_since FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_type=? AND stale=1
                     AND knowledge_reconciled_at IS NULL AND id>?
                   ORDER BY id LIMIT ?""",
                (tenant_id, artifact_type, after_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def artifact_stale_generation_is_current(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str | None,
        artifact_id: str,
        missing_since: float | None,
    ) -> bool:
        """Check a stale artifact generation on the caller's write-locked connection."""
        tenant_id = self._resolve_tenant(tenant_id)
        return (
            conn.execute(
                """SELECT 1 FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_id=? AND stale=1
                     AND missing_since IS ? AND knowledge_reconciled_at IS NULL""",
                (tenant_id, artifact_id, missing_since),
            ).fetchone()
            is not None
        )

    def mark_artifact_knowledge_reconciled(
        self,
        *,
        tenant_id: str | None = None,
        artifact_id: str,
        missing_since: float | None,
    ) -> bool:
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE learned_artifacts SET knowledge_reconciled_at=?
                   WHERE tenant_id=? AND artifact_id=? AND stale=1
                     AND missing_since IS ? AND knowledge_reconciled_at IS NULL""",
                (time.time(), tenant_id, artifact_id, missing_since),
            )
        return cursor.rowcount == 1

    def get_learned_artifact(
        self,
        artifact_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Return one learned artifact by stable artifact ID."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM learned_artifacts WHERE tenant_id = ? AND artifact_id = ?",
                (tenant_id, artifact_id),
            ).fetchone()
        return _deserialize_learned_artifact(row) if row else None

    def list_artifact_extractions(
        self,
        artifact_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Return extracted IR rows for one artifact."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            evidence = conn.execute(
                """SELECT * FROM evidence_requirements
                   WHERE tenant_id = ? AND artifact_id = ? ORDER BY priority, id""",
                (tenant_id, artifact_id),
            ).fetchall()
            ownership = conn.execute(
                "SELECT * FROM ownership_hints WHERE tenant_id = ? AND artifact_id = ? ORDER BY id",
                (tenant_id, artifact_id),
            ).fetchall()
            dependencies = conn.execute(
                "SELECT * FROM dependency_hints WHERE tenant_id = ? AND artifact_id = ? ORDER BY id",
                (tenant_id, artifact_id),
            ).fetchall()
            signal_candidates = conn.execute(
                """SELECT * FROM signal_mapping_candidates
                   WHERE tenant_id = ? AND artifact_id = ? ORDER BY id""",
                (tenant_id, artifact_id),
            ).fetchall()
        return {
            "evidence_requirements": [dict(row) for row in evidence],
            "ownership_hints": [dict(row) for row in ownership],
            "dependency_hints": [dict(row) for row in dependencies],
            "signal_mapping_candidates": [dict(row) for row in signal_candidates],
        }

    def list_artifact_extraction_page(
        self,
        artifact_id: str,
        *,
        extraction_kind: str,
        tenant_id: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> KeysetPage[dict[str, Any]]:
        """Return one bounded extraction-kind page for an artifact."""
        table = _ARTIFACT_EXTRACTION_TABLES.get(extraction_kind)
        if table is None:
            raise ValueError("unsupported artifact extraction kind")
        if limit < 1:
            raise ValueError("extraction page limit must be positive")
        tenant_id = self._resolve_tenant(tenant_id)
        conditions = ["tenant_id = ?", "artifact_id = ?"]
        params: list[Any] = [tenant_id, artifact_id]
        cursor_generation = None
        if cursor:
            cursor_generation, extraction_id = _decode_extraction_cursor(cursor)
            conditions.append("id > ?")
            params.append(extraction_id)
        params.append(limit + 1)
        with self.read_transaction() as conn:
            artifact = conn.execute(
                """SELECT extraction_generation FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_id=?""",
                (tenant_id, artifact_id),
            ).fetchone()
            if artifact is None:
                raise ValueError("learned artifact not found")
            extraction_generation = str(artifact["extraction_generation"] or "")
            if cursor_generation is not None and cursor_generation != extraction_generation:
                raise ArtifactGenerationConflictError(
                    "artifact extractions changed; restart pagination from the first page"
                )
            rows = conn.execute(
                f"""SELECT * FROM {table}
                    WHERE {' AND '.join(conditions)}
                    ORDER BY id ASC LIMIT ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = encode_cursor(extraction_generation, str(visible[-1]["id"])) if has_more and visible else None
        return KeysetPage(
            items=[dict(row) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def artifact_extraction_counts_batch(
        self,
        artifact_ids: list[str],
        *,
        tenant_id: str | None = None,
    ) -> dict[str, dict[str, int]]:
        """Load extraction summaries in bounded set queries instead of per artifact."""
        tenant_id = self._resolve_tenant(tenant_id)
        unique_ids = list(dict.fromkeys(str(value) for value in artifact_ids if str(value)))
        counts = {artifact_id: {kind: 0 for kind in _ARTIFACT_EXTRACTION_TABLES} for artifact_id in unique_ids}
        if not unique_ids:
            return counts
        with self._conn() as conn:
            for start in range(0, len(unique_ids), _ARTIFACT_COUNT_BATCH_SIZE):
                batch = unique_ids[start : start + _ARTIFACT_COUNT_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                for kind, table in _ARTIFACT_EXTRACTION_TABLES.items():
                    rows = conn.execute(
                        f"""SELECT artifact_id, COUNT(*) AS count FROM {table}
                            WHERE tenant_id=? AND artifact_id IN ({placeholders})
                            GROUP BY artifact_id""",
                        (tenant_id, *batch),
                    ).fetchall()
                    for row in rows:
                        counts[str(row["artifact_id"])][kind] = int(row["count"])
        return counts

    def artifact_extraction_counts(
        self,
        artifact_id: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, int]:
        """Return structured extraction row counts for one artifact."""
        return self.artifact_extraction_counts_batch(
            [artifact_id],
            tenant_id=tenant_id,
        )[artifact_id]

    def artifact_context_indexed(
        self,
        *,
        tenant_id: str | None = None,
        artifact_id: str,
        artifact_type: str,
    ) -> bool:
        """Return whether an artifact has rows in the optional learning index."""
        tenant_id = self._resolve_tenant(tenant_id)
        if not self._learning_index_available():
            return True
        try:
            with self._conn() as conn:
                row = conn.execute(
                    """SELECT 1 FROM learning_context_fts
                       WHERE tenant_id = ? AND source_kind = ? AND source_id = ?
                       LIMIT 1""",
                    (tenant_id, artifact_type, artifact_id),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            logger.warning("artifact_context_index_check_failed", error=str(exc))
            return True
        return row is not None

    def mark_missing_alerts_stale(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        seen_alert_uids: set[str],
        crawl_started_at: float | None = None,
    ) -> int:
        """Mark previously ingested backend alerts stale when absent from a crawl."""
        tenant_id = self._resolve_tenant(tenant_id)
        crawl_started_at = crawl_started_at if crawl_started_at is not None else time.time()
        marked_at = time.time()
        approval_claim_cutoff = marked_at - self._settings.learning_approval_claim_ttl_seconds
        after_id = 0
        total = 0
        recovered_claims = 0
        while True:
            with self.transaction() as conn:
                rows = conn.execute(
                    """SELECT id, alert_uid, last_seen_at, status FROM ingested_alerts
                       WHERE tenant_id=? AND backend_name=? AND stale=0
                         AND (status!='approving' OR reviewed_at IS NULL OR reviewed_at<=?)
                         AND id>? AND last_seen_at<=?
                       ORDER BY id LIMIT ?""",
                    (
                        tenant_id,
                        backend_name,
                        approval_claim_cutoff,
                        after_id,
                        crawl_started_at,
                        _STALE_SOURCE_PAGE_SIZE,
                    ),
                ).fetchall()
                if not rows:
                    break
                after_id = int(rows[-1]["id"])
                stale_uids: set[str] = set()
                for row in rows:
                    alert_uid = str(row["alert_uid"])
                    if alert_uid in seen_alert_uids:
                        continue
                    cursor = conn.execute(
                        """UPDATE ingested_alerts
                           SET stale=1, missing_since=COALESCE(missing_since, ?),
                               knowledge_reconciled_at=NULL, status='stale', updated_at=?
                           WHERE tenant_id=? AND id=? AND stale=0
                             AND (status!='approving' OR reviewed_at IS NULL OR reviewed_at<=?)
                             AND last_seen_at=?""",
                        (
                            marked_at,
                            marked_at,
                            tenant_id,
                            row["id"],
                            approval_claim_cutoff,
                            row["last_seen_at"],
                        ),
                    )
                    if cursor.rowcount == 1:
                        stale_uids.add(alert_uid)
                        recovered_claims += int(row["status"] == "approving")
                if not stale_uids:
                    continue
                total += len(stale_uids)
                if self._learning_index_available():
                    try:
                        alert_context_ids = sorted(f"alert:{uid}" for uid in stale_uids)
                        placeholders = ", ".join("?" for _ in alert_context_ids)
                        conn.execute(
                            f"""UPDATE learning_context_fts SET review_state='stale'
                                WHERE tenant_id=? AND source_kind='alert_rule' AND backend_name=?
                                  AND dashboard_uid IN ({placeholders})""",
                            (tenant_id, backend_name, *alert_context_ids),
                        )
                    except sqlite3.OperationalError as exc:
                        logger.warning("stale_alert_context_update_failed", error=str(exc))
                stale_source_refs = {f"{backend_name}:alert:{uid}" if backend_name else uid for uid in stale_uids}
                self._remove_mapping_source_refs(
                    conn,
                    tenant_id=tenant_id,
                    source_type="alert_ingest",
                    stale_refs=stale_source_refs,
                )
        if recovered_claims:
            logger.warning(
                "expired_alert_approval_claims_recovered",
                tenant_id=tenant_id,
                backend_name=backend_name,
                claim_count=recovered_claims,
            )
        return total

    def mark_missing_dashboards_stale(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        seen_dashboard_uids: set[str],
        crawl_started_at: float | None = None,
    ) -> int:
        """Mark dashboards absent from a complete backend crawl as stale."""
        tenant_id = self._resolve_tenant(tenant_id)
        crawl_started_at = crawl_started_at if crawl_started_at is not None else time.time()
        marked_at = time.time()
        approval_claim_cutoff = marked_at - self._settings.learning_approval_claim_ttl_seconds
        after_id = 0
        total = 0
        recovered_claims = 0
        while True:
            with self.transaction() as conn:
                rows = conn.execute(
                    """SELECT id, dashboard_uid, last_seen_at, status FROM ingested_dashboards
                       WHERE tenant_id=? AND backend_name=? AND stale=0
                         AND (status!='approving' OR reviewed_at IS NULL OR reviewed_at<=?)
                         AND id>? AND last_seen_at<=?
                       ORDER BY id LIMIT ?""",
                    (
                        tenant_id,
                        backend_name,
                        approval_claim_cutoff,
                        after_id,
                        crawl_started_at,
                        _STALE_SOURCE_PAGE_SIZE,
                    ),
                ).fetchall()
                if not rows:
                    break
                after_id = int(rows[-1]["id"])
                stale_uids: set[str] = set()
                for row in rows:
                    dashboard_uid = str(row["dashboard_uid"])
                    if dashboard_uid in seen_dashboard_uids:
                        continue
                    cursor = conn.execute(
                        """UPDATE ingested_dashboards
                           SET stale=1, missing_since=COALESCE(missing_since, ?),
                               knowledge_reconciled_at=NULL, status='stale'
                           WHERE tenant_id=? AND id=? AND stale=0
                             AND (status!='approving' OR reviewed_at IS NULL OR reviewed_at<=?)
                             AND last_seen_at=?""",
                        (
                            marked_at,
                            tenant_id,
                            row["id"],
                            approval_claim_cutoff,
                            row["last_seen_at"],
                        ),
                    )
                    if cursor.rowcount == 1:
                        stale_uids.add(dashboard_uid)
                        recovered_claims += int(row["status"] == "approving")
                if not stale_uids:
                    continue
                total += len(stale_uids)
                placeholders = ", ".join("?" for _ in stale_uids)
                if self._learning_index_available():
                    try:
                        conn.execute(
                            f"""UPDATE learning_context_fts SET review_state='stale'
                                WHERE tenant_id=? AND source_kind='dashboard_panel' AND backend_name=?
                                  AND dashboard_uid IN ({placeholders})""",
                            (tenant_id, backend_name, *sorted(stale_uids)),
                        )
                    except sqlite3.OperationalError as exc:
                        logger.warning("stale_dashboard_context_update_failed", error=str(exc))
                stale_refs = {f"{backend_name}:{uid}" if backend_name else uid for uid in stale_uids}
                self._remove_mapping_source_refs(
                    conn,
                    tenant_id=tenant_id,
                    source_type="dashboard_ingest",
                    stale_refs=stale_refs,
                )
        if recovered_claims:
            logger.warning(
                "expired_dashboard_approval_claims_recovered",
                tenant_id=tenant_id,
                backend_name=backend_name,
                claim_count=recovered_claims,
            )
        return total

    @staticmethod
    def _remove_mapping_source_refs(
        conn: sqlite3.Connection,
        *,
        tenant_id: str,
        source_type: str,
        stale_refs: set[str],
    ) -> None:
        """Remove stale provenance from active mappings, deleting unsupported rows."""
        if not stale_refs:
            return
        ordered_refs = sorted(stale_refs)
        placeholders = ", ".join("?" for _ in ordered_refs)
        mappings = conn.execute(
            f"""SELECT DISTINCT mapping.id, mapping.source_refs
                FROM signal_metric_mappings mapping
                JOIN signal_mapping_source_refs source ON source.mapping_id=mapping.id
                WHERE source.tenant_id=? AND mapping.governance_ref=''
                  AND source.source_ref IN ({placeholders})""",
            (tenant_id, *ordered_refs),
        ).fetchall()
        for mapping in mappings:
            refs = json.loads(mapping["source_refs"] or "[]")
            if stale_refs.isdisjoint(refs):
                continue
            remaining_refs = [ref for ref in refs if ref not in stale_refs]
            if remaining_refs:
                conn.execute(
                    "UPDATE signal_metric_mappings SET source_refs = ? WHERE id = ?",
                    (json.dumps(remaining_refs), mapping["id"]),
                )
            else:
                conn.execute("DELETE FROM signal_metric_mappings WHERE id = ?", (mapping["id"],))

    def reconcile_mapping_source(
        self,
        *,
        tenant_id: str,
        source_type: str,
        source_ref: str,
        active_pairs: set[tuple[str, str]],
    ) -> None:
        """Remove one refreshed source from mappings it no longer supports."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            mappings = conn.execute(
                """SELECT mapping.id, mapping.signal_type, mapping.metric_pattern,
                          mapping.source_refs
                   FROM signal_mapping_source_refs source
                   JOIN signal_metric_mappings mapping ON mapping.id=source.mapping_id
                   WHERE source.tenant_id=? AND source.source_ref=?
                     AND mapping.governance_ref=''""",
                (tenant_id, source_ref),
            ).fetchall()
            for mapping in mappings:
                refs = json.loads(mapping["source_refs"] or "[]")
                pair = (mapping["metric_pattern"], mapping["signal_type"])
                if source_ref not in refs or pair in active_pairs:
                    continue
                remaining_refs = [ref for ref in refs if ref != source_ref]
                if remaining_refs:
                    conn.execute(
                        "UPDATE signal_metric_mappings SET source_refs = ? WHERE id = ?",
                        (json.dumps(remaining_refs), mapping["id"]),
                    )
                else:
                    conn.execute("DELETE FROM signal_metric_mappings WHERE id = ?", (mapping["id"],))

    def index_alert_context(
        self,
        *,
        tenant_id: str | None = None,
        alert_uid: str,
        backend_name: str = "",
        alert_title: str = "",
        alert_tags: list[str] | None = None,
        condition: str = "",
        metrics_found: list[str] | None = None,
        query_transformations: list[str] | None = None,
        service_hints: list[str] | None = None,
        signals_inferred: list[dict[str, Any]] | list[str] | None = None,
        status: str = "pending",
        activated_pairs: set[tuple[str, str]] | None = None,
    ) -> int:
        """Index learned alert-rule context for fast operational-language retrieval."""
        tenant_id = self._resolve_tenant(tenant_id)
        if not self._learning_index_available():
            return 0

        rows = build_alert_context_rows(
            alert_uid=alert_uid,
            backend_name=backend_name,
            alert_title=alert_title,
            alert_tags=alert_tags or [],
            condition=condition,
            metrics_found=metrics_found or [],
            query_transformations=query_transformations or [],
            service_hints=service_hints or [],
            signals_inferred=signals_inferred or [],
            status=status,
            activated_pairs=activated_pairs,
        )

        try:
            with self._conn() as conn:
                conn.execute(
                    """DELETE FROM learning_context_fts
                       WHERE tenant_id = ? AND source_kind = 'alert_rule'
                         AND dashboard_uid = ? AND backend_name = ?""",
                    (tenant_id, f"alert:{alert_uid}", backend_name),
                )
                conn.executemany(
                    """INSERT INTO learning_context_fts
                       (tenant_id, source_kind, source_id, backend_name, dashboard_uid,
                        dashboard_title, dashboard_tags, panel_title, metric_name,
                        query_text, service, signal_type, review_state, reason,
                        provenance, indexed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    [(tenant_id, *row) for row in rows],
                )
        except sqlite3.OperationalError as exc:
            logger.warning("alert_context_index_failed", error=str(exc))
            return 0
        return len(rows)

    def update_learning_context_review_state(
        self,
        dashboard_uid: str,
        review_state: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
        tenant_id: str | None = None,
    ) -> int:
        """Reflect dashboard approval/rejection in the retrieval index."""
        tenant_id = self._resolve_tenant(tenant_id)
        if not self._learning_index_available():
            return 0
        backend = backend_name if backend_name is not None else ""
        with self._conn() as conn:
            try:
                if review_state == "approved" and activated_pairs is not None:
                    if backend_name is None:
                        cursor = conn.execute(
                            """UPDATE learning_context_fts SET review_state = 'candidate'
                               WHERE tenant_id = ? AND source_kind = 'dashboard_panel'
                                 AND dashboard_uid = ?""",
                            (tenant_id, dashboard_uid),
                        )
                    else:
                        cursor = conn.execute(
                            """UPDATE learning_context_fts SET review_state = 'candidate'
                               WHERE tenant_id = ? AND source_kind = 'dashboard_panel'
                                 AND dashboard_uid = ? AND backend_name = ?""",
                            (tenant_id, dashboard_uid, backend),
                        )
                    rows_updated = cursor.rowcount
                    for metric, signal_type in activated_pairs:
                        if backend_name is None:
                            cursor = conn.execute(
                                """UPDATE learning_context_fts SET review_state = 'approved'
                                   WHERE tenant_id = ? AND source_kind = 'dashboard_panel'
                                     AND dashboard_uid = ? AND metric_name = ? AND signal_type = ?""",
                                (tenant_id, dashboard_uid, metric, signal_type),
                            )
                        else:
                            cursor = conn.execute(
                                """UPDATE learning_context_fts SET review_state = 'approved'
                                   WHERE tenant_id = ? AND source_kind = 'dashboard_panel'
                                     AND dashboard_uid = ? AND backend_name = ?
                                     AND metric_name = ? AND signal_type = ?""",
                                (tenant_id, dashboard_uid, backend, metric, signal_type),
                            )
                        rows_updated += cursor.rowcount
                    return rows_updated
                if backend_name is None:
                    cursor = conn.execute(
                        """UPDATE learning_context_fts SET review_state = ?
                           WHERE tenant_id = ? AND source_kind = 'dashboard_panel' AND dashboard_uid = ?""",
                        (review_state, tenant_id, dashboard_uid),
                    )
                else:
                    cursor = conn.execute(
                        """UPDATE learning_context_fts SET review_state = ?
                           WHERE tenant_id = ? AND source_kind = 'dashboard_panel'
                             AND dashboard_uid = ? AND backend_name = ?""",
                        (review_state, tenant_id, dashboard_uid, backend),
                    )
                return cursor.rowcount
            except sqlite3.OperationalError as exc:
                logger.warning("learning_context_review_state_update_failed", error=str(exc))
                return 0

    def search_learning_context(
        self,
        query: str,
        *,
        service: str = "",
        include_candidates: bool = True,
        limit: int = 20,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search the learned operational knowledge index."""
        tenant_id = self._resolve_tenant(tenant_id)
        if not self._learning_index_available():
            raise LearningIndexUnavailable(
                "Learned-context search requires SQLite FTS5, but this SQLite build does not provide it."
            )

        match_query = _fts_query(query)
        if not match_query:
            return []

        clauses = ["learning_context_fts MATCH ?", "tenant_id = ?"]
        params: list[Any] = [match_query, tenant_id]
        if service:
            clauses.append("lower(service) LIKE ?")
            params.append(f"%{service.lower()}%")
        if not include_candidates:
            clauses.append("review_state IN ('approved', 'trusted')")
        else:
            clauses.append("review_state NOT IN ('rejected', 'ignored', 'stale')")
        params.append(limit)

        sql = f"""SELECT rowid, *, bm25(learning_context_fts) AS rank
                  FROM learning_context_fts
                  WHERE {" AND ".join(clauses)}
                  ORDER BY rank
                  LIMIT ?"""
        with self._conn() as conn:
            try:
                rows = conn.execute(sql, params).fetchall()
            except sqlite3.OperationalError as exc:
                logger.warning("learning_context_search_failed", query=query, error=str(exc))
                return []
        return [dict(row) for row in rows]

    def describe_service(
        self,
        service: str,
        *,
        include_candidates: bool = True,
        limit: int = 50,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """Summarize what Tacit has learned about a service."""
        tenant_id = self._resolve_tenant(tenant_id)
        rows = self.search_learning_context(
            service,
            service=service,
            include_candidates=include_candidates,
            limit=limit,
            tenant_id=tenant_id,
        )

        dashboards: dict[str, dict[str, Any]] = {}
        metrics: dict[str, dict[str, Any]] = {}
        signals: dict[str, int] = {}
        panels: dict[str, int] = {}
        trusted_rows = 0
        candidate_rows = 0

        for row in rows:
            state = row.get("review_state", "")
            if state in {"approved", "trusted"}:
                trusted_rows += 1
            else:
                candidate_rows += 1

            dash_key = f"{row.get('backend_name', '')}:{row.get('dashboard_uid', '')}"
            dashboards.setdefault(
                dash_key,
                {
                    "dashboard_uid": row.get("dashboard_uid", ""),
                    "backend_name": row.get("backend_name", ""),
                    "dashboard_title": row.get("dashboard_title", ""),
                    "review_state": state,
                },
            )
            metric = row.get("metric_name", "")
            if metric:
                metrics.setdefault(
                    metric,
                    {
                        "metric": metric,
                        "signal_types": [],
                        "review_states": [],
                        "example_panel": row.get("panel_title", ""),
                    },
                )
                signal_type = row.get("signal_type", "")
                if signal_type and signal_type not in metrics[metric]["signal_types"]:
                    metrics[metric]["signal_types"].append(signal_type)
                if state and state not in metrics[metric]["review_states"]:
                    metrics[metric]["review_states"].append(state)

            signal_type = row.get("signal_type", "")
            if signal_type:
                signals[signal_type] = signals.get(signal_type, 0) + 1

            panel_title = row.get("panel_title", "")
            if panel_title:
                panels[panel_title] = panels.get(panel_title, 0) + 1

        return {
            "service": service,
            "matched_context_rows": len(rows),
            "trusted_context_rows": trusted_rows,
            "candidate_context_rows": candidate_rows,
            "dashboards": list(dashboards.values()),
            "top_metrics": sorted(metrics.values(), key=lambda m: len(m["signal_types"]), reverse=True)[:12],
            "signals": dict(sorted(signals.items(), key=lambda item: item[1], reverse=True)),
            "top_panels": [
                {"panel_title": title, "matches": count}
                for title, count in sorted(panels.items(), key=lambda item: item[1], reverse=True)[:10]
            ],
        }

    def _learning_index_available(self) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'learning_context_fts'"
            ).fetchone()
            return row is not None

    def get_ingested_dashboard(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Get ingested dashboard record."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            if backend_name is None:
                rows = conn.execute(
                    """SELECT * FROM ingested_dashboards
                       WHERE tenant_id = ? AND dashboard_uid = ?
                       ORDER BY created_at DESC LIMIT 2""",
                    (tenant_id, dashboard_uid),
                ).fetchall()
                if len(rows) != 1:
                    return None
                row = rows[0]
            else:
                row = conn.execute(
                    """SELECT * FROM ingested_dashboards
                       WHERE tenant_id = ? AND dashboard_uid = ? AND backend_name = ?""",
                    (tenant_id, dashboard_uid, backend_name),
                ).fetchone()
        if row is None:
            return None
        return _deserialize_ingested(row)

    def list_ingested_dashboards(
        self,
        status: str | None = None,
        limit: int = 50,
        *,
        tenant_id: str | None = None,
        backend_name: str | None = None,
        offset: int = 0,
        before_created_at: float | None = None,
        before_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """List ingested dashboards, optionally filtered by status."""
        if not 1 <= limit <= _LEARNING_LIST_MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {_LEARNING_LIST_MAX_LIMIT}")
        if offset < 0:
            raise ValueError("offset cannot be negative")
        if (before_created_at is None) != (before_id is None):
            raise ValueError("before_created_at and before_id must be supplied together")
        if before_created_at is not None and offset:
            raise ValueError("cursor and offset pagination cannot be combined")
        tenant_id = self._resolve_tenant(tenant_id)
        conditions = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if status:
            conditions.append("status = ?")
            params.append(status)
        if backend_name is not None:
            conditions.append("backend_name = ?")
            params.append(backend_name)
        if before_created_at is not None and before_id is not None:
            conditions.append("(created_at < ? OR (created_at = ? AND id < ?))")
            params.extend([before_created_at, before_created_at, before_id])
        params.extend([limit, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT * FROM ingested_dashboards
                    WHERE {" AND ".join(conditions)}
                    ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        return [_deserialize_ingested(r) for r in rows]

    def list_unreconciled_stale_dashboards(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        limit: int = 500,
        after_id: int = 0,
    ) -> list[dict[str, Any]]:
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, dashboard_uid, missing_since FROM ingested_dashboards
                   WHERE tenant_id=? AND backend_name=? AND stale=1
                     AND knowledge_reconciled_at IS NULL AND id>?
                   ORDER BY id LIMIT ?""",
                (tenant_id, backend_name, after_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def dashboard_stale_generation_is_current(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str | None,
        backend_name: str,
        dashboard_uid: str,
        missing_since: float | None,
    ) -> bool:
        """Check a stale dashboard generation on the caller's write-locked connection."""
        tenant_id = self._resolve_tenant(tenant_id)
        return (
            conn.execute(
                """SELECT 1 FROM ingested_dashboards
                   WHERE tenant_id=? AND backend_name=? AND dashboard_uid=? AND stale=1
                     AND missing_since IS ? AND knowledge_reconciled_at IS NULL""",
                (tenant_id, backend_name, dashboard_uid, missing_since),
            ).fetchone()
            is not None
        )

    def mark_dashboard_knowledge_reconciled(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        dashboard_uid: str,
        missing_since: float | None,
    ) -> bool:
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE ingested_dashboards SET knowledge_reconciled_at=?
                   WHERE tenant_id=? AND backend_name=? AND dashboard_uid=? AND stale=1
                     AND missing_since IS ? AND knowledge_reconciled_at IS NULL""",
                (time.time(), tenant_id, backend_name, dashboard_uid, missing_since),
            )
        return cursor.rowcount == 1

    def list_ingested_alerts(
        self,
        status: str | None = None,
        limit: int = 50,
        *,
        tenant_id: str | None = None,
        backend_name: str | None = None,
        offset: int = 0,
        before_created_at: float | None = None,
        before_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """List ingested alerts, optionally filtered by status."""
        if not 1 <= limit <= _LEARNING_LIST_MAX_LIMIT:
            raise ValueError(f"limit must be between 1 and {_LEARNING_LIST_MAX_LIMIT}")
        if offset < 0:
            raise ValueError("offset cannot be negative")
        if (before_created_at is None) != (before_id is None):
            raise ValueError("before_created_at and before_id must be supplied together")
        if before_created_at is not None and offset:
            raise ValueError("cursor and offset pagination cannot be combined")
        tenant_id = self._resolve_tenant(tenant_id)
        conditions = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        if status:
            conditions.append("status = ?")
            params.append(status)
        if backend_name is not None:
            conditions.append("backend_name = ?")
            params.append(backend_name)
        if before_created_at is not None and before_id is not None:
            conditions.append("(created_at < ? OR (created_at = ? AND id < ?))")
            params.extend([before_created_at, before_created_at, before_id])
        params.extend([limit, offset])
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT * FROM ingested_alerts
                    WHERE {" AND ".join(conditions)}
                    ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
                params,
            ).fetchall()
        return [_deserialize_ingested_alert(r) for r in rows]

    def list_unreconciled_stale_alerts(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        limit: int = 500,
        after_id: int = 0,
    ) -> list[dict[str, Any]]:
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, alert_uid, missing_since FROM ingested_alerts
                   WHERE tenant_id=? AND backend_name=? AND stale=1
                     AND knowledge_reconciled_at IS NULL AND id>?
                   ORDER BY id LIMIT ?""",
                (tenant_id, backend_name, after_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def alert_stale_generation_is_current(
        self,
        conn: sqlite3.Connection,
        *,
        tenant_id: str | None,
        backend_name: str,
        alert_uid: str,
        missing_since: float | None,
    ) -> bool:
        """Check a stale alert generation on the caller's write-locked connection."""
        tenant_id = self._resolve_tenant(tenant_id)
        return (
            conn.execute(
                """SELECT 1 FROM ingested_alerts
                   WHERE tenant_id=? AND backend_name=? AND alert_uid=? AND stale=1
                     AND missing_since IS ? AND knowledge_reconciled_at IS NULL""",
                (tenant_id, backend_name, alert_uid, missing_since),
            ).fetchone()
            is not None
        )

    def mark_alert_knowledge_reconciled(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        alert_uid: str,
        missing_since: float | None,
    ) -> bool:
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE ingested_alerts SET knowledge_reconciled_at=?
                   WHERE tenant_id=? AND backend_name=? AND alert_uid=? AND stale=1
                     AND missing_since IS ? AND knowledge_reconciled_at IS NULL""",
                (time.time(), tenant_id, backend_name, alert_uid, missing_since),
            )
        return cursor.rowcount == 1

    def get_ingested_alert(
        self,
        alert_uid: str,
        backend_name: str = "",
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Return one ingested alert row by backend-scoped alert UID."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            row = conn.execute(
                """SELECT * FROM ingested_alerts
                   WHERE tenant_id = ? AND alert_uid = ? AND backend_name = ?""",
                (tenant_id, alert_uid, backend_name),
            ).fetchone()
        if row is None:
            return None
        return _deserialize_ingested_alert(row)

    def finalize_ingested_alert_approval(
        self,
        alert_uid: str,
        backend_name: str,
        *,
        generation_fingerprint: str,
        tenant_id: str | None = None,
    ) -> bool:
        """Finalize the exact claimed alert generation after promotion succeeds."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE ingested_alerts SET status = 'approved', reviewed_at = ?
                   WHERE tenant_id = ? AND alert_uid = ? AND backend_name = ?
                     AND generation_fingerprint = ? AND stale = 0 AND status = 'approving'""",
                (time.time(), tenant_id, alert_uid, backend_name, generation_fingerprint),
            )
        return cursor.rowcount == 1

    def claim_ingested_alert_approval(
        self,
        alert_uid: str,
        backend_name: str,
        *,
        generation_fingerprint: str,
        tenant_id: str | None = None,
    ) -> bool:
        """Claim one pending alert generation before promoting its mappings."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE ingested_alerts SET status='approving', reviewed_at=?
                   WHERE tenant_id=? AND alert_uid=? AND backend_name=?
                     AND generation_fingerprint=? AND stale=0 AND status='pending'""",
                (time.time(), tenant_id, alert_uid, backend_name, generation_fingerprint),
            )
        return cursor.rowcount == 1

    def claim_ingested_dashboard_approval(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        *,
        expected_generation: float,
        tenant_id: str | None = None,
    ) -> bool:
        """Claim one pending dashboard generation before promoting its mappings."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self.transaction() as conn:
            ingested = self.get_ingested_dashboard(dashboard_uid, backend_name, tenant_id=tenant_id)
            if ingested is None:
                return False
            cursor = conn.execute(
                """UPDATE ingested_dashboards SET status = 'approving', reviewed_at = ?
                   WHERE id = ? AND created_at = ? AND stale = 0 AND status = 'pending'""",
                (time.time(), ingested["id"], expected_generation),
            )
        return cursor.rowcount == 1

    def finalize_ingested_dashboard_approval(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
        *,
        expected_generation: float,
        tenant_id: str | None = None,
    ) -> bool:
        """Finalize only the dashboard generation previously claimed for approval."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self.transaction() as conn:
            ingested = self.get_ingested_dashboard(dashboard_uid, backend_name, tenant_id=tenant_id)
            if ingested is None:
                return False
            cursor = conn.execute(
                """UPDATE ingested_dashboards SET status = 'approved', reviewed_at = ?
                   WHERE id = ? AND created_at = ? AND stale = 0 AND status = 'approving'""",
                (time.time(), ingested["id"], expected_generation),
            )
            changed = cursor.rowcount == 1
        if changed:
            pairs = activated_pairs
            if pairs is None:
                pairs = _eligible_pairs_from_ingested_signals(ingested.get("signals_inferred", []))
            self.update_learning_context_review_state(
                dashboard_uid,
                "approved",
                backend_name,
                activated_pairs=pairs,
                tenant_id=tenant_id,
            )
        return changed

    def update_ingested_dashboard_status(
        self,
        dashboard_uid: str,
        status: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
        *,
        tenant_id: str | None = None,
    ) -> bool:
        """Move an eligible dashboard review state to a terminal decision."""
        tenant_id = self._resolve_tenant(tenant_id)
        if status not in {"approved", "rejected", "ignored"}:
            raise ValueError(f"unsupported ingested dashboard status: {status}")

        ingested = self.get_ingested_dashboard(dashboard_uid, backend_name, tenant_id=tenant_id)
        if ingested is None:
            return False

        expected_statuses = ("pending",) if status == "approved" else ("pending", "approved")
        placeholders = ", ".join("?" for _ in expected_statuses)
        with self._conn() as conn:
            cursor = conn.execute(
                f"""UPDATE ingested_dashboards SET status = ?, reviewed_at = ?
                    WHERE id = ? AND status IN ({placeholders})""",
                (status, time.time(), ingested["id"], *expected_statuses),
            )
            changed = cursor.rowcount > 0
        if changed:
            if status == "approved":
                pairs = activated_pairs
                if pairs is None:
                    pairs = _eligible_pairs_from_ingested_signals(ingested.get("signals_inferred", []))
                self.update_learning_context_review_state(
                    dashboard_uid,
                    "approved",
                    backend_name,
                    activated_pairs=pairs,
                    tenant_id=tenant_id,
                )
            else:
                self.update_learning_context_review_state(
                    dashboard_uid,
                    status,
                    backend_name,
                    tenant_id=tenant_id,
                )
        return changed

    def approve_ingested_dashboard(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
        *,
        tenant_id: str | None = None,
    ) -> bool:
        """Approve a pending ingested dashboard (activates its signal mappings)."""
        return self.update_ingested_dashboard_status(
            dashboard_uid,
            "approved",
            backend_name,
            activated_pairs=activated_pairs,
            tenant_id=tenant_id,
        )

    def reject_ingested_dashboard(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        *,
        tenant_id: str | None = None,
    ) -> bool:
        """Reject a pending ingested dashboard as unsuitable for learning."""
        return self.update_ingested_dashboard_status(dashboard_uid, "rejected", backend_name, tenant_id=tenant_id)

    def ignore_ingested_dashboard(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        *,
        tenant_id: str | None = None,
    ) -> bool:
        """Ignore a pending ingested dashboard without treating it as negative signal data."""
        return self.update_ingested_dashboard_status(dashboard_uid, "ignored", backend_name, tenant_id=tenant_id)

    # ── Stats ────────────────────────────────────────────────────────────

    def stats(self, *, tenant_id: str | None = None) -> dict[str, Any]:
        """Summary statistics for the signal store."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            mapping_count = conn.execute(
                "SELECT COUNT(*) FROM signal_metric_mappings WHERE tenant_id IN (?, ?)",
                (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
            ).fetchone()[0]
            ingested_count = conn.execute(
                "SELECT COUNT(*) FROM ingested_dashboards WHERE tenant_id = ?", (tenant_id,)
            ).fetchone()[0]
            ingested_alert_count = conn.execute(
                "SELECT COUNT(*) FROM ingested_alerts WHERE tenant_id = ?", (tenant_id,)
            ).fetchone()[0]
            learned_artifact_count = conn.execute(
                "SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id = ?", (tenant_id,)
            ).fetchone()[0]

            by_source = conn.execute(
                """SELECT source_type, COUNT(*) as n
                   FROM signal_metric_mappings
                   WHERE tenant_id IN (?, ?)
                   GROUP BY source_type""",
                (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
            ).fetchall()

        visible_definitions = self.list_signal_types(tenant_id=tenant_id)
        by_category: dict[str, int] = {}
        for definition in visible_definitions:
            category = str(definition.get("category", ""))
            by_category[category] = by_category.get(category, 0) + 1

        return {
            "signal_types": len(visible_definitions),
            "metric_mappings": mapping_count,
            "ingested_dashboards": ingested_count,
            "ingested_alerts": ingested_alert_count,
            "learned_artifacts": learned_artifact_count,
            "mappings_by_source": {r["source_type"]: r["n"] for r in by_source},
            "signals_by_category": by_category,
        }


def _merge_signal_definition(
    global_definition: dict[str, Any] | None,
    tenant_definition: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if tenant_definition is None:
        return global_definition
    if global_definition is None:
        return tenant_definition
    merged = dict(global_definition)
    for field in ("description", "category", "unit"):
        if tenant_definition.get(field):
            merged[field] = tenant_definition[field]
    merged["updated_at"] = tenant_definition["updated_at"]
    merged["tenant_id"] = tenant_definition["tenant_id"]
    return merged


def _governed_scope_matches(
    mapping: dict[str, Any],
    *,
    knowledge_scope: Any | None,
    context_service: str,
    context_datasource_type: str,
    context_archetype: str,
    context_environment: str,
    now: float,
) -> bool:
    """Fail closed when an immutable mapping's complete scope is unavailable or mismatched."""
    if knowledge_scope is not None and str(getattr(knowledge_scope, "tenant_id", "")) != mapping["tenant_id"]:
        return False

    def normalized(values: Any, *prefixes: str) -> set[str]:
        result: set[str] = set()
        for raw in values or []:
            value = str(raw).strip().casefold()
            for prefix in prefixes:
                if value.startswith(prefix):
                    value = value.removeprefix(prefix)
                    break
            if value:
                result.add(value)
        return result

    scope_values = {
        "context_services": normalized(
            getattr(knowledge_scope, "service_refs", []) if knowledge_scope is not None else [context_service],
            "entity:service:",
            "service:",
        ),
        "context_environments": normalized(
            getattr(knowledge_scope, "environment_refs", []) if knowledge_scope is not None else [context_environment],
            "environment:",
        ),
        "context_archetypes": normalized(
            getattr(knowledge_scope, "archetype_refs", []) if knowledge_scope is not None else [context_archetype],
            "archetype:",
        ),
        "context_regions": normalized(
            getattr(knowledge_scope, "region_refs", []) if knowledge_scope is not None else [],
            "region:",
        ),
        "context_clusters": normalized(
            getattr(knowledge_scope, "cluster_refs", []) if knowledge_scope is not None else [],
            "cluster:",
        ),
        "context_namespaces": normalized(
            getattr(knowledge_scope, "namespace_refs", []) if knowledge_scope is not None else [],
            "namespace:",
        ),
        "context_versions": normalized(
            getattr(knowledge_scope, "version_constraints", []) if knowledge_scope is not None else [],
            "version:",
        ),
        "context_datasource_types": normalized([context_datasource_type]),
    }
    for field, actual in scope_values.items():
        if field == "context_versions":
            continue
        required = normalized(mapping.get(field))
        if required and not required.intersection(actual):
            return False
    if not version_scope_applies(
        normalized(mapping.get("context_versions")),
        scope_values["context_versions"],
    ):
        return False
    valid_from = mapping.get("valid_from")
    valid_until = mapping.get("valid_until")
    return not ((valid_from is not None and now < valid_from) or (valid_until is not None and now >= valid_until))


def _deserialize_mapping(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a DB row to a dict with deserialized JSON fields."""
    d = dict(row)
    for field in (
        "context_services",
        "context_datasource_types",
        "context_environments",
        "context_archetypes",
        "context_regions",
        "context_clusters",
        "context_namespaces",
        "context_versions",
        "source_refs",
    ):
        if field in d and isinstance(d[field], str):
            d[field] = json.loads(d[field])
    return d


def _deserialize_ingested(row: sqlite3.Row) -> dict[str, Any]:
    """Convert an ingested dashboard DB row to a dict."""
    d = dict(row)
    for field in (
        "dashboard_tags",
        "metrics_found",
        "row_groups",
        "metric_cooccurrence",
        "aggregation_patterns",
        "query_transformations",
        "panel_titles",
        "alert_links",
        "drilldown_links",
        "signals_inferred",
    ):
        if field in d and isinstance(d[field], str):
            d[field] = json.loads(d[field])
    if "stale" in d:
        d["stale"] = bool(d["stale"])
    return d


def _deserialize_ingested_alert(row: sqlite3.Row) -> dict[str, Any]:
    """Convert an ingested alert DB row to a dict."""
    d = dict(row)
    for field in (
        "alert_tags",
        "labels",
        "annotations",
        "metrics_found",
        "query_transformations",
        "service_hints",
        "signals_inferred",
    ):
        if field in d and isinstance(d[field], str):
            d[field] = json.loads(d[field])
    if "enabled" in d:
        d["enabled"] = bool(d["enabled"])
    if "stale" in d:
        d["stale"] = bool(d["stale"])
    return d


def _deserialize_learned_artifact(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a learned artifact DB row to a dict."""
    d = dict(row)
    if "stale" in d:
        d["stale"] = bool(d["stale"])
    return d


# ── Singleton ────────────────────────────────────────────────────────────────

_store: SignalStore | None = None


def get_signal_store() -> SignalStore:
    """Get or create the global SignalStore singleton."""
    global _store
    if _store is None:
        _store = SignalStore()
        # Auto-load bootstrap signals on first access
        _store.load_from_yaml(only_if_changed=True)
    return _store
