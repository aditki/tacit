"""SQLite persistence for governed, immutable Operational Knowledge."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import structlog

from tacit.config import settings
from tacit.knowledge.enums import (
    ConflictResolutionStatus,
    EntityResolutionStatus,
    LifecycleStatus,
    PromotionDecisionType,
    ReviewState,
)
from tacit.knowledge.models import (
    CorroborationSummary,
    Entity,
    EntityAlias,
    EntityResolutionResult,
    KnowledgeCandidate,
    KnowledgeConflict,
    KnowledgeCorrection,
    KnowledgeRevision,
    KnowledgeScope,
    KnowledgeSnapshot,
    KnowledgeUsage,
    OperationalKnowledgeItem,
    PromotionDecision,
    utc_now,
)
from tacit.knowledge.normalization import normalize_entity
from tacit.signals.schema import SQLITE_BUSY_TIMEOUT_MS

logger = structlog.get_logger()
_UNSET = object()
_MIGRATION_BATCH_SIZE = 500
_SQLITE_BIND_BATCH_SIZE = 400
_SCOPE_REFERENCE_FIELDS = (
    "environment_refs",
    "region_refs",
    "cluster_refs",
    "namespace_refs",
    "service_refs",
    "archetype_refs",
)


def _candidate_review_priority(candidate: KnowledgeCandidate, *, unresolved_conflict: bool) -> int:
    priority = 100 if unresolved_conflict else 0
    if candidate.payload_ref.startswith(("correction_", "correction:")):
        priority += 90
    if candidate.entity_resolution.status in {
        EntityResolutionStatus.AMBIGUOUS,
        EntityResolutionStatus.UNRESOLVED,
    }:
        priority += 80
    if candidate.security_flags:
        priority += 70
    if candidate.kind.value in {"dependency", "signal_mapping", "evidence_requirement"}:
        priority += 20
    return priority


def _has_unresolved_conflict(conn: sqlite3.Connection, tenant_id: str, proposition_key: str) -> bool:
    return (
        conn.execute(
            """SELECT 1 FROM (
                   SELECT 1 FROM knowledge_conflicts
                   WHERE tenant_id=? AND left_proposition_key=?
                     AND resolution_status='unresolved'
                   UNION ALL
                   SELECT 1 FROM knowledge_conflicts
                   WHERE tenant_id=? AND right_proposition_key=?
                     AND resolution_status='unresolved'
               ) LIMIT 1""",
            (tenant_id, proposition_key, tenant_id, proposition_key),
        ).fetchone()
        is not None
    )


def _candidate_matches_json(raw: str, expected: KnowledgeCandidate) -> bool:
    """Compare candidate state after model defaults normalize legacy JSON."""
    try:
        return KnowledgeCandidate.model_validate_json(raw) == expected
    except ValueError:
        return False


class CandidateReviewConflictError(ValueError):
    """Raised when a reviewer acts on a candidate state that has already changed."""


class CandidateEvaluationConflictError(ValueError):
    """Raised when a candidate changes while its promotion policy is evaluated."""


class CandidateLifecycleConflictError(ValueError):
    """Raised when source lifecycle reconciliation loses to a concurrent transition."""


class CandidateMergeConflictError(ValueError):
    """Raised when re-ingestion loses to a concurrent candidate transition."""


class AliasRegistrationConflictError(ValueError):
    """Raised when an alias changes during a registration transition."""


class KnowledgeRevisionConflictError(ValueError):
    """Raised when another writer advances an immutable knowledge item first."""


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS knowledge_candidates (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_ref TEXT NOT NULL,
    proposition_key TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    review_state TEXT NOT NULL,
    lifecycle_status TEXT NOT NULL,
    eligibility TEXT NOT NULL,
    entity_resolution_status TEXT NOT NULL,
    promotion_policy_id TEXT NOT NULL DEFAULT '',
    promotion_policy_version TEXT NOT NULL DEFAULT '',
    review_priority INTEGER NOT NULL DEFAULT 0,
    has_unresolved_conflict INTEGER NOT NULL DEFAULT 0,
    candidate_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(tenant_id, id)
);
CREATE INDEX IF NOT EXISTS idx_kc_tenant_kind ON knowledge_candidates(tenant_id, kind, created_at);
CREATE INDEX IF NOT EXISTS idx_kc_proposition ON knowledge_candidates(tenant_id, proposition_key);

CREATE TABLE IF NOT EXISTS knowledge_candidate_evidence (
    candidate_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    evidence_ref TEXT NOT NULL,
    evidence_role TEXT NOT NULL,
    source_family TEXT NOT NULL,
    lineage_group TEXT NOT NULL,
    lineage_kind TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY(candidate_id, evidence_ref),
    FOREIGN KEY(candidate_id) REFERENCES knowledge_candidates(id)
);

CREATE TABLE IF NOT EXISTS knowledge_candidate_provenance (
    tenant_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    provenance_ref TEXT NOT NULL,
    source_family TEXT NOT NULL DEFAULT 'unknown',
    PRIMARY KEY(tenant_id, candidate_id, provenance_ref),
    FOREIGN KEY(candidate_id) REFERENCES knowledge_candidates(id)
);
CREATE INDEX IF NOT EXISTS idx_kcp_tenant_provenance
    ON knowledge_candidate_provenance(tenant_id, provenance_ref, candidate_id);

CREATE TABLE IF NOT EXISTS promotion_decisions (
    decision_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    decision TEXT NOT NULL,
    resulting_eligibility TEXT NOT NULL,
    reason_codes_json TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    decision_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    FOREIGN KEY(candidate_id) REFERENCES knowledge_candidates(id)
);

CREATE TABLE IF NOT EXISTS entities (
    id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    entity_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(tenant_id, id)
);
CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(tenant_id, kind, normalized_name);
CREATE INDEX IF NOT EXISTS idx_entities_fuzzy_prefix ON entities(tenant_id, normalized_name, kind);

CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    raw_value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,
    entity_ref TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    method TEXT NOT NULL,
    review_state TEXT NOT NULL,
    lifecycle_status TEXT NOT NULL,
    alias_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(tenant_id, alias_id),
    FOREIGN KEY(tenant_id, entity_ref) REFERENCES entities(tenant_id, id)
);
CREATE INDEX IF NOT EXISTS idx_entity_alias_value ON entity_aliases(tenant_id, normalized_value);

CREATE TABLE IF NOT EXISTS entity_resolution_attempts (
    attempt_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL DEFAULT '',
    raw_value TEXT NOT NULL,
    expected_kind TEXT NOT NULL DEFAULT '',
    scope_json TEXT NOT NULL,
    status TEXT NOT NULL,
    selected_entity_ref TEXT NOT NULL DEFAULT '',
    candidate_entities_json TEXT NOT NULL,
    method TEXT NOT NULL DEFAULT '',
    reason_codes_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_propositions (
    proposition_key TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    subject_ref TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_ref TEXT NOT NULL,
    concept_ref TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    proposition_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY(tenant_id, proposition_key)
);
CREATE INDEX IF NOT EXISTS idx_propositions_conflict_lookup
    ON knowledge_propositions(tenant_id, kind, subject_ref, predicate);

CREATE TABLE IF NOT EXISTS proposition_candidates (
    proposition_key TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    lineage_group TEXT NOT NULL,
    independence_class TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY(proposition_key, candidate_id),
    FOREIGN KEY(candidate_id) REFERENCES knowledge_candidates(id)
);

CREATE TABLE IF NOT EXISTS knowledge_conflicts (
    conflict_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    left_proposition_key TEXT NOT NULL,
    right_proposition_key TEXT NOT NULL,
    conflict_kind TEXT NOT NULL,
    resolution_status TEXT NOT NULL,
    severity TEXT NOT NULL,
    conflict_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    resolved_at REAL
);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_status ON knowledge_conflicts(tenant_id, resolution_status);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_left_status
    ON knowledge_conflicts(tenant_id, left_proposition_key, resolution_status);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_right_status
    ON knowledge_conflicts(tenant_id, right_proposition_key, resolution_status);

CREATE TABLE IF NOT EXISTS knowledge_migrations (
    migration_name TEXT PRIMARY KEY,
    completed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS corroboration_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    proposition_key TEXT NOT NULL,
    raw_source_count INTEGER NOT NULL,
    independent_source_count INTEGER NOT NULL,
    independent_family_count INTEGER NOT NULL,
    status TEXT NOT NULL,
    source_summary_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS operational_knowledge (
    knowledge_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    proposition_key TEXT NOT NULL,
    current_revision INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(tenant_id, knowledge_id),
    UNIQUE(tenant_id, proposition_key)
);
CREATE INDEX IF NOT EXISTS idx_operational_knowledge_active_page
    ON operational_knowledge(tenant_id, status, knowledge_id, current_revision);

CREATE TABLE IF NOT EXISTS operational_knowledge_revisions (
    knowledge_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    parent_revision INTEGER,
    schema_version TEXT NOT NULL,
    proposition_key TEXT NOT NULL,
    scope_json TEXT NOT NULL,
    review_state TEXT NOT NULL,
    lifecycle_status TEXT NOT NULL,
    eligibility TEXT NOT NULL,
    corroboration_snapshot_ref TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    revision_reason TEXT NOT NULL,
    content_json TEXT NOT NULL,
    semantic_fingerprint TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY(tenant_id, knowledge_id, revision),
    FOREIGN KEY(tenant_id, knowledge_id) REFERENCES operational_knowledge(tenant_id, knowledge_id)
);

CREATE TABLE IF NOT EXISTS knowledge_current_scope_refs (
    tenant_id TEXT NOT NULL,
    knowledge_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    dimension TEXT NOT NULL,
    scope_ref TEXT NOT NULL,
    PRIMARY KEY(tenant_id, knowledge_id, dimension, scope_ref),
    FOREIGN KEY(tenant_id, knowledge_id) REFERENCES operational_knowledge(tenant_id, knowledge_id)
);
CREATE INDEX IF NOT EXISTS idx_knowledge_current_scope_lookup
    ON knowledge_current_scope_refs(tenant_id, dimension, scope_ref, knowledge_id, revision);

CREATE TABLE IF NOT EXISTS knowledge_current_contributors (
    tenant_id TEXT NOT NULL,
    knowledge_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    candidate_id TEXT NOT NULL,
    PRIMARY KEY(tenant_id, knowledge_id, candidate_id),
    FOREIGN KEY(tenant_id, knowledge_id) REFERENCES operational_knowledge(tenant_id, knowledge_id)
);
CREATE INDEX IF NOT EXISTS idx_knowledge_current_contributor_lookup
    ON knowledge_current_contributors(tenant_id, candidate_id, knowledge_id, revision);

CREATE TABLE IF NOT EXISTS candidate_promotions (
    promotion_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    knowledge_id TEXT NOT NULL,
    knowledge_revision INTEGER NOT NULL,
    decision_ref TEXT NOT NULL,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    snapshot_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    UNIQUE(tenant_id, fingerprint)
);

CREATE TABLE IF NOT EXISTS knowledge_usage_events (
    usage_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    investigation_id TEXT NOT NULL,
    investigation_revision INTEGER NOT NULL,
    knowledge_id TEXT NOT NULL,
    knowledge_revision INTEGER NOT NULL,
    disposition TEXT NOT NULL,
    used_for_json TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    score_delta REAL NOT NULL,
    decision_ref TEXT NOT NULL,
    usage_json TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_item ON knowledge_usage_events(tenant_id, knowledge_id, created_at);
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_impact
    ON knowledge_usage_events(
        tenant_id, knowledge_id, disposition, investigation_id, investigation_revision, created_at DESC
    );
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_investigation
    ON knowledge_usage_events(tenant_id, investigation_id, investigation_revision);

CREATE TABLE IF NOT EXISTS knowledge_corrections (
    correction_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    investigation_id TEXT NOT NULL,
    investigation_revision INTEGER NOT NULL,
    correction_type TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    applied_knowledge_ref TEXT NOT NULL DEFAULT '',
    applied_knowledge_revision INTEGER,
    review_state TEXT NOT NULL,
    candidate_ref TEXT NOT NULL,
    correction_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_knowledge_corrections_candidate
    ON knowledge_corrections(tenant_id, candidate_ref);

CREATE TABLE IF NOT EXISTS knowledge_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    knowledge_kind TEXT NOT NULL DEFAULT '',
    policy_version TEXT NOT NULL DEFAULT '',
    source_family TEXT NOT NULL DEFAULT '',
    review_state TEXT NOT NULL DEFAULT '',
    lifecycle_status TEXT NOT NULL DEFAULT '',
    eligibility TEXT NOT NULL DEFAULT '',
    reason_code TEXT NOT NULL DEFAULT '',
    subject_ref TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_knowledge_events_tenant ON knowledge_events(tenant_id, created_at);
"""


def _db_path() -> Path:
    configured = getattr(settings, "signals_db_path", None)
    if configured:
        path = Path(configured)
    else:
        from tacit.signals import get_signal_store

        path = get_signal_store()._db_path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _ts(value) -> float:
    return value.timestamp()


def _conflict_lookup_sql(*, unresolved_only: bool) -> str:
    status_clause = " AND resolution_status='unresolved'" if unresolved_only else ""
    return f"""SELECT conflict_json, created_at FROM knowledge_conflicts
               WHERE tenant_id=? AND left_proposition_key=?{status_clause}
               UNION ALL
               SELECT conflict_json, created_at FROM knowledge_conflicts
               WHERE tenant_id=? AND right_proposition_key=?{status_clause}
               ORDER BY created_at DESC"""


class KnowledgeRepository:
    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or _db_path()
        self._transaction_connection: ContextVar[sqlite3.Connection | None] = ContextVar(
            f"knowledge_transaction_{id(self)}",
            default=None,
        )
        self._ensure_schema()
        self._run_review_priority_migration()
        self._run_correction_projection_migration()
        self._run_candidate_provenance_migration()
        self._run_current_scope_projection_migration()
        self._run_current_contributor_projection_migration()
        self._run_conflict_lineage_migration()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        conn = sqlite3.connect(str(self._db_path), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run repository writes in one immediate, nestable SQLite transaction."""
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
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        """Keep a multi-query read on one SQLite snapshot without taking a write lock."""
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

    def _ensure_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(SCHEMA_SQL)
            # executescript commits before returning. Keep the small schema shape
            # changes atomic; derived data is rebuilt separately in bounded batches.
            conn.execute("BEGIN IMMEDIATE")
            candidate_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(knowledge_candidates)").fetchall()
            }
            if "review_priority" not in candidate_columns:
                conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN review_priority INTEGER NOT NULL DEFAULT 0")
            if "has_unresolved_conflict" not in candidate_columns:
                conn.execute("""ALTER TABLE knowledge_candidates
                       ADD COLUMN has_unresolved_conflict INTEGER NOT NULL DEFAULT 0""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_kc_review_queue
                   ON knowledge_candidates(tenant_id, review_state, review_priority DESC, id)""")
            correction_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(knowledge_corrections)").fetchall()
            }
            missing_applied_ref = "applied_knowledge_ref" not in correction_columns
            missing_applied_revision = "applied_knowledge_revision" not in correction_columns
            if missing_applied_ref:
                conn.execute(
                    "ALTER TABLE knowledge_corrections ADD COLUMN applied_knowledge_ref TEXT NOT NULL DEFAULT ''"
                )
            if missing_applied_revision:
                conn.execute("ALTER TABLE knowledge_corrections ADD COLUMN applied_knowledge_revision INTEGER")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_knowledge_corrections_target
                   ON knowledge_corrections(tenant_id, target_ref)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_knowledge_corrections_applied
                   ON knowledge_corrections(tenant_id, applied_knowledge_ref)""")
        logger.info("knowledge_repository_init", db_path=str(self._db_path))

    def _run_review_priority_migration(self) -> None:
        """Backfill queue priority without holding one database-wide write lock."""
        migration_name = "candidate_review_priority_v2"
        migrated = 0
        batch_count = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
                    (migration_name,),
                ).fetchone():
                    return
                rows = conn.execute(
                    """SELECT candidate.id, candidate.tenant_id, candidate.candidate_json,
                              (
                                EXISTS (
                                  SELECT 1 FROM knowledge_conflicts conflict
                                  WHERE conflict.tenant_id=candidate.tenant_id
                                    AND conflict.left_proposition_key=candidate.proposition_key
                                    AND conflict.resolution_status='unresolved'
                                ) OR EXISTS (
                                  SELECT 1 FROM knowledge_conflicts conflict
                                  WHERE conflict.tenant_id=candidate.tenant_id
                                    AND conflict.right_proposition_key=candidate.proposition_key
                                    AND conflict.resolution_status='unresolved'
                                )
                              ) AS has_unresolved_conflict
                       FROM knowledge_candidates candidate
                       WHERE candidate.tenant_id>?
                          OR (candidate.tenant_id=? AND candidate.id>?)
                       ORDER BY candidate.tenant_id, candidate.id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    conn.execute(
                        "INSERT INTO knowledge_migrations (migration_name, completed_at) VALUES (?, ?)",
                        (migration_name, time.time()),
                    )
                    break
                batch_count += 1
                updates: list[tuple[int, int, str, str]] = []
                for row in rows:
                    try:
                        candidate = KnowledgeCandidate.model_validate_json(row["candidate_json"])
                    except ValueError:
                        logger.warning(
                            "knowledge_review_priority_backfill_skipped",
                            candidate_id=row["id"],
                            reason="invalid_candidate_json",
                        )
                        continue
                    updates.append(
                        (
                            _candidate_review_priority(
                                candidate,
                                unresolved_conflict=bool(row["has_unresolved_conflict"]),
                            ),
                            int(bool(row["has_unresolved_conflict"])),
                            str(row["id"]),
                            str(row["tenant_id"]),
                        )
                    )
                if updates:
                    conn.executemany(
                        """UPDATE knowledge_candidates
                           SET review_priority=?, has_unresolved_conflict=?
                           WHERE id=? AND tenant_id=?""",
                        updates,
                    )
                migrated += len(updates)
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["id"])
                logger.info(
                    "knowledge_review_priority_backfill_batch",
                    batch_size=len(rows),
                    migrated=len(updates),
                    batch_count=batch_count,
                )
        if migrated:
            logger.info(
                "knowledge_review_priority_backfilled",
                migrated=migrated,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    def _run_correction_projection_migration(self) -> None:
        """Backfill correction lookup columns in bounded idempotent batches."""
        migration_name = "correction_applied_projection_v1"
        migrated = 0
        batch_count = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
                    (migration_name,),
                ).fetchone():
                    return
                rows = conn.execute(
                    """SELECT tenant_id, correction_id, correction_json
                       FROM knowledge_corrections
                       WHERE tenant_id>? OR (tenant_id=? AND correction_id>?)
                       ORDER BY tenant_id, correction_id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    conn.execute(
                        "INSERT INTO knowledge_migrations (migration_name, completed_at) VALUES (?, ?)",
                        (migration_name, time.time()),
                    )
                    break
                batch_count += 1
                for row in rows:
                    try:
                        correction = KnowledgeCorrection.model_validate_json(row["correction_json"])
                    except ValueError:
                        logger.warning(
                            "knowledge_correction_backfill_skipped",
                            correction_id=row["correction_id"],
                            reason="invalid_correction_json",
                        )
                        continue
                    if correction.applied_knowledge_ref:
                        conn.execute(
                            """UPDATE knowledge_corrections
                               SET applied_knowledge_ref=?, applied_knowledge_revision=?
                               WHERE tenant_id=? AND correction_id=?""",
                            (
                                correction.applied_knowledge_ref,
                                correction.applied_knowledge_revision,
                                correction.tenant_id,
                                correction.id,
                            ),
                        )
                        migrated += 1
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["correction_id"])
        if migrated:
            logger.info(
                "knowledge_correction_projection_backfilled",
                correction_count=migrated,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    @staticmethod
    def _replace_candidate_provenance(conn: sqlite3.Connection, candidate: KnowledgeCandidate) -> None:
        families = {provenance_ref: "unknown" for provenance_ref in candidate.provenance_refs}
        for evidence in candidate.evidence.items:
            for provenance_ref in evidence.provenance_refs:
                families[provenance_ref] = evidence.source_family.value
        conn.execute(
            "DELETE FROM knowledge_candidate_provenance WHERE tenant_id=? AND candidate_id=?",
            (candidate.tenant_id, candidate.id),
        )
        conn.executemany(
            """INSERT INTO knowledge_candidate_provenance (
                   tenant_id, candidate_id, provenance_ref, source_family
               ) VALUES (?, ?, ?, ?)""",
            [
                (candidate.tenant_id, candidate.id, provenance_ref, source_family)
                for provenance_ref, source_family in sorted(families.items())
            ],
        )

    def _run_candidate_provenance_migration(self) -> None:
        migration_name = "candidate_provenance_index_v1"
        migrated = 0
        batch_count = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
                    (migration_name,),
                ).fetchone():
                    return
                rows = conn.execute(
                    """SELECT tenant_id, id, candidate_json FROM knowledge_candidates
                       WHERE tenant_id>? OR (tenant_id=? AND id>?)
                       ORDER BY tenant_id, id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    conn.execute(
                        "INSERT INTO knowledge_migrations (migration_name, completed_at) VALUES (?, ?)",
                        (migration_name, time.time()),
                    )
                    break
                batch_count += 1
                for row in rows:
                    try:
                        candidate = KnowledgeCandidate.model_validate_json(row["candidate_json"])
                    except ValueError:
                        logger.warning(
                            "candidate_provenance_backfill_skipped",
                            candidate_id=row["id"],
                            reason="invalid_candidate_json",
                        )
                        continue
                    self._replace_candidate_provenance(conn, candidate)
                    migrated += 1
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["id"])
        if migrated:
            logger.info(
                "candidate_provenance_index_backfilled",
                candidate_count=migrated,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    @staticmethod
    def _replace_current_scope_refs(conn: sqlite3.Connection, revision: KnowledgeRevision) -> None:
        conn.execute(
            "DELETE FROM knowledge_current_scope_refs WHERE tenant_id=? AND knowledge_id=?",
            (revision.tenant_id, revision.knowledge_id),
        )
        rows = [
            (
                revision.tenant_id,
                revision.knowledge_id,
                revision.revision,
                field_name,
                scope_ref,
            )
            for field_name in _SCOPE_REFERENCE_FIELDS
            for scope_ref in getattr(revision.scope, field_name)
        ]
        if rows:
            conn.executemany(
                """INSERT INTO knowledge_current_scope_refs (
                       tenant_id, knowledge_id, revision, dimension, scope_ref
                   ) VALUES (?, ?, ?, ?, ?)""",
                rows,
            )

    def _run_current_scope_projection_migration(self) -> None:
        migration_name = "current_knowledge_scope_projection_v1"
        migrated = 0
        batch_count = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
                    (migration_name,),
                ).fetchone():
                    return
                rows = conn.execute(
                    """SELECT current.tenant_id, current.knowledge_id, revision.content_json
                       FROM operational_knowledge current
                       JOIN operational_knowledge_revisions revision
                         ON revision.tenant_id=current.tenant_id
                        AND revision.knowledge_id=current.knowledge_id
                        AND revision.revision=current.current_revision
                       WHERE current.tenant_id>?
                          OR (current.tenant_id=? AND current.knowledge_id>?)
                       ORDER BY current.tenant_id, current.knowledge_id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    conn.execute(
                        "INSERT INTO knowledge_migrations (migration_name, completed_at) VALUES (?, ?)",
                        (migration_name, time.time()),
                    )
                    break
                batch_count += 1
                for row in rows:
                    try:
                        revision = KnowledgeRevision.model_validate_json(row["content_json"])
                    except ValueError:
                        logger.warning(
                            "knowledge_scope_projection_backfill_skipped",
                            knowledge_id=row["knowledge_id"],
                            reason="invalid_revision_json",
                        )
                        continue
                    self._replace_current_scope_refs(conn, revision)
                    migrated += 1
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["knowledge_id"])
        if migrated:
            logger.info(
                "knowledge_scope_projection_backfilled",
                revision_count=migrated,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    @staticmethod
    def _replace_current_contributors(conn: sqlite3.Connection, revision: KnowledgeRevision) -> None:
        conn.execute(
            "DELETE FROM knowledge_current_contributors WHERE tenant_id=? AND knowledge_id=?",
            (revision.tenant_id, revision.knowledge_id),
        )
        if revision.promoted_from_candidate_refs:
            conn.executemany(
                """INSERT INTO knowledge_current_contributors (
                       tenant_id, knowledge_id, revision, candidate_id
                   ) VALUES (?, ?, ?, ?)""",
                [
                    (revision.tenant_id, revision.knowledge_id, revision.revision, candidate_id)
                    for candidate_id in sorted(set(revision.promoted_from_candidate_refs))
                ],
            )

    def _run_current_contributor_projection_migration(self) -> None:
        migration_name = "current_knowledge_contributor_projection_v1"
        migrated = 0
        batch_count = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
                    (migration_name,),
                ).fetchone():
                    return
                rows = conn.execute(
                    """SELECT current.tenant_id, current.knowledge_id, revision.content_json
                       FROM operational_knowledge current
                       JOIN operational_knowledge_revisions revision
                         ON revision.tenant_id=current.tenant_id
                        AND revision.knowledge_id=current.knowledge_id
                        AND revision.revision=current.current_revision
                       WHERE current.tenant_id>?
                          OR (current.tenant_id=? AND current.knowledge_id>?)
                       ORDER BY current.tenant_id, current.knowledge_id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    conn.execute(
                        "INSERT INTO knowledge_migrations (migration_name, completed_at) VALUES (?, ?)",
                        (migration_name, time.time()),
                    )
                    break
                batch_count += 1
                for row in rows:
                    try:
                        revision = KnowledgeRevision.model_validate_json(row["content_json"])
                    except ValueError:
                        logger.warning(
                            "knowledge_contributor_projection_backfill_skipped",
                            knowledge_id=row["knowledge_id"],
                            reason="invalid_revision_json",
                        )
                        continue
                    self._replace_current_contributors(conn, revision)
                    migrated += 1
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["knowledge_id"])
        if migrated:
            logger.info(
                "knowledge_contributor_projection_backfilled",
                revision_count=migrated,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    def _run_conflict_lineage_migration(self) -> None:
        """Resolve legacy conflicts whose reviewed side has no independent support."""
        migration_name = "resolve_conflicts_without_independent_support_v1"
        dependent_lineages = (
            "copied_from",
            "generated_from",
            "same_vendor_export",
            "same_source_revision",
        )
        placeholders = ", ".join("?" for _ in dependent_lineages)
        resolved_count = 0
        batch_count = 0
        after_tenant = ""
        after_id = ""
        while True:
            with self.transaction() as conn:
                if conn.execute(
                    "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
                    (migration_name,),
                ).fetchone():
                    return
                rows = conn.execute(
                    """SELECT conflict_id, tenant_id, left_proposition_key,
                              right_proposition_key, conflict_json
                       FROM knowledge_conflicts
                       WHERE resolution_status='unresolved'
                         AND (tenant_id>? OR (tenant_id=? AND conflict_id>?))
                       ORDER BY tenant_id, conflict_id LIMIT ?""",
                    (after_tenant, after_tenant, after_id, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    conn.execute(
                        "INSERT INTO knowledge_migrations (migration_name, completed_at) VALUES (?, ?)",
                        (migration_name, time.time()),
                    )
                    break
                batch_count += 1
                keys_by_tenant: dict[str, set[str]] = {}
                for row in rows:
                    keys_by_tenant.setdefault(str(row["tenant_id"]), set()).update(
                        {str(row["left_proposition_key"]), str(row["right_proposition_key"])}
                    )
                unsupported_by_tenant: dict[str, set[str]] = {}
                for tenant_id, proposition_keys in keys_by_tenant.items():
                    key_placeholders = ", ".join("?" for _ in proposition_keys)
                    selected = conn.execute(
                        f"""SELECT DISTINCT pc.proposition_key
                           FROM proposition_candidates pc
                           JOIN knowledge_candidates c
                             ON c.id=pc.candidate_id AND c.tenant_id=pc.tenant_id
                            AND c.proposition_key=pc.proposition_key
                           WHERE pc.tenant_id=?
                             AND pc.proposition_key IN ({key_placeholders})
                             AND c.review_state IN ('approved', 'trusted')
                             AND c.lifecycle_status='active'
                             AND c.entity_resolution_status='resolved'
                             AND NOT EXISTS (
                               SELECT 1 FROM proposition_candidates supported_pc
                               JOIN knowledge_candidates supported_candidate
                                 ON supported_candidate.id=supported_pc.candidate_id
                                AND supported_candidate.tenant_id=supported_pc.tenant_id
                                AND supported_candidate.proposition_key=supported_pc.proposition_key
                               JOIN knowledge_candidate_evidence evidence
                                 ON evidence.candidate_id=supported_candidate.id
                                AND evidence.tenant_id=supported_candidate.tenant_id
                               WHERE supported_pc.tenant_id=pc.tenant_id
                                 AND supported_pc.proposition_key=pc.proposition_key
                                 AND supported_candidate.review_state IN ('approved', 'trusted')
                                 AND supported_candidate.lifecycle_status='active'
                                 AND supported_candidate.entity_resolution_status='resolved'
                                 AND evidence.evidence_role='supporting'
                                 AND evidence.lineage_kind NOT IN ({placeholders})
                             )""",
                        (tenant_id, *sorted(proposition_keys), *dependent_lineages),
                    ).fetchall()
                    unsupported_by_tenant[tenant_id] = {
                        str(selected_row["proposition_key"]) for selected_row in selected
                    }
                for row in rows:
                    unsupported = unsupported_by_tenant[str(row["tenant_id"])]
                    if not unsupported.intersection(
                        {str(row["left_proposition_key"]), str(row["right_proposition_key"])}
                    ):
                        continue
                    conflict = KnowledgeConflict.model_validate_json(row["conflict_json"])
                    resolved = conflict.model_copy(
                        update={
                            "resolution_status": ConflictResolutionStatus.RESOLVED_BY_REVIEW,
                            "resolution_reason": "counter_proposition_lacks_independent_support",
                            "resolved_by": "system:lineage-policy",
                            "resolved_at": utc_now(),
                            "severity": "low",
                        }
                    )
                    self.save_conflict(resolved)
                    self.append_event(
                        "conflict_resolved",
                        tenant_id=resolved.tenant_id,
                        subject_ref=resolved.id,
                        dimensions={"reason_code": resolved.resolution_reason},
                        payload={"resolved_by": resolved.resolved_by},
                    )
                    resolved_count += 1
                after_tenant = str(rows[-1]["tenant_id"])
                after_id = str(rows[-1]["conflict_id"])
        if resolved_count:
            logger.info(
                "dependent_lineage_conflicts_reconciled",
                count=resolved_count,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    def append_event(
        self,
        event_type: str,
        *,
        tenant_id: str = "default",
        subject_ref: str = "",
        dimensions: dict[str, str] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str:
        dimensions = dimensions or {}
        event_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO knowledge_events (
                   event_id, tenant_id, event_type, knowledge_kind, policy_version, source_family,
                   review_state, lifecycle_status, eligibility, reason_code, subject_ref, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    tenant_id,
                    event_type,
                    dimensions.get("knowledge_kind", ""),
                    dimensions.get("policy_version", ""),
                    dimensions.get("source_family", ""),
                    dimensions.get("review_state", ""),
                    dimensions.get("lifecycle_status", ""),
                    dimensions.get("eligibility", ""),
                    dimensions.get("reason_code", ""),
                    subject_ref,
                    json.dumps(payload or {}, sort_keys=True),
                    time.time(),
                ),
            )
        return event_id

    def list_events(self, tenant_id: str = "default", limit: int = 200) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM knowledge_events WHERE tenant_id=? ORDER BY created_at DESC LIMIT ?",
                (tenant_id, limit),
            ).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]

    def save_candidate(
        self,
        candidate: KnowledgeCandidate,
        *,
        expected: KnowledgeCandidate | None | object = _UNSET,
    ) -> KnowledgeCandidate:
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT tenant_id, candidate_json FROM knowledge_candidates WHERE id=?",
                (candidate.id,),
            ).fetchone()
            if existing and existing["tenant_id"] != candidate.tenant_id:
                raise ValueError("candidate id already belongs to another tenant")
            if expected is not _UNSET:
                matches_expected = (
                    existing is not None
                    and isinstance(expected, KnowledgeCandidate)
                    and _candidate_matches_json(str(existing["candidate_json"]), expected)
                )
                if not matches_expected and not (existing is None and expected is None):
                    raise CandidateMergeConflictError("candidate changed during re-ingestion; retry the merge")
            policy_version = (
                candidate.policy.promotion_policy_ref.rsplit("-", 1)[-1]
                if candidate.policy.promotion_policy_ref
                else ""
            )
            unresolved_conflict = _has_unresolved_conflict(
                conn,
                candidate.tenant_id,
                candidate.proposition.proposition_key,
            )
            review_priority = _candidate_review_priority(
                candidate,
                unresolved_conflict=unresolved_conflict,
            )
            if existing is None:
                conn.execute(
                    """INSERT INTO knowledge_candidates (
                       id, tenant_id, kind, payload_ref, proposition_key, scope_json, review_state,
                       lifecycle_status, eligibility, entity_resolution_status, promotion_policy_id,
                       promotion_policy_version, review_priority, has_unresolved_conflict,
                       candidate_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        candidate.id,
                        candidate.tenant_id,
                        candidate.kind.value,
                        candidate.payload_ref,
                        candidate.proposition.proposition_key,
                        candidate.scope.model_dump_json(),
                        candidate.state.review_state.value,
                        candidate.state.lifecycle_status.value,
                        candidate.state.eligibility.value,
                        candidate.entity_resolution.status.value,
                        candidate.policy.promotion_policy_ref,
                        policy_version,
                        review_priority,
                        int(unresolved_conflict),
                        candidate.model_dump_json(),
                        _ts(candidate.created_at),
                        _ts(candidate.updated_at),
                    ),
                )
            else:
                conn.execute(
                    """UPDATE knowledge_candidates SET
                           proposition_key=?, scope_json=?, review_state=?, lifecycle_status=?,
                           eligibility=?, entity_resolution_status=?, promotion_policy_id=?,
                           promotion_policy_version=?, review_priority=?, has_unresolved_conflict=?,
                           candidate_json=?, updated_at=?
                       WHERE id=? AND tenant_id=?""",
                    (
                        candidate.proposition.proposition_key,
                        candidate.scope.model_dump_json(),
                        candidate.state.review_state.value,
                        candidate.state.lifecycle_status.value,
                        candidate.state.eligibility.value,
                        candidate.entity_resolution.status.value,
                        candidate.policy.promotion_policy_ref,
                        policy_version,
                        review_priority,
                        int(unresolved_conflict),
                        candidate.model_dump_json(),
                        _ts(candidate.updated_at),
                        candidate.id,
                        candidate.tenant_id,
                    ),
                )
            conn.execute("DELETE FROM knowledge_candidate_evidence WHERE candidate_id=?", (candidate.id,))
            for evidence in candidate.evidence.items:
                conn.execute(
                    """INSERT INTO knowledge_candidate_evidence (
                       candidate_id, tenant_id, evidence_ref, evidence_role, source_family,
                       lineage_group, lineage_kind, evidence_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        candidate.id,
                        candidate.tenant_id,
                        evidence.evidence_ref,
                        evidence.evidence_role.value,
                        evidence.source_family.value,
                        evidence.lineage_group,
                        evidence.lineage_kind.value,
                        evidence.model_dump_json(),
                        time.time(),
                    ),
                )
            self._replace_candidate_provenance(conn, candidate)
        return candidate

    def save_candidate_with_proposition(
        self,
        candidate: KnowledgeCandidate,
        *,
        lineage_group: str,
        independence_class: str,
        expected: KnowledgeCandidate | None | object = _UNSET,
    ) -> KnowledgeCandidate:
        """Commit a candidate and its proposition membership as one unit."""
        with self.transaction():
            self.save_candidate(candidate, expected=expected)
            self.save_proposition(candidate, lineage_group, independence_class)
        return candidate

    def get_candidate(self, candidate_id: str, tenant_id: str = "default") -> KnowledgeCandidate | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate_id, tenant_id),
            ).fetchone()
        return KnowledgeCandidate.model_validate_json(row["candidate_json"]) if row else None

    def save_candidate_evaluation(
        self,
        candidate: KnowledgeCandidate,
        *,
        expected: KnowledgeCandidate,
    ) -> KnowledgeCandidate:
        """Persist policy output only if no reviewer or lifecycle writer won first."""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate.id, candidate.tenant_id),
            ).fetchone()
            if existing is None or not _candidate_matches_json(str(existing["candidate_json"]), expected):
                logger.info(
                    "knowledge_candidate_evaluation_conflict",
                    tenant_id=candidate.tenant_id,
                    candidate_id=candidate.id,
                    phase="policy_state_persist",
                )
                raise CandidateEvaluationConflictError(
                    "candidate changed during policy evaluation; reload before evaluating"
                )
            cursor = conn.execute(
                """UPDATE knowledge_candidates SET
                       review_state=?, lifecycle_status=?, eligibility=?,
                       promotion_policy_id=?, promotion_policy_version=?,
                       candidate_json=?, updated_at=?
                   WHERE id=? AND tenant_id=?""",
                (
                    candidate.state.review_state.value,
                    candidate.state.lifecycle_status.value,
                    candidate.state.eligibility.value,
                    candidate.policy.promotion_policy_ref,
                    (
                        candidate.policy.promotion_policy_ref.rsplit("-", 1)[-1]
                        if candidate.policy.promotion_policy_ref
                        else ""
                    ),
                    candidate.model_dump_json(),
                    _ts(candidate.updated_at),
                    candidate.id,
                    candidate.tenant_id,
                ),
            )
            if cursor.rowcount != 1:
                logger.info(
                    "knowledge_candidate_evaluation_conflict",
                    tenant_id=candidate.tenant_id,
                    candidate_id=candidate.id,
                    phase="policy_state_persist",
                )
                raise CandidateEvaluationConflictError(
                    "candidate changed during policy evaluation; reload before evaluating"
                )
        return candidate

    def transition_candidate_lifecycle(
        self,
        candidate: KnowledgeCandidate,
        *,
        expected: KnowledgeCandidate,
    ) -> KnowledgeCandidate:
        """Persist a source lifecycle transition without overwriting reviewer state."""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate.id, candidate.tenant_id),
            ).fetchone()
            if existing is None or not _candidate_matches_json(str(existing["candidate_json"]), expected):
                logger.info(
                    "knowledge_candidate_lifecycle_conflict",
                    tenant_id=candidate.tenant_id,
                    candidate_id=candidate.id,
                    expected_review_state=expected.state.review_state.value,
                    expected_lifecycle_status=expected.state.lifecycle_status.value,
                )
                raise CandidateLifecycleConflictError("candidate changed during source lifecycle reconciliation")
            cursor = conn.execute(
                """UPDATE knowledge_candidates SET
                       review_state=?, lifecycle_status=?, eligibility=?, candidate_json=?, updated_at=?
                   WHERE id=? AND tenant_id=?""",
                (
                    candidate.state.review_state.value,
                    candidate.state.lifecycle_status.value,
                    candidate.state.eligibility.value,
                    candidate.model_dump_json(),
                    _ts(candidate.updated_at),
                    candidate.id,
                    candidate.tenant_id,
                ),
            )
            if cursor.rowcount != 1:
                logger.info(
                    "knowledge_candidate_lifecycle_conflict",
                    tenant_id=candidate.tenant_id,
                    candidate_id=candidate.id,
                    expected_review_state=expected.state.review_state.value,
                    expected_lifecycle_status=expected.state.lifecycle_status.value,
                )
                raise CandidateLifecycleConflictError("candidate changed during source lifecycle reconciliation")
        return candidate

    def transition_candidate_review(
        self,
        candidate: KnowledgeCandidate,
        *,
        expected: KnowledgeCandidate,
    ) -> KnowledgeCandidate:
        """Apply a review only if no reviewer or lifecycle writer won first."""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate.id, candidate.tenant_id),
            ).fetchone()
            if existing is None or not _candidate_matches_json(str(existing["candidate_json"]), expected):
                logger.info(
                    "knowledge_candidate_review_conflict",
                    tenant_id=candidate.tenant_id,
                    candidate_id=candidate.id,
                    expected_review_state=expected.state.review_state.value,
                    expected_lifecycle_status=expected.state.lifecycle_status.value,
                )
                raise CandidateReviewConflictError("candidate changed; reload before reviewing")
            cursor = conn.execute(
                """UPDATE knowledge_candidates
                   SET review_state=?, lifecycle_status=?, eligibility=?, candidate_json=?, updated_at=?
                   WHERE id=? AND tenant_id=?""",
                (
                    candidate.state.review_state.value,
                    candidate.state.lifecycle_status.value,
                    candidate.state.eligibility.value,
                    candidate.model_dump_json(),
                    _ts(candidate.updated_at),
                    candidate.id,
                    candidate.tenant_id,
                ),
            )
            if cursor.rowcount != 1:
                logger.info(
                    "knowledge_candidate_review_conflict",
                    tenant_id=candidate.tenant_id,
                    candidate_id=candidate.id,
                    expected_review_state=expected.state.review_state.value,
                    expected_lifecycle_status=expected.state.lifecycle_status.value,
                )
                raise CandidateReviewConflictError("candidate changed; reload before reviewing")
        return candidate

    def list_candidates(
        self,
        tenant_id: str = "default",
        *,
        kind: str | None = None,
        review_state: str | None = None,
        limit: int | None = 200,
    ) -> list[KnowledgeCandidate]:
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if kind:
            clauses.append("kind=?")
            params.append(kind)
        if review_state:
            clauses.append("review_state=?")
            params.append(review_state)
        limit_clause = ""
        if limit is not None:
            params.append(limit)
            limit_clause = " LIMIT ?"
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT candidate_json FROM knowledge_candidates WHERE {' AND '.join(clauses)} "
                f"ORDER BY created_at DESC{limit_clause}",
                params,
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def list_candidates_for_provenance(
        self,
        tenant_id: str,
        provenance_ref: str,
    ) -> list[KnowledgeCandidate]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT c.candidate_json
                   FROM knowledge_candidate_provenance p
                   JOIN knowledge_candidates c
                     ON c.tenant_id=p.tenant_id AND c.id=p.candidate_id
                   WHERE p.tenant_id=? AND p.provenance_ref=?
                   ORDER BY c.created_at""",
                (tenant_id, provenance_ref),
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def list_review_candidates(self, tenant_id: str, *, limit: int) -> list[KnowledgeCandidate]:
        """Return the highest-priority pending candidates with work bounded in SQL."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT c.candidate_json
                   FROM knowledge_candidates c
                   WHERE c.tenant_id=? AND c.review_state='candidate'
                   ORDER BY c.review_priority DESC, c.id
                   LIMIT ?""",
                (tenant_id, limit),
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def unresolved_proposition_keys(
        self,
        tenant_id: str,
        proposition_keys: set[str],
    ) -> set[str]:
        if not proposition_keys:
            return set()
        ordered_keys = sorted(proposition_keys)
        unresolved: set[str] = set()
        with self._conn() as conn:
            for start in range(0, len(ordered_keys), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_keys[start : start + _SQLITE_BIND_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"""SELECT left_proposition_key AS proposition_key
                        FROM knowledge_conflicts
                        WHERE tenant_id=? AND resolution_status='unresolved'
                          AND left_proposition_key IN ({placeholders})
                        UNION
                        SELECT right_proposition_key AS proposition_key
                        FROM knowledge_conflicts
                        WHERE tenant_id=? AND resolution_status='unresolved'
                          AND right_proposition_key IN ({placeholders})""",
                    [tenant_id, *batch, tenant_id, *batch],
                ).fetchall()
                unresolved.update(str(row["proposition_key"]) for row in rows)
        return unresolved

    def save_entity(self, entity: Entity) -> Entity:
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT kind FROM entities WHERE id=? AND tenant_id=?",
                (entity.id, entity.tenant_id),
            ).fetchone()
            if existing is not None and existing["kind"] != entity.kind.value:
                raise ValueError("entity kind cannot change for an existing entity id")
            conn.execute(
                """INSERT INTO entities (
                   id, tenant_id, kind, canonical_name, normalized_name, display_name, status,
                   scope_json, entity_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, id) DO UPDATE SET
                   canonical_name=excluded.canonical_name, normalized_name=excluded.normalized_name,
                   display_name=excluded.display_name, status=excluded.status,
                   scope_json=excluded.scope_json, entity_json=excluded.entity_json, updated_at=excluded.updated_at""",
                (
                    entity.id,
                    entity.tenant_id,
                    entity.kind.value,
                    entity.canonical_name,
                    normalize_entity(entity.canonical_name),
                    entity.display_name or entity.canonical_name,
                    entity.status.value,
                    entity.scope.model_dump_json(),
                    entity.model_dump_json(),
                    _ts(entity.created_at),
                    _ts(entity.updated_at),
                ),
            )
        return entity

    def save_alias(self, alias: EntityAlias) -> EntityAlias:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO entity_aliases (
                   alias_id, tenant_id, raw_value, normalized_value, entity_ref, scope_json,
                   method, review_state, lifecycle_status, alias_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, alias_id) DO UPDATE SET
                   raw_value=excluded.raw_value, normalized_value=excluded.normalized_value,
                   entity_ref=excluded.entity_ref, scope_json=excluded.scope_json, method=excluded.method,
                   review_state=excluded.review_state, lifecycle_status=excluded.lifecycle_status,
                   alias_json=excluded.alias_json, updated_at=excluded.updated_at""",
                (
                    alias.id,
                    alias.tenant_id,
                    alias.raw_value,
                    alias.normalized_value,
                    alias.entity_ref,
                    alias.scope.model_dump_json(),
                    alias.method.value,
                    alias.review_state.value,
                    alias.lifecycle_status.value,
                    alias.model_dump_json(),
                    _ts(alias.created_at),
                    _ts(alias.updated_at),
                ),
            )
        return alias

    def get_alias(self, alias_id: str, tenant_id: str = "default") -> EntityAlias | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT alias_json FROM entity_aliases WHERE tenant_id=? AND alias_id=?",
                (tenant_id, alias_id),
            ).fetchone()
        return EntityAlias.model_validate_json(row["alias_json"]) if row else None

    def find_entities(
        self,
        tenant_id: str,
        normalized_value: str,
        expected_kind: str | None = None,
        *,
        limit: int = 100,
    ) -> tuple[list[Entity], bool]:
        if limit < 1:
            return [], False
        params: list[Any] = [tenant_id, normalized_value]
        kind_clause = ""
        if expected_kind:
            kind_clause = " AND e.kind=?"
            params.append(expected_kind)
        with self._conn() as conn:
            params.append(limit + 1)
            rows = conn.execute(
                f"""SELECT e.entity_json FROM entities e
                    WHERE e.tenant_id=? AND e.status='active'
                      AND e.normalized_name=?{kind_clause}
                    ORDER BY e.id LIMIT ?""",
                params,
            ).fetchall()
        return (
            [Entity.model_validate_json(row["entity_json"]) for row in rows[:limit]],
            len(rows) > limit,
        )

    def list_entities(self, tenant_id: str = "default") -> list[Entity]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT entity_json FROM entities WHERE tenant_id=? ORDER BY canonical_name",
                (tenant_id,),
            ).fetchall()
        return [Entity.model_validate_json(row["entity_json"]) for row in rows]

    def get_entity(self, entity_id: str, tenant_id: str = "default") -> Entity | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT entity_json FROM entities WHERE tenant_id=? AND id=?",
                (tenant_id, entity_id),
            ).fetchone()
        return Entity.model_validate_json(row["entity_json"]) if row else None

    def find_aliases(self, tenant_id: str, normalized_value: str) -> list[EntityAlias]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT alias_json FROM entity_aliases
                   WHERE tenant_id=? AND normalized_value=?
                     AND review_state IN ('approved', 'trusted') AND lifecycle_status='active'""",
                (tenant_id, normalized_value),
            ).fetchall()
        return [EntityAlias.model_validate_json(row["alias_json"]) for row in rows]

    def find_alias_entities(
        self,
        tenant_id: str,
        normalized_value: str,
        expected_kind: str | None = None,
        *,
        limit: int = 100,
    ) -> tuple[list[tuple[EntityAlias, Entity]], bool]:
        """Resolve a bounded alias bucket in one query while preserving both scopes."""
        if limit < 1:
            return [], False
        params: list[Any] = [tenant_id, normalized_value]
        kind_clause = ""
        if expected_kind:
            kind_clause = " AND entity.kind=?"
            params.append(expected_kind)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT alias.alias_json, entity.entity_json
                    FROM entity_aliases alias
                    JOIN entities entity
                      ON entity.tenant_id=alias.tenant_id AND entity.id=alias.entity_ref
                    WHERE alias.tenant_id=? AND alias.normalized_value=?
                      AND alias.review_state IN ('approved', 'trusted')
                      AND alias.lifecycle_status='active' AND entity.status='active'{kind_clause}
                    ORDER BY alias.alias_id LIMIT ?""",
                [*params, limit + 1],
            ).fetchall()
        truncated = len(rows) > limit
        resolved = [
            (
                EntityAlias.model_validate_json(row["alias_json"]),
                Entity.model_validate_json(row["entity_json"]),
            )
            for row in rows[:limit]
        ]
        return resolved, truncated

    def find_fuzzy_entity_candidates(
        self,
        tenant_id: str,
        normalized_value: str,
        expected_kind: str | None = None,
        *,
        limit: int = 100,
    ) -> tuple[list[Entity], bool]:
        """Return a bounded indexed prefix bucket for non-authoritative fuzzy hints."""
        if not normalized_value or limit < 1:
            return [], False
        prefix = normalized_value[: min(2, len(normalized_value))]
        upper_bound = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        params: list[Any] = [tenant_id, prefix, upper_bound]
        kind_clause = ""
        if expected_kind:
            kind_clause = " AND kind=?"
            params.append(expected_kind)
        params.append(limit + 1)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT entity_json FROM entities
                    WHERE tenant_id=? AND status='active'
                      AND normalized_name>=? AND normalized_name<?{kind_clause}
                    ORDER BY normalized_name LIMIT ?""",
                params,
            ).fetchall()
        return (
            [Entity.model_validate_json(row["entity_json"]) for row in rows[:limit]],
            len(rows) > limit,
        )

    def record_resolution_attempt(
        self,
        result: EntityResolutionResult,
        scope_json: str,
        *,
        tenant_id: str,
        candidate_id: str = "",
        expected_kind: str = "",
    ) -> str:
        attempt_id = uuid.uuid4().hex
        method = result.candidate_bindings[0].method.value if len(result.candidate_bindings) == 1 else ""
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO entity_resolution_attempts (
                   attempt_id, tenant_id, candidate_id, raw_value, expected_kind, scope_json, status,
                   selected_entity_ref, candidate_entities_json, method, reason_codes_json, result_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    attempt_id,
                    tenant_id,
                    candidate_id,
                    result.raw_value,
                    expected_kind,
                    scope_json,
                    result.status.value,
                    result.selected_entity_ref,
                    json.dumps([item.entity_ref for item in result.candidate_bindings]),
                    method,
                    json.dumps(result.reason_codes),
                    result.model_dump_json(),
                    time.time(),
                ),
            )
        return attempt_id

    def save_proposition(self, candidate: KnowledgeCandidate, lineage_group: str, independence_class: str) -> None:
        proposition = candidate.proposition
        with self._conn() as conn:
            conn.execute(
                """DELETE FROM proposition_candidates
                   WHERE candidate_id=? AND tenant_id=? AND proposition_key!=?""",
                (candidate.id, candidate.tenant_id, proposition.proposition_key),
            )
            conn.execute(
                """INSERT OR IGNORE INTO knowledge_propositions (
                   proposition_key, tenant_id, kind, subject_ref, predicate, object_ref,
                   concept_ref, scope_json, proposition_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    proposition.proposition_key,
                    candidate.tenant_id,
                    candidate.kind.value,
                    proposition.subject_ref,
                    proposition.predicate.value,
                    proposition.object_ref,
                    proposition.concept_ref,
                    candidate.scope.model_dump_json(),
                    proposition.model_dump_json(),
                    time.time(),
                ),
            )
            conn.execute(
                """INSERT OR REPLACE INTO proposition_candidates (
                   proposition_key, candidate_id, tenant_id, lineage_group, independence_class, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    proposition.proposition_key,
                    candidate.id,
                    candidate.tenant_id,
                    lineage_group,
                    independence_class,
                    time.time(),
                ),
            )

    def candidates_for_proposition(self, tenant_id: str, proposition_key: str) -> list[KnowledgeCandidate]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT c.candidate_json FROM knowledge_candidates c
                   JOIN proposition_candidates p ON p.candidate_id=c.id
                   WHERE p.tenant_id=? AND p.proposition_key=? ORDER BY c.created_at""",
                (tenant_id, proposition_key),
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def list_propositions(
        self,
        tenant_id: str = "default",
        *,
        proposition_key: str | None = None,
        kind: str | None = None,
        subject_ref: str | None = None,
        predicates: set[str] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses = ["p.tenant_id=?"]
        params: list[Any] = [tenant_id]
        for column, value in (
            ("p.proposition_key", proposition_key),
            ("p.kind", kind),
            ("p.subject_ref", subject_ref),
        ):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        if predicates:
            clauses.append(f"p.predicate IN ({', '.join('?' for _ in predicates)})")
            params.extend(sorted(predicates))
        limit_clause = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            params.append(limit)
            limit_clause = " LIMIT ?"
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT p.* FROM knowledge_propositions p
                   WHERE {" AND ".join(clauses)} AND EXISTS (
                     SELECT 1 FROM proposition_candidates pc
                     JOIN knowledge_candidates c
                       ON c.id=pc.candidate_id AND c.tenant_id=pc.tenant_id
                      AND c.proposition_key=p.proposition_key
                     WHERE pc.tenant_id=p.tenant_id AND pc.proposition_key=p.proposition_key
                       AND c.review_state IN ('approved', 'trusted')
                       AND c.lifecycle_status = 'active'
                       AND c.entity_resolution_status = 'resolved'
                       AND EXISTS (
                         SELECT 1 FROM knowledge_candidate_evidence e
                         WHERE e.tenant_id=c.tenant_id AND e.candidate_id=c.id
                           AND e.evidence_role='supporting'
                           AND e.lineage_kind NOT IN (
                             'copied_from', 'generated_from', 'same_vendor_export', 'same_source_revision'
                           )
                       )
                   ) ORDER BY p.created_at, p.proposition_key{limit_clause}""",
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def save_corroboration(self, summary: CorroborationSummary, tenant_id: str) -> str:
        snapshot_id = f"corroboration_{uuid.uuid4().hex[:16]}"
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO corroboration_snapshots (
                   snapshot_id, tenant_id, proposition_key, raw_source_count, independent_source_count,
                   independent_family_count, status, source_summary_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot_id,
                    tenant_id,
                    summary.proposition_key,
                    summary.raw_source_count,
                    summary.independent_source_count,
                    summary.independent_source_family_count,
                    summary.status.value,
                    summary.model_dump_json(),
                    time.time(),
                ),
            )
        return snapshot_id

    def save_conflict(self, conflict: KnowledgeConflict) -> KnowledgeConflict:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO knowledge_conflicts (
                   conflict_id, tenant_id, left_proposition_key, right_proposition_key,
                   conflict_kind, resolution_status, severity, conflict_json, created_at, resolved_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(conflict_id) DO UPDATE SET
                   resolution_status=excluded.resolution_status, severity=excluded.severity,
                   conflict_json=excluded.conflict_json, resolved_at=excluded.resolved_at""",
                (
                    conflict.id,
                    conflict.tenant_id,
                    conflict.left_proposition_ref,
                    conflict.right_proposition_ref,
                    conflict.conflict_kind.value,
                    conflict.resolution_status.value,
                    conflict.severity,
                    conflict.model_dump_json(),
                    _ts(conflict.created_at),
                    _ts(conflict.resolved_at) if conflict.resolved_at else None,
                ),
            )
            for proposition_key in {
                conflict.left_proposition_ref,
                conflict.right_proposition_ref,
            }:
                unresolved = _has_unresolved_conflict(conn, conflict.tenant_id, proposition_key)
                if unresolved:
                    conn.execute(
                        """UPDATE knowledge_candidates
                           SET review_priority=review_priority +
                                 CASE WHEN has_unresolved_conflict=0 THEN 100 ELSE 0 END,
                               has_unresolved_conflict=1
                           WHERE tenant_id=? AND proposition_key=?""",
                        (conflict.tenant_id, proposition_key),
                    )
                else:
                    conn.execute(
                        """UPDATE knowledge_candidates
                           SET review_priority=MAX(
                                 0,
                                 review_priority - CASE WHEN has_unresolved_conflict=1 THEN 100 ELSE 0 END
                               ),
                               has_unresolved_conflict=0
                           WHERE tenant_id=? AND proposition_key=?""",
                        (conflict.tenant_id, proposition_key),
                    )
        return conflict

    def list_conflicts(
        self,
        tenant_id: str = "default",
        *,
        proposition_key: str | None = None,
        unresolved_only: bool = False,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeConflict]:
        params: list[str | int]
        if proposition_key:
            query = _conflict_lookup_sql(unresolved_only=unresolved_only)
            params = [tenant_id, proposition_key, tenant_id, proposition_key]
        else:
            clauses = ["tenant_id=?"]
            params = [tenant_id]
            if unresolved_only:
                clauses.append("resolution_status='unresolved'")
            query = (
                f"SELECT conflict_json FROM knowledge_conflicts WHERE {' AND '.join(clauses)} ORDER BY created_at DESC"
            )
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            query += " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [KnowledgeConflict.model_validate_json(row["conflict_json"]) for row in rows]

    def save_promotion_decision(self, decision: PromotionDecision, tenant_id: str) -> PromotionDecision:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO promotion_decisions (
                   decision_id, tenant_id, candidate_id, policy_id, policy_version, decision,
                   resulting_eligibility, reason_codes_json, input_fingerprint, decision_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(decision_id) DO NOTHING""",
                (
                    decision.decision_id,
                    tenant_id,
                    decision.candidate_ref,
                    decision.policy_id,
                    decision.policy_version,
                    decision.decision.value,
                    decision.resulting_eligibility.value,
                    json.dumps(decision.reason_codes),
                    decision.input_fingerprint,
                    decision.model_dump_json(),
                    _ts(decision.evaluated_at),
                ),
            )
        return decision

    def persist_revision(
        self,
        revision: KnowledgeRevision,
        *,
        candidate_id: str,
        decision_ref: str,
        expected_candidate: KnowledgeCandidate | None = None,
        expected_contributors: list[KnowledgeCandidate] | None = None,
        expected_parent_revision: int | None = None,
    ) -> KnowledgeRevision:
        with self.transaction() as conn:
            candidate = conn.execute(
                "SELECT tenant_id, candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate_id, revision.tenant_id),
            ).fetchone()
            if candidate is None:
                raise ValueError("promotion candidate does not belong to the knowledge tenant")
            if expected_candidate is not None and not _candidate_matches_json(
                str(candidate["candidate_json"]),
                expected_candidate,
            ):
                logger.info(
                    "knowledge_candidate_evaluation_conflict",
                    tenant_id=revision.tenant_id,
                    candidate_id=candidate_id,
                    phase="revision_persist",
                )
                raise CandidateEvaluationConflictError(
                    "candidate changed before promotion persistence; reload before evaluating"
                )
            if expected_contributors is not None:
                expected_refs = {item.id for item in expected_contributors}
                if expected_refs != set(revision.promoted_from_candidate_refs):
                    raise CandidateEvaluationConflictError("promotion contributors changed before revision persistence")
                for contributor in expected_contributors:
                    row = conn.execute(
                        "SELECT candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                        (contributor.id, revision.tenant_id),
                    ).fetchone()
                    if row is None or not _candidate_matches_json(
                        str(row["candidate_json"]),
                        contributor,
                    ):
                        logger.info(
                            "knowledge_contributor_evaluation_conflict",
                            tenant_id=revision.tenant_id,
                            candidate_id=contributor.id,
                            knowledge_id=revision.knowledge_id,
                        )
                        raise CandidateEvaluationConflictError(
                            "corroborating contributor changed before promotion persistence"
                        )
                    if (
                        contributor.state.review_state not in {ReviewState.APPROVED, ReviewState.TRUSTED}
                        or contributor.state.lifecycle_status != LifecycleStatus.ACTIVE
                        or contributor.entity_resolution.status != EntityResolutionStatus.RESOLVED
                    ):
                        raise CandidateEvaluationConflictError(
                            "corroborating contributor is no longer eligible for promotion"
                        )
                decision_row = conn.execute(
                    "SELECT decision_json FROM promotion_decisions WHERE decision_id=? AND tenant_id=?",
                    (decision_ref, revision.tenant_id),
                ).fetchone()
                if decision_row is None:
                    raise CandidateEvaluationConflictError("promotion decision disappeared before revision persistence")
                promotion_decision = PromotionDecision.model_validate_json(decision_row["decision_json"])
                if (
                    expected_candidate is None
                    or promotion_decision.candidate_ref != expected_candidate.id
                    or promotion_decision.decision != PromotionDecisionType.PROMOTE
                    or promotion_decision.resulting_eligibility != revision.state.eligibility
                    or promotion_decision.authoritative_source != expected_candidate.policy.authoritative_source
                    or promotion_decision.live_verified != expected_candidate.policy.live_verified
                ):
                    raise CandidateEvaluationConflictError("promotion inputs changed before revision persistence")
                corroboration_row = conn.execute(
                    """SELECT source_summary_json FROM corroboration_snapshots
                       WHERE snapshot_id=? AND tenant_id=?""",
                    (revision.corroboration_snapshot_ref, revision.tenant_id),
                ).fetchone()
                if (
                    expected_candidate.corroboration is None
                    or corroboration_row is None
                    or CorroborationSummary.model_validate_json(corroboration_row["source_summary_json"])
                    != expected_candidate.corroboration
                ):
                    raise CandidateEvaluationConflictError("corroboration inputs changed before revision persistence")
            row = conn.execute(
                "SELECT current_revision, created_at FROM operational_knowledge WHERE tenant_id=? AND knowledge_id=?",
                (revision.tenant_id, revision.knowledge_id),
            ).fetchone()
            current = int(row["current_revision"]) if row else 0
            if expected_parent_revision is not None and current != expected_parent_revision:
                raise KnowledgeRevisionConflictError(
                    f"knowledge target advanced from revision {expected_parent_revision} to {current}; "
                    "rebase the correction"
                )
            if revision.revision != current + 1:
                raise KnowledgeRevisionConflictError(
                    f"expected knowledge revision {current + 1}, got {revision.revision}"
                )
            if row is None:
                conn.execute(
                    """INSERT INTO operational_knowledge (
                       knowledge_id, tenant_id, kind, proposition_key, current_revision, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        revision.knowledge_id,
                        revision.tenant_id,
                        revision.proposition.kind.value,
                        revision.proposition.proposition_key,
                        revision.revision,
                        revision.state.lifecycle_status.value,
                        _ts(revision.created_at),
                        _ts(revision.created_at),
                    ),
                )
            else:
                conn.execute(
                    """UPDATE operational_knowledge SET current_revision=?, status=?, updated_at=?
                       WHERE tenant_id=? AND knowledge_id=?""",
                    (
                        revision.revision,
                        revision.state.lifecycle_status.value,
                        _ts(revision.created_at),
                        revision.tenant_id,
                        revision.knowledge_id,
                    ),
                )
            conn.execute(
                """INSERT INTO operational_knowledge_revisions (
                   knowledge_id, tenant_id, revision, parent_revision, schema_version, proposition_key,
                   scope_json, review_state, lifecycle_status, eligibility, corroboration_snapshot_ref,
                   policy_id, policy_version, revision_reason, content_json, semantic_fingerprint, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    revision.knowledge_id,
                    revision.tenant_id,
                    revision.revision,
                    revision.parent_revision,
                    revision.schema_version,
                    revision.proposition.proposition_key,
                    revision.scope.model_dump_json(),
                    revision.state.review_state.value,
                    revision.state.lifecycle_status.value,
                    revision.state.eligibility.value,
                    revision.corroboration_snapshot_ref,
                    revision.policy_id,
                    revision.policy_version,
                    revision.revision_reason,
                    revision.model_dump_json(),
                    revision.semantic_fingerprint,
                    _ts(revision.created_at),
                ),
            )
            self._replace_current_scope_refs(conn, revision)
            self._replace_current_contributors(conn, revision)
            conn.execute(
                """INSERT INTO candidate_promotions (
                   promotion_id, tenant_id, candidate_id, knowledge_id, knowledge_revision, decision_ref, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    uuid.uuid4().hex,
                    revision.tenant_id,
                    candidate_id,
                    revision.knowledge_id,
                    revision.revision,
                    decision_ref,
                    time.time(),
                ),
            )
        return revision

    def find_knowledge_by_proposition(self, tenant_id: str, proposition_key: str) -> OperationalKnowledgeItem | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM operational_knowledge WHERE tenant_id=? AND proposition_key=?",
                (tenant_id, proposition_key),
            ).fetchone()
        return self._item_from_row(row) if row else None

    def get_knowledge_item(self, knowledge_id: str, tenant_id: str = "default") -> OperationalKnowledgeItem | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM operational_knowledge WHERE tenant_id=? AND knowledge_id=?",
                (tenant_id, knowledge_id),
            ).fetchone()
        return self._item_from_row(row) if row else None

    @staticmethod
    def _item_from_row(row: sqlite3.Row) -> OperationalKnowledgeItem:
        from datetime import UTC, datetime

        return OperationalKnowledgeItem(
            id=row["knowledge_id"],
            tenant_id=row["tenant_id"],
            kind=row["kind"],
            current_revision=row["current_revision"],
            status=row["status"],
            created_at=datetime.fromtimestamp(row["created_at"], UTC),
            updated_at=datetime.fromtimestamp(row["updated_at"], UTC),
        )

    def get_revision(
        self,
        knowledge_id: str,
        revision: int | None = None,
        tenant_id: str = "default",
    ) -> KnowledgeRevision | None:
        with self._conn() as conn:
            if revision is None:
                row = conn.execute(
                    """SELECT content_json FROM operational_knowledge_revisions
                       WHERE tenant_id=? AND knowledge_id=? ORDER BY revision DESC LIMIT 1""",
                    (tenant_id, knowledge_id),
                ).fetchone()
            else:
                row = conn.execute(
                    """SELECT content_json FROM operational_knowledge_revisions
                       WHERE tenant_id=? AND knowledge_id=? AND revision=?""",
                    (tenant_id, knowledge_id, revision),
                ).fetchone()
        return KnowledgeRevision.model_validate_json(row["content_json"]) if row else None

    def list_revisions(
        self,
        knowledge_id: str,
        tenant_id: str = "default",
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeRevision]:
        pagination = ""
        params: list[Any] = [tenant_id, knowledge_id]
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT content_json FROM operational_knowledge_revisions
                    WHERE tenant_id=? AND knowledge_id=? ORDER BY revision{pagination}""",
                params,
            ).fetchall()
        return [KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows]

    def list_current_revisions(
        self,
        tenant_id: str = "default",
        *,
        lifecycle_status: str | None = None,
        kind: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeRevision]:
        clauses = ["r.tenant_id=?"]
        params: list[Any] = [tenant_id]
        if lifecycle_status is not None:
            clauses.append("k.status=?")
            params.append(lifecycle_status)
        if kind is not None:
            clauses.append("k.kind=?")
            params.append(kind)
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            limit_clause = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT r.content_json FROM operational_knowledge_revisions r
                   JOIN operational_knowledge k ON k.tenant_id=r.tenant_id
                     AND k.knowledge_id=r.knowledge_id AND k.current_revision=r.revision
                   WHERE {' AND '.join(clauses)} ORDER BY r.knowledge_id{limit_clause}""",
                params,
            ).fetchall()
        return [KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows]

    def list_current_revisions_for_scope(
        self,
        scope: KnowledgeScope,
        *,
        limit: int | None = None,
        ignored_dimensions: set[str] | None = None,
        after_knowledge_id: str = "",
    ) -> list[KnowledgeRevision]:
        """Load current revisions whose indexed exact scope can apply to an investigation."""
        params: list[Any] = [scope.tenant_id]
        base_clauses = [
            "current.tenant_id=?",
            "current.status='active'",
            "revision.review_state IN ('approved', 'trusted')",
            "revision.lifecycle_status='active'",
            "revision.eligibility!='ineligible'",
        ]
        if after_knowledge_id:
            base_clauses.append("current.knowledge_id>?")
            params.append(after_knowledge_id)
        dimension_clauses: list[str] = []
        ignored_dimensions = ignored_dimensions or set()
        for field_name in _SCOPE_REFERENCE_FIELDS:
            if field_name in ignored_dimensions:
                continue
            requested_refs = list(getattr(scope, field_name))
            unscoped = """NOT EXISTS (
                SELECT 1 FROM knowledge_current_scope_refs scoped
                WHERE scoped.tenant_id=current.tenant_id
                  AND scoped.knowledge_id=current.knowledge_id
                  AND scoped.revision=current.current_revision
                  AND scoped.dimension=?
            )"""
            params.append(field_name)
            if not requested_refs:
                dimension_clauses.append(unscoped)
                continue
            placeholders = ", ".join("?" for _ in requested_refs)
            dimension_clauses.append(f"""({unscoped} OR EXISTS (
                    SELECT 1 FROM knowledge_current_scope_refs scoped
                    WHERE scoped.tenant_id=current.tenant_id
                      AND scoped.knowledge_id=current.knowledge_id
                      AND scoped.revision=current.current_revision
                      AND scoped.dimension=?
                      AND scoped.scope_ref IN ({placeholders})
                ))""")
            params.extend([field_name, *requested_refs])
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ?"
            params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT revision.content_json
                    FROM operational_knowledge current
                    JOIN operational_knowledge_revisions revision
                      ON revision.tenant_id=current.tenant_id
                     AND revision.knowledge_id=current.knowledge_id
                     AND revision.revision=current.current_revision
                    WHERE {' AND '.join([*base_clauses, *dimension_clauses])}
                    ORDER BY current.knowledge_id{limit_clause}""",
                params,
            ).fetchall()
        return [KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows]

    def get_revisions_by_refs(
        self,
        tenant_id: str,
        refs: set[tuple[str, int]],
    ) -> dict[tuple[str, int], KnowledgeRevision]:
        """Bulk-load exact immutable revisions for one tenant."""
        if not refs:
            return {}
        ordered_refs = sorted(refs)
        revisions: list[KnowledgeRevision] = []
        with self._conn() as conn:
            for start in range(0, len(ordered_refs), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_refs[start : start + _SQLITE_BIND_BATCH_SIZE]
                values = ", ".join("(?, ?)" for _ in batch)
                params: list[Any] = []
                for knowledge_id, revision in batch:
                    params.extend([knowledge_id, revision])
                rows = conn.execute(
                    f"""WITH requested(knowledge_id, revision) AS (VALUES {values})
                        SELECT stored.content_json
                        FROM requested
                        JOIN operational_knowledge_revisions stored
                          ON stored.knowledge_id=requested.knowledge_id
                         AND stored.revision=requested.revision
                        WHERE stored.tenant_id=?""",
                    [*params, tenant_id],
                ).fetchall()
                revisions.extend(KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows)
        return {(revision.knowledge_id, revision.revision): revision for revision in revisions}

    def list_current_revisions_for_candidates(
        self,
        tenant_id: str,
        candidate_ids: set[str],
    ) -> list[KnowledgeRevision]:
        """Load only current authority revisions contributed to by the candidates."""
        if not candidate_ids:
            return []
        ordered_ids = sorted(candidate_ids)
        revisions_by_ref: dict[tuple[str, int], KnowledgeRevision] = {}
        with self._conn() as conn:
            for start in range(0, len(ordered_ids), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_ids[start : start + _SQLITE_BIND_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"""SELECT DISTINCT revision.content_json
                        FROM knowledge_current_contributors contributor
                        JOIN operational_knowledge current
                          ON current.tenant_id=contributor.tenant_id
                         AND current.knowledge_id=contributor.knowledge_id
                         AND current.current_revision=contributor.revision
                        JOIN operational_knowledge_revisions revision
                          ON revision.tenant_id=current.tenant_id
                         AND revision.knowledge_id=current.knowledge_id
                         AND revision.revision=current.current_revision
                        WHERE contributor.tenant_id=?
                          AND contributor.candidate_id IN ({placeholders})""",
                    (tenant_id, *batch),
                ).fetchall()
                for row in rows:
                    revision = KnowledgeRevision.model_validate_json(row["content_json"])
                    revisions_by_ref[(revision.knowledge_id, revision.revision)] = revision
        return [revisions_by_ref[key] for key in sorted(revisions_by_ref)]

    def list_active_proposition_keys(
        self,
        tenant_id: str,
        proposition_keys: set[str],
    ) -> set[str]:
        """Return active conflict-bearing propositions from a bounded key set."""
        if not proposition_keys:
            return set()
        ordered_keys = sorted(proposition_keys)
        active: set[str] = set()
        with self._conn() as conn:
            for start in range(0, len(ordered_keys), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_keys[start : start + _SQLITE_BIND_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                selected = conn.execute(
                    f"""SELECT p.proposition_key FROM knowledge_propositions p
                       WHERE p.tenant_id=? AND p.proposition_key IN ({placeholders}) AND EXISTS (
                         SELECT 1 FROM proposition_candidates pc
                         JOIN knowledge_candidates c
                           ON c.id=pc.candidate_id AND c.tenant_id=pc.tenant_id
                          AND c.proposition_key=p.proposition_key
                         WHERE pc.tenant_id=p.tenant_id AND pc.proposition_key=p.proposition_key
                           AND c.review_state IN ('approved', 'trusted')
                           AND c.lifecycle_status='active'
                           AND c.entity_resolution_status='resolved'
                           AND EXISTS (
                             SELECT 1 FROM knowledge_candidate_evidence e
                             WHERE e.tenant_id=c.tenant_id AND e.candidate_id=c.id
                               AND e.evidence_role='supporting'
                               AND e.lineage_kind NOT IN (
                                 'copied_from', 'generated_from', 'same_vendor_export', 'same_source_revision'
                               )
                           )
                       )""",
                    (tenant_id, *batch),
                ).fetchall()
                active.update(str(row["proposition_key"]) for row in selected)
        return active

    def list_conflicts_for_propositions(
        self,
        tenant_id: str,
        proposition_keys: set[str],
    ) -> list[KnowledgeConflict]:
        """Load unresolved conflicts touching a bounded proposition set."""
        if not proposition_keys:
            return []
        ordered_keys = sorted(proposition_keys)
        conflicts_by_id: dict[str, tuple[float, KnowledgeConflict]] = {}
        with self._conn() as conn:
            for start in range(0, len(ordered_keys), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_keys[start : start + _SQLITE_BIND_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"""SELECT conflict_id, conflict_json, created_at FROM knowledge_conflicts
                       WHERE tenant_id=? AND resolution_status='unresolved'
                         AND left_proposition_key IN ({placeholders})
                       UNION ALL
                       SELECT conflict_id, conflict_json, created_at FROM knowledge_conflicts
                       WHERE tenant_id=? AND resolution_status='unresolved'
                         AND right_proposition_key IN ({placeholders})""",
                    [tenant_id, *batch, tenant_id, *batch],
                ).fetchall()
                for row in rows:
                    conflicts_by_id[str(row["conflict_id"])] = (
                        float(row["created_at"]),
                        KnowledgeConflict.model_validate_json(row["conflict_json"]),
                    )
        return [item[1] for item in sorted(conflicts_by_id.values(), key=lambda item: item[0], reverse=True)]

    def save_snapshot(self, snapshot: KnowledgeSnapshot) -> KnowledgeSnapshot:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO knowledge_snapshots (
                   snapshot_id, tenant_id, fingerprint, snapshot_json, created_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, fingerprint) DO NOTHING""",
                (
                    snapshot.id,
                    snapshot.tenant_id,
                    snapshot.fingerprint,
                    snapshot.model_dump_json(),
                    _ts(snapshot.created_at),
                ),
            )
            stored = conn.execute(
                "SELECT snapshot_json FROM knowledge_snapshots WHERE tenant_id=? AND fingerprint=?",
                (snapshot.tenant_id, snapshot.fingerprint),
            ).fetchone()
        if stored is None:
            raise RuntimeError("knowledge snapshot insert did not produce a canonical row")
        return KnowledgeSnapshot.model_validate_json(stored["snapshot_json"])

    def get_snapshot(self, snapshot_id: str, tenant_id: str = "default") -> KnowledgeSnapshot | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT snapshot_json FROM knowledge_snapshots WHERE snapshot_id=? AND tenant_id=?",
                (snapshot_id, tenant_id),
            ).fetchone()
        return KnowledgeSnapshot.model_validate_json(row["snapshot_json"]) if row else None

    def get_usage_by_id(self, usage_id: str) -> KnowledgeUsage | None:
        """Return one immutable usage record regardless of tenant ownership."""
        if not usage_id:
            return None
        with self._conn() as conn:
            row = conn.execute(
                "SELECT usage_json FROM knowledge_usage_events WHERE usage_id=?",
                (usage_id,),
            ).fetchone()
        return KnowledgeUsage.model_validate_json(row["usage_json"]) if row else None

    def save_usage(self, usage: KnowledgeUsage) -> KnowledgeUsage:
        if not usage.usage_id:
            usage = usage.model_copy(update={"usage_id": f"usage_{uuid.uuid4().hex[:16]}"})
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT tenant_id, usage_json FROM knowledge_usage_events WHERE usage_id=?",
                (usage.usage_id,),
            ).fetchone()
            if existing is not None and existing["tenant_id"] != usage.tenant_id:
                raise ValueError("knowledge usage id already belongs to another tenant")
            if existing is not None:
                persisted = KnowledgeUsage.model_validate_json(existing["usage_json"])
                if persisted == usage:
                    return persisted
                raise ValueError("knowledge usage id already exists with different audit data")
            if (
                self.get_revision(
                    usage.knowledge_ref,
                    usage.knowledge_revision,
                    tenant_id=usage.tenant_id,
                )
                is None
            ):
                raise ValueError("knowledge usage must reference an existing tenant revision")
            conn.execute(
                """INSERT INTO knowledge_usage_events (
                   usage_id, tenant_id, investigation_id, investigation_revision, knowledge_id,
                   knowledge_revision, disposition, used_for_json, target_ref, score_delta,
                   decision_ref, usage_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    usage.usage_id,
                    usage.tenant_id,
                    usage.investigation_id,
                    usage.investigation_revision,
                    usage.knowledge_ref,
                    usage.knowledge_revision,
                    usage.disposition.value,
                    json.dumps(usage.used_for),
                    usage.target_ref,
                    usage.score_delta,
                    usage.decision_ref,
                    usage.model_dump_json(),
                    _ts(usage.created_at),
                ),
            )
        return usage

    def list_usage(
        self,
        *,
        tenant_id: str = "default",
        knowledge_id: str | None = None,
        investigation_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeUsage]:
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if knowledge_id:
            clauses.append("knowledge_id=?")
            params.append(knowledge_id)
        if investigation_id:
            clauses.append("investigation_id=?")
            params.append(investigation_id)
        pagination = ""
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT usage_json FROM knowledge_usage_events
                    WHERE {' AND '.join(clauses)} ORDER BY created_at DESC{pagination}""",
                params,
            ).fetchall()
        return [KnowledgeUsage.model_validate_json(row["usage_json"]) for row in rows]

    def list_applied_investigations(
        self,
        *,
        tenant_id: str,
        knowledge_id: str,
        limit: int,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return one bounded page of investigations actually affected by knowledge."""
        if limit < 1:
            raise ValueError("limit must be positive")
        with self.read_transaction() as conn:
            total = int(
                conn.execute(
                    """SELECT COUNT(*) FROM (
                           SELECT 1 FROM knowledge_usage_events
                           WHERE tenant_id=? AND knowledge_id=? AND disposition='applied'
                           GROUP BY investigation_id, investigation_revision
                       )""",
                    (tenant_id, knowledge_id),
                ).fetchone()[0]
            )
            rows = conn.execute(
                """SELECT investigation_id, investigation_revision, MAX(created_at) AS last_used_at
                   FROM knowledge_usage_events
                   WHERE tenant_id=? AND knowledge_id=? AND disposition='applied'
                   GROUP BY investigation_id, investigation_revision
                   ORDER BY last_used_at DESC, investigation_id, investigation_revision
                   LIMIT ? OFFSET ?""",
                (tenant_id, knowledge_id, limit, offset),
            ).fetchall()
        return (
            [
                {
                    "investigation_id": str(row["investigation_id"]),
                    "revision": int(row["investigation_revision"]),
                }
                for row in rows
            ],
            total,
        )

    def save_correction(self, correction: KnowledgeCorrection) -> KnowledgeCorrection:
        now = time.time()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO knowledge_corrections (
                   correction_id, tenant_id, investigation_id, investigation_revision, correction_type,
                   target_ref, applied_knowledge_ref, applied_knowledge_revision, review_state,
                   candidate_ref, correction_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(correction_id) DO UPDATE SET
                   review_state=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.review_state ELSE excluded.review_state END,
                   candidate_ref=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.candidate_ref ELSE excluded.candidate_ref END,
                   applied_knowledge_ref=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.applied_knowledge_ref ELSE excluded.applied_knowledge_ref END,
                   applied_knowledge_revision=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.applied_knowledge_revision
                     ELSE excluded.applied_knowledge_revision END,
                   correction_json=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.correction_json ELSE excluded.correction_json END,
                   updated_at=excluded.updated_at""",
                (
                    correction.id,
                    correction.tenant_id,
                    correction.investigation_id,
                    correction.investigation_revision,
                    correction.correction_type.value,
                    correction.target_ref,
                    correction.applied_knowledge_ref,
                    correction.applied_knowledge_revision,
                    correction.review_state.value,
                    correction.knowledge_candidate_ref,
                    correction.model_dump_json(),
                    _ts(correction.created_at),
                    now,
                ),
            )
            row = conn.execute(
                "SELECT correction_json FROM knowledge_corrections WHERE correction_id=? AND tenant_id=?",
                (correction.id, correction.tenant_id),
            ).fetchone()
        return KnowledgeCorrection.model_validate_json(row["correction_json"])

    def get_correction(self, correction_id: str, tenant_id: str = "default") -> KnowledgeCorrection | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT correction_json FROM knowledge_corrections WHERE correction_id=? AND tenant_id=?",
                (correction_id, tenant_id),
            ).fetchone()
        return KnowledgeCorrection.model_validate_json(row["correction_json"]) if row else None

    def get_correction_for_candidate(
        self,
        candidate_id: str,
        tenant_id: str = "default",
    ) -> KnowledgeCorrection | None:
        """Return the correction workflow that owns a candidate, if any."""
        with self._conn() as conn:
            row = conn.execute(
                """SELECT correction_json FROM knowledge_corrections
                   WHERE candidate_ref=? AND tenant_id=?""",
                (candidate_id, tenant_id),
            ).fetchone()
        return KnowledgeCorrection.model_validate_json(row["correction_json"]) if row else None

    def list_corrections_for_knowledge(
        self,
        knowledge_id: str,
        tenant_id: str = "default",
        *,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeCorrection]:
        """Return corrections that targeted or produced one knowledge item."""
        pagination = ""
        params: list[Any] = [tenant_id, knowledge_id, knowledge_id]
        if limit is not None:
            pagination = " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
        elif offset:
            pagination = " LIMIT -1 OFFSET ?"
            params.append(offset)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT correction_json FROM knowledge_corrections
                   WHERE tenant_id=? AND (target_ref=? OR applied_knowledge_ref=?)
                   ORDER BY created_at{pagination}""",
                params,
            ).fetchall()
        corrections = []
        for row in rows:
            try:
                corrections.append(KnowledgeCorrection.model_validate_json(row["correction_json"]))
            except ValueError:
                logger.warning(
                    "knowledge_correction_lookup_skipped",
                    tenant_id=tenant_id,
                    knowledge_id=knowledge_id,
                    reason="invalid_correction_json",
                )
        return corrections

    def stats(self, tenant_id: str = "default") -> dict[str, Any]:
        with self._conn() as conn:
            candidates = conn.execute(
                """SELECT kind, review_state, lifecycle_status, eligibility, entity_resolution_status, COUNT(*) count
                   FROM knowledge_candidates WHERE tenant_id=?
                   GROUP BY kind, review_state, lifecycle_status, eligibility, entity_resolution_status""",
                (tenant_id,),
            ).fetchall()
            knowledge = conn.execute(
                "SELECT status, COUNT(*) count FROM operational_knowledge WHERE tenant_id=? GROUP BY status",
                (tenant_id,),
            ).fetchall()
            usage = conn.execute(
                "SELECT disposition, COUNT(*) count FROM knowledge_usage_events WHERE tenant_id=? GROUP BY disposition",
                (tenant_id,),
            ).fetchall()
            conflicts = conn.execute(
                """SELECT resolution_status, COUNT(*) count FROM knowledge_conflicts
                   WHERE tenant_id=? GROUP BY resolution_status""",
                (tenant_id,),
            ).fetchall()
            corroboration = conn.execute(
                """SELECT status, COUNT(*) count FROM corroboration_snapshots
                   WHERE tenant_id=? GROUP BY status""",
                (tenant_id,),
            ).fetchall()
            corrections = conn.execute(
                """SELECT review_state, COUNT(*) count FROM knowledge_corrections
                   WHERE tenant_id=? GROUP BY review_state""",
                (tenant_id,),
            ).fetchall()
        candidate_rows = [dict(row) for row in candidates]
        knowledge_rows = [dict(row) for row in knowledge]
        usage_rows = [dict(row) for row in usage]
        conflict_rows = [dict(row) for row in conflicts]
        corroboration_rows = [dict(row) for row in corroboration]

        def count(rows: list[dict[str, Any]], field: str, value: str) -> int:
            return sum(int(row["count"]) for row in rows if row.get(field) == value)

        return {
            "tenant_id": tenant_id,
            "discovery": {
                "candidates_discovered": sum(int(row["count"]) for row in candidate_rows),
                "candidates_by_kind": {
                    kind: sum(int(row["count"]) for row in candidate_rows if row.get("kind") == kind)
                    for kind in sorted({str(row.get("kind", "")) for row in candidate_rows})
                },
            },
            "resolution": {
                state: count(candidate_rows, "entity_resolution_status", state)
                for state in ("resolved", "ambiguous", "unresolved")
            },
            "governance": {
                state: count(candidate_rows, "review_state", state) for state in ("approved", "trusted", "rejected")
            },
            "lifecycle": {
                state: count(knowledge_rows, "status", state) + count(candidate_rows, "lifecycle_status", state)
                for state in ("active", "stale", "superseded", "expired", "withdrawn")
            },
            "quality": {
                "corroborated": sum(
                    int(row["count"])
                    for row in corroboration_rows
                    if row.get("status") in {"multi_source", "multi_family", "live_corroborated"}
                ),
                "conflicted": sum(int(row["count"]) for row in conflict_rows),
                "live_corroborated": count(corroboration_rows, "status", "live_corroborated"),
            },
            "usage_summary": {
                "considered_in_investigations": sum(int(row["count"]) for row in usage_rows),
                "applied_in_investigations": count(usage_rows, "disposition", "applied"),
                "contradicted_by_live_evidence": count(usage_rows, "disposition", "contradicted_by_observation"),
                "corrected_by_users": sum(int(row["count"]) for row in corrections),
            },
            "candidates": candidate_rows,
            "knowledge": knowledge_rows,
            "usage": usage_rows,
            "conflicts": conflict_rows,
        }


_repository: KnowledgeRepository | None = None


def get_knowledge_repository() -> KnowledgeRepository:
    global _repository
    expected = _db_path()
    if _repository is None or _repository._db_path != expected:
        _repository = KnowledgeRepository(expected)
    return _repository
