"""SQLite persistence for governed, immutable Operational Knowledge."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Never

import structlog

from tacit.config import Settings, settings
from tacit.errors import RuntimeOwnershipError, safe_failure_diagnostics
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
from tacit.pagination import decode_cursor, encode_cursor
from tacit.runtime_ownership import (
    RuntimeDatabaseIdentity,
    RuntimeOwnershipDescriptor,
    runtime_descriptor_from_settings,
    snapshot_runtime_settings,
)
from tacit.signals.migrations import (
    ensure_schema as ensure_signal_schema,
)
from tacit.signals.migrations import (
    governed_projection_audit_is_current,
    mark_governed_projection_audit_current,
    reconcile_default_tenant_owner_batch,
    signal_schema_is_current,
    signal_tenant_owner_is_current,
)
from tacit.signals.schema import SQLITE_BUSY_TIMEOUT_MS
from tacit.sqlite_identity import (
    SQLiteDatabaseTarget,
    activate_sqlite_wal,
    claim_sqlite_database_identity,
    require_sqlite_database_identity,
    sqlite_database_path,
)

logger = structlog.get_logger()
_UNSET = object()
_MIGRATION_BATCH_SIZE = 500
_SQLITE_BIND_BATCH_SIZE = 400
_MAX_LEGACY_AUDIT_OFFSET = 10_000
_KNOWLEDGE_MIGRATION_NAMES = (
    "candidate_review_priority_v2",
    "correction_applied_projection_v1",
    "candidate_provenance_index_v1",
    "candidate_entity_refs_v1",
    "current_knowledge_scope_projection_v1",
    "current_knowledge_contributor_projection_v1",
    "resolve_conflicts_without_independent_support_v1",
)
_SCOPE_REFERENCE_FIELDS = (
    "environment_refs",
    "region_refs",
    "cluster_refs",
    "namespace_refs",
    "service_refs",
    "archetype_refs",
)
_SIGNAL_OWNER_MARKERS = (
    "default_owner_v1",
    "default_owner_in_progress_v1",
    "legacy_schema_owner_v1",
)


def _authority_fingerprint(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:16]


def _execute_schema_statements(conn: sqlite3.Connection, script: str) -> None:
    """Execute schema SQL without sqlite3.executescript's implicit commit."""
    pending: list[str] = []
    for line in script.splitlines():
        pending.append(line)
        statement = "\n".join(pending).strip()
        if statement and sqlite3.complete_statement(statement):
            conn.execute(statement)
            pending.clear()
    if "\n".join(pending).strip():
        raise RuntimeError("Knowledge schema contains an incomplete SQL statement")


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


def _entity_matches_json(raw: str, expected: Entity) -> bool:
    """Compare entity authority after model defaults normalize legacy JSON."""
    try:
        return Entity.model_validate_json(raw) == expected
    except ValueError:
        return False


@dataclass(frozen=True)
class CandidatePage:
    candidates: list[KnowledgeCandidate]
    has_more: bool
    next_cursor: str | None


@dataclass(frozen=True)
class ConflictPage:
    conflicts: list[KnowledgeConflict]
    has_more: bool
    next_cursor: str | None


@dataclass(frozen=True)
class UsagePage:
    usage: list[KnowledgeUsage]
    has_more: bool
    next_cursor: str | None


@dataclass(frozen=True)
class CurrentRevisionPage:
    revisions: list[KnowledgeRevision]
    has_more: bool
    next_cursor: str | None


@dataclass(frozen=True)
class RevisionPage:
    revisions: list[KnowledgeRevision]
    has_more: bool
    next_cursor: str | None


@dataclass(frozen=True)
class _MigrationCursor:
    started: bool
    tenant_id: str = ""
    row_id: str = ""


class _KnowledgeMigrationFailure(RuntimeError):
    """A startup migration failure already reduced to safe diagnostics."""


def _migration_keyset_boundary(
    cursor: _MigrationCursor,
    *,
    tenant_column: str,
    id_column: str,
) -> tuple[str, tuple[str, ...]]:
    if not cursor.started:
        return "1=1", ()
    return (
        f"({tenant_column}, {id_column}) > (?, ?)",
        (cursor.tenant_id, cursor.row_id),
    )


def _encode_candidate_cursor(created_at: float, candidate_id: str) -> str:
    return encode_cursor(created_at, candidate_id)


def _decode_candidate_cursor(cursor: str) -> tuple[float, str]:
    try:
        raw_created_at, raw_candidate_id = decode_cursor(cursor, field_count=2)
        if isinstance(raw_created_at, bool) or not isinstance(raw_created_at, (int, float)):
            raise ValueError
        if not isinstance(raw_candidate_id, str):
            raise ValueError
        created_at = float(raw_created_at)
        candidate_id = raw_candidate_id
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid candidate cursor") from exc
    if not math.isfinite(created_at) or len(candidate_id) > 500:
        raise ValueError("invalid candidate cursor")
    return created_at, candidate_id


def _decode_review_candidate_cursor(cursor: str) -> tuple[int, str]:
    try:
        raw_priority, raw_candidate_id = decode_cursor(cursor, field_count=2)
        if isinstance(raw_priority, bool) or not isinstance(raw_priority, int):
            raise ValueError
        if not isinstance(raw_candidate_id, str):
            raise ValueError
        priority = int(raw_priority)
        candidate_id = raw_candidate_id
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid review queue cursor") from exc
    if len(candidate_id) > 500:
        raise ValueError("invalid review queue cursor")
    return priority, candidate_id


def _decode_audit_cursor(cursor: str, *, resource: str) -> tuple[float, str]:
    try:
        raw_created_at, raw_id = decode_cursor(cursor, field_count=2)
        if isinstance(raw_created_at, bool) or not isinstance(raw_created_at, (int, float)):
            raise ValueError
        if not isinstance(raw_id, str):
            raise ValueError
        created_at = float(raw_created_at)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {resource} cursor") from exc
    if not math.isfinite(created_at) or len(raw_id) > 500:
        raise ValueError(f"invalid {resource} cursor")
    return created_at, raw_id


def _decode_knowledge_cursor(cursor: str) -> str:
    try:
        (knowledge_id,) = decode_cursor(cursor, field_count=1)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid knowledge cursor") from exc
    if not isinstance(knowledge_id, str) or len(knowledge_id) > 500:
        raise ValueError("invalid knowledge cursor")
    return knowledge_id


def _decode_revision_cursor(cursor: str) -> int:
    try:
        (revision,) = decode_cursor(cursor, field_count=1)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid revision cursor") from exc
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise ValueError("invalid revision cursor")
    return revision


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


class EntityRegistrationConflictError(ValueError):
    """Raised when an entity changes during an authority transition."""


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
    source_updated_at REAL NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(tenant_id, id)
);
CREATE INDEX IF NOT EXISTS idx_kc_tenant_kind ON knowledge_candidates(tenant_id, kind, created_at);
CREATE INDEX IF NOT EXISTS idx_kc_proposition ON knowledge_candidates(tenant_id, proposition_key);
CREATE INDEX IF NOT EXISTS idx_kc_tenant_created_page
    ON knowledge_candidates(tenant_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_kc_tenant_kind_created_page
    ON knowledge_candidates(tenant_id, kind, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_kc_tenant_review_created_page
    ON knowledge_candidates(tenant_id, review_state, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_kc_tenant_payload_kind
    ON knowledge_candidates(tenant_id, payload_ref, kind, updated_at DESC, id DESC);

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

CREATE TABLE IF NOT EXISTS knowledge_candidate_entity_refs (
    tenant_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    entity_ref TEXT NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY(tenant_id, candidate_id, entity_ref, role),
    FOREIGN KEY(candidate_id) REFERENCES knowledge_candidates(id)
);
CREATE INDEX IF NOT EXISTS idx_kcer_entity
    ON knowledge_candidate_entity_refs(tenant_id, entity_ref, candidate_id);

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
CREATE INDEX IF NOT EXISTS idx_conflicts_migration_page
    ON knowledge_conflicts(resolution_status, tenant_id, conflict_id);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_left_status
    ON knowledge_conflicts(tenant_id, left_proposition_key, resolution_status);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_right_status
    ON knowledge_conflicts(tenant_id, right_proposition_key, resolution_status);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_left_status_created_page
    ON knowledge_conflicts(
        tenant_id, left_proposition_key, resolution_status, created_at DESC, conflict_id DESC
    );
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_right_status_created_page
    ON knowledge_conflicts(
        tenant_id, right_proposition_key, resolution_status, created_at DESC, conflict_id DESC
    );
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_left_created_page
    ON knowledge_conflicts(tenant_id, left_proposition_key, created_at DESC, conflict_id DESC);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_right_created_page
    ON knowledge_conflicts(tenant_id, right_proposition_key, created_at DESC, conflict_id DESC);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_created_page
    ON knowledge_conflicts(tenant_id, created_at DESC, conflict_id DESC);
CREATE INDEX IF NOT EXISTS idx_conflicts_tenant_status_created_page
    ON knowledge_conflicts(tenant_id, resolution_status, created_at DESC, conflict_id DESC);

CREATE TABLE IF NOT EXISTS knowledge_migrations (
    migration_name TEXT PRIMARY KEY,
    completed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS knowledge_migration_progress (
    migration_name TEXT PRIMARY KEY,
    cursor_tenant TEXT NOT NULL DEFAULT '',
    cursor_id TEXT NOT NULL DEFAULT '',
    cursor_started INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL
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
CREATE INDEX IF NOT EXISTS idx_operational_knowledge_kind_page
    ON operational_knowledge(tenant_id, kind, status, knowledge_id, current_revision);
CREATE INDEX IF NOT EXISTS idx_operational_knowledge_kind_id_page
    ON operational_knowledge(tenant_id, kind, knowledge_id, current_revision);

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
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_item_created_page
    ON knowledge_usage_events(tenant_id, knowledge_id, created_at DESC, usage_id DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_tenant_created_page
    ON knowledge_usage_events(tenant_id, created_at DESC, usage_id DESC);
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_impact
    ON knowledge_usage_events(
        tenant_id, knowledge_id, disposition, investigation_id, investigation_revision, created_at DESC
    );
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_investigation
    ON knowledge_usage_events(tenant_id, investigation_id, investigation_revision);
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_investigation_created_page
    ON knowledge_usage_events(tenant_id, investigation_id, created_at DESC, usage_id DESC);

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
CREATE INDEX IF NOT EXISTS idx_knowledge_corrections_migration_page
    ON knowledge_corrections(tenant_id, correction_id);

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
    configured = settings.signals_db_path
    if configured:
        path = Path(configured)
    else:
        from tacit.signals import get_signal_store

        path = get_signal_store().database_path
    return path


def _ts(value) -> float:
    return value.timestamp()


def _conflict_lookup_sql(*, unresolved_only: bool) -> str:
    status_clause = " AND resolution_status='unresolved'" if unresolved_only else ""
    return f"""SELECT conflict_json, created_at, conflict_id FROM knowledge_conflicts
               WHERE tenant_id=? AND left_proposition_key=?{status_clause}
               UNION ALL
               SELECT conflict_json, created_at, conflict_id FROM knowledge_conflicts
               WHERE tenant_id=? AND right_proposition_key=?{status_clause}
               ORDER BY created_at DESC, conflict_id DESC"""


class KnowledgeRepository:
    def __init__(
        self,
        db_path: Path | None = None,
        *,
        runtime_settings: Settings | None = None,
    ):
        selected_path = db_path if db_path is not None else _db_path()
        self._db_path = sqlite_database_path(selected_path)
        self._sqlite_target = SQLiteDatabaseTarget(self._db_path)
        self._transaction_connection: ContextVar[sqlite3.Connection | None] = ContextVar(
            f"knowledge_transaction_{id(self)}",
            default=None,
        )
        self._transaction_write_locked: ContextVar[bool] = ContextVar(
            f"knowledge_write_lock_{id(self)}",
            default=False,
        )
        self._database_id: str | None = None
        self._database_had_schema = False
        configured_owner = str(runtime_settings.knowledge_tenant_id or "default") if runtime_settings else "default"
        recorded_owner = self._read_existing_tenant_owner(configured_owner)
        if runtime_settings is not None and recorded_owner is not None and recorded_owner != configured_owner:
            self._reject_owner(
                reason_code="pinned_owner_mismatch",
                configured_owner=configured_owner,
                recorded_owner=recorded_owner,
            )
        self._tenant_owner = recorded_owner or configured_owner
        owner_settings = runtime_settings or settings.model_copy(
            update={
                "knowledge_tenant_id": self._tenant_owner,
                "api_auth_enabled": bool(settings.api_auth_enabled or self._tenant_owner == "*"),
                "signals_db_path": str(self._db_path),
            }
        )
        if str(owner_settings.knowledge_tenant_id or "default") != self._tenant_owner:
            owner_settings = owner_settings.model_copy(
                update={
                    "knowledge_tenant_id": self._tenant_owner,
                    "api_auth_enabled": bool(owner_settings.api_auth_enabled or self._tenant_owner == "*"),
                }
            )
        self._runtime_settings = snapshot_runtime_settings(
            owner_settings,
            database_role="signals",
            database_path=self._db_path,
        )
        if recorded_owner is None and not self._database_had_schema:
            self._initialize_pristine_schema()
        else:
            self._initialize_signal_owner(owner_settings)
            self._ensure_schema()
        startup_migrations: tuple[tuple[str, str, Callable[[], None]], ...] = (
            ("candidate_review_priority_v2", "candidate", self._run_review_priority_migration),
            ("correction_applied_projection_v1", "correction", self._run_correction_projection_migration),
            ("candidate_provenance_index_v1", "candidate", self._run_candidate_provenance_migration),
            ("candidate_entity_refs_v1", "candidate", self._run_candidate_entity_ref_migration),
            (
                "current_knowledge_scope_projection_v1",
                "knowledge_revision",
                self._run_current_scope_projection_migration,
            ),
            (
                "current_knowledge_contributor_projection_v1",
                "knowledge_revision",
                self._run_current_contributor_projection_migration,
            ),
            (
                "resolve_conflicts_without_independent_support_v1",
                "conflict",
                self._run_conflict_lineage_migration,
            ),
        )
        for migration_name, record_class, migration in startup_migrations:
            self._run_startup_migration(
                migration_name,
                record_class=record_class,
                migration=migration,
            )

    @property
    def database_path(self) -> Path:
        """Return the canonical Operational Knowledge database identity."""
        return self._db_path

    @property
    def tenant_owner(self) -> str:
        """Return the immutable signal-database tenant owner capability."""
        return self._tenant_owner

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return the repository's public persistence ownership descriptor."""
        settings_descriptor = runtime_descriptor_from_settings(
            self._runtime_settings,
            component="knowledge_repository_settings",
        )
        return RuntimeOwnershipDescriptor(
            component="knowledge_repository",
            tenant_policy=settings_descriptor.tenant_policy,
            databases=(RuntimeDatabaseIdentity(role="signals", path=self.database_path),),
        )

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        conn = self._sqlite_target.connect(timeout_ms=SQLITE_BUSY_TIMEOUT_MS)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        observed_database_id = require_sqlite_database_identity(
            conn,
            role="signals",
            expected_database_id=self._database_id,
        )
        if observed_database_id is not None:
            self._database_id = observed_database_id
        self._require_owner_on_connection(conn)
        activate_sqlite_wal(conn, timeout_ms=SQLITE_BUSY_TIMEOUT_MS)
        self._require_owner_on_connection(conn)
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
            if not self._transaction_write_locked.get():
                query_only = self._acquire_write_lock_and_require_owner(active)
                write_lock_token = self._transaction_write_locked.set(True)
                try:
                    yield active
                finally:
                    if query_only:
                        active.execute("PRAGMA query_only=ON")
                    self._transaction_write_locked.reset(write_lock_token)
                return
            yield active
            return
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_owner_on_connection(conn)
            connection_token = self._transaction_connection.set(conn)
            write_lock_token = self._transaction_write_locked.set(True)
            try:
                yield conn
            finally:
                self._transaction_write_locked.reset(write_lock_token)
                self._transaction_connection.reset(connection_token)

    @contextmanager
    def bind_transaction_connection(self, conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
        """Join a transaction opened by another store over the same SQLite file."""
        active = self._transaction_connection.get()
        if active is not None:
            if active is not conn:
                raise RuntimeError("knowledge repository is already bound to another transaction")
            if not self._transaction_write_locked.get():
                query_only = self._acquire_write_lock_and_require_owner(active)
                write_lock_token = self._transaction_write_locked.set(True)
                try:
                    yield conn
                finally:
                    if query_only:
                        active.execute("PRAGMA query_only=ON")
                    self._transaction_write_locked.reset(write_lock_token)
                return
            yield conn
            return

        self._sqlite_target.bind_connection(conn)
        observed_database_id = require_sqlite_database_identity(
            conn,
            role="signals",
            expected_database_id=self._database_id,
        )
        if observed_database_id is None:
            raise RuntimeOwnershipError("knowledge repository cannot join an unclaimed database transaction")
        self._database_id = observed_database_id
        if not conn.in_transaction:
            raise RuntimeOwnershipError("knowledge repository external writes require an active transaction")
        self._acquire_write_lock_and_require_owner(conn)

        connection_token = self._transaction_connection.set(conn)
        write_lock_token = self._transaction_write_locked.set(True)
        try:
            yield conn
        finally:
            self._transaction_write_locked.reset(write_lock_token)
            self._transaction_connection.reset(connection_token)

    @contextmanager
    def read_transaction(self) -> Iterator[sqlite3.Connection]:
        """Keep a multi-query read on one SQLite snapshot without taking a write lock."""
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        with self._conn() as conn:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            connection_token = self._transaction_connection.set(conn)
            write_lock_token = self._transaction_write_locked.set(False)
            try:
                yield conn
            finally:
                self._transaction_write_locked.reset(write_lock_token)
                self._transaction_connection.reset(connection_token)
                conn.execute("PRAGMA query_only=OFF")

    def _ensure_schema(self) -> None:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_owner_on_connection(conn)
            self._database_id = claim_sqlite_database_identity(
                conn,
                role="signals",
                expected_database_id=self._database_id,
            )
            self._install_knowledge_schema(conn)
        self._log_initialized()

    def _install_knowledge_schema(self, conn: sqlite3.Connection) -> None:
        """Install the knowledge schema inside the caller's verified write transaction."""
        _execute_schema_statements(conn, SCHEMA_SQL)
        candidate_columns = {row["name"] for row in conn.execute("PRAGMA table_info(knowledge_candidates)").fetchall()}
        if "review_priority" not in candidate_columns:
            conn.execute("ALTER TABLE knowledge_candidates ADD COLUMN review_priority INTEGER NOT NULL DEFAULT 0")
        if "has_unresolved_conflict" not in candidate_columns:
            conn.execute("""ALTER TABLE knowledge_candidates
                   ADD COLUMN has_unresolved_conflict INTEGER NOT NULL DEFAULT 0""")
        if "source_updated_at" not in candidate_columns:
            # Zero means the source generation predates this projection. It
            # remains eligible for the first bounded reconciliation without
            # requiring an unbounded migration update.
            conn.execute("""ALTER TABLE knowledge_candidates
                   ADD COLUMN source_updated_at REAL NOT NULL DEFAULT 0""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_kc_review_queue
               ON knowledge_candidates(tenant_id, review_state, review_priority DESC, id)""")
        correction_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(knowledge_corrections)").fetchall()
        }
        missing_applied_ref = "applied_knowledge_ref" not in correction_columns
        missing_applied_revision = "applied_knowledge_revision" not in correction_columns
        if missing_applied_ref:
            conn.execute("ALTER TABLE knowledge_corrections ADD COLUMN applied_knowledge_ref TEXT NOT NULL DEFAULT ''")
        if missing_applied_revision:
            conn.execute("ALTER TABLE knowledge_corrections ADD COLUMN applied_knowledge_revision INTEGER")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_knowledge_corrections_target
               ON knowledge_corrections(tenant_id, target_ref)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_knowledge_corrections_applied
               ON knowledge_corrections(tenant_id, applied_knowledge_ref)""")
        progress_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(knowledge_migration_progress)").fetchall()
        }
        if "cursor_started" not in progress_columns:
            conn.execute("""ALTER TABLE knowledge_migration_progress
                   ADD COLUMN cursor_started INTEGER NOT NULL DEFAULT 0""")
            conn.execute("""UPDATE knowledge_migration_progress SET cursor_started=1
                   WHERE cursor_tenant<>'' OR cursor_id<>''""")
            placeholders = ", ".join("?" for _ in _KNOWLEDGE_MIGRATION_NAMES)
            conn.execute(
                f"DELETE FROM knowledge_migrations WHERE migration_name IN ({placeholders})",
                _KNOWLEDGE_MIGRATION_NAMES,
            )

    def _log_initialized(self) -> None:
        logger.info(
            "knowledge_repository_init",
            reason_code="knowledge_repository_initialized",
            database_fingerprint=_authority_fingerprint(self._db_path),
            owner_class="wildcard" if self._tenant_owner == "*" else "pinned",
            owner_fingerprint=_authority_fingerprint(self._tenant_owner),
        )

    def _initialize_pristine_schema(self) -> None:
        """Claim a new signal database and install both schemas atomically."""
        conn = self._sqlite_target.connect(timeout_ms=SQLITE_BUSY_TIMEOUT_MS)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing_tables = {str(row[0]) for row in conn.execute("""SELECT name FROM sqlite_master
                       WHERE type='table' AND name NOT LIKE 'sqlite_%'""").fetchall()}
            owner_table_exists = "signal_tenant_migration_metadata" in existing_tables
            owners = (
                {
                    str(row["value"])
                    for row in conn.execute(
                        """SELECT value FROM signal_tenant_migration_metadata
                           WHERE key IN (?, ?, ?)""",
                        _SIGNAL_OWNER_MARKERS,
                    ).fetchall()
                }
                if owner_table_exists
                else set()
            )
            if len(owners) > 1:
                self._reject_owner(
                    reason_code="ambiguous_owner_metadata",
                    configured_owner=self._tenant_owner,
                )
            recorded_owner = next(iter(owners), None)
            if recorded_owner is not None and recorded_owner != self._tenant_owner:
                self._reject_owner(
                    reason_code="pinned_owner_mismatch",
                    configured_owner=self._tenant_owner,
                    recorded_owner=recorded_owner,
                )

            observed_database_id = require_sqlite_database_identity(
                conn,
                role="signals",
                expected_database_id=self._database_id,
            )
            if recorded_owner is None:
                if existing_tables:
                    raise RuntimeOwnershipError(
                        "Signal database changed while acquiring its initialization lock; retry"
                    )
                schema_complete = ensure_signal_schema(
                    conn,
                    legacy_tenant=None if self._tenant_owner == "*" else self._tenant_owner,
                    bootstrap_signal_definitions=None,
                )
                if not schema_complete:
                    raise RuntimeOwnershipError("Fresh signal schema initialization did not complete")
                for _ in range(128):
                    owner_complete, _operation, _row_count = reconcile_default_tenant_owner_batch(
                        conn,
                        legacy_tenant=None if self._tenant_owner == "*" else self._tenant_owner,
                        batch_size=500,
                    )
                    if owner_complete:
                        break
                else:
                    raise RuntimeOwnershipError("Fresh signal tenant owner initialization exceeded its work bound")
                mark_governed_projection_audit_current(conn)
            elif not (
                signal_schema_is_current(conn)
                and governed_projection_audit_is_current(conn)
                and signal_tenant_owner_is_current(
                    conn,
                    legacy_tenant=None if self._tenant_owner == "*" else self._tenant_owner,
                )
            ):
                raise RuntimeOwnershipError(
                    "Signal database initialization is incomplete; retry through the signal-store owner"
                )

            self._database_id = claim_sqlite_database_identity(
                conn,
                role="signals",
                expected_database_id=observed_database_id or self._database_id,
            )
            self._require_owner_on_connection(conn)
            self._install_knowledge_schema(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        self._log_initialized()

    def _initialize_signal_owner(self, runtime_settings: Settings) -> None:
        """Use the signal store's canonical owner transition before knowledge schema writes."""
        from tacit.signals.store import SignalStore

        try:
            SignalStore(self._db_path, runtime_settings=runtime_settings)
        except Exception:
            recorded_owner = self._read_existing_tenant_owner(self._tenant_owner)
            if recorded_owner is not None and recorded_owner != self._tenant_owner:
                self._reject_owner(
                    reason_code="pinned_owner_mismatch",
                    configured_owner=self._tenant_owner,
                    recorded_owner=recorded_owner,
                )
            raise

    def _read_existing_tenant_owner(self, configured_owner: str) -> str | None:
        def inspect(conn: sqlite3.Connection) -> str | None:
            conn.row_factory = sqlite3.Row
            self._database_had_schema = conn.execute("""SELECT 1 FROM sqlite_master
                   WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1""").fetchone() is not None
            observed_database_id = require_sqlite_database_identity(
                conn,
                role="signals",
                expected_database_id=self._database_id,
            )
            if observed_database_id is not None:
                self._database_id = observed_database_id
            metadata_exists = conn.execute("""SELECT 1 FROM sqlite_master
                       WHERE type='table' AND name='signal_tenant_migration_metadata'""").fetchone()
            if metadata_exists is None:
                return None
            owners = {
                str(row["value"])
                for row in conn.execute(
                    """SELECT value FROM signal_tenant_migration_metadata
                       WHERE key IN (?, ?, ?)""",
                    _SIGNAL_OWNER_MARKERS,
                ).fetchall()
            }
            if len(owners) > 1:
                self._reject_owner(
                    reason_code="ambiguous_owner_metadata",
                    configured_owner=configured_owner,
                )
            return next(iter(owners), None)

        return self._sqlite_target.read_existing_readonly(
            inspect,
            timeout_ms=SQLITE_BUSY_TIMEOUT_MS,
        )

    def _require_owner_on_connection(self, conn: sqlite3.Connection) -> None:
        """Revalidate role and immutable tenant ownership on the locked generation."""
        observed_database_id = require_sqlite_database_identity(
            conn,
            role="signals",
            expected_database_id=self._database_id,
        )
        if observed_database_id is None:
            raise RuntimeOwnershipError("knowledge repository cannot use an unclaimed signal database")
        self._database_id = observed_database_id
        if signal_tenant_owner_is_current(
            conn,
            legacy_tenant=None if self._tenant_owner == "*" else self._tenant_owner,
        ):
            row = conn.execute(
                "SELECT value FROM signal_tenant_migration_metadata WHERE key='default_owner_v1'"
            ).fetchone()
            recorded_owner = str(row[0]) if row is not None else None
            if recorded_owner == self._tenant_owner:
                return
        row = conn.execute("SELECT value FROM signal_tenant_migration_metadata WHERE key='default_owner_v1'").fetchone()
        self._reject_owner(
            reason_code="pinned_owner_mismatch" if row is not None else "owner_marker_missing",
            configured_owner=self._tenant_owner,
            recorded_owner=str(row[0]) if row is not None else None,
        )

    def _acquire_write_lock_and_require_owner(self, conn: sqlite3.Connection) -> bool:
        """Upgrade a deferred transaction without changing owner data, then revalidate."""
        query_only = bool(conn.execute("PRAGMA query_only").fetchone()[0])
        if query_only:
            conn.execute("PRAGMA query_only=OFF")
        try:
            conn.execute("UPDATE tacit_runtime_database_identity SET role=role WHERE 0")
            self._require_owner_on_connection(conn)
        except Exception:
            if query_only:
                conn.execute("PRAGMA query_only=ON")
            raise
        return query_only

    def _reject_owner(
        self,
        *,
        reason_code: str,
        configured_owner: str,
        recorded_owner: str | None = None,
    ) -> Never:
        fields: dict[str, object] = {
            "reason_code": reason_code,
            "database_fingerprint": _authority_fingerprint(self._db_path),
            "configured_owner_class": "wildcard" if configured_owner == "*" else "pinned",
            "configured_owner_fingerprint": _authority_fingerprint(configured_owner),
        }
        if recorded_owner is not None:
            fields.update(
                {
                    "recorded_owner_class": "wildcard" if recorded_owner == "*" else "pinned",
                    "recorded_owner_fingerprint": _authority_fingerprint(recorded_owner),
                }
            )
        logger.error("knowledge_repository_owner_rejected", **fields)
        raise RuntimeError(f"Knowledge repository owner rejected (reason={reason_code})")

    @staticmethod
    def _diagnostic_fingerprint(*values: str) -> str:
        return hashlib.blake2s(
            json.dumps(values, separators=(",", ":")).encode(),
            digest_size=6,
        ).hexdigest()

    @classmethod
    def _raise_invalid_migration_record(
        cls,
        exc: BaseException,
        *,
        migration_name: str,
        record_class: str,
        tenant_id: str,
        row_id: str,
    ) -> Never:
        reason_code = "knowledge_migration_invalid_record"
        diagnostics = safe_failure_diagnostics(
            exc,
            reason_code=reason_code,
            counters={"record_count": 1},
        )
        record_fingerprint = cls._diagnostic_fingerprint(record_class, tenant_id, row_id)
        logger.error(
            "knowledge_repository_migration_failed",
            reason_code=reason_code,
            migration_name=migration_name,
            record_class=record_class,
            record_fingerprint=record_fingerprint,
            **diagnostics,
        )
        raise _KnowledgeMigrationFailure(
            ";".join(
                (
                    reason_code,
                    f"migration_name={migration_name}",
                    f"record_class={record_class}",
                    "record_count=1",
                    f"record_fingerprint={record_fingerprint}",
                    f"error_type={diagnostics['error_type']}",
                    f"failure_fingerprint={diagnostics['failure_fingerprint']}",
                )
            )
        ) from None

    @classmethod
    def _validated_migration_model(
        cls,
        row: sqlite3.Row,
        *,
        migration_name: str,
        record_class: str,
        model_type: type[Any],
        json_column: str,
        id_column: str,
        model_id_attribute: str,
        revision_column: str | None = None,
    ) -> Any:
        tenant_id = str(row["tenant_id"])
        row_id = str(row[id_column])
        try:
            model = model_type.model_validate_json(row[json_column])
            if model.tenant_id != tenant_id or getattr(model, model_id_attribute) != row_id:
                raise ValueError("stored migration identity does not match its authority payload")
            if revision_column is not None and model.revision != int(row[revision_column]):
                raise ValueError("stored migration revision does not match its authority payload")
        except Exception as exc:
            cls._raise_invalid_migration_record(
                exc,
                migration_name=migration_name,
                record_class=record_class,
                tenant_id=tenant_id,
                row_id=row_id,
            )
        return model

    @classmethod
    def _run_startup_migration(
        cls,
        migration_name: str,
        *,
        record_class: str,
        migration: Callable[[], None],
    ) -> None:
        try:
            migration()
        except _KnowledgeMigrationFailure:
            raise
        except Exception as exc:
            reason_code = "knowledge_migration_batch_failed"
            diagnostics = safe_failure_diagnostics(
                exc,
                reason_code=reason_code,
                counters={"record_count": 0},
            )
            record_fingerprint = cls._diagnostic_fingerprint(record_class, migration_name)
            logger.error(
                "knowledge_repository_migration_failed",
                reason_code=reason_code,
                migration_name=migration_name,
                record_class=record_class,
                record_fingerprint=record_fingerprint,
                **diagnostics,
            )
            raise _KnowledgeMigrationFailure(
                ";".join(
                    (
                        reason_code,
                        f"migration_name={migration_name}",
                        f"record_class={record_class}",
                        "record_count=0",
                        f"record_fingerprint={record_fingerprint}",
                        f"error_type={diagnostics['error_type']}",
                        f"failure_fingerprint={diagnostics['failure_fingerprint']}",
                    )
                )
            ) from None

    @staticmethod
    def _migration_cursor(
        conn: sqlite3.Connection,
        migration_name: str,
    ) -> _MigrationCursor | None:
        """Return a durable keyset cursor, or None when migration is complete."""
        if conn.execute(
            "SELECT 1 FROM knowledge_migrations WHERE migration_name=?",
            (migration_name,),
        ).fetchone():
            return None
        row = conn.execute(
            """SELECT cursor_tenant, cursor_id, cursor_started FROM knowledge_migration_progress
               WHERE migration_name=?""",
            (migration_name,),
        ).fetchone()
        if row is None:
            return _MigrationCursor(started=False)
        started_value = row["cursor_started"]
        if isinstance(started_value, bool) or not isinstance(started_value, int) or started_value not in {0, 1}:
            raise RuntimeError("Knowledge migration progress is invalid")
        tenant_id = str(row["cursor_tenant"])
        row_id = str(row["cursor_id"])
        if not started_value:
            if tenant_id or row_id:
                raise RuntimeError("Knowledge migration progress is invalid")
            return _MigrationCursor(started=False)
        return _MigrationCursor(started=True, tenant_id=tenant_id, row_id=row_id)

    @staticmethod
    def _save_migration_cursor(
        conn: sqlite3.Connection,
        migration_name: str,
        tenant_id: str,
        row_id: str,
    ) -> None:
        conn.execute(
            """INSERT INTO knowledge_migration_progress (
                   migration_name, cursor_tenant, cursor_id, cursor_started, updated_at
               ) VALUES (?, ?, ?, 1, ?)
               ON CONFLICT(migration_name) DO UPDATE SET
                   cursor_tenant=excluded.cursor_tenant,
                   cursor_id=excluded.cursor_id,
                   cursor_started=1,
                   updated_at=excluded.updated_at""",
            (migration_name, tenant_id, row_id, time.time()),
        )

    @staticmethod
    def _complete_migration(conn: sqlite3.Connection, migration_name: str) -> None:
        conn.execute(
            """INSERT OR IGNORE INTO knowledge_migrations (migration_name, completed_at)
               VALUES (?, ?)""",
            (migration_name, time.time()),
        )
        conn.execute(
            "DELETE FROM knowledge_migration_progress WHERE migration_name=?",
            (migration_name,),
        )

    def _run_review_priority_migration(self) -> None:
        """Backfill queue priority without holding one database-wide write lock."""
        migration_name = "candidate_review_priority_v2"
        migrated = 0
        batch_count = 0
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="candidate.tenant_id",
                    id_column="candidate.id",
                )
                rows = conn.execute(
                    f"""SELECT candidate.id, candidate.tenant_id, candidate.candidate_json,
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
                       WHERE {boundary}
                       ORDER BY candidate.tenant_id, candidate.id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                batch_count += 1
                updates: list[tuple[int, int, str, str]] = []
                for row in rows:
                    candidate = self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="candidate",
                        model_type=KnowledgeCandidate,
                        json_column="candidate_json",
                        id_column="id",
                        model_id_attribute="id",
                    )
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
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["id"]),
                )
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
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="tenant_id",
                    id_column="correction_id",
                )
                rows = conn.execute(
                    f"""SELECT tenant_id, correction_id, correction_json
                       FROM knowledge_corrections
                       WHERE {boundary}
                       ORDER BY tenant_id, correction_id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                batch_count += 1
                for row in rows:
                    correction = self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="correction",
                        model_type=KnowledgeCorrection,
                        json_column="correction_json",
                        id_column="correction_id",
                        model_id_attribute="id",
                    )
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
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["correction_id"]),
                )
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
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="tenant_id",
                    id_column="id",
                )
                rows = conn.execute(
                    f"""SELECT tenant_id, id, candidate_json FROM knowledge_candidates
                       WHERE {boundary}
                       ORDER BY tenant_id, id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                batch_count += 1
                for row in rows:
                    candidate = self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="candidate",
                        model_type=KnowledgeCandidate,
                        json_column="candidate_json",
                        id_column="id",
                        model_id_attribute="id",
                    )
                    self._replace_candidate_provenance(conn, candidate)
                    migrated += 1
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["id"]),
                )
        if migrated:
            logger.info(
                "candidate_provenance_index_backfilled",
                candidate_count=migrated,
                batch_count=batch_count,
                batch_size=_MIGRATION_BATCH_SIZE,
            )

    @staticmethod
    def _replace_candidate_entity_refs(conn: sqlite3.Connection, candidate: KnowledgeCandidate) -> None:
        conn.execute(
            "DELETE FROM knowledge_candidate_entity_refs WHERE tenant_id=? AND candidate_id=?",
            (candidate.tenant_id, candidate.id),
        )
        rows = [
            (candidate.tenant_id, candidate.id, entity_ref, role)
            for role, entity_ref in (
                ("subject", candidate.proposition.subject_ref),
                ("object", candidate.proposition.object_ref),
            )
            if entity_ref.startswith("entity:")
        ]
        if rows:
            conn.executemany(
                """INSERT INTO knowledge_candidate_entity_refs (
                       tenant_id, candidate_id, entity_ref, role
                   ) VALUES (?, ?, ?, ?)""",
                rows,
            )

    def _run_candidate_entity_ref_migration(self) -> None:
        migration_name = "candidate_entity_refs_v1"
        migrated = 0
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="tenant_id",
                    id_column="id",
                )
                rows = conn.execute(
                    f"""SELECT tenant_id, id, candidate_json FROM knowledge_candidates
                       WHERE {boundary}
                       ORDER BY tenant_id, id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                for row in rows:
                    candidate = self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="candidate",
                        model_type=KnowledgeCandidate,
                        json_column="candidate_json",
                        id_column="id",
                        model_id_attribute="id",
                    )
                    self._replace_candidate_entity_refs(conn, candidate)
                    migrated += 1
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["id"]),
                )
        if migrated:
            logger.info(
                "candidate_entity_refs_backfilled",
                candidate_count=migrated,
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
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="current.tenant_id",
                    id_column="current.knowledge_id",
                )
                rows = conn.execute(
                    f"""SELECT current.tenant_id, current.knowledge_id,
                              current.current_revision, revision.content_json
                       FROM operational_knowledge current
                       LEFT JOIN operational_knowledge_revisions revision
                         ON revision.tenant_id=current.tenant_id
                        AND revision.knowledge_id=current.knowledge_id
                        AND revision.revision=current.current_revision
                       WHERE {boundary}
                       ORDER BY current.tenant_id, current.knowledge_id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                batch_count += 1
                for row in rows:
                    revision = self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="knowledge_revision",
                        model_type=KnowledgeRevision,
                        json_column="content_json",
                        id_column="knowledge_id",
                        model_id_attribute="knowledge_id",
                        revision_column="current_revision",
                    )
                    self._replace_current_scope_refs(conn, revision)
                    migrated += 1
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["knowledge_id"]),
                )
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
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="current.tenant_id",
                    id_column="current.knowledge_id",
                )
                rows = conn.execute(
                    f"""SELECT current.tenant_id, current.knowledge_id,
                              current.current_revision, revision.content_json
                       FROM operational_knowledge current
                       LEFT JOIN operational_knowledge_revisions revision
                         ON revision.tenant_id=current.tenant_id
                        AND revision.knowledge_id=current.knowledge_id
                        AND revision.revision=current.current_revision
                       WHERE {boundary}
                       ORDER BY current.tenant_id, current.knowledge_id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                batch_count += 1
                for row in rows:
                    revision = self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="knowledge_revision",
                        model_type=KnowledgeRevision,
                        json_column="content_json",
                        id_column="knowledge_id",
                        model_id_attribute="knowledge_id",
                        revision_column="current_revision",
                    )
                    self._replace_current_contributors(conn, revision)
                    migrated += 1
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["knowledge_id"]),
                )
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
        while True:
            with self.transaction() as conn:
                cursor = self._migration_cursor(conn, migration_name)
                if cursor is None:
                    return
                boundary, boundary_params = _migration_keyset_boundary(
                    cursor,
                    tenant_column="tenant_id",
                    id_column="conflict_id",
                )
                rows = conn.execute(
                    f"""SELECT conflict_id, tenant_id, left_proposition_key,
                              right_proposition_key, conflict_json
                       FROM knowledge_conflicts
                       WHERE resolution_status='unresolved'
                         AND {boundary}
                       ORDER BY tenant_id, conflict_id LIMIT ?""",
                    (*boundary_params, _MIGRATION_BATCH_SIZE),
                ).fetchall()
                if not rows:
                    self._complete_migration(conn, migration_name)
                    break
                batch_count += 1
                conflicts = [
                    self._validated_migration_model(
                        row,
                        migration_name=migration_name,
                        record_class="conflict",
                        model_type=KnowledgeConflict,
                        json_column="conflict_json",
                        id_column="conflict_id",
                        model_id_attribute="id",
                    )
                    for row in rows
                ]
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
                for row, conflict in zip(rows, conflicts, strict=True):
                    unsupported = unsupported_by_tenant[str(row["tenant_id"])]
                    if not unsupported.intersection(
                        {str(row["left_proposition_key"]), str(row["right_proposition_key"])}
                    ):
                        continue
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
                self._save_migration_cursor(
                    conn,
                    migration_name,
                    str(rows[-1]["tenant_id"]),
                    str(rows[-1]["conflict_id"]),
                )
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
        with self.transaction() as conn:
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
        source_material_changed: bool = False,
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
                       candidate_json, source_updated_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                        time.time(),
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
                           candidate_json=?, source_updated_at=CASE WHEN ? THEN ? ELSE source_updated_at END,
                           updated_at=?
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
                        int(source_material_changed),
                        time.time(),
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
            self._replace_candidate_entity_refs(conn, candidate)
        return candidate

    def save_candidate_with_proposition(
        self,
        candidate: KnowledgeCandidate,
        *,
        lineage_group: str,
        independence_class: str,
        expected: KnowledgeCandidate | None | object = _UNSET,
        source_material_changed: bool = True,
    ) -> KnowledgeCandidate:
        """Commit a candidate and its proposition membership as one unit."""
        with self.transaction():
            self.save_candidate(
                candidate,
                expected=expected,
                source_material_changed=source_material_changed,
            )
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
                    tenant_fingerprint=_authority_fingerprint(candidate.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate.id),
                    phase="policy_state_persist",
                )
                raise CandidateEvaluationConflictError(
                    "candidate changed during policy evaluation; reload before evaluating"
                )
            cursor = conn.execute(
                """UPDATE knowledge_candidates SET
                       review_state=?, lifecycle_status=?, eligibility=?,
                       entity_resolution_status=?,
                       promotion_policy_id=?, promotion_policy_version=?,
                       candidate_json=?, updated_at=?
                   WHERE id=? AND tenant_id=?""",
                (
                    candidate.state.review_state.value,
                    candidate.state.lifecycle_status.value,
                    candidate.state.eligibility.value,
                    candidate.entity_resolution.status.value,
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
                    tenant_fingerprint=_authority_fingerprint(candidate.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate.id),
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
                    tenant_fingerprint=_authority_fingerprint(candidate.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate.id),
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
                    tenant_fingerprint=_authority_fingerprint(candidate.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate.id),
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
                    tenant_fingerprint=_authority_fingerprint(candidate.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate.id),
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
                    tenant_fingerprint=_authority_fingerprint(candidate.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate.id),
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
        if limit is not None:
            return self.list_candidates_page(
                tenant_id,
                kind=kind,
                review_state=review_state,
                limit=limit,
            ).candidates

        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if kind is not None:
            clauses.append("kind=?")
            params.append(kind)
        if review_state is not None:
            clauses.append("review_state=?")
            params.append(review_state)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT candidate_json FROM knowledge_candidates WHERE {' AND '.join(clauses)} "
                "ORDER BY created_at DESC, id DESC",
                params,
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def list_candidates_for_payload_ref(
        self,
        tenant_id: str,
        payload_ref: str,
        *,
        kind: str | None = None,
        limit: int = 2,
    ) -> list[KnowledgeCandidate]:
        """Return a bounded set of candidates sharing one source payload identity."""
        if limit < 1:
            raise ValueError("candidate payload lookup limit must be positive")
        clauses = ["tenant_id=?", "payload_ref=?"]
        params: list[Any] = [tenant_id, payload_ref]
        if kind is not None:
            clauses.append("kind=?")
            params.append(kind)
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT candidate_json FROM knowledge_candidates
                    WHERE {" AND ".join(clauses)}
                    ORDER BY updated_at DESC, id DESC LIMIT ?""",
                params,
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def list_candidates_page(
        self,
        tenant_id: str = "default",
        *,
        kind: str | None = None,
        review_state: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> CandidatePage:
        """Return one stable newest-first keyset page of candidate audit history."""
        if not 1 <= limit <= 500:
            raise ValueError("candidate page limit must be between 1 and 500")
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if kind is not None:
            clauses.append("kind=?")
            params.append(kind)
        if review_state is not None:
            clauses.append("review_state=?")
            params.append(review_state)
        if cursor is not None:
            created_at, candidate_id = _decode_candidate_cursor(cursor)
            clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
            params.extend([created_at, created_at, candidate_id])
        params.append(limit + 1)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT id, created_at, candidate_json
                    FROM knowledge_candidates
                    WHERE {" AND ".join(clauses)}
                    ORDER BY created_at DESC, id DESC
                    LIMIT ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = _encode_candidate_cursor(float(last["created_at"]), str(last["id"]))
        return CandidatePage(
            candidates=[KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_candidates_for_provenance(
        self,
        tenant_id: str,
        provenance_ref: str,
        *,
        after_candidate_id: str | None = None,
        limit: int = _MIGRATION_BATCH_SIZE,
        source_updated_before: float | None = None,
        kind: str | None = None,
    ) -> list[KnowledgeCandidate]:
        if limit < 1:
            raise ValueError("limit must be positive")
        clauses = ["p.tenant_id=?", "p.provenance_ref=?"]
        params: list[Any] = [tenant_id, provenance_ref]
        if after_candidate_id is not None:
            clauses.append("c.id>?")
            params.append(after_candidate_id)
        if source_updated_before is not None:
            clauses.append("c.source_updated_at<=?")
            params.append(source_updated_before)
        if kind is not None:
            clauses.append("c.kind=?")
            params.append(kind)
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT c.candidate_json
                   FROM knowledge_candidate_provenance p
                   JOIN knowledge_candidates c
                     ON c.tenant_id=p.tenant_id AND c.id=p.candidate_id
                   WHERE {" AND ".join(clauses)}
                   ORDER BY c.id LIMIT ?""",
                params,
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def has_candidate_for_provenance(
        self,
        tenant_id: str,
        provenance_ref: str,
        *,
        kind: str | None = None,
        source_updated_before: float | None = None,
    ) -> bool:
        clauses = ["p.tenant_id=?", "p.provenance_ref=?"]
        params: list[Any] = [tenant_id, provenance_ref]
        if kind is not None:
            clauses.append("c.kind=?")
            params.append(kind)
        if source_updated_before is not None:
            clauses.append("c.source_updated_at<=?")
            params.append(source_updated_before)
        with self._conn() as conn:
            row = conn.execute(
                f"""SELECT 1
                    FROM knowledge_candidate_provenance p
                    JOIN knowledge_candidates c
                      ON c.tenant_id=p.tenant_id AND c.id=p.candidate_id
                    WHERE {" AND ".join(clauses)} LIMIT 1""",
                params,
            ).fetchone()
        return row is not None

    def count_candidates_for_provenance(
        self,
        tenant_id: str,
        provenance_ref: str,
        *,
        source_updated_before: float | None = None,
        stop_after: int | None = None,
    ) -> int:
        """Count a source's candidates, optionally stopping after a bounded threshold."""
        if stop_after is not None and stop_after < 1:
            raise ValueError("stop_after must be positive")
        clauses = ["p.tenant_id=?", "p.provenance_ref=?"]
        params: list[Any] = [tenant_id, provenance_ref]
        if source_updated_before is not None:
            clauses.append("c.source_updated_at<=?")
            params.append(source_updated_before)
        limit_clause = ""
        if stop_after is not None:
            limit_clause = " LIMIT ?"
            params.append(stop_after)
        with self._conn() as conn:
            row = conn.execute(
                f"""SELECT COUNT(*) AS candidate_count FROM (
                        SELECT 1
                        FROM knowledge_candidate_provenance p
                        JOIN knowledge_candidates c
                          ON c.tenant_id=p.tenant_id AND c.id=p.candidate_id
                        WHERE {" AND ".join(clauses)}{limit_clause}
                    )""",
                params,
            ).fetchone()
        return int(row["candidate_count"] if row is not None else 0)

    def list_candidates_for_entity(
        self,
        tenant_id: str,
        entity_ref: str,
        *,
        after_candidate_id: str | None = None,
        limit: int = _MIGRATION_BATCH_SIZE,
    ) -> list[KnowledgeCandidate]:
        """Return a bounded keyset page of candidates bound to an entity."""
        clauses = ["ref.tenant_id=?", "ref.entity_ref=?"]
        params: list[Any] = [tenant_id, entity_ref]
        if after_candidate_id is not None:
            clauses.append("ref.candidate_id>?")
            params.append(after_candidate_id)
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT candidate.candidate_json
                   FROM knowledge_candidate_entity_refs ref
                   JOIN knowledge_candidates candidate
                     ON candidate.tenant_id=ref.tenant_id AND candidate.id=ref.candidate_id
                   WHERE {" AND ".join(clauses)}
                   GROUP BY candidate.id
                   ORDER BY candidate.id LIMIT ?""",
                params,
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def list_review_candidates_page(
        self,
        tenant_id: str,
        *,
        limit: int,
        cursor: str | None = None,
    ) -> CandidatePage:
        """Return one stable priority-ordered page of pending review work."""
        if not 1 <= limit <= 500:
            raise ValueError("review queue limit must be between 1 and 500")
        clauses = ["c.tenant_id=?", "c.review_state='candidate'"]
        params: list[Any] = [tenant_id]
        if cursor is not None:
            priority, candidate_id = _decode_review_candidate_cursor(cursor)
            clauses.append("(c.review_priority < ? OR (c.review_priority = ? AND c.id > ?))")
            params.extend([priority, priority, candidate_id])
        params.append(limit + 1)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT c.id, c.review_priority, c.candidate_json
                   FROM knowledge_candidates c
                   WHERE {" AND ".join(clauses)}
                   ORDER BY c.review_priority DESC, c.id
                   LIMIT ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(int(last["review_priority"]), str(last["id"]))
        return CandidatePage(
            candidates=[KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_review_candidates(self, tenant_id: str, *, limit: int) -> list[KnowledgeCandidate]:
        """Compatibility wrapper for the first priority-ordered review page."""
        return self.list_review_candidates_page(tenant_id, limit=limit).candidates

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

    def save_entity(
        self,
        entity: Entity,
        *,
        expected: Entity | None | object = _UNSET,
    ) -> Entity:
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT kind, entity_json FROM entities WHERE id=? AND tenant_id=?",
                (entity.id, entity.tenant_id),
            ).fetchone()
            if existing is not None and existing["kind"] != entity.kind.value:
                raise ValueError("entity kind cannot change for an existing entity id")
            if expected is not _UNSET:
                matches_expected = (
                    existing is not None
                    and isinstance(expected, Entity)
                    and _entity_matches_json(str(existing["entity_json"]), expected)
                )
                if not matches_expected and not (existing is None and expected is None):
                    raise EntityRegistrationConflictError(
                        "entity changed during registration; reload before updating authority"
                    )
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
        with self.transaction() as conn:
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

    def get_entities_by_ids(self, tenant_id: str, entity_ids: set[str]) -> dict[str, Entity]:
        entities: dict[str, Entity] = {}
        ordered_ids = sorted(entity_ids)
        if not ordered_ids:
            return entities
        with self._conn() as conn:
            for start in range(0, len(ordered_ids), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_ids[start : start + _SQLITE_BIND_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"""SELECT id, entity_json FROM entities
                        WHERE tenant_id=? AND id IN ({placeholders})""",
                    (tenant_id, *batch),
                ).fetchall()
                entities.update({str(row["id"]): Entity.model_validate_json(row["entity_json"]) for row in rows})
        return entities

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
        with self.transaction() as conn:
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
        with self.transaction() as conn:
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

    def candidates_for_proposition(
        self,
        tenant_id: str,
        proposition_key: str,
        *,
        review_states: set[str] | None = None,
        lifecycle_status: str | None = None,
        entity_resolution_status: str | None = None,
        limit: int | None = None,
    ) -> list[KnowledgeCandidate]:
        clauses = ["p.tenant_id=?", "p.proposition_key=?"]
        params: list[Any] = [tenant_id, proposition_key]
        if review_states:
            placeholders = ", ".join("?" for _ in review_states)
            clauses.append(f"c.review_state IN ({placeholders})")
            params.extend(sorted(review_states))
        if lifecycle_status is not None:
            clauses.append("c.lifecycle_status=?")
            params.append(lifecycle_status)
        if entity_resolution_status is not None:
            clauses.append("c.entity_resolution_status=?")
            params.append(entity_resolution_status)
        limit_clause = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            params.append(limit)
            limit_clause = " LIMIT ?"
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT c.candidate_json FROM knowledge_candidates c
                   JOIN proposition_candidates p
                     ON p.candidate_id=c.id AND p.tenant_id=c.tenant_id
                   WHERE {" AND ".join(clauses)}
                   ORDER BY c.created_at, c.id{limit_clause}""",
                params,
            ).fetchall()
        return [KnowledgeCandidate.model_validate_json(row["candidate_json"]) for row in rows]

    def get_candidates_by_ids(
        self,
        tenant_id: str,
        candidate_ids: set[str],
    ) -> dict[str, KnowledgeCandidate]:
        """Bulk-load a bounded caller-provided candidate set."""
        if not candidate_ids:
            return {}
        candidates: dict[str, KnowledgeCandidate] = {}
        ordered_ids = sorted(candidate_ids)
        with self._conn() as conn:
            for start in range(0, len(ordered_ids), _SQLITE_BIND_BATCH_SIZE):
                batch = ordered_ids[start : start + _SQLITE_BIND_BATCH_SIZE]
                placeholders = ", ".join("?" for _ in batch)
                rows = conn.execute(
                    f"""SELECT candidate_json FROM knowledge_candidates
                        WHERE tenant_id=? AND id IN ({placeholders})""",
                    (tenant_id, *batch),
                ).fetchall()
                for row in rows:
                    candidate = KnowledgeCandidate.model_validate_json(row["candidate_json"])
                    candidates[candidate.id] = candidate
        return candidates

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

    def save_corroboration(
        self,
        summary: CorroborationSummary,
        tenant_id: str,
    ) -> tuple[str, bool]:
        digest = hashlib.sha256(
            json.dumps(
                {"tenant_id": tenant_id, "summary": summary.model_dump(mode="json")},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        snapshot_id = f"corroboration_{digest[:20]}"
        with self.transaction() as conn:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO corroboration_snapshots (
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
        return snapshot_id, cursor.rowcount == 1

    def get_promotion_decision(
        self,
        decision_id: str,
        tenant_id: str,
    ) -> PromotionDecision | None:
        with self._conn() as conn:
            row = conn.execute(
                """SELECT decision_json FROM promotion_decisions
                   WHERE decision_id=? AND tenant_id=?""",
                (decision_id, tenant_id),
            ).fetchone()
        return PromotionDecision.model_validate_json(row["decision_json"]) if row else None

    def save_conflict(self, conflict: KnowledgeConflict) -> KnowledgeConflict:
        with self.transaction() as conn:
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
        if offset < 0 or offset > _MAX_LEGACY_AUDIT_OFFSET:
            raise ValueError(f"conflict offset must be between 0 and {_MAX_LEGACY_AUDIT_OFFSET}")
        params: list[str | int]
        if proposition_key is not None:
            query = _conflict_lookup_sql(unresolved_only=unresolved_only)
            params = [tenant_id, proposition_key, tenant_id, proposition_key]
        else:
            clauses = ["tenant_id=?"]
            params = [tenant_id]
            if unresolved_only:
                clauses.append("resolution_status='unresolved'")
            query = (
                f"SELECT conflict_json FROM knowledge_conflicts WHERE {' AND '.join(clauses)} "
                "ORDER BY created_at DESC, conflict_id DESC"
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

    def list_conflicts_page(
        self,
        tenant_id: str = "default",
        *,
        proposition_key: str | None = None,
        unresolved_only: bool = False,
        limit: int = 200,
        cursor: str | None = None,
    ) -> ConflictPage:
        """Return a stable newest-first keyset page of conflict audit rows."""
        if not 1 <= limit <= 500:
            raise ValueError("conflict page limit must be between 1 and 500")
        common_clauses: list[str] = []
        common_params: list[Any] = []
        if unresolved_only:
            common_clauses.append("resolution_status='unresolved'")
        if cursor is not None:
            created_at, conflict_id = _decode_audit_cursor(cursor, resource="conflict")
            common_clauses.append("(created_at < ? OR (created_at = ? AND conflict_id < ?))")
            common_params.extend([created_at, created_at, conflict_id])
        if proposition_key is not None:
            common_sql = f" AND {' AND '.join(common_clauses)}" if common_clauses else ""
            query = f"""SELECT conflict_id, created_at, conflict_json FROM (
                            SELECT conflict_id, created_at, conflict_json
                            FROM knowledge_conflicts
                            WHERE tenant_id=? AND left_proposition_key=?{common_sql}
                            UNION ALL
                            SELECT conflict_id, created_at, conflict_json
                            FROM knowledge_conflicts
                            WHERE tenant_id=? AND right_proposition_key=?
                              AND left_proposition_key<>?{common_sql}
                        )
                        ORDER BY created_at DESC, conflict_id DESC LIMIT ?"""
            params = [
                tenant_id,
                proposition_key,
                *common_params,
                tenant_id,
                proposition_key,
                proposition_key,
                *common_params,
                limit + 1,
            ]
        else:
            clauses = ["tenant_id=?", *common_clauses]
            params = [tenant_id, *common_params, limit + 1]
            query = f"""SELECT conflict_id, created_at, conflict_json FROM knowledge_conflicts
                        WHERE {" AND ".join(clauses)}
                        ORDER BY created_at DESC, conflict_id DESC LIMIT ?"""
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(float(last["created_at"]), str(last["conflict_id"]))
        return ConflictPage(
            conflicts=[KnowledgeConflict.model_validate_json(row["conflict_json"]) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def save_promotion_decision(self, decision: PromotionDecision, tenant_id: str) -> PromotionDecision:
        with self.transaction() as conn:
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
                    tenant_fingerprint=_authority_fingerprint(revision.tenant_id),
                    candidate_fingerprint=_authority_fingerprint(candidate_id),
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
                            tenant_fingerprint=_authority_fingerprint(revision.tenant_id),
                            candidate_fingerprint=_authority_fingerprint(contributor.id),
                            knowledge_ref_fingerprint=_authority_fingerprint(revision.knowledge_id),
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
        if limit is not None and limit < 1:
            raise ValueError("revision page limit must be positive")
        if offset < 0 or offset > _MAX_LEGACY_AUDIT_OFFSET:
            raise ValueError(f"revision offset must be between 0 and {_MAX_LEGACY_AUDIT_OFFSET}")
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

    def list_revisions_page(
        self,
        knowledge_id: str,
        tenant_id: str = "default",
        *,
        limit: int = 200,
        cursor: str | None = None,
    ) -> RevisionPage:
        """Return a stable oldest-first keyset page of immutable revisions."""
        if not 1 <= limit <= 500:
            raise ValueError("revision page limit must be between 1 and 500")
        clauses = ["tenant_id=?", "knowledge_id=?"]
        params: list[Any] = [tenant_id, knowledge_id]
        if cursor is not None:
            clauses.append("revision>?")
            params.append(_decode_revision_cursor(cursor))
        params.append(limit + 1)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT revision, content_json FROM operational_knowledge_revisions
                    WHERE {" AND ".join(clauses)} ORDER BY revision LIMIT ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = encode_cursor(int(visible[-1]["revision"])) if has_more and visible else None
        return RevisionPage(
            revisions=[KnowledgeRevision.model_validate_json(row["content_json"]) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_current_revisions(
        self,
        tenant_id: str = "default",
        *,
        lifecycle_status: str | None = None,
        kind: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[KnowledgeRevision]:
        if limit is not None and limit < 1:
            raise ValueError("knowledge page limit must be positive")
        if offset < 0 or offset > _MAX_LEGACY_AUDIT_OFFSET:
            raise ValueError(f"knowledge offset must be between 0 and {_MAX_LEGACY_AUDIT_OFFSET}")
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
                   WHERE {" AND ".join(clauses)} ORDER BY r.knowledge_id{limit_clause}""",
                params,
            ).fetchall()
        return [KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows]

    def list_current_revisions_page(
        self,
        tenant_id: str = "default",
        *,
        lifecycle_status: str | None = None,
        kind: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> CurrentRevisionPage:
        """Return one stable keyset page of current knowledge revisions."""
        if not 1 <= limit <= 500:
            raise ValueError("knowledge page limit must be between 1 and 500")
        clauses = ["k.tenant_id=?"]
        params: list[Any] = [tenant_id]
        if lifecycle_status is not None:
            clauses.append("k.status=?")
            params.append(lifecycle_status)
        if kind is not None:
            clauses.append("k.kind=?")
            params.append(kind)
        if cursor is not None:
            clauses.append("k.knowledge_id>?")
            params.append(_decode_knowledge_cursor(cursor))
        params.append(limit + 1)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT k.knowledge_id, r.content_json
                    FROM operational_knowledge k
                    JOIN operational_knowledge_revisions r
                      ON r.tenant_id=k.tenant_id AND r.knowledge_id=k.knowledge_id
                     AND r.revision=k.current_revision
                    WHERE {" AND ".join(clauses)}
                    ORDER BY k.knowledge_id LIMIT ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = encode_cursor(str(visible[-1]["knowledge_id"])) if has_more and visible else None
        return CurrentRevisionPage(
            revisions=[KnowledgeRevision.model_validate_json(row["content_json"]) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

    def list_current_revisions_for_scope(
        self,
        scope: KnowledgeScope,
        *,
        limit: int | None = None,
        ignored_dimensions: set[str] | None = None,
        after_knowledge_id: str | None = None,
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
        if after_knowledge_id is not None:
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
                    WHERE {" AND ".join([*base_clauses, *dimension_clauses])}
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
        with self.transaction() as conn:
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
        if offset < 0 or offset > _MAX_LEGACY_AUDIT_OFFSET:
            raise ValueError(f"usage offset must be between 0 and {_MAX_LEGACY_AUDIT_OFFSET}")
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if knowledge_id is not None:
            clauses.append("knowledge_id=?")
            params.append(knowledge_id)
        if investigation_id is not None:
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
                    WHERE {" AND ".join(clauses)}
                    ORDER BY created_at DESC, usage_id DESC{pagination}""",
                params,
            ).fetchall()
        return [KnowledgeUsage.model_validate_json(row["usage_json"]) for row in rows]

    def list_usage_page(
        self,
        *,
        tenant_id: str = "default",
        knowledge_id: str | None = None,
        investigation_id: str | None = None,
        limit: int = 200,
        cursor: str | None = None,
    ) -> UsagePage:
        """Return a stable newest-first keyset page of usage audit rows."""
        if not 1 <= limit <= 500:
            raise ValueError("usage page limit must be between 1 and 500")
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if knowledge_id is not None:
            clauses.append("knowledge_id=?")
            params.append(knowledge_id)
        if investigation_id is not None:
            clauses.append("investigation_id=?")
            params.append(investigation_id)
        if cursor is not None:
            created_at, usage_id = _decode_audit_cursor(cursor, resource="usage")
            clauses.append("(created_at < ? OR (created_at = ? AND usage_id < ?))")
            params.extend([created_at, created_at, usage_id])
        params.append(limit + 1)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT usage_id, created_at, usage_json FROM knowledge_usage_events
                    WHERE {" AND ".join(clauses)}
                    ORDER BY created_at DESC, usage_id DESC LIMIT ?""",
                params,
            ).fetchall()
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(float(last["created_at"]), str(last["usage_id"]))
        return UsagePage(
            usage=[KnowledgeUsage.model_validate_json(row["usage_json"]) for row in visible],
            has_more=has_more,
            next_cursor=next_cursor,
        )

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
        with self.transaction() as conn:
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
                    tenant_fingerprint=_authority_fingerprint(tenant_id),
                    knowledge_ref_fingerprint=_authority_fingerprint(knowledge_id),
                    reason_code="invalid_correction_json",
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
    expected_owner = str(settings.knowledge_tenant_id or "default")
    if _repository is None or _repository.database_path != expected or _repository.tenant_owner != expected_owner:
        _repository = KnowledgeRepository(expected, runtime_settings=settings)
    return _repository
