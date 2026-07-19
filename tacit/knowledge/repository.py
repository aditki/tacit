"""SQLite persistence for governed, immutable Operational Knowledge."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import structlog

from tacit.config import settings
from tacit.knowledge.models import (
    CorroborationSummary,
    Entity,
    EntityAlias,
    EntityResolutionResult,
    KnowledgeCandidate,
    KnowledgeConflict,
    KnowledgeCorrection,
    KnowledgeRevision,
    KnowledgeSnapshot,
    KnowledgeUsage,
    OperationalKnowledgeItem,
    PromotionDecision,
)
from tacit.knowledge.normalization import normalize_entity
from tacit.signals.schema import SQLITE_BUSY_TIMEOUT_MS

logger = structlog.get_logger()


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
CREATE INDEX IF NOT EXISTS idx_knowledge_usage_investigation
    ON knowledge_usage_events(tenant_id, investigation_id, investigation_revision);

CREATE TABLE IF NOT EXISTS knowledge_corrections (
    correction_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    investigation_id TEXT NOT NULL,
    investigation_revision INTEGER NOT NULL,
    correction_type TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    review_state TEXT NOT NULL,
    candidate_ref TEXT NOT NULL,
    correction_json TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

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


class KnowledgeRepository:
    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or _db_path()
        self._ensure_schema()

    @contextmanager
    def _conn(self):
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

    def _ensure_schema(self) -> None:
        with self._conn() as conn:
            conn.executescript(SCHEMA_SQL)
        logger.info("knowledge_repository_init", db_path=str(self._db_path))

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

    def save_candidate(self, candidate: KnowledgeCandidate) -> KnowledgeCandidate:
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT tenant_id FROM knowledge_candidates WHERE id=?",
                (candidate.id,),
            ).fetchone()
            if existing and existing["tenant_id"] != candidate.tenant_id:
                raise ValueError("candidate id already belongs to another tenant")
            conn.execute(
                """INSERT INTO knowledge_candidates (
                   id, tenant_id, kind, payload_ref, proposition_key, scope_json, review_state,
                   lifecycle_status, eligibility, entity_resolution_status, promotion_policy_id,
                   promotion_policy_version, candidate_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                   proposition_key=excluded.proposition_key, scope_json=excluded.scope_json,
                   review_state=excluded.review_state, lifecycle_status=excluded.lifecycle_status,
                   eligibility=excluded.eligibility, entity_resolution_status=excluded.entity_resolution_status,
                   promotion_policy_id=excluded.promotion_policy_id,
                   promotion_policy_version=excluded.promotion_policy_version,
                   candidate_json=excluded.candidate_json, updated_at=excluded.updated_at""",
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
                    (
                        candidate.policy.promotion_policy_ref.rsplit("-", 1)[-1]
                        if candidate.policy.promotion_policy_ref
                        else ""
                    ),
                    candidate.model_dump_json(),
                    _ts(candidate.created_at),
                    _ts(candidate.updated_at),
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
        return candidate

    def get_candidate(self, candidate_id: str, tenant_id: str = "default") -> KnowledgeCandidate | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT candidate_json FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate_id, tenant_id),
            ).fetchone()
        return KnowledgeCandidate.model_validate_json(row["candidate_json"]) if row else None

    def transition_candidate_review(
        self,
        candidate: KnowledgeCandidate,
        *,
        expected_states: set[str],
    ) -> KnowledgeCandidate:
        """Atomically apply one authorized review transition."""
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            placeholders = ",".join("?" for _ in expected_states)
            params = [
                candidate.state.review_state.value,
                candidate.state.eligibility.value,
                candidate.model_dump_json(),
                _ts(candidate.updated_at),
                candidate.id,
                candidate.tenant_id,
                *sorted(expected_states),
            ]
            cursor = conn.execute(
                f"""UPDATE knowledge_candidates
                    SET review_state=?, eligibility=?, candidate_json=?, updated_at=?
                    WHERE id=? AND tenant_id=? AND review_state IN ({placeholders})""",
                params,
            )
            if cursor.rowcount != 1:
                raise ValueError("candidate review state changed; reload before reviewing")
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

    def find_entities(
        self,
        tenant_id: str,
        normalized_value: str,
        expected_kind: str | None = None,
    ) -> list[Entity]:
        params: list[Any] = [tenant_id, normalized_value]
        kind_clause = ""
        if expected_kind:
            kind_clause = " AND e.kind=?"
            params.append(expected_kind)
        with self._conn() as conn:
            rows = conn.execute(
                f"""SELECT DISTINCT e.entity_json FROM entities e
                    LEFT JOIN entity_aliases a ON a.tenant_id=e.tenant_id AND a.entity_ref=e.id
                    WHERE e.tenant_id=? AND e.status='active'
                      AND (e.normalized_name=? OR (
                        a.normalized_value=? AND a.review_state IN ('approved', 'trusted')
                        AND a.lifecycle_status='active')){kind_clause}""",
                [tenant_id, normalized_value, normalized_value, *params[2:]],
            ).fetchall()
        return [Entity.model_validate_json(row["entity_json"]) for row in rows]

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

    def list_propositions(self, tenant_id: str = "default") -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT p.* FROM knowledge_propositions p
                   WHERE p.tenant_id=? AND EXISTS (
                     SELECT 1 FROM proposition_candidates pc
                     JOIN knowledge_candidates c ON c.id=pc.candidate_id AND c.tenant_id=pc.tenant_id
                     WHERE pc.tenant_id=p.tenant_id AND pc.proposition_key=p.proposition_key
                       AND c.review_state IN ('approved', 'trusted')
                       AND c.lifecycle_status = 'active'
                       AND c.entity_resolution_status = 'resolved'
                   ) ORDER BY p.created_at""",
                (tenant_id,),
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
        return conflict

    def list_conflicts(
        self,
        tenant_id: str = "default",
        *,
        proposition_key: str | None = None,
        unresolved_only: bool = False,
    ) -> list[KnowledgeConflict]:
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if proposition_key:
            clauses.append("(left_proposition_key=? OR right_proposition_key=?)")
            params.extend([proposition_key, proposition_key])
        if unresolved_only:
            clauses.append("resolution_status='unresolved'")
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT conflict_json FROM knowledge_conflicts WHERE {' AND '.join(clauses)} ORDER BY created_at DESC",
                params,
            ).fetchall()
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
    ) -> KnowledgeRevision:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            candidate = conn.execute(
                "SELECT tenant_id FROM knowledge_candidates WHERE id=? AND tenant_id=?",
                (candidate_id, revision.tenant_id),
            ).fetchone()
            if candidate is None:
                raise ValueError("promotion candidate does not belong to the knowledge tenant")
            row = conn.execute(
                "SELECT current_revision, created_at FROM operational_knowledge WHERE tenant_id=? AND knowledge_id=?",
                (revision.tenant_id, revision.knowledge_id),
            ).fetchone()
            current = int(row["current_revision"]) if row else 0
            if revision.revision != current + 1:
                raise ValueError(f"expected knowledge revision {current + 1}, got {revision.revision}")
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

    def list_revisions(self, knowledge_id: str, tenant_id: str = "default") -> list[KnowledgeRevision]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT content_json FROM operational_knowledge_revisions
                   WHERE tenant_id=? AND knowledge_id=? ORDER BY revision""",
                (tenant_id, knowledge_id),
            ).fetchall()
        return [KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows]

    def list_current_revisions(self, tenant_id: str = "default") -> list[KnowledgeRevision]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT r.content_json FROM operational_knowledge_revisions r
                   JOIN operational_knowledge k ON k.tenant_id=r.tenant_id
                     AND k.knowledge_id=r.knowledge_id AND k.current_revision=r.revision
                   WHERE r.tenant_id=? ORDER BY r.knowledge_id""",
                (tenant_id,),
            ).fetchall()
        return [KnowledgeRevision.model_validate_json(row["content_json"]) for row in rows]

    def save_snapshot(self, snapshot: KnowledgeSnapshot) -> KnowledgeSnapshot:
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT snapshot_json FROM knowledge_snapshots WHERE tenant_id=? AND fingerprint=?",
                (snapshot.tenant_id, snapshot.fingerprint),
            ).fetchone()
            if existing:
                return KnowledgeSnapshot.model_validate_json(existing["snapshot_json"])
            conn.execute(
                """INSERT INTO knowledge_snapshots (
                   snapshot_id, tenant_id, fingerprint, snapshot_json, created_at
                ) VALUES (?, ?, ?, ?, ?)""",
                (
                    snapshot.id,
                    snapshot.tenant_id,
                    snapshot.fingerprint,
                    snapshot.model_dump_json(),
                    _ts(snapshot.created_at),
                ),
            )
        return snapshot

    def get_snapshot(self, snapshot_id: str, tenant_id: str = "default") -> KnowledgeSnapshot | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT snapshot_json FROM knowledge_snapshots WHERE snapshot_id=? AND tenant_id=?",
                (snapshot_id, tenant_id),
            ).fetchone()
        return KnowledgeSnapshot.model_validate_json(row["snapshot_json"]) if row else None

    def save_usage(self, usage: KnowledgeUsage) -> KnowledgeUsage:
        if (
            self.get_revision(
                usage.knowledge_ref,
                usage.knowledge_revision,
                tenant_id=usage.tenant_id,
            )
            is None
        ):
            raise ValueError("knowledge usage must reference an existing tenant revision")
        if not usage.usage_id:
            usage = usage.model_copy(update={"usage_id": f"usage_{uuid.uuid4().hex[:16]}"})
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO knowledge_usage_events (
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
    ) -> list[KnowledgeUsage]:
        clauses = ["tenant_id=?"]
        params: list[Any] = [tenant_id]
        if knowledge_id:
            clauses.append("knowledge_id=?")
            params.append(knowledge_id)
        if investigation_id:
            clauses.append("investigation_id=?")
            params.append(investigation_id)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT usage_json FROM knowledge_usage_events WHERE {' AND '.join(clauses)} ORDER BY created_at DESC",
                params,
            ).fetchall()
        return [KnowledgeUsage.model_validate_json(row["usage_json"]) for row in rows]

    def save_correction(self, correction: KnowledgeCorrection) -> KnowledgeCorrection:
        now = time.time()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO knowledge_corrections (
                   correction_id, tenant_id, investigation_id, investigation_revision, correction_type,
                   target_ref, review_state, candidate_ref, correction_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(correction_id) DO UPDATE SET
                   review_state=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.review_state ELSE excluded.review_state END,
                   candidate_ref=CASE
                     WHEN excluded.review_state='candidate' AND knowledge_corrections.review_state!='candidate'
                     THEN knowledge_corrections.candidate_ref ELSE excluded.candidate_ref END,
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
                for state in ("active", "stale", "superseded", "expired")
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
