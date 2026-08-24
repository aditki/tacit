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
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

import structlog

from tacit.config import Settings, settings
from tacit.errors import RuntimeOwnershipError, SemanticAuthorizationError
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.knowledge.versioning import version_scope_applies
from tacit.models.schemas import MetricEntry
from tacit.pagination import MAX_COMPATIBILITY_OFFSET, KeysetPage, decode_cursor, encode_cursor
from tacit.query_parsing.languages import language_to_datasource_type
from tacit.runtime_ownership import (
    RuntimeOwnershipDescriptor,
    copy_runtime_settings,
    runtime_descriptor_for_store,
    snapshot_runtime_settings,
)
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
    MAPPING_SOURCE_REF_INDEX_MARKER,
    SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES,
    SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT,
    SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES,
    SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN,
    ensure_governed_projection_audit_triggers,
    ensure_ingested_alert_columns,
    ensure_ingested_dashboard_backend_scope,
    ensure_learning_index,
    ensure_mapping_columns,
    ensure_projection_authority_page_index,
    ensure_schema,
    governed_projection_audit_is_current,
    mark_governed_projection_audit_current,
    projection_matches_authority,
    rebuild_ingested_dashboards_table,
    reconcile_default_tenant_owner_batch,
    reconcile_legacy_signal_schema_batch,
    reconcile_mapping_source_ref_index_batch,
    require_confirmed_default_tenant_owner,
    require_legacy_tenant_owner,
    signal_schema_is_current,
    signal_tenant_owner_is_current,
)
from tacit.signals.projection import (
    normalize_datasource_types,
    resolver_projection_key,
)
from tacit.signals.resolution import SignalResolutionWorkBudget, SignalResolutionWorkLimitError
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
from tacit.sqlite_identity import (
    SQLiteDatabaseTarget,
    activate_sqlite_wal,
    claim_sqlite_database_identity,
    require_sqlite_database_identity,
    sqlite_database_path,
)
from tacit.tenancy import TenantBoundaryError, resolve_tenant_boundary

logger = structlog.get_logger()
_DEFAULT_OWNER_MIGRATION_BATCH_SIZE = 500
_LEGACY_SCHEMA_MIGRATION_BATCH_SIZE = 500
_MAPPING_SOURCE_REF_MIGRATION_BATCH_SIZE = 500
_PROJECTION_AUDIT_BATCH_SIZE = 500
_PROJECTION_AUTHORITY_VALIDATION_BATCH_SIZE = 100
_PROJECTION_AUDIT_MAX_RETRIES = 3
_SIGNAL_RESOLUTION_PAGE_SIZE = 500
_STALE_SOURCE_PAGE_SIZE = 500
_SIGNAL_RESOLUTION_MIN_SCAN_LIMIT = 10_000
_SIGNAL_RESOLUTION_MAX_SCAN_LIMIT = 100_000
_SIGNAL_RESOLUTION_SCAN_MULTIPLIER = 100
_ARTIFACT_COUNT_BATCH_SIZE = 200
_SIGNAL_TAXONOMY_COMPATIBILITY_LIMIT = 500
_SQLITE_INTEGER_MIN = -(2**63)
_SQLITE_INTEGER_MAX = 2**63 - 1
_ARTIFACT_EXTRACTION_TABLES = {
    "evidence_requirements": "evidence_requirements",
    "ownership_hints": "ownership_hints",
    "dependency_hints": "dependency_hints",
    "signal_mapping_candidates": "signal_mapping_candidates",
}


def _finite_cursor_float(value: object, *, message: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(message)
    try:
        converted = float(value)
    except (OverflowError, TypeError, ValueError) as exc:
        raise ValueError(message) from exc
    if not math.isfinite(converted):
        raise ValueError(message)
    return converted


def _sqlite_cursor_integer(value: object, *, message: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(message)
    if not _SQLITE_INTEGER_MIN <= value <= _SQLITE_INTEGER_MAX:
        raise ValueError(message)
    return value


def _decode_artifact_cursor(cursor: str) -> tuple[float, int]:
    try:
        raw_updated_at, raw_id = decode_cursor(cursor, field_count=2)
        updated_at = _finite_cursor_float(raw_updated_at, message="invalid artifact cursor")
        row_id = _sqlite_cursor_integer(raw_id, message="invalid artifact cursor")
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid artifact cursor") from exc
    return updated_at, row_id


def _decode_extraction_cursor(cursor: str) -> tuple[str, str]:
    try:
        raw_generation, raw_id = decode_cursor(cursor, field_count=2)
        if not isinstance(raw_generation, str) or len(raw_generation) > 128:
            raise ValueError
        if not isinstance(raw_id, str) or len(raw_id) > 500:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid extraction cursor") from exc
    return raw_generation, raw_id


def _decode_signal_mapping_cursor(cursor: str) -> tuple[int, float, int]:
    try:
        raw_priority, raw_confidence, raw_id = decode_cursor(cursor, field_count=3)
        if isinstance(raw_priority, bool) or not isinstance(raw_priority, int) or raw_priority not in {0, 1}:
            raise ValueError
        confidence = _finite_cursor_float(raw_confidence, message="invalid signal mapping cursor")
        row_id = _sqlite_cursor_integer(raw_id, message="invalid signal mapping cursor")
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid signal mapping cursor") from exc
    return raw_priority, confidence, row_id


def _ingested_source_page_index(
    source: str,
    *,
    status: str | None,
    backend_name: str | None,
) -> str:
    """Choose the static keyset index matching the active source filters."""
    if source not in {"dashboard", "alert"}:
        raise ValueError("unsupported ingested source")
    prefix = f"idx_ingested_{source}"
    if backend_name is not None and status:
        return f"{prefix}_backend_status_page"
    if backend_name is not None:
        return f"{prefix}_backend_page"
    if status:
        return f"{prefix}_status_page"
    return f"{prefix}_page"


def _decode_signal_type_cursor(cursor: str) -> tuple[str, str]:
    try:
        raw_category, raw_signal_type = decode_cursor(cursor, field_count=2)
        if not isinstance(raw_category, str) or len(raw_category) > 500:
            raise ValueError
        if not isinstance(raw_signal_type, str) or len(raw_signal_type) > 500:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid signal type cursor") from exc
    return raw_category, raw_signal_type


_INTEGER_KEYSET_COLUMNS = frozenset({"id", "rowid", "mapping.rowid"})


def _ascending_integer_keyset(
    column: str,
    after: int | None,
) -> tuple[str, tuple[int, ...]]:
    """Return an indexed keyset predicate without reserving a legal integer key."""
    if column not in _INTEGER_KEYSET_COLUMNS:
        raise ValueError("unsupported integer keyset column")
    if after is None:
        return "", ()
    return f" AND {column}>?", (after,)


def _validated_ingested_source_cursor(
    before_created_at: float | None,
    before_id: int | None,
) -> tuple[float, int] | None:
    """Validate one public learning cursor without reserving legal persisted values."""
    if (before_created_at is None) != (before_id is None):
        raise ValueError("before_created_at and before_id must be supplied together")
    if before_created_at is None or before_id is None:
        return None
    timestamp = _finite_cursor_float(before_created_at, message="before_created_at must be finite")
    row_id = _sqlite_cursor_integer(before_id, message="before_id must be a SQLite integer")
    return timestamp, row_id


def _ingested_source_page_statement(
    source: str,
    *,
    tenant_id: str,
    status: str | None,
    backend_name: str | None,
    cursor: tuple[float, int] | None,
    limit: int,
    offset: int,
) -> tuple[str, tuple[object, ...]]:
    """Build one indexed dashboard/alert keyset page without narrowing legal keys."""
    if source not in {"dashboard", "alert"}:
        raise ValueError("unsupported ingested source")
    table = f"ingested_{source}s"
    conditions = ["tenant_id = ?"]
    params: list[object] = [tenant_id]
    if status:
        conditions.append("status = ?")
        params.append(status)
    if backend_name is not None:
        conditions.append("backend_name = ?")
        params.append(backend_name)
    if cursor is not None:
        conditions.append("(created_at, id) < (?, ?)")
        params.extend(cursor)
    params.extend((limit, offset))
    page_index = _ingested_source_page_index(
        source,
        status=status,
        backend_name=backend_name,
    )
    sql = f"""SELECT * FROM {table} INDEXED BY {page_index}
              WHERE {" AND ".join(conditions)}
              ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?"""
    return sql, tuple(params)


def _projection_mapping_page_statement(
    *,
    tenant_id: str,
    governance_ref: str,
    governance_revision: int,
    after_id: int | None,
    limit: int,
) -> tuple[str, tuple[object, ...]]:
    """Build the bounded exact-authority seek used by projection validation."""
    if limit < 1:
        raise ValueError("projection mapping page limit must be positive")
    id_clause, id_params = _ascending_integer_keyset("id", after_id)
    sql = f"""SELECT id AS mapping_id, tenant_id, governance_ref,
                      governance_revision, signal_type, metric_pattern,
                      context_datasource_types
               FROM signal_metric_mappings INDEXED BY idx_smm_governed_revision_audit
               WHERE tenant_id=? AND governance_ref=? AND governance_revision=?
                 AND governance_ref!='' AND review_state IN ('approved', 'trusted')
                 {id_clause}
               ORDER BY id LIMIT ?"""
    return sql, (tenant_id, governance_ref, governance_revision, *id_params, limit)


def _projection_audit_key_page_statement(
    kind: str,
    *,
    after: tuple[str, str, int, int] | tuple[str, int] | None,
    limit: int,
) -> tuple[str, tuple[object, ...]]:
    """Page only projection keys through the matching partial covering index."""
    if limit < 1:
        raise ValueError("projection audit page limit must be positive")
    if kind == "governed":
        params: list[object] = []
        cursor_clause = ""
        if after is not None:
            if len(after) != 4:
                raise ValueError("invalid governed projection audit cursor")
            cursor_clause = "AND (tenant_id, governance_ref, governance_revision, id) > (?, ?, ?, ?)"
            params.extend(after)
        sql = f"""SELECT tenant_id, governance_ref, governance_revision, id AS mapping_id
                   FROM signal_metric_mappings INDEXED BY idx_smm_governed_revision_audit
                   WHERE governance_ref!='' AND review_state IN ('approved', 'trusted')
                     {cursor_clause}
                   ORDER BY tenant_id, governance_ref, governance_revision, id LIMIT ?"""
    elif kind == "ungoverned":
        params = []
        cursor_clause = ""
        if after is not None:
            if len(after) != 2:
                raise ValueError("invalid ungoverned projection audit cursor")
            cursor_clause = "AND (tenant_id, id) > (?, ?)"
            params.extend(after)
        sql = f"""SELECT tenant_id, id AS mapping_id
                   FROM signal_metric_mappings INDEXED BY idx_smm_ungoverned_audit
                   WHERE governance_ref='' AND source_type!='bootstrap'
                     AND review_state IN ('approved', 'trusted') {cursor_clause}
                   ORDER BY tenant_id, id LIMIT ?"""
    else:
        raise ValueError("unsupported projection audit kind")
    params.append(limit)
    return sql, tuple(params)


def _projection_authority_audit_page_statement(
    *,
    after: tuple[str, str, int, int] | None,
    limit: int,
) -> tuple[str, tuple[object, ...]]:
    """Page governed projection keys and exact authority state in one indexed join."""
    if limit < 1:
        raise ValueError("projection authority audit page limit must be positive")
    params: list[object] = []
    cursor_clause = ""
    if after is not None:
        if len(after) != 4:
            raise ValueError("invalid governed projection authority cursor")
        cursor_clause = """AND (
            mapping.tenant_id, mapping.governance_ref,
            mapping.governance_revision, mapping.id
        ) > (?, ?, ?, ?)"""
        params.extend(after)
    sql = f"""SELECT mapping.tenant_id, mapping.governance_ref,
                      mapping.governance_revision, mapping.id AS mapping_id,
                      CASE WHEN revision.knowledge_id IS NULL THEN 0 ELSE 1 END
                          AS authority_active
               FROM signal_metric_mappings AS mapping
                    INDEXED BY idx_smm_governed_revision_audit
               LEFT JOIN operational_knowledge AS current
                 ON current.tenant_id=mapping.tenant_id
                AND current.knowledge_id=mapping.governance_ref
                AND current.current_revision=mapping.governance_revision
                AND current.kind='signal_mapping' AND current.status='active'
               LEFT JOIN operational_knowledge_revisions AS revision
                 ON revision.tenant_id=current.tenant_id
                AND revision.knowledge_id=current.knowledge_id
                AND revision.revision=current.current_revision
                AND revision.lifecycle_status='active'
                AND revision.eligibility!='ineligible'
               WHERE mapping.governance_ref!=''
                 AND mapping.review_state IN ('approved', 'trusted')
                 {cursor_clause}
               ORDER BY mapping.tenant_id, mapping.governance_ref,
                        mapping.governance_revision, mapping.id LIMIT ?"""
    params.append(limit)
    return sql, tuple(params)


def _active_projection_authority_page_statement(
    *,
    after: tuple[str, str] | None,
    limit: int,
) -> tuple[str, tuple[object, ...]]:
    """Page active signal authorities with work bounded by an exact covering index."""
    if limit < 1:
        raise ValueError("projection authority page limit must be positive")
    params: list[object] = []
    cursor_clause = ""
    if after is not None:
        if len(after) != 2:
            raise ValueError("invalid active projection authority cursor")
        cursor_clause = "AND (current.tenant_id, current.knowledge_id) > (?, ?)"
        params.extend(after)
    sql = f"""SELECT current.tenant_id, current.knowledge_id, current.current_revision,
                      length(CAST(revision.content_json AS BLOB)) AS content_bytes
               FROM operational_knowledge current
                    INDEXED BY idx_operational_knowledge_signal_projection_page
               JOIN operational_knowledge_revisions revision
                 ON revision.tenant_id=current.tenant_id
                AND revision.knowledge_id=current.knowledge_id
                AND revision.revision=current.current_revision
               WHERE current.kind='signal_mapping' AND current.status='active'
                 AND revision.lifecycle_status='active'
                 AND revision.eligibility!='ineligible'
                 {cursor_clause}
               ORDER BY current.tenant_id, current.knowledge_id LIMIT ?"""
    params.append(limit)
    return sql, tuple(params)


def _projection_audit_mapping_rows_statement(mapping_ids: list[int]) -> tuple[str, tuple[int, ...]]:
    """Hydrate one bounded projection key page through SQLite's integer primary key."""
    if not mapping_ids or len(mapping_ids) > _PROJECTION_AUDIT_BATCH_SIZE:
        raise ValueError("projection audit mapping page is empty or exceeds the batch limit")
    normalized = tuple(
        _sqlite_cursor_integer(mapping_id, message="projection mapping id must be a SQLite integer")
        for mapping_id in mapping_ids
    )
    placeholders = ",".join("?" for _ in normalized)
    return (
        f"SELECT * FROM signal_metric_mappings WHERE id IN ({placeholders}) ORDER BY id",
        normalized,
    )


def _projection_authority_payload_byte_limit(mapping_limit: int) -> int:
    """Bound one authority payload before SQLite materializes it in Python."""
    if mapping_limit < 1:
        raise ValueError("projection mapping limit must be positive")
    return (64 * 1024) + (mapping_limit * 4096)


def _validated_source_ref_payload(values: list[object]) -> tuple[list[str], str]:
    """Validate and serialize one bounded source-reference fanout."""
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError("source_refs must contain non-blank, trimmed strings")
        if value not in seen:
            seen.add(value)
            normalized.append(value)
    if len(normalized) > SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT:
        raise ValueError("source_refs exceeds the count limit")
    payload = json.dumps(normalized)
    if len(payload.encode("utf-8")) > SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES:
        raise ValueError("source_refs exceeds the byte limit")
    return normalized, payload


@dataclass(frozen=True)
class _ValidatedProjectionRevision:
    signal_type: str
    variants: tuple[tuple[str, tuple[str, ...], float], ...]

    @property
    def expected_variants(self) -> frozenset[tuple[str, str, tuple[str, ...]]]:
        return frozenset(
            (self.signal_type, metric_pattern, datasource_types)
            for metric_pattern, datasource_types, _ in self.variants
        )


def _validate_projection_revision(
    revision: Any,
    *,
    mapping_limit: int,
    serialized_payload_bytes: int | None = None,
) -> _ValidatedProjectionRevision:
    """Apply one exact bounded authority contract to repair and write paths."""
    authority_ref = (revision.tenant_id, revision.knowledge_id, revision.revision)
    if serialized_payload_bytes is None:
        try:
            serialized_payload_bytes = len(revision.model_dump_json().encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise _projection_validation_error(
                "governed_signal_projection_payload_invalid",
                "active governed signal authority has invalid resolver payload",
                authority_ref=authority_ref,
                exception=exc,
            ) from exc
    payload_limit = _projection_authority_payload_byte_limit(mapping_limit)
    if serialized_payload_bytes > payload_limit:
        raise _projection_validation_error(
            "governed_signal_projection_payload_limit_exceeded",
            "active governed signal authority exceeds the payload byte limit",
            authority_ref=authority_ref,
            expected_count=payload_limit,
            projected_count=serialized_payload_bytes,
            validation_reason="resolver_payload_byte_limit_exceeded",
        )

    resolver_payload = revision.resolver_payload
    resolver_mappings = resolver_payload.get("mappings") if isinstance(resolver_payload, dict) else None
    if not isinstance(resolver_mappings, list):
        raise _projection_validation_error(
            "governed_signal_projection_payload_invalid",
            "active governed signal authority has invalid resolver payload",
            authority_ref=authority_ref,
            validation_reason="resolver_mappings_not_list",
        )
    if len(resolver_mappings) > mapping_limit:
        raise _projection_validation_error(
            "governed_signal_projection_mapping_limit_exceeded",
            "active governed signal authority exceeds the resolver mapping limit",
            authority_ref=authority_ref,
            expected_count=mapping_limit,
            projected_count=len(resolver_mappings),
            validation_reason="resolver_mapping_limit_exceeded",
        )

    signal_type = str(revision.proposition.concept_ref or "").removeprefix("signal:")
    variants: dict[tuple[str, tuple[str, ...]], float] = {}
    for mapping in resolver_mappings:
        if not isinstance(mapping, dict):
            raise _projection_validation_error(
                "governed_signal_projection_mapping_invalid",
                "active governed signal authority contains an invalid resolver mapping",
                authority_ref=authority_ref,
                validation_reason="resolver_mapping_not_object",
            )
        metric_pattern = mapping.get("metric_pattern")
        datasource_types = mapping.get("context_datasource_types", [])
        confidence = mapping.get("confidence", 0.5)
        if (
            not isinstance(metric_pattern, str)
            or not metric_pattern
            or metric_pattern != metric_pattern.strip()
            or not isinstance(datasource_types, list)
            or any(
                not isinstance(value, str) or not value.strip() or value != value.strip() for value in datasource_types
            )
            or isinstance(confidence, bool)
            or not isinstance(confidence, (int, float))
            or not math.isfinite(float(confidence))
            or not 0.0 <= float(confidence) <= 1.0
        ):
            raise _projection_validation_error(
                "governed_signal_projection_mapping_invalid",
                "active governed signal authority contains an invalid resolver mapping",
                authority_ref=authority_ref,
                validation_reason="resolver_mapping_fields_invalid",
            )
        key = (metric_pattern, normalize_datasource_types(datasource_types))
        variants[key] = max(variants.get(key, 0.0), float(confidence))
    if not signal_type or not variants:
        raise _projection_validation_error(
            "governed_signal_projection_payload_missing",
            "active governed signal authority has no exact resolver payload",
            authority_ref=authority_ref,
        )
    return _ValidatedProjectionRevision(
        signal_type=signal_type,
        variants=tuple(
            (metric_pattern, datasource_types, confidence)
            for (metric_pattern, datasource_types), confidence in sorted(variants.items())
        ),
    )


@dataclass(frozen=True)
class _PreparedProjectionAuthority:
    tenant_id: str
    knowledge_id: str
    revision_number: int
    content_json: str
    content: dict[str, Any]
    revision: Any
    expected_variants: frozenset[tuple[str, str, tuple[str, ...]]]
    review_state: str
    lifecycle_status: str
    eligibility: str

    @property
    def key(self) -> tuple[str, str, int]:
        return self.tenant_id, self.knowledge_id, self.revision_number

    def validation_record(self) -> dict[str, Any]:
        return {
            "content": self.content,
            "content_json": self.content_json,
            "review_state": self.review_state,
            "lifecycle_status": self.lifecycle_status,
            "eligibility": self.eligibility,
        }


def _pattern_literal_fragments(pattern: str) -> tuple[str, ...]:
    """Return only text that every successful pattern match must contain."""
    if not any(token in pattern for token in ("*", "?", "[")):
        return (pattern,)
    fragments: list[str] = []
    current: list[str] = []
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "[":
            if current:
                fragments.append("".join(current))
                current = []
            closing = index + 1
            if closing < len(pattern) and pattern[closing] in {"!", "^"}:
                closing += 1
            if closing < len(pattern) and pattern[closing] == "]":
                closing += 1
            while closing < len(pattern) and pattern[closing] != "]":
                closing += 1
            if closing >= len(pattern):
                # An unusual or malformed class is cheap to scan exactly. Do
                # not derive literals that could turn the prefilter lossy.
                break
            index = closing + 1
            continue
        if character in {"*", "?"}:
            if current:
                fragments.append("".join(current))
                current = []
            index += 1
            continue
        current.append(character)
        index += 1
    if current:
        fragments.append("".join(current))
    return tuple(fragments)


class _ProjectionAuditChanged(RuntimeError):
    pass


_LEARNING_LIST_MAX_LIMIT = 10_000


def _diagnostic_fingerprint(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:16]


def _exception_diagnostics(reason_code: str, exc: BaseException) -> dict[str, str]:
    return {
        "reason_code": reason_code,
        "exception_class": type(exc).__name__[:64],
        "error_fingerprint": _diagnostic_fingerprint(exc),
    }


def _authority_ref_fingerprint(tenant_id: object, knowledge_id: object, revision: object) -> str:
    return _diagnostic_fingerprint(f"{tenant_id}\0{knowledge_id}\0{revision}")


def _projection_cursor_fingerprint(cursor: tuple[str, str] | None) -> str:
    if cursor is None:
        return ""
    tenant_id, knowledge_id = cursor
    return _diagnostic_fingerprint(f"{tenant_id}\0{knowledge_id}")


class _ProjectionValidationError(RuntimeError):
    """A projection validation failure whose message is safe to expose."""


def _projection_validation_error(
    reason_code: str,
    message: str,
    *,
    authority_ref: tuple[object, object, object] | None = None,
    mapping_ref: tuple[object, object] | None = None,
    exception: BaseException | None = None,
    expected_count: int | None = None,
    projected_count: int | None = None,
    validation_reason: str = "",
) -> _ProjectionValidationError:
    diagnostics: dict[str, object] = {"reason_code": reason_code}
    if authority_ref is not None:
        diagnostics["authority_ref_fingerprint"] = _authority_ref_fingerprint(*authority_ref)
    if mapping_ref is not None:
        diagnostics["mapping_ref_fingerprint"] = _diagnostic_fingerprint(f"{mapping_ref[0]}\0{mapping_ref[1]}")
    if exception is not None:
        diagnostics["exception_class"] = type(exception).__name__[:64]
        diagnostics["error_fingerprint"] = _diagnostic_fingerprint(exception)
    if expected_count is not None:
        diagnostics["expected_count"] = expected_count
    if projected_count is not None:
        diagnostics["projected_count"] = projected_count
    if validation_reason:
        diagnostics["validation_reason"] = validation_reason
    logger.error("governed_signal_projection_validation_failed", **diagnostics)
    return _ProjectionValidationError(message)


__all__ = [
    "ArtifactGenerationConflictError",
    "LearningIndexUnavailable",
    "ResolvedMetricSignal",
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
class ResolvedMetricSignal:
    """One reverse-resolved metric-to-signal mapping."""

    signal_type: str
    entry: MetricEntry
    confidence: float
    signal_family: str = ""
    metric_pattern: str = ""
    governance_ref: str = ""
    governance_revision: int = 0

    @property
    def knowledge_revision_ref(self) -> KnowledgeRevisionRef | None:
        if not self.governance_ref or self.governance_revision < 1:
            return None
        return KnowledgeRevisionRef(self.governance_ref, self.governance_revision)


@dataclass(frozen=True)
class _ReverseMappingMatch:
    mapping: dict[str, Any]
    metric_names: tuple[str, ...]


@dataclass(frozen=True)
class _PinnedGovernedMappings:
    tenant_id: str
    mappings: tuple[dict[str, Any], ...]


def _require_pinned_governed_mapping_tenant(*, pinned_tenant: str, requested_tenant: str) -> None:
    """Fail closed when one execution context attempts to cross tenant authority."""
    if pinned_tenant != requested_tenant:
        raise TenantBoundaryError(
            "Pinned governed mappings cross the tenant boundary",
            status_code=403,
        )


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
    return sqlite_database_path(custom or _DEFAULT_DB_PATH)


class SignalStore:
    """SQLite-backed semantic signal mapping store."""

    supports_signal_resolution_work_budget = True

    def __init__(self, db_path: Path | None = None, *, runtime_settings: Settings | None = None):
        settings_owner = runtime_settings or settings
        selected_path = db_path or settings_owner.signals_db_path or _DEFAULT_DB_PATH
        self._settings = snapshot_runtime_settings(
            settings_owner,
            database_role="signals" if db_path is not None else None,
            database_path=db_path,
        )
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        self._legacy_tenant = configured_tenant if configured_tenant != "*" else None
        self._db_path = sqlite_database_path(selected_path)
        self._runtime_ownership = runtime_descriptor_for_store(
            component="signal_store",
            runtime_settings=self._settings,
            database_role="signals",
            database_path=self._db_path,
        )
        self._database_id: str | None = None
        self._sqlite_target = SQLiteDatabaseTarget(self._db_path)
        self._transaction_connection: ContextVar[sqlite3.Connection | None] = ContextVar(
            f"signal_transaction_{id(self)}",
            default=None,
        )
        self._pinned_governed_mappings: ContextVar[_PinnedGovernedMappings | None] = ContextVar(
            f"signal_knowledge_pin_{id(self)}",
            default=None,
        )
        self._bootstrap_signal_definitions = self._load_bootstrap_signal_definitions()
        self._preflight_owner_before_mutation()
        self._ensure_schema()

    def new_signal_resolution_work_budget(
        self,
        *,
        max_calls: int = 1,
        max_mapping_catalog_comparisons: int | None = None,
        max_results: int | None = None,
    ) -> SignalResolutionWorkBudget:
        """Create a first-party resolver budget from this store's limits."""
        return SignalResolutionWorkBudget(
            max_calls=max_calls,
            max_mapping_catalog_comparisons=(
                int(self._settings.signal_resolution_mapping_limit)
                * int(self._settings.signal_resolution_catalog_limit)
                if max_mapping_catalog_comparisons is None
                else max_mapping_catalog_comparisons
            ),
            max_results=(
                int(self._settings.signal_resolution_catalog_limit)
                + int(self._settings.signal_resolution_mapping_limit)
                if max_results is None
                else max_results
            ),
        )

    def _log_resolution_work_limit(
        self,
        exc: SignalResolutionWorkLimitError,
        budget: SignalResolutionWorkBudget,
        *,
        operation: str,
    ) -> None:
        counters = {key: min(max(value, 0), 100_000_000) for key, value in budget.counters().items()}
        logger.error(
            SignalResolutionWorkLimitError.reason_code,
            reason_code=SignalResolutionWorkLimitError.reason_code,
            operation=operation,
            dimension=exc.dimension,
            observed=min(max(exc.observed, 0), 100_000_000),
            limit=min(max(exc.limit, 0), 100_000_000),
            **counters,
        )

    def _begin_resolution_work(
        self,
        budget: SignalResolutionWorkBudget | None,
        *,
        operation: str,
    ) -> SignalResolutionWorkBudget:
        active = budget or self.new_signal_resolution_work_budget(
            max_mapping_catalog_comparisons=(
                int(self._settings.signal_resolution_pattern_check_limit) if operation == "forward" else None
            )
        )
        try:
            active.begin_call()
        except SignalResolutionWorkLimitError as exc:
            self._log_resolution_work_limit(exc, active, operation=operation)
            raise
        return active

    def _reserve_resolution_comparisons(
        self,
        budget: SignalResolutionWorkBudget,
        *,
        mapping_count: int,
        eligible_catalog_count: int,
        operation: str,
    ) -> None:
        try:
            budget.reserve_mapping_catalog_comparisons(
                mapping_count,
                eligible_catalog_count,
            )
        except SignalResolutionWorkLimitError as exc:
            self._log_resolution_work_limit(exc, budget, operation=operation)
            raise

    def _consume_resolution_result(
        self,
        budget: SignalResolutionWorkBudget,
        *,
        operation: str,
    ) -> None:
        try:
            budget.consume_result()
        except SignalResolutionWorkLimitError as exc:
            self._log_resolution_work_limit(exc, budget, operation=operation)
            raise

    @property
    def runtime_settings(self) -> Settings:
        """Return the immutable settings owner for this store."""
        return copy_runtime_settings(self._settings)

    @property
    def database_path(self) -> Path:
        """Return the persistence identity used for runtime composition checks."""
        return self._db_path

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return this store's public runtime ownership descriptor."""
        return self._runtime_ownership

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
            _require_pinned_governed_mapping_tenant(
                pinned_tenant=mapping_tenant,
                requested_tenant=tenant_id,
            )
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
        conn = self._sqlite_target.connect(timeout_ms=SQLITE_BUSY_TIMEOUT_MS)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
            observed_database_id = require_sqlite_database_identity(
                conn,
                role="signals",
                expected_database_id=self._database_id,
            )
            if observed_database_id is not None:
                self._database_id = observed_database_id
            if activate_sqlite_wal(conn, timeout_ms=SQLITE_BUSY_TIMEOUT_MS) != "wal":
                raise RuntimeOwnershipError("Signal store requires confirmed SQLite WAL mode")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _bind_external_connection(
        self,
        connection: sqlite3.Connection,
        *,
        require_transaction: bool,
    ) -> sqlite3.Connection:
        """Bind one externally supplied connection to this store's generation."""
        self._sqlite_target.bind_connection(connection)
        observed_database_id = require_sqlite_database_identity(
            connection,
            role="signals",
            expected_database_id=self._database_id,
        )
        if observed_database_id is None:
            raise RuntimeOwnershipError("Signal store cannot join an unclaimed database transaction")
        if require_transaction and not connection.in_transaction:
            raise RuntimeOwnershipError("Signal store external writes require an active transaction")
        self._database_id = observed_database_id
        return connection

    @contextmanager
    def _write_connection(self, connection: sqlite3.Connection | None = None):
        """Yield this store's connection or bind one verified active unit of work."""
        if connection is None:
            with self._conn() as conn:
                yield conn
            return
        bound = self._bind_external_connection(connection, require_transaction=True)
        if self._transaction_connection.get() is not bound:
            self._require_owner_on_connection(bound)
        yield bound

    @contextmanager
    def transaction(self):
        """Run signal-store writes in one immediate, nestable transaction."""
        active = self._transaction_connection.get()
        if active is not None:
            yield active
            return
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_owner_on_connection(conn)
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
            conn.execute("BEGIN IMMEDIATE")
            self._require_owner_on_connection(conn)
            schema_current = signal_schema_is_current(conn)
            audit_current = governed_projection_audit_is_current(conn)
            owner_current = signal_tenant_owner_is_current(conn, legacy_tenant=self._legacy_tenant)
            if schema_current and audit_current and owner_current:
                self._database_id = claim_sqlite_database_identity(
                    conn,
                    role="signals",
                    expected_database_id=self._database_id,
                )
                logger.info(
                    "signal_store_init",
                    reason_code="signal_store_initialized",
                    database_fingerprint=_diagnostic_fingerprint(self._db_path),
                    schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
                    migration_required=False,
                )
                return
            projection_repair_only = schema_current and owner_current
            if projection_repair_only:
                self._database_id = claim_sqlite_database_identity(
                    conn,
                    role="signals",
                    expected_database_id=self._database_id,
                )

        if projection_repair_only:
            self._reconcile_governed_projection_audit_batched()
            logger.info(
                "signal_store_init",
                reason_code="signal_store_initialized",
                database_fingerprint=_diagnostic_fingerprint(self._db_path),
                schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
                migration_required=True,
                projection_repair_batched=True,
            )
            return

        bootstrap_signal_definitions = self._bootstrap_signal_definitions

        with self._conn() as conn:
            # Structural rebuilds remain atomic. Potentially large owner and
            # projection reconciliation runs in restartable batches below.
            conn.execute("BEGIN IMMEDIATE")
            self._require_owner_on_connection(conn)
            if (
                signal_schema_is_current(conn)
                and governed_projection_audit_is_current(conn)
                and signal_tenant_owner_is_current(conn, legacy_tenant=self._legacy_tenant)
            ):
                self._database_id = claim_sqlite_database_identity(
                    conn,
                    role="signals",
                    expected_database_id=self._database_id,
                )
                logger.info(
                    "signal_store_init",
                    reason_code="signal_store_initialized",
                    database_fingerprint=_diagnostic_fingerprint(self._db_path),
                    schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
                    migration_required=False,
                )
                return
            schema_complete = ensure_schema(
                conn,
                legacy_tenant=self._legacy_tenant,
                bootstrap_signal_definitions=bootstrap_signal_definitions,
            )
            self._database_id = claim_sqlite_database_identity(
                conn,
                role="signals",
                expected_database_id=self._database_id,
            )
        if not schema_complete:
            self._reconcile_legacy_signal_schema_batched()
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._require_owner_on_connection(conn)
                if not ensure_schema(
                    conn,
                    legacy_tenant=self._legacy_tenant,
                    bootstrap_signal_definitions=bootstrap_signal_definitions,
                ):
                    raise RuntimeError("Legacy signal schema copy did not reach its final swap")
                self._database_id = claim_sqlite_database_identity(
                    conn,
                    role="signals",
                    expected_database_id=self._database_id,
                )
        self._reconcile_mapping_source_ref_index_batched()
        self._reconcile_default_tenant_owner_batched()
        self._reconcile_governed_projection_audit_batched()
        logger.info(
            "signal_store_init",
            reason_code="signal_store_initialized",
            database_fingerprint=_diagnostic_fingerprint(self._db_path),
            schema_marker=CURRENT_SIGNAL_SCHEMA_MARKER,
            migration_required=True,
            projection_repair_batched=True,
        )

    def _load_bootstrap_signal_definitions(self) -> dict[str, dict[str, Any]] | None:
        bootstrap_signal_definitions: dict[str, dict[str, Any]] | None = None
        try:
            import yaml

            resource = files("tacit.data").joinpath("signals.yaml")
            if resource.is_file():
                with resource.open() as stream:
                    bootstrap_data = yaml.safe_load(stream) or {}
                bootstrap_signal_definitions = dict(bootstrap_data.get("signals", {}))
        except Exception as exc:
            logger.warning(
                "signals_bootstrap_taxonomy_unavailable",
                reason_code="signals_bootstrap_taxonomy_unavailable",
                exception_class=type(exc).__name__[:64],
                error_fingerprint=_diagnostic_fingerprint(exc),
            )
        return bootstrap_signal_definitions

    def _reject_owner_preflight(self, *, reason_code: str, recorded_owner: str | None = None) -> None:
        configured_owner = self._legacy_tenant or "*"
        fields: dict[str, object] = {
            "reason_code": reason_code,
            "configured_owner_class": "wildcard" if configured_owner == "*" else "pinned",
            "configured_owner_fingerprint": _diagnostic_fingerprint(configured_owner),
        }
        if recorded_owner:
            fields.update(
                {
                    "recorded_owner_class": "wildcard" if recorded_owner == "*" else "pinned",
                    "recorded_owner_fingerprint": _diagnostic_fingerprint(recorded_owner),
                }
            )
        logger.error("signal_owner_preflight_rejected", **fields)
        raise RuntimeError(f"Signal store owner preflight rejected (reason={reason_code})")

    def _preflight_owner_before_mutation(self) -> None:
        def inspect_owner(conn: sqlite3.Connection) -> None:
            conn.row_factory = sqlite3.Row
            self._require_owner_on_connection(conn)

        self._sqlite_target.read_existing_readonly(
            inspect_owner,
            timeout_ms=SQLITE_BUSY_TIMEOUT_MS,
        )

    def _require_owner_on_connection(self, conn: sqlite3.Connection) -> None:
        """Revalidate the tenant owner on the caller's current database generation."""
        observed_database_id = require_sqlite_database_identity(
            conn,
            role="signals",
            expected_database_id=self._database_id,
        )
        if observed_database_id is not None:
            self._database_id = observed_database_id
        metadata_exists = conn.execute("""SELECT 1 FROM sqlite_master
                   WHERE type='table' AND name='signal_tenant_migration_metadata'""").fetchone() is not None
        markers: dict[str, str] = {}
        if metadata_exists:
            markers = {
                str(row["key"]): str(row["value"])
                for row in conn.execute("""SELECT key, value FROM signal_tenant_migration_metadata
                       WHERE key IN (
                         'default_owner_v1', 'default_owner_in_progress_v1',
                         'legacy_schema_owner_v1'
                       )""").fetchall()
            }
        configured_owner = self._legacy_tenant or "*"
        terminal_owner = markers.get("default_owner_v1")
        if self._legacy_tenant is not None and terminal_owner not in {None, self._legacy_tenant}:
            self._reject_owner_preflight(
                reason_code="pinned_owner_mismatch",
                recorded_owner=terminal_owner,
            )
        progress_owner = markers.get("default_owner_in_progress_v1")
        if progress_owner not in {None, configured_owner}:
            self._reject_owner_preflight(
                reason_code="migration_owner_mismatch",
                recorded_owner=progress_owner,
            )
        schema_owner = markers.get("legacy_schema_owner_v1")
        expected_schema_owner = self._legacy_tenant or "default"
        if schema_owner not in {None, expected_schema_owner}:
            self._reject_owner_preflight(
                reason_code="migration_owner_mismatch",
                recorded_owner=schema_owner,
            )
        try:
            require_confirmed_default_tenant_owner(conn, legacy_tenant=self._legacy_tenant)
        except RuntimeError as exc:
            self._reject_owner_preflight(reason_code="unconfirmed_default_owner")
            raise AssertionError("unreachable") from exc
        try:
            require_legacy_tenant_owner(
                conn,
                legacy_tenant=self._legacy_tenant,
                bootstrap_signal_definitions=self._bootstrap_signal_definitions,
            )
        except RuntimeError as exc:
            self._reject_owner_preflight(reason_code="ownerless_wildcard")
            raise AssertionError("unreachable") from exc

    def _reconcile_legacy_signal_schema_batched(self) -> None:
        copied_rows = 0
        batches = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._require_owner_on_connection(conn)
                complete, operation, row_count = reconcile_legacy_signal_schema_batch(
                    conn,
                    legacy_tenant=self._legacy_tenant or "default",
                    batch_size=_LEGACY_SCHEMA_MIGRATION_BATCH_SIZE,
                )
            copied_rows += row_count
            if row_count:
                batches += 1
                logger.info(
                    "signal_legacy_schema_migration_batch",
                    reason_code="signal_legacy_schema_migration_batch",
                    operation=operation,
                    rows=row_count,
                    batch=batches,
                )
            if complete:
                if copied_rows:
                    logger.warning(
                        "signal_legacy_schema_migration_complete",
                        reason_code="signal_legacy_schema_migration_complete",
                        database_fingerprint=_diagnostic_fingerprint(self._db_path),
                        rows=copied_rows,
                        batches=batches,
                    )
                return

    def _reconcile_mapping_source_ref_index_batched(self) -> None:
        source_refs = 0
        batches = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._require_owner_on_connection(conn)
                complete, row_count = reconcile_mapping_source_ref_index_batch(
                    conn,
                    batch_size=_MAPPING_SOURCE_REF_MIGRATION_BATCH_SIZE,
                )
            source_refs += row_count
            if row_count:
                batches += 1
                logger.info(
                    "signal_mapping_source_ref_migration_batch",
                    reason_code="signal_mapping_source_ref_migration_batch",
                    source_ref_count=row_count,
                    batch=batches,
                )
            if complete:
                if source_refs:
                    logger.warning(
                        "signal_mapping_source_ref_migration_complete",
                        reason_code="signal_mapping_source_ref_migration_complete",
                        database_fingerprint=_diagnostic_fingerprint(self._db_path),
                        source_ref_count=source_refs,
                        batches=batches,
                        marker=MAPPING_SOURCE_REF_INDEX_MARKER,
                    )
                return

    def _reconcile_default_tenant_owner_batched(self) -> None:
        migrated_rows = 0
        batches = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._require_owner_on_connection(conn)
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
                    reason_code="signal_tenant_owner_migration_batch",
                    operation=operation,
                    rows=row_count,
                    batch=batches,
                )
            if complete:
                if migrated_rows:
                    logger.warning(
                        "signal_tenant_owner_migration_complete",
                        reason_code="signal_tenant_owner_migration_complete",
                        rows=migrated_rows,
                        batches=batches,
                        owner_fingerprint=_diagnostic_fingerprint(self._legacy_tenant or "*"),
                    )
                return

    @staticmethod
    def _projection_audit_marker_value(connection: sqlite3.Connection) -> str:
        row = connection.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (GOVERNED_PROJECTION_AUDIT_MARKER,),
        ).fetchone()
        return str(row["value"]) if row is not None else ""

    def _prepare_projection_authority(
        self,
        connection: sqlite3.Connection,
        *,
        tenant_id: str,
        knowledge_id: str,
        revision_number: int,
        expected_content_bytes: int | None = None,
    ) -> _PreparedProjectionAuthority:
        """Load and validate one bounded current authority without writing projections."""
        mapping_limit = int(self._settings.signal_resolution_mapping_limit)
        payload_limit = _projection_authority_payload_byte_limit(mapping_limit)
        authority_ref = (tenant_id, knowledge_id, revision_number)
        owns_snapshot = not connection.in_transaction
        if owns_snapshot:
            connection.execute("BEGIN")
        try:
            preflight = connection.execute(
                """SELECT revision.review_state, revision.lifecycle_status,
                          revision.eligibility,
                          length(CAST(revision.content_json AS BLOB)) AS content_bytes
                   FROM operational_knowledge current
                   JOIN operational_knowledge_revisions revision
                     ON revision.tenant_id=current.tenant_id
                    AND revision.knowledge_id=current.knowledge_id
                    AND revision.revision=current.current_revision
                   WHERE current.tenant_id=? AND current.knowledge_id=?
                     AND current.current_revision=? AND current.kind='signal_mapping'
                     AND current.status='active' AND revision.lifecycle_status='active'
                     AND revision.eligibility!='ineligible'""",
                authority_ref,
            ).fetchone()
            if preflight is None:
                raise _ProjectionAuditChanged
            content_bytes = int(preflight["content_bytes"] or 0)
            if expected_content_bytes is not None and content_bytes != expected_content_bytes:
                raise _ProjectionAuditChanged
            if content_bytes > payload_limit:
                raise _projection_validation_error(
                    "governed_signal_projection_payload_limit_exceeded",
                    "active governed signal authority exceeds the payload byte limit",
                    authority_ref=authority_ref,
                    expected_count=payload_limit,
                    projected_count=content_bytes,
                    validation_reason="resolver_payload_byte_limit_exceeded",
                )

            row = connection.execute(
                """SELECT revision.content_json, revision.review_state,
                          revision.lifecycle_status, revision.eligibility,
                          length(CAST(revision.content_json AS BLOB)) AS content_bytes
                   FROM operational_knowledge current
                   JOIN operational_knowledge_revisions revision
                     ON revision.tenant_id=current.tenant_id
                    AND revision.knowledge_id=current.knowledge_id
                    AND revision.revision=current.current_revision
                   WHERE current.tenant_id=? AND current.knowledge_id=?
                     AND current.current_revision=? AND current.kind='signal_mapping'
                     AND current.status='active' AND revision.lifecycle_status='active'
                     AND revision.eligibility!='ineligible'""",
                authority_ref,
            ).fetchone()
            if row is None or any(
                row[field] != preflight[field]
                for field in ("review_state", "lifecycle_status", "eligibility", "content_bytes")
            ):
                raise _ProjectionAuditChanged
            content_json = str(row["content_json"])
            try:
                content = json.loads(content_json)
            except (TypeError, json.JSONDecodeError) as exc:
                raise _projection_validation_error(
                    "governed_signal_projection_payload_invalid",
                    "active governed signal authority has invalid resolver payload",
                    authority_ref=authority_ref,
                    exception=exc,
                ) from exc
            if not isinstance(content, dict):
                raise _projection_validation_error(
                    "governed_signal_projection_payload_invalid",
                    "active governed signal authority has invalid resolver payload",
                    authority_ref=authority_ref,
                    validation_reason="resolver_payload_not_object",
                )
            resolver_payload = content.get("resolver_payload")
            resolver_mappings = resolver_payload.get("mappings", []) if isinstance(resolver_payload, dict) else None
            if isinstance(resolver_mappings, list) and len(resolver_mappings) > mapping_limit:
                raise _projection_validation_error(
                    "governed_signal_projection_mapping_limit_exceeded",
                    "active governed signal authority exceeds the resolver mapping limit",
                    authority_ref=authority_ref,
                    expected_count=mapping_limit,
                    projected_count=len(resolver_mappings),
                    validation_reason="resolver_mapping_limit_exceeded",
                )

            from tacit.knowledge.models import KnowledgeRevision

            revision = KnowledgeRevision.model_validate_json(content_json)
            validated_projection = _validate_projection_revision(
                revision,
                mapping_limit=mapping_limit,
                serialized_payload_bytes=content_bytes,
            )
            if (
                revision.tenant_id != tenant_id
                or revision.knowledge_id != knowledge_id
                or revision.revision != revision_number
            ):
                raise _projection_validation_error(
                    "governed_signal_projection_identity_mismatch",
                    "active governed signal authority identity does not match its persisted key",
                    authority_ref=authority_ref,
                )
            return _PreparedProjectionAuthority(
                tenant_id=tenant_id,
                knowledge_id=knowledge_id,
                revision_number=revision_number,
                content_json=content_json,
                content=content,
                revision=revision,
                expected_variants=validated_projection.expected_variants,
                review_state=str(row["review_state"]),
                lifecycle_status=str(row["lifecycle_status"]),
                eligibility=str(row["eligibility"]),
            )
        finally:
            if owns_snapshot:
                connection.rollback()

    @staticmethod
    def _projection_authority_tables_available(connection: sqlite3.Connection) -> bool:
        tables = {str(row[0]) for row in connection.execute("""SELECT name FROM sqlite_master
                   WHERE type='table'
                     AND name IN ('operational_knowledge', 'operational_knowledge_revisions')""")}
        return tables == {"operational_knowledge", "operational_knowledge_revisions"}

    @staticmethod
    def _active_projection_authority_page(
        connection: sqlite3.Connection,
        *,
        after: tuple[str, str] | None,
        limit: int,
    ) -> list[sqlite3.Row]:
        sql, params = _active_projection_authority_page_statement(after=after, limit=limit)
        return connection.execute(sql, params).fetchall()

    @staticmethod
    def _projection_authority_is_active(
        connection: sqlite3.Connection,
        key: tuple[str, str, int],
    ) -> bool:
        return (
            connection.execute(
                """SELECT 1
                   FROM operational_knowledge current
                   JOIN operational_knowledge_revisions revision
                     ON revision.tenant_id=current.tenant_id
                    AND revision.knowledge_id=current.knowledge_id
                    AND revision.revision=current.current_revision
                   WHERE current.tenant_id=? AND current.knowledge_id=?
                     AND current.current_revision=? AND current.kind='signal_mapping'
                     AND current.status='active' AND revision.lifecycle_status='active'
                     AND revision.eligibility!='ineligible'""",
                key,
            ).fetchone()
            is not None
        )

    def _validate_prepared_projection(
        self,
        connection: sqlite3.Connection,
        prepared: _PreparedProjectionAuthority,
    ) -> None:
        observed: set[tuple[str, str, tuple[str, ...]]] = set()
        projected_count = 0
        after_mapping_id: int | None = None
        scan_limit = len(prepared.expected_variants) + 1
        while projected_count < scan_limit:
            sql, params = _projection_mapping_page_statement(
                tenant_id=prepared.tenant_id,
                governance_ref=prepared.knowledge_id,
                governance_revision=prepared.revision_number,
                after_id=after_mapping_id,
                limit=min(_PROJECTION_AUDIT_BATCH_SIZE, scan_limit - projected_count),
            )
            projection_keys = connection.execute(sql, params).fetchall()
            if not projection_keys:
                break
            mapping_ids = [int(row["mapping_id"]) for row in projection_keys]
            hydration_sql, hydration_params = _projection_audit_mapping_rows_statement(mapping_ids)
            mappings = connection.execute(hydration_sql, hydration_params).fetchall()
            if len(mappings) != len(mapping_ids):
                raise _ProjectionAuditChanged
            for mapping in mappings:
                try:
                    raw_datasource_types = json.loads(str(mapping["context_datasource_types"]))
                except (TypeError, json.JSONDecodeError) as exc:
                    raise _projection_validation_error(
                        "governed_signal_projection_row_invalid",
                        "active governed signal authority has an invalid resolver projection",
                        authority_ref=prepared.key,
                        mapping_ref=(prepared.tenant_id, mapping["id"]),
                        exception=exc,
                    ) from exc
                if not isinstance(raw_datasource_types, list) or any(
                    not isinstance(value, str) or not value.strip() for value in raw_datasource_types
                ):
                    raise _projection_validation_error(
                        "governed_signal_projection_row_invalid",
                        "active governed signal authority has an invalid resolver projection",
                        authority_ref=prepared.key,
                        mapping_ref=(prepared.tenant_id, mapping["id"]),
                        validation_reason="projection_datasource_scope_invalid",
                    )
                valid, reason = projection_matches_authority(mapping, prepared.validation_record())
                if not valid:
                    raise _projection_validation_error(
                        "governed_signal_projection_mapping_invalid",
                        "governed signal projection remains invalid after repair",
                        authority_ref=prepared.key,
                        mapping_ref=(prepared.tenant_id, mapping["id"]),
                        validation_reason=reason,
                    )
                observed.add(
                    (
                        str(mapping["signal_type"]),
                        str(mapping["metric_pattern"]),
                        normalize_datasource_types(raw_datasource_types),
                    )
                )
            projected_count += len(mappings)
            after_mapping_id = mapping_ids[-1]
        if observed != set(prepared.expected_variants) or projected_count != len(prepared.expected_variants):
            raise _projection_validation_error(
                "governed_signal_projection_incomplete",
                "active governed signal authority has an incomplete resolver projection",
                authority_ref=prepared.key,
                expected_count=len(prepared.expected_variants),
                projected_count=projected_count,
            )

    def _repair_projection_authority_batches(self) -> int:
        repaired = 0
        batch = 0
        after_authority: tuple[str, str] | None = None
        with self.transaction() as conn:
            ensure_projection_authority_page_index(conn)
        while True:
            with self.read_transaction() as conn:
                if not self._projection_authority_tables_available(conn):
                    return repaired
                rows = self._active_projection_authority_page(
                    conn,
                    after=after_authority,
                    limit=_PROJECTION_AUDIT_BATCH_SIZE,
                )
            if not rows:
                return repaired
            batch += 1
            logger.info(
                "governed_signal_projection_repair_batch",
                reason_code="governed_signal_projection_repair_batch",
                batch_size=len(rows),
                batch=batch,
                cursor_fingerprint=_projection_cursor_fingerprint(after_authority),
            )
            for row in rows:
                key = (
                    str(row["tenant_id"]),
                    str(row["knowledge_id"]),
                    int(row["current_revision"]),
                )
                try:
                    with self.read_transaction() as conn:
                        prepared = self._prepare_projection_authority(
                            conn,
                            tenant_id=key[0],
                            knowledge_id=key[1],
                            revision_number=key[2],
                            expected_content_bytes=int(row["content_bytes"] or 0),
                        )
                    with self.transaction() as conn:
                        persisted = conn.execute(
                            """SELECT revision.content_json
                               FROM operational_knowledge current
                               JOIN operational_knowledge_revisions revision
                                 ON revision.tenant_id=current.tenant_id
                                AND revision.knowledge_id=current.knowledge_id
                                AND revision.revision=current.current_revision
                               WHERE current.tenant_id=? AND current.knowledge_id=?
                                 AND current.current_revision=? AND current.kind='signal_mapping'
                                 AND current.status='active' AND revision.lifecycle_status='active'
                                 AND revision.eligibility!='ineligible'""",
                            key,
                        ).fetchone()
                        if persisted is None or str(persisted["content_json"]) != prepared.content_json:
                            raise _ProjectionAuditChanged
                        result = self.sync_governed_revision(
                            prepared.revision,
                            connection=conn,
                            allow_dirty=True,
                        )
                        repaired += int(result["projected"])
                except _ProjectionAuditChanged:
                    raise
                except (RuntimeError, ValueError) as exc:
                    logger.error(
                        "governed_signal_projection_authority_unrepairable",
                        reason_code="governed_signal_projection_validation_failed",
                        exception_class=type(exc).__name__[:64],
                        error_fingerprint=_diagnostic_fingerprint(exc),
                        authority_ref_fingerprint=_authority_ref_fingerprint(*key),
                    )
                    detail = f": {exc}" if isinstance(exc, _ProjectionValidationError) else ""
                    raise RuntimeError("active governed signal authority cannot be projected exactly" + detail) from exc
            after_authority = (str(rows[-1]["tenant_id"]), str(rows[-1]["knowledge_id"]))

    def _quarantine_projection_batches(self) -> tuple[int, int]:
        quarantined = 0
        legacy_quarantined = 0
        quarantine_reasons: dict[str, int] = {}
        quarantine_pattern_fingerprints: set[str] = set()
        after_governed: tuple[str, str, int, int] | None = None
        while True:
            with self.transaction() as conn:
                authorities_available = self._projection_authority_tables_available(conn)
                if authorities_available:
                    sql, params = _projection_authority_audit_page_statement(
                        after=after_governed,
                        limit=_PROJECTION_AUDIT_BATCH_SIZE,
                    )
                else:
                    sql, params = _projection_audit_key_page_statement(
                        "governed",
                        after=after_governed,
                        limit=_PROJECTION_AUDIT_BATCH_SIZE,
                    )
                rows = conn.execute(sql, params).fetchall()
                if not rows:
                    break
                invalid_ids = [
                    int(row["mapping_id"])
                    for row in rows
                    if not authorities_available or not bool(row["authority_active"])
                ]
                if invalid_ids:
                    hydration_sql, hydration_params = _projection_audit_mapping_rows_statement(invalid_ids)
                    invalid_rows = conn.execute(hydration_sql, hydration_params).fetchall()
                    quarantine_pattern_fingerprints.update(
                        _diagnostic_fingerprint(row["metric_pattern"]) for row in invalid_rows
                    )
                    conn.executemany(
                        "UPDATE signal_metric_mappings SET review_state='candidate' WHERE id=?",
                        [(mapping_id,) for mapping_id in invalid_ids],
                    )
                    quarantined += len(invalid_ids)
                    reason = "authority_revision_missing_or_inactive"
                    quarantine_reasons[reason] = quarantine_reasons.get(reason, 0) + len(invalid_ids)
                last = rows[-1]
                after_governed = (
                    str(last["tenant_id"]),
                    str(last["governance_ref"]),
                    int(last["governance_revision"]),
                    int(last["mapping_id"]),
                )

        after_ungoverned: tuple[str, int] | None = None
        while True:
            with self.transaction() as conn:
                sql, params = _projection_audit_key_page_statement(
                    "ungoverned",
                    after=after_ungoverned,
                    limit=_PROJECTION_AUDIT_BATCH_SIZE,
                )
                rows = conn.execute(sql, params).fetchall()
                if not rows:
                    break
                mapping_ids = [int(row["mapping_id"]) for row in rows]
                conn.executemany(
                    "UPDATE signal_metric_mappings SET review_state='candidate' WHERE id=?",
                    [(mapping_id,) for mapping_id in mapping_ids],
                )
                legacy_quarantined += len(mapping_ids)
                last = rows[-1]
                after_ungoverned = (str(last["tenant_id"]), int(last["mapping_id"]))
        if quarantined:
            logger.warning(
                "governed_signal_mapping_revision_unknown",
                reason_code="governed_signal_mapping_revision_unknown",
                mappings=quarantined,
                quarantined=quarantined,
                reasons=quarantine_reasons,
                sample_pattern_fingerprints=sorted(quarantine_pattern_fingerprints)[:5],
            )
        return quarantined, legacy_quarantined

    def _validated_projection_audit_token(self) -> str | None:
        with self.transaction() as conn:
            ensure_projection_authority_page_index(conn)
        with self.read_transaction() as conn:
            token = self._projection_audit_marker_value(conn)
            if token == "clean":
                return None
            if not token:
                raise RuntimeError("governed signal projection audit was not dirty during repair")
            authority_tables_available = self._projection_authority_tables_available(conn)

        def require_same_token(connection: sqlite3.Connection) -> bool:
            current = self._projection_audit_marker_value(connection)
            if current == "clean":
                return False
            if not current:
                raise RuntimeError("governed signal projection audit marker disappeared during repair")
            if current != token:
                raise _ProjectionAuditChanged
            return True

        if authority_tables_available:
            after_authority: tuple[str, str] | None = None
            while True:
                with self.read_transaction() as conn:
                    if not require_same_token(conn):
                        return None
                    authorities = self._active_projection_authority_page(
                        conn,
                        after=after_authority,
                        limit=_PROJECTION_AUTHORITY_VALIDATION_BATCH_SIZE,
                    )
                    if not authorities:
                        break
                    for authority in authorities:
                        prepared = self._prepare_projection_authority(
                            conn,
                            tenant_id=str(authority["tenant_id"]),
                            knowledge_id=str(authority["knowledge_id"]),
                            revision_number=int(authority["current_revision"]),
                            expected_content_bytes=int(authority["content_bytes"] or 0),
                        )
                        self._validate_prepared_projection(conn, prepared)
                    after_authority = (
                        str(authorities[-1]["tenant_id"]),
                        str(authorities[-1]["knowledge_id"]),
                    )

        after_governed: tuple[str, str, int, int] | None = None
        while True:
            with self.read_transaction() as conn:
                if not require_same_token(conn):
                    return None
                if authority_tables_available:
                    sql, params = _projection_authority_audit_page_statement(
                        after=after_governed,
                        limit=_PROJECTION_AUDIT_BATCH_SIZE,
                    )
                else:
                    sql, params = _projection_audit_key_page_statement(
                        "governed",
                        after=after_governed,
                        limit=_PROJECTION_AUDIT_BATCH_SIZE,
                    )
                rows = conn.execute(sql, params).fetchall()
                if not rows:
                    break
                for row in rows:
                    key = (
                        str(row["tenant_id"]),
                        str(row["governance_ref"]),
                        int(row["governance_revision"]),
                    )
                    if not authority_tables_available or not bool(row["authority_active"]):
                        raise _projection_validation_error(
                            "governed_signal_projection_authority_missing",
                            "governed signal projection remains invalid after repair",
                            authority_ref=key,
                            mapping_ref=(row["tenant_id"], row["mapping_id"]),
                            validation_reason="authority_revision_missing_or_inactive",
                        )
                last = rows[-1]
                after_governed = (
                    str(last["tenant_id"]),
                    str(last["governance_ref"]),
                    int(last["governance_revision"]),
                    int(last["mapping_id"]),
                )

        with self.read_transaction() as conn:
            if not require_same_token(conn):
                return None
            sql, params = _projection_audit_key_page_statement(
                "ungoverned",
                after=None,
                limit=1,
            )
            row = conn.execute(sql, params).fetchone()
            if row is not None:
                raise _projection_validation_error(
                    "ungoverned_signal_projection_active",
                    "active ungoverned signal mapping remains after projection repair",
                    mapping_ref=(row["tenant_id"], row["mapping_id"]),
                )
        return token

    def _reconcile_governed_projection_audit_batched(self) -> None:
        """Repair a dirty projection audit without one database-wide write lock."""
        with self.transaction() as conn:
            ensure_governed_projection_audit_triggers(conn)
            ensure_projection_authority_page_index(conn)
        for attempt in range(1, _PROJECTION_AUDIT_MAX_RETRIES + 1):
            try:
                repaired = self._repair_projection_authority_batches()
                quarantined, legacy_quarantined = self._quarantine_projection_batches()
                token = self._validated_projection_audit_token()
            except _ProjectionAuditChanged:
                logger.warning(
                    "governed_signal_projection_audit_raced",
                    reason_code="governed_signal_projection_audit_raced",
                    attempt=attempt,
                )
                continue
            if token is None:
                return
            with self.transaction() as conn:
                if self._projection_audit_marker_value(conn) != token:
                    logger.warning(
                        "governed_signal_projection_audit_raced",
                        reason_code="governed_signal_projection_audit_raced",
                        attempt=attempt,
                    )
                    continue
                mark_governed_projection_audit_current(conn)
            if repaired or quarantined or legacy_quarantined:
                logger.warning(
                    "governed_signal_projection_authority_rebuilt",
                    reason_code="governed_signal_projection_authority_rebuilt",
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

    def mark_governed_projection_audit_current(self, connection: sqlite3.Connection) -> None:
        """Mark an authority/projection transaction internally consistent."""
        with self._write_connection(connection) as bound:
            mark_governed_projection_audit_current(bound)

    def governed_projection_audit_is_current(self, connection: sqlite3.Connection) -> bool:
        """Return whether no unresolved projection mutation predates this transaction."""
        bound = self._bind_external_connection(connection, require_transaction=False)
        return governed_projection_audit_is_current(bound)

    def reconcile_governed_projection_audit(self, _connection: sqlite3.Connection) -> None:
        """Reject the removed single-transaction projection repair path."""
        raise RuntimeError(
            "single-transaction projection repair is unsupported; call ensure_governed_projection_audit_current()"
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
        with self._write_connection(connection) as conn:
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

    def list_signal_types_page(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> KeysetPage[dict[str, Any]]:
        """Return one stable page of effective tenant signal definitions."""
        if not 1 <= limit <= _SIGNAL_TAXONOMY_COMPATIBILITY_LIMIT:
            raise ValueError(f"signal type page limit must be between 1 and {_SIGNAL_TAXONOMY_COMPATIBILITY_LIMIT}")
        tenant_id = self._resolve_tenant(tenant_id)
        tenant_cursor_clause = ""
        global_cursor_clause = ""
        uncategorized_cursor_clause = ""
        cursor_params: tuple[Any, ...] = ()
        if cursor:
            category, signal_type = _decode_signal_type_cursor(cursor)
            tenant_cursor_clause = "AND (t.category > ? OR (t.category = ? AND t.signal_type > ?))"
            global_cursor_clause = "AND (g.category > ? OR (g.category = ? AND g.signal_type > ?))"
            uncategorized_cursor_clause = "AND ('' > ? OR ('' = ? AND t.signal_type > ?))"
            cursor_params = (category, category, signal_type)

        with self.read_transaction() as conn:
            categorized_tenant_rows = conn.execute(
                f"""SELECT t.signal_type,
                           CASE WHEN t.description != '' THEN t.description
                                ELSE COALESCE(g.description, '') END AS description,
                           t.category AS category,
                           CASE WHEN t.unit != '' THEN t.unit ELSE COALESCE(g.unit, '') END AS unit,
                           COALESCE(g.created_at, t.created_at) AS created_at,
                           t.updated_at AS updated_at
                    FROM tenant_signal_types t INDEXED BY idx_tenant_signal_types_page
                    LEFT JOIN signal_types g ON g.signal_type=t.signal_type
                    WHERE t.tenant_id=? AND t.category!=''
                    {tenant_cursor_clause}
                    ORDER BY t.category ASC, t.signal_type ASC
                    LIMIT ?""",
                (tenant_id, *cursor_params, limit + 1),
            ).fetchall()
            uncategorized_tenant_rows = conn.execute(
                f"""SELECT t.signal_type, t.description, '' AS category, t.unit,
                           t.created_at, t.updated_at
                    FROM tenant_signal_types t INDEXED BY idx_tenant_signal_types_page
                    WHERE t.tenant_id=? AND t.category=''
                      AND NOT EXISTS (
                          SELECT 1 FROM signal_types g WHERE g.signal_type=t.signal_type
                      )
                    {uncategorized_cursor_clause}
                    ORDER BY t.signal_type ASC
                    LIMIT ?""",
                (tenant_id, *cursor_params, limit + 1),
            ).fetchall()
            inherited_category_rows = conn.execute(
                f"""SELECT t.signal_type,
                           CASE WHEN t.description != '' THEN t.description ELSE g.description END AS description,
                           g.category AS category,
                           CASE WHEN t.unit != '' THEN t.unit ELSE g.unit END AS unit,
                           g.created_at AS created_at,
                           t.updated_at AS updated_at
                    FROM signal_types g INDEXED BY idx_signal_types_page
                    CROSS JOIN tenant_signal_types t
                    WHERE t.tenant_id=? AND t.signal_type=g.signal_type AND t.category=''
                    {global_cursor_clause}
                    ORDER BY g.category ASC, g.signal_type ASC
                    LIMIT ?""",
                (tenant_id, *cursor_params, limit + 1),
            ).fetchall()
            global_rows = conn.execute(
                f"""SELECT g.signal_type, g.description, g.category, g.unit,
                           g.created_at, g.updated_at
                    FROM signal_types g INDEXED BY idx_signal_types_page
                    WHERE NOT EXISTS (
                        SELECT 1 FROM tenant_signal_types t
                        WHERE t.tenant_id=? AND t.signal_type=g.signal_type
                    )
                    {global_cursor_clause}
                    ORDER BY g.category ASC, g.signal_type ASC
                    LIMIT ?""",
                (tenant_id, *cursor_params, limit + 1),
            ).fetchall()
            rows = sorted(
                [
                    *categorized_tenant_rows,
                    *uncategorized_tenant_rows,
                    *inherited_category_rows,
                    *global_rows,
                ],
                key=lambda row: (str(row["category"]), str(row["signal_type"])),
            )[: limit + 1]
            has_more = len(rows) > limit
            visible = rows[:limit]
            counts: dict[str, int] = {}
            if visible:
                signal_types = [str(row["signal_type"]) for row in visible]
                placeholders = ",".join("?" for _ in signal_types)
                mapping_rows = conn.execute(
                    f"""SELECT signal_type, COUNT(*) AS mapping_count
                        FROM signal_metric_mappings
                        WHERE tenant_id IN (?, ?) AND signal_type IN ({placeholders})
                        GROUP BY signal_type""",
                    (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID, *signal_types),
                ).fetchall()
                counts = {str(row["signal_type"]): int(row["mapping_count"]) for row in mapping_rows}

        items: list[dict[str, Any]] = []
        for row in visible:
            item = dict(row)
            item["mapping_count"] = counts.get(str(row["signal_type"]), 0)
            items.append(item)
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = encode_cursor(str(last["category"]), str(last["signal_type"]))
        return KeysetPage(items=items, has_more=has_more, next_cursor=next_cursor)

    def list_signal_types(
        self,
        *,
        tenant_id: str | None = None,
        limit: int = _SIGNAL_TAXONOMY_COMPATIBILITY_LIMIT,
    ) -> list[dict[str, Any]]:
        """Compatibility list bounded to one signal-taxonomy page."""
        return self.list_signal_types_page(tenant_id=tenant_id, limit=limit).items

    def _signal_definitions_for_types(
        self,
        conn: sqlite3.Connection,
        signal_types: set[str],
        *,
        tenant_id: str,
    ) -> dict[str, dict[str, Any]]:
        definitions: dict[str, dict[str, Any]] = {}
        names = sorted(signal_types)
        for start in range(0, len(names), _SIGNAL_RESOLUTION_PAGE_SIZE):
            chunk = names[start : start + _SIGNAL_RESOLUTION_PAGE_SIZE]
            placeholders = ",".join("?" for _ in chunk)
            global_rows = conn.execute(
                f"SELECT * FROM signal_types WHERE signal_type IN ({placeholders})",
                chunk,
            ).fetchall()
            tenant_rows = conn.execute(
                f"""SELECT * FROM tenant_signal_types
                    WHERE tenant_id=? AND signal_type IN ({placeholders})""",
                (tenant_id, *chunk),
            ).fetchall()
            global_by_name = {str(row["signal_type"]): dict(row) for row in global_rows}
            tenant_by_name = {str(row["signal_type"]): dict(row) for row in tenant_rows}
            for signal_type in chunk:
                definition = _merge_signal_definition(
                    global_by_name.get(signal_type),
                    tenant_by_name.get(signal_type),
                )
                if definition is not None:
                    definitions[signal_type] = definition
        return definitions

    def get_signal_definition(
        self,
        signal_type: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Return effective signal metadata without enumerating its mappings."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self.read_transaction() as conn:
            return self._signal_definitions_for_types(
                conn,
                {signal_type},
                tenant_id=tenant_id,
            ).get(signal_type)

    def get_signal_type(self, signal_type: str, *, tenant_id: str | None = None) -> dict[str, Any] | None:
        """Compatibility detail bounded to one metric-mapping page."""
        return self.get_signal_type_page(
            signal_type,
            tenant_id=tenant_id,
            limit=_SIGNAL_TAXONOMY_COMPATIBILITY_LIMIT,
        )

    def get_signal_type_page(
        self,
        signal_type: str,
        *,
        tenant_id: str | None = None,
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict[str, Any] | None:
        """Return signal metadata plus one tenant-prioritized mapping page."""
        if not 1 <= limit <= 500:
            raise ValueError("signal mapping page limit must be between 1 and 500")
        tenant_id = self._resolve_tenant(tenant_id)
        cursor_priority = 0
        cursor_confidence: float | None = None
        cursor_mapping_id: int | None = None
        if cursor:
            cursor_priority, cursor_confidence, cursor_mapping_id = _decode_signal_mapping_cursor(cursor)
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
            rows: list[dict[str, Any]] = []
            for priority, storage_tenant in (
                (0, tenant_id),
                (1, GLOBAL_BOOTSTRAP_TENANT_ID),
            ):
                if priority < cursor_priority or len(rows) >= limit + 1:
                    continue
                branch_cursor = (
                    (cursor_confidence, cursor_mapping_id) if cursor and priority == cursor_priority else None
                )
                branch_clause = ""
                branch_params: list[Any] = [storage_tenant, signal_type]
                if branch_cursor is not None:
                    branch_clause = "AND (confidence < ? OR (confidence = ? AND id < ?))"
                    branch_params.extend([branch_cursor[0], branch_cursor[0], branch_cursor[1]])
                branch_params.append(limit + 1 - len(rows))
                branch_rows = conn.execute(
                    f"""SELECT * FROM signal_metric_mappings INDEXED BY idx_smm_tenant_signal_page
                        WHERE tenant_id=? AND signal_type=? {branch_clause}
                        ORDER BY confidence DESC, id DESC LIMIT ?""",
                    branch_params,
                ).fetchall()
                rows.extend({**dict(row), "_tenant_priority": priority} for row in branch_rows)
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

    def _matching_signal_mappings_for_metrics(
        self,
        conn: sqlite3.Connection,
        metric_names: list[str],
        *,
        context_service: str,
        context_datasource_type: str,
        context_archetype: str,
        context_environment: str,
        tenant_id: str,
        knowledge_scope: Any | None,
        excluded_knowledge_refs: set[KnowledgeRevisionRef] | None,
        resolution_limit: int,
        pattern_check_limit: int,
        work_budget: SignalResolutionWorkBudget,
    ) -> list[_ReverseMappingMatch]:
        """Bulk-match active mappings through a bounded indexed scan."""
        pinned = self._pinned_governed_mappings.get()
        if pinned is not None:
            _require_pinned_governed_mapping_tenant(
                pinned_tenant=pinned.tenant_id,
                requested_tenant=tenant_id,
            )
        unique_metrics = list(dict.fromkeys(metric_names))
        trigram_index: dict[str, set[int]] = {}
        for metric_index, metric_name in enumerate(unique_metrics):
            for offset in range(max(0, len(metric_name) - 2)):
                trigram_index.setdefault(metric_name[offset : offset + 3], set()).add(metric_index)
        now = time.time()
        applicable: dict[tuple[str, str, tuple[str, ...]], dict[str, Any]] = {}

        def consider(mapping: dict[str, Any], *, priority: int) -> None:
            governance_ref = str(mapping.get("governance_ref") or "")
            governance_revision = int(mapping.get("governance_revision") or 0)
            if (
                excluded_knowledge_refs
                and governance_ref
                and governance_revision > 0
                and KnowledgeRevisionRef(governance_ref, governance_revision) in excluded_knowledge_refs
            ):
                return
            resolution_datasource_type = context_datasource_type
            mapping_datasource_types = mapping.get("context_datasource_types", [])
            if not resolution_datasource_type and mapping_datasource_types:
                # Datasource scope is resolved against each MetricEntry after
                # name matching. Use one allowed value here only to defer that
                # dimension through the coarse mapping scan.
                resolution_datasource_type = str(mapping_datasource_types[0])
            if not _context_matches(
                mapping,
                context_service,
                resolution_datasource_type,
                context_archetype,
                context_environment,
            ):
                return
            if governance_ref and not _governed_scope_matches(
                mapping,
                knowledge_scope=knowledge_scope,
                context_service=context_service,
                context_datasource_type=resolution_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                now=now,
            ):
                return
            trust_effective = _effective_confidence(
                mapping,
                now,
                context_service=context_service,
                context_datasource_type=resolution_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                apply_context_penalty=False,
            )
            if trust_effective < TRUST_THRESHOLD:
                return
            metric_pattern = str(mapping.get("metric_pattern") or "")
            signal_type = str(mapping.get("signal_type") or "")
            if not metric_pattern:
                return
            effective = _effective_confidence(
                mapping,
                now,
                context_service=context_service,
                context_datasource_type=resolution_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
            )
            candidate = dict(mapping)
            candidate["effective_confidence"] = round(effective, 4)
            candidate["_resolution_priority"] = priority
            datasource_scope = tuple(
                sorted({str(value).strip().casefold() for value in mapping_datasource_types if str(value).strip()})
            )
            identity = (signal_type, metric_pattern, datasource_scope)
            current = applicable.get(identity)
            sort_key = (
                priority,
                -float(candidate["effective_confidence"]),
                int(candidate.get("id") or 0),
            )
            if current is None:
                applicable[identity] = candidate
                return
            current_key = (
                int(current["_resolution_priority"]),
                -float(current["effective_confidence"]),
                int(current.get("id") or 0),
            )
            if sort_key < current_key:
                applicable[identity] = candidate

        if pinned is not None:
            for mapping in pinned.mappings:
                consider(dict(mapping), priority=0)

        scan_limit = min(
            _SIGNAL_RESOLUTION_MAX_SCAN_LIMIT,
            max(_SIGNAL_RESOLUTION_MIN_SCAN_LIMIT, resolution_limit * _SIGNAL_RESOLUTION_SCAN_MULTIPLIER),
        )
        scanned = len(pinned.mappings) if pinned is not None else 0
        storage_tenants = (
            [GLOBAL_BOOTSTRAP_TENANT_ID] if pinned is not None else [tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID]
        )
        for storage_tenant in storage_tenants:
            last_id: int | None = None
            exhausted = False
            while scanned < scan_limit:
                page_limit = min(_SIGNAL_RESOLUTION_PAGE_SIZE, scan_limit - scanned)
                id_clause, id_params = _ascending_integer_keyset("id", last_id)
                rows = conn.execute(
                    f"""SELECT * FROM signal_metric_mappings INDEXED BY idx_smm_active_reverse
                       WHERE tenant_id=? AND review_state IN ('approved', 'trusted') {id_clause}
                       ORDER BY id LIMIT ?""",
                    (storage_tenant, *id_params, page_limit),
                ).fetchall()
                if not rows:
                    exhausted = True
                    break
                scanned += len(rows)
                last_id = int(rows[-1]["id"])
                for row in rows:
                    mapping = _deserialize_mapping(row)
                    consider(mapping, priority=0 if mapping["tenant_id"] == tenant_id else 1)
                if len(rows) < page_limit:
                    exhausted = True
                    break
            if not exhausted:
                id_clause, id_params = _ascending_integer_keyset("id", last_id)
                more = conn.execute(
                    f"""SELECT 1 FROM signal_metric_mappings INDEXED BY idx_smm_active_reverse
                       WHERE tenant_id=? AND review_state IN ('approved', 'trusted') {id_clause}
                       LIMIT 1""",
                    (storage_tenant, *id_params),
                ).fetchone()
                if more is not None:
                    logger.error(
                        "signal_reverse_resolution_scan_limit_exceeded",
                        reason_code="signal_reverse_resolution_scan_limit_exceeded",
                        tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                        mapping_scan_count=scanned,
                        mapping_scan_limit=scan_limit,
                    )
                    raise RuntimeError(f"Metric-to-signal resolution scan exceeded the {scan_limit}-row safety limit")

        pattern_checks = 0
        matches: list[_ReverseMappingMatch] = []
        candidate_signals: set[str] = set()
        ordered_mappings = sorted(
            applicable.values(),
            key=lambda mapping: (
                -float(mapping["effective_confidence"]),
                int(mapping["_resolution_priority"]),
                int(mapping.get("id") or 0),
            ),
        )
        self._reserve_resolution_comparisons(
            work_budget,
            mapping_count=len(ordered_mappings),
            eligible_catalog_count=len(unique_metrics),
            operation="reverse",
        )
        all_metric_indexes = set(range(len(unique_metrics)))
        for mapping in ordered_mappings:
            pattern = str(mapping["metric_pattern"])
            fragments = _pattern_literal_fragments(pattern)
            required_trigrams = {
                fragment[offset : offset + 3] for fragment in fragments for offset in range(max(0, len(fragment) - 2))
            }
            if required_trigrams:
                postings = [trigram_index.get(trigram, set()) for trigram in required_trigrams]
                candidate_metric_indexes = set.intersection(*postings) if postings else set()
            else:
                candidate_metric_indexes = all_metric_indexes
            matching_names: list[str] = []
            for metric_index in sorted(candidate_metric_indexes):
                pattern_checks += 1
                if pattern_checks > pattern_check_limit:
                    logger.error(
                        "signal_reverse_resolution_pattern_budget_exceeded",
                        reason_code="signal_reverse_resolution_pattern_budget_exceeded",
                        tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                        metric_count=len(unique_metrics),
                        mapping_scan_count=scanned,
                        applicable_mapping_count=len(applicable),
                        pattern_check_count=pattern_checks,
                        pattern_check_limit=pattern_check_limit,
                    )
                    raise RuntimeError(
                        f"Metric-to-signal resolution exceeded the {pattern_check_limit}-check pattern safety limit"
                    )
                metric_name = unique_metrics[metric_index]
                if _metric_matches_pattern(metric_name, pattern):
                    matching_names.append(metric_name)
            if not matching_names:
                continue
            self._consume_resolution_result(work_budget, operation="reverse_mapping")
            matches.append(_ReverseMappingMatch(mapping=mapping, metric_names=tuple(matching_names)))
            candidate_signals.add(str(mapping["signal_type"]))
            if len(matches) > resolution_limit or len(candidate_signals) > resolution_limit:
                logger.error(
                    "signal_reverse_resolution_limit_exceeded",
                    reason_code="signal_reverse_resolution_limit_exceeded",
                    tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                    signal_type_count=len(candidate_signals),
                    matching_mapping_count=len(matches),
                    resolution_limit=resolution_limit,
                )
                raise RuntimeError(
                    f"Metric-to-signal resolution has more than {resolution_limit} active mapping candidates"
                )

        logger.info(
            "signal_reverse_resolution_scan",
            reason_code="signal_reverse_resolution_scan",
            tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
            metric_count=len(unique_metrics),
            mapping_scan_count=scanned,
            applicable_mapping_count=len(applicable),
            pattern_check_count=pattern_checks,
            pattern_check_limit=pattern_check_limit,
            matching_mapping_count=len(matches),
            candidate_signal_count=len(candidate_signals),
        )
        return matches

    def resolve_metric_signal_details(
        self,
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
        work_budget: SignalResolutionWorkBudget | None = None,
    ) -> list[ResolvedMetricSignal]:
        """Reverse-resolve catalog metrics without enumerating the signal taxonomy."""
        tenant_id = self._resolve_tenant(tenant_id)
        work_budget = self._begin_resolution_work(work_budget, operation="reverse")
        target_lang = target_query_language.casefold()
        target_ds = context_datasource_type.casefold()
        eligible_catalog = [
            entry
            for entry in catalog
            if (not target_lang or (entry.query_language or "").casefold() == target_lang)
            and (not target_ds or _datasource_type_matches(entry.datasource_type, target_ds))
        ]
        catalog_limit = int(self._settings.signal_resolution_catalog_limit)
        if len(eligible_catalog) > catalog_limit:
            raise RuntimeError(f"Signal resolution catalog has more than {catalog_limit} eligible metrics")
        if not eligible_catalog:
            return []
        resolution_limit = int(self._settings.signal_resolution_mapping_limit)
        with self.read_transaction() as conn:
            mapping_matches = self._matching_signal_mappings_for_metrics(
                conn,
                [entry.name for entry in eligible_catalog],
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                tenant_id=tenant_id,
                knowledge_scope=knowledge_scope,
                excluded_knowledge_refs=excluded_knowledge_refs,
                resolution_limit=resolution_limit,
                pattern_check_limit=int(self._settings.signal_resolution_pattern_check_limit),
                work_budget=work_budget,
            )
            definitions = self._signal_definitions_for_types(
                conn,
                {str(match.mapping["signal_type"]) for match in mapping_matches},
                tenant_id=tenant_id,
            )

        entries_by_name: dict[str, list[MetricEntry]] = {}
        for entry in eligible_catalog:
            entries_by_name.setdefault(entry.name, []).append(entry)
        resolved: list[ResolvedMetricSignal] = []
        resolved_at = time.time()
        seen_metrics_by_signal: dict[str, set[tuple[str, str, str, str]]] = {}
        for mapping_match in mapping_matches:
            mapping = mapping_match.mapping
            signal_type = str(mapping["signal_type"])
            definition = definitions.get(signal_type)
            if definition is None:
                continue
            seen_metrics = seen_metrics_by_signal.setdefault(signal_type, set())
            for metric_name in mapping_match.metric_names:
                for entry in entries_by_name.get(metric_name, []):
                    entry_datasource_type = entry.datasource_type or context_datasource_type
                    if not _context_matches(
                        mapping,
                        context_service,
                        entry_datasource_type,
                        context_archetype,
                        context_environment,
                    ):
                        continue
                    if mapping.get("governance_ref") and not _governed_scope_matches(
                        mapping,
                        knowledge_scope=knowledge_scope,
                        context_service=context_service,
                        context_datasource_type=entry_datasource_type,
                        context_archetype=context_archetype,
                        context_environment=context_environment,
                        now=resolved_at,
                    ):
                        continue
                    metric_key = (
                        entry.datasource_uid,
                        entry_datasource_type,
                        entry.query_language,
                        entry.name,
                    )
                    if metric_key in seen_metrics:
                        continue
                    effective_confidence = _effective_confidence(
                        mapping,
                        resolved_at,
                        context_service=context_service,
                        context_datasource_type=entry_datasource_type,
                        context_archetype=context_archetype,
                        context_environment=context_environment,
                    )
                    adjusted = effective_confidence * _metric_metadata_compatibility(
                        signal_type,
                        definition,
                        entry,
                    )
                    self._consume_resolution_result(work_budget, operation="reverse_result")
                    resolved.append(
                        ResolvedMetricSignal(
                            signal_type=signal_type,
                            entry=entry,
                            confidence=round(adjusted, 4),
                            signal_family=str(definition.get("category") or ""),
                            metric_pattern=str(mapping["metric_pattern"]),
                            governance_ref=str(mapping.get("governance_ref") or ""),
                            governance_revision=int(mapping.get("governance_revision") or 0),
                        )
                    )
                    seen_metrics.add(metric_key)
        resolved.sort(key=lambda item: (-item.confidence, item.signal_type, item.entry.name))
        return resolved

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
            raise SemanticAuthorizationError(
                "global bootstrap mappings may only be written by the packaged catalog loader"
            )
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
        provided_datasource_types = (
            list(normalize_datasource_types(context_datasource_types)) if context_datasource_types is not None else None
        )
        projection_key = resolver_projection_key(provided_datasource_types) if governance_ref else ""
        validated_source_refs, _validated_source_refs_json = _validated_source_ref_payload(list(source_refs or []))
        now = time.time()
        storage_tenant = GLOBAL_BOOTSTRAP_TENANT_ID if source_type == "bootstrap" else tenant_id
        with self._write_connection(connection) as conn:
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
                      AND governance_ref = ? AND projection_key = ?""",
                (storage_tenant, signal_type, metric_pattern, governance_ref, projection_key),
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
            ds_types = _merge(provided_datasource_types, prior["context_datasource_types"] if prior else None)
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
            seen_refs = set(refs)
            for ref in validated_source_refs:
                if ref not in seen_refs:
                    seen_refs.add(ref)
                    refs.append(ref)
            refs, refs_json = _validated_source_ref_payload(refs)
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
                    source_type, source_refs, governance_ref, governance_revision, projection_key, inference_version,
                    review_state, created_at, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant_id, signal_type, metric_pattern, governance_ref, projection_key) DO UPDATE SET
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
                    refs_json,
                    governance_ref,
                    governance_revision,
                    projection_key,
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
        with self._write_connection(connection) as conn:
            updated = conn.execute(
                """UPDATE signal_metric_mappings SET review_state=?, last_seen=?
                   WHERE tenant_id=? AND signal_type=? AND metric_pattern=? AND governance_ref=?""",
                (review_state, time.time(), tenant_id, signal_type, metric_pattern, governance_ref),
            )
            return updated.rowcount > 0

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
        with self._write_connection(connection) as conn:
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
        self._bind_external_connection(connection, require_transaction=True)
        if revision.proposition.kind.value != "signal_mapping":
            return {"active": False, "deactivated": 0, "projected": 0}
        if not allow_dirty and not self.governed_projection_audit_is_current(connection):
            raise RuntimeError("governed signal projection audit is dirty; reopen the signal store to reconcile it")

        active = revision.state.lifecycle_status.value == "active" and revision.state.eligibility.value != "ineligible"
        validated_projection = (
            _validate_projection_revision(
                revision,
                mapping_limit=int(self._settings.signal_resolution_mapping_limit),
            )
            if active
            else None
        )
        projection_source_refs: list[str] = []
        if validated_projection is not None:
            projection_source_refs, source_ref_payload = _validated_source_ref_payload(
                [f"{revision.knowledge_id}@{revision.revision}", *revision.provenance_refs]
            )
            projected_source_ref_children = len(validated_projection.variants) * len(projection_source_refs)
            if projected_source_ref_children > SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN:
                raise _projection_validation_error(
                    "governed_signal_projection_source_ref_child_work_limit_exceeded",
                    "active governed signal authority exceeds the source-reference child work limit",
                    authority_ref=(revision.tenant_id, revision.knowledge_id, revision.revision),
                    expected_count=SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN,
                    projected_count=projected_source_ref_children,
                    validation_reason="source_ref_child_work_limit_exceeded",
                )
            projected_source_ref_bytes = len(validated_projection.variants) * len(source_ref_payload.encode("utf-8"))
            if projected_source_ref_bytes > SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES:
                raise _projection_validation_error(
                    "governed_signal_projection_source_ref_byte_work_limit_exceeded",
                    "active governed signal authority exceeds the source-reference byte work limit",
                    authority_ref=(revision.tenant_id, revision.knowledge_id, revision.revision),
                    expected_count=SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES,
                    projected_count=projected_source_ref_bytes,
                    validation_reason="source_ref_byte_work_limit_exceeded",
                )

        deactivated = self.deactivate_governed_mappings(
            tenant_id=revision.tenant_id,
            governance_ref=revision.knowledge_id,
            connection=connection,
        )
        if not active:
            return {"active": False, "deactivated": deactivated, "projected": 0}

        assert validated_projection is not None

        review_state = (
            revision.state.review_state.value
            if revision.state.review_state.value in {"approved", "trusted"}
            else "candidate"
        )
        for metric_pattern, datasource_types, confidence in validated_projection.variants:
            self.add_mapping(
                validated_projection.signal_type,
                metric_pattern,
                confidence=confidence,
                context_services=_projection_scope_values(revision.scope.service_refs, "entity:service:"),
                context_environments=_projection_scope_values(revision.scope.environment_refs, "environment:"),
                context_datasource_types=list(datasource_types),
                context_archetypes=_projection_scope_values(revision.scope.archetype_refs, "archetype:"),
                context_regions=_projection_scope_values(revision.scope.region_refs, "region:"),
                context_clusters=_projection_scope_values(revision.scope.cluster_refs, "cluster:"),
                context_namespaces=_projection_scope_values(revision.scope.namespace_refs, "namespace:"),
                context_versions=_projection_scope_values(revision.scope.version_constraints, "version:"),
                valid_from=revision.scope.valid_from.timestamp() if revision.scope.valid_from else None,
                valid_until=revision.scope.valid_until.timestamp() if revision.scope.valid_until else None,
                source_type="operational_knowledge",
                source_refs=projection_source_refs,
                governance_ref=revision.knowledge_id,
                governance_revision=revision.revision,
                inference_version=f"{revision.policy_id}:{revision.policy_version}",
                review_state=review_state,
                tenant_id=revision.tenant_id,
                connection=connection,
                replace_existing=True,
                increment_use_count=False,
            )
        return {"active": True, "deactivated": deactivated, "projected": len(validated_projection.variants)}

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
        defer_datasource_scope: bool = False,
    ) -> list[dict[str, Any]]:
        """Get all metric mappings for a signal, optionally filtered by context.

        Returns mappings sorted by effective confidence (adjusted for decay
        and feedback).
        """
        tenant_id = self._resolve_tenant(tenant_id)
        pinned = self._pinned_governed_mappings.get()
        if pinned is not None:
            _require_pinned_governed_mapping_tenant(
                pinned_tenant=pinned.tenant_id,
                requested_tenant=tenant_id,
            )
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
                "" if defer_datasource_scope else context_datasource_type,
                context_archetype,
                context_environment,
            ):
                return
            if mapping.get("governance_ref") and not _governed_scope_matches(
                mapping,
                knowledge_scope=knowledge_scope,
                context_service=context_service,
                context_datasource_type="" if defer_datasource_scope else context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                now=now,
                defer_datasource_scope=defer_datasource_scope,
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
                    reason_code="signal_resolution_mapping_limit_exceeded",
                    tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
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
        last_id: int | None = None
        exhausted = False
        with self._conn() as conn:
            while scan_limit is None or scanned < scan_limit:
                page_limit = _SIGNAL_RESOLUTION_PAGE_SIZE
                if scan_limit is not None:
                    page_limit = min(page_limit, scan_limit - scanned)
                id_clause, id_params = _ascending_integer_keyset("id", last_id)
                if pinned is None:
                    rows = conn.execute(
                        f"""SELECT * FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id IN (?, ?)
                             AND review_state IN ('approved', 'trusted')
                             {id_clause}
                           ORDER BY id LIMIT ?""",
                        (
                            signal_type,
                            tenant_id,
                            GLOBAL_BOOTSTRAP_TENANT_ID,
                            *id_params,
                            page_limit,
                        ),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        f"""SELECT * FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id = ?
                             AND source_type = 'bootstrap'
                             AND review_state IN ('approved', 'trusted')
                             {id_clause}
                           ORDER BY id LIMIT ?""",
                        (signal_type, GLOBAL_BOOTSTRAP_TENANT_ID, *id_params, page_limit),
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
                id_clause, id_params = _ascending_integer_keyset("id", last_id)
                if pinned is None:
                    more = conn.execute(
                        f"""SELECT 1 FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id IN (?, ?)
                             AND review_state IN ('approved', 'trusted')
                             {id_clause} LIMIT 1""",
                        (signal_type, tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID, *id_params),
                    ).fetchone()
                else:
                    more = conn.execute(
                        f"""SELECT 1 FROM signal_metric_mappings
                           WHERE signal_type = ?
                             AND tenant_id = ?
                             AND source_type = 'bootstrap'
                             AND review_state IN ('approved', 'trusted')
                             {id_clause} LIMIT 1""",
                        (signal_type, GLOBAL_BOOTSTRAP_TENANT_ID, *id_params),
                    ).fetchone()
                if more is not None:
                    logger.error(
                        "signal_resolution_mapping_scan_limit_exceeded",
                        reason_code="signal_resolution_mapping_scan_limit_exceeded",
                        tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
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
        seen_identities: set[tuple[str, tuple[str, ...], str, int]] = set()
        for mapping in applicable:
            identity = (
                str(mapping["metric_pattern"]),
                tuple(sorted(str(value).casefold() for value in mapping.get("context_datasource_types", []))),
                str(mapping.get("governance_ref") or ""),
                int(mapping.get("governance_revision") or 0),
            )
            if identity in seen_identities:
                continue
            seen_identities.add(identity)
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
        work_budget: SignalResolutionWorkBudget | None = None,
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
                work_budget=work_budget,
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
        work_budget: SignalResolutionWorkBudget | None = None,
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
        work_budget = self._begin_resolution_work(work_budget, operation="forward")
        effective_datasource_type = context_datasource_type
        if not effective_datasource_type and target_query_language:
            effective_datasource_type = language_to_datasource_type(target_query_language.casefold())
        mappings = self.get_mappings_for_signal(
            signal_type,
            context_service=context_service,
            context_datasource_type="",
            context_archetype=context_archetype,
            context_environment=context_environment,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            excluded_knowledge_refs=excluded_knowledge_refs,
            resolution_limit=int(self._settings.signal_resolution_mapping_limit),
            defer_datasource_scope=True,
        )

        if not mappings:
            return []

        target_lang = target_query_language.lower()
        target_ds = effective_datasource_type.lower()
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
                reason_code="signal_resolution_catalog_limit_exceeded",
                tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                signal_type=signal_type,
                catalog_count=len(eligible_catalog),
                catalog_limit=catalog_limit,
            )
            raise RuntimeError(f"Signal resolution catalog has more than {catalog_limit} eligible metrics")
        self._reserve_resolution_comparisons(
            work_budget,
            mapping_count=len(mappings),
            eligible_catalog_count=len(eligible_catalog),
            operation="forward",
        )
        matched_by_metric: dict[tuple[str, str, str, str], ResolvedSignal] = {}

        sig_type = self.get_signal_definition(signal_type, tenant_id=tenant_id)

        resolution_now = time.time()
        for mapping in mappings:
            pattern = mapping["metric_pattern"]

            for entry in eligible_catalog:
                entry_datasource_type = entry.datasource_type or effective_datasource_type
                if not _context_matches(
                    mapping,
                    context_service,
                    entry_datasource_type,
                    context_archetype,
                    context_environment,
                ):
                    continue
                if mapping.get("governance_ref") and not _governed_scope_matches(
                    mapping,
                    knowledge_scope=knowledge_scope,
                    context_service=context_service,
                    context_datasource_type=entry_datasource_type,
                    context_archetype=context_archetype,
                    context_environment=context_environment,
                    now=resolution_now,
                ):
                    continue
                if not _metric_matches_pattern(entry.name, pattern):
                    continue
                effective_confidence = _effective_confidence(
                    mapping,
                    resolution_now,
                    context_service=context_service,
                    context_datasource_type=entry_datasource_type,
                    context_archetype=context_archetype,
                    context_environment=context_environment,
                )
                adjusted = effective_confidence * _metric_metadata_compatibility(signal_type, sig_type or {}, entry)
                metric_key = (
                    entry.datasource_uid,
                    entry.datasource_type,
                    entry.query_language,
                    entry.name,
                )
                current = matched_by_metric.get(metric_key)
                if current is None:
                    self._consume_resolution_result(work_budget, operation="forward_result")
                result = ResolvedSignal(
                    entry=entry,
                    confidence=round(adjusted, 4),
                    governance_ref=str(mapping.get("governance_ref") or ""),
                    governance_revision=int(mapping.get("governance_revision") or 0),
                )
                if current is None or result.confidence > current.confidence:
                    matched_by_metric[metric_key] = result

        matched = list(matched_by_metric.values())
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
        work_budget: SignalResolutionWorkBudget | None = None,
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
                work_budget=work_budget,
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
                    logger.info(
                        "signals_yaml_unchanged",
                        reason_code="signals_yaml_unchanged",
                        source_fingerprint=_diagnostic_fingerprint(source),
                        fingerprint=fingerprint,
                    )
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

        logger.info(
            "signals_loaded_from_yaml",
            reason_code="signals_loaded_from_yaml",
            source_fingerprint=_diagnostic_fingerprint(source),
            mappings=count,
            fingerprint=fingerprint,
        )
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
        strict: bool = False,
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
            logger.warning(
                "learning_context_index_failed",
                **_exception_diagnostics("learning_context_index_failed", exc),
            )
            if strict:
                raise
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
        strict: bool = False,
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
            logger.warning(
                "artifact_context_index_failed",
                **_exception_diagnostics("artifact_context_index_failed", exc),
            )
            if strict:
                raise
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
        authority_reconciler: Callable[[sqlite3.Connection, dict[str, Any]], None],
    ) -> int:
        """Atomically stale absent artifacts and retire their runtime authority."""
        tenant_id = self._resolve_tenant(tenant_id)
        crawl_started_at = crawl_started_at if crawl_started_at is not None else time.time()
        marked_at = time.time()
        after_id: int | None = None
        total = 0
        while True:
            clauses = [
                "tenant_id = ?",
                "artifact_type = ?",
                "stale = 0",
                "last_seen_at <= ?",
            ]
            params: list[Any] = [tenant_id, artifact_type, crawl_started_at]
            if after_id is not None:
                clauses.append("id > ?")
                params.append(after_id)
            if source_vendor is not None:
                clauses.append("source_vendor = ?")
                params.append(source_vendor)
            if source_instance is not None:
                clauses.append("source_instance = ?")
                params.append(source_instance)
            if external_id_prefix is not None:
                clauses.append("external_id LIKE ? ESCAPE '\\'")
                params.append(f"{_escape_like_prefix(external_id_prefix)}%")
            with self._conn() as conn:
                rows = conn.execute(
                    f"""SELECT id, artifact_id, last_seen_at, missing_since FROM learned_artifacts
                        WHERE {" AND ".join(clauses)}
                        ORDER BY id LIMIT ?""",
                    (*params, _STALE_SOURCE_PAGE_SIZE),
                ).fetchall()
            if not rows:
                break
            after_id = int(rows[-1]["id"])
            for row in rows:
                artifact_id = str(row["artifact_id"])
                if artifact_id in seen_artifact_ids:
                    continue
                with self.transaction() as conn:
                    cursor = conn.execute(
                        """UPDATE learned_artifacts
                           SET stale=1, missing_since=COALESCE(missing_since, ?),
                               knowledge_reconciled_at=NULL, updated_at=?
                           WHERE tenant_id=? AND id=? AND stale=0 AND last_seen_at=?""",
                        (marked_at, marked_at, tenant_id, row["id"], row["last_seen_at"]),
                    )
                    if cursor.rowcount != 1:
                        continue
                    missing_since = row["missing_since"] if row["missing_since"] is not None else marked_at
                    if self._learning_index_available():
                        conn.execute(
                            """DELETE FROM learning_context_fts
                               WHERE tenant_id=? AND source_kind=? AND source_id=?""",
                            (tenant_id, artifact_type, artifact_id),
                        )
                    self._remove_mapping_source_refs(
                        conn,
                        tenant_id=tenant_id,
                        source_type=artifact_type,
                        stale_refs={artifact_id},
                    )
                    stale_generation = {
                        "id": int(row["id"]),
                        "artifact_id": artifact_id,
                        "missing_since": missing_since,
                    }
                    authority_reconciler(conn, stale_generation)
                    checkpoint = conn.execute(
                        """UPDATE learned_artifacts SET knowledge_reconciled_at=?
                           WHERE tenant_id=? AND id=? AND stale=1 AND missing_since IS ?
                             AND knowledge_reconciled_at IS NULL""",
                        (time.time(), tenant_id, row["id"], missing_since),
                    )
                    if checkpoint.rowcount != 1:
                        raise RuntimeError("stale artifact generation changed during authority reconciliation")
                    total += 1
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
        if not 1 <= limit <= 500 or not 0 <= offset <= MAX_COMPATIBILITY_OFFSET:
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
            conditions.append("(updated_at, id) < (?, ?)")
            params.extend([updated_at, row_id])
        params.extend([limit + 1, offset])
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
        after_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return a keyset page of stale artifacts whose knowledge transition is pending."""
        tenant_id = self._resolve_tenant(tenant_id)
        id_clause, id_params = _ascending_integer_keyset("id", after_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, artifact_id, missing_since FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_type=? AND stale=1
                     AND knowledge_reconciled_at IS NULL"""
                + id_clause
                + """
                   ORDER BY id LIMIT ?""",
                (tenant_id, artifact_type, *id_params, limit),
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
        if not 1 <= limit <= 500:
            raise ValueError("extraction page limit must be between 1 and 500")
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
                    WHERE {" AND ".join(conditions)}
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
        unique_ids = list(dict.fromkeys(str(value) for value in artifact_ids))
        counts = {artifact_id: {kind: 0 for kind in _ARTIFACT_EXTRACTION_TABLES} for artifact_id in unique_ids}
        if not unique_ids:
            return counts
        with self.read_transaction() as conn:
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
        strict: bool = False,
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
            logger.warning(
                "artifact_context_index_check_failed",
                **_exception_diagnostics("artifact_context_index_check_failed", exc),
            )
            if strict:
                raise
            return True
        return row is not None

    def mark_missing_alerts_stale(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        seen_alert_uids: set[str],
        crawl_started_at: float | None = None,
        authority_reconciler: Callable[[sqlite3.Connection, dict[str, Any]], None],
    ) -> int:
        """Atomically stale absent alerts and retire their runtime authority."""
        tenant_id = self._resolve_tenant(tenant_id)
        crawl_started_at = crawl_started_at if crawl_started_at is not None else time.time()
        marked_at = time.time()
        approval_claim_cutoff = marked_at - self._settings.learning_approval_claim_ttl_seconds
        after_id: int | None = None
        total = 0
        recovered_claims = 0
        while True:
            with self._conn() as conn:
                id_clause, id_params = _ascending_integer_keyset("id", after_id)
                rows = conn.execute(
                    """SELECT id, alert_uid, last_seen_at, status, missing_since FROM ingested_alerts
                       WHERE tenant_id=? AND backend_name=? AND stale=0
                         AND (status!='approving' OR reviewed_at IS NULL OR reviewed_at<=?)
                         AND last_seen_at<=?"""
                    + id_clause
                    + """
                       ORDER BY id LIMIT ?""",
                    (
                        tenant_id,
                        backend_name,
                        approval_claim_cutoff,
                        crawl_started_at,
                        *id_params,
                        _STALE_SOURCE_PAGE_SIZE,
                    ),
                ).fetchall()
            if not rows:
                break
            after_id = int(rows[-1]["id"])
            for row in rows:
                alert_uid = str(row["alert_uid"])
                if alert_uid in seen_alert_uids:
                    continue
                with self.transaction() as conn:
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
                    if cursor.rowcount != 1:
                        continue
                    missing_since = row["missing_since"] if row["missing_since"] is not None else marked_at
                    recovered_claims += int(row["status"] == "approving")
                    total += 1
                    if self._learning_index_available():
                        conn.execute(
                            """UPDATE learning_context_fts SET review_state='stale'
                               WHERE tenant_id=? AND source_kind='alert_rule' AND backend_name=?
                                 AND dashboard_uid=?""",
                            (tenant_id, backend_name, f"alert:{alert_uid}"),
                        )
                    source_ref = f"{backend_name}:alert:{alert_uid}" if backend_name else alert_uid
                    self._remove_mapping_source_refs(
                        conn,
                        tenant_id=tenant_id,
                        source_type="alert_ingest",
                        stale_refs={source_ref},
                    )
                    stale_generation = {
                        "id": int(row["id"]),
                        "alert_uid": alert_uid,
                        "missing_since": missing_since,
                    }
                    authority_reconciler(conn, stale_generation)
                    checkpoint = conn.execute(
                        """UPDATE ingested_alerts SET knowledge_reconciled_at=?
                           WHERE tenant_id=? AND id=? AND stale=1 AND missing_since IS ?
                             AND knowledge_reconciled_at IS NULL""",
                        (time.time(), tenant_id, row["id"], missing_since),
                    )
                    if checkpoint.rowcount != 1:
                        raise RuntimeError("stale alert generation changed during authority reconciliation")
        if recovered_claims:
            logger.warning(
                "expired_alert_approval_claims_recovered",
                reason_code="expired_alert_approval_claims_recovered",
                tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                backend_fingerprint=_diagnostic_fingerprint(backend_name),
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
        authority_reconciler: Callable[[sqlite3.Connection, dict[str, Any]], None],
    ) -> int:
        """Atomically stale absent dashboards and retire their runtime authority."""
        tenant_id = self._resolve_tenant(tenant_id)
        crawl_started_at = crawl_started_at if crawl_started_at is not None else time.time()
        marked_at = time.time()
        approval_claim_cutoff = marked_at - self._settings.learning_approval_claim_ttl_seconds
        after_id: int | None = None
        total = 0
        recovered_claims = 0
        while True:
            with self._conn() as conn:
                id_clause, id_params = _ascending_integer_keyset("id", after_id)
                rows = conn.execute(
                    """SELECT id, dashboard_uid, last_seen_at, status, missing_since FROM ingested_dashboards
                       WHERE tenant_id=? AND backend_name=? AND stale=0
                         AND (status!='approving' OR reviewed_at IS NULL OR reviewed_at<=?)
                         AND last_seen_at<=?"""
                    + id_clause
                    + """
                       ORDER BY id LIMIT ?""",
                    (
                        tenant_id,
                        backend_name,
                        approval_claim_cutoff,
                        crawl_started_at,
                        *id_params,
                        _STALE_SOURCE_PAGE_SIZE,
                    ),
                ).fetchall()
            if not rows:
                break
            after_id = int(rows[-1]["id"])
            for row in rows:
                dashboard_uid = str(row["dashboard_uid"])
                if dashboard_uid in seen_dashboard_uids:
                    continue
                with self.transaction() as conn:
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
                    if cursor.rowcount != 1:
                        continue
                    missing_since = row["missing_since"] if row["missing_since"] is not None else marked_at
                    recovered_claims += int(row["status"] == "approving")
                    total += 1
                    if self._learning_index_available():
                        conn.execute(
                            """UPDATE learning_context_fts SET review_state='stale'
                               WHERE tenant_id=? AND source_kind='dashboard_panel' AND backend_name=?
                                 AND dashboard_uid=?""",
                            (tenant_id, backend_name, dashboard_uid),
                        )
                    source_ref = f"{backend_name}:{dashboard_uid}" if backend_name else dashboard_uid
                    self._remove_mapping_source_refs(
                        conn,
                        tenant_id=tenant_id,
                        source_type="dashboard_ingest",
                        stale_refs={source_ref},
                    )
                    stale_generation = {
                        "id": int(row["id"]),
                        "dashboard_uid": dashboard_uid,
                        "missing_since": missing_since,
                    }
                    authority_reconciler(conn, stale_generation)
                    checkpoint = conn.execute(
                        """UPDATE ingested_dashboards SET knowledge_reconciled_at=?
                           WHERE tenant_id=? AND id=? AND stale=1 AND missing_since IS ?
                             AND knowledge_reconciled_at IS NULL""",
                        (time.time(), tenant_id, row["id"], missing_since),
                    )
                    if checkpoint.rowcount != 1:
                        raise RuntimeError("stale dashboard generation changed during authority reconciliation")
        if recovered_claims:
            logger.warning(
                "expired_dashboard_approval_claims_recovered",
                reason_code="expired_dashboard_approval_claims_recovered",
                tenant_fingerprint=_diagnostic_fingerprint(tenant_id),
                backend_fingerprint=_diagnostic_fingerprint(backend_name),
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
        strict: bool = False,
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
            logger.warning(
                "alert_context_index_failed",
                **_exception_diagnostics("alert_context_index_failed", exc),
            )
            if strict:
                raise
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
                logger.warning(
                    "learning_context_review_state_update_failed",
                    **_exception_diagnostics("learning_context_review_state_update_failed", exc),
                )
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
                logger.warning(
                    "learning_context_search_failed",
                    query_fingerprint=_diagnostic_fingerprint(query),
                    **_exception_diagnostics("learning_context_search_failed", exc),
                )
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
        if offset < 0 or offset > MAX_COMPATIBILITY_OFFSET:
            raise ValueError(f"offset must be between 0 and {MAX_COMPATIBILITY_OFFSET}")
        cursor = _validated_ingested_source_cursor(before_created_at, before_id)
        if cursor is not None and offset:
            raise ValueError("cursor and offset pagination cannot be combined")
        tenant_id = self._resolve_tenant(tenant_id)
        sql, params = _ingested_source_page_statement(
            "dashboard",
            tenant_id=tenant_id,
            status=status,
            backend_name=backend_name,
            cursor=cursor,
            limit=limit,
            offset=offset,
        )
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_deserialize_ingested(r) for r in rows]

    def list_unreconciled_stale_dashboards(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        limit: int = 500,
        after_id: int | None = None,
    ) -> list[dict[str, Any]]:
        tenant_id = self._resolve_tenant(tenant_id)
        id_clause, id_params = _ascending_integer_keyset("id", after_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, dashboard_uid, missing_since FROM ingested_dashboards
                   WHERE tenant_id=? AND backend_name=? AND stale=1
                     AND knowledge_reconciled_at IS NULL"""
                + id_clause
                + """
                   ORDER BY id LIMIT ?""",
                (tenant_id, backend_name, *id_params, limit),
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
        if offset < 0 or offset > MAX_COMPATIBILITY_OFFSET:
            raise ValueError(f"offset must be between 0 and {MAX_COMPATIBILITY_OFFSET}")
        cursor = _validated_ingested_source_cursor(before_created_at, before_id)
        if cursor is not None and offset:
            raise ValueError("cursor and offset pagination cannot be combined")
        tenant_id = self._resolve_tenant(tenant_id)
        sql, params = _ingested_source_page_statement(
            "alert",
            tenant_id=tenant_id,
            status=status,
            backend_name=backend_name,
            cursor=cursor,
            limit=limit,
            offset=offset,
        )
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_deserialize_ingested_alert(r) for r in rows]

    def list_unreconciled_stale_alerts(
        self,
        *,
        tenant_id: str | None = None,
        backend_name: str,
        limit: int = 500,
        after_id: int | None = None,
    ) -> list[dict[str, Any]]:
        tenant_id = self._resolve_tenant(tenant_id)
        id_clause, id_params = _ascending_integer_keyset("id", after_id)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, alert_uid, missing_since FROM ingested_alerts
                   WHERE tenant_id=? AND backend_name=? AND stale=1
                     AND knowledge_reconciled_at IS NULL"""
                + id_clause
                + """
                   ORDER BY id LIMIT ?""",
                (tenant_id, backend_name, *id_params, limit),
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
            category_rows = conn.execute(
                """WITH effective_definitions AS (
                       SELECT CASE WHEN t.category != '' THEN t.category
                                   ELSE COALESCE(g.category, '') END AS category
                       FROM tenant_signal_types t
                       LEFT JOIN signal_types g ON g.signal_type=t.signal_type
                       WHERE t.tenant_id=?
                       UNION ALL
                       SELECT g.category
                       FROM signal_types g
                       WHERE NOT EXISTS (
                           SELECT 1 FROM tenant_signal_types t
                           WHERE t.tenant_id=? AND t.signal_type=g.signal_type
                       )
                   )
                   SELECT category, COUNT(*) AS n
                   FROM effective_definitions
                   GROUP BY category""",
                (tenant_id, tenant_id),
            ).fetchall()

        by_category = {str(row["category"]): int(row["n"]) for row in category_rows}
        signal_type_count = sum(by_category.values())

        return {
            "signal_types": signal_type_count,
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
    defer_datasource_scope: bool = False,
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
        if field == "context_datasource_types" and defer_datasource_scope:
            continue
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


def _deserialize_mapping(row: Mapping[str, Any]) -> dict[str, Any]:
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
