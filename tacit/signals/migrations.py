"""SQLite schema migration helpers for the signal store."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import structlog

from tacit.signals.projection import (
    normalize_datasource_types,
    normalize_mapping_confidence,
    resolver_projection_key,
)
from tacit.signals.schema import FTS_SCHEMA_SQL, GLOBAL_BOOTSTRAP_TENANT_ID, SCHEMA_SQL

logger = structlog.get_logger()

_RETARGETABLE_TENANT_TABLES = (
    "tenant_signal_types",
    "rejected_signal_candidates",
    "ingested_dashboards",
    "ingested_alerts",
    "learned_artifacts",
    "evidence_requirements",
    "ownership_hints",
    "dependency_hints",
    "signal_mapping_candidates",
    "learning_context_fts",
)

_GOVERNED_TENANT_TABLES = (
    "knowledge_candidates",
    "knowledge_candidate_evidence",
    "knowledge_candidate_provenance",
    "knowledge_candidate_entity_refs",
    "promotion_decisions",
    "entities",
    "entity_aliases",
    "entity_resolution_attempts",
    "knowledge_propositions",
    "proposition_candidates",
    "knowledge_conflicts",
    "corroboration_snapshots",
    "operational_knowledge",
    "operational_knowledge_revisions",
    "knowledge_current_scope_refs",
    "knowledge_current_contributors",
    "candidate_promotions",
    "knowledge_snapshots",
    "knowledge_usage_events",
    "knowledge_corrections",
    "knowledge_events",
)

_TENANT_OWNED_TABLES = (*_RETARGETABLE_TENANT_TABLES, *_GOVERNED_TENANT_TABLES)

_DEFAULT_OWNER_MARKER = "default_owner_v1"
_DEFAULT_OWNER_PROGRESS_MARKER = "default_owner_in_progress_v1"
_DEFAULT_OWNER_CURSOR_PREFIX = "default_owner_cursor_v1:"
_SIGNAL_DEFINITION_SCOPE_MARKER = "signal_definition_scope_v1"
_SIGNAL_DEFINITION_BOOTSTRAP_MARKER = "signal_definition_bootstrap_v1"
CURRENT_SIGNAL_SCHEMA_MARKER = "signal_schema_operational_knowledge_v4"
GOVERNED_PROJECTION_AUDIT_MARKER = "governed_projection_audit_v2"
MAPPING_SOURCE_REF_INDEX_MARKER = "mapping_source_ref_index_v2"
_MAPPING_SOURCE_REF_CURSOR_MARKER = "mapping_source_ref_cursor_v2"
_MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER = "mapping_source_ref_orphan_cursor_v2"
SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN = 10_001
SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES = 4 * 1024 * 1024
SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT = SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN
SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES = SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES
_FTS_CAPABILITY_MARKER = "learning_context_fts_capability_v1"
_FTS_CAPABILITY_AVAILABLE = "available"
_FTS_CAPABILITY_UNAVAILABLE = "unavailable"
_LEGACY_SCHEMA_OWNER_MARKER = "legacy_schema_owner_v1"
_LEGACY_SCHEMA_COPY_CURSOR_PREFIX = "legacy_schema_copy_cursor_v1:"
_LEGACY_SCHEMA_SUFFIX = "_tacit_tenant_migration_v1"

_GOVERNED_PROJECTION_AUDIT_INDEX_SQL = """CREATE INDEX idx_smm_governed_revision_audit
       ON signal_metric_mappings(
           tenant_id, governance_ref, governance_revision, id,
           signal_type, metric_pattern, context_datasource_types, review_state
       )
       WHERE governance_ref != '' AND review_state IN ('approved', 'trusted')"""
_UNGOVERNED_PROJECTION_AUDIT_INDEX_SQL = """CREATE INDEX idx_smm_ungoverned_audit
       ON signal_metric_mappings(tenant_id, id, governance_ref, source_type, review_state)
       WHERE governance_ref = '' AND source_type != 'bootstrap'
         AND review_state IN ('approved', 'trusted')"""
_SOURCE_REF_INDEX_SQL = """CREATE INDEX IF NOT EXISTS idx_signal_mapping_source_ref
       ON signal_mapping_source_refs(tenant_id, source_ref, mapping_id)"""
_OPERATIONAL_KNOWLEDGE_SIGNAL_PROJECTION_PAGE_INDEX_SQL = """CREATE INDEX
       idx_operational_knowledge_signal_projection_page
       ON operational_knowledge(kind, status, tenant_id, knowledge_id, current_revision)"""

_GOVERNED_PROJECTION_TRIGGER_SQL = {
    "trg_governed_mapping_insert_audit_dirty": f"""CREATE TRIGGER trg_governed_mapping_insert_audit_dirty
       AFTER INSERT ON signal_metric_mappings
       WHEN NEW.governance_ref != ''
         OR (NEW.source_type != 'bootstrap' AND NEW.review_state IN ('approved', 'trusted'))
       BEGIN
         INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
         VALUES ('{GOVERNED_PROJECTION_AUDIT_MARKER}',
                 'dirty:' || lower(hex(randomblob(8))), CAST(strftime('%s', 'now') AS REAL))
         ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
       END""",
    "trg_governed_mapping_update_audit_dirty": f"""CREATE TRIGGER trg_governed_mapping_update_audit_dirty
       AFTER UPDATE ON signal_metric_mappings
       WHEN OLD.governance_ref != '' OR NEW.governance_ref != ''
         OR (OLD.source_type != 'bootstrap' AND OLD.review_state IN ('approved', 'trusted'))
         OR (NEW.source_type != 'bootstrap' AND NEW.review_state IN ('approved', 'trusted'))
       BEGIN
         INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
         VALUES ('{GOVERNED_PROJECTION_AUDIT_MARKER}',
                 'dirty:' || lower(hex(randomblob(8))), CAST(strftime('%s', 'now') AS REAL))
         ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
       END""",
    "trg_governed_mapping_delete_audit_dirty": f"""CREATE TRIGGER trg_governed_mapping_delete_audit_dirty
       AFTER DELETE ON signal_metric_mappings
       WHEN OLD.governance_ref != ''
         OR (OLD.source_type != 'bootstrap' AND OLD.review_state IN ('approved', 'trusted'))
       BEGIN
         INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
         VALUES ('{GOVERNED_PROJECTION_AUDIT_MARKER}',
                 'dirty:' || lower(hex(randomblob(8))), CAST(strftime('%s', 'now') AS REAL))
         ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
       END""",
}

_SOURCE_REF_TRIGGER_SQL = {
    "trg_signal_mapping_source_refs_limit_insert": f"""CREATE TRIGGER trg_signal_mapping_source_refs_limit_insert
       BEFORE INSERT ON signal_metric_mappings
       WHEN length(CAST(NEW.source_refs AS BLOB))>{SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES}
         OR (json_valid(NEW.source_refs)=1 AND json_type(NEW.source_refs)='array'
             AND json_array_length(NEW.source_refs)>{SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT})
       BEGIN
           SELECT RAISE(ABORT, 'signal mapping source_refs exceeds the storage limit');
       END""",
    "trg_signal_mapping_source_refs_limit_update": f"""CREATE TRIGGER trg_signal_mapping_source_refs_limit_update
       BEFORE UPDATE OF source_refs ON signal_metric_mappings
       WHEN length(CAST(NEW.source_refs AS BLOB))>{SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES}
         OR (json_valid(NEW.source_refs)=1 AND json_type(NEW.source_refs)='array'
             AND json_array_length(NEW.source_refs)>{SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT})
       BEGIN
           SELECT RAISE(ABORT, 'signal mapping source_refs exceeds the storage limit');
       END""",
    "trg_signal_mapping_source_refs_validate_insert": """CREATE TRIGGER trg_signal_mapping_source_refs_validate_insert
       BEFORE INSERT ON signal_metric_mappings
       WHEN CASE
           WHEN json_valid(NEW.source_refs)=0 THEN 1
           WHEN json_type(NEW.source_refs)!='array' THEN 1
           ELSE EXISTS (
               SELECT 1 FROM json_each(NEW.source_refs)
               WHERE type!='text' OR value='' OR value!=trim(value)
           )
       END
       BEGIN
           SELECT RAISE(ABORT, 'signal mapping source_refs must be a JSON string array');
       END""",
    "trg_signal_mapping_source_refs_validate_update": """CREATE TRIGGER trg_signal_mapping_source_refs_validate_update
       BEFORE UPDATE OF source_refs ON signal_metric_mappings
       WHEN CASE
           WHEN json_valid(NEW.source_refs)=0 THEN 1
           WHEN json_type(NEW.source_refs)!='array' THEN 1
           ELSE EXISTS (
               SELECT 1 FROM json_each(NEW.source_refs)
               WHERE type!='text' OR value='' OR value!=trim(value)
           )
       END
       BEGIN
           SELECT RAISE(ABORT, 'signal mapping source_refs must be a JSON string array');
       END""",
    "trg_signal_mapping_source_ref_insert": """CREATE TRIGGER trg_signal_mapping_source_ref_insert
       AFTER INSERT ON signal_metric_mappings
       BEGIN
           INSERT OR IGNORE INTO signal_mapping_source_refs (mapping_id, tenant_id, source_ref)
           SELECT NEW.id, NEW.tenant_id, value
           FROM json_each(NEW.source_refs)
           WHERE type='text' AND value != '';
       END""",
    "trg_signal_mapping_source_ref_update": """CREATE TRIGGER trg_signal_mapping_source_ref_update
       AFTER UPDATE OF tenant_id, source_refs ON signal_metric_mappings
       BEGIN
           DELETE FROM signal_mapping_source_refs WHERE mapping_id=OLD.id;
           INSERT OR IGNORE INTO signal_mapping_source_refs (mapping_id, tenant_id, source_ref)
           SELECT NEW.id, NEW.tenant_id, value
           FROM json_each(NEW.source_refs)
           WHERE type='text' AND value != '';
       END""",
    "trg_signal_mapping_source_ref_delete": """CREATE TRIGGER trg_signal_mapping_source_ref_delete
       AFTER DELETE ON signal_metric_mappings
       BEGIN
           DELETE FROM signal_mapping_source_refs WHERE mapping_id=OLD.id;
       END""",
}


def _normalized_schema_sql(value: object) -> str:
    normalized = " ".join(str(value or "").split()).casefold()
    return normalized.replace("create index if not exists", "create index")


def _schema_object_matches(
    conn: sqlite3.Connection,
    *,
    object_type: str,
    name: str,
    expected_sql: str,
) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type=? AND name=?",
        (object_type, name),
    ).fetchone()
    return row is not None and _normalized_schema_sql(row["sql"]) == _normalized_schema_sql(expected_sql)


def _legacy_schema_table(table: str) -> str:
    return f"{table}{_LEGACY_SCHEMA_SUFFIX}"


def _legacy_schema_copy_pending(conn: sqlite3.Connection) -> bool:
    return any(
        str(row["name"]).endswith(_LEGACY_SCHEMA_SUFFIX)
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    )


def _optional_integer_cursor(row: Any | None, *, name: str) -> int | None:
    """Decode a cursor while keeping absence distinct from every integer key."""
    if row is None:
        return None
    try:
        return int(row["value"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"{name} cursor is invalid") from exc


def _diagnostic_fingerprint(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8", errors="replace")).hexdigest()[:16]


def _bind_legacy_schema_owner(
    conn: sqlite3.Connection,
    *,
    owner: str,
    migration_pending: bool,
) -> None:
    row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_LEGACY_SCHEMA_OWNER_MARKER,),
    ).fetchone()
    if row is not None and str(row["value"]) != owner:
        raise RuntimeError("Signal legacy schema migration owner does not match the configured tenant")
    if migration_pending and row is None:
        _record_migration_marker(conn, _LEGACY_SCHEMA_OWNER_MARKER, owner)


def _bind_default_owner_migration(conn: sqlite3.Connection, *, owner: str) -> None:
    """Claim the tenant migration before any tenant-specific schema copy."""
    terminal = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_DEFAULT_OWNER_MARKER,),
    ).fetchone()
    if terminal is not None:
        if str(terminal["value"]) != owner:
            raise RuntimeError("Signal database tenant owner does not match the configured tenant")
        return
    progress = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_DEFAULT_OWNER_PROGRESS_MARKER,),
    ).fetchone()
    if progress is not None and str(progress["value"]) != owner:
        raise RuntimeError("Signal database tenant owner migration belongs to another tenant")
    if progress is None:
        _record_migration_marker(conn, _DEFAULT_OWNER_PROGRESS_MARKER, owner)


def _prepare_legacy_schema_copy(
    conn: sqlite3.Connection,
    *,
    table: str,
    create_target: str,
) -> None:
    """Atomically create an empty shadow while retaining the legacy source."""
    shadow_table = _legacy_schema_table(table)
    if _table_exists(conn, shadow_table):
        if not _table_exists(conn, table):
            raise RuntimeError(f"Incomplete legacy schema migration for {table}")
        return
    target_sql = create_target
    replacements = (
        (f"CREATE VIRTUAL TABLE IF NOT EXISTS {table}", f"CREATE VIRTUAL TABLE {shadow_table}"),
        (f"CREATE TABLE IF NOT EXISTS {table}", f"CREATE TABLE {shadow_table}"),
        (f"CREATE TABLE {table}", f"CREATE TABLE {shadow_table}"),
    )
    for original, replacement in replacements:
        if original in target_sql:
            target_sql = target_sql.replace(original, replacement, 1)
            break
    else:
        raise RuntimeError(f"Target schema does not create {table}")
    execute_script_statements(conn, target_sql)


def _copy_legacy_schema_batch(
    conn: sqlite3.Connection,
    *,
    table: str,
    insert_columns: tuple[str, ...],
    select_expressions: tuple[str, ...],
    select_parameters: tuple[Any, ...],
    finalize: Callable[[sqlite3.Connection], None],
    batch_size: int,
) -> tuple[bool, int]:
    """Copy or finalize one legacy table in one bounded writer transaction."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    shadow_table = _legacy_schema_table(table)
    if not _table_exists(conn, shadow_table):
        return True, 0
    cursor_key = f"{_LEGACY_SCHEMA_COPY_CURSOR_PREFIX}{table}"
    cursor_row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (cursor_key,),
    ).fetchone()
    after_rowid = _optional_integer_cursor(cursor_row, name=f"Legacy {table} migration")
    if after_rowid is None:
        rows = conn.execute(
            f"SELECT rowid AS migration_rowid FROM {table} ORDER BY rowid LIMIT ?",
            (batch_size,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT rowid AS migration_rowid FROM {table} WHERE rowid>? ORDER BY rowid LIMIT ?",
            (after_rowid, batch_size),
        ).fetchall()
    if rows:
        last_rowid = int(rows[-1]["migration_rowid"])
        columns_sql = ", ".join(insert_columns)
        expressions_sql = ", ".join(select_expressions)
        if after_rowid is None:
            row_window = "rowid<=?"
            row_parameters: tuple[Any, ...] = (last_rowid,)
        else:
            row_window = "rowid>? AND rowid<=?"
            row_parameters = (after_rowid, last_rowid)
        if table == "signal_metric_mappings":
            _copy_signal_mapping_window(
                conn,
                source_table=table,
                shadow_table=shadow_table,
                columns_sql=columns_sql,
                expressions_sql=expressions_sql,
                select_parameters=select_parameters,
                row_window=row_window,
                row_parameters=row_parameters,
            )
        else:
            conn.execute(
                f"""INSERT INTO {shadow_table} ({columns_sql})
                    SELECT {expressions_sql} FROM {table}
                    WHERE {row_window} ORDER BY rowid""",
                (*select_parameters, *row_parameters),
            )
        _record_migration_marker(conn, cursor_key, str(last_rowid))
        return False, len(rows)

    replaced_table = f"{table}_tacit_replaced_v1"
    if _table_exists(conn, replaced_table):
        raise RuntimeError(f"Incomplete final legacy schema swap for {table}")
    conn.execute(f"ALTER TABLE {table} RENAME TO {replaced_table}")
    conn.execute(f"ALTER TABLE {shadow_table} RENAME TO {table}")
    conn.execute(f"DROP TABLE {replaced_table}")
    conn.execute("DELETE FROM signal_tenant_migration_metadata WHERE key=?", (cursor_key,))
    finalize(conn)
    return True, 0


def _copy_signal_mapping_window(
    conn: sqlite3.Connection,
    *,
    source_table: str,
    shadow_table: str,
    columns_sql: str,
    expressions_sql: str,
    select_parameters: tuple[Any, ...],
    row_window: str,
    row_parameters: tuple[Any, ...],
) -> None:
    """Copy one mapping page while preferring canonical bootstrap rows."""
    source_columns = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({source_table})")}
    non_bootstrap = f"{row_window} AND (source_type IS NULL OR source_type!='bootstrap')"
    conn.execute(
        f"""INSERT INTO {shadow_table} ({columns_sql})
            SELECT {expressions_sql} FROM {source_table}
            WHERE {non_bootstrap} ORDER BY rowid""",
        (*select_parameters, *row_parameters),
    )

    conflict_target = "tenant_id, signal_type, metric_pattern, governance_ref, projection_key"
    if "tenant_id" in source_columns:
        canonical_predicate = "tenant_id=?"
        canonical_parameters: tuple[Any, ...] = (GLOBAL_BOOTSTRAP_TENANT_ID,)
        noncanonical_predicate = "(tenant_id IS NULL OR tenant_id!=?)"
        noncanonical_parameters: tuple[Any, ...] = (GLOBAL_BOOTSTRAP_TENANT_ID,)
    else:
        canonical_predicate = "0"
        canonical_parameters = ()
        noncanonical_predicate = "1"
        noncanonical_parameters = ()

    update_columns = (
        "signal_type",
        "metric_pattern",
        "confidence",
        "context_services",
        "context_datasource_types",
        "context_environments",
        "context_archetypes",
        "context_regions",
        "context_clusters",
        "context_namespaces",
        "context_versions",
        "valid_from",
        "valid_until",
        "source_type",
        "source_refs",
        "governance_ref",
        "governance_revision",
        "projection_key",
        "inference_version",
        "review_state",
        "use_count",
        "positive_feedback",
        "negative_feedback",
        "created_at",
        "last_seen",
    )
    updates = ", ".join(f"{column}=excluded.{column}" for column in update_columns)
    conn.execute(
        f"""INSERT INTO {shadow_table} ({columns_sql})
            SELECT {expressions_sql} FROM {source_table}
            WHERE {row_window} AND source_type='bootstrap' AND {canonical_predicate}
            ORDER BY rowid
            ON CONFLICT({conflict_target}) DO UPDATE SET {updates}""",
        (*select_parameters, *row_parameters, *canonical_parameters),
    )
    conn.execute(
        f"""INSERT INTO {shadow_table} ({columns_sql})
            SELECT {expressions_sql} FROM {source_table}
            WHERE {row_window} AND source_type='bootstrap' AND {noncanonical_predicate}
            ORDER BY rowid
            ON CONFLICT({conflict_target}) DO NOTHING""",
        (*select_parameters, *row_parameters, *noncanonical_parameters),
    )


@contextmanager
def atomic_rebuild(conn: sqlite3.Connection, name: str) -> Iterator[None]:
    """Keep a table rename/copy/drop migration within one rollback boundary."""
    conn.execute(f"SAVEPOINT {name}")
    try:
        yield
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
        conn.execute(f"RELEASE SAVEPOINT {name}")
        raise
    else:
        conn.execute(f"RELEASE SAVEPOINT {name}")


def execute_script_statements(conn: sqlite3.Connection, script: str) -> None:
    """Execute a SQL script statement-by-statement without implicit commits."""
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if not sqlite3.complete_statement(statement):
            continue
        if statement.strip():
            conn.execute(statement)
        statement = ""
    if statement.strip():
        raise ValueError("incomplete SQL migration statement")


def _create_statement_for_table(script: str, table: str) -> str:
    statement = ""
    prefixes = (f"CREATE TABLE {table}", f"CREATE TABLE IF NOT EXISTS {table}")
    for line in script.splitlines(keepends=True):
        statement += line
        if not sqlite3.complete_statement(statement):
            continue
        stripped = statement.strip()
        if stripped.startswith(prefixes):
            return stripped
        statement = ""
    raise RuntimeError(f"No canonical schema statement exists for {table}")


def ensure_schema(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str | None = "default",
    bootstrap_signal_definitions: dict[str, dict[str, Any]] | None = None,
) -> bool:
    """Install structural schema and report whether legacy copies are complete."""
    require_bootstrap_taxonomy_for_legacy_definitions(
        conn,
        bootstrap_signal_definitions=bootstrap_signal_definitions,
    )
    require_confirmed_default_tenant_owner(conn, legacy_tenant=legacy_tenant)
    require_legacy_tenant_owner(
        conn,
        legacy_tenant=legacy_tenant,
        bootstrap_signal_definitions=bootstrap_signal_definitions,
    )
    migration_tenant = legacy_tenant or "default"
    if _table_exists(conn, "signal_tenant_migration_metadata"):
        _bind_legacy_schema_owner(
            conn,
            owner=migration_tenant,
            migration_pending=_legacy_schema_copy_pending(conn),
        )
    definition_scope_complete = _migration_marker_exists(conn, _SIGNAL_DEFINITION_SCOPE_MARKER)
    execute_script_statements(conn, SCHEMA_SQL)
    _bind_default_owner_migration(conn, owner=legacy_tenant or "*")
    if not definition_scope_complete:
        if bootstrap_signal_definitions is not None:
            migrate_legacy_signal_definitions(
                conn,
                legacy_tenant=migration_tenant,
                bootstrap_signal_definitions=bootstrap_signal_definitions,
            )
    ensure_learning_index(conn, legacy_tenant=migration_tenant)
    ensure_ingested_dashboard_columns(conn)
    ensure_ingested_dashboard_backend_scope(conn, legacy_tenant=migration_tenant)
    ensure_ingested_alert_columns(conn)
    ensure_ingested_alert_tenant_scope(conn, legacy_tenant=migration_tenant)
    ensure_artifact_learning_columns(conn)
    ensure_artifact_tenant_scope(conn, legacy_tenant=migration_tenant)
    ensure_mapping_columns(conn)
    ensure_mapping_tenant_scope(conn, legacy_tenant=migration_tenant)
    ensure_rejected_candidate_tenant_scope(conn, legacy_tenant=migration_tenant)
    ensure_global_bootstrap_mapping_scope(conn)
    migration_pending = _legacy_schema_copy_pending(conn)
    _bind_legacy_schema_owner(
        conn,
        owner=migration_tenant,
        migration_pending=migration_pending,
    )
    if migration_pending:
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (CURRENT_SIGNAL_SCHEMA_MARKER,),
        )
        return False
    ensure_governed_projection_audit_triggers(conn)
    ensure_projection_authority_page_index(conn)
    ensure_mapping_source_ref_index(conn)
    mark_governed_projection_audit_dirty(conn, reason="schema_migration")
    _record_migration_marker(conn, CURRENT_SIGNAL_SCHEMA_MARKER, migration_tenant)
    conn.execute(
        "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
        (_LEGACY_SCHEMA_OWNER_MARKER,),
    )
    return True


def signal_schema_is_current(conn: sqlite3.Connection) -> bool:
    """Verify the current-schema marker against the small physical schema surface."""
    if not _migration_marker_exists(conn, CURRENT_SIGNAL_SCHEMA_MARKER):
        return False
    if _legacy_schema_copy_pending(conn) or _migration_marker_exists(conn, _LEGACY_SCHEMA_OWNER_MARKER):
        return False
    if not _migration_marker_exists(conn, MAPPING_SOURCE_REF_INDEX_MARKER):
        return False
    required_columns = {
        "signal_metric_mappings": {
            "tenant_id",
            "governance_ref",
            "governance_revision",
            "projection_key",
            "context_regions",
            "context_clusters",
            "context_namespaces",
            "context_versions",
            "valid_from",
            "valid_until",
        },
        "ingested_dashboards": {
            "tenant_id",
            "backend_name",
            "last_seen_at",
            "knowledge_reconciled_at",
        },
        "ingested_alerts": {
            "tenant_id",
            "backend_name",
            "generation_fingerprint",
            "knowledge_reconciled_at",
        },
        "learned_artifacts": {
            "tenant_id",
            "knowledge_reconciled_at",
            "extraction_generation",
        },
        "rejected_signal_candidates": {"tenant_id"},
    }
    for table, expected in required_columns.items():
        if not _table_exists(conn, table):
            return False
        columns = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}
        if not expected.issubset(columns):
            return False
    fts_capability = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_FTS_CAPABILITY_MARKER,),
    ).fetchone()
    if fts_capability is None or str(fts_capability["value"]) not in {
        _FTS_CAPABILITY_AVAILABLE,
        _FTS_CAPABILITY_UNAVAILABLE,
    }:
        return False
    required_tables = {
        "signal_types",
        "tenant_signal_types",
        "signal_mapping_source_refs",
        "signal_tenant_migration_metadata",
    }
    if str(fts_capability["value"]) == _FTS_CAPABILITY_AVAILABLE:
        required_tables.add("learning_context_fts")
    existing_tables = {
        str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    }
    if not required_tables.issubset(existing_tables):
        return False
    required_indexes = {
        "idx_signal_types_page",
        "idx_tenant_signal_types_page",
        "idx_smm_governance",
        "idx_smm_governed_revision_audit",
        "idx_smm_ungoverned_audit",
        "idx_smm_tenant_signal_page",
        "idx_smm_active_reverse",
        "idx_signal_mapping_source_ref",
        "idx_ingested_dashboard_reconciliation",
        "idx_ingested_dashboard_stale_scan",
        "idx_ingested_dashboard_page",
        "idx_ingested_dashboard_status_page",
        "idx_ingested_dashboard_backend_page",
        "idx_ingested_dashboard_backend_status_page",
        "idx_ingested_alert_reconciliation",
        "idx_ingested_alert_stale_scan",
        "idx_ingested_alert_page",
        "idx_ingested_alert_status_page",
        "idx_ingested_alert_backend_page",
        "idx_ingested_alert_backend_status_page",
        "idx_learned_artifact_reconciliation",
        "idx_learned_artifact_stale_scan",
        "idx_learned_artifacts_page",
        "idx_evidence_requirements_artifact_page",
        "idx_ownership_hints_artifact_page",
        "idx_dependency_hints_artifact_page",
        "idx_signal_mapping_candidates_artifact_page",
    }
    if _table_exists(conn, "operational_knowledge"):
        required_indexes.add("idx_operational_knowledge_signal_projection_page")
    existing_indexes = {
        str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
    }
    if not required_indexes.issubset(existing_indexes):
        return False
    mapping_unique_indexes = {
        tuple(str(column["name"]) for column in conn.execute(f"PRAGMA index_info({index['name']})"))
        for index in conn.execute("PRAGMA index_list(signal_metric_mappings)")
        if bool(index["unique"])
    }
    if ("tenant_id", "signal_type", "metric_pattern", "governance_ref", "projection_key") not in mapping_unique_indexes:
        return False
    required_schema_objects = {
        ("index", "idx_smm_governed_revision_audit"): _GOVERNED_PROJECTION_AUDIT_INDEX_SQL,
        ("index", "idx_smm_ungoverned_audit"): _UNGOVERNED_PROJECTION_AUDIT_INDEX_SQL,
        ("index", "idx_signal_mapping_source_ref"): _SOURCE_REF_INDEX_SQL,
        **{("trigger", name): sql for name, sql in _GOVERNED_PROJECTION_TRIGGER_SQL.items()},
        **{("trigger", name): sql for name, sql in _SOURCE_REF_TRIGGER_SQL.items()},
    }
    if _table_exists(conn, "operational_knowledge"):
        required_schema_objects[("index", "idx_operational_knowledge_signal_projection_page")] = (
            _OPERATIONAL_KNOWLEDGE_SIGNAL_PROJECTION_PAGE_INDEX_SQL
        )
    return all(
        _schema_object_matches(
            conn,
            object_type=object_type,
            name=name,
            expected_sql=expected_sql,
        )
        for (object_type, name), expected_sql in required_schema_objects.items()
    )


def governed_projection_audit_is_current(conn: sqlite3.Connection) -> bool:
    if not _table_exists(conn, "signal_tenant_migration_metadata"):
        return False
    row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (GOVERNED_PROJECTION_AUDIT_MARKER,),
    ).fetchone()
    return row is not None and str(row["value"]) == "clean"


def signal_tenant_owner_is_current(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str | None,
) -> bool:
    if not _table_exists(conn, "signal_tenant_migration_metadata"):
        return False
    row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_DEFAULT_OWNER_MARKER,),
    ).fetchone()
    if row is None:
        return False
    return legacy_tenant is None or str(row["value"]) == legacy_tenant


def mark_governed_projection_audit_current(conn: sqlite3.Connection) -> None:
    _record_migration_marker(conn, GOVERNED_PROJECTION_AUDIT_MARKER, "clean")


def mark_governed_projection_audit_dirty(conn: sqlite3.Connection, *, reason: str) -> None:
    token = hashlib.sha256(f"{reason}:{time.time_ns()}".encode()).hexdigest()[:16]
    _record_migration_marker(conn, GOVERNED_PROJECTION_AUDIT_MARKER, f"dirty:{token}")


def ensure_governed_projection_audit_triggers(conn: sqlite3.Connection) -> None:
    """Install mutation guards only after legacy mapping columns are current."""
    for trigger_name in _GOVERNED_PROJECTION_TRIGGER_SQL:
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    for statement in _GOVERNED_PROJECTION_TRIGGER_SQL.values():
        conn.execute(statement)


def ensure_projection_authority_page_index(conn: sqlite3.Connection) -> None:
    """Install the exact authority-side index when the knowledge schema is present."""
    if not _table_exists(conn, "operational_knowledge"):
        return
    if _schema_object_matches(
        conn,
        object_type="index",
        name="idx_operational_knowledge_signal_projection_page",
        expected_sql=_OPERATIONAL_KNOWLEDGE_SIGNAL_PROJECTION_PAGE_INDEX_SQL,
    ):
        return
    conn.execute("DROP INDEX IF EXISTS idx_operational_knowledge_signal_projection_page")
    conn.execute(_OPERATIONAL_KNOWLEDGE_SIGNAL_PROJECTION_PAGE_INDEX_SQL)


def _migration_marker_exists(conn: sqlite3.Connection, key: str) -> bool:
    if not _table_exists(conn, "signal_tenant_migration_metadata"):
        return False
    return (
        conn.execute(
            "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
            (key,),
        ).fetchone()
        is not None
    )


def _record_migration_marker(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(key) DO UPDATE SET
               value=excluded.value,
               updated_at=excluded.updated_at""",
        (key, value, time.time()),
    )


def require_bootstrap_taxonomy_for_legacy_definitions(
    conn: sqlite3.Connection,
    *,
    bootstrap_signal_definitions: dict[str, dict[str, Any]] | None,
) -> None:
    """Refuse to classify legacy signal definitions without the product taxonomy."""
    if _migration_marker_exists(conn, _SIGNAL_DEFINITION_SCOPE_MARKER):
        return
    if _migration_marker_exists(conn, _SIGNAL_DEFINITION_BOOTSTRAP_MARKER):
        return
    if not _table_exists(conn, "signal_types"):
        return
    has_legacy_definitions = conn.execute("SELECT 1 FROM signal_types LIMIT 1").fetchone() is not None
    if not has_legacy_definitions or bootstrap_signal_definitions is not None:
        return
    logger.error(
        "legacy_signal_definition_migration_blocked",
        reason_code="bootstrap_taxonomy_unavailable",
    )
    raise RuntimeError(
        "Legacy signal definitions cannot be tenant-scoped because the bootstrap taxonomy is unavailable. "
        "Restore tacit/data/signals.yaml and retry the migration."
    )


def _table_has_default_rows(conn: sqlite3.Connection, table: str) -> bool:
    if not _table_exists(conn, table):
        return False
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if "tenant_id" not in columns:
        return False
    return conn.execute(f"SELECT 1 FROM {table} WHERE tenant_id='default' LIMIT 1").fetchone() is not None


def require_confirmed_default_tenant_owner(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str | None,
) -> None:
    """Refuse synthetic default ownership left by an earlier tenant migration."""
    if legacy_tenant is not None:
        return
    if _table_exists(conn, "signal_tenant_migration_metadata"):
        marker = conn.execute(
            "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        ).fetchone()
        if marker is not None:
            return
    ambiguous_tables = [table for table in _TENANT_OWNED_TABLES if _table_has_default_rows(conn, table)]
    if _table_exists(conn, "signal_metric_mappings"):
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
        if "tenant_id" in columns:
            source_filter = " AND source_type!='bootstrap'" if "source_type" in columns else ""
            if (
                conn.execute(
                    f"SELECT 1 FROM signal_metric_mappings WHERE tenant_id='default'{source_filter} LIMIT 1"
                ).fetchone()
                is not None
            ):
                ambiguous_tables.append("signal_metric_mappings")
    if ambiguous_tables:
        tables = sorted(set(ambiguous_tables))
        logger.error(
            "signal_default_owner_unconfirmed",
            reason_code="signal_default_owner_unconfirmed",
            tables=tables,
        )
        raise RuntimeError(
            "Previously migrated signal and knowledge data has unconfirmed default-tenant ownership. "
            "Start once with knowledge_tenant_id pinned to its owner before enabling wildcard tenancy. "
            "Affected tables: " + ", ".join(tables)
        )


def _tenant_column_exists(conn: sqlite3.Connection, table: str) -> bool:
    if not _table_exists(conn, table):
        return False
    columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return "tenant_id" in columns


def _archive_and_delete_migration_batch(
    conn: sqlite3.Connection,
    *,
    table: str,
    where: str,
    parameters: tuple[Any, ...],
    target_tenant: str,
    reason: str,
    batch_size: int,
) -> int:
    """Archive and remove one bounded rowid batch in the active transaction."""
    rows = conn.execute(
        f"SELECT rowid AS _migration_rowid, * FROM {table} WHERE {where} ORDER BY rowid LIMIT ?",
        (*parameters, batch_size),
    ).fetchall()
    if not rows:
        return 0

    quarantined_at = time.time()
    archive_rows: list[tuple[Any, ...]] = []
    rowids: list[int] = []
    for row in rows:
        payload_row = dict(row)
        rowids.append(int(payload_row.pop("_migration_rowid")))
        payload = json.dumps(payload_row, sort_keys=True, separators=(",", ":"), default=str)
        row_key = hashlib.sha256(f"{table}\0{payload}".encode()).hexdigest()
        original_tenant = str(payload_row.get("tenant_id") or "default")
        archive_rows.append((table, row_key, original_tenant, target_tenant, reason, payload, quarantined_at))
    conn.executemany(
        """INSERT OR IGNORE INTO signal_migration_quarantine
           (source_table, source_row_key, original_tenant_id, target_tenant_id,
            reason, payload_json, quarantined_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        archive_rows,
    )
    placeholders = ",".join("?" for _ in rowids)
    conn.execute(f"DELETE FROM {table} WHERE rowid IN ({placeholders})", rowids)
    return len(rowids)


def reconcile_default_tenant_owner_batch(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str | None,
    batch_size: int = 500,
) -> tuple[bool, str, int]:
    """Reconcile one restartable owner-migration batch.

    The caller commits each invocation independently. The remaining default
    rows are the durable progress record; the owner marker is written only
    after every table is complete.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    marker = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_DEFAULT_OWNER_MARKER,),
    ).fetchone()
    if marker is not None:
        recorded_owner = str(marker["value"])
        if legacy_tenant is not None and recorded_owner != legacy_tenant:
            raise RuntimeError(
                "Signal database tenant owner does not match the configured pinned tenant: "
                f"recorded={recorded_owner}, configured={legacy_tenant}"
            )
        return True, "already_complete", 0

    owner = legacy_tenant or "*"
    progress = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_DEFAULT_OWNER_PROGRESS_MARKER,),
    ).fetchone()
    if progress is not None and str(progress["value"]) != owner:
        raise RuntimeError(
            "Signal database tenant owner migration is already in progress for another tenant: "
            f"recorded={progress['value']}, configured={owner}"
        )
    if owner in {"*", "default"}:
        _record_migration_marker(conn, _DEFAULT_OWNER_MARKER, owner)
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_PROGRESS_MARKER,),
        )
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key LIKE ?",
            (f"{_DEFAULT_OWNER_CURSOR_PREFIX}%",),
        )
        return True, "marker", 0
    if progress is None:
        _record_migration_marker(conn, _DEFAULT_OWNER_PROGRESS_MARKER, owner)

    if _tenant_column_exists(conn, "signal_metric_mappings"):
        projection_conditions = ["(tenant_id='default' AND governance_ref!='')"]
        if _tenant_column_exists(conn, "operational_knowledge"):
            projection_conditions.append("""EXISTS (
                     SELECT 1 FROM operational_knowledge authority
                     WHERE authority.tenant_id='default'
                       AND authority.knowledge_id=signal_metric_mappings.governance_ref
                   )""")
        if _tenant_column_exists(conn, "operational_knowledge_revisions"):
            projection_conditions.append("""EXISTS (
                     SELECT 1 FROM operational_knowledge_revisions authority
                     WHERE authority.tenant_id='default'
                       AND authority.knowledge_id=signal_metric_mappings.governance_ref
                   )""")
        count = _archive_and_delete_migration_batch(
            conn,
            table="signal_metric_mappings",
            where=" OR ".join(projection_conditions),
            parameters=(),
            target_tenant=owner,
            reason="governed_projection_requires_rebuild",
            batch_size=batch_size,
        )
        if count:
            return False, "signal_metric_mappings:quarantine", count

    for table in reversed(_GOVERNED_TENANT_TABLES):
        if not _tenant_column_exists(conn, table):
            continue
        count = _archive_and_delete_migration_batch(
            conn,
            table=table,
            where="tenant_id='default'",
            parameters=(),
            target_tenant=owner,
            reason="tenant_identity_requires_remigration",
            batch_size=batch_size,
        )
        if count:
            return False, f"{table}:quarantine", count

    for table in _RETARGETABLE_TENANT_TABLES:
        if not _tenant_column_exists(conn, table):
            continue
        cursor_key = f"{_DEFAULT_OWNER_CURSOR_PREFIX}{table}"
        cursor_row = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (cursor_key,),
        ).fetchone()
        if cursor_row is not None and str(cursor_row["value"]) == "complete":
            continue
        after_rowid = _optional_integer_cursor(cursor_row, name=f"Default owner {table} migration")
        if after_rowid is None:
            rows = conn.execute(
                f"""SELECT rowid AS migration_rowid FROM {table}
                    WHERE tenant_id='default'
                    ORDER BY rowid LIMIT ?""",
                (batch_size,),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""SELECT rowid AS migration_rowid FROM {table}
                    WHERE rowid>? AND tenant_id='default'
                    ORDER BY rowid LIMIT ?""",
                (after_rowid, batch_size),
            ).fetchall()
        if not rows:
            _record_migration_marker(conn, cursor_key, "complete")
            continue
        last_rowid = int(rows[-1]["migration_rowid"])
        if after_rowid is None:
            cursor = conn.execute(
                f"""UPDATE {table} SET tenant_id=?
                    WHERE rowid<=? AND tenant_id='default'""",
                (owner, last_rowid),
            )
        else:
            cursor = conn.execute(
                f"""UPDATE {table} SET tenant_id=?
                    WHERE rowid>? AND rowid<=? AND tenant_id='default'""",
                (owner, after_rowid, last_rowid),
            )
        _record_migration_marker(conn, cursor_key, str(last_rowid))
        if cursor.rowcount:
            return False, f"{table}:retarget", int(cursor.rowcount)

    if _tenant_column_exists(conn, "signal_metric_mappings"):
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
        source_filter = " AND source_type!='bootstrap'" if "source_type" in columns else ""
        cursor_key = f"{_DEFAULT_OWNER_CURSOR_PREFIX}signal_metric_mappings"
        cursor_row = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (cursor_key,),
        ).fetchone()
        if cursor_row is None or str(cursor_row["value"]) != "complete":
            after_rowid = _optional_integer_cursor(cursor_row, name="Default owner signal mapping migration")
            if after_rowid is None:
                rows = conn.execute(
                    f"""SELECT rowid AS migration_rowid FROM signal_metric_mappings
                        WHERE tenant_id='default'{source_filter}
                        ORDER BY rowid LIMIT ?""",
                    (batch_size,),
                ).fetchall()
            else:
                rows = conn.execute(
                    f"""SELECT rowid AS migration_rowid FROM signal_metric_mappings
                        WHERE rowid>? AND tenant_id='default'{source_filter}
                        ORDER BY rowid LIMIT ?""",
                    (after_rowid, batch_size),
                ).fetchall()
            if rows:
                last_rowid = int(rows[-1]["migration_rowid"])
                if after_rowid is None:
                    cursor = conn.execute(
                        f"""UPDATE signal_metric_mappings SET tenant_id=?
                            WHERE rowid<=? AND tenant_id='default'{source_filter}""",
                        (owner, last_rowid),
                    )
                else:
                    cursor = conn.execute(
                        f"""UPDATE signal_metric_mappings SET tenant_id=?
                            WHERE rowid>? AND rowid<=? AND tenant_id='default'{source_filter}""",
                        (owner, after_rowid, last_rowid),
                    )
                _record_migration_marker(conn, cursor_key, str(last_rowid))
                if cursor.rowcount:
                    return False, "signal_metric_mappings:retarget", int(cursor.rowcount)
            else:
                _record_migration_marker(conn, cursor_key, "complete")

    for table in _RETARGETABLE_TENANT_TABLES:
        if not _tenant_column_exists(conn, table):
            continue
        remaining = conn.execute(f"SELECT MIN(rowid) AS first_rowid FROM {table} WHERE tenant_id='default'").fetchone()
        if remaining is not None and remaining["first_rowid"] is not None:
            conn.execute(
                "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
                (f"{_DEFAULT_OWNER_CURSOR_PREFIX}{table}",),
            )
            return False, f"{table}:rescan", 0

    if _tenant_column_exists(conn, "signal_metric_mappings"):
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
        source_filter = " AND source_type!='bootstrap'" if "source_type" in columns else ""
        remaining = conn.execute(f"""SELECT MIN(rowid) AS first_rowid FROM signal_metric_mappings
                WHERE tenant_id='default'{source_filter}""").fetchone()
        if remaining is not None and remaining["first_rowid"] is not None:
            conn.execute(
                "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
                (f"{_DEFAULT_OWNER_CURSOR_PREFIX}signal_metric_mappings",),
            )
            return False, "signal_metric_mappings:rescan", 0

    _record_migration_marker(conn, _DEFAULT_OWNER_MARKER, owner)
    conn.execute(
        "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
        (_DEFAULT_OWNER_PROGRESS_MARKER,),
    )
    conn.execute(
        "DELETE FROM signal_tenant_migration_metadata WHERE key LIKE ?",
        (f"{_DEFAULT_OWNER_CURSOR_PREFIX}%",),
    )
    return True, "marker", 0


def require_legacy_tenant_owner(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str | None,
    bootstrap_signal_definitions: dict[str, dict[str, Any]] | None,
) -> None:
    """Refuse to guess ownership for pre-tenant data in wildcard deployments."""
    if legacy_tenant is not None:
        return
    tenant_owned_tables: list[str] = []
    for table in (
        "ingested_dashboards",
        "ingested_alerts",
        "learned_artifacts",
        "evidence_requirements",
        "ownership_hints",
        "dependency_hints",
        "signal_mapping_candidates",
        "learning_context_fts",
        "rejected_signal_candidates",
    ):
        if not _table_exists(conn, table):
            continue
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if "tenant_id" not in columns and conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
            tenant_owned_tables.append(table)

    if _table_exists(conn, "signal_metric_mappings"):
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
        if "tenant_id" not in columns:
            where = "WHERE source_type != 'bootstrap'" if "source_type" in columns else ""
            if conn.execute(f"SELECT 1 FROM signal_metric_mappings {where} LIMIT 1").fetchone() is not None:
                tenant_owned_tables.append("signal_metric_mappings")

    if _table_exists(conn, "signal_types") and not _table_exists(conn, "tenant_signal_types"):
        rows = conn.execute("SELECT * FROM signal_types").fetchall()
        if rows and bootstrap_signal_definitions is None:
            tenant_owned_tables.append("signal_types")
        elif bootstrap_signal_definitions is not None:
            for row in rows:
                expected = bootstrap_signal_definitions.get(str(row["signal_type"]))
                if expected is None or any(
                    str(row[field]) != str(expected.get(field) or "") for field in ("description", "category", "unit")
                ):
                    tenant_owned_tables.append("signal_types")
                    break

    if tenant_owned_tables:
        tables = ", ".join(sorted(set(tenant_owned_tables)))
        logger.error(
            "legacy_tenant_owner_required",
            reason_code="legacy_tenant_owner_required",
            tables=sorted(set(tenant_owned_tables)),
        )
        raise RuntimeError(
            "Legacy signal data has no tenant owner. Start once with knowledge_tenant_id pinned to its owner "
            f"before enabling wildcard tenancy. Affected tables: {tables}"
        )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is not None
    )


def _tenant_sql_literal(tenant_id: str) -> str:
    """Return a safely quoted literal for DDL-adjacent INSERT...SELECT migrations."""
    return "'" + tenant_id.replace("'", "''") + "'"


def migrate_legacy_signal_definitions(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str,
    bootstrap_signal_definitions: dict[str, dict[str, Any]],
) -> None:
    """Prepare a bounded text-keyed migration of legacy signal definitions."""
    normalized_bootstrap = {
        str(signal_type): {
            "description": str(definition.get("description") or ""),
            "category": str(definition.get("category") or ""),
            "unit": str(definition.get("unit") or ""),
        }
        for signal_type, definition in bootstrap_signal_definitions.items()
    }
    encoded_bootstrap = json.dumps(normalized_bootstrap, sort_keys=True, separators=(",", ":"))
    staged = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_SIGNAL_DEFINITION_BOOTSTRAP_MARKER,),
    ).fetchone()
    if staged is not None and str(staged["value"]) != encoded_bootstrap:
        raise RuntimeError("Signal definition taxonomy changed during a legacy migration")
    if conn.execute("SELECT 1 FROM signal_types LIMIT 1").fetchone() is None:
        _record_migration_marker(conn, _SIGNAL_DEFINITION_SCOPE_MARKER, legacy_tenant)
        return
    if staged is None:
        _record_migration_marker(conn, _SIGNAL_DEFINITION_BOOTSTRAP_MARKER, encoded_bootstrap)
    _prepare_legacy_schema_copy(
        conn,
        table="signal_types",
        create_target=_create_statement_for_table(SCHEMA_SQL, "signal_types"),
    )


def _copy_legacy_signal_definitions_batch(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str,
    batch_size: int,
) -> tuple[bool, int]:
    """Copy or finalize one text-keyed legacy signal-definition page."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    shadow_table = _legacy_schema_table("signal_types")
    if not _table_exists(conn, shadow_table):
        return True, 0
    bootstrap_row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_SIGNAL_DEFINITION_BOOTSTRAP_MARKER,),
    ).fetchone()
    if bootstrap_row is None:
        raise RuntimeError("Signal definition migration taxonomy is unavailable")
    try:
        bootstrap_definitions = json.loads(str(bootstrap_row["value"]))
    except json.JSONDecodeError as exc:
        raise RuntimeError("Signal definition migration taxonomy is invalid") from exc
    if not isinstance(bootstrap_definitions, dict) or any(
        not isinstance(signal_type, str) or not isinstance(definition, dict)
        for signal_type, definition in bootstrap_definitions.items()
    ):
        raise RuntimeError("Signal definition migration taxonomy is invalid")

    cursor_key = f"{_LEGACY_SCHEMA_COPY_CURSOR_PREFIX}signal_types"
    cursor_row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (cursor_key,),
    ).fetchone()
    after_signal: str | None = None
    if cursor_row is not None:
        try:
            decoded_cursor = json.loads(str(cursor_row["value"]))
        except json.JSONDecodeError as exc:
            raise RuntimeError("Signal definition migration cursor is invalid") from exc
        if not isinstance(decoded_cursor, str):
            raise RuntimeError("Signal definition migration cursor is invalid")
        after_signal = decoded_cursor
    if after_signal is None:
        rows = conn.execute("SELECT * FROM signal_types ORDER BY signal_type LIMIT ?", (batch_size,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM signal_types WHERE signal_type>? ORDER BY signal_type LIMIT ?",
            (after_signal, batch_size),
        ).fetchall()
    if rows:
        for row in rows:
            if row["signal_type"] is None:
                raise RuntimeError("Legacy signal definition identity is invalid")
            signal_type = str(row["signal_type"])
            bootstrap = bootstrap_definitions.get(signal_type)
            expected = {
                "description": str((bootstrap or {}).get("description") or ""),
                "category": str((bootstrap or {}).get("category") or ""),
                "unit": str((bootstrap or {}).get("unit") or ""),
            }
            tenant_override = bootstrap is None or any(str(row[field]) != value for field, value in expected.items())
            if tenant_override:
                conn.execute(
                    """INSERT OR IGNORE INTO tenant_signal_types
                       (tenant_id, signal_type, description, category, unit, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        legacy_tenant,
                        signal_type,
                        row["description"],
                        row["category"],
                        row["unit"],
                        row["created_at"],
                        row["updated_at"],
                    ),
                )
            if bootstrap is not None:
                conn.execute(
                    f"""INSERT INTO {shadow_table}
                       (signal_type, description, category, unit, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        signal_type,
                        expected["description"],
                        expected["category"],
                        expected["unit"],
                        row["created_at"],
                        row["updated_at"],
                    ),
                )
        _record_migration_marker(conn, cursor_key, json.dumps(str(rows[-1]["signal_type"])))
        return False, len(rows)

    replaced_table = "signal_types_tacit_replaced_v1"
    if _table_exists(conn, replaced_table):
        raise RuntimeError("Incomplete final legacy schema swap for signal_types")
    conn.execute(f"ALTER TABLE signal_types RENAME TO {replaced_table}")
    conn.execute(f"ALTER TABLE {shadow_table} RENAME TO signal_types")
    conn.execute(f"DROP TABLE {replaced_table}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_types_page ON signal_types(category, signal_type)")
    _record_migration_marker(conn, _SIGNAL_DEFINITION_SCOPE_MARKER, legacy_tenant)
    conn.execute(
        "DELETE FROM signal_tenant_migration_metadata WHERE key IN (?, ?)",
        (
            cursor_key,
            _SIGNAL_DEFINITION_BOOTSTRAP_MARKER,
        ),
    )
    return True, 0


def ensure_learning_index(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Create the FTS5 operational knowledge index when available."""
    capability = None
    if _table_exists(conn, "signal_tenant_migration_metadata"):
        capability = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (_FTS_CAPABILITY_MARKER,),
        ).fetchone()
    if capability is not None and str(capability["value"]) == _FTS_CAPABILITY_UNAVAILABLE:
        return
    try:
        conn.execute(FTS_SCHEMA_SQL)
        if _table_exists(conn, "signal_tenant_migration_metadata"):
            _record_migration_marker(conn, _FTS_CAPABILITY_MARKER, _FTS_CAPABILITY_AVAILABLE)
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(learning_context_fts)").fetchall()}
        if columns and "tenant_id" not in columns:
            prepare_learning_index_rebuild(conn)
    except sqlite3.OperationalError as exc:
        if "no such module:" not in str(exc).casefold():
            raise
        if _table_exists(conn, "signal_tenant_migration_metadata"):
            _record_migration_marker(conn, _FTS_CAPABILITY_MARKER, _FTS_CAPABILITY_UNAVAILABLE)
        logger.warning(
            "learning_context_fts_unavailable",
            reason_code="fts_module_unavailable",
            exception_class=type(exc).__name__[:64],
        )


def prepare_learning_index_rebuild(conn: sqlite3.Connection) -> None:
    """Prepare a restartable learning-index copy without moving rows."""
    _prepare_legacy_schema_copy(
        conn,
        table="learning_context_fts",
        create_target=FTS_SCHEMA_SQL,
    )


def ensure_mapping_columns(conn: sqlite3.Connection) -> None:
    """Add newer columns to signal_metric_mappings on pre-existing DBs."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
    if "inference_version" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN inference_version TEXT NOT NULL DEFAULT ''")
    if "review_state" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN review_state TEXT NOT NULL DEFAULT 'trusted'")
    if "governance_revision" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN governance_revision INTEGER NOT NULL DEFAULT 0")
    if "projection_key" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN projection_key TEXT NOT NULL DEFAULT ''")
    for column in ("context_regions", "context_clusters", "context_namespaces", "context_versions"):
        if column not in columns:
            conn.execute(f"ALTER TABLE signal_metric_mappings ADD COLUMN {column} TEXT NOT NULL DEFAULT '[]'")
    for column in ("valid_from", "valid_until"):
        if column not in columns:
            conn.execute(f"ALTER TABLE signal_metric_mappings ADD COLUMN {column} REAL")


def ensure_rejected_candidate_tenant_scope(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str = "default",
) -> None:
    """Keep negative signal-training records within their tenant boundary."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(rejected_signal_candidates)").fetchall()}
    if "tenant_id" not in columns:
        conn.execute("ALTER TABLE rejected_signal_candidates ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_rejected_signal_tenant_created
           ON rejected_signal_candidates(tenant_id, created_at)""")


def ensure_mapping_tenant_scope(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Ensure mappings are tenant-isolated and governed scopes remain independent."""
    unique_indexes = [
        [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
        for index in conn.execute("PRAGMA index_list(signal_metric_mappings)").fetchall()
        if index["unique"]
    ]
    has_legacy_foreign_key = bool(conn.execute("PRAGMA foreign_key_list(signal_metric_mappings)").fetchall())
    if (
        ["tenant_id", "signal_type", "metric_pattern", "governance_ref", "projection_key"] in unique_indexes
        and ["tenant_id", "signal_type", "metric_pattern", "governance_ref"] not in unique_indexes
        and ["tenant_id", "signal_type", "metric_pattern"] not in unique_indexes
        and ["signal_type", "metric_pattern"] not in unique_indexes
        and not has_legacy_foreign_key
    ):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_tenant_signal
               ON signal_metric_mappings(tenant_id, signal_type)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_tenant_signal_page
               ON signal_metric_mappings(tenant_id, signal_type, confidence DESC, id DESC)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_governance
               ON signal_metric_mappings(tenant_id, governance_ref) WHERE governance_ref != ''""")
        _ensure_governed_projection_audit_index(conn)
        _ensure_ungoverned_projection_audit_index(conn)
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_active_reverse
               ON signal_metric_mappings(tenant_id, id)
               WHERE review_state IN ('approved', 'trusted')""")
        return
    _prepare_legacy_schema_copy(
        conn,
        table="signal_metric_mappings",
        create_target="""CREATE TABLE signal_metric_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL DEFAULT 'default',
            signal_type TEXT NOT NULL, metric_pattern TEXT NOT NULL,
            confidence REAL NOT NULL DEFAULT 0.5,
            context_services TEXT NOT NULL DEFAULT '[]',
            context_datasource_types TEXT NOT NULL DEFAULT '[]',
            context_environments TEXT NOT NULL DEFAULT '[]',
            context_archetypes TEXT NOT NULL DEFAULT '[]',
            context_regions TEXT NOT NULL DEFAULT '[]',
            context_clusters TEXT NOT NULL DEFAULT '[]',
            context_namespaces TEXT NOT NULL DEFAULT '[]',
            context_versions TEXT NOT NULL DEFAULT '[]',
            valid_from REAL, valid_until REAL,
            source_type TEXT NOT NULL DEFAULT 'bootstrap', source_refs TEXT NOT NULL DEFAULT '[]',
            governance_ref TEXT NOT NULL DEFAULT '',
            governance_revision INTEGER NOT NULL DEFAULT 0,
            projection_key TEXT NOT NULL DEFAULT '',
            inference_version TEXT NOT NULL DEFAULT '', review_state TEXT NOT NULL DEFAULT 'trusted',
            use_count INTEGER NOT NULL DEFAULT 0, positive_feedback INTEGER NOT NULL DEFAULT 0,
            negative_feedback INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, last_seen REAL NOT NULL,
            UNIQUE(tenant_id, signal_type, metric_pattern, governance_ref, projection_key)
        );""",
    )


def _ensure_signal_mapping_indexes(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_smm_signal ON signal_metric_mappings(signal_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_smm_metric ON signal_metric_mappings(metric_pattern)")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_tenant_signal
           ON signal_metric_mappings(tenant_id, signal_type)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_tenant_signal_page
           ON signal_metric_mappings(tenant_id, signal_type, confidence DESC, id DESC)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_governance
           ON signal_metric_mappings(tenant_id, governance_ref) WHERE governance_ref != ''""")
    _ensure_governed_projection_audit_index(conn)
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_active_reverse
           ON signal_metric_mappings(tenant_id, id)
           WHERE review_state IN ('approved', 'trusted')""")
    _ensure_ungoverned_projection_audit_index(conn)


def _ensure_governed_projection_audit_index(conn: sqlite3.Connection) -> None:
    """Install the exact covering key index used by projection validation."""
    if _schema_object_matches(
        conn,
        object_type="index",
        name="idx_smm_governed_revision_audit",
        expected_sql=_GOVERNED_PROJECTION_AUDIT_INDEX_SQL,
    ):
        return
    conn.execute("DROP INDEX IF EXISTS idx_smm_governed_revision_audit")
    conn.execute(_GOVERNED_PROJECTION_AUDIT_INDEX_SQL)


def _ensure_ungoverned_projection_audit_index(conn: sqlite3.Connection) -> None:
    if _schema_object_matches(
        conn,
        object_type="index",
        name="idx_smm_ungoverned_audit",
        expected_sql=_UNGOVERNED_PROJECTION_AUDIT_INDEX_SQL,
    ):
        return
    conn.execute("DROP INDEX IF EXISTS idx_smm_ungoverned_audit")
    conn.execute(_UNGOVERNED_PROJECTION_AUDIT_INDEX_SQL)


def ensure_mapping_source_ref_index(conn: sqlite3.Connection) -> None:
    """Install the source-ref projection schema without a monolithic backfill."""
    projection_objects_current = (
        _table_exists(conn, "signal_mapping_source_refs")
        and _schema_object_matches(
            conn,
            object_type="index",
            name="idx_signal_mapping_source_ref",
            expected_sql=_SOURCE_REF_INDEX_SQL,
        )
        and all(
            _schema_object_matches(
                conn,
                object_type="trigger",
                name=name,
                expected_sql=statement,
            )
            for name, statement in _SOURCE_REF_TRIGGER_SQL.items()
        )
    )
    if not projection_objects_current and _table_exists(conn, "signal_tenant_migration_metadata"):
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key IN (?, ?, ?)",
            (
                MAPPING_SOURCE_REF_INDEX_MARKER,
                _MAPPING_SOURCE_REF_CURSOR_MARKER,
                _MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER,
            ),
        )
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_mapping_source_refs (
               mapping_id INTEGER NOT NULL,
               tenant_id TEXT NOT NULL,
               source_ref TEXT NOT NULL,
               PRIMARY KEY (mapping_id, source_ref)
           )""")
    if not _schema_object_matches(
        conn,
        object_type="index",
        name="idx_signal_mapping_source_ref",
        expected_sql=_SOURCE_REF_INDEX_SQL,
    ):
        conn.execute("DROP INDEX IF EXISTS idx_signal_mapping_source_ref")
        conn.execute(_SOURCE_REF_INDEX_SQL)
    for name, statement in _SOURCE_REF_TRIGGER_SQL.items():
        if not _schema_object_matches(
            conn,
            object_type="trigger",
            name=name,
            expected_sql=statement,
        ):
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")
            conn.execute(statement)


def reconcile_mapping_source_ref_index_batch(
    conn: sqlite3.Connection,
    *,
    batch_size: int = 500,
) -> tuple[bool, int]:
    """Rebuild one restartable keyset page of mapping provenance."""
    if batch_size < 1:
        raise ValueError("source-ref migration batch size must be positive")
    if _migration_marker_exists(conn, MAPPING_SOURCE_REF_INDEX_MARKER):
        return True, 0
    orphan_cursor_row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER,),
    ).fetchone()
    orphan_cursor_value = str(orphan_cursor_row["value"]) if orphan_cursor_row is not None else ""
    if orphan_cursor_value != "complete":
        after_rowid = _optional_integer_cursor(
            orphan_cursor_row,
            name="Signal mapping source-ref orphan migration",
        )
        orphan_cursor_clause = "" if after_rowid is None else "WHERE child.rowid>?"
        orphan_params: tuple[object, ...] = (batch_size,) if after_rowid is None else (after_rowid, batch_size)
        orphan_rows = conn.execute(
            f"""SELECT child.rowid AS projection_rowid,
                       CASE WHEN parent.id IS NULL THEN 1 ELSE 0 END AS orphaned
                FROM signal_mapping_source_refs child
                LEFT JOIN signal_metric_mappings parent ON parent.id=child.mapping_id
                {orphan_cursor_clause}
                ORDER BY child.rowid LIMIT ?""",
            orphan_params,
        ).fetchall()
        if orphan_rows:
            conn.executemany(
                "DELETE FROM signal_mapping_source_refs WHERE rowid=?",
                [(int(row["projection_rowid"]),) for row in orphan_rows if bool(row["orphaned"])],
            )
            _record_migration_marker(
                conn,
                _MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER,
                str(orphan_rows[-1]["projection_rowid"]),
            )
            return False, 0
        _record_migration_marker(conn, _MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER, "complete")

    cursor_row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_MAPPING_SOURCE_REF_CURSOR_MARKER,),
    ).fetchone()
    after_id = _optional_integer_cursor(cursor_row, name="Signal mapping source-ref migration")
    cursor_clause = "" if after_id is None else "WHERE id>?"
    params: tuple[object, ...] = (batch_size,) if after_id is None else (after_id, batch_size)
    rows = conn.execute(
        f"""SELECT id, tenant_id,
                   length(CAST(source_refs AS BLOB)) AS source_ref_bytes
            FROM signal_metric_mappings {cursor_clause}
            ORDER BY id LIMIT ?""",
        params,
    ).fetchall()
    if not rows:
        _record_migration_marker(conn, MAPPING_SOURCE_REF_INDEX_MARKER, "complete")
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key IN (?, ?)",
            (_MAPPING_SOURCE_REF_CURSOR_MARKER, _MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER),
        )
        return True, 0

    selected_rows: list[tuple[sqlite3.Row, list[str], list[str]]] = []
    selected_child_count = 0
    selected_payload_bytes = 0
    for row in rows:
        mapping_id = int(row["id"])
        payload_bytes = int(row["source_ref_bytes"] or 0)
        if payload_bytes > SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES:
            logger.error(
                "signal_mapping_source_ref_migration_failed",
                reason_code="signal_mapping_source_refs_limit_exceeded",
                mapping_ref_fingerprint=_diagnostic_fingerprint(f"{row['tenant_id']}\0{mapping_id}"),
                source_ref_bytes=min(payload_bytes, SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES + 1),
            )
            raise RuntimeError("signal mapping source refs exceed the storage limit")
        if selected_rows and selected_payload_bytes + payload_bytes > SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES:
            break
        payload_row = conn.execute(
            """SELECT source_refs,
                      CASE
                        WHEN json_valid(source_refs)=0 THEN NULL
                        WHEN json_type(source_refs)!='array' THEN NULL
                        ELSE json_array_length(source_refs)
                      END AS source_ref_count
               FROM signal_metric_mappings WHERE id=?""",
            (mapping_id,),
        ).fetchone()
        if payload_row is None:
            raise RuntimeError("signal mapping changed during source-ref migration")
        raw_source_ref_count = payload_row["source_ref_count"]
        if raw_source_ref_count is not None:
            source_ref_count = int(raw_source_ref_count)
            if source_ref_count > SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT:
                logger.error(
                    "signal_mapping_source_ref_migration_failed",
                    reason_code="signal_mapping_source_refs_limit_exceeded",
                    mapping_ref_fingerprint=_diagnostic_fingerprint(f"{row['tenant_id']}\0{mapping_id}"),
                    source_ref_count=SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT + 1,
                    source_ref_bytes=min(payload_bytes, SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES + 1),
                )
                raise RuntimeError("signal mapping source refs exceed the storage limit")
            if selected_rows and selected_child_count + source_ref_count > SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN:
                break
        try:
            source_refs = json.loads(str(payload_row["source_refs"] or ""))
        except (TypeError, json.JSONDecodeError) as exc:
            logger.error(
                "signal_mapping_source_ref_migration_failed",
                reason_code="signal_mapping_source_refs_malformed",
                exception_class=type(exc).__name__[:64],
                mapping_ref_fingerprint=_diagnostic_fingerprint(f"{row['tenant_id']}\0{mapping_id}"),
                error_fingerprint=_diagnostic_fingerprint(exc),
            )
            raise RuntimeError("signal mapping source refs are malformed") from None
        if not isinstance(source_refs, list) or any(not isinstance(value, str) for value in source_refs):
            validation_error = TypeError("source refs must be a JSON string array")
            logger.error(
                "signal_mapping_source_ref_migration_failed",
                reason_code="signal_mapping_source_refs_malformed",
                exception_class=type(validation_error).__name__[:64],
                mapping_ref_fingerprint=_diagnostic_fingerprint(f"{row['tenant_id']}\0{mapping_id}"),
                error_fingerprint=_diagnostic_fingerprint(validation_error),
            )
            raise RuntimeError("signal mapping source refs are malformed") from None
        normalized_refs = sorted({value.strip() for value in source_refs if value.strip()})
        selected_rows.append((row, source_refs, normalized_refs))
        selected_child_count += len(source_refs)
        selected_payload_bytes += payload_bytes

    source_ref_count = 0
    for row, source_refs, normalized_refs in selected_rows:
        mapping_id = int(row["id"])
        if source_refs != normalized_refs:
            conn.execute(
                "UPDATE signal_metric_mappings SET source_refs=? WHERE id=?",
                (json.dumps(normalized_refs), mapping_id),
            )
        conn.execute("DELETE FROM signal_mapping_source_refs WHERE mapping_id=?", (mapping_id,))
        conn.executemany(
            """INSERT INTO signal_mapping_source_refs (mapping_id, tenant_id, source_ref)
               VALUES (?, ?, ?)""",
            [(mapping_id, str(row["tenant_id"]), source_ref) for source_ref in normalized_refs],
        )
        source_ref_count += len(normalized_refs)

    _record_migration_marker(conn, _MAPPING_SOURCE_REF_CURSOR_MARKER, str(selected_rows[-1][0]["id"]))
    complete = len(rows) < batch_size and len(selected_rows) == len(rows)
    if complete:
        _record_migration_marker(conn, MAPPING_SOURCE_REF_INDEX_MARKER, "complete")
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key IN (?, ?)",
            (_MAPPING_SOURCE_REF_CURSOR_MARKER, _MAPPING_SOURCE_REF_ORPHAN_CURSOR_MARKER),
        )
    return complete, source_ref_count


def ensure_global_bootstrap_mapping_scope(conn: sqlite3.Connection) -> None:
    """Prepare a shadow copy when bootstrap rows need global normalization."""
    if _table_exists(conn, _legacy_schema_table("signal_metric_mappings")):
        return
    pending = conn.execute(
        """SELECT 1 FROM signal_metric_mappings
           WHERE tenant_id != ? AND source_type='bootstrap' LIMIT 1""",
        (GLOBAL_BOOTSTRAP_TENANT_ID,),
    ).fetchone()
    if pending is not None:
        _prepare_legacy_schema_copy(
            conn,
            table="signal_metric_mappings",
            create_target=_create_statement_for_table(SCHEMA_SQL, "signal_metric_mappings"),
        )


def _json_values(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value or "[]")
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    return sorted({str(item).strip() for item in value if str(item).strip()})


def _scope_values(scope: dict[str, Any], field: str, prefix: str) -> list[str]:
    values = []
    for value in _json_values(scope.get(field, [])):
        values.append(value.removeprefix(prefix))
    return sorted(set(values))


def _timestamp(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()


def _same_optional_float(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    return abs(float(left) - float(right)) <= 1e-9


def projection_matches_authority(mapping: Any, authority: Any | None) -> tuple[bool, str]:
    """Validate one resolver projection against an already-loaded authority row."""
    governance_revision = int(mapping["governance_revision"] or 0)
    if governance_revision < 1:
        return False, "unversioned_projection"
    if authority is None:
        return False, "authority_revision_missing"
    if (
        str(authority["review_state"]) not in {"approved", "trusted"}
        or str(authority["lifecycle_status"]) != "active"
        or str(authority["eligibility"]) == "ineligible"
    ):
        return False, "authority_revision_inactive"
    content = authority.get("content") if hasattr(authority, "get") else None
    if content is None:
        try:
            content = json.loads(authority["content_json"])
        except (TypeError, json.JSONDecodeError):
            return False, "authority_revision_invalid"
    if not isinstance(content, dict):
        return False, "authority_revision_invalid"
    state = content.get("state", {})
    if any(
        str(state.get(field) or "") != str(authority[column])
        for field, column in (
            ("review_state", "review_state"),
            ("lifecycle_status", "lifecycle_status"),
            ("eligibility", "eligibility"),
        )
    ):
        return False, "authority_state_mismatch"

    proposition = content.get("proposition", {})
    if proposition.get("kind") != "signal_mapping":
        return False, "authority_kind_mismatch"
    expected_signal = str(proposition.get("concept_ref") or "").removeprefix("signal:")
    if expected_signal != str(mapping["signal_type"]):
        return False, "authority_signal_mismatch"

    resolver_mappings = content.get("resolver_payload", {}).get("mappings", [])
    if not isinstance(resolver_mappings, list):
        return False, "authority_resolver_payload_missing"
    projected_datasource_types = normalize_datasource_types(_json_values(mapping["context_datasource_types"]))
    if str(mapping["projection_key"] or "") != resolver_projection_key(projected_datasource_types):
        return False, "authority_projection_key_mismatch"
    exact_mappings = [
        item
        for item in resolver_mappings
        if isinstance(item, dict)
        and str(item.get("metric_pattern") or "") == str(mapping["metric_pattern"])
        and normalize_datasource_types(item.get("context_datasource_types", [])) == projected_datasource_types
    ]
    if not exact_mappings:
        return False, "authority_metric_mismatch"
    exact_mapping = exact_mappings[0]
    expected_confidence = max(normalize_mapping_confidence(item.get("confidence", 0.5)) for item in exact_mappings)
    if abs(float(mapping["confidence"]) - expected_confidence) > 1e-9:
        return False, "authority_confidence_mismatch"
    if str(mapping["source_type"]) != "operational_knowledge":
        return False, "authority_source_type_mismatch"
    if str(mapping["review_state"]) in {"approved", "trusted"} and str(mapping["review_state"]) != str(
        authority["review_state"]
    ):
        return False, "authority_review_state_mismatch"

    scope = content.get("scope", {})
    expected_scope = {
        "context_services": _scope_values(scope, "service_refs", "entity:service:"),
        "context_environments": _scope_values(scope, "environment_refs", "environment:"),
        "context_archetypes": _scope_values(scope, "archetype_refs", "archetype:"),
        "context_regions": _scope_values(scope, "region_refs", "region:"),
        "context_clusters": _scope_values(scope, "cluster_refs", "cluster:"),
        "context_namespaces": _scope_values(scope, "namespace_refs", "namespace:"),
        "context_versions": _scope_values(scope, "version_constraints", "version:"),
        "context_datasource_types": list(normalize_datasource_types(exact_mapping.get("context_datasource_types", []))),
    }
    for field, expected_values in expected_scope.items():
        if _json_values(mapping[field]) != expected_values:
            return False, f"authority_{field}_mismatch"

    try:
        expected_valid_from = _timestamp(scope.get("valid_from"))
        expected_valid_until = _timestamp(scope.get("valid_until"))
    except (TypeError, ValueError):
        return False, "authority_validity_invalid"
    if not _same_optional_float(mapping["valid_from"], expected_valid_from):
        return False, "authority_valid_from_mismatch"
    if not _same_optional_float(mapping["valid_until"], expected_valid_until):
        return False, "authority_valid_until_mismatch"
    expected_inference_version = f"{content.get('policy_id', '')}:{content.get('policy_version', '')}"
    if str(mapping["inference_version"] or "") != expected_inference_version:
        return False, "authority_policy_mismatch"
    return True, "validated"


def ensure_ingested_dashboard_backend_scope(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str = "default",
) -> None:
    """Ensure ingested dashboard uniqueness includes tenant and backend identity."""
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()]
    if "backend_name" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN backend_name TEXT NOT NULL DEFAULT ''")
        columns.append("backend_name")

    for index in conn.execute("PRAGMA index_list(ingested_dashboards)").fetchall():
        if not index["unique"]:
            continue
        indexed_cols = [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
        if indexed_cols == ["tenant_id", "dashboard_uid", "backend_name"]:
            _ensure_ingested_dashboard_indexes(conn)
            return
    rebuild_ingested_dashboards_table(conn, legacy_tenant=legacy_tenant)


def ensure_ingested_dashboard_columns(conn: sqlite3.Connection) -> None:
    """Add source-lifecycle fields to dashboard records."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()}
    needs_last_seen_backfill = "last_seen_at" not in columns
    if "stale" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN stale INTEGER NOT NULL DEFAULT 0")
        columns.add("stale")
    if "missing_since" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN missing_since REAL")
        columns.add("missing_since")
    if "knowledge_reconciled_at" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN knowledge_reconciled_at REAL")
        columns.add("knowledge_reconciled_at")
    if "last_seen_at" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN last_seen_at REAL NOT NULL DEFAULT 0")
        columns.add("last_seen_at")
    if not needs_last_seen_backfill:
        needs_last_seen_backfill = (
            conn.execute("SELECT 1 FROM ingested_dashboards WHERE last_seen_at=0 LIMIT 1").fetchone() is not None
        )
    if needs_last_seen_backfill:
        rebuild_ingested_dashboards_table(conn)
    if "tenant_id" in columns:
        _ensure_ingested_dashboard_indexes(conn)


def ensure_ingested_alert_columns(conn: sqlite3.Connection) -> None:
    """Add alert-ingestion metadata columns on pre-existing DBs."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_alerts)").fetchall()}
    if not columns:
        return

    additions = {
        "source_vendor": "TEXT NOT NULL DEFAULT ''",
        "source_instance": "TEXT NOT NULL DEFAULT ''",
        "external_id": "TEXT NOT NULL DEFAULT ''",
        "fingerprint": "TEXT NOT NULL DEFAULT ''",
        "generation_fingerprint": "TEXT NOT NULL DEFAULT ''",
        "provenance_url": "TEXT NOT NULL DEFAULT ''",
        "confidence": "REAL NOT NULL DEFAULT 0.0",
        "stale": "INTEGER NOT NULL DEFAULT 0",
        "missing_since": "REAL",
        "knowledge_reconciled_at": "REAL",
        "first_seen_at": "REAL NOT NULL DEFAULT 0",
        "last_seen_at": "REAL NOT NULL DEFAULT 0",
        "updated_at": "REAL NOT NULL DEFAULT 0",
    }
    for name, ddl in additions.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE ingested_alerts ADD COLUMN {name} {ddl}")
    if "tenant_id" in columns:
        _ensure_ingested_alert_indexes(conn)


def ensure_ingested_alert_tenant_scope(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str = "default",
) -> None:
    """Ensure ingested alert uniqueness includes tenant and backend identity."""
    for index in conn.execute("PRAGMA index_list(ingested_alerts)").fetchall():
        if not index["unique"]:
            continue
        indexed_cols = [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
        if indexed_cols == ["tenant_id", "alert_uid", "backend_name"]:
            _ensure_ingested_alert_indexes(conn)
            return
    rebuild_ingested_alerts_table(conn, legacy_tenant=legacy_tenant)


def ensure_artifact_learning_columns(conn: sqlite3.Connection) -> None:
    """Add artifact-learning metadata columns on pre-existing DBs."""
    artifact_columns = {row["name"] for row in conn.execute("PRAGMA table_info(learned_artifacts)").fetchall()}
    if artifact_columns:
        additions = {
            "source_vendor": "TEXT NOT NULL DEFAULT ''",
            "source_instance": "TEXT NOT NULL DEFAULT ''",
            "provenance_url": "TEXT NOT NULL DEFAULT ''",
            "stale": "INTEGER NOT NULL DEFAULT 0",
            "missing_since": "REAL",
            "knowledge_reconciled_at": "REAL",
            "extraction_generation": "TEXT NOT NULL DEFAULT ''",
            "first_seen_at": "REAL NOT NULL DEFAULT 0",
            "last_seen_at": "REAL NOT NULL DEFAULT 0",
            "updated_at": "REAL NOT NULL DEFAULT 0",
        }
        for name, ddl in additions.items():
            if name not in artifact_columns:
                conn.execute(f"ALTER TABLE learned_artifacts ADD COLUMN {name} {ddl}")
        if "tenant_id" in artifact_columns:
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_learned_artifact_reconciliation
                   ON learned_artifacts(tenant_id, artifact_type, stale, knowledge_reconciled_at, id)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_learned_artifact_stale_scan
                   ON learned_artifacts(tenant_id, artifact_type, stale, id, last_seen_at)""")

    for table in ("evidence_requirements", "ownership_hints", "dependency_hints", "signal_mapping_candidates"):
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if columns and "extraction_hash" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN extraction_hash TEXT NOT NULL DEFAULT ''")
        if columns and table in {"ownership_hints", "dependency_hints"} and "source_type" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN source_type TEXT NOT NULL DEFAULT ''")


def ensure_artifact_tenant_scope(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Migrate learned artifacts and extracted rows to tenant-scoped identities."""
    unique_indexes = [
        [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
        for index in conn.execute("PRAGMA index_list(learned_artifacts)").fetchall()
        if index["unique"]
    ]
    extraction_tables = (
        "evidence_requirements",
        "ownership_hints",
        "dependency_hints",
        "signal_mapping_candidates",
    )
    extraction_keys_are_scoped = all(
        {row["name"]: row["pk"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}.get("tenant_id") == 1
        for table in extraction_tables
    )
    artifact_key_is_scoped = ["tenant_id", "artifact_id"] in unique_indexes and ["artifact_id"] not in unique_indexes
    if artifact_key_is_scoped and extraction_keys_are_scoped:
        ensure_artifact_tenant_indexes(conn)
        return
    rebuild_artifact_learning_tables(conn, legacy_tenant=legacy_tenant)


def ensure_artifact_tenant_indexes(conn: sqlite3.Connection) -> None:
    """Create tenant-leading lookup indexes after tenant columns are available."""
    execute_script_statements(
        conn,
        """
        CREATE INDEX IF NOT EXISTS idx_learned_artifacts_type
            ON learned_artifacts(tenant_id, artifact_type);
        CREATE INDEX IF NOT EXISTS idx_learned_artifacts_page
            ON learned_artifacts(tenant_id, artifact_type, updated_at DESC, id DESC);
        CREATE INDEX IF NOT EXISTS idx_learned_artifact_reconciliation
            ON learned_artifacts(tenant_id, artifact_type, stale, knowledge_reconciled_at, id);
        CREATE INDEX IF NOT EXISTS idx_learned_artifact_stale_scan
            ON learned_artifacts(tenant_id, artifact_type, stale, id, last_seen_at);
        CREATE INDEX IF NOT EXISTS idx_evidence_requirements_artifact
            ON evidence_requirements(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_evidence_requirements_artifact_page
            ON evidence_requirements(tenant_id, artifact_id, id);
        CREATE INDEX IF NOT EXISTS idx_ownership_hints_artifact
            ON ownership_hints(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_ownership_hints_artifact_page
            ON ownership_hints(tenant_id, artifact_id, id);
        CREATE INDEX IF NOT EXISTS idx_dependency_hints_artifact
            ON dependency_hints(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_dependency_hints_artifact_page
            ON dependency_hints(tenant_id, artifact_id, id);
        CREATE INDEX IF NOT EXISTS idx_signal_mapping_candidates_artifact
            ON signal_mapping_candidates(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_signal_mapping_candidates_artifact_page
            ON signal_mapping_candidates(tenant_id, artifact_id, id);
    """,
    )


def rebuild_artifact_learning_tables(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Prepare restartable legacy artifact copies with tenant-qualified keys."""
    tables = (
        "learned_artifacts",
        "evidence_requirements",
        "ownership_hints",
        "dependency_hints",
        "signal_mapping_candidates",
    )
    _prepare_artifact_learning_tables(conn, tables)


def _prepare_artifact_learning_tables(
    conn: sqlite3.Connection,
    tables: tuple[str, ...],
) -> None:
    for table in tables:
        table_info = {row["name"]: row["pk"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if table == "learned_artifacts":
            unique_indexes = [
                [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
                for index in conn.execute("PRAGMA index_list(learned_artifacts)").fetchall()
                if index["unique"]
            ]
            current = ["tenant_id", "artifact_id"] in unique_indexes and ["artifact_id"] not in unique_indexes
        else:
            current = table_info.get("tenant_id") == 1
        if current:
            continue
        _prepare_legacy_schema_copy(
            conn,
            table=table,
            create_target=_create_statement_for_table(SCHEMA_SQL, table),
        )


def rebuild_ingested_dashboards_table(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Prepare a restartable dashboard copy into the tenant/backend schema."""
    _prepare_legacy_schema_copy(
        conn,
        table="ingested_dashboards",
        create_target="""
        CREATE TABLE ingested_dashboards (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id           TEXT NOT NULL DEFAULT 'default',
            dashboard_uid       TEXT NOT NULL,
            backend_name        TEXT NOT NULL DEFAULT '',
            dashboard_title     TEXT NOT NULL DEFAULT '',
            dashboard_tags      TEXT NOT NULL DEFAULT '[]',
            metrics_found       TEXT NOT NULL DEFAULT '[]',
            panel_count         INTEGER NOT NULL DEFAULT 0,
            row_groups          TEXT NOT NULL DEFAULT '[]',
            metric_cooccurrence TEXT NOT NULL DEFAULT '{}',
            aggregation_patterns TEXT NOT NULL DEFAULT '[]',
            query_transformations TEXT NOT NULL DEFAULT '[]',
            panel_titles        TEXT NOT NULL DEFAULT '[]',
            alert_links         TEXT NOT NULL DEFAULT '[]',
            drilldown_links     TEXT NOT NULL DEFAULT '[]',
            status              TEXT NOT NULL DEFAULT 'pending',
            signals_inferred    TEXT NOT NULL DEFAULT '[]',
            archetype_generated TEXT NOT NULL DEFAULT '',
            stale               INTEGER NOT NULL DEFAULT 0,
            missing_since       REAL,
            knowledge_reconciled_at REAL,
            last_seen_at        REAL NOT NULL DEFAULT 0,
            created_at          REAL NOT NULL,
            reviewed_at         REAL,
            UNIQUE(tenant_id, dashboard_uid, backend_name)
        );
    """,
    )


def _ensure_ingested_dashboard_indexes(conn: sqlite3.Connection) -> None:
    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(ingested_dashboards)")}
    if {"tenant_id", "dashboard_uid", "backend_name"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_tenant_uid_backend
               ON ingested_dashboards(tenant_id, dashboard_uid, backend_name)""")
    if {"tenant_id", "backend_name", "stale", "knowledge_reconciled_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_reconciliation
               ON ingested_dashboards(tenant_id, backend_name, stale, knowledge_reconciled_at, id)""")
    if {"tenant_id", "backend_name", "stale", "id", "last_seen_at"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_stale_scan
               ON ingested_dashboards(tenant_id, backend_name, stale, id, last_seen_at)""")
    if {"tenant_id", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_page
               ON ingested_dashboards(tenant_id, created_at DESC, id DESC)""")
    if {"tenant_id", "status", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_status_page
               ON ingested_dashboards(tenant_id, status, created_at DESC, id DESC)""")
    if {"tenant_id", "backend_name", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_backend_page
               ON ingested_dashboards(tenant_id, backend_name, created_at DESC, id DESC)""")
    if {"tenant_id", "backend_name", "status", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_backend_status_page
               ON ingested_dashboards(tenant_id, backend_name, status, created_at DESC, id DESC)""")


def rebuild_ingested_alerts_table(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Prepare a restartable alert copy into the tenant/backend schema."""
    _prepare_legacy_schema_copy(
        conn,
        table="ingested_alerts",
        create_target="""
        CREATE TABLE ingested_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL DEFAULT 'default', alert_uid TEXT NOT NULL,
            backend_name TEXT NOT NULL DEFAULT '', source_vendor TEXT NOT NULL DEFAULT '',
            source_instance TEXT NOT NULL DEFAULT '', external_id TEXT NOT NULL DEFAULT '',
            fingerprint TEXT NOT NULL DEFAULT '', generation_fingerprint TEXT NOT NULL DEFAULT '',
            alert_title TEXT NOT NULL DEFAULT '',
            alert_tags TEXT NOT NULL DEFAULT '[]', condition TEXT NOT NULL DEFAULT '',
            severity TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
            labels TEXT NOT NULL DEFAULT '{}', annotations TEXT NOT NULL DEFAULT '{}',
            metrics_found TEXT NOT NULL DEFAULT '[]', query_transformations TEXT NOT NULL DEFAULT '[]',
            service_hints TEXT NOT NULL DEFAULT '[]', dashboard_uid TEXT NOT NULL DEFAULT '',
            panel_title TEXT NOT NULL DEFAULT '', source_url TEXT NOT NULL DEFAULT '',
            provenance_url TEXT NOT NULL DEFAULT '', confidence REAL NOT NULL DEFAULT 0.0,
            stale INTEGER NOT NULL DEFAULT 0, missing_since REAL,
            knowledge_reconciled_at REAL,
            status TEXT NOT NULL DEFAULT 'pending', signals_inferred TEXT NOT NULL DEFAULT '[]',
            first_seen_at REAL NOT NULL, last_seen_at REAL NOT NULL,
            updated_at REAL NOT NULL, created_at REAL NOT NULL, reviewed_at REAL,
            UNIQUE(tenant_id, alert_uid, backend_name)
        );
    """,
    )


def _ensure_ingested_alert_indexes(conn: sqlite3.Connection) -> None:
    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(ingested_alerts)")}
    if {"tenant_id", "alert_uid", "backend_name"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_tenant_uid_backend
               ON ingested_alerts(tenant_id, alert_uid, backend_name)""")
    if {"tenant_id", "backend_name", "stale", "knowledge_reconciled_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_reconciliation
               ON ingested_alerts(tenant_id, backend_name, stale, knowledge_reconciled_at, id)""")
    if {"tenant_id", "backend_name", "stale", "id", "last_seen_at"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_stale_scan
               ON ingested_alerts(tenant_id, backend_name, stale, id, last_seen_at)""")
    if {"tenant_id", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_page
               ON ingested_alerts(tenant_id, created_at DESC, id DESC)""")
    if {"tenant_id", "status", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_status_page
               ON ingested_alerts(tenant_id, status, created_at DESC, id DESC)""")
    if {"tenant_id", "backend_name", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_backend_page
               ON ingested_alerts(tenant_id, backend_name, created_at DESC, id DESC)""")
    if {"tenant_id", "backend_name", "status", "created_at", "id"}.issubset(columns):
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_backend_status_page
               ON ingested_alerts(tenant_id, backend_name, status, created_at DESC, id DESC)""")


def _legacy_tenant_expression(columns: set[str]) -> str:
    return "COALESCE(tenant_id, ?)" if "tenant_id" in columns else "?"


def _no_finalize(_conn: sqlite3.Connection) -> None:
    return


def _legacy_schema_copy_spec(
    conn: sqlite3.Connection,
    *,
    table: str,
    legacy_tenant: str,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[Any, ...], Callable[[sqlite3.Connection], None]]:
    legacy_columns = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    tenant = _legacy_tenant_expression(legacy_columns)
    tenant_parameters: tuple[Any, ...] = (legacy_tenant,)
    columns: tuple[str, ...]
    expressions: tuple[str, ...]
    if table == "learning_context_fts":
        columns = (
            "tenant_id",
            "source_kind",
            "source_id",
            "backend_name",
            "dashboard_uid",
            "dashboard_title",
            "dashboard_tags",
            "panel_title",
            "metric_name",
            "query_text",
            "service",
            "signal_type",
            "review_state",
            "reason",
            "provenance",
            "indexed_at",
        )
        return columns, ("?", *columns[1:]), tenant_parameters, _no_finalize
    if table == "signal_metric_mappings":
        columns = (
            "id",
            "tenant_id",
            "signal_type",
            "metric_pattern",
            "confidence",
            "context_services",
            "context_datasource_types",
            "context_environments",
            "context_archetypes",
            "context_regions",
            "context_clusters",
            "context_namespaces",
            "context_versions",
            "valid_from",
            "valid_until",
            "source_type",
            "source_refs",
            "governance_ref",
            "governance_revision",
            "projection_key",
            "inference_version",
            "review_state",
            "use_count",
            "positive_feedback",
            "negative_feedback",
            "created_at",
            "last_seen",
        )
        dynamic = {
            "tenant_id": f"CASE WHEN source_type='bootstrap' THEN '{GLOBAL_BOOTSTRAP_TENANT_ID}' ELSE {tenant} END",
            "governance_ref": "COALESCE(governance_ref, '')" if "governance_ref" in legacy_columns else "''",
            "governance_revision": (
                "COALESCE(governance_revision, 0)" if "governance_revision" in legacy_columns else "0"
            ),
            "projection_key": "COALESCE(projection_key, '')" if "projection_key" in legacy_columns else "''",
            "context_regions": "context_regions" if "context_regions" in legacy_columns else "'[]'",
            "context_clusters": "context_clusters" if "context_clusters" in legacy_columns else "'[]'",
            "context_namespaces": "context_namespaces" if "context_namespaces" in legacy_columns else "'[]'",
            "context_versions": "context_versions" if "context_versions" in legacy_columns else "'[]'",
            "valid_from": "valid_from" if "valid_from" in legacy_columns else "NULL",
            "valid_until": "valid_until" if "valid_until" in legacy_columns else "NULL",
        }
        return (
            columns,
            tuple(dynamic.get(column, column) for column in columns),
            tenant_parameters,
            _ensure_signal_mapping_indexes,
        )
    artifact_columns: dict[str, tuple[str, ...]] = {
        "learned_artifacts": (
            "id",
            "tenant_id",
            "artifact_id",
            "artifact_type",
            "source_vendor",
            "source_instance",
            "external_id",
            "title",
            "body_text",
            "provenance_url",
            "fingerprint",
            "extraction_generation",
            "stale",
            "missing_since",
            "knowledge_reconciled_at",
            "first_seen_at",
            "last_seen_at",
            "updated_at",
            "created_at",
        ),
        "evidence_requirements": (
            "tenant_id",
            "id",
            "artifact_id",
            "subject",
            "evidence_kind",
            "target_entity",
            "signal_hint",
            "query_hint",
            "priority",
            "source_artifact_id",
            "source_excerpt",
            "source_type",
            "confidence_prior",
            "review_state",
            "observation_state",
            "extraction_hash",
            "created_at",
        ),
        "ownership_hints": (
            "tenant_id",
            "id",
            "artifact_id",
            "entity",
            "owner",
            "hint_kind",
            "source_artifact_id",
            "source_excerpt",
            "source_type",
            "confidence_prior",
            "review_state",
            "extraction_hash",
            "created_at",
        ),
        "dependency_hints": (
            "tenant_id",
            "id",
            "artifact_id",
            "source_entity",
            "target_entity",
            "direction",
            "source_artifact_id",
            "source_excerpt",
            "source_type",
            "confidence_prior",
            "review_state",
            "extraction_hash",
            "created_at",
        ),
        "signal_mapping_candidates": (
            "tenant_id",
            "id",
            "artifact_id",
            "source",
            "candidate_metric",
            "symptom",
            "signal_type",
            "source_artifact_id",
            "source_excerpt",
            "query_hint",
            "confidence_prior",
            "review_state",
            "extraction_hash",
            "created_at",
        ),
    }
    if table in artifact_columns:
        columns = artifact_columns[table]
        expressions = tuple(tenant if column == "tenant_id" else column for column in columns)
        return columns, expressions, tenant_parameters, _no_finalize
    if table == "ingested_dashboards":
        columns = (
            "id",
            "tenant_id",
            "dashboard_uid",
            "backend_name",
            "dashboard_title",
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
            "status",
            "signals_inferred",
            "archetype_generated",
            "stale",
            "missing_since",
            "knowledge_reconciled_at",
            "last_seen_at",
            "created_at",
            "reviewed_at",
        )
        expressions = tuple(
            (
                tenant
                if column == "tenant_id"
                else (
                    "COALESCE(backend_name, '')"
                    if column == "backend_name"
                    else (
                        "CASE WHEN COALESCE(last_seen_at, 0)=0 THEN created_at ELSE last_seen_at END"
                        if column == "last_seen_at"
                        else column
                    )
                )
            )
            for column in columns
        )
        return columns, expressions, tenant_parameters, _ensure_ingested_dashboard_indexes
    if table == "ingested_alerts":
        columns = (
            "id",
            "tenant_id",
            "alert_uid",
            "backend_name",
            "source_vendor",
            "source_instance",
            "external_id",
            "fingerprint",
            "generation_fingerprint",
            "alert_title",
            "alert_tags",
            "condition",
            "severity",
            "enabled",
            "labels",
            "annotations",
            "metrics_found",
            "query_transformations",
            "service_hints",
            "dashboard_uid",
            "panel_title",
            "source_url",
            "provenance_url",
            "confidence",
            "stale",
            "missing_since",
            "knowledge_reconciled_at",
            "status",
            "signals_inferred",
            "first_seen_at",
            "last_seen_at",
            "updated_at",
            "created_at",
            "reviewed_at",
        )
        expressions = tuple(tenant if column == "tenant_id" else column for column in columns)
        return columns, expressions, tenant_parameters, _ensure_ingested_alert_indexes
    raise RuntimeError(f"Unsupported legacy signal schema migration table: {table}")


def reconcile_legacy_signal_schema_batch(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str,
    batch_size: int = 500,
) -> tuple[bool, str, int]:
    """Copy or finalize at most one legacy signal-schema batch."""
    owner_row = conn.execute(
        "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
        (_LEGACY_SCHEMA_OWNER_MARKER,),
    ).fetchone()
    if owner_row is None:
        if _legacy_schema_copy_pending(conn):
            raise RuntimeError("Signal legacy schema migration has no pinned owner")
    elif str(owner_row["value"]) != legacy_tenant:
        raise RuntimeError("Signal legacy schema migration owner does not match the configured tenant")
    migration_order = (
        "signal_types",
        "learning_context_fts",
        "signal_metric_mappings",
        "learned_artifacts",
        "evidence_requirements",
        "ownership_hints",
        "dependency_hints",
        "signal_mapping_candidates",
        "ingested_dashboards",
        "ingested_alerts",
    )
    for table in migration_order:
        if not _table_exists(conn, _legacy_schema_table(table)):
            continue
        if table == "signal_types":
            complete, row_count = _copy_legacy_signal_definitions_batch(
                conn,
                legacy_tenant=legacy_tenant,
                batch_size=batch_size,
            )
            operation = f"{table}:finalize" if complete else f"{table}:copy"
            return False, operation, row_count
        columns, expressions, parameters, finalize = _legacy_schema_copy_spec(
            conn,
            table=table,
            legacy_tenant=legacy_tenant,
        )
        complete, row_count = _copy_legacy_schema_batch(
            conn,
            table=table,
            insert_columns=columns,
            select_expressions=expressions,
            select_parameters=parameters,
            finalize=finalize,
            batch_size=batch_size,
        )
        operation = f"{table}:finalize" if complete else f"{table}:copy"
        return False, operation, row_count
    return True, "complete", 0
