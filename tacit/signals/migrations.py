"""SQLite schema migration helpers for the signal store."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

import structlog

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
CURRENT_SIGNAL_SCHEMA_MARKER = "signal_schema_operational_knowledge_v3"
GOVERNED_PROJECTION_AUDIT_MARKER = "governed_projection_audit_v2"


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


def ensure_schema(
    conn: sqlite3.Connection,
    *,
    legacy_tenant: str | None = "default",
    bootstrap_signal_definitions: dict[str, dict[str, Any]] | None = None,
) -> None:
    """Install base schema and run additive migrations."""
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
    definition_scope_complete = _migration_marker_exists(conn, _SIGNAL_DEFINITION_SCOPE_MARKER)
    execute_script_statements(conn, SCHEMA_SQL)
    if not definition_scope_complete:
        if bootstrap_signal_definitions is not None:
            migrate_legacy_signal_definitions(
                conn,
                legacy_tenant=migration_tenant,
                bootstrap_signal_definitions=bootstrap_signal_definitions,
            )
        _record_migration_marker(
            conn,
            _SIGNAL_DEFINITION_SCOPE_MARKER,
            migration_tenant,
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
    ensure_governed_projection_audit_triggers(conn)
    ensure_global_bootstrap_mapping_scope(conn)
    ensure_mapping_source_ref_index(conn)
    ensure_rejected_candidate_tenant_scope(conn, legacy_tenant=migration_tenant)
    mark_governed_projection_audit_dirty(conn, reason="schema_migration")
    _record_migration_marker(conn, CURRENT_SIGNAL_SCHEMA_MARKER, migration_tenant)


def signal_schema_is_current(conn: sqlite3.Connection) -> bool:
    """Verify the current-schema marker against the small physical schema surface."""
    if not _migration_marker_exists(conn, CURRENT_SIGNAL_SCHEMA_MARKER):
        return False
    required_columns = {
        "signal_metric_mappings": {
            "tenant_id",
            "governance_ref",
            "governance_revision",
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
    required_tables = {
        "signal_types",
        "tenant_signal_types",
        "signal_mapping_source_refs",
        "signal_tenant_migration_metadata",
        "learning_context_fts",
    }
    existing_tables = {
        str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    }
    if not required_tables.issubset(existing_tables):
        return False
    required_indexes = {
        "idx_smm_governance",
        "idx_smm_tenant_signal_page",
        "idx_signal_mapping_source_ref",
        "idx_ingested_dashboard_reconciliation",
        "idx_ingested_dashboard_stale_scan",
        "idx_ingested_alert_reconciliation",
        "idx_ingested_alert_stale_scan",
        "idx_learned_artifact_reconciliation",
        "idx_learned_artifact_stale_scan",
        "idx_learned_artifacts_page",
        "idx_evidence_requirements_artifact_page",
        "idx_ownership_hints_artifact_page",
        "idx_dependency_hints_artifact_page",
        "idx_signal_mapping_candidates_artifact_page",
    }
    existing_indexes = {
        str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
    }
    if not required_indexes.issubset(existing_indexes):
        return False
    required_triggers = {
        "trg_governed_mapping_insert_audit_dirty",
        "trg_governed_mapping_update_audit_dirty",
        "trg_governed_mapping_delete_audit_dirty",
        "trg_signal_mapping_source_ref_insert",
        "trg_signal_mapping_source_ref_update",
        "trg_signal_mapping_source_ref_delete",
    }
    existing_triggers = {
        str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()
    }
    return required_triggers.issubset(existing_triggers)


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
    for trigger_name in (
        "trg_governed_mapping_insert_audit_dirty",
        "trg_governed_mapping_update_audit_dirty",
        "trg_governed_mapping_delete_audit_dirty",
    ):
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    conn.execute(f"""CREATE TRIGGER trg_governed_mapping_insert_audit_dirty
           AFTER INSERT ON signal_metric_mappings
           WHEN NEW.governance_ref != ''
             OR (NEW.source_type != 'bootstrap' AND NEW.review_state IN ('approved', 'trusted'))
           BEGIN
             INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
             VALUES ('{GOVERNED_PROJECTION_AUDIT_MARKER}',
                     'dirty:' || lower(hex(randomblob(8))), CAST(strftime('%s', 'now') AS REAL))
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
           END""")
    conn.execute(f"""CREATE TRIGGER trg_governed_mapping_update_audit_dirty
           AFTER UPDATE ON signal_metric_mappings
           WHEN OLD.governance_ref != '' OR NEW.governance_ref != ''
             OR (OLD.source_type != 'bootstrap' AND OLD.review_state IN ('approved', 'trusted'))
             OR (NEW.source_type != 'bootstrap' AND NEW.review_state IN ('approved', 'trusted'))
           BEGIN
             INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
             VALUES ('{GOVERNED_PROJECTION_AUDIT_MARKER}',
                     'dirty:' || lower(hex(randomblob(8))), CAST(strftime('%s', 'now') AS REAL))
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
           END""")
    conn.execute(f"""CREATE TRIGGER trg_governed_mapping_delete_audit_dirty
           AFTER DELETE ON signal_metric_mappings
           WHEN OLD.governance_ref != ''
             OR (OLD.source_type != 'bootstrap' AND OLD.review_state IN ('approved', 'trusted'))
           BEGIN
             INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
             VALUES ('{GOVERNED_PROJECTION_AUDIT_MARKER}',
                     'dirty:' || lower(hex(randomblob(8))), CAST(strftime('%s', 'now') AS REAL))
             ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at;
           END""")


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
    if not _table_exists(conn, "signal_types"):
        return
    has_legacy_definitions = conn.execute("SELECT 1 FROM signal_types LIMIT 1").fetchone() is not None
    if not has_legacy_definitions or bootstrap_signal_definitions is not None:
        return
    logger.error("legacy_signal_definition_migration_blocked", reason="bootstrap_taxonomy_unavailable")
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
        logger.error("signal_default_owner_unconfirmed", tables=tables)
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
        f"SELECT rowid AS _migration_rowid, * FROM {table} WHERE {where} LIMIT ?",
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
        after_rowid = int(cursor_row["value"]) if cursor_row is not None else 0
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
            after_rowid = int(cursor_row["value"]) if cursor_row is not None else 0
            rows = conn.execute(
                f"""SELECT rowid AS migration_rowid FROM signal_metric_mappings
                    WHERE rowid>? AND tenant_id='default'{source_filter}
                    ORDER BY rowid LIMIT ?""",
                (after_rowid, batch_size),
            ).fetchall()
            if rows:
                last_rowid = int(rows[-1]["migration_rowid"])
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
            _record_migration_marker(
                conn,
                f"{_DEFAULT_OWNER_CURSOR_PREFIX}{table}",
                str(max(0, int(remaining["first_rowid"]) - 1)),
            )
            return False, f"{table}:rescan", 0

    if _tenant_column_exists(conn, "signal_metric_mappings"):
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
        source_filter = " AND source_type!='bootstrap'" if "source_type" in columns else ""
        remaining = conn.execute(f"""SELECT MIN(rowid) AS first_rowid FROM signal_metric_mappings
                WHERE tenant_id='default'{source_filter}""").fetchone()
        if remaining is not None and remaining["first_rowid"] is not None:
            _record_migration_marker(
                conn,
                f"{_DEFAULT_OWNER_CURSOR_PREFIX}signal_metric_mappings",
                str(max(0, int(remaining["first_rowid"]) - 1)),
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
        logger.error("legacy_tenant_owner_required", tables=sorted(set(tenant_owned_tables)))
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
    """Move pre-tenant custom definitions while retaining configured taxonomy globals."""
    rows = conn.execute("SELECT * FROM signal_types").fetchall()
    migrated = 0
    for row in rows:
        signal_type = str(row["signal_type"])
        bootstrap = bootstrap_signal_definitions.get(signal_type)
        expected = {
            "description": str((bootstrap or {}).get("description") or ""),
            "category": str((bootstrap or {}).get("category") or ""),
            "unit": str((bootstrap or {}).get("unit") or ""),
        }
        tenant_override = bootstrap is None or any(str(row[field]) != value for field, value in expected.items())
        if not tenant_override:
            continue
        conn.execute(
            """INSERT OR IGNORE INTO tenant_signal_types
               (tenant_id, signal_type, description, category, unit, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                legacy_tenant,
                row["signal_type"],
                row["description"],
                row["category"],
                row["unit"],
                row["created_at"],
                row["updated_at"],
            ),
        )
        if bootstrap is None:
            conn.execute("DELETE FROM signal_types WHERE signal_type=?", (signal_type,))
        else:
            conn.execute(
                """UPDATE signal_types
                   SET description=?, category=?, unit=?
                   WHERE signal_type=?""",
                (expected["description"], expected["category"], expected["unit"], signal_type),
            )
        migrated += 1
    if migrated:
        logger.info(
            "legacy_signal_definitions_tenant_scoped",
            tenant_id=legacy_tenant,
            definitions=migrated,
        )


def ensure_learning_index(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Create the FTS5 operational knowledge index when available."""
    try:
        conn.execute(FTS_SCHEMA_SQL)
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(learning_context_fts)").fetchall()}
        if columns and "tenant_id" not in columns:
            rebuild_learning_index(conn, legacy_tenant=legacy_tenant)
    except sqlite3.OperationalError as exc:
        if "no such module: fts5" not in str(exc).casefold():
            raise
        logger.warning("learning_context_fts_unavailable", error=str(exc))


def rebuild_learning_index(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Migrate legacy learning-index rows into the configured legacy tenant."""
    with atomic_rebuild(conn, "rebuild_learning_context_fts"):
        conn.execute("ALTER TABLE learning_context_fts RENAME TO learning_context_fts_old")
        conn.execute(FTS_SCHEMA_SQL)
        conn.execute(
            """INSERT INTO learning_context_fts
            (tenant_id, source_kind, source_id, backend_name, dashboard_uid,
             dashboard_title, dashboard_tags, panel_title, metric_name, query_text,
             service, signal_type, review_state, reason, provenance, indexed_at)
            SELECT ?, source_kind, source_id, backend_name, dashboard_uid,
                   dashboard_title, dashboard_tags, panel_title, metric_name, query_text,
                   service, signal_type, review_state, reason, provenance, indexed_at
            FROM learning_context_fts_old""",
            (legacy_tenant,),
        )
        conn.execute("DROP TABLE learning_context_fts_old")


def ensure_mapping_columns(conn: sqlite3.Connection) -> None:
    """Add newer columns to signal_metric_mappings on pre-existing DBs."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
    if "inference_version" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN inference_version TEXT NOT NULL DEFAULT ''")
    if "review_state" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN review_state TEXT NOT NULL DEFAULT 'trusted'")
    if "governance_revision" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN governance_revision INTEGER NOT NULL DEFAULT 0")
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
        conn.execute("UPDATE rejected_signal_candidates SET tenant_id=?", (legacy_tenant,))
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
        ["tenant_id", "signal_type", "metric_pattern", "governance_ref"] in unique_indexes
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
        return
    old_columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
    legacy_literal = _tenant_sql_literal(legacy_tenant)
    tenant_select = f"COALESCE(tenant_id, {legacy_literal})" if "tenant_id" in old_columns else legacy_literal
    governance_select = "COALESCE(governance_ref, '')" if "governance_ref" in old_columns else "''"
    governance_revision_select = "COALESCE(governance_revision, 0)" if "governance_revision" in old_columns else "0"
    scope_selects = {
        column: column if column in old_columns else "'[]'"
        for column in ("context_regions", "context_clusters", "context_namespaces", "context_versions")
    }
    valid_from_select = "valid_from" if "valid_from" in old_columns else "NULL"
    valid_until_select = "valid_until" if "valid_until" in old_columns else "NULL"
    with atomic_rebuild(conn, "rebuild_signal_metric_mappings"):
        conn.execute("ALTER TABLE signal_metric_mappings RENAME TO signal_metric_mappings_old")
        conn.execute("""CREATE TABLE signal_metric_mappings (
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
            inference_version TEXT NOT NULL DEFAULT '', review_state TEXT NOT NULL DEFAULT 'trusted',
            use_count INTEGER NOT NULL DEFAULT 0, positive_feedback INTEGER NOT NULL DEFAULT 0,
            negative_feedback INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, last_seen REAL NOT NULL,
            UNIQUE(tenant_id, signal_type, metric_pattern, governance_ref)
        )""")
        conn.execute(f"""INSERT INTO signal_metric_mappings
            (id, tenant_id, signal_type, metric_pattern, confidence, context_services,
             context_datasource_types, context_environments, context_archetypes,
             context_regions, context_clusters, context_namespaces, context_versions,
             valid_from, valid_until,
             source_type, source_refs, governance_ref, governance_revision,
             inference_version, review_state, use_count,
             positive_feedback, negative_feedback, created_at, last_seen)
            SELECT id, {tenant_select}, signal_type, metric_pattern, confidence, context_services,
                   context_datasource_types, context_environments, context_archetypes,
                   {scope_selects["context_regions"]}, {scope_selects["context_clusters"]},
                   {scope_selects["context_namespaces"]}, {scope_selects["context_versions"]},
                   {valid_from_select}, {valid_until_select},
                   source_type, source_refs, {governance_select}, {governance_revision_select},
                   inference_version, review_state, use_count,
                   positive_feedback, negative_feedback, created_at, last_seen
            FROM signal_metric_mappings_old""")
        conn.execute("DROP TABLE signal_metric_mappings_old")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_smm_signal ON signal_metric_mappings(signal_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_smm_metric ON signal_metric_mappings(metric_pattern)")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_tenant_signal
               ON signal_metric_mappings(tenant_id, signal_type)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_tenant_signal_page
               ON signal_metric_mappings(tenant_id, signal_type, confidence DESC, id DESC)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_smm_governance
               ON signal_metric_mappings(tenant_id, governance_ref) WHERE governance_ref != ''""")


def ensure_mapping_source_ref_index(conn: sqlite3.Connection) -> None:
    """Build the indexed source-ref projection and its maintenance triggers."""
    for trigger in (
        "trg_signal_mapping_source_ref_insert",
        "trg_signal_mapping_source_ref_update",
        "trg_signal_mapping_source_ref_delete",
    ):
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger}")
    conn.execute("""CREATE TABLE IF NOT EXISTS signal_mapping_source_refs (
               mapping_id INTEGER NOT NULL,
               tenant_id TEXT NOT NULL,
               source_ref TEXT NOT NULL,
               PRIMARY KEY (mapping_id, source_ref)
           )""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_signal_mapping_source_ref
           ON signal_mapping_source_refs(tenant_id, source_ref, mapping_id)""")
    conn.execute("DELETE FROM signal_mapping_source_refs")
    backfilled = conn.execute("""INSERT OR IGNORE INTO signal_mapping_source_refs (mapping_id, tenant_id, source_ref)
           SELECT mapping.id, mapping.tenant_id, source.value
           FROM signal_metric_mappings mapping
           JOIN json_each(
               CASE WHEN json_valid(mapping.source_refs) THEN mapping.source_refs ELSE '[]' END
           ) source
           WHERE source.type='text' AND source.value != ''""").rowcount
    conn.execute("""CREATE TRIGGER trg_signal_mapping_source_ref_insert
           AFTER INSERT ON signal_metric_mappings
           BEGIN
               INSERT OR IGNORE INTO signal_mapping_source_refs (mapping_id, tenant_id, source_ref)
               SELECT NEW.id, NEW.tenant_id, value
               FROM json_each(CASE WHEN json_valid(NEW.source_refs) THEN NEW.source_refs ELSE '[]' END)
               WHERE type='text' AND value != '';
           END""")
    conn.execute("""CREATE TRIGGER trg_signal_mapping_source_ref_update
           AFTER UPDATE OF tenant_id, source_refs ON signal_metric_mappings
           BEGIN
               DELETE FROM signal_mapping_source_refs WHERE mapping_id=OLD.id;
               INSERT OR IGNORE INTO signal_mapping_source_refs (mapping_id, tenant_id, source_ref)
               SELECT NEW.id, NEW.tenant_id, value
               FROM json_each(CASE WHEN json_valid(NEW.source_refs) THEN NEW.source_refs ELSE '[]' END)
               WHERE type='text' AND value != '';
           END""")
    conn.execute("""CREATE TRIGGER trg_signal_mapping_source_ref_delete
           AFTER DELETE ON signal_metric_mappings
           BEGIN
               DELETE FROM signal_mapping_source_refs WHERE mapping_id=OLD.id;
           END""")
    if backfilled:
        logger.info("signal_mapping_source_refs_indexed", source_ref_count=backfilled)


def ensure_global_bootstrap_mapping_scope(conn: sqlite3.Connection) -> None:
    """Move legacy global defaults out of the real ``default`` tenant."""
    moved = conn.execute(
        """UPDATE OR IGNORE signal_metric_mappings SET tenant_id=?
           WHERE tenant_id != ? AND source_type='bootstrap'""",
        (GLOBAL_BOOTSTRAP_TENANT_ID, GLOBAL_BOOTSTRAP_TENANT_ID),
    ).rowcount
    deduplicated = conn.execute(
        """DELETE FROM signal_metric_mappings
           WHERE tenant_id != ? AND source_type='bootstrap'""",
        (GLOBAL_BOOTSTRAP_TENANT_ID,),
    ).rowcount
    if moved or deduplicated:
        logger.info(
            "bootstrap_signal_mappings_migrated",
            moved=moved,
            deduplicated=deduplicated,
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
    try:
        content = json.loads(authority["content_json"])
    except (TypeError, json.JSONDecodeError):
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
    exact_mapping = next(
        (
            item
            for item in resolver_mappings
            if isinstance(item, dict) and str(item.get("metric_pattern") or "") == str(mapping["metric_pattern"])
        ),
        None,
    )
    if exact_mapping is None:
        return False, "authority_metric_mismatch"
    try:
        expected_confidence = max(0.0, min(1.0, float(exact_mapping.get("confidence", 0.5))))
    except (TypeError, ValueError):
        expected_confidence = 0.5
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
        "context_datasource_types": sorted(
            {
                value
                for item in resolver_mappings
                if isinstance(item, dict)
                for value in _json_values(item.get("context_datasource_types", []))
            }
        ),
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
            return
    rebuild_ingested_dashboards_table(conn, legacy_tenant=legacy_tenant)


def ensure_ingested_dashboard_columns(conn: sqlite3.Connection) -> None:
    """Add source-lifecycle fields to dashboard records."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()}
    if "stale" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN stale INTEGER NOT NULL DEFAULT 0")
    if "missing_since" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN missing_since REAL")
    if "knowledge_reconciled_at" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN knowledge_reconciled_at REAL")
    if "last_seen_at" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN last_seen_at REAL NOT NULL DEFAULT 0")
        conn.execute("UPDATE ingested_dashboards SET last_seen_at=created_at WHERE last_seen_at=0")
    if "tenant_id" in columns:
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_reconciliation
               ON ingested_dashboards(tenant_id, backend_name, stale, knowledge_reconciled_at, id)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_stale_scan
               ON ingested_dashboards(tenant_id, backend_name, stale, id, last_seen_at)""")


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
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_reconciliation
               ON ingested_alerts(tenant_id, backend_name, stale, knowledge_reconciled_at, id)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_stale_scan
               ON ingested_alerts(tenant_id, backend_name, stale, id, last_seen_at)""")


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
    """Rebuild legacy artifact tables with tenant-qualified primary keys."""
    tables = (
        "learned_artifacts",
        "evidence_requirements",
        "ownership_hints",
        "dependency_hints",
        "signal_mapping_candidates",
    )
    legacy_literal = _tenant_sql_literal(legacy_tenant)
    tenant_select = {
        table: (
            f"COALESCE(tenant_id, {legacy_literal})"
            if "tenant_id" in {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            else legacy_literal
        )
        for table in tables
    }
    with atomic_rebuild(conn, "rebuild_artifact_learning"):
        _rebuild_artifact_learning_tables(conn, tables, tenant_select)


def _rebuild_artifact_learning_tables(
    conn: sqlite3.Connection,
    tables: tuple[str, ...],
    tenant_select: dict[str, str],
) -> None:
    for table in tables:
        conn.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
    execute_script_statements(
        conn,
        """
        CREATE TABLE learned_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL DEFAULT 'default', artifact_id TEXT NOT NULL,
            artifact_type TEXT NOT NULL, source_vendor TEXT NOT NULL DEFAULT '',
            source_instance TEXT NOT NULL DEFAULT '', external_id TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '', body_text TEXT NOT NULL DEFAULT '',
            provenance_url TEXT NOT NULL DEFAULT '', fingerprint TEXT NOT NULL DEFAULT '',
            extraction_generation TEXT NOT NULL DEFAULT '',
            stale INTEGER NOT NULL DEFAULT 0, missing_since REAL,
            knowledge_reconciled_at REAL,
            first_seen_at REAL NOT NULL, last_seen_at REAL NOT NULL,
            updated_at REAL NOT NULL, created_at REAL NOT NULL,
            UNIQUE(tenant_id, artifact_id)
        );
        CREATE TABLE evidence_requirements (
            tenant_id TEXT NOT NULL DEFAULT 'default', id TEXT NOT NULL, artifact_id TEXT NOT NULL,
            subject TEXT NOT NULL DEFAULT '', evidence_kind TEXT NOT NULL DEFAULT '', target_entity TEXT,
            signal_hint TEXT, query_hint TEXT, priority INTEGER, source_artifact_id TEXT NOT NULL DEFAULT '',
            source_excerpt TEXT NOT NULL DEFAULT '', source_type TEXT NOT NULL DEFAULT '',
            confidence_prior REAL NOT NULL DEFAULT 0.5, review_state TEXT NOT NULL DEFAULT 'candidate',
            observation_state TEXT NOT NULL DEFAULT 'indeterminate', extraction_hash TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL, PRIMARY KEY (tenant_id, id)
        );
        CREATE TABLE ownership_hints (
            tenant_id TEXT NOT NULL DEFAULT 'default', id TEXT NOT NULL, artifact_id TEXT NOT NULL,
            entity TEXT NOT NULL DEFAULT '', owner TEXT NOT NULL DEFAULT '', hint_kind TEXT NOT NULL DEFAULT '',
            source_artifact_id TEXT NOT NULL DEFAULT '', source_excerpt TEXT NOT NULL DEFAULT '',
            source_type TEXT NOT NULL DEFAULT '', confidence_prior REAL NOT NULL DEFAULT 0.5,
            review_state TEXT NOT NULL DEFAULT 'candidate', extraction_hash TEXT NOT NULL DEFAULT '',
            created_at REAL NOT NULL, PRIMARY KEY (tenant_id, id)
        );
        CREATE TABLE dependency_hints (
            tenant_id TEXT NOT NULL DEFAULT 'default', id TEXT NOT NULL, artifact_id TEXT NOT NULL,
            source_entity TEXT NOT NULL DEFAULT '', target_entity TEXT NOT NULL DEFAULT '',
            direction TEXT NOT NULL DEFAULT 'unknown', source_artifact_id TEXT NOT NULL DEFAULT '',
            source_excerpt TEXT NOT NULL DEFAULT '', source_type TEXT NOT NULL DEFAULT '',
            confidence_prior REAL NOT NULL DEFAULT 0.5, review_state TEXT NOT NULL DEFAULT 'candidate',
            extraction_hash TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
            PRIMARY KEY (tenant_id, id)
        );
        CREATE TABLE signal_mapping_candidates (
            tenant_id TEXT NOT NULL DEFAULT 'default', id TEXT NOT NULL, artifact_id TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '', candidate_metric TEXT NOT NULL DEFAULT '',
            symptom TEXT NOT NULL DEFAULT '', signal_type TEXT NOT NULL DEFAULT '',
            source_artifact_id TEXT NOT NULL DEFAULT '', source_excerpt TEXT NOT NULL DEFAULT '', query_hint TEXT,
            confidence_prior REAL NOT NULL DEFAULT 0.5, review_state TEXT NOT NULL DEFAULT 'candidate',
            extraction_hash TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
            PRIMARY KEY (tenant_id, id)
        );
    """,
    )
    conn.execute(f"""INSERT INTO learned_artifacts
        SELECT id, {tenant_select["learned_artifacts"]}, artifact_id, artifact_type, source_vendor,
               source_instance, external_id, title, body_text, provenance_url, fingerprint,
               extraction_generation, stale, missing_since, knowledge_reconciled_at,
               first_seen_at, last_seen_at, updated_at, created_at
        FROM learned_artifacts_old""")
    conn.execute(f"""INSERT INTO evidence_requirements
        SELECT {tenant_select["evidence_requirements"]}, id, artifact_id, subject, evidence_kind,
               target_entity, signal_hint, query_hint, priority, source_artifact_id, source_excerpt,
               source_type, confidence_prior, review_state, observation_state, extraction_hash, created_at
        FROM evidence_requirements_old""")
    conn.execute(f"""INSERT INTO ownership_hints
        SELECT {tenant_select["ownership_hints"]}, id, artifact_id, entity, owner, hint_kind,
               source_artifact_id, source_excerpt, source_type, confidence_prior, review_state,
               extraction_hash, created_at FROM ownership_hints_old""")
    conn.execute(f"""INSERT INTO dependency_hints
        SELECT {tenant_select["dependency_hints"]}, id, artifact_id, source_entity, target_entity,
               direction, source_artifact_id, source_excerpt, source_type, confidence_prior, review_state,
               extraction_hash, created_at FROM dependency_hints_old""")
    conn.execute(f"""INSERT INTO signal_mapping_candidates
        SELECT {tenant_select["signal_mapping_candidates"]}, id, artifact_id, source, candidate_metric,
               symptom, signal_type, source_artifact_id, source_excerpt, query_hint, confidence_prior,
               review_state, extraction_hash, created_at FROM signal_mapping_candidates_old""")
    for table in tables:
        conn.execute(f"DROP TABLE {table}_old")
    ensure_artifact_tenant_indexes(conn)


def rebuild_ingested_dashboards_table(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Rebuild legacy ingested dashboards with tenant/backend-scoped uniqueness."""
    old_columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()}
    legacy_literal = _tenant_sql_literal(legacy_tenant)
    tenant_select = f"COALESCE(tenant_id, {legacy_literal})" if "tenant_id" in old_columns else legacy_literal
    with atomic_rebuild(conn, "rebuild_ingested_dashboards"):
        _rebuild_ingested_dashboards_table(conn, tenant_select)


def _rebuild_ingested_dashboards_table(conn: sqlite3.Connection, tenant_select: str) -> None:
    conn.execute("ALTER TABLE ingested_dashboards RENAME TO ingested_dashboards_old")
    execute_script_statements(
        conn,
        """
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
    conn.execute(f"""INSERT INTO ingested_dashboards
           (id, tenant_id, dashboard_uid, backend_name, dashboard_title, dashboard_tags,
            metrics_found, panel_count, row_groups, metric_cooccurrence,
            aggregation_patterns, query_transformations, panel_titles,
            alert_links, drilldown_links, status, signals_inferred,
            archetype_generated, stale, missing_since, knowledge_reconciled_at,
            last_seen_at, created_at, reviewed_at)
           SELECT id, {tenant_select}, dashboard_uid, COALESCE(backend_name, ''), dashboard_title, dashboard_tags,
                  metrics_found, panel_count, row_groups, metric_cooccurrence,
                  aggregation_patterns, query_transformations, panel_titles,
                  alert_links, drilldown_links, status, signals_inferred,
                  archetype_generated, stale, missing_since, knowledge_reconciled_at,
                  last_seen_at, created_at, reviewed_at
           FROM ingested_dashboards_old""")
    conn.execute("DROP TABLE ingested_dashboards_old")
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_ingested_tenant_uid_backend
           ON ingested_dashboards(tenant_id, dashboard_uid, backend_name)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_tenant_uid_backend
           ON ingested_dashboards(tenant_id, dashboard_uid, backend_name)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_reconciliation
           ON ingested_dashboards(tenant_id, backend_name, stale, knowledge_reconciled_at, id)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_dashboard_stale_scan
           ON ingested_dashboards(tenant_id, backend_name, stale, id, last_seen_at)""")


def rebuild_ingested_alerts_table(conn: sqlite3.Connection, *, legacy_tenant: str = "default") -> None:
    """Rebuild legacy ingested alerts with tenant/backend-scoped uniqueness."""
    old_columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_alerts)").fetchall()}
    legacy_literal = _tenant_sql_literal(legacy_tenant)
    tenant_select = f"COALESCE(tenant_id, {legacy_literal})" if "tenant_id" in old_columns else legacy_literal
    with atomic_rebuild(conn, "rebuild_ingested_alerts"):
        _rebuild_ingested_alerts_table(conn, tenant_select)


def _rebuild_ingested_alerts_table(conn: sqlite3.Connection, tenant_select: str) -> None:
    conn.execute("ALTER TABLE ingested_alerts RENAME TO ingested_alerts_old")
    execute_script_statements(
        conn,
        """
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
    conn.execute(f"""INSERT INTO ingested_alerts
           (id, tenant_id, alert_uid, backend_name, source_vendor, source_instance, external_id,
            fingerprint, generation_fingerprint, alert_title, alert_tags, condition, severity,
            enabled, labels, annotations,
            metrics_found, query_transformations, service_hints, dashboard_uid, panel_title,
            source_url, provenance_url, confidence, stale, missing_since, knowledge_reconciled_at,
            status, signals_inferred,
            first_seen_at, last_seen_at, updated_at, created_at, reviewed_at)
           SELECT id, {tenant_select}, alert_uid, backend_name, source_vendor, source_instance, external_id,
                  fingerprint, generation_fingerprint, alert_title, alert_tags, condition, severity,
                  enabled, labels, annotations,
                  metrics_found, query_transformations, service_hints, dashboard_uid, panel_title,
                  source_url, provenance_url, confidence, stale, missing_since, knowledge_reconciled_at,
                  status, signals_inferred,
                  first_seen_at, last_seen_at, updated_at, created_at, reviewed_at
           FROM ingested_alerts_old""")
    conn.execute("DROP TABLE ingested_alerts_old")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_tenant_uid_backend
           ON ingested_alerts(tenant_id, alert_uid, backend_name)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_reconciliation
           ON ingested_alerts(tenant_id, backend_name, stale, knowledge_reconciled_at, id)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_stale_scan
           ON ingested_alerts(tenant_id, backend_name, stale, id, last_seen_at)""")
