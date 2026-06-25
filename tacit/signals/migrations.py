"""SQLite schema migration helpers for the signal store."""

from __future__ import annotations

import sqlite3

import structlog

from tacit.signals.schema import FTS_SCHEMA_SQL, SCHEMA_SQL

logger = structlog.get_logger()


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Install base schema and run additive migrations."""
    conn.executescript(SCHEMA_SQL)
    ensure_learning_index(conn)
    ensure_ingested_dashboard_backend_scope(conn)
    ensure_ingested_alert_columns(conn)
    ensure_mapping_columns(conn)


def ensure_learning_index(conn: sqlite3.Connection) -> None:
    """Create the FTS5 operational knowledge index when available."""
    try:
        conn.executescript(FTS_SCHEMA_SQL)
    except sqlite3.OperationalError as exc:
        logger.warning("learning_context_fts_unavailable", error=str(exc))


def ensure_mapping_columns(conn: sqlite3.Connection) -> None:
    """Add newer columns to signal_metric_mappings on pre-existing DBs."""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
    if "inference_version" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN inference_version TEXT NOT NULL DEFAULT ''")
    if "review_state" not in columns:
        conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN review_state TEXT NOT NULL DEFAULT 'trusted'")


def ensure_ingested_dashboard_backend_scope(conn: sqlite3.Connection) -> None:
    """Ensure ingested dashboard uniqueness includes backend identity."""
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()]
    if "backend_name" not in columns:
        conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN backend_name TEXT NOT NULL DEFAULT ''")
        columns.append("backend_name")

    for index in conn.execute("PRAGMA index_list(ingested_dashboards)").fetchall():
        if not index["unique"]:
            continue
        indexed_cols = [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
        if indexed_cols == ["dashboard_uid"]:
            rebuild_ingested_dashboards_table(conn)
            return

    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_ingested_uid_backend
           ON ingested_dashboards(dashboard_uid, backend_name)""")


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
        "provenance_url": "TEXT NOT NULL DEFAULT ''",
        "confidence": "REAL NOT NULL DEFAULT 0.0",
        "stale": "INTEGER NOT NULL DEFAULT 0",
        "missing_since": "REAL",
        "first_seen_at": "REAL NOT NULL DEFAULT 0",
        "last_seen_at": "REAL NOT NULL DEFAULT 0",
        "updated_at": "REAL NOT NULL DEFAULT 0",
    }
    for name, ddl in additions.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE ingested_alerts ADD COLUMN {name} {ddl}")


def rebuild_ingested_dashboards_table(conn: sqlite3.Connection) -> None:
    """Rebuild legacy ingested dashboards table with backend-scoped uniqueness."""
    conn.execute("ALTER TABLE ingested_dashboards RENAME TO ingested_dashboards_old")
    conn.executescript("""
        CREATE TABLE ingested_dashboards (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
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
            created_at          REAL NOT NULL,
            reviewed_at         REAL,
            UNIQUE(dashboard_uid, backend_name)
        );
    """)
    conn.execute("""INSERT INTO ingested_dashboards
           (id, dashboard_uid, backend_name, dashboard_title, dashboard_tags,
            metrics_found, panel_count, row_groups, metric_cooccurrence,
            aggregation_patterns, query_transformations, panel_titles,
            alert_links, drilldown_links, status, signals_inferred,
            archetype_generated, created_at, reviewed_at)
           SELECT id, dashboard_uid, COALESCE(backend_name, ''), dashboard_title, dashboard_tags,
                  metrics_found, panel_count, row_groups, metric_cooccurrence,
                  aggregation_patterns, query_transformations, panel_titles,
                  alert_links, drilldown_links, status, signals_inferred,
                  archetype_generated, created_at, reviewed_at
           FROM ingested_dashboards_old""")
    conn.execute("DROP TABLE ingested_dashboards_old")
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_ingested_uid_backend
           ON ingested_dashboards(dashboard_uid, backend_name)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_uid_backend
           ON ingested_dashboards(dashboard_uid, backend_name)""")
