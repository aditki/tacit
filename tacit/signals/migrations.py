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
    ensure_ingested_alert_tenant_scope(conn)
    ensure_artifact_learning_columns(conn)
    ensure_artifact_tenant_scope(conn)
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
    rebuild_ingested_dashboards_table(conn)


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


def ensure_ingested_alert_tenant_scope(conn: sqlite3.Connection) -> None:
    """Ensure ingested alert uniqueness includes tenant and backend identity."""
    for index in conn.execute("PRAGMA index_list(ingested_alerts)").fetchall():
        if not index["unique"]:
            continue
        indexed_cols = [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
        if indexed_cols == ["tenant_id", "alert_uid", "backend_name"]:
            return
    rebuild_ingested_alerts_table(conn)


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
            "first_seen_at": "REAL NOT NULL DEFAULT 0",
            "last_seen_at": "REAL NOT NULL DEFAULT 0",
            "updated_at": "REAL NOT NULL DEFAULT 0",
        }
        for name, ddl in additions.items():
            if name not in artifact_columns:
                conn.execute(f"ALTER TABLE learned_artifacts ADD COLUMN {name} {ddl}")

    for table in ("evidence_requirements", "ownership_hints", "dependency_hints", "signal_mapping_candidates"):
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if columns and "extraction_hash" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN extraction_hash TEXT NOT NULL DEFAULT ''")
        if columns and table in {"ownership_hints", "dependency_hints"} and "source_type" not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN source_type TEXT NOT NULL DEFAULT ''")


def ensure_artifact_tenant_scope(conn: sqlite3.Connection) -> None:
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
    rebuild_artifact_learning_tables(conn)


def ensure_artifact_tenant_indexes(conn: sqlite3.Connection) -> None:
    """Create tenant-leading lookup indexes after tenant columns are available."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_learned_artifacts_type
            ON learned_artifacts(tenant_id, artifact_type);
        CREATE INDEX IF NOT EXISTS idx_evidence_requirements_artifact
            ON evidence_requirements(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_ownership_hints_artifact
            ON ownership_hints(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_dependency_hints_artifact
            ON dependency_hints(tenant_id, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_signal_mapping_candidates_artifact
            ON signal_mapping_candidates(tenant_id, artifact_id);
    """)


def rebuild_artifact_learning_tables(conn: sqlite3.Connection) -> None:
    """Rebuild legacy artifact tables with tenant-qualified primary keys."""
    tables = (
        "learned_artifacts",
        "evidence_requirements",
        "ownership_hints",
        "dependency_hints",
        "signal_mapping_candidates",
    )
    tenant_select = {
        table: (
            "COALESCE(tenant_id, 'default')"
            if "tenant_id" in {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            else "'default'"
        )
        for table in tables
    }
    for table in tables:
        conn.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
    conn.executescript("""
        CREATE TABLE learned_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL DEFAULT 'default', artifact_id TEXT NOT NULL,
            artifact_type TEXT NOT NULL, source_vendor TEXT NOT NULL DEFAULT '',
            source_instance TEXT NOT NULL DEFAULT '', external_id TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '', body_text TEXT NOT NULL DEFAULT '',
            provenance_url TEXT NOT NULL DEFAULT '', fingerprint TEXT NOT NULL DEFAULT '',
            stale INTEGER NOT NULL DEFAULT 0, missing_since REAL,
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
    """)
    conn.execute(f"""INSERT INTO learned_artifacts
        SELECT id, {tenant_select['learned_artifacts']}, artifact_id, artifact_type, source_vendor,
               source_instance, external_id, title, body_text, provenance_url, fingerprint, stale,
               missing_since, first_seen_at, last_seen_at, updated_at, created_at
        FROM learned_artifacts_old""")
    conn.execute(f"""INSERT INTO evidence_requirements
        SELECT {tenant_select['evidence_requirements']}, id, artifact_id, subject, evidence_kind,
               target_entity, signal_hint, query_hint, priority, source_artifact_id, source_excerpt,
               source_type, confidence_prior, review_state, observation_state, extraction_hash, created_at
        FROM evidence_requirements_old""")
    conn.execute(f"""INSERT INTO ownership_hints
        SELECT {tenant_select['ownership_hints']}, id, artifact_id, entity, owner, hint_kind,
               source_artifact_id, source_excerpt, source_type, confidence_prior, review_state,
               extraction_hash, created_at FROM ownership_hints_old""")
    conn.execute(f"""INSERT INTO dependency_hints
        SELECT {tenant_select['dependency_hints']}, id, artifact_id, source_entity, target_entity,
               direction, source_artifact_id, source_excerpt, source_type, confidence_prior, review_state,
               extraction_hash, created_at FROM dependency_hints_old""")
    conn.execute(f"""INSERT INTO signal_mapping_candidates
        SELECT {tenant_select['signal_mapping_candidates']}, id, artifact_id, source, candidate_metric,
               symptom, signal_type, source_artifact_id, source_excerpt, query_hint, confidence_prior,
               review_state, extraction_hash, created_at FROM signal_mapping_candidates_old""")
    for table in tables:
        conn.execute(f"DROP TABLE {table}_old")
    ensure_artifact_tenant_indexes(conn)


def rebuild_ingested_dashboards_table(conn: sqlite3.Connection) -> None:
    """Rebuild legacy ingested dashboards with tenant/backend-scoped uniqueness."""
    old_columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()}
    tenant_select = "COALESCE(tenant_id, 'default')" if "tenant_id" in old_columns else "'default'"
    conn.execute("ALTER TABLE ingested_dashboards RENAME TO ingested_dashboards_old")
    conn.executescript("""
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
            created_at          REAL NOT NULL,
            reviewed_at         REAL,
            UNIQUE(tenant_id, dashboard_uid, backend_name)
        );
    """)
    conn.execute(f"""INSERT INTO ingested_dashboards
           (id, tenant_id, dashboard_uid, backend_name, dashboard_title, dashboard_tags,
            metrics_found, panel_count, row_groups, metric_cooccurrence,
            aggregation_patterns, query_transformations, panel_titles,
            alert_links, drilldown_links, status, signals_inferred,
            archetype_generated, created_at, reviewed_at)
           SELECT id, {tenant_select}, dashboard_uid, COALESCE(backend_name, ''), dashboard_title, dashboard_tags,
                  metrics_found, panel_count, row_groups, metric_cooccurrence,
                  aggregation_patterns, query_transformations, panel_titles,
                  alert_links, drilldown_links, status, signals_inferred,
                  archetype_generated, created_at, reviewed_at
           FROM ingested_dashboards_old""")
    conn.execute("DROP TABLE ingested_dashboards_old")
    conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_ingested_tenant_uid_backend
           ON ingested_dashboards(tenant_id, dashboard_uid, backend_name)""")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_tenant_uid_backend
           ON ingested_dashboards(tenant_id, dashboard_uid, backend_name)""")


def rebuild_ingested_alerts_table(conn: sqlite3.Connection) -> None:
    """Rebuild legacy ingested alerts with tenant/backend-scoped uniqueness."""
    old_columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_alerts)").fetchall()}
    tenant_select = "COALESCE(tenant_id, 'default')" if "tenant_id" in old_columns else "'default'"
    conn.execute("ALTER TABLE ingested_alerts RENAME TO ingested_alerts_old")
    conn.executescript("""
        CREATE TABLE ingested_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tenant_id TEXT NOT NULL DEFAULT 'default', alert_uid TEXT NOT NULL,
            backend_name TEXT NOT NULL DEFAULT '', source_vendor TEXT NOT NULL DEFAULT '',
            source_instance TEXT NOT NULL DEFAULT '', external_id TEXT NOT NULL DEFAULT '',
            fingerprint TEXT NOT NULL DEFAULT '', alert_title TEXT NOT NULL DEFAULT '',
            alert_tags TEXT NOT NULL DEFAULT '[]', condition TEXT NOT NULL DEFAULT '',
            severity TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
            labels TEXT NOT NULL DEFAULT '{}', annotations TEXT NOT NULL DEFAULT '{}',
            metrics_found TEXT NOT NULL DEFAULT '[]', query_transformations TEXT NOT NULL DEFAULT '[]',
            service_hints TEXT NOT NULL DEFAULT '[]', dashboard_uid TEXT NOT NULL DEFAULT '',
            panel_title TEXT NOT NULL DEFAULT '', source_url TEXT NOT NULL DEFAULT '',
            provenance_url TEXT NOT NULL DEFAULT '', confidence REAL NOT NULL DEFAULT 0.0,
            stale INTEGER NOT NULL DEFAULT 0, missing_since REAL,
            status TEXT NOT NULL DEFAULT 'pending', signals_inferred TEXT NOT NULL DEFAULT '[]',
            first_seen_at REAL NOT NULL, last_seen_at REAL NOT NULL,
            updated_at REAL NOT NULL, created_at REAL NOT NULL, reviewed_at REAL,
            UNIQUE(tenant_id, alert_uid, backend_name)
        );
    """)
    conn.execute(f"""INSERT INTO ingested_alerts
           (id, tenant_id, alert_uid, backend_name, source_vendor, source_instance, external_id,
            fingerprint, alert_title, alert_tags, condition, severity, enabled, labels, annotations,
            metrics_found, query_transformations, service_hints, dashboard_uid, panel_title,
            source_url, provenance_url, confidence, stale, missing_since, status, signals_inferred,
            first_seen_at, last_seen_at, updated_at, created_at, reviewed_at)
           SELECT id, {tenant_select}, alert_uid, backend_name, source_vendor, source_instance, external_id,
                  fingerprint, alert_title, alert_tags, condition, severity, enabled, labels, annotations,
                  metrics_found, query_transformations, service_hints, dashboard_uid, panel_title,
                  source_url, provenance_url, confidence, stale, missing_since, status, signals_inferred,
                  first_seen_at, last_seen_at, updated_at, created_at, reviewed_at
           FROM ingested_alerts_old""")
    conn.execute("DROP TABLE ingested_alerts_old")
    conn.execute("""CREATE INDEX IF NOT EXISTS idx_ingested_alert_tenant_uid_backend
           ON ingested_alerts(tenant_id, alert_uid, backend_name)""")
