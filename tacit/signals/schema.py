"""SQLite schema and storage constants for semantic signals."""

from __future__ import annotations

from pathlib import Path

DEFAULT_DB_PATH = Path("data/tacit_signals.db")
SQLITE_BUSY_TIMEOUT_MS = 30_000

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS signal_types (
    signal_type     TEXT PRIMARY KEY,
    description     TEXT NOT NULL DEFAULT '',
    category        TEXT NOT NULL DEFAULT '',
    unit            TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS signal_metric_mappings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    signal_type         TEXT NOT NULL,
    metric_pattern      TEXT NOT NULL,
    confidence          REAL NOT NULL DEFAULT 0.5,

    -- Context filters (JSON arrays — empty = applies everywhere)
    context_services        TEXT NOT NULL DEFAULT '[]',
    context_datasource_types TEXT NOT NULL DEFAULT '[]',
    context_environments    TEXT NOT NULL DEFAULT '[]',
    context_archetypes      TEXT NOT NULL DEFAULT '[]',

    -- Provenance
    source_type         TEXT NOT NULL DEFAULT 'bootstrap',
    source_refs         TEXT NOT NULL DEFAULT '[]',
    -- Which inference ruleset produced this (for invalidate/replay).
    inference_version   TEXT NOT NULL DEFAULT '',
    -- Lifecycle: heuristic mappings start 'candidate' → 'approved' → 'trusted';
    -- curated/bootstrap/teach mappings are 'trusted' from the start.
    review_state        TEXT NOT NULL DEFAULT 'trusted',

    -- Trust / decay
    use_count           INTEGER NOT NULL DEFAULT 0,
    positive_feedback   INTEGER NOT NULL DEFAULT 0,
    negative_feedback   INTEGER NOT NULL DEFAULT 0,

    -- Timestamps
    created_at          REAL NOT NULL,
    last_seen           REAL NOT NULL,

    UNIQUE(tenant_id, signal_type, metric_pattern),
    FOREIGN KEY (signal_type) REFERENCES signal_types(signal_type)
);

-- Inferred candidates that were NOT auto-taught (negative training data).
CREATE TABLE IF NOT EXISTS rejected_signal_candidates (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_uid       TEXT NOT NULL DEFAULT '',
    backend_name        TEXT NOT NULL DEFAULT '',
    metric              TEXT NOT NULL,
    signal_family       TEXT NOT NULL DEFAULT '',
    signal_name         TEXT NOT NULL DEFAULT '',
    score               REAL NOT NULL DEFAULT 0.0,
    margin              REAL NOT NULL DEFAULT 0.0,
    why_not             TEXT NOT NULL DEFAULT '',
    evidence            TEXT NOT NULL DEFAULT '[]',
    inference_version   TEXT NOT NULL DEFAULT '',
    created_at          REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_dashboards (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    dashboard_uid       TEXT NOT NULL,
    backend_name        TEXT NOT NULL DEFAULT '',
    dashboard_title     TEXT NOT NULL DEFAULT '',
    dashboard_tags      TEXT NOT NULL DEFAULT '[]',

    -- Extracted features
    metrics_found       TEXT NOT NULL DEFAULT '[]',
    panel_count         INTEGER NOT NULL DEFAULT 0,
    row_groups          TEXT NOT NULL DEFAULT '[]',
    metric_cooccurrence TEXT NOT NULL DEFAULT '{}',
    aggregation_patterns TEXT NOT NULL DEFAULT '[]',
    query_transformations TEXT NOT NULL DEFAULT '[]',
    panel_titles        TEXT NOT NULL DEFAULT '[]',
    alert_links         TEXT NOT NULL DEFAULT '[]',
    drilldown_links     TEXT NOT NULL DEFAULT '[]',

    -- Status
    status              TEXT NOT NULL DEFAULT 'pending',
    signals_inferred    TEXT NOT NULL DEFAULT '[]',
    archetype_generated TEXT NOT NULL DEFAULT '',

    created_at          REAL NOT NULL,
    reviewed_at         REAL,
    UNIQUE(tenant_id, dashboard_uid, backend_name)
);

CREATE TABLE IF NOT EXISTS ingested_alerts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    alert_uid           TEXT NOT NULL,
    backend_name        TEXT NOT NULL DEFAULT '',
    source_vendor       TEXT NOT NULL DEFAULT '',
    source_instance     TEXT NOT NULL DEFAULT '',
    external_id         TEXT NOT NULL DEFAULT '',
    fingerprint         TEXT NOT NULL DEFAULT '',
    alert_title         TEXT NOT NULL DEFAULT '',
    alert_tags          TEXT NOT NULL DEFAULT '[]',
    condition           TEXT NOT NULL DEFAULT '',
    severity            TEXT NOT NULL DEFAULT '',
    enabled             INTEGER NOT NULL DEFAULT 1,
    labels              TEXT NOT NULL DEFAULT '{}',
    annotations         TEXT NOT NULL DEFAULT '{}',

    -- Extracted features
    metrics_found       TEXT NOT NULL DEFAULT '[]',
    query_transformations TEXT NOT NULL DEFAULT '[]',
    service_hints       TEXT NOT NULL DEFAULT '[]',
    dashboard_uid       TEXT NOT NULL DEFAULT '',
    panel_title         TEXT NOT NULL DEFAULT '',
    source_url          TEXT NOT NULL DEFAULT '',
    provenance_url      TEXT NOT NULL DEFAULT '',
    confidence          REAL NOT NULL DEFAULT 0.0,
    stale               INTEGER NOT NULL DEFAULT 0,
    missing_since       REAL,

    -- Status
    status              TEXT NOT NULL DEFAULT 'pending',
    signals_inferred    TEXT NOT NULL DEFAULT '[]',

    first_seen_at       REAL NOT NULL,
    last_seen_at        REAL NOT NULL,
    updated_at          REAL NOT NULL,
    created_at          REAL NOT NULL,
    reviewed_at         REAL,
    UNIQUE(tenant_id, alert_uid, backend_name)
);

CREATE TABLE IF NOT EXISTS learned_artifacts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    artifact_id         TEXT NOT NULL,
    artifact_type       TEXT NOT NULL,
    source_vendor       TEXT NOT NULL DEFAULT '',
    source_instance     TEXT NOT NULL DEFAULT '',
    external_id         TEXT NOT NULL DEFAULT '',
    title               TEXT NOT NULL DEFAULT '',
    body_text           TEXT NOT NULL DEFAULT '',
    provenance_url      TEXT NOT NULL DEFAULT '',
    fingerprint         TEXT NOT NULL DEFAULT '',
    stale               INTEGER NOT NULL DEFAULT 0,
    missing_since       REAL,
    first_seen_at       REAL NOT NULL,
    last_seen_at        REAL NOT NULL,
    updated_at          REAL NOT NULL,
    created_at          REAL NOT NULL,
    UNIQUE(tenant_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS evidence_requirements (
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    id                  TEXT NOT NULL,
    artifact_id         TEXT NOT NULL,
    subject             TEXT NOT NULL DEFAULT '',
    evidence_kind       TEXT NOT NULL DEFAULT '',
    target_entity       TEXT,
    signal_hint         TEXT,
    query_hint          TEXT,
    priority            INTEGER,
    source_artifact_id  TEXT NOT NULL DEFAULT '',
    source_excerpt      TEXT NOT NULL DEFAULT '',
    source_type         TEXT NOT NULL DEFAULT '',
    confidence_prior    REAL NOT NULL DEFAULT 0.5,
    review_state        TEXT NOT NULL DEFAULT 'candidate',
    observation_state   TEXT NOT NULL DEFAULT 'indeterminate',
    extraction_hash     TEXT NOT NULL DEFAULT '',
    created_at          REAL NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS ownership_hints (
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    id                  TEXT NOT NULL,
    artifact_id         TEXT NOT NULL,
    entity              TEXT NOT NULL DEFAULT '',
    owner               TEXT NOT NULL DEFAULT '',
    hint_kind           TEXT NOT NULL DEFAULT '',
    source_artifact_id  TEXT NOT NULL DEFAULT '',
    source_excerpt      TEXT NOT NULL DEFAULT '',
    source_type         TEXT NOT NULL DEFAULT '',
    confidence_prior    REAL NOT NULL DEFAULT 0.5,
    review_state        TEXT NOT NULL DEFAULT 'candidate',
    extraction_hash     TEXT NOT NULL DEFAULT '',
    created_at          REAL NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS dependency_hints (
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    id                  TEXT NOT NULL,
    artifact_id         TEXT NOT NULL,
    source_entity       TEXT NOT NULL DEFAULT '',
    target_entity       TEXT NOT NULL DEFAULT '',
    direction           TEXT NOT NULL DEFAULT 'unknown',
    source_artifact_id  TEXT NOT NULL DEFAULT '',
    source_excerpt      TEXT NOT NULL DEFAULT '',
    source_type         TEXT NOT NULL DEFAULT '',
    confidence_prior    REAL NOT NULL DEFAULT 0.5,
    review_state        TEXT NOT NULL DEFAULT 'candidate',
    extraction_hash     TEXT NOT NULL DEFAULT '',
    created_at          REAL NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS signal_mapping_candidates (
    tenant_id           TEXT NOT NULL DEFAULT 'default',
    id                  TEXT NOT NULL,
    artifact_id         TEXT NOT NULL,
    source              TEXT NOT NULL DEFAULT '',
    candidate_metric    TEXT NOT NULL DEFAULT '',
    symptom             TEXT NOT NULL DEFAULT '',
    signal_type         TEXT NOT NULL DEFAULT '',
    source_artifact_id  TEXT NOT NULL DEFAULT '',
    source_excerpt      TEXT NOT NULL DEFAULT '',
    query_hint          TEXT,
    confidence_prior    REAL NOT NULL DEFAULT 0.5,
    review_state        TEXT NOT NULL DEFAULT 'candidate',
    extraction_hash     TEXT NOT NULL DEFAULT '',
    created_at          REAL NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS idx_smm_signal ON signal_metric_mappings(signal_type);
CREATE INDEX IF NOT EXISTS idx_smm_metric ON signal_metric_mappings(metric_pattern);
CREATE INDEX IF NOT EXISTS idx_ingested_alert_uid_backend ON ingested_alerts(alert_uid, backend_name);
"""

FTS_SCHEMA_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS learning_context_fts USING fts5(
    tenant_id UNINDEXED,
    source_kind,
    source_id UNINDEXED,
    backend_name UNINDEXED,
    dashboard_uid UNINDEXED,
    dashboard_title,
    dashboard_tags,
    panel_title,
    metric_name,
    query_text,
    service,
    signal_type,
    review_state UNINDEXED,
    reason,
    provenance,
    indexed_at UNINDEXED
);
"""
