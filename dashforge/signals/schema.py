"""SQLite schema and storage constants for semantic signals."""

from __future__ import annotations

from pathlib import Path

DEFAULT_DB_PATH = Path("data/dashforge_signals.db")
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

    UNIQUE(signal_type, metric_pattern),
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
    UNIQUE(dashboard_uid, backend_name)
);

CREATE INDEX IF NOT EXISTS idx_smm_signal ON signal_metric_mappings(signal_type);
CREATE INDEX IF NOT EXISTS idx_smm_metric ON signal_metric_mappings(metric_pattern);
"""

FTS_SCHEMA_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS learning_context_fts USING fts5(
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
