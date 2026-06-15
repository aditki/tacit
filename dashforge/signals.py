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

import fnmatch
import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path
from typing import Any

import structlog

from dashforge.config import settings
from dashforge.models.schemas import MetricEntry

logger = structlog.get_logger()

_DEFAULT_DB_PATH = Path("data/dashforge_signals.db")

# ── Confidence decay ────────────────────────────────────────────────────────
# Mappings decay in confidence over time if not reinforced.
_DECAY_HALF_LIFE_DAYS = 90  # confidence halves every 90 days without use
_MIN_CONFIDENCE = 0.05  # floor — never fully forget
_TRUST_THRESHOLD = 0.15  # below this, mapping is excluded from resolution
_CONTEXT_MISSING_PENALTY = 0.7  # lower rank when mapping has context caller lacks
_REVIEW_STATE_RANK = {"candidate": 0, "approved": 1, "trusted": 2}
_SQLITE_BUSY_TIMEOUT_MS = 30_000


class LearningIndexUnavailable(RuntimeError):
    """Raised when SQLite FTS5-backed learning retrieval is unavailable."""


def _stronger_review_state(existing: str, incoming: str) -> str:
    """Return the higher-trust review state without allowing downgrades."""
    existing_rank = _REVIEW_STATE_RANK.get(existing, 0)
    incoming_rank = _REVIEW_STATE_RANK.get(incoming, 0)
    return incoming if incoming_rank > existing_rank else existing


def _db_path() -> Path:
    custom = getattr(settings, "signals_db_path", None)
    path = Path(custom) if custom else _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


_SCHEMA_SQL = """
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

_FTS_SCHEMA_SQL = """
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


class SignalStore:
    """SQLite-backed semantic signal mapping store."""

    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or _db_path()
        self._ensure_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self._db_path), timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self):
        with self._conn() as conn:
            conn.executescript(_SCHEMA_SQL)
            self._ensure_learning_index(conn)
            self._ensure_ingested_dashboard_backend_scope(conn)
            self._ensure_mapping_columns(conn)
        logger.info("signal_store_init", db_path=str(self._db_path))

    def _ensure_learning_index(self, conn: sqlite3.Connection) -> None:
        """Create the FTS5 operational knowledge index when available."""
        try:
            conn.executescript(_FTS_SCHEMA_SQL)
        except sqlite3.OperationalError as exc:
            logger.warning("learning_context_fts_unavailable", error=str(exc))

    def _ensure_mapping_columns(self, conn: sqlite3.Connection) -> None:
        """Add newer columns to signal_metric_mappings on pre-existing DBs."""
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)").fetchall()}
        if "inference_version" not in columns:
            conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN inference_version TEXT NOT NULL DEFAULT ''")
        if "review_state" not in columns:
            conn.execute("ALTER TABLE signal_metric_mappings ADD COLUMN review_state TEXT NOT NULL DEFAULT 'trusted'")

    def _ensure_ingested_dashboard_backend_scope(self, conn: sqlite3.Connection) -> None:
        columns = [row["name"] for row in conn.execute("PRAGMA table_info(ingested_dashboards)").fetchall()]
        if "backend_name" not in columns:
            conn.execute("ALTER TABLE ingested_dashboards ADD COLUMN backend_name TEXT NOT NULL DEFAULT ''")
            columns.append("backend_name")

        for index in conn.execute("PRAGMA index_list(ingested_dashboards)").fetchall():
            if not index["unique"]:
                continue
            indexed_cols = [row["name"] for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()]
            if indexed_cols == ["dashboard_uid"]:
                self._rebuild_ingested_dashboards_table(conn)
                return

        conn.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_ingested_uid_backend
               ON ingested_dashboards(dashboard_uid, backend_name)""")

    def _rebuild_ingested_dashboards_table(self, conn: sqlite3.Connection) -> None:
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

    # ── Signal type CRUD ─────────────────────────────────────────────────

    def register_signal_type(
        self,
        signal_type: str,
        description: str = "",
        category: str = "",
        unit: str = "",
    ) -> None:
        """Register or update a canonical signal type."""
        now = time.time()
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO signal_types (signal_type, description, category, unit, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(signal_type) DO UPDATE SET
                       -- Only overwrite metadata when a non-empty value is
                       -- supplied, so a teach call with blank fields doesn't
                       -- wipe bootstrap taxonomy (description/category/unit).
                       description = CASE WHEN excluded.description != '' THEN excluded.description
                                         ELSE signal_types.description END,
                       category = CASE WHEN excluded.category != '' THEN excluded.category
                                       ELSE signal_types.category END,
                       unit = CASE WHEN excluded.unit != '' THEN excluded.unit
                                   ELSE signal_types.unit END,
                       updated_at = excluded.updated_at""",
                (signal_type, description, category, unit, now, now),
            )

    def list_signal_types(self) -> list[dict[str, Any]]:
        """List all registered signal types with mapping counts."""
        with self._conn() as conn:
            rows = conn.execute("""SELECT st.*, COUNT(m.id) AS mapping_count
                   FROM signal_types st
                   LEFT JOIN signal_metric_mappings m
                     ON st.signal_type = m.signal_type
                   GROUP BY st.signal_type
                   ORDER BY st.category, st.signal_type""").fetchall()
        return [dict(r) for r in rows]

    def get_signal_type(self, signal_type: str) -> dict[str, Any] | None:
        """Get a signal type with all its metric mappings."""
        with self._conn() as conn:
            st = conn.execute(
                "SELECT * FROM signal_types WHERE signal_type = ?",
                (signal_type,),
            ).fetchone()
            if st is None:
                return None

            mappings = conn.execute(
                """SELECT * FROM signal_metric_mappings
                   WHERE signal_type = ? ORDER BY confidence DESC""",
                (signal_type,),
            ).fetchall()

        result = dict(st)
        result["mappings"] = [_deserialize_mapping(r) for r in mappings]
        return result

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
        source_type: str = "bootstrap",
        source_refs: list[str] | None = None,
        inference_version: str = "",
        review_state: str = "trusted",
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
        if not 0.0 <= confidence <= 1.0:
            raise ValueError(f"confidence must be within [0.0, 1.0], got {confidence!r}")
        now = time.time()
        with self._conn() as conn:
            # Ensure signal type exists
            existing = conn.execute(
                "SELECT 1 FROM signal_types WHERE signal_type = ?",
                (signal_type,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO signal_types (signal_type, description, category, unit, created_at, updated_at)
                       VALUES (?, '', '', '', ?, ?)""",
                    (signal_type, now, now),
                )

            # Merge context scopes with any existing mapping so re-teaching the
            # same signal/metric for a second service unions rather than
            # replaces. Semantics per dimension:
            #   None  → leave existing unchanged
            #   []    → explicitly clear (make global)
            #   [...] → union with existing
            prior = conn.execute(
                """SELECT context_services, context_datasource_types,
                          context_environments, context_archetypes,
                          source_refs, inference_version, review_state
                     FROM signal_metric_mappings
                    WHERE signal_type = ? AND metric_pattern = ?""",
                (signal_type, metric_pattern),
            ).fetchone()

            def _merge(provided: list[str] | None, existing_json: str | None) -> list[str]:
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
            ds_types = _merge(context_datasource_types, prior["context_datasource_types"] if prior else None)
            environments = _merge(context_environments, prior["context_environments"] if prior else None)
            archetypes = _merge(context_archetypes, prior["context_archetypes"] if prior else None)
            existing_refs = json.loads(prior["source_refs"]) if prior and prior["source_refs"] else []
            refs = list(existing_refs)
            for ref in source_refs or []:
                if ref not in refs:
                    refs.append(ref)
            merged_inference_version = inference_version or (prior["inference_version"] if prior else "")
            merged_review_state = _stronger_review_state(prior["review_state"], review_state) if prior else review_state

            cursor = conn.execute(
                """INSERT INTO signal_metric_mappings
                   (signal_type, metric_pattern, confidence,
                    context_services, context_datasource_types,
                    context_environments, context_archetypes,
                    source_type, source_refs, inference_version, review_state,
                    created_at, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(signal_type, metric_pattern) DO UPDATE SET
                       confidence = MAX(excluded.confidence, signal_metric_mappings.confidence),
                       inference_version = excluded.inference_version,
                       review_state = excluded.review_state,
                       -- excluded.context_* already holds the merged scopes.
                       context_services = excluded.context_services,
                       context_datasource_types = excluded.context_datasource_types,
                       context_environments = excluded.context_environments,
                       context_archetypes = excluded.context_archetypes,
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
                       use_count = signal_metric_mappings.use_count + 1""",
                (
                    signal_type,
                    metric_pattern,
                    confidence,
                    json.dumps(services),
                    json.dumps(ds_types),
                    json.dumps(environments),
                    json.dumps(archetypes),
                    source_type,
                    json.dumps(refs),
                    merged_inference_version,
                    merged_review_state,
                    now,
                    now,
                ),
            )
            return cursor.lastrowid or 0

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
    ) -> int:
        """Persist an inferred candidate that was NOT auto-taught.

        Rejections are negative training data — they record what the heuristic
        proposed and why it was held back ('low_score'|'low_margin'|
        'single_source_only'), so the ruleset can be tuned/replayed later.
        """
        with self._conn() as conn:
            cursor = conn.execute(
                """INSERT INTO rejected_signal_candidates
                   (dashboard_uid, backend_name, metric, signal_family, signal_name,
                    score, margin, why_not, evidence, inference_version, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
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

    def list_rejected_candidates(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return recorded rejected candidates (newest first)."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM rejected_signal_candidates ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("evidence"), str):
                d["evidence"] = json.loads(d["evidence"])
            out.append(d)
        return out

    def record_feedback(self, signal_type: str, metric_pattern: str, positive: bool) -> None:
        """Record positive/negative feedback for a mapping (anti-drift)."""
        col = "positive_feedback" if positive else "negative_feedback"
        with self._conn() as conn:
            conn.execute(
                f"""UPDATE signal_metric_mappings
                    SET {col} = {col} + 1, last_seen = ?
                    WHERE signal_type = ? AND metric_pattern = ?""",
                (time.time(), signal_type, metric_pattern),
            )

    def get_mappings_for_signal(
        self,
        signal_type: str,
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        context_environment: str = "",
        include_decayed: bool = False,
    ) -> list[dict[str, Any]]:
        """Get all metric mappings for a signal, optionally filtered by context.

        Returns mappings sorted by effective confidence (adjusted for decay
        and feedback).
        """
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM signal_metric_mappings
                   WHERE signal_type = ? ORDER BY confidence DESC""",
                (signal_type,),
            ).fetchall()

        now = time.time()
        results = []
        for row in rows:
            m = _deserialize_mapping(row)

            # Context filtering
            if not _context_matches(
                m, context_service, context_datasource_type, context_archetype, context_environment
            ):
                continue

            # Compute effective confidence with decay + feedback + context ranking
            effective = _effective_confidence(
                m,
                now,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
            )
            trust_effective = _effective_confidence(
                m,
                now,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                context_environment=context_environment,
                apply_context_penalty=False,
            )
            if not include_decayed and trust_effective < _TRUST_THRESHOLD:
                continue

            m["effective_confidence"] = round(effective, 4)
            results.append(m)

        results.sort(key=lambda x: x["effective_confidence"], reverse=True)
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
    ) -> list[tuple[MetricEntry, float]]:
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
        mappings = self.get_mappings_for_signal(
            signal_type,
            context_service=context_service,
            context_datasource_type=context_datasource_type,
            context_archetype=context_archetype,
            context_environment=context_environment,
        )

        if not mappings:
            return []

        target_lang = target_query_language.lower()
        target_ds = context_datasource_type.lower()
        matched: list[tuple[MetricEntry, float]] = []
        seen_metrics: set[str] = set()

        for mapping in mappings:
            pattern = mapping["metric_pattern"]
            eff_conf = mapping["effective_confidence"]

            for entry in catalog:
                if entry.name in seen_metrics:
                    continue
                # Restrict to the target backend's query language so we never
                # substitute a cross-backend metric into the wrong template.
                if target_lang and (entry.query_language or "").lower() != target_lang:
                    continue
                if target_ds and not _datasource_type_matches(entry.datasource_type, target_ds):
                    continue
                if _metric_matches_pattern(entry.name, pattern):
                    matched.append((entry, eff_conf))
                    seen_metrics.add(entry.name)

        matched.sort(key=lambda x: x[1], reverse=True)
        return matched

    def resolve_signals_for_archetype(
        self,
        signal_bindings: dict[str, str],
        catalog: list[MetricEntry],
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        target_query_language: str = "",
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
            resolved = self.resolve_signal(
                signal_type,
                catalog,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
                target_query_language=target_query_language,
            )

            if resolved:
                best_entry, confidence = resolved[0]
                substitutions[default_metric] = best_entry.name
                logger.info(
                    "signal_resolved",
                    signal=signal_type,
                    default_metric=default_metric,
                    resolved_to=best_entry.name,
                    confidence=confidence,
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

        env_path = os.environ.get("DASHFORGE_SIGNALS_PATH")
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

        resource = files("dashforge.data").joinpath("signals.yaml")
        if resource.is_file():
            with resource.open() as f:
                return yaml.safe_load(f) or {}, "package:dashforge.data/signals.yaml"
        return None, None

    def load_from_yaml(self, path: Path | None = None) -> int:
        """Load bootstrap signal definitions from signals.yaml.

        Returns the number of mappings loaded.
        """
        data, source = self._load_yaml_data(path)
        if data is None:
            logger.info("signals_yaml_not_found")
            return 0

        count = 0
        for sig_type, sig_def in data.get("signals", {}).items():
            self.register_signal_type(
                signal_type=sig_type,
                description=sig_def.get("description", ""),
                category=sig_def.get("category", ""),
                unit=sig_def.get("unit", ""),
            )
            for mp in sig_def.get("metric_patterns", []):
                self.add_mapping(
                    signal_type=sig_type,
                    metric_pattern=mp["pattern"],
                    confidence=mp.get("confidence", 0.5),
                    context_datasource_types=mp["datasource_types"] if "datasource_types" in mp else None,
                    source_type="bootstrap",
                )
                count += 1

        logger.info("signals_loaded_from_yaml", path=source, mappings=count)
        return count

    # ── Ingested dashboard records ───────────────────────────────────────

    def record_ingested_dashboard(
        self,
        dashboard_uid: str,
        *,
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
    ) -> None:
        """Record features extracted from an ingested dashboard."""
        now = time.time()
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO ingested_dashboards
                   (dashboard_uid, backend_name, dashboard_title, dashboard_tags,
                    metrics_found, panel_count, row_groups,
                    metric_cooccurrence, aggregation_patterns,
                    query_transformations, panel_titles,
                    alert_links, drilldown_links,
                    status, signals_inferred, archetype_generated, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    dashboard_uid,
                    backend_name,
                    dashboard_title,
                    json.dumps(dashboard_tags or []),
                    json.dumps(metrics_found or []),
                    panel_count,
                    json.dumps(row_groups or []),
                    json.dumps(metric_cooccurrence or {}),
                    json.dumps(aggregation_patterns or []),
                    json.dumps(query_transformations or []),
                    json.dumps(panel_titles or []),
                    json.dumps(alert_links or []),
                    json.dumps(drilldown_links or []),
                    status,
                    json.dumps(signals_inferred or []),
                    archetype_generated,
                    now,
                ),
            )

    def index_dashboard_context(
        self,
        *,
        dashboard_uid: str,
        backend_name: str = "",
        dashboard_title: str = "",
        dashboard_tags: list[str] | None = None,
        panels: list[dict[str, Any]] | None = None,
        metrics_found: list[str] | None = None,
        signals_inferred: list[dict[str, Any]] | list[str] | None = None,
        status: str = "pending",
        activated_pairs: set[tuple[str, str]] | None = None,
    ) -> int:
        """Index learned dashboard context for fast operational-language retrieval.

        The index is intentionally a retrieval aid, not the trust source of
        truth. Mapping approval still lives in ``signal_metric_mappings`` and
        dashboard review state still lives in ``ingested_dashboards``.
        """
        if not self._learning_index_available():
            return 0

        tags = dashboard_tags or []
        metrics = list(dict.fromkeys(metrics_found or []))
        signal_by_metric: dict[str, list[dict[str, Any]]] = {}
        for sig in signals_inferred or []:
            if not isinstance(sig, dict):
                continue
            metric = sig.get("metric", "")
            if metric:
                signal_by_metric.setdefault(metric, []).append(sig)

        rows: list[tuple[str, str, str, str, str, str, str, str, str, str, str, str, str, str, float]] = []
        indexed_at = time.time()
        panel_items = panels or []
        if not panel_items and metrics:
            panel_items = [{"title": "", "queries": [], "metrics": metrics}]

        for panel_index, panel in enumerate(panel_items):
            panel_title = str(panel.get("title", "") or "")
            query_values = panel.get("queries", [])
            if not isinstance(query_values, list):
                query_values = [str(query_values)]
            query_text = "\n".join(str(q) for q in query_values if q)
            panel_metrics = _panel_metrics(panel, metrics)
            if not panel_metrics and metrics:
                panel_metrics = metrics

            for metric in panel_metrics:
                related_signals = signal_by_metric.get(metric) or [{}]
                for sig_index, sig in enumerate(related_signals):
                    if not isinstance(sig, dict):
                        sig = {}
                    services = _infer_services_for_learning(
                        metric=metric,
                        query_text=query_text,
                        dashboard_title=dashboard_title,
                        panel_title=panel_title,
                        tags=tags,
                    )
                    service_text = " ".join(services)
                    signal_type = str(sig.get("signal_type", ""))
                    review_state = _learning_row_review_state(
                        status=status,
                        metric=metric,
                        signal_type=signal_type,
                        sig=sig,
                        activated_pairs=activated_pairs,
                    )
                    reason = str(sig.get("reason", ""))
                    provenance = " ".join(
                        part
                        for part in (
                            f"source:{sig.get('source', '')}" if sig.get("source") else "",
                            f"family:{sig.get('signal_family', '')}" if sig.get("signal_family") else "",
                            f"confidence:{sig.get('confidence', '')}" if sig.get("confidence") else "",
                        )
                        if part
                    )
                    rows.append(
                        (
                            "dashboard_panel",
                            f"{backend_name}:{dashboard_uid}:{panel_index}:{metric}:{sig_index}",
                            backend_name,
                            dashboard_uid,
                            dashboard_title,
                            " ".join(tags),
                            panel_title,
                            metric,
                            query_text,
                            service_text,
                            signal_type,
                            review_state,
                            reason,
                            provenance,
                            indexed_at,
                        )
                    )

        try:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM learning_context_fts WHERE dashboard_uid = ? AND backend_name = ?",
                    (dashboard_uid, backend_name),
                )
                conn.executemany(
                    """INSERT INTO learning_context_fts
                       (source_kind, source_id, backend_name, dashboard_uid,
                        dashboard_title, dashboard_tags, panel_title, metric_name,
                        query_text, service, signal_type, review_state, reason,
                        provenance, indexed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    rows,
                )
        except sqlite3.OperationalError as exc:
            logger.warning("learning_context_index_failed", error=str(exc))
            return 0
        return len(rows)

    def update_learning_context_review_state(
        self,
        dashboard_uid: str,
        review_state: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
    ) -> int:
        """Reflect dashboard approval/rejection in the retrieval index."""
        if not self._learning_index_available():
            return 0
        backend = backend_name if backend_name is not None else ""
        with self._conn() as conn:
            try:
                if review_state == "approved" and activated_pairs is not None:
                    if backend_name is None:
                        cursor = conn.execute(
                            "UPDATE learning_context_fts SET review_state = 'candidate' WHERE dashboard_uid = ?",
                            (dashboard_uid,),
                        )
                    else:
                        cursor = conn.execute(
                            """UPDATE learning_context_fts SET review_state = 'candidate'
                               WHERE dashboard_uid = ? AND backend_name = ?""",
                            (dashboard_uid, backend),
                        )
                    rows_updated = cursor.rowcount
                    for metric, signal_type in activated_pairs:
                        if backend_name is None:
                            cursor = conn.execute(
                                """UPDATE learning_context_fts SET review_state = 'approved'
                                   WHERE dashboard_uid = ? AND metric_name = ? AND signal_type = ?""",
                                (dashboard_uid, metric, signal_type),
                            )
                        else:
                            cursor = conn.execute(
                                """UPDATE learning_context_fts SET review_state = 'approved'
                                   WHERE dashboard_uid = ? AND backend_name = ?
                                     AND metric_name = ? AND signal_type = ?""",
                                (dashboard_uid, backend, metric, signal_type),
                            )
                        rows_updated += cursor.rowcount
                    return rows_updated
                if backend_name is None:
                    cursor = conn.execute(
                        "UPDATE learning_context_fts SET review_state = ? WHERE dashboard_uid = ?",
                        (review_state, dashboard_uid),
                    )
                else:
                    cursor = conn.execute(
                        """UPDATE learning_context_fts SET review_state = ?
                           WHERE dashboard_uid = ? AND backend_name = ?""",
                        (review_state, dashboard_uid, backend),
                    )
                return cursor.rowcount
            except sqlite3.OperationalError as exc:
                logger.warning("learning_context_review_state_update_failed", error=str(exc))
                return 0

    def search_learning_context(
        self,
        query: str,
        *,
        service: str = "",
        include_candidates: bool = True,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search the learned operational knowledge index."""
        if not self._learning_index_available():
            raise LearningIndexUnavailable(
                "Learned-context search requires SQLite FTS5, but this SQLite build does not provide it."
            )

        match_query = _fts_query(query)
        if not match_query:
            return []

        clauses = ["learning_context_fts MATCH ?"]
        params: list[Any] = [match_query]
        if service:
            clauses.append("lower(service) LIKE ?")
            params.append(f"%{service.lower()}%")
        if not include_candidates:
            clauses.append("review_state IN ('approved', 'trusted')")
        else:
            clauses.append("review_state NOT IN ('rejected', 'ignored')")
        params.append(limit)

        sql = f"""SELECT rowid, *, bm25(learning_context_fts) AS rank
                  FROM learning_context_fts
                  WHERE {' AND '.join(clauses)}
                  ORDER BY rank
                  LIMIT ?"""
        with self._conn() as conn:
            try:
                rows = conn.execute(sql, params).fetchall()
            except sqlite3.OperationalError as exc:
                logger.warning("learning_context_search_failed", query=query, error=str(exc))
                return []
        return [dict(row) for row in rows]

    def describe_service(
        self,
        service: str,
        *,
        include_candidates: bool = True,
        limit: int = 50,
    ) -> dict[str, Any]:
        """Summarize what DashForge has learned about a service."""
        rows = self.search_learning_context(
            service,
            service=service,
            include_candidates=include_candidates,
            limit=limit,
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

    def get_ingested_dashboard(self, dashboard_uid: str, backend_name: str | None = None) -> dict[str, Any] | None:
        """Get ingested dashboard record."""
        with self._conn() as conn:
            if backend_name is None:
                rows = conn.execute(
                    """SELECT * FROM ingested_dashboards
                       WHERE dashboard_uid = ?
                       ORDER BY created_at DESC LIMIT 2""",
                    (dashboard_uid,),
                ).fetchall()
                if len(rows) != 1:
                    return None
                row = rows[0]
            else:
                row = conn.execute(
                    """SELECT * FROM ingested_dashboards
                       WHERE dashboard_uid = ? AND backend_name = ?""",
                    (dashboard_uid, backend_name),
                ).fetchone()
        if row is None:
            return None
        return _deserialize_ingested(row)

    def list_ingested_dashboards(self, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """List ingested dashboards, optionally filtered by status."""
        with self._conn() as conn:
            if status:
                rows = conn.execute(
                    """SELECT * FROM ingested_dashboards
                       WHERE status = ? ORDER BY created_at DESC LIMIT ?""",
                    (status, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM ingested_dashboards
                       ORDER BY created_at DESC LIMIT ?""",
                    (limit,),
                ).fetchall()
        return [_deserialize_ingested(r) for r in rows]

    def update_ingested_dashboard_status(
        self,
        dashboard_uid: str,
        status: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
    ) -> bool:
        """Move a pending ingested dashboard to a reviewed status."""
        if status not in {"approved", "rejected", "ignored"}:
            raise ValueError(f"unsupported ingested dashboard status: {status}")

        ingested = self.get_ingested_dashboard(dashboard_uid, backend_name)
        if ingested is None:
            return False

        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE ingested_dashboards SET status = ?, reviewed_at = ?
                   WHERE id = ? AND status = 'pending'""",
                (status, time.time(), ingested["id"]),
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
                )
            else:
                self.update_learning_context_review_state(dashboard_uid, status, backend_name)
        return changed

    def approve_ingested_dashboard(
        self,
        dashboard_uid: str,
        backend_name: str | None = None,
        activated_pairs: set[tuple[str, str]] | None = None,
    ) -> bool:
        """Approve a pending ingested dashboard (activates its signal mappings)."""
        return self.update_ingested_dashboard_status(
            dashboard_uid,
            "approved",
            backend_name,
            activated_pairs=activated_pairs,
        )

    def reject_ingested_dashboard(self, dashboard_uid: str, backend_name: str | None = None) -> bool:
        """Reject a pending ingested dashboard as unsuitable for learning."""
        return self.update_ingested_dashboard_status(dashboard_uid, "rejected", backend_name)

    def ignore_ingested_dashboard(self, dashboard_uid: str, backend_name: str | None = None) -> bool:
        """Ignore a pending ingested dashboard without treating it as negative signal data."""
        return self.update_ingested_dashboard_status(dashboard_uid, "ignored", backend_name)

    # ── Stats ────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Summary statistics for the signal store."""
        with self._conn() as conn:
            signal_count = conn.execute("SELECT COUNT(*) FROM signal_types").fetchone()[0]
            mapping_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings").fetchone()[0]
            ingested_count = conn.execute("SELECT COUNT(*) FROM ingested_dashboards").fetchone()[0]

            by_source = conn.execute("""SELECT source_type, COUNT(*) as n
                   FROM signal_metric_mappings GROUP BY source_type""").fetchall()

            by_category = conn.execute("""SELECT category, COUNT(*) as n
                   FROM signal_types GROUP BY category""").fetchall()

        return {
            "signal_types": signal_count,
            "metric_mappings": mapping_count,
            "ingested_dashboards": ingested_count,
            "mappings_by_source": {r["source_type"]: r["n"] for r in by_source},
            "signals_by_category": {r["category"]: r["n"] for r in by_category},
        }


# ── Helper functions ─────────────────────────────────────────────────────────


_TOKEN_RE = re.compile(r"[A-Za-z0-9_.:/-]+")
_LABEL_SERVICE_RE = re.compile(
    r"(?:^|[,{\s])(?:service|app|application|container|pod|job)\s*(?:=|=~)\s*['\"]([^'\"]+)['\"]"
)
_GENERIC_METRIC_PREFIXES = {
    "http",
    "grpc",
    "request",
    "requests",
    "response",
    "duration",
    "latency",
    "error",
    "errors",
    "cpu",
    "memory",
    "container",
    "pod",
    "node",
    "kube",
    "jvm",
    "process",
    "redis",
    "kafka",
    "envoy",
    "nginx",
    "prometheus",
}


def _panel_metrics(panel: dict[str, Any], fallback_metrics: list[str]) -> list[str]:
    raw = panel.get("metrics", [])
    metrics: list[str] = []
    if isinstance(raw, list):
        metrics.extend(str(m) for m in raw if m)
    elif raw:
        metrics.append(str(raw))

    if metrics:
        return list(dict.fromkeys(metrics))

    queries = panel.get("queries", [])
    if not isinstance(queries, list):
        queries = [queries]
    query_text = "\n".join(str(q) for q in queries if q)
    if not query_text:
        return []
    return [metric for metric in fallback_metrics if metric and metric in query_text]


def _infer_services_for_learning(
    *,
    metric: str,
    query_text: str,
    dashboard_title: str,
    panel_title: str,
    tags: list[str],
) -> list[str]:
    candidates: list[str] = []

    def add(value: str) -> None:
        cleaned = value.strip().strip("*").strip()
        cleaned = re.sub(r"^\.\*", "", cleaned)
        cleaned = re.sub(r"\.\*$", "", cleaned)
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)

    for tag in tags:
        if ":" in tag:
            key, value = tag.split(":", 1)
            if key.lower() in {"service", "app", "application", "component", "team"}:
                add(value)

    for match in _LABEL_SERVICE_RE.findall(query_text):
        add(match)

    first = re.split(r"[_.:-]+", metric, maxsplit=1)[0].lower() if metric else ""
    if first and first not in _GENERIC_METRIC_PREFIXES and len(first) > 2:
        add(first)

    text = f"{dashboard_title} {panel_title}".lower()
    for suffix in ("service", "api", "worker", "gateway"):
        for match in re.findall(rf"\b([a-z0-9][a-z0-9_-]+)[\s_-]+{suffix}\b", text):
            if match not in _GENERIC_METRIC_PREFIXES:
                add(match)

    return candidates


def _fts_query(text: str) -> str:
    terms = []
    for token in _TOKEN_RE.findall(text.lower()):
        token = token.strip("-_.:/")
        if len(token) < 2:
            continue
        escaped = token.replace('"', '""')
        terms.append(f'"{escaped}"')
    return " OR ".join(dict.fromkeys(terms))


def _learning_row_review_state(
    *,
    status: str,
    metric: str,
    signal_type: str,
    sig: dict[str, Any],
    activated_pairs: set[tuple[str, str]] | None = None,
) -> str:
    if status in {"rejected", "ignored"}:
        return status
    if status != "approved":
        return "candidate"
    if activated_pairs is not None:
        return "approved" if (metric, signal_type) in activated_pairs else "candidate"
    if not metric or not signal_type:
        return "candidate"
    if sig.get("source") == "heuristic":
        return "approved" if sig.get("auto_teach_eligible") else "candidate"
    return "trusted" if sig.get("confidence", 0.0) >= 0.5 else "candidate"


def _eligible_pairs_from_ingested_signals(signals: list[Any]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for sig in signals:
        if not isinstance(sig, dict):
            continue
        metric = sig.get("metric", "")
        signal_type = sig.get("signal_type", "")
        if not metric or not signal_type:
            continue
        if sig.get("source") == "heuristic":
            if sig.get("auto_teach_eligible"):
                pairs.add((metric, signal_type))
        elif sig.get("confidence", 0.0) >= 0.5:
            pairs.add((metric, signal_type))
    return pairs


def _deserialize_mapping(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a DB row to a dict with deserialized JSON fields."""
    d = dict(row)
    for field in (
        "context_services",
        "context_datasource_types",
        "context_environments",
        "context_archetypes",
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
    return d


def _context_matches(
    mapping: dict[str, Any],
    service: str,
    datasource_type: str,
    archetype: str,
    environment: str,
) -> bool:
    """Check if a mapping's context filters match the given context.

    Empty context list in the mapping = matches everything. If the caller omits
    a context dimension that the mapping constrains, keep the mapping eligible
    so ingestion-time signal inference can still find it; confidence ranking
    applies a missing-context penalty separately.
    """
    if service and mapping.get("context_services"):
        if service.lower() not in [s.lower() for s in mapping["context_services"]]:
            return False
    if datasource_type and mapping.get("context_datasource_types"):
        if datasource_type.lower() not in [d.lower() for d in mapping["context_datasource_types"]]:
            return False
    if archetype and mapping.get("context_archetypes"):
        if archetype.lower() not in [a.lower() for a in mapping["context_archetypes"]]:
            return False
    if environment and mapping.get("context_environments"):
        if environment.lower() not in [e.lower() for e in mapping["context_environments"]]:
            return False
    return True


_PROMETHEUS_DATASOURCE_TYPES = {"prometheus", "mimir", "cortex", "thanos"}
_SIGNALFX_DATASOURCE_TYPES = {"signalfx", "grafana-signalfx-datasource"}


def _datasource_type_matches(candidate: str, requested: str) -> bool:
    candidate = (candidate or "").lower()
    requested = (requested or "").lower()
    if not requested:
        return True
    if candidate == requested:
        return True
    if candidate in _PROMETHEUS_DATASOURCE_TYPES and requested in _PROMETHEUS_DATASOURCE_TYPES:
        return True
    if candidate in _SIGNALFX_DATASOURCE_TYPES and requested in _SIGNALFX_DATASOURCE_TYPES:
        return True
    return False


def _missing_context_multiplier(
    mapping: dict[str, Any],
    service: str = "",
    datasource_type: str = "",
    archetype: str = "",
    environment: str = "",
) -> float:
    """Return a ranking penalty when constrained mapping context is absent.

    Context-specific mappings are intentionally not filtered when the caller
    has no context (notably ingestion-time inference). Penalizing them preserves
    recall while keeping global mappings ahead when there is no evidence that
    the service/datasource/archetype/environment constraint applies.
    """
    missing_context = (
        (not service and bool(mapping.get("context_services")))
        or (not datasource_type and bool(mapping.get("context_datasource_types")))
        or (not archetype and bool(mapping.get("context_archetypes")))
        or (not environment and bool(mapping.get("context_environments")))
    )
    return _CONTEXT_MISSING_PENALTY if missing_context else 1.0


def _effective_confidence(
    mapping: dict[str, Any],
    now: float,
    *,
    context_service: str = "",
    context_datasource_type: str = "",
    context_archetype: str = "",
    context_environment: str = "",
    apply_context_penalty: bool = True,
) -> float:
    """Compute effective confidence with time decay, feedback, and context adjustment.

    - Confidence decays with a half-life based on time since last_seen.
    - Positive feedback boosts, negative feedback penalizes.
    - Context-specific mappings get a ranking penalty when caller context is missing.
    - Bootstrap mappings don't decay (they're canonical starting points).
    """
    base = mapping["confidence"]

    context_multiplier = (
        _missing_context_multiplier(
            mapping,
            context_service,
            context_datasource_type,
            context_archetype,
            context_environment,
        )
        if apply_context_penalty
        else 1.0
    )

    # Bootstrap mappings don't decay, but still receive context ranking penalties.
    if mapping.get("source_type") == "bootstrap":
        return max(base * context_multiplier, _MIN_CONFIDENCE)

    # Time decay
    last_seen = mapping.get("last_seen", now)
    age_days = (now - last_seen) / 86400.0
    if age_days > 0:
        import math

        decay = math.pow(0.5, age_days / _DECAY_HALF_LIFE_DAYS)
        base *= decay

    # Feedback adjustment
    pos = mapping.get("positive_feedback", 0)
    neg = mapping.get("negative_feedback", 0)
    total_fb = pos + neg
    if total_fb > 0:
        fb_ratio = pos / total_fb  # 0.0 (all negative) to 1.0 (all positive)
        # Scale: 0.7x at all-negative, 1.3x at all-positive, 1.0x at balanced
        fb_multiplier = 0.7 + 0.6 * fb_ratio
        base *= fb_multiplier

    base *= context_multiplier

    return max(base, _MIN_CONFIDENCE)


def _metric_matches_pattern(metric_name: str, pattern: str) -> bool:
    """Check if a metric name matches a pattern.

    Supports:
    - Exact match: "http_request_duration_seconds"
    - Glob patterns: "*_latency_*", "sso_*_total"
    - Suffix match: "*_duration_seconds"
    """
    if pattern == metric_name:
        return True
    if "*" in pattern or "?" in pattern:
        return fnmatch.fnmatch(metric_name, pattern)
    # Substring match as fallback
    return pattern in metric_name


# ── Singleton ────────────────────────────────────────────────────────────────

_store: SignalStore | None = None


def get_signal_store() -> SignalStore:
    """Get or create the global SignalStore singleton."""
    global _store
    if _store is None:
        _store = SignalStore()
        # Auto-load bootstrap signals on first access
        _store.load_from_yaml()
    return _store
