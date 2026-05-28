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
import sqlite3
import time
from contextlib import contextmanager
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
_MIN_CONFIDENCE = 0.05      # floor — never fully forget
_TRUST_THRESHOLD = 0.15     # below this, mapping is excluded from resolution
_CONTEXT_MISSING_PENALTY = 0.7  # lower rank when mapping has context caller lacks


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

CREATE TABLE IF NOT EXISTS ingested_dashboards (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_uid       TEXT NOT NULL UNIQUE,
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
    reviewed_at         REAL
);

CREATE INDEX IF NOT EXISTS idx_smm_signal ON signal_metric_mappings(signal_type);
CREATE INDEX IF NOT EXISTS idx_smm_metric ON signal_metric_mappings(metric_pattern);
CREATE INDEX IF NOT EXISTS idx_ingested_uid ON ingested_dashboards(dashboard_uid);
"""


class SignalStore:
    """SQLite-backed semantic signal mapping store."""

    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or _db_path()
        self._ensure_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self._db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
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
        logger.info("signal_store_init", db_path=str(self._db_path))

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
                       description = excluded.description,
                       category = excluded.category,
                       unit = excluded.unit,
                       updated_at = excluded.updated_at""",
                (signal_type, description, category, unit, now, now),
            )

    def list_signal_types(self) -> list[dict[str, Any]]:
        """List all registered signal types with mapping counts."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT st.*, COUNT(m.id) AS mapping_count
                   FROM signal_types st
                   LEFT JOIN signal_metric_mappings m
                     ON st.signal_type = m.signal_type
                   GROUP BY st.signal_type
                   ORDER BY st.category, st.signal_type"""
            ).fetchall()
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
    ) -> int:
        """Add or update a signal-to-metric mapping. Returns mapping ID."""
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

            cursor = conn.execute(
                """INSERT INTO signal_metric_mappings
                   (signal_type, metric_pattern, confidence,
                    context_services, context_datasource_types,
                    context_environments, context_archetypes,
                    source_type, source_refs, created_at, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(signal_type, metric_pattern) DO UPDATE SET
                       confidence = MAX(excluded.confidence, signal_metric_mappings.confidence),
                       source_refs = excluded.source_refs,
                       last_seen = excluded.last_seen,
                       use_count = signal_metric_mappings.use_count + 1""",
                (
                    signal_type,
                    metric_pattern,
                    confidence,
                    json.dumps(context_services or []),
                    json.dumps(context_datasource_types or []),
                    json.dumps(context_environments or []),
                    json.dumps(context_archetypes or []),
                    source_type,
                    json.dumps(source_refs or []),
                    now,
                    now,
                ),
            )
            return cursor.lastrowid or 0

    def record_feedback(
        self, signal_type: str, metric_pattern: str, positive: bool
    ) -> None:
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
            if not _context_matches(m, context_service, context_datasource_type,
                                    context_archetype, context_environment):
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
            if not include_decayed and effective < _TRUST_THRESHOLD:
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
    ) -> list[tuple[MetricEntry, float]]:
        """Resolve a semantic signal to actual metrics from the live catalog.

        Returns a list of (MetricEntry, effective_confidence) sorted by
        confidence, considering:
        - Pattern matching against catalog metric names
        - Context filters (service, datasource, archetype, environment)
        - Confidence decay and feedback adjustment

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

        matched: list[tuple[MetricEntry, float]] = []
        seen_metrics: set[str] = set()

        for mapping in mappings:
            pattern = mapping["metric_pattern"]
            eff_conf = mapping["effective_confidence"]

            for entry in catalog:
                if entry.name in seen_metrics:
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
    ) -> dict[str, str]:
        """Resolve signal bindings to metric substitutions for archetype compile.

        Parameters
        ----------
        signal_bindings : dict[str, str]
            Maps signal_type → default_metric_name (from archetype YAML).
        catalog : list[MetricEntry]
            Live metric catalog from datasource discovery.

        Returns
        -------
        dict[str, str]
            Maps default_metric_name → resolved_actual_metric_name.
            Only contains entries where the default metric was NOT found in
            the catalog and a signal-based resolution succeeded.
        """
        catalog_names = {e.name for e in catalog}
        substitutions: dict[str, str] = {}

        for signal_type, default_metric in signal_bindings.items():
            # If the default metric exists in the catalog, no substitution needed
            if default_metric in catalog_names:
                continue

            # Try signal-based resolution
            resolved = self.resolve_signal(
                signal_type,
                catalog,
                context_service=context_service,
                context_datasource_type=context_datasource_type,
                context_archetype=context_archetype,
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

    def load_from_yaml(self, path: Path | None = None) -> int:
        """Load bootstrap signal definitions from signals.yaml.

        Returns the number of mappings loaded.
        """
        import yaml

        if path is None:
            candidates = [
                Path(__file__).resolve().parent.parent / "signals.yaml",
                Path("signals.yaml"),
            ]
            for p in candidates:
                if p.is_file():
                    path = p
                    break
        if path is None or not path.is_file():
            logger.info("signals_yaml_not_found")
            return 0

        with open(path) as f:
            data = yaml.safe_load(f) or {}

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
                    context_datasource_types=mp.get("datasource_types", []),
                    source_type="bootstrap",
                )
                count += 1

        logger.info("signals_loaded_from_yaml", path=str(path), mappings=count)
        return count

    # ── Ingested dashboard records ───────────────────────────────────────

    def record_ingested_dashboard(
        self,
        dashboard_uid: str,
        *,
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
        signals_inferred: list[str] | None = None,
        archetype_generated: str = "",
        status: str = "pending",
    ) -> None:
        """Record features extracted from an ingested dashboard."""
        now = time.time()
        with self._conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO ingested_dashboards
                   (dashboard_uid, dashboard_title, dashboard_tags,
                    metrics_found, panel_count, row_groups,
                    metric_cooccurrence, aggregation_patterns,
                    query_transformations, panel_titles,
                    alert_links, drilldown_links,
                    status, signals_inferred, archetype_generated, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    dashboard_uid,
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

    def get_ingested_dashboard(self, dashboard_uid: str) -> dict[str, Any] | None:
        """Get ingested dashboard record."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM ingested_dashboards WHERE dashboard_uid = ?",
                (dashboard_uid,),
            ).fetchone()
        if row is None:
            return None
        return _deserialize_ingested(row)

    def list_ingested_dashboards(
        self, status: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
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

    def approve_ingested_dashboard(self, dashboard_uid: str) -> bool:
        """Approve a pending ingested dashboard (activates its signal mappings)."""
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE ingested_dashboards SET status = 'approved', reviewed_at = ?
                   WHERE dashboard_uid = ? AND status = 'pending'""",
                (time.time(), dashboard_uid),
            )
            return cursor.rowcount > 0

    # ── Stats ────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        """Summary statistics for the signal store."""
        with self._conn() as conn:
            signal_count = conn.execute(
                "SELECT COUNT(*) FROM signal_types"
            ).fetchone()[0]
            mapping_count = conn.execute(
                "SELECT COUNT(*) FROM signal_metric_mappings"
            ).fetchone()[0]
            ingested_count = conn.execute(
                "SELECT COUNT(*) FROM ingested_dashboards"
            ).fetchone()[0]

            by_source = conn.execute(
                """SELECT source_type, COUNT(*) as n
                   FROM signal_metric_mappings GROUP BY source_type"""
            ).fetchall()

            by_category = conn.execute(
                """SELECT category, COUNT(*) as n
                   FROM signal_types GROUP BY category"""
            ).fetchall()

        return {
            "signal_types": signal_count,
            "metric_mappings": mapping_count,
            "ingested_dashboards": ingested_count,
            "mappings_by_source": {r["source_type"]: r["n"] for r in by_source},
            "signals_by_category": {r["category"]: r["n"] for r in by_category},
        }


# ── Helper functions ─────────────────────────────────────────────────────────

def _deserialize_mapping(row: sqlite3.Row) -> dict[str, Any]:
    """Convert a DB row to a dict with deserialized JSON fields."""
    d = dict(row)
    for field in ("context_services", "context_datasource_types",
                  "context_environments", "context_archetypes", "source_refs"):
        if field in d and isinstance(d[field], str):
            d[field] = json.loads(d[field])
    return d


def _deserialize_ingested(row: sqlite3.Row) -> dict[str, Any]:
    """Convert an ingested dashboard DB row to a dict."""
    d = dict(row)
    for field in ("dashboard_tags", "metrics_found", "row_groups",
                  "metric_cooccurrence", "aggregation_patterns",
                  "query_transformations", "panel_titles",
                  "alert_links", "drilldown_links", "signals_inferred"):
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
) -> float:
    """Compute effective confidence with time decay, feedback, and context adjustment.

    - Confidence decays with a half-life based on time since last_seen.
    - Positive feedback boosts, negative feedback penalizes.
    - Context-specific mappings get a ranking penalty when caller context is missing.
    - Bootstrap mappings don't decay (they're canonical starting points).
    """
    base = mapping["confidence"]

    context_multiplier = _missing_context_multiplier(
        mapping,
        context_service,
        context_datasource_type,
        context_archetype,
        context_environment,
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
