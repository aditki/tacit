"""Dashboard ingestion — learn operational patterns from existing dashboards.

Vendor-agnostic: each DashboardBackend implements ``ingest_dashboard()``
which returns a common ``DashboardFeatures`` dataclass.  This module handles
the vendor-independent parts: signal inference, archetype generation, and
signal store persistence.

Per-backend parsers extract:
- Metric names from queries (PromQL, SignalFlow, LogQL, CloudWatch, etc.)
- Panel titles and descriptions
- Row/section groupings
- Metric co-occurrence patterns (which metrics appear together)
- Aggregation patterns (rate, histogram_quantile, .percentile, etc.)
- Query transformations (the raw query templates)
- Dashboard tags
- Alert rule links
- Drilldown links to other dashboards

Then infers signal types by matching extracted metrics against the signal
store's taxonomy, and optionally auto-generates an archetype YAML snippet.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

import structlog

from dashforge.signals import get_signal_store

logger = structlog.get_logger()

# ── PromQL metric name extraction ────────────────────────────────────────────
# Matches metric names in PromQL: word chars before { or [ or (
# Also handles rate(metric_name{...}[5m]), histogram_quantile, etc.
_PROMQL_METRIC_RE = re.compile(
    r'(?:^|(?<=[\s(,]))([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{|\[|$)',
    re.MULTILINE,
)

# PromQL functions/keywords to exclude from metric extraction
_PROMQL_FUNCS = frozenset({
    "sum", "avg", "min", "max", "count", "count_values", "bottomk", "topk",
    "quantile", "stddev", "stdvar", "group",
    "rate", "irate", "increase", "delta", "idelta", "deriv", "predict_linear",
    "histogram_quantile", "histogram_count", "histogram_sum",
    "absent", "absent_over_time", "changes", "resets",
    "ceil", "floor", "round", "clamp", "clamp_min", "clamp_max",
    "label_replace", "label_join", "sort", "sort_desc",
    "time", "timestamp", "vector", "scalar",
    "year", "month", "day_of_month", "day_of_week", "hour", "minute",
    "days_in_month",
    "exp", "ln", "log2", "log10", "sqrt", "sgn", "abs",
    "on", "ignoring", "by", "without", "bool", "offset",
    "and", "or", "unless",
    "le", "inf", "nan",
})

# Aggregation function patterns
_AGG_PATTERN = re.compile(
    r'(sum|avg|min|max|count|topk|bottomk|quantile)\s*\(\s*'
    r'(?:(\d+(?:\.\d+)?)\s*,\s*)?'
    r'(rate|irate|increase|delta|histogram_quantile)?\s*\('
)

# histogram_quantile pattern
_HISTOGRAM_PATTERN = re.compile(
    r'histogram_quantile\(\s*([\d.]+)'
)


def extract_metrics_from_promql(expr: str) -> list[str]:
    """Extract metric names from a PromQL expression."""
    candidates = _PROMQL_METRIC_RE.findall(expr)
    metrics = []
    for name in candidates:
        name_lower = name.lower()
        if name_lower not in _PROMQL_FUNCS and not name.startswith("__"):
            metrics.append(name)
    return list(dict.fromkeys(metrics))  # dedupe preserving order


def extract_aggregation_patterns(expr: str) -> list[dict[str, str]]:
    """Extract aggregation function usage from a PromQL expression."""
    patterns = []

    # Aggregation wrapping rate/increase
    for match in _AGG_PATTERN.finditer(expr):
        agg_func = match.group(1)
        inner_func = match.group(3) or ""
        patterns.append({
            "aggregation": agg_func,
            "inner_function": inner_func,
        })

    # histogram_quantile
    for match in _HISTOGRAM_PATTERN.finditer(expr):
        patterns.append({
            "aggregation": "histogram_quantile",
            "quantile": match.group(1),
        })

    # Also emit rate/increase/etc. as their own pattern entries
    # (even when wrapped inside sum/avg — both are useful features)
    for func in ("rate", "irate", "increase", "delta"):
        if f"{func}(" in expr and not any(
            p.get("aggregation") == func for p in patterns
        ):
            patterns.append({"aggregation": func})

    return patterns


# ── Dashboard JSON parsing ───────────────────────────────────────────────────

def _extract_panel_data(panel: dict[str, Any]) -> dict[str, Any] | None:
    """Extract relevant data from a single Grafana panel JSON."""
    panel_type = panel.get("type", "")
    title = panel.get("title", "")

    # Skip row panels, text panels, etc.
    if panel_type in ("row", "text", "news", "dashlist", ""):
        return None

    queries = []
    metrics = []
    agg_patterns = []
    datasource_type = ""

    # Extract from targets (query definitions)
    for target in panel.get("targets", []):
        expr = target.get("expr", "") or target.get("query", "") or ""
        if not expr:
            # CloudWatch uses different fields
            cw_metric = target.get("metricName", "")
            cw_ns = target.get("namespace", "")
            if cw_metric:
                metric_name = f"{cw_ns}/{cw_metric}" if cw_ns else cw_metric
                metrics.append(metric_name)
                datasource_type = "cloudwatch"
            continue

        queries.append(expr)
        extracted = extract_metrics_from_promql(expr)
        metrics.extend(extracted)
        agg_patterns.extend(extract_aggregation_patterns(expr))

        # Detect datasource type from target
        ds = target.get("datasource", {})
        if isinstance(ds, dict):
            datasource_type = ds.get("type", datasource_type)

    if not metrics and not queries:
        return None

    # Detect datasource from panel level
    panel_ds = panel.get("datasource", {})
    if isinstance(panel_ds, dict) and not datasource_type:
        datasource_type = panel_ds.get("type", "")

    return {
        "title": title,
        "description": panel.get("description", ""),
        "panel_type": panel_type,
        "metrics": list(dict.fromkeys(metrics)),
        "queries": queries,
        "aggregation_patterns": agg_patterns,
        "datasource_type": datasource_type,
        "unit": panel.get("fieldConfig", {}).get("defaults", {}).get("unit", ""),
    }


def parse_dashboard_json(dashboard_json: dict[str, Any]) -> dict[str, Any]:
    """Parse a full Grafana dashboard JSON and extract operational features.

    Returns a structured dict with all extracted features suitable for
    signal inference and archetype generation.
    """
    dashboard = dashboard_json.get("dashboard", dashboard_json)

    title = dashboard.get("title", "")
    tags = dashboard.get("tags", [])
    uid = dashboard.get("uid", "")

    # Flatten panels (handle nested row panels + non-collapsed row context)
    all_panels = []
    current_row = ""
    for panel in dashboard.get("panels", []):
        if panel.get("type") == "row":
            current_row = panel.get("title", "")
            # Collapsed rows have their panels nested inside
            for sub in panel.get("panels", []):
                data = _extract_panel_data(sub)
                if data:
                    data["row"] = current_row
                    all_panels.append(data)
        else:
            data = _extract_panel_data(panel)
            if data:
                data["row"] = current_row
                all_panels.append(data)

    # Collect all metrics
    all_metrics = []
    for p in all_panels:
        all_metrics.extend(p["metrics"])
    unique_metrics = list(dict.fromkeys(all_metrics))

    # Row groups
    row_groups = defaultdict(list)
    for p in all_panels:
        row = p.get("row", "") or "ungrouped"
        row_groups[row].append(p["title"])
    row_groups_list = [
        {"row": row, "panels": panels}
        for row, panels in row_groups.items()
    ]

    # Metric co-occurrence: for each metric, which other metrics appear in the same dashboard
    cooccurrence: dict[str, list[str]] = {}
    for metric in unique_metrics:
        co = [m for m in unique_metrics if m != metric]
        if co:
            cooccurrence[metric] = co

    # Aggregation patterns across all panels
    all_agg_patterns = []
    for p in all_panels:
        for agg in p.get("aggregation_patterns", []):
            agg["panel_title"] = p["title"]
            # Find the metric this aggregation applies to
            if p["metrics"]:
                agg["metric"] = p["metrics"][0]
            all_agg_patterns.append(agg)

    # All query transformations
    all_queries = []
    for p in all_panels:
        for q in p.get("queries", []):
            all_queries.append(q)

    # Panel titles
    panel_titles = [p["title"] for p in all_panels if p["title"]]

    # Alert links — look for alert annotations and panel alert rules
    alert_links = []
    annotations = dashboard.get("annotations", {}).get("list", [])
    for ann in annotations:
        if "alert" in ann.get("name", "").lower():
            alert_links.append(ann.get("name", ""))

    # Drilldown links — look for panel links and dashboard links
    drilldown_links = []
    for link in dashboard.get("links", []):
        if link.get("type") == "dashboards":
            drilldown_links.extend(link.get("tags", []))
        elif link.get("type") == "link":
            drilldown_links.append(link.get("url", ""))

    return {
        "dashboard_uid": uid,
        "dashboard_title": title,
        "dashboard_tags": tags,
        "metrics_found": unique_metrics,
        "panel_count": len(all_panels),
        "row_groups": row_groups_list,
        "metric_cooccurrence": cooccurrence,
        "aggregation_patterns": all_agg_patterns,
        "query_transformations": all_queries,
        "panel_titles": panel_titles,
        "alert_links": alert_links,
        "drilldown_links": drilldown_links,
        "panels": all_panels,
    }


# ── Signal inference ─────────────────────────────────────────────────────────

def infer_signals_from_metrics(
    metrics: list[str],
    panel_data: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Infer semantic signal types from a list of extracted metrics.

    Uses the signal store's existing mappings + heuristic patterns.
    Returns a list of {signal_type, metric, confidence, reason}.
    """
    store = get_signal_store()
    all_signal_types = store.list_signal_types()
    inferred: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for metric in metrics:
        for st in all_signal_types:
            signal_type = st["signal_type"]
            mappings = store.get_mappings_for_signal(signal_type)
            for mapping in mappings:
                from dashforge.signals import _metric_matches_pattern
                if _metric_matches_pattern(metric, mapping["metric_pattern"]):
                    key = (signal_type, metric)
                    if key not in seen:
                        seen.add(key)
                        inferred.append({
                            "signal_type": signal_type,
                            "metric": metric,
                            "confidence": mapping.get("effective_confidence",
                                                       mapping["confidence"]),
                            "reason": f"matches pattern '{mapping['metric_pattern']}'",
                        })

    inferred.sort(key=lambda x: x["confidence"], reverse=True)
    return inferred


# ── Archetype generation ─────────────────────────────────────────────────────

_TEMPLATE_PLACEHOLDERS = re.compile(
    r"\{(service_filter|container_filter|rate_interval)\}"
)


def _escape_literal_braces(expr: str) -> str:
    """Escape literal ``{``/``}`` in a concrete query so that
    ``str.format()`` in ``compile_archetype()`` treats them as text.

    Known template placeholders (``{service_filter}``, etc.) are preserved.
    """
    # First, escape ALL braces
    escaped = expr.replace("{", "{{").replace("}", "}}")
    # Then un-escape known template placeholders (they became doubled)
    escaped = re.sub(
        r"\{\{(service_filter|container_filter|rate_interval)\}\}",
        r"{\1}",
        escaped,
    )
    return escaped


def generate_archetype_yaml(
    extracted: dict[str, Any],
    signals: list[dict[str, Any]],
    archetype_id: str = "",
) -> str:
    """Generate an archetype YAML snippet from extracted dashboard features.

    This is a suggestion — engineers should review and customize before
    activating.
    """
    import yaml

    title = extracted["dashboard_title"]
    if not archetype_id:
        # Generate ID from title
        archetype_id = re.sub(r'[^a-z0-9]+', '_', title.lower()).strip('_')

    # Derive problem_types from tags + title
    problem_types = [archetype_id]
    for tag in extracted.get("dashboard_tags", []):
        clean = re.sub(r'[^a-z0-9]+', '_', tag.lower()).strip('_')
        if clean and clean != archetype_id:
            problem_types.append(clean)

    # Build signal bindings from inferred signals
    signal_bindings = {}
    required_signals = []
    for sig in signals:
        if sig["signal_type"] not in signal_bindings:
            signal_bindings[sig["signal_type"]] = sig["metric"]
            required_signals.append(sig["signal_type"])

    # Build panels from extracted panel data
    panels = []
    for p in extracted.get("panels", [])[:12]:  # cap at 12 panels
        queries = []
        for q in p.get("queries", []):
            queries.append({
                "expr": _escape_literal_braces(q),
                "legend_format": "",
            })
        if queries:
            panel_def: dict[str, Any] = {
                "title": p["title"],
                "queries": queries,
            }
            if p.get("row"):
                panel_def["row"] = p["row"]
            if p.get("unit"):
                panel_def["unit"] = p["unit"]
            if p.get("description"):
                panel_def["description"] = p["description"]
            panels.append(panel_def)

    archetype = {
        "id": archetype_id,
        "name": title,
        "description": f"Auto-generated from dashboard '{title}'",
        "problem_types": problem_types,
        "required_metrics": extracted["metrics_found"][:10],
        "required_signals": required_signals[:10],
        "signal_bindings": signal_bindings,
        "tags": extracted.get("dashboard_tags", []) + ["auto-generated"],
        "default_timerange": "1h",
        "panels": panels,
    }

    return yaml.dump(
        {"archetypes": [archetype]},
        default_flow_style=False,
        sort_keys=False,
        width=120,
    )


# ── Full ingestion pipeline ─────────────────────────────────────────────────

def _features_to_dict(features) -> dict[str, Any]:
    """Convert a DashboardFeatures dataclass to a plain dict."""
    from dataclasses import asdict
    return asdict(features)


async def ingest_dashboard(
    dashboard_uid: str,
    backend: Any | None = None,
    backend_name: str = "",
    auto_approve: bool = False,
) -> dict[str, Any]:
    """Full ingestion pipeline: fetch → extract → infer signals → store.

    Vendor-agnostic: delegates to the ``DashboardBackend.ingest_dashboard()``
    method, which handles vendor-specific fetch + parse.  The signal inference
    and archetype generation work against the common ``DashboardFeatures``
    dataclass.

    Parameters
    ----------
    dashboard_uid : str
        Dashboard UID/ID to ingest (interpretation is backend-specific).
    backend : DashboardBackend, optional
        Explicit backend to use. If not provided, iterates over all active
        backends and uses the first one that matches ``backend_name``, or the
        first available backend.
    backend_name : str
        If provided without an explicit ``backend``, selects the backend by
        name (e.g. 'grafana', 'signalfx').
    auto_approve : bool
        If True, automatically approve and create signal mappings.
        If False (default), stores as 'pending' for human review.

    Returns
    -------
    dict with: extracted features, inferred signals, generated archetype YAML,
    and status.
    """
    from dashforge.backends import get_active_backends
    from dashforge.backends.base import DashboardFeatures

    own_backends = False
    if backend is None:
        backends = get_active_backends()
        own_backends = True
        if not backends:
            raise RuntimeError("No active backends configured for dashboard ingestion")

        if backend_name:
            matched = [b for b in backends if b.name == backend_name]
            if not matched:
                available = [b.name for b in backends]
                raise ValueError(
                    f"Backend '{backend_name}' not found. Available: {available}"
                )
            backend = matched[0]
        else:
            backend = backends[0]

    try:
        # Delegate fetch + parse to the backend (vendor-specific)
        features: DashboardFeatures = await backend.ingest_dashboard(dashboard_uid)

        # Everything below is vendor-agnostic
        extracted = _features_to_dict(features)

        # Infer signals
        signals = infer_signals_from_metrics(
            features.metrics_found,
            features.panels,
        )

        # Generate archetype YAML suggestion
        archetype_yaml = generate_archetype_yaml(extracted, signals)

        # Store in signal store
        store = get_signal_store()
        status = "approved" if auto_approve else "pending"

        store.record_ingested_dashboard(
            dashboard_uid=dashboard_uid,
            dashboard_title=features.dashboard_title,
            dashboard_tags=features.dashboard_tags,
            metrics_found=features.metrics_found,
            panel_count=features.panel_count,
            row_groups=features.row_groups,
            metric_cooccurrence=features.metric_cooccurrence,
            aggregation_patterns=features.aggregation_patterns,
            query_transformations=features.query_transformations,
            panel_titles=features.panel_titles,
            alert_links=features.alert_links,
            drilldown_links=features.drilldown_links,
            signals_inferred=[s["signal_type"] for s in signals],
            status=status,
        )

        # If auto-approved, create signal mappings from inferred signals
        if auto_approve:
            mappings_created = 0
            for sig in signals:
                if sig["confidence"] >= 0.5:  # only confident mappings
                    store.add_mapping(
                        signal_type=sig["signal_type"],
                        metric_pattern=sig["metric"],
                        confidence=sig["confidence"],
                        source_type="dashboard_ingest",
                        source_refs=[dashboard_uid],
                    )
                    mappings_created += 1
            logger.info(
                "dashboard_ingested_auto_approved",
                uid=dashboard_uid,
                backend=features.backend_name,
                metrics=len(features.metrics_found),
                signals=len(signals),
                mappings_created=mappings_created,
            )
        else:
            logger.info(
                "dashboard_ingested_pending",
                uid=dashboard_uid,
                backend=features.backend_name,
                metrics=len(features.metrics_found),
                signals=len(signals),
            )

        return {
            "dashboard_uid": dashboard_uid,
            "dashboard_title": features.dashboard_title,
            "backend": features.backend_name,
            "query_language": features.query_language,
            "status": status,
            "metrics_found": features.metrics_found,
            "panel_count": features.panel_count,
            "row_groups": features.row_groups,
            "metric_cooccurrence": features.metric_cooccurrence,
            "aggregation_patterns": features.aggregation_patterns,
            "panel_titles": features.panel_titles,
            "alert_links": features.alert_links,
            "drilldown_links": features.drilldown_links,
            "signals_inferred": signals,
            "archetype_yaml": archetype_yaml,
        }

    finally:
        if own_backends:
            await backend.close()
