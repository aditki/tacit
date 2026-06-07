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

import promql_parser
import structlog

from dashforge.signals import get_signal_store

logger = structlog.get_logger()

# ── PromQL metric name extraction ────────────────────────────────────────────
# Matches metric names in PromQL: word chars that look like identifiers.
# A metric name starts with [a-zA-Z_:] and continues with [a-zA-Z0-9_:].
# It can appear before {, [, ), whitespace, operators, comparisons, or EOL.
_PROMQL_METRIC_RE = re.compile(
    r"(?:^|(?<=[\s(,]))([a-zA-Z_:][a-zA-Z0-9_:]*)(?=\s*[{\[)\/\*\+\-><=!,]|\s|$)",
    re.MULTILINE,
)

# PromQL functions/keywords to exclude from metric extraction
_PROMQL_FUNCS = frozenset(
    {
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "count_values",
        "bottomk",
        "topk",
        "quantile",
        "stddev",
        "stdvar",
        "group",
        "rate",
        "irate",
        "increase",
        "delta",
        "idelta",
        "deriv",
        "predict_linear",
        "histogram_quantile",
        "histogram_count",
        "histogram_sum",
        "absent",
        "absent_over_time",
        "changes",
        "resets",
        "ceil",
        "floor",
        "round",
        "clamp",
        "clamp_min",
        "clamp_max",
        "label_replace",
        "label_join",
        "sort",
        "sort_desc",
        "time",
        "timestamp",
        "vector",
        "scalar",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "hour",
        "minute",
        "days_in_month",
        "exp",
        "ln",
        "log2",
        "log10",
        "sqrt",
        "sgn",
        "abs",
        "on",
        "ignoring",
        "by",
        "without",
        "bool",
        "offset",
        "and",
        "or",
        "unless",
        "le",
        "inf",
        "nan",
    }
)

# Aggregation function patterns
_AGG_PATTERN = re.compile(
    r"(sum|avg|min|max|count|topk|bottomk|quantile)\s*\(\s*"
    r"(?:(\d+(?:\.\d+)?)\s*,\s*)?"
    r"(rate|irate|increase|delta|histogram_quantile)?\s*\("
)

# histogram_quantile pattern
_HISTOGRAM_PATTERN = re.compile(r"histogram_quantile\(\s*([\d.]+)")

_PROMQL_LABEL_LIST_RE = re.compile(r"\b(by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)")


def _strip_promql_label_lists(expr: str) -> str:
    """Remove bare label lists so they are not mistaken for metrics."""
    return _PROMQL_LABEL_LIST_RE.sub(lambda m: f"{m.group(1)} ()", expr)


def _extract_metrics_from_promql_regex(expr: str) -> list[str]:
    candidates = _PROMQL_METRIC_RE.findall(_strip_promql_label_lists(expr))
    metrics = []
    for name in candidates:
        name_lower = name.lower()
        if name_lower not in _PROMQL_FUNCS and not name.startswith("__"):
            metrics.append(name)
    return list(dict.fromkeys(metrics))


def _walk_promql_ast(node: Any, metrics: list[str]) -> None:
    if node is None:
        return

    node_type = type(node).__name__

    if node_type == "VectorSelector":
        name = getattr(node, "name", None)
        if isinstance(name, str) and name and name.lower() not in _PROMQL_FUNCS and not name.startswith("__"):
            metrics.append(name)
        return

    if node_type == "MatrixSelector":
        _walk_promql_ast(getattr(node, "vs", None) or getattr(node, "vector_selector", None), metrics)
        return

    if node_type in {"AggregateExpr", "UnaryExpr", "ParenExpr", "SubqueryExpr", "StepInvariantExpr"}:
        _walk_promql_ast(getattr(node, "expr", None), metrics)
        return

    if node_type == "BinaryExpr":
        _walk_promql_ast(getattr(node, "lhs", None), metrics)
        _walk_promql_ast(getattr(node, "rhs", None), metrics)
        return

    if node_type == "Call":
        for arg in getattr(node, "args", []) or []:
            _walk_promql_ast(arg, metrics)
        return

    for attr in ("expr", "lhs", "rhs", "vs", "vector_selector"):
        child = getattr(node, attr, None)
        if child is not None:
            _walk_promql_ast(child, metrics)

    for child in getattr(node, "args", []) or []:
        _walk_promql_ast(child, metrics)


def extract_metrics_from_promql(expr: str) -> list[str]:
    """Extract metric names from a PromQL expression."""
    try:
        ast = promql_parser.parse(expr)
    except Exception:
        return _extract_metrics_from_promql_regex(expr)

    metrics: list[str] = []
    _walk_promql_ast(ast, metrics)
    return list(dict.fromkeys(metrics))


def extract_aggregation_patterns(expr: str) -> list[dict[str, str]]:
    """Extract aggregation function usage from a PromQL expression."""
    patterns = []

    # Aggregation wrapping rate/increase
    for match in _AGG_PATTERN.finditer(expr):
        agg_func = match.group(1)
        inner_func = match.group(3) or ""
        patterns.append(
            {
                "aggregation": agg_func,
                "inner_function": inner_func,
            }
        )

    # histogram_quantile
    for match in _HISTOGRAM_PATTERN.finditer(expr):
        patterns.append(
            {
                "aggregation": "histogram_quantile",
                "quantile": match.group(1),
            }
        )

    # Also emit rate/increase/etc. as their own pattern entries
    # (even when wrapped inside sum/avg — both are useful features)
    for func in ("rate", "irate", "increase", "delta"):
        if f"{func}(" in expr and not any(p.get("aggregation") == func for p in patterns):
            patterns.append({"aggregation": func})

    return patterns


# ── Dashboard JSON parsing ───────────────────────────────────────────────────


def _datasource_type_to_language(ds_type: str) -> str:
    """Map a Grafana datasource type to its query language.

    Defaults to ``promql`` when the type is unknown or empty, but recognizes the
    common non-Prometheus backends so ingestion can preserve their queries
    verbatim instead of mis-parsing them as PromQL.
    """
    t = (ds_type or "").lower()
    if not t:
        return "promql"
    exact = {
        "prometheus": "promql",
        "mimir": "promql",
        "cortex": "promql",
        "thanos": "promql",
        "loki": "logql",
        "cloudwatch": "cloudwatch",
        "signalfx": "signalflow",
        "elasticsearch": "lucene",
        "opensearch": "lucene",
        "graphite": "graphite",
        "influxdb": "influxql",
    }
    if t in exact:
        return exact[t]
    for needle, lang in (
        ("prometheus", "promql"),
        ("signalfx", "signalflow"),
        ("loki", "logql"),
        ("cloudwatch", "cloudwatch"),
        ("elasticsearch", "lucene"),
        ("opensearch", "lucene"),
        ("graphite", "graphite"),
        ("influx", "influxql"),
    ):
        if needle in t:
            return lang
    return "promql"


def _language_to_datasource_type(language: str) -> str:
    """Best-effort inverse of :func:`_datasource_type_to_language` for tagging."""
    return {
        "promql": "prometheus",
        "logql": "loki",
        "cloudwatch": "cloudwatch",
        "signalflow": "signalfx",
        "lucene": "elasticsearch",
        "graphite": "graphite",
        "influxql": "influxdb",
    }.get(language, "prometheus")


def _extract_panel_data(panel: dict[str, Any]) -> dict[str, Any] | None:
    """Extract relevant data from a single Grafana panel JSON.

    Per-language aware: PromQL metric extraction and aggregation parsing only run
    on Prometheus-family targets. Non-PromQL queries (LogQL, SignalFlow, etc.)
    are preserved verbatim, and CloudWatch targets are captured as structured
    templates (namespace / metric / stat / region / dimensions).
    """
    panel_type = panel.get("type", "")
    title = panel.get("title", "")

    # Skip row panels, text panels, etc.
    if panel_type in ("row", "text", "news", "dashlist", ""):
        return None

    panel_ds = panel.get("datasource", {})
    panel_ds_type = panel_ds.get("type", "") if isinstance(panel_ds, dict) else ""

    queries = []
    metrics = []
    agg_patterns = []
    cloudwatch_targets: list[dict[str, Any]] = []
    datasource_type = ""

    # Extract from targets (query definitions)
    for target in panel.get("targets", []):
        t_ds = target.get("datasource", {})
        t_ds_type = t_ds.get("type", "") if isinstance(t_ds, dict) else ""
        eff_ds = t_ds_type or panel_ds_type
        language = _datasource_type_to_language(eff_ds)

        expr = target.get("expr", "") or target.get("query", "") or ""
        if expr:
            queries.append(expr)
            if language == "promql":
                metrics.extend(extract_metrics_from_promql(expr))
                agg_patterns.extend(extract_aggregation_patterns(expr))
            elif language == "signalflow":
                # SignalFlow has its own metric grammar; reuse the SignalFx
                # extractor rather than the PromQL one.
                try:
                    from dashforge.backends.signalfx import _extract_metrics_from_signalflow

                    metrics.extend(_extract_metrics_from_signalflow(expr))
                except Exception:  # pragma: no cover - defensive
                    pass
            # Other languages (LogQL, etc.): preserve the query, no PromQL parse.
            if eff_ds:
                datasource_type = eff_ds
        else:
            # CloudWatch-style structured target (no expr/query string).
            cw_metric = target.get("metricName", "")
            if cw_metric:
                cw_ns = target.get("namespace", "")
                stat = target.get("statistic", "") or (target.get("statistics") or [""])[0] or ""
                region = target.get("region", "")
                dimensions = target.get("dimensions", {}) or {}
                metric_name = f"{cw_ns}/{cw_metric}" if cw_ns else cw_metric
                metrics.append(metric_name)
                cloudwatch_targets.append(
                    {
                        "namespace": cw_ns,
                        "metric_name": cw_metric,
                        "stat": stat,
                        "region": region,
                        "dimensions": dimensions,
                    }
                )
                datasource_type = eff_ds or "cloudwatch"

    if not metrics and not queries and not cloudwatch_targets:
        return None

    if not datasource_type:
        datasource_type = panel_ds_type
    query_language = _datasource_type_to_language(datasource_type)

    # Per-panel drilldown links (panel.links). These attach navigation paths
    # at the panel level and are distinct from dashboard-level links.
    panel_links = []
    links = panel.get("links", [])
    if not isinstance(links, list):
        links = []
    for link in links:
        if not isinstance(link, dict):
            continue
        link_title = link.get("title", "")
        link_url = link.get("url", "")
        if link_title or link_url:
            panel_links.append({"title": link_title, "url": link_url})

    return {
        "title": title,
        "description": panel.get("description", ""),
        "panel_type": panel_type,
        "metrics": list(dict.fromkeys(metrics)),
        "queries": queries,
        "aggregation_patterns": agg_patterns,
        "datasource_type": datasource_type,
        "query_language": query_language,
        "cloudwatch_targets": cloudwatch_targets,
        "unit": panel.get("fieldConfig", {}).get("defaults", {}).get("unit", ""),
        "links": panel_links,
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
    row_groups_list = [{"row": row, "panels": panels} for row, panels in row_groups.items()]

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
    dashboard_links = dashboard.get("links", [])
    if not isinstance(dashboard_links, list):
        dashboard_links = []
    for link in dashboard_links:
        if not isinstance(link, dict):
            continue
        if link.get("type") == "dashboards":
            drilldown_links.extend(link.get("tags", []))
        elif link.get("type") == "link":
            url = link.get("url", "")
            if url:
                drilldown_links.append(url)

    # Per-panel drilldown links (panel.links) captured in _extract_panel_data.
    # Fold them into the aggregate so navigation paths aren't discarded.
    for p in all_panels:
        for link in p.get("links", []):
            target = link.get("url") or link.get("title")
            if target and target not in drilldown_links:
                drilldown_links.append(target)

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
                        inferred.append(
                            {
                                "signal_type": signal_type,
                                "metric": metric,
                                "confidence": mapping.get("effective_confidence", mapping["confidence"]),
                                "reason": f"matches pattern '{mapping['metric_pattern']}'",
                            }
                        )

    inferred.sort(key=lambda x: x["confidence"], reverse=True)
    return inferred


# ── Archetype generation ─────────────────────────────────────────────────────

_TEMPLATE_PLACEHOLDER_NAMES = ("service_filter", "container_filter", "rate_interval")


def _escape_literal_braces(expr: str) -> str:
    """Escape literal ``{``/``}`` in a concrete query for later ``str.format``.

    Generated archetype YAML can contain two different kinds of braces:

    * concrete query label selectors, e.g. ``{service="api"}``, which must be
      escaped so ``str.format(**params)`` treats them as literal braces; and
    * DashForge template placeholders, e.g. ``{service_filter}`` and the
      PromQL label-selector form ``{{{service_filter}}}``, which must remain
      format placeholders.
    """
    protected: dict[str, str] = {}

    def protect(value: str) -> str:
        token = f"__DASHFORGE_FMT_TOKEN_{len(protected)}__"
        protected[token] = value
        return token

    # Protect the triple-brace label-selector placeholders first so the inner
    # ``{service_filter}`` match below cannot partially consume them.
    for name in _TEMPLATE_PLACEHOLDER_NAMES:
        expr = expr.replace(f"{{{{{{{name}}}}}}}", protect(f"{{{{{{{name}}}}}}}"))

    # Protect simple placeholders such as ``{rate_interval}``.
    for name in _TEMPLATE_PLACEHOLDER_NAMES:
        expr = expr.replace(f"{{{name}}}", protect(f"{{{name}}}"))

    escaped = expr.replace("{", "{{").replace("}", "}}")
    for token, value in protected.items():
        escaped = escaped.replace(token, value)
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
        archetype_id = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")

    # Derive problem_types from tags + title
    problem_types = [archetype_id]
    for tag in extracted.get("dashboard_tags", []):
        clean = re.sub(r"[^a-z0-9]+", "_", tag.lower()).strip("_")
        if clean and clean != archetype_id:
            problem_types.append(clean)

    # Build signal bindings from inferred signals
    signal_bindings = {}
    required_signals = []
    for sig in signals:
        if sig["signal_type"] not in signal_bindings:
            signal_bindings[sig["signal_type"]] = sig["metric"]
            required_signals.append(sig["signal_type"])

    # Dashboard-level language is only a fallback for panels that don't carry
    # their own (e.g. SignalFx-direct ingestion where every panel is signalflow).
    dashboard_language = extracted.get("query_language", "promql")

    # Build panels from extracted panel data
    panels = []
    for p in extracted.get("panels", [])[:12]:  # cap at 12 panels
        # Per-panel language: panel tag → datasource-type mapping → dashboard default.
        panel_language = p.get("query_language")
        if not panel_language:
            ds_type = p.get("datasource_type", "")
            panel_language = _datasource_type_to_language(ds_type) if ds_type else dashboard_language

        queries = []
        cloudwatch_targets = p.get("cloudwatch_targets") or []
        if panel_language == "cloudwatch" and cloudwatch_targets:
            # Preserve the structured CloudWatch query so the panel isn't dropped.
            for ct in cloudwatch_targets:
                queries.append(
                    {
                        "expr": ct.get("metric_name", ""),
                        "query_language": "cloudwatch",
                        "datasource_type": "cloudwatch",
                        "cloudwatch_namespace": ct.get("namespace", ""),
                        "cloudwatch_stat": ct.get("stat", ""),
                        "cloudwatch_region": ct.get("region", ""),
                        "cloudwatch_dimensions": ct.get("dimensions", {}),
                        "legend_format": "",
                    }
                )
        else:
            for q in p.get("queries", []):
                # Only PromQL needs brace-escaping (literal label selectors must
                # survive str.format at compile time). Non-PromQL queries
                # (SignalFlow, LogQL, …) are preserved verbatim.
                query_def: dict[str, Any] = {
                    "expr": _escape_literal_braces(q) if panel_language == "promql" else q,
                    "legend_format": "",
                    "query_language": panel_language,
                }
                if panel_language != "promql":
                    query_def["datasource_type"] = _language_to_datasource_type(panel_language)
                queries.append(query_def)
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


async def ingest_dashboard_features(
    features: Any,
    *,
    auto_approve: bool = False,
) -> dict[str, Any]:
    """Infer, persist, and optionally approve already-extracted dashboard features."""
    extracted = _features_to_dict(features)

    signals = infer_signals_from_metrics(
        features.metrics_found,
        features.panels,
    )

    archetype_yaml = generate_archetype_yaml(extracted, signals)

    store = get_signal_store()
    status = "approved" if auto_approve else "pending"

    store.record_ingested_dashboard(
        dashboard_uid=features.dashboard_uid,
        backend_name=features.backend_name,
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
        signals_inferred=signals,
        archetype_generated=archetype_yaml,
        status=status,
    )

    mappings_created = 0
    if auto_approve:
        source_ref = (
            f"{features.backend_name}:{features.dashboard_uid}" if features.backend_name else features.dashboard_uid
        )
        for sig in signals:
            if sig["confidence"] >= 0.5:
                store.add_mapping(
                    signal_type=sig["signal_type"],
                    metric_pattern=sig["metric"],
                    confidence=sig["confidence"],
                    source_type="dashboard_ingest",
                    source_refs=[source_ref],
                )
                mappings_created += 1
        logger.info(
            "dashboard_ingested_auto_approved",
            uid=features.dashboard_uid,
            backend=features.backend_name,
            metrics=len(features.metrics_found),
            signals=len(signals),
            mappings_created=mappings_created,
        )
    else:
        logger.info(
            "dashboard_ingested_pending",
            uid=features.dashboard_uid,
            backend=features.backend_name,
            metrics=len(features.metrics_found),
            signals=len(signals),
        )

    result = {
        "dashboard_uid": features.dashboard_uid,
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
    if auto_approve:
        result["mappings_created"] = mappings_created
    return result


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

    all_backends: list[Any] = []
    own_backends = False
    if backend is None:
        all_backends = get_active_backends()
        own_backends = True
        if not all_backends:
            raise RuntimeError("No active backends configured for dashboard ingestion")

        if backend_name:
            matched = [b for b in all_backends if b.name == backend_name]
            if not matched:
                available = [b.name for b in all_backends]
                # Close all backends before raising
                for b in all_backends:
                    await b.close()
                raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")
            backend = matched[0]
        else:
            backend = all_backends[0]

    try:
        # Delegate fetch + parse to the backend (vendor-specific)
        features: DashboardFeatures = await backend.ingest_dashboard(dashboard_uid)

        return await ingest_dashboard_features(features, auto_approve=auto_approve)

    finally:
        if own_backends:
            for b in all_backends:
                await b.close()
