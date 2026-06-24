"""Dashboard feature extraction helpers."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict
from typing import Any

from tacit.query_parsing.languages import datasource_type_to_language
from tacit.query_parsing.promql import extract_aggregation_patterns, extract_metrics_from_promql


def extract_panel_data(panel: dict[str, Any]) -> dict[str, Any] | None:
    """Extract relevant data from a single Grafana panel JSON."""
    panel_type = panel.get("type", "")
    title = panel.get("title", "")

    if panel_type in ("row", "text", "news", "dashlist", ""):
        return None

    panel_ds = panel.get("datasource", {})
    panel_ds_type = panel_ds.get("type", "") if isinstance(panel_ds, dict) else ""

    queries = []
    metrics = []
    agg_patterns = []
    cloudwatch_targets: list[dict[str, Any]] = []
    datasource_type = ""

    for target in panel.get("targets", []):
        t_ds = target.get("datasource", {})
        t_ds_type = t_ds.get("type", "") if isinstance(t_ds, dict) else ""
        eff_ds = t_ds_type or panel_ds_type
        language = datasource_type_to_language(eff_ds)

        expr = target.get("expr", "") or target.get("query", "") or ""
        if expr:
            queries.append(expr)
            if language == "promql":
                metrics.extend(extract_metrics_from_promql(expr))
                agg_patterns.extend(extract_aggregation_patterns(expr))
            elif language == "signalflow":
                try:
                    from tacit.backends.signalfx import _extract_metrics_from_signalflow

                    metrics.extend(_extract_metrics_from_signalflow(expr))
                except Exception:  # pragma: no cover - defensive
                    pass
            if eff_ds:
                datasource_type = eff_ds
        else:
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
    query_language = datasource_type_to_language(datasource_type)

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
    """Parse a full Grafana dashboard JSON and extract operational features."""
    dashboard = dashboard_json.get("dashboard", dashboard_json)

    title = dashboard.get("title", "")
    tags = dashboard.get("tags", [])
    uid = dashboard.get("uid", "")

    all_panels = []
    current_row = ""
    for panel in dashboard.get("panels", []):
        if panel.get("type") == "row":
            current_row = panel.get("title", "")
            for sub in panel.get("panels", []):
                data = extract_panel_data(sub)
                if data:
                    data["row"] = current_row
                    all_panels.append(data)
        else:
            data = extract_panel_data(panel)
            if data:
                data["row"] = current_row
                all_panels.append(data)

    all_metrics = []
    for panel in all_panels:
        all_metrics.extend(panel["metrics"])
    unique_metrics = list(dict.fromkeys(all_metrics))

    row_groups = defaultdict(list)
    for panel in all_panels:
        row = panel.get("row", "") or "ungrouped"
        row_groups[row].append(panel["title"])
    row_groups_list = [{"row": row, "panels": panels} for row, panels in row_groups.items()]

    cooccurrence: dict[str, list[str]] = {}
    for metric in unique_metrics:
        co = [candidate for candidate in unique_metrics if candidate != metric]
        if co:
            cooccurrence[metric] = co

    all_agg_patterns = []
    for panel in all_panels:
        for agg in panel.get("aggregation_patterns", []):
            agg["panel_title"] = panel["title"]
            if panel["metrics"]:
                agg["metric"] = panel["metrics"][0]
            all_agg_patterns.append(agg)

    all_queries = []
    for panel in all_panels:
        for query in panel.get("queries", []):
            all_queries.append(query)

    panel_titles = [panel["title"] for panel in all_panels if panel["title"]]

    alert_links = []
    annotations = dashboard.get("annotations", {}).get("list", [])
    for ann in annotations:
        if "alert" in ann.get("name", "").lower():
            alert_links.append(ann.get("name", ""))

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

    for panel in all_panels:
        for link in panel.get("links", []):
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


def features_to_dict(features: Any) -> dict[str, Any]:
    """Convert a DashboardFeatures dataclass to a plain dict."""
    return asdict(features)
