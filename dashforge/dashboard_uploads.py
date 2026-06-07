"""Uploaded dashboard document parsing.

This module turns vendor-specific dashboard JSON exports into the common
``DashboardFeatures`` shape used by the learning pipeline.  The API layer
should not know how Grafana or SignalFx represent dashboards; it only selects a
parser from this registry and passes the resulting features downstream.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from typing import Any, Protocol

from dashforge.backends.base import DashboardFeatures


class DashboardUploadParser(Protocol):
    """Parser for a vendor-specific uploaded dashboard document."""

    vendor: str

    def parse(self, document: dict[str, Any], *, source_name: str = "") -> DashboardFeatures:
        """Convert an uploaded dashboard document into common dashboard features."""
        ...


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "-", value.strip()).strip("-").lower()
    return slug[:80] or "uploaded-dashboard"


def _stable_uid(document: dict[str, Any], *, title: str = "", source_name: str = "") -> str:
    basis = source_name or title or repr(document)[:2048]
    digest = hashlib.sha256(repr(document).encode("utf-8")).hexdigest()[:10]
    return f"{_slug(basis)}-{digest}"


def _dashboard_language(panels: list[dict[str, Any]], default: str) -> str:
    languages = {p.get("query_language", "") for p in panels if p.get("query_language")}
    if not languages:
        return default
    if len(languages) == 1:
        return next(iter(languages))
    return "mixed"


class GrafanaDashboardUploadParser:
    """Parse Grafana dashboard JSON exports without contacting Grafana."""

    vendor = "grafana_json"

    def parse(self, document: dict[str, Any], *, source_name: str = "") -> DashboardFeatures:
        from dashforge.dashboard_ingest import parse_dashboard_json

        extracted = parse_dashboard_json(document)
        dashboard = document.get("dashboard", document)
        title = extracted["dashboard_title"] or dashboard.get("title", "") or source_name
        uid = (
            extracted["dashboard_uid"]
            or dashboard.get("uid", "")
            or _stable_uid(document, title=title, source_name=source_name)
        )

        return DashboardFeatures(
            dashboard_uid=uid,
            dashboard_title=title,
            dashboard_tags=extracted["dashboard_tags"],
            backend_name=self.vendor,
            query_language=_dashboard_language(extracted["panels"], "promql"),
            metrics_found=extracted["metrics_found"],
            panel_count=extracted["panel_count"],
            panel_titles=extracted["panel_titles"],
            row_groups=extracted["row_groups"],
            metric_cooccurrence=extracted["metric_cooccurrence"],
            aggregation_patterns=extracted["aggregation_patterns"],
            query_transformations=extracted["query_transformations"],
            alert_links=extracted["alert_links"],
            drilldown_links=extracted["drilldown_links"],
            panels=extracted["panels"],
        )


class SignalFxDashboardUploadParser:
    """Parse SignalFx/Splunk Observability dashboard export JSON.

    Supports documents that include both dashboard metadata and chart objects in
    the same file.  A future live-export command can produce this same shape,
    which keeps uploaded parsing independent from the SignalFx API client.
    """

    vendor = "signalfx_json"

    def parse(self, document: dict[str, Any], *, source_name: str = "") -> DashboardFeatures:
        dashboard = document.get("dashboard", document)
        charts = document.get("charts", [])
        if not charts and isinstance(dashboard, dict):
            charts = dashboard.get("charts", [])
        if isinstance(charts, dict):
            charts = list(charts.values())
        if not isinstance(charts, list):
            charts = []

        features = _parse_signalfx_dashboard_export(dashboard, charts)
        uid = features.dashboard_uid or _stable_uid(document, title=features.dashboard_title, source_name=source_name)
        title = features.dashboard_title or source_name
        return replace(features, dashboard_uid=uid, dashboard_title=title, backend_name=self.vendor)


def _parse_signalfx_dashboard_export(dashboard: dict[str, Any], charts: list[dict[str, Any]]) -> DashboardFeatures:
    from dashforge.backends.signalfx import _extract_metrics_from_signalflow, _extract_signalflow_patterns

    uid = dashboard.get("id", "") or dashboard.get("uid", "")
    title = dashboard.get("name", "") or dashboard.get("title", "")
    tags = dashboard.get("tags", [])
    if not isinstance(tags, list):
        tags = []

    all_metrics: list[str] = []
    panel_titles: list[str] = []
    all_queries: list[str] = []
    all_agg_patterns: list[dict[str, Any]] = []
    panels: list[dict[str, Any]] = []

    for chart in charts:
        if not isinstance(chart, dict):
            continue
        chart_name = chart.get("name", "") or chart.get("title", "")
        options = chart.get("options", {}) if isinstance(chart.get("options", {}), dict) else {}
        program_options = options.get("programOptions", {})
        if not isinstance(program_options, dict):
            program_options = {}
        program_text = program_options.get("programText", "")
        if not program_text:
            program_text = chart.get("programText", "")

        metrics = _extract_metrics_from_signalflow(program_text) if program_text else []
        agg = _extract_signalflow_patterns(program_text) if program_text else []
        for pattern in agg:
            pattern["panel_title"] = chart_name
            if metrics:
                pattern["metric"] = metrics[0]

        if chart_name:
            panel_titles.append(chart_name)
        if program_text:
            all_queries.append(program_text)
            all_metrics.extend(metrics)
            all_agg_patterns.extend(agg)

        if chart_name or program_text:
            panels.append(
                {
                    "title": chart_name,
                    "description": chart.get("description", ""),
                    "panel_type": options.get("type", "TimeSeriesChart"),
                    "metrics": metrics,
                    "queries": [program_text] if program_text else [],
                    "aggregation_patterns": agg,
                    "datasource_type": "signalfx",
                    "query_language": "signalflow",
                    "unit": options.get("unitPrefix", ""),
                    "row": "",
                }
            )

    unique_metrics = list(dict.fromkeys(all_metrics))
    cooccurrence = {m: [x for x in unique_metrics if x != m] for m in unique_metrics}
    cooccurrence = {k: v for k, v in cooccurrence.items() if v}
    row_name = dashboard.get("groupId", "") or "ungrouped"

    return DashboardFeatures(
        dashboard_uid=uid,
        dashboard_title=title,
        dashboard_tags=tags,
        backend_name="signalfx_json",
        query_language="signalflow",
        metrics_found=unique_metrics,
        panel_count=len(panels),
        panel_titles=[t for t in panel_titles if t],
        row_groups=[{"row": row_name, "panels": panel_titles}],
        metric_cooccurrence=cooccurrence,
        aggregation_patterns=all_agg_patterns,
        query_transformations=all_queries,
        alert_links=[],
        drilldown_links=[],
        panels=panels,
    )


_PARSERS: dict[str, DashboardUploadParser] = {
    "grafana": GrafanaDashboardUploadParser(),
    "grafana_json": GrafanaDashboardUploadParser(),
    "signalfx": SignalFxDashboardUploadParser(),
    "signalfx_json": SignalFxDashboardUploadParser(),
}


def get_dashboard_upload_parser(vendor: str) -> DashboardUploadParser:
    """Return the upload parser for a vendor name."""
    key = vendor.strip().lower()
    try:
        return _PARSERS[key]
    except KeyError:
        available = ", ".join(sorted(_PARSERS))
        raise ValueError(f"Unsupported dashboard upload vendor '{vendor}'. Available: {available}")


def parse_uploaded_dashboard(
    document: dict[str, Any],
    *,
    vendor: str = "grafana",
    source_name: str = "",
) -> DashboardFeatures:
    """Parse an uploaded dashboard JSON document into common features."""
    return get_dashboard_upload_parser(vendor).parse(document, source_name=source_name)
