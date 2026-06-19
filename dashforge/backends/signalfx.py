"""SignalFx backend adapter — wraps existing SignalFx client and helpers."""

from __future__ import annotations

import re
from typing import Any, cast

import structlog

from dashforge.backends.base import DashboardFeatures, DiscoveryStatus, PublishResult
from dashforge.models.schemas import DashboardSpec, Intent, MetricEntry
from dashforge.signalfx.client import SignalFxClient
from dashforge.signalfx.discovery import discover_metrics as sfx_discover
from dashforge.signalfx.publisher import publish_dashboard as sfx_publish
from dashforge.validation import validate_signalflow_queries

logger = structlog.get_logger()


class SignalFxBackend:
    """Dashboard backend that talks to Splunk Observability Cloud (SignalFx)."""

    def __init__(self, client: SignalFxClient | None = None):
        self._client = client or SignalFxClient()
        self.last_discovery_status = DiscoveryStatus()

    # ── Protocol properties ───────────────────────────────────────────

    @property
    def name(self) -> str:
        return "signalfx"

    @property
    def query_language(self) -> str:
        return "signalflow"

    # ── Discovery ─────────────────────────────────────────────────────

    async def discover_metrics(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        try:
            entries = await sfx_discover(self._client, keywords)
            self.last_discovery_status = DiscoveryStatus(available=True)
            return entries
        except Exception as exc:
            self.last_discovery_status = DiscoveryStatus(available=False, error=str(exc))
            logger.error("signalfx_discover_failed", error=str(exc), exc_info=True)
            return []

    async def discover_datasource_targets(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        del keywords, intent
        if not self.last_discovery_status.available:
            return []
        return [
            MetricEntry(
                name="",
                datasource_uid="signalfx-direct",
                datasource_name="SignalFx Direct",
                datasource_type="signalfx",
                query_language="signalflow",
            )
        ]

    # ── Validation ────────────────────────────────────────────────────

    async def validate_queries(
        self,
        spec: DashboardSpec,
        catalog: list[MetricEntry] | None = None,
    ) -> tuple[DashboardSpec, list[str]]:
        del catalog  # SignalFlow validation checks metric existence via its own API
        return await validate_signalflow_queries(self._client, spec)

    # ── Publish ───────────────────────────────────────────────────────

    async def publish(
        self,
        spec: DashboardSpec,
    ) -> PublishResult:
        url, uid = await sfx_publish(self._client, spec)
        return PublishResult(url=url, uid=uid, backend_name="signalfx")

    # ── Ingestion ─────────────────────────────────────────────────────

    async def ingest_dashboard(self, uid: str) -> DashboardFeatures:
        dashboard_json = cast(dict[str, Any], await self._client._get(f"/v2/dashboard/{uid}"))
        return await self._parse_sfx_dashboard(dashboard_json)

    async def list_dashboards(self, limit: int = 500) -> list[dict]:
        """List SignalFx dashboards from dashboard groups when available."""
        data = await self._client.list_dashboard_groups(limit=min(limit, 200))
        groups = data.get("results", []) if isinstance(data, dict) else data
        out: list[dict] = []
        for group in groups if isinstance(groups, list) else []:
            if not isinstance(group, dict):
                continue
            dashboards = group.get("dashboardConfigs") or group.get("dashboards", [])
            for dashboard in dashboards if isinstance(dashboards, list) else []:
                if isinstance(dashboard, dict):
                    uid = dashboard.get("dashboardId", "") or dashboard.get("id", "")
                    title = (
                        dashboard.get("name", "") or dashboard.get("dashboardName", "") or dashboard.get("title", "")
                    )
                else:
                    uid = str(dashboard)
                    title = ""
                if not uid:
                    continue
                out.append(
                    {
                        "uid": uid,
                        "title": title,
                        "folder": group.get("name", "") if isinstance(group, dict) else "",
                        "backend": self.name,
                    }
                )
                if len(out) >= limit:
                    return out
        return out

    async def _parse_sfx_dashboard(self, dashboard_json: dict) -> DashboardFeatures:
        """Parse a SignalFx dashboard + its charts into DashboardFeatures."""
        uid = dashboard_json.get("id", "")
        title = dashboard_json.get("name", "")
        tags = dashboard_json.get("tags", [])

        # Fetch all charts referenced by this dashboard
        chart_ids = dashboard_json.get("charts", [])
        # chart_ids may be [{chartId: "...", ...}] or ["chartId", ...]
        resolved_ids = []
        for c in chart_ids:
            if isinstance(c, dict):
                resolved_ids.append(c.get("chartId", ""))
            elif isinstance(c, str):
                resolved_ids.append(c)

        charts = []
        for chart_id in resolved_ids:
            if not chart_id:
                continue
            try:
                chart = cast(dict[str, Any], await self._client._get(f"/v2/chart/{chart_id}"))
                charts.append(chart)
            except Exception:
                logger.warning("sfx_chart_fetch_failed", chart_id=chart_id)

        # Extract features from charts
        all_metrics: list[str] = []
        panel_titles: list[str] = []
        all_queries: list[str] = []
        all_agg_patterns: list[dict] = []
        panels: list[dict] = []

        for chart in charts:
            chart_name = chart.get("name", "")
            panel_titles.append(chart_name)

            # Extract SignalFlow programs from chart options
            options = chart.get("options", {})
            if not isinstance(options, dict):
                options = {}
            program_options = options.get("programOptions", {})
            if not isinstance(program_options, dict):
                program_options = {}
            program_text = program_options.get("programText", "")
            if not program_text:
                # Fallback: some chart types put programs at top level
                program_text = chart.get("programText", "")

            if program_text:
                all_queries.append(program_text)
                metrics = _extract_metrics_from_signalflow(program_text)
                all_metrics.extend(metrics)
                agg = _extract_signalflow_patterns(program_text)
                for a in agg:
                    a["panel_title"] = chart_name
                    if metrics:
                        a["metric"] = metrics[0]
                all_agg_patterns.extend(agg)

            if chart_name or program_text:
                panels.append(
                    {
                        "title": chart_name,
                        "description": chart.get("description", ""),
                        "panel_type": options.get("type", "TimeSeriesChart"),
                        "metrics": list(
                            dict.fromkeys(_extract_metrics_from_signalflow(program_text) if program_text else [])
                        ),
                        "queries": [program_text] if program_text else [],
                        "aggregation_patterns": agg if program_text else [],
                        "datasource_type": "signalfx",
                        "unit": options.get("unitPrefix", ""),
                        "row": "",
                    }
                )

        unique_metrics = list(dict.fromkeys(all_metrics))

        # Metric co-occurrence
        cooccurrence: dict[str, list[str]] = {}
        for m in unique_metrics:
            co = [x for x in unique_metrics if x != m]
            if co:
                cooccurrence[m] = co

        # Chart groups as "rows" (SignalFx groups charts visually)
        group_id = dashboard_json.get("groupId", "")
        row_groups = []
        if group_id:
            row_groups.append({"row": group_id, "panels": panel_titles})
        else:
            row_groups.append({"row": "ungrouped", "panels": panel_titles})

        return DashboardFeatures(
            dashboard_uid=uid,
            dashboard_title=title,
            dashboard_tags=tags,
            backend_name=self.name,
            query_language=self.query_language,
            metrics_found=unique_metrics,
            panel_count=len(charts),
            panel_titles=[t for t in panel_titles if t],
            row_groups=row_groups,
            metric_cooccurrence=cooccurrence,
            aggregation_patterns=all_agg_patterns,
            query_transformations=all_queries,
            alert_links=[],
            drilldown_links=[],
            panels=panels,
        )

    # ── Cleanup ───────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._client.close()


# ── SignalFlow parsing helpers ───────────────────────────────────────────

# data('metric.name', filter=...).publish()
_SFX_DATA_RE = re.compile(
    r"data\(\s*['\"]([^'\"]+)['\"]\s*[,)]",
)

# SignalFlow analytics function patterns
_SFX_FUNC_RE = re.compile(
    r"\.(percentile|mean|sum|count|min|max|stddev|variance"
    r"|top|bottom|sample_stddev|sample_variance"
    r"|mean_plus_stddev|median|timeshift"
    r"|delta|integrate|rate|ewma|double_ewma)\(",
)


def _extract_metrics_from_signalflow(program: str) -> list[str]:
    """Extract metric names from a SignalFlow program.

    SignalFlow uses ``data('metric.name', filter=...).publish()`` syntax.
    """
    return list(dict.fromkeys(_SFX_DATA_RE.findall(program)))


def _extract_signalflow_patterns(program: str) -> list[dict[str, str]]:
    """Extract analytics function usage from a SignalFlow program."""
    patterns = []
    for match in _SFX_FUNC_RE.finditer(program):
        patterns.append({"aggregation": match.group(1)})
    return patterns
