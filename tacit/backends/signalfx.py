"""SignalFx backend adapter — wraps existing SignalFx client and helpers."""

from __future__ import annotations

import re
from typing import Any, cast

import structlog

from tacit.backends.base import AlertFeatures, DashboardFeatures, DiscoveryStatus, PublishResult
from tacit.config import Settings
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry
from tacit.signalfx.client import SignalFxClient
from tacit.signalfx.discovery import discover_metrics as sfx_discover
from tacit.signalfx.publisher import publish_dashboard as sfx_publish
from tacit.validation import validate_signalflow_queries

logger = structlog.get_logger()
SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE = 200


class SignalFxBackend:
    """Dashboard backend that talks to Splunk Observability Cloud (SignalFx)."""

    def __init__(self, client: SignalFxClient | None = None, runtime_settings: Settings | None = None):
        self._settings = runtime_settings
        self._client = client or SignalFxClient(runtime_settings=runtime_settings)
        self.last_discovery_status = DiscoveryStatus()
        self.last_alert_list_complete = False

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
        group_name = self._settings.signalfx_dashboard_group if self._settings else None
        url, uid = await sfx_publish(self._client, spec, group_name=group_name)
        return PublishResult(url=url, uid=uid, backend_name="signalfx")

    # ── Ingestion ─────────────────────────────────────────────────────

    async def ingest_dashboard(self, uid: str) -> DashboardFeatures:
        dashboard_json = cast(dict[str, Any], await self._client._get(f"/v2/dashboard/{uid}"))
        return await self._parse_sfx_dashboard(dashboard_json)

    async def list_dashboards(self, limit: int = 500) -> list[dict]:
        """List SignalFx dashboards from dashboard groups when available."""
        out: list[dict] = []
        seen: set[str] = set()
        offset = 0
        page_complete = False
        if limit <= 0:
            return out

        while len(out) < limit:
            page_limit = max(1, min(SIGNALFX_DASHBOARD_GROUP_PAGE_SIZE, limit - len(out)))
            data = await self._client.list_dashboard_groups(limit=page_limit, offset=offset)
            groups = data.get("results", []) if isinstance(data, dict) else data
            group_items = groups if isinstance(groups, list) else []

            for group in group_items:
                if not isinstance(group, dict):
                    continue
                dashboards = group.get("dashboardConfigs") or group.get("dashboards", [])
                for dashboard in dashboards if isinstance(dashboards, list) else []:
                    if isinstance(dashboard, dict):
                        uid = dashboard.get("dashboardId", "") or dashboard.get("id", "")
                        title = (
                            dashboard.get("name", "")
                            or dashboard.get("dashboardName", "")
                            or dashboard.get("title", "")
                        )
                    else:
                        uid = str(dashboard)
                        title = ""
                    if not uid or uid in seen:
                        continue
                    seen.add(uid)
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

            page_complete = _signalfx_page_complete(
                data,
                page_count=len(group_items),
                page_limit=page_limit,
                total_seen=offset + len(group_items),
            )
            if page_complete or not group_items:
                break
            offset += len(group_items)
        return out

    async def ingest_alert(self, uid: str) -> AlertFeatures:
        """Fetch a SignalFx detector and extract operational alert features."""
        detector = cast(dict[str, Any], await self._client._get(f"/v2/detector/{uid}"))
        return _parse_signalfx_detector(detector, backend_name=self.name, realm=self._client.realm)

    async def list_alerts(self, limit: int = 500) -> list[dict]:
        """List SignalFx detectors discoverable by the configured token."""
        self.last_alert_list_complete = False
        out: list[dict] = []
        offset = 0
        page_complete = False
        while len(out) < limit:
            page_limit = max(1, min(100, limit - len(out)))
            params = {"limit": page_limit}
            if offset:
                params["offset"] = offset
            data = await self._client._get("/v2/detector", params=params)
            detectors = data.get("results", []) if isinstance(data, dict) else data
            detector_items = detectors if isinstance(detectors, list) else []
            for item in detector_items:
                if not isinstance(item, dict):
                    continue
                uid = str(item.get("id", ""))
                if not uid:
                    continue
                out.append(
                    {
                        "uid": uid,
                        "title": item.get("name", ""),
                        "backend": self.name,
                    }
                )
                if len(out) >= limit:
                    break
            page_complete = _signalfx_page_complete(
                data,
                page_count=len(detector_items),
                page_limit=page_limit,
                total_seen=len(out),
            )
            if page_complete or not detector_items:
                break
            offset += len(detector_items)
        self.last_alert_list_complete = page_complete
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


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _string_dict(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(k): str(v) for k, v in value.items() if v is not None}


def _detector_program(detector: dict[str, Any]) -> str:
    program = detector.get("programText", "")
    if isinstance(program, str) and program:
        return program
    program = detector.get("program", "")
    if isinstance(program, str) and program:
        return program
    return ""


def _detector_severity(detector: dict[str, Any]) -> str:
    severities: list[str] = []
    for rule in detector.get("rules", []) or []:
        if isinstance(rule, dict) and rule.get("severity"):
            severities.append(str(rule["severity"]))
    return severities[0] if severities else ""


def _detector_condition(detector: dict[str, Any]) -> str:
    labels: list[str] = []
    for rule in detector.get("rules", []) or []:
        if isinstance(rule, dict):
            label = rule.get("detectLabel", "") or rule.get("description", "")
            if label:
                labels.append(str(label))
    return "; ".join(dict.fromkeys(labels))


def _detector_annotations(detector: dict[str, Any]) -> dict[str, str]:
    annotations: dict[str, str] = {"description": str(detector.get("description", ""))}
    for index, rule in enumerate(detector.get("rules", []) or []):
        if not isinstance(rule, dict):
            continue
        for key in ("runbookUrl", "runbook_url", "tip", "message", "description"):
            value = rule.get(key)
            if value:
                annotations[f"rule_{index}_{key}"] = str(value)
    return annotations


def _signalfx_page_complete(data: Any, *, page_count: int, page_limit: int, total_seen: int) -> bool:
    """Return true when a SignalFx list response is known to be complete."""
    if not isinstance(data, dict):
        return page_count < page_limit
    if data.get("next") or data.get("nextPage") or data.get("nextPageLink"):
        return False
    if data.get("more") is True or data.get("hasMore") is True:
        return False
    total = data.get("count", data.get("total", data.get("totalCount")))
    if isinstance(total, int):
        return total_seen >= total
    return page_count < page_limit


def _signalfx_detector_page_complete(data: Any, *, page_count: int, page_limit: int, total_seen: int) -> bool:
    """Return true when the detector response is known to be a complete snapshot."""
    return _signalfx_page_complete(data, page_count=page_count, page_limit=page_limit, total_seen=total_seen)


def _parse_signalfx_detector(detector: dict[str, Any], *, backend_name: str, realm: str) -> AlertFeatures:
    uid = str(detector.get("id", ""))
    title = str(detector.get("name", ""))
    program = _detector_program(detector)
    tags = _string_list(detector.get("tags", []))
    labels = _string_dict(detector.get("labels", {}))
    teams = _string_list(detector.get("teams", []))
    if teams:
        labels.setdefault("team", teams[0])
    return AlertFeatures(
        alert_uid=uid,
        alert_title=title,
        alert_tags=tags,
        backend_name=backend_name,
        query_language="signalflow",
        condition=_detector_condition(detector),
        severity=_detector_severity(detector),
        enabled=not bool(detector.get("disabled", False)),
        labels=labels,
        annotations=_detector_annotations(detector),
        metrics_found=_extract_metrics_from_signalflow(program),
        query_transformations=[program] if program else [],
        service_hints=[],
        dashboard_uid="",
        panel_title="",
        source_url=f"https://app.{realm}.signalfx.com/#/detector/{uid}" if uid else "",
    )
