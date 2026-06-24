"""Grafana backend adapter — wraps existing Grafana client and helpers."""

from __future__ import annotations

from typing import Any, cast

import structlog

from tacit.backends.base import AlertFeatures, DashboardFeatures, DiscoveryStatus, PublishResult
from tacit.config import Settings
from tacit.grafana.adapters.registry import get_adapter
from tacit.grafana.client import GrafanaClient
from tacit.grafana.dashboard import publish_dashboard as publish_dashboard_fn
from tacit.grafana.datasource import (
    discover_all_metrics,
    filter_datasources_by_signal,
    filter_searchable_datasources,
    list_datasources,
)
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry
from tacit.validation import validate_dashboard_queries

logger = structlog.get_logger()


class GrafanaBackend:
    """Dashboard backend that talks to Grafana."""

    def __init__(self, client: GrafanaClient | None = None, runtime_settings: Settings | None = None):
        self._settings = runtime_settings
        self._client = client or GrafanaClient(runtime_settings=runtime_settings)
        self.last_discovery_status = DiscoveryStatus()
        self.last_alert_list_complete = False

    # ── Protocol properties ───────────────────────────────────────────

    @property
    def name(self) -> str:
        return "grafana"

    @property
    def query_language(self) -> str:
        return "promql"

    # ── Discovery ─────────────────────────────────────────────────────

    async def discover_metrics(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        try:
            all_ds, searchable_ds = await self._select_searchable_datasources(intent)

            if not searchable_ds:
                logger.warning("grafana_no_searchable_datasources")
                self.last_discovery_status = DiscoveryStatus(
                    available=True,
                    datasource_count=len(all_ds),
                    searchable_datasource_count=0,
                )
                return []

            entries = await discover_all_metrics(self._client, searchable_ds, keywords)
            self.last_discovery_status = DiscoveryStatus(
                available=True,
                datasource_count=len(all_ds),
                searchable_datasource_count=len(searchable_ds),
            )
            return entries
        except Exception as exc:
            self.last_discovery_status = DiscoveryStatus(available=False, error=str(exc))
            logger.error("grafana_discover_failed", error=str(exc), exc_info=True)
            return []

    async def discover_datasource_targets(
        self,
        keywords: list[str],
        intent: Intent,
    ) -> list[MetricEntry]:
        """Return datasource identities even when metric discovery is empty."""
        del keywords
        try:
            all_ds, searchable_ds = await self._select_searchable_datasources(intent)
        except Exception as exc:
            self.last_discovery_status = DiscoveryStatus(available=False, error=str(exc))
            logger.error("grafana_datasource_target_discovery_failed", error=str(exc), exc_info=True)
            return []

        self.last_discovery_status = DiscoveryStatus(
            available=True,
            datasource_count=len(all_ds),
            searchable_datasource_count=len(searchable_ds),
        )
        targets: list[MetricEntry] = []
        for ds in searchable_ds:
            adapter = get_adapter(ds)
            if adapter is None:
                continue
            targets.append(
                MetricEntry(
                    name="",
                    datasource_uid=ds.uid,
                    datasource_name=ds.name,
                    datasource_type=ds.type,
                    query_language=adapter.query_language,
                )
            )
        return targets

    async def _select_searchable_datasources(self, intent: Intent):
        all_ds = await list_datasources(self._client)

        signal_types = [s.value for s in intent.signals]
        relevant_ds = filter_datasources_by_signal(all_ds, signal_types)
        if not relevant_ds:
            relevant_ds = filter_datasources_by_signal(all_ds, ["metrics"])

        searchable_ds = filter_searchable_datasources(relevant_ds)
        if not searchable_ds:
            searchable_ds = filter_searchable_datasources(all_ds)

        return all_ds, searchable_ds

    # ── Validation ────────────────────────────────────────────────────

    async def validate_queries(
        self,
        spec: DashboardSpec,
        catalog: list[MetricEntry] | None = None,
    ) -> tuple[DashboardSpec, list[str]]:
        return await validate_dashboard_queries(self._client, spec, catalog)

    # ── Publish ───────────────────────────────────────────────────────

    async def publish(
        self,
        spec: DashboardSpec,
    ) -> PublishResult:
        url, uid = await publish_dashboard_fn(self._client, spec, runtime_settings=self._settings)
        return PublishResult(url=url, uid=uid, backend_name="grafana")

    # ── Ingestion ─────────────────────────────────────────────────────

    async def ingest_dashboard(self, uid: str) -> DashboardFeatures:
        from tacit.dashboard_ingest import parse_dashboard_json

        dashboard_json = cast(dict[str, Any], await self._client._get(f"/api/dashboards/uid/{uid}"))
        extracted = parse_dashboard_json(dashboard_json)
        return DashboardFeatures(
            dashboard_uid=extracted["dashboard_uid"],
            dashboard_title=extracted["dashboard_title"],
            dashboard_tags=extracted["dashboard_tags"],
            backend_name=self.name,
            query_language=self.query_language,
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

    async def list_dashboards(self, limit: int = 500) -> list[dict]:
        """List Grafana dashboards discoverable by the configured token."""
        raw = await self._client._get(
            "/api/search",
            params={"type": "dash-db", "limit": limit},
        )
        dashboards = raw if isinstance(raw, list) else []
        out: list[dict] = []
        for item in dashboards:
            uid = item.get("uid") if isinstance(item, dict) else ""
            if not uid:
                continue
            out.append(
                {
                    "uid": uid,
                    "title": item.get("title", ""),
                    "folder": item.get("folderTitle", ""),
                    "url": item.get("url", ""),
                    "backend": self.name,
                }
            )
        return out[:limit]

    async def ingest_alert(self, uid: str) -> AlertFeatures:
        """Fetch a Grafana alert rule and extract operational features."""
        try:
            rule = cast(dict[str, Any], await self._client._get(f"/api/v1/provisioning/alert-rules/{uid}"))
            return _parse_grafana_alert_rule(rule, backend_name=self.name, base_url=self._client.base_url)
        except Exception:
            legacy = cast(dict[str, Any], await self._client._get(f"/api/alerts/{uid}"))
            return _parse_legacy_grafana_alert(legacy, backend_name=self.name, base_url=self._client.base_url)

    async def list_alerts(self, limit: int = 500) -> list[dict]:
        """List Grafana alert rules discoverable by the configured token."""
        self.last_alert_list_complete = False
        try:
            raw = await self._client._get("/api/v1/provisioning/alert-rules")
            rules = raw if isinstance(raw, list) else []
            self.last_alert_list_complete = len(rules) <= limit
            out = []
            for item in rules:
                if not isinstance(item, dict):
                    continue
                uid = item.get("uid", "")
                if not uid:
                    continue
                out.append(
                    {
                        "uid": uid,
                        "title": item.get("title", ""),
                        "folder": item.get("folderUID", ""),
                        "backend": self.name,
                    }
                )
            return out[:limit]
        except Exception as exc:
            logger.warning("grafana_unified_alert_list_failed", error=str(exc))

        raw = await self._client._get("/api/alerts", params={"limit": limit})
        self.last_alert_list_complete = False
        alerts = raw if isinstance(raw, list) else []
        out = []
        for item in alerts:
            if not isinstance(item, dict):
                continue
            uid = str(item.get("id", "") or item.get("uid", ""))
            if not uid:
                continue
            out.append(
                {
                    "uid": uid,
                    "title": item.get("name", "") or item.get("title", ""),
                    "folder": item.get("folderTitle", ""),
                    "backend": self.name,
                }
            )
        return out[:limit]

    # ── Cleanup ───────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._client.close()


def _string_dict(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    return {str(k): str(v) for k, v in value.items() if v is not None}


def _service_hints_from_labels(labels: dict[str, str], tags: list[str]) -> list[str]:
    hints: list[str] = []

    def add(value: str) -> None:
        cleaned = value.strip()
        if cleaned and cleaned not in hints:
            hints.append(cleaned)

    for key, value in labels.items():
        if key.lower() in {"service", "service_name", "app", "application", "component", "team"}:
            add(value)
    for tag in tags:
        if ":" in tag:
            key, value = tag.split(":", 1)
            if key.lower() in {"service", "app", "application", "component", "team"}:
                add(value)
    return hints


def _datasource_type(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("type", "") or value.get("name", "")).lower()
    return ""


def _is_prometheus_alert_query_item(item: dict[str, Any], model: dict[str, Any]) -> bool:
    datasource_uids = [
        str(item.get("datasourceUid", "") or "").lower(),
        str(model.get("datasourceUid", "") or "").lower(),
    ]
    item_datasource = item.get("datasource")
    if isinstance(item_datasource, dict):
        datasource_uids.append(str(item_datasource.get("uid", "") or "").lower())
    model_datasource = model.get("datasource")
    if isinstance(model_datasource, dict):
        datasource_uids.append(str(model_datasource.get("uid", "") or "").lower())
    if "__expr__" in datasource_uids:
        return False
    if str(model.get("type", "") or "").lower() in {"math", "reduce", "classic_conditions"}:
        return False

    datasource_types = [
        _datasource_type(item.get("datasource")),
        _datasource_type(model.get("datasource")),
    ]
    explicit_types = [value for value in datasource_types if value]
    if explicit_types:
        return any(value in {"prometheus", "promql"} for value in explicit_types)
    return False


def _extract_grafana_rule_queries(rule: dict[str, Any]) -> list[str]:
    queries: list[str] = []
    for item in rule.get("data", []) or []:
        if not isinstance(item, dict):
            continue
        model = item.get("model", {})
        if not isinstance(model, dict):
            continue
        if not _is_prometheus_alert_query_item(item, model):
            continue
        for key in ("expr", "query"):
            value = model.get(key, "")
            if isinstance(value, str) and value:
                queries.append(value)
    return list(dict.fromkeys(queries))


def _extract_promql_metrics(queries: list[str]) -> list[str]:
    from tacit.dashboard_ingest import extract_metrics_from_promql

    metrics: list[str] = []
    for query in queries:
        try:
            metrics.extend(extract_metrics_from_promql(query))
        except Exception:
            logger.debug("grafana_alert_metric_parse_failed", query=query)
    return list(dict.fromkeys(metrics))


def _parse_grafana_alert_rule(rule: dict[str, Any], *, backend_name: str, base_url: str) -> AlertFeatures:
    labels = _string_dict(rule.get("labels", {}))
    annotations = _string_dict(rule.get("annotations", {}))
    title = str(rule.get("title", ""))
    uid = str(rule.get("uid", ""))
    queries = _extract_grafana_rule_queries(rule)
    tags = [f"{key}:{value}" for key, value in labels.items() if key.lower() in {"service", "team", "severity"}]
    dashboard_uid = annotations.get("__dashboardUid__", "") or annotations.get("dashboardUid", "")
    panel_title = annotations.get("__panelTitle__", "") or annotations.get("panelTitle", "")
    return AlertFeatures(
        alert_uid=uid,
        alert_title=title,
        alert_tags=tags,
        backend_name=backend_name,
        query_language="promql",
        condition=str(rule.get("condition", "")),
        severity=labels.get("severity", ""),
        enabled=not bool(rule.get("isPaused", False)),
        labels=labels,
        annotations=annotations,
        metrics_found=_extract_promql_metrics(queries),
        query_transformations=queries,
        service_hints=_service_hints_from_labels(labels, tags),
        dashboard_uid=dashboard_uid,
        panel_title=panel_title,
        source_url=f"{base_url}/alerting/grafana/{uid}/view" if uid else "",
    )


def _parse_legacy_grafana_alert(alert: dict[str, Any], *, backend_name: str, base_url: str) -> AlertFeatures:
    uid = str(alert.get("id", "") or alert.get("uid", ""))
    title = str(alert.get("name", "") or alert.get("title", ""))
    dashboard_uid = str(alert.get("dashboardUid", ""))
    state = str(alert.get("state", ""))
    return AlertFeatures(
        alert_uid=uid,
        alert_title=title,
        alert_tags=[state] if state else [],
        backend_name=backend_name,
        query_language="promql",
        condition=state,
        severity=state,
        enabled=state.lower() != "paused",
        labels={},
        annotations={},
        metrics_found=[],
        query_transformations=[],
        service_hints=[],
        dashboard_uid=dashboard_uid,
        panel_title=str(alert.get("panelTitle", "")),
        source_url=f"{base_url}{alert.get('url', '')}" if alert.get("url") else "",
    )
