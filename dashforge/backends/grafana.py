"""Grafana backend adapter — wraps existing Grafana client and helpers."""

from __future__ import annotations

from typing import Any, cast

import structlog

from dashforge.backends.base import DashboardFeatures, DiscoveryStatus, PublishResult
from dashforge.grafana.adapters.registry import get_adapter
from dashforge.grafana.client import GrafanaClient
from dashforge.grafana.dashboard import publish_dashboard as publish_dashboard_fn
from dashforge.grafana.datasource import (
    discover_all_metrics,
    filter_datasources_by_signal,
    filter_searchable_datasources,
    list_datasources,
)
from dashforge.models.schemas import DashboardSpec, Intent, MetricEntry
from dashforge.validation import validate_dashboard_queries

logger = structlog.get_logger()


class GrafanaBackend:
    """Dashboard backend that talks to Grafana."""

    def __init__(self, client: GrafanaClient | None = None):
        self._client = client or GrafanaClient()
        self.last_discovery_status = DiscoveryStatus()

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
    ) -> tuple[DashboardSpec, list[str]]:
        return await validate_dashboard_queries(self._client, spec)

    # ── Publish ───────────────────────────────────────────────────────

    async def publish(
        self,
        spec: DashboardSpec,
    ) -> PublishResult:
        url, uid = await publish_dashboard_fn(self._client, spec)
        return PublishResult(url=url, uid=uid, backend_name="grafana")

    # ── Ingestion ─────────────────────────────────────────────────────

    async def ingest_dashboard(self, uid: str) -> DashboardFeatures:
        from dashforge.dashboard_ingest import parse_dashboard_json

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

    # ── Cleanup ───────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._client.close()
