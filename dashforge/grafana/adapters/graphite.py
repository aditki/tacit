"""Adapter for Graphite datasource."""

from __future__ import annotations

import structlog

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()


class GraphiteAdapter(DatasourceAdapter):

    @property
    def query_language(self) -> str:
        return "graphite"

    @property
    def supported_types(self) -> set[str]:
        return {"graphite"}

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        entries: list[MetricEntry] = []

        # Graphite's /metrics/find endpoint supports wildcards
        # We search for each keyword as a glob pattern
        seen: set[str] = set()
        search_patterns = [f"*{kw}*" for kw in keywords] if keywords else ["*"]

        for pattern in search_patterns[:5]:
            try:
                data = await client.datasource_proxy_get(datasource.uid, f"metrics/find?query={pattern}")
                results: list[dict] = data if isinstance(data, list) else []
            except Exception:
                logger.warning("graphite_find_failed", datasource=datasource.name, pattern=pattern)
                continue

            for node in results:
                path = node.get("id", node.get("text", ""))
                if path and path not in seen:
                    seen.add(path)
                    entries.append(
                        MetricEntry(
                            name=path,
                            datasource_uid=datasource.uid,
                            datasource_name=datasource.name,
                            datasource_type=datasource.type,
                            query_language=self.query_language,
                            namespace=path.split(".")[0] if "." in path else "",
                        )
                    )

        logger.info("graphite_metrics_discovered", datasource=datasource.name, count=len(entries))
        return entries
