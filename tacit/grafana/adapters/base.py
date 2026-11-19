"""Abstract base for datasource adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod

from tacit.grafana.client import GrafanaClient
from tacit.models.schemas import DatasourceInfo, MetricEntry


class DatasourceAdapter(ABC):
    """Interface for datasource-specific metric discovery.

    Each adapter knows how to query a specific datasource type through
    Grafana's proxy / resource APIs and return normalized MetricEntry objects.
    """

    @property
    @abstractmethod
    def query_language(self) -> str:
        """The query language this datasource uses (promql, cloudwatch, logql, …)."""

    @property
    @abstractmethod
    def supported_types(self) -> set[str]:
        """Grafana datasource type strings this adapter handles."""

    @abstractmethod
    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        """Discover available metrics from this datasource.

        Args:
            client: Grafana API client.
            datasource: Datasource metadata from Grafana.
            keywords: Intent keywords to focus the search.

        Returns:
            List of normalized MetricEntry objects.
        """

    async def discover_metrics_with_metadata(self, client, datasource, keywords, work_budget):
        """Optional compatibility capability for registered list-only adapters."""
        from tacit.acquisition import legacy_acquisition_catalog

        return legacy_acquisition_catalog(
            await self.discover_metrics(client, datasource, keywords),
            backend="grafana",
            terms=keywords,
            work_budget=work_budget,
        )
