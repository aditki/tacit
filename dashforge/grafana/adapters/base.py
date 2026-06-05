"""Abstract base for datasource adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod

from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry


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
