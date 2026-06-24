"""Adapter for Loki datasource (log-derived metrics via LogQL)."""

from __future__ import annotations

import structlog

from tacit.grafana.adapters.base import DatasourceAdapter
from tacit.grafana.client import GrafanaClient
from tacit.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()


class LokiAdapter(DatasourceAdapter):

    @property
    def query_language(self) -> str:
        return "logql"

    @property
    def supported_types(self) -> set[str]:
        return {"loki"}

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        entries: list[MetricEntry] = []

        # Loki exposes labels, not metric names.
        # Discover available label names → these become log stream selectors.
        try:
            data = await client.datasource_proxy_get(datasource.uid, "loki/api/v1/labels")
            if isinstance(data, dict):
                labels: list[str] = data.get("data", [])
            elif isinstance(data, list):
                labels = data
            else:
                labels = []
        except Exception:
            logger.warning("loki_labels_failed", datasource=datasource.name)
            return []

        # For each interesting label, fetch its values (capped)
        # Focus on labels that look like service/app/namespace identifiers
        interesting_labels = [
            label
            for label in labels
            if label
            in {
                "app",
                "service",
                "namespace",
                "job",
                "container",
                "pod",
                "component",
                "host",
                "level",
                "severity",
                "status_code",
            }
        ]

        label_values_map: dict[str, list[str]] = {}
        for label in interesting_labels[:6]:
            try:
                val_data = await client.datasource_proxy_get(datasource.uid, f"loki/api/v1/label/{label}/values")
                vals: list[str] = val_data.get("data", []) if isinstance(val_data, dict) else []
                label_values_map[label] = vals[:50]
            except Exception:
                continue

        # Build MetricEntry objects representing log stream selectors
        # These are presented to the LLM which will construct actual LogQL queries
        for label in interesting_labels:
            entries.append(
                MetricEntry(
                    name=f"log_stream:{label}",
                    datasource_uid=datasource.uid,
                    datasource_name=datasource.name,
                    datasource_type=datasource.type,
                    query_language=self.query_language,
                    dimensions=label_values_map.get(label, []),
                )
            )

        # Expose raw labels list so the LLM can build selectors
        entries.append(
            MetricEntry(
                name="loki:available_labels",
                datasource_uid=datasource.uid,
                datasource_name=datasource.name,
                datasource_type=datasource.type,
                query_language=self.query_language,
                dimensions=labels,
            )
        )

        logger.info("loki_metrics_discovered", datasource=datasource.name, labels=len(labels))
        return entries
