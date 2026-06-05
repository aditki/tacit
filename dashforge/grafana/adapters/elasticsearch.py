"""Adapter for Elasticsearch / OpenSearch datasource."""

from __future__ import annotations

import structlog

from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()


class ElasticsearchAdapter(DatasourceAdapter):

    @property
    def query_language(self) -> str:
        return "elasticsearch"

    @property
    def supported_types(self) -> set[str]:
        return {"elasticsearch", "opensearch"}

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        entries: list[MetricEntry] = []
        index_name = datasource.json_data.get("index", datasource.json_data.get("database", "*"))

        # Fetch field mappings from the index
        try:
            data = await client.datasource_proxy_get(datasource.uid, f"{index_name}/_mapping")
        except Exception:
            logger.warning("elasticsearch_mapping_failed", datasource=datasource.name)
            return []

        # Extract numeric and keyword fields from the mapping
        fields = _extract_fields(data)
        kw_lower = [k.lower() for k in keywords]

        for field_name, field_type in fields.items():
            if kw_lower and not any(k in field_name.lower() for k in kw_lower):
                continue
            entries.append(
                MetricEntry(
                    name=field_name,
                    datasource_uid=datasource.uid,
                    datasource_name=datasource.name,
                    datasource_type=datasource.type,
                    query_language=self.query_language,
                    namespace=index_name,
                    dimensions=[field_type],
                )
            )

        # If no keyword matches, send the most common field types
        if not entries:
            for field_name, field_type in list(fields.items())[:100]:
                entries.append(
                    MetricEntry(
                        name=field_name,
                        datasource_uid=datasource.uid,
                        datasource_name=datasource.name,
                        datasource_type=datasource.type,
                        query_language=self.query_language,
                        namespace=index_name,
                        dimensions=[field_type],
                    )
                )

        logger.info("elasticsearch_metrics_discovered", datasource=datasource.name, count=len(entries))
        return entries


def _extract_fields(mapping_response: dict | list) -> dict[str, str]:
    """Walk ES mapping JSON and extract {field_name: field_type}."""
    fields: dict[str, str] = {}

    def _walk(obj: dict, prefix: str = ""):
        if "properties" in obj:
            for fname, fdef in obj["properties"].items():
                full = f"{prefix}.{fname}" if prefix else fname
                if "type" in fdef:
                    fields[full] = fdef["type"]
                if "properties" in fdef:
                    _walk(fdef, full)

    if isinstance(mapping_response, dict):
        for index_data in mapping_response.values():
            mappings = index_data.get("mappings", index_data)
            _walk(mappings)
    return fields
