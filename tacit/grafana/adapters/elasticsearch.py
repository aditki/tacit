"""Adapter for Elasticsearch / OpenSearch datasource."""

from __future__ import annotations

import structlog

from tacit.grafana.adapters.base import DatasourceAdapter
from tacit.grafana.client import GrafanaClient
from tacit.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()


class ElasticsearchAdapter(DatasourceAdapter):

    supports_acquisition_inventory_phase = True

    async def discover_metrics_with_metadata(self, client, datasource, keywords, work_budget, *, enrich=True):
        """Return bounded per-search metadata without consulting legacy caches."""
        from tacit.acquisition import discover_adapter_metrics

        return await discover_adapter_metrics(client, datasource, keywords, work_budget, enrich=enrich)

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


def _iter_fields(mapping_response: dict | list):
    """Yield normalized fields without retaining a full mapping catalog."""

    def walk(obj: dict, prefix: str = ""):
        properties = obj.get("properties", {})
        if not isinstance(properties, dict):
            return
        for name, definition in sorted(properties.items()):
            if not isinstance(definition, dict):
                continue
            from tacit.signals.resolution import (
                ResolutionInputTextLimits,
                ResolutionInputWorkLimitError,
                admit_resolution_input_text,
            )

            limits = ResolutionInputTextLimits()
            counts = admit_resolution_input_text((prefix, name))
            separators = int(bool(prefix))
            characters = counts["input_character_count"] + separators
            utf8_bytes = counts["input_utf8_byte_count"] + separators
            if characters > limits.max_scalar_characters:
                raise ResolutionInputWorkLimitError(
                    "derived_scalar_characters", characters, limits.max_scalar_characters
                )
            if utf8_bytes > limits.max_scalar_utf8_bytes:
                raise ResolutionInputWorkLimitError(
                    "derived_scalar_utf8_bytes", utf8_bytes, limits.max_scalar_utf8_bytes
                )
            full = f"{prefix}.{name}" if prefix else name
            if isinstance(definition.get("type"), str):
                yield full, definition["type"]
            if "properties" in definition:
                yield from walk(definition, full)

    if isinstance(mapping_response, dict):
        for _, index_data in sorted(mapping_response.items()):
            if isinstance(index_data, dict):
                yield from walk(index_data.get("mappings", index_data))


def _extract_fields(mapping_response: dict | list, *, max_fields: int | None = None) -> dict[str, str]:
    fields: dict[str, str] = {}
    for name, kind in _iter_fields(mapping_response):
        if max_fields is not None and len(fields) >= max_fields:
            break
        fields[name] = kind
    return fields
