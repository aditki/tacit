"""Adapter for Splunk SignalFx (Splunk Observability Cloud) datasource.

SignalFx uses the SignalFlow streaming analytics language for real-time
computations on metric time series. Grafana's SignalFx datasource plugin
exposes metric search and metadata APIs via the datasource proxy.

Grafana datasource proxy paths used:
  GET /api/datasources/proxy/uid/{uid}/v2/metric?query=<pattern>&limit=<n>
  GET /api/datasources/proxy/uid/{uid}/v2/dimension?query=<pattern>&limit=<n>
  GET /api/datasources/proxy/uid/{uid}/v2/metrictimeseries?query=<filter>&limit=<n>
  GET /api/datasources/proxy/uid/{uid}/v2/metric/<name>
"""

from __future__ import annotations

import asyncio
from urllib.parse import quote

import structlog

from dashforge.cache import make_cache_key, metric_cache
from dashforge.grafana.adapters.base import DatasourceAdapter
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()

_MAX_CATALOG_SIZE = 300
_MAX_SEARCH_RESULTS = 200
_MAX_DIMENSION_KEYS = 15
_MAX_DIMENSION_VALUES = 10

# Common infrastructure keywords → SignalFx metric prefixes / patterns
KEYWORD_METRIC_MAP: dict[str, list[str]] = {
    "cpu": ["cpu.utilization", "cpu.idle", "cpu.num_processors", "process.cpu"],
    "memory": ["memory.utilization", "memory.used", "memory.free", "process.memory"],
    "disk": ["disk.utilization", "disk.summary_utilization", "disk_ops.total"],
    "network": ["network.total", "network.errors", "if_octets", "if_errors"],
    "latency": ["service.request.duration", "http.server.duration", "span.duration"],
    "error": ["service.request.count", "http.status_code", "error.count"],
    "http": ["http.server.duration", "http.server.request.size", "http.status_code"],
    "jvm": ["runtime.jvm.memory", "runtime.jvm.gc", "runtime.jvm.threads"],
    "container": ["container.cpu", "container.memory", "container.filesystem"],
    "kubernetes": ["k8s.pod.cpu", "k8s.pod.memory", "k8s.node.cpu", "k8s.container"],
    "aws": ["aws.ec2", "aws.elb", "aws.rds", "aws.lambda", "aws.sqs"],
    "load_balancer": ["aws.elb", "aws.alb", "aws.nlb"],
    "database": ["db.query.duration", "db.connections", "aws.rds"],
    "queue": ["aws.sqs", "messaging."],
    "lambda": ["aws.lambda.invocations", "aws.lambda.duration", "aws.lambda.errors"],
    "redis": ["redis.", "cache.hits", "cache.misses"],
}


class SignalFxAdapter(DatasourceAdapter):
    """Adapter for Splunk SignalFx / Splunk Observability Cloud."""

    @property
    def query_language(self) -> str:
        return "signalflow"

    @property
    def supported_types(self) -> set[str]:
        # Grafana datasource type names for SignalFx plugin variants
        return {"grafana-signalfx-datasource", "signalfx"}

    async def _search_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        query: str,
        limit: int = _MAX_SEARCH_RESULTS,
    ) -> list[dict]:
        """Search metrics via the SignalFx v2 metric search API."""
        try:
            path = f"v2/metric?query={quote(query)}&limit={limit}"
            data = await client.datasource_proxy_get(datasource.uid, path)
            # API returns {"results": [...]} or a list directly
            if isinstance(data, dict):
                return data.get("results", [])
            return data if isinstance(data, list) else []
        except Exception:
            logger.warning("signalfx_metric_search_failed", datasource=datasource.name, query=query)
            return []

    async def _get_metric_metadata(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        metric_name: str,
    ) -> dict:
        """Fetch metadata for a single metric (description, type, dimensions)."""
        try:
            path = f"v2/metric/{quote(metric_name, safe='')}"
            data = await client.datasource_proxy_get(datasource.uid, path)
            return data if isinstance(data, dict) else {}
        except Exception:
            logger.debug("signalfx_metric_metadata_failed", metric=metric_name)
            return {}

    async def _search_dimensions(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        metric_name: str,
    ) -> list[str]:
        """Fetch dimension keys and sample values for a metric via MTS query."""
        try:
            query = f"sf_metric:{metric_name}"
            path = f"v2/metrictimeseries?query={quote(query)}&limit=20"
            data = await client.datasource_proxy_get(datasource.uid, path)

            results = []
            if isinstance(data, dict):
                results = data.get("results", [])
            elif isinstance(data, list):
                results = data

            if not results:
                return []

            # Collect unique dimension keys and values across MTS
            dim_values: dict[str, set[str]] = {}
            internal_keys = {
                "sf_metric",
                "sf_originatingMetric",
                "sf_key",
                "sf_type",
                "sf_isActive",
                "sf_createdOnMs",
                "sf_tags",
            }
            for mts in results[:20]:
                for k, v in mts.items():
                    if k.startswith("sf_") or k in internal_keys:
                        continue
                    if isinstance(v, str) and v:
                        dim_values.setdefault(k, set()).add(v)

            dims = []
            for key in sorted(dim_values.keys())[:_MAX_DIMENSION_KEYS]:
                values = sorted(dim_values[key])[:_MAX_DIMENSION_VALUES]
                dims.append(f"{key}={{{','.join(values)}}}")
            return dims
        except Exception:
            logger.debug("signalfx_dimensions_failed", metric=metric_name)
            return []

    async def _get_cached_catalog(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> dict[str, list[str]]:
        """Return {metric_name: [dim_strings]} from cache or live fetch."""
        cache_key = make_cache_key("sfx_catalog", datasource.uid, ",".join(sorted(keywords)))
        cached = metric_cache.get(cache_key)
        if cached is not None:
            logger.info("signalfx_catalog_cache_hit", datasource=datasource.name, metrics=len(cached))
            return cached

        # Build search queries from keywords
        search_queries = set()
        kw_lower = [k.lower() for k in keywords]
        for kw in kw_lower:
            search_queries.add(f"*{kw}*")
            for pattern, prefixes in KEYWORD_METRIC_MAP.items():
                if pattern in kw or kw in pattern:
                    search_queries.update(prefixes)

        if not search_queries:
            # Fallback: broad search
            search_queries = {"*"}

        # Execute searches concurrently
        search_tasks = [
            self._search_metrics(client, datasource, q, limit=100)
            for q in list(search_queries)[:10]  # cap parallel searches
        ]
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

        # Deduplicate metric names
        seen_names: set[str] = set()
        metric_names: list[str] = []
        for result in search_results:
            if isinstance(result, list):
                for item in result:
                    name = item.get("name", "") if isinstance(item, dict) else str(item)
                    if name and name not in seen_names:
                        seen_names.add(name)
                        metric_names.append(name)

        # Fetch dimensions for top metrics
        to_sample = metric_names[:50]
        dim_tasks = [self._search_dimensions(client, datasource, name) for name in to_sample]
        dim_results = await asyncio.gather(*dim_tasks, return_exceptions=True)

        catalog: dict[str, list[str]] = {}
        for name in metric_names:
            catalog[name] = []
        for name, dim_result in zip(to_sample, dim_results):
            if isinstance(dim_result, list):
                catalog[name] = dim_result

        metric_cache.set(cache_key, catalog)
        logger.info(
            "signalfx_catalog_cached",
            datasource=datasource.name,
            total=len(catalog),
            sampled_dims=sum(1 for v in catalog.values() if v),
        )
        return catalog

    async def discover_metrics(
        self,
        client: GrafanaClient,
        datasource: DatasourceInfo,
        keywords: list[str],
    ) -> list[MetricEntry]:
        catalog = await self._get_cached_catalog(client, datasource, keywords)
        if not catalog:
            return []

        all_names = list(catalog.keys())

        # Keyword pre-filter: prioritize keyword-matched metrics, then backfill
        kw_lower = [k.lower() for k in keywords]
        matched = [n for n in all_names if any(k in n.lower() for k in kw_lower)]
        unmatched = [n for n in all_names if n not in set(matched)]
        filtered = (matched + unmatched)[:_MAX_CATALOG_SIZE]

        logger.info(
            "signalfx_metrics_discovered",
            datasource=datasource.name,
            total=len(all_names),
            filtered=len(filtered),
        )

        return [
            MetricEntry(
                name=name,
                datasource_uid=datasource.uid,
                datasource_name=datasource.name,
                datasource_type=datasource.type,
                query_language=self.query_language,
                dimensions=catalog.get(name, []),
            )
            for name in filtered
        ]
