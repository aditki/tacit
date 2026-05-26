"""Direct SignalFx metric discovery — reuses keyword mapping from the Grafana adapter."""
from __future__ import annotations

import asyncio
from typing import Any

import structlog

from dashforge.cache import make_cache_key, metric_cache
from dashforge.grafana.adapters.signalfx import KEYWORD_METRIC_MAP
from dashforge.models.schemas import MetricEntry
from dashforge.signalfx.client import SignalFxClient

logger = structlog.get_logger()

_MAX_CATALOG_SIZE = 300
_MAX_DIMENSION_KEYS = 15
_MAX_DIMENSION_VALUES = 10
# Internal SFx keys to exclude from dimension lists
_INTERNAL_KEYS = {"sf_metric", "sf_originatingMetric", "sf_key", "sf_type",
                  "sf_isActive", "sf_createdOnMs", "sf_tags"}


async def _fetch_dimensions(
    client: SignalFxClient,
    metric_name: str,
) -> list[str]:
    """Fetch dimension keys + sample values for a metric via MTS search."""
    try:
        data = await client.search_metric_timeseries(
            query=f"sf_metric:{metric_name}", limit=20
        )
        results = data.get("results", []) if isinstance(data, dict) else data
        if not results:
            return []

        dim_values: dict[str, set[str]] = {}
        for mts in results[:20]:
            for k, v in mts.items():
                if k.startswith("sf_") or k in _INTERNAL_KEYS:
                    continue
                if isinstance(v, str) and v:
                    dim_values.setdefault(k, set()).add(v)

        dims = []
        for key in sorted(dim_values.keys())[:_MAX_DIMENSION_KEYS]:
            values = sorted(dim_values[key])[:_MAX_DIMENSION_VALUES]
            dims.append(f"{key}={{{','.join(values)}}}")
        return dims
    except Exception:
        logger.debug("signalfx_direct_dims_failed", metric=metric_name)
        return []


async def discover_metrics(
    client: SignalFxClient,
    keywords: list[str],
) -> list[MetricEntry]:
    """Discover metrics directly from SignalFx API.

    Reuses KEYWORD_METRIC_MAP from the Grafana adapter for consistent
    keyword-to-metric-prefix mapping.

    Returns normalized MetricEntry objects compatible with the rest of
    the DashForge pipeline.
    """
    cache_key = make_cache_key("sfx_direct", ",".join(sorted(keywords)))
    cached = metric_cache.get(cache_key)
    if cached is not None:
        logger.info("signalfx_direct_cache_hit", metrics=len(cached))
        return cached

    # Build search queries from keywords (reusing adapter's mapping)
    # SignalFx /v2/metric requires 'name:' prefix for metric name searches.
    search_queries: set[str] = set()
    kw_lower = [k.lower() for k in keywords]
    for kw in kw_lower:
        search_queries.add(f"name:*{kw}*")
        for pattern, prefixes in KEYWORD_METRIC_MAP.items():
            if pattern in kw or kw in pattern:
                for prefix in prefixes:
                    search_queries.add(f"name:{prefix}*")

    if not search_queries:
        search_queries = {"*"}

    # Execute searches concurrently
    async def _search_one(query: str) -> list[dict[str, Any]]:
        try:
            data = await client.search_metrics(query=query, limit=100)
            return data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
        except Exception:
            logger.warning("signalfx_direct_search_failed", query=query)
            return []

    tasks = [_search_one(q) for q in list(search_queries)[:10]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Deduplicate
    seen: set[str] = set()
    metric_names: list[str] = []
    for result in results:
        if isinstance(result, list):
            for item in result:
                name = item.get("name", "") if isinstance(item, dict) else str(item)
                if name and name not in seen:
                    seen.add(name)
                    metric_names.append(name)

    # Keyword prioritization
    matched = [n for n in metric_names if any(k in n.lower() for k in kw_lower)]
    unmatched = [n for n in metric_names if n not in set(matched)]
    metric_names = (matched + unmatched)[:_MAX_CATALOG_SIZE]

    # Fetch dimensions for top metrics
    to_sample = metric_names[:50]
    dim_tasks = [_fetch_dimensions(client, name) for name in to_sample]
    dim_results = await asyncio.gather(*dim_tasks, return_exceptions=True)

    catalog: dict[str, list[str]] = {name: [] for name in metric_names}
    for name, result in zip(to_sample, dim_results):
        if isinstance(result, list):
            catalog[name] = result

    # Build MetricEntry list — use "signalfx" type + "signalflow" language
    entries = [
        MetricEntry(
            name=name,
            datasource_uid="signalfx-direct",
            datasource_name="SignalFx Direct",
            datasource_type="signalfx",
            query_language="signalflow",
            dimensions=catalog.get(name, []),
        )
        for name in metric_names
    ]

    metric_cache.set(cache_key, entries)
    logger.info("signalfx_direct_discovered", total=len(entries),
                sampled_dims=sum(1 for v in catalog.values() if v))
    return entries
