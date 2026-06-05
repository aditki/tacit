from __future__ import annotations

import asyncio

import structlog

from dashforge.config import settings
from dashforge.grafana.adapters.registry import get_adapter, supported_datasource_types
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DatasourceInfo, MetricEntry

logger = structlog.get_logger()

# Signal type → datasource types mapping (broad: we search everything that
# could carry the signal, not just the obvious ones).
SIGNAL_TYPE_MAP: dict[str, set[str]] = {
    "metrics": {
        "prometheus",
        "mimir",
        "cortex",
        "thanos",
        "cloudwatch",
        "graphite",
        "influxdb",
        "elasticsearch",
        "opensearch",
        "grafana-signalfx-datasource",
        "signalfx",
    },
    "logs": {"loki", "elasticsearch", "opensearch"},
    "traces": {"tempo", "jaeger", "zipkin"},
}


async def list_datasources(client: GrafanaClient) -> list[DatasourceInfo]:
    """Fetch all datasources from Grafana and return typed info objects."""
    raw = await client.list_datasources()
    out: list[DatasourceInfo] = []
    for ds in raw:
        out.append(
            DatasourceInfo(
                uid=ds["uid"],
                name=ds["name"],
                type=ds["type"],
                url=ds.get("url", ""),
                is_default=ds.get("isDefault", False),
                json_data=ds.get("jsonData", {}),
            )
        )
    logger.info("datasources_discovered", count=len(out))
    return out


def filter_datasources_by_signal(
    datasources: list[DatasourceInfo],
    signal_types: list[str],
) -> list[DatasourceInfo]:
    """Return datasources that match the requested signal types."""
    wanted: set[str] = set()
    for st in signal_types:
        wanted |= SIGNAL_TYPE_MAP.get(st, set())
    return [ds for ds in datasources if ds.type in wanted]


def filter_searchable_datasources(
    datasources: list[DatasourceInfo],
) -> list[DatasourceInfo]:
    """Return only datasources that have an adapter (we can search them)."""
    supported = supported_datasource_types()
    return [ds for ds in datasources if ds.type in supported]


async def discover_all_metrics(
    client: GrafanaClient,
    datasources: list[DatasourceInfo],
    keywords: list[str],
) -> list[MetricEntry]:
    """Search ALL given datasources for metrics, using per-type adapters.

    Returns a unified list of MetricEntry objects across every datasource type,
    capped at settings.max_metric_catalog_size to stay within LLM context limits.
    """
    sem = asyncio.Semaphore(settings.adapter_max_concurrent)
    timeout = settings.adapter_timeout_seconds

    async def _discover_one(ds: DatasourceInfo) -> list[MetricEntry]:
        adapter = get_adapter(ds)
        if adapter is None:
            logger.debug("no_adapter", datasource=ds.name, type=ds.type)
            return []
        async with sem:
            try:
                return await asyncio.wait_for(
                    adapter.discover_metrics(client, ds, keywords),
                    timeout=timeout,
                )
            except TimeoutError:
                logger.warning("adapter_timeout", datasource=ds.name, type=ds.type, timeout=timeout)
                return []
            except Exception:
                logger.exception("adapter_discover_failed", datasource=ds.name, type=ds.type)
                return []

    # Run all adapter discoveries concurrently (bounded by semaphore)
    results = await asyncio.gather(*[_discover_one(ds) for ds in datasources])
    all_entries: list[MetricEntry] = []
    for entries in results:
        all_entries.extend(entries)

    # Cap total catalog size to stay within LLM context limits
    max_total = settings.max_metric_catalog_size
    if len(all_entries) > max_total:
        logger.warning(
            "metric_catalog_capped",
            original=len(all_entries),
            capped_to=max_total,
        )
        all_entries = all_entries[:max_total]

    logger.info(
        "cross_datasource_discovery_complete",
        datasource_count=len(datasources),
        total_metrics=len(all_entries),
        types_searched=list({ds.type for ds in datasources}),
    )
    return all_entries
