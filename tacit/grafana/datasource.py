from __future__ import annotations

import asyncio
from typing import Literal

import structlog

from tacit.config import Settings
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS, safe_failure_diagnostics
from tacit.grafana.adapters.registry import get_adapter, supported_datasource_types
from tacit.grafana.client import GrafanaClient
from tacit.models.schemas import DatasourceInfo, MetricEntry
from tacit.runtime_ownership import resolve_remote_runtime_settings

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


def _cap_metric_results(results: list[list[MetricEntry]], max_total: int) -> list[MetricEntry]:
    """Cap a multi-datasource catalog without starving later datasources."""
    total = sum(len(entries) for entries in results)
    if total <= max_total:
        return [entry for entries in results for entry in entries]

    capped: list[MetricEntry] = []
    offset = 0
    while len(capped) < max_total:
        added = False
        for entries in results:
            if offset < len(entries):
                capped.append(entries[offset])
                added = True
                if len(capped) == max_total:
                    break
        if not added:
            break
        offset += 1
    return capped


async def list_datasources(client: GrafanaClient, work_budget=None) -> list[DatasourceInfo]:
    """Fetch all datasources from Grafana and return typed info objects."""
    raw = (
        await client.list_datasources()
        if work_budget is None
        else await client.acquisition_request("GET", "/api/datasources", work_budget)
    )
    if not isinstance(raw, list) or len(raw) > 5000:
        raise ValueError("datasource catalog cardinality limit")
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
    *,
    runtime_settings: Settings | None = None,
) -> list[MetricEntry]:
    """Search ALL given datasources for metrics, using per-type adapters.

    Returns a unified list of MetricEntry objects across every datasource type,
    capped at settings.max_metric_catalog_size to stay within LLM context limits.
    """
    active_settings = resolve_remote_runtime_settings(
        boundary="grafana_metric_discovery",
        owner=client,
        provider="grafana",
        explicit_settings=runtime_settings,
    )
    sem = asyncio.Semaphore(active_settings.adapter_max_concurrent)
    timeout = active_settings.adapter_timeout_seconds

    async def _discover_one(ds: DatasourceInfo) -> list[MetricEntry]:
        adapter = get_adapter(ds)
        if adapter is None:
            logger.debug("no_adapter", datasource=ds.name, type=ds.type)
            return []
        async with sem:
            try:
                entries = await asyncio.wait_for(
                    adapter.discover_metrics(client, ds, keywords),
                    timeout=timeout,
                )
                return [entry.model_copy(update={"datasource_is_default": ds.is_default}) for entry in entries]
            except TimeoutError:
                logger.warning("adapter_timeout", datasource=ds.name, type=ds.type, timeout=timeout)
                return []
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except Exception as exc:
                logger.warning(
                    "adapter_discover_failed",
                    datasource=ds.name,
                    type=ds.type,
                    **safe_failure_diagnostics(exc, reason_code="adapter_discover_failed"),
                )
                return []

    # Run all adapter discoveries concurrently (bounded by semaphore)
    results = await asyncio.gather(*[_discover_one(ds) for ds in datasources])
    # Cap total catalog size to stay within LLM context limits
    max_total = active_settings.max_metric_catalog_size
    original_count = sum(len(entries) for entries in results)
    if original_count > max_total:
        logger.warning(
            "metric_catalog_capped",
            original=original_count,
            capped_to=max_total,
        )
    all_entries = _cap_metric_results(results, max_total)

    logger.info(
        "cross_datasource_discovery_complete",
        datasource_count=len(datasources),
        total_metrics=len(all_entries),
        types_searched=list({ds.type for ds in datasources}),
    )
    return all_entries


async def discover_all_metrics_with_metadata(client, datasources, keywords, work_budget, *, runtime_settings=None):
    """Typed cache-free route with bounded scheduling and shared retention."""
    from tacit.catalog import CatalogSourceStatus, MetricCatalogResult
    from tacit.config import validate_acquisition_runtime
    from tacit.signals.resolution import AcquisitionWorkLimitError

    active_settings = resolve_remote_runtime_settings(
        boundary="grafana_metric_discovery",
        owner=client,
        provider="grafana",
        explicit_settings=runtime_settings,
    )
    validate_acquisition_runtime(active_settings)
    if len(datasources) > 5000:
        raise ValueError("datasource catalog cardinality limit")
    entries: list[MetricEntry] = []
    sources: list[CatalogSourceStatus] = []
    enrichments = []
    source_slots = max(0, work_budget.max_acquisition_calls - work_budget.acquisition_calls)
    admitted_sources = 0
    # Sequential admission is deliberately a stricter scheduling bound than the
    # configured adapter maximum; no unbounded population of waiting tasks.
    for ds in sorted(datasources, key=lambda source: (source.uid, source.type)):
        if work_budget.acquisition_calls >= work_budget.max_acquisition_calls:
            sources.append(
                CatalogSourceStatus(
                    backend="grafana",
                    terms=(),
                    summary=True,
                    disposition="incomplete",
                    reasons=("work_limit",),
                )
            )
            break
        adapter = get_adapter(ds)
        if adapter is None:
            continue
        if admitted_sources >= source_slots:
            sources.append(CatalogSourceStatus(backend="grafana", summary=True, reasons=("work_limit",)))
            break
        admitted_sources += 1
        try:
            typed = getattr(adapter, "discover_metrics_with_metadata", None)
            if callable(typed):
                phased = bool(getattr(adapter, "supports_acquisition_inventory_phase", False))
                phase_kwargs = {"enrich": False} if phased else {}
                result = await asyncio.wait_for(
                    typed(client, ds, keywords, work_budget, **phase_kwargs),
                    timeout=active_settings.adapter_timeout_seconds,
                )
                if phased:
                    enrichments.append((ds, result.as_list()))
            else:
                from tacit.acquisition import legacy_acquisition_catalog

                result = legacy_acquisition_catalog(
                    await asyncio.wait_for(
                        adapter.discover_metrics(client, ds, keywords), timeout=active_settings.adapter_timeout_seconds
                    ),
                    backend="grafana",
                    terms=keywords,
                    work_budget=work_budget,
                )
            from tacit.acquisition import retain_acquisition_facts

            additions = [entry.model_copy(update={"datasource_is_default": ds.is_default}) for entry in result.entries]
            entries, limited = retain_acquisition_facts(additions, work_budget, previous=entries)
            if limited:
                sources.append(
                    CatalogSourceStatus(backend="grafana", summary=True, reasons=("work_limit", "local_cap"))
                )
            sources.extend(result.sources)
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except AcquisitionWorkLimitError:
            sources.append(
                CatalogSourceStatus(
                    backend="grafana",
                    datasource_uid=ds.uid,
                    datasource_type=ds.type,
                    query_language=adapter.query_language,
                    terms=tuple(keywords),
                    disposition="incomplete",
                    reasons=("work_limit",),
                )
            )
        except Exception:
            sources.append(
                CatalogSourceStatus(
                    backend="grafana",
                    datasource_uid=ds.uid,
                    datasource_type=ds.type,
                    query_language=adapter.query_language,
                    terms=tuple(keywords),
                    disposition="unavailable",
                    reasons=("provider_failed",),
                )
            )
    from tacit.acquisition import enrich_adapter_metrics

    updates: dict[tuple[str, str, str, str], list[MetricEntry]] = {}
    for ds, retained in enrichments:
        if work_budget.acquisition_calls >= work_budget.max_acquisition_calls:
            sources.append(CatalogSourceStatus(backend="grafana", summary=True, reasons=("work_limit",)))
            break
        enriched = await enrich_adapter_metrics(client, ds, retained, work_budget)
        sources.extend(enriched.sources)
        for entry in enriched.entries:
            updates.setdefault((entry.datasource_uid, entry.query_language, entry.name, entry.namespace), []).append(
                entry
            )
    expanded = []
    for entry in entries:
        expanded.extend(updates.get((entry.datasource_uid, entry.query_language, entry.name, entry.namespace), [entry]))
        if len(expanded) > 5000:
            raise ValueError("catalog raw metadata fact limit")
    from tacit.acquisition import retain_acquisition_facts

    entries, _ = retain_acquisition_facts(expanded, work_budget)
    summaries = [source for source in sources if source.summary]
    sources = [source for source in sources if not source.summary]
    if summaries:
        reasons = tuple(sorted({reason for source in summaries for reason in source.reasons}))
        disposition: Literal["unavailable", "incomplete"] = (
            "unavailable" if all(source.disposition == "unavailable" for source in summaries) else "incomplete"
        )
        sources.append(CatalogSourceStatus(backend="grafana", summary=True, reasons=reasons, disposition=disposition))
    return MetricCatalogResult(entries=tuple(entries), sources=tuple(sources))
