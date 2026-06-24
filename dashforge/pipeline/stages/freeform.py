"""LLM-driven freeform dashboard generation stage."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import structlog

from dashforge.agents.metrics_discovery import discover_metrics
from dashforge.agents.providers.base import TokenUsage
from dashforge.agents.query_builder import build_dashboard
from dashforge.dependencies import PipelineDependencies
from dashforge.logging import stage_log
from dashforge.models.schemas import DashboardSpec, DashResponse, Intent, MetricEntry
from dashforge.pipeline.failures import PipelineFailureFactory
from dashforge.pipeline.recording import PipelineRecorder
from dashforge.ranking import prerank_metrics

logger = structlog.get_logger()


@dataclass(frozen=True)
class FreeformBuildResult:
    dashboard_spec: DashboardSpec | None
    token_usage: TokenUsage
    failure: DashResponse | None = None


async def build_freeform_dashboard(
    *,
    intent: Intent,
    metric_catalog: list[MetricEntry],
    context_chunks: list[Any],
    deps: PipelineDependencies,
    recorder: PipelineRecorder,
    timings: dict[str, float],
    started_at: float,
) -> FreeformBuildResult:
    """Build a dashboard through LLM metric discovery and query generation."""
    if not metric_catalog:
        failure = PipelineFailureFactory.finish_failure(
            recorder=recorder,
            error="No metrics found for freeform generation",
            summary=(
                "Datasource metadata was available, but no metrics matched your query. "
                "Approve or teach a dashboard pattern for this service, or connect a "
                "datasource with matching series."
            ),
            timings=timings,
            started_at=started_at,
        )
        return FreeformBuildResult(dashboard_spec=None, token_usage=TokenUsage(), failure=failure)

    t_prerank = time.monotonic()
    ranked_catalog = prerank_metrics(intent, metric_catalog)
    stage_log(
        "metric_ranking",
        (time.monotonic() - t_prerank) * 1000,
        metrics_considered=len(metric_catalog),
        metrics_selected=len(ranked_catalog),
    )

    discovery_cache_key = deps.cache_key_factory(
        "discovery",
        intent.summary,
        ",".join(intent.keywords),
        ",".join(e.name for e in ranked_catalog[:20]),
    )
    provider = deps.llm_provider_factory() if deps.llm_provider_factory else None
    cached_discovery = deps.llm_cache.get(discovery_cache_key)
    discovery_usage = TokenUsage()
    total_usage = TokenUsage()
    t_disc = time.monotonic()
    if cached_discovery is not None:
        discovery = cached_discovery
        discovery_cached = True
    else:
        discovery, discovery_usage = await discover_metrics(intent, ranked_catalog, context_chunks, provider=provider)
        total_usage = total_usage + discovery_usage
        if discovery.metrics:
            deps.llm_cache.set(discovery_cache_key, discovery)
        discovery_cached = False

    stage_log(
        "metrics_discovery",
        (time.monotonic() - t_disc) * 1000,
        token_usage=discovery_usage if not discovery_cached else None,
        catalog_size=len(ranked_catalog),
        metrics_selected=len(discovery.metrics),
        cached=discovery_cached,
    )

    if not discovery.metrics:
        failure = PipelineFailureFactory.finish_failure(
            recorder=recorder,
            error="No relevant metrics found by LLM",
            summary=(
                "Could not find relevant metrics for your query. "
                "Try rephrasing with more specific service or metric names."
            ),
            timings=timings,
            started_at=started_at,
        )
        return FreeformBuildResult(dashboard_spec=None, token_usage=total_usage, failure=failure)

    valid_uids = {entry.datasource_uid for entry in metric_catalog}
    original_count = len(discovery.metrics)
    discovery.metrics = [metric for metric in discovery.metrics if metric.datasource_uid in valid_uids]
    dropped = original_count - len(discovery.metrics)
    if dropped:
        logger.warning("llm_hallucinated_uids_dropped", dropped=dropped)

    if not discovery.metrics:
        failure = PipelineFailureFactory.finish_failure(
            recorder=recorder,
            error="All LLM-selected metrics had invalid datasource UIDs",
            summary="LLM selected metrics with invalid datasource references. Try rephrasing your query.",
            timings=timings,
            started_at=started_at,
        )
        return FreeformBuildResult(dashboard_spec=None, token_usage=total_usage, failure=failure)

    t0 = time.monotonic()
    dashboard_spec, qb_usage = await build_dashboard(intent, discovery, ranked_catalog, provider=provider)
    timings["query_builder"] = time.monotonic() - t0
    total_usage = total_usage + qb_usage
    stage_log(
        "query_builder",
        (time.monotonic() - t0) * 1000,
        token_usage=qb_usage,
        metrics_input=len(discovery.metrics),
        panels_generated=len(dashboard_spec.panels),
    )
    return FreeformBuildResult(dashboard_spec=dashboard_spec, token_usage=total_usage)
