"""Orchestration pipeline: Prompt → Intent → Discover → Build → Publish."""
from __future__ import annotations

import asyncio
import time

import structlog

from dashforge.agents.intent import classify_intent
from dashforge.agents.metrics_discovery import discover_metrics
from dashforge.agents.query_builder import build_dashboard
from dashforge.archetypes.engine import compile_archetype
from dashforge.archetypes.templates import get_archetype
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import settings
from dashforge.context.enrichment import enrich_context
from dashforge.grafana.client import GrafanaClient
from dashforge.grafana.dashboard import publish_dashboard
from dashforge.validation import validate_dashboard_queries
from dashforge.grafana.datasource import (
    discover_all_metrics,
    filter_datasources_by_signal,
    filter_searchable_datasources,
    list_datasources,
)
from dashforge.models.schemas import DashRequest, DashResponse
from dashforge.ranking import prerank_metrics

logger = structlog.get_logger()

# Concurrency gate — prevents thundering-herd on LLM + Grafana APIs
_pipeline_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _pipeline_semaphore
    if _pipeline_semaphore is None:
        _pipeline_semaphore = asyncio.Semaphore(settings.pipeline_max_concurrent)
    return _pipeline_semaphore


async def run_pipeline(request: DashRequest) -> DashResponse:
    """End-to-end: natural language → Grafana dashboard URL."""
    sem = _get_semaphore()
    async with sem:
        try:
            return await asyncio.wait_for(
                _run_pipeline_inner(request),
                timeout=settings.pipeline_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.error("pipeline_timeout", user=request.user_id, timeout=settings.pipeline_timeout_seconds)
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary=f"Pipeline timed out after {settings.pipeline_timeout_seconds}s. "
                "Try a more specific query or check datasource connectivity.",
            )


async def _run_pipeline_inner(request: DashRequest) -> DashResponse:
    """Inner pipeline logic (wrapped with timeout + semaphore above)."""
    client = GrafanaClient()
    t_start = time.monotonic()
    timings: dict[str, float] = {}

    try:
        # ── 1. Intent Agent ──────────────────────────────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="intent",
                    prompt=request.prompt[:100],
                    user_id=request.user_id,
                    channel_id=request.channel_id)
        intent = await classify_intent(request.prompt)
        timings["intent"] = time.monotonic() - t0

        # ── 2. Context enrichment (optional) ───────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="context_enrichment")
        context_chunks = await enrich_context(intent)
        timings["context"] = time.monotonic() - t0

        # ── 3. Datasource discovery ────────────────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="datasource_discovery")
        all_ds = await list_datasources(client)

        # First: filter by signal type, then ensure they're searchable
        signal_types = [s.value for s in intent.signals]
        relevant_ds = filter_datasources_by_signal(all_ds, signal_types)
        if not relevant_ds:
            relevant_ds = filter_datasources_by_signal(all_ds, ["metrics"])

        # Keep only datasources we have adapters for
        searchable_ds = filter_searchable_datasources(relevant_ds)
        if not searchable_ds:
            # Last resort: try all searchable datasources
            searchable_ds = filter_searchable_datasources(all_ds)

        if not searchable_ds:
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No searchable datasources found in Grafana. "
                "Supported types: Prometheus, CloudWatch, Loki, Elasticsearch, Graphite, InfluxDB.",
            )

        ds_types = list({ds.type for ds in searchable_ds})
        logger.info("pipeline_step", step="datasource_filtered",
                    count=len(searchable_ds), types=ds_types)

        timings["datasource_discovery"] = time.monotonic() - t0

        # ── 4. Cross-datasource metric discovery ──────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="metrics_fetch")
        metric_catalog = await discover_all_metrics(client, searchable_ds, intent.keywords)
        timings["metrics_fetch"] = time.monotonic() - t0

        if not metric_catalog:
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No metrics found across any datasource. "
                "Verify your datasources are configured and have data.",
            )

        # ── 4a. Check for investigation archetype match ────────────────
        t0 = time.monotonic()
        archetype = get_archetype(intent.problem_type)

        if archetype is not None:
            # ── ARCHETYPE PATH: deterministic, no LLM needed ──────────
            logger.info("pipeline_step", step="archetype_compile",
                        archetype=archetype.id,
                        problem_type=intent.problem_type)
            dashboard_spec = compile_archetype(archetype, intent, metric_catalog)
            timings["archetype_compile"] = time.monotonic() - t0
        else:
            # ── FREEFORM PATH: LLM-driven discovery + query generation ─
            logger.info("pipeline_step", step="freeform_path",
                        problem_type=intent.problem_type)

            # Pre-rank to reduce LLM token cost
            ranked_catalog = prerank_metrics(intent, metric_catalog)
            logger.info("pipeline_step", step="prerank",
                        before=len(metric_catalog), after=len(ranked_catalog))

            # Metrics Discovery LLM (cached)
            discovery_cache_key = make_cache_key(
                "discovery", intent.summary, ",".join(intent.keywords),
                ",".join(e.name for e in ranked_catalog[:20]),
            )
            cached_discovery = llm_cache.get(discovery_cache_key)
            if cached_discovery is not None:
                logger.info("pipeline_step", step="metrics_discovery", cached=True)
                discovery = cached_discovery
            else:
                logger.info("pipeline_step", step="metrics_discovery",
                            catalog_size=len(ranked_catalog))
                discovery = await discover_metrics(intent, ranked_catalog, context_chunks)
                if discovery.metrics:
                    llm_cache.set(discovery_cache_key, discovery)

            if not discovery.metrics:
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary="Could not find relevant metrics for your query. "
                    "Try rephrasing with more specific service or metric names.",
                )

            # Post-validate LLM output
            valid_uids = {e.datasource_uid for e in metric_catalog}
            original_count = len(discovery.metrics)
            discovery.metrics = [
                m for m in discovery.metrics
                if m.datasource_uid in valid_uids
            ]
            dropped = original_count - len(discovery.metrics)
            if dropped:
                logger.warning("llm_hallucinated_uids_dropped", dropped=dropped)

            if not discovery.metrics:
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary="LLM selected metrics with invalid datasource references. "
                    "Try rephrasing your query.",
                )

            # Query Builder Agent
            t0 = time.monotonic()
            logger.info("pipeline_step", step="query_builder")
            dashboard_spec = await build_dashboard(intent, discovery, ranked_catalog)
            timings["query_builder"] = time.monotonic() - t0

        # ── 6. Validate queries return data ─────────────────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="query_validation")
        dashboard_spec, validation_warnings = await validate_dashboard_queries(
            client, dashboard_spec
        )
        timings["query_validation"] = time.monotonic() - t0

        if not dashboard_spec.panels:
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No panels returned data for your query. "
                "The service or metrics you asked about may not exist "
                "in the connected datasources.\n"
                + "\n".join(validation_warnings),
            )

        # ── 7. Publish to Grafana ────────────────────────────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="publish")
        url, uid = await publish_dashboard(client, dashboard_spec)
        timings["publish"] = time.monotonic() - t0

        path_used = "archetype" if archetype else "freeform"
        ds_info = (
            ", ".join({e.datasource_name for e in metric_catalog[:5]})
            if archetype
            else ", ".join({m.datasource_name for m in discovery.metrics})
        )
        summary = (
            f"Created dashboard **{dashboard_spec.title}** with "
            f"{len(dashboard_spec.panels)} panels.\n"
            f"Timerange: last {dashboard_spec.timerange}\n"
            f"Datasources used: {ds_info}\n"
            f"Path: {path_used}"
        )

        total_s = time.monotonic() - t_start
        timings["total"] = total_s
        timings_rounded = {k: round(v, 2) for k, v in timings.items()}

        logger.info(
            "pipeline_complete",
            user_id=request.user_id,
            channel_id=request.channel_id,
            dashboard_uid=uid,
            panel_count=len(dashboard_spec.panels),
            path=path_used,
            timings=timings_rounded,
        )

        return DashResponse(
            dashboard_url=url,
            dashboard_uid=uid,
            panel_count=len(dashboard_spec.panels),
            summary=summary,
        )

    finally:
        await client.close()
