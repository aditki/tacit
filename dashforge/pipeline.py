"""Orchestration pipeline: Prompt → Intent → Discover → Build → Publish."""
from __future__ import annotations

import asyncio
import time

import structlog

from dashforge.agents.intent import classify_intent
from dashforge.agents.metrics_discovery import discover_metrics
from dashforge.agents.query_builder import build_dashboard
from dashforge.archetypes.engine import blend_archetypes, compile_archetype
from dashforge.archetypes.templates import get_archetype, get_archetypes_by_confidence
from dashforge.backends import get_active_backends
from dashforge.backends.base import PublishResult
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import settings
from dashforge.context.enrichment import enrich_context
from dashforge.models.schemas import DashRequest, DashResponse
from dashforge.history import get_investigation_store
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
            try:
                store = get_investigation_store()
                inv_id = store.start(request.prompt, request.user_id, request.channel_id)
                store.finish(inv_id, status="timeout", error=f"Timed out after {settings.pipeline_timeout_seconds}s")
            except Exception:
                pass
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary=f"Pipeline timed out after {settings.pipeline_timeout_seconds}s. "
                "Try a more specific query or check datasource connectivity.",
            )


async def _run_pipeline_inner(request: DashRequest) -> DashResponse:
    """Inner pipeline logic (wrapped with timeout + semaphore above).

    Uses the backend adapter pattern: each enabled vendor (Grafana, SignalFx,
    etc.) is a DashboardBackend instance.  The pipeline iterates over backends
    for discovery, validation, and publishing — zero vendor-specific if/else.
    """
    backends = get_active_backends()
    if not backends:
        return DashResponse(
            dashboard_url="", dashboard_uid="", panel_count=0,
            summary="No dashboard backends are enabled. "
            "Enable at least one of: grafana, signalfx.",
        )

    primary = backends[0]  # determines query language for compilation

    t_start = time.monotonic()
    timings: dict[str, float] = {}
    history = get_investigation_store()
    inv_id = history.start(request.prompt, request.user_id or "", request.channel_id or "")

    try:
        # ── 1. Intent Agent ──────────────────────────────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="intent",
                    prompt=request.prompt[:100],
                    user_id=request.user_id,
                    channel_id=request.channel_id)
        intent = await classify_intent(request.prompt)
        timings["intent"] = time.monotonic() - t0

        try:
            history.record_intent(
                inv_id,
                summary=intent.summary,
                domain=intent.domain,
                services=intent.services,
                keywords=intent.keywords,
                signals=[s.value for s in intent.signals],
                problem_type=intent.problem_type,
                archetypes=[{"type": a.type, "confidence": a.confidence} for a in intent.archetypes],
                timerange=intent.timerange,
            )
        except Exception:
            logger.warning("history_record_intent_failed", exc_info=True)

        # ── 2. Context enrichment (optional) ───────────────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="context_enrichment")
        context_chunks = await enrich_context(intent)
        timings["context"] = time.monotonic() - t0

        # ── 3. Metric discovery — each backend contributes ───────────
        t0 = time.monotonic()
        metric_catalog = []
        ds_types: list[str] = []
        for backend in backends:
            logger.info("pipeline_step", step="discovery", backend=backend.name)
            entries = await backend.discover_metrics(intent.keywords, intent)
            metric_catalog.extend(entries)
            if entries:
                ds_types.append(backend.name)
        timings["metrics_fetch"] = time.monotonic() - t0

        try:
            history.record_discovery(
                inv_id,
                datasources_found=len(ds_types),
                datasource_types=ds_types,
                metrics_catalog_size=len(metric_catalog),
            )
        except Exception:
            logger.warning("history_record_discovery_failed", exc_info=True)

        if not metric_catalog:
            history.finish(inv_id, status="failed", error="No metrics found",
                           timings=timings, total_time=time.monotonic() - t_start)
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No metrics found across any datasource. "
                "Verify your datasources are configured and have data.",
            )

        # ── 4. Multi-label archetype matching ────────────────────────
        t0 = time.monotonic()
        ranked_archetypes = get_archetypes_by_confidence(
            intent.archetypes, min_confidence=0.3
        )
        # Fallback: try legacy single-label lookup
        if not ranked_archetypes:
            legacy = get_archetype(intent.problem_type)
            if legacy is not None:
                ranked_archetypes = [(legacy, 0.9)]

        # Target query language comes from the primary backend
        target_language = primary.query_language

        if ranked_archetypes:
            primary_arch, primary_conf = ranked_archetypes[0]
            # ── ARCHETYPE PATH: deterministic, no LLM needed ──────────
            logger.info("pipeline_step", step="archetype_match",
                        primary=primary_arch.id,
                        primary_confidence=primary_conf,
                        total_matches=len(ranked_archetypes),
                        problem_type=intent.problem_type,
                        target_language=target_language)

            if len(ranked_archetypes) > 1:
                dashboard_spec = blend_archetypes(
                    ranked_archetypes, intent, metric_catalog,
                    target_language=target_language,
                )
            else:
                dashboard_spec = compile_archetype(
                    primary_arch, intent, metric_catalog,
                    target_language=target_language,
                )
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
                history.finish(inv_id, status="failed", error="No relevant metrics found by LLM",
                               timings=timings, total_time=time.monotonic() - t_start)
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
                history.finish(inv_id, status="failed", error="All LLM-selected metrics had invalid datasource UIDs",
                               timings=timings, total_time=time.monotonic() - t_start)
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

        # ── 5. Validate queries — primary backend validates ──────────
        t0 = time.monotonic()
        logger.info("pipeline_step", step="query_validation", backend=primary.name)
        dashboard_spec, validation_warnings = await primary.validate_queries(dashboard_spec)
        timings["query_validation"] = time.monotonic() - t0

        # Record queries after validation
        try:
            queries_for_history = [
                {"expr": q.expr, "panel_title": p.title}
                for p in dashboard_spec.panels for q in p.queries if q.expr
            ]
            metrics_for_history = list({
                q.expr.split("{")[0].split("(")[-1].strip()
                for p in dashboard_spec.panels for q in p.queries if q.expr
            })
            history.record_queries(
                inv_id,
                metrics_selected=metrics_for_history,
                generated_queries=queries_for_history,
                panel_count=len(dashboard_spec.panels),
                path_used="archetype" if ranked_archetypes else "freeform",
            )
        except Exception:
            logger.warning("history_record_queries_failed", exc_info=True)

        if not dashboard_spec.panels:
            history.finish(inv_id, status="failed", error="All panels empty after validation",
                           timings=timings, total_time=time.monotonic() - t_start)
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No panels returned data for your query. "
                "The service or metrics you asked about may not exist "
                "in the connected datasources.\n"
                + "\n".join(validation_warnings),
            )

        # ── 6. Publish — each backend publishes independently ────────
        publish_results: dict[str, PublishResult] = {}
        for backend in backends:
            t0 = time.monotonic()
            logger.info("pipeline_step", step="publish", backend=backend.name)
            try:
                result = await backend.publish(dashboard_spec)
                publish_results[backend.name] = result
            except Exception:
                logger.warning("publish_failed", backend=backend.name, exc_info=True)
            timings[f"{backend.name}_publish"] = time.monotonic() - t0

        # Effective identifiers — first successful backend wins
        grafana_result = publish_results.get("grafana", PublishResult())
        sfx_result = publish_results.get("signalfx", PublishResult())
        effective_uid = grafana_result.uid or sfx_result.uid or ""
        effective_url = grafana_result.url or sfx_result.url or ""

        path_used = "archetype" if ranked_archetypes else "freeform"
        ds_info = (
            ", ".join({e.datasource_name for e in metric_catalog[:5]})
            if ranked_archetypes
            else ", ".join({m.datasource_name for m in discovery.metrics})
        )
        summary_parts = [
            f"Created dashboard **{dashboard_spec.title}** with "
            f"{len(dashboard_spec.panels)} panels.",
            f"Timerange: last {dashboard_spec.timerange}",
            f"Datasources used: {ds_info}",
            f"Path: {path_used}",
        ]
        for name, result in publish_results.items():
            if result.url:
                summary_parts.append(f"{name.title()}: {result.url}")
        summary = "\n".join(summary_parts)

        total_s = time.monotonic() - t_start
        timings["total"] = total_s
        timings_rounded = {k: round(v, 2) for k, v in timings.items()}

        # Record validation results
        try:
            pre_validation_panels = len(dashboard_spec.panels) + len(validation_warnings)
            history.record_validation(
                inv_id,
                warnings=validation_warnings,
                panels_dropped=pre_validation_panels - len(dashboard_spec.panels),
                final_panel_count=len(dashboard_spec.panels),
            )
        except Exception:
            logger.warning("history_record_validation_failed", exc_info=True)

        logger.info(
            "pipeline_complete",
            user_id=request.user_id,
            channel_id=request.channel_id,
            dashboard_uid=effective_uid,
            panel_count=len(dashboard_spec.panels),
            path=path_used,
            timings=timings_rounded,
        )

        # Record final result
        try:
            history.finish(
                inv_id,
                status="success",
                dashboard_uid=effective_uid,
                dashboard_url=effective_url,
                timings=timings_rounded,
                total_time=total_s,
            )
        except Exception:
            logger.warning("history_finish_failed", exc_info=True)

        # ── 7. Record provenance for feedback system ──────────────────
        try:
            from dashforge.feedback import get_feedback_store
            store = get_feedback_store()
            metrics_used = list({
                q.expr.split("{")[0].split("(")[-1].strip()
                for p in dashboard_spec.panels
                for q in p.queries
                if q.expr
            })
            store.record_provenance(
                dashboard_uid=effective_uid,
                prompt=request.prompt,
                problem_type=intent.problem_type,
                archetypes=[
                    {"type": a.type, "confidence": a.confidence}
                    for a in intent.archetypes
                ],
                metrics_used=metrics_used,
                panel_count=len(dashboard_spec.panels),
                path_used=path_used,
                dashboard_url=effective_url,
                user_id=request.user_id,
                channel_id=request.channel_id,
            )
        except Exception:
            logger.warning("provenance_record_failed", exc_info=True)

        return DashResponse(
            dashboard_url=grafana_result.url,
            dashboard_uid=effective_uid,
            panel_count=len(dashboard_spec.panels),
            summary=summary,
            signalfx_url=sfx_result.url,
            signalfx_dashboard_id=sfx_result.uid,
        )

    finally:
        for backend in backends:
            try:
                await backend.close()
            except Exception:
                logger.warning("backend_close_failed", backend=backend.name, exc_info=True)
