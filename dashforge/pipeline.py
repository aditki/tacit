"""Orchestration pipeline: Prompt → Intent → Discover → Build → Publish."""

from __future__ import annotations

import asyncio
import time

import structlog

from dashforge.agents.intent import classify_intent
from dashforge.agents.metrics_discovery import discover_metrics
from dashforge.agents.providers.base import TokenUsage
from dashforge.agents.query_builder import build_dashboard
from dashforge.archetypes.engine import blend_archetypes, compile_archetype, rank_archetypes_by_coverage
from dashforge.archetypes.templates import (
    get_archetype,
    get_archetypes_by_confidence,
    get_archetypes_by_learning_context,
)
from dashforge.backends import get_active_backends
from dashforge.backends.base import PublishResult
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import settings
from dashforge.context.enrichment import enrich_context
from dashforge.dependencies import PipelineDependencies
from dashforge.errors import EvidenceResolutionError
from dashforge.evidence import (
    contributing_archetypes,
    resolve_requirements_for_archetypes,
)
from dashforge.history import get_investigation_store
from dashforge.logging import bind_request_id, stage_log, unbind_request_id
from dashforge.models.schemas import (
    DashRequest,
    DashResponse,
    EvidenceRequirement,
    EvidenceResolution,
)
from dashforge.pipeline_discovery import (
    confirm_colloquial_keywords,
    discover_catalogs,
    discovery_keywords,
    discovery_stage_status,
    semantic_mapping_diagnostics,
)
from dashforge.pipeline_records import (
    compiled_query_diagnostics,
    dashboard_summary,
    history_archetypes,
    history_signals,
    query_history_payload,
    record_selected_intent,
    record_stage,
    surviving_datasource_names,
)
from dashforge.pipeline_validation import validate_dashboard_and_evidence
from dashforge.ranking import prerank_metrics

logger = structlog.get_logger()

# Backward-compatible test/import aliases for helpers that now live in smaller modules.
_compiled_query_diagnostics = compiled_query_diagnostics
_discovery_keywords = discovery_keywords
_history_archetypes = history_archetypes
_history_signals = history_signals
_semantic_mapping_diagnostics = semantic_mapping_diagnostics


# Concurrency gate — prevents thundering-herd on LLM + Grafana APIs
_pipeline_semaphore: asyncio.Semaphore | None = None


def _default_dependencies() -> PipelineDependencies:
    """Build default dependencies through pipeline-level patch points.

    Several harnesses patch these names on ``dashforge.pipeline`` to create a
    cold isolated runtime. Keeping the default lookup here preserves that
    behavior while the core runner still accepts explicit dependencies.
    """
    return PipelineDependencies(
        settings=settings,
        backend_factory=get_active_backends,
        history_store_factory=get_investigation_store,
        llm_cache=llm_cache,
        cache_key_factory=make_cache_key,
    )


def _get_semaphore(max_concurrent: int) -> asyncio.Semaphore:
    global _pipeline_semaphore
    if _pipeline_semaphore is None:
        _pipeline_semaphore = asyncio.Semaphore(max_concurrent)
    return _pipeline_semaphore


async def run_pipeline(request: DashRequest, deps: PipelineDependencies | None = None) -> DashResponse:
    """End-to-end: natural language → Grafana dashboard URL."""
    deps = deps or _default_dependencies()
    runtime_settings = deps.settings
    bind_request_id()
    sem = _get_semaphore(runtime_settings.pipeline_max_concurrent)
    try:
        async with sem:
            try:
                return await asyncio.wait_for(
                    _run_pipeline_inner(request, deps),
                    timeout=runtime_settings.pipeline_timeout_seconds,
                )
            except TimeoutError:
                logger.error(
                    "pipeline_timeout",
                    user=request.user_id,
                    timeout=runtime_settings.pipeline_timeout_seconds,
                )
                try:
                    store = deps.history_store_factory()
                    inv_id = store.start(request.prompt, request.user_id, request.channel_id)
                    store.finish(
                        inv_id,
                        status="timeout",
                        error=f"Timed out after {runtime_settings.pipeline_timeout_seconds}s",
                    )
                except Exception:
                    pass
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary=f"Pipeline timed out after {runtime_settings.pipeline_timeout_seconds}s. "
                    "Try a more specific query or check datasource connectivity.",
                )
    finally:
        unbind_request_id()


async def _run_pipeline_inner(request: DashRequest, deps: PipelineDependencies) -> DashResponse:
    """Inner pipeline logic (wrapped with timeout + semaphore above).

    Uses the backend adapter pattern: each enabled vendor (Grafana, SignalFx,
    etc.) is a DashboardBackend instance.  The pipeline iterates over backends
    for discovery, validation, and publishing — zero vendor-specific if/else.
    """
    runtime_settings = deps.settings
    backends = deps.backend_factory()
    if not backends:
        return DashResponse(
            dashboard_url="",
            dashboard_uid="",
            panel_count=0,
            summary="No dashboard backends are enabled. " "Enable at least one of: grafana, signalfx.",
        )

    primary = backends[0]  # determines query language for compilation

    t_start = time.monotonic()
    timings: dict[str, float] = {}
    history = deps.history_store_factory()
    inv_id = history.start(request.prompt, request.user_id or "", request.channel_id or "")

    cumulative_tokens = TokenUsage()

    try:
        # ── 1. Intent Agent ──────────────────────────────────────────
        t0 = time.monotonic()
        intent, intent_usage = await classify_intent(request.prompt)
        timings["intent"] = time.monotonic() - t0
        cumulative_tokens = cumulative_tokens + intent_usage
        stage_log(
            "intent",
            (time.monotonic() - t0) * 1000,
            token_usage=intent_usage,
            prompt=request.prompt[:100],
            user_id=request.user_id,
            archetypes_detected=len(intent.archetypes),
            domain=intent.domain,
        )

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
        context_chunks = await enrich_context(intent)
        timings["context"] = time.monotonic() - t0
        stage_log(
            "context_enrichment",
            (time.monotonic() - t0) * 1000,
            chunks_returned=len(context_chunks),
        )

        # ── 3. Metric discovery — each backend contributes ───────────
        t0 = time.monotonic()
        catalog_discovery = await discover_catalogs(backends, intent)
        metric_catalog = catalog_discovery.metric_catalog
        datasource_catalog = catalog_discovery.datasource_catalog
        catalog_for_compile = catalog_discovery.catalog_for_compile
        timings["metrics_fetch"] = time.monotonic() - t0
        stage_log(
            "metrics_fetch",
            (time.monotonic() - t0) * 1000,
            backends_queried=len(backends),
            datasource_types=catalog_discovery.datasource_types,
            metrics_found=len(metric_catalog),
            datasource_targets_found=len(datasource_catalog),
        )

        try:
            history.record_discovery(
                inv_id,
                datasources_found=len(catalog_discovery.datasource_types),
                datasource_types=catalog_discovery.datasource_types,
                metrics_catalog_size=len(metric_catalog),
            )
        except Exception:
            logger.warning("history_record_discovery_failed", exc_info=True)

        discovery_status, discovery_reason, discovery_details = discovery_stage_status(catalog_discovery)
        record_stage(history, inv_id, "discovery", discovery_status, discovery_reason, **discovery_details)

        mapping_status, mapping_reason, mapping_details = semantic_mapping_diagnostics(metric_catalog)
        record_stage(
            history,
            inv_id,
            "semantic_mapping",
            mapping_status,
            mapping_reason,
            **mapping_details,
        )

        confirmed_keywords = confirm_colloquial_keywords(intent, metric_catalog, primary.query_language)
        if confirmed_keywords:
            logger.info("colloquial_evidence_confirmed", keywords=confirmed_keywords)

        if not catalog_for_compile:
            ranked_archetypes = get_archetypes_by_confidence(intent.archetypes, min_confidence=0.3)
            ranked_ids = {arch.id for arch, _ in ranked_archetypes}
            learned_archetypes = get_archetypes_by_learning_context(
                intent,
                metric_catalog,
                min_confidence=0.35,
                exclude_ids=ranked_ids,
            )
            if learned_archetypes:
                ranked_archetypes.extend(learned_archetypes)
                ranked_archetypes.sort(key=lambda item: item[1], reverse=True)
            record_selected_intent(history, inv_id, intent, ranked_archetypes, learned_archetypes)

            unavailable = [
                backend.name
                for backend in backends
                if not getattr(getattr(backend, "last_discovery_status", None), "available", True)
            ]
            if unavailable:
                names = ", ".join(unavailable)
                error = f"Datasource discovery failed for: {names}"
                summary = (
                    f"Could not connect to {names} during datasource discovery. "
                    "Verify the backend is running and reachable, then retry."
                )
            else:
                error = "No metrics or datasource targets found"
                summary = (
                    "No metrics found across any datasource. " "Verify your datasources are configured and have data."
                )
            history.finish(
                inv_id,
                status="failed",
                error=error,
                timings=timings,
                total_time=time.monotonic() - t_start,
            )
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary=summary,
            )

        # ── 4. Multi-label archetype matching ────────────────────
        t0 = time.monotonic()
        ranked_archetypes = get_archetypes_by_confidence(intent.archetypes, min_confidence=0.3)
        ranked_ids = {arch.id for arch, _ in ranked_archetypes}
        learned_archetypes = get_archetypes_by_learning_context(
            intent,
            metric_catalog,
            min_confidence=0.35,
            exclude_ids=ranked_ids,
        )
        if learned_archetypes:
            ranked_archetypes.extend(learned_archetypes)
            ranked_archetypes.sort(key=lambda item: item[1], reverse=True)

        # Fallback: try legacy single-label lookup
        if not ranked_archetypes:
            legacy = get_archetype(intent.problem_type)
            if legacy is not None:
                ranked_archetypes = [(legacy, 0.9)]

        record_selected_intent(history, inv_id, intent, ranked_archetypes, learned_archetypes)

        # Target query language comes from the primary backend
        target_language = primary.query_language

        # Coverage-rank + cap before deciding compile vs blend, so a strongly
        # matching archetype wins over many generic templates and the panel
        # explosion is bounded.
        if ranked_archetypes:
            ranked_archetypes = rank_archetypes_by_coverage(
                ranked_archetypes,
                catalog_for_compile,
                target_language=target_language,
                services=intent.services,
                max_archetypes=runtime_settings.max_blended_archetypes,
                min_secondary_coverage=runtime_settings.min_secondary_coverage,
            )

        if ranked_archetypes:
            primary_arch, primary_conf = ranked_archetypes[0]
            # ── ARCHETYPE PATH: deterministic, no LLM needed ──────────

            # Signal resolution happens inside compile_archetype/blend_archetypes
            # via _resolve_archetype_signals — substitutes missing metrics
            # with signal-resolved alternatives from the live catalog.

            if len(ranked_archetypes) > 1:
                dashboard_spec = blend_archetypes(
                    ranked_archetypes,
                    intent,
                    catalog_for_compile,
                    target_language=target_language,
                )
            else:
                dashboard_spec = compile_archetype(
                    primary_arch,
                    intent,
                    catalog_for_compile,
                    target_language=target_language,
                )
            timings["archetype_compile"] = time.monotonic() - t0
            stage_log(
                "archetype_compile",
                (time.monotonic() - t0) * 1000,
                primary_archetype=primary_arch.id,
                primary_confidence=primary_conf,
                archetypes_matched=len(ranked_archetypes),
                learned_archetypes_matched=len(learned_archetypes),
                panels_generated=len(dashboard_spec.panels),
                target_language=target_language,
                signal_bindings_count=len(primary_arch.signal_bindings),
            )
        else:
            # ── FREEFORM PATH: LLM-driven discovery + query generation ─
            if not metric_catalog:
                history.finish(
                    inv_id,
                    status="failed",
                    error="No metrics found for freeform generation",
                    timings=timings,
                    total_time=time.monotonic() - t_start,
                )
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary=(
                        "Datasource metadata was available, but no metrics matched your query. "
                        "Approve or teach a dashboard pattern for this service, or connect a "
                        "datasource with matching series."
                    ),
                )

            # Pre-rank to reduce LLM token cost
            t_prerank = time.monotonic()
            ranked_catalog = prerank_metrics(intent, metric_catalog)
            stage_log(
                "metric_ranking",
                (time.monotonic() - t_prerank) * 1000,
                metrics_considered=len(metric_catalog),
                metrics_selected=len(ranked_catalog),
            )

            # Metrics Discovery LLM (cached)
            discovery_cache_key = deps.cache_key_factory(
                "discovery",
                intent.summary,
                ",".join(intent.keywords),
                ",".join(e.name for e in ranked_catalog[:20]),
            )
            cached_discovery = deps.llm_cache.get(discovery_cache_key)
            discovery_usage = TokenUsage()
            t_disc = time.monotonic()
            if cached_discovery is not None:
                discovery = cached_discovery
                discovery_cached = True
            else:
                discovery, discovery_usage = await discover_metrics(intent, ranked_catalog, context_chunks)
                cumulative_tokens = cumulative_tokens + discovery_usage
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
                history.finish(
                    inv_id,
                    status="failed",
                    error="No relevant metrics found by LLM",
                    timings=timings,
                    total_time=time.monotonic() - t_start,
                )
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
            discovery.metrics = [m for m in discovery.metrics if m.datasource_uid in valid_uids]
            dropped = original_count - len(discovery.metrics)
            if dropped:
                logger.warning("llm_hallucinated_uids_dropped", dropped=dropped)

            if not discovery.metrics:
                history.finish(
                    inv_id,
                    status="failed",
                    error="All LLM-selected metrics had invalid datasource UIDs",
                    timings=timings,
                    total_time=time.monotonic() - t_start,
                )
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary="LLM selected metrics with invalid datasource references. " "Try rephrasing your query.",
                )

            # Query Builder Agent
            t0 = time.monotonic()
            dashboard_spec, qb_usage = await build_dashboard(intent, discovery, ranked_catalog)
            timings["query_builder"] = time.monotonic() - t0
            cumulative_tokens = cumulative_tokens + qb_usage
            stage_log(
                "query_builder",
                (time.monotonic() - t0) * 1000,
                token_usage=qb_usage,
                metrics_input=len(discovery.metrics),
                panels_generated=len(dashboard_spec.panels),
            )

        evidence_requirements: list[EvidenceRequirement] = []
        evidence_resolutions: list[EvidenceResolution] = []
        if ranked_archetypes:
            try:
                evidence_archetypes = contributing_archetypes(ranked_archetypes, dashboard_spec)
                evidence_requirements, evidence_resolutions = resolve_requirements_for_archetypes(
                    evidence_archetypes,
                    intent,
                    catalog_for_compile,
                    target_language=target_language,
                )
            except Exception:
                logger.warning(
                    "evidence_resolution_failed",
                    error_type=EvidenceResolutionError.__name__,
                    exc_info=True,
                )

        binding_status, binding_reason, binding_details = compiled_query_diagnostics(
            dashboard_spec,
            catalog_for_compile,
        )
        record_stage(
            history,
            inv_id,
            "binding",
            binding_status,
            binding_reason,
            **binding_details,
        )
        compiled_query_count = sum(len(panel.queries) for panel in dashboard_spec.panels)
        if compiled_query_count:
            record_stage(
                history,
                inv_id,
                "compilation",
                "passed",
                "queries_compiled",
                panel_count=len(dashboard_spec.panels),
                query_count=compiled_query_count,
                path="archetype" if ranked_archetypes else "freeform",
            )
        else:
            record_stage(
                history,
                inv_id,
                "compilation",
                "failed",
                "no_queries_compiled",
                panel_count=len(dashboard_spec.panels),
                path="archetype" if ranked_archetypes else "freeform",
            )

        # ── 5. Validate queries — primary backend validates ──────────
        t0 = time.monotonic()

        def record_validation_stage(stage: str, status: str, reason_code: str, **details) -> None:
            record_stage(
                history,
                inv_id,
                stage,
                status,
                reason_code,
                **details,
            )

        validation_result = await validate_dashboard_and_evidence(
            primary=primary,
            dashboard_spec=dashboard_spec,
            catalog=catalog_for_compile,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            intent=intent,
            target_language=target_language,
            ranked_archetypes_present=bool(ranked_archetypes),
            record_stage=record_validation_stage,
        )
        dashboard_spec = validation_result.dashboard_spec
        validation_warnings = validation_result.validation_warnings
        panels_before = validation_result.panels_before
        timings["query_validation"] = time.monotonic() - t0
        stage_log(
            "query_validation",
            (time.monotonic() - t0) * 1000,
            backend=primary.name,
            panels_before=panels_before,
            panels_after=len(dashboard_spec.panels),
            warnings=len(validation_warnings),
        )

        # Record queries after validation
        try:
            queries_for_history, metrics_for_history = query_history_payload(dashboard_spec)
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
            history.finish(
                inv_id,
                status="failed",
                error="All panels empty after validation",
                timings=timings,
                total_time=time.monotonic() - t_start,
            )
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No panels returned data for your query. "
                "The service or metrics you asked about may not exist "
                "in the connected datasources.\n" + "\n".join(validation_warnings),
            )

        # ── 6. Publish — each backend publishes independently ────────
        publish_results: dict[str, PublishResult] = {}
        for backend in backends:
            t0 = time.monotonic()
            try:
                result = await backend.publish(dashboard_spec)
                publish_results[backend.name] = result
            except Exception:
                logger.warning("publish_failed", backend=backend.name, exc_info=True)
            timings[f"{backend.name}_publish"] = time.monotonic() - t0
            stage_log(
                "publish",
                (time.monotonic() - t0) * 1000,
                backend=backend.name,
                success=backend.name in publish_results,
            )

        # Effective identifiers — first successful backend wins
        grafana_result = publish_results.get("grafana", PublishResult())
        sfx_result = publish_results.get("signalfx", PublishResult())
        effective_uid = grafana_result.uid or sfx_result.uid or ""
        effective_url = grafana_result.url or sfx_result.url or ""

        path_used = "archetype" if ranked_archetypes else "freeform"
        summary = dashboard_summary(
            dashboard_spec,
            path_used,
            surviving_datasource_names(dashboard_spec, metric_catalog, datasource_catalog),
            publish_results,
        )

        total_s = time.monotonic() - t_start
        timings["total"] = total_s
        timings_rounded = {k: round(v, 2) for k, v in timings.items()}

        # Record validation results
        try:
            history.record_validation(
                inv_id,
                warnings=validation_warnings,
                panels_dropped=max(panels_before - len(dashboard_spec.panels), 0),
                final_panel_count=len(dashboard_spec.panels),
            )
        except Exception:
            logger.warning("history_record_validation_failed", exc_info=True)

        stage_log(
            "pipeline_complete",
            total_s * 1000,
            token_usage=cumulative_tokens,
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

            feedback_store = get_feedback_store()
            _, metrics_used = query_history_payload(dashboard_spec)
            feedback_store.record_provenance(
                dashboard_uid=effective_uid,
                prompt=request.prompt,
                problem_type=intent.problem_type,
                archetypes=[{"type": a.type, "confidence": a.confidence} for a in intent.archetypes],
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
