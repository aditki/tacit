"""Orchestration pipeline: Prompt → Intent → Discover → Build → Publish."""

from __future__ import annotations

import asyncio
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from tacit.agents.intent import classify_intent
from tacit.backends import get_active_backends
from tacit.config import settings
from tacit.context.enrichment import enrich_context
from tacit.culprit_ranking import rank_culprits
from tacit.dependencies import PipelineDependencies, _get_feedback_store, build_pipeline_dependencies
from tacit.history import get_investigation_store
from tacit.investigation_contract import InvestigationRunType
from tacit.logging import bind_request_id, stage_log, unbind_request_id
from tacit.models.schemas import (
    DashRequest,
    DashResponse,
)
from tacit.pipeline.completion import complete_pipeline
from tacit.pipeline.context import PipelineRunContext
from tacit.pipeline.discovery import (
    discovery_keywords,
    semantic_mapping_diagnostics,
)

if TYPE_CHECKING:
    from tacit.knowledge.models import KnowledgeUsage
from tacit.pipeline.failures import PipelineFailureFactory, handle_empty_catalog
from tacit.pipeline.recording import (
    PipelineRecorder,
    compiled_query_diagnostics,
    history_archetypes,
    history_signals,
)
from tacit.pipeline.side_effects import safe_close_backends
from tacit.pipeline.stages.archetypes import compile_selected_archetypes, select_archetypes
from tacit.pipeline.stages.discovery import run_discovery_stage
from tacit.pipeline.stages.evidence import run_evidence_stage
from tacit.pipeline.stages.freeform import build_freeform_dashboard
from tacit.pipeline.stages.intent import run_intent_stage
from tacit.pipeline.validation import validate_dashboard_and_evidence

logger = structlog.get_logger()

# Backward-compatible test/import aliases for helpers that now live in smaller modules.
_compiled_query_diagnostics = compiled_query_diagnostics
_discovery_keywords = discovery_keywords
_history_archetypes = history_archetypes
_history_signals = history_signals
_semantic_mapping_diagnostics = semantic_mapping_diagnostics


# Concurrency gate — prevents thundering-herd on LLM + Grafana APIs
_pipeline_semaphore: asyncio.Semaphore | None = None
_pipeline_semaphore_limit: int | None = None


@dataclass
class _PipelineCancellation:
    status: str = "cancelled"
    error: str = "Pipeline cancelled"


def _default_dependencies() -> PipelineDependencies:
    """Build default dependencies through pipeline-level patch points.

    Several harnesses patch these names on ``tacit.pipeline`` to create a
    cold isolated runtime. Keeping the default lookup here preserves that
    behavior while the core runner still accepts explicit dependencies.
    """
    return build_pipeline_dependencies(
        settings,
        backend_factory=get_active_backends,
        history_store_factory=get_investigation_store,
        feedback_store_factory=_get_feedback_store,
    )


def _get_semaphore(max_concurrent: int) -> asyncio.Semaphore:
    global _pipeline_semaphore, _pipeline_semaphore_limit
    if _pipeline_semaphore is None or _pipeline_semaphore_limit != max_concurrent:
        _pipeline_semaphore = asyncio.Semaphore(max_concurrent)
        _pipeline_semaphore_limit = max_concurrent
    return _pipeline_semaphore


async def run_pipeline(
    request: DashRequest,
    deps: PipelineDependencies | None = None,
    *,
    investigation_id: str | None = None,
    run_type: InvestigationRunType = InvestigationRunType.INITIAL,
    base_revision: int | None = None,
) -> DashResponse:
    """End-to-end: natural language → Grafana dashboard URL."""
    deps = deps or _default_dependencies()
    runtime_settings = deps.settings
    configured_tenant = str(getattr(runtime_settings, "knowledge_tenant_id", "default") or "default")
    tenant_id = (request.tenant_id or "default") if configured_tenant == "*" else configured_tenant
    request = request.model_copy(update={"tenant_id": tenant_id})
    bind_request_id()
    sem = _get_semaphore(runtime_settings.pipeline_max_concurrent)
    try:
        async with sem:
            cancellation = _PipelineCancellation()
            pipeline_task = asyncio.create_task(
                _run_pipeline_inner(
                    request,
                    deps,
                    investigation_id=investigation_id,
                    run_type=run_type,
                    base_revision=base_revision,
                    cancellation=cancellation,
                )
            )
            try:
                done, _ = await asyncio.wait(
                    {pipeline_task},
                    timeout=runtime_settings.pipeline_timeout_seconds,
                )
                if pipeline_task in done:
                    return pipeline_task.result()

                cancellation.status = "timeout"
                cancellation.error = "Pipeline timed out"
                pipeline_task.cancel()
                with suppress(asyncio.CancelledError):
                    await pipeline_task
                logger.error(
                    "pipeline_timeout",
                    user=request.user_id,
                    timeout=runtime_settings.pipeline_timeout_seconds,
                )
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary=f"Pipeline timed out after {runtime_settings.pipeline_timeout_seconds}s. "
                    "Try a more specific query or check datasource connectivity.",
                )
            except asyncio.CancelledError:
                cancellation.status = "cancelled"
                cancellation.error = "Pipeline cancelled by caller"
                if not pipeline_task.done():
                    pipeline_task.cancel()
                with suppress(asyncio.CancelledError):
                    await pipeline_task
                raise
    finally:
        unbind_request_id()


async def _run_pipeline_inner(
    request: DashRequest,
    deps: PipelineDependencies,
    *,
    investigation_id: str | None = None,
    run_type: InvestigationRunType = InvestigationRunType.INITIAL,
    base_revision: int | None = None,
    cancellation: _PipelineCancellation | None = None,
) -> DashResponse:
    """Inner pipeline logic (wrapped with timeout + semaphore above).

    Uses the backend adapter pattern: each enabled vendor (Grafana, SignalFx,
    etc.) is a DashboardBackend instance.  The pipeline iterates over backends
    for discovery, validation, and publishing — zero vendor-specific if/else.
    """
    runtime_settings = deps.settings
    t_start = time.monotonic()
    timings: dict[str, float] = {}
    history = deps.history_store_factory()
    inv_id = investigation_id or history.start(request.prompt, request.user_id or "", request.channel_id or "")
    if base_revision is None and investigation_id and hasattr(history, "get_contract"):
        current_contract = history.get_contract(investigation_id)
        base_revision = current_contract.investigation.revision if current_contract else None
    run_id = None
    if hasattr(history, "start_run"):
        try:
            run_id = history.start_run(inv_id, run_type=run_type, base_revision=base_revision)
        except Exception:
            logger.warning("investigation_run_start_failed", investigation_id=inv_id, exc_info=True)
    recorder = PipelineRecorder(
        history,
        inv_id,
        run_id=run_id,
        record_investigation_updates=investigation_id is None,
    )
    try:
        backends = deps.backend_factory()
    except Exception as exc:
        recorder.finish(
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
            timings=timings,
            total_time=time.monotonic() - t_start,
        )
        await deps.close_resources()
        raise
    if not backends:
        recorder.finish(
            status="failed",
            error="No dashboard backends are enabled",
            timings={},
            total_time=time.monotonic() - t_start,
        )
        await deps.close_resources()
        return PipelineFailureFactory.no_backends().model_copy(update={"investigation_id": inv_id})

    primary = backends[0]  # determines query language for compilation
    runtime = PipelineRunContext(
        request=request,
        deps=deps,
        settings=runtime_settings,
        backends=backends,
        primary=primary,
        history=history,
        investigation_id=inv_id,
        recorder=recorder,
        started_at=t_start,
        timings=timings,
    )

    try:
        # ── 1. Intent Agent ──────────────────────────────────────────
        llm_provider_factory = runtime.deps.llm_provider_factory
        context_provider_factory = runtime.deps.context_provider_factory
        intent_stage = await run_intent_stage(
            prompt=request.prompt,
            user_id=request.user_id,
            deps=runtime.deps,
            classify=classify_intent,
            enrich=enrich_context,
            classify_provider_factory=llm_provider_factory,
            context_provider_factory=context_provider_factory,
            timings=runtime.timings,
        )
        intent = intent_stage.intent
        context_chunks = intent_stage.context_chunks
        runtime.add_tokens(intent_stage.token_usage)
        runtime.recorder.intent(intent)

        # ── 3. Metric discovery — each backend contributes ───────────
        discovery_stage = await run_discovery_stage(
            backends=backends,
            primary=primary,
            intent=intent,
            timings=runtime.timings,
            recorder=runtime.recorder,
        )
        catalog_discovery = discovery_stage.discovery
        metric_catalog = catalog_discovery.metric_catalog
        datasource_catalog = catalog_discovery.datasource_catalog
        catalog_for_compile = catalog_discovery.catalog_for_compile
        confirmed_keywords = discovery_stage.confirmed_keywords
        if confirmed_keywords:
            logger.info("colloquial_evidence_confirmed", keywords=confirmed_keywords)

        if not catalog_for_compile:
            return handle_empty_catalog(
                intent=intent,
                metric_catalog=metric_catalog,
                backends=backends,
                recorder=runtime.recorder,
                timings=runtime.timings,
                started_at=runtime.started_at,
            )

        # ── 4. Multi-label archetype matching ────────────────────
        target_language = primary.query_language
        selection = select_archetypes(
            intent=intent,
            metric_catalog=metric_catalog,
            catalog_for_compile=catalog_for_compile,
            target_language=target_language,
            settings=runtime.settings,
        )
        ranked_archetypes = selection.ranked_archetypes
        learned_archetypes = selection.learned_archetypes

        runtime.recorder.selected_intent(intent, ranked_archetypes, learned_archetypes)

        compilation = compile_selected_archetypes(
            selection=selection,
            intent=intent,
            catalog_for_compile=catalog_for_compile,
            timings=runtime.timings,
        )
        if compilation is not None:
            dashboard_spec = compilation.dashboard_spec
        else:
            freeform = await build_freeform_dashboard(
                intent=intent,
                metric_catalog=metric_catalog,
                context_chunks=context_chunks,
                deps=runtime.deps,
                recorder=runtime.recorder,
                timings=runtime.timings,
                started_at=runtime.started_at,
            )
            runtime.add_tokens(freeform.token_usage)
            if freeform.failure is not None:
                return freeform.failure
            assert freeform.dashboard_spec is not None
            dashboard_spec = freeform.dashboard_spec

        evidence_stage = run_evidence_stage(
            ranked_archetypes=ranked_archetypes,
            dashboard_spec=dashboard_spec,
            intent=intent,
            catalog=catalog_for_compile,
            target_language=target_language,
        )
        evidence_requirements = evidence_stage.requirements
        evidence_resolutions = evidence_stage.resolutions

        binding_status, binding_reason, binding_details = compiled_query_diagnostics(
            dashboard_spec,
            catalog_for_compile,
        )
        runtime.recorder.stage("binding", binding_status, binding_reason, **binding_details)
        compiled_query_count = sum(len(panel.queries) for panel in dashboard_spec.panels)
        if compiled_query_count:
            runtime.recorder.stage(
                "compilation",
                "passed",
                "queries_compiled",
                panel_count=len(dashboard_spec.panels),
                query_count=compiled_query_count,
                path="archetype" if ranked_archetypes else "freeform",
            )
        else:
            runtime.recorder.stage(
                "compilation",
                "failed",
                "no_queries_compiled",
                panel_count=len(dashboard_spec.panels),
                path="archetype" if ranked_archetypes else "freeform",
            )

        # ── 5. Validate queries — primary backend validates ──────────
        t0 = time.monotonic()

        def record_validation_stage(stage: str, status: str, reason_code: str, **details) -> None:
            runtime.recorder.stage(stage, status, reason_code, **details)

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
        culprit_ranking = rank_culprits(
            intent=intent,
            dashboard_spec=dashboard_spec,
            ranked_archetypes=ranked_archetypes,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            evidence_observations=validation_result.evidence_observations,
        )
        baseline_culprit_ranking = culprit_ranking
        knowledge_snapshot = None
        knowledge_usage: list[KnowledgeUsage] = []
        try:
            from tacit.knowledge.models import KnowledgeScope
            from tacit.knowledge.service import get_knowledge_service

            tenant_id = request.tenant_id or getattr(runtime_settings, "knowledge_tenant_id", "default")
            knowledge_scope = KnowledgeScope(
                tenant_id=tenant_id,
                service_refs=[f"entity:service:{service}" for service in intent.services],
            )
            knowledge_service = get_knowledge_service()
            knowledge_snapshot, knowledge_usage = knowledge_service.create_snapshot(knowledge_scope)
            knowledge_usage = knowledge_service.reconcile_live_observations(
                knowledge_usage,
                validation_result.evidence_observations,
            )
            knowledge_snapshot = knowledge_service.snapshot_from_usage(tenant_id, knowledge_usage)
            culprit_ranking = knowledge_service.apply_to_ranking(culprit_ranking, knowledge_usage)
        except Exception:
            logger.warning("operational_knowledge_selection_failed", exc_info=True)
        ranking_status = "passed" if culprit_ranking.candidates else "skipped"
        ranking_reason = (
            culprit_ranking.abstention_reason
            if culprit_ranking.abstained
            else f"{culprit_ranking.mode.value}_suspects_ranked"
        )
        runtime.recorder.stage(
            "ranking",
            ranking_status,
            ranking_reason,
            **culprit_ranking.model_dump(mode="json"),
        )
        runtime.timings["query_validation"] = time.monotonic() - t0
        stage_log(
            "query_validation",
            (time.monotonic() - t0) * 1000,
            backend=primary.name,
            panels_before=panels_before,
            panels_after=len(dashboard_spec.panels),
            warnings=len(validation_warnings),
        )

        # Record queries after validation
        runtime.recorder.queries(dashboard_spec, path_used="archetype" if ranked_archetypes else "freeform")

        if not dashboard_spec.panels:
            return PipelineFailureFactory.all_panels_empty(
                recorder=runtime.recorder,
                timings=runtime.timings,
                started_at=runtime.started_at,
                validation_warnings=validation_warnings,
                culprit_ranking=culprit_ranking,
            )

        return await complete_pipeline(
            request=runtime.request,
            deps=runtime.deps,
            backends=runtime.backends,
            dashboard_spec=dashboard_spec,
            intent=intent,
            metric_catalog=metric_catalog,
            datasource_catalog=datasource_catalog,
            ranked_archetypes_present=bool(ranked_archetypes),
            validation_warnings=validation_warnings,
            panels_before=panels_before,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            evidence_observations=validation_result.evidence_observations,
            culprit_ranking=culprit_ranking,
            baseline_culprit_ranking=baseline_culprit_ranking,
            context_chunks=context_chunks,
            knowledge_snapshot=knowledge_snapshot,
            knowledge_usage=knowledge_usage,
            run_type=run_type,
            revision_reason="refresh" if run_type == InvestigationRunType.REFRESH else "initial",
            base_revision=base_revision,
            timings=runtime.timings,
            recorder=runtime.recorder,
            token_usage=runtime.token_usage,
            started_at=runtime.started_at,
        )

    except asyncio.CancelledError:
        cancellation = cancellation or _PipelineCancellation()
        runtime.recorder.finish(
            status=cancellation.status,
            error=cancellation.error,
            timings=runtime.timings,
            total_time=time.monotonic() - runtime.started_at,
        )
        raise
    except Exception as exc:
        runtime.recorder.finish(
            status="failed",
            error=f"{type(exc).__name__}: {exc}",
            timings=runtime.timings,
            total_time=time.monotonic() - runtime.started_at,
        )
        raise
    finally:
        await safe_close_backends(backends)
        await deps.close_resources()
