"""Pipeline completion, publishing, and provenance helpers."""

from __future__ import annotations

import time

import structlog

from tacit.agents.providers.base import TokenUsage
from tacit.backends.base import DashboardBackend, PublishResult
from tacit.dependencies import PipelineDependencies
from tacit.investigation_contract import InvestigationContractAssembler, InvestigationRunType, RuntimeManifest
from tacit.investigation_replay import InvestigationReplaySnapshot
from tacit.logging import stage_log
from tacit.models.schemas import (
    ContextChunk,
    CulpritRanking,
    DashboardSpec,
    DashRequest,
    DashResponse,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
)
from tacit.pipeline.progress import emit_progress
from tacit.pipeline.recording import (
    PipelineRecorder,
    dashboard_summary,
    surviving_datasource_names,
)
from tacit.pipeline.side_effects import safe_record_provenance
from tacit.pipeline.stages.publish import publish_dashboard

logger = structlog.get_logger()


async def complete_pipeline(
    *,
    request: DashRequest,
    deps: PipelineDependencies,
    backends: list[DashboardBackend],
    dashboard_spec: DashboardSpec,
    intent: Intent,
    metric_catalog: list[MetricEntry],
    datasource_catalog: list[MetricEntry],
    ranked_archetypes_present: bool,
    validation_warnings: list[str],
    panels_before: int,
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    evidence_observations: list[EvidenceObservation],
    culprit_ranking: CulpritRanking,
    context_chunks: list[ContextChunk] | None = None,
    run_type: InvestigationRunType = InvestigationRunType.INITIAL,
    revision_reason: str = "initial",
    base_revision: int | None = None,
    timings: dict[str, float],
    recorder: PipelineRecorder,
    token_usage: TokenUsage,
    started_at: float,
) -> DashResponse:
    """Publish a validated dashboard and record completion/provenance."""
    emit_progress("publish", "started", "publishing_dashboard", backends=[b.name for b in backends])
    publish_results = await publish_dashboard(backends=backends, dashboard_spec=dashboard_spec, timings=timings)
    emit_progress(
        "publish",
        "passed",
        "dashboard_published",
        backends={name: bool(result.url) for name, result in publish_results.items()},
        panel_count=len(dashboard_spec.panels),
    )

    grafana_result = publish_results.get("grafana", PublishResult())
    sfx_result = publish_results.get("signalfx", PublishResult())
    effective_uid = grafana_result.uid or sfx_result.uid or ""
    effective_url = grafana_result.url or sfx_result.url or ""

    path_used = "archetype" if ranked_archetypes_present else "freeform"
    summary = dashboard_summary(
        dashboard_spec,
        path_used,
        surviving_datasource_names(dashboard_spec, metric_catalog, datasource_catalog),
        publish_results,
    )

    total_s = time.monotonic() - started_at
    timings["total"] = total_s
    timings_rounded = {key: round(value, 2) for key, value in timings.items()}

    recorder.validation(
        validation_warnings,
        panels_before=panels_before,
        final_panel_count=len(dashboard_spec.panels),
    )

    stage_log(
        "pipeline_complete",
        total_s * 1000,
        token_usage=token_usage,
        user_id=request.user_id,
        channel_id=request.channel_id,
        dashboard_uid=effective_uid,
        panel_count=len(dashboard_spec.panels),
        path=path_used,
        timings=timings_rounded,
    )

    safe_record_provenance(
        feedback_store_factory=deps.feedback_store_factory,
        dashboard_uid=effective_uid,
        dashboard_url=effective_url,
        request=request,
        intent=intent,
        dashboard_spec=dashboard_spec,
        path_used=path_used,
    )

    persisted_contract = None
    try:
        draft_contract = InvestigationContractAssembler().from_pipeline(
            investigation_id=recorder.investigation_id,
            revision=0,
            parent_revision=None,
            request=request,
            intent=intent,
            dashboard_spec=dashboard_spec,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            evidence_observations=evidence_observations,
            culprit_ranking=culprit_ranking,
            context_chunks=context_chunks,
            warnings=validation_warnings,
            dashboard_url=effective_url,
            dashboard_uid=effective_uid,
            signalfx_url=sfx_result.url,
            signalfx_dashboard_id=sfx_result.uid,
            runtime_manifest=RuntimeManifest(
                model={
                    "provider": str(getattr(deps.settings, "llm_provider", "")),
                    "name": str(getattr(deps.settings, "llm_model", "")),
                    "prompt_version": "intent-v1",
                }
            ),
        )
        snapshot = InvestigationReplaySnapshot(
            investigation_id=recorder.investigation_id,
            created_at=draft_contract.investigation.created_at,
            completed_at=draft_contract.investigation.completed_at,
            request=request,
            intent=intent,
            dashboard_spec=dashboard_spec,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            resolution_candidates=[resolution.model_dump(mode="json") for resolution in evidence_resolutions],
            evidence_observations=evidence_observations,
            culprit_ranking=culprit_ranking,
            context_chunks=context_chunks or [],
            renderings=draft_contract.renderings,
            external_errors=[{"type": "validation_warning", "detail": warning} for warning in validation_warnings],
            model_inputs={
                "prompt": request.prompt,
                "context_chunks": [chunk.model_dump(mode="json") for chunk in context_chunks or []],
            },
            model_outputs={
                "intent": intent.model_dump(mode="json"),
                "dashboard_spec": dashboard_spec.model_dump(mode="json"),
            },
            query_results=[
                {
                    "panel_title": panel.title,
                    "expression": query.expr,
                    "validation_status": query.validation_status,
                    "has_data": query.validation_has_data,
                }
                for panel in dashboard_spec.panels
                for query in panel.queries
            ],
            runtime=draft_contract.runtime,
        )
        for requirement in draft_contract.evidence_requirements:
            recorder.event("requirement_created", requirement.model_dump(mode="json"))
        for resolution in draft_contract.evidence_resolutions:
            recorder.event("resolution_selected", resolution.model_dump(mode="json"))
        for query in draft_contract.queries:
            recorder.event("query_validated", query.model_dump(mode="json"))
            recorder.event("query_executed", {"query_ref": query.id, **query.execution})
        for observation in draft_contract.observations:
            recorder.event("observation_created", observation.model_dump(mode="json"))
        for candidate in draft_contract.candidate_rankings:
            recorder.event("candidate_ranked", candidate.model_dump(mode="json"))
        recorder.event("conclusion_restricted", draft_contract.grounding.model_dump(mode="json"))
        persisted_contract = recorder.history.persist_contract_revision(
            draft_contract,
            reason=revision_reason,
            run_type=run_type,
            snapshot=snapshot,
            run_id=recorder.run_id,
            expected_parent_revision=(base_revision if run_type == InvestigationRunType.REFRESH else None),
        )
    except Exception:
        logger.warning(
            "investigation_contract_persist_failed",
            investigation_id=recorder.investigation_id,
            dashboard_uid=effective_uid,
            exc_info=True,
        )

    refresh_persist_failed = run_type == InvestigationRunType.REFRESH and persisted_contract is None
    recorder.finish(
        status="failed" if refresh_persist_failed else "success",
        dashboard_uid=effective_uid,
        dashboard_url=effective_url,
        error="Refresh did not produce a new revision." if refresh_persist_failed else "",
        timings=timings_rounded,
        total_time=total_s,
        # Refresh revisions are authoritative. Keep the legacy row as one
        # internally consistent snapshot instead of mixing old pipeline fields
        # with the refreshed dashboard; revision persistence updates only its
        # current_revision pointer.
        persist_record=run_type != InvestigationRunType.REFRESH,
    )

    return DashResponse(
        dashboard_url=grafana_result.url,
        dashboard_uid=effective_uid,
        panel_count=len(dashboard_spec.panels),
        summary=summary,
        investigation_id=recorder.investigation_id,
        investigation_revision=(persisted_contract.investigation.revision if persisted_contract else None),
        signalfx_url=sfx_result.url,
        signalfx_dashboard_id=sfx_result.uid,
        culprit_ranking=culprit_ranking,
    )
