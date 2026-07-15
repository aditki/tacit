"""Pipeline completion, publishing, and provenance helpers."""

from __future__ import annotations

import time

import structlog

from tacit.agents.providers.base import TokenUsage
from tacit.backends.base import DashboardBackend, PublishResult
from tacit.dependencies import PipelineDependencies
from tacit.investigation_contract import InvestigationContractAssembler
from tacit.logging import stage_log
from tacit.models.schemas import (
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

    recorder.finish(
        status="success",
        dashboard_uid=effective_uid,
        dashboard_url=effective_url,
        timings=timings_rounded,
        total_time=total_s,
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
        persisted_contract = recorder.history.persist_contract_revision(
            InvestigationContractAssembler().from_pipeline(
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
                dashboard_url=effective_url,
                dashboard_uid=effective_uid,
                signalfx_url=sfx_result.url,
                signalfx_dashboard_id=sfx_result.uid,
            ),
            reason="initial",
        )
    except Exception:
        logger.warning(
            "investigation_contract_persist_failed",
            investigation_id=recorder.investigation_id,
            dashboard_uid=effective_uid,
            exc_info=True,
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
