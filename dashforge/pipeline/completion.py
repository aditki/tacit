"""Pipeline completion, publishing, and provenance helpers."""

from __future__ import annotations

import time

import structlog

from dashforge.agents.providers.base import TokenUsage
from dashforge.backends.base import DashboardBackend, PublishResult
from dashforge.dependencies import PipelineDependencies
from dashforge.logging import stage_log
from dashforge.models.schemas import DashboardSpec, DashRequest, DashResponse, Intent, MetricEntry
from dashforge.pipeline.recording import (
    PipelineRecorder,
    dashboard_summary,
    query_history_payload,
    surviving_datasource_names,
)
from dashforge.pipeline.stages.publish import publish_dashboard

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
    timings: dict[str, float],
    recorder: PipelineRecorder,
    token_usage: TokenUsage,
    started_at: float,
) -> DashResponse:
    """Publish a validated dashboard and record completion/provenance."""
    publish_results = await publish_dashboard(backends=backends, dashboard_spec=dashboard_spec, timings=timings)

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

    try:
        feedback_store = deps.feedback_store_factory()
        _, metrics_used = query_history_payload(dashboard_spec)
        feedback_store.record_provenance(
            dashboard_uid=effective_uid,
            prompt=request.prompt,
            problem_type=intent.problem_type,
            archetypes=[{"type": item.type, "confidence": item.confidence} for item in intent.archetypes],
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
