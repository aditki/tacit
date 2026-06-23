"""Pipeline failure response helpers."""

from __future__ import annotations

import time

from dashforge.archetypes.templates import get_archetypes_by_confidence, get_archetypes_by_learning_context
from dashforge.backends.base import DashboardBackend
from dashforge.models.schemas import DashResponse, Intent, MetricEntry
from dashforge.pipeline.recording import PipelineRecorder


class PipelineFailureFactory:
    """Create standard failed responses and matching history records."""

    @staticmethod
    def no_backends() -> DashResponse:
        return DashResponse(
            dashboard_url="",
            dashboard_uid="",
            panel_count=0,
            summary="No dashboard backends are enabled. Enable at least one of: grafana, signalfx.",
        )

    @staticmethod
    def finish_failure(
        *,
        recorder: PipelineRecorder,
        error: str,
        summary: str,
        timings: dict[str, float],
        started_at: float,
    ) -> DashResponse:
        recorder.finish(
            status="failed",
            error=error,
            timings=timings,
            total_time=time.monotonic() - started_at,
        )
        return DashResponse(
            dashboard_url="",
            dashboard_uid="",
            panel_count=0,
            summary=summary,
        )

    @staticmethod
    def all_panels_empty(
        *,
        recorder: PipelineRecorder,
        timings: dict[str, float],
        started_at: float,
        validation_warnings: list[str],
    ) -> DashResponse:
        return PipelineFailureFactory.finish_failure(
            recorder=recorder,
            error="All panels empty after validation",
            summary=(
                "No panels returned data for your query. "
                "The service or metrics you asked about may not exist "
                "in the connected datasources.\n" + "\n".join(validation_warnings)
            ),
            timings=timings,
            started_at=started_at,
        )


def handle_empty_catalog(
    *,
    intent: Intent,
    metric_catalog: list[MetricEntry],
    backends: list[DashboardBackend],
    recorder: PipelineRecorder,
    timings: dict[str, float],
    started_at: float,
) -> DashResponse:
    """Record selected context and return a consistent no-catalog failure."""
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
    recorder.selected_intent(intent, ranked_archetypes, learned_archetypes)

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
        summary = "No metrics found across any datasource. Verify your datasources are configured and have data."
    return PipelineFailureFactory.finish_failure(
        recorder=recorder,
        error=error,
        summary=summary,
        timings=timings,
        started_at=started_at,
    )
