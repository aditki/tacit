"""Dashboard publishing stage."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import structlog

from tacit.backends.base import DashboardBackend, PublishResult
from tacit.config import Settings
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS, safe_failure_diagnostics
from tacit.logging import stage_log
from tacit.models.schemas import DashboardSpec
from tacit.runtime_ownership import (
    get_runtime_ownership,
    require_compatible_runtime_ownership,
    runtime_descriptor_from_settings,
)

logger = structlog.get_logger()


@dataclass(slots=True)
class PublicationState:
    """Mutable commit state shared with the synchronous completion phase."""

    commit_started: bool = False
    cancellation_requested: bool = False


def preflight_publish_backends(
    backends: list[DashboardBackend],
    runtime_settings: Settings | None,
) -> None:
    """Validate every realized backend owner before the first remote write."""
    if runtime_settings is None:
        return
    settings_owner = runtime_descriptor_from_settings(
        runtime_settings,
        component="pipeline_publish_settings",
    )
    backend_owners = tuple(
        get_runtime_ownership(backend, component=f"{backend.name}_publish_backend") for backend in backends
    )
    require_compatible_runtime_ownership(
        boundary="pipeline_publication",
        descriptors=(settings_owner, *backend_owners),
    )


async def _publish_all(
    *,
    backends: list[DashboardBackend],
    dashboard_spec: DashboardSpec,
    timings: dict[str, float],
) -> dict[str, PublishResult]:
    publish_results: dict[str, PublishResult] = {}
    for backend in backends:
        t0 = time.monotonic()
        try:
            result = await backend.publish(dashboard_spec)
            publish_results[backend.name] = result
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "publish_failed",
                backend=backend.name,
                **safe_failure_diagnostics(exc, reason_code="dashboard_publish_failed"),
            )
        timings[f"{backend.name}_publish"] = time.monotonic() - t0
        stage_log(
            "publish",
            (time.monotonic() - t0) * 1000,
            backend=backend.name,
            success=backend.name in publish_results,
        )
    return publish_results


async def publish_dashboard(
    *,
    backends: list[DashboardBackend],
    dashboard_spec: DashboardSpec,
    timings: dict[str, float],
    runtime_settings: Settings | None = None,
    preserve_commit_on_cancellation: bool = False,
    state: PublicationState | None = None,
) -> dict[str, PublishResult]:
    """Publish the dashboard to every active backend."""
    preflight_publish_backends(backends, runtime_settings)
    publication_state = state or PublicationState()
    publication_state.commit_started = True
    publish_coro = _publish_all(
        backends=backends,
        dashboard_spec=dashboard_spec,
        timings=timings,
    )
    if not preserve_commit_on_cancellation:
        return await publish_coro

    publish_task = asyncio.create_task(publish_coro)
    while True:
        try:
            return await asyncio.shield(publish_task)
        except asyncio.CancelledError:
            publication_state.cancellation_requested = True
            current_task = asyncio.current_task()
            if current_task is not None:
                current_task.uncancel()
