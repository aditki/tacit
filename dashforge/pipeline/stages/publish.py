"""Dashboard publishing stage."""

from __future__ import annotations

import time

import structlog

from dashforge.backends.base import DashboardBackend, PublishResult
from dashforge.logging import stage_log
from dashforge.models.schemas import DashboardSpec

logger = structlog.get_logger()


async def publish_dashboard(
    *,
    backends: list[DashboardBackend],
    dashboard_spec: DashboardSpec,
    timings: dict[str, float],
) -> dict[str, PublishResult]:
    """Publish the dashboard to every active backend."""
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
    return publish_results
