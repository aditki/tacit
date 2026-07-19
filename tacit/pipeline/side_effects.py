"""Safe non-critical pipeline side effects."""

from __future__ import annotations

import inspect
from collections.abc import Iterable

import structlog

from tacit.backends.base import DashboardBackend
from tacit.models.schemas import DashboardSpec, DashRequest, Intent
from tacit.pipeline.recording import query_history_payload

logger = structlog.get_logger()


def safe_finish_timeout_history(
    *,
    history_store_factory,
    request: DashRequest,
    timeout_seconds: int,
) -> None:
    """Best-effort timeout history persistence."""
    try:
        store = history_store_factory()
        start_parameters = inspect.signature(store.start).parameters
        if "tenant_id" in start_parameters:
            inv_id = store.start(
                request.prompt,
                request.user_id,
                request.channel_id,
                tenant_id=request.tenant_id or "default",
            )
        else:
            inv_id = store.start(request.prompt, request.user_id, request.channel_id)
        store.finish(
            inv_id,
            status="timeout",
            error=f"Timed out after {timeout_seconds}s",
        )
    except Exception:
        logger.warning("timeout_history_record_failed", exc_info=True)


def safe_record_provenance(
    *,
    feedback_store_factory,
    dashboard_uid: str,
    dashboard_url: str,
    request: DashRequest,
    intent: Intent,
    dashboard_spec: DashboardSpec,
    path_used: str,
) -> None:
    """Best-effort feedback provenance persistence."""
    try:
        feedback_store = feedback_store_factory()
        _, metrics_used = query_history_payload(dashboard_spec)
        feedback_store.record_provenance(
            dashboard_uid=dashboard_uid,
            prompt=request.prompt,
            problem_type=intent.problem_type,
            archetypes=[{"type": item.type, "confidence": item.confidence} for item in intent.archetypes],
            metrics_used=metrics_used,
            panel_count=len(dashboard_spec.panels),
            path_used=path_used,
            dashboard_url=dashboard_url,
            user_id=request.user_id,
            channel_id=request.channel_id,
        )
    except Exception:
        logger.warning("provenance_record_failed", exc_info=True)


async def safe_close_backends(backends: Iterable[DashboardBackend]) -> None:
    """Best-effort backend cleanup."""
    for backend in backends:
        try:
            await backend.close()
        except Exception:
            logger.warning("backend_close_failed", backend=backend.name, exc_info=True)
