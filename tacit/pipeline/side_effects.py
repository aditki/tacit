"""Safe non-critical pipeline side effects."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import structlog

from tacit.backends.base import DashboardBackend
from tacit.models.schemas import DashboardSpec, DashRequest, Intent
from tacit.pipeline.recording import query_history_payload
from tacit.runtime_ownership import (
    DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS,
    validate_runtime_cleanup_grace_seconds,
)

if TYPE_CHECKING:
    from tacit.pipeline_admission import PipelineAdmissionController, PipelineAdmissionLease

logger = structlog.get_logger()

DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS = DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS
validate_cleanup_grace_seconds = validate_runtime_cleanup_grace_seconds


def _consume_background_task(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except BaseException:
        return


async def cancel_task_with_grace(
    task: asyncio.Task[Any],
    *,
    grace_seconds: float,
    reason_code: str,
    lifecycle: PipelineAdmissionController | None = None,
    lease: PipelineAdmissionLease | None = None,
) -> bool:
    """Cancel one task and wait only for the configured cleanup grace."""
    grace = validate_cleanup_grace_seconds(grace_seconds)
    if not task.done():
        task.cancel()
    done, _pending = await asyncio.wait({task}, timeout=grace)
    if task in done:
        _consume_background_task(task)
        return True
    retained = lifecycle is not None and lease is not None and lifecycle.retain_task(lease, task)
    if not retained:
        task.add_done_callback(_consume_background_task)
    logger.warning(
        "pipeline_task_cleanup_grace_exceeded",
        reason_code=reason_code,
        cleanup_grace_seconds=grace,
    )
    return False


def safe_finish_timeout_history(
    *,
    history_store_factory,
    request: DashRequest,
    timeout_seconds: int,
) -> None:
    """Best-effort timeout history persistence."""
    try:
        store = history_store_factory()
        tenant_id = request.tenant_id or "default"
        start_parameters = inspect.signature(store.start).parameters
        if "tenant_id" in start_parameters:
            inv_id = store.start(
                request.prompt,
                request.user_id,
                request.channel_id,
                tenant_id=tenant_id,
            )
        else:
            inv_id = store.start(request.prompt, request.user_id, request.channel_id)
        store.finish(
            inv_id,
            status="timeout",
            error=f"Timed out after {timeout_seconds}s",
            tenant_id=tenant_id,
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
            tenant_id=request.tenant_id or "default",
        )
    except Exception:
        logger.warning("provenance_record_failed", exc_info=True)


async def safe_close_backends(
    backends: Iterable[DashboardBackend],
    *,
    grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
    lifecycle: PipelineAdmissionController | None = None,
) -> None:
    """Close all backends concurrently within one bounded grace window."""
    grace = validate_cleanup_grace_seconds(grace_seconds)

    async def close_backend(backend: DashboardBackend) -> None:
        try:
            await backend.close()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("backend_close_failed", backend=backend.name, exc_info=True)

    tasks = {
        asyncio.create_task(close_backend(backend), name=f"tacit-close-backend-{backend.name}"): backend
        for backend in backends
    }
    if not tasks:
        return
    try:
        done, pending = await asyncio.wait(tasks, timeout=grace)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
            if lifecycle is None or not lifecycle.retain_current_task(task):
                task.add_done_callback(_consume_background_task)
        raise
    for task in done:
        _consume_background_task(task)
    for task in pending:
        backend = tasks[task]
        task.cancel()
        if lifecycle is None or not lifecycle.retain_current_task(task):
            task.add_done_callback(_consume_background_task)
        logger.warning(
            "backend_close_grace_exceeded",
            backend=backend.name,
            reason_code="backend_close_grace_exceeded",
            cleanup_grace_seconds=grace,
        )
