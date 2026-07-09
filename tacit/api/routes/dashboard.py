"""Dashboard generation routes."""

from __future__ import annotations

import asyncio
import json

import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from tacit.api.dependencies import get_pipeline_dependencies
from tacit.api.security import sanitize_prompt, verify_api_key
from tacit.dependencies import PipelineDependencies
from tacit.models.schemas import DashRequest, DashResponse
from tacit.pipeline import run_pipeline
from tacit.pipeline.progress import reset_progress_callback, set_progress_callback

logger = structlog.get_logger()
router = APIRouter()


@router.post(
    "/api/v1/chart",
    response_model=DashResponse,
    dependencies=[Depends(verify_api_key)],
    tags=["Investigation Generation"],
    summary="Generate a dashboard",
    response_description="Published dashboard URL, UID, panel count, and summary",
)
async def create_chart(
    request: DashRequest,
    deps: PipelineDependencies = Depends(get_pipeline_dependencies),
):
    """Generate a Grafana dashboard from a natural-language prompt."""
    request = request.model_copy(update={"prompt": sanitize_prompt(request.prompt)})
    if not request.prompt:
        raise HTTPException(status_code=400, detail="prompt is required")
    try:
        return await run_pipeline(request, deps)
    except Exception:
        logger.exception("api_pipeline_error")
        raise HTTPException(status_code=500, detail="Failed to generate dashboard")


def _sse_frame(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


@router.post(
    "/api/v1/chart/stream",
    dependencies=[Depends(verify_api_key)],
    tags=["Investigation Generation"],
    summary="Generate a dashboard with live stage streaming (SSE)",
    response_description="Server-Sent Events: `stage` events while the pipeline runs, "
    "then one `result` (or `error`) event.",
)
async def create_chart_stream(
    request: DashRequest,
    deps: PipelineDependencies = Depends(get_pipeline_dependencies),
):
    """Stream pipeline progress as Server-Sent Events.

    Emits `stage` events (intent, discovery, binding, compilation, validation,
    ranking, publish, ...) as the investigation is built, followed by a final
    `result` event containing the standard DashResponse JSON.
    """
    request = request.model_copy(update={"prompt": sanitize_prompt(request.prompt)})
    if not request.prompt:
        raise HTTPException(status_code=400, detail="prompt is required")

    queue: asyncio.Queue[dict] = asyncio.Queue()

    async def run_with_progress() -> DashResponse:
        token = set_progress_callback(queue.put_nowait)
        try:
            return await run_pipeline(request, deps)
        finally:
            reset_progress_callback(token)

    task = asyncio.create_task(run_with_progress())

    async def event_stream():
        try:
            while True:
                if task.done() and queue.empty():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=0.5)
                except TimeoutError:
                    continue
                yield _sse_frame("stage", event)
            try:
                result = task.result()
            except Exception:
                logger.exception("api_pipeline_stream_error")
                yield _sse_frame("error", {"detail": "Failed to generate dashboard"})
                return
            yield _sse_frame("result", result.model_dump(mode="json"))
        finally:
            if not task.done():
                task.cancel()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
