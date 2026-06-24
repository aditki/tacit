"""Dashboard generation routes."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException

from tacit.api.dependencies import get_pipeline_dependencies
from tacit.api.security import sanitize_prompt, verify_api_key
from tacit.dependencies import PipelineDependencies
from tacit.models.schemas import DashRequest, DashResponse
from tacit.pipeline import run_pipeline

logger = structlog.get_logger()
router = APIRouter()


@router.post(
    "/api/v1/chart",
    response_model=DashResponse,
    dependencies=[Depends(verify_api_key)],
    tags=["Dashboard Generation"],
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
