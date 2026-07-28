"""Feedback and insight routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi import Path as PathParam

from tacit.api.dependencies import get_feedback_store
from tacit.api.security import knowledge_tenant, verify_api_key
from tacit.models.schemas import FeedbackRequest, FeedbackResponse, FeedbackStatsResponse

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.post(
    "/api/v1/feedback",
    response_model=FeedbackResponse,
    tags=["Feedback"],
    summary="Submit dashboard feedback",
    response_description="Confirmation with feedback ID",
)
async def submit_feedback(
    req: FeedbackRequest,
    request: Request,
    store: Any = Depends(get_feedback_store),
):
    """Submit human evaluation feedback for a generated dashboard."""
    tenant_id = knowledge_tenant(request)
    feedback_id = store.submit_feedback(
        dashboard_uid=req.dashboard_uid,
        symptom_visibility=req.symptom_visibility,
        root_cause_support=req.root_cause_support,
        noise_level=req.noise_level,
        investigation_speed=req.investigation_speed,
        overall_useful=req.overall_useful,
        comment=req.comment,
        reviewer=req.reviewer,
        tenant_id=tenant_id,
    )
    from tacit.ranking import invalidate_metric_quality_cache

    invalidate_metric_quality_cache(store, tenant_id=tenant_id)
    return FeedbackResponse(feedback_id=feedback_id, dashboard_uid=req.dashboard_uid)


@router.get(
    "/api/v1/feedback/stats",
    tags=["Insights"],
    summary="Feedback statistics",
    response_model=FeedbackStatsResponse,
    response_description=(
        "Aggregate stats: total feedback, dashboards reviewed, useful rate, average dimensional scores"
    ),
)
async def get_feedback_stats(request: Request, store: Any = Depends(get_feedback_store)):
    """Aggregate feedback statistics across all reviewed dashboards."""
    return store.get_aggregate_stats(tenant_id=knowledge_tenant(request))


@router.get(
    "/api/v1/feedback/analysis",
    tags=["Insights"],
    summary="Feedback analysis & recommendations",
    response_description="Actionable improvement signals with prioritized recommendations",
)
async def get_feedback_analysis(request: Request, store: Any = Depends(get_feedback_store)):
    """Analyze collected feedback to produce actionable improvement signals."""
    return store.analyze(tenant_id=knowledge_tenant(request))


@router.get(
    "/api/v1/feedback/{dashboard_uid}",
    tags=["Feedback"],
    summary="Get feedback for a dashboard",
    response_description="Dashboard provenance metadata and all submitted feedback entries",
)
async def get_feedback(
    request: Request,
    dashboard_uid: str = PathParam(..., pattern=r"^[a-zA-Z0-9_\-]{1,128}$", description="Dashboard UID"),
    store: Any = Depends(get_feedback_store),
):
    """Retrieve provenance and feedback for a dashboard UID."""
    tenant_id = knowledge_tenant(request)
    provenance = store.get_provenance(dashboard_uid, tenant_id=tenant_id)
    feedback = store.get_feedback(dashboard_uid, tenant_id=tenant_id)
    if not provenance and not feedback:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"provenance": provenance, "feedback": feedback}
