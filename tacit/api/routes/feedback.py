"""Feedback and insight routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from fastapi import Path as PathParam

import tacit.feedback as feedback_mod
from tacit.api.security import verify_api_key
from tacit.models.schemas import FeedbackRequest, FeedbackResponse, FeedbackStatsResponse

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.post(
    "/api/v1/feedback",
    response_model=FeedbackResponse,
    tags=["Feedback"],
    summary="Submit dashboard feedback",
    response_description="Confirmation with feedback ID",
)
async def submit_feedback(req: FeedbackRequest):
    """Submit human evaluation feedback for a generated dashboard."""
    store = feedback_mod.get_feedback_store()
    feedback_id = store.submit_feedback(
        dashboard_uid=req.dashboard_uid,
        symptom_visibility=req.symptom_visibility,
        root_cause_support=req.root_cause_support,
        noise_level=req.noise_level,
        investigation_speed=req.investigation_speed,
        overall_useful=req.overall_useful,
        comment=req.comment,
        reviewer=req.reviewer,
    )
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
async def get_feedback_stats():
    """Aggregate feedback statistics across all reviewed dashboards."""
    store = feedback_mod.get_feedback_store()
    return store.get_aggregate_stats()


@router.get(
    "/api/v1/feedback/analysis",
    tags=["Insights"],
    summary="Feedback analysis & recommendations",
    response_description="Actionable improvement signals with prioritized recommendations",
)
async def get_feedback_analysis():
    """Analyze collected feedback to produce actionable improvement signals."""
    store = feedback_mod.get_feedback_store()
    return store.analyze()


@router.get(
    "/api/v1/feedback/{dashboard_uid}",
    tags=["Feedback"],
    summary="Get feedback for a dashboard",
    response_description="Dashboard provenance metadata and all submitted feedback entries",
)
async def get_feedback(
    dashboard_uid: str = PathParam(..., pattern=r"^[a-zA-Z0-9_\-]{1,128}$", description="Dashboard UID"),
):
    """Retrieve provenance and feedback for a dashboard UID."""
    store = feedback_mod.get_feedback_store()
    provenance = store.get_provenance(dashboard_uid)
    feedback = store.get_feedback(dashboard_uid)
    if not provenance and not feedback:
        raise HTTPException(status_code=404, detail="Dashboard not found")
    return {"provenance": provenance, "feedback": feedback}
