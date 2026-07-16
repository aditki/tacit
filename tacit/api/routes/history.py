"""Investigation history routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field

import tacit.history as history_mod
from tacit.api.dependencies import get_pipeline_dependencies
from tacit.api.security import verify_api_key
from tacit.dependencies import PipelineDependencies
from tacit.investigation_bundle import build_investigation_bundle
from tacit.investigation_contract import InvestigationRunType
from tacit.investigation_replay import CounterfactualChanges, ReplayMode
from tacit.models.schemas import DashRequest
from tacit.pipeline import run_pipeline

router = APIRouter(dependencies=[Depends(verify_api_key)])


class CorrectionCandidateRequest(BaseModel):
    correction_text: str = Field(min_length=1, description="User-provided correction or feedback")
    target_ref: str = Field(default="", description="Optional contract object reference the correction applies to")
    revision: int | None = Field(default=None, description="Contract revision being corrected; defaults to current")
    created_by: str = Field(default="", description="Reviewer or user identifier")


class ReplayRequest(BaseModel):
    mode: ReplayMode = ReplayMode.EXACT
    changes: CounterfactualChanges = Field(default_factory=CounterfactualChanges)


class CorrectionReviewRequest(BaseModel):
    approved: bool
    reviewed_by: str = Field(min_length=1)


@router.get(
    "/api/v1/investigations",
    tags=["History"],
    summary="List recent investigations",
    response_description="Investigation history with intent, metrics, queries, timings, and results",
)
async def list_investigations(
    limit: int = 50,
    offset: int = 0,
    status: str | None = None,
    user_id: str | None = None,
):
    """List recent investigation runs, newest first."""
    store = history_mod.get_investigation_store()
    investigations = store.list_recent(limit=limit, offset=offset, status=status, user_id=user_id)
    return {"count": len(investigations), "investigations": investigations}


@router.get(
    "/api/v1/investigations/stats",
    tags=["History"],
    summary="Investigation aggregate stats",
    response_description="Aggregate statistics across all investigations",
)
async def investigation_stats():
    """Aggregate stats: success/failure rates, avg timings, path distribution."""
    store = history_mod.get_investigation_store()
    return store.stats()


@router.get(
    "/api/v1/investigations/{investigation_id}/revisions",
    tags=["History"],
    summary="List investigation contract revisions",
    response_description="Immutable investigation contract revision metadata",
)
async def list_investigation_revisions(investigation_id: str):
    store = history_mod.get_investigation_store()
    revisions = store.list_revisions(investigation_id)
    if not revisions and store.get(investigation_id) is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return {"count": len(revisions), "revisions": revisions}


@router.get(
    "/api/v1/investigations/{investigation_id}/runs",
    tags=["History"],
    summary="List investigation runs and lifecycle status",
)
async def list_investigation_runs(investigation_id: str):
    store = history_mod.get_investigation_store()
    runs = store.list_runs(investigation_id)
    if not runs and store.get(investigation_id) is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return {"count": len(runs), "runs": runs}


@router.get(
    "/api/v1/investigations/{investigation_id}/events",
    tags=["History"],
    summary="List append-only investigation lifecycle events",
)
async def list_investigation_events(investigation_id: str, run_id: str | None = None):
    store = history_mod.get_investigation_store()
    events = store.list_events(investigation_id, run_id)
    if not events and store.get(investigation_id) is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return {"count": len(events), "events": events}


@router.get(
    "/api/v1/investigations/{investigation_id}/contract",
    tags=["History"],
    summary="Get an investigation contract revision",
    response_description="Canonical Investigation Contract v1 document",
)
async def get_investigation_contract(investigation_id: str, revision: int | None = None):
    store = history_mod.get_investigation_store()
    contract = store.get_contract(investigation_id, revision)
    if contract is None:
        raise HTTPException(status_code=404, detail="Investigation contract not found")
    return contract.model_dump(mode="json", by_alias=True)


@router.get(
    "/api/v1/investigations/{investigation_id}/compare",
    tags=["History"],
    summary="Compare two investigation revisions",
    response_description="Fingerprint and top-level section comparison",
)
async def compare_investigation_revisions(investigation_id: str, left: int, right: int):
    store = history_mod.get_investigation_store()
    comparison = store.compare_revisions(investigation_id, left, right)
    if comparison is None:
        raise HTTPException(status_code=404, detail="Investigation revision not found")
    return comparison


@router.post(
    "/api/v1/investigations/{investigation_id}/replay",
    tags=["History"],
    summary="Replay from captured inputs",
    response_description="Stored contract loaded through exact replay without external refetch",
)
async def replay_investigation(
    investigation_id: str,
    request: ReplayRequest | None = None,
    revision: int | None = None,
):
    store = history_mod.get_investigation_store()
    replay_request = request or ReplayRequest()
    try:
        contract = store.replay_contract(
            investigation_id,
            revision,
            mode=replay_request.mode,
            changes=replay_request.changes,
        )
    except (history_mod.StaleRevisionError, history_mod.ReplayError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    if contract is None:
        raise HTTPException(status_code=404, detail="Investigation contract not found")
    return {
        "mode": replay_request.mode.value,
        "refetched_external_systems": False,
        "contract": contract.model_dump(mode="json", by_alias=True),
    }


@router.post(
    "/api/v1/investigations/{investigation_id}/corrections",
    tags=["History"],
    summary="Create a reviewable knowledge candidate from a correction",
    response_description="Correction stored as a scoped, provenance-bearing knowledge candidate",
)
async def create_correction_candidate(investigation_id: str, request: CorrectionCandidateRequest):
    store = history_mod.get_investigation_store()
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=request.revision,
        correction_text=request.correction_text,
        target_ref=request.target_ref,
        created_by=request.created_by,
    )
    if candidate is None:
        raise HTTPException(status_code=404, detail="Investigation contract not found")
    return candidate.model_dump(mode="json")


@router.get(
    "/api/v1/investigations/{investigation_id}/corrections",
    tags=["History"],
    summary="List correction candidates",
)
async def list_correction_candidates(investigation_id: str):
    store = history_mod.get_investigation_store()
    candidates = store.list_knowledge_candidates(investigation_id)
    if not candidates and store.get(investigation_id) is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return {"count": len(candidates), "candidates": [item.model_dump(mode="json") for item in candidates]}


@router.post(
    "/api/v1/investigations/{investigation_id}/corrections/{candidate_id}/review",
    tags=["History"],
    summary="Review a correction candidate",
)
async def review_correction_candidate(
    investigation_id: str,
    candidate_id: str,
    request: CorrectionReviewRequest,
):
    store = history_mod.get_investigation_store()
    candidate = store.review_knowledge_candidate(
        investigation_id,
        candidate_id,
        approved=request.approved,
        reviewed_by=request.reviewed_by,
    )
    if candidate is None:
        raise HTTPException(status_code=404, detail="Correction candidate not found")
    return candidate.model_dump(mode="json")


@router.post(
    "/api/v1/investigations/{investigation_id}/corrections/{candidate_id}/apply",
    tags=["History"],
    summary="Apply an approved correction as a new revision",
)
async def apply_correction_candidate(investigation_id: str, candidate_id: str):
    store = history_mod.get_investigation_store()
    contract = store.apply_knowledge_candidate(investigation_id, candidate_id)
    if contract is None:
        raise HTTPException(status_code=409, detail="Correction must exist, be approved, and not be expired")
    return contract.model_dump(mode="json", by_alias=True)


@router.post(
    "/api/v1/investigations/{investigation_id}/refresh",
    tags=["History"],
    summary="Refresh an investigation from current external inputs",
)
async def refresh_investigation(
    investigation_id: str,
    deps: PipelineDependencies = Depends(get_pipeline_dependencies),
):
    store = deps.history_store_factory()
    contract = store.get_contract(investigation_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="Investigation contract not found")
    response = await run_pipeline(
        DashRequest(prompt=contract.request.question, user_id=contract.request.requester),
        deps,
        investigation_id=investigation_id,
        run_type=InvestigationRunType.REFRESH,
        base_revision=contract.investigation.revision,
    )
    if response.investigation_revision is None:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Refresh did not create an authoritative investigation revision.",
                "investigation_id": investigation_id,
                "dashboard_url": response.dashboard_url,
                "dashboard_uid": response.dashboard_uid,
            },
        )
    return response.model_dump(mode="json")


@router.post(
    "/api/v1/investigations/{investigation_id}/migrate",
    tags=["History"],
    summary="Migrate a legacy history record to Investigation Contract v1",
)
async def migrate_investigation(investigation_id: str):
    store = history_mod.get_investigation_store()
    contract = store.migrate_legacy_investigation(investigation_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return contract.model_dump(mode="json", by_alias=True)


@router.get(
    "/api/v1/investigations/{investigation_id}/assessment-bundle",
    tags=["History"],
    summary="Export a portable investigation assessment bundle",
)
async def export_investigation_assessment_bundle(investigation_id: str, revision: int | None = None):
    store = history_mod.get_investigation_store()
    try:
        content = build_investigation_bundle(store, investigation_id, revision=revision)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    filename = f"tacit-investigation-{investigation_id}.tar.gz"
    return Response(
        content=content,
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/api/v1/investigations/{investigation_id}",
    tags=["History"],
    summary="Get investigation details",
    response_description="Full investigation record with all pipeline data",
)
async def get_investigation(investigation_id: str):
    """Get full details of a single investigation by ID."""
    store = history_mod.get_investigation_store()
    inv = store.get(investigation_id)
    if inv is None:
        raise HTTPException(status_code=404, detail="Investigation not found")
    return inv
