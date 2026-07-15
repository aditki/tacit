"""Investigation history routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

import tacit.history as history_mod
from tacit.api.security import verify_api_key

router = APIRouter(dependencies=[Depends(verify_api_key)])


class CorrectionCandidateRequest(BaseModel):
    correction_text: str = Field(min_length=1, description="User-provided correction or feedback")
    target_ref: str = Field(default="", description="Optional contract object reference the correction applies to")
    revision: int | None = Field(default=None, description="Contract revision being corrected; defaults to current")
    created_by: str = Field(default="", description="Reviewer or user identifier")


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
async def replay_investigation(investigation_id: str, revision: int | None = None):
    store = history_mod.get_investigation_store()
    contract = store.replay_contract(investigation_id, revision)
    if contract is None:
        raise HTTPException(status_code=404, detail="Investigation contract not found")
    return {
        "mode": "exact",
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
