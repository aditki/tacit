"""Investigation history routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

import tacit.history as history_mod
from tacit.api.security import verify_api_key

router = APIRouter(dependencies=[Depends(verify_api_key)])


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
