"""Archetype management routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dashforge.api.security import verify_api_key
from dashforge.models.schemas import ArchetypeListResponse, ArchetypeReloadResponse

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.post(
    "/api/v1/archetypes/reload",
    tags=["Archetypes"],
    summary="Reload archetypes from YAML",
    response_model=ArchetypeReloadResponse,
    response_description="Confirmation with count and summary of loaded archetypes",
)
async def reload_archetypes_endpoint():
    """Hot-reload archetype templates without server restart."""
    from dashforge.archetypes.templates import reload_archetypes

    reload_archetypes()
    from dashforge.archetypes.templates import ALL_ARCHETYPES as reloaded

    return {
        "message": "Archetypes reloaded",
        "count": len(reloaded),
        "archetypes": [{"id": a.id, "name": a.name, "panels": len(a.panels)} for a in reloaded],
    }


@router.get(
    "/api/v1/archetypes",
    tags=["Archetypes"],
    summary="List all archetypes",
    response_model=ArchetypeListResponse,
    response_description="All loaded investigation archetypes with their panels and problem types",
)
async def list_archetypes():
    """List all currently loaded investigation archetypes."""
    from dashforge.archetypes.templates import ALL_ARCHETYPES

    return {
        "count": len(ALL_ARCHETYPES),
        "archetypes": [
            {
                "id": a.id,
                "name": a.name,
                "description": a.description,
                "problem_types": a.problem_types,
                "panel_count": len(a.panels),
                "panels": [p.title for p in a.panels],
                "tags": a.tags,
            }
            for a in ALL_ARCHETYPES
        ],
    }
