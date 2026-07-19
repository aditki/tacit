"""Semantic signal taxonomy routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

import tacit.signals as signals_mod
from tacit.api.security import knowledge_tenant, verify_api_key
from tacit.models.schemas import TeachSignalRequest, TeachSignalResponse

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get(
    "/api/v1/signals",
    tags=["Signals"],
    summary="List all signal types",
    response_description="All registered semantic signal types with categories",
)
async def list_signals(request: Request):
    """List all registered semantic signal types."""
    store = signals_mod.get_signal_store()
    return {"signal_types": store.list_signal_types(tenant_id=knowledge_tenant(request))}


@router.get(
    "/api/v1/signals/stats",
    tags=["Signals"],
    summary="Signal store statistics",
    response_description="Summary stats: signal types, mappings, ingested dashboards",
)
async def signal_stats(request: Request):
    """Summary statistics for the signal mapping store."""
    store = signals_mod.get_signal_store()
    return store.stats(tenant_id=knowledge_tenant(request))


@router.get(
    "/api/v1/signals/{signal_type}",
    tags=["Signals"],
    summary="Get signal type details",
    response_description="Signal type with all metric mappings, confidence scores, and provenance",
)
async def get_signal(signal_type: str, request: Request):
    """Get a signal type with all its metric mappings."""
    store = signals_mod.get_signal_store()
    result = store.get_signal_type(signal_type, tenant_id=knowledge_tenant(request))
    if result is None:
        raise HTTPException(status_code=404, detail=f"Signal type '{signal_type}' not found")
    return result


@router.post(
    "/api/v1/signals/teach",
    tags=["Signals"],
    summary="Teach Tacit a signal mapping",
    response_model=TeachSignalResponse,
    response_description="Confirmation of the created mapping",
)
async def teach_signal(payload: TeachSignalRequest, request: Request) -> TeachSignalResponse:
    """Teach Tacit an organization-specific signal mapping."""
    store = signals_mod.get_signal_store()
    store.register_signal_type(
        signal_type=payload.signal_type,
        description=payload.description,
        category=payload.category,
        unit=payload.unit,
    )

    mappings_created = 0
    tenant_id = knowledge_tenant(request)
    for mp in payload.metric_patterns:
        store.add_mapping(
            signal_type=payload.signal_type,
            metric_pattern=mp.pattern,
            confidence=mp.confidence,
            context_services=payload.services,
            context_datasource_types=payload.datasource_types,
            context_environments=payload.environments,
            source_type="teach",
            source_refs=[f"manual:{payload.taught_by}"],
            tenant_id=tenant_id,
        )
        mappings_created += 1

    return TeachSignalResponse(
        signal_type=payload.signal_type,
        mappings_created=mappings_created,
        message=f"Signal '{payload.signal_type}' updated with {mappings_created} mapping(s)",
    )
