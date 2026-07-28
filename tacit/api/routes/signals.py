"""Semantic signal taxonomy routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from tacit.api.dependencies import get_knowledge_service, get_signal_store
from tacit.api.security import KnowledgeAction, assert_knowledge_action, knowledge_tenant, verify_api_key
from tacit.models.schemas import TeachSignalRequest, TeachSignalResponse

router = APIRouter(dependencies=[Depends(verify_api_key)])


@router.get(
    "/api/v1/signals",
    tags=["Signals"],
    summary="List all signal types",
    response_description="All registered semantic signal types with categories",
)
async def list_signals(request: Request, store: Any = Depends(get_signal_store)):
    """List all registered semantic signal types."""
    return {"signal_types": store.list_signal_types(tenant_id=knowledge_tenant(request))}


@router.get(
    "/api/v1/signals/stats",
    tags=["Signals"],
    summary="Signal store statistics",
    response_description="Summary stats: signal types, mappings, ingested dashboards",
)
async def signal_stats(request: Request, store: Any = Depends(get_signal_store)):
    """Summary statistics for the signal mapping store."""
    return store.stats(tenant_id=knowledge_tenant(request))


@router.get(
    "/api/v1/signals/{signal_type}",
    tags=["Signals"],
    summary="Get signal type details",
    response_description="Signal type with all metric mappings, confidence scores, and provenance",
)
async def get_signal(signal_type: str, request: Request, store: Any = Depends(get_signal_store)):
    """Get a signal type with all its metric mappings."""
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
async def teach_signal(
    payload: TeachSignalRequest,
    request: Request,
    store: Any = Depends(get_signal_store),
    knowledge_service: Any = Depends(get_knowledge_service),
) -> TeachSignalResponse:
    """Teach Tacit an organization-specific signal mapping."""
    assert_knowledge_action(request, KnowledgeAction.TEACH_SIGNALS)
    tenant_id = knowledge_tenant(request)
    store.register_signal_type(
        signal_type=payload.signal_type,
        description=payload.description,
        category=payload.category,
        unit=payload.unit,
        tenant_id=tenant_id,
    )

    mappings_created = 0
    source_ref = f"manual:{payload.taught_by}"
    from tacit.knowledge.enums import KnowledgeEligibility, LifecycleStatus
    from tacit.knowledge.migration import migrate_signal_mapping

    for mp in payload.metric_patterns:
        candidate_id = migrate_signal_mapping(
            {
                "id": f"teach:{payload.signal_type}:{mp.pattern}",
                "signal_type": payload.signal_type,
                "metric_pattern": mp.pattern,
                "confidence": mp.confidence,
                "context_services": payload.services,
                "context_datasource_types": payload.datasource_types,
                "context_environments": payload.environments,
                "source_type": "human",
                "source_refs": [source_ref],
                "review_state": "trusted",
            },
            service=knowledge_service,
            tenant_id=tenant_id,
        )
        _decision, revision = knowledge_service.evaluate_candidate(
            candidate_id,
            tenant_id=tenant_id,
        )
        if (
            revision is not None
            and revision.state.lifecycle_status == LifecycleStatus.ACTIVE
            and revision.state.eligibility != KnowledgeEligibility.INELIGIBLE
        ):
            mappings_created += 1

    return TeachSignalResponse(
        signal_type=payload.signal_type,
        mappings_created=mappings_created,
        message=f"Signal '{payload.signal_type}' updated with {mappings_created} mapping(s)",
    )
