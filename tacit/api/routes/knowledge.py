"""Governed Operational Knowledge lifecycle API."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from tacit.api.security import (
    assert_knowledge_permission,
    knowledge_tenant,
    require_knowledge_permission,
    verify_api_key,
)
from tacit.knowledge.entities import normalize_entity
from tacit.knowledge.enums import CorrectionType, EntityBindingMethod, EntityKind, ReviewState
from tacit.knowledge.models import Entity, EntityAlias, KnowledgeScope
from tacit.knowledge.repository import get_knowledge_repository
from tacit.knowledge.service import get_knowledge_service

router = APIRouter(dependencies=[Depends(verify_api_key), Depends(require_knowledge_permission("knowledge.read"))])


class CandidateReviewRequest(BaseModel):
    decision: Literal["approve", "reject", "trust"]
    reviewer: str = Field(min_length=1, max_length=200)
    evaluate: bool = True
    authoritative_source: bool = False
    live_verified: bool = False


class CorrectionRequest(BaseModel):
    investigation_id: str = Field(min_length=1, max_length=200)
    investigation_revision: int = Field(ge=1)
    correction_type: CorrectionType
    proposed: dict[str, Any]
    scope: KnowledgeScope = Field(default_factory=KnowledgeScope)
    explanation: str = Field(min_length=1, max_length=10_000)
    created_by: str = Field(min_length=1, max_length=200)
    target_ref: str = ""


class CorrectionReviewRequest(BaseModel):
    decision: Literal["approve", "reject"]
    reviewer: str = Field(min_length=1, max_length=200)
    authoritative: bool = True


class EntityRequest(BaseModel):
    id: str = Field(min_length=1, max_length=200)
    kind: EntityKind
    canonical_name: str = Field(min_length=1, max_length=500)
    display_name: str = Field(default="", max_length=500)
    scope: KnowledgeScope = Field(default_factory=KnowledgeScope)
    provenance_refs: list[str] = Field(min_length=1)


class AliasRequest(BaseModel):
    id: str = Field(min_length=1, max_length=200)
    raw_value: str = Field(min_length=1, max_length=500)
    entity_ref: str = Field(min_length=1, max_length=200)
    scope: KnowledgeScope = Field(default_factory=KnowledgeScope)
    method: EntityBindingMethod = EntityBindingMethod.HUMAN_CORRECTION
    review_state: ReviewState = ReviewState.APPROVED
    provenance_refs: list[str] = Field(min_length=1)


def _tenant(request: Request) -> str:
    return knowledge_tenant(request)


def _dump(items) -> list[dict[str, Any]]:
    return [item.model_dump(mode="json") for item in items]


def _candidate_dump(candidate) -> dict[str, Any]:
    """Return review metadata without artifact excerpts or raw entity mentions."""
    value = candidate.model_dump(mode="json")
    value.pop("typed_payload", None)
    value["proposition"]["source_wording"] = ""
    value["entity_resolution"]["raw_value"] = ""
    return value


def _prioritize_candidates(candidates, unresolved_keys: set[str]) -> list[dict[str, Any]]:
    prioritized = []
    for candidate in candidates:
        reasons = []
        score = 0
        if candidate.proposition.proposition_key in unresolved_keys:
            score += 100
            reasons.append("unresolved_conflict")
        if candidate.payload_ref.startswith("correction_") or candidate.payload_ref.startswith("correction:"):
            score += 90
            reasons.append("correction_awaiting_review")
        if candidate.entity_resolution.status.value in {"ambiguous", "unresolved"}:
            score += 80
            reasons.append("entity_resolution_blocked")
        if candidate.security_flags:
            score += 70
            reasons.append("security_review")
        if candidate.kind.value in {"dependency", "signal_mapping", "evidence_requirement"}:
            score += 20
            reasons.append("investigation_impact")
        value = _candidate_dump(candidate)
        value["review_priority"] = score
        value["review_priority_reasons"] = reasons
        prioritized.append(value)
    return sorted(prioritized, key=lambda value: (-value["review_priority"], value["id"]))


@router.get("/api/v1/knowledge/status", tags=["Operational Knowledge"])
async def knowledge_status(request: Request):
    return get_knowledge_repository().stats(_tenant(request))


@router.get("/api/v1/knowledge/review-queue", tags=["Operational Knowledge"])
async def review_queue(request: Request, limit: int = Query(default=100, ge=1, le=500)):
    tenant_id = _tenant(request)
    repository = get_knowledge_repository()
    candidates = repository.list_candidates(tenant_id, review_state=ReviewState.CANDIDATE.value, limit=limit)
    conflicts = repository.list_conflicts(tenant_id, unresolved_only=True)
    unresolved_keys = {
        proposition_ref
        for conflict in conflicts
        for proposition_ref in (conflict.left_proposition_ref, conflict.right_proposition_ref)
    }
    attention_items = [
        revision.model_dump(mode="json")
        for revision in repository.list_current_revisions(tenant_id)
        if revision.state.lifecycle_status.value == "stale"
        and revision.state.review_state in {ReviewState.APPROVED, ReviewState.TRUSTED}
    ]
    return {
        "tenant_id": tenant_id,
        "candidates": _prioritize_candidates(candidates, unresolved_keys),
        "unresolved_conflicts": _dump(conflicts),
        "attention_items": attention_items,
    }


@router.get("/api/v1/knowledge/conflicts", tags=["Operational Knowledge"])
async def list_conflicts(request: Request, unresolved_only: bool = False):
    return _dump(get_knowledge_repository().list_conflicts(_tenant(request), unresolved_only=unresolved_only))


@router.get("/api/v1/knowledge/candidates", tags=["Operational Knowledge"])
async def list_candidates(
    request: Request,
    kind: str | None = None,
    review_state: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
):
    candidates = get_knowledge_repository().list_candidates(
        _tenant(request), kind=kind, review_state=review_state, limit=limit
    )
    return [_candidate_dump(candidate) for candidate in candidates]


@router.post("/api/v1/knowledge/candidates/{candidate_id}/review", tags=["Operational Knowledge"])
async def review_candidate(candidate_id: str, payload: CandidateReviewRequest, request: Request):
    permission = {
        "approve": "knowledge.review",
        "reject": "knowledge.reject",
        "trust": "knowledge.trust",
    }[payload.decision]
    assert_knowledge_permission(request, permission)
    tenant_id = _tenant(request)
    service = get_knowledge_service()
    try:
        candidate = service.review_candidate(
            candidate_id,
            approved=payload.decision != "reject",
            reviewer=payload.reviewer,
            tenant_id=tenant_id,
            trust=payload.decision == "trust",
            can_trust=payload.decision == "trust",
        )
        decision = revision = None
        if payload.evaluate and payload.decision != "reject":
            decision, revision = service.evaluate_candidate(
                candidate_id,
                tenant_id=tenant_id,
                authoritative_source=payload.authoritative_source,
                live_verified=payload.live_verified,
            )
        return {
            "candidate": _candidate_dump(candidate),
            "promotion_decision": decision.model_dump(mode="json") if decision else None,
            "knowledge_revision": revision.model_dump(mode="json") if revision else None,
        }
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/api/v1/knowledge/{knowledge_id}/review", tags=["Operational Knowledge"])
async def review_knowledge(knowledge_id: str, payload: CandidateReviewRequest, request: Request):
    """Review a queued knowledge candidate through the canonical product route."""
    return await review_candidate(knowledge_id, payload, request)


@router.post(
    "/api/v1/knowledge/entities",
    tags=["Operational Knowledge"],
    dependencies=[Depends(require_knowledge_permission("knowledge.review"))],
)
async def create_entity(payload: EntityRequest, request: Request):
    tenant_id = _tenant(request)
    scope = payload.scope.model_copy(update={"tenant_id": tenant_id})
    entity = Entity(
        id=payload.id,
        tenant_id=tenant_id,
        kind=payload.kind,
        canonical_name=payload.canonical_name,
        display_name=payload.display_name,
        scope=scope,
        provenance_refs=payload.provenance_refs,
    )
    return get_knowledge_service().register_entity(entity).model_dump(mode="json")


@router.post(
    "/api/v1/knowledge/aliases",
    tags=["Operational Knowledge"],
    dependencies=[Depends(require_knowledge_permission("knowledge.review"))],
)
async def create_alias(payload: AliasRequest, request: Request):
    tenant_id = _tenant(request)
    alias = EntityAlias(
        id=payload.id,
        tenant_id=tenant_id,
        raw_value=payload.raw_value,
        normalized_value=normalize_entity(payload.raw_value),
        entity_ref=payload.entity_ref,
        scope=payload.scope.model_copy(update={"tenant_id": tenant_id}),
        method=payload.method,
        review_state=payload.review_state,
        provenance_refs=payload.provenance_refs,
    )
    try:
        return get_knowledge_service().register_alias(alias).model_dump(mode="json")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/api/v1/knowledge/corrections",
    tags=["Operational Knowledge"],
    dependencies=[Depends(require_knowledge_permission("knowledge.correct"))],
)
async def create_correction(payload: CorrectionRequest, request: Request):
    tenant_id = _tenant(request)
    try:
        correction, candidate = get_knowledge_service().create_correction(
            investigation_id=payload.investigation_id,
            investigation_revision=payload.investigation_revision,
            correction_type=payload.correction_type,
            proposed=payload.proposed,
            scope=payload.scope.model_copy(update={"tenant_id": tenant_id}),
            explanation=payload.explanation,
            created_by=payload.created_by,
            target_ref=payload.target_ref,
            tenant_id=tenant_id,
        )
        return {
            "correction": correction.model_dump(mode="json"),
            "candidate": _candidate_dump(candidate),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/api/v1/knowledge/corrections/{correction_id}/review",
    tags=["Operational Knowledge"],
)
async def review_correction(correction_id: str, payload: CorrectionReviewRequest, request: Request):
    assert_knowledge_permission(request, "knowledge.review" if payload.decision == "approve" else "knowledge.reject")
    try:
        correction, revision = get_knowledge_service().review_correction(
            correction_id,
            approved=payload.decision == "approve",
            reviewer=payload.reviewer,
            tenant_id=_tenant(request),
            authoritative=payload.authoritative,
        )
        return {
            "correction": correction.model_dump(mode="json"),
            "knowledge_revision": revision.model_dump(mode="json") if revision else None,
        }
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/v1/knowledge/corrections/{correction_id}", tags=["Operational Knowledge"])
async def get_correction(correction_id: str, request: Request):
    correction = get_knowledge_repository().get_correction(correction_id, _tenant(request))
    if correction is None:
        raise HTTPException(status_code=404, detail="knowledge correction not found")
    return correction.model_dump(mode="json")


@router.get("/api/v1/knowledge/{knowledge_id}/revisions", tags=["Operational Knowledge"])
async def list_revisions(knowledge_id: str, request: Request):
    revisions = get_knowledge_repository().list_revisions(knowledge_id, _tenant(request))
    if not revisions:
        raise HTTPException(status_code=404, detail="knowledge item not found")
    return _dump(revisions)


@router.get("/api/v1/knowledge/{knowledge_id}/usage", tags=["Operational Knowledge"])
async def list_usage(knowledge_id: str, request: Request):
    return _dump(get_knowledge_repository().list_usage(tenant_id=_tenant(request), knowledge_id=knowledge_id))


@router.get("/api/v1/knowledge/{knowledge_id}/impact", tags=["Operational Knowledge"])
async def knowledge_impact(knowledge_id: str, request: Request):
    return get_knowledge_service().impact(knowledge_id, _tenant(request)).model_dump(mode="json")


@router.get("/api/v1/knowledge/{knowledge_id}/explain", tags=["Operational Knowledge"])
async def explain_knowledge(knowledge_id: str, request: Request):
    try:
        return get_knowledge_service().explain(knowledge_id, _tenant(request))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/v1/knowledge/{knowledge_id}", tags=["Operational Knowledge"])
async def get_knowledge(knowledge_id: str, request: Request, revision: int | None = None):
    value = get_knowledge_repository().get_revision(knowledge_id, revision=revision, tenant_id=_tenant(request))
    if value is None:
        raise HTTPException(status_code=404, detail="knowledge item not found")
    return value.model_dump(mode="json")


@router.get("/api/v1/knowledge", tags=["Operational Knowledge"])
async def list_knowledge(
    request: Request,
    kind: str | None = None,
    status: str | None = None,
):
    revisions = get_knowledge_repository().list_current_revisions(_tenant(request))
    if kind:
        revisions = [item for item in revisions if item.proposition.kind.value == kind]
    if status:
        revisions = [item for item in revisions if item.state.lifecycle_status.value == status]
    return _dump(revisions)
