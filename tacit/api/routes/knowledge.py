"""Governed Operational Knowledge lifecycle API."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from tacit.api.dependencies import (
    get_correction_knowledge_service,
    get_history_store,
    get_knowledge_repository,
    get_knowledge_service,
)
from tacit.api.security import (
    KnowledgeAction,
    assert_contract_tenant_access,
    assert_knowledge_action,
    authenticated_actor,
    knowledge_tenant,
    require_knowledge_action,
    require_knowledge_tenant,
    verify_api_key,
)
from tacit.errors import SemanticAuthorizationError
from tacit.knowledge.entities import normalize_entity
from tacit.knowledge.enums import CorrectionType, EntityBindingMethod, EntityKind, ReviewState
from tacit.knowledge.models import Entity, EntityAlias, KnowledgeScope
from tacit.knowledge.repository import (
    AliasRegistrationConflictError,
    CandidateEvaluationConflictError,
    CandidateReviewConflictError,
    EntityRegistrationConflictError,
    KnowledgeRevisionConflictError,
)
from tacit.pagination import MAX_COMPATIBILITY_OFFSET

router = APIRouter(
    dependencies=[
        Depends(verify_api_key),
        Depends(require_knowledge_tenant),
        Depends(require_knowledge_action(KnowledgeAction.READ)),
    ]
)


class CandidateReviewRequest(BaseModel):
    decision: Literal["approve", "reject", "trust"]
    reviewer: str = Field(
        default="",
        max_length=200,
        description="Optional unverified display label; the audit actor is derived from authentication",
    )
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
    created_by: str = Field(
        default="",
        max_length=200,
        description="Optional unverified display label; the audit actor is derived from authentication",
    )
    target_ref: str = ""


class CorrectionReviewRequest(BaseModel):
    decision: Literal["approve", "reject"]
    reviewer: str = Field(
        default="",
        max_length=200,
        description="Optional unverified display label; the audit actor is derived from authentication",
    )
    authoritative: bool = False


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
    return get_knowledge_repository(request).stats(_tenant(request))


@router.get("/api/v1/knowledge/review-queue", tags=["Operational Knowledge"])
async def review_queue(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    candidate_cursor: str | None = Query(default=None, max_length=1_024),
    conflict_cursor: str | None = Query(default=None, max_length=1_024),
    attention_cursor: str | None = Query(default=None, max_length=1_024),
):
    tenant_id = _tenant(request)
    repository = get_knowledge_repository(request)
    try:
        candidate_page = repository.list_review_candidates_page(
            tenant_id,
            limit=limit,
            cursor=candidate_cursor,
        )
        conflict_page = repository.list_conflicts_page(
            tenant_id,
            unresolved_only=True,
            limit=limit,
            cursor=conflict_cursor,
        )
        attention_page = repository.list_current_revisions_page(
            tenant_id,
            lifecycle_status="stale",
            limit=limit,
            cursor=attention_cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    candidates = candidate_page.candidates
    unresolved_keys = repository.unresolved_proposition_keys(
        tenant_id,
        {candidate.proposition.proposition_key for candidate in candidates},
    )
    attention_items = [
        revision.model_dump(mode="json")
        for revision in attention_page.revisions
        if revision.state.lifecycle_status.value == "stale"
        and revision.state.review_state in {ReviewState.APPROVED, ReviewState.TRUSTED}
    ]
    return {
        "tenant_id": tenant_id,
        "candidates": _prioritize_candidates(candidates, unresolved_keys),
        "candidate_has_more": candidate_page.has_more,
        "candidate_next_cursor": candidate_page.next_cursor,
        "unresolved_conflicts": _dump(conflict_page.conflicts),
        "conflict_has_more": conflict_page.has_more,
        "conflict_next_cursor": conflict_page.next_cursor,
        "attention_items": attention_items,
        "attention_has_more": attention_page.has_more,
        "attention_next_cursor": attention_page.next_cursor,
    }


@router.get("/api/v1/knowledge/conflicts", tags=["Operational Knowledge"])
async def list_conflicts(
    request: Request,
    response: Response,
    unresolved_only: bool = False,
    limit: int = Query(default=200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=512),
    offset: int | None = Query(default=None, ge=0, le=10_000),
):
    repository = get_knowledge_repository(request)
    tenant_id = _tenant(request)
    try:
        if offset is not None:
            if cursor is not None:
                raise ValueError("conflict cursor and offset cannot be combined")
            return _dump(
                repository.list_conflicts(
                    tenant_id,
                    unresolved_only=unresolved_only,
                    limit=limit,
                    offset=offset,
                )
            )
        page = repository.list_conflicts_page(
            tenant_id,
            unresolved_only=unresolved_only,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response.headers["X-Tacit-Has-More"] = str(page.has_more).lower()
    if page.next_cursor:
        response.headers["X-Tacit-Next-Cursor"] = page.next_cursor
    return _dump(page.conflicts)


@router.get("/api/v1/knowledge/candidates", tags=["Operational Knowledge"])
async def list_candidates(
    request: Request,
    kind: str | None = None,
    review_state: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=512),
):
    try:
        page = get_knowledge_repository(request).list_candidates_page(
            _tenant(request),
            kind=kind,
            review_state=review_state,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "candidates": [_candidate_dump(candidate) for candidate in page.candidates],
        "has_more": page.has_more,
        "next_cursor": page.next_cursor,
    }


@router.post("/api/v1/knowledge/candidates/{candidate_id}/review", tags=["Operational Knowledge"])
async def review_candidate(candidate_id: str, payload: CandidateReviewRequest, request: Request):
    action = {
        "approve": KnowledgeAction.APPROVE,
        "reject": KnowledgeAction.REJECT,
        "trust": KnowledgeAction.TRUST,
    }[payload.decision]
    assert_knowledge_action(request, action)
    if payload.authoritative_source or payload.live_verified:
        assert_knowledge_action(request, KnowledgeAction.OVERRIDE)
    if payload.evaluate and payload.decision != "reject":
        assert_knowledge_action(request, KnowledgeAction.APPLY)
    tenant_id = _tenant(request)
    service = get_knowledge_service(request)
    try:
        candidate = service.review_candidate(
            candidate_id,
            approved=payload.decision != "reject",
            reviewer=authenticated_actor(request),
            tenant_id=tenant_id,
            trust=payload.decision == "trust",
            can_trust=payload.decision == "trust",
        )
        decision = revision = None
        if payload.evaluate and payload.decision != "reject":
            evaluation = service.evaluate_candidate_result(
                candidate_id,
                tenant_id=tenant_id,
                authoritative_source=payload.authoritative_source,
                live_verified=payload.live_verified,
            )
            candidate = evaluation.candidate
            decision = evaluation.decision
            revision = evaluation.revision
        return {
            "candidate": _candidate_dump(candidate),
            "promotion_decision": decision.model_dump(mode="json") if decision else None,
            "knowledge_revision": revision.model_dump(mode="json") if revision else None,
        }
    except SemanticAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except (
        CandidateEvaluationConflictError,
        CandidateReviewConflictError,
        KnowledgeRevisionConflictError,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        status_code = 404 if service.repository.get_candidate(candidate_id, tenant_id) is None else 409
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc


@router.post("/api/v1/knowledge/{knowledge_id}/review", tags=["Operational Knowledge"])
async def review_knowledge(knowledge_id: str, payload: CandidateReviewRequest, request: Request):
    """Review a queued knowledge candidate through the canonical product route."""
    return await review_candidate(knowledge_id, payload, request)


@router.post(
    "/api/v1/knowledge/entities",
    tags=["Operational Knowledge"],
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.APPROVE))],
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
    try:
        return get_knowledge_service(request).register_entity(entity).model_dump(mode="json")
    except SemanticAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except EntityRegistrationConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/api/v1/knowledge/aliases",
    tags=["Operational Knowledge"],
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.APPROVE))],
)
async def create_alias(payload: AliasRequest, request: Request):
    if payload.review_state == ReviewState.TRUSTED:
        assert_knowledge_action(request, KnowledgeAction.TRUST)
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
        return get_knowledge_service(request).register_alias(alias).model_dump(mode="json")
    except AliasRegistrationConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/api/v1/knowledge/corrections",
    tags=["Operational Knowledge"],
    dependencies=[Depends(require_knowledge_action(KnowledgeAction.CORRECT))],
)
async def create_correction(
    payload: CorrectionRequest,
    request: Request,
    history_store: Any = Depends(get_history_store),
    knowledge_service: Any = Depends(get_correction_knowledge_service),
):
    selected_tenant = _tenant(request)
    contract = history_store.get_contract(
        payload.investigation_id,
        payload.investigation_revision,
        tenant_id=selected_tenant,
    )
    if contract is None:
        raise HTTPException(status_code=404, detail="Investigation revision not found")
    tenant_id = assert_contract_tenant_access(request, contract, store=history_store)
    target_usage = next(
        (usage for usage in contract.knowledge_usage if usage.knowledge_ref == payload.target_ref),
        None,
    )
    if payload.target_ref and target_usage is None:
        raise HTTPException(
            status_code=400,
            detail="Correction target was not considered by the referenced investigation revision",
        )
    try:
        correction, candidate = knowledge_service.create_correction(
            investigation_id=payload.investigation_id,
            investigation_revision=payload.investigation_revision,
            correction_type=payload.correction_type,
            proposed=payload.proposed,
            scope=payload.scope.model_copy(update={"tenant_id": tenant_id}),
            explanation=payload.explanation,
            created_by=authenticated_actor(request),
            target_ref=payload.target_ref,
            target_revision=target_usage.knowledge_revision if target_usage is not None else None,
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
    assert_knowledge_action(
        request,
        KnowledgeAction.APPROVE if payload.decision == "approve" else KnowledgeAction.REJECT,
    )
    if payload.decision == "approve":
        assert_knowledge_action(request, KnowledgeAction.APPLY)
    if payload.authoritative:
        assert_knowledge_action(request, KnowledgeAction.OVERRIDE)
    tenant_id = _tenant(request)
    service = get_knowledge_service(request)
    try:
        correction, revision = service.review_correction(
            correction_id,
            approved=payload.decision == "approve",
            reviewer=authenticated_actor(request),
            tenant_id=tenant_id,
            authoritative=payload.authoritative,
        )
        return {
            "correction": correction.model_dump(mode="json"),
            "knowledge_revision": revision.model_dump(mode="json") if revision else None,
        }
    except (
        CandidateEvaluationConflictError,
        CandidateReviewConflictError,
        KnowledgeRevisionConflictError,
    ) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        status_code = 404 if service.repository.get_correction(correction_id, tenant_id) is None else 409
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc


@router.get("/api/v1/knowledge/corrections/{correction_id}", tags=["Operational Knowledge"])
async def get_correction(correction_id: str, request: Request):
    correction = get_knowledge_repository(request).get_correction(correction_id, _tenant(request))
    if correction is None:
        raise HTTPException(status_code=404, detail="knowledge correction not found")
    return correction.model_dump(mode="json")


@router.get("/api/v1/knowledge/{knowledge_id}/revisions", tags=["Operational Knowledge"])
async def list_revisions(
    knowledge_id: str,
    request: Request,
    response: Response,
    limit: int = Query(default=200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=512),
    offset: int | None = Query(default=None, ge=0, le=10_000),
):
    repository = get_knowledge_repository(request)
    tenant_id = _tenant(request)
    try:
        if offset is not None:
            if cursor is not None:
                raise ValueError("revision cursor and offset cannot be combined")
            revisions = repository.list_revisions(
                knowledge_id,
                tenant_id,
                limit=limit,
                offset=offset,
            )
            if not revisions and offset == 0:
                raise HTTPException(status_code=404, detail="knowledge item not found")
            return _dump(revisions)
        page = repository.list_revisions_page(
            knowledge_id,
            tenant_id,
            limit=limit,
            cursor=cursor,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not page.revisions and cursor is None:
        raise HTTPException(status_code=404, detail="knowledge item not found")
    response.headers["X-Tacit-Has-More"] = str(page.has_more).lower()
    if page.next_cursor:
        response.headers["X-Tacit-Next-Cursor"] = page.next_cursor
    return _dump(page.revisions)


@router.get("/api/v1/knowledge/{knowledge_id}/usage", tags=["Operational Knowledge"])
async def list_usage(
    knowledge_id: str,
    request: Request,
    response: Response,
    limit: int = Query(default=200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=512),
    offset: int | None = Query(default=None, ge=0, le=10_000),
):
    repository = get_knowledge_repository(request)
    tenant_id = _tenant(request)
    try:
        if offset is not None:
            if cursor is not None:
                raise ValueError("usage cursor and offset cannot be combined")
            return _dump(
                repository.list_usage(
                    tenant_id=tenant_id,
                    knowledge_id=knowledge_id,
                    limit=limit,
                    offset=offset,
                )
            )
        page = repository.list_usage_page(
            tenant_id=tenant_id,
            knowledge_id=knowledge_id,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response.headers["X-Tacit-Has-More"] = str(page.has_more).lower()
    if page.next_cursor:
        response.headers["X-Tacit-Next-Cursor"] = page.next_cursor
    return _dump(page.usage)


@router.get("/api/v1/knowledge/{knowledge_id}/impact", tags=["Operational Knowledge"])
async def knowledge_impact(
    knowledge_id: str,
    request: Request,
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0, le=MAX_COMPATIBILITY_OFFSET),
):
    return (
        get_knowledge_service(request)
        .impact(
            knowledge_id,
            _tenant(request),
            limit=limit,
            offset=offset,
        )
        .model_dump(mode="json")
    )


@router.get("/api/v1/knowledge/{knowledge_id}/explain", tags=["Operational Knowledge"])
async def explain_knowledge(
    knowledge_id: str,
    request: Request,
    history_limit: int = Query(default=200, ge=1, le=500),
    history_offset: int = Query(default=0, ge=0, le=MAX_COMPATIBILITY_OFFSET),
):
    try:
        return get_knowledge_service(request).explain(
            knowledge_id,
            _tenant(request),
            history_limit=history_limit,
            history_offset=history_offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/v1/knowledge/{knowledge_id}", tags=["Operational Knowledge"])
async def get_knowledge(knowledge_id: str, request: Request, revision: int | None = None):
    value = get_knowledge_repository(request).get_revision(
        knowledge_id,
        revision=revision,
        tenant_id=_tenant(request),
    )
    if value is None:
        raise HTTPException(status_code=404, detail="knowledge item not found")
    return value.model_dump(mode="json")


@router.get("/api/v1/knowledge", tags=["Operational Knowledge"])
async def list_knowledge(
    request: Request,
    response: Response,
    kind: str | None = None,
    status: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
    cursor: str | None = Query(default=None, max_length=512),
    offset: int | None = Query(default=None, ge=0, le=10_000),
):
    tenant_id = _tenant(request)
    repository = get_knowledge_repository(request)
    try:
        if offset is not None:
            if cursor is not None:
                raise ValueError("knowledge cursor and offset cannot be combined")
            return _dump(
                repository.list_current_revisions(
                    tenant_id,
                    kind=kind,
                    lifecycle_status=status,
                    limit=limit,
                    offset=offset,
                )
            )
        page = repository.list_current_revisions_page(
            tenant_id,
            kind=kind,
            lifecycle_status=status,
            limit=limit,
            cursor=cursor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    response.headers["X-Tacit-Has-More"] = str(page.has_more).lower()
    if page.next_cursor:
        response.headers["X-Tacit-Next-Cursor"] = page.next_cursor
    return _dump(page.revisions)
