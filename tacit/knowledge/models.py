"""Typed governance models for Operational Knowledge."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from tacit.knowledge.enums import (
    ConflictKind,
    ConflictResolutionStatus,
    CorrectionType,
    CorroborationStatus,
    EntityBindingMethod,
    EntityKind,
    EntityResolutionStatus,
    EntityStatus,
    EvidenceRole,
    KnowledgeEligibility,
    KnowledgeKind,
    KnowledgeUsageDisposition,
    LifecycleStatus,
    LineageKind,
    Predicate,
    PromotionDecisionType,
    ReviewState,
    SourceFamily,
)


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class KnowledgeScope(BaseModel):
    tenant_id: str = "default"
    environment_refs: list[str] = Field(default_factory=list)
    region_refs: list[str] = Field(default_factory=list)
    cluster_refs: list[str] = Field(default_factory=list)
    namespace_refs: list[str] = Field(default_factory=list)
    service_refs: list[str] = Field(default_factory=list)
    archetype_refs: list[str] = Field(default_factory=list)
    version_constraints: list[str] = Field(default_factory=list)
    valid_from: datetime | None = None
    valid_until: datetime | None = None

    @field_validator("valid_from", "valid_until")
    @classmethod
    def normalize_validity_datetime(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def applies_to(self, other: KnowledgeScope) -> bool:
        if self.tenant_id != other.tenant_id:
            return False
        for field_name in (
            "environment_refs",
            "region_refs",
            "cluster_refs",
            "namespace_refs",
            "service_refs",
            "archetype_refs",
            "version_constraints",
        ):
            required = set(getattr(self, field_name))
            actual = set(getattr(other, field_name))
            if required and not required.intersection(actual):
                return False
        now = utc_now()
        return not ((self.valid_from and now < self.valid_from) or (self.valid_until and now >= self.valid_until))


class KnowledgeProposition(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    kind: KnowledgeKind
    subject_ref: str
    predicate: Predicate
    object_ref: str = ""
    concept_ref: str = ""
    source_wording: str = ""
    uncertainty: str = "unknown"
    proposition_key: str = ""


class KnowledgeState(BaseModel):
    review_state: ReviewState = ReviewState.CANDIDATE
    lifecycle_status: LifecycleStatus = LifecycleStatus.ACTIVE
    eligibility: KnowledgeEligibility = KnowledgeEligibility.INELIGIBLE

    @model_validator(mode="after")
    def enforce_ineligible_states(self) -> KnowledgeState:
        if self.review_state == ReviewState.REJECTED and self.eligibility != KnowledgeEligibility.INELIGIBLE:
            raise ValueError("rejected knowledge must be ineligible")
        if (
            self.lifecycle_status
            in {
                LifecycleStatus.EXPIRED,
                LifecycleStatus.SUPERSEDED,
                LifecycleStatus.WITHDRAWN,
            }
            and self.eligibility != KnowledgeEligibility.INELIGIBLE
        ):
            raise ValueError(f"{self.lifecycle_status.value} knowledge must be ineligible")
        return self


class EntityBinding(BaseModel):
    entity_ref: str
    method: EntityBindingMethod
    confidence: str = "deterministic"
    provenance_refs: list[str] = Field(default_factory=list)


class EntityResolutionResult(BaseModel):
    status: EntityResolutionStatus
    raw_value: str
    candidate_bindings: list[EntityBinding] = Field(default_factory=list)
    selected_entity_ref: str = ""
    reason_codes: list[str] = Field(default_factory=list)


class KnowledgeEvidenceReference(BaseModel):
    evidence_ref: str
    evidence_role: EvidenceRole = EvidenceRole.SUPPORTING
    source_family: SourceFamily = SourceFamily.UNKNOWN
    lineage_group: str = ""
    lineage_kind: LineageKind = LineageKind.UNKNOWN
    provenance_refs: list[str] = Field(default_factory=list)
    observed_at: datetime | None = None


class KnowledgeEvidence(BaseModel):
    items: list[KnowledgeEvidenceReference] = Field(default_factory=list)

    @property
    def supporting_evidence_refs(self) -> list[str]:
        return [item.evidence_ref for item in self.items if item.evidence_role == EvidenceRole.SUPPORTING]

    @property
    def contradicting_evidence_refs(self) -> list[str]:
        return [item.evidence_ref for item in self.items if item.evidence_role == EvidenceRole.CONTRADICTING]


class CorroborationSummary(BaseModel):
    proposition_key: str
    raw_source_count: int = 0
    independent_source_count: int = 0
    independent_source_family_count: int = 0
    source_families: list[SourceFamily] = Field(default_factory=list)
    duplicate_source_count: int = 0
    status: CorroborationStatus = CorroborationStatus.UNCORROBORATED


class CandidatePolicyState(BaseModel):
    promotion_policy_ref: str = ""
    last_evaluated_at: datetime | None = None
    eligibility_reason_codes: list[str] = Field(default_factory=list)


class MigrationProvenance(BaseModel):
    source_type: Literal["migration"] = "migration"
    migration_adapter: Literal["knowledge-migration-v1"] = "knowledge-migration-v1"
    original_record_ref: str


class KnowledgeCandidate(BaseModel):
    id: str
    tenant_id: str = "default"
    record_type: Literal["knowledge_candidate"] = "knowledge_candidate"
    kind: KnowledgeKind
    payload_ref: str
    typed_payload: dict[str, Any] = Field(default_factory=dict)
    proposition: KnowledgeProposition
    scope: KnowledgeScope = Field(default_factory=KnowledgeScope)
    state: KnowledgeState = Field(default_factory=KnowledgeState)
    entity_resolution: EntityResolutionResult
    evidence: KnowledgeEvidence = Field(default_factory=KnowledgeEvidence)
    corroboration: CorroborationSummary | None = None
    confidence: dict[str, Any] = Field(default_factory=lambda: {"type_prior": "medium", "calibrated_score": None})
    policy: CandidatePolicyState = Field(default_factory=CandidatePolicyState)
    provenance_refs: list[str] = Field(default_factory=list)
    security_flags: list[str] = Field(default_factory=list)
    migration_provenance: MigrationProvenance | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def validate_candidate(self) -> KnowledgeCandidate:
        if self.scope.tenant_id != self.tenant_id:
            raise ValueError("candidate tenant and scope tenant must match")
        if self.entity_resolution.status != EntityResolutionStatus.RESOLVED:
            if self.state.eligibility != KnowledgeEligibility.INELIGIBLE:
                raise ValueError("unresolved or ambiguous candidates must be ineligible")
        if not self.provenance_refs:
            raise ValueError("knowledge candidates require provenance")
        if (
            self.state.eligibility == KnowledgeEligibility.LIVE_VERIFIED
            and self.state.review_state == ReviewState.CANDIDATE
        ):
            raise ValueError("new candidates cannot be live_verified")
        return self


class PromotionContext(BaseModel):
    corroboration: CorroborationSummary
    unresolved_conflict_count: int = 0
    authoritative_source: bool = False
    live_verified: bool = False


class PromotionDecision(BaseModel):
    decision_id: str
    candidate_ref: str
    policy_id: str
    policy_version: str
    decision: PromotionDecisionType
    resulting_eligibility: KnowledgeEligibility
    reason_codes: list[str] = Field(default_factory=list)
    input_fingerprint: str
    evaluated_at: datetime = Field(default_factory=utc_now)


class KnowledgeConflict(BaseModel):
    id: str
    tenant_id: str = "default"
    conflict_kind: ConflictKind
    left_proposition_ref: str
    right_proposition_ref: str
    resolution_status: ConflictResolutionStatus = ConflictResolutionStatus.UNRESOLVED
    scope_analysis: dict[str, Any] = Field(default_factory=dict)
    temporal_analysis: dict[str, Any] = Field(default_factory=dict)
    severity: str = "medium"
    resolution_reason: str = ""
    resolved_by: str = ""
    created_at: datetime = Field(default_factory=utc_now)
    resolved_at: datetime | None = None


class OperationalKnowledgeItem(BaseModel):
    id: str
    tenant_id: str = "default"
    kind: KnowledgeKind
    current_revision: int = Field(ge=1)
    status: LifecycleStatus = LifecycleStatus.ACTIVE
    created_at: datetime
    updated_at: datetime


class KnowledgeRevision(BaseModel):
    knowledge_id: str
    tenant_id: str = "default"
    revision: int = Field(ge=1)
    parent_revision: int | None = None
    schema_version: Literal["1.0"] = "1.0"
    proposition: KnowledgeProposition
    scope: KnowledgeScope
    state: KnowledgeState
    corroboration_snapshot_ref: str
    conflict_refs: list[str] = Field(default_factory=list)
    policy_id: str
    policy_version: str
    decision_ref: str
    promoted_from_candidate_refs: list[str] = Field(default_factory=list)
    provenance_refs: list[str] = Field(default_factory=list)
    revision_reason: str = "promoted"
    semantic_fingerprint: str
    created_at: datetime = Field(default_factory=utc_now)


class KnowledgeSnapshotItem(BaseModel):
    knowledge_ref: str
    revision: int


class KnowledgeSnapshot(BaseModel):
    id: str
    tenant_id: str = "default"
    created_at: datetime = Field(default_factory=utc_now)
    items: list[KnowledgeSnapshotItem] = Field(default_factory=list)
    fingerprint: str


class KnowledgeUsage(BaseModel):
    usage_id: str = ""
    tenant_id: str = "default"
    investigation_id: str = ""
    investigation_revision: int = 0
    knowledge_ref: str
    knowledge_revision: int = Field(ge=1)
    disposition: KnowledgeUsageDisposition
    used_for: list[str] = Field(default_factory=list)
    target_ref: str = ""
    score_delta: float = 0.0
    decision_ref: str = ""
    provenance_refs: list[str] = Field(default_factory=list)
    reason_codes: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def reject_non_applied_contributions(self) -> KnowledgeUsage:
        if self.disposition != KnowledgeUsageDisposition.APPLIED and self.score_delta != 0:
            raise ValueError("non-applied knowledge cannot contribute a score delta")
        return self


class KnowledgeCorrection(BaseModel):
    id: str
    tenant_id: str = "default"
    investigation_id: str
    investigation_revision: int
    correction_type: CorrectionType
    target_ref: str = ""
    original: dict[str, Any] = Field(default_factory=dict)
    proposed: dict[str, Any]
    scope: KnowledgeScope
    explanation: str
    review_state: ReviewState = ReviewState.CANDIDATE
    created_by: str
    created_at: datetime = Field(default_factory=utc_now)
    knowledge_candidate_ref: str = ""


class KnowledgeImpact(BaseModel):
    knowledge_ref: str
    affected_investigations: list[dict[str, Any]] = Field(default_factory=list)
    recommended_action: str = "replay_current"


class Entity(BaseModel):
    id: str
    tenant_id: str = "default"
    kind: EntityKind
    canonical_name: str
    display_name: str = ""
    status: EntityStatus = EntityStatus.ACTIVE
    scope: KnowledgeScope = Field(default_factory=KnowledgeScope)
    provenance_refs: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class EntityAlias(BaseModel):
    id: str
    tenant_id: str = "default"
    raw_value: str
    normalized_value: str
    entity_ref: str
    scope: KnowledgeScope = Field(default_factory=KnowledgeScope)
    method: EntityBindingMethod
    review_state: ReviewState = ReviewState.CANDIDATE
    lifecycle_status: LifecycleStatus = LifecycleStatus.ACTIVE
    provenance_refs: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def require_provenance(self) -> EntityAlias:
        if not self.provenance_refs:
            raise ValueError("entity aliases require provenance")
        return self
