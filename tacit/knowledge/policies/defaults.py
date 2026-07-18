"""Conservative v1 promotion policies by knowledge kind."""

from __future__ import annotations

from tacit.knowledge.enums import (
    EntityResolutionStatus,
    KnowledgeEligibility,
    KnowledgeKind,
    LifecycleStatus,
    PromotionDecisionType,
    ReviewState,
    SourceFamily,
)
from tacit.knowledge.models import KnowledgeCandidate, PromotionContext, PromotionDecision
from tacit.knowledge.normalization import stable_fingerprint


class ConservativePromotionPolicy:
    version = "1"

    def __init__(self, kind: KnowledgeKind):
        self.knowledge_kind = kind
        self.policy_id = f"{kind.value}-promotion-v1"

    def evaluate(self, candidate: KnowledgeCandidate, context: PromotionContext) -> PromotionDecision:
        reasons = []
        if candidate.kind != self.knowledge_kind:
            reasons.append("unknown_knowledge_kind")
        if candidate.entity_resolution.status != EntityResolutionStatus.RESOLVED:
            reasons.append("entity_unresolved")
        if candidate.state.review_state == ReviewState.REJECTED:
            reasons.append("candidate_rejected")
        if candidate.state.review_state == ReviewState.CANDIDATE:
            reasons.append("review_required")
        if candidate.state.lifecycle_status != LifecycleStatus.ACTIVE:
            reasons.append(f"lifecycle_{candidate.state.lifecycle_status.value}")
        if not candidate.provenance_refs:
            reasons.append("provenance_missing")
        if context.unresolved_conflict_count:
            reasons.append("unresolved_conflict")
        reasons.extend(self._kind_reasons(candidate, context))

        eligible = not reasons
        resulting = (
            KnowledgeEligibility.LIVE_VERIFIED
            if eligible and context.live_verified
            else KnowledgeEligibility.CONTEXTUAL_ONLY if eligible else KnowledgeEligibility.INELIGIBLE
        )
        input_payload = {
            "candidate": candidate.model_dump(mode="json"),
            "context": context.model_dump(mode="json"),
            "policy": {"id": self.policy_id, "version": self.version},
        }
        fingerprint = stable_fingerprint(input_payload)
        return PromotionDecision(
            decision_id=f"promotion_{fingerprint.split(':', 1)[1][:20]}",
            candidate_ref=candidate.id,
            policy_id=self.policy_id,
            policy_version=self.version,
            decision=PromotionDecisionType.PROMOTE if eligible else PromotionDecisionType.RETAIN_CANDIDATE,
            resulting_eligibility=resulting,
            reason_codes=[] if eligible else sorted(set(reasons)),
            input_fingerprint=fingerprint,
        )

    def _kind_reasons(self, candidate: KnowledgeCandidate, context: PromotionContext) -> list[str]:
        corroboration = context.corroboration
        if self.knowledge_kind == KnowledgeKind.DEPENDENCY:
            if not candidate.proposition.object_ref:
                return ["dependency_object_unresolved"]
            if corroboration.independent_source_family_count < 2 and not context.authoritative_source:
                return ["insufficient_independent_sources"]
        elif self.knowledge_kind == KnowledgeKind.OWNERSHIP:
            if not candidate.scope.service_refs:
                return ["scope_missing"]
            if not context.authoritative_source and SourceFamily.HUMAN_CORRECTION not in corroboration.source_families:
                return ["authoritative_source_or_human_review_required"]
        elif self.knowledge_kind == KnowledgeKind.SIGNAL_MAPPING:
            if not candidate.proposition.concept_ref and not candidate.proposition.object_ref:
                return ["signal_unresolved"]
            if (
                not context.authoritative_source
                and not context.live_verified
                and corroboration.independent_source_count < 2
            ):
                return ["live_coverage_or_repeated_resolution_required"]
        elif self.knowledge_kind == KnowledgeKind.EVIDENCE_REQUIREMENT:
            if not candidate.proposition.concept_ref and not context.authoritative_source:
                return ["investigation_purpose_missing"]
        return []


def default_policies() -> dict[KnowledgeKind, ConservativePromotionPolicy]:
    return {kind: ConservativePromotionPolicy(kind) for kind in KnowledgeKind}
