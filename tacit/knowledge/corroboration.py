"""Independent corroboration and deterministic conflict analysis."""

from __future__ import annotations

from tacit.knowledge.enums import (
    ConflictKind,
    ConflictResolutionStatus,
    CorroborationStatus,
    EntityResolutionStatus,
    EvidenceRole,
    KnowledgeKind,
    LifecycleStatus,
    LineageKind,
    ReviewState,
    SourceFamily,
)
from tacit.knowledge.models import (
    CorroborationSummary,
    KnowledgeCandidate,
    KnowledgeConflict,
    KnowledgeEvidenceReference,
    KnowledgeScope,
)
from tacit.knowledge.normalization import stable_fingerprint
from tacit.knowledge.repository import CandidateEvaluationConflictError, KnowledgeRepository
from tacit.knowledge.versioning import version_scopes_overlap


class CorroborationService:
    def __init__(self, repository: KnowledgeRepository, *, candidate_limit: int = 1_000):
        if candidate_limit < 1:
            raise ValueError("candidate_limit must be positive")
        self.repository = repository
        self.candidate_limit = candidate_limit

    def analyze(self, tenant_id: str, proposition_key: str) -> tuple[CorroborationSummary, str]:
        candidates = self.reviewed_candidates(tenant_id, proposition_key)
        evidence = [
            item
            for candidate in candidates
            for item in candidate.evidence.items
            if item.evidence_role == EvidenceRole.SUPPORTING
        ]
        independent = [item for _, item in self._independent_evidence(candidates)]
        families = sorted({item.source_family for item in independent}, key=lambda family: family.value)
        live = any(item.source_family == SourceFamily.LIVE_OBSERVATION for item in independent)
        if live:
            status = CorroborationStatus.LIVE_CORROBORATED
        elif len(families) >= 2:
            status = CorroborationStatus.MULTI_FAMILY
        elif len(independent) >= 2:
            status = CorroborationStatus.MULTI_SOURCE
        elif independent:
            status = CorroborationStatus.SINGLE_SOURCE
        else:
            status = CorroborationStatus.UNCORROBORATED
        summary = CorroborationSummary(
            proposition_key=proposition_key,
            raw_source_count=len(evidence),
            independent_source_count=len(independent),
            independent_source_family_count=len(families),
            source_families=families,
            duplicate_source_count=max(0, len(evidence) - len(independent)),
            status=status,
        )
        snapshot_id, created = self.repository.save_corroboration(summary, tenant_id)
        if created:
            self.repository.append_event(
                "corroboration_updated",
                tenant_id=tenant_id,
                subject_ref=proposition_key,
                dimensions={"knowledge_kind": candidates[0].kind.value if candidates else ""},
                payload=summary.model_dump(mode="json"),
            )
        return summary, snapshot_id

    def reviewed_candidates(self, tenant_id: str, proposition_key: str) -> list[KnowledgeCandidate]:
        candidates = self.repository.candidates_for_proposition(
            tenant_id,
            proposition_key,
            review_states={ReviewState.APPROVED.value, ReviewState.TRUSTED.value},
            lifecycle_status=LifecycleStatus.ACTIVE.value,
            entity_resolution_status=EntityResolutionStatus.RESOLVED.value,
            limit=self.candidate_limit + 1,
        )
        if len(candidates) > self.candidate_limit:
            raise CandidateEvaluationConflictError(
                "corroboration candidate limit exceeded; consolidate proposition support before evaluation"
            )
        return candidates

    def contributing_candidates(self, tenant_id: str, proposition_key: str) -> list[KnowledgeCandidate]:
        candidates = self.reviewed_candidates(tenant_id, proposition_key)
        contributing_ids = {candidate.id for candidate, _ in self._independent_evidence(candidates)}
        return [candidate for candidate in candidates if candidate.id in contributing_ids]

    @staticmethod
    def _independent_evidence(
        candidates: list[KnowledgeCandidate],
    ) -> list[tuple[KnowledgeCandidate, KnowledgeEvidenceReference]]:
        lineage_groups: dict[str, tuple[KnowledgeCandidate, KnowledgeEvidenceReference]] = {}
        unknown_lineage_seen = False
        for candidate in candidates:
            for item in candidate.evidence.items:
                if item.evidence_role != EvidenceRole.SUPPORTING or item.lineage_kind in {
                    LineageKind.COPIED_FROM,
                    LineageKind.GENERATED_FROM,
                    LineageKind.SAME_VENDOR_EXPORT,
                    LineageKind.SAME_SOURCE_REVISION,
                }:
                    continue
                if item.lineage_kind == LineageKind.UNKNOWN:
                    if unknown_lineage_seen:
                        continue
                    unknown_lineage_seen = True
                    lineage_groups["unknown_lineage"] = (candidate, item)
                    continue
                group = item.lineage_group or item.evidence_ref
                lineage_groups.setdefault(group, (candidate, item))
        return list(lineage_groups.values())


class ConflictDetectionService:
    def __init__(self, repository: KnowledgeRepository, *, comparison_limit: int = 1_000):
        if comparison_limit < 1:
            raise ValueError("comparison_limit must be positive")
        self.repository = repository
        self.comparison_limit = comparison_limit

    def analyze(
        self,
        tenant_id: str,
        proposition_key: str,
        *,
        candidate_id: str = "",
    ) -> list[KnowledgeConflict]:
        current_rows = self.repository.list_propositions(
            tenant_id,
            proposition_key=proposition_key,
            limit=1,
        )
        if not current_rows:
            return []
        current = current_rows[0]
        if current["kind"] == KnowledgeKind.SIGNAL_MAPPING.value:
            return []
        if current["kind"] == KnowledgeKind.DEPENDENCY.value:
            if current["predicate"] not in {"depends_on", "does_not_depend_on"}:
                return []
            predicates = {"depends_on", "does_not_depend_on"}
        elif current["kind"] == KnowledgeKind.OWNERSHIP.value:
            predicates = {current["predicate"]}
        else:
            return []
        rows = self.repository.list_propositions(
            tenant_id,
            kind=current["kind"],
            subject_ref=current["subject_ref"],
            predicates=predicates,
            limit=self.comparison_limit + 1,
        )
        if len(rows) > self.comparison_limit:
            raise CandidateEvaluationConflictError(
                "conflict comparison limit exceeded; narrow the proposition scope before evaluation"
            )
        existing_conflicts = {
            item.id: item
            for item in self.repository.list_conflicts(
                tenant_id,
                proposition_key=proposition_key,
                limit=self.comparison_limit + 1,
            )
        }
        if len(existing_conflicts) > self.comparison_limit:
            raise CandidateEvaluationConflictError(
                "existing conflict limit exceeded; resolve conflicts before evaluating this proposition"
            )
        current_item = self.repository.find_knowledge_by_proposition(tenant_id, proposition_key)
        evaluated_candidate = self.repository.get_candidate(candidate_id, tenant_id) if candidate_id else None
        conflicts = []
        for other in rows:
            if other["proposition_key"] == proposition_key:
                continue
            if current["kind"] != other["kind"]:
                continue
            predicates = {current["predicate"], other["predicate"]}
            directly_negated = predicates == {"depends_on", "does_not_depend_on"}
            if current["predicate"] != other["predicate"] and not directly_negated:
                continue
            if directly_negated and (
                current["object_ref"] != other["object_ref"] or current["concept_ref"] != other["concept_ref"]
            ):
                continue
            if not directly_negated and current["kind"] == KnowledgeKind.DEPENDENCY.value:
                continue
            if not directly_negated and current["kind"] not in {
                KnowledgeKind.OWNERSHIP.value,
                KnowledgeKind.SIGNAL_MAPPING.value,
            }:
                continue
            if (
                not directly_negated
                and current["object_ref"] == other["object_ref"]
                and current["concept_ref"] == other["concept_ref"]
            ):
                continue
            left_scope = KnowledgeScope.model_validate_json(current["scope_json"])
            right_scope = KnowledgeScope.model_validate_json(other["scope_json"])
            scope_compatible, scope_reason = self._scopes_overlap(left_scope, right_scope)
            temporal_compatible = left_scope.validity_overlaps(right_scope)
            kind = (
                ConflictKind.DIRECT_NEGATION
                if directly_negated
                else self._kind_for(current["kind"], current["object_ref"], other["object_ref"])
            )
            resolution = (
                ConflictResolutionStatus.UNRESOLVED if scope_compatible else ConflictResolutionStatus.RESOLVED_BY_SCOPE
            )
            ordered = sorted([proposition_key, other["proposition_key"]])
            conflict = KnowledgeConflict(
                id=f"conflict_{stable_fingerprint(ordered).split(':', 1)[1][:20]}",
                tenant_id=tenant_id,
                conflict_kind=kind,
                left_proposition_ref=ordered[0],
                right_proposition_ref=ordered[1],
                resolution_status=resolution,
                scope_analysis={"compatible": scope_compatible, "reason_code": scope_reason},
                temporal_analysis={
                    "compatible": temporal_compatible,
                    "reason_code": None if temporal_compatible else "temporal_difference",
                },
                severity="high" if resolution == ConflictResolutionStatus.UNRESOLVED else "low",
                resolution_reason="" if scope_compatible else scope_reason,
            )
            existing = existing_conflicts.get(conflict.id)
            reviewed_support_for_superseded = bool(
                existing
                and existing.resolution_reason == "approved_human_correction"
                and current_item
                and current_item.status == LifecycleStatus.SUPERSEDED
                and evaluated_candidate
                and evaluated_candidate.state.review_state in {ReviewState.APPROVED, ReviewState.TRUSTED}
            )
            reopened = bool(
                existing
                and existing.resolution_status
                in {ConflictResolutionStatus.RESOLVED_BY_REVIEW, ConflictResolutionStatus.RESOLVED_BY_TIME}
                and (
                    existing.resolution_reason
                    in {
                        "counter_proposition_rejected",
                        "counter_proposition_stale",
                        "counter_proposition_lacks_independent_support",
                    }
                    or reviewed_support_for_superseded
                )
                and conflict.resolution_status == ConflictResolutionStatus.UNRESOLVED
            )
            if existing and not reopened:
                conflict = existing
            if existing is None or reopened:
                self.repository.save_conflict(conflict)
                if reopened and reviewed_support_for_superseded:
                    event_reason = "new_support_for_superseded_proposition"
                elif reopened and existing and existing.resolution_reason == "counter_proposition_stale":
                    event_reason = "source_reactivated"
                elif (
                    reopened
                    and existing
                    and existing.resolution_reason == "counter_proposition_lacks_independent_support"
                ):
                    event_reason = "new_independent_support"
                elif reopened:
                    event_reason = "new_candidate_after_rejection"
                else:
                    event_reason = kind.value
                self.repository.append_event(
                    "conflict_reopened" if reopened else "conflict_created",
                    tenant_id=tenant_id,
                    subject_ref=conflict.id,
                    dimensions={
                        "knowledge_kind": current["kind"],
                        "reason_code": event_reason,
                    },
                    payload=conflict.model_dump(mode="json"),
                )
            conflicts.append(conflict)
        return conflicts

    @staticmethod
    def _kind_for(kind: str, left_object: str, right_object: str) -> ConflictKind:
        if left_object.startswith("not:") or right_object.startswith("not:"):
            return ConflictKind.DIRECT_NEGATION
        if kind == KnowledgeKind.OWNERSHIP.value:
            return ConflictKind.COMPETING_OWNER
        if kind == KnowledgeKind.DEPENDENCY.value:
            return ConflictKind.COMPETING_DEPENDENCY
        return ConflictKind.COMPETING_SIGNAL_MAPPING

    @staticmethod
    def _scopes_overlap(left: KnowledgeScope, right: KnowledgeScope) -> tuple[bool, str]:
        if left.tenant_id != right.tenant_id:
            return False, "tenant_specific_difference"
        for field_name, reason in (
            ("environment_refs", "environment_specific_difference"),
            ("region_refs", "region_specific_difference"),
            ("cluster_refs", "cluster_specific_difference"),
            ("namespace_refs", "namespace_specific_difference"),
            ("service_refs", "service_specific_difference"),
            ("archetype_refs", "archetype_specific_difference"),
        ):
            left_values = set(getattr(left, field_name))
            right_values = set(getattr(right, field_name))
            if left_values and right_values and left_values.isdisjoint(right_values):
                return False, reason
        if not version_scopes_overlap(left.version_constraints, right.version_constraints):
            return False, "version_specific_difference"
        if not left.validity_overlaps(right):
            return False, "temporal_difference"
        return True, "scopes_overlap"
