"""Independent corroboration and deterministic conflict analysis."""

from __future__ import annotations

from tacit.knowledge.enums import (
    ConflictKind,
    ConflictResolutionStatus,
    CorroborationStatus,
    KnowledgeKind,
    SourceFamily,
)
from tacit.knowledge.models import (
    CorroborationSummary,
    KnowledgeConflict,
    KnowledgeEvidenceReference,
    KnowledgeScope,
)
from tacit.knowledge.normalization import stable_fingerprint
from tacit.knowledge.repository import KnowledgeRepository


class CorroborationService:
    def __init__(self, repository: KnowledgeRepository):
        self.repository = repository

    def analyze(self, tenant_id: str, proposition_key: str) -> tuple[CorroborationSummary, str]:
        candidates = self.repository.candidates_for_proposition(tenant_id, proposition_key)
        evidence = [item for candidate in candidates for item in candidate.evidence.items]
        lineage_groups: dict[str, KnowledgeEvidenceReference] = {}
        for item in evidence:
            group = item.lineage_group or item.evidence_ref
            lineage_groups.setdefault(group, item)
        independent = list(lineage_groups.values())
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
        snapshot_id = self.repository.save_corroboration(summary, tenant_id)
        for candidate in candidates:
            self.repository.save_candidate(candidate.model_copy(update={"corroboration": summary}))
        self.repository.append_event(
            "corroboration_updated",
            tenant_id=tenant_id,
            subject_ref=proposition_key,
            dimensions={"knowledge_kind": candidates[0].kind.value if candidates else ""},
            payload=summary.model_dump(mode="json"),
        )
        return summary, snapshot_id


class ConflictDetectionService:
    def __init__(self, repository: KnowledgeRepository):
        self.repository = repository

    def analyze(self, tenant_id: str, proposition_key: str) -> list[KnowledgeConflict]:
        rows = self.repository.list_propositions(tenant_id)
        existing_conflicts = {item.id: item for item in self.repository.list_conflicts(tenant_id)}
        current = next((row for row in rows if row["proposition_key"] == proposition_key), None)
        if current is None:
            return []
        conflicts = []
        for other in rows:
            if other["proposition_key"] == proposition_key:
                continue
            if current["kind"] != other["kind"] or current["subject_ref"] != other["subject_ref"]:
                continue
            predicates = {current["predicate"], other["predicate"]}
            directly_negated = predicates == {"depends_on", "does_not_depend_on"}
            if current["predicate"] != other["predicate"] and not directly_negated:
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
                temporal_analysis={"compatible": True, "reason_code": None},
                severity="high" if resolution == ConflictResolutionStatus.UNRESOLVED else "low",
                resolution_reason="" if scope_compatible else scope_reason,
            )
            existing = existing_conflicts.get(conflict.id)
            if existing and existing.resolution_status in {
                ConflictResolutionStatus.RESOLVED_BY_AUTHORITY,
                ConflictResolutionStatus.RESOLVED_BY_REVIEW,
                ConflictResolutionStatus.SUPERSEDED,
            }:
                conflict = existing
            self.repository.save_conflict(conflict)
            self.repository.append_event(
                "conflict_created",
                tenant_id=tenant_id,
                subject_ref=conflict.id,
                dimensions={"knowledge_kind": current["kind"], "reason_code": kind.value},
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
            ("version_constraints", "version_specific_difference"),
        ):
            left_values = set(getattr(left, field_name))
            right_values = set(getattr(right, field_name))
            if left_values and right_values and left_values.isdisjoint(right_values):
                return False, reason
        if left.valid_until and right.valid_from and left.valid_until <= right.valid_from:
            return False, "temporal_difference"
        if right.valid_until and left.valid_from and right.valid_until <= left.valid_from:
            return False, "temporal_difference"
        return True, "scopes_overlap"
