"""Scope-aware entity resolution with safe ambiguity handling."""

from __future__ import annotations

from difflib import SequenceMatcher

from tacit.knowledge.enums import EntityBindingMethod, EntityKind, EntityResolutionStatus, EntityStatus
from tacit.knowledge.models import EntityBinding, EntityResolutionResult, KnowledgeScope
from tacit.knowledge.normalization import normalize_entity
from tacit.knowledge.repository import KnowledgeRepository


class EntityResolutionService:
    def __init__(self, repository: KnowledgeRepository):
        self.repository = repository

    def resolve(
        self,
        raw_value: str,
        expected_kind: EntityKind | None,
        scope: KnowledgeScope,
        provenance_refs: list[str],
        *,
        candidate_id: str = "",
    ) -> EntityResolutionResult:
        normalized = normalize_entity(raw_value)
        tenant_id = scope.tenant_id
        if not normalized:
            return self._record(
                EntityResolutionResult(
                    status=EntityResolutionStatus.UNRESOLVED,
                    raw_value=raw_value,
                    reason_codes=["empty_entity_mention"],
                ),
                scope,
                expected_kind,
                candidate_id,
            )

        if normalized.startswith(("concept:", "signal:")) and expected_kind is None:
            return self._record(
                EntityResolutionResult(
                    status=EntityResolutionStatus.RESOLVED,
                    raw_value=raw_value,
                    selected_entity_ref=normalized,
                    candidate_bindings=[
                        EntityBinding(
                            entity_ref=normalized,
                            method=EntityBindingMethod.DETERMINISTIC_NORMALIZATION,
                            provenance_refs=provenance_refs,
                        )
                    ],
                    reason_codes=["intentionally_raw_concept"],
                ),
                scope,
                expected_kind,
                candidate_id,
            )
        if normalized.startswith(("concept:", "signal:")):
            return self._record(
                EntityResolutionResult(
                    status=EntityResolutionStatus.UNRESOLVED,
                    raw_value=raw_value,
                    reason_codes=["raw_concept_does_not_match_expected_entity_kind"],
                ),
                scope,
                expected_kind,
                candidate_id,
            )

        direct = self.repository.get_entity(normalized, tenant_id)
        if (
            direct
            and direct.status == EntityStatus.ACTIVE
            and self._kind_matches(direct.kind, expected_kind)
            and direct.scope.applies_to(scope)
        ):
            return self._resolved(
                raw_value,
                direct.id,
                EntityBindingMethod.EXACT_ID,
                provenance_refs,
                scope,
                expected_kind,
                candidate_id,
            )

        expected_kind_filter = (
            expected_kind.value if expected_kind is not None and expected_kind != EntityKind.UNKNOWN else None
        )
        named = [
            entity
            for entity in self.repository.find_entities(
                tenant_id,
                normalized,
                expected_kind_filter,
            )
            if entity.scope.applies_to(scope)
        ]
        aliases = [
            alias
            for alias in self.repository.find_aliases(tenant_id, normalized)
            if alias.scope.applies_to(scope)
            and (entity := self.repository.get_entity(alias.entity_ref, tenant_id)) is not None
            and entity.status == EntityStatus.ACTIVE
            and self._kind_matches(entity.kind, expected_kind)
        ]
        refs = {entity.id: EntityBindingMethod.EXACT_NAME for entity in named}
        refs.update({alias.entity_ref: EntityBindingMethod.EXACT_ALIAS for alias in aliases})
        if len(refs) == 1:
            entity_ref, method = next(iter(refs.items()))
            return self._resolved(raw_value, entity_ref, method, provenance_refs, scope, expected_kind, candidate_id)
        if refs:
            return self._record(
                EntityResolutionResult(
                    status=EntityResolutionStatus.AMBIGUOUS,
                    raw_value=raw_value,
                    candidate_bindings=[
                        EntityBinding(entity_ref=ref, method=method, provenance_refs=provenance_refs)
                        for ref, method in sorted(refs.items())
                    ],
                    reason_codes=["multiple_scoped_matches"],
                ),
                scope,
                expected_kind,
                candidate_id,
            )

        fuzzy = []
        for entity in self.repository.list_entities(tenant_id):
            if (
                entity.status != EntityStatus.ACTIVE
                or not self._kind_matches(entity.kind, expected_kind)
                or not entity.scope.applies_to(scope)
            ):
                continue
            score = SequenceMatcher(None, normalized, normalize_entity(entity.canonical_name)).ratio()
            if score >= 0.78:
                fuzzy.append(
                    EntityBinding(
                        entity_ref=entity.id,
                        method=EntityBindingMethod.FUZZY_CANDIDATE,
                        confidence=f"candidate:{score:.3f}",
                        provenance_refs=provenance_refs,
                    )
                )
        return self._record(
            EntityResolutionResult(
                status=EntityResolutionStatus.AMBIGUOUS if len(fuzzy) > 1 else EntityResolutionStatus.UNRESOLVED,
                raw_value=raw_value,
                candidate_bindings=fuzzy,
                reason_codes=["fuzzy_candidates_require_confirmation"] if fuzzy else ["entity_not_found"],
            ),
            scope,
            expected_kind,
            candidate_id,
        )

    @staticmethod
    def _kind_matches(actual: EntityKind, expected: EntityKind | None) -> bool:
        return expected is None or expected == EntityKind.UNKNOWN or actual == expected

    def _resolved(self, raw, ref, method, provenance, scope, expected, candidate_id):
        return self._record(
            EntityResolutionResult(
                status=EntityResolutionStatus.RESOLVED,
                raw_value=raw,
                selected_entity_ref=ref,
                candidate_bindings=[EntityBinding(entity_ref=ref, method=method, provenance_refs=provenance)],
                reason_codes=[method.value],
            ),
            scope,
            expected,
            candidate_id,
        )

    def _record(self, result, scope, expected, candidate_id):
        self.repository.record_resolution_attempt(
            result,
            scope.model_dump_json(),
            tenant_id=scope.tenant_id,
            candidate_id=candidate_id,
            expected_kind=expected.value if expected else "",
        )
        event = {
            EntityResolutionStatus.RESOLVED: "entity_resolved",
            EntityResolutionStatus.AMBIGUOUS: "entity_ambiguous",
            EntityResolutionStatus.UNRESOLVED: "entity_unresolved",
            EntityResolutionStatus.REJECTED: "entity_unresolved",
        }[result.status]
        self.repository.append_event(
            event,
            tenant_id=scope.tenant_id,
            subject_ref=result.selected_entity_ref or candidate_id or "unresolved_entity",
            dimensions={"reason_code": result.reason_codes[0] if result.reason_codes else ""},
            payload={
                "status": result.status.value,
                "selected_entity_ref": result.selected_entity_ref,
                "candidate_count": len(result.candidate_bindings),
                "reason_codes": result.reason_codes,
            },
        )
        return result
