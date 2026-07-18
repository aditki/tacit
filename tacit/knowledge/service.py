"""Governed Operational Knowledge lifecycle orchestration."""

from __future__ import annotations

import re
from typing import Any

from tacit.knowledge.corroboration import ConflictDetectionService, CorroborationService
from tacit.knowledge.entities import EntityResolutionService
from tacit.knowledge.enums import (
    ConflictResolutionStatus,
    CorrectionType,
    EntityKind,
    EntityResolutionStatus,
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
from tacit.knowledge.models import (
    CandidatePolicyState,
    Entity,
    EntityAlias,
    EntityResolutionResult,
    KnowledgeCandidate,
    KnowledgeCorrection,
    KnowledgeEvidence,
    KnowledgeEvidenceReference,
    KnowledgeImpact,
    KnowledgeProposition,
    KnowledgeRevision,
    KnowledgeScope,
    KnowledgeSnapshot,
    KnowledgeSnapshotItem,
    KnowledgeUsage,
    MigrationProvenance,
    PromotionContext,
    PromotionDecision,
    utc_now,
)
from tacit.knowledge.normalization import PropositionNormalizer, canonical_scope_payload, stable_fingerprint
from tacit.knowledge.policies import default_policies
from tacit.knowledge.repository import KnowledgeRepository, get_knowledge_repository

PROMPT_INJECTION_RE = re.compile(
    r"\b(ignore (?:all |the )?(?:previous|system) instructions|system prompt|developer message|"
    r"mark (?:this|me) trusted|promote (?:this|me)|override (?:policy|ranking)|reveal secrets?)\b",
    re.I,
)


def _id(prefix: str, value: Any) -> str:
    return f"{prefix}_{stable_fingerprint(value).split(':', 1)[1][:20]}"


def _source_family(value: str) -> SourceFamily:
    aliases = {
        "documentation": SourceFamily.RUNBOOK,
        "incident_history": SourceFamily.INCIDENT,
        "catalog": SourceFamily.SERVICE_CATALOG,
        "human": SourceFamily.HUMAN_CORRECTION,
        "telemetry": SourceFamily.LIVE_OBSERVATION,
        "dashboard_ingest": SourceFamily.DASHBOARD,
        "alert_ingest": SourceFamily.ALERT,
    }
    try:
        return SourceFamily(value)
    except ValueError:
        return aliases.get(value, SourceFamily.UNKNOWN)


class KnowledgeService:
    def __init__(self, repository: KnowledgeRepository | None = None):
        self.repository = repository or get_knowledge_repository()
        self.entity_resolution = EntityResolutionService(self.repository)
        self.normalizer = PropositionNormalizer()
        self.corroboration = CorroborationService(self.repository)
        self.conflicts = ConflictDetectionService(self.repository)
        self.policies = default_policies()

    def register_entity(self, entity: Entity) -> Entity:
        if entity.scope.tenant_id != entity.tenant_id:
            entity = entity.model_copy(
                update={"scope": entity.scope.model_copy(update={"tenant_id": entity.tenant_id})}
            )
        saved = self.repository.save_entity(entity)
        self.repository.append_event(
            "entity_resolved",
            tenant_id=entity.tenant_id,
            subject_ref=entity.id,
            dimensions={"reason_code": "entity_registered"},
            payload=entity.model_dump(mode="json", exclude={"provenance_refs"}),
        )
        return saved

    def register_alias(self, alias: EntityAlias) -> EntityAlias:
        entity = self.repository.get_entity(alias.entity_ref, alias.tenant_id)
        if entity is None:
            raise ValueError("alias target entity does not exist in the tenant")
        if alias.scope.tenant_id != alias.tenant_id:
            alias = alias.model_copy(update={"scope": alias.scope.model_copy(update={"tenant_id": alias.tenant_id})})
        return self.repository.save_alias(alias)

    def create_candidate(
        self,
        *,
        kind: KnowledgeKind | str,
        payload_ref: str,
        typed_payload: dict[str, Any],
        proposition: KnowledgeProposition | dict[str, Any],
        scope: KnowledgeScope | None = None,
        evidence: list[KnowledgeEvidenceReference] | None = None,
        provenance_refs: list[str],
        tenant_id: str = "default",
        candidate_id: str | None = None,
        migration_provenance: MigrationProvenance | None = None,
    ) -> KnowledgeCandidate:
        knowledge_kind = KnowledgeKind(kind)
        scope = scope or KnowledgeScope(tenant_id=tenant_id)
        if scope.tenant_id != tenant_id:
            raise ValueError("candidate scope cannot cross tenants")
        raw = proposition if isinstance(proposition, dict) else proposition.model_dump(mode="python")
        subject_raw = str(raw.get("subject_ref", ""))
        object_raw = str(raw.get("object_ref", ""))
        candidate_id = candidate_id or _id(
            "kc",
            {
                "tenant": tenant_id,
                "payload_ref": payload_ref,
                "proposition": raw,
                "scope": scope.model_dump(mode="json"),
            },
        )
        subject_result = self.entity_resolution.resolve(
            subject_raw,
            self._subject_kind(knowledge_kind),
            scope,
            provenance_refs,
            candidate_id=candidate_id,
        )
        object_result = None
        if object_raw:
            object_result = self.entity_resolution.resolve(
                object_raw,
                self._object_kind(knowledge_kind),
                scope,
                provenance_refs,
                candidate_id=candidate_id,
            )
        resolution = self._combined_resolution(subject_result, object_result)
        normalized = self.normalizer.normalize(
            kind=knowledge_kind,
            subject_ref=subject_result.selected_entity_ref or subject_raw,
            predicate=raw.get("predicate", self._default_predicate(knowledge_kind)),
            object_ref=(object_result.selected_entity_ref if object_result else "") or object_raw,
            concept_ref=str(raw.get("concept_ref", "")),
            source_wording=str(raw.get("source_wording", "")),
            uncertainty=str(raw.get("uncertainty", "unknown")),
            scope=scope,
        )
        evidence_model = KnowledgeEvidence(items=evidence or [])
        security_text = " ".join(
            [normalized.source_wording, *[str(value) for value in typed_payload.values() if isinstance(value, str)]]
        )
        security_flags = ["possible_prompt_injection"] if PROMPT_INJECTION_RE.search(security_text) else []
        now = utc_now()
        candidate = KnowledgeCandidate(
            id=candidate_id,
            tenant_id=tenant_id,
            kind=knowledge_kind,
            payload_ref=payload_ref,
            typed_payload=typed_payload,
            proposition=normalized,
            scope=scope,
            entity_resolution=resolution,
            evidence=evidence_model,
            provenance_refs=provenance_refs,
            security_flags=security_flags,
            migration_provenance=migration_provenance,
            created_at=now,
            updated_at=now,
        )
        existing = self.repository.get_candidate(candidate.id, tenant_id)
        if existing is not None:
            proposition_changed = existing.proposition.proposition_key != candidate.proposition.proposition_key
            if proposition_changed and not self._is_entity_resolution_repair(existing, candidate):
                raise ValueError("candidate identity cannot be reused for a different proposition")
            candidate = candidate.model_copy(
                update={
                    "state": existing.state,
                    "corroboration": existing.corroboration,
                    "policy": existing.policy,
                    "security_flags": sorted(set(existing.security_flags + candidate.security_flags)),
                    "created_at": existing.created_at,
                }
            )
        self.repository.save_candidate(candidate)
        first_evidence = evidence_model.items[0] if evidence_model.items else None
        self.repository.save_proposition(
            candidate,
            lineage_group=(first_evidence.lineage_group if first_evidence else payload_ref),
            independence_class=(first_evidence.lineage_kind.value if first_evidence else LineageKind.UNKNOWN.value),
        )
        self.repository.append_event(
            "candidate_created",
            tenant_id=tenant_id,
            subject_ref=candidate.id,
            dimensions={
                "knowledge_kind": knowledge_kind.value,
                "review_state": candidate.state.review_state.value,
                "lifecycle_status": candidate.state.lifecycle_status.value,
                "eligibility": candidate.state.eligibility.value,
                "source_family": first_evidence.source_family.value if first_evidence else "",
                "reason_code": security_flags[0] if security_flags else "candidate_extracted",
            },
            payload={
                "candidate_id": candidate.id,
                "proposition_key": normalized.proposition_key,
                "security_flags": security_flags,
            },
        )
        self.repository.append_event(
            "proposition_normalized",
            tenant_id=tenant_id,
            subject_ref=normalized.proposition_key,
            dimensions={"knowledge_kind": knowledge_kind.value},
            payload={"candidate_id": candidate.id},
        )
        return candidate

    def review_candidate(
        self,
        candidate_id: str,
        *,
        approved: bool,
        reviewer: str,
        tenant_id: str = "default",
        trust: bool = False,
        can_trust: bool = False,
    ) -> KnowledgeCandidate:
        candidate = self._require_candidate(candidate_id, tenant_id)
        if trust and not can_trust:
            raise PermissionError("knowledge.trust permission is required")
        review_state = ReviewState.TRUSTED if trust else ReviewState.APPROVED if approved else ReviewState.REJECTED
        state = candidate.state.model_copy(
            update={
                "review_state": review_state,
                "eligibility": KnowledgeEligibility.INELIGIBLE,
            }
        )
        updated = candidate.model_copy(update={"state": state, "updated_at": utc_now()})
        expected_states = (
            {ReviewState.CANDIDATE.value, ReviewState.APPROVED.value}
            if review_state == ReviewState.TRUSTED
            else {ReviewState.CANDIDATE.value}
        )
        self.repository.transition_candidate_review(updated, expected_states=expected_states)
        if review_state == ReviewState.REJECTED:
            decision = self._state_decision(updated, PromotionDecisionType.REJECT, "rejected_by_review")
            self.repository.save_promotion_decision(decision, tenant_id)
            self._resolve_conflicts_for_rejected_proposition(updated, reviewer)
        self.repository.append_event(
            "correction_reviewed" if candidate.kind == KnowledgeKind.ARTIFACT_QUALITY else "promotion_evaluated",
            tenant_id=tenant_id,
            subject_ref=candidate_id,
            dimensions={
                "knowledge_kind": candidate.kind.value,
                "review_state": review_state.value,
                "eligibility": state.eligibility.value,
                "reason_code": "reviewed_by_human",
            },
            payload={"reviewer": reviewer},
        )
        return updated

    def evaluate_candidate(
        self,
        candidate_id: str,
        *,
        tenant_id: str = "default",
        authoritative_source: bool = False,
        live_verified: bool = False,
        ignored_conflict_ids: set[str] | None = None,
    ) -> tuple[PromotionDecision, KnowledgeRevision | None]:
        candidate = self._require_candidate(candidate_id, tenant_id)
        summary, corroboration_ref = self.corroboration.analyze(tenant_id, candidate.proposition.proposition_key)
        conflicts = self.conflicts.analyze(tenant_id, candidate.proposition.proposition_key)
        ignored_conflict_ids = ignored_conflict_ids or set()
        unresolved = [
            conflict
            for conflict in conflicts
            if conflict.resolution_status.value == "unresolved" and conflict.id not in ignored_conflict_ids
        ]
        context = PromotionContext(
            corroboration=summary,
            unresolved_conflict_count=len(unresolved),
            authoritative_source=authoritative_source,
            live_verified=live_verified,
        )
        policy = self.policies.get(candidate.kind)
        if policy is None:
            raise ValueError(f"Unknown knowledge kind: {candidate.kind}")
        decision = policy.evaluate(candidate, context)
        self.repository.save_promotion_decision(decision, tenant_id)
        policy_state = CandidatePolicyState(
            promotion_policy_ref=policy.policy_id,
            last_evaluated_at=decision.evaluated_at,
            eligibility_reason_codes=decision.reason_codes,
        )
        state = candidate.state.model_copy(update={"eligibility": decision.resulting_eligibility})
        candidate = candidate.model_copy(
            update={"corroboration": summary, "policy": policy_state, "state": state, "updated_at": utc_now()}
        )
        self.repository.save_candidate(candidate)
        self.repository.append_event(
            "promotion_evaluated",
            tenant_id=tenant_id,
            subject_ref=candidate.id,
            dimensions={
                "knowledge_kind": candidate.kind.value,
                "policy_version": policy.version,
                "review_state": candidate.state.review_state.value,
                "lifecycle_status": candidate.state.lifecycle_status.value,
                "eligibility": decision.resulting_eligibility.value,
                "reason_code": decision.reason_codes[0] if decision.reason_codes else "eligible",
            },
            payload=decision.model_dump(mode="json"),
        )
        if decision.decision != PromotionDecisionType.PROMOTE:
            return decision, None
        contributors = self.corroboration.contributing_candidates(
            tenant_id,
            candidate.proposition.proposition_key,
        )
        if candidate.id not in {item.id for item in contributors}:
            contributors.append(candidate)
        contributor_refs = sorted({item.id for item in contributors})
        provenance_refs = sorted(
            {
                provenance_ref
                for item in contributors
                for provenance_ref in [
                    *item.provenance_refs,
                    *[ref for evidence in item.evidence.items for ref in evidence.provenance_refs],
                ]
            }
        )
        existing = self.repository.find_knowledge_by_proposition(tenant_id, candidate.proposition.proposition_key)
        knowledge_id = existing.id if existing else _id("knowledge", [tenant_id, candidate.proposition.proposition_key])
        revision_number = existing.current_revision + 1 if existing else 1
        semantic = stable_fingerprint(
            {
                "proposition": candidate.proposition.model_dump(mode="json"),
                "scope": canonical_scope_payload(candidate.scope),
                "state": state.model_dump(mode="json"),
                "policy": [policy.policy_id, policy.version],
                "conflicts": sorted(conflict.id for conflict in conflicts),
                "contributors": contributor_refs,
                "provenance": provenance_refs,
            }
        )
        revision = KnowledgeRevision(
            knowledge_id=knowledge_id,
            tenant_id=tenant_id,
            revision=revision_number,
            parent_revision=revision_number - 1 or None,
            proposition=candidate.proposition,
            scope=candidate.scope,
            state=state,
            corroboration_snapshot_ref=corroboration_ref,
            conflict_refs=sorted(conflict.id for conflict in conflicts),
            policy_id=policy.policy_id,
            policy_version=policy.version,
            decision_ref=decision.decision_id,
            promoted_from_candidate_refs=contributor_refs,
            provenance_refs=provenance_refs,
            revision_reason="promoted" if revision_number == 1 else "corroborated",
            semantic_fingerprint=semantic,
        )
        self.repository.persist_revision(revision, candidate_id=candidate.id, decision_ref=decision.decision_id)
        self.repository.append_event(
            "knowledge_promoted" if revision_number == 1 else "knowledge_revised",
            tenant_id=tenant_id,
            subject_ref=knowledge_id,
            dimensions={
                "knowledge_kind": candidate.kind.value,
                "policy_version": policy.version,
                "review_state": state.review_state.value,
                "lifecycle_status": state.lifecycle_status.value,
                "eligibility": state.eligibility.value,
                "reason_code": revision.revision_reason,
            },
            payload={"revision": revision_number, "candidate_id": candidate.id},
        )
        return decision, revision

    def create_snapshot(
        self,
        scope: KnowledgeScope,
    ) -> tuple[KnowledgeSnapshot, list[KnowledgeUsage]]:
        selected: list[KnowledgeSnapshotItem] = []
        usage: list[KnowledgeUsage] = []
        for revision in self.repository.list_current_revisions(scope.tenant_id):
            disposition, reasons = self._disposition(revision, scope)
            applied = disposition == KnowledgeUsageDisposition.APPLIED
            if applied:
                selected.append(KnowledgeSnapshotItem(knowledge_ref=revision.knowledge_id, revision=revision.revision))
            usage.append(
                KnowledgeUsage(
                    tenant_id=scope.tenant_id,
                    knowledge_ref=revision.knowledge_id,
                    knowledge_revision=revision.revision,
                    disposition=disposition,
                    used_for=self._used_for(revision) if applied else [],
                    target_ref=revision.proposition.object_ref or revision.proposition.subject_ref,
                    score_delta=self._score_delta(revision) if applied else 0.0,
                    decision_ref=revision.decision_ref,
                    provenance_refs=revision.provenance_refs,
                    reason_codes=reasons,
                )
            )
        return self._save_snapshot(scope.tenant_id, selected), usage

    def snapshot_from_usage(self, tenant_id: str, usage: list[KnowledgeUsage]) -> KnowledgeSnapshot:
        """Persist the final applied set after live-evidence reconciliation."""
        selected = [
            KnowledgeSnapshotItem(knowledge_ref=item.knowledge_ref, revision=item.knowledge_revision)
            for item in usage
            if item.disposition == KnowledgeUsageDisposition.APPLIED
        ]
        return self._save_snapshot(tenant_id, selected)

    def _save_snapshot(
        self,
        tenant_id: str,
        selected: list[KnowledgeSnapshotItem],
    ) -> KnowledgeSnapshot:
        items = sorted(selected, key=lambda item: (item.knowledge_ref, item.revision))
        fingerprint = stable_fingerprint([item.model_dump(mode="json") for item in items])
        snapshot = KnowledgeSnapshot(
            id=_id("knowledge_snapshot", [tenant_id, fingerprint]),
            tenant_id=tenant_id,
            items=items,
            fingerprint=fingerprint,
        )
        return self.repository.save_snapshot(snapshot)

    def apply_to_ranking(self, ranking, usage: list[KnowledgeUsage]):
        """Apply bounded contextual lift without converting context into telemetry."""
        from tacit.models.schemas import CulpritCandidate

        candidates = list(ranking.candidates)
        by_ref = {f"{candidate.suspect_type}:{candidate.suspect}": candidate for candidate in candidates}
        for item in usage:
            if item.disposition != KnowledgeUsageDisposition.APPLIED or item.score_delta <= 0:
                continue
            revision = self.repository.get_revision(
                item.knowledge_ref,
                item.knowledge_revision,
                tenant_id=item.tenant_id,
            )
            if (
                revision is None
                or revision.proposition.kind != KnowledgeKind.DEPENDENCY
                or revision.proposition.predicate == Predicate.DOES_NOT_DEPEND_ON
            ):
                continue
            candidate_ref = self._candidate_ref(revision.proposition.object_ref)
            reason = (
                f"Operational Knowledge {revision.knowledge_id} revision {revision.revision} "
                "provides scoped dependency context."
            )
            existing = by_ref.get(candidate_ref)
            if existing is not None:
                updated = existing.model_copy(
                    update={
                        "score": min(1.0, existing.score + item.score_delta),
                        "contextual_reasons": [*existing.contextual_reasons, reason],
                    }
                )
                candidates[candidates.index(existing)] = updated
                by_ref[candidate_ref] = updated
            else:
                suspect_type, suspect = candidate_ref.split(":", 1)
                added = CulpritCandidate(
                    rank=len(candidates) + 1,
                    suspect=suspect,
                    suspect_type=suspect_type,
                    score=item.score_delta,
                    contextual_reasons=[reason],
                )
                candidates.append(added)
                by_ref[candidate_ref] = added
        candidates.sort(key=lambda candidate: (-candidate.score, candidate.suspect_type, candidate.suspect))
        candidates = [candidate.model_copy(update={"rank": index}) for index, candidate in enumerate(candidates, 1)]
        update: dict[str, Any] = {"candidates": candidates}
        if candidates and ranking.abstained:
            update.update({"abstained": False, "abstention_reason": ""})
        return ranking.model_copy(update=update)

    def reconcile_live_observations(self, usage: list[KnowledgeUsage], observations) -> list[KnowledgeUsage]:
        """Let exact negative runtime evidence veto matching contextual knowledge."""
        from tacit.models.schemas import EvidenceObservationOutcome

        negative_refs = {
            value.strip().casefold()
            for observation in observations
            if observation.outcome == EvidenceObservationOutcome.NEGATIVE_EVIDENCE
            for value in (observation.requirement_id, observation.resolution_metric)
            if value.strip()
        }
        if not negative_refs:
            return usage
        reconciled = []
        for item in usage:
            revision = self.repository.get_revision(
                item.knowledge_ref,
                item.knowledge_revision,
                tenant_id=item.tenant_id,
            )
            if revision is None or item.disposition != KnowledgeUsageDisposition.APPLIED:
                reconciled.append(item)
                continue
            proposition = revision.proposition
            refs = {
                value.casefold()
                for value in (
                    proposition.subject_ref,
                    proposition.object_ref,
                    proposition.concept_ref,
                    proposition.subject_ref.rsplit(":", 1)[-1],
                    proposition.object_ref.rsplit(":", 1)[-1],
                    proposition.concept_ref.rsplit(":", 1)[-1],
                )
                if value
            }
            if refs.isdisjoint(negative_refs):
                reconciled.append(item)
                continue
            reconciled.append(
                item.model_copy(
                    update={
                        "disposition": KnowledgeUsageDisposition.CONTRADICTED_BY_OBSERVATION,
                        "used_for": [],
                        "score_delta": 0.0,
                        "reason_codes": [*item.reason_codes, "exact_live_negative_evidence"],
                    }
                )
            )
        return reconciled

    def persist_usage(
        self,
        usage: list[KnowledgeUsage],
        *,
        investigation_id: str,
        investigation_revision: int,
    ) -> list[KnowledgeUsage]:
        persisted = []
        for item in usage:
            updated = item.model_copy(
                update={
                    "investigation_id": investigation_id,
                    "investigation_revision": investigation_revision,
                }
            )
            persisted.append(self.repository.save_usage(updated))
            event = (
                "knowledge_applied"
                if updated.disposition == KnowledgeUsageDisposition.APPLIED
                else (
                    "knowledge_rejected_by_scope"
                    if updated.disposition == KnowledgeUsageDisposition.REJECTED_BY_SCOPE
                    else (
                        "knowledge_contradicted_live"
                        if updated.disposition == KnowledgeUsageDisposition.CONTRADICTED_BY_OBSERVATION
                        else "knowledge_considered"
                    )
                )
            )
            self.repository.append_event(
                event,
                tenant_id=updated.tenant_id,
                subject_ref=updated.knowledge_ref,
                dimensions={"reason_code": updated.disposition.value},
                payload={
                    "investigation_id": investigation_id,
                    "investigation_revision": investigation_revision,
                    "knowledge_revision": updated.knowledge_revision,
                },
            )
        return persisted

    def create_correction(
        self,
        *,
        investigation_id: str,
        investigation_revision: int,
        correction_type: CorrectionType | str,
        proposed: dict[str, Any],
        scope: KnowledgeScope,
        explanation: str,
        created_by: str,
        target_ref: str = "",
        tenant_id: str = "default",
    ) -> tuple[KnowledgeCorrection, KnowledgeCandidate]:
        correction_type = CorrectionType(correction_type)
        correction_id = _id(
            "correction",
            [
                tenant_id,
                investigation_id,
                investigation_revision,
                correction_type.value,
                proposed,
                scope.model_dump(mode="json"),
                target_ref,
            ],
        )
        existing_correction = self.repository.get_correction(correction_id, tenant_id)
        if existing_correction is not None:
            return existing_correction, self._require_candidate(
                existing_correction.knowledge_candidate_ref,
                tenant_id,
            )
        original = {}
        if target_ref:
            current = self.repository.get_revision(target_ref, tenant_id=tenant_id)
            if current is None:
                raise ValueError("correction target does not exist in the tenant")
            original = current.proposition.model_dump(mode="json")
        kind = self._kind_for_correction(correction_type, proposed)
        evidence = KnowledgeEvidenceReference(
            evidence_ref=correction_id,
            evidence_role=EvidenceRole.CONTRADICTING if target_ref else EvidenceRole.SUPPORTING,
            source_family=SourceFamily.HUMAN_CORRECTION,
            lineage_group=correction_id,
            lineage_kind=LineageKind.INDEPENDENT,
            provenance_refs=[f"prov_{correction_id}"],
        )
        candidate = self.create_candidate(
            kind=kind,
            payload_ref=correction_id,
            typed_payload={"correction_type": correction_type.value, **proposed},
            proposition=proposed,
            scope=scope,
            evidence=[evidence],
            provenance_refs=[f"prov_{correction_id}"],
            tenant_id=tenant_id,
        )
        correction = KnowledgeCorrection(
            id=correction_id,
            tenant_id=tenant_id,
            investigation_id=investigation_id,
            investigation_revision=investigation_revision,
            correction_type=correction_type,
            target_ref=target_ref,
            original=original,
            proposed=proposed,
            scope=scope,
            explanation=explanation,
            created_by=created_by,
            knowledge_candidate_ref=candidate.id,
        )
        correction = self.repository.save_correction(correction)
        self.repository.append_event(
            "correction_created",
            tenant_id=tenant_id,
            subject_ref=correction.id,
            dimensions={"knowledge_kind": kind.value, "source_family": SourceFamily.HUMAN_CORRECTION.value},
            payload={"candidate_id": candidate.id, "investigation_id": investigation_id},
        )
        return correction, candidate

    def review_correction(
        self,
        correction_id: str,
        *,
        approved: bool,
        reviewer: str,
        tenant_id: str = "default",
        authoritative: bool = False,
    ) -> tuple[KnowledgeCorrection, KnowledgeRevision | None]:
        correction = self.repository.get_correction(correction_id, tenant_id)
        if correction is None:
            raise ValueError("knowledge correction not found")
        target = None
        if correction.target_ref:
            target = self.repository.get_revision(correction.target_ref, tenant_id=tenant_id)
            if target is None:
                raise ValueError("correction target knowledge item not found")
        candidate = self.review_candidate(
            correction.knowledge_candidate_ref,
            approved=approved,
            reviewer=reviewer,
            tenant_id=tenant_id,
        )
        correction = correction.model_copy(update={"review_state": candidate.state.review_state})
        self.repository.save_correction(correction)
        if not approved:
            return correction, None
        conflicts = self.conflicts.analyze(tenant_id, candidate.proposition.proposition_key)
        replaceable_conflicts = [
            conflict
            for conflict in conflicts
            if target is not None
            and conflict.resolution_status == ConflictResolutionStatus.UNRESOLVED
            and target.proposition.proposition_key in {conflict.left_proposition_ref, conflict.right_proposition_ref}
        ]
        _, revision = self.evaluate_candidate(
            candidate.id,
            tenant_id=tenant_id,
            authoritative_source=authoritative,
            ignored_conflict_ids={conflict.id for conflict in replaceable_conflicts},
        )
        superseded = False
        if revision and target is not None and replaceable_conflicts and correction.target_ref != revision.knowledge_id:
            self.supersede(correction.target_ref, candidate.id, tenant_id=tenant_id)
            superseded = True
        if superseded:
            for conflict in replaceable_conflicts:
                resolved = conflict.model_copy(
                    update={
                        "resolution_status": ConflictResolutionStatus.RESOLVED_BY_REVIEW,
                        "resolution_reason": "approved_human_correction",
                        "resolved_by": reviewer,
                        "resolved_at": utc_now(),
                    }
                )
                self.repository.save_conflict(resolved)
                self.repository.append_event(
                    "conflict_resolved",
                    tenant_id=tenant_id,
                    subject_ref=resolved.id,
                    dimensions={
                        "knowledge_kind": candidate.kind.value,
                        "reason_code": "approved_human_correction",
                    },
                    payload={"candidate_id": candidate.id},
                )
        return correction, revision

    def supersede(
        self,
        knowledge_id: str,
        replacement_candidate_id: str,
        *,
        tenant_id: str = "default",
    ) -> KnowledgeRevision:
        current = self.repository.get_revision(knowledge_id, tenant_id=tenant_id)
        candidate = self._require_candidate(replacement_candidate_id, tenant_id)
        if current is None:
            raise ValueError("knowledge item not found")
        decision = self._state_decision(candidate, PromotionDecisionType.SUPERSEDE, "superseded_by_correction")
        self.repository.save_promotion_decision(decision, tenant_id)
        state = current.state.model_copy(
            update={"lifecycle_status": LifecycleStatus.SUPERSEDED, "eligibility": KnowledgeEligibility.INELIGIBLE}
        )
        revision = current.model_copy(
            update={
                "revision": current.revision + 1,
                "parent_revision": current.revision,
                "state": state,
                "policy_id": decision.policy_id,
                "policy_version": decision.policy_version,
                "decision_ref": decision.decision_id,
                "promoted_from_candidate_refs": [candidate.id],
                "revision_reason": "superseded",
                "semantic_fingerprint": stable_fingerprint(
                    [current.semantic_fingerprint, "superseded", candidate.proposition.proposition_key]
                ),
                "created_at": utc_now(),
            }
        )
        self.repository.persist_revision(revision, candidate_id=candidate.id, decision_ref=decision.decision_id)
        self.repository.append_event(
            "knowledge_superseded",
            tenant_id=tenant_id,
            subject_ref=knowledge_id,
            dimensions={
                "knowledge_kind": current.proposition.kind.value,
                "lifecycle_status": LifecycleStatus.SUPERSEDED.value,
                "eligibility": KnowledgeEligibility.INELIGIBLE.value,
            },
            payload={"replacement_candidate_id": candidate.id, "revision": revision.revision},
        )
        return revision

    def impact(self, knowledge_id: str, tenant_id: str = "default") -> KnowledgeImpact:
        usage = self.repository.list_usage(tenant_id=tenant_id, knowledge_id=knowledge_id)
        seen = set()
        affected = []
        for item in usage:
            key = (item.investigation_id, item.investigation_revision)
            if key in seen:
                continue
            seen.add(key)
            affected.append({"investigation_id": key[0], "revision": key[1]})
        return KnowledgeImpact(knowledge_ref=knowledge_id, affected_investigations=affected)

    def explain(self, knowledge_id: str, tenant_id: str = "default") -> dict[str, Any]:
        item = self.repository.get_knowledge_item(knowledge_id, tenant_id)
        if item is None:
            raise ValueError("knowledge item not found")
        revision = self.repository.get_revision(knowledge_id, tenant_id=tenant_id)
        assert revision is not None
        return {
            "item": item.model_dump(mode="json"),
            "proposition": revision.proposition.model_dump(mode="json"),
            "status": revision.state.model_dump(mode="json"),
            "scope": revision.scope.model_dump(mode="json"),
            "supporting_sources": revision.provenance_refs,
            "contradictions": [
                conflict.model_dump(mode="json")
                for conflict in self.repository.list_conflicts(
                    tenant_id,
                    proposition_key=revision.proposition.proposition_key,
                )
            ],
            "freshness": revision.state.lifecycle_status.value,
            "promotion_policy": {"id": revision.policy_id, "version": revision.policy_version},
            "promotion_reasons": revision.revision_reason,
            "investigation_usage": [
                item.model_dump(mode="json")
                for item in self.repository.list_usage(
                    tenant_id=tenant_id,
                    knowledge_id=knowledge_id,
                )
            ],
            "live_corroboration": revision.state.eligibility == KnowledgeEligibility.LIVE_VERIFIED,
            "corrections": [],
            "revision_history": [
                item.model_dump(mode="json") for item in self.repository.list_revisions(knowledge_id, tenant_id)
            ],
        }

    def _require_candidate(self, candidate_id: str, tenant_id: str) -> KnowledgeCandidate:
        candidate = self.repository.get_candidate(candidate_id, tenant_id)
        if candidate is None:
            raise ValueError("knowledge candidate not found")
        return candidate

    @staticmethod
    def _is_entity_resolution_repair(
        existing: KnowledgeCandidate,
        candidate: KnowledgeCandidate,
    ) -> bool:
        return (
            existing.entity_resolution.status in {EntityResolutionStatus.UNRESOLVED, EntityResolutionStatus.AMBIGUOUS}
            and candidate.entity_resolution.status == EntityResolutionStatus.RESOLVED
            and existing.entity_resolution.raw_value == candidate.entity_resolution.raw_value
            and existing.kind == candidate.kind
            and existing.payload_ref == candidate.payload_ref
            and existing.scope == candidate.scope
            and existing.proposition.predicate == candidate.proposition.predicate
            and existing.proposition.concept_ref == candidate.proposition.concept_ref
        )

    def _resolve_conflicts_for_rejected_proposition(
        self,
        candidate: KnowledgeCandidate,
        reviewer: str,
    ) -> None:
        proposition_key = candidate.proposition.proposition_key
        viable_candidates = [
            item
            for item in self.repository.candidates_for_proposition(candidate.tenant_id, proposition_key)
            if item.state.review_state != ReviewState.REJECTED
        ]
        if viable_candidates:
            return
        for conflict in self.repository.list_conflicts(
            candidate.tenant_id,
            proposition_key=proposition_key,
            unresolved_only=True,
        ):
            resolved = conflict.model_copy(
                update={
                    "resolution_status": ConflictResolutionStatus.RESOLVED_BY_REVIEW,
                    "resolution_reason": "counter_proposition_rejected",
                    "resolved_by": reviewer,
                    "resolved_at": utc_now(),
                }
            )
            self.repository.save_conflict(resolved)
            self.repository.append_event(
                "conflict_resolved",
                tenant_id=candidate.tenant_id,
                subject_ref=resolved.id,
                dimensions={
                    "knowledge_kind": candidate.kind.value,
                    "reason_code": "counter_proposition_rejected",
                },
                payload={"candidate_id": candidate.id},
            )

    @staticmethod
    def _combined_resolution(subject: EntityResolutionResult, object_: EntityResolutionResult | None):
        results = [item for item in (subject, object_) if item is not None]
        if any(item.status == EntityResolutionStatus.AMBIGUOUS for item in results):
            status = EntityResolutionStatus.AMBIGUOUS
        elif any(item.status != EntityResolutionStatus.RESOLVED for item in results):
            status = EntityResolutionStatus.UNRESOLVED
        else:
            status = EntityResolutionStatus.RESOLVED
        return EntityResolutionResult(
            status=status,
            raw_value=" -> ".join(item.raw_value for item in results),
            selected_entity_ref=subject.selected_entity_ref if status == EntityResolutionStatus.RESOLVED else "",
            candidate_bindings=[binding for item in results for binding in item.candidate_bindings],
            reason_codes=[reason for item in results for reason in item.reason_codes],
        )

    @staticmethod
    def _subject_kind(kind: KnowledgeKind) -> EntityKind | None:
        if kind in {KnowledgeKind.DEPENDENCY, KnowledgeKind.OWNERSHIP}:
            return EntityKind.SERVICE
        return None

    @staticmethod
    def _object_kind(kind: KnowledgeKind) -> EntityKind | None:
        if kind == KnowledgeKind.OWNERSHIP:
            return EntityKind.TEAM
        if kind == KnowledgeKind.DEPENDENCY:
            return EntityKind.UNKNOWN
        return None

    @staticmethod
    def _default_predicate(kind: KnowledgeKind) -> Predicate:
        return {
            KnowledgeKind.DEPENDENCY: Predicate.DEPENDS_ON,
            KnowledgeKind.OWNERSHIP: Predicate.OWNED_BY,
            KnowledgeKind.SIGNAL_MAPPING: Predicate.REPRESENTED_BY,
            KnowledgeKind.EVIDENCE_REQUIREMENT: Predicate.REQUIRES_OBSERVATION,
            KnowledgeKind.ARTIFACT_QUALITY: Predicate.USEFUL_FOR_INVESTIGATION,
            KnowledgeKind.INVESTIGATION_PATTERN: Predicate.USEFUL_FOR_INVESTIGATION,
        }[kind]

    @staticmethod
    def _kind_for_correction(correction_type: CorrectionType, proposed: dict[str, Any]) -> KnowledgeKind:
        if correction_type == CorrectionType.DEPENDENCY:
            return KnowledgeKind.DEPENDENCY
        if correction_type == CorrectionType.OWNERSHIP:
            return KnowledgeKind.OWNERSHIP
        if correction_type in {CorrectionType.SIGNAL_MEANING, CorrectionType.ENTITY_MAPPING}:
            return KnowledgeKind.SIGNAL_MAPPING
        if correction_type in {CorrectionType.MISSING_CHECK, CorrectionType.OBSERVATION_DISPUTE}:
            return KnowledgeKind.EVIDENCE_REQUIREMENT
        return KnowledgeKind(proposed.get("kind", KnowledgeKind.ARTIFACT_QUALITY.value))

    @staticmethod
    def _used_for(revision: KnowledgeRevision) -> list[str]:
        kind = revision.proposition.kind
        if kind == KnowledgeKind.DEPENDENCY and revision.proposition.predicate == Predicate.DOES_NOT_DEPEND_ON:
            return ["candidate_exclusion"]
        return {
            KnowledgeKind.DEPENDENCY: ["candidate_generation", "ranking"],
            KnowledgeKind.OWNERSHIP: ["routing", "context"],
            KnowledgeKind.SIGNAL_MAPPING: ["evidence_resolution"],
            KnowledgeKind.EVIDENCE_REQUIREMENT: ["evidence_requirement"],
            KnowledgeKind.ARTIFACT_QUALITY: ["artifact_filtering"],
            KnowledgeKind.INVESTIGATION_PATTERN: ["context"],
        }[kind]

    @staticmethod
    def _score_delta(revision: KnowledgeRevision) -> float:
        return (
            0.08
            if revision.proposition.kind == KnowledgeKind.DEPENDENCY
            and revision.proposition.predicate != Predicate.DOES_NOT_DEPEND_ON
            else 0.0
        )

    @staticmethod
    def _candidate_ref(entity_ref: str) -> str:
        parts = entity_ref.split(":")
        if len(parts) >= 3 and parts[0] == "entity":
            return f"{parts[1]}:{':'.join(parts[2:])}"
        if len(parts) >= 2:
            return entity_ref
        return f"service:{entity_ref}"

    def _disposition(self, revision: KnowledgeRevision, scope: KnowledgeScope):
        state = revision.state
        if state.review_state == ReviewState.REJECTED:
            return KnowledgeUsageDisposition.REJECTED_BY_REVIEW_STATE, ["review_state_rejected"]
        if state.lifecycle_status == LifecycleStatus.STALE:
            return KnowledgeUsageDisposition.REJECTED_AS_STALE, ["stale_policy_rejects_ranking"]
        if state.lifecycle_status != LifecycleStatus.ACTIVE:
            return KnowledgeUsageDisposition.REJECTED_BY_ELIGIBILITY, [f"lifecycle_{state.lifecycle_status.value}"]
        if state.eligibility == KnowledgeEligibility.INELIGIBLE:
            return KnowledgeUsageDisposition.REJECTED_BY_ELIGIBILITY, ["knowledge_ineligible"]
        if not revision.scope.applies_to(scope):
            return KnowledgeUsageDisposition.REJECTED_BY_SCOPE, ["scope_mismatch"]
        conflicts = self.repository.list_conflicts(
            revision.tenant_id,
            proposition_key=revision.proposition.proposition_key,
            unresolved_only=True,
        )
        if conflicts:
            return KnowledgeUsageDisposition.REJECTED_BY_CONFLICT, ["unresolved_conflict"]
        return KnowledgeUsageDisposition.APPLIED, ["eligible_under_recorded_policy"]

    @staticmethod
    def _state_decision(candidate, decision_type, reason):
        fingerprint = stable_fingerprint([candidate.id, decision_type.value, reason, candidate.updated_at])
        return PromotionDecision(
            decision_id=f"promotion_{fingerprint.split(':', 1)[1][:20]}",
            candidate_ref=candidate.id,
            policy_id="human-review-v1",
            policy_version="1",
            decision=decision_type,
            resulting_eligibility=KnowledgeEligibility.INELIGIBLE,
            reason_codes=[reason],
            input_fingerprint=fingerprint,
        )


def get_knowledge_service() -> KnowledgeService:
    return KnowledgeService(get_knowledge_repository())
