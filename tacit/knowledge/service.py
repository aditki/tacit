"""Governed Operational Knowledge lifecycle orchestration."""

from __future__ import annotations

import re
from collections.abc import Collection
from typing import Any

import structlog

from tacit.config import settings
from tacit.knowledge.corroboration import ConflictDetectionService, CorroborationService
from tacit.knowledge.entities import EntityResolutionService
from tacit.knowledge.enums import (
    ConflictKind,
    ConflictResolutionStatus,
    CorrectionType,
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
from tacit.knowledge.lifecycle import (
    transition_lifecycle_state,
    transition_review_state,
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
from tacit.knowledge.normalization import (
    PropositionNormalizer,
    candidate_ref_from_entity_ref,
    canonical_scope_payload,
    normalize_entity,
    normalize_entity_id,
    normalize_service_ref,
    stable_fingerprint,
)
from tacit.knowledge.policies import default_policies
from tacit.knowledge.repository import (
    CandidateEvaluationConflictError,
    CandidateLifecycleConflictError,
    CandidateMergeConflictError,
    CandidateReviewConflictError,
    KnowledgeRepository,
    KnowledgeRevisionConflictError,
    get_knowledge_repository,
)
from tacit.knowledge.usage import KnowledgeRevisionRef, KnowledgeStageUse
from tacit.tenancy import resolve_tenant_boundary

PROMPT_INJECTION_RE = re.compile(
    r"\b(ignore (?:all |the )?(?:previous|system) instructions|system prompt|developer message|"
    r"mark (?:this|me) trusted|promote (?:this|me)|override (?:policy|ranking)|reveal secrets?)\b",
    re.I,
)
logger = structlog.get_logger()


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
    def __init__(
        self,
        repository: KnowledgeRepository | None = None,
        *,
        signal_store: Any | None = None,
        runtime_settings: Any | None = None,
    ):
        self.repository = repository or get_knowledge_repository()
        self._signal_store_instance = signal_store
        self._runtime_settings = runtime_settings or getattr(signal_store, "_settings", None) or settings
        self.entity_resolution = EntityResolutionService(self.repository)
        self.normalizer = PropositionNormalizer()
        self.corroboration = CorroborationService(self.repository)
        self.conflicts = ConflictDetectionService(self.repository)
        self.policies = default_policies()

    def _resolve_tenant(self, tenant_id: str | None) -> str:
        return resolve_tenant_boundary(
            str(getattr(self._runtime_settings, "knowledge_tenant_id", "default") or "default"),
            tenant_id,
        )

    def _record_tenant(self, record: Any) -> str:
        requested = record.tenant_id if "tenant_id" in record.model_fields_set else None
        return self._resolve_tenant(requested)

    @staticmethod
    def _scope_for_tenant(scope: KnowledgeScope | None, tenant_id: str) -> KnowledgeScope:
        if scope is None:
            return KnowledgeScope(tenant_id=tenant_id)
        tenant_was_explicit = "tenant_id" in scope.model_fields_set
        scope = KnowledgeScope.model_validate(scope)
        if scope.tenant_id == tenant_id:
            return scope
        if tenant_was_explicit:
            raise ValueError("knowledge scope cannot cross tenants")
        return scope.model_copy(update={"tenant_id": tenant_id})

    def _validated_usage(self, usage: list[KnowledgeUsage]) -> list[KnowledgeUsage]:
        validated: list[KnowledgeUsage] = []
        resolved_tenants: set[str] = set()
        for item in usage:
            requested = item.tenant_id if "tenant_id" in item.model_fields_set else None
            tenant_id = self._resolve_tenant(requested)
            resolved_tenants.add(tenant_id)
            validated.append(item if item.tenant_id == tenant_id else item.model_copy(update={"tenant_id": tenant_id}))
        if len(resolved_tenants) > 1:
            raise ValueError("knowledge usage batch cannot cross tenants")
        return validated

    def register_entity(self, entity: Entity) -> Entity:
        tenant_id = self._record_tenant(entity)
        scope = self._scope_for_tenant(entity.scope, tenant_id)
        entity = entity.model_copy(update={"tenant_id": tenant_id, "scope": scope})
        existing = self.repository.get_entity(normalize_entity(entity.id), entity.tenant_id)
        if existing is not None and existing.kind != entity.kind:
            raise ValueError("entity kind cannot change for an existing entity id")
        canonical_id = normalize_entity_id(entity.id, entity.kind)
        if canonical_id != entity.id:
            entity = entity.model_copy(update={"id": canonical_id})
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
        tenant_id = self._record_tenant(alias)
        scope = self._scope_for_tenant(alias.scope, tenant_id)
        alias = alias.model_copy(update={"tenant_id": tenant_id, "scope": scope})
        entity = self.repository.get_entity(alias.entity_ref, alias.tenant_id)
        if entity is None:
            raise ValueError("alias target entity does not exist in the tenant")
        if entity.status != EntityStatus.ACTIVE:
            raise ValueError("alias target entity is not active")
        saved = self.repository.save_alias(alias)
        self.repository.append_event(
            "entity_alias_registered",
            tenant_id=alias.tenant_id,
            subject_ref=alias.entity_ref,
            dimensions={
                "review_state": alias.review_state.value,
                "lifecycle_status": alias.lifecycle_status.value,
                "reason_code": alias.method.value,
            },
            payload={"alias_id": alias.id, "normalized_value": alias.normalized_value},
        )
        return saved

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
        tenant_id: str | None = None,
        candidate_id: str | None = None,
        migration_provenance: MigrationProvenance | None = None,
        reactivate_stale: bool = False,
    ) -> KnowledgeCandidate:
        tenant_id = self._resolve_tenant(tenant_id)
        knowledge_kind = KnowledgeKind(kind)
        scope = self._scope_for_tenant(scope, tenant_id)
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
        extracted_candidate = candidate
        first_evidence = evidence_model.items[0] if evidence_model.items else None
        for _attempt in range(5):
            existing = self.repository.get_candidate(candidate.id, tenant_id)
            candidate = extracted_candidate
            if existing is not None:
                proposition_changed = existing.proposition.proposition_key != candidate.proposition.proposition_key
                if proposition_changed and not self._is_entity_resolution_repair(existing, candidate):
                    raise ValueError("candidate identity cannot be reused for a different proposition")
                existing_state = existing.state
                if reactivate_stale and existing_state.lifecycle_status == LifecycleStatus.STALE:
                    existing_state = transition_lifecycle_state(existing_state, LifecycleStatus.ACTIVE)
                candidate = candidate.model_copy(
                    update={
                        "state": existing_state,
                        "corroboration": existing.corroboration,
                        "policy": existing.policy,
                        "security_flags": sorted(set(existing.security_flags + candidate.security_flags)),
                        "created_at": existing.created_at,
                    }
                )
            try:
                self.repository.save_candidate_with_proposition(
                    candidate,
                    lineage_group=(first_evidence.lineage_group if first_evidence else payload_ref),
                    independence_class=(
                        first_evidence.lineage_kind.value if first_evidence else LineageKind.UNKNOWN.value
                    ),
                    expected=existing,
                )
                break
            except CandidateMergeConflictError:
                continue
        else:
            raise CandidateMergeConflictError("candidate kept changing during re-ingestion")
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
        tenant_id: str | None = None,
        trust: bool = False,
        can_trust: bool = False,
        _correction_id: str | None = None,
    ) -> KnowledgeCandidate:
        tenant_id = self._resolve_tenant(tenant_id)
        candidate = self._require_candidate(candidate_id, tenant_id)
        self._require_candidate_workflow(candidate_id, tenant_id, _correction_id)
        if trust and not can_trust:
            raise PermissionError("knowledge.trust permission is required")
        review_state = ReviewState.TRUSTED if trust else ReviewState.APPROVED if approved else ReviewState.REJECTED
        if candidate.state.review_state == review_state:
            if review_state == ReviewState.REJECTED:
                if candidate.kind == KnowledgeKind.SIGNAL_MAPPING:
                    self._signal_store()
                with self.repository.transaction():
                    current = self._require_candidate(candidate_id, tenant_id)
                    if current.state.review_state != ReviewState.REJECTED:
                        raise CandidateReviewConflictError("candidate changed; reload before reviewing")
                    self._reconcile_removed_candidates(
                        [current],
                        lifecycle_status=LifecycleStatus.WITHDRAWN,
                        reason="candidate_rejected",
                    )
            return candidate
        state = transition_review_state(candidate.state, review_state)
        updated = candidate.model_copy(update={"state": state, "updated_at": utc_now()})
        if review_state == ReviewState.REJECTED and candidate.kind == KnowledgeKind.SIGNAL_MAPPING:
            self._signal_store()
        try:
            with self.repository.transaction():
                self.repository.transition_candidate_review(updated, expected=candidate)
                if review_state == ReviewState.REJECTED:
                    decision = self._state_decision(updated, PromotionDecisionType.REJECT, "rejected_by_review")
                    self.repository.save_promotion_decision(decision, tenant_id)
                    self._resolve_conflicts_for_rejected_proposition(updated, reviewer)
                    self._reconcile_removed_candidates(
                        [updated],
                        lifecycle_status=LifecycleStatus.WITHDRAWN,
                        reason="candidate_rejected",
                    )
                self.repository.append_event(
                    (
                        "correction_reviewed"
                        if candidate.kind == KnowledgeKind.ARTIFACT_QUALITY
                        else "promotion_evaluated"
                    ),
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
        except CandidateReviewConflictError:
            concurrent = self.repository.get_candidate(candidate.id, tenant_id)
            if concurrent is not None and concurrent.state.review_state == review_state:
                return concurrent
            raise
        return updated

    def evaluate_candidate(
        self,
        candidate_id: str,
        *,
        tenant_id: str | None = None,
        authoritative_source: bool = False,
        live_verified: bool = False,
        ignored_conflict_ids: set[str] | None = None,
        _correction_id: str | None = None,
    ) -> tuple[PromotionDecision, KnowledgeRevision | None]:
        tenant_id = self._resolve_tenant(tenant_id)
        observed = self._require_candidate(candidate_id, tenant_id)
        self._require_candidate_workflow(candidate_id, tenant_id, _correction_id)
        if observed.kind == KnowledgeKind.SIGNAL_MAPPING:
            # SignalStore schema initialization uses its own connection. Resolve
            # it before taking the authority write lock used by the projection.
            self._signal_store()
        with self.repository.transaction():
            return self._evaluate_candidate_in_transaction(
                candidate_id,
                tenant_id=tenant_id,
                authoritative_source=authoritative_source,
                live_verified=live_verified,
                ignored_conflict_ids=ignored_conflict_ids,
                _correction_id=_correction_id,
            )

    def _evaluate_candidate_in_transaction(
        self,
        candidate_id: str,
        *,
        tenant_id: str,
        authoritative_source: bool,
        live_verified: bool,
        ignored_conflict_ids: set[str] | None,
        _correction_id: str | None,
    ) -> tuple[PromotionDecision, KnowledgeRevision | None]:
        """Evaluate and persist authority while one immediate write lock is held."""
        candidate = self._require_candidate(candidate_id, tenant_id)
        self._require_candidate_workflow(candidate_id, tenant_id, _correction_id)
        evaluated_candidate = candidate
        summary, corroboration_ref = self.corroboration.analyze(tenant_id, candidate.proposition.proposition_key)
        conflicts = self.conflicts.analyze(
            tenant_id,
            candidate.proposition.proposition_key,
            candidate_id=candidate.id,
        )
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
            authoritative_source=authoritative_source,
            live_verified=live_verified,
        )
        state = candidate.state.model_copy(update={"eligibility": decision.resulting_eligibility})
        candidate = candidate.model_copy(
            update={"corroboration": summary, "policy": policy_state, "state": state, "updated_at": utc_now()}
        )
        try:
            self.repository.save_candidate_evaluation(candidate, expected=evaluated_candidate)
        except CandidateEvaluationConflictError:
            concurrent_candidate = self.repository.get_candidate(candidate.id, tenant_id)
            if (
                concurrent_candidate is None
                or concurrent_candidate.model_copy(update={"updated_at": candidate.updated_at}) != candidate
            ):
                raise
            candidate = concurrent_candidate
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
        resolver_payload = (
            self._build_signal_mapping_resolver_payload(candidate, contributors)
            if candidate.kind == KnowledgeKind.SIGNAL_MAPPING
            else {}
        )
        semantic = stable_fingerprint(
            {
                "proposition": candidate.proposition.model_dump(mode="json"),
                "scope": canonical_scope_payload(candidate.scope),
                "state": state.model_dump(mode="json"),
                "policy": [policy.policy_id, policy.version],
                "promotion_inputs": {
                    "authoritative_source": authoritative_source,
                    "live_verified": live_verified,
                },
                "conflicts": sorted(conflict.id for conflict in conflicts),
                "contributors": contributor_refs,
                "provenance": provenance_refs,
                "resolver_payload": resolver_payload,
            }
        )
        current_revision = (
            self.repository.get_revision(existing.id, tenant_id=tenant_id) if existing is not None else None
        )
        if current_revision is not None and current_revision.semantic_fingerprint == semantic:
            current_revision = self._repair_signal_mapping_projection(
                current_revision,
                expected_semantic_fingerprint=semantic,
            )
            return decision, current_revision
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
            resolver_payload=resolver_payload,
            revision_reason="promoted" if revision_number == 1 else "corroborated",
            semantic_fingerprint=semantic,
        )
        try:
            self._persist_revision_with_projection(
                revision,
                candidate_id=candidate.id,
                decision_ref=decision.decision_id,
                expected_candidate=candidate,
                expected_contributors=contributors,
            )
        except KnowledgeRevisionConflictError:
            concurrent_revision = self.repository.get_revision(knowledge_id, tenant_id=tenant_id)
            if concurrent_revision is None or concurrent_revision.semantic_fingerprint != revision.semantic_fingerprint:
                raise
            concurrent_revision = self._repair_signal_mapping_projection(
                concurrent_revision,
                expected_semantic_fingerprint=revision.semantic_fingerprint,
            )
            return decision, concurrent_revision
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
        requested_tenant = scope.tenant_id if "tenant_id" in scope.model_fields_set else None
        scope = self._scope_for_tenant(scope, self._resolve_tenant(requested_tenant))
        selected: list[KnowledgeSnapshotItem] = []
        usage: list[KnowledgeUsage] = []
        for revision in self.repository.list_current_revisions(scope.tenant_id):
            disposition, reasons = self._disposition(revision, scope)
            if disposition == KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED:
                selected.append(KnowledgeSnapshotItem(knowledge_ref=revision.knowledge_id, revision=revision.revision))
            usage.append(
                KnowledgeUsage(
                    tenant_id=scope.tenant_id,
                    knowledge_ref=revision.knowledge_id,
                    knowledge_revision=revision.revision,
                    disposition=disposition,
                    used_for=[],
                    target_ref=revision.proposition.object_ref or revision.proposition.subject_ref,
                    score_delta=0.0,
                    decision_ref=revision.decision_ref,
                    provenance_refs=revision.provenance_refs,
                    reason_codes=reasons,
                )
            )
        return self._save_snapshot(scope.tenant_id, selected), usage

    def snapshot_from_usage(self, tenant_id: str | None, usage: list[KnowledgeUsage]) -> KnowledgeSnapshot:
        """Persist the final selected set after reconciliation and stage consumption."""
        tenant_id = self._resolve_tenant(tenant_id)
        usage = self._validated_usage(usage)
        if any(item.tenant_id != tenant_id for item in usage):
            raise ValueError("knowledge usage cannot cross tenants")
        selected = [
            KnowledgeSnapshotItem(knowledge_ref=item.knowledge_ref, revision=item.knowledge_revision)
            for item in usage
            if item.disposition
            in {
                KnowledgeUsageDisposition.APPLIED,
                KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
            }
        ]
        return self._save_snapshot(tenant_id, selected)

    def apply_compilation_usage(
        self,
        usage: list[KnowledgeUsage],
        applied_knowledge_refs: Collection[KnowledgeRevisionRef | str],
    ) -> list[KnowledgeUsage]:
        """Mark governed signal mappings whose resolver rows changed compilation."""
        return self._apply_signal_mapping_stage_usage(
            usage,
            applied_knowledge_refs,
            stage="query_compilation",
            reason_code="signal_mapping_selected_for_compilation",
        )

    def apply_evidence_usage(
        self,
        usage: list[KnowledgeUsage],
        applied_knowledge_refs: Collection[KnowledgeRevisionRef | str],
        refs_by_requirement: dict[str, set[KnowledgeRevisionRef] | frozenset[KnowledgeRevisionRef]] | None = None,
    ) -> list[KnowledgeUsage]:
        """Mark governed mappings that selected an evidence metric."""
        reason_codes_by_ref: dict[KnowledgeRevisionRef, set[str]] = {}
        for requirement_id, refs in (refs_by_requirement or {}).items():
            for revision_ref in refs:
                reason_codes_by_ref.setdefault(revision_ref, set()).add(f"evidence_requirement:{requirement_id}")
        return self._apply_signal_mapping_stage_usage(
            usage,
            applied_knowledge_refs,
            stage="evidence_resolution",
            reason_code="signal_mapping_selected_for_evidence",
            reason_codes_by_ref=reason_codes_by_ref,
        )

    def apply_stage_usage(
        self,
        usage: list[KnowledgeUsage],
        stage_uses: list[KnowledgeStageUse] | tuple[KnowledgeStageUse, ...],
    ) -> list[KnowledgeUsage]:
        """Reconcile exact resolver effects confirmed by non-query stages."""
        reconciled = usage
        grouped: dict[tuple[str, str], set[KnowledgeRevisionRef]] = {}
        reason_codes_by_group: dict[tuple[str, str], dict[KnowledgeRevisionRef, set[str]]] = {}
        for use in stage_uses:
            group = (use.stage.value, use.effect.value)
            grouped.setdefault(group, set()).add(use.revision_ref)
            reason_codes_by_group.setdefault(group, {}).setdefault(use.revision_ref, set()).add(
                f"stage_target:{use.stage.value}:{use.target_ref}"
            )
        for (stage, effect), revision_refs in grouped.items():
            reconciled = self._apply_signal_mapping_stage_usage(
                reconciled,
                revision_refs,
                stage=stage,
                reason_code=effect,
                reason_codes_by_ref=reason_codes_by_group[(stage, effect)],
            )
        return reconciled

    def _apply_signal_mapping_stage_usage(
        self,
        usage: list[KnowledgeUsage],
        applied_knowledge_refs: Collection[KnowledgeRevisionRef | str],
        *,
        stage: str,
        reason_code: str,
        reason_codes_by_ref: dict[KnowledgeRevisionRef, set[str]] | None = None,
    ) -> list[KnowledgeUsage]:
        usage = self._validated_usage(usage)
        if not applied_knowledge_refs:
            return usage
        exact_refs = {item for item in applied_knowledge_refs if isinstance(item, KnowledgeRevisionRef)}
        legacy_refs = {item for item in applied_knowledge_refs if isinstance(item, str)}
        reconciled = list(usage)
        for index, item in enumerate(usage):
            item_ref = KnowledgeRevisionRef(item.knowledge_ref, item.knowledge_revision)
            if item_ref not in exact_refs and item.knowledge_ref not in legacy_refs:
                continue
            if item.disposition not in {
                KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
                KnowledgeUsageDisposition.APPLIED,
            }:
                logger.warning(
                    "signal_mapping_usage_disposition_mismatch",
                    tenant_id=item.tenant_id,
                    knowledge_id=item.knowledge_ref,
                    knowledge_revision=item.knowledge_revision,
                    disposition=item.disposition.value,
                )
                continue
            revision = self.repository.get_revision(
                item.knowledge_ref,
                item.knowledge_revision,
                tenant_id=item.tenant_id,
            )
            if revision is None or revision.proposition.kind != KnowledgeKind.SIGNAL_MAPPING:
                continue
            reason_codes = list(item.reason_codes)
            if reason_code not in reason_codes:
                reason_codes.append(reason_code)
            for extra_reason in sorted((reason_codes_by_ref or {}).get(item_ref, set())):
                if extra_reason not in reason_codes:
                    reason_codes.append(extra_reason)
            used_for = list(item.used_for)
            if stage not in used_for:
                used_for.append(stage)
            reconciled[index] = item.model_copy(
                update={
                    "disposition": KnowledgeUsageDisposition.APPLIED,
                    "used_for": used_for,
                    "score_delta": 0.0,
                    "reason_codes": reason_codes,
                }
            )
        audited_exact_refs = {
            KnowledgeRevisionRef(item.knowledge_ref, item.knowledge_revision)
            for item in reconciled
            if item.disposition == KnowledgeUsageDisposition.APPLIED and stage in item.used_for
        }
        audited_legacy_refs = {item.knowledge_ref for item in reconciled if stage in item.used_for}
        missing_exact_refs = exact_refs.difference(audited_exact_refs)
        missing_legacy_refs = legacy_refs.difference(audited_legacy_refs)
        if missing_exact_refs or missing_legacy_refs:
            requested_revisions = {ref.knowledge_ref: ref.knowledge_revision for ref in missing_exact_refs}
            selected_revisions = {
                item.knowledge_ref: item.knowledge_revision
                for item in usage
                if item.knowledge_ref in requested_revisions
            }
            if selected_revisions:
                logger.error(
                    "knowledge_stage_revision_mismatch",
                    stage=stage,
                    requested_revisions=requested_revisions,
                    selected_revisions=selected_revisions,
                )
            logger.error(
                "governed_stage_usage_missing",
                stage=stage,
                knowledge_refs=sorted(
                    [f"{ref.knowledge_ref}@{ref.knowledge_revision}" for ref in missing_exact_refs]
                    + list(missing_legacy_refs)
                ),
            )
            raise RuntimeError(
                f"Governed {stage} references were not present in the selected knowledge snapshot: "
                + ", ".join(
                    sorted(
                        [f"{ref.knowledge_ref}@{ref.knowledge_revision}" for ref in missing_exact_refs]
                        + list(missing_legacy_refs)
                    )
                )
            )
        return reconciled

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
        """Apply dependency knowledge and return stage-confirmed usage records."""
        from tacit.models.schemas import CulpritCandidate

        usage = self._validated_usage(usage)
        candidates = list(ranking.candidates)
        original_candidate_count = len(candidates)
        updated_usage = list(usage)
        applicable: list[tuple[int, KnowledgeUsage, KnowledgeRevision]] = []
        selectable = {
            KnowledgeUsageDisposition.APPLIED,
            KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
        }
        for index, item in enumerate(usage):
            if item.disposition not in selectable:
                continue
            revision = self.repository.get_revision(
                item.knowledge_ref,
                item.knowledge_revision,
                tenant_id=item.tenant_id,
            )
            if revision is not None and revision.proposition.kind == KnowledgeKind.DEPENDENCY:
                applicable.append((index, item, revision))
                updated_usage[index] = item.model_copy(
                    update={
                        "disposition": KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
                        "used_for": [],
                        "score_delta": 0.0,
                    }
                )

        excluded_refs = {
            self._candidate_ref(revision.proposition.object_ref)
            for _, _, revision in applicable
            if revision.proposition.predicate == Predicate.DOES_NOT_DEPEND_ON
        }
        ranked_refs = {f"{candidate.suspect_type}:{candidate.suspect}" for candidate in candidates}
        matched_exclusions = excluded_refs.intersection(ranked_refs)
        for index, item, revision in applicable:
            candidate_ref = self._candidate_ref(revision.proposition.object_ref)
            if revision.proposition.predicate != Predicate.DOES_NOT_DEPEND_ON:
                continue
            if candidate_ref not in matched_exclusions:
                continue
            reason_codes = list(item.reason_codes)
            if "ranking_candidate_excluded" not in reason_codes:
                reason_codes.append("ranking_candidate_excluded")
            updated_usage[index] = item.model_copy(
                update={
                    "disposition": KnowledgeUsageDisposition.APPLIED,
                    "used_for": ["candidate_exclusion"],
                    "score_delta": 0.0,
                    "reason_codes": reason_codes,
                }
            )
        candidates = [
            candidate
            for candidate in candidates
            if f"{candidate.suspect_type}:{candidate.suspect}" not in excluded_refs
        ]
        excluded_ranked_candidate = len(candidates) < original_candidate_count
        by_ref = {f"{candidate.suspect_type}:{candidate.suspect}": candidate for candidate in candidates}
        for index, item, revision in applicable:
            candidate_ref = self._candidate_ref(revision.proposition.object_ref)
            if revision.proposition.predicate == Predicate.DOES_NOT_DEPEND_ON or candidate_ref in excluded_refs:
                continue
            requested_delta = self._score_delta(revision)
            if requested_delta <= 0:
                continue
            reason = (
                f"Operational Knowledge {revision.knowledge_id} revision {revision.revision} "
                "provides scoped dependency context."
            )
            existing = by_ref.get(candidate_ref)
            used_for: list[str]
            applied_delta: float
            if existing is not None:
                updated_score = min(1.0, existing.score + requested_delta)
                contextual_reasons = list(existing.contextual_reasons)
                if reason not in contextual_reasons:
                    contextual_reasons.append(reason)
                if updated_score == existing.score and contextual_reasons == existing.contextual_reasons:
                    continue
                updated = existing.model_copy(
                    update={
                        "score": updated_score,
                        "contextual_reasons": contextual_reasons,
                    }
                )
                candidates[candidates.index(existing)] = updated
                by_ref[candidate_ref] = updated
                used_for = ["ranking"]
                applied_delta = updated_score - existing.score
            else:
                suspect_type, suspect = candidate_ref.split(":", 1)
                added = CulpritCandidate(
                    rank=len(candidates) + 1,
                    suspect=suspect,
                    suspect_type=suspect_type,
                    score=requested_delta,
                    contextual_reasons=[reason],
                )
                candidates.append(added)
                by_ref[candidate_ref] = added
                used_for = ["candidate_generation", "ranking"]
                applied_delta = requested_delta
            reason_codes = list(item.reason_codes)
            if "ranking_changed" not in reason_codes:
                reason_codes.append("ranking_changed")
            updated_usage[index] = item.model_copy(
                update={
                    "disposition": KnowledgeUsageDisposition.APPLIED,
                    "used_for": used_for,
                    "score_delta": applied_delta,
                    "reason_codes": reason_codes,
                }
            )
        candidates.sort(key=lambda candidate: (-candidate.score, candidate.suspect_type, candidate.suspect))
        candidates = [candidate.model_copy(update={"rank": index}) for index, candidate in enumerate(candidates, 1)]
        update: dict[str, Any] = {"candidates": candidates}
        if excluded_ranked_candidate and not candidates and not ranking.abstained:
            update.update(
                {
                    "abstained": True,
                    "abstention_reason": "operational_knowledge_excluded_ranked_candidates",
                }
            )
        return ranking.model_copy(update=update), updated_usage

    def reconcile_live_observations(self, usage: list[KnowledgeUsage], observations) -> list[KnowledgeUsage]:
        """Let exact negative runtime evidence veto matching contextual knowledge."""
        from tacit.models.schemas import EvidenceObservationOutcome

        usage = self._validated_usage(usage)
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
            if revision is None or item.disposition not in {
                KnowledgeUsageDisposition.APPLIED,
                KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
            }:
                reconciled.append(item)
                continue
            proposition = revision.proposition
            if proposition.kind == KnowledgeKind.DEPENDENCY and proposition.predicate == Predicate.DOES_NOT_DEPEND_ON:
                reconciled.append(item)
                continue
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
            if (
                proposition.kind == KnowledgeKind.SIGNAL_MAPPING
                and item.disposition == KnowledgeUsageDisposition.APPLIED
                and set(item.used_for).intersection({"archetype_selection", "query_compilation", "evidence_resolution"})
            ):
                reason_codes = list(item.reason_codes)
                if "live_negative_observation_after_applied_stage" not in reason_codes:
                    reason_codes.append("live_negative_observation_after_applied_stage")
                reconciled.append(item.model_copy(update={"reason_codes": reason_codes}))
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
        usage = self._validated_usage(usage)
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
        target_revision: int | None = None,
        tenant_id: str | None = None,
    ) -> tuple[KnowledgeCorrection, KnowledgeCandidate]:
        tenant_id = self._resolve_tenant(tenant_id)
        scope = self._scope_for_tenant(scope, tenant_id)
        with self.repository.transaction():
            return self._create_correction_in_transaction(
                investigation_id=investigation_id,
                investigation_revision=investigation_revision,
                correction_type=correction_type,
                proposed=proposed,
                scope=scope,
                explanation=explanation,
                created_by=created_by,
                target_ref=target_ref,
                target_revision=target_revision,
                tenant_id=tenant_id,
            )

    def _create_correction_in_transaction(
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
        target_revision: int | None = None,
        tenant_id: str = "default",
    ) -> tuple[KnowledgeCorrection, KnowledgeCandidate]:
        """Create correction ownership and its candidate under one write lock."""
        correction_type = CorrectionType(correction_type)
        target = None
        if target_ref:
            target = self.repository.get_revision(target_ref, target_revision, tenant_id=tenant_id)
            if target is None:
                if target_revision is None:
                    raise ValueError("correction target does not exist in the tenant")
                raise ValueError(f"correction target revision {target_revision} does not exist in the tenant")
            target_revision = target.revision
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
                target_revision,
            ],
        )
        existing_correction = self.repository.get_correction(correction_id, tenant_id)
        if existing_correction is not None:
            return existing_correction, self._require_candidate(
                existing_correction.knowledge_candidate_ref,
                tenant_id,
            )
        if target is not None:
            current = self.repository.get_revision(target_ref, tenant_id=tenant_id)
            if current is None:
                raise ValueError("correction target does not exist in the tenant")
            if current.revision != target_revision:
                raise ValueError(
                    f"correction target advanced from revision {target_revision} to {current.revision}; "
                    "rebase the correction"
                )
        original = {}
        if target is not None:
            original = target.proposition.model_dump(mode="json")
        kind = self._kind_for_correction(correction_type, proposed)
        candidate_proposition = proposed
        if correction_type == CorrectionType.ENTITY_MAPPING:
            raw_value, entity_ref = self._entity_mapping_values(proposed)
            entity = self.repository.get_entity(entity_ref, tenant_id)
            if entity is None or entity.status != EntityStatus.ACTIVE:
                raise ValueError("entity_mapping correction target entity is not active in the tenant")
            candidate_proposition = {
                "subject_ref": f"concept:{raw_value}",
                "predicate": Predicate.USEFUL_FOR_INVESTIGATION,
                "object_ref": entity_ref,
                "source_wording": str(proposed.get("source_wording") or ""),
            }
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
            proposition=candidate_proposition,
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
            target_revision=target_revision,
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
        tenant_id: str | None = None,
        authoritative: bool = False,
    ) -> tuple[KnowledgeCorrection, KnowledgeRevision | None]:
        """Review and apply a correction within one locked write transaction."""
        tenant_id = self._resolve_tenant(tenant_id)
        correction = self.repository.get_correction(correction_id, tenant_id)
        if correction is not None:
            candidate = self.repository.get_candidate(correction.knowledge_candidate_ref, tenant_id)
            target = (
                self.repository.get_revision(correction.target_ref, tenant_id=tenant_id)
                if correction.target_ref
                else None
            )
            if (candidate is not None and candidate.kind == KnowledgeKind.SIGNAL_MAPPING) or (
                target is not None and target.proposition.kind == KnowledgeKind.SIGNAL_MAPPING
            ):
                self._signal_store()
        with self.repository.transaction():
            return self._review_correction_in_transaction(
                correction_id,
                approved=approved,
                reviewer=reviewer,
                tenant_id=tenant_id,
                authoritative=authoritative,
            )

    def _review_correction_in_transaction(
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
        if not approved and correction.review_state == ReviewState.REJECTED:
            if correction.correction_type != CorrectionType.ENTITY_MAPPING:
                return correction, None
            alias_ref = correction.applied_alias_ref or self._entity_mapping_alias(correction).id
            alias = self.repository.get_alias(alias_ref, tenant_id)
            if (
                alias is not None
                and alias.review_state == ReviewState.REJECTED
                and alias.lifecycle_status == LifecycleStatus.WITHDRAWN
            ):
                return correction, None
        if approved and correction.applied_alias_ref:
            alias = self.repository.get_alias(correction.applied_alias_ref, tenant_id)
            if alias is None:
                raise ValueError("applied correction alias is missing from entity history")
            if alias.review_state not in {ReviewState.APPROVED, ReviewState.TRUSTED} or (
                alias.lifecycle_status != LifecycleStatus.ACTIVE
            ):
                raise ValueError("applied correction alias is no longer active")
            return correction, None
        if approved and correction.applied_knowledge_ref:
            applied = self.repository.get_revision(
                correction.applied_knowledge_ref,
                correction.applied_knowledge_revision,
                tenant_id=tenant_id,
            )
            if applied is None:
                raise ValueError("applied correction result is missing from immutable knowledge history")
            return correction, applied
        if (
            approved
            and correction.correction_type in {CorrectionType.KNOWLEDGE_STALE, CorrectionType.KNOWLEDGE_INCORRECT}
            and not correction.target_ref
        ):
            raise ValueError(f"{correction.correction_type.value} correction requires target_ref")
        target = None
        if correction.target_ref:
            if correction.target_revision is None:
                raise ValueError("correction target revision is unavailable; recreate the correction")
            target = self.repository.get_revision(
                correction.target_ref,
                correction.target_revision,
                tenant_id=tenant_id,
            )
            if target is None:
                raise ValueError("correction target knowledge revision not found")
            current_target = self.repository.get_revision(correction.target_ref, tenant_id=tenant_id)
            if current_target is None:
                raise ValueError("correction target knowledge item not found")
            if current_target.revision != correction.target_revision:
                raise ValueError(
                    f"correction target advanced from revision {correction.target_revision} "
                    f"to {current_target.revision}; rebase the correction"
                )
        candidate = self.review_candidate(
            correction.knowledge_candidate_ref,
            approved=approved,
            reviewer=reviewer,
            tenant_id=tenant_id,
            _correction_id=correction.id,
        )
        correction = correction.model_copy(update={"review_state": candidate.state.review_state})
        if not approved:
            if correction.correction_type == CorrectionType.ENTITY_MAPPING:
                alias_ref = correction.applied_alias_ref or self._entity_mapping_alias(correction).id
                alias = self.repository.get_alias(alias_ref, tenant_id)
                if alias is not None:
                    alias = alias.model_copy(
                        update={
                            "review_state": ReviewState.REJECTED,
                            "lifecycle_status": LifecycleStatus.WITHDRAWN,
                            "updated_at": utc_now(),
                        }
                    )
                    self.repository.save_alias(alias)
                    correction = correction.model_copy(update={"applied_alias_ref": alias.id})
                    self.repository.append_event(
                        "entity_alias_retired",
                        tenant_id=tenant_id,
                        subject_ref=alias.entity_ref,
                        dimensions={
                            "review_state": alias.review_state.value,
                            "lifecycle_status": alias.lifecycle_status.value,
                            "reason_code": "correction_rejected",
                        },
                        payload={"alias_id": alias.id, "correction_id": correction.id},
                    )
            self.repository.save_correction(correction)
            return correction, None
        self.repository.save_correction(correction)
        if correction.correction_type == CorrectionType.ENTITY_MAPPING:
            alias = self._entity_mapping_alias(correction)
            self.register_alias(alias)
            correction = correction.model_copy(update={"applied_alias_ref": alias.id})
            correction = self.repository.save_correction(correction)
            return correction, None
        if correction.correction_type in {
            CorrectionType.KNOWLEDGE_STALE,
            CorrectionType.KNOWLEDGE_INCORRECT,
        }:
            assert target is not None
            lifecycle_status = (
                LifecycleStatus.STALE
                if correction.correction_type == CorrectionType.KNOWLEDGE_STALE
                else LifecycleStatus.WITHDRAWN
            )
            retired_revision = self._retire_knowledge(
                target,
                candidate,
                lifecycle_status=lifecycle_status,
                reason=correction.correction_type.value,
            )
            correction = correction.model_copy(
                update={
                    "applied_knowledge_ref": retired_revision.knowledge_id,
                    "applied_knowledge_revision": retired_revision.revision,
                }
            )
            self.repository.save_correction(correction)
            return correction, retired_revision
        conflicts = self.conflicts.analyze(
            tenant_id,
            candidate.proposition.proposition_key,
            candidate_id=candidate.id,
        )
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
            _correction_id=correction.id,
        )
        superseded = False
        if revision and target is not None and replaceable_conflicts and correction.target_ref != revision.knowledge_id:
            self.supersede(
                correction.target_ref,
                candidate.id,
                tenant_id=tenant_id,
                expected_revision=correction.target_revision,
            )
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
        if revision is not None:
            correction = correction.model_copy(
                update={
                    "applied_knowledge_ref": revision.knowledge_id,
                    "applied_knowledge_revision": revision.revision,
                }
            )
            self.repository.save_correction(correction)
        return correction, revision

    def supersede(
        self,
        knowledge_id: str,
        replacement_candidate_id: str,
        *,
        tenant_id: str | None = None,
        expected_revision: int | None = None,
    ) -> KnowledgeRevision:
        tenant_id = self._resolve_tenant(tenant_id)
        current = self.repository.get_revision(
            knowledge_id,
            expected_revision,
            tenant_id=tenant_id,
        )
        candidate = self._require_candidate(replacement_candidate_id, tenant_id)
        if current is None:
            raise ValueError("knowledge item not found")
        decision = self._state_decision(candidate, PromotionDecisionType.SUPERSEDE, "superseded_by_correction")
        self.repository.save_promotion_decision(decision, tenant_id)
        state = transition_lifecycle_state(current.state, LifecycleStatus.SUPERSEDED)
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
        self._persist_revision_with_projection(
            revision,
            candidate_id=candidate.id,
            decision_ref=decision.decision_id,
            expected_parent_revision=expected_revision,
        )
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

    def reconcile_source_lifecycle(
        self,
        *,
        provenance_ref: str,
        tenant_id: str | None = None,
        active_candidate_ids: set[str] | None = None,
        source_stale: bool = False,
    ) -> list[KnowledgeRevision]:
        """Retire candidates and promoted knowledge no longer backed by a live source."""
        tenant_id = self._resolve_tenant(tenant_id)
        matching_candidates: list[KnowledgeCandidate] = []
        for candidate in self.repository.list_candidates(tenant_id, limit=None):
            evidence_refs = {ref for evidence in candidate.evidence.items for ref in evidence.provenance_refs}
            if provenance_ref not in set(candidate.provenance_refs).union(evidence_refs):
                continue
            if not source_stale and active_candidate_ids is not None and candidate.id in active_candidate_ids:
                continue
            matching_candidates.append(candidate)

        if any(candidate.kind == KnowledgeKind.SIGNAL_MAPPING for candidate in matching_candidates):
            self._signal_store()
        try:
            with self.repository.transaction():
                retired_candidates: list[KnowledgeCandidate] = []
                for observed in matching_candidates:
                    current_candidate = self.repository.get_candidate(observed.id, tenant_id)
                    if current_candidate is None:
                        continue
                    if current_candidate != observed:
                        logger.info(
                            "source_lifecycle_transition_skipped",
                            tenant_id=tenant_id,
                            candidate_id=observed.id,
                            reason="source_generation_advanced",
                            observed_updated_at=observed.updated_at.isoformat(),
                            current_updated_at=current_candidate.updated_at.isoformat(),
                        )
                        continue
                    if observed.state.lifecycle_status == LifecycleStatus.STALE:
                        # A previous split write may have retired the candidate but
                        # not its authority revision. Reconcile it idempotently.
                        retired_candidates.append(observed)
                        continue
                    if observed.state.lifecycle_status != LifecycleStatus.ACTIVE:
                        continue
                    state = transition_lifecycle_state(observed.state, LifecycleStatus.STALE)
                    updated = observed.model_copy(update={"state": state, "updated_at": utc_now()})
                    try:
                        self.repository.transition_candidate_lifecycle(updated, expected=observed)
                    except CandidateLifecycleConflictError:
                        current = self.repository.get_candidate(observed.id, tenant_id)
                        logger.info(
                            "source_lifecycle_transition_skipped",
                            tenant_id=tenant_id,
                            candidate_id=observed.id,
                            reason="candidate_state_advanced",
                            winning_review_state=(current.state.review_state.value if current else "missing"),
                            winning_lifecycle_status=(current.state.lifecycle_status.value if current else "missing"),
                        )
                        continue
                    retired_candidates.append(updated)

                return self._reconcile_removed_candidates(
                    retired_candidates,
                    lifecycle_status=LifecycleStatus.STALE,
                    reason="source_stale" if source_stale else "source_changed",
                )
        except Exception:
            logger.error(
                "source_lifecycle_reconciliation_failed",
                tenant_id=tenant_id,
                provenance_ref=provenance_ref,
                candidate_count=len(matching_candidates),
                source_stale=source_stale,
                exc_info=True,
            )
            raise

    def _reconcile_removed_candidates(
        self,
        removed_candidates: list[KnowledgeCandidate],
        *,
        lifecycle_status: LifecycleStatus,
        reason: str,
    ) -> list[KnowledgeRevision]:
        """Recompute promoted knowledge after support is removed."""
        if not removed_candidates:
            return []
        tenant_id = removed_candidates[0].tenant_id
        lifecycle_revisions: list[KnowledgeRevision] = []
        retired_ids = {candidate.id for candidate in removed_candidates}
        for current in self.repository.list_current_revisions(tenant_id):
            matching_ids = retired_ids.intersection(current.promoted_from_candidate_refs)
            if not matching_ids or current.state.lifecycle_status != LifecycleStatus.ACTIVE:
                continue
            candidate = next(candidate for candidate in removed_candidates if candidate.id in matching_ids)
            surviving_candidates = self.corroboration.reviewed_candidates(
                tenant_id,
                current.proposition.proposition_key,
            )
            if surviving_candidates:
                supported_revision = None
                for survivor in surviving_candidates:
                    correction = self.repository.get_correction_for_candidate(survivor.id, tenant_id)
                    _, supported_revision = self.evaluate_candidate(
                        survivor.id,
                        tenant_id=tenant_id,
                        authoritative_source=survivor.policy.authoritative_source,
                        live_verified=survivor.policy.live_verified,
                        _correction_id=correction.id if correction is not None else None,
                    )
                    if supported_revision is not None:
                        lifecycle_revisions.append(supported_revision)
                        break
                if supported_revision is not None:
                    continue
            lifecycle_revisions.append(
                self._retire_knowledge(
                    current,
                    candidate,
                    lifecycle_status=lifecycle_status,
                    reason=reason,
                )
            )
        return lifecycle_revisions

    def _retire_knowledge(
        self,
        current: KnowledgeRevision,
        candidate: KnowledgeCandidate,
        *,
        lifecycle_status: LifecycleStatus,
        reason: str,
    ) -> KnowledgeRevision:
        decision = self._state_decision(candidate, PromotionDecisionType.EXPIRE, reason)
        self.repository.save_promotion_decision(decision, current.tenant_id)
        state = transition_lifecycle_state(current.state, lifecycle_status)
        revision = current.model_copy(
            update={
                "revision": current.revision + 1,
                "parent_revision": current.revision,
                "state": state,
                "policy_id": decision.policy_id,
                "policy_version": decision.policy_version,
                "decision_ref": decision.decision_id,
                "revision_reason": reason,
                "semantic_fingerprint": stable_fingerprint(
                    [current.semantic_fingerprint, lifecycle_status.value, reason]
                ),
                "created_at": utc_now(),
            }
        )
        self._persist_revision_with_projection(
            revision,
            candidate_id=candidate.id,
            decision_ref=decision.decision_id,
        )
        self.repository.append_event(
            "knowledge_retired",
            tenant_id=current.tenant_id,
            subject_ref=current.knowledge_id,
            dimensions={
                "knowledge_kind": current.proposition.kind.value,
                "lifecycle_status": lifecycle_status.value,
                "reason_code": reason,
            },
            payload={"candidate_id": candidate.id, "revision": revision.revision},
        )
        return revision

    def _signal_store(self):
        if self._signal_store_instance is None:
            from tacit.signals.store import SignalStore

            self._signal_store_instance = SignalStore(
                self.repository._db_path,
                runtime_settings=self._runtime_settings,
            )
        return self._signal_store_instance

    def _repair_signal_mapping_projection(
        self,
        revision: KnowledgeRevision,
        *,
        expected_semantic_fingerprint: str,
    ) -> KnowledgeRevision:
        """Repair an idempotent projection from the current revision under a write lock."""
        if revision.proposition.kind != KnowledgeKind.SIGNAL_MAPPING:
            return revision
        store = self._signal_store()
        with self.repository.transaction() as conn:
            current = self.repository.get_revision(revision.knowledge_id, tenant_id=revision.tenant_id)
            if current is None:
                raise KnowledgeRevisionConflictError("knowledge item disappeared while repairing its projection")
            if current.semantic_fingerprint != expected_semantic_fingerprint:
                raise KnowledgeRevisionConflictError(
                    f"knowledge item advanced from revision {revision.revision} to {current.revision}; "
                    "reload before repairing its projection"
                )
            self._sync_signal_mapping_state(current, store=store, connection=conn)
            logger.info(
                "governed_signal_projection_reconciled",
                tenant_id=current.tenant_id,
                knowledge_id=current.knowledge_id,
                requested_revision=revision.revision,
                authoritative_revision=current.revision,
            )
            return current

    def _persist_revision_with_projection(
        self,
        revision: KnowledgeRevision,
        *,
        candidate_id: str,
        decision_ref: str,
        expected_candidate: KnowledgeCandidate | None = None,
        expected_contributors: list[KnowledgeCandidate] | None = None,
        expected_parent_revision: int | None = None,
    ) -> KnowledgeRevision:
        store = self._signal_store() if revision.proposition.kind == KnowledgeKind.SIGNAL_MAPPING else None
        try:
            with self.repository.transaction() as conn:
                persisted = self.repository.persist_revision(
                    revision,
                    candidate_id=candidate_id,
                    decision_ref=decision_ref,
                    expected_candidate=expected_candidate,
                    expected_contributors=expected_contributors,
                    expected_parent_revision=expected_parent_revision,
                )
                self._sync_signal_mapping_state(persisted, store=store, connection=conn)
        except Exception:
            if store is not None:
                logger.error(
                    "governed_signal_projection_transaction_failed",
                    tenant_id=revision.tenant_id,
                    knowledge_id=revision.knowledge_id,
                    knowledge_revision=revision.revision,
                    exc_info=True,
                )
            raise
        return persisted

    def _sync_signal_mapping_state(
        self,
        revision: KnowledgeRevision,
        *,
        store: Any | None = None,
        connection: Any | None = None,
    ) -> None:
        """Project governed signal eligibility into the legacy resolver index."""
        if revision.proposition.kind != KnowledgeKind.SIGNAL_MAPPING:
            return
        store = store or self._signal_store()
        deactivated = store.deactivate_governed_mappings(
            tenant_id=revision.tenant_id,
            governance_ref=revision.knowledge_id,
            connection=connection,
        )
        signal_type = revision.proposition.concept_ref.removeprefix("signal:")
        metric_patterns = self._signal_metric_patterns(revision)
        active = (
            revision.state.lifecycle_status == LifecycleStatus.ACTIVE
            and revision.state.eligibility != KnowledgeEligibility.INELIGIBLE
        )
        if not active:
            logger.info(
                "governed_signal_projection_deactivated",
                tenant_id=revision.tenant_id,
                knowledge_id=revision.knowledge_id,
                knowledge_revision=revision.revision,
                mapping_count=deactivated,
                lifecycle_status=revision.state.lifecycle_status.value,
                eligibility=revision.state.eligibility.value,
            )
            return
        if not signal_type or not metric_patterns:
            raise ValueError("active governed signal mapping requires a signal and exact metric pattern")
        review_state = (
            revision.state.review_state.value
            if active and revision.state.review_state in {ReviewState.APPROVED, ReviewState.TRUSTED}
            else ReviewState.CANDIDATE.value
        )
        for metric_pattern in metric_patterns:
            store.add_mapping(
                signal_type,
                metric_pattern,
                confidence=self._signal_mapping_confidence(revision, metric_pattern),
                context_services=self._resolver_scope_values(
                    revision.scope.service_refs,
                    "entity:service:",
                ),
                context_environments=self._resolver_scope_values(
                    revision.scope.environment_refs,
                    "environment:",
                ),
                context_datasource_types=self._signal_mapping_payload_values(
                    revision,
                    "context_datasource_types",
                ),
                context_archetypes=self._resolver_scope_values(
                    revision.scope.archetype_refs,
                    "archetype:",
                ),
                context_regions=self._resolver_scope_values(revision.scope.region_refs, "region:"),
                context_clusters=self._resolver_scope_values(revision.scope.cluster_refs, "cluster:"),
                context_namespaces=self._resolver_scope_values(revision.scope.namespace_refs, "namespace:"),
                context_versions=self._resolver_scope_values(revision.scope.version_constraints, "version:"),
                valid_from=revision.scope.valid_from.timestamp() if revision.scope.valid_from else None,
                valid_until=revision.scope.valid_until.timestamp() if revision.scope.valid_until else None,
                source_type="operational_knowledge",
                source_refs=[
                    f"{revision.knowledge_id}@{revision.revision}",
                    *revision.provenance_refs,
                ],
                governance_ref=revision.knowledge_id,
                governance_revision=revision.revision,
                inference_version=f"{revision.policy_id}:{revision.policy_version}",
                review_state=review_state,
                tenant_id=revision.tenant_id,
                connection=connection,
                replace_existing=True,
                increment_use_count=False,
            )

    def _signal_metric_patterns(self, revision: KnowledgeRevision) -> list[str]:
        """Read resolver-exact patterns frozen into the immutable revision."""
        return sorted(
            {
                str(mapping.get("metric_pattern") or "").strip()
                for mapping in revision.resolver_payload.get("mappings", [])
                if str(mapping.get("metric_pattern") or "").strip()
            }
        )

    def _signal_mapping_payload_values(self, revision: KnowledgeRevision, field: str) -> list[str]:
        values: set[str] = set()
        for mapping in revision.resolver_payload.get("mappings", []):
            raw_values = mapping.get(field, [])
            if isinstance(raw_values, list):
                values.update(str(value).strip() for value in raw_values if str(value).strip())
        return sorted(values)

    def _signal_mapping_confidence(self, revision: KnowledgeRevision, metric_pattern: str) -> float:
        confidences = []
        for mapping in revision.resolver_payload.get("mappings", []):
            if str(mapping.get("metric_pattern") or "").strip() != metric_pattern:
                continue
            try:
                confidence = float(mapping.get("confidence", 0.5))
            except (TypeError, ValueError):
                confidence = 0.5
            confidences.append(max(0.0, min(1.0, confidence)))
        return max(confidences, default=0.5)

    @staticmethod
    def _build_signal_mapping_resolver_payload(
        candidate: KnowledgeCandidate,
        contributors: list[KnowledgeCandidate],
    ) -> dict[str, Any]:
        """Freeze exact resolver inputs so revisions never depend on mutable candidates."""
        mappings: dict[str, dict[str, Any]] = {}
        for contributor in contributors or [candidate]:
            pattern = (
                str(
                    contributor.typed_payload.get("metric_pattern")
                    or contributor.typed_payload.get("candidate_metric")
                    or contributor.typed_payload.get("metric")
                    or contributor.typed_payload.get("object_ref")
                    or contributor.proposition.object_ref
                    or ""
                )
                .removeprefix("concept:")
                .strip()
            )
            if not pattern:
                continue
            try:
                confidence = float(contributor.typed_payload.get("confidence", 0.5))
            except (TypeError, ValueError):
                confidence = 0.5
            confidence = max(0.0, min(1.0, confidence))
            datasource_types = contributor.typed_payload.get("context_datasource_types", [])
            if not isinstance(datasource_types, list):
                datasource_types = []
            existing = mappings.setdefault(
                pattern,
                {
                    "metric_pattern": pattern,
                    "confidence": confidence,
                    "context_datasource_types": [],
                },
            )
            existing["confidence"] = max(float(existing["confidence"]), confidence)
            existing["context_datasource_types"] = sorted(
                {
                    *existing["context_datasource_types"],
                    *[str(value).strip() for value in datasource_types if str(value).strip()],
                }
            )
        return {"mappings": [mappings[key] for key in sorted(mappings)]}

    @staticmethod
    def _resolver_scope_values(refs: list[str], prefix: str) -> list[str]:
        values = {
            value.removeprefix(prefix) for value in refs if value and (value.startswith(prefix) or ":" not in value)
        }
        return sorted(values)

    def impact(self, knowledge_id: str, tenant_id: str | None = None) -> KnowledgeImpact:
        tenant_id = self._resolve_tenant(tenant_id)
        usage = self.repository.list_usage(tenant_id=tenant_id, knowledge_id=knowledge_id)
        seen = set()
        affected = []
        for item in usage:
            if item.disposition != KnowledgeUsageDisposition.APPLIED:
                continue
            key = (item.investigation_id, item.investigation_revision)
            if key in seen:
                continue
            seen.add(key)
            affected.append({"investigation_id": key[0], "revision": key[1]})
        return KnowledgeImpact(knowledge_ref=knowledge_id, affected_investigations=affected)

    def explain(self, knowledge_id: str, tenant_id: str | None = None) -> dict[str, Any]:
        tenant_id = self._resolve_tenant(tenant_id)
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

    def _require_candidate_workflow(
        self,
        candidate_id: str,
        tenant_id: str,
        correction_id: str | None,
    ) -> None:
        correction = self.repository.get_correction_for_candidate(candidate_id, tenant_id)
        if correction is not None and correction.id != correction_id:
            raise PermissionError("correction candidates must be reviewed through the correction workflow")

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
            if item.state.review_state in {ReviewState.APPROVED, ReviewState.TRUSTED}
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
        if correction_type == CorrectionType.SIGNAL_MEANING:
            return KnowledgeKind.SIGNAL_MAPPING
        if correction_type == CorrectionType.ENTITY_MAPPING:
            return KnowledgeKind.ARTIFACT_QUALITY
        if correction_type in {CorrectionType.MISSING_CHECK, CorrectionType.OBSERVATION_DISPUTE}:
            return KnowledgeKind.EVIDENCE_REQUIREMENT
        return KnowledgeKind(proposed.get("kind", KnowledgeKind.ARTIFACT_QUALITY.value))

    @staticmethod
    def _entity_mapping_values(proposed: dict[str, Any]) -> tuple[str, str]:
        raw_value = str(proposed.get("raw_value") or proposed.get("alias") or proposed.get("subject_ref") or "").strip()
        entity_ref = str(proposed.get("entity_ref") or proposed.get("object_ref") or "").strip()
        if not raw_value or not entity_ref:
            raise ValueError("entity_mapping correction requires raw_value and entity_ref")
        return raw_value, entity_ref

    @classmethod
    def _entity_mapping_alias(cls, correction: KnowledgeCorrection) -> EntityAlias:
        raw_value, entity_ref = cls._entity_mapping_values(correction.proposed)
        return EntityAlias(
            id=_id(
                "alias",
                [
                    correction.tenant_id,
                    normalize_entity(raw_value),
                    entity_ref,
                    correction.scope.model_dump(mode="json"),
                ],
            ),
            tenant_id=correction.tenant_id,
            raw_value=raw_value,
            normalized_value=normalize_entity(raw_value),
            entity_ref=entity_ref,
            scope=correction.scope,
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=[f"prov_{correction.id}"],
        )

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
        return candidate_ref_from_entity_ref(entity_ref)

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
        if revision.proposition.kind == KnowledgeKind.DEPENDENCY:
            subject_ref = normalize_service_ref(revision.proposition.subject_ref)
            service_refs = {normalize_service_ref(value) for value in scope.service_refs}
            if subject_ref not in service_refs:
                return KnowledgeUsageDisposition.REJECTED_BY_SCOPE, ["dependency_subject_mismatch"]
        conflicts = self.repository.list_conflicts(
            revision.tenant_id,
            proposition_key=revision.proposition.proposition_key,
            unresolved_only=True,
        )
        active_propositions = {row["proposition_key"] for row in self.repository.list_propositions(revision.tenant_id)}
        conflicts = [
            conflict
            for conflict in conflicts
            if conflict.left_proposition_ref in active_propositions
            and conflict.right_proposition_ref in active_propositions
            and (
                conflict.conflict_kind == ConflictKind.DIRECT_NEGATION
                or (
                    revision.proposition.kind == KnowledgeKind.OWNERSHIP
                    and conflict.conflict_kind == ConflictKind.COMPETING_OWNER
                )
                or (
                    revision.proposition.kind == KnowledgeKind.SIGNAL_MAPPING
                    and conflict.conflict_kind == ConflictKind.COMPETING_SIGNAL_MAPPING
                )
            )
        ]
        if conflicts:
            return KnowledgeUsageDisposition.REJECTED_BY_CONFLICT, ["unresolved_conflict"]
        return KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED, ["eligible_under_recorded_policy"]

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
    return KnowledgeService(get_knowledge_repository(), runtime_settings=settings)
