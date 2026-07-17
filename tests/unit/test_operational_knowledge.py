from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tacit.api.app import create_app
from tacit.cli import cli
from tacit.config import Settings
from tacit.knowledge.enums import (
    ConflictResolutionStatus,
    EntityBindingMethod,
    EntityKind,
    EvidenceRole,
    KnowledgeEligibility,
    KnowledgeKind,
    LifecycleStatus,
    LineageKind,
    ReviewState,
    SourceFamily,
)
from tacit.knowledge.migration import migrate_artifact_extractions
from tacit.knowledge.models import (
    Entity,
    EntityAlias,
    KnowledgeEvidenceReference,
    KnowledgeScope,
    KnowledgeState,
)
from tacit.knowledge.normalization import normalize_service_ref
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.models.schemas import CulpritRanking, EvidenceObservation, EvidenceObservationOutcome
from tacit.operational_learning_benchmark import (
    load_operational_learning_corpus,
    run_operational_learning_benchmark,
)


def _service(tmp_path: Path, tenant_id: str = "default") -> KnowledgeService:
    service = KnowledgeService(KnowledgeRepository(tmp_path / "knowledge.db"))
    scope = KnowledgeScope(tenant_id=tenant_id)
    for entity in (
        Entity(
            id="entity:service:checkout",
            tenant_id=tenant_id,
            kind=EntityKind.SERVICE,
            canonical_name="checkout",
            scope=scope,
            provenance_refs=["catalog:service"],
        ),
        Entity(
            id="entity:datastore:redis-session",
            tenant_id=tenant_id,
            kind=EntityKind.DATASTORE,
            canonical_name="redis-session",
            scope=scope,
            provenance_refs=["catalog:datastore"],
        ),
    ):
        service.register_entity(entity)
    return service


def _dependency(
    service: KnowledgeService,
    *,
    payload_ref: str,
    family: SourceFamily,
    lineage_group: str,
    lineage_kind: LineageKind = LineageKind.INDEPENDENT,
    tenant_id: str = "default",
    predicate: str = "depends_on",
    object_ref: str = "entity:datastore:redis-session",
    version_constraints: list[str] | None = None,
):
    scope = KnowledgeScope(
        tenant_id=tenant_id,
        environment_refs=["environment:production"],
        service_refs=["entity:service:checkout"],
        version_constraints=version_constraints or [],
    )
    return service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref=payload_ref,
        typed_payload={"semantic": "unchanged"},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": predicate,
            "object_ref": object_ref,
        },
        scope=scope,
        evidence=[
            KnowledgeEvidenceReference(
                evidence_ref=f"evidence:{payload_ref}",
                evidence_role=EvidenceRole.SUPPORTING,
                source_family=family,
                lineage_group=lineage_group,
                lineage_kind=lineage_kind,
                provenance_refs=[f"provenance:{payload_ref}"],
            )
        ],
        provenance_refs=[f"provenance:{payload_ref}"],
        tenant_id=tenant_id,
    )


def _promoted_dependency(
    service: KnowledgeService,
    tenant_id: str = "default",
    *,
    version_constraints: list[str] | None = None,
):
    first = _dependency(
        service,
        payload_ref="runbook",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:1",
        tenant_id=tenant_id,
        version_constraints=version_constraints,
    )
    second = _dependency(
        service,
        payload_ref="dashboard",
        family=SourceFamily.DASHBOARD,
        lineage_group="dashboard:1",
        tenant_id=tenant_id,
        version_constraints=version_constraints,
    )
    service.review_candidate(first.id, approved=True, reviewer="reviewer", tenant_id=tenant_id)
    service.review_candidate(second.id, approved=True, reviewer="reviewer", tenant_id=tenant_id)
    decision, revision = service.evaluate_candidate(first.id, tenant_id=tenant_id)
    assert decision.decision.value == "promote"
    assert revision is not None
    return first, revision


def test_state_invariants_reject_unsafe_combinations():
    with pytest.raises(ValidationError, match="rejected knowledge must be ineligible"):
        KnowledgeState(
            review_state=ReviewState.REJECTED,
            eligibility=KnowledgeEligibility.CONTEXTUAL_ONLY,
        )
    with pytest.raises(ValidationError, match="superseded knowledge must be ineligible"):
        KnowledgeState(
            lifecycle_status=LifecycleStatus.SUPERSEDED,
            eligibility=KnowledgeEligibility.CONTEXTUAL_ONLY,
        )


def test_resolution_normalization_corroboration_and_promotion(tmp_path: Path):
    service = _service(tmp_path)
    first, revision = _promoted_dependency(service)
    candidates = service.repository.candidates_for_proposition("default", first.proposition.proposition_key)
    assert len(candidates) == 2
    assert first.entity_resolution.status.value == "resolved"
    assert revision.revision == 1
    assert revision.policy_id == "dependency-promotion-v1"
    assert revision.policy_version == "1"
    assert revision.state.eligibility == KnowledgeEligibility.CONTEXTUAL_ONLY
    assert service.repository.get_revision(revision.knowledge_id, 1) == revision

    snapshot_a, usage_a = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )
    snapshot_b, _ = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )
    assert snapshot_a.id == snapshot_b.id
    assert snapshot_a.items[0].revision == 1
    assert usage_a[0].disposition.value == "applied"
    contradicted = service.reconcile_live_observations(
        usage_a,
        [
            EvidenceObservation(
                requirement_id="redis_health",
                resolution_metric="redis-session",
                outcome=EvidenceObservationOutcome.NEGATIVE_EVIDENCE,
            )
        ],
    )
    assert contradicted[0].disposition.value == "contradicted_by_observation"
    assert contradicted[0].score_delta == 0
    reconciled_snapshot = service.snapshot_from_usage("default", contradicted)
    assert reconciled_snapshot.items == []
    assert reconciled_snapshot.id != snapshot_a.id


def test_knowledge_candidate_clears_empty_ranking_abstention(tmp_path: Path):
    service = _service(tmp_path)
    _promoted_dependency(service)
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    ranking = service.apply_to_ranking(
        CulpritRanking(abstained=True, abstention_reason="no_rankable_candidates"),
        usage,
    )

    assert len(ranking.candidates) == 1
    assert ranking.abstained is False
    assert ranking.abstention_reason == ""


def test_duplicate_lineage_does_not_inflate_corroboration(tmp_path: Path):
    service = _service(tmp_path)
    first = _dependency(
        service,
        payload_ref="copy-a",
        family=SourceFamily.RUNBOOK,
        lineage_group="same-document",
    )
    second = _dependency(
        service,
        payload_ref="copy-b",
        family=SourceFamily.RUNBOOK,
        lineage_group="same-document",
    )
    copied = second.evidence.items[0].model_copy(update={"lineage_kind": LineageKind.COPIED_FROM})
    second = second.model_copy(update={"evidence": second.evidence.model_copy(update={"items": [copied]})})
    service.repository.save_candidate(second)
    service.review_candidate(first.id, approved=True, reviewer="reviewer")
    service.review_candidate(second.id, approved=True, reviewer="reviewer")
    summary, _ = service.corroboration.analyze("default", first.proposition.proposition_key)
    assert summary.raw_source_count == 2
    assert summary.independent_source_count == 1
    assert summary.duplicate_source_count == 1


def test_rejected_and_pending_candidates_do_not_corroborate(tmp_path: Path):
    service = _service(tmp_path)
    first = _dependency(
        service,
        payload_ref="approved-runbook",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:approved",
    )
    rejected = _dependency(
        service,
        payload_ref="rejected-dashboard",
        family=SourceFamily.DASHBOARD,
        lineage_group="dashboard:rejected",
    )
    _dependency(
        service,
        payload_ref="pending-incident",
        family=SourceFamily.INCIDENT,
        lineage_group="incident:pending",
    )
    service.review_candidate(first.id, approved=True, reviewer="reviewer")
    service.review_candidate(rejected.id, approved=False, reviewer="reviewer")

    decision, revision = service.evaluate_candidate(first.id)

    assert revision is None
    assert decision.decision.value == "retain_candidate"
    assert decision.resulting_eligibility == KnowledgeEligibility.INELIGIBLE
    assert decision.reason_codes == ["insufficient_independent_sources"]


def test_scope_matching_requires_version_constraints(tmp_path: Path):
    service = _service(tmp_path)
    _promoted_dependency(service, version_constraints=["version:2026.07"])

    _, usage_without_version = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )
    _, usage_with_version = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
            version_constraints=["version:2026.07"],
        )
    )

    assert usage_without_version[0].disposition.value == "rejected_by_scope"
    assert usage_with_version[0].disposition.value == "applied"


def test_proposition_keys_canonicalize_scope_list_order(tmp_path: Path):
    service = _service(tmp_path)
    first = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="scope-a",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout", "entity:service:api"],
        ),
        evidence=[
            KnowledgeEvidenceReference(
                evidence_ref="evidence:scope-a",
                evidence_role=EvidenceRole.SUPPORTING,
                source_family=SourceFamily.RUNBOOK,
                lineage_group="scope-a",
                lineage_kind=LineageKind.INDEPENDENT,
                provenance_refs=["provenance:scope-a"],
            )
        ],
        provenance_refs=["provenance:scope-a"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="scope-b",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:api", "entity:service:checkout"],
        ),
        evidence=[
            KnowledgeEvidenceReference(
                evidence_ref="evidence:scope-b",
                evidence_role=EvidenceRole.SUPPORTING,
                source_family=SourceFamily.DASHBOARD,
                lineage_group="scope-b",
                lineage_kind=LineageKind.INDEPENDENT,
                provenance_refs=["provenance:scope-b"],
            )
        ],
        provenance_refs=["provenance:scope-b"],
    )

    assert first.proposition.proposition_key == second.proposition.proposition_key


def test_direct_negation_conflicts_require_matching_objects(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:datastore:postgres",
            kind=EntityKind.DATASTORE,
            tenant_id="default",
            canonical_name="postgres",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:datastore"],
        )
    )
    positive = _dependency(
        service,
        payload_ref="depends-redis",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:redis",
        object_ref="entity:datastore:redis-session",
    )
    _dependency(
        service,
        payload_ref="not-postgres",
        family=SourceFamily.DASHBOARD,
        lineage_group="dashboard:postgres",
        predicate="does_not_depend_on",
        object_ref="entity:datastore:postgres",
    )

    conflicts = service.conflicts.analyze("default", positive.proposition.proposition_key)

    assert conflicts == []


def test_positive_dependencies_with_different_objects_do_not_conflict(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:datastore:postgres",
            kind=EntityKind.DATASTORE,
            tenant_id="default",
            canonical_name="postgres",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:datastore"],
        )
    )
    redis = _dependency(
        service,
        payload_ref="depends-redis",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:redis",
    )
    _dependency(
        service,
        payload_ref="depends-postgres",
        family=SourceFamily.DASHBOARD,
        lineage_group="dashboard:postgres",
        object_ref="entity:datastore:postgres",
    )

    assert service.conflicts.analyze("default", redis.proposition.proposition_key) == []


def test_rejected_propositions_do_not_create_conflicts(tmp_path: Path):
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="accepted-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    rejected = _dependency(
        service,
        payload_ref="rejected-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
    )
    service.review_candidate(positive.id, approved=True, reviewer="reviewer")
    service.review_candidate(rejected.id, approved=False, reviewer="reviewer")

    assert service.conflicts.analyze("default", positive.proposition.proposition_key) == []


def test_rejecting_last_candidate_resolves_existing_conflicts(tmp_path: Path):
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="accepted-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    rejected = _dependency(
        service,
        payload_ref="rejected-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
    )
    conflicts = service.conflicts.analyze("default", positive.proposition.proposition_key)
    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.UNRESOLVED

    service.review_candidate(rejected.id, approved=False, reviewer="operator")

    assert service.repository.list_conflicts("default", unresolved_only=True) == []
    resolved = service.repository.list_conflicts("default")
    assert resolved[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_REVIEW
    assert resolved[0].resolution_reason == "counter_proposition_rejected"


def test_conflict_scope_analysis_includes_services(tmp_path: Path):
    service = _service(tmp_path)
    first = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="checkout-signal",
        typed_payload={},
        proposition={
            "subject_ref": "concept:latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["catalog:checkout"],
    )
    service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="payment-signal",
        typed_payload={},
        proposition={
            "subject_ref": "concept:latency",
            "predicate": "represented_by",
            "object_ref": "concept:payment_latency_seconds",
        },
        scope=KnowledgeScope(service_refs=["entity:service:payment"]),
        provenance_refs=["catalog:payment"],
    )

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_SCOPE
    assert conflicts[0].scope_analysis["reason_code"] == "service_specific_difference"


def test_canonical_entity_names_use_resolver_normalization(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:service:payment-api",
            kind=EntityKind.SERVICE,
            tenant_id="default",
            canonical_name="Payment API",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:service"],
        )
    )

    candidate = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="canonical-name",
        typed_payload={},
        proposition={
            "subject_ref": "Payment API",
            "predicate": "depends_on",
            "object_ref": "redis-session",
        },
        scope=KnowledgeScope(service_refs=["entity:service:payment-api"]),
        provenance_refs=["catalog:test"],
    )

    assert candidate.entity_resolution.status.value == "resolved"
    assert candidate.proposition.subject_ref == "entity:service:payment-api"


def test_candidate_can_rebind_after_entity_resolution_is_repaired(tmp_path: Path):
    service = _service(tmp_path)
    kwargs = {
        "kind": KnowledgeKind.DEPENDENCY,
        "payload_ref": "runbook:payment-api",
        "typed_payload": {"source": "payment-api"},
        "proposition": {
            "subject_ref": "Payment API",
            "predicate": "depends_on",
            "object_ref": "redis-session",
        },
        "scope": KnowledgeScope(service_refs=["entity:service:payment-api"]),
        "provenance_refs": ["runbook:payment-api"],
        "candidate_id": "kc_payment_api",
    }
    unresolved = service.create_candidate(**kwargs)
    old_key = unresolved.proposition.proposition_key
    assert unresolved.entity_resolution.status.value == "unresolved"
    service.register_entity(
        Entity(
            id="entity:service:payment-api",
            kind=EntityKind.SERVICE,
            canonical_name="Payment API",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:service"],
        )
    )

    repaired = service.create_candidate(**kwargs)

    assert repaired.id == unresolved.id
    assert repaired.entity_resolution.status.value == "resolved"
    assert repaired.proposition.proposition_key != old_key
    assert service.repository.candidates_for_proposition("default", old_key) == []
    assert service.repository.candidates_for_proposition("default", repaired.proposition.proposition_key) == [repaired]


def test_migrated_dependency_scope_uses_source_service(tmp_path: Path):
    service = _service(tmp_path)
    created = migrate_artifact_extractions(
        artifact_id="artifact-1",
        artifact_type="runbook",
        rows={
            "dependency_hints": [
                {
                    "id": "dep-1",
                    "source_entity": "checkout",
                    "target_entity": "redis-session",
                    "direction": "depends_on",
                    "source_excerpt": "checkout depends on redis-session",
                }
            ]
        },
        service=service,
    )

    candidate = service.repository.get_candidate(created[0])

    assert candidate is not None
    assert candidate.scope.service_refs == ["entity:service:checkout"]


def test_service_scope_normalization_matches_governed_knowledge(tmp_path: Path):
    governed_scope = KnowledgeScope(
        service_refs=["entity:service:checkout-service"],
    )
    investigation_scope = KnowledgeScope(service_refs=[normalize_service_ref("Checkout Service")])

    assert normalize_service_ref("Checkout Service") == "entity:service:checkout-service"
    assert governed_scope.applies_to(investigation_scope)


def test_migrated_signal_mapping_preserves_candidate_metric(tmp_path: Path):
    service = _service(tmp_path)
    created = migrate_artifact_extractions(
        artifact_id="artifact-signals",
        artifact_type="dashboard",
        rows={
            "signal_mapping_candidates": [
                {
                    "id": "signal-1",
                    "source": "checkout latency",
                    "signal_type": "latency",
                    "candidate_metric": "http_request_duration_seconds",
                    "source_excerpt": "Latency uses the request duration histogram",
                }
            ]
        },
        service=service,
    )

    candidate = service.repository.get_candidate(created[0])
    assert candidate is not None
    assert candidate.proposition.object_ref == "concept:http_request_duration_seconds"


def test_signal_mapping_usage_does_not_claim_unapplied_score_delta(tmp_path: Path):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:latency",
        typed_payload={"metric": "http_request_duration_seconds"},
        proposition={
            "subject_ref": "concept:latency",
            "predicate": "represented_by",
            "object_ref": "concept:http_request_duration_seconds",
            "concept_ref": "signal:latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None

    _, usage = service.create_snapshot(KnowledgeScope(service_refs=["entity:service:checkout"]))

    assert usage[0].disposition.value == "applied"
    assert usage[0].used_for == ["evidence_resolution"]
    assert usage[0].score_delta == 0


def test_migrated_ownership_scope_uses_owned_service(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:team:payments",
            kind=EntityKind.TEAM,
            canonical_name="payments",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:team"],
        )
    )
    created = migrate_artifact_extractions(
        artifact_id="artifact-ownership",
        artifact_type="runbook",
        rows={
            "ownership_hints": [
                {
                    "id": "owner-1",
                    "entity": "checkout",
                    "owner": "payments",
                    "source_excerpt": "checkout is owned by payments",
                    "review_state": "approved",
                }
            ]
        },
        service=service,
    )

    candidate = service.repository.get_candidate(created[0])
    assert candidate is not None
    assert candidate.scope.service_refs == ["entity:service:checkout"]
    decision, revision = service.evaluate_candidate(candidate.id, authoritative_source=True)
    assert decision.decision.value == "promote"
    assert revision is not None


def test_correction_creates_candidate_revision_and_impact(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    scope = KnowledgeScope(
        environment_refs=["environment:production"],
        service_refs=["entity:service:checkout"],
    )
    correction, candidate = service.create_correction(
        investigation_id="inv_1",
        investigation_revision=1,
        correction_type="dependency",
        target_ref=original.knowledge_id,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=scope,
        explanation="The production path changed.",
        created_by="operator",
    )
    reviewed, replacement = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )
    assert reviewed.review_state == ReviewState.APPROVED
    assert candidate.id == correction.knowledge_candidate_ref
    assert replacement is not None
    assert service.repository.get_revision(original.knowledge_id).state.lifecycle_status == LifecycleStatus.SUPERSEDED
    assert service.repository.get_revision(original.knowledge_id, 1) == original
    assert service.impact(original.knowledge_id).recommended_action == "replay_current"


def test_correction_without_target_keeps_conflict_unresolved(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, _ = service.create_correction(
        investigation_id="inv_no_target",
        investigation_revision=1,
        correction_type="dependency",
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        ),
        explanation="The relationship is disputed, but no replacement target was selected.",
        created_by="operator",
    )

    _, replacement = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )

    conflicts = service.repository.list_conflicts("default", unresolved_only=True)
    assert replacement is None
    assert len(conflicts) == 1
    assert service.repository.get_revision(original.knowledge_id).state.lifecycle_status == LifecycleStatus.ACTIVE


def test_correction_does_not_supersede_an_unrelated_target(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    service.register_entity(
        Entity(
            id="entity:datastore:postgres",
            kind=EntityKind.DATASTORE,
            canonical_name="postgres",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:datastore"],
        )
    )
    correction, _ = service.create_correction(
        investigation_id="inv-mistargeted",
        investigation_revision=1,
        correction_type="dependency",
        target_ref=original.knowledge_id,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:postgres",
        },
        scope=KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        ),
        explanation="Add a separate dependency without replacing Redis.",
        created_by="operator",
    )

    _, added = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )

    assert added is not None
    assert added.knowledge_id != original.knowledge_id
    assert service.repository.get_revision(original.knowledge_id).state.lifecycle_status == LifecycleStatus.ACTIVE


def test_duplicate_correction_submission_preserves_review_state(tmp_path: Path):
    service = _service(tmp_path)
    kwargs = {
        "investigation_id": "inv-duplicate",
        "investigation_revision": 1,
        "correction_type": "dependency",
        "proposed": {
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        "scope": KnowledgeScope(service_refs=["entity:service:checkout"]),
        "explanation": "Record the reviewed dependency.",
        "created_by": "operator",
    }
    correction, candidate = service.create_correction(**kwargs)
    reviewed, _ = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )

    duplicate, duplicate_candidate = service.create_correction(**kwargs)

    assert reviewed.review_state == ReviewState.APPROVED
    assert duplicate.review_state == ReviewState.APPROVED
    assert duplicate_candidate.id == candidate.id
    assert duplicate_candidate.state.review_state == ReviewState.APPROVED
    assert service.repository.get_correction(correction.id).review_state == ReviewState.APPROVED


def test_correction_identity_includes_target_ref(tmp_path: Path):
    service = _service(tmp_path)
    candidate, original = _promoted_dependency(service)
    alternate = original.model_copy(
        update={
            "knowledge_id": "knowledge_alternate_target",
            "revision": 1,
            "parent_revision": None,
            "proposition": original.proposition.model_copy(update={"proposition_key": "sha256:alternate-target"}),
        }
    )
    service.repository.persist_revision(
        alternate,
        candidate_id=candidate.id,
        decision_ref=alternate.decision_ref,
    )
    kwargs = {
        "investigation_id": "inv-target-identity",
        "investigation_revision": 1,
        "correction_type": "dependency",
        "proposed": {
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        "scope": KnowledgeScope(service_refs=["entity:service:checkout"]),
        "explanation": "Correct the selected target.",
        "created_by": "operator",
    }

    first, _ = service.create_correction(target_ref=original.knowledge_id, **kwargs)
    second, _ = service.create_correction(target_ref=alternate.knowledge_id, **kwargs)

    assert first.id != second.id


def test_migration_preserves_payload_review_and_provenance(tmp_path: Path):
    service = _service(tmp_path)
    row = {
        "id": "dep_legacy",
        "source_entity": "entity:service:checkout",
        "target_entity": "entity:datastore:redis-session",
        "direction": "depends_on",
        "source_excerpt": "bounded excerpt",
        "review_state": "approved",
    }
    ids = migrate_artifact_extractions(
        artifact_id="artifact_1",
        artifact_type="runbook",
        rows={"dependency_hints": [row]},
        service=service,
    )
    candidate = service.repository.get_candidate(ids[0])
    assert candidate is not None
    assert candidate.typed_payload == row
    assert candidate.state.review_state == ReviewState.APPROVED
    assert candidate.migration_provenance is not None
    assert candidate.migration_provenance.original_record_ref == "dependency_hints:dep_legacy"
    row["review_state"] = "candidate"
    migrate_artifact_extractions(
        artifact_id="artifact_1",
        artifact_type="runbook",
        rows={"dependency_hints": [row]},
        service=service,
    )
    assert service.repository.get_candidate(ids[0]).state.review_state == ReviewState.APPROVED


def test_migration_reingest_preserves_governed_rejection(tmp_path: Path):
    service = _service(tmp_path)
    row = {
        "id": "dep_rejected",
        "source_entity": "entity:service:checkout",
        "target_entity": "entity:datastore:redis-session",
        "direction": "depends_on",
        "source_excerpt": "bounded excerpt",
        "review_state": "candidate",
    }
    candidate_id = migrate_artifact_extractions(
        artifact_id="artifact_rejected",
        artifact_type="runbook",
        rows={"dependency_hints": [row]},
        service=service,
    )[0]
    service.review_candidate(candidate_id, approved=False, reviewer="operator")
    row["review_state"] = "approved"

    migrate_artifact_extractions(
        artifact_id="artifact_rejected",
        artifact_type="runbook",
        rows={"dependency_hints": [row]},
        service=service,
    )

    assert service.repository.get_candidate(candidate_id).state.review_state == ReviewState.REJECTED


def test_tenant_id_collision_cannot_overwrite_candidate(tmp_path: Path):
    repository = KnowledgeRepository(tmp_path / "knowledge.db")
    first = _service(tmp_path, "tenant-a")
    candidate = _dependency(
        first,
        payload_ref="same",
        family=SourceFamily.RUNBOOK,
        lineage_group="same",
        tenant_id="tenant-a",
    )
    other = candidate.model_copy(
        update={
            "tenant_id": "tenant-b",
            "scope": candidate.scope.model_copy(update={"tenant_id": "tenant-b"}),
        }
    )
    with pytest.raises(ValueError, match="another tenant"):
        repository.save_candidate(other)
    assert repository.get_candidate(candidate.id, "tenant-b") is None


def test_api_queue_tenant_and_permissions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    candidate = _dependency(
        service,
        payload_ref="api",
        family=SourceFamily.RUNBOOK,
        lineage_group="api",
        tenant_id="tenant-a",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read",
        )
    )
    client = TestClient(app)
    response = client.get("/api/v1/knowledge/review-queue")
    assert response.status_code == 200
    assert response.json()["candidates"][0]["id"] == candidate.id
    assert client.get("/api/v1/knowledge/review-queue", headers={"X-Tacit-Tenant": "tenant-b"}).status_code == 403
    assert (
        client.post(
            f"/api/v1/knowledge/{candidate.id}/review",
            json={"decision": "approve", "reviewer": "operator"},
        ).status_code
        == 403
    )


def test_api_aliases_use_resolver_normalization(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read,knowledge.review",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/api/v1/knowledge/aliases",
        json={
            "id": "alias_checkout_api",
            "raw_value": "Checkout API",
            "entity_ref": "entity:service:checkout",
            "method": "exact_alias",
            "review_state": "approved",
            "provenance_refs": ["operator:alias"],
        },
    )

    assert response.status_code == 200
    assert response.json()["normalized_value"] == "checkout-api"


def test_api_trusted_alias_requires_trust_permission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read,knowledge.review",
        )
    )

    response = TestClient(app).post(
        "/api/v1/knowledge/aliases",
        json={
            "id": "alias_trusted_checkout",
            "raw_value": "Trusted Checkout",
            "entity_ref": "entity:service:checkout",
            "review_state": "trusted",
            "provenance_refs": ["operator:alias"],
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.trust"
    assert service.repository.find_aliases("tenant-a", "trusted-checkout") == []


def test_alias_upsert_updates_lookup_columns(tmp_path: Path):
    service = _service(tmp_path)
    first = EntityAlias(
        id="alias-checkout",
        tenant_id="default",
        raw_value="Checkout API",
        normalized_value="checkout-api",
        entity_ref="entity:service:checkout",
        scope=KnowledgeScope(),
        method=EntityBindingMethod.HUMAN_CORRECTION,
        review_state=ReviewState.APPROVED,
        provenance_refs=["operator:first"],
    )
    service.register_alias(first)
    service.register_alias(
        first.model_copy(
            update={
                "raw_value": "Checkout Service",
                "normalized_value": "checkout-service",
                "provenance_refs": ["operator:corrected"],
            }
        )
    )

    assert service.repository.find_aliases("default", "checkout-api") == []
    corrected = service.repository.find_aliases("default", "checkout-service")
    assert len(corrected) == 1
    assert corrected[0].raw_value == "Checkout Service"


def test_api_policy_overrides_require_privileged_permission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    candidate = _dependency(
        service,
        payload_ref="override",
        family=SourceFamily.RUNBOOK,
        lineage_group="override",
        tenant_id="tenant-a",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read,knowledge.review",
        )
    )
    client = TestClient(app)

    response = client.post(
        f"/api/v1/knowledge/{candidate.id}/review",
        json={
            "decision": "approve",
            "reviewer": "operator",
            "authoritative_source": True,
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.override"
    assert service.repository.get_candidate(candidate.id, "tenant-a").state.review_state == ReviewState.CANDIDATE


def test_api_correction_authority_requires_override_permission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    correction, candidate = service.create_correction(
        investigation_id="inv-api-correction",
        investigation_revision=1,
        correction_type="dependency",
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(tenant_id="tenant-a", service_refs=["entity:service:checkout"]),
        explanation="Operator correction",
        created_by="operator",
        tenant_id="tenant-a",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read,knowledge.review",
        )
    )

    response = TestClient(app).post(
        f"/api/v1/knowledge/corrections/{correction.id}/review",
        json={"decision": "approve", "reviewer": "operator", "authoritative": True},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.override"
    assert service.repository.get_candidate(candidate.id, "tenant-a").state.review_state == ReviewState.CANDIDATE


def test_cli_policy_overrides_require_privileged_permission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="cli-override",
        family=SourceFamily.RUNBOOK,
        lineage_group="cli-override",
    )
    monkeypatch.setattr("tacit.knowledge.service.get_knowledge_service", lambda: service)
    monkeypatch.setattr("tacit.config.settings.knowledge_permissions", "knowledge.read,knowledge.review")

    result = CliRunner().invoke(
        cli,
        [
            "knowledge",
            "review",
            candidate.id,
            "--approve",
            "--reviewer",
            "operator",
            "--authoritative-source",
        ],
    )

    assert result.exit_code != 0
    assert "missing permission: knowledge.override" in result.output
    assert service.repository.get_candidate(candidate.id).state.review_state == ReviewState.CANDIDATE


def test_cli_exposes_phase_three_commands():
    runner = CliRunner()
    assert runner.invoke(cli, ["knowledge", "--help"]).exit_code == 0
    output = runner.invoke(cli, ["knowledge", "review", "candidate", "--help"])
    assert output.exit_code == 0
    assert "--approve" in output.output
    assert runner.invoke(cli, ["learn", "status", "--help"]).exit_code == 0
    assert "--tenant" in runner.invoke(cli, ["learn", "runbooks", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["learn", "incidents", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["learn", "pagerduty", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["investigate", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["test", "--help"]).output


def test_wildcard_cli_pipeline_commands_require_tenant(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "*")
    runner = CliRunner()

    investigate_result = runner.invoke(cli, ["investigate", "checkout latency"])
    test_result = runner.invoke(cli, ["test", "--no-open-browser"])

    assert investigate_result.exit_code != 0
    assert "--tenant is required" in investigate_result.output
    assert test_result.exit_code != 0
    assert "--tenant is required" in test_result.output


def test_operational_learning_benchmark_is_packaged_and_safe():
    corpus = load_operational_learning_corpus()
    report = run_operational_learning_benchmark()
    assert corpus["benchmark_version"] == "v1"
    assert report["passed"] is True
    assert report["metrics"]["unsafe_fuzzy_resolution_rate"] == 0
    assert report["metrics"]["rejected_item_contribution_rate"] == 0
    assert report["metrics"]["unresolved_item_contribution_rate"] == 0
    assert report["metrics"]["causal_claim_leakage_rate"] == 0
    assert report["metrics"]["prompt_injection_policy_override_count"] == 0
