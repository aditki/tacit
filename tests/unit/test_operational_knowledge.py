from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click import ClickException
from click.testing import CliRunner
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tacit.api.app import create_app
from tacit.cli import _knowledge_tenant, cli
from tacit.config import Settings
from tacit.knowledge.enums import (
    ConflictResolutionStatus,
    CorrectionType,
    EntityBindingMethod,
    EntityKind,
    EntityStatus,
    EvidenceRole,
    KnowledgeEligibility,
    KnowledgeKind,
    KnowledgeUsageDisposition,
    LifecycleStatus,
    LineageKind,
    ReviewState,
    SourceFamily,
)
from tacit.knowledge.lifecycle import (
    LIFECYCLE_TRANSITIONS,
    REVIEW_TRANSITIONS,
    transition_lifecycle_state,
    transition_review_state,
)
from tacit.knowledge.migration import migrate_artifact_extractions, migrate_signal_mapping
from tacit.knowledge.models import (
    Entity,
    EntityAlias,
    KnowledgeCandidate,
    KnowledgeEvidenceReference,
    KnowledgeRevision,
    KnowledgeScope,
    KnowledgeState,
    KnowledgeUsage,
)
from tacit.knowledge.normalization import normalize_service_ref
from tacit.knowledge.repository import (
    CandidateEvaluationConflictError,
    CandidateReviewConflictError,
    KnowledgeRepository,
    KnowledgeRevisionConflictError,
)
from tacit.knowledge.scope import investigation_knowledge_scope
from tacit.knowledge.service import KnowledgeService
from tacit.models.schemas import CulpritCandidate, CulpritRanking, EvidenceObservation, EvidenceObservationOutcome
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
    subject_ref: str = "entity:service:checkout",
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
            "subject_ref": subject_ref,
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


def _assert_repository_invariants(repository: KnowledgeRepository, tenant_id: str = "default") -> None:
    with repository._conn() as conn:
        candidate_rows = conn.execute(
            """SELECT tenant_id, review_state, lifecycle_status, eligibility, candidate_json
               FROM knowledge_candidates WHERE tenant_id=?""",
            (tenant_id,),
        ).fetchall()
        current_rows = conn.execute(
            """SELECT item.tenant_id, item.current_revision, item.status, revision.content_json
               FROM operational_knowledge AS item
               JOIN operational_knowledge_revisions AS revision
                 ON revision.tenant_id=item.tenant_id
                AND revision.knowledge_id=item.knowledge_id
                AND revision.revision=item.current_revision
               WHERE item.tenant_id=?""",
            (tenant_id,),
        ).fetchall()
        item_count = conn.execute(
            "SELECT COUNT(*) FROM operational_knowledge WHERE tenant_id=?",
            (tenant_id,),
        ).fetchone()[0]

    assert len(current_rows) == item_count

    for row in candidate_rows:
        candidate = KnowledgeCandidate.model_validate_json(row["candidate_json"])
        assert candidate.tenant_id == row["tenant_id"]
        assert candidate.scope.tenant_id == row["tenant_id"]
        assert candidate.state.review_state.value == row["review_state"]
        assert candidate.state.lifecycle_status.value == row["lifecycle_status"]
        assert candidate.state.eligibility.value == row["eligibility"]

    for row in current_rows:
        revision = KnowledgeRevision.model_validate_json(row["content_json"])
        assert revision.tenant_id == row["tenant_id"]
        assert revision.scope.tenant_id == row["tenant_id"]
        assert revision.revision == row["current_revision"]
        assert revision.state.lifecycle_status.value == row["status"]


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
    with pytest.raises(ValidationError, match="stale knowledge must be ineligible"):
        KnowledgeState(
            review_state=ReviewState.APPROVED,
            lifecycle_status=LifecycleStatus.STALE,
            eligibility=KnowledgeEligibility.CONTEXTUAL_ONLY,
        )
    with pytest.raises(ValidationError, match="unreviewed knowledge must be ineligible"):
        KnowledgeState(eligibility=KnowledgeEligibility.CONTEXTUAL_ONLY)


def test_scope_invariants_reject_reversed_validity_windows():
    start = datetime.now(UTC)
    with pytest.raises(ValidationError, match="valid_until must be after valid_from"):
        KnowledgeScope(valid_from=start, valid_until=start - timedelta(seconds=1))


@pytest.mark.parametrize("source", list(ReviewState))
@pytest.mark.parametrize("target", list(ReviewState))
def test_review_lifecycle_transition_matrix(source, target):
    eligibility = (
        KnowledgeEligibility.CONTEXTUAL_ONLY
        if source in {ReviewState.APPROVED, ReviewState.TRUSTED}
        else KnowledgeEligibility.INELIGIBLE
    )
    state = KnowledgeState(review_state=source, eligibility=eligibility)
    allowed = source == target or target in REVIEW_TRANSITIONS[source]

    if not allowed:
        with pytest.raises(ValueError, match="cannot transition review state"):
            transition_review_state(state, target)
        return

    transitioned = transition_review_state(state, target)
    assert transitioned.review_state == target
    if source != target:
        assert transitioned.eligibility == KnowledgeEligibility.INELIGIBLE


@pytest.mark.parametrize("source", list(LifecycleStatus))
@pytest.mark.parametrize("target", list(LifecycleStatus))
def test_source_lifecycle_transition_matrix(source, target):
    eligibility = (
        KnowledgeEligibility.CONTEXTUAL_ONLY if source == LifecycleStatus.ACTIVE else KnowledgeEligibility.INELIGIBLE
    )
    state = KnowledgeState(
        review_state=ReviewState.APPROVED,
        lifecycle_status=source,
        eligibility=eligibility,
    )
    allowed = source == target or target in LIFECYCLE_TRANSITIONS[source]

    if not allowed:
        with pytest.raises(ValueError, match="cannot transition lifecycle"):
            transition_lifecycle_state(state, target)
        return

    transitioned = transition_lifecycle_state(state, target)
    assert transitioned.lifecycle_status == target
    if source != target:
        assert transitioned.eligibility == KnowledgeEligibility.INELIGIBLE


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
    assert set(revision.promoted_from_candidate_refs) == {item.id for item in candidates}
    assert set(revision.provenance_refs) == {
        "provenance:runbook",
        "provenance:dashboard",
    }
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
    assert usage_a[0].disposition.value == "considered_not_applied"
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


def test_revision_invariants_reject_cross_tenant_and_broken_parentage(tmp_path: Path):
    service = _service(tmp_path)
    _, revision = _promoted_dependency(service)
    payload = revision.model_dump(mode="python")

    with pytest.raises(ValidationError, match="tenant and scope tenant must match"):
        KnowledgeRevision.model_validate({**payload, "tenant_id": "tenant-b"})
    with pytest.raises(ValidationError, match="parent must be the preceding revision"):
        KnowledgeRevision.model_validate({**payload, "revision": 2, "parent_revision": None})


def test_repository_invariants_hold_after_source_retirement(tmp_path: Path):
    service = _service(tmp_path)
    first, active = _promoted_dependency(service)
    _assert_repository_invariants(service.repository)

    service.reconcile_source_lifecycle(
        provenance_ref="provenance:runbook",
        active_candidate_ids=set(),
    )

    _assert_repository_invariants(service.repository)
    stale_candidate = service.repository.get_candidate(first.id)
    current = service.repository.get_revision(active.knowledge_id)
    assert stale_candidate is not None
    assert stale_candidate.state.lifecycle_status == LifecycleStatus.STALE
    assert stale_candidate.state.eligibility == KnowledgeEligibility.INELIGIBLE
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.STALE
    assert current.state.eligibility == KnowledgeEligibility.INELIGIBLE


def test_knowledge_candidate_clears_empty_ranking_abstention(tmp_path: Path):
    service = _service(tmp_path)
    _promoted_dependency(service)
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    ranking, applied_usage = service.apply_to_ranking(
        CulpritRanking(abstained=True, abstention_reason="no_rankable_candidates"),
        usage,
    )

    assert len(ranking.candidates) == 1
    assert ranking.abstained is False
    assert ranking.abstention_reason == ""
    assert applied_usage[0].disposition.value == "applied"
    assert applied_usage[0].used_for == ["candidate_generation", "ranking"]


def test_knowledge_preserves_abstention_when_it_only_boosts_an_existing_candidate(tmp_path: Path):
    service = _service(tmp_path)
    _promoted_dependency(service)
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )
    baseline = CulpritRanking(
        abstained=True,
        abstention_reason="runtime_evidence_unavailable",
        candidates=[
            CulpritCandidate(
                rank=1,
                suspect="redis-session",
                suspect_type="datastore",
                score=0.25,
            )
        ],
    )

    ranking, applied_usage = service.apply_to_ranking(baseline, usage)

    assert ranking.candidates[0].score > baseline.candidates[0].score
    assert ranking.abstained is True
    assert ranking.abstention_reason == "runtime_evidence_unavailable"
    assert applied_usage[0].disposition.value == "applied"
    assert applied_usage[0].used_for == ["ranking"]


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
    assert usage_with_version[0].disposition.value == "considered_not_applied"


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
    service.review_candidate(positive.id, approved=True, reviewer="operator")
    service.review_candidate(rejected.id, approved=True, reviewer="operator")
    conflicts = service.conflicts.analyze("default", positive.proposition.proposition_key)
    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.UNRESOLVED

    service.review_candidate(rejected.id, approved=False, reviewer="operator")

    assert service.repository.list_conflicts("default", unresolved_only=True) == []
    resolved = service.repository.list_conflicts("default")
    assert resolved[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_REVIEW
    assert resolved[0].resolution_reason == "counter_proposition_rejected"


def test_new_candidate_reopens_conflict_resolved_by_rejection(tmp_path: Path):
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
    service.review_candidate(positive.id, approved=True, reviewer="operator")
    service.review_candidate(rejected.id, approved=True, reviewer="operator")
    service.conflicts.analyze("default", positive.proposition.proposition_key)
    service.review_candidate(rejected.id, approved=False, reviewer="operator")
    assert service.repository.list_conflicts("default", unresolved_only=True) == []
    replacement = _dependency(
        service,
        payload_ref="replacement-negative",
        family=SourceFamily.INCIDENT,
        lineage_group="replacement-negative",
        predicate="does_not_depend_on",
    )
    service.review_candidate(replacement.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", replacement.proposition.proposition_key)

    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.UNRESOLVED
    assert conflicts[0].resolution_reason == ""
    assert service.repository.list_conflicts("default", unresolved_only=True) == conflicts
    assert any(event["event_type"] == "conflict_reopened" for event in service.repository.list_events("default"))


def test_reviewed_support_reopens_conflict_resolved_by_correction(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, _ = service.create_correction(
        investigation_id="inv-reopen-correction",
        investigation_revision=1,
        correction_type="dependency",
        target_ref=original.knowledge_id,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        ),
        explanation="Redis is no longer a dependency.",
        created_by="operator",
    )
    service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )
    resolved = service.repository.list_conflicts("default")
    assert resolved[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_REVIEW
    assert resolved[0].resolution_reason == "approved_human_correction"

    renewed_support = _dependency(
        service,
        payload_ref="reviewed-renewed-support",
        family=SourceFamily.INCIDENT,
        lineage_group="reviewed-renewed-support",
    )
    service.review_candidate(renewed_support.id, approved=True, reviewer="second-reviewer")

    decision, revision = service.evaluate_candidate(
        renewed_support.id,
        authoritative_source=True,
    )

    assert revision is None
    assert "unresolved_conflict" in decision.reason_codes
    reopened = service.repository.list_conflicts("default", unresolved_only=True)
    assert len(reopened) == 1
    assert reopened[0].resolution_reason == ""
    events = service.repository.list_events("default")
    assert any(
        event["event_type"] == "conflict_reopened" and event["reason_code"] == "new_support_for_superseded_proposition"
        for event in events
    )


def test_signal_mapping_allows_multiple_metrics_for_one_signal(tmp_path: Path):
    service = _service(tmp_path)
    first = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="latency-metric-a",
        typed_payload={},
        proposition={
            "subject_ref": "concept:latency",
            "predicate": "represented_by",
            "object_ref": "concept:metric_a_seconds",
        },
        provenance_refs=["catalog:metric-a"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="latency-metric-b",
        typed_payload={},
        proposition={
            "subject_ref": "concept:latency",
            "predicate": "represented_by",
            "object_ref": "concept:metric_b_seconds",
        },
        provenance_refs=["catalog:metric-b"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    service.review_candidate(second.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert conflicts == []


def test_signal_mapping_allows_multiple_meanings_for_one_metric(tmp_path: Path):
    service = _service(tmp_path)
    first = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="latency-shared-metric",
        typed_payload={},
        proposition={
            "subject_ref": "concept:latency",
            "predicate": "represented_by",
            "object_ref": "concept:shared_metric_seconds",
        },
        provenance_refs=["catalog:latency"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="saturation-shared-metric",
        typed_payload={},
        proposition={
            "subject_ref": "concept:saturation",
            "predicate": "represented_by",
            "object_ref": "concept:shared_metric_seconds",
        },
        provenance_refs=["catalog:saturation"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    service.review_candidate(second.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert conflicts == []


def test_conflict_scope_analysis_includes_services(tmp_path: Path):
    service = _service(tmp_path)
    for team in ("payments", "platform"):
        service.register_entity(
            Entity(
                id=f"entity:team:{team}",
                kind=EntityKind.TEAM,
                canonical_name=team,
                scope=KnowledgeScope(),
                provenance_refs=["catalog:team"],
            )
        )
    first = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="checkout-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:payments",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["catalog:checkout"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="payment-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:platform",
        },
        scope=KnowledgeScope(service_refs=["entity:service:payment"]),
        provenance_refs=["catalog:payment"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    service.review_candidate(second.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_SCOPE
    assert conflicts[0].scope_analysis["reason_code"] == "service_specific_difference"


def test_conflict_scope_analysis_includes_archetypes(tmp_path: Path):
    service = _service(tmp_path)
    for team in ("payments", "platform"):
        service.register_entity(
            Entity(
                id=f"entity:team:{team}",
                kind=EntityKind.TEAM,
                canonical_name=team,
                scope=KnowledgeScope(),
                provenance_refs=["catalog:team"],
            )
        )
    first = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="http-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:payments",
        },
        scope=KnowledgeScope(
            service_refs=["entity:service:checkout"],
            archetype_refs=["archetype:http-service"],
        ),
        provenance_refs=["catalog:http"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="queue-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:platform",
        },
        scope=KnowledgeScope(
            service_refs=["entity:service:checkout"],
            archetype_refs=["archetype:queue-worker"],
        ),
        provenance_refs=["catalog:queue"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    service.review_candidate(second.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_SCOPE
    assert conflicts[0].scope_analysis["reason_code"] == "archetype_specific_difference"


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


def test_entity_registration_normalizes_ids_before_exact_resolution(tmp_path: Path):
    db_path = tmp_path / "entity-normalization.db"
    app = create_app(runtime_settings=Settings(signals_db_path=str(db_path)))
    response = TestClient(app).post(
        "/api/v1/knowledge/entities",
        json={
            "id": "Service:Payment API",
            "kind": "service",
            "canonical_name": "Payment API",
            "provenance_refs": ["operator:entity"],
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["id"] == "entity:service:payment-api"
    service = app.state.runtime_stores.knowledge()
    resolution = service.entity_resolution.resolve(
        "entity:service:payment-api",
        EntityKind.SERVICE,
        KnowledgeScope(),
        ["operator:entity"],
    )
    assert resolution.status.value == "resolved"
    assert resolution.selected_entity_ref == "entity:service:payment-api"


@pytest.mark.parametrize(
    ("subject_ref", "object_ref"),
    [
        ("concept:checkout", "entity:datastore:redis-session"),
        ("entity:service:checkout", "concept:redis-session"),
    ],
)
def test_dependency_concepts_do_not_bypass_entity_kind_resolution(
    tmp_path: Path,
    subject_ref: str,
    object_ref: str,
):
    service = _service(tmp_path)

    candidate = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref=f"concept-dependency:{subject_ref}:{object_ref}",
        typed_payload={},
        proposition={
            "subject_ref": subject_ref,
            "predicate": "depends_on",
            "object_ref": object_ref,
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["artifact:concept-dependency"],
    )

    assert candidate.entity_resolution.status.value == "unresolved"
    assert "raw_concept_does_not_match_expected_entity_kind" in candidate.entity_resolution.reason_codes


def test_exact_id_resolution_rejects_inactive_entities(tmp_path: Path):
    service = _service(tmp_path)
    checkout = service.repository.get_entity("entity:service:checkout")
    assert checkout is not None
    service.register_entity(checkout.model_copy(update={"status": EntityStatus.WITHDRAWN}))

    candidate = _dependency(
        service,
        payload_ref="withdrawn-checkout",
        family=SourceFamily.RUNBOOK,
        lineage_group="withdrawn-checkout",
    )

    assert candidate.entity_resolution.status.value == "unresolved"


def test_alias_scope_defaults_to_alias_tenant(tmp_path: Path):
    service = _service(tmp_path, "tenant-a")
    alias = service.register_alias(
        EntityAlias(
            id="alias-storefront",
            tenant_id="tenant-a",
            raw_value="Storefront",
            normalized_value="storefront",
            entity_ref="entity:service:checkout",
            scope=KnowledgeScope(),
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=["operator:alias"],
        )
    )

    candidate = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="tenant-alias",
        typed_payload={},
        proposition={
            "subject_ref": "Storefront",
            "predicate": "depends_on",
            "object_ref": "redis-session",
        },
        scope=KnowledgeScope(tenant_id="tenant-a", service_refs=["entity:service:checkout"]),
        provenance_refs=["operator:alias"],
        tenant_id="tenant-a",
    )

    assert alias.scope.tenant_id == "tenant-a"
    assert candidate.entity_resolution.status.value == "resolved"


def test_exact_alias_resolution_enforces_alias_scope(tmp_path: Path):
    service = _service(tmp_path)
    service.register_alias(
        EntityAlias(
            id="alias-production-storefront",
            raw_value="Storefront",
            normalized_value="storefront",
            entity_ref="entity:service:checkout",
            scope=KnowledgeScope(environment_refs=["environment:production"]),
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=["operator:alias"],
        )
    )

    staging = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="staging-scoped-alias",
        typed_payload={},
        proposition={
            "subject_ref": "Storefront",
            "predicate": "depends_on",
            "object_ref": "redis-session",
        },
        scope=KnowledgeScope(environment_refs=["environment:staging"]),
        provenance_refs=["runbook:staging"],
    )
    production = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="production-scoped-alias",
        typed_payload={},
        proposition={
            "subject_ref": "Storefront",
            "predicate": "depends_on",
            "object_ref": "redis-session",
        },
        scope=KnowledgeScope(environment_refs=["environment:production"]),
        provenance_refs=["runbook:production"],
    )

    assert staging.entity_resolution.status.value == "unresolved"
    assert production.entity_resolution.status.value == "resolved"
    assert production.entity_resolution.candidate_bindings[0].method == EntityBindingMethod.EXACT_ALIAS


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


def test_copied_artifacts_share_one_independence_group(tmp_path: Path):
    service = _service(tmp_path)
    candidate_ids = []
    for artifact_id, artifact_type, row_id in (
        ("runbook-copy", "runbook", "dep-copy-a"),
        ("incident-copy", "incident", "dep-copy-b"),
    ):
        candidate_ids.extend(
            migrate_artifact_extractions(
                artifact_id=artifact_id,
                artifact_type=artifact_type,
                artifact_fingerprint="same-content-fingerprint",
                rows={
                    "dependency_hints": [
                        {
                            "id": row_id,
                            "source_entity": "checkout",
                            "target_entity": "redis-session",
                            "direction": "depends_on",
                        }
                    ]
                },
                service=service,
            )
        )
    for candidate_id in candidate_ids:
        service.review_candidate(candidate_id, approved=True, reviewer="operator")
    candidate = service.repository.get_candidate(candidate_ids[0])
    assert candidate is not None

    summary, _ = service.corroboration.analyze("default", candidate.proposition.proposition_key)

    assert summary.raw_source_count == 2
    assert summary.independent_source_count == 1
    assert summary.independent_source_family_count == 1


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


def test_signal_mapping_usage_remains_considered_until_a_stage_consumes_it(tmp_path: Path):
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

    assert usage[0].disposition.value == "considered_not_applied"
    assert usage[0].used_for == []
    assert usage[0].score_delta == 0


@pytest.mark.parametrize(
    ("proposition", "reason"),
    [
        (
            {
                "subject_ref": "concept:latency",
                "predicate": "represented_by",
                "concept_ref": "signal:latency",
            },
            "signal_metric_unresolved",
        ),
        (
            {
                "subject_ref": "concept:latency",
                "predicate": "represented_by",
                "object_ref": "concept:http_request_duration_seconds",
            },
            "signal_concept_unresolved",
        ),
    ],
)
def test_signal_mapping_requires_signal_and_metric(tmp_path: Path, proposition, reason):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref=f"incomplete:{reason}",
        typed_payload={},
        proposition=proposition,
        provenance_refs=["human:review"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")

    decision, revision = service.evaluate_candidate(candidate.id, authoritative_source=True)

    assert revision is None
    assert reason in decision.reason_codes


def test_negative_dependency_excludes_matching_ranked_candidate(tmp_path: Path):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="negative-dependency",
        family=SourceFamily.HUMAN_CORRECTION,
        lineage_group="negative-dependency",
        predicate="does_not_depend_on",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, authoritative_source=True)
    assert revision is not None
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )
    considered = next(item for item in usage if item.knowledge_ref == revision.knowledge_id)
    reconciled = service.reconcile_live_observations(
        [considered],
        [
            EvidenceObservation(
                requirement_id="redis_health",
                resolution_metric="redis-session",
                outcome=EvidenceObservationOutcome.NEGATIVE_EVIDENCE,
            )
        ],
    )

    ranking, applied_usage = service.apply_to_ranking(
        CulpritRanking(
            abstained=False,
            candidates=[
                CulpritCandidate(
                    rank=1,
                    suspect="redis-session",
                    suspect_type="datastore",
                    score=0.72,
                )
            ],
        ),
        reconciled,
    )

    assert considered.disposition.value == "considered_not_applied"
    assert considered.used_for == []
    assert applied_usage[0].disposition.value == "applied"
    assert applied_usage[0].used_for == ["candidate_exclusion"]
    assert applied_usage[0].score_delta == 0
    assert ranking.candidates == []
    assert ranking.abstained is True
    assert ranking.abstention_reason == "operational_knowledge_excluded_ranked_candidates"


def test_negative_dependency_without_matching_candidate_remains_considered(tmp_path: Path):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="negative-no-match",
        family=SourceFamily.HUMAN_CORRECTION,
        lineage_group="negative-no-match",
        predicate="does_not_depend_on",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, authoritative_source=True)
    assert revision is not None
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    ranking, resulting_usage = service.apply_to_ranking(
        CulpritRanking(abstained=True, abstention_reason="no_rankable_candidates"),
        usage,
    )

    assert ranking.candidates == []
    assert resulting_usage[0].disposition.value == "considered_not_applied"
    assert resulting_usage[0].used_for == []
    assert resulting_usage[0].score_delta == 0


def test_dependency_subject_must_match_investigation_service(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:service:payments",
            kind=EntityKind.SERVICE,
            canonical_name="payments",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:service"],
        )
    )
    candidate = _dependency(
        service,
        payload_ref="payments-redis",
        family=SourceFamily.HUMAN_CORRECTION,
        lineage_group="payments-redis",
        subject_ref="entity:service:payments",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, authoritative_source=True)
    assert revision is not None

    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    item = next(item for item in usage if item.knowledge_ref == revision.knowledge_id)
    assert item.disposition.value == "rejected_by_scope"
    assert item.reason_codes == ["dependency_subject_mismatch"]


def test_scope_normalizes_naive_validity_datetimes_to_utc():
    scope = KnowledgeScope.model_validate_json(
        '{"valid_from":"2000-01-01T00:00:00","valid_until":"2999-01-01T00:00:00"}'
    )

    assert scope.valid_from is not None
    assert scope.valid_until is not None
    assert scope.valid_from.tzinfo == UTC
    assert scope.valid_until.tzinfo == UTC
    assert scope.applies_to(KnowledgeScope()) is True


def test_entity_resolution_accepts_typed_entity_refs(tmp_path: Path):
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

    candidate = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="typed-entity-refs",
        typed_payload={},
        proposition={
            "subject_ref": "service:checkout",
            "predicate": "owned_by",
            "object_ref": "team:payments",
        },
        provenance_refs=["operator:correction"],
    )

    assert candidate.entity_resolution.status.value == "resolved"
    assert candidate.proposition.subject_ref == "entity:service:checkout"
    assert candidate.proposition.object_ref == "entity:team:payments"


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


def test_impact_includes_only_investigations_where_knowledge_was_applied(tmp_path: Path):
    service = _service(tmp_path)
    _, revision = _promoted_dependency(service)
    for investigation_id, disposition in (
        ("inv-applied", KnowledgeUsageDisposition.APPLIED),
        ("inv-considered", KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED),
        ("inv-rejected", KnowledgeUsageDisposition.REJECTED_BY_SCOPE),
    ):
        service.repository.save_usage(
            KnowledgeUsage(
                investigation_id=investigation_id,
                investigation_revision=1,
                knowledge_ref=revision.knowledge_id,
                knowledge_revision=revision.revision,
                disposition=disposition,
                used_for=["ranking"] if disposition == KnowledgeUsageDisposition.APPLIED else [],
            )
        )

    impact = service.impact(revision.knowledge_id)

    assert impact.affected_investigations == [{"investigation_id": "inv-applied", "revision": 1}]


def test_correction_rejects_target_that_advanced_after_creation(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, correction_candidate = service.create_correction(
        investigation_id="inv_revision_pinned",
        investigation_revision=1,
        correction_type=CorrectionType.KNOWLEDGE_INCORRECT,
        target_ref=original.knowledge_id,
        target_revision=original.revision,
        proposed={
            "subject_ref": "concept:artifact-quality",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:incorrect-knowledge",
        },
        scope=KnowledgeScope(),
        explanation="The investigation showed this revision was incorrect.",
        created_by="operator",
    )
    assert correction.target_revision == original.revision

    additional = _dependency(
        service,
        payload_ref="incident-new-support",
        family=SourceFamily.INCIDENT,
        lineage_group="incident-new-support",
    )
    service.review_candidate(additional.id, approved=True, reviewer="operator")
    _, advanced = service.evaluate_candidate(additional.id)
    assert advanced is not None
    assert advanced.revision > original.revision

    with pytest.raises(ValueError, match="advanced from revision 1 to 2"):
        service.review_correction(
            correction.id,
            approved=True,
            reviewer="operator",
            authoritative=True,
        )

    stored_candidate = service.repository.get_candidate(correction_candidate.id)
    assert stored_candidate is not None
    assert stored_candidate.state.review_state == ReviewState.CANDIDATE


def test_correction_supersession_rechecks_target_under_write_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, _ = service.create_correction(
        investigation_id="inv-concurrent-correction",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        target_ref=original.knowledge_id,
        target_revision=original.revision,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=original.scope,
        explanation="Replace the reviewed dependency.",
        created_by="operator",
    )
    concurrent_support = _dependency(
        service,
        payload_ref="concurrent-support",
        family=SourceFamily.INCIDENT,
        lineage_group="concurrent-support",
    )
    service.review_candidate(concurrent_support.id, approved=True, reviewer="operator")
    supersede = service.supersede

    def advance_then_supersede(*args, **kwargs):
        advanced = original.model_copy(
            update={
                "revision": original.revision + 1,
                "parent_revision": original.revision,
                "revision_reason": "concurrent_update",
                "semantic_fingerprint": f"{original.semantic_fingerprint}:concurrent",
                "created_at": datetime.now(UTC),
            }
        )
        service.repository.persist_revision(
            advanced,
            candidate_id=concurrent_support.id,
            decision_ref="decision-concurrent-update",
        )
        return supersede(*args, **kwargs)

    monkeypatch.setattr(service, "supersede", advance_then_supersede)

    with pytest.raises(KnowledgeRevisionConflictError, match="advanced from revision 1 to 2"):
        service.review_correction(
            correction.id,
            approved=True,
            reviewer="operator",
            authoritative=True,
        )

    current = service.repository.get_revision(original.knowledge_id)
    assert current is not None
    assert current.revision == 2
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE


def test_correction_creation_rejects_an_already_stale_target_revision(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    additional = _dependency(
        service,
        payload_ref="dashboard-new-support",
        family=SourceFamily.ALERT,
        lineage_group="dashboard-new-support",
    )
    service.review_candidate(additional.id, approved=True, reviewer="operator")
    _, advanced = service.evaluate_candidate(additional.id)
    assert advanced is not None

    with pytest.raises(ValueError, match="advanced from revision 1 to 2"):
        service.create_correction(
            investigation_id="inv_stale_at_submit",
            investigation_revision=1,
            correction_type=CorrectionType.KNOWLEDGE_INCORRECT,
            target_ref=original.knowledge_id,
            target_revision=original.revision,
            proposed={
                "subject_ref": "concept:artifact-quality",
                "predicate": "useful_for_investigation",
                "concept_ref": "concept:incorrect-knowledge",
            },
            scope=KnowledgeScope(),
            explanation="This correction was based on an older investigation.",
            created_by="operator",
        )


def test_authoritative_signal_correction_promotes(tmp_path: Path):
    service = _service(tmp_path)
    correction, _ = service.create_correction(
        investigation_id="inv_signal_correction",
        investigation_revision=1,
        correction_type="signal_meaning",
        proposed={
            "subject_ref": "concept:checkout-latency",
            "predicate": "represented_by",
            "object_ref": "concept:http_request_duration_seconds",
            "concept_ref": "signal:latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        explanation="This is the operator-approved latency signal.",
        created_by="operator",
    )

    reviewed, revision = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )

    assert reviewed.review_state == ReviewState.APPROVED
    assert revision is not None
    assert revision.proposition.kind == KnowledgeKind.SIGNAL_MAPPING
    from tacit.signals.store import SignalStore

    mappings = SignalStore(service.repository._db_path).get_mappings_for_signal(
        "latency",
        context_service="checkout",
    )
    assert [mapping["metric_pattern"] for mapping in mappings] == ["http_request_duration_seconds"]


def test_governed_signal_projection_preserves_each_revision_scope(tmp_path: Path):
    service = _service(tmp_path)

    def promote_for(service_name: str) -> KnowledgeRevision:
        correction, _ = service.create_correction(
            investigation_id=f"inv-{service_name}-latency",
            investigation_revision=1,
            correction_type=CorrectionType.SIGNAL_MEANING,
            proposed={
                "subject_ref": "concept:latency",
                "predicate": "represented_by",
                "object_ref": "concept:shared_latency_seconds",
                "concept_ref": "signal:request_latency",
                "metric_pattern": "shared_latency_seconds",
            },
            scope=KnowledgeScope(service_refs=[f"entity:service:{service_name}"]),
            explanation=f"Approve the {service_name} latency mapping.",
            created_by="operator",
        )
        _, revision = service.review_correction(
            correction.id,
            approved=True,
            reviewer="reviewer",
            authoritative=True,
        )
        assert revision is not None
        return revision

    checkout = promote_for("checkout")
    payments = promote_for("payments")

    from tacit.signals.store import SignalStore

    store = SignalStore(service.repository._db_path)
    with store._conn() as conn:
        rows = conn.execute("""SELECT governance_ref, context_services, review_state
               FROM signal_metric_mappings
               WHERE tenant_id='default' AND signal_type='request_latency'
                 AND metric_pattern='shared_latency_seconds'
               ORDER BY governance_ref""").fetchall()
    assert len(rows) == 2
    assert {row["governance_ref"] for row in rows} == {checkout.knowledge_id, payments.knowledge_id}
    assert {row["context_services"] for row in rows} == {'["checkout"]', '["payments"]'}

    _, usage = service.create_snapshot(checkout.scope)
    compilation_usage = service.apply_compilation_usage(usage, {checkout.knowledge_id})
    checkout_usage = next(item for item in compilation_usage if item.knowledge_ref == checkout.knowledge_id)
    assert checkout_usage.disposition == KnowledgeUsageDisposition.APPLIED
    assert checkout_usage.used_for == ["query_compilation"]
    assert checkout_usage.score_delta == 0

    stale, _ = service.create_correction(
        investigation_id="inv-checkout-stale",
        investigation_revision=2,
        correction_type=CorrectionType.KNOWLEDGE_STALE,
        target_ref=checkout.knowledge_id,
        target_revision=checkout.revision,
        proposed={
            "subject_ref": "concept:artifact-quality",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:stale-knowledge",
        },
        scope=checkout.scope,
        explanation="The checkout mapping is stale.",
        created_by="operator",
    )
    service.review_correction(stale.id, approved=True, reviewer="reviewer")

    assert (
        store.get_mappings_for_signal(
            "request_latency",
            context_service="checkout",
        )
        == []
    )
    active_payments = store.get_mappings_for_signal(
        "request_latency",
        context_service="payments",
    )
    assert [mapping["governance_ref"] for mapping in active_payments] == [payments.knowledge_id]


def test_entity_mapping_correction_registers_alias_without_signal_revision(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:service:checkout",
            kind=EntityKind.SERVICE,
            canonical_name="checkout",
            scope=KnowledgeScope(),
            provenance_refs=["catalog:checkout"],
        )
    )
    correction, candidate = service.create_correction(
        investigation_id="inv_entity_mapping",
        investigation_revision=1,
        correction_type=CorrectionType.ENTITY_MAPPING,
        proposed={
            "raw_value": "Checkout API",
            "entity_ref": "entity:service:checkout",
        },
        scope=KnowledgeScope(),
        explanation="Bind the observed service alias to the catalog entity.",
        created_by="operator",
    )

    reviewed, revision = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
    )
    resolution = service.entity_resolution.resolve(
        "Checkout API",
        EntityKind.SERVICE,
        KnowledgeScope(),
        [f"prov_{correction.id}"],
    )

    assert candidate.kind == KnowledgeKind.ARTIFACT_QUALITY
    assert reviewed.review_state == ReviewState.APPROVED
    assert revision is None
    assert resolution.selected_entity_ref == "entity:service:checkout"
    assert service.repository.find_aliases("default", "checkout-api")
    assert service.repository.list_current_revisions("default") == []


def test_promoted_signal_mapping_preserves_exact_backend_metric_pattern(tmp_path: Path):
    service = _service(tmp_path)
    exact_pattern = "AWS/ApplicationELB/TargetResponseTime"
    candidate_id = migrate_signal_mapping(
        {
            "id": "cloudwatch-target-response-time",
            "signal_type": "request_latency",
            "metric_pattern": exact_pattern,
            "source_type": "human",
            "source_refs": ["manual:cloudwatch-review"],
            "review_state": "trusted",
        },
        service=service,
    )

    _decision, revision = service.evaluate_candidate(
        candidate_id,
        authoritative_source=True,
    )

    assert revision is not None
    from tacit.signals.store import SignalStore

    patterns = {
        mapping["metric_pattern"]
        for mapping in SignalStore(service.repository._db_path).get_mappings_for_signal("request_latency")
    }
    assert exact_pattern in patterns
    assert "aws-applicationelb-targetresponsetime" not in patterns


def test_manual_signal_teaching_creates_governed_revision_before_activation(tmp_path: Path):
    db_path = tmp_path / "signals.db"
    app = create_app(
        runtime_settings=Settings(
            signals_db_path=str(db_path),
            knowledge_tenant_id="tenant-a",
        )
    )
    client = TestClient(app)

    response = client.post(
        "/api/v1/signals/teach",
        json={
            "signal_type": "request_latency",
            "metric_patterns": [
                {
                    "pattern": "AWS/ApplicationELB/TargetResponseTime",
                    "confidence": 0.95,
                }
            ],
            "category": "latency",
            "taught_by": "operator",
        },
    )

    assert response.status_code == 200, response.text
    assert response.json()["mappings_created"] == 1
    stores = app.state.runtime_stores
    revisions = stores.knowledge_repository().list_current_revisions("tenant-a")
    assert len(revisions) == 1
    assert revisions[0].state.eligibility != KnowledgeEligibility.INELIGIBLE
    candidate = stores.knowledge_repository().get_candidate(
        revisions[0].promoted_from_candidate_refs[0],
        "tenant-a",
    )
    assert candidate is not None
    assert candidate.policy.authoritative_source is False
    mappings = stores.signals().get_mappings_for_signal("request_latency", tenant_id="tenant-a")
    taught = [mapping for mapping in mappings if mapping["metric_pattern"] == "AWS/ApplicationELB/TargetResponseTime"]
    assert len(taught) == 1
    assert taught[0]["source_type"] == "operational_knowledge"
    assert taught[0]["confidence"] == 0.95


def test_manual_signal_reteach_uses_scope_in_candidate_identity(tmp_path: Path):
    db_path = tmp_path / "scoped-signals.db"
    app = create_app(
        runtime_settings=Settings(
            signals_db_path=str(db_path),
            knowledge_tenant_id="tenant-a",
        )
    )
    client = TestClient(app)
    base_payload = {
        "signal_type": "request_latency",
        "metric_patterns": [{"pattern": "shared_latency_seconds", "confidence": 0.9}],
        "category": "latency",
        "taught_by": "operator",
    }
    scopes = (
        {
            "services": ["checkout"],
            "environments": ["environment:production"],
            "datasource_types": ["prometheus"],
        },
        {
            "services": ["payments"],
            "environments": ["environment:staging"],
            "datasource_types": ["prometheus"],
        },
        {
            "services": ["payments"],
            "environments": ["environment:staging"],
            "datasource_types": ["cloudwatch"],
        },
    )

    responses = [client.post("/api/v1/signals/teach", json={**base_payload, **scope}) for scope in scopes]

    assert [response.status_code for response in responses] == [200, 200, 200]
    candidates = app.state.runtime_stores.knowledge_repository().list_candidates(
        "tenant-a",
        kind=KnowledgeKind.SIGNAL_MAPPING.value,
    )
    assert len(candidates) == 3
    assert len({candidate.id for candidate in candidates}) == 3


@pytest.mark.parametrize(
    ("correction_type", "expected_status"),
    [
        (CorrectionType.KNOWLEDGE_STALE, LifecycleStatus.STALE),
        (CorrectionType.KNOWLEDGE_INCORRECT, LifecycleStatus.WITHDRAWN),
    ],
)
def test_stale_and_incorrect_corrections_retire_their_target(
    tmp_path: Path,
    correction_type: CorrectionType,
    expected_status: LifecycleStatus,
):
    service = _service(tmp_path)
    _, target = _promoted_dependency(service)
    correction, _ = service.create_correction(
        investigation_id="inv-retire-target",
        investigation_revision=1,
        correction_type=correction_type,
        target_ref=target.knowledge_id,
        proposed={
            "subject_ref": "concept:artifact-quality",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:retirement-review",
        },
        scope=KnowledgeScope(),
        explanation="The governed source is no longer valid.",
        created_by="operator",
    )

    _, retired = service.review_correction(
        correction.id,
        approved=True,
        reviewer="operator",
        authoritative=True,
    )

    assert retired is not None
    assert retired.knowledge_id == target.knowledge_id
    assert retired.state.lifecycle_status == expected_status
    assert retired.state.eligibility == KnowledgeEligibility.INELIGIBLE


@pytest.mark.parametrize(
    "correction_type",
    [CorrectionType.KNOWLEDGE_STALE, CorrectionType.KNOWLEDGE_INCORRECT],
)
def test_target_required_corrections_remain_pending_when_target_is_missing(
    tmp_path: Path,
    correction_type: CorrectionType,
):
    service = _service(tmp_path)
    correction, candidate = service.create_correction(
        investigation_id="inv-missing-target",
        investigation_revision=1,
        correction_type=correction_type,
        proposed={
            "subject_ref": "concept:artifact-quality",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:retirement-review",
        },
        scope=KnowledgeScope(),
        explanation="This correction should have selected a target.",
        created_by="operator",
    )

    with pytest.raises(ValueError, match="requires target_ref"):
        service.review_correction(
            correction.id,
            approved=True,
            reviewer="operator",
            authoritative=True,
        )

    assert service.repository.get_correction(correction.id).review_state == ReviewState.CANDIDATE
    assert service.repository.get_candidate(candidate.id).state.review_state == ReviewState.CANDIDATE


def test_entity_kind_is_immutable_for_registered_identity(tmp_path: Path):
    service = _service(tmp_path)
    checkout = service.repository.get_entity("entity:service:checkout")
    assert checkout is not None

    with pytest.raises(ValueError, match="entity kind cannot change"):
        service.register_entity(checkout.model_copy(update={"kind": EntityKind.TEAM}))

    stored = service.repository.get_entity(checkout.id)
    assert stored is not None
    assert stored.kind == EntityKind.SERVICE


def test_pending_counter_proposition_does_not_disable_active_knowledge(tmp_path: Path):
    service = _service(tmp_path)
    _, active = _promoted_dependency(service)
    pending = _dependency(
        service,
        payload_ref="pending-negative",
        family=SourceFamily.INCIDENT,
        lineage_group="pending-negative",
        predicate="does_not_depend_on",
    )

    conflicts = service.conflicts.analyze("default", pending.proposition.proposition_key)
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    assert conflicts == []
    active_usage = next(item for item in usage if item.knowledge_ref == active.knowledge_id)
    assert active_usage.disposition.value == "considered_not_applied"


def test_complementary_evidence_requirements_do_not_conflict(tmp_path: Path):
    service = _service(tmp_path)
    candidates = []
    for signal in ("latency", "error-rate"):
        candidate = service.create_candidate(
            kind=KnowledgeKind.EVIDENCE_REQUIREMENT,
            payload_ref=f"require-{signal}",
            typed_payload={},
            proposition={
                "subject_ref": "entity:service:checkout",
                "predicate": "requires_observation",
                "concept_ref": f"signal:{signal}",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            provenance_refs=[f"runbook:{signal}"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        candidates.append(candidate)

    assert service.conflicts.analyze("default", candidates[0].proposition.proposition_key) == []


def test_removed_source_retires_promoted_knowledge(tmp_path: Path):
    service = _service(tmp_path)
    _, active = _promoted_dependency(service)

    retired = service.reconcile_source_lifecycle(
        provenance_ref="provenance:runbook",
        active_candidate_ids=set(),
    )

    assert len(retired) == 1
    assert retired[0].knowledge_id == active.knowledge_id
    assert retired[0].state.lifecycle_status == LifecycleStatus.STALE
    assert service.repository.get_revision(active.knowledge_id).state.eligibility == KnowledgeEligibility.INELIGIBLE


def test_removed_source_keeps_knowledge_active_when_independent_support_remains(tmp_path: Path):
    service = _service(tmp_path)
    removed, active = _promoted_dependency(service)
    survivor = _dependency(
        service,
        payload_ref="incident",
        family=SourceFamily.INCIDENT,
        lineage_group="incident:1",
    )
    service.review_candidate(survivor.id, approved=True, reviewer="reviewer")

    reconciled = service.reconcile_source_lifecycle(
        provenance_ref="provenance:runbook",
        active_candidate_ids=set(),
    )

    assert len(reconciled) == 1
    current = service.repository.get_revision(active.knowledge_id)
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert current.state.eligibility == KnowledgeEligibility.CONTEXTUAL_ONLY
    assert removed.id not in current.promoted_from_candidate_refs
    assert set(current.promoted_from_candidate_refs) == (set(active.promoted_from_candidate_refs) - {removed.id}) | {
        survivor.id
    }


def test_removed_source_reuses_authoritative_override_from_surviving_candidate(tmp_path: Path):
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
    candidates = []
    latest = None
    for source in ("catalog-a", "catalog-b"):
        candidate = service.create_candidate(
            kind=KnowledgeKind.OWNERSHIP,
            payload_ref=source,
            typed_payload={},
            proposition={
                "subject_ref": "entity:service:checkout",
                "predicate": "owned_by",
                "object_ref": "entity:team:payments",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            evidence=[
                KnowledgeEvidenceReference(
                    evidence_ref=f"evidence:{source}",
                    source_family=SourceFamily.SERVICE_CATALOG,
                    lineage_group=source,
                    provenance_refs=[f"provenance:{source}"],
                )
            ],
            provenance_refs=[f"provenance:{source}"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        _, latest = service.evaluate_candidate(candidate.id, authoritative_source=True)
        assert latest is not None
        candidates.append(candidate)

    persisted_survivor = service.repository.get_candidate(candidates[1].id)
    assert persisted_survivor is not None
    assert persisted_survivor.policy.authoritative_source is True

    service.reconcile_source_lifecycle(
        provenance_ref="provenance:catalog-a",
        active_candidate_ids=set(),
    )

    current = service.repository.get_revision(latest.knowledge_id)
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert current.state.eligibility == KnowledgeEligibility.CONTEXTUAL_ONLY
    assert current.promoted_from_candidate_refs == [candidates[1].id]


def test_removed_source_reuses_live_verified_override_from_surviving_candidate(tmp_path: Path):
    service = _service(tmp_path)
    candidates = []
    latest = None
    for source, family in (("dashboard-a", SourceFamily.DASHBOARD), ("alert-b", SourceFamily.ALERT)):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=source,
            typed_payload={"metric": "http_request_duration_seconds"},
            proposition={
                "subject_ref": "concept:latency",
                "predicate": "represented_by",
                "object_ref": "concept:http_request_duration_seconds",
                "concept_ref": "signal:latency",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            evidence=[
                KnowledgeEvidenceReference(
                    evidence_ref=f"evidence:{source}",
                    source_family=family,
                    lineage_group=source,
                    provenance_refs=[f"provenance:{source}"],
                )
            ],
            provenance_refs=[f"provenance:{source}"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        _, latest = service.evaluate_candidate(candidate.id, live_verified=True)
        assert latest is not None
        candidates.append(candidate)

    persisted_survivor = service.repository.get_candidate(candidates[1].id)
    assert persisted_survivor is not None
    assert persisted_survivor.policy.live_verified is True

    service.reconcile_source_lifecycle(
        provenance_ref="provenance:dashboard-a",
        active_candidate_ids=set(),
    )

    current = service.repository.get_revision(latest.knowledge_id)
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert current.state.eligibility == KnowledgeEligibility.LIVE_VERIFIED
    assert current.promoted_from_candidate_refs == [candidates[1].id]


def test_rejecting_promoted_contributor_recomputes_current_knowledge(tmp_path: Path):
    service = _service(tmp_path)
    rejected, active = _promoted_dependency(service)

    service.review_candidate(rejected.id, approved=False, reviewer="operator")

    current = service.repository.get_revision(active.knowledge_id)
    assert current is not None
    assert current.revision == active.revision + 1
    assert current.state.lifecycle_status == LifecycleStatus.WITHDRAWN
    assert current.state.eligibility == KnowledgeEligibility.INELIGIBLE
    assert service.repository.stats()["lifecycle"]["withdrawn"] == 1


def test_reingested_stale_candidate_reactivates_and_is_reevaluated(tmp_path: Path):
    service = _service(tmp_path)
    first, active = _promoted_dependency(service)
    service.reconcile_source_lifecycle(
        provenance_ref="provenance:runbook",
        active_candidate_ids=set(),
    )
    stale = service.repository.get_candidate(first.id)
    assert stale is not None
    assert stale.state.lifecycle_status == LifecycleStatus.STALE

    restored = service.create_candidate(
        kind=first.kind,
        payload_ref=first.payload_ref,
        typed_payload=first.typed_payload,
        proposition=first.proposition,
        scope=first.scope,
        evidence=first.evidence.items,
        provenance_refs=first.provenance_refs,
        candidate_id=first.id,
        reactivate_stale=True,
    )
    service.evaluate_candidate(restored.id)

    current = service.repository.get_revision(active.knowledge_id)
    assert restored.state.review_state == ReviewState.APPROVED
    assert restored.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert current.state.eligibility == KnowledgeEligibility.CONTEXTUAL_ONLY


def test_repeated_candidate_evaluation_reuses_unchanged_revision(tmp_path: Path):
    service = _service(tmp_path)
    candidate, original = _promoted_dependency(service)

    decision, repeated = service.evaluate_candidate(candidate.id)

    assert decision.decision.value == "promote"
    assert repeated == original
    assert len(service.repository.list_revisions(original.knowledge_id)) == 1


def test_concurrent_candidate_evaluation_reuses_the_committed_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    first = _dependency(
        service,
        payload_ref="concurrent-runbook",
        family=SourceFamily.RUNBOOK,
        lineage_group="concurrent-runbook",
    )
    second = _dependency(
        service,
        payload_ref="concurrent-dashboard",
        family=SourceFamily.DASHBOARD,
        lineage_group="concurrent-dashboard",
    )
    service.review_candidate(first.id, approved=True, reviewer="reviewer")
    service.review_candidate(second.id, approved=True, reviewer="reviewer")
    barrier = threading.Barrier(2)
    persist_revision = service.repository.persist_revision

    def synchronized_persist(revision, **kwargs):
        barrier.wait(timeout=5)
        return persist_revision(revision, **kwargs)

    monkeypatch.setattr(service.repository, "persist_revision", synchronized_persist)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: service.evaluate_candidate(first.id), range(2)))

    revisions = [revision for _, revision in results]
    assert all(revision is not None for revision in revisions)
    assert len({(revision.knowledge_id, revision.revision) for revision in revisions if revision}) == 1
    assert len(service.repository.list_revisions(revisions[0].knowledge_id)) == 1


@pytest.mark.parametrize(
    ("concurrent_transition", "expected_review", "expected_lifecycle"),
    [
        ("reject", ReviewState.REJECTED, LifecycleStatus.ACTIVE),
        ("stale", ReviewState.APPROVED, LifecycleStatus.STALE),
    ],
)
def test_candidate_evaluation_cannot_overwrite_concurrent_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    concurrent_transition: str,
    expected_review: ReviewState,
    expected_lifecycle: LifecycleStatus,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref=f"evaluation-race-{concurrent_transition}",
        family=SourceFamily.HUMAN_CORRECTION,
        lineage_group=f"evaluation-race-{concurrent_transition}",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
    evaluation_reached_save = threading.Event()
    allow_evaluation_save = threading.Event()
    save_evaluation = service.repository.save_candidate_evaluation

    def delayed_save(evaluated, *, expected):
        evaluation_reached_save.set()
        assert allow_evaluation_save.wait(timeout=5)
        return save_evaluation(evaluated, expected=expected)

    monkeypatch.setattr(service.repository, "save_candidate_evaluation", delayed_save)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            service.evaluate_candidate,
            candidate.id,
            authoritative_source=True,
        )
        assert evaluation_reached_save.wait(timeout=5)
        if concurrent_transition == "reject":
            service.review_candidate(candidate.id, approved=False, reviewer="rejector")
        else:
            service.reconcile_source_lifecycle(
                provenance_ref=candidate.provenance_refs[0],
                active_candidate_ids=set(),
            )
        allow_evaluation_save.set()
        with pytest.raises(CandidateEvaluationConflictError):
            future.result(timeout=5)

    persisted = service.repository.get_candidate(candidate.id)
    assert persisted is not None
    assert persisted.state.review_state == expected_review
    assert persisted.state.lifecycle_status == expected_lifecycle
    assert service.repository.list_current_revisions() == []


def test_overlapping_candidate_reviews_compare_against_the_loaded_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="concurrent-review",
        family=SourceFamily.RUNBOOK,
        lineage_group="concurrent-review",
    )
    barrier = threading.Barrier(2)
    require_candidate = service._require_candidate

    def synchronized_load(candidate_id, tenant_id):
        loaded = require_candidate(candidate_id, tenant_id)
        barrier.wait(timeout=5)
        return loaded

    monkeypatch.setattr(service, "_require_candidate", synchronized_load)

    def review(approved: bool):
        try:
            updated = service.review_candidate(
                candidate.id,
                approved=approved,
                reviewer=f"reviewer-{approved}",
            )
        except CandidateReviewConflictError:
            return "conflict"
        return updated.state.review_state.value

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(review, [True, False]))

    assert results.count("conflict") == 1
    stored = service.repository.get_candidate(candidate.id)
    assert stored is not None
    assert stored.state.review_state.value in set(results)


def test_duplicate_overlapping_reviews_are_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="duplicate-review",
        family=SourceFamily.RUNBOOK,
        lineage_group="duplicate-review",
    )
    barrier = threading.Barrier(2)
    require_candidate = service._require_candidate

    def synchronized_load(candidate_id, tenant_id):
        loaded = require_candidate(candidate_id, tenant_id)
        barrier.wait(timeout=5)
        return loaded

    monkeypatch.setattr(service, "_require_candidate", synchronized_load)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda reviewer: service.review_candidate(
                    candidate.id,
                    approved=True,
                    reviewer=reviewer,
                ),
                ["reviewer-a", "reviewer-b"],
            )
        )

    assert {result.state.review_state for result in results} == {ReviewState.APPROVED}
    stored = service.repository.get_candidate(candidate.id)
    assert stored is not None
    assert stored.state.review_state == ReviewState.APPROVED


def test_unresolved_approved_candidate_does_not_create_conflict(tmp_path: Path):
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
    resolved = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="resolved-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:payments",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["catalog:ownership"],
    )
    unresolved = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="unresolved-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "unknown-team",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["artifact:ownership"],
    )
    service.review_candidate(resolved.id, approved=True, reviewer="operator")
    service.review_candidate(unresolved.id, approved=True, reviewer="operator")

    assert unresolved.entity_resolution.status.value == "unresolved"
    assert service.conflicts.analyze("default", resolved.proposition.proposition_key) == []


def test_investigation_scope_extracts_supported_prompt_dimensions():
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt=(
            "Investigate checkout in production us-east-1, cluster: prod-east, namespace payments on release v2.4.1"
        ),
        services=["Checkout API"],
        archetype_ids=["latency_investigation"],
    )

    assert "environment:production" in scope.environment_refs
    assert "region:us-east-1" in scope.region_refs
    assert "cluster:prod-east" in scope.cluster_refs
    assert "namespace:payments" in scope.namespace_refs
    assert "entity:service:checkout-api" in scope.service_refs
    assert "archetype:latency_investigation" in scope.archetype_refs
    assert "version:v2.4.1" in scope.version_constraints


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
    assert candidate.policy.promotion_policy_ref == "dependency-promotion-v1"
    assert candidate.policy.eligibility_reason_codes == ["insufficient_independent_sources"]
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


def test_approved_legacy_rows_with_unknown_lineage_remain_unpromoted(tmp_path: Path):
    service = _service(tmp_path)
    base_row = {
        "source_entity": "entity:service:checkout",
        "target_entity": "entity:datastore:redis-session",
        "direction": "depends_on",
        "source_excerpt": "bounded excerpt",
        "review_state": "approved",
    }
    first_ids = migrate_artifact_extractions(
        artifact_id="artifact_runbook",
        artifact_type="runbook",
        rows={"dependency_hints": [{"id": "dep_runbook", **base_row}]},
        service=service,
    )
    first = service.repository.get_candidate(first_ids[0])
    assert first is not None
    assert service.repository.find_knowledge_by_proposition("default", first.proposition.proposition_key) is None

    second_ids = migrate_artifact_extractions(
        artifact_id="artifact_dashboard",
        artifact_type="dashboard",
        rows={"dependency_hints": [{"id": "dep_dashboard", **base_row}]},
        service=service,
    )

    promoted = service.repository.find_knowledge_by_proposition("default", first.proposition.proposition_key)
    second = service.repository.get_candidate(second_ids[0])
    assert promoted is None
    assert second is not None
    assert second.policy.last_evaluated_at is not None
    assert "insufficient_independent_sources" in second.policy.eligibility_reason_codes


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


def test_signal_mapping_candidate_ids_are_url_safe_and_reviewable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path)
    candidate_id = migrate_signal_mapping(
        {
            "id": "cloudwatch:AWS/ApplicationELB:TargetResponseTime",
            "signal_type": "request_latency",
            "metric_pattern": "AWS/ApplicationELB/TargetResponseTime",
            "source_type": "alert_ingest",
            "source_refs": ["cloudwatch:alarm:checkout-latency"],
        },
        service=service,
    )

    assert candidate_id.startswith("kc_signal_")
    assert candidate_id.removeprefix("kc_signal_").isalnum()
    candidate = service.repository.get_candidate(candidate_id)
    assert candidate is not None
    assert candidate.typed_payload["metric_pattern"] == "AWS/ApplicationELB/TargetResponseTime"

    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    app = create_app(
        runtime_settings=Settings(
            knowledge_permissions="knowledge.read,knowledge.review",
        )
    )
    response = TestClient(app).post(
        f"/api/v1/knowledge/candidates/{candidate_id}/review",
        json={"decision": "approve", "reviewer": "operator", "evaluate": False},
    )

    assert response.status_code == 200
    assert response.json()["candidate"]["id"] == candidate_id


def test_global_knowledge_repository_uses_active_signal_store_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import tacit.knowledge.repository as repository_module
    import tacit.signals as signals_module
    import tacit.signals.store as signal_store_module

    active_path = tmp_path / "isolated-learning.db"
    monkeypatch.setattr(signals_module, "_DEFAULT_DB_PATH", active_path)
    monkeypatch.setattr(signals_module, "_store", None)
    monkeypatch.setattr(signal_store_module, "_DEFAULT_DB_PATH", active_path)
    monkeypatch.setattr(signal_store_module, "_store", None)
    monkeypatch.setattr(repository_module, "_repository", None)
    signal_store = signals_module.get_signal_store()
    candidate_id = migrate_signal_mapping(
        {
            "id": "isolated:request-latency",
            "signal_type": "request_latency",
            "metric_pattern": "isolated_request_latency_seconds",
            "source_type": "dashboard_ingest",
            "source_refs": ["dashboard:isolated"],
        },
        service=KnowledgeService(KnowledgeRepository(signal_store._db_path)),
    )

    active_repository = repository_module.get_knowledge_repository()

    assert active_repository._db_path == signal_store._db_path == active_path
    assert active_repository.get_candidate(candidate_id) is not None


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

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
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


def test_review_queue_prioritizes_before_applying_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path)
    high_priority = _dependency(
        service,
        payload_ref="older-security-review",
        family=SourceFamily.RUNBOOK,
        lineage_group="older-security-review",
    )
    high_priority = high_priority.model_copy(update={"security_flags": ["possible_prompt_injection"]})
    service.repository.save_candidate(high_priority)
    newer_low_priority = _dependency(
        service,
        payload_ref="newer-routine-review",
        family=SourceFamily.RUNBOOK,
        lineage_group="newer-routine-review",
    )
    with service.repository._conn() as conn:
        conn.execute("UPDATE knowledge_candidates SET created_at=1 WHERE id=?", (high_priority.id,))
        conn.execute("UPDATE knowledge_candidates SET created_at=2 WHERE id=?", (newer_low_priority.id,))

    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    app = create_app(runtime_settings=Settings(knowledge_permissions="knowledge.read"))

    response = TestClient(app).get("/api/v1/knowledge/review-queue?limit=1")

    assert response.status_code == 200
    assert [candidate["id"] for candidate in response.json()["candidates"]] == [high_priority.id]
    assert response.json()["candidates"][0]["review_priority_reasons"] == [
        "security_review",
        "investigation_impact",
    ]


def test_api_aliases_use_resolver_normalization(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
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

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
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


@pytest.mark.parametrize(
    ("permissions", "missing_permission"),
    [
        ("knowledge.read,knowledge.trust", "knowledge.review"),
        ("knowledge.read,knowledge.review", "knowledge.trust"),
    ],
)
def test_api_candidate_trust_requires_review_and_trust_permissions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    permissions: str,
    missing_permission: str,
):
    service = _service(tmp_path, "tenant-a")
    candidate = _dependency(
        service,
        payload_ref=f"trust-matrix:{missing_permission}",
        family=SourceFamily.RUNBOOK,
        lineage_group=f"trust-matrix:{missing_permission}",
        tenant_id="tenant-a",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                knowledge_tenant_id="tenant-a",
                knowledge_permissions=permissions,
            )
        )
    )

    response = client.post(
        f"/api/v1/knowledge/candidates/{candidate.id}/review",
        json={"decision": "trust", "reviewer": "operator", "evaluate": False},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == f"Missing permission: {missing_permission}"
    stored = service.repository.get_candidate(candidate.id, "tenant-a")
    assert stored is not None
    assert stored.state.review_state == ReviewState.CANDIDATE


def test_api_reports_concurrent_candidate_review_as_conflict(monkeypatch: pytest.MonkeyPatch):
    import tacit.api.routes.knowledge as routes

    class ConflictingService:
        def review_candidate(self, *args, **kwargs):
            raise CandidateReviewConflictError("candidate review state changed; reload before reviewing")

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: ConflictingService())
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                knowledge_permissions="knowledge.read,knowledge.review",
            )
        )
    )

    response = client.post(
        "/api/v1/knowledge/candidates/kc-concurrent/review",
        json={"decision": "approve", "reviewer": "operator"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "candidate review state changed; reload before reviewing"


def test_api_reports_stale_correction_supersession_as_conflict(monkeypatch: pytest.MonkeyPatch):
    import tacit.api.routes.knowledge as routes

    class ConflictingService:
        def review_correction(self, *args, **kwargs):
            raise KnowledgeRevisionConflictError(
                "knowledge target advanced from revision 1 to 2; rebase the correction"
            )

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: ConflictingService())
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                knowledge_permissions="knowledge.read,knowledge.review",
            )
        )
    )

    response = client.post(
        "/api/v1/knowledge/corrections/correction-concurrent/review",
        json={"decision": "approve", "reviewer": "operator"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "knowledge target advanced from revision 1 to 2; rebase the correction"


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

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
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


def test_api_review_returns_post_evaluation_candidate_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    candidate = _dependency(
        service,
        payload_ref="evaluated-response",
        family=SourceFamily.RUNBOOK,
        lineage_group="evaluated-response",
        tenant_id="tenant-a",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.override",
        )
    )

    response = TestClient(app).post(
        f"/api/v1/knowledge/{candidate.id}/review",
        json={
            "decision": "approve",
            "reviewer": "operator",
            "authoritative_source": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["promotion_decision"]["decision"] == "promote"
    assert body["knowledge_revision"] is not None
    assert body["candidate"]["state"]["eligibility"] == "contextual_only"
    assert body["candidate"]["policy"]["promotion_policy_ref"] == "dependency-promotion-v1"


def test_approved_candidate_can_be_evaluated_on_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    candidate = _dependency(
        service,
        payload_ref="deferred-evaluation",
        family=SourceFamily.RUNBOOK,
        lineage_group="deferred-evaluation",
        tenant_id="tenant-a",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    app = create_app(
        runtime_settings=Settings(
            api_auth_enabled=False,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.override",
        )
    )
    client = TestClient(app)

    reviewed = client.post(
        f"/api/v1/knowledge/{candidate.id}/review",
        json={"decision": "approve", "reviewer": "operator", "evaluate": False},
    )
    evaluated = client.post(
        f"/api/v1/knowledge/{candidate.id}/review",
        json={
            "decision": "approve",
            "reviewer": "operator",
            "evaluate": True,
            "authoritative_source": True,
        },
    )

    assert reviewed.status_code == 200
    assert reviewed.json()["promotion_decision"] is None
    assert evaluated.status_code == 200
    assert evaluated.json()["promotion_decision"]["decision"] == "promote"
    assert evaluated.json()["knowledge_revision"] is not None


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

    monkeypatch.setattr(routes, "get_knowledge_repository", lambda request: service.repository)
    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
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


def test_cli_trust_requires_review_and_trust_permissions(monkeypatch: pytest.MonkeyPatch):
    class UnexpectedService:
        def review_candidate(self, *args, **kwargs):
            raise AssertionError("unauthorized trust reached the knowledge service")

    monkeypatch.setattr("tacit.cli._cli_knowledge_service", lambda: UnexpectedService())
    monkeypatch.setattr("tacit.config.settings.knowledge_permissions", "knowledge.trust")

    result = CliRunner().invoke(
        cli,
        [
            "knowledge",
            "review",
            "candidate",
            "--trust",
            "--reviewer",
            "operator",
        ],
    )

    assert result.exit_code != 0
    assert "missing permission: knowledge.review" in result.output


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
    assert "--tenant" in runner.invoke(cli, ["learn", "dashboard", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["learn", "alerts", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["learn", "approve", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["investigate", "--help"]).output
    assert "--tenant" in runner.invoke(cli, ["test", "--help"]).output


def test_artifact_learning_cli_missing_tenant_exits_nonzero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runbook = tmp_path / "runbook.md"
    incident = tmp_path / "incident.md"
    runbook.write_text("Checkout depends on Redis.")
    incident.write_text("Checkout latency increased during the incident.")
    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "*")
    runner = CliRunner()

    runbook_result = runner.invoke(cli, ["learn", "runbooks", "--file", str(runbook)])
    incident_result = runner.invoke(cli, ["learn", "incidents", "--file", str(incident)])

    assert runbook_result.exit_code != 0
    assert incident_result.exit_code != 0
    assert "--tenant is required" in runbook_result.output
    assert "--tenant is required" in incident_result.output


def test_cli_rejects_the_reserved_bootstrap_tenant():
    with pytest.raises(ClickException, match="Invalid knowledge tenant"):
        _knowledge_tenant(
            "*bootstrap*",
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
        )


def test_pending_cli_learning_preserves_wildcard_tenant(monkeypatch: pytest.MonkeyPatch):
    seen: list[tuple[str, str | None]] = []

    async def ingest_dashboard(**kwargs):
        seen.append(("dashboard", kwargs["tenant_id"]))
        return {"dashboard_uid": kwargs["dashboard_uid"], "status": "pending"}

    async def learn_dashboards(*args, **kwargs):
        seen.append(("bulk-dashboard", kwargs["tenant_id"]))
        return {"dashboards_learned": 0, "signals_inferred": 0, "mappings_created": 0, "warnings": []}

    async def learn_alerts(*args, **kwargs):
        seen.append(("alerts", kwargs["tenant_id"]))
        return {"alerts_learned": 0, "signals_inferred": 0, "mappings_created": 0, "warnings": []}

    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "*")
    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", ingest_dashboard)
    monkeypatch.setattr("tacit.dashboard_ingest.learn_backend_dashboards", learn_dashboards)
    monkeypatch.setattr("tacit.alert_ingest.learn_backend_alerts", learn_alerts)
    runner = CliRunner()

    results = [
        runner.invoke(cli, ["learn", "dashboard", "dash-1", "--pending", "--tenant", "acme"]),
        runner.invoke(cli, ["learn", "grafana", "--pending", "--tenant", "acme"]),
        runner.invoke(cli, ["learn", "alerts", "--pending", "--tenant", "acme"]),
    ]

    assert all(result.exit_code == 0 for result in results)
    assert seen == [("dashboard", "acme"), ("bulk-dashboard", "acme"), ("alerts", "acme")]


def test_cli_reject_and_ignore_thread_wildcard_tenant(monkeypatch: pytest.MonkeyPatch):
    seen: list[tuple[str, str]] = []

    class FakeSignalStore:
        def ignore_ingested_dashboard(self, dashboard_uid, *, backend_name, tenant_id):
            seen.append(("ignore", tenant_id))
            return True

    class FakeStores:
        def signals(self):
            return FakeSignalStore()

    def reject_dashboard_record(**kwargs):
        seen.append(("reject", kwargs["tenant_id"]))
        return {"status": "rejected", "rejected_candidates": 0}

    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "*")
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: FakeStores())
    monkeypatch.setattr(
        "tacit.dashboard_ingest.reject_ingested_dashboard_record",
        reject_dashboard_record,
    )
    runner = CliRunner()

    rejected = runner.invoke(cli, ["learn", "reject", "dash-1", "--tenant", "acme"])
    ignored = runner.invoke(cli, ["learn", "ignore", "dash-2", "--tenant", "acme"])

    assert rejected.exit_code == 0, rejected.output
    assert ignored.exit_code == 0, ignored.output
    assert seen == [("reject", "acme"), ("ignore", "acme")]


def test_knowledge_ui_sends_selected_tenant_header():
    html = Path("tacit/static/index.html").read_text()

    assert 'id="knowledge-tenant"' in html
    assert "'X-Tacit-Tenant': tenant" in html
    assert "knowledgeHeaders({ 'Content-Type': 'application/json' })" in html
    assert "if (tenant) payload.tenant_id = tenant" in html
    assert "headers: knowledgeHeaders()," in html
    assert "fetch(`${BASE}/api/v1/investigations${qs}`, { headers: knowledgeHeaders() })" in html
    assert "fetch(`${BASE}/api/v1/investigations/${id}`, { headers: knowledgeHeaders() })" in html
    assert "fetch(`${BASE}/api/v1/investigations/stats`, { headers: knowledgeHeaders() })" in html


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


def test_causal_benchmark_reviews_candidate_before_evaluation(monkeypatch: pytest.MonkeyPatch):
    observed_states: list[ReviewState] = []
    original_evaluate = KnowledgeService.evaluate_candidate

    def record_review_state(self, candidate_id, *args, **kwargs):
        candidate = self.repository.get_candidate(candidate_id)
        if candidate is not None and candidate.payload_ref == "historical-causal-claim":
            observed_states.append(candidate.state.review_state)
        return original_evaluate(self, candidate_id, *args, **kwargs)

    monkeypatch.setattr(KnowledgeService, "evaluate_candidate", record_review_state)

    report = run_operational_learning_benchmark()

    assert report["passed"] is True
    assert observed_states == [ReviewState.APPROVED]
