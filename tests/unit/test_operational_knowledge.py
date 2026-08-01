from __future__ import annotations

import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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
    Predicate,
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
    KnowledgeCorrection,
    KnowledgeEvidenceReference,
    KnowledgeRevision,
    KnowledgeScope,
    KnowledgeState,
    KnowledgeUsage,
)
from tacit.knowledge.normalization import canonical_scope_payload, normalize_service_ref
from tacit.knowledge.repository import (
    AliasRegistrationConflictError,
    CandidateEvaluationConflictError,
    CandidateReviewConflictError,
    KnowledgeRepository,
    KnowledgeRevisionConflictError,
    _conflict_lookup_sql,
)
from tacit.knowledge.scope import investigation_knowledge_scope
from tacit.knowledge.service import KnowledgeService
from tacit.knowledge.usage import (
    KnowledgeRevisionRef,
    KnowledgeStageUse,
    KnowledgeUsageEffect,
    KnowledgeUsageStage,
)
from tacit.knowledge.versioning import version_scopes_overlap
from tacit.models.schemas import CulpritCandidate, CulpritRanking, EvidenceObservation, EvidenceObservationOutcome
from tacit.operational_learning_benchmark import (
    load_operational_learning_corpus,
    run_operational_learning_benchmark,
)
from tacit.tenancy import TenantBoundaryError


def _service(tmp_path: Path, tenant_id: str = "default") -> KnowledgeService:
    service = KnowledgeService(
        KnowledgeRepository(tmp_path / "knowledge.db"),
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_id),
    )
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
    reactivate_stale: bool = False,
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
        reactivate_stale=reactivate_stale,
    )


def _independent_support(
    evidence_ref: str,
    *,
    family: SourceFamily = SourceFamily.SERVICE_CATALOG,
) -> list[KnowledgeEvidenceReference]:
    return [
        KnowledgeEvidenceReference(
            evidence_ref=evidence_ref,
            evidence_role=EvidenceRole.SUPPORTING,
            source_family=family,
            lineage_group=evidence_ref,
            lineage_kind=LineageKind.INDEPENDENT,
            provenance_refs=[f"provenance:{evidence_ref}"],
        )
    ]


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


def test_snapshot_preloads_active_propositions_and_conflicts_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    _promoted_dependency(service)
    postgres_scope = KnowledgeScope(tenant_id="default")
    service.register_entity(
        Entity(
            id="entity:datastore:postgres",
            kind=EntityKind.DATASTORE,
            canonical_name="postgres",
            scope=postgres_scope,
            provenance_refs=["catalog:postgres"],
        )
    )
    candidates = [
        _dependency(
            service,
            payload_ref=f"postgres-{family.value}",
            family=family,
            lineage_group=f"postgres-{family.value}",
            object_ref="entity:datastore:postgres",
        )
        for family in (SourceFamily.RUNBOOK, SourceFamily.DASHBOARD)
    ]
    for candidate in candidates:
        service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
    _, second_revision = service.evaluate_candidate(candidates[0].id)
    assert second_revision is not None

    calls = {"propositions": 0, "conflicts": 0}
    list_propositions = service.repository.list_propositions
    list_conflicts = service.repository.list_conflicts

    def recording_list_propositions(*args, **kwargs):
        calls["propositions"] += 1
        return list_propositions(*args, **kwargs)

    def recording_list_conflicts(*args, **kwargs):
        calls["conflicts"] += 1
        return list_conflicts(*args, **kwargs)

    monkeypatch.setattr(service.repository, "list_propositions", recording_list_propositions)
    monkeypatch.setattr(service.repository, "list_conflicts", recording_list_conflicts)

    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    assert len(usage) == 2
    assert calls == {"propositions": 1, "conflicts": 1}


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


def test_knowledge_candidate_preserves_evidence_abstention(tmp_path: Path):
    service = _service(tmp_path)
    _promoted_dependency(service)
    _, usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        )
    )

    ranking, applied_usage = service.apply_to_ranking(
        CulpritRanking(abstained=True, abstention_reason="no_supported_runtime_evidence"),
        usage,
    )

    assert len(ranking.candidates) == 1
    assert ranking.abstained is True
    assert ranking.abstention_reason == "no_supported_runtime_evidence"
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


def test_scope_references_are_canonical_before_proposition_hashing(tmp_path: Path):
    service = _service(tmp_path)
    raw_scope = KnowledgeScope(
        environment_refs=["Production"],
        region_refs=["US East 1"],
        cluster_refs=["Checkout Primary"],
        namespace_refs=["Checkout Apps"],
        service_refs=["Checkout"],
        archetype_refs=["HTTP Service"],
        version_constraints=[">=1.2"],
    )
    canonical_scope = KnowledgeScope(
        environment_refs=["environment:production"],
        region_refs=["region:us-east-1"],
        cluster_refs=["cluster:checkout-primary"],
        namespace_refs=["namespace:checkout-apps"],
        service_refs=["entity:service:checkout"],
        archetype_refs=["archetype:http-service"],
        version_constraints=["version:>=1.2"],
    )

    first = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="raw-scope",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=raw_scope,
        provenance_refs=["catalog:raw"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="canonical-scope",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=canonical_scope,
        provenance_refs=["catalog:canonical"],
    )

    assert canonical_scope_payload(first.scope) == canonical_scope_payload(second.scope)
    assert first.proposition.proposition_key == second.proposition.proposition_key
    assert first.scope.service_refs == ["entity:service:checkout"]
    assert first.scope.environment_refs == ["environment:production"]
    assert first.scope.version_constraints == ["version:>=1.2"]


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


def test_scope_matching_evaluates_version_ranges_against_concrete_releases(tmp_path: Path):
    service = _service(tmp_path)
    _promoted_dependency(service, version_constraints=["version:>=1.2"])

    _, matching_usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
            version_constraints=["version:v2.4.1"],
        )
    )
    _, rejected_usage = service.create_snapshot(
        KnowledgeScope(
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
            version_constraints=["version:v1.1.9"],
        )
    )

    assert matching_usage[0].disposition == KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED
    assert rejected_usage[0].disposition == KnowledgeUsageDisposition.REJECTED_BY_SCOPE


@pytest.mark.parametrize(
    ("selector", "release", "expected"),
    [
        (">=1.2", "v2.4.1", True),
        ("<2", "v2.4.1", False),
        ("==2.*", "v2.4.1", True),
        ("!=2.*", "v2.4.1", False),
        ("===vendor-build", "vendor-build", True),
        ("===vendor-build", "other-build", False),
    ],
)
def test_scope_version_selector_semantics(selector: str, release: str, expected: bool):
    governed = KnowledgeScope(version_constraints=[selector])
    investigation = KnowledgeScope(version_constraints=[release])

    assert governed.applies_to(investigation) is expected


def test_version_scope_overlap_detects_disjoint_ranges():
    assert version_scopes_overlap(["version:>=2"], ["version:<2"]) is False
    assert version_scopes_overlap(["version:>=1.2,<3"], ["version:==2.*"]) is True
    assert version_scopes_overlap(["version:==2.*"], ["version:!=2.*"]) is False
    assert version_scopes_overlap(["version:!=2.*"], ["version:==2.*"]) is False
    assert version_scopes_overlap(["version:==2.1.*"], ["version:!=2.*"]) is False
    assert version_scopes_overlap(["version:==2.*"], ["version:!=2.1.*"]) is True
    assert version_scopes_overlap(["version:==2.*"], ["version:==3.*"]) is False
    assert version_scopes_overlap(["version:==2.*"], ["version:==2.1.*"]) is True
    assert version_scopes_overlap(["version:>=2,<3"], ["version:!=2.*"]) is False
    assert version_scopes_overlap(["version:~=2.0"], ["version:!=2.*"]) is False
    assert version_scopes_overlap(["version:>=2.1,<2.2"], ["version:!=2.1.*"]) is False
    assert (
        version_scopes_overlap(
            ["version:>=2,<4"],
            ["version:!=2.*,!=3.*"],
        )
        is False
    )
    assert version_scopes_overlap(["version:>=2,<4"], ["version:!=2.*"]) is True
    assert (
        version_scopes_overlap(
            ["version:>=1!2,<1!4"],
            ["version:!=1!2.*,!=1!3.*"],
        )
        is False
    )
    assert version_scopes_overlap(["version:~=1!2.0"], ["version:!=1!2.*"]) is False
    assert version_scopes_overlap(["version:>=1!2,<1!3"], ["version:!=2.*"]) is True
    assert version_scopes_overlap(["version:===vendor-build"], ["version:vendor-build"]) is True
    assert version_scopes_overlap(["version:===vendor-build"], ["version:other-build"]) is False
    assert version_scopes_overlap(["version:===1.0"], ["version:1.0"]) is True
    assert version_scopes_overlap(["version:===1.0"], ["version:1.0.0"]) is False
    assert version_scopes_overlap(["version:===1.0"], ["version:>=1"]) is True
    assert version_scopes_overlap(["version:===1.0"], ["version:<2"]) is True
    assert version_scopes_overlap(["version:===1.0"], ["version:!=1.0"]) is False


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


def test_copied_only_propositions_do_not_create_active_conflicts(tmp_path: Path):
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="independent-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    copied_negative = _dependency(
        service,
        payload_ref="copied-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="copied-source",
        lineage_kind=LineageKind.COPIED_FROM,
        predicate="does_not_depend_on",
    )
    service.review_candidate(positive.id, approved=True, reviewer="reviewer")
    service.review_candidate(copied_negative.id, approved=True, reviewer="reviewer")

    active_keys = {row["proposition_key"] for row in service.repository.list_propositions("default")}

    assert positive.proposition.proposition_key in active_keys
    assert copied_negative.proposition.proposition_key not in active_keys
    assert service.conflicts.analyze("default", positive.proposition.proposition_key) == []


def test_legacy_conflict_without_independent_support_is_resolved_and_can_reopen(tmp_path: Path):
    repository_path = tmp_path / "knowledge.db"
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="legacy-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    negative = _dependency(
        service,
        payload_ref="legacy-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
    )
    positive = service.review_candidate(positive.id, approved=True, reviewer="reviewer")
    negative = service.review_candidate(negative.id, approved=True, reviewer="reviewer")
    initial = service.conflicts.analyze("default", positive.proposition.proposition_key)
    assert initial[0].resolution_status == ConflictResolutionStatus.UNRESOLVED

    copied_evidence = negative.evidence.items[0].model_copy(update={"lineage_kind": LineageKind.COPIED_FROM})
    copied_negative = negative.model_copy(
        update={"evidence": negative.evidence.model_copy(update={"items": [copied_evidence]})}
    )
    service.repository.save_candidate(copied_negative, expected=negative)
    with service.repository._conn() as conn:
        conn.execute(
            "DELETE FROM knowledge_migrations WHERE migration_name=?",
            ("resolve_conflicts_without_independent_support_v1",),
        )

    reconciled_repository = KnowledgeRepository(repository_path)
    resolved = reconciled_repository.list_conflicts("default")
    assert resolved[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_REVIEW
    assert resolved[0].resolution_reason == "counter_proposition_lacks_independent_support"

    reconciled_service = KnowledgeService(
        reconciled_repository,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="default"),
    )
    new_support = _dependency(
        reconciled_service,
        payload_ref="new-independent-negative",
        family=SourceFamily.INCIDENT,
        lineage_group="new-independent-negative",
        predicate="does_not_depend_on",
    )
    new_support = reconciled_service.review_candidate(new_support.id, approved=True, reviewer="reviewer")
    reopened = reconciled_service.conflicts.analyze(
        "default",
        positive.proposition.proposition_key,
        candidate_id=new_support.id,
    )

    assert reopened[0].resolution_status == ConflictResolutionStatus.UNRESOLVED
    reopen_events = [
        event for event in reconciled_repository.list_events("default") if event["event_type"] == "conflict_reopened"
    ]
    assert reopen_events[0]["reason_code"] == "new_independent_support"


def test_reingestion_lineage_downgrade_resolves_existing_conflict(tmp_path: Path):
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="reingested-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    negative = _dependency(
        service,
        payload_ref="reingested-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
    )
    service.review_candidate(positive.id, approved=True, reviewer="reviewer")
    service.review_candidate(negative.id, approved=True, reviewer="reviewer")
    initial = service.conflicts.analyze("default", positive.proposition.proposition_key)
    assert initial[0].resolution_status == ConflictResolutionStatus.UNRESOLVED

    _dependency(
        service,
        payload_ref="reingested-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="copied-negative",
        lineage_kind=LineageKind.COPIED_FROM,
        predicate="does_not_depend_on",
    )

    resolved = service.repository.list_conflicts("default")
    assert resolved[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_REVIEW
    assert resolved[0].resolution_reason == "counter_proposition_lacks_independent_support"


def test_conflict_analysis_filters_repository_lookup_to_plausible_competitors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="filtered-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    negative = _dependency(
        service,
        payload_ref="filtered-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
    )
    service.review_candidate(positive.id, approved=True, reviewer="reviewer")
    service.review_candidate(negative.id, approved=True, reviewer="reviewer")
    with service.repository._conn() as conn:
        conflict_indexes = {
            row["name"] for row in conn.execute("PRAGMA index_list(knowledge_conflicts)").fetchall()
        }
    assert "idx_conflicts_tenant_left_status" in conflict_indexes
    assert "idx_conflicts_tenant_right_status" in conflict_indexes
    calls: list[dict[str, Any]] = []
    list_propositions = service.repository.list_propositions

    def recording_list_propositions(tenant_id="default", **filters):
        calls.append(filters)
        return list_propositions(tenant_id, **filters)

    monkeypatch.setattr(service.repository, "list_propositions", recording_list_propositions)

    service.conflicts.analyze("default", positive.proposition.proposition_key)

    assert calls == [
        {"proposition_key": positive.proposition.proposition_key},
        {
            "kind": KnowledgeKind.DEPENDENCY.value,
            "subject_ref": "entity:service:checkout",
            "predicates": {"depends_on", "does_not_depend_on"},
        },
    ]


def test_conflict_lookup_uses_each_indexed_side(tmp_path: Path):
    repository = KnowledgeRepository(tmp_path / "knowledge.db")
    with repository._conn() as conn:
        plan = conn.execute(
            f"EXPLAIN QUERY PLAN {_conflict_lookup_sql(unresolved_only=True)}",
            ("default", "proposition-a", "default", "proposition-a"),
        ).fetchall()

    details = "\n".join(str(row["detail"]) for row in plan)
    assert "idx_conflicts_tenant_left_status" in details
    assert "idx_conflicts_tenant_right_status" in details


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


def test_stale_last_source_resolves_conflict_and_reactivation_reopens_it(tmp_path: Path):
    service = _service(tmp_path)
    positive = _dependency(
        service,
        payload_ref="active-positive",
        family=SourceFamily.RUNBOOK,
        lineage_group="positive",
    )
    negative = _dependency(
        service,
        payload_ref="stale-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
    )
    service.review_candidate(positive.id, approved=True, reviewer="operator")
    service.review_candidate(negative.id, approved=True, reviewer="operator")
    assert service.conflicts.analyze("default", positive.proposition.proposition_key)[0].resolution_status == (
        ConflictResolutionStatus.UNRESOLVED
    )

    service.reconcile_source_lifecycle(
        provenance_ref="provenance:stale-negative",
        source_stale=True,
    )

    resolved = service.repository.list_conflicts("default")
    assert resolved[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_TIME
    assert resolved[0].resolution_reason == "counter_proposition_stale"
    reactivated = _dependency(
        service,
        payload_ref="stale-negative",
        family=SourceFamily.DASHBOARD,
        lineage_group="negative",
        predicate="does_not_depend_on",
        reactivate_stale=True,
    )
    reopened = service.conflicts.analyze("default", reactivated.proposition.proposition_key)
    assert reopened[0].resolution_status == ConflictResolutionStatus.UNRESOLVED
    assert any(
        event["event_type"] == "conflict_reopened" and event["reason_code"] == "source_reactivated"
        for event in service.repository.list_events("default")
    )


def test_reviewed_support_reopens_conflict_resolved_by_correction(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, correction_candidate = service.create_correction(
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
        evidence=_independent_support("checkout-owner"),
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
        evidence=_independent_support("payment-owner"),
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
        evidence=_independent_support("http-owner"),
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
        evidence=_independent_support("queue-owner"),
        provenance_refs=["catalog:queue"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    service.review_candidate(second.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_SCOPE
    assert conflicts[0].scope_analysis["reason_code"] == "archetype_specific_difference"


def test_conflict_temporal_analysis_reports_disjoint_validity_windows(tmp_path: Path):
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
    boundary = datetime(2026, 7, 1, tzinfo=UTC)
    first = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="historical-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:payments",
        },
        scope=KnowledgeScope(
            service_refs=["entity:service:checkout"],
            valid_until=boundary,
        ),
        evidence=_independent_support("historical-owner"),
        provenance_refs=["catalog:historical"],
    )
    second = service.create_candidate(
        kind=KnowledgeKind.OWNERSHIP,
        payload_ref="current-owner",
        typed_payload={},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": "owned_by",
            "object_ref": "entity:team:platform",
        },
        scope=KnowledgeScope(
            service_refs=["entity:service:checkout"],
            valid_from=boundary,
        ),
        evidence=_independent_support("current-owner"),
        provenance_refs=["catalog:current"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    service.review_candidate(second.id, approved=True, reviewer="operator")

    conflicts = service.conflicts.analyze("default", first.proposition.proposition_key)

    assert len(conflicts) == 1
    assert conflicts[0].resolution_status == ConflictResolutionStatus.RESOLVED_BY_SCOPE
    assert conflicts[0].scope_analysis == {
        "compatible": False,
        "reason_code": "temporal_difference",
    }
    assert conflicts[0].temporal_analysis == {
        "compatible": False,
        "reason_code": "temporal_difference",
    }


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


def test_entity_api_returns_client_error_for_conflicting_identity_kind(tmp_path: Path):
    app = create_app(
        runtime_settings=Settings(
            signals_db_path=str(tmp_path / "entity-validation.db"),
            knowledge_permissions="knowledge.read,knowledge.review",
        )
    )

    response = TestClient(app).post(
        "/api/v1/knowledge/entities",
        json={
            "id": "entity:team:payments",
            "kind": "service",
            "canonical_name": "Payments",
            "provenance_refs": ["operator:entity"],
        },
    )

    assert response.status_code == 400
    assert "entity id kind 'team' does not match declared kind 'service'" in response.json()["detail"]


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


def test_alias_registration_derives_normalized_value_from_raw_value(tmp_path: Path):
    service = _service(tmp_path)
    alias = service.register_alias(
        EntityAlias(
            id="alias-checkout-api-normalized",
            raw_value="Checkout API",
            normalized_value="wrong-value",
            entity_ref="entity:service:checkout",
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=["operator:alias"],
        )
    )

    assert alias.normalized_value == "checkout-api"
    assert [item.id for item in service.repository.find_aliases("default", "checkout-api")] == [alias.id]
    assert service.repository.find_aliases("default", "wrong-value") == []


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


def test_alias_resolution_also_enforces_the_target_entity_scope(tmp_path: Path):
    service = _service(tmp_path)
    service.register_entity(
        Entity(
            id="entity:service:production-storefront",
            kind=EntityKind.SERVICE,
            canonical_name="production-storefront",
            scope=KnowledgeScope(environment_refs=["environment:production"]),
            provenance_refs=["catalog:production"],
        )
    )
    service.register_alias(
        EntityAlias(
            id="alias-storefront-wide",
            raw_value="Storefront",
            normalized_value="storefront",
            entity_ref="entity:service:production-storefront",
            scope=KnowledgeScope(),
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=["operator:alias"],
        )
    )

    staging = service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref="staging-target-scoped-alias",
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
        payload_ref="production-target-scoped-alias",
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


def test_live_negative_evidence_preserves_already_applied_signal_stage_effects(tmp_path: Path):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:checkout-latency",
        typed_payload={"metric_pattern": "checkout_latency_seconds"},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    _, usage = service.create_snapshot(KnowledgeScope(service_refs=["entity:service:checkout"]))
    usage = service.apply_compilation_usage(usage, {revision.knowledge_id})
    usage = service.apply_evidence_usage(usage, {revision.knowledge_id})
    usage = service.apply_stage_usage(
        usage,
        [
            KnowledgeStageUse(
                revision_ref=KnowledgeRevisionRef(revision.knowledge_id, revision.revision),
                stage=KnowledgeUsageStage.ARCHETYPE_SELECTION,
                effect=KnowledgeUsageEffect.ARCHETYPE_SELECTED_BY_LIVE_COVERAGE,
                target_ref="archetype:latency",
            )
        ],
    )

    reconciled = service.reconcile_live_observations(
        usage,
        [
            EvidenceObservation(
                requirement_id="checkout_latency",
                resolution_metric="checkout_latency_seconds",
                outcome=EvidenceObservationOutcome.NEGATIVE_EVIDENCE,
            )
        ],
    )

    applied = reconciled[0]
    assert applied.disposition == KnowledgeUsageDisposition.APPLIED
    assert set(applied.used_for) == {"archetype_selection", "query_compilation", "evidence_resolution"}
    assert "live_negative_observation_after_applied_stage" in applied.reason_codes


def test_compilation_usage_fails_when_a_governed_reference_is_missing_from_the_snapshot(tmp_path: Path):
    service = _service(tmp_path)

    with pytest.raises(RuntimeError, match="not present in the selected knowledge snapshot"):
        service.apply_compilation_usage([], {"knowledge-missing"})


def test_stage_usage_rejects_a_different_revision_of_selected_knowledge(tmp_path: Path):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:revision-pinning",
        typed_payload={"metric_pattern": "checkout_latency_seconds"},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    _, usage = service.create_snapshot(KnowledgeScope(service_refs=["entity:service:checkout"]))

    with pytest.raises(RuntimeError, match=rf"{revision.knowledge_id}@{revision.revision + 1}"):
        service.apply_compilation_usage(
            usage,
            {KnowledgeRevisionRef(revision.knowledge_id, revision.revision + 1)},
        )


def test_stage_usage_preserves_every_confirmed_target(tmp_path: Path):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:stage-targets",
        typed_payload={"metric_pattern": "checkout_latency_seconds"},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    _, usage = service.create_snapshot(KnowledgeScope(service_refs=["entity:service:checkout"]))
    revision_ref = KnowledgeRevisionRef(revision.knowledge_id, revision.revision)

    reconciled = service.apply_stage_usage(
        usage,
        [
            KnowledgeStageUse(
                revision_ref=revision_ref,
                stage=KnowledgeUsageStage.ARCHETYPE_SELECTION,
                effect=KnowledgeUsageEffect.ARCHETYPE_SELECTED_BY_LIVE_COVERAGE,
                target_ref="archetype:latency",
            ),
            KnowledgeStageUse(
                revision_ref=revision_ref,
                stage=KnowledgeUsageStage.ARCHETYPE_SELECTION,
                effect=KnowledgeUsageEffect.ARCHETYPE_SELECTED_BY_LIVE_COVERAGE,
                target_ref="archetype:saturation",
            ),
        ],
    )

    applied = next(item for item in reconciled if item.knowledge_ref == revision.knowledge_id)
    assert "stage_target:archetype_selection:archetype:latency" in applied.reason_codes
    assert "stage_target:archetype_selection:archetype:saturation" in applied.reason_codes


def test_projection_failure_rolls_back_revision_and_retry_succeeds(tmp_path: Path, monkeypatch):
    from tacit.signals.store import SignalStore

    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:custom-latency",
        typed_payload={
            "metric_pattern": "custom_checkout_latency_seconds",
            "confidence": 0.94,
            "context_datasource_types": ["prometheus"],
        },
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:custom_checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["operator:signal-correction"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    original_sync = service._sync_signal_mapping_state
    sync_attempts = 0

    def flaky_sync(revision, **kwargs):
        nonlocal sync_attempts
        sync_attempts += 1
        if sync_attempts == 1:
            raise OSError("resolver projection unavailable")
        original_sync(revision, **kwargs)

    monkeypatch.setattr(service, "_sync_signal_mapping_state", flaky_sync)

    with pytest.raises(OSError, match="resolver projection unavailable"):
        service.evaluate_candidate(candidate.id, live_verified=True)

    knowledge = service.repository.find_knowledge_by_proposition(
        "default",
        candidate.proposition.proposition_key,
    )
    assert knowledge is None
    signal_store = SignalStore(service.repository._db_path)
    assert not any(
        row["metric_pattern"] == "custom_checkout_latency_seconds"
        for row in signal_store.get_mappings_for_signal(
            "request_latency",
            context_service="checkout",
            context_datasource_type="prometheus",
            tenant_id="default",
            include_decayed=True,
        )
    )

    _, repaired = service.evaluate_candidate(candidate.id, live_verified=True)

    assert repaired is not None
    assert repaired.revision == 1
    mappings = signal_store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        context_datasource_type="prometheus",
        tenant_id="default",
        include_decayed=True,
    )
    projection = next(row for row in mappings if row["metric_pattern"] == "custom_checkout_latency_seconds")
    assert projection["governance_ref"] == repaired.knowledge_id
    assert sync_attempts == 2
    assert len(service.repository.list_revisions(repaired.knowledge_id)) == 1


def test_projection_replacement_deactivates_patterns_removed_from_current_revision(tmp_path: Path):
    from tacit.signals.store import SignalStore

    service = _service(tmp_path)
    first = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:first-pattern",
        typed_payload={"metric_pattern": "checkout_latency_old_seconds", "confidence": 0.9},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout-latency",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:first"],
    )
    service.review_candidate(first.id, approved=True, reviewer="operator")
    _, original = service.evaluate_candidate(first.id, live_verified=True)
    assert original is not None
    second = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:replacement-pattern",
        typed_payload={"metric_pattern": "checkout_latency_new_seconds", "confidence": 0.8},
        proposition=first.proposition,
        scope=first.scope,
        provenance_refs=["dashboard:second"],
    )
    service.review_candidate(second.id, approved=True, reviewer="operator")
    replacement = original.model_copy(
        update={
            "revision": original.revision + 1,
            "parent_revision": original.revision,
            "promoted_from_candidate_refs": [second.id],
            "provenance_refs": second.provenance_refs,
            "resolver_payload": {
                "mappings": [
                    {
                        "metric_pattern": "checkout_latency_new_seconds",
                        "confidence": 0.8,
                        "context_datasource_types": [],
                    }
                ]
            },
            "revision_reason": "replacement_pattern",
            "semantic_fingerprint": f"{original.semantic_fingerprint}:replacement",
            "created_at": datetime.now(UTC),
        }
    )
    service._persist_revision_with_projection(
        replacement,
        candidate_id=second.id,
        decision_ref=replacement.decision_ref,
        expected_parent_revision=original.revision,
    )

    store = SignalStore(service.repository._db_path)
    active = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        tenant_id="default",
        include_decayed=True,
    )
    assert [row["metric_pattern"] for row in active if row["governance_ref"] == original.knowledge_id] == [
        "checkout_latency_new_seconds"
    ]
    with store._conn() as conn:
        old = conn.execute(
            """SELECT review_state FROM signal_metric_mappings
               WHERE tenant_id='default' AND governance_ref=? AND metric_pattern=?""",
            (original.knowledge_id, "checkout_latency_old_seconds"),
        ).fetchone()
    assert old is not None
    assert old["review_state"] == ReviewState.CANDIDATE.value


def test_projection_repair_uses_immutable_revision_payload_after_candidate_changes(tmp_path: Path):
    from tacit.signals.store import SignalStore

    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:mutable-source",
        typed_payload={"metric_pattern": "checkout_latency_original_seconds", "confidence": 0.9},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout-latency",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:mutable-source"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None

    reingested = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:mutable-source",
        typed_payload={"metric_pattern": "checkout_latency_mutated_seconds", "confidence": 0.7},
        proposition=candidate.proposition,
        scope=candidate.scope,
        provenance_refs=["dashboard:mutable-source"],
        candidate_id=candidate.id,
    )
    assert reingested.id == candidate.id
    store = SignalStore(service.repository._db_path)
    store.deactivate_governed_mappings(
        tenant_id="default",
        governance_ref=revision.knowledge_id,
    )

    service._repair_signal_mapping_projection(
        revision,
        expected_semantic_fingerprint=revision.semantic_fingerprint,
    )

    active = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        tenant_id="default",
        include_decayed=True,
    )
    assert [row["metric_pattern"] for row in active if row["governance_ref"] == revision.knowledge_id] == [
        "checkout_latency_original_seconds"
    ]


def test_candidate_reingestion_preserves_a_concurrent_terminal_review(tmp_path: Path, monkeypatch):
    service = _service(tmp_path)
    original = _dependency(
        service,
        payload_ref="runbook:concurrent-review",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:concurrent-review",
    )
    get_candidate = service.repository.get_candidate
    review_won = False

    def get_candidate_after_concurrent_review(candidate_id, tenant_id="default"):
        nonlocal review_won
        candidate = get_candidate(candidate_id, tenant_id)
        if not review_won:
            review_won = True
            service.review_candidate(
                candidate_id,
                approved=False,
                reviewer="concurrent-reviewer",
                tenant_id=tenant_id,
            )
        return candidate

    monkeypatch.setattr(
        service.repository,
        "get_candidate",
        get_candidate_after_concurrent_review,
    )
    reingested = _dependency(
        service,
        payload_ref="runbook:concurrent-review",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:concurrent-review",
    )

    assert review_won is True
    assert reingested.id == original.id
    assert reingested.state.review_state == ReviewState.REJECTED
    assert service.repository.get_candidate(original.id).state.review_state == ReviewState.REJECTED


def test_candidate_and_proposition_membership_roll_back_together(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)

    def fail_proposition_write(*args, **kwargs):
        raise RuntimeError("proposition write failed")

    monkeypatch.setattr(service.repository, "save_proposition", fail_proposition_write)

    with pytest.raises(RuntimeError, match="proposition write failed"):
        _dependency(
            service,
            payload_ref="runbook:atomic-candidate",
            family=SourceFamily.RUNBOOK,
            lineage_group="runbook:atomic-candidate",
        )

    assert service.repository.list_candidates(limit=None) == []
    assert service.repository.list_propositions() == []


def test_correction_candidate_is_never_visible_without_workflow_ownership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    save_correction = service.repository.save_correction
    observed_before_owner_commit = False
    reader = sqlite3.connect(service.repository._db_path)

    def inspect_before_correction_write(correction):
        nonlocal observed_before_owner_commit
        candidate_count = reader.execute(
            "SELECT COUNT(*) FROM knowledge_candidates WHERE id=?",
            (correction.knowledge_candidate_ref,),
        ).fetchone()[0]
        proposition_count = reader.execute(
            "SELECT COUNT(*) FROM proposition_candidates WHERE candidate_id=?",
            (correction.knowledge_candidate_ref,),
        ).fetchone()[0]
        correction_count = reader.execute(
            "SELECT COUNT(*) FROM knowledge_corrections WHERE correction_id=?",
            (correction.id,),
        ).fetchone()[0]
        assert (candidate_count, proposition_count, correction_count) == (0, 0, 0)
        observed_before_owner_commit = True
        return save_correction(correction)

    monkeypatch.setattr(service.repository, "save_correction", inspect_before_correction_write)
    try:
        correction, candidate = service.create_correction(
            investigation_id="inv-atomic-correction",
            investigation_revision=1,
            correction_type=CorrectionType.DEPENDENCY,
            proposed={
                "subject_ref": "entity:service:checkout",
                "predicate": "does_not_depend_on",
                "object_ref": "entity:datastore:redis-session",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            explanation="Own this correction candidate atomically.",
            created_by="operator",
        )
    finally:
        reader.close()

    assert observed_before_owner_commit is True
    assert service.repository.get_candidate(candidate.id) == candidate
    assert service.repository.get_correction(correction.id) == correction
    assert [
        item.id
        for item in service.repository.candidates_for_proposition(
            "default",
            candidate.proposition.proposition_key,
        )
    ] == [candidate.id]


def test_candidate_cas_normalizes_defaults_missing_from_legacy_json(tmp_path: Path):
    service = _service(tmp_path)
    original = _dependency(
        service,
        payload_ref="runbook:legacy-json",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:legacy-json",
    )

    def remove_newer_defaults() -> None:
        with service.repository._conn() as conn:
            row = conn.execute(
                "SELECT candidate_json FROM knowledge_candidates WHERE id=?",
                (original.id,),
            ).fetchone()
            payload = json.loads(row["candidate_json"])
            for field in ("confidence", "policy", "security_flags"):
                payload.pop(field, None)
            conn.execute(
                "UPDATE knowledge_candidates SET candidate_json=? WHERE id=?",
                (json.dumps(payload), original.id),
            )

    remove_newer_defaults()
    reingested = _dependency(
        service,
        payload_ref="runbook:legacy-json",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:legacy-json",
    )
    assert reingested.id == original.id

    remove_newer_defaults()
    reviewed = service.review_candidate(original.id, approved=True, reviewer="operator")
    assert reviewed.state.review_state == ReviewState.APPROVED

    remove_newer_defaults()
    service.evaluate_candidate(original.id)

    remove_newer_defaults()
    service.reconcile_source_lifecycle(
        provenance_ref="provenance:runbook:legacy-json",
        source_stale=True,
    )
    stored = service.repository.get_candidate(original.id)
    assert stored is not None
    assert stored.state.lifecycle_status == LifecycleStatus.STALE


def test_projection_repair_rejects_a_revision_that_advanced_before_the_lock(tmp_path: Path):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:repair-current",
        typed_payload={"metric_pattern": "checkout_latency_seconds"},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, original = service.evaluate_candidate(candidate.id, live_verified=True)
    assert original is not None
    advanced = original.model_copy(
        update={
            "revision": original.revision + 1,
            "parent_revision": original.revision,
            "revision_reason": "advanced",
            "semantic_fingerprint": f"{original.semantic_fingerprint}:advanced",
            "created_at": datetime.now(UTC),
        }
    )
    service._persist_revision_with_projection(
        advanced,
        candidate_id=candidate.id,
        decision_ref=advanced.decision_ref,
        expected_parent_revision=original.revision,
    )

    with pytest.raises(KnowledgeRevisionConflictError, match="advanced"):
        service._repair_signal_mapping_projection(
            original,
            expected_semantic_fingerprint=original.semantic_fingerprint,
        )

    from tacit.signals.store import SignalStore

    mapping = next(
        row
        for row in SignalStore(service.repository._db_path).get_mappings_for_signal(
            "request_latency",
            context_service="checkout",
            tenant_id="default",
            include_decayed=True,
        )
        if row["governance_ref"] == original.knowledge_id
    )
    assert mapping["governance_revision"] == advanced.revision


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


@pytest.mark.parametrize(
    "preexisting_lookup_column",
    [
        "",
        "applied_knowledge_ref TEXT NOT NULL DEFAULT '',",
        "applied_knowledge_revision INTEGER,",
    ],
)
def test_correction_lookup_columns_backfill_legacy_and_partial_schemas(
    tmp_path: Path,
    preexisting_lookup_column: str,
):
    db_path = tmp_path / "legacy-corrections.db"
    correction = KnowledgeCorrection(
        id="correction_legacy",
        investigation_id="inv_legacy",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        target_ref="knowledge_original",
        target_revision=1,
        proposed={"predicate": "does_not_depend_on"},
        scope=KnowledgeScope(),
        explanation="Legacy applied correction.",
        review_state=ReviewState.APPROVED,
        created_by="operator",
        knowledge_candidate_ref="candidate_legacy",
        applied_knowledge_ref="knowledge_replacement",
        applied_knowledge_revision=2,
    )
    with sqlite3.connect(db_path) as conn:
        conn.executescript(f"""CREATE TABLE knowledge_corrections (
                correction_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                investigation_id TEXT NOT NULL,
                investigation_revision INTEGER NOT NULL,
                correction_type TEXT NOT NULL,
                target_ref TEXT NOT NULL,
                {preexisting_lookup_column}
                review_state TEXT NOT NULL,
                candidate_ref TEXT NOT NULL,
                correction_json TEXT NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );""")
        conn.execute(
            """INSERT INTO knowledge_corrections (
               correction_id, tenant_id, investigation_id, investigation_revision,
               correction_type, target_ref, review_state, candidate_ref,
               correction_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                correction.id,
                correction.tenant_id,
                correction.investigation_id,
                correction.investigation_revision,
                correction.correction_type.value,
                correction.target_ref,
                correction.review_state.value,
                correction.knowledge_candidate_ref,
                correction.model_dump_json(),
                correction.created_at.timestamp(),
                correction.created_at.timestamp(),
            ),
        )

    repository = KnowledgeRepository(db_path)

    assert repository.list_corrections_for_knowledge("knowledge_original") == [correction]
    assert repository.list_corrections_for_knowledge("knowledge_replacement") == [correction]
    with repository._conn() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(knowledge_corrections)").fetchall()}
        indexes = {row["name"] for row in conn.execute("PRAGMA index_list(knowledge_corrections)").fetchall()}
    assert {"applied_knowledge_ref", "applied_knowledge_revision"}.issubset(columns)
    assert {"idx_knowledge_corrections_target", "idx_knowledge_corrections_applied"}.issubset(indexes)


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
    now = datetime.now(UTC).timestamp()
    with service.repository._conn() as conn:
        conn.execute(
            """INSERT INTO knowledge_corrections (
               correction_id, tenant_id, investigation_id, investigation_revision, correction_type,
               target_ref, applied_knowledge_ref, applied_knowledge_revision, review_state,
               candidate_ref, correction_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "correction_malformed",
                "default",
                "inv_malformed",
                1,
                CorrectionType.ARTIFACT_QUALITY.value,
                original.knowledge_id,
                "",
                None,
                ReviewState.APPROVED.value,
                "candidate_malformed",
                "{invalid-json",
                now,
                now,
            ),
        )
    assert [item["id"] for item in service.explain(original.knowledge_id)["corrections"]] == [correction.id]
    assert [item["id"] for item in service.explain(replacement.knowledge_id)["corrections"]] == [correction.id]


@pytest.mark.parametrize(
    "correction_type",
    [CorrectionType.SCOPE_CORRECTION, CorrectionType.TIME_WINDOW_CORRECTION],
)
def test_scope_and_time_window_corrections_supersede_the_pinned_target(
    tmp_path: Path,
    correction_type: CorrectionType,
):
    service = _service(tmp_path)
    _, target = _promoted_dependency(service)
    scope_update = (
        {"region_refs": ["region:us-east-1"]}
        if correction_type == CorrectionType.SCOPE_CORRECTION
        else {
            "valid_from": datetime.now(UTC) - timedelta(hours=1),
            "valid_until": datetime.now(UTC) + timedelta(days=30),
        }
    )
    corrected_scope = target.scope.model_copy(update=scope_update)
    correction, candidate = service.create_correction(
        investigation_id=f"inv-{correction_type.value}",
        investigation_revision=1,
        correction_type=correction_type,
        target_ref=target.knowledge_id,
        target_revision=target.revision,
        proposed={"kind": KnowledgeKind.DEPENDENCY.value},
        scope=corrected_scope,
        explanation="Narrow the reviewed knowledge boundary.",
        created_by="operator",
    )

    reviewed, replacement = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )

    assert candidate.kind == target.proposition.kind
    assert candidate.proposition.subject_ref == target.proposition.subject_ref
    assert candidate.proposition.predicate == target.proposition.predicate
    assert candidate.proposition.object_ref == target.proposition.object_ref
    assert replacement is not None
    assert replacement.knowledge_id != target.knowledge_id
    assert replacement.scope == corrected_scope
    assert replacement.state.lifecycle_status == LifecycleStatus.ACTIVE
    current_target = service.repository.get_revision(target.knowledge_id)
    assert current_target is not None
    assert current_target.revision == target.revision + 1
    assert current_target.state.lifecycle_status == LifecycleStatus.SUPERSEDED
    assert service.repository.get_revision(target.knowledge_id, target.revision) == target
    assert reviewed.applied_knowledge_ref == replacement.knowledge_id
    assert reviewed.applied_knowledge_revision == replacement.revision
    assert (
        service.supersede(
            target.knowledge_id,
            candidate.id,
            expected_revision=target.revision,
        )
        == current_target
    )


def test_scope_correction_preserves_signal_mapping_resolver_payload(tmp_path: Path):
    service = _service(tmp_path)
    scope = KnowledgeScope(
        environment_refs=["environment:production"],
        service_refs=["entity:service:checkout"],
    )
    candidates = []
    for index, (pattern, confidence, family) in enumerate(
        (
            ("AWS/ApplicationELB/TargetResponseTime", 0.93, SourceFamily.DASHBOARD),
            ("AWS/ApplicationELB/TargetResponseTime/p99", 0.88, SourceFamily.ALERT),
        )
    ):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=f"signal-scope-source-{index}",
            typed_payload={
                "metric_pattern": pattern,
                "confidence": confidence,
                "context_datasource_types": ["cloudwatch"],
            },
            proposition={
                "subject_ref": "concept:request-latency",
                "predicate": Predicate.REPRESENTED_BY,
                "object_ref": "concept:application-load-balancer-latency",
                "concept_ref": "signal:request_latency",
            },
            scope=scope,
            evidence=[
                KnowledgeEvidenceReference(
                    evidence_ref=f"signal-scope-evidence-{index}",
                    source_family=family,
                    lineage_group=f"signal-scope-lineage-{index}",
                    lineage_kind=LineageKind.INDEPENDENT,
                    provenance_refs=[f"signal-scope-provenance-{index}"],
                )
            ],
            provenance_refs=[f"signal-scope-provenance-{index}"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
        candidates.append(candidate)
    _, target = service.evaluate_candidate(candidates[0].id)
    assert target is not None
    assert len(target.resolver_payload["mappings"]) == 2
    corrected_scope = scope.model_copy(update={"region_refs": ["region:us-east-1"]})
    correction, _ = service.create_correction(
        investigation_id="inv-signal-scope-correction",
        investigation_revision=1,
        correction_type=CorrectionType.SCOPE_CORRECTION,
        target_ref=target.knowledge_id,
        target_revision=target.revision,
        proposed={"kind": KnowledgeKind.SIGNAL_MAPPING.value},
        scope=corrected_scope,
        explanation="Restrict the exact CloudWatch mappings to us-east-1.",
        created_by="operator",
    )

    _, replacement = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )

    assert replacement is not None
    assert replacement.resolver_payload == target.resolver_payload
    assert service.repository.get_revision(target.knowledge_id).state.lifecycle_status == LifecycleStatus.SUPERSEDED
    from tacit.signals.store import SignalStore

    store = SignalStore(service.repository._db_path)
    with store._conn() as conn:
        replacement_rows = conn.execute(
            """SELECT metric_pattern, confidence, context_datasource_types, context_regions, review_state
               FROM signal_metric_mappings
               WHERE tenant_id='default' AND governance_ref=?
               ORDER BY metric_pattern""",
            (replacement.knowledge_id,),
        ).fetchall()
        active_target_rows = conn.execute(
            """SELECT COUNT(*) FROM signal_metric_mappings
               WHERE tenant_id='default' AND governance_ref=? AND review_state IN ('approved', 'trusted')""",
            (target.knowledge_id,),
        ).fetchone()[0]
    assert [row["metric_pattern"] for row in replacement_rows] == [
        "AWS/ApplicationELB/TargetResponseTime",
        "AWS/ApplicationELB/TargetResponseTime/p99",
    ]
    assert [row["confidence"] for row in replacement_rows] == [0.93, 0.88]
    assert {row["context_datasource_types"] for row in replacement_rows} == {'["cloudwatch"]'}
    assert {row["context_regions"] for row in replacement_rows} == {'["us-east-1"]'}
    assert {row["review_state"] for row in replacement_rows} == {ReviewState.APPROVED.value}
    assert active_target_rows == 0


def test_signal_candidates_cannot_override_their_governed_resolver_payload(tmp_path: Path):
    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal-resolver-payload-injection",
        typed_payload={
            "metric_pattern": "reviewed_metric_seconds",
            "confidence": 0.9,
            "resolver_payload": {
                "mappings": [
                    {
                        "metric_pattern": "hidden_extra_metric_seconds",
                        "confidence": 1.0,
                        "context_datasource_types": ["prometheus"],
                    }
                ]
            },
        },
        proposition={
            "subject_ref": "concept:request-latency",
            "predicate": Predicate.REPRESENTED_BY,
            "object_ref": "concept:reviewed_metric_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["operator:reviewed-mapping"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="reviewer")

    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)

    assert revision is not None
    assert service._signal_metric_patterns(revision) == ["reviewed_metric_seconds"]
    from tacit.signals.store import SignalStore

    mappings = SignalStore(service.repository._db_path).get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
    )
    governed_patterns = {
        mapping["metric_pattern"] for mapping in mappings if mapping["governance_ref"] == revision.knowledge_id
    }
    assert governed_patterns == {"reviewed_metric_seconds"}


def test_scope_correction_rolls_back_replacement_and_target_when_application_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    _, target = _promoted_dependency(service)
    correction, candidate = service.create_correction(
        investigation_id="inv-scope-correction-rollback",
        investigation_revision=1,
        correction_type=CorrectionType.SCOPE_CORRECTION,
        target_ref=target.knowledge_id,
        target_revision=target.revision,
        proposed={"kind": KnowledgeKind.DEPENDENCY.value},
        scope=target.scope.model_copy(update={"region_refs": ["region:us-east-1"]}),
        explanation="Apply this narrowed scope atomically.",
        created_by="operator",
    )
    save_correction = service.repository.save_correction

    def fail_after_supersession(updated: KnowledgeCorrection):
        if updated.applied_knowledge_ref:
            raise RuntimeError("correction persistence failed")
        return save_correction(updated)

    monkeypatch.setattr(service.repository, "save_correction", fail_after_supersession)

    with pytest.raises(RuntimeError, match="correction persistence failed"):
        service.review_correction(
            correction.id,
            approved=True,
            reviewer="reviewer",
            authoritative=True,
        )

    current_target = service.repository.get_revision(target.knowledge_id)
    stored_correction = service.repository.get_correction(correction.id)
    stored_candidate = service.repository.get_candidate(candidate.id)
    assert current_target == target
    assert current_target.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert stored_correction is not None
    assert stored_correction.review_state == ReviewState.CANDIDATE
    assert stored_correction.applied_knowledge_ref == ""
    assert stored_candidate is not None
    assert stored_candidate.state.review_state == ReviewState.CANDIDATE
    assert {revision.knowledge_id for revision in service.repository.list_current_revisions()} == {target.knowledge_id}


def test_supersession_rejects_a_reviewed_but_unpromoted_replacement(tmp_path: Path):
    service = _service(tmp_path)
    _, target = _promoted_dependency(service)
    correction, candidate = service.create_correction(
        investigation_id="inv-unpromoted-supersession",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        target_ref=target.knowledge_id,
        target_revision=target.revision,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": Predicate.DOES_NOT_DEPEND_ON,
            "object_ref": "entity:datastore:redis-session",
        },
        scope=target.scope,
        explanation="This still requires promotion policy approval.",
        created_by="operator",
    )
    reviewed, replacement = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=False,
    )
    assert reviewed.review_state == ReviewState.APPROVED
    assert replacement is None

    with pytest.raises(ValueError, match="must have an active promoted revision"):
        service.supersede(
            target.knowledge_id,
            candidate.id,
            expected_revision=target.revision,
        )

    assert service.repository.get_revision(target.knowledge_id) == target
    persisted_correction = service.repository.get_correction(correction.id)
    assert persisted_correction is not None
    assert persisted_correction.applied_knowledge_ref == ""


def test_scope_correction_cannot_change_the_target_proposition(tmp_path: Path):
    service = _service(tmp_path)
    _, target = _promoted_dependency(service)

    with pytest.raises(ValueError, match="cannot change the target proposition"):
        service.create_correction(
            investigation_id="inv-invalid-scope-correction",
            investigation_revision=1,
            correction_type=CorrectionType.SCOPE_CORRECTION,
            target_ref=target.knowledge_id,
            target_revision=target.revision,
            proposed={
                "kind": KnowledgeKind.DEPENDENCY.value,
                "subject_ref": target.proposition.subject_ref,
                "predicate": Predicate.DOES_NOT_DEPEND_ON,
                "object_ref": target.proposition.object_ref,
            },
            scope=target.scope.model_copy(update={"region_refs": ["region:us-east-1"]}),
            explanation="This attempts to change two authority dimensions at once.",
            created_by="operator",
        )

    with service.repository._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM knowledge_corrections").fetchone()[0] == 0


def test_correction_candidate_cannot_use_the_generic_review_or_evaluation_workflow(tmp_path: Path):
    service = _service(tmp_path)
    correction, candidate = service.create_correction(
        investigation_id="inv_correction_boundary",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        explanation="Review this through the correction workflow.",
        created_by="operator",
    )

    with pytest.raises(PermissionError, match="correction workflow"):
        service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
    with pytest.raises(PermissionError, match="correction workflow"):
        service.evaluate_candidate(candidate.id, authoritative_source=True)

    persisted = service.repository.get_candidate(candidate.id)
    assert persisted is not None
    assert persisted.state.review_state == ReviewState.CANDIDATE
    reviewed, revision = service.review_correction(
        correction.id,
        approved=True,
        reviewer="reviewer",
        authoritative=True,
    )
    assert reviewed.review_state == ReviewState.APPROVED
    assert revision is not None


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

    with pytest.raises(KnowledgeRevisionConflictError, match="advanced from revision 1 to 2"):
        service.review_correction(
            correction.id,
            approved=True,
            reviewer="operator",
            authoritative=True,
        )

    stored_candidate = service.repository.get_candidate(correction_candidate.id)
    assert stored_candidate is not None
    assert stored_candidate.state.review_state == ReviewState.CANDIDATE


def test_targeted_correction_retry_returns_its_committed_application(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, _ = service.create_correction(
        investigation_id="inv-idempotent-correction",
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
        explanation="Withdraw the reviewed revision.",
        created_by="operator",
    )

    applied_correction, applied = service.review_correction(
        correction.id,
        approved=True,
        reviewer="operator",
    )
    retried_correction, retried = service.review_correction(
        correction.id,
        approved=True,
        reviewer="operator",
    )

    assert applied is not None
    assert retried == applied
    assert retried_correction == applied_correction
    assert applied_correction.applied_knowledge_ref == applied.knowledge_id
    assert applied_correction.applied_knowledge_revision == applied.revision
    assert len(service.repository.list_revisions(original.knowledge_id)) == 2


def test_applied_knowledge_correction_cannot_later_be_rejected(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, candidate = service.create_correction(
        investigation_id="inv-terminal-correction",
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
        explanation="Withdraw the reviewed revision.",
        created_by="operator",
    )
    applied_correction, applied = service.review_correction(
        correction.id,
        approved=True,
        reviewer="operator",
    )
    assert applied is not None

    with pytest.raises(KnowledgeRevisionConflictError, match="terminal and cannot be rejected"):
        service.review_correction(
            correction.id,
            approved=False,
            reviewer="second-operator",
        )

    stored_correction = service.repository.get_correction(correction.id)
    stored_candidate = service.repository.get_candidate(candidate.id)
    assert stored_correction == applied_correction
    assert stored_correction.review_state == ReviewState.APPROVED
    assert stored_candidate is not None
    assert stored_candidate.state.review_state == ReviewState.APPROVED
    assert service.repository.get_revision(original.knowledge_id).state.lifecycle_status == LifecycleStatus.WITHDRAWN


def test_correction_supersession_rechecks_target_under_write_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, correction_candidate = service.create_correction(
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
    assert current.revision == 1
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE
    stored_correction = service.repository.get_correction(correction.id)
    assert stored_correction is not None
    assert stored_correction.review_state == ReviewState.CANDIDATE
    stored_candidate = service.repository.get_candidate(correction_candidate.id)
    assert stored_candidate is not None
    assert stored_candidate.state.review_state == ReviewState.CANDIDATE


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


def test_governed_signal_projection_preserves_complete_scope_and_validity(tmp_path: Path):
    service = _service(tmp_path)
    now = datetime.now(UTC)
    scope = KnowledgeScope(
        environment_refs=["environment:production"],
        region_refs=["region:us-east-1"],
        cluster_refs=["cluster:prod-a"],
        namespace_refs=["namespace:checkout"],
        service_refs=["entity:service:checkout"],
        archetype_refs=["archetype:resource-saturation"],
        version_constraints=["version:>=2"],
        valid_from=now - timedelta(minutes=5),
        valid_until=now + timedelta(minutes=5),
    )
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:fully-scoped-latency",
        typed_payload={
            "metric_pattern": "fully_scoped_latency_seconds",
            "context_datasource_types": ["prometheus"],
        },
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:fully_scoped_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=scope,
        provenance_refs=["operator:fully-scoped"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None

    from tacit.signals.store import SignalStore

    store = SignalStore(service.repository._db_path)
    matching_scope = scope.model_copy(update={"version_constraints": ["version:v2.4.1"]})
    matches = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        context_datasource_type="prometheus",
        context_archetype="resource-saturation",
        context_environment="production",
        knowledge_scope=matching_scope,
    )
    wrong_region = matching_scope.model_copy(update={"region_refs": ["region:us-west-2"]})
    misses = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        context_datasource_type="prometheus",
        context_archetype="resource-saturation",
        context_environment="production",
        knowledge_scope=wrong_region,
    )
    wrong_version = matching_scope.model_copy(update={"version_constraints": ["version:v1.9.9"]})
    version_misses = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        context_datasource_type="prometheus",
        context_archetype="resource-saturation",
        context_environment="production",
        knowledge_scope=wrong_version,
    )

    assert [mapping["governance_ref"] for mapping in matches] == [revision.knowledge_id]
    assert misses == []
    assert version_misses == []
    projection = matches[0]
    assert projection["context_regions"] == ["us-east-1"]
    assert projection["context_clusters"] == ["prod-a"]
    assert projection["context_namespaces"] == ["checkout"]
    assert projection["context_versions"] == [">=2"]
    assert projection["valid_from"] == pytest.approx(scope.valid_from.timestamp())
    assert projection["valid_until"] == pytest.approx(scope.valid_until.timestamp())


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


def test_rejecting_applied_entity_mapping_correction_retires_alias(tmp_path: Path):
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
    correction, _candidate = service.create_correction(
        investigation_id="inv_entity_mapping_reversal",
        investigation_revision=1,
        correction_type=CorrectionType.ENTITY_MAPPING,
        proposed={"raw_value": "Checkout API", "entity_ref": "entity:service:checkout"},
        scope=KnowledgeScope(),
        explanation="Bind the observed service alias to the catalog entity.",
        created_by="operator",
    )
    approved, _revision = service.review_correction(
        correction.id,
        approved=True,
        reviewer="approver",
    )

    rejected, _revision = service.review_correction(
        correction.id,
        approved=False,
        reviewer="rejector",
    )
    retirement_events = [
        event for event in service.repository.list_events("default") if event["event_type"] == "entity_alias_retired"
    ]
    retried, _revision = service.review_correction(
        correction.id,
        approved=False,
        reviewer="rejector",
    )

    alias = service.repository.get_alias(approved.applied_alias_ref, "default")
    assert approved.applied_alias_ref
    assert rejected.review_state == ReviewState.REJECTED
    assert retried == rejected
    assert rejected.applied_alias_ref == approved.applied_alias_ref
    assert alias is not None
    assert alias.review_state == ReviewState.REJECTED
    assert alias.lifecycle_status == LifecycleStatus.WITHDRAWN
    assert service.repository.find_aliases("default", "checkout-api") == []
    assert [
        event for event in service.repository.list_events("default") if event["event_type"] == "entity_alias_retired"
    ] == retirement_events


def test_knowledge_service_enforces_its_runtime_tenant_boundary(tmp_path: Path):
    pinned = KnowledgeService(
        KnowledgeRepository(tmp_path / "pinned.db"),
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with pytest.raises(TenantBoundaryError, match="Tenant access denied"):
        pinned.create_candidate(
            kind=KnowledgeKind.ARTIFACT_QUALITY,
            payload_ref="cross-tenant",
            typed_payload={},
            proposition={"subject_ref": "concept:checkout", "predicate": "useful_for_investigation"},
            scope=KnowledgeScope(tenant_id="tenant-b"),
            provenance_refs=["test:cross-tenant"],
            tenant_id="tenant-b",
        )

    wildcard = KnowledgeService(
        KnowledgeRepository(tmp_path / "wildcard.db"),
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
    with pytest.raises(TenantBoundaryError, match="Knowledge tenant is required"):
        wildcard.create_candidate(
            kind=KnowledgeKind.ARTIFACT_QUALITY,
            payload_ref="missing-tenant",
            typed_payload={},
            proposition={"subject_ref": "concept:checkout", "predicate": "useful_for_investigation"},
            scope=KnowledgeScope(),
            provenance_refs=["test:missing-tenant"],
        )
    with pytest.raises(TenantBoundaryError, match="Knowledge tenant is required"):
        wildcard.register_entity(
            Entity(
                id="entity:service:checkout",
                kind=EntityKind.SERVICE,
                canonical_name="checkout",
                provenance_refs=["catalog:checkout"],
            )
        )

    registered = wildcard.register_entity(
        Entity(
            id="entity:service:checkout",
            tenant_id="tenant-a",
            kind=EntityKind.SERVICE,
            canonical_name="checkout",
            scope=KnowledgeScope(tenant_id="tenant-a"),
            provenance_refs=["catalog:checkout"],
        )
    )
    assert registered.tenant_id == "tenant-a"

    with pytest.raises(ValueError, match="usage batch cannot cross tenants"):
        wildcard.persist_usage(
            [
                KnowledgeUsage(
                    tenant_id=tenant_id,
                    knowledge_ref=f"knowledge-{tenant_id}",
                    knowledge_revision=1,
                    disposition=KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
                )
                for tenant_id in ("tenant-a", "tenant-b")
            ],
            investigation_id="mixed-tenant-investigation",
            investigation_revision=1,
        )


def test_entity_registration_enforces_runtime_review_permission(tmp_path: Path):
    service = _service(tmp_path)
    original = service.repository.get_entity("entity:service:checkout")
    assert original is not None
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.review"):
        service.register_entity(
            Entity(
                id="entity:service:payments",
                kind=EntityKind.SERVICE,
                canonical_name="payments",
                provenance_refs=["catalog:payments"],
            )
        )
    with pytest.raises(PermissionError, match="Missing permission: knowledge.review"):
        service.register_entity(original.model_copy(update={"canonical_name": "storefront"}))

    assert service.repository.get_entity("entity:service:payments") is None
    assert service.repository.get_entity(original.id) == original


def test_correction_creation_enforces_runtime_correct_permission(tmp_path: Path):
    service = _service(tmp_path)
    candidate_ids = {candidate.id for candidate in service.repository.list_candidates()}
    event_count = len(service.repository.list_events())
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.correct"):
        service.create_correction(
            investigation_id="inv-unauthorized-correction",
            investigation_revision=1,
            correction_type=CorrectionType.DEPENDENCY,
            proposed={
                "subject_ref": "entity:service:checkout",
                "predicate": "depends_on",
                "object_ref": "entity:datastore:redis-session",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            explanation="This must not reach persistence.",
            created_by="embedding",
        )

    assert {candidate.id for candidate in service.repository.list_candidates()} == candidate_ids
    assert len(service.repository.list_events()) == event_count
    with service.repository._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM knowledge_corrections").fetchone()[0] == 0


def test_mutation_permissions_are_checked_before_wildcard_tenant_or_resource_lookups(tmp_path: Path):
    service = KnowledgeService(
        KnowledgeRepository(tmp_path / "permission-order.db"),
        runtime_settings=Settings(
            _env_file=None,
            knowledge_tenant_id="*",
            knowledge_permissions="knowledge.read",
        ),
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.review"):
        service.register_entity(
            Entity(
                id="entity:service:checkout",
                kind=EntityKind.SERVICE,
                canonical_name="checkout",
                provenance_refs=["catalog:checkout"],
            )
        )
    with pytest.raises(PermissionError, match="Missing permission: knowledge.correct"):
        service.create_correction(
            investigation_id="inv-permission-order",
            investigation_revision=1,
            correction_type=CorrectionType.DEPENDENCY,
            proposed={
                "subject_ref": "entity:service:checkout",
                "predicate": "depends_on",
                "object_ref": "entity:datastore:redis-session",
            },
            scope=KnowledgeScope(),
            explanation="Authorization must precede tenant resolution.",
            created_by="embedding",
        )
    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        service.supersede("knowledge-missing", "candidate-missing")

    with service.repository._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM knowledge_corrections").fetchone()[0] == 0


def test_supersession_requires_apply_permission_and_a_reviewed_correction(tmp_path: Path):
    service = _service(tmp_path)
    _, original = _promoted_dependency(service)
    correction, candidate = service.create_correction(
        investigation_id="inv-supersede-boundary",
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
        explanation="Replace the dependency through review.",
        created_by="embedding",
    )
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review,knowledge.correct",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        service.supersede(
            original.knowledge_id,
            candidate.id,
            expected_revision=original.revision,
        )

    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review,knowledge.correct,knowledge.apply",
    )
    with pytest.raises(ValueError, match="replacement candidate must be approved or trusted"):
        service.supersede(
            original.knowledge_id,
            candidate.id,
            expected_revision=original.revision,
        )

    service.review_candidate(
        candidate.id,
        approved=True,
        reviewer="embedding",
        _correction_id=correction.id,
    )
    with pytest.raises(ValueError, match="requires an approved correction"):
        service.supersede(
            original.knowledge_id,
            candidate.id,
            expected_revision=original.revision,
        )

    persisted = service.repository.get_revision(original.knowledge_id)
    assert persisted is not None
    assert persisted.revision == original.revision
    assert persisted.state.lifecycle_status == LifecycleStatus.ACTIVE


@pytest.mark.parametrize(
    ("approved", "trust", "can_trust", "permissions", "missing_permission"),
    [
        (True, False, False, "knowledge.read,knowledge.reject", "knowledge.review"),
        (False, False, False, "knowledge.read,knowledge.review", "knowledge.reject"),
        (True, True, True, "knowledge.read,knowledge.review", "knowledge.trust"),
    ],
)
def test_candidate_review_transition_enforces_runtime_permissions(
    tmp_path: Path,
    approved: bool,
    trust: bool,
    can_trust: bool,
    permissions: str,
    missing_permission: str,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref=f"permission:{missing_permission}",
        family=SourceFamily.RUNBOOK,
        lineage_group=f"permission:{missing_permission}",
    )
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions=permissions,
    )

    with pytest.raises(PermissionError, match=f"Missing permission: {missing_permission}"):
        service.review_candidate(
            candidate.id,
            approved=approved,
            reviewer="embedding",
            trust=trust,
            can_trust=can_trust,
        )

    persisted = service.repository.get_candidate(candidate.id)
    assert persisted is not None
    assert persisted.state.review_state == ReviewState.CANDIDATE


def test_unauthorized_direct_rejection_cannot_withdraw_active_knowledge(tmp_path: Path):
    service = _service(tmp_path)
    candidate, revision = _promoted_dependency(service)
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review,knowledge.trust",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.reject"):
        service.review_candidate(
            candidate.id,
            approved=False,
            reviewer="embedding",
        )

    persisted_candidate = service.repository.get_candidate(candidate.id)
    persisted_revision = service.repository.get_revision(revision.knowledge_id)
    assert persisted_candidate is not None
    assert persisted_candidate.state.review_state == ReviewState.APPROVED
    assert persisted_revision is not None
    assert persisted_revision.state.lifecycle_status == LifecycleStatus.ACTIVE


@pytest.mark.parametrize("override_field", ["authoritative_source", "live_verified"])
def test_candidate_evaluation_enforces_override_permission(
    tmp_path: Path,
    override_field: str,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref=f"evaluation-override:{override_field}",
        family=SourceFamily.RUNBOOK,
        lineage_group=f"evaluation-override:{override_field}",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="embedding")
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.override"):
        service.evaluate_candidate_result(candidate.id, **{override_field: True})

    persisted = service.repository.get_candidate(candidate.id)
    assert persisted is not None
    assert persisted.policy.last_evaluated_at is None
    assert service.repository.list_current_revisions() == []


@pytest.mark.parametrize(
    ("review_state", "permissions", "missing_permission"),
    [
        (ReviewState.APPROVED, "knowledge.read", "knowledge.review"),
        (ReviewState.TRUSTED, "knowledge.read,knowledge.review", "knowledge.trust"),
        (ReviewState.TRUSTED, "knowledge.read,knowledge.trust", "knowledge.review"),
    ],
)
def test_alias_registration_enforces_runtime_permissions(
    tmp_path: Path,
    review_state: ReviewState,
    permissions: str,
    missing_permission: str,
):
    service = _service(tmp_path)
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions=permissions,
    )
    alias = EntityAlias(
        id=f"alias-permission-{missing_permission.replace('.', '-')}",
        raw_value="Storefront",
        normalized_value="storefront",
        entity_ref="entity:service:checkout",
        method=EntityBindingMethod.HUMAN_CORRECTION,
        review_state=review_state,
        provenance_refs=["operator:alias"],
    )

    with pytest.raises(PermissionError, match=f"Missing permission: {missing_permission}"):
        service.register_alias(alias)

    assert service.repository.get_alias(alias.id) is None


def test_alias_registration_rejects_terminal_state_mutations(tmp_path: Path):
    service = _service(tmp_path)
    approved = service.register_alias(
        EntityAlias(
            id="alias-terminal-state",
            raw_value="Storefront",
            normalized_value="storefront",
            entity_ref="entity:service:checkout",
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=["operator:alias"],
        )
    )

    with pytest.raises(ValueError, match="only approved or trusted"):
        service.register_alias(approved.model_copy(update={"review_state": ReviewState.REJECTED}))
    with pytest.raises(ValueError, match="lifecycle transitions must use"):
        service.register_alias(approved.model_copy(update={"lifecycle_status": LifecycleStatus.WITHDRAWN}))

    assert service.repository.get_alias(approved.id) == approved


def test_alias_registration_prevents_trust_downgrades(tmp_path: Path):
    service = _service(tmp_path)
    trusted = service.register_alias(
        EntityAlias(
            id="alias-trusted-terminal",
            raw_value="Storefront",
            normalized_value="storefront",
            entity_ref="entity:service:checkout",
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.TRUSTED,
            provenance_refs=["operator:alias"],
        )
    )

    with pytest.raises(ValueError, match="cannot be downgraded"):
        service.register_alias(trusted.model_copy(update={"review_state": ReviewState.APPROVED}))

    assert service.repository.get_alias(trusted.id) == trusted


def test_alias_registration_detects_concurrent_updates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path)
    alias = service.register_alias(
        EntityAlias(
            id="alias-concurrent-update",
            raw_value="Storefront",
            normalized_value="storefront",
            entity_ref="entity:service:checkout",
            method=EntityBindingMethod.HUMAN_CORRECTION,
            review_state=ReviewState.APPROVED,
            provenance_refs=["operator:initial"],
        )
    )
    competing_service = KnowledgeService(
        KnowledgeRepository(service.repository._db_path),
        runtime_settings=Settings(_env_file=None),
    )
    observed = threading.Event()
    release = threading.Event()
    original_get_alias = service.repository.get_alias
    first_read = True

    def pause_after_observation(alias_id: str, tenant_id: str = "default"):
        nonlocal first_read
        current = original_get_alias(alias_id, tenant_id)
        if first_read:
            first_read = False
            observed.set()
            assert release.wait(timeout=5)
        return current

    monkeypatch.setattr(service.repository, "get_alias", pause_after_observation)
    attempted = alias.model_copy(update={"raw_value": "Storefront API", "provenance_refs": ["operator:attempted"]})
    competing = alias.model_copy(update={"raw_value": "Storefront Service", "provenance_refs": ["operator:competing"]})

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(service.register_alias, attempted)
        assert observed.wait(timeout=5)
        competing_service.register_alias(competing)
        release.set()
        with pytest.raises(AliasRegistrationConflictError, match="changed during registration"):
            future.result(timeout=5)

    persisted = competing_service.repository.get_alias(alias.id)
    assert persisted is not None
    assert persisted.raw_value == "Storefront Service"


@pytest.mark.parametrize(
    ("permissions", "authoritative", "missing_permission"),
    [
        ("knowledge.read,knowledge.review", False, "knowledge.apply"),
        (
            "knowledge.read,knowledge.review,knowledge.apply",
            True,
            "knowledge.override",
        ),
    ],
)
def test_direct_correction_approval_enforces_apply_and_override_permissions(
    tmp_path: Path,
    permissions: str,
    authoritative: bool,
    missing_permission: str,
):
    service = _service(tmp_path)
    correction, candidate = service.create_correction(
        investigation_id="inv-direct-correction-permission",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        explanation="Review this correction.",
        created_by="embedding",
    )
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions=permissions,
    )

    with pytest.raises(PermissionError, match=f"Missing permission: {missing_permission}"):
        service.review_correction(
            correction.id,
            approved=True,
            reviewer="embedding",
            authoritative=authoritative,
        )

    persisted_candidate = service.repository.get_candidate(candidate.id)
    persisted_correction = service.repository.get_correction(correction.id)
    assert persisted_candidate is not None
    assert persisted_candidate.state.review_state == ReviewState.CANDIDATE
    assert persisted_correction is not None
    assert persisted_correction.review_state == ReviewState.CANDIDATE
    assert service.repository.list_current_revisions() == []


def test_imported_review_state_enforces_runtime_permissions(tmp_path: Path):
    service = _service(tmp_path)
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read",
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.review"):
        migrate_signal_mapping(
            {
                "id": "unauthorized-imported-trust",
                "signal_type": "request_latency",
                "metric_pattern": "unauthorized_latency_seconds",
                "source_type": "dashboard_ingest",
                "source_refs": ["dashboard:unauthorized-import"],
                "review_state": "trusted",
            },
            service=service,
        )

    candidates = service.repository.list_candidates()
    assert len(candidates) == 1
    assert candidates[0].state.review_state == ReviewState.CANDIDATE
    assert service.repository.list_current_revisions() == []


def test_knowledge_usage_batch_rolls_back_usage_and_events_together(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    _, dependency_revision = _promoted_dependency(service)
    mapping_candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="signal:usage-batch",
        typed_payload={"metric_pattern": "checkout_latency_seconds"},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["operator:usage-batch"],
    )
    service.review_candidate(mapping_candidate.id, approved=True, reviewer="operator")
    _, mapping_revision = service.evaluate_candidate(mapping_candidate.id, live_verified=True)
    assert mapping_revision is not None
    usage = [
        KnowledgeUsage(
            knowledge_ref=dependency_revision.knowledge_id,
            knowledge_revision=dependency_revision.revision,
            disposition=KnowledgeUsageDisposition.APPLIED,
            used_for=["ranking"],
        ),
        KnowledgeUsage(
            knowledge_ref=mapping_revision.knowledge_id,
            knowledge_revision=mapping_revision.revision,
            disposition=KnowledgeUsageDisposition.APPLIED,
            used_for=["query_compilation"],
        ),
    ]
    save_usage = service.repository.save_usage
    calls = 0

    def fail_second_save(item: KnowledgeUsage):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("usage write interrupted")
        return save_usage(item)

    monkeypatch.setattr(service.repository, "save_usage", fail_second_save)
    with pytest.raises(RuntimeError, match="usage write interrupted"):
        service.persist_usage(
            usage,
            investigation_id="inv-usage-batch",
            investigation_revision=3,
        )

    assert service.repository.list_usage(investigation_id="inv-usage-batch") == []
    assert [
        event
        for event in service.repository.list_events()
        if event["payload"].get("investigation_id") == "inv-usage-batch"
    ] == []

    monkeypatch.setattr(service.repository, "save_usage", save_usage)
    persisted = service.persist_usage(
        usage,
        investigation_id="inv-usage-batch",
        investigation_revision=3,
    )
    events = [
        event
        for event in service.repository.list_events()
        if event["payload"].get("investigation_id") == "inv-usage-batch"
    ]
    assert len(persisted) == 2
    assert len(service.repository.list_usage(investigation_id="inv-usage-batch")) == 2
    assert len(events) == 2


def test_migrated_signal_mapping_preserves_complete_scope_and_validity(tmp_path: Path):
    service = _service(tmp_path)
    now = datetime.now(UTC)
    valid_from = now - timedelta(minutes=5)
    valid_until = now + timedelta(minutes=5)
    candidate_id = migrate_signal_mapping(
        {
            "id": "fully-scoped-migrated-latency",
            "signal_type": "request_latency",
            "metric_pattern": "migrated_latency_seconds",
            "confidence": 0.91,
            "source_type": "dashboard_ingest",
            "source_refs": ["dashboard:fully-scoped"],
            "context_services": ["checkout"],
            "context_datasource_types": ["prometheus"],
            "context_environments": ["production"],
            "context_regions": ["us-east-1"],
            "context_clusters": ["prod-a"],
            "context_namespaces": ["checkout"],
            "context_archetypes": ["resource-saturation"],
            "context_versions": [">=2"],
            "valid_from": valid_from.timestamp(),
            "valid_until": valid_until.isoformat(),
        },
        service=service,
    )
    candidate = service.repository.get_candidate(candidate_id)
    assert candidate is not None
    assert candidate.scope.service_refs == ["entity:service:checkout"]
    assert candidate.scope.environment_refs == ["environment:production"]
    assert candidate.scope.region_refs == ["region:us-east-1"]
    assert candidate.scope.cluster_refs == ["cluster:prod-a"]
    assert candidate.scope.namespace_refs == ["namespace:checkout"]
    assert candidate.scope.archetype_refs == ["archetype:resource-saturation"]
    assert candidate.scope.version_constraints == ["version:>=2"]
    assert candidate.scope.valid_from == valid_from
    assert candidate.scope.valid_until == valid_until

    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None

    from tacit.signals.store import SignalStore

    store = SignalStore(service.repository._db_path)
    matching_scope = candidate.scope.model_copy(
        update={"version_constraints": ["version:v2.4.1"]},
    )
    matches = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        context_datasource_type="prometheus",
        context_archetype="resource-saturation",
        context_environment="production",
        knowledge_scope=matching_scope,
    )
    wrong_region = matching_scope.model_copy(update={"region_refs": ["region:us-west-2"]})
    misses = store.get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        context_datasource_type="prometheus",
        context_archetype="resource-saturation",
        context_environment="production",
        knowledge_scope=wrong_region,
    )

    assert [mapping["governance_ref"] for mapping in matches] == [revision.knowledge_id]
    assert misses == []
    assert matches[0]["context_regions"] == ["us-east-1"]
    assert matches[0]["context_clusters"] == ["prod-a"]
    assert matches[0]["context_namespaces"] == ["checkout"]
    assert matches[0]["context_versions"] == [">=2"]
    assert matches[0]["valid_from"] == pytest.approx(valid_from.timestamp())
    assert matches[0]["valid_until"] == pytest.approx(valid_until.timestamp())


def test_migrated_signal_mapping_validity_changes_have_distinct_candidate_identity(
    tmp_path: Path,
):
    service = _service(tmp_path)
    now = datetime.now(UTC)
    row = {
        "id": "mapping-validity-identity",
        "signal_type": "request_latency",
        "metric_pattern": "validity_scoped_latency_seconds",
        "source_type": "dashboard_ingest",
        "source_refs": ["dashboard:validity-identity"],
        "valid_from": now.timestamp(),
        "valid_until": (now + timedelta(hours=1)).timestamp(),
    }

    first_id = migrate_signal_mapping(row, service=service)
    second_id = migrate_signal_mapping(
        {
            **row,
            "valid_until": (now + timedelta(hours=2)).timestamp(),
        },
        service=service,
    )

    assert second_id != first_id
    first = service.repository.get_candidate(first_id)
    second = service.repository.get_candidate(second_id)
    assert first is not None
    assert second is not None
    assert first.scope.valid_until != second.scope.valid_until


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
    [
        CorrectionType.KNOWLEDGE_STALE,
        CorrectionType.KNOWLEDGE_INCORRECT,
        CorrectionType.SCOPE_CORRECTION,
        CorrectionType.TIME_WINDOW_CORRECTION,
    ],
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


def test_source_retirement_rolls_back_candidate_revision_and_projection_together(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from tacit.signals.store import SignalStore

    service = _service(tmp_path)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:atomic-retirement",
        typed_payload={"metric_pattern": "checkout_latency_seconds"},
        proposition={
            "subject_ref": "concept:request_latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["provenance:atomic-retirement"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, active = service.evaluate_candidate(candidate.id, live_verified=True)
    assert active is not None
    original_sync = service._sync_signal_mapping_state

    def fail_retirement(revision, **kwargs):
        if revision.state.lifecycle_status == LifecycleStatus.STALE:
            raise OSError("projection retirement unavailable")
        return original_sync(revision, **kwargs)

    monkeypatch.setattr(service, "_sync_signal_mapping_state", fail_retirement)

    with pytest.raises(OSError, match="projection retirement unavailable"):
        service.reconcile_source_lifecycle(
            provenance_ref="provenance:atomic-retirement",
            active_candidate_ids=set(),
        )

    persisted_candidate = service.repository.get_candidate(candidate.id)
    persisted_revision = service.repository.get_revision(active.knowledge_id)
    assert persisted_candidate is not None
    assert persisted_candidate.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert persisted_revision == active
    mappings = SignalStore(service.repository._db_path).get_mappings_for_signal(
        "request_latency",
        context_service="checkout",
        tenant_id="default",
        include_decayed=True,
    )
    assert any(row["governance_ref"] == active.knowledge_id for row in mappings)

    monkeypatch.setattr(service, "_sync_signal_mapping_state", original_sync)
    retired = service.reconcile_source_lifecycle(
        provenance_ref="provenance:atomic-retirement",
        active_candidate_ids=set(),
    )

    assert retired[-1].state.lifecycle_status == LifecycleStatus.STALE
    assert service.repository.get_candidate(candidate.id).state.lifecycle_status == LifecycleStatus.STALE
    assert not any(
        row["governance_ref"] == active.knowledge_id
        for row in SignalStore(service.repository._db_path).get_mappings_for_signal(
            "request_latency",
            context_service="checkout",
            tenant_id="default",
            include_decayed=True,
        )
    )


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
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review",
    )

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
    for source, family, confidence in (
        ("dashboard-a", SourceFamily.DASHBOARD, 0.95),
        ("alert-b", SourceFamily.ALERT, 0.55),
    ):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=source,
            typed_payload={"metric": "http_request_duration_seconds", "confidence": confidence},
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
    service._runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read,knowledge.review",
    )

    service.reconcile_source_lifecycle(
        provenance_ref="provenance:dashboard-a",
        active_candidate_ids=set(),
    )

    current = service.repository.get_revision(latest.knowledge_id)
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE
    assert current.state.eligibility == KnowledgeEligibility.LIVE_VERIFIED
    assert current.promoted_from_candidate_refs == [candidates[1].id]
    from tacit.signals import SignalStore

    mappings = SignalStore(service.repository._db_path).get_mappings_for_signal(
        "latency",
        context_service="checkout",
        include_decayed=True,
    )
    projection = next(item for item in mappings if item["governance_ref"] == current.knowledge_id)
    assert projection["confidence"] == pytest.approx(0.55)
    assert projection["governance_revision"] == current.revision


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


def test_imported_approval_does_not_reactivate_withdrawn_knowledge(tmp_path: Path, monkeypatch):
    from tacit.knowledge.migration import _evaluate_imported_approval

    service = _service(tmp_path)
    rejected, active = _promoted_dependency(service)
    survivor = next(
        candidate
        for candidate in service.repository.candidates_for_proposition(
            "default",
            rejected.proposition.proposition_key,
        )
        if candidate.id != rejected.id
    )
    service.review_candidate(rejected.id, approved=False, reviewer="operator")
    current = service.repository.get_revision(active.knowledge_id)
    assert current is not None
    assert current.state.lifecycle_status == LifecycleStatus.WITHDRAWN

    monkeypatch.setattr(
        service,
        "evaluate_candidate",
        lambda *args, **kwargs: pytest.fail("withdrawn knowledge must require explicit reactivation"),
    )

    _evaluate_imported_approval(service, survivor)


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
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: service.evaluate_candidate(first.id), range(2)))

    revisions = [revision for _, revision in results]
    assert all(revision is not None for revision in revisions)
    assert len({(revision.knowledge_id, revision.revision) for revision in revisions if revision}) == 1
    assert len(service.repository.list_revisions(revisions[0].knowledge_id)) == 1


@pytest.mark.parametrize("concurrent_transition", ["reject", "stale"])
def test_terminal_transition_committed_first_prevents_later_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    concurrent_transition: str,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref=f"evaluation-race-{concurrent_transition}",
        family=SourceFamily.HUMAN_CORRECTION,
        lineage_group=f"evaluation-race-{concurrent_transition}",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
    transition_holds_lock = threading.Event()
    allow_transition_commit = threading.Event()
    evaluation_entered_transaction = threading.Event()
    evaluate_in_transaction = service._evaluate_candidate_in_transaction

    def tracked_evaluation(*args, **kwargs):
        evaluation_entered_transaction.set()
        return evaluate_in_transaction(*args, **kwargs)

    monkeypatch.setattr(service, "_evaluate_candidate_in_transaction", tracked_evaluation)
    if concurrent_transition == "reject":
        transition = service.repository.transition_candidate_review

        def delayed_transition(updated, *, expected):
            result = transition(updated, expected=expected)
            transition_holds_lock.set()
            assert allow_transition_commit.wait(timeout=5)
            return result

        monkeypatch.setattr(service.repository, "transition_candidate_review", delayed_transition)

        def run_transition():
            return service.review_candidate(candidate.id, approved=False, reviewer="rejector")

    else:
        transition = service.repository.transition_candidate_lifecycle

        def delayed_transition(updated, *, expected):
            result = transition(updated, expected=expected)
            transition_holds_lock.set()
            assert allow_transition_commit.wait(timeout=5)
            return result

        monkeypatch.setattr(service.repository, "transition_candidate_lifecycle", delayed_transition)

        def run_transition():
            return service.reconcile_source_lifecycle(
                provenance_ref=candidate.provenance_refs[0],
                active_candidate_ids=set(),
            )

    evaluation_started = threading.Event()

    def run_evaluation():
        evaluation_started.set()
        return service.evaluate_candidate(candidate.id, authoritative_source=True)

    with ThreadPoolExecutor(max_workers=2) as executor:
        transition_future = executor.submit(run_transition)
        assert transition_holds_lock.wait(timeout=5)
        evaluation_future = executor.submit(run_evaluation)
        assert evaluation_started.wait(timeout=5)
        assert not evaluation_entered_transaction.wait(timeout=0.1)
        allow_transition_commit.set()
        transition_future.result(timeout=5)
        decision, revision = evaluation_future.result(timeout=5)

    assert decision.decision.value == "retain_candidate"
    assert revision is None
    persisted = service.repository.get_candidate(candidate.id)
    assert persisted is not None
    if concurrent_transition == "reject":
        assert persisted.state.review_state == ReviewState.REJECTED
        assert persisted.state.lifecycle_status == LifecycleStatus.ACTIVE
    else:
        assert persisted.state.review_state == ReviewState.APPROVED
        assert persisted.state.lifecycle_status == LifecycleStatus.STALE
    assert service.repository.list_current_revisions() == []


def test_rejected_corroborating_contributor_cannot_support_later_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    first = _dependency(
        service,
        payload_ref="promotion-race-runbook",
        family=SourceFamily.RUNBOOK,
        lineage_group="promotion-race-runbook",
    )
    second = _dependency(
        service,
        payload_ref="promotion-race-dashboard",
        family=SourceFamily.DASHBOARD,
        lineage_group="promotion-race-dashboard",
    )
    service.review_candidate(first.id, approved=True, reviewer="reviewer")
    service.review_candidate(second.id, approved=True, reviewer="reviewer")
    rejection_holds_lock = threading.Event()
    allow_rejection_commit = threading.Event()
    evaluation_entered_transaction = threading.Event()
    transition = service.repository.transition_candidate_review
    evaluate_in_transaction = service._evaluate_candidate_in_transaction

    def delayed_rejection(updated, *, expected):
        result = transition(updated, expected=expected)
        if updated.id == second.id and updated.state.review_state == ReviewState.REJECTED:
            rejection_holds_lock.set()
            assert allow_rejection_commit.wait(timeout=5)
        return result

    def tracked_evaluation(*args, **kwargs):
        evaluation_entered_transaction.set()
        return evaluate_in_transaction(*args, **kwargs)

    monkeypatch.setattr(service.repository, "transition_candidate_review", delayed_rejection)
    monkeypatch.setattr(service, "_evaluate_candidate_in_transaction", tracked_evaluation)

    with ThreadPoolExecutor(max_workers=2) as executor:
        rejection = executor.submit(
            service.review_candidate,
            second.id,
            approved=False,
            reviewer="rejector",
        )
        assert rejection_holds_lock.wait(timeout=5)
        evaluation = executor.submit(service.evaluate_candidate, first.id)
        assert not evaluation_entered_transaction.wait(timeout=0.1)
        allow_rejection_commit.set()
        rejection.result(timeout=5)
        decision, revision = evaluation.result(timeout=5)

    assert decision.decision.value == "retain_candidate"
    assert "insufficient_independent_sources" in decision.reason_codes
    assert revision is None
    assert service.repository.list_current_revisions() == []


def test_revision_persistence_revalidates_every_loaded_contributor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    first = _dependency(
        service,
        payload_ref="persist-race-runbook",
        family=SourceFamily.RUNBOOK,
        lineage_group="persist-race-runbook",
    )
    second = _dependency(
        service,
        payload_ref="persist-race-dashboard",
        family=SourceFamily.DASHBOARD,
        lineage_group="persist-race-dashboard",
    )
    service.review_candidate(first.id, approved=True, reviewer="reviewer")
    service.review_candidate(second.id, approved=True, reviewer="reviewer")
    persist = service._persist_revision_with_projection

    def reject_after_contributors_are_loaded(revision, **kwargs):
        current = service.repository.get_candidate(second.id)
        assert current is not None
        rejected = current.model_copy(
            update={
                "state": transition_review_state(current.state, ReviewState.REJECTED),
                "updated_at": datetime.now(UTC),
            }
        )
        service.repository.transition_candidate_review(rejected, expected=current)
        return persist(revision, **kwargs)

    monkeypatch.setattr(
        service,
        "_persist_revision_with_projection",
        reject_after_contributors_are_loaded,
    )

    with pytest.raises(CandidateEvaluationConflictError, match="contributor changed"):
        service.evaluate_candidate(first.id)

    assert service.repository.list_current_revisions() == []
    stored = service.repository.get_candidate(second.id)
    assert stored is not None
    assert stored.state.review_state == ReviewState.APPROVED


def test_revision_persistence_revalidates_recorded_promotion_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="persist-race-authority",
        family=SourceFamily.HUMAN_CORRECTION,
        lineage_group="persist-race-authority",
    )
    service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
    persist = service._persist_revision_with_projection

    def alter_decision_inputs(revision, **kwargs):
        with service.repository._conn() as conn:
            row = conn.execute(
                "SELECT decision_json FROM promotion_decisions WHERE decision_id=?",
                (revision.decision_ref,),
            ).fetchone()
            payload = json.loads(row["decision_json"])
            payload["authoritative_source"] = False
            conn.execute(
                "UPDATE promotion_decisions SET decision_json=? WHERE decision_id=?",
                (json.dumps(payload), revision.decision_ref),
            )
        return persist(revision, **kwargs)

    monkeypatch.setattr(service, "_persist_revision_with_projection", alter_decision_inputs)

    with pytest.raises(CandidateEvaluationConflictError, match="promotion inputs changed"):
        service.evaluate_candidate(candidate.id, authoritative_source=True)

    assert service.repository.list_current_revisions() == []


def test_stale_worker_does_not_retire_a_newer_source_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    original = _dependency(
        service,
        payload_ref="runbook:source-generation",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:source-generation",
    )
    list_candidates = service.repository.list_candidates
    reingested = False

    def list_then_reingest(*args, **kwargs):
        nonlocal reingested
        observed = list_candidates(*args, **kwargs)
        if not reingested:
            reingested = True
            service.create_candidate(
                kind=original.kind,
                payload_ref=original.payload_ref,
                typed_payload={"source_generation": 2},
                proposition=original.proposition,
                scope=original.scope,
                evidence=original.evidence.items,
                provenance_refs=original.provenance_refs,
                candidate_id=original.id,
            )
        return observed

    monkeypatch.setattr(service.repository, "list_candidates", list_then_reingest)

    revisions = service.reconcile_source_lifecycle(
        provenance_ref=original.provenance_refs[0],
        source_stale=True,
    )

    current = service.repository.get_candidate(original.id)
    assert reingested is True
    assert revisions == []
    assert current is not None
    assert current.typed_payload == {"source_generation": 2}
    assert current.state.lifecycle_status == LifecycleStatus.ACTIVE


def test_candidate_review_cas_cannot_overwrite_a_lifecycle_transition(tmp_path: Path):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="review-lifecycle-race",
        family=SourceFamily.RUNBOOK,
        lineage_group="review-lifecycle-race",
    )
    approved = service.review_candidate(candidate.id, approved=True, reviewer="reviewer")
    stale_state = transition_lifecycle_state(approved.state, LifecycleStatus.STALE)
    stale = approved.model_copy(update={"state": stale_state, "updated_at": datetime.now(UTC)})
    service.repository.transition_candidate_lifecycle(stale, expected=approved)
    rejected_state = transition_review_state(approved.state, ReviewState.REJECTED)
    stale_review = approved.model_copy(update={"state": rejected_state, "updated_at": datetime.now(UTC)})

    with pytest.raises(CandidateReviewConflictError, match="candidate changed"):
        service.repository.transition_candidate_review(stale_review, expected=approved)

    persisted = service.repository.get_candidate(candidate.id)
    assert persisted is not None
    assert persisted.state.review_state == ReviewState.APPROVED
    assert persisted.state.lifecycle_status == LifecycleStatus.STALE


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
            "Investigate checkout in production us-east-1, cluster: prod-east, namespace: payments on release v2.4.1"
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


@pytest.mark.parametrize(
    "prompt",
    [
        "Checkout latency rose from 1.2 to 2.4 seconds.",
        "Checkout p95 is 2.4 seconds and error rate is 1.2 percent.",
    ],
)
def test_investigation_scope_ignores_unlabelled_decimal_measurements(prompt: str):
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt=prompt,
        services=["checkout"],
        archetype_ids=["latency_investigation"],
    )

    assert scope.version_constraints == []


@pytest.mark.parametrize(
    ("prompt", "expected"),
    [
        ("Investigate checkout release 2.4.1", "version:2.4.1"),
        ("Investigate checkout running v2.4.1", "version:v2.4.1"),
        ("Investigate checkout version: 2.4", "version:2.4"),
    ],
)
def test_investigation_scope_accepts_explicit_version_syntax(prompt: str, expected: str):
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt=prompt,
        services=["checkout"],
        archetype_ids=["latency_investigation"],
    )

    assert expected in scope.version_constraints


@pytest.mark.parametrize(
    "prompt",
    [
        "The checkout cluster is down.",
        "The checkout namespace is unhealthy.",
        "The checkout cluster remains down.",
        "The checkout namespace looks unhealthy.",
        "The checkout cluster health is degraded.",
        "The checkout namespace errors are elevated.",
        "The checkout cluster is currently down.",
        "The checkout namespace is still unhealthy.",
        "The checkout cluster and namespace are unhealthy.",
        "The checkout cluster health degraded.",
        "The checkout namespace errors elevated.",
        "The checkout cluster status shows degraded.",
        "The checkout cluster experienced an outage.",
        "The checkout namespace reports errors.",
        "The checkout cluster went offline.",
        "The checkout namespace started failing.",
        "The checkout cluster production is down and namespace payments is unhealthy.",
        "The checkout cluster prod-east and namespace payments.",
        "The checkout cluster: prod/east.",
        "The checkout namespace called pay@ments.",
        "The checkout cluster prod-east/west.",
        "The checkout cluster: arn:aws:eks:us-east-1:123456789012:cluster/prod.",
    ],
)
def test_investigation_scope_does_not_treat_status_prose_as_identifiers(prompt: str):
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt=prompt,
        services=["checkout"],
        archetype_ids=[],
    )

    assert scope.cluster_refs == []
    assert scope.namespace_refs == []


def test_investigation_scope_preserves_identifiers_before_status_prose():
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt="Cluster: prod-east is down and namespace: payments is unhealthy.",
        services=["checkout"],
        archetype_ids=[],
    )

    assert scope.cluster_refs == ["cluster:prod-east"]
    assert scope.namespace_refs == ["namespace:payments"]


@pytest.mark.parametrize(
    ("prompt", "cluster_ref", "namespace_ref"),
    [
        ("Cluster: down and namespace=slow.", "cluster:down", "namespace:slow"),
        ("Cluster: health and namespace=errors.", "cluster:health", "namespace:errors"),
    ],
)
def test_investigation_scope_preserves_explicit_status_named_identifiers(
    prompt: str,
    cluster_ref: str,
    namespace_ref: str,
):
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt=prompt,
        services=["checkout"],
        archetype_ids=[],
    )

    assert scope.cluster_refs == [cluster_ref]
    assert scope.namespace_refs == [namespace_ref]


@pytest.mark.parametrize(
    ("prompt", "cluster_ref", "namespace_ref"),
    [
        ("Cluster named prod and namespace called payments.", "cluster:prod", "namespace:payments"),
        ('Cluster "prod" and namespace "payments".', "cluster:prod", "namespace:payments"),
    ],
)
def test_investigation_scope_accepts_named_or_quoted_word_identifiers(
    prompt: str,
    cluster_ref: str,
    namespace_ref: str,
):
    scope = investigation_knowledge_scope(
        tenant_id="tenant-a",
        prompt=prompt,
        services=["checkout"],
        archetype_ids=[],
    )

    assert scope.cluster_refs == [cluster_ref]
    assert scope.namespace_refs == [namespace_ref]


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


@pytest.mark.parametrize("migration_kind", ["artifact", "signal"])
def test_legacy_import_review_cas_preserves_a_concurrent_rejection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    migration_kind: str,
):
    service = _service(tmp_path)
    transition = service.repository.transition_candidate_review
    rejection_won = False

    def reject_before_imported_review(updated, *, expected):
        nonlocal rejection_won
        if not rejection_won:
            rejection_won = True
            rejected = expected.model_copy(
                update={
                    "state": transition_review_state(expected.state, ReviewState.REJECTED),
                    "updated_at": datetime.now(UTC),
                }
            )
            transition(rejected, expected=expected)
        return transition(updated, expected=expected)

    monkeypatch.setattr(
        service.repository,
        "transition_candidate_review",
        reject_before_imported_review,
    )

    if migration_kind == "artifact":
        candidate_id = migrate_artifact_extractions(
            artifact_id="artifact-concurrent-import",
            artifact_type="runbook",
            rows={
                "dependency_hints": [
                    {
                        "id": "dep-concurrent-import",
                        "source_entity": "entity:service:checkout",
                        "target_entity": "entity:datastore:redis-session",
                        "direction": "depends_on",
                        "review_state": "approved",
                    }
                ]
            },
            service=service,
        )[0]
    else:
        candidate_id = migrate_signal_mapping(
            {
                "id": "signal-concurrent-import",
                "signal_type": "request_latency",
                "metric_pattern": "checkout_latency_seconds",
                "source_type": "dashboard_ingest",
                "source_refs": ["dashboard:concurrent-import"],
                "review_state": "trusted",
            },
            service=service,
        )

    candidate = service.repository.get_candidate(candidate_id)
    assert rejection_won is True
    assert candidate is not None
    assert candidate.state.review_state == ReviewState.REJECTED
    assert (
        service.repository.find_knowledge_by_proposition(
            "default",
            candidate.proposition.proposition_key,
        )
        is None
    )


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


def test_api_generic_candidate_review_rejects_correction_owned_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    _, candidate = service.create_correction(
        investigation_id="inv-api-correction-boundary",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "depends_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        explanation="Use the correction review workflow.",
        created_by="operator",
    )
    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    client = TestClient(create_app(runtime_settings=Settings(knowledge_permissions="knowledge.read,knowledge.review")))

    response = client.post(
        f"/api/v1/knowledge/candidates/{candidate.id}/review",
        json={"decision": "approve", "reviewer": "operator", "evaluate": False},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "correction candidates must be reviewed through the correction workflow"


def test_api_correction_lookup_is_scoped_before_authorization():
    import tacit.api.routes.knowledge as routes

    class RecordingHistoryStore:
        def __init__(self):
            self.calls = []

        def get_contract(self, investigation_id, revision, *, tenant_id=None):
            self.calls.append((investigation_id, revision, tenant_id))
            return None

    history = RecordingHistoryStore()
    app = create_app(
        runtime_settings=Settings(
            knowledge_tenant_id="*",
            api_auth_enabled=True,
            knowledge_tenant_api_keys={"tenant-a": "tenant-a-secret"},
            knowledge_permissions="knowledge.read,knowledge.correct",
        )
    )
    app.dependency_overrides[routes.get_history_store] = lambda: history
    client = TestClient(app)

    response = client.post(
        "/api/v1/knowledge/corrections",
        headers={"X-Tacit-Tenant": "tenant-a", "X-API-Key": "tenant-a-secret"},
        json={
            "investigation_id": "inv-other-tenant",
            "investigation_revision": 1,
            "correction_type": "dependency",
            "proposed": {
                "subject_ref": "entity:service:checkout",
                "predicate": "depends_on",
                "object_ref": "entity:datastore:redis-session",
            },
            "explanation": "This resource must be tenant-scoped before it is loaded.",
            "created_by": "operator",
        },
    )

    assert response.status_code == 404
    assert history.calls == [("inv-other-tenant", 1, "tenant-a")]


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


@pytest.mark.parametrize(
    "error",
    [
        CandidateReviewConflictError("candidate review state changed; reload before reviewing"),
        CandidateEvaluationConflictError("candidate inputs changed; reload before evaluating"),
        KnowledgeRevisionConflictError("knowledge target advanced from revision 1 to 2; rebase the correction"),
    ],
)
def test_api_reports_concurrent_correction_review_as_conflict(
    monkeypatch: pytest.MonkeyPatch,
    error: ValueError,
):
    import tacit.api.routes.knowledge as routes

    class ConflictingService:
        def review_correction(self, *args, **kwargs):
            raise error

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: ConflictingService())
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                knowledge_permissions="knowledge.read,knowledge.review,knowledge.apply",
            )
        )
    )

    response = client.post(
        "/api/v1/knowledge/corrections/correction-concurrent/review",
        json={"decision": "approve", "reviewer": "operator"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == str(error)


def test_api_reports_real_stale_correction_target_as_conflict(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    _, original = _promoted_dependency(service, "tenant-a")
    correction, candidate = service.create_correction(
        investigation_id="inv-api-stale-correction",
        investigation_revision=1,
        correction_type=CorrectionType.DEPENDENCY,
        target_ref=original.knowledge_id,
        target_revision=original.revision,
        proposed={
            "subject_ref": "entity:service:checkout",
            "predicate": "does_not_depend_on",
            "object_ref": "entity:datastore:redis-session",
        },
        scope=KnowledgeScope(
            tenant_id="tenant-a",
            environment_refs=["environment:production"],
            service_refs=["entity:service:checkout"],
        ),
        explanation="The dependency changed.",
        created_by="operator",
        tenant_id="tenant-a",
    )
    additional = _dependency(
        service,
        payload_ref="api-stale-correction-support",
        family=SourceFamily.INCIDENT,
        lineage_group="api-stale-correction-support",
        tenant_id="tenant-a",
    )
    service.review_candidate(additional.id, approved=True, reviewer="operator", tenant_id="tenant-a")
    _, advanced = service.evaluate_candidate(additional.id, tenant_id="tenant-a")
    assert advanced is not None

    import tacit.api.routes.knowledge as routes

    monkeypatch.setattr(routes, "get_knowledge_service", lambda request: service)
    client = TestClient(
        create_app(
            runtime_settings=Settings(
                knowledge_tenant_id="tenant-a",
                knowledge_permissions="knowledge.read,knowledge.review,knowledge.apply,knowledge.override",
            )
        )
    )

    response = client.post(
        f"/api/v1/knowledge/corrections/{correction.id}/review",
        json={"decision": "approve", "reviewer": "operator", "authoritative": True},
    )

    assert response.status_code == 409
    assert "advanced from revision 1 to 2" in response.json()["detail"]
    persisted = service.repository.get_candidate(candidate.id, "tenant-a")
    assert persisted is not None
    assert persisted.state.review_state == ReviewState.CANDIDATE


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


def test_cli_review_returns_post_evaluation_candidate_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="cli-evaluated-response",
        family=SourceFamily.RUNBOOK,
        lineage_group="cli-evaluated-response",
    )

    class Stores:
        settings = Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.override",
        )

        def knowledge(self):
            return service

    captured: dict[str, Any] = {}
    evaluation_returned = False
    original_evaluate = service.evaluate_candidate_result
    original_get_candidate = service.repository.get_candidate

    def evaluate_atomically(*args, **kwargs):
        nonlocal evaluation_returned
        result = original_evaluate(*args, **kwargs)
        evaluation_returned = True
        return result

    def reject_post_evaluation_reload(*args, **kwargs):
        if evaluation_returned:
            raise AssertionError("CLI must use the candidate captured by the evaluation transaction")
        return original_get_candidate(*args, **kwargs)

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", Stores)
    monkeypatch.setattr("tacit.cli._knowledge_json", lambda payload: captured.update(payload))
    monkeypatch.setattr(service, "evaluate_candidate_result", evaluate_atomically)
    monkeypatch.setattr(service.repository, "get_candidate", reject_post_evaluation_reload)

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

    assert result.exit_code == 0, result.output
    assert captured["promotion_decision"]["decision"] == "promote"
    assert captured["knowledge_revision"] is not None
    assert captured["candidate"]["state"]["eligibility"] == "contextual_only"
    assert captured["candidate"]["policy"]["promotion_policy_ref"] == "dependency-promotion-v1"


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
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.apply",
        )
    )

    response = TestClient(app).post(
        f"/api/v1/knowledge/corrections/{correction.id}/review",
        json={"decision": "approve", "reviewer": "operator", "authoritative": True},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.override"
    assert service.repository.get_candidate(candidate.id, "tenant-a").state.review_state == ReviewState.CANDIDATE


def test_api_correction_approval_requires_apply_permission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path, "tenant-a")
    correction, candidate = service.create_correction(
        investigation_id="inv-api-apply-permission",
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
        json={"decision": "approve", "reviewer": "operator", "authoritative": False},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.apply"
    assert service.repository.get_candidate(candidate.id, "tenant-a").state.review_state == ReviewState.CANDIDATE


def test_cli_policy_overrides_require_privileged_permission(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    service = _service(tmp_path)
    candidate = _dependency(
        service,
        payload_ref="cli-override",
        family=SourceFamily.RUNBOOK,
        lineage_group="cli-override",
    )

    class Stores:
        settings = Settings(_env_file=None, knowledge_permissions="knowledge.read,knowledge.review")

        def knowledge(self):
            raise AssertionError("unauthorized override initialized the knowledge store")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", Stores)

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
    class Stores:
        settings = Settings(_env_file=None, knowledge_permissions="knowledge.trust")

        def knowledge(self):
            raise AssertionError("unauthorized trust initialized the knowledge store")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", Stores)

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


@pytest.mark.parametrize(
    ("arguments", "permissions", "missing_permission"),
    [
        (["learn", "dashboard", "dash-1", "--auto-approve"], "knowledge.read", "knowledge.review"),
        (["learn", "approve", "dash-1"], "knowledge.read,knowledge.review", "knowledge.trust"),
        (["learn", "reject", "dash-1"], "knowledge.read", "knowledge.reject"),
        (["learn", "ignore", "dash-1"], "knowledge.read", "knowledge.reject"),
    ],
)
def test_cli_learning_transitions_enforce_semantic_permissions(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
    permissions: str,
    missing_permission: str,
):
    class ForbiddenStores:
        settings = SimpleNamespace(
            knowledge_tenant_id="default",
            knowledge_permissions=permissions,
        )

        def signals(self):
            raise AssertionError("unauthorized learning transition reached persistence")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: ForbiddenStores())

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code != 0
    assert f"missing permission: {missing_permission}" in result.output


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

    class WildcardStores:
        settings = Settings(_env_file=None, knowledge_tenant_id="*")

        def signals(self):
            raise AssertionError("tenant validation must precede persistence")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", WildcardStores)
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

    class FakeStores:
        settings = SimpleNamespace(
            knowledge_tenant_id="*",
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.trust,knowledge.reject",
        )

        def signals(self):
            return object()

    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "*")
    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", ingest_dashboard)
    monkeypatch.setattr("tacit.dashboard_ingest.learn_backend_dashboards", learn_dashboards)
    monkeypatch.setattr("tacit.alert_ingest.learn_backend_alerts", learn_alerts)
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: FakeStores())
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
        settings = SimpleNamespace(
            knowledge_tenant_id="*",
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.trust,knowledge.reject",
        )

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
    assert "data = await streamChart(payload, onStage, tenant)" in html
    assert html.count("headers: knowledgeHeaders({ 'Content-Type': 'application/json' }, tenant)") == 2
    assert "headers: knowledgeHeaders()," in html
    assert "fetch(`${BASE}/api/v1/investigations${qs}`, { headers: knowledgeHeaders() })" in html
    assert "fetch(`${BASE}/api/v1/investigations/${id}`, { headers: knowledgeHeaders() })" in html
    assert "fetch(`${BASE}/api/v1/investigations/stats`, { headers: knowledgeHeaders() })" in html
    assert 'data-dashboard-tenant="${escAttr(tenant)}"' in html
    assert "headers: knowledgeHeaders({}, tenant)" in html
    assert "btn.dataset.dashboardTenant" in html


def test_cli_resolves_tenants_from_active_runtime_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    runbook = tmp_path / "runbook.md"
    incident = tmp_path / "incident.md"
    runbook.write_text("Checkout depends on Redis.")
    incident.write_text("Checkout latency increased.")

    class WildcardStores:
        settings = Settings(_env_file=None, knowledge_tenant_id="*")

        def __getattr__(self, name):
            raise AssertionError(f"tenant validation must precede store access: {name}")

    monkeypatch.setattr("tacit.config.settings.knowledge_tenant_id", "default")
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", WildcardStores)
    runner = CliRunner()
    commands = [
        ["investigate", "checkout latency"],
        ["test", "--no-open-browser"],
        ["learn", "runbooks", "--file", str(runbook)],
        ["learn", "incidents", "--file", str(incident)],
        ["learn", "pagerduty", "--since", "2026-01-01T00:00:00Z"],
        ["knowledge", "review", "candidate", "--approve", "--reviewer", "operator"],
    ]

    for command in commands:
        result = runner.invoke(cli, command)
        assert result.exit_code != 0
        assert "--tenant is required" in result.output


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


def test_operational_learning_benchmark_isolated_from_runtime_governance(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("KNOWLEDGE_TENANT_ID", "*")
    monkeypatch.setenv("KNOWLEDGE_PERMISSIONS", "knowledge.read")
    monkeypatch.setattr("tacit.knowledge.service.settings.knowledge_tenant_id", "*")
    monkeypatch.setattr(
        "tacit.knowledge.service.settings.knowledge_permissions",
        "knowledge.read",
    )

    report = run_operational_learning_benchmark()

    assert report["passed"] is True
    assert report["metrics"]["passed_cases"]["numerator"] == report["case_count"]


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
