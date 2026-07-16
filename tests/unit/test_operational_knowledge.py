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
    KnowledgeEvidenceReference,
    KnowledgeScope,
    KnowledgeState,
)
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.models.schemas import EvidenceObservation, EvidenceObservationOutcome
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
    tenant_id: str = "default",
    predicate: str = "depends_on",
):
    scope = KnowledgeScope(
        tenant_id=tenant_id,
        environment_refs=["environment:production"],
        service_refs=["entity:service:checkout"],
    )
    return service.create_candidate(
        kind=KnowledgeKind.DEPENDENCY,
        payload_ref=payload_ref,
        typed_payload={"semantic": "unchanged"},
        proposition={
            "subject_ref": "entity:service:checkout",
            "predicate": predicate,
            "object_ref": "entity:datastore:redis-session",
        },
        scope=scope,
        evidence=[
            KnowledgeEvidenceReference(
                evidence_ref=f"evidence:{payload_ref}",
                evidence_role=EvidenceRole.SUPPORTING,
                source_family=family,
                lineage_group=lineage_group,
                lineage_kind=LineageKind.INDEPENDENT,
                provenance_refs=[f"provenance:{payload_ref}"],
            )
        ],
        provenance_refs=[f"provenance:{payload_ref}"],
        tenant_id=tenant_id,
    )


def _promoted_dependency(service: KnowledgeService, tenant_id: str = "default"):
    first = _dependency(
        service,
        payload_ref="runbook",
        family=SourceFamily.RUNBOOK,
        lineage_group="runbook:1",
        tenant_id=tenant_id,
    )
    _dependency(
        service,
        payload_ref="dashboard",
        family=SourceFamily.DASHBOARD,
        lineage_group="dashboard:1",
        tenant_id=tenant_id,
    )
    service.review_candidate(first.id, approved=True, reviewer="reviewer", tenant_id=tenant_id)
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
    service.repository.save_candidate(
        second.model_copy(update={"evidence": second.evidence.model_copy(update={"items": [copied]})})
    )
    summary, _ = service.corroboration.analyze("default", first.proposition.proposition_key)
    assert summary.raw_source_count == 2
    assert summary.independent_source_count == 1
    assert summary.duplicate_source_count == 1


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
    )
    assert reviewed.review_state == ReviewState.APPROVED
    assert candidate.id == correction.knowledge_candidate_ref
    assert replacement is not None
    assert service.repository.get_revision(original.knowledge_id).state.lifecycle_status == LifecycleStatus.SUPERSEDED
    assert service.repository.get_revision(original.knowledge_id, 1) == original
    assert service.impact(original.knowledge_id).recommended_action == "replay_current"


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


def test_cli_exposes_phase_three_commands():
    runner = CliRunner()
    assert runner.invoke(cli, ["knowledge", "--help"]).exit_code == 0
    output = runner.invoke(cli, ["knowledge", "review", "candidate", "--help"])
    assert output.exit_code == 0
    assert "--approve" in output.output
    assert runner.invoke(cli, ["learn", "status", "--help"]).exit_code == 0


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
