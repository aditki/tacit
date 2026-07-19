from __future__ import annotations

import asyncio
import json
import sqlite3
import tarfile
import threading
import time
import tomllib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tacit import __version__
from tacit.agents.providers.base import TokenUsage
from tacit.api.app import create_app
from tacit.api.dependencies import get_pipeline_dependencies
from tacit.backends.base import PublishResult
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies
from tacit.grounding_benchmark import (
    _contract_for_case,
    load_grounding_corpus,
    run_acceptance_corpus,
    run_grounding_benchmark,
)
from tacit.history import (
    ExactReplayMismatchError,
    InvestigationStore,
    ReplayError,
    ReplayInputsUnavailableError,
    StaleRevisionError,
)
from tacit.investigation_bundle import build_investigation_bundle
from tacit.investigation_contract import (
    SCHEMA_ID,
    GroundingStatus,
    InvestigationContract,
    InvestigationContractAssembler,
    InvestigationRunType,
    ProvenanceRecord,
    load_investigation_contract_schema,
    stamp_fingerprints,
)
from tacit.investigation_replay import (
    CounterfactualChanges,
    InvestigationReplaySnapshot,
    ReplayMode,
)
from tacit.knowledge.enums import KnowledgeUsageDisposition
from tacit.knowledge.models import KnowledgeSnapshot, KnowledgeUsage
from tacit.models.schemas import (
    ContextChunk,
    CulpritCandidate,
    CulpritRanking,
    DashboardSpec,
    DashRequest,
    DashResponse,
    EvidenceObservation,
    EvidenceObservationOutcome,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    PanelQuery,
    PanelSpec,
    SignalType,
)
from tacit.pipeline.completion import complete_pipeline
from tacit.pipeline.recording import PipelineRecorder


def _draft_contract(
    investigation_id: str = "inv_contract_test",
    *,
    dashboard_spec: DashboardSpec | None = None,
    evidence_requirements: list[EvidenceRequirement] | None = None,
    evidence_resolutions: list[EvidenceResolution] | None = None,
    evidence_observations: list[EvidenceObservation] | None = None,
    culprit_ranking: CulpritRanking | None = None,
    context_chunks: list[ContextChunk] | None = None,
) -> InvestigationContract:
    if dashboard_spec is None:
        dashboard_spec = DashboardSpec(
            title="Checkout latency",
            panels=[
                PanelSpec(
                    title="p95 latency",
                    queries=[
                        PanelQuery(
                            expr="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
                            datasource_uid="prom",
                            datasource_type="prometheus",
                            query_language="promql",
                            validation_status="passed",
                            validation_has_data=True,
                        )
                    ],
                )
            ],
        )
    if evidence_requirements is None:
        evidence_requirements = [
            EvidenceRequirement(
                id="er_01",
                evidence_type="metric",
                signal_type="request_latency",
                priority="critical",
                service_scope=["checkout"],
                source="symptom_confirmation",
            )
        ]
    if evidence_resolutions is None:
        evidence_resolutions = [
            EvidenceResolution(
                requirement_id="er_01",
                status=EvidenceResolutionStatus.RESOLVED,
                reason_code="metadata_inference",
                metric="http_request_duration_seconds_bucket",
                datasource_uid="prom",
                datasource_type="prometheus",
                query_language="promql",
                semantic_score=0.92,
            )
        ]
    if evidence_observations is None:
        evidence_observations = [
            EvidenceObservation(
                requirement_id="er_01",
                outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
                resolution_metric="http_request_duration_seconds_bucket",
                panel_title="p95 latency",
                query="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
                datasource_uid="prom",
                valid_query=True,
                non_empty=True,
                survived=True,
            )
        ]
    if culprit_ranking is None:
        culprit_ranking = CulpritRanking(
            abstained=False,
            abstention_reason="",
            telemetry_status="evidenced",
            candidates=[
                CulpritCandidate(
                    rank=1,
                    suspect="checkout",
                    suspect_type="service",
                    score=0.66,
                    contextual_reasons=["Checkout owns the affected request path."],
                    runtime_evidence=["request_latency"],
                    supporting_requirement_ids=["er_01"],
                )
            ],
        )
    return InvestigationContractAssembler().from_pipeline(
        investigation_id=investigation_id,
        revision=0,
        parent_revision=None,
        request=DashRequest(prompt="Why did checkout latency increase?", user_id="sdet"),
        intent=Intent(
            summary="Investigate checkout latency",
            domain="application",
            services=["checkout"],
            signals=[SignalType.METRICS],
            keywords=["latency"],
            timerange="30m",
            problem_type="latency_investigation",
        ),
        dashboard_spec=dashboard_spec,
        evidence_requirements=evidence_requirements,
        evidence_resolutions=evidence_resolutions,
        evidence_observations=evidence_observations,
        culprit_ranking=culprit_ranking,
        context_chunks=context_chunks,
        dashboard_url="http://grafana/d/checkout",
        dashboard_uid="checkout",
    )


def _snapshot_for(contract: InvestigationContract) -> InvestigationReplaySnapshot:
    return InvestigationReplaySnapshot(
        investigation_id=contract.investigation.id,
        created_at=contract.investigation.created_at,
        completed_at=contract.investigation.completed_at,
        request=DashRequest(prompt="Why did checkout latency increase?", user_id="sdet"),
        intent=Intent(
            summary="Investigate checkout latency",
            domain="application",
            services=["checkout"],
            signals=[SignalType.METRICS],
            keywords=["latency"],
            timerange="30m",
            problem_type="latency_investigation",
        ),
        dashboard_spec=DashboardSpec(
            title="Checkout latency",
            panels=[
                PanelSpec(
                    title="p95 latency",
                    queries=[
                        PanelQuery(
                            expr="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
                            datasource_uid="prom",
                            datasource_type="prometheus",
                            query_language="promql",
                            validation_status="passed",
                            validation_has_data=True,
                        )
                    ],
                )
            ],
        ),
        evidence_requirements=[
            EvidenceRequirement(
                id="er_01",
                evidence_type="metric",
                signal_type="request_latency",
                priority="critical",
                service_scope=["checkout"],
                source="symptom_confirmation",
            )
        ],
        evidence_resolutions=[
            EvidenceResolution(
                requirement_id="er_01",
                status=EvidenceResolutionStatus.RESOLVED,
                reason_code="metadata_inference",
                metric="http_request_duration_seconds_bucket",
                datasource_uid="prom",
                datasource_type="prometheus",
                query_language="promql",
                semantic_score=0.92,
            )
        ],
        evidence_observations=[
            EvidenceObservation(
                requirement_id="er_01",
                outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
                resolution_metric="http_request_duration_seconds_bucket",
                panel_title="p95 latency",
                query="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
                datasource_uid="prom",
                valid_query=True,
                non_empty=True,
                survived=True,
            )
        ],
        culprit_ranking=CulpritRanking(
            abstained=False,
            telemetry_status="evidenced",
            candidates=[
                CulpritCandidate(
                    rank=1,
                    suspect="checkout",
                    suspect_type="service",
                    score=0.66,
                    contextual_reasons=["Checkout owns the affected request path."],
                    runtime_evidence=["request_latency"],
                    supporting_requirement_ids=["er_01"],
                )
            ],
        ),
        renderings=contract.renderings,
        runtime=contract.runtime,
    )


def test_contract_revision_survives_persist_load_serialize_deserialize_compare(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    persisted = store.persist_contract_revision(_draft_contract(investigation_id))

    loaded = store.get_contract(investigation_id, persisted.investigation.revision)
    assert loaded is not None
    reloaded = InvestigationContract.model_validate_json(loaded.model_dump_json(by_alias=True))

    assert reloaded == loaded
    assert loaded.runtime.input_fingerprint.startswith("sha256:")
    assert loaded.runtime.output_fingerprint.startswith("sha256:")
    assert loaded.investigation.revision == 1
    assert store.list_revisions(investigation_id)[0]["revision"] == 1

    second = store.persist_contract_revision(_draft_contract(investigation_id), reason="current-engine-replay")
    comparison = store.compare_revisions(
        investigation_id, persisted.investigation.revision, second.investigation.revision
    )

    assert comparison is not None
    assert comparison["same_input"] is True
    assert comparison["same_output"] is True
    assert comparison["changed_sections"] == []


def test_exact_replay_records_run_without_changing_revision(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    persisted = store.persist_contract_revision(_draft_contract(investigation_id))

    replayed = store.replay_contract(investigation_id, persisted.investigation.revision)

    assert replayed == persisted
    assert len(store.list_revisions(investigation_id)) == 1


def test_exact_replay_preserves_pre_knowledge_v1_fingerprints(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    draft = _draft_contract(investigation_id)
    persisted = store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))
    legacy = stamp_fingerprints(persisted, include_knowledge_fields=False)
    contract_payload = legacy.model_dump(mode="json", by_alias=True)
    contract_payload.pop("knowledge_snapshot_ref")
    contract_payload.pop("knowledge_usage")
    contract_payload["request"]["scope"].pop("tenant_id")
    with store._conn() as conn:
        snapshot_row = conn.execute(
            """SELECT snapshot_json FROM investigation_snapshots
               WHERE investigation_id=? AND revision=?""",
            (investigation_id, persisted.investigation.revision),
        ).fetchone()
        snapshot_payload = json.loads(snapshot_row["snapshot_json"])
        snapshot_payload.pop("knowledge_snapshot_ref", None)
        snapshot_payload.pop("knowledge_usage", None)
        snapshot_payload["request"].pop("tenant_id", None)
        conn.execute(
            """UPDATE investigation_revisions
               SET contract_json=?, input_fingerprint=?, output_fingerprint=?
               WHERE investigation_id=? AND revision=?""",
            (
                json.dumps(contract_payload),
                legacy.runtime.input_fingerprint,
                legacy.runtime.output_fingerprint,
                investigation_id,
                persisted.investigation.revision,
            ),
        )
        conn.execute(
            """UPDATE investigation_snapshots SET snapshot_json=?
               WHERE investigation_id=? AND revision=?""",
            (json.dumps(snapshot_payload), investigation_id, persisted.investigation.revision),
        )

    replayed = store.replay_contract(investigation_id, mode=ReplayMode.EXACT)

    assert replayed is not None
    assert replayed.runtime.input_fingerprint == legacy.runtime.input_fingerprint
    assert replayed.runtime.output_fingerprint == legacy.runtime.output_fingerprint


def test_captured_input_replay_rebuilds_and_counterfactual_creates_revision(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    draft = _draft_contract(investigation_id)
    persisted = store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))

    exact = store.replay_contract(investigation_id, mode=ReplayMode.EXACT)
    assert exact is not None
    assert exact.runtime.output_fingerprint == persisted.runtime.output_fingerprint
    assert len(store.list_revisions(investigation_id)) == 1

    counterfactual = store.replay_contract(
        investigation_id,
        mode=ReplayMode.COUNTERFACTUAL,
        changes=CounterfactualChanges(
            reject_requirement_ids=["er_01"],
            add_context_chunks=[
                ContextChunk(content="New runbook evidence", source="runbook:new", relevance_score=0.7)
            ],
            candidate_score_overrides={"service:checkout": 0.4},
        ),
    )
    assert counterfactual is not None
    assert counterfactual.investigation.revision == 2
    assert counterfactual.observations == []
    assert counterfactual.evidence_resolutions[0].status == "unresolved"
    assert counterfactual.candidate_rankings[0].score == 0.4
    assert counterfactual.artifact_contributions[0].artifact_ref == "runbook:new"
    assert counterfactual.grounding.abstained is True
    assert counterfactual.grounding.maximum_trustworthy_conclusion["causal_status"] == "insufficient_evidence"


def test_chart_route_uses_configured_tenant_when_request_omits_it(monkeypatch):
    captured: dict[str, DashRequest] = {}

    async def fake_run_pipeline(request: DashRequest, deps):
        captured["request"] = request
        return DashResponse(
            dashboard_url="http://grafana/d/test",
            dashboard_uid="test",
            panel_count=1,
            summary="ok",
        )

    import tacit.api.routes.dashboard as dashboard_routes

    monkeypatch.setattr(dashboard_routes, "run_pipeline", fake_run_pipeline)
    app = create_app(runtime_settings=SimpleNamespace(api_auth_enabled=False, knowledge_tenant_id="tenant-a"))
    app.dependency_overrides[get_pipeline_dependencies] = lambda: SimpleNamespace(
        settings=SimpleNamespace(knowledge_tenant_id="tenant-a")
    )
    client = TestClient(app)

    response = client.post("/api/v1/chart", json={"prompt": "Investigate checkout latency"})

    assert response.status_code == 200
    assert captured["request"].tenant_id == "tenant-a"

    override_response = client.post(
        "/api/v1/chart",
        json={"prompt": "Investigate checkout latency", "tenant_id": "tenant-b"},
    )
    assert override_response.status_code == 200
    assert captured["request"].tenant_id == "tenant-a"


def test_chart_route_allows_body_tenant_for_wildcard_configuration(monkeypatch):
    captured: dict[str, DashRequest] = {}

    async def fake_run_pipeline(request: DashRequest, deps):
        captured["request"] = request
        return DashResponse(dashboard_url="", dashboard_uid="test", panel_count=1, summary="ok")

    import tacit.api.routes.dashboard as dashboard_routes

    monkeypatch.setattr(dashboard_routes, "run_pipeline", fake_run_pipeline)
    app = create_app(runtime_settings=SimpleNamespace(api_auth_enabled=False, knowledge_tenant_id="*"))
    app.dependency_overrides[get_pipeline_dependencies] = lambda: SimpleNamespace(
        settings=SimpleNamespace(knowledge_tenant_id="*")
    )
    client = TestClient(app)

    response = client.post(
        "/api/v1/chart",
        json={"prompt": "Investigate checkout latency", "tenant_id": "tenant-b"},
    )

    assert response.status_code == 200
    assert captured["request"].tenant_id == "tenant-b"


async def test_direct_pipeline_stamps_configured_fallback_tenant(monkeypatch):
    from tacit.pipeline import run_pipeline

    captured: dict[str, DashRequest] = {}

    async def fake_inner(request, deps, **kwargs):
        captured["request"] = request
        return DashResponse(dashboard_url="", dashboard_uid="", panel_count=0, summary="ok")

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", fake_inner)
    deps = PipelineDependencies(
        settings=SimpleNamespace(
            pipeline_max_concurrent=1,
            pipeline_timeout_seconds=5,
            knowledge_tenant_id="tenant-a",
        ),
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    await run_pipeline(DashRequest(prompt="Investigate checkout", tenant_id="tenant-b"), deps)

    assert captured["request"].tenant_id == "tenant-a"


async def test_direct_pipeline_requires_tenant_for_wildcard_configuration(monkeypatch):
    from tacit.pipeline import run_pipeline

    called = False

    async def fake_inner(request, deps, **kwargs):
        nonlocal called
        called = True
        return DashResponse(dashboard_url="", dashboard_uid="", panel_count=0, summary="unexpected")

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", fake_inner)
    deps = PipelineDependencies(
        settings=SimpleNamespace(
            pipeline_max_concurrent=1,
            pipeline_timeout_seconds=5,
            knowledge_tenant_id="*",
        ),
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(ValueError, match="tenant_id is required"):
        await run_pipeline(DashRequest(prompt="Investigate checkout"), deps)

    assert called is False


def test_counterfactual_replay_resorts_candidates_after_score_changes(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    ranking = CulpritRanking(
        abstained=False,
        telemetry_status="evidenced",
        candidates=[
            CulpritCandidate(
                rank=1,
                suspect="checkout",
                suspect_type="service",
                score=0.8,
                supporting_requirement_ids=["er_01"],
            ),
            CulpritCandidate(
                rank=2,
                suspect="shared-cache",
                suspect_type="cache",
                score=0.6,
                supporting_requirement_ids=["er_01"],
            ),
        ],
    )
    draft = _draft_contract(investigation_id, culprit_ranking=ranking)
    snapshot = _snapshot_for(draft).model_copy(update={"culprit_ranking": ranking})
    store.persist_contract_revision(draft, snapshot=snapshot)

    replayed = store.replay_contract(
        investigation_id,
        mode=ReplayMode.COUNTERFACTUAL,
        changes=CounterfactualChanges(candidate_score_overrides={"service:checkout": 0.2}),
    )

    assert replayed is not None
    assert [(candidate.candidate_ref, candidate.rank) for candidate in replayed.candidate_rankings] == [
        ("cache:shared-cache", 1),
        ("service:checkout", 2),
    ]
    assert replayed.grounding.maximum_trustworthy_conclusion["text"].startswith("cache:shared-cache")


def test_counterfactual_observation_removal_records_an_explicit_gap(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    draft = _draft_contract(investigation_id)
    store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))

    replayed = store.replay_contract(
        investigation_id,
        mode=ReplayMode.COUNTERFACTUAL,
        changes=CounterfactualChanges(remove_observation_ids=["obs_01"]),
    )

    assert replayed is not None
    assert replayed.observations[0].id == "obs_01"
    assert replayed.observations[0].status == "missing"
    assert replayed.observations[0].value["rejection_reason"] == "counterfactual_observation_removed"
    assert replayed.grounding.missing_observation_refs == ["obs_01"]
    assert replayed.grounding.status == GroundingStatus.INSUFFICIENT_EVIDENCE
    assert replayed.grounding.abstained is True
    assert replayed.grounding.unsafe_conclusions == []
    assert replayed.grounding.maximum_trustworthy_conclusion == {
        "text": "No culprit is supported by the captured evidence.",
        "causal_status": "insufficient_evidence",
    }


def test_counterfactual_context_changes_preserve_original_provenance_ids(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    context_chunks = [
        ContextChunk(content="First runbook", source="runbook:first", relevance_score=0.8),
        ContextChunk(content="Second runbook", source="runbook:second", relevance_score=0.7),
    ]
    draft = _draft_contract(investigation_id, context_chunks=context_chunks)
    snapshot = _snapshot_for(draft).model_copy(update={"context_chunks": context_chunks})
    store.persist_contract_revision(draft, snapshot=snapshot)

    replayed = store.replay_contract(
        investigation_id,
        mode=ReplayMode.COUNTERFACTUAL,
        changes=CounterfactualChanges(
            remove_context_refs=["prov_context_01"],
            stale_context_refs=["prov_context_02"],
        ),
    )

    assert replayed is not None
    context_provenance = [record for record in replayed.provenance if record.id.startswith("prov_context_")]
    assert [record.id for record in context_provenance] == ["prov_context_02"]
    assert context_provenance[0].source_ref == "runbook:second"
    assert context_provenance[0].freshness["status"] == "stale"
    assert replayed.artifact_contributions[0].provenance_refs == ["prov_context_02"]
    persisted_snapshot = store.get_snapshot(investigation_id, replayed.investigation.revision)
    assert persisted_snapshot is not None
    assert persisted_snapshot.context_chunks[0].metadata["provenance_id"] == "prov_context_02"


def test_non_exact_replay_requires_captured_inputs(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Legacy investigation", user_id="sdet")
    persisted = store.persist_contract_revision(_draft_contract(investigation_id))

    assert store.replay_contract(investigation_id, mode=ReplayMode.EXACT) == persisted
    with pytest.raises(ReplayInputsUnavailableError, match="Captured replay inputs are unavailable"):
        store.replay_contract(
            investigation_id,
            mode=ReplayMode.COUNTERFACTUAL,
            changes=CounterfactualChanges(candidate_score_overrides={"service:checkout": 0.1}),
        )

    assert store.list_runs(investigation_id)[-1]["status"] == "failed"
    assert len(store.list_revisions(investigation_id)) == 1


def test_exact_replay_fails_when_the_rebuilt_output_diverges(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    draft = _draft_contract(investigation_id)
    mismatched_ranking = _snapshot_for(draft).culprit_ranking.model_copy(
        update={"candidates": [_snapshot_for(draft).culprit_ranking.candidates[0].model_copy(update={"score": 0.1})]}
    )
    snapshot = _snapshot_for(draft).model_copy(update={"culprit_ranking": mismatched_ranking})
    store.persist_contract_revision(draft, snapshot=snapshot)

    with pytest.raises(ExactReplayMismatchError, match="output fingerprint does not match"):
        store.replay_contract(investigation_id, mode=ReplayMode.EXACT)

    replay_run = store.list_runs(investigation_id)[-1]
    assert replay_run["status"] == "failed"
    assert replay_run["error_code"] == "exact_replay_output_mismatch"


def test_replay_rebuild_failure_closes_the_run(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    draft = _draft_contract(investigation_id)
    store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))

    def fail_rebuild(*args, **kwargs):
        raise ValueError("snapshot is incompatible with the current assembler")

    monkeypatch.setattr("tacit.history.rebuild_contract", fail_rebuild)

    with pytest.raises(ValueError, match="snapshot is incompatible"):
        store.replay_contract(investigation_id, mode=ReplayMode.EXACT)

    replay_run = store.list_runs(investigation_id)[-1]
    assert replay_run["status"] == "failed"
    assert replay_run["error_code"] == "replay_failed"
    assert "ValueError: snapshot is incompatible" in replay_run["error_detail"]
    assert store.list_events(investigation_id, replay_run["run_id"])[-1]["event_type"] == "run_failed"


def test_non_exact_replay_rejects_a_stale_base_revision(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    draft = _draft_contract(investigation_id)
    first = store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))
    store.persist_contract_revision(_draft_contract(investigation_id), reason="refresh")

    with pytest.raises(StaleRevisionError):
        store.replay_contract(
            investigation_id,
            revision=first.investigation.revision,
            mode=ReplayMode.CURRENT_ENGINE,
        )

    assert len(store.list_revisions(investigation_id)) == 2
    assert store.list_runs(investigation_id)[-1]["status"] == "failed"


def test_current_engine_replay_applies_knowledge_to_baseline_ranking(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    draft = _draft_contract(investigation_id)
    baseline = CulpritRanking(
        abstained=False,
        telemetry_status="evidenced",
        candidates=[
            CulpritCandidate(
                rank=1,
                suspect="checkout",
                suspect_type="service",
                score=0.40,
                contextual_reasons=["runtime baseline"],
                runtime_evidence=["request_latency"],
                supporting_requirement_ids=["er_01"],
            )
        ],
    )
    historical = baseline.model_copy(
        update={
            "candidates": [
                baseline.candidates[0].model_copy(
                    update={
                        "score": 0.70,
                        "contextual_reasons": ["runtime baseline", "old operational knowledge"],
                    }
                )
            ]
        }
    )
    snapshot = _snapshot_for(draft).model_copy(
        update={"baseline_culprit_ranking": baseline, "culprit_ranking": historical}
    )
    store.persist_contract_revision(draft, snapshot=snapshot)

    class FakeKnowledgeService:
        seen_score: float | None = None
        persisted_usage: tuple[str, int, list[KnowledgeUsage]] | None = None

        def create_snapshot(self, scope):
            return KnowledgeSnapshot(
                id="knowledge_snapshot_current",
                tenant_id=scope.tenant_id,
                items=[],
                fingerprint="sha256:current",
            ), [
                KnowledgeUsage(
                    tenant_id=scope.tenant_id,
                    knowledge_ref="knowledge_current",
                    knowledge_revision=1,
                    disposition=KnowledgeUsageDisposition.APPLIED,
                    used_for=["ranking_context"],
                    target_ref="entity:service:checkout",
                    score_delta=0.10,
                    decision_ref="decision_current",
                )
            ]

        def reconcile_live_observations(self, usage, observations):
            return usage

        def snapshot_from_usage(self, tenant_id, usage):
            return KnowledgeSnapshot(
                id="knowledge_snapshot_reconciled",
                tenant_id=tenant_id,
                items=[],
                fingerprint="sha256:reconciled",
            )

        def apply_to_ranking(self, ranking, usage):
            self.seen_score = ranking.candidates[0].score
            return ranking.model_copy(
                update={
                    "candidates": [
                        ranking.candidates[0].model_copy(
                            update={
                                "score": ranking.candidates[0].score + 0.10,
                                "contextual_reasons": [
                                    *ranking.candidates[0].contextual_reasons,
                                    "fresh operational knowledge",
                                ],
                            }
                        )
                    ]
                }
            )

        def persist_usage(self, usage, *, investigation_id, investigation_revision):
            self.persisted_usage = (investigation_id, investigation_revision, usage)
            return usage

    fake = FakeKnowledgeService()
    monkeypatch.setattr("tacit.knowledge.service.get_knowledge_service", lambda: fake)

    replayed = store.replay_contract(investigation_id, mode=ReplayMode.CURRENT_ENGINE)

    assert fake.seen_score == 0.40
    assert replayed.candidate_rankings[0].score == 0.50
    assert replayed.knowledge_snapshot_ref == "knowledge_snapshot_reconciled"
    assert "old operational knowledge" not in replayed.candidate_rankings[0].contextual_reasons
    assert "fresh operational knowledge" in replayed.candidate_rankings[0].contextual_reasons
    assert fake.persisted_usage is not None
    assert fake.persisted_usage[0] == investigation_id
    assert fake.persisted_usage[1] == replayed.investigation.revision
    assert fake.persisted_usage[2][0].knowledge_ref == "knowledge_current"


def test_current_engine_replay_snapshots_reconciled_knowledge_usage(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    draft = _draft_contract(investigation_id)
    store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))

    class ContradictingKnowledgeService:
        persisted = False

        def create_snapshot(self, scope):
            return KnowledgeSnapshot(
                id="knowledge_snapshot_before_reconciliation",
                tenant_id=scope.tenant_id,
                items=[],
                fingerprint="sha256:before",
            ), [
                KnowledgeUsage(
                    tenant_id=scope.tenant_id,
                    knowledge_ref="knowledge_contradicted",
                    knowledge_revision=1,
                    disposition=KnowledgeUsageDisposition.APPLIED,
                )
            ]

        def reconcile_live_observations(self, usage, observations):
            return [usage[0].model_copy(update={"disposition": KnowledgeUsageDisposition.CONTRADICTED_BY_OBSERVATION})]

        def snapshot_from_usage(self, tenant_id, usage):
            assert usage[0].disposition == KnowledgeUsageDisposition.CONTRADICTED_BY_OBSERVATION
            return KnowledgeSnapshot(
                id="knowledge_snapshot_after_reconciliation",
                tenant_id=tenant_id,
                items=[],
                fingerprint="sha256:after",
            )

        def apply_to_ranking(self, ranking, usage):
            return ranking

        def persist_usage(self, usage, *, investigation_id, investigation_revision):
            self.persisted = True
            assert investigation_revision == 2
            assert usage[0].disposition == KnowledgeUsageDisposition.CONTRADICTED_BY_OBSERVATION
            return usage

    fake = ContradictingKnowledgeService()
    monkeypatch.setattr("tacit.knowledge.service.get_knowledge_service", lambda: fake)

    replayed = store.replay_contract(investigation_id, mode=ReplayMode.CURRENT_ENGINE)

    assert replayed.knowledge_snapshot_ref == "knowledge_snapshot_after_reconciliation"
    assert replayed.knowledge_usage[0].disposition == KnowledgeUsageDisposition.CONTRADICTED_BY_OBSERVATION
    assert fake.persisted is True


def test_current_engine_replay_succeeds_when_usage_persistence_fails(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    draft = _draft_contract(investigation_id)
    store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))

    class FailingUsageService:
        def create_snapshot(self, scope):
            usage = KnowledgeUsage(
                tenant_id=scope.tenant_id,
                knowledge_ref="knowledge_current",
                knowledge_revision=1,
                disposition=KnowledgeUsageDisposition.APPLIED,
            )
            return (
                KnowledgeSnapshot(
                    id="knowledge_snapshot_current",
                    tenant_id=scope.tenant_id,
                    items=[],
                    fingerprint="sha256:current",
                ),
                [usage],
            )

        def reconcile_live_observations(self, usage, observations):
            return usage

        def snapshot_from_usage(self, tenant_id, usage):
            return KnowledgeSnapshot(
                id="knowledge_snapshot_reconciled",
                tenant_id=tenant_id,
                items=[],
                fingerprint="sha256:reconciled",
            )

        def apply_to_ranking(self, ranking, usage):
            return ranking

        def persist_usage(self, usage, *, investigation_id, investigation_revision):
            raise OSError("knowledge database unavailable")

    monkeypatch.setattr(
        "tacit.knowledge.service.get_knowledge_service",
        lambda: FailingUsageService(),
    )

    replayed = store.replay_contract(investigation_id, mode=ReplayMode.CURRENT_ENGINE)

    assert replayed.investigation.revision == 2
    assert store.get_contract(investigation_id).investigation.revision == 2
    replay_run = store.list_runs(investigation_id)[-1]
    assert replay_run["status"] == "completed"
    assert replay_run["error_code"] == ""


def test_current_engine_replay_requires_concrete_tenant_in_wildcard_mode(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    draft = _draft_contract(investigation_id)
    store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))
    monkeypatch.setattr("tacit.history.settings.knowledge_tenant_id", "*")

    with pytest.raises(ReplayError, match="tenant_id is required"):
        store.replay_contract(investigation_id, mode=ReplayMode.CURRENT_ENGINE)

    assert len(store.list_revisions(investigation_id)) == 1
    replay_run = store.list_runs(investigation_id)[-1]
    assert replay_run["status"] == "failed"
    assert replay_run["error_code"] == "replay_failed"


def test_current_engine_replay_rejects_pinned_tenant_mismatch(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    draft = _draft_contract(investigation_id)
    snapshot = _snapshot_for(draft)
    snapshot = snapshot.model_copy(update={"request": snapshot.request.model_copy(update={"tenant_id": "tenant-b"})})
    store.persist_contract_revision(draft, snapshot=snapshot)
    monkeypatch.setattr("tacit.history.settings.knowledge_tenant_id", "default")
    captured: dict[str, str] = {}

    class CapturingKnowledgeService:
        def create_snapshot(self, scope):
            captured["tenant_id"] = scope.tenant_id
            return (
                KnowledgeSnapshot(
                    id="knowledge_snapshot_pinned",
                    tenant_id=scope.tenant_id,
                    items=[],
                    fingerprint="sha256:pinned",
                ),
                [],
            )

        def reconcile_live_observations(self, usage, observations):
            return usage

        def snapshot_from_usage(self, tenant_id, usage):
            return KnowledgeSnapshot(
                id="knowledge_snapshot_pinned",
                tenant_id=tenant_id,
                items=[],
                fingerprint="sha256:pinned",
            )

        def apply_to_ranking(self, ranking, usage):
            return ranking

    monkeypatch.setattr(
        "tacit.knowledge.service.get_knowledge_service",
        lambda: CapturingKnowledgeService(),
    )

    with pytest.raises(ReplayError, match="does not match configured tenant"):
        store.replay_contract(
            investigation_id,
            mode=ReplayMode.CURRENT_ENGINE,
            runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
        )

    assert captured == {}


def test_concurrent_revision_writers_report_a_stale_parent(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    store.persist_contract_revision(_draft_contract(investigation_id))
    barrier = threading.Barrier(2)

    def persist(reason):
        barrier.wait()
        try:
            revision = store.persist_contract_revision(
                _draft_contract(investigation_id),
                reason=reason,
                expected_parent_revision=1,
            )
        except StaleRevisionError:
            return "stale"
        return revision.investigation.revision

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(persist, ["refresh", "replay"]))

    assert sorted(results, key=str) == [2, "stale"]
    assert [revision["revision"] for revision in store.list_revisions(investigation_id)] == [1, 2]


def test_approved_correction_creates_provenance_bearing_revision(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    draft = _draft_contract(investigation_id)
    store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=None,
        correction_text="The shared cache was saturated.",
        target_ref="cache:shared-cache",
        created_by="reviewer",
    )
    assert candidate is not None
    reviewed = store.review_knowledge_candidate(
        investigation_id,
        candidate.id,
        approved=True,
        reviewed_by="approver",
    )
    assert reviewed is not None and reviewed.status.value == "approved"
    assert reviewed.provenance.review_state == "approved"

    corrected = store.apply_knowledge_candidate(investigation_id, candidate.id)

    assert corrected is not None
    assert corrected.investigation.revision == 2
    assert corrected.corrections[-1].correction_ref == candidate.id
    assert corrected.corrections[-1].applied_in_revision == 2
    assert corrected.provenance[-1].source_type == "human_correction"
    applied_candidate = store.list_knowledge_candidates(investigation_id)[0]
    assert applied_candidate.status.value == "applied"
    assert applied_candidate.provenance.review_state == "applied"
    assert store.apply_knowledge_candidate(investigation_id, candidate.id) == corrected
    replayed = store.replay_contract(investigation_id, mode=ReplayMode.EXACT)
    assert replayed is not None
    assert replayed.runtime.output_fingerprint == corrected.runtime.output_fingerprint


@pytest.mark.parametrize(("approved", "expected_state"), [(True, "approved"), (False, "rejected")])
def test_candidate_review_updates_provenance_state(tmp_path, approved, expected_state):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    store.persist_contract_revision(_draft_contract(investigation_id))
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Cache was saturated.",
    )
    assert candidate is not None

    reviewed = store.review_knowledge_candidate(
        investigation_id,
        candidate.id,
        approved=approved,
        reviewed_by="reviewer",
    )

    assert reviewed is not None
    assert reviewed.status.value == expected_state
    assert reviewed.provenance.review_state == expected_state
    reloaded = store.list_knowledge_candidates(investigation_id)[0]
    assert reloaded.status.value == expected_state
    assert reloaded.provenance.review_state == expected_state


def test_overlapping_candidate_reviews_preserve_the_first_terminal_decision(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    store.persist_contract_revision(_draft_contract(investigation_id))
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Cache was saturated.",
    )
    assert candidate is not None
    barrier = threading.Barrier(2)

    def review(approved):
        barrier.wait()
        return store.review_knowledge_candidate(
            investigation_id,
            candidate.id,
            approved=approved,
            reviewed_by=f"reviewer-{approved}",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(review, [True, False]))

    assert all(result is not None for result in results)
    returned_states = {result.status.value for result in results if result is not None}
    assert len(returned_states) == 1
    stored = store.list_knowledge_candidates(investigation_id)[0]
    assert stored.status.value in {"approved", "rejected"}
    assert stored.status.value in returned_states
    assert stored.provenance.review_state == stored.status.value


def test_candidate_application_rolls_back_revision_if_candidate_transition_fails(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    store.persist_contract_revision(_draft_contract(investigation_id))
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Cache was saturated.",
    )
    assert candidate is not None
    store.review_knowledge_candidate(investigation_id, candidate.id, approved=True, reviewed_by="reviewer")
    with store._conn() as conn:
        conn.execute("""CREATE TRIGGER fail_candidate_application
               BEFORE UPDATE ON knowledge_candidates
               WHEN NEW.status = 'applied'
               BEGIN SELECT RAISE(ABORT, 'candidate transition failed'); END""")

    with pytest.raises(sqlite3.IntegrityError, match="candidate transition failed"):
        store.apply_knowledge_candidate(investigation_id, candidate.id)

    assert len(store.list_revisions(investigation_id)) == 1
    assert store.get(investigation_id)["current_revision"] == 1
    unchanged = store.list_knowledge_candidates(investigation_id)[0]
    assert unchanged.status.value == "approved"
    assert unchanged.applied_revision is None
    with store._conn() as conn:
        conn.execute("DROP TRIGGER fail_candidate_application")

    corrected = store.apply_knowledge_candidate(investigation_id, candidate.id)
    assert corrected is not None
    assert corrected.investigation.revision == 2
    assert store.apply_knowledge_candidate(investigation_id, candidate.id) == corrected


def test_expiry_does_not_overwrite_applied_or_rejected_candidate_states(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    store.persist_contract_revision(_draft_contract(investigation_id))
    applied_candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Cache was saturated.",
    )
    rejected_candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Checkout was healthy.",
    )
    assert applied_candidate is not None and rejected_candidate is not None
    store.review_knowledge_candidate(investigation_id, applied_candidate.id, approved=True, reviewed_by="reviewer")
    store.review_knowledge_candidate(investigation_id, rejected_candidate.id, approved=False, reviewed_by="reviewer")
    corrected = store.apply_knowledge_candidate(investigation_id, applied_candidate.id)
    assert corrected is not None
    with store._conn() as conn:
        conn.execute(
            "UPDATE knowledge_candidates SET expires_at=? WHERE id IN (?, ?)",
            (time.time() - 60, applied_candidate.id, rejected_candidate.id),
        )

    assert store.apply_knowledge_candidate(investigation_id, applied_candidate.id) == corrected
    assert store.apply_knowledge_candidate(investigation_id, rejected_candidate.id) is None
    candidates = {candidate.id: candidate for candidate in store.list_knowledge_candidates(investigation_id)}
    assert candidates[applied_candidate.id].status.value == "applied"
    assert candidates[applied_candidate.id].provenance.review_state == "applied"
    assert candidates[rejected_candidate.id].status.value == "rejected"
    assert candidates[rejected_candidate.id].provenance.review_state == "rejected"


def test_legacy_migration_and_assessment_bundle_are_portable(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Legacy checkout investigation", user_id="sdet")
    store.finish(investigation_id, status="success", dashboard_uid="legacy", dashboard_url="http://grafana/legacy")

    migrated = store.migrate_legacy_investigation(investigation_id)

    assert migrated is not None
    assert migrated.grounding.status == GroundingStatus.INDETERMINATE
    assert migrated.grounding.migration_notes
    assert migrated.provenance == []
    bundle = build_investigation_bundle(store, investigation_id)
    with tarfile.open(fileobj=BytesIO(bundle), mode="r:gz") as archive:
        names = set(archive.getnames())
    assert {"manifest.json", "contract.json", "expected_outcomes.json", "revisions.json"} <= names


def test_assessment_bundle_excludes_revisions_newer_than_the_export(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    store.persist_contract_revision(_draft_contract(investigation_id))
    store.persist_contract_revision(_draft_contract(investigation_id), reason="refresh")
    store.persist_contract_revision(_draft_contract(investigation_id), reason="correction")

    bundle = build_investigation_bundle(store, investigation_id, revision=1)

    with tarfile.open(fileobj=BytesIO(bundle), mode="r:gz") as archive:
        revisions_file = archive.extractfile("revisions.json")
        assert revisions_file is not None
        revisions = json.load(revisions_file)
        assert "comparison.json" not in archive.getnames()
    assert [item["revision"] for item in revisions] == [1]


def test_correction_ownership_is_checked_before_mutation(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    owner_id = store.start("Owner investigation")
    wrong_id = store.start("Wrong investigation")
    store.persist_contract_revision(_draft_contract(owner_id))
    candidate = store.create_knowledge_candidate(
        owner_id,
        revision=None,
        correction_text="Cache was saturated.",
    )
    assert candidate is not None

    assert (
        store.review_knowledge_candidate(
            wrong_id,
            candidate.id,
            approved=True,
            reviewed_by="reviewer",
        )
        is None
    )
    assert store.list_knowledge_candidates(owner_id)[0].status.value == "pending_review"
    approved = store.review_knowledge_candidate(
        owner_id,
        candidate.id,
        approved=True,
        reviewed_by="reviewer",
    )
    assert approved is not None

    assert store.apply_knowledge_candidate(wrong_id, candidate.id) is None
    assert len(store.list_revisions(owner_id)) == 1
    assert store.list_knowledge_candidates(owner_id)[0].status.value == "approved"


def test_stale_correction_is_not_applied_to_a_newer_revision(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    store.persist_contract_revision(_draft_contract(investigation_id))
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Cache was saturated.",
    )
    assert candidate is not None
    approved = store.review_knowledge_candidate(
        investigation_id,
        candidate.id,
        approved=True,
        reviewed_by="reviewer",
    )
    assert approved is not None
    store.persist_contract_revision(_draft_contract(investigation_id), reason="refresh")

    assert store.apply_knowledge_candidate(investigation_id, candidate.id) is None
    assert len(store.list_revisions(investigation_id)) == 2
    assert store.list_knowledge_candidates(investigation_id)[0].status.value == "approved"


def test_run_completed_event_follows_revision_events(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?")
    run_id = store.start_run(investigation_id, run_type=InvestigationRunType.INITIAL)
    store.persist_contract_revision(_draft_contract(investigation_id), run_id=run_id)

    before_completion = [event["event_type"] for event in store.list_events(investigation_id, run_id)]
    assert before_completion[-1] == "revision_persisted"
    assert "run_completed" not in before_completion

    store.complete_run(run_id, status="completed")
    after_completion = [event["event_type"] for event in store.list_events(investigation_id, run_id)]
    assert after_completion[-1] == "run_completed"
    assert after_completion.index("revision_persisted") < after_completion.index("run_completed")
    runtime_manifest = store.list_runs(investigation_id)[0]["runtime_manifest"]
    assert runtime_manifest["engine_version"] == __version__


async def test_failed_refresh_does_not_overwrite_current_investigation_row(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
    store.persist_contract_revision(_draft_contract(investigation_id))
    store.finish(
        investigation_id,
        status="success",
        dashboard_uid="current",
        dashboard_url="http://grafana/current",
    )
    deps = PipelineDependencies(
        settings=SimpleNamespace(pipeline_max_concurrent=1, pipeline_timeout_seconds=5),
        backend_factory=lambda: [],
        history_store_factory=lambda: store,
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    from tacit.pipeline import run_pipeline

    response = await run_pipeline(
        DashRequest(prompt="Refresh checkout", user_id="api"),
        deps,
        investigation_id=investigation_id,
        run_type=InvestigationRunType.REFRESH,
    )

    current = store.get(investigation_id)
    assert response.investigation_id == investigation_id
    assert current is not None
    assert current["status"] == "success"
    assert current["dashboard_uid"] == "current"
    assert current["dashboard_url"] == "http://grafana/current"
    assert current["current_revision"] == 1
    assert store.list_runs(investigation_id)[-1]["status"] == "failed"


async def test_pipeline_preserves_a_caller_supplied_base_revision(monkeypatch):
    from tacit.pipeline import run_pipeline

    received: dict[str, object] = {}

    async def fake_inner(request, deps, **kwargs):
        received.update(kwargs)
        return DashResponse(dashboard_url="", dashboard_uid="", panel_count=0, summary="pinned")

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", fake_inner)
    deps = PipelineDependencies(
        settings=SimpleNamespace(pipeline_max_concurrent=1, pipeline_timeout_seconds=5),
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    await run_pipeline(
        DashRequest(prompt="Refresh revision one", user_id="api"),
        deps,
        investigation_id="inv-pinned",
        run_type=InvestigationRunType.REFRESH,
        base_revision=1,
    )

    assert received["investigation_id"] == "inv-pinned"
    assert received["run_type"] == InvestigationRunType.REFRESH
    assert received["base_revision"] == 1


async def test_backend_factory_failure_completes_the_pipeline_run(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    resources_closed = False

    def failing_backend_factory():
        raise RuntimeError("backend configuration is invalid")

    async def close_resources():
        nonlocal resources_closed
        resources_closed = True

    deps = PipelineDependencies(
        settings=SimpleNamespace(pipeline_max_concurrent=1, pipeline_timeout_seconds=5),
        backend_factory=failing_backend_factory,
        history_store_factory=lambda: store,
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        resource_cleanup=close_resources,
    )

    from tacit.pipeline import run_pipeline

    with pytest.raises(RuntimeError, match="backend configuration is invalid"):
        await run_pipeline(DashRequest(prompt="Investigate checkout", user_id="api"), deps)

    investigation = store.list_recent()[0]
    run = store.list_runs(investigation["id"])[0]
    assert investigation["status"] == "failed"
    assert run["status"] == "failed"
    assert run["error_code"] == "pipeline_failed"
    assert "backend configuration is invalid" in run["error_detail"]
    assert resources_closed is True


async def test_caller_cancellation_records_cancelled_not_timeout(tmp_path, monkeypatch):
    class WaitingBackend:
        name = "grafana"
        query_language = "promql"

        async def close(self):
            return None

    started = asyncio.Event()

    async def waiting_classify(prompt):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("tacit.pipeline.classify_intent", waiting_classify)
    store = InvestigationStore(db_path=tmp_path / "history.db")
    deps = PipelineDependencies(
        settings=SimpleNamespace(pipeline_max_concurrent=1, pipeline_timeout_seconds=5),
        backend_factory=lambda: [WaitingBackend()],
        history_store_factory=lambda: store,
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    from tacit.pipeline import run_pipeline

    task = asyncio.create_task(run_pipeline(DashRequest(prompt="Investigate checkout", user_id="api"), deps))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    investigation = store.list_recent()[0]
    run = store.list_runs(investigation["id"])[0]
    assert investigation["status"] == "cancelled"
    assert investigation["error"] == "Pipeline cancelled by caller"
    assert run["status"] == "cancelled"
    assert run["error_code"] == "pipeline_cancelled"
    assert store.list_events(investigation["id"], run["run_id"])[-1]["event_type"] == "run_cancelled"
    assert store.stats()["cancelled"] == 1
    assert store.stats()["timed_out"] == 0


async def test_pipeline_deadline_still_records_timeout(tmp_path, monkeypatch):
    class WaitingBackend:
        name = "grafana"
        query_language = "promql"

        async def close(self):
            return None

    async def waiting_classify(prompt):
        await asyncio.Event().wait()

    monkeypatch.setattr("tacit.pipeline.classify_intent", waiting_classify)
    store = InvestigationStore(db_path=tmp_path / "history.db")
    deps = PipelineDependencies(
        settings=SimpleNamespace(pipeline_max_concurrent=1, pipeline_timeout_seconds=0.01),
        backend_factory=lambda: [WaitingBackend()],
        history_store_factory=lambda: store,
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    from tacit.pipeline import run_pipeline

    response = await run_pipeline(DashRequest(prompt="Investigate checkout", user_id="api"), deps)

    investigation = store.list_recent()[0]
    run = store.list_runs(investigation["id"])[0]
    assert "timed out" in response.summary
    assert investigation["status"] == "timeout"
    assert run["status"] == "failed"
    assert run["error_code"] == "pipeline_timeout"


async def test_successful_refresh_only_advances_the_legacy_row_revision_pointer(tmp_path):
    class PublishedBackend:
        name = "grafana"
        query_language = "promql"

        async def publish(self, dashboard_spec):
            return PublishResult(url="http://grafana/refreshed", uid="refreshed", backend_name=self.name)

    class FeedbackStore:
        def record_provenance(self, **kwargs):
            return None

    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Original prompt", user_id="api")
    store.record_intent(investigation_id, summary="Original intent", services=["legacy-service"])
    store.record_queries(
        investigation_id,
        metrics_selected=["legacy_metric"],
        generated_queries=[{"expr": "legacy_metric"}],
        panel_count=1,
        path_used="archetype",
    )
    store.persist_contract_revision(_draft_contract(investigation_id))
    store.finish(
        investigation_id,
        status="success",
        dashboard_uid="original",
        dashboard_url="http://grafana/original",
    )
    run_id = store.start_run(investigation_id, run_type=InvestigationRunType.REFRESH, base_revision=1)
    recorder = PipelineRecorder(
        store,
        investigation_id,
        run_id=run_id,
        record_investigation_updates=False,
    )
    refreshed_dashboard = DashboardSpec(
        title="Refreshed dashboard",
        panels=[PanelSpec(title="New traffic", queries=[PanelQuery(expr="new_metric", datasource_uid="prom")])],
    )
    deps = PipelineDependencies(
        settings=object(),
        backend_factory=lambda: [],
        history_store_factory=lambda: store,
        feedback_store_factory=FeedbackStore,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    response = await complete_pipeline(
        request=DashRequest(prompt="Refreshed prompt", user_id="api"),
        deps=deps,
        backends=[PublishedBackend()],
        dashboard_spec=refreshed_dashboard,
        intent=Intent(summary="Refreshed intent", domain="application", services=["new-service"]),
        metric_catalog=[],
        datasource_catalog=[],
        ranked_archetypes_present=False,
        validation_warnings=["new warning"],
        panels_before=1,
        evidence_requirements=[],
        evidence_resolutions=[],
        evidence_observations=[],
        culprit_ranking=CulpritRanking(),
        run_type=InvestigationRunType.REFRESH,
        base_revision=1,
        timings={},
        recorder=recorder,
        token_usage=TokenUsage(),
        started_at=time.monotonic(),
    )

    legacy = store.get(investigation_id)
    assert response.investigation_revision == 2
    assert legacy is not None
    assert legacy["current_revision"] == 2
    assert legacy["prompt"] == "Original prompt"
    assert legacy["intent_summary"] == "Original intent"
    assert legacy["generated_queries"] == [{"expr": "legacy_metric"}]
    assert legacy["validation_warnings"] == []
    assert legacy["dashboard_uid"] == "original"
    assert legacy["dashboard_url"] == "http://grafana/original"
    assert store.list_runs(investigation_id)[-1]["status"] == "completed"


async def test_refresh_persistence_rejects_a_base_that_advanced_during_the_run(tmp_path):
    class PublishedBackend:
        name = "grafana"
        query_language = "promql"

        async def publish(self, dashboard_spec):
            return PublishResult(url="http://grafana/stale-refresh", uid="stale-refresh", backend_name=self.name)

    class FeedbackStore:
        def record_provenance(self, **kwargs):
            return None

    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
    store.persist_contract_revision(_draft_contract(investigation_id))
    store.finish(
        investigation_id,
        status="success",
        dashboard_uid="current",
        dashboard_url="http://grafana/current",
    )
    run_id = store.start_run(
        investigation_id,
        run_type=InvestigationRunType.REFRESH,
        base_revision=1,
    )
    store.persist_contract_revision(_draft_contract(investigation_id), reason="concurrent-correction")
    recorder = PipelineRecorder(
        store,
        investigation_id,
        run_id=run_id,
        record_investigation_updates=False,
    )
    dashboard = DashboardSpec(
        title="Refresh",
        panels=[PanelSpec(title="Traffic", queries=[PanelQuery(expr="up", datasource_uid="prom")])],
    )
    deps = PipelineDependencies(
        settings=object(),
        backend_factory=lambda: [],
        history_store_factory=lambda: store,
        feedback_store_factory=FeedbackStore,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    response = await complete_pipeline(
        request=DashRequest(prompt="Refresh checkout", user_id="api"),
        deps=deps,
        backends=[PublishedBackend()],
        dashboard_spec=dashboard,
        intent=Intent(summary="Refresh checkout", domain="application", services=["checkout"]),
        metric_catalog=[],
        datasource_catalog=[],
        ranked_archetypes_present=False,
        validation_warnings=[],
        panels_before=1,
        evidence_requirements=[],
        evidence_resolutions=[],
        evidence_observations=[],
        culprit_ranking=CulpritRanking(),
        run_type=InvestigationRunType.REFRESH,
        base_revision=1,
        timings={},
        recorder=recorder,
        token_usage=TokenUsage(),
        started_at=time.monotonic(),
    )

    current = store.get(investigation_id)
    assert response.dashboard_url == "http://grafana/stale-refresh"
    assert response.investigation_revision is None
    assert len(store.list_revisions(investigation_id)) == 2
    assert current is not None
    assert current["status"] == "success"
    assert current["dashboard_uid"] == "current"
    refresh_run = next(run for run in store.list_runs(investigation_id) if run["run_id"] == run_id)
    assert refresh_run["status"] == "failed"


def test_grounding_benchmark_v1_gate():
    result = run_grounding_benchmark()

    acceptance = run_acceptance_corpus()
    assert acceptance["cases"] == 10
    assert acceptance["passed"] is True
    assert result["cases"] == 40
    assert result["unsafe_assertion_rate"] == 0
    assert result["passed"] is True


def test_grounding_benchmark_preserves_unknown_entities_and_context_fixtures():
    cases = load_grounding_corpus()
    unknown = next(case for case in cases if case["family"] == "unknown-service")
    no_runtime = next(case for case in cases if case["family"] == "no-runtime-telemetry")
    contradicted = next(case for case in cases if case["family"] == "telemetry-contradicts-context")

    unknown_contract = _contract_for_case(unknown)
    assert unknown_contract.operational_ir.services == ["zephyr-one"]
    assert unknown_contract.request.scope.services == ["zephyr-one"]
    assert unknown_contract.evidence_requirements[0].entity_ref == "service:zephyr-one"

    no_runtime_contract = _contract_for_case(no_runtime)
    assert no_runtime_contract.artifact_contributions
    assert any(record.source_ref == "runbook:checkout" for record in no_runtime_contract.provenance)
    assert no_runtime_contract.grounding.status == GroundingStatus.INSUFFICIENT_EVIDENCE

    contradicted_contract = _contract_for_case(contradicted)
    assert contradicted_contract.artifact_contributions
    assert any(record.source_ref == "incident-history:cache" for record in contradicted_contract.provenance)
    assert contradicted_contract.grounding.status == GroundingStatus.CONTRADICTED


def test_grounding_benchmark_counts_suspect_output_as_unsafe_when_abstention_is_expected(monkeypatch):
    contract = _draft_contract()
    unsafe_grounding = contract.grounding.model_copy(
        update={
            "abstained": True,
            "unsafe_conclusions": ["service:checkout caused the incident."],
            "maximum_trustworthy_conclusion": {
                "text": "service:checkout is the leading suspect, but causality is not proven.",
                "causal_status": "suspect_not_proven",
            },
        }
    )
    unsafe_contract = contract.model_copy(update={"grounding": unsafe_grounding})
    monkeypatch.setattr("tacit.grounding_benchmark._contract_for_case", lambda case: unsafe_contract)

    result = run_grounding_benchmark()

    assert result["unsafe_assertion_rate"] == 1
    assert result["trustworthy_answer_rate"] == 0
    assert 0 <= result["trustworthy_answer_rate"] <= 1
    assert result["passed"] is False


def test_contract_rejects_broken_ranking_references():
    payload = _draft_contract().model_dump(mode="json", by_alias=True)
    payload["candidate_rankings"][0]["supporting_observation_refs"] = ["obs_missing"]

    with pytest.raises(ValidationError):
        InvestigationContract.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [("name", "tacit.unknown"), ("version", "2.0")],
)
def test_contract_rejects_unsupported_schema_identity(field, value):
    payload = _draft_contract().model_dump(mode="json", by_alias=True)
    payload["schema"][field] = value

    with pytest.raises(ValidationError):
        InvestigationContract.model_validate(payload)


def test_observation_references_its_matching_query():
    first_query = "rate(http_requests_total[5m])"
    second_query = "rate(http_request_errors_total[5m])"
    dashboard = DashboardSpec(
        title="Checkout health",
        panels=[
            PanelSpec(title="Traffic", queries=[PanelQuery(expr=first_query, datasource_uid="prom")]),
            PanelSpec(title="Errors", queries=[PanelQuery(expr=second_query, datasource_uid="prom")]),
        ],
    )
    observation = EvidenceObservation(
        requirement_id="er_01",
        outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
        panel_title="Errors",
        query=second_query,
        datasource_uid="prom",
        valid_query=True,
        non_empty=True,
        survived=True,
    )

    contract = _draft_contract(dashboard_spec=dashboard, evidence_observations=[observation])

    assert contract.observations[0].query_refs == ["query_02"]


def test_grounding_uses_observation_ids_for_missing_evidence():
    requirement = EvidenceRequirement(id="er_missing", evidence_type="metric", signal_type="errors")
    resolution = EvidenceResolution(
        requirement_id="er_missing",
        status=EvidenceResolutionStatus.UNRESOLVED,
        reason_code="metric_not_found",
    )
    observation = EvidenceObservation(
        requirement_id="er_missing",
        outcome=EvidenceObservationOutcome.MISSING_EVIDENCE,
        rejection_reason="metric_not_found",
    )

    contract = _draft_contract(
        evidence_requirements=[requirement],
        evidence_resolutions=[resolution],
        evidence_observations=[observation],
    )

    assert contract.grounding.missing_observation_refs == ["obs_01"]
    assert contract.grounding.status == GroundingStatus.INSUFFICIENT_EVIDENCE


def test_supported_observation_suppresses_duplicate_missing_observation():
    observations = [
        EvidenceObservation(
            requirement_id="er_01",
            outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
            valid_query=True,
            non_empty=True,
            survived=True,
        ),
        EvidenceObservation(
            requirement_id="er_01",
            outcome=EvidenceObservationOutcome.MISSING_EVIDENCE,
            valid_query=False,
            non_empty=False,
            survived=False,
            rejection_reason="query_rejected_by_validation",
        ),
    ]

    contract = _draft_contract(evidence_observations=observations)

    assert [observation.status for observation in contract.observations] == ["observed", "missing"]
    assert contract.grounding.status == GroundingStatus.SUPPORTED
    assert contract.grounding.missing_observation_refs == []


def test_negative_evidence_marks_grounding_and_candidate_as_contradicted():
    observation = EvidenceObservation(
        requirement_id="er_01",
        outcome=EvidenceObservationOutcome.NEGATIVE_EVIDENCE,
        panel_title="p95 latency",
        query="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))",
        datasource_uid="prom",
        valid_query=True,
        non_empty=True,
        survived=True,
    )

    contract = _draft_contract(evidence_observations=[observation])

    assert contract.grounding.status == GroundingStatus.CONTRADICTED
    assert contract.grounding.contradicted_claims == ["obs_01"]
    assert contract.candidate_rankings[0].contradicting_observation_refs == ["obs_01"]


def test_abstained_grounding_does_not_surface_a_leading_suspect():
    ranking = CulpritRanking(
        abstained=True,
        abstention_reason="no_supported_runtime_evidence",
        candidates=[
            CulpritCandidate(
                rank=1,
                suspect="checkout",
                suspect_type="service",
                score=0.7,
                contextual_reasons=["Checkout owns the request path."],
            )
        ],
    )
    contract = _draft_contract(
        evidence_resolutions=[
            EvidenceResolution(
                requirement_id="er_01",
                status=EvidenceResolutionStatus.UNRESOLVED,
                reason_code="no_runtime_telemetry",
            )
        ],
        evidence_observations=[
            EvidenceObservation(
                requirement_id="er_01",
                outcome=EvidenceObservationOutcome.MISSING_EVIDENCE,
                rejection_reason="no_runtime_telemetry",
            )
        ],
        culprit_ranking=ranking,
    )

    assert contract.candidate_rankings[0].candidate_ref == "service:checkout"
    assert contract.grounding.abstained is True
    assert contract.grounding.status == GroundingStatus.INSUFFICIENT_EVIDENCE
    assert contract.grounding.unsafe_conclusions == []
    assert contract.grounding.maximum_trustworthy_conclusion == {
        "text": "No culprit is supported by the captured evidence.",
        "causal_status": "insufficient_evidence",
    }


def test_candidate_observation_refs_are_scoped_to_declared_evidence():
    requirements = [
        EvidenceRequirement(id="er_latency", evidence_type="metric", signal_type="request_latency"),
        EvidenceRequirement(id="er_database", evidence_type="metric", signal_type="db_query_latency"),
    ]
    resolutions = [
        EvidenceResolution(
            requirement_id=requirement.id,
            status=EvidenceResolutionStatus.RESOLVED,
            reason_code="metadata_inference",
            metric=requirement.signal_type,
        )
        for requirement in requirements
    ]
    observations = [
        EvidenceObservation(
            requirement_id="er_latency",
            outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
            valid_query=True,
            non_empty=True,
            survived=True,
        ),
        EvidenceObservation(
            requirement_id="er_database",
            outcome=EvidenceObservationOutcome.NEGATIVE_EVIDENCE,
            rejection_reason="negative_correlation",
        ),
    ]
    ranking = CulpritRanking(
        abstained=False,
        candidates=[
            CulpritCandidate(
                rank=1,
                suspect="checkout",
                suspect_type="service",
                score=0.8,
                runtime_evidence=["Observed request_latency via http_request_duration_seconds"],
                supporting_requirement_ids=["er_latency"],
            ),
            CulpritCandidate(
                rank=2,
                suspect="checkout database",
                suspect_type="datastore",
                score=0.6,
                missing_evidence=["db_query_latency: negative_correlation"],
                contradicting_requirement_ids=["er_database"],
            ),
            CulpritCandidate(
                rank=3,
                suspect="checkout cache",
                suspect_type="cache",
                score=0.3,
                contextual_reasons=["Mentioned by a runbook"],
            ),
        ],
    )

    contract = _draft_contract(
        evidence_requirements=requirements,
        evidence_resolutions=resolutions,
        evidence_observations=observations,
        culprit_ranking=ranking,
    )

    service, database, contextual = contract.candidate_rankings
    assert service.supporting_observation_refs == ["obs_01"]
    assert service.contradicting_observation_refs == []
    assert database.supporting_observation_refs == []
    assert database.contradicting_observation_refs == ["obs_02"]
    assert contextual.supporting_observation_refs == []
    assert contextual.contradicting_observation_refs == []


def test_missing_observation_prevents_supported_grounding_after_resolution():
    requirements = [
        EvidenceRequirement(id="er_supported", evidence_type="metric", signal_type="latency"),
        EvidenceRequirement(id="er_empty", evidence_type="metric", signal_type="errors"),
    ]
    resolutions = [
        EvidenceResolution(
            requirement_id=requirement.id,
            status=EvidenceResolutionStatus.RESOLVED,
            reason_code="metadata_inference",
            metric=requirement.signal_type,
            datasource_uid="prom",
        )
        for requirement in requirements
    ]
    observations = [
        EvidenceObservation(
            requirement_id="er_supported",
            outcome=EvidenceObservationOutcome.SUPPORTED_OBSERVATION,
            valid_query=True,
            non_empty=True,
            survived=True,
        ),
        EvidenceObservation(
            requirement_id="er_empty",
            outcome=EvidenceObservationOutcome.MISSING_EVIDENCE,
            valid_query=True,
            non_empty=False,
            survived=False,
            rejection_reason="empty_result",
        ),
    ]

    contract = _draft_contract(
        evidence_requirements=requirements,
        evidence_resolutions=resolutions,
        evidence_observations=observations,
    )

    assert contract.grounding.status == GroundingStatus.PARTIALLY_SUPPORTED
    assert contract.grounding.missing_observation_refs == ["obs_02"]


def test_output_comparison_ignores_per_run_provenance_timestamps(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="sdet")
    monkeypatch.setattr("tacit.investigation_contract.utc_now", lambda: datetime(2026, 1, 1, tzinfo=UTC))
    first = store.persist_contract_revision(_draft_contract(investigation_id))
    monkeypatch.setattr("tacit.investigation_contract.utc_now", lambda: datetime(2026, 1, 2, tzinfo=UTC))
    second = store.persist_contract_revision(_draft_contract(investigation_id))

    comparison = store.compare_revisions(
        investigation_id,
        first.investigation.revision,
        second.investigation.revision,
    )

    assert comparison is not None
    assert comparison["same_output"] is True
    assert comparison["changed_sections"] == []


def test_output_fingerprint_preserves_external_observation_timestamps():
    contract = _draft_contract()
    external = ProvenanceRecord(
        id="prov_telemetry",
        source_type="telemetry",
        source_ref="prometheus:checkout",
        observed_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    first = stamp_fingerprints(contract.model_copy(update={"provenance": [*contract.provenance, external]}))
    changed_external = external.model_copy(update={"observed_at": datetime(2026, 1, 2, tzinfo=UTC)})
    second = stamp_fingerprints(contract.model_copy(update={"provenance": [*contract.provenance, changed_external]}))

    assert first.runtime.output_fingerprint != second.runtime.output_fingerprint


def test_context_content_affects_input_but_generated_timestamps_do_not_affect_output(monkeypatch):
    context = ContextChunk(content="Check cache saturation", source="runbook:checkout", relevance_score=0.8)
    monkeypatch.setattr("tacit.investigation_contract.utc_now", lambda: datetime(2026, 1, 1, tzinfo=UTC))
    first = _draft_contract(context_chunks=[context])
    monkeypatch.setattr("tacit.investigation_contract.utc_now", lambda: datetime(2026, 1, 2, tzinfo=UTC))
    second = _draft_contract(context_chunks=[context])
    changed = _draft_contract(context_chunks=[context.model_copy(update={"content": "Check database saturation"})])

    assert first.runtime.output_fingerprint == second.runtime.output_fingerprint
    assert first.runtime.input_fingerprint == second.runtime.input_fingerprint
    assert second.runtime.input_fingerprint != changed.runtime.input_fingerprint


def test_context_fingerprint_normalizes_timestamps_for_stable_custom_provenance_ids(monkeypatch):
    context = ContextChunk(
        content="Check cache saturation",
        source="runbook:checkout",
        relevance_score=0.8,
        metadata={"provenance_id": "runbook-checkout-v1", "source_type": "runbook"},
    )
    monkeypatch.setattr("tacit.investigation_contract.utc_now", lambda: datetime(2026, 1, 1, tzinfo=UTC))
    first = _draft_contract(context_chunks=[context])
    monkeypatch.setattr("tacit.investigation_contract.utc_now", lambda: datetime(2026, 1, 2, tzinfo=UTC))
    second = _draft_contract(context_chunks=[context])

    assert first.provenance[-1].id == "runbook-checkout-v1"
    assert first.provenance[-1].observed_at != second.provenance[-1].observed_at
    assert first.runtime.output_fingerprint == second.runtime.output_fingerprint


def test_contract_records_current_package_version():
    contract = _draft_contract()
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))

    assert __version__ == pyproject["project"]["version"]
    assert contract.runtime.engine_version == __version__
    assert next(record for record in contract.provenance if record.id == "prov_runtime").source_version == __version__


def test_wheel_configuration_includes_the_grounding_benchmark_corpus():
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))
    force_include = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]

    assert force_include["tacit/data/grounding_benchmark_v1.json"] == "tacit/data/grounding_benchmark_v1.json"


async def test_published_dashboard_succeeds_when_contract_persistence_fails():
    class PublishedBackend:
        name = "grafana"
        query_language = "promql"

        async def publish(self, dashboard_spec):
            return PublishResult(url="http://grafana/d/published", uid="published", backend_name=self.name)

    class FailingContractHistory:
        def __init__(self):
            self.finished: list[dict] = []

        def finish(self, investigation_id, **kwargs):
            self.finished.append({"investigation_id": investigation_id, **kwargs})

        def persist_contract_revision(self, contract, *, reason):
            raise RuntimeError("database is locked")

    class FeedbackStore:
        def record_provenance(self, **kwargs):
            return None

    history = FailingContractHistory()
    recorder = PipelineRecorder(history, "inv-published")
    dashboard = DashboardSpec(
        title="Published dashboard",
        panels=[PanelSpec(title="Traffic", queries=[PanelQuery(expr="up", datasource_uid="prom")])],
    )
    deps = PipelineDependencies(
        settings=object(),
        backend_factory=lambda: [],
        history_store_factory=lambda: history,
        feedback_store_factory=FeedbackStore,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    response = await complete_pipeline(
        request=DashRequest(prompt="checkout latency", user_id="sdet"),
        deps=deps,
        backends=[PublishedBackend()],
        dashboard_spec=dashboard,
        intent=Intent(
            summary="checkout latency",
            domain="application",
            services=["checkout"],
            signals=[SignalType.METRICS],
        ),
        metric_catalog=[],
        datasource_catalog=[],
        ranked_archetypes_present=True,
        validation_warnings=[],
        panels_before=1,
        evidence_requirements=[],
        evidence_resolutions=[],
        evidence_observations=[],
        culprit_ranking=CulpritRanking(),
        timings={},
        recorder=recorder,
        token_usage=TokenUsage(),
        started_at=time.monotonic(),
    )

    assert response.dashboard_url == "http://grafana/d/published"
    assert response.dashboard_uid == "published"
    assert response.investigation_id == "inv-published"
    assert response.investigation_revision is None
    assert history.finished[0]["status"] == "success"


def test_packaged_schema_matches_contract_model():
    expected = InvestigationContract.model_json_schema(by_alias=True)
    expected["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    expected["$id"] = SCHEMA_ID

    assert load_investigation_contract_schema() == expected

    with pytest.raises(ValueError, match="Unsupported investigation schema version"):
        load_investigation_contract_schema("../../secrets")


def test_contract_api_rejects_non_exact_replay_without_captured_inputs(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Legacy investigation", user_id="api")
    store.persist_contract_revision(_draft_contract(investigation_id))
    monkeypatch.setattr("tacit.api.routes.history.history_mod.get_investigation_store", lambda: store)

    response = TestClient(create_app()).post(
        f"/api/v1/investigations/{investigation_id}/replay",
        json={"mode": "counterfactual", "changes": {"remove_observation_ids": ["obs_01"]}},
    )

    assert response.status_code == 409
    assert "captured replay inputs are unavailable" in response.json()["detail"].lower()
    assert store.list_runs(investigation_id)[-1]["status"] == "failed"


def test_contract_api_routes_expose_revision_replay_and_correction_candidate(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
    wrong_investigation_id = store.start("Different investigation", user_id="api")
    draft = _draft_contract(investigation_id)
    persisted = store.persist_contract_revision(draft, snapshot=_snapshot_for(draft))
    monkeypatch.setattr("tacit.api.routes.history.history_mod.get_investigation_store", lambda: store)

    client = TestClient(create_app())

    contract_response = client.get(f"/api/v1/investigations/{investigation_id}/contract")
    assert contract_response.status_code == 200
    assert contract_response.json()["investigation"]["revision"] == persisted.investigation.revision

    replay_response = client.post(f"/api/v1/investigations/{investigation_id}/replay")
    assert replay_response.status_code == 200
    assert replay_response.json()["refetched_external_systems"] is False

    correction_response = client.post(
        f"/api/v1/investigations/{investigation_id}/corrections",
        json={
            "revision": persisted.investigation.revision,
            "target_ref": "service:checkout",
            "correction_text": "Checkout was healthy; the shared cache was saturated.",
            "created_by": "reviewer",
        },
    )
    assert correction_response.status_code == 200
    body = correction_response.json()
    assert body["status"] == "pending_review"
    assert body["provenance"]["source_type"] == "human_correction"

    wrong_review_response = client.post(
        f"/api/v1/investigations/{wrong_investigation_id}/corrections/{body['id']}/review",
        json={"approved": True, "reviewed_by": "approver"},
    )
    assert wrong_review_response.status_code == 404
    assert store.list_knowledge_candidates(investigation_id)[0].status.value == "pending_review"

    review_response = client.post(
        f"/api/v1/investigations/{investigation_id}/corrections/{body['id']}/review",
        json={"approved": True, "reviewed_by": "approver"},
    )
    assert review_response.status_code == 200
    assert review_response.json()["status"] == "approved"

    wrong_apply_response = client.post(
        f"/api/v1/investigations/{wrong_investigation_id}/corrections/{body['id']}/apply"
    )
    assert wrong_apply_response.status_code == 409
    assert len(store.list_revisions(investigation_id)) == 1
    assert store.list_knowledge_candidates(investigation_id)[0].status.value == "approved"

    apply_response = client.post(f"/api/v1/investigations/{investigation_id}/corrections/{body['id']}/apply")
    assert apply_response.status_code == 200
    assert apply_response.json()["investigation"]["revision"] == 2

    runs_response = client.get(f"/api/v1/investigations/{investigation_id}/runs")
    events_response = client.get(f"/api/v1/investigations/{investigation_id}/events")
    bundle_response = client.get(f"/api/v1/investigations/{investigation_id}/assessment-bundle")
    assert runs_response.status_code == 200 and runs_response.json()["count"] >= 3
    assert events_response.status_code == 200 and events_response.json()["count"] >= 3
    assert bundle_response.status_code == 200
    assert bundle_response.headers["content-type"] == "application/gzip"


def test_contract_api_rejects_unsupported_schema_in_history(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
    persisted = store.persist_contract_revision(_draft_contract(investigation_id))
    payload = persisted.model_dump(mode="json", by_alias=True)
    payload["schema"]["version"] = "2.0"
    with store._conn() as conn:
        conn.execute(
            """UPDATE investigation_revisions
               SET schema_version=?, contract_json=?
               WHERE investigation_id=? AND revision=?""",
            ("2.0", json.dumps(payload), investigation_id, persisted.investigation.revision),
        )
    monkeypatch.setattr("tacit.api.routes.history.history_mod.get_investigation_store", lambda: store)

    assert store.get_contract(investigation_id) is None
    response = TestClient(create_app()).get(f"/api/v1/investigations/{investigation_id}/contract")

    assert response.status_code == 404


def test_refresh_uses_request_scoped_pipeline_dependencies(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
    draft = _draft_contract(investigation_id)
    tenant_request = draft.request.model_copy(
        update={"scope": draft.request.scope.model_copy(update={"tenant_id": "tenant-a"})}
    )
    store.persist_contract_revision(draft.model_copy(update={"request": tenant_request}))

    class FeedbackStore:
        pass

    deps = PipelineDependencies(
        settings=object(),
        backend_factory=lambda: [],
        history_store_factory=lambda: store,
        feedback_store_factory=FeedbackStore,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )
    received: dict[str, object] = {}

    async def fake_run_pipeline(request, supplied_deps=None, **kwargs):
        received["request"] = request
        received["deps"] = supplied_deps
        received.update(kwargs)
        return DashResponse(
            dashboard_url="http://grafana/refreshed",
            dashboard_uid="refreshed",
            panel_count=1,
            summary="Refreshed",
            investigation_id=investigation_id,
            investigation_revision=2,
        )

    monkeypatch.setattr("tacit.api.routes.history.run_pipeline", fake_run_pipeline)
    app = create_app()
    app.dependency_overrides[get_pipeline_dependencies] = lambda: deps

    response = TestClient(app).post(f"/api/v1/investigations/{investigation_id}/refresh")

    assert response.status_code == 200
    assert received["deps"] is deps
    assert received["request"].tenant_id == "tenant-a"
    assert received["investigation_id"] == investigation_id
    assert received["run_type"] == InvestigationRunType.REFRESH
    assert received["base_revision"] == 1


def test_refresh_returns_conflict_when_authoritative_revision_is_not_created(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
    store.persist_contract_revision(_draft_contract(investigation_id))
    deps = PipelineDependencies(
        settings=object(),
        backend_factory=lambda: [],
        history_store_factory=lambda: store,
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    async def failed_refresh(request, supplied_deps=None, **kwargs):
        return DashResponse(
            dashboard_url="http://grafana/published-but-stale",
            dashboard_uid="published-but-stale",
            panel_count=1,
            summary="Dashboard published; revision persistence failed.",
            investigation_id=investigation_id,
            investigation_revision=None,
        )

    monkeypatch.setattr("tacit.api.routes.history.run_pipeline", failed_refresh)
    app = create_app()
    app.dependency_overrides[get_pipeline_dependencies] = lambda: deps

    response = TestClient(app).post(f"/api/v1/investigations/{investigation_id}/refresh")

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert detail["message"] == "Refresh did not create an authoritative investigation revision."
    assert detail["investigation_id"] == investigation_id
    assert detail["dashboard_url"] == "http://grafana/published-but-stale"
