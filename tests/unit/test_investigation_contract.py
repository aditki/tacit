from __future__ import annotations

import json
import tarfile
import time
import tomllib
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tacit import __version__
from tacit.agents.providers.base import TokenUsage
from tacit.api.app import create_app
from tacit.backends.base import PublishResult
from tacit.dependencies import PipelineDependencies
from tacit.grounding_benchmark import run_acceptance_corpus, run_grounding_benchmark
from tacit.history import InvestigationStore
from tacit.investigation_bundle import build_investigation_bundle
from tacit.investigation_contract import (
    SCHEMA_ID,
    GroundingStatus,
    InvestigationContract,
    InvestigationContractAssembler,
    ProvenanceRecord,
    load_investigation_contract_schema,
    stamp_fingerprints,
)
from tacit.investigation_replay import (
    CounterfactualChanges,
    InvestigationReplaySnapshot,
    ReplayMode,
)
from tacit.models.schemas import (
    ContextChunk,
    CulpritCandidate,
    CulpritRanking,
    DashboardSpec,
    DashRequest,
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
            remove_observation_ids=["obs_01"],
            add_context_chunks=[
                ContextChunk(content="New runbook evidence", source="runbook:new", relevance_score=0.7)
            ],
            candidate_score_overrides={"service:checkout": 0.4},
        ),
    )
    assert counterfactual is not None
    assert counterfactual.investigation.revision == 2
    assert counterfactual.observations == []
    assert counterfactual.candidate_rankings[0].score == 0.4
    assert counterfactual.artifact_contributions[0].artifact_ref == "runbook:new"


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
    reviewed = store.review_knowledge_candidate(candidate.id, approved=True, reviewed_by="approver")
    assert reviewed is not None and reviewed.status.value == "approved"

    corrected = store.apply_knowledge_candidate(candidate.id)

    assert corrected is not None
    assert corrected.investigation.revision == 2
    assert corrected.corrections[-1].correction_ref == candidate.id
    assert corrected.corrections[-1].applied_in_revision == 2
    assert corrected.provenance[-1].source_type == "human_correction"
    assert store.list_knowledge_candidates(investigation_id)[0].status.value == "applied"
    replayed = store.replay_contract(investigation_id, mode=ReplayMode.EXACT)
    assert replayed is not None
    assert replayed.runtime.output_fingerprint == corrected.runtime.output_fingerprint


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


def test_grounding_benchmark_v1_gate():
    result = run_grounding_benchmark()

    acceptance = run_acceptance_corpus()
    assert acceptance["cases"] == 10
    assert acceptance["passed"] is True
    assert result["cases"] == 40
    assert result["unsafe_assertion_rate"] == 0
    assert result["passed"] is True


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


def test_contract_records_current_package_version():
    contract = _draft_contract()
    pyproject = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))

    assert __version__ == pyproject["project"]["version"]
    assert contract.runtime.engine_version == __version__
    assert next(record for record in contract.provenance if record.id == "prov_runtime").source_version == __version__


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


def test_contract_api_routes_expose_revision_replay_and_correction_candidate(tmp_path, monkeypatch):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Why did checkout latency increase?", user_id="api")
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

    review_response = client.post(
        f"/api/v1/investigations/{investigation_id}/corrections/{body['id']}/review",
        json={"approved": True, "reviewed_by": "approver"},
    )
    assert review_response.status_code == 200
    assert review_response.json()["status"] == "approved"

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
