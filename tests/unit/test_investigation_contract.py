from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from tacit.api.app import create_app
from tacit.history import InvestigationStore
from tacit.investigation_contract import (
    SCHEMA_ID,
    InvestigationContract,
    InvestigationContractAssembler,
    load_investigation_contract_schema,
)
from tacit.models.schemas import (
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


def _draft_contract(investigation_id: str = "inv_contract_test") -> InvestigationContract:
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
                )
            ],
        ),
        dashboard_url="http://grafana/d/checkout",
        dashboard_uid="checkout",
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


def test_contract_rejects_broken_ranking_references():
    payload = _draft_contract().model_dump(mode="json", by_alias=True)
    payload["candidate_rankings"][0]["supporting_observation_refs"] = ["obs_missing"]

    with pytest.raises(ValidationError):
        InvestigationContract.model_validate(payload)


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
    persisted = store.persist_contract_revision(_draft_contract(investigation_id))
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
