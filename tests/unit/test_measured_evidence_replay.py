"""Numeric capture must preserve contract identity and historical replay."""

from __future__ import annotations

import socket
import sqlite3
import subprocess
from copy import deepcopy

import pytest

import tacit.investigation_contract as contracts
from tacit.catalog import MetricCatalogResult, build_acquisition_capture
from tacit.history import InvestigationStore
from tacit.investigation_contract import InvestigationContract, InvestigationContractAssembler
from tacit.investigation_replay import CounterfactualChanges, InvestigationReplaySnapshot, ReplayMode, rebuild_contract
from tacit.measurements import QueryMeasurement, normalize_prometheus_measurement
from tacit.pipeline.completion import prepare_acquisition_completion
from tacit.signals.resolution import SignalResolutionWorkBudget
from tacit.tenancy import TenantBoundaryError
from tests.unit.test_investigation_contract import _draft_contract, _snapshot_for


def snapshot():
    return _snapshot_for(_draft_contract())


def measurement(snap, value="1.25", *, query=None):
    query = query or snap.dashboard_spec.panels[0].queries[0]
    budget = SignalResolutionWorkBudget(100, 10000, 1000)
    return normalize_prometheus_measurement(
        query,
        {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {"service": "checkout", "request_id": "secret-not-retained"},
                        "value": [1700000000, value],
                    }
                ],
            },
        },
        budget,
    )


def measured_snapshot(value="1.25"):
    snap = snapshot()
    return snap.model_copy(update={"query_measurements": (measurement(snap, value),)})


def assembly_inputs(snap):
    return dict(
        investigation_id=snap.investigation_id,
        revision=0,
        parent_revision=None,
        request=snap.request,
        intent=snap.intent,
        dashboard_spec=snap.dashboard_spec,
        evidence_requirements=snap.evidence_requirements,
        evidence_resolutions=snap.evidence_resolutions,
        evidence_observations=snap.evidence_observations,
        culprit_ranking=snap.culprit_ranking,
        context_chunks=snap.context_chunks,
        warnings=[],
        runtime_manifest=snap.runtime,
        query_measurements=list(snap.query_measurements),
    )


def test_historical_absence_keeps_exact_pre_measurement_fingerprints():
    rebuilt = rebuild_contract(snapshot())
    assert all("measurement" not in query.execution for query in rebuilt.queries)
    assert (
        rebuilt.runtime.input_fingerprint == "sha256:de1dcd88222175de94fbbbc5d4d54354c74076a240862a418b976b9096108406"
    )
    assert (
        rebuilt.runtime.output_fingerprint == "sha256:d71b9202b06144c3896ec6e1d3c1a7192e2d587f52cc2a56d0c05b5537627b1f"
    )


def test_real_numeric_fact_is_exposed_without_promoting_grounding_or_rank():
    legacy = rebuild_contract(snapshot())
    measured = rebuild_contract(measured_snapshot())
    fact = measured.queries[0].execution["measurement"]
    assert fact["disposition"] == "captured" and fact["samples"][0]["value"] == 1.25
    assert fact["samples"][0]["timestamp"] == 1700000000
    assert fact["unit"] is None and fact["evaluation_time"] is None
    assert "secret-not-retained" not in str(measured.model_dump())
    assert measured.grounding == legacy.grounding
    assert measured.candidate_rankings == legacy.candidate_rankings
    assert measured.observations[0].query_refs == [measured.queries[0].id]


def test_changed_value_changes_query_result_and_contract_fingerprints():
    first, second = (rebuild_contract(measured_snapshot(value)) for value in ("1.25", "9.5"))
    assert first.queries[0].execution["result_fingerprint"] != second.queries[0].execution["result_fingerprint"]
    assert first.runtime.input_fingerprint != second.runtime.input_fingerprint
    assert first.runtime.output_fingerprint != second.runtime.output_fingerprint


def test_exact_replay_of_serialized_measurements_uses_no_external_capability(monkeypatch):
    snap = measured_snapshot()
    expected = rebuild_contract(snap)
    payload = snap.model_dump_json()

    def denied(*args, **kwargs):
        raise AssertionError("Captured measurement replay attempted external work")

    monkeypatch.setattr(socket, "socket", denied)
    monkeypatch.setattr(sqlite3, "connect", denied)
    monkeypatch.setattr(subprocess, "Popen", denied)
    for _ in range(2):
        replayed = rebuild_contract(InvestigationReplaySnapshot.model_validate_json(payload))
        assert replayed.model_dump() == expected.model_dump()


def test_foreign_query_measurement_rejected_before_assembly():
    snap = snapshot()
    query = snap.dashboard_spec.panels[0].queries[0].model_copy(update={"datasource_uid": "foreign"})
    snap = snap.model_copy(update={"query_measurements": [measurement(snap, query=query)]})
    with pytest.raises(ValueError):
        rebuild_contract(snap)


@pytest.mark.parametrize("field,value", [("datasource_type", "mimir"), ("query_language", "logql"), ("expr", "up")])
def test_full_query_identity_is_required_for_measurement_binding(field, value):
    snap = snapshot()
    query = snap.dashboard_spec.panels[0].queries[0].model_copy(update={field: value})
    snap = snap.model_copy(update={"query_measurements": [measurement(snap, query=query)]})
    with pytest.raises(ValueError):
        rebuild_contract(snap)


def test_foreign_observation_cannot_borrow_measured_same_expression():
    snap = measured_snapshot()
    snap.evidence_observations[0].datasource_uid = "foreign"
    contract = rebuild_contract(snap)
    assert contract.observations[0].query_refs == []


@pytest.mark.parametrize(
    "changes",
    [
        CounterfactualChanges(remove_observation_ids=["obs_01"]),
        CounterfactualChanges(reject_requirement_ids=["er_01"]),
    ],
)
def test_counterfactual_removal_does_not_keep_unreferenced_measurement(changes):
    contract = rebuild_contract(measured_snapshot(), mode=ReplayMode.COUNTERFACTUAL, changes=changes)
    assert all("measurement" not in query.execution for query in contract.queries)
    assert contract.grounding.abstained is True


def test_counterfactual_duplicate_support_preserves_real_query_fact():
    snap = measured_snapshot()
    snap.evidence_observations.append(deepcopy(snap.evidence_observations[0]))
    contract = rebuild_contract(
        snap, mode=ReplayMode.COUNTERFACTUAL, changes=CounterfactualChanges(remove_observation_ids=["obs_01"])
    )
    assert contract.queries[0].execution["measurement"]["samples"][0]["value"] == 1.25


def test_private_prepublication_admission_pins_measurement_sidecar():
    snap = measured_snapshot()
    inputs = assembly_inputs(snap)
    prepared = prepare_acquisition_completion(
        assembly_inputs=inputs, acquisition_record=build_acquisition_capture(MetricCatalogResult())
    )
    inputs["query_measurements"].clear()
    exposed_copy = prepared.assembly_inputs
    exposed_copy["query_measurements"] = ()
    pinned = prepared.assembly_inputs
    contract = InvestigationContractAssembler().from_pipeline(**pinned, dashboard_url="", dashboard_uid="")
    assert contract.queries[0].execution["measurement"]["samples"][0]["value"] == 1.25


def test_store_revision_preserves_measurement_and_existing_tenant_boundary(tmp_path):
    snap = measured_snapshot()
    store = InvestigationStore(str(tmp_path / "history.db"))
    investigation_id = store.start(snap.request.prompt, user_id=snap.request.user_id)
    snap = snap.model_copy(update={"investigation_id": investigation_id})
    contract = rebuild_contract(snap)
    persisted = store.persist_contract_revision(contract, snapshot=snap)
    replayed = store.replay_contract(persisted.investigation.id)
    assert replayed.runtime.output_fingerprint == persisted.runtime.output_fingerprint
    assert replayed.queries[0].execution["measurement"]["samples"][0]["value"] == 1.25
    with pytest.raises(TenantBoundaryError):
        store.get_contract(persisted.investigation.id, tenant_id="another-tenant")


def test_contract_reload_admits_aggregate_before_any_typed_measurement_copy(monkeypatch):
    snap = snapshot()
    contract = rebuild_contract(snap)
    records = []
    for index in range(9):
        query = snap.dashboard_spec.panels[0].queries[0].model_copy(update={"expr": f"metric_{index}"})
        measured = normalize_prometheus_measurement(
            query,
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [
                        {"metric": {"instance": str(series)}, "value": [1700000000, "1"]} for series in range(64)
                    ],
                },
            },
            SignalResolutionWorkBudget(100, 10000, 1000),
        )
        records.append(
            contract.queries[0].model_copy(
                update={
                    "id": f"query_{index + 1:02d}",
                    "expression": query.expr,
                    "execution": {**contract.queries[0].execution, "measurement": measured.model_dump(mode="json")},
                }
            )
        )
    raw = contract.model_copy(update={"queries": records}).model_dump(mode="json", by_alias=True)

    def reject_early_copy(*args, **kwargs):
        raise AssertionError("Typed measurement copied before aggregate admission")

    monkeypatch.setattr(QueryMeasurement, "model_validate", reject_early_copy)
    with pytest.raises(ValueError, match="measurement aggregate limit"):
        InvestigationContract.model_validate(raw)


def test_repeated_query_measurement_fanout_is_admitted_before_materialization(monkeypatch):
    snap = measured_snapshot()
    query = snap.dashboard_spec.panels[0].queries[0]
    snap.dashboard_spec.panels[0].queries = [query] * 513

    def reject_query_copy(*args, **kwargs):
        raise AssertionError("Query record materialized before fanout admission")

    monkeypatch.setattr(contracts, "QueryRecord", reject_query_copy)
    with pytest.raises(ValueError, match="measurement aggregate limit"):
        rebuild_contract(snap)


def test_raw_contract_duplicate_facts_are_charged_for_every_physical_query_copy(monkeypatch):
    contract = rebuild_contract(measured_snapshot())
    raw = contract.model_dump(mode="json", by_alias=True)
    raw["queries"] = [{**raw["queries"][0], "id": f"query_{index:04d}"} for index in range(513)]

    def reject_early_copy(*args, **kwargs):
        raise AssertionError("Numeric record copied before physical fanout admission")

    monkeypatch.setattr(QueryMeasurement, "model_validate", reject_early_copy)
    with pytest.raises(ValueError, match="measurement aggregate limit"):
        InvestigationContract.model_validate(raw)
