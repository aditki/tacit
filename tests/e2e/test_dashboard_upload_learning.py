from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import tacit.archetypes.templates as templates
import tacit.pipeline as pipeline_mod
from tacit.agents.providers import registry as provider_registry
from tacit.agents.providers.base import TokenUsage
from tacit.main import app
from tacit.models.schemas import ArchetypeMatch, DashRequest, Intent, MetricEntry, SignalType
from tests.e2e.framework import (
    CapturingBackend,
    IncidentFixtureProvider,
    build_grafana_dashboard,
    evaluate_incident,
    incident_cases,
    intent_from_prompt,
    load_scenario,
    scenario_catalog,
)

SCENARIO_PATH = Path(__file__).parent / "scenarios" / "checkout_upload_incident.yaml"


async def _no_context(_intent):
    return []


@pytest.mark.e2e
async def test_uploaded_dashboard_teaches_signals_and_prompt_matrix_scores_useful(
    isolated_learning_runtime,
    monkeypatch,
):
    signal_store, history_store, _feedback_store, _archetypes_path, quarantine_path = isolated_learning_runtime
    scenario = load_scenario(SCENARIO_PATH)
    dashboard_json = build_grafana_dashboard(scenario)
    client = TestClient(app)

    upload = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "grafana",
            "source_name": "checkout-edge-incident.json",
            "auto_approve": False,
            "dashboard": dashboard_json,
        },
    )
    assert upload.status_code == 200, upload.text
    uploaded = upload.json()
    assert uploaded["status"] == "pending"
    assert uploaded["dashboard_uid"] == scenario["dashboard"]["uid"]
    assert set(uploaded["metrics_found"]) >= {
        "checkout_edge_requests_total",
        "checkout_edge_errors_total",
        "checkout_edge_request_duration_seconds_bucket",
    }

    inferred_signals = uploaded["signals_inferred"]
    inferred_metrics = {sig["metric"] for sig in inferred_signals}
    inferred_families = {sig["signal_family"] for sig in inferred_signals}
    assert {
        "checkout_edge_requests_total",
        "checkout_edge_errors_total",
        "checkout_edge_request_duration_seconds_bucket",
        "checkout_edge_inflight_requests",
        "checkout_edge_cpu_usage_seconds_total",
        "checkout_edge_memory_working_set_bytes",
    } <= inferred_metrics
    assert {"errors", "latency", "saturation"} <= inferred_families
    assert inferred_families & {"traffic", "throughput"}

    approve = client.post(f"/api/v1/learn/dashboards/{scenario['dashboard']['uid']}/approve?backend=grafana_json")
    assert approve.status_code == 200, approve.text
    approved = approve.json()
    assert approved["status"] == "approved"
    assert approved["mappings_created"] >= 5
    assert approved["archetype_registered"] is False
    assert approved["archetype_quarantined"] is True
    assert len(list(quarantine_path.rglob("*.yaml"))) == 1

    learned_sources = signal_store.stats()["mappings_by_source"]
    assert learned_sources.get("dashboard_ingest", 0) >= 5

    catalog = scenario_catalog(scenario)
    backend = CapturingBackend(catalog=catalog)
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    current_service = scenario["prompt_matrix"]["services"][0]
    resolved_metrics = {
        entry.name
        for signal_type in {sig["signal_type"] for sig in inferred_signals}
        for entry, _confidence in signal_store.resolve_signal(
            signal_type,
            catalog,
            context_service=current_service,
            target_query_language="promql",
        )
    }
    assert resolved_metrics
    assert resolved_metrics <= inferred_metrics
    fixture_provider = IncidentFixtureProvider(catalog, resolved_metrics, service=current_service)
    monkeypatch.setattr(provider_registry, "create_provider", lambda _settings: fixture_provider)

    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return intent_from_prompt(prompt, service=current_service), TokenUsage()

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    thresholds = scenario["utility_thresholds"]
    cases = incident_cases(scenario)
    evaluations = []
    for case in cases:
        response = await pipeline_mod.run_pipeline(
            DashRequest(prompt=case.prompt, user_id="e2e", channel_id="dashboard-upload-learning")
        )
        assert response.dashboard_uid, case.case_id
        contract = history_store.get_contract(response.investigation_id)
        assert contract is not None, case.case_id
        assert "checkout_edge_incident_response" not in contract.model_dump_json(), case.case_id
        assert backend.published_specs, case.case_id
        spec = backend.published_specs[-1]
        assert "Checkout Edge Incident Response" not in spec.title
        approved_case = replace(
            case,
            expected_metrics=[metric for metric in case.expected_metrics if metric in resolved_metrics],
            critical_metrics=[metric for metric in case.critical_metrics if metric in resolved_metrics],
        )
        assert approved_case.expected_metrics, case.case_id
        evaluation = evaluate_incident(spec, approved_case)
        evaluation.assert_passes(thresholds, case_id=case.case_id)
        evaluations.append(evaluation)

    assert len(evaluations) == 27
    assert sum(e.usefulness_score for e in evaluations) / len(evaluations) >= thresholds["min_usefulness_score"]


@pytest.mark.e2e
async def test_manual_teach_signal_mapping_is_used_before_dashboard_creation(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, _history_store, _feedback_store, archetypes_path, _quarantine_path = isolated_learning_runtime
    archetypes_path.write_text(
        """
archetypes:
  - id: taught_latency
    name: Taught Latency
    description: Uses manually taught latency mappings
    problem_types: [latency_investigation]
    required_metrics:
      - http_request_duration_seconds
    required_signals:
      - request_latency
    signal_bindings:
      request_latency: http_request_duration_seconds
    tags: [manual-teach, latency]
    default_timerange: 1h
    panels:
      - title: Taught p95 latency
        row: Latency
        unit: s
        queries:
          - expr: >
              histogram_quantile(
                0.95,
                sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le)
              )
            legend_format: p95
""",
        encoding="utf-8",
    )
    templates.reload_archetypes()

    client = TestClient(app)
    teach = client.post(
        "/api/v1/signals/teach",
        json={
            "signal_type": "request_latency",
            "metric_patterns": [{"pattern": "acme_checkout_latency_seconds", "confidence": 0.94}],
            "category": "latency",
            "datasource_types": ["prometheus"],
            "taught_by": "e2e",
        },
    )
    assert teach.status_code == 200, teach.text
    assert teach.json()["mappings_created"] == 1

    backend = CapturingBackend(
        catalog=[
            MetricEntry(
                name="acme_checkout_latency_seconds_bucket",
                datasource_uid="prom-e2e",
                datasource_name="Prometheus E2E",
                datasource_type="prometheus",
                query_language="promql",
                dimensions=['service="checkout-api"', "le={0.1,0.5,1,5}"],
            )
        ]
    )
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["checkout-api"],
                signals=[SignalType.METRICS],
                keywords=["checkout", "latency", "p95"],
                timerange="1h",
                problem_type="latency_investigation",
                archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.97)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    response = await pipeline_mod.run_pipeline(
        DashRequest(prompt="checkout-api p95 latency is high", user_id="e2e", channel_id="manual-teach")
    )

    assert response.dashboard_uid
    assert backend.published_specs
    found = {query.expr for panel in backend.published_specs[-1].panels for query in panel.queries}
    assert any("acme_checkout_latency_seconds_bucket" in expr for expr in found)
    assert all("http_request_duration_seconds" not in expr for expr in found)


@pytest.mark.e2e
def test_reject_uploaded_dashboard_records_negative_candidates_without_teaching(
    isolated_learning_runtime,
):
    signal_store, _history_store, _feedback_store, _archetypes_path, _quarantine_path = isolated_learning_runtime
    scenario = load_scenario(SCENARIO_PATH)
    client = TestClient(app)

    upload = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "grafana",
            "source_name": "checkout-edge-incident.json",
            "auto_approve": False,
            "dashboard": build_grafana_dashboard(scenario),
        },
    )
    assert upload.status_code == 200, upload.text

    reject = client.post(f"/api/v1/learn/dashboards/{scenario['dashboard']['uid']}/reject?backend=grafana_json")
    assert reject.status_code == 200, reject.text
    rejected = reject.json()
    assert rejected["status"] == "rejected"
    assert rejected["rejected_candidates"] >= 1

    stored = signal_store.get_ingested_dashboard(scenario["dashboard"]["uid"], backend_name="grafana_json")
    assert stored["status"] == "rejected"
    assert signal_store.stats()["mappings_by_source"].get("dashboard_ingest", 0) == 0
    rejected_metrics = {candidate["metric"] for candidate in signal_store.list_rejected_candidates()}
    assert (
        "checkout_edge_inflight_requests" in rejected_metrics
        or "checkout_edge_db_pool_wait_seconds_bucket" in rejected_metrics
    )


@pytest.mark.e2e
async def test_pipeline_returns_helpful_no_metrics_response_without_publishing(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, _history_store, _feedback_store, _archetypes_path, _quarantine_path = isolated_learning_runtime
    backend = CapturingBackend(catalog=[])
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["missing-service"],
                signals=[SignalType.METRICS],
                keywords=["missing", "latency"],
                timerange="1h",
                problem_type="latency_investigation",
                archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    response = await pipeline_mod.run_pipeline(
        DashRequest(prompt="missing-service is slow", user_id="e2e", channel_id="empty-telemetry")
    )

    assert response.dashboard_uid == ""
    assert response.dashboard_url == ""
    assert response.panel_count == 0
    assert "No metrics found" in response.summary
    assert backend.published_specs == []
