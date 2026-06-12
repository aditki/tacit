from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import dashforge.pipeline as pipeline_mod
from dashforge.agents.providers.base import TokenUsage
from dashforge.config import settings
from dashforge.main import app
from dashforge.models.schemas import ArchetypeMatch, Intent, MetricEntry, SignalType
from tests.e2e.framework import CapturingBackend, build_grafana_dashboard, load_scenario
from tests.e2e.test_dashboard_upload_learning import SCENARIO_PATH, _no_context


def _http_catalog() -> list[MetricEntry]:
    return [
        MetricEntry(
            name=name,
            datasource_uid="prom-e2e",
            datasource_name="Prometheus E2E",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=['service="checkout-service"', 'container="checkout-service"', "status={200,500}", "le={0.1,1}"],
        )
        for name in (
            "http_requests_total",
            "http_request_duration_seconds_bucket",
            "http_requests_in_flight",
            "container_cpu_usage_seconds_total",
            "container_memory_working_set_bytes",
        )
    ]


@pytest.mark.e2e
def test_system_archetype_signal_and_auth_endpoints(isolated_learning_runtime, monkeypatch):
    signal_store, _history_store, _feedback_store, _archetypes_path = isolated_learning_runtime
    client = TestClient(app)

    assert client.get("/healthz").json() == {"status": "ok"}

    archetypes = client.get("/api/v1/archetypes")
    assert archetypes.status_code == 200
    assert archetypes.json()["count"] > 0
    assert any(a["id"] == "latency_investigation" for a in archetypes.json()["archetypes"])

    reload_resp = client.post("/api/v1/archetypes/reload")
    assert reload_resp.status_code == 200
    assert reload_resp.json()["count"] == archetypes.json()["count"]

    stats = client.get("/api/v1/signals/stats")
    assert stats.status_code == 200
    assert stats.json()["signal_types"] > 0
    assert stats.json()["metric_mappings"] > 0

    signal = client.get("/api/v1/signals/request_latency")
    assert signal.status_code == 200
    assert signal.json()["signal_type"] == "request_latency"
    assert signal.json()["mappings"]

    missing = client.get("/api/v1/signals/not_a_real_signal")
    assert missing.status_code == 404

    invalid_teach = client.post(
        "/api/v1/signals/teach",
        json={"signal_type": "bad", "metric_patterns": [{"pattern": "x", "confidence": 2.0}]},
    )
    assert invalid_teach.status_code == 422

    monkeypatch.setattr(settings, "api_auth_enabled", True)
    monkeypatch.setattr(settings, "api_auth_key", "secret-e2e")
    assert client.get("/api/v1/signals").status_code == 401
    assert client.get("/api/v1/signals", headers={"X-API-Key": "wrong"}).status_code == 401
    assert client.get("/api/v1/signals", headers={"X-API-Key": "secret-e2e"}).status_code == 200

    # The authenticated request must still hit the isolated store, not process-global state.
    signal_store.register_signal_type("auth_probe")
    auth_signal_names = {
        item["signal_type"]
        for item in client.get("/api/v1/signals", headers={"X-API-Key": "secret-e2e"}).json()["signal_types"]
    }
    assert "auth_probe" in auth_signal_names


@pytest.mark.e2e
def test_chart_history_feedback_and_insights_endpoints(isolated_learning_runtime, monkeypatch):
    _signal_store, history_store, _feedback_store, _archetypes_path = isolated_learning_runtime
    client = TestClient(app)
    backend = CapturingBackend(catalog=_http_catalog())
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["checkout-service"],
                signals=[SignalType.METRICS],
                keywords=["checkout", "latency", "errors"],
                timerange="30m",
                problem_type="latency_investigation",
                archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.96)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    empty_prompt = client.post("/api/v1/chart", json={"prompt": "\x00\n", "user_id": "api-e2e"})
    assert empty_prompt.status_code == 400

    chart = client.post(
        "/api/v1/chart",
        json={"prompt": "checkout-service p95 latency is high\x00", "user_id": "api-e2e", "channel_id": "web"},
    )
    assert chart.status_code == 200, chart.text
    chart_body = chart.json()
    assert chart_body["dashboard_uid"] == "e2e-1"
    assert chart_body["panel_count"] > 0
    assert backend.published_specs

    investigations = client.get("/api/v1/investigations?user_id=api-e2e")
    assert investigations.status_code == 200
    assert investigations.json()["count"] == 1
    investigation = investigations.json()["investigations"][0]
    assert investigation["status"] == "success"
    assert "\x00" not in investigation["prompt"]

    detail = client.get(f"/api/v1/investigations/{investigation['id']}")
    assert detail.status_code == 200
    assert detail.json()["dashboard_uid"] == chart_body["dashboard_uid"]

    inv_stats = client.get("/api/v1/investigations/stats")
    assert inv_stats.status_code == 200
    assert inv_stats.json()["total"] >= 1
    assert history_store.get_by_dashboard(chart_body["dashboard_uid"]) is not None

    feedback = client.post(
        "/api/v1/feedback",
        json={
            "dashboard_uid": chart_body["dashboard_uid"],
            "symptom_visibility": 5,
            "root_cause_support": 4,
            "noise_level": 4,
            "investigation_speed": 5,
            "overall_useful": True,
            "comment": "Useful incident triage view",
            "reviewer": "e2e-reviewer",
        },
    )
    assert feedback.status_code == 200
    assert feedback.json()["dashboard_uid"] == chart_body["dashboard_uid"]

    feedback_detail = client.get(f"/api/v1/feedback/{chart_body['dashboard_uid']}")
    assert feedback_detail.status_code == 200
    assert feedback_detail.json()["provenance"]["dashboard_uid"] == chart_body["dashboard_uid"]
    assert feedback_detail.json()["feedback"][0]["reviewer"] == "e2e-reviewer"

    missing_feedback = client.get("/api/v1/feedback/not-found")
    assert missing_feedback.status_code == 404

    feedback_stats = client.get("/api/v1/feedback/stats")
    assert feedback_stats.status_code == 200
    assert feedback_stats.json()["total_feedback"] == 1
    assert feedback_stats.json()["useful_rate"] == 1.0

    analysis = client.get("/api/v1/feedback/analysis")
    assert analysis.status_code == 200
    assert analysis.json()["total_feedback"] == 1
    assert "recommendations" in analysis.json()


@pytest.mark.e2e
def test_learning_list_ignore_and_upload_validation_endpoints(isolated_learning_runtime):
    signal_store, _history_store, _feedback_store, _archetypes_path = isolated_learning_runtime
    scenario = load_scenario(SCENARIO_PATH)
    client = TestClient(app)

    unsupported = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "unknown",
            "source_name": "bad.json",
            "auto_approve": False,
            "dashboard": {"dashboard": {"title": "Bad", "panels": []}},
        },
    )
    assert unsupported.status_code == 400
    assert "Unsupported dashboard upload vendor" in unsupported.json()["detail"]

    bad_bool = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "grafana",
            "source_name": "bad-bool.json",
            "auto_approve": "yes",
            "dashboard": {"dashboard": {"title": "Bad Bool", "panels": []}},
        },
    )
    assert bad_bool.status_code == 422

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
    uid = upload.json()["dashboard_uid"]

    pending = client.get("/api/v1/learn/dashboards?status=pending")
    assert pending.status_code == 200
    assert pending.json()["count"] == 1
    assert pending.json()["dashboards"][0]["dashboard_uid"] == uid

    ignore = client.post(f"/api/v1/learn/dashboards/{uid}/ignore?backend=grafana_json")
    assert ignore.status_code == 200
    assert ignore.json()["status"] == "ignored"

    ignored = client.get("/api/v1/learn/dashboards?status=ignored")
    assert ignored.status_code == 200
    assert ignored.json()["count"] == 1
    assert ignored.json()["dashboards"][0]["status"] == "ignored"

    already_ignored = client.post(f"/api/v1/learn/dashboards/{uid}/approve?backend=grafana_json")
    assert already_ignored.status_code == 200
    assert already_ignored.json()["message"] == "Dashboard already ignored"

    missing_ignore = client.post("/api/v1/learn/dashboards/missing/ignore?backend=grafana_json")
    assert missing_ignore.status_code == 404
    assert signal_store.stats()["mappings_by_source"].get("dashboard_ingest", 0) == 0
    assert signal_store.list_rejected_candidates() == []
