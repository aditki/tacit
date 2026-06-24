"""Contract tests for Tacit's own FastAPI surface (GET + POST).

These exercise the request/response models end-to-end through the ASGI app with
the pipeline/store/ingest dependencies mocked, so the API contract (status
codes, validation, response shapes) is locked in.

Skipped automatically on Python < 3.12 because the app transitively imports
``tacit.agents.llm`` which uses 3.12 generic syntax.
"""

from __future__ import annotations

import sys

import pytest

if sys.version_info < (3, 12):  # pragma: no cover - env guard  # noqa: UP036
    pytest.skip("Tacit API contract requires Python 3.12", allow_module_level=True)

from fastapi.testclient import TestClient

import tacit.dashboard_ingest as di
import tacit.signals as signals_mod
from tacit.main import app
from tacit.signals import SignalStore


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "api_signals.db")
    monkeypatch.setattr(signals_mod, "get_signal_store", lambda: store)
    return store


def test_healthz_get(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] in ("ok", "healthy", "up")


def test_list_signals_get(client, temp_store):
    temp_store.register_signal_type("request_latency", description="Latency", category="latency")
    resp = client.get("/api/v1/signals")
    assert resp.status_code == 200
    types = [s["signal_type"] for s in resp.json()["signal_types"]]
    assert "request_latency" in types


def test_teach_post_valid(client, temp_store):
    resp = client.post(
        "/api/v1/signals/teach",
        json={
            "signal_type": "queue_depth",
            "metric_patterns": [{"pattern": "kafka_consumer_lag", "confidence": 0.9}],
            "category": "saturation",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["mappings_created"] == 1


def test_teach_post_rejects_bad_confidence(client, temp_store):
    resp = client.post(
        "/api/v1/signals/teach",
        json={"signal_type": "queue_depth", "metric_patterns": [{"pattern": "x", "confidence": 90}]},
    )
    assert resp.status_code == 422  # Pydantic bounds validation


def test_teach_post_rejects_unknown_field(client, temp_store):
    resp = client.post(
        "/api/v1/signals/teach",
        json={"signal_type": "q", "auto_aprove": True},  # typo'd extra field
    )
    assert resp.status_code == 422  # extra="forbid"


def test_learn_post_valid(client, temp_store, monkeypatch):
    async def fake_ingest(*, dashboard_uid, backend_name, auto_approve):
        return {"dashboard_uid": dashboard_uid, "status": "approved" if auto_approve else "pending"}

    monkeypatch.setattr(di, "ingest_dashboard", fake_ingest)
    resp = client.post("/api/v1/learn/dashboard", json={"dashboard_uid": "abc", "auto_approve": "false"})
    assert resp.status_code == 200
    # "false" string is correctly coerced to a boolean -> pending, not approved.
    assert resp.json()["status"] == "pending"


def test_reject_ingested_dashboard_marks_status_without_mappings(client, temp_store):
    temp_store.record_ingested_dashboard(
        "reject-me",
        backend_name="grafana",
        metrics_found=["custom_errors_total"],
        signals_inferred=[
            {
                "signal_type": "custom_errors",
                "metric": "custom_errors_total",
                "source": "heuristic",
                "signal_family": "errors",
                "score": 0.9,
                "margin": 0.4,
                "evidence": ["name contains error"],
                "inference_version": "test",
            }
        ],
        status="pending",
    )

    resp = client.post("/api/v1/learn/dashboards/reject-me/reject?backend=grafana")

    assert resp.status_code == 200
    assert resp.json()["status"] == "rejected"
    assert temp_store.get_ingested_dashboard("reject-me", backend_name="grafana")["status"] == "rejected"
    assert temp_store.get_mappings_for_signal("custom_errors", include_decayed=True) == []
    rejected = temp_store.list_rejected_candidates()
    assert len(rejected) == 1
    assert rejected[0]["why_not"] == "dashboard_rejected"


def test_ignore_ingested_dashboard_marks_status_quietly(client, temp_store):
    temp_store.record_ingested_dashboard(
        "ignore-me",
        backend_name="grafana",
        metrics_found=["custom_errors_total"],
        signals_inferred=[
            {
                "signal_type": "custom_errors",
                "metric": "custom_errors_total",
                "source": "heuristic",
                "signal_family": "errors",
            }
        ],
        status="pending",
    )

    resp = client.post("/api/v1/learn/dashboards/ignore-me/ignore?backend=grafana")

    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"
    assert temp_store.get_ingested_dashboard("ignore-me", backend_name="grafana")["status"] == "ignored"
    assert temp_store.list_rejected_candidates() == []


def test_learn_dashboard_json_post_valid(client, temp_store, monkeypatch):
    monkeypatch.setattr(di, "get_signal_store", lambda: temp_store)
    temp_store.load_from_yaml()

    resp = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "grafana",
            "source_name": "checkout.json",
            "auto_approve": "false",
            "dashboard": {
                "dashboard": {
                    "uid": "checkout-upload",
                    "title": "Checkout Upload",
                    "panels": [
                        {
                            "type": "timeseries",
                            "title": "Request Rate",
                            "targets": [
                                {
                                    "expr": "sum(rate(http_requests_total[5m]))",
                                    "datasource": {"type": "prometheus", "uid": "prom"},
                                }
                            ],
                        }
                    ],
                }
            },
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["dashboard_uid"] == "checkout-upload"
    assert body["backend"] == "grafana_json"
    assert body["status"] == "pending"
    assert "http_requests_total" in body["metrics_found"]
    assert body["signal_quality"]["metrics_total"] == 1
    assert "recognized_metrics_before_learning" in body["learning_impact"]
    stored = temp_store.get_ingested_dashboard("checkout-upload", backend_name="grafana_json")
    assert stored is not None

    listed = client.get("/api/v1/learn/dashboards")
    assert listed.status_code == 200
    dashboard = listed.json()["dashboards"][0]
    assert dashboard["signal_quality"]["metrics_total"] == 1
    assert "learning_impact" in dashboard


def test_list_ingested_dashboards_handles_legacy_string_signals(client, temp_store):
    temp_store.record_ingested_dashboard(
        "legacy-signals",
        backend_name="grafana",
        metrics_found=["legacy_metric_total"],
        signals_inferred=["request_rate", "error_rate"],
        status="pending",
    )

    resp = client.get("/api/v1/learn/dashboards")

    assert resp.status_code == 200
    dashboard = resp.json()["dashboards"][0]
    assert dashboard["signals_inferred"] == ["request_rate", "error_rate"]
    assert dashboard["signal_quality"]["legacy_signals"] == 2
    assert dashboard["learning_impact"]["unresolved_metrics"] == ["legacy_metric_total"]


def test_learning_search_and_service_summary(client, temp_store):
    if not temp_store._learning_index_available():
        pytest.skip("SQLite FTS5 is not available")

    temp_store.index_dashboard_context(
        dashboard_uid="checkout-dash",
        backend_name="grafana_json",
        dashboard_title="Checkout Service Health",
        dashboard_tags=["service:checkout"],
        panels=[
            {
                "title": "Checkout latency",
                "queries": ['histogram_quantile(0.95, checkout_custom_latency_ms{service="checkout"})'],
                "metrics": ["checkout_custom_latency_ms"],
            }
        ],
        metrics_found=["checkout_custom_latency_ms"],
        signals_inferred=[
            {
                "signal_type": "request_latency",
                "metric": "checkout_custom_latency_ms",
                "source": "heuristic",
                "confidence": 0.87,
                "auto_teach_eligible": True,
                "reason": "Panel title and metric name indicate latency",
            }
        ],
        status="approved",
    )

    search = client.get("/api/v1/learning/search?q=checkout%20latency&service=checkout&include_candidates=false")
    assert search.status_code == 200
    assert search.json()["count"] == 1
    assert search.json()["results"][0]["metric_name"] == "checkout_custom_latency_ms"

    service = client.get("/api/v1/services/checkout?include_candidates=false")
    assert service.status_code == 200
    body = service.json()
    assert body["trusted_context_rows"] == 1
    assert body["top_metrics"][0]["metric"] == "checkout_custom_latency_ms"


def test_learning_search_reports_unavailable_fts(client, temp_store, monkeypatch):
    monkeypatch.setattr(temp_store, "_learning_index_available", lambda: False)

    resp = client.get("/api/v1/learning/search?q=checkout")

    assert resp.status_code == 503
    assert "SQLite FTS5" in resp.json()["detail"]
