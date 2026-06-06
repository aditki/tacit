"""Contract tests for DashForge's own FastAPI surface (GET + POST).

These exercise the request/response models end-to-end through the ASGI app with
the pipeline/store/ingest dependencies mocked, so the API contract (status
codes, validation, response shapes) is locked in.

Skipped automatically on Python < 3.12 because the app transitively imports
``dashforge.agents.llm`` which uses 3.12 generic syntax.
"""

from __future__ import annotations

import sys

import pytest

if sys.version_info < (3, 12):  # pragma: no cover - env guard  # noqa: UP036
    pytest.skip("DashForge API contract requires Python 3.12", allow_module_level=True)

from fastapi.testclient import TestClient

import dashforge.dashboard_ingest as di
import dashforge.signals as signals_mod
from dashforge.main import app
from dashforge.signals import SignalStore


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
    stored = temp_store.get_ingested_dashboard("checkout-upload", backend_name="grafana_json")
    assert stored is not None
