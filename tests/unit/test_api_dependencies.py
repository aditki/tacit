from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import tacit.pipeline as pipeline_mod
from tacit.api.app import create_app
from tacit.backends.base import DashboardFeatures
from tacit.config import Settings
from tacit.models.schemas import DashRequest, DashResponse


def test_chart_route_uses_app_scoped_pipeline_settings(monkeypatch):
    runtime_settings = Settings(pipeline_timeout_seconds=3, pipeline_max_concurrent=1)
    app = create_app(runtime_settings=runtime_settings)
    seen_settings: list[Settings] = []
    seen_backend_settings: list[Settings] = []

    def fake_get_active_backends(settings_arg: Settings):
        seen_backend_settings.append(settings_arg)
        return []

    async def fake_run_pipeline(request: DashRequest, deps):
        seen_settings.append(deps.settings)
        assert deps.backend_factory() == []
        return DashResponse(
            dashboard_url="http://dash",
            dashboard_uid="dash-1",
            panel_count=0,
            summary=request.prompt,
        )

    monkeypatch.setattr(pipeline_mod, "get_active_backends", fake_get_active_backends)
    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", fake_run_pipeline)

    response = TestClient(app).post("/api/v1/chart", json={"prompt": "checkout latency"})

    assert response.status_code == 200
    assert seen_settings == [runtime_settings]
    assert seen_backend_settings == [runtime_settings]


def test_api_auth_uses_app_scoped_settings(monkeypatch):
    runtime_settings = Settings(api_auth_enabled=True, api_auth_key="app-secret")
    app = create_app(runtime_settings=runtime_settings)

    async def fake_run_pipeline(request: DashRequest, deps):
        return DashResponse(
            dashboard_url="http://dash",
            dashboard_uid="dash-1",
            panel_count=0,
            summary=request.prompt,
        )

    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", fake_run_pipeline)
    client = TestClient(app)

    assert client.post("/api/v1/chart", json={"prompt": "checkout latency"}).status_code == 401
    ok = client.post(
        "/api/v1/chart",
        headers={"X-API-Key": "app-secret"},
        json={"prompt": "checkout latency"},
    )
    assert ok.status_code == 200


def test_learning_dashboard_route_uses_app_scoped_backend_settings(monkeypatch, tmp_path):
    runtime_settings = Settings(
        grafana_url="http://runtime-grafana",
        signals_db_path=str(tmp_path / "signals.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    seen_settings: list[Settings] = []

    class FakeBackend:
        name = "grafana"

        async def ingest_dashboard(self, uid: str):
            return DashboardFeatures(
                dashboard_uid=uid,
                dashboard_title="Runtime Dashboard",
                backend_name="grafana",
                query_language="promql",
                metrics_found=["checkout_latency_seconds"],
                panel_count=1,
                panel_titles=["Latency"],
                panels=[],
            )

        async def close(self):
            return None

    def fake_get_active_backends(settings_arg: Settings):
        seen_settings.append(settings_arg)
        return [FakeBackend()]

    async def fake_ingest_features(features, **kwargs):
        return {"dashboard_uid": features.dashboard_uid, "backend": features.backend_name}

    monkeypatch.setattr("tacit.backends.get_active_backends", fake_get_active_backends)
    monkeypatch.setattr("tacit.dashboard_ingest.service.ingest_dashboard_features", fake_ingest_features)

    response = TestClient(app).post(
        "/api/v1/learn/dashboard",
        json={"dashboard_uid": "runtime-dash", "backend": "grafana", "auto_approve": False},
    )

    assert response.status_code == 200
    assert response.json()["dashboard_uid"] == "runtime-dash"
    assert seen_settings == [runtime_settings]


def test_learning_backend_route_uses_app_scoped_backend_settings(monkeypatch, tmp_path):
    runtime_settings = Settings(
        grafana_url="http://runtime-grafana",
        adapter_max_concurrent=3,
        signals_db_path=str(tmp_path / "signals.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    seen_settings: list[Settings] = []

    class FakeBackend:
        name = "grafana"

        async def list_dashboards(self, limit: int = 500):
            return []

        async def close(self):
            return None

    def fake_get_active_backends(settings_arg: Settings):
        seen_settings.append(settings_arg)
        return [FakeBackend()]

    monkeypatch.setattr("tacit.backends.get_active_backends", fake_get_active_backends)

    response = TestClient(app).post("/api/v1/learn/grafana?limit=1")

    assert response.status_code == 200
    assert response.json()["backend"] == "grafana"
    assert seen_settings == [runtime_settings]


def test_uploaded_dashboard_route_uses_app_scoped_settings(monkeypatch, tmp_path):
    runtime_settings = Settings(
        learned_archetypes_generation_enabled=True,
        learned_archetypes_tenant_id="runtime",
        signals_db_path=str(tmp_path / "signals.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    seen_settings: list[Settings] = []

    monkeypatch.setattr("tacit.dashboard_uploads.parse_uploaded_dashboard", lambda *_args, **_kwargs: object())

    async def fake_ingest_features(_features, **kwargs):
        seen_settings.append(kwargs["runtime_settings"])
        return {"dashboard_uid": "uploaded"}

    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard_features", fake_ingest_features)
    response = TestClient(app).post(
        "/api/v1/learn/dashboard/json",
        json={"vendor": "grafana", "source_name": "upload.json", "dashboard": {}, "auto_approve": False},
    )

    assert response.status_code == 200
    assert seen_settings == [runtime_settings]


def test_dashboard_approval_route_uses_app_scoped_settings(monkeypatch):
    runtime_settings = Settings(
        learned_archetypes_automatic_registration_enabled=True,
        learned_archetypes_quarantine_path="runtime-quarantine",
    )
    app = create_app(runtime_settings=runtime_settings)
    seen_settings: list[Settings] = []

    def fake_approve(**kwargs):
        seen_settings.append(kwargs["runtime_settings"])
        return {"dashboard_uid": kwargs["dashboard_uid"], "status": "approved"}

    monkeypatch.setattr("tacit.dashboard_ingest.approve_ingested_dashboard_record", fake_approve)
    monkeypatch.setattr("tacit.api.routes.learning.signals_mod.get_signal_store", lambda: object())
    response = TestClient(app).post("/api/v1/learn/dashboards/uploaded/approve?backend=grafana_json")

    assert response.status_code == 200
    assert seen_settings == [runtime_settings]


def test_app_scoped_database_paths_drive_pipeline_and_api_stores(tmp_path, monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "app" / "history.db"),
        feedback_db_path=str(tmp_path / "app" / "feedback.db"),
        signals_db_path=str(tmp_path / "app" / "signals.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    seen_stores = {}

    async def fake_run_pipeline(request: DashRequest, deps):
        seen_stores["history"] = deps.history_store_factory()
        seen_stores["feedback"] = deps.feedback_store_factory()
        assert deps.signal_store_factory is not None
        seen_stores["signals"] = deps.signal_store_factory()
        return DashResponse(
            dashboard_url="http://dash",
            dashboard_uid="dash-1",
            panel_count=0,
            summary=request.prompt,
        )

    def unexpected_global_store():
        raise AssertionError("app-scoped database path fell back to a process-global store")

    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(pipeline_mod, "get_investigation_store", unexpected_global_store)
    monkeypatch.setattr("tacit.history.get_investigation_store", unexpected_global_store)
    monkeypatch.setattr("tacit.feedback.get_feedback_store", unexpected_global_store)
    monkeypatch.setattr("tacit.signals.get_signal_store", unexpected_global_store)

    client = TestClient(app)
    chart = client.post("/api/v1/chart", json={"prompt": "checkout latency"})
    history = client.get("/api/v1/investigations")
    feedback = client.get("/api/v1/feedback/stats")
    signals = client.get("/api/v1/signals")
    learned = client.post(
        "/api/v1/learn/runbooks",
        json={
            "title": "Checkout recovery",
            "body_text": "The checkout service depends on redis-cart.",
            "external_id": "runbook:checkout-recovery",
        },
    )

    assert chart.status_code == 200
    assert history.status_code == 200
    assert feedback.status_code == 200
    assert signals.status_code == 200
    assert learned.status_code == 200, learned.text
    assert seen_stores["history"] is app.state.runtime_stores.history()
    assert seen_stores["feedback"] is app.state.runtime_stores.feedback()
    assert seen_stores["signals"] is app.state.runtime_stores.signals()
    assert seen_stores["history"]._db_path == tmp_path / "app" / "history.db"
    assert seen_stores["feedback"]._db_path == tmp_path / "app" / "feedback.db"
    assert seen_stores["signals"]._db_path == tmp_path / "app" / "signals.db"
    assert seen_stores["signals"].list_learned_artifacts(artifact_type="runbook")


@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        (
            "/api/v1/learn/runbooks",
            {
                "title": "Checkout recovery",
                "body_text": "Check checkout_latency_seconds.",
                "external_id": "runbook:dry-run",
                "dry_run": True,
            },
        ),
        (
            "/api/v1/learn/incidents",
            {
                "title": "Checkout incident",
                "body_text": "Observed checkout_latency_seconds.",
                "external_id": "incident:dry-run",
                "dry_run": True,
            },
        ),
    ],
)
def test_artifact_dry_runs_do_not_initialize_signal_storage(endpoint, payload):
    app = create_app(runtime_settings=Settings(_env_file=None))
    store_calls = 0

    def unavailable_store():
        nonlocal store_calls
        store_calls += 1
        raise AssertionError("dry-run initialized persistent signal storage")

    app.state.runtime_stores.signals = unavailable_store

    response = TestClient(app).post(endpoint, json=payload)

    assert response.status_code == 200, response.text
    assert response.json()["dry_run"] is True
    assert store_calls == 0
