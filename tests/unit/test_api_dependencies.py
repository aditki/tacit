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


def test_learning_dashboard_route_uses_app_scoped_backend_settings(monkeypatch):
    runtime_settings = Settings(grafana_url="http://runtime-grafana")
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


def test_pending_learning_requires_and_threads_wildcard_tenant(monkeypatch):
    app = create_app(runtime_settings=Settings(knowledge_tenant_id="*"))
    seen_tenants: list[str | None] = []

    async def fake_ingest_dashboard(
        dashboard_uid,
        backend_name=None,
        auto_approve=False,
        runtime_settings=None,
        tenant_id=None,
    ):
        seen_tenants.append(tenant_id)
        return {"dashboard_uid": dashboard_uid, "status": "pending"}

    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", fake_ingest_dashboard)
    client = TestClient(app)

    missing = client.post(
        "/api/v1/learn/dashboard",
        json={"dashboard_uid": "tenant-dash", "auto_approve": False},
    )
    accepted = client.post(
        "/api/v1/learn/dashboard",
        headers={"X-Tacit-Tenant": "tenant-a"},
        json={"dashboard_uid": "tenant-dash", "auto_approve": False},
    )

    assert missing.status_code == 400
    assert accepted.status_code == 200
    assert seen_tenants == ["tenant-a"]


def test_replay_route_uses_app_scoped_runtime_settings(monkeypatch):
    runtime_settings = Settings(knowledge_tenant_id="tenant-a")
    seen_settings: list[Settings] = []

    class FakeContract:
        def model_dump(self, **kwargs):
            return {"investigation": {"id": "inv-app-replay"}}

    class FakeStore:
        def replay_contract(self, investigation_id, revision, *, mode, changes, runtime_settings):
            seen_settings.append(runtime_settings)
            return FakeContract()

    monkeypatch.setattr("tacit.api.routes.history.history_mod.get_investigation_store", lambda: FakeStore())

    response = TestClient(create_app(runtime_settings=runtime_settings)).post(
        "/api/v1/investigations/inv-app-replay/replay",
        json={"mode": "current_engine", "changes": {}},
    )

    assert response.status_code == 200
    assert seen_settings == [runtime_settings]


@pytest.mark.parametrize(
    ("permissions", "missing_permission"),
    [
        ("knowledge.read", "knowledge.review"),
        ("knowledge.read,knowledge.review", "knowledge.trust"),
    ],
)
def test_learning_auto_approval_requires_knowledge_permissions(monkeypatch, permissions, missing_permission):
    app = create_app(runtime_settings=Settings(knowledge_permissions=permissions))
    called = False

    async def fake_ingest_dashboard(**kwargs):
        nonlocal called
        called = True
        return {"dashboard_uid": kwargs["dashboard_uid"]}

    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", fake_ingest_dashboard)

    response = TestClient(app).post(
        "/api/v1/learn/dashboard",
        json={"dashboard_uid": "restricted-dash", "auto_approve": True},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == f"Missing permission: {missing_permission}"
    assert called is False


def test_learning_backend_route_uses_app_scoped_backend_settings(monkeypatch):
    runtime_settings = Settings(grafana_url="http://runtime-grafana", adapter_max_concurrent=3)
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
