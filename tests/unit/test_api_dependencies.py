from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient

import tacit.pipeline as pipeline_mod
from tacit.api.app import create_app
from tacit.api.dependencies import get_history_store, get_signal_store
from tacit.api.security import (
    KnowledgeAction,
    assert_knowledge_action,
    assert_tenant_access,
    resolve_knowledge_tenant,
    verify_api_key,
)
from tacit.backends.base import DashboardFeatures
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies, resolve_knowledge_service
from tacit.models.schemas import DashRequest, DashResponse
from tacit.signals import SignalStore


def _security_request(runtime_settings: Settings, tenant_id: str | None = None) -> Request:
    headers = [] if tenant_id is None else [(b"x-tacit-tenant", tenant_id.encode())]
    return Request(
        {
            "type": "http",
            "app": SimpleNamespace(state=SimpleNamespace(settings=runtime_settings)),
            "headers": headers,
        }
    )


@pytest.mark.parametrize(
    ("action", "required_permissions"),
    [
        (KnowledgeAction.READ, ("knowledge.read",)),
        (KnowledgeAction.APPROVE, ("knowledge.review",)),
        (KnowledgeAction.TRUST, ("knowledge.review", "knowledge.trust")),
        (KnowledgeAction.REJECT, ("knowledge.reject",)),
        (KnowledgeAction.CORRECT, ("knowledge.correct",)),
        (KnowledgeAction.EXPORT, ("knowledge.read", "knowledge.export")),
        (KnowledgeAction.OVERRIDE, ("knowledge.override",)),
        (KnowledgeAction.TEACH_SIGNALS, ("knowledge.review", "knowledge.trust")),
    ],
)
def test_knowledge_action_permission_matrix(action, required_permissions):
    allowed = _security_request(Settings(knowledge_permissions=",".join(required_permissions)))
    assert_knowledge_action(allowed, action)

    for missing_permission in required_permissions:
        granted = [permission for permission in required_permissions if permission != missing_permission]
        denied = _security_request(Settings(knowledge_permissions=",".join(granted)))
        with pytest.raises(HTTPException) as exc_info:
            assert_knowledge_action(denied, action)
        assert exc_info.value.status_code == 403
        assert exc_info.value.detail == f"Missing permission: {missing_permission}"


@pytest.mark.parametrize(
    "endpoint",
    [
        "/api/v1/learn/dashboards",
        "/api/v1/learn/alerts",
        "/api/v1/learn/runbooks",
        "/api/v1/learn/incidents",
        "/api/v1/learning/search?q=checkout",
        "/api/v1/services/checkout",
        "/api/v1/signals",
        "/api/v1/signals/stats",
        "/api/v1/signals/request_latency",
    ],
)
def test_learning_and_signal_reads_require_knowledge_read(endpoint, tmp_path):
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="",
            signals_db_path=str(tmp_path / "signals.db"),
        )
    )

    response = TestClient(app).get(endpoint)

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"


@pytest.mark.parametrize(
    ("configured", "requested", "expected", "status_code"),
    [
        ("tenant-a", None, "tenant-a", None),
        ("tenant-a", "tenant-a", "tenant-a", None),
        ("tenant-a", "tenant-b", None, 403),
        ("*", "tenant-a", "tenant-a", None),
        ("*", None, None, 400),
        ("*", "tenant with spaces", None, 400),
        ("*", "x" * 129, None, 400),
    ],
)
def test_knowledge_tenant_resolution_matrix(configured, requested, expected, status_code):
    if status_code is None:
        assert resolve_knowledge_tenant(configured, requested) == expected
        return
    with pytest.raises(HTTPException) as exc_info:
        resolve_knowledge_tenant(configured, requested)
    assert exc_info.value.status_code == status_code


def test_api_app_rejects_unauthenticated_wildcard_tenancy():
    with pytest.raises(ValueError, match="Wildcard knowledge tenancy requires API authentication"):
        create_app(runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"))


async def test_api_key_dependency_fails_closed_for_unauthenticated_wildcard_tenancy():
    request = _security_request(
        Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=False),
        "tenant-a",
    )

    with pytest.raises(HTTPException) as exc_info:
        await verify_api_key(request, None)

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "Wildcard knowledge tenancy requires API authentication"


@pytest.mark.parametrize(
    ("configured", "selected", "resource_tenant", "status_code"),
    [
        ("tenant-a", None, "tenant-a", None),
        ("*", "tenant-a", "tenant-a", None),
        ("*", "tenant-a", "tenant-b", 403),
        ("*", "tenant-a", "", 403),
    ],
)
def test_resource_tenant_access_matrix(configured, selected, resource_tenant, status_code):
    request = _security_request(Settings(knowledge_tenant_id=configured), selected)
    if status_code is None:
        assert assert_tenant_access(request, resource_tenant) == resource_tenant
        return
    with pytest.raises(HTTPException) as exc_info:
        assert_tenant_access(request, resource_tenant)
    assert exc_info.value.status_code == status_code


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


def test_injected_signal_store_without_database_path_cannot_fall_back_globally():
    injected_store = object()
    deps = PipelineDependencies(
        settings=Settings(),
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        signal_store_factory=lambda: injected_store,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(RuntimeError, match="unavailable for the active signal store"):
        resolve_knowledge_service(deps, signal_store=injected_store)


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


def test_wildcard_api_keys_are_bound_to_the_selected_tenant(monkeypatch):
    runtime_settings = Settings(
        api_auth_enabled=True,
        api_auth_key="shared-key-must-not-cross-tenants",
        knowledge_tenant_id="*",
        knowledge_tenant_api_keys={
            "tenant-a": "tenant-a-key",
            "tenant-b": "tenant-b-key",
        },
    )
    app = create_app(runtime_settings=runtime_settings)

    async def fake_run_pipeline(request: DashRequest, deps):
        return DashResponse(
            dashboard_url="http://dash",
            dashboard_uid="dash-1",
            panel_count=0,
            summary=request.tenant_id,
        )

    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", fake_run_pipeline)
    client = TestClient(app)

    missing_tenant = client.post(
        "/api/v1/chart",
        headers={"X-API-Key": "tenant-a-key"},
        json={"prompt": "checkout latency"},
    )
    wrong_tenant_key = client.post(
        "/api/v1/chart",
        headers={"X-Tacit-Tenant": "tenant-b", "X-API-Key": "tenant-a-key"},
        json={"prompt": "checkout latency"},
    )
    shared_key = client.post(
        "/api/v1/chart",
        headers={
            "X-Tacit-Tenant": "tenant-a",
            "X-API-Key": "shared-key-must-not-cross-tenants",
        },
        json={"prompt": "checkout latency"},
    )
    allowed = client.post(
        "/api/v1/chart",
        headers={"X-Tacit-Tenant": "tenant-b", "X-API-Key": "tenant-b-key"},
        json={"prompt": "checkout latency", "tenant_id": "tenant-b"},
    )

    assert missing_tenant.status_code == 400
    assert wrong_tenant_key.status_code == 401
    assert shared_key.status_code == 401
    assert allowed.status_code == 200
    assert allowed.json()["summary"] == "tenant-b"


def test_wildcard_settings_reject_duplicate_tenant_api_keys_without_disclosure():
    duplicate_secret = "same-secret-for-two-tenants"

    with pytest.raises(ValueError) as exc_info:
        Settings(
            _env_file=None,
            knowledge_tenant_id="*",
            knowledge_tenant_api_keys={
                "tenant-a": duplicate_secret,
                "tenant-b": duplicate_secret,
            },
        )

    assert "unique non-empty key per tenant" in str(exc_info.value)
    assert duplicate_secret not in str(exc_info.value)


def test_wildcard_settings_allow_multiple_unconfigured_tenant_keys():
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="*",
        knowledge_tenant_api_keys={"tenant-a": "", "tenant-b": ""},
    )

    assert runtime_settings.knowledge_tenant_api_keys == {"tenant-a": "", "tenant-b": ""}


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


def test_pending_learning_requires_and_threads_wildcard_tenant(monkeypatch, tmp_path):
    app = create_app(
        runtime_settings=Settings(
            knowledge_tenant_id="*",
            api_auth_enabled=True,
            knowledge_tenant_api_keys={"tenant-a": "tenant-a-secret"},
            signals_db_path=str(tmp_path / "signals.db"),
        )
    )
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
        headers={"X-API-Key": "tenant-a-secret"},
        json={"dashboard_uid": "tenant-dash", "auto_approve": False},
    )
    accepted = client.post(
        "/api/v1/learn/dashboard",
        headers={"X-Tacit-Tenant": "tenant-a", "X-API-Key": "tenant-a-secret"},
        json={"dashboard_uid": "tenant-dash", "auto_approve": False},
    )

    assert missing.status_code == 400
    assert accepted.status_code == 200
    assert seen_tenants == ["tenant-a"]


def test_replay_route_uses_app_scoped_runtime_settings(monkeypatch):
    runtime_settings = Settings(knowledge_tenant_id="tenant-a")
    seen_settings: list[Settings] = []

    class FakeContract:
        class request:
            class scope:
                tenant_id = "tenant-a"

        def model_dump(self, **kwargs):
            return {"investigation": {"id": "inv-app-replay"}}

    class FakeStore:
        def get_contract(self, investigation_id, revision, *, tenant_id=None):
            assert tenant_id == "tenant-a"
            return FakeContract()

        def replay_contract(
            self,
            investigation_id,
            revision,
            *,
            mode,
            changes,
            runtime_settings,
            knowledge_service_factory,
            tenant_id,
        ):
            seen_settings.append(runtime_settings)
            assert knowledge_service_factory is not None
            assert tenant_id == "tenant-a"
            return FakeContract()

    app = create_app(runtime_settings=runtime_settings)
    app.dependency_overrides[get_history_store] = FakeStore
    response = TestClient(app).post(
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


def test_pending_learning_requires_apply_permission(monkeypatch):
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review,knowledge.trust",
        )
    )
    app.dependency_overrides[get_signal_store] = lambda: object()
    called = False

    async def fake_ingest_dashboard(**_kwargs):
        nonlocal called
        called = True
        return {"dashboard_uid": "restricted-pending"}

    monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", fake_ingest_dashboard)

    response = TestClient(app).post(
        "/api/v1/learn/dashboard",
        json={"dashboard_uid": "restricted-pending", "auto_approve": False},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.apply"
    assert called is False


@pytest.mark.parametrize("endpoint", ["runbooks", "incidents"])
def test_artifact_learning_requires_review_permission_before_persistence(endpoint):
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.apply",
        )
    )

    response = TestClient(app).post(
        f"/api/v1/learn/{endpoint}",
        json={"title": "Restricted artifact", "body_text": "checkout depends on redis"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.review"


@pytest.mark.parametrize(
    ("permissions", "missing_permission"),
    [
        ("knowledge.read", "knowledge.review"),
        ("knowledge.read,knowledge.review", "knowledge.trust"),
    ],
)
def test_manual_signal_teaching_requires_review_and_trust_permissions(permissions, missing_permission):
    client = TestClient(create_app(runtime_settings=Settings(knowledge_permissions=permissions)))

    response = client.post(
        "/api/v1/signals/teach",
        json={
            "signal_type": "restricted_signal",
            "metric_patterns": [{"pattern": "restricted_metric", "confidence": 0.9}],
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == f"Missing permission: {missing_permission}"


def test_dashboard_rejection_requires_knowledge_reject_permission(monkeypatch):
    called = False

    def fake_reject(**kwargs):
        nonlocal called
        called = True
        return {"status": "rejected"}

    monkeypatch.setattr("tacit.dashboard_ingest.reject_ingested_dashboard_record", fake_reject)
    client = TestClient(create_app(runtime_settings=Settings(knowledge_permissions="knowledge.read")))

    response = client.post("/api/v1/learn/dashboards/restricted-dash/reject")

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.reject"
    assert called is False


def test_dashboard_ignore_requires_knowledge_reject_permission(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_dashboard("restricted-dash", status="pending")
    monkeypatch.setattr("tacit.api.routes.learning.signals_mod.get_signal_store", lambda: store)
    client = TestClient(create_app(runtime_settings=Settings(knowledge_permissions="knowledge.read")))

    response = client.post("/api/v1/learn/dashboards/restricted-dash/ignore")

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.reject"
    assert store.get_ingested_dashboard("restricted-dash")["status"] == "pending"


def test_artifact_lists_use_the_requested_tenant(monkeypatch, tmp_path):
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
    for tenant_id in ("tenant-a", "tenant-b"):
        store.record_learned_artifact(
            tenant_id=tenant_id,
            artifact_id="shared-runbook",
            artifact_type="runbook",
            title=f"{tenant_id} runbook",
        )
    app = create_app(
        runtime_settings=Settings(
            knowledge_tenant_id="*",
            api_auth_enabled=True,
            knowledge_tenant_api_keys={"tenant-a": "tenant-a-secret"},
        )
    )
    app.dependency_overrides[get_signal_store] = lambda: store
    client = TestClient(app)

    missing = client.get("/api/v1/learn/runbooks", headers={"X-API-Key": "tenant-a-secret"})
    tenant_a = client.get(
        "/api/v1/learn/runbooks",
        headers={"X-Tacit-Tenant": "tenant-a", "X-API-Key": "tenant-a-secret"},
    )

    assert missing.status_code == 400
    assert tenant_a.status_code == 200
    assert tenant_a.json()["count"] == 1
    assert tenant_a.json()["runbooks"][0]["title"] == "tenant-a runbook"


def test_artifact_routes_page_all_rows_and_batch_extraction_summaries(monkeypatch, tmp_path):
    runtime_settings = Settings(_env_file=None)
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=runtime_settings,
    )
    artifact_count = 1_005
    extraction_count = 605
    with store._conn() as conn:
        conn.executemany(
            """INSERT INTO learned_artifacts(
                   tenant_id, artifact_id, artifact_type, title,
                   first_seen_at, last_seen_at, updated_at, created_at
               ) VALUES ('default', ?, 'runbook', ?, ?, ?, ?, ?)""",
            [
                (
                    f"artifact-{index:05d}",
                    f"Runbook {index}",
                    float(index),
                    float(index),
                    float(index),
                    float(index),
                )
                for index in range(1, artifact_count + 1)
            ],
        )
        conn.executemany(
            """INSERT INTO evidence_requirements(
                   tenant_id, id, artifact_id, subject, created_at
               ) VALUES ('default', ?, ?, 'checkout', ?)""",
            [
                (
                    f"er-{index:05d}",
                    "artifact-01005",
                    float(index),
                )
                for index in range(1, extraction_count + 1)
            ],
        )
        artifact_plan = " ".join(
            str(value) for row in conn.execute("""EXPLAIN QUERY PLAN SELECT id FROM learned_artifacts
                    WHERE tenant_id='default' AND artifact_type='runbook'
                    ORDER BY updated_at DESC, id DESC LIMIT 50""") for value in row
        )
    assert "idx_learned_artifacts_page" in artifact_plan

    monkeypatch.setattr(
        store,
        "list_artifact_extractions",
        lambda *args, **kwargs: pytest.fail("artifact list performed an N+1 detail load"),
    )
    batch_count_calls = 0
    load_counts = store.artifact_extraction_counts_batch

    def tracked_count_batch(*args, **kwargs):
        nonlocal batch_count_calls
        batch_count_calls += 1
        return load_counts(*args, **kwargs)

    monkeypatch.setattr(store, "artifact_extraction_counts_batch", tracked_count_batch)
    app = create_app(runtime_settings=runtime_settings)
    app.dependency_overrides[get_signal_store] = lambda: store
    client = TestClient(app)

    artifact_ids = []
    cursor = None
    page_count = 0
    while True:
        params = {"limit": 137}
        if cursor:
            params["cursor"] = cursor
        response = client.get("/api/v1/learn/runbooks", params=params)
        assert response.status_code == 200
        body = response.json()
        page_count += 1
        artifact_ids.extend(item["artifact_id"] for item in body["runbooks"])
        for item in body["runbooks"]:
            expected = 605 if item["artifact_id"] == "artifact-01005" else 0
            assert item["extraction_counts"]["evidence_requirements"] == expected
            assert "extractions" not in item
        if not body["has_more"]:
            assert body["next_cursor"] is None
            break
        cursor = body["next_cursor"]
        assert cursor

    assert artifact_ids == [f"artifact-{index:05d}" for index in range(artifact_count, 0, -1)]
    assert len(set(artifact_ids)) == artifact_count
    assert batch_count_calls == page_count

    extraction_ids = []
    cursor = None
    while True:
        params = {"kind": "evidence_requirements", "limit": 127}
        if cursor:
            params["cursor"] = cursor
        response = client.get(
            "/api/v1/learn/runbooks/artifact-01005/extractions",
            params=params,
        )
        assert response.status_code == 200
        body = response.json()
        extraction_ids.extend(item["id"] for item in body["extractions"])
        if not body["has_more"]:
            assert body["next_cursor"] is None
            break
        cursor = body["next_cursor"]
        assert cursor

    assert extraction_ids == [f"er-{index:05d}" for index in range(1, extraction_count + 1)]
    assert len(set(extraction_ids)) == extraction_count

    first_generation = client.get(
        "/api/v1/learn/runbooks/artifact-01005/extractions",
        params={"kind": "evidence_requirements", "limit": 2},
    )
    assert first_generation.status_code == 200
    stale_cursor = first_generation.json()["next_cursor"]
    assert stale_cursor
    store.replace_artifact_extractions(
        artifact_id="artifact-01005",
        evidence_requirements=[
            {"id": "er-new-050", "subject": "checkout"},
            {"id": "er-new-250", "subject": "checkout"},
            {"id": "er-new-350", "subject": "checkout"},
        ],
    )

    stale_continuation = client.get(
        "/api/v1/learn/runbooks/artifact-01005/extractions",
        params={
            "kind": "evidence_requirements",
            "limit": 2,
            "cursor": stale_cursor,
        },
    )
    restarted = client.get(
        "/api/v1/learn/runbooks/artifact-01005/extractions",
        params={"kind": "evidence_requirements", "limit": 3},
    )

    assert stale_continuation.status_code == 409
    assert "restart pagination" in stale_continuation.json()["detail"]
    assert [row["id"] for row in restarted.json()["extractions"]] == [
        "er-new-050",
        "er-new-250",
        "er-new-350",
    ]


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
        assert deps.knowledge_service_factory is not None
        seen_stores["knowledge"] = deps.knowledge_service_factory()
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
    knowledge = client.get("/api/v1/knowledge/status")
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
    assert knowledge.status_code == 200
    assert learned.status_code == 200, learned.text
    assert seen_stores["history"] is app.state.runtime_stores.history()
    assert seen_stores["feedback"] is app.state.runtime_stores.feedback()
    assert seen_stores["signals"] is app.state.runtime_stores.signals()
    assert seen_stores["knowledge"] is app.state.runtime_stores.knowledge()
    assert seen_stores["history"]._db_path == tmp_path / "app" / "history.db"
    assert seen_stores["feedback"]._db_path == tmp_path / "app" / "feedback.db"
    assert seen_stores["signals"]._db_path == tmp_path / "app" / "signals.db"
    assert seen_stores["knowledge"].repository._db_path == tmp_path / "app" / "signals.db"
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


def test_alert_dry_runs_preserve_selected_wildcard_tenant(monkeypatch):
    seen: list[tuple[str, str | None]] = []

    async def ingest_alert(**kwargs):
        seen.append(("single", kwargs["tenant_id"]))
        return {"alert_uid": kwargs["alert_uid"], "dry_run": True}

    async def learn_alerts(*args, **kwargs):
        seen.append(("bulk", kwargs["tenant_id"]))
        return {
            "alerts_learned": 0,
            "signals_inferred": 0,
            "mappings_created": 0,
            "warnings": [],
            "dry_run": True,
        }

    monkeypatch.setattr("tacit.alert_ingest.ingest_alert", ingest_alert)
    monkeypatch.setattr("tacit.alert_ingest.learn_backend_alerts", learn_alerts)
    app = create_app(
        runtime_settings=Settings(
            knowledge_tenant_id="*",
            api_auth_enabled=True,
            knowledge_tenant_api_keys={"tenant-a": "tenant-a-secret"},
        )
    )
    app.dependency_overrides[get_signal_store] = lambda: object()
    client = TestClient(app)
    headers = {"X-Tacit-Tenant": "tenant-a", "X-API-Key": "tenant-a-secret"}

    single = client.post(
        "/api/v1/learn/alerts",
        headers=headers,
        json={"alert_uid": "checkout-latency", "backend": "grafana", "dry_run": True},
    )
    bulk = client.post(
        "/api/v1/learn/backends/grafana/alerts?dry_run=true",
        headers=headers,
    )

    assert single.status_code == 200, single.text
    assert bulk.status_code == 200, bulk.text
    assert seen == [("single", "tenant-a"), ("bulk", "tenant-a")]


def test_openapi_feedback_description_preserves_governed_authority_boundary():
    from tacit.api.app import OPENAPI_TAGS

    feedback = next(tag for tag in OPENAPI_TAGS if tag["name"] == "Feedback")

    assert "assessment and governed-candidate input" in feedback["description"]
    assert "never changes runtime ranking directly" in feedback["description"]
