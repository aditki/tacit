from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Request

import tacit.pipeline as pipeline_mod
from tacit.agents.providers.base import LLMProvider
from tacit.api.app import create_app
from tacit.api.dependencies import (
    get_feedback_store,
    get_history_store,
    get_knowledge_service,
    get_pipeline_dependencies,
    get_signal_store,
    get_signal_store_factory,
)
from tacit.api.security import (
    KnowledgeAction,
    assert_knowledge_action,
    assert_tenant_access,
    resolve_knowledge_tenant,
    verify_api_key,
)
from tacit.backends.base import DashboardFeatures
from tacit.config import Settings
from tacit.context.base import ContextProvider
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies, resolve_knowledge_service
from tacit.errors import (
    PipelineAdmissionRejected,
    PipelineExecutionError,
    RuntimeOwnershipError,
    SemanticAuthorizationError,
)
from tacit.models.schemas import DashRequest, DashResponse
from tacit.runtime_ownership import (
    RuntimeDatabaseIdentity,
    RuntimeOwnershipDescriptor,
    declare_runtime_factory,
    get_runtime_factory_ownership,
    runtime_descriptor_for_backends,
    runtime_descriptor_for_provider,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
)
from tacit.signals import SignalStore
from tests.http_client import TestClient


def _owned_test_factory(
    factory,
    *,
    runtime_settings: Settings,
    factory_kind: str,
):
    if getattr(factory, "factory_kind", None) == factory_kind:
        return factory
    category, capability = factory_kind.split(":", 1)
    if category in {"store", "knowledge"}:
        settings_owner = runtime_descriptor_from_settings(
            runtime_settings,
            component="test_factory_settings",
        )
        database_path = next(item.path for item in settings_owner.databases if item.role == capability)
        ownership = runtime_descriptor_for_store(
            component=f"test_{factory_kind}_factory",
            runtime_settings=runtime_settings,
            database_role=capability,
            database_path=database_path,
        )
    elif category == "provider":
        ownership = runtime_descriptor_for_provider(
            component=f"test_{factory_kind}_factory",
            runtime_settings=runtime_settings,
            capability=capability,
        )
    else:
        ownership = runtime_descriptor_for_backends(
            component=f"test_{factory_kind}_factory",
            runtime_settings=runtime_settings,
        )
    return declare_runtime_factory(factory, ownership=ownership, factory_kind=factory_kind)


def _isolated_dependencies(**values) -> PipelineDependencies:
    runtime_settings = values["settings"]
    for field, kind in (
        ("backend_factory", "backend:dashboard"),
        ("history_store_factory", "store:history"),
        ("feedback_store_factory", "store:feedback"),
        ("signal_store_factory", "store:signals"),
        ("knowledge_service_factory", "knowledge:signals"),
        ("llm_provider_factory", "provider:llm"),
        ("context_provider_factory", "provider:context"),
    ):
        factory = values.get(field)
        if factory is not None:
            values[field] = _owned_test_factory(
                factory,
                runtime_settings=runtime_settings,
                factory_kind=kind,
            )
    return PipelineDependencies.isolated(**values)


def _security_request(runtime_settings: Settings, tenant_id: str | None = None) -> Request:
    headers = [] if tenant_id is None else [(b"x-tacit-tenant", tenant_id.encode())]
    return Request(
        {
            "type": "http",
            "app": SimpleNamespace(state=SimpleNamespace(settings=runtime_settings)),
            "headers": headers,
        }
    )


def test_auth_enabled_disables_cross_origin_requests_without_an_allowlist() -> None:
    app = create_app(runtime_settings=Settings(_env_file=None, api_auth_enabled=True, api_auth_key="secret"))

    response = TestClient(app).options(
        "/api/v1/signals",
        headers={
            "Origin": "https://console.example",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "X-API-Key,X-Tacit-Tenant",
        },
    )

    assert "access-control-allow-origin" not in response.headers


def test_default_unauthenticated_api_is_same_origin_only() -> None:
    app = create_app(runtime_settings=Settings(_env_file=None))

    preflight = TestClient(app).options(
        "/api/v1/signals",
        headers={
            "Origin": "https://hostile.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        },
    )
    response = TestClient(app).get(
        "/healthz",
        headers={"Origin": "https://hostile.example"},
    )

    assert "access-control-allow-origin" not in preflight.headers
    assert "access-control-allow-origin" not in response.headers


def test_unauthenticated_wildcard_cors_requires_explicit_opt_in() -> None:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_cors_allowed_origins="*",
        )
    )

    response = TestClient(app).get(
        "/healthz",
        headers={"Origin": "https://console.example"},
    )

    assert response.headers["access-control-allow-origin"] == "*"


@pytest.mark.parametrize("api_auth_enabled", [False, True])
def test_default_cors_denies_hostile_browser_origins(api_auth_enabled: bool) -> None:
    runtime_settings = Settings(
        _env_file=None,
        api_auth_enabled=api_auth_enabled,
        api_auth_key="secret" if api_auth_enabled else "",
    )
    client = TestClient(create_app(runtime_settings=runtime_settings))

    get_response = client.get(
        "/healthz",
        headers={"Origin": "https://hostile.example"},
    )
    preflight_response = client.options(
        "/api/v1/chart",
        headers={
            "Origin": "https://hostile.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        },
    )

    assert "access-control-allow-origin" not in get_response.headers
    assert "access-control-allow-origin" not in preflight_response.headers


def test_admission_rejection_normalizes_unknown_public_reason_codes() -> None:
    rejection = PipelineAdmissionRejected("database-hostname-or-other-sensitive-context")

    assert rejection.public_payload() == {
        "detail": "Pipeline capacity is temporarily unavailable",
        "reason_code": "pipeline_admission_rejected",
    }


def test_unauthenticated_cors_wildcard_requires_explicit_configuration() -> None:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_auth_enabled=False,
            api_cors_allowed_origins="*",
        )
    )

    response = TestClient(app).options(
        "/api/v1/chart",
        headers={
            "Origin": "https://console.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"


def test_auth_enabled_uses_the_configured_cors_allowlist() -> None:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_auth_enabled=True,
            api_auth_key="secret",
            api_cors_allowed_origins="https://console.example, https://ops.example",
        )
    )

    response = TestClient(app).options(
        "/api/v1/signals",
        headers={
            "Origin": "https://console.example",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "X-API-Key,X-Tacit-Tenant",
        },
    )

    assert response.headers["access-control-allow-origin"] == "https://console.example"
    allowed_headers = response.headers["access-control-allow-headers"].casefold()
    assert "x-api-key" in allowed_headers
    assert "x-tacit-tenant" in allowed_headers


def test_auth_enabled_rejects_a_wildcard_cors_allowlist() -> None:
    with pytest.raises(ValueError, match="wildcard CORS"):
        Settings(
            _env_file=None,
            api_auth_enabled=True,
            api_auth_key="secret",
            api_cors_allowed_origins="*",
        )


@pytest.mark.parametrize("mutation", ["model_copy", "failed_assignment"])
def test_app_factory_rejects_mutated_authenticated_wildcard_cors(mutation: str) -> None:
    runtime_settings = Settings(_env_file=None, api_auth_enabled=True, api_auth_key="secret")
    if mutation == "model_copy":
        runtime_settings = runtime_settings.model_copy(update={"api_cors_allowed_origins": "*"})
    else:
        with pytest.raises(ValueError, match="wildcard CORS"):
            runtime_settings.api_cors_allowed_origins = "*"

    with pytest.raises(ValueError, match="wildcard CORS"):
        create_app(runtime_settings=runtime_settings)


@pytest.mark.parametrize(
    "origin",
    [
        "https://example.com:notaport",
        "https://example.com:65536",
        "https://exa%6dple.com",
        "https://bad_host.example",
        "https://-bad.example",
        "https://example..com",
        "https://bücher.example",
        "https://[2001:db8::1",
    ],
)
def test_cors_origins_reject_browser_unrepresentable_hosts(origin: str) -> None:
    with pytest.raises(ValueError, match="CORS origins"):
        Settings(_env_file=None, api_cors_allowed_origins=origin)


def test_cors_origins_are_canonical_browser_origins() -> None:
    runtime_settings = Settings(
        _env_file=None,
        api_cors_allowed_origins=(
            "HTTPS://XN--BCHER-KVA.Example:443/, http://LOCALHOST:80, https://[2001:0DB8::1]:8443"
        ),
    )

    assert runtime_settings.api_cors_allowed_origins == (
        "https://xn--bcher-kva.example,http://localhost,https://[2001:db8::1]:8443"
    )


def test_cors_preflight_allows_head_requests() -> None:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_auth_enabled=True,
            api_auth_key="secret",
            api_cors_allowed_origins="https://console.example",
        )
    )

    response = TestClient(app).options(
        "/api/v1/signals",
        headers={
            "Origin": "https://console.example",
            "Access-Control-Request-Method": "HEAD",
            "Access-Control-Request-Headers": "X-API-Key",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://console.example"


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
        (
            KnowledgeAction.TEACH_SIGNALS,
            ("knowledge.read", "knowledge.review", "knowledge.trust", "knowledge.apply"),
        ),
        (
            KnowledgeAction.LEARN_ARTIFACTS,
            ("knowledge.read", "knowledge.review", "knowledge.apply"),
        ),
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
    ("failure", "expected_status", "expected_detail"),
    [
        (
            SemanticAuthorizationError("Missing permission: knowledge.apply"),
            403,
            "Missing permission: knowledge.apply",
        ),
        (
            PermissionError("/private/runtime/secret-signals.db"),
            500,
            "Failed to learn from runbook artifact.",
        ),
    ],
    ids=["semantic-denial", "filesystem-permission"],
)
def test_artifact_route_distinguishes_semantic_and_os_permission_failures(
    monkeypatch,
    failure,
    expected_status,
    expected_detail,
):
    app = create_app(runtime_settings=Settings(_env_file=None))
    monkeypatch.setattr(
        "tacit.artifact_learning.learn_artifact",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(failure),
    )

    response = TestClient(app, raise_server_exceptions=False).post(
        "/api/v1/learn/runbooks",
        json={
            "title": "Checkout recovery",
            "body_text": "Check checkout latency.",
            "dry_run": True,
        },
    )

    assert response.status_code == expected_status
    assert response.json()["detail"] == expected_detail
    assert "/private/runtime/secret-signals.db" not in response.text


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
    database_path = tmp_path / "signals.db"
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="",
            signals_db_path=str(database_path),
        )
    )

    response = TestClient(app).get(endpoint)

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"
    assert not database_path.exists()


@pytest.mark.parametrize(
    ("endpoint", "settings_field"),
    [
        ("/api/v1/signals", "signals_db_path"),
        ("/api/v1/investigations", "history_db_path"),
        ("/api/v1/feedback/stats", "feedback_db_path"),
    ],
)
def test_denied_reads_do_not_initialize_persistence(endpoint, settings_field, tmp_path):
    database_path = tmp_path / f"{settings_field}.db"
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="",
            **{settings_field: str(database_path)},
        )
    )

    response = TestClient(app).get(endpoint)

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"
    assert not database_path.exists()


@pytest.mark.parametrize(
    ("method", "endpoint", "payload"),
    [
        ("get", "/api/v1/knowledge/status", None),
        (
            "post",
            "/api/v1/knowledge/corrections",
            {
                "investigation_id": "inv-other-tenant",
                "investigation_revision": 1,
                "correction_type": "knowledge_incorrect",
                "proposed": {"reason": "incorrect"},
                "explanation": "Incorrect dependency",
                "created_by": "reviewer",
                "target_ref": "knowledge-other-tenant",
            },
        ),
    ],
)
def test_knowledge_tenant_denial_precedes_every_store_dependency(method, endpoint, payload, tmp_path):
    signals_path = tmp_path / "signals.db"
    history_path = tmp_path / "history.db"
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            signals_db_path=str(signals_path),
            history_db_path=str(history_path),
        )
    )

    response = TestClient(app).request(
        method,
        endpoint,
        headers={"X-Tacit-Tenant": "tenant-b"},
        json=payload,
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Tenant access denied"
    assert not signals_path.exists()
    assert not history_path.exists()


def test_denied_auto_approval_does_not_initialize_signal_persistence(tmp_path):
    database_path = tmp_path / "signals.db"
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.apply",
            signals_db_path=str(database_path),
        )
    )

    response = TestClient(app).post(
        "/api/v1/learn/dashboard",
        json={"dashboard_uid": "restricted-dash", "auto_approve": True},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"
    assert not database_path.exists()


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


def test_api_app_rejects_whitespace_wildcard_tenancy_when_auth_is_disabled():
    with pytest.raises(ValueError, match="Wildcard knowledge tenancy requires API authentication"):
        Settings(
            _env_file=None,
            knowledge_tenant_id="  *  ",
            api_auth_enabled=False,
        )


async def test_api_key_dependency_fails_closed_for_unauthenticated_wildcard_tenancy():
    payload = Settings(_env_file=None).model_dump()
    payload.update(knowledge_tenant_id="*", api_auth_enabled=False)
    request = _security_request(
        Settings.model_construct(**payload),
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
    request = _security_request(
        Settings(
            _env_file=None,
            knowledge_tenant_id=configured,
            api_auth_enabled=configured == "*",
        ),
        selected,
    )
    if status_code is None:
        assert assert_tenant_access(request, resource_tenant) == resource_tenant
        return
    with pytest.raises(HTTPException) as exc_info:
        assert_tenant_access(request, resource_tenant)
    assert exc_info.value.status_code == status_code


def test_chart_route_uses_app_scoped_pipeline_settings(monkeypatch):
    runtime_settings = Settings(
        pipeline_timeout_seconds=3,
        pipeline_max_concurrent=1,
        grafana_enabled=False,
    )
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
    assert len(seen_settings) == 1
    assert seen_settings[0].pipeline_timeout_seconds == runtime_settings.pipeline_timeout_seconds
    assert seen_settings[0].pipeline_max_concurrent == runtime_settings.pipeline_max_concurrent
    assert len(seen_backend_settings) == 1
    assert seen_backend_settings[0].pipeline_timeout_seconds == runtime_settings.pipeline_timeout_seconds
    assert seen_backend_settings[0].pipeline_max_concurrent == runtime_settings.pipeline_max_concurrent


def test_api_backend_factory_is_declared_and_lazy(monkeypatch, tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        grafana_enabled=False,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    calls: list[Settings] = []

    def fake_get_active_backends(settings_arg: Settings):
        calls.append(settings_arg)
        return []

    monkeypatch.setattr(pipeline_mod, "get_active_backends", fake_get_active_backends)
    request = Request({"type": "http", "app": app, "headers": []})

    dependencies = get_pipeline_dependencies(request)

    assert calls == []
    ownership = get_runtime_factory_ownership(
        dependencies.backend_factory,
        expected_kind="backend:dashboard",
    )
    assert ownership.settings_identity == dependencies.runtime_ownership.settings_identity
    assert dependencies.backend_factory() == []
    assert calls == [runtime_settings]


@pytest.mark.parametrize("endpoint", ["/api/v1/chart", "/api/v1/chart/stream"])
def test_chart_authorization_precedes_pipeline_dependency_construction(endpoint):
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read",
        )
    )
    dependency_calls = 0

    def fail_if_dependencies_are_built():
        nonlocal dependency_calls
        dependency_calls += 1
        raise AssertionError("pipeline dependencies were built before authorization")

    app.dependency_overrides[get_pipeline_dependencies] = fail_if_dependencies_are_built

    response = TestClient(app).post(endpoint, json={"prompt": "checkout latency"})

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.apply"
    assert dependency_calls == 0


@pytest.mark.parametrize("endpoint", ["/api/v1/chart", "/api/v1/chart/stream"])
def test_chart_exception_exposes_persisted_run_identity(endpoint, monkeypatch):
    app = create_app(runtime_settings=Settings(_env_file=None))

    async def fail_with_run_identity(_request, _deps):
        raise PipelineExecutionError(
            "backend failed",
            investigation_id="inv-failed",
            investigation_run_id="run-failed",
            audit_status="run_created",
        )

    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", fail_with_run_identity)
    response = TestClient(app).post(endpoint, json={"prompt": "checkout latency"})

    expected_status = 500 if endpoint.endswith("/chart") else 200
    assert response.status_code == expected_status
    if endpoint.endswith("/stream"):
        payload = json.loads(response.text.split("data: ", 1)[1].split("\n\n", 1)[0])
    else:
        payload = response.json()
    assert payload == {
        "detail": "Failed to generate dashboard",
        "investigation_id": "inv-failed",
        "investigation_run_id": "run-failed",
        "investigation_status": "failed",
        "audit_status": "run_created",
    }


@pytest.mark.parametrize("endpoint", ["/api/v1/chart", "/api/v1/chart/stream"])
def test_chart_admission_rejection_has_a_stable_overload_response(endpoint, monkeypatch):
    app = create_app(runtime_settings=Settings(_env_file=None))

    async def reject_overload(_request, _deps):
        raise PipelineAdmissionRejected("pipeline_admission_queue_full")

    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", reject_overload)
    response = TestClient(app).post(endpoint, json={"prompt": "checkout latency"})

    expected_status = 503 if endpoint.endswith("/chart") else 200
    assert response.status_code == expected_status
    if endpoint.endswith("/stream"):
        payload = json.loads(response.text.split("data: ", 1)[1].split("\n\n", 1)[0])
    else:
        payload = response.json()
    assert payload == PipelineAdmissionRejected("pipeline_admission_queue_full").public_payload()


def test_ownerless_injected_signal_store_cannot_fall_back_globally():
    injected_store = object()
    deps = _isolated_dependencies(
        settings=Settings(),
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        signal_store_factory=lambda: injected_store,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(RuntimeOwnershipError, match="public runtime ownership descriptor"):
        resolve_knowledge_service(deps, signal_store=injected_store)


def test_production_pipeline_builder_requires_an_explicit_runtime_store_owner(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    factory_calls = 0

    def history_factory():
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("history factory ran without a composition owner")

    with pytest.raises(RuntimeOwnershipError, match="explicit RuntimeStores owner"):
        build_pipeline_dependencies(
            runtime_settings,
            history_store_factory=history_factory,
        )

    assert factory_calls == 0
    assert not tmp_path.joinpath("history.db").exists()
    assert not tmp_path.joinpath("feedback.db").exists()
    assert not tmp_path.joinpath("signals.db").exists()


@pytest.mark.parametrize(
    ("role", "factory_field"),
    [
        ("history", "history_store_factory"),
        ("feedback", "feedback_store_factory"),
    ],
)
@pytest.mark.parametrize(
    "mismatch",
    ["settings", "tenant", "permission", "database", "role"],
)
def test_realized_pipeline_store_must_match_settings_tenant_role_and_database(
    tmp_path,
    role: str,
    factory_field: str,
    mismatch: str,
):
    from tacit.runtime_stores import RuntimeStores

    active_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        history_db_path=str(tmp_path / "active-history.db"),
        feedback_db_path=str(tmp_path / "active-feedback.db"),
        signals_db_path=str(tmp_path / "active-signals.db"),
    )
    updates: dict[str, str] = {}
    if mismatch == "settings":
        updates["llm_model"] = "foreign-model"
    elif mismatch == "tenant":
        updates["knowledge_tenant_id"] = "tenant-b"
    elif mismatch == "permission":
        updates["knowledge_permissions"] = "knowledge.read"
    elif mismatch == "database":
        updates[f"{role}_db_path"] = str(tmp_path / f"foreign-{role}.db")
    foreign_settings = active_settings.model_copy(update=updates)
    descriptor = runtime_descriptor_for_store(
        component=f"foreign_{role}_store",
        runtime_settings=foreign_settings,
        database_role=role,
        database_path=getattr(foreign_settings, f"{role}_db_path"),
    )
    if mismatch == "role":
        wrong_role = "feedback" if role == "history" else "history"
        descriptor = replace(
            descriptor,
            databases=(
                RuntimeDatabaseIdentity(
                    role=wrong_role,
                    path=getattr(active_settings, f"{role}_db_path"),
                ),
            ),
        )

    class StoreProbe:
        runtime_ownership = descriptor

        def __init__(self) -> None:
            self.method_calls = 0

        def start(self, *_args, **_kwargs):
            self.method_calls += 1
            raise AssertionError("mismatched store reached pipeline use")

        def record_provenance(self, **_kwargs):
            self.method_calls += 1
            raise AssertionError("mismatched store reached pipeline use")

    probe = StoreProbe()
    declared_factory = _owned_test_factory(
        lambda: probe,
        runtime_settings=active_settings,
        factory_kind=f"store:{role}",
    )
    dependencies = build_pipeline_dependencies(
        active_settings,
        stores=RuntimeStores(active_settings),
        **{factory_field: declared_factory},
    )

    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        getattr(dependencies, factory_field)()

    assert probe.method_calls == 0


@pytest.mark.parametrize("role", ["history", "feedback"])
def test_ownerless_realized_pipeline_store_fails_before_use(tmp_path, role: str):
    from tacit.runtime_stores import RuntimeStores

    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    field = f"{role}_store_factory"
    declared_factory = _owned_test_factory(
        lambda: object(),
        runtime_settings=runtime_settings,
        factory_kind=f"store:{role}",
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        **{field: declared_factory},
    )

    with pytest.raises(RuntimeOwnershipError, match="public runtime ownership descriptor"):
        getattr(dependencies, field)()


def test_isolated_dependencies_install_explicit_settings_bound_provider_factories(tmp_path, monkeypatch):
    def forbidden_context_registry(_settings):
        raise AssertionError("disabled context consulted the provider registry")

    monkeypatch.setattr("tacit.context.registry.create_context_provider", forbidden_context_registry)
    dependencies = _isolated_dependencies(
        settings=Settings(
            _env_file=None,
            context_provider="none",
            history_db_path=str(tmp_path / "history.db"),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        ),
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    assert dependencies.llm_provider_factory is not None
    assert dependencies.context_provider_factory is not None
    assert dependencies.context_provider_factory() is None


@pytest.mark.parametrize(
    ("missing_capability", "message"),
    [
        ("llm", "explicit LLM provider factory"),
        ("context", "explicit context provider factory"),
    ],
)
def test_direct_dependencies_require_explicit_provider_capabilities(
    tmp_path,
    missing_capability: str,
    message: str,
):
    from tacit.runtime_stores import RuntimeStores

    runtime_settings = Settings(
        _env_file=None,
        context_provider="none",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    stores = RuntimeStores(runtime_settings)

    class ProviderProbe(LLMProvider):
        def __init__(self) -> None:
            super().__init__(runtime_settings, component="direct_test_llm_provider")

        async def chat_json(self, *_args, **_kwargs):
            raise AssertionError("provider should not be used during construction")

        async def chat_text(self, *_args, **_kwargs):
            raise AssertionError("provider should not be used during construction")

    values = {
        "settings": runtime_settings,
        "backend_factory": _owned_test_factory(
            lambda: [],
            runtime_settings=runtime_settings,
            factory_kind="backend:dashboard",
        ),
        "history_store_factory": _owned_test_factory(
            stores.history,
            runtime_settings=runtime_settings,
            factory_kind="store:history",
        ),
        "feedback_store_factory": _owned_test_factory(
            stores.feedback,
            runtime_settings=runtime_settings,
            factory_kind="store:feedback",
        ),
        "llm_cache": stores.llm_cache(),
        "cache_key_factory": lambda *parts: ":".join(parts),
        "pipeline_admission": stores.pipeline_admission(),
        "runtime_ownership": stores.runtime_ownership,
        "llm_provider_factory": (
            None
            if missing_capability == "llm"
            else _owned_test_factory(
                ProviderProbe,
                runtime_settings=runtime_settings,
                factory_kind="provider:llm",
            )
        ),
        "context_provider_factory": (
            None
            if missing_capability == "context"
            else _owned_test_factory(
                lambda: None,
                runtime_settings=runtime_settings,
                factory_kind="provider:context",
            )
        ),
    }

    with pytest.raises(RuntimeOwnershipError, match=message):
        PipelineDependencies(**values)


@pytest.mark.asyncio
async def test_isolated_dependency_rejects_a_mismatched_llm_provider_before_agent_use(tmp_path):
    active_settings = Settings(
        _env_file=None,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    foreign_settings = active_settings.model_copy(update={"llm_api_base": "http://127.0.0.1:11435"})

    class ProviderProbe(LLMProvider):
        def __init__(self) -> None:
            super().__init__(foreign_settings, component="foreign_llm_provider")
            self.calls = 0

        async def chat_json(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("mismatched provider reached agent use")

        async def chat_text(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("mismatched provider reached agent use")

        async def close(self):
            return None

    probe = ProviderProbe()
    dependencies = _isolated_dependencies(
        settings=active_settings,
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=lambda: probe,
        context_provider_factory=lambda: None,
    )

    assert dependencies.llm_provider_factory is not None
    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        await dependencies.acquire_resources()

    assert probe.calls == 0


@pytest.mark.asyncio
async def test_isolated_dependency_rejects_an_ownerless_llm_provider_before_agent_use(tmp_path):
    class OwnerlessProvider(LLMProvider):
        async def chat_json(self, *_args, **_kwargs):
            raise AssertionError("ownerless provider reached agent use")

        async def chat_text(self, *_args, **_kwargs):
            raise AssertionError("ownerless provider reached agent use")

    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    dependencies = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=OwnerlessProvider,
        context_provider_factory=lambda: None,
    )

    assert dependencies.llm_provider_factory is not None
    with pytest.raises(RuntimeOwnershipError, match="public runtime ownership descriptor"):
        await dependencies.acquire_resources()


def test_isolated_dependency_rejects_a_mismatched_context_provider_before_query(tmp_path):
    active_settings = Settings(
        _env_file=None,
        context_provider="mcp",
        context_mcp_server_url="http://127.0.0.1:8765",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    foreign_settings = active_settings.model_copy(update={"context_mcp_server_url": "http://127.0.0.1:9876"})

    class ContextProbe(ContextProvider):
        def __init__(self) -> None:
            super().__init__(foreign_settings, component="foreign_context_provider")
            self.calls = 0

        @property
        def name(self) -> str:
            return "probe"

        async def query(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("mismatched context provider reached query")

    probe = ContextProbe()
    dependencies = _isolated_dependencies(
        settings=active_settings,
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        context_provider_factory=lambda: probe,
    )

    assert dependencies.context_provider_factory is not None
    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        dependencies.context_provider_factory()

    assert probe.calls == 0


def test_pipeline_knowledge_uses_descriptor_only_signal_store_without_global_fallback(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "descriptor-signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(database_path),
        knowledge_tenant_id="tenant-a",
    )
    private_accesses: list[str] = []

    class DescriptorOnlySignalStore:
        def __init__(self):
            self.runtime_settings = runtime_settings
            self.runtime_ownership = runtime_descriptor_for_store(
                component="descriptor-only-signal-store",
                runtime_settings=runtime_settings,
                database_role="signals",
                database_path=database_path,
            )

        def __getattr__(self, name: str):
            if name == "database_path":
                raise AttributeError(name)
            if name.startswith("_"):
                private_accesses.append(name)
                raise AssertionError(f"private ownership probe: {name}")
            raise AttributeError(name)

    injected = DescriptorOnlySignalStore()

    def forbidden_global_store():
        raise AssertionError("descriptor-owned dependency consulted the process-global signal store")

    monkeypatch.setattr("tacit.signals.get_signal_store", forbidden_global_store)
    from tacit.runtime_stores import RuntimeStores

    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        signal_store_factory=_owned_test_factory(
            lambda: injected,
            runtime_settings=runtime_settings,
            factory_kind="store:signals",
        ),
    )
    assert dependencies.knowledge_service_factory is not None

    factory_service = dependencies.knowledge_service_factory()
    direct_dependencies = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        signal_store_factory=lambda: injected,
        knowledge_service_factory=None,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )
    direct_service = resolve_knowledge_service(direct_dependencies, signal_store=injected)

    assert factory_service.database_path == database_path
    assert direct_service.database_path == database_path
    assert private_accesses == []


def test_pipeline_knowledge_preserves_explicit_unavailable_store_without_global_fallback(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "must-not-exist" / "signals.db"
    runtime_settings = Settings(_env_file=None, signals_db_path=str(database_path))

    class UnavailableSignalStore:
        def __init__(self):
            self.runtime_settings = runtime_settings
            self.runtime_ownership = RuntimeOwnershipDescriptor.unavailable(
                component="unavailable-signal-store",
                reason="initialization_failed",
            )

    def forbidden_global_store():
        raise AssertionError("explicitly unavailable dependency consulted the process-global signal store")

    monkeypatch.setattr("tacit.signals.get_signal_store", forbidden_global_store)
    dependencies = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [],
        history_store_factory=lambda: object(),
        feedback_store_factory=lambda: object(),
        signal_store_factory=lambda: UnavailableSignalStore(),
        knowledge_service_factory=None,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(RuntimeOwnershipError, match="explicitly unavailable"):
        resolve_knowledge_service(dependencies)

    assert not database_path.parent.exists()


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


@pytest.mark.parametrize("endpoint", ["/api/v1/chart", "/api/v1/chart/stream"])
def test_chart_routes_use_authenticated_credential_as_requester(endpoint, monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        api_auth_enabled=True,
        api_auth_key="tenant-a-secret",
        knowledge_tenant_id="tenant-a",
    )
    captured: list[DashRequest] = []

    async def fake_run_pipeline(request: DashRequest, _deps):
        captured.append(request)
        return DashResponse(
            dashboard_url="http://dash",
            dashboard_uid="dash-actor",
            panel_count=0,
            summary=request.user_id,
        )

    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", fake_run_pipeline)
    response = TestClient(create_app(runtime_settings=runtime_settings)).post(
        endpoint,
        headers={"X-API-Key": "tenant-a-secret"},
        json={"prompt": "checkout latency", "user_id": "spoofed-operator"},
    )

    assert response.status_code == 200
    assert captured[0].user_id == "api-key:tenant-a"
    assert captured[0].user_id != "spoofed-operator"


def test_feedback_route_uses_authenticated_credential_as_reviewer():
    runtime_settings = Settings(
        _env_file=None,
        api_auth_enabled=True,
        api_auth_key="tenant-a-secret",
        knowledge_tenant_id="tenant-a",
    )

    class CapturingFeedbackStore:
        reviewer = ""

        def submit_feedback(self, **kwargs):
            self.reviewer = kwargs["reviewer"]
            return 1

    store = CapturingFeedbackStore()
    app = create_app(runtime_settings=runtime_settings)
    app.dependency_overrides[get_feedback_store] = lambda: store

    response = TestClient(app).post(
        "/api/v1/feedback",
        headers={"X-API-Key": "tenant-a-secret"},
        json={"dashboard_uid": "dashboard-1", "reviewer": "spoofed-reviewer"},
    )

    assert response.status_code == 200
    assert store.reviewer == "api-key:tenant-a"


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
            api_auth_enabled=True,
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
        api_auth_enabled=True,
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
    assert len(seen_settings) == 1
    assert seen_settings[0].grafana_url == runtime_settings.grafana_url
    assert seen_settings[0].signals_db_path == str((tmp_path / "signals.db").resolve())


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
    assert seen_settings == [app.state.runtime_stores.settings]


@pytest.mark.parametrize(
    ("permissions", "missing_permission"),
    [
        ("", "knowledge.read"),
        ("knowledge.read", "knowledge.review"),
        ("knowledge.read,knowledge.review", "knowledge.trust"),
        ("knowledge.read,knowledge.review,knowledge.trust", "knowledge.apply"),
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


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        ("/api/v1/learn/dashboard", {"dashboard_uid": "read-protected"}),
        (
            "/api/v1/learn/alerts",
            {"alert_uid": "read-protected", "backend": "grafana", "dry_run": True},
        ),
        (
            "/api/v1/learn/dashboard/json",
            {"vendor": "grafana", "source_name": "read-protected.json", "dashboard": {}},
        ),
        ("/api/v1/learn/grafana?limit=1", None),
        ("/api/v1/learn/backends/grafana/alerts?dry_run=true&limit=1", None),
        ("/api/v1/learn/dashboards/read-protected/approve?backend=grafana", None),
    ],
)
def test_signal_learning_requires_read_before_store_construction(path, payload):
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.review,knowledge.trust,knowledge.apply",
        )
    )
    store_constructed = False

    def forbidden_store_factory():
        nonlocal store_constructed
        store_constructed = True
        raise AssertionError("learning store constructed before read authorization")

    app.dependency_overrides[get_signal_store_factory] = lambda: forbidden_store_factory
    response = TestClient(app).post(path, json=payload)

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"
    assert store_constructed is False


@pytest.mark.parametrize("endpoint", ["dashboard", "dashboard/json"])
def test_dashboard_ingestion_conflicts_return_409(endpoint, monkeypatch):
    from tacit.dashboard_ingest import DashboardReviewConflictError

    app = create_app(runtime_settings=Settings(_env_file=None))
    app.dependency_overrides[get_signal_store_factory] = lambda: lambda: object()

    if endpoint == "dashboard":

        async def conflicting_ingest(**_kwargs):
            raise DashboardReviewConflictError("Dashboard generation changed")

        monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard", conflicting_ingest)
        payload = {"dashboard_uid": "racing-dashboard", "auto_approve": True}
    else:

        async def conflicting_ingest(_features, **_kwargs):
            raise DashboardReviewConflictError("Dashboard generation changed")

        monkeypatch.setattr("tacit.dashboard_uploads.parse_uploaded_dashboard", lambda *_args, **_kwargs: object())
        monkeypatch.setattr("tacit.dashboard_ingest.ingest_dashboard_features", conflicting_ingest)
        payload = {
            "vendor": "grafana",
            "source_name": "racing.json",
            "dashboard": {},
            "auto_approve": True,
        }

    response = TestClient(app).post(f"/api/v1/learn/{endpoint}", json=payload)

    assert response.status_code == 409
    assert response.json()["detail"] == "Dashboard generation changed"


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
            knowledge_permissions="knowledge.read,knowledge.apply",
        )
    )

    response = TestClient(app).post(
        f"/api/v1/learn/{endpoint}",
        json={"title": "Restricted artifact", "body_text": "checkout depends on redis"},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.review"


@pytest.mark.parametrize("endpoint", ["runbooks", "incidents"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_artifact_learning_requires_read_permission_before_processing(endpoint, dry_run, tmp_path):
    database_path = tmp_path / f"{endpoint}.db"
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.review,knowledge.apply",
            signals_db_path=str(database_path),
        )
    )

    response = TestClient(app).post(
        f"/api/v1/learn/{endpoint}",
        json={
            "title": "Restricted artifact",
            "body_text": "checkout depends on redis",
            "dry_run": dry_run,
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"
    assert not database_path.exists()


@pytest.mark.parametrize(
    ("permissions", "missing_permission"),
    [
        ("knowledge.read", "knowledge.review"),
        ("knowledge.read,knowledge.review", "knowledge.trust"),
        ("knowledge.read,knowledge.review,knowledge.trust", "knowledge.apply"),
    ],
)
def test_manual_signal_teaching_requires_all_permissions_before_transaction(permissions, missing_permission):
    app = create_app(runtime_settings=Settings(knowledge_permissions=permissions))
    app.dependency_overrides[get_signal_store] = lambda: object()

    def fail_if_transaction_starts():
        raise AssertionError("signal teaching transaction started before authorization")

    app.dependency_overrides[get_knowledge_service] = lambda: SimpleNamespace(
        repository=SimpleNamespace(transaction=fail_if_transaction_starts)
    )
    client = TestClient(app)

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
        runtime_settings=Settings(
            _env_file=None,
            knowledge_tenant_id="*",
            api_auth_enabled=True,
        ),
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
    assert client.get("/api/v1/learn/runbooks", params={"offset": 10_001}).status_code == 422
    with pytest.raises(ValueError, match="artifact page bounds"):
        store.list_learned_artifacts_page(
            tenant_id="default",
            artifact_type="runbook",
            offset=10_001,
        )

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
    assert len(seen_settings) == 1
    assert seen_settings[0].grafana_url == runtime_settings.grafana_url
    assert seen_settings[0].adapter_max_concurrent == runtime_settings.adapter_max_concurrent
    assert seen_settings[0].signals_db_path == str((tmp_path / "signals.db").resolve())


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


@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        (
            "/api/v1/learn/runbooks",
            {
                "title": "Checkout recovery",
                "body_text": "Check checkout_latency_seconds.",
                "dry_run": True,
            },
        ),
        (
            "/api/v1/learn/incidents",
            {
                "title": "Checkout incident",
                "body_text": "Observed checkout_latency_seconds.",
                "dry_run": True,
            },
        ),
    ],
)
def test_artifact_learning_preserves_pinned_tenant_denials(endpoint, payload):
    app = create_app(runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"))

    response = TestClient(app, raise_server_exceptions=False).post(
        endpoint,
        headers={"X-Tacit-Tenant": "tenant-b"},
        json=payload,
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Tenant access denied"


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
