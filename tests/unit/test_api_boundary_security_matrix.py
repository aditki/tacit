from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from fastapi import Response
from fastapi.responses import StreamingResponse

from tacit.api.app import create_app
from tacit.api.dependencies import get_pipeline_dependencies, get_signal_store_factory
from tacit.config import (
    API_MAX_REQUEST_BODY_BYTES_MIN,
    Settings,
    canonical_cors_allowed_origins,
)
from tests.http_client import TestClient

ASGIMessage = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]
REPOSITORY_ROOT = Path(__file__).parents[2]


def _settings(**values: Any) -> Settings:
    return Settings(**{"_env_file": None, **values})


def test_authoritative_browser_handoff_docs_require_same_tab_hidden_frame_protocol() -> None:
    documents = [
        (REPOSITORY_ROOT / "docs" / "foundation-invariant-matrix.md").read_text(encoding="utf-8"),
        (REPOSITORY_ROOT / "docs" / "engineering-design-notes.md").read_text(encoding="utf-8"),
    ]

    for document in documents:
        normalized = document.casefold()
        assert "same-tab" in normalized
        assert "hidden frame" in normalized or "hidden-frame" in normalized
        assert re.search(r"post-promotion\s+acknowledgment", normalized)
        assert re.search(r"final\s+browser-commit\s+acknowledgment", normalized)
        assert "indeterminate" in normalized
        assert "pagehide" in normalized or "same-origin navigation" in normalized
        assert "two-window protocol" not in normalized
        assert "intended popup" not in normalized
        assert "popup `windowproxy`" not in normalized
        assert "popup" not in normalized


def _http_scope(
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
    method: str = "POST",
    path: str = "/side-effect",
) -> dict[str, Any]:
    request_headers = list(headers or [])
    if not any(name.lower() == b"host" for name, _value in request_headers):
        request_headers.append((b"host", b"testserver"))
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": "",
        "headers": request_headers,
        "client": ("test-client", 50000),
        "server": ("test-server", 80),
        "state": {},
    }


async def _drive_asgi(
    app: Any,
    *,
    incoming: list[ASGIMessage],
    headers: list[tuple[bytes, bytes]] | None = None,
    method: str = "POST",
    path: str = "/side-effect",
    scheme: str = "http",
) -> tuple[list[ASGIMessage], int]:
    messages = iter(incoming)
    sent: list[ASGIMessage] = []
    receive_calls = 0

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        return next(messages, {"type": "http.disconnect"})

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    scope = _http_scope(headers=headers, method=method, path=path)
    scope["scheme"] = scheme
    await app(scope, receive, send)
    return sent, receive_calls


def _response(sent: list[ASGIMessage]) -> tuple[int, bytes]:
    start = next(message for message in sent if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in sent if message["type"] == "http.response.body")
    return int(start["status"]), body


def _bodyless_side_effect_app(*, limit: int) -> tuple[Any, list[str]]:
    app = create_app(
        runtime_settings=_settings(api_max_request_body_bytes=limit),
        lifespan=None,
        include_default_routes=False,
    )
    effects: list[str] = []

    @app.post("/side-effect", status_code=204)
    async def side_effect() -> Response:
        effects.append("committed")
        return Response(status_code=204)

    return app, effects


def _browser_mutation_boundary_app(
    *,
    cors_origins: str = "",
) -> tuple[Any, dict[str, int]]:
    app = create_app(
        runtime_settings=_settings(api_cors_allowed_origins=cors_origins),
        lifespan=None,
        include_default_routes=False,
    )
    effects = {"reload": 0, "store": 0, "pipeline": 0, "remote": 0}

    def mutation(effect: str) -> Callable[[], Awaitable[Response]]:
        async def invoke() -> Response:
            effects[effect] += 1
            return Response(status_code=204)

        return invoke

    for path, effect in (
        ("/api/v1/archetypes/reload", "reload"),
        ("/api/v1/investigations/inv-1/refresh", "store"),
        ("/api/v1/chart", "pipeline"),
        ("/api/v1/learn/grafana", "remote"),
    ):
        app.add_api_route(path, mutation(effect), methods=["POST"], status_code=204)
    return app, effects


@pytest.mark.parametrize(
    ("path", "content_type"),
    [
        ("/api/v1/archetypes/reload", "application/x-www-form-urlencoded"),
        ("/api/v1/investigations/inv-1/refresh", "text/plain"),
        ("/api/v1/chart", "application/x-www-form-urlencoded"),
        ("/api/v1/learn/grafana", "text/plain"),
    ],
    ids=["reload", "store", "pipeline", "remote"],
)
async def test_hostile_simple_browser_mutations_stop_before_every_side_effect_boundary(
    path: str,
    content_type: str,
) -> None:
    app, effects = _browser_mutation_boundary_app(cors_origins="https://console.example")
    body = b"action=mutate"

    sent, receive_calls = await _drive_asgi(
        app,
        path=path,
        headers=[
            (b"origin", b"https://attacker.invalid"),
            (b"content-type", content_type.encode("ascii")),
            (b"content-length", str(len(body)).encode("ascii")),
        ],
        incoming=[{"type": "http.request", "body": body, "more_body": False}],
    )

    assert _response(sent) == (403, b'{"detail":"Cross-origin mutation is not allowed"}')
    assert receive_calls == 0
    assert effects == {"reload": 0, "store": 0, "pipeline": 0, "remote": 0}
    start = next(message for message in sent if message["type"] == "http.response.start")
    assert [value for name, value in start["headers"] if name.lower() == b"connection"] == [b"close"]
    assert all(name.lower() != b"access-control-allow-origin" for name, _value in start["headers"])
    encoded_response = repr(sent).encode("utf-8")
    assert len(_response(sent)[1]) <= 128
    assert b"attacker.invalid" not in encoded_response
    assert b"console.example" not in encoded_response
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 0
    assert snapshot.reserved_bytes == 0
    assert snapshot.active_tenant_partitions == 0


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
async def test_hostile_origin_rejects_every_state_changing_method_before_receive(method: str) -> None:
    app = create_app(
        runtime_settings=_settings(),
        lifespan=None,
        include_default_routes=False,
    )
    effects: list[str] = []

    async def mutate() -> Response:
        effects.append(method)
        return Response(status_code=204)

    app.add_api_route("/mutation", mutate, methods=[method], status_code=204)

    sent, receive_calls = await _drive_asgi(
        app,
        method=method,
        path="/mutation",
        headers=[(b"origin", b"https://attacker.invalid")],
        incoming=[{"type": "http.request", "body": b"", "more_body": False}],
    )

    assert _response(sent)[0] == 403
    assert receive_calls == 0
    assert effects == []


@pytest.mark.parametrize(
    ("origin", "cors_origins", "expected_allow_origin"),
    [
        (None, "", None),
        ("http://testserver", "", None),
        ("https://console.example", "https://console.example", "https://console.example"),
    ],
    ids=["non-browser", "same-origin", "configured-cross-origin"],
)
async def test_allowed_mutation_origins_and_non_browser_clients_reach_the_handler(
    origin: str | None,
    cors_origins: str,
    expected_allow_origin: str | None,
) -> None:
    app, effects = _browser_mutation_boundary_app(cors_origins=cors_origins)
    headers = [(b"content-length", b"0")]
    if origin is not None:
        headers.append((b"origin", origin.encode("ascii")))

    sent, _receive_calls = await _drive_asgi(
        app,
        path="/api/v1/chart",
        headers=headers,
        incoming=[{"type": "http.request", "body": b"", "more_body": False}],
    )

    assert _response(sent) == (204, b"")
    assert effects == {"reload": 0, "store": 0, "pipeline": 1, "remote": 0}
    start = next(message for message in sent if message["type"] == "http.response.start")
    allow_origins = [
        value.decode("ascii") for name, value in start["headers"] if name.lower() == b"access-control-allow-origin"
    ]
    assert allow_origins == ([] if expected_allow_origin is None else [expected_allow_origin])
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 0
    assert snapshot.reserved_bytes == 0


async def test_explicit_unauthenticated_wildcard_cors_retains_unsafe_mutation_opt_in() -> None:
    app, effects = _browser_mutation_boundary_app(cors_origins="*")

    sent, _receive_calls = await _drive_asgi(
        app,
        path="/api/v1/chart",
        headers=[
            (b"origin", b"https://attacker.invalid"),
            (b"content-length", b"0"),
        ],
        incoming=[{"type": "http.request", "body": b"", "more_body": False}],
    )

    assert _response(sent) == (204, b"")
    assert effects["pipeline"] == 1
    start = next(message for message in sent if message["type"] == "http.response.start")
    assert [value for name, value in start["headers"] if name.lower() == b"access-control-allow-origin"] == [b"*"]


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/archetypes/reload",
        "/api/v1/chart",
        "/api/v1/learn/grafana",
        "/api/v1/investigations/inv-1/refresh",
    ],
    ids=["reload", "pipeline", "remote-learning", "history-store"],
)
def test_hostile_simple_posts_never_enter_real_mutation_routes(
    monkeypatch: pytest.MonkeyPatch,
    path: str,
) -> None:
    effects: list[str] = []

    def forbidden_pipeline_dependencies() -> None:
        effects.append("pipeline-dependencies")
        raise AssertionError("pipeline dependencies were resolved")

    def forbidden_signal_store_factory() -> None:
        effects.append("signal-store-dependency")
        raise AssertionError("signal-store dependency was resolved")

    async def forbidden_pipeline(*_args: object, **_kwargs: object) -> None:
        effects.append("pipeline")
        raise AssertionError("pipeline was invoked")

    async def forbidden_remote_learning(*_args: object, **_kwargs: object) -> None:
        effects.append("remote")
        raise AssertionError("remote learning was invoked")

    monkeypatch.setattr(
        "tacit.archetypes.templates.reload_archetypes",
        lambda: effects.append("reload"),
    )
    monkeypatch.setattr("tacit.api.routes.dashboard.run_pipeline", forbidden_pipeline)
    monkeypatch.setattr(
        "tacit.dashboard_ingest.learn_backend_dashboards",
        forbidden_remote_learning,
    )
    app = create_app(runtime_settings=_settings(), lifespan=None)
    app.dependency_overrides[get_pipeline_dependencies] = forbidden_pipeline_dependencies
    app.dependency_overrides[get_signal_store_factory] = forbidden_signal_store_factory

    with TestClient(app) as client:
        response = client.post(
            path,
            headers={
                "Origin": "https://attacker.invalid",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            content=b"action=mutate",
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "Cross-origin mutation is not allowed"}
    assert response.headers["connection"] == "close"
    assert effects == []
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 0
    assert snapshot.reserved_bytes == 0


async def test_bodyless_handler_accepts_a_streamed_body_at_the_exact_limit() -> None:
    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    app, effects = _bodyless_side_effect_app(limit=limit)

    sent, _ = await _drive_asgi(
        app,
        headers=[(b"transfer-encoding", b"chunked")],
        incoming=[
            {"type": "http.request", "body": b"x" * limit, "more_body": False},
        ],
    )

    assert _response(sent) == (204, b"")
    assert effects == ["committed"]


@pytest.mark.parametrize(
    ("headers", "incoming"),
    [
        (
            [],
            [
                {
                    "type": "http.request",
                    "body": b"x" * (API_MAX_REQUEST_BODY_BYTES_MIN + 1),
                    "more_body": False,
                }
            ],
        ),
        (
            [(b"transfer-encoding", b"chunked")],
            [
                {
                    "type": "http.request",
                    "body": b"x",
                    "more_body": index < API_MAX_REQUEST_BODY_BYTES_MIN,
                }
                for index in range(API_MAX_REQUEST_BODY_BYTES_MIN + 1)
            ],
        ),
    ],
    ids=["single-limit-plus-one-body", "limit-plus-one-streamed-chunks"],
)
async def test_oversized_body_is_rejected_before_a_bodyless_side_effecting_handler(
    headers: list[tuple[bytes, bytes]],
    incoming: list[ASGIMessage],
) -> None:
    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    app, effects = _bodyless_side_effect_app(limit=limit)

    sent, receive_calls = await _drive_asgi(app, headers=headers, incoming=incoming)

    assert _response(sent) == (413, b'{"detail":"Request body too large"}')
    assert receive_calls == len(incoming)
    assert effects == []


@pytest.mark.parametrize(
    "origin",
    [
        "http://[v1.attacker.example]",
        "https://[vF.future-host.example]:8443",
    ],
)
def test_ipvfuture_bracketed_cors_origins_are_rejected(origin: str) -> None:
    with pytest.raises(ValueError, match="CORS origins"):
        canonical_cors_allowed_origins(origin)

    with pytest.raises(ValueError, match="CORS origins"):
        _settings(api_cors_allowed_origins=origin)


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        ("HTTPS://Console.Example:443/", "https://console.example"),
        ("http://192.0.2.10:80/", "http://192.0.2.10"),
        ("https://[2001:0DB8::1]:8443/", "https://[2001:db8::1]:8443"),
    ],
    ids=["dns", "ipv4", "ipv6"],
)
def test_valid_cors_origins_remain_canonical_and_stable(origin: str, expected: str) -> None:
    assert canonical_cors_allowed_origins(origin) == expected
    assert canonical_cors_allowed_origins(expected) == expected
    assert _settings(api_cors_allowed_origins=origin).api_cors_allowed_origins == expected


def test_authenticated_app_disables_third_party_generated_documentation() -> None:
    app = create_app(
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="test-secret"),
        lifespan=None,
    )

    with TestClient(app) as client:
        assert client.get("/docs").status_code == 404
        assert client.get("/redoc").status_code == 404
        assert client.get("/openapi.json").status_code == 404


def test_authenticated_app_exposes_only_exact_public_bootstrap_and_health_routes() -> None:
    app = create_app(
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="test-secret"),
        lifespan=None,
    )

    with TestClient(app) as client:
        bootstrap = client.get("/")
        health = client.get("/healthz")
        bootstrap_head = client.head("/")
        health_head = client.head("/healthz")
        protected_near_miss = client.get("/healthz/")
        protected_method = client.post("/")

    assert bootstrap.status_code == 200
    assert "tacit-demo-auth-ready-v3" in bootstrap.text
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert bootstrap_head.status_code == bootstrap.status_code
    assert bootstrap_head.content == b""
    assert health_head.status_code == health.status_code
    assert health_head.content == b""
    assert protected_near_miss.status_code == 401
    assert protected_method.status_code == 401


@pytest.mark.parametrize("path", ["/", "/healthz"])
def test_real_anonymous_head_route_bypasses_global_ingress_saturation(path: str) -> None:
    app = create_app(
        runtime_settings=_settings(
            api_auth_enabled=True,
            api_auth_key="test-secret",
            api_request_body_max_concurrent=1,
            api_request_body_tenant_max_concurrent=1,
        ),
        lifespan=None,
    )
    held = app.state.request_body_admission.try_acquire("occupied", 1)
    assert held.lease is not None

    try:
        with TestClient(app) as client:
            get_response = client.get(path)
            head_response = client.head(path)
        saturated = app.state.request_body_admission.snapshot()
    finally:
        held.lease.release()

    assert saturated.active_requests == 1
    assert head_response.status_code == get_response.status_code == 200
    assert head_response.content == b""
    released = app.state.request_body_admission.snapshot()
    assert released.active_requests == 0
    assert released.reserved_bytes == 0


@pytest.mark.parametrize("path", ["/", "/healthz"])
def test_browser_and_api_responses_deny_framing_and_shared_caching(path: str) -> None:
    app = create_app(
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="test-secret"),
        lifespan=None,
    )

    with TestClient(app) as client:
        response = client.get(path)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["content-security-policy"] == "frame-ancestors 'none'"


def test_unhandled_error_response_denies_framing_and_shared_caching() -> None:
    app = create_app(
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="test-secret"),
        lifespan=None,
        include_default_routes=False,
    )

    @app.get("/unhandled")
    async def unhandled() -> None:
        raise RuntimeError("synthetic unhandled failure")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/unhandled", headers={"X-API-Key": "test-secret"})

    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["content-security-policy"] == "frame-ancestors 'none'"
    assert response.headers["connection"] == "close"


def test_unhandled_error_preserves_original_exception_identity_in_tests() -> None:
    app = create_app(
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="test-secret"),
        lifespan=None,
        include_default_routes=False,
    )
    failure = RuntimeError("synthetic identity failure")

    @app.get("/unhandled")
    async def unhandled() -> None:
        raise failure

    with TestClient(app, raise_server_exceptions=True) as client:
        with pytest.raises(RuntimeError) as exc_info:
            client.get("/unhandled", headers={"X-API-Key": "test-secret"})

    assert exc_info.value is failure


def test_security_header_boundary_preserves_sse_response_stream() -> None:
    app = create_app(
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="test-secret"),
        lifespan=None,
        include_default_routes=False,
    )

    async def events():
        yield "data: ready\n\n"
        yield "data: complete\n\n"

    @app.get("/events")
    async def event_stream() -> StreamingResponse:
        return StreamingResponse(events(), media_type="text/event-stream")

    with TestClient(app) as client:
        response = client.get("/events", headers={"X-API-Key": "test-secret"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.text == "data: ready\n\ndata: complete\n\n"
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["content-security-policy"] == "frame-ancestors 'none'"
    admission = app.state.request_body_admission.snapshot()
    assert admission.active_requests == 0
    assert admission.reserved_bytes == 0


async def test_fastapi_eager_body_releases_ingress_before_streaming_response_completes() -> None:
    app = create_app(
        runtime_settings=_settings(
            api_auth_enabled=True,
            api_auth_key="test-secret",
            api_request_body_max_concurrent=1,
            api_request_body_tenant_max_concurrent=1,
        ),
        lifespan=None,
        include_default_routes=False,
    )
    response_started = asyncio.Event()
    finish_stream = asyncio.Event()
    transport_disconnect = asyncio.Event()

    async def events():
        yield b"first-"
        await finish_stream.wait()
        yield b"complete"

    @app.post("/stream")
    async def stream(payload: dict[str, str]) -> StreamingResponse:
        assert payload == {"state": "ready"}
        return StreamingResponse(events(), media_type="text/plain")

    body = b'{"state":"ready"}'
    body_delivered = False
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        nonlocal body_delivered
        if not body_delivered:
            body_delivered = True
            return {"type": "http.request", "body": body, "more_body": False}
        await transport_disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)
        if message["type"] == "http.response.start":
            response_started.set()

    task = asyncio.create_task(
        app(
            _http_scope(
                path="/stream",
                headers=[
                    (b"x-api-key", b"test-secret"),
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            ),
            receive,
            send,
        )
    )
    try:
        await asyncio.wait_for(response_started.wait(), timeout=1)
        assert task.done() is False
        during_stream = app.state.request_body_admission.snapshot()
        assert during_stream.active_requests == 0
        assert during_stream.reserved_bytes == 0

        capacity_probe = app.state.request_body_admission.try_acquire("capacity-probe", 1)
        assert capacity_probe.lease is not None
        capacity_probe.lease.release()
    finally:
        finish_stream.set()
        await asyncio.wait_for(task, timeout=1)

    start = next(message for message in sent if message["type"] == "http.response.start")
    assert [value for name, value in start["headers"] if name.lower() == b"connection"] == []
    assert _response(sent) == (200, b"first-complete")


@pytest.mark.parametrize(
    ("method", "path", "permissions", "expected_status", "missing_permission", "reload_calls"),
    [
        ("get", "/api/v1/archetypes", "", 403, "knowledge.read", 0),
        ("get", "/api/v1/archetypes", "knowledge.read", 200, None, 0),
        (
            "post",
            "/api/v1/archetypes/reload",
            "knowledge.read",
            403,
            "knowledge.override",
            0,
        ),
        (
            "post",
            "/api/v1/archetypes/reload",
            "knowledge.override",
            403,
            "knowledge.read",
            0,
        ),
        (
            "post",
            "/api/v1/archetypes/reload",
            "knowledge.read,knowledge.override",
            200,
            None,
            1,
        ),
    ],
    ids=[
        "list-denied",
        "list-allowed",
        "reload-missing-override",
        "reload-missing-read",
        "reload-allowed",
    ],
)
def test_archetype_route_permission_matrix(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    permissions: str,
    expected_status: int,
    missing_permission: str | None,
    reload_calls: int,
) -> None:
    effects: list[str] = []
    monkeypatch.setattr(
        "tacit.archetypes.templates.reload_archetypes",
        lambda: effects.append("reload"),
    )
    app = create_app(
        runtime_settings=_settings(
            api_auth_enabled=True,
            api_auth_key="test-secret",
            knowledge_permissions=permissions,
        ),
        lifespan=None,
    )

    with TestClient(app) as client:
        response = getattr(client, method)(path, headers={"X-API-Key": "test-secret"})

    assert response.status_code == expected_status
    if missing_permission is not None:
        assert response.json() == {"detail": f"Missing permission: {missing_permission}"}
    assert effects == ["reload"] * reload_calls


def test_refresh_denies_apply_before_pipeline_dependency_side_effects(tmp_path) -> None:
    history_path = tmp_path / "history.db"
    feedback_path = tmp_path / "feedback.db"
    signals_path = tmp_path / "signals.db"
    credential_marker = tmp_path / "credential-plan-captured"
    effects: list[str] = []
    runtime_settings = _settings(
        knowledge_permissions="knowledge.read",
        history_db_path=str(history_path),
        feedback_db_path=str(feedback_path),
        signals_db_path=str(signals_path),
    )
    app = create_app(runtime_settings=runtime_settings, lifespan=None)

    def forbidden_pipeline_dependencies() -> None:
        effects.extend(["credential_read", "provider_factory", "store_initialized"])
        credential_marker.write_text("captured", encoding="utf-8")
        history_path.touch()
        raise AssertionError("pipeline dependencies resolved before authorization")

    app.dependency_overrides[get_pipeline_dependencies] = forbidden_pipeline_dependencies

    response = TestClient(app).post("/api/v1/investigations/inv-denied/refresh")

    assert response.status_code == 403
    assert response.json() == {"detail": "Missing permission: knowledge.apply"}
    assert effects == []
    assert not credential_marker.exists()
    assert not history_path.exists()
    assert not feedback_path.exists()
    assert not signals_path.exists()
