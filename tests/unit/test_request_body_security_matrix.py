from __future__ import annotations

import asyncio
import json
import resource
import socket
import sys
import tracemalloc
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest
import uvicorn
from fastapi import Body, Request
from pydantic import ValidationError
from starlette.responses import StreamingResponse
from structlog.testing import capture_logs

from tacit.api.app import create_app
from tacit.api.request_body_limit import (
    RequestBodyAdmissionController,
    RequestBodyAdmissionReason,
    RequestBodyLimitMiddleware,
)
from tacit.api.routes.system import healthz
from tacit.config import Settings
from tacit.models.schemas import DashRequest

JSON_DECODE_MEMORY_FACTOR = 64

ASGIMessage = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]


def _scope(
    *,
    method: str = "POST",
    path: str = "/consume",
    tenant: str | None = None,
    api_key: str | None = None,
    content_length: int | None = 1,
    content_type: str = "application/octet-stream",
) -> dict[str, Any]:
    headers = [(b"host", b"testserver"), (b"content-type", content_type.encode("ascii"))]
    if tenant is not None:
        headers.append((b"x-tacit-tenant", tenant.encode("ascii")))
    if api_key is not None:
        headers.append((b"x-api-key", api_key.encode("ascii")))
    if content_length is not None:
        headers.append((b"content-length", str(content_length).encode("ascii")))
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
        "headers": headers,
        "client": ("test-client", 50000),
        "server": ("test-server", 80),
        "state": {},
    }


async def _drive(
    app: Any,
    *,
    scope: dict[str, Any],
    incoming: list[ASGIMessage],
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

    await app(scope, receive, send)
    return sent, receive_calls


def _response(sent: list[ASGIMessage]) -> tuple[int, bytes]:
    start = next(message for message in sent if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in sent if message["type"] == "http.response.body")
    return int(start["status"]), body


def _response_header_values(sent: list[ASGIMessage], name: bytes) -> list[bytes]:
    start = next(message for message in sent if message["type"] == "http.response.start")
    return [value for header, value in start.get("headers", []) if header.lower() == name.lower()]


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("condition did not become true")
        await asyncio.sleep(0)


def _assert_complete_stream(sent: list[ASGIMessage], expected_body: bytes) -> None:
    body_messages = [message for message in sent if message["type"] == "http.response.body"]
    assert body_messages
    assert b"".join(message.get("body", b"") for message in body_messages) == expected_body
    assert body_messages[-1].get("more_body", False) is False


async def test_deep_validation_failure_returns_bounded_payload_and_releases_admission() -> None:
    settings = Settings(
        _env_file=None,
        api_max_request_body_bytes=8 * 1_024,
        api_request_body_max_concurrent=1,
        api_request_body_tenant_max_concurrent=1,
    )
    app = create_app(runtime_settings=settings, lifespan=None, include_default_routes=False)
    handler_calls = 0

    @app.post("/consume")
    async def consume(_payload: DashRequest) -> None:
        nonlocal handler_calls
        handler_calls += 1

    marker = b"attacker-secret-marker"
    body = (b"[" * 2_000) + b'"' + marker + b'"' + (b"]" * 2_000)

    with capture_logs() as logs:
        sent, receive_calls = await _drive(
            app,
            scope=_scope(
                content_length=len(body),
                content_type="application/json",
            ),
            incoming=[{"type": "http.request", "body": body, "more_body": False}],
        )

    status, response_body = _response(sent)
    decoded = json.loads(response_body)
    assert status == 422
    assert decoded["detail"]
    assert decoded["detail"][0]["type"]
    assert decoded["detail"][0]["loc"][0] == "body"
    assert len(response_body) <= 2_048
    assert marker not in response_body
    assert b"input" not in response_body
    assert b"ctx" not in response_body
    assert marker.decode() not in str(logs)
    rejection = next(record for record in logs if record["event"] == "request_validation_rejected")
    assert rejection == {
        "event": "request_validation_rejected",
        "log_level": "info",
        "reason_code": "request_validation_failed",
        "error_count": 1,
        "errors_truncated": False,
    }
    assert _response_header_values(sent, b"connection") == [b"close"]
    assert receive_calls == 1
    assert handler_calls == 0
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 0
    assert snapshot.reserved_bytes == 0


def _maximum_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return value if sys.platform == "darwin" else value * 1_024


def _wildcard_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "api_auth_enabled": True,
        "knowledge_tenant_id": "*",
        "knowledge_tenant_api_keys": {
            "tenant-a": "tenant-a-key",
            "tenant-b": "tenant-b-key",
            "tenant-c": "tenant-c-key",
        },
        "api_max_request_body_bytes": 4 * 1_024,
        "api_request_body_max_concurrent": 2,
        "api_request_body_tenant_max_concurrent": 1,
        "api_request_body_max_buffered_bytes": 8 * 1_024 * 1_024,
        "api_request_body_tenant_max_buffered_bytes": 4 * 1_024 * 1_024,
        "api_request_body_memory_amplification_factor": 8,
        "api_request_body_memory_floor_bytes": 4 * 1_024 * 1_024,
        "api_request_body_read_timeout_seconds": 0.1,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


async def test_invalid_credentials_fail_before_body_admission_or_receive() -> None:
    app = create_app(runtime_settings=_wildcard_settings(), include_default_routes=False)
    downstream_calls = 0

    @app.post("/consume")
    async def consume(payload: bytes = Body(media_type="application/octet-stream")) -> None:
        nonlocal downstream_calls
        downstream_calls += 1

    body_allocations = 0
    sent: list[ASGIMessage] = []

    async def receive() -> ASGIMessage:
        nonlocal body_allocations
        body_allocations += 1
        return {"type": "http.request", "body": b"x" * (4 * 1_024), "more_body": False}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await app(
        _scope(tenant="tenant-a", api_key="wrong-key"),
        receive,
        send,
    )

    assert _response(sent) == (401, b'{"detail":"Invalid or missing API key"}')
    assert body_allocations == 0
    assert downstream_calls == 0
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 0
    assert snapshot.reserved_bytes == 0
    assert snapshot.active_tenant_partitions == 0


@pytest.mark.parametrize(
    ("terminal", "expected_status"),
    [
        ("authentication", 401),
        ("host", 400),
        ("cors-preflight", 200),
        ("cors-rejected", 400),
        ("declared-oversize", 413),
        ("global-saturation", 503),
        ("disabled-documentation", 404),
        ("public-body", 400),
    ],
)
async def test_pre_body_terminal_response_matrix_closes_without_receiving(
    terminal: str,
    expected_status: int,
) -> None:
    settings = Settings(
        _env_file=None,
        api_auth_enabled=True,
        api_auth_key="tenant-a-key",
        knowledge_tenant_id="tenant-a",
        api_cors_allowed_origins="https://console.example",
        api_max_request_body_bytes=4 * 1_024,
        api_request_body_max_concurrent=1,
        api_request_body_tenant_max_concurrent=1,
        api_request_body_max_buffered_bytes=8 * 1_024 * 1_024,
        api_request_body_tenant_max_buffered_bytes=8 * 1_024 * 1_024,
        api_request_body_memory_amplification_factor=8,
        api_request_body_memory_floor_bytes=4 * 1_024 * 1_024,
    )
    app = create_app(runtime_settings=settings, lifespan=None, include_default_routes=False)
    held_lease = None

    if terminal == "authentication":
        scope = _scope(tenant="tenant-a", api_key="wrong-key")
    elif terminal == "host":
        scope = _scope(tenant="tenant-a", api_key="tenant-a-key")
        scope["headers"][0] = (b"host", b"attacker.invalid")
    elif terminal in {"cors-preflight", "cors-rejected"}:
        scope = _scope(method="OPTIONS")
        scope["headers"].extend(
            [
                (
                    b"origin",
                    b"https://console.example" if terminal == "cors-preflight" else b"https://attacker.invalid",
                ),
                (b"access-control-request-method", b"POST"),
            ]
        )
    elif terminal == "declared-oversize":
        scope = _scope(
            tenant="tenant-a",
            api_key="tenant-a-key",
            content_length=settings.api_max_request_body_bytes + 1,
        )
    elif terminal == "global-saturation":
        held = app.state.request_body_admission.try_acquire("tenant-b", 1)
        assert held.lease is not None
        held_lease = held.lease
        scope = _scope(tenant="tenant-a", api_key="tenant-a-key")
    elif terminal == "disabled-documentation":
        scope = _scope(method="GET", path="/docs")
    else:
        scope = _scope(method="GET", path="/healthz")

    try:
        sent, receive_calls = await _drive(
            app,
            scope=scope,
            incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
        )
    finally:
        if held_lease is not None:
            held_lease.release()

    assert _response(sent)[0] == expected_status
    assert receive_calls == 0
    assert _response_header_values(sent, b"connection") == [b"close"]


async def test_raw_keep_alive_auth_rejection_closes_without_waiting_for_declared_body() -> None:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_auth_enabled=True,
            api_auth_key="test-secret",
            api_allowed_hosts="127.0.0.1",
        ),
        lifespan=None,
        include_default_routes=False,
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    listener.setblocking(False)
    port = int(listener.getsockname()[1])
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            lifespan="off",
            access_log=False,
            log_level="critical",
            timeout_keep_alive=30,
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    reader: asyncio.StreamReader | None = None
    writer: asyncio.StreamWriter | None = None

    try:
        await _wait_until(lambda: server.started or server_task.done())
        assert server.started
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(
            b"POST /consume HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"X-API-Key: wrong-key\r\n"
            b"Content-Type: application/octet-stream\r\n"
            b"Content-Length: 4096\r\n"
            b"Connection: keep-alive\r\n\r\n"
        )
        await writer.drain()

        raw_headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=1)
        lines = raw_headers[:-4].split(b"\r\n")
        response_headers = {
            name.strip().lower(): value.strip().lower() for name, value in (line.split(b":", 1) for line in lines[1:])
        }
        body = await asyncio.wait_for(
            reader.readexactly(int(response_headers[b"content-length"])),
            timeout=1,
        )
        eof = await asyncio.wait_for(reader.read(1), timeout=0.5)

        assert lines[0] == b"HTTP/1.1 401 Unauthorized"
        assert body == b'{"detail":"Invalid or missing API key"}'
        assert response_headers[b"connection"] == b"close"
        assert eof == b""
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
        server.should_exit = True
        await asyncio.wait_for(server_task, timeout=2)
        listener.close()


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("path", ["/", "/healthz"])
async def test_zero_length_public_request_bypasses_saturated_shared_admission(
    method: str,
    path: str,
) -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    held = controller.try_acquire("tenant-a", 1)
    assert held.lease is not None
    downstream_calls = 0

    async def downstream(_scope: dict[str, Any], _receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    try:
        sent, receive_calls = await _drive(
            RequestBodyLimitMiddleware(
                downstream,
                max_body_bytes=8,
                admission_controller=controller,
                runtime_settings=_wildcard_settings(),
            ),
            scope=_scope(method=method, path=path, content_length=0),
            incoming=[],
        )
    finally:
        held.lease.release()

    assert _response(sent) == (204, b"")
    assert downstream_calls == 1
    assert receive_calls == 0
    assert controller.snapshot().active_requests == 0


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("path", ["/", "/healthz"])
@pytest.mark.parametrize(
    "framing",
    [
        "content-length",
        "transfer-encoding",
        "invalid-content-length",
        "duplicate-content-length",
    ],
)
async def test_body_framed_public_request_is_rejected_before_receive_or_admission(
    method: str,
    path: str,
    framing: str,
) -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    downstream_calls = 0

    async def downstream(_scope: dict[str, Any], _receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    scope = _scope(
        method=method,
        path=path,
        content_length=1 if framing == "content-length" else None,
    )
    if framing == "transfer-encoding":
        scope["headers"].append((b"transfer-encoding", b"chunked"))
    elif framing == "invalid-content-length":
        scope["headers"].append((b"content-length", b"invalid"))
    elif framing == "duplicate-content-length":
        scope["headers"].extend([(b"content-length", b"0"), (b"content-length", b"0")])

    sent, receive_calls = await _drive(
        RequestBodyLimitMiddleware(
            downstream,
            max_body_bytes=8,
            admission_controller=controller,
            runtime_settings=_wildcard_settings(),
        ),
        scope=scope,
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )

    assert _response(sent) == (
        400,
        b"" if method == "HEAD" else b'{"detail":"Request body not allowed"}',
    )
    assert _response_header_values(sent, b"connection") == [b"close"]
    assert downstream_calls == 0
    assert receive_calls == 0
    assert controller.snapshot().active_requests == 0


async def test_body_bearing_public_requests_cannot_starve_the_global_limit() -> None:
    controller = RequestBodyAdmissionController(
        max_concurrent=2,
        max_buffered_bytes=16,
        max_concurrent_per_tenant=2,
        max_buffered_bytes_per_tenant=16,
    )
    release_public = asyncio.Event()
    public_receive_calls = [0, 0]
    public_sent: list[list[ASGIMessage]] = [[], []]

    async def downstream(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await receive()
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=8,
        admission_controller=controller,
        runtime_settings=_wildcard_settings(),
    )

    def public_channel(index: int) -> tuple[ASGIReceive, ASGISend]:
        async def receive() -> ASGIMessage:
            public_receive_calls[index] += 1
            await release_public.wait()
            return {"type": "http.disconnect"}

        async def send(message: ASGIMessage) -> None:
            public_sent[index].append(message)

        return receive, send

    public_tasks = []
    for index in range(2):
        receive, send = public_channel(index)
        public_tasks.append(
            asyncio.create_task(
                middleware(
                    _scope(method="GET", path="/healthz", content_length=1),
                    receive,
                    send,
                )
            )
        )

    protected_sent: list[ASGIMessage] = []
    protected_receive_calls = 0
    try:
        await _wait_until(
            lambda: all(task.done() for task in public_tasks) or controller.snapshot().active_requests == 2
        )
        protected_sent, protected_receive_calls = await _drive(
            middleware,
            scope=_scope(tenant="tenant-a", api_key="tenant-a-key", content_length=1),
            incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
        )
    finally:
        release_public.set()
        await asyncio.wait_for(asyncio.gather(*public_tasks), timeout=1)

    assert _response(protected_sent) == (204, b"")
    assert protected_receive_calls == 1
    assert public_receive_calls == [0, 0]
    assert [_response(sent) for sent in public_sent] == [
        (400, b'{"detail":"Request body not allowed"}'),
        (400, b'{"detail":"Request body not allowed"}'),
    ]
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize(
    ("settings", "tenant", "api_key", "expected"),
    [
        (_wildcard_settings(), None, "tenant-a-key", (400, b'{"detail":"Knowledge tenant is required"}')),
        (_wildcard_settings(), "unknown", "tenant-a-key", (401, b'{"detail":"Invalid or missing API key"}')),
        (
            Settings(
                _env_file=None,
                api_auth_enabled=True,
                api_auth_key="tenant-a-key",
                knowledge_tenant_id="tenant-a",
            ),
            "tenant-b",
            "tenant-a-key",
            (403, b'{"detail":"Tenant access denied"}'),
        ),
    ],
    ids=["wildcard-missing", "wildcard-spoofed", "pinned-mismatch"],
)
async def test_invalid_tenant_headers_cannot_create_admission_partitions(
    settings: Settings,
    tenant: str | None,
    api_key: str,
    expected: tuple[int, bytes],
) -> None:
    app = create_app(runtime_settings=settings, include_default_routes=False)

    sent, receive_calls = await _drive(
        app,
        scope=_scope(tenant=tenant, api_key=api_key),
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )

    assert _response(sent) == expected
    assert receive_calls == 0
    assert app.state.request_body_admission.snapshot().active_tenant_partitions == 0


async def test_duplicate_tenant_headers_fail_before_creating_a_partition() -> None:
    app = create_app(runtime_settings=_wildcard_settings(), include_default_routes=False)
    scope = _scope(tenant="tenant-a", api_key="tenant-a-key")
    scope["headers"].append((b"x-tacit-tenant", b"tenant-b"))

    sent, receive_calls = await _drive(
        app,
        scope=scope,
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )

    assert _response(sent) == (400, b'{"detail":"Invalid authentication headers"}')
    assert receive_calls == 0
    assert app.state.request_body_admission.snapshot().active_tenant_partitions == 0


async def test_tenant_concurrency_limit_does_not_starve_another_authenticated_tenant() -> None:
    app = create_app(runtime_settings=_wildcard_settings(), include_default_routes=False)
    tenant_a_started = asyncio.Event()
    release_tenant_a = asyncio.Event()

    @app.post("/consume")
    async def consume(request: Request, payload: bytes = Body(media_type="application/octet-stream")) -> dict[str, str]:
        tenant = str(request.state.authenticated_tenant)
        if tenant == "tenant-a":
            tenant_a_started.set()
            await release_tenant_a.wait()
        return {"tenant": tenant, "size": str(len(payload))}

    first_sent: list[ASGIMessage] = []

    async def first_receive() -> ASGIMessage:
        return {"type": "http.request", "body": b"a", "more_body": False}

    async def first_send(message: ASGIMessage) -> None:
        first_sent.append(message)

    first = asyncio.create_task(app(_scope(tenant="tenant-a", api_key="tenant-a-key"), first_receive, first_send))
    await asyncio.wait_for(tenant_a_started.wait(), timeout=1)

    same_tenant, same_receive_calls = await _drive(
        app,
        scope=_scope(tenant="tenant-a", api_key="tenant-a-key"),
        incoming=[{"type": "http.request", "body": b"a", "more_body": False}],
    )
    other_tenant, other_receive_calls = await _drive(
        app,
        scope=_scope(tenant="tenant-b", api_key="tenant-b-key"),
        incoming=[{"type": "http.request", "body": b"b", "more_body": False}],
    )

    assert _response(same_tenant) == (503, b'{"detail":"Request body admission unavailable"}')
    assert same_receive_calls == 0
    assert _response(other_tenant) == (200, b'{"tenant":"tenant-b","size":"1"}')
    assert other_receive_calls == 1
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 1
    assert snapshot.active_tenant_partitions == 1
    assert dict(snapshot.rejections_by_reason)[RequestBodyAdmissionReason.TENANT_CONCURRENCY_SATURATED] == 1

    release_tenant_a.set()
    await asyncio.wait_for(first, timeout=1)
    assert _response(first_sent) == (200, b'{"tenant":"tenant-a","size":"1"}')


async def test_global_concurrency_limit_remains_above_tenant_partitions() -> None:
    app = create_app(
        runtime_settings=_wildcard_settings(),
        include_default_routes=False,
    )
    started = {tenant: asyncio.Event() for tenant in ("tenant-a", "tenant-b")}
    release = asyncio.Event()

    @app.post("/consume")
    async def consume(request: Request, payload: bytes = Body(media_type="application/octet-stream")) -> None:
        tenant = str(request.state.authenticated_tenant)
        started[tenant].set()
        await release.wait()

    tasks: list[asyncio.Task[None]] = []
    for tenant in ("tenant-a", "tenant-b"):
        key = f"{tenant}-key"

        async def receive() -> ASGIMessage:
            return {"type": "http.request", "body": b"x", "more_body": False}

        async def send(_message: ASGIMessage) -> None:
            return None

        tasks.append(asyncio.create_task(app(_scope(tenant=tenant, api_key=key), receive, send)))
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started.values()))

    rejected, receive_calls = await _drive(
        app,
        scope=_scope(tenant="tenant-c", api_key="tenant-c-key"),
        incoming=[{"type": "http.request", "body": b"c", "more_body": False}],
    )

    assert _response(rejected) == (503, b'{"detail":"Request body admission unavailable"}')
    assert receive_calls == 0
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 2
    assert snapshot.active_tenant_partitions == 2
    assert dict(snapshot.rejections_by_reason)[RequestBodyAdmissionReason.GLOBAL_CONCURRENCY_SATURATED] == 1

    release.set()
    await asyncio.gather(*tasks)


async def test_auth_disabled_pinned_runtime_can_reach_the_global_ceiling() -> None:
    settings = Settings(
        _env_file=None,
        api_auth_enabled=False,
        knowledge_tenant_id="default",
        api_max_request_body_bytes=4 * 1_024,
        api_request_body_max_concurrent=2,
        api_request_body_tenant_max_concurrent=1,
        api_request_body_max_buffered_bytes=8 * 1_024 * 1_024,
        api_request_body_tenant_max_buffered_bytes=4 * 1_024 * 1_024,
        api_request_body_memory_amplification_factor=8,
        api_request_body_memory_floor_bytes=4 * 1_024 * 1_024,
    )
    app = create_app(runtime_settings=settings, include_default_routes=False)
    started = [asyncio.Event(), asyncio.Event()]
    release = asyncio.Event()
    invocation = 0

    @app.post("/consume")
    async def consume(request: Request, payload: bytes = Body(media_type="application/octet-stream")) -> None:
        nonlocal invocation
        assert request.state.authenticated_actor == "local-unauthenticated"
        assert request.state.authenticated_tenant == "default"
        current = invocation
        invocation += 1
        started[current].set()
        await release.wait()

    tasks: list[asyncio.Task[None]] = []
    for _index in range(2):

        async def receive() -> ASGIMessage:
            return {"type": "http.request", "body": b"x", "more_body": False}

        async def send(_message: ASGIMessage) -> None:
            return None

        tasks.append(asyncio.create_task(app(_scope(), receive, send)))
    await asyncio.gather(*(asyncio.wait_for(event.wait(), timeout=1) for event in started))

    rejected, receive_calls = await _drive(
        app,
        scope=_scope(),
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )

    assert _response(rejected) == (503, b'{"detail":"Request body admission unavailable"}')
    assert receive_calls == 0
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 2
    assert snapshot.max_concurrent_per_tenant == snapshot.max_concurrent == 2
    assert snapshot.max_buffered_bytes_per_tenant == snapshot.max_buffered_bytes == 8 * 1_024 * 1_024
    assert dict(snapshot.rejections_by_reason)[RequestBodyAdmissionReason.GLOBAL_CONCURRENCY_SATURATED] == 1

    release.set()
    await asyncio.gather(*tasks)


def test_tenant_byte_limit_and_release_are_owned_by_the_exact_partition() -> None:
    controller = RequestBodyAdmissionController(
        max_concurrent=4,
        max_buffered_bytes=8 * 1_024,
        max_concurrent_per_tenant=3,
        max_buffered_bytes_per_tenant=2 * 1_024,
        memory_amplification_factor=2,
        memory_floor_bytes=1_024,
    )
    tenant_a = controller.try_acquire("tenant-a", 1_024)
    tenant_b = controller.try_acquire("tenant-b", 1_024)
    assert tenant_a.lease is not None
    assert tenant_b.lease is not None

    tenant_a_rejected = controller.try_acquire("tenant-a", 1)
    assert tenant_a_rejected.reason is RequestBodyAdmissionReason.TENANT_BYTE_BUDGET_SATURATED

    tenant_a.lease.release()
    tenant_a.lease.release()
    tenant_a_recovered = controller.try_acquire("tenant-a", 1)
    tenant_b_still_full = controller.try_acquire("tenant-b", 1)
    assert tenant_a_recovered.lease is not None
    assert tenant_b_still_full.reason is RequestBodyAdmissionReason.TENANT_BYTE_BUDGET_SATURATED

    tenant_a_recovered.lease.release()
    tenant_b.lease.release()
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("terminal", ["cancel", "disconnect", "timeout"])
async def test_request_termination_releases_only_the_owning_tenant_partition(terminal: str) -> None:
    settings = _wildcard_settings(api_request_body_tenant_max_concurrent=1)
    app = create_app(runtime_settings=settings, include_default_routes=False)
    tenant_b = app.state.request_body_admission.try_acquire("tenant-b", 1)
    assert tenant_b.lease is not None
    receive_started = asyncio.Event()
    release_receive = asyncio.Event()

    @app.post("/consume")
    async def consume(payload: bytes = Body(media_type="application/octet-stream")) -> None:
        return None

    async def receive() -> ASGIMessage:
        receive_started.set()
        if terminal == "disconnect":
            return {"type": "http.disconnect"}
        if terminal == "timeout":
            await asyncio.sleep(settings.api_request_body_read_timeout_seconds * 2)
            return {"type": "http.request", "body": b"a", "more_body": False}
        await release_receive.wait()
        return {"type": "http.request", "body": b"a", "more_body": False}

    async def send(_message: ASGIMessage) -> None:
        return None

    task = asyncio.create_task(app(_scope(tenant="tenant-a", api_key="tenant-a-key"), receive, send))
    await asyncio.wait_for(receive_started.wait(), timeout=1)
    if terminal == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        await asyncio.wait_for(task, timeout=1)

    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 1
    assert snapshot.active_tenant_partitions == 1
    tenant_b_still_owned = app.state.request_body_admission.try_acquire("tenant-b", 1)
    assert tenant_b_still_owned.reason is RequestBodyAdmissionReason.TENANT_CONCURRENCY_SATURATED

    recovered, receive_calls = await _drive(
        app,
        scope=_scope(tenant="tenant-a", api_key="tenant-a-key"),
        incoming=[{"type": "http.request", "body": b"a", "more_body": False}],
    )
    assert _response(recovered)[0] == 200
    assert receive_calls == 1
    assert app.state.request_body_admission.snapshot().active_requests == 1
    tenant_b.lease.release()
    assert app.state.request_body_admission.snapshot().active_requests == 0


async def test_unframed_lazy_read_authenticates_before_capacity_or_receive() -> None:
    settings = _wildcard_settings()
    controller = RequestBodyAdmissionController(
        max_concurrent=1,
        max_buffered_bytes=2 * 1_024 * 1_024,
        max_concurrent_per_tenant=1,
        max_buffered_bytes_per_tenant=2 * 1_024 * 1_024,
        memory_amplification_factor=8,
        memory_floor_bytes=2 * 1_024 * 1_024,
    )

    async def downstream(_scope: dict[str, Any], receive: ASGIReceive, _send: ASGISend) -> None:
        await receive()

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=4 * 1_024,
        admission_controller=controller,
        read_timeout_seconds=0.1,
        runtime_settings=settings,
    )
    sent, receive_calls = await _drive(
        middleware,
        scope=_scope(
            method="GET",
            tenant="tenant-a",
            api_key="wrong-key",
            content_length=None,
        ),
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )

    assert _response(sent) == (401, b'{"detail":"Invalid or missing API key"}')
    assert receive_calls == 0
    assert controller.snapshot().active_requests == 0


async def test_starlette_response_first_saturation_precedes_receive_without_truncating_stream() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    held = controller.try_acquire("tenant-b", 1)
    assert held.lease is not None

    async def body():
        yield b"first-"
        await _wait_until(
            lambda: dict(controller.snapshot().rejections_by_reason).get(
                RequestBodyAdmissionReason.GLOBAL_CONCURRENCY_SATURATED,
                0,
            )
            == 1
        )
        yield b"complete"

    response = StreamingResponse(body(), media_type="text/plain")
    receive_calls = 0

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        raise AssertionError("saturated response-first listener read before admission")

    sent: list[ASGIMessage] = []

    middleware = RequestBodyLimitMiddleware(
        response,
        max_body_bytes=8,
        admission_controller=controller,
        read_timeout_seconds=0.01,
        runtime_settings=_wildcard_settings(),
    )

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    try:
        await asyncio.wait_for(
            middleware(
                _scope(method="GET", tenant="tenant-a", api_key="tenant-a-key", content_length=None),
                receive,
                send,
            ),
            timeout=1,
        )
    finally:
        held.lease.release()

    assert receive_calls == 0
    assert _response(sent)[0] == 200
    _assert_complete_stream(sent, b"first-complete")
    assert controller.snapshot().active_requests == 0


async def test_starlette_response_first_limit_plus_one_does_not_truncate_stream() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    incoming = iter(
        [
            {"type": "http.request", "body": b"12345678", "more_body": True},
            {"type": "http.request", "body": b"9", "more_body": False},
        ]
    )
    receive_calls = 0

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        return next(incoming)

    async def body():
        yield b"first-"
        await _wait_until(lambda: receive_calls == 2)
        yield b"complete"

    sent: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            StreamingResponse(body(), media_type="text/plain"),
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.1,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(method="GET", tenant="tenant-a", api_key="tenant-a-key", content_length=None),
            receive,
            send,
        ),
        timeout=1,
    )

    assert receive_calls == 2
    assert _response(sent)[0] == 200
    _assert_complete_stream(sent, b"first-complete")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_starlette_response_first_absolute_timeout_does_not_truncate_stream() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    receive_started = asyncio.Event()

    async def receive() -> ASGIMessage:
        receive_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def body():
        yield b"first-"
        await receive_started.wait()
        await asyncio.sleep(0.03)
        yield b"complete"

    sent: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    with capture_logs() as logs:
        await asyncio.wait_for(
            RequestBodyLimitMiddleware(
                StreamingResponse(body(), media_type="text/plain"),
                max_body_bytes=8,
                admission_controller=controller,
                read_timeout_seconds=0.01,
                runtime_settings=_wildcard_settings(),
            )(
                _scope(method="GET", tenant="tenant-a", api_key="tenant-a-key", content_length=None),
                receive,
                send,
            ),
            timeout=1,
        )

    assert any(record.get("reason_code") == RequestBodyAdmissionReason.READ_TIMEOUT for record in logs)
    assert _response(sent)[0] == 200
    _assert_complete_stream(sent, b"first-complete")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_starlette_response_first_empty_terminal_body_releases_capacity_while_stream_remains_open() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    receive_started = asyncio.Event()
    allow_terminal = asyncio.Event()
    finish_stream = asyncio.Event()
    receive_calls = 0
    active_at_receive = 0

    async def receive() -> ASGIMessage:
        nonlocal active_at_receive, receive_calls
        receive_calls += 1
        if receive_calls == 1:
            active_at_receive = controller.snapshot().active_requests
            receive_started.set()
            await allow_terminal.wait()
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def body():
        yield b"first-"
        await finish_stream.wait()
        yield b"complete"

    sent: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    task = asyncio.create_task(
        RequestBodyLimitMiddleware(
            StreamingResponse(body(), media_type="text/plain"),
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.1,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(method="GET", tenant="tenant-a", api_key="tenant-a-key", content_length=None),
            receive,
            send,
        )
    )
    await asyncio.wait_for(receive_started.wait(), timeout=1)
    assert active_at_receive == 1
    allow_terminal.set()
    await _wait_until(lambda: controller.snapshot().active_requests == 0)
    assert task.done() is False

    finish_stream.set()
    await asyncio.wait_for(task, timeout=1)

    assert receive_calls == 2
    _assert_complete_stream(sent, b"first-complete")
    assert controller.snapshot().reserved_bytes == 0


async def test_starlette_response_first_complete_body_is_consumed_and_response_finishes() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    incoming = iter(
        [
            {"type": "http.request", "body": b"1234", "more_body": True},
            {"type": "http.request", "body": b"5678", "more_body": False},
        ]
    )
    receive_calls = 0

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls <= 2:
            return next(incoming)
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def body():
        yield b"first-"
        await _wait_until(lambda: receive_calls == 3)
        yield b"complete"

    sent: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            StreamingResponse(body(), media_type="text/plain"),
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.1,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(method="GET", tenant="tenant-a", api_key="tenant-a-key", content_length=None),
            receive,
            send,
        ),
        timeout=1,
    )

    assert receive_calls == 3
    _assert_complete_stream(sent, b"first-complete")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_starlette_framed_response_first_replay_hides_body_after_response_start() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []
    raw_receive_calls = 0

    class ObservedStreamingResponse(StreamingResponse):
        async def listen_for_disconnect(self, receive: ASGIReceive) -> None:
            while True:
                message = await receive()
                observed_messages.append(message)
                if message["type"] == "http.disconnect":
                    return

    async def raw_receive() -> ASGIMessage:
        nonlocal raw_receive_calls
        raw_receive_calls += 1
        if raw_receive_calls == 1:
            return {"type": "http.request", "body": b"body", "more_body": False}
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def body():
        yield b"first-"
        await asyncio.sleep(0)
        yield b"complete"

    sent: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            ObservedStreamingResponse(body(), media_type="text/plain"),
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.1,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=4,
            ),
            raw_receive,
            send,
        ),
        timeout=1,
    )

    assert observed_messages == []
    _assert_complete_stream(sent, b"first-complete")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("sender_terminal", ["completed", "cancelled", "failed"])
async def test_terminal_child_response_sender_uses_bounded_parent_disconnect_fallback(
    sender_terminal: str,
) -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []
    sent: list[ASGIMessage] = []
    transport_receive_cancelled = asyncio.Event()

    async def response_from_child(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        async def send_response() -> None:
            await send({"type": "http.response.start", "status": 204, "headers": []})
            if sender_terminal == "cancelled":
                await asyncio.Event().wait()
            if sender_terminal == "failed":
                raise RuntimeError("synthetic sender failure")
            await send({"type": "http.response.body", "body": b""})

        child = asyncio.create_task(send_response())
        await _wait_until(lambda: bool(sent) and sent[0]["type"] == "http.response.start")
        if sender_terminal == "cancelled":
            child.cancel()
        try:
            await child
        except asyncio.CancelledError:
            assert sender_terminal == "cancelled"
        except RuntimeError as exc:
            assert sender_terminal == "failed"
            assert str(exc) == "synthetic sender failure"
        assert child.done()
        observed_messages.append(await receive())

    async def receive() -> ASGIMessage:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            transport_receive_cancelled.set()
            raise
        raise AssertionError("unreachable")

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            response_from_child,
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.01,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=None,
            ),
            receive,
            send,
        ),
        timeout=0.2,
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert transport_receive_cancelled.is_set()
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_cancelling_parent_disconnect_observer_does_not_cancel_live_child_sender() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    sender_started = asyncio.Event()
    release_sender = asyncio.Event()
    receive_started = asyncio.Event()
    transport_receive_cancelled = asyncio.Event()
    child_cancelled = False
    child: asyncio.Task[None] | None = None
    receive_calls = 0

    async def response_from_child(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal child

        async def send_response() -> None:
            nonlocal child_cancelled
            await send({"type": "http.response.start", "status": 204, "headers": []})
            sender_started.set()
            try:
                await release_sender.wait()
            except asyncio.CancelledError:
                child_cancelled = True
                raise
            await send({"type": "http.response.body", "body": b""})

        child = asyncio.create_task(send_response())
        await sender_started.wait()
        await receive()

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls == 1:
            return {"type": "http.request", "body": b"", "more_body": False}
        receive_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            transport_receive_cancelled.set()
            raise
        raise AssertionError("unreachable")

    async def send(_message: ASGIMessage) -> None:
        return

    request = asyncio.create_task(
        RequestBodyLimitMiddleware(
            response_from_child,
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.01,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=0,
            ),
            receive,
            send,
        )
    )
    await asyncio.wait_for(receive_started.wait(), timeout=1)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert child is not None
    assert not child.done()
    assert child_cancelled is False
    assert transport_receive_cancelled.is_set()
    assert receive_calls == 2

    release_sender.set()
    await asyncio.wait_for(child, timeout=1)
    assert child_cancelled is False
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_sequential_framed_response_first_invalid_trailer_has_bounded_disconnect_fallback() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []
    sent: list[ASGIMessage] = []
    receive_calls = 0

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls == 1:
            return {"type": "http.request", "body": b"body", "more_body": False}
        if receive_calls == 2:
            return {"type": "websocket.receive", "bytes": b"invalid"}
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.01,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=4,
            ),
            receive,
            send,
        ),
        timeout=1,
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 3
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_response_first_disconnect_listener_can_observe_disconnect_without_body_admission() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    sent, receive_calls = await _drive(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            runtime_settings=_wildcard_settings(),
        ),
        scope=_scope(
            method="GET",
            tenant="tenant-a",
            api_key="tenant-a-key",
            content_length=None,
        ),
        incoming=[{"type": "http.disconnect"}],
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 1
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("http_version", ["1.0", "1.1"])
@pytest.mark.parametrize(
    "response_headers",
    [
        [],
        [(b"Connection", b"keep-alive"), (b"connection", b"upgrade")],
    ],
    ids=["missing-connection", "replace-conflicting-values"],
)
async def test_http1_response_first_start_before_body_ownership_forces_connection_close(
    http_version: str,
    response_headers: list[tuple[bytes, bytes]],
) -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)

    async def response_first(_scope: dict[str, Any], _receive: ASGIReceive, send: ASGISend) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [*response_headers, (b"x-proof", b"preserved")],
            }
        )
        await send({"type": "http.response.body", "body": b""})

    scope = _scope(
        method="GET",
        tenant="tenant-a",
        api_key="tenant-a-key",
        content_length=None,
    )
    scope["http_version"] = http_version
    sent, receive_calls = await _drive(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            runtime_settings=_wildcard_settings(),
        ),
        scope=scope,
        incoming=[{"type": "http.request", "body": b"unframed", "more_body": False}],
    )

    assert _response(sent) == (204, b"")
    assert receive_calls == 0
    assert _response_header_values(sent, b"connection") == [b"close"]
    assert _response_header_values(sent, b"x-proof") == [b"preserved"]
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_sequential_response_first_normal_terminal_body_has_bounded_disconnect_fallback() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []
    sent: list[ASGIMessage] = []
    receive_calls = 0

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls == 1:
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.01,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=None,
            ),
            receive,
            send,
        ),
        timeout=0.2,
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 2
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_response_first_disconnect_listener_receives_only_disconnect_after_admitted_body() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    sent, receive_calls = await _drive(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            runtime_settings=_wildcard_settings(),
        ),
        scope=_scope(
            method="GET",
            tenant="tenant-a",
            api_key="tenant-a-key",
            content_length=None,
        ),
        incoming=[
            {"type": "http.request", "body": b"1234", "more_body": True},
            {"type": "http.request", "body": b"5678", "more_body": False},
            {"type": "http.disconnect"},
        ],
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 3
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_sequential_response_first_saturation_observes_queued_disconnect() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    held = controller.try_acquire("tenant-b", 1)
    assert held.lease is not None
    observed_messages: list[ASGIMessage] = []

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    try:
        sent, receive_calls = await asyncio.wait_for(
            _drive(
                RequestBodyLimitMiddleware(
                    response_first,
                    max_body_bytes=8,
                    admission_controller=controller,
                    runtime_settings=_wildcard_settings(),
                ),
                scope=_scope(
                    method="GET",
                    tenant="tenant-a",
                    api_key="tenant-a-key",
                    content_length=None,
                ),
                incoming=[{"type": "http.disconnect"}],
            ),
            timeout=1,
        )
    finally:
        held.lease.release()

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 1
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0


@pytest.mark.parametrize(
    "first_message",
    [
        {"type": "http.request", "body": b"123456789", "more_body": False},
        {"type": "websocket.receive", "bytes": b"not-http"},
    ],
    ids=["limit-plus-one", "invalid-message"],
)
async def test_sequential_response_first_terminal_body_failure_observes_queued_disconnect(
    first_message: ASGIMessage,
) -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    sent, receive_calls = await asyncio.wait_for(
        _drive(
            RequestBodyLimitMiddleware(
                response_first,
                max_body_bytes=8,
                admission_controller=controller,
                runtime_settings=_wildcard_settings(),
            ),
            scope=_scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=None,
            ),
            incoming=[first_message, {"type": "http.disconnect"}],
        ),
        timeout=1,
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 2
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_sequential_response_first_timeout_observes_queued_disconnect() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []
    sent: list[ASGIMessage] = []
    receive_calls = 0

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls == 1:
            await asyncio.sleep(1)
            raise AssertionError("timed request-body receive unexpectedly completed")
        return {"type": "http.disconnect"}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.01,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=None,
            ),
            receive,
            send,
        ),
        timeout=1,
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 2
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_sequential_response_first_terminal_failure_has_bounded_disconnect_fallback() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=8)
    observed_messages: list[ASGIMessage] = []
    sent: list[ASGIMessage] = []
    receive_calls = 0

    async def response_first(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await send({"type": "http.response.start", "status": 204, "headers": []})
        observed_messages.append(await receive())
        await send({"type": "http.response.body", "body": b""})

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls == 1:
            return {"type": "http.request", "body": b"123456789", "more_body": False}
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            response_first,
            max_body_bytes=8,
            admission_controller=controller,
            read_timeout_seconds=0.01,
            runtime_settings=_wildcard_settings(),
        )(
            _scope(
                method="GET",
                tenant="tenant-a",
                api_key="tenant-a-key",
                content_length=None,
            ),
            receive,
            send,
        ),
        timeout=1,
    )

    assert observed_messages == [{"type": "http.disconnect"}]
    assert receive_calls == 2
    assert _response(sent) == (204, b"")
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("media_type", ["application/octet-stream", "application/json"])
async def test_memory_envelope_covers_real_framework_peak_allocations(media_type: str) -> None:
    if media_type == "application/json":
        body = b"[" + (b"{}," * 699_049) + b"{}]"
    else:
        body = b"x" * (128 * 1_024)
    maximum_body_bytes = max(512 * 1_024, len(body))
    memory_budget = max(
        16 * 1_024 * 1_024,
        maximum_body_bytes * JSON_DECODE_MEMORY_FACTOR,
    )
    settings = Settings(
        _env_file=None,
        api_max_request_body_bytes=maximum_body_bytes,
        api_request_body_max_concurrent=1,
        api_request_body_tenant_max_concurrent=1,
        api_request_body_max_buffered_bytes=memory_budget,
        api_request_body_tenant_max_buffered_bytes=memory_budget,
        api_request_body_memory_amplification_factor=8,
        api_request_body_memory_floor_bytes=4 * 1_024 * 1_024,
    )
    app = create_app(runtime_settings=settings, include_default_routes=False)
    observed_peak = 0
    reserved_at_handler = 0
    rss_before = _maximum_rss_bytes()

    @app.post("/consume")
    async def consume(request: Request, payload: Any = Body()) -> None:
        nonlocal observed_peak, reserved_at_handler
        _ = payload
        _current, observed_peak = tracemalloc.get_traced_memory()
        reserved_at_handler = request.app.state.request_body_admission.snapshot().reserved_bytes

    tracemalloc.start()
    try:
        sent, receive_calls = await _drive(
            app,
            scope=_scope(
                content_length=len(body),
                content_type=media_type,
            ),
            incoming=[{"type": "http.request", "body": body, "more_body": False}],
        )
    finally:
        tracemalloc.stop()

    assert _response(sent)[0] == 200
    assert receive_calls == 1
    assert reserved_at_handler >= observed_peak
    assert max(0, _maximum_rss_bytes() - rss_before) <= reserved_at_handler
    assert reserved_at_handler <= settings.api_request_body_max_buffered_bytes


@pytest.mark.parametrize(
    "request_limits",
    [
        {
            "api_request_body_max_concurrent": 1,
            "api_request_body_tenant_max_concurrent": 1,
        },
        {
            "api_request_body_max_buffered_bytes": 256 * 1_024 * 1_024,
            "api_request_body_tenant_max_buffered_bytes": 256 * 1_024 * 1_024,
        },
    ],
    ids=["single-global-slot", "tenant-bytes-equal-global"],
)
def test_wildcard_request_body_partitions_must_be_strictly_below_global(
    request_limits: dict[str, int],
) -> None:
    values: dict[str, Any] = {
        "api_auth_enabled": True,
        "knowledge_tenant_id": "*",
        "knowledge_tenant_api_keys": {"tenant-a": "tenant-a-key"},
    }
    values.update(request_limits)

    with pytest.raises(ValidationError, match="lower than"):
        Settings(_env_file=None, **values)


@pytest.mark.parametrize(
    "copied_limit",
    [
        {"api_request_body_tenant_max_concurrent": 2},
        {"api_request_body_tenant_max_buffered_bytes": 8 * 1_024 * 1_024},
    ],
    ids=["concurrency", "accounted-memory"],
)
def test_app_factory_rejects_copied_wildcard_settings_that_remove_the_two_tenant_reserve(
    copied_limit: dict[str, int],
) -> None:
    settings = _wildcard_settings().model_copy(update=copied_limit)

    with pytest.raises(ValueError, match="lower than"):
        create_app(runtime_settings=settings, include_default_routes=False)


def test_pinned_request_body_partition_retains_the_full_global_capacity() -> None:
    settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        api_request_body_max_concurrent=2,
        api_request_body_tenant_max_concurrent=1,
        api_request_body_max_buffered_bytes=256 * 1_024 * 1_024,
        api_request_body_tenant_max_buffered_bytes=128 * 1_024 * 1_024,
    ).model_copy(
        update={
            "api_request_body_tenant_max_concurrent": 2,
            "api_request_body_tenant_max_buffered_bytes": 256 * 1_024 * 1_024,
        }
    )
    app = create_app(runtime_settings=settings, include_default_routes=False)

    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.max_concurrent_per_tenant == snapshot.max_concurrent == 2
    assert snapshot.max_buffered_bytes_per_tenant == snapshot.max_buffered_bytes == 256 * 1_024 * 1_024


def test_app_factory_rejects_copied_budget_below_the_mandatory_json_decode_envelope() -> None:
    settings = Settings(_env_file=None).model_copy(
        update={
            "api_request_body_max_buffered_bytes": 4 * 1_024 * 1_024,
            "api_request_body_tenant_max_buffered_bytes": 4 * 1_024 * 1_024,
            "api_request_body_memory_amplification_factor": 2,
        }
    )

    with pytest.raises(ValueError, match="maximum request memory envelope"):
        create_app(runtime_settings=settings, include_default_routes=False)


async def test_health_exposes_payload_free_bounded_admission_utilization() -> None:
    app = create_app(runtime_settings=_wildcard_settings(), include_default_routes=False)
    controller = app.state.request_body_admission
    decision = controller.try_acquire("tenant-a", 1)
    assert decision.lease is not None
    controller.try_acquire("tenant-a", 1)

    response = await healthz(SimpleNamespace(app=app))

    admission = response["request_body_admission"]
    assert admission == {
        "active_requests": 1,
        "reserved_bytes": 4 * 1_024 * 1_024,
        "max_concurrent": 2,
        "max_buffered_bytes": 8 * 1_024 * 1_024,
        "active_tenant_partitions": 1,
        "max_concurrent_per_tenant": 1,
        "max_buffered_bytes_per_tenant": 4 * 1_024 * 1_024,
        "rejections": {"request_body_tenant_concurrency_saturated": 1},
    }
    rendered = json.dumps(response, sort_keys=True)
    assert "tenant-a" not in rendered
    assert "tenant-a-key" not in rendered
    decision.lease.release()
