from __future__ import annotations

import asyncio
import tracemalloc
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from fastapi import Body
from pydantic import ValidationError
from structlog.testing import capture_logs

from tacit.api.app import create_app
from tacit.api.request_body_limit import (
    RequestBodyAdmissionController,
    RequestBodyAdmissionReason,
    RequestBodyLimitMiddleware,
)
from tacit.config import (
    API_MAX_REQUEST_BODY_BYTES_MAX,
    API_MAX_REQUEST_BODY_BYTES_MIN,
    API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR,
    API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX,
    API_REQUEST_BODY_MAX_CONCURRENT_MAX,
    API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX,
    API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN,
    API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX,
    API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
    API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX,
    API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN,
    DEFAULT_API_MAX_REQUEST_BODY_BYTES,
    DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES,
    DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT,
    DEFAULT_API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR,
    DEFAULT_API_REQUEST_BODY_MEMORY_FLOOR_BYTES,
    DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS,
    DEFAULT_API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES,
    DEFAULT_API_REQUEST_BODY_TENANT_MAX_CONCURRENT,
    Settings,
)

ASGIMessage = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]


def _http_scope(*, path: str = "/consume", headers: list[tuple[bytes, bytes]] | None = None) -> dict[str, Any]:
    request_headers = list(headers or [])
    if not any(name.lower() == b"host" for name, _value in request_headers):
        request_headers.append((b"host", b"testserver"))
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
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


def _body_app(*, limit: int, **setting_values: Any) -> tuple[Any, list[bytes]]:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_max_request_body_bytes=limit,
            **setting_values,
        ),
        include_default_routes=False,
    )
    handled: list[bytes] = []

    @app.post("/consume")
    async def consume(payload: bytes = Body(media_type="application/octet-stream")) -> dict[str, int]:
        handled.append(payload)
        return {"size": len(payload)}

    return app, handled


async def test_request_body_at_limit_reaches_the_handler() -> None:
    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    app, handled = _body_app(limit=limit)
    body = b"x" * limit

    sent, _ = await _drive_asgi(
        app,
        scope=_http_scope(headers=[(b"content-type", b"application/octet-stream")]),
        incoming=[{"type": "http.request", "body": body, "more_body": False}],
    )

    assert _response(sent) == (200, f'{{"size":{limit}}}'.encode())
    assert handled == [body]


async def test_aggregate_body_admission_accepts_at_limit_and_rejects_limit_plus_one_before_receive() -> None:
    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    app, handled = _body_app(
        limit=limit,
        api_request_body_max_concurrent=2,
        api_request_body_tenant_max_concurrent=2,
        api_request_body_max_buffered_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
        api_request_body_tenant_max_buffered_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
        api_request_body_memory_amplification_factor=2,
        api_request_body_memory_floor_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
    )
    first_body_started = asyncio.Event()
    release_first_body = asyncio.Event()
    first_receive_calls = 0
    second_receive_calls = 0
    first_sent: list[ASGIMessage] = []
    second_sent: list[ASGIMessage] = []

    async def first_receive() -> ASGIMessage:
        nonlocal first_receive_calls
        first_receive_calls += 1
        first_body_started.set()
        await release_first_body.wait()
        return {"type": "http.request", "body": b"x" * limit, "more_body": False}

    async def second_receive() -> ASGIMessage:
        nonlocal second_receive_calls
        second_receive_calls += 1
        return {"type": "http.request", "body": b"y", "more_body": False}

    async def first_send(message: ASGIMessage) -> None:
        first_sent.append(message)

    async def second_send(message: ASGIMessage) -> None:
        second_sent.append(message)

    first = asyncio.create_task(
        app(
            _http_scope(headers=[(b"content-length", str(limit).encode("ascii"))]),
            first_receive,
            first_send,
        )
    )
    await asyncio.wait_for(first_body_started.wait(), timeout=1)

    await app(
        _http_scope(headers=[(b"content-length", b"1")]),
        second_receive,
        second_send,
    )

    assert _response(second_sent) == (503, b'{"detail":"Request body admission unavailable"}')
    assert second_receive_calls == 0
    assert handled == []
    snapshot = app.state.request_body_admission.snapshot()
    assert snapshot.active_requests == 1
    assert snapshot.reserved_bytes == API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN
    assert snapshot.rejections_by_reason == ((RequestBodyAdmissionReason.BYTE_BUDGET_SATURATED, 1),)

    release_first_body.set()
    await asyncio.wait_for(first, timeout=1)

    assert _response(first_sent) == (200, f'{{"size":{limit}}}'.encode())
    assert first_receive_calls == 1
    assert handled == [b"x" * limit]
    assert app.state.request_body_admission.snapshot().active_requests == 0
    assert app.state.request_body_admission.snapshot().reserved_bytes == 0


async def test_concurrency_saturation_rejects_without_receiving_and_recovers_after_disconnect() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=128)
    downstream_calls = 0
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    first_sent: list[ASGIMessage] = []

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=64,
        admission_controller=controller,
        read_timeout_seconds=1,
    )

    async def blocked_receive() -> ASGIMessage:
        first_started.set()
        await release_first.wait()
        return {"type": "http.disconnect"}

    async def first_send(message: ASGIMessage) -> None:
        first_sent.append(message)

    first = asyncio.create_task(
        middleware(
            _http_scope(headers=[(b"content-length", b"1")]),
            blocked_receive,
            first_send,
        )
    )
    await asyncio.wait_for(first_started.wait(), timeout=1)

    saturated_sent, receive_calls = await _drive_asgi(
        middleware,
        scope=_http_scope(headers=[(b"content-length", b"1")]),
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )
    assert _response(saturated_sent) == (503, b'{"detail":"Request body admission unavailable"}')
    assert receive_calls == 0

    release_first.set()
    await asyncio.wait_for(first, timeout=1)
    assert first_sent == []
    assert controller.snapshot().active_requests == 0

    recovered_sent, recovered_receive_calls = await _drive_asgi(
        middleware,
        scope=_http_scope(headers=[(b"content-length", b"1")]),
        incoming=[{"type": "http.request", "body": b"x", "more_body": False}],
    )
    assert _response(recovered_sent) == (204, b"")
    assert recovered_receive_calls == 1
    assert downstream_calls == 1


async def test_total_body_read_deadline_stops_slow_partial_client_and_releases_admission() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=64)
    downstream_calls = 0
    sent: list[ASGIMessage] = []

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=64,
        admission_controller=controller,
        read_timeout_seconds=0.05,
    )

    async def slow_receive() -> ASGIMessage:
        await asyncio.sleep(0.03)
        return {"type": "http.request", "body": b"x", "more_body": True}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(
        middleware(
            _http_scope(headers=[(b"transfer-encoding", b"chunked")]),
            slow_receive,
            send,
        ),
        timeout=0.5,
    )

    assert _response(sent) == (408, b'{"detail":"Request body read timed out"}')
    assert downstream_calls == 0
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_cancellation_while_reading_releases_request_and_byte_capacity() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=64)
    receive_started = asyncio.Event()
    never_complete = asyncio.Event()

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        raise AssertionError("downstream must not run")

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=64,
        admission_controller=controller,
        read_timeout_seconds=1,
    )

    async def receive() -> ASGIMessage:
        receive_started.set()
        await never_complete.wait()
        raise AssertionError("unreachable")

    async def send(message: ASGIMessage) -> None:
        raise AssertionError(f"unexpected response: {message}")

    task = asyncio.create_task(
        middleware(
            _http_scope(headers=[(b"content-length", b"64")]),
            receive,
            send,
        )
    )
    await asyncio.wait_for(receive_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


async def test_unframed_get_body_read_is_admitted_and_uses_the_total_deadline() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=64)
    sent: list[ASGIMessage] = []

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        await receive()

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=64,
        admission_controller=controller,
        read_timeout_seconds=0.05,
    )
    scope = _http_scope()
    scope["method"] = "GET"

    async def receive() -> ASGIMessage:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    await asyncio.wait_for(middleware(scope, receive, send), timeout=0.5)

    assert _response(sent) == (408, b'{"detail":"Request body read timed out"}')
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("method", ["POST", "GET"], ids=["eager", "lazy-unframed"])
async def test_absolute_body_deadline_bounds_a_non_yielding_fragment_stream(
    method: str,
) -> None:
    frame_limit = 10_000
    receive_calls = 0
    sent: list[ASGIMessage] = []

    async def downstream(_scope: dict[str, Any], receive: ASGIReceive, _send: ASGISend) -> None:
        await receive()

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls <= frame_limit:
            return {"type": "http.request", "body": b"", "more_body": True}
        return {"type": "http.disconnect"}

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    scope = _http_scope(headers=[])
    scope["method"] = method
    await asyncio.wait_for(
        RequestBodyLimitMiddleware(
            downstream,
            max_body_bytes=64,
            read_timeout_seconds=0.000_001,
        )(scope, receive, send),
        timeout=0.5,
    )

    assert _response(sent) == (408, b'{"detail":"Request body read timed out"}')
    assert receive_calls < frame_limit


async def test_request_body_capacity_remains_held_through_handler_completion() -> None:
    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            api_max_request_body_bytes=limit,
            api_request_body_max_concurrent=1,
            api_request_body_tenant_max_concurrent=1,
            api_request_body_max_buffered_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
            api_request_body_tenant_max_buffered_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
            api_request_body_memory_amplification_factor=2,
            api_request_body_memory_floor_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
        ),
        include_default_routes=False,
    )

    @app.post("/consume")
    async def consume(payload: bytes = Body(media_type="application/octet-stream")) -> dict[str, int]:
        handler_started.set()
        await release_handler.wait()
        return {"size": len(payload)}

    first_sent: list[ASGIMessage] = []

    async def first_receive() -> ASGIMessage:
        return {"type": "http.request", "body": b"x", "more_body": False}

    async def first_send(message: ASGIMessage) -> None:
        first_sent.append(message)

    first = asyncio.create_task(
        app(
            _http_scope(headers=[(b"content-length", b"1"), (b"content-type", b"application/octet-stream")]),
            first_receive,
            first_send,
        )
    )
    await asyncio.wait_for(handler_started.wait(), timeout=1)

    second_sent, second_receive_calls = await _drive_asgi(
        app,
        scope=_http_scope(headers=[(b"content-length", b"1"), (b"content-type", b"application/octet-stream")]),
        incoming=[{"type": "http.request", "body": b"y", "more_body": False}],
    )

    assert _response(second_sent) == (503, b'{"detail":"Request body admission unavailable"}')
    assert second_receive_calls == 0
    assert app.state.request_body_admission.snapshot().active_requests == 1

    release_handler.set()
    await asyncio.wait_for(first, timeout=1)
    assert _response(first_sent) == (200, b'{"size":1}')
    assert app.state.request_body_admission.snapshot().active_requests == 0


async def test_dishonest_content_length_reserves_growth_before_copying_extra_bytes() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=2, max_buffered_bytes=8)
    first_chunk_buffered = asyncio.Event()
    release_first = asyncio.Event()
    first_receive_calls = 0
    second_receive_calls = 0

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        raise AssertionError("downstream must not run")

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=8,
        admission_controller=controller,
        read_timeout_seconds=1,
    )

    async def first_receive() -> ASGIMessage:
        nonlocal first_receive_calls
        first_receive_calls += 1
        if first_receive_calls == 1:
            first_chunk_buffered.set()
            return {"type": "http.request", "body": b"xxxx", "more_body": True}
        await release_first.wait()
        return {"type": "http.disconnect"}

    async def no_send(message: ASGIMessage) -> None:
        raise AssertionError(f"unexpected response: {message}")

    first = asyncio.create_task(
        middleware(
            _http_scope(headers=[(b"content-length", b"4")]),
            first_receive,
            no_send,
        )
    )
    await asyncio.wait_for(first_chunk_buffered.wait(), timeout=1)

    async def second_receive() -> ASGIMessage:
        nonlocal second_receive_calls
        second_receive_calls += 1
        return {"type": "http.request", "body": b"yyyyy", "more_body": False}

    second_sent: list[ASGIMessage] = []

    async def second_send(message: ASGIMessage) -> None:
        second_sent.append(message)

    await middleware(
        _http_scope(headers=[(b"content-length", b"4")]),
        second_receive,
        second_send,
    )

    assert _response(second_sent) == (503, b'{"detail":"Request body admission unavailable"}')
    assert second_receive_calls == 1
    assert controller.snapshot().reserved_bytes == 4

    release_first.set()
    await asyncio.wait_for(first, timeout=1)
    assert controller.snapshot().reserved_bytes == 0


async def test_admission_logs_only_bounded_reason_and_aggregate_counts() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=64)
    held = controller.try_acquire(1)
    assert held.lease is not None

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        raise AssertionError("downstream must not run")

    with capture_logs() as logs:
        sent, receive_calls = await _drive_asgi(
            RequestBodyLimitMiddleware(
                downstream,
                max_body_bytes=64,
                admission_controller=controller,
                read_timeout_seconds=1,
            ),
            scope=_http_scope(headers=[(b"content-length", b"1"), (b"x-api-key", b"super-secret")]),
            incoming=[{"type": "http.request", "body": b"secret-payload", "more_body": False}],
        )
    held.lease.release()

    assert _response(sent) == (503, b'{"detail":"Request body admission unavailable"}')
    assert receive_calls == 0
    event = next(record for record in logs if record["event"] == "request_body_admission_rejected")
    assert event["reason_code"] == RequestBodyAdmissionReason.CONCURRENCY_SATURATED
    assert event["active_requests"] == 1
    assert event["reserved_bytes"] == 1
    rendered = str(event)
    assert "super-secret" not in rendered
    assert "secret-payload" not in rendered


def test_request_body_lease_reservations_are_monotonic_and_release_exactly_once() -> None:
    controller = RequestBodyAdmissionController(max_concurrent=1, max_buffered_bytes=64)
    decision = controller.try_acquire(8)
    assert decision.lease is not None

    assert decision.lease.reserve_to(32) is True
    assert decision.lease.reserve_to(1) is True
    assert controller.snapshot().reserved_bytes == 32

    decision.lease.release()
    decision.lease.release()
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize(
    "headers",
    [
        [],
        [(b"transfer-encoding", b"chunked")],
        [(b"content-length", b"invalid")],
        [(b"content-length", b"1")],
    ],
    ids=["missing-length", "chunked", "invalid-length", "dishonest-length"],
)
async def test_streamed_request_body_over_limit_returns_stable_413_without_handler_invocation(
    headers: list[tuple[bytes, bytes]],
) -> None:
    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    app, handled = _body_app(limit=limit)

    sent, _ = await _drive_asgi(
        app,
        scope=_http_scope(headers=[(b"content-type", b"application/octet-stream"), *headers]),
        incoming=[
            {"type": "http.request", "body": b"x" * limit, "more_body": True},
            {"type": "http.request", "body": b"x", "more_body": False},
        ],
    )

    assert _response(sent) == (413, b'{"detail":"Request body too large"}')
    assert handled == []


async def test_oversized_content_length_rejects_before_receive_or_downstream() -> None:
    downstream_calls = 0

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1

    limit = API_MAX_REQUEST_BODY_BYTES_MIN
    app = RequestBodyLimitMiddleware(downstream, max_body_bytes=limit)

    sent, receive_calls = await _drive_asgi(
        app,
        scope=_http_scope(headers=[(b"content-length", str(limit + 1).encode())]),
        incoming=[{"type": "http.request", "body": b"", "more_body": False}],
    )

    assert _response(sent) == (413, b'{"detail":"Request body too large"}')
    assert receive_calls == 0
    assert downstream_calls == 0


@pytest.mark.parametrize(
    "incoming",
    [
        [{"type": "http.disconnect"}],
        [
            {"type": "http.request", "body": b"partial", "more_body": True},
            {"type": "http.disconnect"},
        ],
    ],
    ids=["disconnect-before-body", "disconnect-after-partial-body"],
)
async def test_disconnected_request_never_reaches_downstream_or_sends_a_response(
    incoming: list[ASGIMessage],
) -> None:
    downstream_calls = 0

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    app = RequestBodyLimitMiddleware(downstream, max_body_bytes=64)
    sent, receive_calls = await _drive_asgi(
        app,
        scope=_http_scope(headers=[(b"content-length", b"16")]),
        incoming=incoming,
    )

    assert downstream_calls == 0
    assert sent == []
    assert receive_calls == len(incoming)


@pytest.mark.parametrize(
    ("declared_length", "body"),
    [(3, b"xx"), (1, b"xx")],
    ids=["declared-longer-than-body", "declared-shorter-than-body"],
)
async def test_declared_content_length_mismatch_fails_before_downstream(
    declared_length: int,
    body: bytes,
) -> None:
    downstream_calls = 0

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1

    limit = 64
    sent, receive_calls = await _drive_asgi(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=limit),
        scope=_http_scope(headers=[(b"content-length", str(declared_length).encode("ascii"))]),
        incoming=[{"type": "http.request", "body": body, "more_body": False}],
    )

    assert _response(sent) == (400, b'{"detail":"Incomplete request body"}')
    assert downstream_calls == 0
    assert receive_calls == 1
    assert all(len(message.get("body", b"")) <= limit for message in sent)


@pytest.mark.parametrize(
    "headers",
    [
        [(b"content-length", b"5")],
        [(b"transfer-encoding", b"chunked")],
        [],
    ],
    ids=["matching-content-length", "chunked", "unframed"],
)
async def test_complete_request_bodies_replay_within_the_admitted_bound(
    headers: list[tuple[bytes, bytes]],
) -> None:
    observed_chunks: list[bytes] = []
    downstream_calls = 0

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        while True:
            message = await receive()
            assert message["type"] == "http.request"
            observed_chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    limit = 5
    sent, receive_calls = await _drive_asgi(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=limit),
        scope=_http_scope(headers=headers),
        incoming=[
            {"type": "http.request", "body": b"xx", "more_body": True},
            {"type": "http.request", "body": b"xxx", "more_body": False},
        ],
    )

    assert _response(sent) == (204, b"")
    assert downstream_calls == 1
    assert receive_calls == 2
    assert b"".join(observed_chunks) == b"xxxxx"
    assert all(len(chunk) <= limit for chunk in observed_chunks)


@pytest.mark.parametrize(
    ("method", "framing_headers"),
    [
        ("POST", [(b"content-length", b"1024")]),
        ("GET", []),
    ],
    ids=["framed-eager", "unframed-lazy"],
)
async def test_fragmented_body_replays_once_without_retaining_chunk_objects(
    method: str,
    framing_headers: list[tuple[bytes, bytes]],
) -> None:
    empty_chunks = 100_000
    body_bytes = 1_024
    receive_calls = 0
    observed: list[ASGIMessage] = []
    reserved_at_handler = 0

    controller = RequestBodyAdmissionController(
        max_concurrent=1,
        max_buffered_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
        memory_amplification_factor=2,
        memory_floor_bytes=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
    )

    async def downstream(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal reserved_at_handler
        observed.append(await receive())
        reserved_at_handler = controller.snapshot().reserved_bytes
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = RequestBodyLimitMiddleware(
        downstream,
        max_body_bytes=body_bytes,
        admission_controller=controller,
        read_timeout_seconds=10,
    )

    async def receive() -> ASGIMessage:
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls <= empty_chunks:
            return {"type": "http.request", "body": b"", "more_body": True}
        offset = receive_calls - empty_chunks
        return {
            "type": "http.request",
            "body": b"x",
            "more_body": offset < body_bytes,
        }

    sent: list[ASGIMessage] = []

    async def send(message: ASGIMessage) -> None:
        sent.append(message)

    tracemalloc.start()
    try:
        scope = _http_scope(
            headers=[
                (b"content-type", b"application/octet-stream"),
                *framing_headers,
            ]
        )
        scope["method"] = method
        await middleware(scope, receive, send)
        _current, observed_peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert _response(sent) == (204, b"")
    assert receive_calls == empty_chunks + body_bytes
    assert observed == [
        {
            "type": "http.request",
            "body": b"x" * body_bytes,
            "more_body": False,
        }
    ]
    assert observed_peak <= reserved_at_handler
    assert controller.snapshot().active_requests == 0
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize(
    ("content_type", "expected_charge"),
    [
        ("application/octet-stream", 8),
        ("application/json; charset=utf-8", 4 * API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR),
        ("application/problem+json", 4 * API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR),
    ],
)
async def test_media_type_selects_a_non_weakenable_decode_memory_envelope(
    content_type: str,
    expected_charge: int,
) -> None:
    controller = RequestBodyAdmissionController(
        max_concurrent=1,
        max_buffered_bytes=1_024,
        memory_amplification_factor=2,
    )
    observed_charge = 0

    async def downstream(_scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal observed_charge
        await receive()
        observed_charge = controller.snapshot().reserved_bytes
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    sent, receive_calls = await _drive_asgi(
        RequestBodyLimitMiddleware(
            downstream,
            max_body_bytes=4,
            admission_controller=controller,
        ),
        scope=_http_scope(
            headers=[
                (b"content-type", content_type.encode("ascii")),
                (b"content-length", b"4"),
            ]
        ),
        incoming=[{"type": "http.request", "body": b"null", "more_body": False}],
    )

    assert _response(sent) == (204, b"")
    assert receive_calls == 1
    assert observed_charge == expected_charge
    assert controller.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("scope_type", ["lifespan", "websocket"])
async def test_non_http_scopes_pass_through_unchanged(scope_type: str) -> None:
    observed: list[tuple[dict[str, Any], ASGIReceive, ASGISend]] = []

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        observed.append((scope, receive, send))

    scope = {"type": scope_type}

    async def receive() -> ASGIMessage:
        return {"type": f"{scope_type}.disconnect"}

    async def send(message: ASGIMessage) -> None:
        raise AssertionError(f"unexpected message: {message}")

    await RequestBodyLimitMiddleware(downstream, max_body_bytes=1)(scope, receive, send)

    assert observed == [(scope, receive, send)]


async def test_http_request_without_a_body_preserves_downstream_behavior() -> None:
    downstream_calls = 0

    async def downstream(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        nonlocal downstream_calls
        downstream_calls += 1
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    scope = _http_scope(path="/health")
    scope["method"] = "GET"
    sent, receive_calls = await _drive_asgi(
        RequestBodyLimitMiddleware(downstream, max_body_bytes=1),
        scope=scope,
        incoming=[],
    )

    assert _response(sent) == (204, b"")
    assert downstream_calls == 1
    assert receive_calls == 0


def test_request_body_limit_has_a_conservative_bounded_default() -> None:
    settings = Settings(_env_file=None)

    assert DEFAULT_API_MAX_REQUEST_BODY_BYTES == 2 * 1_024 * 1_024
    assert DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT > 0
    assert DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES >= DEFAULT_API_MAX_REQUEST_BODY_BYTES
    assert DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS > 0
    assert settings.api_max_request_body_bytes == DEFAULT_API_MAX_REQUEST_BODY_BYTES
    assert settings.api_request_body_max_concurrent == DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT
    assert settings.api_request_body_max_buffered_bytes == DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES
    assert settings.api_request_body_tenant_max_concurrent == DEFAULT_API_REQUEST_BODY_TENANT_MAX_CONCURRENT
    assert settings.api_request_body_tenant_max_buffered_bytes == DEFAULT_API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES
    assert settings.api_request_body_memory_amplification_factor == DEFAULT_API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR
    assert settings.api_request_body_memory_floor_bytes == DEFAULT_API_REQUEST_BODY_MEMORY_FLOOR_BYTES
    assert settings.api_request_body_read_timeout_seconds == DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS
    assert API_MAX_REQUEST_BODY_BYTES_MIN <= settings.api_max_request_body_bytes <= API_MAX_REQUEST_BODY_BYTES_MAX


@pytest.mark.parametrize(
    "value",
    [API_MAX_REQUEST_BODY_BYTES_MIN - 1, API_MAX_REQUEST_BODY_BYTES_MAX + 1],
)
def test_request_body_limit_rejects_out_of_range_configuration(value: int) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_max_request_body_bytes=value)


@pytest.mark.parametrize(
    "value",
    [
        True,
        "1024",
        API_MAX_REQUEST_BODY_BYTES_MIN - 1,
        API_MAX_REQUEST_BODY_BYTES_MAX + 1,
    ],
)
def test_app_factory_revalidates_copied_request_body_limits(value: object) -> None:
    settings = Settings(_env_file=None).model_copy(update={"api_max_request_body_bytes": value})

    with pytest.raises(ValueError, match="api_max_request_body_bytes"):
        create_app(runtime_settings=settings)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("api_request_body_max_concurrent", 0),
        ("api_request_body_max_concurrent", API_REQUEST_BODY_MAX_CONCURRENT_MAX + 1),
        ("api_request_body_max_buffered_bytes", API_MAX_REQUEST_BODY_BYTES_MIN - 1),
        ("api_request_body_max_buffered_bytes", API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX + 1),
        ("api_request_body_tenant_max_concurrent", 0),
        ("api_request_body_tenant_max_concurrent", API_REQUEST_BODY_MAX_CONCURRENT_MAX + 1),
        ("api_request_body_tenant_max_buffered_bytes", API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX + 1),
        (
            "api_request_body_memory_amplification_factor",
            API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN - 1,
        ),
        (
            "api_request_body_memory_amplification_factor",
            API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX + 1,
        ),
        ("api_request_body_memory_floor_bytes", API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX + 1),
        ("api_request_body_read_timeout_seconds", API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN / 2),
        ("api_request_body_read_timeout_seconds", API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX + 1),
    ],
)
def test_aggregate_request_body_settings_reject_out_of_range_configuration(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_aggregate_request_body_budget_must_admit_one_maximum_request() -> None:
    with pytest.raises(ValidationError, match="api_request_body_max_buffered_bytes"):
        Settings(
            _env_file=None,
            api_max_request_body_bytes=2 * API_MAX_REQUEST_BODY_BYTES_MIN,
            api_request_body_max_buffered_bytes=API_MAX_REQUEST_BODY_BYTES_MIN,
        )


@pytest.mark.parametrize(
    ("field", "value", "match"),
    [
        (
            "api_request_body_tenant_max_concurrent",
            DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT + 1,
            "must not exceed api_request_body_max_concurrent",
        ),
        (
            "api_request_body_tenant_max_buffered_bytes",
            DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES + 1,
            "must not exceed api_request_body_max_buffered_bytes",
        ),
    ],
)
def test_tenant_request_body_limits_must_remain_within_global_capacity(
    field: str,
    value: int,
    match: str,
) -> None:
    with pytest.raises(ValidationError, match=match):
        Settings(_env_file=None, **{field: value})


def test_tenant_request_body_budget_must_admit_one_maximum_memory_envelope() -> None:
    with pytest.raises(ValidationError, match="tenant_max_buffered_bytes"):
        Settings(
            _env_file=None,
            api_request_body_tenant_max_buffered_bytes=API_MAX_REQUEST_BODY_BYTES_MIN,
        )


def test_maximum_per_request_configuration_requires_an_explicit_memory_envelope() -> None:
    with pytest.raises(ValidationError, match="maximum request memory envelope"):
        Settings(
            _env_file=None,
            api_max_request_body_bytes=API_MAX_REQUEST_BODY_BYTES_MAX,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("api_request_body_max_concurrent", True),
        ("api_request_body_max_buffered_bytes", "2048"),
        ("api_request_body_tenant_max_concurrent", True),
        ("api_request_body_tenant_max_buffered_bytes", "2048"),
        ("api_request_body_memory_amplification_factor", 1),
        ("api_request_body_memory_floor_bytes", API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN - 1),
        ("api_request_body_read_timeout_seconds", float("nan")),
    ],
)
def test_app_factory_revalidates_copied_aggregate_request_body_settings(field: str, value: object) -> None:
    settings = Settings(_env_file=None).model_copy(update={field: value})

    with pytest.raises(ValueError, match=field):
        create_app(runtime_settings=settings)
