from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from fastapi import Body
from pydantic import ValidationError

from tacit.api.app import create_app
from tacit.api.request_body_limit import RequestBodyLimitMiddleware
from tacit.config import (
    API_MAX_REQUEST_BODY_BYTES_MAX,
    API_MAX_REQUEST_BODY_BYTES_MIN,
    DEFAULT_API_MAX_REQUEST_BODY_BYTES,
    Settings,
)

ASGIMessage = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]


def _http_scope(*, path: str = "/consume", headers: list[tuple[bytes, bytes]] | None = None) -> dict[str, Any]:
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
        "headers": headers or [],
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


def _body_app(*, limit: int) -> tuple[Any, list[bytes]]:
    app = create_app(
        runtime_settings=Settings(_env_file=None, api_max_request_body_bytes=limit),
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
    assert settings.api_max_request_body_bytes == DEFAULT_API_MAX_REQUEST_BODY_BYTES
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
