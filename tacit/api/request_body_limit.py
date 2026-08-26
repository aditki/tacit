"""ASGI request-body admission before framework buffering and decoding."""

from __future__ import annotations

from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_REQUEST_TOO_LARGE_BODY = b'{"detail":"Request body too large"}'
_REQUEST_TOO_LARGE_HEADERS = (
    (b"content-type", b"application/json"),
    (b"content-length", str(len(_REQUEST_TOO_LARGE_BODY)).encode("ascii")),
)


class _RequestBodyTooLarge(HTTPException):
    """Internal receive-channel signal handled by the outer middleware."""

    def __init__(self) -> None:
        super().__init__(status_code=413, detail="Request body too large")


class RequestBodyLimitMiddleware:
    """Reject HTTP request bodies that exceed a fixed byte budget."""

    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        if type(max_body_bytes) is not int or max_body_bytes <= 0:
            raise ValueError("max_body_bytes must be a positive integer")
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if _declared_body_exceeds_limit(scope.get("headers", []), self.max_body_bytes):
            await _send_request_too_large(send)
            return

        received_bytes = 0
        response_started = False

        async def limited_receive() -> Message:
            nonlocal received_bytes
            message = await receive()
            if message["type"] == "http.request":
                received_bytes += len(message.get("body", b""))
                if received_bytes > self.max_body_bytes:
                    raise _RequestBodyTooLarge()
            return message

        async def tracked_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _RequestBodyTooLarge:
            if response_started:
                raise RuntimeError("Request body exceeded the limit after the response started") from None
            await _send_request_too_large(send)


def _declared_body_exceeds_limit(headers: list[tuple[bytes, bytes]], max_body_bytes: int) -> bool:
    values = [value.strip() for name, value in headers if name.lower() == b"content-length"]
    if len(values) != 1:
        return False

    value = values[0]
    if not value or any(byte < ord("0") or byte > ord("9") for byte in value):
        return False

    normalized = value.lstrip(b"0") or b"0"
    limit = str(max_body_bytes).encode("ascii")
    return len(normalized) > len(limit) or (len(normalized) == len(limit) and normalized > limit)


async def _send_request_too_large(send: Send) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": list(_REQUEST_TOO_LARGE_HEADERS),
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": _REQUEST_TOO_LARGE_BODY,
            "more_body": False,
        }
    )
