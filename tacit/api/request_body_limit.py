"""Authenticated ASGI request-body admission before framework decoding."""

from __future__ import annotations

import asyncio
import json
import math
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import structlog
from fastapi import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from tacit.api.security import RequestAuthentication, authenticate_request_headers
from tacit.config import (
    API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR,
)
from tacit.config import (
    settings as default_settings,
)

_INCOMPLETE_REQUEST_BODY = b'{"detail":"Incomplete request body"}'
_NOT_FOUND_BODY = b'{"detail":"Not Found"}'
_REQUEST_BODY_ADMISSION_UNAVAILABLE_BODY = b'{"detail":"Request body admission unavailable"}'
_REQUEST_BODY_NOT_ALLOWED_BODY = b'{"detail":"Request body not allowed"}'
_REQUEST_BODY_READ_TIMEOUT_BODY = b'{"detail":"Request body read timed out"}'
_REQUEST_TOO_LARGE_BODY = b'{"detail":"Request body too large"}'
_PUBLIC_METHODS = frozenset({"GET", "HEAD"})
_PUBLIC_PATHS = frozenset({"/", "/healthz"})
_DISABLED_DOCUMENTATION_PATHS = frozenset({"/docs", "/redoc", "/openapi.json"})

logger = structlog.get_logger()


class _RequestBodyTooLarge(RuntimeError):
    pass


class _RequestBodyAdmissionUnavailable(RuntimeError):
    pass


class _RequestBodyReadTimedOut(RuntimeError):
    pass


class _IncompleteRequestBody(RuntimeError):
    pass


class _RequestDisconnected(RuntimeError):
    pass


class RequestBodyAdmissionReason(StrEnum):
    """Bounded, payload-free reason codes for ingress admission telemetry."""

    GLOBAL_CONCURRENCY_SATURATED = "request_body_concurrency_saturated"
    CONCURRENCY_SATURATED = "request_body_concurrency_saturated"
    GLOBAL_BYTE_BUDGET_SATURATED = "request_body_byte_budget_saturated"
    BYTE_BUDGET_SATURATED = "request_body_byte_budget_saturated"
    TENANT_CONCURRENCY_SATURATED = "request_body_tenant_concurrency_saturated"
    TENANT_BYTE_BUDGET_SATURATED = "request_body_tenant_byte_budget_saturated"
    READ_TIMEOUT = "request_body_read_timeout"
    DISCONNECT_OBSERVATION_TIMEOUT = "request_body_disconnect_observation_timeout"
    CLIENT_DISCONNECTED = "request_body_client_disconnected"
    CANCELLED = "request_body_read_cancelled"


@dataclass(frozen=True, slots=True)
class RequestBodyAdmissionSnapshot:
    """Non-secret aggregate state suitable for health and test assertions."""

    active_requests: int
    reserved_bytes: int
    max_concurrent: int
    max_buffered_bytes: int
    active_tenant_partitions: int
    max_concurrent_per_tenant: int
    max_buffered_bytes_per_tenant: int
    rejections_by_reason: tuple[tuple[RequestBodyAdmissionReason, int], ...]


@dataclass(frozen=True, slots=True)
class RequestBodyAdmissionDecision:
    lease: RequestBodyAdmissionLease | None
    reason: RequestBodyAdmissionReason | None


@dataclass(slots=True)
class _Reservation:
    tenant_id: str
    logical_bytes: int
    accounted_bytes: int
    memory_amplification_factor: int


class RequestBodyAdmissionLease:
    """One tenant-owned, idempotently released request-memory reservation."""

    __slots__ = ("_controller", "_lease_id", "_released")

    def __init__(self, controller: RequestBodyAdmissionController, lease_id: int) -> None:
        self._controller = controller
        self._lease_id = lease_id
        self._released = False

    @property
    def reserved_bytes(self) -> int:
        if self._released:
            return 0
        return self._controller._reserved_bytes_for(self._lease_id)

    def reserve_to(self, required_body_bytes: int) -> bool:
        """Reserve a larger decode envelope before forwarding more body bytes."""
        if self._released:
            return False
        return self._controller._reserve_to(self._lease_id, required_body_bytes)

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._controller._release(self._lease_id)


class RequestBodyAdmissionController:
    """Application-owned global and principal-partitioned ingress admission."""

    def __init__(
        self,
        *,
        max_concurrent: int,
        max_buffered_bytes: int,
        max_concurrent_per_tenant: int | None = None,
        max_buffered_bytes_per_tenant: int | None = None,
        memory_amplification_factor: int = 1,
        memory_floor_bytes: int = 0,
    ) -> None:
        if type(max_concurrent) is not int or max_concurrent <= 0:
            raise ValueError("max_concurrent must be a positive integer")
        if type(max_buffered_bytes) is not int or max_buffered_bytes <= 0:
            raise ValueError("max_buffered_bytes must be a positive integer")
        tenant_concurrent = max_concurrent if max_concurrent_per_tenant is None else max_concurrent_per_tenant
        tenant_bytes = max_buffered_bytes if max_buffered_bytes_per_tenant is None else max_buffered_bytes_per_tenant
        if type(tenant_concurrent) is not int or not 1 <= tenant_concurrent <= max_concurrent:
            raise ValueError("max_concurrent_per_tenant must be between 1 and max_concurrent")
        if type(tenant_bytes) is not int or not 1 <= tenant_bytes <= max_buffered_bytes:
            raise ValueError("max_buffered_bytes_per_tenant must be between 1 and max_buffered_bytes")
        if type(memory_amplification_factor) is not int or memory_amplification_factor <= 0:
            raise ValueError("memory_amplification_factor must be a positive integer")
        if type(memory_floor_bytes) is not int or memory_floor_bytes < 0:
            raise ValueError("memory_floor_bytes must be a non-negative integer")

        self.max_concurrent = max_concurrent
        self.max_buffered_bytes = max_buffered_bytes
        self.max_concurrent_per_tenant = tenant_concurrent
        self.max_buffered_bytes_per_tenant = tenant_bytes
        self.memory_amplification_factor = memory_amplification_factor
        self.memory_floor_bytes = memory_floor_bytes
        self._lock = threading.Lock()
        self._next_lease_id = 1
        self._reservations: dict[int, _Reservation] = {}
        self._reserved_bytes = 0
        self._tenant_active: dict[str, int] = {}
        self._tenant_reserved_bytes: dict[str, int] = {}
        self._rejections = {reason: 0 for reason in RequestBodyAdmissionReason}

    def estimated_memory_bytes(
        self,
        logical_body_bytes: int,
        *,
        memory_amplification_factor: int | None = None,
    ) -> int:
        """Return the conservative retained-memory charge for one request body."""
        if type(logical_body_bytes) is not int or logical_body_bytes < 0:
            raise ValueError("logical_body_bytes must be a non-negative integer")
        factor = self.memory_amplification_factor
        if memory_amplification_factor is not None:
            if type(memory_amplification_factor) is not int or memory_amplification_factor <= 0:
                raise ValueError("memory_amplification_factor must be a positive integer")
            factor = max(factor, memory_amplification_factor)
        return max(self.memory_floor_bytes, logical_body_bytes * factor)

    def try_acquire(
        self,
        tenant_id: str | int,
        logical_body_bytes: int | None = None,
        *,
        memory_amplification_factor: int | None = None,
    ) -> RequestBodyAdmissionDecision:
        """Reserve one global and tenant request slot without waiting.

        The one-argument integer form remains available for isolated legacy tests
        and assigns the reservation to the local tenant partition.
        """
        if logical_body_bytes is None:
            if type(tenant_id) is not int:
                raise ValueError("logical_body_bytes is required")
            logical_body_bytes = tenant_id
            tenant_id = "default"
        if not isinstance(tenant_id, str) or not tenant_id:
            raise ValueError("tenant_id must be a non-empty string")
        factor = self.memory_amplification_factor
        if memory_amplification_factor is not None:
            if type(memory_amplification_factor) is not int or memory_amplification_factor <= 0:
                raise ValueError("memory_amplification_factor must be a positive integer")
            factor = max(factor, memory_amplification_factor)
        accounted_bytes = self.estimated_memory_bytes(
            logical_body_bytes,
            memory_amplification_factor=factor,
        )

        with self._lock:
            if len(self._reservations) >= self.max_concurrent:
                return self._reject_locked(RequestBodyAdmissionReason.GLOBAL_CONCURRENCY_SATURATED)
            if self._tenant_active.get(tenant_id, 0) >= self.max_concurrent_per_tenant:
                return self._reject_locked(RequestBodyAdmissionReason.TENANT_CONCURRENCY_SATURATED)
            if accounted_bytes > self.max_buffered_bytes - self._reserved_bytes:
                return self._reject_locked(RequestBodyAdmissionReason.GLOBAL_BYTE_BUDGET_SATURATED)
            tenant_reserved = self._tenant_reserved_bytes.get(tenant_id, 0)
            if accounted_bytes > self.max_buffered_bytes_per_tenant - tenant_reserved:
                return self._reject_locked(RequestBodyAdmissionReason.TENANT_BYTE_BUDGET_SATURATED)

            lease_id = self._next_lease_id
            self._next_lease_id += 1
            self._reservations[lease_id] = _Reservation(
                tenant_id=tenant_id,
                logical_bytes=logical_body_bytes,
                accounted_bytes=accounted_bytes,
                memory_amplification_factor=factor,
            )
            self._reserved_bytes += accounted_bytes
            self._tenant_active[tenant_id] = self._tenant_active.get(tenant_id, 0) + 1
            self._tenant_reserved_bytes[tenant_id] = tenant_reserved + accounted_bytes
        return RequestBodyAdmissionDecision(RequestBodyAdmissionLease(self, lease_id), None)

    def snapshot(self) -> RequestBodyAdmissionSnapshot:
        with self._lock:
            rejections = tuple((reason, count) for reason, count in self._rejections.items() if count)
            return RequestBodyAdmissionSnapshot(
                active_requests=len(self._reservations),
                reserved_bytes=self._reserved_bytes,
                max_concurrent=self.max_concurrent,
                max_buffered_bytes=self.max_buffered_bytes,
                active_tenant_partitions=len(self._tenant_active),
                max_concurrent_per_tenant=self.max_concurrent_per_tenant,
                max_buffered_bytes_per_tenant=self.max_buffered_bytes_per_tenant,
                rejections_by_reason=rejections,
            )

    def _reject_locked(self, reason: RequestBodyAdmissionReason) -> RequestBodyAdmissionDecision:
        self._rejections[reason] += 1
        return RequestBodyAdmissionDecision(None, reason)

    def _reserved_bytes_for(self, lease_id: int) -> int:
        with self._lock:
            reservation = self._reservations.get(lease_id)
            return reservation.accounted_bytes if reservation is not None else 0

    def _reserve_to(self, lease_id: int, required_body_bytes: int) -> bool:
        with self._lock:
            reservation = self._reservations.get(lease_id)
            if reservation is None:
                return False
            if required_body_bytes <= reservation.logical_bytes:
                return True
            accounted_bytes = self.estimated_memory_bytes(
                required_body_bytes,
                memory_amplification_factor=reservation.memory_amplification_factor,
            )
            additional = accounted_bytes - reservation.accounted_bytes
            if additional > self.max_buffered_bytes - self._reserved_bytes:
                self._rejections[RequestBodyAdmissionReason.GLOBAL_BYTE_BUDGET_SATURATED] += 1
                return False
            tenant_reserved = self._tenant_reserved_bytes[reservation.tenant_id]
            if additional > self.max_buffered_bytes_per_tenant - tenant_reserved:
                self._rejections[RequestBodyAdmissionReason.TENANT_BYTE_BUDGET_SATURATED] += 1
                return False
            reservation.logical_bytes = required_body_bytes
            reservation.accounted_bytes = accounted_bytes
            self._reserved_bytes += additional
            self._tenant_reserved_bytes[reservation.tenant_id] = tenant_reserved + additional
            return True

    def _release(self, lease_id: int) -> None:
        with self._lock:
            reservation = self._reservations.pop(lease_id, None)
            if reservation is None:
                return
            self._reserved_bytes -= reservation.accounted_bytes
            tenant_id = reservation.tenant_id
            active = self._tenant_active[tenant_id] - 1
            tenant_reserved = self._tenant_reserved_bytes[tenant_id] - reservation.accounted_bytes
            if active:
                self._tenant_active[tenant_id] = active
                self._tenant_reserved_bytes[tenant_id] = tenant_reserved
            else:
                self._tenant_active.pop(tenant_id, None)
                self._tenant_reserved_bytes.pop(tenant_id, None)


class RequestBodyLimitMiddleware:
    """Authenticate and admit one request stream through handler disposal."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        max_body_bytes: int,
        admission_controller: RequestBodyAdmissionController | None = None,
        read_timeout_seconds: float = 15.0,
        runtime_settings: Any = default_settings,
    ) -> None:
        if type(max_body_bytes) is not int or max_body_bytes <= 0:
            raise ValueError("max_body_bytes must be a positive integer")
        if (
            isinstance(read_timeout_seconds, bool)
            or not isinstance(read_timeout_seconds, (int, float))
            or not math.isfinite(float(read_timeout_seconds))
            or read_timeout_seconds <= 0
        ):
            raise ValueError("read_timeout_seconds must be a positive finite number")
        self.app = app
        self.max_body_bytes = max_body_bytes
        self.admission_controller = admission_controller or RequestBodyAdmissionController(
            max_concurrent=16,
            max_buffered_bytes=16 * max_body_bytes,
        )
        maximum_charge = self.admission_controller.estimated_memory_bytes(max_body_bytes)
        if self.admission_controller.max_buffered_bytes < maximum_charge:
            raise ValueError("admission_controller must admit one maximum-size request")
        if self.admission_controller.max_buffered_bytes_per_tenant < maximum_charge:
            raise ValueError("admission_controller tenant partition must admit one maximum-size request")
        self.read_timeout_seconds = float(read_timeout_seconds)
        self.runtime_settings = runtime_settings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if _is_disabled_documentation_request(scope, self.runtime_settings):
            await _send_json_response(
                send,
                404,
                _NOT_FOUND_BODY,
                suppress_body=_request_method(scope) == "HEAD",
            )
            return

        if _is_public_request(scope):
            if _public_request_has_body(scope):
                await _send_json_response(
                    send,
                    400,
                    _REQUEST_BODY_NOT_ALLOWED_BODY,
                    suppress_body=_request_method(scope) == "HEAD",
                )
                return
            await self.app(scope, receive, send)
            return

        authentication = await self._authenticate(scope, send)
        if authentication is None:
            return
        admission_partition = authentication.tenant_id

        if not _requires_eager_admission(scope):
            await self._call_with_lazy_admission(
                scope,
                receive,
                send,
                admission_partition=admission_partition,
            )
            return

        headers = list(scope.get("headers", []))
        if _declared_body_exceeds_limit(headers, self.max_body_bytes):
            await _send_json_response(send, 413, _REQUEST_TOO_LARGE_BODY)
            return
        declared_length = _declared_content_length(headers)
        initial_body_bytes = declared_length if declared_length is not None else self.max_body_bytes
        decision = self.admission_controller.try_acquire(
            admission_partition,
            initial_body_bytes,
            memory_amplification_factor=_request_memory_amplification_factor(
                headers,
                self.admission_controller.memory_amplification_factor,
            ),
        )
        if decision.lease is None:
            _log_admission_rejection(self.admission_controller, decision.reason)
            await _send_json_response(send, 503, _REQUEST_BODY_ADMISSION_UNAVAILABLE_BODY, retry_after=True)
            return
        await self._call_eager_admitted(
            scope,
            receive,
            send,
            decision.lease,
            declared_length=declared_length,
        )

    async def _call_eager_admitted(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        lease: RequestBodyAdmissionLease,
        *,
        declared_length: int | None,
    ) -> None:
        body = bytearray()
        lease_released = False

        def release_lease() -> None:
            nonlocal lease_released
            if lease_released:
                return
            lease_released = True
            lease.release()

        try:
            deadline = asyncio.get_running_loop().time() + self.read_timeout_seconds
            received_bytes = 0
            try:
                async with asyncio.timeout_at(deadline):
                    while True:
                        if asyncio.get_running_loop().time() >= deadline:
                            raise TimeoutError
                        message = await receive()
                        if message["type"] != "http.request":
                            _log_request_body_outcome(
                                "request_body_read_disconnected",
                                RequestBodyAdmissionReason.CLIENT_DISCONNECTED,
                                self.admission_controller,
                            )
                            return
                        chunk = bytes(message.get("body", b""))
                        projected_bytes = received_bytes + len(chunk)
                        if projected_bytes > self.max_body_bytes:
                            await _send_json_response(send, 413, _REQUEST_TOO_LARGE_BODY)
                            return
                        if not lease.reserve_to(projected_bytes):
                            await _send_json_response(
                                send,
                                503,
                                _REQUEST_BODY_ADMISSION_UNAVAILABLE_BODY,
                                retry_after=True,
                            )
                            return
                        received_bytes = projected_bytes
                        more_body = bool(message.get("more_body", False))
                        body.extend(chunk)
                        if not more_body:
                            break
            except TimeoutError:
                _log_request_body_outcome(
                    "request_body_read_failed",
                    RequestBodyAdmissionReason.READ_TIMEOUT,
                    self.admission_controller,
                )
                await _send_json_response(send, 408, _REQUEST_BODY_READ_TIMEOUT_BODY)
                return
            if declared_length is not None and received_bytes != declared_length:
                await _send_json_response(send, 400, _INCOMPLETE_REQUEST_BODY)
                return
            replay = _ResponseAwareBodyReplay(
                body,
                receive,
                release_ingress=release_lease,
                read_timeout_seconds=self.read_timeout_seconds,
                admission_controller=self.admission_controller,
            )

            async def tracked_send(message: Message) -> None:
                if message["type"] == "http.response.start":
                    replay.mark_response_started()
                await send(message)

            await self.app(scope, replay, tracked_send)
        except asyncio.CancelledError:
            _log_request_body_outcome(
                "request_body_read_cancelled",
                RequestBodyAdmissionReason.CANCELLED,
                self.admission_controller,
            )
            raise
        finally:
            body.clear()
            release_lease()

    async def _call_with_lazy_admission(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        admission_partition: str,
    ) -> None:
        async def acquire() -> RequestBodyAdmissionLease:
            headers = list(scope.get("headers", []))
            decision = self.admission_controller.try_acquire(
                admission_partition,
                self.max_body_bytes,
                memory_amplification_factor=_request_memory_amplification_factor(
                    headers,
                    self.admission_controller.memory_amplification_factor,
                ),
            )
            if decision.lease is None:
                _log_admission_rejection(self.admission_controller, decision.reason)
                raise _RequestBodyAdmissionUnavailable()
            return decision.lease

        await self._call_admitted(
            scope,
            receive,
            send,
            None,
            declared_length=None,
            lazy_acquire=acquire,
        )

    async def _call_admitted(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        lease: RequestBodyAdmissionLease | None,
        *,
        declared_length: int | None,
        lazy_acquire: Callable[[], Awaitable[RequestBodyAdmissionLease]] | None = None,
    ) -> None:
        received_bytes = 0
        response_started = False
        body_complete = False
        body_failed = False
        body = bytearray()
        deadline: float | None = None
        response_sender_task: asyncio.Task[Any] | None = None

        def release_lease() -> None:
            nonlocal lease
            if lease is None:
                return
            lease.release()
            lease = None

        async def observe_disconnect(*, terminal_failure: bool) -> Message:
            if response_started:
                return await _observe_post_response_disconnect(
                    receive,
                    response_sender_task=response_sender_task,
                    timeout_seconds=self.read_timeout_seconds,
                    admission_controller=self.admission_controller,
                    terminal_failure=terminal_failure,
                )
            while True:
                message = await receive()
                if message.get("type") == "http.disconnect":
                    _log_request_body_outcome(
                        "request_body_read_disconnected",
                        RequestBodyAdmissionReason.CLIENT_DISCONNECTED,
                        self.admission_controller,
                    )
                    return message
                if message.get("type") != "http.request":
                    raise _RequestDisconnected()

        async def terminate_response_listener() -> Message:
            nonlocal body_complete, body_failed, deadline
            body_complete = True
            body_failed = True
            deadline = None
            body.clear()
            release_lease()
            return await observe_disconnect(terminal_failure=True)

        async def read_body_frames() -> Message | None:
            nonlocal body_complete, deadline, received_bytes
            if deadline is None or lease is None:
                raise RuntimeError("request body read authority is unavailable")
            while True:
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError
                message = await receive()
                if message["type"] == "http.disconnect":
                    _log_request_body_outcome(
                        "request_body_read_disconnected",
                        RequestBodyAdmissionReason.CLIENT_DISCONNECTED,
                        self.admission_controller,
                    )
                    if response_started:
                        body.clear()
                        body_complete = True
                        deadline = None
                        release_lease()
                        return message
                    raise _RequestDisconnected()
                if message["type"] != "http.request":
                    raise _RequestDisconnected()

                raw_chunk = message.get("body", b"")
                if not isinstance(raw_chunk, (bytes, bytearray, memoryview)):
                    raise _RequestDisconnected()
                chunk = bytes(raw_chunk)
                projected_bytes = received_bytes + len(chunk)
                if projected_bytes > self.max_body_bytes:
                    raise _RequestBodyTooLarge()
                if not lease.reserve_to(projected_bytes):
                    raise _RequestBodyAdmissionUnavailable()
                received_bytes = projected_bytes
                more_body = bool(message.get("more_body", False))
                if not more_body and declared_length is not None and received_bytes != declared_length:
                    raise _IncompleteRequestBody()
                if response_started:
                    body.clear()
                    body_complete = not more_body
                    if body_complete:
                        deadline = None
                        release_lease()
                        return None
                    continue
                body.extend(chunk)
                if more_body:
                    continue
                body_complete = True
                canonical_body = bytes(body)
                body.clear()
                return {"type": "http.request", "body": canonical_body, "more_body": False}

        async def limited_receive() -> Message:
            nonlocal body_complete, deadline, lease, received_bytes
            if lease is None and not body_complete:
                # Starlette's pre-ASGI-2.4 StreamingResponse races its response task
                # with a request-side disconnect listener. Give the response task one
                # checkpoint so a completed stream can cancel its listener without
                # reserving request-body capacity.
                await asyncio.sleep(0)

            while True:
                if body_complete:
                    return await observe_disconnect(terminal_failure=body_failed)

                if lease is None:
                    if lazy_acquire is None:
                        raise RuntimeError("request body admission owner is unavailable")
                    try:
                        lease = await lazy_acquire()
                    except _RequestBodyAdmissionUnavailable:
                        if response_started:
                            return await terminate_response_listener()
                        raise
                if deadline is None:
                    deadline = asyncio.get_running_loop().time() + self.read_timeout_seconds
                try:
                    async with asyncio.timeout_at(deadline):
                        message = await read_body_frames()
                except TimeoutError:
                    _log_request_body_outcome(
                        "request_body_read_failed",
                        RequestBodyAdmissionReason.READ_TIMEOUT,
                        self.admission_controller,
                    )
                    if response_started:
                        return await terminate_response_listener()
                    raise _RequestBodyReadTimedOut() from None
                except (
                    _IncompleteRequestBody,
                    _RequestBodyAdmissionUnavailable,
                    _RequestBodyTooLarge,
                    _RequestDisconnected,
                ):
                    if response_started:
                        return await terminate_response_listener()
                    raise
                if message is None:
                    return await observe_disconnect(terminal_failure=False)
                return message

        async def tracked_send(message: Message) -> None:
            nonlocal response_sender_task, response_started
            if message["type"] == "http.response.start":
                if not body_complete:
                    message = _force_http1_connection_close(scope, message)
                response_started = True
                response_sender_task = asyncio.current_task()
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _RequestBodyTooLarge:
            _ensure_response_not_started(response_started, "Request body exceeded the limit")
            await _send_json_response(send, 413, _REQUEST_TOO_LARGE_BODY)
        except _IncompleteRequestBody:
            _ensure_response_not_started(response_started, "Request body length validation failed")
            await _send_json_response(send, 400, _INCOMPLETE_REQUEST_BODY)
        except _RequestBodyAdmissionUnavailable:
            _ensure_response_not_started(response_started, "Request body admission failed")
            await _send_json_response(send, 503, _REQUEST_BODY_ADMISSION_UNAVAILABLE_BODY, retry_after=True)
        except _RequestBodyReadTimedOut:
            _ensure_response_not_started(response_started, "Request body read timed out")
            await _send_json_response(send, 408, _REQUEST_BODY_READ_TIMEOUT_BODY)
        except _RequestDisconnected:
            return
        except asyncio.CancelledError:
            _log_request_body_outcome(
                "request_body_read_cancelled",
                RequestBodyAdmissionReason.CANCELLED,
                self.admission_controller,
            )
            raise
        finally:
            body.clear()
            release_lease()

    async def _authenticate(self, scope: Scope, send: Send) -> RequestAuthentication | None:
        try:
            return _authenticate_scope(self.runtime_settings, scope)
        except HTTPException as exc:
            await _send_http_exception(send, exc)
            return None


def _authenticate_scope(runtime_settings: Any, scope: Scope) -> RequestAuthentication:
    authentication = authenticate_request_headers(runtime_settings, list(scope.get("headers", [])))
    state = scope.setdefault("state", {})
    state["authenticated_actor"] = authentication.actor
    state["authenticated_tenant"] = authentication.tenant_id
    return authentication


def _ensure_response_not_started(response_started: bool, detail: str) -> None:
    if response_started:
        raise RuntimeError(f"{detail} after the response started")


def _force_http1_connection_close(scope: Scope, message: Message) -> Message:
    if scope.get("http_version") not in {"1.0", "1.1"}:
        return message
    headers = [(name, value) for name, value in message.get("headers", []) if name.lower() != b"connection"]
    headers.append((b"connection", b"close"))
    return {**message, "headers": headers}


def _requires_eager_admission(scope: Scope) -> bool:
    headers = scope.get("headers", [])
    has_body_framing = any(name.lower() in {b"content-length", b"transfer-encoding"} for name, _value in headers)
    return has_body_framing or _request_method(scope) not in _PUBLIC_METHODS


def _request_method(scope: Scope) -> str:
    return str(scope.get("method", "")).upper()


def _is_public_request(scope: Scope) -> bool:
    return _request_method(scope) in _PUBLIC_METHODS and scope.get("path") in _PUBLIC_PATHS


def _public_request_has_body(scope: Scope) -> bool:
    headers = list(scope.get("headers", []))
    if any(name.lower() == b"transfer-encoding" for name, _value in headers):
        return True
    content_lengths = [value for name, value in headers if name.lower() == b"content-length"]
    if not content_lengths:
        return False
    if len(content_lengths) != 1:
        return True
    return _normalized_declared_content_length(headers) != b"0"


def _is_disabled_documentation_request(scope: Scope, runtime_settings: Any) -> bool:
    return (
        bool(getattr(runtime_settings, "api_auth_enabled", False))
        and _request_method(scope) in _PUBLIC_METHODS
        and scope.get("path") in _DISABLED_DOCUMENTATION_PATHS
    )


class _ResponseAwareBodyReplay:
    """Deliver eager body bytes only before response headers transfer authority."""

    def __init__(
        self,
        body: bytearray,
        receive: Receive,
        *,
        release_ingress: Callable[[], None],
        read_timeout_seconds: float,
        admission_controller: RequestBodyAdmissionController,
    ) -> None:
        self._body = body
        self._receive = receive
        self._release_ingress = release_ingress
        self._read_timeout_seconds = read_timeout_seconds
        self._admission_controller = admission_controller
        self._pending = True
        self._response_started = False
        self._response_sender_task: asyncio.Task[Any] | None = None

    def mark_response_started(self) -> None:
        self._response_started = True
        self._response_sender_task = asyncio.current_task()
        self._pending = False
        self._body.clear()
        self._release_ingress()

    async def __call__(self) -> Message:
        if self._pending:
            # StreamingResponse starts its disconnect listener before its sender.
            # Let a response-first sender claim the replay before exposing bytes.
            await asyncio.sleep(0)
        if self._pending:
            self._pending = False
            body = bytes(self._body)
            self._body.clear()
            return {"type": "http.request", "body": body, "more_body": False}
        if self._response_started:
            return await _observe_post_response_disconnect(
                self._receive,
                response_sender_task=self._response_sender_task,
                timeout_seconds=self._read_timeout_seconds,
                admission_controller=self._admission_controller,
                terminal_failure=False,
            )
        message = await self._receive()
        if not self._response_started or message.get("type") == "http.disconnect":
            return message
        return await _observe_post_response_disconnect(
            self._receive,
            response_sender_task=self._response_sender_task,
            timeout_seconds=self._read_timeout_seconds,
            admission_controller=self._admission_controller,
            terminal_failure=True,
        )


async def _observe_post_response_disconnect(
    receive: Receive,
    *,
    response_sender_task: asyncio.Task[Any] | None,
    timeout_seconds: float,
    admission_controller: RequestBodyAdmissionController,
    terminal_failure: bool,
) -> Message:
    """Hide post-response request frames while preserving sender ownership."""
    loop = asyncio.get_running_loop()
    current_task = asyncio.current_task()
    separate_live_sender = (
        response_sender_task is not None
        and response_sender_task is not current_task
        and not response_sender_task.done()
    )
    observation_deadline = None if separate_live_sender else loop.time() + timeout_seconds

    async def next_disconnect() -> Message:
        await asyncio.sleep(0)
        if not terminal_failure:
            message = await receive()
            if message.get("type") == "http.disconnect":
                _log_request_body_outcome(
                    "request_body_read_disconnected",
                    RequestBodyAdmissionReason.CLIENT_DISCONNECTED,
                    admission_controller,
                )
                return message
        while True:
            if observation_deadline is not None and loop.time() >= observation_deadline:
                raise TimeoutError
            message = await receive()
            if message.get("type") == "http.disconnect":
                _log_request_body_outcome(
                    "request_body_read_disconnected",
                    RequestBodyAdmissionReason.CLIENT_DISCONNECTED,
                    admission_controller,
                )
                return message
            await asyncio.sleep(0)

    disconnect_task: asyncio.Task[Message] | None = None

    def consume_observer_result(task: asyncio.Task[Message]) -> None:
        try:
            task.exception()
        except asyncio.CancelledError:
            return

    try:
        if separate_live_sender:
            assert response_sender_task is not None
            disconnect_task = asyncio.create_task(next_disconnect())
            done, _pending = await asyncio.wait(
                {disconnect_task, response_sender_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if disconnect_task in done:
                return disconnect_task.result()
            observation_deadline = loop.time() + timeout_seconds
            async with asyncio.timeout_at(observation_deadline):
                return await disconnect_task
        async with asyncio.timeout_at(observation_deadline):
            return await next_disconnect()
    except TimeoutError:
        _log_request_body_outcome(
            "request_body_disconnect_observation_expired",
            RequestBodyAdmissionReason.DISCONNECT_OBSERVATION_TIMEOUT,
            admission_controller,
        )
        return {"type": "http.disconnect"}
    finally:
        if disconnect_task is not None and not disconnect_task.done():
            disconnect_task.cancel()
        if disconnect_task is not None:
            if current_task is not None and current_task.cancelling():
                disconnect_task.add_done_callback(consume_observer_result)
            else:
                await asyncio.gather(disconnect_task, return_exceptions=True)


def _request_memory_amplification_factor(
    headers: list[tuple[bytes, bytes]],
    default_factor: int,
) -> int:
    """Choose a non-weakenable decoded-memory envelope from the media type."""
    for name, raw_value in headers:
        if name.lower() != b"content-type":
            continue
        media_type = raw_value.split(b";", 1)[0].strip().lower()
        _main_type, separator, subtype = media_type.partition(b"/")
        if separator and (subtype == b"json" or subtype.endswith(b"+json")):
            return max(default_factor, API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR)
    return default_factor


def _log_admission_rejection(
    controller: RequestBodyAdmissionController,
    reason: RequestBodyAdmissionReason | None,
) -> None:
    bounded_reason = reason or RequestBodyAdmissionReason.GLOBAL_BYTE_BUDGET_SATURATED
    _log_request_body_outcome("request_body_admission_rejected", bounded_reason, controller)


def _log_request_body_outcome(
    event: str,
    reason: RequestBodyAdmissionReason,
    controller: RequestBodyAdmissionController,
) -> None:
    snapshot = controller.snapshot()
    logger.warning(
        event,
        reason_code=reason.value,
        active_requests=snapshot.active_requests,
        reserved_bytes=snapshot.reserved_bytes,
    )


def _declared_body_exceeds_limit(headers: list[tuple[bytes, bytes]], max_body_bytes: int) -> bool:
    normalized = _normalized_declared_content_length(headers)
    if normalized is None:
        return False
    limit = str(max_body_bytes).encode("ascii")
    return len(normalized) > len(limit) or (len(normalized) == len(limit) and normalized > limit)


def _declared_content_length(headers: list[tuple[bytes, bytes]]) -> int | None:
    normalized = _normalized_declared_content_length(headers)
    return int(normalized) if normalized is not None else None


def _normalized_declared_content_length(headers: list[tuple[bytes, bytes]]) -> bytes | None:
    values = [value.strip() for name, value in headers if name.lower() == b"content-length"]
    if len(values) != 1:
        return None
    value = values[0]
    if not value or any(byte < ord("0") or byte > ord("9") for byte in value):
        return None
    return value.lstrip(b"0") or b"0"


async def _send_http_exception(send: Send, exception: HTTPException) -> None:
    body = json.dumps({"detail": str(exception.detail)}, separators=(",", ":")).encode("utf-8")
    await _send_json_response(send, exception.status_code, body)


async def _send_json_response(
    send: Send,
    status: int,
    body: bytes,
    *,
    retry_after: bool = False,
    suppress_body: bool = False,
) -> None:
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        (b"connection", b"close"),
    ]
    if retry_after:
        headers.append((b"retry-after", b"1"))
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": b"" if suppress_body else body, "more_body": False})
