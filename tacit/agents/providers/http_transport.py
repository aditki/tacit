"""Shared HTTP transport policy for credential-bearing async LLM SDKs."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable, MutableMapping
from dataclasses import dataclass
from types import TracebackType
from typing import Literal, Self, cast

import httpx
import structlog

from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
from tacit.runtime_ownership import canonical_remote_endpoint

_CONNECT_TIMEOUT_SECONDS = 10.0
_WRITE_TIMEOUT_SECONDS = 30.0
_POOL_TIMEOUT_SECONDS = 10.0
_KEEPALIVE_EXPIRY_SECONDS = 30.0
_MAX_KEEPALIVE_CONNECTIONS = 20
_CONSTRUCTION_ROLLBACK_TIMEOUT_SECONDS = 5.0
_CONSTRUCTION_ROLLBACK_OWNER_STARTUP_TIMEOUT_SECONDS = 1.0
_CONSTRUCTION_ROLLBACK_QUARANTINE_CAPACITY = 32
_CONSTRUCTION_ROLLBACK_THREAD_NAME = "tacit-llm-constructor-rollback-quarantine"

logger = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class _ConstructionRollbackQuarantineSnapshot:
    """Bounded, non-secret state exposed only to lifecycle matrix tests."""

    capacity: int
    slots: int
    timed_out_slots: int
    owner_threads: int


class _ConstructionRollbackReservation:
    """Pre-allocation capacity for one potentially non-cooperative rollback."""

    def __init__(self, token: int) -> None:
        self.token = token
        self._lock = threading.Lock()
        self._observer_settled = threading.Event()
        self._operation: Callable[[], Awaitable[None]] | None = None
        self._task: asyncio.Task[None] | None = None
        self._observer_error: BaseException | None = None
        self._submitted = False
        self._started = False
        self._timed_out = False
        self._cancellation_sent = False
        self._completed = False
        self._released = False

    @property
    def observer_settled(self) -> threading.Event:
        return self._observer_settled

    @property
    def timed_out(self) -> bool:
        with self._lock:
            return self._timed_out and not self._completed

    def submit(self, operation: Callable[[], Awaitable[None]]) -> None:
        with self._lock:
            if self._submitted or self._released:
                raise RuntimeOwnershipError("LLM SDK rollback reservation was already settled")
            self._submitted = True
            self._operation = operation

    def operation(self) -> Callable[[], Awaitable[None]]:
        with self._lock:
            operation = self._operation
        if operation is None:
            raise RuntimeOwnershipError("LLM SDK rollback operation is unavailable")
        return operation

    def install_task(self, task: asyncio.Task[None]) -> None:
        with self._lock:
            if self._task is not None or self._completed or self._released:
                raise RuntimeOwnershipError("LLM SDK rollback task ownership was corrupted")
            self._task = task

    def mark_started(self) -> bool:
        with self._lock:
            self._started = True
            return self._timed_out

    def settle_timeout(self) -> bool:
        with self._lock:
            if self._observer_settled.is_set():
                return False
            self._timed_out = True
            self._observer_error = TimeoutError()
            self._observer_settled.set()
            return True

    def settle_dispatch_failure(self, error: BaseException) -> None:
        with self._lock:
            if self._observer_settled.is_set():
                return
            self._observer_error = error
            self._observer_settled.set()

    def claim_cancellation(self) -> asyncio.Task[None] | None:
        with self._lock:
            task = self._task
            if not self._timed_out or not self._started or self._cancellation_sent or task is None or task.done():
                return None
            self._cancellation_sent = True
            return task

    def complete(self, error: BaseException | None) -> bool:
        with self._lock:
            if self._completed:
                return False
            self._completed = True
            self._operation = None
            self._task = None
            if not self._observer_settled.is_set():
                self._observer_error = error
                self._observer_settled.set()
            return True

    def observer_error(self) -> BaseException | None:
        with self._lock:
            if not self._observer_settled.is_set():
                raise RuntimeOwnershipError("LLM SDK rollback observer has not settled")
            return self._observer_error

    def release_unsubmitted(self) -> None:
        with self._lock:
            if self._submitted or self._completed or self._released:
                raise RuntimeOwnershipError("LLM SDK rollback reservation was already consumed")
            self._released = True


class _ConstructionRollbackQuarantine:
    """One bounded process owner for cancellation-resistant constructor cleanup."""

    def __init__(self, *, capacity: int) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._startup_error_type: str | None = None
        self._next_token = 0
        self._slots: dict[int, _ConstructionRollbackReservation] = {}

    def reserve(self) -> _ConstructionRollbackReservation:
        self._ensure_started()
        with self._lock:
            if (
                self._startup_error_type is not None
                or self._loop is None
                or self._thread is None
                or not self._thread.is_alive()
            ):
                raise RuntimeOwnershipError("LLM SDK rollback quarantine owner is unavailable")
            if len(self._slots) >= self._capacity:
                raise RuntimeOwnershipError("LLM SDK rollback quarantine capacity is exhausted")
            self._next_token += 1
            reservation = _ConstructionRollbackReservation(self._next_token)
            self._slots[reservation.token] = reservation
            return reservation

    def release(self, reservation: _ConstructionRollbackReservation) -> None:
        reservation.release_unsubmitted()
        with self._lock:
            if self._slots.pop(reservation.token, None) is not reservation:
                raise RuntimeOwnershipError("LLM SDK rollback reservation belongs to another owner")

    def rollback(
        self,
        reservation: _ConstructionRollbackReservation,
        operation: Callable[[], Awaitable[None]],
        *,
        deadline: float,
    ) -> BaseException | None:
        reservation.submit(operation)
        with self._lock:
            loop = self._loop
            thread = self._thread
            owned = self._slots.get(reservation.token) is reservation
        if not owned or loop is None or thread is None or not thread.is_alive():
            error = RuntimeOwnershipError("LLM SDK rollback quarantine owner is unavailable")
            reservation.settle_dispatch_failure(error)
            return error
        try:
            loop.call_soon_threadsafe(self._start_submission, reservation)
        except BaseException as error:
            reservation.settle_dispatch_failure(error)
            return error

        remaining = max(0.0, deadline - time.monotonic())
        if not reservation.observer_settled.wait(timeout=remaining):
            if reservation.settle_timeout():
                try:
                    loop.call_soon_threadsafe(self._cancel_submission, reservation)
                except BaseException:
                    pass
        return reservation.observer_error()

    def snapshot(self) -> _ConstructionRollbackQuarantineSnapshot:
        with self._lock:
            reservations = tuple(self._slots.values())
            thread = self._thread
        return _ConstructionRollbackQuarantineSnapshot(
            capacity=self._capacity,
            slots=len(reservations),
            timed_out_slots=sum(reservation.timed_out for reservation in reservations),
            owner_threads=int(thread is not None and thread.is_alive()),
        )

    def _ensure_started(self) -> None:
        with self._lock:
            thread = self._thread
            if thread is None and self._startup_error_type is None:
                thread = threading.Thread(
                    target=self._run,
                    name=_CONSTRUCTION_ROLLBACK_THREAD_NAME,
                    daemon=True,
                )
                self._thread = thread
                try:
                    thread.start()
                except BaseException as error:
                    self._startup_error_type = type(error).__name__[:128]
                    self._ready.set()
                    raise RuntimeOwnershipError("LLM SDK rollback quarantine owner could not start") from error
        if not self._ready.wait(timeout=_CONSTRUCTION_ROLLBACK_OWNER_STARTUP_TIMEOUT_SECONDS):
            with self._lock:
                self._startup_error_type = "TimeoutError"
            raise RuntimeOwnershipError("LLM SDK rollback quarantine owner readiness timed out")
        with self._lock:
            startup_error_type = self._startup_error_type
        if startup_error_type is not None:
            raise RuntimeOwnershipError(f"LLM SDK rollback quarantine owner is unavailable ({startup_error_type})")

    def _run(self) -> None:
        try:
            loop = asyncio.new_event_loop()
        except BaseException as error:
            with self._lock:
                self._startup_error_type = type(error).__name__[:128]
                self._ready.set()
            return
        with self._lock:
            if self._startup_error_type is None:
                self._loop = loop
            should_run = self._loop is loop
            self._ready.set()
        if not should_run:
            loop.close()
            return
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def _start_submission(self, reservation: _ConstructionRollbackReservation) -> None:
        operation = reservation.operation()

        async def execute() -> None:
            if reservation.mark_started():
                asyncio.get_running_loop().call_soon(self._cancel_submission, reservation)
            await operation()

        try:
            task = asyncio.create_task(execute())
            reservation.install_task(task)
        except BaseException as error:
            reservation.settle_dispatch_failure(error)
            return
        task.add_done_callback(lambda completed: self._complete_submission(reservation, completed))

    @staticmethod
    def _cancel_submission(reservation: _ConstructionRollbackReservation) -> None:
        task = reservation.claim_cancellation()
        if task is not None:
            task.cancel()

    def _complete_submission(
        self,
        reservation: _ConstructionRollbackReservation,
        task: asyncio.Task[None],
    ) -> None:
        try:
            task.result()
        except BaseException as error:
            cleanup_error: BaseException | None = error
        else:
            cleanup_error = None
        with self._lock:
            if self._slots.get(reservation.token) is reservation:
                self._slots.pop(reservation.token)
        reservation.complete(cleanup_error)


_CONSTRUCTION_ROLLBACK_QUARANTINE = _ConstructionRollbackQuarantine(capacity=_CONSTRUCTION_ROLLBACK_QUARANTINE_CAPACITY)


def _construction_rollback_quarantine_snapshot() -> _ConstructionRollbackQuarantineSnapshot:
    return _CONSTRUCTION_ROLLBACK_QUARANTINE.snapshot()


@dataclass(frozen=True, slots=True)
class LLMSDKHTTPPolicy:
    """Bounded transport resources derived from the existing runtime owner."""

    follow_redirects: bool
    trust_env: bool
    connect_timeout_seconds: float
    read_timeout_seconds: float
    write_timeout_seconds: float
    pool_timeout_seconds: float
    max_connections: int
    max_keepalive_connections: int
    keepalive_expiry_seconds: float


def llm_sdk_http_policy(runtime_settings: Settings) -> LLMSDKHTTPPolicy:
    """Return transport bounds without creating a second admission authority."""
    operation_timeout = float(runtime_settings.pipeline_timeout_seconds)
    max_connections = int(runtime_settings.pipeline_max_concurrent)
    return LLMSDKHTTPPolicy(
        follow_redirects=False,
        trust_env=False,
        connect_timeout_seconds=min(_CONNECT_TIMEOUT_SECONDS, operation_timeout),
        read_timeout_seconds=operation_timeout,
        write_timeout_seconds=min(_WRITE_TIMEOUT_SECONDS, operation_timeout),
        pool_timeout_seconds=min(_POOL_TIMEOUT_SECONDS, operation_timeout),
        max_connections=max_connections,
        max_keepalive_connections=min(max_connections, _MAX_KEEPALIVE_CONNECTIONS),
        keepalive_expiry_seconds=_KEEPALIVE_EXPIRY_SECONDS,
    )


def create_llm_sdk_http_client(
    runtime_settings: Settings,
    *,
    endpoint: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Create one provider-owned HTTP client pinned to its admitted endpoint."""
    canonical_endpoint = canonical_remote_endpoint(endpoint)
    policy = llm_sdk_http_policy(runtime_settings)
    return httpx.AsyncClient(
        base_url=canonical_endpoint,
        follow_redirects=policy.follow_redirects,
        trust_env=policy.trust_env,
        timeout=httpx.Timeout(
            connect=policy.connect_timeout_seconds,
            read=policy.read_timeout_seconds,
            write=policy.write_timeout_seconds,
            pool=policy.pool_timeout_seconds,
        ),
        limits=httpx.Limits(
            max_connections=policy.max_connections,
            max_keepalive_connections=policy.max_keepalive_connections,
            keepalive_expiry=policy.keepalive_expiry_seconds,
        ),
        transport=transport,
    )


def isolate_llm_sdk_custom_headers(sdk_client: object) -> None:
    """Remove SDK-captured ambient custom headers before the client is exposed."""
    custom_headers = getattr(sdk_client, "_custom_headers", None)
    if not isinstance(custom_headers, MutableMapping):
        raise RuntimeOwnershipError("LLM SDK custom-header isolation is unavailable")
    custom_headers.clear()


def isolate_llm_sdk_ambient_credentials(
    sdk_client: object,
    *,
    expected_fields: tuple[tuple[str, object], ...],
    normalized_fields: tuple[tuple[str, object], ...] = (),
) -> None:
    """Verify and normalize SDK credential state before lifecycle adoption."""
    missing = object()
    if any(getattr(sdk_client, name, missing) != expected for name, expected in expected_fields):
        raise RuntimeOwnershipError("LLM SDK ambient credential isolation is unavailable")
    try:
        for name, value in normalized_fields:
            setattr(sdk_client, name, value)
    except (AttributeError, TypeError) as exc:
        raise RuntimeOwnershipError("LLM SDK ambient credential isolation is unavailable") from exc
    if any(getattr(sdk_client, name, missing) != expected for name, expected in normalized_fields):
        raise RuntimeOwnershipError("LLM SDK ambient credential isolation is unavailable")


async def close_llm_sdk_http_client(
    sdk_close: Callable[[], Awaitable[None]],
    http_client: httpx.AsyncClient | None,
) -> None:
    """Close the SDK and retain provider authority over its HTTP transport."""
    try:
        await sdk_close()
    finally:
        if http_client is not None and not http_client.is_closed:
            await http_client.aclose()


class LLMSDKHTTPClientCloseGuard:
    """Serialize close attempts and permanently settle only successful cleanup."""

    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http_client = http_client
        self._lock = asyncio.Lock()
        self._closed = False

    async def close(self, sdk_close: Callable[[], Awaitable[None]]) -> None:
        async with self._lock:
            if self._closed:
                return
            await close_llm_sdk_http_client(sdk_close, self._http_client)
            self._closed = True


class LLMSDKHTTPClientConstruction:
    """Keep unadopted SDK resources with their synchronous realizing owner."""

    def __init__(self) -> None:
        self._sdk_client: object | None = None
        self._http_client: httpx.AsyncClient | None = None
        self._rollback_reservation: _ConstructionRollbackReservation | None = None
        self._entered = False
        self._committed = False
        self._rollback_attempted = False

    @classmethod
    def begin(cls) -> Self:
        """Reject an async caller before it can enter the allocation boundary."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return cls()
        raise RuntimeOwnershipError(
            "Async LLM SDK clients require a lifecycle-owned construction boundary outside a running event loop"
        )

    def __enter__(self) -> Self:
        if self._entered:
            raise RuntimeOwnershipError("LLM SDK construction boundary was already entered")
        self._rollback_reservation = _CONSTRUCTION_ROLLBACK_QUARANTINE.reserve()
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        primary_error: BaseException | None,
        traceback: TracebackType | None,
    ) -> Literal[False]:
        del exc_type, traceback
        if primary_error is not None:
            terminal_error = self._rollback_failure(primary_error)
            if terminal_error is not None:
                raise terminal_error from primary_error
        elif not self._committed:
            incomplete_error = RuntimeOwnershipError(
                "LLM SDK construction exited without transferring resource ownership"
            )
            terminal_error = self._rollback_failure(incomplete_error)
            if terminal_error is not None:
                raise terminal_error from incomplete_error
            raise incomplete_error
        else:
            reservation = self._rollback_reservation
            if reservation is None:
                raise RuntimeOwnershipError("LLM SDK construction rollback authority is unavailable")
            _CONSTRUCTION_ROLLBACK_QUARANTINE.release(reservation)
            self._rollback_reservation = None
        return False

    def create_http_client(
        self,
        factory: Callable[[], httpx.AsyncClient],
    ) -> httpx.AsyncClient:
        if not self._entered or self._rollback_reservation is None:
            raise RuntimeOwnershipError("LLM SDK construction boundary is not active")
        if self._http_client is not None:
            raise RuntimeOwnershipError("LLM SDK transport was already allocated")
        http_client = factory()
        self._http_client = http_client
        return http_client

    def create_sdk_client[ClientT](self, factory: Callable[[], ClientT]) -> ClientT:
        if not self._entered or self._rollback_reservation is None:
            raise RuntimeOwnershipError("LLM SDK construction boundary is not active")
        if self._sdk_client is not None:
            raise RuntimeOwnershipError("LLM SDK client was already allocated")
        sdk_client = factory()
        self._sdk_client = sdk_client
        return sdk_client

    def commit(self) -> LLMSDKHTTPClientCloseGuard:
        if self._committed:
            raise RuntimeOwnershipError("LLM SDK construction ownership was already transferred")
        if self._sdk_client is None or self._http_client is None:
            raise RuntimeOwnershipError("LLM SDK construction cannot transfer incomplete resources")
        reservation = self._rollback_reservation
        if reservation is None:
            raise RuntimeOwnershipError("LLM SDK construction rollback authority is unavailable")
        close_guard = LLMSDKHTTPClientCloseGuard(self._http_client)
        self._committed = True
        return close_guard

    async def _rollback(self) -> None:
        sdk_close = getattr(self._sdk_client, "close", None)
        if callable(sdk_close):
            await close_llm_sdk_http_client(
                cast(Callable[[], Awaitable[None]], sdk_close),
                self._http_client,
            )
        elif self._http_client is not None and not self._http_client.is_closed:
            await self._http_client.aclose()

    def _rollback_failure(self, primary_error: BaseException) -> RuntimeOwnershipError | None:
        if self._rollback_attempted:
            return None
        self._rollback_attempted = True
        reservation = self._rollback_reservation
        if reservation is None:
            cleanup_error: BaseException | None = RuntimeOwnershipError(
                "LLM SDK construction rollback authority is unavailable"
            )
        elif self._sdk_client is None and self._http_client is None:
            _CONSTRUCTION_ROLLBACK_QUARANTINE.release(reservation)
            self._rollback_reservation = None
            return None
        else:
            deadline = time.monotonic() + _CONSTRUCTION_ROLLBACK_TIMEOUT_SECONDS
            try:
                cleanup_error = _CONSTRUCTION_ROLLBACK_QUARANTINE.rollback(
                    reservation,
                    self._rollback,
                    deadline=deadline,
                )
            except BaseException as error:
                cleanup_error = error
            self._rollback_reservation = None
        if cleanup_error is None:
            return None
        reason_code = (
            "llm_sdk_construction_rollback_timeout"
            if isinstance(cleanup_error, TimeoutError)
            else "llm_sdk_construction_rollback_failed"
        )
        try:
            logger.warning(
                reason_code,
                reason_code=reason_code,
                primary_error_type=type(primary_error).__name__,
                cleanup_error_type=type(cleanup_error).__name__,
            )
        except BaseException:
            pass
        terminal_error = RuntimeOwnershipError("LLM SDK construction rollback did not complete")
        setattr(terminal_error, "cleanup_reason_code", reason_code)
        setattr(terminal_error, "cleanup_error_type", type(cleanup_error).__name__[:128])
        setattr(terminal_error, "cleanup_retains_capacity", True)
        setattr(terminal_error, "runtime_provider_fatal", True)
        terminal_error.__cause__ = primary_error
        terminal_error.__suppress_context__ = True
        return terminal_error
