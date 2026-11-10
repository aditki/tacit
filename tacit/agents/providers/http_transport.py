"""Shared HTTP transport policy for credential-bearing async LLM SDKs."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine, MutableMapping
from contextvars import ContextVar
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Literal, Self, cast

import httpx
import structlog

from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline_admission import _LifecycleOwnerStartupError, _start_lifecycle_owner_thread
from tacit.runtime_ownership import canonical_remote_endpoint

_CONNECT_TIMEOUT_SECONDS = 10.0
_WRITE_TIMEOUT_SECONDS = 30.0
_POOL_TIMEOUT_SECONDS = 10.0
_KEEPALIVE_EXPIRY_SECONDS = 30.0
_MAX_KEEPALIVE_CONNECTIONS = 20
_CONSTRUCTION_ROLLBACK_TIMEOUT_SECONDS = 5.0
_CONSTRUCTION_ROLLBACK_OWNER_STARTUP_TIMEOUT_SECONDS = 1.0
_CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MIN_SECONDS = 0.01
_CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MAX_SECONDS = 0.25
_CONSTRUCTION_ROLLBACK_QUARANTINE_CAPACITY = 32
_CONSTRUCTION_ROLLBACK_THREAD_NAME = "tacit-llm-constructor-rollback-quarantine"
_CONSTRUCTION_ROLLBACK_STARTUP_THREAD_NAME = "tacit-llm-constructor-rollback-startup"

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
        # Requester-visible observer state. The quarantine owner may only enter
        # this lock through nonblocking terminal-publication methods.
        self._lock = threading.Lock()
        self._observer_settled = threading.Event()
        self._observer_error: BaseException | None = None
        self._submitted = False
        self._timeout_requested = threading.Event()
        self._completed = False
        self._released = False
        self._operation: Callable[[], Awaitable[None]] | None = None
        # Quarantine-loop state. Only the single durable owner mutates it.
        self._start_claimed = False
        self._start_retry_pending = False
        self._task: asyncio.Task[None] | None = None
        self._started = False
        self._started_signal = threading.Event()
        self._cancellation_requested = False
        self._owner_task_completed = False
        self._owner_task_error: BaseException | None = None
        self._descendant_tasks: set[asyncio.Task[object]] = set()
        self._descendant_error: BaseException | None = None
        self._unsupported_work_error: BaseException | None = None
        self._last_start_error: BaseException | None = None
        self._publication_retry_pending = False

    @property
    def observer_settled(self) -> threading.Event:
        return self._observer_settled

    @property
    def timed_out(self) -> bool:
        return self._timeout_requested.is_set() and not self._completed

    def submit(self, operation: Callable[[], Awaitable[None]]) -> None:
        if self._submitted or self._released:
            raise RuntimeOwnershipError("LLM SDK rollback reservation was already settled")
        self._submitted = True
        self._operation = operation

    def request_timeout(self) -> None:
        self._timeout_requested.set()

    def owner_claim_start(self) -> Callable[[], Awaitable[None]] | None:
        if self._start_claimed or self._task is not None or self._owner_task_completed:
            return None
        operation = self._operation
        if operation is None:
            raise RuntimeOwnershipError("LLM SDK rollback operation is unavailable")
        self._start_claimed = True
        return operation

    def owner_start_failed(self, error: BaseException) -> None:
        if not self._start_claimed or self._task is not None or self._owner_task_completed:
            raise RuntimeOwnershipError("LLM SDK rollback start ownership was corrupted")
        self._start_claimed = False
        self._last_start_error = error

    def owner_claim_start_retry(self) -> bool:
        if self._start_retry_pending:
            return False
        self._start_retry_pending = True
        return True

    def owner_begin_start_retry(self) -> None:
        if not self._start_retry_pending:
            raise RuntimeOwnershipError("LLM SDK rollback start retry ownership was corrupted")
        self._start_retry_pending = False

    def owner_install_task(self, task: asyncio.Task[None]) -> None:
        if not self._start_claimed or self._task is not None or self._owner_task_completed:
            raise RuntimeOwnershipError("LLM SDK rollback task ownership was corrupted")
        self._task = task

    def owner_mark_started(self) -> bool:
        if not self._start_claimed or self._started or self._owner_task_completed:
            raise RuntimeOwnershipError("LLM SDK rollback start ownership was corrupted")
        self._started = True
        self._started_signal.set()
        return self._cancellation_requested

    def start_failure(self) -> BaseException | None:
        if self._started_signal.is_set():
            return None
        return self._last_start_error

    def owner_claim_cancellation(self) -> tuple[asyncio.Task[object], ...]:
        self._cancellation_requested = True
        tasks: list[asyncio.Task[object]] = []
        task = self._task
        if self._started and task is not None and not task.done():
            tasks.append(cast(asyncio.Task[object], task))
        tasks.extend(task for task in self._descendant_tasks if not task.done())
        return tuple(tasks)

    def owner_track_descendant(self, task: asyncio.Task[object]) -> bool:
        if self._released or task in self._descendant_tasks:
            raise RuntimeOwnershipError("LLM SDK rollback descendant ownership was corrupted")
        self._descendant_tasks.add(task)
        return self._cancellation_requested

    def owner_complete_task(
        self,
        task: asyncio.Task[None],
        error: BaseException | None,
    ) -> tuple[bool, BaseException | None]:
        if self._task is not task or self._owner_task_completed:
            raise RuntimeOwnershipError("LLM SDK rollback completion ownership was corrupted")
        self._owner_task_completed = True
        self._owner_task_error = error
        self._task = None
        return self._retirement_outcome()

    def owner_complete_descendant(
        self,
        task: asyncio.Task[object],
        error: BaseException | None,
    ) -> tuple[bool, BaseException | None]:
        if task not in self._descendant_tasks:
            raise RuntimeOwnershipError("LLM SDK rollback descendant retirement was corrupted")
        self._descendant_tasks.remove(task)
        if error is not None and self._descendant_error is None:
            self._descendant_error = error
        return self._retirement_outcome()

    def owner_fence_unsupported_work(self, error: BaseException) -> None:
        if self._unsupported_work_error is None:
            self._unsupported_work_error = error

    def unsupported_work_error(self) -> BaseException | None:
        return self._unsupported_work_error

    def _retirement_outcome(self) -> tuple[bool, BaseException | None]:
        cleanup_error = self._owner_task_error or self._descendant_error
        return (
            self._owner_task_completed and not self._descendant_tasks and self._unsupported_work_error is None,
            cleanup_error,
        )

    def owner_release_slot(self) -> None:
        if not self._owner_task_completed or self._descendant_tasks or self._unsupported_work_error is not None:
            raise RuntimeOwnershipError("LLM SDK rollback slot was released before cleanup retired")
        self._operation = None

    def owner_claim_publication_retry(self) -> bool:
        if self._publication_retry_pending:
            return False
        self._publication_retry_pending = True
        return True

    def owner_begin_publication_retry(self) -> None:
        if not self._publication_retry_pending:
            raise RuntimeOwnershipError("LLM SDK rollback publication retry ownership was corrupted")
        self._publication_retry_pending = False

    def try_complete(self, error: BaseException | None) -> bool:
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if self._completed:
                return True
            self._completed = True
            if not self._observer_settled.is_set():
                self._observer_error = error
                self._observer_settled.set()
            return True
        finally:
            self._lock.release()

    def observer_error(self) -> BaseException | None:
        if not self._observer_settled.is_set():
            raise RuntimeOwnershipError("LLM SDK rollback observer has not settled")
        return self._observer_error

    def release_unsubmitted(self) -> None:
        if self._submitted or self._completed or self._released:
            raise RuntimeOwnershipError("LLM SDK rollback reservation was already consumed")
        self._released = True


_ACTIVE_CONSTRUCTION_ROLLBACK: ContextVar[_ConstructionRollbackReservation | None] = ContextVar(
    "tacit_active_construction_rollback",
    default=None,
)


class _ConstructionRollbackQuarantine:
    """One bounded process owner for cancellation-resistant constructor cleanup."""

    def __init__(self, *, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("LLM SDK rollback quarantine capacity must be positive")
        self._capacity = capacity
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._startup_error_type: str | None = None
        self._startup_failure_phase: str | None = None
        self._startup_abort = threading.Event()
        self._owner_finished = threading.Event()
        self._startup_transition_started = False
        self._startup_transition_thread: threading.Thread | None = None
        self._startup_transition_finished = threading.Event()
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
        loop = self._loop
        thread = self._thread
        if loop is None or thread is None or not thread.is_alive():
            return RuntimeOwnershipError("LLM SDK rollback quarantine owner is unavailable")
        dispatch_error = self._dispatch_submission(
            loop,
            reservation,
            deadline=deadline,
        )
        if dispatch_error is not None:
            reservation.request_timeout()
            timeout_error = TimeoutError()
            timeout_error.__cause__ = dispatch_error
            return timeout_error

        remaining = max(0.0, deadline - time.monotonic())
        if reservation.observer_settled.wait(timeout=remaining):
            return reservation.observer_error()
        reservation.request_timeout()
        timeout_error = TimeoutError()
        try:
            loop.call_soon_threadsafe(self._cancel_submission, reservation)
        except BaseException as error:
            timeout_error.__cause__ = error
        start_error = reservation.start_failure()
        if start_error is not None:
            start_failure = RuntimeOwnershipError("LLM SDK rollback task could not be created")
            start_failure.__cause__ = start_error
            return start_failure
        return timeout_error

    def _dispatch_submission(
        self,
        loop: asyncio.AbstractEventLoop,
        reservation: _ConstructionRollbackReservation,
        *,
        deadline: float,
    ) -> BaseException | None:
        retry_seconds = _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MIN_SECONDS
        while True:
            try:
                loop.call_soon_threadsafe(self._start_submission, reservation)
            except BaseException as error:
                if reservation.observer_settled.is_set():
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return error
                time.sleep(min(retry_seconds, remaining))
                retry_seconds = min(
                    retry_seconds * 2,
                    _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MAX_SECONDS,
                )
                continue
            return None

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
        transition_to_start: threading.Thread | None = None
        with self._lock:
            thread = self._thread
            if thread is not None and self._loop is not None and thread.is_alive() and self._startup_error_type is None:
                return
            if not self._startup_transition_started and self._startup_error_type is None:
                transition_to_start = threading.Thread(
                    target=self._start_owner_transition,
                    name=_CONSTRUCTION_ROLLBACK_STARTUP_THREAD_NAME,
                    daemon=True,
                )
                self._startup_transition_started = True
                self._startup_transition_thread = transition_to_start
        if transition_to_start is not None:
            try:
                transition_to_start.start()
            except BaseException as error:
                ambiguous = bool(transition_to_start.ident is not None or transition_to_start.is_alive())
                self._record_startup_failure(
                    error,
                    phase="transition_start_ambiguous" if ambiguous else "transition_start",
                )
                raise RuntimeOwnershipError(
                    "LLM SDK rollback quarantine owner is unavailable"
                    if ambiguous
                    else "LLM SDK rollback quarantine owner could not start"
                ) from error
        if not self._ready.wait(timeout=_CONSTRUCTION_ROLLBACK_OWNER_STARTUP_TIMEOUT_SECONDS):
            self._record_startup_failure(TimeoutError("owner startup timed out"), phase="readiness")
            raise RuntimeOwnershipError("LLM SDK rollback quarantine owner readiness timed out")
        with self._lock:
            startup_error_type = self._startup_error_type
            startup_failure_phase = self._startup_failure_phase
        if startup_error_type is not None:
            if startup_failure_phase in {"construction", "registration", "start", "transition_start"}:
                raise RuntimeOwnershipError("LLM SDK rollback quarantine owner could not start")
            raise RuntimeOwnershipError(f"LLM SDK rollback quarantine owner is unavailable ({startup_error_type})")

    def _start_owner_transition(self) -> None:
        try:
            _start_lifecycle_owner_thread(
                target=self._run,
                name=_CONSTRUCTION_ROLLBACK_THREAD_NAME,
                install=self._install_owner_thread,
                abort=self._abort_owner_startup,
                ready=self._ready,
                readiness_error=self._readiness_error,
                finished=self._owner_finished,
                daemon=True,
                timeout_seconds=_CONSTRUCTION_ROLLBACK_OWNER_STARTUP_TIMEOUT_SECONDS,
            )
        except _LifecycleOwnerStartupError as error:
            phase = error.phase if not error.thread_alive else f"{error.phase}_ambiguous"
            self._record_startup_failure(error.cause, phase=phase)
        except BaseException as error:
            self._record_startup_failure(error, phase="transition")
        finally:
            self._startup_transition_finished.set()

    def _install_owner_thread(self, thread: threading.Thread) -> None:
        with self._lock:
            if self._thread is not None and self._thread is not thread:
                raise RuntimeOwnershipError("LLM SDK rollback quarantine owner was already installed")
            self._thread = thread

    def _record_startup_failure(self, error: BaseException, *, phase: str) -> None:
        with self._lock:
            if self._startup_error_type is None:
                self._startup_error_type = type(error).__name__[:128]
                self._startup_failure_phase = phase
        self._abort_owner_startup()
        self._ready.set()

    def _abort_owner_startup(self) -> None:
        self._startup_abort.set()
        with self._lock:
            loop = self._loop
        if loop is not None and not loop.is_closed():
            try:
                loop.call_soon_threadsafe(loop.stop)
            except RuntimeError:
                pass

    def _readiness_error(self) -> BaseException | None:
        with self._lock:
            startup_error_type = self._startup_error_type
        if startup_error_type is None:
            return None
        return RuntimeOwnershipError(f"LLM SDK rollback quarantine owner is unavailable ({startup_error_type})")

    def _run(self) -> None:
        try:
            loop = asyncio.new_event_loop()
        except BaseException as error:
            self._record_startup_failure(error, phase="loop_construction")
            self._owner_finished.set()
            return
        try:
            loop.set_task_factory(self._create_owned_task)
            setattr(loop, "run_in_executor", self._reject_executor_offload)
        except BaseException as error:
            self._record_startup_failure(error, phase="loop_policy")
            loop.close()
            self._owner_finished.set()
            return
        with self._lock:
            if self._startup_error_type is None and not self._startup_abort.is_set():
                self._loop = loop
            should_run = self._loop is loop
            self._ready.set()
        if not should_run:
            loop.close()
            self._owner_finished.set()
            return
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            loop.close()
            self._owner_finished.set()

    def _start_submission(
        self,
        reservation: _ConstructionRollbackReservation,
    ) -> None:
        operation = reservation.owner_claim_start()
        if operation is None:
            return

        async def execute() -> None:
            context_token = _ACTIVE_CONSTRUCTION_ROLLBACK.set(reservation)
            try:
                if reservation.owner_mark_started():
                    asyncio.get_running_loop().call_soon(self._cancel_submission, reservation)
                await operation()
            finally:
                _ACTIVE_CONSTRUCTION_ROLLBACK.reset(context_token)

        coroutine = execute()
        try:
            task = asyncio.create_task(coroutine)
        except BaseException as error:
            coroutine.close()
            reservation.owner_start_failed(error)
            self._retry_start_submission(
                reservation,
                _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MIN_SECONDS,
            )
            return
        try:
            reservation.owner_install_task(task)
        except BaseException as error:
            task.cancel()
            task.add_done_callback(self._consume_uninstalled_task)
            reservation.owner_start_failed(error)
            self._retry_start_submission(
                reservation,
                _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MIN_SECONDS,
            )
            return
        task.add_done_callback(lambda completed: self._complete_submission(reservation, completed))

    def _create_owned_task(
        self,
        loop: asyncio.AbstractEventLoop,
        coroutine: Coroutine[Any, Any, Any],
        context: Any = None,
    ) -> asyncio.Task[Any]:
        task = asyncio.Task(coroutine, loop=loop, context=context)
        reservation = (
            _ACTIVE_CONSTRUCTION_ROLLBACK.get() if context is None else context.get(_ACTIVE_CONSTRUCTION_ROLLBACK, None)
        )
        if reservation is None:
            return task
        try:
            cancel_now = reservation.owner_track_descendant(cast(asyncio.Task[object], task))
        except BaseException as error:
            reservation.owner_fence_unsupported_work(error)
            task.cancel()
            self._publish_fenced_completion(reservation, error)
            return task
        task.add_done_callback(
            lambda completed: self._complete_descendant(
                reservation,
                cast(asyncio.Task[object], completed),
            )
        )
        if cancel_now:
            task.cancel()
        return task

    def _reject_executor_offload(
        self,
        _executor: object,
        _function: Callable[..., object],
        *_args: object,
    ) -> Any:
        error = RuntimeOwnershipError("LLM SDK rollback executor offload is unsupported")
        reservation = _ACTIVE_CONSTRUCTION_ROLLBACK.get()
        if reservation is None:
            raise error
        reservation.owner_fence_unsupported_work(error)
        self._publish_fenced_completion(reservation, error)
        raise error

    def _retry_start_submission(
        self,
        reservation: _ConstructionRollbackReservation,
        retry_seconds: float,
    ) -> None:
        if not reservation.owner_claim_start_retry():
            return
        asyncio.get_running_loop().call_later(
            retry_seconds,
            self._run_start_retry,
            reservation,
            min(
                retry_seconds * 2,
                _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MAX_SECONDS,
            ),
        )

    def _run_start_retry(
        self,
        reservation: _ConstructionRollbackReservation,
        retry_seconds: float,
    ) -> None:
        reservation.owner_begin_start_retry()
        self._start_submission(reservation)

    @staticmethod
    def _consume_uninstalled_task(task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except BaseException:
            pass

    @staticmethod
    def _cancel_submission(reservation: _ConstructionRollbackReservation) -> None:
        for task in reservation.owner_claim_cancellation():
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

        try:
            ready_to_publish, cleanup_error = reservation.owner_complete_task(task, cleanup_error)
        except BaseException as ownership_error:
            if cleanup_error is None:
                cleanup_error = ownership_error
            ready_to_publish = False

        unsupported_error = reservation.unsupported_work_error()
        if unsupported_error is not None:
            self._publish_fenced_completion(reservation, unsupported_error)
            return
        if ready_to_publish:
            self._publish_completion(reservation, cleanup_error)

    def _complete_descendant(
        self,
        reservation: _ConstructionRollbackReservation,
        task: asyncio.Task[object],
    ) -> None:
        try:
            task.result()
        except BaseException as error:
            cleanup_error: BaseException | None = error
        else:
            cleanup_error = None
        try:
            ready_to_publish, cleanup_error = reservation.owner_complete_descendant(
                task,
                cleanup_error,
            )
        except BaseException as ownership_error:
            reservation.owner_fence_unsupported_work(ownership_error)
            self._publish_fenced_completion(reservation, ownership_error)
            return
        unsupported_error = reservation.unsupported_work_error()
        if unsupported_error is not None:
            self._publish_fenced_completion(reservation, unsupported_error)
            return
        if ready_to_publish:
            self._publish_completion(reservation, cleanup_error)

    def _publish_fenced_completion(
        self,
        reservation: _ConstructionRollbackReservation,
        cleanup_error: BaseException,
        retry_seconds: float = _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MIN_SECONDS,
    ) -> None:
        if reservation.try_complete(cleanup_error):
            return
        self._retry_publication(
            reservation,
            self._publish_fenced_completion,
            cleanup_error,
            retry_seconds,
        )

    def _publish_completion(
        self,
        reservation: _ConstructionRollbackReservation,
        cleanup_error: BaseException | None,
        retry_seconds: float = _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MIN_SECONDS,
    ) -> None:
        if not reservation.try_complete(cleanup_error):
            self._retry_publication(
                reservation,
                self._publish_completion,
                cleanup_error,
                retry_seconds,
            )
            return
        if not self._lock.acquire(blocking=False):
            self._retry_publication(
                reservation,
                self._publish_completion,
                cleanup_error,
                retry_seconds,
            )
            return
        try:
            if self._slots.get(reservation.token) is reservation:
                self._slots.pop(reservation.token)
                reservation.owner_release_slot()
        finally:
            self._lock.release()

    def _retry_publication(
        self,
        reservation: _ConstructionRollbackReservation,
        publisher: Callable[..., None],
        outcome: object,
        retry_seconds: float,
    ) -> None:
        if not reservation.owner_claim_publication_retry():
            return
        asyncio.get_running_loop().call_later(
            retry_seconds,
            self._run_publication_retry,
            reservation,
            publisher,
            outcome,
            min(
                retry_seconds * 2,
                _CONSTRUCTION_ROLLBACK_PUBLICATION_RETRY_MAX_SECONDS,
            ),
        )

    @staticmethod
    def _run_publication_retry(
        reservation: _ConstructionRollbackReservation,
        publisher: Callable[..., None],
        outcome: object,
        retry_seconds: float,
    ) -> None:
        reservation.owner_begin_publication_retry()
        publisher(reservation, outcome, retry_seconds)


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
