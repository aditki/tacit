"""App-scoped status for optional integrations."""

from __future__ import annotations

import asyncio
import queue
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import Executor
from concurrent.futures import Future as ThreadFuture
from contextvars import Context
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypedDict, cast

import structlog
from fastapi import FastAPI

from tacit.pipeline_admission import PipelineAdmissionController, PipelineBlockingPermit

logger = structlog.get_logger()

OptionalIntegrationStatus = Literal[
    "disabled",
    "starting",
    "ready",
    "reconnecting",
    "failed",
    "stopped",
]


class OptionalIntegrationSnapshot(TypedDict):
    """Stable, disclosure-safe integration state exposed to operators."""

    status: OptionalIntegrationStatus
    reason_code: str


class OptionalIntegrationLifecycleProtocol(Protocol):
    """Lifecycle surface shared by one-app and overlapping-app owners."""

    @property
    def callback_authority_active(self) -> bool: ...

    @property
    def detached_task_count(self) -> int: ...

    def publish(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None: ...

    def revoke(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None: ...

    def retain_detached_task(self, task: asyncio.Task[object]) -> None: ...


_VALID_STATUSES = frozenset({"disabled", "starting", "ready", "reconnecting", "failed", "stopped"})
_DEGRADED_STATUSES = frozenset({"starting", "reconnecting", "failed", "stopped"})
_MAX_OPTIONAL_INTEGRATION_EXECUTION_OWNERS = 64
_MAX_OPTIONAL_INTEGRATION_SUBSCRIPTIONS = 256
_MAX_OPTIONAL_INTEGRATION_EXECUTOR_WORKERS = 4
_MAX_OPTIONAL_INTEGRATION_EXECUTOR_WORK_ITEMS = 32
_MAX_OPTIONAL_INTEGRATION_USER_WORK_ITEMS = _MAX_OPTIONAL_INTEGRATION_EXECUTOR_WORK_ITEMS - 2
_MAX_OPTIONAL_INTEGRATION_CALLBACK_DRAIN_TURNS = 4096
_MAX_OPTIONAL_INTEGRATION_PENDING_TIMER_HANDLES = 4096
_MIN_OPTIONAL_INTEGRATION_OWNER_RETRY_SECONDS = 0.001
_MAX_OPTIONAL_INTEGRATION_OWNER_RETRY_SECONDS = 0.25
_OPTIONAL_INTEGRATION_EXECUTION_LOCK = threading.Lock()
_OPTIONAL_INTEGRATION_EXECUTION_OWNERS: dict[
    tuple[str, str],
    _OptionalIntegrationExecutionState,
] = {}
_FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS: set[tuple[str, str]] = set()


def _new_optional_integration_thread(
    *,
    target: Callable[[], None],
    name: str,
    daemon: bool,
) -> threading.Thread:
    return threading.Thread(target=target, name=name, daemon=daemon)


def _new_optional_integration_worker_thread(
    *,
    target: Callable[[], None],
    name: str,
) -> threading.Thread:
    return threading.Thread(target=target, name=name, daemon=True)


def _optional_integration_snapshot(
    *,
    status: OptionalIntegrationStatus,
    reason_code: str,
) -> OptionalIntegrationSnapshot:
    if status not in _VALID_STATUSES:
        raise ValueError("invalid optional integration status")
    if not reason_code:
        raise ValueError("optional integration state requires a stable reason")
    return {"status": status, "reason_code": reason_code}


@dataclass(frozen=True, slots=True)
class OptionalIntegrationTaskSettlement:
    """Bounded outcome for one optional-integration task."""

    completed: bool
    cancelled: bool = False
    failure: BaseException | None = None


def _settlement_has_unretired_resources(settlement: OptionalIntegrationTaskSettlement) -> bool:
    failure = settlement.failure
    return failure is not None and bool(getattr(failure, "optional_integration_resources_unretired", False))


class OptionalIntegrationExecutionUnavailable(RuntimeError):
    """Raised when an optional integration has no safe execution owner."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


@dataclass(slots=True)
class _OptionalIntegrationExecutionState:
    key: tuple[str, str]
    lifecycle: _SharedOptionalIntegrationLifecycle
    completion: ThreadFuture[OptionalIntegrationTaskSettlement]
    stop_requested: threading.Event
    startup_resolved: threading.Event
    finished: threading.Event
    state_lock: threading.Lock
    active_subscriptions: set[int]
    completion_callbacks: dict[int, Callable[[OptionalIntegrationTaskSettlement], None]]
    blocking_lifecycle: PipelineAdmissionController | None = None
    accepting_subscriptions: bool = True
    loop: asyncio.AbstractEventLoop | None = None
    task: asyncio.Task[object] | None = None
    thread: threading.Thread | None = None
    started: bool = False
    terminal_committed: bool = False


_ExecutorWorkItem = tuple[
    ThreadFuture[Any],
    Callable[..., Any],
    tuple[Any, ...],
    dict[str, Any],
    bool,
    PipelineBlockingPermit | None,
]


class _OptionalIntegrationWorkerStartup:
    """Atomically transfer one reserved worker slot to its native worker."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: Literal["pending", "committed", "aborted"] = "pending"

    def commit_worker_ownership(self) -> bool:
        with self._lock:
            if self._state == "aborted":
                return False
            if self._state != "pending":
                raise RuntimeError("optional integration worker startup already committed")
            self._state = "committed"
            return True

    def abort_uncommitted_startup(self) -> bool:
        with self._lock:
            if self._state == "committed":
                return False
            if self._state != "pending":
                raise RuntimeError("optional integration worker startup already aborted")
            self._state = "aborted"
            return True


class _BoundedOptionalIntegrationExecutor(Executor):
    """Bounded daemon work owned by one optional-integration generation."""

    def __init__(
        self,
        *,
        name: str,
        blocking_lifecycle: PipelineAdmissionController | None,
    ) -> None:
        self._name = name
        self._blocking_lifecycle = blocking_lifecycle
        self._queue: queue.Queue[_ExecutorWorkItem] = queue.Queue(
            maxsize=_MAX_OPTIONAL_INTEGRATION_EXECUTOR_WORK_ITEMS,
        )
        self._lock = threading.Lock()
        self._accepting = True
        self._accepting_user_work = True
        self._outstanding = 0
        self._user_outstanding = 0
        self._worker_count = 0
        self._workers_retired = threading.Event()

    @property
    def outstanding_count(self) -> int:
        return self._outstanding

    @property
    def workers_retired(self) -> bool:
        return self._workers_retired.is_set()

    @property
    def user_outstanding_count(self) -> int:
        return self._user_outstanding

    def submit(  # type: ignore[override]
        self,
        fn: Callable[..., Any],
        /,
        *args: Any,
        authority: bool = False,
        **kwargs: Any,
    ) -> ThreadFuture[Any]:
        with self._lock:
            if not self._accepting:
                raise OptionalIntegrationExecutionUnavailable("slack_blocking_work_ingress_sealed")
            if not authority and not self._accepting_user_work:
                raise OptionalIntegrationExecutionUnavailable("slack_blocking_work_ingress_sealed")
            limit = (
                _MAX_OPTIONAL_INTEGRATION_EXECUTOR_WORK_ITEMS
                if authority
                else _MAX_OPTIONAL_INTEGRATION_USER_WORK_ITEMS
            )
            if self._outstanding >= limit:
                raise RuntimeError("optional integration executor capacity exhausted")
            permit: PipelineBlockingPermit | None = None
            if not authority:
                if self._blocking_lifecycle is None:
                    raise OptionalIntegrationExecutionUnavailable("slack_blocking_work_owner_unavailable")
                permit = self._blocking_lifecycle.try_acquire_blocking_permit()
                if permit is None:
                    raise OptionalIntegrationExecutionUnavailable("slack_blocking_work_capacity_exhausted")
            start_worker = self._worker_count < min(
                _MAX_OPTIONAL_INTEGRATION_EXECUTOR_WORKERS,
                self._outstanding + 1,
            )
            if start_worker:
                startup = _OptionalIntegrationWorkerStartup()
                try:
                    worker = _new_optional_integration_worker_thread(
                        target=lambda: self._run_worker_after_startup(startup),
                        name=f"{self._name}-worker",
                    )
                except BaseException:
                    self._release_blocking_permit(permit)
                    raise
                self._worker_count += 1
                try:
                    worker.start()
                except BaseException:
                    if startup.abort_uncommitted_startup():
                        self._worker_count -= 1
                        self._release_blocking_permit(permit)
                        raise
            future: ThreadFuture[Any] = ThreadFuture()
            self._outstanding += 1
            if not authority:
                self._user_outstanding += 1
            self._workers_retired.clear()
            try:
                self._queue.put_nowait((future, fn, args, kwargs, authority, permit))
            except BaseException:
                self._outstanding -= 1
                if not authority:
                    self._user_outstanding -= 1
                self._release_blocking_permit(permit)
                raise
            return future

    def _run_worker_after_startup(self, startup: _OptionalIntegrationWorkerStartup) -> None:
        if not startup.commit_worker_ownership():
            return
        self._run_worker()

    def _release_blocking_permit(self, permit: PipelineBlockingPermit | None) -> None:
        if permit is None or self._blocking_lifecycle is None:
            return
        with self._blocking_lifecycle.blocking_permit_release_transition():
            self._blocking_lifecycle.release_blocking_permit(permit)

    @staticmethod
    def _execute_work_item(
        future: ThreadFuture[Any],
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> None:
        if not future.set_running_or_notify_cancel():
            return
        try:
            result = fn(*args, **kwargs)
        except BaseException as exc:
            future.set_exception(exc)
        else:
            future.set_result(result)

    def _run_worker(self) -> None:
        try:
            while True:
                try:
                    future, fn, args, kwargs, authority, permit = self._queue.get(timeout=0.05)
                except queue.Empty:
                    if not self._accepting and self._outstanding == 0:
                        return
                    continue
                try:
                    if permit is None or self._blocking_lifecycle is None:
                        self._execute_work_item(future, fn, args, kwargs)
                    else:
                        with self._blocking_lifecycle.blocking_worker(permit):
                            self._execute_work_item(future, fn, args, kwargs)
                finally:
                    try:
                        self._release_blocking_permit(permit)
                    finally:
                        with self._lock:
                            self._outstanding -= 1
                            if not authority:
                                self._user_outstanding -= 1
                        self._queue.task_done()
        finally:
            with self._lock:
                self._worker_count -= 1
                if not self._accepting and self._worker_count == 0:
                    self._workers_retired.set()

    def seal(self) -> None:
        with self._lock:
            self._accepting = False
            self._accepting_user_work = False
            if self._worker_count == 0:
                self._workers_retired.set()

    def seal_user_work(self) -> None:
        with self._lock:
            self._accepting_user_work = False

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        del cancel_futures
        self.seal()
        if wait:
            self._workers_retired.wait()


class _OptionalIntegrationExecutableWork:
    """Route and account for all executor work created by the owner loop."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        *,
        blocking_lifecycle: PipelineAdmissionController | None,
    ) -> None:
        self._loop = loop
        self._executor = _BoundedOptionalIntegrationExecutor(
            name="tacit-slack-optional-integration",
            blocking_lifecycle=blocking_lifecycle,
        )
        self._original_run_in_executor = loop.run_in_executor
        setattr(loop, "run_in_executor", self.run_in_executor)

    @property
    def outstanding_count(self) -> int:
        return self._executor.outstanding_count

    @property
    def workers_retired(self) -> bool:
        return self._executor.workers_retired

    @property
    def user_outstanding_count(self) -> int:
        return self._executor.user_outstanding_count

    def run_in_executor(
        self,
        executor: Executor | None,
        func: Callable[..., Any],
        *args: Any,
    ) -> asyncio.Future[Any]:
        if executor is not None:
            raise RuntimeError("optional integration external executors are not allowed")
        future = self._executor.submit(func, *args)
        return asyncio.wrap_future(future, loop=self._loop)

    def submit_authority(self, callback: Callable[[], Any]) -> ThreadFuture[Any]:
        return self._executor.submit(callback, authority=True)

    def seal(self) -> None:
        self._executor.seal()

    def seal_user_work(self) -> None:
        self._executor.seal_user_work()

    def restore_loop(self) -> None:
        setattr(self._loop, "run_in_executor", self._original_run_in_executor)


@dataclass(frozen=True, slots=True)
class _OptionalIntegrationTerminalTransition:
    settlement: OptionalIntegrationTaskSettlement
    callbacks: tuple[Callable[[OptionalIntegrationTaskSettlement], None], ...]
    lifecycle_status: OptionalIntegrationStatus | None = None
    lifecycle_reason_code: str = ""


class _SharedOptionalIntegrationLifecycle:
    """Broadcast one transport generation to every overlapping app root."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subscribers: dict[int, OptionalIntegrationLifecycle] = {}
        self._next_subscription = 0
        self._snapshot: OptionalIntegrationSnapshot | None = None
        self._callback_authority_active = True
        self._detached_tasks: set[asyncio.Task[object]] = set()
        self._owner_thread_id: int | None = None
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._owner_work: _OptionalIntegrationExecutableWork | None = None
        self._owner_pending_publication: tuple[OptionalIntegrationSnapshot, bool] | None = None
        self._owner_publication_in_flight = False
        self._owner_flush_scheduled = False
        self._owner_retry_attempt = 0

    def bind_owner(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        work: _OptionalIntegrationExecutableWork,
    ) -> None:
        self._owner_thread_id = threading.get_ident()
        self._owner_loop = loop
        self._owner_work = work

    @property
    def owner_relay_idle(self) -> bool:
        return self._owner_pending_publication is None and not self._owner_publication_in_flight

    def detached_task_count_on_owner(self) -> int:
        return len(self._detached_tasks)

    def _deliver_publication(
        self,
        snapshot: OptionalIntegrationSnapshot,
        *,
        revoke: bool,
    ) -> OptionalIntegrationSnapshot | None:
        with self._lock:
            if not self._callback_authority_active:
                return None
            if revoke:
                self._callback_authority_active = False
            self._snapshot = snapshot
            subscribers = tuple(self._subscribers.values())
            if revoke:
                self._subscribers.clear()
        for lifecycle in subscribers:
            if revoke:
                lifecycle.revoke(status=snapshot["status"], reason_code=snapshot["reason_code"])
            else:
                lifecycle.publish(status=snapshot["status"], reason_code=snapshot["reason_code"])
        return snapshot

    def _queue_owner_publication(
        self,
        snapshot: OptionalIntegrationSnapshot,
        *,
        revoke: bool,
    ) -> OptionalIntegrationSnapshot:
        pending = self._owner_pending_publication
        if pending is None or revoke or not pending[1]:
            self._owner_pending_publication = (snapshot, revoke)
        if not self._owner_flush_scheduled and not self._owner_publication_in_flight:
            self._owner_flush_scheduled = True
            loop = self._owner_loop
            if loop is None:
                raise RuntimeError("optional integration lifecycle owner is unavailable")
            loop.call_soon(self._flush_owner_publication)
        return snapshot

    def _flush_owner_publication(self) -> None:
        self._owner_flush_scheduled = False
        if self._owner_publication_in_flight:
            return
        pending = self._owner_pending_publication
        if pending is None:
            return
        self._owner_pending_publication = None
        self._owner_publication_in_flight = True
        snapshot, revoke = pending
        loop = self._owner_loop
        work = self._owner_work
        if loop is None or work is None:
            raise RuntimeError("optional integration lifecycle relay is unavailable")

        def deliver() -> None:
            try:
                self._deliver_publication(snapshot, revoke=revoke)
            finally:
                try:
                    loop.call_soon_threadsafe(self._owner_publication_completed)
                except RuntimeError:
                    pass

        try:
            work.submit_authority(deliver)
        except RuntimeError:
            self._owner_publication_in_flight = False
            current = self._owner_pending_publication
            if current is None or revoke or not current[1]:
                self._owner_pending_publication = pending
            self._owner_retry_attempt += 1
            delay = min(
                _MAX_OPTIONAL_INTEGRATION_OWNER_RETRY_SECONDS,
                _MIN_OPTIONAL_INTEGRATION_OWNER_RETRY_SECONDS * (2 ** min(self._owner_retry_attempt, 8)),
            )
            if not self._owner_flush_scheduled:
                self._owner_flush_scheduled = True
                loop.call_later(delay, self._flush_owner_publication)

    def _owner_publication_completed(self) -> None:
        self._owner_publication_in_flight = False
        self._owner_retry_attempt = 0
        if self._owner_pending_publication is not None and not self._owner_flush_scheduled:
            self._owner_flush_scheduled = True
            loop = self._owner_loop
            if loop is not None:
                loop.call_soon(self._flush_owner_publication)

    def _transport_owner_publication(
        self,
        snapshot: OptionalIntegrationSnapshot,
        revoke: bool,
    ) -> None:
        self._queue_owner_publication(snapshot, revoke=revoke)

    @property
    def callback_authority_active(self) -> bool:
        with self._lock:
            return self._callback_authority_active

    @property
    def detached_task_count(self) -> int:
        with self._lock:
            return len(self._detached_tasks)

    def reserve_subscription(
        self,
        lifecycle: OptionalIntegrationLifecycle,
    ) -> tuple[int, OptionalIntegrationSnapshot | None]:
        """Reserve membership without invoking subscriber-owned callbacks."""
        with self._lock:
            if not self._callback_authority_active:
                raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_stopped")
            self._next_subscription += 1
            subscription = self._next_subscription
            self._subscribers[subscription] = lifecycle
            snapshot = self._snapshot
        return subscription, snapshot

    @staticmethod
    def replay_subscription(
        lifecycle: OptionalIntegrationLifecycle,
        snapshot: OptionalIntegrationSnapshot | None,
    ) -> None:
        """Replay confirmed state after execution-state locks are released."""
        if snapshot is not None:
            lifecycle.publish(status=snapshot["status"], reason_code=snapshot["reason_code"])

    def detach(
        self,
        subscription: int,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> None:
        with self._lock:
            lifecycle = self._subscribers.pop(subscription, None)
        if lifecycle is not None:
            lifecycle.revoke(status=status, reason_code=reason_code)

    def publish(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None:
        snapshot = _optional_integration_snapshot(status=status, reason_code=reason_code)
        if threading.get_ident() == self._owner_thread_id:
            return self._queue_owner_publication(snapshot, revoke=False)
        if self._owner_loop is not None:
            self._owner_loop.call_soon_threadsafe(self._transport_owner_publication, snapshot, False)
            return snapshot
        return self._deliver_publication(snapshot, revoke=False)

    def revoke(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None:
        snapshot = _optional_integration_snapshot(status=status, reason_code=reason_code)
        if threading.get_ident() == self._owner_thread_id:
            return self._queue_owner_publication(snapshot, revoke=True)
        if self._owner_loop is not None:
            self._owner_loop.call_soon_threadsafe(self._transport_owner_publication, snapshot, True)
            return snapshot
        return self._deliver_publication(snapshot, revoke=True)

    def retain_detached_task(self, task: asyncio.Task[object]) -> None:
        with self._lock:
            self._detached_tasks.add(task)

        def consume(completed: asyncio.Task[object]) -> None:
            with self._lock:
                self._detached_tasks.discard(completed)
            try:
                completed.result()
            except BaseException:
                pass

        task.add_done_callback(consume)


def _release_optional_integration_execution_state(
    state: _OptionalIntegrationExecutionState,
) -> None:
    with _OPTIONAL_INTEGRATION_EXECUTION_LOCK:
        if _OPTIONAL_INTEGRATION_EXECUTION_OWNERS.get(state.key) is state:
            _OPTIONAL_INTEGRATION_EXECUTION_OWNERS.pop(state.key, None)


def _retire_detached_optional_integration_execution_state(
    state: _OptionalIntegrationExecutionState,
) -> None:
    """Atomically release a fence created only for now-retired detached work."""
    with _OPTIONAL_INTEGRATION_EXECUTION_LOCK:
        if _OPTIONAL_INTEGRATION_EXECUTION_OWNERS.get(state.key) is state:
            _OPTIONAL_INTEGRATION_EXECUTION_OWNERS.pop(state.key, None)
        _FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS.discard(state.key)


def _fence_optional_integration_execution_state(
    state: _OptionalIntegrationExecutionState,
) -> None:
    with _OPTIONAL_INTEGRATION_EXECUTION_LOCK:
        _FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS.add(state.key)


def _optional_integration_execution_counts() -> tuple[int, int]:
    with _OPTIONAL_INTEGRATION_EXECUTION_LOCK:
        return (
            len(_OPTIONAL_INTEGRATION_EXECUTION_OWNERS),
            len(_FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS),
        )


class _OptionalIntegrationCallbackDrain:
    """Drain accepted ready callbacks and close their scheduling boundary."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._owner_thread_id = threading.get_ident()
        self._transport_lock = threading.Lock()
        # Retain the private alias for lifecycle probes. Owner-loop code never
        # acquires this foreign-transport mutex.
        self._lock = self._transport_lock
        self._scheduled_count = 0
        self._transport_sequence = 0
        self._pending_timer_handles: set[asyncio.TimerHandle] = set()
        self._accepting = True
        self._transport_accepting = True
        self._call_soon = loop.call_soon
        self._call_soon_threadsafe = loop.call_soon_threadsafe
        self._call_at = loop.call_at
        setattr(loop, "call_soon", self.call_soon)
        setattr(loop, "call_soon_threadsafe", self.call_soon_threadsafe)
        setattr(loop, "call_at", self.call_at)

    def _schedule(
        self,
        scheduler: Callable[..., asyncio.Handle],
        callback: Callable[..., object],
        args: tuple[object, ...],
        context: Context | None,
    ) -> asyncio.Handle:
        if threading.get_ident() == self._owner_thread_id:
            if not self._accepting:
                raise RuntimeError("optional integration callback owner is stopping")
            if context is None:
                handle = scheduler(callback, *args)
            else:
                handle = scheduler(callback, *args, context=context)
            self._scheduled_count += 1
            return handle
        with self._transport_lock:
            if not self._transport_accepting:
                raise RuntimeError("optional integration callback owner is stopping")
            if context is None:
                handle = scheduler(callback, *args)
            else:
                handle = scheduler(callback, *args, context=context)
            self._transport_sequence += 1
            return handle

    def call_soon(
        self,
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.Handle:
        return self._schedule(self._call_soon, callback, args, context)

    def call_soon_threadsafe(
        self,
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.Handle:
        return self._schedule(self._call_soon_threadsafe, callback, args, context)

    def call_at(
        self,
        when: float,
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.TimerHandle:
        if threading.get_ident() != self._owner_thread_id:
            raise RuntimeError("optional integration delayed callbacks require the owner loop")
        if not self._accepting:
            raise RuntimeError("optional integration callback owner is stopping")
        if len(self._pending_timer_handles) >= _MAX_OPTIONAL_INTEGRATION_PENDING_TIMER_HANDLES:
            self._pending_timer_handles = {handle for handle in self._pending_timer_handles if not handle.cancelled()}
        if len(self._pending_timer_handles) >= _MAX_OPTIONAL_INTEGRATION_PENDING_TIMER_HANDLES:
            raise RuntimeError("optional integration delayed callback capacity exhausted")

        handle: asyncio.TimerHandle | None = None

        def owned_callback() -> object:
            if handle is not None:
                self._pending_timer_handles.discard(handle)
            return callback(*args)

        if context is None:
            handle = self._call_at(when, owned_callback)
        else:
            handle = self._call_at(when, owned_callback, context=context)
        self._pending_timer_handles.add(handle)
        self._scheduled_count += 1
        return handle

    def _transport_snapshot(self) -> int | None:
        if not self._transport_lock.acquire(blocking=False):
            return None
        try:
            return self._transport_sequence
        finally:
            self._transport_lock.release()

    def run_owner_retry(self, attempt: int) -> None:
        """Keep the owner loop responsive while one coalesced retry is pending."""
        delay = min(
            _MAX_OPTIONAL_INTEGRATION_OWNER_RETRY_SECONDS,
            _MIN_OPTIONAL_INTEGRATION_OWNER_RETRY_SECONDS * (2 ** min(attempt, 8)),
        )
        retry = self._call_at(self._loop.time() + delay, self._loop.stop)
        try:
            self._loop.run_forever()
        finally:
            retry.cancel()

    def drain_ready_callbacks(self) -> tuple[bool, int, int, int]:
        """Run transitive ready work until one complete turn schedules nothing."""
        turn = 0
        contention_attempt = 0
        while turn < _MAX_OPTIONAL_INTEGRATION_CALLBACK_DRAIN_TURNS:
            transport_before = self._transport_snapshot()
            if transport_before is None:
                self.run_owner_retry(contention_attempt)
                contention_attempt += 1
                continue
            before = self._scheduled_count
            self._call_soon(self._loop.stop)
            self._loop.run_forever()
            transport_after = self._transport_snapshot()
            after = self._scheduled_count
            if transport_after is None:
                self.run_owner_retry(contention_attempt)
                contention_attempt += 1
                continue
            contention_attempt = 0
            turn += 1
            if transport_after is not None and after == before and transport_after == transport_before:
                return True, turn, after, transport_after
        transport_after = self._transport_snapshot()
        return (
            False,
            _MAX_OPTIONAL_INTEGRATION_CALLBACK_DRAIN_TURNS,
            self._scheduled_count,
            -1 if transport_after is None else transport_after,
        )

    def seal_if_unchanged(
        self,
        expected_scheduled_count: int,
        expected_transport_sequence: int,
    ) -> bool:
        """Seal scheduling and cancel bounded delayed work after a stable turn."""
        if self._scheduled_count != expected_scheduled_count:
            return False
        if not self._transport_lock.acquire(blocking=False):
            return False
        try:
            if self._transport_sequence != expected_transport_sequence:
                return False
            if not self._transport_accepting:
                return False
            self._transport_accepting = False
        finally:
            self._transport_lock.release()
        if self._scheduled_count != expected_scheduled_count:
            return False
        self._accepting = False
        pending_timer_handles = tuple(self._pending_timer_handles)
        self._pending_timer_handles.clear()
        for handle in pending_timer_handles:
            handle.cancel()
        return True


def _park_non_quiescent_optional_integration_owner() -> None:
    """Retain one fenced daemon owner without consuming CPU or dropping work."""
    threading.Event().wait()


def _commit_optional_integration_terminal_transition_locked(
    state: _OptionalIntegrationExecutionState,
    settlement: OptionalIntegrationTaskSettlement,
    *,
    lifecycle_status: OptionalIntegrationStatus | None = None,
    lifecycle_reason_code: str = "",
) -> _OptionalIntegrationTerminalTransition | None:
    """Close admission while the caller holds the execution-state lock."""
    if state.terminal_committed:
        return None
    state.terminal_committed = True
    state.accepting_subscriptions = False
    state.active_subscriptions.clear()
    callbacks = tuple(state.completion_callbacks.values())
    state.completion_callbacks.clear()
    return _OptionalIntegrationTerminalTransition(
        settlement=settlement,
        callbacks=callbacks,
        lifecycle_status=lifecycle_status,
        lifecycle_reason_code=lifecycle_reason_code,
    )


def _commit_optional_integration_terminal_transition(
    state: _OptionalIntegrationExecutionState,
    settlement: OptionalIntegrationTaskSettlement,
    *,
    lifecycle_status: OptionalIntegrationStatus | None = None,
    lifecycle_reason_code: str = "",
) -> _OptionalIntegrationTerminalTransition | None:
    """Close admission and collect terminal work under one state transition."""
    with state.state_lock:
        return _commit_optional_integration_terminal_transition_locked(
            state,
            settlement,
            lifecycle_status=lifecycle_status,
            lifecycle_reason_code=lifecycle_reason_code,
        )


def _commit_optional_integration_terminal_transition_on_owner(
    state: _OptionalIntegrationExecutionState,
    settlement: OptionalIntegrationTaskSettlement,
    callback_drain: _OptionalIntegrationCallbackDrain,
    *,
    lifecycle_status: OptionalIntegrationStatus | None = None,
    lifecycle_reason_code: str = "",
) -> _OptionalIntegrationTerminalTransition | None:
    """Settle terminal state without blocking the durable owner on a foreign lock."""
    attempt = 0
    while not state.state_lock.acquire(blocking=False):
        callback_drain.run_owner_retry(attempt)
        attempt += 1
    try:
        return _commit_optional_integration_terminal_transition_locked(
            state,
            settlement,
            lifecycle_status=lifecycle_status,
            lifecycle_reason_code=lifecycle_reason_code,
        )
    finally:
        state.state_lock.release()


def _publish_optional_integration_terminal_transition(
    state: _OptionalIntegrationExecutionState,
    transition: _OptionalIntegrationTerminalTransition,
    *,
    owner_finished: bool,
    release_registry: bool,
    owner_work: _OptionalIntegrationExecutableWork | None = None,
) -> None:
    """Publish terminal work after releasing execution-state locks."""
    if not state.completion.done():
        state.completion.set_result(transition.settlement)
    if transition.lifecycle_status is not None:
        state.lifecycle.revoke(
            status=transition.lifecycle_status,
            reason_code=transition.lifecycle_reason_code,
        )

    def deliver_completion_callbacks() -> None:
        for callback in transition.callbacks:
            try:
                callback(transition.settlement)
            except BaseException:
                pass

    if owner_work is not None and transition.callbacks:
        owner_work.submit_authority(deliver_completion_callbacks)
    else:
        deliver_completion_callbacks()
    if owner_finished:
        state.finished.set()
    if release_registry:
        _release_optional_integration_execution_state(state)


class OptionalIntegrationExecutionOwner:
    """Own one optional integration on a process-bounded daemon event loop.

    Python cannot terminate a cancellation-resistant coroutine. A daemon owner
    keeps that work off the application loop so process exit remains bounded.
    If shutdown exceeds its deadline, the runtime/integration identity is
    process-fenced and cannot accumulate replacement owners.
    """

    def __init__(
        self,
        state: _OptionalIntegrationExecutionState,
        *,
        subscription: int,
        primary: bool,
        subscriber_lifecycle: OptionalIntegrationLifecycle,
    ) -> None:
        self._state = state
        self._subscription = subscription
        self._primary = primary
        self._subscriber_lifecycle = subscriber_lifecycle
        self._released = False

    @classmethod
    def acquire(
        cls,
        *,
        name: str,
        runtime_identity: str,
        lifecycle: OptionalIntegrationLifecycle,
        blocking_lifecycle: PipelineAdmissionController | None = None,
    ) -> OptionalIntegrationExecutionOwner:
        normalized_name = str(name).strip().casefold()
        normalized_identity = str(runtime_identity).strip()
        if not normalized_name or not normalized_identity:
            raise ValueError("optional integration execution identity is required")
        key = (normalized_name, normalized_identity)
        replay_snapshot: OptionalIntegrationSnapshot | None
        with _OPTIONAL_INTEGRATION_EXECUTION_LOCK:
            if key in _FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS:
                raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_fenced")
            current = _OPTIONAL_INTEGRATION_EXECUTION_OWNERS.get(key)
            if current is not None and current.finished.is_set():
                _OPTIONAL_INTEGRATION_EXECUTION_OWNERS.pop(key, None)
                current = None
            if current is None:
                owner_count = len(
                    set(_OPTIONAL_INTEGRATION_EXECUTION_OWNERS) | _FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS
                )
                if owner_count >= _MAX_OPTIONAL_INTEGRATION_EXECUTION_OWNERS:
                    raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_capacity_exhausted")
                shared_lifecycle = _SharedOptionalIntegrationLifecycle()
                subscription, replay_snapshot = shared_lifecycle.reserve_subscription(lifecycle)
                state = _OptionalIntegrationExecutionState(
                    key=key,
                    lifecycle=shared_lifecycle,
                    completion=ThreadFuture(),
                    stop_requested=threading.Event(),
                    startup_resolved=threading.Event(),
                    finished=threading.Event(),
                    state_lock=threading.Lock(),
                    active_subscriptions={subscription},
                    completion_callbacks={},
                    blocking_lifecycle=blocking_lifecycle,
                )
                _OPTIONAL_INTEGRATION_EXECUTION_OWNERS[key] = state
                owner = cls(
                    state,
                    subscription=subscription,
                    primary=True,
                    subscriber_lifecycle=lifecycle,
                )
            else:
                if current.blocking_lifecycle is not blocking_lifecycle:
                    raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_admission_mismatch")
                with current.state_lock:
                    if not current.accepting_subscriptions:
                        raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_stopping")
                    if len(current.active_subscriptions) >= _MAX_OPTIONAL_INTEGRATION_SUBSCRIPTIONS:
                        raise OptionalIntegrationExecutionUnavailable("slack_execution_subscriber_capacity_exhausted")
                    subscription, replay_snapshot = current.lifecycle.reserve_subscription(lifecycle)
                    current.active_subscriptions.add(subscription)
                owner = cls(
                    current,
                    subscription=subscription,
                    primary=False,
                    subscriber_lifecycle=lifecycle,
                )
                shared_lifecycle = current.lifecycle
                # Membership is now ordered before any terminal transition.
                # Subscriber-owned callbacks remain outside both locks.
        shared_lifecycle.replay_subscription(lifecycle, replay_snapshot)
        return owner

    @property
    def lifecycle(self) -> _SharedOptionalIntegrationLifecycle:
        return self._state.lifecycle

    @property
    def thread_alive(self) -> bool:
        thread = self._state.thread
        return thread is not None and thread.is_alive()

    def start(
        self,
        operation_factory: Callable[[], Coroutine[Any, Any, object]],
        *,
        on_completion: Callable[[OptionalIntegrationTaskSettlement], None],
    ) -> None:
        if not callable(operation_factory) or not callable(on_completion):
            raise TypeError("optional integration execution callbacks must be callable")
        state = self._state
        detach_stopped_subscription = False
        with state.state_lock:
            if not state.accepting_subscriptions:
                self._released = True
                state.active_subscriptions.discard(self._subscription)
                detach_stopped_subscription = True
            else:
                state.completion_callbacks[self._subscription] = (
                    lambda settlement: self._subscriber_lifecycle.transport_completion_callback(
                        on_completion,
                        settlement,
                    )
                )
            if not detach_stopped_subscription and not self._primary:
                return
            if not detach_stopped_subscription and state.started:
                raise RuntimeError("optional integration execution owner already started")
            if not detach_stopped_subscription:
                state.started = True
        if detach_stopped_subscription:
            state.lifecycle.detach(
                self._subscription,
                status="stopped",
                reason_code="slack_execution_owner_stopping",
            )
            raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_stopping")

        def run() -> None:
            loop: asyncio.AbstractEventLoop | None = None
            callback_drain: _OptionalIntegrationCallbackDrain | None = None
            owner_work: _OptionalIntegrationExecutableWork | None = None
            settlement = OptionalIntegrationTaskSettlement(completed=True)
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                callback_drain = _OptionalIntegrationCallbackDrain(loop)
                owner_work = _OptionalIntegrationExecutableWork(
                    loop,
                    blocking_lifecycle=state.blocking_lifecycle,
                )
                state.lifecycle.bind_owner(loop=loop, work=owner_work)
                operation = operation_factory()
                task: asyncio.Task[object] = loop.create_task(operation)
                task.add_done_callback(lambda _task: owner_work.seal_user_work())
                state.loop = loop
                state.task = task
                if state.stop_requested.is_set():
                    task.cancel()
                try:
                    loop.run_until_complete(task)
                except asyncio.CancelledError:
                    settlement = OptionalIntegrationTaskSettlement(completed=True, cancelled=True)
                except BaseException as exc:
                    settlement = OptionalIntegrationTaskSettlement(completed=True, failure=exc)
            except BaseException as exc:
                settlement = OptionalIntegrationTaskSettlement(completed=True, failure=exc)
            finally:
                retained_work_announced = False
                if owner_work is not None:
                    owner_work.seal_user_work()

                def wait_for_startup_resolution() -> None:
                    attempt = 0
                    while not state.startup_resolved.is_set():
                        if callback_drain is None:
                            state.startup_resolved.wait(timeout=0.001)
                        else:
                            callback_drain.run_owner_retry(attempt)
                            attempt += 1

                def announce_retained_work(
                    *,
                    detached_task_count: int,
                    pending_task_count: int,
                    callback_drain_turns: int,
                    scheduled_callback_count: int,
                    callback_drain_exhausted: bool,
                    executable_work_count: int,
                ) -> None:
                    nonlocal retained_work_announced
                    if retained_work_announced:
                        return
                    retained_work_announced = True
                    _fence_optional_integration_execution_state(state)
                    active_owner_count, fenced_identity_count = _optional_integration_execution_counts()
                    logger.warning(
                        "optional_integration_retained_work",
                        reason_code="optional_integration_retained_work",
                        detached_task_count=detached_task_count,
                        pending_task_count=pending_task_count,
                        callback_drain_turns=callback_drain_turns,
                        scheduled_callback_count=scheduled_callback_count,
                        callback_drain_exhausted=callback_drain_exhausted,
                        executable_work_count=executable_work_count,
                        active_owner_count=active_owner_count,
                        fenced_identity_count=fenced_identity_count,
                    )
                    wait_for_startup_resolution()
                    if callback_drain is None:
                        transition = _commit_optional_integration_terminal_transition(state, settlement)
                    else:
                        transition = _commit_optional_integration_terminal_transition_on_owner(
                            state,
                            settlement,
                            callback_drain,
                        )
                    if transition is not None:
                        _publish_optional_integration_terminal_transition(
                            state,
                            transition,
                            owner_finished=False,
                            release_registry=False,
                            owner_work=owner_work,
                        )

                if loop is not None and callback_drain is not None:
                    while True:
                        (
                            quiescent,
                            callback_drain_turns,
                            scheduled_callback_count,
                            transport_sequence,
                        ) = callback_drain.drain_ready_callbacks()
                        pending_tasks = tuple(pending for pending in asyncio.all_tasks(loop) if not pending.done())
                        detached_task_count = state.lifecycle.detached_task_count_on_owner()
                        retains_resources = _settlement_has_unretired_resources(settlement)
                        executable_work_count = 0 if owner_work is None else owner_work.user_outstanding_count
                        if (
                            retains_resources
                            or detached_task_count
                            or pending_tasks
                            or executable_work_count
                            or not quiescent
                        ):
                            announce_retained_work(
                                detached_task_count=detached_task_count,
                                pending_task_count=len(pending_tasks),
                                callback_drain_turns=callback_drain_turns,
                                scheduled_callback_count=scheduled_callback_count,
                                callback_drain_exhausted=not quiescent,
                                executable_work_count=executable_work_count,
                            )
                        if not quiescent:
                            _park_non_quiescent_optional_integration_owner()
                        if pending_tasks:
                            for pending in pending_tasks:
                                pending.cancel()
                            loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                            continue
                        if detached_task_count:
                            _park_non_quiescent_optional_integration_owner()
                        break
                wait_for_startup_resolution()
                if callback_drain is None:
                    transition = _commit_optional_integration_terminal_transition(state, settlement)
                else:
                    transition = _commit_optional_integration_terminal_transition_on_owner(
                        state,
                        settlement,
                        callback_drain,
                    )
                if transition is not None:
                    _publish_optional_integration_terminal_transition(
                        state,
                        transition,
                        owner_finished=False,
                        release_registry=False,
                        owner_work=owner_work,
                    )
                if loop is not None and callback_drain is not None and owner_work is not None:
                    attempt = 0
                    while not state.lifecycle.owner_relay_idle or owner_work.outstanding_count:
                        if owner_work.user_outstanding_count:
                            announce_retained_work(
                                detached_task_count=state.lifecycle.detached_task_count_on_owner(),
                                pending_task_count=len(
                                    tuple(pending for pending in asyncio.all_tasks(loop) if not pending.done())
                                ),
                                callback_drain_turns=0,
                                scheduled_callback_count=callback_drain._scheduled_count,
                                callback_drain_exhausted=False,
                                executable_work_count=owner_work.user_outstanding_count,
                            )
                        callback_drain.run_owner_retry(attempt)
                        attempt += 1
                    owner_work.seal()
                    attempt = 0
                    while not owner_work.workers_retired:
                        callback_drain.run_owner_retry(attempt)
                        attempt += 1
                    owner_work.restore_loop()
                    while True:
                        (
                            quiescent,
                            callback_drain_turns,
                            scheduled_callback_count,
                            transport_sequence,
                        ) = callback_drain.drain_ready_callbacks()
                        pending_tasks = tuple(pending for pending in asyncio.all_tasks(loop) if not pending.done())
                        detached_task_count = state.lifecycle.detached_task_count_on_owner()
                        if not quiescent:
                            announce_retained_work(
                                detached_task_count=detached_task_count,
                                pending_task_count=len(pending_tasks),
                                callback_drain_turns=callback_drain_turns,
                                scheduled_callback_count=scheduled_callback_count,
                                callback_drain_exhausted=True,
                                executable_work_count=0,
                            )
                            _park_non_quiescent_optional_integration_owner()
                        if pending_tasks:
                            for pending in pending_tasks:
                                pending.cancel()
                            loop.run_until_complete(asyncio.gather(*pending_tasks, return_exceptions=True))
                            continue
                        if detached_task_count:
                            _park_non_quiescent_optional_integration_owner()
                        if callback_drain.seal_if_unchanged(
                            scheduled_callback_count,
                            transport_sequence,
                        ):
                            break
                if loop is not None:
                    loop.close()
                if _settlement_has_unretired_resources(settlement):
                    _release_optional_integration_execution_state(state)
                elif retained_work_announced:
                    _retire_detached_optional_integration_execution_state(state)
                else:
                    _release_optional_integration_execution_state(state)
                state.finished.set()

        try:
            thread = _new_optional_integration_thread(
                target=run,
                name="tacit-slack-optional-integration",
                daemon=True,
            )
            state.thread = thread
            thread.start()
            state.startup_resolved.set()
        except BaseException as exc:
            failure = OptionalIntegrationExecutionUnavailable("slack_execution_owner_start_failed")
            failure.__cause__ = exc
            failed_thread = state.thread
            ambiguous_start = failed_thread is not None and failed_thread.ident is not None
            transition = _commit_optional_integration_terminal_transition(
                state,
                OptionalIntegrationTaskSettlement(completed=True, failure=failure),
                lifecycle_status="failed",
                lifecycle_reason_code="slack_execution_owner_start_failed",
            )
            if ambiguous_start:
                _fence_optional_integration_execution_state(state)
                self.request_stop()
            state.startup_resolved.set()
            if transition is not None:
                _publish_optional_integration_terminal_transition(
                    state,
                    transition,
                    owner_finished=not ambiguous_start,
                    release_registry=not ambiguous_start,
                )
            raise failure from exc

    def request_stop(self) -> None:
        state = self._state
        state.stop_requested.set()
        with state.state_lock:
            loop = state.loop
            task = state.task
        if loop is None or task is None or task.done():
            return
        try:
            loop.call_soon_threadsafe(task.cancel)
        except RuntimeError:
            pass

    async def shutdown(
        self,
        *,
        timeout_seconds: float,
        timeout_reason_code: str,
    ) -> OptionalIntegrationTaskSettlement:
        if timeout_seconds <= 0:
            raise ValueError("optional integration timeout must be positive")
        state = self._state
        unstarted_transition: _OptionalIntegrationTerminalTransition | None = None
        with state.state_lock:
            if self._released:
                final_subscription = not state.active_subscriptions
            else:
                self._released = True
                state.active_subscriptions.discard(self._subscription)
                final_subscription = not state.active_subscriptions
            abandoned_startup = self._primary and not state.started
            settle_unstarted_generation = not state.started and (final_subscription or abandoned_startup)
            if final_subscription or settle_unstarted_generation:
                state.accepting_subscriptions = False
            if not final_subscription and not settle_unstarted_generation:
                state.completion_callbacks.pop(self._subscription, None)
            if settle_unstarted_generation:
                unstarted_transition = _commit_optional_integration_terminal_transition_locked(
                    state,
                    OptionalIntegrationTaskSettlement(completed=True, cancelled=True),
                    lifecycle_status="stopped",
                    lifecycle_reason_code="slack_shutdown_completed",
                )

        if unstarted_transition is not None:
            _publish_optional_integration_terminal_transition(
                state,
                unstarted_transition,
                owner_finished=True,
                release_registry=True,
            )
            return unstarted_transition.settlement

        if not final_subscription:
            state.lifecycle.detach(
                self._subscription,
                status="stopped",
                reason_code="slack_shutdown_completed",
            )
            return OptionalIntegrationTaskSettlement(completed=True)

        self.request_stop()
        completion = asyncio.wrap_future(state.completion)
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        try:
            settlement = await asyncio.wait_for(
                asyncio.shield(completion),
                timeout=timeout_seconds,
            )
        except TimeoutError:
            _fence_optional_integration_execution_state(state)
            state.lifecycle.revoke(
                status="failed",
                reason_code=timeout_reason_code,
            )
            return OptionalIntegrationTaskSettlement(completed=False)
        while not state.finished.is_set():
            if asyncio.get_running_loop().time() >= deadline:
                _fence_optional_integration_execution_state(state)
                state.lifecycle.revoke(
                    status="failed",
                    reason_code=timeout_reason_code,
                )
                return OptionalIntegrationTaskSettlement(completed=False)
            await asyncio.sleep(0.001)
        thread = state.thread
        if thread is not None:
            thread.join(timeout=0)
        return settlement


class OptionalIntegrationLifecycle:
    """Own status callback authority and detached cleanup for one integration."""

    def __init__(self, app: FastAPI | None, *, name: str) -> None:
        if not name:
            raise ValueError("optional integration lifecycle requires a name")
        self._app = app
        self._name = name
        self._owner_loop: asyncio.AbstractEventLoop | None
        try:
            self._owner_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._owner_loop = None
        self._state_lock = threading.Lock()
        self._callback_authority_active = True
        self._publication_revision = 0
        self._pending_snapshot: tuple[int, OptionalIntegrationSnapshot] | None = None
        self._transport_callback_pending = False
        self._detached_tasks: set[asyncio.Task[object]] = set()

    @property
    def callback_authority_active(self) -> bool:
        with self._state_lock:
            return self._callback_authority_active

    @property
    def detached_task_count(self) -> int:
        with self._state_lock:
            return len(self._detached_tasks)

    def transport_completion_callback(
        self,
        callback: Callable[[OptionalIntegrationTaskSettlement], None],
        settlement: OptionalIntegrationTaskSettlement,
    ) -> None:
        """Return subscriber-owned completion work to its captured event loop."""
        owner_loop = self._owner_loop
        if owner_loop is None:
            callback(settlement)
            return
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if current_loop is owner_loop:
            callback(settlement)
            return
        try:
            owner_loop.call_soon_threadsafe(callback, settlement)
        except RuntimeError:
            if not owner_loop.is_closed():
                raise

    def _commit_snapshot(
        self,
        revision: int,
        snapshot: OptionalIntegrationSnapshot,
    ) -> None:
        with self._state_lock:
            if revision != self._publication_revision:
                return
        if self._app is not None:
            set_optional_integration_state(
                self._app,
                name=self._name,
                status=snapshot["status"],
                reason_code=snapshot["reason_code"],
            )

    def _drain_pending_snapshot(self) -> None:
        """Commit coalesced status without scheduling another transport callback."""
        while True:
            with self._state_lock:
                pending = self._pending_snapshot
                self._pending_snapshot = None
                if pending is None:
                    self._transport_callback_pending = False
                    return
            self._commit_snapshot(*pending)

    def _publish_snapshot(
        self,
        snapshot: OptionalIntegrationSnapshot,
        *,
        revoke: bool,
    ) -> OptionalIntegrationSnapshot | None:
        owner_loop = self._owner_loop
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        commit_directly = owner_loop is None or current_loop is owner_loop
        schedule_transport = False
        with self._state_lock:
            if not self._callback_authority_active:
                return None
            if revoke:
                self._callback_authority_active = False
            self._publication_revision += 1
            revision = self._publication_revision
            if not commit_directly:
                self._pending_snapshot = (revision, snapshot)
                if not self._transport_callback_pending:
                    self._transport_callback_pending = True
                    schedule_transport = True

        if commit_directly:
            self._commit_snapshot(revision, snapshot)
            return snapshot
        if not schedule_transport:
            return snapshot
        assert owner_loop is not None
        try:
            owner_loop.call_soon_threadsafe(self._drain_pending_snapshot)
        except RuntimeError:
            if owner_loop.is_closed():
                with self._state_lock:
                    self._pending_snapshot = None
                    self._transport_callback_pending = False
        return snapshot

    def publish(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None:
        """Publish while this lifecycle generation still owns callbacks."""
        snapshot = _optional_integration_snapshot(status=status, reason_code=reason_code)
        return self._publish_snapshot(snapshot, revoke=False)

    def revoke(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None:
        """Publish one terminal state and permanently revoke late callbacks."""
        snapshot = _optional_integration_snapshot(status=status, reason_code=reason_code)
        return self._publish_snapshot(snapshot, revoke=True)

    def retain_detached_task(self, task: asyncio.Task[object]) -> None:
        """Retain one non-cooperative cleanup until the event loop settles it."""
        with self._state_lock:
            self._detached_tasks.add(task)

        def consume(completed: asyncio.Task[object]) -> None:
            with self._state_lock:
                self._detached_tasks.discard(completed)
            try:
                completed.result()
            except BaseException:
                pass

        task.add_done_callback(consume)


async def settle_optional_integration_task(
    task: asyncio.Task[object],
    *,
    lifecycle: OptionalIntegrationLifecycleProtocol,
    timeout_seconds: float,
    timeout_reason_code: str,
    cancel_first: bool,
) -> OptionalIntegrationTaskSettlement:
    """Settle optional work within a deadline without owning core shutdown."""
    if timeout_seconds <= 0:
        raise ValueError("optional integration timeout must be positive")
    if cancel_first and not task.done():
        task.cancel()

    done, _pending = await asyncio.wait({task}, timeout=timeout_seconds)
    if not done:
        task.cancel()
        lifecycle.revoke(status="failed", reason_code=timeout_reason_code)
        lifecycle.retain_detached_task(task)
        return OptionalIntegrationTaskSettlement(completed=False)

    if task.cancelled():
        return OptionalIntegrationTaskSettlement(completed=True, cancelled=True)
    try:
        task.result()
    except BaseException as exc:
        return OptionalIntegrationTaskSettlement(completed=True, failure=exc)
    return OptionalIntegrationTaskSettlement(completed=True)


def set_optional_integration_state(
    app: FastAPI,
    *,
    name: str,
    status: OptionalIntegrationStatus,
    reason_code: str,
) -> OptionalIntegrationSnapshot:
    """Publish one copy-on-write status snapshot on the application."""
    if not name:
        raise ValueError("optional integration state requires stable identifiers")

    snapshot = _optional_integration_snapshot(status=status, reason_code=reason_code)
    current = getattr(app.state, "optional_integration_readiness", {})
    app.state.optional_integration_readiness = {
        **(current if isinstance(current, dict) else {}),
        name: snapshot,
    }
    return snapshot


def optional_integration_degradation(app: FastAPI) -> dict[str, OptionalIntegrationSnapshot]:
    """Return only sanitized optional-integration degradation details."""
    current = getattr(app.state, "optional_integration_readiness", {})
    if not isinstance(current, dict):
        return {}

    degraded: dict[str, OptionalIntegrationSnapshot] = {}
    for name, value in current.items():
        if name != "slack" or not isinstance(value, dict):
            continue
        status = value.get("status")
        reason_code = value.get("reason_code")
        if status not in _DEGRADED_STATUSES or not isinstance(reason_code, str):
            continue
        degraded[name] = {
            "status": cast(OptionalIntegrationStatus, status),
            "reason_code": reason_code[:128],
        }
    return degraded
