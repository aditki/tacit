"""App-scoped status for optional integrations."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine
from concurrent.futures import Future as ThreadFuture
from contextvars import Context
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypedDict, cast

import structlog
from fastapi import FastAPI

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
_MAX_OPTIONAL_INTEGRATION_CALLBACK_DRAIN_TURNS = 4096
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
    accepting_subscriptions: bool = True
    loop: asyncio.AbstractEventLoop | None = None
    task: asyncio.Task[object] | None = None
    thread: threading.Thread | None = None
    started: bool = False
    terminal_committed: bool = False


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
        with self._lock:
            if not self._callback_authority_active:
                return None
            self._snapshot = snapshot
            subscribers = tuple(self._subscribers.values())
        for lifecycle in subscribers:
            lifecycle.publish(status=status, reason_code=reason_code)
        return snapshot

    def revoke(
        self,
        *,
        status: OptionalIntegrationStatus,
        reason_code: str,
    ) -> OptionalIntegrationSnapshot | None:
        snapshot = _optional_integration_snapshot(status=status, reason_code=reason_code)
        with self._lock:
            if not self._callback_authority_active:
                return None
            self._callback_authority_active = False
            self._snapshot = snapshot
            subscribers = tuple(self._subscribers.values())
            self._subscribers.clear()
        for lifecycle in subscribers:
            lifecycle.revoke(status=status, reason_code=reason_code)
        return snapshot

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
        self._lock = threading.Lock()
        self._scheduled_count = 0
        self._accepting = True
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
        with self._lock:
            if not self._accepting:
                raise RuntimeError("optional integration callback owner is stopping")
            if context is None:
                handle = scheduler(callback, *args)
            else:
                handle = scheduler(callback, *args, context=context)
            self._scheduled_count += 1
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
        with self._lock:
            if not self._accepting:
                raise RuntimeError("optional integration callback owner is stopping")
            if context is None:
                handle = self._call_at(when, callback, *args)
            else:
                handle = self._call_at(when, callback, *args, context=context)
            self._scheduled_count += 1
            return handle

    def drain_ready_callbacks(self) -> tuple[bool, int, int]:
        """Run transitive ready work until one complete turn schedules nothing."""
        for turn in range(1, _MAX_OPTIONAL_INTEGRATION_CALLBACK_DRAIN_TURNS + 1):
            with self._lock:
                before = self._scheduled_count
            self._call_soon(self._loop.stop)
            self._loop.run_forever()
            with self._lock:
                after = self._scheduled_count
            if after == before:
                return True, turn, after
        return False, _MAX_OPTIONAL_INTEGRATION_CALLBACK_DRAIN_TURNS, after

    def seal_if_unchanged(self, expected_scheduled_count: int) -> bool:
        """Reject future scheduling once the observed empty turn stays current."""
        with self._lock:
            if self._scheduled_count != expected_scheduled_count:
                return False
            self._accepting = False
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


def _publish_optional_integration_terminal_transition(
    state: _OptionalIntegrationExecutionState,
    transition: _OptionalIntegrationTerminalTransition,
    *,
    owner_finished: bool,
    release_registry: bool,
) -> None:
    """Publish terminal work after releasing execution-state locks."""
    if transition.lifecycle_status is not None:
        state.lifecycle.revoke(
            status=transition.lifecycle_status,
            reason_code=transition.lifecycle_reason_code,
        )
    for callback in transition.callbacks:
        try:
            callback(transition.settlement)
        except BaseException:
            pass
    if not state.completion.done():
        state.completion.set_result(transition.settlement)
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
    ) -> None:
        self._state = state
        self._subscription = subscription
        self._primary = primary
        self._released = False

    @classmethod
    def acquire(
        cls,
        *,
        name: str,
        runtime_identity: str,
        lifecycle: OptionalIntegrationLifecycle,
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
                )
                _OPTIONAL_INTEGRATION_EXECUTION_OWNERS[key] = state
                owner = cls(state, subscription=subscription, primary=True)
            else:
                with current.state_lock:
                    if not current.accepting_subscriptions:
                        raise OptionalIntegrationExecutionUnavailable("slack_execution_owner_stopping")
                    subscription, replay_snapshot = current.lifecycle.reserve_subscription(lifecycle)
                    current.active_subscriptions.add(subscription)
                owner = cls(current, subscription=subscription, primary=False)
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
                state.completion_callbacks[self._subscription] = on_completion
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
            settlement = OptionalIntegrationTaskSettlement(completed=True)
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                callback_drain = _OptionalIntegrationCallbackDrain(loop)
                operation = operation_factory()
                task: asyncio.Task[object] = loop.create_task(operation)
                with state.state_lock:
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

                def announce_retained_work(
                    *,
                    detached_task_count: int,
                    pending_task_count: int,
                    callback_drain_turns: int,
                    scheduled_callback_count: int,
                    callback_drain_exhausted: bool,
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
                        active_owner_count=active_owner_count,
                        fenced_identity_count=fenced_identity_count,
                    )
                    state.startup_resolved.wait()
                    transition = _commit_optional_integration_terminal_transition(state, settlement)
                    if transition is not None:
                        _publish_optional_integration_terminal_transition(
                            state,
                            transition,
                            owner_finished=False,
                            release_registry=False,
                        )

                if loop is not None and callback_drain is not None:
                    while True:
                        quiescent, callback_drain_turns, scheduled_callback_count = (
                            callback_drain.drain_ready_callbacks()
                        )
                        pending_tasks = tuple(pending for pending in asyncio.all_tasks(loop) if not pending.done())
                        detached_task_count = state.lifecycle.detached_task_count
                        retains_resources = _settlement_has_unretired_resources(settlement)
                        if retains_resources or detached_task_count or pending_tasks or not quiescent:
                            announce_retained_work(
                                detached_task_count=detached_task_count,
                                pending_task_count=len(pending_tasks),
                                callback_drain_turns=callback_drain_turns,
                                scheduled_callback_count=scheduled_callback_count,
                                callback_drain_exhausted=not quiescent,
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
                        if callback_drain.seal_if_unchanged(scheduled_callback_count):
                            break
                    loop.close()
                elif loop is not None:
                    loop.close()
                if retained_work_announced:
                    if _settlement_has_unretired_resources(settlement):
                        _release_optional_integration_execution_state(state)
                    else:
                        _retire_detached_optional_integration_execution_state(state)
                    state.finished.set()
                    return
                state.startup_resolved.wait()
                transition = _commit_optional_integration_terminal_transition(state, settlement)
                if transition is not None:
                    _publish_optional_integration_terminal_transition(
                        state,
                        transition,
                        owner_finished=True,
                        release_registry=True,
                    )
                else:
                    state.finished.set()
                    _release_optional_integration_execution_state(state)

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
