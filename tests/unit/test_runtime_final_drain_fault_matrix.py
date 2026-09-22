"""Fault matrix for generation-owned final runtime drain authority."""

from __future__ import annotations

import asyncio
import threading
import time
from asyncio import events as asyncio_events
from collections.abc import Callable
from concurrent.futures import Future as ThreadFuture
from contextlib import suppress
from typing import Any

import pytest

import tacit.pipeline_admission as admission_module
from tacit.pipeline_admission import (
    PipelineAdmissionController,
    RuntimeOwnershipError,
    RuntimeRootDrainStartupError,
    RuntimeRootOwnerHandle,
)

_DRAIN_THREAD_PREFIX = "tacit-runtime-root-drain-"


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("timed out waiting for lifecycle state")
        await asyncio.sleep(0.001)


def _wait_for_owned_drain_thread(graph: Any) -> None:
    with graph._lock:
        owner = graph._final_drain_owner
    assert owner is not None
    assert owner.finished.wait(timeout=1.0)
    if owner.thread is not None:
        owner.thread.join(timeout=1.0)
        assert owner.thread.is_alive() is False


def _assert_terminal_zero(controller: PipelineAdmissionController) -> None:
    graph = controller.execution_graph
    _wait_for_owned_drain_thread(graph)
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert graph.provider_manager() is None
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.queued == 0
    assert controller.retained == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_final_drain_waits_for_cancellation_resistant_retained_request_before_provider_shutdown() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    shutdown_started = threading.Event()
    cancellation_observed = asyncio.Event()
    allow_retained_finish = asyncio.Event()
    order: list[str] = []
    order_lock = threading.Lock()

    def record(event: str) -> None:
        with order_lock:
            order.append(event)

    class Manager:
        async def shutdown(self) -> None:
            record("provider-shutdown")
            shutdown_started.set()

    graph.resolve_provider_manager(spec="retained-request-manager", create=Manager)
    lease = await controller.acquire()

    async def cancellation_resistant_request() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_observed.set()
            await allow_retained_finish.wait()
            record("retained-request-finished")

    retained_task = asyncio.create_task(cancellation_resistant_request())
    await asyncio.sleep(0)
    retained_task.cancel()
    await cancellation_observed.wait()
    assert controller.retain_task(lease, retained_task) is True
    controller.release(lease)
    assert controller.retained == 1

    release_task = asyncio.create_task(graph.release_root_owner(root))
    await _wait_until(lambda: graph.root_state == "draining")

    # Keep the requester task suspended while the generation-owned drain runs.
    # A broken request-path predicate reaches provider shutdown in this window.
    premature_shutdown = shutdown_started.wait(timeout=0.15)
    allow_retained_finish.set()
    await retained_task
    await asyncio.sleep(0)
    await release_task

    _assert_terminal_zero(controller)
    assert premature_shutdown is False
    assert order == ["retained-request-finished", "provider-shutdown"]


@pytest.mark.asyncio
async def test_stopped_open_transport_loop_retains_at_most_one_drain_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StoppedOpenLoop:
        def __init__(self) -> None:
            self.callbacks: list[tuple[Callable[..., Any], tuple[Any, ...]]] = []

        def is_closed(self) -> bool:
            return False

        def call_soon_threadsafe(self, callback: Callable[..., Any], *args: Any) -> None:
            self.callbacks.append((callback, args))

    stopped_loop = StoppedOpenLoop()
    handle = RuntimeRootOwnerHandle(
        graph_nonce="heartbeat-matrix",
        generation=1,
        owner_id=1,
    )
    owner = admission_module._RuntimeRootDrainOwner(
        generation=1,
        handle=handle,
        transport_loop=stopped_loop,  # type: ignore[arg-type]
    )
    monkeypatch.setattr(admission_module, "_RUNTIME_DRAIN_HEARTBEAT_SECONDS", 0.001)

    heartbeat = asyncio.create_task(admission_module.RuntimeExecutionGraph._run_final_drain_heartbeat(owner))
    try:
        await asyncio.sleep(0.02)
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat

    assert len(stopped_loop.callbacks) == 1


def _patch_final_drain_failure(
    patcher: pytest.MonkeyPatch,
    mode: str,
) -> str:
    error_message = f"final-drain-{mode}-failed"
    original_start = threading.Thread.start
    if mode == "definite-thread-start":

        def fail_start(thread: threading.Thread) -> None:
            if thread.name.startswith(_DRAIN_THREAD_PREFIX):
                raise RuntimeError(error_message)
            original_start(thread)

        patcher.setattr(threading.Thread, "start", fail_start)
    elif mode == "ambiguous-thread-start":

        def start_then_fail(thread: threading.Thread) -> None:
            original_start(thread)
            if thread.name.startswith(_DRAIN_THREAD_PREFIX):
                raise RuntimeError(error_message)

        patcher.setattr(threading.Thread, "start", start_then_fail)
    elif mode == "runner-construction":

        def fail_runner_construction(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError(error_message)

        patcher.setattr(admission_module.asyncio, "Runner", fail_runner_construction)
    elif mode == "runner-preflight":

        def fail_runner_run(_runner: Any, awaitable: Any, *_args: Any, **_kwargs: Any) -> None:
            close = getattr(awaitable, "close", None)
            if callable(close):
                close()
            raise RuntimeError(error_message)

        patcher.setattr(admission_module.asyncio.Runner, "run", fail_runner_run)
    elif mode == "loop-construction":
        patcher.setattr(
            asyncio_events,
            "new_event_loop",
            lambda: (_ for _ in ()).throw(RuntimeError(error_message)),
        )
    else:  # pragma: no cover - parameter list is the contract
        raise AssertionError(f"unsupported failure mode: {mode}")
    return error_message


@pytest.mark.parametrize(
    "failure_mode",
    [
        "definite-thread-start",
        "ambiguous-thread-start",
        "runner-construction",
        "runner-preflight",
        "loop-construction",
    ],
)
@pytest.mark.asyncio
async def test_final_drain_startup_failures_preserve_recoverable_root_authority(
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()

    with monkeypatch.context() as failure_patch:
        error_message = _patch_final_drain_failure(failure_patch, failure_mode)
        with pytest.raises(RuntimeRootDrainStartupError) as observed:
            await graph.release_root_owner(root)
        assert observed.value.__cause__ is not None
        assert str(observed.value.__cause__) == error_message

    with graph._lock:
        failed_owner = graph._final_drain_owner
    if failed_owner is not None:
        assert failed_owner.finished.wait(timeout=1.0)

    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    assert controller.runtime_root_state == "active"

    await graph.release_root_owner(root)
    _assert_terminal_zero(controller)

    replacement = graph.register_root_owner()
    assert replacement.generation == root.generation + 1
    await graph.release_root_owner(replacement)
    _assert_terminal_zero(controller)


@pytest.mark.asyncio
async def test_prepared_lifecycle_owner_aborts_cleanly_when_fence_installation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    original_begin_root_drain = controller.begin_root_drain
    attempts = 0

    def fail_before_fence(generation: int) -> bool:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("root drain fence precommit failed")

    monkeypatch.setattr(controller, "begin_root_drain", fail_before_fence)
    with pytest.raises(RuntimeError, match="root drain fence precommit failed"):
        await graph.release_root_owner(root)

    assert attempts == 1
    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    assert controller.runtime_root_state == "active"
    assert controller.in_flight == 0
    assert controller.queued == 0

    monkeypatch.setattr(controller, "begin_root_drain", original_begin_root_drain)
    await graph.release_root_owner(root)
    _assert_terminal_zero(controller)


@pytest.mark.asyncio
async def test_root_drain_rollback_keeps_the_caller_loop_responsive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    heartbeat_started = asyncio.Event()
    heartbeat_ticks = 0

    async def heartbeat() -> None:
        nonlocal heartbeat_ticks
        heartbeat_started.set()
        while True:
            heartbeat_ticks += 1
            await asyncio.sleep(0.005)

    def delayed_aborted_owner(owner: Any) -> None:
        owner.lifecycle_ready.set()
        owner.start_gate.wait()
        time.sleep(0.12)
        if not owner.future.done():
            owner.future.set_result(None)
        owner.finished.set()

    def reject_fence(_generation: int) -> bool:
        raise RuntimeError("root drain fence precommit failed")

    monkeypatch.setattr(graph, "_run_final_root_drain", delayed_aborted_owner)
    monkeypatch.setattr(controller, "begin_root_drain", reject_fence)
    heartbeat_task = asyncio.create_task(heartbeat())
    await heartbeat_started.wait()
    ticks_before_release = heartbeat_ticks
    try:
        with pytest.raises(RuntimeError, match="root drain fence precommit failed"):
            await graph.release_root_owner(root)
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task

    assert heartbeat_ticks - ticks_before_release >= 8
    assert graph.root_state == "active"
    assert graph.root_owner_count == 1


@pytest.mark.asyncio
async def test_delayed_final_drain_readiness_keeps_the_caller_loop_responsive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    owner_entered = threading.Event()
    permit_readiness = threading.Event()
    original_drain_owner = graph._run_final_root_drain
    heartbeat_ticks = 0

    def delayed_drain_owner(owner: Any) -> None:
        owner_entered.set()
        assert permit_readiness.wait(timeout=1.0)
        original_drain_owner(owner)

    async def heartbeat() -> None:
        nonlocal heartbeat_ticks
        while True:
            heartbeat_ticks += 1
            await asyncio.sleep(0.002)

    monkeypatch.setattr(graph, "_run_final_root_drain", delayed_drain_owner)
    heartbeat_task = asyncio.create_task(heartbeat())
    await asyncio.sleep(0)
    ticks_before_release = heartbeat_ticks
    release_timer = threading.Timer(0.12, permit_readiness.set)
    release_timer.start()
    try:
        release_task = asyncio.create_task(graph.release_root_owner(root))
        assert await asyncio.to_thread(owner_entered.wait, 1.0)
        await release_task
    finally:
        permit_readiness.set()
        release_timer.cancel()
        release_timer.join(timeout=1.0)
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task

    assert heartbeat_ticks - ticks_before_release >= 20
    _assert_terminal_zero(controller)


@pytest.mark.asyncio
async def test_final_drain_readiness_cancellation_waits_for_owned_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    owner_entered = threading.Event()
    permit_readiness = threading.Event()
    original_drain_owner = graph._run_final_root_drain
    heartbeat_ticks = 0

    def delayed_drain_owner(owner: Any) -> None:
        owner_entered.set()
        assert permit_readiness.wait(timeout=1.0)
        original_drain_owner(owner)

    async def heartbeat() -> None:
        nonlocal heartbeat_ticks
        while True:
            heartbeat_ticks += 1
            await asyncio.sleep(0.002)

    monkeypatch.setattr(graph, "_run_final_root_drain", delayed_drain_owner)
    heartbeat_task = asyncio.create_task(heartbeat())
    release_task = asyncio.create_task(graph.release_root_owner(root))
    safety_timer = threading.Timer(0.8, permit_readiness.set)
    safety_timer.start()
    try:
        assert await asyncio.to_thread(owner_entered.wait, 1.0)
        ticks_before_cancel = heartbeat_ticks
        release_task.cancel()
        await asyncio.sleep(0.05)
        assert not release_task.done()
        assert heartbeat_ticks - ticks_before_cancel >= 10

        permit_readiness.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(release_task, timeout=1.0)
    finally:
        permit_readiness.set()
        safety_timer.cancel()
        safety_timer.join(timeout=1.0)
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task

    _assert_terminal_zero(controller)


@pytest.mark.parametrize("ambiguous_start", [False, True])
@pytest.mark.asyncio
async def test_final_drain_transition_worker_start_failure_preserves_single_authority(
    monkeypatch: pytest.MonkeyPatch,
    ambiguous_start: bool,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    original_start = threading.Thread.start
    transition_starts = 0

    def fail_transition_start(thread: threading.Thread) -> None:
        nonlocal transition_starts
        if not thread.name.startswith("tacit-lifecycle-transition-"):
            original_start(thread)
            return
        transition_starts += 1
        if ambiguous_start:
            original_start(thread)
        raise RuntimeError("transition worker start failed")

    with monkeypatch.context() as start_failure:
        start_failure.setattr(threading.Thread, "start", fail_transition_start)
        if ambiguous_start:
            await graph.release_root_owner(root)
        else:
            with pytest.raises(RuntimeError, match="transition worker start failed"):
                await graph.release_root_owner(root)

    assert transition_starts == 1
    if ambiguous_start:
        _assert_terminal_zero(controller)
        return

    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    assert controller.runtime_root_state == "active"
    await graph.release_root_owner(root)
    _assert_terminal_zero(controller)


@pytest.mark.asyncio
async def test_cancelled_final_drain_start_failure_fences_abandoned_root_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    owner_start_entered = threading.Event()
    permit_start_failure = threading.Event()
    original_start = threading.Thread.start

    def fail_owner_start(thread: threading.Thread) -> None:
        if not thread.name.startswith(_DRAIN_THREAD_PREFIX):
            original_start(thread)
            return
        owner_start_entered.set()
        assert permit_start_failure.wait(timeout=1.0)
        raise RuntimeError("cancelled owner start failed")

    monkeypatch.setattr(threading.Thread, "start", fail_owner_start)
    release_task = asyncio.create_task(graph.release_root_owner(root))
    assert await asyncio.to_thread(owner_start_entered.wait, 1.0)
    release_task.cancel()
    permit_start_failure.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(release_task, timeout=1.0)

    assert graph.root_state == "fenced"
    assert graph.root_owner_count == 0
    assert controller.runtime_fatal_circuit is not None
    assert controller.in_flight == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_stalled_root_drain_rollback_is_bounded_and_terminally_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    allow_owner_exit = threading.Event()

    def stalled_aborted_owner(owner: Any) -> None:
        owner.lifecycle_ready.set()
        owner.start_gate.wait()
        allow_owner_exit.wait(timeout=0.3)
        if not owner.future.done():
            owner.future.set_result(None)
        owner.finished.set()

    def reject_fence(_generation: int) -> bool:
        raise RuntimeError("root drain fence precommit failed")

    monkeypatch.setattr(admission_module, "_LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS", 0.03)
    monkeypatch.setattr(graph, "_run_final_root_drain", stalled_aborted_owner)
    monkeypatch.setattr(controller, "begin_root_drain", reject_fence)
    delayed_exit = threading.Timer(0.25, allow_owner_exit.set)
    delayed_exit.start()
    started = time.monotonic()
    try:
        with pytest.raises(RuntimeOwnershipError, match="rollback did not settle"):
            await graph.release_root_owner(root)
    finally:
        allow_owner_exit.set()
        delayed_exit.cancel()
    elapsed = time.monotonic() - started

    assert elapsed < 0.15
    assert graph.root_state == "fenced"
    assert graph.root_owner_count == 0
    assert controller.runtime_fatal_circuit is not None
    with graph._lock:
        owner = graph._final_drain_owner
    assert owner is not None
    assert owner.finished.wait(timeout=1.0)


@pytest.mark.asyncio
async def test_root_drain_rollback_preserves_cancellation_until_owner_settles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    rollback_started = threading.Event()
    allow_owner_exit = threading.Event()

    def cancellation_resistant_rollback(owner: Any) -> None:
        owner.lifecycle_ready.set()
        owner.start_gate.wait()
        rollback_started.set()
        allow_owner_exit.wait(timeout=1.0)
        if not owner.future.done():
            owner.future.set_result(None)
        owner.finished.set()

    def reject_fence(_generation: int) -> bool:
        raise RuntimeError("root drain fence precommit failed")

    monkeypatch.setattr(graph, "_run_final_root_drain", cancellation_resistant_rollback)
    monkeypatch.setattr(controller, "begin_root_drain", reject_fence)
    release_task = asyncio.create_task(graph.release_root_owner(root))
    assert await asyncio.to_thread(rollback_started.wait, 1.0)
    release_task.cancel()
    await asyncio.sleep(0)
    assert release_task.done() is False

    allow_owner_exit.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(release_task, timeout=1.0)

    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    assert controller.runtime_fatal_circuit is None


@pytest.mark.parametrize("shutdown_fails", [False, True])
@pytest.mark.asyncio
async def test_concurrent_final_release_followers_share_one_drain_and_terminal_result(
    monkeypatch: pytest.MonkeyPatch,
    shutdown_fails: bool,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    shutdown_started = threading.Event()
    allow_shutdown = ThreadFuture[None]()
    drain_calls = 0
    drain_calls_lock = threading.Lock()
    original_drain_owner = graph._run_final_root_drain

    def observed_drain_owner(owner: Any) -> None:
        nonlocal drain_calls
        with drain_calls_lock:
            drain_calls += 1
        original_drain_owner(owner)

    monkeypatch.setattr(graph, "_run_final_root_drain", observed_drain_owner)

    class Manager:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        async def shutdown(self) -> None:
            self.shutdown_calls += 1
            shutdown_started.set()
            await asyncio.wrap_future(allow_shutdown)
            if shutdown_fails:
                raise RuntimeError("shared-terminal-shutdown-failure")

    manager = Manager()
    graph.resolve_provider_manager(spec="leader-follower-manager", create=lambda: manager)

    leader = asyncio.create_task(graph.release_root_owner(root))
    assert await asyncio.to_thread(shutdown_started.wait, 1.0)
    follower = asyncio.create_task(graph.release_root_owner(root))
    await asyncio.sleep(0)
    assert leader.done() is False
    assert follower.done() is False

    allow_shutdown.set_result(None)
    results = await asyncio.gather(leader, follower, return_exceptions=True)
    _assert_terminal_zero(controller)

    assert drain_calls == 1
    assert manager.shutdown_calls == 1
    if shutdown_fails:
        assert [type(result) for result in results] == [RuntimeError, RuntimeError]
        assert [str(result) for result in results] == [
            "shared-terminal-shutdown-failure",
            "shared-terminal-shutdown-failure",
        ]
    else:
        assert results == [None, None]
