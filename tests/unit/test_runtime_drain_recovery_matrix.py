"""Recovery matrix for final runtime drain and public pipeline outcomes."""

from __future__ import annotations

import asyncio
import threading
import time
from asyncio import events as asyncio_events
from collections.abc import Callable
from typing import Any

import pytest

import tacit.pipeline.runner as runner_module
import tacit.pipeline_admission as admission_module
from tacit.config import Settings
from tacit.dependencies import (
    PipelineDependencies,
    acquire_runtime_root_scope,
    build_pipeline_dependencies,
    release_runtime_root_scope,
)
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.models.schemas import DashRequest, DashResponse
from tacit.pipeline.runner import run_pipeline
from tacit.pipeline_admission import (
    PipelineAdmissionController,
    RuntimeRootDrainStartupError,
    release_runtime_root_with_startup_retry,
)
from tacit.runtime_stores import RuntimeStores

_ROOT_CLEANUP_REASON = "pipeline_runtime_root_cleanup_failed"
_CLEANUP_SECRET = "ROOT_CLEANUP_SECRET=must-not-enter-results"
_FOLLOWER_COUNT = 128


class _RootCleanupFailure(RuntimeError):
    pass


class _PrimaryPipelineFailure(RuntimeError):
    pass


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("timed out waiting for lifecycle state")
        await asyncio.sleep(0.001)


def _patch_lifecycle_loop_start_failure(
    patcher: pytest.MonkeyPatch,
    mode: str,
) -> str:
    message = f"final-drain-{mode}-failed"
    original_thread_start = threading.Thread.start

    if mode == "definite-thread-start":

        def fail_thread_start(thread: threading.Thread) -> None:
            if thread.name.startswith("tacit-runtime-root-drain-"):
                raise RuntimeError(message)
            original_thread_start(thread)

        patcher.setattr(threading.Thread, "start", fail_thread_start)
    elif mode == "ambiguous-thread-start":

        def start_then_fail(thread: threading.Thread) -> None:
            original_thread_start(thread)
            if thread.name.startswith("tacit-runtime-root-drain-"):
                raise RuntimeError(message)

        patcher.setattr(threading.Thread, "start", start_then_fail)
    elif mode == "runner-construction":

        def fail_runner_construction(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError(message)

        patcher.setattr(admission_module.asyncio, "Runner", fail_runner_construction)
    elif mode == "runner-preflight":

        def fail_run(_runner: Any, awaitable: Any, *_args: Any, **_kwargs: Any) -> None:
            close = getattr(awaitable, "close", None)
            if callable(close):
                close()
            raise RuntimeError(message)

        patcher.setattr(admission_module.asyncio.Runner, "run", fail_run)
    elif mode == "loop-construction":
        patcher.setattr(
            asyncio_events,
            "new_event_loop",
            lambda: (_ for _ in ()).throw(RuntimeError(message)),
        )
    else:  # pragma: no cover - the parameter list is the contract
        raise AssertionError(f"unsupported lifecycle failure mode: {mode}")
    return message


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
async def test_drain_preflight_failure_preserves_every_queue_waiter_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=32)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    blocker = await controller.acquire()

    admissions = [0] * 16
    admission_order: list[int] = []

    async def queued_request(index: int) -> int:
        try:
            lease = await controller.acquire(timeout_seconds=1.0)
        except PipelineAdmissionRejected as exc:  # pragma: no cover - the matrix forbids rejection here
            pytest.fail(f"preflight failure rejected queued request: {exc.reason_code}")
        admissions[index] += 1
        admission_order.append(index)
        controller.release(lease)
        return index

    queued = [asyncio.create_task(queued_request(index)) for index in range(16)]
    await _wait_until(lambda: controller.queued == len(queued))

    with monkeypatch.context() as failure_patch:
        message = _patch_lifecycle_loop_start_failure(failure_patch, failure_mode)
        with pytest.raises(RuntimeRootDrainStartupError) as observed:
            await graph.release_root_owner(root)
        assert observed.value.__cause__ is not None
        assert str(observed.value.__cause__) == message

    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    assert controller.runtime_root_state == "active"
    assert controller.queued == len(queued)
    assert all(task.done() is False for task in queued)

    controller.release(blocker)
    outcomes = await asyncio.wait_for(asyncio.gather(*queued), timeout=1.0)
    assert outcomes == list(range(len(queued)))
    assert admissions == [1] * len(queued)
    assert admission_order == list(range(len(queued)))

    await graph.release_root_owner(root)
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.queued == 0


@pytest.mark.asyncio
async def test_large_follower_cohort_uses_one_bounded_relay_per_stopped_loop() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    shutdown_started = threading.Event()
    allow_shutdown = threading.Event()

    class BlockingManager:
        async def shutdown(self) -> None:
            shutdown_started.set()
            while not allow_shutdown.is_set():
                await asyncio.sleep(0.001)

    graph.resolve_provider_manager(spec="bounded-follower-relay", create=BlockingManager)
    leader = asyncio.create_task(graph.release_root_owner(root))
    assert await asyncio.to_thread(shutdown_started.wait, 1.0)

    with graph._lock:
        owner = graph._final_drain_owner
    assert owner is not None

    follower_loop_ready = threading.Event()
    followers_suspended = threading.Event()
    follower_loop_stopped = threading.Event()
    resume_followers = threading.Event()
    follower_loop_holder: list[asyncio.AbstractEventLoop] = []
    follower_results: list[Any] = []
    follower_errors: list[BaseException] = []

    def run_followers() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        follower_loop_holder.append(loop)
        follower_loop_ready.set()
        tasks: list[asyncio.Task[None]] = []
        entered = 0

        async def follow() -> None:
            nonlocal entered
            entered += 1
            await graph.release_root_owner(root)

        async def suspend_after_registration() -> None:
            tasks.extend(asyncio.create_task(follow()) for _ in range(_FOLLOWER_COUNT))
            while entered < _FOLLOWER_COUNT:
                await asyncio.sleep(0)
            # Every follower has now run release_root_owner through its first
            # await, which is where a transport relay would be registered.
            await asyncio.sleep(0)
            followers_suspended.set()
            loop.stop()

        loop.create_task(suspend_after_registration())
        try:
            loop.run_forever()
            follower_loop_stopped.set()
            resume_followers.wait(timeout=2.0)
            follower_results.extend(loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True)))
        except BaseException as exc:  # pragma: no cover - reported in the parent assertion
            follower_errors.append(exc)
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            asyncio.set_event_loop(None)
            loop.close()

    follower_thread = threading.Thread(target=run_followers, name="drain-follower-loop")
    follower_thread.start()
    try:
        assert follower_loop_ready.wait(timeout=1.0)
        assert followers_suspended.wait(timeout=1.0)
        assert follower_loop_stopped.wait(timeout=1.0)
        follower_loop = follower_loop_holder[0]

        pending_source_relays = len(getattr(owner.future, "_done_callbacks"))
        allow_shutdown.set()
        await asyncio.wait_for(leader, timeout=1.0)
        assert owner.finished.wait(timeout=1.0)
        pending_stopped_loop_callbacks = len(getattr(follower_loop, "_ready"))
    finally:
        allow_shutdown.set()
        resume_followers.set()
        follower_thread.join(timeout=2.0)

    assert follower_thread.is_alive() is False
    assert follower_errors == []
    assert follower_results == [None] * _FOLLOWER_COUNT
    # One source completion callback and one stopped-loop wakeup are enough for
    # an arbitrary number of followers sharing the same transport loop. The
    # leader's live loop may own one additional source relay.
    assert pending_source_relays <= 2
    assert pending_stopped_loop_callbacks <= 1
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"


def _settings(tmp_path: Any, *, suffix: str) -> Settings:
    return Settings(  # type: ignore[call-arg]
        _env_file=None,
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )


def _request() -> DashRequest:
    return DashRequest(
        prompt="checkout latency",
        user_id="runtime-drain-recovery",
        channel_id="matrix",
    )


def _published_response() -> DashResponse:
    return DashResponse(
        dashboard_url="https://dashboards.example/published-once",
        dashboard_uid="published-once",
        panel_count=1,
        summary="published exactly once",
        investigation_status="completed",
        audit_status="run_completed",
    )


def _dependencies_with_unreleased_root_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    *,
    suffix: str,
) -> tuple[
    RuntimeStores,
    PipelineDependencies,
    Callable[[Any], Any],
    list[str],
]:
    runtime_settings = _settings(tmp_path, suffix=suffix)
    stores = RuntimeStores(runtime_settings)
    original_shutdown = stores.shutdown_runtime_services

    shutdown_attempts: list[str] = []

    async def fail_before_release(_handle: Any) -> None:
        shutdown_attempts.append("shutdown")
        raise _RootCleanupFailure(_CLEANUP_SECRET)

    monkeypatch.setattr(stores, "shutdown_runtime_services", fail_before_release)
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    return stores, dependencies, original_shutdown, shutdown_attempts


async def _repair_unreleased_root(
    stores: RuntimeStores,
    dependencies: PipelineDependencies,
    original_shutdown: Callable[[Any], Any],
) -> None:
    coordinator = dependencies._runtime_root_coordinator
    assert coordinator is not None
    with coordinator._lock:
        owned_handle = coordinator._owned_handle
    assert owned_handle is not None
    await original_shutdown(owned_handle)
    with coordinator._lock:
        coordinator._owned_handle = None
        coordinator._owned_release = None
        coordinator._release_in_progress = False
    graph = stores.pipeline_admission().execution_graph
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0


def _is_explicit_post_publication_cleanup_result(response: DashResponse) -> bool:
    statuses = {
        response.investigation_status.strip().casefold(),
        response.audit_status.strip().casefold(),
    }
    return any("cleanup" in status or "degraded" in status for status in statuses)


def _is_nonretryable_post_publication_cleanup_error(error: BaseException) -> bool:
    return (
        getattr(error, "reason_code", None) == _ROOT_CLEANUP_REASON
        and getattr(error, "publication_committed", False) is True
        and getattr(error, "retryable", True) is False
        and getattr(error, "cleanup_error_type", None) == _RootCleanupFailure.__name__
    )


@pytest.mark.asyncio
async def test_published_success_cannot_hide_root_cleanup_failure_with_live_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    stores, dependencies, original_shutdown, shutdown_attempts = _dependencies_with_unreleased_root_failure(
        monkeypatch,
        tmp_path,
        suffix="published-cleanup",
    )
    publications = 0

    async def publish_once(*_args: Any, **_kwargs: Any) -> DashResponse:
        nonlocal publications
        publications += 1
        return _published_response()

    monkeypatch.setattr(runner_module, "_run_pipeline_with_dependencies", publish_once)
    response: DashResponse | None = None
    observed_error: BaseException | None = None
    try:
        response = await run_pipeline(_request(), dependencies)
    except BaseException as exc:
        observed_error = exc

    graph = stores.pipeline_admission().execution_graph
    authority_was_still_active = graph.root_state == "active" and graph.root_owner_count == 1
    await _repair_unreleased_root(stores, dependencies, original_shutdown)

    assert publications == 1
    assert shutdown_attempts == ["shutdown"]
    assert authority_was_still_active is True
    if response is not None:
        assert observed_error is None
        assert _is_explicit_post_publication_cleanup_result(response)
    else:
        assert observed_error is not None
        assert _is_nonretryable_post_publication_cleanup_error(observed_error)
    assert _CLEANUP_SECRET not in repr(response)
    assert _CLEANUP_SECRET not in repr(observed_error)


@pytest.mark.asyncio
async def test_primary_pipeline_failure_keeps_identity_and_structured_cleanup_detail(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    stores, dependencies, original_shutdown, shutdown_attempts = _dependencies_with_unreleased_root_failure(
        monkeypatch,
        tmp_path,
        suffix="primary-cleanup",
    )
    primary = _PrimaryPipelineFailure("primary pipeline failure")

    async def fail_pipeline(*_args: Any, **_kwargs: Any) -> DashResponse:
        raise primary

    monkeypatch.setattr(runner_module, "_run_pipeline_with_dependencies", fail_pipeline)
    observed_error: BaseException | None = None
    try:
        await run_pipeline(_request(), dependencies)
    except BaseException as exc:
        observed_error = exc

    graph = stores.pipeline_admission().execution_graph
    authority_was_still_active = graph.root_state == "active" and graph.root_owner_count == 1
    await _repair_unreleased_root(stores, dependencies, original_shutdown)

    assert observed_error is primary
    assert shutdown_attempts == ["shutdown"]
    assert authority_was_still_active is True
    assert getattr(primary, "cleanup_reason_code", None) == _ROOT_CLEANUP_REASON
    assert getattr(primary, "cleanup_error_type", None) == _RootCleanupFailure.__name__
    assert _CLEANUP_SECRET not in repr(primary)


@pytest.mark.asyncio
@pytest.mark.parametrize("injected_dependencies", [False, True])
async def test_direct_pipeline_retries_root_drain_startup_once_and_reaches_terminal_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    injected_dependencies: bool,
) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix=f"direct-startup-retry-{injected_dependencies}",
    )
    stores = RuntimeStores(runtime_settings)
    original_shutdown = stores.shutdown_runtime_services
    shutdown_calls = 0

    async def fail_preflight_once(handle: Any) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls == 1:
            raise RuntimeRootDrainStartupError("lifecycle owner preflight failed")
        await original_shutdown(handle)

    monkeypatch.setattr(stores, "shutdown_runtime_services", fail_preflight_once)
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)

    async def publish_once(*_args: Any, **_kwargs: Any) -> DashResponse:
        return _published_response()

    monkeypatch.setattr(runner_module, "_run_pipeline_with_dependencies", publish_once)

    if injected_dependencies:
        response = await run_pipeline(_request(), dependencies)
    else:
        monkeypatch.setattr(runner_module, "settings", runtime_settings)
        monkeypatch.setattr(runner_module, "_default_runtime_stores", lambda: stores)
        monkeypatch.setattr(runner_module, "_default_dependencies", lambda selected: dependencies)
        response = await run_pipeline(_request())

    controller = stores.pipeline_admission()
    graph = controller.execution_graph
    assert response.dashboard_uid == "published-once"
    assert shutdown_calls == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.in_flight == 0
    assert controller.queued == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_direct_root_retry_exhaustion_transfers_authority_and_fences_runtime() -> None:
    controller = PipelineAdmissionController(
        1,
        max_queued=0,
        runtime_identity="matrix:direct-root-retry-exhaustion",
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    release_calls = 0

    async def fail_twice(handle: Any) -> None:
        nonlocal release_calls
        release_calls += 1
        if release_calls <= 2:
            raise RuntimeRootDrainStartupError(f"startup failed {release_calls}")
        await graph.release_root_owner(handle)

    with pytest.raises(RuntimeRootDrainStartupError, match="startup failed 2"):
        await release_runtime_root_with_startup_retry(fail_twice, root)

    assert release_calls == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.queued == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0
    assert controller.runtime_fatal_circuit is not None

    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        graph.register_root_owner()


@pytest.mark.asyncio
async def test_retry_exhaustion_keeps_prefence_capacity_charged_until_graph_drain() -> None:
    controller = PipelineAdmissionController(
        1,
        max_queued=0,
        runtime_identity="matrix:retry-exhaustion-prefence-capacity",
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    lease = await controller.acquire()
    release_calls = 0
    provider_shutdowns = 0

    class Manager:
        async def shutdown(self) -> None:
            nonlocal provider_shutdowns
            provider_shutdowns += 1

    graph.resolve_provider_manager(spec="retry-exhaustion-manager", create=Manager)

    async def fail_twice(handle: Any) -> None:
        nonlocal release_calls
        release_calls += 1
        if release_calls <= 2:
            raise RuntimeRootDrainStartupError(f"startup failed {release_calls}")
        await graph.release_root_owner(handle)

    release_task = asyncio.create_task(release_runtime_root_with_startup_retry(fail_twice, root))
    await _wait_until(lambda: graph.root_state == "draining")

    assert release_calls == 2
    assert controller.in_flight == 1
    assert provider_shutdowns == 0

    controller.release(lease)
    with pytest.raises(RuntimeRootDrainStartupError, match="startup failed 2"):
        await asyncio.wait_for(release_task, timeout=1.0)

    assert provider_shutdowns == 1
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.retained == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_direct_root_retry_exhaustion_preserves_cancellation_after_graph_cleanup() -> None:
    controller = PipelineAdmissionController(
        1,
        max_queued=0,
        runtime_identity="matrix:direct-retry-exhaustion-cancellation",
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    release_calls = 0
    shutdown_started = threading.Event()
    allow_shutdown = threading.Event()

    class Manager:
        async def shutdown(self) -> None:
            shutdown_started.set()
            while not allow_shutdown.is_set():
                await asyncio.sleep(0.001)

    graph.resolve_provider_manager(spec="direct-cancelled-recovery", create=Manager)

    async def fail_twice(handle: Any) -> None:
        nonlocal release_calls
        release_calls += 1
        if release_calls <= 2:
            raise RuntimeRootDrainStartupError(f"startup failed {release_calls}")
        await graph.release_root_owner(handle)

    release_task = asyncio.create_task(release_runtime_root_with_startup_retry(fail_twice, root))
    assert await asyncio.to_thread(shutdown_started.wait, 1.0)
    release_task.cancel()
    await asyncio.sleep(0)
    assert release_task.done() is False

    allow_shutdown.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(release_task, timeout=1.0)

    assert release_calls == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.retained == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_borrowed_root_retry_exhaustion_preserves_cancellation_and_consumes_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="borrow-retry-exhaustion-cancellation")
    stores = RuntimeStores(runtime_settings)
    original_shutdown = stores.shutdown_runtime_services
    shutdown_calls = 0
    shutdown_started = threading.Event()
    allow_shutdown = threading.Event()

    async def fail_twice(handle: Any) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls <= 2:
            raise RuntimeRootDrainStartupError(f"startup failed {shutdown_calls}")
        await original_shutdown(handle)

    class Manager:
        async def shutdown(self) -> None:
            shutdown_started.set()
            while not allow_shutdown.is_set():
                await asyncio.sleep(0.001)

    monkeypatch.setattr(stores, "shutdown_runtime_services", fail_twice)
    handle = acquire_runtime_root_scope(stores)
    graph = stores.pipeline_admission().execution_graph
    graph.resolve_provider_manager(spec="borrowed-cancelled-recovery", create=Manager)

    release_task = asyncio.create_task(release_runtime_root_with_startup_retry(release_runtime_root_scope, handle))
    assert await asyncio.to_thread(shutdown_started.wait, 1.0)
    release_task.cancel()
    await asyncio.sleep(0)
    assert release_task.done() is False

    allow_shutdown.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(release_task, timeout=1.0)

    coordinator = handle._coordinator
    with coordinator._lock:
        assert coordinator._uses == {}
        assert coordinator._owned_handle is None
        assert coordinator._owned_release is None
        assert coordinator._release_in_progress is False
        assert coordinator._startup_retry_tokens == set()
    assert shutdown_calls == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert stores.pipeline_admission().in_flight == 0
    assert stores.pipeline_admission().retained == 0


@pytest.mark.asyncio
async def test_borrowed_root_exhaustion_blocks_new_borrow_before_graph_handoff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="borrow-exhaustion-handoff-race")
    stores = RuntimeStores(runtime_settings)
    original_shutdown = stores.shutdown_runtime_services
    shutdown_calls = 0
    transfer_entered = asyncio.Event()
    allow_transfer = asyncio.Event()

    async def fail_twice(handle: Any) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls <= 2:
            raise RuntimeRootDrainStartupError(f"startup failed {shutdown_calls}")
        await original_shutdown(handle)

    monkeypatch.setattr(stores, "shutdown_runtime_services", fail_twice)
    handle = acquire_runtime_root_scope(stores)
    handle_type = type(handle)
    original_transfer = handle_type._transfer_drain_startup_exhaustion

    async def pause_transfer(selected: Any, error: BaseException) -> None:
        transfer_entered.set()
        await allow_transfer.wait()
        await original_transfer(selected, error)

    monkeypatch.setattr(handle_type, "_transfer_drain_startup_exhaustion", pause_transfer)
    release_task = asyncio.create_task(release_runtime_root_with_startup_retry(release_runtime_root_scope, handle))
    await asyncio.wait_for(transfer_entered.wait(), timeout=1.0)

    second_handle = None
    second_error: BaseException | None = None
    try:
        second_handle = acquire_runtime_root_scope(stores)
    except BaseException as exc:
        second_error = exc

    allow_transfer.set()
    with pytest.raises(RuntimeRootDrainStartupError, match="startup failed 2"):
        await asyncio.wait_for(release_task, timeout=1.0)
    if second_handle is not None:
        await release_runtime_root_with_startup_retry(release_runtime_root_scope, second_handle)

    graph = stores.pipeline_admission().execution_graph
    assert second_handle is None
    assert isinstance(second_error, RuntimeOwnershipError)
    assert "shutting down" in str(second_error)
    assert shutdown_calls == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert stores.pipeline_admission().runtime_fatal_circuit is not None


@pytest.mark.asyncio
async def test_pending_cancellation_cannot_prevent_borrowed_root_handoff(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="borrow-exhaustion-pending-cancel")
    stores = RuntimeStores(runtime_settings)
    shutdown_calls = 0

    async def fail_twice(_handle: Any) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls == 2:
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
        raise RuntimeRootDrainStartupError(f"startup failed {shutdown_calls}")

    monkeypatch.setattr(stores, "shutdown_runtime_services", fail_twice)
    handle = acquire_runtime_root_scope(stores)

    with pytest.raises(asyncio.CancelledError):
        await release_runtime_root_with_startup_retry(release_runtime_root_scope, handle)

    coordinator = handle._coordinator
    with coordinator._lock:
        assert coordinator._uses == {}
        assert coordinator._startup_retry_tokens == set()
        assert coordinator._owned_handle is None
        assert coordinator._owned_release is None
        assert coordinator._release_in_progress is False
    graph = stores.pipeline_admission().execution_graph
    assert shutdown_calls == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert stores.pipeline_admission().runtime_fatal_circuit is not None


@pytest.mark.asyncio
async def test_retry_exhaustion_keeps_a_durable_owner_when_lifecycle_start_stays_broken(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(
        1,
        max_queued=0,
        runtime_identity="matrix:persistent-root-start-failure",
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    original_thread_start = threading.Thread.start
    drain_start_calls = 0
    recovery_start_calls = 0

    def reject_drain_thread(thread: threading.Thread) -> None:
        nonlocal drain_start_calls, recovery_start_calls
        if thread.name.startswith("tacit-runtime-root-drain-"):
            drain_start_calls += 1
            raise RuntimeError("runtime cannot start lifecycle owner")
        if thread.name.startswith("tacit-runtime-root-recovery-"):
            recovery_start_calls += 1
            raise RuntimeError("runtime cannot start recovery owner")
        original_thread_start(thread)

    monkeypatch.setattr(threading.Thread, "start", reject_drain_thread)

    with pytest.raises(RuntimeRootDrainStartupError):
        await release_runtime_root_with_startup_retry(graph.release_root_owner, root)

    assert drain_start_calls == 2
    assert recovery_start_calls == 1
    assert graph.root_state == "draining"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "draining"
    assert controller.in_flight == 0
    assert controller.queued == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0
    assert controller.runtime_fatal_circuit is not None
    with graph._lock:
        recovery_owner = graph._final_drain_owner
    assert recovery_owner is not None
    assert recovery_owner.handle == root
    assert recovery_owner.terminal is False
    assert recovery_owner.future.done() is True

    with pytest.raises(RuntimeOwnershipError, match="stale or duplicate"):
        await asyncio.wait_for(graph.release_root_owner(root), timeout=0.1)

    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        graph.register_root_owner()


@pytest.mark.asyncio
async def test_pipeline_retry_exhaustion_cannot_leave_a_discarded_borrow_for_later_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="borrow-retry-exhaustion")
    stores = RuntimeStores(runtime_settings)
    original_shutdown = stores.shutdown_runtime_services
    shutdown_calls = 0
    publications = 0

    async def fail_twice(handle: Any) -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1
        if shutdown_calls <= 2:
            raise RuntimeRootDrainStartupError(f"startup failed {shutdown_calls}")
        await original_shutdown(handle)

    async def publish_once(*_args: Any, **_kwargs: Any) -> DashResponse:
        nonlocal publications
        publications += 1
        return _published_response()

    monkeypatch.setattr(stores, "shutdown_runtime_services", fail_twice)
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    monkeypatch.setattr(runner_module, "_run_pipeline_with_dependencies", publish_once)

    response = await run_pipeline(_request(), dependencies)

    controller = stores.pipeline_admission()
    graph = controller.execution_graph
    assert response.dashboard_uid == "published-once"
    assert shutdown_calls == 2
    assert publications == 1
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.queued == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0
    assert controller.runtime_fatal_circuit is not None

    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await run_pipeline(_request(), dependencies)
    assert publications == 1
