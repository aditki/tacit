from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

import pytest

import tacit.dependencies as dependencies_module
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.config import Settings
from tacit.dependencies import (
    PipelineDependencies,
    ProviderLeaseHandle,
    ProviderLifecycleState,
    _RuntimeProviderResources,
    declare_backend_factory,
)
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController
from tacit.runtime_ownership import (
    declare_runtime_factory,
    runtime_descriptor_for_provider,
    runtime_descriptor_for_store,
)


class _ProbeProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings, *, label: str) -> None:
        super().__init__(runtime_settings, component=f"provider_atomicity_{label}")
        self.calls = 0
        self.close_calls = 0
        self.closed = False

    async def chat_json(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self.calls += 1
        return LLMResult("{}")

    async def chat_text(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self.calls += 1
        return LLMResult("ok")

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


def _settings(
    tmp_path: Any,
    *,
    suffix: str,
    limit: int = 2,
    queued: int = 0,
) -> Settings:
    values: dict[str, Any] = {
        "_env_file": None,
        "history_db_path": str(tmp_path / f"{suffix}-history.db"),
        "feedback_db_path": str(tmp_path / f"{suffix}-feedback.db"),
        "signals_db_path": str(tmp_path / f"{suffix}-signals.db"),
        "llm_provider": "ollama",
        "llm_api_base": "http://127.0.0.1:11434",
        "context_provider": "none",
        "pipeline_max_concurrent": limit,
        "pipeline_max_queued": queued,
    }
    return Settings(**values)


def _resources(
    tmp_path: Any,
    *,
    suffix: str,
    limit: int = 2,
    queued: int = 0,
    provider_factory: Callable[[], LLMProvider] | None = None,
    chained_cleanup: Callable[[], Awaitable[None]] | None = None,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController, Settings]:
    runtime_settings = _settings(
        tmp_path,
        suffix=suffix,
        limit=limit,
        queued=queued,
    )
    controller = PipelineAdmissionController(limit, max_queued=queued)
    selected_factory = provider_factory or (lambda: _ProbeProvider(runtime_settings, label=suffix))
    declared_factory = declare_runtime_factory(
        selected_factory,
        ownership=runtime_descriptor_for_provider(
            component=f"provider_atomicity_{suffix}",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    resources = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=controller,
        llm_factory=declared_factory,
        chained_cleanup=chained_cleanup,
        cleanup_grace_seconds=0.1,
    )
    return resources, controller, runtime_settings


def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        threading.Event().wait(0.001)
    return bool(predicate())


def _active_generation_operations(resources: _RuntimeProviderResources) -> int:
    with resources._lock:
        state = resources._generation_owner
        return 0 if state is None else len(state.active_operations)


def _terminal_counts(
    resources: _RuntimeProviderResources,
    controller: PipelineAdmissionController,
) -> dict[str, int]:
    return {
        "operations": _active_generation_operations(resources),
        "active": controller.in_flight,
        "queued": controller.queued,
        "retained": controller.retained,
        "blocking": controller.blocking_in_flight,
        "service_owner": controller.service_owner_in_flight,
    }


def _gated_owner_runner(
    *,
    owner_entered: threading.Event,
    release_owner: threading.Event,
) -> type[Any]:
    real_runner = asyncio.Runner

    class GatedOwnerRunner:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._runner = real_runner(*args, **kwargs)
            self._patched = False

        def __enter__(self) -> GatedOwnerRunner:
            self._runner.__enter__()
            return self

        def __exit__(self, *args: Any) -> Any:
            return self._runner.__exit__(*args)

        def get_loop(self) -> asyncio.AbstractEventLoop:
            loop = self._runner.get_loop()
            if not self._patched:
                original_run_forever = loop.run_forever
                run_count = 0

                def gated_run_forever() -> None:
                    nonlocal run_count
                    run_count += 1
                    if run_count == 1:
                        owner_entered.set()
                        assert release_owner.wait(timeout=2.0)
                    original_run_forever()

                loop.run_forever = gated_run_forever  # type: ignore[method-assign]
                self._patched = True
            return loop

    return GatedOwnerRunner


@pytest.mark.asyncio
async def test_delayed_generation_readiness_does_not_block_requester_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    owner_entered = threading.Event()
    release_owner = threading.Event()
    requester_heartbeat = threading.Event()
    heartbeat_before_release: list[bool] = []
    monkeypatch.setattr(
        dependencies_module.asyncio,
        "Runner",
        _gated_owner_runner(
            owner_entered=owner_entered,
            release_owner=release_owner,
        ),
    )
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="delayed-readiness-heartbeat",
    )

    def release_after_heartbeat_window() -> None:
        assert owner_entered.wait(timeout=1.0)
        threading.Event().wait(0.1)
        heartbeat_before_release.append(requester_heartbeat.is_set())
        release_owner.set()

    release_thread = threading.Thread(
        target=release_after_heartbeat_window,
        name="provider-readiness-heartbeat-release",
    )
    release_thread.start()
    asyncio.get_running_loop().call_later(0.01, requester_heartbeat.set)

    handle = await asyncio.wait_for(resources.acquire(), timeout=2.0)
    await resources.close(handle)
    release_thread.join(timeout=1.0)

    assert release_thread.is_alive() is False
    assert heartbeat_before_release == [True]
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_cancel_during_delayed_generation_readiness_retires_to_terminal_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    owner_entered = threading.Event()
    release_owner = threading.Event()
    cancellation_dispatched = threading.Event()
    cancellation_before_release: list[bool] = []
    monkeypatch.setattr(
        dependencies_module.asyncio,
        "Runner",
        _gated_owner_runner(
            owner_entered=owner_entered,
            release_owner=release_owner,
        ),
    )
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="cancel-delayed-readiness",
    )

    request = asyncio.create_task(resources.acquire())

    def cancel_request() -> None:
        cancellation_dispatched.set()
        request.cancel()

    def release_after_cancellation_window() -> None:
        assert owner_entered.wait(timeout=1.0)
        threading.Event().wait(0.1)
        cancellation_before_release.append(cancellation_dispatched.is_set())
        release_owner.set()

    release_thread = threading.Thread(
        target=release_after_cancellation_window,
        name="provider-readiness-cancellation-release",
    )
    release_thread.start()
    asyncio.get_running_loop().call_later(0.01, cancel_request)

    with pytest.raises(asyncio.CancelledError):
        await request
    assert await asyncio.to_thread(
        _wait_until,
        lambda: _terminal_counts(resources, controller)
        == {
            "operations": 0,
            "active": 0,
            "queued": 0,
            "retained": 0,
            "blocking": 0,
            "service_owner": 0,
        },
    )
    release_thread.join(timeout=1.0)

    assert release_thread.is_alive() is False
    assert cancellation_before_release == [True]


@pytest.mark.asyncio
async def test_shutdown_during_delayed_generation_readiness_reaches_terminal_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    owner_entered = threading.Event()
    release_owner = threading.Event()
    shutdown_dispatched = threading.Event()
    monkeypatch.setattr(
        dependencies_module.asyncio,
        "Runner",
        _gated_owner_runner(
            owner_entered=owner_entered,
            release_owner=release_owner,
        ),
    )
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="shutdown-delayed-readiness",
    )
    request = asyncio.create_task(resources.acquire())
    assert await asyncio.to_thread(owner_entered.wait, 1.0)

    async def shutdown() -> None:
        shutdown_dispatched.set()
        await resources.shutdown()

    shutdown_task = asyncio.create_task(shutdown())
    await asyncio.wait_for(asyncio.to_thread(shutdown_dispatched.wait, 1.0), timeout=1.0)
    await asyncio.sleep(0)
    assert shutdown_task.done() is False
    release_owner.set()

    request_result = await asyncio.wait_for(
        asyncio.gather(request, return_exceptions=True),
        timeout=2.0,
    )
    await asyncio.wait_for(shutdown_task, timeout=2.0)

    assert len(request_result) == 1
    assert isinstance(request_result[0], RuntimeOwnershipError)
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_shutdown_fences_late_initialization_before_owner_settlement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    first_owner_entered = threading.Event()
    release_first_readiness = threading.Event()
    shutdown_joined_first_owner = asyncio.Event()
    allow_shutdown_return = asyncio.Event()
    late_initialization_finished = threading.Event()
    second_owner_start_attempted = threading.Event()
    release_second_owner_start = threading.Event()
    monkeypatch.setattr(
        dependencies_module.asyncio,
        "Runner",
        _gated_owner_runner(
            owner_entered=first_owner_entered,
            release_owner=release_first_readiness,
        ),
    )
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="shutdown-late-initialization-fence",
    )
    real_start_owner = dependencies_module._start_lifecycle_owner_thread
    owner_start_calls = 0

    def start_owner(**kwargs: Any) -> threading.Thread:
        nonlocal owner_start_calls
        owner_start_calls += 1
        if owner_start_calls == 2:
            second_owner_start_attempted.set()
            assert release_second_owner_start.wait(timeout=2.0)
        return real_start_owner(**kwargs)

    real_wait_for_close = resources._wait_for_generation_close
    shutdown_task: asyncio.Task[None] | None = None

    async def wait_for_close_after_owner_join(state: Any) -> bool:
        result = await real_wait_for_close(state)
        if asyncio.current_task() is shutdown_task:
            shutdown_joined_first_owner.set()
            await allow_shutdown_return.wait()
        return result

    real_ensure_llm = resources._ensure_llm_provider

    async def ensure_llm_after_first_release() -> None:
        await shutdown_joined_first_owner.wait()
        try:
            await real_ensure_llm()
        finally:
            late_initialization_finished.set()

    monkeypatch.setattr(dependencies_module, "_start_lifecycle_owner_thread", start_owner)
    monkeypatch.setattr(resources, "_wait_for_generation_close", wait_for_close_after_owner_join)
    monkeypatch.setattr(resources, "_ensure_llm_provider", ensure_llm_after_first_release)

    request = asyncio.create_task(resources.acquire())
    assert await asyncio.to_thread(first_owner_entered.wait, 1.0)
    shutdown_task = asyncio.create_task(resources.shutdown())
    release_first_readiness.set()

    try:
        await asyncio.wait_for(shutdown_joined_first_owner.wait(), timeout=1.0)
        assert await asyncio.to_thread(
            _wait_until,
            lambda: late_initialization_finished.is_set() or second_owner_start_attempted.is_set(),
        )
        allow_shutdown_return.set()
        await asyncio.wait_for(shutdown_task, timeout=2.0)
        service_owners_at_shutdown_return = controller.service_owner_in_flight
    finally:
        allow_shutdown_return.set()
        release_second_owner_start.set()

    request_result = await asyncio.wait_for(
        asyncio.gather(request, return_exceptions=True),
        timeout=2.0,
    )
    assert await asyncio.to_thread(
        _wait_until,
        lambda: _terminal_counts(resources, controller)
        == {
            "operations": 0,
            "active": 0,
            "queued": 0,
            "retained": 0,
            "blocking": 0,
            "service_owner": 0,
        },
    )

    assert len(request_result) == 1
    assert isinstance(request_result[0], RuntimeOwnershipError)
    assert second_owner_start_attempted.is_set() is False
    assert service_owners_at_shutdown_return == 0
    assert owner_start_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", range(5))
async def test_delayed_generation_readiness_coalesces_concurrent_start_races(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    attempt: int,
) -> None:
    owner_entered = threading.Event()
    release_owner = threading.Event()
    requesters_lock = threading.Lock()
    requesters_entered = 0
    requesters_before_release: list[int] = []
    all_acquired = asyncio.Event()
    release_handles = asyncio.Event()
    acquired_count = 0
    owner_start_count = 0
    real_start = dependencies_module._start_lifecycle_owner_thread
    monkeypatch.setattr(
        dependencies_module.asyncio,
        "Runner",
        _gated_owner_runner(
            owner_entered=owner_entered,
            release_owner=release_owner,
        ),
    )

    def counted_start(**kwargs: Any) -> threading.Thread:
        nonlocal owner_start_count
        owner_start_count += 1
        return real_start(**kwargs)

    monkeypatch.setattr(dependencies_module, "_start_lifecycle_owner_thread", counted_start)
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix=f"delayed-readiness-race-{attempt}",
    )

    async def acquire() -> ProviderLeaseHandle:
        nonlocal acquired_count, requesters_entered
        with requesters_lock:
            requesters_entered += 1
        handle = await resources.acquire()
        acquired_count += 1
        if acquired_count == 8:
            all_acquired.set()
        await release_handles.wait()
        await resources.close(handle)
        return handle

    def release_after_requester_window() -> None:
        assert owner_entered.wait(timeout=1.0)
        threading.Event().wait(0.05)
        with requesters_lock:
            requesters_before_release.append(requesters_entered)
        release_owner.set()

    release_thread = threading.Thread(
        target=release_after_requester_window,
        name=f"provider-readiness-race-release-{attempt}",
    )
    release_thread.start()
    requests = [asyncio.create_task(acquire()) for _ in range(8)]
    await asyncio.wait_for(all_acquired.wait(), timeout=2.0)
    release_handles.set()
    handles = await asyncio.wait_for(asyncio.gather(*requests), timeout=2.0)
    release_thread.join(timeout=1.0)

    assert release_thread.is_alive() is False
    assert requesters_before_release == [8]
    assert owner_start_count == 1
    assert {handle.generation_epoch for handle in handles} == {handles[0].generation_epoch}
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_generation_transition_thread_constructor_failure_rolls_back_and_retries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="generation-transition-constructor-failure",
    )
    original_thread = threading.Thread
    construction_attempts = 0

    def fail_first_transition_construction(*args: Any, **kwargs: Any) -> threading.Thread:
        nonlocal construction_attempts
        if str(kwargs.get("name", "")).startswith("tacit-provider-generation-startup-transition-"):
            construction_attempts += 1
            if construction_attempts == 1:
                raise RuntimeError("synthetic generation transition construction failure")
        return original_thread(*args, **kwargs)

    monkeypatch.setattr(threading, "Thread", fail_first_transition_construction)

    with pytest.raises(RuntimeError, match="transition could not start"):
        await resources.acquire()

    assert resources.lifecycle_state is ProviderLifecycleState.EMPTY
    assert resources._generation_owner is None
    assert controller.service_owner_in_flight == 0

    handle = await resources.acquire()
    await resources.close(handle)

    assert construction_attempts == 2
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_generation_start_transition_definite_failure_rolls_back_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="generation-transition-definite-failure",
    )
    original_start = threading.Thread.start

    def fail_transition_start(thread: threading.Thread) -> None:
        if thread.name.startswith("tacit-provider-generation-startup-transition-"):
            raise RuntimeError("synthetic generation transition start failure")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_transition_start)

    with pytest.raises(RuntimeError, match="transition could not start"):
        await resources.acquire()

    assert resources._llm_provider is None
    assert resources.lifecycle_state is ProviderLifecycleState.EMPTY
    assert resources._generation_owner is None
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_generation_start_transition_ambiguous_start_is_not_replayed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    transition_runs = 0
    owner_starts = 0
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="generation-transition-ambiguous-start",
    )
    original_start = threading.Thread.start

    def start_then_raise(thread: threading.Thread) -> None:
        nonlocal owner_starts, transition_runs
        if thread.name.startswith("tacit-provider-generation-startup-transition-"):
            target = thread._target  # type: ignore[attr-defined]
            assert target is not None

            def counted_transition() -> None:
                nonlocal transition_runs
                transition_runs += 1
                target()

            thread._target = counted_transition  # type: ignore[attr-defined]
            original_start(thread)
            raise RuntimeError("synthetic ambiguous generation transition start")
        if thread.name == "tacit-lifecycle-provider-owner":
            owner_starts += 1
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start_then_raise)

    handle = await resources.acquire()
    await resources.close(handle)

    assert transition_runs == 1
    assert owner_starts == 1
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_owner_readiness_is_published_only_after_callbacks_can_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    run_forever_entered = threading.Event()
    permit_run_forever = threading.Event()
    callback_executed = threading.Event()
    real_runner = asyncio.Runner

    class GatedRunner:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._runner = real_runner(*args, **kwargs)
            self._patched = False

        def __enter__(self) -> GatedRunner:
            self._runner.__enter__()
            return self

        def __exit__(self, *args: Any) -> Any:
            return self._runner.__exit__(*args)

        def get_loop(self) -> asyncio.AbstractEventLoop:
            loop = self._runner.get_loop()
            if not self._patched:
                original_run_forever = loop.run_forever

                def gated_run_forever() -> None:
                    run_forever_entered.set()
                    assert permit_run_forever.wait(timeout=2.0)
                    original_run_forever()

                loop.run_forever = gated_run_forever  # type: ignore[method-assign]
                self._patched = True
            return loop

    monkeypatch.setattr(dependencies_module.asyncio, "Runner", GatedRunner)
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="owner-readiness",
    )

    def start_owner() -> Any:
        with resources._lock:
            return resources._start_generation_owner_locked()

    owner_start = asyncio.create_task(asyncio.to_thread(start_owner))
    assert await asyncio.to_thread(run_forever_entered.wait, 1.0)
    with resources._lock:
        state = resources._generation_owner
    assert state is not None
    assert state.loop is not None
    state.loop.call_soon_threadsafe(callback_executed.set)
    readiness_published_before_callbacks = state.ready.done()

    permit_run_forever.set()
    assert await owner_start is state
    assert await asyncio.to_thread(callback_executed.wait, 1.0)
    await asyncio.wrap_future(state.ready)
    await resources.shutdown()

    assert readiness_published_before_callbacks is False
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_ambiguous_live_owner_start_fences_and_releases_service_owner_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    release_owner = threading.Event()
    owner_started = threading.Event()
    release_calls = 0
    real_thread_start = threading.Thread.start
    real_start_transition = dependencies_module._start_lifecycle_owner_thread

    def bounded_start_transition(**kwargs: Any) -> threading.Thread:
        kwargs["timeout_seconds"] = 0.02
        return real_start_transition(**kwargs)

    def start_then_fail(thread: threading.Thread) -> None:
        if thread.name != "tacit-lifecycle-provider-owner":
            real_thread_start(thread)
            return
        target = thread._target  # type: ignore[attr-defined]

        def delayed_target() -> None:
            owner_started.set()
            assert release_owner.wait(timeout=1.0)
            assert target is not None
            target()

        thread._target = delayed_target  # type: ignore[attr-defined]
        real_thread_start(thread)
        raise RuntimeError("synthetic ambiguous provider-owner start")

    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="ambiguous-live-owner",
    )
    real_release = controller.release_service_owner

    def observe_release(permit: Any) -> None:
        nonlocal release_calls
        release_calls += 1
        real_release(permit)

    monkeypatch.setattr(dependencies_module, "_start_lifecycle_owner_thread", bounded_start_transition)
    monkeypatch.setattr(threading.Thread, "start", start_then_fail)
    monkeypatch.setattr(controller, "release_service_owner", observe_release)

    try:
        with pytest.raises(RuntimeError, match="startup was ambiguous"):
            await asyncio.wait_for(resources.acquire(), timeout=0.5)
        assert owner_started.is_set()
        assert resources.lifecycle_state is ProviderLifecycleState.REVOKED
        assert controller.runtime_fatal_circuit is not None
        assert controller.service_owner_in_flight == 1
    finally:
        release_owner.set()

    assert _wait_until(lambda: controller.service_owner_in_flight == 0)
    assert release_calls == 1
    assert resources.lifecycle_state is not ProviderLifecycleState.STARTING
    assert resources._generation_owner is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ("runner_construction", "pre_run_forever", "callback_readiness"))
async def test_provider_owner_readiness_errors_use_the_shared_bounded_transition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    failure_phase: str,
) -> None:
    readiness_arguments: list[threading.Event | None] = []
    real_start_transition = dependencies_module._start_lifecycle_owner_thread
    real_runner = asyncio.Runner

    def bounded_start_transition(**kwargs: Any) -> threading.Thread:
        readiness_arguments.append(kwargs.get("ready"))
        kwargs["timeout_seconds"] = 0.02
        return real_start_transition(**kwargs)

    if failure_phase == "runner_construction":

        class ConstructionFailingRunner:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                raise RuntimeError("synthetic runner construction failure")

        monkeypatch.setattr(dependencies_module.asyncio, "Runner", ConstructionFailingRunner)
    else:

        class ReadinessFailingRunner:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self._runner = real_runner(*args, **kwargs)

            def __enter__(self) -> ReadinessFailingRunner:
                self._runner.__enter__()
                return self

            def __exit__(self, *args: Any) -> Any:
                return self._runner.__exit__(*args)

            def get_loop(self) -> asyncio.AbstractEventLoop:
                if failure_phase == "pre_run_forever":
                    raise RuntimeError("synthetic pre-run-forever failure")
                loop = self._runner.get_loop()
                original_call_soon = loop.call_soon

                def fail_readiness(callback: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
                    if getattr(callback, "__name__", "") == "publish_readiness":
                        raise RuntimeError("synthetic callback readiness failure")
                    return original_call_soon(callback, *args, **kwargs)

                loop.call_soon = fail_readiness  # type: ignore[assignment,method-assign]
                return loop

        monkeypatch.setattr(dependencies_module.asyncio, "Runner", ReadinessFailingRunner)

    monkeypatch.setattr(dependencies_module, "_start_lifecycle_owner_thread", bounded_start_transition)
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix=f"readiness-{failure_phase}",
    )

    with pytest.raises((RuntimeError, RuntimeOwnershipError)):
        await asyncio.wait_for(resources.acquire(), timeout=0.5)

    assert readiness_arguments and readiness_arguments[0] is not None
    assert resources.lifecycle_state is not ProviderLifecycleState.STARTING
    assert _wait_until(lambda: controller.service_owner_in_flight == 0)


@pytest.mark.asyncio
async def test_provider_owner_callback_readiness_timeout_is_bounded_and_fatal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    release_run_forever = threading.Event()
    run_forever_entered = threading.Event()
    real_start_transition = dependencies_module._start_lifecycle_owner_thread
    real_runner = asyncio.Runner

    def bounded_start_transition(**kwargs: Any) -> threading.Thread:
        kwargs["timeout_seconds"] = 0.02
        return real_start_transition(**kwargs)

    class StalledRunner:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._runner = real_runner(*args, **kwargs)

        def __enter__(self) -> StalledRunner:
            self._runner.__enter__()
            return self

        def __exit__(self, *args: Any) -> Any:
            return self._runner.__exit__(*args)

        def get_loop(self) -> asyncio.AbstractEventLoop:
            loop = self._runner.get_loop()
            original_run_forever = loop.run_forever
            run_calls = 0

            def stalled_run_forever() -> None:
                nonlocal run_calls
                run_calls += 1
                if run_calls > 1:
                    original_run_forever()
                    return
                run_forever_entered.set()
                release_run_forever.wait(timeout=1.0)

            loop.run_forever = stalled_run_forever  # type: ignore[method-assign]
            return loop

    monkeypatch.setattr(dependencies_module, "_start_lifecycle_owner_thread", bounded_start_transition)
    monkeypatch.setattr(dependencies_module.asyncio, "Runner", StalledRunner)
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="callback-readiness-timeout",
    )

    try:
        with pytest.raises(RuntimeError, match="startup was ambiguous"):
            await asyncio.wait_for(resources.acquire(), timeout=0.5)
        assert run_forever_entered.is_set()
        assert resources.lifecycle_state is ProviderLifecycleState.REVOKED
        assert controller.runtime_fatal_circuit is not None
    finally:
        release_run_forever.set()

    assert _wait_until(lambda: controller.service_owner_in_flight == 0)
    assert resources.lifecycle_state is not ProviderLifecycleState.STARTING


@pytest.mark.asyncio
async def test_enqueue_then_raise_keeps_charge_until_callback_terminal_and_bounds_limit_plus_one(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="enqueue-then-raise",
        limit=1,
        queued=0,
    )
    handle = await resources.acquire()
    provider = resources.llm()
    assert isinstance(provider, _ProbeProvider)
    state = resources._generation_owner
    assert state is not None and state.loop is not None

    owner_blocked = threading.Event()
    release_owner = threading.Event()

    def block_owner() -> None:
        owner_blocked.set()
        assert release_owner.wait(timeout=2.0)

    state.loop.call_soon_threadsafe(block_owner)
    assert await asyncio.to_thread(owner_blocked.wait, 1.0)

    original_call_soon_threadsafe = state.loop.call_soon_threadsafe
    ambiguous_submission = threading.Event()
    submission_count = 0

    def enqueue_then_raise_once(callback: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal submission_count
        if getattr(callback, "__name__", "") == "submit_operation":
            submission_count += 1
            result = original_call_soon_threadsafe(callback, *args, **kwargs)
            if not ambiguous_submission.is_set():
                ambiguous_submission.set()
                raise RuntimeError("synthetic enqueue-then-raise")
            return result
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(state.loop, "call_soon_threadsafe", enqueue_then_raise_once)
    first = asyncio.create_task(provider.chat_text("system", "first"))
    assert await asyncio.to_thread(ambiguous_submission.wait, 1.0)
    await asyncio.sleep(0)
    charged_before_terminal = controller.in_flight == 1 and _active_generation_operations(resources) == 1

    second = asyncio.create_task(provider.chat_text("system", "second"))
    for _ in range(20):
        if second.done() or submission_count > 1:
            break
        await asyncio.sleep(0)
    second_was_rejected = second.done() and isinstance(second.exception(), PipelineAdmissionRejected)
    submissions_before_release = submission_count

    release_owner.set()
    first_result, second_result = await asyncio.wait_for(
        asyncio.gather(first, second, return_exceptions=True),
        timeout=2.0,
    )
    await resources.close(handle)

    assert charged_before_terminal is True
    assert submissions_before_release == 1
    assert isinstance(first_result, LLMResult)
    assert second_was_rejected is True
    assert isinstance(second_result, PipelineAdmissionRejected)
    assert provider.calls == 1
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_double_owner_dispatch_failure_cancels_request_and_final_root_reaches_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="double-owner-dispatch-failure",
        limit=1,
        queued=0,
    )
    root_handle = controller.execution_graph.register_root_owner()
    await resources.acquire()
    provider = resources.llm()
    state = resources._generation_owner
    assert state is not None and state.loop is not None
    owner_loop = state.loop
    terminal_monitor = state.terminal_monitor_task
    assert terminal_monitor is not None
    original_call_soon_threadsafe = owner_loop.call_soon_threadsafe
    dispatch_error = RuntimeError("synthetic primary owner dispatch failure")
    probe_error = RuntimeError("synthetic owner dispatch probe failure")
    dispatches: list[str] = []

    def fail_submission_and_probe(callback: Any, *args: Any, **kwargs: Any) -> Any:
        callback_name = getattr(callback, "__name__", "")
        if callback_name == "submit_operation":
            dispatches.append(callback_name)
            raise dispatch_error
        if callback_name == "settle_ambiguous_submission":
            dispatches.append(callback_name)
            raise probe_error
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(state.loop, "call_soon_threadsafe", fail_submission_and_probe)
    request = asyncio.create_task(provider.chat_text("system", "cancel me"))
    assert await asyncio.to_thread(_wait_until, lambda: len(dispatches) == 2)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    controller.execution_graph.release_root_owner_detached(root_handle)
    reached_terminal_zero = await asyncio.to_thread(
        _wait_until,
        lambda: _terminal_counts(resources, controller)
        == {
            "operations": 0,
            "active": 0,
            "queued": 0,
            "retained": 0,
            "blocking": 0,
            "service_owner": 0,
        },
        timeout=1.0,
    )

    assert reached_terminal_zero is True
    assert dispatches == ["submit_operation", "settle_ambiguous_submission"]
    assert state.done.is_set()
    assert state.service_owner_released is True
    assert state.active_operations == set()
    assert state.active_handoffs == set()
    assert state.committed_submissions == {}
    assert state.terminal_error is not None
    assert state.terminal_error.__cause__ is dispatch_error
    assert terminal_monitor.done()


@pytest.mark.asyncio
async def test_ambiguous_submission_with_failed_probe_preserves_operation_error_and_reaches_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    operation_error = RuntimeError("synthetic primary provider operation failure")

    class FailingProbeProvider(_ProbeProvider):
        async def chat_text(self, *_args: Any, **_kwargs: Any) -> LLMResult:
            self.calls += 1
            raise operation_error

    resources, controller, runtime_settings = _resources(
        tmp_path,
        suffix="ambiguous-dispatch-failed-probe",
        limit=1,
        queued=0,
        provider_factory=lambda: FailingProbeProvider(runtime_settings, label="ambiguous-dispatch"),
    )
    root_handle = controller.execution_graph.register_root_owner()
    await resources.acquire()
    provider = resources.llm()
    state = resources._generation_owner
    assert state is not None and state.loop is not None
    terminal_monitor = state.terminal_monitor_task
    assert terminal_monitor is not None

    owner_blocked = threading.Event()
    release_owner = threading.Event()

    def block_owner() -> None:
        owner_blocked.set()
        assert release_owner.wait(timeout=2.0)

    state.loop.call_soon_threadsafe(block_owner)
    assert await asyncio.to_thread(owner_blocked.wait, 1.0)
    original_call_soon_threadsafe = state.loop.call_soon_threadsafe
    dispatch_error = RuntimeError("synthetic ambiguous primary dispatch failure")
    probe_error = RuntimeError("synthetic failed ambiguity probe")
    dispatches: list[str] = []

    def enqueue_first_and_fail_probe(callback: Any, *args: Any, **kwargs: Any) -> Any:
        callback_name = getattr(callback, "__name__", "")
        if callback_name == "submit_operation":
            dispatches.append(callback_name)
            original_call_soon_threadsafe(callback, *args, **kwargs)
            raise dispatch_error
        if callback_name == "settle_ambiguous_submission":
            dispatches.append(callback_name)
            raise probe_error
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(state.loop, "call_soon_threadsafe", enqueue_first_and_fail_probe)
    request = asyncio.create_task(provider.chat_text("system", "preserve my error"))
    assert await asyncio.to_thread(_wait_until, lambda: len(dispatches) == 2)
    release_owner.set()
    result = await asyncio.wait_for(asyncio.gather(request, return_exceptions=True), timeout=1.0)

    controller.execution_graph.release_root_owner_detached(root_handle)
    reached_terminal_zero = await asyncio.to_thread(
        _wait_until,
        lambda: _terminal_counts(resources, controller)
        == {
            "operations": 0,
            "active": 0,
            "queued": 0,
            "retained": 0,
            "blocking": 0,
            "service_owner": 0,
        },
        timeout=1.0,
    )

    assert result == [operation_error]
    assert result[0] is operation_error
    assert reached_terminal_zero is True
    assert dispatches == ["submit_operation", "settle_ambiguous_submission"]
    assert state.active_operations == set()
    assert state.active_handoffs == set()
    assert state.committed_submissions == {}
    assert state.terminal_error is not None
    assert state.terminal_error.__cause__ is dispatch_error
    assert terminal_monitor.done()


@pytest.mark.asyncio
async def test_cleanup_stop_dispatch_repeated_failure_uses_owner_terminal_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="cleanup-stop-dispatch-failure",
    )
    handle = await resources.acquire()
    provider = resources.llm()
    assert isinstance(provider, _ProbeProvider)
    state = resources._generation_owner
    assert state is not None and state.loop is not None
    owner_loop = state.loop
    terminal_monitor = state.terminal_monitor_task
    assert terminal_monitor is not None
    original_call_soon_threadsafe = owner_loop.call_soon_threadsafe
    stop_dispatches = 0

    def fail_owner_stop(callback: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal stop_dispatches
        if callback == owner_loop.stop:
            stop_dispatches += 1
            raise RuntimeError("synthetic owner-stop dispatch failure")
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(owner_loop, "call_soon_threadsafe", fail_owner_stop)
    await asyncio.wait_for(resources.close(handle), timeout=1.0)

    assert stop_dispatches == 2
    assert provider.close_calls == 1
    assert controller.runtime_fatal_circuit is None
    assert state.done.is_set()
    assert state.service_owner_released is True
    assert terminal_monitor.done()
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


def test_requester_loop_loss_during_initialization_cannot_strand_generation_capacity(
    tmp_path: Any,
) -> None:
    factory_started = threading.Event()
    release_factory = threading.Event()
    providers: list[_ProbeProvider] = []
    caller_ready = threading.Event()
    caller_stopped = threading.Event()
    caller_loop_box: list[asyncio.AbstractEventLoop] = []
    caller_task_box: list[asyncio.Task[ProviderLeaseHandle]] = []

    runtime_settings = _settings(tmp_path, suffix="requester-loop-loss")

    def provider_factory() -> LLMProvider:
        factory_started.set()
        assert release_factory.wait(timeout=2.0)
        provider = _ProbeProvider(runtime_settings, label="requester-loop-loss")
        providers.append(provider)
        return provider

    resources, controller, _settings_from_resources = _resources(
        tmp_path,
        suffix="requester-loop-loss",
        provider_factory=provider_factory,
    )

    def run_requester_until_stopped() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        caller_loop_box.append(loop)
        caller_task_box.append(loop.create_task(resources.acquire()))
        caller_ready.set()
        loop.run_forever()
        caller_stopped.set()

    caller = threading.Thread(
        target=run_requester_until_stopped,
        name="provider-initialization-requester",
    )
    caller.start()
    assert caller_ready.wait(timeout=1.0)
    assert factory_started.wait(timeout=1.0)
    caller_loop = caller_loop_box[0]
    caller_loop.call_soon_threadsafe(caller_loop.stop)
    assert caller_stopped.wait(timeout=1.0)
    caller.join(timeout=1.0)
    assert caller.is_alive() is False

    release_factory.set()
    assert _wait_until(lambda: bool(providers), timeout=1.0)

    shutdown_done = threading.Event()
    shutdown_errors: list[BaseException] = []

    def shutdown_runtime() -> None:
        try:
            asyncio.run(resources.shutdown())
        except BaseException as exc:  # pragma: no cover - retained for diagnostic evidence
            shutdown_errors.append(exc)
        finally:
            shutdown_done.set()

    shutdown = threading.Thread(
        target=shutdown_runtime,
        name="provider-initialization-shutdown",
        daemon=True,
    )
    shutdown.start()
    shutdown_completed_without_requester = shutdown_done.wait(timeout=0.25)
    counts_without_requester = _terminal_counts(resources, controller)

    caller_task = caller_task_box[0]

    def settle_requester() -> None:
        asyncio.set_event_loop(caller_loop)
        try:
            caller_loop.run_until_complete(asyncio.gather(caller_task, return_exceptions=True))
        finally:
            caller_loop.close()

    requester_cleanup = threading.Thread(
        target=settle_requester,
        name="provider-initialization-requester-cleanup",
    )
    requester_cleanup.start()
    requester_cleanup.join(timeout=2.0)
    shutdown.join(timeout=2.0)

    assert requester_cleanup.is_alive() is False
    assert shutdown.is_alive() is False
    assert shutdown_errors == []
    assert shutdown_completed_without_requester is True
    assert counts_without_requester == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_final_lease_release_and_replacement_acquire_have_one_atomic_winner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    providers: list[_ProbeProvider] = []
    runtime_settings = _settings(tmp_path, suffix="lease-retirement-race")

    def provider_factory() -> LLMProvider:
        provider = _ProbeProvider(runtime_settings, label=f"lease-retirement-{len(providers)}")
        providers.append(provider)
        return provider

    resources, controller, _settings_from_resources = _resources(
        tmp_path,
        suffix="lease-retirement-race",
        provider_factory=provider_factory,
    )
    first = await resources.acquire()
    assert len(providers) == 1

    retirement_reached = threading.Event()
    allow_retirement = threading.Event()
    close_errors: list[BaseException] = []
    original_begin_retirement = resources._begin_generation_retirement

    def gated_begin_retirement(*args: Any, **kwargs: Any) -> None:
        retirement_reached.set()
        assert allow_retirement.wait(timeout=2.0)
        original_begin_retirement(*args, **kwargs)

    monkeypatch.setattr(resources, "_begin_generation_retirement", gated_begin_retirement)

    def release_first() -> None:
        try:
            asyncio.run(resources.close(first))
        except BaseException as exc:  # pragma: no cover - retained for diagnostic evidence
            close_errors.append(exc)

    release_thread = threading.Thread(target=release_first, name="provider-final-lease-release")
    release_thread.start()
    assert await asyncio.to_thread(retirement_reached.wait, 1.0)

    replacement_task = asyncio.create_task(resources.acquire())
    for _ in range(20):
        if replacement_task.done():
            break
        await asyncio.sleep(0)
    allow_retirement.set()
    replacement = await asyncio.wait_for(replacement_task, timeout=2.0)
    release_thread.join(timeout=2.0)
    assert release_thread.is_alive() is False
    assert close_errors == []

    same_generation_survived = replacement.generation_epoch == first.generation_epoch
    if same_generation_survived:
        invariant_preserved = providers[0].closed is False
    else:
        invariant_preserved = (
            replacement.generation_epoch > first.generation_epoch
            and providers[0].closed is True
            and len(providers) == 2
        )

    replacement_close_error: BaseException | None = None
    try:
        await resources.close(replacement)
    except BaseException as exc:  # pragma: no cover - retained for diagnostic evidence
        replacement_close_error = exc

    assert invariant_preserved is True
    assert replacement_close_error is None
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.parametrize("attempt", range(5))
def test_startup_failure_retirement_reserves_the_generation_before_replacement_acquire(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    attempt: int,
) -> None:
    provider_calls = 0
    provider_calls_lock = threading.Lock()
    replacement_factory_started = threading.Event()
    runtime_settings = _settings(
        tmp_path,
        suffix=f"startup-retirement-reservation-{attempt}",
    )

    def provider_factory() -> LLMProvider:
        nonlocal provider_calls
        with provider_calls_lock:
            provider_calls += 1
            call_number = provider_calls
        if call_number == 1:
            raise RuntimeError("synthetic provider startup failure")
        replacement_factory_started.set()
        return _ProbeProvider(
            runtime_settings,
            label=f"startup-retirement-replacement-{attempt}",
        )

    resources, controller, _settings_from_resources = _resources(
        tmp_path,
        suffix=f"startup-retirement-reservation-{attempt}",
        provider_factory=provider_factory,
    )
    retirement_reached = threading.Event()
    allow_retirement = threading.Event()
    retirement_states: list[Any] = []
    original_retire_unleased = resources._retire_unleased_generation

    async def gated_retire_unleased(state: Any, **kwargs: Any) -> None:
        retirement_states.append(state)
        retirement_reached.set()
        assert await asyncio.to_thread(allow_retirement.wait, 2.0)
        await original_retire_unleased(state, **kwargs)

    monkeypatch.setattr(resources, "_retire_unleased_generation", gated_retire_unleased)

    first_errors: list[BaseException] = []

    def acquire_failing_generation() -> None:
        try:
            asyncio.run(resources.acquire())
        except BaseException as exc:
            first_errors.append(exc)

    first_thread = threading.Thread(
        target=acquire_failing_generation,
        name=f"provider-startup-failure-{attempt}",
    )
    first_thread.start()
    assert retirement_reached.wait(timeout=1.0)
    assert len(retirement_states) == 1
    failed_state = retirement_states[0]
    with resources._lock:
        reserved_before_retirement_wait = (
            resources._generation_owner is failed_state
            and resources._closing_event is failed_state
            and resources.lifecycle_state in {ProviderLifecycleState.DRAINING, ProviderLifecycleState.REVOKED}
            and not resources._active_leases
        )

    replacement_reached_manager = threading.Event()
    original_resolve_manager = resources._resolve_active_manager

    def tracked_resolve_manager() -> _RuntimeProviderResources:
        if threading.current_thread().name == f"provider-replacement-acquire-{attempt}":
            replacement_reached_manager.set()
        return original_resolve_manager()

    monkeypatch.setattr(resources, "_resolve_active_manager", tracked_resolve_manager)
    replacement_handles: list[ProviderLeaseHandle] = []
    replacement_errors: list[BaseException] = []

    def acquire_replacement_generation() -> None:
        try:
            replacement_handles.append(asyncio.run(resources.acquire()))
        except BaseException as exc:
            replacement_errors.append(exc)

    replacement_thread = threading.Thread(
        target=acquire_replacement_generation,
        name=f"provider-replacement-acquire-{attempt}",
    )
    replacement_thread.start()
    assert replacement_reached_manager.wait(timeout=1.0)
    replacement_entered_unreserved_generation = replacement_factory_started.wait(
        timeout=0.05 if reserved_before_retirement_wait else 1.0
    )

    allow_retirement.set()
    first_thread.join(timeout=2.0)
    replacement_thread.join(timeout=2.0)
    assert first_thread.is_alive() is False
    assert replacement_thread.is_alive() is False

    retry_handle: ProviderLeaseHandle | None = None
    if not replacement_handles and len(replacement_errors) == 1:
        retry_handle = asyncio.run(resources.acquire())
    try:
        terminal_handle = replacement_handles[0] if replacement_handles else retry_handle
        if terminal_handle is not None:
            asyncio.run(resources.close(terminal_handle))
        else:
            try:
                asyncio.run(resources.shutdown())
            except RuntimeOwnershipError:
                pass
    finally:
        allow_retirement.set()

    assert reserved_before_retirement_wait is True
    assert replacement_entered_unreserved_generation is False
    assert len(first_errors) == 1
    assert isinstance(first_errors[0], RuntimeError)
    assert len(replacement_errors) == 1
    assert isinstance(replacement_errors[0], RuntimeOwnershipError)
    assert replacement_handles == []
    assert retry_handle is not None
    assert retry_handle.generation_epoch > 1
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.asyncio
async def test_pre_generation_cleanup_survives_requester_cancellation_and_is_retry_safe(
    tmp_path: Any,
) -> None:
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    cleanup_completed = threading.Event()
    cleanup_calls = 0

    async def chained_cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        cleanup_started.set()
        while not release_cleanup.is_set():
            await asyncio.sleep(0)
        cleanup_completed.set()

    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="pre-generation-cancel",
        chained_cleanup=chained_cleanup,
    )
    cleanup_task = asyncio.create_task(resources.close())
    assert await asyncio.to_thread(cleanup_started.wait, 1.0)
    cleanup_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cleanup_task
    release_cleanup.set()
    completed_without_requester = await asyncio.to_thread(cleanup_completed.wait, 0.25)
    await resources.close()

    assert completed_without_requester is True
    assert cleanup_calls == 1
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


def test_pre_generation_cleanup_survives_stopped_requester_loop(
    tmp_path: Any,
) -> None:
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    cleanup_completed = threading.Event()
    caller_ready = threading.Event()
    caller_stopped = threading.Event()
    caller_loop_box: list[asyncio.AbstractEventLoop] = []
    caller_task_box: list[asyncio.Task[None]] = []
    cleanup_calls = 0

    async def chained_cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        cleanup_started.set()
        while not release_cleanup.is_set():
            await asyncio.sleep(0)
        cleanup_completed.set()

    resources, controller, _runtime_settings = _resources(
        tmp_path,
        suffix="pre-generation-loop-loss",
        chained_cleanup=chained_cleanup,
    )

    def run_requester() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        caller_loop_box.append(loop)
        caller_task_box.append(loop.create_task(resources.close()))
        caller_ready.set()
        loop.run_forever()
        caller_stopped.set()

    caller = threading.Thread(target=run_requester, name="pre-generation-cleanup-requester")
    caller.start()
    assert caller_ready.wait(timeout=1.0)
    assert cleanup_started.wait(timeout=1.0)
    caller_loop = caller_loop_box[0]
    caller_loop.call_soon_threadsafe(caller_loop.stop)
    assert caller_stopped.wait(timeout=1.0)
    caller.join(timeout=1.0)
    assert caller.is_alive() is False

    release_cleanup.set()
    completed_without_requester = cleanup_completed.wait(timeout=0.25)
    caller_task = caller_task_box[0]

    def settle_requester() -> None:
        asyncio.set_event_loop(caller_loop)
        try:
            caller_loop.run_until_complete(asyncio.gather(caller_task, return_exceptions=True))
        finally:
            caller_loop.close()

    cleanup_thread = threading.Thread(target=settle_requester, name="pre-generation-cleanup-settlement")
    cleanup_thread.start()
    cleanup_thread.join(timeout=2.0)

    assert cleanup_thread.is_alive() is False
    assert completed_without_requester is True
    assert cleanup_calls == 1
    assert _terminal_counts(resources, controller) == {
        "operations": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


@pytest.mark.parametrize(
    ("drop_acquire", "drop_cleanup"),
    [(True, False), (False, True), (True, True)],
)
def test_raw_dependencies_cannot_remove_accepted_provider_lifecycle_hooks(
    tmp_path: Any,
    drop_acquire: bool,
    drop_cleanup: bool,
) -> None:
    runtime_settings = _settings(tmp_path, suffix=f"raw-hooks-{drop_acquire}-{drop_cleanup}")
    provider_calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal provider_calls
        provider_calls += 1
        return _ProbeProvider(runtime_settings, label="raw-lifecycle-hooks")

    owned = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=declare_backend_factory(
            lambda: [],
            runtime_settings=runtime_settings,
            component="provider_atomicity_backends",
        ),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="provider_atomicity_history",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="provider_atomicity_feedback",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="provider_atomicity_raw_dependencies",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
    )

    with pytest.raises(RuntimeOwnershipError, match="provider lifecycle"):
        replace(
            owned,
            resource_acquire=None if drop_acquire else owned.resource_acquire,
            resource_cleanup=None if drop_cleanup else owned.resource_cleanup,
        )

    assert provider_calls == 0


def test_pipeline_dependencies_reject_foreign_provider_lifecycle_callbacks(tmp_path: Any) -> None:
    provider_calls = {"first": 0, "second": 0}

    def dependencies(label: str) -> PipelineDependencies:
        runtime_settings = _settings(tmp_path, suffix=f"foreign-provider-{label}")

        def provider_factory() -> LLMProvider:
            provider_calls[label] += 1
            return _ProbeProvider(runtime_settings, label=label)

        return PipelineDependencies.isolated(
            settings=runtime_settings,
            backend_factory=declare_backend_factory(
                lambda: [],
                runtime_settings=runtime_settings,
                component=f"provider_atomicity_{label}_backends",
            ),
            history_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component=f"provider_atomicity_{label}_history",
                    runtime_settings=runtime_settings,
                    database_role="history",
                    database_path=runtime_settings.history_db_path,
                ),
                factory_kind="store:history",
            ),
            feedback_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component=f"provider_atomicity_{label}_feedback",
                    runtime_settings=runtime_settings,
                    database_role="feedback",
                    database_path=runtime_settings.feedback_db_path,
                ),
                factory_kind="store:feedback",
            ),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
            llm_provider_factory=declare_runtime_factory(
                provider_factory,
                ownership=runtime_descriptor_for_provider(
                    component=f"provider_atomicity_{label}_provider",
                    runtime_settings=runtime_settings,
                    capability="llm",
                ),
                factory_kind="provider:llm",
            ),
        )

    first = dependencies("first")
    second = dependencies("second")

    with pytest.raises(RuntimeOwnershipError, match="provider lifecycle owner"):
        replace(
            first,
            resource_acquire=second.resource_acquire,
            resource_cleanup=second.resource_cleanup,
        )

    assert provider_calls == {"first": 0, "second": 0}
    assert first.pipeline_admission is not None
    assert second.pipeline_admission is not None
    assert first.pipeline_admission.service_owner_in_flight == 0
    assert second.pipeline_admission.service_owner_in_flight == 0


def test_pipeline_dependencies_reject_split_provider_lifecycle_callbacks(tmp_path: Any) -> None:
    provider_calls = {"first": 0, "second": 0}

    def dependencies(label: str) -> PipelineDependencies:
        runtime_settings = _settings(tmp_path, suffix=f"split-provider-{label}")

        def provider_factory() -> LLMProvider:
            provider_calls[label] += 1
            return _ProbeProvider(runtime_settings, label=label)

        return PipelineDependencies.isolated(
            settings=runtime_settings,
            backend_factory=declare_backend_factory(
                lambda: [],
                runtime_settings=runtime_settings,
                component=f"provider_atomicity_split_{label}_backends",
            ),
            history_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component=f"provider_atomicity_split_{label}_history",
                    runtime_settings=runtime_settings,
                    database_role="history",
                    database_path=runtime_settings.history_db_path,
                ),
                factory_kind="store:history",
            ),
            feedback_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component=f"provider_atomicity_split_{label}_feedback",
                    runtime_settings=runtime_settings,
                    database_role="feedback",
                    database_path=runtime_settings.feedback_db_path,
                ),
                factory_kind="store:feedback",
            ),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
            llm_provider_factory=declare_runtime_factory(
                provider_factory,
                ownership=runtime_descriptor_for_provider(
                    component=f"provider_atomicity_split_{label}_provider",
                    runtime_settings=runtime_settings,
                    capability="llm",
                ),
                factory_kind="provider:llm",
            ),
        )

    first = dependencies("first")
    second = dependencies("second")

    with pytest.raises(RuntimeOwnershipError, match="provider lifecycle owner"):
        replace(
            first,
            resource_cleanup=second.resource_cleanup,
        )

    assert provider_calls == {"first": 0, "second": 0}
    assert first.pipeline_admission is not None
    assert second.pipeline_admission is not None
    assert first.pipeline_admission.service_owner_in_flight == 0
    assert second.pipeline_admission.service_owner_in_flight == 0
