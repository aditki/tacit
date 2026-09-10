from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any

import pytest

import tacit.dependencies as dependencies_module
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.config import Settings
from tacit.dependencies import _RuntimeProviderResources, build_pipeline_dependencies
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController, RuntimeRootOwnerHandle
from tacit.runtime_ownership import (
    declare_runtime_factory,
    runtime_descriptor_for_provider,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStores


class _ImmediateProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="provider_handoff_matrix")
        self.calls = 0

    async def chat_json(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self.calls += 1
        return LLMResult("{}")

    async def chat_text(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self.calls += 1
        return LLMResult("ok")

    async def close(self) -> None:
        return None


class _CancellationResistantProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="provider_handoff_cancellation_matrix")
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    async def chat_json(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        return await self.chat_text()

    async def chat_text(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self.started.set()
        try:
            while not self.release.is_set():
                await asyncio.sleep(0.001)
            return LLMResult("settled")
        finally:
            self.finished.set()

    async def close(self) -> None:
        self.release.set()


class _CallerLoop:
    """One foreign request loop with deterministic startup and teardown."""

    def __init__(self, name: str) -> None:
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()
        assert self._ready.wait(timeout=1.0)

    def _run(self) -> None:
        with asyncio.Runner() as runner:
            self._loop = runner.get_loop()
            self._ready.set()
            self._loop.run_forever()

    def submit(self, provider: LLMProvider, index: int) -> Future[LLMResult]:
        loop = self._loop
        assert loop is not None
        return asyncio.run_coroutine_threadsafe(
            provider.chat_text("system", f"request-{index}"),
            loop,
        )

    def close(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        self._thread.join(timeout=1.0)
        assert self._thread.is_alive() is False


class _TrackedThreadFuture(Future[Any]):
    """Count operation handoff futures without retaining their payloads."""

    _lock = threading.Lock()
    _created = 0
    _live = 0

    def __init__(self) -> None:
        super().__init__()
        with self._lock:
            type(self)._created += 1
            type(self)._live += 1
        self.add_done_callback(self._finished)

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls._created = 0
            cls._live = 0

    @classmethod
    def snapshot(cls) -> tuple[int, int]:
        with cls._lock:
            return cls._created, cls._live

    @classmethod
    def _finished(cls, _future: Future[Any]) -> None:
        with cls._lock:
            cls._live -= 1


def _runtime(
    tmp_path: Any,
    *,
    active_limit: int,
    queue_limit: int,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController, _ImmediateProvider]:
    setting_values: dict[str, Any] = {
        "_env_file": None,
        "history_db_path": str(tmp_path / "history.db"),
        "feedback_db_path": str(tmp_path / "feedback.db"),
        "signals_db_path": str(tmp_path / "signals.db"),
        "llm_provider": "ollama",
        "llm_api_base": "http://127.0.0.1:11434",
        "context_provider": "none",
        "pipeline_max_concurrent": active_limit,
        "pipeline_max_queued": queue_limit,
    }
    settings = Settings(**setting_values)
    controller = PipelineAdmissionController(
        active_limit,
        max_queued=queue_limit,
    )
    controller.bind_runtime_identity(
        runtime_descriptor_from_settings(
            settings,
            component="provider_handoff_admission_matrix",
        ).admission_namespace
        or "provider-handoff-admission-matrix"
    )
    product = _ImmediateProvider(settings)
    factory = declare_runtime_factory(
        lambda: product,
        ownership=runtime_descriptor_for_provider(
            component="provider_handoff_admission_matrix",
            runtime_settings=settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    resources = _RuntimeProviderResources(
        settings,
        lifecycle=controller,
        llm_factory=factory,
        cleanup_grace_seconds=0.1,
    )
    assert resources.llm() is product
    return resources, controller, product


def _managed_runtime(
    tmp_path: Any,
    *,
    suffix: str,
) -> tuple[
    _RuntimeProviderResources,
    PipelineAdmissionController,
    _ImmediateProvider,
    RuntimeRootOwnerHandle,
]:
    settings = Settings(
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
    controller = PipelineAdmissionController(1, max_queued=0)
    controller.bind_runtime_identity(
        runtime_descriptor_from_settings(
            settings,
            component=f"provider_handoff_owner_loss_{suffix}",
        ).admission_namespace
        or f"provider-handoff-owner-loss-{suffix}"
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    products: list[_ImmediateProvider] = []

    def provider_factory() -> _ImmediateProvider:
        product = _ImmediateProvider(settings)
        products.append(product)
        return product

    factory = declare_runtime_factory(
        provider_factory,
        ownership=runtime_descriptor_for_provider(
            component=f"provider_handoff_owner_loss_{suffix}",
            runtime_settings=settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    resources = _RuntimeProviderResources(
        settings,
        lifecycle=controller,
        llm_factory=factory,
        cleanup_grace_seconds=0.1,
    )
    product = resources.llm()
    assert isinstance(product, _ImmediateProvider)
    assert products == [product]
    return resources, controller, product, root


def _wait_until(predicate: Any, *, timeout: float = 0.75) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.002)
    return bool(predicate())


def _completed_overload_reasons(futures: list[Future[LLMResult]]) -> list[str]:
    reasons: list[str] = []
    for future in futures:
        if not future.done() or future.cancelled():
            continue
        error = future.exception()
        if isinstance(error, PipelineAdmissionRejected):
            reasons.append(error.reason_code)
    return reasons


def _settle_outcomes(
    futures: list[Future[LLMResult]],
    *,
    timeout: float = 2.0,
) -> list[tuple[str, str]]:
    deadline = time.monotonic() + timeout
    outcomes: list[tuple[str, str]] = []
    for future in futures:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            future.result(timeout=remaining)
        except FutureCancelledError:
            outcomes.append(("cancelled", ""))
        except PipelineAdmissionRejected as exc:
            outcomes.append(("overload", exc.reason_code))
        except FutureTimeoutError:
            future.cancel()
            outcomes.append(("timeout", ""))
        except BaseException as exc:  # pragma: no cover - retained for useful failure output
            outcomes.append(("error", type(exc).__name__))
        else:
            outcomes.append(("success", ""))
    return outcomes


def _terminal_state(
    resources: _RuntimeProviderResources,
    controller: PipelineAdmissionController,
) -> dict[str, int]:
    state = resources._generation_owner
    with resources._lock:
        active_operations = 0 if state is None else len(state.active_operations)
        active_handoffs = 0 if state is None else len(state.active_handoffs)
    _created, live_futures = _TrackedThreadFuture.snapshot()
    return {
        "operation_ids": active_operations,
        "handoff_permits": active_handoffs,
        "handoff_futures": live_futures,
        "active": controller.in_flight,
        "queued": controller.queued,
        "retained": controller.retained,
        "blocking": controller.blocking_in_flight,
        "service_owner": controller.service_owner_in_flight,
    }


def _exercise_blocked_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    *,
    active_limit: int,
    queue_limit: int,
    operation_count: int,
    cancel_count: int = 0,
) -> dict[str, Any]:
    resources, controller, provider = _runtime(
        tmp_path,
        active_limit=active_limit,
        queue_limit=queue_limit,
    )
    state = resources._generation_owner
    assert state is not None and state.loop is not None

    owner_blocked = threading.Event()
    release_owner = threading.Event()

    def block_owner_loop() -> None:
        owner_blocked.set()
        release_owner.wait(timeout=2.0)

    state.loop.call_soon_threadsafe(block_owner_loop)
    assert owner_blocked.wait(timeout=1.0)

    _TrackedThreadFuture.reset()
    monkeypatch.setattr(dependencies_module, "ThreadFuture", _TrackedThreadFuture)

    callback_lock = threading.Lock()
    callback_submissions = 0
    original_call_soon_threadsafe = state.loop.call_soon_threadsafe

    def tracked_call_soon_threadsafe(callback: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal callback_submissions
        with callback_lock:
            callback_submissions += 1
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(state.loop, "call_soon_threadsafe", tracked_call_soon_threadsafe)

    caller_loops = [_CallerLoop("provider-handoff-caller-a"), _CallerLoop("provider-handoff-caller-b")]
    futures: list[Future[LLMResult]] = []
    outcomes: list[tuple[str, str]] = []
    pre_release: dict[str, Any] = {}
    post_cancel: dict[str, Any] = {}
    pre_close: dict[str, int] = {}
    expected_overloads = operation_count - active_limit - queue_limit
    try:
        futures = [caller_loops[index % len(caller_loops)].submit(provider, index) for index in range(operation_count)]

        def pre_release_terminal() -> bool:
            with callback_lock:
                submitted = callback_submissions
            return (
                len(_completed_overload_reasons(futures)) >= expected_overloads
                or submitted > active_limit + queue_limit
            )

        _wait_until(pre_release_terminal)
        with resources._lock:
            operation_ids = len(state.active_operations)
        created_futures, live_futures = _TrackedThreadFuture.snapshot()
        with callback_lock:
            submitted_callbacks = callback_submissions
        pre_release = {
            "operation_ids": operation_ids,
            "created_futures": created_futures,
            "live_futures": live_futures,
            "callback_submissions": submitted_callbacks,
            "overload_reasons": _completed_overload_reasons(futures),
        }

        if cancel_count:
            pending = [future for future in futures if not future.done()]
            assert len(pending) >= cancel_count
            for future in pending[:cancel_count]:
                assert future.cancel() is True
            assert _wait_until(lambda: sum(future.cancelled() for future in futures) == cancel_count)
            with resources._lock:
                operation_ids = len(state.active_operations)
            _created, live_futures = _TrackedThreadFuture.snapshot()
            with callback_lock:
                submitted_callbacks = callback_submissions
            post_cancel = {
                "operation_ids": operation_ids,
                "live_futures": live_futures,
                "callback_submissions": submitted_callbacks,
            }
    finally:
        release_owner.set()
        if futures:
            outcomes = _settle_outcomes(futures)
        with resources._lock:
            pre_close = {
                "operation_ids": len(state.active_operations),
                "handoff_permits": len(state.active_handoffs),
                "committed_submissions": len(state.committed_submissions),
            }
        try:
            asyncio.run(resources.close())
        finally:
            for caller_loop in caller_loops:
                caller_loop.close()

    assert _wait_until(
        lambda: all(value == 0 for value in _terminal_state(resources, controller).values()),
        timeout=1.0,
    )
    return {
        "budget": active_limit + queue_limit,
        "expected_overloads": expected_overloads,
        "provider_calls": provider.calls,
        "pre_release": pre_release,
        "post_cancel": post_cancel,
        "pre_close": pre_close,
        "outcomes": outcomes,
        "terminal": _terminal_state(resources, controller),
    }


def _assert_pre_submission_bound(result: dict[str, Any]) -> None:
    budget = result["budget"]
    pre_release = result["pre_release"]
    assert pre_release["operation_ids"] <= budget
    assert pre_release["created_futures"] <= budget
    assert pre_release["live_futures"] <= budget
    assert pre_release["callback_submissions"] <= budget
    assert pre_release["overload_reasons"] == ["pipeline_admission_queue_full"] * result["expected_overloads"]
    assert result["pre_close"] == {
        "operation_ids": 0,
        "handoff_permits": 0,
        "committed_submissions": 0,
    }


def test_cross_loop_handoff_rejects_limit_plus_one_before_owner_callback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    result = _exercise_blocked_owner(
        monkeypatch,
        tmp_path,
        active_limit=2,
        queue_limit=2,
        operation_count=5,
    )

    _assert_pre_submission_bound(result)
    assert result["outcomes"].count(("success", "")) == 4
    assert result["outcomes"].count(("overload", "pipeline_admission_queue_full")) == 1
    assert result["terminal"] == {
        "operation_ids": 0,
        "handoff_permits": 0,
        "handoff_futures": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


def test_cross_loop_handoff_cancellation_preserves_the_aggregate_bound(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    result = _exercise_blocked_owner(
        monkeypatch,
        tmp_path,
        active_limit=2,
        queue_limit=2,
        operation_count=5,
        cancel_count=2,
    )

    _assert_pre_submission_bound(result)
    assert result["post_cancel"]["operation_ids"] <= result["budget"]
    assert result["post_cancel"]["live_futures"] <= result["budget"]
    assert result["post_cancel"]["callback_submissions"] <= result["budget"]
    assert result["outcomes"].count(("cancelled", "")) == 2
    assert result["outcomes"].count(("overload", "pipeline_admission_queue_full")) == 1
    assert all(value == 0 for value in result["terminal"].values())


@pytest.mark.asyncio
async def test_cancelled_inherited_provider_operation_retains_aggregate_admission_until_settlement(
    tmp_path: Any,
) -> None:
    """Caller cancellation cannot turn queue capacity into live provider work."""
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=1,
        pipeline_max_queued=1,
        pipeline_wait_timeout_seconds=0.02,
    )
    stores = RuntimeStores(runtime_settings)
    controller = stores.pipeline_admission()
    products: list[_CancellationResistantProvider] = []

    def create_provider() -> _CancellationResistantProvider:
        product = _CancellationResistantProvider(runtime_settings)
        products.append(product)
        return product

    factory = declare_runtime_factory(
        create_provider,
        ownership=runtime_descriptor_for_provider(
            component="provider_handoff_cancellation_matrix",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=stores,
        llm_provider_factory=factory,
    )
    assert dependencies.llm_provider_factory is not None
    provider = dependencies.llm_provider_factory()
    assert len(products) == 1
    product = products[0]

    try:
        async with controller.slot():
            operation = asyncio.create_task(provider.chat_text("system", "cancelled"))
            assert await asyncio.to_thread(product.started.wait, 1.0)
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation

        assert controller.in_flight == 1
        assert controller.retained == 1
        with pytest.raises(PipelineAdmissionRejected) as rejected:
            await controller.acquire(timeout_seconds=0.02)
        assert rejected.value.reason_code == "pipeline_admission_wait_timeout"

        product.release.set()
        assert await asyncio.to_thread(product.finished.wait, 1.0)
        assert await asyncio.to_thread(_wait_until, lambda: controller.in_flight == 0)

        async with controller.slot(timeout_seconds=0.02):
            assert controller.in_flight == 1
    finally:
        product.release.set()
        if dependencies.resource_cleanup is not None:
            await dependencies.resource_cleanup()

    assert controller.in_flight == 0
    assert controller.retained == 0


def test_cross_loop_handoff_large_burst_is_bounded_by_active_plus_queue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    result = _exercise_blocked_owner(
        monkeypatch,
        tmp_path,
        active_limit=2,
        queue_limit=3,
        operation_count=32,
    )

    _assert_pre_submission_bound(result)
    assert result["outcomes"].count(("success", "")) == 5
    assert result["outcomes"].count(("overload", "pipeline_admission_queue_full")) == 27
    assert all(value == 0 for value in result["terminal"].values())


@pytest.mark.parametrize("attempt", range(5))
def test_inherited_request_handoff_rejects_limit_plus_one_before_owner_callback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    attempt: int,
) -> None:
    active_limit = 2
    queue_limit = 2
    handoff_limit = active_limit + queue_limit
    resources, controller, provider = _runtime(
        tmp_path,
        active_limit=active_limit,
        queue_limit=queue_limit,
    )
    state = resources._generation_owner
    assert state is not None and state.loop is not None

    owner_blocked = threading.Event()
    release_owner = threading.Event()

    def block_owner_loop() -> None:
        owner_blocked.set()
        release_owner.wait(timeout=2.0)

    state.loop.call_soon_threadsafe(block_owner_loop)
    assert owner_blocked.wait(timeout=1.0)

    _TrackedThreadFuture.reset()
    monkeypatch.setattr(dependencies_module, "ThreadFuture", _TrackedThreadFuture)
    callback_lock = threading.Lock()
    callback_submissions = 0
    original_call_soon_threadsafe = state.loop.call_soon_threadsafe

    def tracked_call_soon_threadsafe(callback: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal callback_submissions
        if getattr(callback, "__name__", "") == "submit_operation":
            with callback_lock:
                callback_submissions += 1
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(state.loop, "call_soon_threadsafe", tracked_call_soon_threadsafe)

    async def exercise_handoffs() -> tuple[list[Any], dict[str, int], dict[str, int]]:
        operations: list[asyncio.Task[LLMResult]] = []
        results: list[Any] = []
        snapshot: dict[str, int] = {}
        settled_snapshot: dict[str, int] = {}
        try:
            async with controller.slot():
                operations = [
                    asyncio.create_task(provider.chat_text("system", f"inherited-{attempt}-{index}"))
                    for index in range(handoff_limit + 1)
                ]
                for _ in range(1_000):
                    with resources._lock:
                        operation_ids = len(state.active_operations)
                        committed_submissions = len(state.committed_submissions)
                    if operation_ids >= handoff_limit and (
                        operation_ids > handoff_limit or any(operation.done() for operation in operations)
                    ):
                        break
                    await asyncio.sleep(0)
                created_futures, live_futures = _TrackedThreadFuture.snapshot()
                with callback_lock:
                    submitted_callbacks = callback_submissions
                snapshot = {
                    "operation_ids": operation_ids,
                    "committed_submissions": committed_submissions,
                    "created_futures": created_futures,
                    "live_futures": live_futures,
                    "callback_submissions": submitted_callbacks,
                }
                release_owner.set()
                results = await asyncio.gather(*operations, return_exceptions=True)
                with resources._lock:
                    settled_snapshot = {
                        "operation_ids": len(state.active_operations),
                        "handoff_permits": len(state.active_handoffs),
                        "committed_submissions": len(state.committed_submissions),
                    }
        finally:
            release_owner.set()
            if operations:
                await asyncio.gather(*operations, return_exceptions=True)
            await resources.close()
        return results, snapshot, settled_snapshot

    results, snapshot, settled_snapshot = asyncio.run(exercise_handoffs())

    rejections = [result for result in results if isinstance(result, PipelineAdmissionRejected)]
    assert snapshot == {
        "operation_ids": handoff_limit,
        "committed_submissions": handoff_limit,
        "created_futures": handoff_limit,
        "live_futures": handoff_limit,
        "callback_submissions": handoff_limit,
    }
    assert len(rejections) == 1
    assert rejections[0].reason_code == "pipeline_admission_queue_full"
    assert len([result for result in results if isinstance(result, LLMResult)]) == handoff_limit
    assert settled_snapshot == {
        "operation_ids": 0,
        "handoff_permits": 0,
        "committed_submissions": 0,
    }
    assert all(value == 0 for value in _terminal_state(resources, controller).values())


@pytest.mark.parametrize("attempt", range(5))
def test_handoff_future_allocation_failure_rolls_back_generation_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    attempt: int,
) -> None:
    resources, controller, provider = _runtime(
        tmp_path,
        active_limit=1,
        queue_limit=0,
    )
    state = resources._generation_owner
    assert state is not None

    class SyntheticAllocationError(RuntimeError):
        pass

    class RaisingThreadFuture:
        def __init__(self) -> None:
            raise SyntheticAllocationError("synthetic handoff future allocation failure")

    original_thread_future = dependencies_module.ThreadFuture
    monkeypatch.setattr(dependencies_module, "ThreadFuture", RaisingThreadFuture)

    async def invoke_failure() -> None:
        with pytest.raises(SyntheticAllocationError):
            await provider.chat_text("system", f"allocation-failure-{attempt}")

    asyncio.run(invoke_failure())
    with resources._lock:
        leaked_handoffs = tuple(state.active_handoffs)
        leaked_operations = tuple(state.active_operations)
        snapshot = {
            "handoff_permits": len(leaked_handoffs),
            "operation_ids": len(leaked_operations),
            "committed_submissions": len(state.committed_submissions),
            "active": controller.in_flight,
            "queued": controller.queued,
        }

    # Keep the intentionally failing pre-fix run hermetic so the matrix also
    # proves that the repaired runtime can still drain to its real root zero.
    for handoff_id in leaked_handoffs:
        resources._finish_generation_handoff(state, handoff_id)
    for operation_id in leaked_operations:
        resources._finish_generation_operation(state, operation_id)
    monkeypatch.setattr(dependencies_module, "ThreadFuture", original_thread_future)
    asyncio.run(resources.close())

    assert snapshot == {
        "handoff_permits": 0,
        "operation_ids": 0,
        "committed_submissions": 0,
        "active": 0,
        "queued": 0,
    }
    assert all(value == 0 for value in _terminal_state(resources, controller).values())


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", range(5))
async def test_owner_loop_loss_settles_committed_handoff_once_before_final_drain(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
    attempt: int,
) -> None:
    resources, controller, provider, root = _managed_runtime(
        tmp_path,
        suffix=f"committed-handoff-{attempt}",
    )
    state = resources._generation_owner
    assert state is not None and state.loop is not None

    admitted_handoffs: list[Any] = []
    admission_type = dependencies_module._ProviderOperationAdmission

    def capture_admission(*args: Any, **kwargs: Any) -> Any:
        admission = admission_type(*args, **kwargs)
        if admission.lifecycle is not None:
            admitted_handoffs.append(admission)
        return admission

    monkeypatch.setattr(
        dependencies_module,
        "_ProviderOperationAdmission",
        capture_admission,
    )
    released_tokens: list[int] = []
    original_release = controller.release

    def track_release(lease: Any) -> None:
        released_tokens.append(lease.token)
        original_release(lease)

    monkeypatch.setattr(controller, "release", track_release)

    handoff_committed = threading.Event()
    original_call_soon_threadsafe = state.loop.call_soon_threadsafe

    def accept_without_dispatch(callback: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(callback, "__name__", "") == "submit_operation":
            handoff_committed.set()
            # Fault injection: the loop accepts the wakeup, loses the provider
            # callback before task creation, and then becomes unavailable.
            return original_call_soon_threadsafe(state.loop.stop)
        return original_call_soon_threadsafe(callback, *args, **kwargs)

    monkeypatch.setattr(state.loop, "call_soon_threadsafe", accept_without_dispatch)
    operation = asyncio.create_task(provider.chat_text("system", "owner-loop-loss"))
    assert await asyncio.to_thread(handoff_committed.wait, 1.0)
    assert await asyncio.to_thread(state.done.wait, 1.0)
    await asyncio.sleep(0)
    assert len(admitted_handoffs) == 1
    admission = admitted_handoffs[0]
    with resources._lock:
        committed_submissions = getattr(state, "committed_submissions", None)
        operation_count = len(state.active_operations)
        handoff_count = len(state.active_handoffs)
    recovered_before_repair = (
        operation.done()
        and admission.released
        and committed_submissions == {}
        and operation_count == 0
        and handoff_count == 0
        and controller.in_flight == 0
    )

    # Keep the intentionally failing pre-fix run hermetic. This branch is not
    # reached by the fixed implementation, but releases the captured lease so
    # the managed root can still prove its terminal state.
    if not admission.released:
        admission.finish()
    if not operation.done():
        operation.cancel()
    await asyncio.gather(operation, return_exceptions=True)

    root_error: BaseException | None = None
    try:
        await asyncio.wait_for(
            controller.execution_graph.release_root_owner(root),
            timeout=2.0,
        )
    except BaseException as exc:
        root_error = exc

    assert recovered_before_repair is True
    assert isinstance(root_error, RuntimeOwnershipError)
    assert len(released_tokens) == 1
    assert controller.execution_graph.root_owner_count == 0
    assert controller.execution_graph.root_state == "closed"
    assert controller.runtime_root_state == "closed"
    assert _terminal_state(resources, controller) == {
        "operation_ids": 0,
        "handoff_permits": 0,
        "handoff_futures": 0,
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }
