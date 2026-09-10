from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

import tacit.pipeline.side_effects as side_effects
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.config import Settings
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork
from tacit.pipeline_admission import PipelineAdmissionController


class _AsyncCloseProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="rejected_cleanup_matrix_provider")
        self.close_calls = 0
        self.closed = False
        self.close_thread_ids: list[int] = []
        self.close_loop_ids: list[int] = []

    async def chat_json(self, *_args, **_kwargs) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, *_args, **_kwargs) -> LLMResult:
        return LLMResult("")

    async def close(self) -> None:
        self.close_calls += 1
        self.close_thread_ids.append(threading.get_ident())
        self.close_loop_ids.append(id(asyncio.get_running_loop()))
        await asyncio.sleep(0)
        self.closed = True


def _settings(tmp_path, *, suffix: str, api_base: str) -> Settings:
    return Settings(
        _env_file=None,
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        llm_provider="ollama",
        llm_api_base=api_base,
    )


def _lifecycle_thread_ids() -> set[int | None]:
    return {thread.ident for thread in threading.enumerate() if thread.name.startswith("tacit-lifecycle-")}


def _wait_for_cleanup(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        threading.Event().wait(0.001)


@pytest.mark.asyncio
async def test_active_loop_rejected_provider_reserves_worker_before_realization(
    tmp_path,
) -> None:
    settings = _settings(
        tmp_path,
        suffix="admission-rejected",
        api_base="http://127.0.0.1:11434",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    products: list[_AsyncCloseProvider] = []
    active_lease = await lifecycle.acquire()

    def realize() -> _AsyncCloseProvider:
        product = _AsyncCloseProvider(settings)
        products.append(product)
        return product

    try:
        with pytest.raises(PipelineAdmissionRejected):
            blocking_work.realize_owned_sync(
                realize,
                validate=lambda _product: None,
                retire=lambda product: product.close(),
                reason_code="rejected_provider_admission",
            )
    finally:
        lifecycle.release(active_lease)

    assert products == []
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.parametrize("start_failure", ["definite", "ambiguous"])
@pytest.mark.asyncio
async def test_active_loop_worker_start_failure_prevents_product_realization(
    monkeypatch,
    tmp_path,
    start_failure: str,
) -> None:
    settings = _settings(
        tmp_path,
        suffix=f"start-{start_failure}",
        api_base="http://127.0.0.1:11434",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    products: list[_AsyncCloseProvider] = []
    original_start = threading.Thread.start
    started_threads: list[threading.Thread] = []

    def fail_cleanup_worker_start(thread: threading.Thread) -> None:
        if thread.name.startswith("tacit-lifecycle-"):
            if start_failure == "ambiguous":
                started_threads.append(thread)
                original_start(thread)
            raise RuntimeError("synthetic cleanup worker start failure")
        original_start(thread)

    def realize() -> _AsyncCloseProvider:
        product = _AsyncCloseProvider(settings)
        products.append(product)
        return product

    monkeypatch.setattr(threading.Thread, "start", fail_cleanup_worker_start)
    threads_before = _lifecycle_thread_ids()

    with pytest.raises(RuntimeError, match="blocking worker could not start"):
        blocking_work.realize_owned_sync(
            realize,
            validate=lambda _product: None,
            retire=lambda product: product.close(),
            reason_code="rejected_provider_start_failure",
        )

    for thread in started_threads:
        thread.join(timeout=1.0)

    assert products == []
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0
    assert _lifecycle_thread_ids() == threads_before


@pytest.mark.asyncio
async def test_sync_realization_adopts_product_inside_active_event_loop(
    tmp_path,
) -> None:
    settings = _settings(
        tmp_path,
        suffix="sync-active-loop",
        api_base="http://127.0.0.1:11434",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    factory_thread_ids: list[int] = []
    caller_thread_id = threading.get_ident()

    def realize() -> _AsyncCloseProvider:
        factory_thread_ids.append(threading.get_ident())
        return _AsyncCloseProvider(settings)

    product = blocking_work.realize_owned_sync(
        realize,
        validate=lambda _product: None,
        retire=lambda rejected: rejected.close(),
        reason_code="sync_provider_realization_active_loop",
    )

    assert factory_thread_ids != [caller_thread_id]
    assert product.closed is False
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_sync_validation_rejection_closes_async_product_on_worker_loop(
    tmp_path,
) -> None:
    settings = _settings(
        tmp_path,
        suffix="sync-validation-rejected",
        api_base="http://127.0.0.1:11434",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    products: list[_AsyncCloseProvider] = []
    caller_thread_id = threading.get_ident()
    caller_loop_id = id(asyncio.get_running_loop())

    def realize() -> _AsyncCloseProvider:
        product = _AsyncCloseProvider(settings)
        products.append(product)
        return product

    def reject(_product: _AsyncCloseProvider) -> None:
        raise RuntimeOwnershipError("synthetic sync ownership rejection")

    with pytest.raises(RuntimeOwnershipError, match="synthetic sync ownership rejection"):
        blocking_work.realize_owned_sync(
            realize,
            validate=reject,
            retire=lambda product: product.close(),
            reason_code="sync_rejected_provider_validation",
        )

    assert len(products) == 1
    assert products[0].close_calls == 1
    assert products[0].closed is True
    assert products[0].close_thread_ids != [caller_thread_id]
    assert products[0].close_loop_ids != [caller_loop_id]
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_validation_rejection_retires_product_on_worker_local_loop(
    tmp_path,
) -> None:
    settings = _settings(
        tmp_path,
        suffix="validation-rejected",
        api_base="http://127.0.0.1:11434",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    products: list[_AsyncCloseProvider] = []
    caller_thread_id = threading.get_ident()
    caller_loop_id = id(asyncio.get_running_loop())

    def realize() -> _AsyncCloseProvider:
        product = _AsyncCloseProvider(settings)
        products.append(product)
        return product

    def reject(_product: _AsyncCloseProvider) -> None:
        raise RuntimeOwnershipError("synthetic ownership rejection")

    with pytest.raises(RuntimeOwnershipError, match="synthetic ownership rejection"):
        await blocking_work.realize_owned(
            realize,
            validate=reject,
            retire=lambda product: product.close(),
            reason_code="rejected_provider_validation",
        )

    assert len(products) == 1
    assert products[0].close_calls == 1
    assert products[0].closed is True
    assert products[0].close_thread_ids != [caller_thread_id]
    assert products[0].close_loop_ids != [caller_loop_id]
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_abandoned_realized_product_is_retired_on_worker_local_loop(
    tmp_path,
) -> None:
    settings = _settings(
        tmp_path,
        suffix="abandoned",
        api_base="http://127.0.0.1:11434",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    products: list[_AsyncCloseProvider] = []
    realization_started = threading.Event()
    release_realization = threading.Event()

    def realize() -> _AsyncCloseProvider:
        realization_started.set()
        assert release_realization.wait(timeout=2.0)
        product = _AsyncCloseProvider(settings)
        products.append(product)
        return product

    task = asyncio.create_task(
        blocking_work.realize_owned(
            realize,
            validate=lambda _product: None,
            retire=lambda product: product.close(),
            reason_code="abandoned_provider_realization",
        )
    )
    assert await asyncio.to_thread(realization_started.wait, 0.5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    release_realization.set()

    _wait_for_cleanup(
        lambda: bool(products) and products[0].closed and lifecycle.in_flight == 0 and blocking_work.active == 0,
    )
    assert len(products) == 1
    assert products[0].close_calls == 1
    assert products[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.parametrize(
    "construction_failure",
    ["call_before_first", "registration_mid_group", "thread_mid_group"],
)
def test_reserved_cleanup_group_construction_failure_returns_every_member_without_running_it(
    monkeypatch,
    construction_failure: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(3)
    assert permits is not None
    cleanup_calls = [0, 0, 0]
    cleanup_functions: tuple[Callable[[], None], ...] = tuple(
        lambda index=index: cleanup_calls.__setitem__(index, cleanup_calls[index] + 1) for index in range(3)
    )
    threads_before = _lifecycle_thread_ids()

    if construction_failure == "call_before_first":
        original_call = side_effects._LifecycleBlockingCall
        call_constructions = 0

        def fail_first_call_construction(*args: Any, **kwargs: Any):
            nonlocal call_constructions
            call_constructions += 1
            if call_constructions == 1:
                raise RuntimeError("synthetic blocking-call construction failure")
            return original_call(*args, **kwargs)

        monkeypatch.setattr(side_effects, "_LifecycleBlockingCall", fail_first_call_construction)
    elif construction_failure == "registration_mid_group":
        original_register = blocking_work._register
        registrations = 0

        def fail_second_registration(call: Any) -> None:
            nonlocal registrations
            registrations += 1
            if registrations == 2:
                raise RuntimeError("synthetic registration failure")
            original_register(call)

        monkeypatch.setattr(blocking_work, "_register", fail_second_registration)
    else:
        original_thread = threading.Thread
        thread_constructions = 0

        def fail_second_thread_construction(*args: Any, **kwargs: Any):
            nonlocal thread_constructions
            thread_constructions += 1
            if thread_constructions == 2:
                raise RuntimeError("synthetic thread construction failure")
            return original_thread(*args, **kwargs)

        monkeypatch.setattr(threading, "Thread", fail_second_thread_construction)

    try:
        try:
            blocking_work.run_reserved_background_group(
                cleanup_functions,
                permits,
                reason_code="reserved_cleanup_construction_failure",
            )
        except RuntimeError:
            pass

        assert cleanup_calls == [0, 0, 0]
        assert blocking_work.active == 0
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert _lifecycle_thread_ids() == threads_before
    finally:
        for permit in permits:
            try:
                lifecycle.release_blocking_permit(permit)
            except RuntimeError:
                pass


def test_ambiguous_thread_start_aborts_group_without_replaying_thread_run(
    monkeypatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(1)
    assert permits is not None
    cleanup_calls = 0
    inline_thread_run_calls = 0
    internal_thread_errors: list[BaseException] = []
    started_threads: list[threading.Thread] = []
    original_start = threading.Thread.start
    original_run = threading.Thread.run

    def cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1

    def track_thread_run(thread: threading.Thread) -> None:
        nonlocal inline_thread_run_calls
        if threading.current_thread() is not thread:
            inline_thread_run_calls += 1
        original_run(thread)

    def start_then_raise(thread: threading.Thread) -> None:
        if not thread.name.startswith("tacit-lifecycle-"):
            original_start(thread)
            return
        started_threads.append(thread)
        original_start(thread)
        raise RuntimeError("synthetic ambiguous thread start")

    def capture_thread_error(args: threading.ExceptHookArgs) -> None:
        internal_thread_errors.append(args.exc_value)

    monkeypatch.setattr(threading.Thread, "run", track_thread_run)
    monkeypatch.setattr(threading.Thread, "start", start_then_raise)
    monkeypatch.setattr(threading, "excepthook", capture_thread_error)

    started = blocking_work.run_reserved_background_group(
        (cleanup,),
        permits,
        reason_code="ambiguous_cleanup_worker_start",
    )
    for thread in started_threads:
        thread.join(timeout=1.0)

    assert started is False
    assert inline_thread_run_calls == 0
    assert internal_thread_errors == []
    assert cleanup_calls == 0
    assert all(not thread.is_alive() for thread in started_threads)
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


def test_partial_group_start_failure_aborts_every_worker_before_cleanup(
    monkeypatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(3)
    assert permits is not None
    cleanup_calls = [0, 0, 0]
    started_threads: list[threading.Thread] = []
    original_start = threading.Thread.start
    start_calls = 0

    def start_until_second(thread: threading.Thread) -> None:
        nonlocal start_calls
        if not thread.name.startswith("tacit-lifecycle-"):
            original_start(thread)
            return
        start_calls += 1
        if start_calls == 2:
            raise RuntimeError("synthetic partial group start failure")
        started_threads.append(thread)
        original_start(thread)

    cleanup_functions: tuple[Callable[[], None], ...] = tuple(
        lambda index=index: cleanup_calls.__setitem__(index, cleanup_calls[index] + 1) for index in range(3)
    )
    monkeypatch.setattr(threading.Thread, "start", start_until_second)

    started = blocking_work.run_reserved_background_group(
        cleanup_functions,
        permits,
        reason_code="partial_cleanup_group_start",
    )
    for thread in started_threads:
        thread.join(timeout=1.0)

    assert started is False
    assert cleanup_calls == [0, 0, 0]
    assert all(not thread.is_alive() for thread in started_threads)
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0
