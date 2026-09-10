"""Provider shutdown must not borrow the caller loop's default executor."""

from __future__ import annotations

import asyncio
import subprocess
import sys
import threading
import time
from concurrent.futures import Future as ThreadFuture
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

import tacit.pipeline_admission as pipeline_admission_module
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.config import Settings
from tacit.dependencies import ProviderLifecycleState, _RuntimeProviderResources, declare_runtime_factory
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController
from tacit.runtime_ownership import runtime_descriptor_for_provider

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class _ShutdownProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="provider_shutdown_executor_test")

    async def chat_json(self, *_args: object, **_kwargs: object) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, *_args: object, **_kwargs: object) -> LLMResult:
        return LLMResult("ok")


def _provider_manager(
    tmp_path,
    *,
    close_started: threading.Event | None = None,
    release_close: threading.Event | None = None,
    cleanup_grace_seconds: float = 0.2,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController]:
    runtime_settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    declared = declare_runtime_factory(
        lambda: _ShutdownProvider(runtime_settings),
        ownership=runtime_descriptor_for_provider(
            component="provider_shutdown_executor_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )

    async def chained_cleanup() -> None:
        if close_started is not None:
            close_started.set()
        if release_close is not None:
            release_close.wait(timeout=2)

    manager = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=lifecycle,
        llm_factory=declared,
        chained_cleanup=chained_cleanup,
        cleanup_grace_seconds=cleanup_grace_seconds,
    )
    return manager, lifecycle


async def _wait_for_thread_event(event: threading.Event, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not event.is_set() and time.monotonic() < deadline:
        await asyncio.sleep(0.001)
    assert event.is_set()


async def _wait_for_terminal_zero(
    manager: _RuntimeProviderResources,
    lifecycle: PipelineAdmissionController,
    *,
    timeout: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = manager._generation_owner or manager._retired_generation
        owner_alive = state is not None and state.owner_thread is not None and state.owner_thread.is_alive()
        if (
            manager.lifecycle_state in {ProviderLifecycleState.EMPTY, ProviderLifecycleState.REVOKED}
            and lifecycle.in_flight == 0
            and lifecycle.blocking_in_flight == 0
            and lifecycle.retained == 0
            and lifecycle.service_owner_in_flight == 0
            and not owner_alive
        ):
            return
        await asyncio.sleep(0.001)
    raise AssertionError("provider lifecycle did not reach terminal zero")


async def _occupy_default_executor(
    loop: asyncio.AbstractEventLoop,
) -> tuple[threading.Event, asyncio.Future[None]]:
    started = threading.Event()
    release = threading.Event()

    def occupy() -> None:
        started.set()
        release.wait(timeout=3)

    future = loop.run_in_executor(None, occupy)
    await _wait_for_thread_event(started)
    return release, future


@pytest.fixture(autouse=True)
def _isolated_runtime_fatal_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pipeline_admission_module,
        "_PROCESS_RUNTIME_FATAL_REGISTRY",
        pipeline_admission_module._ProcessRuntimeFatalRegistry(limit=1024),
    )


def test_provider_shutdown_does_not_depend_on_saturated_default_executor(tmp_path) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="saturated-default")
        loop.set_default_executor(executor)
        release_executor, executor_work = await _occupy_default_executor(loop)
        close_started = threading.Event()
        release_close = threading.Event()
        manager, lifecycle = _provider_manager(
            tmp_path,
            close_started=close_started,
            release_close=release_close,
        )
        try:
            close_task = asyncio.create_task(manager.close())
            await _wait_for_thread_event(close_started)
            state = manager._generation_owner
            assert state is not None
            owner_exited = state.owner_exited
            assert isinstance(owner_exited, ThreadFuture)
            assert owner_exited.done() is False

            release_close.set()
            await asyncio.wait_for(close_task, timeout=0.5)
            assert owner_exited.done() is True
            assert owner_exited.cancelled() is False
            assert owner_exited.result() is None
            await _wait_for_terminal_zero(manager, lifecycle)
            assert manager._retired_generation is None
        finally:
            release_close.set()
            release_executor.set()
            await executor_work

    asyncio.run(scenario())


def test_cancelled_shutdown_waiter_does_not_own_provider_cleanup(tmp_path) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="saturated-default")
        loop.set_default_executor(executor)
        release_executor, executor_work = await _occupy_default_executor(loop)
        close_started = threading.Event()
        release_close = threading.Event()
        manager, lifecycle = _provider_manager(
            tmp_path,
            close_started=close_started,
            release_close=release_close,
        )
        try:
            close_task = asyncio.create_task(manager.close())
            await _wait_for_thread_event(close_started)
            state = manager._generation_owner
            assert state is not None
            owner_exited = state.owner_exited
            assert isinstance(owner_exited, ThreadFuture)
            close_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await close_task
            assert owner_exited.cancelled() is False

            release_close.set()
            await _wait_for_terminal_zero(manager, lifecycle)
            assert owner_exited.done() is True
            assert owner_exited.cancelled() is False
            await asyncio.wait_for(manager.shutdown(), timeout=0.5)
            assert manager._retired_generation is None
        finally:
            release_close.set()
            release_executor.set()
            await executor_work

    asyncio.run(scenario())


def test_provider_cleanup_timeout_is_wall_clock_bounded_and_eventually_reaches_zero(tmp_path) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="saturated-default")
        loop.set_default_executor(executor)
        release_executor, executor_work = await _occupy_default_executor(loop)
        close_started = threading.Event()
        release_close = threading.Event()
        manager, lifecycle = _provider_manager(
            tmp_path,
            close_started=close_started,
            release_close=release_close,
            cleanup_grace_seconds=0.05,
        )
        try:
            close_task = asyncio.create_task(manager.close())
            await _wait_for_thread_event(close_started)
            state = manager._generation_owner
            assert state is not None
            owner_exited = state.owner_exited
            assert isinstance(owner_exited, ThreadFuture)
            started = time.monotonic()
            with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
                await asyncio.wait_for(close_task, timeout=0.4)
            assert time.monotonic() - started < 0.35
            assert owner_exited.cancelled() is False

            release_close.set()
            await _wait_for_terminal_zero(manager, lifecycle)
            assert owner_exited.done() is True
            assert owner_exited.cancelled() is False
            with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
                await asyncio.wait_for(manager.shutdown(), timeout=0.5)
            assert manager._retired_generation is None
        finally:
            release_close.set()
            release_executor.set()
            await executor_work

    asyncio.run(scenario())


def test_bedrock_loop_loss_then_provider_shutdown_has_no_resource_warnings() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-W",
            "error::ResourceWarning",
            "-W",
            "error::pytest.PytestUnraisableExceptionWarning",
            (
                "tests/unit/test_bedrock_bridge_cancellation.py::"
                "test_cancellation_during_sts_realization_closes_clients_before_releasing_capacity"
            ),
            (
                "tests/unit/test_bedrock_bridge_cancellation.py::"
                "test_cancellation_during_runtime_client_construction_closes_all_created_resources"
            ),
            (
                "tests/unit/test_bedrock_bridge_cancellation.py::"
                "test_closed_originating_loop_does_not_own_bedrock_cleanup_or_capacity"
            ),
            (
                "tests/unit/test_provider_shutdown_executor_independence.py::"
                "test_provider_shutdown_does_not_depend_on_saturated_default_executor"
            ),
        ],
        cwd=_REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
