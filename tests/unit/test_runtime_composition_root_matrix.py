"""Public composition-root lifecycle matrix.

These tests exercise the production app, Slack, and direct-pipeline entry
points.  They intentionally keep provider and vendor behavior fake so failures
identify runtime-root ownership rather than network behavior.
"""

from __future__ import annotations

import asyncio
import gc
import sys
import threading
from dataclasses import dataclass, replace
from typing import Any

import pytest

import tacit.integrations.slack as slack_module
import tacit.pipeline.runner as runner_module
from tacit.api.app import create_app
from tacit.api.lifespan import create_lifespan
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies
from tacit.errors import RuntimeOwnershipError
from tacit.history import InvestigationStore
from tacit.models.schemas import DashRequest, DashResponse
from tacit.pipeline import run_pipeline
from tacit.runtime_ownership import (
    declare_runtime_factory,
    runtime_descriptor_for_backends,
)
from tacit.runtime_stores import RuntimeStoreReadinessError, RuntimeStores

_PRIMARY_SECRET = "PRIMARY_PIPELINE_SECRET=must-not-enter-diagnostics"
_CLEANUP_SECRET = "ROOT_CLEANUP_SECRET=must-not-enter-diagnostics"


class _PrimaryPipelineFailure(RuntimeError):
    pass


class _RootCleanupFailure(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _LogRecord:
    level: str
    event: str
    fields: dict[str, Any]


class _RecordingLogger:
    def __init__(self) -> None:
        self.records: list[_LogRecord] = []

    def _record(self, level: str, event: str, **fields: Any) -> None:
        self.records.append(_LogRecord(level=level, event=event, fields=fields))

    def debug(self, event: str, **fields: Any) -> None:
        self._record("debug", event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._record("info", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._record("warning", event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._record("error", event, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        self._record("exception", event, **fields)


def _settings(
    tmp_path,
    *,
    suffix: str,
    slack: bool = False,
    concurrency: int = 4,
    max_queued: int = 0,
) -> Settings:
    return Settings(
        _env_file=None,
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=concurrency,
        pipeline_max_queued=max_queued,
        pipeline_timeout_seconds=2.0,
        slack_bot_token="xoxb-test" if slack else "",
        slack_app_token="xapp-test" if slack else "",
        slack_signing_secret="signing-test" if slack else "",
    )


def _request(index: int = 1) -> DashRequest:
    return DashRequest(
        prompt=f"checkout latency request {index}",
        user_id=f"user-{index}",
        channel_id="channel-1",
    )


def _response(index: int = 1) -> DashResponse:
    return DashResponse(
        dashboard_url=f"https://dashboards.example/{index}",
        dashboard_uid=f"dashboard-{index}",
        panel_count=1,
        summary=f"request {index} completed",
    )


def _assert_terminal_zero(stores: RuntimeStores) -> None:
    admission = stores.pipeline_admission()
    graph = admission.execution_graph
    assert graph.root_owner_count == 0
    assert graph.root_state == "closed"
    assert admission.runtime_root_state == "closed"
    assert admission.in_flight == 0
    assert admission.queued == 0
    assert admission.retained == 0
    assert admission.blocking_in_flight == 0
    assert admission.service_owner_in_flight == 0


def _isolated_dependencies(runtime_settings: Settings) -> PipelineDependencies:
    return PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=declare_runtime_factory(
            lambda: [],
            ownership=runtime_descriptor_for_backends(
                component="isolated_root_matrix_backends",
                runtime_settings=runtime_settings,
            ),
            factory_kind="backend:dashboard",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )


def _assert_isolated_terminal_zero(dependencies: PipelineDependencies) -> None:
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    assert graph.root_owner_count == 0
    assert graph.root_state == "closed"
    assert graph.provider_manager_count == 0
    assert graph.provider_manager() is None
    assert admission.runtime_root_state == "closed"
    assert admission.in_flight == 0
    assert admission.queued == 0
    assert admission.retained == 0
    assert admission.blocking_in_flight == 0
    assert admission.service_owner_in_flight == 0


def test_pipeline_dependencies_require_runtime_root_capability_pair(tmp_path) -> None:
    dependencies = _isolated_dependencies(_settings(tmp_path, suffix="root-capability-required"))

    with pytest.raises(RuntimeOwnershipError, match="runtime root lifecycle"):
        replace(
            dependencies,
            runtime_root_acquire=None,
            runtime_root_release=None,
        )

    assert dependencies.pipeline_admission is not None
    assert dependencies.pipeline_admission.execution_graph.root_owner_count == 0
    assert not any(tmp_path.iterdir())


def test_pipeline_dependencies_reject_foreign_runtime_root_capability(tmp_path) -> None:
    first = _isolated_dependencies(_settings(tmp_path, suffix="root-capability-first"))
    second = _isolated_dependencies(_settings(tmp_path, suffix="root-capability-second"))

    with pytest.raises(RuntimeOwnershipError, match="runtime root lifecycle owner"):
        replace(
            first,
            runtime_root_acquire=second.runtime_root_acquire,
            runtime_root_release=second.runtime_root_release,
        )

    assert first.pipeline_admission is not None
    assert second.pipeline_admission is not None
    assert first.pipeline_admission.execution_graph.root_owner_count == 0
    assert second.pipeline_admission.execution_graph.root_owner_count == 0
    assert not any(tmp_path.iterdir())


def test_pipeline_dependencies_reject_split_runtime_root_capability(tmp_path) -> None:
    first = _isolated_dependencies(_settings(tmp_path, suffix="root-capability-split-first"))
    second = _isolated_dependencies(_settings(tmp_path, suffix="root-capability-split-second"))

    with pytest.raises(RuntimeOwnershipError, match="runtime root lifecycle owner"):
        replace(
            first,
            runtime_root_release=second.runtime_root_release,
        )

    assert first.pipeline_admission is not None
    assert second.pipeline_admission is not None
    assert first.pipeline_admission.execution_graph.root_owner_count == 0
    assert second.pipeline_admission.execution_graph.root_owner_count == 0
    assert not any(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_generation_pinned_dependencies_cannot_reopen_a_closed_runtime_root(tmp_path) -> None:
    """A late optional callback may borrow its generation but never create another."""
    runtime_settings = _settings(tmp_path, suffix="generation-pinned-callback")
    stores = RuntimeStores(runtime_settings)
    owner = stores.start_runtime_services()
    graph = stores.pipeline_admission().execution_graph
    generation = graph.root_generation
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=stores,
        required_runtime_root_generation=generation,
    )

    await stores.shutdown_runtime_services(owner)

    with pytest.raises(RuntimeOwnershipError, match="runtime root generation"):
        dependencies.start_runtime_root()

    assert graph.root_generation == generation
    _assert_terminal_zero(stores)


def _install_slack_event_driver(
    monkeypatch: pytest.MonkeyPatch,
    *,
    event_count: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    handlers: dict[str, Any] = {}
    messages: list[dict[str, Any]] = []
    close_calls: list[str] = []

    async def say(**payload: Any) -> None:
        messages.append(payload)

    class FakeSlackApp:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def event(self, name: str):
            def register(handler: Any) -> Any:
                handlers[name] = handler
                return handler

            return register

        def command(self, name: str):
            def register(handler: Any) -> Any:
                handlers[name] = handler
                return handler

            return register

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def start_async(self) -> None:
            mention = handlers["app_mention"]
            for index in range(event_count):
                await mention(
                    {
                        "text": f"<@BOT> checkout latency request {index + 1}",
                        "channel": "channel-1",
                        "user": f"user-{index + 1}",
                        "team": "default",
                        "ts": f"{index + 1}.0",
                    },
                    say,
                )

        async def close_async(self) -> None:
            close_calls.append("closed")

    monkeypatch.setattr(slack_module, "AsyncApp", FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    return messages, close_calls


def _assert_sanitized_root_cleanup_record(logs: _RecordingLogger) -> None:
    matching = [record for record in logs.records if record.event == "pipeline_runtime_root_cleanup_failed"]
    assert len(matching) == 1
    record = matching[0]
    assert record.fields["reason_code"] == "pipeline_runtime_root_cleanup_failed"
    assert record.fields["error_type"] == "_RootCleanupFailure"
    assert "exc_info" not in record.fields
    rendered = repr(logs.records)
    assert _PRIMARY_SECRET not in rendered
    assert _CLEANUP_SECRET not in rendered
    assert "Traceback" not in rendered


def _dependencies_with_failing_root_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    runtime_settings: Settings,
    stores: RuntimeStores,
) -> PipelineDependencies:
    original_shutdown = stores.shutdown_runtime_services

    async def shutdown_then_fail(handle: Any) -> None:
        await original_shutdown(handle)
        raise _RootCleanupFailure(_CLEANUP_SECRET)

    monkeypatch.setattr(stores, "shutdown_runtime_services", shutdown_then_fail)
    return build_pipeline_dependencies(runtime_settings, stores=stores)


async def test_control_default_create_app_lifespan_owns_one_root_and_reaches_terminal_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="clean-app")
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    graph = stores.pipeline_admission().execution_graph

    async with app.router.lifespan_context(app):
        assert graph.root_owner_count == 1
        assert graph.root_state == "active"

    _assert_terminal_zero(stores)


async def test_deferred_root_release_survives_worker_thread_capacity_completion(tmp_path) -> None:
    """Worker-owned capacity release must not depend on a caller event loop."""
    dependencies = _isolated_dependencies(_settings(tmp_path, suffix="worker-release"))
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    root_handle = dependencies.start_runtime_root()
    assert root_handle is not None
    permit = await admission.acquire_blocking_permit()

    await dependencies.stop_runtime_root(root_handle, wait_for_drain=False)

    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    worker_errors: list[BaseException] = []

    def release_capacity() -> None:
        try:
            admission.release_blocking_permit(permit)
        except BaseException as exc:
            worker_errors.append(exc)

    worker = threading.Thread(target=release_capacity)
    worker.start()
    worker.join(timeout=1)
    assert worker.is_alive() is False
    assert worker_errors == []

    for _ in range(200):
        if graph.root_state == "closed":
            break
        await asyncio.sleep(0.005)

    _assert_isolated_terminal_zero(dependencies)


async def _wait_for_root_state(graph: Any, expected: str) -> None:
    for _ in range(200):
        if graph.root_state == expected:
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"expected runtime root state {expected!r}, found {graph.root_state!r}")


def _count_detached_root_releases(monkeypatch: pytest.MonkeyPatch, graph: Any) -> list[int]:
    releases: list[int] = []
    original_release = graph.release_root_owner_detached

    def counted_release(handle: Any) -> None:
        releases.append(handle.generation)
        original_release(handle)

    monkeypatch.setattr(graph, "release_root_owner_detached", counted_release)
    return releases


async def test_deferred_root_release_fires_after_last_ordinary_lease(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    dependencies = _isolated_dependencies(_settings(tmp_path, suffix="ordinary-release"))
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    root_handle = dependencies.start_runtime_root()
    assert root_handle is not None
    lease = await admission.acquire()
    detached_releases = _count_detached_root_releases(monkeypatch, graph)

    await dependencies.stop_runtime_root(root_handle, wait_for_drain=False)
    admission.release(lease)
    await _wait_for_root_state(graph, "closed")

    assert detached_releases == [root_handle.generation]
    _assert_isolated_terminal_zero(dependencies)


@pytest.mark.parametrize("final_transition", ["lease", "permit"])
async def test_deferred_root_release_fires_once_for_mixed_lease_and_permit_completion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    final_transition: str,
) -> None:
    dependencies = _isolated_dependencies(
        _settings(tmp_path, suffix=f"mixed-release-{final_transition}", concurrency=2)
    )
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    root_handle = dependencies.start_runtime_root()
    assert root_handle is not None
    lease = await admission.acquire()
    permit = await admission.acquire_blocking_permit()
    detached_releases = _count_detached_root_releases(monkeypatch, graph)

    await dependencies.stop_runtime_root(root_handle, wait_for_drain=False)
    if final_transition == "lease":
        admission.release_blocking_permit(permit)
        assert graph.root_state == "active"
        admission.release(lease)
    else:
        admission.release(lease)
        assert graph.root_state == "active"
        admission.release_blocking_permit(permit)
    await _wait_for_root_state(graph, "closed")

    assert detached_releases == [root_handle.generation]
    _assert_isolated_terminal_zero(dependencies)


async def test_deferred_root_release_fires_after_final_queued_waiter_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    dependencies = _isolated_dependencies(
        _settings(tmp_path, suffix="queued-cancellation", concurrency=1, max_queued=1)
    )
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    root_handle = dependencies.start_runtime_root()
    assert root_handle is not None
    service_owner = admission.try_acquire_service_owner()
    assert service_owner is not None
    with admission.service_owner(service_owner):
        service_lease = await admission.acquire()
    queued = asyncio.create_task(admission.acquire(timeout_seconds=1))
    for _ in range(100):
        if admission.queued == 1:
            break
        await asyncio.sleep(0)
    assert admission.queued == 1
    detached_releases = _count_detached_root_releases(monkeypatch, graph)

    await dependencies.stop_runtime_root(root_handle, wait_for_drain=False)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    await _wait_for_root_state(graph, "draining")

    admission.release(service_lease)
    admission.release_service_owner(service_owner)
    await _wait_for_root_state(graph, "closed")
    assert detached_releases == [root_handle.generation]
    _assert_isolated_terminal_zero(dependencies)


async def test_deferred_root_release_fires_after_selected_maintenance_exits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    dependencies = _isolated_dependencies(_settings(tmp_path, suffix="maintenance-exit", concurrency=1, max_queued=1))
    admission = dependencies.pipeline_admission
    assert admission is not None
    admission._selected_maintenance_idle_seconds = 0.01
    graph = admission.execution_graph
    root_handle = dependencies.start_runtime_root()
    assert root_handle is not None
    active = await admission.acquire()
    queued = asyncio.create_task(admission.acquire(timeout_seconds=1))
    for _ in range(100):
        if admission.queued == 1:
            break
        await asyncio.sleep(0)
    assert admission.queued == 1
    detached_releases = _count_detached_root_releases(monkeypatch, graph)

    await dependencies.stop_runtime_root(root_handle, wait_for_drain=False)
    admission.release(active)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    await _wait_for_root_state(graph, "closed")

    assert detached_releases == [root_handle.generation]
    _assert_isolated_terminal_zero(dependencies)


async def test_control_public_direct_pipeline_first_and_second_requests_use_clean_generations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="direct-sequential")
    stores = RuntimeStores(runtime_settings)
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    graph = stores.pipeline_admission().execution_graph
    observed_roots: list[int] = []
    observed_generations: list[int] = []

    async def successful_inner(request: DashRequest, *_args: Any, **_kwargs: Any) -> DashResponse:
        observed_roots.append(graph.root_owner_count)
        observed_generations.append(graph.root_generation)
        return _response(int(request.user_id.rsplit("-", 1)[-1]))

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)

    first = await run_pipeline(_request(1), dependencies)
    _assert_terminal_zero(stores)
    second = await run_pipeline(_request(2), dependencies)
    _assert_terminal_zero(stores)

    assert [first.dashboard_uid, second.dashboard_uid] == ["dashboard-1", "dashboard-2"]
    assert observed_roots == [1, 1]
    assert observed_generations == [1, 2]


async def test_isolated_direct_pipeline_owns_one_root_and_reaches_terminal_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="isolated-normal")
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    observed: list[tuple[int, str]] = []

    async def successful_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        observed.append((graph.root_owner_count, graph.root_state))
        return _response(1)

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)

    response = await run_pipeline(_request(1), dependencies)

    assert response.dashboard_uid == "dashboard-1"
    assert observed == [(1, "active")]
    _assert_isolated_terminal_zero(dependencies)


async def test_isolated_direct_pipeline_rejects_unready_stores_before_root_or_pipeline_work(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="isolated-unready").model_copy(
        update={"sqlite_snapshot_max_bytes": 1}
    )
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    pipeline_calls = 0

    async def unexpected_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        nonlocal pipeline_calls
        pipeline_calls += 1
        return _response(1)

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", unexpected_inner)

    with pytest.raises(RuntimeStoreReadinessError):
        await run_pipeline(_request(1), dependencies)

    assert pipeline_calls == 0
    assert admission.execution_graph.root_owner_count == 0
    assert admission.execution_graph.root_state == "unmanaged"


async def test_isolated_direct_pipeline_revalidates_store_generation_before_sequential_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="isolated-replaced")
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    pipeline_calls = 0

    async def successful_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        nonlocal pipeline_calls
        pipeline_calls += 1
        return _response(pipeline_calls)

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)

    first = await run_pipeline(_request(1), dependencies)
    history_path = tmp_path / "isolated-replaced-history.db"
    history_path.replace(tmp_path / "isolated-replaced-history.previous.db")
    InvestigationStore(history_path, runtime_settings=runtime_settings)

    with pytest.raises(RuntimeStoreReadinessError):
        await run_pipeline(_request(2), dependencies)

    assert first.dashboard_uid == "dashboard-1"
    assert pipeline_calls == 1
    assert admission.execution_graph.root_owner_count == 0


async def test_isolated_direct_pipeline_uses_clean_sequential_root_generations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="isolated-sequential")
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    observed: list[tuple[int, int]] = []

    async def successful_inner(request: DashRequest, *_args: Any, **_kwargs: Any) -> DashResponse:
        observed.append((graph.root_owner_count, graph.root_generation))
        return _response(int(request.user_id.rsplit("-", 1)[-1]))

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)

    first = await run_pipeline(_request(1), dependencies)
    _assert_isolated_terminal_zero(dependencies)
    second = await run_pipeline(_request(2), dependencies)

    assert [first.dashboard_uid, second.dashboard_uid] == ["dashboard-1", "dashboard-2"]
    assert observed == [(1, 1), (1, 2)]
    _assert_isolated_terminal_zero(dependencies)


async def test_isolated_direct_pipeline_cancellation_releases_root_and_authority(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="isolated-cancel")
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    started = asyncio.Event()
    observed: list[tuple[int, str]] = []

    async def blocked_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        observed.append((graph.root_owner_count, graph.root_state))
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled isolated pipeline resumed")

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", blocked_inner)
    task = asyncio.create_task(run_pipeline(_request(1), dependencies))
    await asyncio.wait_for(started.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert observed == [(1, "active")]
    _assert_isolated_terminal_zero(dependencies)


async def test_forced_pipeline_coroutine_close_releases_root_and_runtime_capacity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """GeneratorExit must transfer cleanup before the requester disappears."""
    runtime_settings = _settings(tmp_path, suffix="isolated-forced-close")
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    pipeline_calls = 0
    logs = _RecordingLogger()

    async def blocked_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        nonlocal pipeline_calls
        pipeline_calls += 1
        await asyncio.Event().wait()
        raise AssertionError("forced-close pipeline resumed")

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", blocked_inner)
    monkeypatch.setattr(runner_module, "logger", logs)
    pipeline = run_pipeline(_request(1), dependencies)

    # Drive the public coroutine only until it owns the runtime root and its
    # admission lease, then emulate an adapter forcibly discarding it.
    pipeline.send(None)
    assert graph.root_owner_count == 1
    assert graph.root_state == "active"
    assert admission.in_flight == 1

    pipeline.close()

    for _ in range(200):
        if graph.root_state == "closed":
            break
        await asyncio.sleep(0.005)

    assert pipeline_calls <= 1
    forced_close_records = [record for record in logs.records if record.event == "pipeline_task_force_close_requested"]
    assert len(forced_close_records) == 1
    assert forced_close_records[0].fields == {
        "reason_code": "pipeline_force_closed",
        "retained_by_runtime": True,
    }
    _assert_isolated_terminal_zero(dependencies)


def test_isolated_direct_pipeline_drain_survives_requester_loop_loss(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="isolated-loop-loss", concurrency=2)
    dependencies = _isolated_dependencies(runtime_settings)
    admission = dependencies.pipeline_admission
    assert admission is not None
    graph = admission.execution_graph
    drain_started = threading.Event()
    drain_finished = threading.Event()
    loop_ready = threading.Event()
    loop_closed = threading.Event()
    requester_loops: list[asyncio.AbstractEventLoop] = []
    requester_tasks: list[asyncio.Task[DashResponse]] = []
    retained_leases: list[Any] = []
    requester_errors: list[BaseException] = []
    unraisable: list[Any] = []
    logs = _RecordingLogger()
    original_begin = admission.begin_root_drain
    original_finish = admission.finish_root_drain

    monkeypatch.setattr(runner_module, "logger", logs)
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)

    def observed_begin(generation: int) -> bool:
        active = original_begin(generation)
        drain_started.set()
        return active

    def observed_finish(generation: int) -> None:
        original_finish(generation)
        drain_finished.set()

    async def successful_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        retained_leases.append(await admission.acquire())
        return _response(1)

    monkeypatch.setattr(admission, "begin_root_drain", observed_begin)
    monkeypatch.setattr(admission, "finish_root_drain", observed_finish)
    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)

    def run_requester_loop() -> None:
        loop = asyncio.new_event_loop()
        requester_loops.append(loop)
        asyncio.set_event_loop(loop)
        requester_tasks.append(loop.create_task(run_pipeline(_request(1), dependencies)))
        loop_ready.set()
        try:
            loop.run_forever()
        except BaseException as exc:
            requester_errors.append(exc)
        finally:
            for task in asyncio.all_tasks(loop):
                setattr(task, "_log_destroy_pending", False)
            asyncio.set_event_loop(None)
            loop.close()
            loop_closed.set()

    requester = threading.Thread(target=run_requester_loop, name="isolated-pipeline-requester")
    requester.start()
    assert loop_ready.wait(timeout=1)
    if not drain_started.wait(timeout=1):
        requester_loops[0].call_soon_threadsafe(requester_loops[0].stop)
        assert loop_closed.wait(timeout=1)
        requester.join(timeout=1)
        for lease in retained_leases:
            admission.release(lease)
        pytest.fail("isolated pipeline did not start a final runtime-root drain")
    assert graph.root_state == "draining"
    assert graph.root_owner_count == 0
    assert admission.in_flight == 1

    requester_loops[0].call_soon_threadsafe(requester_loops[0].stop)
    assert loop_closed.wait(timeout=1)
    requester.join(timeout=1)
    assert requester.is_alive() is False
    assert requester_errors == []
    assert requester_tasks[0].done() is False

    admission.release(retained_leases[0])

    assert drain_finished.wait(timeout=1)
    requester_task = requester_tasks.pop()
    requester_coroutine = requester_task.get_coro()
    assert requester_coroutine is not None
    requester_coroutine.close()
    del requester_coroutine, requester_task
    gc.collect()

    assert [record for record in logs.records if record.event == "pipeline_runtime_root_cleanup_failed"] == []
    assert unraisable == []
    _assert_isolated_terminal_zero(dependencies)


def test_requester_loop_loss_current_task_probe_is_safe(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_running_loop() -> None:
        raise RuntimeError("no running event loop")

    monkeypatch.setattr(runner_module.asyncio, "current_task", no_running_loop)

    assert runner_module._current_task_is_cancelling() is False


async def test_control_standalone_slack_normal_completion_closes_transport_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-transport-control", slack=True)
    stores = RuntimeStores(runtime_settings)
    messages, close_calls = _install_slack_event_driver(monkeypatch, event_count=0)

    await slack_module.start_slack_bot(runtime_settings, stores=stores)

    assert messages == []
    assert close_calls == ["closed"]
    assert stores.pipeline_admission().in_flight == 0


async def test_default_create_app_keeps_root_cardinality_constant_across_first_and_second_requests(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="app-sequential")
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    graph = stores.pipeline_admission().execution_graph
    observed_roots: list[int] = []
    observed_generations: list[int] = []

    async def successful_inner(request: DashRequest, *_args: Any, **_kwargs: Any) -> DashResponse:
        observed_roots.append(graph.root_owner_count)
        observed_generations.append(graph.root_generation)
        return _response(int(request.user_id.rsplit("-", 1)[-1]))

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)

    async with app.router.lifespan_context(app):
        first = await run_pipeline(_request(1), dependencies)
        assert graph.root_owner_count == 1
        second = await run_pipeline(_request(2), dependencies)
        assert graph.root_owner_count == 1

    _assert_terminal_zero(stores)
    assert [first.dashboard_uid, second.dashboard_uid] == ["dashboard-1", "dashboard-2"]
    assert observed_roots == [1, 1]
    assert observed_generations == [1, 1]


async def test_app_direct_overlap_is_bounded_by_the_app_root_not_request_volume(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    request_count = 3
    runtime_settings = _settings(tmp_path, suffix="app-direct-overlap", concurrency=request_count)
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    graph = stores.pipeline_admission().execution_graph
    all_started = asyncio.Event()
    release = asyncio.Event()
    started = 0
    observed_roots: list[int] = []

    async def blocked_inner(request: DashRequest, *_args: Any, **_kwargs: Any) -> DashResponse:
        nonlocal started
        observed_roots.append(graph.root_owner_count)
        started += 1
        if started == request_count:
            all_started.set()
        await release.wait()
        return _response(int(request.user_id.rsplit("-", 1)[-1]))

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", blocked_inner)

    async with app.router.lifespan_context(app):
        tasks = [asyncio.create_task(run_pipeline(_request(index), dependencies)) for index in range(1, 4)]
        try:
            await asyncio.wait_for(all_started.wait(), timeout=1)
            roots_at_saturation = graph.root_owner_count
        finally:
            release.set()
        responses = await asyncio.gather(*tasks)
        root_count_after_requests = graph.root_owner_count

    _assert_terminal_zero(stores)
    assert [response.dashboard_uid for response in responses] == ["dashboard-1", "dashboard-2", "dashboard-3"]
    assert roots_at_saturation == 1
    assert observed_roots == [1, 1, 1]
    assert root_count_after_requests == 1


async def test_app_shutdown_waits_for_active_direct_work_and_finalizes_the_shared_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="app-active-direct")
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    graph = stores.pipeline_admission().execution_graph
    request_started = asyncio.Event()
    release_request = asyncio.Event()
    observed_roots: list[int] = []

    async def blocked_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        observed_roots.append(graph.root_owner_count)
        request_started.set()
        await release_request.wait()
        return _response(1)

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", blocked_inner)
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    request_task = asyncio.create_task(run_pipeline(_request(1), dependencies))
    exit_task: asyncio.Task[Any] | None = None
    try:
        await asyncio.wait_for(request_started.wait(), timeout=1)
        exit_task = asyncio.create_task(lifespan.__aexit__(None, None, None))
        done, _ = await asyncio.wait({exit_task}, timeout=0.05)
        shutdown_waited_for_request = exit_task not in done
    finally:
        release_request.set()

    response = await asyncio.wait_for(request_task, timeout=1)
    if exit_task is not None:
        await asyncio.wait_for(exit_task, timeout=1)

    _assert_terminal_zero(stores)
    assert response.dashboard_uid == "dashboard-1"
    assert observed_roots == [1]
    assert shutdown_waited_for_request is True


async def test_standalone_slack_owns_one_root_across_first_and_second_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="standalone-slack", slack=True)
    stores = RuntimeStores(runtime_settings)
    graph = stores.pipeline_admission().execution_graph
    messages, close_calls = _install_slack_event_driver(monkeypatch, event_count=2)
    slack_logs = _RecordingLogger()
    observed_roots: list[int] = []
    observed_generations: list[int] = []

    async def successful_inner(request: DashRequest, *_args: Any, **_kwargs: Any) -> DashResponse:
        observed_roots.append(graph.root_owner_count)
        observed_generations.append(graph.root_generation)
        return _response(int(request.user_id.rsplit("-", 1)[-1]))

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)
    monkeypatch.setattr(slack_module, "logger", slack_logs)

    await slack_module.start_slack_bot(runtime_settings, stores=stores)

    successful_messages = [message for message in messages if "blocks" in message]
    assert len(successful_messages) == 2
    assert observed_roots == [1, 1]
    assert observed_generations == [1, 1]
    assert close_calls == ["closed"]
    _assert_terminal_zero(stores)


async def test_fresh_direct_caller_restarts_after_a_closed_app_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="fresh-after-app")
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    graph = stores.pipeline_admission().execution_graph
    async with app.router.lifespan_context(app):
        assert graph.root_generation == 1
    _assert_terminal_zero(stores)

    observed_direct: list[tuple[int, int]] = []

    async def direct_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        observed_direct.append((graph.root_owner_count, graph.root_generation))
        return _response(1)

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", direct_inner)
    fresh_stores = RuntimeStores(runtime_settings)
    direct_dependencies = build_pipeline_dependencies(runtime_settings, stores=fresh_stores)
    direct_response = await run_pipeline(_request(1), direct_dependencies)
    _assert_terminal_zero(fresh_stores)

    assert direct_response.dashboard_uid == "dashboard-1"
    assert observed_direct == [(1, 2)]


async def test_fresh_standalone_slack_restarts_after_a_closed_app_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="fresh-slack-after-app", slack=True)
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    graph = stores.pipeline_admission().execution_graph
    production_start_slack_bot = slack_module.start_slack_bot

    async def idle_slack_bot(_settings: Settings, *, stores: RuntimeStores) -> None:
        assert stores is app.state.runtime_stores
        await asyncio.Event().wait()

    monkeypatch.setattr(slack_module, "start_slack_bot", idle_slack_bot)
    async with app.router.lifespan_context(app):
        assert graph.root_generation == 1
    _assert_terminal_zero(stores)
    monkeypatch.setattr(slack_module, "start_slack_bot", production_start_slack_bot)

    observed_slack: list[tuple[int, int]] = []
    messages, close_calls = _install_slack_event_driver(monkeypatch, event_count=1)
    slack_logs = _RecordingLogger()

    async def slack_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        observed_slack.append((graph.root_owner_count, graph.root_generation))
        return _response(2)

    monkeypatch.setattr(runner_module, "_run_pipeline_inner", slack_inner)
    monkeypatch.setattr(slack_module, "logger", slack_logs)
    await slack_module.start_slack_bot(runtime_settings, stores=stores)

    assert len([message for message in messages if "blocks" in message]) == 1
    assert observed_slack == [(1, 2)]
    assert close_calls == ["closed"]
    _assert_terminal_zero(stores)


async def test_create_app_slack_teardown_cancellation_cannot_skip_runtime_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="app-slack-cancel", slack=True)
    monkeypatch.setattr("tacit.logging.configure_logging", lambda _level: None)
    app = create_app(
        runtime_settings=runtime_settings,
        lifespan=create_lifespan(runtime_settings),
        include_default_routes=False,
    )
    stores = app.state.runtime_stores
    graph = stores.pipeline_admission().execution_graph
    slack_started = threading.Event()
    slack_close_started = threading.Event()
    release_slack_close = threading.Event()
    slack_close_finished = threading.Event()
    handles: list[Any] = []
    shutdown_calls: list[Any] = []
    original_start = stores.start_runtime_services
    original_shutdown = stores.shutdown_runtime_services

    def observed_start() -> Any:
        handle = original_start()
        handles.append(handle)
        return handle

    async def observed_shutdown(handle: Any) -> None:
        shutdown_calls.append(handle)
        await original_shutdown(handle)

    async def fake_start_slack_bot(
        _settings: Settings,
        *,
        stores: RuntimeStores,
        on_ready: Any = None,
    ) -> None:
        assert stores is app.state.runtime_stores
        if on_ready is not None:
            on_ready()
        slack_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            slack_close_started.set()
            while not release_slack_close.is_set():
                try:
                    await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    continue
            slack_close_finished.set()

    async def wait_for_thread_event(event: threading.Event) -> None:
        for _ in range(1_000):
            if event.is_set():
                return
            await asyncio.sleep(0.001)
        raise TimeoutError("Slack thread event did not settle")

    monkeypatch.setattr(stores, "start_runtime_services", observed_start)
    monkeypatch.setattr(stores, "shutdown_runtime_services", observed_shutdown)
    monkeypatch.setattr(slack_module, "start_slack_bot", fake_start_slack_bot)

    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    await wait_for_thread_event(slack_started)
    exit_task = asyncio.create_task(lifespan.__aexit__(None, None, None))
    await wait_for_thread_event(slack_close_started)
    exit_task.cancel()
    await asyncio.sleep(0)
    cancellation_was_deferred = not exit_task.done()
    release_slack_close.set()
    await wait_for_thread_event(slack_close_finished)
    cancelled = False
    try:
        await exit_task
    except asyncio.CancelledError:
        cancelled = True

    shutdown_calls_before_repair = list(shutdown_calls)
    terminal_state_before_repair = (graph.root_state, graph.root_owner_count)
    if graph.root_owner_count:
        assert handles
        await original_shutdown(handles[0])

    _assert_terminal_zero(stores)
    assert cancelled is True
    assert cancellation_was_deferred is True
    assert len(shutdown_calls_before_repair) == 1
    assert terminal_state_before_repair == ("closed", 0)


async def test_successful_pipeline_outcome_survives_root_cleanup_failure_with_sanitized_audit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="root-cleanup-success")
    stores = RuntimeStores(runtime_settings)
    dependencies = _dependencies_with_failing_root_cleanup(monkeypatch, runtime_settings, stores)
    logs = _RecordingLogger()

    async def successful_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        return _response(1)

    monkeypatch.setattr(runner_module, "logger", logs)
    monkeypatch.setattr(runner_module, "_run_pipeline_inner", successful_inner)
    response: DashResponse | None = None
    unexpected_error: BaseException | None = None
    try:
        response = await run_pipeline(_request(1), dependencies)
    except BaseException as exc:
        unexpected_error = exc

    _assert_terminal_zero(stores)
    assert unexpected_error is None
    assert response is not None
    assert response.dashboard_uid == "dashboard-1"
    _assert_sanitized_root_cleanup_record(logs)


async def test_primary_pipeline_failure_survives_root_cleanup_failure_with_sanitized_audit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="root-cleanup-primary")
    stores = RuntimeStores(runtime_settings)
    dependencies = _dependencies_with_failing_root_cleanup(monkeypatch, runtime_settings, stores)
    logs = _RecordingLogger()
    primary_error = _PrimaryPipelineFailure(_PRIMARY_SECRET)

    async def failing_inner(*_args: Any, **_kwargs: Any) -> DashResponse:
        raise primary_error

    monkeypatch.setattr(runner_module, "logger", logs)
    monkeypatch.setattr(runner_module, "_run_pipeline_inner", failing_inner)
    observed_error: BaseException | None = None
    try:
        await run_pipeline(_request(1), dependencies)
    except BaseException as exc:
        observed_error = exc

    _assert_terminal_zero(stores)
    assert observed_error is primary_error
    _assert_sanitized_root_cleanup_record(logs)
