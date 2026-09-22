"""Regression matrix for composition-root and cross-loop runtime shutdown."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future as ThreadFuture
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner
from fastapi import FastAPI

from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime
from tacit.api.lifespan import create_lifespan
from tacit.cli import cli
from tacit.config import Settings
from tacit.dependencies import _RuntimeProviderResources, build_pipeline_dependencies
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController, RuntimeRootDrainStartupError
from tacit.runtime_ownership import (
    BedrockCredentialIdentity,
    credential_fingerprint,
    declare_runtime_factory,
    runtime_descriptor_for_provider,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStores


class _ProviderProbe(LLMProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="runtime_lifecycle_matrix_provider")
        self.closed = False

    async def chat_json(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        return LLMResult("ok")

    async def close(self) -> None:
        self.closed = True


class _Closeable:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _BlockingRuntimeClient(_Closeable):
    def __init__(self, started: threading.Event, release: threading.Event) -> None:
        super().__init__()
        self._started = started
        self._release = release

    def converse(self, **_kwargs: Any) -> dict[str, Any]:
        self._started.set()
        assert self._release.wait(timeout=2.0), "test did not release the Bedrock worker"
        return {"output": {"message": {"content": [{"text": "ok"}]}}}


class _RuntimeSession(_Closeable):
    def __init__(self, client: _BlockingRuntimeClient) -> None:
        super().__init__()
        self._client = client

    def client(self, service_name: str, **_kwargs: Any) -> _BlockingRuntimeClient:
        assert service_name == "bedrock-runtime"
        return self._client

    def get_credentials(self) -> None:
        return None


def _settings(tmp_path, *, suffix: str, **updates: Any) -> Settings:
    values: dict[str, Any] = {
        "_env_file": None,
        "history_db_path": str(tmp_path / f"{suffix}-history.db"),
        "feedback_db_path": str(tmp_path / f"{suffix}-feedback.db"),
        "signals_db_path": str(tmp_path / f"{suffix}-signals.db"),
        "llm_provider": "ollama",
        "llm_api_base": "http://127.0.0.1:11434",
        "context_provider": "none",
        "pipeline_max_concurrent": 1,
        "pipeline_max_queued": 0,
    }
    values.update(updates)
    return Settings(**values)


def _declared_provider(runtime_settings: Settings, factory):
    return declare_runtime_factory(
        factory,
        ownership=runtime_descriptor_for_provider(
            component="runtime_lifecycle_matrix_provider_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )


def _app(runtime_settings: Settings, stores: RuntimeStores) -> FastAPI:
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = stores
    return app


def _provider_resources(
    runtime_settings: Settings,
    *,
    limit: int = 1,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController, list[_ProviderProbe]]:
    providers: list[_ProviderProbe] = []

    def factory() -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    lifecycle = PipelineAdmissionController(limit, max_queued=0)
    lifecycle.bind_runtime_identity(
        runtime_descriptor_from_settings(
            runtime_settings,
            component="runtime_lifecycle_matrix",
        ).admission_namespace
        or "runtime-lifecycle-matrix"
    )
    resources = _RuntimeProviderResources.resolve(
        runtime_settings,
        lifecycle=lifecycle,
        llm_factory=_declared_provider(runtime_settings, factory),
        cleanup_grace_seconds=0.1,
    )
    return resources, lifecycle, providers


@pytest.mark.asyncio
async def test_overlapping_app_roots_release_only_the_exiting_runtime_owner(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, suffix="overlapping-app-roots")
    providers: list[_ProviderProbe] = []

    def factory() -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    declared = _declared_provider(runtime_settings, factory)
    first_stores = RuntimeStores(runtime_settings)
    second_stores = RuntimeStores(runtime_settings)
    first = build_pipeline_dependencies(runtime_settings, stores=first_stores, llm_provider_factory=declared)
    second = build_pipeline_dependencies(runtime_settings, stores=second_stores, llm_provider_factory=declared)
    assert first.pipeline_admission is second.pipeline_admission

    first_lifespan = create_lifespan(runtime_settings)(_app(runtime_settings, first_stores))
    second_lifespan = create_lifespan(runtime_settings)(_app(runtime_settings, second_stores))
    await first_lifespan.__aenter__()
    await second_lifespan.__aenter__()
    first_exited = False
    second_exited = False
    sibling_error: BaseException | None = None
    sibling_text = ""
    closed_after_first_exit = False
    try:
        first_handle = await first.acquire_resources()
        await second.acquire_resources()
        assert first_handle is not None
        assert second.llm_provider_factory is not None
        sibling_provider = second.llm_provider_factory()

        await first.close_resources(first_handle)
        await first_lifespan.__aexit__(None, None, None)
        first_exited = True
        closed_after_first_exit = providers[0].closed
        try:
            sibling_text = (await sibling_provider.chat_text("system", "user")).text
        except BaseException as exc:
            sibling_error = exc

        await second_lifespan.__aexit__(None, None, None)
        second_exited = True
    finally:
        if not first_exited:
            await first_lifespan.__aexit__(None, None, None)
        if not second_exited:
            await second_lifespan.__aexit__(None, None, None)

    lifecycle = first.pipeline_admission
    assert lifecycle is not None
    assert closed_after_first_exit is False
    assert sibling_error is None
    assert sibling_text == "ok"
    assert providers[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_final_root_exit_allows_a_later_sequential_lifespan_generation(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, suffix="sequential-app-roots")
    providers: list[_ProviderProbe] = []

    def factory() -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    declared = _declared_provider(runtime_settings, factory)
    first_stores = RuntimeStores(runtime_settings)
    first = build_pipeline_dependencies(runtime_settings, stores=first_stores, llm_provider_factory=declared)
    first_lifespan = create_lifespan(runtime_settings)(_app(runtime_settings, first_stores))
    await first_lifespan.__aenter__()
    await first.acquire_resources()
    await first_lifespan.__aexit__(None, None, None)
    with pytest.raises(RuntimeOwnershipError):
        await first.acquire_resources()

    second_stores = RuntimeStores(runtime_settings)
    second_lifespan = create_lifespan(runtime_settings)(_app(runtime_settings, second_stores))
    await second_lifespan.__aenter__()
    second = build_pipeline_dependencies(runtime_settings, stores=second_stores, llm_provider_factory=declared)
    second_error: BaseException | None = None
    second_text = ""
    try:
        try:
            await second.acquire_resources()
            assert second.llm_provider_factory is not None
            second_text = (await second.llm_provider_factory().chat_text("system", "user")).text
        except BaseException as exc:
            second_error = exc
    finally:
        await second_lifespan.__aexit__(None, None, None)

    assert second.pipeline_admission is first.pipeline_admission
    assert second_error is None
    assert second_text == "ok"
    assert len(providers) == 2
    assert all(provider.closed for provider in providers)


def test_completed_requester_owner_is_reclaimed_from_a_stopped_open_loop(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, suffix="completed-requester-owner")
    resources, lifecycle, providers = _provider_resources(runtime_settings)
    root_handle = lifecycle.execution_graph.register_root_owner()
    requester_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(requester_loop)
    owner_task = requester_loop.create_task(resources.acquire())
    requester_loop.run_until_complete(owner_task)
    assert owner_task.done() is True
    assert requester_loop.is_running() is False
    assert requester_loop.is_closed() is False

    asyncio.set_event_loop(None)
    asyncio.run(lifecycle.execution_graph.release_root_owner(root_handle))
    reclaimed_while_open = providers[0].closed
    counters_after_reclaim = (
        lifecycle.in_flight,
        lifecycle.retained,
        lifecycle.blocking_in_flight,
        lifecycle.service_owner_in_flight,
    )

    requester_loop.close()

    assert reclaimed_while_open is True
    assert counters_after_reclaim == (0, 0, 0, 0)


def test_pending_requester_owner_survives_a_deliberately_paused_loop(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, suffix="paused-requester-owner")
    resources, lifecycle, providers = _provider_resources(runtime_settings)
    requester_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(requester_loop)
    ready = asyncio.Event()
    resume = asyncio.Event()

    async def owner() -> None:
        handle = await resources.acquire()
        ready.set()
        await resume.wait()
        await resources.close(handle)

    owner_task = requester_loop.create_task(owner())
    requester_loop.run_until_complete(ready.wait())
    assert owner_task.done() is False
    assert requester_loop.is_running() is False
    assert requester_loop.is_closed() is False

    asyncio.set_event_loop(None)
    asyncio.run(resources.close())
    assert providers[0].closed is False
    assert lifecycle.service_owner_in_flight == 1

    requester_loop.call_soon(resume.set)
    asyncio.set_event_loop(requester_loop)
    requester_loop.run_until_complete(owner_task)
    requester_loop.close()
    asyncio.set_event_loop(None)

    assert providers[0].closed is True
    assert lifecycle.service_owner_in_flight == 0


def test_final_root_drain_does_not_falsify_capacity_for_a_stopped_requester_task() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    root_handle = controller.execution_graph.register_root_owner()
    requester_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(requester_loop)
    effective_work = ThreadFuture[None]()
    work_started = asyncio.Event()
    cancellation_observed = asyncio.Event()

    async def resistant_work() -> None:
        work_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_observed.set()
            await asyncio.wrap_future(effective_work)

    async def owner() -> asyncio.Task[None]:
        lease = await controller.acquire()
        task = asyncio.create_task(resistant_work())
        await work_started.wait()
        task.cancel()
        await cancellation_observed.wait()
        assert controller.retain_task(lease, task) is True
        controller.release(lease)
        return task

    retained_task = requester_loop.run_until_complete(owner())
    assert retained_task.done() is False
    assert requester_loop.is_running() is False
    assert controller.in_flight == 1
    assert controller.retained == 1

    asyncio.set_event_loop(None)
    with pytest.raises(PipelineAdmissionRejected):
        asyncio.run(controller.acquire())

    drain_started = threading.Event()
    drain_finished = threading.Event()
    drain_failures: list[BaseException] = []
    original_begin_drain = controller.begin_root_drain

    def observed_begin_drain(generation: int) -> bool:
        admitted_work_active = original_begin_drain(generation)
        drain_started.set()
        return admitted_work_active

    controller.begin_root_drain = observed_begin_drain  # type: ignore[method-assign]

    def drain_root() -> None:
        try:
            asyncio.run(controller.execution_graph.release_root_owner(root_handle))
        except BaseException as exc:
            drain_failures.append(exc)
        finally:
            drain_finished.set()

    drain_thread = threading.Thread(target=drain_root)
    drain_thread.start()
    assert drain_started.wait(timeout=1.0)
    assert drain_finished.is_set() is False
    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        asyncio.run(controller.acquire())
    assert exc_info.value.reason_code == "pipeline_admission_rejected"

    effective_work.set_result(None)
    assert effective_work.done() is True
    assert retained_task.done() is False
    assert drain_finished.is_set() is False

    asyncio.set_event_loop(requester_loop)
    requester_loop.run_until_complete(retained_task)
    callback_barrier = requester_loop.create_future()
    requester_loop.call_soon(callback_barrier.set_result, None)
    requester_loop.run_until_complete(callback_barrier)
    requester_loop.close()
    asyncio.set_event_loop(None)

    assert drain_finished.wait(timeout=1.0)
    drain_thread.join(timeout=1.0)
    assert drain_thread.is_alive() is False
    assert drain_failures == []
    next_root = controller.execution_graph.register_root_owner()
    final_lease = asyncio.run(controller.acquire())
    controller.release(final_lease)
    asyncio.run(controller.execution_graph.release_root_owner(next_root))
    assert controller.in_flight == 0
    assert controller.retained == 0


@pytest.mark.asyncio
async def test_api_final_root_waits_for_cancelled_bedrock_worker_and_zero_capacity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    access_key = "test-runtime-access-key"
    secret_key = "runtime-lifecycle-secret"
    runtime_settings = _settings(
        tmp_path,
        suffix="api-bedrock-drain",
        llm_provider="bedrock",
        llm_api_base="",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        llm_aws_access_key_id=access_key,
        llm_aws_secret_access_key=secret_key,
    )
    request_started = threading.Event()
    release_request = threading.Event()
    runtime_client = _BlockingRuntimeClient(request_started, release_request)
    runtime_session = _RuntimeSession(runtime_client)
    credential_client = _Closeable()
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _ResolvedBedrockRuntime(
            session=runtime_session,
            credential_identity=BedrockCredentialIdentity(
                account=f"access-key:{credential_fingerprint(access_key)}",
                credential_fingerprint=credential_fingerprint("\0".join((access_key, secret_key, ""))),
                uses_sts=False,
            ),
            credential_clients=(credential_client,),
        ),
    )

    stores = RuntimeStores(runtime_settings)
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    lifespan = create_lifespan(runtime_settings)(_app(runtime_settings, stores))
    await lifespan.__aenter__()
    await dependencies.acquire_resources()
    assert dependencies.llm_provider_factory is not None

    worker_released = threading.Event()
    original_release = lifecycle.release_blocking_permit

    def observed_release(permit) -> None:
        original_release(permit)
        if request_started.is_set() and runtime_client.close_calls and lifecycle.blocking_in_flight == 0:
            worker_released.set()

    monkeypatch.setattr(lifecycle, "release_blocking_permit", observed_release)
    request_task = asyncio.create_task(dependencies.llm_provider_factory().chat_text("system", "user"))
    assert await asyncio.to_thread(request_started.wait, 1.0)
    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task
    assert lifecycle.in_flight == 1
    assert lifecycle.blocking_in_flight == 1
    assert lifecycle.service_owner_in_flight == 1

    manager = lifecycle.execution_graph.provider_manager()
    assert manager is not None
    shutdown_started = asyncio.Event()
    original_shutdown = getattr(manager, "shutdown", None)
    assert callable(original_shutdown)

    async def observed_shutdown() -> None:
        shutdown_started.set()
        await original_shutdown()

    monkeypatch.setattr(manager, "shutdown", observed_shutdown)
    exit_task = asyncio.create_task(lifespan.__aexit__(None, None, None))
    await shutdown_started.wait()
    with pytest.raises(RuntimeOwnershipError, match="shutting down"):
        await dependencies.acquire_resources()

    assert exit_task.done() is False
    assert lifecycle.blocking_in_flight == 1
    release_request.set()
    await exit_task

    assert await asyncio.to_thread(worker_released.wait, 1.0)
    assert runtime_client.close_calls == 1
    assert runtime_session.close_calls == 1
    assert credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


def test_cli_pipeline_command_terminally_shuts_down_its_runtime_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    root_handle = object()
    start_calls: list[str] = []
    shutdown_calls: list[str] = []
    runtime_settings = Settings(_env_file=None)  # type: ignore[call-arg]

    class Stores:
        settings = runtime_settings

        def start_runtime_services(self):
            start_calls.append("start")
            return root_handle

        async def shutdown_runtime_services(self, handle) -> None:
            assert handle is root_handle
            shutdown_calls.append("shutdown")

    async def fake_run_pipeline(_request, _dependencies):
        return SimpleNamespace(
            investigation_status="completed",
            audit_status="run_completed",
            dashboard_url="http://127.0.0.1/dashboard",
            dashboard_uid="runtime-lifecycle",
            panel_count=1,
            path_used="freeform",
            archetypes=[],
        )

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", Stores)
    monkeypatch.setattr("tacit.dependencies.build_pipeline_dependencies", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("tacit.pipeline.run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(cli, ["test", "--no-open-browser"])

    assert result.exit_code == 0, result.output
    assert start_calls == ["start"]
    assert shutdown_calls == ["shutdown"]


def test_cli_investigate_terminally_shuts_down_its_runtime_owner(monkeypatch: pytest.MonkeyPatch) -> None:
    root_handle = object()
    lifecycle_calls: list[str] = []
    runtime_settings = Settings(_env_file=None)  # type: ignore[call-arg]

    class Contract:
        def model_dump(self, **_kwargs: Any) -> dict[str, object]:
            return {"investigation": {"id": "inv-runtime-lifecycle"}}

    class History:
        def get_contract(self, investigation_id, revision, *, tenant_id):
            assert (investigation_id, revision, tenant_id) == ("inv-runtime-lifecycle", 1, "default")
            return Contract()

    class Stores:
        settings = runtime_settings

        def start_runtime_services(self):
            lifecycle_calls.append("start")
            return root_handle

        async def shutdown_runtime_services(self, handle) -> None:
            assert handle is root_handle
            lifecycle_calls.append("shutdown")

        def history(self):
            return History()

    async def fake_run_pipeline(_request, _dependencies):
        return SimpleNamespace(
            investigation_id="inv-runtime-lifecycle",
            investigation_revision=1,
            investigation_status="completed",
            audit_status="run_completed",
        )

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", Stores)
    monkeypatch.setattr("tacit.dependencies.build_pipeline_dependencies", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("tacit.pipeline.run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(cli, ["investigate", "check runtime lifecycle", "--json"])

    assert result.exit_code == 0, result.output
    assert lifecycle_calls == ["start", "shutdown"]
    assert "inv-runtime-lifecycle" in result.output


def test_cli_test_retries_one_root_drain_startup_failure_and_reaches_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_handle = SimpleNamespace(generation=1)
    runtime_settings = Settings(_env_file=None)  # type: ignore[call-arg]

    class Stores:
        settings = runtime_settings
        root_active = False
        shutdown_calls = 0

        def start_runtime_services(self):
            self.root_active = True
            return root_handle

        async def shutdown_runtime_services(self, handle) -> None:
            assert handle is root_handle
            self.shutdown_calls += 1
            if self.shutdown_calls == 1:
                raise RuntimeRootDrainStartupError("preflight failed once")
            self.root_active = False

    stores = Stores()

    async def fake_run_pipeline(_request, _dependencies):
        return SimpleNamespace(
            investigation_status="completed",
            audit_status="run_completed",
            dashboard_url="http://127.0.0.1/dashboard",
            dashboard_uid="runtime-lifecycle-retry",
            panel_count=1,
            path_used="freeform",
            archetypes=[],
        )

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    monkeypatch.setattr("tacit.dependencies.build_pipeline_dependencies", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("tacit.pipeline.run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(cli, ["test", "--no-open-browser"])

    assert result.exit_code == 0, result.output
    assert stores.shutdown_calls == 2
    assert stores.root_active is False


def test_cli_investigate_retries_one_root_drain_startup_failure_and_reaches_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_handle = SimpleNamespace(generation=1)
    runtime_settings = Settings(_env_file=None)  # type: ignore[call-arg]

    class Contract:
        def model_dump(self, **_kwargs: Any) -> dict[str, object]:
            return {"investigation": {"id": "inv-runtime-retry"}}

    class History:
        def get_contract(self, investigation_id, revision, *, tenant_id):
            assert (investigation_id, revision, tenant_id) == ("inv-runtime-retry", 1, "default")
            return Contract()

    class Stores:
        settings = runtime_settings
        root_active = False
        shutdown_calls = 0

        def start_runtime_services(self):
            self.root_active = True
            return root_handle

        async def shutdown_runtime_services(self, handle) -> None:
            assert handle is root_handle
            self.shutdown_calls += 1
            if self.shutdown_calls == 1:
                raise RuntimeRootDrainStartupError("preflight failed once")
            self.root_active = False

        def history(self):
            return History()

    stores = Stores()

    async def fake_run_pipeline(_request, _dependencies):
        return SimpleNamespace(
            investigation_id="inv-runtime-retry",
            investigation_revision=1,
            investigation_status="completed",
            audit_status="run_completed",
        )

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    monkeypatch.setattr("tacit.dependencies.build_pipeline_dependencies", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("tacit.pipeline.run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(cli, ["investigate", "check runtime retry", "--json"])

    assert result.exit_code == 0, result.output
    assert stores.shutdown_calls == 2
    assert stores.root_active is False


@pytest.mark.parametrize(
    "arguments",
    [
        ["test", "--no-open-browser"],
        ["investigate", "check nonretryable cleanup", "--json"],
    ],
)
def test_cli_commands_do_not_retry_nonretryable_root_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    arguments: list[str],
) -> None:
    runtime_settings = Settings(_env_file=None)  # type: ignore[call-arg]

    class Stores:
        settings = runtime_settings
        shutdown_calls = 0

        def start_runtime_services(self):
            return object()

        async def shutdown_runtime_services(self, _handle) -> None:
            self.shutdown_calls += 1
            raise RuntimeError("post-publication cleanup failed")

        def history(self):
            raise AssertionError("history must not be read after cleanup failure")

    stores = Stores()

    async def fake_run_pipeline(_request, _dependencies):
        return SimpleNamespace(
            investigation_id="inv-nonretryable",
            investigation_revision=1,
            investigation_status="completed",
            audit_status="run_completed",
            dashboard_url="http://127.0.0.1/dashboard",
            dashboard_uid="runtime-lifecycle-nonretryable",
            panel_count=1,
            path_used="freeform",
            archetypes=[],
        )

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    monkeypatch.setattr("tacit.dependencies.build_pipeline_dependencies", lambda *_args, **_kwargs: object())
    monkeypatch.setattr("tacit.pipeline.run_pipeline", fake_run_pipeline)

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code == 1
    assert stores.shutdown_calls == 1


@pytest.mark.asyncio
async def test_root_handles_are_generation_fenced_and_duplicate_release_is_rejected() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    first = graph.register_root_owner()
    sibling = graph.register_root_owner()
    foreign_graph = PipelineAdmissionController(1, max_queued=0).execution_graph
    foreign = foreign_graph.register_root_owner()

    with pytest.raises(RuntimeOwnershipError, match="another execution graph"):
        await graph.release_root_owner(foreign)
    assert graph.root_owner_count == 2

    await graph.release_root_owner(first)
    assert graph.root_state == "active"
    assert graph.root_owner_count == 1
    with pytest.raises(RuntimeOwnershipError, match="stale or duplicate"):
        await graph.release_root_owner(first)

    await graph.release_root_owner(sibling)
    assert graph.root_state == "closed"
    replacement = graph.register_root_owner()
    assert replacement.generation == sibling.generation + 1
    await graph.release_root_owner(replacement)
    await foreign_graph.release_root_owner(foreign)


@pytest.mark.asyncio
async def test_final_root_failure_closes_the_generation_and_allows_an_explicit_retry() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph

    class FailingManager:
        async def shutdown(self) -> None:
            raise RuntimeError("provider shutdown failed")

    graph.resolve_provider_manager(spec="failing-manager", create=FailingManager)
    failed_root = graph.register_root_owner()

    with pytest.raises(RuntimeError, match="provider shutdown failed"):
        await graph.release_root_owner(failed_root)

    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    replacement = graph.register_root_owner()
    await graph.release_root_owner(replacement)


@pytest.mark.asyncio
async def test_final_root_cancellation_is_deferred_until_terminal_drain() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    shutdown_started = asyncio.Event()
    release_shutdown = asyncio.Event()

    class BlockingManager:
        async def shutdown(self) -> None:
            shutdown_started.set()
            await release_shutdown.wait()

    graph.resolve_provider_manager(spec="blocking-manager", create=BlockingManager)
    root = graph.register_root_owner()
    release_task = asyncio.create_task(graph.release_root_owner(root))
    await shutdown_started.wait()

    release_task.cancel()
    await asyncio.sleep(0)
    assert release_task.done() is False
    assert graph.root_state == "draining"

    release_shutdown.set()
    with pytest.raises(asyncio.CancelledError):
        await release_task

    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"


def test_final_root_drain_survives_releasing_caller_loop_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    active_lease = asyncio.run(controller.acquire())
    drain_started = threading.Event()
    drain_finished = threading.Event()
    releasing_loop_ready = threading.Event()
    releasing_loop_closed = threading.Event()
    releasing_loop: list[asyncio.AbstractEventLoop] = []
    releasing_loop_errors: list[BaseException] = []
    begin_root_drain = controller.begin_root_drain
    finish_root_drain = controller.finish_root_drain

    def observed_begin_root_drain(generation: int) -> bool:
        admitted_work_active = begin_root_drain(generation)
        drain_started.set()
        return admitted_work_active

    def observed_finish_root_drain(generation: int) -> None:
        finish_root_drain(generation)
        drain_finished.set()

    monkeypatch.setattr(controller, "begin_root_drain", observed_begin_root_drain)
    monkeypatch.setattr(controller, "finish_root_drain", observed_finish_root_drain)

    def run_releasing_loop() -> None:
        loop = asyncio.new_event_loop()
        releasing_loop.append(loop)
        asyncio.set_event_loop(loop)
        releasing_loop_ready.set()
        try:
            loop.run_forever()
        except BaseException as exc:
            releasing_loop_errors.append(exc)
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task._log_destroy_pending = False
            asyncio.set_event_loop(None)
            loop.close()
            releasing_loop_closed.set()

    loop_thread = threading.Thread(target=run_releasing_loop)
    loop_thread.start()
    assert releasing_loop_ready.wait(timeout=1.0)
    release_future = asyncio.run_coroutine_threadsafe(
        graph.release_root_owner(root),
        releasing_loop[0],
    )

    assert drain_started.wait(timeout=1.0)
    assert graph.root_state == "draining"
    assert controller.runtime_root_state == "draining"
    assert controller.in_flight == 1

    releasing_loop[0].call_soon_threadsafe(releasing_loop[0].stop)
    assert releasing_loop_closed.wait(timeout=1.0)
    loop_thread.join(timeout=1.0)
    assert loop_thread.is_alive() is False
    assert releasing_loop_errors == []
    assert release_future.done() is False

    controller.release(active_lease)

    assert drain_finished.wait(timeout=1.0)
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "closed"
    assert controller.in_flight == 0
    assert controller.retained == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0

    replacement = graph.register_root_owner()
    assert replacement.generation == root.generation + 1
    replacement_lease = asyncio.run(controller.acquire())
    controller.release(replacement_lease)
    asyncio.run(graph.release_root_owner(replacement))


@pytest.mark.asyncio
async def test_runtime_identity_binding_is_fenced_after_a_root_handle_exists() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    original_runtime_identity = controller.runtime_identity
    original_graph_identity = graph.runtime_identity
    original_graph_nonce = graph.graph_nonce

    with pytest.raises(RuntimeOwnershipError, match="another runtime"):
        controller.bind_runtime_identity("late-runtime-identity")

    assert controller.runtime_identity == original_runtime_identity
    assert graph.runtime_identity == original_graph_identity
    assert graph.graph_nonce == original_graph_nonce
    assert graph.root_owner_count == 1
    await graph.release_root_owner(root)
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0


def test_overlapping_roots_on_separate_event_loops_share_one_final_drain() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    loops: list[asyncio.AbstractEventLoop | None] = [None, None]
    roots: list[Any | None] = [None, None]
    ready = [threading.Event(), threading.Event()]
    loop_errors: list[BaseException] = []

    class Manager:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        async def shutdown(self) -> None:
            self.shutdown_calls += 1

    manager = Manager()

    def run_root_loop(index: int) -> None:
        loop = asyncio.new_event_loop()
        loops[index] = loop
        asyncio.set_event_loop(loop)
        try:
            roots[index] = graph.register_root_owner()
            ready[index].set()
            loop.run_forever()
        except BaseException as exc:
            loop_errors.append(exc)
            ready[index].set()
        finally:
            asyncio.set_event_loop(None)
            loop.close()

    threads = [
        threading.Thread(target=run_root_loop, args=(0,)),
        threading.Thread(target=run_root_loop, args=(1,)),
    ]
    for thread in threads:
        thread.start()

    try:
        assert all(event.wait(timeout=1.0) for event in ready)
        assert loop_errors == []
        assert all(loop is not None for loop in loops)
        assert all(root is not None for root in roots)
        assert graph.root_state == "active"
        assert graph.root_owner_count == 2
        graph.resolve_provider_manager(spec="cross-loop-manager", create=lambda: manager)

        first_release = asyncio.run_coroutine_threadsafe(
            graph.release_root_owner(roots[0]),  # type: ignore[arg-type]
            loops[0],  # type: ignore[arg-type]
        )
        first_release.result(timeout=1.0)

        assert graph.root_state == "active"
        assert graph.root_owner_count == 1
        assert manager.shutdown_calls == 0

        async def use_sibling_root() -> None:
            lease = await controller.acquire()
            controller.release(lease)

        sibling_use = asyncio.run_coroutine_threadsafe(
            use_sibling_root(),
            loops[1],  # type: ignore[arg-type]
        )
        sibling_use.result(timeout=1.0)

        final_release = asyncio.run_coroutine_threadsafe(
            graph.release_root_owner(roots[1]),  # type: ignore[arg-type]
            loops[1],  # type: ignore[arg-type]
        )
        final_release.result(timeout=1.0)

        assert manager.shutdown_calls == 1
        assert graph.root_state == "closed"
        assert graph.root_owner_count == 0
        assert controller.runtime_root_state == "closed"
        assert controller.in_flight == 0
        assert controller.retained == 0
        assert controller.blocking_in_flight == 0
        assert controller.service_owner_in_flight == 0
    finally:
        for loop in loops:
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
        for thread in threads:
            thread.join(timeout=1.0)
            assert thread.is_alive() is False


@pytest.mark.asyncio
async def test_final_root_rejects_queued_work_and_waits_for_the_active_lease() -> None:
    controller = PipelineAdmissionController(1, max_queued=2)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    active = await controller.acquire()
    queued = asyncio.create_task(controller.acquire())
    while controller.queued != 1:
        await asyncio.sleep(0)

    release_task = asyncio.create_task(graph.release_root_owner(root))
    with pytest.raises(PipelineAdmissionRejected):
        await queued

    assert release_task.done() is False
    assert controller.in_flight == 1
    assert controller.queued == 0
    controller.release(active)
    await release_task

    assert graph.root_state == "closed"
    assert controller.in_flight == 0
    assert controller.queued == 0


@pytest.mark.asyncio
async def test_final_root_rejects_a_selected_handoff_before_terminal_return() -> None:
    controller = PipelineAdmissionController(1, max_queued=1)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    active = await controller.acquire()
    queued = asyncio.create_task(controller.acquire())
    while controller.queued != 1:
        await asyncio.sleep(0)

    class DeferredLoop:
        def __init__(self) -> None:
            self.callbacks: list[tuple[Any, tuple[Any, ...]]] = []

        def is_closed(self) -> bool:
            return False

        def is_running(self) -> bool:
            return True

        def call_soon_threadsafe(self, callback, *args) -> None:
            self.callbacks.append((callback, args))

    deferred_loop = DeferredLoop()
    waiter = next(iter(next(iter(controller._queues.values())).values()))
    setattr(waiter, "loop", deferred_loop)
    controller.release(active)
    assert waiter.state == "selected"
    assert controller._selected

    await graph.release_root_owner(root)

    assert waiter.state == "rejected"
    assert controller._selected == {}
    assert controller._selected_maintenance_thread is None
    for callback, args in deferred_loop.callbacks:
        callback(*args)
    with pytest.raises(PipelineAdmissionRejected):
        await queued


@pytest.mark.asyncio
async def test_final_root_fences_submission_before_scheduling_its_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    drain_scheduled = asyncio.Event()
    allow_drain = asyncio.Event()
    drain_final_root = graph._drain_final_root

    async def blocked_drain(generation, manager, provider_recheck_required) -> None:
        drain_scheduled.set()
        await allow_drain.wait()
        await drain_final_root(generation, manager, provider_recheck_required)

    monkeypatch.setattr(graph, "_drain_final_root", blocked_drain)
    release_task = asyncio.create_task(graph.release_root_owner(root))
    await drain_scheduled.wait()

    assert controller.runtime_root_state == "draining"
    with pytest.raises(PipelineAdmissionRejected):
        await controller.acquire()

    allow_drain.set()
    await release_task


@pytest.mark.asyncio
async def test_final_root_drains_a_manager_realized_by_already_admitted_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    admitted = asyncio.Event()
    realize_manager = asyncio.Event()
    manager_realized = asyncio.Event()
    drain_started = asyncio.Event()

    class Manager:
        def __init__(self) -> None:
            self.shutdown_calls = 0

        async def shutdown(self) -> None:
            self.shutdown_calls += 1

    manager = Manager()
    begin_root_drain = controller.begin_root_drain

    def observed_begin_root_drain(generation: int) -> bool:
        result = begin_root_drain(generation)
        drain_started.set()
        return result

    monkeypatch.setattr(controller, "begin_root_drain", observed_begin_root_drain)

    async def admitted_work() -> None:
        async with controller.slot():
            admitted.set()
            await realize_manager.wait()
            graph.resolve_provider_manager(spec="late-manager", create=lambda: manager)
            manager_realized.set()

    work_task = asyncio.create_task(admitted_work())
    await admitted.wait()
    release_task = asyncio.create_task(graph.release_root_owner(root))
    await drain_started.wait()

    realize_manager.set()
    await manager_realized.wait()
    await work_task
    await release_task

    assert manager.shutdown_calls >= 1
    assert graph.provider_manager() is None
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_final_root_waits_for_admitted_provider_paths_before_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix="admitted-provider-paths-before-shutdown",
        pipeline_max_concurrent=2,
    )
    resources, controller, providers = _provider_resources(runtime_settings, limit=2)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    first_admitted = asyncio.Event()
    allow_first_acquisition = asyncio.Event()
    second_ready = asyncio.Event()
    allow_second_operation = asyncio.Event()
    order: list[str] = []
    order_lock = threading.Lock()
    results: dict[str, str] = {}
    path_errors: dict[str, BaseException] = {}

    def record(event: str) -> None:
        with order_lock:
            order.append(event)

    async def close_handle(handle, *, path: str) -> None:
        if handle is None:
            return
        try:
            await resources.close(handle)
        except BaseException as exc:
            path_errors.setdefault(f"{path}-close", exc)

    async def acquire_after_drain_starts() -> None:
        handle = None
        try:
            async with controller.slot():
                try:
                    first_admitted.set()
                    await allow_first_acquisition.wait()
                    handle = await resources.acquire()
                    results["first"] = (await resources.llm().chat_text("system", "first")).text
                except BaseException as exc:
                    path_errors["first"] = exc
                finally:
                    await close_handle(handle, path="first")
                    record("first-ended")
        except BaseException as exc:
            path_errors.setdefault("first-slot", exc)

    async def use_provider_twice() -> None:
        handle = None
        try:
            async with controller.slot():
                try:
                    handle = await resources.acquire()
                    provider = resources.llm()
                    results["second-before-drain"] = (await provider.chat_text("system", "before")).text
                    second_ready.set()
                    await allow_second_operation.wait()
                    results["second-during-drain"] = (await provider.chat_text("system", "during")).text
                except BaseException as exc:
                    path_errors["second"] = exc
                finally:
                    await close_handle(handle, path="second")
                    record("second-ended")
        except BaseException as exc:
            path_errors.setdefault("second-slot", exc)

    assert graph.provider_manager() is resources
    first_task = asyncio.create_task(acquire_after_drain_starts())
    await first_admitted.wait()
    second_task = asyncio.create_task(use_provider_twice())
    await second_ready.wait()
    assert controller.in_flight == 2

    original_shutdown = resources.shutdown

    async def observed_shutdown() -> None:
        record("shutdown-started")
        await original_shutdown()

    monkeypatch.setattr(resources, "shutdown", observed_shutdown)
    release_task = asyncio.create_task(graph.release_root_owner(root))
    while graph.root_state != "draining":
        await asyncio.sleep(0)

    with pytest.raises(PipelineAdmissionRejected):
        await controller.acquire()

    allow_first_acquisition.set()
    allow_second_operation.set()
    await asyncio.gather(first_task, second_task)
    await release_task

    shutdown_positions = [index for index, event in enumerate(order) if event == "shutdown-started"]
    assert len(providers) == 1
    assert providers[0].closed is True
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert graph.provider_manager() is None
    assert controller.in_flight == 0
    assert controller.retained == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0
    outcomes = {
        "shutdown_started": bool(shutdown_positions),
        "shutdown_after_first_path": bool(shutdown_positions)
        and all(position > order.index("first-ended") for position in shutdown_positions),
        "shutdown_after_second_path": bool(shutdown_positions)
        and all(position > order.index("second-ended") for position in shutdown_positions),
        "path_error_types": {path: type(error).__name__ for path, error in path_errors.items()},
        "results": results,
    }
    assert outcomes == {
        "shutdown_started": True,
        "shutdown_after_first_path": True,
        "shutdown_after_second_path": True,
        "path_error_types": {},
        "results": {
            "first": "ok",
            "second-before-drain": "ok",
            "second-during-drain": "ok",
        },
    }


def test_runtime_identity_binding_races_root_registration_without_split_authority() -> None:
    for iteration in range(20):
        controller = PipelineAdmissionController(1, max_queued=0)
        graph = controller.execution_graph
        original_identity = controller.runtime_identity
        requested_identity = f"race-runtime-{iteration}"
        start = threading.Barrier(3)
        handles: list[Any] = []
        bind_errors: list[BaseException] = []
        registration_errors: list[BaseException] = []

        def bind_identity() -> None:
            start.wait()
            try:
                controller.bind_runtime_identity(requested_identity)
            except BaseException as exc:
                bind_errors.append(exc)

        def register_root() -> None:
            start.wait()
            try:
                handles.append(graph.register_root_owner())
            except BaseException as exc:
                registration_errors.append(exc)

        bind_thread = threading.Thread(target=bind_identity)
        register_thread = threading.Thread(target=register_root)
        bind_thread.start()
        register_thread.start()
        start.wait()
        bind_thread.join(timeout=1.0)
        register_thread.join(timeout=1.0)

        assert bind_thread.is_alive() is False
        assert register_thread.is_alive() is False
        assert registration_errors == []
        assert len(handles) == 1
        assert len(bind_errors) <= 1

        handle = handles[0]
        if bind_errors:
            assert isinstance(bind_errors[0], RuntimeOwnershipError)
            assert controller.runtime_identity == original_identity
        else:
            assert controller.runtime_identity == requested_identity
        assert graph.runtime_identity == controller.runtime_identity
        assert handle.graph_nonce == graph.graph_nonce
        assert graph.root_owner_count == 1

        asyncio.run(graph.release_root_owner(handle))

        assert graph.root_state == "closed"
        assert graph.root_owner_count == 0
        assert controller.runtime_root_state == "closed"
        assert controller.in_flight == 0
        assert controller.retained == 0
        assert controller.blocking_in_flight == 0
        assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_distinct_runtime_roots_remain_isolated(tmp_path) -> None:
    first_settings = _settings(tmp_path, suffix="isolated-first")
    second_settings = _settings(tmp_path, suffix="isolated-second")
    first_stores = RuntimeStores(first_settings)
    second_stores = RuntimeStores(second_settings)
    first_lifespan = create_lifespan(first_settings)(_app(first_settings, first_stores))
    second_lifespan = create_lifespan(second_settings)(_app(second_settings, second_stores))

    await first_lifespan.__aenter__()
    await second_lifespan.__aenter__()
    try:
        first_controller = first_stores.pipeline_admission()
        second_controller = second_stores.pipeline_admission()
        assert first_controller is not second_controller
        assert first_controller.execution_graph.root_owner_count == 1
        assert second_controller.execution_graph.root_owner_count == 1

        await first_lifespan.__aexit__(None, None, None)
        first_lifespan = None

        assert first_controller.execution_graph.root_state == "closed"
        assert second_controller.execution_graph.root_state == "active"
        lease = await second_controller.acquire()
        second_controller.release(lease)
    finally:
        if first_lifespan is not None:
            await first_lifespan.__aexit__(None, None, None)
        await second_lifespan.__aexit__(None, None, None)
