from __future__ import annotations

import asyncio
import sys
import threading
from asyncio import events as asyncio_events
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

import tacit.api.optional_integrations as optional_integrations_module
from tacit.api.lifespan import create_lifespan
from tacit.api.routes.system import healthz
from tacit.config import Settings
from tacit.errors import PipelineAdmissionRejected
from tacit.integrations.slack import create_slack_app, handle_mention, handle_slash_command, start_slack_bot
from tacit.models.schemas import DashResponse
from tacit.runtime_ownership import RuntimeOwnershipMismatchError
from tacit.runtime_stores import RuntimeStoreReadiness, RuntimeStoreReadinessError, RuntimeStores


async def _wait_for_thread_event(event: threading.Event, *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not event.is_set():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("thread event did not become set before its deadline")
        await asyncio.sleep(0.001)


def _fail_first_lifecycle_loop_construction(monkeypatch) -> list[int]:
    original_new_event_loop = asyncio_events.new_event_loop
    calls: list[int] = []
    lock = threading.Lock()

    def fail_once():
        with lock:
            calls.append(threading.get_ident())
            should_fail = len(calls) == 1
        if should_fail:
            raise RuntimeError("transient lifecycle loop construction failure")
        return original_new_event_loop()

    monkeypatch.setattr(asyncio_events, "new_event_loop", fail_once)
    return calls


async def test_lifespan_prepares_required_stores_before_runtime_services(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    events: list[str] = []
    readiness = RuntimeStoreReadiness(
        ready=True,
        prepared_roles=("history", "feedback", "signals", "knowledge"),
        snapshot_max_bytes=1024,
        snapshot_copy_count=1,
        snapshot_copy_bytes=512,
        snapshot_copy_roles=("signals",),
        duration_ms=1.25,
    )
    original_start = runtime_stores.start_runtime_services

    def prepare_required_stores() -> RuntimeStoreReadiness:
        events.append("stores_ready")
        return readiness

    def start_runtime_services():
        assert getattr(app.state, "runtime_store_readiness", None) is readiness
        handle = original_start()
        events.append("runtime_started")
        return handle

    monkeypatch.setattr(runtime_stores, "prepare_required_stores", prepare_required_stores)
    monkeypatch.setattr(runtime_stores, "start_runtime_services", start_runtime_services)

    async with create_lifespan(runtime_settings)(app):
        assert events == ["stores_ready", "stores_ready", "runtime_started"]

    assert app.state.runtime_store_readiness is readiness


async def test_lifespan_store_readiness_failure_precedes_runtime_and_optional_work(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    slack_started = False

    def fail_readiness() -> RuntimeStoreReadiness:
        raise RuntimeStoreReadinessError(
            role="signals",
            cause_reason_code="sqlite_admission_snapshot_limit",
        )

    def forbidden_runtime_start():
        raise AssertionError("runtime services started before store readiness")

    async def forbidden_slack_start(*_args, **_kwargs):
        nonlocal slack_started
        slack_started = True

    monkeypatch.setattr(runtime_stores, "prepare_required_stores", fail_readiness)
    monkeypatch.setattr(runtime_stores, "start_runtime_services", forbidden_runtime_start)
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=forbidden_slack_start),
    )

    with pytest.raises(RuntimeStoreReadinessError) as exc_info:
        async with create_lifespan(runtime_settings)(app):
            pytest.fail("unready application accepted traffic")

    assert exc_info.value.role == "signals"
    assert slack_started is False
    assert not hasattr(app.state, "runtime_store_readiness")


async def test_lifespan_starts_slack_with_runtime_settings(monkeypatch):
    runtime_settings = Settings(
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    seen_settings: list[Settings] = []
    started = threading.Event()

    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores

    async def fake_start_slack_bot(settings_arg: Settings, *, stores, on_ready):
        seen_settings.append(settings_arg)
        assert stores is runtime_stores
        started.set()
        on_ready()
        await asyncio.Event().wait()

    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=fake_start_slack_bot),
    )

    async with create_lifespan(runtime_settings)(app):
        await _wait_for_thread_event(started)

    assert seen_settings == [runtime_settings]


async def test_lifespan_reports_slack_starting_until_socket_connection_is_confirmed(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    connect_started = threading.Event()
    allow_connection = threading.Event()
    connected = threading.Event()

    async def blocked_slack_bot(_settings_arg: Settings, *, stores, on_ready):
        assert stores is runtime_stores
        connect_started.set()
        while not allow_connection.is_set():
            await asyncio.sleep(0.001)
        on_ready()
        connected.set()
        await asyncio.Event().wait()

    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=blocked_slack_bot),
    )

    async with create_lifespan(runtime_settings)(app):
        await _wait_for_thread_event(connect_started)
        assert app.state.optional_integration_readiness["slack"] == {
            "status": "starting",
            "reason_code": "slack_starting",
        }
        health = await healthz(SimpleNamespace(app=app))
        assert health["status"] == "ok"
        assert health["degraded"] is True
        assert health["optional_integrations"] == {
            "slack": {
                "status": "starting",
                "reason_code": "slack_starting",
            }
        }

        allow_connection.set()
        await _wait_for_thread_event(connected)
        for _ in range(100):
            if app.state.optional_integration_readiness["slack"]["status"] == "ready":
                break
            await asyncio.sleep(0.001)
        assert app.state.optional_integration_readiness["slack"] == {
            "status": "ready",
            "reason_code": "slack_ready",
        }
        assert (await healthz(SimpleNamespace(app=app)))["status"] == "ok"


async def test_lifespan_observes_slack_background_failure_in_optional_readiness(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    started = threading.Event()

    async def failed_slack_bot(_settings_arg: Settings, *, stores, on_ready):
        assert stores is runtime_stores
        started.set()
        raise RuntimeError("private-slack-failure-canary")

    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=failed_slack_bot),
    )

    async with create_lifespan(runtime_settings)(app):
        await _wait_for_thread_event(started)
        for _ in range(20):
            await asyncio.sleep(0)
            readiness = getattr(app.state, "optional_integration_readiness", {})
            if readiness.get("slack", {}).get("status") == "failed":
                break

        slack_readiness = app.state.optional_integration_readiness["slack"]
        assert slack_readiness["status"] == "failed"
        assert slack_readiness["reason_code"] == "slack_background_task_failed"
        assert "private-slack-failure-canary" not in repr(slack_readiness)
        health = await healthz(SimpleNamespace(app=app))
        assert health["status"] == "ok"
        assert health["degraded"] is True
        assert health["optional_integrations"] == {
            "slack": {
                "status": "failed",
                "reason_code": "slack_background_task_failed",
            }
        }

    assert app.state.optional_integration_readiness["slack"] == slack_readiness


async def test_lifespan_drains_the_runtime_provider_manager_on_shutdown(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    shutdown_calls: list[str] = []

    class ProviderManagerProbe:
        async def shutdown(self) -> None:
            shutdown_calls.append("shutdown")

    runtime_stores.pipeline_admission().execution_graph.resolve_provider_manager(
        spec="provider-manager-probe",
        create=ProviderManagerProbe,
    )
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores

    async with create_lifespan(runtime_settings)(app):
        assert shutdown_calls == []

    assert shutdown_calls == ["shutdown"]


@pytest.mark.parametrize("mismatch", ["lifespan_settings", "runtime_stores"])
async def test_lifespan_rejects_runtime_owner_mismatch_before_slack_task_scheduling(
    mismatch,
    monkeypatch,
):
    app_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
    )
    conflicting_settings = app_settings.model_copy(update={"knowledge_tenant_id": "tenant-b"})
    lifespan_settings = conflicting_settings if mismatch == "lifespan_settings" else app_settings
    store_settings = conflicting_settings if mismatch == "runtime_stores" else app_settings
    app = FastAPI()
    app.state.settings = app_settings
    app.state.runtime_stores = RuntimeStores(store_settings)
    task_calls: list[object] = []
    slack_start_calls: list[object] = []

    def reject_task_creation(coroutine):
        task_calls.append(coroutine)
        raise AssertionError("Slack task scheduled before runtime ownership validation")

    async def fake_start_slack_bot(*args, **kwargs):
        slack_start_calls.append((args, kwargs))

    monkeypatch.setattr(asyncio, "create_task", reject_task_creation)
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=fake_start_slack_bot),
    )

    with pytest.raises(RuntimeOwnershipMismatchError, match="runtime"):
        async with create_lifespan(lifespan_settings)(app):
            pytest.fail("mismatched lifespan started")

    assert task_calls == []
    assert slack_start_calls == []


@pytest.mark.parametrize("entry_point", ["mention", "slash"])
async def test_slack_admission_rejection_uses_safe_capacity_message_without_exception_log(
    entry_point,
    monkeypatch,
):
    messages: list[dict] = []
    exception_logs: list[tuple] = []

    async def reject_overload(*_args, **_kwargs):
        raise PipelineAdmissionRejected("pipeline_admission_queue_full")

    async def fake_say(**kwargs):
        messages.append(kwargs)

    async def fake_ack():
        return None

    monkeypatch.setattr("tacit.integrations.slack.run_pipeline", reject_overload)
    monkeypatch.setattr(
        "tacit.integrations.slack.logger.exception",
        lambda *args, **kwargs: exception_logs.append((args, kwargs)),
    )

    def deps_factory():
        return SimpleNamespace(settings=Settings(_env_file=None))

    if entry_point == "mention":
        await handle_mention(
            {"text": "<@BOT> checkout latency", "channel": "C1", "user": "U1", "ts": "1.0"},
            fake_say,
            deps_factory=deps_factory,
        )
    else:
        await handle_slash_command(
            fake_ack,
            {"text": "checkout latency", "channel_id": "C1", "user_id": "U1"},
            fake_say,
            deps_factory=deps_factory,
        )

    assert messages[-1]["text"] == PipelineAdmissionRejected("pipeline_admission_queue_full").public_message()
    assert exception_logs == []


async def test_slack_mention_handler_passes_runtime_dependencies(monkeypatch):
    dependency_bundle = object()
    seen_deps: list[object] = []
    messages: list[dict] = []

    async def fake_run_pipeline(request, deps=None):
        seen_deps.append(deps)
        return DashResponse(
            dashboard_url="http://dash",
            dashboard_uid="dash-1",
            panel_count=1,
            summary=request.prompt,
        )

    async def fake_say(**kwargs):
        messages.append(kwargs)

    monkeypatch.setattr("tacit.integrations.slack.run_pipeline", fake_run_pipeline)

    await handle_mention(
        {"text": "<@BOT> checkout latency", "channel": "C1", "user": "U1", "ts": "1.0"},
        fake_say,
        deps_factory=lambda: dependency_bundle,
    )

    assert seen_deps == [dependency_bundle]
    assert messages[-1]["text"] == "checkout latency"


async def test_slack_mention_uses_team_as_wildcard_tenant(monkeypatch):
    dependency_bundle = SimpleNamespace(settings=SimpleNamespace(knowledge_tenant_id="*"))
    seen_tenant: list[str] = []

    async def fake_run_pipeline(request, deps=None):
        seen_tenant.append(request.tenant_id)
        return DashResponse(dashboard_url="", dashboard_uid="", panel_count=0, summary=request.prompt)

    async def fake_say(**kwargs):
        return None

    monkeypatch.setattr("tacit.integrations.slack.run_pipeline", fake_run_pipeline)

    await handle_mention(
        {
            "text": "<@BOT> checkout latency",
            "channel": "C1",
            "user": "U1",
            "team": "tenant-a",
            "ts": "1.0",
        },
        fake_say,
        deps_factory=lambda: dependency_bundle,
    )

    assert seen_tenant == ["tenant-a"]


async def test_slack_app_reuses_one_runtime_owner_for_every_event(monkeypatch):
    handlers: dict[str, object] = {}
    seen_stores: list[object] = []

    class FakeSlackApp:
        def __init__(self, **_kwargs):
            pass

        def event(self, name):
            def register(handler):
                handlers[name] = handler
                return handler

            return register

        def command(self, name):
            def register(handler):
                handlers[name] = handler
                return handler

            return register

    def fake_build(_settings, *, stores, required_runtime_root_generation=None):
        assert required_runtime_root_generation is None
        seen_stores.append(stores)
        return SimpleNamespace(settings=_settings)

    async def fake_mention(_event, _say, *, deps_factory):
        deps_factory()

    async def fake_slash(_ack, _command, _say, *, deps_factory):
        deps_factory()

    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", FakeSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.build_pipeline_dependencies", fake_build)
    monkeypatch.setattr("tacit.integrations.slack.handle_mention", fake_mention)
    monkeypatch.setattr("tacit.integrations.slack.handle_slash_command", fake_slash)

    create_slack_app(Settings(_env_file=None))
    mention = handlers["app_mention"]
    slash = handlers["/tacit"]

    assert callable(mention)
    assert callable(slash)
    await mention({}, object())
    await slash(object(), {}, object())
    assert len(seen_stores) == 2
    assert seen_stores[0] is seen_stores[1]


async def test_slack_event_bundles_share_the_runtime_provider_resource(monkeypatch, tmp_path):
    handlers: dict[str, object] = {}
    bundles: list[object] = []

    class FakeSlackApp:
        def __init__(self, **_kwargs):
            pass

        def event(self, name):
            def register(handler):
                handlers[name] = handler
                return handler

            return register

        def command(self, name):
            def register(handler):
                handlers[name] = handler
                return handler

            return register

    async def fake_mention(_event, _say, *, deps_factory):
        bundles.append(deps_factory())

    async def fake_slash(_ack, _command, _say, *, deps_factory):
        bundles.append(deps_factory())

    runtime_settings = Settings(
        _env_file=None,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    stores = RuntimeStores(runtime_settings)
    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", FakeSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.handle_mention", fake_mention)
    monkeypatch.setattr("tacit.integrations.slack.handle_slash_command", fake_slash)

    create_slack_app(runtime_settings, stores=stores)
    mention = handlers["app_mention"]
    slash = handlers["/tacit"]
    assert callable(mention)
    assert callable(slash)
    await mention({}, object())
    await slash(object(), {}, object())

    assert len(bundles) == 2
    first, second = bundles
    assert first.pipeline_admission is second.pipeline_admission
    assert first.resource_acquire.__self__ is second.resource_acquire.__self__
    assert first.resource_cleanup.__self__ is second.resource_cleanup.__self__


async def test_slack_startup_rejects_store_mismatch_before_client_construction(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
    )
    conflicting_settings = runtime_settings.model_copy(update={"knowledge_tenant_id": "tenant-b"})
    stores = RuntimeStores(conflicting_settings)
    constructed: list[str] = []

    class UnexpectedSlackApp:
        def __init__(self, **_kwargs):
            constructed.append("app")

    class UnexpectedSocketHandler:
        def __init__(self, *_args, **_kwargs):
            constructed.append("socket")

    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", UnexpectedSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.AsyncSocketModeHandler", UnexpectedSocketHandler)

    with pytest.raises(RuntimeOwnershipMismatchError, match="Slack"):
        await start_slack_bot(runtime_settings, stores=stores)

    assert constructed == []


async def test_slack_startup_constructs_clients_after_runtime_validation(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    stores = RuntimeStores(runtime_settings)
    constructed: list[tuple[str, object]] = []

    class FakeSlackApp:
        def __init__(self, **kwargs):
            constructed.append(("app", kwargs["token"]))

        def event(self, _name):
            return lambda handler: handler

        def command(self, _name):
            return lambda handler: handler

    class FakeSocketHandler:
        def __init__(self, slack_app, token):
            constructed.append(("socket", token))
            self.slack_app = slack_app

        async def start_async(self):
            constructed.append(("started", self.slack_app))

        async def close_async(self):
            constructed.append(("closed", self.slack_app))

    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", FakeSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.AsyncSocketModeHandler", FakeSocketHandler)

    await start_slack_bot(runtime_settings, stores=stores)

    assert [name for name, _value in constructed] == ["app", "socket", "started", "closed"]


@pytest.mark.parametrize("terminal", ["return", "failure"])
async def test_slack_socket_handler_closes_exactly_once_after_start_terminal_state(
    terminal,
    monkeypatch,
):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    stores = RuntimeStores(runtime_settings)
    close_calls: list[str] = []

    class FakeSlackApp:
        def __init__(self, **_kwargs):
            pass

        def event(self, _name):
            return lambda handler: handler

        def command(self, _name):
            return lambda handler: handler

    class FakeSocketHandler:
        def __init__(self, _slack_app, _token):
            pass

        async def start_async(self):
            if terminal == "failure":
                raise RuntimeError("socket startup failed")

        async def close_async(self):
            close_calls.append("closed")

    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", FakeSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.AsyncSocketModeHandler", FakeSocketHandler)

    if terminal == "failure":
        with pytest.raises(RuntimeError, match="socket startup failed"):
            await start_slack_bot(runtime_settings, stores=stores)
    else:
        await start_slack_bot(runtime_settings, stores=stores)

    assert close_calls == ["closed"]


async def test_slack_socket_handler_closes_exactly_once_on_cancellation(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    stores = RuntimeStores(runtime_settings)
    started = asyncio.Event()
    close_calls: list[str] = []

    class FakeSlackApp:
        def __init__(self, **_kwargs):
            pass

        def event(self, _name):
            return lambda handler: handler

        def command(self, _name):
            return lambda handler: handler

    class FakeSocketHandler:
        def __init__(self, _slack_app, _token):
            pass

        async def start_async(self):
            started.set()
            await asyncio.Event().wait()

        async def close_async(self):
            close_calls.append("closed")

    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", FakeSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.AsyncSocketModeHandler", FakeSocketHandler)

    task = asyncio.create_task(start_slack_bot(runtime_settings, stores=stores))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert close_calls == ["closed"]


async def test_lifespan_closes_slack_before_runtime_services(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    started = threading.Event()
    events: list[str] = []

    async def fake_start_slack_bot(_settings_arg, *, stores, on_ready):
        assert stores is runtime_stores
        on_ready()
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("slack_closed")

    original_shutdown = runtime_stores.shutdown_runtime_services

    async def fake_shutdown_runtime_services(handle):
        events.append("runtime_shutdown")
        await original_shutdown(handle)

    monkeypatch.setattr(runtime_stores, "shutdown_runtime_services", fake_shutdown_runtime_services)
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=fake_start_slack_bot),
    )

    async with create_lifespan(runtime_settings)(app):
        await _wait_for_thread_event(started)

    assert events == ["slack_closed", "runtime_shutdown"]


async def test_lifespan_retries_same_root_after_lifecycle_loop_preflight_failure(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    loop_attempts = _fail_first_lifecycle_loop_construction(monkeypatch)

    async with create_lifespan(runtime_settings)(app):
        assert runtime_stores.pipeline_admission().execution_graph.root_owner_count == 1

    admission = runtime_stores.pipeline_admission()
    graph = admission.execution_graph
    assert len(loop_attempts) == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert admission.runtime_root_state == "closed"
    assert admission.in_flight == 0
    assert admission.queued == 0
    assert admission.retained == 0
    assert admission.blocking_in_flight == 0
    assert admission.service_owner_in_flight == 0


async def test_lifespan_shutdown_cancellation_preserves_slack_failure_and_runtime_cleanup(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    lifespan_entered = asyncio.Event()
    slack_started = threading.Event()
    slack_cleanup_started = threading.Event()
    release_slack_cleanup = threading.Event()
    runtime_shutdown_completed = asyncio.Event()

    async def failed_during_shutdown(_settings_arg: Settings, *, stores, on_ready):
        assert stores is runtime_stores
        on_ready()
        slack_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            slack_cleanup_started.set()
            while not release_slack_cleanup.is_set():
                try:
                    await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    # A second cancellation is part of the fault matrix. The
                    # owner remains charged until the operation really settles.
                    pass
            raise RuntimeError("private-slack-shutdown-canary")

    original_shutdown = runtime_stores.shutdown_runtime_services

    async def observed_runtime_shutdown(handle):
        try:
            await original_shutdown(handle)
        finally:
            runtime_shutdown_completed.set()

    monkeypatch.setattr(runtime_stores, "shutdown_runtime_services", observed_runtime_shutdown)
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=failed_during_shutdown),
    )

    async def run_application_lifespan() -> None:
        async with create_lifespan(runtime_settings)(app):
            lifespan_entered.set()
            await asyncio.Event().wait()

    application_task = asyncio.create_task(run_application_lifespan())
    await asyncio.wait_for(lifespan_entered.wait(), timeout=1)
    await _wait_for_thread_event(slack_started)

    application_task.cancel()
    await _wait_for_thread_event(slack_cleanup_started)
    application_task.cancel()
    release_slack_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(application_task, timeout=2)

    assert runtime_shutdown_completed.is_set()
    slack_readiness = app.state.optional_integration_readiness["slack"]
    assert slack_readiness["status"] == "failed"
    assert slack_readiness["reason_code"] == "slack_background_task_failed"
    assert "private-slack-shutdown-canary" not in repr(slack_readiness)


async def test_lifespan_bounds_noncooperative_slack_shutdown_before_mandatory_root_cleanup(
    monkeypatch,
    tmp_path,
):
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    slack_started = threading.Event()
    slack_cancelled = threading.Event()
    release_slack = threading.Event()
    runtime_shutdown_completed = asyncio.Event()
    captured_lifecycle = None

    async def noncooperative_slack(_settings_arg: Settings, *, stores, lifecycle):
        nonlocal captured_lifecycle
        assert stores is runtime_stores
        captured_lifecycle = lifecycle
        lifecycle.publish(status="ready", reason_code="slack_ready")
        slack_started.set()
        try:
            while not release_slack.is_set():
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            slack_cancelled.set()
            while not release_slack.is_set():
                try:
                    await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    pass
        lifecycle.publish(status="ready", reason_code="late_slack_ready")

    original_shutdown = runtime_stores.shutdown_runtime_services

    async def observed_runtime_shutdown(handle):
        try:
            await original_shutdown(handle)
        finally:
            runtime_shutdown_completed.set()

    monkeypatch.setattr(runtime_stores, "shutdown_runtime_services", observed_runtime_shutdown)
    monkeypatch.setattr("tacit.api.lifespan._OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=noncooperative_slack),
    )

    async with create_lifespan(runtime_settings)(app):
        await _wait_for_thread_event(slack_started)

    assert slack_cancelled.is_set()
    assert runtime_shutdown_completed.is_set()
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "failed",
        "reason_code": "slack_shutdown_timed_out",
    }
    graph = runtime_stores.pipeline_admission().execution_graph
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0

    assert captured_lifecycle is not None
    runtime_identity = runtime_stores.runtime_ownership.admission_namespace
    assert runtime_identity is not None
    execution_key = ("slack", runtime_identity)
    execution_state = optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS[execution_key]
    release_slack.set()
    await _wait_for_thread_event(execution_state.finished)
    assert captured_lifecycle.detached_task_count == 0
    assert execution_key not in optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    assert execution_key in optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "failed",
        "reason_code": "slack_shutdown_timed_out",
    }
