"""Standalone Slack composition-root lifecycle matrix.

The Slack transport is fake and all pipeline work is local. These tests assert
runtime ownership and teardown ordering rather than Slack SDK behavior.
"""

from __future__ import annotations

import asyncio
import threading
from asyncio import events as asyncio_events
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi import FastAPI

import tacit.integrations.slack as slack_module
from tacit.api.optional_integrations import OptionalIntegrationLifecycle
from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
from tacit.models.schemas import DashRequest, DashResponse
from tacit.runtime_stores import RuntimeStores


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

    def info(self, event: str, **fields: Any) -> None:
        self._record("info", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._record("warning", event, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        self._record("exception", event, **fields)


async def test_borrowed_slack_root_requires_an_active_composition_root(
    monkeypatch,
    tmp_path,
) -> None:
    """A direct caller cannot opt out of runtime-root ownership."""
    runtime_settings = Settings(
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    stores = RuntimeStores(runtime_settings)

    class UnexpectedSlackApp:
        def __init__(self, **_kwargs):
            raise AssertionError("Slack client constructed without an active runtime root")

    monkeypatch.setattr(slack_module, "AsyncApp", UnexpectedSlackApp)

    with pytest.raises(RuntimeError, match="active runtime root"):
        await slack_module.start_slack_bot(
            runtime_settings,
            stores=stores,
            owns_runtime_root=False,
        )


class _FakeSlackApp:
    def __init__(self, **_kwargs: Any) -> None:
        self.handlers: dict[str, Any] = {}

    def event(self, name: str):
        def register(handler: Any) -> Any:
            self.handlers[name] = handler
            return handler

        return register

    def command(self, name: str):
        def register(handler: Any) -> Any:
            self.handlers[name] = handler
            return handler

        return register


class _SlackCloseFailure(RuntimeError):
    pass


def _settings(tmp_path, *, suffix: str) -> Settings:
    return Settings(  # type: ignore[call-arg]
        _env_file=None,
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=2,
        pipeline_max_queued=0,
        pipeline_timeout_seconds=2.0,
        slack_bot_token="xoxb-test",
        slack_app_token="xapp-test",
        slack_signing_secret="signing-test",
    )


def _response(index: int) -> DashResponse:
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


def _fail_first_lifecycle_loop_construction(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    original_new_event_loop = asyncio_events.new_event_loop
    calls: list[int] = []
    lock = threading.Lock()

    def fail_once() -> asyncio.AbstractEventLoop:
        with lock:
            calls.append(threading.get_ident())
            should_fail = len(calls) == 1
        if should_fail:
            raise RuntimeError("transient lifecycle loop construction failure")
        return original_new_event_loop()

    monkeypatch.setattr(asyncio_events, "new_event_loop", fail_once)
    return calls


async def test_slack_readiness_is_published_only_after_socket_connection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-readiness")
    stores = RuntimeStores(runtime_settings)
    connect_started = asyncio.Event()
    allow_connection = asyncio.Event()
    connected = asyncio.Event()
    readiness_calls: list[str] = []

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def connect_async(self) -> None:
            connect_started.set()
            await allow_connection.wait()
            connected.set()

        async def start_async(self) -> None:
            await self.connect_async()

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)

    slack_task = asyncio.create_task(
        slack_module.start_slack_bot(
            runtime_settings,
            stores=stores,
            on_ready=lambda: readiness_calls.append("ready"),
        )
    )
    await asyncio.wait_for(connect_started.wait(), timeout=1)
    assert readiness_calls == []

    allow_connection.set()
    await asyncio.wait_for(connected.wait(), timeout=1)
    for _ in range(10):
        if readiness_calls:
            break
        await asyncio.sleep(0)
    assert readiness_calls == ["ready"]

    slack_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slack_task, timeout=1)
    _assert_terminal_zero(stores)


async def test_slack_connection_failure_never_publishes_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-readiness-failure")
    stores = RuntimeStores(runtime_settings)
    readiness_calls: list[str] = []

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def connect_async(self) -> None:
            raise RuntimeError("socket startup failed")

        async def start_async(self) -> None:
            await self.connect_async()

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)

    with pytest.raises(RuntimeError, match="socket startup failed"):
        await slack_module.start_slack_bot(
            runtime_settings,
            stores=stores,
            on_ready=lambda: readiness_calls.append("ready"),
        )

    assert readiness_calls == []
    _assert_terminal_zero(stores)


async def test_slack_transport_health_downgrades_and_recovers_after_confirmed_reconnect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-reconnect-health")
    stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    lifecycle = OptionalIntegrationLifecycle(app, name="slack")
    lifecycle.publish(status="starting", reason_code="slack_starting")
    connected = True

    class FakeClient:
        async def is_connected(self) -> bool:
            return connected

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            self.client = FakeClient()

        async def connect_async(self) -> None:
            return None

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS", 0.001)

    slack_task = asyncio.create_task(slack_module.start_slack_bot(runtime_settings, stores=stores, lifecycle=lifecycle))
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "ready":
            break
        await asyncio.sleep(0.001)
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "ready",
        "reason_code": "slack_ready",
    }

    connected = False
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "reconnecting":
            break
        await asyncio.sleep(0.001)
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "reconnecting",
        "reason_code": "slack_reconnecting",
    }

    connected = True
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "ready":
            break
        await asyncio.sleep(0.001)
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "ready",
        "reason_code": "slack_ready",
    }

    slack_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slack_task, timeout=1)
    _assert_terminal_zero(stores)


async def test_slack_transport_probe_failure_degrades_until_a_confirmed_reconnect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-reconnect-failure")
    stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    lifecycle = OptionalIntegrationLifecycle(app, name="slack")
    lifecycle.publish(status="starting", reason_code="slack_starting")
    probe_failure = False

    class FakeClient:
        async def is_connected(self) -> bool:
            if probe_failure:
                raise RuntimeError("private reconnect failure")
            return True

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            self.client = FakeClient()

        async def connect_async(self) -> None:
            return None

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS", 0.001)

    slack_task = asyncio.create_task(slack_module.start_slack_bot(runtime_settings, stores=stores, lifecycle=lifecycle))
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "ready":
            break
        await asyncio.sleep(0.001)

    probe_failure = True
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "failed":
            break
        await asyncio.sleep(0.001)
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "failed",
        "reason_code": "slack_connection_state_unavailable",
    }
    assert "private reconnect failure" not in repr(app.state.optional_integration_readiness)

    probe_failure = False
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "ready":
            break
        await asyncio.sleep(0.001)
    assert app.state.optional_integration_readiness["slack"]["status"] == "ready"

    slack_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slack_task, timeout=1)
    _assert_terminal_zero(stores)


async def test_noncooperative_slack_health_probe_degrades_without_blocking_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-health-probe-timeout")
    stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    lifecycle = OptionalIntegrationLifecycle(app, name="slack")
    lifecycle.publish(status="starting", reason_code="slack_starting")
    probe_started = asyncio.Event()
    probe_cancelled = asyncio.Event()
    release_probe = asyncio.Event()

    class FakeClient:
        async def is_connected(self) -> bool:
            probe_started.set()
            try:
                await release_probe.wait()
            except asyncio.CancelledError:
                probe_cancelled.set()
                await release_probe.wait()
            return True

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            self.client = FakeClient()

        async def connect_async(self) -> None:
            return None

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_PROBE_TIMEOUT_SECONDS", 0.01)

    slack_task = asyncio.create_task(slack_module.start_slack_bot(runtime_settings, stores=stores, lifecycle=lifecycle))
    await asyncio.wait_for(probe_started.wait(), timeout=1)
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "failed":
            break
        await asyncio.sleep(0.001)

    await asyncio.wait_for(probe_cancelled.wait(), timeout=1)
    assert probe_cancelled.is_set()
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "failed",
        "reason_code": "slack_connection_probe_timed_out",
    }
    assert lifecycle.detached_task_count == 1

    slack_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slack_task, timeout=1)
    _assert_terminal_zero(stores)

    release_probe.set()
    for _ in range(20):
        if lifecycle.detached_task_count == 0:
            break
        await asyncio.sleep(0)
    assert lifecycle.detached_task_count == 0


async def test_slack_shutdown_during_reconnect_revokes_late_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-reconnect-shutdown")
    stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    lifecycle = OptionalIntegrationLifecycle(app, name="slack")
    lifecycle.publish(status="starting", reason_code="slack_starting")
    connected = True

    class FakeClient:
        async def is_connected(self) -> bool:
            return connected

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            self.client = FakeClient()

        async def connect_async(self) -> None:
            return None

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS", 0.001)

    slack_task = asyncio.create_task(slack_module.start_slack_bot(runtime_settings, stores=stores, lifecycle=lifecycle))
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "ready":
            break
        await asyncio.sleep(0.001)
    connected = False
    for _ in range(100):
        if app.state.optional_integration_readiness["slack"]["status"] == "reconnecting":
            break
        await asyncio.sleep(0.001)

    slack_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(slack_task, timeout=1)
    lifecycle.publish(status="ready", reason_code="late_slack_ready")

    assert app.state.optional_integration_readiness["slack"] == {
        "status": "stopped",
        "reason_code": "slack_shutdown_completed",
    }
    _assert_terminal_zero(stores)


async def test_standalone_slack_owns_a_durable_root_beside_the_api_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-beside-api")
    stores = RuntimeStores(runtime_settings)
    graph = stores.pipeline_admission().execution_graph
    api_root = stores.start_runtime_services()
    socket_started = asyncio.Event()
    finish_socket = asyncio.Event()
    socket_close_started = asyncio.Event()
    socket_closed = asyncio.Event()

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def start_async(self) -> None:
            socket_started.set()
            await finish_socket.wait()

        async def close_async(self) -> None:
            socket_close_started.set()
            socket_closed.set()

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)

    slack_task = asyncio.create_task(slack_module.start_slack_bot(runtime_settings, stores=stores))
    await asyncio.wait_for(socket_started.wait(), timeout=1)
    roots_while_both_live = graph.root_owner_count

    await stores.shutdown_runtime_services(api_root)
    state_after_api_shutdown = (graph.root_state, graph.root_owner_count)
    socket_was_still_live = not socket_close_started.is_set()

    finish_socket.set()
    await asyncio.wait_for(socket_closed.wait(), timeout=1)
    await asyncio.wait_for(slack_task, timeout=1)

    assert roots_while_both_live == 2
    assert state_after_api_shutdown == ("active", 1)
    assert socket_was_still_live is True
    _assert_terminal_zero(stores)


async def test_standalone_slack_handles_two_events_then_restarts_after_closure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-restart")
    stores = RuntimeStores(runtime_settings)
    graph = stores.pipeline_admission().execution_graph
    sessions = 0
    closed_sessions: list[int] = []
    observed: list[tuple[int, int, int]] = []
    messages: list[dict[str, Any]] = []

    async def say(**payload: Any) -> None:
        messages.append(payload)

    async def fake_run_pipeline(request: DashRequest, _deps: Any) -> DashResponse:
        event_index = int(request.user_id.rsplit("-", 1)[-1])
        observed.append((sessions, graph.root_generation, graph.root_owner_count))
        return _response(event_index)

    class FakeSocketModeHandler:
        def __init__(self, app: _FakeSlackApp, _token: str) -> None:
            nonlocal sessions
            sessions += 1
            self.session = sessions
            self.app = app

        async def start_async(self) -> None:
            mention = self.app.handlers["app_mention"]
            for event_index in (1, 2):
                await mention(
                    {
                        "text": f"<@BOT> checkout request {event_index}",
                        "channel": "channel-1",
                        "user": f"user-{event_index}",
                        "team": "default",
                        "ts": f"{self.session}.{event_index}",
                    },
                    say,
                )

        async def close_async(self) -> None:
            closed_sessions.append(self.session)

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "run_pipeline", fake_run_pipeline)

    await slack_module.start_slack_bot(runtime_settings, stores=stores)
    first_terminal_generation = graph.root_generation
    _assert_terminal_zero(stores)
    await slack_module.start_slack_bot(runtime_settings, stores=stores)

    successful_messages = [message for message in messages if "blocks" in message]
    assert observed == [
        (1, 1, 1),
        (1, 1, 1),
        (2, 2, 1),
        (2, 2, 1),
    ]
    assert first_terminal_generation == 1
    assert closed_sessions == [1, 2]
    assert len(successful_messages) == 4
    _assert_terminal_zero(stores)


async def test_late_slack_handler_cannot_reopen_its_retired_runtime_generation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-late-handler")
    stores = RuntimeStores(runtime_settings)
    graph = stores.pipeline_admission().execution_graph
    captured_apps: list[_FakeSlackApp] = []
    messages: list[dict[str, Any]] = []
    pipeline_calls = 0

    async def say(**payload: Any) -> None:
        messages.append(payload)

    async def fake_run_pipeline(request: DashRequest, deps: Any) -> DashResponse:
        nonlocal pipeline_calls
        del request
        pipeline_calls += 1
        deps.start_runtime_root()
        raise AssertionError("a retired Slack generation reopened the runtime root")

    class FakeSocketModeHandler:
        def __init__(self, app: _FakeSlackApp, _token: str) -> None:
            captured_apps.append(app)

        async def start_async(self) -> None:
            return None

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "run_pipeline", fake_run_pipeline)

    await slack_module.start_slack_bot(runtime_settings, stores=stores)
    retired_generation = graph.root_generation
    _assert_terminal_zero(stores)

    await captured_apps[0].handlers["app_mention"](
        {
            "text": "<@BOT> checkout latency",
            "channel": "channel-1",
            "user": "user-1",
            "team": "default",
            "ts": "1.0",
        },
        say,
    )

    assert graph.root_generation == retired_generation
    _assert_terminal_zero(stores)
    assert pipeline_calls == 0
    assert messages == []


async def test_standalone_slack_retries_same_root_after_lifecycle_loop_preflight_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-drain-preflight-retry")
    stores = RuntimeStores(runtime_settings)
    loop_attempts = _fail_first_lifecycle_loop_construction(monkeypatch)

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def start_async(self) -> None:
            return None

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)

    await asyncio.wait_for(
        slack_module.start_slack_bot(runtime_settings, stores=stores),
        timeout=1.0,
    )

    assert len(loop_attempts) == 2
    _assert_terminal_zero(stores)


async def test_repeated_cancellation_waits_for_socket_close_before_root_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-double-cancel")
    stores = RuntimeStores(runtime_settings)
    graph = stores.pipeline_admission().execution_graph
    socket_started = asyncio.Event()
    socket_close_started = asyncio.Event()
    allow_socket_close = asyncio.Event()
    socket_close_finished = asyncio.Event()

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def start_async(self) -> None:
            socket_started.set()
            await asyncio.Event().wait()

        async def close_async(self) -> None:
            socket_close_started.set()
            await allow_socket_close.wait()
            socket_close_finished.set()

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)

    slack_task = asyncio.create_task(slack_module.start_slack_bot(runtime_settings, stores=stores))
    await asyncio.wait_for(socket_started.wait(), timeout=1)
    slack_task.cancel()
    await asyncio.wait_for(socket_close_started.wait(), timeout=1)
    slack_task.cancel()
    await asyncio.sleep(0)

    task_finished_before_close = slack_task.done()
    root_before_close = (graph.root_state, graph.root_owner_count)

    allow_socket_close.set()
    try:
        await asyncio.wait_for(slack_task, timeout=1)
    except asyncio.CancelledError:
        pass

    assert task_finished_before_close is False
    assert root_before_close == ("active", 1)
    assert socket_close_finished.is_set()
    _assert_terminal_zero(stores)


async def test_socket_close_failure_is_observable_without_logging_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-close-failure")
    stores = RuntimeStores(runtime_settings)
    logs = _RecordingLogger()
    secret = "SLACK_CLOSE_SECRET=/private/runtime/socket"
    close_failure = _SlackCloseFailure(secret)

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def start_async(self) -> None:
            return None

        async def close_async(self) -> None:
            raise close_failure

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "logger", logs)

    with pytest.raises(_SlackCloseFailure) as observed:
        await slack_module.start_slack_bot(runtime_settings, stores=stores)

    cleanup_records = [record for record in logs.records if record.event == "slack_socket_cleanup_failed"]
    assert observed.value is close_failure
    assert len(cleanup_records) == 1
    assert cleanup_records[0].fields == {
        "reason_code": "slack_socket_cleanup_failed",
        "error_type": "_SlackCloseFailure",
    }
    rendered_logs = repr(logs.records)
    assert secret not in rendered_logs
    assert "/private/runtime/socket" not in rendered_logs
    assert "exc_info" not in rendered_logs
    assert "Traceback" not in rendered_logs
    _assert_terminal_zero(stores)


async def test_noncooperative_socket_close_is_bounded_and_releases_runtime_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-close-timeout")
    stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    lifecycle = OptionalIntegrationLifecycle(app, name="slack")
    close_started = asyncio.Event()
    close_cancelled = asyncio.Event()
    release_close = asyncio.Event()

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def connect_async(self) -> None:
            return None

        async def start_async(self) -> None:
            return None

        async def close_async(self) -> None:
            close_started.set()
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                await release_close.wait()

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CLOSE_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(slack_module.SlackOptionalIntegrationShutdownTimeout):
        await asyncio.wait_for(
            slack_module.start_slack_bot(runtime_settings, stores=stores, lifecycle=lifecycle),
            timeout=0.5,
        )

    assert close_started.is_set()
    assert close_cancelled.is_set()
    assert app.state.optional_integration_readiness["slack"] == {
        "status": "failed",
        "reason_code": "slack_shutdown_timed_out",
    }
    _assert_terminal_zero(stores)

    release_close.set()
    for _ in range(20):
        if lifecycle.detached_task_count == 0:
            break
        await asyncio.sleep(0)
    assert lifecycle.detached_task_count == 0


async def test_startup_failure_preserves_unretired_socket_marker_through_cleanup_wrapper(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(tmp_path, suffix="slack-wrapped-close-timeout")
    stores = RuntimeStores(runtime_settings)
    lifecycle = OptionalIntegrationLifecycle(None, name="slack")
    close_cancelled = asyncio.Event()
    release_close = asyncio.Event()

    class FakeSocketModeHandler:
        def __init__(self, _app: Any, _token: str) -> None:
            pass

        async def connect_async(self) -> None:
            raise RuntimeError("socket startup failed")

        async def close_async(self) -> None:
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                await release_close.wait()

    monkeypatch.setattr(slack_module, "AsyncApp", _FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketModeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CLOSE_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(RuntimeOwnershipError) as observed:
        await slack_module.start_slack_bot(
            runtime_settings,
            stores=stores,
            lifecycle=lifecycle,
        )

    assert close_cancelled.is_set()
    assert getattr(observed.value, "optional_integration_resources_unretired", False)
    release_close.set()
    for _ in range(20):
        if lifecycle.detached_task_count == 0:
            break
        await asyncio.sleep(0)
    assert lifecycle.detached_task_count == 0
    _assert_terminal_zero(stores)
