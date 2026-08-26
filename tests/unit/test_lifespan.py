from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from tacit.api.lifespan import create_lifespan
from tacit.config import Settings
from tacit.errors import PipelineAdmissionRejected
from tacit.integrations.slack import create_slack_app, handle_mention, handle_slash_command, start_slack_bot
from tacit.models.schemas import DashResponse
from tacit.runtime_ownership import RuntimeOwnershipMismatchError
from tacit.runtime_stores import RuntimeStores


async def test_lifespan_starts_slack_with_runtime_settings(monkeypatch):
    runtime_settings = Settings(
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    seen_settings: list[Settings] = []
    started = asyncio.Event()

    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores

    async def fake_start_slack_bot(settings_arg: Settings, *, stores):
        seen_settings.append(settings_arg)
        assert stores is runtime_stores
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=fake_start_slack_bot),
    )

    async with create_lifespan(runtime_settings)(app):
        await asyncio.wait_for(started.wait(), timeout=1)

    assert seen_settings == [runtime_settings]


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

    def fake_build(_settings, *, stores):
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

    monkeypatch.setattr("tacit.integrations.slack.AsyncApp", FakeSlackApp)
    monkeypatch.setattr("tacit.integrations.slack.AsyncSocketModeHandler", FakeSocketHandler)

    await start_slack_bot(runtime_settings, stores=stores)

    assert [name for name, _value in constructed] == ["app", "socket", "started"]
