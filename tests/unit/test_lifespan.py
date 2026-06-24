from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

from fastapi import FastAPI

from dashforge.api.lifespan import create_lifespan
from dashforge.config import Settings
from dashforge.integrations.slack import handle_mention
from dashforge.models.schemas import DashResponse


async def test_lifespan_starts_slack_with_runtime_settings(monkeypatch):
    runtime_settings = Settings(
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
    )
    seen_settings: list[Settings] = []
    started = asyncio.Event()

    async def fake_start_slack_bot(settings_arg: Settings):
        seen_settings.append(settings_arg)
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setitem(
        sys.modules,
        "dashforge.integrations.slack",
        SimpleNamespace(start_slack_bot=fake_start_slack_bot),
    )

    async with create_lifespan(runtime_settings)(FastAPI()):
        await asyncio.wait_for(started.wait(), timeout=1)

    assert seen_settings == [runtime_settings]


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

    monkeypatch.setattr("dashforge.integrations.slack.run_pipeline", fake_run_pipeline)

    await handle_mention(
        {"text": "<@BOT> checkout latency", "channel": "C1", "user": "U1", "ts": "1.0"},
        fake_say,
        deps_factory=lambda: dependency_bundle,
    )

    assert seen_deps == [dependency_bundle]
    assert messages[-1]["text"] == "checkout latency"
