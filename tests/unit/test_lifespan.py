from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

from fastapi import FastAPI

from dashforge.api.lifespan import create_lifespan
from dashforge.config import Settings


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
