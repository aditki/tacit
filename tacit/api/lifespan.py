"""FastAPI lifespan wiring."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from tacit.config import Settings
from tacit.config import settings as default_settings

logger = structlog.get_logger()


def create_lifespan(runtime_settings: Settings = default_settings):
    """Create an app lifespan using explicit runtime settings."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        from tacit.logging import configure_logging

        configure_logging(runtime_settings.log_level)

        slack_task: asyncio.Task | None = None
        if runtime_settings.slack_bot_token and runtime_settings.slack_app_token:
            from tacit.integrations.slack import start_slack_bot

            slack_task = asyncio.create_task(start_slack_bot(runtime_settings))
            logger.info("slack_bot_scheduled")
        else:
            logger.warning("slack_not_configured", hint="Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN to enable Slack")
        yield
        if slack_task and not slack_task.done():
            slack_task.cancel()

    return lifespan
