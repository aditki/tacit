"""FastAPI lifespan wiring."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from dashforge.config import Settings
from dashforge.config import settings as default_settings

logger = structlog.get_logger()

_slack_task: asyncio.Task | None = None


def create_lifespan(runtime_settings: Settings = default_settings):
    """Create an app lifespan using explicit runtime settings."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        from dashforge.logging import configure_logging

        configure_logging(runtime_settings.log_level)

        global _slack_task
        if runtime_settings.slack_bot_token and runtime_settings.slack_app_token:
            from dashforge.integrations.slack import start_slack_bot

            _slack_task = asyncio.create_task(start_slack_bot(runtime_settings))
            logger.info("slack_bot_scheduled")
        else:
            logger.warning("slack_not_configured", hint="Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN to enable Slack")
        yield
        if _slack_task and not _slack_task.done():
            _slack_task.cancel()

    return lifespan
