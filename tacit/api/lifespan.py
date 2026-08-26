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


def _validate_lifespan_runtime(app: FastAPI, runtime_settings: Settings):
    """Validate the app, lifespan, and store owners before startup effects."""
    from tacit.runtime_ownership import (
        get_runtime_ownership,
        require_compatible_runtime_ownership,
        runtime_descriptor_from_settings,
    )
    from tacit.runtime_stores import RuntimeStores

    app_settings = getattr(app.state, "settings", None)
    if app_settings is None:
        app_settings = runtime_settings
        app.state.settings = app_settings

    settings_descriptors = (
        runtime_descriptor_from_settings(runtime_settings, component="lifespan_settings"),
        runtime_descriptor_from_settings(app_settings, component="app_settings"),
    )
    require_compatible_runtime_ownership(
        boundary="API lifespan runtime",
        descriptors=settings_descriptors,
    )

    runtime_stores = getattr(app.state, "runtime_stores", None)
    if runtime_stores is None:
        runtime_stores = RuntimeStores(app_settings)
        app.state.runtime_stores = runtime_stores
    require_compatible_runtime_ownership(
        boundary="API lifespan runtime",
        descriptors=(*settings_descriptors, get_runtime_ownership(runtime_stores, component="runtime_stores")),
    )
    return runtime_stores


def create_lifespan(runtime_settings: Settings = default_settings):
    """Create an app lifespan using explicit runtime settings."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        from tacit.logging import configure_logging

        runtime_stores = _validate_lifespan_runtime(app, runtime_settings)
        configure_logging(runtime_settings.log_level)

        slack_task: asyncio.Task | None = None
        if runtime_settings.slack_bot_token and runtime_settings.slack_app_token:
            from tacit.integrations.slack import start_slack_bot

            slack_task = asyncio.create_task(start_slack_bot(runtime_settings, stores=runtime_stores))
            logger.info("slack_bot_scheduled")
        else:
            logger.warning("slack_not_configured", hint="Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN to enable Slack")
        yield
        if slack_task and not slack_task.done():
            slack_task.cancel()

    return lifespan
