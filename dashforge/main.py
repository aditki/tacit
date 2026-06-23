"""DashForge FastAPI application entrypoint."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI

from dashforge.api.app import create_app
from dashforge.api.routes import include_routes
from dashforge.api.routes.archetypes import list_archetypes, reload_archetypes_endpoint
from dashforge.api.routes.dashboard import create_chart
from dashforge.api.routes.feedback import get_feedback, get_feedback_analysis, get_feedback_stats, submit_feedback
from dashforge.api.routes.history import get_investigation, investigation_stats, list_investigations
from dashforge.api.routes.learning import (
    approve_ingested_dashboard,
    describe_service,
    ignore_ingested_dashboard,
    learn_backend,
    learn_from_dashboard,
    learn_from_dashboard_json,
    list_ingested_dashboards,
    reject_ingested_dashboard,
    search_learning_context,
)
from dashforge.api.routes.signals import get_signal, list_signals, signal_stats, teach_signal
from dashforge.api.routes.system import healthz, web_ui
from dashforge.api.security import MAX_PROMPT_LENGTH as _MAX_PROMPT_LENGTH
from dashforge.api.security import sanitize_prompt
from dashforge.config import settings
from dashforge.feedback import get_feedback_store

logger = structlog.get_logger()

_slack_task: asyncio.Task | None = None

# Backward-compatible aliases for tests/importers that used the old entrypoint-local helpers.
MAX_PROMPT_LENGTH = _MAX_PROMPT_LENGTH
_sanitize_prompt = sanitize_prompt

__all__ = [
    "MAX_PROMPT_LENGTH",
    "_sanitize_prompt",
    "app",
    "approve_ingested_dashboard",
    "create_chart",
    "describe_service",
    "get_feedback",
    "get_feedback_analysis",
    "get_feedback_stats",
    "get_feedback_store",
    "get_investigation",
    "get_signal",
    "healthz",
    "ignore_ingested_dashboard",
    "investigation_stats",
    "learn_backend",
    "learn_from_dashboard",
    "learn_from_dashboard_json",
    "list_archetypes",
    "list_ingested_dashboards",
    "list_investigations",
    "list_signals",
    "main",
    "reject_ingested_dashboard",
    "reload_archetypes_endpoint",
    "search_learning_context",
    "signal_stats",
    "submit_feedback",
    "teach_signal",
    "web_ui",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Configure logging and start the Slack bot alongside the API server."""
    from dashforge.logging import configure_logging

    configure_logging(settings.log_level)

    global _slack_task
    if settings.slack_bot_token and settings.slack_app_token:
        from dashforge.integrations.slack import start_slack_bot

        _slack_task = asyncio.create_task(start_slack_bot())
        logger.info("slack_bot_scheduled")
    else:
        logger.warning("slack_not_configured", hint="Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN to enable Slack")
    yield
    if _slack_task and not _slack_task.done():
        _slack_task.cancel()


app = create_app(lifespan=lifespan)
include_routes(app)


def main():
    uvicorn.run(
        "dashforge.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
