"""Slack Bot integration using Slack Bolt (Socket Mode)."""
from __future__ import annotations

import asyncio
import re

import structlog
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp

from dashforge.config import settings
from dashforge.models.schemas import DashRequest
from dashforge.pipeline import run_pipeline

logger = structlog.get_logger()

app = AsyncApp(
    token=settings.slack_bot_token,
    signing_secret=settings.slack_signing_secret,
)


def _strip_mention(text: str) -> str:
    """Remove the <@BOT_ID> mention prefix from the message."""
    return re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()


@app.event("app_mention")
async def handle_mention(event: dict, say):
    """Respond to @DashForge mentions in channels."""
    prompt = _strip_mention(event.get("text", ""))
    channel = event.get("channel", "")
    user = event.get("user", "")
    thread_ts = event.get("thread_ts") or event.get("ts", "")

    if not prompt:
        await say(
            text="Please provide a problem statement, e.g.:\n"
            "> @DashForge high latency on the checkout service in the last hour",
            thread_ts=thread_ts,
        )
        return

    # Acknowledge immediately
    await say(
        text=f"🔍 Analyzing: _{prompt}_\nBuilding your dashboard — this takes ~15-30 seconds…",
        thread_ts=thread_ts,
    )

    try:
        request = DashRequest(
            prompt=prompt,
            channel_id=channel,
            user_id=user,
            thread_ts=thread_ts,
        )
        response = await run_pipeline(request)

        if response.dashboard_url:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Dashboard ready!*\n{response.summary}",
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open Dashboard"},
                            "url": response.dashboard_url,
                            "style": "primary",
                        }
                    ],
                },
            ]
            await say(blocks=blocks, text=response.summary, thread_ts=thread_ts)
        else:
            await say(text=f"⚠️ {response.summary}", thread_ts=thread_ts)

    except Exception:
        logger.exception("pipeline_error")
        await say(
            text="❌ Something went wrong building the dashboard. Check the logs for details.",
            thread_ts=thread_ts,
        )


@app.command("/dashforge")
async def handle_slash_command(ack, command, say):
    """Handle /dashforge slash commands."""
    await ack()
    prompt = command.get("text", "").strip()
    channel = command.get("channel_id", "")
    user = command.get("user_id", "")

    if not prompt:
        await say(
            text="Usage: `/dashforge <problem statement>`\n"
            "Example: `/dashforge high error rate on payments API since 2pm`",
        )
        return

    await say(text=f"🔍 Analyzing: _{prompt}_\nBuilding your dashboard…")

    try:
        request = DashRequest(prompt=prompt, channel_id=channel, user_id=user)
        response = await run_pipeline(request)

        if response.dashboard_url:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Dashboard ready!*\n{response.summary}",
                    },
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "Open Dashboard"},
                            "url": response.dashboard_url,
                            "style": "primary",
                        }
                    ],
                },
            ]
            await say(blocks=blocks, text=response.summary)
        else:
            await say(text=f"⚠️ {response.summary}")

    except Exception:
        logger.exception("slash_command_error")
        await say(text="❌ Something went wrong building the dashboard.")


async def start_slack_bot():
    """Start the Slack bot in Socket Mode."""
    handler = AsyncSocketModeHandler(app, settings.slack_app_token)
    logger.info("slack_bot_starting")
    await handler.start_async()
