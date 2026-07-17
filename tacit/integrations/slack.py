"""Slack Bot integration using Slack Bolt (Socket Mode)."""

from __future__ import annotations

import re
from collections.abc import Callable

import structlog
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp

from tacit.config import Settings, settings
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies
from tacit.models.schemas import DashRequest
from tacit.pipeline import run_pipeline

logger = structlog.get_logger()


def _strip_mention(text: str) -> str:
    """Remove the <@BOT_ID> mention prefix from the message."""
    return re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()


def _tenant_for_slack(payload: dict, deps: PipelineDependencies | None) -> str:
    runtime_settings = getattr(deps, "settings", settings)
    configured = str(getattr(runtime_settings, "knowledge_tenant_id", "default") or "default")
    if configured != "*":
        return configured
    tenant_id = str(payload.get("team_id") or payload.get("team") or "").strip()
    if not tenant_id:
        raise ValueError("Slack team id is required when knowledge_tenant_id is '*'")
    return tenant_id


def _build_action_buttons(response) -> list[dict]:
    """Build Slack action buttons for Grafana (and optionally SignalFx)."""
    buttons = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "Open in Grafana"},
            "url": response.dashboard_url,
            "style": "primary",
        }
    ]
    if response.signalfx_url:
        buttons.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Open in SignalFx"},
                "url": response.signalfx_url,
            }
        )
    return buttons


def _contract_text(response, contract) -> str:
    """Render the canonical grounding result without upgrading it to a causal claim."""
    if contract is None:
        return response.summary
    conclusion = contract.grounding.maximum_trustworthy_conclusion.get("text", "")
    revision = contract.investigation.revision
    return (
        f"{response.summary}\n"
        f"*Grounding:* `{contract.grounding.status.value}`\n"
        f"*Maximum trustworthy conclusion:* {conclusion}\n"
        f"*Investigation:* `{contract.investigation.id}` revision `{revision}`"
    )


def _load_contract(deps: PipelineDependencies | None, response):
    if not response.investigation_id:
        return None
    try:
        if deps is not None:
            store = deps.history_store_factory()
        else:
            from tacit.history import get_investigation_store

            store = get_investigation_store()
        return store.get_contract(response.investigation_id, response.investigation_revision)
    except Exception:
        logger.warning("slack_contract_load_failed", investigation_id=response.investigation_id, exc_info=True)
        return None


async def handle_mention(
    event: dict,
    say,
    deps_factory: Callable[[], PipelineDependencies] | None = None,
):
    """Respond to @Tacit mentions in channels."""
    prompt = _strip_mention(event.get("text", ""))
    channel = event.get("channel", "")
    user = event.get("user", "")
    thread_ts = event.get("thread_ts") or event.get("ts", "")

    if not prompt:
        await say(
            text="Please provide a problem statement, e.g.:\n"
            "> @Tacit high latency on the checkout service in the last hour",
            thread_ts=thread_ts,
        )
        return

    await say(
        text=f"🔍 Analyzing: _{prompt}_\nBuilding your dashboard — this takes ~15-30 seconds…",
        thread_ts=thread_ts,
    )

    try:
        deps = deps_factory() if deps_factory else None
        request = DashRequest(
            prompt=prompt,
            channel_id=channel,
            user_id=user,
            thread_ts=thread_ts,
            tenant_id=_tenant_for_slack(event, deps),
        )
        response = await run_pipeline(request, deps)
        contract_text = _contract_text(response, _load_contract(deps, response))

        if response.dashboard_url:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Investigation complete*\n{contract_text}",
                    },
                },
                {"type": "actions", "elements": _build_action_buttons(response)},
            ]
            await say(blocks=blocks, text=contract_text, thread_ts=thread_ts)
        else:
            await say(text=f"⚠️ {response.summary}", thread_ts=thread_ts)

    except Exception:
        logger.exception("pipeline_error")
        await say(
            text="❌ Something went wrong building the dashboard. Check the logs for details.",
            thread_ts=thread_ts,
        )


async def handle_slash_command(
    ack,
    command,
    say,
    deps_factory: Callable[[], PipelineDependencies] | None = None,
):
    """Handle /tacit slash commands."""
    await ack()
    prompt = command.get("text", "").strip()
    channel = command.get("channel_id", "")
    user = command.get("user_id", "")

    if not prompt:
        await say(
            text="Usage: `/tacit <problem statement>`\nExample: `/tacit high error rate on payments API since 2pm`",
        )
        return

    await say(text=f"🔍 Analyzing: _{prompt}_\nBuilding your dashboard…")

    try:
        deps = deps_factory() if deps_factory else None
        request = DashRequest(
            prompt=prompt,
            channel_id=channel,
            user_id=user,
            tenant_id=_tenant_for_slack(command, deps),
        )
        response = await run_pipeline(request, deps)
        contract_text = _contract_text(response, _load_contract(deps, response))

        if response.dashboard_url:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"✅ *Investigation complete*\n{contract_text}",
                    },
                },
                {"type": "actions", "elements": _build_action_buttons(response)},
            ]
            await say(blocks=blocks, text=contract_text)
        else:
            await say(text=f"⚠️ {response.summary}")

    except Exception:
        logger.exception("slash_command_error")
        await say(text="❌ Something went wrong building the dashboard.")


def create_slack_app(runtime_settings: Settings = settings) -> AsyncApp:
    """Create a Slack app bound to one runtime settings object."""
    slack_app = AsyncApp(
        token=runtime_settings.slack_bot_token,
        signing_secret=runtime_settings.slack_signing_secret,
    )

    def deps_factory() -> PipelineDependencies:
        return build_pipeline_dependencies(runtime_settings)

    async def runtime_handle_mention(event: dict, say) -> None:
        await handle_mention(event, say, deps_factory=deps_factory)

    async def runtime_handle_slash_command(ack, command, say) -> None:
        await handle_slash_command(ack, command, say, deps_factory=deps_factory)

    slack_app.event("app_mention")(runtime_handle_mention)
    slack_app.command("/tacit")(runtime_handle_slash_command)
    return slack_app


async def start_slack_bot(runtime_settings: Settings = settings):
    """Start the Slack bot in Socket Mode."""
    slack_app = create_slack_app(runtime_settings)
    handler = AsyncSocketModeHandler(slack_app, runtime_settings.slack_app_token)
    logger.info("slack_bot_starting")
    await handler.start_async()
