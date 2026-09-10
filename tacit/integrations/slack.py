"""Slack Bot integration using Slack Bolt (Socket Mode)."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from types import TracebackType

import structlog
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp
from slack_sdk.web.async_client import AsyncWebClient

from tacit.api.optional_integrations import (
    OptionalIntegrationLifecycle,
    OptionalIntegrationLifecycleProtocol,
    settle_optional_integration_task,
)
from tacit.config import Settings, settings
from tacit.dependencies import (
    PipelineDependencies,
    build_pipeline_dependencies,
)
from tacit.errors import PipelineAdmissionRejected
from tacit.models.schemas import DashRequest
from tacit.pipeline import run_pipeline
from tacit.pipeline.side_effects import terminal_cleanup_failure
from tacit.pipeline_admission import release_runtime_root_with_startup_retry
from tacit.runtime_ownership import (
    get_runtime_ownership,
    require_compatible_runtime_ownership,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStores

logger = structlog.get_logger()

_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS = 1.0
_SLACK_CONNECTION_PROBE_TIMEOUT_SECONDS = 5.0
_SLACK_CLOSE_TIMEOUT_SECONDS = 5.0


class SlackOptionalIntegrationShutdownTimeout(RuntimeError):
    """Raised after Slack cleanup exceeds the optional-integration deadline."""

    optional_integration_resources_unretired = True


def _validated_slack_runtime_stores(
    runtime_settings: Settings,
    stores: RuntimeStores | None,
) -> RuntimeStores:
    """Resolve one Slack runtime owner before constructing Slack clients."""
    runtime_stores = stores or RuntimeStores(runtime_settings)
    require_compatible_runtime_ownership(
        boundary="Slack runtime",
        descriptors=(
            runtime_descriptor_from_settings(runtime_settings, component="slack_settings"),
            get_runtime_ownership(runtime_stores, component="runtime_stores"),
        ),
    )
    return runtime_stores


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


def _load_contract(deps: PipelineDependencies | None, response, *, tenant_id: str):
    if not response.investigation_id:
        return None
    try:
        if deps is not None:
            store = deps.history_store_factory()
        else:
            from tacit.history import get_investigation_store

            store = get_investigation_store()
        return store.get_contract(
            response.investigation_id,
            response.investigation_revision,
            tenant_id=tenant_id,
        )
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
        contract_text = _contract_text(response, _load_contract(deps, response, tenant_id=request.tenant_id))

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

    except PipelineAdmissionRejected as exc:
        await say(text=exc.public_message(), thread_ts=thread_ts)
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
        contract_text = _contract_text(response, _load_contract(deps, response, tenant_id=request.tenant_id))

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

    except PipelineAdmissionRejected as exc:
        await say(text=exc.public_message())
    except Exception:
        logger.exception("slash_command_error")
        await say(text="❌ Something went wrong building the dashboard.")


def create_slack_app(
    runtime_settings: Settings = settings,
    *,
    stores: RuntimeStores | None = None,
    required_runtime_root_generation: int | None = None,
    lifecycle: OptionalIntegrationLifecycleProtocol | None = None,
) -> AsyncApp:
    """Create a Slack app bound to one runtime settings object."""
    runtime_stores = _validated_slack_runtime_stores(runtime_settings, stores)
    web_client = AsyncWebClient(
        token=runtime_settings.slack_bot_token,
        trust_env_in_session=False,
    )
    # Slack SDK discovers proxy environment variables even when proxy=None.
    # Construction performs no I/O, so clear that undeclared route before the
    # client can issue a credential-bearing request.
    web_client.proxy = None
    slack_app = AsyncApp(
        token=runtime_settings.slack_bot_token,
        signing_secret=runtime_settings.slack_signing_secret,
        client=web_client,
    )

    def deps_factory() -> PipelineDependencies:
        return build_pipeline_dependencies(
            runtime_settings,
            stores=runtime_stores,
            required_runtime_root_generation=required_runtime_root_generation,
        )

    async def runtime_handle_mention(event: dict, say) -> None:
        if lifecycle is not None and not lifecycle.callback_authority_active:
            logger.info("slack_callback_rejected", reason_code="slack_generation_retired")
            return
        await handle_mention(event, say, deps_factory=deps_factory)

    async def runtime_handle_slash_command(ack, command, say) -> None:
        if lifecycle is not None and not lifecycle.callback_authority_active:
            logger.info("slack_callback_rejected", reason_code="slack_generation_retired")
            return
        await handle_slash_command(ack, command, say, deps_factory=deps_factory)

    slack_app.event("app_mention")(runtime_handle_mention)
    slack_app.command("/tacit")(runtime_handle_slash_command)
    return slack_app


def _new_slack_socket_mode_handler(slack_app: AsyncApp, app_token: str) -> AsyncSocketModeHandler:
    """Construct Socket Mode without retaining an environment-selected proxy."""
    handler = AsyncSocketModeHandler(slack_app, app_token)
    socket_client = getattr(handler, "client", None)
    if socket_client is not None:
        socket_client.proxy = None
    return handler


async def _close_slack_and_runtime(
    handler: AsyncSocketModeHandler | None,
    runtime_stores: RuntimeStores,
    root_handle,
    *,
    lifecycle: OptionalIntegrationLifecycleProtocol,
    connection_monitor: asyncio.Task[None] | None,
) -> None:
    """Close the socket before releasing its independently owned root."""
    close_error: BaseException | None = None
    if connection_monitor is not None:
        connection_monitor.cancel()
        try:
            await connection_monitor
        except asyncio.CancelledError:
            pass
        except BaseException as exc:
            logger.warning(
                "slack_connection_monitor_cleanup_failed",
                reason_code="slack_connection_monitor_cleanup_failed",
                error_type=type(exc).__name__,
            )

    if handler is not None:
        close_task = asyncio.create_task(
            handler.close_async(),
            name="tacit-slack-socket-close",
        )
        settlement = await settle_optional_integration_task(
            close_task,
            lifecycle=lifecycle,
            timeout_seconds=_SLACK_CLOSE_TIMEOUT_SECONDS,
            timeout_reason_code="slack_shutdown_timed_out",
            cancel_first=False,
        )
        if not settlement.completed:
            close_error = SlackOptionalIntegrationShutdownTimeout("Slack cleanup exceeded its deadline")
            logger.warning(
                "slack_socket_cleanup_timed_out",
                reason_code="slack_shutdown_timed_out",
            )
        elif settlement.failure is not None:
            close_error = settlement.failure
            logger.warning(
                "slack_socket_cleanup_failed",
                reason_code="slack_socket_cleanup_failed",
                error_type=type(settlement.failure).__name__,
            )

    root_error: BaseException | None = None
    if root_handle is not None:
        try:
            await release_runtime_root_with_startup_retry(
                runtime_stores.shutdown_runtime_services,
                root_handle,
            )
        except BaseException as exc:
            root_error = exc
            logger.warning(
                "slack_runtime_root_cleanup_failed",
                reason_code="slack_runtime_root_cleanup_failed",
                error_type=type(exc).__name__,
            )

    if root_error is not None:
        if close_error is not None:
            raise root_error from close_error
        raise root_error
    if close_error is not None:
        raise close_error


async def _monitor_slack_connection(
    handler: AsyncSocketModeHandler,
    lifecycle: OptionalIntegrationLifecycleProtocol,
) -> None:
    """Publish confirmed transport health while the SDK owns reconnects."""
    while True:
        await asyncio.sleep(_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS)
        probe = asyncio.create_task(
            handler.client.is_connected(),
            name="tacit-slack-connection-probe",
        )
        try:
            done, _pending = await asyncio.wait(
                {probe},
                timeout=_SLACK_CONNECTION_PROBE_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            probe.cancel()
            if not probe.done():
                lifecycle.retain_detached_task(probe)
            raise

        if not done:
            probe.cancel()
            lifecycle.publish(
                status="failed",
                reason_code="slack_connection_probe_timed_out",
            )
            lifecycle.retain_detached_task(probe)
            logger.warning(
                "slack_connection_probe_timed_out",
                reason_code="slack_connection_probe_timed_out",
            )
            return

        if probe.cancelled():
            lifecycle.publish(
                status="failed",
                reason_code="slack_connection_state_unavailable",
            )
            logger.warning(
                "slack_connection_state_unavailable",
                reason_code="slack_connection_state_unavailable",
                error_type="CancelledError",
            )
            continue
        try:
            connected = probe.result()
        except Exception as exc:
            lifecycle.publish(
                status="failed",
                reason_code="slack_connection_state_unavailable",
            )
            logger.warning(
                "slack_connection_state_unavailable",
                reason_code="slack_connection_state_unavailable",
                error_type=type(exc).__name__,
            )
            continue

        if connected:
            lifecycle.publish(status="ready", reason_code="slack_ready")
        else:
            lifecycle.publish(status="reconnecting", reason_code="slack_reconnecting")


async def _await_slack_teardown(
    task: asyncio.Task[None],
    *,
    cancellation_requested: bool,
) -> tuple[BaseException | None, bool]:
    """Keep teardown alive through repeated cancellation of its caller."""
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancellation_requested = True
        except BaseException:
            # The completed task remains the sole source of teardown outcome.
            # Read it below so primary and cleanup failures retain their order.
            pass

    teardown_error: BaseException | None = None
    try:
        task.result()
    except BaseException as exc:
        teardown_error = exc
    return teardown_error, cancellation_requested


async def start_slack_bot(
    runtime_settings: Settings = settings,
    *,
    stores: RuntimeStores | None = None,
    on_ready: Callable[[], None] | None = None,
    lifecycle: OptionalIntegrationLifecycleProtocol | None = None,
    owns_runtime_root: bool = True,
):
    """Start Socket Mode, optionally owning a standalone runtime root."""
    runtime_stores = _validated_slack_runtime_stores(runtime_settings, stores)
    # Standalone Slack owns a root. API lifespan passes ``False`` because its
    # root already owns the shared graph for the complete optional lifetime.
    if not owns_runtime_root:
        execution_graph = runtime_stores.pipeline_admission().execution_graph
        if execution_graph.root_state != "active" or execution_graph.root_owner_count <= 0:
            raise RuntimeError("Slack root borrowing requires an active runtime root")
    root_handle = runtime_stores.start_runtime_services() if owns_runtime_root else None
    execution_graph = runtime_stores.pipeline_admission().execution_graph
    required_runtime_root_generation = execution_graph.root_generation
    lifecycle_owner = lifecycle or OptionalIntegrationLifecycle(None, name="slack")
    handler: AsyncSocketModeHandler | None = None
    connection_monitor: asyncio.Task[None] | None = None
    primary_error: BaseException | None = None
    primary_traceback: TracebackType | None = None
    try:
        slack_app = create_slack_app(
            runtime_settings,
            stores=runtime_stores,
            required_runtime_root_generation=required_runtime_root_generation,
            lifecycle=lifecycle_owner,
        )
        handler = _new_slack_socket_mode_handler(slack_app, runtime_settings.slack_app_token)
        logger.info("slack_bot_starting")
        if lifecycle is None and on_ready is None:
            await handler.start_async()
        else:
            await handler.connect_async()
            lifecycle_owner.publish(status="ready", reason_code="slack_ready")
            if on_ready is not None:
                on_ready()
            logger.info("slack_bot_ready")
            if lifecycle is not None:
                connection_monitor = asyncio.create_task(
                    _monitor_slack_connection(handler, lifecycle_owner),
                    name="tacit-slack-connection-monitor",
                )
            await asyncio.Event().wait()
    except BaseException as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__

    teardown_task = asyncio.create_task(
        _close_slack_and_runtime(
            handler,
            runtime_stores,
            root_handle,
            lifecycle=lifecycle_owner,
            connection_monitor=connection_monitor,
        ),
        name="tacit-slack-runtime-teardown",
    )
    teardown_error, cancellation_requested = await _await_slack_teardown(
        teardown_task,
        cancellation_requested=isinstance(primary_error, asyncio.CancelledError),
    )

    if cancellation_requested:
        if isinstance(teardown_error, SlackOptionalIntegrationShutdownTimeout):
            raise teardown_error from primary_error
        if teardown_error is not None:
            logger.warning(
                "slack_runtime_teardown_failed_during_cancellation",
                reason_code="slack_runtime_teardown_failed_during_cancellation",
                error_type=type(teardown_error).__name__,
            )
        if lifecycle_owner.callback_authority_active:
            lifecycle_owner.revoke(status="stopped", reason_code="slack_shutdown_completed")
        cancellation = primary_error if isinstance(primary_error, asyncio.CancelledError) else asyncio.CancelledError()
        cause = teardown_error or (primary_error if cancellation is not primary_error else None)
        if cause is not None:
            raise cancellation.with_traceback(primary_traceback) from cause
        raise cancellation.with_traceback(primary_traceback)
    if teardown_error is not None:
        if lifecycle_owner.callback_authority_active:
            lifecycle_owner.revoke(status="failed", reason_code="slack_runtime_cleanup_failed")
        if primary_error is not None:
            cleanup_failure = terminal_cleanup_failure(
                primary_error,
                teardown_error,
                reason_code="slack_runtime_cleanup_failed",
                message="Slack runtime cleanup failed",
            )
            if isinstance(teardown_error, SlackOptionalIntegrationShutdownTimeout):
                setattr(cleanup_failure, "optional_integration_resources_unretired", True)
            raise cleanup_failure
        raise teardown_error
    if primary_error is not None:
        if lifecycle_owner.callback_authority_active:
            lifecycle_owner.revoke(status="failed", reason_code="slack_background_task_failed")
        raise primary_error.with_traceback(primary_traceback)
    if lifecycle_owner.callback_authority_active:
        lifecycle_owner.revoke(status="stopped", reason_code="slack_shutdown_completed")
