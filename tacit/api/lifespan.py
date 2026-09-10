"""FastAPI lifespan wiring."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import structlog
from fastapi import FastAPI

from tacit.api.optional_integrations import (
    OptionalIntegrationExecutionOwner,
    OptionalIntegrationExecutionUnavailable,
    OptionalIntegrationLifecycle,
    OptionalIntegrationTaskSettlement,
    set_optional_integration_state,
)
from tacit.config import Settings
from tacit.config import settings as default_settings
from tacit.pipeline_admission import release_runtime_root_with_startup_retry

if TYPE_CHECKING:
    from tacit.pipeline_admission import RuntimeRootOwnerHandle
    from tacit.runtime_stores import RuntimeStores

logger = structlog.get_logger()

_OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS = 6.0


def _slack_callback_parameter(starter: Callable[..., object]) -> str | None:
    """Return the strongest lifecycle callback supported by a Slack starter."""
    try:
        parameters = inspect.signature(starter).parameters
    except (TypeError, ValueError):
        return None
    if "lifecycle" in parameters:
        return "lifecycle"
    if "on_ready" in parameters:
        return "on_ready"
    return None


def _slack_supports_parameter(starter: Callable[..., object], parameter: str) -> bool:
    try:
        return parameter in inspect.signature(starter).parameters
    except (TypeError, ValueError):
        return False


def _observe_slack_completion(
    app: FastAPI,
    settlement: OptionalIntegrationTaskSettlement,
    *,
    lifecycle: OptionalIntegrationLifecycle,
    shutdown_requested: bool,
) -> BaseException | None:
    """Consume a Slack task result and publish a disclosure-safe status."""
    if settlement.cancelled:
        reason_code = "slack_shutdown_completed" if shutdown_requested else "slack_background_task_cancelled"
        owns_callback = lifecycle.callback_authority_active
        lifecycle.revoke(status="stopped", reason_code=reason_code)
        if owns_callback and not shutdown_requested:
            logger.warning("slack_background_task_stopped", reason_code=reason_code)
        return None

    failure = settlement.failure
    if failure is not None:
        reason_code = "slack_background_task_failed"
        owns_callback = lifecycle.callback_authority_active
        lifecycle.revoke(status="failed", reason_code=reason_code)
        if owns_callback:
            logger.error(
                "slack_background_task_failed",
                reason_code=reason_code,
                error_type=type(failure).__name__,
            )
        return failure

    reason_code = "slack_shutdown_completed" if shutdown_requested else "slack_background_task_stopped"
    owns_callback = lifecycle.callback_authority_active
    lifecycle.revoke(status="stopped", reason_code=reason_code)
    if owns_callback and not shutdown_requested:
        logger.warning("slack_background_task_stopped", reason_code=reason_code)
    return None


async def _stop_slack_and_runtime(
    app: FastAPI,
    *,
    slack_owner: OptionalIntegrationExecutionOwner | None,
    slack_lifecycle: OptionalIntegrationLifecycle,
    runtime_stores: RuntimeStores,
    root_handle: RuntimeRootOwnerHandle,
) -> None:
    """Settle optional Slack first, then release mandatory runtime authority."""
    slack_failure: BaseException | None = None
    if slack_owner is not None:
        settlement = await slack_owner.shutdown(
            timeout_seconds=_OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS,
            timeout_reason_code="slack_shutdown_timed_out",
        )
        if not settlement.completed:
            logger.warning(
                "slack_optional_integration_shutdown_timed_out",
                reason_code="slack_shutdown_timed_out",
            )
        else:
            slack_failure = settlement.failure

    try:
        await release_runtime_root_with_startup_retry(
            runtime_stores.shutdown_runtime_services,
            root_handle,
        )
    except BaseException as runtime_failure:
        logger.error(
            "api_runtime_shutdown_failed",
            reason_code="api_runtime_shutdown_failed",
            error_type=type(runtime_failure).__name__,
            slack_failure_type=(type(slack_failure).__name__ if slack_failure is not None else None),
        )
        if slack_failure is not None:
            raise runtime_failure from slack_failure
        raise


async def _await_teardown(task: asyncio.Task[None]) -> None:
    """Wait through repeated caller cancellation, then restore cancellation."""
    cancellation_requested = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancellation_requested = True

    teardown_failure: BaseException | None = None
    try:
        task.result()
    except BaseException as exc:
        teardown_failure = exc

    if cancellation_requested:
        if teardown_failure is not None:
            logger.error(
                "api_lifespan_teardown_failed_during_cancellation",
                reason_code="api_lifespan_teardown_failed_during_cancellation",
                error_type=type(teardown_failure).__name__,
            )
        raise asyncio.CancelledError from teardown_failure
    if teardown_failure is not None:
        raise teardown_failure


def _validate_lifespan_runtime(app: FastAPI, runtime_settings: Settings) -> RuntimeStores:
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
        app.state.runtime_store_readiness = runtime_stores.prepare_required_stores()
        root_handle = runtime_stores.start_runtime_services()

        slack_owner: OptionalIntegrationExecutionOwner | None = None
        slack_lifecycle = OptionalIntegrationLifecycle(app, name="slack")
        slack_shutdown_requested = threading.Event()
        try:
            if runtime_settings.slack_bot_token and runtime_settings.slack_app_token:
                from tacit.integrations.slack import start_slack_bot

                slack_lifecycle.publish(
                    status="starting",
                    reason_code="slack_starting",
                )

                async def run_slack() -> None:
                    if slack_owner is None:
                        raise RuntimeError("Slack execution subscription is unavailable")
                    execution_lifecycle = slack_owner.lifecycle
                    callback_parameter = _slack_callback_parameter(start_slack_bot)
                    borrows_api_root = _slack_supports_parameter(start_slack_bot, "owns_runtime_root")
                    if callback_parameter == "lifecycle":
                        if borrows_api_root:
                            await start_slack_bot(
                                runtime_settings,
                                stores=runtime_stores,
                                lifecycle=execution_lifecycle,
                                owns_runtime_root=False,
                            )
                        else:
                            await start_slack_bot(
                                runtime_settings,
                                stores=runtime_stores,
                                lifecycle=execution_lifecycle,
                            )
                    elif callback_parameter == "on_ready":

                        def publish_slack_readiness() -> None:
                            execution_lifecycle.publish(
                                status="ready",
                                reason_code="slack_ready",
                            )

                        if borrows_api_root:
                            await start_slack_bot(
                                runtime_settings,
                                stores=runtime_stores,
                                on_ready=publish_slack_readiness,
                                owns_runtime_root=False,
                            )
                        else:
                            await start_slack_bot(
                                runtime_settings,
                                stores=runtime_stores,
                                on_ready=publish_slack_readiness,
                            )
                    else:
                        if borrows_api_root:
                            await start_slack_bot(
                                runtime_settings,
                                stores=runtime_stores,
                                owns_runtime_root=False,
                            )
                        else:
                            await start_slack_bot(runtime_settings, stores=runtime_stores)

                runtime_identity = runtime_stores.runtime_ownership.admission_namespace
                if runtime_identity is None:
                    raise RuntimeError("Slack runtime has no admission identity")

                def observe_slack_completion(settlement: OptionalIntegrationTaskSettlement) -> None:
                    _observe_slack_completion(
                        app,
                        settlement,
                        lifecycle=slack_lifecycle,
                        shutdown_requested=slack_shutdown_requested.is_set(),
                    )

                try:
                    slack_owner = OptionalIntegrationExecutionOwner.acquire(
                        name="slack",
                        runtime_identity=runtime_identity,
                        lifecycle=slack_lifecycle,
                    )
                    slack_owner.start(
                        run_slack,
                        on_completion=observe_slack_completion,
                    )
                except OptionalIntegrationExecutionUnavailable as exc:
                    slack_owner = None
                    slack_lifecycle.revoke(status="failed", reason_code=exc.reason_code)
                    logger.warning(
                        "slack_execution_owner_unavailable",
                        reason_code=exc.reason_code,
                    )
                else:
                    logger.info("slack_bot_scheduled")
            else:
                set_optional_integration_state(
                    app,
                    name="slack",
                    status="disabled",
                    reason_code="slack_not_configured",
                )
                logger.warning("slack_not_configured", hint="Set SLACK_BOT_TOKEN and SLACK_APP_TOKEN to enable Slack")
            yield
        finally:
            slack_shutdown_requested.set()
            teardown_task = asyncio.create_task(
                _stop_slack_and_runtime(
                    app,
                    slack_owner=slack_owner,
                    slack_lifecycle=slack_lifecycle,
                    runtime_stores=runtime_stores,
                    root_handle=root_handle,
                ),
                name="tacit-api-lifespan-teardown",
            )
            await _await_teardown(teardown_task)

    return lifespan
