"""Cross-runtime matrix for the API-owned optional Slack execution boundary."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

import tacit.api.lifespan as lifespan_module
import tacit.api.optional_integrations as optional_integrations_module
from tacit.api.lifespan import create_lifespan
from tacit.config import Settings
from tacit.runtime_stores import RuntimeStores

_OWNER_THREAD_NAME = "tacit-slack-optional-integration"


def _settings(tmp_path, *, suffix: str) -> Settings:
    return Settings(  # type: ignore[call-arg]
        _env_file=None,
        slack_bot_token="xoxb-runtime",
        slack_app_token="xapp-runtime",
        slack_signing_secret="signing-runtime",
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
    )


async def _wait_until(predicate, *, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition did not become true before its deadline")
        await asyncio.sleep(0.001)


def _owner_thread_count() -> int:
    return sum(thread.name == _OWNER_THREAD_NAME and thread.is_alive() for thread in threading.enumerate())


def _acquire_direct_owner(
    *,
    runtime_identity: str,
    lifecycle: optional_integrations_module.OptionalIntegrationLifecycle | None = None,
) -> optional_integrations_module.OptionalIntegrationExecutionOwner:
    return optional_integrations_module.OptionalIntegrationExecutionOwner.acquire(
        name="slack",
        runtime_identity=runtime_identity,
        lifecycle=lifecycle or optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack"),
    )


def _assert_unstarted_generation_released(
    state: optional_integrations_module._OptionalIntegrationExecutionState,
    settlement: optional_integrations_module.OptionalIntegrationTaskSettlement,
) -> None:
    assert settlement == optional_integrations_module.OptionalIntegrationTaskSettlement(
        completed=True,
        cancelled=True,
    )
    assert state.completion.result(timeout=0) == settlement
    assert state.finished.is_set()
    assert state.terminal_committed
    assert not state.accepting_subscriptions
    assert state.active_subscriptions == set()
    assert state.completion_callbacks == {}
    assert state.thread is None
    assert state.loop is None
    assert state.task is None
    assert state.key not in optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    assert state.key not in optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS


def test_stopped_open_subscriber_coalesces_publications_to_one_terminal_callback() -> None:
    app = FastAPI()
    owner_loop = asyncio.new_event_loop()
    lifecycle_ready = threading.Event()
    owner_stopped = threading.Event()
    resume_owner = threading.Event()
    lifecycle_holder: dict[str, optional_integrations_module.OptionalIntegrationLifecycle] = {}

    def run_owner_loop() -> None:
        asyncio.set_event_loop(owner_loop)

        def create_lifecycle() -> None:
            lifecycle_holder["lifecycle"] = optional_integrations_module.OptionalIntegrationLifecycle(
                app,
                name="slack",
            )
            lifecycle_ready.set()

        owner_loop.call_soon(create_lifecycle)
        owner_loop.run_forever()
        owner_stopped.set()
        assert resume_owner.wait(timeout=2.0)
        owner_loop.run_forever()
        owner_loop.close()

    owner_thread = threading.Thread(target=run_owner_loop, daemon=True)
    owner_thread.start()
    assert lifecycle_ready.wait(timeout=1.0)
    lifecycle = lifecycle_holder["lifecycle"]
    owner_loop.call_soon_threadsafe(owner_loop.stop)
    assert owner_stopped.wait(timeout=1.0)

    scheduled_callbacks = 0
    scheduled_lock = threading.Lock()
    callback_delivered = threading.Event()
    original_call_soon_threadsafe = owner_loop.call_soon_threadsafe

    def counted_call_soon_threadsafe(callback, *args):
        nonlocal scheduled_callbacks
        with scheduled_lock:
            scheduled_callbacks += 1

        def observed_callback(*callback_args):
            try:
                callback(*callback_args)
            finally:
                callback_delivered.set()

        return original_call_soon_threadsafe(observed_callback, *args)

    owner_loop.call_soon_threadsafe = counted_call_soon_threadsafe  # type: ignore[assignment]
    for index in range(1_000):
        lifecycle.publish(
            status="reconnecting",
            reason_code=f"slack_reconnecting_{index}",
        )
    lifecycle.revoke(status="stopped", reason_code="slack_stopped")
    assert lifecycle.publish(status="ready", reason_code="late_ready") is None

    with scheduled_lock:
        assert scheduled_callbacks == 1
    assert not lifecycle.callback_authority_active

    resume_owner.set()
    assert callback_delivered.wait(timeout=1.0)
    assert getattr(app.state, "optional_integration_readiness", {}).get("slack") == {
        "status": "stopped",
        "reason_code": "slack_stopped",
    }

    original_call_soon_threadsafe(owner_loop.stop)
    owner_thread.join(timeout=1.0)
    assert not owner_thread.is_alive()
    with lifecycle._state_lock:
        assert lifecycle._transport_callback_pending is False
        assert lifecycle._pending_snapshot is None


def test_primary_exit_terminally_settles_shared_unstarted_slack_generation() -> None:
    """A departed startup owner cannot strand surviving subscribers."""
    runtime_identity = "optional-integration-unstarted-shared-generation"
    baseline_threads = _owner_thread_count()
    primary_lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    secondary_lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    primary = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=primary_lifecycle,
    )
    secondary = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=secondary_lifecycle,
    )
    state = primary._state
    callback_settlements: list[optional_integrations_module.OptionalIntegrationTaskSettlement] = []

    async def must_not_run() -> None:
        raise AssertionError("an unstarted generation must not invoke its operation")

    def observe(settlement: optional_integrations_module.OptionalIntegrationTaskSettlement) -> None:
        assert state.state_lock.acquire(blocking=False)
        state.state_lock.release()
        callback_settlements.append(settlement)

    # A non-primary subscriber can register completion interest, but it cannot
    # start the shared transport generation.
    secondary.start(must_not_run, on_completion=observe)
    assert not state.started
    assert state.thread is None

    first_settlement = asyncio.run(
        primary.shutdown(
            timeout_seconds=1.0,
            timeout_reason_code="slack_shutdown_timed_out",
        )
    )
    primary_exit_was_terminal = state.finished.is_set()
    callbacks_after_primary_exit = list(callback_settlements)

    settlement = asyncio.run(
        secondary.shutdown(
            timeout_seconds=1.0,
            timeout_reason_code="slack_shutdown_timed_out",
        )
    )

    assert primary_exit_was_terminal
    assert first_settlement == settlement
    assert callbacks_after_primary_exit == [first_settlement]
    _assert_unstarted_generation_released(state, settlement)
    assert callback_settlements == [settlement]
    assert not primary_lifecycle.callback_authority_active
    assert not secondary_lifecycle.callback_authority_active
    assert state.lifecycle._subscribers == {}
    assert _owner_thread_count() == baseline_threads

    replacement = _acquire_direct_owner(runtime_identity=runtime_identity)
    assert replacement._primary
    assert replacement._state is not state
    replacement_settlement = asyncio.run(
        replacement.shutdown(
            timeout_seconds=1.0,
            timeout_reason_code="slack_shutdown_timed_out",
        )
    )
    _assert_unstarted_generation_released(replacement._state, replacement_settlement)


def test_cancelled_primary_settles_secondary_on_a_separate_event_loop_thread() -> None:
    """Cancelled startup ownership settles subscribers across caller loops."""
    runtime_identity = "optional-integration-unstarted-cross-loop"
    baseline_threads = _owner_thread_count()
    primary_ready = threading.Event()
    secondary_registered = threading.Event()
    primary_done = threading.Event()
    allow_secondary_shutdown = threading.Event()
    callback_called = threading.Event()
    callback_lock = threading.Lock()
    shared: dict[str, object] = {}
    callback_settlements: list[optional_integrations_module.OptionalIntegrationTaskSettlement] = []

    async def must_not_run() -> None:
        raise AssertionError("an unstarted generation must not invoke its operation")

    def observe(settlement: optional_integrations_module.OptionalIntegrationTaskSettlement) -> None:
        with callback_lock:
            callback_settlements.append(settlement)
        callback_called.set()

    def run_primary() -> None:
        async def exercise() -> None:
            lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
            owner = _acquire_direct_owner(
                runtime_identity=runtime_identity,
                lifecycle=lifecycle,
            )
            shared["primary"] = owner
            shared["state"] = owner._state
            shared["primary_lifecycle"] = lifecycle
            primary_ready.set()
            assert secondary_registered.wait(timeout=1.0)
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                shared["primary_settlement"] = await asyncio.shield(
                    owner.shutdown(
                        timeout_seconds=1.0,
                        timeout_reason_code="slack_shutdown_timed_out",
                    )
                )
            else:
                raise AssertionError("the startup owner cancellation must be delivered")
            primary_done.set()

        asyncio.run(exercise())

    def run_secondary() -> None:
        assert primary_ready.wait(timeout=1.0)

        async def exercise() -> None:
            lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
            owner = _acquire_direct_owner(
                runtime_identity=runtime_identity,
                lifecycle=lifecycle,
            )
            shared["secondary"] = owner
            shared["secondary_lifecycle"] = lifecycle
            owner.start(must_not_run, on_completion=observe)
            secondary_registered.set()
            assert allow_secondary_shutdown.wait(timeout=1.0)
            shared["secondary_settlement"] = await owner.shutdown(
                timeout_seconds=1.0,
                timeout_reason_code="slack_shutdown_timed_out",
            )

        asyncio.run(exercise())

    primary_thread = threading.Thread(target=run_primary, daemon=True)
    secondary_thread = threading.Thread(target=run_secondary, daemon=True)
    primary_thread.start()
    secondary_thread.start()

    assert primary_done.wait(timeout=2.0)
    state = shared["state"]
    assert isinstance(state, optional_integrations_module._OptionalIntegrationExecutionState)
    primary_exit_was_terminal = state.finished.is_set()
    callback_was_published = callback_called.is_set()
    allow_secondary_shutdown.set()
    primary_thread.join(timeout=2.0)
    secondary_thread.join(timeout=2.0)

    assert not primary_thread.is_alive()
    assert not secondary_thread.is_alive()
    assert primary_exit_was_terminal
    assert callback_was_published
    primary_settlement = shared["primary_settlement"]
    secondary_settlement = shared["secondary_settlement"]
    assert isinstance(
        primary_settlement,
        optional_integrations_module.OptionalIntegrationTaskSettlement,
    )
    primary_lifecycle = shared["primary_lifecycle"]
    secondary_lifecycle = shared["secondary_lifecycle"]
    assert isinstance(
        primary_lifecycle,
        optional_integrations_module.OptionalIntegrationLifecycle,
    )
    assert isinstance(
        secondary_lifecycle,
        optional_integrations_module.OptionalIntegrationLifecycle,
    )
    assert primary_settlement == secondary_settlement
    _assert_unstarted_generation_released(state, primary_settlement)
    assert callback_settlements == [primary_settlement]
    assert not primary_lifecycle.callback_authority_active
    assert not secondary_lifecycle.callback_authority_active
    assert _owner_thread_count() == baseline_threads


@pytest.mark.parametrize("interruption", ["exception", "cancellation"])
def test_interruption_between_slack_acquire_and_start_releases_generation(
    interruption: str,
) -> None:
    """Caller loss before startup cannot retain optional-integration capacity."""
    runtime_identity = f"optional-integration-unstarted-{interruption}"
    baseline_threads = _owner_thread_count()
    lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    owner = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=lifecycle,
    )
    state = owner._state

    async def interrupt_before_start() -> optional_integrations_module.OptionalIntegrationTaskSettlement:
        try:
            if interruption == "exception":
                raise RuntimeError("synthetic failure before optional integration startup")
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            await asyncio.sleep(0)
        except (asyncio.CancelledError, RuntimeError):
            return await asyncio.shield(
                owner.shutdown(
                    timeout_seconds=1.0,
                    timeout_reason_code="slack_shutdown_timed_out",
                )
            )
        raise AssertionError("the synthetic interruption must terminate startup")

    settlement = asyncio.run(interrupt_before_start())

    _assert_unstarted_generation_released(state, settlement)
    assert not lifecycle.callback_authority_active
    assert state.lifecycle._subscribers == {}
    assert _owner_thread_count() == baseline_threads


def test_repeated_unstarted_slack_generations_reuse_registry_capacity() -> None:
    """Abandoned shared generations release, rather than consume, capacity."""
    baseline_threads = _owner_thread_count()
    runtime_identity = "optional-integration-unstarted-capacity"

    async def must_not_run() -> None:
        raise AssertionError("an unstarted generation must not invoke its operation")

    for generation in range(optional_integrations_module._MAX_OPTIONAL_INTEGRATION_EXECUTION_OWNERS * 2):
        primary = _acquire_direct_owner(runtime_identity=runtime_identity)
        secondary = _acquire_direct_owner(runtime_identity=runtime_identity)
        state = primary._state
        callbacks: list[optional_integrations_module.OptionalIntegrationTaskSettlement] = []
        secondary.start(must_not_run, on_completion=callbacks.append)
        primary_settlement = asyncio.run(
            primary.shutdown(
                timeout_seconds=1.0,
                timeout_reason_code="slack_shutdown_timed_out",
            )
        )
        secondary_settlement = asyncio.run(
            secondary.shutdown(
                timeout_seconds=1.0,
                timeout_reason_code="slack_shutdown_timed_out",
            )
        )
        assert primary_settlement == secondary_settlement, generation
        _assert_unstarted_generation_released(state, primary_settlement)
        assert callbacks == [primary_settlement]

    assert _owner_thread_count() == baseline_threads


def test_definite_slack_owner_start_failure_settles_every_subscriber(monkeypatch) -> None:
    """A pre-start failure terminates and wakes the complete shared generation."""
    runtime_identity = "optional-integration-definite-start-failure"
    primary_lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    secondary_lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    primary = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=primary_lifecycle,
    )
    secondary = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=secondary_lifecycle,
    )
    original_thread_factory = optional_integrations_module._new_optional_integration_thread
    state = primary._state
    callback_settlements: list[optional_integrations_module.OptionalIntegrationTaskSettlement] = []

    def observe(settlement: optional_integrations_module.OptionalIntegrationTaskSettlement) -> None:
        assert state.state_lock.acquire(blocking=False)
        state.state_lock.release()
        callback_settlements.append(settlement)

    async def operation() -> None:
        raise AssertionError("a definite start failure must not invoke the operation factory")

    secondary.start(operation, on_completion=observe)

    def fail_thread_construction(*, target, name, daemon):
        del target, name, daemon
        raise RuntimeError("synthetic definite Slack start failure")

    monkeypatch.setattr(
        optional_integrations_module,
        "_new_optional_integration_thread",
        fail_thread_construction,
    )

    with pytest.raises(
        optional_integrations_module.OptionalIntegrationExecutionUnavailable,
        match="slack_execution_owner_start_failed",
    ):
        primary.start(operation, on_completion=observe)

    assert state.finished.wait(timeout=1.0)
    settlement = state.completion.result(timeout=0)
    assert len(callback_settlements) == 2
    assert callback_settlements == [settlement, settlement]
    assert isinstance(settlement.failure, optional_integrations_module.OptionalIntegrationExecutionUnavailable)
    assert settlement.failure.reason_code == "slack_execution_owner_start_failed"
    assert not state.accepting_subscriptions
    assert state.active_subscriptions == set()
    assert state.completion_callbacks == {}
    assert not primary_lifecycle.callback_authority_active
    assert not secondary_lifecycle.callback_authority_active
    assert state.lifecycle._subscribers == {}
    assert state.key not in optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    assert state.key not in optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS

    monkeypatch.setattr(
        optional_integrations_module,
        "_new_optional_integration_thread",
        original_thread_factory,
    )
    replacement = _acquire_direct_owner(runtime_identity=runtime_identity)

    async def complete() -> None:
        return None

    replacement.start(complete, on_completion=lambda _settlement: None)
    asyncio.run(
        replacement.shutdown(
            timeout_seconds=1.0,
            timeout_reason_code="slack_shutdown_timed_out",
        )
    )


def test_ambiguous_live_slack_owner_start_failure_fences_and_settles_subscribers(
    monkeypatch,
) -> None:
    """A start-then-raise failure wakes subscribers without replacement risk."""
    runtime_identity = "optional-integration-ambiguous-start-failure"
    primary_lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    secondary_lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    primary = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=primary_lifecycle,
    )
    secondary = _acquire_direct_owner(
        runtime_identity=runtime_identity,
        lifecycle=secondary_lifecycle,
    )
    state = primary._state
    operation_started = threading.Event()
    operation_finished = threading.Event()
    callback_settlements: list[optional_integrations_module.OptionalIntegrationTaskSettlement] = []

    def observe(settlement: optional_integrations_module.OptionalIntegrationTaskSettlement) -> None:
        assert state.state_lock.acquire(blocking=False)
        state.state_lock.release()
        callback_settlements.append(settlement)

    async def operation() -> None:
        operation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            operation_finished.set()

    secondary.start(operation, on_completion=observe)

    class AmbiguousStartThread(threading.Thread):
        def start(self) -> None:
            super().start()
            assert operation_started.wait(timeout=1.0)
            raise RuntimeError("synthetic ambiguous Slack start failure")

    def ambiguous_thread(*, target, name, daemon):
        return AmbiguousStartThread(target=target, name=name, daemon=daemon)

    monkeypatch.setattr(
        optional_integrations_module,
        "_new_optional_integration_thread",
        ambiguous_thread,
    )

    with pytest.raises(
        optional_integrations_module.OptionalIntegrationExecutionUnavailable,
        match="slack_execution_owner_start_failed",
    ):
        primary.start(operation, on_completion=observe)

    assert state.finished.wait(timeout=1.0)
    assert operation_finished.wait(timeout=1.0)
    settlement = state.completion.result(timeout=0)
    assert len(callback_settlements) == 2
    assert callback_settlements == [settlement, settlement]
    assert isinstance(settlement.failure, optional_integrations_module.OptionalIntegrationExecutionUnavailable)
    assert settlement.failure.reason_code == "slack_execution_owner_start_failed"
    assert state.stop_requested.is_set()
    assert not state.accepting_subscriptions
    assert state.active_subscriptions == set()
    assert state.completion_callbacks == {}
    assert not primary_lifecycle.callback_authority_active
    assert not secondary_lifecycle.callback_authority_active
    assert state.lifecycle._subscribers == {}
    assert state.key not in optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    assert state.key in optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS

    with pytest.raises(
        optional_integrations_module.OptionalIntegrationExecutionUnavailable,
        match="slack_execution_owner_fenced",
    ):
        _acquire_direct_owner(runtime_identity=runtime_identity)


def test_final_slack_release_is_linearized_before_transport_cancellation(monkeypatch) -> None:
    """A subscriber cannot attach after final shutdown owns the generation."""
    runtime_identity = "optional-integration-final-release-race"
    operation_started = threading.Event()
    stop_committed = threading.Event()
    allow_stop = threading.Event()
    shutdown_finished = threading.Event()
    shutdown_results: list[optional_integrations_module.OptionalIntegrationTaskSettlement] = []
    primary = _acquire_direct_owner(runtime_identity=runtime_identity)

    async def operation() -> None:
        operation_started.set()
        await asyncio.Event().wait()

    primary.start(operation, on_completion=lambda _settlement: None)
    assert operation_started.wait(timeout=1.0)
    original_request_stop = primary.request_stop

    def barrier_request_stop() -> None:
        stop_committed.set()
        assert allow_stop.wait(timeout=1.0)
        original_request_stop()

    monkeypatch.setattr(primary, "request_stop", barrier_request_stop)

    def shutdown_primary() -> None:
        try:
            shutdown_results.append(
                asyncio.run(
                    primary.shutdown(
                        timeout_seconds=1.0,
                        timeout_reason_code="slack_shutdown_timed_out",
                    )
                )
            )
        finally:
            shutdown_finished.set()

    shutdown_thread = threading.Thread(target=shutdown_primary, daemon=True)
    shutdown_thread.start()
    assert stop_committed.wait(timeout=1.0)

    late_owner = None
    late_reason = None
    try:
        try:
            late_owner = _acquire_direct_owner(runtime_identity=runtime_identity)
        except optional_integrations_module.OptionalIntegrationExecutionUnavailable as exc:
            late_reason = exc.reason_code
    finally:
        allow_stop.set()
        assert shutdown_finished.wait(timeout=1.0)
        shutdown_thread.join(timeout=0)
        if late_owner is not None:
            asyncio.run(
                late_owner.shutdown(
                    timeout_seconds=1.0,
                    timeout_reason_code="slack_shutdown_timed_out",
                )
            )

    assert late_owner is None
    assert late_reason == "slack_execution_owner_stopping"
    assert shutdown_results == [
        optional_integrations_module.OptionalIntegrationTaskSettlement(
            completed=True,
            cancelled=True,
        )
    ]


def test_new_slack_subscriber_winning_the_transition_preserves_transport(monkeypatch) -> None:
    """An admitted subscriber makes the concurrent release non-final."""
    runtime_identity = "optional-integration-acquire-wins-release-race"
    operation_started = threading.Event()
    operation_stopped = threading.Event()
    subscription_reserved = threading.Event()
    allow_subscription = threading.Event()
    shutdown_started = threading.Event()
    shutdown_finished = threading.Event()
    acquire_finished = threading.Event()
    primary = _acquire_direct_owner(runtime_identity=runtime_identity)

    async def operation() -> None:
        operation_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            operation_stopped.set()

    primary.start(operation, on_completion=lambda _settlement: None)
    assert operation_started.wait(timeout=1.0)
    original_reserve = primary.lifecycle.reserve_subscription

    def barrier_reserve(lifecycle):
        subscription_reserved.set()
        assert allow_subscription.wait(timeout=1.0)
        return original_reserve(lifecycle)

    monkeypatch.setattr(primary.lifecycle, "reserve_subscription", barrier_reserve)
    late_owners: list[optional_integrations_module.OptionalIntegrationExecutionOwner] = []

    def acquire_late_subscriber() -> None:
        try:
            owner = _acquire_direct_owner(runtime_identity=runtime_identity)
            owner.start(operation, on_completion=lambda _settlement: None)
            late_owners.append(owner)
        finally:
            acquire_finished.set()

    acquire_thread = threading.Thread(target=acquire_late_subscriber, daemon=True)
    acquire_thread.start()
    assert subscription_reserved.wait(timeout=1.0)

    def shutdown_primary() -> None:
        shutdown_started.set()
        try:
            asyncio.run(
                primary.shutdown(
                    timeout_seconds=1.0,
                    timeout_reason_code="slack_shutdown_timed_out",
                )
            )
        finally:
            shutdown_finished.set()

    shutdown_thread = threading.Thread(target=shutdown_primary, daemon=True)
    shutdown_thread.start()
    assert shutdown_started.wait(timeout=1.0)
    allow_subscription.set()
    assert acquire_finished.wait(timeout=1.0)
    assert shutdown_finished.wait(timeout=1.0)
    acquire_thread.join(timeout=0)
    shutdown_thread.join(timeout=0)

    assert len(late_owners) == 1
    assert not operation_stopped.is_set()

    asyncio.run(
        late_owners[0].shutdown(
            timeout_seconds=1.0,
            timeout_reason_code="slack_shutdown_timed_out",
        )
    )
    assert operation_stopped.wait(timeout=1.0)


def test_natural_slack_completion_rejects_late_subscribers_before_publication() -> None:
    """Callback collection closes acquisition before terminal publication."""
    runtime_identity = "optional-integration-natural-completion-race"
    operation_started = threading.Event()
    finish_operation = threading.Event()
    completion_committed = threading.Event()
    allow_completion = threading.Event()
    late_callback_called = threading.Event()
    primary = _acquire_direct_owner(runtime_identity=runtime_identity)

    async def operation() -> None:
        operation_started.set()
        while not finish_operation.is_set():
            await asyncio.sleep(0.001)

    def block_completion(
        _settlement: optional_integrations_module.OptionalIntegrationTaskSettlement,
    ) -> None:
        completion_committed.set()
        assert allow_completion.wait(timeout=1.0)

    primary.start(operation, on_completion=block_completion)
    assert operation_started.wait(timeout=1.0)
    finish_operation.set()
    assert completion_committed.wait(timeout=1.0)

    late_owner = None
    late_reason = None
    try:
        try:
            late_owner = _acquire_direct_owner(runtime_identity=runtime_identity)
            late_owner.start(
                operation,
                on_completion=lambda _settlement: late_callback_called.set(),
            )
        except optional_integrations_module.OptionalIntegrationExecutionUnavailable as exc:
            late_reason = exc.reason_code
    finally:
        allow_completion.set()
        asyncio.run(
            primary.shutdown(
                timeout_seconds=1.0,
                timeout_reason_code="slack_shutdown_timed_out",
            )
        )
        if late_owner is not None:
            asyncio.run(
                late_owner.shutdown(
                    timeout_seconds=1.0,
                    timeout_reason_code="slack_shutdown_timed_out",
                )
            )

    assert late_owner is None
    assert late_reason == "slack_execution_owner_stopping"
    assert not late_callback_called.is_set()


def test_noncooperative_slack_cannot_hold_asyncio_run_process_exit(tmp_path) -> None:
    """A permanently cancellation-resistant optional task cannot own process exit."""
    repository_root = os.fspath(Path(__file__).resolve().parents[2])
    child_environment = os.environ.copy()
    child_environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (os.fspath(os.environ.get("PYTHONPATH", "")), repository_root) if part
    )
    script = textwrap.dedent(f"""
        import asyncio
        import threading

        from fastapi import FastAPI

        import tacit.api.lifespan as lifespan_module
        import tacit.integrations.slack as slack_module
        from tacit.api.lifespan import create_lifespan
        from tacit.config import Settings
        from tacit.runtime_stores import RuntimeStores

        settings = Settings(
            _env_file=None,
            slack_bot_token="xoxb-runtime",
            slack_app_token="xapp-runtime",
            slack_signing_secret="signing-runtime",
            history_db_path={str(tmp_path / "child-history.db")!r},
            feedback_db_path={str(tmp_path / "child-feedback.db")!r},
            signals_db_path={str(tmp_path / "child-signals.db")!r},
        )
        stores = RuntimeStores(settings)
        app = FastAPI()
        app.state.settings = settings
        app.state.runtime_stores = stores
        started = threading.Event()

        class FakeSlackApp:
            def __init__(self, **_kwargs):
                pass

            def event(self, _name):
                return lambda callback: callback

            def command(self, _name):
                return lambda callback: callback

        class NoncooperativeSocketHandler:
            def __init__(self, _app, _token):
                pass

            async def connect_async(self):
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    while True:
                        try:
                            await asyncio.sleep(3600)
                        except asyncio.CancelledError:
                            pass

            async def close_async(self):
                return None

        slack_module.AsyncApp = FakeSlackApp
        slack_module.AsyncSocketModeHandler = NoncooperativeSocketHandler
        lifespan_module._OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS = 0.02

        async def run():
            async with create_lifespan(settings)(app):
                while not started.is_set():
                    await asyncio.sleep(0.001)

        asyncio.run(run())
        graph = stores.pipeline_admission().execution_graph
        assert graph.root_state == "closed"
        assert graph.root_owner_count == 0
        print("bounded-process-exit", flush=True)
        """)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repository_root,
        env=child_environment,
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines()[-1] == "bounded-process-exit"


async def test_overlapping_api_roots_share_slack_until_the_final_subscriber_exits(
    monkeypatch,
    tmp_path,
) -> None:
    """One runtime owns one Slack transport across overlapping app roots."""
    runtime_settings = _settings(tmp_path, suffix="overlapping-roots")
    runtime_stores = RuntimeStores(runtime_settings)
    started = threading.Event()
    stopped = threading.Event()
    invocation_count = 0

    async def cooperative_slack(_settings, *, stores, lifecycle):
        nonlocal invocation_count
        assert stores is runtime_stores
        invocation_count += 1
        lifecycle.publish(status="ready", reason_code="slack_ready")
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=cooperative_slack),
    )

    first_app = FastAPI()
    first_app.state.settings = runtime_settings
    first_app.state.runtime_stores = runtime_stores
    second_app = FastAPI()
    second_app.state.settings = runtime_settings
    second_app.state.runtime_stores = runtime_stores
    first_context = create_lifespan(runtime_settings)(first_app)
    second_context = create_lifespan(runtime_settings)(second_app)

    await first_context.__aenter__()
    await _wait_until(started.is_set)
    await second_context.__aenter__()
    try:
        await _wait_until(
            lambda: second_app.state.optional_integration_readiness.get("slack", {}).get("status") == "ready"
        )
        assert invocation_count == 1
        assert runtime_stores.pipeline_admission().execution_graph.root_owner_count == 2

        await first_context.__aexit__(None, None, None)
        assert not stopped.is_set()
        assert invocation_count == 1
        assert runtime_stores.pipeline_admission().execution_graph.root_owner_count == 1
        assert second_app.state.optional_integration_readiness["slack"] == {
            "status": "ready",
            "reason_code": "slack_ready",
        }
    finally:
        await second_context.__aexit__(None, None, None)

    await _wait_until(stopped.is_set)
    graph = runtime_stores.pipeline_admission().execution_graph
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert invocation_count == 1


async def test_noncooperative_slack_fences_restart_without_accumulating_owner_threads(
    monkeypatch,
    tmp_path,
) -> None:
    """A timed-out owner is process-fenced instead of duplicated on restart."""
    runtime_settings = _settings(tmp_path, suffix="fenced-restart")
    runtime_stores = RuntimeStores(runtime_settings)
    started = threading.Event()
    release = threading.Event()
    invocation_threads: list[int] = []
    baseline_threads = _owner_thread_count()

    async def noncooperative_slack(_settings, *, stores, lifecycle):
        assert stores is runtime_stores
        invocation_threads.append(threading.get_ident())
        lifecycle.publish(status="ready", reason_code="slack_ready")
        started.set()
        try:
            while not release.is_set():
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            while not release.is_set():
                try:
                    await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    pass
        lifecycle.publish(status="ready", reason_code="late_slack_ready")

    monkeypatch.setattr(lifespan_module, "_OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS", 0.02)
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=noncooperative_slack),
    )

    first_app = FastAPI()
    first_app.state.settings = runtime_settings
    first_app.state.runtime_stores = runtime_stores
    second_app = FastAPI()
    second_app.state.settings = runtime_settings
    second_app.state.runtime_stores = runtime_stores

    try:
        async with create_lifespan(runtime_settings)(first_app):
            await _wait_until(started.is_set)
            assert _owner_thread_count() == baseline_threads + 1

        assert first_app.state.optional_integration_readiness["slack"] == {
            "status": "failed",
            "reason_code": "slack_shutdown_timed_out",
        }
        assert runtime_stores.pipeline_admission().execution_graph.root_state == "closed"

        async with create_lifespan(runtime_settings)(second_app):
            await asyncio.sleep(0.03)
            assert len(invocation_threads) == 1
            assert _owner_thread_count() == baseline_threads + 1
            assert second_app.state.optional_integration_readiness["slack"] == {
                "status": "failed",
                "reason_code": "slack_execution_owner_fenced",
            }
    finally:
        release.set()
        await _wait_until(lambda: _owner_thread_count() == baseline_threads)

    assert first_app.state.optional_integration_readiness["slack"] == {
        "status": "failed",
        "reason_code": "slack_shutdown_timed_out",
    }
    assert len(invocation_threads) == 1


async def test_inner_slack_close_timeout_retains_and_fences_the_execution_owner(
    monkeypatch,
    tmp_path,
) -> None:
    """An unretired socket task keeps its daemon owner and blocks replacement."""
    import tacit.integrations.slack as slack_module

    runtime_settings = _settings(tmp_path, suffix="inner-close-fence")
    runtime_stores = RuntimeStores(runtime_settings)
    connected = threading.Event()
    close_started = threading.Event()
    close_cancelled = threading.Event()
    release_close = threading.Event()
    invocation_count = 0
    baseline_threads = _owner_thread_count()

    class FakeSlackApp:
        def __init__(self, **_kwargs):
            pass

        def event(self, _name):
            return lambda callback: callback

        def command(self, _name):
            return lambda callback: callback

    class CancellationResistantCloseHandler:
        def __init__(self, _app, _token):
            nonlocal invocation_count
            invocation_count += 1

        async def connect_async(self) -> None:
            connected.set()

        async def close_async(self) -> None:
            close_started.set()
            try:
                while not release_close.is_set():
                    await asyncio.sleep(0.001)
            except asyncio.CancelledError:
                close_cancelled.set()
                while not release_close.is_set():
                    try:
                        await asyncio.sleep(0.001)
                    except asyncio.CancelledError:
                        pass

    monkeypatch.setattr(slack_module, "AsyncApp", FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", CancellationResistantCloseHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CLOSE_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(lifespan_module, "_OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS", 0.05)

    first_app = FastAPI()
    first_app.state.settings = runtime_settings
    first_app.state.runtime_stores = runtime_stores
    second_app = FastAPI()
    second_app.state.settings = runtime_settings
    second_app.state.runtime_stores = runtime_stores

    try:
        async with create_lifespan(runtime_settings)(first_app):
            await _wait_until(connected.is_set)

        await _wait_until(close_started.is_set)
        await _wait_until(close_cancelled.is_set)
        assert _owner_thread_count() == baseline_threads + 1
        assert first_app.state.optional_integration_readiness["slack"] == {
            "status": "failed",
            "reason_code": "slack_shutdown_timed_out",
        }

        async with create_lifespan(runtime_settings)(second_app):
            await asyncio.sleep(0.02)
            assert invocation_count == 1
            assert second_app.state.optional_integration_readiness["slack"] == {
                "status": "failed",
                "reason_code": "slack_execution_owner_fenced",
            }
    finally:
        release_close.set()
        await _wait_until(lambda: _owner_thread_count() == baseline_threads)

    assert invocation_count == 1


async def test_retained_slack_health_probe_keeps_execution_owner_fenced_until_retirement(
    monkeypatch,
    tmp_path,
) -> None:
    """The real Slack path cannot drop a retained probe with plain cancellation."""
    import tacit.integrations.slack as slack_module

    runtime_settings = _settings(tmp_path, suffix="retained-health-probe-fence")
    runtime_stores = RuntimeStores(runtime_settings)
    connected = threading.Event()
    probe_started = threading.Event()
    probe_cancelled = threading.Event()
    release_probe = threading.Event()
    observed_events: list[tuple[str, dict[str, object]]] = []
    probe_calls = 0
    baseline_threads = _owner_thread_count()

    class FakeSlackApp:
        def __init__(self, **_kwargs):
            pass

        def event(self, _name):
            return lambda callback: callback

        def command(self, _name):
            return lambda callback: callback

    class CancellationResistantProbeClient:
        async def is_connected(self) -> bool:
            nonlocal probe_calls
            probe_calls += 1
            probe_started.set()
            while not release_probe.is_set():
                try:
                    await asyncio.sleep(0.001)
                except asyncio.CancelledError:
                    probe_cancelled.set()
            return True

    class ProbeHandler:
        def __init__(self, _app, _token):
            self.client = CancellationResistantProbeClient()

        async def connect_async(self) -> None:
            connected.set()

        async def close_async(self) -> None:
            return None

    monkeypatch.setattr(slack_module, "AsyncApp", FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", ProbeHandler)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_MONITOR_INTERVAL_SECONDS", 0.001)
    monkeypatch.setattr(slack_module, "_SLACK_CONNECTION_PROBE_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(lifespan_module, "_OPTIONAL_INTEGRATION_SHUTDOWN_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        optional_integrations_module,
        "logger",
        SimpleNamespace(
            warning=lambda event, **fields: observed_events.append((event, fields)),
        ),
        raising=False,
    )

    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    runtime_identity = runtime_stores.runtime_ownership.admission_namespace
    assert runtime_identity is not None

    try:
        async with create_lifespan(runtime_settings)(app):
            await _wait_until(connected.is_set)
            await _wait_until(probe_started.is_set)
            await _wait_until(probe_cancelled.is_set)

        await _wait_until(lambda: _owner_thread_count() == baseline_threads + 1)
        with pytest.raises(
            optional_integrations_module.OptionalIntegrationExecutionUnavailable,
            match="slack_execution_owner_fenced",
        ):
            _acquire_direct_owner(runtime_identity=runtime_identity)

        retained_events = [fields for event, fields in observed_events if event == "optional_integration_retained_work"]
        assert len(retained_events) == 1
        assert retained_events[0]["detached_task_count"] == 1
        assert retained_events[0]["pending_task_count"] >= 1
        assert runtime_identity not in repr(retained_events[0])
        assert probe_calls == 1
    finally:
        release_probe.set()
        await _wait_until(lambda: _owner_thread_count() == baseline_threads)

    assert probe_calls == 1
    replacement = _acquire_direct_owner(runtime_identity=runtime_identity)
    replacement_settlement = await replacement.shutdown(
        timeout_seconds=0.1,
        timeout_reason_code="slack_shutdown_timed_out",
    )
    assert replacement_settlement.completed


async def test_retained_done_callback_runs_before_execution_owner_closes_its_loop() -> None:
    lifecycle = optional_integrations_module.OptionalIntegrationLifecycle(None, name="slack")
    owner = _acquire_direct_owner(
        runtime_identity="retained-done-callback-drain",
        lifecycle=lifecycle,
    )
    state = owner._state

    async def cancelled_after_deferring_retention() -> None:
        completed = asyncio.create_task(asyncio.sleep(0))
        await completed
        asyncio.get_running_loop().call_soon(owner.lifecycle.retain_detached_task, completed)
        raise asyncio.CancelledError

    owner.start(cancelled_after_deferring_retention, on_completion=lambda _settlement: None)
    settlement = await asyncio.wait_for(asyncio.wrap_future(state.completion), timeout=1.0)
    assert settlement.cancelled
    assert await asyncio.to_thread(state.finished.wait, 1.0)
    assert state.lifecycle.detached_task_count == 0


async def test_unregistered_cancellation_resistant_child_retains_identity_until_retired() -> None:
    runtime_identity = "unregistered-cancellation-resistant-child"
    owner = _acquire_direct_owner(runtime_identity=runtime_identity)
    state = owner._state
    child_started = threading.Event()
    child_cancelled = threading.Event()
    release_child = threading.Event()

    async def child() -> None:
        child_started.set()
        while not release_child.is_set():
            try:
                await asyncio.sleep(0.001)
            except asyncio.CancelledError:
                child_cancelled.set()

    async def operation() -> None:
        asyncio.create_task(child(), name="unregistered-sdk-child")
        while not child_started.is_set():
            await asyncio.sleep(0)

    try:
        owner.start(operation, on_completion=lambda _settlement: None)
        settlement = await asyncio.wait_for(asyncio.wrap_future(state.completion), timeout=1.0)
        assert settlement.completed
        assert await asyncio.to_thread(child_cancelled.wait, 1.0)
        assert state.finished.is_set() is False
        assert owner.thread_alive
        assert state.key in optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
        assert state.key in optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS

        with pytest.raises(
            optional_integrations_module.OptionalIntegrationExecutionUnavailable,
            match="slack_execution_owner_fenced",
        ):
            _acquire_direct_owner(runtime_identity=runtime_identity)
    finally:
        release_child.set()

    assert await asyncio.to_thread(state.finished.wait, 1.0)
    assert state.key not in optional_integrations_module._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
    assert state.key not in optional_integrations_module._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS

    replacement = _acquire_direct_owner(runtime_identity=runtime_identity)
    replacement_settlement = await replacement.shutdown(
        timeout_seconds=0.1,
        timeout_reason_code="slack_shutdown_timed_out",
    )
    assert replacement_settlement.completed


async def test_deferred_sdk_child_is_retired_before_owner_loop_close() -> None:
    owner = _acquire_direct_owner(runtime_identity="deferred-sdk-child-before-loop-close")
    state = owner._state
    child_holder: list[asyncio.Task[None]] = []
    child_retired = threading.Event()

    async def child() -> None:
        try:
            await asyncio.sleep(0)
        finally:
            child_retired.set()

    async def operation() -> None:
        loop = asyncio.get_running_loop()

        def create_child() -> None:
            child_holder.append(loop.create_task(child(), name="deferred-sdk-child"))

        def defer_child(remaining: int) -> None:
            if remaining:
                if remaining % 2:
                    loop.call_soon(defer_child, remaining - 1)
                else:
                    loop.call_later(0, defer_child, remaining - 1)
                return
            create_child()

        loop.call_soon(defer_child, 64)

    try:
        owner.start(operation, on_completion=lambda _settlement: None)
        assert await asyncio.to_thread(state.finished.wait, 1.0)
        assert len(child_holder) == 1
        assert child_holder[0].done()
        assert child_retired.is_set()
    finally:
        if child_holder and not child_holder[0].done():
            child_holder[0]._log_destroy_pending = False
            child_holder[0].get_coro().close()


def test_non_quiescent_sdk_callback_chain_retains_fenced_owner_without_spinning() -> None:
    repository_root = os.fspath(Path(__file__).resolve().parents[2])
    child_environment = os.environ.copy()
    child_environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (os.fspath(os.environ.get("PYTHONPATH", "")), repository_root) if part
    )
    script = textwrap.dedent("""
        import asyncio
        import time

        import tacit.api.optional_integrations as optional_integrations

        runtime_identity = "non-quiescent-sdk-callback-chain"
        lifecycle = optional_integrations.OptionalIntegrationLifecycle(None, name="slack")
        owner = optional_integrations.OptionalIntegrationExecutionOwner.acquire(
            name="slack",
            runtime_identity=runtime_identity,
            lifecycle=lifecycle,
        )
        state = owner._state

        async def operation():
            loop = asyncio.get_running_loop()

            def reschedule():
                loop.call_soon(reschedule)

            loop.call_soon(reschedule)

        owner.start(operation, on_completion=lambda _settlement: None)
        settlement = state.completion.result(timeout=2.0)
        assert settlement.completed
        assert state.loop is not None
        callback_drain = state.loop.call_soon.__self__
        scheduled_before = callback_drain._scheduled_count
        time.sleep(0.25)
        assert callback_drain._scheduled_count == scheduled_before
        assert not state.finished.is_set()
        assert state.thread is not None and state.thread.is_alive()
        assert state.key in optional_integrations._OPTIONAL_INTEGRATION_EXECUTION_OWNERS
        assert state.key in optional_integrations._FENCED_OPTIONAL_INTEGRATION_EXECUTION_OWNERS

        try:
            optional_integrations.OptionalIntegrationExecutionOwner.acquire(
                name="slack",
                runtime_identity=runtime_identity,
                lifecycle=optional_integrations.OptionalIntegrationLifecycle(None, name="slack"),
            )
        except optional_integrations.OptionalIntegrationExecutionUnavailable as exc:
            assert exc.reason_code == "slack_execution_owner_fenced"
        else:
            raise AssertionError("non-quiescent callback owner admitted a replacement")

        print("non-quiescent-owner-fenced", flush=True)
        """)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repository_root,
        env=child_environment,
        capture_output=True,
        text=True,
        timeout=5.0,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.splitlines()[-1] == "non-quiescent-owner-fenced"


async def test_clean_slack_shutdown_allows_one_later_in_process_generation(
    monkeypatch,
    tmp_path,
) -> None:
    """A cooperative owner exits fully and permits an explicit later generation."""
    runtime_settings = _settings(tmp_path, suffix="clean-restart")
    runtime_stores = RuntimeStores(runtime_settings)
    invocation_threads: list[int] = []
    baseline_threads = _owner_thread_count()

    async def cooperative_slack(_settings, *, stores, lifecycle):
        assert stores is runtime_stores
        invocation_threads.append(threading.get_ident())
        lifecycle.publish(status="ready", reason_code="slack_ready")
        await asyncio.Event().wait()

    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=cooperative_slack),
    )

    for _generation in range(2):
        app = FastAPI()
        app.state.settings = runtime_settings
        app.state.runtime_stores = runtime_stores
        async with create_lifespan(runtime_settings)(app):
            await _wait_until(lambda: len(invocation_threads) == _generation + 1)
            assert _owner_thread_count() == baseline_threads + 1
        await _wait_until(lambda: _owner_thread_count() == baseline_threads)
        assert runtime_stores.pipeline_admission().execution_graph.root_state == "closed"

    assert len(invocation_threads) == 2
    assert all(thread_id != threading.get_ident() for thread_id in invocation_threads)


async def test_owner_start_failure_degrades_slack_without_leaking_the_api_root(
    monkeypatch,
    tmp_path,
) -> None:
    """Definite owner-start failure has no Slack effects and no root leak."""
    runtime_settings = _settings(tmp_path, suffix="owner-start-failure")
    runtime_stores = RuntimeStores(runtime_settings)
    app = FastAPI()
    app.state.settings = runtime_settings
    app.state.runtime_stores = runtime_stores
    starter_calls = 0

    async def should_not_start(_settings, *, stores, lifecycle):
        nonlocal starter_calls
        del stores, lifecycle
        starter_calls += 1

    def fail_thread_construction(*, target, name, daemon):
        del target, name, daemon
        raise RuntimeError("private-thread-start-failure")

    monkeypatch.setattr(
        optional_integrations_module,
        "_new_optional_integration_thread",
        fail_thread_construction,
        raising=False,
    )
    monkeypatch.setitem(
        sys.modules,
        "tacit.integrations.slack",
        SimpleNamespace(start_slack_bot=should_not_start),
    )

    async with create_lifespan(runtime_settings)(app):
        assert starter_calls == 0
        assert app.state.optional_integration_readiness["slack"] == {
            "status": "failed",
            "reason_code": "slack_execution_owner_start_failed",
        }

    assert runtime_stores.pipeline_admission().execution_graph.root_state == "closed"
    assert starter_calls == 0
