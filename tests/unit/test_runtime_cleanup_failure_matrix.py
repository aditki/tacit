"""Failure matrix for primary errors that overlap terminal cleanup failures.

These tests intentionally exercise the shared lifecycle contract rather than
one caller's happy-path exception handling.  Cleanup remains charged while it
is executable, terminal failure is reported separately from the primary
operation failure, and only bounded message-free metadata survives retirement.
"""

from __future__ import annotations

import asyncio
import gc
import re
import sys
import threading
import time
import weakref
from dataclasses import FrozenInstanceError, dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import tacit.agents.providers.bedrock as bedrock_module
import tacit.dependencies as dependencies_module
import tacit.pipeline.runner as runner_module
import tacit.pipeline.side_effects as side_effects_module
import tacit.pipeline_admission as pipeline_admission_module
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.agents.providers.bedrock import BedrockProvider, _ResolvedBedrockRuntime
from tacit.config import Settings
from tacit.dependencies import ProviderLifecycleState, _RuntimeProviderResources, declare_runtime_factory
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline.runner import _cleanup_pipeline_resources
from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork
from tacit.pipeline_admission import PipelineAdmissionController, RuntimeFatalCircuit, RuntimeRootOwnerHandle
from tacit.runtime_ownership import (
    BedrockCredentialIdentity,
    RuntimeOwnershipMismatchError,
    credential_fingerprint,
    runtime_descriptor_for_provider,
)

_PRIMARY_SECRET = "PRIMARY_OPERATION_SECRET=must-not-escape"
_CLEANUP_SECRET = "TERMINAL_CLEANUP_SECRET=must-not-escape"
_ROLE_ACCOUNT = "arn:aws:iam::123456789012:role/tacitruntime"
_REPEATED_CLEANUP_FAULTS = 4


@pytest.fixture(autouse=True)
def _isolated_runtime_fatal_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model process restart without exposing a production reset operation."""
    monkeypatch.setattr(
        pipeline_admission_module,
        "_PROCESS_RUNTIME_FATAL_REGISTRY",
        pipeline_admission_module._ProcessRuntimeFatalRegistry(limit=1024),
    )


async def _collect_after_async_frames_settle(*references: weakref.ReferenceType[Any]) -> bool:
    for _attempt in range(10):
        await asyncio.sleep(0)
        gc.collect()
        if all(reference() is None for reference in references):
            return True
    return False


async def _assert_terminal_cleanup_fenced_runtime(
    lifecycle: PipelineAdmissionController,
    *,
    root: RuntimeRootOwnerHandle,
    reason_code: str,
    error_type: str,
) -> None:
    deadline = time.monotonic() + 1
    while lifecycle.blocking_in_flight != 0 and time.monotonic() < deadline:
        await asyncio.sleep(0)

    _assert_runtime_capacity(
        lifecycle,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )
    fatal = lifecycle.runtime_fatal_circuit
    assert fatal is not None
    assert fatal.reason_code == reason_code
    assert fatal.error_type == error_type
    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await lifecycle.acquire()

    cleanup_permit = (await lifecycle.acquire_cleanup_permits(1))[0]
    lifecycle.release_blocking_permit(cleanup_permit)
    await asyncio.wait_for(lifecycle.execution_graph.release_root_owner(root), timeout=1)
    assert lifecycle.execution_graph.root_state == "closed"
    assert lifecycle.execution_graph.root_owner_count == 0

    registry = pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY
    assert len(registry._records) == 1
    assert registry._overflow is None
    assert all(re.fullmatch(r"[0-9a-f]{64}", key) for key in registry._records)
    assert all(isinstance(item, RuntimeFatalCircuit) for item in registry._records.values())


def _observe_fatal_fence_before_release(
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: PipelineAdmissionController,
) -> list[str]:
    events: list[str] = []
    original_fence = lifecycle.fence_runtime_fatal
    original_release = lifecycle.release_blocking_permit

    def observe_fence(error: BaseException) -> RuntimeFatalCircuit:
        assert lifecycle.blocking_in_flight == 1
        events.append("fatal_fence")
        return original_fence(error)

    def observe_release(permit: Any) -> None:
        if not permit.cleanup:
            assert lifecycle.runtime_fatal_circuit is not None
            events.append("capacity_release")
        original_release(permit)

    monkeypatch.setattr(lifecycle, "fence_runtime_fatal", observe_fence)
    monkeypatch.setattr(lifecycle, "release_blocking_permit", observe_release)
    return events


class _PrimaryOperationFailure(RuntimeError):
    """Synthetic primary failure whose message must never enter diagnostics."""


class _TerminalCleanupFailure(RuntimeError):
    """Synthetic cleanup failure whose message must never enter diagnostics."""


@dataclass(frozen=True, slots=True)
class _LogRecord:
    level: str
    event: str
    fields: dict[str, Any]


class _RecordingLogger:
    def __init__(self) -> None:
        self.records: list[_LogRecord] = []

    def _record(self, level: str, event: str, **fields: Any) -> None:
        self.records.append(_LogRecord(level=level, event=event, fields=fields))

    def debug(self, event: str, **fields: Any) -> None:
        self._record("debug", event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._record("info", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._record("warning", event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._record("error", event, **fields)


def test_terminal_cleanup_failure_cannot_be_configured_to_fail_open() -> None:
    failure = side_effects_module.terminal_cleanup_failure(
        _PrimaryOperationFailure(_PRIMARY_SECRET),
        _TerminalCleanupFailure(_CLEANUP_SECRET),
        reason_code="terminal_cleanup_policy",
        message="Pipeline runtime cleanup failed",
        retain_capacity=False,
    )

    assert getattr(failure, "cleanup_retains_capacity", False) is True


def _exception_chain(error: BaseException) -> tuple[BaseException, ...]:
    pending = [error]
    visited: set[int] = set()
    result: list[BaseException] = []
    while pending:
        current = pending.pop()
        if id(current) in visited:
            continue
        visited.add(id(current))
        result.append(current)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(current.exceptions)
    return tuple(result)


def _assert_stable_terminal_error(
    error: BaseException,
    *,
    primary_type: type[BaseException],
) -> None:
    assert isinstance(error, RuntimeOwnershipError)
    assert "cleanup failed" in str(error).casefold()
    assert _PRIMARY_SECRET not in str(error)
    assert _CLEANUP_SECRET not in str(error)
    assert any(isinstance(item, primary_type) for item in _exception_chain(error)[1:])


def _assert_sanitized_logs(logs: _RecordingLogger) -> None:
    rendered = repr(logs.records)
    assert _PRIMARY_SECRET not in rendered
    assert _CLEANUP_SECRET not in rendered
    assert "Traceback" not in rendered


def _assert_cleanup_fault_contract(
    *,
    errors: list[BaseException],
    resources: list[_CloseProbe],
    lifecycle: PipelineAdmissionController,
    logs: _RecordingLogger,
    primary_type: type[BaseException],
    reason_code: str,
) -> None:
    """Report every terminal-cleanup invariant violated by one fault family."""
    violations: list[str] = []
    if not resources:
        violations.append("fault injection did not construct a cleanup-owned resource")
    if len(errors) < len(resources):
        violations.append("one or more constructed resources produced no terminal result")

    for index, error in enumerate(errors[: len(resources)]):
        chain = _exception_chain(error)
        if not isinstance(error, RuntimeOwnershipError):
            violations.append(f"attempt {index}: cleanup failure was not the terminal runtime error")
        if "cleanup failed" not in str(error).casefold():
            violations.append(f"attempt {index}: terminal error did not identify cleanup failure")
        if not any(isinstance(item, primary_type) for item in chain[1:]):
            violations.append(f"attempt {index}: primary causal error was not preserved")
        if getattr(error, "cleanup_reason_code", None) != reason_code:
            violations.append(f"attempt {index}: cleanup reason was not durably attached")
        cleanup_type = getattr(error, "cleanup_error_type", None)
        if cleanup_type != _TerminalCleanupFailure.__name__:
            violations.append(f"attempt {index}: cleanup error type was not durably attached")
        if not isinstance(cleanup_type, str) or len(cleanup_type) > 128:
            violations.append(f"attempt {index}: cleanup metadata was not bounded")
        rendered_error = repr(error)
        if _PRIMARY_SECRET in rendered_error or _CLEANUP_SECRET in rendered_error:
            violations.append(f"attempt {index}: terminal error disclosed failure text")

    if any(resource.close_calls != 1 for resource in resources):
        violations.append("a cleanup-owned resource was not closed exactly once")
    if any(resource.retired for resource in resources):
        violations.append("a failed close was falsely recorded as retired")
    live_resources = [resource for resource in resources if resource.executable]
    if len(live_resources) > 1:
        violations.append("repeated cleanup faults accumulated executable resources beyond capacity")
    capacity_is_free = (
        lifecycle.in_flight == 0
        and lifecycle.blocking_in_flight == 0
        and lifecycle.retained == 0
        and lifecycle.service_owner_in_flight == 0
    )
    fatal = lifecycle.runtime_fatal_circuit
    if not capacity_is_free:
        violations.append("fatal-fenced runtime retained capacity after dropping its executable references")
    if fatal is None:
        violations.append("terminal cleanup failure did not fatal-fence the runtime")
    elif (
        fatal.reason_code != pipeline_admission_module._canonical_runtime_fatal_reason(reason_code)
        or fatal.error_type != "RuntimeOwnershipError"
    ):
        violations.append("fatal-fence metadata did not preserve the bounded cleanup identity")

    rendered_logs = repr(logs.records)
    if _PRIMARY_SECRET in rendered_logs or _CLEANUP_SECRET in rendered_logs or "Traceback" in rendered_logs:
        violations.append("cleanup diagnostics were not sanitized")
    cleanup_records = [
        record for record in logs.records if record.event in {reason_code, "pipeline_blocking_work_cleanup_failed"}
    ]
    if not cleanup_records:
        violations.append("cleanup failure produced no bounded diagnostic")
    for record in cleanup_records:
        if set(record.fields) - {"reason_code", "error_type"}:
            violations.append("cleanup diagnostic retained fields beyond the bounded schema")
            break
        if any(len(str(value)) > 128 for value in record.fields.values()):
            violations.append("cleanup diagnostic metadata exceeded its bound")
            break

    assert not violations, "terminal cleanup contract violations:\n- " + "\n- ".join(violations)


def _assert_runtime_capacity(
    lifecycle: PipelineAdmissionController,
    *,
    in_flight: int,
    blocking: int,
    retained: int,
    service_owners: int,
) -> None:
    assert lifecycle.in_flight == in_flight
    assert lifecycle.blocking_in_flight == blocking
    assert lifecycle.retained == retained
    assert lifecycle.service_owner_in_flight == service_owners


@pytest.mark.asyncio
async def test_runtime_fatal_fence_rejects_queued_work_but_allows_terminal_cleanup() -> None:
    lifecycle = PipelineAdmissionController(
        1,
        max_queued=1,
        runtime_identity="fatal-queue-and-cleanup-runtime",
    )
    graph = lifecycle.execution_graph
    root = graph.register_root_owner()
    active = await lifecycle.acquire()
    queued = asyncio.create_task(lifecycle.acquire(timeout_seconds=1))
    deadline = time.monotonic() + 1
    while lifecycle.queued != 1 and time.monotonic() < deadline:
        await asyncio.sleep(0)
    assert lifecycle.queued == 1

    failure = side_effects_module.terminal_cleanup_failure(
        None,
        _TerminalCleanupFailure(_CLEANUP_SECRET),
        reason_code="fatal_queue_cleanup_failure",
        message="Pipeline runtime cleanup failed",
        retain_capacity=True,
    )
    fatal = lifecycle.fence_runtime_fatal(failure)

    with pytest.raises(RuntimeOwnershipError) as queued_error:
        await queued
    assert getattr(queued_error.value, "cleanup_reason_code", None) == fatal.reason_code
    assert getattr(queued_error.value, "cleanup_error_type", None) == fatal.error_type
    with pytest.raises(RuntimeOwnershipError):
        await lifecycle.acquire()
    with pytest.raises(RuntimeOwnershipError):
        graph.register_root_owner()

    lifecycle.release(active)
    cleanup_permit = (await lifecycle.acquire_cleanup_permits(1))[0]
    lifecycle.release_blocking_permit(cleanup_permit)

    replacement = side_effects_module.terminal_cleanup_failure(
        None,
        RuntimeError("replacement metadata must not win"),
        reason_code="replacement_cleanup_failure",
        message="Pipeline runtime cleanup failed",
        retain_capacity=True,
    )
    assert lifecycle.fence_runtime_fatal(replacement) is fatal
    assert lifecycle.runtime_fatal_circuit is fatal
    await graph.release_root_owner(root)
    assert graph.root_owner_count == 0
    assert graph.root_state == "closed"
    assert lifecycle.queued == 0
    _assert_runtime_capacity(
        lifecycle,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )


@pytest.mark.asyncio
async def test_shared_runtime_fatal_registry_survives_controller_collection_and_isolates_identities() -> None:
    runtime_identity = "tenant-secret=/private/runtime-collection?token=must-not-survive"
    lifecycle = PipelineAdmissionController(1, runtime_identity=runtime_identity)
    failure = side_effects_module.terminal_cleanup_failure(
        None,
        _TerminalCleanupFailure(_CLEANUP_SECRET),
        reason_code="shared_runtime_cleanup_failed",
        message="Pipeline runtime cleanup failed",
        retain_capacity=True,
    )
    record = lifecycle.fence_runtime_fatal(failure)

    with pytest.raises(RuntimeOwnershipError):
        await lifecycle.acquire()
    lifecycle_ref = weakref.ref(lifecycle)
    del lifecycle
    assert await _collect_after_async_frames_settle(lifecycle_ref)

    registry = pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY
    assert len(registry._records) == 1
    key = next(iter(registry._records))
    assert re.fullmatch(r"[0-9a-f]{64}", key)
    assert runtime_identity not in key
    assert registry._records[key] is record
    assert isinstance(record, RuntimeFatalCircuit)

    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        PipelineAdmissionController(1, runtime_identity=runtime_identity)

    isolated = PipelineAdmissionController(1, runtime_identity="independent-runtime")
    root = isolated.execution_graph.register_root_owner()
    lease = await isolated.acquire()
    isolated.release(lease)
    await isolated.execution_graph.release_root_owner(root)
    assert isolated.execution_graph.root_owner_count == 0
    assert isolated.runtime_root_state == "closed"
    _assert_runtime_capacity(
        isolated,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )


def test_runtime_fatal_metadata_rejects_noncanonical_exception_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs = _RecordingLogger()
    monkeypatch.setattr(pipeline_admission_module, "logger", logs)
    runtime_identity = "tenant-secret=/private/runtime-metadata?token=must-not-survive"
    lifecycle = PipelineAdmissionController(1, runtime_identity=runtime_identity)
    failure = RuntimeError("message must not survive")
    setattr(failure, "cleanup_reason_code", f"reason={_CLEANUP_SECRET}/private/runtime")
    setattr(failure, "cleanup_error_type", f"type={_PRIMARY_SECRET}/private/runtime")

    record = lifecycle.fence_runtime_fatal(failure)

    assert record == RuntimeFatalCircuit(
        reason_code="runtime_cleanup_failed",
        error_type="RuntimeError",
    )
    retained = repr(pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY._records)
    rendered_logs = repr(logs.records)
    for forbidden in (_PRIMARY_SECRET, _CLEANUP_SECRET, runtime_identity, "/private/runtime"):
        assert forbidden not in retained
        assert forbidden not in rendered_logs


def test_runtime_fatal_metadata_rejects_credential_shaped_canonical_attributes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs = _RecordingLogger()
    monkeypatch.setattr(pipeline_admission_module, "logger", logs)
    lifecycle = PipelineAdmissionController(1, runtime_identity="credential-shaped-metadata")
    failure = RuntimeError("message must not survive")
    synthetic_aws_key = "".join(("AK", "IA", "ABCDEFGHIJKLMNOP"))
    setattr(failure, "cleanup_reason_code", "secretcredential123456789")
    setattr(failure, "cleanup_error_type", synthetic_aws_key)

    record = lifecycle.fence_runtime_fatal(failure)

    assert record == RuntimeFatalCircuit(
        reason_code="runtime_cleanup_failed",
        error_type="RuntimeError",
    )
    retained = repr(pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY._records)
    rendered_logs = repr(logs.records)
    assert "secretcredential123456789" not in retained
    assert "secretcredential123456789" not in rendered_logs
    assert synthetic_aws_key not in retained
    assert synthetic_aws_key not in rendered_logs


def test_runtime_fatal_registry_notifies_only_matching_runtime_identity() -> None:
    registry = pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY
    first = PipelineAdmissionController(1, runtime_identity="shared-runtime-a")
    unrelated = PipelineAdmissionController(1, runtime_identity="independent-runtime-b")

    record, latched, controllers = registry.latch(
        first.runtime_identity,
        RuntimeFatalCircuit(
            reason_code="runtime_cleanup_failed",
            error_type="RuntimeError",
        ),
    )

    assert record.reason_code == "runtime_cleanup_failed"
    assert latched is True
    assert set(controllers) == {first}
    assert unrelated not in controllers


def test_generated_isolated_runtime_identities_are_never_reused_after_collection() -> None:
    first = PipelineAdmissionController(1)
    first_identity = first.runtime_identity
    failure = side_effects_module.terminal_cleanup_failure(
        None,
        _TerminalCleanupFailure(_CLEANUP_SECRET),
        reason_code="isolated_runtime_cleanup_failed",
        message="Pipeline runtime cleanup failed",
        retain_capacity=True,
    )
    first.fence_runtime_fatal(failure)
    first_ref = weakref.ref(first)
    del first
    gc.collect()
    assert first_ref() is None

    observed = {first_identity}
    for _index in range(64):
        isolated = PipelineAdmissionController(1)
        assert isolated.runtime_identity not in observed
        observed.add(isolated.runtime_identity)
        del isolated
        gc.collect()


@pytest.mark.asyncio
async def test_shared_runtime_fatal_registry_is_bounded_and_overflow_fails_closed_globally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_type = pipeline_admission_module._ProcessRuntimeFatalRegistry
    registry = registry_type(limit=2)
    monkeypatch.setattr(pipeline_admission_module, "_PROCESS_RUNTIME_FATAL_REGISTRY", registry)
    identities = [f"fatal-runtime-{index}" for index in range(3)]
    controller_refs: list[weakref.ReferenceType[PipelineAdmissionController]] = []

    selected_controller = PipelineAdmissionController(1, max_queued=1, runtime_identity="selected-runtime")
    queued_controller = PipelineAdmissionController(1, max_queued=1, runtime_identity="queued-runtime")
    selected_active = await selected_controller.acquire()
    queued_active = await queued_controller.acquire()
    selected_waiter = asyncio.create_task(selected_controller.acquire())
    queued_waiter = asyncio.create_task(queued_controller.acquire())
    deadline = time.monotonic() + 1
    while (selected_controller.queued != 1 or queued_controller.queued != 1) and time.monotonic() < deadline:
        await asyncio.sleep(0)
    assert selected_controller.queued == queued_controller.queued == 1

    # Select one waiter without yielding to its transport loop. Overflow must
    # reject both selected and still-queued work in every live controller.
    selected_controller.release(selected_active)

    for index, runtime_identity in enumerate(identities):
        lifecycle = PipelineAdmissionController(1, runtime_identity=runtime_identity)
        controller_refs.append(weakref.ref(lifecycle))
        failure = side_effects_module.terminal_cleanup_failure(
            None,
            _TerminalCleanupFailure(_CLEANUP_SECRET),
            reason_code=f"fatal_cleanup_{index}",
            message="Pipeline runtime cleanup failed",
            retain_capacity=True,
        )
        lifecycle.fence_runtime_fatal(failure)
        del lifecycle

    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await selected_waiter
    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await queued_waiter
    queued_controller.release(queued_active)
    assert selected_controller.queued == queued_controller.queued == 0

    gc.collect()
    assert all(reference() is None for reference in controller_refs)
    assert len(registry._records) == 2
    assert all(re.fullmatch(r"[0-9a-f]{64}", key) for key in registry._records)
    assert all(identity not in key for identity in identities for key in registry._records)
    assert all(isinstance(item, RuntimeFatalCircuit) for item in registry._records.values())

    overflow = registry._overflow
    assert isinstance(overflow, RuntimeFatalCircuit)
    assert overflow.reason_code == "runtime_fatal_registry_capacity_exhausted"
    with pytest.raises(FrozenInstanceError):
        setattr(overflow, "reason_code", "mutable")
    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        PipelineAdmissionController(1, runtime_identity="otherwise-independent-runtime")
    assert len(registry._records) == 2


class _CloseProbe:
    def __init__(
        self,
        lifecycle: PipelineAdmissionController,
        *,
        fail: bool = False,
        started: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        self._lifecycle = lifecycle
        self._fail = fail
        self._started = started
        self._release = release
        self.close_calls = 0
        self.close_capacity: list[int] = []
        self.executable = True
        self.retired = False

    def close(self) -> None:
        self.close_calls += 1
        self.close_capacity.append(self._lifecycle.blocking_in_flight)
        if self._started is not None:
            self._started.set()
        if self._release is not None:
            assert self._release.wait(timeout=2), "test did not release terminal cleanup"
        if self._fail:
            raise _TerminalCleanupFailure(_CLEANUP_SECRET)
        self.executable = False
        self.retired = True


class _FailingBedrockClient(_CloseProbe):
    def converse(self, **_kwargs: object) -> dict[str, Any]:
        raise _PrimaryOperationFailure(_PRIMARY_SECRET)


class _BedrockSession(_CloseProbe):
    def __init__(self, lifecycle: PipelineAdmissionController, runtime_client: _FailingBedrockClient, **kwargs: Any):
        super().__init__(lifecycle, **kwargs)
        self._runtime_client = runtime_client

    def client(self, service_name: str, **_kwargs: object) -> _FailingBedrockClient:
        assert service_name == "bedrock-runtime"
        return self._runtime_client


def _bedrock_settings(*, signals_db_path: str = "data/signals.db") -> Settings:
    return Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        signals_db_path=signals_db_path,
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failing_resource", ("runtime_client", "credential_client", "session"))
async def test_bedrock_dual_failure_reports_terminal_cleanup_without_releasing_capacity_early(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failing_resource: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    provider = BedrockProvider(_bedrock_settings(signals_db_path=str(tmp_path / f"signals-{failing_resource}.db")))
    provider.bind_pipeline_lifecycle(lifecycle)
    root = lifecycle.execution_graph.register_root_owner()
    terminal_events = _observe_fatal_fence_before_release(monkeypatch, lifecycle)
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    logs = _RecordingLogger()

    def cleanup_options(name: str) -> dict[str, object]:
        if name != failing_resource:
            return {}
        return {
            "fail": True,
            "started": cleanup_started,
            "release": release_cleanup,
        }

    runtime_client = _FailingBedrockClient(lifecycle, **cleanup_options("runtime_client"))
    credential_client = _CloseProbe(lifecycle, **cleanup_options("credential_client"))
    session = _BedrockSession(
        lifecycle,
        runtime_client,
        **cleanup_options("session"),
    )
    realized = _ResolvedBedrockRuntime(
        session=session,
        credential_identity=BedrockCredentialIdentity(
            account=_ROLE_ACCOUNT,
            credential_fingerprint=credential_fingerprint("temporary-generation"),
            uses_sts=True,
        ),
        credential_clients=(credential_client,),
    )
    realized_products = [realized]
    monkeypatch.setattr(bedrock_module, "logger", logs)
    monkeypatch.setattr(bedrock_module, "_build_boto3_session", lambda **_kwargs: realized_products.pop())

    operation = asyncio.create_task(provider.chat_text("system", "user"))
    try:
        assert await asyncio.to_thread(cleanup_started.wait, 1), "terminal cleanup did not start"
        _assert_runtime_capacity(
            lifecycle,
            in_flight=1,
            blocking=1,
            retained=1,
            service_owners=0,
        )
    finally:
        release_cleanup.set()

    with pytest.raises(RuntimeOwnershipError) as exc_info:
        await operation

    _assert_stable_terminal_error(exc_info.value, primary_type=_PrimaryOperationFailure)
    assert runtime_client.close_calls == 1
    assert credential_client.close_calls == 1
    assert session.close_calls == 1
    assert runtime_client.close_capacity == [1]
    assert credential_client.close_capacity == [1]
    assert session.close_capacity == [1]
    await _assert_terminal_cleanup_fenced_runtime(
        lifecycle,
        root=root,
        reason_code="bedrock_operation_cleanup_failed",
        error_type="RuntimeOwnershipError",
    )
    assert terminal_events == ["fatal_fence", "capacity_release"]
    assert provider._blocking_work.active == 0
    cleanup_records = [record for record in logs.records if record.event == "bedrock_operation_cleanup_failed"]
    assert cleanup_records
    assert cleanup_records[0].fields == {"error_type": "_TerminalCleanupFailure"}
    _assert_sanitized_logs(logs)

    runtime_client_ref = weakref.ref(runtime_client)
    credential_client_ref = weakref.ref(credential_client)
    session_ref = weakref.ref(session)
    del exc_info, operation
    del realized, session, credential_client, runtime_client
    assert await _collect_after_async_frames_settle(
        runtime_client_ref,
        credential_client_ref,
        session_ref,
    )


@pytest.mark.asyncio
async def test_rejected_dashboard_cleanup_failure_uses_shared_terminal_circuit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    root = lifecycle.execution_graph.register_root_owner()
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    terminal_events = _observe_fatal_fence_before_release(monkeypatch, lifecycle)

    class RejectedDashboard:
        pass

    products = [RejectedDashboard()]
    product_ref = weakref.ref(products[0])

    def factory() -> RejectedDashboard:
        return products[0]

    def reject(_product: RejectedDashboard) -> None:
        raise _PrimaryOperationFailure(_PRIMARY_SECRET)

    def fail_retirement(_product: RejectedDashboard) -> None:
        raise _TerminalCleanupFailure(_CLEANUP_SECRET)

    with pytest.raises(RuntimeOwnershipError) as exc_info:
        await blocking_work.realize_owned(
            factory,
            validate=reject,
            retire=fail_retirement,
            reason_code="backend:dashboard_realization",
        )

    _assert_stable_terminal_error(exc_info.value, primary_type=_PrimaryOperationFailure)
    await _assert_terminal_cleanup_fenced_runtime(
        lifecycle,
        root=root,
        reason_code="backend:dashboard_realization",
        error_type="RuntimeOwnershipError",
    )
    assert terminal_events == ["fatal_fence", "capacity_release"]
    assert blocking_work.active == 0

    del exc_info
    products.clear()
    assert await _collect_after_async_frames_settle(product_ref)


class _ManagedProvider(LLMProvider):
    def __init__(
        self,
        runtime_settings: Settings,
        *,
        close_started: threading.Event | None = None,
        release_close: threading.Event | None = None,
        fail_close: bool,
    ) -> None:
        super().__init__(runtime_settings, component="cleanup_failure_matrix_provider")
        self._close_started = close_started
        self._release_close = release_close
        self._fail_close = fail_close
        self.close_calls = 0

    async def chat_json(self, *_args: object, **_kwargs: object) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, *_args: object, **_kwargs: object) -> LLMResult:
        return LLMResult("ok")

    async def close(self) -> None:
        self.close_calls += 1
        if self._close_started is not None:
            self._close_started.set()
        if self._release_close is not None:
            assert self._release_close.wait(timeout=2), "test did not release provider cleanup"
        if self._fail_close:
            raise _TerminalCleanupFailure(_CLEANUP_SECRET)


def _provider_manager(
    tmp_path,
    *,
    factory: Any,
    suffix: str,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController, Settings]:
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    declared = declare_runtime_factory(
        lambda: factory(runtime_settings),
        ownership=runtime_descriptor_for_provider(
            component=f"{suffix}_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    manager = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=lifecycle,
        llm_factory=declared,
        cleanup_grace_seconds=0.5,
    )
    return manager, lifecycle, runtime_settings


@pytest.mark.asyncio
async def test_rejected_provider_validation_and_retirement_failure_are_distinct_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    close_started = threading.Event()
    release_close = threading.Event()
    products: list[_ManagedProvider] = []
    logs = _RecordingLogger()

    def rejected_factory(active_settings: Settings) -> _ManagedProvider:
        foreign = active_settings.model_copy(update={"llm_api_base": "http://127.0.0.1:11435"})
        product = _ManagedProvider(
            foreign,
            close_started=close_started if not products else None,
            release_close=release_close if not products else None,
            fail_close=True,
        )
        products.append(product)
        return product

    manager, lifecycle, _runtime_settings = _provider_manager(
        tmp_path,
        factory=rejected_factory,
        suffix="rejected-product",
    )
    monkeypatch.setattr(dependencies_module, "logger", logs)

    first_attempt = asyncio.create_task(manager.acquire())
    try:
        assert await asyncio.to_thread(close_started.wait, 1), "rejected product retirement did not start"
        _assert_runtime_capacity(
            lifecycle,
            in_flight=1,
            blocking=1,
            retained=1,
            service_owners=1,
        )
    finally:
        release_close.set()

    with pytest.raises(RuntimeOwnershipError) as first_error:
        await first_attempt
    _assert_stable_terminal_error(first_error.value, primary_type=RuntimeOwnershipMismatchError)

    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await manager.acquire()

    assert len(products) == 1
    assert all(product.close_calls == 1 for product in products)
    assert lifecycle.runtime_fatal_circuit is not None
    _assert_runtime_capacity(
        lifecycle,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )
    assert manager.quarantined_generation_count == 1
    with manager._lock:
        quarantine = tuple(manager._quarantine)
    assert len(quarantine) == 1
    assert {item.reason_code for item in quarantine} == {"provider_generation_cleanup_failed"}
    assert {item.error_type for item in quarantine} <= {
        "RuntimeOwnershipError",
        "_TerminalCleanupFailure",
    }
    assert _PRIMARY_SECRET not in repr(quarantine)
    assert _CLEANUP_SECRET not in repr(quarantine)
    assert any(record.event == "provider_rejected_cleanup_failed_closed" for record in logs.records)
    _assert_sanitized_logs(logs)


@dataclass(slots=True)
class _CleanupDependencies:
    manager: _RuntimeProviderResources
    handle: Any
    pipeline_admission: PipelineAdmissionController
    cleanup_grace_seconds: float = 0.5

    async def close_resources(self) -> None:
        await self.manager.close(self.handle)


@pytest.mark.asyncio
async def test_pipeline_failure_preserves_primary_error_when_generation_cleanup_also_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    close_started = threading.Event()
    release_close = threading.Event()
    products: list[_ManagedProvider] = []
    logs = _RecordingLogger()

    def provider_factory(runtime_settings: Settings) -> _ManagedProvider:
        product = _ManagedProvider(
            runtime_settings,
            close_started=close_started,
            release_close=release_close,
            fail_close=True,
        )
        products.append(product)
        return product

    manager, lifecycle, _runtime_settings = _provider_manager(
        tmp_path,
        factory=provider_factory,
        suffix="pipeline-dual-failure",
    )
    monkeypatch.setattr(dependencies_module, "logger", logs)
    monkeypatch.setattr(runner_module, "logger", logs)
    handle = await manager.acquire()
    deps = _CleanupDependencies(
        manager=manager,
        handle=handle,
        pipeline_admission=lifecycle,
    )

    async def fail_pipeline_then_cleanup() -> None:
        try:
            raise _PrimaryOperationFailure(_PRIMARY_SECRET)
        finally:
            await _cleanup_pipeline_resources(cast(Any, deps), [])

    pipeline = asyncio.create_task(fail_pipeline_then_cleanup())
    try:
        assert await asyncio.to_thread(close_started.wait, 1), "generation cleanup did not start"
        assert manager.lifecycle_state is ProviderLifecycleState.DRAINING
        _assert_runtime_capacity(
            lifecycle,
            in_flight=0,
            blocking=0,
            retained=0,
            service_owners=1,
        )
    finally:
        release_close.set()

    with pytest.raises(RuntimeOwnershipError) as exc_info:
        await pipeline

    _assert_stable_terminal_error(exc_info.value, primary_type=_PrimaryOperationFailure)
    assert products[0].close_calls == 2
    assert manager.quarantined_generation_count == 1
    _assert_runtime_capacity(
        lifecycle,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )
    assert any(record.event == "provider_child_cleanup_failed" for record in logs.records)
    assert any(record.event == "pipeline_resource_cleanup_failed" for record in logs.records)
    _assert_sanitized_logs(logs)


class _FailingStsClient(_CloseProbe):
    def __init__(
        self,
        lifecycle: PipelineAdmissionController,
        *,
        fail_request: bool,
        **kwargs: Any,
    ) -> None:
        super().__init__(lifecycle, **kwargs)
        self._fail_request = fail_request

    def assume_role(self, **_kwargs: object) -> dict[str, dict[str, str]]:
        if self._fail_request:
            raise _PrimaryOperationFailure(_PRIMARY_SECRET)
        return {
            "Credentials": {
                "AccessKeyId": "ASIAREALIZED",
                "SecretAccessKey": "realized-secret",
                "SessionToken": "realized-token",
            }
        }


class _StsSession:
    def __init__(self, client: _FailingStsClient) -> None:
        self._client = client

    def client(self, service_name: str, **_kwargs: object) -> _FailingStsClient:
        assert service_name == "sts"
        return self._client


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ("sts_request", "credential_plan_validation"))
async def test_pre_assignment_credential_cleanup_failure_fatal_fences_runtime(
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    clients: list[_FailingStsClient] = []
    errors: list[BaseException] = []
    logs = _RecordingLogger()

    def session_factory(**kwargs: object) -> object:
        if kwargs.get("aws_access_key_id") != "AKIABASE":
            return SimpleNamespace()
        client = _FailingStsClient(
            lifecycle,
            fail_request=failure_phase == "sts_request",
            fail=True,
            started=cleanup_started if not clients else None,
            release=release_cleanup if not clients else None,
        )
        clients.append(client)
        return _StsSession(client)

    def reject_realized_identity(*_args: object, **_kwargs: object) -> BedrockCredentialIdentity:
        raise _PrimaryOperationFailure(_PRIMARY_SECRET)

    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(Session=session_factory))
    monkeypatch.setattr(bedrock_module, "logger", logs)
    if failure_phase == "credential_plan_validation":
        monkeypatch.setattr(
            bedrock_module.BedrockCredentialPlan,
            "realized_identity",
            reject_realized_identity,
        )

    provider = BedrockProvider(_bedrock_settings())
    provider.bind_pipeline_lifecycle(lifecycle)
    first_attempt = asyncio.create_task(provider.chat_text("system", "user"))
    try:
        assert await asyncio.to_thread(cleanup_started.wait, 1), "credential cleanup did not start"
        _assert_runtime_capacity(
            lifecycle,
            in_flight=1,
            blocking=1,
            retained=1,
            service_owners=0,
        )
    finally:
        release_cleanup.set()
    try:
        await first_attempt
    except BaseException as exc:
        errors.append(exc)

    for _ in range(_REPEATED_CLEANUP_FAULTS - 1):
        try:
            await provider.chat_text("system", "user")
        except BaseException as exc:
            errors.append(exc)

    _assert_cleanup_fault_contract(
        errors=errors,
        resources=clients,
        lifecycle=lifecycle,
        logs=logs,
        primary_type=_PrimaryOperationFailure,
        reason_code="bedrock_credential_cleanup_failed",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("_iteration", range(10))
async def test_abandoned_result_cleanup_failure_fatal_fences_runtime_and_drains_final_root(
    monkeypatch: pytest.MonkeyPatch,
    _iteration: int,
) -> None:
    class ObservedAdmissionController(PipelineAdmissionController):
        def __init__(self) -> None:
            super().__init__(1, max_queued=0)
            self.product_ref: weakref.ReferenceType[_CloseProbe] | None = None
            self.release_observations: list[tuple[bool, bool]] = []

        def release_blocking_permit(self, permit: Any) -> None:
            fatal = self.runtime_fatal_circuit
            product_ref = self.product_ref
            self.release_observations.append(
                (
                    fatal is not None,
                    product_ref is not None and product_ref() is None,
                )
            )
            super().release_blocking_permit(permit)

    lifecycle = ObservedAdmissionController()
    root = lifecycle.execution_graph.register_root_owner()
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    close_calls = 0
    logs = _RecordingLogger()
    observed_capacity: list[tuple[int, int, int]] = []
    monkeypatch.setattr(side_effects_module, "logger", logs)

    class FailingFatalLogger:
        def __init__(self, delegate: Any) -> None:
            self._delegate = delegate

        def error(self, _event: str, **_fields: Any) -> None:
            raise RuntimeError(_CLEANUP_SECRET)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._delegate, name)

    original_admission_logger = pipeline_admission_module.logger
    monkeypatch.setattr(pipeline_admission_module, "logger", FailingFatalLogger(original_admission_logger))
    factory_started = threading.Event()
    release_factory = threading.Event()
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()

    class CleanupProbe(_CloseProbe):
        def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            super().close()

    def realize() -> _CloseProbe:
        factory_started.set()
        assert release_factory.wait(timeout=2), "test did not release product realization"
        resource = CleanupProbe(
            lifecycle,
            fail=True,
            started=cleanup_started,
            release=release_cleanup,
        )
        lifecycle.product_ref = weakref.ref(resource)
        return resource

    def observe_cleanup() -> None:
        if cleanup_started.wait(timeout=2):
            observed_capacity.append(
                (
                    lifecycle.in_flight,
                    lifecycle.blocking_in_flight,
                    lifecycle.retained,
                )
            )
        release_cleanup.set()

    observer = threading.Thread(target=observe_cleanup, daemon=True)
    observer.start()
    task = asyncio.create_task(
        blocking_work.realize_owned(
            realize,
            validate=lambda _resource: None,
            retire=lambda resource: resource.close(),
            reason_code="abandoned_result_cleanup_failure",
            result_handoff_seconds=0.01,
        )
    )
    assert await asyncio.to_thread(factory_started.wait, 1), "product realization did not start"
    release_factory.set()
    time.sleep(0.05)
    with pytest.raises(RuntimeError, match="transport expired before adoption") as first_error:
        await task
    observer.join(timeout=1)
    assert not observer.is_alive()

    deadline = time.monotonic() + 1
    while blocking_work.active and time.monotonic() < deadline:
        await asyncio.sleep(0.001)

    assert _PRIMARY_SECRET not in repr(first_error.value)
    assert _CLEANUP_SECRET not in repr(first_error.value)
    assert observed_capacity == [(1, 1, 1)]
    assert close_calls == 1
    assert lifecycle.product_ref is not None
    assert lifecycle.product_ref() is None
    assert lifecycle.release_observations == [(True, True)]
    assert blocking_work.active == 0
    _assert_runtime_capacity(
        lifecycle,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )

    fatal = lifecycle.runtime_fatal_circuit
    assert fatal is not None
    assert fatal.reason_code == "runtime_cleanup_failed"
    assert fatal.error_type == "RuntimeOwnershipError"
    assert len(fatal.reason_code) <= 128
    assert len(fatal.error_type) <= 128
    assert _PRIMARY_SECRET not in repr(fatal)
    assert _CLEANUP_SECRET not in repr(fatal)

    await asyncio.wait_for(lifecycle.execution_graph.release_root_owner(root), timeout=1)
    assert lifecycle.execution_graph.root_state == "closed"
    assert lifecycle.execution_graph.root_owner_count == 0
    assert lifecycle.execution_graph.provider_manager_count == 0
    assert lifecycle.runtime_root_state == "closed"
    _assert_runtime_capacity(
        lifecycle,
        in_flight=0,
        blocking=0,
        retained=0,
        service_owners=0,
    )

    retry_factory_calls = 0

    def unexpected_realize() -> _CloseProbe:
        nonlocal retry_factory_calls
        retry_factory_calls += 1
        return _CloseProbe(lifecycle, fail=True)

    for _ in range(_REPEATED_CLEANUP_FAULTS - 1):
        with pytest.raises(RuntimeOwnershipError) as retry_error:
            await blocking_work.realize_owned(
                unexpected_realize,
                validate=lambda _resource: None,
                retire=lambda resource: resource.close(),
                reason_code="abandoned_result_cleanup_failure",
                result_handoff_seconds=0.01,
            )
        assert getattr(retry_error.value, "cleanup_reason_code", None) == fatal.reason_code
        assert getattr(retry_error.value, "cleanup_error_type", None) == fatal.error_type
        assert getattr(retry_error.value, "runtime_provider_fatal", False) is True
        assert retry_factory_calls == 0
        _assert_runtime_capacity(
            lifecycle,
            in_flight=0,
            blocking=0,
            retained=0,
            service_owners=0,
        )

    with pytest.raises(RuntimeOwnershipError) as root_error:
        lifecycle.execution_graph.register_root_owner()
    assert getattr(root_error.value, "cleanup_reason_code", None) == fatal.reason_code
    assert lifecycle.execution_graph.root_owner_count == 0

    cleanup_records = [record for record in logs.records if record.event == "pipeline_blocking_work_cleanup_failed"]
    assert len(cleanup_records) == 1
    assert cleanup_records[0].fields == {
        "reason_code": "abandoned_result_cleanup_failure",
        "error_type": _TerminalCleanupFailure.__name__,
    }
    _assert_sanitized_logs(logs)


@pytest.mark.asyncio
async def test_worker_validation_cleanup_failure_fatal_fences_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    resources: list[_CloseProbe] = []
    errors: list[BaseException] = []
    logs = _RecordingLogger()
    monkeypatch.setattr(side_effects_module, "logger", logs)

    def reject(_resource: _CloseProbe) -> None:
        raise _PrimaryOperationFailure(_PRIMARY_SECRET)

    def realize() -> _CloseProbe:
        resource = _CloseProbe(
            lifecycle,
            fail=True,
            started=cleanup_started if not resources else None,
            release=release_cleanup if not resources else None,
        )
        resources.append(resource)
        return resource

    first_attempt = asyncio.create_task(
        blocking_work.realize_owned(
            realize,
            validate=reject,
            retire=lambda resource: resource.close(),
            reason_code="worker_validation_cleanup_failure",
        )
    )
    try:
        assert await asyncio.to_thread(cleanup_started.wait, 1), "validation cleanup did not start"
        _assert_runtime_capacity(
            lifecycle,
            in_flight=1,
            blocking=1,
            retained=1,
            service_owners=0,
        )
    finally:
        release_cleanup.set()
    try:
        await first_attempt
    except BaseException as exc:
        errors.append(exc)

    for _ in range(_REPEATED_CLEANUP_FAULTS - 1):
        try:
            await blocking_work.realize_owned(
                realize,
                validate=reject,
                retire=lambda resource: resource.close(),
                reason_code="worker_validation_cleanup_failure",
            )
        except BaseException as exc:
            errors.append(exc)

    settle_deadline = time.monotonic() + 1
    while blocking_work.active and time.monotonic() < settle_deadline:
        await asyncio.sleep(0.001)
    assert blocking_work.active == 0
    _assert_cleanup_fault_contract(
        errors=errors,
        resources=resources,
        lifecycle=lifecycle,
        logs=logs,
        primary_type=_PrimaryOperationFailure,
        reason_code="worker_validation_cleanup_failure",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rejection_kind",
    ("invalid_wrapper", "invalid_identity", "ownership_mismatch"),
)
async def test_bedrock_generation_rejection_cleanup_failure_fatal_fences_runtime(
    monkeypatch: pytest.MonkeyPatch,
    rejection_kind: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    provider = BedrockProvider(_bedrock_settings())
    provider.bind_pipeline_lifecycle(lifecycle)
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    resources: list[_CloseProbe] = []
    errors: list[BaseException] = []
    logs = _RecordingLogger()
    ordering: list[str] = []
    ordering_lock = threading.Lock()
    fatal_fence_entered = threading.Event()
    allow_fatal_fence = threading.Event()

    def record(event: str) -> None:
        with ordering_lock:
            ordering.append(event)

    class _ObservedFinishedEvent(threading.Event):
        def set(self) -> None:
            super().set()
            record("worker_finished")

    original_register = LifecycleOwnedBlockingWork._register
    original_deliver = side_effects_module._LifecycleBlockingCall._deliver
    original_fence = lifecycle.fence_runtime_fatal
    original_release = lifecycle.release_blocking_permit

    def observe_register(work: LifecycleOwnedBlockingWork, call: Any) -> None:
        call.finished = _ObservedFinishedEvent()
        original_register(work, call)

    def observe_deliver(call: Any) -> None:
        original_deliver(call)
        if call._future is not None and call._future.done():
            record("terminal_result")

    def observe_fatal_fence(error: BaseException) -> RuntimeFatalCircuit:
        record("fatal_fence_entered")
        fatal_fence_entered.set()
        assert allow_fatal_fence.wait(1), "fatal fence ordering gate was not released"
        fatal = original_fence(error)
        record("fatal_fence")
        return fatal

    def observe_release(permit: Any) -> None:
        original_release(permit)
        if not permit.cleanup:
            record("permit_release")

    monkeypatch.setattr(LifecycleOwnedBlockingWork, "_register", observe_register)
    monkeypatch.setattr(side_effects_module._LifecycleBlockingCall, "_deliver", observe_deliver)
    monkeypatch.setattr(lifecycle, "fence_runtime_fatal", observe_fatal_fence)
    monkeypatch.setattr(lifecycle, "release_blocking_permit", observe_release)

    def rejected_runtime(**_kwargs: object) -> object:
        resource = _CloseProbe(
            lifecycle,
            fail=True,
            started=cleanup_started if not resources else None,
            release=release_cleanup if not resources else None,
        )
        resources.append(resource)
        if rejection_kind == "invalid_wrapper":
            return resource
        if rejection_kind == "invalid_identity":
            return _ResolvedBedrockRuntime(
                session=cast(Any, resource),
                credential_identity=cast(Any, object()),
            )
        return _ResolvedBedrockRuntime(
            session=cast(Any, resource),
            credential_identity=BedrockCredentialIdentity(
                account="arn:aws:iam::999999999999:role/foreign",
                credential_fingerprint=credential_fingerprint("foreign-generation"),
                uses_sts=True,
            ),
        )

    monkeypatch.setattr(bedrock_module, "logger", logs)
    monkeypatch.setattr(bedrock_module, "_build_boto3_session", rejected_runtime)

    first_attempt = asyncio.create_task(provider.chat_text("system", "user"))
    try:
        assert await asyncio.to_thread(cleanup_started.wait, 1), "rejected runtime cleanup did not start"
        _assert_runtime_capacity(
            lifecycle,
            in_flight=1,
            blocking=1,
            retained=1,
            service_owners=0,
        )
    finally:
        release_cleanup.set()

    assert await asyncio.to_thread(fatal_fence_entered.wait, 1), "fatal fencing did not start"
    record("pre_terminal_assertion")
    premature_terminal_result = "terminal_result" in ordering or first_attempt.done()
    premature_ordering = tuple(ordering)
    allow_fatal_fence.set()
    try:
        await first_attempt
    except BaseException as exc:
        errors.append(exc)
        record("caller_observed")

    assert not premature_terminal_result, premature_ordering
    assert ordering.index("fatal_fence_entered") < ordering.index("pre_terminal_assertion")
    assert ordering.index("pre_terminal_assertion") < ordering.index("fatal_fence")
    assert ordering.index("fatal_fence") < ordering.index("permit_release")
    assert ordering.index("permit_release") < ordering.index("worker_finished")
    assert ordering.index("worker_finished") < ordering.index("terminal_result")
    assert ordering.index("terminal_result") < ordering.index("caller_observed")
    assert ordering.count("permit_release") == 1

    for _ in range(_REPEATED_CLEANUP_FAULTS - 1):
        try:
            await provider.chat_text("system", "user")
        except BaseException as exc:
            errors.append(exc)

    _assert_cleanup_fault_contract(
        errors=errors,
        resources=resources,
        lifecycle=lifecycle,
        logs=logs,
        primary_type=RuntimeOwnershipError,
        reason_code="bedrock_rejected_runtime_cleanup_failed",
    )
