"""Terminal-authority matrix for runtime-managed provider generations.

These tests define the boundary between an executable provider and the runtime
authority that owns it.  A provider generation may release capacity only after
cleanup has succeeded or executable authority has been irrevocably fenced and
made unreachable.  A permanent cleanup failure is runtime-fatal; it is not a
signal to create another generation in the same runtime.
"""

from __future__ import annotations

import asyncio
import gc
import re
import threading
import weakref
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

import pytest

import tacit.dependencies as dependencies_module
import tacit.pipeline_admission as pipeline_admission_module
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.config import Settings
from tacit.context.base import ContextProvider
from tacit.dependencies import (
    ProviderLeaseHandle,
    ProviderLifecycleState,
    _RuntimeProviderResources,
)
from tacit.errors import RuntimeOwnershipError
from tacit.models.schemas import ContextChunk, Intent
from tacit.pipeline_admission import PipelineAdmissionController, RuntimeFatalCircuit
from tacit.runtime_ownership import (
    declare_runtime_factory,
    runtime_descriptor_for_provider,
    runtime_descriptor_from_settings,
)


@pytest.fixture(autouse=True)
def _isolated_process_fatal_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model a fresh process without adding a production reset capability."""
    monkeypatch.setattr(
        pipeline_admission_module,
        "_PROCESS_RUNTIME_FATAL_REGISTRY",
        pipeline_admission_module._ProcessRuntimeFatalRegistry(limit=1024),
    )


@dataclass
class _ClosePlan:
    """External test state that never owns the provider product itself."""

    failures_before_success: int = 0
    always_fail: bool = False
    wait_for_release: bool = False
    attempts: int = 0
    closed: bool = False
    started: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    settled: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)

    async def close(self) -> None:
        with self.lock:
            self.attempts += 1
            attempt = self.attempts
        self.started.set()
        if self.wait_for_release:
            await asyncio.to_thread(self.release.wait)
        try:
            if self.always_fail or attempt <= self.failures_before_success:
                raise RuntimeError("synthetic provider close failure")
            self.closed = True
        finally:
            self.settled.set()


class _ManagedLLMProbe(LLMProvider):
    def __init__(
        self,
        runtime_settings: Settings,
        close_plan: _ClosePlan,
        *,
        operation_started: threading.Event | None = None,
        operation_release: threading.Event | None = None,
    ) -> None:
        super().__init__(runtime_settings, component="terminal_authority_llm")
        self._close_plan = close_plan
        self._operation_started = operation_started
        self._operation_release = operation_release

    def _ensure_usable(self) -> None:
        if self._close_plan.closed:
            raise RuntimeError("provider was closed outside runtime authority")

    async def chat_json(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self._ensure_usable()
        return LLMResult("{}")

    async def chat_text(self, *_args: Any, **_kwargs: Any) -> LLMResult:
        self._ensure_usable()
        if self._operation_started is not None:
            self._operation_started.set()
        if self._operation_release is not None:
            await asyncio.to_thread(self._operation_release.wait)
        return LLMResult("ok")

    async def close(self) -> None:
        await self._close_plan.close()


class _ManagedContextProbe(ContextProvider):
    def __init__(self, runtime_settings: Settings, close_plan: _ClosePlan) -> None:
        super().__init__(runtime_settings, component="terminal_authority_context")
        self._close_plan = close_plan

    @property
    def name(self) -> str:
        return "terminal-authority-context"

    async def query(self, *_args: Any, **_kwargs: Any) -> list[ContextChunk]:
        if self._close_plan.closed:
            raise RuntimeError("context provider was closed outside runtime authority")
        return []

    async def close(self) -> None:
        await self._close_plan.close()


def _settings(tmp_path: Path, *, suffix: str, with_context: bool = False) -> Settings:
    values: dict[str, Any] = {
        "_env_file": None,
        "history_db_path": str(tmp_path / f"{suffix}-history.db"),
        "feedback_db_path": str(tmp_path / f"{suffix}-feedback.db"),
        "signals_db_path": str(tmp_path / f"{suffix}-signals.db"),
        "llm_provider": "ollama",
        "llm_api_base": "http://127.0.0.1:11434",
        "context_provider": "mcp" if with_context else "none",
        "pipeline_max_concurrent": 2,
        "pipeline_max_queued": 0,
    }
    if with_context:
        values["context_mcp_server_url"] = "http://127.0.0.1:8765"
    return Settings(**values)


def _managed_resources(
    tmp_path: Path,
    *,
    suffix: str,
    llm_factory: Callable[[Settings], LLMProvider],
    context_factory: Callable[[Settings], ContextProvider] | None = None,
    cleanup_grace_seconds: float = 0.2,
    canonical_runtime: bool = False,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController, Settings]:
    runtime_settings = _settings(
        tmp_path,
        suffix=suffix,
        with_context=context_factory is not None,
    )
    runtime_identity = None
    if canonical_runtime:
        runtime_owner = runtime_descriptor_from_settings(
            runtime_settings,
            component="terminal_authority_runtime",
        )
        runtime_identity = runtime_owner.admission_namespace or runtime_owner.settings_identity
    controller = PipelineAdmissionController(
        2,
        max_queued=0,
        runtime_identity=runtime_identity,
    )
    declared_llm = declare_runtime_factory(
        lambda: llm_factory(runtime_settings),
        ownership=runtime_descriptor_for_provider(
            component=f"terminal_authority_{suffix}_llm_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    declared_context = None
    if context_factory is not None:
        declared_context = declare_runtime_factory(
            lambda: context_factory(runtime_settings),
            ownership=runtime_descriptor_for_provider(
                component=f"terminal_authority_{suffix}_context_factory",
                runtime_settings=runtime_settings,
                capability="context",
            ),
            factory_kind="provider:context",
        )
    resources = _RuntimeProviderResources.resolve(
        runtime_settings,
        lifecycle=controller,
        llm_factory=declared_llm,
        context_factory=declared_context,
        cleanup_grace_seconds=cleanup_grace_seconds,
    )
    return resources, controller, runtime_settings


def _counts(controller: PipelineAdmissionController) -> dict[str, int]:
    return {
        "active": controller.in_flight,
        "queued": controller.queued,
        "retained": controller.retained,
        "blocking": controller.blocking_in_flight,
        "service_owner": controller.service_owner_in_flight,
    }


def _assert_zero(controller: PipelineAdmissionController) -> None:
    assert _counts(controller) == {
        "active": 0,
        "queued": 0,
        "retained": 0,
        "blocking": 0,
        "service_owner": 0,
    }


def _collect_until_dead(reference: weakref.ReferenceType[Any]) -> bool:
    gc.collect()
    return reference() is None


async def _collect_after_async_frames_settle(*references: weakref.ReferenceType[Any]) -> bool:
    for _attempt in range(10):
        await asyncio.sleep(0)
        gc.collect()
        if all(reference() is None for reference in references):
            return True
    return False


async def _trip_permanent_cleanup_failure(
    tmp_path: Path,
    *,
    suffix: str,
) -> tuple[
    str,
    weakref.ReferenceType[_RuntimeProviderResources],
    weakref.ReferenceType[PipelineAdmissionController],
    weakref.ReferenceType[_ManagedLLMProbe],
    weakref.ReferenceType[Settings],
]:
    close_plan = _ClosePlan(always_fail=True)

    def provider_factory(settings: Settings) -> LLMProvider:
        return _ManagedLLMProbe(settings, close_plan)

    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix=suffix,
        llm_factory=provider_factory,
        canonical_runtime=True,
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    handle = await resources.acquire()
    provider = cast(_ManagedLLMProbe, resources.llm())
    resources_ref = weakref.ref(resources)
    controller_ref = weakref.ref(controller)
    provider_ref = weakref.ref(provider)
    settings_ref = weakref.ref(_runtime_settings)
    runtime_identity = controller.runtime_identity

    with pytest.raises(RuntimeOwnershipError):
        await resources.close(handle)
    del handle, provider
    with pytest.raises(RuntimeOwnershipError):
        await graph.release_root_owner(root)

    assert graph.root_state in {"failed", "closed"}
    assert graph.root_owner_count == 0
    assert graph.provider_manager() is None
    assert controller.runtime_root_state in {"failed", "closed"}
    _assert_zero(controller)
    return runtime_identity, resources_ref, controller_ref, provider_ref, settings_ref


def _intent() -> Intent:
    return Intent(
        summary="checkout latency",
        domain="application",
        services=["checkout"],
        keywords=["latency"],
    )


@pytest.mark.asyncio
async def test_control_manager_release_keeps_sibling_live_and_closes_generation_once(tmp_path: Path) -> None:
    plan = _ClosePlan()
    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="clean-control",
        llm_factory=lambda settings: _ManagedLLMProbe(settings, plan),
    )

    first = await resources.acquire()
    second = await resources.acquire()
    provider = resources.llm()

    await resources.close(first)
    result = await provider.chat_text("system", "user")
    attempts_with_sibling = plan.attempts
    await resources.close(second)

    assert result.text == "ok"
    assert attempts_with_sibling == 0
    assert plan.attempts == 1
    assert plan.closed is True
    assert resources.lifecycle_state is ProviderLifecycleState.EMPTY
    _assert_zero(controller)


@pytest.mark.asyncio
async def test_startup_retirement_and_replacement_acquire_have_one_atomic_winner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    llm_plans: list[_ClosePlan] = []
    context_calls = 0
    context_lock = threading.Lock()

    def llm_factory(settings: Settings) -> LLMProvider:
        plan = _ClosePlan()
        llm_plans.append(plan)
        return _ManagedLLMProbe(settings, plan)

    def context_factory(settings: Settings) -> ContextProvider:
        nonlocal context_calls
        with context_lock:
            context_calls += 1
            call = context_calls
        if call == 1:
            raise RuntimeError("synthetic context startup failure")
        return _ManagedContextProbe(settings, _ClosePlan())

    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="startup-retirement-race",
        llm_factory=llm_factory,
        context_factory=context_factory,
    )
    retirement_checked = threading.Event()
    permit_retirement = threading.Event()
    original_begin_retirement = resources._begin_generation_retirement

    def gated_begin_retirement(*args: Any, **kwargs: Any) -> None:
        retirement_checked.set()
        assert permit_retirement.wait(timeout=2.0), "test did not release startup retirement"
        original_begin_retirement(*args, **kwargs)

    monkeypatch.setattr(resources, "_begin_generation_retirement", gated_begin_retirement)
    first_errors: list[BaseException] = []

    def fail_first_startup() -> None:
        try:
            asyncio.run(resources.acquire())
        except BaseException as exc:
            first_errors.append(exc)

    first_thread = threading.Thread(target=fail_first_startup, name="provider-startup-retirement")
    first_thread.start()
    assert retirement_checked.wait(timeout=1.0)

    replacement_entered = threading.Event()
    replacement_handles: list[ProviderLeaseHandle] = []
    replacement_errors: list[BaseException] = []

    def acquire_replacement() -> None:
        async def acquire() -> ProviderLeaseHandle:
            replacement_entered.set()
            return await resources.acquire()

        try:
            replacement_handles.append(asyncio.run(acquire()))
        except BaseException as exc:
            replacement_errors.append(exc)

    replacement_thread = threading.Thread(
        target=acquire_replacement,
        name="provider-startup-replacement",
    )
    replacement_thread.start()
    assert replacement_entered.wait(timeout=1.0)
    await asyncio.sleep(0.05)
    replacement_entered_before_retirement = context_calls > 1 or len(llm_plans) > 1

    replacement: ProviderLeaseHandle | None = None
    provider_result: LLMResult | None = None
    provider_error: BaseException | None = None
    try:
        permit_retirement.set()
        await asyncio.to_thread(first_thread.join, 2.0)
        await asyncio.to_thread(replacement_thread.join, 2.0)
        replacement = await resources.acquire()
        exposed_provider = resources.llm()
        try:
            provider_result = await exposed_provider.chat_text("system", "replacement")
        except BaseException as exc:
            provider_error = exc
    finally:
        permit_retirement.set()
        await asyncio.to_thread(first_thread.join, 2.0)
        await asyncio.to_thread(replacement_thread.join, 2.0)
        if replacement is not None:
            try:
                await resources.close(replacement)
            except BaseException:
                pass
        else:
            try:
                await resources.shutdown()
            except BaseException:
                pass

    assert first_thread.is_alive() is False
    assert replacement_thread.is_alive() is False
    assert replacement_entered_before_retirement is False
    assert len(first_errors) == 1
    assert isinstance(first_errors[0], RuntimeError)
    assert replacement_handles == []
    assert len(replacement_errors) == 1
    assert isinstance(replacement_errors[0], RuntimeOwnershipError)
    assert replacement is not None
    assert replacement.generation_epoch > 1
    assert provider_error is None
    assert provider_result is not None and provider_result.text == "ok"
    assert llm_plans[0].attempts == 1
    assert llm_plans[0].closed is True
    _assert_zero(controller)


@pytest.mark.filterwarnings("error::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.asyncio
async def test_owner_loop_loss_keeps_authority_charged_until_provider_cleanup_finishes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    operation_started = threading.Event()
    operation_release = threading.Event()
    close_plan = _ClosePlan(wait_for_release=True)
    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="owner-loop-loss",
        llm_factory=lambda settings: _ManagedLLMProbe(
            settings,
            close_plan,
            operation_started=operation_started,
            operation_release=operation_release,
        ),
        cleanup_grace_seconds=0.5,
    )
    handle = await resources.acquire()
    provider = resources.llm()
    provider_ref = weakref.ref(provider)
    operation = asyncio.create_task(provider.chat_text("system", "blocked"))
    del provider
    assert await asyncio.to_thread(operation_started.wait, 1.0)

    retirement_started = threading.Event()
    original_begin_retirement = resources._begin_generation_retirement

    def observe_retirement(*args: Any, **kwargs: Any) -> None:
        original_begin_retirement(*args, **kwargs)
        retirement_started.set()

    monkeypatch.setattr(resources, "_begin_generation_retirement", observe_retirement)
    close_task = asyncio.create_task(resources.close(handle))
    assert await asyncio.to_thread(retirement_started.wait, 1.0)
    assert resources.lifecycle_state is ProviderLifecycleState.DRAINING
    state = resources._generation_owner
    assert state is not None and state.loop is not None
    owner_loop = state.loop
    owner_thread = state.owner_thread
    owner_loop.call_soon_threadsafe(owner_loop.stop)

    cleanup_started = await asyncio.to_thread(close_plan.started.wait, 1.0)
    counts_before_cleanup_finished = _counts(controller)
    cleanup_finished_early = close_plan.settled.is_set()

    operation_release.set()
    close_plan.release.set()
    await asyncio.gather(operation, close_task, return_exceptions=True)
    await asyncio.to_thread(close_plan.settled.wait, 1.0)
    del operation, close_task

    assert owner_thread is not None and owner_thread.is_alive() is False
    assert not tuple(task for task in asyncio.all_tasks(owner_loop) if not task.done())
    assert _collect_until_dead(provider_ref)

    assert cleanup_started is True
    assert cleanup_finished_early is False
    assert sum(counts_before_cleanup_finished.values()) > 0
    assert close_plan.closed is True
    _assert_zero(controller)


@pytest.mark.parametrize("capability", ["llm", "context"])
@pytest.mark.asyncio
async def test_managed_proxies_deny_external_close_while_sibling_lease_exists(
    capability: Literal["llm", "context"],
    tmp_path: Path,
) -> None:
    llm_plan = _ClosePlan()
    context_plan = _ClosePlan()
    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix=f"external-close-{capability}",
        llm_factory=lambda settings: _ManagedLLMProbe(settings, llm_plan),
        context_factory=(
            (lambda settings: _ManagedContextProbe(settings, context_plan)) if capability == "context" else None
        ),
    )
    first = await resources.acquire()
    second = await resources.acquire()
    target = resources.llm() if capability == "llm" else cast(ContextProvider, resources.context())
    plan = llm_plan if capability == "llm" else context_plan

    external_error: BaseException | None = None
    sibling_usable = False
    try:
        try:
            await target.close()
        except BaseException as exc:
            external_error = exc
        if capability == "llm":
            sibling_usable = (await cast(LLMProvider, target).chat_text("system", "sibling")).text == "ok"
        else:
            sibling_usable = await cast(ContextProvider, target).query(_intent()) == []
        attempts_before_manager_release = plan.attempts
    except BaseException:
        attempts_before_manager_release = plan.attempts
    finally:
        await resources.close(first)
        try:
            await resources.close(second)
        except BaseException:
            pass

    violations: list[str] = []
    if not isinstance(external_error, RuntimeOwnershipError):
        violations.append("external close did not return the runtime ownership denial")
    if attempts_before_manager_release != 0:
        violations.append("external close reached the shared provider implementation")
    if not sibling_usable:
        violations.append("external close made the sibling lease unusable")
    if plan.attempts != 1:
        violations.append("manager-owned final release did not close exactly once")
    if any(_counts(controller).values()):
        violations.append("external-close test did not return runtime capacity to zero")
    assert not violations, "managed proxy close violations:\n- " + "\n- ".join(violations)


@pytest.mark.asyncio
async def test_transient_terminal_cleanup_failure_retries_then_allows_a_clean_generation(tmp_path: Path) -> None:
    plans: list[_ClosePlan] = []
    references: list[weakref.ReferenceType[_ManagedLLMProbe]] = []

    def provider_factory(settings: Settings) -> LLMProvider:
        plan = _ClosePlan(failures_before_success=1 if not plans else 0)
        provider = _ManagedLLMProbe(settings, plan)
        plans.append(plan)
        references.append(weakref.ref(provider))
        return provider

    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="transient-cleanup-retry",
        llm_factory=provider_factory,
    )
    first = await resources.acquire()
    first_error: tuple[type[BaseException], str] | None = None
    try:
        await resources.close(first)
    except BaseException as exc:
        first_error = (type(exc), str(exc))
    first_collected = _collect_until_dead(references[0])

    second: ProviderLeaseHandle | None = None
    if first_error is None:
        second = await resources.acquire()
        assert (await resources.llm().chat_text("system", "new generation")).text == "ok"
        await resources.close(second)

    violations: list[str] = []
    if first_error is not None:
        violations.append("one transient close failure tripped terminal generation failure")
    if plans[0].attempts != 2:
        violations.append("transient cleanup was not retried exactly once")
    if not plans[0].closed:
        violations.append("transient cleanup retry never retired the first provider")
    if not first_collected:
        violations.append("the retired first provider remained strongly reachable")
    if len(plans) != 2:
        violations.append("a clean provider generation could not start after transient recovery")
    elif plans[1].attempts != 1:
        violations.append("the clean replacement generation did not close exactly once")
    if any(_counts(controller).values()):
        violations.append("transient cleanup recovery did not return capacity to zero")
    assert not violations, "transient cleanup violations:\n- " + "\n- ".join(violations)


@pytest.mark.asyncio
async def test_permanent_cleanup_failure_trips_runtime_fatal_circuit_and_final_root_terminates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    close_plan = _ClosePlan(always_fail=True, wait_for_release=True)
    references: list[weakref.ReferenceType[_ManagedLLMProbe]] = []
    factory_calls = 0

    def provider_factory(settings: Settings) -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        provider = _ManagedLLMProbe(settings, close_plan)
        references.append(weakref.ref(provider))
        return provider

    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="permanent-cleanup-circuit",
        llm_factory=provider_factory,
        cleanup_grace_seconds=0.5,
    )
    graph = controller.execution_graph
    root = graph.register_root_owner()
    handle = await resources.acquire()
    release_observations: list[tuple[RuntimeFatalCircuit | None, bool, bool]] = []
    original_release_service_owner = controller.release_service_owner

    def observe_service_owner_release(permit: Any) -> None:
        with resources._lock:
            state = resources._generation_owner or resources._retired_generation
            release_observations.append(
                (
                    controller.runtime_fatal_circuit,
                    resources._llm_provider is None and resources._context_provider is None,
                    state is None or not state.retained_products,
                )
            )
        original_release_service_owner(permit)

    monkeypatch.setattr(controller, "release_service_owner", observe_service_owner_release)

    close_task = asyncio.create_task(resources.close(handle))
    assert await asyncio.to_thread(close_plan.started.wait, 1.0)
    counts_while_executable = _counts(controller)
    authority_reachable_while_closing = references[0]() is not None
    close_plan.release.set()

    close_error: tuple[type[BaseException], str] | None = None
    try:
        await close_task
    except BaseException as exc:
        close_error = (type(exc), str(exc))
    del close_task
    first_authority_collected = _collect_until_dead(references[0])
    counts_after_revocation = _counts(controller)

    circuit_error: tuple[type[BaseException], str] | None = None
    unexpected_handle: ProviderLeaseHandle | None = None
    try:
        unexpected_handle = await resources.acquire()
    except BaseException as exc:
        circuit_error = (type(exc), str(exc))
    if unexpected_handle is not None:
        try:
            await resources.close(unexpected_handle)
        except BaseException:
            pass

    root_error: tuple[type[BaseException], str] | None = None
    try:
        await asyncio.wait_for(graph.release_root_owner(root), timeout=2.0)
    except BaseException as exc:
        root_error = (type(exc), str(exc))

    violations: list[str] = []
    if not authority_reachable_while_closing:
        violations.append("the close probe lost its target before cleanup ran")
    if sum(counts_while_executable.values()) == 0:
        violations.append("capacity reached zero while executable cleanup authority remained")
    if close_error is None or not issubclass(close_error[0], RuntimeOwnershipError):
        violations.append("permanent cleanup failure was not reported as terminal runtime failure")
    if not close_plan.settled.is_set():
        violations.append("terminal state was published before the close attempt settled")
    if not first_authority_collected:
        violations.append("runtime retained a strong reference after revoking executable authority")
    if any(counts_after_revocation.values()):
        violations.append("capacity did not return to zero after authority revocation")
    if release_observations != [(controller.runtime_fatal_circuit, True, True)]:
        violations.append("service-owner capacity was released before fatal fencing and authority revocation")
    if circuit_error is None or not issubclass(circuit_error[0], RuntimeOwnershipError):
        violations.append("runtime-fatal provider circuit did not reject the next acquire")
    if unexpected_handle is not None:
        violations.append("a replacement generation was admitted after runtime-fatal cleanup")
    if factory_calls != 1:
        violations.append("runtime-fatal cleanup executed another provider factory")
    if graph.root_state not in {"failed", "closed"}:
        violations.append("final root did not reach a terminal failed/closed state")
    if root_error is None and graph.root_state != "failed":
        violations.append("final root discarded the runtime-fatal provider result")
    if graph.root_owner_count != 0:
        violations.append("final root retained an owner token")
    if graph.provider_manager() is not None:
        violations.append("final root retained the failed provider manager")
    if controller.runtime_root_state not in {"failed", "closed"}:
        violations.append("admission controller remained nonterminal")
    if any(_counts(controller).values()):
        violations.append("final root did not terminate with zero authority counters")
    assert not violations, "permanent cleanup circuit violations:\n- " + "\n- ".join(violations)


@pytest.mark.asyncio
async def test_generic_runtime_fatal_fence_rejects_operations_on_an_existing_provider_proxy(
    tmp_path: Path,
) -> None:
    close_plan = _ClosePlan()
    resources, controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="generic-fatal-existing-proxy",
        llm_factory=lambda settings: _ManagedLLMProbe(settings, close_plan),
    )
    handle = await resources.acquire()
    provider = resources.llm()
    async with controller.slot():
        failure = RuntimeError("generic cleanup failed")
        setattr(failure, "cleanup_reason_code", "generic_runtime_cleanup_failed")
        setattr(failure, "cleanup_error_type", "RuntimeError")
        controller.fence_runtime_fatal(failure)

        with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
            await provider.chat_text("system", "must not start")

    await resources.close(handle)
    assert close_plan.closed is True
    _assert_zero(controller)


@pytest.mark.asyncio
async def test_runtime_fatal_circuit_survives_controller_collection_and_isolates_other_runtimes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runtime_identity, resources_ref, controller_ref, provider_ref, settings_ref = await _trip_permanent_cleanup_failure(
        tmp_path,
        suffix="process-lifetime-fatal",
    )

    assert await _collect_after_async_frames_settle(
        provider_ref,
        resources_ref,
        controller_ref,
        settings_ref,
    )

    assert not any(
        hasattr(_RuntimeProviderResources, name)
        for name in (
            "_FATAL_CIRCUITS",
            "_FATAL_CIRCUIT_OVERFLOW",
            "_FATAL_CIRCUIT_LIMIT",
            "_FATAL_CIRCUIT_KEY_DOMAIN",
        )
    )
    registry = pipeline_admission_module._PROCESS_RUNTIME_FATAL_REGISTRY._records
    assert len(registry) == 1
    key = next(iter(registry))
    assert isinstance(key, str)
    assert re.fullmatch(r"[0-9a-f]{64}", key)
    assert key != runtime_identity
    assert runtime_identity not in key
    record = next(iter(registry.values()))
    assert isinstance(record, RuntimeFatalCircuit)

    replacement_factory_calls = 0
    semantic_spec_reads = 0

    def replacement_factory(settings: Settings) -> LLMProvider:
        nonlocal replacement_factory_calls
        replacement_factory_calls += 1
        return _ManagedLLMProbe(settings, _ClosePlan())

    def forbidden_semantic_spec(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal semantic_spec_reads
        semantic_spec_reads += 1
        raise AssertionError("fatal runtime read provider semantics")

    with monkeypatch.context() as fatal_context:
        fatal_context.setattr(dependencies_module, "_semantic_provider_spec", forbidden_semantic_spec)
        with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
            _managed_resources(
                tmp_path,
                suffix="process-lifetime-fatal",
                llm_factory=replacement_factory,
                canonical_runtime=True,
            )
    assert semantic_spec_reads == 0
    assert replacement_factory_calls == 0

    isolated_plan = _ClosePlan()
    isolated, isolated_controller, _runtime_settings = _managed_resources(
        tmp_path,
        suffix="process-lifetime-isolated",
        llm_factory=lambda settings: _ManagedLLMProbe(settings, isolated_plan),
    )
    isolated_handle = await isolated.acquire()
    assert (await isolated.llm().chat_text("system", "isolated runtime")).text == "ok"
    await isolated.close(isolated_handle)
    assert isolated_plan.closed is True
    _assert_zero(isolated_controller)
