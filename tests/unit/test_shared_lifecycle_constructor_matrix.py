"""Constructor-phase fault matrix for shared runtime lifecycle owners.

The cross-runtime lifecycle gate assigns provider generations, final root drain,
drain recovery, and selected-waiter maintenance to runtime-owned threads. Thread
construction is part of each ownership transition: it must either install one
bounded owner or roll back/fence all authority before control returns.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from uuid import uuid4

import pytest

from tacit.config import Settings
from tacit.dependencies import ProviderLifecycleState, _RuntimeProviderResources
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import (
    PipelineAdmissionController,
    RuntimeRootDrainStartupError,
    release_runtime_root_with_startup_retry,
    runtime_admission_controller,
)
from tacit.runtime_ownership import runtime_descriptor_from_settings


def _settings(tmp_path: Any, *, suffix: str, limit: int = 1, queued: int = 0) -> Settings:
    return Settings(  # type: ignore[call-arg]
        _env_file=None,
        history_db_path=str(tmp_path / f"{suffix}-history.db"),
        feedback_db_path=str(tmp_path / f"{suffix}-feedback.db"),
        signals_db_path=str(tmp_path / f"{suffix}-signals.db"),
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
        pipeline_max_concurrent=limit,
        pipeline_max_queued=queued,
    )


def test_isolated_settings_binding_preserves_unique_admission_authorities(tmp_path: Any) -> None:
    settings = _settings(tmp_path, suffix="isolated-settings-binding")
    first = PipelineAdmissionController(1, max_queued=0)
    second = PipelineAdmissionController(1, max_queued=0)
    expected = runtime_descriptor_from_settings(
        settings,
        component="isolated-settings-binding",
    ).admission_namespace
    assert expected is not None

    first.bind_runtime_settings_identity(expected)
    second.bind_runtime_settings_identity(expected)

    assert first.runtime_identity != second.runtime_identity
    assert first.runtime_identity != expected
    assert second.runtime_identity != expected
    canonical = runtime_admission_controller(settings, runtime_identity=expected)
    assert canonical.runtime_identity == expected


@pytest.mark.asyncio
async def test_provider_thread_constructor_failure_rolls_back_starting_and_service_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    settings = _settings(tmp_path, suffix="provider-constructor")
    controller = PipelineAdmissionController(1, max_queued=0)
    resources = _RuntimeProviderResources(settings, lifecycle=controller)
    real_thread = threading.Thread

    def reject_provider_owner(*args: Any, **kwargs: Any) -> threading.Thread:
        if kwargs.get("name") == "tacit-lifecycle-provider-owner":
            raise RuntimeError("provider owner construction failed")
        return real_thread(*args, **kwargs)

    monkeypatch.setattr(threading, "Thread", reject_provider_owner)

    with resources._lock, pytest.raises(RuntimeError, match="construction failed"):
        resources._start_generation_owner_locked()

    assert resources.lifecycle_state is ProviderLifecycleState.EMPTY
    assert controller.service_owner_in_flight == 0
    assert controller.in_flight == 0
    assert controller.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_final_drain_thread_constructor_failure_uses_bounded_startup_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = f"matrix:drain-constructor:{uuid4().hex}"
    controller = PipelineAdmissionController(1, max_queued=0, runtime_identity=identity)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    real_thread = threading.Thread
    construction_attempts = 0
    observed_errors: list[BaseException] = []

    def reject_first_drain_owner(*args: Any, **kwargs: Any) -> threading.Thread:
        nonlocal construction_attempts
        if str(kwargs.get("name", "")).startswith("tacit-runtime-root-drain-"):
            construction_attempts += 1
            if construction_attempts == 1:
                raise RuntimeError("final drain owner construction failed")
        return real_thread(*args, **kwargs)

    async def observed_release(handle: Any) -> None:
        try:
            await graph.release_root_owner(handle)
        except BaseException as exc:
            observed_errors.append(exc)
            raise

    release_error: BaseException | None = None
    with monkeypatch.context() as constructor_fault:
        constructor_fault.setattr(threading, "Thread", reject_first_drain_owner)
        try:
            await release_runtime_root_with_startup_retry(observed_release, root)
        except BaseException as exc:
            release_error = exc

    if graph.root_state == "active":
        await graph.release_root_owner(root)

    assert release_error is None
    assert construction_attempts == 2
    assert len(observed_errors) == 1
    assert isinstance(observed_errors[0], RuntimeRootDrainStartupError)
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_root_transition_thread_constructor_failure_keeps_root_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = f"matrix:root-transition-constructor:{uuid4().hex}"
    controller = PipelineAdmissionController(1, max_queued=0, runtime_identity=identity)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    real_thread = threading.Thread
    construction_attempts = 0

    def reject_first_transition(*args: Any, **kwargs: Any) -> threading.Thread:
        nonlocal construction_attempts
        if str(kwargs.get("name", "")).startswith("tacit-lifecycle-transition-"):
            construction_attempts += 1
            if construction_attempts == 1:
                raise RuntimeError("root transition construction failed")
        return real_thread(*args, **kwargs)

    monkeypatch.setattr(threading, "Thread", reject_first_transition)

    await release_runtime_root_with_startup_retry(graph.release_root_owner, root)

    assert construction_attempts == 2
    assert graph.root_state == "closed"
    assert graph.root_owner_count == 0
    assert controller.runtime_fatal_circuit is None
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_root_recovery_transition_constructor_failure_fences_owned_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = f"matrix:root-recovery-transition-constructor:{uuid4().hex}"
    controller = PipelineAdmissionController(1, max_queued=0, runtime_identity=identity)
    graph = controller.execution_graph
    root = graph.register_root_owner()
    release_calls = 0
    real_thread = threading.Thread

    async def fail_release(_handle: Any) -> None:
        nonlocal release_calls
        release_calls += 1
        raise RuntimeRootDrainStartupError(f"root owner startup failed {release_calls}")

    def reject_recovery_transition(*args: Any, **kwargs: Any) -> threading.Thread:
        if str(kwargs.get("name", "")).startswith("tacit-lifecycle-transition-"):
            raise RuntimeError("root recovery transition construction failed")
        return real_thread(*args, **kwargs)

    monkeypatch.setattr(threading, "Thread", reject_recovery_transition)

    with pytest.raises(RuntimeRootDrainStartupError, match="root owner startup failed 2"):
        await release_runtime_root_with_startup_retry(fail_release, root)

    assert release_calls == 2
    assert graph.root_state == "fenced"
    assert graph.root_owner_count == 0
    assert controller.runtime_root_state == "draining"
    assert controller.runtime_fatal_circuit is not None


@pytest.mark.asyncio
async def test_recovery_thread_constructor_failure_retains_fatal_fenced_graph_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = f"matrix:recovery-constructor:{uuid4().hex}"
    controller = PipelineAdmissionController(1, max_queued=0, runtime_identity=identity)
    graph = controller.execution_graph
    root = graph.register_root_owner()

    class ConstructorFaultThread(threading.Thread):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            if str(kwargs.get("name", "")).startswith("tacit-runtime-root-recovery-"):
                raise RuntimeError("recovery owner construction failed")
            super().__init__(*args, **kwargs)

        def start(self) -> None:
            if self.name.startswith("tacit-runtime-root-drain-"):
                raise RuntimeError("final drain owner could not start")
            super().start()

    with monkeypatch.context() as constructor_fault:
        constructor_fault.setattr(threading, "Thread", ConstructorFaultThread)
        with pytest.raises(RuntimeRootDrainStartupError):
            await release_runtime_root_with_startup_retry(graph.release_root_owner, root)

    retained_state = (
        graph.root_state,
        graph.root_owner_count,
        controller.runtime_root_state,
        controller.runtime_fatal_circuit,
    )
    if graph.root_state == "active":
        await graph.release_root_owner(root)

    assert retained_state[0] == "draining"
    assert retained_state[1] == 0
    assert retained_state[2] == "draining"
    assert retained_state[3] is not None
    assert controller.in_flight == 0
    assert controller.blocking_in_flight == 0
    assert controller.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_maintenance_thread_constructor_failure_rejects_claims_without_release_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=2)
    active = await controller.acquire()

    async def queued_waiter(partition: str) -> PipelineAdmissionRejected | None:
        try:
            lease = await controller.acquire(timeout_seconds=1.0, partition_key=partition)
        except PipelineAdmissionRejected as exc:
            return exc
        controller.release(lease)
        return None

    waiters = [
        asyncio.create_task(queued_waiter("tenant-a")),
        asyncio.create_task(queued_waiter("tenant-b")),
    ]
    for _ in range(100):
        if controller.queued == len(waiters):
            break
        await asyncio.sleep(0.001)
    assert controller.queued == len(waiters)

    real_thread = threading.Thread
    maintenance_name = f"tacit-pipeline-admission-maintenance-{id(controller)}"

    def reject_maintenance_owner(*args: Any, **kwargs: Any) -> threading.Thread:
        if kwargs.get("name") == maintenance_name:
            raise RuntimeError("maintenance owner construction failed")
        return real_thread(*args, **kwargs)

    release_error: BaseException | None = None
    with monkeypatch.context() as constructor_fault:
        constructor_fault.setattr(threading, "Thread", reject_maintenance_owner)
        try:
            controller.release(active)
        except BaseException as exc:
            release_error = exc
    results = await asyncio.wait_for(asyncio.gather(*waiters), timeout=1.0)

    assert release_error is None
    assert all(isinstance(result, PipelineAdmissionRejected) for result in results)
    assert controller.in_flight == 0
    assert controller.queued == 0
    assert controller.retained == 0
    with controller._lock:
        assert not controller._selected
        assert not controller._selected_claims
        assert controller._selected_maintenance_thread is None


def test_late_bind_rejects_identity_owned_by_process_controller(tmp_path: Any) -> None:
    identity = f"matrix:process-controller-collision:{uuid4().hex}"
    settings = _settings(tmp_path, suffix="process-controller-collision")
    process_controller = runtime_admission_controller(settings, runtime_identity=identity)
    isolated = PipelineAdmissionController(1, max_queued=0)
    original_identity = isolated.runtime_identity

    with pytest.raises(RuntimeOwnershipError):
        isolated.bind_runtime_identity(identity)

    assert isolated.runtime_identity == original_identity
    assert isolated.execution_graph.runtime_identity == original_identity
    assert process_controller.runtime_identity == identity


@pytest.mark.asyncio
async def test_explicit_runtime_identity_has_one_canonical_limit_authority(tmp_path: Any) -> None:
    identity = f"matrix:canonical-explicit:{uuid4().hex}"
    settings = _settings(tmp_path, suffix="canonical-explicit", limit=1, queued=0)
    canonical = PipelineAdmissionController(
        1,
        max_queued=0,
        runtime_identity=identity,
    )

    with pytest.raises(RuntimeOwnershipError, match="already has an authority"):
        PipelineAdmissionController(
            1,
            max_queued=0,
            runtime_identity=identity,
        )

    assert runtime_admission_controller(settings, runtime_identity=identity) is canonical
    active = await canonical.acquire()
    with pytest.raises(PipelineAdmissionRejected):
        await canonical.acquire()
    canonical.release(active)
    assert canonical.in_flight == 0


def test_concurrent_bind_rejects_canonical_owner_without_provider_lock_inversion() -> None:
    identity = f"matrix:bind-provider-order:{uuid4().hex}"
    canonical = PipelineAdmissionController(1, max_queued=0, runtime_identity=identity)
    isolated = PipelineAdmissionController(1, max_queued=0)
    manager_lock = threading.Lock()
    provider_has_manager_lock = threading.Event()
    allow_provider_controller_check = threading.Event()
    provider_finished = threading.Event()
    bind_finished = threading.Event()
    bind_errors: list[BaseException] = []
    provider_controller_checks: list[bool] = []

    class ProviderManager:
        @property
        def lifecycle_state(self) -> str:
            with manager_lock:
                return "empty"

    with canonical.execution_graph._lock:
        canonical.execution_graph._provider_manager = ProviderManager()

    def provider_start_transition() -> None:
        with manager_lock:
            provider_has_manager_lock.set()
            allow_provider_controller_check.wait(timeout=1.0)
            acquired = canonical._lock.acquire(timeout=0.5)
            provider_controller_checks.append(acquired)
            if acquired:
                canonical._lock.release()
        provider_finished.set()

    def bind_isolated_controller() -> None:
        try:
            isolated.bind_runtime_identity(identity)
        except BaseException as exc:
            bind_errors.append(exc)
        finally:
            bind_finished.set()

    provider_thread = threading.Thread(target=provider_start_transition)
    bind_thread = threading.Thread(target=bind_isolated_controller)
    provider_thread.start()
    assert provider_has_manager_lock.wait(timeout=1.0)
    bind_thread.start()
    try:
        assert bind_finished.wait(timeout=0.1)
    finally:
        allow_provider_controller_check.set()
        provider_thread.join(timeout=1.0)
        bind_thread.join(timeout=1.0)

    assert provider_finished.is_set()
    assert provider_controller_checks == [True]
    assert len(bind_errors) == 1
    assert isinstance(bind_errors[0], RuntimeOwnershipError)
    assert isolated.runtime_identity != identity


@pytest.mark.asyncio
async def test_noncolliding_rebind_moves_fatal_notification_membership() -> None:
    identity = f"matrix:fatal-membership-rebind:{uuid4().hex}"
    rebound = PipelineAdmissionController(1, max_queued=1)
    rebound.bind_runtime_identity(identity)
    active = await rebound.acquire()
    waiter = asyncio.create_task(rebound.acquire(timeout_seconds=1.0))
    for _ in range(100):
        if rebound.queued == 1:
            break
        await asyncio.sleep(0.001)
    assert rebound.queued == 1

    fatal = RuntimeOwnershipError("peer runtime cleanup failed")
    setattr(fatal, "cleanup_reason_code", "runtime_cleanup_failed")
    rebound.fence_runtime_fatal(fatal)
    await asyncio.sleep(0)
    notified_before_capacity_changed = waiter.done()

    rebound.release(active)
    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await waiter

    assert notified_before_capacity_changed is True
    assert rebound.in_flight == 0
    assert rebound.queued == 0
    assert rebound.runtime_fatal_circuit is not None
