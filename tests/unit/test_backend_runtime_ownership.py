"""Matrix tests for durable backend realization and cleanup ownership.

This suite defines the backend lifecycle contract before its production API is
implemented. ``PipelineDependencies.realize_backends()`` returns an explicit,
opaque lease with ``backends`` and ``lease_id``; it is not a compatibility
sequence for the former synchronous factory result. The deprecated
``backend_factory`` accessor must fail before construction and direct callers
must migrate to the async lease API. ``PipelineDependencies.close_backends()``
releases that lease through the shared ``backend_lifecycle_owner``. The
requester event loop transports the lease but never owns cleanup or admission
capacity.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
import threading
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from structlog.testing import capture_logs

from tacit.backends.base import DashboardBackend
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies, declare_backend_factory
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork
from tacit.pipeline_admission import PipelineAdmissionController, PipelineBlockingPermit
from tacit.runtime_ownership import runtime_descriptor_for_backends
from tacit.runtime_stores import RuntimeStores


class _CrossThreadAsyncGate:
    """Release an async waiter from a test thread without loop affinity."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self._lock = threading.Lock()
        self._released = False
        self._wakes: list[Any] = []

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        with self._lock:
            if self._released:
                event.set()
            else:
                self._wakes.append(lambda: loop.call_soon_threadsafe(event.set))
        self.started.set()
        await event.wait()

    def release(self) -> None:
        with self._lock:
            self._released = True
            wakes = tuple(self._wakes)
            self._wakes.clear()
        for wake in wakes:
            try:
                wake()
            except RuntimeError:
                pass


class _BackendProbe:
    name = "backend-runtime-owner-probe"
    query_language = "promql"

    def __init__(
        self,
        runtime_settings: Settings,
        lifecycle: PipelineAdmissionController,
        *,
        close_gate: _CrossThreadAsyncGate | None = None,
        fail_close: bool = False,
        fail_close_attempts: int = 0,
        resist_close_cancellation: bool = False,
        operation_gate: _CrossThreadAsyncGate | None = None,
    ) -> None:
        self.runtime_ownership = runtime_descriptor_for_backends(
            component="backend_runtime_owner_probe",
            runtime_settings=runtime_settings,
        )
        self.lifecycle = lifecycle
        self.close_gate = close_gate
        self.fail_close = fail_close
        self.fail_close_attempts = fail_close_attempts
        self.resist_close_cancellation = resist_close_cancellation
        self.operation_gate = operation_gate
        self.close_started = threading.Event()
        self.close_finished = threading.Event()
        self.operation_started = threading.Event()
        self.operation_finished = threading.Event()
        self.close_calls = 0
        self.close_cancellations = 0
        self.close_threads: list[int] = []
        self.operation_threads: list[int] = []
        self.active_operations = 0
        self.max_active_operations = 0
        self.close_capacity: list[tuple[int, int, int]] = []
        self._lock = threading.Lock()

    async def close(self) -> None:
        with self._lock:
            self.close_calls += 1
            attempt = self.close_calls
            self.close_threads.append(threading.get_ident())
            snapshot = self.lifecycle.health_snapshot()
            self.close_capacity.append(
                (
                    snapshot.active,
                    snapshot.retained,
                    snapshot.cleanup_in_flight,
                )
            )
        self.close_started.set()
        try:
            if self.resist_close_cancellation:
                while True:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        with self._lock:
                            self.close_cancellations += 1
            if self.close_gate is not None:
                await self.close_gate.wait()
            if self.fail_close or attempt <= self.fail_close_attempts:
                raise RuntimeError("synthetic backend close failure")
        finally:
            self.close_finished.set()

    async def discover_metrics(self, _keywords: list[str], _intent: Any) -> list[Any]:
        with self._lock:
            self.operation_threads.append(threading.get_ident())
            self.active_operations += 1
            self.max_active_operations = max(self.max_active_operations, self.active_operations)
        self.operation_started.set()
        try:
            if self.operation_gate is not None:
                await self.operation_gate.wait()
            return []
        finally:
            with self._lock:
                self.active_operations -= 1
            self.operation_finished.set()


def _settings(tmp_path: Path, *, limit: int = 1, queued: int = 0) -> Settings:
    return Settings(  # type: ignore[call-arg]
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
        grafana_enabled=True,
        grafana_url="http://127.0.0.1:3000",
        signalfx_enabled=False,
        pipeline_max_concurrent=limit,
        pipeline_max_queued=queued,
    )


def _dependencies(
    tmp_path: Path,
    *,
    limit: int = 1,
    queued: int = 0,
    close_gate: _CrossThreadAsyncGate | None = None,
    fail_close: bool = False,
    fail_close_attempts: int = 0,
    resist_close_cancellation: bool = False,
    operation_gate: _CrossThreadAsyncGate | None = None,
    cleanup_grace_seconds: float = 0.05,
) -> tuple[PipelineDependencies, list[_BackendProbe], list[int]]:
    runtime_settings = _settings(tmp_path, limit=limit, queued=queued)
    stores = RuntimeStores(runtime_settings)
    lifecycle = stores.pipeline_admission()
    products: list[_BackendProbe] = []
    factory_threads: list[int] = []

    def factory() -> list[DashboardBackend]:
        factory_threads.append(threading.get_ident())
        backend = _BackendProbe(
            runtime_settings,
            lifecycle,
            close_gate=close_gate,
            fail_close=fail_close,
            fail_close_attempts=fail_close_attempts,
            resist_close_cancellation=resist_close_cancellation,
            operation_gate=operation_gate,
        )
        products.append(backend)
        return [cast(DashboardBackend, backend)]

    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=stores,
        backend_factory=declare_backend_factory(
            factory,
            runtime_settings=runtime_settings,
            component="backend_runtime_owner_factory",
        ),
        cleanup_grace_seconds=cleanup_grace_seconds,
    )
    return dependencies, products, factory_threads


def _require_backend_lease(value: Any) -> Any:
    """Assert the public handoff is an owner lease rather than a raw list."""
    assert not isinstance(value, list), "backend products escaped without a durable lease"
    assert isinstance(value.lease_id, str) and value.lease_id
    assert tuple(value.backends)
    return value


def _backend_owner(dependencies: PipelineDependencies) -> Any:
    owner = getattr(dependencies, "backend_lifecycle_owner", None)
    assert owner is not None, "PipelineDependencies has no durable backend owner"
    return owner


def _active_backend_leases(dependencies: PipelineDependencies) -> int:
    count = getattr(_backend_owner(dependencies), "active_lease_count", None)
    assert isinstance(count, int)
    return count


async def _close_backend_lease(dependencies: PipelineDependencies, lease: Any) -> None:
    close = getattr(dependencies, "close_backends", None)
    assert callable(close), "PipelineDependencies cannot release an owned backend lease"
    await close(lease)


async def _wait_for_event(event: threading.Event, *, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not event.is_set():
        if loop.time() >= deadline:
            raise AssertionError("timed out waiting for backend lifecycle transition")
        await asyncio.sleep(0.001)


async def _wait_for_idle(
    dependencies: PipelineDependencies,
    *,
    timeout: float = 1.0,
) -> None:
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if (
            lifecycle.in_flight == 0
            and lifecycle.blocking_in_flight == 0
            and lifecycle.retained == 0
            and _active_backend_leases(dependencies) == 0
        ):
            return
        await asyncio.sleep(0.001)
    raise AssertionError("backend owner and admission capacity did not settle")


async def _stop_root(
    dependencies: PipelineDependencies,
    root: Any,
    *,
    tolerate_fatal: bool = False,
) -> None:
    try:
        await asyncio.wait_for(dependencies.stop_runtime_root(root), timeout=1.0)
    except RuntimeOwnershipError:
        if not tolerate_fatal:
            raise


def _run_matrix_probe_in_subprocess(
    probe_name: str,
    tmp_path: Path,
    *,
    timeout: float = 4.0,
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; import sys; "
            "from tests.unit import test_backend_runtime_ownership as matrix; "
            "getattr(matrix, sys.argv[1])(Path(sys.argv[2]))"
        ),
        probe_name,
        str(tmp_path),
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=Path(__file__).resolve().parents[2],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        pytest.fail(f"{probe_name} exceeded its {timeout:.1f}s process deadline: {exc}")
    assert (
        completed.returncode == 0
    ), f"{probe_name} failed in its isolated process\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    return completed


class _ForeignLockBlocker:
    """Hold one real component lock precisely when the owner reaches it."""

    def __init__(self, lock: Any) -> None:
        self._lock = lock
        self._begin = threading.Event()
        self.held = threading.Event()
        self.owner_waiting = threading.Event()
        self._release = threading.Event()
        self._thread = threading.Thread(target=self._hold, name="backend-foreign-lock-holder")

    def _hold(self) -> None:
        assert self._begin.wait(timeout=2.0)
        self._lock.acquire()
        try:
            self.held.set()
            assert self._release.wait(timeout=2.0)
        finally:
            self._lock.release()

    def start(self) -> None:
        self._thread.start()

    def intercept_owner(self) -> None:
        self._begin.set()
        assert self.held.wait(timeout=1.0)
        self.owner_waiting.set()

    def release(self) -> None:
        self._begin.set()
        self._release.set()
        self._thread.join(timeout=1.0)
        assert self._thread.is_alive() is False


async def _owner_loop_responds(state: Any, *, timeout: float = 0.2) -> bool:
    owner_loop = state.loop
    assert owner_loop is not None
    heartbeat = threading.Event()
    owner_loop.call_soon_threadsafe(heartbeat.set)
    return await asyncio.to_thread(heartbeat.wait, timeout)


def _probe_inherited_admission_abandonment(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    requester_ready = threading.Event()
    requester_paused = threading.Event()
    requester_done = threading.Event()
    resume_requester = threading.Event()
    resume_operation = _CrossThreadAsyncGate()
    transfer_observations: list[bool] = []
    operation_results: list[list[Any]] = []
    requester_errors: list[BaseException] = []

    def run_requester() -> None:
        loop = asyncio.new_event_loop()
        loop.set_exception_handler(lambda *_args: None)
        asyncio.set_event_loop(loop)

        async def pause_then_resume_after_adoption() -> None:
            try:
                async with lifecycle.slot() as request_lease:
                    lease = _require_backend_lease(await dependencies.realize_backends())
                    service_tokens = cast(set[int], getattr(lifecycle, "_service_admitted_tokens", set()))
                    transfer_observations.append(request_lease.token in service_tokens)
                    requester_ready.set()
                    loop.call_soon(loop.stop)
                    await resume_operation.wait()
                    operation_results.append(await lease.backends[0].discover_metrics([], None))
                    await _close_backend_lease(dependencies, lease)
            except BaseException as exc:
                requester_errors.append(exc)
            finally:
                requester_done.set()
                loop.call_soon(loop.stop)

        task = loop.create_task(pause_then_resume_after_adoption())
        setattr(task, "_log_destroy_pending", False)
        try:
            loop.run_forever()
            requester_paused.set()
            assert resume_requester.wait(timeout=2.0)
            loop.run_forever()
        finally:
            if not task.done():
                task.cancel()
                with suppress(BaseException):
                    loop.run_until_complete(task)
            loop.close()
            asyncio.set_event_loop(None)

    requester = threading.Thread(target=run_requester, name="inherited-backend-requester")
    requester.start()
    assert requester_ready.wait(timeout=1.0)
    assert requester_paused.wait(timeout=1.0)
    assert requester.is_alive() is True

    shutdown_done = threading.Event()
    shutdown_errors: list[BaseException] = []

    def stop_root() -> None:
        try:
            asyncio.run(_stop_root(dependencies, root))
        except BaseException as exc:
            shutdown_errors.append(exc)
        finally:
            shutdown_done.set()

    shutdown = threading.Thread(target=stop_root, name="backend-root-shutdown")
    shutdown.start()
    shutdown_waited_for_requester = not shutdown_done.wait(timeout=0.2)
    try:
        resume_operation.release()
        resume_requester.set()
        assert requester_done.wait(timeout=2.0)
        assert shutdown_done.wait(timeout=2.0)
    finally:
        resume_operation.release()
        resume_requester.set()
        requester.join(timeout=1.0)
        shutdown.join(timeout=1.0)
    assert requester.is_alive() is False
    assert shutdown.is_alive() is False

    assert transfer_observations == [True]
    assert shutdown_waited_for_requester, "final-root shutdown revoked a stopped but resumable requester"
    assert requester_errors == []
    assert shutdown_errors == []
    assert operation_results == [[]]
    assert products[0].close_finished.wait(timeout=1.0)
    assert products[0].close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.execution_graph.root_state == "closed"


def _probe_cancellation_resistant_backend_close(tmp_path: Path) -> None:
    async def run() -> None:
        dependencies, products, _factory_threads = _dependencies(
            tmp_path,
            resist_close_cancellation=True,
        )
        lifecycle = dependencies.pipeline_admission
        assert lifecycle is not None
        root = dependencies.start_runtime_root()
        assert root is not None
        lease = _require_backend_lease(await dependencies.realize_backends())
        close_task = asyncio.create_task(_close_backend_lease(dependencies, lease))
        await _wait_for_event(products[0].close_started)

        done, _pending = await asyncio.wait({close_task}, timeout=0.4)
        assert close_task in done, "cancellation-resistant backend close did not reach a bounded terminal state"
        with suppress(RuntimeOwnershipError):
            close_task.result()
        assert products[0].close_cancellations >= 1
        fatal = lifecycle.runtime_fatal_circuit
        assert fatal is not None
        assert fatal.reason_code == "backend_owner_cleanup_timeout"
        await _wait_for_idle(dependencies, timeout=0.5)
        await _stop_root(dependencies, root, tolerate_fatal=True)
        assert lifecycle.in_flight == 0
        assert lifecycle.retained == 0
        assert lifecycle.service_owner_in_flight == 0
        assert lifecycle.execution_graph.root_state == "closed"

    asyncio.run(run())


class _ObservedRegistryLock:
    def __init__(self, owner_thread_id: int) -> None:
        self._lock = threading.RLock()
        self._owner_thread_id = owner_thread_id
        self.owner_attempted = threading.Event()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if threading.get_ident() == self._owner_thread_id:
            self.owner_attempted.set()
        return self._lock.acquire(blocking, timeout)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self) -> _ObservedRegistryLock:
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


class _DrainObservedTaskSet(set[asyncio.Task[Any]]):
    """Signal when backend retirement observes its first active operation."""

    def __init__(self) -> None:
        super().__init__()
        self.draining_observed = threading.Event()

    def __bool__(self) -> bool:
        self.draining_observed.set()
        return super().__len__() > 0


@pytest.mark.asyncio
async def test_owned_resource_realization_requires_durable_adopt_before_factory() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    work = LifecycleOwnedBlockingWork(lifecycle)
    calls = 0

    def factory() -> object:
        nonlocal calls
        calls += 1
        return object()

    with pytest.raises((TypeError, RuntimeOwnershipError), match="adopt|durable"):
        await work.realize_owned(
            factory,
            validate=lambda _product: None,
            adopt=None,
            retire=lambda _product: None,
            reason_code="backend_resource_requires_durable_adopt",
        )

    assert calls == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


def test_sync_owned_resource_realization_requires_durable_adopt_before_factory() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    work = LifecycleOwnedBlockingWork(lifecycle)
    calls = 0

    def factory() -> object:
        nonlocal calls
        calls += 1
        return object()

    with pytest.raises((TypeError, RuntimeOwnershipError), match="adopt|durable"):
        work.realize_owned_sync(
            factory,
            validate=lambda _product: None,
            retire=lambda _product: None,
            reason_code="sync_backend_resource_requires_durable_adopt",
        )

    assert calls == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_cancellation_during_factory_permit_release_retires_adopted_backend_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    release_entered = threading.Event()
    allow_release = threading.Event()
    original_release = lifecycle.release_blocking_permit

    def delayed_release(permit: PipelineBlockingPermit) -> None:
        if products and not permit.cleanup:
            release_entered.set()
            assert allow_release.wait(timeout=1.0)
        original_release(permit)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", delayed_release)
    realization = asyncio.create_task(dependencies.realize_backends())
    try:
        await _wait_for_event(release_entered)
        assert len(products) == 1
        assert _active_backend_leases(dependencies) == 1
        realization.cancel()
        allow_release.set()
        with pytest.raises(asyncio.CancelledError):
            await realization
        await _wait_for_event(products[0].close_finished)
        assert products[0].close_calls == 1
        assert products[0].close_capacity[0][0] == 1
        await _wait_for_idle(dependencies)
    finally:
        allow_release.set()
        await _stop_root(dependencies, root)


def test_closed_requester_loop_after_adoption_is_reclaimed_by_runtime_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    requester_loop: list[asyncio.AbstractEventLoop] = []
    requester_done = threading.Event()
    release_finished = threading.Event()
    original_release = lifecycle.release_blocking_permit

    def stop_before_caller_resume(permit: PipelineBlockingPermit) -> None:
        original_release(permit)
        if products and not permit.cleanup and requester_loop:
            release_finished.set()
            requester_loop[0].call_soon_threadsafe(requester_loop[0].stop)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", stop_before_caller_resume)

    def run_requester() -> None:
        loop = asyncio.new_event_loop()
        loop.set_exception_handler(lambda *_args: None)
        asyncio.set_event_loop(loop)
        requester_loop.append(loop)
        task = loop.create_task(dependencies.realize_backends())
        setattr(task, "_log_destroy_pending", False)
        try:
            loop.run_forever()
        finally:
            loop.close()
            asyncio.set_event_loop(None)
            requester_done.set()

    requester = threading.Thread(target=run_requester, name="backend-requester-loop")
    requester.start()
    assert release_finished.wait(timeout=1.0)
    assert requester_done.wait(timeout=1.0)
    requester.join(timeout=1.0)
    assert requester.is_alive() is False
    assert len(products) == 1
    assert factory_threads[0] != requester.ident

    asyncio.run(_stop_root(dependencies, root))

    assert products[0].close_finished.wait(timeout=1.0)
    assert products[0].close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert _active_backend_leases(dependencies) == 0


@pytest.mark.asyncio
async def test_concurrent_runs_share_owner_but_keep_backend_leases_isolated(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path, limit=2)
    root = dependencies.start_runtime_root()
    assert root is not None
    try:
        first, second = await asyncio.gather(
            dependencies.realize_backends(),
            dependencies.realize_backends(),
        )
        first = _require_backend_lease(first)
        second = _require_backend_lease(second)
        assert first.lease_id != second.lease_id
        assert first.backends[0] is not products[0]
        assert second.backends[0] is not products[1]
        assert _active_backend_leases(dependencies) == 2

        await _close_backend_lease(dependencies, first)
        assert sum(product.close_calls for product in products) == 1
        assert _active_backend_leases(dependencies) == 1

        await _close_backend_lease(dependencies, second)
        assert sorted(product.close_calls for product in products) == [1, 1]
        await _wait_for_idle(dependencies)
    finally:
        await _stop_root(dependencies, root)


@pytest.mark.parametrize(
    "authority_dimension",
    (
        "graph_nonce",
        "generation_epoch",
        "lease_id",
        "provider_lease",
        "proxy_set",
        "provider_release_disposition",
    ),
)
@pytest.mark.asyncio
async def test_backend_release_rejects_every_mutated_lease_authority_dimension(
    tmp_path: Path,
    authority_dimension: str,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path, limit=2)
    root = dependencies.start_runtime_root()
    assert root is not None
    first, second = await asyncio.gather(
        dependencies.realize_backends(),
        dependencies.realize_backends(),
    )
    first = _require_backend_lease(first)
    second = _require_backend_lease(second)
    mutations = {
        "graph_nonce": {"graph_nonce": f"{first.graph_nonce}-forged"},
        "generation_epoch": {"generation_epoch": first.generation_epoch + 1},
        "lease_id": {"lease_id": second.lease_id},
        "provider_lease": {"_provider_lease": second._provider_lease},
        "proxy_set": {"backends": second.backends},
        "provider_release_disposition": {
            "_release_provider_lease": not first._release_provider_lease,
        },
    }
    forged = replace(first, **mutations[authority_dimension])
    try:
        with pytest.raises(RuntimeOwnershipError, match="lease|authority|identity"):
            await _close_backend_lease(dependencies, forged)
        assert [product.close_calls for product in products] == [0, 0]
        assert _active_backend_leases(dependencies) == 2

        await _close_backend_lease(dependencies, first)
        await _close_backend_lease(dependencies, second)
        assert sorted(product.close_calls for product in products) == [1, 1]
        await _wait_for_idle(dependencies)
    finally:
        with suppress(BaseException):
            await _close_backend_lease(dependencies, first)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, second)
        await _stop_root(dependencies, root, tolerate_fatal=True)


@pytest.mark.asyncio
async def test_backend_owner_close_is_concurrently_idempotent(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    try:
        lease = _require_backend_lease(await dependencies.realize_backends())
        await asyncio.gather(
            _close_backend_lease(dependencies, lease),
            _close_backend_lease(dependencies, lease),
        )
        await _close_backend_lease(dependencies, lease)
        assert products[0].close_calls == 1
        await _wait_for_idle(dependencies)
    finally:
        await _stop_root(dependencies, root)


@pytest.mark.asyncio
async def test_backend_proxy_cannot_close_its_shared_owner_lease(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    try:
        lease = _require_backend_lease(await dependencies.realize_backends())
        with pytest.raises(RuntimeOwnershipError, match="lease owner"):
            await lease.backends[0].close()
        assert products[0].close_calls == 0
        assert _active_backend_leases(dependencies) == 1

        await _close_backend_lease(dependencies, lease)
        assert products[0].close_calls == 1
        await _wait_for_idle(dependencies)
    finally:
        await _stop_root(dependencies, root)


@pytest.mark.asyncio
async def test_final_runtime_root_drain_discovers_and_closes_abandoned_backend_owner(
    tmp_path: Path,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    root_stopped = False
    try:
        lease = _require_backend_lease(await dependencies.realize_backends())
        assert lease.backends[0] is not products[0]
        assert _active_backend_leases(dependencies) == 1
        assert lifecycle.in_flight == 1

        await _stop_root(dependencies, root)
        root_stopped = True

        assert products[0].close_calls == 1
        assert _active_backend_leases(dependencies) == 0
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.execution_graph.root_state == "closed"
    finally:
        if not root_stopped:
            await _stop_root(dependencies, root)


@pytest.mark.asyncio
async def test_backend_capacity_stays_charged_until_owner_close_finishes(tmp_path: Path) -> None:
    close_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        close_gate=close_gate,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    try:
        lease = _require_backend_lease(await dependencies.realize_backends())
        close_task = asyncio.create_task(_close_backend_lease(dependencies, lease))
        await _wait_for_event(close_gate.started)
        assert lifecycle.in_flight == 1
        assert lifecycle.retained >= 1
        assert _active_backend_leases(dependencies) == 1
        with pytest.raises(PipelineAdmissionRejected):
            async with lifecycle.slot(timeout_seconds=0.01):
                raise AssertionError("limit+1 work acquired capacity during backend close")

        close_gate.release()
        await close_task
        assert products[0].close_calls == 1
        assert products[0].close_capacity[0][0] == 1
        await _wait_for_idle(dependencies)
    finally:
        close_gate.release()
        await _stop_root(dependencies, root)


@pytest.mark.asyncio
async def test_backend_operations_and_close_run_on_the_durable_owner_loop(tmp_path: Path) -> None:
    dependencies, products, factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    try:
        lease = _require_backend_lease(await dependencies.realize_backends())
        assert await lease.backends[0].discover_metrics([], None) == []
        await _close_backend_lease(dependencies, lease)

        assert products[0].operation_threads
        assert products[0].close_threads
        assert products[0].operation_threads[0] == products[0].close_threads[0]
        assert products[0].operation_threads[0] != factory_threads[0]
        assert products[0].operation_threads[0] != threading.get_ident()
        await _wait_for_idle(dependencies)
    finally:
        await _stop_root(dependencies, root)


@pytest.mark.asyncio
async def test_permanent_backend_close_failure_fatal_fences_runtime(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path, fail_close=True)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    try:
        lease = _require_backend_lease(await dependencies.realize_backends())
        with pytest.raises(RuntimeOwnershipError, match="cleanup|close"):
            await _close_backend_lease(dependencies, lease)
        assert 1 <= products[0].close_calls <= 2
        assert products[0].close_capacity[0][0] == 1
        fatal = lifecycle.runtime_fatal_circuit
        assert fatal is not None
        assert fatal.reason_code == "backend_owner_cleanup_failed"
        assert _active_backend_leases(dependencies) == 0
        with pytest.raises(RuntimeOwnershipError, match="cleanup"):
            await dependencies.realize_backends()
    finally:
        await _stop_root(dependencies, root, tolerate_fatal=True)


@pytest.mark.asyncio
async def test_ambiguous_factory_permit_release_retires_owner_and_fatal_fences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    original_release = lifecycle.release_blocking_permit
    failed = False

    def ambiguous_release(permit: PipelineBlockingPermit) -> None:
        nonlocal failed
        original_release(permit)
        if products and not permit.cleanup and not failed:
            failed = True
            raise RuntimeError("synthetic post-release ambiguity")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", ambiguous_release)
    try:
        with pytest.raises(RuntimeOwnershipError, match="capacity release|cleanup"):
            await dependencies.realize_backends()
        await _wait_for_event(products[0].close_finished)
        assert products[0].close_calls == 1
        assert lifecycle.runtime_fatal_circuit is not None
        assert _active_backend_leases(dependencies) == 0
    finally:
        await _stop_root(dependencies, root, tolerate_fatal=True)


@pytest.mark.asyncio
async def test_backend_owner_holds_aggregate_limit_and_rejects_limit_plus_one(
    tmp_path: Path,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path, limit=2)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    leases: list[Any] = []
    try:
        leases.extend(
            _require_backend_lease(value)
            for value in await asyncio.gather(
                dependencies.realize_backends(),
                dependencies.realize_backends(),
            )
        )
        assert len(products) == 2
        assert lifecycle.in_flight == 2
        assert _active_backend_leases(dependencies) == 2

        with pytest.raises(PipelineAdmissionRejected):
            await dependencies.realize_backends()
        assert len(products) == 2

        await asyncio.gather(*(_close_backend_lease(dependencies, lease) for lease in leases))
        assert [product.close_calls for product in products] == [1, 1]
        await _wait_for_idle(dependencies)
    finally:
        for lease in leases:
            with suppress(BaseException):
                await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root)


def test_stopped_but_open_requester_remains_live_until_resume_and_backend_use(
    tmp_path: Path,
) -> None:
    _run_matrix_probe_in_subprocess("_probe_inherited_admission_abandonment", tmp_path)


def test_cancellation_resistant_backend_close_has_a_bounded_terminal_owner(
    tmp_path: Path,
) -> None:
    completed = _run_matrix_probe_in_subprocess("_probe_cancellation_resistant_backend_close", tmp_path)
    rendered = f"{completed.stdout}\n{completed.stderr}"
    assert "reason_code=backend_owner_cleanup_timeout" in rendered
    assert "runtime_owner_pending_tasks_abandoned" in rendered


@pytest.mark.asyncio
async def test_foreign_registry_lock_cannot_block_owner_loop_progress(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    owner = _backend_owner(dependencies)
    original_record = owner._record
    record = owner._records[lease.lease_id]
    owner_thread = record.generation.owner_thread
    assert owner_thread is not None and owner_thread.ident is not None
    observed_lock = _ObservedRegistryLock(owner_thread.ident)
    lock_held = threading.Event()
    release_lock = threading.Event()
    operation: asyncio.Task[Any] | None = None

    def hold_registry_lock() -> None:
        with observed_lock:
            lock_held.set()
            assert release_lock.wait(timeout=1.0)

    holder = threading.Thread(target=hold_registry_lock, name="backend-registry-holder")
    original_id_lock = owner._id_lock
    owner._id_lock = observed_lock
    owner._record = lambda lease_id: record if lease_id == lease.lease_id else original_record(lease_id)
    holder.start()
    try:
        await _wait_for_event(lock_held)
        operation = asyncio.create_task(lease.backends[0].discover_metrics([], None))
        loop = asyncio.get_running_loop()
        attempt_deadline = loop.time() + 0.3
        while not observed_lock.owner_attempted.is_set() and not operation.done():
            if loop.time() >= attempt_deadline:
                raise AssertionError("backend operation neither completed nor reached registry synchronization")
            await asyncio.sleep(0.001)

        heartbeat = threading.Event()
        record.generation.loop.call_soon_threadsafe(heartbeat.set)
        heartbeat_deadline = loop.time() + 0.1
        while not heartbeat.is_set() and loop.time() < heartbeat_deadline:
            await asyncio.sleep(0.001)
        assert heartbeat.is_set(), "foreign registry synchronization blocked the runtime owner loop"
    finally:
        release_lock.set()
        holder.join(timeout=1.0)
        assert holder.is_alive() is False
        if operation is not None:
            with suppress(BaseException):
                await asyncio.wait_for(operation, timeout=1.0)
        owner._record = original_record
        owner._id_lock = original_id_lock
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)
    assert products[0].operation_threads


@pytest.mark.parametrize("lock_boundary", ("admission", "provider_host", "release_state"))
@pytest.mark.asyncio
async def test_real_foreign_component_locks_never_block_backend_owner_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lock_boundary: str,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    lease: Any | None = None
    operation: asyncio.Task[Any] | None = None
    intercept_threads: list[int] = []

    if lock_boundary == "admission":
        blocker = _ForeignLockBlocker(lifecycle._lock)
        original_transfer = lifecycle.transfer_retained_work_to_service_owner

        def blocked_admission_transfer(permit: Any) -> None:
            intercept_threads.append(threading.get_ident())
            blocker.intercept_owner()
            original_transfer(permit)

        monkeypatch.setattr(
            lifecycle,
            "transfer_retained_work_to_service_owner",
            blocked_admission_transfer,
        )
        blocker.start()
        operation = asyncio.create_task(dependencies.realize_backends())
    elif lock_boundary == "provider_host":
        blocker = _ForeignLockBlocker(host._lock)
        original_transfer = host._transfer_provider_lease_to_backend_owner

        def blocked_host_transfer(handle: Any, state: Any) -> None:
            intercept_threads.append(threading.get_ident())
            blocker.intercept_owner()
            original_transfer(handle, state)

        monkeypatch.setattr(host, "_transfer_provider_lease_to_backend_owner", blocked_host_transfer)
        blocker.start()
        operation = asyncio.create_task(dependencies.realize_backends())
    else:
        lease = _require_backend_lease(await dependencies.realize_backends())
        release_state = lease._release_state
        blocker = _ForeignLockBlocker(release_state.lock)
        release_state_type = type(release_state)
        original_mark_released = release_state_type.try_mark_resource_released_on_owner

        def blocked_mark_released(state: Any) -> bool:
            if state is release_state:
                intercept_threads.append(threading.get_ident())
                blocker.intercept_owner()
            return original_mark_released(state)

        monkeypatch.setattr(
            release_state_type,
            "try_mark_resource_released_on_owner",
            blocked_mark_released,
        )
        blocker.start()
        operation = asyncio.create_task(_close_backend_lease(dependencies, lease))

    owner_was_responsive = False
    try:
        await _wait_for_event(blocker.owner_waiting)
        state = host._generation_owner
        assert state is not None
        owner_was_responsive = await _owner_loop_responds(state)
    finally:
        blocker.release()

    try:
        result = await asyncio.wait_for(operation, timeout=1.0)
        if lock_boundary != "release_state":
            lease = _require_backend_lease(result)
            await _close_backend_lease(dependencies, lease)
        await _wait_for_idle(dependencies)
    finally:
        if lease is not None:
            with suppress(BaseException):
                await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert intercept_threads
    assert not any(thread_id == threading.get_ident() for thread_id in intercept_threads)
    assert owner_was_responsive, f"foreign {lock_boundary} lock blocked the backend owner loop"
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_provider_close_retries_composite_release_lock_without_blocking_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    provider_lease = await dependencies.acquire_resources()
    assert provider_lease is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    release_state = lease._release_state
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    state_type = type(release_state)
    blocker = _ForeignLockBlocker(release_state.lock)
    original_try_begin = state_type.try_begin_composite_release_on_owner
    original_schedule_retry = host._schedule_generation_retry_on_owner
    owner_attempts: list[int] = []
    retry_scheduled = threading.Event()
    pending_retries = 0
    max_pending_retries = 0

    def observe_try_begin(state: Any) -> Any:
        if state is release_state:
            owner_attempts.append(threading.get_ident())
            blocker.intercept_owner()
        return original_try_begin(state)

    def observe_retry(state: Any, phase: str, callback: Any) -> None:
        nonlocal pending_retries, max_pending_retries
        if phase != f"backend_composite_release:{release_state.lease_id}":
            original_schedule_retry(state, phase, callback)
            return
        pending_retries += 1
        max_pending_retries = max(max_pending_retries, pending_retries)
        retry_scheduled.set()

        def finish_retry() -> None:
            nonlocal pending_retries
            try:
                callback()
            finally:
                pending_retries -= 1

        original_schedule_retry(state, phase, finish_retry)

    monkeypatch.setattr(
        state_type,
        "try_begin_composite_release_on_owner",
        observe_try_begin,
    )
    monkeypatch.setattr(host, "_schedule_generation_retry_on_owner", observe_retry)
    blocker.start()
    provider_close = asyncio.create_task(dependencies.close_resources(provider_lease))
    owner_was_responsive = False
    capacity_remained_charged = False
    try:
        await _wait_for_event(blocker.owner_waiting)
        await _wait_for_event(retry_scheduled)
        state = host._generation_owner
        assert state is not None
        owner_was_responsive = await _owner_loop_responds(state)
        capacity_remained_charged = (
            lifecycle.in_flight == 1
            and lifecycle.service_owner_in_flight == 1
            and _active_backend_leases(dependencies) == 1
            and not provider_close.done()
        )
        with pytest.raises(PipelineAdmissionRejected):
            async with lifecycle.slot(timeout_seconds=0.01):
                raise AssertionError("limit+1 work acquired capacity during composite release contention")
    finally:
        blocker.release()

    try:
        await asyncio.wait_for(provider_close, timeout=1.0)
        await _wait_for_idle(dependencies)
    finally:
        if not provider_close.done():
            provider_close.cancel()
            await asyncio.gather(provider_close, return_exceptions=True)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert owner_attempts
    assert all(thread_id != threading.get_ident() for thread_id in owner_attempts)
    assert max_pending_retries == 1
    assert pending_retries == 0
    assert owner_was_responsive
    assert capacity_remained_charged
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_direct_backend_release_never_waits_on_foreign_composite_lock(
    tmp_path: Path,
) -> None:
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        cleanup_grace_seconds=1.0,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    release_state = lease._release_state
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    state = host._generation_owner
    assert state is not None
    lock_held = threading.Event()
    release_lock = threading.Event()
    safety_released = threading.Event()

    def hold_foreign_lock() -> None:
        release_state.lock.acquire()
        try:
            lock_held.set()
            assert release_lock.wait(timeout=2.0)
        finally:
            release_state.lock.release()

    holder = threading.Thread(target=hold_foreign_lock, name="backend-composite-lock-holder")
    holder.start()
    assert await asyncio.to_thread(lock_held.wait, 1.0)

    def safety_release() -> None:
        safety_released.set()
        release_lock.set()

    safety = threading.Timer(0.5, safety_release)
    requester_heartbeat = threading.Event()
    release_task = asyncio.create_task(_close_backend_lease(dependencies, lease))
    started = time.monotonic()
    safety.start()
    try:
        asyncio.get_running_loop().call_soon(requester_heartbeat.set)
        await asyncio.sleep(0)
        elapsed = time.monotonic() - started
        assert requester_heartbeat.is_set(), "requester loop stopped at the backend composite lock"
        assert elapsed < 0.2, "requester release waited on the backend composite lock"
        assert not safety_released.is_set(), "requester resumed only after the safety lock release"
        assert await _owner_loop_responds(state)
        assert not release_task.done()
        assert _active_backend_leases(dependencies) == 1
    finally:
        release_lock.set()
        safety.cancel()
        holder.join(timeout=1.0)

    assert holder.is_alive() is False
    try:
        await asyncio.wait_for(release_task, timeout=1.0)
        await _wait_for_idle(dependencies)
    finally:
        if not release_task.done():
            release_task.cancel()
            await asyncio.gather(release_task, return_exceptions=True)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    snapshot = lifecycle.health_snapshot()
    assert snapshot.active == 0
    assert snapshot.queued == 0
    assert snapshot.retained == 0
    assert snapshot.blocking_in_flight == 0
    assert snapshot.service_owner_in_flight == 0
    assert _active_backend_leases(dependencies) == 0
    assert products[0].close_calls == 1


@pytest.mark.parametrize("lock_boundary", ("submission", "manager"))
@pytest.mark.asyncio
async def test_requester_held_provider_locks_cannot_block_owner_heartbeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    lock_boundary: str,
) -> None:
    operation_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        operation_gate=operation_gate,
    )
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    operation = asyncio.create_task(lease.backends[0].discover_metrics([], None))
    observed_lock: _ObservedRegistryLock | None = None
    original_lock: Any = None
    submission: Any = None
    owner_was_responsive = False
    try:
        await _wait_for_event(products[0].operation_started)
        with host._lock:
            state = host._generation_owner
            assert state is not None
            assert len(state.committed_submissions) == 1
            submission = next(iter(state.committed_submissions.values()))
            owner_thread = state.owner_thread
            assert owner_thread is not None and owner_thread.ident is not None

        observed_lock = _ObservedRegistryLock(owner_thread.ident)
        settlement_entered = threading.Event()
        original_settle = host._settle_generation_submission

        def observe_settlement(
            settled_state: Any,
            settled_submission: Any,
            **kwargs: Any,
        ) -> bool:
            if settled_submission is submission:
                settlement_entered.set()
            return bool(original_settle(settled_state, settled_submission, **kwargs))

        monkeypatch.setattr(host, "_settle_generation_submission", observe_settlement)
        if lock_boundary == "submission":
            original_lock = submission.lock
            submission.lock = observed_lock
        else:
            original_lock = host._lock
            host._lock = observed_lock
        observed_lock.acquire()

        operation_gate.release()
        await _wait_for_event(settlement_entered)
        owner_was_responsive = await _owner_loop_responds(state)
    finally:
        if observed_lock is not None:
            observed_lock.release()
        if not operation.done():
            with suppress(BaseException):
                await asyncio.wait_for(operation, timeout=1.0)
        if original_lock is not None:
            if lock_boundary == "submission" and submission is not None:
                submission.lock = original_lock
            elif lock_boundary == "manager":
                host._lock = original_lock
        operation_gate.release()
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert owner_was_responsive, f"requester-held provider {lock_boundary} lock blocked the owner loop"
    assert products[0].operation_finished.is_set()


@pytest.mark.asyncio
async def test_provider_terminal_monitor_never_waits_on_requester_manager_lock(
    tmp_path: Path,
) -> None:
    dependencies, _products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None

    with host._lock:
        state = host._generation_owner
        assert state is not None
        owner_thread = state.owner_thread
        assert owner_thread is not None and owner_thread.ident is not None
        original_lock = host._lock
        observed_lock = _ObservedRegistryLock(owner_thread.ident)
        host._lock = observed_lock

    owner_was_responsive = False
    observed_lock.acquire()
    try:
        await _wait_for_event(observed_lock.owner_attempted)
        owner_was_responsive = await _owner_loop_responds(state)
    finally:
        observed_lock.release()
        host._lock = original_lock
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert owner_was_responsive, "requester-held manager lock blocked the provider terminal monitor"


@pytest.mark.parametrize("cancel_requester", (False, True), ids=("live-caller", "cancelled-caller"))
@pytest.mark.asyncio
async def test_backend_close_waits_for_active_operations_before_closing_product(
    tmp_path: Path,
    cancel_requester: bool,
) -> None:
    operation_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        operation_gate=operation_gate,
    )
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    operation = asyncio.create_task(lease.backends[0].discover_metrics([], None))
    close_task: asyncio.Task[None] | None = None
    close_started_while_operation_active = False
    try:
        await _wait_for_event(products[0].operation_started)
        if cancel_requester:
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation

        close_task = asyncio.create_task(_close_backend_lease(dependencies, lease))
        close_started_while_operation_active = await asyncio.to_thread(
            products[0].close_started.wait,
            0.2,
        )
        operation_gate.release()
        if not cancel_requester:
            assert await operation == []
        await _wait_for_event(products[0].operation_finished)
        await asyncio.wait_for(close_task, timeout=1.0)
        await _wait_for_idle(dependencies)
    finally:
        operation_gate.release()
        if not operation.done():
            operation.cancel()
            with suppress(BaseException):
                await operation
        if close_task is not None and not close_task.done():
            with suppress(BaseException):
                await asyncio.wait_for(close_task, timeout=1.0)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert not close_started_while_operation_active, "backend close raced an active owner-loop operation"
    assert products[0].operation_finished.is_set()
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_cancelled_close_during_active_operation_drain_keeps_retirement_owned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        operation_gate=operation_gate,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    owner = _backend_owner(dependencies)
    record = owner._records[lease.lease_id]
    active_operations = _DrainObservedTaskSet()
    record.active_operations = active_operations
    operation = asyncio.create_task(lease.backends[0].discover_metrics([], None))
    first_close: asyncio.Task[None] | None = None
    retry_close: asyncio.Task[None] | None = None
    cancellation_observed = asyncio.Event()
    capacity_after_cancellation: tuple[int, int, int, str, int] | None = None
    release_results: list[Any] = []
    try:
        await _wait_for_event(products[0].operation_started)
        host = cast(Any, dependencies.provider_lifecycle_owner)
        original_invoke = host._invoke_on_generation_owner

        async def observe_release_cancellation(*args: Any, **kwargs: Any) -> Any:
            try:
                return await original_invoke(*args, **kwargs)
            except asyncio.CancelledError:
                cancellation_observed.set()
                raise

        monkeypatch.setattr(host, "_invoke_on_generation_owner", observe_release_cancellation)
        first_close = asyncio.create_task(_close_backend_lease(dependencies, lease))
        await _wait_for_event(active_operations.draining_observed)
        assert record.phase == "draining"

        first_close.cancel()
        await asyncio.wait_for(cancellation_observed.wait(), timeout=1.0)
        capacity_after_cancellation = (
            lifecycle.in_flight,
            lifecycle.retained,
            _active_backend_leases(dependencies),
            record.phase,
            products[0].close_calls,
        )
        retry_close = asyncio.create_task(_close_backend_lease(dependencies, lease))
        operation_gate.release()
        assert await operation == []
        release_results = list(await asyncio.gather(first_close, retry_close, return_exceptions=True))
    finally:
        operation_gate.release()
        if not operation.done():
            operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
        pending_releases = tuple(task for task in (first_close, retry_close) if task is not None and not task.done())
        if pending_releases:
            await asyncio.gather(*pending_releases, return_exceptions=True)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert capacity_after_cancellation == (1, 1, 1, "draining", 0)
    assert isinstance(release_results[0], asyncio.CancelledError)
    assert release_results[1] is None
    assert products[0].close_calls == 1
    assert lifecycle.runtime_fatal_circuit is None


@pytest.mark.asyncio
async def test_concurrent_close_joins_after_backend_authority_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    release_state = lease._release_state
    state_type = type(release_state)
    authority_committed = threading.Event()
    publish_phase = threading.Event()
    second_validated = threading.Event()
    validation_count = 0
    original_mark = state_type.try_mark_resource_released_on_owner
    original_validate = state_type.validate

    def gated_phase_publication(state: Any) -> bool:
        if state is release_state:
            authority_committed.set()
            if not publish_phase.is_set():
                return False
        return original_mark(state)

    def observe_validation(state: Any, candidate: Any) -> None:
        nonlocal validation_count
        original_validate(state, candidate)
        if state is release_state:
            validation_count += 1
            if validation_count == 2:
                second_validated.set()

    monkeypatch.setattr(
        state_type,
        "try_mark_resource_released_on_owner",
        gated_phase_publication,
    )
    monkeypatch.setattr(state_type, "validate", observe_validation)
    first_close = asyncio.create_task(_close_backend_lease(dependencies, lease))
    second_close: asyncio.Task[None] | None = None
    authority_snapshot: tuple[int, bool] | None = None
    results: list[Any] = []
    try:
        await _wait_for_event(authority_committed)
        authority_snapshot = (
            _active_backend_leases(dependencies),
            release_state.is_resource_released(),
        )
        second_close = asyncio.create_task(_close_backend_lease(dependencies, lease))
        await _wait_for_event(second_validated)
        publish_phase.set()
        results = list(await asyncio.gather(first_close, second_close, return_exceptions=True))
    finally:
        publish_phase.set()
        pending = tuple(task for task in (first_close, second_close) if task is not None and not task.done())
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert authority_snapshot == (0, False)
    assert results == [None, None]
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_backend_release_does_not_require_owner_loop_default_executor(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    host = lifecycle.execution_graph.provider_manager()
    assert host is not None
    state = host._generation_owner
    assert state is not None

    async def stop_default_executor() -> None:
        await asyncio.get_running_loop().shutdown_default_executor()

    await host._invoke_on_generation_owner(
        state,
        stop_default_executor,
        track_operation=False,
    )
    await _close_backend_lease(dependencies, lease)
    await _wait_for_idle(dependencies)
    await _stop_root(dependencies, root)

    assert products[0].close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_repeated_backend_realization_under_one_request_obeys_aggregate_record_bound(
    tmp_path: Path,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path, limit=1, queued=0)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    results: list[Any] = []
    active_record_count = 0
    product_count = 0
    request_lease = await lifecycle.acquire()
    try:
        with lifecycle.admitted_lease_context(request_lease):
            for _attempt in range(8):
                try:
                    results.append(await dependencies.realize_backends())
                except BaseException as exc:
                    results.append(exc)
            leases = [item for item in results if not isinstance(item, BaseException)]
            active_record_count = _active_backend_leases(dependencies)
            product_count = len(products)
            await asyncio.gather(
                *(_close_backend_lease(dependencies, _require_backend_lease(lease)) for lease in leases),
                return_exceptions=True,
            )
        with suppress(RuntimeError):
            lifecycle.release(request_lease)
        await _wait_for_idle(dependencies)
    finally:
        with suppress(RuntimeError):
            lifecycle.release(request_lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    unexpected_errors = [
        item for item in results if isinstance(item, BaseException) and not isinstance(item, PipelineAdmissionRejected)
    ]
    assert unexpected_errors == []
    assert active_record_count <= lifecycle.limit
    assert product_count <= lifecycle.limit


@pytest.mark.asyncio
async def test_backend_commands_obey_aggregate_admission_and_handoff_limits(tmp_path: Path) -> None:
    operation_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        limit=1,
        queued=0,
        operation_gate=operation_gate,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    operations = [asyncio.create_task(lease.backends[0].discover_metrics([], None)) for _ in range(4)]
    observed_handoffs = 0
    observed_in_flight = 0
    results: list[Any] = []
    try:
        await _wait_for_event(products[0].operation_started)
        await asyncio.sleep(0.05)
        host = cast(Any, dependencies.provider_lifecycle_owner)
        assert host is not None
        state = host._generation_owner
        assert state is not None
        with host._lock:
            observed_handoffs = len(state.active_handoffs)
        observed_in_flight = lifecycle.in_flight
        operation_gate.release()
        results = list(await asyncio.gather(*operations, return_exceptions=True))
        await _close_backend_lease(dependencies, lease)
        await _wait_for_idle(dependencies)
    finally:
        operation_gate.release()
        for operation in operations:
            if not operation.done():
                operation.cancel()
        await asyncio.gather(*operations, return_exceptions=True)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    unexpected_errors = [
        item for item in results if isinstance(item, BaseException) and not isinstance(item, PipelineAdmissionRejected)
    ]
    assert unexpected_errors == []
    assert products[0].max_active_operations <= lifecycle.limit
    assert observed_handoffs == 1
    assert observed_in_flight <= lifecycle.limit


@pytest.mark.asyncio
async def test_final_generation_cleanup_finishes_backends_before_other_providers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    close_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        close_gate=close_gate,
        cleanup_grace_seconds=1.0,
    )
    root = dependencies.start_runtime_root()
    assert root is not None
    _require_backend_lease(await dependencies.realize_backends())
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    events: list[str] = []
    original_settle = host._settle_generation_cleanup

    async def record_cleanup(label: str, cleanup: Any) -> Any:
        events.append(f"{label}:start")
        if label == "backends":
            result = await original_settle(label, cleanup)
        else:
            result = None
        events.append(f"{label}:end")
        return result

    async def chained_cleanup() -> None:
        return None

    monkeypatch.setattr(host, "_settle_generation_cleanup", record_cleanup)
    with host._lock:
        host._context_provider = object()
        host._context_initialized = True
        host._llm_provider = object()
        host._chained_cleanup = chained_cleanup
        host._cleanup_pending = True

    stop_task = asyncio.create_task(_stop_root(dependencies, root))
    try:
        await _wait_for_event(products[0].close_started)
        await asyncio.sleep(0.05)
        events_before_backend_finished = list(events)
        close_gate.release()
        await asyncio.wait_for(stop_task, timeout=1.0)
    finally:
        close_gate.release()
        if not stop_task.done():
            with suppress(BaseException):
                await asyncio.wait_for(stop_task, timeout=1.0)

    assert events_before_backend_finished == ["backends:start"]
    backend_end = events.index("backends:end")
    for label in ("context", "llm", "chained"):
        assert events.index(f"{label}:start") > backend_end
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_cancelled_composite_release_retries_without_provider_lease_growth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    original_settle = host._settle_generation_cleanup
    active_cleanup_gate: _CrossThreadAsyncGate | None = None
    cleanup_entries = 0
    active_provider_lease_counts: list[int] = []
    cleanup_entries_per_release: list[int] = []
    leases: list[Any] = []

    async def gate_provider_cleanup(label: str, cleanup: Any) -> Any:
        nonlocal cleanup_entries
        if label == "backends" and active_cleanup_gate is not None:
            cleanup_entries += 1
            await active_cleanup_gate.wait()
        return await original_settle(label, cleanup)

    monkeypatch.setattr(host, "_settle_generation_cleanup", gate_provider_cleanup)
    try:
        for _attempt in range(3):
            lease = _require_backend_lease(await dependencies.realize_backends())
            leases.append(lease)
            active_cleanup_gate = _CrossThreadAsyncGate()
            entries_before_release = cleanup_entries
            first_release = asyncio.create_task(_close_backend_lease(dependencies, lease))
            await _wait_for_event(active_cleanup_gate.started)
            first_release.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first_release

            retries = [asyncio.create_task(_close_backend_lease(dependencies, lease)) for _ in range(4)]
            active_cleanup_gate.release()
            retry_results = await asyncio.gather(*retries, return_exceptions=True)
            assert not [item for item in retry_results if isinstance(item, BaseException)]
            cleanup_entries_per_release.append(cleanup_entries - entries_before_release)
            with host._lock:
                active_provider_lease_counts.append(len(host._active_leases))
    finally:
        if active_cleanup_gate is not None:
            active_cleanup_gate.release()
        for lease in leases:
            with suppress(BaseException):
                await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert cleanup_entries_per_release == [1, 1, 1]
    assert active_provider_lease_counts == [0, 0, 0]
    assert [product.close_calls for product in products] == [1, 1, 1]


@pytest.mark.asyncio
async def test_cancel_after_provider_authority_commit_can_join_successful_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        cleanup_grace_seconds=1.0,
    )
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    release_state = lease._release_state
    state_type = type(release_state)
    host = cast(Any, dependencies.provider_lifecycle_owner)
    assert host is not None
    cleanup_gate = _CrossThreadAsyncGate()
    provider_authority_committed = threading.Event()
    retry_release_started = threading.Event()
    validation_count = 0
    original_settle = host._settle_generation_cleanup
    original_validate = state_type.validate

    async def gate_cleanup(label: str, cleanup: Any) -> Any:
        if label == "backends" and not provider_authority_committed.is_set():
            with host._lock:
                assert lease._provider_lease.lease_id not in host._active_leases
            provider_authority_committed.set()
            await cleanup_gate.wait()
        return await original_settle(label, cleanup)

    def observe_validation(state: Any, candidate: Any) -> None:
        nonlocal validation_count
        original_validate(state, candidate)
        if state is release_state:
            validation_count += 1
            if validation_count == 2:
                retry_release_started.set()

    monkeypatch.setattr(host, "_settle_generation_cleanup", gate_cleanup)
    monkeypatch.setattr(state_type, "validate", observe_validation)
    first_close = asyncio.create_task(_close_backend_lease(dependencies, lease))
    retry_close: asyncio.Task[None] | None = None
    first_result: Any = None
    retry_result: Any = None
    provider_lease_count_after_commit = -1
    try:
        await _wait_for_event(provider_authority_committed)
        with host._lock:
            provider_lease_count_after_commit = len(host._active_leases)

        first_close.cancel()
        first_result = (await asyncio.gather(first_close, return_exceptions=True))[0]
        retry_close = asyncio.create_task(_close_backend_lease(dependencies, lease))
        await _wait_for_event(retry_release_started)
        cleanup_gate.release()
        retry_result = (await asyncio.gather(retry_close, return_exceptions=True))[0]
    finally:
        cleanup_gate.release()
        pending = tuple(task for task in (first_close, retry_close) if task is not None and not task.done())
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert provider_lease_count_after_commit == 0
    assert isinstance(first_result, asyncio.CancelledError)
    assert retry_result is None
    assert release_state.is_provider_released()
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_cancelled_provider_cleanup_joins_backend_composite_release(
    tmp_path: Path,
) -> None:
    operation_gate = _CrossThreadAsyncGate()
    dependencies, products, _factory_threads = _dependencies(
        tmp_path,
        operation_gate=operation_gate,
        cleanup_grace_seconds=1.0,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    provider_lease = await dependencies.acquire_resources()
    assert provider_lease is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    assert lease._provider_lease == provider_lease
    assert lease._release_provider_lease is False
    owner = _backend_owner(dependencies)
    record = owner._records[lease.lease_id]
    active_operations = _DrainObservedTaskSet()
    record.active_operations = active_operations
    operation = asyncio.create_task(lease.backends[0].discover_metrics([], None))
    provider_close: asyncio.Task[None] | None = None
    direct_retry: asyncio.Task[None] | None = None
    capacity_after_cancellation: tuple[int, int, int, str, int, bool] | None = None
    results: list[Any] = []
    try:
        await _wait_for_event(products[0].operation_started)
        provider_close = asyncio.create_task(dependencies.close_resources(provider_lease))
        await _wait_for_event(active_operations.draining_observed)
        provider_close.cancel()
        provider_result = (await asyncio.gather(provider_close, return_exceptions=True))[0]
        assert isinstance(provider_result, asyncio.CancelledError)

        capacity_after_cancellation = (
            lifecycle.in_flight,
            lifecycle.retained,
            _active_backend_leases(dependencies),
            record.phase,
            products[0].close_calls,
            operation.done(),
        )
        direct_retry = asyncio.create_task(dependencies.close_resources(provider_lease))
        operation_gate.release()
        results = list(await asyncio.gather(operation, direct_retry, return_exceptions=True))
        await _close_backend_lease(dependencies, lease)
        await _wait_for_idle(dependencies)
    finally:
        operation_gate.release()
        pending = tuple(
            task for task in (operation, provider_close, direct_retry) if task is not None and not task.done()
        )
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert capacity_after_cancellation == (1, 1, 1, "draining", 0, False)
    assert results == [[], None]
    assert products[0].close_calls == 1
    assert lifecycle.runtime_fatal_circuit is None


@pytest.mark.asyncio
async def test_provider_and_direct_backend_close_race_has_one_release_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    provider_lease = await dependencies.acquire_resources()
    assert provider_lease is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    assert lease._release_provider_lease is False
    release_state = lease._release_state
    host = cast(Any, dependencies.provider_lifecycle_owner)
    owner = _backend_owner(dependencies)
    record = owner._records[lease.lease_id]
    owner_thread = record.generation.owner_thread
    assert host is not None
    assert owner_thread is not None

    direct_lookup_entered = threading.Event()
    allow_direct_lookup = threading.Event()
    owner_release_entered = threading.Event()
    direct_done = threading.Event()
    direct_errors: list[BaseException] = []
    release_state_type = type(release_state)
    original_published_release = release_state_type.published_composite_release
    original_try_begin = release_state_type.try_begin_composite_release_on_owner

    def gate_direct_release_lookup(state: Any) -> Any:
        if (
            state is release_state
            and threading.current_thread().name == "backend-direct-release-racer"
            and not direct_lookup_entered.is_set()
        ):
            direct_lookup_entered.set()
            assert allow_direct_lookup.wait(timeout=2.0)
        return original_published_release(state)

    def observe_owner_release(state: Any) -> Any:
        result = original_try_begin(state)
        if state is release_state and threading.current_thread() is owner_thread:
            owner_release_entered.set()
        return result

    monkeypatch.setattr(
        release_state_type,
        "published_composite_release",
        gate_direct_release_lookup,
    )
    monkeypatch.setattr(
        release_state_type,
        "try_begin_composite_release_on_owner",
        observe_owner_release,
    )

    def run_direct_close() -> None:
        try:
            asyncio.run(_close_backend_lease(dependencies, lease))
        except BaseException as exc:
            direct_errors.append(exc)
        finally:
            direct_done.set()

    direct_thread = threading.Thread(
        target=run_direct_close,
        name="backend-direct-release-racer",
    )
    provider_close: asyncio.Task[None] | None = None
    direct_thread.start()
    try:
        await _wait_for_event(direct_lookup_entered)
        provider_close = asyncio.create_task(dependencies.close_resources(provider_lease))
        await _wait_for_event(owner_release_entered)
        allow_direct_lookup.set()
        assert await asyncio.to_thread(direct_done.wait, 1.0)
        await asyncio.wait_for(provider_close, timeout=1.0)
    finally:
        allow_direct_lookup.set()
        if provider_close is not None and not provider_close.done():
            provider_close.cancel()
            await asyncio.gather(provider_close, return_exceptions=True)
        if direct_thread.is_alive():
            release = release_state.composite_release
            if release is not None:
                release_state.complete_composite_release(release)
            direct_thread.join(timeout=1.0)
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        with suppress(BaseException):
            await dependencies.close_resources(provider_lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert direct_thread.is_alive() is False
    assert direct_errors == []
    assert products[0].close_calls == 1


@pytest.mark.asyncio
async def test_transient_backend_close_retries_without_fencing_or_leaking(
    tmp_path: Path,
) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path, fail_close_attempts=1)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    try:
        with capture_logs() as logs:
            await _close_backend_lease(dependencies, lease)
        assert products[0].close_calls == 2
        assert lifecycle.runtime_fatal_circuit is None
        assert any(
            record.get("event") == "backend_owner_cleanup_retry"
            and record.get("reason_code") == "backend_owner_cleanup_retry"
            for record in logs
        )
        await _wait_for_idle(dependencies)
    finally:
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)


def test_sync_backend_factory_rejection_has_an_explicit_deprecation_contract(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)

    with pytest.warns(DeprecationWarning, match="realize_backends"):
        with pytest.raises(RuntimeOwnershipError, match="realize_backends"):
            dependencies.backend_factory()

    assert products == []


@pytest.mark.asyncio
async def test_backend_lease_is_explicit_and_not_a_legacy_sequence_adapter(tmp_path: Path) -> None:
    dependencies, products, _factory_threads = _dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    assert root is not None
    lease = _require_backend_lease(await dependencies.realize_backends())
    try:
        assert not hasattr(lease, "__iter__")
        assert not hasattr(lease, "__getitem__")
        with pytest.raises(TypeError):
            iter(lease)
        assert tuple(lease.backends)
        await _close_backend_lease(dependencies, lease)
        await _wait_for_idle(dependencies)
    finally:
        with suppress(BaseException):
            await _close_backend_lease(dependencies, lease)
        await _stop_root(dependencies, root, tolerate_fatal=True)

    assert products[0].close_calls == 1
