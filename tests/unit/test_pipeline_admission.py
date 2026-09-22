from __future__ import annotations

import asyncio
import contextlib
import gc
import threading
import weakref
from dataclasses import replace
from typing import Any, cast
from unittest import mock

import pytest

from tacit.config import Settings
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController, pipeline_admission_limits


class _ShortClaimAdmissionController(PipelineAdmissionController):
    _selected_claim_lease_seconds = 0.05
    _selected_maintenance_idle_seconds = 0.05


class _CountingAdmissionController(PipelineAdmissionController):
    _selected_maintenance_idle_seconds = 0.05

    def __init__(
        self,
        limit: int,
        *,
        max_queued: int = 100,
        max_queued_per_partition: int | None = None,
        max_in_flight_per_partition: int | None = None,
    ) -> None:
        super().__init__(
            limit,
            max_queued=max_queued,
            max_queued_per_partition=max_queued_per_partition,
            max_in_flight_per_partition=max_in_flight_per_partition,
        )
        self.capacity_checks = 0
        self.selected_maintenance_checks = 0
        self.max_selected_maintenance_batch = 0

    def _can_activate_locked(self, partition: str) -> bool:
        self.capacity_checks += 1
        return super()._can_activate_locked(partition)

    def _maintain_selected_locked(self, now: float) -> int:
        checks = super()._maintain_selected_locked(now)
        self.selected_maintenance_checks += checks
        self.max_selected_maintenance_batch = max(self.max_selected_maintenance_batch, checks)
        return checks


async def _wait_for_queue(controller: PipelineAdmissionController, expected: int) -> None:
    for _ in range(100):
        if controller.queued == expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {expected} queued pipeline runs, found {controller.queued}")


async def _wait_for_admission_maintenance_exit(controller: PipelineAdmissionController) -> None:
    maintenance_name = f"tacit-pipeline-admission-maintenance-{id(controller)}"
    for _ in range(100):
        if not any(thread.name == maintenance_name and thread.is_alive() for thread in threading.enumerate()):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"admission maintenance thread {maintenance_name!r} did not exit")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pipeline_max_concurrent_per_tenant", 1_001),
        ("pipeline_max_queued", 1_001),
        ("pipeline_max_queued_per_tenant", 1_001),
    ],
)
def test_pipeline_queue_configuration_has_a_supported_upper_bound(field: str, value: int) -> None:
    with pytest.raises(ValueError):
        Settings(_env_file=None, **{field: value})


def test_blocking_bedrock_bridge_has_a_conservative_runtime_limit() -> None:
    accepted = Settings(
        _env_file=None,
        llm_provider="bedrock",
        pipeline_max_concurrent=32,
    )
    assert accepted.pipeline_max_concurrent == 32

    with pytest.raises(ValueError, match="Bedrock compatibility bridge"):
        Settings(
            _env_file=None,
            llm_provider="bedrock",
            pipeline_max_concurrent=33,
        )

    unbridged = Settings(
        _env_file=None,
        llm_provider="openai",
        pipeline_max_concurrent=33,
    )
    assert unbridged.pipeline_max_concurrent == 33

    unvalidated_copy = accepted.model_copy(update={"pipeline_max_concurrent": 33})
    with pytest.raises(ValueError, match="Bedrock compatibility bridge"):
        pipeline_admission_limits(unvalidated_copy)


def test_wildcard_queue_reserves_capacity_for_another_tenant() -> None:
    with pytest.raises(ValueError, match="must be lower"):
        Settings(
            _env_file=None,
            api_auth_enabled=True,
            knowledge_tenant_id="*",
            knowledge_tenant_api_keys={"tenant-a": "secret-a", "tenant-b": "secret-b"},
            pipeline_max_queued=25,
            pipeline_max_queued_per_tenant=25,
        )


def test_wildcard_runtime_reserves_active_capacity_when_multiple_slots_exist() -> None:
    single_slot = Settings(
        _env_file=None,
        api_auth_enabled=True,
        knowledge_tenant_id="*",
        knowledge_tenant_api_keys={"tenant-a": "secret-a", "tenant-b": "secret-b"},
        pipeline_max_concurrent=1,
    )
    assert single_slot.pipeline_max_concurrent_per_tenant == 0

    with pytest.raises(ValueError, match="must be lower than pipeline_max_concurrent"):
        Settings(
            _env_file=None,
            api_auth_enabled=True,
            knowledge_tenant_id="*",
            knowledge_tenant_api_keys={"tenant-a": "secret-a", "tenant-b": "secret-b"},
            pipeline_max_concurrent=4,
            pipeline_max_concurrent_per_tenant=4,
        )


async def test_cancelled_waiter_does_not_leak_an_admission_slot() -> None:
    controller = PipelineAdmissionController(1)
    active = await controller.acquire()
    waiter = asyncio.create_task(controller.acquire())
    await _wait_for_queue(controller, 1)

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    controller.release(active)

    assert controller.in_flight == 0
    assert controller.queued == 0
    final = await controller.acquire()
    controller.release(final)


async def test_runtime_admission_can_transfer_a_slot_between_event_loops() -> None:
    controller = PipelineAdmissionController(1)
    active = await controller.acquire()

    async def acquire_in_worker_loop() -> None:
        async with controller.slot():
            assert controller.in_flight == 1

    worker = asyncio.create_task(asyncio.to_thread(asyncio.run, acquire_in_worker_loop()))
    await _wait_for_queue(controller, 1)
    controller.release(active)
    await asyncio.wait_for(worker, timeout=1)

    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_runtime_admission_rejects_work_beyond_the_bounded_queue() -> None:
    controller = PipelineAdmissionController(1, max_queued=1)
    active = await controller.acquire()
    queued = asyncio.create_task(controller.acquire())
    await _wait_for_queue(controller, 1)

    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await controller.acquire()

    assert exc_info.value.reason_code == "pipeline_admission_queue_full"
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    controller.release(active)


async def test_runtime_admission_wait_obeys_the_pipeline_deadline() -> None:
    controller = PipelineAdmissionController(1, max_queued=1)
    active = await controller.acquire()

    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await controller.acquire(timeout_seconds=0.01)

    assert exc_info.value.reason_code == "pipeline_admission_wait_timeout"
    assert controller.queued == 0
    controller.release(active)


async def test_closed_waiter_loop_cannot_consume_an_admission_slot() -> None:
    controller = PipelineAdmissionController(1, max_queued=2)
    active = await controller.acquire()

    def abandon_waiter_loop() -> None:
        loop = asyncio.new_event_loop()

        async def enqueue() -> None:
            task = asyncio.create_task(controller.acquire(timeout_seconds=10))
            while controller.queued != 1:
                await asyncio.sleep(0)
            task._log_destroy_pending = False

        loop.run_until_complete(enqueue())
        loop.close()

    await asyncio.to_thread(abandon_waiter_loop)
    controller.release(active)

    lease = await asyncio.wait_for(controller.acquire(), timeout=1)
    assert lease.queued is False
    controller.release(lease)
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_stopped_open_waiter_loop_cannot_block_an_idle_slot() -> None:
    controller = PipelineAdmissionController(1, max_queued=2)
    active = await controller.acquire()
    worker_loop = asyncio.new_event_loop()
    worker_task: asyncio.Task[object] | None = None

    def stop_with_waiter() -> None:
        nonlocal worker_task

        async def enqueue() -> None:
            nonlocal worker_task
            worker_task = asyncio.create_task(controller.acquire(timeout_seconds=10))
            await _wait_for_queue(controller, 1)

        worker_loop.run_until_complete(enqueue())

    await asyncio.to_thread(stop_with_waiter)
    assert worker_loop.is_running() is False
    assert worker_loop.is_closed() is False
    controller.release(active)

    lease = await asyncio.wait_for(controller.acquire(timeout_seconds=0.5), timeout=1)
    assert lease.queued is False
    controller.release(lease)

    assert worker_task is not None
    worker_loop.call_soon_threadsafe(worker_task.cancel)

    def close_worker_loop() -> None:
        with contextlib.suppress(asyncio.CancelledError, PipelineAdmissionRejected):
            worker_loop.run_until_complete(worker_task)
        worker_loop.close()

    await asyncio.to_thread(close_worker_loop)
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_selected_waiter_claim_expires_when_loop_stops_before_accepted_wake_runs() -> None:
    controller = _ShortClaimAdmissionController(1, max_queued=2)
    active = await controller.acquire()
    worker_loop = asyncio.new_event_loop()
    worker_task: asyncio.Task[object] | None = None
    worker_ready = threading.Event()
    stop_callback_started = threading.Event()
    allow_stop = threading.Event()
    worker_stopped = threading.Event()

    def run_worker_loop() -> None:
        nonlocal worker_task
        asyncio.set_event_loop(worker_loop)

        async def enqueue() -> None:
            nonlocal worker_task
            worker_task = asyncio.create_task(controller.acquire(timeout_seconds=5))
            await _wait_for_queue(controller, 1)

        worker_loop.run_until_complete(enqueue())
        worker_ready.set()
        worker_loop.run_forever()
        worker_stopped.set()

    worker_thread = threading.Thread(
        target=run_worker_loop,
        name="tacit-admission-stopped-loop-test",
    )
    worker_thread.start()
    live_waiter: asyncio.Task[object] | None = None
    try:
        assert await asyncio.to_thread(worker_ready.wait, 1) is True

        def stop_before_next_iteration() -> None:
            stop_callback_started.set()
            allow_stop.wait()
            worker_loop.stop()

        worker_loop.call_soon_threadsafe(stop_before_next_iteration)
        assert await asyncio.to_thread(stop_callback_started.wait, 1) is True

        live_waiter = asyncio.create_task(controller.acquire(timeout_seconds=1))
        await _wait_for_queue(controller, 2)
        assert worker_loop.is_running() is True
        with mock.patch.object(
            worker_loop,
            "call_soon_threadsafe",
            wraps=worker_loop.call_soon_threadsafe,
        ) as accepted_wake:
            controller.release(active)
            assert accepted_wake.call_count == 1
        allow_stop.set()
        assert await asyncio.to_thread(worker_stopped.wait, 1) is True

        lease = await asyncio.wait_for(live_waiter, timeout=0.5)
        controller.release(lease)
    finally:
        allow_stop.set()
        worker_thread.join(1)
        assert worker_thread.is_alive() is False
        if live_waiter is not None and not live_waiter.done():
            live_waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await live_waiter

        assert worker_task is not None

        def drain_and_close_worker_loop() -> None:
            with contextlib.suppress(asyncio.CancelledError, PipelineAdmissionRejected):
                worker_loop.run_until_complete(worker_task)
            assert not [task for task in asyncio.all_tasks(worker_loop) if not task.done()]
            worker_loop.close()

        await asyncio.to_thread(drain_and_close_worker_loop)

    await _wait_for_admission_maintenance_exit(controller)
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_maintenance_start_failure_reclaims_stopped_loop_selection_and_preserves_fairness() -> None:
    controller = _ShortClaimAdmissionController(1, max_queued=3)
    active = await controller.acquire()
    worker_loop = asyncio.new_event_loop()
    worker_task: asyncio.Task[object] | None = None
    worker_ready = threading.Event()
    stop_callback_started = threading.Event()
    allow_stop = threading.Event()
    worker_stopped = threading.Event()

    def run_worker_loop() -> None:
        nonlocal worker_task
        asyncio.set_event_loop(worker_loop)

        async def enqueue() -> None:
            nonlocal worker_task
            worker_task = asyncio.create_task(
                controller.acquire(
                    timeout_seconds=5,
                    partition_key="tenant-stopped",
                )
            )
            await _wait_for_queue(controller, 1)

        worker_loop.run_until_complete(enqueue())
        worker_ready.set()
        worker_loop.run_forever()
        worker_stopped.set()

    worker_thread = threading.Thread(
        target=run_worker_loop,
        name="tacit-admission-maintenance-start-failure-test",
    )
    worker_thread.start()
    admitted: list[str] = []

    async def live_waiter(partition: str) -> None:
        lease = await controller.acquire(
            timeout_seconds=1,
            partition_key=partition,
        )
        admitted.append(partition)
        await asyncio.sleep(0)
        controller.release(lease)

    live_waiters: list[asyncio.Task[None]] = []
    try:
        assert await asyncio.to_thread(worker_ready.wait, 1) is True

        def stop_before_next_iteration() -> None:
            stop_callback_started.set()
            allow_stop.wait()
            worker_loop.stop()

        worker_loop.call_soon_threadsafe(stop_before_next_iteration)
        assert await asyncio.to_thread(stop_callback_started.wait, 1) is True

        live_waiters = [
            asyncio.create_task(live_waiter("tenant-b")),
            asyncio.create_task(live_waiter("tenant-c")),
        ]
        await _wait_for_queue(controller, 3)

        maintenance_name = f"tacit-pipeline-admission-maintenance-{id(controller)}"
        real_start = threading.Thread.start
        maintenance_start_attempts = 0

        def fail_first_maintenance_start(thread: threading.Thread) -> None:
            nonlocal maintenance_start_attempts
            if thread.name == maintenance_name:
                maintenance_start_attempts += 1
                if maintenance_start_attempts == 1:
                    raise RuntimeError("injected maintenance start failure")
            real_start(thread)

        with mock.patch.object(threading.Thread, "start", new=fail_first_maintenance_start):
            controller.release(active)
            allow_stop.set()
            assert await asyncio.to_thread(worker_stopped.wait, 1) is True
            await asyncio.wait_for(asyncio.gather(*live_waiters), timeout=1)
            assert maintenance_start_attempts == 2
    finally:
        allow_stop.set()
        worker_thread.join(1)
        assert worker_thread.is_alive() is False
        for waiter in live_waiters:
            if not waiter.done():
                waiter.cancel()
        if live_waiters:
            await asyncio.gather(*live_waiters, return_exceptions=True)

        assert worker_task is not None

        def drain_and_close_worker_loop() -> None:
            with contextlib.suppress(asyncio.CancelledError, PipelineAdmissionRejected):
                worker_loop.run_until_complete(worker_task)
            assert not [task for task in asyncio.all_tasks(worker_loop) if not task.done()]
            worker_loop.close()

        await asyncio.to_thread(drain_and_close_worker_loop)

    await _wait_for_admission_maintenance_exit(controller)
    assert admitted == ["tenant-b", "tenant-c"]
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_repeated_maintenance_start_failure_rejects_waiters_without_retry_loop() -> None:
    controller = _ShortClaimAdmissionController(1, max_queued=2)
    active = await controller.acquire()

    async def queued_waiter(partition: str) -> PipelineAdmissionRejected | None:
        try:
            lease = await controller.acquire(
                timeout_seconds=5,
                partition_key=partition,
            )
        except PipelineAdmissionRejected as exc:
            return exc
        controller.release(lease)
        return None

    waiters = [
        asyncio.create_task(queued_waiter("tenant-a")),
        asyncio.create_task(queued_waiter("tenant-b")),
    ]
    await _wait_for_queue(controller, 2)
    maintenance_name = f"tacit-pipeline-admission-maintenance-{id(controller)}"
    real_start = threading.Thread.start
    maintenance_start_attempts = 0

    def fail_first_two_maintenance_starts(thread: threading.Thread) -> None:
        nonlocal maintenance_start_attempts
        if thread.name == maintenance_name:
            maintenance_start_attempts += 1
            if maintenance_start_attempts <= 2:
                raise RuntimeError("injected maintenance start failure")
        real_start(thread)

    with mock.patch.object(threading.Thread, "start", new=fail_first_two_maintenance_starts):
        controller.release(active)
        results = await asyncio.wait_for(asyncio.gather(*waiters), timeout=1)

    assert maintenance_start_attempts == 2
    assert all(isinstance(result, PipelineAdmissionRejected) for result in results)
    assert all(result.reason_code == "pipeline_admission_queue_full" for result in results if result is not None)
    with controller._lock:
        assert controller._selected_maintenance_thread is None
        assert not controller._selected
        assert not controller._selected_claims
        assert not controller._selected_checks
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_selected_waiter_cancellation_cleans_claim_maintenance() -> None:
    controller = _ShortClaimAdmissionController(1, max_queued=1)
    active = await controller.acquire()
    waiter = asyncio.create_task(controller.acquire(timeout_seconds=1))
    await _wait_for_queue(controller, 1)

    controller.release(active)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    await _wait_for_admission_maintenance_exit(controller)
    assert waiter.done()
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_idle_admission_queue_does_not_start_maintenance_worker() -> None:
    controller = _ShortClaimAdmissionController(1, max_queued=1)
    active = await controller.acquire()
    waiter = asyncio.create_task(controller.acquire(timeout_seconds=1))
    await _wait_for_queue(controller, 1)
    maintenance_name = f"tacit-pipeline-admission-maintenance-{id(controller)}"

    await asyncio.sleep(0)
    assert not any(thread.name == maintenance_name and thread.is_alive() for thread in threading.enumerate())

    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    controller.release(active)
    assert controller.in_flight == 0
    assert controller.queued == 0


@pytest.mark.parametrize("terminal_transition", ["cancelled", "expired", "rejected"])
async def test_last_queued_path_fires_idle_callback_once(terminal_transition: str) -> None:
    controller = PipelineAdmissionController(1, max_queued=1)
    service_owner = controller.try_acquire_service_owner()
    assert service_owner is not None
    with controller.service_owner(service_owner):
        service_lease = await controller.acquire()
    timeout_seconds = 0.01 if terminal_transition == "expired" else 1.0
    queued = asyncio.create_task(controller.acquire(timeout_seconds=timeout_seconds))
    await _wait_for_queue(controller, 1)
    callbacks: list[str] = []
    controller.when_request_paths_idle(lambda: callbacks.append(terminal_transition))

    if terminal_transition == "cancelled":
        queued.cancel()
        with pytest.raises(asyncio.CancelledError):
            await queued
    elif terminal_transition == "expired":
        with pytest.raises(PipelineAdmissionRejected) as exc_info:
            await queued
        assert exc_info.value.reason_code == "pipeline_admission_wait_timeout"
    else:
        controller.fence_runtime_fatal(RuntimeError("injected cleanup failure"))
        with pytest.raises(RuntimeOwnershipError, match="Pipeline runtime cleanup failed"):
            await queued

    assert callbacks == [terminal_transition]
    controller.release(service_lease)
    controller.release_service_owner(service_owner)
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_partitioned_queue_reserves_capacity_and_wakes_tenants_round_robin() -> None:
    controller = PipelineAdmissionController(
        1,
        max_queued=4,
        max_queued_per_partition=2,
    )
    active = await controller.acquire(partition_key="tenant-running")
    admitted: list[str] = []

    async def queued_work(tenant: str) -> None:
        async with controller.slot(partition_key=tenant, timeout_seconds=1):
            admitted.append(tenant)
            await asyncio.sleep(0)

    tasks = [
        asyncio.create_task(queued_work("tenant-a")),
        asyncio.create_task(queued_work("tenant-a")),
        asyncio.create_task(queued_work("tenant-b")),
        asyncio.create_task(queued_work("tenant-b")),
    ]
    await _wait_for_queue(controller, 4)
    controller.release(active)
    await asyncio.gather(*tasks)

    assert admitted == ["tenant-a", "tenant-b", "tenant-a", "tenant-b"]
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_partition_queue_cap_prevents_one_tenant_from_filling_the_global_queue() -> None:
    controller = PipelineAdmissionController(
        1,
        max_queued=3,
        max_queued_per_partition=2,
    )
    active = await controller.acquire(partition_key="tenant-running")
    tenant_a = [
        asyncio.create_task(controller.acquire(partition_key="tenant-a")),
        asyncio.create_task(controller.acquire(partition_key="tenant-a")),
    ]
    await _wait_for_queue(controller, 2)

    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await controller.acquire(partition_key="tenant-a")
    assert exc_info.value.reason_code == "pipeline_admission_queue_full"

    tenant_b = asyncio.create_task(controller.acquire(partition_key="tenant-b"))
    await _wait_for_queue(controller, 3)
    for task in (*tenant_a, tenant_b):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    controller.release(active)


async def test_admission_queue_work_remains_bounded_at_the_supported_limit() -> None:
    queue_limit = 1_000
    controller = PipelineAdmissionController(1, max_queued=queue_limit)
    active = await controller.acquire()
    tasks = [asyncio.create_task(controller.acquire(timeout_seconds=5)) for _ in range(queue_limit)]
    await _wait_for_queue(controller, queue_limit)

    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await controller.acquire()
    assert exc_info.value.reason_code == "pipeline_admission_queue_full"

    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    controller.release(active)


async def test_active_partition_cap_reserves_a_runtime_slot_for_another_tenant() -> None:
    controller = PipelineAdmissionController(
        4,
        max_queued=4,
        max_queued_per_partition=4,
        max_in_flight_per_partition=3,
    )
    tenant_a = [await controller.acquire(partition_key="tenant-a") for _ in range(3)]
    blocked_a = asyncio.create_task(controller.acquire(partition_key="tenant-a", timeout_seconds=1))
    await _wait_for_queue(controller, 1)

    tenant_b = await asyncio.wait_for(
        controller.acquire(partition_key="tenant-b", timeout_seconds=1),
        timeout=1,
    )

    assert tenant_b.wait_seconds < 0.1
    assert controller.in_flight_for("tenant-a") == 3
    assert controller.in_flight_for("tenant-b") == 1

    controller.release(tenant_a.pop())
    admitted_a = await asyncio.wait_for(blocked_a, timeout=1)
    controller.release(admitted_a)
    controller.release(tenant_b)
    for lease in tenant_a:
        controller.release(lease)
    assert controller.in_flight == 0


async def test_eligible_tenant_uses_spare_capacity_when_blocked_queues_are_full() -> None:
    controller = PipelineAdmissionController(
        5,
        max_queued=2,
        max_queued_per_partition=1,
        max_in_flight_per_partition=2,
    )
    tenant_a = [await controller.acquire(partition_key="tenant-a") for _ in range(2)]
    tenant_b = [await controller.acquire(partition_key="tenant-b") for _ in range(2)]
    blocked_a = asyncio.create_task(controller.acquire(partition_key="tenant-a", timeout_seconds=1))
    blocked_b = asyncio.create_task(controller.acquire(partition_key="tenant-b", timeout_seconds=1))
    await _wait_for_queue(controller, 2)

    tenant_c = await asyncio.wait_for(
        controller.acquire(partition_key="tenant-c", timeout_seconds=1),
        timeout=1,
    )

    assert tenant_c.queued is False
    assert controller.in_flight == 5
    assert controller.queued == 2

    controller.release(tenant_c)
    controller.release(tenant_a.pop())
    admitted_a = await asyncio.wait_for(blocked_a, timeout=1)
    controller.release(admitted_a)
    controller.release(tenant_b.pop())
    admitted_b = await asyncio.wait_for(blocked_b, timeout=1)
    controller.release(admitted_b)
    for lease in (*tenant_a, *tenant_b):
        controller.release(lease)
    assert controller.in_flight == 0
    assert controller.queued == 0


async def test_admission_lease_cannot_be_released_twice_or_as_another_partition() -> None:
    controller = PipelineAdmissionController(2, max_in_flight_per_partition=1)
    tenant_a = await controller.acquire(partition_key="tenant-a")
    tenant_b = await controller.acquire(partition_key="tenant-b")

    with pytest.raises(RuntimeError, match="partition was corrupted"):
        controller.release(replace(tenant_a, partition="tenant-b"))
    with pytest.raises(RuntimeError, match="partition was corrupted"):
        controller.release(replace(tenant_a, token=tenant_b.token))

    assert controller.in_flight == 2
    assert controller.in_flight_for("tenant-a") == 1
    assert controller.in_flight_for("tenant-b") == 1
    controller.release(tenant_a)
    controller.release(tenant_b)

    with pytest.raises(RuntimeError, match="not active"):
        controller.release(tenant_a)


async def test_admission_lease_is_bound_to_its_controller() -> None:
    controller_a = PipelineAdmissionController(1)
    controller_b = PipelineAdmissionController(1)
    lease_a = await controller_a.acquire(partition_key="tenant-a")
    lease_b = await controller_b.acquire(partition_key="tenant-a")

    assert lease_a.token == lease_b.token
    assert lease_a.partition == lease_b.partition
    with pytest.raises(RuntimeError, match="controller"):
        controller_b.release(lease_a)

    assert controller_a.in_flight == 1
    assert controller_b.in_flight == 1
    controller_b.release(lease_b)
    controller_a.release(lease_a)
    assert controller_a.in_flight == 0
    assert controller_b.in_flight == 0


async def test_retained_work_keeps_its_effective_slot_and_fences_late_side_effects() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()

    async with controller.slot() as lease:

        async def resistant_work() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_started.set()
                await release_cleanup.wait()
                raise

        task = asyncio.create_task(resistant_work())
        await asyncio.sleep(0)
        lease.fence.close("pipeline_timeout")
        task.cancel()
        await cleanup_started.wait()
        controller.retain_task(lease, task)

    assert controller.in_flight == 1
    assert controller.retained == 1
    with pytest.raises(RuntimeError, match="fenced"):
        lease.fence.ensure_side_effects_allowed()
    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await controller.acquire()
    assert exc_info.value.reason_code == "pipeline_admission_queue_full"

    release_cleanup.set()
    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert controller.in_flight == 0
    assert controller.retained == 0


async def test_retained_work_is_strongly_owned_until_its_completion_callback_runs() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    cleanup_started = asyncio.Event()

    lease = await controller.acquire()

    async def resistant_work() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(resistant_work())
    await asyncio.sleep(0)
    task.cancel()
    await cleanup_started.wait()
    assert controller.retain_task(lease, task) is True
    controller.release(lease)

    task_ref = weakref.ref(task)
    del task
    gc.collect()
    await asyncio.sleep(0)

    retained_task = task_ref()
    assert retained_task is not None
    assert controller.in_flight == 1
    assert controller.retained == 1

    retained_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await retained_task
    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert controller.in_flight == 0
    assert controller.retained == 0


@pytest.mark.parametrize("first_completion", ["task", "permit"])
async def test_mixed_retained_work_releases_only_after_every_owner_completes(
    first_completion: str,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    allow_task = asyncio.Event()

    async def retained_work() -> None:
        await allow_task.wait()

    task: asyncio.Task[None] | None = None
    permit = None
    try:
        async with controller.slot() as lease:
            task = asyncio.create_task(retained_work())
            assert controller.retain_task(lease, task) is True
            permit = (await controller.acquire_cleanup_permits(1))[0]

        assert controller.in_flight == 1
        assert controller.retained == 1

        if first_completion == "task":
            allow_task.set()
            await task
            await asyncio.sleep(0)
        else:
            assert permit is not None
            controller.release_blocking_permit(permit)

        assert controller.in_flight == 1
        assert controller.retained == 1
        with pytest.raises(PipelineAdmissionRejected) as exc_info:
            await controller.acquire()
        assert exc_info.value.reason_code == "pipeline_admission_queue_full"
    finally:
        allow_task.set()
        if task is not None:
            await task
        if permit is not None and first_completion != "permit":
            controller.release_blocking_permit(permit)

    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0.01)
    assert controller.in_flight == 0
    assert controller.retained == 0

    lease = await controller.acquire()
    assert controller.in_flight == 1
    controller.release(lease)
    assert controller.in_flight == 0


async def test_cleanup_permit_group_is_atomic_and_bounded_per_lease() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)

    async with controller.slot():
        permits = await controller.acquire_cleanup_permits(2)
        assert len(permits) == 2
        assert controller.try_acquire_cleanup_permits(2) is None
        third = controller.try_acquire_cleanup_permits(1)
        assert third is not None
        assert controller.blocking_in_flight == 3
        assert controller.try_acquire_cleanup_permits(1) is None
        for permit in (*permits, *third):
            controller.release_blocking_permit(permit)

    assert controller.in_flight == 0
    assert controller.blocking_in_flight == 0


@pytest.mark.parametrize(
    "invalid_count",
    [True, False, 1.0, -1.0, float("nan"), float("inf"), "1", object()],
    ids=["true", "false", "float", "negative-float", "nan", "inf", "string", "object"],
)
@pytest.mark.parametrize("acquisition", ["async", "immediate"])
async def test_cleanup_permit_count_rejects_non_integers_before_lease_activation(
    invalid_count: object,
    acquisition: str,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    count = cast(Any, invalid_count)

    with pytest.raises(ValueError, match="integer"):
        if acquisition == "async":
            await controller.acquire_cleanup_permits(count)
        else:
            controller.try_acquire_cleanup_permits(count)

    assert controller.in_flight == 0
    assert controller.blocking_in_flight == 0
    assert controller.retained == 0

    lease = await controller.acquire()
    controller.release(lease)
    assert controller.in_flight == 0


async def test_foreign_controller_cannot_release_blocking_permit() -> None:
    owner = PipelineAdmissionController(1, max_queued=0)
    foreign = PipelineAdmissionController(1, max_queued=0)
    permit = (await owner.acquire_cleanup_permits(1))[0]

    with pytest.raises(RuntimeError, match="another controller"):
        foreign.release_blocking_permit(permit)
    assert owner.in_flight == 1
    assert owner.blocking_in_flight == 1

    owner.release_blocking_permit(permit)
    assert owner.in_flight == 0


def test_service_owner_capability_is_single_bounded_and_exactly_releasable() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)

    permit = controller.try_acquire_service_owner()
    assert permit is not None
    assert controller.service_owner_in_flight == 1
    assert controller.blocking_in_flight == 0
    assert controller.in_flight == 0
    assert controller.try_acquire_service_owner() is None

    with controller.service_owner(permit):
        assert controller.current_thread_owns_service_owner() is True
        assert controller.current_thread_can_reuse_blocking_capacity(cleanup=False) is False
        assert controller.current_thread_can_reuse_blocking_capacity(cleanup=True) is False

    assert controller.current_thread_owns_service_owner() is False
    controller.release_service_owner(permit)
    assert controller.service_owner_in_flight == 0

    replacement = controller.try_acquire_service_owner()
    assert replacement is not None
    controller.release_service_owner(replacement)
    assert controller.service_owner_in_flight == 0


def test_foreign_controller_cannot_release_service_owner() -> None:
    owner = PipelineAdmissionController(1, max_queued=0)
    foreign = PipelineAdmissionController(1, max_queued=0)
    permit = owner.try_acquire_service_owner()
    assert permit is not None

    with pytest.raises(RuntimeError, match="another controller"):
        foreign.release_service_owner(permit)
    assert owner.service_owner_in_flight == 1

    owner.release_service_owner(permit)
    assert owner.service_owner_in_flight == 0


async def test_capped_partition_handoff_does_not_rescan_blocked_partitions() -> None:
    partition_count = 1_000
    controller = _CountingAdmissionController(
        partition_count,
        max_queued=partition_count,
        max_queued_per_partition=1,
        max_in_flight_per_partition=1,
    )
    active = [await controller.acquire(partition_key=f"tenant-{index}") for index in range(partition_count)]
    queued = [
        asyncio.create_task(
            controller.acquire(
                partition_key=f"tenant-{index}",
                timeout_seconds=5,
            )
        )
        for index in range(partition_count)
    ]
    await _wait_for_queue(controller, partition_count)

    controller.capacity_checks = 0
    controller.selected_maintenance_checks = 0
    controller.max_selected_maintenance_batch = 0
    for lease in reversed(active):
        controller.release(lease)

    assert controller.capacity_checks <= partition_count * 3
    assert controller.selected_maintenance_checks <= partition_count * 8
    assert controller.max_selected_maintenance_batch <= 8
    maintenance_name = f"tacit-pipeline-admission-maintenance-{id(controller)}"
    assert sum(thread.name == maintenance_name and thread.is_alive() for thread in threading.enumerate()) <= 1

    for task in queued:
        task.cancel()
    results = await asyncio.gather(*queued, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert all(task.done() for task in queued)
    await _wait_for_admission_maintenance_exit(controller)
    assert controller.in_flight == 0
    assert controller.queued == 0
