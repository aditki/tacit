from __future__ import annotations

import asyncio
import contextlib
import gc
import threading
import weakref
from dataclasses import replace

import pytest

from tacit.config import Settings
from tacit.errors import PipelineAdmissionRejected
from tacit.pipeline_admission import PipelineAdmissionController


class _CountingAdmissionController(PipelineAdmissionController):
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

    def _can_activate_locked(self, partition: str) -> bool:
        self.capacity_checks += 1
        return super()._can_activate_locked(partition)


async def _wait_for_queue(controller: PipelineAdmissionController, expected: int) -> None:
    for _ in range(100):
        if controller.queued == expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {expected} queued pipeline runs, found {controller.queued}")


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


async def test_rejected_resource_sync_cleanup_does_not_block_the_event_loop() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    close_started = threading.Event()
    allow_close = threading.Event()

    class BlockingResource:
        def close(self) -> None:
            close_started.set()
            allow_close.wait()

    release_timer = threading.Timer(1.0, allow_close.set)
    release_timer.start()
    try:
        async with controller.slot():
            started_at = asyncio.get_running_loop().time()
            controller.close_rejected_resources(
                [BlockingResource()],
                grace_seconds=0.01,
                reason_code="ownership_mismatch",
            )
            await asyncio.sleep(0.02)
            assert asyncio.get_running_loop().time() - started_at < 0.2
            assert close_started.is_set()

        assert controller.in_flight == 1
        assert controller.retained == 1
    finally:
        allow_close.set()
        release_timer.cancel()
        release_timer.join()

    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0.01)
    assert controller.in_flight == 0
    assert controller.retained == 0


async def test_rejected_resource_resistant_cleanup_detaches_at_hard_deadline() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    close_started = asyncio.Event()
    close_cancelled = asyncio.Event()
    allow_close = asyncio.Event()

    class ResistantResource:
        async def close(self) -> None:
            close_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                await allow_close.wait()
                raise

    async with controller.slot():
        controller.close_rejected_resources(
            [ResistantResource()],
            grace_seconds=0.01,
            reason_code="ownership_mismatch",
        )
        await close_started.wait()
        await asyncio.wait_for(close_cancelled.wait(), timeout=0.5)
        await asyncio.sleep(0)
        assert not any(
            task.get_name() == "tacit-rejected-resource-cleanup" and not task.done() for task in asyncio.all_tasks()
        )

    assert controller.in_flight == 1
    assert controller.retained == 1
    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await controller.acquire()
    assert exc_info.value.reason_code == "pipeline_admission_queue_full"

    allow_close.set()
    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert controller.in_flight == 0
    assert controller.retained == 0


async def test_rejected_resource_cleanup_without_event_loop_has_a_hard_deadline() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    close_started = threading.Event()
    allow_close = threading.Event()

    class BlockingResource:
        def close(self) -> None:
            close_started.set()
            allow_close.wait()

    caller = threading.Thread(
        target=controller.close_rejected_resources,
        kwargs={
            "resources": [BlockingResource()],
            "grace_seconds": 0.01,
            "reason_code": "ownership_mismatch",
        },
        daemon=True,
    )
    release_timer = threading.Timer(1.0, allow_close.set)
    release_timer.start()
    caller.start()
    try:
        assert await asyncio.to_thread(close_started.wait, 0.5) is True
        caller.join(0.2)
        assert caller.is_alive() is False
        assert controller.in_flight == 1
        assert controller.retained == 1
        with pytest.raises(PipelineAdmissionRejected) as exc_info:
            await controller.acquire()
        assert exc_info.value.reason_code == "pipeline_admission_queue_full"
    finally:
        allow_close.set()
        release_timer.cancel()
        release_timer.join()
        caller.join(1)

    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0.01)
    assert controller.in_flight == 0
    assert controller.retained == 0


async def test_no_event_loop_cleanup_cancels_and_retains_resistant_awaitable_close() -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    close_started = threading.Event()
    close_cancelled = threading.Event()
    allow_close = threading.Event()

    class ResistantResource:
        async def close(self) -> None:
            close_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                close_cancelled.set()
                await asyncio.to_thread(allow_close.wait)
                raise

    caller = threading.Thread(
        target=controller.close_rejected_resources,
        kwargs={
            "resources": [ResistantResource()],
            "grace_seconds": 0.01,
            "reason_code": "ownership_mismatch",
        },
        daemon=True,
    )
    caller.start()
    try:
        assert await asyncio.to_thread(close_started.wait, 0.5) is True
        caller.join(0.2)
        assert caller.is_alive() is False
        assert await asyncio.to_thread(close_cancelled.wait, 0.5) is True
        assert controller.in_flight == 1
        assert controller.retained == 1
    finally:
        allow_close.set()
        caller.join(1)

    for _ in range(100):
        if controller.in_flight == 0:
            break
        await asyncio.sleep(0.01)
    assert controller.in_flight == 0
    assert controller.retained == 0


@pytest.mark.parametrize("first_completion", ["task", "thread"])
async def test_mixed_retained_work_releases_only_after_every_category_completes(
    first_completion: str,
) -> None:
    controller = PipelineAdmissionController(1, max_queued=0)
    allow_task = asyncio.Event()
    close_started = threading.Event()
    allow_close = threading.Event()

    async def retained_work() -> None:
        await allow_task.wait()

    class BlockingResource:
        def close(self) -> None:
            close_started.set()
            allow_close.wait()

    task: asyncio.Task[None] | None = None
    try:
        async with controller.slot() as lease:
            task = asyncio.create_task(retained_work())
            assert controller.retain_task(lease, task) is True
            await asyncio.to_thread(
                controller.close_rejected_resources,
                [BlockingResource()],
                grace_seconds=0.01,
                reason_code="ownership_mismatch",
            )
            assert close_started.is_set()

        assert controller.in_flight == 1
        assert controller.retained == 1

        if first_completion == "task":
            allow_task.set()
            await task
            await asyncio.sleep(0)
        else:
            allow_close.set()
            for _ in range(100):
                if not any(
                    thread.name == "tacit-rejected-resource-close" and thread.is_alive()
                    for thread in threading.enumerate()
                ):
                    break
                await asyncio.sleep(0.01)

        assert controller.in_flight == 1
        assert controller.retained == 1
        with pytest.raises(PipelineAdmissionRejected) as exc_info:
            await controller.acquire()
        assert exc_info.value.reason_code == "pipeline_admission_queue_full"
    finally:
        allow_task.set()
        allow_close.set()
        if task is not None:
            await task

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
    for lease in reversed(active):
        controller.release(lease)

    assert controller.capacity_checks <= partition_count * 3

    for task in queued:
        task.cancel()
    results = await asyncio.gather(*queued, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    assert controller.in_flight == 0
    assert controller.queued == 0
