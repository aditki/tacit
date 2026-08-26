"""Runtime-owned admission control for pipeline executions."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import threading
import time
from collections import OrderedDict, deque
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import structlog

from tacit.errors import PipelineAdmissionRejected

_DEFAULT_PARTITION = "__default__"
_SELECTED_MAINTENANCE_BUDGET = 8
logger = structlog.get_logger()


class PipelineSideEffectFence:
    """Fence publication-like work after a run loses its request lifecycle."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._reason_code: str | None = None

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._reason_code is not None

    def close(self, reason_code: str) -> None:
        with self._lock:
            if self._reason_code is None:
                self._reason_code = reason_code

    def ensure_side_effects_allowed(self) -> None:
        with self._lock:
            if self._reason_code is not None:
                raise RuntimeError("pipeline side effects are fenced")


@dataclass(frozen=True)
class PipelineAdmissionLimits:
    """Validated limits for one runtime-owned admission controller."""

    concurrent: int
    concurrent_per_partition: int
    queued: int
    queued_per_partition: int


def pipeline_admission_limits(runtime_settings: Any) -> PipelineAdmissionLimits:
    """Resolve and revalidate admission settings at the runtime boundary."""
    concurrent = int(getattr(runtime_settings, "pipeline_max_concurrent", 5))
    configured_concurrent_per_partition = int(getattr(runtime_settings, "pipeline_max_concurrent_per_tenant", 0))
    queued = int(getattr(runtime_settings, "pipeline_max_queued", 100))
    configured_queued_per_partition = int(getattr(runtime_settings, "pipeline_max_queued_per_tenant", 25))
    wildcard = str(getattr(runtime_settings, "knowledge_tenant_id", "default")) == "*"

    if not 1 <= concurrent <= 1_000:
        raise ValueError("pipeline_max_concurrent must be between 1 and 1000")
    if not 0 <= configured_concurrent_per_partition <= 1_000:
        raise ValueError("pipeline_max_concurrent_per_tenant must be between 0 and 1000")
    if not 0 <= queued <= 1_000:
        raise ValueError("pipeline_max_queued must be between 0 and 1000")
    if not 0 <= configured_queued_per_partition <= 1_000:
        raise ValueError("pipeline_max_queued_per_tenant must be between 0 and 1000")

    if wildcard:
        concurrent_per_partition = configured_concurrent_per_partition or max(1, concurrent - 1)
        if concurrent_per_partition > concurrent or (concurrent > 1 and concurrent_per_partition == concurrent):
            raise ValueError(
                "pipeline_max_concurrent_per_tenant must be lower than " "pipeline_max_concurrent for wildcard tenancy"
            )
        queued_per_partition = configured_queued_per_partition
        if queued > 0 and queued_per_partition >= queued:
            raise ValueError(
                "pipeline_max_queued_per_tenant must be lower than pipeline_max_queued " "for wildcard tenancy"
            )
    else:
        concurrent_per_partition = concurrent
        queued_per_partition = queued

    return PipelineAdmissionLimits(
        concurrent=concurrent,
        concurrent_per_partition=concurrent_per_partition,
        queued=queued,
        queued_per_partition=queued_per_partition,
    )


@dataclass(frozen=True)
class PipelineAdmissionLease:
    wait_seconds: float
    queued: bool
    queue_depth_at_entry: int
    partition_queue_depth_at_entry: int
    controller_identity: object = field(repr=False)
    partition: str = field(repr=False)
    token: int = field(repr=False)
    fence: PipelineSideEffectFence = field(repr=False, compare=False)


@dataclass
class _Waiter:
    token: int
    partition: str
    loop: asyncio.AbstractEventLoop
    event: asyncio.Event
    deadline: float | None
    state: str = "queued"


class PipelineAdmissionController:
    """Bound and fairly schedule pipeline work across one runtime graph."""

    def __init__(
        self,
        limit: int,
        *,
        max_queued: int = 100,
        max_queued_per_partition: int | None = None,
        max_in_flight_per_partition: int | None = None,
    ) -> None:
        if limit < 1:
            raise ValueError("pipeline concurrency limit must be positive")
        if max_queued < 0:
            raise ValueError("pipeline queue limit cannot be negative")
        partition_queue_limit = max_queued if max_queued_per_partition is None else max_queued_per_partition
        if partition_queue_limit < 0:
            raise ValueError("pipeline partition queue limit cannot be negative")
        partition_active_limit = limit if max_in_flight_per_partition is None else max_in_flight_per_partition
        if not 1 <= partition_active_limit <= limit:
            raise ValueError(
                "pipeline partition concurrency limit must be positive and no greater than the global limit"
            )
        self.limit = limit
        self.max_queued = max_queued
        self.max_queued_per_partition = partition_queue_limit
        self.max_in_flight_per_partition = partition_active_limit
        self._lock = threading.Lock()
        self._identity = object()
        self._in_flight = 0
        self._in_flight_by_partition: dict[str, int] = {}
        self._active_leases: dict[int, str] = {}
        self._queued_count = 0
        self._next_token = 0
        self._queues: dict[str, OrderedDict[int, _Waiter]] = {}
        self._eligible_partitions: OrderedDict[str, None] = OrderedDict()
        self._selected: dict[int, _Waiter] = {}
        self._selected_checks: deque[int] = deque()
        self._selected_by_partition: dict[str, int] = {}
        self._retained_tasks: dict[int, set[asyncio.Task[Any]]] = {}
        self._retained_task_tokens: dict[asyncio.Task[Any], int] = {}
        self._retained_threads: dict[int, set[threading.Thread]] = {}
        self._release_pending: set[int] = set()
        self._current_leases: contextvars.ContextVar[tuple[int, ...]] = contextvars.ContextVar(
            f"tacit_pipeline_lifecycle_{id(self)}",
            default=(),
        )

    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    def in_flight_for(self, partition_key: str) -> int:
        """Return active work for one availability partition."""
        partition = self._partition(partition_key)
        with self._lock:
            return self._in_flight_by_partition.get(partition, 0)

    @property
    def queued(self) -> int:
        with self._lock:
            now = time.monotonic()
            self._maintain_selected_locked(now)
            self._notify_available_locked(now)
            return self._queued_count

    def queued_for(self, partition_key: str) -> int:
        """Return queued work for one availability partition."""
        partition = self._partition(partition_key)
        with self._lock:
            queue = self._queues.get(partition)
            return len(queue) if queue is not None else 0

    @property
    def retained(self) -> int:
        """Return request-complete work still consuming effective capacity."""
        with self._lock:
            return len(self._release_pending)

    async def acquire(
        self,
        *,
        timeout_seconds: float | None = None,
        partition_key: str = "",
    ) -> PipelineAdmissionLease:
        started_at = time.monotonic()
        deadline = started_at + timeout_seconds if timeout_seconds is not None else None
        loop = asyncio.get_running_loop()
        partition = self._partition(partition_key)
        waiter: _Waiter | None = None
        queue_depth = 0
        partition_queue_depth = 0

        with self._lock:
            self._next_token += 1
            token = self._next_token
            self._maintain_selected_locked(started_at)
            self._notify_available_locked(started_at)
            if self._can_activate_locked(partition) and not self._queues.get(partition):
                self._activate_locked(token, partition)
            else:
                partition_depth = len(self._queues.get(partition, ()))
                if self._queued_count >= self.max_queued or partition_depth >= self.max_queued_per_partition:
                    raise PipelineAdmissionRejected("pipeline_admission_queue_full")
                waiter = _Waiter(
                    token=token,
                    partition=partition,
                    loop=loop,
                    event=asyncio.Event(),
                    deadline=deadline,
                )
                queue = self._queues.setdefault(partition, OrderedDict())
                queue[waiter.token] = waiter
                self._queued_count += 1
                self._refresh_partition_eligibility_locked(partition)
                queue_depth = self._queued_count
                partition_queue_depth = len(queue)
                self._notify_available_locked(started_at)

        if waiter is not None:
            try:
                while True:
                    with self._lock:
                        now = time.monotonic()
                        self._maintain_selected_locked(now)
                        self._notify_available_locked(now)
                        if waiter.state == "selected":
                            self._claim_waiter_locked(waiter, now)
                            break
                        if waiter.state in {"abandoned", "expired"}:
                            raise PipelineAdmissionRejected("pipeline_admission_wait_timeout")
                        remaining = deadline - now if deadline is not None else None
                        if remaining is not None and remaining <= 0:
                            self._remove_waiter_locked(waiter, state="expired")
                            self._notify_available_locked(now)
                            raise PipelineAdmissionRejected("pipeline_admission_wait_timeout")

                    try:
                        if remaining is None:
                            await waiter.event.wait()
                        else:
                            await asyncio.wait_for(waiter.event.wait(), timeout=remaining)
                    except TimeoutError:
                        pass
                    waiter.event.clear()
            except BaseException:
                with self._lock:
                    if self._remove_waiter_locked(waiter, state="cancelled"):
                        self._notify_available_locked(time.monotonic())
                raise

        return PipelineAdmissionLease(
            wait_seconds=time.monotonic() - started_at,
            queued=waiter is not None,
            queue_depth_at_entry=queue_depth,
            partition_queue_depth_at_entry=partition_queue_depth,
            controller_identity=self._identity,
            partition=partition,
            token=token,
            fence=PipelineSideEffectFence(),
        )

    def release(self, lease: PipelineAdmissionLease) -> None:
        with self._lock:
            self._validate_active_lease_locked(lease)
            if lease.token in self._release_pending:
                raise RuntimeError("pipeline admission lease is not active")
            self._release_pending.add(lease.token)
            self._complete_pending_release_if_idle_locked(lease.token)

    def retain_task(self, lease: PipelineAdmissionLease, task: asyncio.Task[Any]) -> bool:
        """Keep a lease charged until resistant work has actually terminated."""
        with self._lock:
            self._validate_active_lease_locked(lease)
            return self._retain_task_locked(lease.token, task)

    def retain_current_task(self, task: asyncio.Task[Any]) -> bool:
        """Charge cleanup spawned by the current run to its effective-work lease."""
        with self._lock:
            for token in reversed(self._current_leases.get()):
                if token in self._active_leases:
                    return self._retain_task_locked(token, task)
            if self._in_flight + len(self._selected) >= self.limit:
                return False
            self._next_token += 1
            token = self._next_token
            partition = _DEFAULT_PARTITION
            self._activate_locked(token, partition)
            self._release_pending.add(token)
            retained = self._retain_task_locked(token, task)
            if not retained:
                self._complete_pending_release_if_idle_locked(token)
            return retained

    def close_rejected_resources(
        self,
        resources: Iterable[Any],
        *,
        grace_seconds: float,
        reason_code: str,
    ) -> None:
        """Close rejected products under the same bounded effective-work owner."""
        unique_resources = tuple({id(resource): resource for resource in resources if resource is not None}.values())
        if not unique_resources:
            return

        async def cleanup_one(resource: Any, *, async_close: bool) -> None:
            try:
                if async_close:
                    close_result = resource.close()
                else:
                    close_result = await asyncio.to_thread(resource.close)
                if inspect.isawaitable(close_result):
                    await close_result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "pipeline_rejected_resource_cleanup_failed",
                    reason_code=reason_code,
                    error_type=type(exc).__name__,
                )

        async def cleanup_all() -> None:
            parent = asyncio.current_task()
            tasks = {
                asyncio.create_task(
                    cleanup_one(
                        resource,
                        async_close=inspect.iscoroutinefunction(resource.close),
                    ),
                    name="tacit-rejected-resource-close",
                ): inspect.iscoroutinefunction(resource.close)
                for resource in unique_resources
            }
            done, pending = await asyncio.wait(tasks, timeout=grace_seconds)
            for task in done:
                self._consume_task(task)
            for task in pending:
                if parent is None or not self._retain_task_with_parent(parent, task):
                    task.cancel()
                    task.add_done_callback(self._consume_task)
                    logger.warning(
                        "pipeline_cleanup_budget_exhausted",
                        reason_code="pipeline_cleanup_budget_exhausted",
                    )
                    continue
                if tasks[task]:
                    task.cancel()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._close_rejected_resources_without_loop(
                unique_resources,
                grace_seconds=grace_seconds,
                reason_code=reason_code,
            )
            return

        cleanup_task = loop.create_task(
            cleanup_all(),
            name="tacit-rejected-resource-cleanup",
        )
        if not self.retain_current_task(cleanup_task):
            cleanup_task.cancel()
            cleanup_task.add_done_callback(self._consume_task)
            logger.warning(
                "pipeline_cleanup_budget_exhausted",
                reason_code="pipeline_cleanup_budget_exhausted",
            )

    def _close_rejected_resources_without_loop(
        self,
        resources: tuple[Any, ...],
        *,
        grace_seconds: float,
        reason_code: str,
    ) -> None:
        async_cleanup: dict[threading.Thread, tuple[asyncio.AbstractEventLoop, asyncio.Future[Any]]] = {}
        async_cleanup_lock = threading.Lock()
        cancel_requested = threading.Event()

        def cleanup_one(token: int, resource: Any) -> None:
            worker = threading.current_thread()
            loop: asyncio.AbstractEventLoop | None = None
            try:
                close_result = resource.close()
                if inspect.isawaitable(close_result):
                    loop = asyncio.new_event_loop()
                    task = asyncio.ensure_future(close_result, loop=loop)
                    with async_cleanup_lock:
                        async_cleanup[worker] = (loop, task)
                    if cancel_requested.is_set():
                        task.cancel()
                    loop.run_until_complete(task)
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning(
                    "pipeline_rejected_resource_cleanup_failed",
                    reason_code=reason_code,
                    error_type=type(exc).__name__,
                )
            finally:
                with async_cleanup_lock:
                    async_cleanup.pop(worker, None)
                if loop is not None:
                    loop.close()
                self._retained_thread_done(token, worker)

        with self._lock:
            token = next(
                (
                    current_token
                    for current_token in reversed(self._current_leases.get())
                    if current_token in self._active_leases
                ),
                None,
            )
            if token is None:
                if self._in_flight + len(self._selected) >= self.limit:
                    logger.warning(
                        "pipeline_cleanup_budget_exhausted",
                        reason_code="pipeline_cleanup_budget_exhausted",
                    )
                    return
                self._next_token += 1
                token = self._next_token
                self._activate_locked(token, _DEFAULT_PARTITION)
                self._release_pending.add(token)
            workers = {
                threading.Thread(
                    target=cleanup_one,
                    args=(token, resource),
                    name="tacit-rejected-resource-close",
                    daemon=True,
                )
                for resource in resources
            }
            self._retained_threads.setdefault(token, set()).update(workers)

        for worker in workers:
            try:
                worker.start()
            except Exception as exc:
                logger.warning(
                    "pipeline_rejected_resource_cleanup_failed",
                    reason_code=reason_code,
                    error_type=type(exc).__name__,
                )
                self._retained_thread_done(token, worker)

        deadline = time.monotonic() + max(0.0, grace_seconds)
        for worker in workers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            worker.join(remaining)

        cancel_requested.set()
        with async_cleanup_lock:
            pending_async = tuple(async_cleanup.values())
        for worker_loop, task in pending_async:
            if task.done():
                continue
            with contextlib.suppress(RuntimeError):
                worker_loop.call_soon_threadsafe(task.cancel)

    @asynccontextmanager
    async def slot(
        self,
        *,
        timeout_seconds: float | None = None,
        partition_key: str = "",
    ) -> AsyncIterator[PipelineAdmissionLease]:
        lease = await self.acquire(
            timeout_seconds=timeout_seconds,
            partition_key=partition_key,
        )
        context_token = self._current_leases.set((*self._current_leases.get(), lease.token))
        try:
            yield lease
        finally:
            self._current_leases.reset(context_token)
            self.release(lease)

    @staticmethod
    def _partition(partition_key: str) -> str:
        return str(partition_key or _DEFAULT_PARTITION)

    def _can_activate_locked(self, partition: str) -> bool:
        return self._in_flight + len(self._selected) < self.limit and self._partition_has_capacity_locked(partition)

    def _partition_has_capacity_locked(self, partition: str) -> bool:
        reserved = self._selected_by_partition.get(partition, 0)
        active = self._in_flight_by_partition.get(partition, 0)
        return active + reserved < self.max_in_flight_per_partition

    def _activate_locked(self, token: int, partition: str) -> None:
        if token in self._active_leases:
            raise RuntimeError("pipeline admission lease identity was reused")
        self._active_leases[token] = partition
        self._in_flight += 1
        self._in_flight_by_partition[partition] = self._in_flight_by_partition.get(partition, 0) + 1

    def _validate_active_lease_locked(self, lease: PipelineAdmissionLease) -> str:
        if lease.controller_identity is not self._identity:
            raise RuntimeError("pipeline admission lease belongs to another controller")
        partition = self._active_leases.get(lease.token)
        if partition is None:
            raise RuntimeError("pipeline admission lease is not active")
        if partition != lease.partition:
            raise RuntimeError("pipeline admission lease partition was corrupted")
        return partition

    def _retain_task_locked(self, token: int, task: asyncio.Task[Any]) -> bool:
        if task.done():
            self._consume_task(task)
            return False
        retained = self._retained_tasks.setdefault(token, set())
        if task in retained:
            return True
        retained.add(task)
        self._retained_task_tokens[task] = token

        def retained_done(completed: asyncio.Task[Any]) -> None:
            self._retained_task_done(token, completed)

        task.add_done_callback(retained_done)
        return True

    def _retain_task_with_parent(
        self,
        parent: asyncio.Task[Any],
        task: asyncio.Task[Any],
    ) -> bool:
        with self._lock:
            token = self._retained_task_tokens.get(parent)
            if token is None:
                return False
            self._retain_task_locked(token, task)
            return True

    def _retained_task_done(
        self,
        token: int,
        task: asyncio.Task[Any],
    ) -> None:
        self._consume_task(task)
        with self._lock:
            retained = self._retained_tasks.get(token)
            if retained is None or task not in retained:
                return
            retained.remove(task)
            self._retained_task_tokens.pop(task, None)
            if not retained:
                self._retained_tasks.pop(token, None)
            self._complete_pending_release_if_idle_locked(token)

    def _retained_thread_done(
        self,
        token: int,
        thread: threading.Thread,
    ) -> None:
        with self._lock:
            retained = self._retained_threads.get(token)
            if retained is None or thread not in retained:
                return
            retained.remove(thread)
            if not retained:
                self._retained_threads.pop(token, None)
            self._complete_pending_release_if_idle_locked(token)

    def _complete_pending_release_if_idle_locked(self, token: int) -> None:
        if token not in self._release_pending or self._has_retained_work_locked(token):
            return
        partition = self._active_leases.get(token)
        if partition is None:
            return
        self._release_pending.remove(token)
        self._complete_release_locked(token, partition)

    def _has_retained_work_locked(self, token: int) -> bool:
        return bool(self._retained_tasks.get(token) or self._retained_threads.get(token))

    def _complete_release_locked(self, token: int, partition: str) -> None:
        if self._has_retained_work_locked(token):
            raise RuntimeError("pipeline admission lease still has retained work")
        self._active_leases.pop(token)
        self._release_pending.discard(token)
        self._in_flight -= 1
        self._decrement_partition_count(self._in_flight_by_partition, partition)
        self._refresh_partition_eligibility_locked(partition)
        now = time.monotonic()
        self._maintain_selected_locked(now)
        self._notify_available_locked(now)

    @staticmethod
    def _consume_task(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except BaseException:
            return

    def _claim_waiter_locked(self, waiter: _Waiter, now: float) -> None:
        queue = self._queues.get(waiter.partition)
        if queue is None or next(iter(queue), None) != waiter.token:
            raise RuntimeError("pipeline admission queue order was corrupted")
        if waiter.token not in self._selected:
            raise RuntimeError("pipeline admission selected state was corrupted")
        queue.pop(waiter.token)
        self._selected.pop(waiter.token)
        self._decrement_partition_count(self._selected_by_partition, waiter.partition)
        self._queued_count -= 1
        waiter.state = "claimed"
        self._activate_locked(waiter.token, waiter.partition)
        self._refresh_partition_eligibility_locked(waiter.partition)
        self._maintain_selected_locked(now)
        self._notify_available_locked(now)

    def _remove_waiter_locked(self, waiter: _Waiter, *, state: str) -> bool:
        if waiter.state not in {"queued", "selected"}:
            return False
        queue = self._queues.get(waiter.partition)
        if queue is None or queue.pop(waiter.token, None) is None:
            return False
        if self._selected.pop(waiter.token, None) is not None:
            self._decrement_partition_count(self._selected_by_partition, waiter.partition)
        self._queued_count -= 1
        waiter.state = state
        self._refresh_partition_eligibility_locked(waiter.partition)
        if state in {"abandoned", "expired"}:
            self._wake_waiter_locked(waiter)
        return True

    def _refresh_partition_eligibility_locked(self, partition: str) -> None:
        queue = self._queues.get(partition)
        if not queue:
            self._queues.pop(partition, None)
            self._eligible_partitions.pop(partition, None)
            return
        head = next(iter(queue.values()))
        if head.state == "queued" and self._partition_has_capacity_locked(partition):
            self._eligible_partitions.setdefault(partition, None)
        else:
            self._eligible_partitions.pop(partition, None)

    def _maintain_selected_locked(self, now: float) -> None:
        # Inspect a fixed rotating sample. Removed tokens are discarded lazily,
        # so handoff cost stays amortized even at the supported 1,000-slot limit.
        checks = min(_SELECTED_MAINTENANCE_BUDGET, len(self._selected_checks))
        for _ in range(checks):
            token = self._selected_checks.popleft()
            waiter = self._selected.get(token)
            if waiter is None:
                continue
            expired = waiter.deadline is not None and now >= waiter.deadline
            unavailable = waiter.loop.is_closed() or not waiter.loop.is_running()
            if expired or unavailable:
                self._remove_waiter_locked(
                    waiter,
                    state="expired" if expired else "abandoned",
                )
            else:
                self._selected_checks.append(token)

    def _notify_available_locked(self, now: float) -> None:
        while self._in_flight + len(self._selected) < self.limit:
            waiter = self._next_waiter_locked(now)
            if waiter is None:
                return
            waiter.state = "selected"
            self._selected[waiter.token] = waiter
            self._selected_checks.append(waiter.token)
            self._selected_by_partition[waiter.partition] = self._selected_by_partition.get(waiter.partition, 0) + 1
            self._refresh_partition_eligibility_locked(waiter.partition)
            if not self._wake_waiter_locked(waiter):
                self._remove_waiter_locked(waiter, state="abandoned")

    def _next_waiter_locked(self, now: float) -> _Waiter | None:
        while self._eligible_partitions:
            partition, _ = self._eligible_partitions.popitem(last=False)
            queue = self._queues.get(partition)
            if not queue:
                self._queues.pop(partition, None)
                continue
            if not self._can_activate_locked(partition):
                self._refresh_partition_eligibility_locked(partition)
                continue
            waiter = next(iter(queue.values()))
            expired = waiter.deadline is not None and now >= waiter.deadline
            unavailable = waiter.loop.is_closed() or not waiter.loop.is_running()
            if expired or unavailable:
                self._remove_waiter_locked(
                    waiter,
                    state="expired" if expired else "abandoned",
                )
                continue
            if waiter.state == "queued":
                return waiter
            self._refresh_partition_eligibility_locked(partition)
        return None

    @staticmethod
    def _decrement_partition_count(counts: dict[str, int], partition: str) -> None:
        remaining = counts.get(partition, 0) - 1
        if remaining < 0:
            raise RuntimeError("pipeline admission partition accounting underflow")
        if remaining == 0:
            counts.pop(partition, None)
        else:
            counts[partition] = remaining

    @staticmethod
    def _wake_waiter_locked(waiter: _Waiter) -> bool:
        try:
            waiter.loop.call_soon_threadsafe(waiter.event.set)
        except RuntimeError:
            return False
        return True
