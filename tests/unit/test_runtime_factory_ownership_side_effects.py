from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import replace

import pytest

import tacit.pipeline.side_effects as side_effects
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline.side_effects import (
    LifecycleOwnedBlockingWork,
    cancel_task_with_grace,
    safe_close_backends,
)
from tacit.pipeline_admission import PipelineAdmissionController, PipelineBlockingPermit


async def _wait_for_zero(controller: PipelineAdmissionController) -> None:
    for _ in range(100):
        if controller.in_flight == 0 and controller.blocking_in_flight == 0:
            return
        await asyncio.sleep(0)
    raise AssertionError("runtime admission did not return to zero")


@pytest.mark.parametrize("nested_operation", ["run", "realize_owned"])
@pytest.mark.asyncio
async def test_async_nested_same_worker_realization_reuses_owned_permit(
    nested_operation: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=1)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    worker_threads: list[int] = []
    retired: list[object] = []
    outer_product = object()
    nested_product = object()

    def outer_factory() -> object:
        worker_threads.append(threading.get_ident())
        return outer_product

    async def validate_outer(_product: object) -> None:
        if nested_operation == "run":
            result = await blocking_work.run(
                lambda: worker_threads.append(threading.get_ident()) or nested_product,
                reason_code="nested_same_worker_run",
                timeout_seconds=0.01,
            )
        else:
            result = await blocking_work.realize_owned(
                lambda: worker_threads.append(threading.get_ident()) or nested_product,
                validate=lambda _nested: None,
                retire=retired.append,
                reason_code="nested_same_worker_realization",
                timeout_seconds=0.01,
            )
        assert result is nested_product

    result = await blocking_work.realize_owned(
        outer_factory,
        validate=validate_outer,
        retire=retired.append,
        reason_code="outer_same_worker_realization",
    )

    assert result is outer_product
    assert len(worker_threads) == 2
    assert len(set(worker_threads)) == 1
    assert retired == []
    assert lifecycle.queued == 0
    await _wait_for_zero(lifecycle)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert blocking_work.active == 0


@pytest.mark.parametrize("failure_mode", ["iterable", "cardinality"])
def test_reserved_cleanup_group_preflight_releases_every_permit_without_callbacks(
    failure_mode: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(2)
    assert permits is not None
    callback_calls = 0

    def callback() -> None:
        nonlocal callback_calls
        callback_calls += 1

    error_type: type[Exception]
    if failure_mode == "iterable":

        def functions():
            yield callback
            raise RuntimeError("synthetic iterable failure")

        error_type = RuntimeError
        error_match = "synthetic iterable failure"
        selected_functions = functions()
    else:
        error_type = ValueError
        error_match = "same size"
        selected_functions = (callback,)

    with pytest.raises(error_type, match=error_match):
        blocking_work.run_reserved_background_group(
            selected_functions,
            permits,
            reason_code="cleanup_group_preflight_failure",
        )

    assert callback_calls == 0
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


def test_reserved_cleanup_group_bounds_iterable_and_rolls_back_every_permit() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(2)
    assert permits is not None
    pulls = 0
    callback_calls = 0

    def callback() -> None:
        nonlocal callback_calls
        callback_calls += 1

    def functions():
        nonlocal pulls
        while True:
            pulls += 1
            yield callback

    with pytest.raises(ValueError, match="same size"):
        blocking_work.run_reserved_background_group(
            functions(),
            permits,
            reason_code="cleanup_group_bounded_overflow",
        )

    assert pulls == len(permits) + 1
    assert callback_calls == 0
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.parametrize(
    "invalid_permits",
    ["duplicate", "foreign", "wrong_runtime", "inactive", "normal"],
)
def test_reserved_cleanup_group_rejects_invalid_permits_before_callbacks(
    invalid_permits: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    foreign = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    callback_called = threading.Event()
    release_after: list[tuple[PipelineAdmissionController, PipelineBlockingPermit]] = []
    permits: tuple[PipelineBlockingPermit, ...]
    callbacks: tuple[Callable[[], None], ...]

    if invalid_permits == "duplicate":
        owned = blocking_work.reserve_cleanup_permits(1)
        assert owned is not None
        permits = (owned[0], owned[0])
        callbacks = (callback_called.set, callback_called.set)
        release_after.append((lifecycle, owned[0]))
        error_match = "unique"
    elif invalid_permits == "foreign":
        foreign_permits = foreign.try_acquire_cleanup_permits(1)
        assert foreign_permits is not None
        permits = foreign_permits
        callbacks = (callback_called.set,)
        release_after.append((foreign, foreign_permits[0]))
        error_match = "another controller"
    elif invalid_permits == "wrong_runtime":
        owned = blocking_work.reserve_cleanup_permits(1)
        assert owned is not None
        permits = (replace(owned[0], runtime_identity="another-runtime"),)
        callbacks = (callback_called.set,)
        release_after.append((lifecycle, owned[0]))
        error_match = "another runtime"
    elif invalid_permits == "inactive":
        owned = blocking_work.reserve_cleanup_permits(1)
        assert owned is not None
        lifecycle.release_blocking_permit(owned[0])
        permits = owned
        callbacks = (callback_called.set,)
        error_match = "not active"
    else:
        normal = lifecycle.try_acquire_blocking_permit()
        assert normal is not None
        permits = (normal,)
        callbacks = (callback_called.set,)
        release_after.append((lifecycle, normal))
        error_match = "cleanup"

    with pytest.raises(RuntimeError, match=error_match):
        blocking_work.run_reserved_background_group(
            callbacks,
            permits,
            reason_code=f"cleanup_group_invalid_{invalid_permits}",
        )

    assert callback_called.is_set() is False
    assert blocking_work.active == 0
    for owner, permit in release_after:
        assert owner.blocking_in_flight == 1
        owner.release_blocking_permit(permit)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert foreign.in_flight == 0
    assert foreign.blocking_in_flight == 0


@pytest.mark.parametrize("missing_owner", ["lifecycle", "lease"])
@pytest.mark.asyncio
async def test_cancel_task_with_grace_rejects_partial_ownership_before_task_mutation(
    missing_owner: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    lease = await lifecycle.acquire()
    started = asyncio.Event()
    allow_finish = asyncio.Event()
    completed = False

    async def work() -> None:
        nonlocal completed
        started.set()
        await allow_finish.wait()
        completed = True

    task = asyncio.create_task(work())
    await started.wait()
    try:
        with pytest.raises(ValueError, match="together"):
            await cancel_task_with_grace(
                task,
                grace_seconds=0,
                reason_code="partial_cancellation_ownership",
                lifecycle=None if missing_owner == "lifecycle" else lifecycle,
                lease=None if missing_owner == "lease" else lease,
            )

        assert task.done() is False
        assert task.cancelling() == 0
        allow_finish.set()
        await task
        assert completed is True
    finally:
        allow_finish.set()
        if not task.done():
            await task
        lifecycle.release(lease)

    assert lifecycle.in_flight == 0


@pytest.mark.parametrize("nested_operation", ["run", "realize_owned", "realize_owned_sync"])
def test_cleanup_permit_rejects_normal_same_worker_reentry(
    monkeypatch: pytest.MonkeyPatch,
    nested_operation: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(1)
    assert permits is not None
    factory_calls = 0
    errors: list[BaseException] = []
    cleanup_finished = threading.Event()
    permit_released = threading.Event()
    release_permit = lifecycle.release_blocking_permit

    def tracked_release(permit: PipelineBlockingPermit) -> None:
        release_permit(permit)
        permit_released.set()

    def factory() -> object:
        nonlocal factory_calls
        factory_calls += 1
        return object()

    def cleanup() -> None:
        try:
            if nested_operation == "run":
                asyncio.run(
                    blocking_work.run(
                        factory,
                        reason_code="normal_run_from_cleanup_worker",
                    )
                )
            elif nested_operation == "realize_owned":
                asyncio.run(
                    blocking_work.realize_owned(
                        factory,
                        validate=lambda _product: None,
                        retire=lambda _product: None,
                        reason_code="normal_realization_from_cleanup_worker",
                    )
                )
            else:
                blocking_work.realize_owned_sync(
                    factory,
                    validate=lambda _product: None,
                    retire=lambda _product: None,
                    reason_code="normal_sync_realization_from_cleanup_worker",
                )
        except BaseException as exc:
            errors.append(exc)
        finally:
            cleanup_finished.set()

    monkeypatch.setattr(lifecycle, "release_blocking_permit", tracked_release)
    assert blocking_work.run_reserved_background_group(
        (cleanup,),
        permits,
        reason_code="cleanup_worker_normal_reentry",
    )
    assert cleanup_finished.wait(timeout=1)
    assert permit_released.wait(timeout=1)

    assert factory_calls == 0
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeOwnershipError)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert blocking_work.active == 0


def test_cleanup_permit_reuses_same_worker_for_nested_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(1)
    assert permits is not None
    worker_threads: list[int] = []
    cleanup_finished = threading.Event()
    permit_released = threading.Event()
    release_permit = lifecycle.release_blocking_permit

    def tracked_release(permit: PipelineBlockingPermit) -> None:
        release_permit(permit)
        permit_released.set()

    def cleanup() -> None:
        worker_threads.append(threading.get_ident())
        asyncio.run(
            blocking_work.run(
                lambda: worker_threads.append(threading.get_ident()),
                reason_code="nested_cleanup_same_worker",
                cleanup=True,
            )
        )
        cleanup_finished.set()

    monkeypatch.setattr(lifecycle, "release_blocking_permit", tracked_release)
    assert blocking_work.run_reserved_background_group(
        (cleanup,),
        permits,
        reason_code="cleanup_worker_cleanup_reentry",
    )
    assert cleanup_finished.wait(timeout=1)
    assert permit_released.wait(timeout=1)

    assert len(worker_threads) == 2
    assert len(set(worker_threads)) == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert blocking_work.active == 0


@pytest.mark.parametrize("call_style", ["async", "sync"])
@pytest.mark.parametrize("setup_failure", ["registration", "worker_construction"])
@pytest.mark.asyncio
async def test_setup_failure_after_reservation_releases_capacity_once(
    monkeypatch: pytest.MonkeyPatch,
    call_style: str,
    setup_failure: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    function_ran = threading.Event()

    release_calls = 0
    release_permit = lifecycle.release_blocking_permit

    def count_release(permit: PipelineBlockingPermit) -> None:
        nonlocal release_calls
        release_calls += 1
        release_permit(permit)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", count_release)
    if setup_failure == "registration":

        def reject_registration(_call: object) -> None:
            raise RuntimeError("synthetic registration failure")

        monkeypatch.setattr(blocking_work, "_register", reject_registration)
        error_match = "synthetic registration failure"
    else:

        def reject_worker_construction(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("synthetic worker construction failure")

        monkeypatch.setattr(blocking_work, "_worker_thread", reject_worker_construction)
        error_match = "blocking worker could not start"

    with pytest.raises(RuntimeError, match=error_match):
        if call_style == "async":
            await blocking_work.run(
                function_ran.set,
                reason_code="async_registration_failure",
            )
        else:
            blocking_work.realize_owned_sync(
                lambda: function_ran.set() or object(),
                validate=lambda _product: None,
                retire=lambda _product: None,
                reason_code="sync_registration_failure",
            )

    assert function_ran.is_set() is False
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0
    assert release_calls == 1


@pytest.mark.asyncio
async def test_sync_call_construction_failure_releases_reserved_capacity_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    release_calls = 0
    release_permit = lifecycle.release_blocking_permit

    def count_release(permit: PipelineBlockingPermit) -> None:
        nonlocal release_calls
        release_calls += 1
        release_permit(permit)

    def reject_call_construction(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("synthetic call construction failure")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", count_release)
    monkeypatch.setattr(side_effects, "_LifecycleBlockingCall", reject_call_construction)

    with pytest.raises(RuntimeError, match="synthetic call construction failure"):
        blocking_work.realize_owned_sync(
            object,
            validate=lambda _product: None,
            retire=lambda _product: None,
            reason_code="sync_call_construction_failure",
        )

    assert release_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0


@pytest.mark.asyncio
async def test_repeated_cancellation_retains_resistant_task_before_handoff_wait() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    lease = await lifecycle.acquire()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()

    async def resistant_work() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            await allow_cleanup.wait()
            raise

    resistant = asyncio.create_task(resistant_work())
    handoff = asyncio.create_task(
        cancel_task_with_grace(
            resistant,
            grace_seconds=1,
            reason_code="repeated_cancellation_handoff",
            lifecycle=lifecycle,
            lease=lease,
        )
    )
    await cleanup_started.wait()
    handoff.cancel()
    handoff.cancel()
    with pytest.raises(asyncio.CancelledError):
        await handoff

    lifecycle.release(lease)
    assert resistant.done() is False
    assert lifecycle.in_flight == 1
    assert lifecycle.retained == 1

    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await resistant
    await _wait_for_zero(lifecycle)
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_backend_cleanup_cancellation_retains_cooperative_close() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    class Backend:
        name = "cooperative"

        def __init__(self) -> None:
            self.closed = False
            self.cancelled = False
            self.close_finished = asyncio.Event()

        async def close(self) -> None:
            close_started.set()
            try:
                await allow_close.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            self.closed = True
            self.close_finished.set()

    backend = Backend()
    async with lifecycle.slot():
        cleanup = asyncio.create_task(
            safe_close_backends(
                [backend],
                grace_seconds=1,
                lifecycle=lifecycle,
            )
        )
        await close_started.wait()
        cleanup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cleanup

    assert backend.cancelled is False
    assert backend.closed is False
    assert lifecycle.in_flight == 1
    assert lifecycle.retained == 1

    allow_close.set()
    await asyncio.wait_for(backend.close_finished.wait(), timeout=1)
    assert backend.closed is True
    await _wait_for_zero(lifecycle)
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_backend_cleanup_timeout_retains_close_until_completion() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    class Backend:
        name = "timeout-cooperative"

        def __init__(self) -> None:
            self.closed = False
            self.close_finished = asyncio.Event()

        async def close(self) -> None:
            close_started.set()
            await allow_close.wait()
            self.closed = True
            self.close_finished.set()

    backend = Backend()
    async with lifecycle.slot():
        await safe_close_backends(
            [backend],
            grace_seconds=0.01,
            lifecycle=lifecycle,
        )
        assert close_started.is_set()
        assert backend.closed is False

    assert lifecycle.in_flight == 1
    assert lifecycle.retained == 1
    allow_close.set()
    await asyncio.wait_for(backend.close_finished.wait(), timeout=1)
    assert backend.closed is True
    await _wait_for_zero(lifecycle)
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_backend_cleanup_rejects_missing_runtime_owner_before_close() -> None:
    close_calls = 0

    class Backend:
        name = "ownerless"

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    with pytest.raises(RuntimeOwnershipError, match="runtime admission owner"):
        await safe_close_backends(
            [Backend()],
            lifecycle=None,  # type: ignore[arg-type]
        )

    assert close_calls == 0


@pytest.mark.asyncio
async def test_backend_cleanup_rejects_unavailable_runtime_owner_before_close() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    occupied = await lifecycle.acquire()
    close_calls = 0

    class Backend:
        name = "uncharged"

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    try:
        with pytest.raises(RuntimeOwnershipError, match="admitted runtime owner"):
            await safe_close_backends([Backend()], lifecycle=lifecycle)
        assert close_calls == 0
    finally:
        lifecycle.release(occupied)

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_backend_cleanup_retention_failure_rolls_back_before_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    close_calls = 0

    class Backend:
        name = "retention-failure"

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1

    def fail_retention(_task: asyncio.Task[object]) -> bool:
        raise RuntimeError("synthetic retention failure")

    monkeypatch.setattr(lifecycle, "retain_current_task", fail_retention)
    with pytest.raises(RuntimeError, match="synthetic retention failure"):
        await safe_close_backends([Backend()], lifecycle=lifecycle)

    assert close_calls == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
