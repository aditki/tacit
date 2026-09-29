from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from dataclasses import replace

import pytest

import tacit.pipeline.side_effects as side_effects
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
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


async def _wait_for_worker_zero(blocking_work: LifecycleOwnedBlockingWork) -> None:
    deadline = asyncio.get_running_loop().time() + 1.0
    while blocking_work.active and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0)
    assert blocking_work.active == 0


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
                adopt=lambda _nested: None,
                retire=retired.append,
                reason_code="nested_same_worker_realization",
                timeout_seconds=0.01,
            )
        assert result is nested_product

    result = await blocking_work.realize_owned(
        outer_factory,
        validate=validate_outer,
        adopt=lambda _product: None,
        retire=retired.append,
        reason_code="outer_same_worker_realization",
    )

    assert result is outer_product
    assert len(worker_threads) == 2
    assert len(set(worker_threads)) == 1
    assert retired == []
    assert lifecycle.queued == 0
    await _wait_for_zero(lifecycle)
    await _wait_for_worker_zero(blocking_work)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert blocking_work.active == 0


@pytest.mark.asyncio
async def test_adopted_result_is_not_published_before_worker_releases_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    release_entered = threading.Event()
    allow_release = threading.Event()
    original_release = lifecycle.release_blocking_permit
    adopted = threading.Event()

    def delayed_release(permit: PipelineBlockingPermit) -> None:
        release_entered.set()
        assert allow_release.wait(timeout=1.0)
        original_release(permit)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", delayed_release)
    realization = asyncio.create_task(
        blocking_work.realize_owned(
            lambda: product,
            validate=lambda _product: None,
            adopt=lambda _product: adopted.set(),
            retire=lambda _product: None,
            reason_code="adopted_result_release_order",
        )
    )
    try:
        assert await asyncio.to_thread(release_entered.wait, 1.0)
        assert adopted.is_set()
        await asyncio.sleep(0)
        assert realization.done() is False
        assert lifecycle.blocking_in_flight == 1

        allow_release.set()
        assert await realization is product
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert blocking_work.active == 0
    finally:
        allow_release.set()
        if not realization.done():
            realization.cancel()
            await asyncio.gather(realization, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("_iteration", range(25))
async def test_cancellation_after_handoff_ack_retires_escrow_before_final_settlement(
    monkeypatch: pytest.MonkeyPatch,
    _iteration: int,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    handoff_acknowledged = threading.Event()
    second_abandonment_check_returned = threading.Event()
    allow_final_settlement = threading.Event()
    retired: list[object] = []
    factory_threads: list[int] = []
    retirement_threads: list[int] = []
    abandonment_checks = 0
    abandonment_checks_lock = threading.Lock()
    original_acknowledge = side_effects._LifecycleBlockingCall.acknowledge_result_handoff
    original_retire = side_effects._LifecycleBlockingCall.retire_abandoned_result_before_final_settlement

    def realize() -> object:
        factory_threads.append(threading.get_ident())
        return product

    def retire(candidate: object) -> None:
        retirement_threads.append(threading.get_ident())
        retired.append(candidate)

    def observe_acknowledgement(
        call: side_effects._LifecycleBlockingCall,
        result: object,
    ) -> None:
        original_acknowledge(call, result)
        handoff_acknowledged.set()

    def pause_after_second_abandonment_check(
        call: side_effects._LifecycleBlockingCall,
    ) -> None:
        nonlocal abandonment_checks
        original_retire(call)
        with abandonment_checks_lock:
            abandonment_checks += 1
            check_index = abandonment_checks
        if check_index == 2:
            second_abandonment_check_returned.set()
            assert allow_final_settlement.wait(timeout=1.0)

    monkeypatch.setattr(
        side_effects._LifecycleBlockingCall,
        "acknowledge_result_handoff",
        observe_acknowledgement,
    )
    monkeypatch.setattr(
        side_effects._LifecycleBlockingCall,
        "retire_abandoned_result_before_final_settlement",
        pause_after_second_abandonment_check,
    )
    realization = asyncio.create_task(
        blocking_work.realize_owned(
            realize,
            validate=lambda _product: None,
            adopt=lambda _product: None,
            retire=retire,
            reason_code="post_ack_cancellation_matrix",
        )
    )
    try:
        assert await asyncio.to_thread(handoff_acknowledged.wait, 1.0)
        assert await asyncio.to_thread(second_abandonment_check_returned.wait, 1.0)
        assert lifecycle.blocking_in_flight == 1
        assert blocking_work.active == 1

        realization.cancel()
        with pytest.raises(asyncio.CancelledError):
            await realization

        allow_final_settlement.set()
        deadline = asyncio.get_running_loop().time() + 1.0
        while blocking_work.active and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.001)

        assert retired == [product]
        assert retirement_threads == factory_threads
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert blocking_work.active == 0
    finally:
        allow_final_settlement.set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "settlement_phase",
    ["before_permit_release", "permit_release", "worker_finish"],
)
@pytest.mark.parametrize("_iteration", range(10))
async def test_post_ack_cancellation_has_one_owner_at_each_settlement_phase(
    monkeypatch: pytest.MonkeyPatch,
    settlement_phase: str,
    _iteration: int,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    settlement_entered = threading.Event()
    allow_settlement = threading.Event()
    retired: list[object] = []
    factory_threads: list[int] = []
    retirement_threads: list[int] = []
    retirement_capacity: list[tuple[int, int]] = []

    def realize() -> object:
        factory_threads.append(threading.get_ident())
        return product

    def retire(candidate: object) -> None:
        retirement_threads.append(threading.get_ident())
        retirement_capacity.append((lifecycle.blocking_in_flight, lifecycle.retained))
        retired.append(candidate)

    if settlement_phase == "before_permit_release":
        original_release_transition = blocking_work._release_permit_failure_aware

        def pause_settlement(
            permit: PipelineBlockingPermit,
            *,
            call: side_effects._LifecycleBlockingCall | None = None,
            primary_error: BaseException | None = None,
        ):
            settlement_entered.set()
            assert allow_settlement.wait(timeout=1.0)
            return original_release_transition(
                permit,
                call=call,
                primary_error=primary_error,
            )

        monkeypatch.setattr(
            blocking_work,
            "_release_permit_failure_aware",
            pause_settlement,
        )
    elif settlement_phase == "permit_release":
        original_release = lifecycle.release_blocking_permit

        def pause_settlement(permit: PipelineBlockingPermit) -> None:
            settlement_entered.set()
            assert allow_settlement.wait(timeout=1.0)
            original_release(permit)

        monkeypatch.setattr(lifecycle, "release_blocking_permit", pause_settlement)
    else:
        original_mark_finished = side_effects._LifecycleBlockingCall.mark_finished

        def pause_settlement(call: side_effects._LifecycleBlockingCall) -> None:
            settlement_entered.set()
            assert allow_settlement.wait(timeout=1.0)
            original_mark_finished(call)

        monkeypatch.setattr(
            side_effects._LifecycleBlockingCall,
            "mark_finished",
            pause_settlement,
        )

    realization = asyncio.create_task(
        blocking_work.realize_owned(
            realize,
            validate=lambda _product: None,
            adopt=lambda _product: None,
            retire=retire,
            reason_code=f"post_claim_cancellation_{settlement_phase}",
        )
    )
    try:
        assert await asyncio.to_thread(settlement_entered.wait, 1.0)
        realization.cancel()
        await asyncio.sleep(0)
        allow_settlement.set()

        if settlement_phase != "worker_finish":
            with pytest.raises(asyncio.CancelledError):
                await realization
            deadline = asyncio.get_running_loop().time() + 1.0
            while blocking_work.active and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.001)
            assert retired == [product]
            assert retirement_threads == factory_threads
            expected_capacity = (1, 2) if settlement_phase == "before_permit_release" else (0, 1)
            assert retirement_capacity == [expected_capacity]
        else:
            assert await realization is product
            assert retired == []
            assert retirement_threads == []
            assert retirement_capacity == []
        await _wait_for_zero(lifecycle)
        await _wait_for_worker_zero(blocking_work)
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert blocking_work.active == 0
    finally:
        allow_settlement.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("ownership_phase", ["pre_commit", "post_commit"])
async def test_real_coroutine_close_preserves_exactly_one_result_owner(
    monkeypatch: pytest.MonkeyPatch,
    ownership_phase: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    boundary_entered = threading.Event()
    close_handler_entered = threading.Event()
    allow_worker = threading.Event()
    close_in_progress = threading.Event()
    retired: list[object] = []
    factory_threads: list[int] = []
    retirement_threads: list[int] = []
    retirement_capacity: list[tuple[int, int]] = []

    def realize() -> object:
        factory_threads.append(threading.get_ident())
        return product

    def retire(candidate: object) -> None:
        retirement_threads.append(threading.get_ident())
        retirement_capacity.append((lifecycle.blocking_in_flight, lifecycle.retained))
        retired.append(candidate)

    if ownership_phase == "pre_commit":
        original_release = lifecycle.release_blocking_permit

        def pause_before_commit(permit: PipelineBlockingPermit) -> None:
            boundary_entered.set()
            assert allow_worker.wait(timeout=1.0)
            original_release(permit)

        monkeypatch.setattr(lifecycle, "release_blocking_permit", pause_before_commit)
    else:
        original_mark_finished = side_effects._LifecycleBlockingCall.mark_finished

        def pause_after_commit(call: side_effects._LifecycleBlockingCall) -> None:
            boundary_entered.set()
            assert allow_worker.wait(timeout=1.0)
            original_mark_finished(call)

        monkeypatch.setattr(
            side_effects._LifecycleBlockingCall,
            "mark_finished",
            pause_after_commit,
        )

    original_abandon = side_effects._LifecycleBlockingCall.abandon

    def observe_close_handler(
        call: side_effects._LifecycleBlockingCall,
        *,
        discard_before_start: bool = True,
    ) -> bool:
        abandoned = original_abandon(call, discard_before_start=discard_before_start)
        if close_in_progress.is_set():
            close_handler_entered.set()
        return abandoned

    monkeypatch.setattr(side_effects._LifecycleBlockingCall, "abandon", observe_close_handler)
    operation = blocking_work.realize_owned(
        realize,
        validate=lambda _product: None,
        adopt=lambda _product: None,
        retire=retire,
        reason_code=f"generator_exit_{ownership_phase}",
    )
    realization = asyncio.create_task(operation)

    def release_worker_after_close_handler() -> None:
        assert close_handler_entered.wait(timeout=1.0)
        allow_worker.set()

    release_thread = threading.Thread(
        target=release_worker_after_close_handler,
        name="release-generator-exit-worker",
    )
    try:
        assert await asyncio.to_thread(boundary_entered.wait, 1.0)
        release_thread.start()
        close_in_progress.set()
        close_error: BaseException | None = None
        try:
            operation.close()
        except BaseException as exc:
            close_error = exc
        finally:
            close_in_progress.clear()
            allow_worker.set()
        await asyncio.to_thread(release_thread.join, 1.0)
        assert release_thread.is_alive() is False

        deadline = asyncio.get_running_loop().time() + 1.0
        while (blocking_work.active or not realization.done()) and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.001)

        assert close_error is None
        assert realization.done()
        if not realization.cancelled():
            task_error = realization.exception()
            assert not (isinstance(task_error, RuntimeError) and str(task_error) == "coroutine ignored GeneratorExit")
        assert retired == [product]
        assert retirement_threads == factory_threads
        assert retirement_capacity == [(0, 1)]
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert blocking_work.active == 0
    finally:
        allow_worker.set()
        if release_thread.is_alive():
            await asyncio.to_thread(release_thread.join, 1.0)


def test_closed_requester_loop_retires_escrow_before_ownership_commit() -> None:
    loop = asyncio.new_event_loop()
    future: asyncio.Future[object] = loop.create_future()
    product = object()
    retired: list[object] = []
    call = side_effects._LifecycleBlockingCall(
        lambda: product,
        reason_code="closed_requester_loop",
        loop=loop,
        future=future,
        on_abandoned_result=retired.append,
        on_discarded=None,
        background=False,
        result_handoff_seconds=0.1,
        defer_result_publication=True,
    )
    call.execute()
    call.acknowledge_result_handoff(product)
    call.commit_result_claim_intent(product)
    loop.close()

    call.settle_result_ownership_after_release()

    assert retired == [product]


@pytest.mark.asyncio
async def test_second_terminal_mark_keeps_capacity_charged_without_blocking_requester_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    second_mark_entered = threading.Event()
    allow_second_mark = threading.Event()
    requester_loop_progressed = threading.Event()
    worker_unregistered = threading.Event()
    mark_count = 0
    mark_lock = threading.Lock()
    heartbeat_responsive: list[bool] = []
    charged_state: list[tuple[int, int, int]] = []
    requester_loop = asyncio.get_running_loop()
    original_mark_finished = side_effects._LifecycleBlockingCall.mark_finished
    original_unregister = blocking_work._unregister

    def pause_second_mark(call: side_effects._LifecycleBlockingCall) -> None:
        nonlocal mark_count
        with mark_lock:
            mark_count += 1
            current_mark = mark_count
        if current_mark == 2:
            second_mark_entered.set()
            assert allow_second_mark.wait(timeout=1.0)
        original_mark_finished(call)

    def observe_unregister(call: side_effects._LifecycleBlockingCall) -> None:
        original_unregister(call)
        worker_unregistered.set()

    def observe_paused_terminal_mark() -> None:
        assert second_mark_entered.wait(timeout=1.0)
        charged_state.append(
            (
                lifecycle.blocking_in_flight,
                lifecycle.retained,
                blocking_work.active,
            )
        )
        requester_loop.call_soon_threadsafe(requester_loop_progressed.set)
        heartbeat_responsive.append(requester_loop_progressed.wait(timeout=0.2))
        allow_second_mark.set()

    monkeypatch.setattr(
        side_effects._LifecycleBlockingCall,
        "mark_finished",
        pause_second_mark,
    )
    monkeypatch.setattr(blocking_work, "_unregister", observe_unregister)
    observer = threading.Thread(
        target=observe_paused_terminal_mark,
        name="observe-paused-terminal-mark",
    )
    observer.start()
    try:
        assert (
            await blocking_work.realize_owned(
                lambda: product,
                validate=lambda _product: None,
                adopt=lambda _product: None,
                retire=lambda _product: None,
                reason_code="paused_terminal_publication",
            )
            is product
        )
        await asyncio.to_thread(observer.join, 1.0)
        assert observer.is_alive() is False
        assert heartbeat_responsive == [True]
        assert charged_state == [(0, 1, 1)]
        assert await asyncio.to_thread(worker_unregistered.wait, 1.0)
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert blocking_work.active == 0
    finally:
        allow_second_mark.set()
        if observer.is_alive():
            await asyncio.to_thread(observer.join, 1.0)


@pytest.mark.asyncio
async def test_release_race_retirement_failure_fences_with_transition_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    release_entered = threading.Event()
    allow_release = threading.Event()
    factory_threads: list[int] = []
    retirement_threads: list[int] = []
    fence_capacity: list[tuple[int, int]] = []
    original_release = lifecycle.release_blocking_permit
    original_fence = lifecycle.fence_runtime_fatal

    def realize() -> object:
        factory_threads.append(threading.get_ident())
        return product

    def fail_retirement(_candidate: object) -> None:
        retirement_threads.append(threading.get_ident())
        raise RuntimeError("synthetic post-release retirement failure")

    def pause_release(permit: PipelineBlockingPermit) -> None:
        if not permit.cleanup:
            release_entered.set()
            assert allow_release.wait(timeout=1.0)
        original_release(permit)

    def observe_fence(error: BaseException):
        fence_capacity.append((lifecycle.blocking_in_flight, lifecycle.retained))
        return original_fence(error)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", pause_release)
    monkeypatch.setattr(lifecycle, "fence_runtime_fatal", observe_fence)
    realization = asyncio.create_task(
        blocking_work.realize_owned(
            realize,
            validate=lambda _product: None,
            adopt=lambda _product: None,
            retire=fail_retirement,
            reason_code="post_release_retirement_failure",
        )
    )
    try:
        assert await asyncio.to_thread(release_entered.wait, 1.0)
        realization.cancel()
        allow_release.set()
        with pytest.raises(asyncio.CancelledError):
            await realization

        deadline = asyncio.get_running_loop().time() + 1.0
        while blocking_work.active and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.001)

        assert retirement_threads == factory_threads
        assert fence_capacity == [(0, 1)]
        assert lifecycle.runtime_fatal_circuit is not None
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert blocking_work.active == 0
    finally:
        allow_release.set()


@pytest.mark.asyncio
async def test_plain_async_result_is_not_published_before_worker_releases_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    release_entered = threading.Event()
    allow_release = threading.Event()
    original_release = lifecycle.release_blocking_permit

    def delayed_release(permit: PipelineBlockingPermit) -> None:
        release_entered.set()
        assert allow_release.wait(timeout=1.0)
        original_release(permit)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", delayed_release)
    result = asyncio.create_task(
        blocking_work.run(
            lambda: product,
            reason_code="plain_result_release_order",
        )
    )
    try:
        assert await asyncio.to_thread(release_entered.wait, 1.0)
        await asyncio.sleep(0)
        assert result.done() is False
        assert lifecycle.blocking_in_flight == 1

        allow_release.set()
        assert await result is product
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert blocking_work.active == 0
    finally:
        allow_release.set()
        if not result.done():
            result.cancel()
            await asyncio.gather(result, return_exceptions=True)


@pytest.mark.parametrize("requester_loop_state", ["paused", "closed"])
def test_deferred_result_handoff_retains_capacity_until_publication_and_thread_exit(
    monkeypatch: pytest.MonkeyPatch,
    requester_loop_state: str,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    requester_loop = asyncio.new_event_loop()
    product = object()
    worker_started = threading.Event()
    allow_worker_result = threading.Event()
    publication_entered = threading.Event()
    allow_publication = threading.Event()
    publication_finished = threading.Event()
    handoff_threads: list[threading.Thread] = []
    accounting: list[tuple[str, int, int]] = []
    retired: list[object] = []
    original_publish = side_effects._LifecycleBlockingCall.publish_deferred_result
    original_worker_thread = blocking_work._worker_thread

    def work() -> object:
        worker_started.set()
        assert allow_worker_result.wait(timeout=1.0)
        return product

    def observe_publication(call: side_effects._LifecycleBlockingCall) -> None:
        accounting.append(("entered", lifecycle.blocking_in_flight, blocking_work.active))
        publication_entered.set()
        assert allow_publication.wait(timeout=1.0)
        original_publish(call)
        accounting.append(("returned", lifecycle.blocking_in_flight, blocking_work.active))
        publication_finished.set()

    def capture_worker_thread(
        call: side_effects._LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
        *,
        thread_name: str = "tacit-lifecycle-blocking-work",
    ) -> threading.Thread:
        thread = original_worker_thread(call, permit, thread_name=thread_name)
        handoff_threads.append(thread)
        return thread

    monkeypatch.setattr(
        side_effects._LifecycleBlockingCall,
        "publish_deferred_result",
        observe_publication,
    )
    monkeypatch.setattr(blocking_work, "_worker_thread", capture_worker_thread)
    task = requester_loop.create_task(
        blocking_work.run(
            work,
            reason_code=f"{requester_loop_state}_deferred_result_handoff",
            on_abandoned_result=retired.append,
            defer_result_publication=True,
            result_handoff_seconds=0.02,
        )
    )

    async def wait_for_worker_start() -> None:
        deadline = requester_loop.time() + 1.0
        while not worker_started.is_set():
            if requester_loop.time() >= deadline:
                raise AssertionError("blocking worker did not start")
            await asyncio.sleep(0.001)

    try:
        requester_loop.run_until_complete(wait_for_worker_start())
        assert lifecycle.blocking_in_flight == 1
        assert blocking_work.active == 1
        if requester_loop_state == "closed":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                requester_loop.run_until_complete(task)
            requester_loop.close()

        allow_worker_result.set()
        assert publication_entered.wait(timeout=1.0)
        assert len(handoff_threads) == 1
        assert handoff_threads[0].is_alive() is True
        assert lifecycle.blocking_in_flight == 1
        assert blocking_work.active == 1
        allow_publication.set()
        assert publication_finished.wait(timeout=1.0)
        handoff_threads[0].join(timeout=1.0)
        assert handoff_threads[0].is_alive() is False

        assert accounting == [("entered", 1, 1), ("returned", 1, 1)]
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert blocking_work.active == 0
        assert retired == [product]

        if requester_loop_state == "paused":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                requester_loop.run_until_complete(task)
    finally:
        allow_worker_result.set()
        allow_publication.set()
        for thread in handoff_threads:
            thread.join(timeout=1.0)
        if not requester_loop.is_closed():
            if not task.done():
                task.cancel()
                requester_loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
            requester_loop.close()


@pytest.mark.parametrize("release_before_failure", [False, True])
@pytest.mark.asyncio
async def test_deferred_result_release_failure_fences_runtime_and_settles_waiter(
    monkeypatch: pytest.MonkeyPatch,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    captured_permits: list[PipelineBlockingPermit] = []
    retired: list[object] = []
    original_release = lifecycle.release_blocking_permit
    adopted = threading.Event()

    def retire_unadopted(candidate: object) -> None:
        if not adopted.is_set():
            retired.append(candidate)

    def fail_release(permit: PipelineBlockingPermit) -> None:
        captured_permits.append(permit)
        if release_before_failure:
            original_release(permit)
        raise RuntimeError("synthetic permit release failure")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", fail_release)
    realization = asyncio.create_task(
        blocking_work.realize_owned(
            lambda: product,
            validate=lambda _product: None,
            adopt=lambda _product: adopted.set(),
            retire=retire_unadopted,
            reason_code="deferred_result_release_failure",
        )
    )

    with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
        await asyncio.wait_for(realization, timeout=1.0)

    assert lifecycle.runtime_fatal_circuit is not None
    assert adopted.is_set()
    with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
        await blocking_work.run(lambda: None, reason_code="work_after_release_failure")
    assert retired == []
    assert blocking_work.active == 0

    if not release_before_failure:
        assert len(captured_permits) == 1
        original_release(captured_permits[0])
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.parametrize("release_before_failure", [False, True])
@pytest.mark.asyncio
async def test_plain_async_release_failure_fences_before_replacement_admission(
    monkeypatch: pytest.MonkeyPatch,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    captured_permits: list[PipelineBlockingPermit] = []
    replacement_attempts: list[PipelineBlockingPermit | None] = []
    replacement_releases: list[PipelineBlockingPermit] = []
    original_release = lifecycle.release_blocking_permit

    def fail_release(permit: PipelineBlockingPermit) -> None:
        captured_permits.append(permit)
        if release_before_failure:
            original_release(permit)
            replacement = lifecycle.try_acquire_blocking_permit()
            replacement_attempts.append(replacement)
            if replacement is not None:
                replacement_releases.append(replacement)
                original_release(replacement)
        raise RuntimeError("synthetic permit release failure")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", fail_release)
    with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
        await asyncio.wait_for(
            blocking_work.run(
                lambda: "must-not-escape",
                reason_code="plain_result_release_failure",
            ),
            timeout=1.0,
        )

    assert lifecycle.runtime_fatal_circuit is not None
    await _wait_for_worker_zero(blocking_work)
    assert blocking_work.active == 0
    if release_before_failure:
        assert replacement_attempts == [None]
        assert replacement_releases == []
    else:
        original_release(captured_permits[0])
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.parametrize("release_before_failure", [False, True])
@pytest.mark.asyncio
async def test_inherited_child_task_cannot_reserve_during_release_transition(
    monkeypatch: pytest.MonkeyPatch,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(2, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    release_transition_started = asyncio.Event()
    replacement_finished = threading.Event()
    replacement_outcomes: list[str] = []
    captured_permits: list[PipelineBlockingPermit] = []
    original_release = lifecycle.release_blocking_permit
    loop = asyncio.get_running_loop()

    async with lifecycle.slot():

        async def reserve_from_inherited_lease() -> None:
            await release_transition_started.wait()
            try:
                replacement = await lifecycle.acquire_blocking_permit()
            except PipelineAdmissionRejected as exc:
                replacement_outcomes.append(exc.reason_code)
            else:
                replacement_outcomes.append("admitted")
                original_release(replacement)
            finally:
                replacement_finished.set()

        inherited_child = asyncio.create_task(reserve_from_inherited_lease())

        def fail_release(permit: PipelineBlockingPermit) -> None:
            captured_permits.append(permit)
            if release_before_failure:
                original_release(permit)
            loop.call_soon_threadsafe(release_transition_started.set)
            assert replacement_finished.wait(timeout=1.0)
            raise RuntimeError("synthetic permit release failure")

        monkeypatch.setattr(lifecycle, "release_blocking_permit", fail_release)
        with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
            await asyncio.wait_for(
                blocking_work.run(
                    lambda: "must-not-escape",
                    reason_code="inherited_lease_release_transition",
                ),
                timeout=1.0,
            )
        await inherited_child

        assert replacement_outcomes == ["pipeline_admission_queue_full"]
        assert lifecycle.runtime_fatal_circuit is not None
        assert blocking_work.active == 0
        if not release_before_failure:
            original_release(captured_permits[0])

    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.parametrize("release_before_failure", [False, True])
@pytest.mark.asyncio
async def test_cleanup_and_release_failure_still_settles_waiter(
    monkeypatch: pytest.MonkeyPatch,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    captured_permits: list[PipelineBlockingPermit] = []
    original_release = lifecycle.release_blocking_permit

    def fail_release(permit: PipelineBlockingPermit) -> None:
        captured_permits.append(permit)
        if release_before_failure:
            original_release(permit)
        raise RuntimeError("synthetic permit release failure")

    def reject(_product: object) -> None:
        raise ValueError("synthetic validation failure")

    def fail_retirement(_product: object) -> None:
        raise RuntimeError("synthetic retirement failure")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", fail_release)
    with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
        await asyncio.wait_for(
            blocking_work.realize_owned(
                object,
                validate=reject,
                adopt=lambda _product: None,
                retire=fail_retirement,
                reason_code="cleanup_and_release_failure",
            ),
            timeout=1.0,
        )

    assert lifecycle.runtime_fatal_circuit is not None
    assert blocking_work.active == 0
    if not release_before_failure:
        original_release(captured_permits[0])
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.parametrize("release_before_failure", [False, True])
def test_sync_realization_never_returns_product_after_release_failure(
    monkeypatch: pytest.MonkeyPatch,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    product = object()
    adopted: list[object] = []
    retired: list[object] = []
    captured_permits: list[PipelineBlockingPermit] = []
    original_release = lifecycle.release_blocking_permit

    def fail_release(permit: PipelineBlockingPermit) -> None:
        captured_permits.append(permit)
        if release_before_failure:
            original_release(permit)
        raise RuntimeError("synthetic permit release failure")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", fail_release)
    with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
        blocking_work.realize_owned_sync(
            lambda: product,
            validate=lambda _product: None,
            adopt=adopted.append,
            retire=retired.append,
            reason_code="sync_release_failure",
        )

    assert adopted == [product]
    assert retired == [product]
    assert lifecycle.runtime_fatal_circuit is not None
    assert blocking_work.active == 0
    if not release_before_failure:
        original_release(captured_permits[0])
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


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
                        adopt=lambda _product: None,
                        retire=lambda _product: None,
                        reason_code="normal_realization_from_cleanup_worker",
                    )
                )
            else:
                blocking_work.realize_owned_sync(
                    factory,
                    validate=lambda _product: None,
                    adopt=lambda _product: None,
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
                adopt=lambda _product: None,
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
            adopt=lambda _product: None,
            retire=lambda _product: None,
            reason_code="sync_call_construction_failure",
        )

    assert release_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0


@pytest.mark.parametrize("release_before_failure", [False, True])
def test_sync_call_construction_release_failure_fences_runtime(
    monkeypatch: pytest.MonkeyPatch,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    captured_permits: list[PipelineBlockingPermit] = []
    original_release = lifecycle.release_blocking_permit

    def fail_release(permit: PipelineBlockingPermit) -> None:
        captured_permits.append(permit)
        if release_before_failure:
            original_release(permit)
        raise RuntimeError("synthetic construction rollback release failure")

    def reject_call_construction(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("synthetic call construction failure")

    monkeypatch.setattr(lifecycle, "release_blocking_permit", fail_release)
    monkeypatch.setattr(side_effects, "_LifecycleBlockingCall", reject_call_construction)

    with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
        blocking_work.realize_owned_sync(
            object,
            validate=lambda _product: None,
            adopt=lambda _product: None,
            retire=lambda _product: None,
            reason_code="sync_call_construction_release_failure",
        )

    assert lifecycle.runtime_fatal_circuit is not None
    assert len(captured_permits) == 1
    if not release_before_failure:
        original_release(captured_permits[0])
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0


@pytest.mark.parametrize("failure_index", [0, 1, 2])
@pytest.mark.parametrize("release_before_failure", [False, True])
def test_cleanup_group_rollback_attempts_every_permit_after_release_failure(
    monkeypatch: pytest.MonkeyPatch,
    failure_index: int,
    release_before_failure: bool,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permits = blocking_work.reserve_cleanup_permits(3)
    assert permits is not None
    original_release = lifecycle.release_blocking_permit
    attempts: list[int] = []
    released: set[int] = set()

    def flaky_release(permit: PipelineBlockingPermit) -> None:
        index = len(attempts)
        attempts.append(permit.permit_id)
        if index == failure_index:
            if release_before_failure:
                original_release(permit)
                released.add(permit.permit_id)
            raise RuntimeError("synthetic group rollback release failure")
        original_release(permit)
        released.add(permit.permit_id)

    monkeypatch.setattr(lifecycle, "release_blocking_permit", flaky_release)
    with pytest.raises(RuntimeOwnershipError, match="capacity release failed"):
        blocking_work.run_reserved_background_group(
            (),
            permits,
            reason_code="cleanup_group_release_failure",
        )

    assert attempts == [permit.permit_id for permit in permits]
    assert lifecycle.runtime_fatal_circuit is not None
    assert blocking_work.active == 0
    for permit in permits:
        if permit.permit_id not in released:
            original_release(permit)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


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
