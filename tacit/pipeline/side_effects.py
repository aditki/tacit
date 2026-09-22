"""Safe non-critical pipeline side effects."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Callable, Iterable
from itertools import islice
from typing import TYPE_CHECKING, Any

import structlog

from tacit.backends.base import DashboardBackend
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.models.schemas import DashboardSpec, DashRequest, Intent
from tacit.pipeline.recording import query_history_payload
from tacit.pipeline_admission import PipelineAdmissionController, PipelineBlockingPermit
from tacit.runtime_ownership import (
    DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS,
    validate_runtime_cleanup_grace_seconds,
)

if TYPE_CHECKING:
    from tacit.pipeline_admission import PipelineAdmissionLease

logger = structlog.get_logger()

DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS = DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS
validate_cleanup_grace_seconds = validate_runtime_cleanup_grace_seconds

_MAX_TERMINAL_FAILURE_FIELD_LENGTH = 128


def terminal_cleanup_failure(
    primary_error: BaseException | None,
    cleanup_error: BaseException | str,
    *,
    reason_code: str,
    message: str,
    retain_capacity: bool = True,
) -> RuntimeOwnershipError:
    """Build one stable cleanup failure without retaining cleanup exception data.

    The primary error remains the causal exception. Cleanup contributes only
    bounded type and reason metadata so durable audit and logs cannot retain an
    exception message, traceback, resource value, or path.
    """
    cleanup_error_type = (cleanup_error if isinstance(cleanup_error, str) else type(cleanup_error).__name__)[
        :_MAX_TERMINAL_FAILURE_FIELD_LENGTH
    ]
    failure = RuntimeOwnershipError(message)
    setattr(failure, "cleanup_reason_code", reason_code[:_MAX_TERMINAL_FAILURE_FIELD_LENGTH])
    setattr(failure, "cleanup_error_type", cleanup_error_type)
    # Terminal cleanup failure always revokes runtime authority before the
    # realizing worker releases capacity. Keep the argument temporarily for
    # source compatibility, but never let a caller weaken that invariant.
    setattr(failure, "cleanup_retains_capacity", True)
    if primary_error is not None:
        failure.__cause__ = primary_error
        failure.__suppress_context__ = True
    return failure


def _retains_cleanup_capacity(error: BaseException | None) -> bool:
    return bool(error is not None and getattr(error, "cleanup_retains_capacity", False))


def _realization_cleanup_message(reason_code: str) -> str:
    if reason_code == "backend:dashboard_realization":
        return "Pipeline backend realization failed because cleanup failed"
    return "Pipeline blocking work cleanup failed"


def _consume_background_task(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except BaseException:
        return


_BLOCKING_RESULT_MISSING = object()


class _WorkerStartError(RuntimeError):
    def __init__(self, error_type: str) -> None:
        super().__init__(error_type)
        self.error_type = error_type


class _WorkerStartDecision:
    """Keep worker execution gated until thread submission is committed."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._decision = "pending"
        self._decided = threading.Event()

    def commit(self) -> None:
        with self._lock:
            if self._decision != "pending":
                raise RuntimeError("blocking worker start decision was already finalized")
            self._decision = "committed"
            self._decided.set()

    def abort(self) -> None:
        with self._lock:
            if self._decision == "pending":
                self._decision = "aborted"
                self._decided.set()

    def wait_for_commit(self) -> bool:
        self._decided.wait()
        with self._lock:
            return self._decision == "committed"


class _LifecycleBlockingCall:
    """Bridge a worker result without making its transport own capacity."""

    def __init__(
        self,
        function: Callable[[], Any],
        *,
        reason_code: str,
        loop: asyncio.AbstractEventLoop | None,
        future: asyncio.Future[Any] | None,
        on_abandoned_result: Callable[[Any], None] | None,
        on_discarded: Callable[[], None] | None,
        background: bool,
        result_handoff_seconds: float,
        start_decision: _WorkerStartDecision | None = None,
    ) -> None:
        self._function: Callable[[], Any] | None = function
        self._reason_code = reason_code
        self._loop = loop
        self._future = future
        self._on_abandoned_result = on_abandoned_result
        self._on_discarded = on_discarded
        self._background = background
        self._synchronous = loop is None and not background
        self._result_handoff_seconds = result_handoff_seconds
        self._start_decision = start_decision or _WorkerStartDecision()
        self._lock = threading.Lock()
        self._abandoned = background
        self._result_expired = False
        self._started = False
        self._worker_claimed = False
        self._release_claimed = False
        self._discarded = False
        self._result: Any = _BLOCKING_RESULT_MISSING
        self._error: BaseException | None = None
        self._terminal_cleanup_error: RuntimeOwnershipError | None = None
        self._terminal_result_published = False
        self.started = threading.Event()
        self.completed = threading.Event()
        self.finished = threading.Event()
        self._result_handoff = threading.Event()

    def commit_start(self) -> None:
        self._start_decision.commit()

    def abort_start(self) -> None:
        self._start_decision.abort()

    def wait_for_start_commit(self) -> bool:
        return self._start_decision.wait_for_commit()

    def claim_worker(self) -> bool:
        """Claim execution once when thread startup has an uncertain outcome."""
        with self._lock:
            if self._worker_claimed:
                return False
            self._worker_claimed = True
            return True

    def claim_release(self) -> bool:
        """Select exactly one permit-release owner across startup races."""
        with self._lock:
            if self._release_claimed:
                return False
            self._release_claimed = True
            return True

    def execute(self) -> None:
        """Run the submitted function and publish only its result transport."""
        with self._lock:
            if self._discarded:
                return
            self._started = True
            function = self._function
        self.started.set()
        if function is None:
            return
        try:
            result = function()
        except BaseException as exc:
            with self._lock:
                self._error = exc
                if isinstance(exc, RuntimeOwnershipError) and _retains_cleanup_capacity(exc):
                    self._terminal_cleanup_error = exc
        else:
            with self._lock:
                self._result = result
        finally:
            with self._lock:
                self._function = None
                terminal_cleanup_pending = self._terminal_cleanup_error is not None
        if terminal_cleanup_pending:
            return
        if self._publish_or_cleanup():
            if not self._result_handoff.wait(timeout=self._result_handoff_seconds):
                self._expire_result_transport()
            else:
                self._cleanup_abandoned_result()

    def abandon(self, *, discard_before_start: bool = True) -> None:
        """Mark the async result transport abandoned without touching capacity."""
        discarded_callback: Callable[[], None] | None = None
        with self._lock:
            self._abandoned = True
            if discard_before_start and not self._started and not self._discarded:
                self._discarded = True
                self._function = None
                discarded_callback = self._on_discarded
                self._on_discarded = None
            if self._result is not _BLOCKING_RESULT_MISSING and self._on_abandoned_result is not None:
                self._result_handoff.set()
        if discarded_callback is not None:
            self._run_callback(discarded_callback, event="pipeline_blocking_work_discard_failed")

    def discard(self) -> None:
        self.abandon(discard_before_start=True)

    def fail_to_start(self, _error_type: str) -> None:
        self.abort_start()
        self.discard()

    def wait_for_sync_result(self) -> Any:
        """Adopt one worker result without depending on an asyncio loop."""
        self.completed.wait()
        with self._lock:
            error = self._error
            self._error = None
            result = self._result
        if error is not None:
            raise error
        if result is _BLOCKING_RESULT_MISSING:
            raise RuntimeError("blocking worker completed without a result")
        return self.claim(result)

    def claim(self, result: Any) -> Any:
        """Atomically transfer an escrowed result to its asyncio consumer."""
        with self._lock:
            terminal_cleanup_error = self._terminal_cleanup_error
            if terminal_cleanup_error is not None:
                raise terminal_cleanup_error
            if self._abandoned or self._result_expired:
                raise RuntimeError("blocking result transport expired before adoption")
            if self._result is _BLOCKING_RESULT_MISSING or self._result is not result:
                raise RuntimeError("blocking result transport ownership was corrupted")
            self._result = _BLOCKING_RESULT_MISSING
            self._on_abandoned_result = None
            self._result_handoff.set()
        return result

    def _publish_or_cleanup(self) -> bool:
        cleanup: Callable[[Any], None] | None = None
        result: Any = _BLOCKING_RESULT_MISSING
        with self._lock:
            abandoned = self._abandoned
            if abandoned and self._result is not _BLOCKING_RESULT_MISSING:
                result = self._result
                self._result = _BLOCKING_RESULT_MISSING
                cleanup = self._on_abandoned_result
                self._on_abandoned_result = None
            loop = self._loop
        if cleanup is not None and result is not _BLOCKING_RESULT_MISSING:
            self._run_result_cleanup(cleanup, result)
        if abandoned:
            self._result_handoff.set()
            self._log_background_error()
            self.completed.set()
            return False
        if self._synchronous:
            with self._lock:
                escrowed = self._on_abandoned_result is not None and self._result is not _BLOCKING_RESULT_MISSING
            self.completed.set()
            return escrowed
        if loop is None or loop.is_closed():
            self.abandon(discard_before_start=False)
            self._cleanup_abandoned_result()
            self.completed.set()
            return False
        try:
            loop.call_soon_threadsafe(self._deliver)
        except RuntimeError:
            self.abandon(discard_before_start=False)
            self._cleanup_abandoned_result()
            self.completed.set()
            return False
        with self._lock:
            return self._result is not _BLOCKING_RESULT_MISSING and self._on_abandoned_result is not None

    def _deliver(self) -> None:
        result: Any = _BLOCKING_RESULT_MISSING
        error: BaseException | None = None
        transport_expired = False
        future: asyncio.Future[Any] | None
        with self._lock:
            future = self._future
            abandoned = self._abandoned or future is None or future.done()
            transport_expired = self._result_expired
            escrowed = self._on_abandoned_result is not None and self._result is not _BLOCKING_RESULT_MISSING
            if abandoned:
                if escrowed:
                    self._result_handoff.set()
            else:
                result = self._result
                error = self._error
                if not escrowed:
                    self._result = _BLOCKING_RESULT_MISSING
                self._error = None
        if future is None or future.done():
            return
        if transport_expired:
            future.set_exception(RuntimeError("blocking result transport expired before adoption"))
        elif error is not None:
            future.set_exception(error)
        elif result is _BLOCKING_RESULT_MISSING:
            future.set_exception(RuntimeError("blocking worker completed without a result"))
        else:
            future.set_result(result)

    def _expire_result_transport(self) -> None:
        with self._lock:
            if self._result is _BLOCKING_RESULT_MISSING or self._on_abandoned_result is None:
                return
            self._result_expired = True
            self._abandoned = True
        logger.warning(
            "pipeline_blocking_result_handoff_expired",
            reason_code=self._reason_code,
            handoff_seconds=self._result_handoff_seconds,
        )
        self._cleanup_abandoned_result()

    def _cleanup_abandoned_result(self) -> None:
        cleanup: Callable[[Any], None] | None = None
        result: Any = _BLOCKING_RESULT_MISSING
        with self._lock:
            if not self._abandoned and not self._result_expired:
                return
            if self._result is not _BLOCKING_RESULT_MISSING:
                result = self._result
                self._result = _BLOCKING_RESULT_MISSING
                cleanup = self._on_abandoned_result
                self._on_abandoned_result = None
            self._result_handoff.set()
        if cleanup is not None and result is not _BLOCKING_RESULT_MISSING:
            self._run_result_cleanup(cleanup, result)

    def _log_background_error(self) -> None:
        with self._lock:
            error = self._error
            self._error = None
        if error is not None and self._background:
            logger.warning(
                "pipeline_blocking_work_failed",
                reason_code=self._reason_code,
                error_type=type(error).__name__,
            )

    def _run_result_cleanup(self, cleanup: Callable[[Any], None], result: Any) -> None:
        try:
            cleanup(result)
        except BaseException as exc:
            primary_error = RuntimeError("blocking result transport expired before adoption")
            terminal_error = terminal_cleanup_failure(
                primary_error,
                exc,
                reason_code=self._reason_code,
                message="Pipeline blocking work cleanup failed",
                retain_capacity=True,
            )
            with self._lock:
                self._terminal_cleanup_error = terminal_error
            logger.warning(
                "pipeline_blocking_work_cleanup_failed",
                reason_code=self._reason_code,
                error_type=type(exc).__name__,
            )

    def terminal_cleanup_error(self) -> RuntimeOwnershipError | None:
        """Return the bounded terminal failure used to fatal-fence the runtime."""
        with self._lock:
            return self._terminal_cleanup_error

    def publish_terminal_result(self) -> None:
        """Publish a terminal failure only after worker-owned finalization."""
        with self._lock:
            if self._terminal_cleanup_error is None:
                raise RuntimeError("blocking call has no terminal cleanup result")
            if self._terminal_result_published:
                return
            self._terminal_result_published = True
        self._publish_or_cleanup()

    def revoke_executable_authority(self) -> None:
        """Drop every call-owned handle before its runtime permit is released."""
        with self._lock:
            self._function = None
            self._result = _BLOCKING_RESULT_MISSING
            self._on_abandoned_result = None
            self._on_discarded = None

    def _run_callback(self, callback: Callable[[], None], *, event: str) -> None:
        try:
            callback()
        except BaseException as exc:
            logger.warning(
                event,
                reason_code=self._reason_code,
                error_type=type(exc).__name__,
            )


class LifecycleOwnedBlockingWork:
    """Submit blocking work only after its runtime controller grants a permit."""

    def __init__(self, lifecycle: PipelineAdmissionController | None = None) -> None:
        self._lock = threading.Lock()
        self._lifecycle: PipelineAdmissionController | None = None
        self._runtime_identity: str | None = None
        self._workers: set[_LifecycleBlockingCall] = set()
        if lifecycle is not None:
            self.bind(lifecycle)

    @property
    def active(self) -> int:
        """Return submitted workers that have not yet exited."""
        with self._lock:
            return len(self._workers)

    def bind(self, lifecycle: PipelineAdmissionController) -> None:
        """Bind once to one exact controller and its immutable runtime identity."""
        runtime_identity = lifecycle.runtime_identity
        if not runtime_identity:
            raise RuntimeOwnershipError("Blocking work requires a runtime-owned admission controller")
        with self._lock:
            if self._lifecycle is None:
                if self._workers:
                    raise RuntimeOwnershipError("Blocking work cannot change owners while active")
                self._lifecycle = lifecycle
                self._runtime_identity = runtime_identity
                return
            if self._lifecycle is not lifecycle or self._runtime_identity != runtime_identity:
                raise RuntimeOwnershipError("Blocking work belongs to another runtime lifecycle")

    async def run[Result](
        self,
        function: Callable[[], Result],
        *,
        reason_code: str,
        on_abandoned_result: Callable[[Result], None] | None = None,
        on_discarded: Callable[[], None] | None = None,
        cancel_pending: bool = True,
        cleanup: bool = False,
        result_handoff_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
        timeout_seconds: float | None = None,
    ) -> Result:
        """Reserve capacity, submit one thread, and use asyncio only for its result."""
        lifecycle = self._require_lifecycle()
        if not cleanup:
            lifecycle.raise_if_runtime_fatal()
        if lifecycle.current_thread_can_reuse_blocking_capacity(cleanup=cleanup):
            return function()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Result] = loop.create_future()
        call = _LifecycleBlockingCall(
            function,
            reason_code=reason_code,
            loop=loop,
            future=future,
            on_abandoned_result=on_abandoned_result,
            on_discarded=on_discarded,
            background=False,
            result_handoff_seconds=validate_cleanup_grace_seconds(result_handoff_seconds),
        )
        try:
            if cleanup:
                cleanup_permits = await lifecycle.acquire_cleanup_permits(1)
                permit = cleanup_permits[0]
            else:
                permit = await lifecycle.acquire_blocking_permit(
                    timeout_seconds=timeout_seconds,
                )
        except BaseException:
            call.discard()
            raise
        try:
            self._register_and_start(call, permit)
        except _WorkerStartError as exc:
            raise RuntimeError(f"blocking worker could not start ({exc.error_type})") from None
        try:
            result = await future
            if on_abandoned_result is not None:
                return call.claim(result)
            return result
        except asyncio.CancelledError:
            call.abandon(discard_before_start=cancel_pending)
            logger.warning(
                "pipeline_blocking_work_cancelled",
                reason_code=reason_code,
                admission_retained=True,
            )
            raise

    async def realize_owned[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        retire: Callable[[Product], Any],
        reason_code: str,
        result_handoff_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
        timeout_seconds: float | None = None,
    ) -> Product:
        """Create, validate, escrow, and retire one product on its admitted worker."""
        lifecycle = self._require_lifecycle()
        lifecycle.raise_if_runtime_fatal()
        if lifecycle.current_thread_can_reuse_blocking_capacity(cleanup=False):
            return await self._realize_owned_on_current_worker(
                factory,
                validate=validate,
                retire=retire,
                reason_code=reason_code,
            )
        operation, retire_product = self._owned_realization(
            factory,
            validate=validate,
            retire=retire,
            reason_code=reason_code,
        )
        return await self.run(
            operation,
            reason_code=reason_code,
            on_abandoned_result=retire_product,
            result_handoff_seconds=result_handoff_seconds,
            timeout_seconds=timeout_seconds,
        )

    def realize_owned_sync[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        retire: Callable[[Product], Any],
        reason_code: str,
        result_handoff_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
    ) -> Product:
        """Synchronously adopt an admitted product without using the caller loop."""
        lifecycle = self._require_lifecycle()
        lifecycle.raise_if_runtime_fatal()
        operation, retire_product = self._owned_realization(
            factory,
            validate=validate,
            retire=retire,
            reason_code=reason_code,
        )
        if lifecycle.current_thread_can_reuse_blocking_capacity(cleanup=False):
            return operation()
        permit = lifecycle.try_acquire_blocking_permit()
        if permit is None:
            raise PipelineAdmissionRejected("pipeline_admission_queue_full")
        try:
            call = _LifecycleBlockingCall(
                operation,
                reason_code=reason_code,
                loop=None,
                future=None,
                on_abandoned_result=retire_product,
                on_discarded=None,
                background=False,
                result_handoff_seconds=validate_cleanup_grace_seconds(result_handoff_seconds),
            )
        except BaseException:
            lifecycle.release_blocking_permit(permit)
            raise
        try:
            self._register_and_start(call, permit)
        except _WorkerStartError as exc:
            raise RuntimeError(f"blocking worker could not start ({exc.error_type})") from None
        try:
            return call.wait_for_sync_result()
        except BaseException:
            call.abandon(discard_before_start=False)
            raise
        finally:
            call.finished.wait()

    def run_background(
        self,
        function: Callable[[], Any],
        *,
        reason_code: str,
    ) -> bool:
        """Transfer cleanup only after reserving its complete execution owner."""
        permits = self.reserve_cleanup_permits(1)
        if permits is None:
            logger.warning(
                "pipeline_blocking_work_rejected",
                reason_code=reason_code,
            )
            return False
        return self.run_reserved_background_group(
            (function,),
            permits,
            reason_code=reason_code,
            thread_name="tacit-lifecycle-blocking-work",
        )

    def reserve_cleanup_permits(self, count: int) -> tuple[PipelineBlockingPermit, ...] | None:
        """Reserve a complete cleanup group before any resource is detached."""
        return self._require_lifecycle().try_acquire_cleanup_permits(count)

    def run_reserved_background_group(
        self,
        functions: Iterable[Callable[[], Any]],
        permits: tuple[PipelineBlockingPermit, ...],
        *,
        reason_code: str,
        thread_name: str = "tacit-lifecycle-cleanup-work",
    ) -> bool:
        """Start an atomically reserved cleanup group without loop-owned state."""
        calls: list[_LifecycleBlockingCall] = []
        threads: list[threading.Thread] = []
        start_decision = _WorkerStartDecision()
        lifecycle = self._require_lifecycle()
        lifecycle.validate_cleanup_permits(permits)
        try:
            selected_functions = tuple(islice(iter(functions), len(permits) + 1))
            if len(selected_functions) != len(permits):
                raise ValueError("cleanup functions and permits must have the same size")
            for function, permit in zip(selected_functions, permits, strict=True):
                call = _LifecycleBlockingCall(
                    function,
                    reason_code=reason_code,
                    loop=None,
                    future=None,
                    on_abandoned_result=None,
                    on_discarded=None,
                    background=True,
                    result_handoff_seconds=DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
                    start_decision=start_decision,
                )
                calls.append(call)
                self._register(call)
                threads.append(self._worker_thread(call, permit, thread_name=thread_name))
        except BaseException:
            start_decision.abort()
            self._rollback_group_construction(
                calls,
                permits,
            )
            raise

        start_error: BaseException | None = None
        for thread in threads:
            try:
                thread.start()
            except BaseException as exc:
                start_error = exc
                logger.warning(
                    "pipeline_blocking_worker_start_failed",
                    reason_code=reason_code,
                    error_type=type(exc).__name__,
                )
                break
        if start_error is not None:
            start_decision.abort()
            for call, permit in zip(calls, permits, strict=True):
                call.fail_to_start(type(start_error).__name__)
                self._release_reserved_call(call, permit)
            return False
        start_decision.commit()
        return True

    def _start_worker(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
    ) -> None:
        thread = self._worker_thread(call, permit)
        try:
            thread.start()
        except BaseException:
            call.abort_start()
            raise
        call.commit_start()

    def _register_and_start(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
    ) -> None:
        try:
            self._register(call)
        except BaseException:
            call.fail_to_start("registration_failure")
            self._release_reserved_call(call, permit)
            raise
        try:
            self._start_worker(call, permit)
        except BaseException as exc:
            error_type = type(exc).__name__
            call.fail_to_start(error_type)
            self._release_reserved_call(call, permit)
            raise _WorkerStartError(error_type) from exc

    def _worker_thread(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
        *,
        thread_name: str = "tacit-lifecycle-blocking-work",
    ) -> threading.Thread:
        return threading.Thread(
            target=self._run_reserved_call,
            args=(call, permit),
            name=thread_name,
            daemon=True,
        )

    def _run_reserved_call(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
    ) -> None:
        """Execute only committed work and release its reserved capacity once."""
        if not call.claim_worker():
            return
        if not call.wait_for_start_commit():
            self._release_reserved_call(call, permit)
            return
        lifecycle = self._require_lifecycle()
        try:
            with lifecycle.blocking_worker(permit):
                call.execute()
        finally:
            terminal_error = call.terminal_cleanup_error()
            if terminal_error is not None and _retains_cleanup_capacity(terminal_error):
                self._fatal_fence_reserved_call(call, permit, terminal_error)
            else:
                self._release_reserved_call(call, permit)

    def _fatal_fence_reserved_call(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
        terminal_error: RuntimeOwnershipError,
    ) -> None:
        """Revoke failed authority, fatal-fence its runtime, then release capacity."""
        if not call.claim_release():
            return
        lifecycle = self._require_lifecycle()
        try:
            call.revoke_executable_authority()
            lifecycle.fence_runtime_fatal(terminal_error)
            lifecycle.release_blocking_permit(permit)
        finally:
            self._unregister(call)
            call.finished.set()
        call.publish_terminal_result()

    def _release_reserved_call(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
    ) -> None:
        if not call.claim_release():
            return
        try:
            self._require_lifecycle().release_blocking_permit(permit)
        finally:
            self._unregister(call)
            call.finished.set()

    def _rollback_group_construction(
        self,
        calls: list[_LifecycleBlockingCall],
        permits: tuple[PipelineBlockingPermit, ...],
    ) -> None:
        for call in calls:
            call.fail_to_start("construction_failure")
        lifecycle = self._require_lifecycle()
        for index, permit in enumerate(permits):
            if index < len(calls):
                self._release_reserved_call(calls[index], permit)
                continue
            lifecycle.release_blocking_permit(permit)

    async def _realize_owned_on_current_worker[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        retire: Callable[[Product], Any],
        reason_code: str,
    ) -> Product:
        product = factory()
        try:
            validation = validate(product)
            if inspect.isawaitable(validation):
                await validation
        except BaseException as primary_error:
            try:
                retirement = retire(product)
                if inspect.isawaitable(retirement):
                    await retirement
            except BaseException as exc:
                logger.warning(
                    "pipeline_blocking_work_cleanup_failed",
                    reason_code=reason_code,
                    error_type=type(exc).__name__,
                )
                raise terminal_cleanup_failure(
                    primary_error,
                    exc,
                    reason_code=reason_code,
                    message=_realization_cleanup_message(reason_code),
                ) from primary_error
            raise
        return product

    def _owned_realization[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        retire: Callable[[Product], Any],
        reason_code: str,
    ) -> tuple[Callable[[], Product], Callable[[Product], None]]:
        def retire_product(product: Product) -> None:
            result = retire(product)
            self._complete_worker_awaitable(result)

        def realize_product() -> Product:
            product = factory()
            try:
                validation = validate(product)
                self._complete_worker_awaitable(validation)
            except BaseException as primary_error:
                try:
                    retire_product(product)
                except BaseException as exc:
                    logger.warning(
                        "pipeline_blocking_work_cleanup_failed",
                        reason_code=reason_code,
                        error_type=type(exc).__name__,
                    )
                    raise terminal_cleanup_failure(
                        primary_error,
                        exc,
                        reason_code=reason_code,
                        message=_realization_cleanup_message(reason_code),
                    ) from primary_error
                raise
            return product

        return realize_product, retire_product

    @staticmethod
    def _complete_worker_awaitable(result: Any) -> None:
        if not inspect.isawaitable(result):
            return

        async def await_result() -> None:
            await result

        with asyncio.Runner() as runner:
            runner.run(await_result())

    def _require_lifecycle(self) -> PipelineAdmissionController:
        with self._lock:
            lifecycle = self._lifecycle
            runtime_identity = self._runtime_identity
        if lifecycle is None or runtime_identity is None:
            raise RuntimeOwnershipError("Blocking work has no runtime admission owner")
        if lifecycle.runtime_identity != runtime_identity:
            raise RuntimeOwnershipError("Blocking work runtime identity changed")
        return lifecycle

    def _register(self, call: _LifecycleBlockingCall) -> None:
        with self._lock:
            self._workers.add(call)

    def _unregister(self, call: _LifecycleBlockingCall) -> None:
        with self._lock:
            self._workers.discard(call)


def begin_task_cancellation(
    task: asyncio.Task[Any],
    *,
    lifecycle: PipelineAdmissionController | None = None,
    lease: PipelineAdmissionLease | None = None,
) -> bool:
    """Transfer task cleanup to its admission owner and request cancellation."""
    if (lifecycle is None) != (lease is None):
        raise ValueError("lifecycle and lease must be supplied together")
    retained = False
    if lifecycle is not None:
        assert lease is not None
        retained = lifecycle.retain_task(lease, task)
    if not retained:
        task.add_done_callback(_consume_background_task)
    if not task.done():
        task.cancel()
    return retained


async def cancel_task_with_grace(
    task: asyncio.Task[Any],
    *,
    grace_seconds: float,
    reason_code: str,
    lifecycle: PipelineAdmissionController | None = None,
    lease: PipelineAdmissionLease | None = None,
) -> bool:
    """Cancel one task and wait only for the configured cleanup grace."""
    if (lifecycle is None) != (lease is None):
        raise ValueError("lifecycle and lease must be supplied together")
    grace = validate_cleanup_grace_seconds(grace_seconds)
    begin_task_cancellation(
        task,
        lifecycle=lifecycle,
        lease=lease,
    )
    done, _pending = await asyncio.wait({task}, timeout=grace)
    if task in done:
        _consume_background_task(task)
        return True
    logger.warning(
        "pipeline_task_cleanup_grace_exceeded",
        reason_code=reason_code,
        cleanup_grace_seconds=grace,
    )
    return False


def safe_finish_timeout_history(
    *,
    history_store_factory,
    request: DashRequest,
    timeout_seconds: int,
) -> None:
    """Best-effort timeout history persistence."""
    try:
        store = history_store_factory()
        tenant_id = request.tenant_id or "default"
        start_parameters = inspect.signature(store.start).parameters
        if "tenant_id" in start_parameters:
            inv_id = store.start(
                request.prompt,
                request.user_id,
                request.channel_id,
                tenant_id=tenant_id,
            )
        else:
            inv_id = store.start(request.prompt, request.user_id, request.channel_id)
        store.finish(
            inv_id,
            status="timeout",
            error=f"Timed out after {timeout_seconds}s",
            tenant_id=tenant_id,
        )
    except Exception:
        logger.warning("timeout_history_record_failed", exc_info=True)


def safe_record_provenance(
    *,
    feedback_store_factory,
    dashboard_uid: str,
    dashboard_url: str,
    request: DashRequest,
    intent: Intent,
    dashboard_spec: DashboardSpec,
    path_used: str,
) -> None:
    """Best-effort feedback provenance persistence."""
    try:
        feedback_store = feedback_store_factory()
        _, metrics_used = query_history_payload(dashboard_spec)
        feedback_store.record_provenance(
            dashboard_uid=dashboard_uid,
            prompt=request.prompt,
            problem_type=intent.problem_type,
            archetypes=[{"type": item.type, "confidence": item.confidence} for item in intent.archetypes],
            metrics_used=metrics_used,
            panel_count=len(dashboard_spec.panels),
            path_used=path_used,
            dashboard_url=dashboard_url,
            user_id=request.user_id,
            channel_id=request.channel_id,
            tenant_id=request.tenant_id or "default",
        )
    except Exception:
        logger.warning("provenance_record_failed", exc_info=True)


async def safe_close_backends(
    backends: Iterable[DashboardBackend],
    *,
    grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
    lifecycle: PipelineAdmissionController | None,
) -> None:
    """Close all backends under one retained runtime-owned cleanup task."""
    if lifecycle is None:
        raise RuntimeOwnershipError("Backend cleanup requires a runtime admission owner")
    grace = validate_cleanup_grace_seconds(grace_seconds)
    selected_backends = tuple(backends)
    if not selected_backends:
        return
    unfinished = set(range(len(selected_backends)))
    cleanup_start = asyncio.Event()

    async def close_backend(index: int, backend: DashboardBackend) -> None:
        try:
            await backend.close()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "backend_close_failed",
                reason_code="backend_close_failed",
                backend=backend.name,
                error_type=type(exc).__name__,
            )
        finally:
            unfinished.discard(index)

    async def close_all() -> None:
        await cleanup_start.wait()
        tasks = tuple(
            asyncio.create_task(
                close_backend(index, backend),
                name=f"tacit-close-backend-{backend.name}",
            )
            for index, backend in enumerate(selected_backends)
        )
        await asyncio.gather(*tasks, return_exceptions=True)

    cleanup = asyncio.create_task(close_all(), name="tacit-close-backends")
    try:
        retained = lifecycle.retain_current_task(cleanup)
    except BaseException:
        cleanup.cancel()
        await asyncio.gather(cleanup, return_exceptions=True)
        raise
    if not retained:
        cleanup.cancel()
        await asyncio.gather(cleanup, return_exceptions=True)
        raise RuntimeOwnershipError("Backend cleanup has no admitted runtime owner")
    cleanup_start.set()

    try:
        done, _ = await asyncio.wait({cleanup}, timeout=grace)
    except asyncio.CancelledError:
        raise
    if cleanup in done:
        _consume_background_task(cleanup)
        return
    for index in sorted(unfinished):
        backend = selected_backends[index]
        logger.warning(
            "backend_close_grace_exceeded",
            backend=backend.name,
            reason_code="backend_close_grace_exceeded",
            cleanup_grace_seconds=grace,
        )
