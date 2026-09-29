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
        defer_result_publication: bool = False,
        start_decision: _WorkerStartDecision | None = None,
    ) -> None:
        self._function: Callable[[], Any] | None = function
        self._reason_code = reason_code
        self._loop = loop
        self._future = future
        self._finished_future: asyncio.Future[None] | None = loop.create_future() if loop is not None else None
        self._requester_settlement_future: asyncio.Future[None] | None = (
            loop.create_future() if loop is not None else None
        )
        self._result_claim_ready_future: asyncio.Future[None] | None = (
            loop.create_future() if loop is not None else None
        )
        self._on_abandoned_result = on_abandoned_result
        self._on_discarded = on_discarded
        self._background = background
        self._synchronous = loop is None and not background
        self._defer_result_publication = defer_result_publication
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
        self._result_claim_intent = False
        self._result_claim_requested = False
        self._result_ownership_committed = False
        self._worker_finish_claimed = False
        self._worker_terminal_published = False
        self.started = threading.Event()
        self.completed = threading.Event()
        self.finished = threading.Event()
        self._result_handoff = threading.Event()
        self._result_claim_decision = threading.Event()
        self._result_final_decision = threading.Event()
        self._result_terminal_decision = threading.Event()

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
        if self._defer_result_publication:
            return
        self._publish_result_transport()

    def _publish_result_transport(self) -> None:
        """Publish or retire a completed result from its worker owner."""
        if self._publish_or_cleanup():
            if not self._result_handoff.wait(timeout=self._result_handoff_seconds):
                self._expire_result_transport()
            else:
                self._cleanup_abandoned_result()

    def publish_deferred_result(self) -> None:
        """Publish a foreground result while its worker still owns capacity."""
        if self._defer_result_publication:
            self._publish_result_transport()

    def retire_abandoned_result_before_final_settlement(self) -> None:
        """Retire an unreachable result before worker finalization is published."""
        with self._lock:
            loop = self._loop
            future = self._future
            owns_result_cleanup = self._on_abandoned_result is not None
            transport_unavailable = (
                not self._synchronous
                and not self._background
                and (
                    loop is None
                    or loop.is_closed()
                    or (not loop.is_running() and owns_result_cleanup)
                    or future is None
                    or future.cancelled()
                )
            )
        if transport_unavailable:
            self.abandon(discard_before_start=False)
        self._cleanup_abandoned_result()

    def abandon(self, *, discard_before_start: bool = True) -> bool:
        """Mark an uncommitted result abandoned without touching capacity."""
        discarded_callback: Callable[[], None] | None = None
        with self._lock:
            if self._result_ownership_committed:
                return False
            self._abandoned = True
            if discard_before_start and not self._started and not self._discarded:
                self._discarded = True
                self._function = None
                discarded_callback = self._on_discarded
                self._on_discarded = None
            if self._result is not _BLOCKING_RESULT_MISSING and self._on_abandoned_result is not None:
                self._result_handoff.set()
            self._result_claim_decision.set()
        if discarded_callback is not None:
            self._run_callback(discarded_callback, event="pipeline_blocking_work_discard_failed")
        return True

    def discard(self) -> None:
        self.abandon(discard_before_start=True)

    def fail_to_start(self, _error_type: str) -> None:
        self.abort_start()
        self.discard()

    def wait_for_sync_result(self) -> Any:
        """Adopt one worker result only after its release outcome is terminal."""
        self.completed.wait()
        with self._lock:
            result = self._result
        if result is not _BLOCKING_RESULT_MISSING:
            self._result_handoff.set()
        self.finished.wait()
        with self._lock:
            error = self._error
            result = self._result
        if error is not None:
            raise error
        if result is _BLOCKING_RESULT_MISSING:
            raise RuntimeError("blocking worker completed without a result")
        return self.claim(result)

    def mark_finished(self) -> None:
        """Resolve ownership and publish terminal readiness while charged."""
        with self._lock:
            if not self._worker_finish_claimed:
                self._worker_finish_claimed = True
                resolve_ownership = True
            elif not self._worker_terminal_published:
                self._worker_terminal_published = True
                resolve_ownership = False
            else:
                return
            loop = self._loop
            finished_future = self._finished_future
            requester_settlement_future = self._requester_settlement_future
            committed_escrow = (
                self._result_ownership_committed
                and self._result is not _BLOCKING_RESULT_MISSING
                and self._on_abandoned_result is not None
            )
            claim_requested = self._result_claim_requested
            loop_can_queue = loop is not None and not loop.is_closed()
            loop_available = loop.is_running() if loop_can_queue and loop is not None else False
            settlement_deliverable = (
                loop_can_queue
                and requester_settlement_future is not None
                and not requester_settlement_future.cancelled()
            )
        if resolve_ownership:
            if not committed_escrow:
                return
            if settlement_deliverable:
                assert loop is not None
                try:
                    loop.call_soon_threadsafe(self._deliver_requester_settlement)
                except RuntimeError:
                    loop_available = False
            if not loop_available:
                self.revoke_committed_result_transport()
            elif not self._result_final_decision.wait(timeout=self._result_handoff_seconds):
                self._expire_result_transport()
            self._cleanup_abandoned_result()
            return
        if committed_escrow and claim_requested:
            if loop_can_queue and finished_future is not None and not finished_future.done():
                assert loop is not None
                try:
                    loop.call_soon_threadsafe(self._deliver_finished)
                except RuntimeError:
                    loop_available = False
            if not loop_available:
                self.revoke_committed_result_transport()
            elif not self._result_terminal_decision.wait(timeout=self._result_handoff_seconds):
                self._expire_result_transport()
            self._cleanup_abandoned_result()
        self.finished.set()
        if loop is None or finished_future is None or loop.is_closed():
            return
        try:
            self._deliver_requester_settlement_soon(loop, requester_settlement_future)
            loop.call_soon_threadsafe(self._deliver_finished)
        except RuntimeError:
            return

    def _deliver_requester_settlement_soon(
        self,
        loop: asyncio.AbstractEventLoop,
        future: asyncio.Future[None] | None,
    ) -> None:
        if future is not None and not future.done():
            loop.call_soon_threadsafe(self._deliver_requester_settlement)

    async def wait_for_async_finish(self) -> None:
        """Wait until result handoff and worker permit settlement both finish."""
        with self._lock:
            requester_settlement_future = self._requester_settlement_future
        if requester_settlement_future is None:
            raise RuntimeError("blocking call has no asynchronous finalization transport")
        await requester_settlement_future
        with self._lock:
            terminal_cleanup_error = self._terminal_cleanup_error
        if terminal_cleanup_error is not None:
            raise terminal_cleanup_error

    async def wait_for_async_finish_after_claim_intent(self) -> None:
        """Await terminal readiness while preserving worker cleanup on loop loss."""
        with self._lock:
            finished_future = self._finished_future
        if finished_future is None:
            raise RuntimeError("blocking call has no protected finalization transport")
        while True:
            try:
                await asyncio.shield(finished_future)
            except asyncio.CancelledError:
                continue
            except GeneratorExit:
                self.revoke_committed_result_transport()
                raise
            break
        with self._lock:
            terminal_cleanup_error = self._terminal_cleanup_error
        if terminal_cleanup_error is not None:
            raise terminal_cleanup_error

    async def wait_for_result_claim_ready(self) -> None:
        """Wait until the worker is ready to linearize result ownership."""
        with self._lock:
            claim_ready_future = self._result_claim_ready_future
        if claim_ready_future is None:
            raise RuntimeError("blocking call has no asynchronous result-claim transport")
        await asyncio.shield(claim_ready_future)

    def commit_result_claim_intent(self, result: Any) -> None:
        """Commit the requester to receive the result after permit settlement."""
        with self._lock:
            if self._abandoned or self._result_expired:
                raise RuntimeError("blocking result transport expired before adoption")
            if self._result is _BLOCKING_RESULT_MISSING or self._result is not result:
                raise RuntimeError("blocking result transport ownership was corrupted")
            if self._result_claim_intent:
                raise RuntimeError("blocking result claim intent was already committed")
            self._result_claim_intent = True
            self._result_claim_decision.set()

    def prepare_result_claim_before_release(self) -> None:
        """Let cancellation or one requester claim intent win before release."""
        with self._lock:
            escrowed = (
                not self._synchronous
                and not self._background
                and self._result is not _BLOCKING_RESULT_MISSING
                and self._on_abandoned_result is not None
            )
            abandoned = self._abandoned or self._result_expired
            loop = self._loop
            claim_ready_future = self._result_claim_ready_future
        if not escrowed:
            return
        if abandoned:
            self._cleanup_abandoned_result()
            return
        if loop is None or claim_ready_future is None or loop.is_closed():
            self.abandon(discard_before_start=False)
            self._cleanup_abandoned_result()
            return
        try:
            loop.call_soon_threadsafe(self._deliver_result_claim_ready)
        except RuntimeError:
            self.abandon(discard_before_start=False)
            self._cleanup_abandoned_result()
            return
        if not self._result_claim_decision.wait(timeout=self._result_handoff_seconds):
            self._expire_result_transport()
        self._cleanup_abandoned_result()

    def settle_result_ownership_after_release(self) -> None:
        """Provisionally commit one claimant or retire a lost transport."""
        cleanup: Callable[[Any], None] | None = None
        result: Any = _BLOCKING_RESULT_MISSING
        with self._lock:
            if self._synchronous or self._background or self._on_abandoned_result is None:
                return
            if self._result is _BLOCKING_RESULT_MISSING:
                return
            requester_settlement_future = self._requester_settlement_future
            requester_cancelled = requester_settlement_future is not None and requester_settlement_future.cancelled()
            loop = self._loop
            transport_unavailable = loop is None or loop.is_closed() or not loop.is_running()
            if self._abandoned or self._result_expired or requester_cancelled or transport_unavailable:
                self._abandoned = True
                result = self._result
                cleanup = self._on_abandoned_result
                self._result = _BLOCKING_RESULT_MISSING
                self._on_abandoned_result = None
                self._result_handoff.set()
            elif not self._result_claim_intent:
                raise RuntimeError("blocking result release has no committed claimant")
            else:
                self._result_ownership_committed = True
        if cleanup is not None and result is not _BLOCKING_RESULT_MISSING:
            self._run_result_cleanup(cleanup, result)

    def finalize_committed_result(self, result: Any) -> Any:
        """Irreversibly claim a terminal-ready result without another suspension."""
        with self._lock:
            terminal_cleanup_error = self._terminal_cleanup_error
            ownership_committed = self._result_ownership_committed
            claim_requested = self._result_claim_requested
            abandoned = self._abandoned or self._result_expired
            result_matches = self._result is result and self._on_abandoned_result is not None
            if (
                terminal_cleanup_error is None
                and ownership_committed
                and claim_requested
                and not abandoned
                and result_matches
            ):
                self._result = _BLOCKING_RESULT_MISSING
                self._on_abandoned_result = None
                self._result_terminal_decision.set()
        if terminal_cleanup_error is not None:
            raise terminal_cleanup_error
        if not ownership_committed or not claim_requested or abandoned or not result_matches:
            raise RuntimeError("blocking result ownership did not settle")
        return result

    def request_committed_result_claim(self, result: Any) -> None:
        """Request terminal ownership while the worker retains cleanup escrow."""
        with self._lock:
            if self._terminal_cleanup_error is not None:
                raise self._terminal_cleanup_error
            if not self._result_ownership_committed or self._abandoned or self._result_expired:
                raise RuntimeError("blocking result ownership did not settle")
            if self._result is not result or self._on_abandoned_result is None:
                raise RuntimeError("blocking result transport ownership was corrupted")
            self._result_claim_requested = True
            self._result_final_decision.set()

    def revoke_committed_result_transport(self) -> bool:
        """Return a provisional requester claim to its worker cleanup owner."""
        with self._lock:
            if not self._result_ownership_committed:
                return False
            if self._result is _BLOCKING_RESULT_MISSING or self._on_abandoned_result is None:
                return False
            self._abandoned = True
            self._result_handoff.set()
            self._result_final_decision.set()
            self._result_terminal_decision.set()
            return True

    def acknowledge_result_handoff(self, result: Any) -> None:
        """Let the worker finish while result ownership remains in escrow."""
        with self._lock:
            if self._abandoned or self._result_expired:
                raise RuntimeError("blocking result transport expired before adoption")
            if self._result is _BLOCKING_RESULT_MISSING or self._result is not result:
                raise RuntimeError("blocking result transport ownership was corrupted")
            self._result_handoff.set()

    def _deliver_result_claim_ready(self) -> None:
        with self._lock:
            claim_ready_future = self._result_claim_ready_future
        if claim_ready_future is not None and not claim_ready_future.done():
            claim_ready_future.set_result(None)

    def _deliver_requester_settlement(self) -> None:
        with self._lock:
            requester_settlement_future = self._requester_settlement_future
        if requester_settlement_future is not None and not requester_settlement_future.done():
            requester_settlement_future.set_result(None)

    def _deliver_finished(self) -> None:
        with self._lock:
            finished_future = self._finished_future
        if finished_future is not None and not finished_future.done():
            finished_future.set_result(None)

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
            self._result_final_decision.set()
            self._result_terminal_decision.set()
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

    def replace_result_with_terminal_failure(self, error: RuntimeOwnershipError) -> None:
        """Retire unclaimed result transport and install one terminal failure."""
        cleanup: Callable[[Any], None] | None = None
        result: Any = _BLOCKING_RESULT_MISSING
        with self._lock:
            if self._result is not _BLOCKING_RESULT_MISSING:
                result = self._result
                cleanup = self._on_abandoned_result
            self._result = _BLOCKING_RESULT_MISSING
            self._on_abandoned_result = None
            self._terminal_cleanup_error = error
            self._error = error
            self._result_handoff.set()
        if cleanup is not None and result is not _BLOCKING_RESULT_MISSING:
            try:
                cleanup(result)
            except BaseException as cleanup_error:
                combined = terminal_cleanup_failure(
                    error,
                    cleanup_error,
                    reason_code="blocking_permit_release_failed",
                    message="Pipeline blocking capacity release failed",
                )
                with self._lock:
                    self._terminal_cleanup_error = combined
                    self._error = combined
                logger.warning(
                    "pipeline_blocking_work_cleanup_failed",
                    reason_code="blocking_permit_release_failed",
                    error_type=type(cleanup_error).__name__,
                )

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
        defer_result_publication: bool = False,
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
            defer_result_publication=defer_result_publication or on_abandoned_result is None,
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
                call.acknowledge_result_handoff(result)
                await call.wait_for_result_claim_ready()
                call.commit_result_claim_intent(result)
        except asyncio.CancelledError:
            abandoned = call.abandon(discard_before_start=cancel_pending)
            if abandoned:
                logger.warning(
                    "pipeline_blocking_work_cancelled",
                    reason_code=reason_code,
                    admission_retained=True,
                )
                raise
            logger.warning(
                "pipeline_blocking_work_cancellation_deferred",
                reason_code=reason_code,
                ownership_committed=True,
            )
            call.request_committed_result_claim(result)
            await call.wait_for_async_finish_after_claim_intent()
            return call.finalize_committed_result(result)
        except GeneratorExit:
            abandoned = call.abandon(discard_before_start=cancel_pending)
            if not abandoned:
                call.revoke_committed_result_transport()
            raise
        except BaseException:
            await call.wait_for_async_finish()
            raise
        try:
            await call.wait_for_async_finish()
        except asyncio.CancelledError:
            abandoned = call.abandon(discard_before_start=False)
            if abandoned:
                logger.warning(
                    "pipeline_blocking_work_cancelled",
                    reason_code=reason_code,
                    admission_retained=True,
                )
                raise
            logger.warning(
                "pipeline_blocking_work_cancellation_deferred",
                reason_code=reason_code,
                ownership_committed=True,
            )
            call.request_committed_result_claim(result)
            await call.wait_for_async_finish_after_claim_intent()
            return call.finalize_committed_result(result)
        except GeneratorExit:
            abandoned = call.abandon(discard_before_start=False)
            if not abandoned:
                call.revoke_committed_result_transport()
            raise
        if on_abandoned_result is not None:
            call.request_committed_result_claim(result)
            await call.wait_for_async_finish_after_claim_intent()
            result = call.finalize_committed_result(result)
        return result

    async def realize_owned[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        adopt: Callable[[Product], Any] | None = None,
        retire: Callable[[Product], Any],
        on_discarded: Callable[[], None] | None = None,
        reason_code: str,
        result_handoff_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
        timeout_seconds: float | None = None,
    ) -> Product:
        """Create, validate, escrow, and retire one product on its admitted worker."""
        if adopt is None:
            raise RuntimeOwnershipError("Owned resource realization requires a durable adopt callback")
        lifecycle = self._require_lifecycle()
        lifecycle.raise_if_runtime_fatal()
        if lifecycle.current_thread_can_reuse_blocking_capacity(cleanup=False):
            return await self._realize_owned_on_current_worker(
                factory,
                validate=validate,
                adopt=adopt,
                retire=retire,
                reason_code=reason_code,
            )
        operation, retire_product = self._owned_realization(
            factory,
            validate=validate,
            adopt=adopt,
            retire=retire,
            reason_code=reason_code,
        )
        return await self.run(
            operation,
            reason_code=reason_code,
            on_abandoned_result=retire_product,
            on_discarded=on_discarded,
            defer_result_publication=adopt is not None,
            result_handoff_seconds=result_handoff_seconds,
            timeout_seconds=timeout_seconds,
        )

    def realize_owned_sync[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        adopt: Callable[[Product], Any] | None = None,
        retire: Callable[[Product], Any],
        reason_code: str,
        result_handoff_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
    ) -> Product:
        """Synchronously adopt an admitted product without using the caller loop."""
        if adopt is None:
            raise RuntimeOwnershipError("Owned resource realization requires a durable adopt callback")
        lifecycle = self._require_lifecycle()
        lifecycle.raise_if_runtime_fatal()
        operation, retire_product = self._owned_realization(
            factory,
            validate=validate,
            adopt=adopt,
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
        except BaseException as construction_error:
            release_error = self._release_unmaterialized_permits(
                (permit,),
                primary_error=construction_error,
            )
            if release_error is not None:
                raise release_error from construction_error
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
        except BaseException as construction_error:
            start_decision.abort()
            try:
                self._rollback_group_construction(
                    calls,
                    permits,
                    primary_error=construction_error,
                )
            except RuntimeOwnershipError as rollback_error:
                raise rollback_error from construction_error
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
            terminal_error: RuntimeOwnershipError | None = None
            for call, permit in zip(calls, permits, strict=True):
                call.fail_to_start(type(start_error).__name__)
                release_error = self._release_reserved_call(call, permit, primary_error=start_error)
                if terminal_error is None and release_error is not None:
                    terminal_error = release_error
            if terminal_error is not None:
                raise terminal_error from start_error
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
        except BaseException as registration_error:
            call.fail_to_start("registration_failure")
            release_error = self._release_reserved_call(call, permit, primary_error=registration_error)
            if release_error is not None:
                raise release_error from registration_error
            raise
        try:
            self._start_worker(call, permit)
        except BaseException as exc:
            error_type = type(exc).__name__
            call.fail_to_start(error_type)
            release_error = self._release_reserved_call(call, permit, primary_error=exc)
            if release_error is not None:
                raise release_error from exc
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
                call.retire_abandoned_result_before_final_settlement()
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
        call.revoke_executable_authority()
        try:
            lifecycle.fence_runtime_fatal(terminal_error)
            self._release_permit_failure_aware(
                permit,
                call=call,
                primary_error=terminal_error,
            )
        finally:
            self._unregister(call)
            call.mark_finished()
        call.publish_terminal_result()

    def _release_reserved_call(
        self,
        call: _LifecycleBlockingCall,
        permit: PipelineBlockingPermit,
        *,
        primary_error: BaseException | None = None,
    ) -> RuntimeOwnershipError | None:
        if not call.claim_release():
            return None
        terminal_error: RuntimeOwnershipError | None = None
        try:
            call.publish_deferred_result()
            call.retire_abandoned_result_before_final_settlement()
            call.prepare_result_claim_before_release()
            deferred_cleanup_error = call.terminal_cleanup_error()
            if deferred_cleanup_error is not None:
                self._require_lifecycle().fence_runtime_fatal(deferred_cleanup_error)
        finally:
            try:
                terminal_error = self._release_permit_failure_aware(
                    permit,
                    call=call,
                    primary_error=primary_error,
                )
            finally:
                self._unregister(call)
                call.mark_finished()
        if terminal_error is not None:
            call.publish_terminal_result()
        return terminal_error

    def _release_permit_failure_aware(
        self,
        permit: PipelineBlockingPermit,
        *,
        call: _LifecycleBlockingCall | None = None,
        primary_error: BaseException | None = None,
    ) -> RuntimeOwnershipError | None:
        """Release one permit without opening a replacement-admission window."""
        lifecycle = self._require_lifecycle()
        terminal_error: RuntimeOwnershipError | None = None
        preexisting_terminal_error = None if call is None else call.terminal_cleanup_error()
        with lifecycle.blocking_permit_release_transition():
            try:
                if call is not None and preexisting_terminal_error is None:
                    call.retire_abandoned_result_before_final_settlement()
                    terminal_error = call.terminal_cleanup_error()
                    if terminal_error is not None:
                        lifecycle.fence_runtime_fatal(terminal_error)
                try:
                    lifecycle.release_blocking_permit(permit)
                except BaseException as release_error:
                    terminal_error = terminal_cleanup_failure(
                        primary_error,
                        release_error,
                        reason_code="blocking_permit_release_failed",
                        message="Pipeline blocking capacity release failed",
                    )
                    if call is not None:
                        call.replace_result_with_terminal_failure(terminal_error)
                        terminal_error = call.terminal_cleanup_error() or terminal_error
                    lifecycle.fence_runtime_fatal(terminal_error)
                else:
                    if call is not None and terminal_error is None and preexisting_terminal_error is None:
                        try:
                            call.settle_result_ownership_after_release()
                        except BaseException as ownership_error:
                            terminal_error = terminal_cleanup_failure(
                                primary_error,
                                ownership_error,
                                reason_code="blocking_result_ownership_failed",
                                message="Pipeline blocking result ownership failed",
                            )
                            call.replace_result_with_terminal_failure(terminal_error)
                            terminal_error = call.terminal_cleanup_error() or terminal_error
                            lifecycle.fence_runtime_fatal(terminal_error)
            finally:
                if call is not None:
                    call.mark_finished()
                    settled_error = call.terminal_cleanup_error()
                    if (
                        settled_error is not None
                        and settled_error is not terminal_error
                        and settled_error is not preexisting_terminal_error
                    ):
                        terminal_error = settled_error
                        lifecycle.fence_runtime_fatal(terminal_error)
                    call.mark_finished()
        return terminal_error

    def _release_unmaterialized_permits(
        self,
        permits: Iterable[PipelineBlockingPermit],
        *,
        primary_error: BaseException,
    ) -> RuntimeOwnershipError | None:
        first_error: RuntimeOwnershipError | None = None
        for permit in permits:
            release_error = self._release_permit_failure_aware(
                permit,
                primary_error=primary_error,
            )
            if first_error is None and release_error is not None:
                first_error = release_error
        return first_error

    def _rollback_group_construction(
        self,
        calls: list[_LifecycleBlockingCall],
        permits: tuple[PipelineBlockingPermit, ...],
        *,
        primary_error: BaseException,
    ) -> None:
        for call in calls:
            call.fail_to_start("construction_failure")
        first_error: RuntimeOwnershipError | None = None
        for index, permit in enumerate(permits):
            if index < len(calls):
                release_error = self._release_reserved_call(
                    calls[index],
                    permit,
                    primary_error=primary_error,
                )
                if first_error is None and release_error is not None:
                    first_error = release_error
                continue
            release_error = self._release_permit_failure_aware(
                permit,
                primary_error=primary_error,
            )
            if first_error is None and release_error is not None:
                first_error = release_error
        if first_error is not None:
            raise first_error

    async def _realize_owned_on_current_worker[Product](
        self,
        factory: Callable[[], Product],
        *,
        validate: Callable[[Product], Any],
        adopt: Callable[[Product], Any] | None,
        retire: Callable[[Product], Any],
        reason_code: str,
    ) -> Product:
        product = factory()
        try:
            validation = validate(product)
            if inspect.isawaitable(validation):
                await validation
            if adopt is not None:
                adoption = adopt(product)
                if inspect.isawaitable(adoption):
                    await adoption
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
        adopt: Callable[[Product], Any] | None,
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
                if adopt is not None:
                    adoption = adopt(product)
                    self._complete_worker_awaitable(adoption)
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
