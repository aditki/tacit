"""Runtime-owned admission control for pipeline executions."""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import math
import threading
import time
import weakref
from collections import OrderedDict, deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from concurrent.futures import Future as ThreadFuture
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from functools import partial
from itertools import count
from typing import Any, Protocol

import structlog

from tacit.config import (
    BEDROCK_COMPATIBILITY_MAX_CONCURRENT,
    DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES,
)
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.models.request_limits import pipeline_retained_request_memory_bound

_DEFAULT_PARTITION = "__default__"
_SELECTED_MAINTENANCE_BUDGET = 8
_SELECTED_CLAIM_LEASE_SECONDS = 1.0
_CLEANUP_PERMITS_PER_LEASE = 3
_RUNTIME_DRAIN_HEARTBEAT_SECONDS = 0.05
_LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS = 5.0
_MAX_RUNTIME_FATAL_FIELD_LENGTH = 128
_RUNTIME_FATAL_REASON_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_:-.")
_RUNTIME_FATAL_ERROR_TYPE_CHARACTERS = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.")
_RUNTIME_FATAL_REASON_CODES = frozenset(
    {
        "api_lifespan_teardown_failed_during_cancellation",
        "api_runtime_shutdown_failed",
        "backend:dashboard_realization",
        "bedrock_credential_cleanup_failed",
        "bedrock_operation_cleanup_failed",
        "bedrock_rejected_runtime_cleanup_failed",
        "pipeline_resource_cleanup_failed",
        "pipeline_runtime_root_cleanup_failed",
        "provider:context_realization",
        "provider:llm_realization",
        "provider_authority_revoke_failed",
        "provider_child_cleanup_cancelled",
        "provider_child_cleanup_failed",
        "provider_rejected_cleanup_failed_closed",
        "runtime_cleanup_failed",
        "runtime_root_drain_startup_exhausted",
        "slack_runtime_cleanup_failed",
        "slack_runtime_root_cleanup_failed",
        "slack_runtime_teardown_failed_during_cancellation",
        "slack_socket_cleanup_failed",
    }
)
_RUNTIME_FATAL_ERROR_TYPES = frozenset(
    {
        "CancelledError",
        "ConnectionError",
        "OSError",
        "RuntimeError",
        "RuntimeOwnershipError",
        "RuntimeOwnershipMismatchError",
        "RuntimeRootDrainStartupError",
        "TimeoutError",
    }
)
_ISOLATED_RUNTIME_ID_SEQUENCE = count(1)
_ISOLATED_RUNTIME_ID_LOCK = threading.Lock()
logger = structlog.get_logger()
_PIPELINE_EXECUTION_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "tacit_pipeline_execution_deadline",
    default=None,
)


def _drain_transport_tick(owner: _RuntimeRootDrainOwner) -> None:
    """Complete one coalesced transport wakeup without owning lifecycle work."""
    with owner.transport_wakeup_lock:
        owner.transport_wakeup_pending = False


@contextmanager
def pipeline_execution_deadline(deadline: float) -> Iterator[None]:
    """Propagate one monotonic pipeline deadline into owned stage work."""
    selected = float(deadline)
    if not math.isfinite(selected) or selected <= 0:
        raise ValueError("pipeline execution deadline must be a positive finite value")
    token = _PIPELINE_EXECUTION_DEADLINE.set(selected)
    try:
        yield
    finally:
        _PIPELINE_EXECUTION_DEADLINE.reset(token)


def current_pipeline_execution_deadline() -> float | None:
    """Return the current pipeline's monotonic deadline, when one is active."""
    return _PIPELINE_EXECUTION_DEADLINE.get()


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


class RuntimeRootDrainStartupError(RuntimeOwnershipError):
    """Retry-safe failure before final-root drain authority is installed."""


class _RuntimeRootGenerationHandle(Protocol):
    """Structural generation identity shared by owner and borrowed root handles."""

    @property
    def generation(self) -> int: ...


@dataclass(frozen=True)
class PipelineAdmissionLimits:
    """Validated limits for one runtime-owned admission controller."""

    concurrent: int
    concurrent_per_partition: int
    queued: int
    queued_per_partition: int


@dataclass(frozen=True, slots=True)
class PipelineAdmissionHealthSnapshot:
    """Bounded process-level admission state for health and telemetry."""

    capacity: int
    active: int
    queued: int
    retained: int
    blocking_in_flight: int
    cleanup_in_flight: int
    service_owner_in_flight: int
    saturated: bool
    fatal: bool
    degraded: bool
    reason_code: str | None = None


def pipeline_admission_limits(runtime_settings: Any) -> PipelineAdmissionLimits:
    """Resolve and revalidate admission settings at the runtime boundary."""
    concurrent = int(getattr(runtime_settings, "pipeline_max_concurrent", 5))
    configured_concurrent_per_partition = int(getattr(runtime_settings, "pipeline_max_concurrent_per_tenant", 0))
    queued = int(getattr(runtime_settings, "pipeline_max_queued", 100))
    configured_queued_per_partition = int(getattr(runtime_settings, "pipeline_max_queued_per_tenant", 25))
    wildcard = str(getattr(runtime_settings, "knowledge_tenant_id", "default")) == "*"

    if not 1 <= concurrent <= 1_000:
        raise ValueError("pipeline_max_concurrent must be between 1 and 1000")
    if (
        str(getattr(runtime_settings, "llm_provider", "")).strip().casefold() == "bedrock"
        and concurrent > BEDROCK_COMPATIBILITY_MAX_CONCURRENT
    ):
        raise ValueError(
            f"Bedrock compatibility bridge limits pipeline_max_concurrent to {BEDROCK_COMPATIBILITY_MAX_CONCURRENT}"
        )
    if not 0 <= configured_concurrent_per_partition <= 1_000:
        raise ValueError("pipeline_max_concurrent_per_tenant must be between 0 and 1000")
    if not 0 <= queued <= 1_000:
        raise ValueError("pipeline_max_queued must be between 0 and 1000")
    if not 0 <= configured_queued_per_partition <= 1_000:
        raise ValueError("pipeline_max_queued_per_tenant must be between 0 and 1000")
    retained_request_memory = pipeline_retained_request_memory_bound(
        max_concurrent=concurrent,
        max_queued=queued,
    )
    decoded_memory_envelope = int(
        getattr(
            runtime_settings,
            "api_request_body_max_buffered_bytes",
            DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES,
        )
    )
    if retained_request_memory > decoded_memory_envelope:
        raise ValueError("api_request_body_max_buffered_bytes must cover pipeline retained request memory")

    if wildcard:
        concurrent_per_partition = configured_concurrent_per_partition or max(1, concurrent - 1)
        if concurrent_per_partition > concurrent or (concurrent > 1 and concurrent_per_partition == concurrent):
            raise ValueError(
                "pipeline_max_concurrent_per_tenant must be lower than pipeline_max_concurrent for wildcard tenancy"
            )
        queued_per_partition = configured_queued_per_partition
        if queued > 0 and queued_per_partition >= queued:
            raise ValueError(
                "pipeline_max_queued_per_tenant must be lower than pipeline_max_queued for wildcard tenancy"
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


@dataclass(frozen=True)
class PipelineBlockingPermit:
    """Controller-owned authority for one submitted blocking worker."""

    controller_identity: object = field(repr=False)
    runtime_identity: str
    token: int = field(repr=False)
    permit_id: int = field(repr=False)
    cleanup: bool = False


@dataclass(frozen=True)
class PipelineRetainedWorkPermit:
    """Controller-owned authority for work that can outlive its request task."""

    controller_identity: object = field(repr=False)
    runtime_identity: str
    token: int = field(repr=False)
    permit_id: int = field(repr=False)


@dataclass(frozen=True)
class PipelineServiceOwnerPermit:
    """Controller-owned authority for one persistent async service loop."""

    controller_identity: object = field(repr=False)
    runtime_identity: str
    permit_id: int = field(repr=False)


@dataclass(frozen=True)
class RuntimeRootOwnerHandle:
    """Generation-fenced ownership for one independently started runtime root."""

    graph_nonce: str
    generation: int
    owner_id: int = field(repr=False)
    _graph: weakref.ReferenceType[Any] | None = field(default=None, repr=False, compare=False)

    async def _transfer_drain_startup_exhaustion(self, error: BaseException) -> None:
        """Move an exhausted caller release into graph-owned recovery."""
        graph = self._graph() if self._graph is not None else None
        if graph is None:
            raise RuntimeOwnershipError("Runtime root recovery authority is unavailable")
        await graph.transfer_root_drain_startup_exhaustion(self, error)


@dataclass(frozen=True, slots=True)
class RuntimeFatalCircuit:
    """Bounded process-lifetime fence metadata for an unsafe runtime."""

    reason_code: str
    error_type: str


class _ProcessRuntimeFatalRegistry:
    """Process-lifetime fatal metadata without retaining runtime authority."""

    __slots__ = ("_controllers", "_limit", "_lock", "_records", "_overflow")

    _KEY_DOMAIN = b"tacit:pipeline-runtime-fatal-circuit:v1\0"

    def __init__(self, *, limit: int) -> None:
        if type(limit) is not int or limit < 1:
            raise ValueError("runtime fatal registry limit must be a positive integer")
        self._limit = limit
        self._lock = threading.RLock()
        self._records: dict[str, RuntimeFatalCircuit] = {}
        self._overflow: RuntimeFatalCircuit | None = None
        self._controllers: dict[
            str,
            set[weakref.ReferenceType[PipelineAdmissionController]],
        ] = {}

    @classmethod
    def _key(cls, runtime_identity: str) -> str:
        selected = str(runtime_identity or "").strip()
        if not selected:
            raise RuntimeOwnershipError("Pipeline admission runtime identity is required")
        encoded = selected.encode("utf-8", errors="surrogatepass")
        return hashlib.sha256(cls._KEY_DOMAIN + encoded).hexdigest()

    def get(self, runtime_identity: str) -> RuntimeFatalCircuit | None:
        key = self._key(runtime_identity)
        with self._lock:
            if self._overflow is not None:
                return self._overflow
            return self._records.get(key)

    def _discard_controller(
        self,
        key: str,
        controller_ref: weakref.ReferenceType[PipelineAdmissionController],
    ) -> None:
        with self._lock:
            references = self._controllers.get(key)
            if references is None:
                return
            references.discard(controller_ref)
            if not references:
                self._controllers.pop(key, None)

    def _live_controllers_locked(
        self,
        keys: tuple[str, ...],
    ) -> tuple[PipelineAdmissionController, ...]:
        controllers: list[PipelineAdmissionController] = []
        seen: set[int] = set()
        for key in keys:
            references = self._controllers.get(key)
            if references is None:
                continue
            dead: list[weakref.ReferenceType[PipelineAdmissionController]] = []
            for controller_ref in references:
                controller = controller_ref()
                if controller is None:
                    dead.append(controller_ref)
                    continue
                identity = id(controller)
                if identity not in seen:
                    seen.add(identity)
                    controllers.append(controller)
            for controller_ref in dead:
                references.discard(controller_ref)
            if not references:
                self._controllers.pop(key, None)
        return tuple(controllers)

    def register(self, controller: PipelineAdmissionController) -> RuntimeFatalCircuit | None:
        """Track one live controller weakly and return any existing fence."""
        key = self._key(controller.runtime_identity)
        controller_ref = weakref.ref(
            controller,
            partial(self._discard_controller, key),
        )
        with self._lock:
            self._controllers.setdefault(key, set()).add(controller_ref)
            if self._overflow is not None:
                return self._overflow
            return self._records.get(key)

    def rebind(
        self,
        controller: PipelineAdmissionController,
        *,
        previous_identity: str,
        runtime_identity: str,
    ) -> RuntimeFatalCircuit | None:
        """Move one unused controller between identities without losing notification."""
        previous_key = self._key(previous_identity)
        selected_key = self._key(runtime_identity)
        with self._lock:
            if self._overflow is not None:
                return self._overflow
            fatal = self._records.get(selected_key)
            if fatal is not None:
                return fatal

            previous_references = self._controllers.get(previous_key)
            if previous_references is not None:
                for controller_ref in tuple(previous_references):
                    if controller_ref() is controller:
                        previous_references.discard(controller_ref)
                if not previous_references:
                    self._controllers.pop(previous_key, None)

            controller_ref = weakref.ref(
                controller,
                partial(self._discard_controller, selected_key),
            )
            self._controllers.setdefault(selected_key, set()).add(controller_ref)
            return None

    def unregister(self, controller: PipelineAdmissionController, *, runtime_identity: str) -> None:
        """Remove one retired controller from fatal-notification membership."""
        key = self._key(runtime_identity)
        with self._lock:
            references = self._controllers.get(key)
            if references is None:
                return
            for controller_ref in tuple(references):
                if controller_ref() is controller:
                    references.discard(controller_ref)
            if not references:
                self._controllers.pop(key, None)

    def latch(
        self,
        runtime_identity: str,
        candidate: RuntimeFatalCircuit,
    ) -> tuple[
        RuntimeFatalCircuit,
        bool,
        tuple[PipelineAdmissionController, ...],
    ]:
        key = self._key(runtime_identity)
        with self._lock:
            if self._overflow is not None:
                return self._overflow, False, ()
            existing = self._records.get(key)
            if existing is not None:
                return existing, False, ()
            if len(self._records) >= self._limit:
                self._overflow = RuntimeFatalCircuit(
                    reason_code="runtime_fatal_registry_capacity_exhausted",
                    error_type="RuntimeOwnershipError",
                )
                return self._overflow, True, self._live_controllers_locked(tuple(self._controllers))
            self._records[key] = candidate
            return candidate, True, self._live_controllers_locked((key,))


_PROCESS_RUNTIME_FATAL_REGISTRY = _ProcessRuntimeFatalRegistry(limit=1024)


def _next_isolated_runtime_identity() -> str:
    """Return a process-unique identity that object-address reuse cannot alias."""
    with _ISOLATED_RUNTIME_ID_LOCK:
        sequence = next(_ISOLATED_RUNTIME_ID_SEQUENCE)
    return f"internal-isolated:v1:{sequence}"


def _canonical_runtime_fatal_reason(value: object) -> str:
    """Accept only bounded stable reason codes at the process-lifetime boundary."""
    if not isinstance(value, str) or not value or len(value) > _MAX_RUNTIME_FATAL_FIELD_LENGTH:
        return "runtime_cleanup_failed"
    if value != value.strip() or value[0] not in "abcdefghijklmnopqrstuvwxyz":
        return "runtime_cleanup_failed"
    if any(character not in _RUNTIME_FATAL_REASON_CHARACTERS for character in value):
        return "runtime_cleanup_failed"
    return value if value in _RUNTIME_FATAL_REASON_CODES else "runtime_cleanup_failed"


def _canonical_runtime_fatal_error_type(value: object, *, fallback: type[BaseException]) -> str:
    """Map cleanup errors into a fixed process-lifetime diagnostic taxonomy."""
    selected = value if isinstance(value, str) else ""
    if (
        not selected
        or len(selected) > _MAX_RUNTIME_FATAL_FIELD_LENGTH
        or selected != selected.strip()
        or selected[0] not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_"
        or any(character not in _RUNTIME_FATAL_ERROR_TYPE_CHARACTERS for character in selected)
    ):
        selected = ""
    if selected in _RUNTIME_FATAL_ERROR_TYPES:
        return selected

    fallback_name = fallback.__name__
    if fallback_name in _RUNTIME_FATAL_ERROR_TYPES:
        return fallback_name
    for error_type, category in (
        (asyncio.CancelledError, "CancelledError"),
        (TimeoutError, "TimeoutError"),
        (ConnectionError, "ConnectionError"),
        (OSError, "OSError"),
        (RuntimeOwnershipError, "RuntimeOwnershipError"),
        (RuntimeError, "RuntimeError"),
    ):
        if issubclass(fallback, error_type):
            return category
    return "RuntimeError"


@dataclass(frozen=True)
class _RuntimeDrainWaiter:
    generation: int
    include_service_owner: bool
    request_paths_only: bool
    future: ThreadFuture[None]


@dataclass
class _RuntimeDrainTransportRelay:
    """One cross-thread completion relay shared by every waiter on one loop."""

    loop: asyncio.AbstractEventLoop
    future: asyncio.Future[None]
    completion_pending: bool = False


@dataclass
class _RuntimeRootDrainOwner:
    """Generation-scoped owner for final cleanup independent of caller loops."""

    generation: int
    handle: RuntimeRootOwnerHandle
    transport_loop: asyncio.AbstractEventLoop | None
    future: ThreadFuture[None] = field(default_factory=ThreadFuture)
    lifecycle_ready: threading.Event = field(default_factory=threading.Event)
    start_gate: threading.Event = field(default_factory=threading.Event)
    drain_started: threading.Event = field(default_factory=threading.Event)
    finished: threading.Event = field(default_factory=threading.Event)
    transport_wakeup_lock: threading.Lock = field(default_factory=threading.Lock)
    transport_wakeup_pending: bool = False
    transport_relay_lock: threading.Lock = field(default_factory=threading.Lock)
    transport_relays: dict[asyncio.AbstractEventLoop, _RuntimeDrainTransportRelay] = field(default_factory=dict)
    thread: threading.Thread | None = None
    manager: object | None = None
    provider_recheck_required: bool = False
    aborted: bool = False
    terminal: bool = False
    startup_error: BaseException | None = None
    rollback_error: BaseException | None = None


def fence_runtime_root_after_transport_failure(
    handle: RuntimeRootOwnerHandle,
    error: BaseException,
) -> None:
    """Synchronously retain and fence a root whose cleanup transport cannot start."""
    if not isinstance(handle, RuntimeRootOwnerHandle):
        raise RuntimeOwnershipError("Runtime root owner handle is invalid")
    graph = handle._graph() if handle._graph is not None else None
    if graph is None:
        raise RuntimeOwnershipError("Runtime root recovery authority is unavailable")
    graph.fence_root_owner_without_transport(handle, error)


class _LifecycleOwnerStartupError(RuntimeError):
    """One bounded lifecycle-thread transition failed before authority committed."""

    def __init__(self, *, phase: str, cause: BaseException, thread_alive: bool) -> None:
        super().__init__(f"Lifecycle owner {phase} failed")
        self.phase = phase
        self.cause = cause
        self.thread_alive = thread_alive


def _start_lifecycle_owner_thread(
    *,
    target: Callable[..., None],
    name: str,
    args: tuple[Any, ...] = (),
    install: Callable[[threading.Thread], None] | None = None,
    abort: Callable[[], None] | None = None,
    ready: threading.Event | None = None,
    readiness_error: Callable[[], BaseException | None] | None = None,
    finished: threading.Event | None = None,
    daemon: bool | None = None,
    timeout_seconds: float = _LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS,
) -> threading.Thread:
    """Construct, register, start, and prove one lifecycle owner as one phase.

    The caller retains its pre-transition authority until this returns. On any
    failure the gated target is aborted and given a bounded opportunity to exit;
    the owner-specific caller then rolls back or terminally fences its authority.
    """
    thread: threading.Thread | None = None
    phase = "construction"
    try:
        thread_kwargs: dict[str, Any] = {"target": target, "name": name}
        if args:
            thread_kwargs["args"] = args
        if daemon is not None:
            thread_kwargs["daemon"] = daemon
        thread = threading.Thread(**thread_kwargs)
        if install is not None:
            phase = "registration"
            install(thread)
        phase = "start"
        thread.start()
        if ready is not None:
            phase = "readiness"
            if not ready.wait(timeout=timeout_seconds):
                raise TimeoutError("Lifecycle owner readiness timed out")
            if readiness_error is not None:
                error = readiness_error()
                if error is not None:
                    raise error
        return thread
    except BaseException as cause:
        if abort is not None:
            abort()
        thread_ident = getattr(thread, "ident", None) if thread is not None else None
        if thread is not None and thread_ident is not None and finished is not None:
            finished.wait(timeout=timeout_seconds)
            if finished.is_set():
                thread.join(timeout=0)
        is_alive = getattr(thread, "is_alive", None) if thread is not None else None
        raise _LifecycleOwnerStartupError(
            phase=phase,
            cause=cause,
            thread_alive=bool(callable(is_alive) and is_alive()),
        ) from cause


async def release_runtime_root_with_startup_retry[HandleT: _RuntimeRootGenerationHandle](
    release: Callable[[HandleT], Awaitable[None]],
    handle: HandleT,
) -> None:
    """Retry one lifecycle preflight failure while the same root stays owned."""
    try:
        await release(handle)
    except RuntimeRootDrainStartupError as exc:
        cause = exc.__cause__ or exc
        logger.warning(
            "runtime_root_drain_startup_retry",
            reason_code="lifecycle_owner_preflight_failed",
            error_type=type(cause).__name__,
            root_generation=handle.generation,
        )
        mark_retry = getattr(handle, "_mark_drain_startup_retry", None)
        if callable(mark_retry):
            mark_retry()
        try:
            await release(handle)
        except RuntimeRootDrainStartupError as exhausted:
            transfer = getattr(handle, "_transfer_drain_startup_exhaustion", None)
            if callable(transfer):
                try:
                    outcome = transfer(exhausted)
                    if not inspect.isawaitable(outcome):
                        raise RuntimeOwnershipError("Runtime root recovery handoff is not awaitable")
                    await outcome
                except asyncio.CancelledError:
                    raise
                except BaseException as recovery_error:
                    logger.error(
                        "runtime_root_drain_startup_recovery_failed",
                        reason_code="lifecycle_owner_startup_exhausted",
                        error_type=type(recovery_error).__name__,
                        root_generation=handle.generation,
                    )
            raise


@dataclass
class _Waiter:
    token: int
    partition: str
    loop: asyncio.AbstractEventLoop
    event: asyncio.Event
    deadline: float | None
    state: str = "queued"
    claim_deadline: float | None = None


class RuntimeExecutionGraph:
    """One bounded runtime graph shared by every composition adapter."""

    def __init__(self, admission: PipelineAdmissionController, *, runtime_identity: str) -> None:
        self.admission = admission
        self.runtime_identity = runtime_identity
        self.graph_nonce = f"{runtime_identity}:{id(self)}"
        self._lock = threading.RLock()
        self._provider_spec: object | None = None
        self._provider_manager: object | None = None
        self._root_generation = 0
        self._next_root_owner = 0
        self._root_owners: dict[int, RuntimeRootOwnerHandle] = {}
        self._root_state = "unmanaged"
        self._final_drain_owner: _RuntimeRootDrainOwner | None = None

    def resolve_provider_manager(
        self,
        *,
        spec: object,
        create: Callable[[], object],
    ) -> object:
        """Return the namespace's sole provider manager after semantic preflight."""
        with self._lock:
            self.admission.raise_if_runtime_fatal()
            if self._root_state == "closed":
                raise RuntimeOwnershipError("Runtime execution graph is closed")
            if self._root_state == "draining" and not self.admission.current_execution_owns_capacity():
                raise RuntimeOwnershipError("Runtime execution graph is shutting down")
            if self._provider_manager is not None:
                if self._provider_spec != spec:
                    raise RuntimeOwnershipError(
                        "Runtime provider specification conflicts with the active execution graph"
                    )
                return self._provider_manager
            manager = create()
            self._provider_spec = spec
            self._provider_manager = manager
            return manager

    def register_root_owner(self) -> RuntimeRootOwnerHandle:
        """Register one composition root and open a clean shared generation."""
        with self._lock:
            self.admission.raise_if_runtime_fatal()
            if self._root_state == "draining":
                raise RuntimeOwnershipError("Runtime execution graph is draining")
            if not self._root_owners:
                drain_owner = self._final_drain_owner
                if drain_owner is not None:
                    if not drain_owner.terminal:
                        raise RuntimeOwnershipError("Runtime execution graph is draining")
                    self._final_drain_owner = None
                next_generation = self._root_generation + 1
                self.admission.open_root_generation(next_generation)
                self._root_generation = next_generation
                self._root_state = "active"
            self._next_root_owner += 1
            handle = RuntimeRootOwnerHandle(
                graph_nonce=self.graph_nonce,
                generation=self._root_generation,
                owner_id=self._next_root_owner,
                _graph=weakref.ref(self),
            )
            self._root_owners[handle.owner_id] = handle
            return handle

    async def transfer_root_drain_startup_exhaustion(
        self,
        handle: RuntimeRootOwnerHandle,
        error: BaseException,
    ) -> None:
        """Fence the runtime and adopt a final-root release after retry exhaustion."""
        cause = error.__cause__ or error
        fatal_error = RuntimeOwnershipError("Runtime root drain lifecycle owner failed to start")
        setattr(fatal_error, "cleanup_reason_code", "runtime_root_drain_startup_exhausted")
        setattr(fatal_error, "cleanup_error_type", type(cause).__name__)
        owner = _RuntimeRootDrainOwner(
            generation=handle.generation,
            handle=handle,
            transport_loop=asyncio.get_running_loop(),
        )

        def start_recovery() -> bool:
            recovery_error: BaseException | None = None
            with self._lock:
                self._validate_root_owner_locked(handle)
                # Validation and fencing share the graph lock. A successful release
                # that wins this lock closes cleanly; an exhaustion handoff that
                # wins it cannot be followed by a stale cross-thread fatal latch.
                self.admission.fence_runtime_fatal(fatal_error)
                try:
                    _start_lifecycle_owner_thread(
                        target=self._run_final_root_drain,
                        args=(owner,),
                        name=f"tacit-runtime-root-recovery-{id(self)}-{handle.generation}",
                        install=lambda thread: setattr(owner, "thread", thread),
                        abort=lambda: self._abort_root_drain_owner(owner),
                        ready=owner.lifecycle_ready,
                        readiness_error=lambda: owner.startup_error,
                        finished=owner.finished,
                        daemon=True,
                    )
                except _LifecycleOwnerStartupError as exc:
                    recovery_error = RuntimeRootDrainStartupError(
                        "Runtime root recovery lifecycle owner failed to start"
                    )
                    recovery_error.__cause__ = exc.cause
                else:
                    try:
                        owner.provider_recheck_required = self.admission.begin_root_drain(handle.generation)
                    except BaseException as exc:
                        self._abort_root_drain_owner(owner)
                        recovery_error = exc
                    else:
                        self._root_owners.pop(handle.owner_id)
                        self._root_state = "draining"
                        owner.manager = self._provider_manager
                        self._final_drain_owner = owner
                        owner.start_gate.set()

            if recovery_error is None:
                return True
            if owner.thread is not None and owner.thread.ident is not None and not owner.finished.is_set():
                owner.finished.wait(timeout=_LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS)
                if owner.finished.is_set():
                    owner.thread.join(timeout=0)
            with self._lock:
                self._retain_root_drain_recovery_locked(handle, recovery_error)
            logger.error(
                "runtime_root_drain_recovery_retained",
                reason_code="lifecycle_owner_startup_exhausted",
                error_type=type(recovery_error).__name__,
                root_generation=handle.generation,
            )
            return False

        try:
            started, cancelled = await self._run_synchronous_lifecycle_transition(start_recovery)
        except _LifecycleOwnerStartupError as exc:
            self.fence_root_owner_without_transport(handle, exc.cause)
            return
        if started:
            await self._await_final_drain_owner(owner)
        if cancelled:
            raise asyncio.CancelledError

    @staticmethod
    def _abort_root_drain_owner(owner: _RuntimeRootDrainOwner) -> None:
        owner.aborted = True
        owner.start_gate.set()

    def fence_root_owner_without_transport(
        self,
        handle: RuntimeRootOwnerHandle,
        error: BaseException,
    ) -> None:
        """Synchronously transfer an unreachable caller root into fatal graph ownership."""
        cause = error.__cause__ or error
        fatal_error = RuntimeOwnershipError("Runtime root cleanup transport failed to start")
        setattr(fatal_error, "cleanup_reason_code", "runtime_root_drain_startup_exhausted")
        setattr(fatal_error, "cleanup_error_type", type(cause).__name__)
        with self._lock:
            self._validate_root_owner_locked(handle)
            self.admission.fence_runtime_fatal(fatal_error)
            self._retain_root_drain_recovery_locked(
                handle,
                fatal_error,
                transport_loop=None,
                root_state="fenced",
            )

    def _retain_root_drain_recovery_locked(
        self,
        handle: RuntimeRootOwnerHandle,
        recovery_error: BaseException,
        *,
        transport_loop: asyncio.AbstractEventLoop | None = None,
        root_state: str = "draining",
    ) -> None:
        """Retain terminally fenced authority when no lifecycle thread can run."""
        if self._root_state == "closed" and not self._root_owners:
            return
        if self._root_state == "draining" and self._final_drain_owner is not None:
            return
        self._validate_root_owner_locked(handle)
        provider_recheck_required = self.admission.begin_root_drain(handle.generation)
        self._root_owners.pop(handle.owner_id)
        self._root_state = root_state
        retained_error = RuntimeOwnershipError("Runtime root drain recovery is unavailable")
        setattr(retained_error, "cleanup_reason_code", "runtime_root_drain_startup_exhausted")
        setattr(retained_error, "cleanup_error_type", type(recovery_error).__name__)
        owner = _RuntimeRootDrainOwner(
            generation=handle.generation,
            handle=handle,
            transport_loop=transport_loop,
            provider_recheck_required=provider_recheck_required,
            manager=self._provider_manager,
            startup_error=retained_error,
        )
        # The graph, rather than the discarded caller handle, now retains the
        # fenced generation and every still-charged permit. Process restart is
        # required if no lifecycle thread can be constructed.
        owner.lifecycle_ready.set()
        owner.start_gate.set()
        owner.future.set_exception(retained_error)
        owner.finished.set()
        self._final_drain_owner = owner

    async def _await_final_drain_owner(self, owner: _RuntimeRootDrainOwner) -> None:
        """Transport one owned drain result while preserving cancellation."""
        relay = self._final_drain_transport_relay(owner)
        cancelled = False
        while True:
            try:
                await asyncio.shield(relay)
                break
            except asyncio.CancelledError:
                cancelled = True
                if owner.future.done():
                    break
        owner.future.result()
        if cancelled:
            raise asyncio.CancelledError

    @staticmethod
    async def _run_synchronous_lifecycle_transition(
        operation: Callable[[], Any],
        *,
        on_cancelled_failure: Callable[[BaseException], None] | None = None,
    ) -> tuple[Any, bool]:
        """Run one synchronous ownership transition without blocking its caller loop."""
        source: ThreadFuture[Any] = ThreadFuture()

        def run_transition() -> None:
            try:
                source.set_result(operation())
            except BaseException as exc:
                source.set_exception(exc)

        try:
            transition_thread = threading.Thread(
                target=run_transition,
                name=f"tacit-lifecycle-transition-{id(source)}",
                daemon=True,
            )
        except BaseException as cause:
            raise _LifecycleOwnerStartupError(
                phase="construction",
                cause=cause,
                thread_alive=False,
            ) from cause
        try:
            transition_thread.start()
        except BaseException:
            if transition_thread.ident is None or (not transition_thread.is_alive() and not source.done()):
                raise
        transition = asyncio.wrap_future(source)
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(transition)
                transition_thread.join(timeout=0)
                return result, cancelled
            except asyncio.CancelledError:
                cancelled = True
                if transition.done():
                    try:
                        result = transition.result()
                    except BaseException as exc:
                        if on_cancelled_failure is not None:
                            on_cancelled_failure(exc)
                        raise asyncio.CancelledError from exc
                    transition_thread.join(timeout=0)
                    return result, cancelled
            except BaseException as exc:
                if cancelled:
                    if on_cancelled_failure is not None:
                        on_cancelled_failure(exc)
                    raise asyncio.CancelledError from exc
                raise

    def _fence_cancelled_root_startup_failure(
        self,
        handle: RuntimeRootOwnerHandle,
        error: BaseException,
    ) -> None:
        """Retain a root when its cancelled caller cannot retry failed startup."""
        with self._lock:
            if self._root_owners.get(handle.owner_id) == handle:
                self.fence_root_owner_without_transport(handle, error)

    def _begin_root_owner_release(
        self,
        handle: RuntimeRootOwnerHandle,
        *,
        transport_loop: asyncio.AbstractEventLoop | None,
    ) -> tuple[_RuntimeRootDrainOwner | None, BaseException | None]:
        """Synchronously transfer a final root into graph-owned drain authority."""
        startup_error: BaseException | None = None
        with self._lock:
            drain_owner = self._final_drain_owner
            if drain_owner is not None and drain_owner.handle == handle and not drain_owner.future.done():
                owner = drain_owner
            else:
                self._validate_root_owner_locked(handle)
                if len(self._root_owners) > 1:
                    self._root_owners.pop(handle.owner_id)
                    return None, None
                owner = _RuntimeRootDrainOwner(
                    generation=handle.generation,
                    handle=handle,
                    transport_loop=transport_loop,
                )
                # Start a gated lifecycle owner before mutating graph state. A
                # start failure therefore leaves the root valid for a retry.
                try:
                    _start_lifecycle_owner_thread(
                        target=self._run_final_root_drain,
                        args=(owner,),
                        name=f"tacit-runtime-root-drain-{id(self)}-{handle.generation}",
                        install=lambda thread: setattr(owner, "thread", thread),
                        abort=lambda: self._abort_root_drain_owner(owner),
                        ready=owner.lifecycle_ready,
                        readiness_error=lambda: owner.startup_error,
                        finished=owner.finished,
                        daemon=True,
                    )
                except _LifecycleOwnerStartupError as exc:
                    if exc.thread_alive:
                        self.fence_root_owner_without_transport(handle, exc.cause)
                        raise RuntimeOwnershipError(
                            "Runtime root drain lifecycle owner startup was ambiguous"
                        ) from exc.cause
                    raise RuntimeRootDrainStartupError(
                        "Runtime root drain lifecycle owner failed to start"
                    ) from exc.cause
                try:
                    owner.provider_recheck_required = self.admission.begin_root_drain(handle.generation)
                except BaseException as exc:
                    self._abort_root_drain_owner(owner)
                    owner.rollback_error = exc
                    self._final_drain_owner = owner
                    startup_error = exc
                else:
                    self._root_owners.pop(handle.owner_id)
                    self._root_state = "draining"
                    owner.manager = self._provider_manager
                    self._final_drain_owner = owner
                    owner.start_gate.set()

        return owner, startup_error

    async def release_root_owner(self, handle: RuntimeRootOwnerHandle) -> None:
        """Release one root; the final owner drains the complete runtime graph."""
        with self._lock:
            existing_owner = self._final_drain_owner
            if existing_owner is not None and existing_owner.handle == handle and not existing_owner.future.done():
                transition_result = (existing_owner, None)
            else:
                transition_result = None
        startup_cancelled = False
        if transition_result is None:
            transition = partial(
                self._begin_root_owner_release,
                handle,
                transport_loop=asyncio.get_running_loop(),
            )
            try:
                transition_result, startup_cancelled = await self._run_synchronous_lifecycle_transition(
                    transition,
                    on_cancelled_failure=partial(
                        self._fence_cancelled_root_startup_failure,
                        handle,
                    ),
                )
            except _LifecycleOwnerStartupError as exc:
                raise RuntimeRootDrainStartupError("Runtime root lifecycle transition failed to start") from exc.cause
        owner, startup_error = transition_result
        if owner is None:
            if startup_cancelled:
                raise asyncio.CancelledError
            return

        if startup_error is not None:
            try:
                await self._await_root_drain_rollback(owner, handle, startup_error)
            except BaseException:
                if startup_cancelled:
                    raise asyncio.CancelledError
                raise

        relay = self._final_drain_transport_relay(owner)
        cancelled = startup_cancelled
        while True:
            try:
                await asyncio.shield(relay)
                break
            except asyncio.CancelledError:
                cancelled = True
                if owner.future.done():
                    break
        owner.future.result()
        if cancelled:
            raise asyncio.CancelledError

    def release_root_owner_detached(self, handle: RuntimeRootOwnerHandle) -> None:
        """Transfer final drain to the graph without retaining caller transport."""
        owner, startup_error = self._begin_root_owner_release(handle, transport_loop=None)
        if owner is None:
            return
        if startup_error is None:
            return

        rollback_error = RuntimeOwnershipError("Runtime root drain startup did not commit")
        setattr(rollback_error, "cleanup_reason_code", "runtime_root_drain_startup_exhausted")
        setattr(rollback_error, "cleanup_error_type", type(startup_error).__name__)
        self._fence_root_drain_rollback(handle, owner, rollback_error)
        raise rollback_error from startup_error

    async def _await_root_drain_rollback(
        self,
        owner: _RuntimeRootDrainOwner,
        handle: RuntimeRootOwnerHandle,
        startup_error: BaseException,
    ) -> None:
        """Await pre-fence owner rollback without blocking its requester loop."""
        relay = self._final_drain_transport_relay(owner)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + _LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS
        cancelled = False
        settled = False
        while not settled:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                done, _pending = await asyncio.wait((relay,), timeout=remaining)
                settled = bool(done)
            except asyncio.CancelledError:
                cancelled = True
                settled = relay.done()

        if not settled:
            rollback_error = RuntimeOwnershipError("Runtime root drain rollback did not settle")
            setattr(rollback_error, "cleanup_reason_code", "runtime_root_drain_startup_exhausted")
            setattr(rollback_error, "cleanup_error_type", type(startup_error).__name__)
            self._fence_root_drain_rollback(handle, owner, rollback_error)
            if cancelled:
                raise asyncio.CancelledError
            raise rollback_error from startup_error

        with self._lock:
            if self._final_drain_owner is owner and self._root_state == "active":
                self._final_drain_owner = None
        relay.result()
        if cancelled:
            raise asyncio.CancelledError
        raise startup_error

    def _fence_root_drain_rollback(
        self,
        handle: RuntimeRootOwnerHandle,
        owner: _RuntimeRootDrainOwner,
        rollback_error: BaseException,
    ) -> None:
        """Retain a non-settling rollback as process-fatal graph authority."""
        self.admission.fence_runtime_fatal(rollback_error)
        with self._lock:
            self._validate_root_owner_locked(handle)
            if self._final_drain_owner is not owner:
                raise RuntimeOwnershipError("Runtime root rollback owner changed before fencing")
            self.admission.fence_root_generation(handle.generation)
            self._root_owners.pop(handle.owner_id)
            self._root_state = "fenced"
            owner.manager = self._provider_manager

    @staticmethod
    def _finish_final_drain_transport_relay(
        owner: _RuntimeRootDrainOwner,
        relay: _RuntimeDrainTransportRelay,
        source: ThreadFuture[None],
    ) -> None:
        """Publish one lifecycle result after its transport loop resumes."""
        with owner.transport_relay_lock:
            relay.completion_pending = False
        if relay.future.done():
            return
        try:
            source.result()
        except BaseException as exc:
            relay.future.set_exception(exc)
        else:
            relay.future.set_result(None)

    @staticmethod
    def _queue_final_drain_transport_relay(
        owner: _RuntimeRootDrainOwner,
        relay: _RuntimeDrainTransportRelay,
        source: ThreadFuture[None],
    ) -> None:
        """Queue at most one completion callback on a live or stopped loop."""
        with owner.transport_relay_lock:
            if relay.completion_pending or relay.future.done():
                return
            relay.completion_pending = True
            owner.transport_relays.pop(relay.loop, None)
        if relay.loop.is_closed():
            with owner.transport_relay_lock:
                relay.completion_pending = False
            return
        try:
            relay.loop.call_soon_threadsafe(
                RuntimeExecutionGraph._finish_final_drain_transport_relay,
                owner,
                relay,
                source,
            )
        except RuntimeError:
            with owner.transport_relay_lock:
                relay.completion_pending = False

    def _final_drain_transport_relay(
        self,
        owner: _RuntimeRootDrainOwner,
    ) -> asyncio.Future[None]:
        """Return the generation's single source relay for this requester loop."""
        loop = asyncio.get_running_loop()
        register_source_callback = False
        with owner.transport_relay_lock:
            relay = owner.transport_relays.get(loop)
            if relay is not None:
                return relay.future
            relay_future: asyncio.Future[None] = loop.create_future()
            relay = _RuntimeDrainTransportRelay(loop=loop, future=relay_future)
            owner.transport_relays[loop] = relay
            register_source_callback = True
        if register_source_callback:
            owner.future.add_done_callback(
                partial(
                    self._queue_final_drain_transport_relay,
                    owner,
                    relay,
                )
            )
        return relay_future

    def _run_final_root_drain(self, owner: _RuntimeRootDrainOwner) -> None:
        """Run one final drain on its generation-owned lifecycle thread."""
        runner: asyncio.Runner | None = None
        terminal_error: BaseException | None = None
        try:
            try:
                runner = asyncio.Runner()
                preparation = self._prepare_final_drain_lifecycle()
                try:
                    runner.run(preparation)
                except BaseException:
                    preparation.close()
                    raise
            except BaseException as exc:
                owner.startup_error = exc
                owner.lifecycle_ready.set()
                return
            owner.lifecycle_ready.set()
            owner.start_gate.wait()
            if owner.aborted:
                return
            drain = self._run_owned_final_root_drain(owner)
            try:
                runner.run(drain)
            except BaseException:
                drain.close()
                raise
        except BaseException as exc:
            terminal_error = exc
        finally:
            if runner is not None:
                try:
                    runner.close()
                except BaseException as exc:
                    if terminal_error is None:
                        terminal_error = exc
            if not owner.future.done():
                if owner.aborted:
                    owner.future.set_result(None)
                elif owner.startup_error is None:
                    if terminal_error is not None:
                        owner.future.set_exception(terminal_error)
                    else:
                        owner.future.set_result(None)
            owner.lifecycle_ready.set()
            owner.finished.set()

    @staticmethod
    async def _prepare_final_drain_lifecycle() -> None:
        """Prove the lifecycle loop can execute before normal work is fenced."""
        await asyncio.sleep(0)

    async def _run_owned_final_root_drain(self, owner: _RuntimeRootDrainOwner) -> None:
        """Keep lifecycle and result-transport loops responsive during cleanup."""
        owner.drain_started.set()
        heartbeat = asyncio.create_task(
            self._run_final_drain_heartbeat(owner),
            name=f"tacit-runtime-root-drain-heartbeat-{owner.generation}",
        )
        try:
            await self._drain_final_root(
                owner.generation,
                owner.manager,
                owner.provider_recheck_required,
            )
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass

    @staticmethod
    async def _run_final_drain_heartbeat(owner: _RuntimeRootDrainOwner) -> None:
        """Wake both loops without transferring cleanup or capacity ownership."""
        while True:
            transport_loop = owner.transport_loop
            if transport_loop is not None and not transport_loop.is_closed():
                with owner.transport_wakeup_lock:
                    should_wake = not owner.transport_wakeup_pending
                    if should_wake:
                        owner.transport_wakeup_pending = True
                if should_wake:
                    try:
                        transport_loop.call_soon_threadsafe(_drain_transport_tick, owner)
                    except RuntimeError:
                        # A stopped or closing transport loop is not a lifecycle
                        # dependency. Keep the token charged so repeated ticks
                        # cannot grow its callback queue after an ambiguous send.
                        pass
            await asyncio.sleep(_RUNTIME_DRAIN_HEARTBEAT_SECONDS)

    async def _drain_final_root(
        self,
        generation: int,
        initial_manager: object | None,
        _provider_recheck_required: bool,
    ) -> None:
        logger.info(
            "runtime_root_drain_started",
            reason_code="final_runtime_root_released",
            root_generation=generation,
        )
        manager_error: BaseException | None = None
        await self.admission.wait_for_root_request_paths(generation)
        logger.info(
            "runtime_root_request_paths_drained",
            root_generation=generation,
        )

        # Work admitted before the synchronous fence may install the manager
        # after release_root_owner captured its snapshot. Request-path
        # quiescence makes this the authoritative generation to revoke.
        settled_manager = self.provider_manager()
        if initial_manager is not None and settled_manager is not initial_manager:
            manager_error = RuntimeOwnershipError("Runtime provider manager changed during final drain")
        shutdown_error = await self._shutdown_provider_manager(settled_manager)
        if manager_error is None:
            manager_error = shutdown_error

        # Provider shutdown may settle retained operations and create cleanup
        # permits. Drain those before the final late-manager recheck, so no
        # pre-fence worker can publish an unclosed manager after our snapshot.
        await self.admission.wait_for_root_drain(generation, include_service_owner=False)
        late_manager = self.provider_manager()
        if late_manager is not None and late_manager is not settled_manager:
            shutdown_error = await self._shutdown_provider_manager(late_manager)
            if manager_error is None:
                manager_error = shutdown_error

        logger.info(
            "runtime_root_provider_shutdown_settled",
            root_generation=generation,
            status="failed" if manager_error is not None else "completed",
        )
        await self.admission.wait_for_root_drain(generation, include_service_owner=True)
        self.admission.finish_root_drain(generation)
        with self._lock:
            if self._root_generation != generation or self._root_owners or self._root_state != "draining":
                raise RuntimeOwnershipError("Runtime root generation changed during final drain")
            drain_owner = self._final_drain_owner
            if drain_owner is None or drain_owner.generation != generation:
                raise RuntimeOwnershipError("Runtime root drain owner changed during final drain")
            if self._provider_manager is not None:
                self._provider_manager = None
                self._provider_spec = None
            self._root_state = "closed"
            drain_owner.terminal = True
        logger.info(
            "runtime_root_drain_completed",
            root_generation=generation,
        )
        if manager_error is not None:
            raise manager_error

    @staticmethod
    async def _shutdown_provider_manager(manager: object | None) -> BaseException | None:
        """Shut down one exact manager generation without losing terminal drain."""
        if manager is None:
            return None
        shutdown = getattr(manager, "shutdown", None)
        if not callable(shutdown):
            return RuntimeOwnershipError("Runtime provider manager does not support shutdown")
        try:
            outcome = shutdown()
        except BaseException as exc:
            return exc
        if not inspect.isawaitable(outcome):
            return RuntimeOwnershipError("Runtime provider manager shutdown is not awaitable")
        try:
            await outcome
        except BaseException as exc:
            return exc
        return None

    def _validate_root_owner_locked(self, handle: RuntimeRootOwnerHandle) -> None:
        if not isinstance(handle, RuntimeRootOwnerHandle):
            raise RuntimeOwnershipError("Runtime root owner handle is invalid")
        if handle.graph_nonce != self.graph_nonce:
            raise RuntimeOwnershipError("Runtime root owner belongs to another execution graph")
        if handle.generation != self._root_generation:
            raise RuntimeOwnershipError("Runtime root owner belongs to a prior generation")
        if self._root_owners.get(handle.owner_id) != handle:
            raise RuntimeOwnershipError("Runtime root owner is stale or duplicate")

    @property
    def provider_manager_count(self) -> int:
        """Return the bounded provider-manager cardinality (zero or one)."""
        with self._lock:
            return int(self._provider_manager is not None)

    def provider_manager(self) -> object | None:
        """Return the installed manager without constructing one."""
        with self._lock:
            return self._provider_manager

    @property
    def root_owner_count(self) -> int:
        with self._lock:
            return len(self._root_owners)

    @property
    def root_generation(self) -> int:
        with self._lock:
            return self._root_generation

    @property
    def root_state(self) -> str:
        with self._lock:
            return self._root_state

    def bind_runtime_identity(self, runtime_identity: str) -> None:
        """Bind through the controller's process-wide canonical claim."""
        self.admission.bind_runtime_identity(runtime_identity)

    def _bind_runtime_identity_under_registry(self, selected: str) -> None:
        """Apply a canonical identity while the process registry claim is held."""
        with self._lock:
            self.admission._bind_runtime_identity_from_graph_locked(self, selected)


class PipelineAdmissionController:
    """Bound and fairly schedule pipeline work across one runtime graph."""

    # Selection reserves capacity before the target loop confirms its claim.
    # Bound that reservation independently of the target loop's liveness.
    _selected_claim_lease_seconds = _SELECTED_CLAIM_LEASE_SECONDS
    _selected_maintenance_idle_seconds = _SELECTED_CLAIM_LEASE_SECONDS

    def __init__(
        self,
        limit: int,
        *,
        max_queued: int = 100,
        max_queued_per_partition: int | None = None,
        max_in_flight_per_partition: int | None = None,
        runtime_identity: str | None = None,
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
        self._selected_maintenance_condition = threading.Condition(self._lock)
        self._identity = object()
        self._runtime_identity = runtime_identity or _next_isolated_runtime_identity()
        self._runtime_identity_explicit = runtime_identity is not None
        self._settings_runtime_identity = runtime_identity
        self._process_authority_replaceable = False
        self._process_authority_retired = False
        existing_fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
        if existing_fatal is not None:
            raise self._runtime_fatal_error(existing_fatal)
        self._in_flight = 0
        self._in_flight_by_partition: dict[str, int] = {}
        self._active_leases: dict[int, str] = {}
        self._service_admitted_tokens: set[int] = set()
        self._queued_count = 0
        self._next_token = 0
        self._queues: dict[str, OrderedDict[int, _Waiter]] = {}
        self._eligible_partitions: OrderedDict[str, None] = OrderedDict()
        self._selected: dict[int, _Waiter] = {}
        self._selected_checks: deque[int] = deque()
        self._selected_claims: OrderedDict[int, float] = OrderedDict()
        self._selected_by_partition: dict[str, int] = {}
        self._selected_maintenance_thread: threading.Thread | None = None
        self._retained_tasks: dict[int, set[asyncio.Task[Any]]] = {}
        self._retained_task_tokens: dict[asyncio.Task[Any], int] = {}
        self._retained_work_permits: dict[int, set[int]] = {}
        self._retained_work_permit_tokens: dict[int, int] = {}
        self._next_retained_work_permit = 0
        self._blocking_permits: dict[int, set[int]] = {}
        self._blocking_permit_tokens: dict[int, int] = {}
        self._cleanup_permits: set[int] = set()
        # One close coordinator plus the Bedrock runtime and credential clients,
        # charged to each admitted runtime generation rather than to an event loop.
        self._cleanup_permit_limit_per_token = _CLEANUP_PERMITS_PER_LEASE
        self._next_blocking_permit = 0
        self._service_owner_permit: PipelineServiceOwnerPermit | None = None
        self._next_service_owner_permit = 0
        self._service_owner_thread_id: int | None = None
        self._release_pending: set[int] = set()
        self._blocking_worker_context = threading.local()
        self._service_owner_context = threading.local()
        self._runtime_root_generation = 0
        self._runtime_root_state = "unmanaged"
        self._runtime_drain_waiters: list[_RuntimeDrainWaiter] = []
        self._request_paths_idle_callbacks: list[Callable[[], None]] = []
        self._execution_graph = RuntimeExecutionGraph(
            self,
            runtime_identity=self._runtime_identity,
        )
        self._current_leases: contextvars.ContextVar[tuple[int, ...]] = contextvars.ContextVar(
            f"tacit_pipeline_lifecycle_{id(self)}",
            default=(),
        )
        self._register_runtime_authority()

    def _register_runtime_authority(self) -> None:
        """Register an isolated controller or atomically claim an explicit identity."""
        if not self._runtime_identity_explicit:
            registered_fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.register(self)
            if registered_fatal is not None:
                raise self._runtime_fatal_error(registered_fatal)
            return

        with _RUNTIME_ADMISSION_CONTROLLERS_LOCK:
            existing = _RUNTIME_ADMISSION_CONTROLLERS.get(self._runtime_identity)
            if existing is not None and existing is not self:
                raise RuntimeOwnershipError("Pipeline admission runtime identity already has an authority")
            registered_fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.register(self)
            if registered_fatal is not None:
                _PROCESS_RUNTIME_FATAL_REGISTRY.unregister(
                    self,
                    runtime_identity=self._runtime_identity,
                )
                raise self._runtime_fatal_error(registered_fatal)
            _RUNTIME_ADMISSION_CONTROLLERS[self._runtime_identity] = self

    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    @property
    def runtime_identity(self) -> str:
        """Return the immutable runtime identity associated with this controller."""
        with self._lock:
            return self._runtime_identity

    @property
    def execution_graph(self) -> RuntimeExecutionGraph:
        """Return the sole execution graph attached to this admission namespace."""
        return self._execution_graph

    def bind_runtime_identity(self, runtime_identity: str) -> None:
        """Bind an unused isolated controller to one explicit runtime identity."""
        selected = str(runtime_identity or "").strip()
        if not selected:
            raise RuntimeOwnershipError("Pipeline admission runtime identity is required")
        with _RUNTIME_ADMISSION_CONTROLLERS_LOCK:
            existing = _RUNTIME_ADMISSION_CONTROLLERS.get(selected)
            if existing is not None and existing is not self:
                if not existing._retire_idle_late_bound_authority():
                    raise RuntimeOwnershipError("Pipeline admission runtime identity already has an authority")
                _PROCESS_RUNTIME_FATAL_REGISTRY.unregister(
                    existing,
                    runtime_identity=selected,
                )
                _RUNTIME_ADMISSION_CONTROLLERS.pop(selected, None)
            self._execution_graph._bind_runtime_identity_under_registry(selected)
            _RUNTIME_ADMISSION_CONTROLLERS[selected] = self

    def bind_runtime_settings_identity(self, runtime_identity: str) -> None:
        """Validate settings ownership without claiming a process identity.

        Canonical controllers already carry the settings-derived identity.
        Deliberately isolated controllers retain their process-unique admission
        namespace while recording which settings declaration their resources
        must satisfy.
        """
        selected = str(runtime_identity or "").strip()
        if not selected:
            raise RuntimeOwnershipError("Pipeline admission runtime identity is required")
        with self._lock:
            if self._runtime_identity_explicit and self._runtime_identity != selected:
                raise RuntimeOwnershipError("Pipeline admission controller belongs to another runtime")
            if self._settings_runtime_identity not in {None, selected}:
                raise RuntimeOwnershipError("Pipeline admission settings identity changed")
            self._settings_runtime_identity = selected

    def _retire_idle_late_bound_authority(self) -> bool:
        """Atomically retire an unused late-bound authority without foreign locks."""
        graph = self._execution_graph
        with graph._lock, self._lock:
            if not self._process_authority_replaceable or self._process_authority_retired:
                return False
            if (
                graph._root_owners
                or graph._provider_manager is not None
                or graph._final_drain_owner is not None
                or graph._root_state not in {"unmanaged", "closed"}
                or self._in_flight
                or self._queued_count
                or self._selected
                or self._retained_tasks
                or self._retained_work_permit_tokens
                or self._release_pending
                or self._blocking_permit_tokens
                or self._cleanup_permits
                or self._service_owner_permit is not None
                or self._runtime_drain_waiters
                or self._selected_maintenance_thread is not None
            ):
                return False
            self._process_authority_retired = True
            return True

    def _bind_runtime_identity_from_graph_locked(
        self,
        graph: RuntimeExecutionGraph,
        selected: str,
    ) -> None:
        """Validate and apply one graph-first identity bind atomically."""
        with self._lock:
            if graph is not self._execution_graph:
                raise RuntimeOwnershipError("Pipeline admission controller belongs to another runtime")
            fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(selected)
            if fatal is not None:
                raise self._runtime_fatal_error(fatal)
            if self._runtime_identity == selected:
                self._runtime_identity_explicit = True
                self._settings_runtime_identity = selected
                return
            if self._settings_runtime_identity not in {None, selected}:
                raise RuntimeOwnershipError("Pipeline admission settings identity changed")
            if (
                self._runtime_identity_explicit
                or graph._root_generation
                or graph._root_owners
                or graph._provider_spec is not None
                or graph._provider_manager is not None
                or graph._final_drain_owner is not None
                or graph._root_state != "unmanaged"
                or self._runtime_root_generation
                or self._runtime_root_state != "unmanaged"
                or self._next_token
                or self._next_blocking_permit
                or self._next_service_owner_permit
                or self._in_flight
                or self._queued_count
                or self._selected
                or self._retained_tasks
                or self._retained_work_permit_tokens
                or self._release_pending
                or self._blocking_permit_tokens
                or self._cleanup_permits
                or self._service_owner_permit is not None
                or self._runtime_drain_waiters
                or self._selected_maintenance_thread is not None
            ):
                raise RuntimeOwnershipError("Pipeline admission controller belongs to another runtime")
            previous_identity = self._runtime_identity
            registered_fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.rebind(
                self,
                previous_identity=previous_identity,
                runtime_identity=selected,
            )
            if registered_fatal is not None:
                raise self._runtime_fatal_error(registered_fatal)
            self._runtime_identity = selected
            self._runtime_identity_explicit = True
            self._settings_runtime_identity = selected
            self._process_authority_replaceable = True
            graph.runtime_identity = selected
            graph.graph_nonce = f"{selected}:{id(graph)}"

    def in_flight_for(self, partition_key: str) -> int:
        """Return active work for one availability partition."""
        partition = self._partition(partition_key)
        with self._lock:
            return self._in_flight_by_partition.get(partition, 0)

    @property
    def queued(self) -> int:
        with self._request_path_state_transition():
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

    @property
    def blocking_in_flight(self) -> int:
        """Return workers holding controller-owned blocking permits."""
        with self._lock:
            return len(self._blocking_permit_tokens)

    @property
    def service_owner_in_flight(self) -> int:
        """Return the persistent async service-loop owner count (zero or one)."""
        with self._lock:
            return int(self._service_owner_permit is not None)

    @property
    def runtime_fatal_circuit(self) -> RuntimeFatalCircuit | None:
        """Return bounded fatal-fence metadata without retaining an exception."""
        with self._lock:
            return _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)

    def health_snapshot(self) -> PipelineAdmissionHealthSnapshot:
        """Return one read-only aggregate snapshot without queue maintenance."""
        with self._lock:
            fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
            active = self._in_flight
            queued = self._queued_count
            retained = len(self._release_pending)
            blocking = len(self._blocking_permit_tokens)
            cleanup = len(self._cleanup_permits)
            service_owner = int(self._service_owner_permit is not None)
            saturated = active + len(self._selected) >= self.limit
            degraded = fatal is not None or saturated or queued > 0 or retained > 0
            return PipelineAdmissionHealthSnapshot(
                capacity=self.limit,
                active=active,
                queued=queued,
                retained=retained,
                blocking_in_flight=blocking,
                cleanup_in_flight=cleanup,
                service_owner_in_flight=service_owner,
                saturated=saturated,
                fatal=fatal is not None,
                degraded=degraded,
                reason_code=fatal.reason_code if fatal is not None else None,
            )

    @staticmethod
    def _runtime_fatal_error(record: RuntimeFatalCircuit) -> RuntimeOwnershipError:
        failure = RuntimeOwnershipError("Pipeline runtime cleanup failed")
        setattr(failure, "cleanup_reason_code", record.reason_code)
        setattr(failure, "cleanup_error_type", record.error_type)
        setattr(failure, "cleanup_retains_capacity", False)
        setattr(failure, "runtime_provider_fatal", True)
        return failure

    def raise_if_runtime_fatal(self) -> None:
        """Reject new runtime authority after an unrecoverable cleanup failure."""
        with self._lock:
            record = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
        if record is not None:
            raise self._runtime_fatal_error(record)

    def fence_runtime_fatal(self, error: BaseException) -> RuntimeFatalCircuit:
        """Latch one fatal cleanup circuit and reject queued normal work."""
        raw_reason = getattr(error, "cleanup_reason_code", None)
        raw_error_type = getattr(error, "cleanup_error_type", None)
        candidate = RuntimeFatalCircuit(
            reason_code=_canonical_runtime_fatal_reason(raw_reason),
            error_type=_canonical_runtime_fatal_error_type(raw_error_type, fallback=type(error)),
        )
        record, latched, controllers = _PROCESS_RUNTIME_FATAL_REGISTRY.latch(
            self._runtime_identity,
            candidate,
        )
        for controller in controllers:
            controller._reject_waiters_for_registered_runtime_fatal()
        self._reject_waiters_for_registered_runtime_fatal()
        if latched:
            try:
                logger.error(
                    "pipeline_runtime_fatal_fenced",
                    reason_code=record.reason_code,
                    error_type=record.error_type,
                )
            except BaseException:
                pass
        return record

    def _reject_waiters_for_registered_runtime_fatal(self) -> None:
        """Wake queued and selected normal work after a shared fatal transition."""
        with self._request_path_state_transition():
            if _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity) is None:
                return
            waiters = tuple(waiter for queue in self._queues.values() for waiter in queue.values())
            for waiter in waiters:
                self._remove_waiter_locked(waiter, state="fatal")
            self._selected_maintenance_condition.notify_all()
            self._wake_runtime_drain_waiters_locked()

    def try_acquire_service_owner(self) -> PipelineServiceOwnerPermit | None:
        """Reserve the runtime's single persistent async service-loop owner."""
        with self._lock:
            if not self._accepting_normal_work_locked():
                return None
            if self._service_owner_permit is not None:
                return None
            self._next_service_owner_permit += 1
            permit = PipelineServiceOwnerPermit(
                controller_identity=self._identity,
                runtime_identity=self._runtime_identity,
                permit_id=self._next_service_owner_permit,
            )
            self._service_owner_permit = permit
            return permit

    def release_service_owner(self, permit: PipelineServiceOwnerPermit) -> None:
        """Release a service owner after its loop and owned products terminate."""
        with self._lock:
            if permit.controller_identity is not self._identity:
                raise RuntimeError("pipeline service owner belongs to another controller")
            if permit.runtime_identity != self._runtime_identity:
                raise RuntimeError("pipeline service owner belongs to another runtime")
            if self._service_owner_permit != permit:
                raise RuntimeError("pipeline service owner is not active")
            if self._service_owner_thread_id is not None:
                raise RuntimeError("pipeline service owner is still running")
            self._service_owner_permit = None
            self._wake_runtime_drain_waiters_locked()

    def open_root_generation(self, generation: int) -> None:
        """Open one managed root generation before any composition work starts."""
        if type(generation) is not int or generation <= 0:
            raise RuntimeOwnershipError("Runtime root generation must be a positive integer")
        with self._lock:
            if self._process_authority_retired:
                raise RuntimeOwnershipError("Pipeline admission authority was replaced")
            fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
            if fatal is not None:
                raise self._runtime_fatal_error(fatal)
            if self._runtime_root_state not in {"unmanaged", "closed"}:
                raise RuntimeOwnershipError("Runtime root generation is already active")
            if generation <= self._runtime_root_generation:
                raise RuntimeOwnershipError("Runtime root generation is stale")
            if not self._runtime_idle_locked(include_service_owner=True):
                raise RuntimeOwnershipError("Runtime admission state was not empty before root startup")
            self._runtime_root_generation = generation
            self._runtime_root_state = "active"

    def begin_root_drain(self, generation: int) -> bool:
        """Fence new normal work and reject every queued request for the final root."""
        with self._lock:
            self._validate_root_generation_locked(generation, expected_state="active")
            admitted_work_active = bool(
                self._in_flight
                or self._blocking_permit_tokens
                or self._retained_tasks
                or self._retained_work_permit_tokens
                or self._release_pending
                or self._service_owner_permit is not None
            )
            self._runtime_root_state = "draining"
            waiters = tuple(waiter for queue in self._queues.values() for waiter in queue.values())
            for waiter in waiters:
                self._remove_waiter_locked(waiter, state="rejected")
            self._selected_maintenance_condition.notify_all()
            self._wake_runtime_drain_waiters_locked()
            return admitted_work_active

    def fence_root_generation(self, generation: int) -> None:
        """Terminally fence one active root when its rollback owner cannot settle."""
        with self._lock:
            self._validate_root_generation_locked(generation, expected_state="active")
            self._runtime_root_state = "fenced"
            waiters = tuple(waiter for queue in self._queues.values() for waiter in queue.values())
            for waiter in waiters:
                self._remove_waiter_locked(waiter, state="fatal")
            self._selected_maintenance_condition.notify_all()
            self._wake_runtime_drain_waiters_locked()

    async def wait_for_root_drain(self, generation: int, *, include_service_owner: bool) -> None:
        """Wait for real controller ownership to settle without changing counters."""
        await self._wait_for_root_condition(
            generation,
            include_service_owner=include_service_owner,
            request_paths_only=False,
        )

    async def wait_for_root_request_paths(self, generation: int) -> None:
        """Wait for pre-fence request paths without revoking retained provider work."""
        await self._wait_for_root_condition(
            generation,
            include_service_owner=False,
            request_paths_only=True,
        )

    async def _wait_for_root_condition(
        self,
        generation: int,
        *,
        include_service_owner: bool,
        request_paths_only: bool,
    ) -> None:
        waiter: _RuntimeDrainWaiter | None = None
        with self._lock:
            self._validate_root_generation_locked(generation, expected_state="draining")
            if self._runtime_wait_condition_locked(
                include_service_owner=include_service_owner,
                request_paths_only=request_paths_only,
            ):
                return
            waiter = _RuntimeDrainWaiter(
                generation=generation,
                include_service_owner=include_service_owner,
                request_paths_only=request_paths_only,
                future=ThreadFuture(),
            )
            self._runtime_drain_waiters.append(waiter)
        try:
            await asyncio.shield(asyncio.wrap_future(waiter.future))
        finally:
            with self._lock:
                if waiter in self._runtime_drain_waiters:
                    self._runtime_drain_waiters.remove(waiter)

    def finish_root_drain(self, generation: int) -> None:
        """Close a fully drained managed generation until another root registers."""
        with self._lock:
            self._validate_root_generation_locked(generation, expected_state="draining")
            if not self._runtime_idle_locked(include_service_owner=True):
                raise RuntimeOwnershipError("Runtime root drain completed with live capacity")
            self._runtime_root_state = "closed"
            self._wake_runtime_drain_waiters_locked()

    @property
    def runtime_root_state(self) -> str:
        with self._lock:
            return self._runtime_root_state

    @contextmanager
    def service_owner(self, permit: PipelineServiceOwnerPermit) -> Iterator[None]:
        """Bind one persistent service-loop capability to its owning thread."""
        thread_id = threading.get_ident()
        with self._lock:
            if permit.controller_identity is not self._identity:
                raise RuntimeError("pipeline service owner belongs to another controller")
            if permit.runtime_identity != self._runtime_identity:
                raise RuntimeError("pipeline service owner belongs to another runtime")
            if self._service_owner_permit != permit:
                raise RuntimeError("pipeline service owner is not active")
            if self._service_owner_thread_id is not None:
                raise RuntimeError("pipeline service owner is already running")
            self._service_owner_thread_id = thread_id
        self._service_owner_context.permit = permit
        try:
            yield
        finally:
            del self._service_owner_context.permit
            with self._lock:
                if self._service_owner_thread_id != thread_id:
                    raise RuntimeError("pipeline service owner thread state was corrupted")
                self._service_owner_thread_id = None

    def current_thread_owns_service_owner(self) -> bool:
        """Return whether this thread owns the persistent async service loop."""
        permit = getattr(self._service_owner_context, "permit", None)
        with self._lock:
            return permit is not None and self._service_owner_permit == permit

    async def acquire_cleanup_permits(
        self,
        count: int,
        *,
        partition_key: str = "",
    ) -> tuple[PipelineBlockingPermit, ...]:
        """Reserve cleanup workers on the current lease or one admitted lease."""
        self._validate_cleanup_permit_count(count)
        if count == 0:
            return ()
        with self._lock:
            permits = self._reserve_current_cleanup_permits_locked(count)
            if permits is not None:
                return permits
            if self._current_leases.get() or self._current_worker_permit_locked() is not None:
                raise PipelineAdmissionRejected("pipeline_admission_queue_full")

        lease = await self._acquire(partition_key=partition_key, allow_root_drain=True)
        try:
            with self._lock:
                permits = self._reserve_cleanup_permits_locked(lease.token, count)
                if permits is None:
                    raise PipelineAdmissionRejected("pipeline_admission_queue_full")
                self._mark_release_pending_locked(lease.token)
                return permits
        except BaseException:
            self.release(lease)
            raise

    def try_acquire_cleanup_permits(
        self,
        count: int,
    ) -> tuple[PipelineBlockingPermit, ...] | None:
        """Atomically reserve cleanup workers before synchronous submission."""
        self._validate_cleanup_permit_count(count)
        if count == 0:
            return ()
        with self._request_path_state_transition():
            if self._runtime_root_state == "closed":
                return None
            permits = self._reserve_current_cleanup_permits_locked(count)
            if permits is not None:
                return permits
            if self._current_leases.get() or self._current_worker_permit_locked() is not None:
                return None
            partition = _DEFAULT_PARTITION
            if not self._has_activation_capacity_locked(partition) or self._queues.get(partition):
                return None
            self._next_token += 1
            token = self._next_token
            self._activate_locked(token, partition)
            permits = self._reserve_cleanup_permits_locked(token, count)
            if permits is None:
                self._complete_release_locked(token, partition)
                return None
            self._mark_release_pending_locked(token)
            return permits

    async def acquire_blocking_permit(
        self,
        *,
        timeout_seconds: float | None = None,
        partition_key: str = "",
    ) -> PipelineBlockingPermit:
        """Reserve blocking capacity before a worker thread is submitted."""
        self.raise_if_runtime_fatal()
        with self._lock:
            permit = self._reserve_current_blocking_permit_locked()
            if permit is not None:
                return permit
            if self._current_leases.get():
                raise PipelineAdmissionRejected("pipeline_admission_queue_full")

        lease = await self.acquire(
            timeout_seconds=timeout_seconds,
            partition_key=partition_key,
        )
        try:
            with self._lock:
                permit = self._reserve_blocking_permit_locked(lease.token)
                self._mark_release_pending_locked(lease.token)
                return permit
        except BaseException:
            self.release(lease)
            raise

    def try_acquire_blocking_permit(
        self,
        *,
        partition_key: str = "",
    ) -> PipelineBlockingPermit | None:
        """Reserve immediate blocking capacity without starting or queueing work."""
        partition = self._partition(partition_key)
        with self._request_path_state_transition():
            if _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity) is not None:
                return None
            permit = self._reserve_current_blocking_permit_locked()
            if permit is not None:
                return permit
            if self._current_leases.get():
                return None
            if not self._accepting_normal_work_locked():
                return None
            if len(self._blocking_permit_tokens) >= self.limit:
                return None
            if not self._can_activate_locked(partition) or self._queues.get(partition):
                return None
            self._next_token += 1
            token = self._next_token
            self._activate_locked(token, partition)
            try:
                permit = self._reserve_blocking_permit_locked(token)
            except BaseException:
                self._complete_release_locked(token, partition)
                raise
            self._mark_release_pending_locked(token)
            return permit

    def release_blocking_permit(self, permit: PipelineBlockingPermit) -> None:
        """Release worker capacity from the worker thread that consumed it."""
        with self._request_path_state_transition():
            if permit.controller_identity is not self._identity:
                raise RuntimeError("pipeline blocking permit belongs to another controller")
            if permit.runtime_identity != self._runtime_identity:
                raise RuntimeError("pipeline blocking permit belongs to another runtime")
            token = self._blocking_permit_tokens.get(permit.permit_id)
            if token != permit.token:
                raise RuntimeError("pipeline blocking permit is not active")
            if permit.cleanup != (permit.permit_id in self._cleanup_permits):
                raise RuntimeError("pipeline blocking permit kind was corrupted")
            retained = self._blocking_permits.get(token)
            if retained is None or permit.permit_id not in retained:
                raise RuntimeError("pipeline blocking permit state was corrupted")
            retained.remove(permit.permit_id)
            self._blocking_permit_tokens.pop(permit.permit_id, None)
            self._cleanup_permits.discard(permit.permit_id)
            if not retained:
                self._blocking_permits.pop(token, None)
            self._complete_pending_release_if_idle_locked(token)

    def validate_cleanup_permits(self, permits: tuple[PipelineBlockingPermit, ...]) -> None:
        """Validate a complete cleanup group without consuming any permit."""
        with self._lock:
            seen: set[int] = set()
            for permit in permits:
                if not isinstance(permit, PipelineBlockingPermit):
                    raise RuntimeError("cleanup permit has an invalid type")
                if permit.controller_identity is not self._identity:
                    raise RuntimeError("pipeline blocking permit belongs to another controller")
                if permit.runtime_identity != self._runtime_identity:
                    raise RuntimeError("pipeline blocking permit belongs to another runtime")
                if permit.permit_id in seen:
                    raise RuntimeError("cleanup permits must be unique")
                seen.add(permit.permit_id)
                if not permit.cleanup:
                    raise RuntimeError("pipeline blocking permit is not a cleanup permit")
                if self._blocking_permit_tokens.get(permit.permit_id) != permit.token:
                    raise RuntimeError("pipeline blocking permit is not active")
                retained = self._blocking_permits.get(permit.token)
                if retained is None or permit.permit_id not in retained:
                    raise RuntimeError("pipeline blocking permit state was corrupted")
                if permit.permit_id not in self._cleanup_permits:
                    raise RuntimeError("pipeline blocking permit kind was corrupted")

    @contextmanager
    def blocking_worker(self, permit: PipelineBlockingPermit) -> Iterator[None]:
        """Expose one active permit only to its worker thread for child cleanup."""
        with self._lock:
            if permit.controller_identity is not self._identity:
                raise RuntimeError("pipeline blocking permit belongs to another controller")
            if permit.runtime_identity != self._runtime_identity:
                raise RuntimeError("pipeline blocking permit belongs to another runtime")
            if self._blocking_permit_tokens.get(permit.permit_id) != permit.token:
                raise RuntimeError("pipeline blocking permit is not active")
        current = getattr(self._blocking_worker_context, "permits", ())
        self._blocking_worker_context.permits = (*current, permit)
        try:
            yield
        finally:
            if current:
                self._blocking_worker_context.permits = current
            else:
                del self._blocking_worker_context.permits

    def current_thread_owns_blocking_capacity(self) -> bool:
        """Return whether this thread is already executing under one owned permit."""
        with self._lock:
            return self._current_worker_permit_locked() is not None

    def current_execution_owns_capacity(self) -> bool:
        """Return whether this context may finish work admitted before a root fence."""
        with self._lock:
            owns_lease = any(token in self._active_leases for token in self._current_leases.get())
            owns_worker = self._current_worker_permit_locked() is not None
            owns_service = (
                self._service_owner_permit is not None and self._service_owner_thread_id == threading.get_ident()
            )
            return owns_lease or owns_worker or owns_service

    def current_thread_can_reuse_blocking_capacity(self, *, cleanup: bool) -> bool:
        """Authorize same-worker re-entry without changing the permit's purpose."""
        with self._lock:
            fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
            if fatal is not None and not cleanup:
                raise self._runtime_fatal_error(fatal)
            permit = self._current_worker_permit_locked()
            if permit is None:
                return False
            if permit.cleanup and not cleanup:
                raise RuntimeOwnershipError("Cleanup capacity cannot authorize normal blocking work")
            return True

    async def acquire(
        self,
        *,
        timeout_seconds: float | None = None,
        partition_key: str = "",
    ) -> PipelineAdmissionLease:
        return await self._acquire(
            timeout_seconds=timeout_seconds,
            partition_key=partition_key,
            allow_root_drain=False,
        )

    async def _acquire(
        self,
        *,
        timeout_seconds: float | None = None,
        partition_key: str = "",
        allow_root_drain: bool,
    ) -> PipelineAdmissionLease:
        started_at = time.monotonic()
        deadline = started_at + timeout_seconds if timeout_seconds is not None else None
        loop = asyncio.get_running_loop()
        partition = self._partition(partition_key)
        waiter: _Waiter | None = None
        queue_depth = 0
        partition_queue_depth = 0

        with self._request_path_state_transition():
            fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
            if fatal is not None and not allow_root_drain:
                raise self._runtime_fatal_error(fatal)
            accepting = self._accepting_normal_work_locked()
            cleanup_drain = allow_root_drain and (
                self._runtime_root_state == "draining"
                or (fatal is not None and self._runtime_root_state in {"unmanaged", "active"})
            )
            if not accepting and not cleanup_drain:
                raise PipelineAdmissionRejected("pipeline_runtime_not_active")
            self._next_token += 1
            token = self._next_token
            self._maintain_selected_locked(started_at)
            self._notify_available_locked(started_at)
            if self._has_activation_capacity_locked(partition) and not self._queues.get(partition):
                self._activate_locked(token, partition)
            else:
                if cleanup_drain:
                    raise PipelineAdmissionRejected("pipeline_admission_queue_full")
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
                    with self._request_path_state_transition():
                        now = time.monotonic()
                        fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
                        if fatal is not None:
                            self._remove_waiter_locked(waiter, state="fatal")
                            raise self._runtime_fatal_error(fatal)
                        self._maintain_selected_locked(now)
                        self._notify_available_locked(now)
                        if waiter.state == "selected":
                            self._claim_waiter_locked(waiter, now)
                            break
                        if waiter.state == "fatal":
                            record = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
                            if record is None:
                                raise RuntimeOwnershipError("Pipeline runtime fatal circuit was lost")
                            raise self._runtime_fatal_error(record)
                        if waiter.state == "rejected":
                            raise PipelineAdmissionRejected("pipeline_admission_queue_full")
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
                with self._request_path_state_transition():
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
        with self._request_path_state_transition():
            self._validate_active_lease_locked(lease)
            if lease.token in self._release_pending:
                raise RuntimeError("pipeline admission lease is not active")
            self._mark_release_pending_locked(lease.token)
            self._complete_pending_release_if_idle_locked(lease.token)

    def retain_task(self, lease: PipelineAdmissionLease, task: asyncio.Task[Any]) -> bool:
        """Keep a lease charged until resistant work has actually terminated."""
        with self._lock:
            self._validate_active_lease_locked(lease)
            return self._retain_task_locked(lease.token, task)

    def retain_current_task(self, task: asyncio.Task[Any]) -> bool:
        """Charge cleanup spawned by the current run to its effective-work lease."""
        with self._request_path_state_transition():
            for token in reversed(self._current_leases.get()):
                if token in self._active_leases:
                    return self._retain_task_locked(token, task)
            if not self._accepting_normal_work_locked():
                return False
            if self._in_flight + len(self._selected) >= self.limit:
                return False
            self._next_token += 1
            token = self._next_token
            partition = _DEFAULT_PARTITION
            self._activate_locked(token, partition)
            self._mark_release_pending_locked(token)
            retained = self._retain_task_locked(token, task)
            if not retained:
                self._complete_pending_release_if_idle_locked(token)
            return retained

    def retain_current_work(self) -> PipelineRetainedWorkPermit:
        """Transfer the current request lease to controller-owned terminal work."""
        with self._request_path_state_transition():
            for token in reversed(self._current_leases.get()):
                if token not in self._active_leases or token in self._release_pending:
                    continue
                self._next_retained_work_permit += 1
                permit_id = self._next_retained_work_permit
                self._retained_work_permits.setdefault(token, set()).add(permit_id)
                self._retained_work_permit_tokens[permit_id] = token
                return PipelineRetainedWorkPermit(
                    controller_identity=self._identity,
                    runtime_identity=self._runtime_identity,
                    token=token,
                    permit_id=permit_id,
                )
        raise PipelineAdmissionRejected("pipeline_admission_queue_full")

    def release_retained_work(self, permit: PipelineRetainedWorkPermit) -> None:
        """Release transferred work from its actual cross-runtime terminal edge."""
        with self._request_path_state_transition():
            if permit.controller_identity is not self._identity:
                raise RuntimeError("pipeline retained-work permit belongs to another controller")
            if permit.runtime_identity != self._runtime_identity:
                raise RuntimeError("pipeline retained-work permit belongs to another runtime")
            token = self._retained_work_permit_tokens.get(permit.permit_id)
            if token != permit.token:
                raise RuntimeError("pipeline retained-work permit is not active")
            retained = self._retained_work_permits.get(token)
            if retained is None or permit.permit_id not in retained:
                raise RuntimeError("pipeline retained-work permit state was corrupted")
            retained.remove(permit.permit_id)
            self._retained_work_permit_tokens.pop(permit.permit_id, None)
            if not retained:
                self._retained_work_permits.pop(token, None)
            self._complete_pending_release_if_idle_locked(token)

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

    def _reserve_current_blocking_permit_locked(self) -> PipelineBlockingPermit | None:
        for token in reversed(self._current_leases.get()):
            if token not in self._active_leases:
                continue
            if token in self._release_pending:
                return None
            return self._reserve_blocking_permit_locked(token)
        return None

    def _reserve_current_cleanup_permits_locked(
        self,
        count: int,
    ) -> tuple[PipelineBlockingPermit, ...] | None:
        for token in reversed(self._current_leases.get()):
            if token not in self._active_leases or token in self._release_pending:
                continue
            return self._reserve_cleanup_permits_locked(token, count)
        worker_permit = self._current_worker_permit_locked()
        if worker_permit is None:
            return None
        return self._reserve_cleanup_permits_locked(worker_permit.token, count)

    def _current_worker_permit_locked(self) -> PipelineBlockingPermit | None:
        permits = getattr(self._blocking_worker_context, "permits", ())
        permit = permits[-1] if permits else None
        if not isinstance(permit, PipelineBlockingPermit):
            return None
        if permit.controller_identity is not self._identity:
            return None
        if permit.runtime_identity != self._runtime_identity:
            return None
        if self._blocking_permit_tokens.get(permit.permit_id) != permit.token:
            return None
        return permit

    def _validate_cleanup_permit_count(self, count: int) -> None:
        if type(count) is not int:
            raise ValueError("cleanup permit count must be an integer")
        if count < 0 or count > self._cleanup_permit_limit_per_token:
            raise ValueError("cleanup permit count exceeds the per-lease limit")

    def _reserve_cleanup_permits_locked(
        self,
        token: int,
        count: int,
    ) -> tuple[PipelineBlockingPermit, ...] | None:
        if token not in self._active_leases:
            return None
        retained = self._blocking_permits.get(token, set())
        active_cleanup = sum(permit_id in self._cleanup_permits for permit_id in retained)
        if active_cleanup + count > self._cleanup_permit_limit_per_token:
            return None
        permits: list[PipelineBlockingPermit] = []
        for _ in range(count):
            self._next_blocking_permit += 1
            permit_id = self._next_blocking_permit
            self._blocking_permits.setdefault(token, set()).add(permit_id)
            self._blocking_permit_tokens[permit_id] = token
            self._cleanup_permits.add(permit_id)
            permits.append(
                PipelineBlockingPermit(
                    controller_identity=self._identity,
                    runtime_identity=self._runtime_identity,
                    token=token,
                    permit_id=permit_id,
                    cleanup=True,
                )
            )
        return tuple(permits)

    def _reserve_blocking_permit_locked(self, token: int) -> PipelineBlockingPermit:
        fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
        if fatal is not None:
            raise self._runtime_fatal_error(fatal)
        if token not in self._active_leases or token in self._release_pending:
            raise PipelineAdmissionRejected("pipeline_admission_queue_full")
        normal_permits = len(self._blocking_permit_tokens) - len(self._cleanup_permits)
        if normal_permits >= self.limit:
            raise PipelineAdmissionRejected("pipeline_admission_queue_full")
        self._next_blocking_permit += 1
        permit_id = self._next_blocking_permit
        self._blocking_permits.setdefault(token, set()).add(permit_id)
        self._blocking_permit_tokens[permit_id] = token
        return PipelineBlockingPermit(
            controller_identity=self._identity,
            runtime_identity=self._runtime_identity,
            token=token,
            permit_id=permit_id,
        )

    def _can_activate_locked(self, partition: str) -> bool:
        return self._accepting_normal_work_locked() and self._has_activation_capacity_locked(partition)

    def _has_activation_capacity_locked(self, partition: str) -> bool:
        return self._in_flight + len(self._selected) < self.limit and self._partition_has_capacity_locked(partition)

    def _partition_has_capacity_locked(self, partition: str) -> bool:
        reserved = self._selected_by_partition.get(partition, 0)
        active = self._in_flight_by_partition.get(partition, 0)
        return active + reserved < self.max_in_flight_per_partition

    def _activate_locked(self, token: int, partition: str) -> None:
        if token in self._active_leases:
            raise RuntimeError("pipeline admission lease identity was reused")
        self._active_leases[token] = partition
        if self._service_owner_permit is not None and self._service_owner_thread_id == threading.get_ident():
            self._service_admitted_tokens.add(token)
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
        with self._request_path_state_transition():
            retained = self._retained_tasks.get(token)
            if retained is None or task not in retained:
                return
            retained.remove(task)
            self._retained_task_tokens.pop(task, None)
            if not retained:
                self._retained_tasks.pop(token, None)
            self._complete_pending_release_if_idle_locked(token)

    def _complete_pending_release_if_idle_locked(self, token: int) -> None:
        if token not in self._release_pending or self._has_retained_work_locked(token):
            return
        partition = self._active_leases.get(token)
        if partition is None:
            return
        self._release_pending.remove(token)
        self._complete_release_locked(token, partition)

    def _mark_release_pending_locked(self, token: int) -> None:
        self._release_pending.add(token)
        self._wake_runtime_drain_waiters_locked()

    def _has_retained_work_locked(self, token: int) -> bool:
        return bool(
            self._retained_tasks.get(token)
            or self._retained_work_permits.get(token)
            or self._blocking_permits.get(token)
        )

    def _complete_release_locked(self, token: int, partition: str) -> None:
        if self._has_retained_work_locked(token):
            raise RuntimeError("pipeline admission lease still has retained work")
        self._active_leases.pop(token)
        self._service_admitted_tokens.discard(token)
        self._release_pending.discard(token)
        self._in_flight -= 1
        self._decrement_partition_count(self._in_flight_by_partition, partition)
        self._refresh_partition_eligibility_locked(partition)
        now = time.monotonic()
        self._maintain_selected_locked(now)
        self._notify_available_locked(now)
        self._wake_runtime_drain_waiters_locked()

    @staticmethod
    def _consume_task(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except BaseException:
            return

    def _claim_waiter_locked(self, waiter: _Waiter, now: float) -> None:
        fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
        if fatal is not None:
            self._remove_waiter_locked(waiter, state="fatal")
            raise self._runtime_fatal_error(fatal)
        queue = self._queues.get(waiter.partition)
        if queue is None or next(iter(queue), None) != waiter.token:
            raise RuntimeError("pipeline admission queue order was corrupted")
        if waiter.token not in self._selected:
            raise RuntimeError("pipeline admission selected state was corrupted")
        queue.pop(waiter.token)
        self._selected.pop(waiter.token)
        self._selected_claims.pop(waiter.token, None)
        waiter.claim_deadline = None
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
            self._selected_claims.pop(waiter.token, None)
            waiter.claim_deadline = None
            self._decrement_partition_count(self._selected_by_partition, waiter.partition)
        self._queued_count -= 1
        waiter.state = state
        self._refresh_partition_eligibility_locked(waiter.partition)
        if state in {"abandoned", "expired", "rejected", "fatal"}:
            self._wake_waiter_locked(waiter)
        self._wake_runtime_drain_waiters_locked()
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

    def _maintain_selected_locked(self, now: float) -> int:
        # Inspect a fixed rotating sample. Removed tokens are discarded lazily,
        # so handoff cost stays amortized even at the supported 1,000-slot limit.
        checks = 0
        while checks < _SELECTED_MAINTENANCE_BUDGET and self._selected_claims:
            token, claim_deadline = next(iter(self._selected_claims.items()))
            if claim_deadline > now:
                break
            checks += 1
            waiter = self._selected.get(token)
            if waiter is None or waiter.claim_deadline != claim_deadline:
                self._selected_claims.pop(token, None)
                continue
            self._remove_waiter_locked(
                waiter,
                state="expired" if waiter.deadline is not None and now >= waiter.deadline else "abandoned",
            )

        rotating_checks = min(
            _SELECTED_MAINTENANCE_BUDGET - checks,
            len(self._selected_checks),
        )
        for _ in range(rotating_checks):
            checks += 1
            token = self._selected_checks.popleft()
            waiter = self._selected.get(token)
            if waiter is None:
                continue
            expired = waiter.deadline is not None and now >= waiter.deadline
            unavailable = waiter.loop.is_closed() or not waiter.loop.is_running()
            claim_expired = waiter.claim_deadline is not None and now >= waiter.claim_deadline
            if expired or unavailable or claim_expired:
                self._remove_waiter_locked(
                    waiter,
                    state="expired" if expired else "abandoned",
                )
            else:
                self._selected_checks.append(token)
        return checks

    def _notify_available_locked(
        self,
        now: float,
        *,
        retry_maintenance_start: bool = True,
    ) -> None:
        now = max(now, time.monotonic())
        fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity)
        if fatal is not None:
            fatal_waiters = tuple(waiter for queue in self._queues.values() for waiter in queue.values())
            for fatal_waiter in fatal_waiters:
                self._remove_waiter_locked(fatal_waiter, state="fatal")
            self._selected_maintenance_condition.notify_all()
            self._wake_runtime_drain_waiters_locked()
            return
        if not self._accepting_normal_work_locked():
            self._schedule_selected_maintenance_locked(retry_on_failure=retry_maintenance_start)
            return
        while self._in_flight + len(self._selected) < self.limit:
            waiter = self._next_waiter_locked(now)
            if waiter is None:
                break
            waiter.state = "selected"
            waiter.claim_deadline = now + self._selected_claim_lease_seconds
            self._selected[waiter.token] = waiter
            self._selected_checks.append(waiter.token)
            self._selected_claims[waiter.token] = waiter.claim_deadline
            self._selected_by_partition[waiter.partition] = self._selected_by_partition.get(waiter.partition, 0) + 1
            self._refresh_partition_eligibility_locked(waiter.partition)
            if not self._wake_waiter_locked(waiter):
                self._remove_waiter_locked(waiter, state="abandoned")
        self._schedule_selected_maintenance_locked(retry_on_failure=retry_maintenance_start)

    def _schedule_selected_maintenance_locked(self, *, retry_on_failure: bool = True) -> None:
        maintenance_thread = self._selected_maintenance_thread
        if maintenance_thread is not None:
            self._selected_maintenance_condition.notify()
            return
        if not self._selected_claims:
            return

        try:
            maintenance_thread = _start_lifecycle_owner_thread(
                target=self._run_selected_maintenance,
                name=f"tacit-pipeline-admission-maintenance-{id(self)}",
                install=lambda thread: setattr(self, "_selected_maintenance_thread", thread),
                daemon=True,
            )
        except _LifecycleOwnerStartupError as exc:
            if exc.thread_alive:
                logger.warning(
                    "pipeline_admission_maintenance_start_ambiguous",
                    error_type=type(exc.cause).__name__,
                    selected_claims_retained=len(self._selected_claims),
                )
                return
            self._selected_maintenance_thread = None
            now = time.monotonic()
            reclaimed = self._requeue_selected_after_maintenance_start_failure_locked(now)
            if retry_on_failure and self._queued_count:
                logger.warning(
                    "pipeline_admission_maintenance_start_failed",
                    error_type=type(exc.cause).__name__,
                    selected_claims_reclaimed=reclaimed,
                    retrying=True,
                )
                self._notify_available_locked(
                    now,
                    retry_maintenance_start=False,
                )
                return

            rejected = self._reject_waiters_after_maintenance_start_failure_locked()
            logger.warning(
                "pipeline_admission_maintenance_start_failed",
                error_type=type(exc.cause).__name__,
                selected_claims_reclaimed=reclaimed,
                rejected_waiters=rejected,
                retrying=False,
            )

    def _requeue_selected_after_maintenance_start_failure_locked(self, now: float) -> int:
        selected_waiters = tuple(self._selected.values())
        for waiter in selected_waiters:
            if self._selected.pop(waiter.token, None) is not waiter:
                raise RuntimeError("pipeline admission selected state was corrupted")
            if self._selected_claims.pop(waiter.token, None) is None:
                raise RuntimeError("pipeline admission selected claim state was corrupted")
            self._decrement_partition_count(self._selected_by_partition, waiter.partition)
            waiter.claim_deadline = None

            expired = waiter.deadline is not None and now >= waiter.deadline
            unavailable = waiter.loop.is_closed() or not waiter.loop.is_running()
            if expired or unavailable:
                self._remove_waiter_locked(
                    waiter,
                    state="expired" if expired else "abandoned",
                )
                continue

            waiter.state = "queued"
            self._refresh_partition_eligibility_locked(waiter.partition)

        self._selected_checks.clear()
        return len(selected_waiters)

    def _reject_waiters_after_maintenance_start_failure_locked(self) -> int:
        waiters = tuple(waiter for queue in self._queues.values() for waiter in queue.values())
        rejected = 0
        for waiter in waiters:
            if self._remove_waiter_locked(waiter, state="rejected"):
                rejected += 1
        return rejected

    def _run_selected_maintenance(self) -> None:
        # One event-driven worker covers all selected claims and leaves after a
        # bounded idle grace; queued-but-unselected work never starts it.
        maintenance_thread = threading.current_thread()
        idle_deadline: float | None = None
        while True:
            due_again = False
            should_exit = False
            idle_callbacks: tuple[Callable[[], None], ...] = ()
            with self._selected_maintenance_condition:
                if self._selected_maintenance_thread is not maintenance_thread:
                    return
                now = time.monotonic()
                next_deadline = next(iter(self._selected_claims.values()), None)
                if next_deadline is None:
                    if (
                        self._runtime_root_state in {"draining", "closed"}
                        or _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity) is not None
                    ):
                        should_exit = True
                    else:
                        if idle_deadline is None:
                            idle_deadline = now + self._selected_maintenance_idle_seconds
                        remaining = idle_deadline - now
                        if remaining <= 0:
                            should_exit = True
                        else:
                            self._selected_maintenance_condition.wait(timeout=remaining)
                            continue
                else:
                    idle_deadline = None
                    remaining = next_deadline - now
                    if remaining > 0:
                        self._selected_maintenance_condition.wait(timeout=remaining)
                        continue

                    self._maintain_selected_locked(now)
                    self._notify_available_locked(now)
                    next_deadline = next(iter(self._selected_claims.values()), None)
                    due_again = next_deadline is not None and next_deadline <= now

                if should_exit:
                    self._selected_maintenance_thread = None
                    self._wake_runtime_drain_waiters_locked()
                    idle_callbacks = self._take_request_paths_idle_callbacks_locked()

            self._invoke_request_paths_idle_callbacks(idle_callbacks)
            if should_exit:
                return
            if due_again:
                time.sleep(0)

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
            if asyncio.get_running_loop() is waiter.loop:
                waiter.event.set()
                return True
        except RuntimeError:
            pass
        try:
            waiter.loop.call_soon_threadsafe(waiter.event.set)
        except RuntimeError:
            return False
        return True

    def _accepting_normal_work_locked(self) -> bool:
        return (
            not self._process_authority_retired
            and _PROCESS_RUNTIME_FATAL_REGISTRY.get(self._runtime_identity) is None
            and self._runtime_root_state in {"unmanaged", "active"}
        )

    def _runtime_idle_locked(self, *, include_service_owner: bool) -> bool:
        return not (
            self._in_flight
            or self._queued_count
            or self._selected
            or self._retained_tasks
            or self._retained_work_permit_tokens
            or self._blocking_permit_tokens
            or self._release_pending
            or self._selected_maintenance_thread is not None
            or (include_service_owner and self._service_owner_permit is not None)
        )

    def _runtime_request_paths_idle_locked(self) -> bool:
        # A release-pending requester can still retain a cancellation-resistant
        # task or worker that uses the provider. Only service-loop work belongs
        # to provider shutdown; every requester-owned lease must really finish.
        active_request_tokens = self._active_leases.keys() - self._service_admitted_tokens
        return not (
            active_request_tokens
            or self._queued_count
            or self._selected
            or self._selected_maintenance_thread is not None
        )

    def when_request_paths_idle(self, callback: Callable[[], None]) -> None:
        """Invoke one bounded callback after all requester-owned work settles."""
        invoke_now = False
        with self._lock:
            if self._runtime_request_paths_idle_locked():
                invoke_now = True
            elif callback not in self._request_paths_idle_callbacks:
                self._request_paths_idle_callbacks.append(callback)
        if invoke_now:
            self._invoke_request_paths_idle_callbacks((callback,))

    @contextmanager
    def _request_path_state_transition(self) -> Iterator[None]:
        """Collect an idle handoff atomically and invoke it after unlocking."""
        idle_callbacks: tuple[Callable[[], None], ...] = ()
        try:
            with self._lock:
                try:
                    yield
                finally:
                    idle_callbacks = self._take_request_paths_idle_callbacks_locked()
        finally:
            self._invoke_request_paths_idle_callbacks(idle_callbacks)

    def _take_request_paths_idle_callbacks_locked(self) -> tuple[Callable[[], None], ...]:
        if not self._runtime_request_paths_idle_locked() or not self._request_paths_idle_callbacks:
            return ()
        callbacks = tuple(self._request_paths_idle_callbacks)
        self._request_paths_idle_callbacks.clear()
        return callbacks

    @staticmethod
    def _invoke_request_paths_idle_callbacks(
        callbacks: tuple[Callable[[], None], ...],
    ) -> None:
        for callback in callbacks:
            try:
                callback()
            except BaseException as exc:
                logger.error(
                    "pipeline_request_paths_idle_callback_failed",
                    reason_code="runtime_root_deferred_release_failed",
                    error_type=type(exc).__name__,
                )

    def _runtime_wait_condition_locked(
        self,
        *,
        include_service_owner: bool,
        request_paths_only: bool,
    ) -> bool:
        if request_paths_only:
            return self._runtime_request_paths_idle_locked()
        return self._runtime_idle_locked(include_service_owner=include_service_owner)

    def _validate_root_generation_locked(self, generation: int, *, expected_state: str) -> None:
        if generation != self._runtime_root_generation:
            raise RuntimeOwnershipError("Runtime root generation is stale")
        if self._runtime_root_state != expected_state:
            raise RuntimeOwnershipError(f"Runtime root is not {expected_state}")

    def _wake_runtime_drain_waiters_locked(self) -> None:
        for waiter in tuple(self._runtime_drain_waiters):
            if waiter.generation != self._runtime_root_generation:
                if not waiter.future.done():
                    waiter.future.set_exception(RuntimeOwnershipError("Runtime root generation changed during drain"))
                self._runtime_drain_waiters.remove(waiter)
                continue
            if self._runtime_wait_condition_locked(
                include_service_owner=waiter.include_service_owner,
                request_paths_only=waiter.request_paths_only,
            ):
                if not waiter.future.done():
                    waiter.future.set_result(None)
                self._runtime_drain_waiters.remove(waiter)


_RUNTIME_ADMISSION_CONTROLLERS_LOCK = threading.RLock()
_RUNTIME_ADMISSION_CONTROLLERS: weakref.WeakValueDictionary[
    str,
    PipelineAdmissionController,
] = weakref.WeakValueDictionary()


def runtime_admission_health_snapshot(
    runtime_identity: str,
) -> PipelineAdmissionHealthSnapshot | None:
    """Read one canonical controller without constructing runtime authority."""
    selected_identity = str(runtime_identity or "").strip()
    if not selected_identity:
        return None
    with _RUNTIME_ADMISSION_CONTROLLERS_LOCK:
        controller = _RUNTIME_ADMISSION_CONTROLLERS.get(selected_identity)
    if controller is None:
        return None
    return controller.health_snapshot()


def runtime_admission_controller(
    runtime_settings: Any,
    *,
    runtime_identity: str,
) -> PipelineAdmissionController:
    """Return the one process controller for an explicit runtime identity."""
    selected_identity = str(runtime_identity or "").strip()
    if not selected_identity:
        raise RuntimeOwnershipError("Pipeline admission runtime identity is required")
    fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(selected_identity)
    if fatal is not None:
        raise PipelineAdmissionController._runtime_fatal_error(fatal)
    limits = pipeline_admission_limits(runtime_settings)
    with _RUNTIME_ADMISSION_CONTROLLERS_LOCK:
        fatal = _PROCESS_RUNTIME_FATAL_REGISTRY.get(selected_identity)
        if fatal is not None:
            raise PipelineAdmissionController._runtime_fatal_error(fatal)
        existing = _RUNTIME_ADMISSION_CONTROLLERS.get(selected_identity)
        if existing is not None:
            configured = (
                existing.limit,
                existing.max_in_flight_per_partition,
                existing.max_queued,
                existing.max_queued_per_partition,
            )
            expected = (
                limits.concurrent,
                limits.concurrent_per_partition,
                limits.queued,
                limits.queued_per_partition,
            )
            if configured != expected:
                raise RuntimeOwnershipError("Pipeline admission limits disagree for one runtime")
            return existing
        controller = PipelineAdmissionController(
            limits.concurrent,
            max_queued=limits.queued,
            max_queued_per_partition=limits.queued_per_partition,
            max_in_flight_per_partition=limits.concurrent_per_partition,
            runtime_identity=selected_identity,
        )
        if _RUNTIME_ADMISSION_CONTROLLERS.get(selected_identity) is not controller:
            raise RuntimeOwnershipError("Pipeline admission runtime identity claim was lost")
        return controller
