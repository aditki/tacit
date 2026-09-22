"""Application dependency containers."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import threading
import weakref
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Coroutine, Iterator
from concurrent.futures import Future as ThreadFuture
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from enum import StrEnum
from pathlib import Path
from typing import Any, cast

import structlog

from tacit.agents.providers.base import LLMProvider
from tacit.backends.base import DashboardBackend
from tacit.cache import make_cache_key
from tacit.config import Settings, settings
from tacit.context.base import ContextProvider
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import (
    _LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS,
    PipelineAdmissionController,
    PipelineAdmissionLease,
    PipelineRetainedWorkPermit,
    PipelineServiceOwnerPermit,
    RuntimeFatalCircuit,
    RuntimeRootDrainStartupError,
    RuntimeRootOwnerHandle,
    _LifecycleOwnerStartupError,
    _start_lifecycle_owner_thread,
    fence_runtime_root_after_transport_failure,
    pipeline_admission_limits,
    release_runtime_root_with_startup_retry,
)
from tacit.runtime_ownership import (
    DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS,
    BedrockCredentialPlan,
    RuntimeOwnedFactory,
    RuntimeOwnershipDescriptor,
    RuntimeOwnershipMismatchError,
    RuntimeRemoteIdentity,
    canonical_aws_sts_endpoint,
    canonical_bedrock_runtime_endpoint,
    declare_runtime_factory,
    describe_runtime_owner,
    get_runtime_factory_ownership,
    get_runtime_ownership,
    observe_runtime_factory_failure,
    observe_runtime_factory_realization,
    require_compatible_runtime_ownership,
    require_runtime_factory_ownership,
    require_runtime_store_ownership,
    resolve_runtime_settings,
    runtime_descriptor_for_backends,
    runtime_descriptor_for_provider,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
    snapshot_runtime_settings,
    validate_runtime_cleanup_grace_seconds,
)
from tacit.runtime_stores import (
    RuntimeStores,
    get_process_runtime_stores,
    require_shared_signal_knowledge_admission,
)

logger = structlog.get_logger()
DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS = DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS
_validate_cleanup_grace_seconds = validate_runtime_cleanup_grace_seconds
_PROVIDER_OWNER_TERMINAL_POLL_SECONDS = 0.01
_PROVIDER_OWNER_STOP_ATTEMPTS = 2


def _consume_cleanup_task(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except BaseException:
        return


def _require_lifecycle_callback_owner(
    *,
    boundary: str,
    owner: object | None,
    callbacks: tuple[tuple[object | None, str], ...],
) -> object:
    """Bind a lifecycle capability's callbacks to one explicit owner."""
    if owner is None:
        raise RuntimeOwnershipError(f"{boundary} requires an explicit owner")
    for callback, method_name in callbacks:
        expected = getattr(owner, method_name, None)
        if callback is None or expected is None or callback != expected:
            raise RuntimeOwnershipError(f"{boundary} callbacks must belong to one lifecycle owner")
    return owner


def _admitted_provider_lifecycle_owner(factory: object) -> object | None:
    if not isinstance(factory, RuntimeOwnedFactory):
        return None
    accessor = factory.factory
    if not isinstance(accessor, _AdmittedProviderAccessor):
        return None
    return getattr(accessor.getter, "__self__", None)


def _factory_preflight(
    factory: Callable[[], Any],
    *,
    expected: RuntimeOwnershipDescriptor,
    factory_kind: str,
) -> RuntimeOwnershipDescriptor:
    """Validate a public factory declaration without invoking the factory."""
    return require_runtime_factory_ownership(
        boundary=f"Pipeline {factory_kind} factory preflight",
        factory=factory,
        expected=expected,
        factory_kind=factory_kind,
    )


def _lifecycle_blocking_work(lifecycle: PipelineAdmissionController) -> Any:
    """Construct the worker bridge lazily to avoid the pipeline package cycle."""
    from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork

    return LifecycleOwnedBlockingWork(lifecycle)


def _expected_provider_declaration(
    runtime_settings: Settings,
    factory: Callable[[], Any],
) -> RuntimeOwnershipDescriptor:
    """Bind Bedrock validation to its plan without rereading ambient identity."""
    expected = runtime_descriptor_for_provider(
        component="pipeline_provider_settings",
        runtime_settings=runtime_settings,
        capability="llm",
    )
    if str(runtime_settings.llm_provider or "").strip().casefold() != "bedrock":
        return expected
    declared = get_runtime_factory_ownership(factory, expected_kind="provider:llm")
    settings_bound = replace(
        declared,
        component="pipeline_provider_settings",
        settings_identity=expected.settings_identity,
        tenant_policy=expected.tenant_policy,
    )
    require_compatible_runtime_ownership(
        boundary="Pipeline Bedrock provider plan settings",
        descriptors=(declared, settings_bound),
    )
    remotes = {remote.provider: remote for remote in declared.remotes}
    if set(remotes) not in ({"llm:bedrock"}, {"llm:bedrock", "llm:bedrock:sts"}):
        raise RuntimeOwnershipError("Pipeline Bedrock provider plan declares invalid remotes")
    region = str(runtime_settings.llm_bedrock_region or "").strip()
    if remotes["llm:bedrock"].endpoint != canonical_bedrock_runtime_endpoint(region):
        raise RuntimeOwnershipError("Pipeline Bedrock provider plan declares an invalid endpoint")
    sts_remote = remotes.get("llm:bedrock:sts")
    if sts_remote is not None and sts_remote.endpoint != canonical_aws_sts_endpoint(region):
        raise RuntimeOwnershipError("Pipeline Bedrock provider plan declares an invalid STS endpoint")
    return replace(declared, component="pipeline_provider_settings")


def _cleanup_rejected_products(
    lifecycle: PipelineAdmissionController,
    products: tuple[Any, ...],
    *,
    cleanup_grace_seconds: float,
    reason_code: str,
    inline: bool = False,
) -> None:
    """Transfer rejected realized products to the runtime cleanup owner."""
    if inline:
        if not lifecycle.current_thread_owns_blocking_capacity():
            raise RuntimeOwnershipError("Rejected resource cleanup has no admitted cleanup owner")
        for product in products:
            try:
                if not _retire_rejected_product_blocking(
                    product,
                    reason_code=reason_code,
                    cleanup_grace_seconds=cleanup_grace_seconds,
                ):
                    _close_product_blocking(product)
            except BaseException as exc:
                logger.warning(
                    "pipeline_rejected_resource_cleanup_failed",
                    reason_code=reason_code,
                    resource="rejected_runtime_product",
                    error_type=type(exc).__name__,
                )
        return
    if products:
        from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork

        cleanup_work = LifecycleOwnedBlockingWork(lifecycle)

        def close_products() -> None:
            for product in products:
                try:
                    if not _retire_rejected_product_blocking(
                        product,
                        reason_code=reason_code,
                        cleanup_grace_seconds=cleanup_grace_seconds,
                    ):
                        _close_product_blocking(product)
                except BaseException as exc:
                    logger.warning(
                        "pipeline_rejected_resource_cleanup_failed",
                        reason_code=reason_code,
                        resource="rejected_runtime_product",
                        error_type=type(exc).__name__,
                    )

        if not cleanup_work.run_background(close_products, reason_code=reason_code):
            logger.warning(
                "pipeline_cleanup_budget_exhausted",
                reason_code="pipeline_cleanup_budget_exhausted",
            )
            raise RuntimeOwnershipError("Rejected resource cleanup has no admitted cleanup owner")


def _retire_rejected_product_blocking(
    product: Any,
    *,
    reason_code: str,
    cleanup_grace_seconds: float,
) -> bool:
    """Run one rejected-product handoff from an admitted blocking owner."""
    retire_rejected = getattr(product, "retire_rejected", None)
    if not callable(retire_rejected):
        return False
    result = retire_rejected(reason_code=reason_code)
    if not inspect.isawaitable(result):
        return result is True

    async def await_retirement() -> bool:
        try:
            retired = await asyncio.wait_for(result, timeout=cleanup_grace_seconds)
        except TimeoutError as exc:
            raise RuntimeOwnershipError("Rejected resource cleanup exceeded its grace period") from exc
        return retired is True

    return asyncio.run(await_retirement())


def _close_product_blocking(product: Any) -> None:
    close_blocking = getattr(product, "close_blocking", None)
    if callable(close_blocking):
        close_blocking()
        return
    close = getattr(product, "close", None)
    if not callable(close):
        return
    close_result = close()
    if inspect.isawaitable(close_result):

        async def await_close() -> None:
            await close_result

        asyncio.run(await_close())


async def _retire_products(
    products: Any,
    *,
    reason_code: str,
    cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
) -> None:
    """Retire every product even when one close path fails."""
    selected = tuple(products) if isinstance(products, (list, tuple)) else (products,)
    first_error: BaseException | None = None
    for product in selected:
        retired = False
        retire_rejected = getattr(product, "retire_rejected", None)
        if callable(retire_rejected):
            try:
                retirement = retire_rejected(reason_code=reason_code)
                if inspect.isawaitable(retirement):
                    try:
                        retired = (
                            await asyncio.wait_for(
                                retirement,
                                timeout=cleanup_grace_seconds,
                            )
                        ) is True
                    except TimeoutError as exc:
                        raise RuntimeOwnershipError("Rejected resource cleanup exceeded its grace period") from exc
                else:
                    retired = retirement is True
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
                # A rejected resource with an explicit retirement owner must
                # never fall through to a close on this (foreign) loop.
                continue
        if retired:
            continue
        try:
            close_blocking = getattr(product, "close_blocking", None)
            if callable(close_blocking):
                close_blocking()
                continue
            close = getattr(product, "close", None)
            if callable(close):
                result = close()
                if inspect.isawaitable(result):
                    await result
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


@dataclass(frozen=True, slots=True)
class _AdmittedProviderAccessor[Product]:
    """Mark a cache accessor whose resource owner already admitted realization."""

    getter: Callable[[], Product]

    def __call__(self) -> Product:
        return self.getter()


class ProviderLifecycleState(StrEnum):
    """Runtime provider generation state."""

    EMPTY = "empty"
    STARTING = "starting"
    ACTIVE = "active"
    DRAINING = "draining"
    REVOKED = "revoked"


@dataclass(frozen=True, slots=True)
class _RetainedProviderIdentity:
    """Compare executable declarations by identity while retaining their owner."""

    kind: str
    target: object = field(repr=False)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _RetainedProviderIdentity) and self.kind == other.kind and self.target is other.target

    def __hash__(self) -> int:
        return hash((self.kind, id(self.target)))


@dataclass(frozen=True, slots=True)
class ProviderSpec:
    """Stable semantic identity for one runtime provider manager."""

    runtime: RuntimeOwnershipDescriptor
    llm: RuntimeOwnershipDescriptor
    context: RuntimeOwnershipDescriptor
    context_disabled: bool
    cleanup_grace_seconds: float
    llm_factory_identity: _RetainedProviderIdentity | None = field(default=None, repr=False)
    context_factory_identity: _RetainedProviderIdentity | None = field(default=None, repr=False)
    chained_cleanup_identity: _RetainedProviderIdentity | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class ProviderLeaseHandle:
    """Generation-fenced authority held by one dependency-bundle run."""

    graph_nonce: str
    generation_epoch: int
    lease_id: int


@dataclass(frozen=True, slots=True)
class _ProviderLeaseOwner:
    """Requester identity retained only for abandoned-lease reclamation."""

    thread_id: int
    loop: asyncio.AbstractEventLoop = field(repr=False, compare=False)
    task: asyncio.Task[Any] | None = field(repr=False, compare=False)

    def matches_current(self) -> bool:
        try:
            loop = asyncio.get_running_loop()
            task = asyncio.current_task()
        except RuntimeError:
            return False
        return threading.get_ident() == self.thread_id and loop is self.loop and task is self.task

    def is_abandoned(self) -> bool:
        return self.loop.is_closed() or (self.task is not None and self.task.done())


@dataclass(frozen=True, slots=True)
class ProviderFailureQuarantine:
    """Bounded, immutable metadata for a retired failed generation."""

    generation_epoch: int
    reason_code: str
    error_type: str


@dataclass(slots=True)
class _ProviderAdoptionHandoff:
    """Choose exactly one cache-adoption or worker-retirement outcome."""

    phase: str = "pending"
    lock: Any = field(default_factory=threading.Lock, repr=False)

    def _acquire_on_owner(self) -> None:
        if not self.lock.acquire(blocking=False):
            raise RuntimeOwnershipError("Provider adoption handoff is contended")

    def begin_on_owner(self) -> bool:
        self._acquire_on_owner()
        try:
            if self.phase != "pending":
                return False
            self.phase = "adopting"
            return True
        finally:
            self.lock.release()

    def commit_on_owner(self) -> None:
        self._acquire_on_owner()
        try:
            if self.phase != "adopting":
                raise RuntimeOwnershipError("Provider adoption lost its ownership reservation")
            self.phase = "adopted"
        finally:
            self.lock.release()

    def reject_on_owner(self) -> None:
        if not self.lock.acquire(blocking=False):
            return
        try:
            if self.phase == "adopting":
                self.phase = "rejected"
        finally:
            self.lock.release()

    def is_adopted_on_owner(self) -> bool:
        self._acquire_on_owner()
        try:
            return self.phase == "adopted"
        finally:
            self.lock.release()

    def revoke_pending(self) -> bool:
        if not self.lock.acquire(blocking=False):
            return False
        try:
            if self.phase != "pending":
                return False
            self.phase = "revoked"
            return True
        finally:
            self.lock.release()

    @property
    def adopted(self) -> bool:
        if not self.lock.acquire(blocking=False):
            return False
        try:
            return self.phase == "adopted"
        finally:
            self.lock.release()


@dataclass(slots=True)
class _ProviderGenerationCloseState:
    """One admitted loop owns a provider generation through final cleanup."""

    ready: ThreadFuture[None] = field(default_factory=ThreadFuture)
    owner_exited: ThreadFuture[None] = field(default_factory=ThreadFuture)
    startup_ready: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Event]] = field(default_factory=list)
    retained_products: tuple[Any, ...] = ()
    terminal_error: BaseException | None = None
    loop: asyncio.AbstractEventLoop | None = None
    cleanup_scheduled: bool = False
    cleanup_submission_failed: bool = False
    cleanup_succeeded: bool = False
    owner_ready: bool = False
    startup_claimed: bool = False
    startup_aborted: bool = False
    owner_recovery_active: bool = False
    generation_epoch: int = 0
    active_operations: set[int] = field(default_factory=set)
    next_operation_id: int = 0
    active_handoffs: set[int] = field(default_factory=set)
    next_handoff_id: int = 0
    handoff_limit: int = 1
    committed_submissions: dict[int, _ProviderCommittedSubmission] = field(default_factory=dict)
    next_submission_id: int = 0
    cleanup_future: ThreadFuture[None] | None = None
    terminal_monitor_task: asyncio.Task[None] | None = field(default=None, repr=False)
    service_owner_permit: PipelineServiceOwnerPermit | None = None
    service_owner_released: bool = False
    owner_thread: threading.Thread | None = None


@dataclass(slots=True)
class _ProviderOperationAdmission:
    """Transfer one aggregate admission lease into an owner-loop handoff."""

    lifecycle: PipelineAdmissionController | None = field(default=None, repr=False)
    lease: PipelineAdmissionLease | None = field(default=None, repr=False)
    retained_work: PipelineRetainedWorkPermit | None = field(default=None, repr=False)
    submitted: bool = False
    operation_started: bool = False
    cancel_requested: bool = False
    released: bool = False
    lock: Any = field(default_factory=threading.Lock, repr=False)

    def mark_submitted(self) -> None:
        """Commit capacity to a handoff before the owner callback is published."""
        lifecycle: PipelineAdmissionController | None = None
        lease: PipelineAdmissionLease | None = None
        with self.lock:
            if self.submitted:
                return
            self.submitted = True
            lifecycle = self.lifecycle
            lease = self.lease
        if lifecycle is not None and lease is not None:
            controller_lock = cast(threading.Lock, getattr(lifecycle, "_lock"))
            with controller_lock:
                validate_lease = cast(
                    Callable[[PipelineAdmissionLease], str],
                    getattr(lifecycle, "_validate_active_lease_locked"),
                )
                validate_lease(lease)
                release_pending = cast(set[int], getattr(lifecycle, "_release_pending"))
                if lease.token in release_pending:
                    raise RuntimeOwnershipError("Provider handoff admission is no longer active")
                service_tokens = cast(set[int], getattr(lifecycle, "_service_admitted_tokens"))
                service_tokens.add(lease.token)
                wake_drain_waiters = cast(
                    Callable[[], None],
                    getattr(lifecycle, "_wake_runtime_drain_waiters_locked"),
                )
                wake_drain_waiters()

    def begin_operation(self) -> bool:
        """Start only when cancellation did not win before owner execution."""
        with self.lock:
            self.submitted = True
            if self.cancel_requested:
                return False
            self.operation_started = True
            return True

    def request_cancel(self) -> bool:
        """Prevent unstarted work while leaving committed capacity charged."""
        with self.lock:
            if self.operation_started:
                return False
            self.cancel_requested = True
            return True

    def finish(self) -> None:
        """Release a standalone lease exactly once at the real terminal edge."""
        release: tuple[PipelineAdmissionController, PipelineAdmissionLease] | None = None
        retained_release: tuple[PipelineAdmissionController, PipelineRetainedWorkPermit] | None = None
        with self.lock:
            if self.released:
                return
            self.released = True
            if self.lifecycle is not None and self.lease is not None:
                release = (self.lifecycle, self.lease)
            if self.lifecycle is not None and self.retained_work is not None:
                retained_release = (self.lifecycle, self.retained_work)
        if release is not None:
            lifecycle, lease = release
            lifecycle.release(lease)
        if retained_release is not None:
            lifecycle, permit = retained_release
            lifecycle.release_retained_work(permit)

    def finish_if_unsubmitted(self) -> None:
        """Roll back admission when no owner-loop handoff was committed."""
        with self.lock:
            submitted = self.submitted
        if not submitted:
            self.finish()


@dataclass(slots=True)
class _ProviderCommittedSubmission:
    """Generation-owned authority for one published owner-loop handoff."""

    submission_id: int
    operation_id: int | None
    handoff_id: int | None
    future: ThreadFuture[Any] = field(repr=False)
    admission: _ProviderOperationAdmission | None = field(default=None, repr=False)
    owner_task: asyncio.Task[Any] | None = field(default=None, repr=False)
    cancellation_requested: bool = False
    submission_started: bool = False
    settled: bool = False
    lock: Any = field(default_factory=threading.Lock, repr=False)


def _validate_provider_factory_event_loop(
    provider: LLMProvider | ContextProvider,
    expected_loop: asyncio.AbstractEventLoop | None,
) -> None:
    """Reject a factory product already owned by a different event loop."""
    validate_loop = getattr(provider, "validate_factory_event_loop", None)
    if callable(validate_loop):
        validate_loop(expected_loop)


def declare_backend_factory(
    factory: Callable[[], list[DashboardBackend]],
    *,
    runtime_settings: Settings,
    component: str = "dashboard_backend_factory",
) -> RuntimeOwnedFactory[list[DashboardBackend]]:
    """Declare the runtime owner of one lazy dashboard-backend factory.

    Callers that inject ``backend_factory`` into ``build_pipeline_dependencies``
    must use this helper. Declaration is side-effect free; each backend is
    independently revalidated when the factory is eventually invoked.
    """
    return declare_runtime_factory(
        factory,
        ownership=runtime_descriptor_for_backends(
            component=component,
            runtime_settings=runtime_settings,
        ),
        factory_kind="backend:dashboard",
    )


def _declared_store_factory(
    factory: Callable[[], Any],
    *,
    runtime_settings: Settings,
    expected: RuntimeOwnershipDescriptor,
    role: str,
    component: str,
) -> Callable[[], Any]:
    """Declare one trusted settings-owned store method without invoking it."""
    database = next((item for item in expected.databases if item.role == role), None)
    if database is None:
        raise RuntimeOwnershipError(f"runtime owner must expose the {role} database")
    return declare_runtime_factory(
        factory,
        ownership=runtime_descriptor_for_store(
            component=component,
            runtime_settings=runtime_settings,
            database_role=role,
            database_path=database.path,
        ),
        factory_kind=f"store:{role}",
    )


def _validated_store_factory(
    factory: Callable[[], Any],
    *,
    expected: RuntimeOwnershipDescriptor,
    role: str,
    factory_kind: str | None = None,
    allow_none: bool = False,
) -> Callable[[], Any]:
    """Validate a store declaration and every realized store before use."""
    factory_kind = factory_kind or f"store:{role}"
    declared = _factory_preflight(factory, expected=expected, factory_kind=factory_kind)

    def realize() -> Any:
        with observe_runtime_factory_realization(factory_kind):
            store = factory()
        if store is None and allow_none:
            return None
        try:
            require_runtime_store_ownership(
                boundary=f"Pipeline {role} store realization",
                expected=expected,
                store=store,
                database_role=role,
            )
        except RuntimeOwnershipMismatchError as exc:
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_mismatch",
                dimensions=exc.dimensions,
            )
            raise
        except RuntimeOwnershipError:
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_invalid",
            )
            raise
        return store

    return declare_runtime_factory(
        realize,
        ownership=declared,
        factory_kind=factory_kind,
    )


def _validate_llm_provider_product(
    provider: Any,
    *,
    expected: RuntimeOwnershipDescriptor,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
    capability: str = "llm",
    cleanup_bound_product: bool = False,
    inline_cleanup: bool = False,
    cleanup_rejected: bool = True,
    expected_event_loop: asyncio.AbstractEventLoop | None = None,
) -> LLMProvider:
    """Admit one realized provider, including a frozen Bedrock snapshot."""
    factory_kind = f"provider:{capability}"
    if not isinstance(provider, LLMProvider):
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_invalid",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_invalid",
        )
        raise RuntimeOwnershipError(f"Pipeline {capability} provider factory returned an invalid provider")
    cleanup_owned_product = True
    try:
        _validate_provider_factory_event_loop(provider, expected_event_loop)
        from tacit.agents.providers.bedrock import BedrockProvider

        if isinstance(provider, BedrockProvider):
            cleanup_owned_product = False
            cleanup_owned_product = provider.bind_pipeline_lifecycle(lifecycle) or cleanup_bound_product
        actual = get_runtime_ownership(provider, component=f"realized_{capability}_provider")
        ownership_declarations = getattr(provider, "bedrock_ownership_declarations", None)
        declarations = (
            ownership_declarations(component=expected.component) if callable(ownership_declarations) else None
        )
        if declarations is not None:
            planned_declaration, realized_declaration = declarations
            if not isinstance(planned_declaration, RuntimeOwnershipDescriptor) or not isinstance(
                realized_declaration,
                RuntimeOwnershipDescriptor,
            ):
                raise RuntimeOwnershipError("AWS Bedrock ownership declarations are invalid")
            require_compatible_runtime_ownership(
                boundary=f"Pipeline {capability} provider credential plan",
                descriptors=(expected, planned_declaration),
            )
            planned_remotes = {remote.provider: remote for remote in planned_declaration.remotes}
            realized_remotes = {remote.provider: remote for remote in realized_declaration.remotes}
            realization_dimensions: set[str] = set()
            if set(planned_remotes) != set(realized_remotes):
                realization_dimensions.add("remote")
            for remote_name, planned_remote in planned_remotes.items():
                realized_remote = realized_remotes.get(remote_name)
                if realized_remote is None:
                    continue
                if realized_remote.endpoint != planned_remote.endpoint:
                    realization_dimensions.add("endpoint")
                selector_refines = planned_remote.account == "default-chain" or planned_remote.account.startswith(
                    "profile:"
                )
                if not selector_refines and realized_remote.account != planned_remote.account:
                    realization_dimensions.add("account")
                if realized_remote.credential_fingerprint == "none":
                    realization_dimensions.add("credential")
            if realization_dimensions:
                raise RuntimeOwnershipMismatchError(
                    f"Pipeline {capability} provider credential realization",
                    realization_dimensions,
                    (planned_declaration.component, realized_declaration.component),
                )
            require_compatible_runtime_ownership(
                boundary=f"Pipeline {capability} provider credential realization",
                descriptors=(
                    planned_declaration,
                    replace(realized_declaration, remotes=planned_declaration.remotes),
                ),
            )
            expected = realized_declaration
        missing_dimensions: set[str] = set()
        if actual.settings_identity is None:
            missing_dimensions.add("settings")
        if actual.tenant_policy is None:
            missing_dimensions.update(("tenant", "permission"))
        expected_remotes = {remote.provider: remote for remote in expected.remotes}
        actual_remotes = {remote.provider: remote for remote in actual.remotes}
        if set(actual_remotes) != set(expected_remotes):
            missing_dimensions.add("remote")
        if missing_dimensions:
            raise RuntimeOwnershipMismatchError(
                f"Pipeline {capability} provider realization",
                missing_dimensions,
                (expected.component, actual.component),
            )
        require_compatible_runtime_ownership(
            boundary=f"Pipeline {capability} provider realization",
            descriptors=(expected, actual),
        )
    except RuntimeOwnershipMismatchError as exc:
        if cleanup_rejected and cleanup_owned_product:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_mismatch",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_mismatch",
            dimensions=exc.dimensions,
        )
        raise
    except RuntimeOwnershipError:
        if cleanup_rejected and cleanup_owned_product:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_owner_missing",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_owner_missing",
        )
        raise
    return provider


def _validated_llm_provider_factory(
    factory: Callable[[], LLMProvider],
    *,
    expected: RuntimeOwnershipDescriptor,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
    capability: str = "llm",
) -> Callable[[], LLMProvider]:
    """Validate a provider declaration and realized owner before agent use."""
    factory_kind = f"provider:{capability}"
    declared = _factory_preflight(factory, expected=expected, factory_kind=factory_kind)
    if isinstance(factory, RuntimeOwnedFactory) and isinstance(factory.factory, _AdmittedProviderAccessor):
        return factory
    blocking_work = _lifecycle_blocking_work(lifecycle)

    def realize() -> LLMProvider:
        try:
            expected_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            expected_event_loop = None

        def construct() -> LLMProvider:
            with observe_runtime_factory_realization(factory_kind):
                return factory()

        return blocking_work.realize_owned_sync(
            construct,
            validate=lambda provider: _validate_llm_provider_product(
                provider,
                expected=expected,
                lifecycle=lifecycle,
                cleanup_grace_seconds=cleanup_grace_seconds,
                capability=capability,
                cleanup_rejected=False,
                expected_event_loop=expected_event_loop,
            ),
            retire=lambda provider: _retire_products(
                provider,
                reason_code="runtime_factory_realization_rejected",
                cleanup_grace_seconds=cleanup_grace_seconds,
            ),
            reason_code=f"{factory_kind}_realization",
            result_handoff_seconds=cleanup_grace_seconds,
        )

    return declare_runtime_factory(realize, ownership=declared, factory_kind=factory_kind)


def _validated_context_provider_factory(
    factory: Callable[[], ContextProvider | None],
    *,
    expected: RuntimeOwnershipDescriptor,
    context_disabled: bool,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
) -> Callable[[], ContextProvider | None]:
    """Validate explicit context disablement or one settings-owned provider."""
    factory_kind = "provider:context"
    declared = _factory_preflight(factory, expected=expected, factory_kind=factory_kind)
    if isinstance(factory, RuntimeOwnedFactory) and isinstance(factory.factory, _AdmittedProviderAccessor):
        return factory
    blocking_work = _lifecycle_blocking_work(lifecycle)

    def realize() -> ContextProvider | None:
        try:
            expected_event_loop = asyncio.get_running_loop()
        except RuntimeError:
            expected_event_loop = None

        def construct() -> ContextProvider | None:
            with observe_runtime_factory_realization(factory_kind):
                return factory()

        return blocking_work.realize_owned_sync(
            construct,
            validate=lambda provider: _validate_context_provider_product(
                provider,
                expected=expected,
                context_disabled=context_disabled,
                lifecycle=lifecycle,
                cleanup_grace_seconds=cleanup_grace_seconds,
                cleanup_rejected=False,
                expected_event_loop=expected_event_loop,
            ),
            retire=lambda provider: _retire_products(
                provider,
                reason_code="runtime_factory_realization_rejected",
                cleanup_grace_seconds=cleanup_grace_seconds,
            ),
            reason_code=f"{factory_kind}_realization",
            result_handoff_seconds=cleanup_grace_seconds,
        )

    return declare_runtime_factory(realize, ownership=declared, factory_kind=factory_kind)


def _validate_context_provider_product(
    provider: Any,
    *,
    expected: RuntimeOwnershipDescriptor,
    context_disabled: bool,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
    inline_cleanup: bool = False,
    cleanup_rejected: bool = True,
    expected_event_loop: asyncio.AbstractEventLoop | None = None,
) -> ContextProvider | None:
    """Validate one context product while its realization owner still holds it."""
    factory_kind = "provider:context"
    if provider is None:
        if context_disabled:
            return None
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_unavailable",
        )
        raise RuntimeOwnershipError("Pipeline context provider is configured but unavailable")
    if context_disabled:
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_invalid",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_invalid",
        )
        raise RuntimeOwnershipError("Pipeline context provider must remain disabled for this runtime")
    if not isinstance(provider, ContextProvider):
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_invalid",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_invalid",
        )
        raise RuntimeOwnershipError("Pipeline context provider factory returned an invalid provider")
    try:
        _validate_provider_factory_event_loop(provider, expected_event_loop)
        actual = get_runtime_ownership(provider, component="realized_context_provider")
        missing_dimensions: set[str] = set()
        if actual.settings_identity is None:
            missing_dimensions.add("settings")
        if actual.tenant_policy is None:
            missing_dimensions.update(("tenant", "permission"))
        if actual.remotes != expected.remotes:
            missing_dimensions.add("remote")
        if missing_dimensions:
            raise RuntimeOwnershipMismatchError(
                "Pipeline context provider realization",
                missing_dimensions,
                (expected.component, actual.component),
            )
        require_compatible_runtime_ownership(
            boundary="Pipeline context provider realization",
            descriptors=(expected, actual),
        )
    except RuntimeOwnershipMismatchError as exc:
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_mismatch",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_mismatch",
            dimensions=exc.dimensions,
        )
        raise
    except RuntimeOwnershipError:
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_owner_missing",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_owner_missing",
        )
        raise
    return provider


class _BackendFactoryRealizationError(RuntimeOwnershipError):
    """Carry rejected realized backends to the runner for bounded cleanup."""

    def __init__(self, backends: list[DashboardBackend]) -> None:
        super().__init__("Pipeline backend realization failed ownership validation")
        self.backends = tuple(backends)


def _validated_backend_factory(
    factory: Callable[[], list[DashboardBackend]],
    *,
    expected: RuntimeOwnershipDescriptor,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
) -> Callable[[], list[DashboardBackend]]:
    """Validate a backend declaration and every realized backend before use."""
    factory_kind = "backend:dashboard"
    declared = _factory_preflight(factory, expected=expected, factory_kind=factory_kind)
    blocking_work = _lifecycle_blocking_work(lifecycle)

    def realize() -> list[DashboardBackend]:
        def construct() -> list[DashboardBackend]:
            with observe_runtime_factory_realization(factory_kind):
                return factory()

        return blocking_work.realize_owned_sync(
            construct,
            validate=lambda backends: _validate_backend_products(
                backends,
                expected=expected,
                lifecycle=lifecycle,
                cleanup_grace_seconds=cleanup_grace_seconds,
                cleanup_rejected=False,
            ),
            retire=lambda backends: _retire_products(
                backends,
                reason_code="runtime_factory_realization_rejected",
                cleanup_grace_seconds=cleanup_grace_seconds,
            ),
            reason_code=f"{factory_kind}_realization",
            result_handoff_seconds=cleanup_grace_seconds,
        )

    return declare_runtime_factory(realize, ownership=declared, factory_kind=factory_kind)


def _validated_backend_realizer(
    factory: Callable[[], list[DashboardBackend]],
    *,
    expected: RuntimeOwnershipDescriptor,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
) -> Callable[[], Awaitable[list[DashboardBackend]]]:
    """Build and validate backends asynchronously under runtime admission."""
    factory_kind = "backend:dashboard"
    _factory_preflight(factory, expected=expected, factory_kind=factory_kind)
    blocking_work = _lifecycle_blocking_work(lifecycle)

    async def realize() -> list[DashboardBackend]:
        def construct() -> list[DashboardBackend]:
            with observe_runtime_factory_realization(factory_kind):
                return factory()

        return await blocking_work.realize_owned(
            construct,
            validate=lambda backends: _validate_backend_products(
                backends,
                expected=expected,
                lifecycle=lifecycle,
                cleanup_grace_seconds=cleanup_grace_seconds,
                cleanup_rejected=False,
            ),
            retire=lambda backends: _retire_products(
                backends,
                reason_code="runtime_factory_realization_rejected",
                cleanup_grace_seconds=cleanup_grace_seconds,
            ),
            reason_code=f"{factory_kind}_realization",
            result_handoff_seconds=cleanup_grace_seconds,
        )

    return realize


def _validate_backend_products(
    backends: Any,
    *,
    expected: RuntimeOwnershipDescriptor,
    lifecycle: PipelineAdmissionController,
    cleanup_grace_seconds: float,
    inline_cleanup: bool = False,
    cleanup_rejected: bool = True,
) -> list[DashboardBackend]:
    """Validate one backend set while its realization owner still holds it."""
    factory_kind = "backend:dashboard"
    if not isinstance(backends, list):
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_invalid",
        )
        raise RuntimeOwnershipError("Pipeline backend factory returned an invalid backend collection")
    try:
        expected_remotes = {remote.provider: remote for remote in expected.remotes}
        realized_remotes: list[RuntimeRemoteIdentity] = []
        for backend in backends:
            actual = get_runtime_ownership(
                backend,
                component=f"realized_{getattr(backend, 'name', 'dashboard')}_backend",
            )
            missing_dimensions: set[str] = set()
            if actual.settings_identity is None:
                missing_dimensions.add("settings")
            if actual.tenant_policy is None:
                missing_dimensions.update(("tenant", "permission"))
            if len(actual.remotes) != 1 or any(
                expected_remotes.get(remote.provider) != remote for remote in actual.remotes
            ):
                missing_dimensions.add("remote")
            if missing_dimensions:
                raise RuntimeOwnershipMismatchError(
                    "Pipeline backend realization",
                    missing_dimensions,
                    (expected.component, actual.component),
                )
            require_compatible_runtime_ownership(
                boundary="Pipeline backend realization",
                descriptors=(expected, actual),
            )
            realized_remotes.extend(actual.remotes)
        if Counter(realized_remotes) != Counter(expected.remotes):
            raise RuntimeOwnershipMismatchError(
                "Pipeline backend realization",
                {"remote"},
                (expected.component, "realized_backend_set"),
            )
    except RuntimeOwnershipMismatchError as exc:
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                tuple(backends),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_mismatch",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_mismatch",
            dimensions=exc.dimensions,
        )
        raise _BackendFactoryRealizationError(backends) from exc
    except RuntimeOwnershipError as exc:
        if cleanup_rejected:
            _cleanup_rejected_products(
                lifecycle,
                tuple(backends),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_owner_missing",
                inline=inline_cleanup,
            )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_owner_missing",
        )
        raise _BackendFactoryRealizationError(backends) from exc
    return backends


def _semantic_provider_spec(
    runtime_settings: Settings,
    *,
    llm_factory: Callable[[], LLMProvider] | None,
    context_factory: Callable[[], ContextProvider | None] | None,
    chained_cleanup: Callable[[], Awaitable[None]] | None,
    cleanup_grace_seconds: float,
) -> ProviderSpec:
    """Build a semantic default or identity-pinned injected provider declaration."""
    llm_expected = runtime_descriptor_for_provider(
        component="provider_spec_llm",
        runtime_settings=runtime_settings,
        capability="llm",
    )
    if llm_factory is not None:
        if str(runtime_settings.llm_provider or "").strip().casefold() == "bedrock":
            llm_expected = replace(
                _expected_provider_declaration(runtime_settings, llm_factory),
                component="provider_spec_llm",
            )
        else:
            declared_llm = get_runtime_factory_ownership(
                llm_factory,
                expected_kind="provider:llm",
            )
            require_compatible_runtime_ownership(
                boundary="Runtime provider specification",
                descriptors=(llm_expected, declared_llm),
            )
            llm_expected = replace(declared_llm, component="provider_spec_llm")

    context_expected = runtime_descriptor_for_provider(
        component="provider_spec_context",
        runtime_settings=runtime_settings,
        capability="context",
    )
    if context_factory is not None:
        declared_context = get_runtime_factory_ownership(
            context_factory,
            expected_kind="provider:context",
        )
        require_compatible_runtime_ownership(
            boundary="Runtime context provider specification",
            descriptors=(context_expected, declared_context),
        )
        context_expected = replace(declared_context, component="provider_spec_context")

    return ProviderSpec(
        runtime=replace(
            runtime_descriptor_from_settings(
                runtime_settings,
                component="provider_spec_runtime",
            ),
            component="provider_spec_runtime",
        ),
        llm=replace(llm_expected, component="provider_spec_llm"),
        context=replace(context_expected, component="provider_spec_context"),
        context_disabled=str(runtime_settings.context_provider or "").strip().casefold() in {"", "none"},
        cleanup_grace_seconds=_validate_cleanup_grace_seconds(cleanup_grace_seconds),
        llm_factory_identity=(
            _RetainedProviderIdentity("llm_factory", llm_factory) if llm_factory is not None else None
        ),
        context_factory_identity=(
            _RetainedProviderIdentity("context_factory", context_factory) if context_factory is not None else None
        ),
        chained_cleanup_identity=(
            _RetainedProviderIdentity("chained_cleanup", chained_cleanup) if chained_cleanup is not None else None
        ),
    )


class _RuntimeProviderResources:
    """Reference-count providers shared by active runs in one runtime."""

    _QUARANTINE_LIMIT = 16

    @staticmethod
    def _bind_lifecycle_identity(
        runtime_settings: Settings,
        lifecycle: PipelineAdmissionController,
    ) -> None:
        lifecycle_owner = runtime_descriptor_from_settings(
            runtime_settings,
            component="pipeline_provider_lifecycle",
        )
        lifecycle_identity = lifecycle_owner.admission_namespace or lifecycle_owner.settings_identity
        if lifecycle_identity is None:
            raise RuntimeOwnershipError("Pipeline provider lifecycle has no admission identity")
        lifecycle.bind_runtime_settings_identity(lifecycle_identity)

    @staticmethod
    def _fatal_circuit_error(record: RuntimeFatalCircuit) -> RuntimeOwnershipError:
        failure = RuntimeOwnershipError("Pipeline provider generation cleanup failed")
        setattr(failure, "cleanup_reason_code", record.reason_code)
        setattr(failure, "cleanup_error_type", record.error_type)
        setattr(failure, "cleanup_retains_capacity", False)
        setattr(failure, "runtime_provider_fatal", True)
        return failure

    @classmethod
    def resolve(
        cls,
        runtime_settings: Settings,
        *,
        lifecycle: PipelineAdmissionController,
        llm_factory: Callable[[], LLMProvider] | None = None,
        context_factory: Callable[[], ContextProvider | None] | None = None,
        chained_cleanup: Callable[[], Awaitable[None]] | None = None,
        cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
        spec: ProviderSpec | None = None,
    ) -> _RuntimeProviderResources:
        """Resolve the admission namespace's sole compatible provider manager."""
        cls._bind_lifecycle_identity(runtime_settings, lifecycle)
        lifecycle.raise_if_runtime_fatal()
        selected_spec = spec or _semantic_provider_spec(
            runtime_settings,
            llm_factory=llm_factory,
            context_factory=context_factory,
            chained_cleanup=chained_cleanup,
            cleanup_grace_seconds=cleanup_grace_seconds,
        )
        graph = lifecycle.execution_graph
        manager = graph.resolve_provider_manager(
            spec=selected_spec,
            create=lambda: cls(
                runtime_settings,
                lifecycle=lifecycle,
                llm_factory=llm_factory,
                context_factory=context_factory,
                chained_cleanup=chained_cleanup,
                cleanup_grace_seconds=cleanup_grace_seconds,
                spec=selected_spec,
            ),
        )
        if not isinstance(manager, cls):
            raise RuntimeOwnershipError("Runtime provider manager has an incompatible implementation")
        return manager

    def __init__(
        self,
        runtime_settings: Settings,
        *,
        lifecycle: PipelineAdmissionController,
        llm_factory: Callable[[], LLMProvider] | None = None,
        context_factory: Callable[[], ContextProvider | None] | None = None,
        chained_cleanup: Callable[[], Awaitable[None]] | None = None,
        cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
        spec: ProviderSpec | None = None,
    ) -> None:
        self._settings = runtime_settings
        self._lifecycle = lifecycle
        self._bind_lifecycle_identity(runtime_settings, lifecycle)
        lifecycle.raise_if_runtime_fatal()
        self._spec = spec or _semantic_provider_spec(
            runtime_settings,
            llm_factory=llm_factory,
            context_factory=context_factory,
            chained_cleanup=chained_cleanup,
            cleanup_grace_seconds=cleanup_grace_seconds,
        )
        self._graph_nonce = lifecycle.execution_graph.graph_nonce
        self._cleanup_grace_seconds = _validate_cleanup_grace_seconds(cleanup_grace_seconds)
        self._requires_async_llm_realization = str(runtime_settings.llm_provider).strip().casefold() == "bedrock"
        self._provider_factory_work = _lifecycle_blocking_work(lifecycle)
        self._bedrock_credential_plan = (
            BedrockCredentialPlan.capture(runtime_settings).as_cross_generation_declaration()
            if self._requires_async_llm_realization and llm_factory is None
            else None
        )
        if self._bedrock_credential_plan is not None:
            expected = self._bedrock_credential_plan.ownership(component="pipeline_provider_settings")
        elif self._requires_async_llm_realization and llm_factory is not None:
            expected = _expected_provider_declaration(runtime_settings, llm_factory)
        else:
            expected = runtime_descriptor_for_provider(
                component="pipeline_provider_settings",
                runtime_settings=runtime_settings,
                capability="llm",
            )
        expected_context = runtime_descriptor_for_provider(
            component="pipeline_context_provider_settings",
            runtime_settings=runtime_settings,
            capability="context",
        )

        def default_llm_factory() -> LLMProvider:
            if self._bedrock_credential_plan is not None:
                from tacit.agents.providers.bedrock import BedrockProvider

                return BedrockProvider(credential_plan=self._bedrock_credential_plan)
            from tacit.agents.providers.registry import create_provider

            return create_provider(self._settings)

        def default_context_factory() -> ContextProvider | None:
            from tacit.context.registry import create_context_provider

            return create_context_provider(self._settings)

        context_disabled = str(runtime_settings.context_provider or "").strip().casefold() in {"", "none"}
        default_llm = declare_runtime_factory(
            default_llm_factory,
            ownership=expected,
            factory_kind="provider:llm",
        )
        default_context = declare_runtime_factory(
            default_context_factory if not context_disabled else (lambda: None),
            ownership=expected_context,
            factory_kind="provider:context",
        )
        selected_context_factory = context_factory
        if selected_context_factory is None:
            selected_context_factory = default_context

        self._llm_factory = llm_factory or default_llm
        _factory_preflight(
            self._llm_factory,
            expected=expected,
            factory_kind="provider:llm",
        )
        self._llm_expected = expected
        _factory_preflight(
            selected_context_factory,
            expected=expected_context,
            factory_kind="provider:context",
        )
        self._context_factory = selected_context_factory
        self._context_expected = expected_context
        self._context_disabled = context_disabled
        self._chained_cleanup = chained_cleanup
        self._llm_provider: LLMProvider | None = None
        self._context_provider: ContextProvider | None = None
        self._context_initialized = False
        self._lock = threading.RLock()
        self._sync_realization_lock = threading.Lock()
        self._state = ProviderLifecycleState.EMPTY
        self._generation_epoch = 0
        self._active_leases: dict[int, ProviderLeaseHandle] = {}
        self._lease_owners: dict[int, _ProviderLeaseOwner] = {}
        self._next_lease = 1
        self._quarantine: deque[ProviderFailureQuarantine] = deque(maxlen=self._QUARANTINE_LIMIT)
        self._shutdown_requested = False
        self._shutdown_root_generation = -1
        self._cleanup_pending = chained_cleanup is not None
        self._generation_owner: _ProviderGenerationCloseState | None = None
        self._retired_generation: _ProviderGenerationCloseState | None = None
        self._cleanup_in_flight = 0
        self._closing_event: _ProviderGenerationCloseState | None = None
        self._llm_initializing: ThreadFuture[None] | None = None
        self._context_initializing: ThreadFuture[None] | None = None
        self._llm_adoption: _ProviderAdoptionHandoff | None = None
        self._context_adoption: _ProviderAdoptionHandoff | None = None
        self._task_leases: contextvars.ContextVar[tuple[tuple[ProviderLeaseHandle, _ProviderLeaseOwner], ...]] = (
            contextvars.ContextVar(
                f"tacit_provider_leases_{id(self)}",
                default=(),
            )
        )

    def _resolve_active_manager(self) -> _RuntimeProviderResources:
        """Attach retained dependency bundles to the current root generation.

        Dependency bundles can outlive one application lifespan. The execution
        graph clears its provider manager after the final root drains, so a
        later direct run must either reattach this fully retired manager or
        delegate to the compatible manager already selected by a sibling root.
        """
        self._raise_if_runtime_fatal()
        graph = self._lifecycle.execution_graph
        manager = graph.resolve_provider_manager(
            spec=self._spec,
            create=lambda: self,
        )
        if not isinstance(manager, _RuntimeProviderResources):
            raise RuntimeOwnershipError("Runtime provider manager has an incompatible implementation")
        if manager is not self:
            return manager

        with self._lock:
            if not self._shutdown_requested:
                return self
            root_generation = graph.root_generation
            if graph.root_state != "active" or root_generation <= self._shutdown_root_generation:
                raise RuntimeOwnershipError("Pipeline provider runtime is shutting down")
            if (
                self._state is not ProviderLifecycleState.EMPTY
                or self._generation_owner is not None
                or self._retired_generation is not None
                or self._active_leases
                or self._cleanup_in_flight
            ):
                raise RuntimeOwnershipError("Pipeline provider runtime did not finish its prior root generation")
            self._shutdown_requested = False
            self._cleanup_pending = self._chained_cleanup is not None
        return self

    @property
    def generation_epoch(self) -> int:
        with self._lock:
            return self._generation_epoch

    @property
    def lifecycle_state(self) -> ProviderLifecycleState:
        with self._lock:
            return self._state

    @property
    def quarantined_generation_count(self) -> int:
        with self._lock:
            return len(self._quarantine)

    def _raise_if_runtime_fatal(self) -> None:
        try:
            self._lifecycle.raise_if_runtime_fatal()
        except RuntimeOwnershipError as error:
            record = self._lifecycle.runtime_fatal_circuit
            if record is None:
                raise
            raise self._fatal_circuit_error(record) from error

    @staticmethod
    def _lease_owner() -> _ProviderLeaseOwner:
        return _ProviderLeaseOwner(
            thread_id=threading.get_ident(),
            loop=asyncio.get_running_loop(),
            task=asyncio.current_task(),
        )

    def _prepare_generation_owner_locked(self) -> _ProviderGenerationCloseState:
        """Install one starting generation without waiting for owner readiness."""
        self._raise_if_runtime_fatal()
        if self._shutdown_requested:
            raise RuntimeOwnershipError("Pipeline provider runtime is shutting down")
        if self._state is not ProviderLifecycleState.EMPTY:
            raise RuntimeOwnershipError("Pipeline provider generation is not empty")
        self._join_retired_generation_sync_locked()
        service_permit = self._lifecycle.try_acquire_service_owner()
        if service_permit is None:
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner is already active")
        self._generation_epoch += 1
        state = _ProviderGenerationCloseState(
            generation_epoch=self._generation_epoch,
            service_owner_permit=service_permit,
            handoff_limit=max(1, self._lifecycle.limit + self._lifecycle.max_queued),
        )
        self._state = ProviderLifecycleState.STARTING
        self._generation_owner = state
        return state

    @staticmethod
    def _claim_generation_owner_start_locked(state: _ProviderGenerationCloseState) -> bool:
        if state.startup_claimed:
            return False
        state.startup_claimed = True
        return True

    def _start_prepared_generation_owner(self, state: _ProviderGenerationCloseState) -> None:
        """Start and prove readiness for one already-installed generation."""
        service_permit = state.service_owner_permit
        if service_permit is None:
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner has no service permit")
        owner_abort = threading.Event()
        owner_finished = threading.Event()

        def own_generation() -> None:
            try:
                if owner_abort.is_set():
                    terminal_error: BaseException | None = state.terminal_error or RuntimeOwnershipError(
                        "Pipeline provider lifecycle owner startup was aborted"
                    )
                else:
                    try:
                        with self._lifecycle.service_owner(service_permit):
                            terminal_error = self._run_generation_owner(state)
                    except BaseException as exc:
                        self._publish_generation_startup_failure(state, exc)
                        terminal_error = exc
                try:
                    if terminal_error is None and state.cleanup_succeeded:
                        self._finish_generation_cleanup(state)
                    else:
                        self._finish_generation_terminal_failure(
                            state,
                            terminal_error
                            or RuntimeOwnershipError("Pipeline provider lifecycle owner stopped unexpectedly"),
                        )
                finally:
                    self._release_generation_service_owner(state)
            finally:
                with self._lock:
                    waiters = tuple(state.waiters)
                    state.waiters.clear()
                self._notify_generation_waiters(waiters)
                owner_finished.set()

        def abort_owner_start() -> None:
            owner_abort.set()
            with self._lock:
                state.startup_aborted = True
                owner_loop = state.loop
            if owner_loop is not None and not owner_loop.is_closed():
                try:
                    owner_loop.call_soon_threadsafe(owner_loop.stop)
                except RuntimeError:
                    pass

        def readiness_error() -> BaseException | None:
            if not state.ready.done():
                return None
            try:
                state.ready.result()
            except BaseException as exc:
                return exc
            return None

        try:
            _start_lifecycle_owner_thread(
                target=own_generation,
                name="tacit-lifecycle-provider-owner",
                install=lambda thread: self._install_generation_owner_thread(state, thread),
                abort=abort_owner_start,
                ready=state.startup_ready,
                readiness_error=readiness_error,
                finished=owner_finished,
                daemon=True,
            )
        except _LifecycleOwnerStartupError as exc:
            if exc.thread_alive:
                fatal_error = RuntimeOwnershipError("Pipeline provider lifecycle owner startup was ambiguous")
                setattr(fatal_error, "cleanup_reason_code", "runtime_cleanup_failed")
                setattr(fatal_error, "cleanup_error_type", type(exc.cause).__name__)
                setattr(fatal_error, "runtime_provider_fatal", True)
                self._fence_generation(state, fatal_error)
                self._lifecycle.fence_runtime_fatal(fatal_error)
                startup_error = RuntimeError("Pipeline provider lifecycle worker startup was ambiguous")
                self._publish_generation_startup_failure(state, startup_error)
                raise startup_error from exc.cause
            with self._lock:
                settled = state.done.is_set()
                if not settled and self._generation_owner is state:
                    self._generation_owner = None
                    self._closing_event = None
                    self._state = ProviderLifecycleState.EMPTY
            if not settled:
                self._release_generation_service_owner(state)
            message = (
                "Pipeline provider lifecycle owner construction failed"
                if exc.phase == "construction"
                else "Pipeline provider lifecycle worker could not start"
            )
            startup_error = RuntimeError(message)
            self._publish_generation_startup_failure(state, startup_error)
            raise startup_error from exc.cause

    def _start_generation_owner_locked(self) -> _ProviderGenerationCloseState:
        """Synchronously start one generation for compatibility with sync accessors."""
        state = self._prepare_generation_owner_locked()
        if not self._claim_generation_owner_start_locked(state):
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner startup was already claimed")
        # This compatibility path is called with the manager lock held. The
        # installed STARTING state is the concurrency fence while readiness is
        # proved, so the long transition must not retain the manager lock.
        self._lock.release()
        try:
            self._start_prepared_generation_owner(state)
        finally:
            self._lock.acquire()
        return state

    def _rollback_generation_start_transport(
        self,
        state: _ProviderGenerationCloseState,
        cause: BaseException,
    ) -> RuntimeError:
        """Roll back a generation whose transition thread definitely did not start."""
        startup_error = RuntimeError("Pipeline provider lifecycle transition could not start")
        with self._lock:
            state.startup_aborted = True
            settled = state.done.is_set()
            if not settled and self._generation_owner is state:
                self._generation_owner = None
                self._closing_event = None
                self._state = ProviderLifecycleState.EMPTY
        self._publish_generation_startup_failure(state, startup_error)
        if not settled:
            self._release_generation_service_owner(state)
        startup_error.__cause__ = cause
        return startup_error

    async def _start_prepared_generation_owner_async(
        self,
        state: _ProviderGenerationCloseState,
    ) -> None:
        """Transport the blocking readiness proof without blocking its caller loop."""
        transition_result: ThreadFuture[None] = ThreadFuture()

        def run_transition() -> None:
            try:
                self._start_prepared_generation_owner(state)
            except BaseException as exc:
                transition_result.set_exception(exc)
            else:
                transition_result.set_result(None)

        transition_thread: threading.Thread | None = None
        try:
            transition_thread = threading.Thread(
                target=run_transition,
                name=f"tacit-provider-generation-startup-transition-{state.generation_epoch}",
                daemon=True,
            )
            transition_thread.start()
        except BaseException as exc:
            if (
                transition_thread is None
                or transition_thread.ident is None
                or (not transition_thread.is_alive() and not transition_result.done())
            ):
                raise self._rollback_generation_start_transport(state, exc) from exc

        transition = asyncio.wrap_future(transition_result)
        cancelled = False
        while True:
            try:
                await asyncio.shield(transition)
                break
            except asyncio.CancelledError:
                cancelled = True
                if transition.done():
                    try:
                        transition.result()
                    except BaseException as exc:
                        raise asyncio.CancelledError from exc
                    break
            except BaseException as exc:
                if cancelled:
                    raise asyncio.CancelledError from exc
                raise
        assert transition_thread is not None
        transition_thread.join(timeout=0)
        if cancelled:
            raise asyncio.CancelledError

    def _publish_generation_startup_failure(
        self,
        state: _ProviderGenerationCloseState,
        error: BaseException,
    ) -> None:
        if not state.ready.done():
            state.ready.set_exception(error)
        state.startup_ready.set()

    def _release_generation_service_owner(self, state: _ProviderGenerationCloseState) -> None:
        permit: PipelineServiceOwnerPermit | None = None
        with self._lock:
            if not state.service_owner_released:
                state.service_owner_released = True
                permit = state.service_owner_permit
                state.service_owner_permit = None
        if permit is not None:
            self._lifecycle.release_service_owner(permit)

    def _install_generation_owner_thread(
        self,
        state: _ProviderGenerationCloseState,
        thread: threading.Thread,
    ) -> None:
        with self._lock:
            if self._generation_owner is not state:
                raise RuntimeOwnershipError("Pipeline provider lifecycle owner was superseded during startup")
            run_owner = thread.run

            def run_owner_to_exit() -> None:
                try:
                    run_owner()
                finally:
                    if not state.owner_exited.done():
                        state.owner_exited.set_result(None)

            cast(Any, thread).run = run_owner_to_exit
            state.owner_thread = thread

    async def _ensure_generation_owner(
        self,
        expected_state: _ProviderGenerationCloseState | None = None,
    ) -> _ProviderGenerationCloseState:
        with self._lock:
            state = self._generation_owner
            if expected_state is not None and state is not expected_state:
                raise RuntimeOwnershipError("Pipeline provider generation changed during startup")
            if state is None:
                state = self._prepare_generation_owner_locked()
            start_owner = self._claim_generation_owner_start_locked(state)
        if start_owner:
            await self._start_prepared_generation_owner_async(state)
        await asyncio.wait_for(
            asyncio.shield(asyncio.wrap_future(state.ready)),
            timeout=_LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS,
        )
        return state

    def _ensure_generation_owner_sync(self) -> _ProviderGenerationCloseState:
        with self._lock:
            state = self._generation_owner
            if state is None:
                state = self._prepare_generation_owner_locked()
            start_owner = self._claim_generation_owner_start_locked(state)
        if start_owner:
            self._start_prepared_generation_owner(state)
        state.ready.result(timeout=_LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS)
        return state

    async def acquire(self) -> ProviderLeaseHandle:
        """Lease the runtime provider generation and return its fenced handle."""
        self._raise_if_runtime_fatal()
        manager = self._resolve_active_manager()
        if manager is not self:
            return await manager.acquire()
        lease_owner = self._lease_owner()
        while True:
            retired_state: _ProviderGenerationCloseState | None = None
            with self._lock:
                if self._shutdown_requested:
                    raise RuntimeOwnershipError("Pipeline provider runtime is shutting down")
                retired_state = self._retired_generation
                closing_state = self._closing_event
                if (
                    retired_state is None
                    and closing_state is None
                    and self._state
                    in {
                        ProviderLifecycleState.EMPTY,
                        ProviderLifecycleState.STARTING,
                        ProviderLifecycleState.ACTIVE,
                    }
                ):
                    state = self._generation_owner
                    if state is None:
                        state = self._prepare_generation_owner_locked()
                    lease_id = self._next_lease
                    self._next_lease += 1
                    handle = ProviderLeaseHandle(
                        graph_nonce=self._graph_nonce,
                        generation_epoch=state.generation_epoch,
                        lease_id=lease_id,
                    )
                    self._active_leases[lease_id] = handle
                    self._lease_owners[lease_id] = lease_owner
                    self._cleanup_pending = self._chained_cleanup is not None
                    break
                if retired_state is None and closing_state is None:
                    closing_state = self._generation_owner
                    if closing_state is None:
                        continue
            if retired_state is not None:
                await self._join_retired_generation(retired_state)
                continue
            if closing_state is None:
                continue
            if not await self._wait_for_generation_close(closing_state):
                raise RuntimeOwnershipError("Pipeline provider resources are still closing after cleanup grace")
        try:
            state = await self._ensure_generation_owner(expected_state=state)
            await self._ensure_llm_provider()
            await self._ensure_context_provider()
            with self._lock:
                if self._generation_owner is not state or state.terminal_error is not None:
                    raise RuntimeOwnershipError("Pipeline provider generation failed during startup")
                if self._shutdown_requested or self._active_leases.get(lease_id) != handle:
                    raise RuntimeOwnershipError("Pipeline provider runtime shut down during startup")
                if self._state is ProviderLifecycleState.STARTING:
                    self._state = ProviderLifecycleState.ACTIVE
        except BaseException as startup_error:
            with self._lock:
                self._active_leases.pop(lease_id, None)
                self._lease_owners.pop(lease_id, None)
                revoked_by = (
                    None
                    if self._shutdown_requested or isinstance(startup_error, asyncio.CancelledError)
                    else startup_error
                )
                if self._generation_owner is state and not self._active_leases:
                    # The failed lease and terminal reservation are one state
                    # transition. No replacement may lease this generation in
                    # the gap before the asynchronous cleanup wait begins.
                    self._begin_generation_retirement(state, revoked_by=revoked_by)
            try:
                await self._retire_unleased_generation(state, revoked_by=revoked_by)
            except (PipelineAdmissionRejected, RuntimeOwnershipError) as cleanup_error:
                logger.warning(
                    "provider_cleanup_admission_unavailable",
                    reason_code="provider_cleanup_admission_unavailable",
                    resource="provider_generation",
                    error_type=type(cleanup_error).__name__,
                )
            raise
        self._task_leases.set((*self._task_leases.get(), (handle, lease_owner)))
        return handle

    async def _ensure_llm_provider(self) -> None:
        while True:
            initializer = False
            with self._lock:
                if self._llm_provider is not None:
                    return
                initializing = self._llm_initializing
                if initializing is None:
                    initializing = ThreadFuture()
                    self._llm_initializing = initializing
                    initializer = True
            if not initializer:
                await asyncio.shield(asyncio.wrap_future(initializing))
                continue

            try:
                state = await self._ensure_generation_owner()
            except BaseException:
                self._finish_llm_initialization(initializing)
                raise
            realization_task: asyncio.Task[LLMProvider] | None = None
            try:
                realization_task = asyncio.create_task(
                    self._realize_llm_provider(),
                    name="tacit-llm-provider-realization",
                )
                try:
                    await asyncio.shield(realization_task)
                except asyncio.CancelledError:
                    await self._settle_cancelled_realization(realization_task)
                    raise
                else:
                    with self._lock:
                        if (
                            self._shutdown_requested
                            or self._generation_owner is not state
                            or self._llm_provider is None
                        ):
                            raise RuntimeOwnershipError(
                                "Pipeline LLM provider generation was retired during realization"
                            )
                    return
            except asyncio.CancelledError:
                raise
            except BaseException:
                if realization_task is None or (self._llm_initializing is initializing and realization_task.done()):
                    self._finish_llm_initialization(initializing)
                raise

    def _commit_generation_submission(
        self,
        state: _ProviderGenerationCloseState,
        *,
        operation_id: int | None,
        handoff_id: int | None,
        future: ThreadFuture[Any],
        admission: _ProviderOperationAdmission | None,
    ) -> _ProviderCommittedSubmission:
        """Transfer one handoff from its requester to the generation owner."""
        with self._lock:
            if self._generation_owner is not state or state.terminal_error is not None:
                raise RuntimeOwnershipError("Pipeline provider generation is no longer active")
            if admission is not None:
                admission.mark_submitted()
            state.next_submission_id += 1
            submission = _ProviderCommittedSubmission(
                submission_id=state.next_submission_id,
                operation_id=operation_id,
                handoff_id=handoff_id,
                future=future,
                admission=admission,
            )
            state.committed_submissions[submission.submission_id] = submission
            return submission

    def _settle_generation_submission(
        self,
        state: _ProviderGenerationCloseState,
        submission: _ProviderCommittedSubmission,
        *,
        task: asyncio.Task[Any] | None = None,
        error: BaseException | None = None,
        schedule_cleanup: bool = True,
    ) -> bool:
        """Settle one committed handoff exactly once from any terminal path."""
        with submission.lock:
            if submission.settled:
                return False
            submission.settled = True

        with self._lock:
            current = state.committed_submissions.get(submission.submission_id)
            if current is submission:
                state.committed_submissions.pop(submission.submission_id, None)

        if submission.handoff_id is not None:
            self._finish_generation_handoff(state, submission.handoff_id)
        if submission.operation_id is not None:
            self._finish_generation_operation(
                state,
                submission.operation_id,
                schedule_cleanup=schedule_cleanup,
            )
        if submission.admission is not None:
            submission.admission.finish()
        try:
            if not submission.future.done():
                if task is not None:
                    if task.cancelled():
                        submission.future.cancel()
                    else:
                        task_error = task.exception()
                        if task_error is not None:
                            submission.future.set_exception(task_error)
                        else:
                            submission.future.set_result(task.result())
                elif error is not None:
                    submission.future.set_exception(error)
                else:
                    submission.future.set_exception(RuntimeOwnershipError("Provider operation ended without a result"))
        except BaseException as transport_error:
            # Capacity and generation ownership are already settled above. A
            # dead requester loop may reject result publication, but it cannot
            # interrupt owner-side retirement.
            logger.warning(
                "provider_generation_result_transport_failed",
                reason_code="provider_generation_result_transport_failed",
                error_type=type(transport_error).__name__,
                generation_epoch=state.generation_epoch,
            )
        return True

    def _settle_committed_generation_submissions(
        self,
        state: _ProviderGenerationCloseState,
        error: BaseException,
    ) -> None:
        """Return every unpublished or abandoned handoff to terminal zero."""
        with self._lock:
            submissions = tuple(state.committed_submissions.values())
        for submission in submissions:
            self._settle_generation_submission(
                state,
                submission,
                error=error,
                schedule_cleanup=False,
            )

    def _settle_owner_terminal_submissions(
        self,
        state: _ProviderGenerationCloseState,
        error: BaseException,
    ) -> int:
        """Settle only work the owner proves never started on its event loop."""
        with self._lock:
            submissions = tuple(state.committed_submissions.values())
        settled = 0
        for submission in submissions:
            with submission.lock:
                owner_task = submission.owner_task
                submission_started = submission.submission_started
                cancellation_requested = submission.cancellation_requested
            if owner_task is not None:
                if cancellation_requested and not owner_task.done():
                    owner_task.cancel()
                continue
            if submission_started:
                continue
            settled += int(
                self._settle_generation_submission(
                    state,
                    submission,
                    error=error,
                    schedule_cleanup=False,
                )
            )
        return settled

    async def _invoke_on_generation_owner(
        self,
        state: _ProviderGenerationCloseState,
        operation: Callable[[], Awaitable[Any]],
        *,
        settle_on_cancel: bool = False,
        track_operation: bool = True,
        limit_handoff: bool = False,
        allow_starting: bool = False,
        admission: _ProviderOperationAdmission | None = None,
    ) -> Any:
        owner_loop = state.loop
        if owner_loop is None:
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner is not ready")
        handoff_id = self._begin_generation_handoff(state) if limit_handoff else None
        try:
            operation_id = (
                self._begin_generation_operation(
                    state,
                    allow_starting=allow_starting,
                )
                if track_operation
                else None
            )
        except BaseException:
            if handoff_id is not None:
                self._finish_generation_handoff(state, handoff_id)
            raise
        if owner_loop.is_closed() or not owner_loop.is_running():
            if handoff_id is not None:
                self._finish_generation_handoff(state, handoff_id)
            if operation_id is not None:
                self._finish_generation_operation(state, operation_id)
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner is unavailable")

        async def owned_operation() -> Any:
            if admission is not None and not admission.begin_operation():
                raise asyncio.CancelledError
            return await operation()

        if asyncio.get_running_loop() is owner_loop:
            try:
                if admission is not None:
                    admission.mark_submitted()
                return await owned_operation()
            finally:
                if handoff_id is not None:
                    self._finish_generation_handoff(state, handoff_id)
                if operation_id is not None:
                    self._finish_generation_operation(state, operation_id)
                if admission is not None:
                    admission.finish()

        try:
            future: ThreadFuture[Any] = ThreadFuture()
            submission = self._commit_generation_submission(
                state,
                operation_id=operation_id,
                handoff_id=handoff_id,
                future=future,
                admission=admission,
            )
        except BaseException:
            if handoff_id is not None:
                self._finish_generation_handoff(state, handoff_id)
            if operation_id is not None:
                self._finish_generation_operation(state, operation_id)
            if admission is not None:
                admission.finish()
            raise

        def finish_submission(error: BaseException | None = None) -> None:
            self._settle_generation_submission(
                state,
                submission,
                error=error,
            )

        def operation_completed(task: asyncio.Task[Any]) -> None:
            settled = self._settle_generation_submission(
                state,
                submission,
                task=task,
            )
            if not settled and not task.cancelled():
                task.exception()

        def submit_operation() -> None:
            with submission.lock:
                if submission.settled:
                    return
                submission.submission_started = True
            coroutine = owned_operation()
            try:
                task = owner_loop.create_task(coroutine)
            except BaseException as exc:
                coroutine.close()
                finish_submission(exc)
                return
            task.add_done_callback(operation_completed)
            with submission.lock:
                submission.owner_task = task
                cancel_now = submission.cancellation_requested
            if cancel_now:
                task.cancel()

        def cancel_operation() -> None:
            with submission.lock:
                submission.cancellation_requested = True
                task = submission.owner_task
            if task is not None:
                try:
                    owner_loop.call_soon_threadsafe(task.cancel)
                except RuntimeError:
                    pass

        def settle_ambiguous_submission(error: BaseException) -> None:
            with submission.lock:
                started = submission.submission_started or submission.owner_task is not None
            if not started:
                finish_submission(
                    RuntimeOwnershipError("Provider operation could not be submitted to its lifecycle owner")
                )

        try:
            owner_loop.call_soon_threadsafe(submit_operation)
        except BaseException as submission_error:
            # call_soon_threadsafe() may raise after enqueueing the callback.
            # A second FIFO callback lets the owner decide whether submission
            # committed; only that owner-side decision may release capacity.
            try:
                owner_loop.call_soon_threadsafe(
                    settle_ambiguous_submission,
                    submission_error,
                )
            except BaseException as probe_error:
                if owner_loop.is_closed() or state.owner_thread is None or not state.owner_thread.is_alive():
                    finish_submission(
                        RuntimeOwnershipError("Provider lifecycle owner became unavailable during submission")
                    )
                else:
                    # The first dispatch failure is the primary terminal cause.
                    # The owner-local monitor decides whether its callback was
                    # enqueued and settles the committed handoff without caller
                    # transport.
                    self._fence_generation(state, submission_error)
                    logger.warning(
                        "provider_owner_dispatch_recovery_requested",
                        reason_code="provider_owner_dispatch_recovery_requested",
                        error_type=type(submission_error).__name__,
                        probe_error_type=type(probe_error).__name__,
                        generation_epoch=state.generation_epoch,
                    )
        wrapped = asyncio.wrap_future(future)
        try:
            return await asyncio.shield(wrapped)
        except asyncio.CancelledError:
            if settle_on_cancel:
                cancel_operation()
                while not wrapped.done():
                    try:
                        await asyncio.shield(wrapped)
                    except asyncio.CancelledError:
                        continue
                    except BaseException:
                        break
                if wrapped.done():
                    try:
                        wrapped.result()
                    except BaseException:
                        pass
                raise
            if admission is not None and admission.request_cancel():
                cancel_operation()
            future.add_done_callback(self._consume_provider_operation)
            wrapped.add_done_callback(self._consume_async_provider_operation)
            raise

    @staticmethod
    def _consume_provider_operation(future: ThreadFuture[Any]) -> None:
        try:
            future.result()
        except BaseException:
            return

    @staticmethod
    def _consume_async_provider_operation(future: asyncio.Future[Any]) -> None:
        try:
            future.result()
        except BaseException:
            return

    def _begin_generation_operation(
        self,
        state: _ProviderGenerationCloseState,
        *,
        allow_starting: bool = False,
    ) -> int:
        with self._lock:
            if self._generation_owner is not state:
                raise RuntimeOwnershipError("Pipeline provider generation is no longer active")
            if self._closing_event is state or state.terminal_error is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            allowed_states = {ProviderLifecycleState.ACTIVE}
            if allow_starting:
                allowed_states.add(ProviderLifecycleState.STARTING)
            if self._state not in allowed_states:
                raise RuntimeOwnershipError("Pipeline provider generation is no longer active")
            state.next_operation_id += 1
            operation_id = state.next_operation_id
            state.active_operations.add(operation_id)
            return operation_id

    def _begin_generation_handoff(self, state: _ProviderGenerationCloseState) -> int:
        """Reserve bounded generation capacity before allocating handoff state."""
        with self._lock:
            if self._generation_owner is not state:
                raise RuntimeOwnershipError("Pipeline provider generation is no longer active")
            if self._closing_event is state or state.terminal_error is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            if self._state is not ProviderLifecycleState.ACTIVE:
                raise RuntimeOwnershipError("Pipeline provider generation is no longer active")
            if len(state.active_handoffs) >= state.handoff_limit:
                raise PipelineAdmissionRejected("pipeline_admission_queue_full")
            state.next_handoff_id += 1
            handoff_id = state.next_handoff_id
            state.active_handoffs.add(handoff_id)
            return handoff_id

    def _finish_generation_handoff(
        self,
        state: _ProviderGenerationCloseState,
        handoff_id: int,
    ) -> None:
        """Release one generation handoff permit at its real terminal edge."""
        with self._lock:
            state.active_handoffs.discard(handoff_id)

    def _finish_generation_operation(
        self,
        state: _ProviderGenerationCloseState,
        operation_id: int,
        *,
        schedule_cleanup: bool = True,
    ) -> None:
        should_schedule_cleanup = False
        with self._lock:
            if self._generation_owner is not state:
                return
            state.active_operations.discard(operation_id)
            should_schedule_cleanup = (
                schedule_cleanup
                and not state.active_operations
                and self._state in {ProviderLifecycleState.DRAINING, ProviderLifecycleState.REVOKED}
                and not state.cleanup_scheduled
            )
        if should_schedule_cleanup:
            self._schedule_generation_cleanup_from_state(state)

    def _caller_has_active_request_admission(self) -> bool:
        """Read the controller's inherited request context without creating a second lease.

        The controller currently has no public query for this state. Keep this
        compatibility read local to the provider boundary until that API lands.
        """
        lease_context = self._provider_admission_lease_context()
        tokens = lease_context.get()
        if not tokens:
            return False
        controller_lock = cast(threading.Lock, getattr(self._lifecycle, "_lock"))
        active_leases = cast(dict[int, str], getattr(self._lifecycle, "_active_leases"))
        release_pending = cast(set[int], getattr(self._lifecycle, "_release_pending"))
        with controller_lock:
            return any(token in active_leases and token not in release_pending for token in tokens)

    def _provider_admission_lease_context(self) -> contextvars.ContextVar[tuple[int, ...]]:
        """Centralize the temporary provider bridge's inherited lease context."""
        return cast(
            contextvars.ContextVar[tuple[int, ...]],
            getattr(self._lifecycle, "_current_leases"),
        )

    async def _invoke_provider_operation(
        self,
        state: _ProviderGenerationCloseState,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        self._raise_if_runtime_fatal()
        caller_admitted = self._caller_has_active_request_admission()
        if caller_admitted:
            admission = _ProviderOperationAdmission(
                lifecycle=self._lifecycle,
                retained_work=self._lifecycle.retain_current_work(),
            )
            try:
                return await self._invoke_on_generation_owner(
                    state,
                    operation,
                    limit_handoff=True,
                    admission=admission,
                )
            finally:
                admission.finish_if_unsubmitted()

        lease = await self._lifecycle.acquire()
        admission = _ProviderOperationAdmission(
            lifecycle=self._lifecycle,
            lease=lease,
        )
        lease_context = self._provider_admission_lease_context()
        context_token = lease_context.set((*lease_context.get(), lease.token))
        try:
            return await self._invoke_on_generation_owner(
                state,
                operation,
                limit_handoff=True,
                admission=admission,
            )
        finally:
            lease_context.reset(context_token)
            admission.finish_if_unsubmitted()

    def _invoke_on_generation_owner_sync(
        self,
        state: _ProviderGenerationCloseState,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Transport a synchronous accessor onto the persistent owner loop."""
        owner_loop = state.loop
        if owner_loop is None:
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner is not ready")
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is owner_loop:
            raise RuntimeOwnershipError("Pipeline provider realization cannot recurse on its owner loop")
        with self._lock:
            if self._generation_owner is not state:
                raise RuntimeOwnershipError("Pipeline provider generation is no longer active")
            if self._closing_event is state or state.terminal_error is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            if owner_loop.is_closed() or not owner_loop.is_running():
                raise RuntimeOwnershipError("Pipeline provider lifecycle owner is unavailable")
        coroutine = cast(Coroutine[Any, Any, Any], operation())
        try:
            future: ThreadFuture[Any] = asyncio.run_coroutine_threadsafe(coroutine, owner_loop)
        except BaseException:
            coroutine.close()
            raise
        return future.result()

    async def _realize_provider_on_owner(
        self,
        state: _ProviderGenerationCloseState,
        construct: Callable[[], Any],
        validate: Callable[[Any], None],
        adopt: Callable[[Any, _ProviderAdoptionHandoff], Awaitable[None]],
        *,
        reason_code: str,
        finish_initialization: Callable[[], None] | None = None,
    ) -> Any:
        owner_operation_started = threading.Event()
        handoff = _ProviderAdoptionHandoff()

        async def construct_and_validate() -> Any:
            owner_operation_started.set()
            worker_entered = threading.Event()
            worker_cleanup_owns_initialization = False
            validation_rejected = threading.Event()
            rejection_lock = threading.Lock()
            rejection_primary_error: BaseException | None = None
            rejection_cleanup_failure: RuntimeOwnershipError | None = None

            def finish_initialization_once() -> None:
                if finish_initialization is not None:
                    finish_initialization()

            def construct_on_worker() -> Any:
                worker_entered.set()
                try:
                    return construct()
                except BaseException:
                    finish_initialization_once()
                    raise

            async def adopt_on_owner(product: Any) -> None:
                if state.owner_thread is not threading.current_thread():
                    raise RuntimeOwnershipError("Provider adoption must run on its lifecycle owner thread")
                if not handoff.begin_on_owner():
                    raise RuntimeOwnershipError("Provider realization was cancelled before adoption")
                try:
                    await adopt(product, handoff)
                    if not handoff.is_adopted_on_owner():
                        raise RuntimeOwnershipError("Provider adoption did not commit its cache handoff")
                except BaseException:
                    handoff.reject_on_owner()
                    raise

            def validate_and_adopt(product: Any) -> None:
                nonlocal rejection_primary_error
                try:
                    validate(product)
                except BaseException as validation_error:
                    with rejection_lock:
                        rejection_primary_error = validation_error
                    validation_rejected.set()
                    raise
                operation_id = self._begin_generation_operation(state, allow_starting=True)
                adoption_coroutine = adopt_on_owner(product)
                owner_loop = state.loop
                if owner_loop is None or owner_loop.is_closed() or not owner_loop.is_running():
                    adoption_coroutine.close()
                    self._finish_generation_operation(state, operation_id)
                    raise RuntimeOwnershipError("Pipeline provider lifecycle owner is unavailable")
                try:
                    adoption_future = asyncio.run_coroutine_threadsafe(adoption_coroutine, owner_loop)
                except BaseException:
                    adoption_coroutine.close()
                    self._finish_generation_operation(state, operation_id)
                    raise
                try:
                    adoption_future.result()
                finally:
                    self._finish_generation_operation(state, operation_id)

            async def retire_bound_product(product: Any) -> bool:
                if product is None or getattr(product, "_lifecycle_invocation_owner", None) is not state:
                    return False
                first_error: BaseException | None = None
                close_operation = getattr(product, "_lifecycle_close_operation", None)
                try:
                    if callable(close_operation):
                        close_result = close_operation()
                        if inspect.isawaitable(close_result):
                            await asyncio.wait_for(
                                close_result,
                                timeout=self._cleanup_grace_seconds,
                            )
                except BaseException as exc:
                    first_error = exc
                try:
                    if isinstance(product, LLMProvider):
                        LLMProvider.revoke_lifecycle_invoker(product, owner=state)
                    elif isinstance(product, ContextProvider):
                        ContextProvider.revoke_lifecycle_invoker(product, owner=state)
                except BaseException as exc:
                    if first_error is None:
                        first_error = exc
                if first_error is not None:
                    raise first_error
                return True

            async def retire_unadopted(product: Any) -> None:
                nonlocal rejection_cleanup_failure
                try:
                    if handoff.adopted:
                        return
                    try:
                        retired_bound_product = await retire_bound_product(product)
                        if not retired_bound_product:
                            await _retire_products(
                                product,
                                reason_code="provider_realization_lifecycle_expired",
                                cleanup_grace_seconds=self._cleanup_grace_seconds,
                            )
                    except BaseException as cleanup_error:
                        if not validation_rejected.is_set():
                            raise
                        with rejection_lock:
                            primary_error = rejection_primary_error
                        product_loop = getattr(product, "_factory_event_loop", None)
                        foreign_loop_rejection = bool(getattr(product, "_foreign_loop_rejection", False))
                        if foreign_loop_rejection and (
                            product_loop is None or product_loop.is_closed() or not product_loop.is_running()
                        ):
                            failure = RuntimeOwnershipError("Rejected provider event loop owner is unavailable")
                            if primary_error is not None:
                                failure.__cause__ = primary_error
                                failure.__suppress_context__ = True
                        else:
                            from tacit.pipeline.side_effects import terminal_cleanup_failure

                            failure = terminal_cleanup_failure(
                                primary_error,
                                cleanup_error,
                                reason_code="provider_rejected_cleanup_failed_closed",
                                message="Provider rejected product cleanup failed",
                            )
                        setattr(failure, "cleanup_reason_code", "provider_rejected_cleanup_failed_closed")
                        setattr(failure, "cleanup_error_type", type(cleanup_error).__name__[:128])
                        setattr(failure, "runtime_provider_fatal", True)
                        logger.warning(
                            "provider_rejected_cleanup_failed_closed",
                            reason_code="provider_rejected_cleanup_failed_closed",
                            error_type=type(cleanup_error).__name__,
                        )
                        self._fence_generation(state, failure)
                        self._lifecycle.fence_runtime_fatal(failure)
                        with rejection_lock:
                            rejection_cleanup_failure = failure
                finally:
                    finish_initialization_once()

            try:
                return await self._provider_factory_work.realize_owned(
                    construct_on_worker,
                    validate=validate_and_adopt,
                    retire=retire_unadopted,
                    reason_code=reason_code,
                    result_handoff_seconds=self._cleanup_grace_seconds,
                )
            except asyncio.CancelledError:
                handoff.revoke_pending()
                worker_cleanup_owns_initialization = worker_entered.is_set()
                raise
            except BaseException as realization_error:
                with rejection_lock:
                    failure = rejection_cleanup_failure
                if failure is not None:
                    raise failure from realization_error
                raise
            finally:
                if not worker_cleanup_owns_initialization:
                    finish_initialization_once()

        try:
            return await self._invoke_on_generation_owner(
                state,
                construct_and_validate,
                settle_on_cancel=True,
                allow_starting=True,
            )
        except BaseException:
            if not owner_operation_started.is_set() and finish_initialization is not None:
                finish_initialization()
            raise

    async def _realize_llm_provider(self) -> LLMProvider:
        state = await self._ensure_generation_owner()

        def construct() -> LLMProvider:
            with observe_runtime_factory_realization("provider:llm"):
                return self._llm_factory()

        def validate(provider: LLMProvider) -> None:
            if self._requires_async_llm_realization:
                from tacit.agents.providers.bedrock import BedrockProvider

                if not isinstance(provider, BedrockProvider):
                    raise RuntimeOwnershipError(
                        "Configured Bedrock runtimes require the operation-scoped Bedrock provider"
                    )
            _validate_llm_provider_product(
                provider,
                expected=self._llm_expected,
                lifecycle=self._lifecycle,
                cleanup_grace_seconds=self._cleanup_grace_seconds,
                cleanup_rejected=False,
                expected_event_loop=state.loop,
            )

        return cast(
            LLMProvider,
            await self._realize_provider_on_owner(
                state,
                construct,
                validate,
                lambda provider, handoff: self._adopt_llm_provider(
                    provider,
                    handoff=handoff,
                    expected_state=state,
                ),
                reason_code="provider:llm_realization",
                finish_initialization=self._finish_active_llm_initialization,
            ),
        )

    def llm_ownership(self, *, component: str) -> RuntimeOwnershipDescriptor:
        """Return the frozen provider-plan declaration for resource wrappers."""
        return replace(self._llm_expected, component=component)

    async def _acquire_adoption_lock_on_owner(
        self,
        state: _ProviderGenerationCloseState,
    ) -> None:
        if state.owner_thread is not threading.current_thread():
            raise RuntimeOwnershipError("Provider adoption must run on its lifecycle owner thread")
        while not self._lock.acquire(blocking=False):
            await asyncio.sleep(0)

    async def _adopt_llm_provider(
        self,
        provider: LLMProvider,
        *,
        handoff: _ProviderAdoptionHandoff,
        expected_state: _ProviderGenerationCloseState,
    ) -> None:
        state = expected_state
        await self._acquire_adoption_lock_on_owner(state)
        try:
            if self._llm_provider is not None and self._llm_provider is not provider:
                raise RuntimeOwnershipError("Pipeline LLM provider generation was already adopted")
            if self._generation_owner is not state:
                raise RuntimeOwnershipError("Pipeline LLM provider generation was superseded")
            if state.terminal_error is not None:
                raise RuntimeOwnershipError("Pipeline LLM provider generation is no longer active")
            if self._llm_adoption is not None and self._llm_adoption is not handoff:
                raise RuntimeOwnershipError("Pipeline LLM provider adoption is already reserved")
            self._llm_adoption = handoff
        finally:
            self._lock.release()

        try:
            LLMProvider.bind_lifecycle_invoker(
                provider,
                owner=state,
                invoke=lambda operation: self._invoke_provider_operation(state, operation),
                close_invoke=lambda operation: self._invoke_on_generation_owner(
                    state,
                    operation,
                    track_operation=False,
                ),
            )
            await self._acquire_adoption_lock_on_owner(state)
            try:
                if self._generation_owner is not state or state.terminal_error is not None:
                    raise RuntimeOwnershipError("Pipeline LLM provider generation was superseded")
                if self._llm_adoption is not handoff:
                    raise RuntimeOwnershipError("Pipeline LLM provider adoption reservation was lost")
                self._llm_provider = provider
                self._cleanup_pending = True
                handoff.commit_on_owner()
                self._llm_adoption = None
            finally:
                self._lock.release()
        except BaseException:
            await self._acquire_adoption_lock_on_owner(state)
            try:
                if self._llm_adoption is handoff:
                    self._llm_adoption = None
            finally:
                self._lock.release()
            raise

    def _finish_llm_initialization(self, initializing: ThreadFuture[None]) -> None:
        with self._lock:
            if not initializing.done():
                initializing.set_result(None)
            if self._llm_initializing is initializing:
                self._llm_initializing = None

    def _finish_active_llm_initialization(self) -> None:
        with self._lock:
            initializing = self._llm_initializing
        if initializing is not None:
            self._finish_llm_initialization(initializing)

    async def _ensure_context_provider(self) -> None:
        while True:
            initializer = False
            with self._lock:
                if self._context_initialized:
                    return
                initializing = self._context_initializing
                if initializing is None:
                    initializing = ThreadFuture()
                    self._context_initializing = initializing
                    initializer = True
            if not initializer:
                await asyncio.shield(asyncio.wrap_future(initializing))
                continue

            try:
                state = await self._ensure_generation_owner()
            except BaseException:
                self._finish_context_initialization(initializing)
                raise
            realization_task: asyncio.Task[ContextProvider | None] | None = None
            try:
                realization_task = asyncio.create_task(
                    self._realize_context_provider(),
                    name="tacit-context-provider-realization",
                )
                try:
                    await asyncio.shield(realization_task)
                except asyncio.CancelledError:
                    await self._settle_cancelled_realization(realization_task)
                    raise
                else:
                    with self._lock:
                        if (
                            self._shutdown_requested
                            or self._generation_owner is not state
                            or not self._context_initialized
                        ):
                            raise RuntimeOwnershipError(
                                "Pipeline context provider generation was retired during realization"
                            )
                    return
            except asyncio.CancelledError:
                raise
            except BaseException:
                if realization_task is None or (self._context_initializing is initializing and realization_task.done()):
                    self._finish_context_initialization(initializing)
                raise

    async def _realize_context_provider(self) -> ContextProvider | None:
        state = await self._ensure_generation_owner()

        def construct() -> ContextProvider | None:
            with observe_runtime_factory_realization("provider:context"):
                return self._context_factory()

        def validate(provider: ContextProvider | None) -> None:
            _validate_context_provider_product(
                provider,
                expected=self._context_expected,
                context_disabled=self._context_disabled,
                lifecycle=self._lifecycle,
                cleanup_grace_seconds=self._cleanup_grace_seconds,
                cleanup_rejected=False,
                expected_event_loop=state.loop,
            )

        return cast(
            ContextProvider | None,
            await self._realize_provider_on_owner(
                state,
                construct,
                validate,
                lambda provider, handoff: self._adopt_context_provider(
                    provider,
                    handoff=handoff,
                    expected_state=state,
                ),
                reason_code="provider:context_realization",
                finish_initialization=self._finish_active_context_initialization,
            ),
        )

    async def _adopt_context_provider(
        self,
        provider: ContextProvider | None,
        *,
        handoff: _ProviderAdoptionHandoff,
        expected_state: _ProviderGenerationCloseState,
    ) -> None:
        state = expected_state
        await self._acquire_adoption_lock_on_owner(state)
        try:
            if self._context_initialized and self._context_provider is not provider:
                raise RuntimeOwnershipError("Pipeline context provider generation was already adopted")
            if self._generation_owner is not state:
                raise RuntimeOwnershipError("Pipeline context provider generation was superseded")
            if state.terminal_error is not None:
                raise RuntimeOwnershipError("Pipeline context provider generation is no longer active")
            if self._context_adoption is not None and self._context_adoption is not handoff:
                raise RuntimeOwnershipError("Pipeline context provider adoption is already reserved")
            self._context_adoption = handoff
        finally:
            self._lock.release()

        try:
            if provider is not None:
                ContextProvider.bind_lifecycle_invoker(
                    provider,
                    owner=state,
                    invoke=lambda operation: self._invoke_provider_operation(state, operation),
                    close_invoke=lambda operation: self._invoke_on_generation_owner(
                        state,
                        operation,
                        track_operation=False,
                    ),
                )
            await self._acquire_adoption_lock_on_owner(state)
            try:
                if self._generation_owner is not state or state.terminal_error is not None:
                    raise RuntimeOwnershipError("Pipeline context provider generation was superseded")
                if self._context_adoption is not handoff:
                    raise RuntimeOwnershipError("Pipeline context provider adoption reservation was lost")
                self._context_provider = provider
                self._context_initialized = True
                self._cleanup_pending = True
                handoff.commit_on_owner()
                self._context_adoption = None
            finally:
                self._lock.release()
        except BaseException:
            await self._acquire_adoption_lock_on_owner(state)
            try:
                if self._context_adoption is handoff:
                    self._context_adoption = None
            finally:
                self._lock.release()
            raise

    def _finish_context_initialization(self, initializing: ThreadFuture[None]) -> None:
        with self._lock:
            if not initializing.done():
                initializing.set_result(None)
            if self._context_initializing is initializing:
                self._context_initializing = None

    def _finish_active_context_initialization(self) -> None:
        with self._lock:
            initializing = self._context_initializing
        if initializing is not None:
            self._finish_context_initialization(initializing)

    @staticmethod
    async def _settle_cancelled_realization(
        task: asyncio.Task[Any],
    ) -> tuple[bool, Any]:
        task.cancel()
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if task.cancelled():
            return False, None
        try:
            return True, task.result()
        except BaseException:
            return False, None

    def llm(self) -> LLMProvider:
        manager = self._resolve_active_manager()
        if manager is not self:
            return manager.llm()
        with self._lock:
            if self._closing_event is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            provider = self._llm_provider
            if provider is not None:
                return provider
            if self._requires_async_llm_realization:
                raise RuntimeOwnershipError("Pipeline LLM provider resources were not acquired")
            if self._llm_initializing is not None:
                raise RuntimeOwnershipError("Pipeline LLM provider resources are being acquired")
        with self._sync_realization_lock:
            with self._lock:
                provider = self._llm_provider
                if provider is not None:
                    return provider
            state = self._ensure_generation_owner_sync()
            operation_id = self._begin_generation_operation(state, allow_starting=True)
            try:
                provider = cast(
                    LLMProvider,
                    self._invoke_on_generation_owner_sync(state, self._realize_llm_provider),
                )
                with self._lock:
                    if self._shutdown_requested or self._generation_owner is not state:
                        raise RuntimeOwnershipError("Pipeline provider runtime shut down during startup")
                    if self._state is ProviderLifecycleState.STARTING:
                        self._state = ProviderLifecycleState.ACTIVE
                return provider
            finally:
                self._finish_generation_operation(state, operation_id)

    def context(self) -> ContextProvider | None:
        manager = self._resolve_active_manager()
        if manager is not self:
            return manager.context()
        with self._lock:
            if self._closing_event is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            if self._context_initialized:
                return self._context_provider
            if self._context_initializing is not None:
                raise RuntimeOwnershipError("Pipeline context provider resources are being acquired")
        with self._sync_realization_lock:
            with self._lock:
                if self._context_initialized:
                    return self._context_provider
            state = self._ensure_generation_owner_sync()
            operation_id = self._begin_generation_operation(state, allow_starting=True)
            try:
                provider = cast(
                    ContextProvider | None,
                    self._invoke_on_generation_owner_sync(state, self._realize_context_provider),
                )
                with self._lock:
                    if self._shutdown_requested or self._generation_owner is not state:
                        raise RuntimeOwnershipError("Pipeline provider runtime shut down during startup")
                    if self._state is ProviderLifecycleState.STARTING:
                        self._state = ProviderLifecycleState.ACTIVE
                return provider
            finally:
                self._finish_generation_operation(state, operation_id)

    async def _wait_for_generation_close(self, state: _ProviderGenerationCloseState) -> bool:
        loop = asyncio.get_running_loop()
        waiter = asyncio.Event()
        with self._lock:
            if not state.done.is_set():
                state.waiters.append((loop, waiter))
        if not state.done.is_set():
            try:
                await asyncio.wait_for(waiter.wait(), timeout=self._cleanup_grace_seconds)
            except TimeoutError:
                if not state.done.is_set():
                    self._revoke_generation(
                        state,
                        RuntimeOwnershipError("Pipeline provider cleanup exceeded its grace period"),
                    )
                try:
                    await asyncio.wait_for(waiter.wait(), timeout=self._cleanup_grace_seconds)
                except TimeoutError as timeout_error:
                    raise self._fence_stalled_generation(state) from timeout_error
            finally:
                with self._lock:
                    try:
                        state.waiters.remove((loop, waiter))
                    except ValueError:
                        pass
        if state.done.is_set():
            await self._join_retired_generation(state)
        with self._lock:
            terminal_error = state.terminal_error
        if terminal_error is not None:
            raise self._stable_generation_error(terminal_error) from terminal_error
        return True

    def _fence_stalled_generation(self, state: _ProviderGenerationCloseState) -> RuntimeOwnershipError:
        stalled_error = RuntimeOwnershipError(
            "Pipeline provider lifecycle owner did not settle within its grace period"
        )
        setattr(stalled_error, "cleanup_reason_code", "provider_generation_owner_stalled")
        setattr(stalled_error, "cleanup_error_type", "TimeoutError")
        setattr(stalled_error, "runtime_provider_fatal", True)
        stable_error = self._fence_generation(state, stalled_error)
        self._lifecycle.fence_runtime_fatal(stable_error)
        return self._stable_generation_error(stable_error)

    async def _wait_for_generation_owner_exit(self, state: _ProviderGenerationCloseState) -> None:
        thread = state.owner_thread
        if thread is None or thread is threading.current_thread():
            return
        owner_exited = asyncio.wrap_future(state.owner_exited)
        try:
            await asyncio.wait_for(
                asyncio.shield(owner_exited),
                timeout=_LIFECYCLE_OWNER_STARTUP_TIMEOUT_SECONDS,
            )
        except TimeoutError as timeout_error:
            raise self._fence_stalled_generation(state) from timeout_error
        thread.join(timeout=0)

    async def _join_retired_generation(self, state: _ProviderGenerationCloseState) -> None:
        await self._wait_for_generation_owner_exit(state)
        with self._lock:
            if self._retired_generation is state:
                self._retired_generation = None

    def _join_retired_generation_sync_locked(self) -> None:
        state = self._retired_generation
        if state is None:
            return
        thread = state.owner_thread
        if thread is not None and thread is not threading.current_thread():
            if thread.is_alive():
                raise RuntimeOwnershipError("Pipeline provider lifecycle owner is still retiring")
            thread.join(timeout=0)
        if self._retired_generation is state:
            self._retired_generation = None

    async def _settle_generation_cleanup(
        self,
        label: str,
        cleanup: Callable[[], Awaitable[None]],
    ) -> RuntimeOwnershipError | None:
        for attempt in range(2):
            cleanup_task: asyncio.Task[None] = asyncio.create_task(
                cast(Coroutine[Any, Any, None], cleanup()),
                name=f"tacit-provider-{label}-cleanup",
            )
            try:
                await asyncio.shield(cleanup_task)
                return None
            except asyncio.CancelledError as exc:
                owner_task = asyncio.current_task()
                if owner_task is not None and owner_task.cancelling():
                    cleanup_task.cancel()
                    await asyncio.gather(cleanup_task, return_exceptions=True)
                    raise
                logger.warning(
                    "provider_child_cleanup_cancelled",
                    reason_code="provider_child_cleanup_cancelled",
                    resource=label,
                    error_type=type(exc).__name__,
                    retrying=attempt == 0,
                )
                if attempt == 1:
                    from tacit.pipeline.side_effects import terminal_cleanup_failure

                    return terminal_cleanup_failure(
                        None,
                        exc,
                        reason_code="provider_child_cleanup_cancelled",
                        message="Provider child cleanup did not complete",
                    )
            except GeneratorExit:
                raise
            except BaseException as exc:
                logger.warning(
                    "provider_child_cleanup_failed",
                    reason_code="provider_child_cleanup_failed",
                    resource=label,
                    error_type=type(exc).__name__,
                    retrying=attempt == 0,
                )
                if attempt == 1:
                    from tacit.pipeline.side_effects import terminal_cleanup_failure

                    return terminal_cleanup_failure(
                        None,
                        exc,
                        reason_code="provider_child_cleanup_failed",
                        message="Provider child cleanup did not complete",
                    )
        return None

    @staticmethod
    def _notify_generation_waiters(
        waiters: tuple[tuple[asyncio.AbstractEventLoop, asyncio.Event], ...],
    ) -> None:
        for loop, waiter in waiters:
            if loop.is_closed():
                continue
            try:
                loop.call_soon_threadsafe(waiter.set)
            except RuntimeError:
                continue

    @staticmethod
    def _stable_generation_error(error: BaseException) -> RuntimeOwnershipError:
        failure = RuntimeOwnershipError("Pipeline provider generation cleanup failed")
        for attribute in (
            "cleanup_reason_code",
            "cleanup_error_type",
            "cleanup_retains_capacity",
            "runtime_provider_fatal",
        ):
            value = getattr(error, attribute, None)
            if value is not None:
                setattr(failure, attribute, value)
        return failure

    def _fence_generation(
        self,
        state: _ProviderGenerationCloseState,
        error: BaseException,
    ) -> RuntimeOwnershipError:
        with self._lock:
            if state.terminal_error is None:
                stable_error = self._stable_generation_error(error)
                if stable_error is not error:
                    stable_error.__cause__ = error
                state.terminal_error = stable_error
            else:
                stable_error = cast(RuntimeOwnershipError, state.terminal_error)
            if self._generation_owner is state:
                self._state = ProviderLifecycleState.REVOKED
                self._closing_event = state
        logger.warning(
            "provider_generation_revoked",
            reason_code="provider_generation_revoked",
            error_type=type(error).__name__,
            generation_epoch=state.generation_epoch,
        )
        return stable_error

    def _revoke_generation(
        self,
        state: _ProviderGenerationCloseState,
        error: BaseException,
    ) -> None:
        self._fence_generation(state, error)
        with self._lock:
            cleanup_future = state.cleanup_future
            owner_loop = state.loop
            active_operations = bool(state.active_operations)
            owner_recovery_active = state.owner_recovery_active
        if active_operations:
            return
        if cleanup_future is not None and not cleanup_future.done():
            cleanup_future.cancel()
        if owner_loop is not None and not owner_loop.is_closed() and not owner_recovery_active:
            self._request_generation_owner_stop(state, owner_loop)

    @staticmethod
    def _request_generation_owner_stop(
        state: _ProviderGenerationCloseState,
        owner_loop: asyncio.AbstractEventLoop,
    ) -> bool:
        """Request stop with a fixed transport bound; the owner monitor is fallback."""
        last_error: RuntimeError | None = None
        for _attempt in range(_PROVIDER_OWNER_STOP_ATTEMPTS):
            try:
                owner_loop.call_soon_threadsafe(owner_loop.stop)
                return True
            except RuntimeError as exc:
                last_error = exc
        logger.warning(
            "provider_owner_stop_transport_failed",
            reason_code="provider_owner_stop_transport_failed",
            error_type=type(last_error).__name__ if last_error is not None else "RuntimeError",
            attempts=_PROVIDER_OWNER_STOP_ATTEMPTS,
            generation_epoch=state.generation_epoch,
        )
        return False

    async def _monitor_generation_terminal_authority(
        self,
        state: _ProviderGenerationCloseState,
        owner_loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Let the generation owner recover when cross-thread callback transport fails."""
        recovery_started = False
        try:
            while True:
                await asyncio.sleep(_PROVIDER_OWNER_TERMINAL_POLL_SECONDS)
                with self._lock:
                    if self._generation_owner is not state:
                        return
                    cleanup_succeeded = state.cleanup_succeeded
                    terminal_error = state.terminal_error
                    cleanup_future = state.cleanup_future
                    cleanup_submission_failed = state.cleanup_submission_failed

                if cleanup_succeeded:
                    if recovery_started:
                        logger.info(
                            "provider_generation_terminal_recovery_settled",
                            reason_code="provider_generation_terminal_recovery_settled",
                            generation_epoch=state.generation_epoch,
                        )
                    owner_loop.stop()
                    return
                if terminal_error is None:
                    continue

                if not recovery_started:
                    # Let any ambiguously enqueued submission callback run before
                    # the owner decides that the handoff never started.
                    recovery_started = True
                    await asyncio.sleep(0)

                settled = self._settle_owner_terminal_submissions(state, terminal_error)
                with self._lock:
                    active_operations = bool(state.active_operations)
                if settled:
                    logger.info(
                        "provider_generation_terminal_submissions_settled",
                        reason_code="provider_generation_terminal_submissions_settled",
                        settled_submissions=settled,
                        generation_epoch=state.generation_epoch,
                    )
                if not active_operations:
                    self._begin_generation_retirement(state)

                with self._lock:
                    cleanup_succeeded = state.cleanup_succeeded
                    cleanup_future = state.cleanup_future
                    cleanup_submission_failed = state.cleanup_submission_failed
                if cleanup_succeeded:
                    logger.info(
                        "provider_generation_terminal_recovery_settled",
                        reason_code="provider_generation_terminal_recovery_settled",
                        generation_epoch=state.generation_epoch,
                    )
                    owner_loop.stop()
                    return
                if cleanup_future is not None and cleanup_future.done():
                    owner_loop.stop()
                    return
                if cleanup_submission_failed and cleanup_future is None:
                    # Cleanup submission itself failed. The owner-thread outer
                    # terminal transition revokes products and releases service
                    # ownership without waiting for caller transport.
                    owner_loop.stop()
                    return
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._fence_generation(state, exc)
            owner_loop.stop()

    async def _settle_generation_after_owner_loop_loss(
        self,
        state: _ProviderGenerationCloseState,
        owner_loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Reach a cleanup terminal state before the dedicated owner loop closes."""
        current_task = asyncio.current_task()
        with self._lock:
            cleanup_scheduled = state.cleanup_scheduled
            terminal_error = state.terminal_error or RuntimeOwnershipError(
                "Pipeline provider lifecycle loop stopped before cleanup"
            )
        if not cleanup_scheduled:
            active_tasks = tuple(
                task for task in asyncio.all_tasks(owner_loop) if task is not current_task and not task.done()
            )
            for task in active_tasks:
                task.cancel()
            if active_tasks:
                await asyncio.gather(*active_tasks, return_exceptions=True)
            self._settle_committed_generation_submissions(state, terminal_error)
            await asyncio.sleep(0)

        self._begin_generation_retirement(state)
        await asyncio.sleep(0)
        with self._lock:
            cleanup_future = state.cleanup_future
        if cleanup_future is not None:
            try:
                await asyncio.shield(asyncio.wrap_future(cleanup_future))
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            await asyncio.sleep(0)

        remaining_tasks = tuple(
            task for task in asyncio.all_tasks(owner_loop) if task is not current_task and not task.done()
        )
        for task in remaining_tasks:
            task.cancel()
        if remaining_tasks:
            await asyncio.gather(*remaining_tasks, return_exceptions=True)
        self._settle_committed_generation_submissions(state, terminal_error)

    def _run_generation_owner(self, state: _ProviderGenerationCloseState) -> BaseException | None:
        ready = False
        try:
            with asyncio.Runner() as runner:
                owner_loop = runner.get_loop()
                with self._lock:
                    if self._generation_owner is not state:
                        raise RuntimeOwnershipError("Pipeline provider lifecycle owner was superseded")
                    state.loop = owner_loop

                def publish_readiness() -> None:
                    nonlocal ready
                    schedule_cleanup = False
                    with self._lock:
                        if state.startup_aborted:
                            if not state.ready.done():
                                state.ready.set_exception(
                                    RuntimeOwnershipError("Pipeline provider lifecycle owner startup was aborted")
                                )
                            state.startup_ready.set()
                            owner_loop.stop()
                            return
                        if self._generation_owner is not state:
                            if not state.ready.done():
                                state.ready.set_exception(
                                    RuntimeOwnershipError("Pipeline provider lifecycle owner was superseded")
                                )
                            owner_loop.stop()
                            return
                        terminal_monitor = owner_loop.create_task(
                            self._monitor_generation_terminal_authority(state, owner_loop),
                            name=f"tacit-provider-terminal-monitor-{state.generation_epoch}",
                        )
                        state.terminal_monitor_task = terminal_monitor
                        state.owner_ready = True
                        ready = True
                        schedule_cleanup = (
                            self._state in {ProviderLifecycleState.DRAINING, ProviderLifecycleState.REVOKED}
                            and not state.active_operations
                            and not state.cleanup_scheduled
                        )
                    if not state.ready.done():
                        state.ready.set_result(None)
                    state.startup_ready.set()
                    if schedule_cleanup:
                        self._schedule_generation_cleanup_from_state(state)

                owner_loop.call_soon(publish_readiness)
                owner_loop.run_forever()
                with self._lock:
                    cleanup_succeeded = state.cleanup_succeeded
                    terminal_error = state.terminal_error
                if terminal_error is not None:
                    return terminal_error
                if cleanup_succeeded:
                    return None
                with self._lock:
                    state.owner_recovery_active = True
                owner_loss_error = RuntimeOwnershipError("Pipeline provider lifecycle loop stopped before cleanup")
                setattr(owner_loss_error, "cleanup_reason_code", "provider_generation_owner_lost")
                setattr(owner_loss_error, "cleanup_error_type", "RuntimeOwnershipError")
                setattr(owner_loss_error, "runtime_provider_fatal", True)
                self._publish_generation_startup_failure(state, owner_loss_error)
                owner_loss_error = self._fence_generation(state, owner_loss_error)
                try:
                    runner.run(self._settle_generation_after_owner_loop_loss(state, owner_loop))
                finally:
                    with self._lock:
                        state.owner_recovery_active = False
                with self._lock:
                    return state.terminal_error or owner_loss_error
        except BaseException as exc:
            self._publish_generation_startup_failure(state, exc)
            if ready:
                return self._fence_generation(state, exc)
            return exc

    def _schedule_generation_cleanup_from_state(
        self,
        state: _ProviderGenerationCloseState,
    ) -> None:
        with self._lock:
            if self._generation_owner is not state or state.cleanup_scheduled:
                return
            if state.active_operations:
                return
            state.cleanup_scheduled = True
            self._cleanup_in_flight += 1
            context_provider = self._context_provider if self._context_initialized else None
            llm_provider = self._llm_provider
            chained_cleanup = self._chained_cleanup if self._cleanup_pending else None
            state.retained_products = tuple(
                product for product in (context_provider, llm_provider) if product is not None
            )
        owner_loop = state.loop
        if owner_loop is None or owner_loop.is_closed() or not owner_loop.is_running():
            self._revoke_generation(
                state,
                RuntimeOwnershipError("Pipeline provider lifecycle owner is unavailable for cleanup"),
            )
            return

        async def cleanup_generation() -> None:
            cleanup_calls: list[tuple[str, Callable[[], Awaitable[None]]]] = []
            retained_products = tuple(product for product in (context_provider, llm_provider) if product is not None)
            if context_provider is not None:
                cleanup_calls.append(
                    (
                        "context",
                        lambda: ContextProvider.close_from_lifecycle(context_provider, owner=state),
                    )
                )
            if llm_provider is not None:
                cleanup_calls.append(
                    (
                        "llm",
                        lambda: LLMProvider.close_from_lifecycle(llm_provider, owner=state),
                    )
                )
            if chained_cleanup is not None:
                cleanup_calls.append(("chained", chained_cleanup))
            results: list[RuntimeOwnershipError | None] = []
            revoke_error: RuntimeOwnershipError | None = None
            try:
                results = list(
                    await asyncio.gather(
                        *(self._settle_generation_cleanup(label, cleanup) for label, cleanup in cleanup_calls)
                    )
                )
            finally:
                for product in retained_products:
                    try:
                        if isinstance(product, LLMProvider):
                            LLMProvider.revoke_lifecycle_invoker(product, owner=state)
                        elif isinstance(product, ContextProvider):
                            ContextProvider.revoke_lifecycle_invoker(product, owner=state)
                    except BaseException as exc:
                        if revoke_error is None:
                            from tacit.pipeline.side_effects import terminal_cleanup_failure

                            revoke_error = terminal_cleanup_failure(
                                None,
                                exc,
                                reason_code="provider_authority_revoke_failed",
                                message="Provider authority revocation failed",
                            )
                cleanup_calls.clear()
            cleanup_error = next((result for result in results if result is not None), None)
            if cleanup_error is None:
                cleanup_error = revoke_error
            results.clear()
            if cleanup_error is not None:
                raise cleanup_error.with_traceback(None) from None

        cleanup_coroutine = cleanup_generation()
        try:
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            cleanup_future: ThreadFuture[None]
            if current_loop is owner_loop:
                cleanup_future = ThreadFuture()
                cleanup_task = owner_loop.create_task(
                    cleanup_coroutine,
                    name=f"tacit-provider-generation-{state.generation_epoch}-cleanup",
                )

                def publish_cleanup_result(task: asyncio.Task[None]) -> None:
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        cleanup_future.cancel()
                    except BaseException as exc:
                        cleanup_future.set_exception(exc)
                    else:
                        cleanup_future.set_result(None)

                cleanup_task.add_done_callback(publish_cleanup_result)
            else:
                cleanup_future = asyncio.run_coroutine_threadsafe(cleanup_coroutine, owner_loop)
        except BaseException as exc:
            cleanup_coroutine.close()
            with self._lock:
                state.cleanup_submission_failed = True
            self._revoke_generation(state, exc)
            return
        with self._lock:
            state.cleanup_future = cleanup_future

        def cleanup_completed(_future: ThreadFuture[None]) -> None:
            try:
                _future.result()
            except BaseException as exc:
                self._revoke_generation(state, exc)
                return
            with self._lock:
                state.cleanup_succeeded = True
                owner_recovery_active = state.owner_recovery_active
            if owner_recovery_active:
                return
            self._request_generation_owner_stop(state, owner_loop)

        cleanup_future.add_done_callback(cleanup_completed)

    def _begin_generation_retirement(
        self,
        state: _ProviderGenerationCloseState,
        *,
        revoked_by: BaseException | None = None,
    ) -> None:
        schedule_cleanup = False
        with self._lock:
            if self._generation_owner is not state:
                return
            if revoked_by is not None:
                self._fence_generation(state, revoked_by)
            elif self._state not in {ProviderLifecycleState.REVOKED, ProviderLifecycleState.DRAINING}:
                self._state = ProviderLifecycleState.DRAINING
                self._closing_event = state
            schedule_cleanup = state.owner_ready and not state.active_operations and not state.cleanup_scheduled
        if schedule_cleanup:
            self._schedule_generation_cleanup_from_state(state)

    async def _retire_unleased_generation(
        self,
        state: _ProviderGenerationCloseState,
        *,
        revoked_by: BaseException | None = None,
        wait: bool = True,
    ) -> None:
        with self._lock:
            if self._generation_owner is not state or self._active_leases:
                return
            # Reserve DRAINING/REVOKED while the no-lease observation is still
            # protected. A replacement acquire can win before this block or
            # observe the reservation afterward, never lease between them.
            self._begin_generation_retirement(state, revoked_by=revoked_by)
        if wait:
            await self._wait_for_generation_close(state)

    def _clear_generation_locked(self, state: _ProviderGenerationCloseState) -> None:
        state.retained_products = ()
        state.cleanup_future = None
        state.terminal_monitor_task = None
        state.loop = None
        self._context_provider = None
        self._context_initialized = False
        self._llm_provider = None
        self._context_adoption = None
        self._llm_adoption = None
        self._cleanup_pending = False
        if state.cleanup_scheduled:
            self._cleanup_in_flight = max(self._cleanup_in_flight - 1, 0)
        if self._closing_event is state:
            self._closing_event = None
        if self._generation_owner is state:
            self._generation_owner = None
            self._retired_generation = state
        self._state = ProviderLifecycleState.EMPTY

    def _finish_generation_cleanup(self, state: _ProviderGenerationCloseState) -> None:
        with self._lock:
            if not state.cleanup_succeeded:
                raise RuntimeOwnershipError("Pipeline provider cleanup cannot release an incomplete generation")
            self._clear_generation_locked(state)
            state.done.set()

    def _finish_generation_terminal_failure(
        self,
        state: _ProviderGenerationCloseState,
        error: BaseException,
    ) -> None:
        with self._lock:
            retained_products = tuple(
                product
                for product in (
                    *state.retained_products,
                    self._context_provider,
                    self._llm_provider,
                )
                if product is not None
            )
        for product in {id(product): product for product in retained_products}.values():
            try:
                if isinstance(product, LLMProvider):
                    LLMProvider.revoke_lifecycle_invoker(product, owner=state)
                elif isinstance(product, ContextProvider):
                    ContextProvider.revoke_lifecycle_invoker(product, owner=state)
            except BaseException as revoke_error:
                logger.warning(
                    "provider_authority_revoke_failed",
                    reason_code="provider_authority_revoke_failed",
                    error_type=type(revoke_error).__name__,
                    generation_epoch=state.generation_epoch,
                )
        stable_error = self._fence_generation(state, error)
        self._settle_committed_generation_submissions(state, stable_error)
        with self._lock:
            authority_unretired = bool(
                not state.cleanup_succeeded
                and (
                    state.retained_products
                    or self._llm_provider is not None
                    or self._context_provider is not None
                    or self._cleanup_pending
                )
            )
            quarantine = ProviderFailureQuarantine(
                generation_epoch=state.generation_epoch,
                reason_code="provider_generation_cleanup_failed",
                error_type=type(error).__name__[:128],
            )
            self._quarantine.append(quarantine)
            runtime_fatal = authority_unretired or bool(getattr(stable_error, "runtime_provider_fatal", False))
            if runtime_fatal:
                fatal_circuit = self._lifecycle.fence_runtime_fatal(stable_error)
                stable_error = self._fatal_circuit_error(fatal_circuit)
            state.terminal_error = stable_error
            state.active_operations.clear()
            state.active_handoffs.clear()
            self._active_leases.clear()
            self._lease_owners.clear()
            self._clear_generation_locked(state)
            if runtime_fatal:
                self._state = ProviderLifecycleState.REVOKED
            state.done.set()

    def _quarantined_epoch_locked(self, epoch: int) -> bool:
        return any(item.generation_epoch == epoch for item in self._quarantine)

    def _validate_release_handle_locked(self, handle: ProviderLeaseHandle) -> None:
        if handle.graph_nonce != self._graph_nonce:
            raise RuntimeOwnershipError("Provider lease belongs to another runtime graph")
        active = self._active_leases.get(handle.lease_id)
        if active == handle:
            return
        if self._quarantined_epoch_locked(handle.generation_epoch):
            raise RuntimeOwnershipError("Pipeline provider generation cleanup failed")
        if handle.generation_epoch < self._generation_epoch:
            raise RuntimeOwnershipError("Provider lease belongs to a prior provider generation")
        raise RuntimeOwnershipError("Provider lease is stale or duplicate")

    async def close(self, handle: ProviderLeaseHandle | None = None) -> None:
        installed = self._lifecycle.execution_graph.provider_manager()
        manager = self if installed is self else self._resolve_active_manager()
        if manager is not self:
            await manager.close(handle)
            return
        wait_state: _ProviderGenerationCloseState | None = None
        retired_state: _ProviderGenerationCloseState | None = None
        with self._lock:
            leases = self._task_leases.get()
            if handle is None and leases and leases[-1][1].matches_current():
                handle, _owner = leases[-1]
                self._task_leases.set(leases[:-1])
            elif handle is not None:
                self._task_leases.set(tuple(item for item in leases if item[0] != handle))

            if handle is not None:
                self._validate_release_handle_locked(handle)
                self._active_leases.pop(handle.lease_id, None)
                self._lease_owners.pop(handle.lease_id, None)

            abandoned = tuple(
                lease_id
                for lease_id in self._active_leases
                if (owner := self._lease_owners.get(lease_id)) is not None and owner.is_abandoned()
            )
            for lease_id in abandoned:
                self._active_leases.pop(lease_id, None)
                self._lease_owners.pop(lease_id, None)
            if abandoned:
                logger.warning(
                    "provider_generation_requesters_abandoned",
                    reason_code="provider_generation_requesters_abandoned",
                    abandoned_leases=len(abandoned),
                )
            if self._active_leases:
                return
            wait_state = self._generation_owner
            if wait_state is None:
                if handle is not None:
                    raise RuntimeOwnershipError("Provider lease lost its provider generation")
                if self._cleanup_pending and self._chained_cleanup is not None:
                    wait_state = self._start_generation_owner_locked()
                else:
                    retired_state = self._retired_generation
            if wait_state is not None and self._generation_owner is wait_state:
                # The no-lease observation and generation fence are one atomic
                # transition. A replacement acquire either wins before this
                # point and keeps the generation alive, or observes DRAINING
                # and waits for the next epoch.
                if self._state not in {ProviderLifecycleState.REVOKED, ProviderLifecycleState.DRAINING}:
                    self._state = ProviderLifecycleState.DRAINING
                    self._closing_event = wait_state

        if retired_state is not None:
            await self._join_retired_generation(retired_state)
        if wait_state is None:
            return

        self._begin_generation_retirement(wait_state)
        await self._wait_for_generation_close(wait_state)

    async def shutdown(self) -> None:
        """Revoke new leases and drain the runtime's current provider generation."""
        with self._lock:
            self._shutdown_requested = True
            self._shutdown_root_generation = self._lifecycle.execution_graph.root_generation
            self._active_leases.clear()
            self._lease_owners.clear()
            state = self._generation_owner
            retired_state = self._retired_generation
        if state is None:
            if retired_state is not None:
                await self._join_retired_generation(retired_state)
            self._raise_if_runtime_fatal()
            return
        self._begin_generation_retirement(state)
        await self._wait_for_generation_close(state)


def resolve_owned_database_path(
    *,
    boundary: str,
    database_role: str,
    owners: tuple[tuple[str, Any], ...],
    runtime_settings: Settings | None = None,
) -> Path:
    """Resolve one database through public ownership descriptors only."""
    owner_descriptors = tuple(get_runtime_ownership(owner, component=name) for name, owner in owners)
    if owner_descriptors:
        require_compatible_runtime_ownership(
            boundary=boundary,
            descriptors=owner_descriptors,
        )
    owner_paths: list[Path] = []
    for descriptor in owner_descriptors:
        matches = tuple(database.path for database in descriptor.databases if database.role == database_role)
        if len(matches) != 1:
            raise RuntimeOwnershipError(
                f"{boundary} {descriptor.component} must expose one {database_role} database identity"
            )
        owner_paths.append(matches[0])

    descriptors = list(owner_descriptors)
    if runtime_settings is not None:
        selected_settings = (
            snapshot_runtime_settings(
                runtime_settings,
                database_role=database_role,
                database_path=owner_paths[0],
            )
            if owner_paths
            else snapshot_runtime_settings(runtime_settings)
        )
        descriptors.insert(
            0,
            runtime_descriptor_from_settings(
                selected_settings,
                component=f"{boundary}_settings",
            ),
        )
    if not descriptors:
        raise RuntimeOwnershipError(f"{boundary} requires a runtime database owner")

    require_compatible_runtime_ownership(
        boundary=boundary,
        descriptors=tuple(descriptors),
    )
    paths: list[Path] = []
    for descriptor in descriptors:
        matches = tuple(database.path for database in descriptor.databases if database.role == database_role)
        if len(matches) != 1:
            raise RuntimeOwnershipError(
                f"{boundary} {descriptor.component} must expose one {database_role} database identity"
            )
        paths.append(matches[0])
    if len(set(paths)) != 1:
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"database"},
            tuple(descriptor.component for descriptor in descriptors),
            message=f"{boundary} persistence owners must use the same database",
        )
    return paths[0]


def create_scoped_knowledge_service(
    signal_store: Any,
    *,
    runtime_settings: Settings,
    history_store_factory: Callable[[], Any] | None = None,
    boundary: str = "Operational Knowledge service",
) -> Any:
    """Build Operational Knowledge beside one descriptor-owned signal store."""
    resolve_owned_database_path(
        boundary=boundary,
        database_role="signals",
        owners=(("signal_store", signal_store),),
        runtime_settings=runtime_settings,
    )
    from tacit.knowledge.service import KnowledgeService

    return KnowledgeService(
        signal_store=signal_store,
        history_store_factory=history_store_factory,
        runtime_settings=runtime_settings,
    )


class _DeferredRuntimeProviderResources:
    """Resolve a provider manager only after a closed graph is reopened."""

    def __init__(
        self,
        runtime_settings: Settings,
        *,
        lifecycle: PipelineAdmissionController,
        llm_factory: Callable[[], LLMProvider] | None,
        context_factory: Callable[[], ContextProvider | None] | None,
        cleanup_grace_seconds: float,
    ) -> None:
        self._settings = runtime_settings
        self._lifecycle = lifecycle
        self._llm_factory = llm_factory
        self._context_factory = context_factory
        self._cleanup_grace_seconds = cleanup_grace_seconds
        _RuntimeProviderResources._bind_lifecycle_identity(runtime_settings, lifecycle)
        lifecycle.raise_if_runtime_fatal()
        self._spec = _semantic_provider_spec(
            runtime_settings,
            llm_factory=llm_factory,
            context_factory=context_factory,
            chained_cleanup=None,
            cleanup_grace_seconds=cleanup_grace_seconds,
        )

    def _resolve(self) -> _RuntimeProviderResources:
        return _RuntimeProviderResources.resolve(
            self._settings,
            lifecycle=self._lifecycle,
            llm_factory=self._llm_factory,
            context_factory=self._context_factory,
            cleanup_grace_seconds=self._cleanup_grace_seconds,
            spec=self._spec,
        )

    def llm_ownership(self, *, component: str) -> RuntimeOwnershipDescriptor:
        return replace(self._spec.llm, component=component)

    def llm(self) -> LLMProvider:
        return self._resolve().llm()

    def context(self) -> ContextProvider | None:
        return self._resolve().context()

    async def acquire(self) -> ProviderLeaseHandle | None:
        return await self._resolve().acquire()

    async def close(self, handle: ProviderLeaseHandle | None = None) -> None:
        manager = self._resolve()
        if handle is None:
            await manager.close()
        else:
            await manager.close(handle)


@dataclass(frozen=True, slots=True)
class RuntimeRootUseHandle:
    """One request or adapter's borrow of a bounded composition authority."""

    _coordinator: _RuntimeRootCoordinator = field(repr=False)
    token: int = field(repr=False)
    generation: int

    def _mark_drain_startup_retry(self) -> None:
        """Mark the next release as the final caller-owned startup attempt."""
        self._coordinator.mark_drain_startup_retry(self)

    async def _transfer_drain_startup_exhaustion(self, error: BaseException) -> None:
        """Consume this borrow and transfer its root to durable recovery."""
        await self._coordinator.transfer_drain_startup_exhaustion(self, error)


class _RuntimeRootCoordinator:
    """Keep request volume from manufacturing composition-root owners."""

    def __init__(self, graph: Any) -> None:
        self._graph = weakref.ref(graph)
        self._lock = threading.RLock()
        self._next_token = 0
        self._uses: dict[int, bool] = {}
        self._owned_handle: RuntimeRootOwnerHandle | None = None
        self._owned_release: Callable[[RuntimeRootOwnerHandle], Awaitable[None]] | None = None
        self._release_in_progress = False
        self._deferred_release_registered = False
        self._startup_retry_tokens: set[int] = set()

    def mark_drain_startup_retry(self, handle: RuntimeRootUseHandle) -> None:
        """Identify the one retry whose startup failure requires handoff."""
        if handle._coordinator is not self:
            raise RuntimeOwnershipError("Pipeline runtime root use belongs to another coordinator")
        with self._lock:
            if handle.token not in self._uses:
                raise RuntimeOwnershipError("Pipeline runtime root use was already released")
            self._startup_retry_tokens.add(handle.token)

    def acquire(
        self,
        acquire_root: Callable[[], RuntimeRootOwnerHandle],
        release_root: Callable[[RuntimeRootOwnerHandle], Awaitable[None]],
        *,
        required_generation: int | None = None,
    ) -> RuntimeRootUseHandle:
        graph = self._graph()
        if graph is None:
            raise RuntimeOwnershipError("Pipeline runtime execution graph is unavailable")
        graph.admission.raise_if_runtime_fatal()
        with self._lock:
            if self._release_in_progress:
                raise RuntimeOwnershipError("Pipeline runtime composition root is shutting down")
            managed = self._owned_handle is not None
            if required_generation is not None:
                if (
                    graph.root_state != "active"
                    or graph.root_owner_count <= 0
                    or graph.root_generation != required_generation
                ):
                    raise RuntimeOwnershipError("Required runtime root generation is no longer active")
                managed = False
            elif not managed and graph.root_state != "active":
                if graph.root_state == "draining":
                    raise RuntimeOwnershipError("Pipeline runtime composition root is shutting down")
                owned_handle = acquire_root()
                self._owned_handle = owned_handle
                self._owned_release = release_root
                managed = True
            self._next_token += 1
            token = self._next_token
            self._uses[token] = managed
            return RuntimeRootUseHandle(
                _coordinator=self,
                token=token,
                generation=graph.root_generation,
            )

    def release_detached(self, handle: RuntimeRootUseHandle) -> None:
        """Transfer a discarded request's final root to graph-owned cleanup."""
        if handle._coordinator is not self:
            raise RuntimeOwnershipError("Pipeline runtime root use belongs to another coordinator")
        graph = self._graph()
        if graph is None:
            raise RuntimeOwnershipError("Pipeline runtime execution graph is unavailable")
        with self._lock:
            managed = self._uses.pop(handle.token, None)
            if managed is None:
                raise RuntimeOwnershipError("Pipeline runtime root use was already released")
            self._startup_retry_tokens.discard(handle.token)
            if not managed or any(self._uses.values()):
                return
            if self._owned_handle is None or self._owned_release is None:
                raise RuntimeOwnershipError("Pipeline runtime root authority is unavailable")
            if self._deferred_release_registered:
                return
            self._deferred_release_registered = True

        graph.admission.when_request_paths_idle(self._release_deferred_root_when_idle)

    async def release(
        self,
        handle: RuntimeRootUseHandle,
        *,
        wait_for_drain: bool = True,
    ) -> None:
        if not wait_for_drain:
            self.release_detached(handle)
            return
        if handle._coordinator is not self:
            raise RuntimeOwnershipError("Pipeline runtime root use belongs to another coordinator")
        graph = self._graph()
        if graph is None:
            raise RuntimeOwnershipError("Pipeline runtime execution graph is unavailable")
        with self._lock:
            managed = self._uses.pop(handle.token, None)
            if managed is None:
                raise RuntimeOwnershipError("Pipeline runtime root use was already released")
            startup_retry = handle.token in self._startup_retry_tokens
            if not managed:
                self._startup_retry_tokens.discard(handle.token)
                return
            if any(self._uses.values()):
                self._startup_retry_tokens.discard(handle.token)
                return
            owned_handle = self._owned_handle
            release_root = self._owned_release
            if owned_handle is None or release_root is None:
                raise RuntimeOwnershipError("Pipeline runtime root authority is unavailable")
            self._release_in_progress = True

        try:
            await release_root(owned_handle)
        except BaseException as exc:
            with self._lock:
                self._release_in_progress = False
                retryable_release_failure = (
                    isinstance(exc, (GeneratorExit, RuntimeRootDrainStartupError))
                    and graph is not None
                    and graph.root_state == "active"
                    and graph.root_owner_count > 0
                )
                if retryable_release_failure:
                    self._uses[handle.token] = managed
                    self._release_in_progress = startup_retry
                elif graph is None or graph.root_state != "active" or graph.root_owner_count == 0:
                    self._owned_handle = None
                    self._owned_release = None
                if not retryable_release_failure:
                    self._startup_retry_tokens.discard(handle.token)
            raise
        else:
            with self._lock:
                self._release_in_progress = False
                self._owned_handle = None
                self._owned_release = None
                self._startup_retry_tokens.discard(handle.token)

    def _release_deferred_root_when_idle(self) -> None:
        """Synchronously hand an idle retained-work generation to its graph."""
        graph = self._graph()
        if graph is None:
            return
        with self._lock:
            if not self._deferred_release_registered:
                return
            self._deferred_release_registered = False
            if any(self._uses.values()):
                return
            owned_handle = self._owned_handle
            if owned_handle is None:
                return
            self._release_in_progress = True

        release_error: BaseException | None = None
        try:
            for attempt in range(2):
                try:
                    graph.release_root_owner_detached(owned_handle)
                    release_error = None
                    break
                except RuntimeRootDrainStartupError as exc:
                    release_error = exc
                    if attempt == 0:
                        logger.warning(
                            "runtime_root_deferred_release_retry",
                            reason_code="lifecycle_owner_preflight_failed",
                            error_type=type(exc.__cause__ or exc).__name__,
                            root_generation=owned_handle.generation,
                        )
                        continue
                    break
                except BaseException as exc:
                    release_error = exc
                    break
            if release_error is not None and graph.root_state == "active" and graph.root_owner_count > 0:
                fence_runtime_root_after_transport_failure(owned_handle, release_error)
                logger.error(
                    "runtime_root_deferred_release_fenced",
                    reason_code="runtime_root_drain_startup_exhausted",
                    error_type=type(release_error).__name__,
                    root_generation=owned_handle.generation,
                )
        finally:
            with self._lock:
                self._release_in_progress = False
                if graph.root_state != "active" or graph.root_owner_count == 0:
                    self._owned_handle = None
                    self._owned_release = None

    async def transfer_drain_startup_exhaustion(
        self,
        handle: RuntimeRootUseHandle,
        error: BaseException,
    ) -> None:
        """Move an exhausted final borrow into execution-graph ownership."""
        if handle._coordinator is not self:
            raise RuntimeOwnershipError("Pipeline runtime root use belongs to another coordinator")
        with self._lock:
            managed = self._uses.pop(handle.token, None)
            if managed is None:
                raise RuntimeOwnershipError("Pipeline runtime root use was already released")
            if not managed:
                raise RuntimeOwnershipError("Pipeline runtime root use does not own recovery authority")
            if any(self._uses.values()):
                raise RuntimeOwnershipError("Pipeline runtime root still has active borrowers")
            if handle.token not in self._startup_retry_tokens:
                raise RuntimeOwnershipError("Pipeline runtime root recovery handoff was not marked")
            self._startup_retry_tokens.remove(handle.token)
            owned_handle = self._owned_handle
            if owned_handle is None:
                raise RuntimeOwnershipError("Pipeline runtime root authority is unavailable")
            self._release_in_progress = True

        transferred = False
        try:
            await owned_handle._transfer_drain_startup_exhaustion(error)
            transferred = True
        except BaseException as transfer_error:
            graph = self._graph()
            if graph is not None and graph.root_owner_count == 0 and graph.root_state != "active":
                transferred = True
            else:
                try:
                    fence_runtime_root_after_transport_failure(owned_handle, transfer_error)
                except BaseException:
                    with self._lock:
                        self._release_in_progress = False
                    raise
                transferred = True
            raise
        finally:
            with self._lock:
                self._release_in_progress = False
                if transferred:
                    self._owned_handle = None
                    self._owned_release = None


_RUNTIME_ROOT_COORDINATORS_LOCK = threading.Lock()
_RUNTIME_ROOT_COORDINATORS: weakref.WeakKeyDictionary[Any, _RuntimeRootCoordinator] = weakref.WeakKeyDictionary()


def _runtime_root_coordinator(admission: PipelineAdmissionController) -> _RuntimeRootCoordinator:
    graph = admission.execution_graph
    with _RUNTIME_ROOT_COORDINATORS_LOCK:
        coordinator = _RUNTIME_ROOT_COORDINATORS.get(graph)
        if coordinator is None:
            coordinator = _RuntimeRootCoordinator(graph)
            _RUNTIME_ROOT_COORDINATORS[graph] = coordinator
        return coordinator


def acquire_runtime_root_scope(stores: RuntimeStores) -> RuntimeRootUseHandle:
    """Acquire or borrow one runtime root for a standalone composition adapter."""
    admission = stores.pipeline_admission()
    return _runtime_root_coordinator(admission).acquire(
        stores.start_runtime_services,
        stores.shutdown_runtime_services,
    )


async def release_runtime_root_scope(
    handle: RuntimeRootUseHandle,
    *,
    wait_for_drain: bool = True,
) -> None:
    """Release a composition borrow without manufacturing another root owner."""
    await handle._coordinator.release(handle, wait_for_drain=wait_for_drain)


def release_runtime_root_scope_detached(handle: RuntimeRootUseHandle) -> None:
    """Transfer cleanup when the requesting coroutine cannot await release."""
    handle._coordinator.release_detached(handle)


@dataclass(frozen=True)
class _PinnedPipelineStoreFactories:
    history: Callable[[], Any]
    feedback: Callable[[], Any]
    signals: Callable[[], Any]
    knowledge: Callable[[], Any]


def _bind_pipeline_store_factories(
    *,
    runtime_stores: RuntimeStores,
    runtime_settings: Settings,
    history_store_factory: Callable[[], Any] | None,
    feedback_store_factory: Callable[[], Any] | None,
    signal_store_factory: Callable[[], Any] | None,
    knowledge_service_factory: Callable[[], Any] | None,
) -> _PinnedPipelineStoreFactories:
    """Bind one exact four-role store product set to a runtime composition root."""
    runtime_owner = get_runtime_ownership(runtime_stores, component="runtime_stores")
    default_history_store_factory = _declared_store_factory(
        runtime_stores._owned_history,
        runtime_settings=runtime_settings,
        expected=runtime_owner,
        role="history",
        component="runtime_history_store_factory",
    )
    resolved_history_store_factory = _validated_store_factory(
        history_store_factory or default_history_store_factory,
        expected=runtime_owner,
        role="history",
    )
    default_feedback_store_factory = _declared_store_factory(
        runtime_stores._owned_feedback,
        runtime_settings=runtime_settings,
        expected=runtime_owner,
        role="feedback",
        component="runtime_feedback_store_factory",
    )
    resolved_feedback_store_factory = _validated_store_factory(
        feedback_store_factory or default_feedback_store_factory,
        expected=runtime_owner,
        role="feedback",
    )
    default_signal_store_factory = _declared_store_factory(
        runtime_stores._owned_signals,
        runtime_settings=runtime_settings,
        expected=runtime_owner,
        role="signals",
        component="runtime_signal_store_factory",
    )
    resolved_signal_store_factory = _validated_store_factory(
        signal_store_factory or default_signal_store_factory,
        expected=runtime_owner,
        role="signals",
        allow_none=True,
    )
    validated_knowledge_service_factory = (
        _validated_store_factory(
            knowledge_service_factory,
            expected=runtime_owner,
            role="signals",
            factory_kind="knowledge:signals",
        )
        if knowledge_service_factory is not None
        else None
    )

    def runtime_knowledge_service() -> Any:
        if validated_knowledge_service_factory is not None:
            service = validated_knowledge_service_factory()
            signal_store = runtime_stores.pipeline_store("signals")
            resolve_owned_database_path(
                boundary="Pipeline knowledge service realization",
                database_role="signals",
                owners=(
                    ("signal_store", signal_store),
                    ("knowledge_service", service),
                ),
                runtime_settings=runtime_settings,
            )
            require_shared_signal_knowledge_admission(
                signal_store,
                service,
                runtime_settings=runtime_settings,
                boundary="Pipeline knowledge service realization",
            )
            return service
        if signal_store_factory is None and history_store_factory is None:
            return runtime_stores._owned_knowledge()
        signal_store = runtime_stores.pipeline_store("signals")
        resolve_owned_database_path(
            boundary="Pipeline signal and knowledge persistence",
            database_role="signals",
            owners=(("signal_store", signal_store),),
            runtime_settings=runtime_settings,
        )
        return create_scoped_knowledge_service(
            signal_store,
            history_store_factory=lambda: runtime_stores.pipeline_store("history"),
            runtime_settings=runtime_settings,
            boundary="Pipeline signal and knowledge persistence",
        )

    signals_path = next(item.path for item in runtime_owner.databases if item.role == "signals")
    declared_knowledge_service_factory = declare_runtime_factory(
        runtime_knowledge_service,
        ownership=runtime_descriptor_for_store(
            component="runtime_knowledge_service_factory",
            runtime_settings=runtime_settings,
            database_role="signals",
            database_path=signals_path,
        ),
        factory_kind="knowledge:signals",
    )
    selection_key: tuple[object, ...] = (
        "runtime:history" if history_store_factory is None else ("injected:history", id(history_store_factory)),
        "runtime:feedback" if feedback_store_factory is None else ("injected:feedback", id(feedback_store_factory)),
        "runtime:signals" if signal_store_factory is None else ("injected:signals", id(signal_store_factory)),
        (
            "runtime:knowledge"
            if knowledge_service_factory is None
            else ("injected:knowledge", id(knowledge_service_factory))
        ),
    )
    runtime_stores.bind_pipeline_store_factories(
        history=resolved_history_store_factory,
        feedback=resolved_feedback_store_factory,
        signals=resolved_signal_store_factory,
        knowledge=declared_knowledge_service_factory,
        selection_key=selection_key,
        uses_runtime_owned_products=all(
            factory is None
            for factory in (
                history_store_factory,
                feedback_store_factory,
                signal_store_factory,
                knowledge_service_factory,
            )
        ),
    )
    return _PinnedPipelineStoreFactories(
        history=_declared_store_factory(
            lambda: runtime_stores.pipeline_store("history"),
            runtime_settings=runtime_settings,
            expected=runtime_owner,
            role="history",
            component="runtime_pinned_history_store_factory",
        ),
        feedback=_declared_store_factory(
            lambda: runtime_stores.pipeline_store("feedback"),
            runtime_settings=runtime_settings,
            expected=runtime_owner,
            role="feedback",
            component="runtime_pinned_feedback_store_factory",
        ),
        signals=_declared_store_factory(
            lambda: runtime_stores.pipeline_store("signals"),
            runtime_settings=runtime_settings,
            expected=runtime_owner,
            role="signals",
            component="runtime_pinned_signal_store_factory",
        ),
        knowledge=declare_runtime_factory(
            lambda: runtime_stores.pipeline_store("knowledge"),
            ownership=runtime_descriptor_for_store(
                component="runtime_pinned_knowledge_service_factory",
                runtime_settings=runtime_settings,
                database_role="signals",
                database_path=signals_path,
            ),
            factory_kind="knowledge:signals",
        ),
    )


@dataclass(frozen=True)
class PipelineDependencies:
    settings: Settings
    backend_factory: Callable[[], list[DashboardBackend]]
    history_store_factory: Callable[[], Any]
    feedback_store_factory: Callable[[], Any]
    llm_cache: Any
    cache_key_factory: Callable[..., str]
    pipeline_admission: PipelineAdmissionController | None = None
    runtime_ownership: RuntimeOwnershipDescriptor | None = None
    signal_store_factory: Callable[[], Any] | None = None
    knowledge_service_factory: Callable[[], Any] | None = None
    llm_provider_factory: Callable[[], LLMProvider] | None = None
    context_provider_factory: Callable[[], ContextProvider | None] | None = None
    resource_acquire: Callable[[], Awaitable[ProviderLeaseHandle | None]] | None = None
    resource_cleanup: Callable[..., Awaitable[None]] | None = None
    runtime_root_acquire: Callable[[], RuntimeRootOwnerHandle] | None = None
    runtime_root_release: Callable[[RuntimeRootOwnerHandle], Awaitable[None]] | None = None
    provider_lifecycle_owner: object | None = field(default=None, repr=False, compare=False)
    runtime_root_owner: RuntimeStores | None = field(default=None, repr=False, compare=False)
    cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS
    required_runtime_root_generation: int | None = None
    _runtime_root_coordinator: _RuntimeRootCoordinator | None = field(
        init=False,
        repr=False,
        compare=False,
        default=None,
    )
    _backend_realizer: Callable[[], Awaitable[list[DashboardBackend]]] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        admission_value: object = self.pipeline_admission
        if not isinstance(admission_value, PipelineAdmissionController):
            raise ValueError(
                "PipelineDependencies requires a runtime-owned pipeline admission controller; "
                "use build_pipeline_dependencies() or PipelineDependencies.isolated()"
            )
        admission = admission_value
        admission.raise_if_runtime_fatal()
        object.__setattr__(
            self,
            "cleanup_grace_seconds",
            _validate_cleanup_grace_seconds(self.cleanup_grace_seconds),
        )
        if self.required_runtime_root_generation is not None and self.required_runtime_root_generation <= 0:
            raise ValueError("Required runtime root generation must be positive")
        limits = pipeline_admission_limits(self.settings)
        if admission.limit != limits.concurrent:
            raise ValueError("Pipeline admission limit must match runtime settings")
        if admission.max_queued != limits.queued:
            raise ValueError("Pipeline admission queue limit must match runtime settings")
        if admission.max_queued_per_partition != limits.queued_per_partition:
            raise ValueError("Pipeline admission partition queue limit must match runtime settings")
        if admission.max_in_flight_per_partition != limits.concurrent_per_partition:
            raise ValueError("Pipeline admission partition concurrency limit must match runtime settings")
        if self.llm_provider_factory is None:
            raise RuntimeOwnershipError("PipelineDependencies requires an explicit LLM provider factory")
        if self.context_provider_factory is None:
            raise RuntimeOwnershipError("PipelineDependencies requires an explicit context provider factory")
        if self.runtime_root_acquire is None or self.runtime_root_release is None:
            raise RuntimeOwnershipError("Pipeline runtime root lifecycle requires acquisition and release hooks")
        owner = self.runtime_ownership
        if owner is None:
            raise RuntimeOwnershipError(
                "PipelineDependencies requires an explicit runtime owner; "
                "use build_pipeline_dependencies() or PipelineDependencies.isolated()"
            )
        expected_settings = runtime_descriptor_from_settings(
            self.settings,
            component="pipeline_dependency_settings",
        )
        require_compatible_runtime_ownership(
            boundary="Pipeline dependency construction",
            descriptors=(expected_settings, owner),
        )
        root_owner = _require_lifecycle_callback_owner(
            boundary="Pipeline runtime root lifecycle owner",
            owner=self.runtime_root_owner,
            callbacks=(
                (self.runtime_root_acquire, "start_runtime_services"),
                (self.runtime_root_release, "shutdown_runtime_services"),
            ),
        )
        if not isinstance(root_owner, RuntimeStores):
            raise RuntimeOwnershipError("Pipeline runtime root lifecycle owner must be RuntimeStores")
        if root_owner.pipeline_admission() is not admission:
            raise RuntimeOwnershipError("Pipeline runtime root lifecycle owner uses another admission graph")
        require_compatible_runtime_ownership(
            boundary="Pipeline runtime root lifecycle owner",
            descriptors=(owner, root_owner.runtime_ownership),
        )
        object.__setattr__(
            self,
            "_runtime_root_coordinator",
            _runtime_root_coordinator(admission),
        )
        expected_backends = runtime_descriptor_for_backends(
            component="pipeline_backend_settings",
            runtime_settings=self.settings,
        )
        declared_backend_factory = self.backend_factory
        object.__setattr__(
            self,
            "backend_factory",
            _validated_backend_factory(
                declared_backend_factory,
                expected=expected_backends,
                lifecycle=admission,
                cleanup_grace_seconds=self.cleanup_grace_seconds,
            ),
        )
        object.__setattr__(
            self,
            "_backend_realizer",
            _validated_backend_realizer(
                declared_backend_factory,
                expected=expected_backends,
                lifecycle=admission,
                cleanup_grace_seconds=self.cleanup_grace_seconds,
            ),
        )
        object.__setattr__(
            self,
            "history_store_factory",
            _validated_store_factory(
                self.history_store_factory,
                expected=owner,
                role="history",
            ),
        )
        object.__setattr__(
            self,
            "feedback_store_factory",
            _validated_store_factory(
                self.feedback_store_factory,
                expected=owner,
                role="feedback",
            ),
        )
        if self.signal_store_factory is not None:
            object.__setattr__(
                self,
                "signal_store_factory",
                _validated_store_factory(
                    self.signal_store_factory,
                    expected=owner,
                    role="signals",
                    allow_none=True,
                ),
            )
        if self.knowledge_service_factory is not None:
            object.__setattr__(
                self,
                "knowledge_service_factory",
                _validated_store_factory(
                    self.knowledge_service_factory,
                    expected=owner,
                    role="signals",
                    factory_kind="knowledge:signals",
                ),
            )
        if self.resource_acquire is None or self.resource_cleanup is None:
            raise RuntimeOwnershipError("Pipeline provider lifecycle requires acquisition and cleanup hooks")
        provider_owner = _require_lifecycle_callback_owner(
            boundary="Pipeline provider lifecycle owner",
            owner=self.provider_lifecycle_owner,
            callbacks=(
                (self.resource_acquire, "acquire"),
                (self.resource_cleanup, "close"),
            ),
        )
        if getattr(provider_owner, "_lifecycle", None) is not admission:
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner uses another admission graph")
        if (
            _admitted_provider_lifecycle_owner(self.llm_provider_factory) is not provider_owner
            or _admitted_provider_lifecycle_owner(self.context_provider_factory) is not provider_owner
        ):
            raise RuntimeOwnershipError("Pipeline provider lifecycle owner does not own provider factories")
        expected_provider = _expected_provider_declaration(
            self.settings,
            self.llm_provider_factory,
        )
        expected_context_provider = runtime_descriptor_for_provider(
            component="pipeline_context_provider_settings",
            runtime_settings=self.settings,
            capability="context",
        )
        object.__setattr__(
            self,
            "llm_provider_factory",
            _validated_llm_provider_factory(
                self.llm_provider_factory,
                expected=expected_provider,
                lifecycle=admission,
                cleanup_grace_seconds=self.cleanup_grace_seconds,
            ),
        )
        object.__setattr__(
            self,
            "context_provider_factory",
            _validated_context_provider_factory(
                self.context_provider_factory,
                expected=expected_context_provider,
                context_disabled=str(self.settings.context_provider or "").strip().casefold() in {"", "none"},
                lifecycle=admission,
                cleanup_grace_seconds=self.cleanup_grace_seconds,
            ),
        )

    @classmethod
    def isolated(cls, **values: Any) -> PipelineDependencies:
        """Construct a deliberately isolated dependency graph for tests or one-off embeddings."""
        runtime_settings = values.get("settings")
        if runtime_settings is None:
            raise TypeError("isolated pipeline dependencies require runtime settings")
        if not isinstance(runtime_settings, Settings):
            raise TypeError("isolated pipeline dependencies require validated Settings")
        if values.get("pipeline_admission") is not None:
            raise ValueError("isolated pipeline dependencies own their admission controller")
        if values.get("runtime_root_acquire") is not None or values.get("runtime_root_release") is not None:
            raise ValueError("isolated pipeline dependencies own their runtime root lifecycle")
        if values.get("runtime_root_owner") is not None or values.get("provider_lifecycle_owner") is not None:
            raise ValueError("isolated pipeline dependencies own their lifecycle capabilities")
        limits = pipeline_admission_limits(runtime_settings)
        pipeline_admission = PipelineAdmissionController(
            limits.concurrent,
            max_queued=limits.queued,
            max_queued_per_partition=limits.queued_per_partition,
            max_in_flight_per_partition=limits.concurrent_per_partition,
        )
        runtime_stores = RuntimeStores(
            runtime_settings,
            pipeline_admission=pipeline_admission,
        )
        has_signal_store_factory = values.get("signal_store_factory") is not None
        has_knowledge_service_factory = values.get("knowledge_service_factory") is not None
        pinned_stores = _bind_pipeline_store_factories(
            runtime_stores=runtime_stores,
            runtime_settings=runtime_settings,
            history_store_factory=values.get("history_store_factory"),
            feedback_store_factory=values.get("feedback_store_factory"),
            signal_store_factory=values.get("signal_store_factory"),
            knowledge_service_factory=values.get("knowledge_service_factory"),
        )
        values["history_store_factory"] = pinned_stores.history
        values["feedback_store_factory"] = pinned_stores.feedback
        values["signal_store_factory"] = pinned_stores.signals if has_signal_store_factory else None
        values["knowledge_service_factory"] = pinned_stores.knowledge if has_knowledge_service_factory else None
        values["pipeline_admission"] = pipeline_admission
        values["runtime_root_acquire"] = runtime_stores.start_runtime_services
        values["runtime_root_release"] = runtime_stores.shutdown_runtime_services
        values["runtime_root_owner"] = runtime_stores
        if values.get("runtime_ownership") is not None:
            raise ValueError("isolated pipeline dependencies own their runtime identity")
        values["runtime_ownership"] = runtime_descriptor_from_settings(
            runtime_settings,
            component="isolated_pipeline_dependencies",
        )
        provider_resources = _RuntimeProviderResources.resolve(
            runtime_settings,
            lifecycle=values["pipeline_admission"],
            llm_factory=values.get("llm_provider_factory"),
            context_factory=values.get("context_provider_factory"),
            chained_cleanup=values.get("resource_cleanup"),
            cleanup_grace_seconds=values.get(
                "cleanup_grace_seconds",
                DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
            ),
        )
        values["llm_provider_factory"] = declare_runtime_factory(
            _AdmittedProviderAccessor(provider_resources.llm),
            ownership=provider_resources.llm_ownership(
                component="isolated_llm_resource_factory",
            ),
            factory_kind="provider:llm",
        )
        values["context_provider_factory"] = declare_runtime_factory(
            _AdmittedProviderAccessor(provider_resources.context),
            ownership=runtime_descriptor_for_provider(
                component="isolated_context_resource_factory",
                runtime_settings=runtime_settings,
                capability="context",
            ),
            factory_kind="provider:context",
        )
        values["resource_acquire"] = provider_resources.acquire
        values["resource_cleanup"] = provider_resources.close
        values["provider_lifecycle_owner"] = provider_resources
        return cls(**values)

    async def acquire_resources(self) -> ProviderLeaseHandle | None:
        """Lease shared resources to the current pipeline run."""
        if self.resource_acquire is not None:
            return await self.resource_acquire()
        return None

    async def realize_backends(self) -> list[DashboardBackend]:
        """Construct and validate dashboard backends under runtime admission."""
        return await self._backend_realizer()

    async def close_resources(self, handle: ProviderLeaseHandle | None = None) -> None:
        """Close resources owned by this dependency bundle."""
        if self.resource_cleanup is not None:
            if handle is None:
                await self.resource_cleanup()
            else:
                await self.resource_cleanup(handle)

    def start_runtime_root(self) -> RuntimeRootUseHandle | None:
        """Acquire or borrow the production composition authority."""
        if self.runtime_root_acquire is None or self.runtime_root_release is None:
            return None
        coordinator = self._runtime_root_coordinator
        if coordinator is None:
            raise RuntimeOwnershipError("Pipeline runtime root coordinator is unavailable")
        return coordinator.acquire(
            self.runtime_root_acquire,
            self.runtime_root_release,
            required_generation=self.required_runtime_root_generation,
        )

    async def stop_runtime_root(
        self,
        handle: RuntimeRootUseHandle | None,
        *,
        wait_for_drain: bool = True,
    ) -> None:
        """Release this request's composition borrow."""
        if handle is None:
            return
        await release_runtime_root_scope(handle, wait_for_drain=wait_for_drain)

    def stop_runtime_root_detached(self, handle: RuntimeRootUseHandle | None) -> None:
        """Transfer a discarded request's root to the existing lifecycle owner."""
        if handle is None:
            return
        release_runtime_root_scope_detached(handle)

    @classmethod
    def defaults(cls) -> PipelineDependencies:
        from tacit.history import get_investigation_store

        stores = get_process_runtime_stores(
            settings,
            history_fallback=get_investigation_store,
        )
        return build_pipeline_dependencies(settings, stores=stores)


def build_pipeline_dependencies(
    runtime_settings: Settings,
    *,
    stores: RuntimeStores | None = None,
    backend_factory: Callable[[], list[DashboardBackend]] | None = None,
    history_store_factory: Callable[[], Any] | None = None,
    feedback_store_factory: Callable[[], Any] | None = None,
    signal_store_factory: Callable[[], Any] | None = None,
    knowledge_service_factory: Callable[[], Any] | None = None,
    llm_provider_factory: Callable[[], LLMProvider] | None = None,
    context_provider_factory: Callable[[], ContextProvider | None] | None = None,
    cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
    required_runtime_root_generation: int | None = None,
) -> PipelineDependencies:
    """Build a dependency bundle scoped to one runtime settings object."""

    if stores is None:
        raise RuntimeOwnershipError(
            "Production pipeline dependencies require an explicit RuntimeStores owner; "
            "use PipelineDependencies.isolated() for an isolated graph"
        )
    stores_owner = describe_runtime_owner("runtime_stores", stores)
    if stores_owner.settings is None:
        raise RuntimeOwnershipError("Pipeline runtime stores must expose their runtime settings")
    resolved_settings = resolve_runtime_settings(
        boundary="Pipeline dependencies",
        explicit_settings=runtime_settings,
        owners=(stores_owner,),
        fallback_settings=runtime_settings,
    )
    runtime_stores = stores
    runtime_owner = get_runtime_ownership(runtime_stores, component="runtime_stores")
    require_compatible_runtime_ownership(
        boundary="Pipeline dependency construction",
        descriptors=(
            runtime_descriptor_from_settings(
                resolved_settings,
                component="pipeline_dependency_settings",
            ),
            runtime_owner,
        ),
    )
    resolved_pipeline_admission = runtime_stores.pipeline_admission()
    resolved_pipeline_admission.raise_if_runtime_fatal()
    pinned_stores = _bind_pipeline_store_factories(
        runtime_stores=runtime_stores,
        runtime_settings=resolved_settings,
        history_store_factory=history_store_factory,
        feedback_store_factory=feedback_store_factory,
        signal_store_factory=signal_store_factory,
        knowledge_service_factory=knowledge_service_factory,
    )

    def runtime_backends() -> list[DashboardBackend]:
        from tacit import backends

        return backends.get_active_backends(resolved_settings)

    default_backend_factory = declare_backend_factory(
        runtime_backends,
        runtime_settings=resolved_settings,
        component="runtime_backend_factory",
    )
    resolved_backend_factory = backend_factory or default_backend_factory

    graph = resolved_pipeline_admission.execution_graph
    if graph.root_state == "closed":
        provider_resources: _RuntimeProviderResources | _DeferredRuntimeProviderResources = (
            _DeferredRuntimeProviderResources(
                resolved_settings,
                lifecycle=resolved_pipeline_admission,
                llm_factory=llm_provider_factory,
                context_factory=context_provider_factory,
                cleanup_grace_seconds=cleanup_grace_seconds,
            )
        )
    else:
        try:
            provider_resources = _RuntimeProviderResources.resolve(
                resolved_settings,
                lifecycle=resolved_pipeline_admission,
                llm_factory=llm_provider_factory,
                context_factory=context_provider_factory,
                cleanup_grace_seconds=cleanup_grace_seconds,
            )
        except RuntimeOwnershipError:
            if graph.root_state != "closed":
                raise
            provider_resources = _DeferredRuntimeProviderResources(
                resolved_settings,
                lifecycle=resolved_pipeline_admission,
                llm_factory=llm_provider_factory,
                context_factory=context_provider_factory,
                cleanup_grace_seconds=cleanup_grace_seconds,
            )
    declared_llm_resource_factory = declare_runtime_factory(
        _AdmittedProviderAccessor(provider_resources.llm),
        ownership=provider_resources.llm_ownership(
            component="runtime_llm_resource_factory",
        ),
        factory_kind="provider:llm",
    )
    declared_context_resource_factory = declare_runtime_factory(
        _AdmittedProviderAccessor(provider_resources.context),
        ownership=runtime_descriptor_for_provider(
            component="runtime_context_resource_factory",
            runtime_settings=resolved_settings,
            capability="context",
        ),
        factory_kind="provider:context",
    )

    return PipelineDependencies(
        settings=resolved_settings,
        backend_factory=resolved_backend_factory,
        history_store_factory=pinned_stores.history,
        feedback_store_factory=pinned_stores.feedback,
        llm_cache=runtime_stores.llm_cache(),
        cache_key_factory=make_cache_key,
        pipeline_admission=resolved_pipeline_admission,
        runtime_ownership=runtime_owner,
        signal_store_factory=pinned_stores.signals,
        knowledge_service_factory=pinned_stores.knowledge,
        llm_provider_factory=declared_llm_resource_factory,
        context_provider_factory=declared_context_resource_factory,
        resource_acquire=provider_resources.acquire,
        resource_cleanup=provider_resources.close,
        runtime_root_acquire=runtime_stores.start_runtime_services,
        runtime_root_release=runtime_stores.shutdown_runtime_services,
        provider_lifecycle_owner=provider_resources,
        runtime_root_owner=runtime_stores,
        cleanup_grace_seconds=cleanup_grace_seconds,
        required_runtime_root_generation=required_runtime_root_generation,
    )


@contextmanager
def managed_nonpipeline_llm_provider(
    runtime_settings: Settings,
    *,
    runtime_stores: RuntimeStores | None = None,
    dependencies: PipelineDependencies | None = None,
) -> Iterator[LLMProvider]:
    """Lease one LLM provider through the shared runtime lifecycle owner.

    Synchronous adapters use this boundary to construct, operate, and retire a
    provider without making their caller-owned event loop an authority.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeOwnershipError("Non-pipeline LLM composition must begin outside a running event loop")

    if runtime_stores is not None and dependencies is not None:
        raise RuntimeOwnershipError("Non-pipeline LLM composition accepts one runtime owner")

    selected_dependencies = dependencies
    if selected_dependencies is None:
        selected_stores = runtime_stores or RuntimeStores(runtime_settings)
        selected_dependencies = build_pipeline_dependencies(
            runtime_settings,
            stores=selected_stores,
        )
    else:
        require_compatible_runtime_ownership(
            boundary="Non-pipeline LLM lifecycle owner",
            descriptors=(
                runtime_descriptor_from_settings(
                    runtime_settings,
                    component="nonpipeline_llm_settings",
                ),
                runtime_descriptor_from_settings(
                    selected_dependencies.settings,
                    component="nonpipeline_llm_dependencies",
                ),
            ),
        )

    root_handle = selected_dependencies.start_runtime_root()
    if root_handle is None:
        raise RuntimeOwnershipError("Non-pipeline LLM runtime root is unavailable")

    provider_lease: ProviderLeaseHandle | None = None
    acquire_started = False
    try:
        acquire_started = True
        provider_lease = asyncio.run(selected_dependencies.acquire_resources())
        provider_factory = selected_dependencies.llm_provider_factory
        if provider_factory is None:
            raise RuntimeOwnershipError("Non-pipeline LLM provider factory is unavailable")
        yield provider_factory()
    finally:

        async def release_owned_resources() -> None:
            provider_error: BaseException | None = None
            root_error: BaseException | None = None
            try:
                if acquire_started:
                    await selected_dependencies.close_resources(provider_lease)
            except BaseException as exc:
                provider_error = exc
            try:
                await release_runtime_root_with_startup_retry(
                    selected_dependencies.stop_runtime_root,
                    root_handle,
                )
            except BaseException as exc:
                root_error = exc

            if provider_error is not None:
                if root_error is not None:
                    provider_error.add_note(f"Runtime root release also failed ({type(root_error).__name__})")
                raise provider_error
            if root_error is not None:
                raise root_error

        asyncio.run(release_owned_resources())


def get_default_dependencies() -> PipelineDependencies:
    """Return the production dependency bundle."""
    return PipelineDependencies.defaults()


def resolve_knowledge_service(
    deps: PipelineDependencies,
    *,
    signal_store: Any | None = None,
) -> Any:
    """Resolve Operational Knowledge from the active runtime's signal database."""
    if deps.knowledge_service_factory is not None:
        service = deps.knowledge_service_factory()
        owners: list[tuple[str, Any]] = [("knowledge_service", service)]
        if signal_store is not None:
            owners.append(("signal_store", signal_store))
        resolve_owned_database_path(
            boundary="Pipeline knowledge service resolution",
            database_role="signals",
            owners=tuple(owners),
            runtime_settings=deps.settings,
        )
        if signal_store is not None:
            require_shared_signal_knowledge_admission(
                signal_store,
                service,
                runtime_settings=deps.settings,
                boundary="Pipeline knowledge service resolution",
            )
        return service
    active_signal_store = signal_store
    if active_signal_store is None and deps.signal_store_factory is not None:
        active_signal_store = deps.signal_store_factory()
    if active_signal_store is None:
        logger.error("knowledge_service_scoped_store_unavailable", signal_store_type="none")
        raise RuntimeError("Operational Knowledge service is unavailable for the active signal store")
    return create_scoped_knowledge_service(
        active_signal_store,
        history_store_factory=deps.history_store_factory,
        runtime_settings=deps.settings,
        boundary="Pipeline signal and knowledge persistence",
    )
