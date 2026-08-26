"""Application dependency containers."""

from __future__ import annotations

import asyncio
import contextvars
import threading
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import structlog

from tacit.agents.providers.base import LLMProvider
from tacit.backends.base import DashboardBackend
from tacit.cache import make_cache_key
from tacit.config import Settings, settings
from tacit.context.base import ContextProvider
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController, pipeline_admission_limits
from tacit.runtime_ownership import (
    DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS,
    BedrockCredentialIdentity,
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
    realize_runtime_factory_async,
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
from tacit.runtime_stores import RuntimeStores, get_process_runtime_stores

logger = structlog.get_logger()
DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS = DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS
_validate_cleanup_grace_seconds = validate_runtime_cleanup_grace_seconds


def _consume_cleanup_task(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except BaseException:
        return


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
) -> None:
    """Transfer rejected realized products to the runtime cleanup owner."""
    lifecycle.close_rejected_resources(
        products,
        grace_seconds=cleanup_grace_seconds,
        reason_code=reason_code,
    )


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
) -> LLMProvider:
    """Admit one realized provider, including a frozen Bedrock snapshot."""
    factory_kind = f"provider:{capability}"
    if not isinstance(provider, LLMProvider):
        _cleanup_rejected_products(
            lifecycle,
            (provider,),
            cleanup_grace_seconds=cleanup_grace_seconds,
            reason_code="runtime_factory_realization_invalid",
        )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_invalid",
        )
        raise RuntimeOwnershipError(f"Pipeline {capability} provider factory returned an invalid provider")
    try:
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
            comparison = (
                realized_declaration if expected.remotes == realized_declaration.remotes else planned_declaration
            )
            require_compatible_runtime_ownership(
                boundary=f"Pipeline {capability} provider credential plan",
                descriptors=(expected, comparison),
            )
            expected = realized_declaration
        missing_dimensions: set[str] = set()
        if actual.settings_identity is None:
            missing_dimensions.add("settings")
        if actual.tenant_policy is None:
            missing_dimensions.update(("tenant", "permission"))
        compatible_actual = actual
        expected_remotes = {remote.provider: remote for remote in expected.remotes}
        actual_remotes = {remote.provider: remote for remote in actual.remotes}
        bedrock_remote_names = {"llm:bedrock", "llm:bedrock:sts"}
        if "llm:bedrock" in expected_remotes and set(expected_remotes) <= bedrock_remote_names:
            if set(actual_remotes) != set(expected_remotes):
                missing_dimensions.add("remote")
            for remote_name, expected_remote in expected_remotes.items():
                actual_remote = actual_remotes.get(remote_name)
                if actual_remote is None:
                    continue
                if actual_remote.endpoint != expected_remote.endpoint:
                    missing_dimensions.add("endpoint")
                selector_refines = expected_remote.account == "default-chain" or expected_remote.account.startswith(
                    "profile:"
                )
                if not selector_refines and actual_remote.account != expected_remote.account:
                    missing_dimensions.add("account")
                if actual_remote.credential_fingerprint == "none":
                    missing_dimensions.add("credential")
            compatible_actual = replace(actual, remotes=expected.remotes)
        elif actual.remotes != expected.remotes:
            missing_dimensions.add("remote")
        if missing_dimensions:
            raise RuntimeOwnershipMismatchError(
                f"Pipeline {capability} provider realization",
                missing_dimensions,
                (expected.component, actual.component),
            )
        require_compatible_runtime_ownership(
            boundary=f"Pipeline {capability} provider realization",
            descriptors=(expected, compatible_actual),
        )
    except RuntimeOwnershipMismatchError as exc:
        _cleanup_rejected_products(
            lifecycle,
            (provider,),
            cleanup_grace_seconds=cleanup_grace_seconds,
            reason_code="runtime_factory_realization_mismatch",
        )
        observe_runtime_factory_failure(
            phase="realization",
            factory_kind=factory_kind,
            reason_code="runtime_factory_realization_mismatch",
            dimensions=exc.dimensions,
        )
        raise
    except RuntimeOwnershipError:
        _cleanup_rejected_products(
            lifecycle,
            (provider,),
            cleanup_grace_seconds=cleanup_grace_seconds,
            reason_code="runtime_factory_realization_owner_missing",
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

    def realize() -> LLMProvider:
        with observe_runtime_factory_realization(factory_kind):
            provider = factory()
        return _validate_llm_provider_product(
            provider,
            expected=expected,
            lifecycle=lifecycle,
            cleanup_grace_seconds=cleanup_grace_seconds,
            capability=capability,
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

    def realize() -> ContextProvider | None:
        with observe_runtime_factory_realization(factory_kind):
            provider = factory()
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
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_invalid",
            )
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_invalid",
            )
            raise RuntimeOwnershipError("Pipeline context provider must remain disabled for this runtime")
        if not isinstance(provider, ContextProvider):
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_invalid",
            )
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_invalid",
            )
            raise RuntimeOwnershipError("Pipeline context provider factory returned an invalid provider")
        try:
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
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_mismatch",
            )
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_mismatch",
                dimensions=exc.dimensions,
            )
            raise
        except RuntimeOwnershipError:
            _cleanup_rejected_products(
                lifecycle,
                (provider,),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_owner_missing",
            )
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_owner_missing",
            )
            raise
        return provider

    return declare_runtime_factory(realize, ownership=declared, factory_kind=factory_kind)


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

    def realize() -> list[DashboardBackend]:
        with observe_runtime_factory_realization(factory_kind):
            backends = factory()
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
            _cleanup_rejected_products(
                lifecycle,
                tuple(backends),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_mismatch",
            )
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_mismatch",
                dimensions=exc.dimensions,
            )
            raise _BackendFactoryRealizationError(backends) from exc
        except RuntimeOwnershipError as exc:
            _cleanup_rejected_products(
                lifecycle,
                tuple(backends),
                cleanup_grace_seconds=cleanup_grace_seconds,
                reason_code="runtime_factory_realization_owner_missing",
            )
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_owner_missing",
            )
            raise _BackendFactoryRealizationError(backends) from exc
        return backends

    return declare_runtime_factory(realize, ownership=declared, factory_kind=factory_kind)


class _RuntimeProviderResources:
    """Reference-count providers shared by active runs in one runtime."""

    def __init__(
        self,
        runtime_settings: Settings,
        *,
        lifecycle: PipelineAdmissionController,
        llm_factory: Callable[[], LLMProvider] | None = None,
        context_factory: Callable[[], ContextProvider | None] | None = None,
        chained_cleanup: Callable[[], Awaitable[None]] | None = None,
        cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS,
    ) -> None:
        self._settings = runtime_settings
        self._lifecycle = lifecycle
        self._cleanup_grace_seconds = _validate_cleanup_grace_seconds(cleanup_grace_seconds)
        self._requires_async_llm_realization = str(runtime_settings.llm_provider).strip().casefold() == "bedrock"
        self._bedrock_credential_plan = (
            BedrockCredentialPlan.capture(runtime_settings)
            if self._requires_async_llm_realization and llm_factory is None
            else None
        )
        expected = (
            self._bedrock_credential_plan.ownership(component="pipeline_provider_settings")
            if self._bedrock_credential_plan is not None
            else runtime_descriptor_for_provider(
                component="pipeline_provider_settings",
                runtime_settings=runtime_settings,
                capability="llm",
            )
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
        self._context_factory = _validated_context_provider_factory(
            selected_context_factory,
            expected=expected_context,
            context_disabled=context_disabled,
            lifecycle=lifecycle,
            cleanup_grace_seconds=self._cleanup_grace_seconds,
        )
        self._chained_cleanup = chained_cleanup
        self._llm_provider: LLMProvider | None = None
        self._context_provider: ContextProvider | None = None
        self._context_initialized = False
        self._lock = threading.RLock()
        self._active_leases: set[int] = set()
        self._next_lease = 1
        self._cleanup_pending = chained_cleanup is not None
        self._closing_event: threading.Event | None = None
        self._llm_initializing: threading.Event | None = None
        self._task_leases: contextvars.ContextVar[tuple[tuple[int, tuple[int, int | None]], ...]] = (
            contextvars.ContextVar(
                f"tacit_provider_leases_{id(self)}",
                default=(),
            )
        )

    @staticmethod
    def _lease_owner() -> tuple[int, int | None]:
        try:
            task = asyncio.current_task()
        except RuntimeError:
            task = None
        return threading.get_ident(), id(task) if task is not None else None

    async def acquire(self) -> None:
        """Lease the provider lifecycle after the prior generation closes."""
        while True:
            with self._lock:
                closing_event = self._closing_event
                if closing_event is None:
                    lease = self._next_lease
                    self._next_lease += 1
                    self._active_leases.add(lease)
                    self._cleanup_pending = True
                    break
            if not closing_event.is_set():
                closed = await asyncio.to_thread(
                    closing_event.wait,
                    self._cleanup_grace_seconds,
                )
                if not closed:
                    raise RuntimeOwnershipError("Pipeline provider resources are still closing after cleanup grace")
        try:
            await self._ensure_llm_provider()
        except BaseException:
            with self._lock:
                self._active_leases.discard(lease)
            raise
        self._task_leases.set((*self._task_leases.get(), (lease, self._lease_owner())))

    async def _ensure_llm_provider(self) -> None:
        while True:
            initializer = False
            with self._lock:
                if self._llm_provider is not None:
                    return
                initializing = self._llm_initializing
                if initializing is None:
                    initializing = threading.Event()
                    self._llm_initializing = initializing
                    initializer = True
            if not initializer:
                while not initializing.is_set():
                    await asyncio.sleep(0)
                continue

            realization_task = asyncio.create_task(
                self._realize_llm_provider(),
                name="tacit-llm-provider-realization",
            )
            try:
                provider = await asyncio.shield(realization_task)
            except asyncio.CancelledError:
                retirement = asyncio.create_task(
                    self._retire_late_provider(realization_task, initializing),
                    name="tacit-late-provider-retirement",
                )
                if not self._lifecycle.retain_current_task(retirement):
                    retirement.add_done_callback(_consume_cleanup_task)
                raise
            except BaseException:
                self._finish_llm_initialization(initializing)
                raise

            with self._lock:
                self._llm_provider = provider
                self._cleanup_pending = True
            self._finish_llm_initialization(initializing)
            return

    async def _realize_llm_provider(self) -> LLMProvider:
        with observe_runtime_factory_realization("provider:llm"):
            provider = await realize_runtime_factory_async(self._llm_factory)
        expected = self._llm_expected
        if self._bedrock_credential_plan is not None:
            credential_identity = getattr(provider, "bedrock_credential_identity", None)
            if isinstance(credential_identity, BedrockCredentialIdentity):
                expected = self._bedrock_credential_plan.ownership(
                    component="pipeline_provider_settings",
                    credential_identity=credential_identity,
                )
        admitted = _validate_llm_provider_product(
            provider,
            expected=expected,
            lifecycle=self._lifecycle,
            cleanup_grace_seconds=self._cleanup_grace_seconds,
        )
        self._llm_expected = expected
        return admitted

    def llm_ownership(self, *, component: str) -> RuntimeOwnershipDescriptor:
        """Return the frozen provider-plan declaration for resource wrappers."""
        return replace(self._llm_expected, component=component)

    async def _retire_late_provider(
        self,
        realization_task: asyncio.Task[LLMProvider],
        initializing: threading.Event,
    ) -> None:
        try:
            provider = await realization_task
        except BaseException:
            return
        else:
            _cleanup_rejected_products(
                self._lifecycle,
                (provider,),
                cleanup_grace_seconds=self._cleanup_grace_seconds,
                reason_code="provider_realization_lifecycle_expired",
            )
        finally:
            self._finish_llm_initialization(initializing)

    def _finish_llm_initialization(self, initializing: threading.Event) -> None:
        with self._lock:
            initializing.set()
            if self._llm_initializing is initializing:
                self._llm_initializing = None

    def llm(self) -> LLMProvider:
        with self._lock:
            if self._closing_event is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            if self._llm_provider is None:
                if self._requires_async_llm_realization:
                    raise RuntimeOwnershipError("Pipeline LLM provider resources were not acquired")
                if self._llm_initializing is not None:
                    raise RuntimeOwnershipError("Pipeline LLM provider resources are being acquired")
                with observe_runtime_factory_realization("provider:llm"):
                    provider = self._llm_factory()
                self._llm_provider = _validate_llm_provider_product(
                    provider,
                    expected=self._llm_expected,
                    lifecycle=self._lifecycle,
                    cleanup_grace_seconds=self._cleanup_grace_seconds,
                )
                self._cleanup_pending = True
            return self._llm_provider

    def context(self) -> ContextProvider | None:
        with self._lock:
            if self._closing_event is not None:
                raise RuntimeOwnershipError("Pipeline provider resources are closing")
            if not self._context_initialized:
                self._context_provider = self._context_factory()
                self._context_initialized = True
                self._cleanup_pending = True
            return self._context_provider

    async def close(self) -> None:
        context_provider: ContextProvider | None = None
        llm_provider: LLMProvider | None = None
        chained_cleanup: Callable[[], Awaitable[None]] | None = None
        close_event: threading.Event | None = None
        wait_event: threading.Event | None = None
        leases = self._task_leases.get()
        lease_owner = self._lease_owner()
        with self._lock:
            if leases and leases[-1][1] == lease_owner:
                lease, _owner = leases[-1]
                self._task_leases.set(leases[:-1])
                self._active_leases.discard(lease)
            elif self._active_leases:
                # An unrelated cleanup request must not consume another task's
                # run lease or close resources while that run is active.
                return
            if self._active_leases:
                return
            if self._closing_event is not None:
                wait_event = self._closing_event
            elif not self._context_initialized and self._llm_provider is None and not self._cleanup_pending:
                return
            else:
                close_event = threading.Event()
                self._closing_event = close_event
            if wait_event is not None:
                context_provider = None
                llm_provider = None
                chained_cleanup = None
            else:
                if self._context_initialized:
                    context_provider = self._context_provider
                    self._context_provider = None
                    self._context_initialized = False
                llm_provider = self._llm_provider
                self._llm_provider = None
                if self._cleanup_pending:
                    chained_cleanup = self._chained_cleanup
                    self._cleanup_pending = False

        if wait_event is not None:
            if not wait_event.is_set():
                await asyncio.to_thread(
                    wait_event.wait,
                    self._cleanup_grace_seconds,
                )
            return

        async def cleanup_generation() -> None:
            async def close_context() -> None:
                try:
                    assert context_provider is not None
                    await context_provider.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("context_provider_close_failed", exc_info=True)

            async def close_llm() -> None:
                try:
                    assert llm_provider is not None
                    await llm_provider.close()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("llm_provider_close_failed", exc_info=True)

            async def close_chained() -> None:
                try:
                    assert chained_cleanup is not None
                    await chained_cleanup()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("pipeline_resource_cleanup_failed", exc_info=True)

            tasks: list[Awaitable[None]] = []
            if context_provider is not None:
                tasks.append(close_context())
            if llm_provider is not None:
                tasks.append(close_llm())
            if chained_cleanup is not None:
                tasks.append(close_chained())
            try:
                if tasks:
                    await asyncio.gather(*tasks)
            finally:
                assert close_event is not None
                with self._lock:
                    close_event.set()
                    if self._closing_event is close_event:
                        self._closing_event = None

        cleanup_task = asyncio.create_task(
            cleanup_generation(),
            name="tacit-provider-generation-cleanup",
        )
        done, _pending = await asyncio.wait(
            {cleanup_task},
            timeout=self._cleanup_grace_seconds,
        )
        if cleanup_task in done:
            try:
                cleanup_task.result()
            except asyncio.CancelledError:
                return
            return
        cleanup_task.cancel()
        retained = self._lifecycle.retain_current_task(cleanup_task)
        if retained:
            assert close_event is not None
            with self._lock:
                close_event.set()
                if self._closing_event is close_event:
                    self._closing_event = None
        else:
            cleanup_task.add_done_callback(_consume_cleanup_task)
        logger.warning(
            "provider_cleanup_grace_exceeded",
            reason_code="provider_cleanup_grace_exceeded",
            cleanup_grace_seconds=self._cleanup_grace_seconds,
        )


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
    database_path = resolve_owned_database_path(
        boundary=boundary,
        database_role="signals",
        owners=(("signal_store", signal_store),),
        runtime_settings=runtime_settings,
    )
    from tacit.knowledge.repository import KnowledgeRepository
    from tacit.knowledge.service import KnowledgeService

    return KnowledgeService(
        KnowledgeRepository(database_path, runtime_settings=runtime_settings),
        signal_store=signal_store,
        history_store_factory=history_store_factory,
        runtime_settings=runtime_settings,
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
    resource_acquire: Callable[[], Awaitable[None]] | None = None
    resource_cleanup: Callable[[], Awaitable[None]] | None = None
    cleanup_grace_seconds: float = DEFAULT_PIPELINE_CLEANUP_GRACE_SECONDS

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "cleanup_grace_seconds",
            _validate_cleanup_grace_seconds(self.cleanup_grace_seconds),
        )
        limits = pipeline_admission_limits(self.settings)
        admission = self.pipeline_admission
        if admission is None:
            raise ValueError(
                "PipelineDependencies requires a runtime-owned pipeline admission controller; "
                "use build_pipeline_dependencies() or PipelineDependencies.isolated()"
            )
        if admission.limit != limits.concurrent:
            raise ValueError("Pipeline admission limit must match runtime settings")
        if admission.max_queued != limits.queued:
            raise ValueError("Pipeline admission queue limit must match runtime settings")
        if admission.max_queued_per_partition != limits.queued_per_partition:
            raise ValueError("Pipeline admission partition queue limit must match runtime settings")
        if admission.max_in_flight_per_partition != limits.concurrent_per_partition:
            raise ValueError("Pipeline admission partition concurrency limit must match runtime settings")
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
        expected_backends = runtime_descriptor_for_backends(
            component="pipeline_backend_settings",
            runtime_settings=self.settings,
        )
        object.__setattr__(
            self,
            "backend_factory",
            _validated_backend_factory(
                self.backend_factory,
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
        if self.llm_provider_factory is None:
            raise RuntimeOwnershipError("PipelineDependencies requires an explicit LLM provider factory")
        if self.context_provider_factory is None:
            raise RuntimeOwnershipError("PipelineDependencies requires an explicit context provider factory")
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
        limits = pipeline_admission_limits(runtime_settings)
        values["pipeline_admission"] = PipelineAdmissionController(
            limits.concurrent,
            max_queued=limits.queued,
            max_queued_per_partition=limits.queued_per_partition,
            max_in_flight_per_partition=limits.concurrent_per_partition,
        )
        if values.get("runtime_ownership") is not None:
            raise ValueError("isolated pipeline dependencies own their runtime identity")
        values["runtime_ownership"] = runtime_descriptor_from_settings(
            runtime_settings,
            component="isolated_pipeline_dependencies",
        )
        provider_resources = _RuntimeProviderResources(
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
            provider_resources.llm,
            ownership=provider_resources.llm_ownership(
                component="isolated_llm_resource_factory",
            ),
            factory_kind="provider:llm",
        )
        values["context_provider_factory"] = declare_runtime_factory(
            provider_resources.context,
            ownership=runtime_descriptor_for_provider(
                component="isolated_context_resource_factory",
                runtime_settings=runtime_settings,
                capability="context",
            ),
            factory_kind="provider:context",
        )
        values["resource_acquire"] = provider_resources.acquire
        values["resource_cleanup"] = provider_resources.close
        return cls(**values)

    async def acquire_resources(self) -> None:
        """Lease shared resources to the current pipeline run."""
        if self.resource_acquire is not None:
            await self.resource_acquire()

    async def close_resources(self) -> None:
        """Close resources owned by this dependency bundle."""
        if self.resource_cleanup is not None:
            await self.resource_cleanup()

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
    default_signal_store_factory = _declared_store_factory(
        runtime_stores.signals,
        runtime_settings=resolved_settings,
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
    default_history_store_factory = _declared_store_factory(
        runtime_stores.history,
        runtime_settings=resolved_settings,
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
        runtime_stores.feedback,
        runtime_settings=resolved_settings,
        expected=runtime_owner,
        role="feedback",
        component="runtime_feedback_store_factory",
    )
    resolved_feedback_store_factory = _validated_store_factory(
        feedback_store_factory or default_feedback_store_factory,
        expected=runtime_owner,
        role="feedback",
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
    scoped_knowledge_service: Any | None = None
    scoped_knowledge_path: Any | None = None

    def runtime_knowledge_service() -> Any:
        nonlocal scoped_knowledge_path, scoped_knowledge_service
        if validated_knowledge_service_factory is not None:
            service = validated_knowledge_service_factory()
            resolve_owned_database_path(
                boundary="Pipeline knowledge service realization",
                database_role="signals",
                owners=(("knowledge_service", service),),
                runtime_settings=resolved_settings,
            )
            return service
        if signal_store_factory is None and history_store_factory is None:
            return runtime_stores.knowledge()
        signal_store = resolved_signal_store_factory()
        db_path = resolve_owned_database_path(
            boundary="Pipeline signal and knowledge persistence",
            database_role="signals",
            owners=(("signal_store", signal_store),),
            runtime_settings=resolved_settings,
        )
        if scoped_knowledge_service is None or scoped_knowledge_path != db_path:
            scoped_knowledge_service = create_scoped_knowledge_service(
                signal_store,
                history_store_factory=resolved_history_store_factory,
                runtime_settings=resolved_settings,
                boundary="Pipeline signal and knowledge persistence",
            )
            scoped_knowledge_path = db_path
        return scoped_knowledge_service

    def runtime_backends() -> list[DashboardBackend]:
        from tacit import backends

        return backends.get_active_backends(resolved_settings)

    default_backend_factory = declare_backend_factory(
        runtime_backends,
        runtime_settings=resolved_settings,
        component="runtime_backend_factory",
    )
    resolved_backend_factory = _validated_backend_factory(
        backend_factory or default_backend_factory,
        expected=runtime_descriptor_for_backends(
            component="runtime_backend_settings",
            runtime_settings=resolved_settings,
        ),
        lifecycle=resolved_pipeline_admission,
        cleanup_grace_seconds=cleanup_grace_seconds,
    )

    provider_resources = _RuntimeProviderResources(
        resolved_settings,
        lifecycle=resolved_pipeline_admission,
        llm_factory=llm_provider_factory,
        context_factory=context_provider_factory,
        cleanup_grace_seconds=cleanup_grace_seconds,
    )
    declared_knowledge_service_factory = declare_runtime_factory(
        runtime_knowledge_service,
        ownership=runtime_descriptor_for_store(
            component="runtime_knowledge_service_factory",
            runtime_settings=resolved_settings,
            database_role="signals",
            database_path=next(item.path for item in runtime_owner.databases if item.role == "signals"),
        ),
        factory_kind="knowledge:signals",
    )
    declared_llm_resource_factory = declare_runtime_factory(
        provider_resources.llm,
        ownership=provider_resources.llm_ownership(
            component="runtime_llm_resource_factory",
        ),
        factory_kind="provider:llm",
    )
    declared_context_resource_factory = declare_runtime_factory(
        provider_resources.context,
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
        history_store_factory=resolved_history_store_factory,
        feedback_store_factory=resolved_feedback_store_factory,
        llm_cache=runtime_stores.llm_cache(),
        cache_key_factory=make_cache_key,
        pipeline_admission=resolved_pipeline_admission,
        runtime_ownership=runtime_owner,
        signal_store_factory=resolved_signal_store_factory,
        knowledge_service_factory=declared_knowledge_service_factory,
        llm_provider_factory=declared_llm_resource_factory,
        context_provider_factory=declared_context_resource_factory,
        resource_acquire=provider_resources.acquire,
        resource_cleanup=provider_resources.close,
        cleanup_grace_seconds=cleanup_grace_seconds,
    )


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
