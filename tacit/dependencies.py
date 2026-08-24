"""Application dependency containers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from tacit.agents.providers.base import LLMProvider
from tacit.backends.base import DashboardBackend
from tacit.cache import make_cache_key
from tacit.config import Settings, settings
from tacit.context.base import ContextProvider
from tacit.errors import RuntimeOwnershipError
from tacit.runtime_ownership import (
    RuntimeOwnershipMismatchError,
    describe_runtime_owner,
    get_runtime_ownership,
    require_compatible_runtime_ownership,
    resolve_runtime_settings,
    runtime_descriptor_from_settings,
    snapshot_runtime_settings,
)
from tacit.runtime_stores import RuntimeStores

logger = structlog.get_logger()


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
    signal_store_factory: Callable[[], Any] | None = None
    knowledge_service_factory: Callable[[], Any] | None = None
    llm_provider_factory: Callable[[], LLMProvider] | None = None
    context_provider_factory: Callable[[], ContextProvider | None] | None = None
    resource_cleanup: Callable[[], Awaitable[None]] | None = None

    async def close_resources(self) -> None:
        """Close resources owned by this dependency bundle."""
        if self.resource_cleanup is not None:
            await self.resource_cleanup()

    @classmethod
    def defaults(cls) -> PipelineDependencies:
        return build_pipeline_dependencies(settings)


def build_pipeline_dependencies(
    runtime_settings: Settings,
    *,
    stores: RuntimeStores | None = None,
    backend_factory: Callable[[], list[DashboardBackend]] | None = None,
    history_store_factory: Callable[[], Any] | None = None,
    feedback_store_factory: Callable[[], Any] | None = None,
    signal_store_factory: Callable[[], Any] | None = None,
    knowledge_service_factory: Callable[[], Any] | None = None,
) -> PipelineDependencies:
    """Build a dependency bundle scoped to one runtime settings object."""

    stores_owner = describe_runtime_owner("runtime_stores", stores)
    if stores is not None and stores_owner.settings is None:
        raise ValueError("Pipeline runtime stores must expose their runtime settings")
    resolve_runtime_settings(
        boundary="Pipeline dependencies",
        explicit_settings=runtime_settings,
        owners=(stores_owner,),
        fallback_settings=runtime_settings,
    )
    runtime_stores = stores or RuntimeStores(runtime_settings)
    resolved_signal_store_factory = signal_store_factory or runtime_stores.signals
    resolved_history_store_factory = history_store_factory or runtime_stores.history
    scoped_knowledge_service: Any | None = None
    scoped_knowledge_path: Any | None = None

    def runtime_knowledge_service() -> Any:
        nonlocal scoped_knowledge_path, scoped_knowledge_service
        if knowledge_service_factory is not None:
            service = knowledge_service_factory()
            resolve_owned_database_path(
                boundary="Pipeline knowledge service realization",
                database_role="signals",
                owners=(("knowledge_service", service),),
                runtime_settings=runtime_settings,
            )
            return service
        if signal_store_factory is None and history_store_factory is None:
            return runtime_stores.knowledge()
        signal_store = resolved_signal_store_factory()
        db_path = resolve_owned_database_path(
            boundary="Pipeline signal and knowledge persistence",
            database_role="signals",
            owners=(("signal_store", signal_store),),
            runtime_settings=runtime_settings,
        )
        if scoped_knowledge_service is None or scoped_knowledge_path != db_path:
            scoped_knowledge_service = create_scoped_knowledge_service(
                signal_store,
                history_store_factory=resolved_history_store_factory,
                runtime_settings=runtime_settings,
                boundary="Pipeline signal and knowledge persistence",
            )
            scoped_knowledge_path = db_path
        return scoped_knowledge_service

    def runtime_backends() -> list[DashboardBackend]:
        from tacit import backends

        return backends.get_active_backends(runtime_settings)

    llm_provider: LLMProvider | None = None
    context_provider: ContextProvider | None = None

    def runtime_llm_provider() -> LLMProvider:
        nonlocal llm_provider
        if llm_provider is not None:
            return llm_provider
        from tacit.agents.providers.registry import create_provider

        llm_provider = create_provider(runtime_settings)
        return llm_provider

    def runtime_context_provider() -> ContextProvider | None:
        nonlocal context_provider
        if context_provider is not None:
            return context_provider
        from tacit.context.registry import create_context_provider

        context_provider = create_context_provider(runtime_settings)
        return context_provider

    async def close_runtime_resources() -> None:
        nonlocal context_provider, llm_provider
        if context_provider is not None:
            try:
                await context_provider.close()
            except Exception:
                logger.warning("context_provider_close_failed", exc_info=True)
            finally:
                context_provider = None
        if llm_provider is not None:
            try:
                await llm_provider.close()
            except Exception:
                logger.warning("llm_provider_close_failed", exc_info=True)
            finally:
                llm_provider = None

    return PipelineDependencies(
        settings=runtime_settings,
        backend_factory=backend_factory or runtime_backends,
        history_store_factory=resolved_history_store_factory,
        feedback_store_factory=feedback_store_factory or runtime_stores.feedback,
        llm_cache=runtime_stores.llm_cache(),
        cache_key_factory=make_cache_key,
        signal_store_factory=resolved_signal_store_factory,
        knowledge_service_factory=runtime_knowledge_service,
        llm_provider_factory=runtime_llm_provider,
        context_provider_factory=runtime_context_provider,
        resource_cleanup=close_runtime_resources,
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
