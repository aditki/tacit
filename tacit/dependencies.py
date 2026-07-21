"""Application dependency containers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog

from tacit.agents.providers.base import LLMProvider
from tacit.backends.base import DashboardBackend
from tacit.cache import llm_cache, make_cache_key
from tacit.config import Settings, settings
from tacit.context.base import ContextProvider
from tacit.runtime_stores import RuntimeStores

logger = structlog.get_logger()


@dataclass(frozen=True)
class PipelineDependencies:
    settings: Settings
    backend_factory: Callable[[], list[DashboardBackend]]
    history_store_factory: Callable[[], Any]
    feedback_store_factory: Callable[[], Any]
    llm_cache: Any
    cache_key_factory: Callable[..., str]
    signal_store_factory: Callable[[], Any] | None = None
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
) -> PipelineDependencies:
    """Build a dependency bundle scoped to one runtime settings object."""

    runtime_stores = stores or RuntimeStores(runtime_settings)

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
        history_store_factory=history_store_factory or runtime_stores.history,
        feedback_store_factory=feedback_store_factory or runtime_stores.feedback,
        llm_cache=llm_cache,
        cache_key_factory=make_cache_key,
        signal_store_factory=signal_store_factory or runtime_stores.signals,
        llm_provider_factory=runtime_llm_provider,
        context_provider_factory=runtime_context_provider,
        resource_cleanup=close_runtime_resources,
    )


def get_default_dependencies() -> PipelineDependencies:
    """Return the production dependency bundle."""
    return PipelineDependencies.defaults()
