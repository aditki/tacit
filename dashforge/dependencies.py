"""Application dependency container.

The default app still uses the existing singleton factories, but core flows can
now receive an explicit dependency bundle in tests, CLIs, or future app
factories. This starts moving orchestration code away from hidden global lookups
without forcing a broad rewrite of every store/backend today.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from dashforge.agents.providers.base import LLMProvider
from dashforge.backends import get_active_backends
from dashforge.backends.base import DashboardBackend
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import Settings, settings
from dashforge.context.base import ContextProvider
from dashforge.history import get_investigation_store


@dataclass(frozen=True)
class PipelineDependencies:
    settings: Settings
    backend_factory: Callable[[], list[DashboardBackend]]
    history_store_factory: Callable[[], Any]
    feedback_store_factory: Callable[[], Any]
    llm_cache: Any
    cache_key_factory: Callable[..., str]
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
    backend_factory: Callable[[], list[DashboardBackend]] | None = None,
    history_store_factory: Callable[[], Any] = get_investigation_store,
    feedback_store_factory: Callable[[], Any] | None = None,
) -> PipelineDependencies:
    """Build a dependency bundle scoped to one runtime settings object."""

    def runtime_backends() -> list[DashboardBackend]:
        return get_active_backends(runtime_settings)

    llm_provider: LLMProvider | None = None
    context_provider: ContextProvider | None = None

    def runtime_llm_provider() -> LLMProvider:
        nonlocal llm_provider
        if llm_provider is not None:
            return llm_provider
        from dashforge.agents.providers.registry import create_provider

        llm_provider = create_provider(runtime_settings)
        return llm_provider

    def runtime_context_provider() -> ContextProvider | None:
        nonlocal context_provider
        if context_provider is not None:
            return context_provider
        from dashforge.context.registry import create_context_provider

        context_provider = create_context_provider(runtime_settings)
        return context_provider

    async def close_runtime_resources() -> None:
        if context_provider is not None:
            await context_provider.close()
        if llm_provider is not None:
            await llm_provider.close()

    return PipelineDependencies(
        settings=runtime_settings,
        backend_factory=backend_factory or runtime_backends,
        history_store_factory=history_store_factory,
        feedback_store_factory=feedback_store_factory or _get_feedback_store,
        llm_cache=llm_cache,
        cache_key_factory=make_cache_key,
        llm_provider_factory=runtime_llm_provider,
        context_provider_factory=runtime_context_provider,
        resource_cleanup=close_runtime_resources,
    )


def _get_feedback_store() -> Any:
    """Resolve the feedback store lazily so monkeypatched runtimes are honored."""
    from dashforge import feedback

    return feedback.get_feedback_store()


def get_default_dependencies() -> PipelineDependencies:
    """Return the production dependency bundle."""
    return PipelineDependencies.defaults()
