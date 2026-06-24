"""Application dependency container.

The default app still uses the existing singleton factories, but core flows can
now receive an explicit dependency bundle in tests, CLIs, or future app
factories. This starts moving orchestration code away from hidden global lookups
without forcing a broad rewrite of every store/backend today.
"""

from __future__ import annotations

from collections.abc import Callable
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

    def runtime_llm_provider() -> LLMProvider:
        from dashforge.agents.providers.registry import create_provider

        return create_provider(runtime_settings)

    def runtime_context_provider() -> ContextProvider | None:
        from dashforge.context.registry import create_context_provider

        return create_context_provider(runtime_settings)

    return PipelineDependencies(
        settings=runtime_settings,
        backend_factory=backend_factory or runtime_backends,
        history_store_factory=history_store_factory,
        feedback_store_factory=feedback_store_factory or _get_feedback_store,
        llm_cache=llm_cache,
        cache_key_factory=make_cache_key,
        llm_provider_factory=runtime_llm_provider,
        context_provider_factory=runtime_context_provider,
    )


def _get_feedback_store() -> Any:
    """Resolve the feedback store lazily so monkeypatched runtimes are honored."""
    from dashforge import feedback

    return feedback.get_feedback_store()


def get_default_dependencies() -> PipelineDependencies:
    """Return the production dependency bundle."""
    return PipelineDependencies.defaults()
