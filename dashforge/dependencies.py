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

from dashforge.backends import get_active_backends
from dashforge.backends.base import DashboardBackend
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import Settings, settings
from dashforge.history import get_investigation_store


@dataclass(frozen=True)
class PipelineDependencies:
    settings: Settings
    backend_factory: Callable[[], list[DashboardBackend]]
    history_store_factory: Callable[[], Any]
    llm_cache: Any
    cache_key_factory: Callable[..., str]

    @classmethod
    def defaults(cls) -> PipelineDependencies:
        return cls(
            settings=settings,
            backend_factory=get_active_backends,
            history_store_factory=get_investigation_store,
            llm_cache=llm_cache,
            cache_key_factory=make_cache_key,
        )


def get_default_dependencies() -> PipelineDependencies:
    """Return the production dependency bundle."""
    return PipelineDependencies.defaults()
