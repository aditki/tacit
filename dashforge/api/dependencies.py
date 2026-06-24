"""FastAPI dependency providers."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from fastapi import Request

import dashforge.pipeline as pipeline_mod
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import Settings, settings
from dashforge.dependencies import PipelineDependencies


def _get_feedback_store():
    from dashforge import feedback

    return feedback.get_feedback_store()


def _backend_factory_for(runtime_settings: Settings) -> Callable[[], Any]:
    """Build backends from app-scoped settings while honoring test monkeypatches."""

    def build_backends() -> Any:
        factory = pipeline_mod.get_active_backends
        try:
            accepts_settings = bool(inspect.signature(factory).parameters)
        except (TypeError, ValueError):
            accepts_settings = False
        if accepts_settings:
            return factory(runtime_settings)
        return factory()

    return build_backends


def get_pipeline_dependencies(request: Request) -> PipelineDependencies:
    """Return pipeline dependencies for API requests.

    Build through the pipeline package façade so tests and local harnesses that
    patch ``dashforge.pipeline.get_active_backends`` keep working even though
    the API now injects dependencies explicitly.
    """
    sync = getattr(pipeline_mod, "_sync_patch_points", None)
    if sync is not None:
        sync()
    runtime_settings = getattr(request.app.state, "settings", settings)
    return PipelineDependencies(
        settings=runtime_settings,
        backend_factory=_backend_factory_for(runtime_settings),
        history_store_factory=pipeline_mod.get_investigation_store,
        feedback_store_factory=_get_feedback_store,
        llm_cache=llm_cache,
        cache_key_factory=make_cache_key,
    )
