"""FastAPI dependency providers."""

from __future__ import annotations

import inspect
import threading
from collections.abc import Callable
from typing import Any

from fastapi import Request

import tacit.pipeline as pipeline_mod
from tacit.config import Settings, settings
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies
from tacit.runtime_stores import RuntimeStores

_APP_STORE_LOCK = threading.Lock()


def get_runtime_stores(request: Request) -> RuntimeStores:
    """Return the persistence owner bound to this FastAPI application."""
    state = request.app.state
    existing = getattr(state, "runtime_stores", None)
    if existing is not None:
        return existing
    with _APP_STORE_LOCK:
        existing = getattr(state, "runtime_stores", None)
        if existing is None:
            runtime_settings = getattr(state, "settings", settings)
            existing = RuntimeStores(runtime_settings)
            state.runtime_stores = existing
    return existing


def get_history_store(request: Request) -> Any:
    """Return one history store bound to this app's runtime settings."""
    return get_runtime_stores(request).history()


def get_feedback_store(request: Request) -> Any:
    """Return one feedback store bound to this app's runtime settings."""
    return get_runtime_stores(request).feedback()


def get_signal_store(request: Request) -> Any:
    """Return one bootstrapped signal store bound to this app's runtime settings."""
    return get_runtime_stores(request).signals()


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
    patch ``tacit.pipeline.get_active_backends`` keep working even though
    the API now injects dependencies explicitly.
    """
    sync = getattr(pipeline_mod, "_sync_patch_points", None)
    if sync is not None:
        sync()
    runtime_settings = getattr(request.app.state, "settings", settings)
    stores = get_runtime_stores(request)
    return build_pipeline_dependencies(
        runtime_settings,
        stores=stores,
        backend_factory=_backend_factory_for(runtime_settings),
    )
