"""FastAPI dependency providers."""

from __future__ import annotations

import dashforge.pipeline as pipeline_mod
from dashforge.cache import llm_cache, make_cache_key
from dashforge.config import settings
from dashforge.dependencies import PipelineDependencies


def _get_feedback_store():
    from dashforge import feedback

    return feedback.get_feedback_store()


def get_pipeline_dependencies() -> PipelineDependencies:
    """Return pipeline dependencies for API requests.

    Build through the pipeline package façade so tests and local harnesses that
    patch ``dashforge.pipeline.get_active_backends`` keep working even though
    the API now injects dependencies explicitly.
    """
    sync = getattr(pipeline_mod, "_sync_patch_points", None)
    if sync is not None:
        sync()
    return PipelineDependencies(
        settings=settings,
        backend_factory=pipeline_mod.get_active_backends,
        history_store_factory=pipeline_mod.get_investigation_store,
        feedback_store_factory=_get_feedback_store,
        llm_cache=llm_cache,
        cache_key_factory=make_cache_key,
    )
