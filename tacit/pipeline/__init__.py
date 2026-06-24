"""Investigation pipeline package."""

from __future__ import annotations

from tacit.pipeline import runner as _runner
from tacit.pipeline.discovery import discovery_keywords as _discovery_keywords
from tacit.pipeline.discovery import semantic_mapping_diagnostics as _semantic_mapping_diagnostics
from tacit.pipeline.recording import compiled_query_diagnostics as _compiled_query_diagnostics
from tacit.pipeline.recording import history_archetypes as _history_archetypes
from tacit.pipeline.recording import history_signals as _history_signals

classify_intent = _runner.classify_intent
enrich_context = _runner.enrich_context
get_active_backends = _runner.get_active_backends
get_investigation_store = _runner.get_investigation_store


def _sync_patch_points() -> None:
    _runner.classify_intent = classify_intent
    _runner.enrich_context = enrich_context
    _runner.get_active_backends = get_active_backends
    _runner.get_investigation_store = get_investigation_store


async def run_pipeline(request, deps=None):
    """Run the pipeline, honoring package-level monkeypatch compatibility."""
    _sync_patch_points()
    return await _runner.run_pipeline(request, deps)


__all__ = [
    "_compiled_query_diagnostics",
    "_discovery_keywords",
    "_history_archetypes",
    "_history_signals",
    "_semantic_mapping_diagnostics",
    "classify_intent",
    "enrich_context",
    "get_active_backends",
    "get_investigation_store",
    "run_pipeline",
]
