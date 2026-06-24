"""Semantic signal mapping package."""

from __future__ import annotations

from tacit.signals import store as _store_module
from tacit.signals.resolution import (
    context_matches as _context_matches,
)
from tacit.signals.resolution import (
    datasource_type_matches as _datasource_type_matches,
)
from tacit.signals.resolution import (
    effective_confidence as _effective_confidence,
)
from tacit.signals.resolution import (
    metric_matches_pattern as _metric_matches_pattern,
)
from tacit.signals.resolution import (
    metric_metadata_compatibility as _metric_metadata_compatibility,
)
from tacit.signals.resolution import (
    missing_context_multiplier as _missing_context_multiplier,
)
from tacit.signals.resolution import (
    unit_class as _unit_class,
)
from tacit.signals.resolution import (
    unit_compatibility as _unit_compatibility,
)
from tacit.signals.store import LearningIndexUnavailable, SignalStore

_DEFAULT_DB_PATH = _store_module._DEFAULT_DB_PATH
_store = _store_module._store


def get_signal_store() -> SignalStore:
    """Return the global signal store while preserving legacy patch points."""
    global _store
    _store_module._DEFAULT_DB_PATH = _DEFAULT_DB_PATH
    _store_module._store = _store
    result = _store_module.get_signal_store()
    _store = result
    return result


__all__ = [
    "LearningIndexUnavailable",
    "SignalStore",
    "_DEFAULT_DB_PATH",
    "_context_matches",
    "_datasource_type_matches",
    "_effective_confidence",
    "_metric_metadata_compatibility",
    "_metric_matches_pattern",
    "_missing_context_multiplier",
    "_store",
    "_unit_class",
    "_unit_compatibility",
    "get_signal_store",
]
