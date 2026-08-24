"""Canonical identity helpers for governed signal resolver projections."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def normalize_datasource_types(values: Any) -> tuple[str, ...]:
    """Return the case-insensitive datasource scope as a stable set tuple."""
    if not isinstance(values, (list, tuple, set, frozenset)):
        return ()
    return tuple(sorted({str(value).strip().casefold() for value in values if str(value).strip()}))


def normalize_mapping_confidence(value: Any) -> float:
    """Clamp resolver confidence with one rule for authority and projection."""
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        confidence = 0.5
    return max(0.0, min(1.0, confidence))


def resolver_projection_key(datasource_types: Any) -> str:
    """Identify one per-pattern datasource variant without exposing raw scope text."""
    normalized = normalize_datasource_types(datasource_types)
    if not normalized:
        return ""
    payload = json.dumps(normalized, separators=(",", ":"))
    return f"datasource:{hashlib.sha256(payload.encode()).hexdigest()[:20]}"
