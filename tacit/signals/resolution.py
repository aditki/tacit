"""Pure signal-resolution scoring helpers."""

from __future__ import annotations

import fnmatch
import math
from typing import Any

from tacit.models.schemas import MetricEntry

DECAY_HALF_LIFE_DAYS = 90
MIN_CONFIDENCE = 0.05
CONTEXT_MISSING_PENALTY = 0.7


def context_matches(
    mapping: dict[str, Any],
    service: str,
    datasource_type: str,
    archetype: str,
    environment: str,
) -> bool:
    """Check if a mapping's context filters match the given context."""
    if service and mapping.get("context_services"):
        if service.lower() not in [s.lower() for s in mapping["context_services"]]:
            return False
    if datasource_type and mapping.get("context_datasource_types"):
        if datasource_type.lower() not in [d.lower() for d in mapping["context_datasource_types"]]:
            return False
    if archetype and mapping.get("context_archetypes"):
        if archetype.lower() not in [a.lower() for a in mapping["context_archetypes"]]:
            return False
    if environment and mapping.get("context_environments"):
        if environment.lower() not in [e.lower() for e in mapping["context_environments"]]:
            return False
    return True


PROMETHEUS_DATASOURCE_TYPES = {"prometheus", "mimir", "cortex", "thanos"}
SIGNALFX_DATASOURCE_TYPES = {"signalfx", "grafana-signalfx-datasource"}


def datasource_type_matches(candidate: str, requested: str) -> bool:
    candidate = (candidate or "").lower()
    requested = (requested or "").lower()
    if not requested:
        return True
    if candidate == requested:
        return True
    if candidate in PROMETHEUS_DATASOURCE_TYPES and requested in PROMETHEUS_DATASOURCE_TYPES:
        return True
    if candidate in SIGNALFX_DATASOURCE_TYPES and requested in SIGNALFX_DATASOURCE_TYPES:
        return True
    return False


UNIT_CLASSES: list[tuple[str, frozenset[str]]] = [
    ("time", frozenset({"s", "ms", "ns", "us", "µs", "seconds", "milliseconds", "nanoseconds"})),
    ("bytes", frozenset({"bytes", "decbytes", "bits", "kb", "mb", "gb", "kib", "mib", "gib"})),
    ("percent", frozenset({"percent", "percentunit", "%"})),
    ("rate", frozenset({"ops", "reqps", "rps", "cps", "wps", "/s", "persec"})),
]


def unit_class(unit: str) -> str:
    unit = (unit or "").strip().lower()
    if not unit:
        return ""
    for name, members in UNIT_CLASSES:
        if unit in members:
            return name
    return ""


def unit_compatibility(expected_unit: str, metric_unit: str) -> float:
    """Multiplier for confidence based on unit agreement."""
    exp = unit_class(expected_unit)
    got = unit_class(metric_unit)
    if not exp or not got:
        return 1.0
    if exp == got:
        return 1.1
    return 0.5


def metric_metadata_compatibility(
    signal_type: str,
    signal_definition: dict[str, Any],
    entry: MetricEntry,
) -> float:
    """Score datasource metadata that supports or contradicts a name match."""
    score = unit_compatibility(signal_definition.get("unit", ""), entry.unit)
    category = str(signal_definition.get("category", "")).lower()
    metric_type = (entry.metric_type or "").lower()

    if category == "latency":
        if metric_type in {"histogram", "summary", "gaugehistogram"}:
            score *= 1.15
        elif metric_type in {"gauge", "info"} and unit_class(entry.unit) != "time":
            score *= 0.8
    elif category in {"throughput", "errors"} and metric_type in {"counter", "sum"}:
        score *= 1.1
    elif category in {"saturation", "resource", "resource_usage", "capacity"} and metric_type == "gauge":
        score *= 1.05

    context = " ".join([entry.namespace, *entry.dimensions]).lower()
    semantic_hints: tuple[str, ...] = ()
    if signal_type.startswith(("request_", "api_", "error_")):
        semantic_hints = ("http", "rpc", "route", "method", "status_code")
    elif signal_type.startswith("cache_"):
        semantic_hints = ("cache", "redis", "memcached", "keyspace", "db")
    elif signal_type in {"queue_depth", "consumer_lag", "message_rate"}:
        semantic_hints = ("queue", "messaging", "kafka", "consumer", "destination")
    elif signal_type in {"cpu_usage", "memory_usage", "disk_usage", "network_bytes"}:
        semantic_hints = ("host", "container", "process", "pod", "device")
    if context and semantic_hints and any(hint in context for hint in semantic_hints):
        score *= 1.08

    return score


def missing_context_multiplier(
    mapping: dict[str, Any],
    service: str = "",
    datasource_type: str = "",
    archetype: str = "",
    environment: str = "",
    *,
    context_missing_penalty: float = CONTEXT_MISSING_PENALTY,
) -> float:
    """Return a ranking penalty when constrained mapping context is absent."""
    missing_context = (
        (not service and bool(mapping.get("context_services")))
        or (not datasource_type and bool(mapping.get("context_datasource_types")))
        or (not archetype and bool(mapping.get("context_archetypes")))
        or (not environment and bool(mapping.get("context_environments")))
    )
    return context_missing_penalty if missing_context else 1.0


def effective_confidence(
    mapping: dict[str, Any],
    now: float,
    *,
    context_service: str = "",
    context_datasource_type: str = "",
    context_archetype: str = "",
    context_environment: str = "",
    apply_context_penalty: bool = True,
    min_confidence: float = MIN_CONFIDENCE,
    decay_half_life_days: int = DECAY_HALF_LIFE_DAYS,
    context_missing_penalty: float = CONTEXT_MISSING_PENALTY,
) -> float:
    """Compute effective confidence with time decay, feedback, and context adjustment."""
    base = mapping["confidence"]

    context_multiplier = (
        missing_context_multiplier(
            mapping,
            context_service,
            context_datasource_type,
            context_archetype,
            context_environment,
            context_missing_penalty=context_missing_penalty,
        )
        if apply_context_penalty
        else 1.0
    )

    if mapping.get("source_type") == "bootstrap":
        return max(base * context_multiplier, min_confidence)

    last_seen = mapping.get("last_seen", now)
    age_days = (now - last_seen) / 86400.0
    if age_days > 0:
        decay = math.pow(0.5, age_days / decay_half_life_days)
        base *= decay

    pos = mapping.get("positive_feedback", 0)
    neg = mapping.get("negative_feedback", 0)
    total_fb = pos + neg
    if total_fb > 0:
        fb_ratio = pos / total_fb
        fb_multiplier = 0.7 + 0.6 * fb_ratio
        base *= fb_multiplier

    return max(base * context_multiplier, min_confidence)


def metric_matches_pattern(metric_name: str, pattern: str) -> bool:
    """Check if a metric name matches a signal mapping pattern."""
    if pattern == metric_name:
        return True
    if "*" in pattern or "?" in pattern:
        return fnmatch.fnmatch(metric_name, pattern)
    return pattern in metric_name
