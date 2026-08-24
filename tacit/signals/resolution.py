"""Pure signal-resolution scoring helpers."""

from __future__ import annotations

import fnmatch
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Literal

from tacit.models.schemas import MetricEntry

DECAY_HALF_LIFE_DAYS = 90
MIN_CONFIDENCE = 0.05
CONTEXT_MISSING_PENALTY = 0.7


@dataclass(frozen=True, slots=True)
class ResolutionInputTextLimits:
    """Character and UTF-8 admission limits for resolver-adjacent inputs."""

    max_scalar_characters: int = 65_536
    max_scalar_utf8_bytes: int = 262_144
    max_total_characters: int = 2_000_000
    max_total_utf8_bytes: int = 8_000_000

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")


class ResolutionInputWorkLimitError(RuntimeError):
    """Text admission failed before semantic processing could start."""

    reason_code = "resolution_input_work_limit_exceeded"

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"{self.reason_code}: {dimension} exceeds {limit}")


def admit_resolution_input_text(
    values: Iterable[object],
    *,
    limits: ResolutionInputTextLimits | None = None,
) -> dict[str, int]:
    """Admit bounded text in character-first, UTF-8-second passes.

    Callers must bound collection cardinality before building ``values``. The
    first pass uses constant-time string lengths, allowing aggregate character
    rejection before any UTF-8 allocation. Only then are byte lengths computed.
    """
    active_limits = limits or ResolutionInputTextLimits()
    texts: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise ResolutionInputWorkLimitError("scalar_type", 1, 0)
        texts.append(value)
    total_characters = 0
    for value in texts:
        scalar_characters = len(value)
        if scalar_characters > active_limits.max_scalar_characters:
            raise ResolutionInputWorkLimitError(
                "scalar_characters",
                scalar_characters,
                active_limits.max_scalar_characters,
            )
        total_characters += scalar_characters
        if total_characters > active_limits.max_total_characters:
            raise ResolutionInputWorkLimitError(
                "total_input_characters",
                total_characters,
                active_limits.max_total_characters,
            )

    total_utf8_bytes = 0
    for value in texts:
        scalar_utf8_bytes = len(value.encode("utf-8"))
        if scalar_utf8_bytes > active_limits.max_scalar_utf8_bytes:
            raise ResolutionInputWorkLimitError(
                "scalar_utf8_bytes",
                scalar_utf8_bytes,
                active_limits.max_scalar_utf8_bytes,
            )
        total_utf8_bytes += scalar_utf8_bytes
        if total_utf8_bytes > active_limits.max_total_utf8_bytes:
            raise ResolutionInputWorkLimitError(
                "total_input_utf8_bytes",
                total_utf8_bytes,
                active_limits.max_total_utf8_bytes,
            )
    return {
        "input_scalar_count": len(texts),
        "input_character_count": total_characters,
        "input_utf8_byte_count": total_utf8_bytes,
    }


SignalResolutionWorkDimension = Literal[
    "calls",
    "mapping_catalog_comparisons",
    "results_constructed",
]


class SignalResolutionWorkLimitError(RuntimeError):
    """A first-party signal-resolution operation exhausted aggregate work."""

    reason_code = "signal_resolution_aggregate_work_limit_exceeded"

    def __init__(
        self,
        dimension: SignalResolutionWorkDimension,
        observed: int,
        limit: int,
    ) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"{self.reason_code}: {dimension} exceeds {limit}")


@dataclass(slots=True)
class SignalResolutionWorkBudget:
    """Mutable aggregate admission budget shared by one investigation.

    The budget reserves the full mapping-by-catalog comparison product before
    matching starts and accounts for each result before it is materialized.
    A lock keeps shared accounting deterministic if a future first-party caller
    parallelizes independent resolution requests inside one operation.
    """

    max_calls: int
    max_mapping_catalog_comparisons: int
    max_results: int
    calls: int = field(default=0, init=False)
    mapping_catalog_comparisons: int = field(default=0, init=False)
    results_constructed: int = field(default=0, init=False)
    _exhaustion: tuple[SignalResolutionWorkDimension, int, int] | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _lock: Lock = field(default_factory=Lock, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for field_name in (
            "max_calls",
            "max_mapping_catalog_comparisons",
            "max_results",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")

    def _reserve(
        self,
        dimension: SignalResolutionWorkDimension,
        amount: int,
        limit: int,
    ) -> None:
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise ValueError("signal-resolution work increments must be non-negative integers")
        with self._lock:
            if self._exhaustion is not None:
                exhausted_dimension, exhausted_observed, exhausted_limit = self._exhaustion
                raise SignalResolutionWorkLimitError(
                    exhausted_dimension,
                    exhausted_observed,
                    exhausted_limit,
                )
            current = int(getattr(self, dimension))
            observed = current + amount
            if observed > limit:
                self._exhaustion = (dimension, observed, limit)
                raise SignalResolutionWorkLimitError(dimension, observed, limit)
            setattr(self, dimension, observed)

    def begin_call(self) -> None:
        """Reserve one first-party resolver invocation."""
        self._reserve("calls", 1, self.max_calls)

    def reserve_mapping_catalog_comparisons(
        self,
        mapping_count: int,
        eligible_catalog_count: int,
    ) -> None:
        """Reserve the worst-case comparison product before matching."""
        if mapping_count < 0 or eligible_catalog_count < 0:
            raise ValueError("resolution cardinalities must be non-negative")
        self._reserve(
            "mapping_catalog_comparisons",
            mapping_count * eligible_catalog_count,
            self.max_mapping_catalog_comparisons,
        )

    def consume_result(self) -> None:
        """Reserve one result slot before constructing or appending it."""
        self._reserve("results_constructed", 1, self.max_results)

    def raise_if_exhausted(self) -> None:
        """Re-raise a swallowed limit error before derived output can escape."""
        with self._lock:
            exhaustion = self._exhaustion
        if exhaustion is not None:
            dimension, observed, limit = exhaustion
            raise SignalResolutionWorkLimitError(dimension, observed, limit)

    def counters(self) -> dict[str, int]:
        """Return bounded, payload-free observability counters."""
        with self._lock:
            return {
                "resolution_call_count": self.calls,
                "mapping_catalog_comparison_count": self.mapping_catalog_comparisons,
                "result_construction_count": self.results_constructed,
                "resolution_call_limit": self.max_calls,
                "mapping_catalog_comparison_limit": self.max_mapping_catalog_comparisons,
                "result_construction_limit": self.max_results,
                "signal_resolution_work_limit_exhausted": int(self._exhaustion is not None),
            }


def signal_resolution_work_kwargs(
    resolver: Any,
    work_budget: SignalResolutionWorkBudget | None,
) -> dict[str, SignalResolutionWorkBudget]:
    """Pass work accounting only to resolvers declaring the first-party contract."""
    if work_budget is None or not bool(getattr(resolver, "supports_signal_resolution_work_budget", False)):
        return {}
    return {"work_budget": work_budget}


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
ELASTICSEARCH_DATASOURCE_TYPES = {"elasticsearch", "opensearch"}


def datasource_type_matches(candidate: str, requested: str) -> bool:
    candidate = (candidate or "").lower()
    requested = (requested or "").lower()
    if not requested:
        return True
    if candidate == requested:
        return True
    candidate_prometheus = candidate in PROMETHEUS_DATASOURCE_TYPES or any(
        marker in candidate for marker in PROMETHEUS_DATASOURCE_TYPES
    )
    requested_prometheus = requested in PROMETHEUS_DATASOURCE_TYPES or any(
        marker in requested for marker in PROMETHEUS_DATASOURCE_TYPES
    )
    if candidate_prometheus and requested_prometheus:
        return True
    candidate_signalfx = candidate in SIGNALFX_DATASOURCE_TYPES or "signalfx" in candidate
    requested_signalfx = requested in SIGNALFX_DATASOURCE_TYPES or "signalfx" in requested
    if candidate_signalfx and requested_signalfx:
        return True
    candidate_elastic = candidate in ELASTICSEARCH_DATASOURCE_TYPES or any(
        marker in candidate for marker in ELASTICSEARCH_DATASOURCE_TYPES
    )
    requested_elastic = requested in ELASTICSEARCH_DATASOURCE_TYPES or any(
        marker in requested for marker in ELASTICSEARCH_DATASOURCE_TYPES
    )
    if candidate_elastic and requested_elastic:
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
