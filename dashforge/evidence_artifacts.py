"""Build validation-gated evidence artifacts from resolved evidence state."""

from __future__ import annotations

import re

from dashforge.catalog import catalog_for_services
from dashforge.evidence import SUPPORTED_OBSERVATION
from dashforge.models.schemas import (
    DashboardSpec,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
)

_SYMPTOM_SIGNAL_PANELS = {
    "request_latency": ("Observed Request Latency", "Application request timing evidence", "s"),
    "api_latency": ("Observed API Latency", "Application API timing evidence", "s"),
    "request_rate": ("Observed Request Rate", "Application request traffic evidence", "reqps"),
    "error_rate": ("Observed Error Rate", "Application error evidence", "percentunit"),
}

_EVIDENCE_GAP_SIGNAL_PANELS = {
    "cpu_usage": ("Supported CPU Observation", "Validated CPU evidence observation", "short"),
    "memory_usage": ("Supported Memory Observation", "Validated memory evidence observation", "bytes"),
    "network_bytes": ("Supported Network Observation", "Validated network traffic evidence observation", "Bps"),
    "disk_usage": ("Supported Disk Observation", "Validated disk usage evidence observation", "bytes"),
    "io_wait": ("Supported I/O Observation", "Validated I/O wait evidence observation", "s"),
    "queue_depth": ("Supported Queue Observation", "Validated queue depth evidence observation", "short"),
    "db_connection_pool": (
        "Supported DB Connection Observation",
        "Validated DB connection evidence observation",
        "short",
    ),
    "in_flight_requests": (
        "Supported In-Flight Request Observation",
        "Validated concurrency evidence observation",
        "short",
    ),
    "cache_memory_pressure": (
        "Supported Cache Memory Observation",
        "Validated cache memory evidence observation",
        "bytes",
    ),
    "cache_client_pressure": (
        "Supported Cache Client Observation",
        "Validated cache client pressure evidence observation",
        "short",
    ),
    "consumer_lag": ("Supported Consumer Lag Observation", "Validated consumer lag evidence observation", "short"),
    "pod_restarts": ("Supported Restart Observation", "Validated restart evidence observation", "short"),
}

_SERVICE_SELECTOR_LABELS = ("service", "service_name", "service.name", "app", "application", "container", "pod")
_PROMETHEUS_COMPATIBLE_DATASOURCE_TYPES = {"", "prometheus", "mimir", "cortex", "thanos"}
_MIN_GUARDED_FALLBACK_SCORE = 0.6


def _service_aliases(service: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", "-", service.lower()).strip("-")
    if not normalized:
        return set()
    aliases = {normalized}
    for suffix in ("-service", "-svc"):
        if normalized.endswith(suffix) and len(normalized) > len(suffix):
            aliases.add(normalized[: -len(suffix)])
    return aliases


def _dimension_label_values(dimension: str) -> tuple[str, list[str]]:
    key, separator, raw_value = dimension.partition("=")
    label = key.strip()
    if not separator:
        return label, []
    values = [value.strip().strip("\"'") for value in raw_value.strip().strip("{}").split(",")]
    return label, [value for value in values if value]


def _service_value_matches(value: str, aliases: set[str]) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return any(re.search(rf"(?:^|-){re.escape(alias)}(?:-|$)", normalized) for alias in aliases)


def _promql_service_selector(services: list[str], metric_entry: MetricEntry | None = None) -> str:
    from dashforge.archetypes.engine import _re2_escape

    if not services:
        return ""
    target = services[0].lower().replace(" ", "-")
    fallback = f'{{service=~".*{_re2_escape(target)}.*"}}' if target else ""
    if metric_entry is None:
        return fallback
    aliases = {alias for service in services for alias in _service_aliases(service)}
    if not aliases:
        return fallback

    for dimension in metric_entry.dimensions:
        label, values = _dimension_label_values(dimension)
        if label.lower() not in _SERVICE_SELECTOR_LABELS:
            continue
        if values:
            selected = [value for value in values if _service_value_matches(value, aliases)]
            if not selected:
                continue
            escaped = "|".join(_re2_escape(value) for value in sorted(selected))
            return f'{{{label}=~"{escaped}"}}'
        return f'{{{label}=~".*{_re2_escape(target)}.*"}}' if target else ""
    return fallback


def _signalflow_service_filter(services: list[str], metric_entry: MetricEntry | None = None) -> str:
    if not services:
        return ""
    target = services[0].lower().replace(" ", "-").replace("'", "\\'")
    fallback = f", filter=filter('service', '*{target}*')" if target else ""
    if metric_entry is None:
        return fallback
    aliases = {alias for service in services for alias in _service_aliases(service)}
    if not aliases:
        return fallback
    for dimension in metric_entry.dimensions:
        label, values = _dimension_label_values(dimension)
        if label.lower() not in _SERVICE_SELECTOR_LABELS:
            continue
        if values:
            selected = [value for value in values if _service_value_matches(value, aliases)]
            if not selected:
                continue
            value = sorted(selected)[0]
        else:
            return fallback
        return f", filter=filter('{label}', '{value}')"
    return fallback


def _catalog_entry_for_resolution(resolution: EvidenceResolution, catalog: list[MetricEntry]) -> MetricEntry | None:
    for entry in catalog:
        if entry.name != resolution.metric:
            continue
        if resolution.datasource_uid and entry.datasource_uid != resolution.datasource_uid:
            continue
        if resolution.datasource_type and entry.datasource_type != resolution.datasource_type:
            continue
        if resolution.query_language and entry.query_language != resolution.query_language:
            continue
        return entry
    return None


def _is_counter_metric(metric: str, entry: MetricEntry | None) -> bool:
    metric_type = (entry.metric_type if entry else "").lower()
    return metric_type == "counter" or metric.endswith(("_total", "_count"))


def _symptom_signal_type(requirement: EvidenceRequirement, resolution: EvidenceResolution) -> str:
    if requirement.signal_type:
        return requirement.signal_type
    metric_text = " ".join([requirement.default_metric, resolution.metric]).lower()
    source_text = " ".join([requirement.id, requirement.source]).lower()
    if "error" in metric_text or "5xx" in metric_text or "error" in source_text:
        return "error_rate"
    if "latency" in metric_text or "duration" in metric_text:
        return "request_latency"
    if "request_rate" in metric_text or "requests_total" in metric_text:
        return "request_rate"
    return ""


def _evidence_gap_signal_type(requirement: EvidenceRequirement, resolution: EvidenceResolution) -> str:
    if requirement.signal_type in _EVIDENCE_GAP_SIGNAL_PANELS:
        return requirement.signal_type
    metric_text = " ".join([requirement.default_metric, resolution.metric]).lower()
    if "cpu" in metric_text:
        return "cpu_usage"
    if "memory" in metric_text or "_mem_" in metric_text:
        return "memory_usage"
    if "network" in metric_text or "rx_bytes" in metric_text or "tx_bytes" in metric_text:
        return "network_bytes"
    if "disk" in metric_text or "filesystem" in metric_text:
        return "disk_usage"
    if "queue" in metric_text or "backlog" in metric_text:
        return "queue_depth"
    if "restart" in metric_text:
        return "pod_restarts"
    return ""


def _symptom_unit(signal_type: str, metric: str, entry: MetricEntry | None) -> str:
    if signal_type == "error_rate" and _is_counter_metric(metric, entry):
        return "ops"
    return _SYMPTOM_SIGNAL_PANELS[signal_type][2]


def _promql_symptom_query(signal_type: str, metric: str, selector: str, entry: MetricEntry | None) -> str:
    metric_lower = metric.lower()
    if signal_type in {"request_latency", "api_latency"} and metric_lower.endswith("_bucket"):
        return f"histogram_quantile(0.95, sum(rate({metric}{selector}[5m])) by (le))"
    if signal_type in {"request_latency", "api_latency"} and metric_lower.endswith(("_sum", "_count")):
        return ""
    if signal_type == "request_rate" and _is_counter_metric(metric, entry):
        return f"sum(rate({metric}{selector}[5m]))"
    if signal_type == "error_rate" and _is_counter_metric(metric, entry):
        if any(token in metric_lower for token in ("error", "errors", "failure", "failures", "5xx")):
            return f"sum(rate({metric}{selector}[5m]))"
        return ""
    return f"{metric}{selector}"


def _signalflow_symptom_query(signal_type: str, metric: str, filt: str, entry: MetricEntry | None) -> str:
    metric_lower = metric.lower()
    if signal_type in {"request_latency", "api_latency"} and metric_lower.endswith("_bucket"):
        return f"data('{metric.removesuffix('_bucket')}'{filt}).percentile(pct=95).publish(label='p95')"
    if signal_type in {"request_latency", "api_latency"} and metric_lower.endswith(("_sum", "_count")):
        return ""
    if signal_type == "request_rate" and _is_counter_metric(metric, entry):
        return f"data('{metric}'{filt}, rollup='rate').sum().publish(label='rate')"
    if signal_type == "error_rate" and _is_counter_metric(metric, entry):
        if any(token in metric_lower for token in ("error", "errors", "failure", "failures", "5xx")):
            return f"data('{metric}'{filt}, rollup='rate').sum().publish(label='errors')"
        return ""
    return f"data('{metric}'{filt}).mean().publish(label='value')"


def _promql_evidence_gap_query(signal_type: str, metric: str, selector: str, entry: MetricEntry | None) -> str:
    metric_lower = metric.lower()
    if signal_type == "cpu_usage" and _is_counter_metric(metric, entry):
        return f"sum(rate({metric}{selector}[5m]))"
    if signal_type in {"network_bytes", "io_wait"} and _is_counter_metric(metric, entry):
        return f"sum(rate({metric}{selector}[5m]))"
    if signal_type == "pod_restarts" and _is_counter_metric(metric, entry):
        return f"sum(increase({metric}{selector}[15m]))"
    if metric_lower.endswith("_bucket"):
        return ""
    return f"{metric}{selector}"


def _signalflow_evidence_gap_query(signal_type: str, metric: str, filt: str, entry: MetricEntry | None) -> str:
    if signal_type == "cpu_usage" and _is_counter_metric(metric, entry):
        return f"data('{metric}'{filt}, rollup='rate').sum().publish(label='cpu')"
    if signal_type in {"network_bytes", "io_wait"} and _is_counter_metric(metric, entry):
        return f"data('{metric}'{filt}, rollup='rate').sum().publish(label='rate')"
    if signal_type == "pod_restarts" and _is_counter_metric(metric, entry):
        return f"data('{metric}'{filt}, rollup='delta').sum().publish(label='restarts')"
    return f"data('{metric}'{filt}).mean().publish(label='value')"


def _symptom_query_expr(
    signal_type: str,
    resolution: EvidenceResolution,
    intent: Intent,
    metric_entry: MetricEntry | None,
) -> str:
    query_language = (resolution.query_language or "promql").lower()
    datasource_type = (resolution.datasource_type or "prometheus").lower()
    if query_language in {"", "promql"} and datasource_type in _PROMETHEUS_COMPATIBLE_DATASOURCE_TYPES:
        selector = _promql_service_selector(intent.services, metric_entry)
        return _promql_symptom_query(signal_type, resolution.metric, selector, metric_entry)
    if query_language == "signalflow" or datasource_type in {"signalfx", "grafana-signalfx-datasource"}:
        filt = _signalflow_service_filter(intent.services, metric_entry)
        return _signalflow_symptom_query(signal_type, resolution.metric, filt, metric_entry)
    return ""


def _evidence_gap_query_expr(
    signal_type: str,
    resolution: EvidenceResolution,
    intent: Intent,
    metric_entry: MetricEntry | None,
) -> str:
    query_language = (resolution.query_language or "promql").lower()
    datasource_type = (resolution.datasource_type or "prometheus").lower()
    if query_language in {"", "promql"} and datasource_type in _PROMETHEUS_COMPATIBLE_DATASOURCE_TYPES:
        selector = _promql_service_selector(intent.services, metric_entry)
        return _promql_evidence_gap_query(signal_type, resolution.metric, selector, metric_entry)
    if query_language == "signalflow" or datasource_type in {"signalfx", "grafana-signalfx-datasource"}:
        filt = _signalflow_service_filter(intent.services, metric_entry)
        return _signalflow_evidence_gap_query(signal_type, resolution.metric, filt, metric_entry)
    return ""


def build_symptom_evidence_dashboard(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    intent: Intent,
    *,
    catalog: list[MetricEntry],
    target_language: str,
    timerange: str,
) -> tuple[DashboardSpec, list[EvidenceResolution]]:
    """Build direct, validation-gated panels for observed application symptoms."""
    resolutions_by_id = {resolution.requirement_id: resolution for resolution in resolutions}
    panels: list[PanelSpec] = []
    rescue_resolutions: list[EvidenceResolution] = []
    seen: set[tuple[str, str, str]] = set()

    for requirement in requirements:
        resolution = resolutions_by_id.get(requirement.id)
        if resolution is None or resolution.status != EvidenceResolutionStatus.RESOLVED or not resolution.metric:
            resolution = _resolve_direct_symptom_evidence(
                requirement,
                intent,
                catalog,
                target_language=target_language,
            )
        if resolution is None or resolution.status != EvidenceResolutionStatus.RESOLVED or not resolution.metric:
            continue
        signal_type = _symptom_signal_type(requirement, resolution)
        if signal_type not in _SYMPTOM_SIGNAL_PANELS:
            continue
        query_language = (resolution.query_language or "promql").lower()
        datasource_type = (resolution.datasource_type or "prometheus").lower()
        supports_promql = (
            query_language in {"", "promql"} and datasource_type in _PROMETHEUS_COMPATIBLE_DATASOURCE_TYPES
        )
        supports_signalflow = query_language == "signalflow" or datasource_type in {
            "signalfx",
            "grafana-signalfx-datasource",
        }
        if not (supports_promql or supports_signalflow):
            continue
        key = (signal_type, resolution.metric, resolution.datasource_uid)
        metric_entry = _catalog_entry_for_resolution(resolution, catalog)
        query_expr = _symptom_query_expr(signal_type, resolution, intent, metric_entry)
        if not query_expr:
            continue
        if key in seen:
            rescue_resolutions.append(resolution)
            continue
        seen.add(key)
        rescue_resolutions.append(resolution)
        title, description, _ = _SYMPTOM_SIGNAL_PANELS[signal_type]
        panels.append(
            PanelSpec(
                title=title,
                description=description,
                row="Observed Symptoms",
                source_archetype=requirement.source,
                unit=_symptom_unit(signal_type, resolution.metric, metric_entry),
                queries=[
                    PanelQuery(
                        expr=query_expr,
                        legend_format="{{service}}",
                        datasource_uid=resolution.datasource_uid,
                        datasource_type=resolution.datasource_type or "prometheus",
                        query_language=resolution.query_language or "promql",
                    )
                ],
            )
        )

    return (
        DashboardSpec(
            title=f"{intent.services[0].title() if intent.services else 'Service'} — Observed Symptoms",
            tags=["dashforge", "evidence", "symptom"],
            timerange=timerange,
            panels=panels,
        ),
        rescue_resolutions,
    )


def build_evidence_gap_dashboard(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    intent: Intent,
    *,
    catalog: list[MetricEntry],
    target_language: str,
    timerange: str,
) -> tuple[DashboardSpec, list[EvidenceResolution]]:
    """Build validation-gated panels for supported observations found while closing evidence gaps."""
    resolutions_by_id = {resolution.requirement_id: resolution for resolution in resolutions}
    panels: list[PanelSpec] = []
    gap_resolutions: list[EvidenceResolution] = []
    seen: set[tuple[str, str, str]] = set()

    def mark_gap_resolution(resolution: EvidenceResolution) -> EvidenceResolution:
        return resolution.model_copy(
            update={
                "status": EvidenceResolutionStatus.RESOLVED,
                "reason_code": "evidence_gap_supported_observation",
            }
        )

    for requirement in requirements:
        resolution = resolutions_by_id.get(requirement.id)
        if resolution is None or resolution.status != EvidenceResolutionStatus.RESOLVED or not resolution.metric:
            resolution = _resolve_evidence_gap_observation(
                requirement,
                intent,
                catalog,
                target_language=target_language,
            )
        if resolution is None or resolution.status != EvidenceResolutionStatus.RESOLVED or not resolution.metric:
            continue
        signal_type = _evidence_gap_signal_type(requirement, resolution)
        if signal_type not in _EVIDENCE_GAP_SIGNAL_PANELS:
            continue
        query_language = (resolution.query_language or "promql").lower()
        datasource_type = (resolution.datasource_type or "prometheus").lower()
        supports_promql = (
            query_language in {"", "promql"} and datasource_type in _PROMETHEUS_COMPATIBLE_DATASOURCE_TYPES
        )
        supports_signalflow = query_language == "signalflow" or datasource_type in {
            "signalfx",
            "grafana-signalfx-datasource",
        }
        if not (supports_promql or supports_signalflow):
            continue
        metric_entry = _catalog_entry_for_resolution(resolution, catalog)
        if intent.services and metric_entry is None:
            continue
        if intent.services and metric_entry is not None:
            scoped = catalog_for_services([metric_entry], intent.services, include_unscoped=False)
            if not scoped:
                continue
        query_expr = _evidence_gap_query_expr(signal_type, resolution, intent, metric_entry)
        if not query_expr:
            continue
        gap_resolution = mark_gap_resolution(resolution)
        key = (signal_type, resolution.metric, resolution.datasource_uid)
        if key in seen:
            gap_resolutions.append(gap_resolution)
            continue
        seen.add(key)
        gap_resolutions.append(gap_resolution)
        title, description, unit = _EVIDENCE_GAP_SIGNAL_PANELS[signal_type]
        panels.append(
            PanelSpec(
                title=title,
                description=description,
                row="Supported Observations",
                source_archetype=requirement.source,
                unit=unit,
                queries=[
                    PanelQuery(
                        expr=query_expr,
                        legend_format="{{service}}",
                        datasource_uid=resolution.datasource_uid,
                        datasource_type=resolution.datasource_type or "prometheus",
                        query_language=resolution.query_language or "promql",
                    )
                ],
            )
        )

    return (
        DashboardSpec(
            title=f"{intent.services[0].title() if intent.services else 'Service'} — Evidence Gap Observations",
            tags=["dashforge", "evidence", "gap-observation"],
            timerange=timerange,
            panels=panels,
        ),
        gap_resolutions,
    )


def missing_critical_symptom_requirements(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    observations: list[EvidenceObservation],
) -> list[EvidenceRequirement]:
    surfaced_ids = {
        observation.requirement_id
        for observation in observations
        if observation.non_empty or (observation.survived and observation.rejection_reason == "exists")
    }
    resolutions_by_id = {resolution.requirement_id: resolution for resolution in resolutions}
    missing: list[EvidenceRequirement] = []
    for requirement in requirements:
        if requirement.priority != "critical" or requirement.id in surfaced_ids:
            continue
        resolution = resolutions_by_id.get(requirement.id)
        if resolution is None:
            resolution = EvidenceResolution(
                requirement_id=requirement.id,
                status=EvidenceResolutionStatus.UNKNOWN,
                reason_code="unknown",
            )
        if _symptom_signal_type(requirement, resolution) in _SYMPTOM_SIGNAL_PANELS:
            missing.append(requirement)
    return missing


def missing_critical_evidence_gap_requirements(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    observations: list[EvidenceObservation],
) -> list[EvidenceRequirement]:
    surfaced_ids = {
        observation.requirement_id
        for observation in observations
        if observation.outcome == SUPPORTED_OBSERVATION
        or observation.non_empty
        or (observation.survived and observation.rejection_reason == "exists")
    }
    resolutions_by_id = {resolution.requirement_id: resolution for resolution in resolutions}
    missing: list[EvidenceRequirement] = []
    for requirement in requirements:
        if requirement.priority != "critical" or requirement.id in surfaced_ids:
            continue
        resolution = resolutions_by_id.get(requirement.id)
        if resolution is None:
            resolution = EvidenceResolution(
                requirement_id=requirement.id,
                status=EvidenceResolutionStatus.UNKNOWN,
                reason_code="unknown",
            )
        if _symptom_signal_type(requirement, resolution) in _SYMPTOM_SIGNAL_PANELS:
            continue
        if _evidence_gap_signal_type(requirement, resolution) in _EVIDENCE_GAP_SIGNAL_PANELS:
            missing.append(requirement)
    return missing


def _resolve_direct_symptom_evidence(
    requirement: EvidenceRequirement,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str,
) -> EvidenceResolution | None:
    """Resolve symptom evidence for direct observation panels."""
    from dashforge.archetypes.engine import _datasource_type_for_language, _legacy_metric_signal
    from dashforge.signals import get_signal_store

    try:
        store = get_signal_store()
    except Exception:
        return None

    target_catalog = [
        entry for entry in catalog if (entry.query_language or "").lower() in {"", target_language.lower()}
    ]
    scoped_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=True)
    signal_type = requirement.signal_type or _legacy_metric_signal(
        store,
        requirement.default_metric,
        scoped_catalog,
        target_language,
    )
    if signal_type not in _SYMPTOM_SIGNAL_PANELS:
        return None
    resolved = store.resolve_signal(
        signal_type,
        scoped_catalog,
        context_service=intent.services[0] if intent.services else "",
        context_datasource_type=_datasource_type_for_language(target_language),
        context_archetype=requirement.source,
        target_query_language=target_language,
    )
    if not resolved:
        return None
    best_score = resolved[0][1]
    best = [item for item in resolved if item[1] == best_score]
    best_owners = {(entry.name, entry.datasource_uid, entry.datasource_type, entry.query_language) for entry, _ in best}
    if len(best_owners) > 1:
        return None
    entry, score = best[0]
    return EvidenceResolution(
        requirement_id=requirement.id,
        status=EvidenceResolutionStatus.RESOLVED,
        reason_code="direct_symptom_signal_resolved",
        metric=entry.name,
        datasource_uid=entry.datasource_uid,
        datasource_type=entry.datasource_type,
        query_language=entry.query_language,
        semantic_score=score,
        ownership_score=1.0,
    )


def _resolve_evidence_gap_observation(
    requirement: EvidenceRequirement,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str,
) -> EvidenceResolution | None:
    """Resolve an evidence gap only when ownership is specific enough to observe safely."""
    from dashforge.archetypes.engine import _datasource_type_for_language, _legacy_metric_signal
    from dashforge.signals import get_signal_store

    try:
        store = get_signal_store()
    except Exception:
        return None

    target_catalog = [
        entry for entry in catalog if (entry.query_language or "").lower() in {"", target_language.lower()}
    ]
    scoped_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=False)
    signal_type = requirement.signal_type or _legacy_metric_signal(
        store,
        requirement.default_metric,
        scoped_catalog,
        target_language,
    )
    if signal_type not in _EVIDENCE_GAP_SIGNAL_PANELS:
        return None
    resolved = store.resolve_signal(
        signal_type,
        scoped_catalog,
        context_service=intent.services[0] if intent.services else "",
        context_datasource_type=_datasource_type_for_language(target_language),
        context_archetype=requirement.source,
        target_query_language=target_language,
    )
    if not resolved:
        return None
    best_score = resolved[0][1]
    if best_score < _MIN_GUARDED_FALLBACK_SCORE:
        return None
    best = [item for item in resolved if item[1] == best_score]
    best_owners = {(entry.name, entry.datasource_uid, entry.datasource_type, entry.query_language) for entry, _ in best}
    if len(best_owners) > 1:
        return None
    entry, score = best[0]
    return EvidenceResolution(
        requirement_id=requirement.id,
        status=EvidenceResolutionStatus.RESOLVED,
        reason_code="evidence_gap_supported_observation",
        metric=entry.name,
        datasource_uid=entry.datasource_uid,
        datasource_type=entry.datasource_type,
        query_language=entry.query_language,
        semantic_score=score,
        ownership_score=1.0,
    )
