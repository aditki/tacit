"""Deterministic culprit-suspect ranking from investigation context.

The ranking here is intentionally conservative. It produces ordered suspects
and evidence provenance, but it does not assert root cause. A ranking becomes
``telemetry_evidenced`` only when at least one evidence requirement survived as
a validated, non-empty observation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from tacit.evidence import build_evidence_records
from tacit.models.schemas import (
    CulpritCandidate,
    CulpritRanking,
    CulpritRankingMode,
    DashboardSpec,
    EvidenceLifecycleStatus,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
)


@dataclass
class _CandidateAccumulator:
    suspect: str
    suspect_type: str
    score: float = 0.0
    contextual_reasons: list[str] = field(default_factory=list)
    runtime_evidence: list[str] = field(default_factory=list)
    missing_evidence: list[str] = field(default_factory=list)
    supporting_requirement_ids: list[str] = field(default_factory=list)
    contradicting_requirement_ids: list[str] = field(default_factory=list)
    missing_requirement_ids: list[str] = field(default_factory=list)


_DATASTORE_SIGNALS = {"db_query_latency", "db_connection_pool"}
_CACHE_PREFIXES = ("cache_",)
_QUEUE_SIGNALS = {"queue_depth", "consumer_lag", "message_throughput"}
_RESOURCE_SIGNALS = {
    "cpu_usage",
    "memory_usage",
    "disk_usage",
    "network_bytes",
    "io_wait",
    "pod_restarts",
    "in_flight_requests",
    "concurrent_executions",
}
_SERVICE_SIGNALS = {"request_latency", "api_latency", "request_rate", "error_rate", "rate_limit_hits"}


def _service_label(services: list[str]) -> str:
    if not services:
        return ""
    if len(services) == 1:
        return services[0]
    return ", ".join(services)


def _suspect_for_requirement(requirement: EvidenceRequirement, services: list[str]) -> tuple[str, str]:
    signal = requirement.signal_type
    metric = requirement.default_metric
    service = _service_label(requirement.service_scope or services)
    service_prefix = f"{service} " if service else ""

    if signal in _DATASTORE_SIGNALS or metric.startswith("db_"):
        return f"{service_prefix}database".strip().title(), "datastore"
    if signal.startswith(_CACHE_PREFIXES) or "redis" in metric or "cache" in metric:
        return f"{service_prefix}cache".strip().title(), "cache"
    if signal in _QUEUE_SIGNALS or "queue" in metric or "consumer_lag" in metric:
        return f"{service_prefix}queue".strip().title(), "queue"
    if signal in _RESOURCE_SIGNALS or any(token in metric for token in ("cpu", "memory", "disk", "network")):
        return f"{service_prefix}runtime resources".strip().title(), "resource"
    if signal in _SERVICE_SIGNALS and service:
        return service, "service"
    if service:
        return service, "service"
    if signal:
        return signal.replace("_", " ").title(), "unknown"
    if metric:
        return metric, "unknown"
    return "Unspecified component", "unknown"


def _confidence(score: float, *, has_runtime_evidence: bool) -> str:
    if has_runtime_evidence and score >= 0.75:
        return "high"
    if score >= 0.5:
        return "medium"
    return "low"


def _add_unique(items: list[str], value: str) -> None:
    if value and value not in items:
        items.append(value)


def _archetype_names(ranked_archetypes: list[tuple[Any, float]]) -> dict[str, str]:
    names: dict[str, str] = {}
    for archetype, _ in ranked_archetypes:
        arch_id = getattr(archetype, "id", "")
        if not arch_id:
            continue
        names[arch_id] = getattr(archetype, "name", arch_id)
    return names


def _has_runtime_evidence(candidate: _CandidateAccumulator) -> bool:
    return bool(candidate.runtime_evidence)


def rank_culprits(
    *,
    intent: Intent,
    dashboard_spec: DashboardSpec,
    ranked_archetypes: list[tuple[Any, float]],
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    evidence_observations: list[EvidenceObservation],
) -> CulpritRanking:
    """Rank suspects using selected context plus validated evidence observations."""
    candidates: dict[tuple[str, str], _CandidateAccumulator] = {}
    archetype_names = _archetype_names(ranked_archetypes)
    records = build_evidence_records(evidence_requirements, evidence_resolutions, evidence_observations)

    def get_candidate(suspect: str, suspect_type: str) -> _CandidateAccumulator:
        key = (suspect, suspect_type)
        if key not in candidates:
            candidates[key] = _CandidateAccumulator(suspect=suspect, suspect_type=suspect_type)
        return candidates[key]

    for service in intent.services:
        candidate = get_candidate(service, "service")
        candidate.score += 0.2
        _add_unique(candidate.contextual_reasons, "Mentioned in the incident prompt")

    for archetype, confidence in ranked_archetypes:
        arch_id = getattr(archetype, "id", "")
        name = getattr(archetype, "name", arch_id or "selected investigation pattern")
        services = intent.services or [""]
        for service in services:
            suspect = service or name
            suspect_type = "service" if service else "unknown"
            candidate = get_candidate(suspect, suspect_type)
            candidate.score += min(max(confidence, 0.0), 1.0) * 0.15
            _add_unique(candidate.contextual_reasons, f"Selected investigation pattern: {name}")

    for record in records:
        requirement = record.requirement
        suspect, suspect_type = _suspect_for_requirement(requirement, intent.services)
        candidate = get_candidate(suspect, suspect_type)
        source_name = archetype_names.get(requirement.source, requirement.source)
        if source_name:
            _add_unique(candidate.contextual_reasons, f"Required by {source_name}")
        elif requirement.signal_type:
            _add_unique(candidate.contextual_reasons, f"Required signal: {requirement.signal_type}")
        candidate.score += 0.2

        resolution = record.gap_resolution or record.primary_resolution
        observation = record.observation
        signal_label = requirement.signal_type or requirement.default_metric or "evidence"

        if record.final_status == EvidenceLifecycleStatus.SUPPORTED_OBSERVATION and observation is not None:
            metric = observation.resolution_metric or (resolution.metric if resolution else "")
            panel = f" in '{observation.panel_title}'" if observation.panel_title else ""
            _add_unique(candidate.runtime_evidence, f"Observed {signal_label} via {metric}{panel}".strip())
            _add_unique(candidate.supporting_requirement_ids, requirement.id)
            candidate.score += 0.45 if requirement.priority == "critical" else 0.25
        elif resolution is not None:
            reason = observation.rejection_reason if observation is not None else resolution.reason_code
            _add_unique(candidate.missing_evidence, f"{signal_label}: {reason}")
            if record.final_status == EvidenceLifecycleStatus.NEGATIVE_EVIDENCE:
                _add_unique(candidate.contradicting_requirement_ids, requirement.id)
            else:
                _add_unique(candidate.missing_requirement_ids, requirement.id)
            if resolution.status.value == "resolved":
                candidate.score += 0.1

    if not candidates and dashboard_spec.panels:
        candidate = get_candidate(dashboard_spec.title, "unknown")
        candidate.score = 0.2
        _add_unique(candidate.contextual_reasons, "Generated investigation artifact without declared evidence records")

    ranked_accumulators = sorted(
        candidates.values(),
        key=lambda candidate: (
            bool(candidate.runtime_evidence),
            candidate.score,
            len(candidate.contextual_reasons),
            candidate.suspect,
        ),
        reverse=True,
    )

    ranked_candidates = [
        CulpritCandidate(
            rank=index,
            suspect=candidate.suspect,
            suspect_type=candidate.suspect_type,
            score=round(min(candidate.score, 1.0), 4),
            confidence=_confidence(candidate.score, has_runtime_evidence=_has_runtime_evidence(candidate)),
            contextual_reasons=candidate.contextual_reasons,
            runtime_evidence=candidate.runtime_evidence,
            missing_evidence=candidate.missing_evidence,
            supporting_requirement_ids=candidate.supporting_requirement_ids,
            contradicting_requirement_ids=candidate.contradicting_requirement_ids,
            missing_requirement_ids=candidate.missing_requirement_ids,
        )
        for index, candidate in enumerate(ranked_accumulators, start=1)
    ]
    has_supported_runtime = any(candidate.runtime_evidence for candidate in ranked_accumulators)
    mode = CulpritRankingMode.TELEMETRY_EVIDENCED if has_supported_runtime else CulpritRankingMode.CONTEXTUAL
    evidence_sources = ["Operational context"]
    if has_supported_runtime:
        evidence_sources.append("Validated runtime observations")

    if not ranked_candidates:
        return CulpritRanking(
            mode=CulpritRankingMode.CONTEXTUAL,
            abstained=True,
            abstention_reason="no_contextual_or_runtime_candidates",
            candidates=[],
            evidence_sources=[],
            telemetry_status="not_checked",
        )

    abstained = not has_supported_runtime
    return CulpritRanking(
        mode=mode,
        abstained=abstained,
        abstention_reason="no_supported_runtime_evidence" if abstained else "",
        candidates=ranked_candidates,
        evidence_sources=evidence_sources,
        telemetry_status="supported" if has_supported_runtime else "not_evidenced",
    )
