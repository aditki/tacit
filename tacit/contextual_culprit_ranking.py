"""Synthetic context-bundle culprit ranking.

This module is deliberately integration-free. It lets tests pretend that
service catalogs, runbooks, incident history, recent changes, and evidence
observations already exist, then evaluates only:

ContextBundle + EvidenceObservations -> RankedSuspects

The output is a suspect ranking, not root-cause analysis. Every ranked entity
uses ``causal_status="suspect_not_proven"`` until a future evaluator explicitly
adds stronger proof semantics.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Literal

from pydantic import BaseModel, Field

CAUSAL_STATUS = "suspect_not_proven"


class IncidentInput(BaseModel):
    symptom: str
    affected_service: str


class ServiceContext(BaseModel):
    name: str
    depends_on: list[str] = Field(default_factory=list)
    owner: str = ""


class RunbookHint(BaseModel):
    symptom: str
    suspects: list[str] = Field(default_factory=list)
    stale: bool = False
    source: str = "runbook"


class HistoricalIncident(BaseModel):
    symptom: str
    culprit: str
    evidence: list[str] = Field(default_factory=list)
    source: str = "incident_history"


class RecentChange(BaseModel):
    service: str
    time_delta_minutes: int
    summary: str = ""
    source: str = "deployments"


class DashboardAssociation(BaseModel):
    service: str
    signals: list[str] = Field(default_factory=list)
    source: str = "dashboard_catalog"


class EvidenceObservationInput(BaseModel):
    signal: str
    status: Literal["abnormal", "normal", "missing", "unknown"] = "unknown"
    related_entity: str = ""


class ContextSection(BaseModel):
    services: list[ServiceContext] = Field(default_factory=list)
    runbook_hints: list[RunbookHint] = Field(default_factory=list)
    historical_incidents: list[HistoricalIncident] = Field(default_factory=list)
    recent_changes: list[RecentChange] = Field(default_factory=list)
    dashboards: list[DashboardAssociation] = Field(default_factory=list)


class EvidenceSection(BaseModel):
    observations: list[EvidenceObservationInput] = Field(default_factory=list)


class ContextBundle(BaseModel):
    incident: IncidentInput
    context: ContextSection = Field(default_factory=ContextSection)
    evidence: EvidenceSection = Field(default_factory=EvidenceSection)


class RankingReason(BaseModel):
    type: str
    source: str
    confidence: float = Field(ge=0.0, le=1.0)
    detail: str = ""


class RankedSuspect(BaseModel):
    entity: str
    rank: int
    score: float = Field(ge=0.0, le=1.0)
    reasons: list[RankingReason]
    causal_status: str = CAUSAL_STATUS


class RankedSuspectsResult(BaseModel):
    suspects: list[RankedSuspect]
    abstained: bool = True
    abstention_reason: str = ""


@dataclass
class _Candidate:
    entity: str
    raw_score: float = 0.0
    reasons: list[RankingReason] = field(default_factory=list)
    has_runtime_support: bool = False


WEIGHTS = {
    "runtime_observation_match": 40,
    "direct_dependency": 25,
    "recent_deploy": 20,
    "runbook_match": 15,
    "historical_incident_match": 15,
    "ownership_context": 5,
    "dashboard_association": 5,
    "stale_artifact": -20,
    "contradictory_evidence": -30,
}


def _norm(value: str) -> str:
    return value.strip().lower()


def _tokens(value: str) -> set[str]:
    return {part for part in _norm(value).replace("-", " ").replace("_", " ").split() if len(part) > 2}


def _symptom_matches(actual: str, expected: str) -> bool:
    actual_tokens = _tokens(actual)
    expected_tokens = _tokens(expected)
    if not actual_tokens or not expected_tokens:
        return False
    return len(actual_tokens & expected_tokens) / len(expected_tokens) >= 0.5


def _candidate(candidates: dict[str, _Candidate], entity: str) -> _Candidate:
    normalized = entity.strip()
    if normalized not in candidates:
        candidates[normalized] = _Candidate(entity=normalized)
    return candidates[normalized]


def _add_reason(candidate: _Candidate, *, reason_type: str, source: str, confidence: float, detail: str = "") -> None:
    reason = RankingReason(type=reason_type, source=source, confidence=confidence, detail=detail)
    if reason not in candidate.reasons:
        candidate.reasons.append(reason)


def _connected_entities(bundle: ContextBundle) -> set[str]:
    affected = bundle.incident.affected_service
    connected = {affected}
    for service in bundle.context.services:
        if service.name == affected:
            connected.update(service.depends_on)
    return connected


def _dependency_types(bundle: ContextBundle) -> dict[str, set[str]]:
    kinds: dict[str, set[str]] = defaultdict(set)
    for entity in _connected_entities(bundle):
        lowered = _norm(entity)
        if "db" in lowered or "postgres" in lowered or "mysql" in lowered:
            kinds["database"].add(entity)
            kinds["db"].add(entity)
        if "redis" in lowered or "cache" in lowered:
            kinds["cache"].add(entity)
            kinds["redis"].add(entity)
        if "queue" in lowered or "kafka" in lowered:
            kinds["queue"].add(entity)
    return kinds


def _entities_for_observation(bundle: ContextBundle, observation: EvidenceObservationInput) -> set[str]:
    if observation.related_entity:
        return {observation.related_entity}
    signal_tokens = _tokens(observation.signal)
    kinds = _dependency_types(bundle)
    matched: set[str] = set()
    for token in signal_tokens:
        matched.update(kinds.get(token, set()))
    return matched


def rank_context_bundle(bundle: ContextBundle) -> RankedSuspectsResult:
    """Return ranked suspects from normalized synthetic context."""
    candidates: dict[str, _Candidate] = {}
    affected = bundle.incident.affected_service
    connected = _connected_entities(bundle)

    for service in bundle.context.services:
        if service.name != affected:
            continue
        for dependency in service.depends_on:
            cand = _candidate(candidates, dependency)
            cand.raw_score += WEIGHTS["direct_dependency"]
            _add_reason(
                cand,
                reason_type="dependency_match",
                source="service_graph",
                confidence=0.8,
                detail=f"{affected} depends on {dependency}",
            )

    for hint in bundle.context.runbook_hints:
        if not _symptom_matches(bundle.incident.symptom, hint.symptom):
            continue
        for suspect in hint.suspects:
            cand = _candidate(candidates, suspect)
            if hint.stale:
                cand.raw_score += WEIGHTS["stale_artifact"]
                _add_reason(
                    cand,
                    reason_type="stale_artifact",
                    source=hint.source,
                    confidence=0.2,
                    detail="Runbook hint is marked stale",
                )
            elif suspect in connected:
                cand.raw_score += WEIGHTS["runbook_match"]
                _add_reason(
                    cand,
                    reason_type="runbook_match",
                    source=hint.source,
                    confidence=0.6,
                    detail=f"Runbook names {suspect} for {hint.symptom}",
                )
            else:
                _add_reason(
                    cand,
                    reason_type="unconnected_runbook_mention",
                    source=hint.source,
                    confidence=0.2,
                    detail=f"{suspect} is not connected to {affected}",
                )

    for incident in bundle.context.historical_incidents:
        if not _symptom_matches(bundle.incident.symptom, incident.symptom):
            continue
        if incident.culprit not in connected:
            continue
        cand = _candidate(candidates, incident.culprit)
        cand.raw_score += WEIGHTS["historical_incident_match"]
        _add_reason(
            cand,
            reason_type="historical_incident_match",
            source=incident.source,
            confidence=0.7,
            detail="; ".join(incident.evidence),
        )

    for change in bundle.context.recent_changes:
        if change.service not in connected:
            continue
        if change.time_delta_minutes > 60:
            continue
        cand = _candidate(candidates, change.service)
        cand.raw_score += WEIGHTS["recent_deploy"]
        _add_reason(
            cand,
            reason_type="recent_change",
            source=change.source,
            confidence=0.65,
            detail=f"{change.summary} ({change.time_delta_minutes}m ago)",
        )

    for dashboard in bundle.context.dashboards:
        if dashboard.service not in candidates:
            continue
        cand = candidates[dashboard.service]
        if cand.reasons:
            cand.raw_score += WEIGHTS["dashboard_association"]
            _add_reason(
                cand,
                reason_type="dashboard_association",
                source=dashboard.source,
                confidence=0.2,
                detail=", ".join(dashboard.signals),
            )

    for observation in bundle.evidence.observations:
        matched = _entities_for_observation(bundle, observation)
        if observation.status == "abnormal":
            for entity in matched:
                if entity not in connected:
                    continue
                cand = _candidate(candidates, entity)
                cand.raw_score += WEIGHTS["runtime_observation_match"]
                cand.has_runtime_support = True
                _add_reason(
                    cand,
                    reason_type="runtime_observation_match",
                    source="evidence_observations",
                    confidence=0.9,
                    detail=f"{observation.signal} is abnormal",
                )
        elif observation.status == "normal":
            for entity in matched:
                if entity not in candidates:
                    continue
                cand = candidates[entity]
                cand.raw_score += WEIGHTS["contradictory_evidence"]
                _add_reason(
                    cand,
                    reason_type="contradictory_evidence",
                    source="evidence_observations",
                    confidence=0.8,
                    detail=f"{observation.signal} is normal",
                )

    for service in bundle.context.services:
        if not service.owner or service.name not in candidates:
            continue
        cand = candidates[service.name]
        if cand.reasons:
            cand.raw_score += WEIGHTS["ownership_context"]
            _add_reason(
                cand,
                reason_type="ownership_context",
                source="service_catalog",
                confidence=0.2,
                detail=f"Owner: {service.owner}",
            )

    ranked = sorted(
        (candidate for candidate in candidates.values() if candidate.raw_score > 0 and candidate.reasons),
        key=lambda candidate: (-candidate.raw_score, candidate.entity),
    )
    suspects = [
        RankedSuspect(
            entity=candidate.entity,
            rank=index,
            score=round(min(candidate.raw_score / 80.0, 1.0), 4),
            reasons=candidate.reasons,
        )
        for index, candidate in enumerate(ranked, start=1)
    ]
    runtime_supported = any(candidate.has_runtime_support for candidate in ranked)
    if not suspects:
        return RankedSuspectsResult(
            suspects=[],
            abstained=True,
            abstention_reason="no_context_points_to_culprit",
        )
    if not runtime_supported:
        return RankedSuspectsResult(
            suspects=suspects,
            abstained=True,
            abstention_reason="suspects_ranked_without_runtime_proof",
        )
    return RankedSuspectsResult(suspects=suspects, abstained=True, abstention_reason="suspect_not_proven")
