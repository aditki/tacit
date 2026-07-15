"""Captured inputs used for deterministic Investigation Contract replay."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from tacit.investigation_contract import (
    CorrectionReference,
    DecisionLogEntry,
    ProvenanceRecord,
    RuntimeManifest,
    stamp_fingerprints,
    utc_now,
)
from tacit.models.schemas import (
    ContextChunk,
    CulpritRanking,
    DashboardSpec,
    DashRequest,
    EvidenceObservation,
    EvidenceObservationOutcome,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
)


class ReplayMode(StrEnum):
    EXACT = "exact"
    CURRENT_ENGINE = "current_engine"
    COUNTERFACTUAL = "counterfactual"


class CounterfactualChanges(BaseModel):
    """Controlled changes allowed during an offline counterfactual replay."""

    remove_observation_ids: list[str] = Field(default_factory=list)
    reject_requirement_ids: list[str] = Field(default_factory=list)
    remove_candidate_refs: list[str] = Field(default_factory=list)
    remove_context_refs: list[str] = Field(default_factory=list)
    stale_context_refs: list[str] = Field(default_factory=list)
    add_context_chunks: list[ContextChunk] = Field(default_factory=list)
    candidate_score_overrides: dict[str, float] = Field(default_factory=dict)

    @field_validator("candidate_score_overrides")
    @classmethod
    def validate_score_overrides(cls, value: dict[str, float]) -> dict[str, float]:
        if any(score < 0 or score > 1 for score in value.values()):
            raise ValueError("counterfactual candidate scores must be between 0 and 1")
        return value


class InvestigationReplaySnapshot(BaseModel):
    """Typed, portable capture of all inputs needed to rebuild a contract.

    This object intentionally contains no live clients or external-system
    locators that replay would need to dereference.
    """

    snapshot_version: Literal["1.0"] = "1.0"
    investigation_id: str
    revision: int = Field(default=0, ge=0)
    captured_at: datetime = Field(default_factory=utc_now)
    created_at: datetime
    completed_at: datetime | None = None
    request: DashRequest
    intent: Intent
    dashboard_spec: DashboardSpec
    evidence_requirements: list[EvidenceRequirement] = Field(default_factory=list)
    evidence_resolutions: list[EvidenceResolution] = Field(default_factory=list)
    resolution_candidates: list[dict[str, Any]] = Field(default_factory=list)
    evidence_observations: list[EvidenceObservation] = Field(default_factory=list)
    culprit_ranking: CulpritRanking = Field(default_factory=CulpritRanking)
    context_chunks: list[ContextChunk] = Field(default_factory=list)
    renderings: dict[str, Any] = Field(default_factory=dict)
    external_errors: list[dict[str, Any]] = Field(default_factory=list)
    model_inputs: dict[str, Any] = Field(default_factory=dict)
    model_outputs: dict[str, Any] = Field(default_factory=dict)
    query_results: list[dict[str, Any]] = Field(default_factory=list)
    runtime: RuntimeManifest = Field(default_factory=RuntimeManifest)
    corrections: list[CorrectionReference] = Field(default_factory=list)
    additional_provenance: list[ProvenanceRecord] = Field(default_factory=list)
    additional_decisions: list[DecisionLogEntry] = Field(default_factory=list)


def rebuild_contract(
    snapshot: InvestigationReplaySnapshot,
    *,
    mode: ReplayMode = ReplayMode.EXACT,
    changes: CounterfactualChanges | None = None,
):
    """Rebuild a contract solely from captured inputs."""
    from tacit.investigation_contract import InvestigationContractAssembler

    changes = changes or CounterfactualChanges()
    if mode == ReplayMode.COUNTERFACTUAL:
        snapshot = apply_counterfactual(snapshot, changes)

    dashboard = snapshot.renderings.get("dashboard", {})
    signalfx = snapshot.renderings.get("signalfx", {})
    runtime = snapshot.runtime if mode == ReplayMode.EXACT else RuntimeManifest()
    contract = InvestigationContractAssembler().from_pipeline(
        investigation_id=snapshot.investigation_id,
        revision=snapshot.revision,
        parent_revision=snapshot.revision - 1 or None,
        request=snapshot.request,
        intent=snapshot.intent,
        dashboard_spec=snapshot.dashboard_spec,
        evidence_requirements=snapshot.evidence_requirements,
        evidence_resolutions=snapshot.evidence_resolutions,
        evidence_observations=snapshot.evidence_observations,
        culprit_ranking=snapshot.culprit_ranking,
        context_chunks=snapshot.context_chunks,
        warnings=[
            str(error.get("detail", ""))
            for error in snapshot.external_errors
            if error.get("type") == "validation_warning"
        ],
        dashboard_url=str(dashboard.get("dashboard_url", "")),
        dashboard_uid=str(dashboard.get("dashboard_uid", "")),
        signalfx_url=str(signalfx.get("dashboard_url", "")),
        signalfx_dashboard_id=str(signalfx.get("dashboard_id", "")),
        created_at=snapshot.created_at,
        completed_at=snapshot.completed_at,
        runtime_manifest=runtime,
    )
    if snapshot.corrections or snapshot.additional_provenance or snapshot.additional_decisions:
        contract = contract.model_copy(
            update={
                "corrections": snapshot.corrections,
                "provenance": [*contract.provenance, *snapshot.additional_provenance],
                "decision_log": [*contract.decision_log, *snapshot.additional_decisions],
            }
        )
    return stamp_fingerprints(contract)


def apply_counterfactual(
    snapshot: InvestigationReplaySnapshot,
    changes: CounterfactualChanges,
) -> InvestigationReplaySnapshot:
    removed_observation_ids = set(changes.remove_observation_ids)
    rejected_requirement_ids = set(changes.reject_requirement_ids)
    observations = []
    for index, observation in enumerate(snapshot.evidence_observations, start=1):
        if observation.requirement_id in rejected_requirement_ids:
            continue
        if f"obs_{index:02d}" in removed_observation_ids:
            observation = observation.model_copy(
                update={
                    "outcome": EvidenceObservationOutcome.MISSING_EVIDENCE,
                    "query": "",
                    "valid_query": False,
                    "non_empty": False,
                    "survived": False,
                    "rejection_reason": "counterfactual_observation_removed",
                }
            )
        observations.append(observation)
    resolutions = [
        (
            resolution.model_copy(
                update={
                    "status": EvidenceResolutionStatus.UNRESOLVED,
                    "reason_code": "counterfactual_binding_rejected",
                }
            )
            if resolution.requirement_id in rejected_requirement_ids
            else resolution
        )
        for resolution in snapshot.evidence_resolutions
    ]
    candidates = []
    for candidate in snapshot.culprit_ranking.candidates:
        candidate_ref = f"{candidate.suspect_type}:{candidate.suspect}"
        if candidate_ref in changes.remove_candidate_refs:
            continue
        if candidate_ref in changes.candidate_score_overrides:
            candidate = candidate.model_copy(update={"score": changes.candidate_score_overrides[candidate_ref]})
        candidates.append(candidate)
    candidates.sort(key=lambda candidate: (-candidate.score, candidate.rank, candidate.suspect_type, candidate.suspect))
    candidates = [candidate.model_copy(update={"rank": rank}) for rank, candidate in enumerate(candidates, start=1)]
    supported_requirements = {
        observation.requirement_id
        for observation in observations
        if observation.outcome == EvidenceObservationOutcome.SUPPORTED_OBSERVATION
    }
    has_candidate_support = any(
        supported_requirements.intersection(candidate.supporting_requirement_ids) for candidate in candidates
    )
    abstained = snapshot.culprit_ranking.abstained or not candidates or not has_candidate_support
    abstention_reason = snapshot.culprit_ranking.abstention_reason
    if abstained and not abstention_reason:
        abstention_reason = "counterfactual_removed_runtime_support"
    culprit_ranking = snapshot.culprit_ranking.model_copy(
        update={
            "candidates": candidates,
            "abstained": abstained,
            "abstention_reason": abstention_reason,
            "telemetry_status": (
                snapshot.culprit_ranking.telemetry_status
                if has_candidate_support
                else "counterfactual_evidence_removed"
            ),
        }
    )
    removed_context_refs = set(changes.remove_context_refs)
    stale_context_refs = set(changes.stale_context_refs)
    context_chunks = []
    used_context_refs: set[str] = set()
    for index, chunk in enumerate(snapshot.context_chunks, start=1):
        provenance_id = str(chunk.metadata.get("provenance_id") or f"prov_context_{index:02d}")
        used_context_refs.add(provenance_id)
        if provenance_id in removed_context_refs:
            continue
        metadata = {**chunk.metadata, "provenance_id": provenance_id}
        if provenance_id in stale_context_refs:
            metadata["stale"] = True
        context_chunks.append(chunk.model_copy(update={"metadata": metadata}))
    next_context_index = len(snapshot.context_chunks) + 1
    for chunk in changes.add_context_chunks:
        provenance_id = str(chunk.metadata.get("provenance_id", ""))
        while not provenance_id or provenance_id in used_context_refs:
            provenance_id = f"prov_context_{next_context_index:02d}"
            next_context_index += 1
        used_context_refs.add(provenance_id)
        context_chunks.append(chunk.model_copy(update={"metadata": {**chunk.metadata, "provenance_id": provenance_id}}))
    return snapshot.model_copy(
        update={
            "evidence_resolutions": resolutions,
            "evidence_observations": observations,
            "culprit_ranking": culprit_ranking,
            "context_chunks": context_chunks,
        }
    )
