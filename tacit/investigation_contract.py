"""Versioned Investigation Contract v1.

The contract is Tacit's canonical product object. Dashboard URLs, Slack
messages, CLI text, and future agent responses are renderings of this object.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from importlib.resources import files
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from tacit import __version__
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

SCHEMA_NAME = "tacit.investigation"
SCHEMA_VERSION = "1.0"
SCHEMA_ID = f"urn:tacit:schema:investigation:{SCHEMA_VERSION}"
_SCHEMA_FILES = {SCHEMA_VERSION: f"v{SCHEMA_VERSION}.schema.json"}


def load_investigation_contract_schema(version: str = SCHEMA_VERSION) -> dict[str, Any]:
    """Load a supported Investigation Contract JSON Schema from package data."""
    try:
        filename = _SCHEMA_FILES[version]
    except KeyError as exc:
        supported = ", ".join(sorted(_SCHEMA_FILES))
        raise ValueError(f"Unsupported investigation schema version {version!r}; supported: {supported}") from exc

    resource = files("tacit.schemas.investigation").joinpath(filename)
    return json.loads(resource.read_text(encoding="utf-8"))


class InvestigationLifecycleStatus(StrEnum):
    CREATED = "created"
    RESOLVING = "resolving"
    OBSERVING = "observing"
    RANKING = "ranking"
    GROUNDING = "grounding"
    COMPLETED = "completed"
    FAILED_RESOLUTION = "failed_resolution"
    FAILED_OBSERVATION = "failed_observation"
    FAILED_RANKING = "failed_ranking"
    FAILED_VALIDATION = "failed_validation"
    CANCELLED = "cancelled"


class GroundingStatus(StrEnum):
    SUPPORTED = "supported"
    PARTIALLY_SUPPORTED = "partially_supported"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    CONTRADICTED = "contradicted"
    INDETERMINATE = "indeterminate"


class InvestigationRunType(StrEnum):
    INITIAL = "initial"
    REPLAY = "replay"
    REFRESH = "refresh"
    CORRECTION_APPLICATION = "correction_application"
    MIGRATION = "migration"


class KnowledgeCandidateStatus(StrEnum):
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"
    APPLIED = "applied"


class CausalStatus(StrEnum):
    PROVEN = "proven"
    SUSPECT_NOT_PROVEN = "suspect_not_proven"
    CONTRADICTED = "contradicted"
    INSUFFICIENT_EVIDENCE = "insufficient_evidence"
    INDETERMINATE = "indeterminate"


class SchemaInfo(BaseModel):
    name: Literal["tacit.investigation"] = "tacit.investigation"
    version: Literal["1.0"] = "1.0"


class TimeWindow(BaseModel):
    start: datetime | None = None
    end: datetime | None = None
    label: str = ""


class InvestigationScope(BaseModel):
    services: list[str] = Field(default_factory=list)
    service_ids: list[str] = Field(default_factory=list)
    environments: list[str] = Field(default_factory=list)
    regions: list[str] = Field(default_factory=list)


class InvestigationMetadata(BaseModel):
    id: str
    revision: int = Field(default=0, ge=0)
    parent_revision: int | None = None
    created_at: datetime
    completed_at: datetime | None = None
    status: InvestigationLifecycleStatus = InvestigationLifecycleStatus.CREATED


class InvestigationRequestContract(BaseModel):
    question: str
    normalized_intent: str = ""
    requester: str = ""
    time_window: TimeWindow = Field(default_factory=TimeWindow)
    scope: InvestigationScope = Field(default_factory=InvestigationScope)


class OperationalIR(BaseModel):
    summary: str = ""
    domain: str = ""
    services: list[str] = Field(default_factory=list)
    signals: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    problem_type: str = ""
    archetypes: list[dict[str, Any]] = Field(default_factory=list)
    context_refs: list[str] = Field(default_factory=list)


class EvidenceRequirementContract(BaseModel):
    id: str
    type: str
    concept: str = ""
    entity_ref: str = ""
    required_for: list[str] = Field(default_factory=list)
    priority: str = "required"
    resolution_status: str = "unknown"
    provenance_refs: list[str] = Field(default_factory=list)


class EvidenceResolutionContract(BaseModel):
    id: str
    requirement_ref: str
    status: str
    backend: str = ""
    binding: dict[str, Any] = Field(default_factory=dict)
    rejected_bindings: list[dict[str, Any]] = Field(default_factory=list)
    resolution_method: str = ""
    confidence: str = "unknown"
    provenance_refs: list[str] = Field(default_factory=list)


class QueryRecord(BaseModel):
    id: str
    backend: str = ""
    language: str = ""
    expression: str = ""
    generated_by: str = "evidence_resolver"
    validation: dict[str, Any] = Field(default_factory=dict)
    execution: dict[str, Any] = Field(default_factory=dict)
    provenance_refs: list[str] = Field(default_factory=list)


class ObservationContract(BaseModel):
    id: str
    subject_ref: str = ""
    type: str = ""
    statement: str = ""
    value: dict[str, Any] = Field(default_factory=dict)
    status: str = "missing"
    time_window: TimeWindow = Field(default_factory=TimeWindow)
    query_refs: list[str] = Field(default_factory=list)
    provenance_refs: list[str] = Field(default_factory=list)


class CandidateRankingContract(BaseModel):
    candidate_ref: str
    rank: int = Field(ge=1)
    score: float = Field(ge=0.0, le=1.0)
    causal_status: CausalStatus = CausalStatus.SUSPECT_NOT_PROVEN
    supporting_observation_refs: list[str] = Field(default_factory=list)
    contradicting_observation_refs: list[str] = Field(default_factory=list)
    missing_requirement_refs: list[str] = Field(default_factory=list)
    contribution_refs: list[str] = Field(default_factory=list)
    contextual_reasons: list[str] = Field(default_factory=list)
    runtime_evidence: list[str] = Field(default_factory=list)


class ArtifactContributionContract(BaseModel):
    id: str
    artifact_ref: str
    target_ref: str
    contribution_type: str
    used_for: list[str] = Field(default_factory=list)
    score_delta: float = 0.0
    status: str = "supported"
    reason: str = ""
    provenance_refs: list[str]


class GroundingContract(BaseModel):
    status: GroundingStatus = GroundingStatus.INDETERMINATE
    confidence: dict[str, Any] = Field(default_factory=lambda: {"level": "contextual", "score": None})
    supported_claims: list[str] = Field(default_factory=list)
    unsupported_claims: list[str] = Field(default_factory=list)
    contradicted_claims: list[str] = Field(default_factory=list)
    missing_observation_refs: list[str] = Field(default_factory=list)
    unsafe_to_conclude: bool = True
    unsafe_conclusions: list[str] = Field(default_factory=list)
    maximum_trustworthy_conclusion: dict[str, Any] = Field(default_factory=dict)
    migration_notes: list[str] = Field(default_factory=list)
    abstained: bool = True
    reason: str = ""


class DecisionLogEntry(BaseModel):
    id: str
    sequence: int = Field(ge=1)
    stage: str
    action: str
    subject_ref: str = ""
    reason_code: str = ""
    reason: str = ""
    inputs: list[str] = Field(default_factory=list)
    output_ref: str = ""
    mechanism: dict[str, Any] = Field(default_factory=dict)
    score_before: float | None = None
    score_after: float | None = None
    output_status: str = ""


class ProvenanceRecord(BaseModel):
    id: str
    source_type: str
    source_ref: str
    source_version: str = ""
    locator: dict[str, Any] = Field(default_factory=dict)
    ingested_at: datetime | None = None
    observed_at: datetime | None = None
    freshness: dict[str, Any] = Field(default_factory=lambda: {"status": "unknown"})
    review_state: str = "unreviewed"


class CorrectionReference(BaseModel):
    correction_ref: str
    applied_in_revision: int | None = None


class RuntimeManifest(BaseModel):
    engine_version: str = __version__
    policy_version: str = "investigation-contract-v1"
    ranking_version: str = "culprit-ranking-v1"
    vocabulary_version: str = "investigation-lifecycle-v1"
    model: dict[str, Any] = Field(default_factory=dict)
    input_fingerprint: str = ""
    output_fingerprint: str = ""


class InvestigationContract(BaseModel):
    schema_: SchemaInfo = Field(default_factory=SchemaInfo, alias="schema")
    investigation: InvestigationMetadata
    request: InvestigationRequestContract
    operational_ir: OperationalIR = Field(default_factory=OperationalIR)
    evidence_requirements: list[EvidenceRequirementContract] = Field(default_factory=list)
    evidence_resolutions: list[EvidenceResolutionContract] = Field(default_factory=list)
    observations: list[ObservationContract] = Field(default_factory=list)
    candidate_rankings: list[CandidateRankingContract] = Field(default_factory=list)
    artifact_contributions: list[ArtifactContributionContract] = Field(default_factory=list)
    grounding: GroundingContract = Field(default_factory=GroundingContract)
    decision_log: list[DecisionLogEntry] = Field(default_factory=list)
    provenance: list[ProvenanceRecord] = Field(default_factory=list)
    queries: list[QueryRecord] = Field(default_factory=list)
    corrections: list[CorrectionReference] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    renderings: dict[str, Any] = Field(default_factory=dict)
    runtime: RuntimeManifest = Field(default_factory=RuntimeManifest)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_reference_integrity(self) -> InvestigationContract:
        requirement_ids = {r.id for r in self.evidence_requirements}
        observation_ids = {o.id for o in self.observations}
        contribution_ids = {c.id for c in self.artifact_contributions}
        provenance_ids = {p.id for p in self.provenance}
        query_ids = {q.id for q in self.queries}

        _require_known_refs(self.operational_ir.context_refs, provenance_ids, "operational_ir")
        for requirement in self.evidence_requirements:
            _require_known_refs(requirement.provenance_refs, provenance_ids, f"requirement {requirement.id}")
        for resolution in self.evidence_resolutions:
            if resolution.requirement_ref not in requirement_ids:
                raise ValueError(f"resolution {resolution.id} references unknown requirement")
            _require_known_refs(resolution.provenance_refs, provenance_ids, f"resolution {resolution.id}")
        for observation in self.observations:
            _require_known_refs(observation.query_refs, query_ids, f"observation {observation.id}")
            _require_known_refs(observation.provenance_refs, provenance_ids, f"observation {observation.id}")
            if observation.status == "observed":
                if not observation.value.get("valid_query") or not observation.value.get("non_empty"):
                    raise ValueError(f"observation {observation.id} cannot be observed without successful telemetry")
                for query_ref in observation.query_refs:
                    query = next(query for query in self.queries if query.id == query_ref)
                    if query.execution.get("status") in {"failed", "empty"}:
                        raise ValueError(f"observation {observation.id} references unsuccessful query {query_ref}")
        for query in self.queries:
            _require_known_refs(query.provenance_refs, provenance_ids, f"query {query.id}")
        _require_known_refs(self.grounding.missing_observation_refs, observation_ids, "grounding")
        _require_known_refs(self.grounding.supported_claims, observation_ids, "grounding supported_claims")
        _require_known_refs(self.grounding.contradicted_claims, observation_ids, "grounding contradicted_claims")
        if self.grounding.status == GroundingStatus.SUPPORTED and (
            self.grounding.missing_observation_refs or self.grounding.contradicted_claims
        ):
            raise ValueError("supported grounding cannot contain missing or contradicted evidence")
        for contribution in self.artifact_contributions:
            if not contribution.provenance_refs:
                raise ValueError(f"contribution {contribution.id} must include provenance")
            _require_known_refs(contribution.provenance_refs, provenance_ids, f"contribution {contribution.id}")
            contribution_provenance = [p for p in self.provenance if p.id in contribution.provenance_refs]
            if contribution.status == "stale" and not any(
                p.freshness.get("status") == "stale" for p in contribution_provenance
            ):
                raise ValueError(f"stale contribution {contribution.id} must expose stale provenance")
            if contribution.status == "rejected" and (contribution.used_for or contribution.score_delta != 0):
                raise ValueError(f"rejected contribution {contribution.id} cannot influence the investigation")
        for ranking in self.candidate_rankings:
            _require_known_refs(ranking.supporting_observation_refs, observation_ids, ranking.candidate_ref)
            _require_known_refs(ranking.contradicting_observation_refs, observation_ids, ranking.candidate_ref)
            _require_known_refs(ranking.missing_requirement_refs, requirement_ids, ranking.candidate_ref)
            _require_known_refs(ranking.contribution_refs, contribution_ids, ranking.candidate_ref)
            if ranking.causal_status == CausalStatus.PROVEN and self.grounding.unsafe_to_conclude:
                raise ValueError("proven causal status cannot coexist with unsafe_to_conclude")
        sequences = [entry.sequence for entry in self.decision_log]
        if sequences != sorted(sequences):
            raise ValueError("decision_log sequence must be ordered")
        conclusion = str(self.grounding.maximum_trustworthy_conclusion.get("text", "")).lower()
        if self.grounding.unsafe_to_conclude and any(
            phrase in conclusion for phrase in (" caused ", " root cause", "proven cause")
        ):
            raise ValueError("maximum trustworthy conclusion contains unsafe causal language")
        return self


class KnowledgeCandidate(BaseModel):
    id: str
    investigation_id: str
    revision: int
    correction_text: str
    target_ref: str = ""
    candidate_type: str = "human_correction"
    status: KnowledgeCandidateStatus = KnowledgeCandidateStatus.PENDING_REVIEW
    created_by: str = ""
    created_at: datetime
    expires_at: datetime | None = None
    provenance: ProvenanceRecord
    reviewed_by: str = ""
    reviewed_at: datetime | None = None
    applied_revision: int | None = None


def _require_known_refs(refs: list[str], known: set[str], subject: str) -> None:
    missing = [ref for ref in refs if ref not in known]
    if missing:
        raise ValueError(f"{subject} references unknown ids: {missing}")


def _observation_requirement_ref(observation: ObservationContract) -> str:
    requirement_ref = observation.value.get("requirement_ref", "")
    return str(requirement_ref) if requirement_ref else observation.id


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def fingerprint(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def input_fingerprint(contract: InvestigationContract) -> str:
    data = contract.model_dump(mode="json", by_alias=True)
    captured_provenance = [
        {
            "id": record["id"],
            "source_type": record["source_type"],
            "source_ref": record["source_ref"],
            "source_version": record["source_version"],
            "locator": record["locator"],
            "freshness_status": record.get("freshness", {}).get("status"),
            "review_state": record["review_state"],
        }
        for record in data["provenance"]
        if record["source_type"] not in {"request", "runtime"}
    ]
    return fingerprint(
        {
            "request": data["request"],
            "operational_ir": data["operational_ir"],
            "evidence_requirements": data["evidence_requirements"],
            "evidence_resolutions": data["evidence_resolutions"],
            "queries": data["queries"],
            "observations": data["observations"],
            "captured_provenance": captured_provenance,
            "policy_version": data["runtime"]["policy_version"],
            "ranking_version": data["runtime"]["ranking_version"],
            "vocabulary_version": data["runtime"]["vocabulary_version"],
            "model": data["runtime"]["model"],
        }
    )


def output_fingerprint(contract: InvestigationContract) -> str:
    return fingerprint(normalized_output_payload(contract))


def normalized_output_payload(contract: InvestigationContract) -> dict[str, Any]:
    """Return contract output with revision-local timestamps removed."""
    data = contract.model_dump(mode="json", by_alias=True)
    data["investigation"]["revision"] = 0
    data["investigation"]["parent_revision"] = None
    data["investigation"]["created_at"] = ""
    data["investigation"]["completed_at"] = ""
    dashboard_references = data.get("renderings", {}).get("dashboard", {}).get("references", {})
    if isinstance(dashboard_references, dict):
        dashboard_references["revision"] = 0
    for provenance in data["provenance"]:
        if provenance["source_type"] not in {"request", "runtime"} and not provenance["id"].startswith("prov_context_"):
            continue
        provenance["ingested_at"] = None
        provenance["observed_at"] = None
        freshness = provenance.get("freshness")
        if isinstance(freshness, dict) and "last_verified_at" in freshness:
            freshness["last_verified_at"] = ""
    data["runtime"]["output_fingerprint"] = ""
    return data


def stamp_fingerprints(contract: InvestigationContract) -> InvestigationContract:
    runtime = contract.runtime.model_copy(update={"input_fingerprint": input_fingerprint(contract)})
    stamped = contract.model_copy(update={"runtime": runtime})
    runtime = stamped.runtime.model_copy(update={"output_fingerprint": output_fingerprint(stamped)})
    return stamped.model_copy(update={"runtime": runtime})


class InvestigationContractAssembler:
    """Normalize current pipeline outputs into the stable v1 contract."""

    def from_legacy_history(self, record: dict[str, Any]) -> InvestigationContract:
        """Migrate legacy history without inventing evidence or provenance."""
        started = datetime.fromtimestamp(float(record.get("started_at") or 0), tz=UTC)
        finished_raw = record.get("finished_at")
        finished = datetime.fromtimestamp(float(finished_raw), tz=UTC) if finished_raw else None
        status = (
            InvestigationLifecycleStatus.COMPLETED
            if record.get("status") == "success"
            else InvestigationLifecycleStatus.FAILED_VALIDATION
        )
        contract = InvestigationContract(
            investigation=InvestigationMetadata(
                id=str(record["id"]),
                created_at=started,
                completed_at=finished,
                status=status,
            ),
            request=InvestigationRequestContract(
                question=str(record.get("prompt", "")),
                normalized_intent=str(record.get("problem_type", "")),
                requester=str(record.get("user_id") or record.get("channel_id") or ""),
                time_window=TimeWindow(label=str(record.get("timerange", ""))),
                scope=InvestigationScope(services=list(record.get("intent_services") or [])),
            ),
            operational_ir=OperationalIR(
                summary=str(record.get("intent_summary", "")),
                domain=str(record.get("intent_domain", "")),
                services=list(record.get("intent_services") or []),
                signals=list(record.get("intent_signals") or []),
                keywords=list(record.get("intent_keywords") or []),
                problem_type=str(record.get("problem_type", "")),
                archetypes=list(record.get("archetypes") or []),
            ),
            grounding=GroundingContract(
                status=GroundingStatus.INDETERMINATE,
                abstained=True,
                reason="Legacy history did not capture sufficient evidence for grounding.",
                migration_notes=[
                    "Migrated from pre-contract history; missing evidence, provenance, "
                    "and replay inputs were not fabricated."
                ],
                maximum_trustworthy_conclusion={
                    "text": "This migrated record preserves historical metadata only.",
                    "causal_status": CausalStatus.INDETERMINATE,
                },
            ),
            warnings=["Legacy history did not capture structured evidence or replay inputs."],
            decision_log=[
                DecisionLogEntry(
                    id="decision_01",
                    sequence=1,
                    stage="migration",
                    action="migrated_legacy_history",
                    reason_code="legacy_history_adapter",
                    reason="Mapped available legacy fields without inferring absent evidence.",
                    mechanism={"type": "migration", "version": "legacy-history-v1"},
                    output_status="indeterminate",
                )
            ],
            renderings={
                "dashboard": {
                    "dashboard_url": str(record.get("dashboard_url", "")),
                    "dashboard_uid": str(record.get("dashboard_uid", "")),
                    "panel_count": int(record.get("panel_count") or 0),
                    "references": {
                        "investigation_id": str(record["id"]),
                        "revision": 0,
                        "requirement_refs": [],
                        "query_refs": [],
                        "observation_refs": [],
                    },
                }
            },
        )
        return stamp_fingerprints(contract)

    def from_pipeline(
        self,
        *,
        investigation_id: str,
        revision: int,
        parent_revision: int | None,
        request: DashRequest,
        intent: Intent,
        dashboard_spec: DashboardSpec,
        evidence_requirements: list[EvidenceRequirement],
        evidence_resolutions: list[EvidenceResolution],
        evidence_observations: list[EvidenceObservation],
        culprit_ranking: CulpritRanking,
        context_chunks: list[ContextChunk] | None = None,
        warnings: list[str] | None = None,
        dashboard_url: str,
        dashboard_uid: str,
        signalfx_url: str = "",
        signalfx_dashboard_id: str = "",
        created_at: datetime | None = None,
        completed_at: datetime | None = None,
        runtime_manifest: RuntimeManifest | None = None,
    ) -> InvestigationContract:
        now = utc_now()
        created_at = created_at or now
        completed_at = completed_at or now
        context_chunks = context_chunks or []
        provenance = self._provenance(
            requester=request.user_id or request.channel_id or "pipeline",
            observed_at=completed_at,
            context_chunks=context_chunks,
        )
        queries = self._queries(dashboard_spec, provenance[1].id)
        requirements = self._requirements(evidence_requirements, evidence_resolutions, provenance[1].id)
        resolutions = self._resolutions(evidence_resolutions, provenance[1].id)
        observations = self._observations(
            evidence_observations,
            intent,
            dashboard_spec,
            queries,
            provenance[1].id,
        )
        rankings = self._rankings(culprit_ranking, observations, requirements)
        grounding = self._grounding(culprit_ranking, observations, requirements, rankings)
        decision_log = self._decision_log(resolutions, rankings, grounding, dashboard_uid)
        context_provenance = provenance[2:]
        contributions = self._artifact_contributions(context_chunks, context_provenance)

        contract = InvestigationContract(
            investigation=InvestigationMetadata(
                id=investigation_id,
                revision=revision,
                parent_revision=parent_revision,
                created_at=created_at,
                completed_at=completed_at,
                status=InvestigationLifecycleStatus.COMPLETED,
            ),
            request=InvestigationRequestContract(
                question=request.prompt,
                normalized_intent=intent.problem_type,
                requester=request.user_id or request.channel_id or "",
                time_window=TimeWindow(label=intent.timerange),
                scope=InvestigationScope(
                    services=intent.services,
                    service_ids=[f"service:{service}" for service in intent.services],
                ),
            ),
            operational_ir=OperationalIR(
                summary=intent.summary,
                domain=intent.domain,
                services=intent.services,
                signals=[signal.value for signal in intent.signals],
                keywords=intent.keywords,
                problem_type=intent.problem_type,
                archetypes=[a.model_dump(mode="json") for a in intent.archetypes],
                context_refs=[record.id for record in context_provenance],
            ),
            evidence_requirements=requirements,
            evidence_resolutions=resolutions,
            observations=observations,
            candidate_rankings=rankings,
            artifact_contributions=contributions,
            grounding=grounding,
            decision_log=decision_log,
            provenance=provenance,
            queries=queries,
            warnings=warnings or [],
            renderings={
                "dashboard": {
                    "dashboard_url": dashboard_url,
                    "dashboard_uid": dashboard_uid,
                    "panel_count": len(dashboard_spec.panels),
                    "references": {
                        "investigation_id": investigation_id,
                        "revision": revision,
                        "requirement_refs": [requirement.id for requirement in requirements],
                        "query_refs": [query.id for query in queries],
                        "observation_refs": [observation.id for observation in observations],
                    },
                },
                "signalfx": {
                    "dashboard_url": signalfx_url,
                    "dashboard_id": signalfx_dashboard_id,
                },
            },
            runtime=runtime_manifest or RuntimeManifest(),
        )
        return stamp_fingerprints(contract)

    def _provenance(
        self,
        *,
        requester: str,
        observed_at: datetime,
        context_chunks: list[ContextChunk],
    ) -> list[ProvenanceRecord]:
        records = [
            ProvenanceRecord(
                id="prov_request",
                source_type="request",
                source_ref=requester or "anonymous",
                observed_at=observed_at,
                freshness={"status": "current", "last_verified_at": observed_at.isoformat()},
                review_state="unreviewed",
            ),
            ProvenanceRecord(
                id="prov_runtime",
                source_type="runtime",
                source_ref="tacit.pipeline",
                source_version=__version__,
                observed_at=observed_at,
                freshness={"status": "current", "last_verified_at": observed_at.isoformat()},
                review_state="system_generated",
            ),
        ]
        for index, chunk in enumerate(context_chunks, start=1):
            freshness_status = str(chunk.metadata.get("freshness_status", "unknown"))
            if chunk.metadata.get("stale") is True:
                freshness_status = "stale"
            records.append(
                ProvenanceRecord(
                    id=f"prov_context_{index:02d}",
                    source_type=str(chunk.metadata.get("source_type", "artifact")),
                    source_ref=chunk.source or f"context:{index}",
                    source_version=fingerprint(
                        {"content": chunk.content, "source": chunk.source, "metadata": chunk.metadata}
                    ),
                    locator={key: value for key, value in chunk.metadata.items() if key != "content"},
                    ingested_at=observed_at,
                    observed_at=observed_at,
                    freshness={"status": freshness_status, "last_verified_at": observed_at.isoformat()},
                    review_state=str(chunk.metadata.get("review_state", "unreviewed")),
                )
            )
        return records

    def _artifact_contributions(
        self,
        context_chunks: list[ContextChunk],
        provenance: list[ProvenanceRecord],
    ) -> list[ArtifactContributionContract]:
        contributions: list[ArtifactContributionContract] = []
        for index, (chunk, record) in enumerate(zip(context_chunks, provenance, strict=True), start=1):
            rejected = record.review_state == "rejected"
            status = "rejected" if rejected else str(record.freshness.get("status", "unknown"))
            if status not in {"rejected", "stale"}:
                status = "supported"
            contributions.append(
                ArtifactContributionContract(
                    id=f"contribution_{index:02d}",
                    artifact_ref=record.source_ref,
                    target_ref="operational_ir",
                    contribution_type="retrieved_context",
                    used_for=[] if rejected else ["intent_enrichment"],
                    score_delta=0.0,
                    status=status,
                    reason=(
                        "Artifact was rejected during review and did not influence the investigation."
                        if rejected
                        else f"Retrieved context was supplied with relevance {chunk.relevance_score:.3f}."
                    ),
                    provenance_refs=[record.id],
                )
            )
        return contributions

    def _requirements(
        self,
        requirements: list[EvidenceRequirement],
        resolutions: list[EvidenceResolution],
        provenance_ref: str,
    ) -> list[EvidenceRequirementContract]:
        status_by_req = {resolution.requirement_id: resolution.status.value for resolution in resolutions}
        records: list[EvidenceRequirementContract] = []
        for requirement in requirements:
            concept = requirement.signal_type or requirement.default_metric or requirement.evidence_type
            entity = requirement.service_scope[0] if requirement.service_scope else ""
            records.append(
                EvidenceRequirementContract(
                    id=requirement.id,
                    type=requirement.evidence_type,
                    concept=concept,
                    entity_ref=f"service:{entity}" if entity else "",
                    required_for=[requirement.source or "investigation_grounding"],
                    priority="required" if requirement.priority == "critical" else requirement.priority,
                    resolution_status=status_by_req.get(requirement.id, "unknown"),
                    provenance_refs=[provenance_ref],
                )
            )
        return records

    def _resolutions(
        self, resolutions: list[EvidenceResolution], provenance_ref: str
    ) -> list[EvidenceResolutionContract]:
        records: list[EvidenceResolutionContract] = []
        for index, resolution in enumerate(resolutions, start=1):
            confidence = "unknown"
            score = max(resolution.semantic_score, resolution.ownership_score)
            if score >= 0.75:
                confidence = "high"
            elif score >= 0.45:
                confidence = "medium"
            elif score > 0:
                confidence = "low"
            records.append(
                EvidenceResolutionContract(
                    id=f"resolution_{index:02d}",
                    requirement_ref=resolution.requirement_id,
                    status=resolution.status.value,
                    backend=resolution.datasource_type,
                    binding={
                        "metric": resolution.metric,
                        "datasource_uid": resolution.datasource_uid,
                        "query_language": resolution.query_language,
                    },
                    rejected_bindings=(
                        []
                        if resolution.status == EvidenceResolutionStatus.RESOLVED or not resolution.metric
                        else [
                            {
                                "metric": resolution.metric,
                                "datasource_uid": resolution.datasource_uid,
                                "reason_code": resolution.reason_code,
                            }
                        ]
                    ),
                    resolution_method=resolution.reason_code,
                    confidence=confidence,
                    provenance_refs=[provenance_ref],
                )
            )
        return records

    def _queries(self, dashboard_spec: DashboardSpec, provenance_ref: str) -> list[QueryRecord]:
        records: list[QueryRecord] = []
        counter = 1
        for panel in dashboard_spec.panels:
            for query in panel.queries:
                status = query.validation_status or ("passed" if query.validation_has_data else "unknown")
                if query.validation_has_data:
                    execution_status = "succeeded"
                elif status.lower() in {"failed", "rejected", "error", "invalid"}:
                    execution_status = "failed"
                elif status.lower() in {"passed", "ok", "empty"}:
                    execution_status = "empty"
                else:
                    execution_status = "not_captured"
                records.append(
                    QueryRecord(
                        id=f"query_{counter:02d}",
                        backend=query.datasource_type,
                        language=query.query_language or query.datasource_type,
                        expression=query.expr,
                        validation={"status": status},
                        execution={
                            "status": execution_status,
                            "result_fingerprint": fingerprint(
                                {
                                    "validation_status": status,
                                    "has_data": query.validation_has_data,
                                }
                            ),
                        },
                        provenance_refs=[provenance_ref],
                    )
                )
                counter += 1
        return records

    def _observations(
        self,
        observations: list[EvidenceObservation],
        intent: Intent,
        dashboard_spec: DashboardSpec,
        queries: list[QueryRecord],
        provenance_ref: str,
    ) -> list[ObservationContract]:
        entity = f"service:{intent.services[0]}" if intent.services else ""
        exact_query_refs: dict[tuple[str, str, str], str] = {}
        datasource_query_refs: dict[tuple[str, str], str] = {}
        expression_query_refs: dict[str, str] = {}
        query_index = 0
        for panel in dashboard_spec.panels:
            for panel_query in panel.queries:
                query_record = queries[query_index]
                expression = panel_query.expr.strip()
                exact_query_refs.setdefault((panel.title, expression, panel_query.datasource_uid), query_record.id)
                datasource_query_refs.setdefault((expression, panel_query.datasource_uid), query_record.id)
                expression_query_refs.setdefault(expression, query_record.id)
                query_index += 1

        records: list[ObservationContract] = []
        for index, observation in enumerate(observations, start=1):
            if observation.outcome == EvidenceObservationOutcome.SUPPORTED_OBSERVATION:
                status = "observed"
                statement = f"Evidence requirement {observation.requirement_id} is supported by validated telemetry."
            elif observation.outcome == EvidenceObservationOutcome.NEGATIVE_EVIDENCE:
                status = "contradicted"
                statement = f"Evidence requirement {observation.requirement_id} produced negative evidence."
            elif observation.outcome == EvidenceObservationOutcome.AMBIGUOUS_EVIDENCE:
                status = "inferred"
                statement = f"Evidence requirement {observation.requirement_id} has ambiguous telemetry support."
            else:
                status = "missing"
                statement = f"Evidence requirement {observation.requirement_id} is missing validated telemetry."
            query_ref: list[str] = []
            if observation.query:
                expression = observation.query.strip()
                matched_ref = exact_query_refs.get(
                    (observation.panel_title, expression, observation.datasource_uid)
                ) or datasource_query_refs.get((expression, observation.datasource_uid))
                matched_ref = matched_ref or expression_query_refs.get(expression)
                if matched_ref:
                    query_ref = [matched_ref]
            records.append(
                ObservationContract(
                    id=f"obs_{index:02d}",
                    subject_ref=entity,
                    type=observation.outcome.value.lower(),
                    statement=statement,
                    value={
                        "requirement_ref": observation.requirement_id,
                        "resolution_metric": observation.resolution_metric,
                        "panel_title": observation.panel_title,
                        "valid_query": observation.valid_query,
                        "non_empty": observation.non_empty,
                        "survived": observation.survived,
                        "rejection_reason": observation.rejection_reason,
                    },
                    status=status,
                    query_refs=query_ref,
                    provenance_refs=[provenance_ref],
                )
            )
        return records

    def _rankings(
        self,
        ranking: CulpritRanking,
        observations: list[ObservationContract],
        requirements: list[EvidenceRequirementContract],
    ) -> list[CandidateRankingContract]:
        records: list[CandidateRankingContract] = []
        known_requirement_refs = {requirement.id for requirement in requirements}
        for candidate in ranking.candidates:
            supporting_requirement_refs = set(candidate.supporting_requirement_ids) & known_requirement_refs
            contradicting_requirement_refs = set(candidate.contradicting_requirement_ids) & known_requirement_refs
            missing_requirement_refs = set(candidate.missing_requirement_ids) & known_requirement_refs
            tied_requirement_refs = (
                supporting_requirement_refs | contradicting_requirement_refs | missing_requirement_refs
            )
            supporting_observation_refs = [
                observation.id
                for observation in observations
                if observation.status == "observed"
                and _observation_requirement_ref(observation) in supporting_requirement_refs
            ]
            contradicting_observation_refs = [
                observation.id
                for observation in observations
                if observation.status == "contradicted"
                and _observation_requirement_ref(observation) in tied_requirement_refs
            ]
            records.append(
                CandidateRankingContract(
                    candidate_ref=f"{candidate.suspect_type}:{candidate.suspect}",
                    rank=candidate.rank,
                    score=candidate.score,
                    causal_status=CausalStatus.SUSPECT_NOT_PROVEN,
                    supporting_observation_refs=supporting_observation_refs,
                    contradicting_observation_refs=contradicting_observation_refs,
                    missing_requirement_refs=[
                        requirement.id for requirement in requirements if requirement.id in missing_requirement_refs
                    ],
                    contextual_reasons=candidate.contextual_reasons,
                    runtime_evidence=candidate.runtime_evidence,
                )
            )
        return records

    def _grounding(
        self,
        ranking: CulpritRanking,
        observations: list[ObservationContract],
        requirements: list[EvidenceRequirementContract],
        rankings: list[CandidateRankingContract],
    ) -> GroundingContract:
        supported_requirements = {
            _observation_requirement_ref(observation)
            for observation in observations
            if observation.status == "observed"
        }
        supported = [observation.id for observation in observations if observation.status == "observed"]
        contradicted = [
            observation.id
            for observation in observations
            if observation.status == "contradicted"
            and _observation_requirement_ref(observation) not in supported_requirements
        ]
        contradicted_requirements = {
            _observation_requirement_ref(observation) for observation in observations if observation.id in contradicted
        }
        decisive_requirements = supported_requirements | contradicted_requirements
        ambiguous = [
            observation.id
            for observation in observations
            if observation.status == "inferred"
            and _observation_requirement_ref(observation) not in decisive_requirements
        ]
        ambiguous_requirements = {
            _observation_requirement_ref(observation) for observation in observations if observation.id in ambiguous
        }
        missing = [
            observation.id
            for observation in observations
            if observation.status == "missing"
            and _observation_requirement_ref(observation) not in decisive_requirements | ambiguous_requirements
        ]
        unresolved = [
            requirement.id
            for requirement in requirements
            if requirement.resolution_status != EvidenceResolutionStatus.RESOLVED
            and requirement.id not in supported_requirements
        ]
        if contradicted:
            status = GroundingStatus.CONTRADICTED
        elif ranking.abstained or not rankings:
            status = GroundingStatus.INSUFFICIENT_EVIDENCE
        elif missing or ambiguous or unresolved:
            status = GroundingStatus.PARTIALLY_SUPPORTED if supported else GroundingStatus.INSUFFICIENT_EVIDENCE
        elif supported:
            status = GroundingStatus.SUPPORTED
        else:
            status = GroundingStatus.INDETERMINATE

        leading = rankings[0] if rankings and not ranking.abstained else None
        conclusion = "No culprit is supported by the captured evidence."
        unsafe_conclusions: list[str] = []
        if leading:
            conclusion = f"{leading.candidate_ref} is the leading suspect, but causality is not proven."
            unsafe_conclusions = [f"{leading.candidate_ref} caused the incident."]

        return GroundingContract(
            status=status,
            confidence={"level": ranking.mode.value if rankings else "none", "score": None},
            supported_claims=supported,
            unsupported_claims=ambiguous or ([] if supported else ["culprit_identified"]),
            contradicted_claims=contradicted,
            missing_observation_refs=missing,
            unsafe_to_conclude=status != GroundingStatus.SUPPORTED or bool(rankings),
            unsafe_conclusions=unsafe_conclusions,
            maximum_trustworthy_conclusion={
                "text": conclusion,
                "causal_status": CausalStatus.SUSPECT_NOT_PROVEN if leading else CausalStatus.INSUFFICIENT_EVIDENCE,
            },
            abstained=ranking.abstained or not rankings,
            reason=ranking.abstention_reason or ("No ranked candidate was produced." if not rankings else ""),
        )

    def _decision_log(
        self,
        resolutions: list[EvidenceResolutionContract],
        rankings: list[CandidateRankingContract],
        grounding: GroundingContract,
        dashboard_uid: str,
    ) -> list[DecisionLogEntry]:
        decisions: list[DecisionLogEntry] = []
        sequence = 1
        for resolution in resolutions:
            decisions.append(
                DecisionLogEntry(
                    id=f"decision_{sequence:02d}",
                    sequence=sequence,
                    stage="evidence_resolution",
                    action="selected_binding" if resolution.status == "resolved" else "recorded_gap",
                    subject_ref=resolution.requirement_ref,
                    reason_code=resolution.resolution_method,
                    reason="Recorded the evidence binding or explicit gap selected by the resolver.",
                    output_ref=resolution.id,
                    mechanism={"type": "deterministic_rule", "version": "evidence-resolution-v1"},
                )
            )
            sequence += 1
        if rankings:
            decisions.append(
                DecisionLogEntry(
                    id=f"decision_{sequence:02d}",
                    sequence=sequence,
                    stage="ranking",
                    action="ranked_candidates",
                    subject_ref=rankings[0].candidate_ref,
                    reason_code="culprit_ranking_completed",
                    reason="Ranked candidate suspects without asserting proven causality.",
                    output_ref=rankings[0].candidate_ref,
                    mechanism={"type": "deterministic_rule", "version": "culprit-ranking-v1"},
                    score_after=rankings[0].score,
                )
            )
            sequence += 1
        decisions.append(
            DecisionLogEntry(
                id=f"decision_{sequence:02d}",
                sequence=sequence,
                stage="grounding",
                action="restricted_conclusion" if grounding.unsafe_to_conclude else "accepted_conclusion",
                reason_code=grounding.status.value,
                reason="Calculated the maximum trustworthy conclusion from captured observations.",
                output_status=grounding.status.value,
                mechanism={"type": "deterministic_rule", "version": "grounding-v1"},
            )
        )
        sequence += 1
        if dashboard_uid:
            decisions.append(
                DecisionLogEntry(
                    id=f"decision_{sequence:02d}",
                    sequence=sequence,
                    stage="rendering",
                    action="published_dashboard",
                    subject_ref=dashboard_uid,
                    reason_code="dashboard_rendering_created",
                    reason="Published dashboard as a rendering of the investigation contract.",
                    mechanism={"type": "renderer", "version": "dashboard-rendering-v1"},
                )
            )
        return decisions
