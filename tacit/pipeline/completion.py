"""Pipeline completion, publishing, and provenance helpers."""

from __future__ import annotations

import time
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog
from pydantic import BaseModel

from tacit.agents.providers.base import TokenUsage
from tacit.backends.base import DashboardBackend, PublishResult
from tacit.catalog import AcquisitionCaptureInvalidError, AcquisitionCaptureLimitError
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies, resolve_knowledge_service
from tacit.errors import safe_failure_detail, safe_failure_diagnostics
from tacit.investigation_contract import (
    InvestigationContractAssembler,
    InvestigationRunType,
    RuntimeManifest,
    stamp_fingerprints,
    utc_now,
)
from tacit.investigation_replay import InvestigationReplaySnapshot
from tacit.knowledge.models import KnowledgeSnapshot, KnowledgeUsage
from tacit.logging import stage_log
from tacit.models.schemas import (
    ContextChunk,
    CulpritRanking,
    DashboardSpec,
    DashRequest,
    DashResponse,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
)
from tacit.pipeline.progress import emit_progress
from tacit.pipeline.recording import (
    PipelineRecorder,
    dashboard_summary,
    surviving_datasource_names,
)
from tacit.pipeline.side_effects import safe_record_provenance
from tacit.pipeline.stages.publish import (
    PublicationState,
    preflight_publish_backends,
    publish_dashboard,
)
from tacit.signals.resolution import ResolutionInputWorkLimitError, admit_resolution_input_text

logger = structlog.get_logger()
_TIMING_DECIMAL_PLACES = 4


@dataclass(frozen=True)
class PreparedAcquisitionCompletion:
    """Run-owned admitted values; no clients, lifecycle authority or mutable inputs."""

    _assembly_inputs: dict[str, Any]
    base_index: Any
    admitted: Any
    created_at: datetime
    _baseline: CulpritRanking | None = None

    @property
    def assembly_inputs(self) -> dict[str, Any]:
        return deepcopy(self._assembly_inputs)


def _admit_completion_inputs(inputs: Mapping[str, Any]) -> None:
    """Bound the existing model graph before copying or assembling it."""
    stack: list[tuple[Any, int]] = [(inputs, 0)]
    nodes = 0
    texts: list[str] = []
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if nodes > 200_000:
            raise AcquisitionCaptureLimitError("completion_input_nodes", nodes, 200_000)
        if depth > 32:
            raise AcquisitionCaptureLimitError("completion_input_depth", depth, 32)
        if isinstance(value, str):
            texts.append(value)
        elif isinstance(value, BaseModel):
            fields = type(value).model_fields
            stack.extend((getattr(value, name), depth + 1) for name in fields)
        elif isinstance(value, Mapping):
            if len(value) > 200_000 - nodes:
                raise AcquisitionCaptureLimitError("completion_input_nodes", nodes + len(value), 200_000)
            for key, item in value.items():
                if not isinstance(key, str):
                    raise AcquisitionCaptureInvalidError("completion_input_key")
                texts.append(key)
                stack.append((item, depth + 1))
        elif isinstance(value, (tuple, list, set, frozenset)):
            if len(value) > 200_000 - nodes:
                raise AcquisitionCaptureLimitError("completion_input_nodes", nodes + len(value), 200_000)
            stack.extend((item, depth + 1) for item in value)
        elif value is not None and not isinstance(value, (int, float, bool, datetime)):
            raise AcquisitionCaptureInvalidError("completion_input_type")
    try:
        admit_resolution_input_text(texts)
    except ResolutionInputWorkLimitError as exc:
        raise AcquisitionCaptureLimitError(exc.dimension, exc.observed, exc.limit) from None
    except UnicodeError:
        raise AcquisitionCaptureInvalidError("completion_input_unicode") from None


def prepare_acquisition_completion(
    *,
    assembly_inputs: Mapping[str, Any],
    acquisition_record: dict[str, Any],
    slot_subjects: tuple[str, ...] = (),
    freeform_subjects: tuple[str, ...] = (),
    baseline_culprit_ranking: CulpritRanking | None = None,
) -> PreparedAcquisitionCompletion:
    """Validate IDs, references and byte bounds before completion can publish."""
    from tacit.catalog import BaseReferenceIndex, admit_acquisition_capture, capture_extensions

    _admit_completion_inputs({**assembly_inputs, "baseline_culprit_ranking": baseline_culprit_ranking})
    inputs = deepcopy(dict(assembly_inputs))
    created_at = utc_now()
    base = InvestigationContractAssembler().from_pipeline(
        **inputs,
        created_at=created_at,
        completed_at=created_at,
        dashboard_url="",
        dashboard_uid="",
        signalfx_url="",
        signalfx_dashboard_id="",
    )
    identifiers = [base.investigation.id]
    for collection in (
        base.provenance,
        base.decision_log,
        base.evidence_requirements,
        base.evidence_resolutions,
        base.observations,
        base.queries,
        base.artifact_contributions,
    ):
        identifiers.extend(item.id for item in collection)
    identifiers.extend(item.candidate_ref for item in base.candidate_rankings)
    if len(set(identifiers)) != len(identifiers):
        raise AcquisitionCaptureInvalidError("duplicate_base_id")
    maximum = max((item.sequence for item in base.decision_log), default=0)
    index = BaseReferenceIndex(
        contract_ids=frozenset(identifiers),
        requirement_ids=frozenset(item.id for item in base.evidence_requirements),
        slot_subjects=frozenset(slot_subjects),
        freeform_subjects=frozenset(freeform_subjects),
        base_decision_max=maximum,
    )
    decisions, provenance = capture_extensions(acquisition_record)
    admitted = admit_acquisition_capture(acquisition_record, decisions, provenance, base_index=index)
    return PreparedAcquisitionCompletion(inputs, index, admitted, created_at, deepcopy(baseline_culprit_ranking))


def _rounded_timings(timings: dict[str, float]) -> dict[str, float]:
    """Retain sub-10ms stage costs in persisted pipeline diagnostics."""
    return {key: round(value, _TIMING_DECIMAL_PLACES) for key, value in timings.items()}


async def complete_pipeline(
    *,
    request: DashRequest,
    deps: PipelineDependencies,
    backends: list[DashboardBackend],
    dashboard_spec: DashboardSpec,
    intent: Intent,
    metric_catalog: list[MetricEntry],
    datasource_catalog: list[MetricEntry],
    ranked_archetypes_present: bool,
    validation_warnings: list[str],
    panels_before: int,
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    evidence_observations: list[EvidenceObservation],
    culprit_ranking: CulpritRanking,
    baseline_culprit_ranking: CulpritRanking | None = None,
    prepared_acquisition: PreparedAcquisitionCompletion | None = None,
    context_chunks: list[ContextChunk] | None = None,
    knowledge_snapshot: KnowledgeSnapshot | None = None,
    knowledge_usage: list[KnowledgeUsage] | None = None,
    run_type: InvestigationRunType = InvestigationRunType.INITIAL,
    revision_reason: str = "initial",
    base_revision: int | None = None,
    timings: dict[str, float],
    recorder: PipelineRecorder,
    token_usage: TokenUsage,
    started_at: float,
) -> DashResponse:
    """Publish a validated dashboard and record completion/provenance."""
    if prepared_acquisition is not None:
        pinned = prepared_acquisition.assembly_inputs
        request = pinned["request"]
        intent = pinned["intent"]
        dashboard_spec = pinned["dashboard_spec"]
        evidence_requirements = pinned["evidence_requirements"]
        evidence_resolutions = pinned["evidence_resolutions"]
        evidence_observations = pinned["evidence_observations"]
        culprit_ranking = pinned["culprit_ranking"]
        context_chunks = pinned["context_chunks"]
        knowledge_usage = pinned["knowledge_usage"]
        validation_warnings = pinned["warnings"]
        baseline_culprit_ranking = deepcopy(prepared_acquisition._baseline)
    runtime_settings = deps.settings if isinstance(deps.settings, Settings) else None
    preflight_publish_backends(backends, runtime_settings, dashboard_spec)
    recorder.required_event(
        "publication_commit_started",
        {"backends": [backend.name for backend in backends]},
    )
    publication_state = PublicationState(commit_started=True)
    emit_progress("publish", "started", "publishing_dashboard", backends=[b.name for b in backends])
    publish_results = await publish_dashboard(
        backends=backends,
        dashboard_spec=dashboard_spec,
        timings=timings,
        runtime_settings=runtime_settings,
        preserve_commit_on_cancellation=True,
        state=publication_state,
    )
    emit_progress(
        "publish",
        "passed",
        "dashboard_published",
        backends={name: bool(result.url) for name, result in publish_results.items()},
        panel_count=len(dashboard_spec.panels),
    )

    grafana_result = publish_results.get("grafana", PublishResult())
    sfx_result = publish_results.get("signalfx", PublishResult())
    effective_uid = grafana_result.uid or sfx_result.uid or ""
    effective_url = grafana_result.url or sfx_result.url or ""

    path_used = "archetype" if ranked_archetypes_present else "freeform"
    summary = dashboard_summary(
        dashboard_spec,
        path_used,
        surviving_datasource_names(dashboard_spec, metric_catalog, datasource_catalog),
        publish_results,
    )

    total_s = time.monotonic() - started_at
    timings["total"] = total_s
    timings_rounded = _rounded_timings(timings)

    recorder.validation(
        validation_warnings,
        panels_before=panels_before,
        final_panel_count=len(dashboard_spec.panels),
    )

    stage_log(
        "pipeline_complete",
        total_s * 1000,
        token_usage=token_usage,
        user_id=request.user_id,
        channel_id=request.channel_id,
        dashboard_uid=effective_uid,
        panel_count=len(dashboard_spec.panels),
        path=path_used,
        timings=timings_rounded,
    )

    safe_record_provenance(
        feedback_store_factory=deps.feedback_store_factory,
        dashboard_uid=effective_uid,
        dashboard_url=effective_url,
        request=request,
        intent=intent,
        dashboard_spec=dashboard_spec,
        path_used=path_used,
    )

    persisted_contract = None
    contract_persist_error = ""
    try:
        if prepared_acquisition is None:
            draft_contract = InvestigationContractAssembler().from_pipeline(
                investigation_id=recorder.investigation_id,
                revision=0,
                parent_revision=None,
                request=request,
                intent=intent,
                dashboard_spec=dashboard_spec,
                evidence_requirements=evidence_requirements,
                evidence_resolutions=evidence_resolutions,
                evidence_observations=evidence_observations,
                culprit_ranking=culprit_ranking,
                context_chunks=context_chunks,
                warnings=validation_warnings,
                dashboard_url=effective_url,
                dashboard_uid=effective_uid,
                signalfx_url=sfx_result.url,
                signalfx_dashboard_id=sfx_result.uid,
                runtime_manifest=RuntimeManifest(
                    model={
                        "provider": str(getattr(deps.settings, "llm_provider", "")),
                        "name": str(getattr(deps.settings, "llm_model", "")),
                        "prompt_version": "intent-v1",
                    }
                ),
                knowledge_snapshot_ref=knowledge_snapshot.id if knowledge_snapshot else "",
                knowledge_usage=knowledge_usage,
            )
        else:
            draft_contract = InvestigationContractAssembler().from_pipeline(
                **prepared_acquisition.assembly_inputs,
                dashboard_url=effective_url,
                dashboard_uid=effective_uid,
                signalfx_url=sfx_result.url,
                signalfx_dashboard_id=sfx_result.uid,
                created_at=prepared_acquisition.created_at,
                completed_at=utc_now(),
            )
        if prepared_acquisition is not None:
            provenance = list(prepared_acquisition.admitted.additional_provenance)
            decisions = sorted(
                prepared_acquisition.admitted.additional_decisions,
                key=lambda item: (item.stage, item.action, item.subject_ref, item.id),
            )
            maximum = max((item.sequence for item in draft_contract.decision_log), default=0)
            decisions = [
                item.model_copy(update={"sequence": maximum + offset}) for offset, item in enumerate(decisions, 1)
            ]
            draft_contract = stamp_fingerprints(
                draft_contract.model_copy(
                    update={
                        "provenance": [*draft_contract.provenance, *sorted(provenance, key=lambda item: item.id)],
                        "decision_log": [*draft_contract.decision_log, *decisions],
                    }
                )
            )
        snapshot = InvestigationReplaySnapshot(
            investigation_id=recorder.investigation_id,
            created_at=draft_contract.investigation.created_at,
            completed_at=draft_contract.investigation.completed_at,
            request=request,
            intent=intent,
            dashboard_spec=dashboard_spec,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            resolution_candidates=[resolution.model_dump(mode="json") for resolution in evidence_resolutions],
            evidence_observations=evidence_observations,
            culprit_ranking=culprit_ranking,
            baseline_culprit_ranking=baseline_culprit_ranking,
            context_chunks=context_chunks or [],
            renderings=draft_contract.renderings,
            external_errors=[{"type": "validation_warning", "detail": warning} for warning in validation_warnings],
            model_inputs={
                "prompt": request.prompt,
                "context_chunks": [chunk.model_dump(mode="json") for chunk in context_chunks or []],
            },
            model_outputs={
                "intent": intent.model_dump(mode="json"),
                "dashboard_spec": dashboard_spec.model_dump(mode="json"),
            },
            query_results=[
                {
                    "panel_title": panel.title,
                    "expression": query.expr,
                    "validation_status": query.validation_status,
                    "has_data": query.validation_has_data,
                }
                for panel in dashboard_spec.panels
                for query in panel.queries
            ],
            runtime=draft_contract.runtime,
            knowledge_snapshot_ref=knowledge_snapshot.id if knowledge_snapshot else "",
            knowledge_usage=knowledge_usage or [],
        )
        if prepared_acquisition is not None:
            pinned = prepared_acquisition.assembly_inputs
            for field_name in (
                "request",
                "intent",
                "dashboard_spec",
                "evidence_requirements",
                "evidence_resolutions",
                "evidence_observations",
                "culprit_ranking",
                "context_chunks",
                "knowledge_usage",
            ):
                setattr(snapshot, field_name, pinned[field_name])
            snapshot.baseline_culprit_ranking = deepcopy(prepared_acquisition._baseline)
            snapshot.knowledge_snapshot_ref = pinned.get("knowledge_snapshot_ref", "")
            snapshot.additional_decisions = decisions
            snapshot.additional_provenance = sorted(provenance, key=lambda item: item.id)
            snapshot.model_inputs["catalog_acquisition_v1"] = prepared_acquisition.admitted.record
            snapshot.model_inputs["prompt"] = snapshot.request.prompt
            snapshot.model_inputs["context_chunks"] = [
                chunk.model_dump(mode="json") for chunk in snapshot.context_chunks
            ]
            snapshot.model_outputs["intent"] = snapshot.intent.model_dump(mode="json")
            snapshot.model_outputs["dashboard_spec"] = snapshot.dashboard_spec.model_dump(mode="json")
        for requirement in draft_contract.evidence_requirements:
            recorder.event("requirement_created", requirement.model_dump(mode="json"))
        for resolution in draft_contract.evidence_resolutions:
            recorder.event("resolution_selected", resolution.model_dump(mode="json"))
        for query in draft_contract.queries:
            recorder.event("query_validated", query.model_dump(mode="json"))
            recorder.event("query_executed", {"query_ref": query.id, **query.execution})
        for observation in draft_contract.observations:
            recorder.event("observation_created", observation.model_dump(mode="json"))
        for candidate in draft_contract.candidate_rankings:
            recorder.event("candidate_ranked", candidate.model_dump(mode="json"))
        for usage in draft_contract.knowledge_usage:
            recorder.event("knowledge_considered", usage.model_dump(mode="json"))
        recorder.event("conclusion_restricted", draft_contract.grounding.model_dump(mode="json"))
        persisted_contract = recorder.history.persist_contract_revision(
            draft_contract,
            reason=revision_reason,
            run_type=run_type,
            snapshot=snapshot,
            run_id=recorder.run_id,
            expected_parent_revision=(base_revision if run_type == InvestigationRunType.REFRESH else None),
        )
    except Exception as exc:
        contract_persist_error, diagnostics = safe_failure_detail(
            exc,
            reason_code="contract_revision_persist_failed",
        )
        logger.warning(
            "investigation_contract_persist_failed",
            investigation_id=recorder.investigation_id,
            dashboard_uid=effective_uid,
            reason_code="contract_revision_persist_failed",
            **diagnostics,
        )
        recorder.stage(
            "contract_persistence",
            "failed",
            "contract_revision_persist_failed",
            **diagnostics,
        )
        recorder.event(
            "contract_persistence_failed",
            {**diagnostics, "dashboard_uid": effective_uid},
        )
    else:
        recorder.stage(
            "contract_persistence",
            "passed",
            "contract_revision_persisted",
            investigation_revision=persisted_contract.investigation.revision,
        )
    if persisted_contract and persisted_contract.knowledge_usage:
        try:
            resolve_knowledge_service(deps).persist_usage(
                persisted_contract.knowledge_usage,
                investigation_id=persisted_contract.investigation.id,
                investigation_revision=persisted_contract.investigation.revision,
            )
        except Exception as exc:
            logger.warning(
                "knowledge_usage_persist_failed",
                investigation_id=recorder.investigation_id,
                reason_code="knowledge_usage_persist_failed",
                **safe_failure_diagnostics(
                    exc,
                    reason_code="knowledge_usage_persist_failed",
                ),
            )

    refresh_persist_failed = run_type == InvestigationRunType.REFRESH and persisted_contract is None
    cancellation_warning_code = (
        "cancellation_after_publication_commit" if publication_state.cancellation_requested else ""
    )
    if publication_state.cancellation_requested:
        recorder.event(
            "publication_commit_cancellation_deferred",
            {"published_backends": sorted(publish_results)},
        )
    run_warning_code = (
        "contract_persistence_failed"
        if contract_persist_error and not refresh_persist_failed
        else cancellation_warning_code
    )
    run_warning_detail = (
        contract_persist_error if contract_persist_error and not refresh_persist_failed else cancellation_warning_code
    )
    recorder.finish(
        status="failed" if refresh_persist_failed else "success",
        dashboard_uid=effective_uid,
        dashboard_url=effective_url,
        error="Refresh did not produce a new revision." if refresh_persist_failed else "",
        timings=timings_rounded,
        total_time=total_s,
        # Refresh revisions are authoritative. Keep the legacy row as one
        # internally consistent snapshot instead of mixing old pipeline fields
        # with the refreshed dashboard; revision persistence updates only its
        # current_revision pointer.
        persist_record=run_type != InvestigationRunType.REFRESH,
        run_warning_code=run_warning_code,
        run_warning_detail=run_warning_detail,
    )

    return DashResponse(
        dashboard_url=grafana_result.url,
        dashboard_uid=effective_uid,
        panel_count=len(dashboard_spec.panels),
        summary=summary,
        investigation_id=recorder.investigation_id,
        investigation_run_id=recorder.run_id or "",
        investigation_status="failed" if refresh_persist_failed else "completed",
        audit_status=recorder.audit_status,
        investigation_revision=(persisted_contract.investigation.revision if persisted_contract else None),
        signalfx_url=sfx_result.url,
        signalfx_dashboard_id=sfx_result.uid,
        culprit_ranking=culprit_ranking,
    )
