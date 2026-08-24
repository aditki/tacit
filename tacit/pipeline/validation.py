"""Validation and evidence-preservation stage for the investigation pipeline."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast

import structlog

from tacit.backends.base import DashboardBackend
from tacit.errors import EvidenceResolutionError
from tacit.evidence import (
    EvidenceObservationWorkBudget,
    EvidenceObservationWorkLimits,
    observe_evidence,
    summarize_evidence,
)
from tacit.evidence_artifacts import (
    EVIDENCE_AUTHORITY_ERRORS,
    build_evidence_gap_dashboard,
    build_symptom_evidence_dashboard,
    evidence_failure_diagnostics,
    missing_critical_evidence_gap_requirements,
    missing_critical_symptom_requirements,
)
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.models.schemas import (
    DashboardSpec,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
    validate_dashboard_composition_work_limits,
    validate_dashboard_spec_work_limits,
)
from tacit.signals.resolution import SignalResolutionWorkBudget, SignalResolutionWorkLimitError

logger = structlog.get_logger()
_SYMPTOM_RESCUE_RESOLUTION_FAILED = "symptom_evidence_resolution_failed"
_GAP_RESCUE_RESOLUTION_FAILED = "evidence_gap_resolution_failed"
_SYMPTOM_RESCUE_VALIDATION_FAILED = "symptom_evidence_validation_failed"
_GAP_RESCUE_VALIDATION_FAILED = "evidence_gap_validation_failed"


@dataclass
class ValidationEvidenceResult:
    """Dashboard and accounting state after validation plus evidence preservation."""

    dashboard_spec: DashboardSpec
    validation_warnings: list[str]
    panels_before: int
    evidence_observations: list[EvidenceObservation]
    evidence_summary: dict[str, object]
    applied_knowledge_refs: frozenset[str] = frozenset()
    applied_knowledge_revision_refs: frozenset[KnowledgeRevisionRef] = frozenset()
    knowledge_revision_refs_by_requirement: dict[str, frozenset[KnowledgeRevisionRef]] = field(default_factory=dict)


def _append_validated_panels(
    *,
    pre_validation_spec: DashboardSpec,
    dashboard_spec: DashboardSpec,
    extra_pre_validation_spec: DashboardSpec,
    extra_validated_spec: DashboardSpec,
) -> tuple[DashboardSpec, DashboardSpec]:
    validate_dashboard_composition_work_limits(pre_validation_spec, extra_pre_validation_spec)
    validate_dashboard_composition_work_limits(dashboard_spec, extra_validated_spec)
    return (
        pre_validation_spec.model_copy(
            update={
                "panels": [
                    *pre_validation_spec.panels,
                    *extra_pre_validation_spec.panels,
                ]
            }
        ),
        dashboard_spec.model_copy(update={"panels": [*dashboard_spec.panels, *extra_validated_spec.panels]}),
    )


def _validation_status(panels_before: int, panels_after: int) -> tuple[str, str]:
    if panels_after == 0:
        return "failed", "all_panels_rejected"
    if panels_after < panels_before:
        return "partial", "some_panels_rejected"
    return "passed", "all_panels_survived"


def _evidence_status(evidence_summary: dict[str, object]) -> tuple[str, str]:
    critical_total = cast(int, evidence_summary["critical_total"])
    critical_observed = cast(int, evidence_summary["critical_observed"])
    if critical_total and critical_observed == critical_total:
        return "passed", "all_critical_evidence_observed"
    if critical_observed:
        return "partial", "some_critical_evidence_observed"
    return "failed", "no_critical_evidence_observed"


async def _preserve_symptom_evidence(
    *,
    primary: DashboardBackend,
    intent: Intent,
    catalog: list[MetricEntry],
    target_language: str,
    pre_validation_spec: DashboardSpec,
    dashboard_spec: DashboardSpec,
    validation_warnings: list[str],
    panels_before: int,
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    record_stage: Callable[..., None],
    signal_store: Any | None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    knowledge_query_uses: list[Any] | None = None,
    observation_work_budget: EvidenceObservationWorkBudget,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None,
) -> tuple[DashboardSpec, DashboardSpec, int]:
    initial_observations = observe_evidence(
        evidence_requirements,
        evidence_resolutions,
        pre_validation_spec,
        dashboard_spec,
        work_budget=observation_work_budget,
    )
    rescue_requirements = missing_critical_symptom_requirements(
        evidence_requirements,
        evidence_resolutions,
        initial_observations,
    )
    if not rescue_requirements:
        return pre_validation_spec, dashboard_spec, panels_before

    original_validation_warnings = list(validation_warnings)
    original_panels_before = panels_before
    original_panels_after = len(dashboard_spec.panels)
    rescue_query_uses: list[Any] = []
    try:
        symptom_pre_validation_spec, symptom_resolutions = build_symptom_evidence_dashboard(
            rescue_requirements,
            evidence_resolutions,
            intent,
            catalog=catalog,
            target_language=target_language,
            timerange=pre_validation_spec.timerange,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=rescue_query_uses,
            work_budget=observation_work_budget,
            signal_resolution_work_budget=signal_resolution_work_budget,
        )
    except EVIDENCE_AUTHORITY_ERRORS:
        raise
    except SignalResolutionWorkLimitError:
        raise
    except EvidenceResolutionError as exc:
        diagnostics = evidence_failure_diagnostics(
            exc,
            reason_code=_SYMPTOM_RESCUE_RESOLUTION_FAILED,
            requirement_count=len(rescue_requirements),
        )
        logger.warning(
            "symptom_evidence_resolution_failed",
            reason_code=_SYMPTOM_RESCUE_RESOLUTION_FAILED,
            **diagnostics,
        )
        record_stage(
            "symptom_evidence_rescue",
            "skipped",
            _SYMPTOM_RESCUE_RESOLUTION_FAILED,
            **diagnostics,
        )
        return pre_validation_spec, dashboard_spec, panels_before
    observation_work_budget.validate_counts(
        len(evidence_requirements),
        len(evidence_resolutions) + len(symptom_resolutions),
    )
    if not symptom_pre_validation_spec.panels:
        record_stage(
            "symptom_evidence_rescue",
            "skipped",
            "no_resolved_symptom_evidence",
        )
        return pre_validation_spec, dashboard_spec, panels_before

    validate_dashboard_spec_work_limits(symptom_pre_validation_spec)
    if original_panels_after:
        validate_dashboard_composition_work_limits(
            pre_validation_spec,
            symptom_pre_validation_spec,
        )

    try:
        symptom_spec, symptom_warnings = await primary.validate_queries(symptom_pre_validation_spec, catalog)
    except EVIDENCE_AUTHORITY_ERRORS:
        raise
    except Exception as exc:
        diagnostics = evidence_failure_diagnostics(
            exc,
            reason_code=_SYMPTOM_RESCUE_VALIDATION_FAILED,
            requirement_count=len(rescue_requirements),
        )
        logger.warning(
            "symptom_evidence_validation_failed",
            reason_code=_SYMPTOM_RESCUE_VALIDATION_FAILED,
            **diagnostics,
        )
        record_stage(
            "symptom_evidence_rescue",
            "skipped",
            _SYMPTOM_RESCUE_VALIDATION_FAILED,
            **diagnostics,
        )
        return pre_validation_spec, dashboard_spec, panels_before
    validate_dashboard_spec_work_limits(symptom_spec)
    if original_panels_after:
        validate_dashboard_composition_work_limits(dashboard_spec, symptom_spec)
    validation_warnings.extend(symptom_warnings)
    record_stage(
        "symptom_evidence_rescue",
        "passed" if symptom_spec.panels else "failed",
        "symptom_panels_validated" if symptom_spec.panels else "symptom_panels_rejected",
        original_panels_before=original_panels_before,
        original_panels_after=original_panels_after,
        original_warnings=original_validation_warnings,
        panels_before=len(symptom_pre_validation_spec.panels),
        panels_after=len(symptom_spec.panels),
    )
    if not symptom_spec.panels:
        return pre_validation_spec, dashboard_spec, panels_before

    evidence_resolutions.extend(symptom_resolutions)
    if knowledge_query_uses is not None:
        knowledge_query_uses.extend(rescue_query_uses)
    if original_panels_after:
        pre_validation_spec, dashboard_spec = _append_validated_panels(
            pre_validation_spec=pre_validation_spec,
            dashboard_spec=dashboard_spec,
            extra_pre_validation_spec=symptom_pre_validation_spec,
            extra_validated_spec=symptom_spec,
        )
    else:
        pre_validation_spec = symptom_pre_validation_spec
        dashboard_spec = symptom_spec
    return (
        pre_validation_spec,
        dashboard_spec,
        original_panels_before + len(symptom_pre_validation_spec.panels),
    )


async def _preserve_gap_evidence(
    *,
    primary: DashboardBackend,
    intent: Intent,
    catalog: list[MetricEntry],
    target_language: str,
    pre_validation_spec: DashboardSpec,
    dashboard_spec: DashboardSpec,
    validation_warnings: list[str],
    panels_before: int,
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    record_stage: Callable[..., None],
    signal_store: Any | None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    knowledge_query_uses: list[Any] | None = None,
    observation_work_budget: EvidenceObservationWorkBudget,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None,
) -> tuple[DashboardSpec, DashboardSpec, int]:
    gap_observations = observe_evidence(
        evidence_requirements,
        evidence_resolutions,
        pre_validation_spec,
        dashboard_spec,
        work_budget=observation_work_budget,
    )
    gap_requirements = missing_critical_evidence_gap_requirements(
        evidence_requirements,
        evidence_resolutions,
        gap_observations,
    )
    if not gap_requirements:
        record_stage(
            "evidence_gap_resolution",
            "skipped",
            "no_missing_gap_evidence",
        )
        return pre_validation_spec, dashboard_spec, panels_before

    rescue_query_uses: list[Any] = []
    try:
        gap_pre_validation_spec, gap_resolutions = build_evidence_gap_dashboard(
            gap_requirements,
            evidence_resolutions,
            intent,
            catalog=catalog,
            target_language=target_language,
            timerange=pre_validation_spec.timerange,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=rescue_query_uses,
            work_budget=observation_work_budget,
            signal_resolution_work_budget=signal_resolution_work_budget,
        )
    except EVIDENCE_AUTHORITY_ERRORS:
        raise
    except SignalResolutionWorkLimitError:
        raise
    except EvidenceResolutionError as exc:
        diagnostics = evidence_failure_diagnostics(
            exc,
            reason_code=_GAP_RESCUE_RESOLUTION_FAILED,
            requirement_count=len(gap_requirements),
        )
        logger.warning(
            "evidence_gap_resolution_failed",
            reason_code=_GAP_RESCUE_RESOLUTION_FAILED,
            **diagnostics,
        )
        record_stage(
            "evidence_gap_resolution",
            "skipped",
            _GAP_RESCUE_RESOLUTION_FAILED,
            **diagnostics,
        )
        return pre_validation_spec, dashboard_spec, panels_before
    observation_work_budget.validate_counts(
        len(evidence_requirements),
        len(evidence_resolutions) + len(gap_resolutions),
    )
    if not gap_pre_validation_spec.panels:
        record_stage(
            "evidence_gap_resolution",
            "skipped",
            "no_supported_gap_observation",
            requirements=len(gap_requirements),
        )
        return pre_validation_spec, dashboard_spec, panels_before

    validate_dashboard_spec_work_limits(gap_pre_validation_spec)
    validate_dashboard_composition_work_limits(pre_validation_spec, gap_pre_validation_spec)

    try:
        gap_spec, gap_warnings = await primary.validate_queries(gap_pre_validation_spec, catalog)
    except EVIDENCE_AUTHORITY_ERRORS:
        raise
    except Exception as exc:
        diagnostics = evidence_failure_diagnostics(
            exc,
            reason_code=_GAP_RESCUE_VALIDATION_FAILED,
            requirement_count=len(gap_requirements),
        )
        logger.warning(
            "evidence_gap_validation_failed",
            reason_code=_GAP_RESCUE_VALIDATION_FAILED,
            **diagnostics,
        )
        record_stage(
            "evidence_gap_resolution",
            "skipped",
            _GAP_RESCUE_VALIDATION_FAILED,
            **diagnostics,
        )
        return pre_validation_spec, dashboard_spec, panels_before
    validate_dashboard_spec_work_limits(gap_spec)
    validate_dashboard_composition_work_limits(dashboard_spec, gap_spec)
    validation_warnings.extend(gap_warnings)
    record_stage(
        "evidence_gap_resolution",
        "passed" if gap_spec.panels else "failed",
        "supported_observations_validated" if gap_spec.panels else "gap_observations_rejected",
        requirements=len(gap_requirements),
        panels_before=len(gap_pre_validation_spec.panels),
        panels_after=len(gap_spec.panels),
    )
    if not gap_spec.panels:
        return pre_validation_spec, dashboard_spec, panels_before

    evidence_resolutions.extend(gap_resolutions)
    if knowledge_query_uses is not None:
        knowledge_query_uses.extend(rescue_query_uses)
    pre_validation_spec, dashboard_spec = _append_validated_panels(
        pre_validation_spec=pre_validation_spec,
        dashboard_spec=dashboard_spec,
        extra_pre_validation_spec=gap_pre_validation_spec,
        extra_validated_spec=gap_spec,
    )
    return pre_validation_spec, dashboard_spec, panels_before + len(gap_pre_validation_spec.panels)


def _evidence_stage_payload(
    *,
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    pre_validation_spec: DashboardSpec,
    dashboard_spec: DashboardSpec,
    ranked_archetypes_present: bool,
    observation_work_budget: EvidenceObservationWorkBudget,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None,
) -> tuple[list[EvidenceObservation], dict[str, object], str, str]:
    if evidence_requirements:
        evidence_observations = observe_evidence(
            evidence_requirements,
            evidence_resolutions,
            pre_validation_spec,
            dashboard_spec,
            work_budget=observation_work_budget,
        )
        evidence_summary = summarize_evidence(
            evidence_requirements,
            evidence_resolutions,
            evidence_observations,
        )
        evidence_summary.update(observation_work_budget.diagnostics())
        if signal_resolution_work_budget is not None:
            evidence_summary.update(signal_resolution_work_budget.counters())
        evidence_status, evidence_reason = _evidence_status(evidence_summary)
        return evidence_observations, evidence_summary, evidence_status, evidence_reason
    return (
        [],
        {
            "path": "archetype" if ranked_archetypes_present else "freeform",
            **(signal_resolution_work_budget.counters() if signal_resolution_work_budget is not None else {}),
        },
        "skipped",
        "no_declared_evidence_requirements",
    )


def _record_evidence_stage(
    *,
    evidence_summary: dict[str, object],
    evidence_status: str,
    evidence_reason: str,
    record_stage: Callable[..., None],
) -> None:
    try:
        record_stage(
            "evidence",
            evidence_status,
            evidence_reason,
            **evidence_summary,
        )
    except Exception as exc:
        logger.warning(
            "history_record_evidence_failed",
            **evidence_failure_diagnostics(
                exc,
                reason_code="history_record_evidence_failed",
                requirement_count=cast(int, evidence_summary.get("total", 0) or 0),
            ),
        )


async def validate_dashboard_and_evidence(
    *,
    primary: DashboardBackend,
    dashboard_spec: DashboardSpec,
    catalog: list[MetricEntry],
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    intent: Intent,
    target_language: str,
    ranked_archetypes_present: bool,
    record_stage: Callable[..., None],
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    evidence_work_limits: EvidenceObservationWorkLimits | None = None,
    evidence_work_budget: EvidenceObservationWorkBudget | None = None,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> ValidationEvidenceResult:
    """Validate dashboard queries and preserve critical evidence when possible."""
    if evidence_work_budget is not None and evidence_work_limits is not None:
        if evidence_work_budget.limits != evidence_work_limits:
            raise ValueError("evidence work budget and limits disagree")
    observation_work_budget = evidence_work_budget or EvidenceObservationWorkBudget(
        evidence_work_limits or EvidenceObservationWorkLimits()
    )
    observation_work_budget.validate_inputs(evidence_requirements, evidence_resolutions)
    observation_work_budget.validate_catalog(catalog, service_count=len(intent.services))
    validate_dashboard_spec_work_limits(dashboard_spec)
    panels_before = len(dashboard_spec.panels)
    pre_validation_spec = dashboard_spec.model_copy(deep=True)
    dashboard_spec, validation_warnings = await primary.validate_queries(dashboard_spec, catalog)
    validate_dashboard_spec_work_limits(dashboard_spec)
    rescue_knowledge_query_uses: list[Any] = []

    if evidence_requirements:
        pre_validation_spec, dashboard_spec, panels_before = await _preserve_symptom_evidence(
            primary=primary,
            intent=intent,
            catalog=catalog,
            target_language=target_language,
            pre_validation_spec=pre_validation_spec,
            dashboard_spec=dashboard_spec,
            validation_warnings=validation_warnings,
            panels_before=panels_before,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            record_stage=record_stage,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=rescue_knowledge_query_uses,
            observation_work_budget=observation_work_budget,
            signal_resolution_work_budget=signal_resolution_work_budget,
        )
        pre_validation_spec, dashboard_spec, panels_before = await _preserve_gap_evidence(
            primary=primary,
            intent=intent,
            catalog=catalog,
            target_language=target_language,
            pre_validation_spec=pre_validation_spec,
            dashboard_spec=dashboard_spec,
            validation_warnings=validation_warnings,
            panels_before=panels_before,
            evidence_requirements=evidence_requirements,
            evidence_resolutions=evidence_resolutions,
            record_stage=record_stage,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=rescue_knowledge_query_uses,
            observation_work_budget=observation_work_budget,
            signal_resolution_work_budget=signal_resolution_work_budget,
        )

    validation_status, validation_reason = _validation_status(panels_before, len(dashboard_spec.panels))
    record_stage(
        "validation",
        validation_status,
        validation_reason,
        panels_before=panels_before,
        panels_after=len(dashboard_spec.panels),
        warnings=validation_warnings,
    )
    evidence_observations, evidence_summary, evidence_status, evidence_reason = _evidence_stage_payload(
        evidence_requirements=evidence_requirements,
        evidence_resolutions=evidence_resolutions,
        pre_validation_spec=pre_validation_spec,
        dashboard_spec=dashboard_spec,
        ranked_archetypes_present=ranked_archetypes_present,
        observation_work_budget=observation_work_budget,
        signal_resolution_work_budget=signal_resolution_work_budget,
    )
    _record_evidence_stage(
        evidence_summary=evidence_summary,
        evidence_status=evidence_status,
        evidence_reason=evidence_reason,
        record_stage=record_stage,
    )
    from tacit.archetypes.engine import dashboard_query_identities

    surviving_query_ids = dashboard_query_identities(dashboard_spec)
    applied_knowledge_refs = frozenset(
        use.knowledge_ref for use in rescue_knowledge_query_uses if use.query_identity() in surviving_query_ids
    )
    applied_knowledge_revision_refs = frozenset(
        KnowledgeRevisionRef(use.knowledge_ref, use.knowledge_revision)
        for use in rescue_knowledge_query_uses
        if use.knowledge_revision > 0 and use.query_identity() in surviving_query_ids
    )
    knowledge_revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] = {}
    for use in rescue_knowledge_query_uses:
        if use.requirement_id and use.knowledge_revision > 0 and use.query_identity() in surviving_query_ids:
            knowledge_revision_refs_by_requirement.setdefault(use.requirement_id, set()).add(
                KnowledgeRevisionRef(use.knowledge_ref, use.knowledge_revision)
            )
    return ValidationEvidenceResult(
        dashboard_spec=dashboard_spec,
        validation_warnings=validation_warnings,
        panels_before=panels_before,
        evidence_observations=evidence_observations,
        evidence_summary=evidence_summary,
        applied_knowledge_refs=applied_knowledge_refs,
        applied_knowledge_revision_refs=applied_knowledge_revision_refs,
        knowledge_revision_refs_by_requirement={
            key: frozenset(value) for key, value in knowledge_revision_refs_by_requirement.items()
        },
    )
