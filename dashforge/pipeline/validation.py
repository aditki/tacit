"""Validation and evidence-preservation stage for the investigation pipeline."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import structlog

from dashforge.backends.base import DashboardBackend
from dashforge.errors import EvidenceResolutionError
from dashforge.evidence import observe_evidence, summarize_evidence
from dashforge.evidence_artifacts import (
    build_evidence_gap_dashboard,
    build_symptom_evidence_dashboard,
    missing_critical_evidence_gap_requirements,
    missing_critical_symptom_requirements,
)
from dashforge.models.schemas import (
    DashboardSpec,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
)

logger = structlog.get_logger()


@dataclass
class ValidationEvidenceResult:
    """Dashboard and accounting state after validation plus evidence preservation."""

    dashboard_spec: DashboardSpec
    validation_warnings: list[str]
    panels_before: int


def _append_validated_panels(
    *,
    pre_validation_spec: DashboardSpec,
    dashboard_spec: DashboardSpec,
    extra_pre_validation_spec: DashboardSpec,
    extra_validated_spec: DashboardSpec,
) -> tuple[DashboardSpec, DashboardSpec]:
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
) -> tuple[DashboardSpec, DashboardSpec, int]:
    initial_observations = observe_evidence(
        evidence_requirements,
        evidence_resolutions,
        pre_validation_spec,
        dashboard_spec,
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
    symptom_pre_validation_spec, symptom_resolutions = build_symptom_evidence_dashboard(
        rescue_requirements,
        evidence_resolutions,
        intent,
        catalog=catalog,
        target_language=target_language,
        timerange=pre_validation_spec.timerange,
    )
    if not symptom_pre_validation_spec.panels:
        record_stage(
            "symptom_evidence_rescue",
            "skipped",
            "no_resolved_symptom_evidence",
        )
        return pre_validation_spec, dashboard_spec, panels_before

    symptom_spec, symptom_warnings = await primary.validate_queries(symptom_pre_validation_spec, catalog)
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
) -> tuple[DashboardSpec, DashboardSpec, int]:
    gap_observations = observe_evidence(
        evidence_requirements,
        evidence_resolutions,
        pre_validation_spec,
        dashboard_spec,
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

    gap_pre_validation_spec, gap_resolutions = build_evidence_gap_dashboard(
        gap_requirements,
        evidence_resolutions,
        intent,
        catalog=catalog,
        target_language=target_language,
        timerange=pre_validation_spec.timerange,
    )
    if not gap_pre_validation_spec.panels:
        record_stage(
            "evidence_gap_resolution",
            "skipped",
            "no_supported_gap_observation",
            requirements=len(gap_requirements),
        )
        return pre_validation_spec, dashboard_spec, panels_before

    gap_spec, gap_warnings = await primary.validate_queries(gap_pre_validation_spec, catalog)
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
    pre_validation_spec, dashboard_spec = _append_validated_panels(
        pre_validation_spec=pre_validation_spec,
        dashboard_spec=dashboard_spec,
        extra_pre_validation_spec=gap_pre_validation_spec,
        extra_validated_spec=gap_spec,
    )
    return pre_validation_spec, dashboard_spec, panels_before + len(gap_pre_validation_spec.panels)


def _record_evidence_stage(
    *,
    evidence_requirements: list[EvidenceRequirement],
    evidence_resolutions: list[EvidenceResolution],
    pre_validation_spec: DashboardSpec,
    dashboard_spec: DashboardSpec,
    ranked_archetypes_present: bool,
    record_stage: Callable[..., None],
) -> None:
    try:
        if evidence_requirements:
            evidence_observations = observe_evidence(
                evidence_requirements,
                evidence_resolutions,
                pre_validation_spec,
                dashboard_spec,
            )
            evidence_summary = summarize_evidence(
                evidence_requirements,
                evidence_resolutions,
                evidence_observations,
            )
            evidence_status, evidence_reason = _evidence_status(evidence_summary)
            record_stage(
                "evidence",
                evidence_status,
                evidence_reason,
                **evidence_summary,
            )
        else:
            record_stage(
                "evidence",
                "skipped",
                "no_declared_evidence_requirements",
                path="archetype" if ranked_archetypes_present else "freeform",
            )
    except Exception:
        logger.warning(
            "history_record_evidence_failed",
            error_type=EvidenceResolutionError.__name__,
            exc_info=True,
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
) -> ValidationEvidenceResult:
    """Validate dashboard queries and preserve critical evidence when possible."""
    panels_before = len(dashboard_spec.panels)
    pre_validation_spec = dashboard_spec.model_copy(deep=True)
    dashboard_spec, validation_warnings = await primary.validate_queries(dashboard_spec, catalog)

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
    _record_evidence_stage(
        evidence_requirements=evidence_requirements,
        evidence_resolutions=evidence_resolutions,
        pre_validation_spec=pre_validation_spec,
        dashboard_spec=dashboard_spec,
        ranked_archetypes_present=ranked_archetypes_present,
        record_stage=record_stage,
    )
    return ValidationEvidenceResult(
        dashboard_spec=dashboard_spec,
        validation_warnings=validation_warnings,
        panels_before=panels_before,
    )
