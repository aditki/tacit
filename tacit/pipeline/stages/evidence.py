"""Evidence resolution stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

from tacit.evidence import (
    EVIDENCE_RESOLUTION_FAILED,
    EvidenceObservationWorkBudget,
    EvidenceObservationWorkLimitError,
    EvidenceObservationWorkLimits,
    contributing_archetypes,
    requirements_for_archetypes,
    resolve_declared_requirements_for_archetypes,
    unresolved_resolutions_for_requirements,
)
from tacit.evidence_artifacts import EVIDENCE_AUTHORITY_ERRORS, evidence_failure_diagnostics
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.models.schemas import (
    DashboardSpec,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
    validate_dashboard_spec_work_limits,
)
from tacit.signals.resolution import SignalResolutionWorkBudget, SignalResolutionWorkLimitError

logger = structlog.get_logger()


@dataclass(frozen=True)
class EvidenceStageResult:
    requirements: list[EvidenceRequirement]
    resolutions: list[EvidenceResolution]
    applied_knowledge_refs: frozenset[str] = frozenset()
    knowledge_refs_by_requirement: dict[str, frozenset[str]] = field(default_factory=dict)
    applied_knowledge_revision_refs: frozenset[KnowledgeRevisionRef] = frozenset()
    knowledge_revision_refs_by_requirement: dict[str, frozenset[KnowledgeRevisionRef]] = field(default_factory=dict)
    work_budget: EvidenceObservationWorkBudget | None = None


def run_evidence_stage(
    *,
    ranked_archetypes: list[tuple[Any, float]],
    dashboard_spec: DashboardSpec,
    intent: Intent,
    catalog: list[MetricEntry],
    target_language: str,
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    evidence_work_limits: EvidenceObservationWorkLimits | None = None,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> EvidenceStageResult:
    """Resolve evidence requirements for the archetypes that contributed panels."""
    limits = evidence_work_limits or EvidenceObservationWorkLimits()
    work_budget = EvidenceObservationWorkBudget(limits)
    if not ranked_archetypes:
        return EvidenceStageResult(requirements=[], resolutions=[], work_budget=work_budget)
    validate_dashboard_spec_work_limits(dashboard_spec)
    work_budget.validate_archetypes(ranked_archetypes, intent, project_requirements=False)
    work_budget.validate_catalog(catalog, service_count=len(intent.services))
    evidence_archetypes = contributing_archetypes(
        ranked_archetypes,
        dashboard_spec,
        work_limits=limits,
    )
    requirements = requirements_for_archetypes(
        evidence_archetypes,
        intent,
        work_limits=limits,
    )
    if not requirements:
        return EvidenceStageResult(requirements=[], resolutions=[], work_budget=work_budget)
    work_budget.validate_inputs(requirements, [])
    try:
        applied_knowledge_refs: set[str] = set()
        refs_by_requirement: dict[str, set[str]] = {}
        applied_revision_refs: set[KnowledgeRevisionRef] = set()
        revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] = {}
        resolutions = resolve_declared_requirements_for_archetypes(
            evidence_archetypes,
            intent,
            catalog,
            requirements,
            target_language=target_language,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            applied_governance_refs=applied_knowledge_refs,
            governance_refs_by_requirement=refs_by_requirement,
            applied_governance_revision_refs=applied_revision_refs,
            governance_revision_refs_by_requirement=revision_refs_by_requirement,
            work_limits=limits,
            work_budget=work_budget,
            signal_resolution_work_budget=signal_resolution_work_budget,
        )
        work_budget.validate_inputs(requirements, resolutions)
        return EvidenceStageResult(
            requirements=requirements,
            resolutions=resolutions,
            applied_knowledge_refs=frozenset(applied_knowledge_refs),
            knowledge_refs_by_requirement={key: frozenset(value) for key, value in refs_by_requirement.items()},
            applied_knowledge_revision_refs=frozenset(applied_revision_refs),
            knowledge_revision_refs_by_requirement={
                key: frozenset(value) for key, value in revision_refs_by_requirement.items()
            },
            work_budget=work_budget,
        )
    except EVIDENCE_AUTHORITY_ERRORS:
        raise
    except EvidenceObservationWorkLimitError:
        raise
    except SignalResolutionWorkLimitError:
        raise
    except Exception as exc:
        logger.warning(
            "evidence_resolution_failed",
            reason_code=EVIDENCE_RESOLUTION_FAILED,
            **evidence_failure_diagnostics(
                exc,
                reason_code=EVIDENCE_RESOLUTION_FAILED,
                requirement_count=len(requirements),
            ),
        )
        return EvidenceStageResult(
            requirements=requirements,
            resolutions=unresolved_resolutions_for_requirements(
                requirements,
                reason_code=EVIDENCE_RESOLUTION_FAILED,
            ),
            work_budget=work_budget,
        )
