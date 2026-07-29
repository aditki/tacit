"""Evidence resolution stage."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import structlog

from tacit.errors import EvidenceResolutionError
from tacit.evidence import contributing_archetypes, resolve_requirements_for_archetypes
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.models.schemas import DashboardSpec, EvidenceRequirement, EvidenceResolution, Intent, MetricEntry

logger = structlog.get_logger()


@dataclass(frozen=True)
class EvidenceStageResult:
    requirements: list[EvidenceRequirement]
    resolutions: list[EvidenceResolution]
    applied_knowledge_refs: frozenset[str] = frozenset()
    knowledge_refs_by_requirement: dict[str, frozenset[str]] = field(default_factory=dict)
    applied_knowledge_revision_refs: frozenset[KnowledgeRevisionRef] = frozenset()
    knowledge_revision_refs_by_requirement: dict[str, frozenset[KnowledgeRevisionRef]] = field(default_factory=dict)


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
) -> EvidenceStageResult:
    """Resolve evidence requirements for the archetypes that contributed panels."""
    if not ranked_archetypes:
        return EvidenceStageResult(requirements=[], resolutions=[])
    try:
        evidence_archetypes = contributing_archetypes(ranked_archetypes, dashboard_spec)
        applied_knowledge_refs: set[str] = set()
        refs_by_requirement: dict[str, set[str]] = {}
        applied_revision_refs: set[KnowledgeRevisionRef] = set()
        revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] = {}
        requirements, resolutions = resolve_requirements_for_archetypes(
            evidence_archetypes,
            intent,
            catalog,
            target_language=target_language,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            applied_governance_refs=applied_knowledge_refs,
            governance_refs_by_requirement=refs_by_requirement,
            applied_governance_revision_refs=applied_revision_refs,
            governance_revision_refs_by_requirement=revision_refs_by_requirement,
        )
        return EvidenceStageResult(
            requirements=requirements,
            resolutions=resolutions,
            applied_knowledge_refs=frozenset(applied_knowledge_refs),
            knowledge_refs_by_requirement={key: frozenset(value) for key, value in refs_by_requirement.items()},
            applied_knowledge_revision_refs=frozenset(applied_revision_refs),
            knowledge_revision_refs_by_requirement={
                key: frozenset(value) for key, value in revision_refs_by_requirement.items()
            },
        )
    except Exception:
        logger.warning(
            "evidence_resolution_failed",
            error_type=EvidenceResolutionError.__name__,
            exc_info=True,
        )
        return EvidenceStageResult(requirements=[], resolutions=[])
