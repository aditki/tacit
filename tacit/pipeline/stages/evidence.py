"""Evidence resolution stage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from tacit.errors import EvidenceResolutionError
from tacit.evidence import contributing_archetypes, resolve_requirements_for_archetypes
from tacit.models.schemas import DashboardSpec, EvidenceRequirement, EvidenceResolution, Intent, MetricEntry

logger = structlog.get_logger()


@dataclass(frozen=True)
class EvidenceStageResult:
    requirements: list[EvidenceRequirement]
    resolutions: list[EvidenceResolution]


def run_evidence_stage(
    *,
    ranked_archetypes: list[tuple[Any, float]],
    dashboard_spec: DashboardSpec,
    intent: Intent,
    catalog: list[MetricEntry],
    target_language: str,
    signal_store: Any | None = None,
) -> EvidenceStageResult:
    """Resolve evidence requirements for the archetypes that contributed panels."""
    if not ranked_archetypes:
        return EvidenceStageResult(requirements=[], resolutions=[])
    try:
        evidence_archetypes = contributing_archetypes(ranked_archetypes, dashboard_spec)
        requirements, resolutions = resolve_requirements_for_archetypes(
            evidence_archetypes,
            intent,
            catalog,
            target_language=target_language,
            signal_store=signal_store,
        )
        return EvidenceStageResult(requirements=requirements, resolutions=resolutions)
    except Exception:
        logger.warning(
            "evidence_resolution_failed",
            error_type=EvidenceResolutionError.__name__,
            exc_info=True,
        )
        return EvidenceStageResult(requirements=[], resolutions=[])
