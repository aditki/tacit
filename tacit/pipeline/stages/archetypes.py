"""Archetype selection and deterministic dashboard compilation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tacit.archetypes.engine import blend_archetypes, compile_archetype, rank_archetypes_by_coverage
from tacit.archetypes.generated import (
    ArchetypeRetrievalMode,
    GeneratedArchetypeQuery,
    GeneratedArchetypeRetrieval,
    load_experimental_archetypes,
)
from tacit.archetypes.templates import (
    get_archetype,
    get_archetypes_by_confidence,
    get_archetypes_by_learning_context,
)
from tacit.config import Settings
from tacit.logging import stage_log
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry


@dataclass(frozen=True)
class ArchetypeSelection:
    ranked_archetypes: list[tuple[Any, float]]
    learned_archetypes: list[tuple[Any, float]]
    experimental_archetypes: list[tuple[Any, float]]
    experimental_retrieval: GeneratedArchetypeRetrieval
    context_sources: dict[str, int]
    unexpected_cross_service_matches: int
    retrieval_mode: ArchetypeRetrievalMode
    target_language: str


@dataclass(frozen=True)
class ArchetypeCompilation:
    dashboard_spec: DashboardSpec
    primary_archetype: Any
    primary_confidence: float


def select_archetypes(
    *,
    intent: Intent,
    metric_catalog: list[MetricEntry],
    catalog_for_compile: list[MetricEntry],
    target_language: str,
    settings: Settings,
    tenant_id: str | None = None,
    environment_refs: list[str] | None = None,
    archetype_kind: str = "investigation_dashboard",
) -> ArchetypeSelection:
    """Select curated archetypes plus explicitly enabled exact-scope experiments."""
    ranked_archetypes = get_archetypes_by_confidence(intent.archetypes, min_confidence=0.3)
    ranked_ids = {arch.id for arch, _ in ranked_archetypes}
    learned_archetypes = get_archetypes_by_learning_context(
        intent,
        metric_catalog,
        min_confidence=0.35,
        exclude_ids=ranked_ids,
    )
    if learned_archetypes:
        ranked_archetypes.extend(learned_archetypes)
        ranked_archetypes.sort(key=lambda item: item[1], reverse=True)

    retrieval_mode = ArchetypeRetrievalMode(
        getattr(settings, "learned_archetypes_retrieval_mode", ArchetypeRetrievalMode.CURATED_ONLY)
    )
    experimental_archetypes: list[tuple[Any, float]] = []
    experimental_retrieval = GeneratedArchetypeRetrieval()
    unexpected_cross_service_matches = 0
    if retrieval_mode == ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE:
        exact_query = GeneratedArchetypeQuery.exact(
            tenant_id=str(tenant_id or getattr(settings, "learned_archetypes_tenant_id", "default") or "default"),
            service_refs=intent.services,
            environment_refs=environment_refs or [],
            archetype_kind=archetype_kind,
            generation_version=getattr(
                settings,
                "learned_archetypes_generation_version",
                "generated-archetype-v1",
            ),
        )
        experimental_retrieval = load_experimental_archetypes(
            Path(
                getattr(
                    settings,
                    "learned_archetypes_quarantine_path",
                    "data/generated_archetypes/quarantine",
                )
            ),
            exact_query,
        )
        existing_ids = {archetype.id for archetype, _ in ranked_archetypes}
        experimental_archetypes = [
            (archetype, 1.0) for archetype in experimental_retrieval.archetypes if archetype.id not in existing_ids
        ]
        unexpected_cross_service_matches = sum(
            archetype.service_refs != exact_query.service_refs for archetype, _ in experimental_archetypes
        )
        ranked_archetypes.extend(experimental_archetypes)

    if not ranked_archetypes:
        legacy = get_archetype(intent.problem_type)
        if legacy is not None:
            ranked_archetypes = [(legacy, 0.9)]

    if ranked_archetypes:
        ranked_archetypes = rank_archetypes_by_coverage(
            ranked_archetypes,
            catalog_for_compile,
            target_language=target_language,
            services=intent.services,
            max_archetypes=settings.max_blended_archetypes,
            min_secondary_coverage=settings.min_secondary_coverage,
        )

    experimental_ids = {archetype.id for archetype, _ in experimental_archetypes}
    experimental_archetypes = [
        (archetype, confidence) for archetype, confidence in ranked_archetypes if archetype.id in experimental_ids
    ]
    selected_experimental = len(experimental_archetypes)

    return ArchetypeSelection(
        ranked_archetypes=ranked_archetypes,
        learned_archetypes=learned_archetypes,
        experimental_archetypes=experimental_archetypes,
        experimental_retrieval=experimental_retrieval,
        context_sources={
            "curated_archetypes": len(ranked_archetypes) - selected_experimental,
            "operational_knowledge_items": 0,
            "generated_archetypes": selected_experimental,
        },
        unexpected_cross_service_matches=unexpected_cross_service_matches,
        retrieval_mode=retrieval_mode,
        target_language=target_language,
    )


def compile_selected_archetypes(
    *,
    selection: ArchetypeSelection,
    intent: Intent,
    catalog_for_compile: list[MetricEntry],
    timings: dict[str, float],
) -> ArchetypeCompilation | None:
    """Compile a dashboard from selected archetypes, if any."""
    if not selection.ranked_archetypes:
        return None

    t0 = time.monotonic()
    primary_arch, primary_conf = selection.ranked_archetypes[0]
    if len(selection.ranked_archetypes) > 1:
        dashboard_spec = blend_archetypes(
            selection.ranked_archetypes,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
        )
    else:
        dashboard_spec = compile_archetype(
            primary_arch,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
        )
    timings["archetype_compile"] = time.monotonic() - t0
    stage_log(
        "archetype_compile",
        (time.monotonic() - t0) * 1000,
        primary_archetype=primary_arch.id,
        primary_confidence=primary_conf,
        archetypes_matched=len(selection.ranked_archetypes),
        learned_archetypes_matched=len(selection.learned_archetypes),
        generated_archetypes_matched=len(selection.experimental_archetypes),
        investigation_context_sources=selection.context_sources,
        generated_rejected_by_scope=selection.experimental_retrieval.rejected_by_scope,
        generated_quarantined=selection.experimental_retrieval.quarantined,
        panels_generated=len(dashboard_spec.panels),
        target_language=selection.target_language,
        signal_bindings_count=len(primary_arch.signal_bindings),
    )
    return ArchetypeCompilation(
        dashboard_spec=dashboard_spec,
        primary_archetype=primary_arch,
        primary_confidence=primary_conf,
    )
