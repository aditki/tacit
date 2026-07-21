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
    shadow_archetypes: list[tuple[Any, float]]
    experimental_retrieval: GeneratedArchetypeRetrieval
    context_sources: dict[str, int]
    unexpected_cross_service_matches: int
    retrieval_mode: ArchetypeRetrievalMode
    target_language: str

    @property
    def retrieval_reason_code(self) -> str:
        if self.retrieval_mode == ArchetypeRetrievalMode.CURATED_ONLY:
            return "curated_only"
        if self.shadow_archetypes:
            return "experimental_exact_scope_shadow_only"
        return "experimental_exact_scope_no_match"


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
    signal_store: Any | None = None,
) -> ArchetypeSelection:
    """Select authoritative curated archetypes and discover shadow-only generated candidates."""
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
    shadow_archetypes: list[tuple[Any, float]] = []
    experimental_retrieval = GeneratedArchetypeRetrieval()
    unexpected_cross_service_matches = 0
    if retrieval_mode == ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE:
        exact_query = GeneratedArchetypeQuery.exact(
            tenant_id=str(tenant_id or getattr(settings, "learned_archetypes_tenant_id", "default") or "default"),
            service_refs=intent.services,
            environment_refs=intent.environments if environment_refs is None else environment_refs,
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
        shadow_archetypes = [(archetype, 1.0) for archetype in experimental_retrieval.archetypes]
        unexpected_cross_service_matches = sum(
            archetype.service_refs != exact_query.service_refs for archetype, _ in shadow_archetypes
        )

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
            signal_store=signal_store,
        )

    return ArchetypeSelection(
        ranked_archetypes=ranked_archetypes,
        learned_archetypes=learned_archetypes,
        shadow_archetypes=shadow_archetypes,
        experimental_retrieval=experimental_retrieval,
        context_sources={
            "curated_archetypes": len(ranked_archetypes),
            "operational_knowledge_items": 0,
            "generated_archetypes": 0,
            "shadow_generated_archetypes": len(shadow_archetypes),
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
    signal_store: Any | None = None,
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
            signal_store=signal_store,
        )
    else:
        dashboard_spec = compile_archetype(
            primary_arch,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
            signal_store=signal_store,
        )
    timings["archetype_compile"] = time.monotonic() - t0
    stage_log(
        "archetype_compile",
        (time.monotonic() - t0) * 1000,
        primary_archetype=primary_arch.id,
        primary_confidence=primary_conf,
        archetypes_matched=len(selection.ranked_archetypes),
        learned_archetypes_matched=len(selection.learned_archetypes),
        generated_archetypes_matched=0,
        generated_shadow_candidates=len(selection.shadow_archetypes),
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
