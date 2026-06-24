"""Archetype selection and deterministic dashboard compilation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from tacit.archetypes.engine import blend_archetypes, compile_archetype, rank_archetypes_by_coverage
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
) -> ArchetypeSelection:
    """Select learned/classifier archetypes and rank them by live coverage."""
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

    return ArchetypeSelection(
        ranked_archetypes=ranked_archetypes,
        learned_archetypes=learned_archetypes,
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
        panels_generated=len(dashboard_spec.panels),
        target_language=selection.target_language,
        signal_bindings_count=len(primary_arch.signal_bindings),
    )
    return ArchetypeCompilation(
        dashboard_spec=dashboard_spec,
        primary_archetype=primary_arch,
        primary_confidence=primary_conf,
    )
