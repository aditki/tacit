"""Archetype selection and deterministic dashboard compilation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tacit.archetypes.engine import (
    KnowledgeQueryUse,
    blend_archetypes,
    compile_archetype,
    dashboard_query_identities,
    rank_archetypes_by_coverage,
)
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
from tacit.knowledge.usage import (
    KnowledgeRevisionRef,
    KnowledgeStageUse,
    KnowledgeUsageEffect,
    KnowledgeUsageStage,
)
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
    knowledge_stage_uses: tuple[KnowledgeStageUse, ...] = ()

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
    knowledge_query_uses: tuple[KnowledgeQueryUse, ...]

    @property
    def applied_knowledge_refs(self) -> frozenset[str]:
        return frozenset(use.knowledge_ref for use in self.knowledge_query_uses)

    @property
    def applied_knowledge_revision_refs(self) -> frozenset[KnowledgeRevisionRef]:
        return frozenset(
            KnowledgeRevisionRef(use.knowledge_ref, use.knowledge_revision)
            for use in self.knowledge_query_uses
            if use.knowledge_revision > 0
        )

    def surviving_knowledge_refs(self, dashboard_spec: DashboardSpec) -> frozenset[str]:
        surviving_query_ids = dashboard_query_identities(dashboard_spec)
        return frozenset(
            use.knowledge_ref for use in self.knowledge_query_uses if use.query_identity() in surviving_query_ids
        )

    def surviving_knowledge_revision_refs(self, dashboard_spec: DashboardSpec) -> frozenset[KnowledgeRevisionRef]:
        surviving_query_ids = dashboard_query_identities(dashboard_spec)
        return frozenset(
            KnowledgeRevisionRef(use.knowledge_ref, use.knowledge_revision)
            for use in self.knowledge_query_uses
            if use.knowledge_revision > 0 and use.query_identity() in surviving_query_ids
        )


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
    knowledge_scope: Any | None = None,
    confirmed_keywords: list[str] | None = None,
) -> ArchetypeSelection:
    """Select authoritative curated archetypes and discover shadow-only generated candidates."""

    def retrieve(candidate_intent: Intent) -> tuple[list[tuple[Any, float]], list[tuple[Any, float]]]:
        candidates = get_archetypes_by_confidence(candidate_intent.archetypes, min_confidence=0.3)
        ranked_ids = {arch.id for arch, _ in candidates}
        learned = get_archetypes_by_learning_context(
            candidate_intent,
            metric_catalog,
            min_confidence=0.35,
            exclude_ids=ranked_ids,
        )
        if learned:
            candidates.extend(learned)
            candidates.sort(key=lambda item: item[1], reverse=True)
        if not candidates:
            legacy = get_archetype(candidate_intent.problem_type)
            if legacy is not None:
                candidates = [(legacy, 0.9)]
        return candidates, learned

    def coverage_rank(
        candidates: list[tuple[Any, float]],
        stage_uses: list[KnowledgeStageUse] | None = None,
    ) -> list[tuple[Any, float]]:
        if not candidates:
            return candidates
        return rank_archetypes_by_coverage(
            candidates,
            catalog_for_compile,
            target_language=target_language,
            services=intent.services,
            max_archetypes=settings.max_blended_archetypes,
            min_secondary_coverage=settings.min_secondary_coverage,
            signal_store=signal_store,
            tenant_id=tenant_id or "default",
            knowledge_scope=knowledge_scope,
            knowledge_stage_uses=stage_uses,
        )

    ranked_archetypes, learned_archetypes = retrieve(intent)

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

    knowledge_stage_uses: list[KnowledgeStageUse] = []
    ranked_archetypes = coverage_rank(ranked_archetypes, knowledge_stage_uses)

    refs_by_keyword: dict[str, frozenset[KnowledgeRevisionRef]] = getattr(
        confirmed_keywords,
        "revision_refs_by_keyword",
        {},
    )
    added_keywords: frozenset[str] = getattr(confirmed_keywords, "added_keywords", frozenset())
    selected_positions = {archetype.id: index for index, (archetype, _) in enumerate(ranked_archetypes)}
    discovery_refs = {revision_ref for refs in refs_by_keyword.values() for revision_ref in refs}
    for revision_ref in sorted(discovery_refs):
        attributable_keywords = {
            keyword for keyword, refs in refs_by_keyword.items() if keyword in added_keywords and revision_ref in refs
        }
        if not attributable_keywords:
            continue
        counterfactual_intent = intent.model_copy(
            update={
                "keywords": [keyword for keyword in intent.keywords if keyword not in attributable_keywords],
            }
        )
        counterfactual_candidates, _ = retrieve(counterfactual_intent)
        counterfactual_ranked = coverage_rank(counterfactual_candidates)
        counterfactual_positions = {archetype.id: index for index, (archetype, _) in enumerate(counterfactual_ranked)}
        for archetype_id, selected_position in selected_positions.items():
            if counterfactual_positions.get(archetype_id) == selected_position:
                continue
            use = KnowledgeStageUse(
                revision_ref=revision_ref,
                stage=KnowledgeUsageStage.ARCHETYPE_SELECTION,
                effect=KnowledgeUsageEffect.ARCHETYPE_SELECTED_BY_LIVE_COVERAGE,
                target_ref=f"archetype:{archetype_id}",
            )
            if use not in knowledge_stage_uses:
                knowledge_stage_uses.append(use)

    applied_stage_refs = {use.revision_ref for use in knowledge_stage_uses}

    return ArchetypeSelection(
        ranked_archetypes=ranked_archetypes,
        learned_archetypes=learned_archetypes,
        shadow_archetypes=shadow_archetypes,
        experimental_retrieval=experimental_retrieval,
        context_sources={
            "curated_archetypes": len(ranked_archetypes),
            "operational_knowledge_items": len(applied_stage_refs),
            "generated_archetypes": 0,
            "shadow_generated_archetypes": len(shadow_archetypes),
        },
        unexpected_cross_service_matches=unexpected_cross_service_matches,
        retrieval_mode=retrieval_mode,
        target_language=target_language,
        knowledge_stage_uses=tuple(knowledge_stage_uses),
    )


def compile_selected_archetypes(
    *,
    selection: ArchetypeSelection,
    intent: Intent,
    catalog_for_compile: list[MetricEntry],
    timings: dict[str, float],
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
) -> ArchetypeCompilation | None:
    """Compile a dashboard from selected archetypes, if any."""
    if not selection.ranked_archetypes:
        return None

    t0 = time.monotonic()
    primary_arch, primary_conf = selection.ranked_archetypes[0]
    knowledge_query_uses: list[KnowledgeQueryUse] = []
    if len(selection.ranked_archetypes) > 1:
        dashboard_spec = blend_archetypes(
            selection.ranked_archetypes,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=knowledge_query_uses,
        )
    else:
        dashboard_spec = compile_archetype(
            primary_arch,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=knowledge_query_uses,
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
        knowledge_query_uses=tuple(knowledge_query_uses),
    )
