"""Archetype selection and deterministic dashboard compilation."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tacit.archetypes.engine import (
    ArchetypeCoverageWorkLimits,
    KnowledgeQueryUse,
    admit_intent_resolution_inputs,
    blend_archetypes,
    compile_archetype,
    dashboard_query_identities,
    rank_archetypes_by_coverage,
)
from tacit.archetypes.generated import (
    GENERATED_ARCHETYPE_ENVIRONMENT_SCOPE_REQUIRED,
    GENERATED_ARCHETYPE_SERVICE_SCOPE_REQUIRED,
    ArchetypeRetrievalMode,
    GeneratedArchetypeQuery,
    GeneratedArchetypeRetrieval,
    load_experimental_archetypes,
)
from tacit.archetypes.generated.schema import GeneratedArchetypeRetrievalStatus
from tacit.archetypes.templates import (
    curated_archetype_count,
    get_archetype,
    get_archetypes_by_confidence,
    get_archetypes_by_learning_context,
)
from tacit.config import Settings
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS
from tacit.knowledge.usage import (
    KnowledgeRevisionRef,
    KnowledgeStageUse,
    KnowledgeUsageEffect,
    KnowledgeUsageStage,
)
from tacit.logging import stage_log
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry
from tacit.pipeline.discovery import confirm_colloquial_keywords
from tacit.signals.resolution import SignalResolutionWorkBudget

MAX_DISCOVERY_ATTRIBUTION_REVISIONS = 32
MAX_DISCOVERY_ATTRIBUTION_SIGNAL_RESOLUTIONS = 1_024


@dataclass(frozen=True)
class InvestigationSignalResolutionWorkLimits:
    """Aggregate resolver limits shared by every deterministic investigation stage."""

    max_calls: int = 10_000
    max_mapping_catalog_comparisons: int = 8_000_000
    max_results: int = 100_000

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{field_name} must be a positive integer")


class _InvestigationBudgetResolver:
    """Keep nested legacy engine calls on the stage-owned investigation budget."""

    supports_signal_resolution_work_budget = True

    def __init__(self, resolver: Any, budget: SignalResolutionWorkBudget) -> None:
        self._resolver = resolver
        self._budget = budget

    def new_signal_resolution_work_budget(self, **_kwargs: Any) -> SignalResolutionWorkBudget:
        return self._budget

    def __getattr__(self, name: str) -> Any:
        return getattr(self._resolver, name)


def _resolver_with_investigation_budget(
    resolver: Any,
    budget: SignalResolutionWorkBudget | None,
) -> Any:
    if budget is None or not bool(getattr(resolver, "supports_signal_resolution_work_budget", False)):
        return resolver
    return _InvestigationBudgetResolver(resolver, budget)


def new_investigation_signal_resolution_work_budget(
    signal_store: Any,
    *,
    limits: InvestigationSignalResolutionWorkLimits | None = None,
) -> SignalResolutionWorkBudget | None:
    """Create one resolver budget at the investigation orchestration boundary."""
    factory = getattr(signal_store, "new_signal_resolution_work_budget", None)
    if not bool(getattr(signal_store, "supports_signal_resolution_work_budget", False)) or not callable(factory):
        return None
    active_limits = limits or InvestigationSignalResolutionWorkLimits()
    return factory(
        max_calls=active_limits.max_calls,
        max_mapping_catalog_comparisons=active_limits.max_mapping_catalog_comparisons,
        max_results=active_limits.max_results,
    )


@dataclass(frozen=True)
class ArchetypeSelectionWorkLimits:
    """Admission limits for deterministic selection and exact attribution."""

    max_classifier_matches: int = 64
    max_services: int = 64
    max_environments: int = 64
    max_keywords: int = 256
    max_keyword_evidence: int = 64
    max_metric_catalog_entries: int = 5_000
    max_compile_catalog_entries: int = 5_000
    max_curated_archetypes: int = 512
    max_attribution_retrieval_work: int = 200_000

    def __post_init__(self) -> None:
        for field_name in self.__dataclass_fields__:
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name} must be positive")


class DiscoveryAttributionWorkLimitError(RuntimeError):
    """Discovery attribution exceeded a stable request work dimension."""

    reason_code = "discovery_attribution_work_limit_exceeded"

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"{self.reason_code}: {dimension} exceeds {limit}")


def _raise_discovery_attribution_work_limit(
    dimension: str,
    observed: int,
    limit: int,
) -> None:
    stage_log(
        "archetype_selection_work_limit",
        0.0,
        status="failed",
        reason_code=DiscoveryAttributionWorkLimitError.reason_code,
        dimension=dimension,
        observed=min(max(observed, 0), 1_000_000),
        limit=min(max(limit, 0), 1_000_000),
    )
    raise DiscoveryAttributionWorkLimitError(dimension, observed, limit)


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
    max_blended_archetypes: int
    max_dashboard_panels: int
    min_secondary_coverage: float
    learned_archetype_min_coverage: float
    learned_archetype_boost: float
    knowledge_stage_uses: tuple[KnowledgeStageUse, ...] = ()

    @property
    def retrieval_reason_code(self) -> str:
        if self.retrieval_mode == ArchetypeRetrievalMode.CURATED_ONLY:
            return "curated_only"
        return _experimental_retrieval_reason_code(
            self.experimental_retrieval,
            has_matches=bool(self.shadow_archetypes),
        )

    @property
    def retrieval_stage_status(self) -> str:
        if self.retrieval_mode == ArchetypeRetrievalMode.CURATED_ONLY:
            return GeneratedArchetypeRetrievalStatus.PASSED.value
        return self.experimental_retrieval.status.value


def _experimental_retrieval_reason_code(
    retrieval: GeneratedArchetypeRetrieval,
    *,
    has_matches: bool,
) -> str:
    if retrieval.status != GeneratedArchetypeRetrievalStatus.PASSED:
        return retrieval.reason_code
    if has_matches:
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
    work_limits: ArchetypeSelectionWorkLimits | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> ArchetypeSelection:
    """Select authoritative curated archetypes and discover shadow-only generated candidates."""

    limits = work_limits or ArchetypeSelectionWorkLimits()
    refs_by_keyword: dict[str, frozenset[KnowledgeRevisionRef]] = getattr(
        confirmed_keywords,
        "revision_refs_by_keyword",
        {},
    )
    added_keywords: frozenset[str] = getattr(confirmed_keywords, "added_keywords", frozenset())
    discovery_refs = {revision_ref for refs in refs_by_keyword.values() for revision_ref in refs}
    raw_dimensions = (
        ("classifier_matches", len(intent.archetypes), limits.max_classifier_matches),
        ("services", len(intent.services), limits.max_services),
        ("environments", len(intent.environments), limits.max_environments),
        ("keywords", len(intent.keywords), limits.max_keywords),
        ("keyword_evidence", len(intent.keyword_evidence), limits.max_keyword_evidence),
        ("metric_catalog_entries", len(metric_catalog), limits.max_metric_catalog_entries),
        ("compile_catalog_entries", len(catalog_for_compile), limits.max_compile_catalog_entries),
        ("revision_count", len(discovery_refs), MAX_DISCOVERY_ATTRIBUTION_REVISIONS),
    )
    for dimension, observed, limit in raw_dimensions:
        if observed > limit:
            _raise_discovery_attribution_work_limit(dimension, observed, limit)

    coverage_work_limits = ArchetypeCoverageWorkLimits()
    admit_intent_resolution_inputs(intent, work_limits=coverage_work_limits)
    active_resolution_work_budget = resolution_work_budget or getattr(
        confirmed_keywords,
        "resolution_work_budget",
        None,
    )
    budget_factory = getattr(signal_store, "new_signal_resolution_work_budget", None)
    if (
        active_resolution_work_budget is None
        and bool(getattr(signal_store, "supports_signal_resolution_work_budget", False))
        and callable(budget_factory)
    ):
        active_resolution_work_budget = budget_factory(
            max_calls=(coverage_work_limits.max_total_resolver_calls + MAX_DISCOVERY_ATTRIBUTION_SIGNAL_RESOLUTIONS),
            max_mapping_catalog_comparisons=(coverage_work_limits.max_total_catalog_comparisons),
            max_results=coverage_work_limits.max_total_resolution_results,
        )

    curated_count = curated_archetype_count()
    if curated_count > limits.max_curated_archetypes:
        _raise_discovery_attribution_work_limit(
            "curated_archetypes",
            curated_count,
            limits.max_curated_archetypes,
        )
    retrieval_work = (len(discovery_refs) + 1) * (len(intent.archetypes) + len(metric_catalog) + curated_count)
    if retrieval_work > limits.max_attribution_retrieval_work:
        _raise_discovery_attribution_work_limit(
            "attribution_retrieval_work",
            retrieval_work,
            limits.max_attribution_retrieval_work,
        )

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
            learned_archetype_min_coverage=settings.learned_archetype_min_coverage,
            learned_archetype_boost=settings.learned_archetype_boost,
            signal_store=signal_store,
            tenant_id=tenant_id or "default",
            knowledge_scope=knowledge_scope,
            knowledge_stage_uses=stage_uses,
            work_limits=coverage_work_limits,
            resolution_work_budget=active_resolution_work_budget,
        )

    ranked_archetypes, learned_archetypes = retrieve(intent)

    retrieval_mode = ArchetypeRetrievalMode(
        getattr(settings, "learned_archetypes_retrieval_mode", ArchetypeRetrievalMode.CURATED_ONLY)
    )
    shadow_archetypes: list[tuple[Any, float]] = []
    experimental_retrieval = GeneratedArchetypeRetrieval()
    unexpected_cross_service_matches = 0
    if retrieval_mode == ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE:
        retrieval_started_at = time.monotonic()
        retrieval_exception_class = ""
        try:
            concrete_tenant = str(tenant_id or "").strip()
            if not concrete_tenant or concrete_tenant == "*":
                reason_code = "generated_archetype_concrete_tenant_required"
                experimental_retrieval = GeneratedArchetypeRetrieval(
                    status=GeneratedArchetypeRetrievalStatus.SKIPPED,
                    reason_code=reason_code,
                    reason_counts=((reason_code, 1),),
                )
            elif not intent.services:
                reason_code = GENERATED_ARCHETYPE_SERVICE_SCOPE_REQUIRED
                experimental_retrieval = GeneratedArchetypeRetrieval(
                    rejected_by_scope=1,
                    status=GeneratedArchetypeRetrievalStatus.SKIPPED,
                    reason_code=reason_code,
                    reason_counts=((reason_code, 1),),
                )
            elif not (intent.environments if environment_refs is None else environment_refs):
                reason_code = GENERATED_ARCHETYPE_ENVIRONMENT_SCOPE_REQUIRED
                experimental_retrieval = GeneratedArchetypeRetrieval(
                    rejected_by_scope=1,
                    status=GeneratedArchetypeRetrievalStatus.SKIPPED,
                    reason_code=reason_code,
                    reason_counts=((reason_code, 1),),
                )
            else:
                exact_query = GeneratedArchetypeQuery.exact(
                    tenant_id=concrete_tenant,
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
                    max_directory_entries=settings.learned_archetypes_retrieval_max_directory_entries,
                    max_files=settings.learned_archetypes_retrieval_max_files,
                    max_file_bytes=settings.learned_archetypes_retrieval_max_file_bytes,
                    max_total_bytes=settings.learned_archetypes_retrieval_max_total_bytes,
                    max_yaml_nodes=settings.learned_archetypes_retrieval_max_yaml_nodes,
                    max_yaml_depth=settings.learned_archetypes_retrieval_max_yaml_depth,
                    max_yaml_scalars=settings.learned_archetypes_retrieval_max_yaml_scalars,
                    max_yaml_scalar_bytes=settings.learned_archetypes_retrieval_max_yaml_scalar_bytes,
                    max_artifacts_per_file=(settings.learned_archetypes_retrieval_max_artifacts_per_file),
                    max_panels_per_file=settings.learned_archetypes_retrieval_max_panels_per_file,
                    max_queries_per_file=settings.learned_archetypes_retrieval_max_queries_per_file,
                    max_total_artifacts=settings.learned_archetypes_retrieval_max_total_artifacts,
                    max_total_panels=settings.learned_archetypes_retrieval_max_total_panels,
                    max_total_queries=settings.learned_archetypes_retrieval_max_total_queries,
                    max_results=settings.learned_archetypes_retrieval_max_results,
                )
                shadow_archetypes = [(archetype, 1.0) for archetype in experimental_retrieval.archetypes]
                unexpected_cross_service_matches = sum(
                    archetype.service_refs != exact_query.service_refs for archetype, _ in shadow_archetypes
                )
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except Exception as exc:
            retrieval_exception_class = type(exc).__name__[:64]
            experimental_retrieval = GeneratedArchetypeRetrieval(
                invalid=1,
                status=GeneratedArchetypeRetrievalStatus.SKIPPED,
                reason_code="generated_archetype_retrieval_failed",
                reason_counts=(("generated_archetype_retrieval_failed", 1),),
            )
        finally:
            retrieval_reason_code = _experimental_retrieval_reason_code(
                experimental_retrieval,
                has_matches=bool(shadow_archetypes),
            )
            stage_log(
                "archetype_retrieval",
                (time.monotonic() - retrieval_started_at) * 1000,
                directory_entries_discovered=(experimental_retrieval.directory_entries_discovered),
                files_discovered=experimental_retrieval.files_discovered,
                files_scanned=experimental_retrieval.files_scanned,
                bytes_scanned=experimental_retrieval.bytes_scanned,
                invalid=experimental_retrieval.invalid,
                oversized_files=experimental_retrieval.oversized_files,
                symlinks_rejected=experimental_retrieval.symlinks_rejected,
                quarantined=experimental_retrieval.quarantined,
                rejected_by_scope=experimental_retrieval.rejected_by_scope,
                rejected_by_limit=experimental_retrieval.rejected_by_limit,
                matches=len(experimental_retrieval.archetypes),
                total_artifacts=experimental_retrieval.total_artifacts,
                total_panels=experimental_retrieval.total_panels,
                total_queries=experimental_retrieval.total_queries,
                status=experimental_retrieval.status.value,
                reason_code=retrieval_reason_code,
                reason_counts=experimental_retrieval.reason_counts,
                exception_class=retrieval_exception_class,
            )

    knowledge_stage_uses: list[KnowledgeStageUse] = []
    ranked_archetypes = coverage_rank(ranked_archetypes, knowledge_stage_uses)

    selected_positions = {archetype.id: index for index, (archetype, _) in enumerate(ranked_archetypes)}
    from tacit.agents.synonyms import KEYWORD_SIGNALS

    distinct_confirmation_signals = {
        signal for keyword in added_keywords for signal in KEYWORD_SIGNALS.get(keyword, ())
    }
    attribution_signal_resolutions = len(discovery_refs) * len(distinct_confirmation_signals)
    if attribution_signal_resolutions > MAX_DISCOVERY_ATTRIBUTION_SIGNAL_RESOLUTIONS:
        _raise_discovery_attribution_work_limit(
            "signal_resolutions",
            attribution_signal_resolutions,
            MAX_DISCOVERY_ATTRIBUTION_SIGNAL_RESOLUTIONS,
        )
    base_keywords = [keyword for keyword in intent.keywords if keyword not in added_keywords]
    for revision_ref in sorted(discovery_refs):
        candidate_keywords = {
            keyword for keyword, refs in refs_by_keyword.items() if keyword in added_keywords and revision_ref in refs
        }
        if not candidate_keywords:
            continue
        reconfirmation_intent = intent.model_copy(
            deep=True,
            update={"keywords": list(base_keywords)},
        )
        reconfirmed = confirm_colloquial_keywords(
            reconfirmation_intent,
            metric_catalog,
            target_language,
            signal_store,
            tenant_id=tenant_id or "default",
            knowledge_scope=knowledge_scope,
            excluded_knowledge_refs={revision_ref},
            apply_to_intent=False,
            resolution_work_budget=active_resolution_work_budget,
        )
        if reconfirmed.degraded:
            continue
        attributable_keywords = candidate_keywords.difference(reconfirmed)
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
        max_blended_archetypes=settings.max_blended_archetypes,
        max_dashboard_panels=settings.max_dashboard_panels,
        min_secondary_coverage=settings.min_secondary_coverage,
        learned_archetype_min_coverage=settings.learned_archetype_min_coverage,
        learned_archetype_boost=settings.learned_archetype_boost,
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
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> ArchetypeCompilation | None:
    """Compile a dashboard from selected archetypes, if any."""
    if not selection.ranked_archetypes:
        return None

    t0 = time.monotonic()
    primary_arch, primary_conf = selection.ranked_archetypes[0]
    knowledge_query_uses: list[KnowledgeQueryUse] = []
    compilation_signal_store = _resolver_with_investigation_budget(signal_store, resolution_work_budget)
    if len(selection.ranked_archetypes) > 1:
        dashboard_spec = blend_archetypes(
            selection.ranked_archetypes,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
            signal_store=compilation_signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=knowledge_query_uses,
            max_archetypes=selection.max_blended_archetypes,
            min_secondary_coverage=selection.min_secondary_coverage,
            learned_archetype_min_coverage=selection.learned_archetype_min_coverage,
            learned_archetype_boost=selection.learned_archetype_boost,
            max_dashboard_panels=selection.max_dashboard_panels,
        )
    else:
        dashboard_spec = compile_archetype(
            primary_arch,
            intent,
            catalog_for_compile,
            target_language=selection.target_language,
            signal_store=compilation_signal_store,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            knowledge_query_uses=knowledge_query_uses,
            resolution_work_budget=resolution_work_budget,
        )
    if resolution_work_budget is not None:
        resolution_work_budget.raise_if_exhausted()
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
        generated_directory_entries_discovered=(selection.experimental_retrieval.directory_entries_discovered),
        generated_files_discovered=selection.experimental_retrieval.files_discovered,
        generated_bytes_scanned=selection.experimental_retrieval.bytes_scanned,
        generated_rejected_by_limit=selection.experimental_retrieval.rejected_by_limit,
        generated_symlinks_rejected=selection.experimental_retrieval.symlinks_rejected,
        generated_limit_reason_codes=selection.experimental_retrieval.limit_reason_codes,
        panels_generated=len(dashboard_spec.panels),
        target_language=selection.target_language,
        signal_bindings_count=len(primary_arch.signal_bindings),
        signal_resolution_work=(resolution_work_budget.counters() if resolution_work_budget is not None else {}),
    )
    return ArchetypeCompilation(
        dashboard_spec=dashboard_spec,
        primary_archetype=primary_arch,
        primary_confidence=primary_conf,
        knowledge_query_uses=tuple(knowledge_query_uses),
    )
