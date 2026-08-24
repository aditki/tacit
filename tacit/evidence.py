"""First-class evidence accounting for investigation diagnostics.

Evidence is intentionally modeled as a lightweight lifecycle:

Need -> Resolved -> Observed

This module does not choose dashboards. It mirrors the current archetype binder
so tests and history can measure what evidence was required, what metric owned
that requirement, and whether the resulting query survived validation.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import structlog

from tacit.archetypes.schema import InvestigationArchetype
from tacit.catalog import catalog_for_services
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.models.schemas import (
    DashboardSpec,
    EvidenceLifecycleStatus,
    EvidenceObservation,
    EvidenceObservationOutcome,
    EvidenceRecord,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    MetricEntry,
    dashboard_spec_work_counts,
    validate_dashboard_nested_collection_work_limits,
    validate_dashboard_scalar_work_limits,
    validate_dashboard_spec_work_limits,
)
from tacit.signals.availability import resolve_signal_store
from tacit.signals.resolution import (
    SignalResolutionWorkBudget,
    signal_resolution_work_kwargs,
)

_METRIC_TOKEN_CHARS = r"A-Za-z0-9_:."
_PROMETHEUS_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")
SUPPORTED_OBSERVATION = EvidenceObservationOutcome.SUPPORTED_OBSERVATION
MISSING_EVIDENCE = EvidenceObservationOutcome.MISSING_EVIDENCE
AMBIGUOUS_EVIDENCE = EvidenceObservationOutcome.AMBIGUOUS_EVIDENCE
NEGATIVE_EVIDENCE = EvidenceObservationOutcome.NEGATIVE_EVIDENCE
UNSUPPORTED_CAUSE = EvidenceObservationOutcome.UNSUPPORTED_CAUSE
EVIDENCE_RESOLUTION_FAILED = "evidence_resolution_failed"
_EVIDENCE_OBSERVATION_WORK_LIMIT_EXCEEDED = "evidence_observation_work_limit_exceeded"
_GAP_RESOLUTION_REASON_CODES = {
    "direct_symptom_signal_resolved",
    "evidence_gap_supported_observation",
}

logger = structlog.get_logger()


@dataclass(frozen=True)
class EvidenceObservationWorkLimits:
    """Aggregate bounds for one validation stage's observation work."""

    max_ranked_archetypes: int = 64
    max_archetype_panels: int = 256
    max_services: int = 64
    max_catalog_entries: int = 5_000
    max_dimensions_per_catalog_entry: int = 128
    max_total_catalog_dimensions: int = 100_000
    max_requirements: int = 256
    max_resolutions: int = 512
    max_total_resolution_catalog_checks: int = 2_000_000
    max_signal_resolution_results_per_call: int = 5_000
    max_total_signal_resolution_results: int = 32_768
    max_observation_passes: int = 3
    max_total_query_checks: int = 2_000_000
    max_total_observation_slots: int = 32_768

    def __post_init__(self) -> None:
        for field_name in (
            "max_ranked_archetypes",
            "max_archetype_panels",
            "max_services",
            "max_catalog_entries",
            "max_dimensions_per_catalog_entry",
            "max_total_catalog_dimensions",
            "max_requirements",
            "max_resolutions",
            "max_total_resolution_catalog_checks",
            "max_signal_resolution_results_per_call",
            "max_total_signal_resolution_results",
            "max_observation_passes",
            "max_total_query_checks",
            "max_total_observation_slots",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name} must be positive")


class EvidenceObservationWorkLimitError(RuntimeError):
    """Evidence observation exceeded a stable, payload-free work bound."""

    reason_code = _EVIDENCE_OBSERVATION_WORK_LIMIT_EXCEEDED

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"{self.reason_code}: {dimension} exceeds {limit}")


def _raise_evidence_observation_work_limit(
    dimension: str,
    observed: int,
    limit: int,
) -> None:
    logger.warning(
        _EVIDENCE_OBSERVATION_WORK_LIMIT_EXCEEDED,
        reason_code=_EVIDENCE_OBSERVATION_WORK_LIMIT_EXCEEDED,
        dimension=dimension,
        observed=min(observed, 10_000_000),
        limit=min(limit, 10_000_000),
    )
    raise EvidenceObservationWorkLimitError(dimension, observed, limit)


@dataclass
class EvidenceObservationWorkBudget:
    """Mutable aggregate budget shared by initial, rescue, and final observation."""

    limits: EvidenceObservationWorkLimits = EvidenceObservationWorkLimits()
    total_resolution_catalog_checks: int = 0
    total_signal_resolution_results: int = 0
    observation_passes: int = 0
    total_query_checks: int = 0
    total_observation_slots: int = 0

    def validate_ranked_archetypes(
        self,
        ranked_archetypes: list[tuple[InvestigationArchetype, float]],
        *,
        project_requirements: bool,
    ) -> None:
        """Admit raw archetype inputs before panel or requirement traversal."""
        archetype_count = len(ranked_archetypes)
        if archetype_count > self.limits.max_ranked_archetypes:
            _raise_evidence_observation_work_limit(
                "ranked_archetypes",
                archetype_count,
                self.limits.max_ranked_archetypes,
            )
        archetype_ids = [archetype.id for archetype, _ in ranked_archetypes]
        duplicate_count = len(archetype_ids) - len(set(archetype_ids))
        if duplicate_count:
            _raise_evidence_observation_work_limit(
                "duplicate_archetype_ids",
                duplicate_count,
                0,
            )
        projected_requirements = 0
        for archetype, _ in ranked_archetypes:
            panel_count = len(archetype.panels)
            if panel_count > self.limits.max_archetype_panels:
                _raise_evidence_observation_work_limit(
                    "archetype_panels",
                    panel_count,
                    self.limits.max_archetype_panels,
                )
            if not project_requirements:
                continue
            projected_requirements += (
                len(archetype.required_signals) + len(archetype.signal_bindings) + len(archetype.required_metrics)
            )
            if projected_requirements > self.limits.max_requirements:
                _raise_evidence_observation_work_limit(
                    "projected_requirements",
                    projected_requirements,
                    self.limits.max_requirements,
                )

    def validate_archetypes(
        self,
        ranked_archetypes: list[tuple[InvestigationArchetype, float]],
        intent: Intent,
        *,
        project_requirements: bool,
    ) -> None:
        """Admit archetypes and their request scope before derived work."""
        self.validate_ranked_archetypes(
            ranked_archetypes,
            project_requirements=project_requirements,
        )
        service_count = len(intent.services)
        if service_count > self.limits.max_services:
            _raise_evidence_observation_work_limit(
                "services",
                service_count,
                self.limits.max_services,
            )

    def validate_catalog(
        self,
        catalog: list[MetricEntry],
        *,
        service_count: int = 0,
    ) -> None:
        """Admit a raw catalog before service filtering or resolver traversal."""
        catalog_count = len(catalog)
        if catalog_count > self.limits.max_catalog_entries:
            _raise_evidence_observation_work_limit(
                "catalog_entries",
                catalog_count,
                self.limits.max_catalog_entries,
            )
        if service_count > self.limits.max_services:
            _raise_evidence_observation_work_limit(
                "services",
                service_count,
                self.limits.max_services,
            )
        total_dimensions = 0
        for entry in catalog:
            dimension_count = len(entry.dimensions)
            if dimension_count > self.limits.max_dimensions_per_catalog_entry:
                _raise_evidence_observation_work_limit(
                    "dimensions_per_catalog_entry",
                    dimension_count,
                    self.limits.max_dimensions_per_catalog_entry,
                )
            total_dimensions += dimension_count
            if total_dimensions > self.limits.max_total_catalog_dimensions:
                _raise_evidence_observation_work_limit(
                    "total_catalog_dimensions",
                    total_dimensions,
                    self.limits.max_total_catalog_dimensions,
                )

    def validate_resolution_plan(
        self,
        ranked_archetypes: list[tuple[InvestigationArchetype, float]],
        requirements: list[EvidenceRequirement],
        catalog: list[MetricEntry],
        intent: Intent,
    ) -> None:
        """Bound aggregate catalog work before evidence resolution begins."""
        self.validate_archetypes(ranked_archetypes, intent, project_requirements=False)
        self.validate_inputs(requirements, [])
        self.validate_catalog(catalog, service_count=len(intent.services))
        projected_checks = len(catalog) * (
            len(ranked_archetypes) + len(requirements) * (4 + (2 * len(intent.services)))
        )
        self.reserve_resolution_catalog_checks(projected_checks)

    def reserve_resolution_catalog_checks(self, projected_checks: int) -> None:
        """Reserve aggregate catalog comparisons before resolution starts."""
        next_checks = self.total_resolution_catalog_checks + projected_checks
        if next_checks > self.limits.max_total_resolution_catalog_checks:
            _raise_evidence_observation_work_limit(
                "total_resolution_catalog_checks",
                next_checks,
                self.limits.max_total_resolution_catalog_checks,
            )
        self.total_resolution_catalog_checks = next_checks

    def reserve_signal_resolution_results(self, result_count: int) -> None:
        """Admit one resolver result before aggregation or sorting."""
        if result_count > self.limits.max_signal_resolution_results_per_call:
            _raise_evidence_observation_work_limit(
                "signal_resolution_results_per_call",
                result_count,
                self.limits.max_signal_resolution_results_per_call,
            )
        next_results = self.total_signal_resolution_results + result_count
        if next_results > self.limits.max_total_signal_resolution_results:
            _raise_evidence_observation_work_limit(
                "total_signal_resolution_results",
                next_results,
                self.limits.max_total_signal_resolution_results,
            )
        self.total_signal_resolution_results = next_results

    def validate_rescue_plan(
        self,
        requirements: list[EvidenceRequirement],
        resolutions: list[EvidenceResolution],
        catalog: list[MetricEntry],
        intent: Intent,
    ) -> None:
        """Bound one direct rescue helper before indexing or resolving inputs."""
        self.validate_inputs(requirements, resolutions)
        self.validate_catalog(catalog, service_count=len(intent.services))
        projected_checks = len(catalog) * (len(requirements) * (4 + (2 * len(intent.services))))
        self.reserve_resolution_catalog_checks(projected_checks)

    def diagnostics(self) -> dict[str, int]:
        """Return bounded, payload-free counters for stage observability."""
        return {
            "evidence_resolution_catalog_checks": self.total_resolution_catalog_checks,
            "evidence_resolution_catalog_check_limit": self.limits.max_total_resolution_catalog_checks,
            "evidence_signal_resolution_results": self.total_signal_resolution_results,
            "evidence_signal_resolution_result_limit": self.limits.max_total_signal_resolution_results,
            "observation_passes": self.observation_passes,
            "observation_pass_limit": self.limits.max_observation_passes,
            "evidence_query_checks": self.total_query_checks,
            "evidence_query_check_limit": self.limits.max_total_query_checks,
            "evidence_observation_slots": self.total_observation_slots,
            "evidence_observation_slot_limit": self.limits.max_total_observation_slots,
        }

    def validate_inputs(
        self,
        requirements: list[EvidenceRequirement],
        resolutions: list[EvidenceResolution],
    ) -> None:
        self.validate_counts(len(requirements), len(resolutions))

    def validate_counts(self, requirement_count: int, resolution_count: int) -> None:
        """Check projected collection sizes without allocating a combined list."""
        if requirement_count > self.limits.max_requirements:
            _raise_evidence_observation_work_limit(
                "requirements",
                requirement_count,
                self.limits.max_requirements,
            )
        if resolution_count > self.limits.max_resolutions:
            _raise_evidence_observation_work_limit(
                "resolutions",
                resolution_count,
                self.limits.max_resolutions,
            )

    def reserve_observation_pass(
        self,
        requirements: list[EvidenceRequirement],
        resolutions: list[EvidenceResolution],
        pre_validation: DashboardSpec,
        post_validation: DashboardSpec,
    ) -> None:
        """Reserve the worst-case pass before traversing or allocating observations."""
        self.validate_inputs(requirements, resolutions)
        next_passes = self.observation_passes + 1
        if next_passes > self.limits.max_observation_passes:
            _raise_evidence_observation_work_limit(
                "observation_passes",
                next_passes,
                self.limits.max_observation_passes,
            )

        _, pre_query_count = dashboard_spec_work_counts(pre_validation)
        _, post_query_count = dashboard_spec_work_counts(post_validation)
        resolved_count = sum(resolution.status == EvidenceResolutionStatus.RESOLVED for resolution in resolutions)
        unresolved_count = len(resolutions) - resolved_count

        # Each resolved query comparison can test the resolved and default
        # metrics, each with the base token plus three histogram suffixes.
        pass_query_checks = post_query_count + (resolved_count * pre_query_count * 8)
        next_query_checks = self.total_query_checks + pass_query_checks
        if next_query_checks > self.limits.max_total_query_checks:
            _raise_evidence_observation_work_limit(
                "total_query_checks",
                next_query_checks,
                self.limits.max_total_query_checks,
            )

        pass_observation_slots = unresolved_count + (resolved_count * max(1, pre_query_count))
        next_observation_slots = self.total_observation_slots + pass_observation_slots
        if next_observation_slots > self.limits.max_total_observation_slots:
            _raise_evidence_observation_work_limit(
                "total_observation_slots",
                next_observation_slots,
                self.limits.max_total_observation_slots,
            )

        validate_dashboard_nested_collection_work_limits(pre_validation)
        validate_dashboard_scalar_work_limits(pre_validation)
        if post_validation is not pre_validation:
            validate_dashboard_nested_collection_work_limits(post_validation)
            validate_dashboard_scalar_work_limits(post_validation)

        self.observation_passes = next_passes
        self.total_query_checks = next_query_checks
        self.total_observation_slots = next_observation_slots


def _gap_outcome(reason_code: str) -> EvidenceObservationOutcome:
    if "ambiguous" in reason_code:
        return AMBIGUOUS_EVIDENCE
    return MISSING_EVIDENCE


def _is_gap_resolution(resolution: EvidenceResolution) -> bool:
    return resolution.reason_code in _GAP_RESOLUTION_REASON_CODES


def _query_mentions_metric(expr: str, metric: str) -> bool:
    if not metric:
        return False
    pattern = re.compile(rf"(?<![{_METRIC_TOKEN_CHARS}]){re.escape(metric)}(?![{_METRIC_TOKEN_CHARS}])")
    return bool(pattern.search(expr))


def _query_mentions_requirement_metric(expr: str, metric: str) -> bool:
    if _query_mentions_metric(expr, metric):
        return True
    return any(_query_mentions_metric(expr, f"{metric}{suffix}") for suffix in _PROMETHEUS_HISTOGRAM_SUFFIXES)


def requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    *,
    priority: str = "critical",
    work_limits: EvidenceObservationWorkLimits | None = None,
) -> list[EvidenceRequirement]:
    """Return declared evidence needs for one selected archetype."""
    EvidenceObservationWorkBudget(work_limits or EvidenceObservationWorkLimits()).validate_archetypes(
        [(archetype, 1.0)],
        intent,
        project_requirements=True,
    )
    return _requirements_for_archetype(archetype, intent, priority=priority)


def _requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    *,
    priority: str,
) -> list[EvidenceRequirement]:
    """Build requirements after the enclosing collection has been admitted."""
    requirements: list[EvidenceRequirement] = []
    seen: set[tuple[str, str, str]] = set()

    def add(evidence_type: str, signal_type: str = "", default_metric: str = "") -> None:
        key = (evidence_type, signal_type, default_metric)
        if key in seen:
            return
        seen.add(key)
        requirements.append(
            EvidenceRequirement(
                id=f"{archetype.id}:{len(requirements) + 1}",
                evidence_type=evidence_type,
                signal_type=signal_type,
                default_metric=default_metric,
                priority=priority,
                service_scope=list(intent.services),
                source=archetype.id,
            )
        )

    for signal_type in archetype.required_signals:
        add("semantic_signal", signal_type=signal_type, default_metric=archetype.signal_bindings.get(signal_type, ""))
    for signal_type, default_metric in archetype.signal_bindings.items():
        add("semantic_signal", signal_type=signal_type, default_metric=default_metric)
    for metric in archetype.required_metrics:
        add("required_metric", default_metric=metric)
    return requirements


def requirements_for_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    intent: Intent,
    *,
    work_limits: EvidenceObservationWorkLimits | None = None,
) -> list[EvidenceRequirement]:
    """Return evidence needs for the selected archetype set."""
    budget = EvidenceObservationWorkBudget(work_limits or EvidenceObservationWorkLimits())
    budget.validate_archetypes(ranked_archetypes, intent, project_requirements=True)
    requirements: list[EvidenceRequirement] = []
    for archetype, _ in ranked_archetypes:
        requirements.extend(_requirements_for_archetype(archetype, intent, priority="critical"))
    budget.validate_inputs(requirements, [])
    return requirements


def unresolved_resolutions_for_requirements(
    requirements: list[EvidenceRequirement],
    *,
    reason_code: str,
) -> list[EvidenceResolution]:
    """Preserve declared obligations when their binding stage cannot complete."""
    return [
        EvidenceResolution(
            requirement_id=requirement.id,
            status=EvidenceResolutionStatus.UNRESOLVED,
            reason_code=reason_code,
        )
        for requirement in requirements
    ]


def contributing_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    dashboard_spec: DashboardSpec,
    *,
    work_limits: EvidenceObservationWorkLimits | None = None,
) -> list[tuple[InvestigationArchetype, float]]:
    """Return selected archetypes that actually contributed compiled panels."""
    budget = EvidenceObservationWorkBudget(work_limits or EvidenceObservationWorkLimits())
    budget.validate_ranked_archetypes(
        ranked_archetypes,
        project_requirements=False,
    )
    validate_dashboard_spec_work_limits(dashboard_spec)
    if not ranked_archetypes or not dashboard_spec.panels:
        return []
    source_ids = {panel.source_archetype for panel in dashboard_spec.panels if panel.source_archetype}
    if source_ids:
        return [(archetype, confidence) for archetype, confidence in ranked_archetypes if archetype.id in source_ids]
    contributed: list[tuple[InvestigationArchetype, float]] = []
    for index, (archetype, confidence) in enumerate(ranked_archetypes):
        template_titles = {panel.title for panel in archetype.panels}
        template_rows = {panel.row for panel in archetype.panels if panel.row}
        matching_panels = [panel for panel in dashboard_spec.panels if panel.title in template_titles]
        if index == 0 and matching_panels:
            contributed.append((archetype, confidence))
        elif any(panel.row == archetype.name or panel.row in template_rows for panel in matching_panels):
            contributed.append((archetype, confidence))
    return contributed


def _unique_owner(entries: list[MetricEntry]) -> MetricEntry | None:
    owners = {(entry.datasource_uid, entry.datasource_type, entry.query_language) for entry in entries}
    return entries[0] if len(owners) == 1 else None


def resolve_declared_requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    requirements: list[EvidenceRequirement],
    *,
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    applied_governance_refs: set[str] | None = None,
    governance_refs_by_requirement: dict[str, set[str]] | None = None,
    applied_governance_revision_refs: set[KnowledgeRevisionRef] | None = None,
    governance_revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] | None = None,
    work_limits: EvidenceObservationWorkLimits | None = None,
    work_budget: EvidenceObservationWorkBudget | None = None,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None = None,
    _work_admitted: bool = False,
) -> list[EvidenceResolution]:
    """Bind already-declared evidence needs for one archetype."""
    from tacit.archetypes.engine import (
        _archetype_query_languages,
        _datasource_type_for_language,
        _legacy_metric_signal_details,
        _substitution_shape_compatible,
    )
    from tacit.signals import get_signal_store

    if not requirements:
        return []

    budget = work_budget or EvidenceObservationWorkBudget(work_limits or EvidenceObservationWorkLimits())
    if not _work_admitted:
        budget.validate_resolution_plan(
            [(archetype, 1.0)],
            requirements,
            catalog,
            intent,
        )

    query_languages = _archetype_query_languages(archetype, target_language)
    target_catalog = [
        entry for entry in catalog if not query_languages or (entry.query_language or "").lower() in query_languages
    ]
    resolution_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=True)
    catalog_by_name: dict[str, list[MetricEntry]] = defaultdict(list)
    for entry in resolution_catalog:
        if entry.name:
            catalog_by_name[entry.name].append(entry)

    store = resolve_signal_store(signal_store, get_signal_store)

    resolutions: list[EvidenceResolution] = []

    def resolved_from_entry(
        requirement: EvidenceRequirement,
        entry: MetricEntry,
        *,
        reason_code: str,
        semantic_score: float = 1.0,
        ownership_score: float = 1.0,
    ) -> EvidenceResolution:
        return EvidenceResolution(
            requirement_id=requirement.id,
            status=EvidenceResolutionStatus.RESOLVED,
            reason_code=reason_code,
            metric=entry.name,
            datasource_uid=entry.datasource_uid,
            datasource_type=entry.datasource_type,
            query_language=entry.query_language,
            semantic_score=semantic_score,
            ownership_score=ownership_score,
        )

    for requirement in requirements:
        default_metric = requirement.default_metric
        if default_metric and default_metric in catalog_by_name:
            owner = _unique_owner(catalog_by_name[default_metric])
            if owner is None:
                resolutions.append(
                    EvidenceResolution(
                        requirement_id=requirement.id,
                        status=EvidenceResolutionStatus.UNRESOLVED,
                        reason_code="ambiguous_default_metric_owner",
                    )
                )
                continue
            resolutions.append(
                resolved_from_entry(
                    requirement,
                    owner,
                    reason_code="default_metric_present",
                )
            )
            continue

        if store is None:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status=EvidenceResolutionStatus.UNKNOWN,
                    reason_code="signal_store_unavailable",
                )
            )
            continue

        signal_type = requirement.signal_type
        inferred_by: KnowledgeRevisionRef | None = None
        if not signal_type and default_metric:
            for language in sorted(query_languages or {target_language}):
                language_catalog = [
                    entry for entry in target_catalog if (entry.query_language or "").lower() == language.lower()
                ]
                signal_type, inferred_by = _legacy_metric_signal_details(
                    store,
                    default_metric,
                    language_catalog,
                    language,
                    tenant_id,
                    knowledge_scope,
                    signal_resolution_work_budget,
                )
                if signal_type:
                    break
        if not signal_type:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status=EvidenceResolutionStatus.UNRESOLVED,
                    reason_code="no_semantic_signal_for_requirement",
                )
            )
            continue

        resolved = []
        for language in sorted(query_languages or {target_language}):
            target_datasource_type = _datasource_type_for_language(language)
            matches = store.resolve_signal_details(
                signal_type,
                resolution_catalog,
                context_service=intent.services[0] if intent.services else "",
                context_datasource_type=target_datasource_type,
                context_archetype=archetype.id,
                target_query_language=language,
                tenant_id=tenant_id,
                knowledge_scope=knowledge_scope,
                **signal_resolution_work_kwargs(store, signal_resolution_work_budget),
            )
            budget.reserve_signal_resolution_results(len(matches))
            resolved.extend(matches)
        resolved.sort(key=lambda item: item.confidence, reverse=True)
        compatible = [
            match
            for match in resolved
            if not default_metric or _substitution_shape_compatible(archetype, default_metric, match.entry)
        ]
        if not compatible:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status=EvidenceResolutionStatus.UNRESOLVED,
                    reason_code="no_compatible_live_signal",
                )
            )
            continue

        if requirement.evidence_type != "semantic_signal":
            best_score = compatible[0].confidence
            best = [item for item in compatible if item.confidence == best_score]
            best_owners = {
                (item.entry.name, item.entry.datasource_uid, item.entry.datasource_type, item.entry.query_language)
                for item in best
            }
            if len(best_owners) > 1:
                resolutions.append(
                    EvidenceResolution(
                        requirement_id=requirement.id,
                        status=EvidenceResolutionStatus.UNRESOLVED,
                        reason_code="ambiguous_live_signal",
                        semantic_score=best_score,
                    )
                )
                continue

        selected = compatible[0]
        entry, score = selected.entry, selected.confidence
        if applied_governance_refs is not None:
            if inferred_by:
                applied_governance_refs.add(inferred_by.knowledge_ref)
            if selected.governance_ref:
                applied_governance_refs.add(selected.governance_ref)
        if governance_refs_by_requirement is not None:
            refs = governance_refs_by_requirement.setdefault(requirement.id, set())
            if inferred_by:
                refs.add(inferred_by.knowledge_ref)
            if selected.governance_ref:
                refs.add(selected.governance_ref)
        selected_revision_ref = selected.knowledge_revision_ref
        if applied_governance_revision_refs is not None:
            if inferred_by is not None:
                applied_governance_revision_refs.add(inferred_by)
            if selected_revision_ref is not None:
                applied_governance_revision_refs.add(selected_revision_ref)
        if governance_revision_refs_by_requirement is not None:
            revision_refs = governance_revision_refs_by_requirement.setdefault(requirement.id, set())
            if inferred_by is not None:
                revision_refs.add(inferred_by)
            if selected_revision_ref is not None:
                revision_refs.add(selected_revision_ref)
        resolutions.append(
            resolved_from_entry(
                requirement,
                entry,
                reason_code="live_signal_resolved",
                semantic_score=score,
                ownership_score=1.0,
            )
        )

    return resolutions


def resolve_requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    applied_governance_refs: set[str] | None = None,
    governance_refs_by_requirement: dict[str, set[str]] | None = None,
    applied_governance_revision_refs: set[KnowledgeRevisionRef] | None = None,
    governance_revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] | None = None,
    work_limits: EvidenceObservationWorkLimits | None = None,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    """Declare and resolve one archetype's evidence needs against the live catalog."""
    requirements = requirements_for_archetype(archetype, intent, work_limits=work_limits)
    resolutions = resolve_declared_requirements_for_archetype(
        archetype,
        intent,
        catalog,
        requirements,
        target_language=target_language,
        signal_store=signal_store,
        tenant_id=tenant_id,
        knowledge_scope=knowledge_scope,
        applied_governance_refs=applied_governance_refs,
        governance_refs_by_requirement=governance_refs_by_requirement,
        applied_governance_revision_refs=applied_governance_revision_refs,
        governance_revision_refs_by_requirement=governance_revision_refs_by_requirement,
        work_limits=work_limits,
        signal_resolution_work_budget=signal_resolution_work_budget,
    )
    return requirements, resolutions


def resolve_declared_requirements_for_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    intent: Intent,
    catalog: list[MetricEntry],
    requirements: list[EvidenceRequirement],
    *,
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    applied_governance_refs: set[str] | None = None,
    governance_refs_by_requirement: dict[str, set[str]] | None = None,
    applied_governance_revision_refs: set[KnowledgeRevisionRef] | None = None,
    governance_revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] | None = None,
    work_limits: EvidenceObservationWorkLimits | None = None,
    work_budget: EvidenceObservationWorkBudget | None = None,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> list[EvidenceResolution]:
    """Bind a frozen declaration set for the selected archetypes."""
    budget = work_budget or EvidenceObservationWorkBudget(work_limits or EvidenceObservationWorkLimits())
    budget.validate_resolution_plan(ranked_archetypes, requirements, catalog, intent)
    requirements_by_source: dict[str, list[EvidenceRequirement]] = defaultdict(list)
    for requirement in requirements:
        requirements_by_source[requirement.source].append(requirement)

    resolutions: list[EvidenceResolution] = []
    for archetype, _ in ranked_archetypes:
        resolutions.extend(
            resolve_declared_requirements_for_archetype(
                archetype,
                intent,
                catalog,
                requirements_by_source.get(archetype.id, []),
                target_language=target_language,
                signal_store=signal_store,
                tenant_id=tenant_id,
                knowledge_scope=knowledge_scope,
                applied_governance_refs=applied_governance_refs,
                governance_refs_by_requirement=governance_refs_by_requirement,
                applied_governance_revision_refs=applied_governance_revision_refs,
                governance_revision_refs_by_requirement=governance_revision_refs_by_requirement,
                work_limits=work_limits,
                work_budget=budget,
                signal_resolution_work_budget=signal_resolution_work_budget,
                _work_admitted=True,
            )
        )
    return resolutions


def resolve_requirements_for_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    applied_governance_refs: set[str] | None = None,
    governance_refs_by_requirement: dict[str, set[str]] | None = None,
    applied_governance_revision_refs: set[KnowledgeRevisionRef] | None = None,
    governance_revision_refs_by_requirement: dict[str, set[KnowledgeRevisionRef]] | None = None,
    work_limits: EvidenceObservationWorkLimits | None = None,
    signal_resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    """Resolve evidence needs for all selected archetypes."""
    requirements = requirements_for_archetypes(
        ranked_archetypes,
        intent,
        work_limits=work_limits,
    )
    resolutions = resolve_declared_requirements_for_archetypes(
        ranked_archetypes,
        intent,
        catalog,
        requirements,
        target_language=target_language,
        signal_store=signal_store,
        tenant_id=tenant_id,
        knowledge_scope=knowledge_scope,
        applied_governance_refs=applied_governance_refs,
        governance_refs_by_requirement=governance_refs_by_requirement,
        applied_governance_revision_refs=applied_governance_revision_refs,
        governance_revision_refs_by_requirement=governance_revision_refs_by_requirement,
        work_limits=work_limits,
        signal_resolution_work_budget=signal_resolution_work_budget,
    )
    return requirements, resolutions


def _query_matches_resolution_owner(query, resolution: EvidenceResolution) -> bool:
    if resolution.datasource_uid and query.datasource_uid != resolution.datasource_uid:
        return False
    if resolution.datasource_type and query.datasource_type != resolution.datasource_type:
        return False
    if resolution.query_language and query.query_language != resolution.query_language:
        return False
    return True


def observe_evidence(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    pre_validation: DashboardSpec,
    post_validation: DashboardSpec,
    *,
    work_budget: EvidenceObservationWorkBudget | None = None,
) -> list[EvidenceObservation]:
    """Record whether resolved evidence appears in a query that survived validation."""
    budget = work_budget or EvidenceObservationWorkBudget()
    budget.reserve_observation_pass(
        requirements,
        resolutions,
        pre_validation,
        post_validation,
    )
    requirements_by_id = {requirement.id: requirement for requirement in requirements}
    surviving_queries = {
        (query.expr, query.datasource_uid): query
        for panel in post_validation.panels
        for query in panel.queries
        if query.expr
    }
    observations: list[EvidenceObservation] = []

    for resolution in resolutions:
        requirement = requirements_by_id.get(resolution.requirement_id)
        if resolution.status != EvidenceResolutionStatus.RESOLVED or requirement is None:
            observations.append(
                EvidenceObservation(
                    requirement_id=resolution.requirement_id,
                    outcome=_gap_outcome(resolution.reason_code),
                    resolution_metric=resolution.metric,
                    rejection_reason=resolution.reason_code,
                )
            )
            continue

        matches = []
        metric_tokens = {resolution.metric}
        if requirement.default_metric:
            metric_tokens.add(requirement.default_metric)
        for panel in pre_validation.panels:
            for query in panel.queries:
                if not query.expr:
                    continue
                if any(_query_mentions_requirement_metric(query.expr, token) for token in metric_tokens):
                    surviving_query = surviving_queries.get((query.expr, query.datasource_uid))
                    survived = surviving_query is not None and _query_matches_resolution_owner(
                        surviving_query, resolution
                    )
                    validation_status = surviving_query.validation_status if surviving_query else ""
                    valid_query = survived and validation_status not in {
                        "absent",
                        "bad_uid",
                        "syntax_error",
                        "error",
                    }
                    non_empty = bool(surviving_query and validation_status and surviving_query.validation_has_data)
                    outcome = SUPPORTED_OBSERVATION if non_empty else _gap_outcome(validation_status)
                    rejection_reason = ""
                    if not non_empty:
                        if survived:
                            rejection_reason = validation_status or "query_validation_unverified"
                        else:
                            rejection_reason = "query_rejected_by_validation"
                    matches.append(
                        EvidenceObservation(
                            requirement_id=requirement.id,
                            outcome=outcome,
                            resolution_metric=resolution.metric,
                            panel_title=panel.title,
                            query=query.expr,
                            datasource_uid=query.datasource_uid,
                            valid_query=valid_query,
                            non_empty=non_empty,
                            survived=survived,
                            rejection_reason=rejection_reason,
                        )
                    )
        if matches:
            observations.extend(matches)
        else:
            observations.append(
                EvidenceObservation(
                    requirement_id=requirement.id,
                    outcome=MISSING_EVIDENCE,
                    resolution_metric=resolution.metric,
                    rejection_reason="resolved_metric_not_observed_in_queries",
                )
            )
    return observations


def _is_supported_observation(observation: EvidenceObservation) -> bool:
    if observation.outcome:
        return observation.outcome == SUPPORTED_OBSERVATION
    return observation.non_empty


def _select_observation(observations: list[EvidenceObservation]) -> EvidenceObservation | None:
    if not observations:
        return None
    supported = [observation for observation in observations if _is_supported_observation(observation)]
    if supported:
        return supported[0]
    ambiguous = [observation for observation in observations if observation.outcome == AMBIGUOUS_EVIDENCE]
    if ambiguous:
        return ambiguous[0]
    return observations[0]


def _record_final_status(
    primary: EvidenceResolution | None,
    gap: EvidenceResolution | None,
    observation: EvidenceObservation | None,
) -> EvidenceLifecycleStatus:
    if observation is not None:
        if observation.outcome == EvidenceObservationOutcome.SUPPORTED_OBSERVATION:
            return EvidenceLifecycleStatus.SUPPORTED_OBSERVATION
        if observation.outcome == EvidenceObservationOutcome.AMBIGUOUS_EVIDENCE:
            return EvidenceLifecycleStatus.AMBIGUOUS_EVIDENCE
        if observation.outcome == EvidenceObservationOutcome.NEGATIVE_EVIDENCE:
            return EvidenceLifecycleStatus.NEGATIVE_EVIDENCE
        if observation.outcome == EvidenceObservationOutcome.UNSUPPORTED_CAUSE:
            return EvidenceLifecycleStatus.UNSUPPORTED_CAUSE
        return EvidenceLifecycleStatus.MISSING_EVIDENCE
    if gap is not None:
        if gap.status == EvidenceResolutionStatus.RESOLVED:
            return EvidenceLifecycleStatus.GAP_RESOLVED
        if "ambiguous" in gap.reason_code:
            return EvidenceLifecycleStatus.AMBIGUOUS_EVIDENCE
        return EvidenceLifecycleStatus.GAP_UNRESOLVED
    if primary is not None:
        if primary.status == EvidenceResolutionStatus.RESOLVED:
            return EvidenceLifecycleStatus.PRIMARY_RESOLVED
        if "ambiguous" in primary.reason_code:
            return EvidenceLifecycleStatus.AMBIGUOUS_EVIDENCE
        return EvidenceLifecycleStatus.PRIMARY_UNRESOLVED
    return EvidenceLifecycleStatus.REQUIRED


def build_evidence_records(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    observations: list[EvidenceObservation],
) -> list[EvidenceRecord]:
    """Bind requirement, primary/gap resolutions, observation, and final state."""
    resolutions_by_requirement: dict[str, list[EvidenceResolution]] = defaultdict(list)
    observations_by_requirement: dict[str, list[EvidenceObservation]] = defaultdict(list)
    for resolution in resolutions:
        resolutions_by_requirement[resolution.requirement_id].append(resolution)
    for observation in observations:
        observations_by_requirement[observation.requirement_id].append(observation)

    records: list[EvidenceRecord] = []
    for requirement in requirements:
        requirement_resolutions = resolutions_by_requirement.get(requirement.id, [])
        primary = next(
            (resolution for resolution in requirement_resolutions if not _is_gap_resolution(resolution)),
            None,
        )
        gap = next((resolution for resolution in requirement_resolutions if _is_gap_resolution(resolution)), None)
        selected_observation = _select_observation(observations_by_requirement.get(requirement.id, []))
        records.append(
            EvidenceRecord(
                requirement=requirement,
                primary_resolution=primary,
                gap_resolution=gap,
                observation=selected_observation,
                final_status=_record_final_status(primary, gap, selected_observation),
            )
        )
    return records


def summarize_evidence(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    observations: list[EvidenceObservation],
) -> dict[str, object]:
    """Return compact counts suitable for stage history and benchmark gates."""
    records = build_evidence_records(requirements, resolutions, observations)
    resolved_ids = {
        record.requirement.id
        for record in records
        if (record.gap_resolution and record.gap_resolution.status == EvidenceResolutionStatus.RESOLVED)
        or (record.primary_resolution and record.primary_resolution.status == EvidenceResolutionStatus.RESOLVED)
    }
    surviving_ids = {
        record.requirement.id
        for record in records
        if record.final_status == EvidenceLifecycleStatus.SUPPORTED_OBSERVATION
    }
    critical_ids = {requirement.id for requirement in requirements if requirement.priority == "critical"}
    critical_resolved = critical_ids & resolved_ids
    critical_survived = critical_ids & surviving_ids
    unresolved_reasons: dict[str, int] = {}
    observation_outcomes: dict[str, int] = {}
    lifecycle_statuses: dict[str, int] = {}
    for record in records:
        resolution = record.gap_resolution or record.primary_resolution
        if resolution is None or resolution.status == EvidenceResolutionStatus.RESOLVED:
            continue
        unresolved_reasons[resolution.reason_code] = unresolved_reasons.get(resolution.reason_code, 0) + 1
    for observation in observations:
        outcome = observation.outcome or (SUPPORTED_OBSERVATION if observation.non_empty else MISSING_EVIDENCE)
        outcome_key = outcome.value if isinstance(outcome, EvidenceObservationOutcome) else str(outcome)
        observation_outcomes[outcome_key] = observation_outcomes.get(outcome_key, 0) + 1
    for record in records:
        lifecycle_statuses[record.final_status.value] = lifecycle_statuses.get(record.final_status.value, 0) + 1

    def ratio(numerator: int, denominator: int) -> float:
        return round(numerator / denominator, 4) if denominator else 0.0

    return {
        "requirements_total": len(requirements),
        "requirements_resolved": len(resolved_ids),
        "requirements_observed": len(surviving_ids),
        "critical_total": len(critical_ids),
        "critical_resolved": len(critical_resolved),
        "critical_observed": len(critical_survived),
        "resolution_recall": ratio(len(resolved_ids), len(requirements)),
        "critical_resolution_recall": ratio(len(critical_resolved), len(critical_ids)),
        "survival_recall": ratio(len(surviving_ids), len(requirements)),
        "critical_survival_recall": ratio(len(critical_survived), len(critical_ids)),
        "unresolved_reasons": unresolved_reasons,
        "observation_outcomes": observation_outcomes,
        "lifecycle_statuses": lifecycle_statuses,
        "records": [record.model_dump() for record in records],
        "requirements": [requirement.model_dump() for requirement in requirements],
        "resolutions": [resolution.model_dump() for resolution in resolutions],
        "observations": [observation.model_dump() for observation in observations],
    }
