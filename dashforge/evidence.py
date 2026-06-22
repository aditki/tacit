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

from dashforge.archetypes.schema import InvestigationArchetype
from dashforge.catalog import catalog_for_services
from dashforge.models.schemas import (
    DashboardSpec,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
)

_METRIC_TOKEN_CHARS = r"A-Za-z0-9_:."


def _query_mentions_metric(expr: str, metric: str) -> bool:
    if not metric:
        return False
    pattern = re.compile(rf"(?<![{_METRIC_TOKEN_CHARS}]){re.escape(metric)}(?![{_METRIC_TOKEN_CHARS}])")
    return bool(pattern.search(expr))


def requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    *,
    priority: str = "critical",
) -> list[EvidenceRequirement]:
    """Return declared evidence needs for one selected archetype."""
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
) -> list[EvidenceRequirement]:
    """Return evidence needs for the selected archetype set."""
    requirements: list[EvidenceRequirement] = []
    for archetype, _ in ranked_archetypes:
        requirements.extend(requirements_for_archetype(archetype, intent))
    return requirements


def resolve_requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
) -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    """Resolve one archetype's evidence needs against the live catalog."""
    from dashforge.archetypes.engine import (
        _datasource_type_for_language,
        _datasource_type_matches,
        _legacy_metric_signal,
        _substitution_shape_compatible,
    )
    from dashforge.signals import get_signal_store

    requirements = requirements_for_archetype(archetype, intent)
    if not requirements:
        return requirements, []

    target_datasource_type = _datasource_type_for_language(target_language)
    target_catalog = [
        entry
        for entry in catalog
        if (not target_language or (entry.query_language or "").lower() == target_language.lower())
        and _datasource_type_matches(entry.datasource_type, target_datasource_type)
    ]
    resolution_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=True)
    catalog_by_name: dict[str, list[MetricEntry]] = defaultdict(list)
    for entry in resolution_catalog:
        if entry.name:
            catalog_by_name[entry.name].append(entry)

    try:
        store = get_signal_store()
    except Exception:
        store = None

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
            status="resolved",
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
            resolutions.append(
                resolved_from_entry(
                    requirement,
                    catalog_by_name[default_metric][0],
                    reason_code="default_metric_present",
                )
            )
            continue

        if store is None:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status="unknown",
                    reason_code="signal_store_unavailable",
                )
            )
            continue

        signal_type = requirement.signal_type
        if not signal_type and default_metric:
            signal_type = _legacy_metric_signal(store, default_metric, target_catalog, target_language)
        if not signal_type:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status="unresolved",
                    reason_code="no_semantic_signal_for_requirement",
                )
            )
            continue

        resolved = store.resolve_signal(
            signal_type,
            resolution_catalog,
            context_service=intent.services[0] if intent.services else "",
            context_datasource_type=target_datasource_type,
            context_archetype=archetype.id,
            target_query_language=target_language,
        )
        compatible = [
            (entry, score)
            for entry, score in resolved
            if not default_metric or _substitution_shape_compatible(archetype, default_metric, entry)
        ]
        if not compatible:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status="unresolved",
                    reason_code="no_compatible_live_signal",
                )
            )
            continue

        best_score = compatible[0][1]
        best = [item for item in compatible if item[1] == best_score]
        best_names = {entry.name for entry, _ in best}
        if len(best_names) > 1:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status="unresolved",
                    reason_code="ambiguous_live_signal",
                    semantic_score=best_score,
                )
            )
            continue

        entry, score = best[0]
        resolutions.append(
            resolved_from_entry(
                requirement,
                entry,
                reason_code="live_signal_resolved",
                semantic_score=score,
                ownership_score=1.0,
            )
        )

    return requirements, resolutions


def resolve_requirements_for_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
) -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    """Resolve evidence needs for all selected archetypes."""
    requirements: list[EvidenceRequirement] = []
    resolutions: list[EvidenceResolution] = []
    for archetype, _ in ranked_archetypes:
        arch_requirements, arch_resolutions = resolve_requirements_for_archetype(
            archetype,
            intent,
            catalog,
            target_language=target_language,
        )
        requirements.extend(arch_requirements)
        resolutions.extend(arch_resolutions)
    return requirements, resolutions


def observe_evidence(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    pre_validation: DashboardSpec,
    post_validation: DashboardSpec,
) -> list[EvidenceObservation]:
    """Record whether resolved evidence appears in a query that survived validation."""
    requirements_by_id = {requirement.id: requirement for requirement in requirements}
    surviving_queries = {
        (query.expr, query.datasource_uid)
        for panel in post_validation.panels
        for query in panel.queries
        if query.expr
    }
    observations: list[EvidenceObservation] = []

    for resolution in resolutions:
        requirement = requirements_by_id.get(resolution.requirement_id)
        if resolution.status != "resolved" or requirement is None:
            observations.append(
                EvidenceObservation(
                    requirement_id=resolution.requirement_id,
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
                if any(_query_mentions_metric(query.expr, token) for token in metric_tokens):
                    survived = (query.expr, query.datasource_uid) in surviving_queries
                    matches.append(
                        EvidenceObservation(
                            requirement_id=requirement.id,
                            resolution_metric=resolution.metric,
                            panel_title=panel.title,
                            query=query.expr,
                            datasource_uid=query.datasource_uid,
                            valid_query=survived,
                            non_empty=survived,
                            survived=survived,
                            rejection_reason="" if survived else "query_rejected_by_validation",
                        )
                    )
        if matches:
            observations.extend(matches)
        else:
            observations.append(
                EvidenceObservation(
                    requirement_id=requirement.id,
                    resolution_metric=resolution.metric,
                    rejection_reason="resolved_metric_not_observed_in_queries",
                )
            )
    return observations


def summarize_evidence(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    observations: list[EvidenceObservation],
) -> dict[str, object]:
    """Return compact counts suitable for stage history and benchmark gates."""
    resolved_ids = {resolution.requirement_id for resolution in resolutions if resolution.status == "resolved"}
    surviving_ids = {observation.requirement_id for observation in observations if observation.survived}
    critical_ids = {requirement.id for requirement in requirements if requirement.priority == "critical"}
    critical_resolved = critical_ids & resolved_ids
    critical_survived = critical_ids & surviving_ids
    unresolved_reasons: dict[str, int] = {}
    for resolution in resolutions:
        if resolution.status == "resolved":
            continue
        unresolved_reasons[resolution.reason_code] = unresolved_reasons.get(resolution.reason_code, 0) + 1

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
        "requirements": [requirement.model_dump() for requirement in requirements],
        "resolutions": [resolution.model_dump() for resolution in resolutions],
        "observations": [observation.model_dump() for observation in observations],
    }
