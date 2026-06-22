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


def contributing_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    dashboard_spec: DashboardSpec,
) -> list[tuple[InvestigationArchetype, float]]:
    """Return selected archetypes that actually contributed compiled panels."""
    if not ranked_archetypes or not dashboard_spec.panels:
        return []
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


def resolve_requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
) -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    """Resolve one archetype's evidence needs against the live catalog."""
    from dashforge.archetypes.engine import (
        _archetype_query_languages,
        _datasource_type_for_language,
        _legacy_metric_signal,
        _substitution_shape_compatible,
    )
    from dashforge.signals import get_signal_store

    requirements = requirements_for_archetype(archetype, intent)
    if not requirements:
        return requirements, []

    query_languages = _archetype_query_languages(archetype, target_language)
    target_catalog = [
        entry for entry in catalog if not query_languages or (entry.query_language or "").lower() in query_languages
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
            owner = _unique_owner(catalog_by_name[default_metric])
            if owner is None:
                resolutions.append(
                    EvidenceResolution(
                        requirement_id=requirement.id,
                        status="unresolved",
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
                    status="unknown",
                    reason_code="signal_store_unavailable",
                )
            )
            continue

        signal_type = requirement.signal_type
        if not signal_type and default_metric:
            for language in sorted(query_languages or {target_language}):
                language_catalog = [
                    entry for entry in target_catalog if (entry.query_language or "").lower() == language.lower()
                ]
                signal_type = _legacy_metric_signal(store, default_metric, language_catalog, language)
                if signal_type:
                    break
        if not signal_type:
            resolutions.append(
                EvidenceResolution(
                    requirement_id=requirement.id,
                    status="unresolved",
                    reason_code="no_semantic_signal_for_requirement",
                )
            )
            continue

        resolved: list[tuple[MetricEntry, float]] = []
        for language in sorted(query_languages or {target_language}):
            target_datasource_type = _datasource_type_for_language(language)
            resolved.extend(
                store.resolve_signal(
                    signal_type,
                    resolution_catalog,
                    context_service=intent.services[0] if intent.services else "",
                    context_datasource_type=target_datasource_type,
                    context_archetype=archetype.id,
                    target_query_language=language,
                )
            )
        resolved.sort(key=lambda item: item[1], reverse=True)
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

        if requirement.evidence_type != "semantic_signal":
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

        entry, score = compatible[0]
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
) -> list[EvidenceObservation]:
    """Record whether resolved evidence appears in a query that survived validation."""
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
                    if validation_status:
                        non_empty = bool(surviving_query and surviving_query.validation_has_data)
                    else:
                        non_empty = survived
                    matches.append(
                        EvidenceObservation(
                            requirement_id=requirement.id,
                            resolution_metric=resolution.metric,
                            panel_title=panel.title,
                            query=query.expr,
                            datasource_uid=query.datasource_uid,
                            valid_query=valid_query,
                            non_empty=non_empty,
                            survived=survived,
                            rejection_reason=(
                                ""
                                if non_empty
                                else (
                                    validation_status or "query_rejected_by_validation"
                                    if survived
                                    else "query_rejected_by_validation"
                                )
                            ),
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
    surviving_ids = {observation.requirement_id for observation in observations if observation.non_empty}
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
