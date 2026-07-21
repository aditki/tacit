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
from typing import Any

from tacit.archetypes.schema import InvestigationArchetype
from tacit.catalog import catalog_for_services
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
)

_METRIC_TOKEN_CHARS = r"A-Za-z0-9_:."
_PROMETHEUS_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")
SUPPORTED_OBSERVATION = EvidenceObservationOutcome.SUPPORTED_OBSERVATION
MISSING_EVIDENCE = EvidenceObservationOutcome.MISSING_EVIDENCE
AMBIGUOUS_EVIDENCE = EvidenceObservationOutcome.AMBIGUOUS_EVIDENCE
NEGATIVE_EVIDENCE = EvidenceObservationOutcome.NEGATIVE_EVIDENCE
UNSUPPORTED_CAUSE = EvidenceObservationOutcome.UNSUPPORTED_CAUSE
_GAP_RESOLUTION_REASON_CODES = {
    "direct_symptom_signal_resolved",
    "evidence_gap_supported_observation",
}


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


def resolve_requirements_for_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
    signal_store: Any | None = None,
) -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    """Resolve one archetype's evidence needs against the live catalog."""
    from tacit.archetypes.engine import (
        _archetype_query_languages,
        _datasource_type_for_language,
        _legacy_metric_signal,
        _substitution_shape_compatible,
    )
    from tacit.signals import get_signal_store

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
        store = signal_store or get_signal_store()
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
                    status=EvidenceResolutionStatus.UNRESOLVED,
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
                    status=EvidenceResolutionStatus.UNRESOLVED,
                    reason_code="no_compatible_live_signal",
                )
            )
            continue

        if requirement.evidence_type != "semantic_signal":
            best_score = compatible[0][1]
            best = [item for item in compatible if item[1] == best_score]
            best_owners = {
                (entry.name, entry.datasource_uid, entry.datasource_type, entry.query_language) for entry, _ in best
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
    signal_store: Any | None = None,
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
            signal_store=signal_store,
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
