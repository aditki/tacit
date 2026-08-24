"""Archetype engine — resolves templates into concrete DashboardSpec.

Given an archetype + intent + discovered label values, deterministically
compiles query templates into real PromQL or SignalFlow depending on the
target backend. No LLM needed for query generation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, NoReturn

import structlog

from tacit.archetypes.schema import (
    MAX_ARCHETYPE_PANELS,
    MAX_ARCHETYPE_QUERIES_PER_PANEL,
    MAX_ARCHETYPE_REQUIRED_METRICS,
    MAX_ARCHETYPE_REQUIRED_SIGNALS,
    MAX_ARCHETYPE_SIGNAL_BINDINGS,
    MAX_ARCHETYPE_SIGNAL_REQUIREMENTS,
    MAX_ARCHETYPE_TAGS,
    MAX_ARCHETYPE_TOTAL_QUERIES,
    InvestigationArchetype,
    PanelTemplate,
    QueryTemplate,
)
from tacit.catalog import catalog_for_services, metric_matches_services
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS, safe_failure_diagnostics
from tacit.knowledge.usage import (
    KnowledgeRevisionRef,
    KnowledgeStageUse,
    KnowledgeUsageEffect,
    KnowledgeUsageStage,
)
from tacit.models.schemas import (
    DashboardSpec,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    QueryTarget,
)
from tacit.signals.availability import SIGNAL_STORE_UNAVAILABLE, resolve_signal_store
from tacit.signals.resolution import (
    ResolutionInputTextLimits,
    ResolutionInputWorkLimitError,
    SignalResolutionWorkBudget,
    SignalResolutionWorkLimitError,
    admit_resolution_input_text,
)

logger = structlog.get_logger()

_PROMETHEUS_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")
_ARCHETYPE_SIGNAL_RESOLUTION_FAILED = "archetype_signal_resolution_failed"
_ARCHETYPE_COVERAGE_SIGNAL_RESOLUTION_FAILED = "archetype_coverage_signal_resolution_failed"
_ARCHETYPE_COVERAGE_WORK_LIMIT_EXCEEDED = "archetype_coverage_work_limit_exceeded"

# Characters that are special in RE2 (used by PromQL) and need escaping.
# Note: dash `-` is NOT special in RE2 outside character classes.
_RE2_SPECIAL = frozenset(r"\.+*?()[]{}|^$")


@dataclass(frozen=True)
class ArchetypeCoverageWorkLimits:
    """Hard bounds for coverage ranking and exact knowledge attribution."""

    max_candidates: int = 64
    max_catalog_entries: int = 5_000
    max_dimensions_per_catalog_entry: int = 128
    max_total_catalog_dimensions: int = 100_000
    max_services: int = 64
    max_environments: int = 64
    max_intent_archetypes: int = 64
    max_keywords: int = 256
    max_keyword_evidence: int = 64
    max_keyword_evidence_fields: int = 8
    max_required_signals_per_archetype: int = MAX_ARCHETYPE_REQUIRED_SIGNALS
    max_signal_bindings_per_archetype: int = MAX_ARCHETYPE_SIGNAL_BINDINGS
    max_signal_requirements_per_archetype: int = MAX_ARCHETYPE_SIGNAL_REQUIREMENTS
    max_required_metrics_per_archetype: int = MAX_ARCHETYPE_REQUIRED_METRICS
    max_tags_per_archetype: int = MAX_ARCHETYPE_TAGS
    max_problem_types_per_archetype: int = 256
    max_panels_per_archetype: int = MAX_ARCHETYPE_PANELS
    max_queries_per_panel: int = MAX_ARCHETYPE_QUERIES_PER_PANEL
    max_total_queries_per_archetype: int = MAX_ARCHETYPE_TOTAL_QUERIES
    max_cloudwatch_dimensions_per_query: int = 128
    max_cloudwatch_dimension_values_per_query: int = 128
    max_total_cloudwatch_dimension_values_per_archetype: int = 4_096
    max_scalar_characters: int = 65_536
    max_scalar_utf8_bytes: int = 262_144
    max_total_input_characters: int = 2_000_000
    max_total_input_utf8_bytes: int = 8_000_000
    max_unique_revisions: int = 64
    max_counterfactual_candidate_scores: int = 1024
    max_total_resolver_calls: int = 8192
    max_total_catalog_comparisons: int = 8_000_000
    max_total_resolution_results: int = 100_000

    def __post_init__(self) -> None:
        for field_name in (
            "max_candidates",
            "max_catalog_entries",
            "max_dimensions_per_catalog_entry",
            "max_total_catalog_dimensions",
            "max_services",
            "max_environments",
            "max_intent_archetypes",
            "max_keywords",
            "max_keyword_evidence",
            "max_keyword_evidence_fields",
            "max_required_signals_per_archetype",
            "max_signal_bindings_per_archetype",
            "max_signal_requirements_per_archetype",
            "max_required_metrics_per_archetype",
            "max_tags_per_archetype",
            "max_problem_types_per_archetype",
            "max_panels_per_archetype",
            "max_queries_per_panel",
            "max_total_queries_per_archetype",
            "max_cloudwatch_dimensions_per_query",
            "max_cloudwatch_dimension_values_per_query",
            "max_total_cloudwatch_dimension_values_per_archetype",
            "max_scalar_characters",
            "max_scalar_utf8_bytes",
            "max_total_input_characters",
            "max_total_input_utf8_bytes",
            "max_unique_revisions",
            "max_counterfactual_candidate_scores",
            "max_total_resolver_calls",
            "max_total_catalog_comparisons",
            "max_total_resolution_results",
        ):
            if getattr(self, field_name) < 1:
                raise ValueError(f"{field_name} must be positive")


class ArchetypeCoverageWorkLimitError(RuntimeError):
    """Coverage ranking exceeded a stable, payload-free work dimension."""

    reason_code = _ARCHETYPE_COVERAGE_WORK_LIMIT_EXCEEDED

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"{self.reason_code}: {dimension} exceeds {limit}")


def _raise_archetype_coverage_work_limit(
    dimension: str,
    observed: int,
    limit: int,
) -> NoReturn:
    logger.warning(
        _ARCHETYPE_COVERAGE_WORK_LIMIT_EXCEEDED,
        reason_code=_ARCHETYPE_COVERAGE_WORK_LIMIT_EXCEEDED,
        dimension=dimension,
        observed=min(max(observed, 0), 100_000_000),
        limit=min(max(limit, 0), 100_000_000),
    )
    raise ArchetypeCoverageWorkLimitError(dimension, observed, limit)


def _resolve_archetype_signal_store(signal_store: Any | None) -> Any | None:
    """Resolve the optional store without hiding authority acquisition failures."""
    from tacit.signals import get_signal_store

    if signal_store is None:
        return get_signal_store()
    return resolve_signal_store(signal_store, get_signal_store)


def _signal_resolution_work_kwargs(
    store: Any,
    work_budget: SignalResolutionWorkBudget | None,
) -> dict[str, SignalResolutionWorkBudget]:
    """Pass aggregate work only to the explicit first-party resolver contract."""
    if work_budget is None or not bool(getattr(store, "supports_signal_resolution_work_budget", False)):
        return {}
    return {"work_budget": work_budget}


def _new_signal_resolution_work_budget(
    store: Any,
    *,
    max_calls: int,
    max_mapping_catalog_comparisons: int | None = None,
    max_results: int | None = None,
) -> SignalResolutionWorkBudget | None:
    """Create one operation budget only for a first-party store."""
    factory = getattr(store, "new_signal_resolution_work_budget", None)
    if not bool(getattr(store, "supports_signal_resolution_work_budget", False)) or not callable(factory):
        return None
    return factory(
        max_calls=max(1, max_calls),
        max_mapping_catalog_comparisons=max_mapping_catalog_comparisons,
        max_results=max_results,
    )


def _resolution_input_text_limits(
    limits: ArchetypeCoverageWorkLimits,
) -> ResolutionInputTextLimits:
    return ResolutionInputTextLimits(
        max_scalar_characters=limits.max_scalar_characters,
        max_scalar_utf8_bytes=limits.max_scalar_utf8_bytes,
        max_total_characters=limits.max_total_input_characters,
        max_total_utf8_bytes=limits.max_total_input_utf8_bytes,
    )


def _admit_archetype_text(
    values: list[object],
    limits: ArchetypeCoverageWorkLimits,
) -> dict[str, int]:
    try:
        return admit_resolution_input_text(
            values,
            limits=_resolution_input_text_limits(limits),
        )
    except ResolutionInputWorkLimitError as exc:
        _raise_archetype_coverage_work_limit(
            exc.dimension,
            exc.observed,
            exc.limit,
        )


def admit_intent_resolution_inputs(
    intent: Intent,
    *,
    work_limits: ArchetypeCoverageWorkLimits | None = None,
) -> dict[str, int]:
    """Admit intent text before retrieval, normalization, or matching."""
    limits = work_limits or ArchetypeCoverageWorkLimits()
    dimensions = (
        ("services", len(intent.services), limits.max_services),
        ("environments", len(intent.environments), limits.max_environments),
        ("intent_archetypes", len(intent.archetypes), limits.max_intent_archetypes),
        ("keywords", len(intent.keywords), limits.max_keywords),
        ("keyword_evidence", len(intent.keyword_evidence), limits.max_keyword_evidence),
    )
    for dimension, observed, limit in dimensions:
        if observed > limit:
            _raise_archetype_coverage_work_limit(dimension, observed, limit)
    for evidence in intent.keyword_evidence:
        if not isinstance(evidence, dict):
            _raise_archetype_coverage_work_limit("keyword_evidence_shape", 1, 0)
        if len(evidence) > limits.max_keyword_evidence_fields:
            _raise_archetype_coverage_work_limit(
                "keyword_evidence_fields",
                len(evidence),
                limits.max_keyword_evidence_fields,
            )

    values: list[object] = [
        intent.summary,
        intent.domain,
        intent.timerange,
        intent.problem_type,
        *intent.services,
        *intent.environments,
        *intent.keywords,
        *(getattr(match, "type", None) for match in intent.archetypes),
    ]
    for evidence in intent.keyword_evidence:
        values.extend(
            (
                evidence.get("keyword", ""),
                evidence.get("tier", ""),
                evidence.get("source", ""),
            )
        )
    return _admit_archetype_text(values, limits)


def _metric_entry_text_values(entry: MetricEntry) -> list[object]:
    return [
        entry.name,
        entry.datasource_uid,
        entry.datasource_name,
        entry.datasource_type,
        entry.query_language,
        entry.namespace,
        *entry.dimensions,
        entry.unit,
        entry.metric_type,
    ]


def _archetype_text_values(archetype: InvestigationArchetype) -> list[object]:
    values: list[object] = [
        archetype.id,
        archetype.name,
        archetype.description,
        archetype.default_timerange,
        *archetype.problem_types,
        *archetype.required_metrics,
        *archetype.required_signals,
        *archetype.signal_bindings.keys(),
        *archetype.signal_bindings.values(),
        *archetype.tags,
    ]
    for panel in archetype.panels:
        values.extend(
            (
                panel.title,
                panel.description,
                panel.panel_type,
                panel.row,
                panel.unit,
            )
        )
        for query in panel.queries:
            values.extend(
                (
                    query.expr,
                    query.legend_format,
                    query.query_language,
                    query.datasource_type,
                    query.cloudwatch_namespace,
                    query.cloudwatch_stat,
                    query.cloudwatch_region,
                )
            )
            for key, raw_value in query.cloudwatch_dimensions.items():
                values.append(key)
                if isinstance(raw_value, list):
                    values.extend(raw_value)
                else:
                    values.append(raw_value)
    return values


def admit_archetype_resolution_inputs(
    archetypes: list[InvestigationArchetype],
    catalog: list[MetricEntry],
    *,
    services: list[str] | None = None,
    work_limits: ArchetypeCoverageWorkLimits | None = None,
) -> dict[str, int]:
    """Bound collection shape, then text, before semantic engine work."""
    limits = work_limits or ArchetypeCoverageWorkLimits()
    top_level_dimensions = (
        ("candidate_count", len(archetypes), limits.max_candidates),
        ("catalog_entries", len(catalog), limits.max_catalog_entries),
        ("services", len(services or ()), limits.max_services),
    )
    for dimension, observed, limit in top_level_dimensions:
        if observed > limit:
            _raise_archetype_coverage_work_limit(dimension, observed, limit)

    total_catalog_dimensions = 0
    for entry in catalog:
        if not isinstance(entry.dimensions, list):
            _raise_archetype_coverage_work_limit("catalog_dimensions_shape", 1, 0)
        dimension_count = len(entry.dimensions)
        if dimension_count > limits.max_dimensions_per_catalog_entry:
            _raise_archetype_coverage_work_limit(
                "dimensions_per_catalog_entry",
                dimension_count,
                limits.max_dimensions_per_catalog_entry,
            )
        total_catalog_dimensions += dimension_count
        if total_catalog_dimensions > limits.max_total_catalog_dimensions:
            _raise_archetype_coverage_work_limit(
                "total_catalog_dimensions",
                total_catalog_dimensions,
                limits.max_total_catalog_dimensions,
            )

    for archetype in archetypes:
        raw_dimensions = (
            (
                "problem_types_per_archetype",
                len(archetype.problem_types),
                limits.max_problem_types_per_archetype,
            ),
            (
                "required_signals_per_archetype",
                len(archetype.required_signals),
                limits.max_required_signals_per_archetype,
            ),
            (
                "signal_bindings_per_archetype",
                len(archetype.signal_bindings),
                limits.max_signal_bindings_per_archetype,
            ),
            (
                "required_metrics_per_archetype",
                len(archetype.required_metrics),
                limits.max_required_metrics_per_archetype,
            ),
            ("tags_per_archetype", len(archetype.tags), limits.max_tags_per_archetype),
            ("panels_per_archetype", len(archetype.panels), limits.max_panels_per_archetype),
        )
        for dimension, observed, limit in raw_dimensions:
            if observed > limit:
                _raise_archetype_coverage_work_limit(dimension, observed, limit)
        if not isinstance(archetype.signal_bindings, dict):
            _raise_archetype_coverage_work_limit("signal_bindings_shape", 1, 0)

        total_query_count = 0
        total_cloudwatch_dimension_values = 0
        for panel in archetype.panels:
            query_count = len(panel.queries)
            if query_count > limits.max_queries_per_panel:
                _raise_archetype_coverage_work_limit(
                    "queries_per_panel",
                    query_count,
                    limits.max_queries_per_panel,
                )
            total_query_count += query_count
            if total_query_count > limits.max_total_queries_per_archetype:
                _raise_archetype_coverage_work_limit(
                    "total_queries_per_archetype",
                    total_query_count,
                    limits.max_total_queries_per_archetype,
                )
            for query in panel.queries:
                if not isinstance(query.cloudwatch_dimensions, dict):
                    _raise_archetype_coverage_work_limit(
                        "cloudwatch_dimensions_shape",
                        1,
                        0,
                    )
                dimension_count = len(query.cloudwatch_dimensions)
                if dimension_count > limits.max_cloudwatch_dimensions_per_query:
                    _raise_archetype_coverage_work_limit(
                        "cloudwatch_dimensions_per_query",
                        dimension_count,
                        limits.max_cloudwatch_dimensions_per_query,
                    )
                query_value_count = 0
                for raw_value in query.cloudwatch_dimensions.values():
                    value_count = len(raw_value) if isinstance(raw_value, list) else 1
                    if value_count > limits.max_cloudwatch_dimension_values_per_query:
                        _raise_archetype_coverage_work_limit(
                            "cloudwatch_dimension_values_per_query",
                            value_count,
                            limits.max_cloudwatch_dimension_values_per_query,
                        )
                    query_value_count += value_count
                total_cloudwatch_dimension_values += query_value_count
                if total_cloudwatch_dimension_values > limits.max_total_cloudwatch_dimension_values_per_archetype:
                    _raise_archetype_coverage_work_limit(
                        "total_cloudwatch_dimension_values_per_archetype",
                        total_cloudwatch_dimension_values,
                        limits.max_total_cloudwatch_dimension_values_per_archetype,
                    )

    text_values: list[object] = list(services or ())
    for entry in catalog:
        text_values.extend(_metric_entry_text_values(entry))
    for archetype in archetypes:
        text_values.extend(_archetype_text_values(archetype))
    counters = _admit_archetype_text(text_values, limits)
    counters["catalog_dimension_count"] = total_catalog_dimensions
    return counters


@dataclass(frozen=True)
class KnowledgeQueryUse:
    """One governed mapping's contribution to a compiled query."""

    knowledge_ref: str
    knowledge_revision: int
    source_archetype: str
    panel_title: str
    query_expr: str
    datasource_uid: str
    datasource_type: str
    query_language: str
    requirement_id: str = ""

    @classmethod
    def from_query(
        cls,
        knowledge_ref: str | KnowledgeRevisionRef,
        panel: PanelSpec,
        query: PanelQuery,
        *,
        knowledge_revision: int = 0,
        requirement_id: str = "",
    ) -> KnowledgeQueryUse:
        if isinstance(knowledge_ref, KnowledgeRevisionRef):
            knowledge_revision = knowledge_ref.knowledge_revision
            raw_ref = knowledge_ref.knowledge_ref
        else:
            raw_ref = knowledge_ref
        return cls(
            knowledge_ref=raw_ref,
            knowledge_revision=knowledge_revision,
            source_archetype=panel.source_archetype,
            panel_title=panel.title,
            query_expr=query.expr,
            datasource_uid=query.datasource_uid,
            datasource_type=query.datasource_type,
            query_language=query.query_language,
            requirement_id=requirement_id,
        )

    def query_identity(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.source_archetype,
            self.panel_title,
            self.query_expr,
            self.datasource_uid,
            self.datasource_type,
            self.query_language,
        )


def dashboard_query_identities(dashboard_spec: DashboardSpec) -> set[tuple[str, str, str, str, str, str]]:
    return {
        (
            panel.source_archetype,
            panel.title,
            query.expr,
            query.datasource_uid,
            query.datasource_type,
            query.query_language,
        )
        for panel in dashboard_spec.panels
        for query in panel.queries
    }


def _query_references_metric(query_expr: str, metric: str) -> bool:
    """Match one exact metric token across supported query syntaxes."""
    token_character = r"a-zA-Z0-9_:./-"
    return (
        re.search(
            rf"(?<![{token_character}]){re.escape(metric)}(?![{token_character}])",
            query_expr,
        )
        is not None
    )


def _query_changes_under_metric_substitution(query_expr: str, old_metric: str, new_metric: str) -> bool:
    """Use the compiler's substitution semantics to attribute a query change."""
    return _suffix_aware_replace(query_expr, old_metric, new_metric) != query_expr


def _re2_escape(s: str) -> str:
    """Escape a string for safe use in PromQL regex matchers."""
    return "".join(f"\\{c}" if c in _RE2_SPECIAL else c for c in s)


def _find_best_label(
    intent: Intent,
    catalog: list[MetricEntry],
    label_priority: dict[str, int] | None = None,
    restrict_to: set[str] | None = None,
) -> tuple[str, str] | None:
    """Find the best (label_name, value) pair for the target service.

    Shared logic for both PromQL and SignalFlow filter resolution.
    """
    if not intent.services:
        return None

    target = intent.services[0].lower().replace(" ", "-")
    _LABEL_PRIORITY = label_priority or {"service": 0, "app": 1, "application": 1, "container": 2, "pod": 3}
    candidates: list[tuple[int, str, str]] = []

    for entry in catalog:
        for dim in entry.dimensions:
            match = re.match(r"(\w+)=\{(.+)\}", dim)
            if not match:
                continue
            label_name, values_str = match.group(1), match.group(2)
            if restrict_to and label_name not in restrict_to:
                continue
            values = [v.strip() for v in values_str.split(",")]
            for val in values:
                val_normalized = val.lower().replace("_", "-")
                if target in val_normalized or val_normalized in target:
                    priority = _LABEL_PRIORITY.get(label_name, 10)
                    candidates.append((priority, label_name, val))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1], candidates[0][2]
    return None


def _resolve_service_filter(
    intent: Intent,
    catalog: list[MetricEntry],
) -> str:
    """Build the PromQL label selector for the target service."""
    result = _find_best_label(intent, catalog)
    if result:
        label_name, val = result
        return f'{label_name}="{val}"'

    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f'service=~".*{_re2_escape(target)}.*"'


def _resolve_container_filter(
    intent: Intent,
    catalog: list[MetricEntry],
) -> str:
    """Build PromQL label selector for container-level metrics."""
    result = _find_best_label(intent, catalog, restrict_to={"container", "pod"})
    if result:
        label_name, val = result
        return f'{label_name}="{val}"'

    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f'container=~".*{_re2_escape(target)}.*"'


_PROMETHEUS_DATASOURCE_TYPES = {"prometheus", "mimir", "cortex", "thanos"}
_SIGNALFX_DATASOURCE_TYPES = {"signalfx", "grafana-signalfx-datasource"}


def _datasource_type_matches(candidate: str, requested: str) -> bool:
    candidate = candidate.lower()
    requested = requested.lower()
    if not requested:
        return True
    if candidate == requested:
        return True
    if candidate in _PROMETHEUS_DATASOURCE_TYPES and requested in _PROMETHEUS_DATASOURCE_TYPES:
        return True
    if candidate in _SIGNALFX_DATASOURCE_TYPES and requested in _SIGNALFX_DATASOURCE_TYPES:
        return True
    return False


def _datasource_type_for_language(query_language: str, fallback: str = "prometheus") -> str:
    return {
        "signalflow": "signalfx",
        "logql": "loki",
        "cloudwatch": "cloudwatch",
        "lucene": "elasticsearch",
        "graphite": "graphite",
        "influxql": "influxdb",
    }.get(query_language.lower(), fallback)


def _resolve_query_target(
    catalog: list[MetricEntry],
    datasource_type: str = "",
    query_language: str = "",
    fallback_uid: str = "",
) -> QueryTarget:
    """Resolve datasource identity as one object, preferring matching catalog entries."""
    query_language = query_language.lower()
    datasource_type = datasource_type.lower()
    matched_entries: list[MetricEntry] = []
    for entry in catalog:
        if datasource_type and not _datasource_type_matches(entry.datasource_type, datasource_type):
            continue
        if query_language and (entry.query_language or "").lower() != query_language:
            continue
        matched_entries.append(entry)
    for entry in matched_entries:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if matched_entries:
        return QueryTarget.from_metric(matched_entries[0])
    type_matched_entries: list[MetricEntry] = []
    for entry in catalog:
        if datasource_type and _datasource_type_matches(entry.datasource_type, datasource_type):
            type_matched_entries.append(entry)
    for entry in type_matched_entries:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if type_matched_entries:
        return QueryTarget.from_metric(type_matched_entries[0])
    if datasource_type or query_language:
        return QueryTarget(
            datasource_uid=fallback_uid,
            datasource_type=datasource_type or _datasource_type_for_language(query_language, ""),
            query_language=query_language,
        )
    for entry in catalog:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if catalog:
        return QueryTarget.from_metric(catalog[0])
    return QueryTarget(
        datasource_uid=fallback_uid,
        datasource_type=datasource_type,
        query_language=query_language,
    )


def _resolve_promql_query_target(
    catalog: list[MetricEntry],
    expr: str,
    default_target: QueryTarget,
    intent: Intent,
) -> QueryTarget:
    """Route a PromQL query to the datasource that actually owns its metrics."""
    from tacit.dashboard_ingest import extract_metrics_from_promql

    metric_names = set(extract_metrics_from_promql(expr))
    if metric_names:
        candidates = [
            entry
            for entry in catalog
            if entry.name in metric_names
            and _datasource_type_matches(entry.datasource_type, "prometheus")
            and (not entry.query_language or entry.query_language.lower() == "promql")
        ]
        if len(metric_names) == 1 and len(candidates) == 1:
            return QueryTarget.from_metric(candidates[0])

        owners_by_metric = {
            metric: {entry.datasource_uid for entry in candidates if entry.name == metric} for metric in metric_names
        }
        if owners_by_metric and all(owners_by_metric.values()):
            common_owners = set.intersection(*owners_by_metric.values())
            if len(common_owners) == 1:
                owner = next(iter(common_owners))
                return QueryTarget.from_metric(next(entry for entry in candidates if entry.datasource_uid == owner))

        service_candidates = [entry for entry in candidates if metric_matches_services(entry, intent.services)]
        service_owners = {entry.datasource_uid for entry in service_candidates}
        complete_service_owners = {
            owner
            for owner in service_owners
            if all(owner in owners_by_metric.get(metric, set()) for metric in metric_names)
        }
        if len(complete_service_owners) == 1:
            owner = next(iter(complete_service_owners))
            return QueryTarget.from_metric(next(entry for entry in service_candidates if entry.datasource_uid == owner))
        if owners_by_metric and all(owners_by_metric.values()):
            common_owners = set.intersection(*owners_by_metric.values())
            for entry in candidates:
                if entry.datasource_is_default and entry.datasource_uid in common_owners:
                    return QueryTarget.from_metric(entry)
            if common_owners:
                owner_entry = next(entry for entry in candidates if entry.datasource_uid in common_owners)
                return QueryTarget.from_metric(owner_entry)
    return default_target


def _resolve_native_query_target(
    catalog: list[MetricEntry],
    datasource_type: str,
    query_language: str,
    metric_names: set[str],
) -> QueryTarget:
    """Route native datasource queries to a datasource that owns the metric."""
    candidates = [
        entry
        for entry in catalog
        if entry.name in metric_names
        and _datasource_type_matches(entry.datasource_type, datasource_type)
        and (not query_language or (entry.query_language or "").lower() == query_language)
    ]
    for entry in candidates:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if candidates:
        return QueryTarget.from_metric(candidates[0])
    return _resolve_query_target(
        catalog,
        datasource_type,
        query_language,
    )


def _resolve_rate_interval(intent: Intent) -> str:
    """Choose an appropriate rate() interval based on the timerange."""
    tr = intent.timerange.lower()
    if "5m" in tr or "10m" in tr or "15m" in tr:
        return "1m"
    if "30m" in tr:
        return "2m"
    return "5m"


# ── SignalFlow filter resolvers ──────────────────────────────────────────────


def _resolve_sfx_service_filter(intent: Intent, catalog: list[MetricEntry]) -> str:
    """Build a SignalFlow filter() expression for the target service."""
    result = _find_best_label(intent, catalog)
    if result:
        label_name, val = result
        return f"filter('{label_name}', '{val}')"
    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f"filter('service', '*{target}*')"


def _resolve_sfx_container_filter(intent: Intent, catalog: list[MetricEntry]) -> str:
    """Build a SignalFlow filter() expression for container-level metrics."""
    result = _find_best_label(intent, catalog, restrict_to={"container", "pod"})
    if result:
        label_name, val = result
        return f"filter('{label_name}', '{val}')"
    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f"filter('container', '*{target}*')"


def _promql_template_to_signalflow(
    expr_template: str,
    service_filter: str,
    container_filter: str,
    legend: str,
) -> str:
    """Convert a PromQL archetype template expression directly to SignalFlow.

    Handles the archetype patterns deterministically:
    - histogram_quantile(X, sum(rate(metric_bucket{filter}[interval])) by (le))
      → data('metric', filter=...).percentile(pct=X*100)
    - sum(rate(metric{filter}[interval])) by (dim)
      → data('metric', filter=..., rollup='rate').sum(by=['dim'])
    - rate(metric{filter}[interval])
      → data('metric', filter=..., rollup='rate')
    - increase(metric{filter}[interval])
      → data('metric', filter=..., rollup='delta')
    - metric{filter}
      → data('metric', filter=...)
    - ratio: expr / expr
      → (A / B)
    """
    expr = expr_template.strip()

    # Helper: extract filter string from {service_filter} or {container_filter}
    def _filter_for(content: str) -> str:
        """Map placeholder content to the resolved SignalFlow filter."""
        content = content.strip()
        if not content:
            return ""
        if "container_filter" in expr_template and content == container_filter.replace("filter(", "").rstrip(")"):
            return container_filter
        return service_filter

    def _build_sfx_filter(label_block: str) -> str:
        """Parse a PromQL label block and return a SignalFlow filter."""
        # The label block has already had {service_filter} etc. substituted
        # with the SignalFlow filter() strings. We just need to join them.
        parts = []
        # Split on comma, but respect nested parens
        depth = 0
        current = ""
        for ch in label_block:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
                current = current.strip()
                if current:
                    parts.append(current)
                current = ""
            else:
                current += ch
        current = current.strip()
        if current:
            parts.append(current)

        filters = []
        for p in parts:
            p = p.strip()
            if p.startswith("filter("):
                filters.append(p)
            elif "=~" in p:
                # status=~"5.." → filter('status', '*')
                k, _ = p.split("=~", 1)
                filters.append(f"filter('{k.strip()}', '*')")
            elif "=" in p:
                k, v = p.split("=", 1)
                v = v.strip().strip('"')
                filters.append(f"filter('{k.strip()}', '{v}')")
        return " and ".join(filters) if filters else ""

    # ── ratio: expr / expr ──
    # Split on top-level /
    slash_pos = _find_top_level_slash(expr)
    if slash_pos is not None:
        left = _promql_template_to_signalflow(expr[:slash_pos].strip(), service_filter, container_filter, "_num")
        right = _promql_template_to_signalflow(expr[slash_pos + 1 :].strip(), service_filter, container_filter, "_den")
        # Strip .publish() from sub-expressions
        left = re.sub(r"\.publish\([^)]*\)$", "", left)
        right = re.sub(r"\.publish\([^)]*\)$", "", right)
        return f"({left} / {right}).publish(label='{legend}')"

    # ── histogram_quantile ──
    hq = re.match(
        r"histogram_quantile\(([\d.]+),\s*sum\(rate\(([\w.:]+?)_bucket\{(.*?)\}\[.*?\]\)\)\s*by\s*\(le(?:,\s*(\w+))?\)\)",
        expr,
    )
    if hq:
        pct = int(float(hq.group(1)) * 100)
        metric = hq.group(2)
        filt = _build_sfx_filter(hq.group(3))
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        by_dim = hq.group(4)
        if by_dim and by_dim != "le":
            return f"{base}.percentile(pct={pct}, by=['{by_dim}']).publish(label='{legend}')"
        return f"{base}.percentile(pct={pct}).publish(label='{legend}')"

    # ── topk ──
    topk = re.match(r"topk\((\d+),\s*(.+)\)$", expr, re.DOTALL)
    if topk:
        k = topk.group(1)
        inner = _promql_template_to_signalflow(topk.group(2), service_filter, container_filter, legend)
        inner = re.sub(r"\.publish\([^)]*\)$", "", inner)
        return f"{inner}.top(count={k}).publish(label='{legend}')"

    # ── agg(rate/increase(metric{labels}[interval])) by (dims) ──
    agg = re.match(
        r"(sum|avg|count|min|max)\((rate|increase)\(([\w.:]+)\{(.*?)\}\[.*?\]\)\)(?:\s*by\s*\(([^)]+)\))?",
        expr,
    )
    if agg:
        agg_fn = agg.group(1)
        func = agg.group(2)
        metric = agg.group(3)
        filt = _build_sfx_filter(agg.group(4))
        by_dims = agg.group(5)
        rollup = "rate" if func == "rate" else "delta"
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += f", rollup='{rollup}')"
        if by_dims:
            dims = [d.strip() for d in by_dims.split(",") if d.strip() != "le"]
            if dims:
                base += f".{agg_fn}(by={dims})"
            else:
                base += f".{agg_fn}()"
        else:
            base += f".{agg_fn}()"
        return f"{base}.publish(label='{legend}')"

    # ── bare rate/increase ──
    rate = re.match(r"(rate|increase)\(([\w.:]+)\{(.*?)\}\[.*?\]\)", expr)
    if rate:
        func = rate.group(1)
        metric = rate.group(2)
        filt = _build_sfx_filter(rate.group(3))
        rollup = "rate" if func == "rate" else "delta"
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += f", rollup='{rollup}')"
        return f"{base}.publish(label='{legend}')"

    # ── simple metric{labels} ──
    simple = re.match(r"([\w.:]+)\{(.*?)\}$", expr)
    if simple:
        metric = simple.group(1)
        filt = _build_sfx_filter(simple.group(2))
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        return f"{base}.publish(label='{legend}')"

    # ── bare metric name ──
    bare = re.match(r"^([\w.:]+)$", expr)
    if bare:
        return f"data('{bare.group(1)}').publish(label='{legend}')"

    # Fallback
    logger.warning("signalflow_compile_fallback", expr=expr[:100])
    return f"data('{expr}').publish(label='{legend}')"


def _find_top_level_slash(expr: str) -> int | None:
    """Find position of top-level '/' operator (not inside parens)."""
    depth = 0
    for i, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "/" and depth == 0 and i > 0:
            return i
    return None


_HISTOGRAM_SUFFIXES = ("_bucket", "_count", "_sum", "_total", "_created", "_info")


_METRIC_TOKEN_CHARS = r"A-Za-z0-9_:."


def _metric_name_pattern(metric_name: str) -> re.Pattern[str]:
    """Match a metric name as a complete metric token, not a substring."""
    return re.compile(rf"(?<![{_METRIC_TOKEN_CHARS}]){re.escape(metric_name)}(?![{_METRIC_TOKEN_CHARS}])")


def _strip_known_metric_suffix(metric_name: str) -> tuple[str, str]:
    """Return (base, suffix) if *metric_name* ends in a known metric suffix."""
    for suffix in sorted(_HISTOGRAM_SUFFIXES, key=len, reverse=True):
        if metric_name.endswith(suffix):
            return metric_name[: -len(suffix)], suffix
    return metric_name, ""


def _suffix_aware_replace(expr: str, old_metric: str, new_metric: str) -> str:
    """Replace *old_metric* with *new_metric* in *expr*, handling suffixes.

    The replacement is token-aware and suffix-aware:

    * ``old_metric_bucket`` becomes ``new_metric_bucket``;
    * if ``new_metric`` already has that same suffix, the suffix is not doubled;
    * if ``new_metric`` has a different known suffix, that suffix is stripped
      before appending the suffix from the expression; and
    * bare replacements are bounded to metric-token characters so similarly
      named metrics and already-replaced text are not rewritten accidentally.
    """
    if not old_metric or old_metric == new_metric:
        return expr

    protected: dict[str, str] = {}

    def protect(value: str) -> str:
        token = f"__TACIT_METRIC_TOKEN_{len(protected)}__"
        protected[token] = value
        return token

    new_base, new_suffix = _strip_known_metric_suffix(new_metric)

    # Replace suffixed variants first and protect replacements so the later bare
    # pass cannot rewrite inside a new metric that happens to contain old_metric.
    for suffix in sorted(_HISTOGRAM_SUFFIXES, key=len, reverse=True):
        old_suffixed = old_metric + suffix
        if new_metric.endswith(suffix):
            new_suffixed = new_metric
        elif new_suffix:
            new_suffixed = new_base + suffix
        else:
            new_suffixed = new_metric + suffix

        def replace_suffixed(_match: re.Match[str], replacement: str = new_suffixed) -> str:
            return protect(replacement)

        expr = _metric_name_pattern(old_suffixed).sub(
            replace_suffixed,
            expr,
        )

    expr = _metric_name_pattern(old_metric).sub(
        lambda _m: protect(new_metric),
        expr,
    )

    for token, value in protected.items():
        expr = expr.replace(token, value)
    return expr


def _apply_metric_substitutions(
    archetype: InvestigationArchetype,
    substitutions: dict[str, str],
) -> InvestigationArchetype:
    """Return a copy of the archetype with metric names substituted in queries.

    Used when signal resolution finds that the default metric names in the
    archetype templates don't exist in the environment, but equivalent
    metrics do (e.g. auth_requests_total → sso_auth_requests_total).

    Suffix-aware: if the template references ``base_metric_bucket`` and the
    substitution maps ``base_metric`` → ``new_metric``, the result is
    ``new_metric_bucket`` (not ``new_metric_bucket`` from a naive replace
    that could also cause ``new_metric_bucket_bucket`` when the resolved
    metric is already suffixed).
    """
    if not substitutions:
        return archetype

    new_panels = []
    for panel in archetype.panels:
        new_queries = []
        for qt in panel.queries:
            expr = qt.expr
            for old_metric, new_metric in substitutions.items():
                expr = _suffix_aware_replace(expr, old_metric, new_metric)
            new_queries.append(
                QueryTemplate(
                    expr=expr,
                    legend_format=qt.legend_format,
                    query_language=qt.query_language,
                    datasource_type=qt.datasource_type,
                    cloudwatch_namespace=qt.cloudwatch_namespace,
                    cloudwatch_stat=qt.cloudwatch_stat,
                    cloudwatch_dimensions=qt.cloudwatch_dimensions,
                    cloudwatch_region=qt.cloudwatch_region,
                )
            )
        new_panels.append(
            PanelTemplate(
                title=panel.title,
                description=panel.description,
                panel_type=panel.panel_type,
                row=panel.row,
                queries=new_queries,
                unit=panel.unit,
            )
        )

    return InvestigationArchetype(
        id=archetype.id,
        name=archetype.name,
        description=archetype.description,
        problem_types=archetype.problem_types,
        required_metrics=archetype.required_metrics,
        required_signals=archetype.required_signals,
        signal_bindings=archetype.signal_bindings,
        panels=new_panels,
        tags=archetype.tags,
        default_timerange=archetype.default_timerange,
    )


def _legacy_metric_signal_details(
    store,
    default_metric: str,
    catalog: list[MetricEntry],
    target_language: str,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> tuple[str, KnowledgeRevisionRef | None]:
    """Infer the taxonomy signal represented by a legacy required metric."""
    if not catalog:
        return "", None
    exemplar = catalog[0]
    pseudo = MetricEntry(
        name=default_metric,
        datasource_uid=exemplar.datasource_uid,
        datasource_name=exemplar.datasource_name,
        datasource_type=exemplar.datasource_type,
        query_language=target_language or exemplar.query_language,
    )
    candidates = [
        (match.signal_type, match.confidence, match.knowledge_revision_ref)
        for match in store.resolve_metric_signal_details(
            [pseudo],
            context_datasource_type=exemplar.datasource_type,
            target_query_language=target_language,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            **_signal_resolution_work_kwargs(store, resolution_work_budget),
        )
    ]
    candidates.sort(key=lambda item: item[1], reverse=True)
    return (candidates[0][0], candidates[0][2]) if candidates else ("", None)


def _legacy_metric_signal(
    store,
    default_metric: str,
    catalog: list[MetricEntry],
    target_language: str,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> str:
    """Infer the taxonomy signal represented by a legacy required metric."""
    signal_type, _ = _legacy_metric_signal_details(
        store,
        default_metric,
        catalog,
        target_language,
        tenant_id,
        knowledge_scope,
        resolution_work_budget,
    )
    return signal_type


def _substitution_shape_compatible(
    archetype: InvestigationArchetype,
    default_metric: str,
    candidate: MetricEntry,
) -> bool:
    """Reject semantic substitutions that would change the required query shape."""
    expressions = [query.expr for panel in archetype.panels for query in panel.queries if default_metric in query.expr]
    if not expressions:
        return False
    name = candidate.name
    metric_type = (candidate.metric_type or "").lower()
    if any(f"{default_metric}_bucket" in expr for expr in expressions):
        return name.endswith("_bucket") or metric_type == "histogram"
    rate_pattern = re.compile(rf"\b(?:rate|irate|increase)\([^)]*\b{re.escape(default_metric)}\b")
    uses_rate = any(rate_pattern.search(expr) for expr in expressions)
    counter_shaped = metric_type in {"counter", "histogram", "summary"} or name.endswith(
        ("_total", "_count", "_sum", "_bucket")
    )
    return not uses_rate or counter_shaped


def _unambiguous_legacy_candidate(
    resolved: list[tuple[MetricEntry, float]],
    archetype: InvestigationArchetype,
    default_metric: str,
) -> tuple[MetricEntry, float] | None:
    """Return a unique best compatible metric, or abstain on an unresolved tie.

    Raw datasets often encode service identity in the metric name instead of a
    label.  Picking the first equally scored service would make a valid query,
    but not a justified one.
    """
    compatible = [
        (candidate, confidence)
        for candidate, confidence in resolved
        if _substitution_shape_compatible(archetype, default_metric, candidate)
    ]
    if not compatible:
        return None
    best_confidence = compatible[0][1]
    best = [item for item in compatible if item[1] == best_confidence]
    best_names = {candidate.name for candidate, _ in best}
    return best[0] if len(best_names) == 1 else None


def _resolve_legacy_required_metrics(
    archetype: InvestigationArchetype,
    store,
    catalog: list[MetricEntry],
    intent: Intent,
    target_language: str,
    tenant_id: str = "default",
    governance_refs_by_default_metric: dict[str, set[KnowledgeRevisionRef]] | None = None,
    knowledge_scope: Any | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> dict[str, str]:
    """Resolve legacy required_metrics through the semantic taxonomy."""
    target_datasource_type = _datasource_type_for_language(target_language)
    target_catalog = [
        entry
        for entry in catalog
        if (not target_language or (entry.query_language or "").lower() == target_language.lower())
        and _datasource_type_matches(entry.datasource_type, target_datasource_type)
    ]
    resolution_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=True)
    catalog_names = {entry.name for entry in resolution_catalog}
    substitutions: dict[str, str] = {}
    for default_metric in archetype.required_metrics:
        if default_metric in catalog_names:
            continue
        signal_type, inferred_by = _legacy_metric_signal_details(
            store,
            default_metric,
            target_catalog,
            target_language,
            tenant_id,
            knowledge_scope,
            resolution_work_budget,
        )
        if not signal_type:
            continue
        resolved_details = store.resolve_signal_details(
            signal_type,
            resolution_catalog,
            context_service=intent.services[0] if intent.services else "",
            context_datasource_type=target_datasource_type,
            context_archetype=archetype.id,
            context_environment=intent.environments[0] if intent.environments else "",
            target_query_language=target_language,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            **_signal_resolution_work_kwargs(store, resolution_work_budget),
        )
        resolved = [(match.entry, match.confidence) for match in resolved_details]
        selected = _unambiguous_legacy_candidate(resolved, archetype, default_metric)
        if selected is None:
            if resolved:
                logger.info(
                    "legacy_metric_signal_ambiguous",
                    archetype=archetype.id,
                    default_metric=default_metric,
                    signal=signal_type,
                    candidate_count=len(resolved),
                )
            continue
        candidate, confidence = selected
        substitutions[default_metric] = candidate.name
        if governance_refs_by_default_metric is not None:
            refs = governance_refs_by_default_metric.setdefault(default_metric, set())
            if inferred_by:
                refs.add(inferred_by)
            selected_match = next(
                (match for match in resolved_details if match.entry == candidate and match.confidence == confidence),
                None,
            )
            if selected_match is not None and selected_match.knowledge_revision_ref is not None:
                refs.add(selected_match.knowledge_revision_ref)
        logger.info(
            "legacy_metric_signal_resolved",
            archetype=archetype.id,
            default_metric=default_metric,
            signal=signal_type,
            resolved_to=candidate.name,
            confidence=confidence,
        )
    return substitutions


def _resolve_archetype_signals(
    archetype: InvestigationArchetype,
    catalog: list[MetricEntry],
    intent: Intent,
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    governance_refs_by_template_query: dict[tuple[int, int], set[KnowledgeRevisionRef]] | None = None,
    knowledge_scope: Any | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> InvestigationArchetype:
    """Resolve signal bindings and substitute metrics if needed.

    If the archetype has signal_bindings and any default metrics are missing
    from the catalog, the signal store is consulted to find alternatives.
    ``target_language`` keeps substitutions within the backend being compiled
    for (e.g. don't pull a SignalFx metric into a PromQL dashboard).
    Returns the (possibly modified) archetype.
    """
    if not archetype.signal_bindings and not archetype.required_metrics:
        return archetype

    try:
        store = _resolve_archetype_signal_store(signal_store)
        if store is None:
            return archetype
        active_work_budget = resolution_work_budget or _new_signal_resolution_work_budget(
            store,
            max_calls=(len(archetype.signal_bindings) + (2 * len(archetype.required_metrics)) + 1),
        )
        refs_by_default_metric: dict[str, set[KnowledgeRevisionRef]] = {}
        substitutions = store.resolve_signals_for_archetype(
            signal_bindings=archetype.signal_bindings,
            catalog=catalog,
            context_service=intent.services[0] if intent.services else "",
            context_datasource_type=_datasource_type_for_language(target_language),
            context_archetype=archetype.id,
            context_environment=intent.environments[0] if intent.environments else "",
            target_query_language=target_language,
            tenant_id=tenant_id,
            knowledge_scope=knowledge_scope,
            governance_revision_refs_by_default_metric=refs_by_default_metric,
            **_signal_resolution_work_kwargs(store, active_work_budget),
        )
        legacy_substitutions = _resolve_legacy_required_metrics(
            archetype,
            store,
            catalog,
            intent,
            target_language,
            tenant_id,
            refs_by_default_metric,
            knowledge_scope,
            active_work_budget,
        )
        for default_metric, resolved_metric in legacy_substitutions.items():
            substitutions.setdefault(default_metric, resolved_metric)
        if substitutions:
            logger.info(
                "archetype_signals_resolved",
                archetype=archetype.id,
                substitutions=substitutions,
            )
            if governance_refs_by_template_query is not None:
                for panel_index, panel in enumerate(archetype.panels):
                    for query_index, query in enumerate(panel.queries):
                        refs = {
                            revision_ref
                            for default_metric, resolved_metric in substitutions.items()
                            if _query_changes_under_metric_substitution(
                                query.expr,
                                default_metric,
                                resolved_metric,
                            )
                            for revision_ref in refs_by_default_metric.get(default_metric, set())
                        }
                        if refs:
                            governance_refs_by_template_query[(panel_index, query_index)] = refs
            resolved_archetype = _apply_metric_substitutions(archetype, substitutions)
            return resolved_archetype
    except SignalResolutionWorkLimitError:
        raise
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except Exception as exc:
        logger.warning(
            "signal_resolution_failed",
            reason_code=_ARCHETYPE_SIGNAL_RESOLUTION_FAILED,
            **safe_failure_diagnostics(
                exc,
                reason_code=_ARCHETYPE_SIGNAL_RESOLUTION_FAILED,
                counters={
                    "signal_count": len(set(archetype.required_signals) | set(archetype.signal_bindings)),
                    "required_metric_count": len(archetype.required_metrics),
                },
            ),
        )

    return archetype


def compile_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_query_uses: list[KnowledgeQueryUse] | None = None,
    knowledge_scope: Any | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
    work_limits: ArchetypeCoverageWorkLimits | None = None,
) -> DashboardSpec:
    """Compile an archetype template into a concrete DashboardSpec.

    This is fully deterministic — no LLM call needed.
    Resolves {service_filter}, {container_filter}, {rate_interval}
    from the intent and catalog.

    If the archetype has signal_bindings, metric names are resolved via the
    signal store before template compilation.

    target_language: 'promql' (default) or 'signalflow'
    """
    limits = work_limits or ArchetypeCoverageWorkLimits()
    admit_intent_resolution_inputs(intent, work_limits=limits)
    admit_archetype_resolution_inputs(
        [archetype],
        catalog,
        services=intent.services,
        work_limits=limits,
    )

    # Resolve signals → actual metrics before compiling templates
    governance_refs_by_template_query: dict[tuple[int, int], set[KnowledgeRevisionRef]] = {}
    archetype = _resolve_archetype_signals(
        archetype,
        catalog,
        intent,
        target_language,
        signal_store,
        tenant_id,
        governance_refs_by_template_query,
        knowledge_scope,
        resolution_work_budget,
    )

    rate_interval = _resolve_rate_interval(intent)

    if target_language == "signalflow":
        service_filter = _resolve_sfx_service_filter(intent, catalog)
        container_filter = _resolve_sfx_container_filter(intent, catalog)
        default_target = _resolve_query_target(
            catalog,
            "signalfx",
            "signalflow",
            fallback_uid="signalfx-direct",
        )
    else:
        service_filter = _resolve_service_filter(intent, catalog)
        container_filter = _resolve_container_filter(intent, catalog)
        default_target = _resolve_query_target(catalog, "prometheus", "promql")

    params = {
        "service_filter": service_filter,
        "container_filter": container_filter,
        "rate_interval": rate_interval,
    }

    panels: list[PanelSpec] = []
    skipped = 0

    for panel_index, pt in enumerate(archetype.panels):
        panel_queries: list[PanelQuery] = []
        query_uses: list[tuple[PanelQuery, set[KnowledgeRevisionRef]]] = []
        for query_index, qt in enumerate(pt.queries):
            # Determine whether this query is PromQL. An explicit non-PromQL
            # query_language (signalflow/logql/cloudwatch/…) — or a SignalFx
            # datasource tag — marks it as a native query to honor verbatim.
            qt_language = (qt.query_language or "promql").lower()
            if qt_language in ("", "promql") and "signalfx" in (qt.datasource_type or "").lower():
                qt_language = "signalflow"
            is_promql_query = qt_language in ("", "promql")

            if is_promql_query:
                # PromQL templates use {service_filter} etc. and need str.format.
                try:
                    expr = qt.expr.format(**params)
                except KeyError as e:
                    logger.warning("archetype_placeholder_missing", panel=pt.title, key=str(e))
                    continue
                if target_language == "signalflow":
                    # Compile the resolved PromQL template directly to SignalFlow.
                    legend = qt.legend_format or pt.title
                    expr = _promql_template_to_signalflow(expr, service_filter, container_filter, legend)
                    query_target = default_target
                else:
                    query_target = _resolve_promql_query_target(catalog, expr, default_target, intent)
            else:
                # Non-PromQL queries are honored verbatim — no PromQL
                # format/escaping or PromQL→SignalFlow conversion.
                expr = qt.expr
                if not qt.datasource_type or qt.datasource_type == "prometheus":
                    query_datasource_type = _datasource_type_for_language(qt_language, default_target.datasource_type)
                else:
                    query_datasource_type = qt.datasource_type
                native_metric_names = {expr}
                if qt.cloudwatch_namespace and not expr.startswith(f"{qt.cloudwatch_namespace}/"):
                    native_metric_names.add(f"{qt.cloudwatch_namespace}/{expr}")
                query_target = _resolve_native_query_target(
                    catalog,
                    query_datasource_type,
                    qt_language,
                    native_metric_names,
                )

            compiled_query = PanelQuery(
                expr=expr,
                legend_format=qt.legend_format,
                datasource_uid=query_target.datasource_uid,
                datasource_type=query_target.datasource_type,
                query_language=query_target.query_language,
                cloudwatch_namespace=qt.cloudwatch_namespace,
                cloudwatch_stat=qt.cloudwatch_stat,
                cloudwatch_dimensions=qt.cloudwatch_dimensions,
                cloudwatch_region=qt.cloudwatch_region,
            )
            panel_queries.append(compiled_query)
            query_uses.append(
                (
                    compiled_query,
                    governance_refs_by_template_query.get((panel_index, query_index), set()),
                )
            )

        if not panel_queries:
            skipped += 1
            continue

        compiled_panel = PanelSpec(
            title=pt.title,
            description=pt.description,
            panel_type=pt.panel_type,
            row=pt.row,
            source_archetype=archetype.id,
            queries=panel_queries,
            unit=pt.unit,
        )
        panels.append(compiled_panel)
        if knowledge_query_uses is not None:
            for query, refs in query_uses:
                knowledge_query_uses.extend(
                    KnowledgeQueryUse.from_query(revision_ref, compiled_panel, query) for revision_ref in sorted(refs)
                )

    # Build title from archetype name + service
    service_name = intent.services[0] if intent.services else "Service"
    title = f"{service_name.title()} — {archetype.name}"

    spec = DashboardSpec(
        title=title,
        tags=archetype.tags + ["tacit", "archetype"],
        timerange=intent.timerange or archetype.default_timerange,
        panels=panels,
    )
    logger.info(
        "archetype_compiled",
        archetype=archetype.id,
        panels=len(panels),
        skipped=skipped,
        service_filter=service_filter,
        rate_interval=rate_interval,
        language=target_language,
    )

    return spec


def _archetype_query_languages(
    archetype: InvestigationArchetype,
    target_language: str,
) -> set[str]:
    """Return native query languages used by an archetype for this backend."""
    datasource_languages = {
        "cloudwatch": "cloudwatch",
        "loki": "logql",
        "graphite": "graphite",
        "influxdb": "influxql",
        "elasticsearch": "lucene",
        "opensearch": "lucene",
        "signalfx": "signalflow",
        "grafana-signalfx-datasource": "signalflow",
    }
    fallback = target_language.lower()
    languages: set[str] = set()
    for panel in archetype.panels:
        for query in panel.queries:
            datasource_type = (query.datasource_type or "").lower()
            if datasource_type in datasource_languages:
                languages.add(datasource_languages[datasource_type])
                continue
            query_language = (query.query_language or "").lower()
            if datasource_type in _PROMETHEUS_DATASOURCE_TYPES and query_language in {"", "promql"}:
                languages.add(fallback or "promql")
            elif query_language:
                languages.add(query_language)
    return languages or {fallback or "promql"}


def _archetype_live_coverage(
    archetype: InvestigationArchetype,
    catalog: list[MetricEntry],
    target_language: str = "promql",
    services: list[str] | None = None,
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    knowledge_stage_uses: list[KnowledgeStageUse] | None = None,
    excluded_knowledge_refs: set[KnowledgeRevisionRef] | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> float | None:
    """Fraction of an archetype's declared evidence covered by the live catalog.

    Evidence includes semantic signals/bindings and legacy ``required_metrics``.
    Returns ``None`` when no evidence is declared or the catalog contains only
    datasource targets without metric names, because coverage is then unknown.
    """
    signals = set(archetype.required_signals) | set(archetype.signal_bindings.keys())
    required_metrics = set(archetype.required_metrics)
    if not signals and not required_metrics:
        return None

    named_catalog = [entry for entry in catalog if entry.name]
    if not named_catalog:
        return None
    scoped_catalog = catalog_for_services(named_catalog, services or [], include_unscoped=True)
    query_languages = _archetype_query_languages(archetype, target_language)
    coverage_catalog = [entry for entry in scoped_catalog if (entry.query_language or "").lower() in query_languages]
    catalog_names = {entry.name for entry in coverage_catalog}
    if not catalog_names:
        return 0.0

    resolution_failure_count = 0
    first_resolution_failure: dict[str, str | int] | None = None
    try:
        store = _resolve_archetype_signal_store(signal_store)
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except Exception as exc:
        store = None
        resolution_failure_count = 1
        first_resolution_failure = safe_failure_diagnostics(
            exc,
            reason_code=_ARCHETYPE_COVERAGE_SIGNAL_RESOLUTION_FAILED,
        )

    resolved = 0
    for sig in signals:
        default_metric = archetype.signal_bindings.get(sig, "")
        if default_metric and default_metric in catalog_names:
            resolved += 1
            continue
        if store is not None:
            try:
                resolve_details = getattr(store, "resolve_signal_details", None)
                matches = (
                    resolve_details(
                        sig,
                        coverage_catalog,
                        context_service=services[0] if services else "",
                        context_datasource_type=_datasource_type_for_language(target_language),
                        context_archetype=archetype.id,
                        tenant_id=tenant_id,
                        knowledge_scope=knowledge_scope,
                        **({"excluded_knowledge_refs": excluded_knowledge_refs} if excluded_knowledge_refs else {}),
                        **_signal_resolution_work_kwargs(store, resolution_work_budget),
                    )
                    if callable(resolve_details)
                    else []
                )
                if matches:
                    resolved += 1
                    revision_ref = matches[0].knowledge_revision_ref
                    if knowledge_stage_uses is not None and revision_ref is not None:
                        knowledge_stage_uses.append(
                            KnowledgeStageUse(
                                revision_ref=revision_ref,
                                stage=KnowledgeUsageStage.ARCHETYPE_SELECTION,
                                effect=KnowledgeUsageEffect.ARCHETYPE_SELECTED_BY_LIVE_COVERAGE,
                                target_ref=f"archetype:{archetype.id}",
                            )
                        )
                elif not callable(resolve_details) and store.resolve_signal(
                    sig,
                    coverage_catalog,
                    context_service=services[0] if services else "",
                    context_datasource_type=_datasource_type_for_language(target_language),
                    context_archetype=archetype.id,
                    tenant_id=tenant_id,
                    knowledge_scope=knowledge_scope,
                    **_signal_resolution_work_kwargs(store, resolution_work_budget),
                ):
                    resolved += 1
            except SignalResolutionWorkLimitError:
                raise
            except AUTHORITY_BOUNDARY_ERRORS:
                raise
            except Exception as exc:
                resolution_failure_count += 1
                if first_resolution_failure is None:
                    first_resolution_failure = safe_failure_diagnostics(
                        exc,
                        reason_code=_ARCHETYPE_COVERAGE_SIGNAL_RESOLUTION_FAILED,
                    )
    for required_metric in required_metrics:
        if any(
            name == required_metric
            or any(
                name.endswith(suffix) and name[: -len(suffix)] == required_metric
                for suffix in _PROMETHEUS_HISTOGRAM_SUFFIXES
            )
            for name in catalog_names
        ):
            resolved += 1

    if first_resolution_failure is not None:
        logger.warning(
            "archetype_coverage_signal_resolution_failed",
            reason_code=_ARCHETYPE_COVERAGE_SIGNAL_RESOLUTION_FAILED,
            resolution_failure_count=min(resolution_failure_count, 1_000_000),
            **first_resolution_failure,
        )

    return resolved / (len(signals) + len(required_metrics))


def rank_archetypes_by_coverage(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
    services: list[str] | None = None,
    max_archetypes: int | None = None,
    min_secondary_coverage: float = 0.0,
    learned_archetype_min_coverage: float = 0.75,
    learned_archetype_boost: float = 0.15,
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    knowledge_stage_uses: list[KnowledgeStageUse] | None = None,
    work_limits: ArchetypeCoverageWorkLimits | None = None,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
) -> list[tuple[InvestigationArchetype, float]]:
    """Re-rank archetypes by classifier_confidence × live signal coverage.

    This prefers a strongly-matching (well-covered) archetype over numerous
    generic templates whose signals are absent from the environment, then caps
    the list so blending cannot explode into many loosely-matched archetypes.
    The primary archetype (rank 0 after re-sort) is always kept; secondaries
    below ``min_secondary_coverage`` are dropped. Coverage and exact
    knowledge-attribution work are bounded before resolver or counterfactual
    calls. Exceeding a bound fails closed instead of returning an unaudited
    knowledge-influenced selection.
    """
    if not ranked_archetypes:
        return ranked_archetypes

    limits = work_limits or ArchetypeCoverageWorkLimits()
    candidate_count = len(ranked_archetypes)
    if candidate_count > limits.max_candidates:
        _raise_archetype_coverage_work_limit(
            "candidate_count",
            candidate_count,
            limits.max_candidates,
        )
    catalog_count = len(catalog)
    service_count = len(services or ())
    admission = admit_archetype_resolution_inputs(
        [archetype for archetype, _ in ranked_archetypes],
        catalog,
        services=services,
        work_limits=limits,
    )
    total_catalog_dimensions = admission["catalog_dimension_count"]

    signals_by_candidate: list[frozenset[str]] = []
    catalog_comparisons_by_candidate: list[int] = []
    for archetype, _ in ranked_archetypes:
        signals = frozenset(archetype.required_signals) | frozenset(archetype.signal_bindings.keys())
        required_metrics = frozenset(archetype.required_metrics)
        if len(signals) > limits.max_signal_requirements_per_archetype:
            _raise_archetype_coverage_work_limit(
                "signal_requirements_per_archetype",
                len(signals),
                limits.max_signal_requirements_per_archetype,
            )
        signals_by_candidate.append(signals)
        catalog_comparisons_by_candidate.append(
            catalog_count * (4 + (2 * service_count) + len(signals) + (4 * len(required_metrics)))
            + (2 * total_catalog_dimensions if service_count else 0)
        )

    base_resolver_call_upper_bound = sum(len(signals) for signals in signals_by_candidate)
    if base_resolver_call_upper_bound > limits.max_total_resolver_calls:
        _raise_archetype_coverage_work_limit(
            "total_resolver_calls",
            base_resolver_call_upper_bound,
            limits.max_total_resolver_calls,
        )
    base_catalog_comparison_upper_bound = sum(catalog_comparisons_by_candidate)
    if base_catalog_comparison_upper_bound > limits.max_total_catalog_comparisons:
        _raise_archetype_coverage_work_limit(
            "total_catalog_comparisons",
            base_catalog_comparison_upper_bound,
            limits.max_total_catalog_comparisons,
        )

    try:
        resolved_signal_store = _resolve_archetype_signal_store(signal_store)
        operation_signal_store = resolved_signal_store if resolved_signal_store is not None else signal_store
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except Exception as exc:
        resolved_signal_store = None
        operation_signal_store = SIGNAL_STORE_UNAVAILABLE
        logger.warning(
            "archetype_coverage_signal_resolution_failed",
            reason_code=_ARCHETYPE_COVERAGE_SIGNAL_RESOLUTION_FAILED,
            resolution_failure_count=1,
            **safe_failure_diagnostics(
                exc,
                reason_code=_ARCHETYPE_COVERAGE_SIGNAL_RESOLUTION_FAILED,
            ),
        )
    active_resolution_work_budget = resolution_work_budget or _new_signal_resolution_work_budget(
        resolved_signal_store,
        max_calls=limits.max_total_resolver_calls,
        max_mapping_catalog_comparisons=limits.max_total_catalog_comparisons,
        max_results=limits.max_total_resolution_results,
    )

    ScoredArchetype = tuple[
        int,
        InvestigationArchetype,
        float,
        float,
        float,
        tuple[KnowledgeStageUse, ...],
    ]

    def effective_score(arch: InvestigationArchetype, confidence: float, coverage: float | None) -> float:
        effective = confidence if coverage is None else confidence * coverage
        is_learned = bool({"learned", "auto-generated"} & set(arch.tags))
        if is_learned and coverage is not None and coverage >= learned_archetype_min_coverage:
            effective += learned_archetype_boost * coverage
        return effective

    def score_candidate(
        candidate_index: int,
        excluded_refs: set[KnowledgeRevisionRef] | None = None,
    ) -> ScoredArchetype:
        arch, confidence = ranked_archetypes[candidate_index]
        candidate_knowledge_uses: list[KnowledgeStageUse] = []
        coverage = _archetype_live_coverage(
            arch,
            catalog,
            target_language,
            services,
            operation_signal_store,
            tenant_id,
            knowledge_scope,
            candidate_knowledge_uses,
            excluded_refs,
            active_resolution_work_budget,
        )
        return (
            candidate_index,
            arch,
            confidence,
            coverage if coverage is not None else -1.0,
            effective_score(arch, confidence, coverage),
            tuple(candidate_knowledge_uses),
        )

    def sorted_scores(
        scores_by_index: dict[int, ScoredArchetype],
    ) -> list[ScoredArchetype]:
        return sorted(
            (scores_by_index[index] for index in range(candidate_count)),
            key=lambda item: item[4],
            reverse=True,
        )

    def select(
        scored_candidates: list[ScoredArchetype],
        *,
        emit_diagnostics: bool,
    ) -> list[ScoredArchetype]:
        selected: list[ScoredArchetype] = []
        for rank, candidate in enumerate(scored_candidates):
            _, arch, confidence, coverage, _, _ = candidate
            if rank > 0 and coverage >= 0.0 and coverage < min_secondary_coverage:
                if emit_diagnostics:
                    logger.info(
                        "archetype_dropped_low_coverage",
                        archetype=arch.id,
                        confidence=confidence,
                        coverage=round(coverage, 3),
                    )
                continue
            selected.append(candidate)
            if max_archetypes is not None and len(selected) >= max_archetypes:
                break
        return selected

    base_scores_by_index = {
        candidate_index: score_candidate(candidate_index) for candidate_index in range(candidate_count)
    }
    selected = select(sorted_scores(base_scores_by_index), emit_diagnostics=True)
    kept = [(arch, confidence) for _, arch, confidence, _, _, _ in selected]
    selected_knowledge_uses = [use for _, _, _, _, _, uses in selected for use in uses]
    if knowledge_stage_uses is not None and selected_knowledge_uses:
        selected_ids = [arch.id for _, arch, _, _, _, _ in selected]
        selected_positions = {archetype_id: index for index, archetype_id in enumerate(selected_ids)}
        revision_refs = sorted({use.revision_ref for use in selected_knowledge_uses})
        if len(revision_refs) > limits.max_unique_revisions:
            _raise_archetype_coverage_work_limit(
                "unique_knowledge_revisions",
                len(revision_refs),
                limits.max_unique_revisions,
            )

        affected_indexes_by_revision: dict[KnowledgeRevisionRef, set[int]] = {
            revision_ref: set() for revision_ref in revision_refs
        }
        # Excluding a revision can change only candidates whose base resolution
        # selected that revision. Unaffected base scores remain exact and reusable.
        for candidate_index, _, _, _, _, uses in base_scores_by_index.values():
            for revision_ref in {use.revision_ref for use in uses}:
                if revision_ref in affected_indexes_by_revision:
                    affected_indexes_by_revision[revision_ref].add(candidate_index)

        counterfactual_candidate_scores = sum(
            len(affected_indexes_by_revision[revision_ref]) for revision_ref in revision_refs
        )
        if counterfactual_candidate_scores > limits.max_counterfactual_candidate_scores:
            _raise_archetype_coverage_work_limit(
                "counterfactual_candidate_scores",
                counterfactual_candidate_scores,
                limits.max_counterfactual_candidate_scores,
            )

        counterfactual_resolver_call_upper_bound = sum(
            len(signals_by_candidate[candidate_index])
            for revision_ref in revision_refs
            for candidate_index in affected_indexes_by_revision[revision_ref]
        )
        total_resolver_call_upper_bound = base_resolver_call_upper_bound + counterfactual_resolver_call_upper_bound
        if total_resolver_call_upper_bound > limits.max_total_resolver_calls:
            _raise_archetype_coverage_work_limit(
                "total_resolver_calls",
                total_resolver_call_upper_bound,
                limits.max_total_resolver_calls,
            )
        counterfactual_catalog_comparison_upper_bound = sum(
            catalog_comparisons_by_candidate[candidate_index]
            for revision_ref in revision_refs
            for candidate_index in affected_indexes_by_revision[revision_ref]
        )
        total_catalog_comparison_upper_bound = (
            base_catalog_comparison_upper_bound + counterfactual_catalog_comparison_upper_bound
        )
        if total_catalog_comparison_upper_bound > limits.max_total_catalog_comparisons:
            _raise_archetype_coverage_work_limit(
                "total_catalog_comparisons",
                total_catalog_comparison_upper_bound,
                limits.max_total_catalog_comparisons,
            )

        causal_targets: dict[KnowledgeRevisionRef, set[str]] = {}
        for revision_ref in revision_refs:
            counterfactual_scores = dict(base_scores_by_index)
            for candidate_index in affected_indexes_by_revision[revision_ref]:
                counterfactual_scores[candidate_index] = score_candidate(
                    candidate_index,
                    {revision_ref},
                )
            counterfactual = select(
                sorted_scores(counterfactual_scores),
                emit_diagnostics=False,
            )
            counterfactual_positions = {arch.id: index for index, (_, arch, _, _, _, _) in enumerate(counterfactual)}
            causal_targets[revision_ref] = {
                archetype_id
                for archetype_id, selected_position in selected_positions.items()
                if counterfactual_positions.get(archetype_id) != selected_position
            }
        for use in selected_knowledge_uses:
            target_archetype = use.target_ref.removeprefix("archetype:")
            if target_archetype in causal_targets.get(use.revision_ref, set()) and use not in knowledge_stage_uses:
                knowledge_stage_uses.append(use)

    return kept


def _panel_signature(panel: PanelSpec) -> frozenset[tuple[str, str, str, str]] | str:
    """Identify equivalent panels without collapsing cross-datasource evidence."""
    queries = {
        (
            re.sub(r"\s+", "", query.expr.lower()),
            query.datasource_uid,
            query.datasource_type.lower(),
            query.query_language.lower(),
        )
        for query in panel.queries
        if query.expr
    }
    return frozenset(queries) if queries else panel.title.lower()


def blend_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    intent: Intent,
    catalog: list[MetricEntry],
    secondary_min_confidence: float = 0.4,
    target_language: str = "promql",
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_query_uses: list[KnowledgeQueryUse] | None = None,
    knowledge_scope: Any | None = None,
    max_archetypes: int | None = 3,
    min_secondary_coverage: float = 0.25,
    learned_archetype_min_coverage: float = 0.75,
    learned_archetype_boost: float = 0.15,
    max_dashboard_panels: int = 10,
) -> DashboardSpec:
    """Blend panels from multiple archetypes into a single dashboard.

    The primary (highest-confidence) archetype contributes all its panels.
    Secondary archetypes contribute panels whose titles don't duplicate the
    primary's, giving broader investigation coverage without redundancy.

    Parameters
    ----------
    ranked_archetypes : list[tuple[InvestigationArchetype, float]]
        (archetype, confidence) pairs, highest confidence first.
    intent : Intent
        The classified user intent.
    catalog : list[MetricEntry]
        Discovered metrics from datasources.
    secondary_min_confidence : float
        Minimum confidence for secondary archetypes to contribute panels.
    target_language : str
        'promql' (default) or 'signalflow'
    """
    if not ranked_archetypes:
        raise ValueError("blend_archetypes called with empty archetype list")

    # Coverage-rank and cap the archetype set BEFORE blending so a flood of
    # loosely-matched generic templates can't add dozens of irrelevant panels.
    ranked_archetypes = rank_archetypes_by_coverage(
        ranked_archetypes,
        catalog,
        target_language=target_language,
        services=intent.services,
        max_archetypes=max_archetypes,
        min_secondary_coverage=min_secondary_coverage,
        learned_archetype_min_coverage=learned_archetype_min_coverage,
        learned_archetype_boost=learned_archetype_boost,
        signal_store=signal_store,
        tenant_id=tenant_id,
        knowledge_scope=knowledge_scope,
    )
    max_panels = max_dashboard_panels

    primary_arch, primary_conf = ranked_archetypes[0]
    compiled_query_uses: list[KnowledgeQueryUse] = []
    primary_spec = compile_archetype(
        primary_arch,
        intent,
        catalog,
        target_language=target_language,
        signal_store=signal_store,
        tenant_id=tenant_id,
        knowledge_query_uses=compiled_query_uses,
        knowledge_scope=knowledge_scope,
    )

    # De-dup on the panel's *query signature* (the set of normalized query
    # expressions), not just its title — so the same panel arriving from two
    # archetypes under different titles collapses, while distinct views of the
    # same metric (e.g. p99 vs avg latency) are preserved.
    seen_signatures: set[frozenset[tuple[str, str, str, str]] | str] = {
        _panel_signature(p) for p in primary_spec.panels
    }
    blended_panels = list(primary_spec.panels)
    blended_tags = list(primary_spec.tags)

    for arch, conf in ranked_archetypes[1:]:
        if conf < secondary_min_confidence:
            continue
        if len(blended_panels) >= max_panels:
            break

        secondary_spec = compile_archetype(
            arch,
            intent,
            catalog,
            target_language=target_language,
            signal_store=signal_store,
            tenant_id=tenant_id,
            knowledge_query_uses=compiled_query_uses,
            knowledge_scope=knowledge_scope,
        )
        added = 0
        for panel in secondary_spec.panels:
            if len(blended_panels) >= max_panels:
                break
            sig = _panel_signature(panel)
            if sig not in seen_signatures:
                # Tag panel with its source archetype for traceability
                panel_with_row = panel.model_copy(update={"row": panel.row or arch.name})
                blended_panels.append(panel_with_row)
                seen_signatures.add(sig)
                added += 1

        if added > 0:
            blended_tags.extend(arch.tags)
            logger.info(
                "archetype_blended",
                secondary=arch.id,
                confidence=conf,
                panels_added=added,
            )

    # Build final title
    service_name = intent.services[0] if intent.services else "Service"
    arch_names = " + ".join(a.name for a, c in ranked_archetypes[:3] if c >= secondary_min_confidence)
    title = f"{service_name.title()} — {arch_names}"

    spec = DashboardSpec(
        title=title,
        tags=list(dict.fromkeys(blended_tags)),  # dedupe preserving order
        timerange=intent.timerange or primary_arch.default_timerange,
        panels=blended_panels[:max_panels],
    )
    if knowledge_query_uses is not None:
        surviving_query_ids = dashboard_query_identities(spec)
        knowledge_query_uses.extend(use for use in compiled_query_uses if use.query_identity() in surviving_query_ids)

    logger.info(
        "archetype_blend_complete",
        primary=primary_arch.id,
        primary_confidence=primary_conf,
        total_archetypes=len(ranked_archetypes),
        total_panels=len(spec.panels),
    )

    return spec
