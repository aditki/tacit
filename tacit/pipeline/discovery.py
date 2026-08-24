"""Discovery and live-signal confirmation helpers for the pipeline."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import structlog

from tacit.backends.base import DashboardBackend
from tacit.catalog import catalog_for_services
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS, safe_failure_diagnostics
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.models.schemas import Intent, MetricEntry
from tacit.signals.availability import resolve_signal_store
from tacit.signals.resolution import (
    ResolutionInputTextLimits,
    ResolutionInputWorkLimitError,
    SignalResolutionWorkBudget,
    SignalResolutionWorkLimitError,
    admit_resolution_input_text,
)

logger = structlog.get_logger()

MAX_COLLOQUIAL_EVIDENCE_ITEMS = 64
MAX_COLLOQUIAL_SIGNAL_RESOLUTIONS = 256
MAX_COLLOQUIAL_CATALOG_ENTRIES = 5_000
MAX_COLLOQUIAL_SERVICES = 64
MAX_COLLOQUIAL_ENVIRONMENTS = 64
MAX_COLLOQUIAL_KEYWORDS = 256
MAX_COLLOQUIAL_ARCHETYPES = 64
MAX_COLLOQUIAL_EVIDENCE_FIELDS = 8
MAX_COLLOQUIAL_DIMENSIONS_PER_ENTRY = 128
MAX_COLLOQUIAL_TOTAL_DIMENSIONS = 100_000


class ColloquialConfirmationWorkLimitError(RuntimeError):
    """Colloquial confirmation exceeded a stable request work dimension."""

    reason_code = "colloquial_confirmation_work_limit_exceeded"

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension = dimension
        self.observed = observed
        self.limit = limit
        super().__init__(f"{self.reason_code}: {dimension} exceeds {limit}")


class ConfirmedKeywords(list[str]):
    """Confirmed keywords with the exact governed mappings that confirmed them.

    This remains list-compatible for the existing discovery-stage boundary while
    retaining immutable attribution until archetype routing can prove an effect.
    """

    def __init__(
        self,
        values: Iterable[str] = (),
        *,
        revision_refs_by_keyword: dict[str, set[KnowledgeRevisionRef]] | None = None,
        added_keywords: Iterable[str] = (),
        degraded: bool = False,
        resolution_work_budget: SignalResolutionWorkBudget | None = None,
    ) -> None:
        super().__init__(values)
        self.revision_refs_by_keyword = {
            keyword: frozenset(refs) for keyword, refs in (revision_refs_by_keyword or {}).items() if refs
        }
        self.added_keywords = frozenset(added_keywords)
        self.degraded = degraded
        self.resolution_work_budget = resolution_work_budget


@dataclass
class DiscoveryResult:
    metric_catalog: list[MetricEntry]
    datasource_catalog: list[MetricEntry]
    datasource_types: list[str]

    @property
    def catalog_for_compile(self) -> list[MetricEntry]:
        return self.metric_catalog or self.datasource_catalog


def _raise_discovery_input_text_limit(exc: ResolutionInputWorkLimitError) -> None:
    logger.warning(
        ColloquialConfirmationWorkLimitError.reason_code,
        reason_code=ColloquialConfirmationWorkLimitError.reason_code,
        dimension=exc.dimension,
        observed=min(max(exc.observed, 0), 100_000_000),
        limit=min(max(exc.limit, 0), 100_000_000),
    )
    raise ColloquialConfirmationWorkLimitError(
        exc.dimension,
        exc.observed,
        exc.limit,
    ) from exc


def _admit_discovery_intent_text(intent: Intent) -> None:
    for dimension, observed, limit in (
        ("services", len(intent.services), MAX_COLLOQUIAL_SERVICES),
        ("environments", len(intent.environments), MAX_COLLOQUIAL_ENVIRONMENTS),
        ("keywords", len(intent.keywords), MAX_COLLOQUIAL_KEYWORDS),
        ("intent_archetypes", len(intent.archetypes), MAX_COLLOQUIAL_ARCHETYPES),
        ("keyword_evidence", len(intent.keyword_evidence), MAX_COLLOQUIAL_EVIDENCE_ITEMS),
    ):
        if observed > limit:
            raise ColloquialConfirmationWorkLimitError(dimension, observed, limit)
    for evidence in intent.keyword_evidence:
        if not isinstance(evidence, dict):
            raise ColloquialConfirmationWorkLimitError("keyword_evidence_shape", 1, 0)
        if len(evidence) > MAX_COLLOQUIAL_EVIDENCE_FIELDS:
            raise ColloquialConfirmationWorkLimitError(
                "keyword_evidence_fields",
                len(evidence),
                MAX_COLLOQUIAL_EVIDENCE_FIELDS,
            )
    text_values: list[object] = [
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
        text_values.extend(
            (
                evidence.get("keyword", ""),
                evidence.get("tier", ""),
                evidence.get("source", ""),
            )
        )
    try:
        admit_resolution_input_text(text_values)
    except ResolutionInputWorkLimitError as exc:
        _raise_discovery_input_text_limit(exc)


def discovery_keywords(intent: Intent) -> list[str]:
    """Include advisory evidence when searching provider-scoped catalogs.

    Colloquial evidence may broaden discovery so providers such as CloudWatch
    can inspect the relevant namespace. It does not become trusted intent until
    post-discovery semantic-signal confirmation succeeds.
    """
    _admit_discovery_intent_text(intent)
    keywords = list(intent.keywords)
    seen = {str(keyword).lower() for keyword in keywords}
    for item in intent.keyword_evidence:
        keyword = str(item.get("keyword", ""))
        if keyword and keyword.lower() not in seen:
            seen.add(keyword.lower())
            keywords.append(keyword)
    return keywords


async def discover_catalogs(backends: Iterable[DashboardBackend], intent: Intent) -> DiscoveryResult:
    """Collect metric and datasource-target catalogs from every active backend."""
    keywords = discovery_keywords(intent)
    metric_catalog: list[MetricEntry] = []
    datasource_catalog: list[MetricEntry] = []
    datasource_types: list[str] = []

    for backend in backends:
        entries = await backend.discover_metrics(keywords, intent)
        metric_catalog.extend(entries)
        if entries:
            datasource_types.append(backend.name)
            continue
        if not getattr(getattr(backend, "last_discovery_status", None), "available", True):
            continue
        target_discovery = getattr(backend, "discover_datasource_targets", None)
        if target_discovery is None:
            continue
        targets = await target_discovery(keywords, intent)
        datasource_catalog.extend(targets)
        if targets and backend.name not in datasource_types:
            datasource_types.append(backend.name)

    return DiscoveryResult(
        metric_catalog=metric_catalog,
        datasource_catalog=datasource_catalog,
        datasource_types=datasource_types,
    )


def semantic_mapping_diagnostics(metric_catalog: list[MetricEntry]) -> tuple[str, str, dict]:
    """Measure deterministic name-level semantic mapping independently of binding."""
    from tacit.signal_inference import infer_signals

    if len(metric_catalog) > MAX_COLLOQUIAL_CATALOG_ENTRIES:
        raise ColloquialConfirmationWorkLimitError(
            "catalog_entries",
            len(metric_catalog),
            MAX_COLLOQUIAL_CATALOG_ENTRIES,
        )
    total_dimensions = 0
    text_values: list[object] = []
    for entry in metric_catalog:
        dimension_count = len(entry.dimensions)
        if dimension_count > MAX_COLLOQUIAL_DIMENSIONS_PER_ENTRY:
            raise ColloquialConfirmationWorkLimitError(
                "dimensions_per_catalog_entry",
                dimension_count,
                MAX_COLLOQUIAL_DIMENSIONS_PER_ENTRY,
            )
        total_dimensions += dimension_count
        if total_dimensions > MAX_COLLOQUIAL_TOTAL_DIMENSIONS:
            raise ColloquialConfirmationWorkLimitError(
                "total_catalog_dimensions",
                total_dimensions,
                MAX_COLLOQUIAL_TOTAL_DIMENSIONS,
            )
        text_values.extend(
            (
                entry.name,
                entry.datasource_uid,
                entry.datasource_name,
                entry.datasource_type,
                entry.query_language,
                entry.namespace,
                *entry.dimensions,
                entry.unit,
                entry.metric_type,
            )
        )
    try:
        admit_resolution_input_text(text_values)
    except ResolutionInputWorkLimitError as exc:
        _raise_discovery_input_text_limit(exc)
    names = list(dict.fromkeys(entry.name for entry in metric_catalog if entry.name))
    inferred = infer_signals(names)
    mapped = {item.metric: item.signal_family for item in inferred}
    unmapped = [name for name in names if name not in mapped]
    if not names:
        return "skipped", "no_named_metrics", {"metrics_total": 0}
    if not mapped:
        status, reason = "failed", "no_metrics_semantically_mapped"
    elif unmapped:
        status, reason = "partial", "some_metrics_unmapped"
    else:
        status, reason = "passed", "all_metrics_semantically_mapped"
    return (
        status,
        reason,
        {
            "metrics_total": len(names),
            "metrics_mapped": len(mapped),
            "coverage": round(len(mapped) / len(names), 4),
            "mapped": mapped,
            "unmapped": unmapped,
        },
    )


def discovery_stage_status(result: DiscoveryResult) -> tuple[str, str, dict]:
    """Return status/reason/details for the discovery diagnostic stage."""
    if result.metric_catalog:
        return (
            "passed",
            "named_metrics_discovered",
            {
                "metric_count": len(result.metric_catalog),
                "datasource_count": len(result.datasource_types),
                "datasource_uids": sorted({entry.datasource_uid for entry in result.metric_catalog}),
            },
        )
    if result.datasource_catalog:
        return (
            "partial",
            "datasource_targets_without_metric_names",
            {
                "target_count": len(result.datasource_catalog),
                "datasource_count": len(result.datasource_types),
            },
        )
    return (
        "failed",
        "no_metrics_or_datasource_targets",
        {"datasource_count": len(result.datasource_types)},
    )


def confirm_colloquial_keywords(
    intent: Intent,
    metric_catalog: list[MetricEntry],
    target_query_language: str,
    signal_store: Any | None = None,
    tenant_id: str = "default",
    knowledge_scope: Any | None = None,
    excluded_knowledge_refs: set[KnowledgeRevisionRef] | None = None,
    apply_to_intent: bool = True,
    resolution_work_budget: SignalResolutionWorkBudget | None = None,
    input_text_limits: ResolutionInputTextLimits | None = None,
) -> ConfirmedKeywords:
    """Promote low-confidence colloquial evidence only after live signal coverage.

    A metaphor implying "cache" becomes a real keyword only if a cache signal
    resolves against the service-scoped discovered metrics, using the signal
    store instead of a global substring match.
    """
    if not intent.keyword_evidence or not metric_catalog:
        return ConfirmedKeywords()
    if len(intent.keyword_evidence) > MAX_COLLOQUIAL_EVIDENCE_ITEMS:
        raise ColloquialConfirmationWorkLimitError(
            "evidence_items",
            len(intent.keyword_evidence),
            MAX_COLLOQUIAL_EVIDENCE_ITEMS,
        )
    if len(metric_catalog) > MAX_COLLOQUIAL_CATALOG_ENTRIES:
        raise ColloquialConfirmationWorkLimitError(
            "catalog_entries",
            len(metric_catalog),
            MAX_COLLOQUIAL_CATALOG_ENTRIES,
        )
    if len(intent.services) > MAX_COLLOQUIAL_SERVICES:
        raise ColloquialConfirmationWorkLimitError(
            "services",
            len(intent.services),
            MAX_COLLOQUIAL_SERVICES,
        )
    for dimension, observed, limit in (
        ("environments", len(intent.environments), MAX_COLLOQUIAL_ENVIRONMENTS),
        ("keywords", len(intent.keywords), MAX_COLLOQUIAL_KEYWORDS),
        ("intent_archetypes", len(intent.archetypes), MAX_COLLOQUIAL_ARCHETYPES),
    ):
        if observed > limit:
            raise ColloquialConfirmationWorkLimitError(dimension, observed, limit)
    for evidence in intent.keyword_evidence:
        if not isinstance(evidence, dict):
            raise ColloquialConfirmationWorkLimitError("keyword_evidence_shape", 1, 0)
        if len(evidence) > MAX_COLLOQUIAL_EVIDENCE_FIELDS:
            raise ColloquialConfirmationWorkLimitError(
                "keyword_evidence_fields",
                len(evidence),
                MAX_COLLOQUIAL_EVIDENCE_FIELDS,
            )
    total_dimensions = 0
    for entry in metric_catalog:
        dimension_count = len(entry.dimensions)
        if dimension_count > MAX_COLLOQUIAL_DIMENSIONS_PER_ENTRY:
            raise ColloquialConfirmationWorkLimitError(
                "dimensions_per_catalog_entry",
                dimension_count,
                MAX_COLLOQUIAL_DIMENSIONS_PER_ENTRY,
            )
        total_dimensions += dimension_count
        if total_dimensions > MAX_COLLOQUIAL_TOTAL_DIMENSIONS:
            raise ColloquialConfirmationWorkLimitError(
                "total_catalog_dimensions",
                total_dimensions,
                MAX_COLLOQUIAL_TOTAL_DIMENSIONS,
            )

    text_values: list[object] = [
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
        text_values.extend(
            (
                evidence.get("keyword", ""),
                evidence.get("tier", ""),
                evidence.get("source", ""),
            )
        )
    for entry in metric_catalog:
        text_values.extend(
            (
                entry.name,
                entry.datasource_uid,
                entry.datasource_name,
                entry.datasource_type,
                entry.query_language,
                entry.namespace,
                *entry.dimensions,
                entry.unit,
                entry.metric_type,
            )
        )
    try:
        admit_resolution_input_text(text_values, limits=input_text_limits)
    except ResolutionInputWorkLimitError as exc:
        logger.warning(
            ColloquialConfirmationWorkLimitError.reason_code,
            reason_code=ColloquialConfirmationWorkLimitError.reason_code,
            dimension=exc.dimension,
            observed=min(max(exc.observed, 0), 100_000_000),
            limit=min(max(exc.limit, 0), 100_000_000),
        )
        raise ColloquialConfirmationWorkLimitError(
            exc.dimension,
            exc.observed,
            exc.limit,
        ) from exc

    try:
        from tacit.agents.synonyms import KEYWORD_SIGNALS, SynonymEvidence, confirm_colloquial
        from tacit.archetypes.engine import ArchetypeCoverageWorkLimits, _datasource_type_for_language
        from tacit.signals import get_signal_store

        signal_store = resolve_signal_store(signal_store, get_signal_store)
        if signal_store is None:
            return ConfirmedKeywords()
        active_resolution_work_budget = resolution_work_budget
        budget_factory = getattr(signal_store, "new_signal_resolution_work_budget", None)
        if (
            active_resolution_work_budget is None
            and bool(getattr(signal_store, "supports_signal_resolution_work_budget", False))
            and callable(budget_factory)
        ):
            coverage_limits = ArchetypeCoverageWorkLimits()
            active_resolution_work_budget = budget_factory(
                max_calls=(MAX_COLLOQUIAL_SIGNAL_RESOLUTIONS + coverage_limits.max_total_resolver_calls),
                max_mapping_catalog_comparisons=(coverage_limits.max_total_catalog_comparisons),
                max_results=coverage_limits.max_total_resolution_results,
            )
        resolve_cache: dict[str, bool] = {}
        revision_refs_by_signal: dict[str, set[KnowledgeRevisionRef]] = {}
        confirmation_catalog = catalog_for_services(metric_catalog, intent.services)
        context_service = intent.services[0] if intent.services else ""
        resolution_attempts = 0
        degraded = False

        def signal_resolves(sig: str) -> bool:
            nonlocal degraded, resolution_attempts
            if sig not in resolve_cache:
                resolution_attempts += 1
                if resolution_attempts > MAX_COLLOQUIAL_SIGNAL_RESOLUTIONS:
                    raise ColloquialConfirmationWorkLimitError(
                        "signal_resolutions",
                        resolution_attempts,
                        MAX_COLLOQUIAL_SIGNAL_RESOLUTIONS,
                    )
                try:
                    work_kwargs = (
                        {"work_budget": active_resolution_work_budget}
                        if active_resolution_work_budget is not None
                        and bool(
                            getattr(
                                signal_store,
                                "supports_signal_resolution_work_budget",
                                False,
                            )
                        )
                        else {}
                    )
                    hits = signal_store.resolve_signal_details(
                        sig,
                        confirmation_catalog,
                        context_service=context_service,
                        context_datasource_type=_datasource_type_for_language(target_query_language),
                        target_query_language=target_query_language,
                        tenant_id=tenant_id,
                        knowledge_scope=knowledge_scope,
                        excluded_knowledge_refs=excluded_knowledge_refs,
                        **work_kwargs,
                    )
                    resolve_cache[sig] = bool(hits)
                    revision_refs = {
                        hit.knowledge_revision_ref for hit in hits if hit.knowledge_revision_ref is not None
                    }
                    if revision_refs:
                        revision_refs_by_signal[sig] = revision_refs
                except AUTHORITY_BOUNDARY_ERRORS:
                    raise
                except asyncio.CancelledError:
                    raise
                except ColloquialConfirmationWorkLimitError:
                    raise
                except SignalResolutionWorkLimitError:
                    raise
                except Exception as exc:
                    degraded = True
                    logger.warning(
                        "colloquial_signal_resolution_failed",
                        reason_code="colloquial_signal_resolution_failed",
                        **safe_failure_diagnostics(
                            exc,
                            reason_code="colloquial_signal_resolution_failed",
                        ),
                    )
                    resolve_cache[sig] = False
            return resolve_cache[sig]

        synonym_evidence = [
            SynonymEvidence(
                keyword=str(e.get("keyword", "")),
                score=float(e.get("score", 0.0)),
                tier=str(e.get("tier", "")),
                source=str(e.get("source", "")),
            )
            for e in intent.keyword_evidence
        ]
        confirmed = confirm_colloquial(synonym_evidence, signal_resolves)
        added_keywords: list[str] = []
        for kw in confirmed:
            if apply_to_intent and kw not in intent.keywords:
                intent.keywords.append(kw)
                added_keywords.append(kw)
            elif kw not in intent.keywords:
                added_keywords.append(kw)
        revision_refs_by_keyword = {
            keyword: set().union(
                *(revision_refs_by_signal.get(signal, set()) for signal in KEYWORD_SIGNALS.get(keyword, ()))
            )
            for keyword in confirmed
        }
        return ConfirmedKeywords(
            confirmed,
            revision_refs_by_keyword=revision_refs_by_keyword,
            added_keywords=added_keywords,
            degraded=degraded,
            resolution_work_budget=active_resolution_work_budget,
        )
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except asyncio.CancelledError:
        raise
    except ColloquialConfirmationWorkLimitError:
        raise
    except SignalResolutionWorkLimitError:
        raise
    except Exception as exc:
        logger.warning(
            "colloquial_confirmation_failed",
            reason_code="colloquial_confirmation_failed",
            **safe_failure_diagnostics(
                exc,
                reason_code="colloquial_confirmation_failed",
            ),
        )
        return ConfirmedKeywords(degraded=True)
