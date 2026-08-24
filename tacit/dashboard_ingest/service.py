"""Dashboard ingestion — learn operational patterns from existing dashboards.

Vendor-agnostic: each DashboardBackend implements ``ingest_dashboard()``
which returns a common ``DashboardFeatures`` dataclass.  This module handles
the vendor-independent parts: signal inference, optional quarantined
archetype-candidate generation, and signal store persistence.

Per-backend parsers extract:
- Metric names from queries (PromQL, SignalFlow, LogQL, CloudWatch, etc.)
- Panel titles and descriptions
- Row/section groupings
- Metric co-occurrence patterns (which metrics appear together)
- Aggregation patterns (rate, histogram_quantile, .percentile, etc.)
- Query transformations (the raw query templates)
- Dashboard tags
- Alert rule links
- Drilldown links to other dashboards

Then infers signal types by matching extracted metrics against the signal
store's taxonomy. Experimental archetype generation is disabled by default;
when explicitly enabled, its YAML output is quarantined and cannot enter the
curated registry or normal retrieval.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import structlog

from tacit.archetypes.generated.store import quarantine_generated_archetype_yaml
from tacit.config import Settings, settings
from tacit.dashboard_ingest.archetype_generation import generate_archetype_yaml
from tacit.dashboard_ingest.features import (
    features_to_dict as _features_to_dict,
)
from tacit.dashboard_ingest.reports import build_learning_impact_report, build_signal_quality_report
from tacit.dependencies import create_scoped_knowledge_service, resolve_owned_database_path
from tacit.knowledge.authorization import KnowledgeAction, enforce_knowledge_action
from tacit.models.schemas import MetricEntry
from tacit.runtime_ownership import describe_runtime_owner, resolve_runtime_settings
from tacit.signals import get_signal_store as _default_get_signal_store

logger = structlog.get_logger()
_ARCHETYPE_QUARANTINE_LOCK = threading.Lock()
_LEGACY_MAPPING_EXPANSION_LIMIT = 500


class DashboardReviewConflictError(RuntimeError):
    """Raised when another dashboard lifecycle transition wins review."""


def get_signal_store():
    """Resolve the signal store through the package façade for test isolation."""
    import tacit.dashboard_ingest as dashboard_ingest_pkg

    package_getter = getattr(dashboard_ingest_pkg, "get_signal_store", _default_get_signal_store)
    if package_getter is get_signal_store:
        return _default_get_signal_store()
    return package_getter()


def _active_runtime_settings(
    runtime_settings: Settings | None,
    store: Any | None,
    knowledge_service: Any | None = None,
) -> Settings:
    """Resolve policy and tenancy from the same runtime that owns persistence."""
    store_owner = describe_runtime_owner("signal_store", store)
    service_owner = describe_runtime_owner("knowledge_service", knowledge_service)
    active_settings = resolve_runtime_settings(
        boundary="Dashboard and alert learning",
        explicit_settings=runtime_settings,
        owners=(store_owner, service_owner),
        fallback_settings=settings,
    )
    supplied_owners = tuple(
        (name, owner)
        for name, owner in (("signal_store", store), ("knowledge_service", knowledge_service))
        if owner is not None
    )
    if supplied_owners:
        resolve_owned_database_path(
            boundary="Dashboard and alert learning",
            database_role="signals",
            owners=supplied_owners,
            runtime_settings=active_settings,
        )
    return active_settings


def _signal_store_for_runtime(
    store: Any | None,
    runtime_settings: Settings | None,
    *,
    fallback_factory: Any | None = None,
    knowledge_service: Any | None = None,
    resolved_runtime_settings: Settings | None = None,
) -> Any:
    """Resolve persistence from an explicit runtime before consulting legacy globals."""
    if store is not None:
        return store
    if knowledge_service is not None:
        selected_settings = resolved_runtime_settings or runtime_settings
        if selected_settings is None:
            raise ValueError("Knowledge service signal-store resolution requires resolved runtime settings")
        database_path = resolve_owned_database_path(
            boundary="Dashboard learning signal store resolution",
            database_role="signals",
            owners=(("knowledge_service", knowledge_service),),
            runtime_settings=selected_settings,
        )
        from tacit.signals import SignalStore

        return SignalStore(database_path, runtime_settings=selected_settings)
    if runtime_settings is not None:
        from tacit.runtime_stores import RuntimeStores

        return RuntimeStores(runtime_settings).signals()
    return (fallback_factory or get_signal_store)()


def _authorized_signal_store(
    *,
    runtime_settings: Settings | None,
    store: Any | None,
    knowledge_service: Any | None,
    tenant_id: str | None,
    actions: tuple[KnowledgeAction, ...],
) -> tuple[Settings, str, Any]:
    """Authorize before realization, then bind authorization to the realized owner."""
    preliminary_settings = _active_runtime_settings(runtime_settings, store, knowledge_service)
    for action in actions:
        enforce_knowledge_action(preliminary_settings, action)
    resolve_learning_tenant(tenant_id, runtime_settings=preliminary_settings)

    realized_store = _signal_store_for_runtime(
        store,
        runtime_settings,
        knowledge_service=knowledge_service,
        resolved_runtime_settings=preliminary_settings,
    )
    active_settings = _active_runtime_settings(runtime_settings, realized_store, knowledge_service)
    for action in actions:
        enforce_knowledge_action(active_settings, action)
    effective_tenant = resolve_learning_tenant(tenant_id, runtime_settings=active_settings)
    return active_settings, effective_tenant, realized_store


def _build_active_backends(factory: Any, runtime_settings: Settings) -> list[Any]:
    """Pass scoped settings while preserving established zero-argument patch points."""
    try:
        accepts_settings = bool(inspect.signature(factory).parameters)
    except (TypeError, ValueError):
        accepts_settings = True
    return factory(runtime_settings) if accepts_settings else factory()


# ── Signal inference ─────────────────────────────────────────────────────────


def infer_signals_from_metrics(
    metrics: list[str],
    panel_data: list[dict[str, Any]] | None = None,
    *,
    store: Any | None = None,
    tenant_id: str = "default",
) -> list[dict[str, Any]]:
    """Infer semantic signals from extracted metrics.

    Two layers:
      1. Curated taxonomy — match metrics against signals already known/taught
         (authoritative; highest confidence).
      2. Deterministic heuristic inference (``signal_inference``) for everything
         the taxonomy doesn't recognize, using metric morphology + panel context.
         This is what lets *custom* metrics (e.g. ``felix_*``) map to signals
         without anyone hand-teaching them first.

    Returns a list of dicts with: signal_type (name), metric, confidence,
    signal_family, source ('taxonomy'|'heuristic'), reason, evidence.
    """
    store = store or get_signal_store()
    inferred: list[dict[str, Any]] = []
    matched_entry_keys: set[tuple[str, str, str]] = set()

    # 1. Curated taxonomy matches.
    taxonomy_started_at = time.monotonic()
    metric_entries = _metric_entries_for_signal_inference(metrics, panel_data or [])
    entries_by_metric: dict[str, list[MetricEntry]] = {}
    for entry in metric_entries:
        entries_by_metric.setdefault(entry.name, []).append(entry)
    taxonomy_matches = store.resolve_metric_signal_details(
        metric_entries,
        tenant_id=tenant_id,
    )
    taxonomy_rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for match in taxonomy_matches:
        datasource_type = match.entry.datasource_type.casefold()
        query_language = match.entry.query_language.casefold()
        key = (match.signal_type, match.entry.name, datasource_type, query_language)
        matched_entry_keys.add((match.entry.name, datasource_type, query_language))
        row = taxonomy_rows.get(key)
        if row is None:
            row = {
                "signal_type": match.signal_type,
                "metric": match.entry.name,
                "confidence": match.confidence,
                "signal_family": match.signal_family,
                "source": "taxonomy",
                "reason": f"matches pattern '{match.metric_pattern}'",
                "evidence": [f"matches taught pattern '{match.metric_pattern}'"],
                "datasource_types": [match.entry.datasource_type] if match.entry.datasource_type else [],
                "query_languages": [match.entry.query_language] if match.entry.query_language else [],
            }
            taxonomy_rows[key] = row
    inferred.extend(taxonomy_rows.values())

    taxonomy_duration_ms = round((time.monotonic() - taxonomy_started_at) * 1000, 2)
    matched_signal_types = {match.signal_type for match in taxonomy_matches}
    taxonomy_log = logger.warning if taxonomy_duration_ms >= 500 else logger.info
    taxonomy_log(
        "signal_inference_taxonomy_scan",
        tenant_id=tenant_id,
        metric_count=len(metrics),
        signal_type_count=len(matched_signal_types),
        mapping_count=len(taxonomy_matches),
        mapping_lookup_count=len(matched_signal_types),
        matched_metric_count=len({key[0] for key in matched_entry_keys}),
        duration_ms=taxonomy_duration_ms,
    )

    # 2. Heuristic fallback for metrics the taxonomy didn't recognize.
    from tacit.signal_inference import INFERENCE_VERSION
    from tacit.signal_inference import infer_signals as _infer_heuristic

    unmatched = [
        metric
        for metric in dict.fromkeys(metrics)
        if any(
            (entry.name, entry.datasource_type.casefold(), entry.query_language.casefold()) not in matched_entry_keys
            for entry in entries_by_metric.get(metric, [])
        )
    ]
    for sig in _infer_heuristic(unmatched, panel_data or []):
        signal_type = _canonical_signal_type_for_heuristic(sig)
        signal_family = "saturation" if signal_type == "db_connection_pool" else sig.signal_family
        unresolved_entries = [
            entry
            for entry in entries_by_metric.get(sig.metric, [])
            if (entry.name, entry.datasource_type.casefold(), entry.query_language.casefold()) not in matched_entry_keys
        ] or [
            MetricEntry(
                name=sig.metric,
                datasource_uid="",
                datasource_name="",
                datasource_type="",
                query_language="",
            )
        ]
        for entry in unresolved_entries:
            inferred.append(
                {
                    "signal_type": signal_type,
                    "raw_signal_type": sig.signal_name,
                    "metric": sig.metric,
                    "confidence": sig.confidence,
                    "score": sig.score,
                    "margin": sig.margin,
                    "confidence_label": sig.confidence_label,
                    "signal_family": signal_family,
                    "source": "heuristic",
                    "reason": "; ".join(sig.evidence),
                    "evidence": sig.evidence,
                    "evidence_sources": sig.evidence_sources,
                    "auto_teach_eligible": sig.auto_teach_eligible,
                    "why_not_auto_taught": sig.why_not_auto_taught,
                    "inference_version": INFERENCE_VERSION,
                    "datasource_types": [entry.datasource_type] if entry.datasource_type else [],
                    "query_languages": [entry.query_language] if entry.query_language else [],
                }
            )

    inferred.sort(key=lambda x: x["confidence"], reverse=True)
    return inferred


def _metric_entries_for_signal_inference(
    metrics: list[str],
    panel_data: list[dict[str, Any]],
) -> list[MetricEntry]:
    """Retain the datasource identity that made each learned metric observable."""
    ordered_metrics = list(dict.fromkeys(metrics))
    requested = set(ordered_metrics)
    entries_by_metric: dict[str, list[MetricEntry]] = {metric: [] for metric in ordered_metrics}
    seen: set[tuple[str, str, str]] = set()
    for panel in panel_data:
        source_metrics: set[str] = set()
        for source in panel.get("metric_sources", []) or []:
            metric = str(source.get("metric") or "")
            if metric not in requested:
                continue
            datasource_type = str(source.get("datasource_type") or "")
            query_language = str(source.get("query_language") or "")
            key = (metric, datasource_type.casefold(), query_language.casefold())
            source_metrics.add(metric)
            if key in seen:
                continue
            seen.add(key)
            entries_by_metric[metric].append(
                MetricEntry(
                    name=metric,
                    datasource_uid="",
                    datasource_name="",
                    datasource_type=datasource_type,
                    query_language=query_language,
                )
            )
        datasource_type = str(panel.get("datasource_type") or "")
        query_language = str(panel.get("query_language") or "")
        for raw_metric in panel.get("metrics", []) or []:
            metric = str(raw_metric)
            if metric in source_metrics:
                continue
            key = (metric, datasource_type.casefold(), query_language.casefold())
            if metric not in requested or key in seen:
                continue
            seen.add(key)
            entries_by_metric[metric].append(
                MetricEntry(
                    name=metric,
                    datasource_uid="",
                    datasource_name="",
                    datasource_type=datasource_type,
                    query_language=query_language,
                )
            )
    return [
        entry
        for metric in ordered_metrics
        for entry in (
            entries_by_metric[metric]
            or [
                MetricEntry(
                    name=metric,
                    datasource_uid="",
                    datasource_name="",
                    datasource_type="",
                    query_language="",
                )
            ]
        )
    ]


def _canonical_signal_type_for_heuristic(sig: Any) -> str:
    """Map heuristic families onto canonical signals used by archetypes."""
    metric = sig.metric.lower()
    family = sig.signal_family
    if family == "latency":
        if "pool" in metric and "wait" in metric:
            if any(token in metric for token in ("db", "database", "sql", "query", "connection")):
                return "db_connection_pool"
            return sig.signal_name
        if any(token in metric for token in ("db", "sql", "query")):
            return "db_query_latency"
        if "dns" in metric:
            return "dns_latency"
        return "request_latency"
    if family == "errors":
        if "dns" in metric:
            return "dns_failures"
        if any(token in metric for token in ("tls", "cert", "handshake")):
            return "tls_handshake_failures"
        return "error_rate"
    if family == "traffic":
        return "request_rate"
    if family == "backlog":
        if "lag" in metric:
            return "consumer_lag"
        return "queue_depth"
    if family == "resource_usage":
        if "cpu" in metric:
            return "cpu_usage"
        if "memory" in metric or "_mem_" in metric:
            return "memory_usage"
        if "disk" in metric:
            return "disk_usage"
    if family == "saturation":
        return "in_flight_requests"
    return sig.signal_name


# ── Full ingestion pipeline ─────────────────────────────────────────────────


def persist_inferred_signal_review(
    *,
    store: Any,
    sig: dict[str, Any],
    source_ref: str,
    dashboard_uid: str,
    backend_name: str = "",
    tenant_id: str | None = None,
    source_type: str = "dashboard_ingest",
    source_fingerprint: str = "",
    runtime_settings: Settings | None = None,
    governed_candidate_ids: set[str] | None = None,
    governed_pairs: set[tuple[str, str]] | None = None,
    knowledge_service: Any | None = None,
) -> bool:
    """Persist one inferred signal using the same gate for all approval paths."""
    from tacit.knowledge.authorization import KnowledgeAction, enforce_knowledge_action

    active_settings = _active_runtime_settings(runtime_settings, store, knowledge_service)
    enforce_knowledge_action(active_settings, KnowledgeAction.READ)
    enforce_knowledge_action(active_settings, KnowledgeAction.TEACH_SIGNALS)
    enforce_knowledge_action(active_settings, KnowledgeAction.APPLY)
    signal_type = sig["signal_type"]
    metric = sig.get("metric", "")
    confidence = sig.get("confidence", 0.6)
    is_heuristic = sig.get("source") == "heuristic"
    effective_tenant = resolve_learning_tenant(tenant_id, runtime_settings=active_settings)

    should_teach = _governable_signal(sig)

    if should_teach:
        knowledge_service = knowledge_service or _knowledge_service_for_store(
            store,
            runtime_settings=active_settings,
        )
        if governed_pairs is not None:
            governed_pairs.add((metric, signal_type))
        family = sig.get("signal_family", "")
        if family:
            store.register_signal_type(signal_type=signal_type, category=family, tenant_id=effective_tenant)
        store.add_mapping(
            signal_type=signal_type,
            metric_pattern=metric,
            confidence=confidence,
            context_datasource_types=sig.get("datasource_types", []),
            source_type=source_type,
            source_refs=[source_ref],
            inference_version=sig.get("inference_version", ""),
            review_state="candidate",
            tenant_id=effective_tenant,
        )
        governed_candidate_id = _govern_signal_mapping(
            store=store,
            sig=sig,
            source_ref=source_ref,
            source_type=source_type,
            source_fingerprint=source_fingerprint,
            tenant_id=effective_tenant,
            runtime_settings=runtime_settings,
            knowledge_service=knowledge_service,
        )
        if governed_candidate_ids is not None:
            governed_candidate_ids.add(governed_candidate_id)
        governed_knowledge_ref = _active_governed_signal_mapping_ref(
            store=store,
            candidate_id=governed_candidate_id,
            tenant_id=effective_tenant,
            repository=knowledge_service.repository,
        )
        if governed_knowledge_ref:
            return True
        return False

    if is_heuristic and metric:
        store.record_rejected_candidate(
            metric=metric,
            signal_family=sig.get("signal_family", ""),
            signal_name=signal_type,
            score=sig.get("score", 0.0),
            margin=sig.get("margin", 0.0),
            why_not=sig.get("why_not_auto_taught") or "low_score",
            evidence=sig.get("evidence", []),
            inference_version=sig.get("inference_version", ""),
            dashboard_uid=dashboard_uid,
            backend_name=backend_name,
            tenant_id=effective_tenant,
        )
    return False


def _governable_signal(sig: dict[str, Any]) -> bool:
    metric = str(sig.get("metric") or "")
    if sig.get("source") == "heuristic":
        return bool(metric) and bool(sig.get("auto_teach_eligible"))
    return bool(metric) and float(sig.get("confidence", 0.6)) >= 0.5


def _governable_signal_pairs(signals: list[dict[str, Any]]) -> set[tuple[str, str]]:
    return {
        (str(sig.get("metric") or ""), str(sig.get("signal_type") or "")) for sig in signals if _governable_signal(sig)
    }


def _existing_governed_candidate_ids(
    *,
    store: Any,
    tenant_id: str,
    source_ref: str,
    active_pairs: set[tuple[str, str]],
    source_fingerprint: str = "",
    repository: Any | None = None,
) -> set[str]:
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.repository import KnowledgeRepository

    repository = repository or KnowledgeRepository(
        resolve_owned_database_path(
            boundary="Dashboard governed candidate lookup",
            database_role="signals",
            owners=(("signal_store", store),),
        )
    )
    active: set[str] = set()
    after_candidate_id: str | None = None
    while True:
        page = repository.list_candidates_for_provenance(
            tenant_id,
            source_ref,
            after_candidate_id=after_candidate_id,
            kind=KnowledgeKind.SIGNAL_MAPPING.value,
        )
        if not page:
            break
        after_candidate_id = page[-1].id
        for candidate in page:
            source_evidence = [item for item in candidate.evidence.items if source_ref in item.provenance_refs]
            if source_fingerprint and not any(item.lineage_group == source_fingerprint for item in source_evidence):
                logger.info(
                    "source_lineage_changed_pending_review",
                    tenant_id=tenant_id,
                    source_ref=source_ref,
                    candidate_id=candidate.id,
                )
                continue
            metric = str(
                candidate.typed_payload.get("metric_pattern")
                or candidate.typed_payload.get("candidate_metric")
                or candidate.typed_payload.get("metric")
                or ""
            )
            signal = str(candidate.typed_payload.get("signal_type") or "")
            if (metric, signal) in active_pairs:
                active.add(candidate.id)
    return active


def _govern_signal_mapping(
    *,
    store: Any,
    sig: dict[str, Any],
    source_ref: str,
    source_type: str,
    source_fingerprint: str = "",
    tenant_id: str | None,
    runtime_settings: Settings | None = None,
    knowledge_service: Any | None = None,
) -> str:
    active_settings = _active_runtime_settings(runtime_settings, store, knowledge_service)
    effective_tenant = resolve_learning_tenant(tenant_id, runtime_settings=active_settings)
    from tacit.knowledge.migration import migrate_signal_mapping

    knowledge_service = knowledge_service or _knowledge_service_for_store(
        store,
        runtime_settings=active_settings,
    )

    record_ref = f"{source_ref}:{sig['signal_type']}:{sig.get('metric', '')}"
    return migrate_signal_mapping(
        {
            "id": record_ref,
            "signal_type": sig["signal_type"],
            "metric_pattern": sig.get("metric", ""),
            "context_services": sig.get("services", []),
            "context_environments": sig.get("environments", []),
            "context_archetypes": sig.get("archetypes", []),
            "context_datasource_types": sig.get("datasource_types", []),
            "source_type": source_type,
            "source_refs": [source_ref],
            "source_fingerprint": source_fingerprint,
            "review_state": "approved" if sig.get("source") == "heuristic" else "trusted",
        },
        service=knowledge_service,
        tenant_id=effective_tenant,
    )


def _knowledge_service_for_store(
    store: Any,
    *,
    runtime_settings: Settings,
) -> Any:
    return create_scoped_knowledge_service(
        store,
        runtime_settings=runtime_settings,
        boundary="Dashboard signal and knowledge persistence",
    )


@contextmanager
def _source_authority_transaction(
    *,
    store: Any,
    knowledge_service: Any,
    tenant_id: str,
    source_ref: str,
    operation: str,
):
    """Commit source state, resolver mappings, and governed authority together."""
    bind_connection = getattr(knowledge_service.repository, "bind_transaction_connection", None)
    if bind_connection is None:
        raise ValueError("source authority reconciliation requires a transactional knowledge repository")

    store.ensure_governed_projection_audit_current()
    started_at = time.monotonic()
    committed = False
    try:
        with store.transaction() as conn:
            if not store.governed_projection_audit_is_current(conn):
                raise DashboardReviewConflictError(
                    "governed signal projection changed before dashboard authority reconciliation; retry"
                )
            with bind_connection(conn):
                yield conn
        committed = True
    finally:
        duration_ms = round((time.monotonic() - started_at) * 1000, 2)
        log = logger.warning if duration_ms >= 1000 else logger.info
        log(
            "source_authority_transaction",
            tenant_id=tenant_id,
            source_ref=source_ref,
            operation=operation,
            duration_ms=duration_ms,
            committed=committed,
        )


def _dashboard_content_fingerprint(features: Any) -> str:
    payload = _features_to_dict(features) if not isinstance(features, dict) else features
    content = {
        key: payload.get(key)
        for key in (
            "dashboard_tags",
            "metrics_found",
            "panel_count",
            "row_groups",
            "metric_cooccurrence",
            "aggregation_patterns",
            "query_transformations",
            "panel_titles",
            "alert_links",
            "drilldown_links",
            "signals_inferred",
        )
    }
    return hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()


def _active_governed_signal_mapping_ref(
    *,
    store: Any,
    candidate_id: str,
    tenant_id: str,
    repository: Any | None = None,
) -> str:
    from tacit.knowledge.enums import KnowledgeEligibility, LifecycleStatus
    from tacit.knowledge.repository import KnowledgeRepository

    repository = repository or KnowledgeRepository(
        resolve_owned_database_path(
            boundary="Dashboard governed mapping lookup",
            database_role="signals",
            owners=(("signal_store", store),),
        )
    )
    candidate = repository.get_candidate(candidate_id, tenant_id)
    if candidate is None:
        return ""
    item = repository.find_knowledge_by_proposition(tenant_id, candidate.proposition.proposition_key)
    revision = repository.get_revision(item.id, tenant_id=tenant_id) if item is not None else None
    if (
        revision is not None
        and revision.state.lifecycle_status == LifecycleStatus.ACTIVE
        and revision.state.eligibility != KnowledgeEligibility.INELIGIBLE
    ):
        return revision.knowledge_id
    return ""


def reconcile_signal_source(
    *,
    store: Any,
    tenant_id: str,
    source_type: str,
    source_ref: str,
    active_pairs: set[tuple[str, str]],
    active_candidate_ids: set[str],
    runtime_settings: Settings | None = None,
    knowledge_service: Any | None = None,
    max_candidate_count: int | None = None,
) -> None:
    """Reconcile refreshed legacy mappings and governed knowledge together."""
    active_settings = _active_runtime_settings(runtime_settings, store, knowledge_service)
    enforce_knowledge_action(active_settings, KnowledgeAction.READ)
    enforce_knowledge_action(active_settings, KnowledgeAction.APPLY)
    store.reconcile_mapping_source(
        tenant_id=tenant_id,
        source_type=source_type,
        source_ref=source_ref,
        active_pairs=active_pairs,
    )
    knowledge_service = knowledge_service or create_scoped_knowledge_service(
        store,
        runtime_settings=active_settings,
        boundary="Dashboard source reconciliation",
    )
    knowledge_service.reconcile_source_lifecycle(
        provenance_ref=source_ref,
        tenant_id=tenant_id,
        active_candidate_ids=active_candidate_ids,
        max_candidate_count=max_candidate_count,
    )


def _dashboard_source_ref(ingested: dict[str, Any]) -> str:
    backend_name = str(ingested.get("backend_name") or "")
    dashboard_uid = str(ingested.get("dashboard_uid") or "")
    return f"{backend_name}:{dashboard_uid}" if backend_name else dashboard_uid


def _record_dashboard_generation(
    *,
    store: Any,
    features: Any,
    signals: list[dict[str, Any]],
    archetype_yaml: str,
    tenant_id: str,
) -> dict[str, Any]:
    """Persist and reload one dashboard source generation."""
    store.record_ingested_dashboard(
        dashboard_uid=features.dashboard_uid,
        tenant_id=tenant_id,
        backend_name=features.backend_name,
        dashboard_title=features.dashboard_title,
        dashboard_tags=features.dashboard_tags,
        metrics_found=features.metrics_found,
        panel_count=features.panel_count,
        row_groups=features.row_groups,
        metric_cooccurrence=features.metric_cooccurrence,
        aggregation_patterns=features.aggregation_patterns,
        query_transformations=features.query_transformations,
        panel_titles=features.panel_titles,
        alert_links=features.alert_links,
        drilldown_links=features.drilldown_links,
        signals_inferred=signals,
        archetype_generated=archetype_yaml,
        status="pending",
    )
    stored_dashboard = store.get_ingested_dashboard(
        features.dashboard_uid,
        backend_name=features.backend_name,
        tenant_id=tenant_id,
    )
    if stored_dashboard is None:
        raise RuntimeError("Persisted dashboard source record could not be reloaded")
    return stored_dashboard


def _index_dashboard_generation(
    *,
    store: Any,
    features: Any,
    signals: list[dict[str, Any]],
    tenant_id: str,
    status: str,
    activated_pairs: set[tuple[str, str]],
    strict: bool = False,
) -> int:
    """Project one dashboard generation into the tenant retrieval index."""
    return store.index_dashboard_context(
        tenant_id=tenant_id,
        dashboard_uid=features.dashboard_uid,
        backend_name=features.backend_name,
        dashboard_title=features.dashboard_title,
        dashboard_tags=features.dashboard_tags,
        panels=features.panels,
        metrics_found=features.metrics_found,
        signals_inferred=signals,
        status=status,
        activated_pairs=activated_pairs if status == "approved" else None,
        strict=strict,
    )


def _active_pairs_for_candidates(
    *,
    store: Any,
    tenant_id: str,
    candidate_ids: set[str],
    repository: Any,
) -> set[tuple[str, str]]:
    from tacit.knowledge.enums import KnowledgeEligibility, LifecycleStatus

    active: set[tuple[str, str]] = set()
    if not candidate_ids:
        return active
    candidates = repository.get_candidates_by_ids(tenant_id, candidate_ids)
    active_candidate_ids: set[str] = set()
    for revision in repository.list_current_revisions_for_candidates(tenant_id, candidate_ids):
        if (
            revision.state.lifecycle_status == LifecycleStatus.ACTIVE
            and revision.state.eligibility != KnowledgeEligibility.INELIGIBLE
        ):
            active_candidate_ids.update(revision.promoted_from_candidate_refs)
    for candidate_id in sorted(candidate_ids.intersection(active_candidate_ids)):
        candidate = candidates.get(candidate_id)
        if candidate is None:
            continue
        metric = str(
            candidate.typed_payload.get("metric_pattern")
            or candidate.typed_payload.get("candidate_metric")
            or candidate.typed_payload.get("metric")
            or ""
        )
        signal_type = str(candidate.typed_payload.get("signal_type") or "")
        if metric and signal_type:
            active.add((metric, signal_type))
    return active


def _reconcile_dashboard_authority_for_state(
    *,
    store: Any,
    ingested: dict[str, Any],
    tenant_id: str,
    runtime_settings: Settings,
    knowledge_service: Any,
    max_candidate_count: int | None = None,
) -> set[tuple[str, str]]:
    """Reconcile source support to the review state currently stored."""
    source_ref = _dashboard_source_ref(ingested)
    preserve_support = str(ingested.get("status") or "") in {"approved", "approving"}
    governed_pairs = _governable_signal_pairs(ingested.get("signals_inferred", [])) if preserve_support else set()
    candidate_ids = (
        _existing_governed_candidate_ids(
            store=store,
            tenant_id=tenant_id,
            source_ref=source_ref,
            active_pairs=governed_pairs,
            source_fingerprint=_dashboard_content_fingerprint(ingested),
            repository=knowledge_service.repository,
        )
        if preserve_support
        else set()
    )
    reconcile_signal_source(
        store=store,
        tenant_id=tenant_id,
        source_type="dashboard_ingest",
        source_ref=source_ref,
        active_pairs=governed_pairs,
        active_candidate_ids=candidate_ids,
        runtime_settings=runtime_settings,
        knowledge_service=knowledge_service,
        max_candidate_count=max_candidate_count,
    )
    return _active_pairs_for_candidates(
        store=store,
        tenant_id=tenant_id,
        candidate_ids=candidate_ids,
        repository=knowledge_service.repository,
    )


def _reconcile_dashboard_after_approval_loss(
    *,
    store: Any,
    ingested: dict[str, Any],
    tenant_id: str,
    runtime_settings: Settings,
    knowledge_service: Any,
) -> None:
    source_ref = _dashboard_source_ref(ingested)
    with _source_authority_transaction(
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
        source_ref=source_ref,
        operation="repair_approval_loss",
    ):
        current = store.get_ingested_dashboard(
            str(ingested["dashboard_uid"]),
            backend_name=ingested.get("backend_name"),
            tenant_id=tenant_id,
        )
        if current is None:
            reconcile_signal_source(
                store=store,
                tenant_id=tenant_id,
                source_type="dashboard_ingest",
                source_ref=source_ref,
                active_pairs=set(),
                active_candidate_ids=set(),
                runtime_settings=runtime_settings,
                knowledge_service=knowledge_service,
                max_candidate_count=int(runtime_settings.knowledge_source_atomic_candidate_limit),
            )
            return
        _reconcile_dashboard_authority_for_state(
            store=store,
            ingested=current,
            tenant_id=tenant_id,
            runtime_settings=runtime_settings,
            knowledge_service=knowledge_service,
            max_candidate_count=int(runtime_settings.knowledge_source_atomic_candidate_limit),
        )


def resolve_learning_tenant(
    tenant_id: str | None,
    *,
    runtime_settings: Settings | None = None,
) -> str:
    """Resolve an ingestion tenant against the active runtime boundary."""
    from tacit.tenancy import TenantBoundaryError, resolve_tenant_boundary

    active_settings = runtime_settings or settings
    try:
        return resolve_tenant_boundary(
            str(active_settings.knowledge_tenant_id or "default"),
            tenant_id,
        )
    except TenantBoundaryError as exc:
        detail = exc.detail
        if detail == "Knowledge tenant is required":
            detail = "tenant_id is required when knowledge_tenant_id is '*'"
        raise ValueError(detail) from exc


def register_generated_archetype_if_enabled(archetype_yaml: str, *, dashboard_uid: str = "") -> bool:
    """Compatibility guard: generated artifacts can never enter curated YAML."""
    if archetype_yaml:
        logger.warning("generated_archetype_curated_registration_blocked", uid=dashboard_uid)
    return False


def register_generated_archetypes_if_enabled(
    archetype_yamls: list[str],
    *,
    dashboard_uid: str = "bulk",
) -> bool:
    """Compatibility guard for the retired bulk curated-registration path."""
    if any(archetype_yamls):
        logger.warning("generated_archetype_bulk_curated_registration_blocked", uid=dashboard_uid)
    return False


def quarantine_generated_archetype_if_enabled(
    archetype_yaml: str,
    *,
    dashboard_uid: str = "",
    runtime_settings: Settings | None = None,
) -> list[str]:
    """Persist generated output only in the experimental quarantine namespace."""
    active_settings = runtime_settings or settings
    if (
        not bool(getattr(active_settings, "learned_archetypes_automatic_registration_enabled", False))
        or not archetype_yaml
    ):
        return []
    try:
        with _ARCHETYPE_QUARANTINE_LOCK:
            paths = quarantine_generated_archetype_yaml(
                archetype_yaml,
                Path(
                    getattr(
                        active_settings,
                        "learned_archetypes_quarantine_path",
                        "data/generated_archetypes/quarantine",
                    )
                ),
            )
        return [str(path) for path in paths]
    except Exception:
        logger.exception("generated_archetype_quarantine_failed", uid=dashboard_uid)
        return []


def quarantine_generated_archetypes_if_enabled(
    archetype_yamls: list[str],
    *,
    dashboard_uid: str = "bulk",
    runtime_settings: Settings | None = None,
) -> list[str]:
    """Quarantine a batch without combining it into a global registry document."""
    paths: list[str] = []
    for archetype_yaml in archetype_yamls:
        paths.extend(
            quarantine_generated_archetype_if_enabled(
                archetype_yaml,
                dashboard_uid=dashboard_uid,
                runtime_settings=runtime_settings,
            )
        )
    return paths


def _apply_dashboard_approval_generation(
    *,
    store: Any,
    knowledge_service: Any,
    dashboard_uid: str,
    backend_name: str | None,
    generation: float,
    tenant_id: str,
    runtime_settings: Settings,
) -> tuple[dict[str, Any], int, set[tuple[str, str]]]:
    """Promote and finalize one claimed dashboard generation in the active transaction."""
    ingested = store.get_ingested_dashboard(
        dashboard_uid,
        backend_name=backend_name,
        tenant_id=tenant_id,
    )
    if ingested is None or float(ingested["created_at"]) != generation:
        raise DashboardReviewConflictError("Dashboard was re-ingested during approval")
    status = str(ingested.get("status") or "pending")
    if status == "approved":
        active_pairs = _reconcile_dashboard_authority_for_state(
            store=store,
            ingested=ingested,
            tenant_id=tenant_id,
            runtime_settings=runtime_settings,
            knowledge_service=knowledge_service,
            max_candidate_count=int(runtime_settings.knowledge_source_atomic_candidate_limit),
        )
        return ingested, 0, active_pairs
    if status != "approving":
        raise DashboardReviewConflictError(f"Dashboard is already {status}")

    mappings_created = 0
    activated_pairs: set[tuple[str, str]] = set()
    governed_pairs: set[tuple[str, str]] = set()
    governed_candidate_ids: set[str] = set()
    source_ref = _dashboard_source_ref(ingested)
    source_fingerprint = _dashboard_content_fingerprint(ingested)
    for sig in ingested.get("signals_inferred", []):
        if isinstance(sig, dict):
            if persist_inferred_signal_review(
                store=store,
                sig=sig,
                source_ref=source_ref,
                dashboard_uid=dashboard_uid,
                backend_name=ingested.get("backend_name", ""),
                tenant_id=tenant_id,
                runtime_settings=runtime_settings,
                source_fingerprint=source_fingerprint,
                governed_candidate_ids=governed_candidate_ids,
                governed_pairs=governed_pairs,
                knowledge_service=knowledge_service,
            ):
                mappings_created += 1
                activated_pairs.add((sig.get("metric", ""), sig.get("signal_type", "")))
            continue

        from tacit.signals import _metric_matches_pattern

        signal_data = store.get_signal_type_page(
            sig,
            tenant_id=tenant_id,
            limit=_LEGACY_MAPPING_EXPANSION_LIMIT,
        )
        if not signal_data:
            continue
        if signal_data["has_more"]:
            raise DashboardReviewConflictError(
                f"Legacy signal '{sig}' exceeds the {_LEGACY_MAPPING_EXPANSION_LIMIT}-mapping "
                "approval limit; re-ingest the dashboard with explicit inferred mappings"
            )
        for metric in ingested.get("metrics_found", []):
            for mapping in signal_data.get("mappings", []):
                if not _metric_matches_pattern(metric, mapping["metric_pattern"]):
                    continue
                if persist_inferred_signal_review(
                    store=store,
                    sig={
                        "signal_type": sig,
                        "metric": metric,
                        "confidence": mapping.get("confidence", 0.6),
                        "source": "reviewed_mapping",
                        "services": [],
                    },
                    source_ref=source_ref,
                    dashboard_uid=dashboard_uid,
                    backend_name=ingested.get("backend_name", ""),
                    tenant_id=tenant_id,
                    runtime_settings=runtime_settings,
                    source_fingerprint=source_fingerprint,
                    governed_candidate_ids=governed_candidate_ids,
                    governed_pairs=governed_pairs,
                    knowledge_service=knowledge_service,
                ):
                    mappings_created += 1
                    activated_pairs.add((metric, sig))
                break

    reconcile_signal_source(
        store=store,
        tenant_id=tenant_id,
        source_type="dashboard_ingest",
        source_ref=source_ref,
        active_pairs=governed_pairs,
        active_candidate_ids=governed_candidate_ids,
        runtime_settings=runtime_settings,
        knowledge_service=knowledge_service,
        max_candidate_count=int(runtime_settings.knowledge_source_atomic_candidate_limit),
    )
    finalized = store.finalize_ingested_dashboard_approval(
        dashboard_uid,
        backend_name=backend_name,
        activated_pairs=activated_pairs,
        expected_generation=generation,
        tenant_id=tenant_id,
    )
    if not finalized:
        raise DashboardReviewConflictError("Dashboard changed before approval finalized")
    return ingested, mappings_created, activated_pairs


def approve_ingested_dashboard_record(
    *,
    dashboard_uid: str,
    backend_name: str | None = None,
    store: Any | None = None,
    runtime_settings: Settings | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
    quarantine_archetype: bool = True,
    include_activated_pairs: bool = False,
    context_indexer: Callable[[set[tuple[str, str]]], int] | None = None,
) -> dict[str, Any]:
    """Recoverably approve one claimed dashboard generation."""
    active_settings, effective_tenant, store = _authorized_signal_store(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
        actions=(KnowledgeAction.READ, KnowledgeAction.TEACH_SIGNALS, KnowledgeAction.APPLY),
    )
    ingested = store.get_ingested_dashboard(
        dashboard_uid,
        backend_name=backend_name,
        tenant_id=effective_tenant,
    )
    if ingested is None:
        raise LookupError("Ingested dashboard not found")

    knowledge_service = knowledge_service or _knowledge_service_for_store(
        store,
        runtime_settings=active_settings,
    )
    status = str(ingested.get("status") or "pending")
    generation = float(ingested["created_at"])
    if status == "approved":
        source_ref = _dashboard_source_ref(ingested)
        indexed_context_rows = 0
        with _source_authority_transaction(
            store=store,
            knowledge_service=knowledge_service,
            tenant_id=effective_tenant,
            source_ref=source_ref,
            operation="recover_approval",
        ):
            ingested, _mappings_created, active_pairs = _apply_dashboard_approval_generation(
                store=store,
                knowledge_service=knowledge_service,
                dashboard_uid=dashboard_uid,
                backend_name=backend_name,
                generation=generation,
                tenant_id=effective_tenant,
                runtime_settings=active_settings,
            )
            if context_indexer is not None:
                indexed_context_rows = context_indexer(active_pairs)
        result = {
            "dashboard_uid": dashboard_uid,
            "backend_name": ingested.get("backend_name", ""),
            "status": "approved",
            "mappings_created": 0,
            "archetype_registered": False,
            "archetype_quarantined": False,
            "message": "Dashboard already approved",
            "indexed_context_rows": indexed_context_rows,
        }
        if include_activated_pairs:
            result["activated_pairs"] = sorted(active_pairs)
        return result
    if status not in {"pending", "approving"}:
        _reconcile_dashboard_after_approval_loss(
            store=store,
            ingested=ingested,
            tenant_id=effective_tenant,
            runtime_settings=active_settings,
            knowledge_service=knowledge_service,
        )
        raise DashboardReviewConflictError(f"Dashboard is already {status}")
    if status == "pending" and not store.claim_ingested_dashboard_approval(
        dashboard_uid,
        backend_name=backend_name,
        expected_generation=generation,
        tenant_id=effective_tenant,
    ):
        current = store.get_ingested_dashboard(
            dashboard_uid,
            backend_name=backend_name,
            tenant_id=effective_tenant,
        )
        if current is not None and float(current["created_at"]) == generation:
            current_status = str(current.get("status") or "")
            if current_status == "approved":
                source_ref = _dashboard_source_ref(current)
                indexed_context_rows = 0
                with _source_authority_transaction(
                    store=store,
                    knowledge_service=knowledge_service,
                    tenant_id=effective_tenant,
                    source_ref=source_ref,
                    operation="recover_approval",
                ):
                    current, _mappings_created, active_pairs = _apply_dashboard_approval_generation(
                        store=store,
                        knowledge_service=knowledge_service,
                        dashboard_uid=dashboard_uid,
                        backend_name=backend_name,
                        generation=generation,
                        tenant_id=effective_tenant,
                        runtime_settings=active_settings,
                    )
                    if context_indexer is not None:
                        indexed_context_rows = context_indexer(active_pairs)
                result = {
                    "dashboard_uid": dashboard_uid,
                    "backend_name": current.get("backend_name", ""),
                    "status": "approved",
                    "mappings_created": 0,
                    "archetype_registered": False,
                    "archetype_quarantined": False,
                    "message": "Dashboard already approved",
                    "indexed_context_rows": indexed_context_rows,
                }
                if include_activated_pairs:
                    result["activated_pairs"] = sorted(active_pairs)
                return result
            if current_status == "approving":
                ingested = current
            else:
                _reconcile_dashboard_after_approval_loss(
                    store=store,
                    ingested=ingested,
                    tenant_id=effective_tenant,
                    runtime_settings=active_settings,
                    knowledge_service=knowledge_service,
                )
                raise DashboardReviewConflictError("Dashboard approval claim was lost")
        else:
            _reconcile_dashboard_after_approval_loss(
                store=store,
                ingested=ingested,
                tenant_id=effective_tenant,
                runtime_settings=active_settings,
                knowledge_service=knowledge_service,
            )
            raise DashboardReviewConflictError("Dashboard was re-ingested during approval")

    source_ref = _dashboard_source_ref(ingested)
    indexed_context_rows = 0
    with _source_authority_transaction(
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=effective_tenant,
        source_ref=source_ref,
        operation="approve",
    ):
        ingested, mappings_created, activated_pairs = _apply_dashboard_approval_generation(
            store=store,
            knowledge_service=knowledge_service,
            dashboard_uid=dashboard_uid,
            backend_name=backend_name,
            generation=generation,
            tenant_id=effective_tenant,
            runtime_settings=active_settings,
        )
        if context_indexer is not None:
            indexed_context_rows = context_indexer(activated_pairs)

    quarantine_paths = (
        quarantine_generated_archetype_if_enabled(
            ingested.get("archetype_generated", ""),
            dashboard_uid=dashboard_uid,
            runtime_settings=active_settings,
        )
        if quarantine_archetype
        else []
    )

    result = {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "approved",
        "mappings_created": mappings_created,
        "archetype_registered": False,
        "archetype_quarantined": bool(quarantine_paths),
        "archetype_quarantine_paths": quarantine_paths,
        "message": f"Dashboard approved, {mappings_created} signal mapping(s) created",
        "indexed_context_rows": indexed_context_rows,
    }
    if include_activated_pairs:
        result["activated_pairs"] = sorted(activated_pairs)
    return result


def reject_ingested_dashboard_record(
    *,
    dashboard_uid: str,
    backend_name: str | None = None,
    store: Any | None = None,
    runtime_settings: Settings | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, Any]:
    """Reject a dashboard and retire all authority supported by that source."""
    active_settings, effective_tenant, store = _authorized_signal_store(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
        actions=(KnowledgeAction.READ, KnowledgeAction.REJECT, KnowledgeAction.APPLY),
    )
    knowledge_service = knowledge_service or _knowledge_service_for_store(
        store,
        runtime_settings=active_settings,
    )
    rejected_candidates = 0
    source_ref = f"{backend_name}:{dashboard_uid}" if backend_name else dashboard_uid
    with _source_authority_transaction(
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=effective_tenant,
        source_ref=source_ref,
        operation="reject",
    ):
        ingested = store.get_ingested_dashboard(
            dashboard_uid,
            backend_name=backend_name,
            tenant_id=effective_tenant,
        )
        if ingested is None:
            raise LookupError("Ingested dashboard not found")

        current_status = str(ingested.get("status") or "pending")
        if current_status == "rejected":
            transitioned = False
        elif current_status in {"pending", "approved"}:
            transitioned = store.reject_ingested_dashboard(
                dashboard_uid,
                backend_name=backend_name,
                tenant_id=effective_tenant,
            )
            if not transitioned:
                raise DashboardReviewConflictError("Dashboard review state changed during rejection")
        else:
            raise DashboardReviewConflictError(f"Dashboard is already {current_status}")

        if transitioned:
            for sig in ingested.get("signals_inferred", []):
                if isinstance(sig, dict) and sig.get("source") == "heuristic" and sig.get("metric"):
                    store.record_rejected_candidate(
                        metric=sig["metric"],
                        signal_family=sig.get("signal_family", ""),
                        signal_name=sig.get("signal_type", ""),
                        score=sig.get("score", 0.0),
                        margin=sig.get("margin", 0.0),
                        why_not="dashboard_rejected",
                        evidence=sig.get("evidence", []),
                        inference_version=sig.get("inference_version", ""),
                        dashboard_uid=dashboard_uid,
                        backend_name=ingested.get("backend_name", ""),
                        tenant_id=effective_tenant,
                    )
                    rejected_candidates += 1
        current = store.get_ingested_dashboard(
            dashboard_uid,
            backend_name=backend_name,
            tenant_id=effective_tenant,
        )
        assert current is not None
        _reconcile_dashboard_authority_for_state(
            store=store,
            ingested=current,
            tenant_id=effective_tenant,
            runtime_settings=active_settings,
            knowledge_service=knowledge_service,
            max_candidate_count=int(active_settings.knowledge_source_atomic_candidate_limit),
        )

    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "rejected",
        "rejected_candidates": rejected_candidates,
        "message": "Dashboard rejected; no mappings created",
    }


def ignore_ingested_dashboard_record(
    *,
    dashboard_uid: str,
    backend_name: str | None = None,
    store: Any | None = None,
    runtime_settings: Settings | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, Any]:
    """Ignore a dashboard and retire all authority supported by that source."""
    active_settings, effective_tenant, store = _authorized_signal_store(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
        actions=(KnowledgeAction.READ, KnowledgeAction.REJECT, KnowledgeAction.APPLY),
    )
    knowledge_service = knowledge_service or _knowledge_service_for_store(
        store,
        runtime_settings=active_settings,
    )
    source_ref = f"{backend_name}:{dashboard_uid}" if backend_name else dashboard_uid
    with _source_authority_transaction(
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=effective_tenant,
        source_ref=source_ref,
        operation="ignore",
    ):
        ingested = store.get_ingested_dashboard(
            dashboard_uid,
            backend_name=backend_name,
            tenant_id=effective_tenant,
        )
        if ingested is None:
            raise LookupError("Ingested dashboard not found")
        current_status = str(ingested.get("status") or "pending")
        if current_status == "ignored":
            transitioned = False
        elif current_status in {"pending", "approved"}:
            transitioned = store.ignore_ingested_dashboard(
                dashboard_uid,
                backend_name=backend_name,
                tenant_id=effective_tenant,
            )
            if not transitioned:
                raise DashboardReviewConflictError("Dashboard review state changed while ignoring")
        else:
            raise DashboardReviewConflictError(f"Dashboard is already {current_status}")
        current = store.get_ingested_dashboard(
            dashboard_uid,
            backend_name=backend_name,
            tenant_id=effective_tenant,
        )
        assert current is not None
        _reconcile_dashboard_authority_for_state(
            store=store,
            ingested=current,
            tenant_id=effective_tenant,
            runtime_settings=active_settings,
            knowledge_service=knowledge_service,
            max_candidate_count=int(active_settings.knowledge_source_atomic_candidate_limit),
        )
    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "ignored",
        "message": "Dashboard ignored; no mappings created",
    }


async def ingest_dashboard_features(
    features: Any,
    *,
    auto_approve: bool = False,
    register_archetype: bool = True,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, Any]:
    """Infer, persist, and optionally approve already-extracted dashboard features."""
    actions = [KnowledgeAction.READ]
    if auto_approve:
        actions.append(KnowledgeAction.TEACH_SIGNALS)
    actions.append(KnowledgeAction.APPLY)
    active_settings, effective_tenant, store = _authorized_signal_store(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
        actions=tuple(actions),
    )
    extracted = _features_to_dict(features)

    signals = infer_signals_from_metrics(
        features.metrics_found,
        features.panels,
        store=store,
        tenant_id=effective_tenant,
    )
    signal_quality = build_signal_quality_report(metrics=features.metrics_found, signals=signals)

    source_ref = (
        f"{features.backend_name}:{features.dashboard_uid}" if features.backend_name else features.dashboard_uid
    )
    archetype_yaml = ""
    generation_enabled = bool(getattr(active_settings, "learned_archetypes_generation_enabled", False))
    if generation_enabled:
        archetype_yaml = generate_archetype_yaml(
            extracted,
            signals,
            tenant_id=effective_tenant,
            generation_version=getattr(
                active_settings,
                "learned_archetypes_generation_version",
                "generated-archetype-v1",
            ),
            generation_run_id=f"dashboard_ingest:{source_ref}",
            source_refs=[source_ref],
        )

    knowledge_service = knowledge_service or _knowledge_service_for_store(
        store,
        runtime_settings=active_settings,
    )

    mappings_created = 0
    quarantine_paths: list[str] = []
    activated_pairs: set[tuple[str, str]] = set()
    if auto_approve:
        with _source_authority_transaction(
            store=store,
            knowledge_service=knowledge_service,
            tenant_id=effective_tenant,
            source_ref=source_ref,
            operation="prepare_auto_approved_dashboard",
        ):
            stored_dashboard = _record_dashboard_generation(
                store=store,
                features=features,
                signals=signals,
                archetype_yaml=archetype_yaml,
                tenant_id=effective_tenant,
            )
            prepared_status = str(stored_dashboard.get("status") or "pending")
            prepared_pairs = _reconcile_dashboard_authority_for_state(
                store=store,
                ingested=stored_dashboard,
                tenant_id=effective_tenant,
                runtime_settings=active_settings,
                knowledge_service=knowledge_service,
                max_candidate_count=int(active_settings.knowledge_source_atomic_candidate_limit),
            )
            _index_dashboard_generation(
                store=store,
                features=features,
                signals=signals,
                tenant_id=effective_tenant,
                status=prepared_status,
                activated_pairs=prepared_pairs,
                strict=True,
            )
        approval = approve_ingested_dashboard_record(
            dashboard_uid=features.dashboard_uid,
            backend_name=features.backend_name,
            store=store,
            runtime_settings=active_settings,
            tenant_id=effective_tenant,
            knowledge_service=knowledge_service,
            quarantine_archetype=register_archetype,
            include_activated_pairs=True,
            context_indexer=lambda active_pairs: _index_dashboard_generation(
                store=store,
                features=features,
                signals=signals,
                tenant_id=effective_tenant,
                status="approved",
                activated_pairs=active_pairs,
                strict=True,
            ),
        )
        effective_status = str(approval["status"])
        mappings_created = int(approval["mappings_created"])
        activated_pairs = {tuple(pair) for pair in approval.get("activated_pairs", [])}
        quarantine_paths = list(approval.get("archetype_quarantine_paths", []))
        indexed_context_rows = int(approval.get("indexed_context_rows", 0))
        logger.info(
            "dashboard_ingested_auto_approved",
            uid=features.dashboard_uid,
            backend=features.backend_name,
            metrics=len(features.metrics_found),
            signals=len(signals),
            mappings_created=mappings_created,
            archetype_registered=False,
            archetype_quarantined=bool(quarantine_paths),
        )
    else:
        with _source_authority_transaction(
            store=store,
            knowledge_service=knowledge_service,
            tenant_id=effective_tenant,
            source_ref=source_ref,
            operation="reconcile_pending_dashboard",
        ):
            stored_dashboard = _record_dashboard_generation(
                store=store,
                features=features,
                signals=signals,
                archetype_yaml=archetype_yaml,
                tenant_id=effective_tenant,
            )
            effective_status = str(stored_dashboard.get("status") or "pending")
            activated_pairs = _reconcile_dashboard_authority_for_state(
                store=store,
                ingested=stored_dashboard,
                tenant_id=effective_tenant,
                runtime_settings=active_settings,
                knowledge_service=knowledge_service,
                max_candidate_count=int(active_settings.knowledge_source_atomic_candidate_limit),
            )
            indexed_context_rows = _index_dashboard_generation(
                store=store,
                features=features,
                signals=signals,
                tenant_id=effective_tenant,
                status=effective_status,
                activated_pairs=activated_pairs,
                strict=True,
            )
        quarantine_paths = (
            quarantine_generated_archetype_if_enabled(
                archetype_yaml,
                dashboard_uid=features.dashboard_uid,
                runtime_settings=active_settings,
            )
            if register_archetype
            else []
        )
        logger.info(
            "dashboard_ingested",
            uid=features.dashboard_uid,
            backend=features.backend_name,
            status=effective_status,
            metrics=len(features.metrics_found),
            signals=len(signals),
        )

    learning_impact = build_learning_impact_report(
        metrics=features.metrics_found,
        signals=signals,
        approved=effective_status == "approved",
    )
    if auto_approve:
        teachable_count = len(_governable_signal_pairs(signals))
        learning_impact["candidate_mappings_pending_approval"] = max(0, teachable_count - mappings_created)
        learning_impact["new_active_mappings_after_approval"] = mappings_created

    result = {
        "dashboard_uid": features.dashboard_uid,
        "dashboard_title": features.dashboard_title,
        "backend": features.backend_name,
        "query_language": features.query_language,
        "status": effective_status,
        "metrics_found": features.metrics_found,
        "panel_count": features.panel_count,
        "row_groups": features.row_groups,
        "metric_cooccurrence": features.metric_cooccurrence,
        "aggregation_patterns": features.aggregation_patterns,
        "panel_titles": features.panel_titles,
        "alert_links": features.alert_links,
        "drilldown_links": features.drilldown_links,
        "signals_inferred": signals,
        "signal_quality": signal_quality,
        "learning_impact": learning_impact,
        "indexed_context_rows": indexed_context_rows,
        "archetype_yaml": archetype_yaml,
        "archetype_generation_enabled": generation_enabled,
        "archetype_registered": False,
        "archetype_quarantined": bool(quarantine_paths),
        "archetype_quarantine_paths": quarantine_paths,
    }
    if auto_approve:
        result["mappings_created"] = mappings_created
    return result


async def ingest_dashboard(
    dashboard_uid: str,
    backend: Any | None = None,
    backend_name: str = "",
    auto_approve: bool = False,
    register_archetype: bool = True,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
    knowledge_service: Any | None = None,
) -> dict[str, Any]:
    """Full ingestion pipeline: fetch → extract → infer signals → store.

    Vendor-agnostic: delegates to the ``DashboardBackend.ingest_dashboard()``
    method, which handles vendor-specific fetch + parse.  The signal inference
    and archetype generation work against the common ``DashboardFeatures``
    dataclass.

    Parameters
    ----------
    dashboard_uid : str
        Dashboard UID/ID to ingest (interpretation is backend-specific).
    backend : DashboardBackend, optional
        Explicit backend to use. If not provided, iterates over all active
        backends and uses the first one that matches ``backend_name``, or the
        first available backend.
    backend_name : str
        If provided without an explicit ``backend``, selects the backend by
        name (e.g. 'grafana', 'signalfx').
    auto_approve : bool
        If True, request automated review for eligible signal mappings only.
        Governance determines activation, and generated archetypes remain
        quarantined. If False (default), stores as 'pending' for human review.

    Returns
    -------
    dict with extracted features, inferred signals, optional quarantined
    archetype-candidate YAML, and status.
    """
    from tacit.backends import get_active_backends
    from tacit.backends.base import DashboardFeatures

    actions = [KnowledgeAction.READ]
    if auto_approve:
        actions.append(KnowledgeAction.TEACH_SIGNALS)
    actions.append(KnowledgeAction.APPLY)
    active_settings, effective_tenant, store = _authorized_signal_store(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=knowledge_service,
        tenant_id=tenant_id,
        actions=tuple(actions),
    )
    all_backends: list[Any] = []
    own_backends = False
    if backend is None:
        all_backends = _build_active_backends(get_active_backends, active_settings)
        own_backends = True
        if not all_backends:
            raise RuntimeError("No active backends configured for dashboard ingestion")

        if backend_name:
            matched = [b for b in all_backends if b.name == backend_name]
            if not matched:
                available = [b.name for b in all_backends]
                # Close all backends before raising
                for b in all_backends:
                    await b.close()
                raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")
            backend = matched[0]
        else:
            backend = all_backends[0]

    try:
        # Delegate fetch + parse to the backend (vendor-specific)
        features: DashboardFeatures = await backend.ingest_dashboard(dashboard_uid)

        return await ingest_dashboard_features(
            features,
            auto_approve=auto_approve,
            register_archetype=register_archetype,
            runtime_settings=active_settings,
            store=store,
            tenant_id=effective_tenant,
            knowledge_service=knowledge_service,
        )

    finally:
        if own_backends:
            for b in all_backends:
                await b.close()


async def learn_backend_dashboards(
    backend_name: str,
    *,
    auto_approve: bool = False,
    limit: int = 500,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Crawl a backend and learn from every discoverable dashboard."""
    import asyncio

    from tacit.backends import get_active_backends

    actions = [KnowledgeAction.READ]
    if auto_approve:
        actions.append(KnowledgeAction.TEACH_SIGNALS)
    actions.append(KnowledgeAction.APPLY)
    active_settings, effective_tenant, store = _authorized_signal_store(
        runtime_settings=runtime_settings,
        store=store,
        knowledge_service=None,
        tenant_id=tenant_id,
        actions=tuple(actions),
    )
    knowledge_service = _knowledge_service_for_store(store, runtime_settings=active_settings)
    all_backends = _build_active_backends(get_active_backends, active_settings)
    if not all_backends:
        raise RuntimeError("No active backends configured for dashboard learning")

    try:
        matched = [b for b in all_backends if b.name == backend_name]
        if not matched:
            available = [b.name for b in all_backends]
            raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")
        backend = matched[0]
        crawl_started_at = time.time()
        dashboards = await backend.list_dashboards(limit=limit)

        learned: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        totals = {
            "dashboards_discovered": len(dashboards),
            "dashboards_learned": 0,
            "dashboards_failed": 0,
            "metrics_found": 0,
            "signals_inferred": 0,
            "indexed_context_rows": 0,
            "mappings_created": 0,
        }

        sem = asyncio.Semaphore(max(1, active_settings.adapter_max_concurrent))

        async def learn_one(item: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
            uid = item.get("uid", "")
            if not uid:
                return None, None
            try:
                async with sem:
                    result = await ingest_dashboard(
                        uid,
                        backend=backend,
                        auto_approve=auto_approve,
                        register_archetype=True,
                        runtime_settings=active_settings,
                        store=store,
                        tenant_id=effective_tenant,
                        knowledge_service=knowledge_service,
                    )
                return (
                    {
                        "dashboard_uid": result.get("dashboard_uid", uid),
                        "dashboard_title": result.get("dashboard_title", item.get("title", "")),
                        "status": result.get("status", "pending"),
                        "metrics_found": len(result.get("metrics_found", [])),
                        "signals_inferred": len(result.get("signals_inferred", [])),
                        "indexed_context_rows": result.get("indexed_context_rows", 0),
                        "mappings_created": result.get("mappings_created", 0),
                        "archetype_registered": False,
                        "archetype_quarantined": result.get("archetype_quarantined", False),
                        "archetype_quarantine_paths": result.get("archetype_quarantine_paths", []),
                        "archetype_yaml": result.get("archetype_yaml", ""),
                    },
                    None,
                )
            except Exception as exc:
                return None, {"dashboard_uid": uid, "title": item.get("title", ""), "error": str(exc)}

        results = await asyncio.gather(*(learn_one(item) for item in dashboards))
        for learned_item, failure in results:
            if learned_item is not None:
                learned.append(learned_item)
                totals["dashboards_learned"] += 1
                totals["metrics_found"] += int(learned_item.get("metrics_found", 0) or 0)
                totals["signals_inferred"] += int(learned_item.get("signals_inferred", 0) or 0)
                totals["indexed_context_rows"] += int(learned_item.get("indexed_context_rows", 0) or 0)
                totals["mappings_created"] += int(learned_item.get("mappings_created", 0) or 0)
            if failure is not None:
                failures.append(failure)
                totals["dashboards_failed"] += 1

        stale_reconciliation_complete = bool(getattr(backend, "last_dashboard_list_complete", False))
        if stale_reconciliation_complete:
            seen_dashboard_uids = {str(item.get("uid", "")) for item in dashboards if item.get("uid")}
            store.ensure_governed_projection_audit_current()
            bind_connection = knowledge_service.repository.bind_transaction_connection

            def reconcile_stale_dashboard_authority(conn, dashboard):
                if not store.governed_projection_audit_is_current(conn):
                    raise DashboardReviewConflictError(
                        "governed signal projection changed before stale dashboard reconciliation; retry"
                    )

                def source_generation_guard(guard_conn):
                    return store.dashboard_stale_generation_is_current(
                        guard_conn,
                        tenant_id=effective_tenant,
                        backend_name=backend_name,
                        dashboard_uid=str(dashboard["dashboard_uid"]),
                        missing_since=dashboard["missing_since"],
                    )

                with bind_connection(conn):
                    knowledge_service.reconcile_source_lifecycle(
                        provenance_ref=(
                            f"{backend_name}:{dashboard['dashboard_uid']}"
                            if backend_name
                            else str(dashboard["dashboard_uid"])
                        ),
                        tenant_id=effective_tenant,
                        source_stale=True,
                        source_generation_guard=source_generation_guard,
                    )

            stale_started_at = time.monotonic()
            totals["stale_marked"] = store.mark_missing_dashboards_stale(
                tenant_id=effective_tenant,
                backend_name=backend_name,
                seen_dashboard_uids=seen_dashboard_uids,
                crawl_started_at=crawl_started_at,
                authority_reconciler=reconcile_stale_dashboard_authority,
            )
            totals["stale_reconciliation_failures"] = 0
            logger.info(
                "stale_dashboard_knowledge_reconciled",
                tenant_id=effective_tenant,
                backend_name=backend_name,
                stale_marked=totals["stale_marked"],
                records_reconciled=totals["stale_marked"],
                reconciliation_failures=0,
                duration_ms=round((time.monotonic() - stale_started_at) * 1000, 2),
            )
        else:
            totals["stale_reconciliation_skipped"] = True

        totals["archetypes_registered"] = 0
        totals["archetypes_quarantined"] = sum(bool(item.get("archetype_quarantined")) for item in learned)

        return {
            "backend": backend_name,
            "auto_approve": auto_approve,
            **totals,
            "learned": learned,
            "failures": failures,
        }
    finally:
        for backend in all_backends:
            await backend.close()
