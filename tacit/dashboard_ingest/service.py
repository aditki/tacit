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

import threading
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
from tacit.signals import get_signal_store as _default_get_signal_store

logger = structlog.get_logger()
_ARCHETYPE_QUARANTINE_LOCK = threading.Lock()


def get_signal_store():
    """Resolve the signal store through the package façade for test isolation."""
    import tacit.dashboard_ingest as dashboard_ingest_pkg

    package_getter = getattr(dashboard_ingest_pkg, "get_signal_store", _default_get_signal_store)
    if package_getter is get_signal_store:
        return _default_get_signal_store()
    return package_getter()


# ── Signal inference ─────────────────────────────────────────────────────────


def infer_signals_from_metrics(
    metrics: list[str],
    panel_data: list[dict[str, Any]] | None = None,
    *,
    store: Any | None = None,
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
    all_signal_types = store.list_signal_types()
    inferred: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    matched_metrics: set[str] = set()

    # 1. Curated taxonomy matches.
    for metric in metrics:
        for st in all_signal_types:
            signal_type = st["signal_type"]
            mappings = store.get_mappings_for_signal(signal_type)
            for mapping in mappings:
                from tacit.signals import _metric_matches_pattern

                if _metric_matches_pattern(metric, mapping["metric_pattern"]):
                    key = (signal_type, metric)
                    if key not in seen:
                        seen.add(key)
                        matched_metrics.add(metric)
                        inferred.append(
                            {
                                "signal_type": signal_type,
                                "metric": metric,
                                "confidence": mapping.get("effective_confidence", mapping["confidence"]),
                                "signal_family": st.get("category", ""),
                                "source": "taxonomy",
                                "reason": f"matches pattern '{mapping['metric_pattern']}'",
                                "evidence": [f"matches taught pattern '{mapping['metric_pattern']}'"],
                            }
                        )

    # 2. Heuristic fallback for metrics the taxonomy didn't recognize.
    from tacit.signal_inference import INFERENCE_VERSION
    from tacit.signal_inference import infer_signals as _infer_heuristic

    unmatched = [m for m in dict.fromkeys(metrics) if m not in matched_metrics]
    for sig in _infer_heuristic(unmatched, panel_data or []):
        signal_type = _canonical_signal_type_for_heuristic(sig)
        signal_family = "saturation" if signal_type == "db_connection_pool" else sig.signal_family
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
            }
        )

    inferred.sort(key=lambda x: x["confidence"], reverse=True)
    return inferred


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
) -> bool:
    """Persist one inferred signal using the same gate for all approval paths."""
    signal_type = sig["signal_type"]
    metric = sig.get("metric", "")
    confidence = sig.get("confidence", 0.6)
    is_heuristic = sig.get("source") == "heuristic"

    if is_heuristic:
        should_teach = bool(metric) and bool(sig.get("auto_teach_eligible"))
    else:
        should_teach = bool(metric) and confidence >= 0.5

    if should_teach:
        family = sig.get("signal_family", "")
        if family:
            store.register_signal_type(signal_type=signal_type, category=family)
        store.add_mapping(
            signal_type=signal_type,
            metric_pattern=metric,
            confidence=confidence,
            source_type="dashboard_ingest",
            source_refs=[source_ref],
            inference_version=sig.get("inference_version", ""),
            review_state="approved" if is_heuristic else "trusted",
        )
        return True

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
        )
    return False


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


def approve_ingested_dashboard_record(
    *,
    dashboard_uid: str,
    backend_name: str | None = None,
    store: Any | None = None,
    runtime_settings: Settings | None = None,
) -> dict[str, Any]:
    """Approve a pending ingested dashboard and activate learned artifacts."""
    store = store or get_signal_store()
    ingested = store.get_ingested_dashboard(dashboard_uid, backend_name=backend_name)
    if ingested is None:
        raise LookupError("Ingested dashboard not found")

    if ingested["status"] != "pending":
        return {
            "dashboard_uid": dashboard_uid,
            "backend_name": ingested.get("backend_name", ""),
            "status": ingested["status"],
            "mappings_created": 0,
            "archetype_registered": False,
            "archetype_quarantined": False,
            "message": f"Dashboard already {ingested['status']}",
        }

    mappings_created = 0
    activated_pairs: set[tuple[str, str]] = set()
    source_ref = f"{ingested['backend_name']}:{dashboard_uid}" if ingested.get("backend_name") else dashboard_uid
    for sig in ingested.get("signals_inferred", []):
        if isinstance(sig, dict):
            if persist_inferred_signal_review(
                store=store,
                sig=sig,
                source_ref=source_ref,
                dashboard_uid=dashboard_uid,
                backend_name=ingested.get("backend_name", ""),
            ):
                mappings_created += 1
                activated_pairs.add((sig.get("metric", ""), sig.get("signal_type", "")))
        else:
            from tacit.signals import _metric_matches_pattern

            signal_data = store.get_signal_type(sig)
            if not signal_data:
                continue
            for metric in ingested.get("metrics_found", []):
                for mapping in signal_data.get("mappings", []):
                    if _metric_matches_pattern(metric, mapping["metric_pattern"]):
                        store.add_mapping(
                            signal_type=sig,
                            metric_pattern=metric,
                            confidence=mapping.get("confidence", 0.6),
                            source_type="dashboard_ingest",
                            source_refs=[source_ref],
                            review_state="approved",
                        )
                        mappings_created += 1
                        activated_pairs.add((metric, sig))
                        break

    store.approve_ingested_dashboard(
        dashboard_uid,
        backend_name=backend_name,
        activated_pairs=activated_pairs,
    )
    quarantine_paths = quarantine_generated_archetype_if_enabled(
        ingested.get("archetype_generated", ""),
        dashboard_uid=dashboard_uid,
        runtime_settings=runtime_settings,
    )

    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "approved",
        "mappings_created": mappings_created,
        "archetype_registered": False,
        "archetype_quarantined": bool(quarantine_paths),
        "archetype_quarantine_paths": quarantine_paths,
        "message": f"Dashboard approved, {mappings_created} signal mapping(s) created",
    }


def reject_ingested_dashboard_record(
    *,
    dashboard_uid: str,
    backend_name: str | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    """Reject a pending ingested dashboard and persist heuristic negatives."""
    store = store or get_signal_store()
    ingested = store.get_ingested_dashboard(dashboard_uid, backend_name=backend_name)
    if ingested is None:
        raise LookupError("Ingested dashboard not found")

    if ingested["status"] != "pending":
        return {
            "dashboard_uid": dashboard_uid,
            "backend_name": ingested.get("backend_name", ""),
            "status": ingested["status"],
            "rejected_candidates": 0,
            "message": f"Dashboard already {ingested['status']}",
        }

    rejected_candidates = 0
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
            )
            rejected_candidates += 1

    if not store.reject_ingested_dashboard(dashboard_uid, backend_name=backend_name):
        raise RuntimeError("Dashboard is no longer pending")

    return {
        "dashboard_uid": dashboard_uid,
        "backend_name": ingested.get("backend_name", ""),
        "status": "rejected",
        "rejected_candidates": rejected_candidates,
        "message": "Dashboard rejected; no mappings created",
    }


async def ingest_dashboard_features(
    features: Any,
    *,
    auto_approve: bool = False,
    register_archetype: bool = True,
    runtime_settings: Settings | None = None,
    store: Any | None = None,
) -> dict[str, Any]:
    """Infer, persist, and optionally approve already-extracted dashboard features."""
    active_settings = runtime_settings or settings
    extracted = _features_to_dict(features)

    signals = infer_signals_from_metrics(
        features.metrics_found,
        features.panels,
        store=store,
    )
    signal_quality = build_signal_quality_report(metrics=features.metrics_found, signals=signals)
    learning_impact = build_learning_impact_report(
        metrics=features.metrics_found,
        signals=signals,
        approved=auto_approve,
    )

    source_ref = (
        f"{features.backend_name}:{features.dashboard_uid}" if features.backend_name else features.dashboard_uid
    )
    archetype_yaml = ""
    generation_enabled = bool(getattr(active_settings, "learned_archetypes_generation_enabled", False))
    if generation_enabled:
        archetype_yaml = generate_archetype_yaml(
            extracted,
            signals,
            tenant_id=getattr(active_settings, "learned_archetypes_tenant_id", "default"),
            generation_version=getattr(
                active_settings,
                "learned_archetypes_generation_version",
                "generated-archetype-v1",
            ),
            generation_run_id=f"dashboard_ingest:{source_ref}",
            source_refs=[source_ref],
        )

    store = store or get_signal_store()
    status = "approved" if auto_approve else "pending"

    store.record_ingested_dashboard(
        dashboard_uid=features.dashboard_uid,
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
        status=status,
    )
    mappings_created = 0
    quarantine_paths = (
        quarantine_generated_archetype_if_enabled(
            archetype_yaml,
            dashboard_uid=features.dashboard_uid,
            runtime_settings=active_settings,
        )
        if register_archetype
        else []
    )
    activated_pairs: set[tuple[str, str]] = set()
    if auto_approve:
        for sig in signals:
            if persist_inferred_signal_review(
                store=store,
                sig=sig,
                source_ref=source_ref,
                dashboard_uid=features.dashboard_uid,
                backend_name=features.backend_name,
            ):
                mappings_created += 1
                activated_pairs.add((sig.get("metric", ""), sig.get("signal_type", "")))
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
        logger.info(
            "dashboard_ingested_pending",
            uid=features.dashboard_uid,
            backend=features.backend_name,
            metrics=len(features.metrics_found),
            signals=len(signals),
        )

    indexed_context_rows = store.index_dashboard_context(
        dashboard_uid=features.dashboard_uid,
        backend_name=features.backend_name,
        dashboard_title=features.dashboard_title,
        dashboard_tags=features.dashboard_tags,
        panels=features.panels,
        metrics_found=features.metrics_found,
        signals_inferred=signals,
        status=status,
        activated_pairs=activated_pairs if auto_approve else None,
    )

    result = {
        "dashboard_uid": features.dashboard_uid,
        "dashboard_title": features.dashboard_title,
        "backend": features.backend_name,
        "query_language": features.query_language,
        "status": status,
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

    all_backends: list[Any] = []
    own_backends = False
    if backend is None:
        all_backends = get_active_backends(runtime_settings) if runtime_settings is not None else get_active_backends()
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
            runtime_settings=runtime_settings,
            store=store,
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
) -> dict[str, Any]:
    """Crawl a backend and learn from every discoverable dashboard."""
    import asyncio

    from tacit.backends import get_active_backends

    active_settings = runtime_settings or settings
    all_backends = get_active_backends(runtime_settings) if runtime_settings is not None else get_active_backends()
    if not all_backends:
        raise RuntimeError("No active backends configured for dashboard learning")

    try:
        matched = [b for b in all_backends if b.name == backend_name]
        if not matched:
            available = [b.name for b in all_backends]
            raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")
        backend = matched[0]
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
