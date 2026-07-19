"""Alert ingestion and normalization.

Alert ingestion mirrors dashboard ingestion: backend adapters handle vendor API
shape, then this module performs signal inference, SQLite persistence, and FTS
indexing against a common ``AlertFeatures`` structure.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import asdict
from typing import Any
from urllib.parse import urlparse

import structlog

from tacit.backends.base import AlertFeatures
from tacit.config import Settings, settings
from tacit.dashboard_ingest import infer_signals_from_metrics, persist_inferred_signal_review
from tacit.dashboard_ingest.reports import build_learning_impact_report, build_signal_quality_report
from tacit.dashboard_ingest.service import resolve_learning_tenant
from tacit.signals import get_signal_store as _default_get_signal_store
from tacit.signals.learning_index import infer_services_for_learning

logger = structlog.get_logger()


def get_signal_store():
    """Resolve the signal store through the package façade for test isolation."""
    import tacit.signals as signals_pkg

    return getattr(signals_pkg, "get_signal_store", _default_get_signal_store)()


def alert_to_panel(features: AlertFeatures) -> dict[str, Any]:
    """Represent an alert rule as a learning-index panel-like row."""
    return {
        "title": features.alert_title,
        "description": features.condition,
        "panel_type": "alert_rule",
        "metrics": features.metrics_found,
        "queries": features.query_transformations,
        "datasource_type": features.backend_name,
        "query_language": features.query_language,
        "row": "alerts",
        "service_hints": features.service_hints,
    }


def _services_for_alert(features: AlertFeatures) -> list[str]:
    services: list[str] = []

    def add(value: str) -> None:
        cleaned = value.strip()
        if cleaned and cleaned not in services:
            services.append(cleaned)

    for key, value in features.labels.items():
        if key.lower() in {"service", "service_name", "app", "application", "component", "team"}:
            add(value)
    for tag in features.alert_tags:
        if ":" in tag:
            key, value = tag.split(":", 1)
            if key.lower() in {"service", "app", "application", "component", "team"}:
                add(value)

    query_text = "\n".join(features.query_transformations)
    for metric in features.metrics_found:
        for service in infer_services_for_learning(
            metric=metric,
            query_text=query_text,
            dashboard_title=features.alert_title,
            panel_title=features.condition,
            tags=features.alert_tags,
        ):
            add(service)
    return services


def _alert_fingerprint(features: AlertFeatures) -> str:
    payload = {
        "title": features.alert_title,
        "tags": sorted(dict.fromkeys(features.alert_tags)),
        "condition": features.condition,
        "severity": features.severity,
        "enabled": features.enabled,
        "labels": dict(sorted(features.labels.items())),
        "annotations": dict(sorted(features.annotations.items())),
        "metrics": sorted(dict.fromkeys(features.metrics_found)),
        "queries": sorted(dict.fromkeys(features.query_transformations)),
        "service_hints": sorted(dict.fromkeys(features.service_hints)),
        "dashboard_uid": features.dashboard_uid,
        "panel_title": features.panel_title,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _runbook_links(features: AlertFeatures) -> list[str]:
    values = [*features.alert_tags, *features.annotations.values()]
    return [
        value
        for value in values
        if isinstance(value, str) and ("runbook" in value.lower() or "playbook" in value.lower())
    ]


def _confidence_from_signals(signals: list[dict[str, Any]]) -> float:
    confidences = [float(sig.get("confidence", 0.0)) for sig in signals if isinstance(sig, dict)]
    return round(max(confidences), 4) if confidences else 0.0


def _source_instance(features: AlertFeatures) -> str:
    if features.source_url:
        host = urlparse(features.source_url).netloc
        if host:
            return host
    return features.backend_name


def _alert_summary(
    *,
    source: str,
    ingested: int,
    updated: int,
    skipped: int,
    signals_mapped: int,
    runbooks_found: int,
    ownership_hints: int,
    stale_marked: int = 0,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "source": source,
        "artifact_type": "alert_rule",
        "ingested": ingested,
        "updated": updated,
        "skipped": skipped,
        "stale_marked": stale_marked,
        "signals_mapped": signals_mapped,
        "runbooks_found": runbooks_found,
        "ownership_hints": ownership_hints,
        "warnings": warnings or [],
    }


async def ingest_alert_features(
    features: AlertFeatures,
    *,
    auto_approve: bool = False,
    dry_run: bool = False,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Infer, persist, and optionally approve already-extracted alert features."""
    effective_tenant = tenant_id if dry_run else resolve_learning_tenant(tenant_id)
    store = get_signal_store()
    status = "approved" if auto_approve else "pending"
    panel = alert_to_panel(features)
    signals = infer_signals_from_metrics(
        features.metrics_found,
        [panel],
        tenant_id=effective_tenant or "default",
    )
    signal_quality = build_signal_quality_report(metrics=features.metrics_found, signals=signals)
    learning_impact = build_learning_impact_report(
        metrics=features.metrics_found,
        signals=signals,
        approved=auto_approve,
    )
    service_hints = list(dict.fromkeys([*features.service_hints, *_services_for_alert(features)]))
    runbook_links = _runbook_links(features)
    fingerprint = _alert_fingerprint(features)
    confidence = _confidence_from_signals(signals)

    source_ref = f"{features.backend_name}:alert:{features.alert_uid}" if features.backend_name else features.alert_uid
    mappings_created = 0
    activated_pairs: set[tuple[str, str]] = set()
    if auto_approve and not dry_run:
        for sig in signals:
            if persist_inferred_signal_review(
                store=store,
                sig=sig,
                source_ref=source_ref,
                dashboard_uid=features.alert_uid,
                backend_name=features.backend_name,
                tenant_id=effective_tenant,
                source_type="alert_ingest",
            ):
                mappings_created += 1
                activated_pairs.add((sig.get("metric", ""), sig.get("signal_type", "")))

    change_state = "dry_run"
    indexed_context_rows = 0
    effective_status = status
    if not dry_run:
        change_state = store.record_ingested_alert(
            alert_uid=features.alert_uid,
            tenant_id=effective_tenant,
            backend_name=features.backend_name,
            source_vendor=features.backend_name,
            source_instance=_source_instance(features),
            external_id=features.alert_uid,
            fingerprint=fingerprint,
            alert_title=features.alert_title,
            alert_tags=features.alert_tags,
            condition=features.condition,
            severity=features.severity,
            enabled=features.enabled,
            labels=features.labels,
            annotations=features.annotations,
            metrics_found=features.metrics_found,
            query_transformations=features.query_transformations,
            service_hints=service_hints,
            dashboard_uid=features.dashboard_uid,
            panel_title=features.panel_title,
            source_url=features.source_url,
            provenance_url=features.source_url,
            confidence=confidence,
            signals_inferred=signals,
            status=status,
        )
        stored_alert = store.get_ingested_alert(
            features.alert_uid,
            features.backend_name,
            tenant_id=effective_tenant,
        )
        if stored_alert is not None:
            effective_status = str(stored_alert.get("status") or status)
        indexed_context_rows = store.index_alert_context(
            tenant_id=effective_tenant,
            alert_uid=features.alert_uid,
            backend_name=features.backend_name,
            alert_title=features.alert_title,
            alert_tags=features.alert_tags,
            condition=features.condition,
            metrics_found=features.metrics_found,
            query_transformations=features.query_transformations,
            service_hints=service_hints,
            signals_inferred=signals,
            status=effective_status,
            activated_pairs=activated_pairs if auto_approve else None,
        )

    result = {
        **asdict(features),
        "service_hints": service_hints,
        "fingerprint": fingerprint,
        "confidence": confidence,
        "status": effective_status,
        "change_state": change_state,
        "dry_run": dry_run,
        "signals_inferred": signals,
        "signal_quality": signal_quality,
        "learning_impact": learning_impact,
        "indexed_context_rows": indexed_context_rows,
        "summary": _alert_summary(
            source=features.backend_name,
            ingested=0 if dry_run else int(change_state in {"created", "updated", "skipped"}),
            updated=int(change_state == "updated"),
            skipped=int(change_state == "skipped"),
            signals_mapped=len(signals),
            runbooks_found=len(runbook_links),
            ownership_hints=len(service_hints),
            warnings=[] if features.metrics_found else ["no_metrics_extracted"],
        ),
    }
    if auto_approve:
        result["mappings_created"] = mappings_created
    logger.info(
        "alert_ingested",
        uid=features.alert_uid,
        backend=features.backend_name,
        status=status,
        dry_run=dry_run,
        metrics=len(features.metrics_found),
        signals=len(signals),
        indexed_context_rows=indexed_context_rows,
    )
    return result


async def ingest_alert(
    alert_uid: str,
    backend: Any | None = None,
    backend_name: str = "",
    auto_approve: bool = False,
    dry_run: bool = False,
    runtime_settings: Settings | None = None,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Full alert ingestion pipeline: fetch -> extract -> infer -> persist."""
    from tacit.backends import get_active_backends

    all_backends: list[Any] = []
    own_backends = False
    try:
        if backend is None:
            all_backends = (
                get_active_backends(runtime_settings) if runtime_settings is not None else get_active_backends()
            )
            own_backends = True
            if not all_backends:
                raise RuntimeError("No active backends configured for alert ingestion")
            if backend_name:
                matched = [b for b in all_backends if b.name == backend_name]
                if not matched:
                    available = [b.name for b in all_backends]
                    raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")
                backend = matched[0]
            else:
                backend = all_backends[0]
        if backend is None:
            raise RuntimeError("No backend selected for alert ingestion")
        features = await backend.ingest_alert(alert_uid)
        return await ingest_alert_features(
            features,
            auto_approve=auto_approve,
            dry_run=dry_run,
            tenant_id=tenant_id,
        )
    finally:
        if own_backends:
            for item in all_backends:
                await item.close()


async def learn_backend_alerts(
    backend_name: str,
    *,
    auto_approve: bool = False,
    dry_run: bool = False,
    limit: int = 500,
    runtime_settings: Settings | None = None,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Crawl a backend and learn from every discoverable alert rule."""
    from tacit.backends import get_active_backends

    effective_tenant = tenant_id if dry_run else resolve_learning_tenant(tenant_id)
    active_settings = runtime_settings or settings
    all_backends = get_active_backends(runtime_settings) if runtime_settings is not None else get_active_backends()
    if not all_backends:
        raise RuntimeError("No active backends configured for alert ingestion")

    try:
        matched = [b for b in all_backends if b.name == backend_name]
        if not matched:
            available = [b.name for b in all_backends]
            raise ValueError(f"Backend '{backend_name}' not found. Available: {available}")
        backend = matched[0]
        alerts = await backend.list_alerts(limit=limit)

        learned: list[dict[str, Any]] = []
        failures: list[dict[str, str]] = []
        totals = {
            "alerts_discovered": len(alerts),
            "alerts_learned": 0,
            "alerts_failed": 0,
            "metrics_found": 0,
            "signals_inferred": 0,
            "indexed_context_rows": 0,
            "mappings_created": 0,
            "stale_marked": 0,
        }
        sem = asyncio.Semaphore(max(1, active_settings.adapter_max_concurrent))

        async def learn_one(item: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
            uid = item.get("uid", "")
            if not uid:
                return None, None
            try:
                async with sem:
                    result = await ingest_alert(
                        uid,
                        backend=backend,
                        auto_approve=auto_approve,
                        dry_run=dry_run,
                        tenant_id=effective_tenant,
                    )
                return (
                    {
                        "alert_uid": result.get("alert_uid", uid),
                        "alert_title": result.get("alert_title", item.get("title", "")),
                        "status": result.get("status", "pending"),
                        "metrics_found": len(result.get("metrics_found", [])),
                        "signals_inferred": len(result.get("signals_inferred", [])),
                        "indexed_context_rows": result.get("indexed_context_rows", 0),
                        "mappings_created": result.get("mappings_created", 0),
                        "summary": result.get("summary", {}),
                    },
                    None,
                )
            except Exception as exc:
                return None, {"alert_uid": uid, "title": item.get("title", ""), "error": str(exc)}

        results = await asyncio.gather(*(learn_one(item) for item in alerts))
        for learned_item, failure in results:
            if learned_item is not None:
                learned.append(learned_item)
                summary = learned_item.get("summary", {})
                totals["alerts_learned"] += 1
                totals["metrics_found"] += int(learned_item.get("metrics_found", 0) or 0)
                totals["signals_inferred"] += int(learned_item.get("signals_inferred", 0) or 0)
                totals["indexed_context_rows"] += int(learned_item.get("indexed_context_rows", 0) or 0)
                totals["mappings_created"] += int(learned_item.get("mappings_created", 0) or 0)
                totals.setdefault("updated", 0)
                totals.setdefault("skipped", 0)
                totals["updated"] += int(summary.get("updated", 0) or 0)
                totals["skipped"] += int(summary.get("skipped", 0) or 0)
            if failure is not None:
                failures.append(failure)
                totals["alerts_failed"] += 1

        stale_reconciliation_complete = bool(getattr(backend, "last_alert_list_complete", False))
        if not dry_run and stale_reconciliation_complete:
            assert effective_tenant is not None
            store = get_signal_store()
            seen_alert_uids = {str(item.get("uid", "")) for item in alerts if item.get("uid")}
            totals["stale_marked"] = store.mark_missing_alerts_stale(
                tenant_id=effective_tenant,
                backend_name=backend_name,
                seen_alert_uids=seen_alert_uids,
            )
            if totals["stale_marked"]:
                from tacit.knowledge.repository import KnowledgeRepository
                from tacit.knowledge.service import KnowledgeService

                knowledge_service = KnowledgeService(KnowledgeRepository(store._db_path))
                for alert in store.list_ingested_alerts(
                    status="stale",
                    limit=10_000,
                    tenant_id=effective_tenant,
                ):
                    if alert.get("backend_name") == backend_name:
                        knowledge_service.reconcile_source_lifecycle(
                            provenance_ref=(
                                f"{backend_name}:alert:{alert['alert_uid']}"
                                if backend_name
                                else str(alert["alert_uid"])
                            ),
                            tenant_id=effective_tenant,
                            source_stale=True,
                        )
        elif not dry_run:
            totals["stale_reconciliation_skipped"] = True

        return {
            "backend": backend_name,
            "auto_approve": auto_approve,
            "dry_run": dry_run,
            **totals,
            "summary": _alert_summary(
                source=backend_name,
                ingested=0 if dry_run else totals["alerts_learned"],
                updated=int(totals.get("updated", 0)),
                skipped=int(totals.get("skipped", 0)),
                stale_marked=int(totals.get("stale_marked", 0)),
                signals_mapped=totals["signals_inferred"],
                runbooks_found=sum(int((item.get("summary") or {}).get("runbooks_found", 0) or 0) for item in learned),
                ownership_hints=sum(
                    int((item.get("summary") or {}).get("ownership_hints", 0) or 0) for item in learned
                ),
                warnings=(
                    ["stale_reconciliation_skipped_partial_crawl"] if totals.get("stale_reconciliation_skipped") else []
                ),
            ),
            "learned": learned,
            "failures": failures,
        }
    finally:
        for backend in all_backends:
            await backend.close()
