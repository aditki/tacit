"""Discovery and live-signal confirmation helpers for the pipeline."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import structlog

from tacit.backends.base import DashboardBackend
from tacit.catalog import catalog_for_services
from tacit.models.schemas import Intent, MetricEntry
from tacit.signals.availability import resolve_signal_store

logger = structlog.get_logger()


@dataclass
class DiscoveryResult:
    metric_catalog: list[MetricEntry]
    datasource_catalog: list[MetricEntry]
    datasource_types: list[str]

    @property
    def catalog_for_compile(self) -> list[MetricEntry]:
        return self.metric_catalog or self.datasource_catalog


def discovery_keywords(intent: Intent) -> list[str]:
    """Include advisory evidence when searching provider-scoped catalogs.

    Colloquial evidence may broaden discovery so providers such as CloudWatch
    can inspect the relevant namespace. It does not become trusted intent until
    post-discovery semantic-signal confirmation succeeds.
    """
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
) -> list[str]:
    """Promote low-confidence colloquial evidence only after live signal coverage.

    A metaphor implying "cache" becomes a real keyword only if a cache signal
    resolves against the service-scoped discovered metrics, using the signal
    store instead of a global substring match.
    """
    if not intent.keyword_evidence or not metric_catalog:
        return []

    try:
        from tacit.agents.synonyms import SynonymEvidence, confirm_colloquial
        from tacit.signals import get_signal_store

        signal_store = resolve_signal_store(signal_store, get_signal_store)
        if signal_store is None:
            return []
        resolve_cache: dict[str, bool] = {}
        confirmation_catalog = catalog_for_services(metric_catalog, intent.services)
        context_service = intent.services[0] if intent.services else ""

        def signal_resolves(sig: str) -> bool:
            if sig not in resolve_cache:
                try:
                    hits = signal_store.resolve_signal(
                        sig,
                        confirmation_catalog,
                        context_service=context_service,
                        target_query_language=target_query_language,
                    )
                    resolve_cache[sig] = bool(hits)
                except Exception:
                    resolve_cache[sig] = False
            return resolve_cache[sig]

        evidence = [
            SynonymEvidence(
                keyword=str(e.get("keyword", "")),
                score=float(e.get("score", 0.0)),
                tier=str(e.get("tier", "")),
                source=str(e.get("source", "")),
            )
            for e in intent.keyword_evidence
        ]
        confirmed = confirm_colloquial(evidence, signal_resolves)
        for kw in confirmed:
            if kw not in intent.keywords:
                intent.keywords.append(kw)
        return confirmed
    except Exception:
        logger.warning("colloquial_confirmation_failed", exc_info=True)
        return []
