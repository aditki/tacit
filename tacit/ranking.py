"""Lightweight pre-ranking of metrics before LLM reasoning.

Reduces the catalog from hundreds/thousands of metrics to a manageable
set of top candidates, cutting LLM token cost and latency.

Human feedback is retained for evaluation and governed candidate production. It
does not directly change runtime ranking because that would bypass Operational
Knowledge revisions, snapshots, and usage attribution.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from tacit.models.schemas import Intent, MetricEntry

# Max metrics to send to the LLM after pre-ranking
MAX_LLM_CANDIDATES = 60


def invalidate_metric_quality_cache(store: Any | None = None, *, tenant_id: str | None = None) -> None:
    """Compatibility hook; feedback no longer owns a runtime-ranking cache."""
    del store, tenant_id


def _score_metric(
    name: str,
    keywords: list[str],
    services: list[str],
) -> float:
    """Score a metric by keyword and service relevance."""
    name_lower = name.lower()
    score = 0.0

    # Keyword matches in metric name
    for kw in keywords:
        kw_l = kw.lower().replace(" ", "_")
        if kw_l in name_lower:
            score += 10.0
        # Partial match (prefix of a segment)
        for segment in name_lower.split("_"):
            if segment.startswith(kw_l[:3]) and len(kw_l) >= 3:
                score += 2.0
                break

    # Service name matches
    for svc in services:
        svc_l = svc.lower().replace(" ", "_").replace("-", "_")
        if svc_l in name_lower:
            score += 5.0
        # Check common prefix patterns
        for part in svc_l.split("_"):
            if part in name_lower and len(part) >= 3:
                score += 1.0

    # Boost common observability metrics
    observability_signals = [
        "request",
        "latency",
        "duration",
        "error",
        "total",
        "bytes",
        "cpu",
        "memory",
        "connections",
        "in_flight",
        "queue",
        "restarts",
        "health",
        "up",
        "status",
    ]
    for sig in observability_signals:
        if sig in name_lower:
            score += 1.0

    return score


def prerank_metrics(
    intent: Intent,
    catalog: list[MetricEntry],
    max_candidates: int = MAX_LLM_CANDIDATES,
    *,
    feedback_store: Any | None = None,
    feedback_store_factory: Callable[[], Any] | None = None,
    tenant_id: str = "default",
) -> list[MetricEntry]:
    """Rank and truncate the metric catalog before sending to the LLM.

    Returns at most `max_candidates` metrics, scored by relevance to the intent.

    The feedback parameters remain for source compatibility but are deliberately
    ignored until feedback-derived behavior is represented by governed knowledge.
    """
    del feedback_store, feedback_store_factory, tenant_id
    if len(catalog) <= max_candidates:
        return catalog

    scored = [(entry, _score_metric(entry.name, intent.keywords, intent.services)) for entry in catalog]
    # Sort by score descending, stable (preserves original order for ties)
    scored.sort(key=lambda x: x[1], reverse=True)

    return [entry for entry, _ in scored[:max_candidates]]
