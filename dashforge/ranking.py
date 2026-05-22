"""Lightweight pre-ranking of metrics before LLM reasoning.

Reduces the catalog from hundreds/thousands of metrics to a manageable
set of top candidates, cutting LLM token cost and latency.
"""
from __future__ import annotations

from dashforge.models.schemas import Intent, MetricEntry

# Max metrics to send to the LLM after pre-ranking
MAX_LLM_CANDIDATES = 60


def _score_metric(name: str, keywords: list[str], services: list[str]) -> float:
    """Score a metric by keyword + service relevance. Higher = more relevant."""
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
        "request", "latency", "duration", "error", "total",
        "bytes", "cpu", "memory", "connections", "in_flight",
        "queue", "restarts", "health", "up", "status",
    ]
    for sig in observability_signals:
        if sig in name_lower:
            score += 1.0

    return score


def prerank_metrics(
    intent: Intent,
    catalog: list[MetricEntry],
    max_candidates: int = MAX_LLM_CANDIDATES,
) -> list[MetricEntry]:
    """Rank and truncate the metric catalog before sending to the LLM.

    Returns at most `max_candidates` metrics, scored by relevance to the intent.
    All keyword-matched metrics are included first, then backfilled by score.
    """
    if len(catalog) <= max_candidates:
        return catalog

    scored = [
        (entry, _score_metric(entry.name, intent.keywords, intent.services))
        for entry in catalog
    ]
    # Sort by score descending, stable (preserves original order for ties)
    scored.sort(key=lambda x: x[1], reverse=True)

    return [entry for entry, _ in scored[:max_candidates]]
