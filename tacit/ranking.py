"""Lightweight pre-ranking of metrics before LLM reasoning.

Reduces the catalog from hundreds/thousands of metrics to a manageable
set of top candidates, cutting LLM token cost and latency.

Feedback loop: when feedback data is available, metric quality scores
from human reviews are used to boost/penalize metrics during ranking.
Metrics that consistently appear in well-rated dashboards get promoted;
metrics that correlate with poorly-rated dashboards get demoted.
"""

from __future__ import annotations

import time

import structlog

from tacit.models.schemas import Intent, MetricEntry

logger = structlog.get_logger()

# Max metrics to send to the LLM after pre-ranking
MAX_LLM_CANDIDATES = 60

# ── Feedback-driven metric quality cache ──────────────────────────────────
# Cached metric quality scores from the feedback store.
# Refreshed at most every 10 minutes to avoid hitting SQLite on every request.

_metric_quality_cache: dict[str, float] = {}
_metric_quality_expires: float = 0.0
_QUALITY_CACHE_TTL = 600  # seconds


def _load_metric_quality() -> dict[str, float]:
    """Load metric quality scores from feedback analysis.

    Returns a dict of {metric_name: quality_score} where:
    - quality_score > 0.5 → metric appears more in useful dashboards (boost)
    - quality_score < 0.5 → metric appears more in poor dashboards (penalize)
    - quality_score = 0.5 → neutral (no data or balanced)
    """
    global _metric_quality_cache, _metric_quality_expires

    now = time.monotonic()
    if now < _metric_quality_expires and _metric_quality_cache:
        return _metric_quality_cache

    try:
        from tacit.feedback import get_feedback_store

        store = get_feedback_store()
        report = store.analyze()
        quality_list = report.get("metric_quality", [])
        _metric_quality_cache = {m["metric"]: m["quality_score"] for m in quality_list}
        _metric_quality_expires = now + _QUALITY_CACHE_TTL
        if _metric_quality_cache:
            logger.debug("metric_quality_loaded", count=len(_metric_quality_cache))
    except Exception:
        _metric_quality_cache = {}
        _metric_quality_expires = now + 60  # retry sooner on failure

    return _metric_quality_cache


def _score_metric(
    name: str,
    keywords: list[str],
    services: list[str],
    feedback_scores: dict[str, float] | None = None,
) -> float:
    """Score a metric by keyword + service relevance. Higher = more relevant.

    When feedback_scores is provided, applies a feedback multiplier:
    - quality > 0.7 → 1.3x boost (metric in good dashboards)
    - quality < 0.3 → 0.7x penalty (metric in bad dashboards)
    """
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

    # Feedback-driven boost/penalty
    if feedback_scores and score > 0:
        quality = feedback_scores.get(name)
        if quality is not None:
            if quality >= 0.7:
                score *= 1.3  # boost: metric in good dashboards
            elif quality <= 0.3:
                score *= 0.7  # penalize: metric in bad dashboards

    return score


def prerank_metrics(
    intent: Intent,
    catalog: list[MetricEntry],
    max_candidates: int = MAX_LLM_CANDIDATES,
) -> list[MetricEntry]:
    """Rank and truncate the metric catalog before sending to the LLM.

    Returns at most `max_candidates` metrics, scored by relevance to the intent.
    Incorporates feedback-driven quality scores when available.
    """
    if len(catalog) <= max_candidates:
        return catalog

    # Load feedback quality scores (cached, lightweight)
    feedback_scores = _load_metric_quality()

    scored = [
        (entry, _score_metric(entry.name, intent.keywords, intent.services, feedback_scores)) for entry in catalog
    ]
    # Sort by score descending, stable (preserves original order for ties)
    scored.sort(key=lambda x: x[1], reverse=True)

    return [entry for entry, _ in scored[:max_candidates]]
