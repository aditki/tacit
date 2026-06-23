"""Dashboard ingestion report builders."""

from __future__ import annotations

from typing import Any


def normalize_signal_records(signals: list[dict[str, Any]] | list[str]) -> list[dict[str, Any]]:
    """Return signal records as dictionaries, preserving legacy string entries."""
    normalized: list[dict[str, Any]] = []
    for sig in signals:
        if isinstance(sig, dict):
            normalized.append(sig)
        elif isinstance(sig, str):
            normalized.append(
                {
                    "signal_type": sig,
                    "metric": "",
                    "confidence": 0.0,
                    "source": "legacy",
                    "reason": "Legacy ingested dashboard stored only the signal name.",
                }
            )
    return normalized


def build_signal_quality_report(
    *,
    metrics: list[str],
    signals: list[dict[str, Any]] | list[str],
) -> dict[str, Any]:
    """Summarize how conservatively DashForge understood an ingested dashboard."""
    metrics = list(dict.fromkeys(metrics))
    normalized = normalize_signal_records(signals)
    mapped_metrics = sorted({sig.get("metric", "") for sig in normalized if sig.get("metric")})
    taxonomy = [sig for sig in normalized if sig.get("source") == "taxonomy"]
    heuristic = [sig for sig in normalized if sig.get("source") == "heuristic"]
    legacy = [sig for sig in normalized if sig.get("source") == "legacy"]
    auto_teachable = [sig for sig in heuristic if sig.get("auto_teach_eligible")]
    held_for_review = [sig for sig in heuristic if not sig.get("auto_teach_eligible")]

    confidence_buckets = {
        "high": sum(1 for sig in normalized if sig.get("confidence", 0.0) >= 0.8),
        "medium": sum(1 for sig in normalized if 0.5 <= sig.get("confidence", 0.0) < 0.8),
        "low": sum(1 for sig in normalized if sig.get("confidence", 0.0) < 0.5),
    }

    return {
        "metrics_total": len(metrics),
        "metrics_mapped": len(mapped_metrics),
        "metrics_unmapped": [metric for metric in metrics if metric not in mapped_metrics],
        "taxonomy_matches": len(taxonomy),
        "heuristic_candidates": len(heuristic),
        "legacy_signals": len(legacy),
        "auto_teach_eligible": len(auto_teachable),
        "held_for_review": len(held_for_review),
        "confidence_buckets": confidence_buckets,
        "explanations": [
            {
                "signal_type": sig.get("signal_type", ""),
                "metric": sig.get("metric", ""),
                "confidence": sig.get("confidence", 0.0),
                "source": sig.get("source", ""),
                "review_state": (
                    "trusted"
                    if sig.get("source") == "taxonomy"
                    else "eligible" if sig.get("auto_teach_eligible") else "review"
                ),
                "reason": sig.get("reason", ""),
                "evidence": sig.get("evidence", []),
                "why_not_auto_taught": sig.get("why_not_auto_taught", ""),
            }
            for sig in normalized
        ],
    }


def build_learning_impact_report(
    *,
    metrics: list[str],
    signals: list[dict[str, Any]] | list[str],
    approved: bool = False,
) -> dict[str, Any]:
    """Show what approval would change for future dashboard generation."""
    normalized = normalize_signal_records(signals)
    taxonomy_metrics = sorted(
        {sig.get("metric", "") for sig in normalized if sig.get("source") == "taxonomy" and sig.get("metric")}
    )
    teachable = [
        sig
        for sig in normalized
        if sig.get("metric")
        and (
            (sig.get("source") == "heuristic" and sig.get("auto_teach_eligible"))
            or (sig.get("source") != "heuristic" and sig.get("confidence", 0.0) >= 0.5)
        )
    ]
    teachable_metrics = sorted({sig.get("metric", "") for sig in teachable if sig.get("metric")})
    before = len(taxonomy_metrics)
    after = len(sorted(set(taxonomy_metrics) | set(teachable_metrics)))
    candidate_metrics = [metric for metric in teachable_metrics if metric not in taxonomy_metrics]
    active_after_approval = candidate_metrics if approved else []
    unresolved = [
        metric
        for metric in dict.fromkeys(metrics)
        if metric not in taxonomy_metrics and metric not in teachable_metrics
    ]

    return {
        "recognized_metrics_before_learning": before,
        "recognized_metrics_after_approval": after,
        "active_mappings_before_learning": before,
        "active_mappings_after_approval": before + len(active_after_approval),
        "candidate_mappings_pending_approval": 0 if approved else len(candidate_metrics),
        "new_active_mappings_after_approval": len(active_after_approval),
        "new_mappings_available": len(candidate_metrics),
        "newly_understood_metrics": [
            {
                "metric": sig.get("metric", ""),
                "signal_type": sig.get("signal_type", ""),
                "confidence": sig.get("confidence", 0.0),
                "source": sig.get("source", ""),
                "mapping_state": "approved" if approved else "candidate",
                "reason": sig.get("reason", ""),
            }
            for sig in teachable
            if sig.get("metric") in candidate_metrics
        ],
        "newly_active_metrics_after_approval": [
            {
                "metric": sig.get("metric", ""),
                "signal_type": sig.get("signal_type", ""),
                "confidence": sig.get("confidence", 0.0),
                "source": sig.get("source", ""),
                "mapping_state": "approved",
                "reason": sig.get("reason", ""),
            }
            for sig in teachable
            if sig.get("metric") in active_after_approval
        ],
        "unresolved_metrics": unresolved,
    }
