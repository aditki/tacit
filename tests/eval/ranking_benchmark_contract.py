"""Shared benchmark metadata for contextual culprit-ranking gates."""

from __future__ import annotations

from typing import Any

DEFAULT_CANDIDATE_SET_SIZE = 5
DEFAULT_TOP_K = 3


def random_baselines(
    *, candidate_set_size: int = DEFAULT_CANDIDATE_SET_SIZE, top_k: int = DEFAULT_TOP_K
) -> dict[str, float]:
    """Return chance baselines for a uniform random ranker over the full candidate set."""
    mrr = sum(1 / rank for rank in range(1, candidate_set_size + 1)) / candidate_set_size
    return {
        "top1_recall": round(1 / candidate_set_size, 4),
        "top3_recall": round(min(top_k, candidate_set_size) / candidate_set_size, 4),
        "mrr": round(mrr, 4),
    }


def benchmark_contract(
    *,
    case_count: int,
    scorable_case_count: int,
    negative_case_count: int,
    total_ranked_denominator: int,
    context_available: list[str],
    metric_denominators: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Return the shared denominator contract for ranking benchmark reports."""
    denominators = {
        "top1_recall": scorable_case_count,
        "top3_recall": scorable_case_count,
        "mrr": scorable_case_count,
        "false_culprit_rate": negative_case_count,
        "unsupported_rca_rate": total_ranked_denominator,
    }
    denominators.update(metric_denominators or {})
    return {
        "total_cases": case_count,
        "scorable_cases": scorable_case_count,
        "negative_cases": negative_case_count,
        "candidate_set_size": DEFAULT_CANDIDATE_SET_SIZE,
        "context_available": context_available,
        "top_k": DEFAULT_TOP_K,
        "metric_conventions": {
            "mrr": "Reciprocal rank over the full candidate set, not truncated at top_k.",
            "top3_recall": "Recall of scorable cases whose expected culprit ranks at or above top_k.",
            "source_contribution_rates": "Artifact-source contribution/noise rates use total_cases unless overridden.",
        },
        "metric_denominators": denominators,
        "random_baselines": random_baselines(),
    }
