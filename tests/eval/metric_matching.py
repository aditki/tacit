"""Deterministic one-to-one metric matching for quality gates."""

from __future__ import annotations

_PROMETHEUS_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count", "_created")


def match_metric_sets(expected: set[str], found: set[str]) -> dict[str, str]:
    """Return a one-to-one expected-to-found mapping.

    Exact identity wins before the only supported fuzzy relation: a Prometheus
    histogram family base may match one of its generated suffix series.
    """
    matches: dict[str, str] = {}
    available = set(found)

    for metric in sorted(expected & available):
        matches[metric] = metric
        available.remove(metric)

    for metric in sorted(expected - matches.keys()):
        derived = next(
            (candidate for suffix in _PROMETHEUS_HISTOGRAM_SUFFIXES if (candidate := f"{metric}{suffix}") in available),
            None,
        )
        if derived is not None:
            matches[metric] = derived
            available.remove(derived)

    return matches
