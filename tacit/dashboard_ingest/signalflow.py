"""SignalFlow extraction helpers for dashboard ingestion."""

from __future__ import annotations


def extract_metrics_from_signalflow(expr: str) -> list[str]:
    """Extract metric names from a SignalFlow expression."""
    from tacit.backends.signalfx import _extract_metrics_from_signalflow

    return _extract_metrics_from_signalflow(expr)


__all__ = ["extract_metrics_from_signalflow"]
