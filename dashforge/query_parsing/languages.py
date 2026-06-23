"""Datasource type and query-language normalization."""

from __future__ import annotations


def datasource_type_to_language(ds_type: str) -> str:
    """Map a Grafana datasource type to its query language."""
    t = (ds_type or "").lower()
    if not t:
        return "promql"
    exact = {
        "prometheus": "promql",
        "mimir": "promql",
        "cortex": "promql",
        "thanos": "promql",
        "loki": "logql",
        "cloudwatch": "cloudwatch",
        "signalfx": "signalflow",
        "elasticsearch": "lucene",
        "opensearch": "lucene",
        "graphite": "graphite",
        "influxdb": "influxql",
    }
    if t in exact:
        return exact[t]
    for needle, lang in (
        ("prometheus", "promql"),
        ("signalfx", "signalflow"),
        ("loki", "logql"),
        ("cloudwatch", "cloudwatch"),
        ("elasticsearch", "lucene"),
        ("opensearch", "lucene"),
        ("graphite", "graphite"),
        ("influx", "influxql"),
    ):
        if needle in t:
            return lang
    return "promql"


def language_to_datasource_type(language: str) -> str:
    """Best-effort inverse of :func:`datasource_type_to_language` for tagging."""
    return {
        "promql": "prometheus",
        "logql": "loki",
        "cloudwatch": "cloudwatch",
        "signalflow": "signalfx",
        "lucene": "elasticsearch",
        "graphite": "graphite",
        "influxql": "influxdb",
    }.get(language, "prometheus")
