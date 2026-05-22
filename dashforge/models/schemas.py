from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Intent ───────────────────────────────────────────────────────────────────

class SignalType(str, Enum):
    METRICS = "metrics"
    LOGS = "logs"
    TRACES = "traces"


class Intent(BaseModel):
    """Output of the Intent Agent."""

    summary: str = Field(description="One-line restatement of the user problem")
    domain: str = Field(description="Observability domain, e.g. 'infrastructure', 'application', 'network'")
    services: list[str] = Field(default_factory=list, description="Mentioned service/component names")
    signals: list[SignalType] = Field(default_factory=lambda: [SignalType.METRICS], description="Signal types to explore")
    keywords: list[str] = Field(default_factory=list, description="Key terms for metric search, e.g. 'latency', 'error_rate', 'cpu'")
    timerange: str = Field(default="1h", description="Suggested lookback window")
    problem_type: str = Field(
        default="general",
        description="Investigation archetype classification. One of: "
        "latency_investigation, slow_requests, high_latency, p99_spike, "
        "error_spike, 5xx_errors, error_rate, failed_requests, "
        "golden_signals, sre_overview, service_health, service_overview, "
        "resource_saturation, high_cpu, high_memory, oom, memory_leak, "
        "cpu_throttling, general"
    )


# ── Context Enrichment ───────────────────────────────────────────────────────

class ContextChunk(BaseModel):
    """A chunk of context retrieved from an external knowledge base."""

    content: str = Field(description="Retrieved text (runbook excerpt, wiki snippet, service doc, etc.)")
    source: str = Field(default="", description="Origin identifier, e.g. 'runbook:checkout-service', 'wiki:incident-playbook'")
    relevance_score: float = Field(default=0.0, description="Relevance score from the retrieval system (0-1)")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Provider-specific metadata")


# ── Metrics Discovery ────────────────────────────────────────────────────────

class DatasourceInfo(BaseModel):
    uid: str
    name: str
    type: str  # prometheus, cloudwatch, loki, elasticsearch, graphite, influxdb, …
    url: str = ""
    is_default: bool = False
    json_data: dict = Field(default_factory=dict, description="Datasource-specific config from Grafana")


class MetricEntry(BaseModel):
    """Normalized metric from any datasource type."""

    name: str = Field(description="Metric name (e.g. 'http_requests_total' or 'AWS/ELB HTTPCode_ELB_5XX')")
    datasource_uid: str
    datasource_name: str
    datasource_type: str = Field(description="Grafana datasource type")
    query_language: str = Field(description="Query language: promql, logql, cloudwatch, elasticsearch, graphite, influxql, flux")
    namespace: str = Field(default="", description="Metric namespace or group (e.g. 'AWS/ELB', 'node_exporter', 'kube-state-metrics')")
    dimensions: list[str] = Field(default_factory=list, description="Available dimensions / label names")


class DiscoveredMetric(BaseModel):
    metric_name: str
    datasource_uid: str
    datasource_name: str
    datasource_type: str = "prometheus"
    query_language: str = "promql"
    namespace: str = ""
    relevance_reason: str = ""


class MetricsDiscoveryResult(BaseModel):
    metrics: list[DiscoveredMetric]


# ── Query Builder ────────────────────────────────────────────────────────────

class PanelQuery(BaseModel):
    """A single PromQL / LogQL expression with context."""

    expr: str = Field(description="PromQL or LogQL expression")
    legend_format: str = Field(default="{{instance}}", description="Legend template")
    datasource_uid: str
    datasource_type: str = "prometheus"


class PanelSpec(BaseModel):
    """Specification for one Grafana panel."""

    title: str
    description: str = ""
    panel_type: str = Field(default="timeseries", description="Grafana panel type: timeseries, stat, gauge, table, logs …")
    queries: list[PanelQuery]
    unit: str = Field(default="", description="Grafana unit id, e.g. 'percentunit', 's', 'bytes'")
    thresholds: list[dict[str, Any]] = Field(default_factory=list)
    row: str = Field(default="", description="Optional row/section name for grouping, e.g. 'Latency', 'Traffic'. Leave empty for no grouping.")


class DashboardSpec(BaseModel):
    """Full spec handed to the Dashboard Builder."""

    title: str
    tags: list[str] = Field(default_factory=list)
    timerange: str = "1h"
    panels: list[PanelSpec]


# ── Pipeline request / response ──────────────────────────────────────────────

class DashRequest(BaseModel):
    """Inbound request from Slack (or HTTP)."""

    prompt: str
    channel_id: str = ""
    user_id: str = ""
    thread_ts: str = ""


class DashResponse(BaseModel):
    dashboard_url: str
    dashboard_uid: str
    panel_count: int
    summary: str
