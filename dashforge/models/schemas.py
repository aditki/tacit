from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Intent ───────────────────────────────────────────────────────────────────

class SignalType(str, Enum):
    METRICS = "metrics"
    LOGS = "logs"
    TRACES = "traces"


class ArchetypeMatch(BaseModel):
    """A single archetype classification with confidence score.

    Archetypes are treated as *retrieval priors* — they influence metric
    ranking and template selection, not hard-route the pipeline.
    """

    type: str = Field(description="Archetype identifier, e.g. 'latency_investigation'")
    confidence: float = Field(
        description="Confidence score 0.0–1.0",
        ge=0.0,
        le=1.0,
    )


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
    archetypes: list[ArchetypeMatch] = Field(
        default_factory=list,
        description="Multi-label archetype classifications ordered by confidence "
        "(highest first). Used as retrieval priors for metric ranking "
        "and template blending.",
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
    query_language: str = Field(description="Query language: promql, logql, cloudwatch, elasticsearch, graphite, influxql, flux, signalflow")
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
    """A single query expression for any supported datasource."""

    expr: str = Field(description="Query expression (PromQL, LogQL, SignalFlow, Lucene, metric name for CloudWatch, etc.)")
    legend_format: str = Field(default="{{instance}}", description="Legend template")
    datasource_uid: str
    datasource_type: str = "prometheus"
    # CloudWatch-specific fields (only set when datasource_type='cloudwatch')
    cloudwatch_namespace: str = Field(default="", description="AWS CloudWatch namespace, e.g. 'AWS/ApplicationELB'")
    cloudwatch_stat: str = Field(default="", description="CloudWatch statistic: Sum, Average, p99, etc.")
    cloudwatch_dimensions: dict[str, list[str]] = Field(default_factory=dict, description="CloudWatch dimensions, e.g. {'LoadBalancer': ['*']}")
    cloudwatch_dimensions: dict[str, str | list[str]] = Field(default_factory=dict, description="CloudWatch dimensions, e.g. {'LoadBalancer': '*'} or {'AvailabilityZone': ['us-east-1a', 'us-east-1b']}")
    cloudwatch_region: str = Field(default="", description="AWS region for this CloudWatch query, e.g. 'us-east-1'")


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

    prompt: str = Field(description="Natural-language description of the dashboard you need")
    channel_id: str = Field(default="", description="Slack channel ID (set automatically by Slack integration)")
    user_id: str = Field(default="", description="User identifier for provenance tracking")
    thread_ts: str = Field(default="", description="Slack thread timestamp (set automatically by Slack integration)")

    model_config = {"json_schema_extra": {"examples": [{
        "prompt": "High 5xx error rate on checkout-service in the last 30 minutes",
        "user_id": "web-ui",
    }]}}


class DashResponse(BaseModel):
    """Result of a successful dashboard generation."""

    dashboard_url: str = Field(description="Full Grafana URL to the published dashboard")
    dashboard_uid: str = Field(description="Unique Grafana dashboard UID")
    panel_count: int = Field(description="Number of panels in the generated dashboard")
    summary: str = Field(description="Human-readable summary of what was generated")
    signalfx_url: str = Field(default="", description="SignalFx dashboard URL (when signalfx_enabled)")
    signalfx_dashboard_id: str = Field(default="", description="SignalFx dashboard ID (when signalfx_enabled)")


# ── Feedback ────────────────────────────────────────────────────────────────

class FeedbackRequest(BaseModel):
    """Human evaluation of a generated dashboard."""

    dashboard_uid: str = Field(description="UID of the dashboard being reviewed")
    symptom_visibility: int | None = Field(
        default=None, ge=1, le=5,
        description="How well did the dashboard surface the symptom? (1=not at all, 5=immediately obvious)",
    )
    root_cause_support: int | None = Field(
        default=None, ge=1, le=5,
        description="Did the dashboard help identify root cause? (1=no help, 5=pointed directly to it)",
    )
    noise_level: int | None = Field(
        default=None, ge=1, le=5,
        description="How much irrelevant information? (1=very noisy, 5=all signal)",
    )
    investigation_speed: int | None = Field(
        default=None, ge=1, le=5,
        description="Did it accelerate the investigation? (1=slowed down, 5=significantly faster)",
    )
    overall_useful: bool | None = Field(
        default=None,
        description="Would you use this dashboard in a real incident?",
    )
    comment: str = Field(default="", description="Free-text feedback")
    reviewer: str = Field(default="", description="Reviewer identifier (user ID or email)")


class FeedbackResponse(BaseModel):
    """Response after submitting feedback."""

    feedback_id: int = Field(description="Auto-generated ID of the stored feedback record")
    dashboard_uid: str = Field(description="UID of the dashboard that was reviewed")
    message: str = Field(default="Feedback recorded", description="Confirmation message")


# ── Response models for untyped endpoints ──────────────────────────────────

class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(description="Server status", examples=["ok"])


class FeedbackStatsResponse(BaseModel):
    """Aggregate feedback statistics."""
    total_feedback: int = Field(description="Total number of feedback submissions")
    total_dashboards: int = Field(description="Number of distinct dashboards reviewed")
    useful_rate: float | None = Field(description="Fraction of dashboards rated as useful (0.0-1.0)")
    avg_symptom_visibility: float | None = Field(description="Average symptom visibility score (1-5)")
    avg_root_cause_support: float | None = Field(description="Average root cause support score (1-5)")
    avg_noise_level: float | None = Field(description="Average signal clarity score (1-5)")
    avg_investigation_speed: float | None = Field(description="Average investigation speed score (1-5)")


class ArchetypeSummary(BaseModel):
    """Summary of a single investigation archetype."""
    id: str = Field(description="Unique archetype identifier")
    name: str = Field(description="Human-readable archetype name")
    description: str = Field(description="What this archetype investigates")
    problem_types: list[str] = Field(description="Intent problem_type values that map to this archetype")
    panel_count: int = Field(description="Number of panels in this archetype")
    panels: list[str] = Field(description="Panel titles")
    tags: list[str] = Field(description="Archetype tags")


class ArchetypeListResponse(BaseModel):
    """List of all loaded investigation archetypes."""
    count: int = Field(description="Number of loaded archetypes")
    archetypes: list[ArchetypeSummary]


class ArchetypeReloadResponse(BaseModel):
    """Result of an archetype hot-reload operation."""
    message: str = Field(description="Status message")
    count: int = Field(description="Number of archetypes loaded")
    archetypes: list[dict[str, Any]] = Field(description="Summary of each loaded archetype")
