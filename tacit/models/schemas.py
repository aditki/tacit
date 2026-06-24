from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

# ── Intent ───────────────────────────────────────────────────────────────────


class SignalType(StrEnum):
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
    signals: list[SignalType] = Field(
        default_factory=lambda: [SignalType.METRICS],
        description="Signal types to explore",
    )
    keywords: list[str] = Field(
        default_factory=list,
        description="Key terms for metric search, e.g. 'latency', 'error_rate', 'cpu'",
    )
    timerange: str = Field(default="1h", description="Suggested lookback window")
    problem_type: str = Field(
        default="general",
        description="Investigation archetype classification. One of: "
        "latency_investigation, slow_requests, high_latency, p99_spike, "
        "error_spike, 5xx_errors, error_rate, failed_requests, "
        "golden_signals, sre_overview, service_health, service_overview, "
        "resource_saturation, high_cpu, high_memory, oom, memory_leak, "
        "cpu_throttling, general",
    )
    archetypes: list[ArchetypeMatch] = Field(
        default_factory=list,
        description="Multi-label archetype classifications ordered by confidence "
        "(highest first). Used as retrieval priors for metric ranking "
        "and template blending.",
    )
    keyword_evidence: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Scored operational-synonym evidence with provenance: "
        "[{keyword, score, tier, source}]. Conventional terms are also folded "
        "into 'keywords'; colloquial (low-score) terms are advisory only and "
        "must be confirmed against live coverage / learned archetypes before use.",
    )


# ── Context Enrichment ───────────────────────────────────────────────────────


class ContextChunk(BaseModel):
    """A chunk of context retrieved from an external knowledge base."""

    content: str = Field(description="Retrieved text (runbook excerpt, wiki snippet, service doc, etc.)")
    source: str = Field(
        default="",
        description="Origin identifier, e.g. 'runbook:checkout-service', 'wiki:incident-playbook'",
    )
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
    query_language: str = Field(
        description="Query language: promql, logql, cloudwatch, elasticsearch, graphite, influxql, flux, signalflow"
    )
    namespace: str = Field(
        default="",
        description="Metric namespace or group (e.g. 'AWS/ELB', 'node_exporter', 'kube-state-metrics')",
    )
    dimensions: list[str] = Field(default_factory=list, description="Available dimensions / label names")
    unit: str = Field(
        default="",
        description="Metric unit from datasource metadata (e.g. 'seconds', 'bytes', 'percent'). Empty if unknown.",
    )
    metric_type: str = Field(
        default="",
        description="Metric type from datasource metadata: counter, gauge, histogram, summary. Empty if unknown.",
    )


class QueryTarget(BaseModel):
    """Resolved datasource identity for a query.

    Keep datasource UID, datasource type, and query language together so code
    paths cannot accidentally update one without the others.
    """

    datasource_uid: str
    datasource_type: str
    query_language: str
    datasource_name: str = ""

    @classmethod
    def from_metric(cls, metric: MetricEntry) -> QueryTarget:
        return cls(
            datasource_uid=metric.datasource_uid,
            datasource_type=metric.datasource_type,
            query_language=metric.query_language,
            datasource_name=metric.datasource_name,
        )


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

    expr: str = Field(
        description="Query expression (PromQL, LogQL, SignalFlow, Lucene, metric name for CloudWatch, etc.)"
    )
    legend_format: str = Field(default="{{instance}}", description="Legend template")
    datasource_uid: str
    datasource_type: str = "prometheus"
    query_language: str = ""
    # CloudWatch-specific fields (only set when datasource_type='cloudwatch')
    cloudwatch_namespace: str = Field(default="", description="AWS CloudWatch namespace, e.g. 'AWS/ApplicationELB'")
    cloudwatch_stat: str = Field(default="", description="CloudWatch statistic: Sum, Average, p99, etc.")
    cloudwatch_dimensions: dict[str, str | list[str]] = Field(
        default_factory=dict,
        description=(
            "CloudWatch dimensions, e.g. {'LoadBalancer': '*'} " "or {'AvailabilityZone': ['us-east-1a', 'us-east-1b']}"
        ),
    )
    cloudwatch_region: str = Field(default="", description="AWS region for this CloudWatch query, e.g. 'us-east-1'")
    validation_status: str = Field(default="", description="Validation verdict for this query, e.g. ok/skipped")
    validation_has_data: bool = Field(default=False, description="Whether validation proved this query returned data")


class PanelSpec(BaseModel):
    """Specification for one Grafana panel."""

    title: str
    description: str = ""
    panel_type: str = Field(
        default="timeseries",
        description="Grafana panel type: timeseries, stat, gauge, table, logs …",
    )
    queries: list[PanelQuery]
    unit: str = Field(default="", description="Grafana unit id, e.g. 'percentunit', 's', 'bytes'")
    thresholds: list[dict[str, Any]] = Field(default_factory=list)
    source_archetype: str = Field(default="", description="Archetype id that compiled this panel, when known")
    row: str = Field(
        default="",
        description="Optional row/section name for grouping, e.g. 'Latency', 'Traffic'. Leave empty for no grouping.",
    )


class DashboardSpec(BaseModel):
    """Full spec handed to the Dashboard Builder."""

    title: str
    tags: list[str] = Field(default_factory=list)
    timerange: str = "1h"
    panels: list[PanelSpec]


# ── Evidence model ───────────────────────────────────────────────────────────


class EvidenceResolutionStatus(StrEnum):
    RESOLVED = "resolved"
    UNRESOLVED = "unresolved"
    UNKNOWN = "unknown"


class EvidenceObservationOutcome(StrEnum):
    SUPPORTED_OBSERVATION = "SUPPORTED_OBSERVATION"
    MISSING_EVIDENCE = "MISSING_EVIDENCE"
    AMBIGUOUS_EVIDENCE = "AMBIGUOUS_EVIDENCE"
    NEGATIVE_EVIDENCE = "NEGATIVE_EVIDENCE"
    UNSUPPORTED_CAUSE = "UNSUPPORTED_CAUSE"


class EvidenceLifecycleStatus(StrEnum):
    REQUIRED = "required"
    PRIMARY_RESOLVED = "primary_resolved"
    PRIMARY_UNRESOLVED = "primary_unresolved"
    GAP_RESOLVED = "gap_resolved"
    GAP_UNRESOLVED = "gap_unresolved"
    SUPPORTED_OBSERVATION = "supported_observation"
    MISSING_EVIDENCE = "missing_evidence"
    AMBIGUOUS_EVIDENCE = "ambiguous_evidence"
    NEGATIVE_EVIDENCE = "negative_evidence"
    UNSUPPORTED_CAUSE = "unsupported_cause"


class EvidenceRequirement(BaseModel):
    """A signal or metric the investigation needs before it can claim support."""

    id: str = Field(description="Stable requirement id within one investigation")
    evidence_type: str = Field(description="semantic_signal or required_metric")
    signal_type: str = Field(default="", description="Semantic signal family, when known")
    default_metric: str = Field(default="", description="Canonical/template metric name requested by an archetype")
    priority: str = Field(default="critical", description="critical or supporting")
    service_scope: list[str] = Field(default_factory=list, description="Requested service context")
    source: str = Field(default="", description="Where the requirement came from, e.g. archetype id")


class EvidenceResolution(BaseModel):
    """How a requirement resolved, or why it abstained."""

    requirement_id: str
    status: EvidenceResolutionStatus = Field(description="resolved, unresolved, or unknown")
    reason_code: str
    metric: str = ""
    datasource_uid: str = ""
    datasource_type: str = ""
    query_language: str = ""
    semantic_score: float = 0.0
    ownership_score: float = 0.0


class EvidenceObservation(BaseModel):
    """Whether resolved evidence survived into a validated query/panel."""

    requirement_id: str
    outcome: EvidenceObservationOutcome = Field(
        default=EvidenceObservationOutcome.MISSING_EVIDENCE,
        description="Explicit evidence-state outcome for this observation.",
    )
    resolution_metric: str = ""
    panel_title: str = ""
    query: str = ""
    datasource_uid: str = ""
    valid_query: bool = False
    non_empty: bool = False
    survived: bool = False
    rejection_reason: str = ""

    @model_validator(mode="before")
    @classmethod
    def infer_legacy_outcome(cls, data: Any) -> Any:
        """Preserve pre-outcome callers that used non_empty as the support bit."""
        if isinstance(data, dict) and "outcome" not in data and data.get("non_empty"):
            data = {**data, "outcome": EvidenceObservationOutcome.SUPPORTED_OBSERVATION}
        return data


class EvidenceRecord(BaseModel):
    """One requirement's full lifecycle through resolution and observation."""

    requirement: EvidenceRequirement
    primary_resolution: EvidenceResolution | None = None
    gap_resolution: EvidenceResolution | None = None
    observation: EvidenceObservation | None = None
    final_status: EvidenceLifecycleStatus = EvidenceLifecycleStatus.REQUIRED


# ── Culprit ranking ──────────────────────────────────────────────────────────


class CulpritRankingMode(StrEnum):
    CONTEXTUAL = "contextual"
    TELEMETRY_EVIDENCED = "telemetry_evidenced"


class CulpritCandidate(BaseModel):
    """One ranked suspect.

    This is a suspect ranking, not a root-cause assertion. Runtime evidence is
    kept separate from contextual reasons so callers can see whether the
    ranking crossed from operational context into validated telemetry.
    """

    rank: int
    suspect: str
    suspect_type: str = Field(default="unknown", description="service, datastore, cache, queue, resource, or unknown")
    score: float = Field(ge=0.0, le=1.0)
    confidence: str = Field(default="low", description="low, medium, or high")
    contextual_reasons: list[str] = Field(default_factory=list)
    runtime_evidence: list[str] = Field(default_factory=list)
    missing_evidence: list[str] = Field(default_factory=list)


class CulpritRanking(BaseModel):
    """Reason-coded suspect ranking for an investigation."""

    mode: CulpritRankingMode = CulpritRankingMode.CONTEXTUAL
    abstained: bool = True
    abstention_reason: str = ""
    candidates: list[CulpritCandidate] = Field(default_factory=list)
    evidence_sources: list[str] = Field(default_factory=list)
    telemetry_status: str = Field(default="not_evidenced")


# ── Pipeline request / response ──────────────────────────────────────────────


class DashRequest(BaseModel):
    """Inbound request from Slack (or HTTP)."""

    prompt: str = Field(description="Natural-language description of the dashboard you need")
    channel_id: str = Field(default="", description="Slack channel ID (set automatically by Slack integration)")
    user_id: str = Field(default="", description="User identifier for provenance tracking")
    thread_ts: str = Field(default="", description="Slack thread timestamp (set automatically by Slack integration)")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "prompt": "High 5xx error rate on checkout-service in the last 30 minutes",
                    "user_id": "web-ui",
                }
            ]
        }
    }


class DashResponse(BaseModel):
    """Result of a successful dashboard generation."""

    dashboard_url: str = Field(description="Full Grafana URL to the published dashboard")
    dashboard_uid: str = Field(description="Unique Grafana dashboard UID")
    panel_count: int = Field(description="Number of panels in the generated dashboard")
    summary: str = Field(description="Human-readable summary of what was generated")
    signalfx_url: str = Field(default="", description="SignalFx dashboard URL (when signalfx_enabled)")
    signalfx_dashboard_id: str = Field(default="", description="SignalFx dashboard ID (when signalfx_enabled)")
    culprit_ranking: CulpritRanking | None = Field(
        default=None,
        description="Reason-coded suspect ranking, when enough investigation context exists.",
    )


# ── Feedback ────────────────────────────────────────────────────────────────


class FeedbackRequest(BaseModel):
    """Human evaluation of a generated dashboard."""

    dashboard_uid: str = Field(description="UID of the dashboard being reviewed")
    symptom_visibility: int | None = Field(
        default=None,
        ge=1,
        le=5,
        description="How well did the dashboard surface the symptom? (1=not at all, 5=immediately obvious)",
    )
    root_cause_support: int | None = Field(
        default=None,
        ge=1,
        le=5,
        description="Did the dashboard help identify root cause? (1=no help, 5=pointed directly to it)",
    )
    noise_level: int | None = Field(
        default=None,
        ge=1,
        le=5,
        description="How much irrelevant information? (1=very noisy, 5=all signal)",
    )
    investigation_speed: int | None = Field(
        default=None,
        ge=1,
        le=5,
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


# ── Signals / Learning ───────────────────────────────────────────────────────


class MetricPattern(BaseModel):
    """A single signal→metric mapping taught via the API."""

    model_config = {"extra": "forbid"}

    pattern: str = Field(description="Metric name or pattern this signal maps to")
    confidence: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Confidence score (0.0–1.0). Values like 90 (meant 0.9) are rejected.",
    )

    @field_validator("pattern")
    @classmethod
    def _strip_pattern(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("pattern must not be empty")
        return v


class TeachSignalRequest(BaseModel):
    """Request body for ``POST /api/v1/signals/teach``.

    Strictly validated: confidence bounds, non-empty identifiers, and unknown
    fields are all enforced here rather than in the endpoint body.
    """

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "examples": [
                {
                    "signal_type": "queue_depth",
                    "metric_patterns": [
                        {"pattern": "kafka_consumer_lag", "confidence": 0.9},
                        {"pattern": "inflight_messages", "confidence": 0.8},
                    ],
                    "description": "Queue pressure metrics for our Kafka setup",
                    "category": "saturation",
                    "services": ["payment-service"],
                }
            ]
        },
    }

    signal_type: str = Field(description="Organization-specific signal name, e.g. 'queue_depth'")
    metric_patterns: list[MetricPattern] = Field(
        default_factory=list, description="Metric patterns this signal maps to"
    )
    description: str = Field(default="", description="Human-readable description of the signal")
    category: str = Field(default="", description="Signal category, e.g. 'saturation'")
    unit: str = Field(default="", description="Unit hint for the signal")
    # Scope fields use None (omitted) vs [] (explicit clear) vs [..] (union):
    # omitting a field leaves existing scope unchanged on re-teach; an empty
    # list clears it (makes the mapping global); values union with existing.
    services: list[str] | None = Field(default=None, description="Context: services this mapping applies to")
    datasource_types: list[str] | None = Field(
        default=None, description="Context: datasource types this mapping applies to"
    )
    environments: list[str] | None = Field(default=None, description="Context: environments this mapping applies to")
    taught_by: str = Field(default="api", description="Provenance: who taught this mapping")

    @field_validator("signal_type")
    @classmethod
    def _strip_signal_type(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("signal_type must not be empty")
        return v


class TeachSignalResponse(BaseModel):
    """Result of teaching a signal mapping."""

    signal_type: str
    mappings_created: int
    message: str


class LearnDashboardRequest(BaseModel):
    """Request body for ``POST /api/v1/learn/dashboard``."""

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {"examples": [{"dashboard_uid": "abc123", "backend": "grafana", "auto_approve": False}]},
    }

    dashboard_uid: str = Field(description="Dashboard UID/ID to ingest (interpretation is backend-specific)")
    backend: str = Field(
        default="", description="Backend to fetch from: 'grafana' or 'signalfx' (default: first active)"
    )
    auto_approve: bool = Field(
        default=False,
        description="If true, approve and create signal mappings immediately; otherwise store as 'pending'.",
    )

    @field_validator("dashboard_uid")
    @classmethod
    def _strip_uid(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("dashboard_uid must not be empty")
        return v

    @field_validator("auto_approve", mode="before")
    @classmethod
    def _parse_auto_approve(cls, v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            normalized = v.strip().lower()
            if normalized == "true":
                return True
            if normalized == "false":
                return False
        raise ValueError("auto_approve must be a boolean or the string 'true'/'false'")


class LearnDashboardUploadRequest(BaseModel):
    """Request body for ``POST /api/v1/learn/dashboard/json``."""

    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "examples": [
                {
                    "vendor": "grafana",
                    "source_name": "checkout-prod.json",
                    "auto_approve": False,
                    "dashboard": {"dashboard": {"uid": "checkout-prod", "title": "Checkout Prod", "panels": []}},
                }
            ]
        },
    }

    vendor: str = Field(default="grafana", description="Uploaded dashboard vendor: 'grafana' or 'signalfx'.")
    source_name: str = Field(default="", description="Optional filename or provenance label for the uploaded JSON.")
    dashboard: dict[str, Any] = Field(description="Exported dashboard JSON document.")
    auto_approve: bool = Field(
        default=False,
        description="If true, approve and create signal mappings immediately; otherwise store as 'pending'.",
    )

    @field_validator("vendor")
    @classmethod
    def _strip_vendor(cls, v: str) -> str:
        v = v.strip().lower()
        if not v:
            raise ValueError("vendor must not be empty")
        return v

    @field_validator("source_name")
    @classmethod
    def _strip_source_name(cls, v: str) -> str:
        return v.strip()

    @field_validator("auto_approve", mode="before")
    @classmethod
    def _parse_auto_approve(cls, v: Any) -> bool:
        return LearnDashboardRequest._parse_auto_approve(v)
