"""Built-in investigation archetype definitions.

Each archetype encodes known-good investigation patterns that SREs use daily.
Query templates use {placeholders} resolved from the intent + discovered labels.
"""
from __future__ import annotations

from dashforge.archetypes.schema import (
    InvestigationArchetype,
    PanelTemplate,
    QueryTemplate,
)

# ── Latency Investigation ────────────────────────────────────────────────────

LATENCY_INVESTIGATION = InvestigationArchetype(
    id="latency_investigation",
    name="Latency Investigation",
    description="Diagnose high latency / slow requests for a service",
    problem_types=["latency_investigation", "slow_requests", "high_latency", "p99_spike"],
    required_metrics=["http_request_duration_seconds", "http_requests_total"],
    tags=["latency", "performance"],
    default_timerange="1h",
    panels=[
        PanelTemplate(
            title="Request Rate",
            description="HTTP request throughput by status code",
            row="Traffic",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}])) by (status)',
                legend_format="{{status}}",
            )],
            unit="reqps",
        ),
        PanelTemplate(
            title="Error Rate (5xx)",
            description="Rate of server errors",
            row="Errors",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}])) / sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))',
                legend_format="error ratio",
            )],
            unit="percentunit",
        ),
        PanelTemplate(
            title="P50 / P95 / P99 Latency",
            description="Request duration percentiles",
            row="Latency",
            queries=[
                QueryTemplate(
                    expr='histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                    legend_format="p50",
                ),
                QueryTemplate(
                    expr='histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                    legend_format="p95",
                ),
                QueryTemplate(
                    expr='histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                    legend_format="p99",
                ),
            ],
            unit="s",
        ),
        PanelTemplate(
            title="In-Flight Requests",
            description="Current request concurrency (saturation signal)",
            row="Saturation",
            queries=[QueryTemplate(
                expr='http_requests_in_flight{{{service_filter}}}',
                legend_format="in-flight",
            )],
        ),
        PanelTemplate(
            title="CPU Usage",
            description="Container CPU consumption",
            row="Resources",
            queries=[QueryTemplate(
                expr='rate(container_cpu_usage_seconds_total{{{container_filter}}}[{rate_interval}])',
                legend_format="cpu",
            )],
            unit="s",
        ),
        PanelTemplate(
            title="Memory Usage",
            description="Container memory working set",
            row="Resources",
            queries=[QueryTemplate(
                expr='container_memory_working_set_bytes{{{container_filter}}}',
                legend_format="memory",
            )],
            unit="bytes",
        ),
    ],
)

# ── Error Spike Investigation ────────────────────────────────────────────────

ERROR_SPIKE = InvestigationArchetype(
    id="error_spike",
    name="Error Spike Investigation",
    description="Diagnose a spike in errors / 5xx responses",
    problem_types=["error_spike", "5xx_errors", "error_rate", "failed_requests"],
    required_metrics=["http_requests_total"],
    tags=["errors", "5xx"],
    default_timerange="30m",
    panels=[
        PanelTemplate(
            title="Error Rate Over Time",
            description="5xx error rate as a ratio of total requests",
            row="Errors",
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}]))',
                    legend_format="5xx rate",
                ),
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))',
                    legend_format="total rate",
                ),
            ],
            unit="reqps",
        ),
        PanelTemplate(
            title="Error Ratio",
            description="Percentage of requests returning 5xx",
            row="Errors",
            panel_type="stat",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}])) / sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))',
                legend_format="error ratio",
            )],
            unit="percentunit",
        ),
        PanelTemplate(
            title="Errors by Status Code",
            description="Breakdown of error responses by HTTP status",
            row="Errors",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}, status=~"[45].."}}[{rate_interval}])) by (status)',
                legend_format="{{status}}",
            )],
            unit="reqps",
        ),
        PanelTemplate(
            title="Errors by Path",
            description="Which endpoints are failing",
            row="Breakdown",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}])) by (path)',
                legend_format="{{path}}",
            )],
            unit="reqps",
        ),
        PanelTemplate(
            title="Request Latency During Errors",
            description="p95 latency — often spikes correlate with errors",
            row="Latency",
            queries=[QueryTemplate(
                expr='histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                legend_format="p95",
            )],
            unit="s",
        ),
        PanelTemplate(
            title="Pod Restarts",
            description="Container restarts may indicate crash loops causing errors",
            row="Resources",
            queries=[QueryTemplate(
                expr='increase(kube_pod_container_restarts_total{{{container_filter}}}[{rate_interval}])',
                legend_format="restarts",
            )],
        ),
    ],
)

# ── Golden Signals (SRE) ─────────────────────────────────────────────────────

GOLDEN_SIGNALS = InvestigationArchetype(
    id="golden_signals",
    name="SRE Golden Signals",
    description="The four golden signals: latency, traffic, errors, saturation",
    problem_types=["golden_signals", "sre_overview", "service_health", "service_overview"],
    required_metrics=["http_requests_total", "http_request_duration_seconds"],
    tags=["golden-signals", "sre"],
    default_timerange="1h",
    panels=[
        PanelTemplate(
            title="Request Throughput",
            description="Total request rate — traffic signal",
            row="Traffic",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}])) by (method)',
                legend_format="{{method}}",
            )],
            unit="reqps",
        ),
        PanelTemplate(
            title="Request Latency (p50 / p95 / p99)",
            description="Duration percentiles — latency signal",
            row="Latency",
            queries=[
                QueryTemplate(
                    expr='histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                    legend_format="p50",
                ),
                QueryTemplate(
                    expr='histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                    legend_format="p95",
                ),
                QueryTemplate(
                    expr='histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))',
                    legend_format="p99",
                ),
            ],
            unit="s",
        ),
        PanelTemplate(
            title="Error Rate (5xx / total)",
            description="Server error ratio — errors signal",
            row="Errors",
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}])) / sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))',
                    legend_format="error ratio",
                ),
            ],
            unit="percentunit",
        ),
        PanelTemplate(
            title="Errors by Status",
            description="Error breakdown by HTTP status code",
            row="Errors",
            queries=[QueryTemplate(
                expr='sum(rate(http_requests_total{{{service_filter}, status=~"[45].."}}[{rate_interval}])) by (status)',
                legend_format="{{status}}",
            )],
            unit="reqps",
        ),
        PanelTemplate(
            title="In-Flight Requests",
            description="Concurrent request count — saturation signal",
            row="Saturation",
            queries=[QueryTemplate(
                expr='http_requests_in_flight{{{service_filter}}}',
                legend_format="in-flight",
            )],
        ),
        PanelTemplate(
            title="CPU Usage",
            description="Container CPU — resource saturation",
            row="Saturation",
            queries=[QueryTemplate(
                expr='rate(container_cpu_usage_seconds_total{{{container_filter}}}[{rate_interval}])',
                legend_format="cpu",
            )],
            unit="s",
        ),
        PanelTemplate(
            title="Memory Usage",
            description="Container memory — resource saturation",
            row="Saturation",
            queries=[QueryTemplate(
                expr='container_memory_working_set_bytes{{{container_filter}}}',
                legend_format="memory",
            )],
            unit="bytes",
        ),
    ],
)

# ── Resource Saturation ──────────────────────────────────────────────────────

RESOURCE_SATURATION = InvestigationArchetype(
    id="resource_saturation",
    name="Resource Saturation Investigation",
    description="Diagnose CPU, memory, or connection pool exhaustion",
    problem_types=["resource_saturation", "high_cpu", "high_memory", "oom", "memory_leak", "cpu_throttling"],
    required_metrics=["container_cpu_usage_seconds_total", "container_memory_working_set_bytes"],
    tags=["resources", "saturation"],
    default_timerange="1h",
    panels=[
        PanelTemplate(
            title="CPU Usage",
            description="Container CPU consumption rate",
            row="CPU",
            queries=[QueryTemplate(
                expr='rate(container_cpu_usage_seconds_total{{{container_filter}}}[{rate_interval}])',
                legend_format="{{container}}",
            )],
            unit="s",
        ),
        PanelTemplate(
            title="Memory Working Set",
            description="Active memory usage",
            row="Memory",
            queries=[QueryTemplate(
                expr='container_memory_working_set_bytes{{{container_filter}}}',
                legend_format="{{container}}",
            )],
            unit="bytes",
        ),
        PanelTemplate(
            title="Database Connections",
            description="Active database connection pool usage",
            row="Connections",
            queries=[QueryTemplate(
                expr='db_connections_active{{{service_filter}}}',
                legend_format="active",
            )],
        ),
        PanelTemplate(
            title="DB Query Latency",
            description="Average database query duration",
            row="Connections",
            queries=[QueryTemplate(
                expr='rate(db_query_duration_seconds_sum{{{service_filter}}}[{rate_interval}]) / rate(db_query_duration_seconds_count{{{service_filter}}}[{rate_interval}])',
                legend_format="avg query time",
            )],
            unit="s",
        ),
        PanelTemplate(
            title="In-Flight Requests",
            description="Concurrency pressure",
            row="Saturation",
            queries=[QueryTemplate(
                expr='http_requests_in_flight{{{service_filter}}}',
                legend_format="in-flight",
            )],
        ),
        PanelTemplate(
            title="Pod Restarts",
            description="OOM kills and crash loops",
            row="Stability",
            queries=[QueryTemplate(
                expr='increase(kube_pod_container_restarts_total{{{container_filter}}}[{rate_interval}])',
                legend_format="restarts",
            )],
        ),
    ],
)

# ── Registry ─────────────────────────────────────────────────────────────────

ALL_ARCHETYPES: list[InvestigationArchetype] = [
    LATENCY_INVESTIGATION,
    ERROR_SPIKE,
    GOLDEN_SIGNALS,
    RESOURCE_SATURATION,
]

# Build lookup: problem_type → archetype
_ARCHETYPE_BY_PROBLEM: dict[str, InvestigationArchetype] = {}
for _arch in ALL_ARCHETYPES:
    for _pt in _arch.problem_types:
        _ARCHETYPE_BY_PROBLEM[_pt] = _arch


def get_archetype(problem_type: str) -> InvestigationArchetype | None:
    """Look up an archetype by problem_type. Returns None if no match."""
    return _ARCHETYPE_BY_PROBLEM.get(problem_type)


def list_problem_types() -> list[str]:
    """Return all known problem_type values."""
    return sorted(_ARCHETYPE_BY_PROBLEM.keys())
