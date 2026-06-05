"""Built-in investigation archetype definitions.

Each archetype encodes known-good investigation patterns that SREs use daily.
Query templates use {placeholders} resolved from the intent + discovered labels.

Archetypes are loaded from ``archetypes.yaml`` if it exists (project root or
``DASHFORGE_ARCHETYPES_PATH`` env var).  Otherwise, the hardcoded definitions
below are used as the default.  This lets engineers edit templates without
touching Python code — just edit the YAML and restart.
"""

from __future__ import annotations

import os
from pathlib import Path

import structlog

from dashforge.archetypes.schema import (
    InvestigationArchetype,
    PanelTemplate,
    QueryTemplate,
)

logger = structlog.get_logger()

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
            queries=[
                QueryTemplate(
                    expr="sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}])) by (status)",
                    legend_format="{{status}}",
                )
            ],
            unit="reqps",
        ),
        PanelTemplate(
            title="Error Rate (5xx)",
            description="Rate of server errors",
            row="Errors",
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}]))'
                    " / sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))",
                    legend_format="error ratio",
                )
            ],
            unit="percentunit",
        ),
        PanelTemplate(
            title="P50 / P95 / P99 Latency",
            description="Request duration percentiles",
            row="Latency",
            queries=[
                QueryTemplate(
                    expr="histogram_quantile(0.50, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
                    legend_format="p50",
                ),
                QueryTemplate(
                    expr="histogram_quantile(0.95, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
                    legend_format="p95",
                ),
                QueryTemplate(
                    expr="histogram_quantile(0.99, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
                    legend_format="p99",
                ),
            ],
            unit="s",
        ),
        PanelTemplate(
            title="In-Flight Requests",
            description="Current request concurrency (saturation signal)",
            row="Saturation",
            queries=[
                QueryTemplate(
                    expr="http_requests_in_flight{{{service_filter}}}",
                    legend_format="in-flight",
                )
            ],
        ),
        PanelTemplate(
            title="CPU Usage",
            description="Container CPU consumption",
            row="Resources",
            queries=[
                QueryTemplate(
                    expr="rate(container_cpu_usage_seconds_total{{{container_filter}}}[{rate_interval}])",
                    legend_format="cpu",
                )
            ],
            unit="s",
        ),
        PanelTemplate(
            title="Memory Usage",
            description="Container memory working set",
            row="Resources",
            queries=[
                QueryTemplate(
                    expr="container_memory_working_set_bytes{{{container_filter}}}",
                    legend_format="memory",
                )
            ],
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
                    expr="sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))",
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
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}]))'
                    " / sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))",
                    legend_format="error ratio",
                )
            ],
            unit="percentunit",
        ),
        PanelTemplate(
            title="Errors by Status Code",
            description="Breakdown of error responses by HTTP status",
            row="Errors",
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"[45].."}}'
                    "[{rate_interval}])) by (status)",
                    legend_format="{{status}}",
                )
            ],
            unit="reqps",
        ),
        PanelTemplate(
            title="Errors by Path",
            description="Which endpoints are failing",
            row="Breakdown",
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}])) by (path)',
                    legend_format="{{path}}",
                )
            ],
            unit="reqps",
        ),
        PanelTemplate(
            title="Request Latency During Errors",
            description="p95 latency — often spikes correlate with errors",
            row="Latency",
            queries=[
                QueryTemplate(
                    expr="histogram_quantile(0.95, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
                    legend_format="p95",
                )
            ],
            unit="s",
        ),
        PanelTemplate(
            title="Pod Restarts",
            description="Container restarts may indicate crash loops causing errors",
            row="Resources",
            queries=[
                QueryTemplate(
                    expr="increase(kube_pod_container_restarts_total{{{container_filter}}}[{rate_interval}])",
                    legend_format="restarts",
                )
            ],
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
            queries=[
                QueryTemplate(
                    expr="sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}])) by (method)",
                    legend_format="{{method}}",
                )
            ],
            unit="reqps",
        ),
        PanelTemplate(
            title="Request Latency (p50 / p95 / p99)",
            description="Duration percentiles — latency signal",
            row="Latency",
            queries=[
                QueryTemplate(
                    expr="histogram_quantile(0.50, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
                    legend_format="p50",
                ),
                QueryTemplate(
                    expr="histogram_quantile(0.95, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
                    legend_format="p95",
                ),
                QueryTemplate(
                    expr="histogram_quantile(0.99, sum(rate("
                    "http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le))",
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
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"5.."}}[{rate_interval}]))'
                    " / sum(rate(http_requests_total{{{service_filter}}}[{rate_interval}]))",
                    legend_format="error ratio",
                ),
            ],
            unit="percentunit",
        ),
        PanelTemplate(
            title="Errors by Status",
            description="Error breakdown by HTTP status code",
            row="Errors",
            queries=[
                QueryTemplate(
                    expr='sum(rate(http_requests_total{{{service_filter}, status=~"[45].."}}'
                    "[{rate_interval}])) by (status)",
                    legend_format="{{status}}",
                )
            ],
            unit="reqps",
        ),
        PanelTemplate(
            title="In-Flight Requests",
            description="Concurrent request count — saturation signal",
            row="Saturation",
            queries=[
                QueryTemplate(
                    expr="http_requests_in_flight{{{service_filter}}}",
                    legend_format="in-flight",
                )
            ],
        ),
        PanelTemplate(
            title="CPU Usage",
            description="Container CPU — resource saturation",
            row="Saturation",
            queries=[
                QueryTemplate(
                    expr="rate(container_cpu_usage_seconds_total{{{container_filter}}}[{rate_interval}])",
                    legend_format="cpu",
                )
            ],
            unit="s",
        ),
        PanelTemplate(
            title="Memory Usage",
            description="Container memory — resource saturation",
            row="Saturation",
            queries=[
                QueryTemplate(
                    expr="container_memory_working_set_bytes{{{container_filter}}}",
                    legend_format="memory",
                )
            ],
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
            queries=[
                QueryTemplate(
                    expr="rate(container_cpu_usage_seconds_total{{{container_filter}}}[{rate_interval}])",
                    legend_format="{{container}}",
                )
            ],
            unit="s",
        ),
        PanelTemplate(
            title="Memory Working Set",
            description="Active memory usage",
            row="Memory",
            queries=[
                QueryTemplate(
                    expr="container_memory_working_set_bytes{{{container_filter}}}",
                    legend_format="{{container}}",
                )
            ],
            unit="bytes",
        ),
        PanelTemplate(
            title="Database Connections",
            description="Active database connection pool usage",
            row="Connections",
            queries=[
                QueryTemplate(
                    expr="db_connections_active{{{service_filter}}}",
                    legend_format="active",
                )
            ],
        ),
        PanelTemplate(
            title="DB Query Latency",
            description="Average database query duration",
            row="Connections",
            queries=[
                QueryTemplate(
                    expr="rate(db_query_duration_seconds_sum{{{service_filter}}}[{rate_interval}])"
                    " / rate(db_query_duration_seconds_count{{{service_filter}}}[{rate_interval}])",
                    legend_format="avg query time",
                )
            ],
            unit="s",
        ),
        PanelTemplate(
            title="In-Flight Requests",
            description="Concurrency pressure",
            row="Saturation",
            queries=[
                QueryTemplate(
                    expr="http_requests_in_flight{{{service_filter}}}",
                    legend_format="in-flight",
                )
            ],
        ),
        PanelTemplate(
            title="Pod Restarts",
            description="OOM kills and crash loops",
            row="Stability",
            queries=[
                QueryTemplate(
                    expr="increase(kube_pod_container_restarts_total{{{container_filter}}}[{rate_interval}])",
                    legend_format="restarts",
                )
            ],
        ),
    ],
)

# ── YAML loader ──────────────────────────────────────────────────────────────

_DEFAULT_YAML_PATHS = [
    Path(__file__).resolve().parent.parent.parent / "archetypes.yaml",  # project root
    Path("archetypes.yaml"),  # cwd
]


def _load_archetypes_from_yaml(path: Path) -> list[InvestigationArchetype]:
    """Parse archetypes.yaml into InvestigationArchetype objects."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f)

    archetypes = []
    for entry in data.get("archetypes", []):
        panels = []
        for p in entry.get("panels", []):
            queries = [
                QueryTemplate(
                    expr=q["expr"],
                    legend_format=q.get("legend_format", ""),
                    datasource_type=q.get("datasource_type", "prometheus"),
                )
                for q in p.get("queries", [])
            ]
            panels.append(
                PanelTemplate(
                    title=p["title"],
                    description=p.get("description", ""),
                    panel_type=p.get("panel_type", "timeseries"),
                    row=p.get("row", ""),
                    queries=queries,
                    unit=p.get("unit", ""),
                )
            )
        archetypes.append(
            InvestigationArchetype(
                id=entry["id"],
                name=entry["name"],
                description=entry.get("description", ""),
                problem_types=entry.get("problem_types", []),
                required_metrics=entry.get("required_metrics", []),
                required_signals=entry.get("required_signals", []),
                signal_bindings=entry.get("signal_bindings", {}),
                panels=panels,
                tags=entry.get("tags", []),
                default_timerange=entry.get("default_timerange", "1h"),
            )
        )
    return archetypes


def _build_registry() -> tuple[list[InvestigationArchetype], dict[str, InvestigationArchetype]]:
    """Build archetype registry. YAML first, Python fallback."""
    yaml_path = os.environ.get("DASHFORGE_ARCHETYPES_PATH")
    if yaml_path:
        candidates = [Path(yaml_path)]
    else:
        candidates = _DEFAULT_YAML_PATHS

    for path in candidates:
        if path.is_file():
            try:
                archetypes = _load_archetypes_from_yaml(path)
                by_problem: dict[str, InvestigationArchetype] = {}
                for arch in archetypes:
                    for pt in arch.problem_types:
                        by_problem[pt] = arch
                logger.info(
                    "archetypes_loaded_from_yaml",
                    path=str(path),
                    count=len(archetypes),
                    problem_types=len(by_problem),
                )
                return archetypes, by_problem
            except Exception as e:
                logger.warning("archetypes_yaml_load_failed", path=str(path), error=str(e))

    # Fallback to hardcoded Python definitions
    archetypes = [LATENCY_INVESTIGATION, ERROR_SPIKE, GOLDEN_SIGNALS, RESOURCE_SATURATION]
    by_problem = {}
    for arch in archetypes:
        for pt in arch.problem_types:
            by_problem[pt] = arch
    logger.info("archetypes_loaded_from_python", count=len(archetypes))
    return archetypes, by_problem


ALL_ARCHETYPES, _ARCHETYPE_BY_PROBLEM = _build_registry()


def reload_archetypes() -> None:
    """Hot-reload archetypes from YAML. Call after editing archetypes.yaml."""
    global ALL_ARCHETYPES, _ARCHETYPE_BY_PROBLEM
    ALL_ARCHETYPES, _ARCHETYPE_BY_PROBLEM = _build_registry()
    logger.info("archetypes_reloaded", count=len(ALL_ARCHETYPES))


def get_archetype(problem_type: str) -> InvestigationArchetype | None:
    """Look up an archetype by problem_type. Returns None if no match."""
    return _ARCHETYPE_BY_PROBLEM.get(problem_type)


def get_archetypes_by_confidence(
    archetype_matches: list,
    min_confidence: float = 0.3,
) -> list[tuple[InvestigationArchetype, float]]:
    """Resolve multi-label archetypes to templates above *min_confidence*.

    Parameters
    ----------
    archetype_matches : list[ArchetypeMatch]
        From ``intent.archetypes`` — already sorted by confidence desc.
    min_confidence : float
        Minimum confidence to include (default 0.3).

    Returns
    -------
    list[tuple[InvestigationArchetype, float]]
        Matching (archetype_template, confidence) pairs, highest first.
        Deduplicates: if two problem_types map to the same template,
        only the higher-confidence entry is kept.
    """
    seen_ids: set[str] = set()
    results: list[tuple[InvestigationArchetype, float]] = []

    for match in archetype_matches:
        if match.confidence < min_confidence:
            continue
        arch = _ARCHETYPE_BY_PROBLEM.get(match.type)
        if arch is None or arch.id in seen_ids:
            continue
        seen_ids.add(arch.id)
        results.append((arch, match.confidence))

    return results


def list_problem_types() -> list[str]:
    """Return all known problem_type values."""
    return sorted(_ARCHETYPE_BY_PROBLEM.keys())
