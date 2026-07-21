"""Built-in investigation archetype definitions.

Each archetype encodes known-good investigation patterns that SREs use daily.
Query templates use {placeholders} resolved from the intent + discovered labels.

Archetypes are loaded from ``TACIT_ARCHETYPES_PATH`` or a local
``archetypes.yaml`` override, then from packaged data.  Otherwise, the
hardcoded definitions below are used as the default.  This lets engineers edit
templates without touching Python code — just edit the YAML and restart.
"""

from __future__ import annotations

import os
import re
from importlib.resources import files
from pathlib import Path
from typing import Any

import structlog

from tacit.archetypes.schema import (
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

_GENERATED_TAGS = {"learned", "auto-generated"}


def _is_generated_entry(entry: object) -> bool:
    if not isinstance(entry, dict):
        return False
    origin = str(entry.get("origin", ""))
    tags = {str(tag) for tag in entry.get("tags", []) or []}
    return origin == "generated_experimental" or bool(_GENERATED_TAGS & tags)


def _is_generated_archetype(archetype: InvestigationArchetype) -> bool:
    return bool(_GENERATED_TAGS & set(archetype.tags))


def _load_archetypes_from_yaml(path: Path) -> list[InvestigationArchetype]:
    """Parse archetypes.yaml into InvestigationArchetype objects."""
    import yaml

    with open(path) as f:
        data = yaml.safe_load(f)

    return _load_archetypes_from_data(data)


def _load_archetypes_from_data(data: object) -> list[InvestigationArchetype]:
    """Parse archetype YAML data into InvestigationArchetype objects."""
    if not isinstance(data, dict):
        raise ValueError("archetype YAML must contain a mapping with an 'archetypes' list")
    raw_archetypes = data.get("archetypes")
    if not isinstance(raw_archetypes, list) or not raw_archetypes:
        raise ValueError("archetype YAML must define at least one archetype")

    archetypes = []
    generated_ids: list[str] = []
    for entry in raw_archetypes:
        if _is_generated_entry(entry):
            generated_ids.append(str(entry.get("id", "unknown")))
            continue
        panels = []
        for p in entry.get("panels", []):
            queries = [
                QueryTemplate(
                    expr=q.get("expr") or q.get("metric_name", ""),
                    legend_format=q.get("legend_format", ""),
                    query_language=q.get("query_language", "promql"),
                    datasource_type=q.get("datasource_type", "prometheus"),
                    cloudwatch_namespace=q.get("cloudwatch_namespace", q.get("namespace", "")),
                    cloudwatch_stat=q.get("cloudwatch_stat", q.get("stat", "")),
                    cloudwatch_dimensions=q.get("cloudwatch_dimensions", q.get("dimensions", {})),
                    cloudwatch_region=q.get("cloudwatch_region", q.get("region", "")),
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
    if generated_ids:
        logger.warning(
            "generated_archetypes_excluded_from_curated_registry",
            count=len(generated_ids),
            archetype_ids=sorted(generated_ids),
        )
    if not archetypes:
        raise ValueError("archetype YAML did not load any archetypes")
    return archetypes


def _load_packaged_archetypes() -> list[InvestigationArchetype] | None:
    """Load archetypes from package data for wheels and PyInstaller builds."""
    import yaml

    resource = files("tacit.data").joinpath("archetypes.yaml")
    if not resource.is_file():
        return None

    with resource.open() as f:
        data = yaml.safe_load(f)
    return _load_archetypes_from_data(data)


def _build_registry() -> tuple[list[InvestigationArchetype], dict[str, InvestigationArchetype]]:
    """Build archetype registry. YAML first, Python fallback."""
    yaml_path = os.environ.get("TACIT_ARCHETYPES_PATH")
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

    try:
        packaged_archetypes = _load_packaged_archetypes()
        if packaged_archetypes is not None:
            by_problem = {}
            for arch in packaged_archetypes:
                for pt in arch.problem_types:
                    by_problem[pt] = arch
            logger.info(
                "archetypes_loaded_from_package",
                path="package:tacit.data/archetypes.yaml",
                count=len(packaged_archetypes),
                problem_types=len(by_problem),
            )
            return packaged_archetypes, by_problem
    except Exception as e:
        logger.warning(
            "archetypes_package_yaml_load_failed",
            path="package:tacit.data/archetypes.yaml",
            error=str(e),
        )

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
    """Hot-reload curated archetypes after editing the configured YAML override."""
    global ALL_ARCHETYPES, _ARCHETYPE_BY_PROBLEM
    ALL_ARCHETYPES, _ARCHETYPE_BY_PROBLEM = _build_registry()
    logger.info("archetypes_reloaded", count=len(ALL_ARCHETYPES))


def append_archetype_to_yaml(archetype_yaml: str, path: Path | None = None) -> Path | None:
    """Merge curated operator-authored archetypes into an override, then reload.

    De-dupes by archetype ``id`` (an existing id is overwritten). Returns the
    path written, or ``None`` if no writable override is configured — we never
    write into packaged read-only archetypes. Generated artifacts are rejected;
    they belong in the separate quarantine store.
    """
    import yaml

    env_path = os.environ.get("TACIT_ARCHETYPES_PATH")
    target = path or (Path(env_path) if env_path else None)
    if target is None:
        return None

    new_doc = yaml.safe_load(archetype_yaml) or {}
    new_items = new_doc.get("archetypes", []) or []
    if not new_items:
        return None
    generated_ids = [str(item.get("id", "unknown")) for item in new_items if _is_generated_entry(item)]
    if generated_ids:
        raise ValueError("Generated archetypes cannot enter the curated registry: " + ", ".join(sorted(generated_ids)))

    def _seed_archetypes() -> list[dict]:
        return [arch.model_dump(mode="python") for arch in ALL_ARCHETYPES]

    existing = yaml.safe_load(target.read_text()) if target.is_file() else None
    if not isinstance(existing, dict):
        existing = {}
    items = existing.get("archetypes")
    if not isinstance(items, list) or not items:
        items = _seed_archetypes()
    else:
        items = list(items)
    by_id = {a.get("id"): i for i, a in enumerate(items) if isinstance(a, dict)}
    for arch in new_items:
        aid = arch.get("id")
        if aid in by_id:
            items[by_id[aid]] = arch  # overwrite same id
        else:
            by_id[aid] = len(items)
            items.append(arch)
    existing["archetypes"] = items

    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(existing, sort_keys=False, width=120))
    reload_archetypes()
    logger.info("archetype_registered_from_ingest", path=str(target), archetypes=len(new_items))
    return target


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


def get_archetypes_by_learning_context(
    intent: Any,
    catalog: list[Any],
    *,
    min_confidence: float = 0.35,
    exclude_ids: set[str] | None = None,
) -> list[tuple[InvestigationArchetype, float]]:
    """Retrieve curated archetypes by deterministic signal/metric overlap.

    The intent classifier only knows labels it was trained/prompted to emit.
    This supplements classifier labels with a deterministic retrieval pass based
    on:

    - prompt/intent text overlap with archetype ids, tags, problem types, and
      required signals
    - live catalog metric overlap with required_metrics and signal_bindings

    Generated artifacts are excluded defensively even if a legacy override file
    still contains them.
    """
    exclude_ids = exclude_ids or set()
    catalog_names = {getattr(entry, "name", "") for entry in catalog if getattr(entry, "name", "")}
    prompt_tokens = _intent_tokens(intent)
    service_tokens = set()
    for service in getattr(intent, "services", []) or []:
        service_tokens |= _tokens(service)
    results: list[tuple[InvestigationArchetype, float]] = []

    for arch in ALL_ARCHETYPES:
        if arch.id in exclude_ids or _is_generated_archetype(arch):
            continue

        arch_tokens = _archetype_tokens(arch)
        token_score = 0.0
        if prompt_tokens and arch_tokens:
            token_score = min(len(prompt_tokens & arch_tokens) / max(min(len(prompt_tokens), 8), 1), 1.0)

        expected_metrics = set(arch.required_metrics) | set(arch.signal_bindings.values())
        expected_metrics = {m for m in expected_metrics if m}
        metric_score = 0.0
        if expected_metrics:
            metric_score = min(len(expected_metrics & catalog_names) / max(min(len(expected_metrics), 4), 1), 1.0)

        service_score = 1.0 if service_tokens and service_tokens <= arch_tokens else 0.0
        if catalog_names:
            confidence = round((0.25 * token_score) + (0.55 * metric_score) + (0.20 * service_score), 4)
        else:
            confidence = round((0.55 * token_score) + (0.45 * service_score), 4)
        if confidence >= min_confidence:
            results.append((arch, confidence))

    results.sort(key=lambda item: item[1], reverse=True)
    return results


def _tokens(value: str) -> set[str]:
    return {t for t in re.split(r"[^a-z0-9]+", value.lower()) if len(t) >= 3}


def _intent_tokens(intent: Any) -> set[str]:
    parts: list[str] = [
        getattr(intent, "summary", ""),
        getattr(intent, "domain", ""),
        getattr(intent, "problem_type", ""),
    ]
    parts.extend(getattr(intent, "services", []) or [])
    parts.extend(getattr(intent, "keywords", []) or [])
    for signal in getattr(intent, "signals", []) or []:
        parts.append(getattr(signal, "value", str(signal)))
    for match in getattr(intent, "archetypes", []) or []:
        parts.append(getattr(match, "type", ""))
    return _tokens(" ".join(parts))


def _archetype_tokens(arch: InvestigationArchetype) -> set[str]:
    parts = [
        arch.id,
        arch.name,
        arch.description,
        *arch.problem_types,
        *arch.required_signals,
        *arch.required_metrics,
        *arch.signal_bindings.keys(),
        *arch.signal_bindings.values(),
        *arch.tags,
    ]
    for panel in arch.panels:
        parts.extend([panel.title, panel.description, panel.row])
    return _tokens(" ".join(parts))


def list_problem_types() -> list[str]:
    """Return all known problem_type values."""
    return sorted(_ARCHETYPE_BY_PROBLEM.keys())
