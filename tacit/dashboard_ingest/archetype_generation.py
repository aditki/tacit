"""Generate quarantined experimental archetype candidates from dashboard features."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

import yaml

from tacit.archetypes.generated.schema import (
    GeneratedArchetypeOrigin,
    GeneratedArchetypeStatus,
    normalize_environment_ref,
    normalize_service_ref,
    normalize_tenant_id,
)
from tacit.query_parsing.languages import datasource_type_to_language, language_to_datasource_type

_TEMPLATE_PLACEHOLDER_NAMES = ("service_filter", "container_filter", "rate_interval")
_SERVICE_LABEL_PATTERN = re.compile(
    r'\b(?:service|service_name|app|application|component)\s*(=~|=)\s*["\']([^"\']+)["\']',
    re.I,
)
_REGEX_META_PATTERN = re.compile(r"[.*+?()\[\]{}|^$\\]")
_SIGNALFLOW_SERVICE_PATTERN = re.compile(
    r"\bfilter\(\s*['\"](?:service|service_name|app|application|component)['\"]\s*,\s*['\"]([^'\"]+)['\"]",
    re.I,
)
_GRAFANA_VARIABLE_PATTERN = re.compile(r"(?:\$\{?[A-Za-z_][A-Za-z0-9_]*\}?|\[\[[^\]]+\]\])")


def _is_resolved_scope_value(value: object) -> bool:
    """Return false for Grafana template variables that have no concrete scope."""
    return bool(value) and not _GRAFANA_VARIABLE_PATTERN.search(str(value))


def escape_literal_braces(expr: str) -> str:
    """Escape concrete query braces while preserving Tacit placeholders."""
    protected: dict[str, str] = {}

    def protect(value: str) -> str:
        token = f"__TACIT_FMT_TOKEN_{len(protected)}__"
        protected[token] = value
        return token

    for name in _TEMPLATE_PLACEHOLDER_NAMES:
        expr = expr.replace(f"{{{{{{{name}}}}}}}", protect(f"{{{{{{{name}}}}}}}"))

    for name in _TEMPLATE_PLACEHOLDER_NAMES:
        expr = expr.replace(f"{{{name}}}", protect(f"{{{name}}}"))

    escaped = expr.replace("{", "{{").replace("}", "}}")
    for token, value in protected.items():
        escaped = escaped.replace(token, value)
    return escaped


def generate_archetype_yaml(
    extracted: dict[str, Any],
    signals: list[dict[str, Any]],
    archetype_id: str = "",
    *,
    tenant_id: str = "",
    service_refs: list[str] | None = None,
    environment_refs: list[str] | None = None,
    archetype_kind: str = "investigation_dashboard",
    generation_version: str = "generated-archetype-v1",
    generation_run_id: str = "",
    source_refs: list[str] | None = None,
    created_at: datetime | None = None,
) -> str:
    """Generate a quarantined artifact candidate, never a curated template."""
    title = extracted["dashboard_title"]
    if not archetype_id:
        archetype_id = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")

    problem_types = [archetype_id]
    for tag in extracted.get("dashboard_tags", []):
        clean = re.sub(r"[^a-z0-9]+", "_", tag.lower()).strip("_")
        if clean and clean != archetype_id:
            problem_types.append(clean)

    signal_bindings = {}
    required_signals = []
    for sig in signals:
        if sig.get("source") == "heuristic" and not sig.get("auto_teach_eligible"):
            continue
        if sig["signal_type"] not in signal_bindings:
            signal_bindings[sig["signal_type"]] = sig["metric"]
            required_signals.append(sig["signal_type"])

    dashboard_language = extracted.get("query_language", "promql")

    panels = []
    for panel in extracted.get("panels", [])[:12]:
        panel_language = panel.get("query_language")
        if not panel_language:
            ds_type = panel.get("datasource_type", "")
            panel_language = datasource_type_to_language(ds_type) if ds_type else dashboard_language

        queries = []
        cloudwatch_targets = panel.get("cloudwatch_targets") or []
        if panel_language == "cloudwatch" and cloudwatch_targets:
            for target in cloudwatch_targets:
                queries.append(
                    {
                        "expr": target.get("metric_name", ""),
                        "query_language": "cloudwatch",
                        "datasource_type": "cloudwatch",
                        "cloudwatch_namespace": target.get("namespace", ""),
                        "cloudwatch_stat": target.get("stat", ""),
                        "cloudwatch_region": target.get("region", ""),
                        "cloudwatch_dimensions": target.get("dimensions", {}),
                        "legend_format": "",
                    }
                )
        else:
            for query in panel.get("queries", []):
                query_def: dict[str, Any] = {
                    "expr": escape_literal_braces(query) if panel_language == "promql" else query,
                    "legend_format": "",
                    "query_language": panel_language,
                }
                if panel_language != "promql":
                    query_def["datasource_type"] = language_to_datasource_type(panel_language)
                queries.append(query_def)
        if queries:
            panel_def: dict[str, Any] = {
                "title": panel["title"],
                "queries": queries,
            }
            if panel.get("row"):
                panel_def["row"] = panel["row"]
            if panel.get("unit"):
                panel_def["unit"] = panel["unit"]
            if panel.get("description"):
                panel_def["description"] = panel["description"]
            panels.append(panel_def)

    explicit_services = list(service_refs or [])
    if not explicit_services:
        explicit_services = [
            str(service) for signal in signals for service in (signal.get("services", []) or []) if service
        ]
    for panel in extracted.get("panels", []):
        for query in panel.get("queries", []) or []:
            if not isinstance(query, str):
                continue
            for operator, value in _SERVICE_LABEL_PATTERN.findall(query):
                if operator == "=~" and _REGEX_META_PATTERN.search(value):
                    continue
                explicit_services.append(value)
            explicit_services.extend(_SIGNALFLOW_SERVICE_PATTERN.findall(query))
        for target in panel.get("cloudwatch_targets", []) or []:
            for name, value in (target.get("dimensions", {}) or {}).items():
                if name.casefold() not in {"service", "service_name", "app", "application", "component"}:
                    continue
                if isinstance(value, list):
                    explicit_services.extend(str(item) for item in value)
                elif value:
                    explicit_services.append(str(value))
    explicit_environments = list(environment_refs or [])
    for tag in extracted.get("dashboard_tags", []):
        match = re.match(r"^(?:service|service_name|app|application|component)\s*[:=]\s*(.+)$", tag, re.I)
        if match:
            explicit_services.append(match.group(1))
        environment_match = re.match(r"^(?:environment|env)\s*[:=]\s*(.+)$", tag, re.I)
        if environment_match:
            explicit_environments.append(environment_match.group(1))

    archetype = {
        "id": archetype_id,
        "name": title,
        "description": f"Auto-generated from dashboard '{title}'",
        "problem_types": problem_types,
        "required_metrics": extracted["metrics_found"][:10],
        "required_signals": required_signals[:10],
        "signal_bindings": signal_bindings,
        "tags": list(dict.fromkeys(extracted.get("dashboard_tags", []) + ["auto-generated", "learned"])),
        "default_timerange": "1h",
        "panels": panels,
        "origin": GeneratedArchetypeOrigin.GENERATED_EXPERIMENTAL.value,
        "retrieval_status": GeneratedArchetypeStatus.QUARANTINED.value,
        "tenant_id": normalize_tenant_id(tenant_id),
        "service_refs": sorted(
            {
                ref
                for value in explicit_services
                if _is_resolved_scope_value(value) and (ref := normalize_service_ref(value))
            }
        ),
        "environment_refs": sorted(
            {ref for value in explicit_environments if (ref := normalize_environment_ref(value))}
        ),
        "archetype_kind": archetype_kind,
        "generation_version": generation_version,
        "generation_run_id": generation_run_id,
        "source_refs": list(dict.fromkeys(source_refs or [])),
        "created_at": (created_at or datetime.now(UTC)).isoformat(),
    }

    return yaml.dump(
        {"archetypes": [archetype]},
        default_flow_style=False,
        sort_keys=False,
        width=120,
    )
