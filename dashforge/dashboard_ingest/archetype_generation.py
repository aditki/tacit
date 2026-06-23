"""Generate learned archetypes from parsed dashboard features."""

from __future__ import annotations

import re
from typing import Any

import yaml

from dashforge.query_parsing.languages import datasource_type_to_language, language_to_datasource_type

_TEMPLATE_PLACEHOLDER_NAMES = ("service_filter", "container_filter", "rate_interval")


def escape_literal_braces(expr: str) -> str:
    """Escape concrete query braces while preserving DashForge placeholders."""
    protected: dict[str, str] = {}

    def protect(value: str) -> str:
        token = f"__DASHFORGE_FMT_TOKEN_{len(protected)}__"
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
) -> str:
    """Generate an archetype YAML snippet from extracted dashboard features."""
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
    }

    return yaml.dump(
        {"archetypes": [archetype]},
        default_flow_style=False,
        sort_keys=False,
        width=120,
    )
