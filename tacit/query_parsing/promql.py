"""PromQL parsing utilities shared by ingestion, validation, and diagnostics."""

from __future__ import annotations

import re
from typing import Any

import promql_parser

PROMQL_METRIC_RE = re.compile(
    r"(?:^|(?<=[\s(,]))([a-zA-Z_:][a-zA-Z0-9_:]*)(?=\s*[{\[)\/\*\+\-><=!,]|\s|$)",
    re.MULTILINE,
)

PROMQL_FUNCS = frozenset(
    {
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "count_values",
        "bottomk",
        "topk",
        "quantile",
        "stddev",
        "stdvar",
        "group",
        "rate",
        "irate",
        "increase",
        "delta",
        "idelta",
        "deriv",
        "predict_linear",
        "histogram_quantile",
        "histogram_count",
        "histogram_sum",
        "absent",
        "absent_over_time",
        "changes",
        "resets",
        "ceil",
        "floor",
        "round",
        "clamp",
        "clamp_min",
        "clamp_max",
        "label_replace",
        "label_join",
        "sort",
        "sort_desc",
        "time",
        "timestamp",
        "vector",
        "scalar",
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "hour",
        "minute",
        "days_in_month",
        "exp",
        "ln",
        "log2",
        "log10",
        "sqrt",
        "sgn",
        "abs",
        "on",
        "ignoring",
        "by",
        "without",
        "bool",
        "offset",
        "and",
        "or",
        "unless",
        "le",
        "inf",
        "nan",
    }
)

AGG_PATTERN = re.compile(
    r"(sum|avg|min|max|count|topk|bottomk|quantile)\s*\(\s*"
    r"(?:(\d+(?:\.\d+)?)\s*,\s*)?"
    r"(rate|irate|increase|delta|histogram_quantile)?\s*\("
)
HISTOGRAM_PATTERN = re.compile(r"histogram_quantile\(\s*([\d.]+)")
PROMQL_LABEL_LIST_RE = re.compile(r"\b(by|without|on|ignoring|group_left|group_right)\s*\(([^)]*)\)")


def strip_promql_label_lists(expr: str) -> str:
    """Remove bare label lists so they are not mistaken for metrics."""
    return PROMQL_LABEL_LIST_RE.sub(lambda m: f"{m.group(1)} ()", expr)


def extract_metrics_from_promql_regex(expr: str) -> list[str]:
    candidates = PROMQL_METRIC_RE.findall(strip_promql_label_lists(expr))
    metrics = []
    for name in candidates:
        name_lower = name.lower()
        if name_lower not in PROMQL_FUNCS and not name.startswith("__"):
            metrics.append(name)
    return list(dict.fromkeys(metrics))


def walk_promql_ast(node: Any, metrics: list[str]) -> None:
    if node is None:
        return

    node_type = type(node).__name__

    if node_type == "VectorSelector":
        name = getattr(node, "name", None)
        if isinstance(name, str) and name and name.lower() not in PROMQL_FUNCS and not name.startswith("__"):
            metrics.append(name)
        return

    if node_type == "MatrixSelector":
        walk_promql_ast(getattr(node, "vs", None) or getattr(node, "vector_selector", None), metrics)
        return

    if node_type in {"AggregateExpr", "UnaryExpr", "ParenExpr", "SubqueryExpr", "StepInvariantExpr"}:
        walk_promql_ast(getattr(node, "expr", None), metrics)
        return

    if node_type == "BinaryExpr":
        walk_promql_ast(getattr(node, "lhs", None), metrics)
        walk_promql_ast(getattr(node, "rhs", None), metrics)
        return

    if node_type == "Call":
        for arg in getattr(node, "args", []) or []:
            walk_promql_ast(arg, metrics)
        return

    for attr in ("expr", "lhs", "rhs", "vs", "vector_selector"):
        child = getattr(node, attr, None)
        if child is not None:
            walk_promql_ast(child, metrics)

    for child in getattr(node, "args", []) or []:
        walk_promql_ast(child, metrics)


def extract_metrics_from_promql(expr: str) -> list[str]:
    """Extract metric names from a PromQL expression."""
    try:
        ast = promql_parser.parse(expr)
    except Exception:
        return extract_metrics_from_promql_regex(expr)

    metrics: list[str] = []
    walk_promql_ast(ast, metrics)
    return list(dict.fromkeys(metrics))


def extract_aggregation_patterns(expr: str) -> list[dict[str, str]]:
    """Extract aggregation function usage from a PromQL expression."""
    patterns = []

    for match in AGG_PATTERN.finditer(expr):
        agg_func = match.group(1)
        inner_func = match.group(3) or ""
        patterns.append(
            {
                "aggregation": agg_func,
                "inner_function": inner_func,
            }
        )

    for match in HISTOGRAM_PATTERN.finditer(expr):
        patterns.append(
            {
                "aggregation": "histogram_quantile",
                "quantile": match.group(1),
            }
        )

    for func in ("rate", "irate", "increase", "delta"):
        if f"{func}(" in expr and not any(p.get("aggregation") == func for p in patterns):
            patterns.append({"aggregation": func})

    return patterns
