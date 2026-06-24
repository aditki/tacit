"""Publish DashboardSpec as native SignalFx dashboards and charts.

SignalFx dashboard model:
- Dashboard Group contains Dashboards
- Dashboard contains chart references (chart IDs)
- Charts are created independently, then linked into dashboards

Flow: DashboardSpec → create charts → create dashboard → link charts → return URL
"""

from __future__ import annotations

import re
import time
from typing import Any

import structlog

from tacit.config import settings
from tacit.models.schemas import DashboardSpec, PanelSpec
from tacit.signalfx.client import SignalFxClient

logger = structlog.get_logger()

# Map Grafana panel types → SignalFx chart modes
_PANEL_TYPE_MAP = {
    "timeseries": "TimeSeriesChart",
    "graph": "TimeSeriesChart",
    "stat": "SingleValue",
    "gauge": "SingleValue",
    "table": "List",
    "heatmap": "Heatmap",
    "logs": "List",
}

# Map Grafana units → SignalFx display units (subset)
_UNIT_MAP = {
    "s": "Second",
    "ms": "Millisecond",
    "bytes": "Byte",
    "decbytes": "Byte",
    "percentunit": "Percentage",
    "percent": "Percentage",
    "reqps": "RequestPerSecond",
    "ops": "OperationPerSecond",
    "short": None,
    "": None,
}

# SignalFx timerange presets (milliseconds)
_TIMERANGE_MS = {
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "3h": 10_800_000,
    "6h": 21_600_000,
    "12h": 43_200_000,
    "24h": 86_400_000,
    "1d": 86_400_000,
    "7d": 604_800_000,
}


# ── PromQL → SignalFlow translator ────────────────────────────────────────────

# Regex to parse PromQL label matchers: {key="val", key=~"regex", ...}
_LABEL_RE = re.compile(r'(\w+)\s*(=~|!=|=)\s*"([^"]*?)"')
# Regex to detect PromQL patterns (curly-brace selectors, rate(), sum(), etc.)
_PROMQL_INDICATORS = re.compile(r"\b(rate|sum|increase|histogram_quantile|count|avg|topk|bottomk)\s*\(")


def _is_promql(expr: str) -> bool:
    """Heuristic: does this look like PromQL rather than SignalFlow?"""
    # SignalFlow uses data('...'), PromQL uses metric_name{...} and functions like rate()
    if "data(" in expr:
        return False
    if _PROMQL_INDICATORS.search(expr):
        return True
    # Bare metric with label selectors: metric_name{key="val"}
    if re.search(r"\w+\{.*?\}", expr) and "filter(" not in expr:
        return True
    return False


def _parse_labels(label_str: str) -> list[tuple[str, str, str]]:
    """Parse PromQL label matchers into (key, operator, value) tuples."""
    return [(m[0], m[1], m[2]) for m in _LABEL_RE.findall(label_str)]


def _labels_to_filter(labels: list[tuple[str, str, str]]) -> str:
    """Convert parsed labels to SignalFlow filter expression."""
    parts = []
    for key, op, val in labels:
        if op == "=~":
            # Regex match → SignalFlow doesn't have native regex, use filter with
            # a simplified approach: if it's a simple alternation, use OR filters.
            # Otherwise fall back to a broad filter.
            if val.startswith("[") or "|" in val:
                parts.append(f"filter('{key}', '*')")
            else:
                parts.append(f"filter('{key}', '{val}')")
        elif op == "!=":
            parts.append(f"not filter('{key}', '{val}')")
        else:  # =
            parts.append(f"filter('{key}', '{val}')")
    return " and ".join(parts) if parts else ""


def _promql_to_signalflow(expr: str, label: str = "A") -> str:
    """Best-effort translation of a PromQL expression to SignalFlow.

    Handles the common patterns found in Tacit archetypes:
    - metric{labels}  →  data('metric', filter=...)
    - rate(metric{labels}[5m])  →  data('metric', filter=..., rollup='rate')
    - sum(rate(...)) by (x)  →  data(...).sum(by=['x'])
    - increase(metric{labels}[5m])  →  data('metric', filter=..., rollup='delta')
    - histogram_quantile(0.95, sum(rate(metric_bucket{...}[5m])) by (le))
      →  data('metric', filter=...).percentile(pct=95)
    """
    expr = expr.strip()

    # ── histogram_quantile → percentile ──
    hq_match = re.match(
        r"histogram_quantile\(([\d.]+),\s*sum\(rate\((\w+?)_bucket\{(.*?)\}\[(\w+)\]\)\)\s*by\s*\(le(?:,\s*(\w+))?\)\)",
        expr,
    )
    if hq_match:
        pct = int(float(hq_match.group(1)) * 100)
        metric = hq_match.group(2)
        labels = _parse_labels(hq_match.group(3))
        filt = _labels_to_filter(labels)
        by_dim = hq_match.group(5)
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        chain = f"{base}.percentile(pct={pct})"
        if by_dim:
            chain = f"{base}.percentile(pct={pct}, by=['{by_dim}'])"
        return f"{chain}.publish(label='{label}')"

    # ── topk → top ──
    topk_match = re.match(r"topk\((\d+),\s*(.+)\)$", expr, re.DOTALL)
    if topk_match:
        k = topk_match.group(1)
        inner = _promql_to_signalflow(topk_match.group(2), label)
        # Strip .publish(...) from inner, add .top(count=k)
        inner = re.sub(r"\.publish\([^)]*\)$", "", inner)
        return f"{inner}.top(count={k}).publish(label='{label}')"

    # ── sum/avg/count/min/max wrapping rate/increase ──
    agg_rate_match = re.match(
        r"(sum|avg|count|min|max)\((rate|increase)\((\w+)\{(.*?)\}\[(\w+)\]\)\)(?:\s*by\s*\(([^)]+)\))?", expr
    )
    if agg_rate_match:
        agg = agg_rate_match.group(1)
        func = agg_rate_match.group(2)
        metric = agg_rate_match.group(3)
        labels = _parse_labels(agg_rate_match.group(4))
        by_dims = agg_rate_match.group(6)
        filt = _labels_to_filter(labels)
        rollup = "rate" if func == "rate" else "delta"
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += f", rollup='{rollup}')"
        if by_dims:
            dims = [d.strip() for d in by_dims.split(",") if d.strip() != "le"]
            if dims:
                base += f".{agg}(by={dims})"
            else:
                base += f".{agg}()"
        else:
            base += f".{agg}()"
        return f"{base}.publish(label='{label}')"

    # ── sum/avg of two aggregated expressions (ratio) ──
    ratio_match = re.match(
        r"(sum|avg)\(rate\((\w+)\{(.*?)\}\[(\w+)\]\)\)\s*/\s*(sum|avg)\(rate\((\w+)\{(.*?)\}\[(\w+)\]\)\)", expr
    )
    if ratio_match:
        m1 = ratio_match.group(2)
        l1 = _parse_labels(ratio_match.group(3))
        m2 = ratio_match.group(6)
        l2 = _parse_labels(ratio_match.group(7))
        f1 = _labels_to_filter(l1)
        f2 = _labels_to_filter(l2)
        num = f"data('{m1}'"
        if f1:
            num += f", filter={f1}"
        num += ", rollup='rate').sum()"
        den = f"data('{m2}'"
        if f2:
            den += f", filter={f2}"
        den += ", rollup='rate').sum()"
        return f"({num} / {den}).publish(label='{label}')"

    # ── bare rate/increase ──
    rate_match = re.match(r"(rate|increase)\((\w+)\{(.*?)\}\[(\w+)\]\)", expr)
    if rate_match:
        func = rate_match.group(1)
        metric = rate_match.group(2)
        labels = _parse_labels(rate_match.group(3))
        filt = _labels_to_filter(labels)
        rollup = "rate" if func == "rate" else "delta"
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += f", rollup='{rollup}')"
        return f"{base}.publish(label='{label}')"

    # ── simple metric{labels} ──
    simple_match = re.match(r"(\w+)\{(.*?)\}$", expr)
    if simple_match:
        metric = simple_match.group(1)
        labels = _parse_labels(simple_match.group(2))
        filt = _labels_to_filter(labels)
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        return f"{base}.publish(label='{label}')"

    # ── bare metric name ──
    bare_match = re.match(r"^(\w+)$", expr)
    if bare_match:
        return f"data('{bare_match.group(1)}').publish(label='{label}')"

    # Fallback: wrap in data() and hope for the best
    logger.warning("promql_to_signalflow_fallback", expr=expr[:100])
    return f"data('{expr}').publish(label='{label}')"


def _build_chart_json(panel: PanelSpec) -> dict[str, Any]:
    """Convert a PanelSpec into a SignalFx chart create payload."""
    chart_type = _PANEL_TYPE_MAP.get(panel.panel_type, "TimeSeriesChart")

    # Build SignalFlow program from panel queries.
    # The archetype engine generates SignalFlow natively when target_language
    # is 'signalflow'. The fallback converter is a safety net only.
    program_lines = []
    for idx, q in enumerate(panel.queries):
        expr = q.expr.strip()
        if not expr:
            continue

        label = q.legend_format or f"Query {chr(65 + idx)}"

        if _is_promql(expr):
            # Safety net: should not happen if engine compiled correctly
            logger.warning("publisher_promql_fallback", expr=expr[:80])
            expr = _promql_to_signalflow(expr, label)
        elif ".publish(" not in expr:
            expr = f"{expr}.publish(label='{label}')"

        program_lines.append(expr)

    program_text = "\n".join(program_lines)

    chart: dict[str, Any] = {
        "name": panel.title,
        "description": panel.description,
        "options": {
            "type": chart_type,
            "programOptions": {
                "minimumResolution": 0,
                "disableSampling": False,
            },
        },
        "programText": program_text,
    }

    # Add unit if mapped
    sfx_unit = _UNIT_MAP.get(panel.unit)
    if sfx_unit:
        chart["options"]["unitPrefix"] = "Metric"

    # Thresholds → color thresholds
    if panel.thresholds:
        color_scale = []
        for t in panel.thresholds:
            if "value" in t:
                color_scale.append(
                    {
                        "gt": t["value"],
                        "color": t.get("color", ""),
                    }
                )
        if color_scale:
            chart["options"]["colorScale2"] = color_scale

    return chart


def _build_dashboard_json(
    spec: DashboardSpec,
    chart_ids: list[str],
    group_id: str,
) -> dict[str, Any]:
    """Build a SignalFx dashboard payload with chart layout."""
    # Lay out charts in a 2-column grid (each chart 6 units wide, 2 tall)
    charts = []
    col, row = 0, 0
    chart_width = 6
    chart_height = 2

    # Check if any panels use row grouping
    has_rows = any(p.row for p in spec.panels)

    if has_rows:
        # Group by row name
        seen_rows: list[str] = []
        for panel in spec.panels:
            row_name = panel.row or "Other"
            if row_name not in seen_rows:
                seen_rows.append(row_name)

        chart_idx = 0
        for row_name in seen_rows:
            row_panels = [p for p in spec.panels if (p.row or "Other") == row_name]
            col = 0
            for _ in row_panels:
                if chart_idx < len(chart_ids):
                    charts.append(
                        {
                            "chartId": chart_ids[chart_idx],
                            "column": col,
                            "row": row,
                            "height": chart_height,
                            "width": chart_width,
                        }
                    )
                    chart_idx += 1
                    col += chart_width
                    if col >= 12:
                        col = 0
                        row += chart_height
            if col > 0:
                row += chart_height
                col = 0
    else:
        for idx, chart_id in enumerate(chart_ids):
            charts.append(
                {
                    "chartId": chart_id,
                    "column": col,
                    "row": row,
                    "height": chart_height,
                    "width": chart_width,
                }
            )
            col += chart_width
            if col >= 12:
                col = 0
                row += chart_height

    dashboard: dict[str, Any] = {
        "name": spec.title,
        "description": f"Auto-generated by Tacit at {time.strftime('%Y-%m-%d %H:%M:%S')}",
        "groupId": group_id,
        "charts": charts,
        "filters": {
            "sources": [],
            "time": {
                "start": f"-{spec.timerange}",
                "end": "Now",
            },
        },
        "tags": spec.tags + ["tacit"],
    }

    return dashboard


async def publish_dashboard(
    client: SignalFxClient,
    spec: DashboardSpec,
    group_name: str | None = None,
) -> tuple[str, str]:
    """Publish a DashboardSpec to SignalFx.

    Creates charts, then a dashboard linking them.
    Returns (dashboard_url, dashboard_id).
    """
    group_name = group_name or str(getattr(settings, "signalfx_dashboard_group", "Tacit"))

    # 1. Get or create dashboard group
    group = await client.get_or_create_dashboard_group(group_name)
    group_id = group.get("id", "")
    logger.info("signalfx_dashboard_group", name=group_name, id=group_id)

    # 2. Create charts
    chart_ids: list[str] = []
    for panel in spec.panels:
        chart_json = _build_chart_json(panel)
        try:
            result = await client.create_chart(chart_json)
            chart_id = result.get("id", "")
            if chart_id:
                chart_ids.append(chart_id)
                logger.debug("signalfx_chart_created", title=panel.title, id=chart_id)
            else:
                logger.warning("signalfx_chart_no_id", title=panel.title)
        except Exception:
            logger.warning("signalfx_chart_create_failed", title=panel.title, exc_info=True)

    if not chart_ids:
        logger.error("signalfx_no_charts_created")
        return "", ""

    # 3. Create dashboard with chart layout
    dashboard_json = _build_dashboard_json(spec, chart_ids, group_id)
    try:
        result = await client.create_dashboard(dashboard_json)
        dashboard_id = result.get("id", "")
        realm = client.realm
        dashboard_url = f"https://app.{realm}.signalfx.com/#/dashboard/{dashboard_id}"

        logger.info(
            "signalfx_dashboard_published",
            id=dashboard_id,
            url=dashboard_url,
            charts=len(chart_ids),
            panels=len(spec.panels),
        )
        return dashboard_url, dashboard_id
    except Exception:
        logger.error("signalfx_dashboard_create_failed", exc_info=True)
        # Clean up orphaned charts
        for cid in chart_ids:
            try:
                await client.delete_chart(cid)
            except Exception:
                pass
        return "", ""
