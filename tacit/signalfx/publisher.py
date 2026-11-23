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

from tacit.config import Settings
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS, safe_failure_diagnostics
from tacit.models.schemas import DashboardSpec, PanelSpec
from tacit.runtime_ownership import resolve_remote_runtime_settings
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


def _promql_to_signalflow(expr: str, label: str = "A") -> str:
    """Use the existing guarded compiler; publication has no second translator."""
    from tacit.archetypes.engine import _promql_template_to_signalflow

    return _promql_template_to_signalflow(expr, "", "", label)


def validate_signalflow_publication(spec: DashboardSpec) -> None:
    """Preflight all chart programs before any vendor can mutate state."""
    for panel in spec.panels:
        _build_chart_json(panel)


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
            logger.warning("publisher_promql_fallback", reason_code="guarded_translation")
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
    runtime_settings: Settings | None = None,
) -> tuple[str, str]:
    """Publish a DashboardSpec to SignalFx.

    Creates charts, then a dashboard linking them.
    Returns (dashboard_url, dashboard_id).
    """
    active_settings = resolve_remote_runtime_settings(
        boundary="signalfx_dashboard_publication",
        owner=client,
        provider="signalfx",
        explicit_settings=runtime_settings,
    )
    group_name = group_name or active_settings.signalfx_dashboard_group
    validate_signalflow_publication(spec)

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
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except Exception as exc:
            logger.warning(
                "signalfx_chart_create_failed",
                title=panel.title,
                **safe_failure_diagnostics(
                    exc,
                    reason_code="signalfx_chart_create_failed",
                ),
            )

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
    except AUTHORITY_BOUNDARY_ERRORS:
        raise
    except Exception as exc:
        logger.error(
            "signalfx_dashboard_create_failed",
            **safe_failure_diagnostics(
                exc,
                reason_code="signalfx_dashboard_create_failed",
            ),
        )
        # Clean up orphaned charts
        for cid in chart_ids:
            try:
                await client.delete_chart(cid)
            except Exception:
                pass
        return "", ""
