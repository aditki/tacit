from __future__ import annotations

import time
from typing import Any

import structlog

from dashforge.config import settings
from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DashboardSpec, PanelSpec

logger = structlog.get_logger()

TIMERANGE_MAP = {
    "5m": "now-5m",
    "15m": "now-15m",
    "30m": "now-30m",
    "1h": "now-1h",
    "3h": "now-3h",
    "6h": "now-6h",
    "12h": "now-12h",
    "24h": "now-24h",
    "1d": "now-1d",
    "7d": "now-7d",
}


def _build_panel_json(panel: PanelSpec, panel_id: int, grid_pos: dict) -> dict[str, Any]:
    """Convert a PanelSpec into Grafana panel JSON."""
    targets = []
    for idx, q in enumerate(panel.queries):
        target: dict[str, Any] = {
            "refId": chr(65 + idx),  # A, B, C, …
            "expr": q.expr,
            "legendFormat": q.legend_format,
            "datasource": {"uid": q.datasource_uid, "type": q.datasource_type},
        }
        if q.cloudwatch_namespace:
            target["namespace"] = q.cloudwatch_namespace
            target["metricName"] = q.expr  # expr holds the CW metric name
            target["region"] = q.cloudwatch_region or "default"
            target["statistics"] = [q.cloudwatch_stat or "Average"]
            if q.cloudwatch_dimensions:
                target["dimensions"] = q.cloudwatch_dimensions
        targets.append(target)

    panel_json: dict[str, Any] = {
        "id": panel_id,
        "title": panel.title,
        "description": panel.description,
        "type": panel.panel_type,
        "gridPos": grid_pos,
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "custom": {},
            },
            "overrides": [],
        },
        "options": {},
    }

    if panel.unit:
        panel_json["fieldConfig"]["defaults"]["unit"] = panel.unit

    if panel.thresholds:
        panel_json["fieldConfig"]["defaults"]["thresholds"] = {
            "mode": "absolute",
            "steps": panel.thresholds,
        }

    return panel_json


def _build_row_panel(title: str, panel_id: int, y: int) -> dict[str, Any]:
    """Build a Grafana row panel (collapsible section header)."""
    return {
        "id": panel_id,
        "type": "row",
        "title": title,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "collapsed": False,
        "panels": [],
    }


def build_dashboard_json(spec: DashboardSpec) -> dict[str, Any]:
    """Build the full Grafana dashboard model JSON from a DashboardSpec."""
    panels_json: list[dict] = []
    col, row = 0, 0
    panel_width = 12
    panel_height = 8
    next_id = 1

    # Check if any panels use row grouping
    has_rows = any(p.row for p in spec.panels)

    if has_rows:
        # Group panels by their row name, preserving order
        seen_rows: list[str] = []
        for panel in spec.panels:
            row_name = panel.row or "Other"
            if row_name not in seen_rows:
                seen_rows.append(row_name)

        for row_name in seen_rows:
            # Insert row header
            panels_json.append(_build_row_panel(row_name, next_id, row))
            next_id += 1
            row += 1  # row panel is 1 unit tall
            col = 0

            row_panels = [p for p in spec.panels if (p.row or "Other") == row_name]
            for panel in row_panels:
                grid_pos = {"x": col, "y": row, "w": panel_width, "h": panel_height}
                panels_json.append(_build_panel_json(panel, next_id, grid_pos))
                next_id += 1
                col += panel_width
                if col >= 24:
                    col = 0
                    row += panel_height
            if col > 0:
                col = 0
                row += panel_height
    else:
        for panel in spec.panels:
            grid_pos = {"x": col, "y": row, "w": panel_width, "h": panel_height}
            panels_json.append(_build_panel_json(panel, next_id, grid_pos))
            next_id += 1
            col += panel_width
            if col >= 24:
                col = 0
                row += panel_height

    time_from = TIMERANGE_MAP.get(spec.timerange, f"now-{spec.timerange}")

    dashboard: dict[str, Any] = {
        "id": None,
        "uid": None,
        "title": spec.title,
        "tags": spec.tags + ["dashforge"],
        "timezone": "browser",
        "refresh": "30s",
        "time": {"from": time_from, "to": "now"},
        "panels": panels_json,
        "schemaVersion": 39,
        "version": 0,
    }
    return dashboard


async def publish_dashboard(
    client: GrafanaClient,
    spec: DashboardSpec,
) -> tuple[str, str]:
    """Create / update a Grafana dashboard. Returns (dashboard_url, dashboard_uid)."""
    folder = await client.get_or_create_folder(settings.dashforge_dashboard_folder)
    folder_uid = folder.get("uid", "")

    dashboard_json = build_dashboard_json(spec)
    result = await client.create_dashboard(dashboard_json, folder_uid)

    uid = result.get("uid", "")
    url = f"{settings.grafana_url}{result.get('url', f'/d/{uid}')}"

    logger.info("dashboard_published", uid=uid, url=url, panels=len(spec.panels))
    return url, uid
