"""Pre-publish validation: verify dashboard queries return real data.

Prevents publishing empty dashboards that waste engineer time.
"""
from __future__ import annotations

import asyncio
from urllib.parse import quote

import structlog

from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DashboardSpec, PanelSpec

logger = structlog.get_logger()


async def _check_query(
    client: GrafanaClient,
    datasource_uid: str,
    expr: str,
) -> bool:
    """Check if a PromQL expression returns any data."""
    try:
        encoded = quote(expr, safe="")
        data = await client.datasource_proxy_get(
            datasource_uid, f"api/v1/query?query={encoded}"
        )
        result = data.get("data", {}).get("result", []) if isinstance(data, dict) else []
        has_data = len(result) > 0
        logger.debug("query_check", expr=expr[:80], has_data=has_data, result_count=len(result))
        return has_data
    except Exception as e:
        logger.warning("query_check_error", expr=expr[:80], error=str(e))
        return False


async def validate_dashboard_queries(
    client: GrafanaClient,
    spec: DashboardSpec,
) -> tuple[DashboardSpec, list[str]]:
    """Validate that dashboard panels have data. Returns (filtered_spec, warnings).

    Removes panels where ALL queries return no data.
    Keeps panels where at least one query returns data.
    Returns warnings listing which panels were dropped.
    """
    valid_panels: list[PanelSpec] = []
    warnings: list[str] = []

    # Collect all (panel_idx, query_idx, datasource_uid, expr) to check
    checks: list[tuple[int, str, str]] = []
    for panel_idx, panel in enumerate(spec.panels):
        for query in panel.queries:
            checks.append((panel_idx, query.datasource_uid, query.expr))

    if not checks:
        return spec, ["No queries to validate"]

    # Run all checks concurrently
    tasks = [
        _check_query(client, ds_uid, expr)
        for _, ds_uid, expr in checks
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Group results by panel
    panel_has_data: dict[int, bool] = {}
    check_idx = 0
    for panel_idx, panel in enumerate(spec.panels):
        has_any_data = False
        for _ in panel.queries:
            result = results[check_idx]
            if isinstance(result, bool) and result:
                has_any_data = True
            check_idx += 1
        panel_has_data[panel_idx] = has_any_data

    # Filter panels
    for panel_idx, panel in enumerate(spec.panels):
        if panel_has_data.get(panel_idx, False):
            valid_panels.append(panel)
        else:
            warnings.append(f'Panel "{panel.title}" dropped — no matching series')
            logger.warning("panel_no_data", panel=panel.title,
                           queries=[q.expr[:80] for q in panel.queries])

    spec = spec.model_copy(update={"panels": valid_panels})
    if not valid_panels:
        warnings.append("ALL panels returned no data — dashboard not created")

    logger.info("query_validation_complete",
                total_panels=len(spec.panels) + len(warnings),
                valid_panels=len(valid_panels),
                dropped=len(warnings))

    return spec, warnings
