"""Pre-publish validation: verify dashboard queries return real data.

Prevents publishing empty dashboards that waste engineer time.
"""
from __future__ import annotations

import asyncio
import re
from urllib.parse import quote

import structlog

from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DashboardSpec, PanelSpec

logger = structlog.get_logger()

# Regex to extract metric names from SignalFlow data('metric_name', ...)
_SFX_DATA_RE = re.compile(r"data\(\s*['\"]([^'\"]+)['\"]\s*[),]")


async def _check_query(
    client: GrafanaClient,
    datasource_uid: str,
    datasource_type: str,
    expr: str,
) -> bool:
    """Check if a query returns data for datasources we can validate via Prometheus API."""
    normalized_type = (datasource_type or "").lower()
    if normalized_type in {"cloudwatch"}:
        logger.debug("query_check_skipped", datasource_type=normalized_type, reason="unsupported_datasource_validation")
        return True

    try:
        encoded = quote(expr, safe="")
        data = await client.datasource_proxy_get(
            datasource_uid, f"api/v1/query?query={encoded}"
        )
        result = data.get("data", {}).get("result", []) if isinstance(data, dict) else []
        has_data = len(result) > 0
        logger.debug("query_check", expr=expr[:80], datasource_type=normalized_type, has_data=has_data, result_count=len(result))
        return has_data
    except Exception as e:
        logger.warning("query_check_error", expr=expr[:80], datasource_type=normalized_type, error=str(e))
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

    # Collect all (panel_idx, datasource_uid, datasource_type, expr) to check
    checks: list[tuple[int, str, str, str]] = []
    for panel_idx, panel in enumerate(spec.panels):
        for query in panel.queries:
            checks.append((panel_idx, query.datasource_uid, query.datasource_type, query.expr))

    if not checks:
        return spec, ["No queries to validate"]

    # Run all checks concurrently
    tasks = [
        _check_query(client, ds_uid, ds_type, expr)
        for _, ds_uid, ds_type, expr in checks
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


# ── SignalFx validation ──────────────────────────────────────────────────────


def _extract_signalflow_metrics(expr: str) -> list[str]:
    """Extract metric names from SignalFlow data('metric_name') calls."""
    return _SFX_DATA_RE.findall(expr)


async def _check_metric_exists(
    sfx_client: "SignalFxClient",
    metric_name: str,
    _cache: dict[str, bool] | None = None,
) -> bool:
    """Check if a metric exists in SignalFx via the metadata API."""
    if _cache is not None and metric_name in _cache:
        return _cache[metric_name]
    try:
        await sfx_client.get_metric(metric_name)
        exists = True
    except Exception:
        # 404 or other error → metric doesn't exist
        exists = False
    logger.debug("sfx_metric_check", metric=metric_name, exists=exists)
    if _cache is not None:
        _cache[metric_name] = exists
    return exists


async def validate_signalflow_queries(
    sfx_client: "SignalFxClient",
    spec: DashboardSpec,
) -> tuple[DashboardSpec, list[str]]:
    """Validate SignalFlow panels by checking that referenced metrics exist.

    For each panel, extracts metric names from data('...') calls and verifies
    they exist in SignalFx. Drops panels where ALL referenced metrics are missing.
    Returns (filtered_spec, warnings).
    """
    from dashforge.signalfx.client import SignalFxClient  # noqa: F811

    valid_panels: list[PanelSpec] = []
    warnings: list[str] = []

    # Collect unique metric names across all panels for batch checking
    all_metrics: set[str] = set()
    panel_metrics: dict[int, list[str]] = {}
    for panel_idx, panel in enumerate(spec.panels):
        metrics = []
        for q in panel.queries:
            metrics.extend(_extract_signalflow_metrics(q.expr))
        panel_metrics[panel_idx] = metrics
        all_metrics.update(metrics)

    if not all_metrics:
        logger.warning("sfx_validation_no_metrics",
                        reason="no data() calls found in any panel")
        return spec, ["No SignalFlow data() calls found to validate"]

    # Check all unique metrics concurrently with a shared cache
    cache: dict[str, bool] = {}
    tasks = [_check_metric_exists(sfx_client, m, cache) for m in all_metrics]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Filter panels: keep if at least one referenced metric exists
    for panel_idx, panel in enumerate(spec.panels):
        metrics = panel_metrics.get(panel_idx, [])
        if not metrics:
            # No data() calls → keep the panel (might be a static/text panel)
            valid_panels.append(panel)
            continue

        has_any = any(cache.get(m, False) for m in metrics)
        if has_any:
            valid_panels.append(panel)
        else:
            warnings.append(
                f'Panel "{panel.title}" dropped — metrics not found in SignalFx: '
                f'{", ".join(metrics[:5])}'
            )
            logger.warning("sfx_panel_no_data", panel=panel.title,
                           missing_metrics=metrics[:5])

    spec = spec.model_copy(update={"panels": valid_panels})
    if not valid_panels:
        warnings.append("ALL panels returned no data — dashboard not created")

    logger.info("sfx_query_validation_complete",
                total_panels=len(valid_panels) + len(warnings),
                valid_panels=len(valid_panels),
                dropped=len(warnings),
                metrics_checked=len(all_metrics),
                metrics_found=sum(1 for v in cache.values() if v))

    return spec, warnings
