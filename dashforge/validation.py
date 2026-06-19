"""Pre-publish validation: verify dashboard queries return real data.

Prevents publishing empty dashboards that waste engineer time.
"""

from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING
from urllib.parse import quote

import httpx
import structlog

from dashforge.grafana.client import GrafanaClient
from dashforge.models.schemas import DashboardSpec, MetricEntry, PanelQuery, PanelSpec

if TYPE_CHECKING:
    from dashforge.signalfx.client import SignalFxClient

logger = structlog.get_logger()

_PROMETHEUS_PROBE_TYPES = {"prometheus", "mimir", "cortex", "thanos"}

# Regex to extract metric names from SignalFlow data('metric_name', ...)
_SFX_DATA_RE = re.compile(r"data\(\s*['\"]([^'\"]+)['\"]\s*[),]")

# Per-query verdicts kept deliberately distinct so callers never conflate a
# fabricated metric with a real-but-sparse one.
QUERY_OK = "ok"  # returned data in-window
QUERY_EMPTY = "empty"  # valid + exists, but no series in-window (sparse)
QUERY_SYNTAX = "syntax_error"  # query failed to parse/compile
QUERY_ABSENT = "absent"  # metric is not in the routed datasource catalog
QUERY_BAD_UID = "bad_uid"  # datasource UID is not among discovered datasources
QUERY_SKIPPED = "skipped"  # datasource type we cannot probe via the prom API
QUERY_ERROR = "error"  # transport/other error — cannot classify


async def _probe_query(
    client: GrafanaClient,
    datasource_uid: str,
    datasource_type: str,
    expr: str,
    query_language: str = "",
) -> str:
    """Probe a single query and return one of the QUERY_* verdicts.

    Separates *syntax validity* from *data presence* by reading the Prometheus
    response status, so a real-but-sparse metric (success + empty result) is
    never reported as a parse failure.
    """
    normalized_type = (datasource_type or "").lower()
    normalized_language = (query_language or "").lower()
    unsupported_type = normalized_type and normalized_type not in _PROMETHEUS_PROBE_TYPES
    unsupported_unrouted_language = not normalized_type and normalized_language and normalized_language != "promql"
    if unsupported_type or unsupported_unrouted_language:
        logger.debug(
            "query_check_skipped",
            datasource_type=normalized_type,
            query_language=normalized_language,
            reason="unsupported_datasource_validation",
        )
        return QUERY_SKIPPED

    try:
        encoded = quote(expr, safe="")
        data = await client.datasource_proxy_get(datasource_uid, f"api/v1/query?query={encoded}")
        if isinstance(data, dict) and data.get("status") == "error":
            err_type = (data.get("errorType") or "").lower()
            if err_type in {"bad_data", "parse", "execution"}:
                logger.warning("query_syntax_error", expr=expr[:80], error_type=err_type)
                return QUERY_SYNTAX
            return QUERY_ERROR
        result = data.get("data", {}).get("result", []) if isinstance(data, dict) else []
        verdict = QUERY_OK if len(result) > 0 else QUERY_EMPTY
        logger.debug("query_check", expr=expr[:80], datasource_type=normalized_type, verdict=verdict)
        return verdict
    except httpx.HTTPStatusError as e:
        try:
            payload = e.response.json()
        except Exception:
            payload = {}
        error_type = str(payload.get("errorType", "")).lower() if isinstance(payload, dict) else ""
        if e.response.status_code in {400, 422} or error_type in {"bad_data", "parse", "execution"}:
            logger.warning("query_syntax_error", expr=expr[:80], status=e.response.status_code)
            return QUERY_SYNTAX
        logger.warning("query_check_http_error", expr=expr[:80], status=e.response.status_code)
        return QUERY_ERROR
    except Exception as e:
        logger.warning("query_check_error", expr=expr[:80], datasource_type=normalized_type, error=str(e))
        return QUERY_ERROR


def _referenced_metrics(expr: str, query_language: str) -> set[str]:
    """Best-effort metric names referenced by a query (PromQL only)."""
    if query_language and query_language.lower() not in ("", "promql"):
        return set()
    try:
        from dashforge.dashboard_ingest import extract_metrics_from_promql

        return set(extract_metrics_from_promql(expr))
    except Exception:
        return set()


async def validate_dashboard_queries(
    client: GrafanaClient,
    spec: DashboardSpec,
    catalog: list[MetricEntry] | None = None,
    *,
    catalog_authoritative: bool = False,
) -> tuple[DashboardSpec, list[str]]:
    """Validate dashboard queries independently. Returns (filtered_spec, warnings).

    Each query is judged on three independent axes — *exists* (in the routed
    datasource catalog), *syntax valid*, and *returns data in-window* — and the
    failing query is dropped, not the whole panel. A panel survives if at least
    one of its queries returns data (or is an unprobeable type). When ``catalog``
    is provided, queries whose datasource UID is unknown or whose metric is
    absent from an explicitly authoritative catalog are dropped and flagged
    (hallucination/routing). Discovered catalogs are advisory by default because
    provider discovery may be capped or filtered; their misses are probed rather
    than treated as proof that a metric does not exist.
    """
    valid_panels: list[PanelSpec] = []
    warnings: list[str] = []

    if not any(panel.queries for panel in spec.panels):
        return spec, ["No queries to validate"]

    catalog_names_by_uid: dict[str, set[str]] = {}
    if catalog:
        for entry in catalog:
            if entry.datasource_uid:
                catalog_names_by_uid.setdefault(entry.datasource_uid, set())
                if entry.name:
                    catalog_names_by_uid[entry.datasource_uid].add(entry.name)

    # Apply static routing checks before probing. Metric existence checks are
    # static only when the caller explicitly marks the catalog authoritative.
    flat: list[tuple[int, PanelQuery]] = [
        (panel_idx, q) for panel_idx, panel in enumerate(spec.panels) for q in panel.queries
    ]

    async def _validate_query(query: PanelQuery) -> str:
        if catalog:
            if query.datasource_uid not in catalog_names_by_uid:
                return QUERY_BAD_UID
            owned_names = catalog_names_by_uid[query.datasource_uid]
            is_promql = (query.query_language or "").lower() == "promql" or (
                not query.query_language and (query.datasource_type or "").lower() == "prometheus"
            )
            # Empty owned_names means discovery found the datasource target but
            # could not enumerate its metrics. Routing is still known, so defer
            # existence/data checks to the backend probe instead of declaring
            # every query absent. Non-Prometheus queries have provider-specific
            # identifiers and must not be parsed as PromQL.
            if catalog_authoritative and owned_names and is_promql:
                refs = _referenced_metrics(query.expr, "promql")
                if refs and not refs.issubset(owned_names):
                    return QUERY_ABSENT
        return await _probe_query(
            client,
            query.datasource_uid,
            query.datasource_type,
            query.expr,
            query.query_language,
        )

    probe_results = await asyncio.gather(*[_validate_query(q) for _, q in flat], return_exceptions=True)

    # Resolve a verdict per query.
    verdicts: dict[int, str] = {}
    for i, (_, q) in enumerate(flat):
        probe = probe_results[i]
        verdicts[i] = probe if isinstance(probe, str) else QUERY_ERROR

    hallucinated = 0
    idx = 0
    for panel_idx, panel in enumerate(spec.panels):
        kept_queries: list[PanelQuery] = []
        panel_has_data = False
        for q in panel.queries:
            verdict = verdicts[idx]
            idx += 1
            if verdict in (QUERY_OK, QUERY_SKIPPED):
                kept_queries.append(q)
                panel_has_data = True
            elif verdict == QUERY_ABSENT:
                hallucinated += 1
                warnings.append(f'Panel "{panel.title}" — query dropped: metric not in catalog ({q.expr[:60]})')
                logger.warning("query_absent_from_catalog", panel=panel.title, expr=q.expr[:80])
            elif verdict == QUERY_BAD_UID:
                warnings.append(
                    f'Panel "{panel.title}" — query dropped: datasource not discovered ({q.datasource_uid})'
                )
                logger.warning("query_bad_uid", panel=panel.title, uid=q.datasource_uid)
            elif verdict == QUERY_SYNTAX:
                warnings.append(f'Panel "{panel.title}" — query dropped: invalid syntax ({q.expr[:60]})')
            elif verdict == QUERY_EMPTY:
                warnings.append(f'Panel "{panel.title}" — query dropped: no series in window ({q.expr[:60]})')
            else:  # QUERY_ERROR
                warnings.append(f'Panel "{panel.title}" — query dropped: validation error ({q.expr[:60]})')

        if panel_has_data and kept_queries:
            valid_panels.append(panel.model_copy(update={"queries": kept_queries}))
        else:
            warnings.append(f'Panel "{panel.title}" dropped — no matching series')
            logger.warning("panel_no_data", panel=panel.title, queries=[q.expr[:80] for q in panel.queries])

    spec = spec.model_copy(update={"panels": valid_panels})
    if not valid_panels:
        warnings.append("ALL panels returned no data — dashboard not created")

    logger.info(
        "query_validation_complete",
        valid_panels=len(valid_panels),
        hallucinated_queries=hallucinated,
        warnings=len(warnings),
    )

    return spec, warnings


# ── SignalFx validation ──────────────────────────────────────────────────────


def _extract_signalflow_metrics(expr: str) -> list[str]:
    """Extract metric names from SignalFlow data('metric_name') calls."""
    return _SFX_DATA_RE.findall(expr)


async def _check_metric_exists(
    sfx_client: SignalFxClient,
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
    sfx_client: SignalFxClient,
    spec: DashboardSpec,
) -> tuple[DashboardSpec, list[str]]:
    """Validate SignalFlow panels by checking that referenced metrics exist.

    For each panel, extracts metric names from data('...') calls and verifies
    they exist in SignalFx. Drops panels where ALL referenced metrics are missing.
    Returns (filtered_spec, warnings).
    """

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
        logger.warning("sfx_validation_no_metrics", reason="no data() calls found in any panel")
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
                f'Panel "{panel.title}" dropped — metrics not found in SignalFx: ' f'{", ".join(metrics[:5])}'
            )
            logger.warning("sfx_panel_no_data", panel=panel.title, missing_metrics=metrics[:5])

    spec = spec.model_copy(update={"panels": valid_panels})
    if not valid_panels:
        warnings.append("ALL panels returned no data — dashboard not created")

    logger.info(
        "sfx_query_validation_complete",
        total_panels=len(valid_panels) + len(warnings),
        valid_panels=len(valid_panels),
        dropped=len(warnings),
        metrics_checked=len(all_metrics),
        metrics_found=sum(1 for v in cache.values() if v),
    )

    return spec, warnings
