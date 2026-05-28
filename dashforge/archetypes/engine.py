"""Archetype engine — resolves templates into concrete DashboardSpec.

Given an archetype + intent + discovered label values, deterministically
compiles query templates into real PromQL or SignalFlow depending on the
target backend. No LLM needed for query generation.
"""
from __future__ import annotations

import re

import structlog

from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.models.schemas import (
    DashboardSpec,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
)

logger = structlog.get_logger()

# Characters that are special in RE2 (used by PromQL) and need escaping.
# Note: dash `-` is NOT special in RE2 outside character classes.
_RE2_SPECIAL = frozenset(r"\.+*?()[]{}|^$")


def _re2_escape(s: str) -> str:
    """Escape a string for safe use in PromQL regex matchers."""
    return "".join(f"\\{c}" if c in _RE2_SPECIAL else c for c in s)


def _find_best_label(
    intent: Intent,
    catalog: list[MetricEntry],
    label_priority: dict[str, int] | None = None,
    restrict_to: set[str] | None = None,
) -> tuple[str, str] | None:
    """Find the best (label_name, value) pair for the target service.

    Shared logic for both PromQL and SignalFlow filter resolution.
    """
    if not intent.services:
        return None

    target = intent.services[0].lower().replace(" ", "-")
    _LABEL_PRIORITY = label_priority or {"service": 0, "app": 1, "application": 1, "container": 2, "pod": 3}
    candidates: list[tuple[int, str, str]] = []

    for entry in catalog:
        for dim in entry.dimensions:
            match = re.match(r"(\w+)=\{(.+)\}", dim)
            if not match:
                continue
            label_name, values_str = match.group(1), match.group(2)
            if restrict_to and label_name not in restrict_to:
                continue
            values = [v.strip() for v in values_str.split(",")]
            for val in values:
                val_normalized = val.lower().replace("_", "-")
                if target in val_normalized or val_normalized in target:
                    priority = _LABEL_PRIORITY.get(label_name, 10)
                    candidates.append((priority, label_name, val))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1], candidates[0][2]
    return None


def _resolve_service_filter(
    intent: Intent,
    catalog: list[MetricEntry],
) -> str:
    """Build the PromQL label selector for the target service."""
    result = _find_best_label(intent, catalog)
    if result:
        label_name, val = result
        return f'{label_name}="{val}"'

    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f'service=~".*{_re2_escape(target)}.*"'


def _resolve_container_filter(
    intent: Intent,
    catalog: list[MetricEntry],
) -> str:
    """Build PromQL label selector for container-level metrics."""
    result = _find_best_label(intent, catalog, restrict_to={"container", "pod"})
    if result:
        label_name, val = result
        return f'{label_name}="{val}"'

    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f'container=~".*{_re2_escape(target)}.*"'


def _get_datasource_uid(catalog: list[MetricEntry]) -> str:
    """Get the datasource UID from the catalog (first entry)."""
    if catalog:
        return catalog[0].datasource_uid
    return ""


def _resolve_rate_interval(intent: Intent) -> str:
    """Choose an appropriate rate() interval based on the timerange."""
    tr = intent.timerange.lower()
    if "5m" in tr or "10m" in tr or "15m" in tr:
        return "1m"
    if "30m" in tr:
        return "2m"
    return "5m"


# ── SignalFlow filter resolvers ──────────────────────────────────────────────

def _resolve_sfx_service_filter(intent: Intent, catalog: list[MetricEntry]) -> str:
    """Build a SignalFlow filter() expression for the target service."""
    result = _find_best_label(intent, catalog)
    if result:
        label_name, val = result
        return f"filter('{label_name}', '{val}')"
    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f"filter('service', '*{target}*')"


def _resolve_sfx_container_filter(intent: Intent, catalog: list[MetricEntry]) -> str:
    """Build a SignalFlow filter() expression for container-level metrics."""
    result = _find_best_label(intent, catalog, restrict_to={"container", "pod"})
    if result:
        label_name, val = result
        return f"filter('{label_name}', '{val}')"
    if not intent.services:
        return ""
    target = intent.services[0].lower().replace(" ", "-")
    return f"filter('container', '*{target}*')"


def _promql_template_to_signalflow(
    expr_template: str,
    service_filter: str,
    container_filter: str,
    legend: str,
) -> str:
    """Convert a PromQL archetype template expression directly to SignalFlow.

    Handles the archetype patterns deterministically:
    - histogram_quantile(X, sum(rate(metric_bucket{filter}[interval])) by (le))
      → data('metric', filter=...).percentile(pct=X*100)
    - sum(rate(metric{filter}[interval])) by (dim)
      → data('metric', filter=..., rollup='rate').sum(by=['dim'])
    - rate(metric{filter}[interval])
      → data('metric', filter=..., rollup='rate')
    - increase(metric{filter}[interval])
      → data('metric', filter=..., rollup='delta')
    - metric{filter}
      → data('metric', filter=...)
    - ratio: expr / expr
      → (A / B)
    """
    expr = expr_template.strip()

    # Helper: extract filter string from {service_filter} or {container_filter}
    def _filter_for(content: str) -> str:
        """Map placeholder content to the resolved SignalFlow filter."""
        content = content.strip()
        if not content:
            return ""
        if "container_filter" in expr_template and content == container_filter.replace("filter(", "").rstrip(")"):
            return container_filter
        return service_filter

    def _build_sfx_filter(label_block: str) -> str:
        """Parse a PromQL label block and return a SignalFlow filter."""
        # The label block has already had {service_filter} etc. substituted
        # with the SignalFlow filter() strings. We just need to join them.
        parts = []
        # Split on comma, but respect nested parens
        depth = 0
        current = ""
        for ch in label_block:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            if ch == ',' and depth == 0:
                current = current.strip()
                if current:
                    parts.append(current)
                current = ""
            else:
                current += ch
        current = current.strip()
        if current:
            parts.append(current)

        filters = []
        for p in parts:
            p = p.strip()
            if p.startswith("filter("):
                filters.append(p)
            elif "=~" in p:
                # status=~"5.." → filter('status', '*')
                k, _ = p.split("=~", 1)
                filters.append(f"filter('{k.strip()}', '*')")
            elif "=" in p:
                k, v = p.split("=", 1)
                v = v.strip().strip('"')
                filters.append(f"filter('{k.strip()}', '{v}')")
        return " and ".join(filters) if filters else ""

    # ── ratio: expr / expr ──
    # Split on top-level /
    slash_pos = _find_top_level_slash(expr)
    if slash_pos is not None:
        left = _promql_template_to_signalflow(expr[:slash_pos].strip(), service_filter, container_filter, "_num")
        right = _promql_template_to_signalflow(expr[slash_pos + 1:].strip(), service_filter, container_filter, "_den")
        # Strip .publish() from sub-expressions
        left = re.sub(r"\.publish\([^)]*\)$", "", left)
        right = re.sub(r"\.publish\([^)]*\)$", "", right)
        return f"({left} / {right}).publish(label='{legend}')"

    # ── histogram_quantile ──
    hq = re.match(
        r'histogram_quantile\(([\d.]+),\s*sum\(rate\((\w+?)_bucket\{(.*?)\}\[.*?\]\)\)\s*by\s*\(le(?:,\s*(\w+))?\)\)',
        expr
    )
    if hq:
        pct = int(float(hq.group(1)) * 100)
        metric = hq.group(2)
        filt = _build_sfx_filter(hq.group(3))
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        by_dim = hq.group(4)
        if by_dim and by_dim != "le":
            return f"{base}.percentile(pct={pct}, by=['{by_dim}']).publish(label='{legend}')"
        return f"{base}.percentile(pct={pct}).publish(label='{legend}')"

    # ── topk ──
    topk = re.match(r'topk\((\d+),\s*(.+)\)$', expr, re.DOTALL)
    if topk:
        k = topk.group(1)
        inner = _promql_template_to_signalflow(topk.group(2), service_filter, container_filter, legend)
        inner = re.sub(r"\.publish\([^)]*\)$", "", inner)
        return f"{inner}.top(count={k}).publish(label='{legend}')"

    # ── agg(rate/increase(metric{labels}[interval])) by (dims) ──
    agg = re.match(
        r'(sum|avg|count|min|max)\((rate|increase)\((\w+)\{(.*?)\}\[.*?\]\)\)(?:\s*by\s*\(([^)]+)\))?',
        expr
    )
    if agg:
        agg_fn = agg.group(1)
        func = agg.group(2)
        metric = agg.group(3)
        filt = _build_sfx_filter(agg.group(4))
        by_dims = agg.group(5)
        rollup = "rate" if func == "rate" else "delta"
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += f", rollup='{rollup}')"
        if by_dims:
            dims = [d.strip() for d in by_dims.split(",") if d.strip() != "le"]
            if dims:
                base += f".{agg_fn}(by={dims})"
            else:
                base += f".{agg_fn}()"
        else:
            base += f".{agg_fn}()"
        return f"{base}.publish(label='{legend}')"

    # ── bare rate/increase ──
    rate = re.match(r'(rate|increase)\((\w+)\{(.*?)\}\[.*?\]\)', expr)
    if rate:
        func = rate.group(1)
        metric = rate.group(2)
        filt = _build_sfx_filter(rate.group(3))
        rollup = "rate" if func == "rate" else "delta"
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += f", rollup='{rollup}')"
        return f"{base}.publish(label='{legend}')"

    # ── simple metric{labels} ──
    simple = re.match(r'(\w+)\{(.*?)\}$', expr)
    if simple:
        metric = simple.group(1)
        filt = _build_sfx_filter(simple.group(2))
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        return f"{base}.publish(label='{legend}')"

    # ── bare metric name ──
    bare = re.match(r'^(\w+)$', expr)
    if bare:
        return f"data('{bare.group(1)}').publish(label='{legend}')"

    # Fallback
    logger.warning("signalflow_compile_fallback", expr=expr[:100])
    return f"data('{expr}').publish(label='{legend}')"


def _find_top_level_slash(expr: str) -> int | None:
    """Find position of top-level '/' operator (not inside parens)."""
    depth = 0
    for i, ch in enumerate(expr):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif ch == '/' and depth == 0 and i > 0:
            return i
    return None


_HISTOGRAM_SUFFIXES = ("_bucket", "_count", "_sum", "_total", "_created", "_info")


def _suffix_aware_replace(expr: str, old_metric: str, new_metric: str) -> str:
    """Replace *old_metric* with *new_metric* in *expr*, handling suffixes.

    When *old_metric* is ``http_request_duration_seconds`` and the expression
    contains ``http_request_duration_seconds_bucket``, a naive ``.replace()``
    with a *new_metric* of ``custom_duration_seconds_bucket`` would produce
    ``custom_duration_seconds_bucket_bucket``.

    This function:
    1. Replaces suffixed variants first (longest match first) — if the new
       metric already ends with that suffix, only the base portion is used
       for substitution so the suffix is not doubled.
    2. Replaces the bare base metric last.
    """
    # Sort suffixes longest-first to avoid partial matches
    suffixes = sorted(_HISTOGRAM_SUFFIXES, key=len, reverse=True)

    # Replace suffixed variants first
    for suffix in suffixes:
        old_suffixed = old_metric + suffix
        if old_suffixed not in expr:
            continue
        if new_metric.endswith(suffix):
            # new_metric already has this suffix — use it as-is
            new_suffixed = new_metric
        else:
            new_suffixed = new_metric + suffix
        expr = expr.replace(old_suffixed, new_suffixed)

    # Replace remaining bare base metric occurrences
    # (only hits instances that are NOT part of a suffixed variant, since
    # those were already replaced above)
    if old_metric in expr:
        expr = expr.replace(old_metric, new_metric)

    return expr


def _apply_metric_substitutions(
    archetype: InvestigationArchetype,
    substitutions: dict[str, str],
) -> InvestigationArchetype:
    """Return a copy of the archetype with metric names substituted in queries.

    Used when signal resolution finds that the default metric names in the
    archetype templates don't exist in the environment, but equivalent
    metrics do (e.g. auth_requests_total → sso_auth_requests_total).

    Suffix-aware: if the template references ``base_metric_bucket`` and the
    substitution maps ``base_metric`` → ``new_metric``, the result is
    ``new_metric_bucket`` (not ``new_metric_bucket`` from a naive replace
    that could also cause ``new_metric_bucket_bucket`` when the resolved
    metric is already suffixed).
    """
    if not substitutions:
        return archetype

    new_panels = []
    for panel in archetype.panels:
        new_queries = []
        for qt in panel.queries:
            expr = qt.expr
            for old_metric, new_metric in substitutions.items():
                expr = _suffix_aware_replace(expr, old_metric, new_metric)
            new_queries.append(QueryTemplate(
                expr=expr,
                legend_format=qt.legend_format,
                datasource_type=qt.datasource_type,
            ))
        new_panels.append(PanelTemplate(
            title=panel.title,
            description=panel.description,
            panel_type=panel.panel_type,
            row=panel.row,
            queries=new_queries,
            unit=panel.unit,
        ))

    return InvestigationArchetype(
        id=archetype.id,
        name=archetype.name,
        description=archetype.description,
        problem_types=archetype.problem_types,
        required_metrics=archetype.required_metrics,
        required_signals=archetype.required_signals,
        signal_bindings=archetype.signal_bindings,
        panels=new_panels,
        tags=archetype.tags,
        default_timerange=archetype.default_timerange,
    )


def _resolve_archetype_signals(
    archetype: InvestigationArchetype,
    catalog: list[MetricEntry],
    intent: Intent,
) -> InvestigationArchetype:
    """Resolve signal bindings and substitute metrics if needed.

    If the archetype has signal_bindings and any default metrics are missing
    from the catalog, the signal store is consulted to find alternatives.
    Returns the (possibly modified) archetype.
    """
    if not archetype.signal_bindings:
        return archetype

    try:
        from dashforge.signals import get_signal_store
        store = get_signal_store()
        substitutions = store.resolve_signals_for_archetype(
            signal_bindings=archetype.signal_bindings,
            catalog=catalog,
            context_service=intent.services[0] if intent.services else "",
            context_archetype=archetype.id,
        )
        if substitutions:
            logger.info(
                "archetype_signals_resolved",
                archetype=archetype.id,
                substitutions=substitutions,
            )
            return _apply_metric_substitutions(archetype, substitutions)
    except Exception:
        logger.warning("signal_resolution_failed", archetype=archetype.id, exc_info=True)

    return archetype


def compile_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
    target_language: str = "promql",
) -> DashboardSpec:
    """Compile an archetype template into a concrete DashboardSpec.

    This is fully deterministic — no LLM call needed.
    Resolves {service_filter}, {container_filter}, {rate_interval}
    from the intent and catalog.

    If the archetype has signal_bindings, metric names are resolved via the
    signal store before template compilation.

    target_language: 'promql' (default) or 'signalflow'
    """
    # Resolve signals → actual metrics before compiling templates
    archetype = _resolve_archetype_signals(archetype, catalog, intent)

    rate_interval = _resolve_rate_interval(intent)

    if target_language == "signalflow":
        service_filter = _resolve_sfx_service_filter(intent, catalog)
        container_filter = _resolve_sfx_container_filter(intent, catalog)
        datasource_uid = "signalfx-direct"
        datasource_type = "signalfx"
    else:
        service_filter = _resolve_service_filter(intent, catalog)
        container_filter = _resolve_container_filter(intent, catalog)
        datasource_uid = _get_datasource_uid(catalog)
        datasource_type = "prometheus"

    # Available metric names for validation
    available_metrics = {e.name for e in catalog}

    params = {
        "service_filter": service_filter,
        "container_filter": container_filter,
        "rate_interval": rate_interval,
    }

    panels: list[PanelSpec] = []
    skipped = 0

    for pt in archetype.panels:
        panel_queries: list[PanelQuery] = []
        for qt in pt.queries:
            try:
                expr = qt.expr.format(**params)
            except KeyError as e:
                logger.warning("archetype_placeholder_missing", panel=pt.title, key=str(e))
                continue

            if target_language == "signalflow":
                # Compile the resolved PromQL template directly to SignalFlow
                legend = qt.legend_format or pt.title
                expr = _promql_template_to_signalflow(
                    expr, service_filter, container_filter, legend
                )

            panel_queries.append(PanelQuery(
                expr=expr,
                legend_format=qt.legend_format,
                datasource_uid=datasource_uid,
                datasource_type=datasource_type,
            ))

        if not panel_queries:
            skipped += 1
            continue

        panels.append(PanelSpec(
            title=pt.title,
            description=pt.description,
            panel_type=pt.panel_type,
            row=pt.row,
            queries=panel_queries,
            unit=pt.unit,
        ))

    # Build title from archetype name + service
    service_name = intent.services[0] if intent.services else "Service"
    title = f"{service_name.title()} — {archetype.name}"

    spec = DashboardSpec(
        title=title,
        tags=archetype.tags + ["dashforge", "archetype"],
        timerange=intent.timerange or archetype.default_timerange,
        panels=panels,
    )

    logger.info(
        "archetype_compiled",
        archetype=archetype.id,
        panels=len(panels),
        skipped=skipped,
        service_filter=service_filter,
        rate_interval=rate_interval,
        language=target_language,
    )

    return spec


def blend_archetypes(
    ranked_archetypes: list[tuple["InvestigationArchetype", float]],
    intent: Intent,
    catalog: list[MetricEntry],
    secondary_min_confidence: float = 0.4,
    target_language: str = "promql",
) -> DashboardSpec:
    """Blend panels from multiple archetypes into a single dashboard.

    The primary (highest-confidence) archetype contributes all its panels.
    Secondary archetypes contribute panels whose titles don't duplicate the
    primary's, giving broader investigation coverage without redundancy.

    Parameters
    ----------
    ranked_archetypes : list[tuple[InvestigationArchetype, float]]
        (archetype, confidence) pairs, highest confidence first.
    intent : Intent
        The classified user intent.
    catalog : list[MetricEntry]
        Discovered metrics from datasources.
    secondary_min_confidence : float
        Minimum confidence for secondary archetypes to contribute panels.
    target_language : str
        'promql' (default) or 'signalflow'
    """
    if not ranked_archetypes:
        raise ValueError("blend_archetypes called with empty archetype list")

    primary_arch, primary_conf = ranked_archetypes[0]
    primary_spec = compile_archetype(primary_arch, intent, catalog, target_language=target_language)

    # Track existing panel titles to avoid duplicates
    existing_titles: set[str] = {p.title.lower() for p in primary_spec.panels}
    blended_panels = list(primary_spec.panels)
    blended_tags = list(primary_spec.tags)

    for arch, conf in ranked_archetypes[1:]:
        if conf < secondary_min_confidence:
            continue

        secondary_spec = compile_archetype(arch, intent, catalog, target_language=target_language)
        added = 0
        for panel in secondary_spec.panels:
            if panel.title.lower() not in existing_titles:
                # Tag panel with its source archetype for traceability
                panel_with_row = panel.model_copy(
                    update={"row": panel.row or arch.name}
                )
                blended_panels.append(panel_with_row)
                existing_titles.add(panel.title.lower())
                added += 1

        if added > 0:
            blended_tags.extend(arch.tags)
            logger.info(
                "archetype_blended",
                secondary=arch.id,
                confidence=conf,
                panels_added=added,
            )

    # Build final title
    service_name = intent.services[0] if intent.services else "Service"
    arch_names = " + ".join(
        a.name for a, c in ranked_archetypes[:3] if c >= secondary_min_confidence
    )
    title = f"{service_name.title()} — {arch_names}"

    spec = DashboardSpec(
        title=title,
        tags=list(dict.fromkeys(blended_tags)),  # dedupe preserving order
        timerange=intent.timerange or primary_arch.default_timerange,
        panels=blended_panels,
    )

    logger.info(
        "archetype_blend_complete",
        primary=primary_arch.id,
        primary_confidence=primary_conf,
        total_archetypes=len(ranked_archetypes),
        total_panels=len(blended_panels),
    )

    return spec
