"""Archetype engine — resolves templates into concrete DashboardSpec.

Given an archetype + intent + discovered label values, deterministically
compiles query templates into real PromQL or SignalFlow depending on the
target backend. No LLM needed for query generation.
"""

from __future__ import annotations

import re

import structlog

from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.catalog import catalog_for_services, metric_matches_services
from tacit.config import settings
from tacit.models.schemas import (
    DashboardSpec,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    QueryTarget,
)

logger = structlog.get_logger()

_PROMETHEUS_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")

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


_PROMETHEUS_DATASOURCE_TYPES = {"prometheus", "mimir", "cortex", "thanos"}
_SIGNALFX_DATASOURCE_TYPES = {"signalfx", "grafana-signalfx-datasource"}


def _datasource_type_matches(candidate: str, requested: str) -> bool:
    candidate = candidate.lower()
    requested = requested.lower()
    if not requested:
        return True
    if candidate == requested:
        return True
    if candidate in _PROMETHEUS_DATASOURCE_TYPES and requested in _PROMETHEUS_DATASOURCE_TYPES:
        return True
    if candidate in _SIGNALFX_DATASOURCE_TYPES and requested in _SIGNALFX_DATASOURCE_TYPES:
        return True
    return False


def _datasource_type_for_language(query_language: str, fallback: str = "prometheus") -> str:
    return {
        "signalflow": "signalfx",
        "logql": "loki",
        "cloudwatch": "cloudwatch",
        "lucene": "elasticsearch",
        "graphite": "graphite",
        "influxql": "influxdb",
    }.get(query_language.lower(), fallback)


def _resolve_query_target(
    catalog: list[MetricEntry],
    datasource_type: str = "",
    query_language: str = "",
    fallback_uid: str = "",
) -> QueryTarget:
    """Resolve datasource identity as one object, preferring matching catalog entries."""
    query_language = query_language.lower()
    datasource_type = datasource_type.lower()
    matched_entries: list[MetricEntry] = []
    for entry in catalog:
        if datasource_type and not _datasource_type_matches(entry.datasource_type, datasource_type):
            continue
        if query_language and (entry.query_language or "").lower() != query_language:
            continue
        matched_entries.append(entry)
    for entry in matched_entries:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if matched_entries:
        return QueryTarget.from_metric(matched_entries[0])
    type_matched_entries: list[MetricEntry] = []
    for entry in catalog:
        if datasource_type and _datasource_type_matches(entry.datasource_type, datasource_type):
            type_matched_entries.append(entry)
    for entry in type_matched_entries:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if type_matched_entries:
        return QueryTarget.from_metric(type_matched_entries[0])
    if datasource_type or query_language:
        return QueryTarget(
            datasource_uid=fallback_uid,
            datasource_type=datasource_type or _datasource_type_for_language(query_language, ""),
            query_language=query_language,
        )
    for entry in catalog:
        if entry.datasource_is_default:
            return QueryTarget.from_metric(entry)
    if catalog:
        return QueryTarget.from_metric(catalog[0])
    return QueryTarget(
        datasource_uid=fallback_uid,
        datasource_type=datasource_type,
        query_language=query_language,
    )


def _resolve_promql_query_target(
    catalog: list[MetricEntry],
    expr: str,
    default_target: QueryTarget,
    intent: Intent,
) -> QueryTarget:
    """Route a PromQL query to the datasource that actually owns its metrics."""
    from tacit.dashboard_ingest import extract_metrics_from_promql

    metric_names = set(extract_metrics_from_promql(expr))
    if metric_names:
        candidates = [
            entry
            for entry in catalog
            if entry.name in metric_names
            and _datasource_type_matches(entry.datasource_type, "prometheus")
            and (not entry.query_language or entry.query_language.lower() == "promql")
        ]
        if len(metric_names) == 1 and len(candidates) == 1:
            return QueryTarget.from_metric(candidates[0])

        owners_by_metric = {
            metric: {entry.datasource_uid for entry in candidates if entry.name == metric} for metric in metric_names
        }
        if owners_by_metric and all(owners_by_metric.values()):
            common_owners = set.intersection(*owners_by_metric.values())
            if len(common_owners) == 1:
                owner = next(iter(common_owners))
                return QueryTarget.from_metric(next(entry for entry in candidates if entry.datasource_uid == owner))

        service_candidates = [entry for entry in candidates if metric_matches_services(entry, intent.services)]
        service_owners = {entry.datasource_uid for entry in service_candidates}
        complete_service_owners = {
            owner
            for owner in service_owners
            if all(owner in owners_by_metric.get(metric, set()) for metric in metric_names)
        }
        if len(complete_service_owners) == 1:
            owner = next(iter(complete_service_owners))
            return QueryTarget.from_metric(next(entry for entry in service_candidates if entry.datasource_uid == owner))
    return default_target


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
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if ch == "," and depth == 0:
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
        right = _promql_template_to_signalflow(expr[slash_pos + 1 :].strip(), service_filter, container_filter, "_den")
        # Strip .publish() from sub-expressions
        left = re.sub(r"\.publish\([^)]*\)$", "", left)
        right = re.sub(r"\.publish\([^)]*\)$", "", right)
        return f"({left} / {right}).publish(label='{legend}')"

    # ── histogram_quantile ──
    hq = re.match(
        r"histogram_quantile\(([\d.]+),\s*sum\(rate\(([\w.:]+?)_bucket\{(.*?)\}\[.*?\]\)\)\s*by\s*\(le(?:,\s*(\w+))?\)\)",
        expr,
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
    topk = re.match(r"topk\((\d+),\s*(.+)\)$", expr, re.DOTALL)
    if topk:
        k = topk.group(1)
        inner = _promql_template_to_signalflow(topk.group(2), service_filter, container_filter, legend)
        inner = re.sub(r"\.publish\([^)]*\)$", "", inner)
        return f"{inner}.top(count={k}).publish(label='{legend}')"

    # ── agg(rate/increase(metric{labels}[interval])) by (dims) ──
    agg = re.match(
        r"(sum|avg|count|min|max)\((rate|increase)\(([\w.:]+)\{(.*?)\}\[.*?\]\)\)(?:\s*by\s*\(([^)]+)\))?",
        expr,
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
    rate = re.match(r"(rate|increase)\(([\w.:]+)\{(.*?)\}\[.*?\]\)", expr)
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
    simple = re.match(r"([\w.:]+)\{(.*?)\}$", expr)
    if simple:
        metric = simple.group(1)
        filt = _build_sfx_filter(simple.group(2))
        base = f"data('{metric}'"
        if filt:
            base += f", filter={filt}"
        base += ")"
        return f"{base}.publish(label='{legend}')"

    # ── bare metric name ──
    bare = re.match(r"^([\w.:]+)$", expr)
    if bare:
        return f"data('{bare.group(1)}').publish(label='{legend}')"

    # Fallback
    logger.warning("signalflow_compile_fallback", expr=expr[:100])
    return f"data('{expr}').publish(label='{legend}')"


def _find_top_level_slash(expr: str) -> int | None:
    """Find position of top-level '/' operator (not inside parens)."""
    depth = 0
    for i, ch in enumerate(expr):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "/" and depth == 0 and i > 0:
            return i
    return None


_HISTOGRAM_SUFFIXES = ("_bucket", "_count", "_sum", "_total", "_created", "_info")


_METRIC_TOKEN_CHARS = r"A-Za-z0-9_:."


def _metric_name_pattern(metric_name: str) -> re.Pattern[str]:
    """Match a metric name as a complete metric token, not a substring."""
    return re.compile(rf"(?<![{_METRIC_TOKEN_CHARS}]){re.escape(metric_name)}(?![{_METRIC_TOKEN_CHARS}])")


def _strip_known_metric_suffix(metric_name: str) -> tuple[str, str]:
    """Return (base, suffix) if *metric_name* ends in a known metric suffix."""
    for suffix in sorted(_HISTOGRAM_SUFFIXES, key=len, reverse=True):
        if metric_name.endswith(suffix):
            return metric_name[: -len(suffix)], suffix
    return metric_name, ""


def _suffix_aware_replace(expr: str, old_metric: str, new_metric: str) -> str:
    """Replace *old_metric* with *new_metric* in *expr*, handling suffixes.

    The replacement is token-aware and suffix-aware:

    * ``old_metric_bucket`` becomes ``new_metric_bucket``;
    * if ``new_metric`` already has that same suffix, the suffix is not doubled;
    * if ``new_metric`` has a different known suffix, that suffix is stripped
      before appending the suffix from the expression; and
    * bare replacements are bounded to metric-token characters so similarly
      named metrics and already-replaced text are not rewritten accidentally.
    """
    if not old_metric or old_metric == new_metric:
        return expr

    protected: dict[str, str] = {}

    def protect(value: str) -> str:
        token = f"__TACIT_METRIC_TOKEN_{len(protected)}__"
        protected[token] = value
        return token

    new_base, new_suffix = _strip_known_metric_suffix(new_metric)

    # Replace suffixed variants first and protect replacements so the later bare
    # pass cannot rewrite inside a new metric that happens to contain old_metric.
    for suffix in sorted(_HISTOGRAM_SUFFIXES, key=len, reverse=True):
        old_suffixed = old_metric + suffix
        if new_metric.endswith(suffix):
            new_suffixed = new_metric
        elif new_suffix:
            new_suffixed = new_base + suffix
        else:
            new_suffixed = new_metric + suffix

        def replace_suffixed(_match: re.Match[str], replacement: str = new_suffixed) -> str:
            return protect(replacement)

        expr = _metric_name_pattern(old_suffixed).sub(
            replace_suffixed,
            expr,
        )

    expr = _metric_name_pattern(old_metric).sub(
        lambda _m: protect(new_metric),
        expr,
    )

    for token, value in protected.items():
        expr = expr.replace(token, value)
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
            new_queries.append(
                QueryTemplate(
                    expr=expr,
                    legend_format=qt.legend_format,
                    query_language=qt.query_language,
                    datasource_type=qt.datasource_type,
                    cloudwatch_namespace=qt.cloudwatch_namespace,
                    cloudwatch_stat=qt.cloudwatch_stat,
                    cloudwatch_dimensions=qt.cloudwatch_dimensions,
                    cloudwatch_region=qt.cloudwatch_region,
                )
            )
        new_panels.append(
            PanelTemplate(
                title=panel.title,
                description=panel.description,
                panel_type=panel.panel_type,
                row=panel.row,
                queries=new_queries,
                unit=panel.unit,
            )
        )

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


def _legacy_metric_signal(
    store,
    default_metric: str,
    catalog: list[MetricEntry],
    target_language: str,
) -> str:
    """Infer the taxonomy signal represented by a legacy required metric."""
    if not catalog:
        return ""
    exemplar = catalog[0]
    pseudo = MetricEntry(
        name=default_metric,
        datasource_uid=exemplar.datasource_uid,
        datasource_name=exemplar.datasource_name,
        datasource_type=exemplar.datasource_type,
        query_language=target_language or exemplar.query_language,
    )
    candidates: list[tuple[str, float]] = []
    for signal in store.list_signal_types():
        signal_type = str(signal.get("signal_type", ""))
        if not signal_type:
            continue
        matches = store.resolve_signal(
            signal_type,
            [pseudo],
            target_query_language=target_language,
        )
        if matches:
            candidates.append((signal_type, matches[0][1]))
    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates[0][0] if candidates else ""


def _substitution_shape_compatible(
    archetype: InvestigationArchetype,
    default_metric: str,
    candidate: MetricEntry,
) -> bool:
    """Reject semantic substitutions that would change the required query shape."""
    expressions = [query.expr for panel in archetype.panels for query in panel.queries if default_metric in query.expr]
    if not expressions:
        return False
    name = candidate.name
    metric_type = (candidate.metric_type or "").lower()
    if any(f"{default_metric}_bucket" in expr for expr in expressions):
        return name.endswith("_bucket") or metric_type == "histogram"
    rate_pattern = re.compile(rf"\b(?:rate|irate|increase)\([^)]*\b{re.escape(default_metric)}\b")
    uses_rate = any(rate_pattern.search(expr) for expr in expressions)
    counter_shaped = metric_type in {"counter", "histogram", "summary"} or name.endswith(
        ("_total", "_count", "_sum", "_bucket")
    )
    return not uses_rate or counter_shaped


def _unambiguous_legacy_candidate(
    resolved: list[tuple[MetricEntry, float]],
    archetype: InvestigationArchetype,
    default_metric: str,
) -> tuple[MetricEntry, float] | None:
    """Return a unique best compatible metric, or abstain on an unresolved tie.

    Raw datasets often encode service identity in the metric name instead of a
    label.  Picking the first equally scored service would make a valid query,
    but not a justified one.
    """
    compatible = [
        (candidate, confidence)
        for candidate, confidence in resolved
        if _substitution_shape_compatible(archetype, default_metric, candidate)
    ]
    if not compatible:
        return None
    best_confidence = compatible[0][1]
    best = [item for item in compatible if item[1] == best_confidence]
    best_names = {candidate.name for candidate, _ in best}
    return best[0] if len(best_names) == 1 else None


def _resolve_legacy_required_metrics(
    archetype: InvestigationArchetype,
    store,
    catalog: list[MetricEntry],
    intent: Intent,
    target_language: str,
) -> dict[str, str]:
    """Resolve legacy required_metrics through the semantic taxonomy."""
    target_datasource_type = _datasource_type_for_language(target_language)
    target_catalog = [
        entry
        for entry in catalog
        if (not target_language or (entry.query_language or "").lower() == target_language.lower())
        and _datasource_type_matches(entry.datasource_type, target_datasource_type)
    ]
    resolution_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=True)
    catalog_names = {entry.name for entry in resolution_catalog}
    substitutions: dict[str, str] = {}
    for default_metric in archetype.required_metrics:
        if default_metric in catalog_names:
            continue
        signal_type = _legacy_metric_signal(store, default_metric, target_catalog, target_language)
        if not signal_type:
            continue
        resolved = store.resolve_signal(
            signal_type,
            resolution_catalog,
            context_service=intent.services[0] if intent.services else "",
            context_datasource_type=target_datasource_type,
            context_archetype=archetype.id,
            target_query_language=target_language,
        )
        selected = _unambiguous_legacy_candidate(resolved, archetype, default_metric)
        if selected is None:
            if resolved:
                logger.info(
                    "legacy_metric_signal_ambiguous",
                    archetype=archetype.id,
                    default_metric=default_metric,
                    signal=signal_type,
                    candidate_count=len(resolved),
                )
            continue
        candidate, confidence = selected
        substitutions[default_metric] = candidate.name
        logger.info(
            "legacy_metric_signal_resolved",
            archetype=archetype.id,
            default_metric=default_metric,
            signal=signal_type,
            resolved_to=candidate.name,
            confidence=confidence,
        )
    return substitutions


def _resolve_archetype_signals(
    archetype: InvestigationArchetype,
    catalog: list[MetricEntry],
    intent: Intent,
    target_language: str = "promql",
) -> InvestigationArchetype:
    """Resolve signal bindings and substitute metrics if needed.

    If the archetype has signal_bindings and any default metrics are missing
    from the catalog, the signal store is consulted to find alternatives.
    ``target_language`` keeps substitutions within the backend being compiled
    for (e.g. don't pull a SignalFx metric into a PromQL dashboard).
    Returns the (possibly modified) archetype.
    """
    if not archetype.signal_bindings and not archetype.required_metrics:
        return archetype

    try:
        from tacit.signals import get_signal_store

        store = get_signal_store()
        substitutions = store.resolve_signals_for_archetype(
            signal_bindings=archetype.signal_bindings,
            catalog=catalog,
            context_service=intent.services[0] if intent.services else "",
            context_datasource_type=_datasource_type_for_language(target_language),
            context_archetype=archetype.id,
            target_query_language=target_language,
        )
        legacy_substitutions = _resolve_legacy_required_metrics(
            archetype,
            store,
            catalog,
            intent,
            target_language,
        )
        for default_metric, resolved_metric in legacy_substitutions.items():
            substitutions.setdefault(default_metric, resolved_metric)
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
    archetype = _resolve_archetype_signals(archetype, catalog, intent, target_language)

    rate_interval = _resolve_rate_interval(intent)

    if target_language == "signalflow":
        service_filter = _resolve_sfx_service_filter(intent, catalog)
        container_filter = _resolve_sfx_container_filter(intent, catalog)
        default_target = _resolve_query_target(
            catalog,
            "signalfx",
            "signalflow",
            fallback_uid="signalfx-direct",
        )
    else:
        service_filter = _resolve_service_filter(intent, catalog)
        container_filter = _resolve_container_filter(intent, catalog)
        default_target = _resolve_query_target(catalog, "prometheus", "promql")

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
            # Determine whether this query is PromQL. An explicit non-PromQL
            # query_language (signalflow/logql/cloudwatch/…) — or a SignalFx
            # datasource tag — marks it as a native query to honor verbatim.
            qt_language = (qt.query_language or "promql").lower()
            if qt_language in ("", "promql") and "signalfx" in (qt.datasource_type or "").lower():
                qt_language = "signalflow"
            is_promql_query = qt_language in ("", "promql")

            if is_promql_query:
                # PromQL templates use {service_filter} etc. and need str.format.
                try:
                    expr = qt.expr.format(**params)
                except KeyError as e:
                    logger.warning("archetype_placeholder_missing", panel=pt.title, key=str(e))
                    continue
                if target_language == "signalflow":
                    # Compile the resolved PromQL template directly to SignalFlow.
                    legend = qt.legend_format or pt.title
                    expr = _promql_template_to_signalflow(expr, service_filter, container_filter, legend)
                    query_target = default_target
                else:
                    query_target = _resolve_promql_query_target(catalog, expr, default_target, intent)
            else:
                # Non-PromQL queries are honored verbatim — no PromQL
                # format/escaping or PromQL→SignalFlow conversion.
                expr = qt.expr
                if not qt.datasource_type or qt.datasource_type == "prometheus":
                    query_datasource_type = _datasource_type_for_language(qt_language, default_target.datasource_type)
                else:
                    query_datasource_type = qt.datasource_type
                query_target = _resolve_query_target(
                    catalog,
                    query_datasource_type,
                    qt_language,
                )

            panel_queries.append(
                PanelQuery(
                    expr=expr,
                    legend_format=qt.legend_format,
                    datasource_uid=query_target.datasource_uid,
                    datasource_type=query_target.datasource_type,
                    query_language=query_target.query_language,
                    cloudwatch_namespace=qt.cloudwatch_namespace,
                    cloudwatch_stat=qt.cloudwatch_stat,
                    cloudwatch_dimensions=qt.cloudwatch_dimensions,
                    cloudwatch_region=qt.cloudwatch_region,
                )
            )

        if not panel_queries:
            skipped += 1
            continue

        panels.append(
            PanelSpec(
                title=pt.title,
                description=pt.description,
                panel_type=pt.panel_type,
                row=pt.row,
                source_archetype=archetype.id,
                queries=panel_queries,
                unit=pt.unit,
            )
        )

    # Build title from archetype name + service
    service_name = intent.services[0] if intent.services else "Service"
    title = f"{service_name.title()} — {archetype.name}"

    spec = DashboardSpec(
        title=title,
        tags=archetype.tags + ["tacit", "archetype"],
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


def _archetype_query_languages(
    archetype: InvestigationArchetype,
    target_language: str,
) -> set[str]:
    """Return native query languages used by an archetype for this backend."""
    datasource_languages = {
        "cloudwatch": "cloudwatch",
        "loki": "logql",
        "graphite": "graphite",
        "influxdb": "influxql",
        "elasticsearch": "lucene",
        "opensearch": "lucene",
        "signalfx": "signalflow",
        "grafana-signalfx-datasource": "signalflow",
    }
    fallback = target_language.lower()
    languages: set[str] = set()
    for panel in archetype.panels:
        for query in panel.queries:
            datasource_type = (query.datasource_type or "").lower()
            if datasource_type in datasource_languages:
                languages.add(datasource_languages[datasource_type])
                continue
            query_language = (query.query_language or "").lower()
            if datasource_type in _PROMETHEUS_DATASOURCE_TYPES and query_language in {"", "promql"}:
                languages.add(fallback or "promql")
            elif query_language:
                languages.add(query_language)
    return languages or {fallback or "promql"}


def _archetype_live_coverage(
    archetype: InvestigationArchetype,
    catalog: list[MetricEntry],
    target_language: str = "promql",
    services: list[str] | None = None,
) -> float | None:
    """Fraction of an archetype's declared evidence covered by the live catalog.

    Evidence includes semantic signals/bindings and legacy ``required_metrics``.
    Returns ``None`` when no evidence is declared or the catalog contains only
    datasource targets without metric names, because coverage is then unknown.
    """
    signals = set(archetype.required_signals) | set(archetype.signal_bindings.keys())
    required_metrics = set(archetype.required_metrics)
    if not signals and not required_metrics:
        return None

    named_catalog = [entry for entry in catalog if entry.name]
    if not named_catalog:
        return None
    scoped_catalog = catalog_for_services(named_catalog, services or [], include_unscoped=True)
    query_languages = _archetype_query_languages(archetype, target_language)
    coverage_catalog = [entry for entry in scoped_catalog if (entry.query_language or "").lower() in query_languages]
    catalog_names = {entry.name for entry in coverage_catalog}
    if not catalog_names:
        return 0.0

    store = None
    try:
        from tacit.signals import get_signal_store

        store = get_signal_store()
    except Exception:
        store = None

    resolved = 0
    for sig in signals:
        default_metric = archetype.signal_bindings.get(sig, "")
        if default_metric and default_metric in catalog_names:
            resolved += 1
            continue
        if store is not None:
            try:
                if store.resolve_signal(
                    sig,
                    coverage_catalog,
                    context_service=services[0] if services else "",
                ):
                    resolved += 1
            except Exception:
                pass
    for required_metric in required_metrics:
        if any(
            name == required_metric
            or any(
                name.endswith(suffix) and name[: -len(suffix)] == required_metric
                for suffix in _PROMETHEUS_HISTOGRAM_SUFFIXES
            )
            for name in catalog_names
        ):
            resolved += 1

    return resolved / (len(signals) + len(required_metrics))


def rank_archetypes_by_coverage(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
    catalog: list[MetricEntry],
    *,
    target_language: str = "promql",
    services: list[str] | None = None,
    max_archetypes: int | None = None,
    min_secondary_coverage: float = 0.0,
) -> list[tuple[InvestigationArchetype, float]]:
    """Re-rank archetypes by classifier_confidence × live signal coverage.

    This prefers a strongly-matching (well-covered) archetype over numerous
    generic templates whose signals are absent from the environment, then caps
    the list so blending cannot explode into many loosely-matched archetypes.
    The primary archetype (rank 0 after re-sort) is always kept; secondaries
    below ``min_secondary_coverage`` are dropped.
    """
    if not ranked_archetypes:
        return ranked_archetypes

    scored: list[tuple[InvestigationArchetype, float, float, float]] = []
    for arch, confidence in ranked_archetypes:
        coverage = _archetype_live_coverage(arch, catalog, target_language, services)
        # Unknown coverage (no declared signals) keeps the classifier confidence.
        effective = confidence if coverage is None else confidence * coverage
        is_learned = bool({"learned", "auto-generated"} & set(arch.tags))
        if is_learned and coverage is not None and coverage >= settings.learned_archetype_min_coverage:
            # Learned and classifier retrieval scores are not calibrated on
            # the same scale. Prefer learned context only with strong live
            # evidence, and keep the adjustment deliberately bounded.
            effective += settings.learned_archetype_boost * coverage
        scored.append((arch, confidence, coverage if coverage is not None else -1.0, effective))

    scored.sort(key=lambda x: x[3], reverse=True)

    kept: list[tuple[InvestigationArchetype, float]] = []
    for rank, (arch, confidence, coverage, effective) in enumerate(scored):
        if rank > 0 and coverage >= 0.0 and coverage < min_secondary_coverage:
            logger.info(
                "archetype_dropped_low_coverage",
                archetype=arch.id,
                confidence=confidence,
                coverage=round(coverage, 3),
            )
            continue
        kept.append((arch, confidence))
        if max_archetypes is not None and len(kept) >= max_archetypes:
            break

    return kept


def _panel_signature(panel: PanelSpec) -> frozenset[tuple[str, str, str, str]] | str:
    """Identify equivalent panels without collapsing cross-datasource evidence."""
    queries = {
        (
            re.sub(r"\s+", "", query.expr.lower()),
            query.datasource_uid,
            query.datasource_type.lower(),
            query.query_language.lower(),
        )
        for query in panel.queries
        if query.expr
    }
    return frozenset(queries) if queries else panel.title.lower()


def blend_archetypes(
    ranked_archetypes: list[tuple[InvestigationArchetype, float]],
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

    # Coverage-rank and cap the archetype set BEFORE blending so a flood of
    # loosely-matched generic templates can't add dozens of irrelevant panels.
    ranked_archetypes = rank_archetypes_by_coverage(
        ranked_archetypes,
        catalog,
        target_language=target_language,
        services=intent.services,
        max_archetypes=settings.max_blended_archetypes,
        min_secondary_coverage=settings.min_secondary_coverage,
    )
    max_panels = settings.max_dashboard_panels

    primary_arch, primary_conf = ranked_archetypes[0]
    primary_spec = compile_archetype(primary_arch, intent, catalog, target_language=target_language)

    # De-dup on the panel's *query signature* (the set of normalized query
    # expressions), not just its title — so the same panel arriving from two
    # archetypes under different titles collapses, while distinct views of the
    # same metric (e.g. p99 vs avg latency) are preserved.
    seen_signatures: set[frozenset[tuple[str, str, str, str]] | str] = {
        _panel_signature(p) for p in primary_spec.panels
    }
    blended_panels = list(primary_spec.panels)
    blended_tags = list(primary_spec.tags)

    for arch, conf in ranked_archetypes[1:]:
        if conf < secondary_min_confidence:
            continue
        if len(blended_panels) >= max_panels:
            break

        secondary_spec = compile_archetype(arch, intent, catalog, target_language=target_language)
        added = 0
        for panel in secondary_spec.panels:
            if len(blended_panels) >= max_panels:
                break
            sig = _panel_signature(panel)
            if sig not in seen_signatures:
                # Tag panel with its source archetype for traceability
                panel_with_row = panel.model_copy(update={"row": panel.row or arch.name})
                blended_panels.append(panel_with_row)
                seen_signatures.add(sig)
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
    arch_names = " + ".join(a.name for a, c in ranked_archetypes[:3] if c >= secondary_min_confidence)
    title = f"{service_name.title()} — {arch_names}"

    spec = DashboardSpec(
        title=title,
        tags=list(dict.fromkeys(blended_tags)),  # dedupe preserving order
        timerange=intent.timerange or primary_arch.default_timerange,
        panels=blended_panels[:max_panels],
    )

    logger.info(
        "archetype_blend_complete",
        primary=primary_arch.id,
        primary_confidence=primary_conf,
        total_archetypes=len(ranked_archetypes),
        total_panels=len(spec.panels),
    )

    return spec
