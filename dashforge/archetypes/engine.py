"""Archetype engine — resolves templates into concrete DashboardSpec.

Given an archetype + intent + discovered label values, deterministically
compiles query templates into real PromQL. No LLM needed for query generation.
"""
from __future__ import annotations

import re

import structlog

from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate
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


def _resolve_service_filter(
    intent: Intent,
    catalog: list[MetricEntry],
) -> str:
    """Build the PromQL label selector for the target service.

    Looks at the catalog's dimensions to find the correct label name
    and value for the service the user is asking about.
    Prefers the 'service' label over others like 'container' or 'pod'.
    """
    if not intent.services:
        return ""

    target = intent.services[0].lower().replace(" ", "-")

    # Collect all matching (label_name, value) pairs
    # Prefer: service > app > container > anything else
    _LABEL_PRIORITY = {"service": 0, "app": 1, "application": 1, "container": 2, "pod": 3}
    candidates: list[tuple[int, str, str]] = []

    for entry in catalog:
        for dim in entry.dimensions:
            match = re.match(r"(\w+)=\{(.+)\}", dim)
            if not match:
                continue
            label_name, values_str = match.group(1), match.group(2)
            values = [v.strip() for v in values_str.split(",")]
            for val in values:
                val_normalized = val.lower().replace("_", "-")
                if target in val_normalized or val_normalized in target:
                    priority = _LABEL_PRIORITY.get(label_name, 10)
                    candidates.append((priority, label_name, val))

    if candidates:
        candidates.sort(key=lambda x: x[0])
        _, label_name, val = candidates[0]
        return f'{label_name}="{val}"'

    # Fallback: use service label with best-guess value
    return f'service=~".*{_re2_escape(target)}.*"'


def _resolve_container_filter(
    intent: Intent,
    catalog: list[MetricEntry],
) -> str:
    """Build PromQL label selector for container-level metrics."""
    if not intent.services:
        return ""

    target = intent.services[0].lower().replace(" ", "-")

    for entry in catalog:
        for dim in entry.dimensions:
            match = re.match(r"(\w+)=\{(.+)\}", dim)
            if not match:
                continue
            label_name, values_str = match.group(1), match.group(2)
            if label_name not in ("container", "pod"):
                continue
            values = [v.strip() for v in values_str.split(",")]
            for val in values:
                val_normalized = val.lower().replace("_", "-")
                if target in val_normalized or val_normalized in target:
                    return f'{label_name}="{val}"'

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


def compile_archetype(
    archetype: InvestigationArchetype,
    intent: Intent,
    catalog: list[MetricEntry],
) -> DashboardSpec:
    """Compile an archetype template into a concrete DashboardSpec.

    This is fully deterministic — no LLM call needed.
    Resolves {service_filter}, {container_filter}, {rate_interval}
    from the intent and catalog.
    """
    service_filter = _resolve_service_filter(intent, catalog)
    container_filter = _resolve_container_filter(intent, catalog)
    rate_interval = _resolve_rate_interval(intent)
    datasource_uid = _get_datasource_uid(catalog)

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
        # Check if required metrics exist in the catalog
        panel_queries: list[PanelQuery] = []
        for qt in pt.queries:
            try:
                expr = qt.expr.format(**params)
            except KeyError as e:
                logger.warning("archetype_placeholder_missing", panel=pt.title, key=str(e))
                continue

            panel_queries.append(PanelQuery(
                expr=expr,
                legend_format=qt.legend_format,
                datasource_uid=datasource_uid,
                datasource_type=qt.datasource_type,
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
    )

    return spec


def blend_archetypes(
    ranked_archetypes: list[tuple["InvestigationArchetype", float]],
    intent: Intent,
    catalog: list[MetricEntry],
    secondary_min_confidence: float = 0.4,
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
        Discovered metrics from Grafana datasources.
    secondary_min_confidence : float
        Minimum confidence for secondary archetypes to contribute panels.
    """
    if not ranked_archetypes:
        raise ValueError("blend_archetypes called with empty archetype list")

    primary_arch, primary_conf = ranked_archetypes[0]
    primary_spec = compile_archetype(primary_arch, intent, catalog)

    # Track existing panel titles to avoid duplicates
    existing_titles: set[str] = {p.title.lower() for p in primary_spec.panels}
    blended_panels = list(primary_spec.panels)
    blended_tags = list(primary_spec.tags)

    for arch, conf in ranked_archetypes[1:]:
        if conf < secondary_min_confidence:
            continue

        secondary_spec = compile_archetype(arch, intent, catalog)
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
