"""Helpers for applying request context to discovered metric catalogs."""

from __future__ import annotations

import re
from collections.abc import Iterable

from tacit.models.schemas import MetricEntry

_SERVICE_DIMENSION_KEYS = {
    "service",
    "service_name",
    "service.name",
    "app",
    "application",
    "container",
    "pod",
}


def _service_aliases(service: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", "-", service.lower()).strip("-")
    if not normalized:
        return set()
    aliases = {normalized}
    for suffix in ("-service", "-svc"):
        if normalized.endswith(suffix) and len(normalized) > len(suffix):
            aliases.add(normalized[: -len(suffix)])
    return aliases


def _explicit_service_metadata(entry: MetricEntry) -> str:
    values: list[str] = []
    for dimension in entry.dimensions:
        key, separator, value = dimension.partition("=")
        if separator and key.strip().lower() in _SERVICE_DIMENSION_KEYS:
            cleaned = value.strip().strip("{}").strip()
            if cleaned:
                values.append(cleaned)
    return " ".join(values)


def metric_has_service_metadata(entry: MetricEntry) -> bool:
    """Return whether discovery supplied concrete service label values."""
    return bool(_explicit_service_metadata(entry))


def metric_matches_services(entry: MetricEntry, services: Iterable[str]) -> bool:
    """Return whether metric metadata identifies one of the requested services."""
    aliases = {alias for service in services for alias in _service_aliases(service)}
    if not aliases:
        return True
    explicit = _explicit_service_metadata(entry)
    metadata = explicit or " ".join([entry.name, entry.namespace, entry.datasource_name])
    normalized = re.sub(r"[^a-z0-9]+", "-", metadata.lower()).strip("-")
    return any(re.search(rf"(?:^|-){re.escape(alias)}(?:-|$)", normalized) for alias in aliases)


def catalog_for_services(
    catalog: list[MetricEntry],
    services: Iterable[str],
    *,
    include_unscoped: bool = False,
) -> list[MetricEntry]:
    """Restrict a catalog to matching service context and optional unknowns."""
    requested = list(services)
    if not requested:
        return catalog
    return [
        entry
        for entry in catalog
        if metric_matches_services(entry, requested) or (include_unscoped and not metric_has_service_metadata(entry))
    ]
