"""Helpers for applying request context to discovered metric catalogs."""

from __future__ import annotations

import re
from collections.abc import Iterable

from dashforge.models.schemas import MetricEntry


def _service_aliases(service: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9]+", "-", service.lower()).strip("-")
    if not normalized:
        return set()
    aliases = {normalized}
    for suffix in ("-service", "-svc"):
        if normalized.endswith(suffix) and len(normalized) > len(suffix):
            aliases.add(normalized[: -len(suffix)])
    return aliases


def metric_matches_services(entry: MetricEntry, services: Iterable[str]) -> bool:
    """Return whether metric metadata identifies one of the requested services."""
    aliases = {alias for service in services for alias in _service_aliases(service)}
    if not aliases:
        return True
    metadata = " ".join([entry.name, entry.namespace, entry.datasource_name, *entry.dimensions])
    normalized = re.sub(r"[^a-z0-9]+", "-", metadata.lower()).strip("-")
    return any(re.search(rf"(?:^|-){re.escape(alias)}(?:-|$)", normalized) for alias in aliases)


def catalog_for_services(catalog: list[MetricEntry], services: Iterable[str]) -> list[MetricEntry]:
    """Restrict a catalog to metrics carrying the requested service context."""
    requested = list(services)
    if not requested:
        return catalog
    return [entry for entry in catalog if metric_matches_services(entry, requested)]
