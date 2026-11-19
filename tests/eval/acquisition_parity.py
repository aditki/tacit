"""Frozen acquisition availability and validated-observation accounting.

This diagnostic consumes recorded typed outcomes. It does not execute queries or
claim that metric availability proves a numeric condition or an incident cause.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tacit.models.schemas import MetricEntry

FIXTURE = Path(__file__).parent / "fixtures" / "acquisition_parity_v1.json"


def load_fixture() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def catalog(fixture: dict[str, Any]) -> list[MetricEntry]:
    """Put all requested signals beyond the premature 300-entry projection."""
    names = [f"irrelevant_metric_{index:04d}" for index in range(fixture["original_count"])]
    requested = sorted(
        {metric for group in fixture["categories"].values() for metric in group} | set(fixture["fixture_critical"])
    )
    for offset, name in enumerate(requested, fixture["critical_offset"]):
        names[offset] = name
    return [
        MetricEntry(
            name=name,
            datasource_uid=fixture["datasource_uid"],
            datasource_name="Frozen telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
        for name in names
    ]


def _mapping(value: Any) -> dict[str, Any]:
    return value.model_dump(mode="json") if hasattr(value, "model_dump") else value


def score(
    fixture: dict[str, Any], retained: list[MetricEntry], observations: list[Any], unsafe: list[Any]
) -> dict[str, Any]:
    """Keep request obligations distinct from the historical fixture metrics."""
    uid = fixture["datasource_uid"]
    available = {entry.name for entry in retained if entry.datasource_uid == uid}
    supported: set[str] = set()
    routing_errors = 0
    valid_observations = 0
    for value in observations:
        item = _mapping(value)
        if item.get("datasource_uid") != uid:
            routing_errors += 1
            continue
        if (
            item.get("valid_query")
            and item.get("non_empty")
            and item.get("survived")
            and item.get("outcome") == "SUPPORTED_OBSERVATION"
        ):
            name = item.get("resolution_metric", "")
            if name in available:
                supported.add(name)
                valid_observations += 1
    categories = fixture["categories"]
    available_count = sum(set(names) <= available for names in categories.values())
    observed_count = sum(set(names) <= supported for names in categories.values())
    critical = set(fixture["fixture_critical"])
    return {
        "retained_entries": len(retained),
        "available_categories": available_count,
        "observed_categories": observed_count,
        "category_denominator": len(categories),
        "fixture_critical_available": len(critical & available),
        "fixture_critical_observed": len(critical & supported),
        "fixture_critical_denominator": len(critical),
        "validated_observations": valid_observations,
        "routing_errors": routing_errors,
        "unsafe_assertions": len(unsafe),
        "passed": observed_count == len(categories) and not routing_errors and not unsafe,
    }
