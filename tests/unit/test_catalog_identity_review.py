from dataclasses import replace
from itertools import permutations

import pytest

from tacit.catalog import (
    AcquisitionCaptureInvalidError,
    BaseReferenceIndex,
    MetricCatalogResult,
    admit_acquisition_capture,
    build_acquisition_capture,
    capture_extensions,
    capture_selection,
    select_query_target,
)
from tacit.models.schemas import Intent, MetricEntry
from tacit.pipeline.discovery import ColloquialConfirmationWorkLimitError, discover_catalogs
from tacit.signals.resolution import SignalResolutionWorkBudget


def metric(name="requests", **updates):
    return MetricEntry(
        name=name,
        datasource_uid="signalfx-direct",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
    ).model_copy(update=updates)


class TypedBackend:
    name = "grafana"

    def __init__(self, entries):
        self.result = MetricCatalogResult(entries)
        self.calls = 0

    async def discover_metrics_with_metadata(self, keywords, intent, work_budget):
        self.calls += 1
        return self.result


def work_budget(limit):
    return SignalResolutionWorkBudget(10_000, 8_000_000, 100_000, max_acquisition_entries=limit)


@pytest.mark.asyncio
@pytest.mark.parametrize("separate_backends", [False, True])
async def test_discovery_counts_identities_and_retains_conflicting_facts(separate_backends):
    conflicts = [metric(unit="seconds"), metric(unit="bytes")]
    valid = metric("errors")
    backends = (
        [TypedBackend(conflicts), TypedBackend([valid])] if separate_backends else [TypedBackend([*conflicts, valid])]
    )
    result = await discover_catalogs(
        backends, Intent(summary="Errors", domain="application"), work_budget=work_budget(2)
    )
    assert len(result.metric_catalog) == 3
    assert {entry.unit for entry in result.metric_catalog if entry.name == "requests"} == {"seconds", "bytes"}
    assert select_query_target(result.metric_catalog, ["requests"], "promql").reason == "metadata_conflict"
    assert select_query_target(result.metric_catalog, ["errors"], "promql").status == "selected"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed",
    [{"name": "errors"}, {"datasource_type": "graphite"}, {"query_language": "graphite"}, {"namespace": "other"}],
)
async def test_discovery_rejects_actual_identity_limit_plus_one_before_later_backend(changed):
    backends = [TypedBackend([metric(), metric("latency"), metric(**changed)]), TypedBackend([])]
    with pytest.raises(ColloquialConfirmationWorkLimitError) as exc:
        await discover_catalogs(backends, Intent(summary="Errors", domain="application"), work_budget=work_budget(2))
    assert (exc.value.dimension, exc.value.observed, exc.value.limit) == ("catalog_entries", 3, 2)
    assert backends[1].calls == 0


@pytest.mark.asyncio
async def test_discovery_physical_fact_ceiling_is_independent_of_identity_budget():
    backends = [TypedBackend([metric()] * 5000), TypedBackend([metric()]), TypedBackend([])]
    with pytest.raises(ColloquialConfirmationWorkLimitError) as exc:
        await discover_catalogs(backends, Intent(summary="Errors", domain="application"), work_budget=work_budget(1))
    assert (exc.value.dimension, exc.value.observed, exc.value.limit) == ("catalog_entries", 5001, 5000)
    assert [backend.calls for backend in backends] == [1, 1, 0]


@pytest.mark.asyncio
async def test_discovery_allows_exact_physical_fact_ceiling_with_one_identity():
    result = await discover_catalogs(
        [TypedBackend([metric()] * 5000)],
        Intent(summary="Errors", domain="application"),
        work_budget=work_budget(1),
    )
    assert len(result.metric_catalog) == 5000


def captured_selection(catalog, language):
    return capture_selection(
        select_query_target(catalog, ["requests"], language),
        consumer="archetype",
        subject_ref=f"{language}/1/1",
        required_metrics=["requests"],
        query_language=language,
    )


def admit_record(record):
    decisions, provenance = capture_extensions(record)
    index = BaseReferenceIndex(slot_subjects=frozenset(selection["subject_ref"] for selection in record["selections"]))
    return admit_acquisition_capture(record, decisions, provenance, base_index=index)


def test_capture_conflicts_share_selector_language_scope_and_canonical_order():
    entries = [
        metric(unit="seconds"),
        metric(unit="bytes"),
        metric(datasource_name="Direct", datasource_type="signalfx", query_language="signalflow", unit="bytes"),
    ]
    expected = None
    for ordering in permutations(entries):
        selections = [captured_selection(ordering, language) for language in ("signalflow", "promql")]
        assert selections[0]["status"] == "selected" and not selections[0]["conflict_ids"]
        assert selections[1]["status"] == "ambiguous" and selections[1]["conflict_ids"]
        record = build_acquisition_capture(MetricCatalogResult(ordering), selections=selections)
        assert admit_record(record).record == record
        if expected is None:
            expected = record
        assert record == expected


@pytest.mark.parametrize(
    "updates,language",
    [
        ({"unit": "bytes"}, "promql"),
        ({"datasource_type": "graphite"}, "promql"),
        ({"query_language": "opensearch", "unit": "bytes"}, "lucene"),
    ],
)
def test_capture_retains_same_language_conflicts_and_rejects_forged_selection(updates, language):
    first = metric(unit="seconds")
    if language == "lucene":
        first = first.model_copy(update={"query_language": "opensearch"})
    second = first.model_copy(update=updates)
    catalog = MetricCatalogResult([first, second])
    selection = captured_selection(catalog.entries, language)
    assert selection["reason"] == "metadata_conflict"
    record = build_acquisition_capture(catalog, selections=[selection])
    assert admit_record(record).record == record
    forged = captured_selection([first], language)
    assert forged["status"] == "selected"
    with pytest.raises(AcquisitionCaptureInvalidError, match="selection_owner_conflict"):
        build_acquisition_capture(catalog, selections=[forged])


@pytest.mark.parametrize("case", ["selected", "multiple_owners", "metadata_conflict"])
def test_capture_rejects_forged_complete_owner_candidates_in_every_selection_branch(case):
    entries = [metric(unit="seconds")]
    if case == "multiple_owners":
        entries.append(metric(datasource_uid="other-owner", unit="seconds"))
    elif case == "metadata_conflict":
        entries.append(metric(unit="bytes"))
    catalog = MetricCatalogResult(entries)
    actual = select_query_target(catalog, ["requests"], "promql")
    assert actual.reason == {"selected": "unique_owner"}.get(case, case)
    target = actual.target
    forged = replace(
        actual,
        candidate_uids=("forged-owner",),
        _target=target.model_copy(update={"datasource_uid": "forged-owner"}) if target is not None else None,
    )
    selection = capture_selection(
        forged,
        consumer="archetype",
        subject_ref="promql/1/1",
        required_metrics=["requests"],
        query_language="promql",
    )

    with pytest.raises(AcquisitionCaptureInvalidError, match="selection_owner"):
        build_acquisition_capture(catalog, selections=[selection])
