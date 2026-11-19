from dataclasses import FrozenInstanceError

import pytest

from tacit.catalog import CatalogSourceStatus, MetricCatalogResult, acquisition_entry_id, build_acquisition_capture
from tacit.models.schemas import MetricEntry


def metric(name="requests", **kwargs):
    return MetricEntry(
        name=name,
        datasource_uid="p",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
        **kwargs,
    )


def test_result_owns_defensive_copies_and_frozen_sources():
    entry = metric(dimensions=["job=a"])
    source = CatalogSourceStatus(
        backend="grafana",
        datasource_uid="p",
        datasource_type="prometheus",
        query_language="promql",
        terms=("z", "a", "a"),
        returned_count=1,
        retained_count=1,
        total_before_cap=1,
        disposition="complete",
        reasons=("authoritative_terminal",),
        retained_entry_ids=(acquisition_entry_id(entry),),
    )
    result = MetricCatalogResult(entries=(entry,), sources=(source,))
    entry.dimensions.append("wrong")
    result.entries[0].dimensions.append("also wrong")
    result.as_list()[0].name = "changed"
    assert result.entries[0].dimensions == ["job=a"]
    assert result.entries[0].name == "requests"
    assert result.sources[0].terms == ("a", "z")
    with pytest.raises(FrozenInstanceError):
        source.backend = "changed"


@pytest.mark.parametrize("overlap", [40, 60])
def test_repeated_search_uses_original_call_facts_and_set_union(overlap):
    entries = [metric(f"m{i}") for i in range(120 - overlap)]
    calls = []
    for selected in (entries[:60], entries[60 - overlap : 120 - overlap]):
        calls.append(
            CatalogSourceStatus(
                backend="grafana",
                datasource_uid="p",
                datasource_type="prometheus",
                query_language="promql",
                terms=("requests",),
                returned_count=60,
                retained_count=60,
                total_before_cap=100,
                disposition="complete",
                reasons=("authoritative_terminal",),
                retained_entry_ids=tuple(acquisition_entry_id(e) for e in selected),
            )
        )
    a = build_acquisition_capture(MetricCatalogResult(entries=entries, sources=calls))
    b = build_acquisition_capture(MetricCatalogResult(entries=reversed(entries), sources=reversed(calls)))
    assert a == b
    source = a["sources"][0]
    assert source["returned_count"] == 120
    assert source["retained_count"] == 120 - overlap
    assert source["total_before_cap"] is None
    assert source["disposition"] == "incomplete"
    assert "provider_cap_unknown" in source["reasons"]


@pytest.mark.parametrize("totals", [(100, None, 100), (100, 101, 100)])
def test_discarded_total_cannot_be_restored(totals):
    calls = [
        CatalogSourceStatus(
            backend="grafana",
            returned_count=0,
            datasource_uid="p",
            datasource_type="prometheus",
            query_language="promql",
            retained_count=0,
            total_before_cap=total,
            disposition="complete" if total is not None else "incomplete",
            reasons=("authoritative_terminal",) if total is not None else ("provider_cap_unknown",),
        )
        for total in totals
    ]
    a = build_acquisition_capture(MetricCatalogResult(sources=calls))
    b = build_acquisition_capture(MetricCatalogResult(sources=reversed(calls)))
    assert a == b
    assert a["sources"][0]["total_before_cap"] is None


def test_empty_success_prevents_unavailable_and_failure_prevents_complete():
    success = CatalogSourceStatus(
        backend="grafana",
        datasource_uid="p",
        datasource_type="prometheus",
        query_language="promql",
        disposition="complete",
        total_before_cap=0,
        reasons=("authoritative_terminal",),
    )
    failure = CatalogSourceStatus(
        backend="grafana",
        datasource_uid="p",
        datasource_type="prometheus",
        query_language="promql",
        disposition="unavailable",
        reasons=("provider_failed",),
    )
    for sources in ((success, failure), (failure, success)):
        source = build_acquisition_capture(MetricCatalogResult(sources=sources))["sources"][0]
        assert source["disposition"] == "incomplete"
        assert source["total_before_cap"] is None
    assert (
        build_acquisition_capture(MetricCatalogResult(sources=(failure,)))["sources"][0]["disposition"] == "unavailable"
    )


def test_optional_protocol_does_not_extend_required_dashboard_protocol():
    from tacit.backends.base import MetricDiscoveryWithMetadata

    class ListOnly:
        async def discover_metrics(self, keywords, intent):
            return []

    assert not isinstance(ListOnly(), MetricDiscoveryWithMetadata)


def test_entry_dimensions_exact_cap_and_plus_one():
    from tacit.catalog import AcquisitionCaptureLimitError

    MetricCatalogResult((metric(dimensions=[str(i) for i in range(128)]),))
    with pytest.raises(AcquisitionCaptureLimitError, match="entry_dimensions"):
        MetricCatalogResult((metric(dimensions=[str(i) for i in range(129)]),))


def test_terms_exact_cap_and_plus_one():
    from tacit.catalog import AcquisitionCaptureLimitError

    CatalogSourceStatus(
        backend="grafana",
        datasource_uid="p",
        datasource_type="prometheus",
        query_language="promql",
        terms=tuple(str(i) for i in range(256)),
    )
    with pytest.raises(AcquisitionCaptureLimitError, match="source_terms"):
        CatalogSourceStatus(
            backend="grafana",
            datasource_uid="p",
            datasource_type="prometheus",
            query_language="promql",
            terms=tuple(str(i) for i in range(257)),
        )


def test_result_rejects_aggregate_text_before_copying_member(monkeypatch):
    from tacit.catalog import AcquisitionCaptureLimitError

    calls = []
    original = MetricEntry.model_copy

    def counted(self, **kwargs):
        calls.append(self.name)
        return original(self, **kwargs)

    monkeypatch.setattr(MetricEntry, "model_copy", counted)
    entries = [metric("x" * 65000) for _ in range(31)]
    with pytest.raises(AcquisitionCaptureLimitError, match="total_input_characters"):
        MetricCatalogResult(entries)
    assert len(calls) == 30


def test_legacy_adaptation_preserves_conflicting_metadata_for_selector_and_capture():
    from tacit.catalog import legacy_metric_catalog, select_query_target

    result = legacy_metric_catalog([metric(unit="seconds"), metric(unit="bytes")], backend="third_party")
    assert select_query_target(result, ["requests"], "promql").reason == "metadata_conflict"
    record = build_acquisition_capture(result)
    assert len(record["entries"]) == 1 and len(record["conflicts"]) == 1
    assert record["sources"][0]["returned_count"] == 2
    assert record["sources"][0]["retained_count"] == 1


def test_non_summary_search_requires_real_complete_source_identity():
    from tacit.catalog import AcquisitionCaptureInvalidError

    with pytest.raises(AcquisitionCaptureInvalidError, match="source_identity"):
        CatalogSourceStatus(
            backend="grafana",
            terms=("datasources",),
            returned_count=2,
            total_before_cap=2,
            disposition="complete",
            reasons=("authoritative_terminal",),
        )
    CatalogSourceStatus(backend="grafana", summary=True, disposition="unavailable", reasons=("provider_failed",))


def test_legacy_sources_group_real_identities_and_unknown_identity_is_only_summary():
    from tacit.catalog import legacy_metric_catalog, select_query_target

    result = legacy_metric_catalog(
        [metric(), metric("unknown").model_copy(update={"datasource_uid": ""})], backend="legacy", terms=("requests",)
    )
    assert len(result.entries) == 2
    assert select_query_target(result, ["unknown"], "promql").status == "missing"
    assert len(result.sources) == 2
    summary = next(source for source in result.sources if source.summary)
    assert summary.returned_count == summary.retained_count == 0 and summary.terms == ()
    source = next(source for source in result.sources if not source.summary)
    assert source.datasource_uid == "p" and source.returned_count == source.retained_count == 1


def test_shared_catalog_fact_admission_preserves_conflicts_and_deduplicates_only_equivalents():
    from tacit.catalog import admit_catalog_facts, select_query_target

    a = metric(unit="seconds", dimensions=["z", "a"])
    same = metric(unit="seconds", dimensions=["a", "z", "a"])
    b = metric(unit="bytes", dimensions=["a"])
    one = admit_catalog_facts([a, same, b])
    two = admit_catalog_facts([b, same, a])
    assert one == two and len(one) == 2
    assert select_query_target(one, ["requests"], "promql").reason == "metadata_conflict"
    one[0].name = "changed"
    assert a.name == "requests" and a.dimensions == ["z", "a"]


def test_metadata_expansion_preserves_every_distinct_definition_in_any_order():
    from tacit.catalog import expand_metric_metadata_facts, select_query_target

    base = metric(dimensions=["service=api"])
    definitions = [{"unit": "seconds", "type": "counter"}, {"unit": "bytes", "type": "gauge"}]
    one = expand_metric_metadata_facts(base, definitions)
    two = expand_metric_metadata_facts(base, reversed(definitions))
    assert one == two and len(one) == 2
    selected = select_query_target(one, ["requests"], "promql")
    assert selected.reason == "metadata_conflict" and {conflict.field for conflict in selected.conflicts} == {
        "unit",
        "metric_type",
    }
    assert base.unit == "" and base.metric_type == ""


def test_metadata_unique_unknown_gaps_merge_without_doubling_retention():
    from tacit.catalog import expand_metric_metadata_facts

    result = expand_metric_metadata_facts(metric(), [{"unit": "seconds"}, {"type": "counter"}, {}])
    assert len(result) == 1 and result[0].unit == "seconds" and result[0].metric_type == "counter"


def test_raw_fact_cardinality_guard_precedes_equivalent_deduplication():
    from tacit.catalog import AcquisitionCaptureLimitError, admit_catalog_facts

    entry = metric()
    with pytest.raises(AcquisitionCaptureLimitError, match="entries"):
        admit_catalog_facts([entry] * 5001)
