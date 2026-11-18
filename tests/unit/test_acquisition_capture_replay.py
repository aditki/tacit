import hashlib
from copy import deepcopy

import pytest

from tacit.catalog import (
    AcquisitionCaptureInvalidError,
    AcquisitionCaptureLimitError,
    BaseReferenceIndex,
    CatalogSourceStatus,
    MetricCatalogResult,
    admit_acquisition_capture,
    build_acquisition_capture,
    canonical_acquisition_bytes,
    capture_extensions,
    capture_selection,
    select_query_target,
)
from tacit.models.schemas import MetricEntry


def metric():
    return MetricEntry(
        name="http_requests_total",
        datasource_uid="p",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
    )


def fixture():
    entry = metric()
    result = MetricCatalogResult(
        entries=(entry,),
        sources=(
            CatalogSourceStatus(
                backend="grafana",
                datasource_uid="p",
                datasource_type="prometheus",
                query_language="promql",
                terms=("http",),
                returned_count=1,
                retained_count=1,
                total_before_cap=1,
                disposition="complete",
                reasons=("authoritative_terminal",),
            ),
        ),
    )
    selection = capture_selection(
        select_query_target(result.entries, [entry.name, "missing_requests_total"], "promql"),
        consumer="archetype",
        subject_ref="latency_investigation/1/1",
        required_metrics=[entry.name, "missing_requests_total"],
        query_language="promql",
    )
    record = build_acquisition_capture(result, selections=(selection,))
    decisions, provenance = capture_extensions(record)
    index = BaseReferenceIndex(
        frozenset({"investigation_one"}), frozenset(), frozenset({"latency_investigation/1/1"}), frozenset(), 10
    )
    return record, decisions, provenance, index


def test_canonical_hash_mapping_missing_literal_and_no_mutation():
    record, decisions, provenance, index = fixture()
    before = deepcopy(record)
    admitted = admit_acquisition_capture(record, decisions, provenance, base_index=index)
    assert record == before
    assert admitted.record == before
    assert admitted.record["selections"][0]["required_metrics"] == ["http_requests_total", "missing_requests_total"]
    source = record["sources"][0]
    identity = {
        key: source[key]
        for key in ("backend", "datasource_uid", "datasource_type", "query_language", "terms", "summary")
    }
    expected = hashlib.sha256(b"acq-source-v1\0" + canonical_acquisition_bytes(identity)).hexdigest()
    assert source["id"] == "acq_source_" + expected
    assert provenance[0].id == "prov_" + source["id"]
    assert all(d.reason == "" and d.stage == "discovery" for d in decisions)
    admitted.record["entries"].clear()
    assert len(admitted.record["entries"]) == 1


def test_namespace_collision_and_unknown_slot_reject():
    record, decisions, provenance, index = fixture()
    with pytest.raises(AcquisitionCaptureInvalidError):
        admit_acquisition_capture(
            record,
            decisions,
            provenance,
            base_index=BaseReferenceIndex(
                frozenset({provenance[0].id}), index.requirement_ids, index.slot_subjects, index.freeform_subjects, 0
            ),
        )
    with pytest.raises(AcquisitionCaptureInvalidError):
        admit_acquisition_capture(
            record,
            decisions,
            provenance,
            base_index=BaseReferenceIndex(frozenset(), frozenset(), frozenset(), frozenset(), 0),
        )


@pytest.mark.parametrize("field,value", [("version", "catalog-acquisition/2"), ("unknown", 1)])
def test_closed_schema_rejects_unknown_version_and_fields(field, value):
    record, decisions, provenance, index = fixture()
    record[field] = value
    with pytest.raises(AcquisitionCaptureInvalidError):
        admit_acquisition_capture(record, decisions, provenance, base_index=index)


def test_declared_record_byte_cap_and_plus_one():
    record, decisions, provenance, index = fixture()
    # The limit field participates in its own encoding; fix its decimal width.
    record["limits"]["record_bytes"] = 9999
    size = len(canonical_acquisition_bytes(record))
    for _ in range(3):
        record["limits"]["record_bytes"] = size
        size = len(canonical_acquisition_bytes(record))
    decisions, provenance = capture_extensions(record)
    admit_acquisition_capture(record, decisions, provenance, base_index=index)
    record["limits"]["record_bytes"] = size - 1
    decisions, provenance = capture_extensions(record)
    with pytest.raises(AcquisitionCaptureLimitError) as exc:
        admit_acquisition_capture(record, decisions, provenance, base_index=index)
    assert exc.value.dimension == "record_bytes"


def test_no_root_floats_and_bool_counts():
    record, decisions, provenance, index = fixture()
    for value in (True, 1.0):
        modified = deepcopy(record)
        modified["counters"]["calls_reserved"] = value
        with pytest.raises(AcquisitionCaptureInvalidError):
            admit_acquisition_capture(modified, decisions, provenance, base_index=index)


def test_empty_example_has_exact_normative_record_and_only_stop_extension():
    record = build_acquisition_capture(MetricCatalogResult())
    assert record["version"] == "catalog-acquisition/1" and record["stop_reason"] == "completed"
    assert record["counters"] == dict(
        calls_reserved=0,
        response_bytes_reserved=0,
        entries_retained=0,
        sources_recorded=0,
        selections_recorded=0,
        conflicts_recorded=0,
    )
    decisions, provenance = capture_extensions(record)
    assert len(decisions) == 1 and decisions[0].action == "catalog_stop" and not provenance
    admit_acquisition_capture(record, decisions, provenance, base_index=BaseReferenceIndex())


@pytest.mark.parametrize(
    "array,ceiling", [("entries", 5000), ("sources", 64), ("selections", 1024), ("conflicts", 1024)]
)
def test_root_cardinality_plus_one_rejects_before_traversal(array, ceiling):
    record, decisions, provenance, index = fixture()
    record[array] = [None] * (ceiling + 1)
    with pytest.raises(AcquisitionCaptureLimitError) as exc:
        admit_acquisition_capture(record, decisions, provenance, base_index=index)
    assert exc.value.dimension == array


@pytest.mark.parametrize("kind,count", [("decisions", 1090), ("provenance", 65)])
def test_extension_cardinality_plus_one(kind, count):
    record, decisions, provenance, index = fixture()
    if kind == "decisions":
        decisions = tuple(decisions[0] for _ in range(count))
    else:
        provenance = tuple(provenance[0] for _ in range(count))
    with pytest.raises(AcquisitionCaptureLimitError) as exc:
        admit_acquisition_capture(record, decisions, provenance, base_index=index)
    assert exc.value.dimension == kind


@pytest.mark.parametrize(
    "dimension,at_limit,over_limit",
    [
        ("scalar_characters", "a" * 65536, "a" * 65537),
        ("scalar_utf8_bytes", "\U0001f600" * 65536, "\U0001f600" * 65537),
    ],
)
def test_scalar_admission_caps(dimension, at_limit, over_limit):
    from tacit.catalog import _text

    _text((at_limit,))
    with pytest.raises(AcquisitionCaptureLimitError) as exc:
        _text((over_limit,))
    # Character-first is deliberate when both limits are crossed.
    assert exc.value.dimension in {dimension, "scalar_characters"}


def test_aggregate_text_exact_cap_and_plus_one():
    from tacit.catalog import _text

    texts = ["a" * 62500] * 32
    _text(texts)
    with pytest.raises(AcquisitionCaptureLimitError, match="total_input_characters"):
        _text([*texts, "x"])


def test_bounded_tree_depth_and_nodes_exact_and_plus_one():
    from tacit.catalog import _walk_capture

    tree = None
    for _ in range(12):
        tree = [tree]
    _walk_capture(tree)
    with pytest.raises(AcquisitionCaptureLimitError, match="tree_depth"):
        _walk_capture([tree])
    _walk_capture([None] * 199999)
    with pytest.raises(AcquisitionCaptureLimitError, match="tree_nodes"):
        _walk_capture([None] * 200000)


@pytest.mark.parametrize("dimension", ["record_bytes", "extensions_bytes", "capture_bytes"])
def test_incremental_bytes_exact_and_plus_one_escaping(dimension):
    from tacit.catalog import _canonical_count

    value = ['\0\n"\\' * 10000]
    size = len(canonical_acquisition_bytes(value))
    _canonical_count(value, size, dimension)
    with pytest.raises(AcquisitionCaptureLimitError) as exc:
        _canonical_count(value, size - 1, dimension)
    assert exc.value.dimension == dimension


def test_extension_and_combined_public_caps_are_independent_and_reserve_sequence_width():
    record, decisions, provenance, index = fixture()
    reserved = [decision.model_copy(update={"sequence": 9999999999}).model_dump(mode="json") for decision in decisions]
    prov = [p.model_dump(mode="json") for p in provenance]
    for field, frame in (
        ("extensions_bytes", {"additional_decisions": reserved, "additional_provenance": prov}),
        ("capture_bytes", {"record": record, "additional_decisions": reserved, "additional_provenance": prov}),
    ):
        modified = deepcopy(record)
        # Deliberately tighter than even the 1-digit representation: independent public check must reject.
        modified["limits"][field] = len(canonical_acquisition_bytes(frame)) - 100
        changed, changed_prov = capture_extensions(modified)
        with pytest.raises(AcquisitionCaptureLimitError) as exc:
            admit_acquisition_capture(modified, changed, changed_prov, base_index=index)
        assert exc.value.dimension == field


def test_decision_sequence_width_reserved_before_final_rendering():
    record, decisions, provenance, index = fixture()
    admissible = 9999999999 - 1 - len(decisions)
    admit_acquisition_capture(
        record,
        decisions,
        provenance,
        base_index=BaseReferenceIndex(
            index.contract_ids, index.requirement_ids, index.slot_subjects, index.freeform_subjects, admissible
        ),
    )
    with pytest.raises(AcquisitionCaptureLimitError, match="sequence"):
        admit_acquisition_capture(
            record,
            decisions,
            provenance,
            base_index=BaseReferenceIndex(
                index.contract_ids, index.requirement_ids, index.slot_subjects, index.freeform_subjects, admissible + 1
            ),
        )


def test_consumer_reference_namespaces_are_not_interchangeable():
    from tacit.catalog import freeform_selection_subject

    chosen = select_query_target([metric()], [metric().name], "promql")
    subject = freeform_selection_subject([metric().name], "promql")
    selection = capture_selection(
        chosen,
        consumer="freeform_selected",
        subject_ref=subject,
        required_metrics=[metric().name],
        query_language="promql",
    )
    record = build_acquisition_capture(MetricCatalogResult([metric()]), selections=[selection])
    decisions, provenance = capture_extensions(record)
    admit_acquisition_capture(
        record, decisions, provenance, base_index=BaseReferenceIndex(freeform_subjects=frozenset({subject}))
    )
    with pytest.raises(AcquisitionCaptureInvalidError, match="selection_subject_reference"):
        admit_acquisition_capture(
            record,
            decisions,
            provenance,
            base_index=BaseReferenceIndex(contract_ids=frozenset({subject}), requirement_ids=frozenset({subject})),
        )


def test_mutated_extension_mapping_rejects_instead_of_trusting_generic_model():
    record, decisions, provenance, index = fixture()
    changed = list(decisions)
    changed[0] = changed[0].model_copy(update={"mechanism": {"raw_response": "must not persist"}})
    with pytest.raises(AcquisitionCaptureInvalidError, match="decision_mapping"):
        admit_acquisition_capture(record, changed, provenance, base_index=index)


@pytest.mark.parametrize(
    "array,field",
    [("sources", "disposition"), ("selections", "consumer"), ("selections", "status"), ("selections", "reason")],
)
def test_malformed_enum_has_typed_capture_failure(array, field):
    record, decisions, provenance, index = fixture()
    record[array][0][field] = []
    with pytest.raises(AcquisitionCaptureInvalidError):
        admit_acquisition_capture(record, decisions, provenance, base_index=index)


def test_exact_entry_cardinality_is_admitted_and_plus_one_is_rejected():
    entries = [metric().model_copy(update={"name": f"metric_{i}"}) for i in range(5000)]
    record = build_acquisition_capture(MetricCatalogResult(entries))
    decisions, provenance = capture_extensions(record)
    admit_acquisition_capture(record, decisions, provenance, base_index=BaseReferenceIndex())
    with pytest.raises(AcquisitionCaptureLimitError, match="entries"):
        MetricCatalogResult((*entries, metric()))


def test_exact_source_and_selection_cardinality_is_admitted():
    from tacit.catalog import capture_selection

    sources = [
        CatalogSourceStatus(
            backend="grafana",
            terms=(f"search{i}",),
            datasource_uid="p",
            datasource_type="prometheus",
            query_language="promql",
            total_before_cap=0,
            disposition="complete",
            reasons=("authoritative_terminal",),
        )
        for i in range(64)
    ]
    chosen = select_query_target([], ["missing"], "promql")
    subjects = [f"a/{i}/1" for i in range(1, 1025)]
    selections = [
        capture_selection(
            chosen, consumer="archetype", subject_ref=subject, required_metrics=["missing"], query_language="promql"
        )
        for subject in subjects
    ]
    record = build_acquisition_capture(MetricCatalogResult(sources=sources), selections=selections)
    decisions, provenance = capture_extensions(record)
    assert len(decisions) == 1089 and len(provenance) == 64
    admit_acquisition_capture(
        record, decisions, provenance, base_index=BaseReferenceIndex(slot_subjects=frozenset(subjects))
    )


def test_exact_conflict_cardinality_is_admitted():
    entries = []
    for i in range(512):
        for unit, metric_type in (("seconds", "gauge"), ("bytes", "counter")):
            entries.append(metric().model_copy(update={"name": f"m{i}", "unit": unit, "metric_type": metric_type}))
    record = build_acquisition_capture(MetricCatalogResult(entries))
    assert len(record["conflicts"]) == 1024
    decisions, provenance = capture_extensions(record)
    admit_acquisition_capture(record, decisions, provenance, base_index=BaseReferenceIndex())


def test_conflicts_are_preserved_across_canonical_capture_and_reversal():
    entries = [metric().model_copy(update={"unit": value}) for value in ("seconds", "bytes")]
    selections = [
        capture_selection(
            select_query_target(entries, [metric().name], "promql"),
            consumer="archetype",
            subject_ref="a/1/1",
            required_metrics=[metric().name],
            query_language="promql",
        )
    ]
    a = build_acquisition_capture(MetricCatalogResult(entries), selections=selections)
    b = build_acquisition_capture(MetricCatalogResult(reversed(entries)), selections=reversed(selections))
    assert a == b
    decisions, provenance = capture_extensions(a)
    admit_acquisition_capture(
        a, decisions, provenance, base_index=BaseReferenceIndex(slot_subjects=frozenset({"a/1/1"}))
    )


def test_invalid_unicode_and_boolean_extension_sequence_have_typed_failures():
    record, decisions, provenance, index = fixture()
    modified = deepcopy(record)
    modified["entries"][0]["metric"]["name"] = "\ud800"
    with pytest.raises(AcquisitionCaptureInvalidError):
        admit_acquisition_capture(modified, decisions, provenance, base_index=index)
    raw = [decision.model_dump(mode="json") for decision in decisions]
    raw[0]["sequence"] = True
    with pytest.raises(AcquisitionCaptureInvalidError):
        admit_acquisition_capture(record, raw, provenance, base_index=index)
