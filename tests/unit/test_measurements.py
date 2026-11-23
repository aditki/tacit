"""Test-first gates for the single bounded numeric capture boundary."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest
from pydantic import ValidationError

from tacit.measurements import (
    MeasurementSample,
    QueryMeasurement,
    admit_measurements,
    normalize_prometheus_measurement,
    query_identity,
    unavailable_measurement,
)
from tacit.models.schemas import DashboardSpec, PanelQuery, PanelSpec
from tacit.signals.resolution import AcquisitionWorkLimitError, SignalResolutionWorkBudget


def budget(**changes):
    return SignalResolutionWorkBudget(100, 100_000, 1000, **changes)


def query(**changes):
    return PanelQuery(expr="rate(cpu_seconds_total[5m])", datasource_uid="metrics-a", **changes)


def vector(*values, labels=None):
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": labels or {"service": "checkout", "instance": f"instance-{index}"},
                    "value": [1234, str(value)],
                }
                for index, value in enumerate(values)
            ],
        },
    }


def dashboard(*queries):
    return DashboardSpec(title="Recorded metrics", panels=[PanelSpec(title="Resources", queries=list(queries))])


def test_real_values_survive_without_units_or_abnormality_inference():
    result = normalize_prometheus_measurement(query(), vector(0, -3.5), budget())
    assert result.disposition == "captured"
    assert {sample.value for sample in result.samples} == {0.0, -3.5}
    assert {sample.timestamp for sample in result.samples} == {1234.0}
    assert result.unit is None and result.evaluation_time is None
    assert not {"abnormal", "comparison", "confidence", "rank", "score"}.intersection(QueryMeasurement.model_fields)
    assert result.samples[0].scope[0].name == "service"
    assert result.samples[0].scope[0].value == "checkout"
    assert "instance-" not in result.model_dump_json()
    with pytest.raises(ValidationError):
        result.samples[0].value = 100


def test_sample_and_provider_order_do_not_change_fact_identity():
    payload = vector(3, 4)
    reverse = deepcopy(payload)
    reverse["data"]["result"].reverse()
    first = normalize_prometheus_measurement(query(), payload, budget())
    second = normalize_prometheus_measurement(query(), reverse, budget())
    changed = normalize_prometheus_measurement(query(), vector(3, 5), budget())
    assert first == second and first.fact_fingerprint != changed.fact_fingerprint
    assert len(first.fact_fingerprint) == len(first.query_fingerprint) == 64


def test_query_identity_binds_every_exact_source_component():
    base = query_identity(query())
    for field, value in (
        ("expr", "other"),
        ("datasource_uid", "metrics-b"),
        ("datasource_type", "mimir"),
        ("query_language", "promql"),
    ):
        assert query_identity(query().model_copy(update={field: value})) != base


@pytest.mark.parametrize("value", [True, False, "NaN", "+Inf", "-Inf", "1e9999", "", " 1 ", None, {}, []])
def test_invalid_values_never_escape_as_a_complete_numeric_prefix(value):
    payload = vector(1, 2)
    payload["data"]["result"][1]["value"][1] = value
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.disposition == "invalid" and record.samples == ()


@pytest.mark.parametrize("timestamp", [True, "1234", None, float("nan"), float("inf")])
def test_malformed_timestamps_are_not_numeric_evidence(timestamp):
    payload = vector(1)
    payload["data"]["result"][0]["value"][0] = timestamp
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.disposition == "invalid" and record.samples == ()


def test_partial_and_unsupported_results_remain_explicit():
    payload = vector(5)
    payload["warnings"] = ["partial results"]
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.reason_code == "partial_response" and not record.samples
    payload = vector(5)
    payload["data"]["resultType"] = "matrix"
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.disposition == "unsupported" and not record.samples
    assert normalize_prometheus_measurement(query(), {"status": "error"}, budget()).disposition == "invalid"


def test_scalar_and_empty_results_are_distinct():
    scalar = {"status": "success", "data": {"resultType": "scalar", "result": [0, "-1.5"]}}
    record = normalize_prometheus_measurement(query(), scalar, budget(), evaluation_time=0)
    assert record.disposition == "captured" and record.samples[0].value == -1.5
    assert record.samples[0].timestamp == 0 and record.evaluation_time == 0
    empty = normalize_prometheus_measurement(query(), vector(), budget())
    assert empty.disposition == "empty" and empty.returned_series == 0 and not empty.samples


def test_series_and_label_limit_plus_one_rejects_the_whole_result():
    assert normalize_prometheus_measurement(query(), vector(*range(64)), budget()).disposition == "captured"
    assert normalize_prometheus_measurement(query(), vector(*range(65)), budget()).disposition == "limited"
    labels = {f"label{index}": "x" for index in range(16)}
    assert normalize_prometheus_measurement(query(), vector(1, labels=labels), budget()).disposition == "captured"
    labels["extra"] = "x"
    assert normalize_prometheus_measurement(query(), vector(1, labels=labels), budget()).disposition == "limited"
    for labels in ({"x" * 65: "x"}, {"x": "x" * 257}, {"x": "\U0001f600" * 257}):
        record = normalize_prometheus_measurement(query(), vector(1, labels=labels), budget())
        assert record.disposition == "limited" and not record.samples


def test_duplicate_series_is_not_an_arbitrary_first_value():
    record = normalize_prometheus_measurement(query(), vector(1, 2, labels={"service": "same"}), budget())
    assert record.disposition == "invalid" and record.reason_code == "duplicate_series" and not record.samples


def test_one_run_budget_spans_queries_and_reserves_samples_atomically():
    work = budget()
    for _ in range(8):
        assert normalize_prometheus_measurement(query(), vector(*range(64)), work).disposition == "captured"
    assert work.measurement_samples == 512
    limited = normalize_prometheus_measurement(query(), vector(1), work)
    assert limited.disposition == "limited" and not limited.samples and work.measurement_samples == 512
    assert work.measurement_bytes > 0


def test_byte_limit_and_contention_fail_closed_without_partial_counter_mutation():
    work = budget(max_measurement_bytes=20)
    assert normalize_prometheus_measurement(query(), vector(1), work).disposition == "limited"
    assert work.measurement_samples == work.measurement_bytes == 0
    work = budget()
    assert work._lock.acquire(blocking=False)
    try:
        record = normalize_prometheus_measurement(query(), vector(1), work)
    finally:
        work._lock.release()
    assert record.disposition == "limited" and record.reason_code == "contention" and not record.samples
    assert normalize_prometheus_measurement(query(), vector(1), work).disposition == "limited"


@pytest.mark.parametrize(
    "changes",
    [
        {"max_measurement_samples": 513},
        {"max_measurement_bytes": 262145},
        {"max_measurement_samples": True},
        {"max_measurement_bytes": 0},
    ],
)
def test_budget_configuration_cannot_multiply_capture_allowance(changes):
    with pytest.raises(ValueError):
        budget(**changes)


def test_admission_revalidates_forged_models_and_hashes_and_source_matching():
    measured = normalize_prometheus_measurement(query(), vector(4), budget())
    assert admit_measurements([measured], dashboard(query())) == (measured,)
    assert admit_measurements([measured, measured], dashboard(query())) == (measured,)
    with pytest.raises(ValueError):
        admit_measurements([measured], dashboard(query(datasource_type="mimir")))
    with pytest.raises(ValueError):
        admit_measurements([measured.model_copy(update={"fact_fingerprint": "0" * 64})])
    forged = measured.model_copy(
        update={
            "samples": (
                MeasurementSample.model_construct(
                    timestamp=0, value=float("inf"), series_fingerprint="1" * 64, scope=()
                ),
            )
        }
    )
    with pytest.raises(ValueError):
        admit_measurements([forged])
    changed = normalize_prometheus_measurement(query(), vector(8), budget())
    with pytest.raises(ValueError):
        admit_measurements([measured, changed])


def test_unavailable_record_is_closed_bounded_and_contains_no_samples():
    record = unavailable_measurement(query(), "legacy_validation")
    assert record.disposition == "not_captured" and record.samples == ()
    assert QueryMeasurement.model_validate_json(record.model_dump_json()) == record
    with pytest.raises(ValueError):
        unavailable_measurement(query(), "arbitrary source text")
    with pytest.raises(ValidationError):
        QueryMeasurement.model_validate({**record.model_dump(), "secret": "not admitted"})


def test_scope_allowlist_hides_other_plaintext_but_preserves_series_identity():
    a = normalize_prometheus_measurement(
        query(), vector(1, labels={"service": "api", "request_id": "secret-a"}), budget()
    )
    b = normalize_prometheus_measurement(
        query(), vector(1, labels={"service": "api", "request_id": "secret-b"}), budget()
    )
    assert "secret-a" not in a.model_dump_json() and "request_id" not in a.model_dump_json()
    assert a.samples[0].series_fingerprint != b.samples[0].series_fingerprint
    assert a.samples[0].scope == b.samples[0].scope
    assert (
        hashlib.sha256(
            json.dumps({"request_id": "secret-a", "service": "api"}, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        == a.samples[0].series_fingerprint
    )


def test_direct_budget_reservation_rejects_invalid_increments():
    work = budget()
    for samples, content in ((True, 1), (-1, 1), (1, True), (1, -1)):
        with pytest.raises(ValueError):
            work.reserve_measurements(samples, content)
    work.reserve_measurements(512, 262144)
    with pytest.raises(AcquisitionWorkLimitError):
        work.reserve_measurements(1, 1)
    assert (work.measurement_samples, work.measurement_bytes) == (512, 262144)


@pytest.mark.parametrize("kind", [[], {}, None, True])
def test_malformed_result_type_is_an_explicit_invalid_result(kind):
    payload = vector(1)
    payload["data"]["resultType"] = kind
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.disposition == "invalid" and not record.samples


def test_unsupported_partial_extension_does_not_look_complete():
    payload = vector(1)
    payload["unrecognized_partial_state"] = True
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.disposition == "invalid" and not record.samples


def test_static_sidecar_cardinality_and_metadata_byte_caps():
    records = [
        unavailable_measurement(query().model_copy(update={"expr": str(index)}), "not_captured")
        for index in range(1024)
    ]
    assert len(admit_measurements(records)) == 1024
    with pytest.raises(ValueError, match="record limit"):
        admit_measurements([*records, records[0]])
    # UTF-8 expansion is bounded at each identity field and across the sidecar,
    # including unavailable records which carry no numerical samples.
    expanded = [
        unavailable_measurement(
            query().model_copy(
                update={
                    "expr": str(index),
                    "datasource_uid": "\U0001f600" * 256,
                    "datasource_type": "\U0001f600" * 128,
                    "query_language": "\U0001f600" * 64,
                }
            ),
            "not_captured",
        )
        for index in range(1024)
    ]
    with pytest.raises(ValueError, match="sidecar byte limit"):
        admit_measurements(expanded)


def test_forged_closed_scalar_fields_are_rejected_before_any_model_serialization(monkeypatch):
    measured = normalize_prometheus_measurement(query(), vector(1), budget())
    invalid = [
        measured.model_copy(update={key: {"large": ["untrusted"] * 1000}})
        for key in (
            "schema_version",
            "disposition",
            "reason_code",
            "result_type",
            "evaluation_time",
            "returned_series",
            "unit",
        )
    ]
    invalid.append(
        measured.model_copy(
            update={
                "samples": (
                    measured.samples[0].model_copy(update={"series_fingerprint": {"large": ["untrusted"] * 1000}}),
                )
            }
        )
    )
    invalid.append(
        measured.model_copy(
            update={
                "samples": (
                    measured.samples[0].model_copy(
                        update={"scope": (measured.samples[0].scope[0].model_copy(update={"name": "x" * 100_000}),)}
                    ),
                )
            }
        )
    )

    def no_serialization(*args, **kwargs):
        pytest.fail("invalid closed scalar reached serialization")

    monkeypatch.setattr(QueryMeasurement, "model_dump", no_serialization)
    for forged in invalid:
        with pytest.raises(ValueError):
            admit_measurements([forged])


def test_reason_and_disposition_cannot_disagree_with_recomputed_hash():
    record = unavailable_measurement(query(), "legacy_validation")
    payload = record.model_dump(mode="json")
    payload["reason_code"] = "numeric_samples"
    payload.pop("fact_fingerprint")
    payload["fact_fingerprint"] = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    with pytest.raises(ValidationError):
        QueryMeasurement.model_validate(payload)


def test_raw_json_sidecar_reuses_the_same_admission_and_revalidation():
    record = normalize_prometheus_measurement(query(), vector(1), budget())
    assert admit_measurements([json.loads(record.model_dump_json())]) == (record,)
    raw = json.loads(record.model_dump_json())
    raw["samples"][0]["scope"][0]["name"] = {"nested": ["not a scope"] * 1000}
    with pytest.raises(ValueError):
        admit_measurements([raw])


def test_numeric_byte_limit_precedes_retained_primitive_copy(monkeypatch):
    import tacit.measurements as measurements

    raw = unavailable_measurement(query(), "legacy_validation").model_dump(mode="json")
    raw.update(disposition="captured", reason_code="numeric_samples", result_type="vector", returned_series=64)
    raw["samples"] = [
        {
            "timestamp": 0,
            "value": 1,
            "series_fingerprint": f"{index:064x}",
            "scope": [{"name": name, "value": "\U0001f600" * 256} for name in sorted(measurements.SCOPE_LABELS)],
        }
        for index in range(64)
    ]

    def no_copy(*args, **kwargs):
        pytest.fail("over-limit source facts reached retained primitive copy")

    monkeypatch.setattr(measurements, "_primitive_record", no_copy)
    with pytest.raises(ValueError, match="aggregate limit"):
        admit_measurements([raw])


def test_raw_aggregate_sample_limit_precedes_all_pydantic_copies(monkeypatch):
    record = normalize_prometheus_measurement(query(), vector(*range(64)), budget())
    records = []
    for index in range(9):
        raw = record.model_dump(mode="json")
        raw["query_fingerprint"] = f"{index:064x}"
        records.append(raw)

    def no_pydantic_copy(*args, **kwargs):
        pytest.fail("over-limit collection reached typed model copy")

    monkeypatch.setattr(QueryMeasurement, "model_validate", no_pydantic_copy)
    with pytest.raises(ValueError, match="aggregate limit"):
        admit_measurements(records)


@pytest.mark.parametrize("target", ["root", "data"])
def test_envelope_cardinality_is_checked_before_key_set_allocation(monkeypatch, target):
    import tacit.measurements as measurements

    payload = vector(1)
    selected = payload if target == "root" else payload["data"]
    for index in range(8):
        selected[f"extra{index}"] = None
    original_set = set

    def guarded_set(value=()):
        if value is selected:
            pytest.fail("oversized envelope reached a key-set allocation")
        return original_set(value)

    monkeypatch.setattr(measurements, "set", guarded_set, raising=False)
    record = normalize_prometheus_measurement(query(), payload, budget())
    assert record.disposition == "invalid" and not record.samples
