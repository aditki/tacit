"""Bounded, source-bound numeric facts from an already-issued instant query.

This module owns no I/O, runtime, transport, task, retry or storage. Source labels
outside the declared scope vocabulary contribute only to a stable series hash.
Availability and captured numeric values do not assert an abnormal condition.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Literal, cast, get_args

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tacit.signals.resolution import AcquisitionWorkLimitError, SignalResolutionWorkBudget

if TYPE_CHECKING:
    from tacit.models.schemas import DashboardSpec, PanelQuery

MAX_SERIES = 64
MAX_LABELS = 16
MAX_RECORDS = 1024
MAX_SAMPLES = 512
MAX_CONTENT_BYTES = 256 * 1024
MAX_SIDECAR_BYTES = 2 * 1024 * 1024
SCOPE_LABELS = frozenset({"service", "service_name", "environment", "namespace", "cluster", "region", "node"})
MeasurementDisposition = Literal["captured", "empty", "unsupported", "invalid", "limited", "not_captured"]
MeasurementReason = Literal[
    "numeric_samples",
    "no_samples",
    "unsupported_datasource",
    "unsupported_result_type",
    "invalid_response",
    "partial_response",
    "invalid_samples",
    "duplicate_series",
    "query_limit",
    "measurement_limit",
    "contention",
    "legacy_validation",
    "not_captured",
    "query_absent",
    "query_bad_uid",
    "query_syntax_error",
    "query_error",
    "validation_unavailable",
]
ResultType = Literal["vector", "scalar", "unknown"]
ScopeName = Literal["service", "service_name", "environment", "namespace", "cluster", "region", "node"]
_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_DISPOSITION_REASONS = {
    "captured": frozenset({"numeric_samples"}),
    "empty": frozenset({"no_samples"}),
    "unsupported": frozenset({"unsupported_datasource", "unsupported_result_type"}),
    "limited": frozenset({"query_limit", "measurement_limit", "contention"}),
    "not_captured": frozenset({"legacy_validation", "not_captured", "validation_unavailable"}),
    "invalid": frozenset(
        {
            "invalid_response",
            "partial_response",
            "invalid_samples",
            "duplicate_series",
            "query_absent",
            "query_bad_uid",
            "query_syntax_error",
            "query_error",
            "validation_unavailable",
        }
    ),
}


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _disposition_facts(disposition, reason, result_type, returned_series, sample_count):
    if reason not in _DISPOSITION_REASONS[disposition]:
        raise ValueError("measurement disposition and reason disagree")
    if disposition == "captured":
        if not sample_count or returned_series != sample_count or result_type not in {"vector", "scalar"}:
            raise ValueError("invalid captured measurement")
        if result_type == "scalar" and sample_count != 1:
            raise ValueError("invalid scalar measurement")
    elif sample_count:
        raise ValueError("unavailable measurement cannot contain numeric facts")
    if disposition == "empty" and (returned_series or result_type != "vector"):
        raise ValueError("invalid empty measurement")


def _text(value: Any, characters: int, utf8: int) -> str:
    if type(value) is not str or len(value) > characters:
        raise ValueError("invalid measurement text")
    try:
        if len(value.encode("utf-8")) > utf8:
            raise ValueError("invalid measurement text")
    except UnicodeError:
        raise ValueError("invalid measurement text") from None
    return value


def _finite(value: Any, *, numeric_string: bool = False) -> float:
    if type(value) is str and numeric_string:
        if len(value) > 128 or _NUMBER.fullmatch(value) is None:
            raise ValueError("invalid measurement number")
    elif type(value) not in (int, float):
        raise ValueError("invalid measurement number")
    try:
        normalized = float(value)
    except (OverflowError, ValueError):
        raise ValueError("invalid measurement number") from None
    if not math.isfinite(normalized):
        raise ValueError("invalid measurement number")
    return normalized


def _query_fields(query: PanelQuery) -> dict[str, str]:
    return {
        "expression": _text(query.expr, 65536, 262144),
        "datasource_uid": _text(query.datasource_uid, 256, 1024),
        "datasource_type": _text(query.datasource_type, 128, 512),
        "query_language": _text(query.query_language, 64, 256),
    }


def query_identity(query: PanelQuery) -> str:
    """Exact output-source identity; no normalization or owner inference."""
    return _sha({"domain": "measured-query/v1", **_query_fields(query)})


class ScopeLabel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: ScopeName
    value: str = Field(max_length=256)

    @field_validator("value", mode="before")
    @classmethod
    def check_text(cls, value):
        return _text(value, 256, 1024)


class MeasurementSample(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    timestamp: float
    value: float
    series_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope: tuple[ScopeLabel, ...] = Field(default=(), max_length=7)

    @field_validator("timestamp", "value", mode="before")
    @classmethod
    def finite_number(cls, value):
        return _finite(value)

    @model_validator(mode="after")
    def unique_scope(self):
        names = [label.name for label in self.scope]
        if names != sorted(set(names)):
            raise ValueError("measurement scope must be unique and sorted")
        return self


class QueryMeasurement(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    schema_version: Literal["query-measurement/1"] = "query-measurement/1"
    query_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    datasource_uid: str = Field(max_length=256)
    datasource_type: str = Field(max_length=128)
    query_language: str = Field(max_length=64)
    disposition: MeasurementDisposition
    reason_code: MeasurementReason
    result_type: ResultType = "unknown"
    evaluation_time: float | None = None
    unit: None = None
    returned_series: int = Field(default=0, ge=0, le=2**31 - 1)
    samples: tuple[MeasurementSample, ...] = Field(default=(), max_length=MAX_SERIES)
    fact_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("datasource_uid", "datasource_type", "query_language", mode="before")
    @classmethod
    def identity_text(cls, value, info):
        characters, utf8 = {"datasource_uid": (256, 1024), "datasource_type": (128, 512), "query_language": (64, 256)}[
            info.field_name
        ]
        return _text(value, characters, utf8)

    @field_validator("returned_series", mode="before")
    @classmethod
    def strict_count(cls, value):
        if type(value) is not int:
            raise ValueError("invalid measurement count")
        return value

    @field_validator("evaluation_time", mode="before")
    @classmethod
    def finite_evaluation(cls, value):
        return None if value is None else _finite(value)

    @model_validator(mode="after")
    def validate_facts(self):
        identities = [sample.series_fingerprint for sample in self.samples]
        if identities != sorted(set(identities)):
            raise ValueError("measurement series must be unique and sorted")
        _disposition_facts(
            self.disposition, self.reason_code, self.result_type, self.returned_series, len(self.samples)
        )
        data = self.model_dump(mode="json", exclude={"fact_fingerprint"})
        if self.fact_fingerprint != _sha(data):
            raise ValueError("measurement fact fingerprint mismatch")
        return self


def _record(data: dict[str, Any]) -> QueryMeasurement:
    data = {
        "schema_version": "query-measurement/1",
        "result_type": "unknown",
        "evaluation_time": None,
        "unit": None,
        "returned_series": 0,
        "samples": [],
        **data,
    }
    data.pop("fact_fingerprint", None)
    return QueryMeasurement.model_validate({**data, "fact_fingerprint": _sha(data)})


def _base(query: PanelQuery, evaluation_time: float | None) -> dict[str, Any]:
    identity = _query_fields(query)
    identity.pop("expression")
    return {
        "query_fingerprint": query_identity(query),
        **identity,
        "evaluation_time": None if evaluation_time is None else _finite(evaluation_time),
    }


def unavailable_measurement(
    query: PanelQuery,
    reason_code: MeasurementReason,
    *,
    disposition: MeasurementDisposition = "not_captured",
    result_type: ResultType = "unknown",
    returned_series: int = 0,
    evaluation_time: float | None = None,
) -> QueryMeasurement:
    return _record(
        {
            **_base(query, evaluation_time),
            "disposition": disposition,
            "reason_code": reason_code,
            "result_type": result_type,
            "returned_series": returned_series,
        }
    )


class _QueryLimit(ValueError):
    pass


def _sample(row: Any) -> dict[str, Any]:
    if type(row) is not dict or set(row) != {"metric", "value"}:
        raise ValueError("invalid measurement sample")
    labels, point = row["metric"], row["value"]
    if type(labels) is not dict or type(point) is not list or len(point) != 2:
        raise ValueError("invalid measurement sample")
    if len(labels) > MAX_LABELS:
        raise _QueryLimit()
    scope = []
    for key, value in labels.items():
        try:
            _text(key, 64, 256)
            _text(value, 256, 1024)
        except ValueError:
            raise _QueryLimit() from None
        if key in SCOPE_LABELS:
            scope.append({"name": key, "value": value})
    return {
        "timestamp": _finite(point[0]),
        "value": _finite(point[1], numeric_string=True),
        "series_fingerprint": _sha(labels),
        "scope": sorted(scope, key=lambda label: label["name"]),
    }


def normalize_prometheus_measurement(
    query: PanelQuery,
    payload: Any,
    work_budget: SignalResolutionWorkBudget,
    *,
    evaluation_time: float | None = None,
) -> QueryMeasurement:
    """Normalize an admitted instant response, rejecting invalid/limited prefixes.

    The first bounded pass retains only series hashes, row indexes and byte
    counts. The shared run budget reserves the whole canonical record before
    typed numeric samples or their retained list are materialized.
    """
    base = _base(query, evaluation_time)

    def unavailable(
        reason: MeasurementReason,
        disposition: MeasurementDisposition = "invalid",
        result_type: ResultType = "unknown",
        count=0,
    ):
        return _record(
            {
                **base,
                "reason_code": reason,
                "disposition": disposition,
                "result_type": result_type,
                "returned_series": count,
            }
        )

    if type(payload) is not dict or payload.get("status") != "success" or type(payload.get("data")) is not dict:
        return unavailable("invalid_response")
    if len(payload) > 6 or set(payload) - {"status", "data", "warnings", "infos", "partial", "isPartial"}:
        return unavailable("invalid_response")
    if len(payload["data"]) != 2 or set(payload["data"]) != {"resultType", "result"}:
        return unavailable("invalid_response")
    if any(key in payload and type(payload[key]) is not list for key in ("warnings", "infos")):
        return unavailable("invalid_response")
    if any(key in payload and type(payload[key]) is not bool for key in ("partial", "isPartial")):
        return unavailable("invalid_response")
    if payload.get("warnings") or payload.get("infos") or payload.get("partial") or payload.get("isPartial"):
        return unavailable("partial_response")
    data = payload["data"]
    result_type = data.get("resultType")
    if type(result_type) is not str:
        return unavailable("invalid_response")
    if result_type not in {"vector", "scalar"}:
        return unavailable("unsupported_result_type", "unsupported")
    result_type = cast(ResultType, result_type)
    result = data.get("result")
    if type(result) is not list:
        return unavailable("invalid_response", result_type=result_type)
    rows = [{"metric": {}, "value": result}] if result_type == "scalar" else result
    count = len(rows)
    if count > MAX_SERIES:
        return unavailable("query_limit", "limited", result_type, min(count, 2**31 - 1))
    if not count:
        return unavailable("no_samples", "empty", result_type)
    order = []
    identities = set()
    sample_bytes = 0
    try:
        for index, row in enumerate(rows):
            sample = _sample(row)
            series = sample["series_fingerprint"]
            if series in identities:
                return unavailable("duplicate_series", result_type=result_type, count=count)
            encoded = _canonical(sample)
            identities.add(series)
            order.append((series, index, hashlib.sha256(encoded).digest()))
            sample_bytes += len(encoded)
    except _QueryLimit:
        return unavailable("query_limit", "limited", result_type, count)
    except (ValueError, TypeError, OverflowError, UnicodeError):
        return unavailable("invalid_samples", result_type=result_type, count=count)
    header = {
        "schema_version": "query-measurement/1",
        **base,
        "disposition": "captured",
        "reason_code": "numeric_samples",
        "result_type": result_type,
        "unit": None,
        "returned_series": count,
        "samples": [],
        "fact_fingerprint": "0" * 64,
    }
    content_bytes = len(_canonical(header)) + sample_bytes + max(0, count - 1)
    try:
        work_budget.reserve_measurements(count, content_bytes)
    except AcquisitionWorkLimitError as error:
        return unavailable(
            "contention" if error.dimension == "contention" else "measurement_limit", "limited", result_type, count
        )
    samples = []
    try:
        for _, index, expected in sorted(order):
            sample = _sample(rows[index])
            if hashlib.sha256(_canonical(sample)).digest() != expected:
                return unavailable("invalid_samples", result_type=result_type, count=count)
            samples.append(sample)
        return _record({**header, "samples": samples})
    except (ValueError, TypeError, OverflowError, UnicodeError):
        return unavailable("invalid_samples", result_type=result_type, count=count)


def _model_shape(record: BaseModel, model: type[BaseModel]) -> None:
    if type(record) is not model:
        raise ValueError("invalid measurement record")
    fields = record.model_fields_set
    if type(fields) is not set or len(fields) > len(model.model_fields):
        raise ValueError("invalid measurement fields")
    for name in fields:
        if type(name) is not str or len(name) > 64 or name not in model.model_fields:
            raise ValueError("invalid measurement fields")
    extra = record.__pydantic_extra__
    if extra is not None and (type(extra) is not dict or len(extra)):
        raise ValueError("invalid measurement extra fields")


def _digest(value: Any) -> None:
    if type(value) is not str or len(value) != 64 or _HASH.fullmatch(value) is None:
        raise ValueError("invalid measurement fingerprint")


def _view_fields(value: Any, model: type[BaseModel]) -> dict[str, Any]:
    if type(value) is dict:
        fields = value
    else:
        _model_shape(value, model)
        fields = value.__dict__
    if type(fields) is not dict or len(fields) != len(model.model_fields):
        raise ValueError("invalid measurement field shape")
    for name in fields:
        if type(name) is not str or len(name) > 64 or name not in model.model_fields:
            raise ValueError("invalid measurement field shape")
    return fields


def _admit_shape(original: QueryMeasurement | dict[str, Any]) -> dict[str, Any]:
    record = _view_fields(original, QueryMeasurement)
    if type(record["schema_version"]) is not str or record["schema_version"] != "query-measurement/1":
        raise ValueError("invalid measurement schema")
    for value, permitted in (
        (record["disposition"], get_args(MeasurementDisposition)),
        (record["reason_code"], get_args(MeasurementReason)),
        (record["result_type"], get_args(ResultType)),
    ):
        if type(value) is not str or len(value) > 40 or value not in permitted:
            raise ValueError("invalid measurement closed field")
    if type(record["returned_series"]) is not int or not 0 <= record["returned_series"] <= 2**31 - 1:
        raise ValueError("invalid measurement count")
    if record["unit"] is not None:
        raise ValueError("unproven measurement unit")
    if record["evaluation_time"] is not None:
        _finite(record["evaluation_time"])
    _text(record["datasource_uid"], 256, 1024)
    _text(record["datasource_type"], 128, 512)
    _text(record["query_language"], 64, 256)
    for digest in (record["query_fingerprint"], record["fact_fingerprint"]):
        _digest(digest)
    if type(record["samples"]) not in (tuple, list) or len(record["samples"]) > MAX_SERIES:
        raise ValueError("invalid measurement cardinality")
    _disposition_facts(
        record["disposition"],
        record["reason_code"],
        record["result_type"],
        record["returned_series"],
        len(record["samples"]),
    )
    for source_sample in record["samples"]:
        sample = _view_fields(source_sample, MeasurementSample)
        if type(sample["scope"]) not in (tuple, list) or len(sample["scope"]) > 7:
            raise ValueError("invalid measurement sample")
        _finite(sample["timestamp"])
        _finite(sample["value"])
        _digest(sample["series_fingerprint"])
        for source_label in sample["scope"]:
            label = _view_fields(source_label, ScopeLabel)
            _text(label["name"], 64, 256)
            if label["name"] not in SCOPE_LABELS:
                raise ValueError("invalid measurement scope")
            _text(label["value"], 256, 1024)
    return record


def _primitive_sample(source: Any) -> dict[str, Any]:
    sample = _view_fields(source, MeasurementSample)
    return {
        "timestamp": _finite(sample["timestamp"]),
        "value": _finite(sample["value"]),
        "series_fingerprint": sample["series_fingerprint"],
        "scope": [dict(_view_fields(label, ScopeLabel)) for label in sample["scope"]],
    }


def _record_size(record: dict[str, Any]) -> int:
    """Count one bounded sample at a time before retained collection copies."""
    header = {
        **record,
        "samples": [],
        "evaluation_time": None if record["evaluation_time"] is None else _finite(record["evaluation_time"]),
    }
    return (
        len(_canonical(header))
        + max(0, len(record["samples"]) - 1)
        + sum(len(_canonical(_primitive_sample(sample))) for sample in record["samples"])
    )


def _primitive_record(record: dict[str, Any]) -> dict[str, Any]:
    """Copy only after every scalar, nested cardinality and byte was admitted."""
    samples = [_primitive_sample(source) for source in record["samples"]]
    return {
        **record,
        "samples": samples,
        "evaluation_time": None if record["evaluation_time"] is None else _finite(record["evaluation_time"]),
    }


def admit_measurements(
    records: Iterable[QueryMeasurement | dict[str, Any]],
    dashboard_spec: DashboardSpec | None = None,
    *,
    count_duplicates: bool = False,
) -> tuple[QueryMeasurement, ...]:
    """Revalidate copied/constructed records before persistence/publication.

    Numeric content has the run ceiling; unavailable identity metadata has an
    independent hard two-MiB admission bound and the existing dashboard query
    cardinality bound. This creates no second mutable work authority.
    """
    queries = None
    if dashboard_spec is not None:
        queries = {}
        count = 0
        for panel in dashboard_spec.panels:
            for query in panel.queries:
                count += 1
                if count > MAX_RECORDS:
                    raise ValueError("measurement query limit")
                queries[query_identity(query)] = _query_fields(query)
    admitted: dict[str, dict[str, Any]] = {}
    samples = numeric_bytes = total_bytes = 0
    for index, original in enumerate(records):
        if index >= MAX_RECORDS:
            raise ValueError("measurement record limit")
        view = _admit_shape(original)
        previous = admitted.get(view["query_fingerprint"])
        if (previous is None or count_duplicates) and view["disposition"] == "captured":
            samples += len(view["samples"])
            if samples > MAX_SAMPLES:
                raise ValueError("measurement aggregate limit")
        encoded_size = _record_size(view)
        total_bytes += encoded_size
        if total_bytes > MAX_SIDECAR_BYTES:
            raise ValueError("measurement sidecar byte limit")
        if (previous is None or count_duplicates) and view["disposition"] == "captured":
            numeric_bytes += encoded_size
            if numeric_bytes > MAX_CONTENT_BYTES:
                raise ValueError("measurement aggregate limit")
        record = _primitive_record(view)
        if queries is not None:
            source = queries.get(record["query_fingerprint"])
            if source is None or any(
                record[key] != source[key] for key in ("datasource_uid", "datasource_type", "query_language")
            ):
                raise ValueError("measurement query identity mismatch")
        if previous is not None:
            if previous != record:
                raise ValueError("conflicting measurement facts")
            continue
        admitted[record["query_fingerprint"]] = record
    # All collection-level bounds precede Pydantic's full typed copies.
    return tuple(QueryMeasurement.model_validate(admitted[key]) for key in sorted(admitted))
