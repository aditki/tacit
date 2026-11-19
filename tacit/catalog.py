"""Helpers for applying request context to discovered metric catalogs."""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal

from tacit.investigation_contract import DecisionLogEntry, ProvenanceRecord
from tacit.models.schemas import MetricEntry, QueryTarget
from tacit.signals.resolution import (
    ResolutionInputTextLimits,
    ResolutionInputWorkLimitError,
    admit_resolution_input_text,
)

ACQUISITION_VERSION = "catalog-acquisition/1"
ACQUISITION_LIMITS = {
    "calls": 32,
    "response_bytes": 8 * 1024 * 1024,
    "entries": 5000,
    "sources": 64,
    "selections": 1024,
    "conflicts": 1024,
    "record_bytes": 4 * 1024 * 1024,
    "extensions_bytes": 2 * 1024 * 1024,
    "capture_bytes": 6 * 1024 * 1024,
}
_REASONS = frozenset(
    {"authoritative_terminal", "local_cap", "provider_cap_unknown", "legacy_unknown", "provider_failed", "work_limit"}
)
_METRIC_FIELDS = frozenset(MetricEntry.model_fields)
_SOURCE_ID_FIELDS = ("backend", "datasource_uid", "datasource_type", "query_language", "terms", "summary")
_SELECTION_ID_FIELDS = ("consumer", "subject_ref", "required_metrics", "query_language", "requested_uid")
_CONSUMERS = frozenset({"archetype", "declared_evidence", "symptom_rescue", "gap_rescue", "freeform_selected"})
_MAX_SEQUENCE = 9_999_999_999


class AcquisitionCaptureInvalidError(ValueError):
    """A closed acquisition record or its references failed admission."""

    reason = reason_code = "acquisition_capture_invalid"

    def __init__(self, dimension: str = "schema") -> None:
        self.dimension = dimension
        super().__init__(f"{self.reason}: {dimension}")


class AcquisitionCaptureLimitError(ValueError):
    """A bounded acquisition value exceeded its admitted envelope."""

    reason = reason_code = "acquisition_capture_limit"

    def __init__(self, dimension: str, observed: int, limit: int) -> None:
        self.dimension, self.observed, self.limit = dimension, observed, limit
        super().__init__(f"{self.reason}: {dimension} exceeds {limit}")


def _limit(dimension: str, observed: int, limit: int) -> None:
    if observed > limit:
        raise AcquisitionCaptureLimitError(dimension, observed, limit)


def _text(values: Iterable[object]) -> dict[str, int]:
    try:
        return admit_resolution_input_text(values)
    except ResolutionInputWorkLimitError as exc:
        if exc.dimension == "scalar_type":
            raise AcquisitionCaptureInvalidError("scalar_type") from None
        raise AcquisitionCaptureLimitError(exc.dimension, exc.observed, exc.limit) from None
    except UnicodeError:
        raise AcquisitionCaptureInvalidError("unicode") from None


@dataclass(slots=True)
class _CatalogTextAdmission:
    characters: int = 0
    utf8_bytes: int = 0

    def add(self, values: Iterable[object]) -> None:
        counts = _text(values)
        limits = ResolutionInputTextLimits()
        characters = self.characters + counts["input_character_count"]
        utf8_bytes = self.utf8_bytes + counts["input_utf8_byte_count"]
        _limit("total_input_characters", characters, limits.max_total_characters)
        _limit("total_input_utf8_bytes", utf8_bytes, limits.max_total_utf8_bytes)
        self.characters, self.utf8_bytes = characters, utf8_bytes


def _canonical_count(value: Any, ceiling: int, dimension: str, *, collect: bool = False) -> bytes:
    """Count encoder pieces before accepting them; never build an oversized JSON body."""
    encoder = json.JSONEncoder(sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    pieces: list[bytes] = []
    count = 0
    try:
        for piece in encoder.iterencode(value):
            encoded = piece.encode("utf-8")
            count += len(encoded)
            _limit(dimension, count + 1, ceiling)
            if collect:
                pieces.append(encoded)
    except (TypeError, ValueError, UnicodeError) as exc:
        if isinstance(exc, AcquisitionCaptureLimitError):
            raise
        raise AcquisitionCaptureInvalidError("json") from None
    if collect:
        return b"".join(pieces) + b"\n"
    return b""


def canonical_acquisition_bytes(value: Any) -> bytes:
    """Canonical C(x), for already bounded acquisition values."""
    return _canonical_count(value, ACQUISITION_LIMITS["capture_bytes"], "capture_bytes", collect=True)


def acquisition_hash(domain: str, value: Any) -> str:
    return hashlib.sha256(domain.encode("ascii") + b"\0" + canonical_acquisition_bytes(value)).hexdigest()


def acquisition_entry_id(entry: MetricEntry | Mapping[str, Any]) -> str:
    metric = entry.model_dump() if isinstance(entry, MetricEntry) else entry
    return "acq_entry_" + acquisition_hash(
        "acq-entry-v1",
        {key: metric[key] for key in ("name", "datasource_uid", "datasource_type", "query_language", "namespace")},
    )


def acquisition_entry_ids(entries: Iterable[MetricEntry]) -> frozenset[str]:
    """Collect canonical identities independently of their bounded metadata facts."""
    identities: set[str] = set()
    for count, entry in enumerate(entries, 1):
        _limit("entries", count, ACQUISITION_LIMITS["entries"])
        identities.add(acquisition_entry_id(entry))
    return frozenset(identities)


@dataclass(frozen=True, slots=True)
class CatalogSourceStatus:
    """Immutable facts for one admitted search, before repeated-search aggregation."""

    backend: str
    datasource_uid: str = ""
    datasource_type: str = ""
    query_language: str = ""
    terms: tuple[str, ...] = ()
    returned_count: int = 0
    retained_count: int = 0
    total_before_cap: int | None = None
    disposition: Literal["complete", "incomplete", "unavailable"] = "incomplete"
    reasons: tuple[str, ...] = ("provider_cap_unknown",)
    summary: bool = False
    retained_entry_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if any(
            isinstance(value, str) or not isinstance(value, (tuple, list))
            for value in (self.terms, self.reasons, self.retained_entry_ids)
        ):
            raise AcquisitionCaptureInvalidError("source_array")
        _limit("source_terms", len(self.terms), 256)
        _limit("source_retained_ids", len(self.retained_entry_ids), 5000)
        _text(
            (
                self.backend,
                self.datasource_uid,
                self.datasource_type,
                self.query_language,
                *self.terms,
                *self.reasons,
                *self.retained_entry_ids,
            )
        )
        object.__setattr__(self, "terms", tuple(sorted(set(self.terms))))
        object.__setattr__(self, "reasons", tuple(sorted(set(self.reasons))))
        object.__setattr__(self, "retained_entry_ids", tuple(sorted(set(self.retained_entry_ids))))
        _validate_source(self.to_record(), verify_id=False)
        if self.retained_entry_ids and len(self.retained_entry_ids) != self.retained_count:
            raise AcquisitionCaptureInvalidError("source_retained_ids")

    @property
    def id(self) -> str:
        return "acq_source_" + acquisition_hash(
            "acq-source-v1",
            {key: list(getattr(self, key)) if key == "terms" else getattr(self, key) for key in _SOURCE_ID_FIELDS},
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "backend": self.backend,
            "datasource_uid": self.datasource_uid,
            "datasource_type": self.datasource_type,
            "query_language": self.query_language,
            "terms": list(self.terms),
            "returned_count": self.returned_count,
            "retained_count": self.retained_count,
            "total_before_cap": self.total_before_cap,
            "disposition": self.disposition,
            "reasons": list(self.reasons),
            "summary": self.summary,
        }


@dataclass(frozen=True, slots=True, init=False)
class MetricCatalogResult:
    """A shareable result; mutable historical MetricEntry objects never escape by reference."""

    _entries: tuple[MetricEntry, ...] = field(repr=False)
    sources: tuple[CatalogSourceStatus, ...]

    def __init__(self, entries: Iterable[MetricEntry] = (), sources: Iterable[CatalogSourceStatus] = ()) -> None:
        copied: list[MetricEntry] = []
        text_admission = _CatalogTextAdmission()
        dimension_count = 0
        for entry in entries:
            _limit("entries", len(copied) + 1, 5000)
            raw = entry.model_dump()
            _validate_metric(raw)
            text_admission.add([value for value in raw.values() if isinstance(value, str)] + entry.dimensions)
            dimension_count += len(entry.dimensions)
            _limit("dimensions", dimension_count, 100_000)
            copied.append(entry.model_copy(deep=True))
        statuses: list[CatalogSourceStatus] = []
        for source in sources:
            # Calls are bounded independently of the aggregated output source count.
            _limit("source_calls", len(statuses) + 1, 96)
            if not isinstance(source, CatalogSourceStatus):
                raise AcquisitionCaptureInvalidError("source_type")
            text_admission.add(
                (
                    source.backend,
                    source.datasource_uid,
                    source.datasource_type,
                    source.query_language,
                    *source.terms,
                    *source.reasons,
                )
            )
            statuses.append(source)
        object.__setattr__(self, "_entries", tuple(copied))
        object.__setattr__(self, "sources", tuple(statuses))

    @property
    def entries(self) -> tuple[MetricEntry, ...]:
        return tuple(entry.model_copy(deep=True) for entry in self._entries)

    def as_list(self) -> list[MetricEntry]:
        return list(self.entries)


def admit_catalog_facts(entries: Iterable[MetricEntry]) -> tuple[MetricEntry, ...]:
    """Admit raw facts once; only semantically equivalent full facts coalesce.

    Retention accounting belongs to the existing investigation budget. Distinct
    unit/type/source declarations stay available for shared conflict detection.
    """
    bounded = MetricCatalogResult(entries)
    unique: dict[bytes, MetricEntry] = {}
    for entry in bounded.entries:
        entry.dimensions = sorted(set(entry.dimensions))
        encoded = canonical_acquisition_bytes(entry.model_dump())
        unique.setdefault(encoded, entry)
    return tuple(unique[key] for key in sorted(unique))


def expand_metric_metadata_facts(
    entry: MetricEntry, definitions: Iterable[Mapping[str, Any]]
) -> tuple[MetricEntry, ...]:
    """Merge unique known metadata; retain contradictory definitions as facts."""
    _validate_metric(entry.model_dump())
    pairs: set[tuple[str, str]] = set()
    text_admission = _CatalogTextAdmission()
    for count, definition in enumerate(definitions, 1):
        _limit("metadata_definitions", count, 5000)
        if not isinstance(definition, Mapping):
            raise AcquisitionCaptureInvalidError("metadata_definition")
        unit = definition.get("unit", "")
        kind = definition.get("metric_type", definition.get("type", ""))
        text_admission.add((unit, kind))
        pairs.add((unit, kind))
    if entry.unit or entry.metric_type:
        pairs.add((entry.unit, entry.metric_type))
    units = {unit for unit, _ in pairs if unit}
    kinds = {kind for _, kind in pairs if kind}
    if len(units) <= 1 and len(kinds) <= 1:
        return admit_catalog_facts(
            (entry.model_copy(update={"unit": next(iter(units), ""), "metric_type": next(iter(kinds), "")}),)
        )
    pairs.discard(("", ""))
    filled = sorted(
        {
            (
                unit or (next(iter(units)) if len(units) == 1 else ""),
                kind or (next(iter(kinds)) if len(kinds) == 1 else ""),
            )
            for unit, kind in pairs
        }
    )
    return admit_catalog_facts(entry.model_copy(update={"unit": unit, "metric_type": kind}) for unit, kind in filled)


def legacy_metric_catalog(
    entries: Iterable[MetricEntry], *, backend: str, terms: Iterable[str] = ()
) -> MetricCatalogResult:
    copied = MetricCatalogResult(entries).entries
    # Keep original bounded metadata facts until shared normalization can audit
    # conflicts; projection here must never turn a conflicting duplicate into proof.
    groups: dict[tuple[str, str, str], list[MetricEntry]] = {}
    unknown = not copied
    for entry in copied:
        identity = (entry.datasource_uid, entry.datasource_type, entry.query_language)
        if not all(identity):
            unknown = True
            continue
        if identity not in groups:
            _limit("sources", len(groups) + 1, 64)
        groups.setdefault(identity, []).append(entry)
    statuses = []
    search_terms = tuple(terms)
    for (uid, datasource_type, language), group in sorted(groups.items()):
        retained_ids = tuple(sorted(acquisition_entry_ids(group)))
        statuses.append(
            CatalogSourceStatus(
                backend=backend,
                datasource_uid=uid,
                datasource_type=datasource_type,
                query_language=language,
                terms=search_terms,
                returned_count=len(group),
                retained_count=len(retained_ids),
                reasons=("legacy_unknown",),
                retained_entry_ids=retained_ids,
            )
        )
    if unknown:
        _limit("sources", len(statuses) + 1, 64)
        statuses.append(CatalogSourceStatus(backend=backend, summary=True, reasons=("legacy_unknown",)))
    return MetricCatalogResult(entries=copied, sources=statuses)


@dataclass(frozen=True, slots=True)
class CatalogConflict:
    entry_id: str
    field: str
    values: tuple[str, ...]

    @property
    def id(self) -> str:
        return "acq_conflict_" + acquisition_hash(
            "acq-conflict-v1", {"entry_id": self.entry_id, "field": self.field, "values": list(self.values)}
        )

    def to_record(self) -> dict[str, Any]:
        return {"id": self.id, "entry_id": self.entry_id, "field": self.field, "values": list(self.values)}


@dataclass(frozen=True, slots=True)
class CatalogNormalization:
    catalog: MetricCatalogResult
    conflicts: tuple[CatalogConflict, ...]

    @property
    def entries(self) -> tuple[MetricEntry, ...]:
        return self.catalog.entries


def normalize_catalog(catalog: Iterable[MetricEntry] | MetricCatalogResult) -> CatalogNormalization:
    groups: dict[str, list[MetricEntry]] = {}
    text_admission = _CatalogTextAdmission()
    dimensions = 0
    for count, entry in enumerate(catalog.entries if isinstance(catalog, MetricCatalogResult) else catalog, 1):
        _limit("catalog_inputs", count, 100_000)
        raw = entry.model_dump()
        _validate_metric(raw)
        dimensions += len(entry.dimensions)
        _limit("dimensions", dimensions, 100_000)
        text_admission.add([value for value in raw.values() if isinstance(value, str)] + entry.dimensions)
        identity = acquisition_entry_id(entry)
        if identity not in groups:
            _limit("entries", len(groups) + 1, 5000)
        groups.setdefault(identity, []).append(entry)
    merged: list[MetricEntry] = []
    conflicts: list[CatalogConflict] = []
    source_declarations: dict[tuple[str, str], set[str]] = {}
    for group in groups.values():
        for entry in group:
            source_declarations.setdefault((entry.datasource_uid, _language(entry.query_language)), set()).add(
                json.dumps(
                    {
                        "datasource_name": entry.datasource_name,
                        "datasource_type": _provider(entry.datasource_type),
                        "datasource_is_default": entry.datasource_is_default,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
    for identity, group in sorted(groups.items()):
        raw = min((entry.model_dump() for entry in group), key=canonical_acquisition_bytes)
        raw["dimensions"] = sorted({dimension for entry in group for dimension in entry.dimensions})
        _limit("entry_dimensions", len(raw["dimensions"]), 128)
        for field_name in ("unit", "metric_type"):
            known = sorted({getattr(entry, field_name) for entry in group if getattr(entry, field_name)})
            raw[field_name] = known[0] if known else ""
            if len(known) > 1:
                conflicts.append(CatalogConflict(identity, field_name, tuple(known)))
        declarations = source_declarations[(raw["datasource_uid"], _language(raw["query_language"]))]
        if len(declarations) > 1:
            conflicts.append(CatalogConflict(identity, "source_declaration", tuple(sorted(declarations))))
        _limit("conflicts", len(conflicts), 1024)
        merged.append(MetricEntry.model_validate(raw))
    return CatalogNormalization(MetricCatalogResult(merged), tuple(sorted(conflicts, key=lambda item: item.id)))


def _language(language: str) -> str:
    return {"elasticsearch": "lucene", "opensearch": "lucene"}.get(language.lower(), language.lower())


def _provider(provider: str) -> str:
    lowered = provider.lower()
    return {
        "grafana-signalfx-datasource": "signalfx",
        "opensearch": "elasticsearch",
        "grafana-opensearch-datasource": "elasticsearch",
        "grafana-prometheus-datasource": "prometheus",
    }.get(lowered, lowered)


@dataclass(frozen=True, slots=True, init=False)
class QueryTargetCatalog:
    """One admitted ownership snapshot for a synchronous compilation/resolution operation.

    Construction always derives normalization from raw facts. The private index
    shares only this snapshot's owned facts; public results never expose them.
    The source reference binds nested operations, not a cache or runtime owner.
    """

    _source: object = field(repr=False)
    _normalized: CatalogNormalization = field(repr=False)
    _by_metric: Mapping[tuple[str, str], tuple[MetricEntry, ...]] = field(repr=False)

    def __init__(self, catalog: Iterable[MetricEntry] | MetricCatalogResult) -> None:
        normalized = normalize_catalog(catalog)
        by_metric: dict[tuple[str, str], list[MetricEntry]] = {}
        for entry in normalized.catalog._entries:
            by_metric.setdefault((_language(entry.query_language), entry.name), []).append(entry)
        object.__setattr__(self, "_source", catalog)
        object.__setattr__(self, "_normalized", normalized)
        object.__setattr__(
            self, "_by_metric", MappingProxyType({key: tuple(value) for key, value in by_metric.items()})
        )

    @property
    def conflicts(self) -> tuple[CatalogConflict, ...]:
        return self._normalized.conflicts


def query_target_catalog(
    catalog: Iterable[MetricEntry] | MetricCatalogResult | QueryTargetCatalog,
    *,
    _prepared: QueryTargetCatalog | None = None,
) -> QueryTargetCatalog:
    """Prepare once, or reuse the exact same operation's admitted ownership view."""
    if _prepared is not None:
        if not isinstance(_prepared, QueryTargetCatalog) or _prepared._source is not catalog:
            raise AcquisitionCaptureInvalidError("ownership_catalog_source")
        return _prepared
    return catalog if isinstance(catalog, QueryTargetCatalog) else QueryTargetCatalog(catalog)


@dataclass(frozen=True, slots=True)
class QueryTargetSelection:
    status: Literal["selected", "missing", "ambiguous"]
    _target: QueryTarget | None = field(repr=False)
    candidate_uids: tuple[str, ...] = ()
    conflicts: tuple[CatalogConflict, ...] = ()
    reason: str = "no_owner"

    @property
    def target(self) -> QueryTarget | None:
        return self._target.model_copy(deep=True) if self._target is not None else None


def select_query_target(
    catalog: Iterable[MetricEntry] | MetricCatalogResult | QueryTargetCatalog,
    required_metric_names: Iterable[str],
    query_language: str,
    requested_uid: str | None = None,
) -> QueryTargetSelection:
    """Select one exact, all-operand owner without default or sampled-label heuristics."""
    operands: set[str] = set()
    for name in required_metric_names:
        if not isinstance(name, str) or not name:
            raise AcquisitionCaptureInvalidError("required_metrics")
        operands.add(name)
        _limit("selection_operands", len(operands), 64)
    _text((*operands, query_language, *(() if requested_uid is None else (requested_uid,))))
    if not operands:
        return QueryTargetSelection("missing", None, reason="no_owner")
    admitted = query_target_catalog(catalog)
    return _select_normalized_target(admitted, operands, query_language, requested_uid)


def _select_normalized_target(
    normalized: QueryTargetCatalog, operands: set[str], query_language: str, requested_uid: str | None
) -> QueryTargetSelection:
    language = _language(query_language)
    by_owner: dict[str, dict[str, list[MetricEntry]]] = {}
    available: set[str] = set()
    for name in operands:
        for entry in normalized._by_metric.get((language, name), ()):
            if not entry.datasource_uid:
                continue
            available.add(entry.name)
            by_owner.setdefault(entry.datasource_uid, {}).setdefault(entry.name, []).append(entry)
    complete = {uid: entries for uid, entries in by_owner.items() if operands <= entries.keys()}
    if requested_uid is not None:
        complete = {uid: entries for uid, entries in complete.items() if uid == requested_uid}
    candidates = tuple(sorted(complete))
    _limit("candidate_uids", len(candidates), 64)
    relevant_ids = _selection_entry_ids(
        (entry for entries in complete.values() for matches in entries.values() for entry in matches),
        operands,
        language,
        candidates,
    )
    conflicts = tuple(conflict for conflict in normalized.conflicts if conflict.entry_id in relevant_ids)
    _limit("selection_conflicts", len(conflicts), 64)
    if conflicts:
        return QueryTargetSelection("ambiguous", None, candidates, conflicts, "metadata_conflict")
    if len(candidates) > 1:
        return QueryTargetSelection("ambiguous", None, candidates, reason="multiple_owners")
    if candidates:
        first = min(
            (entry for entries in complete[candidates[0]].values() for entry in entries), key=acquisition_entry_id
        )
        target = QueryTarget.from_metric(first).model_copy(update={"query_language": language})
        return QueryTargetSelection("selected", target, candidates, reason="unique_owner")
    reason = (
        "explicit_uid_mismatch"
        if requested_uid is not None and available >= operands
        else ("missing_operand" if available and available < operands else "no_owner")
    )
    return QueryTargetSelection("missing", None, reason=reason)


def _selection_entry_ids(
    entries: Iterable[MetricEntry], operands: set[str], query_language: str, candidate_uids: tuple[str, ...]
) -> frozenset[str]:
    """Scope conflicts to the exact language and complete owners used by selection."""
    language = _language(query_language)
    return acquisition_entry_ids(
        entry
        for entry in entries
        if entry.datasource_uid in candidate_uids
        and entry.name in operands
        and _language(entry.query_language) == language
    )


def freeform_selection_subject(
    required_metrics: Iterable[str], query_language: str, requested_uid: str | None = None
) -> str:
    return "freeform/" + acquisition_hash(
        "acq-freeform-subject-v1",
        {
            "required_metrics": sorted(set(required_metrics)),
            "query_language": query_language,
            "requested_uid": requested_uid,
        },
    )


def capture_selection(
    selection: QueryTargetSelection,
    *,
    consumer: str,
    subject_ref: str,
    required_metrics: Iterable[str],
    query_language: str,
    requested_uid: str | None = None,
) -> dict[str, Any]:
    names = sorted(set(required_metrics))
    _limit("selection_operands", len(names), 64)
    record = {
        "consumer": consumer,
        "subject_ref": subject_ref,
        "required_metrics": names,
        "query_language": query_language,
        "requested_uid": requested_uid,
        "status": selection.status,
        "target": selection.target.model_dump() if selection.target is not None else None,
        "candidate_uids": list(selection.candidate_uids),
        "conflict_ids": sorted(conflict.id for conflict in selection.conflicts),
        "reason": selection.reason,
    }
    record["id"] = "acq_selection_" + acquisition_hash(
        "acq-selection-v1", {key: record[key] for key in _SELECTION_ID_FIELDS}
    )
    _validate_selection(record)
    return record


_SELECTION_AUDIT: ContextVar[list[dict[str, Any]] | None] = ContextVar("acquisition_selection_audit", default=None)


def begin_selection_audit() -> Token:
    return _SELECTION_AUDIT.set([])


def end_selection_audit(token: Token) -> None:
    _SELECTION_AUDIT.reset(token)


def recorded_selections() -> tuple[dict[str, Any], ...]:
    return tuple(copy.deepcopy(_SELECTION_AUDIT.get() or ()))


def record_query_selection(selection: QueryTargetSelection, **kwargs: Any) -> None:
    audit = _SELECTION_AUDIT.get()
    if audit is not None:
        captured = capture_selection(selection, **kwargs)
        previous = next((item for item in audit if item["id"] == captured["id"]), None)
        if previous is not None:
            if previous != captured:
                raise AcquisitionCaptureInvalidError("selection_conflict")
            return
        _limit("selections", len(audit) + 1, 1024)
        audit.append(captured)


def _aggregate_sources(sources: Iterable[CatalogSourceStatus], entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[CatalogSourceStatus]] = {}
    entry_ids = {entry["id"] for entry in entries}
    for source in sources:
        if source.id not in groups:
            _limit("sources", len(groups) + 1, 64)
        groups.setdefault(source.id, []).append(source)
        _limit("source_calls", sum(map(len, groups.values())), 96)
    result = []
    _limit("backend_summaries", sum(any(call.summary for call in calls) for calls in groups.values()), 32)
    _limit("active_backends", len({calls[0].backend for calls in groups.values()}), 32)
    for identity, calls in sorted(groups.items()):
        record = calls[0].to_record()
        returned = sum(call.returned_count for call in calls)
        retained: set[str] = set()
        for call in calls:
            ids = set(call.retained_entry_ids)
            if not ids and call.retained_count:
                matches = {
                    entry["id"]
                    for entry in entries
                    if all(
                        not getattr(call, key) or entry["metric"][key] == getattr(call, key)
                        for key in ("datasource_uid", "datasource_type", "query_language")
                    )
                }
                if len(matches) != call.retained_count:
                    raise AcquisitionCaptureInvalidError("source_retained_ids_required")
                ids = matches
            if not ids <= entry_ids:
                raise AcquisitionCaptureInvalidError("source_entry_reference")
            retained.update(ids)
        reasons = {reason for call in calls for reason in call.reasons}
        totals = {call.total_before_cap for call in calls}
        only_total = next(iter(totals)) if len(totals) == 1 else None
        known = only_total is not None and returned <= only_total
        total = calls[0].total_before_cap if known else None
        if not known:
            reasons.add("provider_cap_unknown")
        failed = all(call.disposition == "unavailable" for call in calls)
        complete = known and all(call.disposition == "complete" for call in calls)
        record.update(
            returned_count=returned,
            retained_count=len(retained),
            total_before_cap=total,
            reasons=sorted(reasons),
            disposition="unavailable" if failed else "complete" if complete else "incomplete",
        )
        _validate_source(record)
        result.append(record)
    return result


def _stop_reason(sources: Iterable[Mapping[str, Any]]) -> str:
    reasons = {reason for source in sources for reason in source["reasons"]}
    for reason, stop in (
        ("work_limit", "work_limit"),
        ("provider_failed", "provider_failure"),
        ("legacy_unknown", "legacy_unknown"),
    ):
        if reason in reasons:
            return stop
    return "completed"


def build_acquisition_capture(
    catalog: MetricCatalogResult | Iterable[MetricEntry] = (),
    sources: Iterable[CatalogSourceStatus] = (),
    selections: Iterable[Mapping[str, Any]] = (),
    *,
    limits: Mapping[str, int] | None = None,
    counters: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    normalized = normalize_catalog(catalog)
    entries = [{"id": acquisition_entry_id(entry), "metric": entry.model_dump()} for entry in normalized.entries]
    source_records = _aggregate_sources(
        catalog.sources if isinstance(catalog, MetricCatalogResult) else sources, entries
    )
    selection_records: dict[str, dict[str, Any]] = {}
    for selection in selections:
        _validate_selection(selection)
        identity = selection["id"]
        if identity in selection_records and selection_records[identity] != selection:
            raise AcquisitionCaptureInvalidError("selection_conflict")
        if identity not in selection_records:
            _limit("selections", len(selection_records) + 1, 1024)
            selection_records[identity] = copy.deepcopy(dict(selection))
    effective_limits = dict(ACQUISITION_LIMITS)
    if limits is not None:
        if set(limits) - set(effective_limits):
            raise AcquisitionCaptureInvalidError("limits_fields")
        effective_limits.update(limits)
    effective_counters = {
        "calls_reserved": 0,
        "response_bytes_reserved": 0,
        "entries_retained": len(entries),
        "sources_recorded": len(source_records),
        "selections_recorded": len(selection_records),
        "conflicts_recorded": len(normalized.conflicts),
    }
    if counters is not None:
        if set(counters) - set(effective_counters):
            raise AcquisitionCaptureInvalidError("counters_fields")
        effective_counters.update(counters)
    record = {
        "version": ACQUISITION_VERSION,
        "entries": entries,
        "sources": source_records,
        "selections": [selection_records[key] for key in sorted(selection_records)],
        "conflicts": [conflict.to_record() for conflict in normalized.conflicts],
        "limits": effective_limits,
        "counters": effective_counters,
        "stop_reason": _stop_reason(source_records),
    }
    _validate_record(record)
    return record


def capture_extensions(record: Mapping[str, Any]) -> tuple[tuple[DecisionLogEntry, ...], tuple[ProvenanceRecord, ...]]:
    """Map the canonical payload once; mechanisms reference it rather than duplicating it."""
    _validate_record(record)
    provenance: list[ProvenanceRecord] = []
    decisions: list[DecisionLogEntry] = []
    common = {"version": ACQUISITION_VERSION, "record_ref": "model_inputs.catalog_acquisition_v1"}
    for source in record["sources"]:
        source_id = source["id"]
        provenance.append(
            ProvenanceRecord(
                id="prov_" + source_id,
                source_type="metric_catalog",
                source_ref=source_id,
                source_version=ACQUISITION_VERSION,
                locator={"source_id": source_id},
            )
        )
        decisions.append(
            DecisionLogEntry(
                id="decision_" + source_id,
                sequence=1,
                stage="discovery",
                action="catalog_source",
                subject_ref=source_id,
                inputs=["prov_" + source_id],
                output_ref=source_id,
                reason_code=source["reasons"][0],
                output_status=source["disposition"],
                mechanism={**common, "source_id": source_id},
            )
        )
    for selection in record["selections"]:
        decisions.append(
            DecisionLogEntry(
                id="decision_" + selection["id"],
                sequence=1,
                stage="discovery",
                action="catalog_selection",
                subject_ref=selection["subject_ref"],
                inputs=selection["required_metrics"],
                output_ref=selection["id"],
                reason_code=selection["reason"],
                output_status=selection["status"],
                mechanism={**common, "selection_id": selection["id"]},
            )
        )
    stop_preimage = {
        "source_ids": [source["id"] for source in record["sources"]],
        "selection_ids": [selection["id"] for selection in record["selections"]],
        "stop_reason": record["stop_reason"],
        "limits": record["limits"],
        "counters": record["counters"],
    }
    decisions.append(
        DecisionLogEntry(
            id="decision_acq_stop_" + acquisition_hash("acq-stop-v1", stop_preimage),
            sequence=1,
            stage="discovery",
            action="catalog_stop",
            subject_ref="catalog_acquisition_v1",
            inputs=stop_preimage["source_ids"],
            output_ref="model_inputs.catalog_acquisition_v1",
            reason_code=record["stop_reason"],
            output_status="complete" if record["stop_reason"] == "completed" else "incomplete",
            mechanism=common,
        )
    )
    decisions.sort(key=lambda item: (item.stage, item.action, item.subject_ref, item.id))
    return tuple(decisions), tuple(sorted(provenance, key=lambda item: item.id))


@dataclass(frozen=True, slots=True)
class BaseReferenceIndex:
    contract_ids: frozenset[str] = frozenset()
    requirement_ids: frozenset[str] = frozenset()
    slot_subjects: frozenset[str] = frozenset()
    freeform_subjects: frozenset[str] = frozenset()
    base_decision_max: int = 0

    def __post_init__(self) -> None:
        for name in ("contract_ids", "requirement_ids", "slot_subjects", "freeform_subjects"):
            original = getattr(self, name)
            admitted: set[str] = set()
            for identifier in original:
                if not isinstance(identifier, str) or not identifier or identifier in admitted:
                    raise AcquisitionCaptureInvalidError("base_identifiers")
                admitted.add(identifier)
                _limit("base_identifiers", len(admitted), 200_000)
            _text(admitted)
            object.__setattr__(self, name, frozenset(admitted))
        if not self.requirement_ids <= self.contract_ids:
            raise AcquisitionCaptureInvalidError("base_requirement_ids")
        _count(self.base_decision_max, "base_decision_max", maximum=_MAX_SEQUENCE)
        if any(not re.fullmatch(r"[^/]+/[1-9][0-9]*/[1-9][0-9]*", subject) for subject in self.slot_subjects):
            raise AcquisitionCaptureInvalidError("slot_subject")
        if any(not re.fullmatch(r"freeform/[a-f0-9]{64}", subject) for subject in self.freeform_subjects):
            raise AcquisitionCaptureInvalidError("freeform_subject")


@dataclass(frozen=True, slots=True, init=False)
class AdmittedAcquisitionCapture:
    _record: dict[str, Any] = field(repr=False)
    _decisions: tuple[DecisionLogEntry, ...] = field(repr=False)
    _provenance: tuple[ProvenanceRecord, ...] = field(repr=False)

    def __init__(
        self, record: Mapping[str, Any], decisions: Iterable[DecisionLogEntry], provenance: Iterable[ProvenanceRecord]
    ) -> None:
        object.__setattr__(self, "_record", copy.deepcopy(dict(record)))
        object.__setattr__(self, "_decisions", tuple(item.model_copy(deep=True) for item in decisions))
        object.__setattr__(self, "_provenance", tuple(item.model_copy(deep=True) for item in provenance))

    @property
    def record(self) -> dict[str, Any]:
        return copy.deepcopy(self._record)

    @property
    def additional_decisions(self) -> tuple[DecisionLogEntry, ...]:
        return tuple(item.model_copy(deep=True) for item in self._decisions)

    @property
    def additional_provenance(self) -> tuple[ProvenanceRecord, ...]:
        return tuple(item.model_copy(deep=True) for item in self._provenance)


def _count(value: Any, dimension: str, *, maximum: int = 100_000_000, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < int(positive):
        raise AcquisitionCaptureInvalidError(dimension)
    _limit(dimension, value, maximum)


def _closed(value: Any, keys: Iterable[str], dimension: str) -> None:
    if not isinstance(value, Mapping) or set(value) != set(keys):
        raise AcquisitionCaptureInvalidError(dimension)


def _strings(value: Any, dimension: str, maximum: int, *, nonempty: bool = False) -> None:
    if not isinstance(value, (list, tuple)):
        raise AcquisitionCaptureInvalidError(dimension)
    _limit(dimension, len(value), maximum)
    if any(not isinstance(item, str) or (nonempty and not item) for item in value):
        raise AcquisitionCaptureInvalidError(dimension)
    if list(value) != sorted(set(value)):
        raise AcquisitionCaptureInvalidError(dimension + "_canonical")
    _text(value)


def _validate_metric(metric: Any) -> None:
    _closed(metric, _METRIC_FIELDS, "metric_fields")
    if any(
        not isinstance(value, str)
        for key, value in metric.items()
        if key not in {"dimensions", "datasource_is_default"}
    ):
        raise AcquisitionCaptureInvalidError("metric_scalar")
    if not isinstance(metric["datasource_is_default"], bool):
        raise AcquisitionCaptureInvalidError("metric_default")
    dimensions = metric["dimensions"]
    if not isinstance(dimensions, (tuple, list)):
        raise AcquisitionCaptureInvalidError("dimensions")
    _limit("entry_dimensions", len(dimensions), 128)
    _text([value for value in metric.values() if isinstance(value, str)] + list(dimensions))


def _validate_source(source: Mapping[str, Any], *, verify_id: bool = True) -> None:
    _closed(
        source,
        (*_SOURCE_ID_FIELDS, "id", "returned_count", "retained_count", "total_before_cap", "disposition", "reasons"),
        "source_fields",
    )
    for key in ("backend", "datasource_uid", "datasource_type", "query_language"):
        if not isinstance(source[key], str) or (key == "backend" and not source[key]):
            raise AcquisitionCaptureInvalidError("source_scalar")
    if not source["summary"] and not all(
        source[key] for key in ("datasource_uid", "datasource_type", "query_language")
    ):
        raise AcquisitionCaptureInvalidError("source_identity")
    _strings(source["terms"], "source_terms", 256, nonempty=True)
    _strings(source["reasons"], "source_reasons", 6, nonempty=True)
    if not source["reasons"] or not set(source["reasons"]) <= _REASONS:
        raise AcquisitionCaptureInvalidError("source_reasons")
    for key in ("returned_count", "retained_count"):
        _count(source[key], key)
    total = source["total_before_cap"]
    if total is not None:
        _count(total, "total_before_cap")
    if source["retained_count"] > source["returned_count"] or (total is not None and total < source["returned_count"]):
        raise AcquisitionCaptureInvalidError("source_counts")
    if (
        not isinstance(source["summary"], bool)
        or not isinstance(source["disposition"], str)
        or source["disposition"]
        not in {
            "complete",
            "incomplete",
            "unavailable",
        }
    ):
        raise AcquisitionCaptureInvalidError("source_disposition")
    if source["summary"] and (
        source["terms"] or source["disposition"] == "complete" or source["returned_count"] or source["retained_count"]
    ):
        raise AcquisitionCaptureInvalidError("source_summary")
    if source["disposition"] == "unavailable" and (source["returned_count"] or source["retained_count"]):
        raise AcquisitionCaptureInvalidError("unavailable_entries")
    if source["disposition"] == "complete" and (total is None or source["reasons"] != ["authoritative_terminal"]):
        raise AcquisitionCaptureInvalidError("complete_source")
    if verify_id and source["id"] != "acq_source_" + acquisition_hash(
        "acq-source-v1", {key: source[key] for key in _SOURCE_ID_FIELDS}
    ):
        raise AcquisitionCaptureInvalidError("source_id")


def _validate_selection(selection: Mapping[str, Any]) -> None:
    _closed(
        selection,
        (*_SELECTION_ID_FIELDS, "id", "status", "target", "candidate_uids", "conflict_ids", "reason"),
        "selection_fields",
    )
    if not isinstance(selection["consumer"], str) or selection["consumer"] not in _CONSUMERS:
        raise AcquisitionCaptureInvalidError("consumer")
    for key in ("subject_ref", "query_language"):
        if not isinstance(selection[key], str) or not selection[key]:
            raise AcquisitionCaptureInvalidError("selection_scalar")
    if selection["requested_uid"] is not None and not isinstance(selection["requested_uid"], str):
        raise AcquisitionCaptureInvalidError("requested_uid")
    for key in ("required_metrics", "candidate_uids", "conflict_ids"):
        _strings(selection[key], key, 64, nonempty=True)
    if not selection["required_metrics"]:
        raise AcquisitionCaptureInvalidError("required_metrics")
    status, reason, target = selection["status"], selection["reason"], selection["target"]
    allowed_reasons = {
        "selected": {"unique_owner"},
        "missing": {"missing_operand", "no_owner", "explicit_uid_mismatch"},
        "ambiguous": {"multiple_owners", "metadata_conflict"},
    }
    if (
        not isinstance(status, str)
        or not isinstance(reason, str)
        or status not in allowed_reasons
        or reason not in allowed_reasons[status]
    ):
        raise AcquisitionCaptureInvalidError("selection_status")
    if status == "selected":
        _closed(target, QueryTarget.model_fields, "target_fields")
        if any(not isinstance(value, str) for value in target.values()) or not target["datasource_uid"]:
            raise AcquisitionCaptureInvalidError("target_scalar")
        if selection["candidate_uids"] != [target["datasource_uid"]] or selection["conflict_ids"]:
            raise AcquisitionCaptureInvalidError("selected_candidate")
    elif target is not None:
        raise AcquisitionCaptureInvalidError("unexpected_target")
    if selection["id"] != "acq_selection_" + acquisition_hash(
        "acq-selection-v1", {key: selection[key] for key in _SELECTION_ID_FIELDS}
    ):
        raise AcquisitionCaptureInvalidError("selection_id")


def _walk_capture(value: Any, *, forbid_floats: bool = True) -> None:
    pending = [(value, 0)]
    strings: list[str] = []
    nodes = 0
    while pending:
        node, depth = pending.pop()
        nodes += 1
        _limit("tree_nodes", nodes, 200_000)
        _limit("tree_depth", depth, 12)
        if isinstance(node, Mapping):
            _limit("tree_nodes", nodes + len(pending) + len(node) * 2, 200_000)
            for key, child in node.items():
                if not isinstance(key, str):
                    raise AcquisitionCaptureInvalidError("object_key")
                pending.extend(((key, depth + 1), (child, depth + 1)))
        elif isinstance(node, (list, tuple)):
            _limit("tree_nodes", nodes + len(pending) + len(node), 200_000)
            pending.extend((child, depth + 1) for child in node)
        elif isinstance(node, str):
            strings.append(node)
        elif node is None or isinstance(node, (bool, int)):
            pass
        elif isinstance(node, float) and not forbid_floats:
            pass
        else:
            raise AcquisitionCaptureInvalidError("scalar_type")
    _text(strings)


def _validate_record(record: Mapping[str, Any]) -> None:
    _closed(
        record,
        ("version", "entries", "sources", "selections", "conflicts", "limits", "counters", "stop_reason"),
        "record_fields",
    )
    if record["version"] != ACQUISITION_VERSION:
        raise AcquisitionCaptureInvalidError("version")
    _closed(record["limits"], ACQUISITION_LIMITS, "limits_fields")
    for dimension, maximum in ACQUISITION_LIMITS.items():
        _count(record["limits"][dimension], dimension, maximum=maximum, positive=True)
    for key in ("entries", "sources", "selections", "conflicts"):
        if not isinstance(record[key], (list, tuple)):
            raise AcquisitionCaptureInvalidError(key)
        _limit(key, len(record[key]), record["limits"][key])
    _walk_capture(record)
    indexes: dict[str, dict[str, Any]] = {}
    for key in ("entries", "sources", "selections", "conflicts"):
        ids: list[str] = []
        indexes[key] = {}
        for item in record[key]:
            if not isinstance(item, Mapping) or not isinstance(item.get("id"), str):
                raise AcquisitionCaptureInvalidError(key + "_id")
            ids.append(item["id"])
            indexes[key][item["id"]] = item
        if ids != sorted(set(ids)):
            raise AcquisitionCaptureInvalidError(key + "_canonical")
    dimensions = 0
    metrics = []
    for entry in record["entries"]:
        _closed(entry, ("id", "metric"), "entry_fields")
        _validate_metric(entry["metric"])
        _strings(entry["metric"]["dimensions"], "entry_dimensions", 128)
        dimensions += len(entry["metric"]["dimensions"])
        _limit("dimensions", dimensions, 100_000)
        if entry["id"] != acquisition_entry_id(entry["metric"]):
            raise AcquisitionCaptureInvalidError("entry_id")
        metrics.append(MetricEntry.model_validate(entry["metric"]))
    for source in record["sources"]:
        _validate_source(source)
        if source["retained_count"] > len(record["entries"]):
            raise AcquisitionCaptureInvalidError("source_retained_count")
    _limit("backend_summaries", sum(source["summary"] for source in record["sources"]), 32)
    _limit("active_backends", len({source["backend"] for source in record["sources"]}), 32)
    for conflict in record["conflicts"]:
        _closed(conflict, ("id", "entry_id", "field", "values"), "conflict_fields")
        _strings(conflict["values"], "conflict_values", 100_000, nonempty=True)
        if (
            not isinstance(conflict["field"], str)
            or conflict["field"] not in {"unit", "metric_type", "source_declaration"}
            or len(conflict["values"]) < 2
        ):
            raise AcquisitionCaptureInvalidError("conflict_values")
        if conflict["entry_id"] not in indexes["entries"]:
            raise AcquisitionCaptureInvalidError("conflict_entry_reference")
        if conflict["id"] != CatalogConflict(conflict["entry_id"], conflict["field"], tuple(conflict["values"])).id:
            raise AcquisitionCaptureInvalidError("conflict_id")
    normalized = query_target_catalog(metrics) if record["selections"] else None
    selection_cache: dict[tuple[Any, ...], QueryTargetSelection] = {}
    for selection in record["selections"]:
        _validate_selection(selection)
        if not set(selection["conflict_ids"]) <= indexes["conflicts"].keys():
            raise AcquisitionCaptureInvalidError("selection_conflict_reference")
        # Validate ownership from retained exact names, including conflicts captured before normalization.
        cache_key = (tuple(selection["required_metrics"]), selection["query_language"], selection["requested_uid"])
        if cache_key not in selection_cache:
            assert normalized is not None
            selection_cache[cache_key] = _select_normalized_target(
                normalized, set(selection["required_metrics"]), selection["query_language"], selection["requested_uid"]
            )
        expected = selection_cache[cache_key]
        if selection["candidate_uids"] != list(expected.candidate_uids):
            raise AcquisitionCaptureInvalidError("selection_owner")
        relevant_ids = _selection_entry_ids(
            metrics, set(selection["required_metrics"]), selection["query_language"], expected.candidate_uids
        )
        relevant = {conflict["id"] for conflict in record["conflicts"] if conflict["entry_id"] in relevant_ids}
        if relevant:
            if (
                selection["status"] != "ambiguous"
                or selection["reason"] != "metadata_conflict"
                or set(selection["conflict_ids"]) != relevant
            ):
                raise AcquisitionCaptureInvalidError("selection_owner_conflict")
        else:
            if selection["status"] != expected.status or selection["reason"] != expected.reason:
                raise AcquisitionCaptureInvalidError("selection_owner")
            if selection["target"] != (expected.target.model_dump() if expected.target is not None else None):
                raise AcquisitionCaptureInvalidError("selection_target")
    count_keys = (
        "calls_reserved",
        "response_bytes_reserved",
        "entries_retained",
        "sources_recorded",
        "selections_recorded",
        "conflicts_recorded",
    )
    _closed(record["counters"], count_keys, "counters_fields")
    for key, ceiling in zip(
        count_keys, ("calls", "response_bytes", "entries", "sources", "selections", "conflicts"), strict=True
    ):
        _count(record["counters"][key], key, maximum=record["limits"][ceiling])
        if ceiling not in {"calls", "response_bytes"} and record["counters"][key] != len(record[ceiling]):
            raise AcquisitionCaptureInvalidError("counters_arrays")
    if record["stop_reason"] != _stop_reason(record["sources"]):
        raise AcquisitionCaptureInvalidError("stop_reason")


def admit_acquisition_capture(
    record: Mapping[str, Any],
    additional_decisions: Iterable[DecisionLogEntry | Mapping[str, Any]],
    additional_provenance: Iterable[ProvenanceRecord | Mapping[str, Any]],
    *,
    base_index: BaseReferenceIndex,
) -> AdmittedAcquisitionCapture:
    """Validate schemas, field-specific references, collision domains and bounded canonical bytes."""
    _validate_record(record)
    decision_records: list[dict[str, Any]] = []
    provenance_records: list[dict[str, Any]] = []
    for values, raw_output, model, maximum in (
        (additional_decisions, decision_records, DecisionLogEntry, 1089),
        (additional_provenance, provenance_records, ProvenanceRecord, 64),
    ):
        for value in values:
            _limit("decisions" if maximum == 1089 else "provenance", len(raw_output) + 1, maximum)
            raw = (
                value.model_dump(mode="json")
                if isinstance(value, (DecisionLogEntry, ProvenanceRecord))
                else dict(value)
            )
            _closed(raw, model.model_fields, "extension_fields")
            _walk_capture(raw)
            if model is DecisionLogEntry:
                _count(raw["sequence"], "sequence", maximum=_MAX_SEQUENCE, positive=True)
            try:
                model.model_validate(raw)
            except ValueError:
                raise AcquisitionCaptureInvalidError("extension_schema") from None
            raw_output.append(raw)
    decisions = [DecisionLogEntry.model_validate(raw) for raw in decision_records]
    provenance = [ProvenanceRecord.model_validate(raw) for raw in provenance_records]
    _limit("sequence", base_index.base_decision_max + 1 + len(decisions), _MAX_SEQUENCE)
    expected_decisions, expected_provenance = capture_extensions(record)
    if len(decisions) != len(expected_decisions) or len(provenance) != len(expected_provenance):
        raise AcquisitionCaptureInvalidError("extension_mapping")
    ids = [item.id for item in decisions] + [item.id for item in provenance]
    if (
        len(ids) != len(set(ids))
        or set(ids) & base_index.contract_ids
        or any(re.fullmatch(r"decision_[0-9]+", identity) for identity in ids)
    ):
        raise AcquisitionCaptureInvalidError("extension_id_collision")
    for actual, expected in zip(decisions, expected_decisions, strict=True):
        _count(actual.sequence, "sequence", maximum=_MAX_SEQUENCE, positive=True)
        if actual.model_dump(exclude={"sequence"}) != expected.model_dump(exclude={"sequence"}):
            raise AcquisitionCaptureInvalidError("decision_mapping")
    if provenance != list(expected_provenance):
        raise AcquisitionCaptureInvalidError("provenance_mapping")
    for selection in record["selections"]:
        subject, consumer = selection["subject_ref"], selection["consumer"]
        if consumer == "archetype":
            valid = subject in base_index.slot_subjects
        elif consumer == "freeform_selected":
            valid = subject in base_index.freeform_subjects and subject == freeform_selection_subject(
                selection["required_metrics"], selection["query_language"], selection["requested_uid"]
            )
        else:
            valid = subject in base_index.requirement_ids
        if not valid:
            raise AcquisitionCaptureInvalidError("selection_subject_reference")
    # Every prepublication sequence reserves ten digits, including optional rendering's slot.
    reserved = [{**item, "sequence": _MAX_SEQUENCE} for item in decision_records]
    extensions = {"additional_decisions": reserved, "additional_provenance": provenance_records}
    combined = {"record": record, **extensions}
    _walk_capture(combined)
    _canonical_count(record, record["limits"]["record_bytes"], "record_bytes")
    _canonical_count(extensions, record["limits"]["extensions_bytes"], "extensions_bytes")
    _canonical_count(combined, record["limits"]["capture_bytes"], "capture_bytes")
    return AdmittedAcquisitionCapture(record, decisions, provenance)


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
