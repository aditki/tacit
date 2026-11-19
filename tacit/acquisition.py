"""Bounded acquisition on existing owned clients; no lifecycle authority."""

from __future__ import annotations

import asyncio
import heapq
import json
import math
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import replace
from typing import Any
from urllib.parse import quote

import httpx
import structlog

from tacit.catalog import CatalogSourceStatus, MetricCatalogResult, acquisition_entry_id
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS
from tacit.models.schemas import DatasourceInfo, MetricEntry
from tacit.signals.resolution import (
    AcquisitionWorkLimitError,
    ResolutionInputTextLimits,
    ResolutionInputWorkLimitError,
    SignalResolutionWorkBudget,
    admit_resolution_input_text,
)

RAW_CHUNK_BYTES = 65_536
MAX_JSON_NODES = 100_000
MAX_JSON_DEPTH = 32
MAX_DATASOURCES = 5000
logger = structlog.get_logger()


def standalone_acquisition_budget(settings: Any) -> SignalResolutionWorkBudget:
    from tacit.config import validate_acquisition_runtime

    validate_acquisition_runtime(settings)
    return SignalResolutionWorkBudget(
        256,
        10_000_000,
        100_000,
        max_acquisition_calls=settings.max_acquisition_calls,
        max_acquisition_response_bytes=settings.max_acquisition_response_bytes,
        max_acquisition_entries=min(settings.max_acquisition_entries, settings.signal_resolution_catalog_limit),
    )


def retain_acquisition_facts(
    entries: Iterable[MetricEntry], work_budget: SignalResolutionWorkBudget, *, previous: Iterable[MetricEntry] = ()
) -> tuple[list[MetricEntry], bool]:
    """One retention boundary: reserve identities, preserve metadata facts."""
    from tacit.catalog import admit_catalog_facts, canonical_acquisition_bytes

    admitted = admit_catalog_facts(entries)
    facts = {canonical_acquisition_bytes(entry.model_dump()): entry for entry in admit_catalog_facts(previous)}
    identities = {(entry.datasource_uid, entry.query_language, entry.name, entry.namespace) for entry in facts.values()}
    limited = False
    for entry in admitted:
        fingerprint = canonical_acquisition_bytes(entry.model_dump())
        if fingerprint in facts:
            continue
        identity = (entry.datasource_uid, entry.query_language, entry.name, entry.namespace)
        if identity not in identities:
            if limited:
                continue
            try:
                work_budget.reserve_acquisition_entry(identity)
            except AcquisitionWorkLimitError:
                limited = True
                continue
            identities.add(identity)
        if len(facts) >= 5000:
            raise ValueError("catalog raw metadata fact limit")
        facts[fingerprint] = entry
    return list(admit_catalog_facts(facts.values())), limited


def legacy_acquisition_catalog(
    entries: Iterable[MetricEntry], *, backend: str, terms: Iterable[str], work_budget: SignalResolutionWorkBudget
) -> MetricCatalogResult:
    """Bound a third-party return without claiming its internal transport work."""
    from tacit.catalog import legacy_metric_catalog

    raw_result = legacy_metric_catalog(entries, backend=backend, terms=terms)
    retained, limited = retain_acquisition_facts(raw_result.entries, work_budget)
    identities = {acquisition_entry_id(entry) for entry in retained}
    sources = []
    for source in raw_result.sources:
        members = tuple(sorted(set(source.retained_entry_ids) & identities))
        reasons = tuple(sorted(set(source.reasons) | ({"local_cap", "work_limit"} if limited else set())))
        sources.append(replace(source, retained_count=len(members), retained_entry_ids=members, reasons=reasons))
    return MetricCatalogResult(entries=retained, sources=sources)


def _admit_json_body(body: bytearray) -> Any:
    # Reject deeply nested input before the recursive standard-library decoder.
    depth = 0
    quoted = False
    escaped = False
    syntax_nodes = 0
    for character in body:
        if quoted:
            if escaped:
                escaped = False
            elif character == 92:
                escaped = True
            elif character == 34:
                quoted = False
        elif character == 34:
            quoted = True
            syntax_nodes += 1
        elif character in (44, 58):
            syntax_nodes += 1
        elif character in (91, 123):
            syntax_nodes += 1
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise ValueError("acquisition JSON depth limit")
        elif character in (93, 125):
            depth -= 1
        if syntax_nodes > MAX_JSON_NODES:
            raise ValueError("acquisition JSON node limit")

    def reject_constant(value: str) -> Any:
        raise ValueError("acquisition JSON nonfinite number")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("acquisition JSON duplicate object key")
            result[key] = value
        return result

    data = json.loads(body, parse_constant=reject_constant, object_pairs_hook=unique_object)
    stack = [data]
    count = 0
    texts: list[str] = []
    while stack:
        value = stack.pop()
        count += 1
        if count > MAX_JSON_NODES:
            raise ValueError("acquisition JSON node limit")
        if isinstance(value, dict):
            if len(value) > MAX_JSON_NODES - count:
                raise ValueError("acquisition JSON node limit")
            texts.extend(value.keys())
            stack.extend(value.values())
        elif isinstance(value, list):
            if len(value) > MAX_JSON_NODES - count:
                raise ValueError("acquisition JSON node limit")
            stack.extend(value)
        elif isinstance(value, str):
            texts.append(value)
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError("acquisition JSON nonfinite number")
    admit_resolution_input_text(texts)
    if not isinstance(data, (dict, list)):
        raise ValueError("acquisition response must be a JSON object or array")
    return data


async def bounded_acquisition_request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    work_budget: SignalResolutionWorkBudget,
    **kwargs: Any,
) -> dict | list:
    """Reserve attempt/chunks before dispatch/growth, close on every exit."""
    work_budget.reserve_acquisition_call()
    headers = dict(kwargs.pop("headers", {}))
    headers["Accept-Encoding"] = "identity"
    chunk_size = min(RAW_CHUNK_BYTES, work_budget.max_acquisition_response_bytes)
    async with client.stream(method, path, headers=headers, **kwargs) as response:
        response.raise_for_status()
        if response.headers.get("content-encoding", "identity").lower().strip() not in ("", "identity"):
            raise ValueError("acquisition response encoding must be identity")
        chunks = response.aiter_raw(chunk_size=chunk_size).__aiter__()
        body = bytearray()
        while True:
            allowance = work_budget.reserve_acquisition_read_allowance(chunk_size)
            try:
                chunk = await anext(chunks)
            except StopAsyncIteration:
                break
            if len(chunk) > allowance:
                raise AcquisitionWorkLimitError("acquisition_response_bytes")
            body.extend(chunk)
        return _admit_json_body(body)


def canonical_terms(keywords: Iterable[str]) -> tuple[str, ...]:
    terms = tuple(sorted({word.strip().lower() for word in keywords if word.strip()}))
    if len(terms) > 100:
        raise ValueError("acquisition keyword limit")
    admit_resolution_input_text(terms)
    return terms


class AcquisitionSearches:
    """Bounded per-call metadata; skipped work is one backend summary."""

    def __init__(self, backend: str, work_budget: SignalResolutionWorkBudget, datasource: DatasourceInfo | None = None):
        self.backend = backend
        self.budget = work_budget
        self.datasource = datasource
        self.sources: list[CatalogSourceStatus] = []
        self.skipped = False
        self.skip_reasons: set[str] = set()
        self.inventory_names: dict[int, set[str]] = {}

    def status(self, terms: tuple[str, ...], **kwargs: Any) -> CatalogSourceStatus:
        ds = None if kwargs.get("summary") else self.datasource
        return CatalogSourceStatus(
            backend=self.backend,
            datasource_uid=ds.uid if ds else "",
            datasource_type=ds.type if ds else "",
            query_language=_query_language(ds) if ds else "",
            terms=terms,
            **kwargs,
        )

    async def fetch(
        self,
        terms: tuple[str, ...],
        operation: Callable[[], Awaitable[Any]],
        *,
        cap: int | None = None,
        terminal: bool = False,
    ) -> Any:
        from tacit.config import AcquisitionConfigurationError

        before = self.budget.acquisition_calls
        try:
            data = await operation()
            if isinstance(data, dict) and data.get("status") in {"error", "failed"}:
                raise ValueError("provider reported failure")
            if terminal and not (
                isinstance(data, list) or (isinstance(data, dict) and isinstance(data.get("data"), list))
            ):
                raise ValueError("invalid terminal inventory shape")
            values = data.get("results", data.get("data", data)) if isinstance(data, dict) else data
            count = len(values) if isinstance(values, (dict, list)) else 0
            total = data.get("count") if isinstance(data, dict) else None
            total = total if isinstance(total, int) and not isinstance(total, bool) and total >= count else None
            complete = terminal and (cap is None or count < cap) and (total is None or total <= count)
            self.sources.append(
                self.status(
                    terms,
                    returned_count=count,
                    total_before_cap=count if complete else total,
                    disposition="complete" if complete else "incomplete",
                    reasons=("authoritative_terminal",) if complete else ("provider_cap_unknown",),
                )
            )
            return data
        except AUTHORITY_BOUNDARY_ERRORS:
            raise
        except AcquisitionConfigurationError:
            raise
        except AcquisitionWorkLimitError:
            if self.budget.acquisition_calls == before:
                self.skipped = True
                self.skip_reasons.add("work_limit")
            else:
                self.sources.append(self.status(terms, disposition="incomplete", reasons=("work_limit",)))
            return None
        except Exception:
            self.sources.append(self.status(terms, disposition="unavailable", reasons=("provider_failed",)))
            return None

    def inventory(self, names: Iterable[str]) -> None:
        if self.sources:
            self.inventory_names[len(self.sources) - 1] = set(
                _bounded_names(names, self.budget.max_acquisition_entries + 1)
            )

    def finish(self, entries: list[MetricEntry], *, local_cap: bool = False) -> MetricCatalogResult:
        # Retained source membership is assigned only to inventory searches, not
        # labels/metadata. A source may attest several searches of one catalog.
        sources = []
        for index, source in enumerate(self.sources):
            names = self.inventory_names.get(index, set())
            members = tuple(sorted({acquisition_entry_id(entry) for entry in entries if entry.name in names}))
            if source.disposition == "unavailable":
                members = ()
            reasons = tuple(sorted(set(source.reasons) | ({"local_cap"} if local_cap else set())))
            sources.append(
                replace(
                    source,
                    retained_count=len(members),
                    retained_entry_ids=members,
                    reasons=reasons,
                    disposition=(
                        "incomplete" if local_cap and source.disposition != "unavailable" else source.disposition
                    ),
                )
            )
        if self.skipped:
            sources.append(
                self.status(
                    (),
                    disposition="incomplete",
                    reasons=tuple(sorted(self.skip_reasons or {"work_limit"})),
                    summary=True,
                )
            )
        return MetricCatalogResult(entries=tuple(entries), sources=tuple(sources))


def _query_language(ds: DatasourceInfo) -> str:
    if ds.type in {"prometheus", "mimir", "cortex", "thanos"}:
        return "promql"
    if ds.type in {"signalfx", "grafana-signalfx-datasource"}:
        return "signalflow"
    if ds.type == "loki":
        return "logql"
    if ds.type == "influxdb":
        return "flux" if ds.json_data.get("version") == "Flux" else "influxql"
    return "elasticsearch" if ds.type == "opensearch" else ds.type


def _values(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        values = data.get("results", data.get("data", []))
        return values if isinstance(values, list) else []
    return []


def merge_search_sources(sources: Iterable[CatalogSourceStatus]) -> tuple[CatalogSourceStatus, ...]:
    regular = []
    summaries: dict[str, list[CatalogSourceStatus]] = {}
    for source in sources:
        if source.summary:
            summaries.setdefault(source.backend, []).append(source)
        else:
            regular.append(source)
    for backend, values in sorted(summaries.items()):
        reasons = tuple(sorted({reason for source in values for reason in source.reasons}))
        unavailable = all(source.disposition == "unavailable" for source in values)
        regular.append(
            CatalogSourceStatus(
                backend=backend,
                summary=True,
                reasons=reasons,
                disposition="unavailable" if unavailable else "incomplete",
            )
        )
    return tuple(regular)


class _DescendingName:
    __slots__ = ("value",)

    def __init__(self, value: str):
        self.value = value

    def __lt__(self, other: _DescendingName) -> bool:
        return self.value > other.value


def _admit_inventory_text(values: Iterable[str], totals: list[int], *, removed: Iterable[str] = ()) -> None:
    """Incremental existing text guards, before bounded derived-output growth."""
    incoming = admit_resolution_input_text(values)
    outgoing = admit_resolution_input_text(removed)
    limits = ResolutionInputTextLimits()
    characters = totals[0] - outgoing["input_character_count"] + incoming["input_character_count"]
    utf8_bytes = totals[1] - outgoing["input_utf8_byte_count"] + incoming["input_utf8_byte_count"]
    if characters > limits.max_total_characters:
        raise ResolutionInputWorkLimitError("inventory_characters", characters, limits.max_total_characters)
    if utf8_bytes > limits.max_total_utf8_bytes:
        raise ResolutionInputWorkLimitError("inventory_utf8_bytes", utf8_bytes, limits.max_total_utf8_bytes)
    totals[:] = (characters, utf8_bytes)


def _offer_name(heap: list[_DescendingName], names: set[str], value: str, limit: int, totals: list[int]) -> bool:
    if value in names:
        return False
    if len(heap) < limit:
        _admit_inventory_text((value,), totals)
        heapq.heappush(heap, _DescendingName(value))
        names.add(value)
        return False
    if value < heap[0].value:
        _admit_inventory_text((value,), totals, removed=(heap[0].value,))
        names.remove(heapq.heapreplace(heap, _DescendingName(value)).value)
        names.add(value)
    return True


def _bounded_names(values: Iterable[Any], limit: int = 5001) -> list[str]:
    heap: list[_DescendingName] = []
    names: set[str] = set()
    totals = [0, 0]
    for value in values:
        if isinstance(value, str) and value:
            _offer_name(heap, names, value, limit, totals)
    return sorted(names)


def _iter_names(data: Any) -> Iterable[str]:
    for item in _values(data):
        name = (
            item
            if isinstance(item, str)
            else item.get("name", item.get("id", item.get("text", ""))) if isinstance(item, dict) else ""
        )
        if isinstance(name, str) and name:
            yield name


def _names(data: Any, limit: int = 5001) -> list[str]:
    return _bounded_names(_iter_names(data), limit)


def _select_inventory_names(
    values: Iterable[str],
    terms: tuple[str, ...],
    limit: int,
    *,
    fallback_limit: int = 0,
    excluded_suffixes: tuple[str, ...] = (),
    backfill: bool = False,
) -> tuple[list[str], bool]:
    """Filter each streamed candidate before bounded matching/fallback heaps."""
    matches: set[str] = set()
    fallback: set[str] = set()
    match_heap: list[_DescendingName] = []
    fallback_heap: list[_DescendingName] = []
    match_cap = fallback_cap = False
    match_totals, fallback_totals = [0, 0], [0, 0]
    for name in values:
        if not name or name.endswith(excluded_suffixes):
            continue
        if fallback_limit:
            fallback_cap |= _offer_name(fallback_heap, fallback, name, fallback_limit, fallback_totals)
        if not terms or any(term in name.lower() for term in terms):
            match_cap |= _offer_name(match_heap, matches, name, limit, match_totals)
    if backfill:
        # Keep both match and backfill heaps bounded. Final admission likewise
        # prioritizes matches, so the limit-plus-one witness cannot displace one.
        remainder = sorted(fallback - matches)
        selected = [*sorted(matches), *remainder[: max(0, limit - len(matches))]]
        return sorted(selected), match_cap or fallback_cap or len(matches) + len(remainder) > limit
    return (sorted(matches), match_cap) if matches else (sorted(fallback), fallback_cap)


def _append_inventory(
    inventory: list[tuple[str, str, list[str]]], candidates: Iterable[tuple[str, str, list[str]]], limit: int
) -> bool:
    """Cap before each normalized tuple is retained; return a cap witness."""
    seen = {(name, namespace) for name, namespace, _ in inventory}
    totals = [0, 0]
    _admit_inventory_text(
        (text for name, namespace, dimensions in inventory for text in (name, namespace, *dimensions)), totals
    )
    for name, namespace, dimensions in candidates:
        if (name, namespace) in seen:
            continue
        if len(inventory) >= limit:
            return True
        _admit_inventory_text((name, namespace, *dimensions), totals)
        inventory.append((name, namespace, dimensions))
        seen.add((name, namespace))
    return False


async def discover_adapter_metrics(
    client: Any,
    datasource: DatasourceInfo,
    keywords: list[str],
    work_budget: SignalResolutionWorkBudget,
    *,
    backend: str = "grafana",
    enrich: bool = True,
) -> MetricCatalogResult:
    """Built-in adapter searches; bounded inventory precedes enrichment."""
    from tacit.runtime_ownership import resolve_remote_runtime_settings

    settings = resolve_remote_runtime_settings(boundary="metric_acquisition", owner=client, provider=backend)
    adapter_deadline = time.monotonic() + settings.adapter_timeout_seconds
    try:
        result = await asyncio.wait_for(
            _discover_adapter_inventory(client, datasource, keywords, work_budget, backend=backend),
            timeout=max(0, adapter_deadline - time.monotonic()),
        )
    except TimeoutError:
        logger.info("metric_inventory_incomplete", reason_code="adapter_timeout")
        return MetricCatalogResult(
            sources=(
                CatalogSourceStatus(
                    backend=backend,
                    datasource_uid=datasource.uid,
                    datasource_type=datasource.type,
                    query_language=_query_language(datasource),
                    terms=("inventory", *canonical_terms(keywords)),
                    disposition="incomplete",
                    reasons=("provider_failed",),
                ),
            )
        )
    if not enrich:
        return result
    enriched = await enrich_adapter_metrics_before_deadline(
        client, datasource, result.as_list(), work_budget, deadline=adapter_deadline, backend=backend
    )
    return MetricCatalogResult(
        entries=enriched.entries, sources=merge_search_sources((*result.sources, *enriched.sources))
    )


async def _discover_adapter_inventory(
    client: Any,
    datasource: DatasourceInfo,
    keywords: list[str],
    work_budget: SignalResolutionWorkBudget,
    *,
    backend: str,
) -> MetricCatalogResult:
    """Shared inventory phase; its admitted caller owns the deadline."""
    ds = datasource
    terms = canonical_terms(keywords)
    tracker = AcquisitionSearches(backend, work_budget, ds)
    entries: dict[tuple[str, str], MetricEntry] = {}
    local_cap = False
    inventory: list[tuple[str, str, list[str]]] = []
    inventory_limit = work_budget.max_acquisition_entries + 1

    async def get(path: str, search_terms: tuple[str, ...], *, cap: int | None = None, terminal: bool = False) -> Any:
        return await tracker.fetch(
            search_terms,
            lambda: client.acquisition_request(
                "GET",
                "/" + path if backend == "signalfx" else f"/api/datasources/proxy/uid/{ds.uid}/{path}",
                work_budget,
            ),
            cap=cap,
            terminal=terminal,
        )

    async def resource(path: str, body: dict[str, Any], search_terms: tuple[str, ...]) -> Any:
        return await tracker.fetch(
            search_terms,
            lambda: client.acquisition_request(
                "POST", f"/api/datasources/uid/{ds.uid}/resources/{path}", work_budget, json=body
            ),
        )

    if _query_language(ds) == "promql":
        data = await get("api/v1/label/__name__/values", ("metric_names", *terms), terminal=True)
        names, name_cap = _select_inventory_names(
            _iter_names(data),
            terms,
            inventory_limit,
            fallback_limit=inventory_limit,
            excluded_suffixes=("_created", "_info"),
            backfill=True,
        )
        local_cap |= name_cap
        inventory = [(name, "", []) for name in names]
        tracker.inventory(names)
    elif _query_language(ds) == "signalflow":
        from tacit.grafana.adapters.signalfx import KEYWORD_METRIC_MAP

        searches = {f"name:*{term}*" for term in terms}
        for term in terms:
            for pattern, prefixes in KEYWORD_METRIC_MAP.items():
                if pattern in term or term in pattern:
                    searches.update(f"name:{prefix}*" for prefix in prefixes)
        if len(searches) > 10:
            local_cap = True
            tracker.skipped = True
            tracker.skip_reasons.add("local_cap")
        for search in sorted(searches or {"*"})[:10]:
            data = await get(f"v2/metric?query={quote(search)}&limit=100", (search,), cap=100)
            names = _names(data)
            tracker.inventory(names)
            if len(inventory) + len(names) > inventory_limit:
                local_cap = True
            local_cap |= _append_inventory(inventory, ((name, "", []) for name in names), inventory_limit)
    elif ds.type == "graphite":
        if len(terms) > 5:
            local_cap = True
            tracker.skipped = True
            tracker.skip_reasons.add("local_cap")
        for pattern in sorted({f"*{term}*" for term in terms} or {"*"})[:5]:
            data = await get(f"metrics/find?query={quote(pattern)}", (pattern,))
            names = _names(data)
            tracker.inventory(names)
            if len(inventory) + len(names) > inventory_limit:
                local_cap = True
            local_cap |= _append_inventory(
                inventory,
                ((name, name.partition(".")[0] if "." in name else "", []) for name in names),
                inventory_limit,
            )
    elif ds.type == "cloudwatch":
        from tacit.grafana.adapters.cloudwatch import _select_namespaces

        region = ds.json_data.get("defaultRegion", "us-east-1")
        namespaces = await resource("namespaces", {"region": region}, ("namespaces", region))
        available = _bounded_names(namespaces) if isinstance(namespaces, dict) else _names(namespaces)
        for namespace in sorted(_select_namespaces(list(terms), available)):
            data = await resource(
                "metrics", {"region": region, "namespace": namespace}, ("metrics", region, namespace, *terms)
            )
            values = data.keys() if isinstance(data, dict) else _iter_names(data)
            selected, name_cap = _select_inventory_names(values, terms, inventory_limit, fallback_limit=20)
            local_cap |= name_cap
            tracker.inventory(f"{namespace}/{name}" for name in selected)
            local_cap |= _append_inventory(
                inventory, ((f"{namespace}/{name}", namespace, []) for name in selected), inventory_limit
            )
    elif ds.type == "influxdb":
        database = ds.json_data.get("database", "")
        data = await get("query?q=SHOW+MEASUREMENTS&db=" + quote(database), ("measurements", *terms))

        normalized_measurement_count = 0

        def measurement_names() -> Iterable[str]:
            nonlocal normalized_measurement_count
            for result in _values(data):
                if isinstance(result, dict):
                    for series in result.get("series", []):
                        if isinstance(series, dict):
                            for value in series.get("values", []):
                                if isinstance(value, list) and value and isinstance(value[0], str):
                                    normalized_measurement_count += 1
                                    yield value[0]

        selected, name_cap = _select_inventory_names(measurement_names(), terms, inventory_limit, fallback_limit=100)
        local_cap |= name_cap
        if tracker.sources and data is not None:
            tracker.sources[-1] = replace(tracker.sources[-1], returned_count=normalized_measurement_count)
        tracker.inventory(selected)
        local_cap |= _append_inventory(inventory, ((name, "", []) for name in selected), inventory_limit)
    elif ds.type in {"elasticsearch", "opensearch"}:
        from tacit.grafana.adapters.elasticsearch import _iter_fields

        index = ds.json_data.get("index", ds.json_data.get("database", "*"))
        data = await get(f"{index}/_mapping", ("mapping", index, *terms))
        returned_fields = 0

        def field_names() -> Iterable[str]:
            nonlocal returned_fields
            for name, _ in _iter_fields(data if isinstance(data, dict) else {}):
                returned_fields += 1
                yield name

        selected, name_cap = _select_inventory_names(field_names(), terms, inventory_limit, fallback_limit=100)
        local_cap |= name_cap
        selected_set = set(selected)
        fields = {
            name: kind for name, kind in _iter_fields(data if isinstance(data, dict) else {}) if name in selected_set
        }
        if tracker.sources and data is not None:
            tracker.sources[-1] = replace(tracker.sources[-1], returned_count=returned_fields)
        tracker.inventory(selected)
        local_cap |= _append_inventory(
            inventory, ((name, index, [kind]) for name, kind in sorted(fields.items())), inventory_limit
        )
    elif ds.type == "loki":
        data = await get("loki/api/v1/labels", ("labels", *terms))
        labels = _names(data)
        interesting = {
            "app",
            "service",
            "namespace",
            "job",
            "container",
            "pod",
            "component",
            "host",
            "level",
            "severity",
            "status_code",
        }
        inventory = [(f"log_stream:{label}", "", []) for label in labels if label in interesting]
        if data is not None:
            tracker.inventory(name for name, _, _ in inventory)

    if len(inventory) > inventory_limit:
        local_cap = True
    inventory = inventory[:inventory_limit]
    for name, namespace, dimensions in sorted(
        set((name, ns, tuple(dims)) for name, ns, dims in inventory),
        key=lambda item: (bool(terms) and not any(term in item[0].lower() for term in terms), item),
    ):
        key = (name, namespace)
        if key in entries:
            continue
        try:
            work_budget.reserve_acquisition_entry((ds.uid, _query_language(ds), name, namespace))
        except AcquisitionWorkLimitError:
            local_cap = True
            tracker.skipped = True
            tracker.skip_reasons.update({"local_cap", "work_limit"})
            continue
        entries[key] = MetricEntry(
            name=name,
            datasource_uid=ds.uid,
            datasource_name=ds.name,
            datasource_type=ds.type,
            query_language=_query_language(ds),
            datasource_is_default=ds.is_default,
            namespace=namespace,
            dimensions=list(dimensions),
        )

    return tracker.finish(list(entries.values()), local_cap=local_cap)


async def enrich_adapter_metrics_before_deadline(
    client: Any,
    ds: DatasourceInfo,
    retained: list[MetricEntry],
    work_budget: SignalResolutionWorkBudget,
    *,
    deadline: float,
    backend: str = "grafana",
) -> MetricCatalogResult:
    """One existing-operation deadline; timeout never erases admitted inventory."""
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        return await asyncio.wait_for(
            enrich_adapter_metrics(client, ds, retained, work_budget, backend=backend), timeout=remaining
        )
    except TimeoutError:
        logger.info("metric_enrichment_incomplete", reason_code="adapter_timeout", retained_count=len(retained))
        return MetricCatalogResult(
            entries=retained,
            sources=(
                CatalogSourceStatus(
                    backend=backend,
                    datasource_uid=ds.uid,
                    datasource_type=ds.type,
                    query_language=_query_language(ds),
                    terms=("metadata", "enrichment"),
                    disposition="incomplete",
                    reasons=("provider_failed",),
                ),
            ),
        )


async def enrich_adapter_metrics(
    client: Any,
    ds: DatasourceInfo,
    retained: list[MetricEntry],
    work_budget: SignalResolutionWorkBudget,
    *,
    backend: str = "grafana",
) -> MetricCatalogResult:
    """Enrich retained identities only after all source inventory admission."""
    tracker = AcquisitionSearches(backend, work_budget, ds)
    entries = {(entry.name, entry.namespace): entry for entry in retained}

    async def get(path: str, terms: tuple[str, ...]) -> Any:
        target = "/" + path if backend == "signalfx" else f"/api/datasources/proxy/uid/{ds.uid}/{path}"
        return await tracker.fetch(terms, lambda: client.acquisition_request("GET", target, work_budget))

    # Metadata is optional. Retained names survive exhausted enrichment budgets.
    if entries and _query_language(ds) == "promql":
        metadata = await get("api/v1/metadata", ("metadata",))
        payload = metadata.get("data", {}) if isinstance(metadata, dict) else {}
        from tacit.catalog import expand_metric_metadata_facts

        metadata_facts: list[MetricEntry] = []
        for entry in entries.values():
            definitions = payload.get(entry.name) if isinstance(payload, dict) else None
            if definitions is None and isinstance(payload, dict):
                for suffix in ("_bucket", "_sum", "_count", "_total"):
                    if entry.name.endswith(suffix) and entry.name[: -len(suffix)] in payload:
                        definitions = payload[entry.name[: -len(suffix)]]
                        break
            definitions = (
                definitions if isinstance(definitions, list) else [definitions] if isinstance(definitions, dict) else []
            )
            metadata_facts.extend(expand_metric_metadata_facts(entry, definitions))
            if len(metadata_facts) > 5000:
                raise ValueError("catalog raw metadata fact limit")
        for entry in list(entries.values())[:50]:
            data = await get(f"api/v1/series?match[]={quote(entry.name)}", ("dimensions", entry.name))
            if data is None:
                if tracker.skipped:
                    break
                continue
            dims: dict[str, set[str]] = {}
            for series in _values(data)[:20]:
                if isinstance(series, dict):
                    for key, value in series.items():
                        if key not in {"__name__", "instance", "job"} and isinstance(value, str):
                            dims.setdefault(key, set()).add(value)
            entry.dimensions = [
                f"{key}={{{','.join(sorted(values)[:10])}}}" for key, values in sorted(dims.items())[:15]
            ]
    elif entries and _query_language(ds) == "signalflow":
        for entry in list(entries.values())[:50]:
            data = await get(
                f"v2/metrictimeseries?query={quote('sf_metric:' + entry.name)}&limit=20", ("dimensions", entry.name)
            )
            if data is None:
                if tracker.skipped:
                    break
                continue
            mts_dims: dict[str, set[str]] = {}
            for mts in _values(data)[:20]:
                if isinstance(mts, dict):
                    for key, value in mts.items():
                        if (
                            not key.startswith("sf_")
                            and key
                            not in {"name", "description", "id", "created", "lastUpdated", "creator", "lastUpdatedBy"}
                            and isinstance(value, str)
                            and value
                        ):
                            mts_dims.setdefault(key, set()).add(value)
            entry.dimensions = [
                f"{key}={{{','.join(sorted(values)[:10])}}}" for key, values in sorted(mts_dims.items())[:15]
            ]
    elif entries and ds.type == "cloudwatch":
        region = ds.json_data.get("defaultRegion", "us-east-1")

        async def dimension_keys(entry: MetricEntry) -> Any:
            return await client.acquisition_request(
                "POST",
                f"/api/datasources/uid/{ds.uid}/resources/dimension-keys",
                work_budget,
                json={
                    "region": region,
                    "namespace": entry.namespace,
                    "metricName": entry.name[len(entry.namespace) + 1 :],
                },
            )

        for entry in entries.values():

            async def fetch_dimensions() -> Any:
                return await dimension_keys(entry)

            data = await tracker.fetch(("dimensions", entry.namespace, entry.name), fetch_dimensions)
            if data is None:
                if tracker.skipped:
                    break
                continue
            from tacit.grafana.adapters.cloudwatch import _coerce_dimension_keys

            entry.dimensions = _coerce_dimension_keys(data)[:128]
    elif entries and ds.type == "loki":
        for entry in list(entries.values())[:6]:
            if not entry.name.startswith("log_stream:"):
                continue
            label = entry.name.partition(":")[2]
            data = await get(f"loki/api/v1/label/{quote(label)}/values", ("dimensions", label))
            if data is None:
                if tracker.skipped:
                    break
                continue
            entry.dimensions = _names(data)[:50]
    output = list(entries.values())
    if entries and _query_language(ds) == "promql":
        output = [
            entry.model_copy(update={"dimensions": entries[(entry.name, entry.namespace)].dimensions})
            for entry in metadata_facts
        ]
    output, _ = retain_acquisition_facts(output, work_budget)
    return tracker.finish(output)
