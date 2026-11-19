"""Acquisition work belongs to the existing admitted runtime operation."""

import httpx
import pytest

from tacit.grafana.client import GrafanaClient
from tacit.signalfx.client import SignalFxClient
from tacit.signals.resolution import SignalResolutionWorkBudget


def budget(**kwargs):
    return SignalResolutionWorkBudget(100, 10000, 10000, **kwargs)


def test_acquisition_limits_are_independent_and_nonblocking():
    work = budget(max_acquisition_calls=2, max_acquisition_response_bytes=8, max_acquisition_entries=2)
    work.reserve_acquisition_call()
    work.reserve_acquisition_call()
    with pytest.raises(RuntimeError, match="work_limit"):
        work.reserve_acquisition_call()
    work.begin_call()
    work.reserve_acquisition_bytes(8)
    with pytest.raises(RuntimeError, match="work_limit"):
        work.reserve_acquisition_bytes(1)
    work.reserve_acquisition_entry("same")
    work.reserve_acquisition_entry("same")
    work.reserve_acquisition_entry("other")
    with pytest.raises(RuntimeError, match="work_limit"):
        work.reserve_acquisition_entry("third")
    assert work.counters()["acquisition_entry_count"] == 2
    assert work._lock.acquire(blocking=False)
    try:
        with pytest.raises(RuntimeError, match="work_limit"):
            work.reserve_acquisition_call()
    finally:
        work._lock.release()


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
@pytest.mark.parametrize("client_class", [GrafanaClient, SignalFxClient])
async def test_bounded_identity_response_ignores_content_length_and_closes(client_class):
    stream = Chunks([b'{"data":', b'"too large"}'])
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, stream=stream, headers={"Content-Length": "1"})

    client = client_class()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="https://test.invalid")
    work = budget(max_acquisition_response_bytes=8)
    try:
        with pytest.raises(RuntimeError, match="work_limit"):
            await client.acquisition_request("GET", "/metrics", work)
        assert stream.closed
        assert requests[0].headers["accept-encoding"] == "identity"
        assert work.counters()["acquisition_response_bytes"] <= 8
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br"])
async def test_encoded_and_deep_catalogs_are_rejected_and_closed(encoding):
    stream = Chunks([b"{}"])
    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(200, stream=stream, headers={"Content-Encoding": encoding})
        ),
        base_url="https://test.invalid",
    )
    try:
        with pytest.raises(ValueError, match="encoding"):
            await client.acquisition_request("GET", "/metrics", budget())
        assert stream.closed
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_call_limit_rejects_before_dispatch():
    calls = []
    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: calls.append(r) or httpx.Response(200, stream=Chunks([b"{}"]))),
        base_url="https://test.invalid",
    )
    work = budget(max_acquisition_calls=1)
    try:
        await client.acquisition_request("GET", "/metrics", work)
        with pytest.raises(RuntimeError, match="work_limit"):
            await client.acquisition_request("GET", "/metrics", work)
        assert len(calls) == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_typed_prometheus_retains_5000_and_bypasses_nested_caches():
    import json
    from unittest.mock import patch

    from tacit.backends.grafana import GrafanaBackend
    from tacit.models.schemas import Intent

    paths = []

    def handle(request):
        paths.append(request.url.path)
        if request.url.path == "/api/datasources":
            data = [{"uid": "prom", "name": "Prom", "type": "prometheus"}]
        elif request.url.path.endswith("/__name__/values"):
            data = {"data": [f"metric_{i:05}" for i in range(5001)]}
        elif request.url.path.endswith("/metadata"):
            data = {"data": {"metric_00000": [{"unit": "seconds", "type": "counter"}]}}
        else:
            data = {"data": []}
        return httpx.Response(200, stream=Chunks([json.dumps(data).encode()]))

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    backend = GrafanaBackend(client=client)
    try:
        with patch("tacit.cache.metric_cache.get", side_effect=AssertionError("typed discovery touched legacy cache")):
            result = await backend.discover_metrics_with_metadata(
                [], Intent(summary="test", domain="application"), budget()
            )
        assert len(result.entries) == 5000
        assert result.entries[0].unit == ""
        assert any("local_cap" in source.reasons for source in result.sources)
        assert len(paths) == 2
        assert sum(source.summary for source in result.sources) <= 1
        inventory = next(
            source for source in result.sources if source.datasource_uid == "prom" and "metric_names" in source.terms
        )
        assert inventory.returned_count == 5001
        assert inventory.retained_count == 5000
        assert inventory.disposition == "incomplete"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_partial_search_failures_keep_names_and_explicit_status():
    import json

    from tacit.acquisition import discover_adapter_metrics
    from tacit.models.schemas import DatasourceInfo

    calls = []

    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(503, stream=Chunks([b"{}"]))
        return httpx.Response(
            200, stream=Chunks([json.dumps({"count": 1, "results": [{"name": "http_count"}]}).encode()])
        )

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    try:
        result = await discover_adapter_metrics(
            client, DatasourceInfo(uid="sfx", name="SignalFx", type="signalfx"), ["http"], budget()
        )
        assert [entry.name for entry in result.entries] == ["http_count"]
        assert any(source.disposition == "unavailable" for source in result.sources)
        assert any(source.disposition == "incomplete" for source in result.sources)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body", [b"[" * 33 + b"0" + b"]" * 33, b'{"name":"' + b"a" * 65537 + b'"}', b"[" + b"0," * 100000 + b"0]"]
)
async def test_deep_scalar_and_many_small_value_payloads_fail_closed(body):
    client = GrafanaClient()
    await client.close()
    stream = Chunks([body])
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=stream)), base_url=client.base_url
    )
    try:
        with pytest.raises((ValueError, RuntimeError)):
            await client.acquisition_request("GET", "/metrics", budget())
        assert stream.closed
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_all_source_inventories_precede_enrichment():
    import json

    from tacit.backends.grafana import GrafanaBackend
    from tacit.models.schemas import Intent

    paths = []

    def handle(request):
        paths.append(request.url.path)
        if request.url.path == "/api/datasources":
            data = [{"uid": uid, "name": uid, "type": "prometheus"} for uid in ("b", "a")]
        elif request.url.path.endswith("/__name__/values"):
            data = {"data": ["http_count"]}
        else:
            data = {"data": []}
        return httpx.Response(200, stream=Chunks([json.dumps(data).encode()]))

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    try:
        result = await GrafanaBackend(client=client).discover_metrics_with_metadata(
            [], Intent(summary="test", domain="application"), budget(max_acquisition_calls=3)
        )
        assert {entry.datasource_uid for entry in result.entries} == {"a", "b"}
        assert [path.split("/uid/")[-1].split("/")[0] for path in paths[1:]] == ["a", "b"]
        assert len(paths) == 3
    finally:
        await client.close()


def test_effective_acquisition_configuration_rejects_unmeasured_concurrency_and_mutation():
    from tacit.config import Settings, validate_acquisition_runtime, validate_acquisition_settings

    with pytest.raises(ValueError, match="acquisition"):
        validate_acquisition_runtime(
            Settings(_env_file=None, pipeline_max_concurrent=1000, max_acquisition_response_bytes=1)
        )
    configured = Settings(_env_file=None).model_copy(update={"max_acquisition_calls": 33})
    with pytest.raises(ValueError, match="max_acquisition_calls"):
        validate_acquisition_settings(configured)


def test_isolated_adversarial_allocation_envelope():
    """Retain five raw/catalog/index/capture graphs in an isolated child."""
    import json
    import subprocess
    import sys
    from pathlib import Path

    script = r"""
import gc, json, resource, tracemalloc
from tacit.acquisition import _admit_json_body, _select_inventory_names
from tacit.grafana.adapters.elasticsearch import _iter_fields
from tacit.catalog import (
    MetricCatalogResult,
    QueryTargetCatalog,
    CatalogSourceStatus,
    acquisition_entry_id,
    build_acquisition_capture,
    canonical_acquisition_bytes,
)
from tacit.models.schemas import MetricEntry, DashRequest, Intent, DashboardSpec, CulpritRanking, ContextChunk
from tacit.pipeline.completion import prepare_acquisition_completion
from tacit.investigation_contract import InvestigationContractAssembler

tracemalloc.start()
peaks = {}
for name, raw in (
    ("many_small", b"[" + b"0," * 4_000_000 + b"0]"),
    ("deep", b"[" * 33 + b"0" + b"]" * 33),
    ("large_scalar", b'{"v":"' + b"x" * 8_000_000 + b'"}'),
):
    tracemalloc.reset_peak()
    try:
        _admit_json_body(bytearray(raw))
    except (ValueError, RuntimeError):
        pass
    else:
        raise AssertionError(name + " admitted")
    peaks[name] = tracemalloc.get_traced_memory()[1]
prefix = "😀" * 20000
mapping = {
    "index": {
        "mappings": {"properties": {prefix: {"properties": {f"leaf_{i}": {"type": "double"} for i in range(200)}}}}
    }
}
tracemalloc.reset_peak()
try:
    _select_inventory_names((name for name, _ in _iter_fields(mapping)), (), 5001)
except RuntimeError:
    pass
else:
    raise AssertionError("derived prefix admitted")
peaks["derived_prefix"] = tracemalloc.get_traced_memory()[1]
del mapping, prefix


def text_tree_counts(value):
    from pydantic import BaseModel

    nodes = characters = 0
    pending = [value]
    while pending:
        item = pending.pop()
        nodes += 1
        if isinstance(item, str):
            characters += len(item)
        elif isinstance(item, BaseModel):
            pending.extend(getattr(item, name) for name in type(item).model_fields)
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend(item)
    return nodes, characters


retained = []
tracemalloc.reset_peak()
for pipeline in range(5):
    raw = bytearray(json.dumps({"data": ["metric_%04d" % i for i in range(5000)]}).encode())
    decoded = _admit_json_body(raw)
    entries = [
        MetricEntry(
            name=name,
            datasource_uid="prom",
            datasource_name="Prom",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["label_%d=value" % i for i in range(11)],
        )
        for name in decoded["data"]
    ]
    source = CatalogSourceStatus(
        backend="grafana",
        datasource_uid="prom",
        datasource_type="prometheus",
        query_language="promql",
        terms=("metric_names",),
        returned_count=5000,
        retained_count=5000,
        total_before_cap=5000,
        disposition="complete",
        reasons=("authoritative_terminal",),
        retained_entry_ids=tuple(acquisition_entry_id(entry) for entry in entries),
    )
    result = MetricCatalogResult(entries=entries, sources=(source,))
    ownership = QueryTargetCatalog(result)
    record = build_acquisition_capture(result)
    encoded = canonical_acquisition_bytes(record)
    contexts = [ContextChunk(source="runbook", content="x" * 15500, metadata={"tree": [0] * 1500}) for _ in range(128)]
    inputs = dict(
        investigation_id="allocation_%d" % pipeline,
        revision=1,
        parent_revision=None,
        request=DashRequest(prompt="Inspect telemetry", user_id="allocation"),
        intent=Intent(summary="allocation", domain="application"),
        dashboard_spec=DashboardSpec(title="Allocation", panels=[]),
        evidence_requirements=[],
        evidence_resolutions=[],
        evidence_observations=[],
        culprit_ranking=CulpritRanking(),
        context_chunks=contexts,
        warnings=[],
    )
    record_nodes, record_characters = text_tree_counts(record)
    assembly_nodes, assembly_characters = text_tree_counts(inputs)
    assert 180000 < record_nodes <= 200000
    assert 1800000 < record_characters <= 2000000
    assert 190000 < assembly_nodes <= 200000
    assert 1980000 < assembly_characters <= 2000000
    prepared = prepare_acquisition_completion(assembly_inputs=inputs, acquisition_record=record)
    preflight_contract = InvestigationContractAssembler().from_pipeline(
        **prepared.assembly_inputs, dashboard_url="", dashboard_uid="", signalfx_url="", signalfx_dashboard_id=""
    )
    retained.append((raw, decoded, entries, result, ownership, record, encoded, inputs, prepared, preflight_contract))
peaks["five_pipeline_graphs"] = tracemalloc.get_traced_memory()[1]
peaks["record_nodes"] = record_nodes
peaks["record_characters"] = record_characters
peaks["assembly_nodes"] = assembly_nodes
peaks["assembly_characters"] = assembly_characters
peaks["rss_high_water_native_units"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(json.dumps(peaks))
assert peaks["five_pipeline_graphs"] < 5 * 512 * 1024 * 1024

"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=True,
        # This child measures a fixed allocation workload, not a latency SLO.
        # Tracemalloc is materially slower on shared Linux CI than native macOS.
        timeout=180,
    )
    measurements = json.loads(completed.stdout.strip().splitlines()[-1])
    assert measurements["five_pipeline_graphs"] > 0
    print("acquisition allocation high-water:", json.dumps(measurements, sort_keys=True))


@pytest.mark.asyncio
async def test_default_attempt_ceiling_rejects_request_33():
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, stream=Chunks([b"{}"]))

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    try:
        work = budget()
        for _ in range(32):
            await client.acquisition_request("GET", "/metrics", work)
        with pytest.raises(RuntimeError, match="work_limit"):
            await client.acquisition_request("GET", "/metrics", work)
        assert len(requests) == 32
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cancellation_closes_admitted_raw_response_without_refunding_work():
    import asyncio

    started = asyncio.Event()

    class Waiting(Chunks):
        async def __aiter__(self):
            started.set()
            await asyncio.Event().wait()
            yield b"{}"

    stream = Waiting([])
    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=stream)), base_url=client.base_url
    )
    work = budget()
    task = asyncio.create_task(client.acquisition_request("GET", "/metrics", work))
    try:
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed
        assert work.acquisition_calls == 1
        assert work.acquisition_response_bytes > 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["bytes", "entries", "exact_bytes"])
async def test_acquisition_stop_rejects_next_outbound_call_before_dispatch(stop):
    requests = []
    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: requests.append(request) or httpx.Response(200, stream=Chunks([b"{}"]))
        ),
        base_url=client.base_url,
    )
    work = budget(max_acquisition_response_bytes=8, max_acquisition_entries=1)
    if stop == "entries":
        work.reserve_acquisition_entry("one")
        with pytest.raises(RuntimeError, match="work_limit"):
            work.reserve_acquisition_entry("two")
    else:
        work.reserve_acquisition_bytes(8)
        if stop == "bytes":
            with pytest.raises(RuntimeError, match="work_limit"):
                work.reserve_acquisition_bytes(1)
    try:
        with pytest.raises(RuntimeError, match="work_limit"):
            await client.acquisition_request("GET", "/metrics", work)
        assert requests == []
        work.begin_call()
        assert work.calls == 1
    finally:
        await client.close()


def test_legacy_acquisition_preserves_metadata_conflicts_and_charges_identity_once():
    from tacit.acquisition import legacy_acquisition_catalog
    from tacit.catalog import select_query_target
    from tacit.models.schemas import MetricEntry

    first = MetricEntry(
        name="duration",
        datasource_uid="prom",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
        unit="seconds",
    )
    second = first.model_copy(update={"unit": "bytes"})
    work = budget()
    result = legacy_acquisition_catalog(
        [first, second, first.model_copy()], backend="custom", terms=(), work_budget=work
    )
    assert work.acquisition_entries == 1
    assert len(result.entries) == 2
    assert result.sources[0].returned_count == 3
    assert result.sources[0].retained_count == 1
    assert select_query_target(result, ["duration"], "promql").status == "ambiguous"


@pytest.mark.asyncio
async def test_adapter_aggregation_preserves_conflicts_and_legacy_summary_reasons(monkeypatch):
    from tacit.catalog import CatalogSourceStatus, MetricCatalogResult, select_query_target
    from tacit.grafana.datasource import discover_all_metrics_with_metadata
    from tacit.models.schemas import DatasourceInfo, MetricEntry

    first = MetricEntry(
        name="duration",
        datasource_uid="prom",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
        unit="seconds",
    )

    class Adapter:
        query_language = "promql"

        async def discover_metrics_with_metadata(self, *args):
            return MetricCatalogResult(
                entries=[first, first.model_copy(update={"unit": "bytes"})],
                sources=[CatalogSourceStatus(backend="grafana", summary=True, reasons=("legacy_unknown",))],
            )

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda ds: Adapter())
    client = GrafanaClient()
    work = budget()
    try:
        result = await discover_all_metrics_with_metadata(
            client, [DatasourceInfo(uid="prom", name="Prom", type="prometheus")], [], work
        )
        assert work.acquisition_entries == 1
        assert len(result.entries) == 2
        assert select_query_target(result, ["duration"], "promql").status == "ambiguous"
        assert result.sources[0].reasons == ("legacy_unknown",)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True])
async def test_prometheus_metadata_conflicts_survive_typed_acquisition(reverse):
    import json

    from tacit.backends.grafana import GrafanaBackend
    from tacit.catalog import build_acquisition_capture, select_query_target
    from tacit.models.schemas import Intent

    definitions = [{"unit": "seconds", "type": "counter"}, {"unit": "bytes", "type": "gauge"}]
    if reverse:
        definitions.reverse()

    def handle(request):
        if request.url.path == "/api/datasources":
            data = [{"uid": "prom", "name": "Prom", "type": "prometheus"}]
        elif request.url.path.endswith("/__name__/values"):
            data = {"data": ["duration"]}
        elif request.url.path.endswith("/metadata"):
            data = {"data": {"duration": definitions}}
        else:
            data = {"data": []}
        return httpx.Response(200, stream=Chunks([json.dumps(data).encode()]))

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    try:
        result = await GrafanaBackend(client=client).discover_metrics_with_metadata(
            [], Intent(summary="test", domain="application"), budget()
        )
        assert select_query_target(result, ["duration"], "promql").status == "ambiguous"
        record = build_acquisition_capture(result)
        assert {(conflict["field"], tuple(conflict["values"])) for conflict in record["conflicts"]} == {
            ("unit", ("bytes", "seconds")),
            ("metric_type", ("counter", "gauge")),
        }
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider,expected",
    [
        ("prometheus", 2),
        ("cloudwatch", 2),
        ("graphite", 2),
        ("influxdb", 2),
        ("elasticsearch", 2),
        ("loki", 2),
        ("signalfx", 2),
    ],
)
async def test_every_builtin_typed_adapter_reports_actual_search_counts(provider, expected):
    import json

    from tacit.acquisition import discover_adapter_metrics
    from tacit.models.schemas import DatasourceInfo

    requests = []

    def handle(request):
        requests.append(request)
        path = request.url.path
        if provider == "prometheus":
            data = (
                {"data": ["cpu", "memory"]}
                if path.endswith("/__name__/values")
                else {"data": {}} if path.endswith("/metadata") else {"data": []}
            )
        elif provider == "cloudwatch":
            data = (
                ["AWS/EC2"]
                if path.endswith("/namespaces")
                else ["CPUUtilization", "MemoryUtilization"] if path.endswith("/metrics") else ["InstanceId"]
            )
        elif provider == "graphite":
            data = [{"id": "host.cpu"}, {"id": "host.memory"}]
        elif provider == "influxdb":
            data = {"results": [{"series": [{"values": [["cpu"], ["memory"]]}]}]}
        elif provider == "elasticsearch":
            data = {"index": {"mappings": {"properties": {"cpu": {"type": "double"}, "memory": {"type": "long"}}}}}
        elif provider == "loki":
            data = {"data": ["app", "service"]} if path.endswith("/labels") else {"data": ["api"]}
        else:
            data = (
                {"count": 2, "results": [{"name": "cpu"}, {"name": "memory"}]}
                if path.endswith("/metric")
                else {"results": []}
            )
        return httpx.Response(200, stream=Chunks([json.dumps(data).encode()]))

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    try:
        result = await discover_adapter_metrics(
            client, DatasourceInfo(uid="source", name=provider, type=provider), [], budget()
        )
        assert len(result.entries) == expected
        inventory = [source for source in result.sources if source.retained_count]
        assert sum(source.retained_count for source in inventory) >= expected
        assert all(source.retained_count <= source.returned_count for source in result.sources)
        assert all(
            source.disposition == ("complete" if provider == "prometheus" and source.retained_count else "incomplete")
            for source in result.sources
        )
        assert all(request.headers["accept-encoding"] == "identity" for request in requests)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_legacy_adapter_task_creation_is_bounded_by_remaining_search_work(monkeypatch):
    from tacit.grafana.datasource import discover_all_metrics_with_metadata
    from tacit.models.schemas import DatasourceInfo

    invoked = []

    class Adapter:
        query_language = "promql"

        async def discover_metrics(self, client, datasource, keywords):
            invoked.append(datasource.uid)
            return []

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda ds: Adapter())
    client = GrafanaClient()
    try:
        result = await discover_all_metrics_with_metadata(
            client,
            [DatasourceInfo(uid=f"ds_{i:04}", name="Prom", type="custom") for i in range(5000)],
            [],
            budget(max_acquisition_calls=3),
        )
        assert len(invoked) <= 3
        assert len(result.sources) == 1
        assert set(result.sources[0].reasons) == {"legacy_unknown", "work_limit"}
    finally:
        await client.close()


@pytest.mark.parametrize("reverse", [False, True])
def test_full_entry_budget_still_retains_late_conflicts_for_admitted_identities(reverse):
    from tacit.acquisition import retain_acquisition_facts
    from tacit.catalog import select_query_target
    from tacit.models.schemas import MetricEntry

    first = MetricEntry(
        name="a",
        datasource_uid="prom",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
        metric_type="counter",
        unit="seconds",
    )
    unrelated = first.model_copy(update={"name": "b", "metric_type": "", "unit": ""})
    conflicting = first.model_copy(update={"unit": "bytes"})
    work = budget(max_acquisition_entries=1)
    previous, _ = retain_acquisition_facts([first], work)
    incoming = [unrelated, conflicting]
    if reverse:
        incoming.reverse()
    retained, limited = retain_acquisition_facts(incoming, work, previous=previous)
    assert limited
    assert work.acquisition_entries == 1
    assert {entry.name for entry in retained} == {"a"}
    assert select_query_target(retained, ["a"], "promql").status == "ambiguous"


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["prometheus", "cloudwatch", "influxdb", "elasticsearch"])
async def test_native_inventory_filters_before_bounded_retention(provider):
    import json

    from tacit.acquisition import discover_adapter_metrics
    from tacit.models.schemas import DatasourceInfo

    irrelevant = [f"a_{i:04}_created" if provider == "prometheus" else f"a_{i:04}" for i in range(5001)]
    names = [*irrelevant, "z_required_metric"]

    def handle(request):
        if provider == "prometheus":
            data = {"data": names}
        elif provider == "cloudwatch":
            data = ["AWS/EC2"] if request.url.path.endswith("/namespaces") else names
        elif provider == "influxdb":
            data = {"results": [{"series": [{"values": [[name] for name in names]}]}]}
        else:
            data = {"index": {"mappings": {"properties": {name: {"type": "double"} for name in names}}}}
        return httpx.Response(200, stream=Chunks([json.dumps(data).encode()]))

    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url=client.base_url)
    try:
        result = await discover_adapter_metrics(
            client,
            DatasourceInfo(uid="source", name=provider, type=provider),
            ["required"] if provider != "prometheus" else [],
            budget(),
            enrich=False,
        )
        assert any(entry.name.endswith("z_required_metric") for entry in result.entries)
        assert len(result.entries) == 1
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["prometheus", "mimir", "cortex", "thanos"])
@pytest.mark.parametrize("reverse", [False, True])
async def test_prometheus_keyword_matches_survive_ordinary_name_cap_with_backfill(provider, reverse):
    import json

    from tacit.acquisition import discover_adapter_metrics
    from tacit.models.schemas import DatasourceInfo

    names = [*[f"a_unrelated_{i:04}" for i in range(5001)], "z_required_requests_total"]
    if reverse:
        names.reverse()
    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Chunks([json.dumps({"data": names}).encode()]))
        ),
        base_url=client.base_url,
    )
    try:
        result = await discover_adapter_metrics(
            client,
            DatasourceInfo(uid="source", name=provider, type=provider),
            ["required"],
            budget(),
            enrich=False,
        )
        retained = {entry.name for entry in result.entries}
        assert "z_required_requests_total" in retained
        assert len(retained) == 5000
        inventory = next(source for source in result.sources if "metric_names" in source.terms)
        assert inventory.returned_count == 5002 and inventory.retained_count == 5000
        assert inventory.disposition == "incomplete" and "local_cap" in inventory.reasons
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_prometheus_matching_does_not_discard_below_cap_backfill():
    import json

    from tacit.acquisition import discover_adapter_metrics
    from tacit.models.schemas import DatasourceInfo

    names = ["a_other", "z_required", "b_other"]
    client = GrafanaClient()
    await client.close()
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=Chunks([json.dumps({"data": names}).encode()]))
        ),
        base_url=client.base_url,
    )
    try:
        result = await discover_adapter_metrics(
            client, DatasourceInfo(uid="source", name="Prom", type="prometheus"), ["required"], budget(), enrich=False
        )
        assert {entry.name for entry in result.entries} == set(names)
        assert result.sources[0].disposition == "complete"
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["prometheus", "signalfx", "cloudwatch", "loki"])
async def test_phased_adapter_deadline_covers_inventory_and_enrichment(provider, monkeypatch):
    import asyncio

    from tacit.catalog import CatalogSourceStatus, MetricCatalogResult
    from tacit.config import Settings
    from tacit.grafana.datasource import discover_all_metrics_with_metadata
    from tacit.models.schemas import DatasourceInfo, MetricEntry

    enrichment_settled = asyncio.Event()
    entry = MetricEntry(
        name="requested_metric",
        datasource_uid="source",
        datasource_name="Source",
        datasource_type=provider,
        query_language="promql",
    )

    class Adapter:
        supports_acquisition_inventory_phase = True
        query_language = "promql"

        async def discover_metrics_with_metadata(self, client, datasource, keywords, work_budget, *, enrich):
            assert enrich is False
            await asyncio.sleep(0.2)
            return MetricCatalogResult(
                entries=[entry],
                sources=[
                    CatalogSourceStatus(
                        backend="grafana", datasource_uid="source", datasource_type=provider, query_language="promql"
                    )
                ],
            )

    async def enrich(client, datasource, retained, work_budget, *, backend="grafana", _progress=None):
        try:
            await asyncio.sleep(0.9)
            return MetricCatalogResult(entries=retained)
        finally:
            enrichment_settled.set()

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda datasource: Adapter())
    monkeypatch.setattr("tacit.acquisition.enrich_adapter_metrics", enrich)
    settings = Settings(_env_file=None, adapter_timeout_seconds=1)
    client = GrafanaClient(runtime_settings=settings)
    try:
        result = await asyncio.wait_for(
            discover_all_metrics_with_metadata(
                client,
                [DatasourceInfo(uid="source", name="Source", type=provider)],
                ["requested"],
                budget(),
                runtime_settings=settings,
            ),
            timeout=3,
        )
        assert enrichment_settled.is_set()
        assert [item.name for item in result.entries] == ["requested_metric"]
        assert any(
            source.datasource_uid == "source"
            and source.disposition == "incomplete"
            and "enrichment" in source.terms
            and "provider_failed" in source.reasons
            for source in result.sources
        )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_phased_enrichment_cancellation_settles_and_is_not_partial_success(monkeypatch):
    import asyncio

    from tacit.catalog import MetricCatalogResult
    from tacit.grafana.datasource import discover_all_metrics_with_metadata
    from tacit.models.schemas import DatasourceInfo, MetricEntry

    started, settled = asyncio.Event(), asyncio.Event()
    entry = MetricEntry(
        name="requested",
        datasource_uid="source",
        datasource_name="Source",
        datasource_type="prometheus",
        query_language="promql",
    )

    class Adapter:
        supports_acquisition_inventory_phase = True
        query_language = "promql"

        async def discover_metrics_with_metadata(self, *args, **kwargs):
            return MetricCatalogResult(entries=[entry])

    async def enrich(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda datasource: Adapter())
    monkeypatch.setattr("tacit.acquisition.enrich_adapter_metrics", enrich)
    client = GrafanaClient()
    task = asyncio.create_task(
        discover_all_metrics_with_metadata(
            client, [DatasourceInfo(uid="source", name="Source", type="prometheus")], [], budget()
        )
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert settled.is_set()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()


@pytest.mark.asyncio
async def test_direct_signalfx_shares_inventory_enrichment_deadline_and_retains_names():
    import asyncio
    import json

    from tacit.config import Settings
    from tacit.signalfx.discovery import discover_metrics_with_metadata

    settled = asyncio.Event()
    paths = []

    async def handle(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/metric"):
            await asyncio.sleep(0.2)
            body = {"count": 1, "results": [{"name": "requested_metric"}]}
        else:
            try:
                await asyncio.sleep(0.9)
                body = {"results": []}
            finally:
                settled.set()
        return httpx.Response(200, stream=Chunks([json.dumps(body).encode()]))

    settings = Settings(_env_file=None, adapter_timeout_seconds=1)
    client = SignalFxClient(runtime_settings=settings)
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="https://test.invalid")
    try:
        result = await asyncio.wait_for(discover_metrics_with_metadata(client, ["requested"], budget()), timeout=3)
        assert settled.is_set()
        assert [entry.name for entry in result.entries] == ["requested_metric"]
        assert len(paths) == 2
        assert any(
            source.backend == "signalfx"
            and source.disposition == "incomplete"
            and "enrichment" in source.terms
            and "provider_failed" in source.reasons
            for source in result.sources
        )
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        b'{"data":["requests",NaN]}',
        b'{"data":[Infinity]}',
        b'{"data":[-Infinity]}',
        b'{"data":[1e9999]}',
        b'{"data":["requests"],"data":[]}',
    ],
)
async def test_strict_raw_json_rejects_nonfinite_and_duplicate_facts(body):
    client = GrafanaClient()
    await client.close()
    stream = Chunks([body])
    client._http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=stream)), base_url=client.base_url
    )
    try:
        with pytest.raises(ValueError):
            await client.acquisition_request("GET", "/metrics", budget())
        assert stream.closed
    finally:
        await client.close()


def test_derived_inventory_text_is_admitted_before_heap_or_tuple_retention():
    from tacit.acquisition import AcquisitionSearches, _append_inventory, _select_inventory_names
    from tacit.grafana.adapters.elasticsearch import _iter_fields
    from tacit.models.schemas import DatasourceInfo

    prefix = "😀" * 20000
    mapping = {
        "index": {
            "mappings": {"properties": {prefix: {"properties": {f"leaf_{i}": {"type": "double"} for i in range(200)}}}}
        }
    }
    with pytest.raises(RuntimeError, match="work_limit"):
        _select_inventory_names((name for name, _ in _iter_fields(mapping)), (), 5001)
    with pytest.raises(RuntimeError, match="work_limit"):
        _append_inventory([], ((f"{prefix}/leaf_{i}", prefix, []) for i in range(200)), 5001)
    tracker = AcquisitionSearches("grafana", budget(), DatasourceInfo(uid="cw", name="CW", type="cloudwatch"))
    tracker.sources.append(tracker.status(("metrics",)))
    with pytest.raises(RuntimeError, match="work_limit"):
        tracker.inventory(f"{prefix}/leaf_{i}" for i in range(200))


def test_retained_metadata_conflicts_survive_new_identity_lock_contention():
    from tacit.acquisition import retain_acquisition_facts
    from tacit.catalog import select_query_target
    from tacit.models.schemas import MetricEntry

    first = MetricEntry(
        name="a",
        datasource_uid="prom",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
        metric_type="counter",
        unit="seconds",
    )
    work = budget()
    previous, _ = retain_acquisition_facts([first], work)
    work._lock.acquire()
    try:
        retained, limited = retain_acquisition_facts(
            [
                first.model_copy(update={"name": "b", "metric_type": "", "unit": ""}),
                first.model_copy(update={"unit": "bytes"}),
            ],
            work,
            previous=previous,
        )
        assert limited
        assert select_query_target(retained, ["a"], "promql").status == "ambiguous"
    finally:
        work._lock.release()
