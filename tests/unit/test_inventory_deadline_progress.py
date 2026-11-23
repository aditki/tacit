"""Completed inventory survives its own deadline without changing caller cancellation."""

import asyncio
import json

import httpx
import pytest

from tacit.acquisition import discover_adapter_metrics
from tacit.catalog import MetricCatalogResult, acquisition_entry_id, select_query_target
from tacit.config import Settings
from tacit.grafana.adapters.registry import get_adapter
from tacit.grafana.client import GrafanaClient
from tacit.grafana.datasource import discover_all_metrics_with_metadata
from tacit.models.schemas import DatasourceInfo, MetricEntry
from tacit.signalfx.client import SignalFxClient
from tacit.signalfx.discovery import discover_metrics_with_metadata
from tacit.signals.resolution import SignalResolutionWorkBudget


class JsonBody(httpx.AsyncByteStream):
    def __init__(self, payload):
        self.payload = payload

    async def __aiter__(self):
        yield json.dumps(self.payload).encode()


class PendingBody(httpx.AsyncByteStream):
    def __init__(self):
        self.started = asyncio.Event()
        self.settled = asyncio.Event()
        self.closed = False

    async def __aiter__(self):
        self.started.set()
        try:
            yield b'{"results":['
            await asyncio.Event().wait()
        finally:
            self.settled.set()

    async def aclose(self):
        self.closed = True


def response(payload):
    return httpx.Response(200, stream=JsonBody(payload))


def budget(**updates):
    return SignalResolutionWorkBudget(256, 10_000_000, 100_000, **updates)


async def client_for(handle, *, direct=False):
    settings = Settings(_env_file=None, adapter_timeout_seconds=1)
    client = SignalFxClient(runtime_settings=settings) if direct else GrafanaClient(runtime_settings=settings)
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="https://fixture.invalid")
    return client, settings


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["direct", "signalfx", "graphite", "cloudwatch"])
async def test_own_inventory_deadline_preserves_completed_search_and_settles_pending_body(provider):
    pending = PendingBody()
    paths = []
    if provider == "cloudwatch":
        expected_name, language = "AWS/EC2/CPUUtilization", "cloudwatch"
    elif provider == "graphite":
        expected_name, language = "casealpha.metric", "graphite"
    else:
        expected_name, language = "casealpha.metric", "signalflow"

    def handle(request):
        paths.append((request.url.path, dict(request.url.params), request.content))
        if request.url.path.endswith("/namespaces"):
            return response(["AWS/EC2", "AWS/ELB", "AWS/Lambda"])
        is_inventory = request.url.path.endswith(("/metric", "/find", "/metrics"))
        assert is_inventory, "enrichment or later I/O ran after the inventory deadline"
        if provider == "cloudwatch":
            first = json.loads(request.content)["namespace"] == "AWS/EC2"
            payload = ["CPUUtilization"]
        else:
            first = "casealpha" in request.url.params["query"]
            payload = (
                [{"text": expected_name}]
                if provider == "graphite"
                else {"count": 1, "results": [{"name": expected_name}]}
            )
        return response(payload) if first else httpx.Response(200, stream=pending)

    client, _settings = await client_for(handle, direct=provider == "direct")
    work = budget()
    datasource = DatasourceInfo(uid="owner", name="Source", type="signalfx" if provider == "direct" else provider)
    try:
        if provider == "direct":
            result = await discover_metrics_with_metadata(client, ["casealpha", "casezulu", "zzlater"], work)
            expected_uid = "signalfx-direct"
        else:
            adapter = get_adapter(datasource)
            assert adapter is not None
            keywords = [] if provider == "cloudwatch" else ["casealpha", "casezulu", "zzlater"]
            result = await adapter.discover_metrics_with_metadata(client, datasource, keywords, work)
            expected_uid = "owner"

        assert [entry.name for entry in result.entries] == [expected_name]
        assert pending.started.is_set() and pending.settled.is_set() and pending.closed
        assert work.acquisition_entries == 1
        assert work.acquisition_calls == len(paths) == (3 if provider == "cloudwatch" else 2)
        assert 0 < work.acquisition_response_bytes <= work.max_acquisition_response_bytes
        selected = select_query_target(result, [expected_name], language)
        assert selected.target.datasource_uid == expected_uid
        completed = [source for source in result.sources if source.retained_count]
        assert len(completed) == 1
        assert completed[0].returned_count == completed[0].retained_count == 1
        assert completed[0].retained_entry_ids == (acquisition_entry_id(result.entries[0]),)
        unfinished = [source for source in result.sources if "provider_failed" in source.reasons]
        assert len(unfinished) == 1 and unfinished[0].disposition == "incomplete"
        assert unfinished[0].returned_count == unfinished[0].retained_count == 0
        assert ("AWS/ELB" if provider == "cloudwatch" else "casezulu") in " ".join(unfinished[0].terms)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("later_match", [False, True])
async def test_timeout_final_retention_keeps_keyword_priority_before_single_identity_reservation(later_match):
    pending = PendingBody()
    calls = []

    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            names = ["a_noise"] if later_match else ["a_noise", "z_required"]
            return response({"count": len(names), "results": [{"name": name} for name in names]})
        if later_match and len(calls) == 2:
            return response({"count": 1, "results": [{"name": "z_required"}]})
        return httpx.Response(200, stream=pending)

    client, _ = await client_for(handle, direct=True)
    work = budget(max_acquisition_entries=1)
    try:
        result = await discover_metrics_with_metadata(client, ["casealpha", "required", "timeout"], work)
        assert [entry.name for entry in result.entries] == ["z_required"]
        assert work.acquisition_entries == 1 and work.acquisition_calls == len(calls) == (3 if later_match else 2)
        assert pending.settled.is_set() and pending.closed
        assert any(source.summary and "work_limit" in source.reasons for source in result.sources)
        assert any(source.retained_count == 1 and "local_cap" in source.reasons for source in result.sources)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_first_search_timeout_has_no_synthetic_entries_or_complete_source():
    pending = PendingBody()
    client, _ = await client_for(lambda _request: httpx.Response(200, stream=pending), direct=True)
    work = budget()
    try:
        result = await discover_metrics_with_metadata(client, ["casealpha", "casezulu"], work)
        assert result.entries == ()
        assert pending.settled.is_set() and pending.closed
        assert work.acquisition_calls == 1 and work.acquisition_entries == 0
        assert len(result.sources) == 1
        assert result.sources[0].terms == ("name:*casealpha*",)
        assert result.sources[0].disposition == "incomplete"
        assert result.sources[0].retained_count == result.sources[0].returned_count == 0
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("coordinator", [False, True])
async def test_caller_cancellation_after_completed_search_does_not_return_partial_success(coordinator):
    pending = PendingBody()
    calls = []

    def handle(request):
        calls.append(request)
        return response([{"text": "casealpha.metric"}]) if len(calls) == 1 else httpx.Response(200, stream=pending)

    client, settings = await client_for(handle)
    datasource = DatasourceInfo(uid="g", name="Graphite", type="graphite")
    work = budget()
    operation = (
        discover_all_metrics_with_metadata(
            client, [datasource], ["casealpha", "casezulu"], work, runtime_settings=settings
        )
        if coordinator
        else discover_adapter_metrics(client, datasource, ["casealpha", "casezulu"], work)
    )
    task = asyncio.create_task(operation)
    try:
        await asyncio.wait_for(pending.started.wait(), timeout=2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert pending.settled.is_set() and pending.closed
        assert work.acquisition_calls == len(calls) == 2
        assert work.acquisition_entries == 0
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await client.close()


@pytest.mark.asyncio
async def test_coordinator_outer_deadline_and_external_phased_signature_remain_unchanged(monkeypatch):
    settled = asyncio.Event()

    class ExternalAdapter:
        supports_acquisition_inventory_phase = True
        query_language = "graphite"

        async def discover_metrics_with_metadata(self, client, datasource, keywords, work_budget, *, enrich):
            assert enrich is False
            retained = MetricCatalogResult(
                [
                    MetricEntry(
                        name="known",
                        datasource_uid="g",
                        datasource_name="Graphite",
                        datasource_type="graphite",
                        query_language="graphite",
                    )
                ]
            )
            try:
                await asyncio.Event().wait()
            finally:
                settled.set()
            return retained

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda _ds: ExternalAdapter())
    client, settings = await client_for(lambda _request: pytest.fail("external fixture made unexpected HTTP"))
    try:
        result = await discover_all_metrics_with_metadata(
            client, [DatasourceInfo(uid="g", name="Graphite", type="graphite")], [], budget(), runtime_settings=settings
        )
        assert result.entries == () and settled.is_set()
        assert any(source.disposition == "unavailable" for source in result.sources)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider", ["prometheus", "signalfx", "graphite", "cloudwatch", "influxdb", "elasticsearch", "loki"]
)
async def test_each_builtin_inventory_still_finishes_with_one_admitted_identity(provider):
    payloads = {
        "prometheus": {"data": ["requested"]},
        "signalfx": {"count": 1, "results": [{"name": "requested"}]},
        "graphite": [{"text": "requested.metric"}],
        "cloudwatch": ["CPUUtilization"],
        "influxdb": {"results": [{"series": [{"values": [["requested"]]}]}]},
        "elasticsearch": {"logs": {"mappings": {"properties": {"requested": {"type": "double"}}}}},
        "loki": {"data": ["app"]},
    }
    expected = {"graphite": "requested.metric", "cloudwatch": "AWS/EC2/CPUUtilization", "loki": "log_stream:app"}.get(
        provider, "requested"
    )

    def handle(request):
        return response(["AWS/EC2"] if request.url.path.endswith("/namespaces") else payloads[provider])

    client, _ = await client_for(handle)
    work = budget()
    datasource = DatasourceInfo(uid="owner", name="Source", type=provider)
    try:
        adapter = get_adapter(datasource)
        result = await adapter.discover_metrics_with_metadata(client, datasource, [], work, enrich=False)
        assert [entry.name for entry in result.entries] == [expected]
        assert work.acquisition_entries == 1
        assert sum(source.retained_count for source in result.sources) == 1
        assert all(source.retained_count <= source.returned_count for source in result.sources)
    finally:
        await client.close()
