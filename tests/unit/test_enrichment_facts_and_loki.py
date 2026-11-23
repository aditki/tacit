"""Completed enrichment and bounded native fallbacks survive partial acquisition."""

import asyncio
import json
import time

import httpx
import pytest

from tacit.acquisition import enrich_adapter_metrics, enrich_adapter_metrics_before_deadline, retain_acquisition_facts
from tacit.agents.metrics_discovery import _build_user_prompt
from tacit.catalog import MetricCatalogResult, acquisition_entry_id, select_query_target
from tacit.config import Settings
from tacit.grafana.adapters.loki import LokiAdapter
from tacit.grafana.client import GrafanaClient
from tacit.grafana.datasource import discover_all_metrics_with_metadata
from tacit.models.schemas import DatasourceInfo, Intent, MetricEntry
from tacit.signals.resolution import SignalResolutionWorkBudget


class JsonBody(httpx.AsyncByteStream):
    def __init__(self, payload):
        self.payload = payload

    async def __aiter__(self):
        yield json.dumps(self.payload).encode()


def response(payload, status=200):
    return httpx.Response(status, stream=JsonBody(payload))


def budget(limit=5000):
    return SignalResolutionWorkBudget(256, 10_000_000, 100_000, max_acquisition_entries=limit)


async def client_for(handle, settings=None):
    client = GrafanaClient(runtime_settings=settings or Settings(_env_file=None))
    await client.close()
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handle), base_url="https://fixture.invalid")
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["prometheus", "mimir", "cortex", "thanos"])
@pytest.mark.parametrize("entrypoint", ["standalone", "coordinator"])
async def test_completed_metadata_conflicts_and_dimensions_survive_later_timeout(provider, entrypoint):
    paths = []
    settled = asyncio.Event()
    names = ["a_requests", "z_errors", "zz_later"]

    async def handle(request):
        paths.append((request.url.path, request.url.params.get("match[]")))
        if request.url.path.endswith("/label/__name__/values"):
            data = {"data": names}
        elif request.url.path.endswith("/metadata"):
            data = {
                "data": {
                    "a_requests": [{"unit": "seconds", "type": "counter"}, {"unit": "bytes", "type": "counter"}],
                    "z_errors": [{"unit": "count", "type": "counter"}],
                }
            }
        elif request.url.params.get("match[]") == "a_requests":
            data = {"data": [{"__name__": "a_requests", "service": "checkout"}]}
        else:
            try:
                await asyncio.Event().wait()
            finally:
                settled.set()
        return response(data)

    settings = Settings(_env_file=None, adapter_timeout_seconds=1)
    client = await client_for(handle, settings)
    work = budget(3)
    datasource = DatasourceInfo(uid="owner", name="Source", type=provider)
    try:
        if entrypoint == "standalone":
            retained, _ = retain_acquisition_facts(
                [
                    MetricEntry(
                        name=name,
                        datasource_uid=datasource.uid,
                        datasource_name=datasource.name,
                        datasource_type=provider,
                        query_language="promql",
                    )
                    for name in names
                ],
                work,
            )
            result = await enrich_adapter_metrics_before_deadline(
                client, datasource, retained, work, deadline=time.monotonic() + 0.05
            )
        else:
            result = await discover_all_metrics_with_metadata(client, [datasource], [], work, runtime_settings=settings)
        requests = [entry for entry in result.entries if entry.name == "a_requests"]
        assert {entry.unit for entry in requests} == {"seconds", "bytes"}
        assert all(entry.dimensions == ["service={checkout}"] for entry in requests)
        assert select_query_target(result, ["a_requests"], "promql").reason == "metadata_conflict"
        assert settled.is_set()
        assert work.acquisition_entries == 3
        assert not any(metric == "zz_later" for _, metric in paths)
        assert any(source.terms == ("metadata",) for source in result.sources)
        assert any(source.terms == tuple(sorted(("dimensions", "a_requests"))) for source in result.sources)
        assert any(
            "enrichment" in source.terms and source.disposition == "incomplete" and "provider_failed" in source.reasons
            for source in result.sources
        )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_caller_cancellation_after_metadata_remains_cancellation_and_settles():
    started, settled = asyncio.Event(), asyncio.Event()

    async def handle(request):
        if request.url.path.endswith("/metadata"):
            return response({"data": {"requests": [{"unit": "seconds"}, {"unit": "bytes"}]}})
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            settled.set()

    client = await client_for(handle)
    retained = [
        MetricEntry(
            name="requests",
            datasource_uid="p",
            datasource_name="Prom",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]
    task = asyncio.create_task(
        enrich_adapter_metrics_before_deadline(
            client,
            DatasourceInfo(uid="p", name="Prom", type="prometheus"),
            retained,
            budget(),
            deadline=time.monotonic() + 10,
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
@pytest.mark.parametrize("entrypoint", ["adapter", "coordinator"])
async def test_loki_custom_labels_reach_freeform_catalog_with_exact_source_membership(entrypoint):
    labels = ["service_name", "deployment_environment"]
    paths = []

    def handle(request):
        paths.append(request.url.path)
        return response({"data": labels})

    client = await client_for(handle)
    work = budget(1)
    datasource = DatasourceInfo(uid="loki-owner", name="Logs", type="loki")
    try:
        if entrypoint == "adapter":
            result = await LokiAdapter().discover_metrics_with_metadata(client, datasource, ["checkout"], work)
        else:
            result = await discover_all_metrics_with_metadata(client, [datasource], ["checkout"], work)
        assert [entry.name for entry in result.entries] == ["loki:available_labels"]
        entry = result.entries[0]
        assert entry.dimensions == sorted(labels)
        assert (entry.datasource_uid, entry.datasource_type, entry.query_language) == ("loki-owner", "loki", "logql")
        assert work.acquisition_entries == 1
        assert paths == ["/api/datasources/proxy/uid/loki-owner/loki/api/v1/labels"]
        assert result.sources[0].retained_entry_ids == (acquisition_entry_id(entry),)
        assert result.sources[0].retained_count == 1
        assert "local_cap" not in result.sources[0].reasons
        prompt = _build_user_prompt(Intent(summary="checkout errors", domain="application"), result.as_list())
        assert "loki:available_labels" in prompt and all(label in prompt for label in labels)
        assert select_query_target(result, [entry.name], "logql").status == "selected"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_loki_available_label_limit_is_bounded_and_reported_incomplete():
    labels = [f"custom_{index:03}" for index in range(129)]
    client = await client_for(lambda request: response({"data": list(reversed(labels))}))
    try:
        result = await LokiAdapter().discover_metrics_with_metadata(
            client, DatasourceInfo(uid="l", name="Logs", type="loki"), [], budget()
        )
        assert result.entries[0].dimensions == labels[:128]
        assert result.sources[0].returned_count == 129
        assert result.sources[0].disposition == "incomplete"
        assert "local_cap" in result.sources[0].reasons
        assert result.sources[0].retained_entry_ids == (acquisition_entry_id(result.entries[0]),)
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload,status", [({"data": []}, 200), ({"status": "error"}, 500)])
async def test_loki_failed_or_empty_labels_do_not_invent_available_catalog(payload, status):
    client = await client_for(lambda request: response(payload, status))
    try:
        result = await LokiAdapter().discover_metrics_with_metadata(
            client, DatasourceInfo(uid="l", name="Logs", type="loki"), [], budget()
        )
        assert result.entries == ()
        assert all(source.retained_count == 0 for source in result.sources)
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_loki_fallback_does_not_displace_six_log_stream_dimension_probes():
    labels = ["app", "component", "container", "host", "job", "namespace", "service_name"]
    probed = []

    def handle(request):
        if request.url.path.endswith("/labels"):
            return response({"data": labels})
        probed.append(request.url.path.split("/")[-2])
        return response({"data": ["checkout"]})

    client = await client_for(handle)
    try:
        result = await LokiAdapter().discover_metrics_with_metadata(
            client, DatasourceInfo(uid="l", name="Logs", type="loki"), ["available"], budget()
        )
        assert {entry.name for entry in result.entries} == {
            "loki:available_labels",
            *(f"log_stream:{label}" for label in labels[:-1]),
        }
        assert probed == labels[:-1]
        assert next(entry for entry in result.entries if entry.name == "loki:available_labels").dimensions == sorted(
            labels
        )
        assert result.sources[0].returned_count == result.sources[0].retained_count == 7
        assert set(result.sources[0].retained_entry_ids) == {acquisition_entry_id(entry) for entry in result.entries}
    finally:
        await client.close()


def conflict_facts():
    first = MetricEntry(
        name="requests",
        datasource_uid="p",
        datasource_name="Prom",
        datasource_type="prometheus",
        query_language="promql",
        unit="seconds",
    )
    return [first, first.model_copy(update={"unit": "bytes"})]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "provider"])
async def test_incoming_metadata_conflicts_survive_unsuccessful_metadata_fetch(failure):
    async def handle(request):
        if failure == "timeout":
            await asyncio.Event().wait()
        return response({"status": "error"}, 500)

    client = await client_for(handle)
    work = budget(1)
    retained, _ = retain_acquisition_facts(conflict_facts(), work)
    try:
        result = await enrich_adapter_metrics_before_deadline(
            client,
            DatasourceInfo(uid="p", name="Prom", type="prometheus"),
            retained,
            work,
            deadline=time.monotonic() + 0.05,
        )
        assert {entry.unit for entry in result.entries} == {"seconds", "bytes"}
        assert select_query_target(result, ["requests"], "promql").reason == "metadata_conflict"
        assert work.acquisition_entries == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_raw_direct_enrichment_still_reserves_identity_capacity_before_output():
    work = budget(1)
    retained = [
        MetricEntry(
            name=name,
            datasource_uid="g",
            datasource_name="Graphite",
            datasource_type="graphite",
            query_language="graphite",
        )
        for name in ("servers.cpu", "servers.memory")
    ]
    result = await enrich_adapter_metrics(
        None, DatasourceInfo(uid="g", name="Graphite", type="graphite"), retained, work
    )
    assert len(result.entries) == 1
    assert work.acquisition_entries == 1
    assert any(source.summary and "work_limit" in source.reasons for source in result.sources)


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_already_admitted_conflicts_survive_contention_before_enrichment(expired):
    work = budget(1)
    retained, _ = retain_acquisition_facts(conflict_facts(), work)
    client = await client_for(lambda request: pytest.fail("HTTP after budget contention"))
    assert work._lock.acquire(blocking=False)
    try:
        result = await enrich_adapter_metrics_before_deadline(
            client,
            DatasourceInfo(uid="p", name="Prom", type="prometheus"),
            retained,
            work,
            deadline=time.monotonic() + (-1 if expired else 1),
            _retained_admitted=True,
        )
        assert {entry.unit for entry in result.entries} == {"seconds", "bytes"}
        assert select_query_target(result, ["requests"], "promql").reason == "metadata_conflict"
        assert work.acquisition_entries == 1 and work.acquisition_calls == 0
    finally:
        work._lock.release()
        await client.close()


@pytest.mark.asyncio
async def test_legacy_coordinator_keeps_all_initial_conflicting_facts_without_updates(monkeypatch):
    class Adapter:
        query_language = "promql"

        async def discover_metrics(self, *args):
            return conflict_facts()

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda datasource: Adapter())
    client = await client_for(lambda request: pytest.fail("legacy discovery unexpectedly contacted HTTP"))
    try:
        work = budget(1)
        result = await discover_all_metrics_with_metadata(
            client, [DatasourceInfo(uid="p", name="Prom", type="prometheus")], [], work
        )
        assert {entry.unit for entry in result.entries} == {"seconds", "bytes"}
        assert select_query_target(result, ["requests"], "promql").reason == "metadata_conflict"
        assert work.acquisition_entries == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_coordinator_rejects_enrichment_that_introduces_new_identity(monkeypatch):
    class Adapter:
        supports_acquisition_inventory_phase = True
        query_language = "promql"

        async def discover_metrics_with_metadata(self, *args, **kwargs):
            return MetricCatalogResult(conflict_facts()[:1])

    async def foreign_enrichment(*args, **kwargs):
        return MetricCatalogResult([conflict_facts()[0].model_copy(update={"name": "foreign_requests"})])

    monkeypatch.setattr("tacit.grafana.datasource.get_adapter", lambda datasource: Adapter())
    monkeypatch.setattr("tacit.acquisition.enrich_adapter_metrics_before_deadline", foreign_enrichment)
    client = await client_for(lambda request: pytest.fail("fake adapter unexpectedly contacted HTTP"))
    try:
        with pytest.raises(ValueError, match="enrichment.*identity"):
            await discover_all_metrics_with_metadata(
                client, [DatasourceInfo(uid="p", name="Prom", type="prometheus")], [], budget(1)
            )
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_actual_coordinator_keeps_completed_conflicts_after_timeout_with_budget_contention():
    work = budget(1)
    settled = asyncio.Event()
    paths = []

    async def handle(request):
        paths.append(request.url.path)
        if request.url.path.endswith("/label/__name__/values"):
            return response({"data": ["requests"]})
        if request.url.path.endswith("/metadata"):
            return response({"data": {"requests": [{"unit": "seconds"}, {"unit": "bytes"}]}})
        try:
            await asyncio.Event().wait()
        finally:
            assert work._lock.acquire(blocking=False)
            settled.set()

    settings = Settings(_env_file=None, adapter_timeout_seconds=1)
    client = await client_for(handle, settings)
    try:
        result = await discover_all_metrics_with_metadata(
            client,
            [DatasourceInfo(uid="p", name="Prom", type="prometheus")],
            [],
            work,
            runtime_settings=settings,
        )
        assert settled.is_set()
        assert {entry.unit for entry in result.entries} == {"seconds", "bytes"}
        assert select_query_target(result, ["requests"], "promql").reason == "metadata_conflict"
        assert len(paths) == work.acquisition_calls == 3
        assert work.acquisition_entries == 1
    finally:
        if settled.is_set():
            work._lock.release()
        await client.close()


@pytest.mark.asyncio
async def test_loki_filters_recognized_and_requested_labels_before_bounded_retention():
    labels = [*(f"a_noise_{index:04}" for index in range(5002)), "service", "z_required_custom"]
    probed = []

    def handle(request):
        if request.url.path.endswith("/labels"):
            return response({"data": labels})
        probed.append(request.url.path.split("/")[-2])
        return response({"data": ["checkout"]})

    client = await client_for(handle)
    try:
        result = await LokiAdapter().discover_metrics_with_metadata(
            client, DatasourceInfo(uid="l", name="Logs", type="loki"), ["z_required_custom"], budget()
        )
        assert {entry.name for entry in result.entries} == {"log_stream:service", "loki:available_labels"}
        fallback = next(entry for entry in result.entries if entry.name == "loki:available_labels")
        assert len(fallback.dimensions) == 128 and "z_required_custom" in fallback.dimensions
        assert probed == ["service"]
        source = result.sources[0]
        assert source.returned_count == 5004 and source.retained_count == 2
        assert "local_cap" in source.reasons and source.disposition == "incomplete"
    finally:
        await client.close()
