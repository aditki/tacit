"""Measured validation keeps the existing query, budget and backend owners.

These integration-boundary gates are written before the optional capability.
No live vendor or model is used.
"""

from __future__ import annotations

import asyncio
import json
import threading
from unittest.mock import AsyncMock

import httpx
import pytest

from tacit.backends.grafana import GrafanaBackend
from tacit.backends.signalfx import SignalFxBackend
from tacit.config import Settings
from tacit.grafana.client import GrafanaClient
from tacit.models.schemas import DashboardSpec, EvidenceRequirement, Intent, MetricEntry, PanelQuery, PanelSpec
from tacit.signalfx.client import SignalFxClient
from tacit.signals.resolution import SignalResolutionWorkBudget


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        grafana_url="https://measurement.invalid",
        grafana_api_key="",
        grafana_enabled=True,
        signalfx_enabled=False,
        pipeline_max_concurrent=1,
    )


def _budget(**overrides) -> SignalResolutionWorkBudget:
    return SignalResolutionWorkBudget(100, 10_000, 10_000, **overrides)


def _spec(expr="rate(requests_total[5m])", *, uid="ds-a", kind="prometheus", language="promql"):
    return DashboardSpec(
        title="Recorded values",
        panels=[
            PanelSpec(
                title=expr,
                panel_type="timeseries",
                queries=[PanelQuery(expr=expr, datasource_uid=uid, datasource_type=kind, query_language=language)],
            )
        ],
    )


def _catalog(*names, uid="ds-a"):
    return [
        MetricEntry(
            name=name,
            datasource_uid=uid,
            datasource_name="Recorded",
            datasource_type="prometheus",
            query_language="promql",
            unit="seconds",
        )
        for name in names
    ]


def _payload(value="7.5"):
    return {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {"service": "checkout", "customer_identifier": "must-not-be-retained"},
                    "value": [1_795_000_000, value],
                }
            ],
        },
    }


class _Chunks(httpx.AsyncByteStream):
    def __init__(self, body: bytes):
        self.body = body
        self.closed = False

    async def __aiter__(self):
        yield self.body

    async def aclose(self):
        self.closed = True


def _grafana(handler):
    settings = _settings()
    client = GrafanaClient(base_url=settings.grafana_url, api_key="", runtime_settings=settings)
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url=client.base_url)
    return client, GrafanaBackend(client=client, runtime_settings=settings)


@pytest.mark.asyncio
async def test_numeric_facts_come_from_the_one_existing_bounded_probe():
    requests = []
    stream = _Chunks(json.dumps(_payload()).encode())
    client, backend = _grafana(lambda request: requests.append(request) or httpx.Response(200, stream=stream))
    work = _budget()
    try:
        result = await backend.validate_queries_with_measurements(_spec(), _catalog("requests_total"), work)
        assert len(requests) == work.acquisition_calls == 1
        assert requests[0].url.path == "/api/datasources/proxy/uid/ds-a/api/v1/query"
        assert requests[0].url.params["query"] == "rate(requests_total[5m])"
        assert requests[0].headers["accept-encoding"] == "identity"
        assert stream.closed and work.acquisition_response_bytes > 0
        assert result.spec.panels[0].queries[0].validation_has_data is True
        assert len(result.measurements) == 1
        measurement = result.measurements[0]
        assert measurement.disposition == "captured"
        assert measurement.samples[0].value == 7.5 and measurement.samples[0].timestamp == 1_795_000_000
        assert measurement.unit is None  # Raw metric seconds do not prove the rate's unit.
        assert "must-not-be-retained" not in measurement.model_dump_json()
        assert measurement.samples[0].scope[0].value == "checkout"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_multiple_measured_probes_are_serial_and_do_not_refetch():
    active = maximum = 0
    requests = []

    async def handle(request):
        nonlocal active, maximum
        requests.append(request)
        active += 1
        maximum = max(maximum, active)
        try:
            await asyncio.sleep(0)
            return httpx.Response(200, stream=_Chunks(json.dumps(_payload()).encode()))
        finally:
            active -= 1

    client, backend = _grafana(handle)
    spec = _spec("first_metric")
    spec.panels[0].queries.append(PanelQuery(expr="second_metric", datasource_uid="ds-a", query_language="promql"))
    work = _budget()
    try:
        result = await backend.validate_queries_with_measurements(spec, _catalog("first_metric", "second_metric"), work)
        assert maximum == 1 and len(requests) == work.acquisition_calls == 2
        assert len(result.measurements) == 2 and len(result.spec.panels[0].queries) == 2
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_duplicate_queries_share_one_actual_instant_result():
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, stream=_Chunks(json.dumps(_payload(str(len(requests)))).encode()))

    client, backend = _grafana(handle)
    spec = _spec()
    spec.panels.append(spec.panels[0].model_copy(update={"title": "Same source in another panel"}))
    try:
        result = await backend.validate_queries_with_measurements(spec, _catalog("requests_total"), _budget())
        assert len(requests) == len(result.measurements) == 1
        assert result.measurements[0].samples[0].value == 1
        assert len(result.spec.panels) == 2
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_exhausted_investigation_budget_causes_zero_validation_io():
    requests = []
    client, backend = _grafana(lambda request: requests.append(request) or httpx.Response(200))
    work = _budget(max_acquisition_calls=1)
    work.reserve_acquisition_call()  # Existing discovery already owns the entire allowance.
    try:
        result = await backend.validate_queries_with_measurements(_spec(), _catalog("requests_total"), work)
        assert requests == [] and work.acquisition_calls == 1
        assert result.spec.panels == []
        assert result.measurements[0].disposition == "limited"
        assert result.measurements[0].samples == ()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_unrepresentable_query_identity_fails_before_remote_io():
    requests = []
    client, backend = _grafana(lambda request: requests.append(request) or httpx.Response(200))
    spec = _spec()
    spec.panels.append(_spec(uid="u" * 257).panels[0])
    try:
        with pytest.raises(ValueError):
            await backend.validate_queries_with_measurements(spec, None, _budget())
        assert requests == []
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_measurement_values_and_raw_errors_never_enter_diagnostics(failed):
    from structlog.testing import capture_logs

    secret = "private-measurement-918273645.125"

    def handle(request):
        if failed:
            raise ValueError(secret)
        payload = _payload("918273645.125")
        payload["data"]["result"][0]["metric"]["customer_identifier"] = secret
        return httpx.Response(200, stream=_Chunks(json.dumps(payload).encode()))

    client, backend = _grafana(handle)
    try:
        with capture_logs() as events:
            result = await backend.validate_queries_with_measurements(_spec(), _catalog("requests_total"), _budget())
        rendered = json.dumps(events)
        assert secret not in rendered and "918273645.125" not in rendered
        assert "customer_identifier" not in rendered
        assert result.measurements[0].disposition == ("invalid" if failed else "captured")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_oversized_response_is_stopped_before_json_decode_and_stream_closes(monkeypatch):
    stream = _Chunks(b'{"data":"' + b"x" * 128 + b'"}')
    client, backend = _grafana(lambda request: httpx.Response(200, stream=stream, headers={"Content-Length": "1"}))
    import tacit.acquisition as acquisition

    def forbidden_decode(_body):
        raise AssertionError("The oversized response reached the JSON decoder")

    monkeypatch.setattr(acquisition, "_admit_json_body", forbidden_decode)
    work = _budget(max_acquisition_response_bytes=32)
    try:
        result = await backend.validate_queries_with_measurements(_spec(), _catalog("requests_total"), work)
        assert stream.closed and work.acquisition_response_bytes <= 32
        assert result.measurements[0].disposition == "limited"
        assert result.measurements[0].samples == ()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_cancelled_measured_probe_closes_stream_without_releasing_remote_work_early():
    entered = asyncio.Event()

    class Waiting(_Chunks):
        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b"{}"

    stream = Waiting(b"")
    client, backend = _grafana(lambda request: httpx.Response(200, stream=stream))
    work = _budget()
    task = asyncio.create_task(backend.validate_queries_with_measurements(_spec(), _catalog("requests_total"), work))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert stream.closed and work.acquisition_calls == 1 and work.acquisition_response_bytes > 0
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await client.close()


@pytest.mark.asyncio
async def test_unsupported_grafana_type_cannot_manufacture_numeric_data():
    requests = []
    client, backend = _grafana(lambda request: requests.append(request) or httpx.Response(200))
    try:
        result = await backend.validate_queries_with_measurements(
            _spec("logs", kind="loki", language="logql"), None, _budget()
        )
        assert requests == []
        assert result.measurements[0].disposition == "unsupported" and result.measurements[0].samples == ()
        assert result.spec.panels[0].queries[0].validation_status == "skipped"
        assert not result.spec.panels[0].queries[0].validation_has_data
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_signalfx_measured_capability_remains_metadata_only(monkeypatch):
    settings = _settings().model_copy(update={"signalfx_enabled": True})
    client = SignalFxClient(api_token="", runtime_settings=settings)
    client.get_metric = AsyncMock(return_value={"name": "requests"})
    execute = AsyncMock(side_effect=AssertionError("SignalFx numeric execution is outside this increment"))
    monkeypatch.setattr(client, "execute", execute, raising=False)
    backend = SignalFxBackend(client=client, runtime_settings=settings)
    try:
        result = await backend.validate_queries_with_measurements(
            _spec("data('requests').publish()", uid="", kind="signalfx", language="signalflow"), None, _budget()
        )
        client.get_metric.assert_awaited_once_with("requests")
        execute.assert_not_called()
        assert result.measurements[0].disposition == "unsupported" and result.measurements[0].samples == ()
        assert result.spec.panels[0].queries[0].validation_status == "exists"
        assert result.spec.panels[0].queries[0].validation_has_data is False
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_spoofed_generated_query_fields_do_not_become_verified_telemetry():
    spoofed = _spec().model_dump(mode="json")
    spoofed["panels"][0]["queries"][0]["measurement"] = {"disposition": "captured", "samples": [{"value": 999}]}
    spoofed["query_measurements"] = [{"samples": [{"value": 999}]}]
    spec = DashboardSpec.model_validate(spoofed)
    stream = _Chunks(json.dumps(_payload("2")).encode())
    client, backend = _grafana(lambda request: httpx.Response(200, stream=stream))
    try:
        result = await backend.validate_queries_with_measurements(spec, _catalog("requests_total"), _budget())
        assert result.measurements[0].samples[0].value == 2
        assert [sample.value for record in result.measurements for sample in record.samples] == [2]
        assert "measurement" not in result.spec.panels[0].queries[0].model_dump()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_legacy_backend_helper_reports_absent_numeric_capture_without_extra_call():
    from tacit.validation import validate_queries_with_measurements

    class Legacy:
        name = "legacy"
        calls = 0

        async def validate_queries(self, spec, catalog=None):
            self.calls += 1
            return spec, ["Legacy result"]

    backend = Legacy()
    spec = _spec()
    result = await validate_queries_with_measurements(backend, spec, _catalog("requests_total"), _budget())
    assert backend.calls == 1 and result.warnings == ["Legacy result"]
    assert result.spec == spec
    assert result.measurements[0].disposition == "not_captured" and result.measurements[0].samples == ()


@pytest.mark.asyncio
async def test_main_symptom_and_gap_paths_share_the_same_budget(monkeypatch):
    from tacit.backends.base import MeasuredValidationResult
    from tacit.measurements import query_identity, unavailable_measurement
    from tacit.pipeline import validation

    work = _budget()
    calls = []

    class Measured:
        name = "measured"

        async def validate_queries_with_measurements(self, spec, catalog, work_budget):
            calls.append(work_budget)
            stamped = spec.model_copy(deep=True)
            for panel in stamped.panels:
                for query in panel.queries:
                    query.validation_status, query.validation_has_data = "ok", True
            return MeasuredValidationResult(
                stamped,
                [],
                tuple(unavailable_measurement(q, "legacy_validation") for p in stamped.panels for q in p.queries),
            )

        async def validate_queries(self, *_args):
            raise AssertionError("The legacy path bypassed measured validation")

    requirement = EvidenceRequirement(id="required", evidence_type="required_metric", default_metric="main_metric")
    monkeypatch.setattr(validation, "missing_critical_symptom_requirements", lambda *args: [requirement])
    monkeypatch.setattr(validation, "missing_critical_evidence_gap_requirements", lambda *args: [requirement])
    monkeypatch.setattr(
        validation, "build_symptom_evidence_dashboard", lambda *args, **kwargs: (_spec("symptom_metric"), [])
    )
    monkeypatch.setattr(validation, "build_evidence_gap_dashboard", lambda *args, **kwargs: (_spec("gap_metric"), []))
    result = await validation.validate_dashboard_and_evidence(
        primary=Measured(),
        dashboard_spec=_spec("main_metric"),
        catalog=_catalog("main_metric", "symptom_metric", "gap_metric"),
        evidence_requirements=[requirement],
        evidence_resolutions=[],
        intent=Intent(summary="Inspect", domain="web", problem_type="general"),
        target_language="promql",
        ranked_archetypes_present=True,
        record_stage=lambda *args, **kwargs: None,
        signal_resolution_work_budget=work,
    )
    assert calls == [work, work, work]
    assert {measurement.query_fingerprint for measurement in result.query_measurements} == {
        query_identity(_spec(name).panels[0].queries[0]) for name in ("main_metric", "symptom_metric", "gap_metric")
    }


@pytest.mark.asyncio
async def test_owned_proxy_legacy_fallback_runs_on_existing_owner_loop(tmp_path):
    from tacit.validation import validate_queries_with_measurements
    from tests.unit import test_backend_runtime_ownership as ownership

    dependencies, products, _ = ownership._dependencies(tmp_path)
    root = dependencies.start_runtime_root()
    lease = None
    calls = []
    requester = threading.get_ident()
    try:
        lease = await dependencies.realize_backends()

        async def legacy(spec, catalog=None):
            calls.append(threading.get_ident())
            return spec, []

        products[0].validate_queries = legacy
        result = await validate_queries_with_measurements(lease.backends[0], _spec(), None, _budget())
        assert len(calls) == 1 and calls[0] != requester
        assert result.measurements[0].disposition == "not_captured"
        assert result.measurements[0].samples == ()
    finally:
        if lease is not None:
            await dependencies.close_backends(lease)
        await ownership._stop_root(dependencies, root)
