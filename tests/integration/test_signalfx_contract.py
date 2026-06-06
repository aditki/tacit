"""Hermetic contract tests for the SignalFx (Splunk Observability) v2 REST API.

Reads:  GET /v2/metric
Writes: POST /v2/chart, POST /v2/dashboard
"""

from __future__ import annotations

import json

import respx
from httpx import Response

from dashforge.models.schemas import DashboardSpec, PanelQuery, PanelSpec
from dashforge.signalfx.client import SignalFxClient
from dashforge.signalfx.publisher import _build_chart_json, _build_dashboard_json
from tests.contracts import factories as f
from tests.contracts.signalfx_models import SignalFxChartCreate, SignalFxDashboardCreate

SFX_BASE = "https://api.us1.signalfx.com"


def _client() -> SignalFxClient:
    return SignalFxClient(api_token="test-token", realm="us1")


@respx.mock
async def test_search_metrics_get_contract():
    respx.get(f"{SFX_BASE}/v2/metric").mock(
        return_value=Response(200, json=f.signalfx_metric_search("cpu.utilization", "memory.used"))
    )
    client = _client()
    try:
        result = await client.search_metrics(query="*")
    finally:
        await client.close()
    names = {r["name"] for r in result["results"]}
    assert {"cpu.utilization", "memory.used"} <= names


@respx.mock
async def test_create_chart_post_contract():
    route = respx.post(f"{SFX_BASE}/v2/chart").mock(
        return_value=Response(200, json=f.signalfx_chart_response(chart_id="CHART1", name="CPU"))
    )
    panel = PanelSpec(
        title="CPU",
        description="cpu chart",
        panel_type="timeseries",
        unit="percent",
        queries=[
            PanelQuery(
                expr="data('cpu.utilization').publish(label='A')",
                legend_format="A",
                datasource_uid="signalfx-direct",
                datasource_type="signalfx",
            )
        ],
    )
    chart_json = _build_chart_json(panel)
    client = _client()
    try:
        created = await client.create_chart(chart_json)
    finally:
        await client.close()

    assert created["id"] == "CHART1"
    # DashForge's outgoing chart body satisfies the SignalFx chart contract.
    sent = json.loads(route.calls.last.request.content)
    chart = SignalFxChartCreate.model_validate(sent)
    assert chart.name == "CPU"
    assert "cpu.utilization" in chart.programText


@respx.mock
async def test_create_dashboard_post_contract():
    route = respx.post(f"{SFX_BASE}/v2/dashboard").mock(
        return_value=Response(200, json=f.signalfx_dashboard_response(dash_id="DASH1", name="DashForge"))
    )
    spec = DashboardSpec(
        title="DashForge",
        tags=["sre"],
        timerange="1h",
        panels=[],
    )
    dashboard_json = _build_dashboard_json(spec, ["CHART1"], "GROUP1")
    client = _client()
    try:
        created = await client.create_dashboard(dashboard_json)
    finally:
        await client.close()

    assert created["id"] == "DASH1"
    sent = json.loads(route.calls.last.request.content)
    dashboard = SignalFxDashboardCreate.model_validate(sent)
    assert dashboard.name == "DashForge"
