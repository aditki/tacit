"""Hermetic contract tests for the Grafana HTTP API.

Reads:  GET /api/datasources, GET /api/dashboards/uid/{uid}
Writes: POST /api/folders, POST /api/dashboards/db
"""

from __future__ import annotations

import json

import respx
from httpx import Response

from dashforge.grafana.dashboard import publish_dashboard
from dashforge.grafana.datasource import list_datasources
from dashforge.models.schemas import DashboardSpec, PanelQuery, PanelSpec
from tests.contracts import factories as f
from tests.contracts.grafana_models import GrafanaDashboardSaveCommand
from tests.integration.conftest import GRAFANA_BASE, make_grafana_client


@respx.mock
async def test_list_datasources_get_contract():
    respx.get(f"{GRAFANA_BASE}/api/datasources").mock(
        return_value=Response(
            200,
            json=f.grafana_datasources(
                f.grafana_datasource("prom-1", "Prometheus", "prometheus", isDefault=True),
                f.grafana_datasource("loki-1", "Loki", "loki"),
            ),
        )
    )
    client = make_grafana_client()
    try:
        result = await list_datasources(client)
    finally:
        await client.close()

    by_type = {d.type: d for d in result}
    assert by_type["prometheus"].uid == "prom-1"
    assert by_type["prometheus"].is_default is True
    assert by_type["loki"].name == "Loki"


@respx.mock
async def test_ingest_dashboard_get_contract():
    from dashforge.backends.grafana import GrafanaBackend

    panels = [
        {
            "type": "timeseries",
            "title": "Latency",
            "datasource": {"type": "prometheus"},
            "targets": [{"expr": "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))"}],
        }
    ]
    respx.get(f"{GRAFANA_BASE}/api/dashboards/uid/dash-1").mock(
        return_value=Response(200, json=f.grafana_dashboard_envelope(uid="dash-1", title="Svc", panels=panels))
    )
    client = make_grafana_client()
    backend = GrafanaBackend(client=client)
    try:
        features = await backend.ingest_dashboard("dash-1")
    finally:
        await client.close()

    assert features.dashboard_uid == "dash-1"
    assert features.panel_count == 1
    assert "http_request_duration_seconds_bucket" in features.metrics_found


@respx.mock
async def test_publish_dashboard_post_contract():
    """DashForge's POST body must satisfy the Grafana save-command contract."""
    respx.get(f"{GRAFANA_BASE}/api/folders").mock(return_value=Response(200, json=[]))
    respx.post(f"{GRAFANA_BASE}/api/folders").mock(return_value=Response(200, json=f.grafana_folder()))
    save_route = respx.post(f"{GRAFANA_BASE}/api/dashboards/db").mock(
        return_value=Response(200, json=f.grafana_save_response(uid="abc123"))
    )

    spec = DashboardSpec(
        title="Checkout Health",
        tags=["sre"],
        timerange="1h",
        panels=[
            PanelSpec(
                title="Error rate",
                description="",
                panel_type="timeseries",
                unit="percent",
                queries=[
                    PanelQuery(
                        expr='rate(http_requests_total{status=~"5.."}[5m])',
                        legend_format="errors",
                        datasource_uid="prom-1",
                        datasource_type="prometheus",
                    )
                ],
            )
        ],
    )

    client = make_grafana_client()
    try:
        url, uid = await publish_dashboard(client, spec)
    finally:
        await client.close()

    assert uid == "abc123"
    # The outgoing POST body conforms to the Grafana save-command schema.
    sent = json.loads(save_route.calls.last.request.content)
    command = GrafanaDashboardSaveCommand.model_validate(sent)
    assert command.overwrite is True
    assert command.dashboard.title == "Checkout Health"
    assert command.dashboard.panels, "dashboard must carry panels"
