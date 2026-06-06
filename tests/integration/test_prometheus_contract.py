"""Hermetic contract test for Prometheus metric discovery (via Grafana proxy)."""

from __future__ import annotations

import re

import respx
from httpx import Response

from dashforge.grafana.adapters.prometheus import PrometheusAdapter
from tests.contracts import factories as f
from tests.integration.conftest import GRAFANA_BASE, datasource, make_grafana_client, proxy_url


@respx.mock
async def test_prometheus_discovery_get_contract():
    ds = datasource("prom-1", "Prometheus", "prometheus")
    respx.get(proxy_url("prom-1", "api/v1/label/__name__/values")).mock(
        return_value=Response(200, json=f.prometheus_label_values("http_requests_total", "node_cpu_seconds_total"))
    )
    # Per-metric series lookups (any match[]) — return a representative label set.
    respx.get(url__regex=rf"{re.escape(GRAFANA_BASE)}/api/datasources/proxy/uid/prom-1/api/v1/series.*").mock(
        return_value=Response(200, json=f.prometheus_series({"__name__": "http_requests_total", "job": "api"}))
    )

    client = make_grafana_client()
    try:
        entries = await PrometheusAdapter().discover_metrics(client, ds, ["http", "requests"])
    finally:
        await client.close()

    names = {e.name for e in entries}
    assert "http_requests_total" in names
    assert all(e.query_language == "promql" for e in entries)
