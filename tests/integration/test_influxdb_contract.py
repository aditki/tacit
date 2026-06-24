"""Hermetic contract test for InfluxDB measurement discovery (via Grafana proxy)."""

from __future__ import annotations

import re

import respx
from httpx import Response

from tacit.grafana.adapters.influxdb import InfluxDBAdapter
from tests.contracts import factories as f
from tests.integration.conftest import GRAFANA_BASE, datasource, make_grafana_client


@respx.mock
async def test_influxdb_discovery_get_contract():
    ds = datasource("influx-1", "InfluxDB", "influxdb", database="telegraf")
    respx.get(url__regex=rf"{re.escape(GRAFANA_BASE)}/api/datasources/proxy/uid/influx-1/query.*").mock(
        return_value=Response(200, json=f.influx_measurements("cpu", "mem", "disk"))
    )

    client = make_grafana_client()
    try:
        entries = await InfluxDBAdapter().discover_metrics(client, ds, ["cpu"])
    finally:
        await client.close()

    assert entries, "InfluxDB SHOW MEASUREMENTS should yield discovery entries"
    assert all(e.query_language in ("influxql", "flux") for e in entries)
