"""Hermetic contract test for Graphite metric discovery (via Grafana proxy)."""

from __future__ import annotations

import re

import respx
from httpx import Response

from dashforge.grafana.adapters.graphite import GraphiteAdapter
from tests.contracts import factories as f
from tests.integration.conftest import GRAFANA_BASE, datasource, make_grafana_client


@respx.mock
async def test_graphite_discovery_get_contract():
    ds = datasource("graphite-1", "Graphite", "graphite")
    respx.get(url__regex=rf"{re.escape(GRAFANA_BASE)}/api/datasources/proxy/uid/graphite-1/metrics/find.*").mock(
        return_value=Response(200, json=f.graphite_find("servers.web01.cpu", "servers.web01.mem"))
    )

    client = make_grafana_client()
    try:
        entries = await GraphiteAdapter().discover_metrics(client, ds, ["cpu"])
    finally:
        await client.close()

    assert entries, "Graphite find should yield discovery entries"
    assert all(e.query_language == "graphite" for e in entries)
