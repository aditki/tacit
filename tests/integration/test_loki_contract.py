"""Hermetic contract test for Loki label discovery (via Grafana proxy)."""

from __future__ import annotations

import respx
from httpx import Response

from dashforge.grafana.adapters.loki import LokiAdapter
from tests.contracts import factories as f
from tests.integration.conftest import datasource, make_grafana_client, proxy_url


@respx.mock
async def test_loki_discovery_get_contract():
    ds = datasource("loki-1", "Loki", "loki")
    respx.get(proxy_url("loki-1", "loki/api/v1/labels")).mock(
        return_value=Response(200, json=f.loki_labels("app", "namespace", "level"))
    )
    client = make_grafana_client()
    try:
        entries = await LokiAdapter().discover_metrics(client, ds, ["app"])
    finally:
        await client.close()

    assert entries, "Loki labels should yield discovery entries"
    assert all(e.query_language == "logql" for e in entries)
