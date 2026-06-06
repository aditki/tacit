"""Hermetic contract test for CloudWatch discovery (Grafana resource API)."""

from __future__ import annotations

import respx
from httpx import Response

from dashforge.grafana.adapters.cloudwatch import CloudWatchAdapter
from tests.contracts import factories as f
from tests.integration.conftest import datasource, make_grafana_client, resource_url


@respx.mock
async def test_cloudwatch_discovery_resource_contract():
    ds = datasource("cw-1", "CloudWatch", "cloudwatch", defaultRegion="us-east-1")
    respx.post(resource_url("cw-1", "namespaces")).mock(
        return_value=Response(200, json=f.cloudwatch_namespaces("AWS/ELB", "AWS/ApplicationELB", "AWS/EC2"))
    )
    respx.post(resource_url("cw-1", "metrics")).mock(
        return_value=Response(200, json=f.cloudwatch_metrics("HTTPCode_ELB_5XX", "Latency"))
    )
    respx.post(resource_url("cw-1", "dimension-keys")).mock(return_value=Response(200, json=["LoadBalancer"]))

    client = make_grafana_client()
    try:
        entries = await CloudWatchAdapter().discover_metrics(client, ds, ["elb", "5xx"])
    finally:
        await client.close()

    names = {e.name for e in entries}
    assert any(n.endswith("/HTTPCode_ELB_5XX") for n in names)
    assert all(e.query_language == "cloudwatch" for e in entries)
