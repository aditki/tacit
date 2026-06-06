"""Hermetic contract test for Elasticsearch/OpenSearch field discovery."""

from __future__ import annotations

import respx
from httpx import Response

from dashforge.grafana.adapters.elasticsearch import ElasticsearchAdapter
from tests.contracts import factories as f
from tests.integration.conftest import datasource, make_grafana_client, proxy_url


@respx.mock
async def test_elasticsearch_discovery_get_contract():
    ds = datasource("es-1", "Elasticsearch", "elasticsearch", index="logs-*")
    respx.get(proxy_url("es-1", "logs-*/_mapping")).mock(
        return_value=Response(
            200,
            json=f.elasticsearch_mapping(
                "logs-*", {"status_code": "integer", "duration_ms": "float", "message": "text"}
            ),
        )
    )
    client = make_grafana_client()
    try:
        entries = await ElasticsearchAdapter().discover_metrics(client, ds, ["status"])
    finally:
        await client.close()

    assert entries, "Elasticsearch mapping should yield discovery entries"
    assert all(e.query_language in ("lucene", "elasticsearch") for e in entries)
