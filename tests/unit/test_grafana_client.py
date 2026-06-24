import pytest

from tacit.config import settings
from tacit.grafana.client import GrafanaClient


@pytest.mark.asyncio
async def test_grafana_client_omits_empty_authorization_header(monkeypatch):
    monkeypatch.setattr(settings, "grafana_api_key", "")

    client = GrafanaClient(base_url="http://grafana.test", api_key="")
    try:
        assert "authorization" not in client._client.headers
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_grafana_client_sends_configured_bearer_token():
    client = GrafanaClient(base_url="http://grafana.test", api_key="test-token")
    try:
        assert client._client.headers["authorization"] == "Bearer test-token"
    finally:
        await client.close()
