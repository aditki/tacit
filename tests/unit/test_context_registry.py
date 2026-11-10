from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import structlog

from tacit.config import Settings
from tacit.context.enrichment import enrich_context
from tacit.context.registry import create_context_provider
from tacit.logging import configure_logging
from tacit.models.schemas import Intent


def test_create_context_provider_uses_runtime_settings():
    runtime_settings = Settings(
        context_provider="rag_api",
        context_rag_api_url="http://runtime-rag.test",
        context_api_key="runtime-key",
    )

    provider = create_context_provider(runtime_settings)

    assert provider is not None
    assert provider.name == "rag_api"
    assert provider._base_url == "http://runtime-rag.test"
    assert provider._client.headers["authorization"] == "Bearer runtime-key"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_name", "event"),
    [
        ("rag_api", "rag_api_query_failed"),
        ("mcp", "mcp_query_failed"),
        ("a2a", "a2a_query_failed"),
        ("injected", "context_enrichment_failed"),
    ],
)
async def test_context_failures_emit_message_free_diagnostics(provider_name, event, monkeypatch, capsys):
    endpoint = "https://example.invalid/synthetic-private-endpoint"
    payload = "synthetic-private-response-payload"
    failure = AsyncMock(side_effect=RuntimeError(payload))
    intent = Intent(summary="Audit context failure", domain="application")
    original_config = structlog.get_config()
    try:
        configure_logging()
        if provider_name == "injected":
            provider = SimpleNamespace(name=provider_name, query=failure)
            assert await enrich_context(intent, provider=provider) == []
        else:
            runtime_settings = Settings(
                _env_file=None,
                context_provider=provider_name,
                context_rag_api_url=endpoint,
                context_mcp_server_url=endpoint,
                context_a2a_agent_url=endpoint,
            )
            provider = create_context_provider(runtime_settings)
            assert provider is not None
            try:
                monkeypatch.setattr(provider._client, "post", failure)
                assert await provider.query(intent) == []
            finally:
                await provider.close()
    finally:
        structlog.configure(**original_config)

    failure.assert_awaited_once()
    output = capsys.readouterr().out
    assert endpoint not in output
    assert payload not in output
    records = [json.loads(line) for line in output.splitlines()]
    failures = [record for record in records if record["event"] == event]
    assert len(failures) == 1
    diagnostic = failures[0]
    assert diagnostic["level"] == "warning"
    assert diagnostic["reason_code"] == event
    assert diagnostic["error_type"] == "RuntimeError"
    assert len(diagnostic["failure_fingerprint"]) == 12
    assert "exception" not in diagnostic
