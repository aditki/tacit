from __future__ import annotations

from dashforge.config import Settings
from dashforge.context.registry import create_context_provider


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
