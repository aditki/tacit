"""Tests for Azure OpenAI LLM provider.

Covers:
- AzureOpenAIProvider: api_base required, deployment resolution
"""

import asyncio
import os
import sys
from typing import Any
from unittest.mock import ANY, AsyncMock, patch

from tacit.config import Settings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _close_provider_and_assert_owner_hygiene(provider: Any) -> None:
    """Retire every test-created provider and verify its realized children."""
    sdk_client = provider._client
    http_client = provider._http_client
    asyncio.run(provider.close())
    if sdk_client is not None and isinstance(sdk_client.close, AsyncMock):
        sdk_client.close.assert_awaited_once_with()
    if http_client is not None:
        assert http_client.is_closed is True


def _configure_openai_sdk_mock(sdk_client: Any, *, api_key: str, azure: bool = False) -> None:
    """Expose the neutral credential state required before provider adoption."""
    sdk_client.api_key = api_key
    sdk_client.admin_api_key = ""
    sdk_client.workload_identity = None
    sdk_client._api_key_provider = None
    sdk_client.organization = ""
    sdk_client.project = ""
    sdk_client.webhook_secret = ""
    sdk_client._custom_headers = {}
    sdk_client.close = AsyncMock()
    if azure:
        sdk_client._azure_ad_token = ""
        sdk_client._azure_ad_token_provider = None


def test_azure_provider_requires_api_base():
    """AzureOpenAIProvider should raise ValueError if llm_api_base is empty."""
    runtime_settings = Settings.model_validate(
        {
            "llm_api_base": "",
            "llm_api_key": "test-key",
            "llm_azure_deployment": "",
            "llm_model": "gpt-4o",
        }
    )
    try:
        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        AzureOpenAIProvider(runtime_settings)
        assert False, "Should have raised ValueError"
    except ValueError as exc:
        assert "azure_endpoint" in str(exc).lower() or "llm_api_base" in str(exc)

    print("[PASS] test_azure_provider_requires_api_base")


def test_azure_provider_without_key_does_not_construct_sdk_client():
    """Zero-key fallback must be able to inspect configuration before SDK init."""
    runtime_settings = Settings.model_validate(
        {
            "llm_api_base": "",
            "llm_api_key": "",
            "llm_azure_deployment": "",
            "llm_model": "gpt-4o",
        }
    )
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        provider = AzureOpenAIProvider(runtime_settings)
        try:
            assert provider.is_configured is False
            mock_openai.AsyncAzureOpenAI.assert_not_called()
        finally:
            _close_provider_and_assert_owner_hygiene(provider)

    print("[PASS] test_azure_provider_without_key_does_not_construct_sdk_client")


def test_openai_provider_without_key_does_not_construct_sdk_client():
    """OpenAI zero-key fallback must not be blocked by SDK construction."""
    runtime_settings = Settings.model_validate({"llm_api_key": "", "llm_api_base": ""})
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import OpenAIProvider

        provider = OpenAIProvider(runtime_settings)
        try:
            assert provider.is_configured is False
            mock_openai.AsyncOpenAI.assert_not_called()
        finally:
            _close_provider_and_assert_owner_hygiene(provider)

    print("[PASS] test_openai_provider_without_key_does_not_construct_sdk_client")


def test_openai_provider_without_key_uses_custom_api_base():
    """OpenAI-compatible local endpoints may not require real API keys."""
    runtime_settings = Settings.model_validate({"llm_api_key": "", "llm_api_base": "http://localhost:8001/v1"})
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import OpenAIProvider

        _configure_openai_sdk_mock(
            mock_openai.AsyncOpenAI.return_value,
            api_key="tacit-local-openai-compatible",
        )
        provider = OpenAIProvider(runtime_settings)
        try:
            assert provider.is_configured is True
            mock_openai.AsyncOpenAI.assert_called_once_with(
                api_key="tacit-local-openai-compatible",
                admin_api_key="",
                workload_identity=None,
                base_url="http://localhost:8001/v1",
                organization="",
                project="",
                webhook_secret="",
                default_headers={},
                http_client=ANY,
            )
        finally:
            _close_provider_and_assert_owner_hygiene(provider)

    print("[PASS] test_openai_provider_without_key_uses_custom_api_base")


def test_azure_deployment_fallback_to_model():
    """When llm_azure_deployment is empty, should use llm_model."""
    runtime_settings = Settings.model_validate(
        {
            "llm_api_base": "https://test.openai.azure.com",
            "llm_api_key": "test-key",
            "llm_azure_deployment": "",
            "llm_model": "gpt-4o",
            "llm_azure_api_version": "2024-06-01",
        }
    )
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        _configure_openai_sdk_mock(
            mock_openai.AsyncAzureOpenAI.return_value,
            api_key="test-key",
            azure=True,
        )
        provider = AzureOpenAIProvider(runtime_settings)
        try:
            assert provider._deployment == "gpt-4o"
        finally:
            _close_provider_and_assert_owner_hygiene(provider)

    print("[PASS] test_azure_deployment_fallback_to_model")


def test_azure_deployment_explicit():
    """When llm_azure_deployment is set, should use it over llm_model."""
    runtime_settings = Settings.model_validate(
        {
            "llm_api_base": "https://test.openai.azure.com",
            "llm_api_key": "test-key",
            "llm_azure_deployment": "my-custom-deployment",
            "llm_model": "gpt-4o",
            "llm_azure_api_version": "2024-06-01",
        }
    )
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        _configure_openai_sdk_mock(
            mock_openai.AsyncAzureOpenAI.return_value,
            api_key="test-key",
            azure=True,
        )
        provider = AzureOpenAIProvider(runtime_settings)
        try:
            assert provider._deployment == "my-custom-deployment"
        finally:
            _close_provider_and_assert_owner_hygiene(provider)

    print("[PASS] test_azure_deployment_explicit")


def test_azure_provider_suppresses_ambient_organization_and_project(monkeypatch):
    monkeypatch.setenv("OPENAI_ORG_ID", "ambient-org")
    monkeypatch.setenv("OPENAI_PROJECT_ID", "ambient-project")
    runtime_settings = Settings.model_validate(
        {
            "llm_provider": "azure",
            "llm_api_base": "https://test.openai.azure.com",
            "llm_api_key": "test-key",
            "llm_azure_deployment": "deployment-a",
        }
    )

    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:
        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        _configure_openai_sdk_mock(
            mock_openai.AsyncAzureOpenAI.return_value,
            api_key="test-key",
            azure=True,
        )
        provider = AzureOpenAIProvider(runtime_settings)
        try:
            mock_openai.AsyncAzureOpenAI.assert_called_once_with(
                api_key="test-key",
                admin_api_key="",
                azure_endpoint="https://test.openai.azure.com",
                api_version=runtime_settings.llm_azure_api_version,
                azure_deployment="deployment-a",
                azure_ad_token="",
                azure_ad_token_provider=None,
                organization="",
                project="",
                webhook_secret="",
                default_headers={},
                http_client=ANY,
            )
        finally:
            _close_provider_and_assert_owner_hygiene(provider)


# ── Runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test_fn.__name__}: {e}")
            import traceback

            traceback.print_exc()
            failed += 1

    print(f"\n=== {passed} passed, {failed} failed out of {passed + failed} ===")
    if failed:
        sys.exit(1)
    print("=== All Azure provider tests passed ===")
