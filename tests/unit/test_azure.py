"""Tests for Azure OpenAI LLM provider.

Covers:
- AzureOpenAIProvider: api_base required, deployment resolution
"""

import os
import sys
from unittest.mock import patch

from tacit.config import Settings

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


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
        assert provider.is_configured is False
        mock_openai.AsyncAzureOpenAI.assert_not_called()

    print("[PASS] test_azure_provider_without_key_does_not_construct_sdk_client")


def test_openai_provider_without_key_does_not_construct_sdk_client():
    """OpenAI zero-key fallback must not be blocked by SDK construction."""
    runtime_settings = Settings.model_validate({"llm_api_key": "", "llm_api_base": ""})
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import OpenAIProvider

        provider = OpenAIProvider(runtime_settings)
        assert provider.is_configured is False
        mock_openai.AsyncOpenAI.assert_not_called()

    print("[PASS] test_openai_provider_without_key_does_not_construct_sdk_client")


def test_openai_provider_without_key_uses_custom_api_base():
    """OpenAI-compatible local endpoints may not require real API keys."""
    runtime_settings = Settings.model_validate({"llm_api_key": "", "llm_api_base": "http://localhost:8001/v1"})
    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:

        from tacit.agents.providers.openai_provider import OpenAIProvider

        provider = OpenAIProvider(runtime_settings)
        assert provider.is_configured is True
        mock_openai.AsyncOpenAI.assert_called_once_with(
            api_key="tacit-local-openai-compatible",
            base_url="http://localhost:8001/v1",
            organization="",
            project="",
        )

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
    with patch("tacit.agents.providers.openai_provider.openai"):

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        provider = AzureOpenAIProvider(runtime_settings)
        assert provider._deployment == "gpt-4o"

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
    with patch("tacit.agents.providers.openai_provider.openai"):

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        provider = AzureOpenAIProvider(runtime_settings)
        assert provider._deployment == "my-custom-deployment"

    print("[PASS] test_azure_deployment_explicit")


def test_azure_provider_suppresses_ambient_organization_and_project(monkeypatch):
    monkeypatch.setenv("OPENAI_ORG_ID", "ambient-org")
    monkeypatch.setenv("OPENAI_PROJECT_ID", "ambient-project")
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="azure",
        llm_api_base="https://test.openai.azure.com",
        llm_api_key="test-key",
        llm_azure_deployment="deployment-a",
    )

    with patch("tacit.agents.providers.openai_provider.openai") as mock_openai:
        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        AzureOpenAIProvider(runtime_settings)

    mock_openai.AsyncAzureOpenAI.assert_called_once_with(
        api_key="test-key",
        azure_endpoint="https://test.openai.azure.com",
        api_version=runtime_settings.llm_azure_api_version,
        azure_deployment="deployment-a",
        organization="",
        project="",
    )


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
