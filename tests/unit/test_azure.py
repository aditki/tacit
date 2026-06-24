"""Tests for Azure OpenAI LLM provider.

Covers:
- AzureOpenAIProvider: api_base required, deployment resolution
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_azure_provider_requires_api_base():
    """AzureOpenAIProvider should raise ValueError if llm_api_base is empty."""
    with patch("tacit.agents.providers.openai_provider.settings") as mock_settings:
        mock_settings.llm_api_base = ""
        mock_settings.llm_api_key = "test-key"
        mock_settings.llm_azure_deployment = ""
        mock_settings.llm_model = "gpt-4o"

        try:
            from tacit.agents.providers.openai_provider import AzureOpenAIProvider

            AzureOpenAIProvider()
            assert False, "Should have raised ValueError"
        except ValueError as exc:
            assert "azure_endpoint" in str(exc).lower() or "llm_api_base" in str(exc)

    print("[PASS] test_azure_provider_requires_api_base")


def test_azure_deployment_fallback_to_model():
    """When llm_azure_deployment is empty, should use llm_model."""
    with (
        patch("tacit.agents.providers.openai_provider.settings") as mock_settings,
        patch("tacit.agents.providers.openai_provider.openai"),
    ):
        mock_settings.llm_api_base = "https://test.openai.azure.com"
        mock_settings.llm_api_key = "test-key"
        mock_settings.llm_azure_deployment = ""
        mock_settings.llm_model = "gpt-4o"
        mock_settings.llm_azure_api_version = "2024-06-01"

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        provider = AzureOpenAIProvider()
        assert provider._deployment == "gpt-4o"

    print("[PASS] test_azure_deployment_fallback_to_model")


def test_azure_deployment_explicit():
    """When llm_azure_deployment is set, should use it over llm_model."""
    with (
        patch("tacit.agents.providers.openai_provider.settings") as mock_settings,
        patch("tacit.agents.providers.openai_provider.openai"),
    ):
        mock_settings.llm_api_base = "https://test.openai.azure.com"
        mock_settings.llm_api_key = "test-key"
        mock_settings.llm_azure_deployment = "my-custom-deployment"
        mock_settings.llm_model = "gpt-4o"
        mock_settings.llm_azure_api_version = "2024-06-01"

        from tacit.agents.providers.openai_provider import AzureOpenAIProvider

        provider = AzureOpenAIProvider()
        assert provider._deployment == "my-custom-deployment"

    print("[PASS] test_azure_deployment_explicit")


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
