"""Executable lower-bound contract for the OpenAI-family and Anthropic SDKs."""

from __future__ import annotations

import asyncio
import importlib.metadata
import os
import tomllib
from pathlib import Path

import pytest
import yaml
from packaging.version import Version

from tacit.agents.providers.anthropic import AnthropicProvider
from tacit.agents.providers.openai_provider import AzureOpenAIProvider, OpenAIProvider
from tacit.config import Settings

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SDK_LOWER_BOUND_ENV = "TACIT_SDK_LOWER_BOUND_CONTRACT"
SDK_FLOORS = {
    "anthropic": "0.100.0",
    "openai": "2.34.0",
}


def _project_metadata() -> dict:
    pyproject = tomllib.loads((REPOSITORY_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return pyproject


def test_provider_sdk_lower_bounds_are_declared_and_exercised_in_ci() -> None:
    """Keep the published dependency contract tied to an executable CI check."""
    pyproject = _project_metadata()
    dependencies = set(pyproject["project"]["dependencies"])
    assert f"anthropic>={SDK_FLOORS['anthropic']}" in dependencies
    assert f"openai>={SDK_FLOORS['openai']}" in dependencies

    workflow = yaml.safe_load((REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["provider-sdk-lower-bounds"]
    assert job["timeout-minutes"] == 10
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "uv sync --locked --all-extras --dev" in commands
    assert "uv pip install" in commands
    assert "--python .venv/bin/python" in commands
    assert "--no-deps" in commands
    assert "--only-binary :all:" in commands
    assert "anthropic==0.100.0" in commands
    assert "openai==2.34.0" in commands
    assert f"{SDK_LOWER_BOUND_ENV}=1" in commands
    assert ".venv/bin/python -m pytest" in commands
    assert "tests/unit/test_provider_sdk_lower_bounds.py" in commands


def test_openai_azure_and_anthropic_provider_constructors_support_declared_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Instantiate all providers without allowing ambient credential authority."""
    for package, minimum in SDK_FLOORS.items():
        installed = Version(importlib.metadata.version(package))
        assert installed >= Version(minimum)
        if os.environ.get(SDK_LOWER_BOUND_ENV) == "1":
            assert installed == Version(minimum)

    monkeypatch.setenv("OPENAI_ADMIN_KEY", "ambient-admin-key")
    monkeypatch.setenv("OPENAI_ORG_ID", "ambient-org")
    monkeypatch.setenv("OPENAI_PROJECT_ID", "ambient-project")
    monkeypatch.setenv("OPENAI_WEBHOOK_SECRET", "ambient-webhook")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "ambient-auth-token")
    monkeypatch.setenv("ANTHROPIC_PROFILE", "ambient-profile")
    monkeypatch.setenv("ANTHROPIC_WEBHOOK_SIGNING_KEY", "ambient-webhook-key")

    openai_provider = OpenAIProvider(
        Settings.model_validate(
            {
                "llm_provider": "openai",
                "llm_api_key": "configured-openai-key",
                "llm_api_base": "https://openai.example.test/v1",
            }
        )
    )
    azure_provider = AzureOpenAIProvider(
        Settings.model_validate(
            {
                "llm_provider": "azure",
                "llm_api_key": "configured-azure-key",
                "llm_api_base": "https://azure.example.test",
                "llm_azure_deployment": "deployment-a",
            }
        )
    )
    anthropic_provider = AnthropicProvider(
        Settings.model_validate(
            {
                "llm_provider": "anthropic",
                "llm_api_key": "configured-anthropic-key",
                "llm_api_base": "https://anthropic.example.test",
            }
        )
    )
    try:
        assert openai_provider._client.api_key == "configured-openai-key"
        assert openai_provider._client.admin_api_key == ""
        assert openai_provider._client.workload_identity is None
        assert openai_provider._client.organization == ""
        assert openai_provider._client.project == ""
        assert openai_provider._client.webhook_secret == ""

        assert azure_provider._client.api_key == "configured-azure-key"
        assert azure_provider._client.admin_api_key == ""
        assert azure_provider._client.workload_identity is None
        assert azure_provider._client.organization == ""
        assert azure_provider._client.project == ""
        assert azure_provider._client.webhook_secret == ""
        assert azure_provider._client._azure_ad_token is None
        assert azure_provider._client._azure_ad_token_provider is None

        assert anthropic_provider._client.api_key == "configured-anthropic-key"
        assert anthropic_provider._client.auth_token is None
        assert anthropic_provider._client.credentials is None
        assert anthropic_provider._client.webhook_key == ""
    finally:
        asyncio.run(anthropic_provider.close())
        asyncio.run(azure_provider.close())
        asyncio.run(openai_provider.close())
