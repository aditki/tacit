from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass  # truststore not installed; fall back to default SSL

import yaml
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# ── Config file discovery ──────────────────────────────────────────────────
# Priority: DASHFORGE_CONFIG env var → ./dashforge.yaml → ./dashforge.yml → None

_CONFIG_SEARCH_PATHS = [
    "dashforge.yaml",
    "dashforge.yml",
    "config/dashforge.yaml",
    str(Path.home() / ".dashforge" / "config.yaml"),
]


def _find_config_file() -> Path | None:
    """Locate the YAML config file."""
    explicit = os.environ.get("DASHFORGE_CONFIG")
    if explicit:
        p = Path(explicit)
        if p.is_file():
            return p
        raise FileNotFoundError(f"DASHFORGE_CONFIG={explicit} does not exist")

    for name in _CONFIG_SEARCH_PATHS:
        p = Path(name)
        if p.is_file():
            return p
    return None


def _load_yaml_config() -> dict[str, Any]:
    """Load and flatten the YAML config into a dict suitable for Pydantic."""
    path = _find_config_file()
    if path is None:
        return {}

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    # Flatten nested sections: {llm: {provider: x}} → {llm_provider: x}
    flat: dict[str, Any] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat[f"{key}_{sub_key}"] = sub_value
        else:
            flat[key] = value
    return flat


class Settings(BaseSettings):
    """DashForge configuration.

    Loading order (last wins):
    1. Defaults defined here
    2. YAML config file (dashforge.yaml or DASHFORGE_CONFIG env var)
    3. .env file
    4. Environment variables

    Secrets (api keys, tokens) should use env vars or .env, not YAML.
    """

    model_config = SettingsConfigDict(
        env_file=[".env", str(Path.home() / ".dashforge" / ".env")],
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # LLM
    llm_provider: str = "anthropic"  # anthropic | openai | azure | ollama
    llm_api_key: str = Field(default="", repr=False)
    llm_model: str = "claude-sonnet-4-20250514"
    llm_api_base: str = ""  # optional: custom endpoint (Azure, Ollama, vLLM, etc.)
    # Azure OpenAI-specific (only used when llm_provider=azure)
    llm_azure_api_version: str = "2024-06-01"  # Azure OpenAI API version
    llm_azure_deployment: str = ""  # Azure deployment name (defaults to llm_model if empty)
    # AWS Bedrock-specific (only used when llm_provider=bedrock)
    llm_bedrock_region: str = "us-east-1"  # AWS region for Bedrock endpoint
    # Bedrock model ID; defaults to llm_model.
    llm_bedrock_model_id: str = ""
    llm_bedrock_role_arn: str = ""  # Optional IAM role ARN to assume (cross-account)
    llm_aws_access_key_id: str = Field(default="", repr=False)  # Optional explicit AWS key
    llm_aws_secret_access_key: str = Field(default="", repr=False)  # Optional explicit AWS secret

    # Grafana
    grafana_enabled: bool = True
    grafana_url: str = "http://localhost:3000"
    grafana_api_key: str = Field(default="", repr=False)
    grafana_org_id: int = 1

    # Splunk SignalFx (direct integration — publishes natively to Observability Cloud)
    signalfx_enabled: bool = False
    signalfx_api_token: str = Field(default="", repr=False)
    signalfx_realm: str = "us1"  # us0, us1, us2, eu0, jp0, au0
    signalfx_dashboard_group: str = "DashForge"

    # Slack
    slack_bot_token: str = Field(default="", repr=False)
    slack_app_token: str = Field(default="", repr=False)
    slack_signing_secret: str = Field(default="", repr=False)

    # Context enrichment (knowledge base)
    context_provider: str = "none"  # none | mcp | a2a | rag_api
    context_api_key: str = Field(default="", repr=False)
    context_mcp_server_url: str = ""  # MCP server URL
    context_mcp_tool_name: str = "search"  # MCP tool to call for retrieval
    context_a2a_agent_url: str = ""  # A2A agent endpoint
    context_rag_api_url: str = ""  # RAG API gateway base URL
    context_max_chunks: int = 10  # max context chunks per query

    # Concurrency & timeouts
    pipeline_max_concurrent: int = 5  # max simultaneous pipeline runs
    pipeline_timeout_seconds: int = 120  # overall pipeline timeout
    adapter_max_concurrent: int = 5  # max simultaneous datasource adapter calls
    adapter_timeout_seconds: int = 30  # per-adapter timeout
    max_metric_catalog_size: int = 300  # total metrics across all datasources sent to LLM

    # HTTP API auth
    api_auth_enabled: bool = False  # set True to require API key
    api_auth_key: str = Field(default="", repr=False)

    # App
    log_level: str = "INFO"
    dashforge_dashboard_folder: str = "DashForge"
    dashforge_default_timerange: str = "1h"

    @model_validator(mode="before")
    @classmethod
    def _inject_yaml(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Merge YAML config as the lowest-priority layer (before env vars)."""
        yaml_values = _load_yaml_config()
        # YAML provides defaults; env vars / .env override
        merged = {**yaml_values, **{k: v for k, v in values.items() if v is not None}}
        return merged


def _load_settings() -> Settings:
    """Load settings with YAML + env layering."""
    config_path = _find_config_file()
    if config_path:
        import structlog

        structlog.get_logger().info("config_loaded", source=str(config_path))
    return Settings()


settings = _load_settings()
