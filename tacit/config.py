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

from tacit.archetypes.generated.schema import ArchetypeRetrievalMode

# ── Config file discovery ──────────────────────────────────────────────────
# Priority: TACIT_CONFIG env var → ./tacit.yaml → ./tacit.yml → None

_CONFIG_SEARCH_PATHS = [
    "tacit.yaml",
    "tacit.yml",
    "config/tacit.yaml",
    str(Path.home() / ".tacit" / "config.yaml"),
]


def _find_config_file() -> Path | None:
    """Locate the YAML config file."""
    explicit = os.environ.get("TACIT_CONFIG")
    if explicit:
        p = Path(explicit)
        if p.is_file():
            return p
        raise FileNotFoundError(f"TACIT_CONFIG={explicit} does not exist")

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
    """Tacit configuration.

    Loading order (last wins):
    1. Defaults defined here
    2. YAML config file (tacit.yaml or TACIT_CONFIG env var)
    3. .env file
    4. Environment variables

    Secrets (api keys, tokens) should use env vars or .env, not YAML.
    """

    model_config = SettingsConfigDict(
        env_file=[".env", str(Path.home() / ".tacit" / ".env")],
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
    # Zero-key mode: when the configured provider has no API key, fall back to
    # deterministic keyword-based intent classification instead of failing.
    # The archetype engine then compiles the dashboard without any LLM calls.
    intent_fallback_enabled: bool = True

    # Grafana
    grafana_enabled: bool = True
    grafana_url: str = "http://localhost:3000"
    # Browser-facing base URL for generated dashboard links. Set this when the
    # API URL above is only reachable from Tacit's network (e.g. Docker's
    # http://grafana:3000) but users open dashboards at a different address.
    # Empty = use grafana_url.
    grafana_public_url: str = ""
    grafana_api_key: str = Field(default="", repr=False)
    grafana_org_id: int = 1

    # Splunk SignalFx (direct integration — publishes natively to Observability Cloud)
    signalfx_enabled: bool = False
    signalfx_api_token: str = Field(default="", repr=False)
    signalfx_realm: str = "us1"  # us0, us1, us2, eu0, jp0, au0
    signalfx_dashboard_group: str = "Tacit"

    # PagerDuty (read-only incident-metadata ingestion for artifact learning)
    pagerduty_api_token: str = Field(default="", repr=False)
    pagerduty_base_url: str = "https://api.pagerduty.com"

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

    # Archetype blending caps — bound the irrelevant-panel explosion from
    # blending many loosely-matched archetypes.
    max_blended_archetypes: int = 3  # primary + up to N-1 secondaries
    max_dashboard_panels: int = 10  # hard ceiling on a blended dashboard's panels
    min_secondary_coverage: float = 0.25  # drop secondaries below this live-signal coverage
    learned_archetype_min_coverage: float = 0.75
    learned_archetype_boost: float = 0.15

    # SQLite storage. Empty values preserve the built-in data/*.db defaults;
    # configured paths may be relative to the process working directory or absolute.
    history_db_path: str = ""
    feedback_db_path: str = ""
    signals_db_path: str = ""

    # Generated archetypes are experimental artifacts, never curated registry
    # entries. Generation, quarantine persistence, and explicit experimental
    # retrieval are separate controls and are all disabled by default.
    learned_archetypes_generation_enabled: bool = False
    # Legacy compatibility name. This can permit quarantine writes only; direct
    # registration into the curated registry has been removed.
    learned_archetypes_automatic_registration_enabled: bool = False
    learned_archetypes_normal_retrieval_enabled: bool = False
    learned_archetypes_retrieval_mode: ArchetypeRetrievalMode = ArchetypeRetrievalMode.CURATED_ONLY
    learned_archetypes_quarantine_path: str = "data/generated_archetypes/quarantine"
    learned_archetypes_generation_version: str = "generated-archetype-v1"
    learned_archetypes_tenant_id: str = "default"

    # Deprecated compatibility input. It is intentionally ignored so an old
    # deployment cannot restore direct writes into TACIT_ARCHETYPES_PATH.
    learning_auto_register_archetype: bool = False

    # Local benchmark result storage. Raw result files may contain fixture
    # content; anonymous exports include only sanitized summaries derived from
    # this directory.
    evaluation_results_dir: str = ""

    # HTTP API auth
    api_auth_enabled: bool = False  # set True to require API key
    api_auth_key: str = Field(default="", repr=False)

    # App
    log_level: str = "INFO"
    tacit_dashboard_folder: str = "Tacit"
    tacit_default_timerange: str = "1h"

    @model_validator(mode="before")
    @classmethod
    def _inject_yaml(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Merge YAML config as the lowest-priority layer (before env vars)."""
        yaml_values = _load_yaml_config()
        # YAML provides defaults; env vars / .env override
        merged = {**yaml_values, **{k: v for k, v in values.items() if v is not None}}
        return merged


def create_settings() -> Settings:
    """Load settings with YAML + env layering."""
    config_path = _find_config_file()
    if config_path:
        import structlog

        structlog.get_logger().info("config_loaded", source=str(config_path))
    return Settings()


_load_settings = create_settings

settings = create_settings()
