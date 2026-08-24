from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass  # truststore not installed; fall back to default SSL

import yaml
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tacit.archetypes.generated.schema import ArchetypeRetrievalMode
from tacit.sqlite_identity import inspect_sqlite_database_target, sqlite_database_path
from tacit.tenancy import TenantBoundaryError, resolve_tenant_boundary

if TYPE_CHECKING:
    from tacit.runtime_ownership import RuntimeOwnershipDescriptor

DEFAULT_HISTORY_DB_PATH = Path("data/tacit_history.db")
DEFAULT_FEEDBACK_DB_PATH = Path("data/tacit_feedback.db")
DEFAULT_SIGNALS_DB_PATH = Path("data/tacit_signals.db")
SQLITE_DATABASE_ROLE_DEFAULTS = {
    "history": DEFAULT_HISTORY_DB_PATH,
    "feedback": DEFAULT_FEEDBACK_DB_PATH,
    "signals": DEFAULT_SIGNALS_DB_PATH,
}
_SIGNALFX_REALM_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?",
    re.ASCII | re.IGNORECASE,
)
_KNOWLEDGE_PERMISSION_RE = re.compile(r"[A-Za-z0-9_.:-]+", re.ASCII)


def validate_distinct_sqlite_role_paths(
    role_paths: Mapping[str, str | Path],
) -> dict[str, Path]:
    """Canonicalize roles and reject cross-role pathname or file reuse."""
    unknown_roles = set(role_paths) - set(SQLITE_DATABASE_ROLE_DEFAULTS)
    if unknown_roles:
        raise ValueError("unsupported SQLite database role")

    canonical = {role: sqlite_database_path(path) for role, path in role_paths.items()}
    roles_by_path: dict[Path, list[str]] = {}
    for role, path in canonical.items():
        roles_by_path.setdefault(path, []).append(role)
    collisions = [roles for roles in roles_by_path.values() if len(roles) > 1]
    roles_by_file = _inspected_file_roles(canonical)
    collisions.extend(roles for roles in roles_by_file.values() if len(roles) > 1)
    if collisions:
        roles = ", ".join(sorted(collisions[0]))
        raise ValueError(f"SQLite database roles must use distinct files: {roles}")
    return canonical


def _inspected_file_roles(role_paths: Mapping[str, Path]) -> dict[tuple[int, int], list[str]]:
    """Inspect existing role files without opening or following their targets."""
    roles_by_file: dict[tuple[int, int], list[str]] = {}
    for role, path in role_paths.items():
        metadata = _sqlite_target_metadata(path)
        if metadata is not None:
            roles_by_file.setdefault((metadata.st_dev, metadata.st_ino), []).append(role)
    return roles_by_file


def _sqlite_target_metadata(path: Path) -> os.stat_result | None:
    """Return target metadata without creating or opening the configured file."""
    return inspect_sqlite_database_target(path)


def canonical_sqlite_role_paths(
    role_paths: Mapping[str, str | Path | None],
) -> dict[str, Path]:
    """Canonicalize and validate the complete effective SQLite role map."""
    effective_paths = {
        role: role_paths.get(role) or default_path for role, default_path in SQLITE_DATABASE_ROLE_DEFAULTS.items()
    }
    return validate_distinct_sqlite_role_paths(effective_paths)


def canonical_signalfx_realm(value: str) -> str:
    """Return one canonical, injection-safe SignalFx realm DNS label."""
    raw = str(value or "")
    if _SIGNALFX_REALM_RE.fullmatch(raw) is None:
        raise ValueError("SignalFx realm is invalid")
    return raw.casefold()


def canonical_knowledge_tenant_id(value: object) -> str:
    """Return the tenant identity used by settings, auth, and ownership."""
    tenant_id = str(value or "").strip() or "default"
    if tenant_id == "*":
        return tenant_id
    try:
        return resolve_tenant_boundary(tenant_id, None)
    except TenantBoundaryError as exc:
        raise ValueError(exc.detail) from None


def canonical_knowledge_permissions(value: object) -> str:
    """Canonicalize permission-set syntax without changing token case."""
    permissions: set[str] = set()
    for candidate in str(value or "").split(","):
        permission = candidate.strip()
        if not permission:
            continue
        if _KNOWLEDGE_PERMISSION_RE.fullmatch(permission) is None:
            raise ValueError("knowledge permission token is invalid")
        permissions.add(permission)
    return ",".join(sorted(permissions))


def validated_knowledge_tenant_api_keys(
    value: Mapping[str, str],
) -> dict[str, str]:
    """Validate tenant-key names exactly as wildcard lookup consumes them."""
    validated: dict[str, str] = {}
    for tenant, secret in value.items():
        tenant_name = str(tenant)
        try:
            canonical_name = canonical_knowledge_tenant_id(tenant_name)
        except ValueError:
            raise ValueError("knowledge tenant key name is invalid") from None
        if tenant_name != canonical_name or tenant_name == "*":
            raise ValueError("knowledge tenant key name is invalid")
        validated[tenant_name] = secret
    return validated


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
        validate_assignment=True,
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
    learned_archetypes_retrieval_max_directory_entries: int = Field(default=1_024, ge=1, le=100_000)
    learned_archetypes_retrieval_max_files: int = Field(default=256, ge=1, le=10_000)
    learned_archetypes_retrieval_max_file_bytes: int = Field(
        default=512 * 1_024,
        ge=1_024,
        le=64 * 1_024 * 1_024,
    )
    learned_archetypes_retrieval_max_total_bytes: int = Field(
        default=8 * 1_024 * 1_024,
        ge=1_024,
        le=256 * 1_024 * 1_024,
    )
    learned_archetypes_retrieval_max_yaml_nodes: int = Field(default=12_000, ge=1, le=1_000_000)
    learned_archetypes_retrieval_max_yaml_depth: int = Field(default=32, ge=1, le=256)
    learned_archetypes_retrieval_max_yaml_scalars: int = Field(default=8_000, ge=1, le=1_000_000)
    learned_archetypes_retrieval_max_yaml_scalar_bytes: int = Field(
        default=64 * 1_024,
        ge=1,
        le=16 * 1_024 * 1_024,
    )
    learned_archetypes_retrieval_max_artifacts_per_file: int = Field(
        default=64,
        ge=1,
        le=10_000,
    )
    learned_archetypes_retrieval_max_panels_per_file: int = Field(
        default=256,
        ge=1,
        le=100_000,
    )
    learned_archetypes_retrieval_max_queries_per_file: int = Field(
        default=1_024,
        ge=1,
        le=1_000_000,
    )
    learned_archetypes_retrieval_max_total_artifacts: int = Field(
        default=256,
        ge=1,
        le=4_096,
    )
    learned_archetypes_retrieval_max_total_panels: int = Field(
        default=1_024,
        ge=1,
        le=16_384,
    )
    learned_archetypes_retrieval_max_total_queries: int = Field(
        default=4_096,
        ge=1,
        le=65_536,
    )
    learned_archetypes_retrieval_max_results: int = Field(
        default=256,
        ge=1,
        le=4_096,
    )

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
    knowledge_tenant_id: str = "default"
    knowledge_tenant_api_keys: dict[str, str] = Field(default_factory=dict, repr=False)
    knowledge_permissions: str = (
        "knowledge.read,knowledge.review,knowledge.trust,knowledge.reject,knowledge.correct,knowledge.apply,knowledge.export,"
        "knowledge.override"
    )
    knowledge_snapshot_candidate_limit: int = Field(default=1_000, ge=1, le=100_000)
    knowledge_snapshot_scan_limit: int = Field(default=10_000, ge=100, le=1_000_000)
    knowledge_conflict_comparison_limit: int = Field(default=1_000, ge=10, le=10_000)
    knowledge_source_atomic_candidate_limit: int = Field(default=1_000, ge=1, le=10_000)
    artifact_learning_directory_file_limit: int = Field(default=10_000, ge=1, le=100_000)
    signal_resolution_mapping_limit: int = Field(default=500, ge=10, le=5_000)
    signal_resolution_catalog_limit: int = Field(default=5_000, ge=100, le=100_000)
    signal_resolution_pattern_check_limit: int = Field(default=1_000_000, ge=100, le=50_000_000)
    learning_approval_claim_ttl_seconds: int = Field(default=900, ge=30, le=86_400)

    # App
    log_level: str = "INFO"
    tacit_dashboard_folder: str = "Tacit"
    tacit_default_timerange: str = "1h"

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return this configuration's side-effect-free composition identity."""
        from tacit.runtime_ownership import runtime_descriptor_from_settings

        return runtime_descriptor_from_settings(self, component="settings")

    @model_validator(mode="before")
    @classmethod
    def _inject_yaml(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Merge YAML config as the lowest-priority layer (before env vars)."""
        yaml_values = _load_yaml_config()
        # YAML provides defaults; env vars / .env override
        merged = {**yaml_values, **{k: v for k, v in values.items() if v is not None}}
        return merged

    @field_validator("signalfx_realm")
    @classmethod
    def _validate_signalfx_realm(cls, value: str) -> str:
        return canonical_signalfx_realm(value)

    @field_validator("knowledge_tenant_id", mode="before")
    @classmethod
    def _canonicalize_knowledge_tenant_id(cls, value: object) -> str:
        return canonical_knowledge_tenant_id(value)

    @field_validator("knowledge_permissions", mode="before")
    @classmethod
    def _canonicalize_knowledge_permissions(cls, value: object) -> str:
        return canonical_knowledge_permissions(value)

    @field_validator("knowledge_tenant_api_keys")
    @classmethod
    def _validate_knowledge_tenant_api_key_names(
        cls,
        value: dict[str, str],
    ) -> dict[str, str]:
        return validated_knowledge_tenant_api_keys(value)

    @model_validator(mode="after")
    def _validate_tenant_api_keys(self) -> Settings:
        if self.knowledge_tenant_id != "*":
            return self
        if not self.api_auth_enabled:
            raise ValueError("Wildcard knowledge tenancy requires API authentication")
        non_empty_keys = [value for value in self.knowledge_tenant_api_keys.values() if value]
        if len(non_empty_keys) != len(set(non_empty_keys)):
            raise ValueError("knowledge_tenant_api_keys must use a unique non-empty key per tenant")
        return self

    @model_validator(mode="after")
    def _validate_sqlite_store_paths(self) -> Settings:
        canonical_sqlite_role_paths(
            {
                "history": self.history_db_path,
                "feedback": self.feedback_db_path,
                "signals": self.signals_db_path,
            }
        )
        return self


def create_settings() -> Settings:
    """Load settings with YAML + env layering."""
    config_path = _find_config_file()
    if config_path:
        import structlog

        structlog.get_logger().info("config_loaded", source=str(config_path))
    return Settings()


_load_settings = create_settings

settings = create_settings()
