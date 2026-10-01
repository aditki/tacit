from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping
from ipaddress import IPv6Address, ip_address
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass  # truststore not installed; fall back to default SSL

import yaml
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from tacit.archetypes.generated.schema import ArchetypeRetrievalMode
from tacit.models.request_limits import pipeline_retained_request_memory_bound
from tacit.sqlite_identity import (
    DEFAULT_SQLITE_SNAPSHOT_MAX_BYTES,
    inspect_sqlite_database_target,
    sqlite_database_path,
)
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
_CORS_HOST_LABEL_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", re.ASCII)
_AWS_REGION_RE = re.compile(
    r"(?:af|ap|ca|cn|eu|eusc|il|me|mx|sa|us)(?:-[a-z0-9]+)+-[1-9][0-9]*",
    re.ASCII,
)

API_MAX_REQUEST_BODY_BYTES_MIN = 1_024
API_MAX_REQUEST_BODY_BYTES_MAX = 64 * 1_024 * 1_024
DEFAULT_API_MAX_REQUEST_BODY_BYTES = 2 * 1_024 * 1_024
API_REQUEST_BODY_MAX_CONCURRENT_MAX = 1_024
DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT = 16
DEFAULT_API_REQUEST_BODY_TENANT_MAX_CONCURRENT = 4
API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX = 8 * 1_024 * 1_024 * 1_024
DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES = 512 * 1_024 * 1_024
DEFAULT_API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES = 128 * 1_024 * 1_024
API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN = 2
API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX = 16
DEFAULT_API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR = 8
# CPython's decoded representation of dense JSON arrays, including allocator
# overhead, can retain more than 60 bytes per wire byte. This floor is
# intentionally not configurable: the
# generic factor remains useful for byte-oriented bodies, while JSON admission
# must not be weakened below its measured structural allocation envelope.
API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR = 64
API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN = 4 * 1_024 * 1_024
API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX = 16 * 1_024 * 1_024
DEFAULT_API_REQUEST_BODY_MEMORY_FLOOR_BYTES = API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN
API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN = 0.05
API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX = 300.0
DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS = 15.0
DEFAULT_API_ALLOWED_HOSTS = "localhost,127.0.0.1,[::1],testserver"
BEDROCK_COMPATIBILITY_MAX_CONCURRENT = 32


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


def canonical_aws_region(value: object) -> str:
    """Return one canonical AWS region identifier safe for endpoint synthesis."""
    region = str(value or "").strip().casefold()
    if _AWS_REGION_RE.fullmatch(region) is None:
        raise ValueError("AWS region is invalid")
    return region


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


def canonical_cors_allowed_origins(value: object) -> str:
    """Canonicalize a comma-separated list of exact browser origins."""
    origins: list[str] = []
    for candidate in str(value or "").split(","):
        origin = candidate.strip()
        if not origin:
            continue
        if origin == "*":
            origins.append(origin)
            continue
        try:
            parsed = urlsplit(origin)
            hostname = parsed.hostname
            port = parsed.port
        except ValueError:
            raise ValueError("CORS origins must be exact http(s) origins") from None
        if (
            parsed.scheme.casefold() not in {"http", "https"}
            or not parsed.netloc
            or hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.netloc.endswith(":")
            or "%" in parsed.netloc
            or "\\" in parsed.netloc
            or any(
                character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F for character in parsed.netloc
            )
        ):
            raise ValueError("CORS origins must be exact http(s) origins")
        if parsed.netloc.startswith("["):
            try:
                bracketed_address = ip_address(hostname)
            except ValueError:
                raise ValueError("CORS origins must be exact http(s) origins") from None
            if not isinstance(bracketed_address, IPv6Address):
                raise ValueError("CORS origins must be exact http(s) origins")
        canonical_host = _canonical_cors_host(hostname)
        scheme = parsed.scheme.casefold()
        default_port = 80 if scheme == "http" else 443
        authority = canonical_host if port in {None, default_port} else f"{canonical_host}:{port}"
        origins.append(f"{scheme}://{authority}")
    return ",".join(dict.fromkeys(origins))


def canonical_api_host_name(value: object) -> str:
    """Return one exact, port-free host identity for request admission."""
    raw = str(value or "")
    if not raw or raw != raw.strip() or any(character.isspace() for character in raw):
        raise ValueError("API allowed hosts must be exact DNS or IP hosts")
    if raw.startswith("[") or raw.endswith("]"):
        if not (raw.startswith("[") and raw.endswith("]")):
            raise ValueError("API allowed hosts must be exact DNS or IP hosts")
        try:
            address = ip_address(raw[1:-1])
        except ValueError:
            raise ValueError("API allowed hosts must be exact DNS or IP hosts") from None
        if not isinstance(address, IPv6Address):
            raise ValueError("API allowed hosts must be exact DNS or IP hosts")
        return f"[{address.compressed}]"
    try:
        address = ip_address(raw)
    except ValueError:
        try:
            return _canonical_cors_host(raw)
        except ValueError:
            raise ValueError("API allowed hosts must be exact DNS or IP hosts") from None
    if isinstance(address, IPv6Address):
        return f"[{address.compressed}]"
    return address.compressed


def canonical_api_allowed_hosts(value: object) -> str:
    """Canonicalize an explicit comma-separated Host-header allowlist."""
    patterns: list[str] = []
    for candidate in str(value or "").split(","):
        raw = candidate.strip()
        if not raw:
            continue
        wildcard = raw.startswith("*.")
        if "*" in raw and not wildcard:
            raise ValueError("API allowed hosts cannot contain an unrestricted wildcard")
        host = canonical_api_host_name(raw[2:] if wildcard else raw)
        if wildcard:
            try:
                ip_address(host.strip("[]"))
            except ValueError:
                pass
            else:
                raise ValueError("API allowed hosts cannot wildcard an IP address")
            host = f"*.{host}"
        patterns.append(host)
    if not patterns:
        raise ValueError("API allowed hosts must include at least one exact host")
    return ",".join(dict.fromkeys(patterns))


def api_host_is_allowed(host: str, allowed_hosts: Iterable[str]) -> bool:
    """Return whether one canonical Host identity matches an allowed pattern."""
    return any(
        host == pattern or (pattern.startswith("*.") and host.endswith(pattern[1:]) and host != pattern[2:])
        for pattern in allowed_hosts
    )


def is_loopback_bind_host(value: object) -> bool:
    """Return whether a server bind target is explicitly loopback-only."""
    raw = str(value or "").strip().casefold()
    if raw == "localhost":
        return True
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    try:
        return ip_address(raw).is_loopback
    except ValueError:
        return False


def validate_api_server_bind(runtime_settings: object, bind_host: object) -> str:
    """Admit one server bind through the shared auth and Host-header policy."""
    raw_host = str(bind_host or "")
    canonical_host = canonical_api_host_name(raw_host)
    if is_loopback_bind_host(raw_host):
        return raw_host
    if not bool(getattr(runtime_settings, "api_auth_enabled", False)):
        raise ValueError("Non-loopback serving requires API authentication")

    configured_fields = set(getattr(runtime_settings, "model_fields_set", set()))
    if "api_allowed_hosts" not in configured_fields:
        raise ValueError("Non-loopback serving requires an explicit API allowed-host policy")
    allowed_hosts = tuple(canonical_api_allowed_hosts(getattr(runtime_settings, "api_allowed_hosts", "")).split(","))

    try:
        wildcard_bind = ip_address(canonical_host.strip("[]")).is_unspecified
    except ValueError:
        wildcard_bind = False
    compatible = (
        any(host != "testserver" for host in allowed_hosts)
        if wildcard_bind
        else api_host_is_allowed(canonical_host, allowed_hosts)
    )
    if not compatible:
        raise ValueError("Non-loopback serving requires a compatible API allowed host")
    return raw_host


def _canonical_cors_host(hostname: str) -> str:
    """Return a browser-compatible DNS or IP host without parser ambiguity."""
    try:
        address = ip_address(hostname)
    except ValueError:
        if ":" in hostname:
            raise ValueError("CORS origins must be exact http(s) origins") from None
        if not hostname.isascii():
            raise ValueError("CORS origins must be exact http(s) origins") from None
        canonical = hostname.casefold()
        labels = canonical.split(".")
        final_label = labels[-1] if labels else ""
        browser_numeric_host = final_label.isdigit() or (
            final_label.startswith("0x")
            and len(final_label) > 2
            and all(character in "0123456789abcdef" for character in final_label[2:])
        )
        if (
            not canonical
            or len(canonical) > 253
            or browser_numeric_host
            or any(_CORS_HOST_LABEL_RE.fullmatch(label) is None for label in labels)
        ):
            raise ValueError("CORS origins must be exact http(s) origins")
        return canonical
    if isinstance(address, IPv6Address):
        return f"[{address.compressed}]"
    return address.compressed


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
    if not isinstance(raw, dict):
        raise ValueError("Tacit YAML configuration must be a mapping")

    # Flatten nested sections: {llm: {provider: x}} → {llm_provider: x}
    flat: dict[str, Any] = {}
    source_names: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                field_name = f"{key}_{sub_key}"
                flat[field_name] = sub_value
                source_names[field_name] = f"{key}.{sub_key}"
        else:
            flat[key] = value
            source_names[key] = key

    settings_type = globals().get("Settings")
    if settings_type is not None:
        unknown = sorted(source_names[name] for name in flat if name not in settings_type.model_fields)
        if unknown:
            raise ValueError(f"Unsupported Tacit YAML configuration key(s): {', '.join(unknown)}")
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
        hide_input_in_errors=True,
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
    llm_aws_session_token: str = Field(default="", repr=False)  # Optional temporary-credential token
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
    pipeline_max_concurrent: int = Field(default=5, ge=1, le=1_000)
    # Zero selects the wildcard-safe default of global concurrency minus one.
    pipeline_max_concurrent_per_tenant: int = Field(default=0, ge=0, le=1_000)
    pipeline_max_queued: int = Field(default=100, ge=0, le=1_000)
    pipeline_max_queued_per_tenant: int = Field(default=25, ge=0, le=1_000)
    pipeline_timeout_seconds: float = Field(default=120, gt=0, le=86_400)
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
    sqlite_snapshot_max_bytes: int = Field(
        default=DEFAULT_SQLITE_SNAPSHOT_MAX_BYTES,
        gt=0,
        description=(
            "Per-physical-database copy cap for protected SQLite admission; "
            "each admission shares it across main/WAL copies and retries"
        ),
    )

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
    api_allowed_hosts: str = DEFAULT_API_ALLOWED_HOSTS
    api_cors_allowed_origins: str = ""
    api_max_request_body_bytes: int = Field(
        default=DEFAULT_API_MAX_REQUEST_BODY_BYTES,
        ge=API_MAX_REQUEST_BODY_BYTES_MIN,
        le=API_MAX_REQUEST_BODY_BYTES_MAX,
    )
    api_request_body_max_concurrent: int = Field(
        default=DEFAULT_API_REQUEST_BODY_MAX_CONCURRENT,
        ge=1,
        le=API_REQUEST_BODY_MAX_CONCURRENT_MAX,
    )
    api_request_body_max_buffered_bytes: int = Field(
        default=DEFAULT_API_REQUEST_BODY_MAX_BUFFERED_BYTES,
        ge=API_MAX_REQUEST_BODY_BYTES_MIN,
        le=API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX,
    )
    api_request_body_tenant_max_concurrent: int = Field(
        default=DEFAULT_API_REQUEST_BODY_TENANT_MAX_CONCURRENT,
        ge=1,
        le=API_REQUEST_BODY_MAX_CONCURRENT_MAX,
    )
    api_request_body_tenant_max_buffered_bytes: int = Field(
        default=DEFAULT_API_REQUEST_BODY_TENANT_MAX_BUFFERED_BYTES,
        ge=API_MAX_REQUEST_BODY_BYTES_MIN,
        le=API_REQUEST_BODY_MAX_BUFFERED_BYTES_MAX,
    )
    api_request_body_memory_amplification_factor: int = Field(
        default=DEFAULT_API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR,
        ge=API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MIN,
        le=API_REQUEST_BODY_MEMORY_AMPLIFICATION_FACTOR_MAX,
    )
    api_request_body_memory_floor_bytes: int = Field(
        default=DEFAULT_API_REQUEST_BODY_MEMORY_FLOOR_BYTES,
        ge=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MIN,
        le=API_REQUEST_BODY_MEMORY_FLOOR_BYTES_MAX,
    )
    api_request_body_read_timeout_seconds: float = Field(
        default=DEFAULT_API_REQUEST_BODY_READ_TIMEOUT_SECONDS,
        ge=API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MIN,
        le=API_REQUEST_BODY_READ_TIMEOUT_SECONDS_MAX,
        allow_inf_nan=False,
    )
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

    @field_validator("llm_bedrock_region", mode="before")
    @classmethod
    def _canonicalize_llm_bedrock_region(cls, value: object) -> str:
        return canonical_aws_region(value)

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

    @field_validator("api_cors_allowed_origins", mode="before")
    @classmethod
    def _canonicalize_api_cors_allowed_origins(cls, value: object) -> str:
        return canonical_cors_allowed_origins(value)

    @field_validator("api_allowed_hosts", mode="before")
    @classmethod
    def _canonicalize_api_allowed_hosts(cls, value: object) -> str:
        return canonical_api_allowed_hosts(value)

    @model_validator(mode="after")
    def _validate_authenticated_cors(self) -> Settings:
        if self.api_auth_enabled and "*" in self.api_cors_allowed_origins.split(","):
            raise ValueError("Authenticated API deployments cannot use wildcard CORS")
        return self

    @model_validator(mode="after")
    def _validate_request_body_admission(self) -> Settings:
        required_memory = max(
            self.api_request_body_memory_floor_bytes,
            self.api_max_request_body_bytes
            * max(
                self.api_request_body_memory_amplification_factor,
                API_REQUEST_BODY_JSON_MEMORY_AMPLIFICATION_FACTOR,
            ),
        )
        if self.api_request_body_max_buffered_bytes < required_memory:
            raise ValueError(
                "api_request_body_max_buffered_bytes must admit the configured maximum request memory envelope"
            )
        if self.api_request_body_tenant_max_buffered_bytes < required_memory:
            raise ValueError(
                "api_request_body_tenant_max_buffered_bytes must admit the configured maximum request memory envelope"
            )
        wildcard_tenancy = self.knowledge_tenant_id == "*"
        if self.api_request_body_tenant_max_buffered_bytes > self.api_request_body_max_buffered_bytes:
            raise ValueError(
                "api_request_body_tenant_max_buffered_bytes must not exceed api_request_body_max_buffered_bytes"
            )
        if wildcard_tenancy and (
            self.api_request_body_tenant_max_buffered_bytes >= self.api_request_body_max_buffered_bytes
        ):
            raise ValueError(
                "api_request_body_tenant_max_buffered_bytes must be lower than "
                "api_request_body_max_buffered_bytes for wildcard tenancy"
            )
        if self.api_request_body_tenant_max_concurrent > self.api_request_body_max_concurrent:
            raise ValueError("api_request_body_tenant_max_concurrent must not exceed api_request_body_max_concurrent")
        if wildcard_tenancy and self.api_request_body_tenant_max_concurrent >= self.api_request_body_max_concurrent:
            raise ValueError(
                "api_request_body_tenant_max_concurrent must be lower than "
                "api_request_body_max_concurrent for wildcard tenancy"
            )
        retained_request_memory = pipeline_retained_request_memory_bound(
            max_concurrent=self.pipeline_max_concurrent,
            max_queued=self.pipeline_max_queued,
        )
        if retained_request_memory > self.api_request_body_max_buffered_bytes:
            raise ValueError("api_request_body_max_buffered_bytes must cover pipeline retained request memory")
        return self

    @model_validator(mode="after")
    def _validate_bedrock_compatibility_capacity(self) -> Settings:
        if (
            self.llm_provider.strip().casefold() == "bedrock"
            and self.pipeline_max_concurrent > BEDROCK_COMPATIBILITY_MAX_CONCURRENT
        ):
            raise ValueError(
                "Bedrock compatibility bridge limits pipeline_max_concurrent to "
                f"{BEDROCK_COMPATIBILITY_MAX_CONCURRENT}"
            )
        return self

    @model_validator(mode="after")
    def _validate_explicit_aws_credentials(self) -> Settings:
        access_key = str(self.llm_aws_access_key_id or "").strip()
        secret_key = str(self.llm_aws_secret_access_key or "").strip()
        session_token = str(self.llm_aws_session_token or "").strip()
        if bool(access_key) != bool(secret_key) or (session_token and not (access_key and secret_key)):
            raise ValueError("AWS credentials must include both access key and secret key")
        return self

    @model_validator(mode="after")
    def _validate_tenant_api_keys(self) -> Settings:
        if self.knowledge_tenant_id != "*":
            return self
        if not self.api_auth_enabled:
            raise ValueError("Wildcard knowledge tenancy requires API authentication")
        non_empty_keys = [value for value in self.knowledge_tenant_api_keys.values() if value]
        if len(non_empty_keys) != len(set(non_empty_keys)):
            raise ValueError("knowledge_tenant_api_keys must use a unique non-empty key per tenant")
        active_per_tenant = self.pipeline_max_concurrent_per_tenant or max(1, self.pipeline_max_concurrent - 1)
        if active_per_tenant > self.pipeline_max_concurrent or (
            self.pipeline_max_concurrent > 1 and active_per_tenant == self.pipeline_max_concurrent
        ):
            raise ValueError(
                "pipeline_max_concurrent_per_tenant must be lower than " "pipeline_max_concurrent for wildcard tenancy"
            )
        if self.pipeline_max_queued > 0 and self.pipeline_max_queued_per_tenant >= self.pipeline_max_queued:
            raise ValueError(
                "pipeline_max_queued_per_tenant must be lower than pipeline_max_queued " "for wildcard tenancy"
            )
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
