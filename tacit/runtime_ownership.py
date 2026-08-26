"""Typed, side-effect-free runtime ownership identities.

The descriptor in this module is the public composition contract for Tacit
components. It contains only canonical paths, non-secret endpoint/account
identities, and one-way fingerprints. Constructing or comparing descriptors
must never initialize a store or contact a remote service.
"""

from __future__ import annotations

import asyncio
import configparser
import errno
import hashlib
import ipaddress
import json
import os
import re
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

import structlog

import tacit.config as config_module
from tacit.config import (
    Settings,
    canonical_knowledge_permissions,
    canonical_knowledge_tenant_id,
    canonical_sqlite_role_paths,
    validate_distinct_sqlite_role_paths,
    validated_knowledge_tenant_api_keys,
)
from tacit.config import (
    canonical_signalfx_realm as _canonical_signalfx_realm,
)
from tacit.errors import RuntimeOwnershipError

logger = structlog.get_logger()

_FACTORY_REALIZATION_DEPTH: ContextVar[int] = ContextVar(
    "tacit_factory_realization_depth",
    default=0,
)
_FACTORY_REALIZATION_OBSERVATIONS: ContextVar[int] = ContextVar(
    "tacit_factory_realization_observations",
    default=0,
)

_FINGERPRINT_PREFIX = "sha256:"
_SECRET_FIELD_MARKERS = (
    "api_key",
    "api_token",
    "access_key",
    "password",
    "secret",
    "signing_key",
    "tenant_api_keys",
    "token",
)
_REASON_CODE_RE = re.compile(r"[^a-z0-9_.:-]+")
_DATABASE_SETTING_DEFAULTS = {
    "history_db_path": config_module.DEFAULT_HISTORY_DB_PATH,
    "feedback_db_path": config_module.DEFAULT_FEEDBACK_DB_PATH,
    "signals_db_path": config_module.DEFAULT_SIGNALS_DB_PATH,
}
_DATABASE_ROLE_SETTING_FIELDS = {
    "history": "history_db_path",
    "feedback": "feedback_db_path",
    "signals": "signals_db_path",
}
_PATH_SETTING_FIELDS = (
    "evaluation_results_dir",
    "learned_archetypes_quarantine_path",
)
_ENDPOINT_SETTING_FIELDS = (
    "context_a2a_agent_url",
    "context_mcp_server_url",
    "context_rag_api_url",
    "grafana_public_url",
    "grafana_url",
    "llm_api_base",
    "pagerduty_base_url",
)
_CASEFOLD_SETTING_FIELDS = (
    "context_provider",
    "llm_bedrock_region",
    "llm_provider",
    "log_level",
    "signalfx_realm",
)
_STRIPPED_SETTING_FIELDS = ("learned_archetypes_tenant_id",)
_OPENAI_API_ENDPOINT = "https://api.openai.com/v1"
_ANTHROPIC_API_ENDPOINT = "https://api.anthropic.com"
_OLLAMA_API_ENDPOINT = "http://localhost:11434"
DEFAULT_RUNTIME_CLEANUP_GRACE_SECONDS = 5.0
_BEDROCK_IDENTITY_ENVIRONMENT_FIELDS = (
    "HOME",
    "AWS_ACCESS_KEY_ID",
    "AWS_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SECRET_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_SECURITY_TOKEN",
    "AWS_PROFILE",
    "AWS_DEFAULT_PROFILE",
    "AWS_ROLE_ARN",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_SESSION_NAME",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
    "AWS_ENDPOINT_URL",
    "AWS_ENDPOINT_URL_STS",
    "AWS_STS_REGIONAL_ENDPOINTS",
    "AWS_USE_FIPS_ENDPOINT",
    "AWS_USE_DUALSTACK_ENDPOINT",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN",
    "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
    "AWS_EC2_METADATA_DISABLED",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT",
    "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE",
)
_BEDROCK_CREDENTIAL_SOURCE_MAX_BYTES = 4 * 1024 * 1024


def validate_runtime_cleanup_grace_seconds(value: float) -> float:
    """Return one finite cleanup grace shared by every runtime resource."""
    grace = float(value)
    if not 0 < grace <= 300:
        raise ValueError("pipeline cleanup grace must be between 0 and 300 seconds")
    return grace


def _aws_dns_suffix(region: str) -> str:
    return "amazonaws.com.cn" if region.startswith("cn-") else "amazonaws.com"


def canonical_bedrock_runtime_endpoint(region: str) -> str:
    normalized = str(region or "").strip().casefold()
    if not normalized:
        raise RuntimeOwnershipError("AWS Bedrock region is required")
    return f"https://bedrock-runtime.{normalized}.{_aws_dns_suffix(normalized)}"


def canonical_aws_sts_endpoint(region: str) -> str:
    normalized = str(region or "").strip().casefold()
    if not normalized:
        raise RuntimeOwnershipError("AWS STS region is required")
    return f"https://sts.{normalized}.{_aws_dns_suffix(normalized)}"


def capture_bedrock_environment() -> dict[str, str]:
    """Capture ambient AWS identity inputs once for one provider generation."""
    return {name: str(os.environ[name]) for name in _BEDROCK_IDENTITY_ENVIRONMENT_FIELDS if name in os.environ}


def _bedrock_environment_value(environment: Mapping[str, str], name: str) -> str:
    if name not in environment:
        return ""
    value = str(environment[name])
    if not value or value != value.strip():
        raise RuntimeOwnershipError("AWS credential environment value is invalid")
    return value


def _validate_bedrock_sts_environment(
    environment: Mapping[str, str],
    *,
    present_fields: tuple[str, ...] | None = None,
) -> None:
    present = set(environment) if present_fields is None else set(present_fields)
    endpoint_override = bool({"AWS_ENDPOINT_URL_STS", "AWS_ENDPOINT_URL"} & present)
    sts_endpoint_mode = (
        _bedrock_environment_value(environment, "AWS_STS_REGIONAL_ENDPOINTS").casefold()
        if "AWS_STS_REGIONAL_ENDPOINTS" in present
        else ""
    )
    fips = (
        _bedrock_environment_value(environment, "AWS_USE_FIPS_ENDPOINT").casefold()
        if "AWS_USE_FIPS_ENDPOINT" in present
        else ""
    )
    dualstack = (
        _bedrock_environment_value(environment, "AWS_USE_DUALSTACK_ENDPOINT").casefold()
        if "AWS_USE_DUALSTACK_ENDPOINT" in present
        else ""
    )
    enabled_values = {"1", "true", "yes", "on"}
    if (
        endpoint_override
        or sts_endpoint_mode not in {"", "regional"}
        or fips in enabled_values
        or dualstack in enabled_values
    ):
        raise RuntimeOwnershipError("Ambient AWS STS endpoint overrides are not supported")


def bedrock_credential_selector(
    runtime_settings: Settings,
    *,
    environment: Mapping[str, str] | None = None,
) -> tuple[str, str, str]:
    """Return a non-secret stable account, credential, and profile selector."""
    environment = os.environ if environment is None else environment
    role_arn = str(runtime_settings.llm_bedrock_role_arn or "").strip()
    explicit_access_key = str(runtime_settings.llm_aws_access_key_id or "").strip()
    explicit_secret_key = str(runtime_settings.llm_aws_secret_access_key or "").strip()
    profile = ""
    if explicit_access_key or explicit_secret_key:
        if not explicit_access_key or not explicit_secret_key:
            raise RuntimeOwnershipError("AWS credentials must include both access key and secret key")
        credential_identity = f"settings-access-key:{explicit_access_key}"
        selector = f"access-key:{credential_fingerprint(explicit_access_key)}"
    else:
        ambient_access_key = _bedrock_environment_value(environment, "AWS_ACCESS_KEY_ID")
        if ambient_access_key:
            ambient_secret_key = _bedrock_environment_value(environment, "AWS_SECRET_ACCESS_KEY")
            if not ambient_secret_key:
                raise RuntimeOwnershipError("AWS credentials must include both access key and secret key")
            credential_identity = f"environment-access-key:{ambient_access_key}"
            selector = f"access-key:{credential_fingerprint(ambient_access_key)}"
        else:
            if "AWS_DEFAULT_PROFILE" in environment:
                profile = _bedrock_environment_value(environment, "AWS_DEFAULT_PROFILE")
            else:
                profile = _bedrock_environment_value(environment, "AWS_PROFILE")
            if profile:
                credential_identity = f"profile:{profile}"
                selector = f"profile:{profile}"
            else:
                credential_identity = "aws-default-chain"
                selector = "default-chain"

    account = role_arn or selector
    if role_arn:
        credential_identity = f"{credential_identity}\0assume-role:{role_arn}"
    return account, credential_identity, profile


@dataclass(frozen=True, slots=True)
class BedrockCredentialIdentity:
    """Public identity derived from one frozen AWS credential snapshot."""

    account: str
    credential_fingerprint: str
    uses_sts: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.account, str) or not self.account.strip():
            raise RuntimeOwnershipError("AWS credential account identity is invalid")
        if not _is_fingerprint(self.credential_fingerprint):
            raise RuntimeOwnershipError("AWS credential identity must be a non-secret fingerprint")
        if not isinstance(self.uses_sts, bool):
            raise RuntimeOwnershipError("AWS credential STS identity is invalid")
        object.__setattr__(self, "account", self.account.strip().casefold())


@dataclass(frozen=True, slots=True)
class _BedrockCredentialSourceIdentity:
    """Non-secret identity for one local AWS credential input."""

    kind: str
    path: Path = field(repr=False)
    exists: bool
    device: int
    inode: int
    size: int
    mtime_ns: int
    fingerprint: str


def _bedrock_source_path(raw_path: str, *, default_path: Path) -> Path:
    selected = Path(raw_path).expanduser() if raw_path else default_path
    if not selected.is_absolute():
        raise RuntimeOwnershipError("AWS credential source paths must be absolute")
    return Path(os.path.abspath(selected))


def _snapshot_bedrock_credential_source(
    *,
    kind: str,
    path: Path,
) -> tuple[_BedrockCredentialSourceIdentity, bytes]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return (
            _BedrockCredentialSourceIdentity(
                kind=kind,
                path=path,
                exists=False,
                device=0,
                inode=0,
                size=0,
                mtime_ns=0,
                fingerprint=credential_fingerprint(f"missing:{kind}"),
            ),
            b"",
        )
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise RuntimeOwnershipError("AWS credential sources must be regular files") from None
        raise RuntimeOwnershipError("AWS credential source cannot be inspected") from exc

    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RuntimeOwnershipError("AWS credential sources must be regular files")
        if before.st_size > _BEDROCK_CREDENTIAL_SOURCE_MAX_BYTES:
            raise RuntimeOwnershipError("AWS credential source exceeds the supported size")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, _BEDROCK_CREDENTIAL_SOURCE_MAX_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _BEDROCK_CREDENTIAL_SOURCE_MAX_BYTES:
                raise RuntimeOwnershipError("AWS credential source exceeds the supported size")
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)

    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity or total != after.st_size:
        raise RuntimeOwnershipError("AWS credential source changed during capture")
    content = b"".join(chunks)
    return (
        _BedrockCredentialSourceIdentity(
            kind=kind,
            path=path,
            exists=True,
            device=after.st_dev,
            inode=after.st_ino,
            size=after.st_size,
            mtime_ns=after.st_mtime_ns,
            fingerprint=credential_fingerprint(content),
        ),
        content,
    )


def _bedrock_profile_section(content: bytes, section_name: str) -> dict[str, str]:
    if not content:
        return {}
    parser = configparser.RawConfigParser(interpolation=None)
    try:
        parser.read_string(content.decode("utf-8-sig"))
    except (UnicodeDecodeError, configparser.Error) as exc:
        raise RuntimeOwnershipError("AWS credential source metadata is invalid") from exc
    if not parser.has_section(section_name):
        return {}
    return {str(key).strip().casefold(): str(value).strip() for key, value in parser.items(section_name)}


def _bedrock_profile_metadata(
    *,
    profile: str,
    credentials_content: bytes,
    config_content: bytes,
) -> dict[str, str]:

    selected_profile = profile or "default"
    config_section = "default" if selected_profile == "default" else f"profile {selected_profile}"
    # Botocore merges shared-credentials fields over config fields for the
    # selected profile. Ownership classification must use that same effective
    # profile or the SDK could follow a different role/source chain.
    metadata = _bedrock_profile_section(config_content, config_section)
    metadata.update(_bedrock_profile_section(credentials_content, selected_profile))
    return metadata


_BEDROCK_UNMODELED_PROFILE_FIELDS = frozenset(
    {
        "credential_process",
        "credential_source",
        "login_session",
        "mfa_serial",
        "sso_account_id",
        "sso_region",
        "sso_role_name",
        "sso_session",
        "sso_start_url",
    }
)


def _bedrock_static_profile_method(
    *,
    profile: str,
    credentials_content: bytes,
    config_content: bytes,
) -> str | None:
    selected_profile = profile or "default"
    config_section = "default" if selected_profile == "default" else f"profile {selected_profile}"
    # Botocore checks these as two providers in this order. It does not merge
    # an access key from one file with a secret key from the other.
    providers = (
        ("shared-credentials-file", _bedrock_profile_section(credentials_content, selected_profile)),
        ("config-file", _bedrock_profile_section(config_content, config_section)),
    )
    for method, metadata in providers:
        if "aws_access_key_id" not in metadata:
            continue
        access_key = str(metadata.get("aws_access_key_id") or "").strip()
        secret_key = str(metadata.get("aws_secret_access_key") or "").strip()
        if not access_key or not secret_key:
            raise RuntimeOwnershipError("AWS profile credentials must include both access key and secret key")
        return method
    return None


def _reject_unmodeled_bedrock_profile(metadata: Mapping[str, str]) -> None:
    # Botocore uses key presence, not truthiness, for several provider
    # selectors. A blank higher-precedence value must therefore remain a
    # rejected provider declaration rather than being treated as absent.
    if any(field in metadata for field in _BEDROCK_UNMODELED_PROFILE_FIELDS):
        raise RuntimeOwnershipError("AWS Bedrock credential provider is unsupported by runtime ownership")


@dataclass(frozen=True, slots=True)
class BedrockCredentialPlan:
    """Immutable selector, local-source, and remote plan for one provider generation."""

    _runtime_settings: Settings = field(repr=False)
    _environment_items: tuple[tuple[str, str], ...] = field(repr=False)
    _environment_presence: tuple[str, ...] = field(repr=False)
    _source_identities: tuple[_BedrockCredentialSourceIdentity, ...] = field(repr=False)
    _source_contents: tuple[tuple[str, bytes], ...] = field(repr=False)
    _declared_identity: BedrockCredentialIdentity
    _profile: str
    _source_uses_sts: bool
    _web_identity_role_arn: str
    _web_identity_role_session_name: str
    _discovery_methods: tuple[str, ...]

    @classmethod
    def capture(
        cls,
        runtime_settings: Settings,
        *,
        environment: Mapping[str, str] | None = None,
    ) -> BedrockCredentialPlan:
        frozen_settings = snapshot_runtime_settings(runtime_settings)
        captured = capture_bedrock_environment() if environment is None else dict(environment)
        environment_presence = tuple(sorted(name for name in _BEDROCK_IDENTITY_ENVIRONMENT_FIELDS if name in captured))
        frozen_environment = {name: str(captured.get(name) or "") for name in _BEDROCK_IDENTITY_ENVIRONMENT_FIELDS}
        account, selector_identity, profile = bedrock_credential_selector(
            frozen_settings,
            environment=captured,
        )
        explicit_settings = bool(frozen_settings.llm_aws_access_key_id or frozen_settings.llm_aws_secret_access_key)
        explicit_environment = bool(str(frozen_environment.get("AWS_ACCESS_KEY_ID") or ""))
        if explicit_environment:
            for token_name in ("AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN"):
                if token_name in environment_presence:
                    _bedrock_environment_value(captured, token_name)
        source_identities: list[_BedrockCredentialSourceIdentity] = []
        source_contents: list[tuple[str, bytes]] = []
        profile_metadata: dict[str, str] = {}
        credentials_content = b""
        config_content = b""
        if not explicit_settings and not explicit_environment:
            raw_credentials_path = (
                _bedrock_environment_value(captured, "AWS_SHARED_CREDENTIALS_FILE")
                if "AWS_SHARED_CREDENTIALS_FILE" in environment_presence
                else ""
            )
            raw_config_path = (
                _bedrock_environment_value(captured, "AWS_CONFIG_FILE")
                if "AWS_CONFIG_FILE" in environment_presence
                else ""
            )
            home_path: Path | None = None
            if not raw_credentials_path or not raw_config_path:
                home = _bedrock_environment_value(captured, "HOME")
                if not home:
                    raise RuntimeOwnershipError("AWS credential source home is unavailable")
                home_path = Path(home)
                if not home_path.is_absolute():
                    raise RuntimeOwnershipError("AWS credential source home must be absolute")
            credentials_path = _bedrock_source_path(
                raw_credentials_path,
                default_path=(home_path / ".aws" / "credentials") if home_path else Path(raw_credentials_path),
            )
            config_path = _bedrock_source_path(
                raw_config_path,
                default_path=(home_path / ".aws" / "config") if home_path else Path(raw_config_path),
            )
            credentials_identity, credentials_content = _snapshot_bedrock_credential_source(
                kind="shared_credentials",
                path=credentials_path,
            )
            config_identity, config_content = _snapshot_bedrock_credential_source(
                kind="config",
                path=config_path,
            )
            source_identities.extend((credentials_identity, config_identity))
            source_contents.extend(
                (
                    (credentials_identity.kind, credentials_content),
                    (config_identity.kind, config_content),
                )
            )
            profile_metadata = _bedrock_profile_metadata(
                profile=profile,
                credentials_content=credentials_content,
                config_content=config_content,
            )

        _reject_unmodeled_bedrock_profile(profile_metadata)

        profile_web_identity_declared = "web_identity_token_file" in profile_metadata
        profile_role_declared = "role_arn" in profile_metadata
        profile_web_identity_token = str(profile_metadata.get("web_identity_token_file") or "").strip()
        profile_role_arn = str(profile_metadata.get("role_arn") or "").strip()
        source_profile = str(profile_metadata.get("source_profile") or "").strip()
        web_identity_role_arn = ""
        web_identity_role_session_name = ""
        if explicit_settings or explicit_environment:
            discovery_methods: tuple[str, ...] = ()
        elif profile_role_declared and not profile_web_identity_declared:
            if not profile_role_arn:
                raise RuntimeOwnershipError("AWS Bedrock role profile ARN is invalid")
            if not source_profile:
                raise RuntimeOwnershipError("AWS Bedrock role profile requires a static source profile")
            source_metadata = _bedrock_profile_metadata(
                profile=source_profile,
                credentials_content=credentials_content,
                config_content=config_content,
            )
            _reject_unmodeled_bedrock_profile(source_metadata)
            source_static_method = _bedrock_static_profile_method(
                profile=source_profile,
                credentials_content=credentials_content,
                config_content=config_content,
            )
            if (
                "role_arn" in source_metadata
                or "web_identity_token_file" in source_metadata
                or source_static_method is None
            ):
                raise RuntimeOwnershipError("AWS Bedrock role source profile is unsupported by runtime ownership")
            if (
                "role_session_name" in profile_metadata
                and not str(profile_metadata.get("role_session_name") or "").strip()
            ):
                raise RuntimeOwnershipError("AWS Bedrock role session name is invalid")
            discovery_methods = ("assume-role",)
        else:
            ambient_token_present = "AWS_WEB_IDENTITY_TOKEN_FILE" in environment_presence
            ambient_role_present = "AWS_ROLE_ARN" in environment_presence
            ambient_session_name_present = "AWS_ROLE_SESSION_NAME" in environment_presence
            selected_web_identity_token = str(
                _bedrock_environment_value(captured, "AWS_WEB_IDENTITY_TOKEN_FILE")
                if ambient_token_present
                else profile_web_identity_token
            )
            if profile_web_identity_declared and not profile_web_identity_token and not ambient_token_present:
                raise RuntimeOwnershipError("AWS web identity profile token file is invalid")
            if selected_web_identity_token:
                web_identity_role_arn = str(
                    _bedrock_environment_value(captured, "AWS_ROLE_ARN") if ambient_role_present else profile_role_arn
                )
                if not web_identity_role_arn:
                    raise RuntimeOwnershipError("AWS web identity profile requires a role ARN")
                profile_session_name_present = "role_session_name" in profile_metadata
                web_identity_role_session_name = str(
                    _bedrock_environment_value(captured, "AWS_ROLE_SESSION_NAME")
                    if ambient_session_name_present
                    else profile_metadata.get("role_session_name") or ""
                ).strip()
                if (
                    ambient_session_name_present or profile_session_name_present
                ) and not web_identity_role_session_name:
                    raise RuntimeOwnershipError("AWS web identity role session name is invalid")
                _validate_bedrock_sts_environment(
                    frozen_environment,
                    present_fields=environment_presence,
                )
                token_path = _bedrock_source_path(
                    selected_web_identity_token,
                    default_path=Path(selected_web_identity_token),
                )
                token_identity, token_content = _snapshot_bedrock_credential_source(
                    kind="web_identity_token",
                    path=token_path,
                )
                if not token_identity.exists or not token_content:
                    raise RuntimeOwnershipError("AWS web identity token is unavailable")
                source_identities.append(token_identity)
                source_contents.append((token_identity.kind, token_content))
                discovery_methods = ("assume-role-with-web-identity",)
            else:
                static_method = _bedrock_static_profile_method(
                    profile=profile,
                    credentials_content=credentials_content,
                    config_content=config_content,
                )
                if static_method is None:
                    raise RuntimeOwnershipError("AWS Bedrock credential provider is unsupported by runtime ownership")
                discovery_methods = (static_method,)
        source_uses_sts = bool(
            discovery_methods and discovery_methods[0] in {"assume-role", "assume-role-with-web-identity"}
        )
        if source_uses_sts and frozen_settings.llm_bedrock_role_arn:
            raise RuntimeOwnershipError("AWS Bedrock chained role assumption is unsupported")
        uses_sts = bool(frozen_settings.llm_bedrock_role_arn) or source_uses_sts
        if source_uses_sts:
            _validate_bedrock_sts_environment(
                frozen_environment,
                present_fields=environment_presence,
            )
        declared_account = str(
            frozen_settings.llm_bedrock_role_arn
            or (profile_role_arn if discovery_methods == ("assume-role",) else "")
            or web_identity_role_arn
            or account
        ).strip()
        fingerprint_material = [selector_identity]
        fingerprint_material.extend(f"{item.kind}:{item.fingerprint}" for item in source_identities)
        if explicit_settings:
            fingerprint_material.extend(
                (
                    str(frozen_settings.llm_aws_access_key_id or ""),
                    str(frozen_settings.llm_aws_secret_access_key or ""),
                )
            )
        elif explicit_environment:
            fingerprint_material.extend(
                (
                    str(frozen_environment.get("AWS_ACCESS_KEY_ID") or ""),
                    str(frozen_environment.get("AWS_SECRET_ACCESS_KEY") or ""),
                    str(
                        frozen_environment.get("AWS_SECURITY_TOKEN")
                        or frozen_environment.get("AWS_SESSION_TOKEN")
                        or ""
                    ),
                )
            )
        return cls(
            _runtime_settings=frozen_settings,
            _environment_items=tuple(sorted(frozen_environment.items())),
            _environment_presence=environment_presence,
            _source_identities=tuple(source_identities),
            _source_contents=tuple(source_contents),
            _declared_identity=BedrockCredentialIdentity(
                account=declared_account,
                credential_fingerprint=credential_fingerprint("\0".join(fingerprint_material)),
                uses_sts=uses_sts,
            ),
            _profile=profile,
            _source_uses_sts=source_uses_sts,
            _web_identity_role_arn=web_identity_role_arn,
            _web_identity_role_session_name=web_identity_role_session_name,
            _discovery_methods=discovery_methods,
        )

    @property
    def runtime_settings(self) -> Settings:
        """Return a detached copy of the generation's settings snapshot."""
        return copy_runtime_settings(self._runtime_settings)

    @property
    def environment(self) -> dict[str, str]:
        """Return a detached copy of the generation's ambient AWS snapshot."""
        values = dict(self._environment_items)
        return {name: values[name] for name in self._environment_presence}

    @property
    def profile(self) -> str:
        """Return the selected named profile, or an empty value for default."""
        return self._profile

    @property
    def source_uses_sts(self) -> bool:
        """Whether the captured source itself requires STS during freezing."""
        return self._source_uses_sts

    @property
    def web_identity_role_arn(self) -> str:
        """Return the effective role ARN for an admitted web-identity source."""
        return self._web_identity_role_arn

    @property
    def web_identity_role_session_name(self) -> str:
        """Return the effective optional session name for web identity."""
        return self._web_identity_role_session_name

    def has_source(self, kind: str) -> bool:
        """Return whether the plan captured one local source kind."""
        return any(source_kind == kind for source_kind, _content in self._source_contents)

    @property
    def discovery_methods(self) -> tuple[str, ...]:
        """Return the Botocore provider methods admitted by this plan."""
        return self._discovery_methods

    def source_content(self, kind: str) -> bytes:
        """Return the captured bytes for one local AWS credential source."""
        for source_kind, content in self._source_contents:
            if source_kind == kind:
                return bytes(content)
        return b""

    @property
    def uses_sts(self) -> bool:
        """Whether this generation declares any STS authority."""
        return self._declared_identity.uses_sts

    @property
    def account(self) -> str:
        """Return the non-secret account or selector bound to the plan."""
        return self._declared_identity.account

    def verify_unchanged(self) -> None:
        """Fail before SDK use when ambient or local credential inputs moved."""
        current_environment = capture_bedrock_environment()
        current_presence = tuple(
            sorted(name for name in _BEDROCK_IDENTITY_ENVIRONMENT_FIELDS if name in current_environment)
        )
        normalized_current = {
            name: str(current_environment.get(name) or "") for name in _BEDROCK_IDENTITY_ENVIRONMENT_FIELDS
        }
        if (
            tuple(sorted(normalized_current.items())) != self._environment_items
            or current_presence != self._environment_presence
        ):
            raise RuntimeOwnershipError("AWS Bedrock credential environment changed after plan capture")
        for expected in self._source_identities:
            current, _content = _snapshot_bedrock_credential_source(
                kind=expected.kind,
                path=expected.path,
            )
            if current != expected:
                raise RuntimeOwnershipError("AWS Bedrock credential source changed after plan capture")

    def realized_identity(
        self,
        *,
        fallback_account: str,
        credential_fingerprint_value: str,
        source_uses_sts: bool,
    ) -> BedrockCredentialIdentity:
        """Refine this plan with credentials produced by its admitted source."""
        if source_uses_sts != self._source_uses_sts:
            raise RuntimeOwnershipError("AWS Bedrock credential source no longer matches its declared remote plan")
        account = self.account if self.uses_sts else fallback_account
        return BedrockCredentialIdentity(
            account=account,
            credential_fingerprint=credential_fingerprint_value,
            uses_sts=self.uses_sts,
        )

    def ownership(
        self,
        *,
        component: str,
        credential_identity: BedrockCredentialIdentity | None = None,
    ) -> RuntimeOwnershipDescriptor:
        """Describe this exact plan, refined by its realized credential identity."""
        identity = credential_identity or self._declared_identity
        if identity.uses_sts != self._declared_identity.uses_sts:
            raise RuntimeOwnershipError("AWS Bedrock credential identity does not match its declared remote plan")
        return runtime_descriptor_for_provider(
            component=component,
            runtime_settings=self._runtime_settings,
            capability="llm",
            bedrock_environment=self.environment,
            bedrock_credential_identity=identity,
        )


class RuntimeOwnershipMismatchError(RuntimeOwnershipError):
    """Raised when supplied runtime owners identify different compositions."""

    def __init__(
        self,
        boundary: str,
        dimensions: set[str],
        components: tuple[str, ...],
        *,
        message: str | None = None,
    ) -> None:
        self.boundary = boundary
        self.dimensions = frozenset(dimensions)
        self.components = components
        joined = ", ".join(sorted(self.dimensions))
        super().__init__(message or f"{boundary} runtime ownership mismatch: {joined}")


class RuntimeAvailability(StrEnum):
    """Whether an owner was supplied and can be consumed."""

    ABSENT = "absent"
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class RuntimeTenantPolicy:
    """Tenant and semantic-permission identity without credential disclosure."""

    mode: str
    tenant_id: str
    permissions: tuple[str, ...]
    api_auth_enabled: bool
    tenant_credential_fingerprints: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.mode, str):
            raise RuntimeOwnershipError("runtime tenant policy mode is invalid")
        if not isinstance(self.tenant_id, str):
            raise RuntimeOwnershipError("runtime tenant identity is invalid")
        if not isinstance(self.permissions, tuple) or any(
            not isinstance(permission, str) for permission in self.permissions
        ):
            raise RuntimeOwnershipError("runtime permission identity is invalid")
        if not isinstance(self.tenant_credential_fingerprints, tuple) or any(
            not isinstance(item, tuple)
            or len(item) != 2
            or not isinstance(item[0], str)
            or not isinstance(item[1], str)
            for item in self.tenant_credential_fingerprints
        ):
            raise RuntimeOwnershipError("runtime tenant credential identity is invalid")
        try:
            tenant_id = canonical_knowledge_tenant_id(self.tenant_id)
            permission_identity = canonical_knowledge_permissions(",".join(self.permissions))
            credential_names = validated_knowledge_tenant_api_keys(
                {tenant: fingerprint for tenant, fingerprint in self.tenant_credential_fingerprints}
            )
        except ValueError as exc:
            raise RuntimeOwnershipError(str(exc)) from exc
        if self.mode not in {"pinned", "wildcard"}:
            raise RuntimeOwnershipError("runtime tenant policy mode is invalid")
        expected_mode = "wildcard" if tenant_id == "*" else "pinned"
        if self.mode != expected_mode:
            raise RuntimeOwnershipError("runtime tenant policy mode conflicts with tenant identity")
        if not isinstance(self.api_auth_enabled, bool):
            raise RuntimeOwnershipError("runtime tenant authentication policy is invalid")
        if len(credential_names) != len(self.tenant_credential_fingerprints):
            raise RuntimeOwnershipError("runtime tenant credential identity is duplicated")
        credentials = tuple(sorted(self.tenant_credential_fingerprints))
        if any(not _is_fingerprint(fingerprint) and fingerprint != "none" for _tenant, fingerprint in credentials):
            raise RuntimeOwnershipError("runtime tenant credential identity must be a non-secret fingerprint")
        if expected_mode == "wildcard" and not self.api_auth_enabled:
            raise RuntimeOwnershipError("Wildcard knowledge tenancy requires API authentication")
        non_empty_fingerprints = [fingerprint for _tenant, fingerprint in credentials if fingerprint != "none"]
        if expected_mode == "wildcard" and len(non_empty_fingerprints) != len(set(non_empty_fingerprints)):
            raise RuntimeOwnershipError("knowledge_tenant_api_keys must use a unique non-empty key per tenant")
        permissions = tuple(permission_identity.split(",")) if permission_identity else ()
        object.__setattr__(self, "tenant_id", tenant_id)
        object.__setattr__(self, "permissions", permissions)
        object.__setattr__(self, "tenant_credential_fingerprints", credentials)


@dataclass(frozen=True, slots=True)
class RuntimeDatabaseIdentity:
    """One role-scoped SQLite identity.

    Different roles are intentionally allowed to use different files. Two
    owners conflict only when they claim the same role with different paths.
    """

    role: str
    path: Path

    def __post_init__(self) -> None:
        if not isinstance(self.role, str):
            raise RuntimeOwnershipError("runtime database role is invalid")
        object.__setattr__(self, "role", self.role.strip().casefold())
        object.__setattr__(self, "path", _canonical_path(self.path))
        if not self.role:
            raise RuntimeOwnershipError("runtime database role is required")


@dataclass(frozen=True, slots=True)
class RuntimeRemoteIdentity:
    """Effective remote identity after client overrides have been applied."""

    provider: str
    endpoint: str
    account: str = ""
    credential_fingerprint: str = "none"

    def __post_init__(self) -> None:
        if not isinstance(self.provider, str):
            raise RuntimeOwnershipError("runtime remote provider is invalid")
        if not isinstance(self.credential_fingerprint, str):
            raise RuntimeOwnershipError("runtime credential identity is invalid")
        object.__setattr__(self, "provider", self.provider.strip().casefold())
        object.__setattr__(self, "endpoint", canonical_remote_endpoint(self.endpoint))
        object.__setattr__(self, "account", str(self.account).strip().casefold())
        if not self.provider:
            raise RuntimeOwnershipError("runtime remote provider is required")
        fingerprint = self.credential_fingerprint
        if fingerprint != "none" and not (
            fingerprint.startswith(_FINGERPRINT_PREFIX)
            and len(fingerprint) == len(_FINGERPRINT_PREFIX) + 64
            and all(character in "0123456789abcdef" for character in fingerprint[len(_FINGERPRINT_PREFIX) :])
        ):
            raise RuntimeOwnershipError("runtime credential identity must be a non-secret fingerprint")


@dataclass(frozen=True, slots=True)
class RuntimeOwnershipDescriptor:
    """Immutable public identity for one composed runtime component."""

    component: str
    availability: RuntimeAvailability = RuntimeAvailability.AVAILABLE
    settings_identity: str | None = None
    tenant_policy: RuntimeTenantPolicy | None = None
    databases: tuple[RuntimeDatabaseIdentity, ...] = ()
    remotes: tuple[RuntimeRemoteIdentity, ...] = ()
    cache_namespace: str | None = None
    admission_namespace: str | None = None
    availability_reason: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.component, str):
            raise RuntimeOwnershipError("runtime ownership component is invalid")
        if not isinstance(self.availability, RuntimeAvailability):
            raise RuntimeOwnershipError("runtime availability identity is invalid")
        if self.tenant_policy is not None and not isinstance(self.tenant_policy, RuntimeTenantPolicy):
            raise RuntimeOwnershipError("runtime tenant policy identity is invalid")
        if not isinstance(self.databases, tuple) or any(
            not isinstance(database, RuntimeDatabaseIdentity) for database in self.databases
        ):
            raise RuntimeOwnershipError("runtime database identity is invalid")
        if not isinstance(self.remotes, tuple) or any(
            not isinstance(remote, RuntimeRemoteIdentity) for remote in self.remotes
        ):
            raise RuntimeOwnershipError("runtime remote identity is invalid")
        if not isinstance(self.availability_reason, str):
            raise RuntimeOwnershipError("runtime availability reason is invalid")
        component = self.component.strip()
        if not component:
            raise RuntimeOwnershipError("runtime ownership component is required")
        object.__setattr__(self, "component", component)

        exposes_identity = (
            self.settings_identity is not None
            or self.tenant_policy is not None
            or bool(self.databases)
            or bool(self.remotes)
            or self.cache_namespace is not None
            or self.admission_namespace is not None
        )
        reason = _reason_code(self.availability_reason)
        if self.availability is not RuntimeAvailability.AVAILABLE:
            if exposes_identity:
                raise RuntimeOwnershipError("non-available runtime owners must not expose runtime identity")
            if self.availability is RuntimeAvailability.ABSENT:
                if reason:
                    raise RuntimeOwnershipError("absent runtime owners must not include a reason")
            elif not reason:
                raise RuntimeOwnershipError("unavailable runtime owners require a reason")
            object.__setattr__(self, "availability_reason", reason)
            return

        object.__setattr__(self, "databases", _unique_databases(self.databases))
        object.__setattr__(self, "remotes", _unique_remotes(self.remotes))
        object.__setattr__(self, "availability_reason", reason)

        if self.settings_identity is not None and (
            not isinstance(self.settings_identity, str) or not _is_fingerprint(self.settings_identity)
        ):
            raise RuntimeOwnershipError("settings identity must be a non-secret fingerprint")
        for namespace_name, namespace in (
            ("cache", self.cache_namespace),
            ("admission", self.admission_namespace),
        ):
            if namespace is not None and (not isinstance(namespace, str) or not namespace):
                raise RuntimeOwnershipError(f"runtime {namespace_name} namespace is invalid")
        if self.availability is RuntimeAvailability.AVAILABLE and not any(
            (
                self.settings_identity,
                self.tenant_policy,
                self.databases,
                self.remotes,
                self.cache_namespace,
                self.admission_namespace,
            )
        ):
            raise RuntimeOwnershipError("available runtime owners must expose at least one identity dimension")

    @classmethod
    def absent(cls, *, component: str) -> RuntimeOwnershipDescriptor:
        """Represent an owner that was not supplied."""
        return cls(component=component, availability=RuntimeAvailability.ABSENT)

    @classmethod
    def unavailable(cls, *, component: str, reason: str) -> RuntimeOwnershipDescriptor:
        """Represent an explicitly supplied owner that cannot be consumed."""
        return cls(
            component=component,
            availability=RuntimeAvailability.UNAVAILABLE,
            availability_reason=reason,
        )


@dataclass(frozen=True, slots=True)
class RuntimeOwnedFactory[FactoryResult]:
    """Side-effect-free ownership declaration for one lazy factory.

    The declaration is checked before ``factory`` is invoked. The realized
    object is still validated independently because a valid declaration does
    not prove that a factory returned the object it promised.
    """

    factory: Callable[[], FactoryResult] = field(repr=False)
    runtime_ownership: RuntimeOwnershipDescriptor
    factory_kind: str

    def __post_init__(self) -> None:
        if not callable(self.factory):
            raise RuntimeOwnershipError("runtime factory must be callable")
        normalized_kind = _reason_code(self.factory_kind)
        if not normalized_kind:
            raise RuntimeOwnershipError("runtime factory kind is required")
        if self.runtime_ownership.availability is not RuntimeAvailability.AVAILABLE:
            raise RuntimeOwnershipError("runtime factory owner must be available")
        object.__setattr__(self, "factory_kind", normalized_kind)

    def __call__(self) -> FactoryResult:
        return self.factory()


def declare_runtime_factory[FactoryResult](
    factory: Callable[[], FactoryResult],
    *,
    ownership: RuntimeOwnershipDescriptor,
    factory_kind: str,
) -> RuntimeOwnedFactory[FactoryResult]:
    """Attach an immutable public owner to a lazy factory without invoking it."""
    if isinstance(factory, RuntimeOwnedFactory):
        if factory.runtime_ownership != ownership or factory.factory_kind != _reason_code(factory_kind):
            raise RuntimeOwnershipError("runtime factory declaration cannot be replaced")
        return factory
    return RuntimeOwnedFactory(
        factory=factory,
        runtime_ownership=ownership,
        factory_kind=factory_kind,
    )


async def realize_runtime_factory_async[FactoryResult](
    factory: Callable[[], FactoryResult],
) -> FactoryResult:
    """Realize a synchronous dependency factory without blocking its event loop."""
    return await asyncio.to_thread(factory)


def get_runtime_factory_ownership(
    factory: object,
    *,
    expected_kind: str,
) -> RuntimeOwnershipDescriptor:
    """Read a factory declaration without invoking or inspecting its closure."""
    normalized_kind = _reason_code(expected_kind)
    if not isinstance(factory, RuntimeOwnedFactory) or factory.factory_kind != normalized_kind:
        raise RuntimeOwnershipError("injected factory must expose its declared runtime owner and capability")
    return factory.runtime_ownership


def observe_runtime_factory_failure(
    *,
    phase: str,
    factory_kind: str,
    reason_code: str,
    dimensions: set[str] | frozenset[str] = frozenset(),
) -> None:
    """Emit stable ownership diagnostics without sensitive runtime identity."""
    if _FACTORY_REALIZATION_DEPTH.get() > 0:
        _FACTORY_REALIZATION_OBSERVATIONS.set(_FACTORY_REALIZATION_OBSERVATIONS.get() + 1)
    logger.warning(
        "runtime_factory_ownership_failed",
        phase=_reason_code(phase),
        factory_kind=_reason_code(factory_kind),
        reason_code=_reason_code(reason_code),
        dimensions=sorted(_reason_code(item) for item in dimensions),
    )


@contextmanager
def observe_runtime_factory_realization(factory_kind: str) -> Iterator[None]:
    """Record one safe event when a nested declared factory invocation fails."""
    depth = _FACTORY_REALIZATION_DEPTH.get()
    depth_token = _FACTORY_REALIZATION_DEPTH.set(depth + 1)
    observation_token = _FACTORY_REALIZATION_OBSERVATIONS.set(0) if depth == 0 else None
    try:
        yield
    except Exception:
        if depth == 0 and _FACTORY_REALIZATION_OBSERVATIONS.get() == 0:
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=factory_kind,
                reason_code="runtime_factory_realization_failed",
            )
        raise
    finally:
        _FACTORY_REALIZATION_DEPTH.reset(depth_token)
        if observation_token is not None:
            _FACTORY_REALIZATION_OBSERVATIONS.reset(observation_token)


def require_runtime_factory_ownership(
    *,
    boundary: str,
    factory: object,
    expected: RuntimeOwnershipDescriptor,
    factory_kind: str,
) -> RuntimeOwnershipDescriptor:
    """Validate a lazy factory's declared owner without invoking it.

    Store and knowledge factories must claim exactly one matching database
    role. Provider factories must claim the exact effective remote identity.
    Every factory also carries settings, tenant, and permission identity.
    """
    try:
        actual = get_runtime_factory_ownership(factory, expected_kind=factory_kind)
    except RuntimeOwnershipError:
        observe_runtime_factory_failure(
            phase="preflight",
            factory_kind=factory_kind,
            reason_code="runtime_factory_owner_missing",
        )
        raise

    try:
        missing_dimensions: set[str] = set()
        if actual.settings_identity is None:
            missing_dimensions.add("settings")
        if actual.tenant_policy is None:
            missing_dimensions.update(("tenant", "permission"))
        if missing_dimensions:
            raise RuntimeOwnershipMismatchError(
                boundary,
                missing_dimensions,
                (expected.component, actual.component),
            )
        require_compatible_runtime_ownership(
            boundary=boundary,
            descriptors=(expected, actual),
        )
        category, _, capability = _reason_code(factory_kind).partition(":")
        if category in {"store", "knowledge"}:
            expected_databases = tuple(item for item in expected.databases if item.role == capability)
            actual_databases = tuple(item for item in actual.databases if item.role == capability)
            if len(expected_databases) != 1 or len(actual_databases) != 1 or len(actual.databases) != 1:
                raise RuntimeOwnershipMismatchError(
                    boundary,
                    {"database_role"},
                    (expected.component, actual.component),
                )
            if expected_databases[0] != actual_databases[0]:
                raise RuntimeOwnershipMismatchError(
                    boundary,
                    {"database"},
                    (expected.component, actual.component),
                )
        elif category in {"provider", "backend"}:
            if category == "backend" and capability != "dashboard":
                raise RuntimeOwnershipError("runtime backend factory capability is invalid")
            if expected.remotes != actual.remotes:
                raise RuntimeOwnershipMismatchError(
                    boundary,
                    {"remote"},
                    (expected.component, actual.component),
                )
        else:
            raise RuntimeOwnershipError("runtime factory capability is invalid")
    except RuntimeOwnershipMismatchError as exc:
        observe_runtime_factory_failure(
            phase="preflight",
            factory_kind=factory_kind,
            reason_code="runtime_factory_owner_mismatch",
            dimensions=exc.dimensions,
        )
        raise
    return actual


@runtime_checkable
class RuntimeOwnershipProvider(Protocol):
    """Public protocol implemented by Tacit-owned runtime components."""

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return the component's immutable ownership descriptor."""
        ...


def credential_fingerprint(secret: str | bytes | None) -> str:
    """Return a stable one-way credential identity without exposing the secret."""
    if secret is None or secret == "" or secret == b"":
        return "none"
    if not isinstance(secret, (str, bytes)):
        raise RuntimeOwnershipError("runtime credential value is invalid")
    raw = secret if isinstance(secret, bytes) else secret.encode("utf-8")
    digest = hashlib.sha256(b"tacit-runtime-credential\0" + raw).hexdigest()
    return f"{_FINGERPRINT_PREFIX}{digest}"


def canonical_remote_endpoint(value: str) -> str:
    """Canonicalize a credential-free remote base endpoint."""
    raw = str(value or "")
    if (
        not raw
        or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in raw)
        or "\\" in raw
    ):
        raise RuntimeOwnershipError("remote endpoint is invalid")
    try:
        parsed = urlsplit(raw)
        username = parsed.username
        password = parsed.password
        hostname = parsed.hostname
        parsed_port = parsed.port
    except ValueError:
        raise RuntimeOwnershipError("remote endpoint is invalid") from None
    if username is not None or password is not None or "@" in parsed.netloc:
        raise RuntimeOwnershipError("remote endpoint credentials are not allowed")
    if parsed.netloc.endswith(":"):
        raise RuntimeOwnershipError("remote endpoint is invalid")
    if "?" in raw or "#" in raw:
        raise RuntimeOwnershipError("remote endpoint must not include a query or fragment")
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or not parsed.netloc or hostname is None:
        raise RuntimeOwnershipError("remote endpoint is invalid")
    canonical_host = _canonical_remote_host(hostname)
    if parsed_port == 0:
        raise RuntimeOwnershipError("remote endpoint is invalid")
    if ":" in canonical_host:
        canonical_host = f"[{canonical_host}]"
    default_port = (scheme, parsed_port) in {("http", 80), ("https", 443)}
    port = f":{parsed_port}" if parsed_port is not None and not default_port else ""
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, f"{canonical_host}{port}", path, "", ""))


def canonical_signalfx_realm(value: str) -> str:
    """Return one canonical SignalFx realm as a typed runtime identity."""
    try:
        return _canonical_signalfx_realm(value)
    except ValueError:
        raise RuntimeOwnershipError("SignalFx realm is invalid") from None


def _canonical_remote_host(hostname: str) -> str:
    """Return a validated ASCII DNS or IP host without disclosing failures."""
    try:
        if ":" in hostname:
            return str(ipaddress.IPv6Address(hostname))
        if re.fullmatch(r"[0-9.]+", hostname):
            return str(ipaddress.IPv4Address(hostname))
        ascii_host = hostname.encode("idna").decode("ascii").casefold()
    except (UnicodeError, ValueError):
        raise RuntimeOwnershipError("remote endpoint host is invalid") from None

    labels = ascii_host[:-1].split(".") if ascii_host.endswith(".") else ascii_host.split(".")
    if (
        not labels
        or len(ascii_host.rstrip(".")) > 253
        or any(
            not label or len(label) > 63 or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", label) is None
            for label in labels
        )
    ):
        raise RuntimeOwnershipError("remote endpoint host is invalid")
    return ascii_host


def snapshot_runtime_settings(
    runtime_settings: Settings,
    *,
    database_role: str | None = None,
    database_path: str | Path | None = None,
) -> Settings:
    """Capture settings for one owner without retaining caller-owned state.

    Configured persistence paths are anchored because their meaning otherwise
    changes with the process CWD. Endpoint and policy equivalence belongs to the
    immutable descriptor; retaining those configured values avoids rewriting
    caller-visible settings behavior.
    """
    if not isinstance(runtime_settings, Settings):
        raise RuntimeOwnershipError("runtime settings owner is invalid")
    _tenant_policy(runtime_settings)
    if (database_role is None) != (database_path is None):
        raise RuntimeOwnershipError("database role and path must be supplied together")
    updates: dict[str, Any] = {}
    for field_name in _DATABASE_SETTING_DEFAULTS:
        configured = str(getattr(runtime_settings, field_name) or "").strip()
        updates[field_name] = str(_canonical_path(configured)) if configured else ""
    for field_name in _PATH_SETTING_FIELDS:
        configured = str(getattr(runtime_settings, field_name) or "").strip()
        if configured:
            updates[field_name] = str(_canonical_path(configured))
    if database_role is not None and database_path is not None:
        if not isinstance(database_role, str):
            raise RuntimeOwnershipError("runtime database role is invalid")
        normalized_role = database_role.strip().casefold()
        role_field = _DATABASE_ROLE_SETTING_FIELDS.get(normalized_role)
        if role_field is None:
            raise RuntimeOwnershipError(f"unsupported runtime database role: {normalized_role}")
        actual_path = _canonical_path(database_path)
        configured = str(getattr(runtime_settings, role_field) or "").strip()
        if configured and _canonical_path(configured) != actual_path:
            raise RuntimeOwnershipError(f"runtime settings and explicit {normalized_role} database path must match")
        updates[role_field] = str(actual_path)
    effective_role_paths = {
        role: updates.get(field_name, getattr(runtime_settings, field_name))
        for role, field_name in _DATABASE_ROLE_SETTING_FIELDS.items()
    }
    try:
        canonical_sqlite_role_paths(effective_role_paths)
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    return runtime_settings.model_copy(deep=True, update=updates)


def copy_runtime_settings(runtime_settings: Settings) -> Settings:
    """Return a detached public copy of an owner's settings snapshot."""
    if not isinstance(runtime_settings, Settings):
        raise RuntimeOwnershipError("runtime settings owner is invalid")
    return runtime_settings.model_copy(deep=True)


def runtime_descriptor_from_settings(
    runtime_settings: Settings,
    *,
    component: str,
) -> RuntimeOwnershipDescriptor:
    """Describe a complete settings owner without initializing its resources."""
    if not isinstance(runtime_settings, Settings):
        raise RuntimeOwnershipError("runtime settings owner is invalid")
    tenant_policy = _tenant_policy(runtime_settings)
    settings_identity = _settings_identity(runtime_settings)
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=settings_identity,
        tenant_policy=tenant_policy,
        databases=(
            RuntimeDatabaseIdentity(
                role="history",
                path=Path(runtime_settings.history_db_path or config_module.DEFAULT_HISTORY_DB_PATH),
            ),
            RuntimeDatabaseIdentity(
                role="feedback",
                path=Path(runtime_settings.feedback_db_path or config_module.DEFAULT_FEEDBACK_DB_PATH),
            ),
            RuntimeDatabaseIdentity(
                role="signals",
                path=Path(runtime_settings.signals_db_path or config_module.DEFAULT_SIGNALS_DB_PATH),
            ),
        ),
        remotes=_settings_remotes(runtime_settings),
        cache_namespace=f"runtime-cache:{settings_identity}",
        admission_namespace=f"runtime-admission:{settings_identity}",
    )


def runtime_descriptor_for_store(
    *,
    component: str,
    runtime_settings: Settings,
    database_role: str,
    database_path: str | Path,
) -> RuntimeOwnershipDescriptor:
    """Describe one settings-backed persistence owner."""
    base = runtime_descriptor_from_settings(runtime_settings, component=component)
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity,
        tenant_policy=base.tenant_policy,
        databases=(RuntimeDatabaseIdentity(role=database_role, path=Path(database_path)),),
        cache_namespace=base.cache_namespace,
        admission_namespace=base.admission_namespace,
    )


def runtime_descriptor_for_provider(
    *,
    component: str,
    runtime_settings: Settings,
    capability: str = "llm",
    bedrock_environment: Mapping[str, str] | None = None,
    bedrock_credential_identity: BedrockCredentialIdentity | None = None,
) -> RuntimeOwnershipDescriptor:
    """Describe one settings-bound execution provider without store claims."""
    base = runtime_descriptor_from_settings(runtime_settings, component=component)
    normalized_capability = str(capability or "").strip().casefold()
    if normalized_capability == "llm":
        remotes = _llm_provider_remotes(
            runtime_settings,
            bedrock_environment=bedrock_environment,
            bedrock_credential_identity=bedrock_credential_identity,
        )
    elif normalized_capability == "context":
        remotes = _context_provider_remotes(runtime_settings)
    else:
        raise RuntimeOwnershipError("runtime provider capability is invalid")
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity,
        tenant_policy=base.tenant_policy,
        remotes=remotes,
    )


def runtime_descriptor_for_backends(
    *,
    component: str,
    runtime_settings: Settings,
) -> RuntimeOwnershipDescriptor:
    """Describe the dashboard backend factory without constructing clients."""
    base = runtime_descriptor_from_settings(runtime_settings, component=component)
    configured = {item.provider: item for item in _settings_remotes(runtime_settings)}
    remotes: list[RuntimeRemoteIdentity] = []
    if runtime_settings.grafana_enabled:
        remotes.append(configured["grafana"])
    if runtime_settings.signalfx_enabled and runtime_settings.signalfx_api_token:
        remotes.append(configured["signalfx"])
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity,
        tenant_policy=base.tenant_policy,
        remotes=tuple(remotes),
    )


def require_runtime_store_ownership(
    *,
    boundary: str,
    expected: RuntimeOwnershipDescriptor,
    store: object,
    database_role: str,
) -> RuntimeOwnershipDescriptor:
    """Validate one realized role-scoped store before its first use."""
    normalized_role = str(database_role or "").strip().casefold()
    expected_databases = tuple(database for database in expected.databases if database.role == normalized_role)
    if len(expected_databases) != 1:
        raise RuntimeOwnershipError(f"{boundary} expected owner must expose one {normalized_role} database identity")

    actual = get_runtime_ownership(store, component=f"realized_{normalized_role}_store")
    if actual.availability is not RuntimeAvailability.AVAILABLE:
        require_compatible_runtime_ownership(
            boundary=boundary,
            descriptors=(expected, actual),
        )
    missing_dimensions: set[str] = set()
    if actual.settings_identity is None:
        missing_dimensions.add("settings")
    if actual.tenant_policy is None:
        missing_dimensions.update(("tenant", "permission"))
    actual_databases = tuple(database for database in actual.databases if database.role == normalized_role)
    if len(actual_databases) != 1 or len(actual.databases) != 1:
        missing_dimensions.add("database_role")
    if missing_dimensions:
        raise RuntimeOwnershipMismatchError(
            boundary,
            missing_dimensions,
            (expected.component, actual.component),
        )

    expected_store = RuntimeOwnershipDescriptor(
        component=f"expected_{normalized_role}_store",
        settings_identity=expected.settings_identity,
        tenant_policy=expected.tenant_policy,
        databases=expected_databases,
        remotes=expected.remotes,
        cache_namespace=expected.cache_namespace,
        admission_namespace=expected.admission_namespace,
    )
    require_compatible_runtime_ownership(
        boundary=boundary,
        descriptors=(expected_store, actual),
    )
    if actual_databases[0] != expected_databases[0]:
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"database"},
            (expected_store.component, actual.component),
        )
    return actual


def runtime_descriptor_for_remote(
    *,
    component: str,
    runtime_settings: Settings,
    remote: RuntimeRemoteIdentity,
) -> RuntimeOwnershipDescriptor:
    """Describe a client using its effective, post-override remote identity."""
    base = runtime_descriptor_from_settings(runtime_settings, component=component)
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity,
        tenant_policy=base.tenant_policy,
        remotes=(remote,),
        cache_namespace=base.cache_namespace,
        admission_namespace=base.admission_namespace,
    )


def get_runtime_ownership(owner: object | None, *, component: str | None = None) -> RuntimeOwnershipDescriptor:
    """Read the public descriptor from a Tacit owner.

    This strict API never probes private fields. Third-party objects must be
    adapted explicitly with :func:`adapt_third_party_runtime_owner`.
    """
    if owner is None:
        return RuntimeOwnershipDescriptor.absent(component=component or "runtime-owner")
    if isinstance(owner, RuntimeOwnershipDescriptor):
        return owner
    try:
        descriptor = getattr(owner, "runtime_ownership", None)
    except RuntimeOwnershipError:
        raise
    except Exception as exc:
        owner_name = component or type(owner).__name__
        raise RuntimeOwnershipError(f"{owner_name} must expose a public runtime ownership descriptor") from exc
    if not isinstance(descriptor, RuntimeOwnershipDescriptor):
        owner_name = component or type(owner).__name__
        raise RuntimeOwnershipError(f"{owner_name} must expose a public runtime ownership descriptor")
    return descriptor


def adapt_third_party_runtime_owner(
    *,
    component: str,
    owner: object,
    runtime_settings: Settings | None = None,
    database_role: str | None = None,
    database_path: str | Path | None = None,
    remote: RuntimeRemoteIdentity | None = None,
    availability: RuntimeAvailability = RuntimeAvailability.AVAILABLE,
) -> RuntimeOwnershipDescriptor:
    """Explicit compatibility adapter for non-Tacit components.

    The adapter accepts only public values supplied by the caller. It never
    inspects private attributes, initializes persistence, or contacts a remote.
    """
    del owner
    if availability is RuntimeAvailability.ABSENT:
        return RuntimeOwnershipDescriptor.absent(component=component)
    if availability is RuntimeAvailability.UNAVAILABLE:
        return RuntimeOwnershipDescriptor.unavailable(component=component, reason="third_party_unavailable")

    if database_role is None and database_path is not None:
        raise RuntimeOwnershipError("third-party database paths require an explicit role")
    if database_role is not None and database_path is None:
        raise RuntimeOwnershipError("third-party database roles require an explicit path")

    base = (
        runtime_descriptor_from_settings(runtime_settings, component=component)
        if runtime_settings is not None
        else None
    )
    databases = (
        (RuntimeDatabaseIdentity(role=database_role, path=Path(database_path)),)
        if database_role is not None and database_path is not None
        else ()
    )
    remotes = (remote,) if remote is not None else ()
    return RuntimeOwnershipDescriptor(
        component=component,
        settings_identity=base.settings_identity if base is not None else None,
        tenant_policy=base.tenant_policy if base is not None else None,
        databases=databases,
        remotes=remotes,
        cache_namespace=base.cache_namespace if base is not None else None,
        admission_namespace=base.admission_namespace if base is not None else None,
    )


def require_compatible_runtime_ownership(
    *,
    boundary: str,
    descriptors: tuple[RuntimeOwnershipDescriptor, ...],
) -> RuntimeOwnershipDescriptor:
    """Require every supplied descriptor to identify one compatible runtime."""
    absent = [item.component for item in descriptors if item.availability is RuntimeAvailability.ABSENT]
    if absent:
        logger.warning("runtime_owner_absent", boundary=boundary, component_count=len(absent))
        raise RuntimeOwnershipError(f"{boundary} runtime owner was not supplied")
    unavailable = [item.component for item in descriptors if item.availability is RuntimeAvailability.UNAVAILABLE]
    if unavailable:
        logger.warning("runtime_owner_unavailable", boundary=boundary, component_count=len(unavailable))
        raise RuntimeOwnershipError(f"{boundary} runtime owner is explicitly unavailable")

    available = tuple(item for item in descriptors if item.availability is RuntimeAvailability.AVAILABLE)
    if not available:
        raise RuntimeOwnershipError(f"{boundary} requires an available runtime owner")

    dimensions: set[str] = set()
    for index, expected in enumerate(available):
        for actual in available[index + 1 :]:
            dimensions.update(_mismatch_dimensions(expected, actual))
    claimed_database_paths: dict[str, Path] = {}
    for descriptor in available:
        for database in descriptor.databases:
            if database.role in _DATABASE_ROLE_SETTING_FIELDS:
                claimed_database_paths.setdefault(database.role, database.path)
    try:
        validate_distinct_sqlite_role_paths(claimed_database_paths)
    except ValueError:
        dimensions.add("database_role_collision")
    if not _ownership_graph_is_connected(available):
        dimensions.add("ownership_graph")
    if dimensions:
        components = tuple(item.component for item in available)
        logger.warning(
            "runtime_owner_mismatch",
            boundary=boundary,
            component_count=len(components),
            dimensions=sorted(dimensions),
        )
        raise RuntimeOwnershipMismatchError(boundary, dimensions, components)
    return available[0]


def require_remote_runtime_ownership(
    *,
    boundary: str,
    descriptor: RuntimeOwnershipDescriptor,
    provider: str,
    settings_descriptor: RuntimeOwnershipDescriptor | None = None,
) -> RuntimeOwnershipDescriptor:
    """Require one available owner with the expected remote identity."""
    descriptors = (descriptor,) if settings_descriptor is None else (settings_descriptor, descriptor)
    require_compatible_runtime_ownership(boundary=boundary, descriptors=descriptors)
    expected_provider = str(provider or "").strip().casefold()
    if not expected_provider or len(descriptor.remotes) != 1 or descriptor.remotes[0].provider != expected_provider:
        components = tuple(item.component for item in descriptors)
        logger.warning(
            "runtime_remote_owner_missing",
            boundary=boundary,
            components=components,
            provider=expected_provider,
        )
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"remote"},
            components,
            message=f"{boundary} runtime owner must expose the expected remote identity as the sole provider",
        )
    return descriptor


def resolve_remote_runtime_settings(
    *,
    boundary: str,
    owner: object,
    provider: str,
    explicit_settings: Settings | None = None,
) -> Settings:
    """Resolve one executable remote owner without process-global fallback."""
    descriptor = get_runtime_ownership(owner, component=f"{provider}_remote_owner")
    require_remote_runtime_ownership(
        boundary=boundary,
        descriptor=descriptor,
        provider=provider,
    )
    owner_settings = getattr(owner, "runtime_settings", None)
    if not isinstance(owner_settings, Settings):
        raise RuntimeOwnershipError(f"{boundary} requires a public runtime settings owner")
    selected = snapshot_runtime_settings(owner_settings)
    if explicit_settings is not None:
        explicit = snapshot_runtime_settings(explicit_settings)
        require_remote_runtime_ownership(
            boundary=boundary,
            descriptor=descriptor,
            provider=provider,
            settings_descriptor=runtime_descriptor_from_settings(
                explicit,
                component=f"{boundary}_settings",
            ),
        )
        selected = explicit
    return selected


# Compatibility facade used by pre-S2 composition call sites. New code should
# consume RuntimeOwnershipDescriptor directly through get_runtime_ownership().
@dataclass(frozen=True)
class RuntimeOwner:
    """Legacy settings carrier around the public typed descriptor."""

    name: str
    supplied: bool
    settings: Settings | None = field(default=None, repr=False)
    database_path: Path | None = None
    descriptor: RuntimeOwnershipDescriptor | None = None


def describe_runtime_owner(name: str, owner: Any | None) -> RuntimeOwner:
    """Describe an owner for legacy call sites without private-field probing."""
    if owner is None:
        return RuntimeOwner(
            name=name,
            supplied=False,
            descriptor=RuntimeOwnershipDescriptor.absent(component=name),
        )

    settings_owner = _public_settings(owner)
    database_path = _public_database_path(owner)
    try:
        descriptor = get_runtime_ownership(owner, component=name)
    except RuntimeOwnershipError:
        if settings_owner is None and database_path is None:
            raise
        descriptor = adapt_third_party_runtime_owner(
            component=name,
            owner=owner,
            runtime_settings=settings_owner,
            database_role="persistence" if database_path is not None else None,
            database_path=database_path,
        )
    return RuntimeOwner(
        name=name,
        supplied=True,
        settings=settings_owner,
        database_path=database_path,
        descriptor=descriptor,
    )


def resolve_runtime_settings(
    *,
    boundary: str,
    explicit_settings: Settings | None,
    owners: tuple[RuntimeOwner, ...] = (),
    fallback_settings: Settings,
) -> Settings:
    """Resolve one settings owner for legacy composition call sites."""
    configured: list[tuple[str, Settings]] = []
    if explicit_settings is not None:
        configured.append(("explicit", explicit_settings))
    for owner in owners:
        if owner.settings is not None:
            configured.append((owner.name, owner.settings))

    supplied_owners = [owner.name for owner in owners if owner.supplied]
    if supplied_owners and not configured:
        logger.warning("runtime_settings_owner_missing", boundary=boundary, owners=supplied_owners)
        raise RuntimeOwnershipError(f"{boundary} injected owners require explicit runtime settings or a settings owner")

    if configured:
        configured = [
            (name, _settings_with_owner_databases(value, owners, boundary=boundary)) for name, value in configured
        ]
    active_settings = (
        configured[0][1] if configured else _settings_with_owner_databases(fallback_settings, owners, boundary=boundary)
    )
    expected_identity = _settings_identity(active_settings)
    mismatched = [name for name, value in configured[1:] if _settings_identity(value) != expected_identity]
    if mismatched:
        logger.warning(
            "runtime_settings_owner_mismatch",
            boundary=boundary,
            owners=[name for name, _value in configured],
            mismatched_owners=mismatched,
        )
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"settings"},
            tuple(name for name, _value in configured),
            message=f"{boundary} runtime settings must match across all supplied owners",
        )
    return active_settings


def _settings_with_owner_databases(
    runtime_settings: Settings,
    owners: tuple[RuntimeOwner, ...],
    *,
    boundary: str,
) -> Settings:
    """Adopt supplied database identities when settings leave their paths unset."""
    active = snapshot_runtime_settings(runtime_settings)
    databases: dict[str, Path] = {}
    components: list[str] = []
    for owner in owners:
        descriptor = owner.descriptor
        if not owner.supplied or descriptor is None:
            continue
        components.append(owner.name)
        for database in descriptor.databases:
            existing = databases.get(database.role)
            if existing is not None and existing != database.path:
                raise RuntimeOwnershipMismatchError(
                    boundary,
                    {"database"},
                    tuple(components),
                    message=f"{boundary} persistence owners must use the same database",
                )
            databases[database.role] = database.path

    updates: dict[str, str] = {}
    for role, database_path in databases.items():
        field_name = _DATABASE_ROLE_SETTING_FIELDS.get(role)
        if field_name is None:
            continue
        configured = str(getattr(active, field_name) or "").strip()
        if configured and _canonical_path(configured) != database_path:
            raise RuntimeOwnershipMismatchError(
                boundary,
                {"database"},
                tuple(["settings", *components]),
                message=f"{boundary} settings and persistence owners must use the same database",
            )
        updates[field_name] = str(database_path)
    if not updates:
        return active
    return snapshot_runtime_settings(active.model_copy(deep=True, update=updates))


def require_same_database(*, boundary: str, owners: tuple[RuntimeOwner, ...]) -> None:
    """Require supplied legacy persistence owners to identify one database."""
    supplied = [owner for owner in owners if owner.supplied]
    if len(supplied) < 2:
        return
    missing = [owner.name for owner in supplied if owner.database_path is None]
    if missing:
        logger.warning(
            "runtime_database_owner_missing",
            boundary=boundary,
            owners=[owner.name for owner in supplied],
            missing_database_owners=missing,
        )
        raise RuntimeOwnershipError(f"{boundary} persistence owners must expose their database paths")
    expected = supplied[0].database_path
    mismatched = [owner.name for owner in supplied[1:] if owner.database_path != expected]
    if mismatched:
        logger.warning(
            "runtime_database_owner_mismatch",
            boundary=boundary,
            owners=[owner.name for owner in supplied],
            mismatched_owners=mismatched,
        )
        raise RuntimeOwnershipMismatchError(
            boundary,
            {"database"},
            tuple(owner.name for owner in supplied),
            message=f"{boundary} persistence owners must use the same database",
        )


def _settings_identity(runtime_settings: Settings) -> str:
    payload = runtime_settings.model_dump(mode="json")
    for field_name, default_path in _DATABASE_SETTING_DEFAULTS.items():
        configured = str(payload.get(field_name) or "").strip()
        payload[field_name] = str(_canonical_path(configured or default_path))
    for field_name in _PATH_SETTING_FIELDS:
        configured = str(payload.get(field_name) or "").strip()
        if configured:
            payload[field_name] = str(_canonical_path(configured))
    for field_name in _ENDPOINT_SETTING_FIELDS:
        configured = str(payload.get(field_name) or "").strip()
        if configured:
            payload[field_name] = canonical_remote_endpoint(configured)
    if not payload.get("grafana_public_url"):
        payload["grafana_public_url"] = payload.get("grafana_url", "")
    for field_name in _CASEFOLD_SETTING_FIELDS:
        payload[field_name] = str(payload.get(field_name) or "").strip().casefold()
    for field_name in _STRIPPED_SETTING_FIELDS:
        payload[field_name] = str(payload.get(field_name) or "").strip()
    try:
        payload["knowledge_tenant_id"] = canonical_knowledge_tenant_id(payload.get("knowledge_tenant_id"))
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    payload["knowledge_permissions"] = _canonical_permissions(str(payload.get("knowledge_permissions") or ""))
    tenant_api_keys = payload.get("knowledge_tenant_api_keys")
    if isinstance(tenant_api_keys, Mapping):
        try:
            payload["knowledge_tenant_api_keys"] = validated_knowledge_tenant_api_keys(tenant_api_keys)
        except ValueError as exc:
            raise RuntimeOwnershipError(str(exc)) from exc
    sanitized = {key: _sanitize_setting(key, value) for key, value in sorted(payload.items())}
    encoded = json.dumps(sanitized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return f"{_FINGERPRINT_PREFIX}{hashlib.sha256(encoded).hexdigest()}"


def _sanitize_setting(name: str, value: Any) -> Any:
    normalized_name = name.casefold()
    if any(marker in normalized_name for marker in _SECRET_FIELD_MARKERS):
        if isinstance(value, Mapping):
            return {
                str(key): credential_fingerprint(str(item))
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            }
        return credential_fingerprint(str(value) if value is not None else None)
    if isinstance(value, Mapping):
        return {
            str(key): _sanitize_setting(str(key), item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, list):
        return [_sanitize_setting(name, item) for item in value]
    return value


def _tenant_policy(runtime_settings: Settings) -> RuntimeTenantPolicy:
    try:
        tenant_id = canonical_knowledge_tenant_id(runtime_settings.knowledge_tenant_id)
        raw_tenant_api_keys = runtime_settings.knowledge_tenant_api_keys
        if not isinstance(raw_tenant_api_keys, Mapping):
            raise ValueError("knowledge_tenant_api_keys must be a tenant-key mapping")
        tenant_api_keys = validated_knowledge_tenant_api_keys(raw_tenant_api_keys)
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    permission_identity = _canonical_permissions(runtime_settings.knowledge_permissions)
    permissions = tuple(permission_identity.split(",")) if permission_identity else ()
    credentials = tuple(
        sorted(
            (
                tenant,
                credential_fingerprint(secret),
            )
            for tenant, secret in tenant_api_keys.items()
        )
    )
    return RuntimeTenantPolicy(
        mode="wildcard" if tenant_id == "*" else "pinned",
        tenant_id=tenant_id,
        permissions=permissions,
        api_auth_enabled=runtime_settings.api_auth_enabled,
        tenant_credential_fingerprints=credentials,
    )


def _settings_remotes(runtime_settings: Settings) -> tuple[RuntimeRemoteIdentity, ...]:
    signalfx_realm = canonical_signalfx_realm(runtime_settings.signalfx_realm)
    return (
        RuntimeRemoteIdentity(
            provider="grafana",
            endpoint=runtime_settings.grafana_url,
            account=str(runtime_settings.grafana_org_id),
            credential_fingerprint=credential_fingerprint(runtime_settings.grafana_api_key),
        ),
        RuntimeRemoteIdentity(
            provider="pagerduty",
            endpoint=runtime_settings.pagerduty_base_url,
            credential_fingerprint=credential_fingerprint(runtime_settings.pagerduty_api_token),
        ),
        RuntimeRemoteIdentity(
            provider="signalfx",
            endpoint=f"https://api.{signalfx_realm}.signalfx.com",
            account=signalfx_realm,
            credential_fingerprint=credential_fingerprint(runtime_settings.signalfx_api_token),
        ),
    )


def _llm_provider_remotes(
    runtime_settings: Settings,
    *,
    bedrock_environment: Mapping[str, str] | None = None,
    bedrock_credential_identity: BedrockCredentialIdentity | None = None,
) -> tuple[RuntimeRemoteIdentity, ...]:
    provider = str(runtime_settings.llm_provider or "").strip().casefold()
    api_key_fingerprint = credential_fingerprint(runtime_settings.llm_api_key)
    if provider == "openai":
        return (
            RuntimeRemoteIdentity(
                provider="llm:openai",
                endpoint=runtime_settings.llm_api_base or _OPENAI_API_ENDPOINT,
                account="organization:none;project:none",
                credential_fingerprint=api_key_fingerprint,
            ),
        )
    if provider == "anthropic":
        return (
            RuntimeRemoteIdentity(
                provider="llm:anthropic",
                endpoint=runtime_settings.llm_api_base or _ANTHROPIC_API_ENDPOINT,
                credential_fingerprint=api_key_fingerprint,
            ),
        )
    if provider == "ollama":
        return (
            RuntimeRemoteIdentity(
                provider="llm:ollama",
                endpoint=runtime_settings.llm_api_base or _OLLAMA_API_ENDPOINT,
            ),
        )
    if provider == "azure":
        if not str(runtime_settings.llm_api_base or "").strip():
            return ()
        return (
            RuntimeRemoteIdentity(
                provider="llm:azure",
                endpoint=runtime_settings.llm_api_base,
                account=runtime_settings.llm_azure_deployment or runtime_settings.llm_model,
                credential_fingerprint=api_key_fingerprint,
            ),
        )
    if provider == "bedrock":
        region = str(runtime_settings.llm_bedrock_region or "").strip().casefold()
        if bedrock_credential_identity is None:
            account, credential_identity, _profile = bedrock_credential_selector(
                runtime_settings,
                environment=bedrock_environment,
            )
            fingerprint = credential_fingerprint(credential_identity)
            uses_sts = bool(runtime_settings.llm_bedrock_role_arn) or credential_identity.startswith(
                "web-identity-role:"
            )
        else:
            account = bedrock_credential_identity.account
            fingerprint = bedrock_credential_identity.credential_fingerprint
            uses_sts = bedrock_credential_identity.uses_sts
        remotes = [
            RuntimeRemoteIdentity(
                provider="llm:bedrock",
                endpoint=canonical_bedrock_runtime_endpoint(region),
                account=account,
                credential_fingerprint=fingerprint,
            ),
        ]
        if uses_sts:
            remotes.append(
                RuntimeRemoteIdentity(
                    provider="llm:bedrock:sts",
                    endpoint=canonical_aws_sts_endpoint(region),
                    account=account,
                    credential_fingerprint=fingerprint,
                )
            )
        return tuple(remotes)
    return ()


def _context_provider_remotes(runtime_settings: Settings) -> tuple[RuntimeRemoteIdentity, ...]:
    provider = str(runtime_settings.context_provider or "").strip().casefold()
    if provider in {"", "none"}:
        return ()
    endpoint_by_provider = {
        "mcp": runtime_settings.context_mcp_server_url,
        "a2a": runtime_settings.context_a2a_agent_url,
        "rag_api": runtime_settings.context_rag_api_url,
    }
    endpoint = str(endpoint_by_provider.get(provider, "") or "").strip()
    if not endpoint:
        return ()
    return (
        RuntimeRemoteIdentity(
            provider=f"context:{provider}",
            endpoint=endpoint,
            credential_fingerprint=credential_fingerprint(runtime_settings.context_api_key),
        ),
    )


def _mismatch_dimensions(
    expected: RuntimeOwnershipDescriptor,
    actual: RuntimeOwnershipDescriptor,
) -> set[str]:
    dimensions: set[str] = set()
    if (
        expected.settings_identity is not None
        and actual.settings_identity is not None
        and expected.settings_identity != actual.settings_identity
    ):
        dimensions.add("settings")

    if expected.tenant_policy is not None and actual.tenant_policy is not None:
        expected_tenant = (
            expected.tenant_policy.mode,
            expected.tenant_policy.tenant_id,
            expected.tenant_policy.api_auth_enabled,
            expected.tenant_policy.tenant_credential_fingerprints,
        )
        actual_tenant = (
            actual.tenant_policy.mode,
            actual.tenant_policy.tenant_id,
            actual.tenant_policy.api_auth_enabled,
            actual.tenant_policy.tenant_credential_fingerprints,
        )
        if expected_tenant != actual_tenant:
            dimensions.add("tenant")
        if expected.tenant_policy.permissions != actual.tenant_policy.permissions:
            dimensions.add("permission")

    expected_databases = {item.role: item.path for item in expected.databases}
    actual_databases = {item.role: item.path for item in actual.databases}
    if any(
        expected_databases[role] != actual_databases[role]
        for role in expected_databases.keys() & actual_databases.keys()
    ):
        dimensions.add("database")

    expected_remotes = {item.provider: item for item in expected.remotes}
    actual_remotes = {item.provider: item for item in actual.remotes}
    for provider in expected_remotes.keys() & actual_remotes.keys():
        expected_remote = expected_remotes[provider]
        actual_remote = actual_remotes[provider]
        if expected_remote.endpoint != actual_remote.endpoint:
            dimensions.add("endpoint")
        if expected_remote.account != actual_remote.account:
            dimensions.add("account")
        if expected_remote.credential_fingerprint != actual_remote.credential_fingerprint:
            dimensions.add("credential")

    if (
        expected.cache_namespace is not None
        and actual.cache_namespace is not None
        and expected.cache_namespace != actual.cache_namespace
    ):
        dimensions.add("cache_namespace")
    if (
        expected.admission_namespace is not None
        and actual.admission_namespace is not None
        and expected.admission_namespace != actual.admission_namespace
    ):
        dimensions.add("admission_namespace")
    return dimensions


def _ownership_graph_is_connected(descriptors: tuple[RuntimeOwnershipDescriptor, ...]) -> bool:
    if len(descriptors) < 2:
        return True
    visited = {0}
    pending = [0]
    while pending:
        current = pending.pop()
        for index, candidate in enumerate(descriptors):
            if index in visited:
                continue
            if _shares_identity_dimension(descriptors[current], candidate):
                visited.add(index)
                pending.append(index)
    return len(visited) == len(descriptors)


def _shares_identity_dimension(
    expected: RuntimeOwnershipDescriptor,
    actual: RuntimeOwnershipDescriptor,
) -> bool:
    if expected.settings_identity is not None and expected.settings_identity == actual.settings_identity:
        return True
    if expected.tenant_policy is not None and expected.tenant_policy == actual.tenant_policy:
        return True
    if set(expected.databases) & set(actual.databases):
        return True
    if set(expected.remotes) & set(actual.remotes):
        return True
    if expected.cache_namespace is not None and expected.cache_namespace == actual.cache_namespace:
        return True
    return expected.admission_namespace is not None and expected.admission_namespace == actual.admission_namespace


def _canonical_path(value: str | Path) -> Path:
    try:
        return Path(value).expanduser().resolve()
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise RuntimeOwnershipError("runtime database path is invalid") from exc


def _canonical_permissions(value: object) -> str:
    try:
        return canonical_knowledge_permissions(value)
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc


def _unique_databases(values: tuple[RuntimeDatabaseIdentity, ...]) -> tuple[RuntimeDatabaseIdentity, ...]:
    by_role: dict[str, RuntimeDatabaseIdentity] = {}
    for value in values:
        if not isinstance(value, RuntimeDatabaseIdentity):
            raise RuntimeOwnershipError("runtime database identity is invalid")
        existing = by_role.get(value.role)
        if existing is not None and existing.path != value.path:
            raise RuntimeOwnershipError(f"runtime descriptor contains conflicting database role: {value.role}")
        by_role[value.role] = value
    try:
        validate_distinct_sqlite_role_paths(
            {role: value.path for role, value in by_role.items() if role in _DATABASE_ROLE_SETTING_FIELDS}
        )
    except ValueError as exc:
        raise RuntimeOwnershipError(str(exc)) from exc
    return tuple(by_role[role] for role in sorted(by_role))


def _unique_remotes(values: tuple[RuntimeRemoteIdentity, ...]) -> tuple[RuntimeRemoteIdentity, ...]:
    by_provider: dict[str, RuntimeRemoteIdentity] = {}
    for value in values:
        if not isinstance(value, RuntimeRemoteIdentity):
            raise RuntimeOwnershipError("runtime remote identity is invalid")
        existing = by_provider.get(value.provider)
        if existing is not None and existing != value:
            raise RuntimeOwnershipError(f"runtime descriptor contains conflicting remote provider: {value.provider}")
        by_provider[value.provider] = value
    return tuple(by_provider[provider] for provider in sorted(by_provider))


def _is_fingerprint(value: str) -> bool:
    return (
        value.startswith(_FINGERPRINT_PREFIX)
        and len(value) == len(_FINGERPRINT_PREFIX) + 64
        and all(character in "0123456789abcdef" for character in value[len(_FINGERPRINT_PREFIX) :])
    )


def _reason_code(value: str) -> str:
    normalized = _REASON_CODE_RE.sub("_", str(value or "").strip().casefold()).strip("_")
    return normalized[:96]


def _public_settings(owner: object) -> Settings | None:
    runtime_settings = getattr(owner, "runtime_settings", None)
    if isinstance(runtime_settings, Settings):
        return runtime_settings
    owner_settings = getattr(owner, "settings", None)
    return owner_settings if isinstance(owner_settings, Settings) else None


def _public_database_path(owner: object) -> Path | None:
    value = getattr(owner, "database_path", None)
    return _canonical_path(value) if value is not None else None
