"""AWS Bedrock provider.

Supports explicit credentials and an allowlisted subset of Botocore's file and
web-identity providers. Unsupported ambient providers fail before SDK use.

Requires `boto3` to be installed (optional dependency).
"""

from __future__ import annotations

import configparser
import io
import os
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Never, Protocol, cast

import structlog

from tacit.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from tacit.config import Settings, settings
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork, terminal_cleanup_failure
from tacit.pipeline_admission import (
    PipelineAdmissionController,
    current_pipeline_execution_deadline,
    runtime_admission_controller,
)
from tacit.runtime_ownership import (
    BedrockCredentialIdentity,
    BedrockCredentialPlan,
    RuntimeOwnershipDescriptor,
    canonical_aws_sts_endpoint,
    canonical_bedrock_runtime_endpoint,
    credential_fingerprint,
    snapshot_runtime_settings,
)

logger = structlog.get_logger()

# Bedrock uses Anthropic's Messages API format for Claude models
# and a generic InvokeModel API for others.
_ANTHROPIC_MODEL_PREFIXES = ("anthropic.",)
_META_MODEL_PREFIXES = ("meta.",)
_MISTRAL_MODEL_PREFIXES = ("mistral.",)

# Map common Anthropic API model names to their Bedrock model IDs
_ANTHROPIC_TO_BEDROCK: dict[str, str] = {
    "claude-sonnet-4-20250514": "anthropic.claude-sonnet-4-20250514-v1:0",
    "claude-3-5-sonnet-20241022": "anthropic.claude-3-5-sonnet-20241022-v2:0",
    "claude-3-5-haiku-20241022": "anthropic.claude-3-5-haiku-20241022-v1:0",
    "claude-3-opus-20240229": "anthropic.claude-3-opus-20240229-v1:0",
    "claude-3-haiku-20240307": "anthropic.claude-3-haiku-20240307-v1:0",
}

# Known Bedrock provider prefixes — if llm_model starts with one of these,
# it's already a valid Bedrock model ID and should be used as-is.
# Includes regional/global inference profile prefixes (us., eu., ap., etc.).
_BEDROCK_PROVIDER_PREFIXES = (
    "anthropic.",
    "meta.",
    "mistral.",
    "amazon.",
    "cohere.",
    "ai21.",
    "stability.",
    # Regional and cross-region inference profile prefixes
    "us.",
    "eu.",
    "apac.",
    "ap.",
    "sa.",
    "me.",
    "ca.",
    "af.",
    "global.",
)

# Map AWS region prefix to geography-preserving inference profile prefixes.
# Global routing is never derived from a regional endpoint; callers must
# configure a global profile ID explicitly when that wider routing is intended.
_REGION_INFERENCE_PREFIX: dict[str, str] = {
    "us": "us",
    "eu": "eu",
    "ap": "apac",
}

# Prefixes that indicate a model ID is already an inference profile
_INFERENCE_PROFILE_PREFIXES = ("us.", "eu.", "apac.", "ap.", "sa.", "me.", "ca.", "af.", "global.")
_ON_DEMAND_THROUGHPUT_REASON = "on-demand throughput"
_INFERENCE_PROFILE_REASON = "inference profile"


def _inference_profile_id(bare_model_id: str, region: str) -> str | None:
    """Return a geography-preserving profile ID when the region declares one.

    Uses ``us.``, ``eu.``, or ``apac.`` for their documented geographies.
    Other regions return ``None`` instead of silently widening to ``global.``.

    Example: ('anthropic.claude-sonnet-4-20250514-v1:0', 'us-east-1')
             -> 'us.anthropic.claude-sonnet-4-20250514-v1:0'
    Example: ('anthropic.claude-sonnet-4-20250514-v1:0', 'ap-northeast-1')
             -> 'apac.anthropic.claude-sonnet-4-20250514-v1:0'
    """
    region_prefix = region.split("-")[0]  # "us-east-1" -> "us"
    prefix = _REGION_INFERENCE_PREFIX.get(region_prefix)
    if prefix is None:
        return None
    return f"{prefix}.{bare_model_id}"


def _validation_error_message(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict):
            message = error.get("Message")
            if isinstance(message, str):
                return message
    return str(exc)


class _Boto3Session(Protocol):
    def get_credentials(self) -> Any:
        """Return the credential provider result for discovery sessions."""

    def client(self, service_name: str, **kwargs: object) -> Any:
        """Create one explicitly configured AWS service client."""


@dataclass(frozen=True, slots=True)
class _FrozenBedrockCredentials:
    """One non-refreshing AWS credential value owned by a provider generation."""

    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    token: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.access_key, str) or not self.access_key:
            raise RuntimeOwnershipError("AWS Bedrock credentials are unavailable")
        if not isinstance(self.secret_key, str) or not self.secret_key:
            raise RuntimeOwnershipError("AWS Bedrock credentials are unavailable")
        if not isinstance(self.token, str):
            raise RuntimeOwnershipError("AWS Bedrock session token is invalid")

    @property
    def fingerprint(self) -> str:
        material = "\0".join((self.access_key, self.secret_key, self.token))
        return credential_fingerprint(material)

    @property
    def account(self) -> str:
        return f"access-key:{credential_fingerprint(self.access_key)}"


@dataclass(frozen=True, slots=True)
class _ResolvedBedrockRuntime:
    """Private credential snapshot and clients owned by one blocking operation."""

    session: _Boto3Session = field(repr=False)
    credential_identity: BedrockCredentialIdentity
    credential_clients: tuple[object, ...] = field(default=(), repr=False)


def _direct_bedrock_lifecycle(credential_plan: BedrockCredentialPlan) -> PipelineAdmissionController:
    """Return one bounded admission owner shared by direct callers in a runtime."""
    declaration = credential_plan.ownership(component="direct_bedrock_runtime")
    identity = declaration.admission_namespace or (
        f"runtime-admission:{declaration.settings_identity}" if declaration.settings_identity else None
    )
    if not identity:
        raise RuntimeOwnershipError("AWS Bedrock direct runtime has no admission identity")
    return runtime_admission_controller(
        credential_plan.runtime_settings,
        runtime_identity=identity,
    )


def _write_private_snapshot(path: Path, content: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
    finally:
        os.close(descriptor)


def _frozen_web_identity_sources(
    credential_plan: BedrockCredentialPlan,
    *,
    token_path: Path,
) -> tuple[bytes, bytes]:
    profile = credential_plan.profile or "default"
    parser = configparser.RawConfigParser(interpolation=None)
    section = "default" if profile == "default" else f"profile {profile}"
    parser.add_section(section)
    parser.set(section, "role_arn", credential_plan.web_identity_role_arn)
    parser.set(section, "web_identity_token_file", str(token_path))
    if credential_plan.web_identity_role_session_name:
        parser.set(section, "role_session_name", credential_plan.web_identity_role_session_name)
    output = io.StringIO()
    parser.write(output)
    # A synthesized effective profile prevents either captured source file from
    # reapplying lower-level fields after provider precedence was admitted.
    return b"", output.getvalue().encode("utf-8")


@contextmanager
def _frozen_file_discovery_session(
    boto3_module: Any,
    credential_plan: BedrockCredentialPlan,
) -> Iterator[_Boto3Session]:
    """Resolve profiles from private copies of the plan's captured source bytes."""
    try:
        import botocore.session
    except ImportError as exc:  # pragma: no cover - boto3 always installs botocore
        raise ImportError("AWS Bedrock provider requires botocore") from exc

    with tempfile.TemporaryDirectory(prefix="tacit-bedrock-credentials-") as directory:
        root = Path(directory)
        credentials_path = root / "credentials"
        config_path = root / "config"
        token_path = root / "web-identity-token"
        credentials_content = credential_plan.source_content("shared_credentials")
        config_content = credential_plan.source_content("config")
        if credential_plan.has_source("web_identity_token"):
            _write_private_snapshot(
                token_path,
                credential_plan.source_content("web_identity_token"),
            )
            credentials_content, config_content = _frozen_web_identity_sources(
                credential_plan,
                token_path=token_path,
            )
        _write_private_snapshot(
            credentials_path,
            credentials_content,
        )
        _write_private_snapshot(
            config_path,
            config_content,
        )

        # Setting an explicit profile removes Botocore's live EnvProvider. The
        # implicit default must also be explicit so a post-admission ambient web
        # identity cannot outrank the captured default profile.
        selected_profile = credential_plan.profile or "default"
        core_session = botocore.session.Session(profile=selected_profile)
        # Store the value explicitly so a later AWS_PROFILE mutation cannot
        # redirect this already-admitted generation.
        core_session.set_config_variable("profile", selected_profile)
        core_session.set_config_variable("credentials_file", str(credentials_path))
        core_session.set_config_variable("config_file", str(config_path))
        core_session.set_config_variable("region", credential_plan.runtime_settings.llm_bedrock_region)
        core_session.set_config_variable("ignore_configured_endpoint_urls", True)
        core_session.set_config_variable("sts_regional_endpoints", "regional")
        core_session.set_config_variable("use_fips_endpoint", False)
        core_session.set_config_variable("use_dualstack_endpoint", False)
        core_session.set_default_client_config(_bedrock_client_config(credential_plan.runtime_settings))
        credential_resolver = core_session.get_component("credential_provider")
        allowed_methods = frozenset(credential_plan.discovery_methods)
        credential_resolver.providers[:] = [
            provider
            for provider in credential_resolver.providers
            if str(getattr(provider, "METHOD", "")) in allowed_methods
        ]
        if not credential_resolver.providers:
            raise RuntimeOwnershipError("AWS Bedrock credential provider is unavailable")
        yield cast(_Boto3Session, boto3_module.Session(botocore_session=core_session))


def _remaining_operation_timeout(runtime_settings: Settings, deadline: float | None) -> float:
    configured = float(runtime_settings.pipeline_timeout_seconds)
    if deadline is None:
        return configured
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("AWS Bedrock operation deadline expired")
    return min(configured, remaining)


def _bedrock_client_config(
    runtime_settings: Settings,
    *,
    deadline: float | None = None,
    unsigned: bool = False,
) -> Any:
    """Return bounded Botocore transport settings for one blocking operation."""
    try:
        from botocore.config import Config
    except ImportError as exc:  # pragma: no cover - boto3 always installs botocore
        raise ImportError("AWS Bedrock provider requires botocore") from exc

    operation_timeout = _remaining_operation_timeout(runtime_settings, deadline)
    kwargs: dict[str, Any] = {
        "connect_timeout": min(10.0, operation_timeout),
        "read_timeout": operation_timeout,
        "retries": {"mode": "standard", "total_max_attempts": 1},
        "tcp_keepalive": True,
        "proxies": {},
    }
    if unsigned:
        from botocore import UNSIGNED

        kwargs["signature_version"] = UNSIGNED
    return Config(
        **kwargs,
    )


def _credential_cleanup_failure(
    primary_error: BaseException,
    resources: tuple[object, ...],
) -> RuntimeOwnershipError | None:
    """Return a stable terminal error when credential cleanup is incomplete."""
    cleanup_error = BedrockProvider._close_resources(
        resources,
        event="bedrock_credential_cleanup_failed",
    )
    if cleanup_error is None:
        return None
    return terminal_cleanup_failure(
        primary_error,
        cleanup_error,
        reason_code="bedrock_credential_cleanup_failed",
        message="AWS Bedrock credential cleanup failed",
        retain_capacity=True,
    )


def _build_boto3_session(
    runtime_settings: Settings | None = None,
    *,
    environment: dict[str, str] | None = None,
    credential_plan: BedrockCredentialPlan | None = None,
    deadline: float | None = None,
) -> _ResolvedBedrockRuntime:
    """Resolve and pin one credential snapshot for a provider generation."""
    if credential_plan is None:
        credential_plan = BedrockCredentialPlan.capture(
            runtime_settings or settings,
            environment=environment,
        )
    elif runtime_settings is not None or environment is not None:
        raise RuntimeOwnershipError("AWS Bedrock credential plan must be the sole credential input")
    credential_plan.verify_unchanged()
    runtime_settings = credential_plan.runtime_settings
    _remaining_operation_timeout(runtime_settings, deadline)
    environment = credential_plan.environment
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(
            "AWS Bedrock provider requires the pinned Bedrock extra. "
            "Install it with: pip install 'tacit-ai[bedrock]'"
        ) from exc

    profile_name = credential_plan.profile
    ambient_access_key = str(environment.get("AWS_ACCESS_KEY_ID") or "")
    ambient_secret_key = str(environment.get("AWS_SECRET_ACCESS_KEY") or "")
    ambient_session_token = str(environment.get("AWS_SECURITY_TOKEN") or environment.get("AWS_SESSION_TOKEN") or "")

    def pinned_session(credentials: _FrozenBedrockCredentials) -> _Boto3Session:
        kwargs = {
            "region_name": runtime_settings.llm_bedrock_region,
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
        }
        if credentials.token:
            kwargs["aws_session_token"] = credentials.token
        return boto3.Session(**kwargs)

    auth_method: str
    selected_source_sts = False
    credential_clients: tuple[object, ...] = ()
    sts: Any | None = None
    if runtime_settings.llm_aws_access_key_id or runtime_settings.llm_aws_secret_access_key:
        if not runtime_settings.llm_aws_access_key_id or not runtime_settings.llm_aws_secret_access_key:
            raise RuntimeOwnershipError("AWS credentials must include both access key and secret key")
        frozen = _FrozenBedrockCredentials(
            access_key=runtime_settings.llm_aws_access_key_id,
            secret_key=runtime_settings.llm_aws_secret_access_key,
            token=runtime_settings.llm_aws_session_token,
        )
        auth_method = "explicit_keys"
    elif ambient_access_key or ambient_secret_key:
        if not ambient_access_key or not ambient_secret_key:
            raise RuntimeOwnershipError("AWS credentials must include both access key and secret key")
        frozen = _FrozenBedrockCredentials(
            access_key=ambient_access_key,
            secret_key=ambient_secret_key,
            token=ambient_session_token,
        )
        auth_method = "environment_keys"
    elif credential_plan.discovery_methods == ("assume-role",):
        access_key, secret_key, token = credential_plan.role_source_credentials()
        source_credentials = _FrozenBedrockCredentials(access_key=access_key, secret_key=secret_key, token=token)
        sts = pinned_session(source_credentials).client(
            "sts",
            endpoint_url=canonical_aws_sts_endpoint(runtime_settings.llm_bedrock_region),
            config=_bedrock_client_config(runtime_settings, deadline=deadline),
            verify=True,
        )
        try:
            assumed = sts.assume_role(
                RoleArn=credential_plan.source_role_arn,
                RoleSessionName=credential_plan.source_role_session_name or "tacit-bedrock",
                DurationSeconds=3600,
            )
            creds = assumed["Credentials"]
            frozen = _FrozenBedrockCredentials(
                access_key=creds["AccessKeyId"],
                secret_key=creds["SecretAccessKey"],
                token=creds["SessionToken"],
            )
        except BaseException as exc:
            cleanup_failure = _credential_cleanup_failure(exc, (sts,))
            if cleanup_failure is not None:
                raise cleanup_failure from exc
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RuntimeOwnershipError(f"AWS Bedrock credential realization failed ({type(exc).__name__})") from None
        credential_clients = (sts,)
        selected_source_sts = True
        auth_method = "profile_assume_role"
    elif credential_plan.discovery_methods == ("assume-role-with-web-identity",):
        unsigned_session = boto3.Session(region_name=runtime_settings.llm_bedrock_region)
        sts = unsigned_session.client(
            "sts",
            endpoint_url=canonical_aws_sts_endpoint(runtime_settings.llm_bedrock_region),
            config=_bedrock_client_config(runtime_settings, deadline=deadline, unsigned=True),
            verify=True,
        )
        try:
            token = credential_plan.source_content("web_identity_token").decode("utf-8").strip()
            if not token:
                raise RuntimeOwnershipError("AWS web identity token is unavailable")
            assumed = sts.assume_role_with_web_identity(
                RoleArn=credential_plan.web_identity_role_arn,
                RoleSessionName=credential_plan.web_identity_role_session_name or "tacit-bedrock",
                WebIdentityToken=token,
                DurationSeconds=3600,
            )
            creds = assumed["Credentials"]
            frozen = _FrozenBedrockCredentials(
                access_key=creds["AccessKeyId"],
                secret_key=creds["SecretAccessKey"],
                token=creds["SessionToken"],
            )
        except BaseException as exc:
            cleanup_failure = _credential_cleanup_failure(exc, (sts,))
            if cleanup_failure is not None:
                raise cleanup_failure from exc
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise RuntimeOwnershipError(f"AWS Bedrock credential realization failed ({type(exc).__name__})") from None
        credential_clients = (sts,)
        selected_source_sts = True
        auth_method = "web_identity"
    else:

        def resolve_discovery_credentials(discovery_session: _Boto3Session) -> _FrozenBedrockCredentials:
            nonlocal selected_source_sts
            credentials = discovery_session.get_credentials()
            if credentials is None:
                raise RuntimeOwnershipError("AWS Bedrock credentials are unavailable")
            get_frozen_credentials = getattr(credentials, "get_frozen_credentials", None)
            if not callable(get_frozen_credentials):
                raise RuntimeOwnershipError("AWS Bedrock credentials cannot be frozen")
            credential_method = str(getattr(credentials, "method", "") or "").strip().casefold()
            if credential_method not in credential_plan.discovery_methods:
                raise RuntimeOwnershipError("AWS Bedrock credential provider was not admitted")
            selected_source_sts = credential_method in {
                "assume-role",
                "assume-role-with-web-identity",
            }
            if selected_source_sts != credential_plan.source_uses_sts:
                raise RuntimeOwnershipError("AWS Bedrock credential source no longer matches its declared remote plan")
            resolved = get_frozen_credentials()
            return _FrozenBedrockCredentials(
                access_key=resolved.access_key,
                secret_key=resolved.secret_key,
                token=resolved.token or "",
            )

        with _frozen_file_discovery_session(boto3, credential_plan) as discovery_session:
            frozen = resolve_discovery_credentials(discovery_session)
        auth_method = "profile" if profile_name else "default_chain"

    try:
        if runtime_settings.llm_bedrock_role_arn:
            sts = pinned_session(frozen).client(
                "sts",
                endpoint_url=canonical_aws_sts_endpoint(runtime_settings.llm_bedrock_region),
                config=_bedrock_client_config(runtime_settings, deadline=deadline),
                verify=True,
            )
            assumed = sts.assume_role(
                RoleArn=runtime_settings.llm_bedrock_role_arn,
                RoleSessionName="tacit-bedrock",
                DurationSeconds=3600,
            )
            creds = assumed["Credentials"]
            frozen = _FrozenBedrockCredentials(
                access_key=creds["AccessKeyId"],
                secret_key=creds["SecretAccessKey"],
                token=creds["SessionToken"],
            )
            credential_clients = (sts,)
            auth_method = "assume_role"

        _remaining_operation_timeout(runtime_settings, deadline)
        session = pinned_session(frozen)
        credential_identity = credential_plan.realized_identity(
            fallback_account=frozen.account,
            credential_fingerprint_value=frozen.fingerprint,
            source_uses_sts=selected_source_sts,
        )
    except BaseException as exc:
        cleanup_failure = _credential_cleanup_failure(exc, (sts,))
        if cleanup_failure is not None:
            raise cleanup_failure from exc
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        raise RuntimeOwnershipError(f"AWS Bedrock credential realization failed ({type(exc).__name__})") from None

    logger.info("bedrock_auth", method=auth_method, region=runtime_settings.llm_bedrock_region)
    return _ResolvedBedrockRuntime(
        session=session,
        credential_identity=credential_identity,
        credential_clients=credential_clients,
    )


class BedrockProvider(LLMProvider):
    """Operation-scoped compatibility bridge for Botocore's blocking API."""

    _NO_SYSTEM_PREFIXES = ("mistral.",)

    def __init__(
        self,
        runtime_settings: Settings | None = None,
        *,
        credential_plan: BedrockCredentialPlan | None = None,
    ) -> None:
        if credential_plan is None:
            credential_plan = BedrockCredentialPlan.capture(runtime_settings or settings)
        elif runtime_settings is not None:
            supplied_settings = snapshot_runtime_settings(runtime_settings)
            if supplied_settings != credential_plan.runtime_settings:
                raise RuntimeOwnershipError("AWS Bedrock credential plan settings do not match")
        credential_plan = credential_plan.as_cross_generation_declaration()
        self._credential_plan = credential_plan
        self._runtime_settings = credential_plan.runtime_settings
        self._settings = self.runtime_settings
        self._runtime_ownership = credential_plan.ownership(component="bedrock_llm_provider")
        self._blocking_work = LifecycleOwnedBlockingWork()
        self._lifecycle_lock = threading.Lock()
        self._pipeline_lifecycle: PipelineAdmissionController | None = None
        self._direct_lifecycle: PipelineAdmissionController | None = None
        self._closed = threading.Event()
        self._model_lock = threading.Lock()
        configured_model = self._settings.llm_model
        if self._settings.llm_bedrock_model_id:
            self._model_id = self._settings.llm_bedrock_model_id
        elif configured_model.startswith(_BEDROCK_PROVIDER_PREFIXES):
            self._model_id = configured_model
        elif configured_model in _ANTHROPIC_TO_BEDROCK:
            self._model_id = _ANTHROPIC_TO_BEDROCK[configured_model]
        else:
            raise RuntimeOwnershipError("Unknown Bedrock model; configure LLM_BEDROCK_MODEL_ID explicitly")
        logger.info(
            "bedrock_configured",
            model_id=self._model_id,
            region=self._settings.llm_bedrock_region,
            resource_scope="operation",
        )

    @property
    def bedrock_credential_identity(self) -> None:
        """The bridge deliberately retains no realized credential generation."""
        return None

    def bedrock_ownership_declarations(
        self,
        *,
        component: str,
    ) -> tuple[RuntimeOwnershipDescriptor, RuntimeOwnershipDescriptor]:
        """Use the stable plan as both provider and cross-generation expectation."""
        planned = self._credential_plan.ownership(component=component)
        return planned, planned

    def realize_blocking(self) -> None:
        """Compatibility preflight that performs no SDK construction or I/O."""
        if self._closed.is_set():
            raise RuntimeOwnershipError("AWS Bedrock provider is closed")

    def bind_pipeline_lifecycle(self, lifecycle: PipelineAdmissionController) -> bool:
        """Bind this provider to the composition root's admission controller."""
        return self._bind_lifecycle(lifecycle, direct=False)

    def claim_abandoned_cleanup(self, lifecycle: PipelineAdmissionController) -> bool:
        """Accept compatible abandonment; no SDK resource needs retirement."""
        try:
            self._bind_lifecycle(lifecycle, direct=False)
        except RuntimeOwnershipError:
            return False
        self.close_blocking()
        return True

    def _bind_lifecycle(
        self,
        lifecycle: PipelineAdmissionController,
        *,
        direct: bool,
    ) -> bool:
        declaration = self._credential_plan.ownership(component="bedrock_lifecycle_binding")
        runtime_identity = declaration.admission_namespace or (
            f"runtime-admission:{declaration.settings_identity}" if declaration.settings_identity else None
        )
        if runtime_identity is None:
            raise RuntimeOwnershipError("AWS Bedrock credential plan has no admission identity")
        lifecycle.bind_runtime_settings_identity(runtime_identity)
        with self._lifecycle_lock:
            if self._pipeline_lifecycle is lifecycle:
                if not direct and self._direct_lifecycle is lifecycle:
                    self._direct_lifecycle = None
                return False
            if self._pipeline_lifecycle is not None:
                raise RuntimeOwnershipError("AWS Bedrock provider belongs to another runtime lifecycle")
            try:
                self._blocking_work.bind(lifecycle)
            except ValueError as exc:
                raise RuntimeOwnershipError("AWS Bedrock provider belongs to another runtime lifecycle") from exc
            self._pipeline_lifecycle = lifecycle
            if direct:
                self._direct_lifecycle = lifecycle
            else:
                self._direct_lifecycle = None
        return True

    def _execution_lifecycle(self) -> tuple[PipelineAdmissionController, bool]:
        """Resolve the injected owner or bind direct use to its shared runtime."""
        with self._lifecycle_lock:
            lifecycle = self._pipeline_lifecycle
            if lifecycle is not None:
                return lifecycle, lifecycle is self._direct_lifecycle
        direct_lifecycle = _direct_bedrock_lifecycle(self._credential_plan)
        try:
            self._bind_lifecycle(direct_lifecycle, direct=True)
        except RuntimeOwnershipError:
            with self._lifecycle_lock:
                lifecycle = self._pipeline_lifecycle
                if lifecycle is None:
                    raise
                return lifecycle, lifecycle is self._direct_lifecycle
        return direct_lifecycle, True

    async def _run_owned[Result](
        self,
        function: Callable[[], Result],
        *,
        reason_code: str,
        deadline: float,
    ) -> Result:
        lifecycle, manages_slot = self._execution_lifecycle()
        timeout_seconds = _remaining_operation_timeout(self._settings, deadline)
        if not manages_slot:
            return await self._blocking_work.run(
                function,
                reason_code=reason_code,
                timeout_seconds=timeout_seconds,
            )
        async with lifecycle.slot(timeout_seconds=timeout_seconds):
            return await self._blocking_work.run(
                function,
                reason_code=reason_code,
                timeout_seconds=_remaining_operation_timeout(self._settings, deadline),
            )

    async def retire_rejected(self, *, reason_code: str) -> bool:
        """Reject future calls; operation-scoped SDK resources cannot survive."""
        del reason_code
        self.close_blocking()
        return True

    def _build_converse_kwargs(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        *,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        """Build one Converse request while handling model-family quirks."""
        if model_id is None:
            with self._model_lock:
                model_id = self._model_id
        if model_id.startswith(self._NO_SYSTEM_PREFIXES):
            merged_user = f"{system_prompt}\n\n{user_prompt}"
            return {
                "modelId": model_id,
                "messages": [{"role": "user", "content": [{"text": merged_user}]}],
                "inferenceConfig": {"temperature": temperature, "maxTokens": 4096},
            }
        return {
            "modelId": model_id,
            "system": [{"text": system_prompt}],
            "messages": [{"role": "user", "content": [{"text": user_prompt}]}],
            "inferenceConfig": {"temperature": temperature, "maxTokens": 4096},
        }

    @staticmethod
    def _extract_usage(response: dict[str, Any]) -> TokenUsage:
        usage = response.get("usage", {})
        inp = usage.get("inputTokens", 0) or 0
        out = usage.get("outputTokens", 0) or 0
        return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)

    def _validate_runtime_generation(
        self,
        resolved_runtime: object,
        *,
        operation_plan: BedrockCredentialPlan,
    ) -> _ResolvedBedrockRuntime:
        if not isinstance(resolved_runtime, _ResolvedBedrockRuntime):
            self._reject_runtime_generation(
                (resolved_runtime,),
                RuntimeOwnershipError("AWS Bedrock provider has no realized credential identity"),
            )
        if not isinstance(resolved_runtime.credential_identity, BedrockCredentialIdentity):
            self._reject_runtime_generation(
                (*resolved_runtime.credential_clients, resolved_runtime.session),
                RuntimeOwnershipError("AWS Bedrock provider has no realized credential identity"),
            )
        planned = self._credential_plan.ownership(component="bedrock_operation_plan")
        realized = operation_plan.ownership(
            component="bedrock_operation_generation",
            credential_identity=resolved_runtime.credential_identity,
        )
        planned_remotes = {remote.provider: remote for remote in planned.remotes}
        realized_remotes = {remote.provider: remote for remote in realized.remotes}
        mismatch: set[str] = set()
        if set(planned_remotes) != set(realized_remotes):
            mismatch.add("remote")
        for provider, planned_remote in planned_remotes.items():
            realized_remote = realized_remotes.get(provider)
            if realized_remote is None:
                continue
            if realized_remote.endpoint != planned_remote.endpoint:
                mismatch.add("endpoint")
            selector_refines = planned_remote.account == "default-chain" or planned_remote.account.startswith(
                "profile:"
            )
            if not selector_refines and realized_remote.account != planned_remote.account:
                mismatch.add("account")
            if realized_remote.credential_fingerprint == "none":
                mismatch.add("credential")
        if mismatch:
            joined = ", ".join(sorted(mismatch))
            self._reject_runtime_generation(
                (*resolved_runtime.credential_clients, resolved_runtime.session),
                RuntimeOwnershipError(f"AWS Bedrock credential realization mismatch: {joined}"),
            )
        return resolved_runtime

    def _reject_runtime_generation(
        self,
        resources: tuple[object, ...],
        primary_error: RuntimeOwnershipError,
    ) -> Never:
        cleanup_error = self._close_resources(
            resources,
            event="bedrock_rejected_runtime_cleanup_failed",
        )
        if cleanup_error is not None:
            raise terminal_cleanup_failure(
                primary_error,
                cleanup_error,
                reason_code="bedrock_rejected_runtime_cleanup_failed",
                message="AWS Bedrock rejected runtime cleanup failed",
                retain_capacity=True,
            ) from primary_error
        raise primary_error

    @staticmethod
    def _observe_phase(phase: str, started: float, *, error: BaseException | None = None) -> None:
        fields: dict[str, Any] = {
            "phase": phase,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "status": "failed" if error is not None else "completed",
        }
        if error is not None:
            fields["error_type"] = type(error).__name__
        logger.info("bedrock_blocking_phase", **fields)

    @staticmethod
    def _close_resources(resources: tuple[object, ...], *, event: str) -> str | None:
        first_error: str | None = None
        unique = {id(resource): resource for resource in resources if resource is not None}
        for resource in unique.values():
            close = getattr(resource, "close", None)
            if not callable(close):
                continue
            try:
                close()
            except BaseException as exc:
                if first_error is None:
                    first_error = type(exc).__name__
                logger.warning(event, error_type=type(exc).__name__)
        return first_error

    def _converse_with_client(
        self,
        client: Any,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        *,
        deadline: float | None = None,
        retry_client_factory: Callable[[], Any] | None = None,
    ) -> LLMResult:
        with self._model_lock:
            model_id = self._model_id
        kwargs = self._build_converse_kwargs(
            system_prompt,
            user_prompt,
            temperature,
            model_id=model_id,
        )
        _remaining_operation_timeout(self._settings, deadline)
        profile_id: str | None = None
        try:
            response = client.converse(**kwargs)
        except Exception as exc:
            profile_id = self._profile_retry_id(exc, model_id=model_id)
            if profile_id is None or retry_client_factory is None:
                raise
            _remaining_operation_timeout(self._settings, deadline)
            logger.warning("bedrock_model_retry_with_profile", bare=model_id, profile=profile_id)
            kwargs["modelId"] = profile_id
            response = retry_client_factory().converse(**kwargs)
        _remaining_operation_timeout(self._settings, deadline)
        if profile_id is not None:
            with self._model_lock:
                if self._model_id == model_id:
                    self._model_id = profile_id
            logger.info("bedrock_model_updated", model_id=profile_id)
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])
        text_parts = [block["text"] for block in content_blocks if "text" in block]
        result = LLMResult(text="".join(text_parts), usage=self._extract_usage(response))
        _remaining_operation_timeout(self._settings, deadline)
        return result

    def _profile_retry_id(self, exc: Exception, *, model_id: str | None = None) -> str | None:
        if type(exc).__name__ != "ValidationException":
            return None
        if model_id is None:
            with self._model_lock:
                model_id = self._model_id
        if model_id.startswith(_INFERENCE_PROFILE_PREFIXES):
            return None
        reason = _validation_error_message(exc).casefold()
        if _ON_DEMAND_THROUGHPUT_REASON not in reason or _INFERENCE_PROFILE_REASON not in reason:
            return None
        return _inference_profile_id(model_id, self._settings.llm_bedrock_region)

    def _should_retry_with_profile(self, exc: Exception, *, model_id: str | None = None) -> bool:
        return self._profile_retry_id(exc, model_id=model_id) is not None

    def _execute_converse_operation(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
        *,
        deadline: float,
    ) -> LLMResult:
        resolved_runtime: _ResolvedBedrockRuntime | None = None
        clients: list[object] = []
        operation_error: BaseException | None = None
        try:
            phase_started = time.monotonic()
            try:
                operation_plan = self._credential_plan.capture_operation_generation()
                resolved_runtime = self._validate_runtime_generation(
                    _build_boto3_session(
                        credential_plan=operation_plan,
                        deadline=deadline,
                    ),
                    operation_plan=operation_plan,
                )
            except BaseException as exc:
                self._observe_phase("credential_realization", phase_started, error=exc)
                raise
            self._observe_phase("credential_realization", phase_started)

            phase_started = time.monotonic()
            try:
                client = resolved_runtime.session.client(
                    "bedrock-runtime",
                    endpoint_url=canonical_bedrock_runtime_endpoint(self._settings.llm_bedrock_region),
                    config=_bedrock_client_config(self._settings, deadline=deadline),
                    verify=True,
                )
                clients.append(client)
            except BaseException as exc:
                self._observe_phase("client_construction", phase_started, error=exc)
                raise
            self._observe_phase("client_construction", phase_started)

            def build_retry_client() -> object:
                retry_started = time.monotonic()
                try:
                    retry_client = resolved_runtime.session.client(
                        "bedrock-runtime",
                        endpoint_url=canonical_bedrock_runtime_endpoint(self._settings.llm_bedrock_region),
                        config=_bedrock_client_config(self._settings, deadline=deadline),
                        verify=True,
                    )
                    clients.append(retry_client)
                except BaseException as exc:
                    self._observe_phase("client_reconstruction", retry_started, error=exc)
                    raise
                self._observe_phase("client_reconstruction", retry_started)
                return retry_client

            phase_started = time.monotonic()
            try:
                result = self._converse_with_client(
                    client,
                    system_prompt,
                    user_prompt,
                    temperature,
                    deadline=deadline,
                    retry_client_factory=build_retry_client,
                )
            except BaseException as exc:
                self._observe_phase("converse", phase_started, error=exc)
                raise
            self._observe_phase("converse", phase_started)
            return result
        except BaseException as exc:
            operation_error = exc
            raise
        finally:
            cleanup_started = time.monotonic()
            credential_clients = resolved_runtime.credential_clients if resolved_runtime is not None else ()
            session = resolved_runtime.session if resolved_runtime is not None else None
            cleanup_error = self._close_resources(
                (*clients, *credential_clients, session),
                event="bedrock_operation_cleanup_failed",
            )
            cleanup_exception = (
                terminal_cleanup_failure(
                    operation_error,
                    cleanup_error,
                    reason_code="bedrock_operation_cleanup_failed",
                    message="AWS Bedrock operation cleanup failed",
                    retain_capacity=True,
                )
                if cleanup_error is not None
                else None
            )
            self._observe_phase("cleanup", cleanup_started, error=cleanup_exception)
            if cleanup_exception is not None:
                raise cleanup_exception

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        system = f"{system_prompt}\n\n" "Respond ONLY with a valid JSON object. No markdown, no explanation."
        result = await self._run_converse(system, user_prompt, temperature)
        logger.debug("bedrock_raw", raw=result.text[:500])
        return result

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        return await self._run_converse(system_prompt, user_prompt, temperature)

    async def _run_converse(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> LLMResult:
        if self._closed.is_set():
            raise RuntimeOwnershipError("AWS Bedrock provider is closed")
        deadline = current_pipeline_execution_deadline()
        if deadline is None:
            deadline = time.monotonic() + float(self._settings.pipeline_timeout_seconds)
        return await self._run_owned(
            lambda: self._execute_converse_operation(
                system_prompt,
                user_prompt,
                temperature,
                deadline=deadline,
            ),
            reason_code="bedrock_request_worker_retained",
            deadline=deadline,
        )

    async def close(self) -> None:
        """Reject future calls; in-flight workers own their own cleanup."""
        self._closed.set()

    def close_blocking(self) -> None:
        """Close an unmanaged provider without bypassing a bound runtime owner."""
        if getattr(self, "_lifecycle_invocation_owner", None) is not None:
            raise RuntimeOwnershipError("Managed LLM providers can only be closed by their runtime owner")
        self._closed.set()
