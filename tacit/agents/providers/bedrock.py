"""AWS Bedrock provider.

Supports explicit credentials and an allowlisted subset of Botocore's file and
web-identity providers. Unsupported ambient providers fail before SDK use.

Requires `boto3` to be installed (optional dependency).
"""

from __future__ import annotations

import asyncio
import configparser
import io
import os
import tempfile
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

import structlog

from tacit.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from tacit.config import Settings, settings
from tacit.errors import RuntimeOwnershipError
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

_BEDROCK_DEFAULT_MODEL = "anthropic.claude-sonnet-4-20250514-v1:0"

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
    "ap.",
    "sa.",
    "me.",
    "ca.",
    "af.",
    "global.",
)

# Map AWS region prefix to inference profile prefix.
# Only us. and eu. have documented geo-specific inference profiles;
# all other regions (ap, sa, me, ca, af) use the global. profile.
_REGION_INFERENCE_PREFIX: dict[str, str] = {
    "us": "us",
    "eu": "eu",
}

# Prefixes that indicate a model ID is already an inference profile
_INFERENCE_PROFILE_PREFIXES = ("us.", "eu.", "ap.", "sa.", "me.", "ca.", "af.", "global.")


def _inference_profile_id(bare_model_id: str, region: str) -> str:
    """Prefix a bare model ID with the region's inference profile prefix.

    Uses ``us.`` / ``eu.`` for those geo regions and ``global.`` for
    everything else (ap-*, sa-*, me-*, etc.), matching AWS's documented
    inference profile availability.

    Example: ('anthropic.claude-sonnet-4-20250514-v1:0', 'us-east-1')
             -> 'us.anthropic.claude-sonnet-4-20250514-v1:0'
    Example: ('anthropic.claude-sonnet-4-20250514-v1:0', 'ap-northeast-1')
             -> 'global.anthropic.claude-sonnet-4-20250514-v1:0'
    """
    region_prefix = region.split("-")[0]  # "us-east-1" -> "us"
    prefix = _REGION_INFERENCE_PREFIX.get(region_prefix, "global")
    return f"{prefix}.{bare_model_id}"


# Cache for resolved model IDs — avoids repeated ListFoundationModels calls
_resolve_cache: dict[str, str] = {}


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
    """Private credential snapshot and clients retained by one provider generation."""

    session: _Boto3Session = field(repr=False)
    credential_identity: BedrockCredentialIdentity
    credential_clients: tuple[object, ...] = field(default=(), repr=False)


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


def _resolve_bedrock_model_id(anthropic_model_name: str, bedrock_client) -> str:
    """Resolve an Anthropic API model name to a Bedrock model ID.

    Strategy:
    0. If already provider-prefixed (e.g. meta.llama3-*), use as-is
    1. Check cache
    2. Call ListFoundationModels API to find a matching model ID
    3. Fall back to static _ANTHROPIC_TO_BEDROCK map
    4. Fall back to _BEDROCK_DEFAULT_MODEL
    """
    # Already a valid Bedrock model ID — pass through
    if anthropic_model_name.startswith(_BEDROCK_PROVIDER_PREFIXES):
        logger.info(
            "bedrock_model_resolved", source="passthrough", input=anthropic_model_name, resolved=anthropic_model_name
        )
        return anthropic_model_name

    if anthropic_model_name in _resolve_cache:
        return _resolve_cache[anthropic_model_name]

    # Try runtime resolution via ListFoundationModels
    try:
        resp = bedrock_client.list_foundation_models()
        for model in resp.get("modelSummaries", []):
            model_id = model.get("modelId", "")
            if anthropic_model_name in model_id:
                _resolve_cache[anthropic_model_name] = model_id
                logger.info("bedrock_model_resolved", source="api", input=anthropic_model_name, resolved=model_id)
                return model_id
    except Exception as exc:
        logger.debug("bedrock_list_models_failed", error=str(exc))

    # Fall back to static map, then default.
    # Returns the bare foundation model ID; _converse() will auto-retry
    # with an inference profile prefix if invocation fails.
    resolved = _ANTHROPIC_TO_BEDROCK.get(anthropic_model_name, _BEDROCK_DEFAULT_MODEL)
    _resolve_cache[anthropic_model_name] = resolved
    logger.info("bedrock_model_resolved", source="static_map", input=anthropic_model_name, resolved=resolved)
    return resolved


def _build_boto3_session(
    runtime_settings: Settings | None = None,
    *,
    environment: dict[str, str] | None = None,
    credential_plan: BedrockCredentialPlan | None = None,
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
    environment = credential_plan.environment
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("AWS Bedrock provider requires boto3. " "Install it with: pip install boto3") from exc

    profile_name = credential_plan.profile
    ambient_access_key = str(environment.get("AWS_ACCESS_KEY_ID") or "")
    ambient_secret_key = str(environment.get("AWS_SECRET_ACCESS_KEY") or "")
    ambient_session_token = str(environment.get("AWS_SECURITY_TOKEN") or environment.get("AWS_SESSION_TOKEN") or "")

    auth_method: str
    selected_source_sts = False
    if runtime_settings.llm_aws_access_key_id or runtime_settings.llm_aws_secret_access_key:
        if not runtime_settings.llm_aws_access_key_id or not runtime_settings.llm_aws_secret_access_key:
            raise RuntimeOwnershipError("AWS credentials must include both access key and secret key")
        frozen = _FrozenBedrockCredentials(
            access_key=runtime_settings.llm_aws_access_key_id,
            secret_key=runtime_settings.llm_aws_secret_access_key,
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

    def pinned_session(credentials: _FrozenBedrockCredentials) -> _Boto3Session:
        kwargs = {
            "region_name": runtime_settings.llm_bedrock_region,
            "aws_access_key_id": credentials.access_key,
            "aws_secret_access_key": credentials.secret_key,
        }
        if credentials.token:
            kwargs["aws_session_token"] = credentials.token
        return boto3.Session(**kwargs)

    credential_clients: tuple[object, ...] = ()
    if runtime_settings.llm_bedrock_role_arn:
        sts = pinned_session(frozen).client(
            "sts",
            endpoint_url=canonical_aws_sts_endpoint(runtime_settings.llm_bedrock_region),
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

    session = pinned_session(frozen)
    logger.info("bedrock_auth", method=auth_method, region=runtime_settings.llm_bedrock_region)
    return _ResolvedBedrockRuntime(
        session=session,
        credential_identity=credential_plan.realized_identity(
            fallback_account=frozen.account,
            credential_fingerprint_value=frozen.fingerprint,
            source_uses_sts=selected_source_sts,
        ),
        credential_clients=credential_clients,
    )


class BedrockProvider(LLMProvider):
    """AWS Bedrock LLM provider.

    Uses the Bedrock Runtime `converse` API which provides a unified
    interface across all Bedrock foundation models (Claude, Llama, Mistral, etc.).
    """

    def __init__(
        self,
        runtime_settings: Settings | None = None,
        *,
        credential_plan: BedrockCredentialPlan | None = None,
    ):
        if credential_plan is None:
            credential_plan = BedrockCredentialPlan.capture(runtime_settings or settings)
        elif runtime_settings is not None:
            supplied_settings = snapshot_runtime_settings(runtime_settings)
            if supplied_settings != credential_plan.runtime_settings:
                raise RuntimeOwnershipError("AWS Bedrock credential plan settings do not match")
        self._credential_plan = credential_plan
        self._runtime_settings = credential_plan.runtime_settings
        self._settings = self.runtime_settings
        resolved_runtime = _build_boto3_session(credential_plan=credential_plan)
        if isinstance(resolved_runtime, _ResolvedBedrockRuntime):
            self._session = resolved_runtime.session
            bedrock_credential_identity = resolved_runtime.credential_identity
            self._credential_clients = list(resolved_runtime.credential_clients)
        else:
            # Test doubles predating credential snapshots remain usable, while
            # production construction always returns _ResolvedBedrockRuntime.
            self._session = cast(_Boto3Session, resolved_runtime)
            bedrock_credential_identity = None
            self._credential_clients = []
        self._bedrock_credential_identity = bedrock_credential_identity
        self._runtime_ownership = credential_plan.ownership(
            component="bedrock_llm_provider",
            credential_identity=bedrock_credential_identity,
        )
        self._client = None
        self._client_lock = threading.Lock()
        self._configured_model = self._settings.llm_model
        if self._settings.llm_bedrock_model_id:
            self._model_id = self._settings.llm_bedrock_model_id
        elif self._configured_model.startswith(_BEDROCK_PROVIDER_PREFIXES):
            self._model_id = self._configured_model
        else:
            self._model_id = _ANTHROPIC_TO_BEDROCK.get(self._configured_model, _BEDROCK_DEFAULT_MODEL)
        logger.info(
            "bedrock_configured",
            model_id=self._model_id,
            region=self._settings.llm_bedrock_region,
        )

    @property
    def bedrock_credential_identity(self) -> BedrockCredentialIdentity | None:
        """Return the non-secret credential identity realized from this plan."""
        return self._bedrock_credential_identity

    def bedrock_ownership_declarations(
        self,
        *,
        component: str,
    ) -> tuple[RuntimeOwnershipDescriptor, RuntimeOwnershipDescriptor] | None:
        """Return non-secret planned and realized ownership for admission."""
        if self._bedrock_credential_identity is None:
            return None
        return (
            self._credential_plan.ownership(component=component),
            self._credential_plan.ownership(
                component=component,
                credential_identity=self._bedrock_credential_identity,
            ),
        )

    def _ensure_client(self):
        """Create the pinned runtime client only on first use."""
        if self._client is not None:
            return self._client
        with self._client_lock:
            if self._client is not None:
                return self._client
            self._client = self._session.client(
                "bedrock-runtime",
                endpoint_url=canonical_bedrock_runtime_endpoint(self._settings.llm_bedrock_region),
            )
            logger.info(
                "bedrock_initialized",
                model_id=self._model_id,
                region=self._settings.llm_bedrock_region,
            )
            return self._client

    # Model families that do NOT support the Converse ``system`` parameter.
    # For these, we fold the system prompt into the first user message.
    _NO_SYSTEM_PREFIXES = ("mistral.",)

    def _build_converse_kwargs(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> dict:
        """Build kwargs for client.converse(), handling model-family quirks.

        Mistral AI Instruct models accept Converse but reject the ``system``
        field (AWS model-feature table), so the system prompt is folded into
        the user message for those families.
        """
        model_id = self._model_id

        # Mistral (and any future no-system families): fold system into user msg
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
    def _extract_usage(response: dict) -> TokenUsage:
        usage = response.get("usage", {})
        inp = usage.get("inputTokens", 0) or 0
        out = usage.get("outputTokens", 0) or 0
        return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)

    def _converse(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> LLMResult:
        """Call Bedrock Converse API (sync — wrapped async by callers).

        If the bare model ID fails with a ValidationException (common for
        models that require inference profiles, e.g. Sonnet 4 in us-east-1),
        automatically retries with a region-appropriate inference profile ID
        and caches the working ID for subsequent calls.
        """
        client = self._ensure_client()
        kwargs = self._build_converse_kwargs(system_prompt, user_prompt, temperature)

        try:
            response = client.converse(**kwargs)
        except Exception as exc:
            if not self._should_retry_with_profile(exc):
                raise
            # Retry with inference profile
            profile_id = _inference_profile_id(self._model_id, self._settings.llm_bedrock_region)
            logger.warning("bedrock_model_retry_with_profile", bare=self._model_id, profile=profile_id)
            kwargs["modelId"] = profile_id
            response = client.converse(**kwargs)
            # Success — cache the working profile ID for future calls
            self._model_id = profile_id
            logger.info("bedrock_model_updated", model_id=profile_id)

        # Extract text from the response
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])
        text_parts = [b["text"] for b in content_blocks if "text" in b]
        return LLMResult(text="".join(text_parts), usage=self._extract_usage(response))

    def _should_retry_with_profile(self, exc: Exception) -> bool:
        """Return True if the exception indicates the model needs an inference
        profile and the current model ID is a bare (non-prefixed) ID."""
        if type(exc).__name__ != "ValidationException":
            return False
        # Already an inference profile ID — don't double-prefix
        if self._model_id.startswith(_INFERENCE_PROFILE_PREFIXES):
            return False
        return True

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        system = f"{system_prompt}\n\n" "Respond ONLY with a valid JSON object. No markdown, no explanation."
        result = await asyncio.to_thread(self._converse, system, user_prompt, temperature)
        logger.debug("bedrock_raw", raw=result.text[:500])
        return result

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        return await asyncio.to_thread(self._converse, system_prompt, user_prompt, temperature)

    async def close(self) -> None:
        with self._client_lock:
            client = self._client
            self._client = None
            resources = [client, *self._credential_clients]
            self._credential_clients = []

        async def close_resource(resource: object) -> BaseException | None:
            close = getattr(resource, "close", None)
            if close is None:
                return None
            try:
                await asyncio.to_thread(close)
            except BaseException as exc:
                return exc
            return None

        results = await asyncio.gather(
            *(close_resource(resource) for resource in resources if resource is not None),
        )
        for result in results:
            if result is not None:
                raise result
