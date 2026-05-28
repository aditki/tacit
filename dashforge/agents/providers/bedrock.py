"""AWS Bedrock provider.

Supports three authentication strategies (resolved in order):
1. Explicit credentials — LLM_AWS_ACCESS_KEY_ID + LLM_AWS_SECRET_ACCESS_KEY
2. Assume-role — LLM_BEDROCK_ROLE_ARN (uses STS to get temporary creds)
3. Default boto3 chain — env vars, ~/.aws/credentials, instance profile, ECS task role

Requires `boto3` to be installed (optional dependency).
"""
from __future__ import annotations

import json

import structlog

from dashforge.agents.providers.base import LLMProvider
from dashforge.config import settings

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
    "anthropic.", "meta.", "mistral.", "amazon.", "cohere.",
    "ai21.", "stability.",
    # Regional and cross-region inference profile prefixes
    "us.", "eu.", "ap.", "sa.", "me.", "ca.", "af.", "global.",
)

# Map AWS region prefix to inference profile prefix.
# Only us. and eu. have documented geo-specific inference profiles;
# all other regions (ap, sa, me, ca, af) use the global. profile.
_REGION_INFERENCE_PREFIX: dict[str, str] = {
    "us": "us", "eu": "eu",
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
        logger.info("bedrock_model_resolved", source="passthrough",
                    input=anthropic_model_name, resolved=anthropic_model_name)
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
                logger.info("bedrock_model_resolved", source="api",
                            input=anthropic_model_name, resolved=model_id)
                return model_id
    except Exception as exc:
        logger.debug("bedrock_list_models_failed", error=str(exc))

    # Fall back to static map, then default.
    # Returns the bare foundation model ID; _converse() will auto-retry
    # with an inference profile prefix if invocation fails.
    resolved = _ANTHROPIC_TO_BEDROCK.get(anthropic_model_name, _BEDROCK_DEFAULT_MODEL)
    _resolve_cache[anthropic_model_name] = resolved
    logger.info("bedrock_model_resolved", source="static_map",
                input=anthropic_model_name, resolved=resolved)
    return resolved


def _build_boto3_session():
    """Build a boto3.Session with the appropriate credentials."""
    try:
        import boto3
    except ImportError as exc:
        raise ImportError(
            "AWS Bedrock provider requires boto3. "
            "Install it with: pip install boto3"
        ) from exc

    session_kwargs: dict = {
        "region_name": settings.llm_bedrock_region,
    }

    # Strategy 1: explicit key pair
    if settings.llm_aws_access_key_id and settings.llm_aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = settings.llm_aws_access_key_id
        session_kwargs["aws_secret_access_key"] = settings.llm_aws_secret_access_key
        logger.info("bedrock_auth", method="explicit_keys", region=settings.llm_bedrock_region)
        session = boto3.Session(**session_kwargs)
    else:
        # Strategy 3 (default chain): boto3 resolves from env/config/instance profile
        session = boto3.Session(**session_kwargs)
        logger.info("bedrock_auth", method="default_chain", region=settings.llm_bedrock_region)

    # Strategy 2: assume-role with auto-refreshable credentials
    # Uses botocore RefreshableCredentials so the singleton provider
    # doesn't expire after DurationSeconds in long-running processes.
    if settings.llm_bedrock_role_arn:
        from botocore.credentials import RefreshableCredentials
        import botocore.session

        sts = session.client("sts")

        def _refresh_credentials():
            assumed = sts.assume_role(
                RoleArn=settings.llm_bedrock_role_arn,
                RoleSessionName="dashforge-bedrock",
                DurationSeconds=3600,
            )
            creds = assumed["Credentials"]
            return {
                "access_key": creds["AccessKeyId"],
                "secret_key": creds["SecretAccessKey"],
                "token": creds["SessionToken"],
                "expiry_time": creds["Expiration"].isoformat(),
            }

        refreshable_creds = RefreshableCredentials.create_from_metadata(
            metadata=_refresh_credentials(),
            refresh_using=_refresh_credentials,
            method="sts-assume-role",
        )

        botocore_sess = botocore.session.get_session()
        botocore_sess._credentials = refreshable_creds
        botocore_sess.set_config_variable("region", settings.llm_bedrock_region)
        session = boto3.Session(botocore_session=botocore_sess)

        logger.info("bedrock_auth", method="assume_role_refreshable",
                     role_arn=settings.llm_bedrock_role_arn)

    return session


class BedrockProvider(LLMProvider):
    """AWS Bedrock LLM provider.

    Uses the Bedrock Runtime `converse` API which provides a unified
    interface across all Bedrock foundation models (Claude, Llama, Mistral, etc.).
    """

    def __init__(self):
        session = _build_boto3_session()
        self._client = session.client("bedrock-runtime")
        if settings.llm_bedrock_model_id:
            self._model_id = settings.llm_bedrock_model_id
        else:
            # Resolve Anthropic API model name to Bedrock model ID.
            # Returns a bare foundation model ID; _converse() auto-retries
            # with an inference profile prefix if on-demand invocation fails.
            bedrock_ctrl = session.client("bedrock")
            self._model_id = _resolve_bedrock_model_id(
                settings.llm_model, bedrock_ctrl
            )
        logger.info(
            "bedrock_init",
            model_id=self._model_id,
            region=settings.llm_bedrock_region,
        )

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

    def _converse(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float,
    ) -> str:
        """Call Bedrock Converse API (sync — wrapped async by callers).

        If the bare model ID fails with a ValidationException (common for
        models that require inference profiles, e.g. Sonnet 4 in us-east-1),
        automatically retries with a region-appropriate inference profile ID
        and caches the working ID for subsequent calls.
        """
        kwargs = self._build_converse_kwargs(system_prompt, user_prompt, temperature)

        try:
            response = self._client.converse(**kwargs)
        except Exception as exc:
            if not self._should_retry_with_profile(exc):
                raise
            # Retry with inference profile
            profile_id = _inference_profile_id(self._model_id, settings.llm_bedrock_region)
            logger.warning("bedrock_model_retry_with_profile",
                          bare=self._model_id, profile=profile_id)
            kwargs["modelId"] = profile_id
            response = self._client.converse(**kwargs)
            # Success — cache the working profile ID for future calls
            self._model_id = profile_id
            logger.info("bedrock_model_updated", model_id=profile_id)

        # Extract text from the response
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])
        text_parts = [b["text"] for b in content_blocks if "text" in b]
        return "".join(text_parts)

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
    ) -> str:
        import asyncio

        system = (
            f"{system_prompt}\n\n"
            "Respond ONLY with a valid JSON object. No markdown, no explanation."
        )
        raw = await asyncio.to_thread(
            self._converse, system, user_prompt, temperature
        )
        logger.debug("bedrock_raw", raw=raw[:500])
        return raw

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> str:
        import asyncio

        raw = await asyncio.to_thread(
            self._converse, system_prompt, user_prompt, temperature
        )
        return raw
