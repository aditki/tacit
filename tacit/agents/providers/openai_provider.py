"""OpenAI / Azure OpenAI providers."""

from __future__ import annotations

import openai
import structlog

from tacit.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from tacit.agents.providers.http_transport import (
    LLMSDKHTTPClientCloseGuard,
    LLMSDKHTTPClientConstruction,
    create_llm_sdk_http_client,
    isolate_llm_sdk_ambient_credentials,
    isolate_llm_sdk_custom_headers,
)
from tacit.config import Settings, settings
from tacit.runtime_ownership import canonical_remote_endpoint

logger = structlog.get_logger()

_OPENAI_API_ENDPOINT = "https://api.openai.com/v1"


def _extract_openai_usage(response) -> TokenUsage:
    usage = getattr(response, "usage", None)
    if usage:
        inp = getattr(usage, "prompt_tokens", 0) or 0
        out = getattr(usage, "completion_tokens", 0) or 0
        return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)
    return TokenUsage()


class OpenAIProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings | None = None):
        super().__init__(runtime_settings or settings, component="openai_llm_provider")
        self._settings = self.runtime_settings
        runtime_settings = self._settings
        self._client = None
        self._http_client = None
        self._close_guard: LLMSDKHTTPClientCloseGuard | None = None
        if not runtime_settings.llm_api_key and not runtime_settings.llm_api_base:
            return
        construction = LLMSDKHTTPClientConstruction.begin()
        endpoint = canonical_remote_endpoint(runtime_settings.llm_api_base or _OPENAI_API_ENDPOINT)
        with construction:
            http_client = construction.create_http_client(
                lambda: create_llm_sdk_http_client(
                    runtime_settings,
                    endpoint=endpoint,
                )
            )
            kwargs: dict = {
                "api_key": runtime_settings.llm_api_key or "tacit-local-openai-compatible",
                "admin_api_key": "",
                "workload_identity": None,
                "base_url": endpoint,
                # Explicit empty values prevent the SDK from consulting ambient
                # OPENAI_ORG_ID and OPENAI_PROJECT_ID.
                "organization": "",
                "project": "",
                "webhook_secret": "",
                "default_headers": {},
                "http_client": http_client,
            }
            client = construction.create_sdk_client(lambda: openai.AsyncOpenAI(**kwargs))
            isolate_llm_sdk_ambient_credentials(
                client,
                expected_fields=(
                    ("api_key", kwargs["api_key"]),
                    ("admin_api_key", ""),
                    ("workload_identity", None),
                    ("_api_key_provider", None),
                    ("organization", ""),
                    ("project", ""),
                    ("webhook_secret", ""),
                ),
            )
            isolate_llm_sdk_custom_headers(client)
            self._http_client = http_client
            self._client = client
            self._close_guard = construction.commit()

    @property
    def is_configured(self) -> bool:
        return bool(self._settings.llm_api_key or self._settings.llm_api_base)

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        if self._client is None:
            raise ValueError("OpenAI requires LLM_API_KEY")
        response = await self._client.chat.completions.create(
            model=self._settings.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        logger.debug("openai_raw", raw=raw[:500])
        return LLMResult(text=raw, usage=_extract_openai_usage(response))

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        if self._client is None:
            raise ValueError("OpenAI requires LLM_API_KEY")
        response = await self._client.chat.completions.create(
            model=self._settings.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return LLMResult(text=response.choices[0].message.content or "", usage=_extract_openai_usage(response))

    async def close(self) -> None:
        if self._client is not None and self._close_guard is not None:
            await self._close_guard.close(self._client.close)


class AzureOpenAIProvider(LLMProvider):
    """First-class Azure OpenAI provider.

    Uses the ``openai.AsyncAzureOpenAI`` client which handles
    Azure-specific endpoint/version/deployment semantics:
      - ``azure_endpoint``  — e.g. https://my-resource.openai.azure.com
      - ``api_version``     — e.g. 2024-06-01
      - ``azure_deployment``— maps to model in chat calls
    """

    def __init__(self, runtime_settings: Settings | None = None):
        super().__init__(runtime_settings or settings, component="azure_llm_provider")
        self._settings = self.runtime_settings
        runtime_settings = self._settings
        self._client = None
        self._http_client = None
        self._close_guard: LLMSDKHTTPClientCloseGuard | None = None
        if not runtime_settings.llm_api_base:
            if not runtime_settings.llm_api_key:
                self._deployment = runtime_settings.llm_azure_deployment or runtime_settings.llm_model
                return
            raise ValueError(
                "Azure OpenAI requires llm_api_base (azure_endpoint). "
                "Set LLM_API_BASE=https://<resource>.openai.azure.com"
            )
        if not runtime_settings.llm_api_key:
            self._deployment = runtime_settings.llm_azure_deployment or runtime_settings.llm_model
            return
        self._deployment = runtime_settings.llm_azure_deployment or runtime_settings.llm_model
        construction = LLMSDKHTTPClientConstruction.begin()
        endpoint = canonical_remote_endpoint(runtime_settings.llm_api_base)
        with construction:
            http_client = construction.create_http_client(
                lambda: create_llm_sdk_http_client(
                    runtime_settings,
                    endpoint=endpoint,
                )
            )
            client = construction.create_sdk_client(
                lambda: openai.AsyncAzureOpenAI(
                    api_key=runtime_settings.llm_api_key,
                    admin_api_key="",
                    azure_endpoint=endpoint,
                    api_version=runtime_settings.llm_azure_api_version,
                    azure_deployment=self._deployment,
                    # Empty suppresses SDK environment lookup; the isolation
                    # boundary converts it to None before client adoption.
                    azure_ad_token="",
                    azure_ad_token_provider=None,
                    # Azure clients inherit the OpenAI SDK's ambient account fields.
                    # Pin empty values so OPENAI_ORG_ID/OPENAI_PROJECT_ID cannot alter
                    # the accepted runtime owner.
                    organization="",
                    project="",
                    webhook_secret="",
                    default_headers={},
                    http_client=http_client,
                )
            )
            isolate_llm_sdk_ambient_credentials(
                client,
                expected_fields=(
                    ("api_key", runtime_settings.llm_api_key),
                    ("admin_api_key", ""),
                    ("workload_identity", None),
                    ("_api_key_provider", None),
                    ("organization", ""),
                    ("project", ""),
                    ("webhook_secret", ""),
                    ("_azure_ad_token", ""),
                    ("_azure_ad_token_provider", None),
                ),
                normalized_fields=(("_azure_ad_token", None),),
            )
            isolate_llm_sdk_custom_headers(client)
            logger.info(
                "azure_openai_init",
                endpoint=runtime_settings.llm_api_base,
                deployment=self._deployment,
                api_version=runtime_settings.llm_azure_api_version,
            )
            self._http_client = http_client
            self._client = client
            self._close_guard = construction.commit()

    @property
    def is_configured(self) -> bool:
        return bool(self._settings.llm_api_key)

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        if self._client is None:
            raise ValueError("Azure OpenAI requires LLM_API_KEY")
        response = await self._client.chat.completions.create(
            model=self._deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or "{}"
        logger.debug("azure_openai_raw", raw=raw[:500])
        return LLMResult(text=raw, usage=_extract_openai_usage(response))

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        if self._client is None:
            raise ValueError("Azure OpenAI requires LLM_API_KEY")
        response = await self._client.chat.completions.create(
            model=self._deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return LLMResult(text=response.choices[0].message.content or "", usage=_extract_openai_usage(response))

    async def close(self) -> None:
        if self._client is not None and self._close_guard is not None:
            await self._close_guard.close(self._client.close)
