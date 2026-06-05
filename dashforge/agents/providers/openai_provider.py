"""OpenAI / Azure OpenAI providers."""

from __future__ import annotations

import openai
import structlog

from dashforge.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from dashforge.config import settings

logger = structlog.get_logger()


def _extract_openai_usage(response) -> TokenUsage:
    usage = getattr(response, "usage", None)
    if usage:
        inp = getattr(usage, "prompt_tokens", 0) or 0
        out = getattr(usage, "completion_tokens", 0) or 0
        return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)
    return TokenUsage()


class OpenAIProvider(LLMProvider):
    def __init__(self):
        kwargs: dict = {"api_key": settings.llm_api_key}
        if settings.llm_api_base:
            kwargs["base_url"] = settings.llm_api_base
        self._client = openai.AsyncOpenAI(**kwargs)

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        response = await self._client.chat.completions.create(
            model=settings.llm_model,
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
        response = await self._client.chat.completions.create(
            model=settings.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return LLMResult(text=response.choices[0].message.content or "", usage=_extract_openai_usage(response))


class AzureOpenAIProvider(LLMProvider):
    """First-class Azure OpenAI provider.

    Uses the ``openai.AsyncAzureOpenAI`` client which handles
    Azure-specific endpoint/version/deployment semantics:
      - ``azure_endpoint``  — e.g. https://my-resource.openai.azure.com
      - ``api_version``     — e.g. 2024-06-01
      - ``azure_deployment``— maps to model in chat calls
    """

    def __init__(self):
        if not settings.llm_api_base:
            raise ValueError(
                "Azure OpenAI requires llm_api_base (azure_endpoint). "
                "Set LLM_API_BASE=https://<resource>.openai.azure.com"
            )
        self._deployment = settings.llm_azure_deployment or settings.llm_model
        self._client = openai.AsyncAzureOpenAI(
            api_key=settings.llm_api_key,
            azure_endpoint=settings.llm_api_base,
            api_version=settings.llm_azure_api_version,
            azure_deployment=self._deployment,
        )
        logger.info(
            "azure_openai_init",
            endpoint=settings.llm_api_base,
            deployment=self._deployment,
            api_version=settings.llm_azure_api_version,
        )

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
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
        response = await self._client.chat.completions.create(
            model=self._deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return LLMResult(text=response.choices[0].message.content or "", usage=_extract_openai_usage(response))
