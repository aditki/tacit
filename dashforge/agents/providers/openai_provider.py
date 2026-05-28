"""OpenAI / Azure OpenAI providers."""
from __future__ import annotations

import openai
import structlog

from dashforge.agents.providers.base import LLMProvider
from dashforge.config import settings

logger = structlog.get_logger()


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
    ) -> str:
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
        return raw

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> str:
        response = await self._client.chat.completions.create(
            model=settings.llm_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return response.choices[0].message.content or ""


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
    ) -> str:
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
        return raw

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> str:
        response = await self._client.chat.completions.create(
            model=self._deployment,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
        )
        return response.choices[0].message.content or ""
