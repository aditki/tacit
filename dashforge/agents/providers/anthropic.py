"""Anthropic (Claude) provider."""
from __future__ import annotations

import anthropic
import structlog

from dashforge.agents.providers.base import LLMProvider
from dashforge.config import settings

logger = structlog.get_logger()

JSON_PREAMBLE = "Respond ONLY with a valid JSON object. No markdown, no explanation."


class AnthropicProvider(LLMProvider):
    def __init__(self):
        self._client = anthropic.AsyncAnthropic(api_key=settings.llm_api_key)

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> str:
        system = f"{system_prompt}\n\n{JSON_PREAMBLE}"
        response = await self._client.messages.create(
            model=settings.llm_model,
            max_tokens=4096,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = response.content[0].text
        logger.debug("anthropic_raw", raw=raw[:500])
        return raw

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> str:
        response = await self._client.messages.create(
            model=settings.llm_model,
            max_tokens=4096,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return response.content[0].text
