"""Anthropic (Claude) provider."""
from __future__ import annotations

import anthropic
import structlog

from dashforge.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from dashforge.config import settings

logger = structlog.get_logger()

JSON_PREAMBLE = "Respond ONLY with a valid JSON object. No markdown, no explanation."


class AnthropicProvider(LLMProvider):
    def __init__(self):
        self._client = anthropic.AsyncAnthropic(api_key=settings.llm_api_key)

    def _extract_usage(self, response) -> TokenUsage:
        usage = getattr(response, "usage", None)
        if usage:
            inp = getattr(usage, "input_tokens", 0) or 0
            out = getattr(usage, "output_tokens", 0) or 0
            return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)
        return TokenUsage()

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
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
        return LLMResult(text=raw, usage=self._extract_usage(response))

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        response = await self._client.messages.create(
            model=settings.llm_model,
            max_tokens=4096,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return LLMResult(text=response.content[0].text, usage=self._extract_usage(response))
