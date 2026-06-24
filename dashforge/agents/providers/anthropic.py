"""Anthropic (Claude) provider."""

from __future__ import annotations

import anthropic
import structlog

from dashforge.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from dashforge.config import Settings, settings

logger = structlog.get_logger()

JSON_PREAMBLE = "Respond ONLY with a valid JSON object. No markdown, no explanation."

# Newer Claude models (Opus 4.8+) reject the `temperature` parameter. We discover
# this at runtime and remember it per-model so we only pay the failed call once.
_NO_TEMPERATURE_MODELS: set[str] = set()


def _response_text(response) -> str:
    for block in response.content:
        text = getattr(block, "text", None)
        if isinstance(text, str):
            return text
    return ""


def _is_temperature_unsupported(exc: Exception) -> bool:
    """True if a 400 says the model doesn't accept `temperature`."""
    msg = str(getattr(exc, "message", "") or exc).lower()
    return "temperature" in msg and ("deprecated" in msg or "unsupported" in msg or "not supported" in msg)


class AnthropicProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings | None = None):
        self._settings = runtime_settings or settings
        self._client = anthropic.AsyncAnthropic(api_key=self._settings.llm_api_key)

    def _extract_usage(self, response) -> TokenUsage:
        usage = getattr(response, "usage", None)
        if usage:
            inp = getattr(usage, "input_tokens", 0) or 0
            out = getattr(usage, "output_tokens", 0) or 0
            return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)
        return TokenUsage()

    async def _create(self, system: str, user_prompt: str, temperature: float):
        """Call messages.create, omitting `temperature` for models that reject it.

        Uses two explicit call shapes (rather than a dynamic kwargs dict) so the
        SDK's typed overloads still apply.
        """
        model = self._settings.llm_model

        async def _without_temperature():
            return await self._client.messages.create(
                model=model,
                max_tokens=4096,
                system=system,
                messages=[{"role": "user", "content": user_prompt}],
            )

        if model in _NO_TEMPERATURE_MODELS:
            return await _without_temperature()
        try:
            return await self._client.messages.create(
                model=model,
                max_tokens=4096,
                temperature=temperature,
                system=system,
                messages=[{"role": "user", "content": user_prompt}],
            )
        except anthropic.BadRequestError as exc:
            if _is_temperature_unsupported(exc):
                logger.info("anthropic_temperature_unsupported", model=model)
                _NO_TEMPERATURE_MODELS.add(model)
                return await _without_temperature()
            raise

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        system = f"{system_prompt}\n\n{JSON_PREAMBLE}"
        response = await self._create(system, user_prompt, temperature)
        raw = _response_text(response)
        logger.debug("anthropic_raw", raw=raw[:500])
        return LLMResult(text=raw, usage=self._extract_usage(response))

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        response = await self._create(system_prompt, user_prompt, temperature)
        return LLMResult(text=_response_text(response), usage=self._extract_usage(response))

    async def close(self) -> None:
        await self._client.close()
