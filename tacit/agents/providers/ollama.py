"""Ollama provider — local models via the Ollama HTTP API."""

from __future__ import annotations

import httpx
import structlog

from tacit.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from tacit.config import Settings, settings

logger = structlog.get_logger()


class OllamaProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings | None = None):
        self._settings = runtime_settings or settings
        base = self._settings.llm_api_base or "http://localhost:11434"
        self._base_url = base.rstrip("/")
        self._client = httpx.AsyncClient(timeout=120.0)

    @staticmethod
    def _extract_usage(data: dict) -> TokenUsage:
        inp = data.get("prompt_eval_count", 0) or 0
        out = data.get("eval_count", 0) or 0
        return TokenUsage(prompt_tokens=inp, completion_tokens=out, total_tokens=inp + out)

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> LLMResult:
        payload = {
            "model": self._settings.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "format": "json",
            "options": {"temperature": temperature},
        }
        resp = await self._client.post(f"{self._base_url}/api/chat", json=payload)
        resp.raise_for_status()
        data = resp.json()
        raw = data["message"]["content"]
        logger.debug("ollama_raw", raw=raw[:500])
        return LLMResult(text=raw, usage=self._extract_usage(data))

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> LLMResult:
        payload = {
            "model": self._settings.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"temperature": temperature},
        }
        resp = await self._client.post(f"{self._base_url}/api/chat", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return LLMResult(text=data["message"]["content"], usage=self._extract_usage(data))

    async def close(self) -> None:
        await self._client.aclose()
