"""Ollama provider — local models via the Ollama HTTP API."""
from __future__ import annotations

import httpx
import structlog

from dashforge.agents.providers.base import LLMProvider
from dashforge.config import settings

logger = structlog.get_logger()


class OllamaProvider(LLMProvider):
    def __init__(self):
        base = settings.llm_api_base or "http://localhost:11434"
        self._base_url = base.rstrip("/")
        self._client = httpx.AsyncClient(timeout=120.0)

    async def chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.2,
    ) -> str:
        payload = {
            "model": settings.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "format": "json",
            "options": {"temperature": temperature},
        }
        resp = await self._client.post(
            f"{self._base_url}/api/chat", json=payload
        )
        resp.raise_for_status()
        raw = resp.json()["message"]["content"]
        logger.debug("ollama_raw", raw=raw[:500])
        return raw

    async def chat_text(
        self,
        system_prompt: str,
        user_prompt: str,
        temperature: float = 0.3,
    ) -> str:
        payload = {
            "model": settings.llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
            "options": {"temperature": temperature},
        }
        resp = await self._client.post(
            f"{self._base_url}/api/chat", json=payload
        )
        resp.raise_for_status()
        return resp.json()["message"]["content"]
