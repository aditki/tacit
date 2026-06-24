"""RAG API Gateway context provider.

Connects to a company's RAG service via a simple REST API.
This is the most common enterprise pattern — the company hosts a search
endpoint behind an API gateway with OAuth2, mTLS, or API key auth.

Expected API contract:
  POST {CONTEXT_RAG_API_URL}/search
  Headers: Authorization: Bearer {CONTEXT_API_KEY}
  Body: {"query": "...", "max_results": 10, "filters": {...}}
  Response: {"results": [{"content": "...", "source": "...", "score": 0.9, "metadata": {...}}]}
"""

from __future__ import annotations

import httpx
import structlog

from dashforge.config import Settings, settings
from dashforge.context.base import ContextProvider
from dashforge.models.schemas import ContextChunk, Intent

logger = structlog.get_logger()


class RAGAPIProvider(ContextProvider):

    @property
    def name(self) -> str:
        return "rag_api"

    def __init__(self, runtime_settings: Settings | None = None):
        runtime_settings = runtime_settings or settings
        self._base_url = runtime_settings.context_rag_api_url.rstrip("/")
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if runtime_settings.context_api_key:
            headers["Authorization"] = f"Bearer {runtime_settings.context_api_key}"
        self._client = httpx.AsyncClient(
            headers=headers,
            timeout=30.0,
        )

    async def query(
        self,
        intent: Intent,
        max_chunks: int = 10,
    ) -> list[ContextChunk]:
        search_query = _build_search_query(intent)

        try:
            payload: dict = {
                "query": search_query,
                "max_results": max_chunks,
            }

            # Add optional filters based on intent
            filters: dict = {}
            if intent.services:
                filters["services"] = intent.services
            if intent.domain:
                filters["domain"] = intent.domain
            if filters:
                payload["filters"] = filters

            resp = await self._client.post(
                f"{self._base_url}/search",
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()

            chunks: list[ContextChunk] = []
            for item in data.get("results", []):
                chunks.append(
                    ContextChunk(
                        content=item.get("content", ""),
                        source=item.get("source", f"rag:{self._base_url}"),
                        relevance_score=float(item.get("score", 0.0)),
                        metadata=item.get("metadata", {}),
                    )
                )

            logger.info("rag_api_context_retrieved", chunks=len(chunks))
            return chunks[:max_chunks]

        except Exception:
            logger.exception("rag_api_query_failed", url=self._base_url)
            return []


def _build_search_query(intent: Intent) -> str:
    """Build a search query from the intent."""
    parts = [intent.summary]
    if intent.services:
        parts.append(f"services: {', '.join(intent.services)}")
    if intent.keywords:
        parts.append(f"signals: {', '.join(intent.keywords)}")
    return " | ".join(parts)
