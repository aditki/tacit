"""A2A (Agent-to-Agent) context provider.

Connects to a company's knowledge agent via Google's A2A protocol.
The company hosts an A2A-compatible agent that can answer questions
about services, runbooks, architecture, and past incidents.

A2A spec: https://google.github.io/A2A/
"""

from __future__ import annotations

import uuid

import httpx
import structlog

from dashforge.config import Settings, settings
from dashforge.context.base import ContextProvider
from dashforge.models.schemas import ContextChunk, Intent

logger = structlog.get_logger()


class A2AProvider(ContextProvider):

    @property
    def name(self) -> str:
        return "a2a"

    def __init__(self, runtime_settings: Settings | None = None):
        runtime_settings = runtime_settings or settings
        self._agent_url = runtime_settings.context_a2a_agent_url.rstrip("/")
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
            # A2A uses a task-based model: send a task, get a response
            task_id = str(uuid.uuid4())
            payload = {
                "jsonrpc": "2.0",
                "id": task_id,
                "method": "tasks/send",
                "params": {
                    "id": task_id,
                    "message": {
                        "role": "user",
                        "parts": [
                            {
                                "type": "text",
                                "text": search_query,
                            }
                        ],
                    },
                },
            }

            resp = await self._client.post(self._agent_url, json=payload)
            resp.raise_for_status()
            result = resp.json()

            # Parse the A2A response — the agent returns artifacts
            chunks: list[ContextChunk] = []
            task_result = result.get("result", {})

            # Extract from artifacts (structured output from the agent)
            for artifact in task_result.get("artifacts", []):
                for part in artifact.get("parts", []):
                    if part.get("type") == "text":
                        chunks.append(
                            ContextChunk(
                                content=part["text"],
                                source=f"a2a:{self._agent_url}",
                                relevance_score=0.8,
                                metadata={
                                    "a2a_agent": self._agent_url,
                                    "task_id": task_id,
                                },
                            )
                        )

            # Also extract from message parts in the status
            status = task_result.get("status", {})
            if status.get("message"):
                for part in status["message"].get("parts", []):
                    if part.get("type") == "text":
                        chunks.append(
                            ContextChunk(
                                content=part["text"],
                                source=f"a2a:{self._agent_url}",
                                relevance_score=0.7,
                                metadata={"a2a_agent": self._agent_url},
                            )
                        )

            logger.info("a2a_context_retrieved", chunks=len(chunks))
            return chunks[:max_chunks]

        except Exception:
            logger.exception("a2a_query_failed", agent_url=self._agent_url)
            return []

    async def close(self) -> None:
        await self._client.aclose()


def _build_search_query(intent: Intent) -> str:
    """Build a query for the A2A knowledge agent."""
    parts = [
        f"I'm investigating: {intent.summary}",
    ]
    if intent.services:
        parts.append(
            f"What do you know about these services: {', '.join(intent.services)}? "
            f"Include: architecture, dependencies, key metrics to watch, known failure modes, "
            f"and relevant runbooks."
        )
    if intent.keywords:
        parts.append(f"Relevant signals: {', '.join(intent.keywords)}")
    return " ".join(parts)
