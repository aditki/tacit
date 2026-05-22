"""MCP (Model Context Protocol) context provider.

Connects to an MCP server that wraps the company's internal knowledge base.
The MCP server is hosted behind the company's auth layer — DashForge only
needs the server URL and an optional auth token.

MCP spec: https://modelcontextprotocol.io
"""
from __future__ import annotations

import httpx
import structlog

from dashforge.config import settings
from dashforge.context.base import ContextProvider
from dashforge.models.schemas import ContextChunk, Intent

logger = structlog.get_logger()


class MCPProvider(ContextProvider):

    @property
    def name(self) -> str:
        return "mcp"

    def __init__(self):
        self._server_url = settings.context_mcp_server_url.rstrip("/")
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if settings.context_api_key:
            headers["Authorization"] = f"Bearer {settings.context_api_key}"
        self._client = httpx.AsyncClient(
            base_url=self._server_url,
            headers=headers,
            timeout=30.0,
        )

    async def query(
        self,
        intent: Intent,
        max_chunks: int = 10,
    ) -> list[ContextChunk]:
        # Build the MCP tool call — we call the "search" or "query" resource
        # on the MCP server.  The exact resource name is configurable.
        search_query = _build_search_query(intent)

        try:
            # MCP uses JSON-RPC 2.0 over HTTP (SSE transport or streamable HTTP)
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": settings.context_mcp_tool_name,
                    "arguments": {
                        "query": search_query,
                        "max_results": max_chunks,
                    },
                },
            }

            resp = await self._client.post("/mcp", json=payload)
            resp.raise_for_status()
            result = resp.json()

            # Parse the MCP response
            chunks: list[ContextChunk] = []
            content_items = result.get("result", {}).get("content", [])

            for item in content_items[:max_chunks]:
                if item.get("type") == "text":
                    chunks.append(
                        ContextChunk(
                            content=item["text"],
                            source=f"mcp:{settings.context_mcp_tool_name}",
                            relevance_score=item.get("score", 0.0),
                            metadata={"mcp_server": self._server_url},
                        )
                    )

            logger.info("mcp_context_retrieved", chunks=len(chunks))
            return chunks

        except Exception:
            logger.exception("mcp_query_failed", server=self._server_url)
            return []


def _build_search_query(intent: Intent) -> str:
    """Build a natural-language search query from the intent."""
    parts = [intent.summary]
    if intent.services:
        parts.append(f"Services: {', '.join(intent.services)}")
    if intent.keywords:
        parts.append(f"Keywords: {', '.join(intent.keywords)}")
    return " | ".join(parts)
