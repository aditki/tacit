"""Context enrichment orchestrator.

Sits between the Intent Agent and Metrics Discovery in the pipeline.
When a context provider is configured, it queries the company's knowledge
base and returns relevant context chunks.  When not configured, it's a
no-op — the pipeline works exactly as before.
"""

from __future__ import annotations

import asyncio
from typing import cast

import structlog

from dashforge.context.base import ContextProvider
from dashforge.context.registry import get_context_provider
from dashforge.models.schemas import ContextChunk, Intent

logger = structlog.get_logger()

CONTEXT_TIMEOUT = 15  # seconds — context is optional, don't block the pipeline
_PROVIDER_UNSET = object()


async def enrich_context(
    intent: Intent,
    max_chunks: int = 10,
    *,
    provider: ContextProvider | None | object = _PROVIDER_UNSET,
) -> list[ContextChunk]:
    """Query the configured knowledge base for context relevant to the intent.

    Returns an empty list if no context provider is configured (graceful no-op).
    Failures are logged as warnings and never block the pipeline.
    """
    if provider is _PROVIDER_UNSET:
        provider = get_context_provider()
    if provider is None:
        return []
    provider = cast(ContextProvider, provider)

    logger.info(
        "context_enrichment_start",
        provider=provider.name,
        services=intent.services,
        keywords=intent.keywords,
    )

    try:
        chunks = await asyncio.wait_for(
            provider.query(intent, max_chunks=max_chunks),
            timeout=CONTEXT_TIMEOUT,
        )
        # Sort by relevance score (highest first)
        chunks.sort(key=lambda c: c.relevance_score, reverse=True)

        logger.info(
            "context_enrichment_done",
            provider=provider.name,
            chunks_returned=len(chunks),
            sources=list({c.source for c in chunks}),
        )
        return chunks

    except TimeoutError:
        logger.warning("context_enrichment_timeout", provider=provider.name, timeout=CONTEXT_TIMEOUT)
        return []

    except Exception:
        logger.exception("context_enrichment_failed", provider=provider.name)
        return []


def format_context_for_prompt(chunks: list[ContextChunk]) -> str:
    """Format context chunks into a string suitable for injection into agent prompts."""
    if not chunks:
        return ""

    parts = ["## Knowledge Base Context", ""]
    for i, chunk in enumerate(chunks, 1):
        source_label = f" (source: {chunk.source})" if chunk.source else ""
        parts.append(f"### Context {i}{source_label}")
        parts.append(chunk.content)
        parts.append("")

    return "\n".join(parts)
