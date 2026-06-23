"""Intent classification and context-enrichment stage."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from dashforge.agents.providers.base import TokenUsage
from dashforge.logging import stage_log
from dashforge.models.schemas import Intent


@dataclass(frozen=True)
class IntentStageResult:
    intent: Intent
    context_chunks: list[Any]
    token_usage: TokenUsage


async def run_intent_stage(
    *,
    prompt: str,
    user_id: str | None,
    classify: Callable[[str], Awaitable[tuple[Intent, TokenUsage]]],
    enrich: Callable[[Intent], Awaitable[list[Any]]],
    timings: dict[str, float],
) -> IntentStageResult:
    """Classify the prompt and fetch optional context chunks."""
    t0 = time.monotonic()
    intent, intent_usage = await classify(prompt)
    timings["intent"] = time.monotonic() - t0
    stage_log(
        "intent",
        (time.monotonic() - t0) * 1000,
        token_usage=intent_usage,
        prompt=prompt[:100],
        user_id=user_id,
        archetypes_detected=len(intent.archetypes),
        domain=intent.domain,
    )

    t0 = time.monotonic()
    context_chunks = await enrich(intent)
    timings["context"] = time.monotonic() - t0
    stage_log(
        "context_enrichment",
        (time.monotonic() - t0) * 1000,
        chunks_returned=len(context_chunks),
    )
    return IntentStageResult(intent=intent, context_chunks=context_chunks, token_usage=intent_usage)
