"""Intent classification and context-enrichment stage."""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from dashforge.agents.providers.base import LLMProvider, TokenUsage
from dashforge.context.base import ContextProvider
from dashforge.dependencies import PipelineDependencies
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
    deps: PipelineDependencies,
    classify: Callable[..., Awaitable[tuple[Intent, TokenUsage]]],
    enrich: Callable[..., Awaitable[list[Any]]],
    classify_provider_factory: Callable[[], LLMProvider] | None,
    context_provider_factory: Callable[[], ContextProvider | None] | None,
    timings: dict[str, float],
) -> IntentStageResult:
    """Classify the prompt and fetch optional context chunks."""
    t0 = time.monotonic()
    if "provider" in inspect.signature(classify).parameters:
        classify_provider = classify_provider_factory() if classify_provider_factory else None
        intent, intent_usage = await classify(prompt, provider=classify_provider)
    else:
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
    enrich_parameters = inspect.signature(enrich).parameters
    enrich_kwargs: dict[str, Any] = {}
    if "max_chunks" in enrich_parameters:
        enrich_kwargs["max_chunks"] = deps.settings.context_max_chunks
    if "provider" in enrich_parameters:
        enrich_kwargs["provider"] = context_provider_factory() if context_provider_factory else None
    context_chunks = await enrich(intent, **enrich_kwargs)
    timings["context"] = time.monotonic() - t0
    stage_log(
        "context_enrichment",
        (time.monotonic() - t0) * 1000,
        chunks_returned=len(context_chunks),
    )
    return IntentStageResult(intent=intent, context_chunks=context_chunks, token_usage=intent_usage)
