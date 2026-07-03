"""Intent classification and context-enrichment stage."""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from tacit.agents.intent_fallback import zero_key_mode
from tacit.agents.providers.base import LLMProvider, TokenUsage
from tacit.context.base import ContextProvider
from tacit.dependencies import PipelineDependencies
from tacit.logging import stage_log
from tacit.models.schemas import Intent


class _ZeroKeyProvider(LLMProvider):
    @property
    def is_configured(self) -> bool:
        return False

    async def chat_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.2):
        raise RuntimeError("zero-key fallback provider must not be called")

    async def chat_text(self, system_prompt: str, user_prompt: str, temperature: float = 0.3):
        raise RuntimeError("zero-key fallback provider must not be called")


_ZERO_KEY_PROVIDER = _ZeroKeyProvider()


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
    classify_parameters = inspect.signature(classify).parameters
    if "provider" in classify_parameters:
        classify_provider: LLMProvider | None
        if deps.settings.intent_fallback_enabled and zero_key_mode(deps.settings):
            classify_provider = _ZERO_KEY_PROVIDER
        else:
            classify_provider = classify_provider_factory() if classify_provider_factory else None
        classify_kwargs: dict[str, Any] = {"provider": classify_provider}
        if "runtime_settings" in classify_parameters:
            classify_kwargs["runtime_settings"] = deps.settings
        intent, intent_usage = await classify(prompt, **classify_kwargs)
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
    if "provider" in enrich_parameters and context_provider_factory is not None:
        enrich_kwargs["provider"] = context_provider_factory()
    context_chunks = await enrich(intent, **enrich_kwargs)
    timings["context"] = time.monotonic() - t0
    stage_log(
        "context_enrichment",
        (time.monotonic() - t0) * 1000,
        chunks_returned=len(context_chunks),
    )
    return IntentStageResult(intent=intent, context_chunks=context_chunks, token_usage=intent_usage)
