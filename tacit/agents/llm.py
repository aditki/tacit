"""Shared LLM helpers used by all agents."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from pydantic import BaseModel, ValidationError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from tacit.agents.llm_errors import LLMParseError, LLMTransientError, wrap_transient_llm_error
from tacit.agents.llm_json import (
    attempt_json_repair as _attempt_json_repair,
)
from tacit.agents.llm_json import (
    parse_json_with_repair,
)
from tacit.agents.llm_json import (
    strip_trailing_commas as _strip_trailing_commas,
)
from tacit.agents.providers.base import LLMProvider, TokenUsage
from tacit.config import Settings, settings

logger = structlog.get_logger()


@asynccontextmanager
async def _provider_scope(
    provider: LLMProvider | None,
    runtime_settings: Settings | None,
) -> AsyncIterator[LLMProvider]:
    if provider is not None:
        yield provider
        return
    from tacit.dependencies import managed_async_nonpipeline_llm_provider

    async with managed_async_nonpipeline_llm_provider(runtime_settings or settings) as owned_provider:
        yield owned_provider


@retry(
    retry=retry_if_exception_type(LLMTransientError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=10),
)
async def call_llm[T: BaseModel](
    system_prompt: str,
    user_prompt: str,
    response_model: type[T],
    temperature: float = 0.2,
    *,
    provider: LLMProvider | None = None,
    runtime_settings: Settings | None = None,
) -> tuple[T, TokenUsage]:
    """Call an LLM provider and parse JSON into *response_model*."""
    total_usage = TokenUsage()
    async with _provider_scope(provider, runtime_settings) as active_provider:
        try:
            result = await active_provider.chat_json(system_prompt, user_prompt, temperature)
        except Exception as exc:
            transient = wrap_transient_llm_error(exc)
            if transient is not None:
                logger.warning("llm_transient_error", error=str(exc), exc_type=type(exc).__name__)
                raise transient from exc
            raise

        raw = result.text
        total_usage = total_usage + result.usage
        logger.debug("llm_raw_response", raw=raw[:500])

        parsed, repair_usage = await parse_json_with_repair(active_provider, raw)
        total_usage = total_usage + repair_usage

    try:
        return response_model.model_validate(parsed), total_usage
    except ValidationError as exc:
        logger.error("llm_validation_error", errors=exc.error_count())
        raise LLMParseError(f"LLM output failed validation: {exc}") from exc


async def call_llm_text(
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.3,
    *,
    provider: LLMProvider | None = None,
    runtime_settings: Settings | None = None,
) -> tuple[str, TokenUsage]:
    """Return plain text from the LLM (no structured output)."""
    async with _provider_scope(provider, runtime_settings) as active_provider:
        result = await active_provider.chat_text(system_prompt, user_prompt, temperature)
        return result.text, result.usage


__all__ = [
    "LLMParseError",
    "LLMTransientError",
    "_attempt_json_repair",
    "_strip_trailing_commas",
    "call_llm",
    "call_llm_text",
]
