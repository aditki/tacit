"""Shared LLM helpers used by all agents."""
from __future__ import annotations

import json
import re
from typing import Type, TypeVar

import httpx
import structlog
from pydantic import BaseModel, ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dashforge.agents.providers import get_provider

logger = structlog.get_logger()

T = TypeVar("T", bound=BaseModel)


class LLMTransientError(Exception):
    """Wraps transient LLM errors that are safe to retry."""


class LLMParseError(Exception):
    """LLM returned unparseable or invalid output. Do NOT retry blindly."""


def _attempt_json_repair(raw: str) -> str | None:
    """Try lightweight programmatic fixes for common LLM JSON issues.

    Returns repaired JSON string or None if repair failed.
    """
    text = raw.strip()

    # Strip markdown code fences: ```json ... ``` or ``` ... ```
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()

    # Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)

    # Try parsing the repaired text
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        return None


_REPAIR_SYSTEM_PROMPT = """\
You are a JSON repair tool. The following text was intended to be valid JSON \
but has syntax errors. Fix the JSON and return ONLY the corrected JSON object. \
Do not add commentary, markdown fences, or extra text. Preserve all data."""


@retry(
    retry=retry_if_exception_type(LLMTransientError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=10),
)
async def call_llm(
    system_prompt: str,
    user_prompt: str,
    response_model: Type[T],
    temperature: float = 0.2,
) -> T:
    """Call the configured LLM provider and parse JSON into *response_model*."""
    provider = get_provider()

    try:
        raw = await provider.chat_json(system_prompt, user_prompt, temperature)
    except (httpx.TimeoutException, httpx.NetworkError, ConnectionError, OSError) as exc:
        logger.warning("llm_transient_error", error=str(exc))
        raise LLMTransientError(str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code in {429, 500, 502, 503, 529}:
            logger.warning("llm_rate_or_server_error", status=exc.response.status_code)
            raise LLMTransientError(str(exc)) from exc
        raise  # 401, 403, 400 etc. are not retryable

    logger.debug("llm_raw_response", raw=raw[:500])

    # ── Parse JSON (with repair fallback) ─────────────────────────────
    parsed = None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Step 1: lightweight programmatic repair
        repaired = _attempt_json_repair(raw)
        if repaired is not None:
            logger.info("llm_json_repaired", method="programmatic")
            parsed = json.loads(repaired)
        else:
            # Step 2: one-shot LLM repair (strict token cap)
            logger.warning("llm_json_repair_attempting", raw_preview=raw[:200])
            try:
                repair_raw = await provider.chat_json(
                    _REPAIR_SYSTEM_PROMPT,
                    raw[:4000],  # cap input to avoid cost blowout
                    temperature=0.0,
                )
                parsed = json.loads(repair_raw)
                logger.info("llm_json_repaired", method="llm_reask")
            except (httpx.TimeoutException, httpx.NetworkError, ConnectionError, OSError) as repair_exc:
                logger.warning("llm_repair_transient_error", error=str(repair_exc))
                raise LLMTransientError(str(repair_exc)) from repair_exc
            except httpx.HTTPStatusError as repair_exc:
                if repair_exc.response.status_code in {429, 500, 502, 503, 529}:
                    logger.warning("llm_repair_rate_or_server_error", status=repair_exc.response.status_code)
                    raise LLMTransientError(str(repair_exc)) from repair_exc
                logger.error("llm_json_repair_failed", error=str(repair_exc), raw=raw[:300])
                raise LLMParseError(f"LLM returned invalid JSON (repair failed): {repair_exc}") from repair_exc
            except json.JSONDecodeError as repair_exc:
                logger.error("llm_json_repair_failed", error=str(repair_exc), raw=raw[:300])
                raise LLMParseError(f"LLM returned invalid JSON (repair failed): {repair_exc}") from repair_exc
            except Exception as repair_exc:
                # Provider SDK exceptions (OpenAI APIConnectionError, Anthropic
                # rate limits, botocore throttling, etc.) that aren't httpx types.
                # Check if the exception looks transient before giving up.
                _TRANSIENT_EXC_NAMES = {
                    "APIConnectionError", "APITimeoutError", "RateLimitError",
                    "InternalServerError", "ServiceUnavailableError",
                    "ThrottlingException", "TooManyRequestsException",
                    "ServiceUnavailableException", "ModelTimeoutException",
                    "EndpointConnectionError", "ReadTimeoutError",
                    "ConnectTimeoutError",
                }
                exc_name = type(repair_exc).__name__
                if exc_name in _TRANSIENT_EXC_NAMES:
                    logger.warning("llm_repair_transient_error", error=str(repair_exc), exc_type=exc_name)
                    raise LLMTransientError(str(repair_exc)) from repair_exc
                # Check for response-based error codes (botocore ClientError)
                if hasattr(repair_exc, "response") and isinstance(repair_exc.response, dict):
                    err_code = repair_exc.response.get("Error", {}).get("Code", "")
                    if err_code in {"ThrottlingException", "TooManyRequestsException",
                                    "ServiceUnavailableException", "InternalServerException"}:
                        logger.warning("llm_repair_transient_error", error=str(repair_exc), code=err_code)
                        raise LLMTransientError(str(repair_exc)) from repair_exc
                # Check for status_code attribute (common in provider SDKs)
                status = getattr(repair_exc, "status_code", None) or getattr(repair_exc, "status", None)
                if isinstance(status, int) and status in {429, 500, 502, 503, 529}:
                    logger.warning("llm_repair_transient_error", error=str(repair_exc), status=status)
                    raise LLMTransientError(str(repair_exc)) from repair_exc
                logger.error("llm_json_repair_failed", error=str(repair_exc), raw=raw[:300])
                raise LLMParseError(f"LLM returned invalid JSON (repair failed): {repair_exc}") from repair_exc

    try:
        return response_model.model_validate(parsed)
    except ValidationError as exc:
        logger.error("llm_validation_error", errors=exc.error_count())
        raise LLMParseError(f"LLM output failed validation: {exc}") from exc


async def call_llm_text(
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.3,
) -> str:
    """Return plain text from the LLM (no structured output)."""
    provider = get_provider()
    return await provider.chat_text(system_prompt, user_prompt, temperature)
