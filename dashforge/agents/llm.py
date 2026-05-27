"""Shared LLM helpers used by all agents."""
from __future__ import annotations

import json
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
    except Exception as exc:
        # Catch botocore/boto3 transient errors (throttling, service unavailable).
        # Bedrock Runtime raises service-specific exceptions whose type names
        # match the error code directly (e.g. ThrottlingException, not ClientError).
        exc_name = type(exc).__name__
        _TRANSIENT_EXC_NAMES = {
            "EndpointConnectionError", "ReadTimeoutError", "ConnectTimeoutError",
            "ThrottlingException", "TooManyRequestsException",
            "ServiceUnavailableException", "InternalServerException",
            "ModelTimeoutException",
        }
        if exc_name in _TRANSIENT_EXC_NAMES:
            logger.warning("llm_boto_transient_error", error=str(exc), exc_type=exc_name)
            raise LLMTransientError(str(exc)) from exc
        if exc_name == "ClientError":
            err_code = ""
            if hasattr(exc, "response"):
                err_code = exc.response.get("Error", {}).get("Code", "")
            _RETRYABLE_CODES = {"ThrottlingException", "TooManyRequestsException",
                                "ServiceUnavailableException", "InternalServerException"}
            if err_code in _RETRYABLE_CODES:
                logger.warning("llm_boto_transient_error", error=str(exc), code=err_code)
                raise LLMTransientError(str(exc)) from exc
        raise

    logger.debug("llm_raw_response", raw=raw[:500])

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("llm_json_parse_error", raw=raw[:300])
        raise LLMParseError(f"LLM returned invalid JSON: {exc}") from exc

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
