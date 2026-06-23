"""LLM exception normalization helpers."""

from __future__ import annotations

import httpx


class LLMTransientError(Exception):
    """Wraps transient LLM errors that are safe to retry."""


class LLMParseError(Exception):
    """LLM returned unparseable or invalid output. Do NOT retry blindly."""


TRANSIENT_HTTP_STATUS_CODES = {429, 500, 502, 503, 529}

TRANSIENT_EXCEPTION_NAMES = {
    "APIConnectionError",
    "APITimeoutError",
    "ConnectTimeoutError",
    "EndpointConnectionError",
    "InternalServerError",
    "InternalServerException",
    "ModelTimeoutException",
    "RateLimitError",
    "ReadTimeoutError",
    "ServiceUnavailableError",
    "ServiceUnavailableException",
    "ThrottlingException",
    "TooManyRequestsException",
}

RETRYABLE_PROVIDER_CODES = {
    "InternalServerException",
    "ServiceUnavailableException",
    "ThrottlingException",
    "TooManyRequestsException",
}


def is_transient_llm_error(exc: Exception) -> bool:
    """Return whether an LLM/provider exception is safe for retry."""
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError, ConnectionError, OSError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in TRANSIENT_HTTP_STATUS_CODES

    exc_name = type(exc).__name__
    if exc_name in TRANSIENT_EXCEPTION_NAMES:
        return True

    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        err_code = response.get("Error", {}).get("Code", "")
        if err_code in RETRYABLE_PROVIDER_CODES:
            return True

    status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
    return isinstance(status, int) and status in TRANSIENT_HTTP_STATUS_CODES


def wrap_transient_llm_error(exc: Exception) -> LLMTransientError | None:
    """Return a retryable wrapper for transient errors, otherwise None."""
    if is_transient_llm_error(exc):
        return LLMTransientError(str(exc))
    return None
