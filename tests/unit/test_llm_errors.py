from __future__ import annotations

import httpx

from dashforge.agents.llm_errors import LLMTransientError, is_transient_llm_error, wrap_transient_llm_error


def test_wrap_transient_llm_error_for_named_provider_error():
    RateLimitError = type("RateLimitError", (Exception,), {})
    exc = RateLimitError("slow down")

    wrapped = wrap_transient_llm_error(exc)

    assert isinstance(wrapped, LLMTransientError)
    assert is_transient_llm_error(exc)


def test_wrap_transient_llm_error_for_http_status():
    request = httpx.Request("POST", "https://llm.example.test")
    response = httpx.Response(429, request=request)
    exc = httpx.HTTPStatusError("rate limited", request=request, response=response)

    assert isinstance(wrap_transient_llm_error(exc), LLMTransientError)


def test_wrap_transient_llm_error_ignores_non_retryable_error():
    assert wrap_transient_llm_error(ValueError("bad prompt")) is None
