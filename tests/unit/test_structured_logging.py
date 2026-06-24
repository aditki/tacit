"""Tests for structured logging: TokenUsage, LLMResult, stage_log, request_id binding."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import structlog

from tacit.agents.providers.base import LLMResult, TokenUsage

# ── TokenUsage ────────────────────────────────────────────────────────────────


def test_token_usage_defaults():
    u = TokenUsage()
    assert u.prompt_tokens == 0
    assert u.completion_tokens == 0
    assert u.total_tokens == 0


def test_token_usage_addition():
    a = TokenUsage(prompt_tokens=100, completion_tokens=50, total_tokens=150)
    b = TokenUsage(prompt_tokens=200, completion_tokens=80, total_tokens=280)
    c = a + b
    assert c.prompt_tokens == 300
    assert c.completion_tokens == 130
    assert c.total_tokens == 430


def test_token_usage_addition_preserves_originals():
    a = TokenUsage(prompt_tokens=10, completion_tokens=5, total_tokens=15)
    b = TokenUsage(prompt_tokens=20, completion_tokens=10, total_tokens=30)
    _ = a + b
    assert a.prompt_tokens == 10
    assert b.prompt_tokens == 20


# ── LLMResult ─────────────────────────────────────────────────────────────────


def test_llm_result_defaults():
    r = LLMResult(text="hello")
    assert r.text == "hello"
    assert r.usage.total_tokens == 0


def test_llm_result_with_usage():
    u = TokenUsage(prompt_tokens=100, completion_tokens=50, total_tokens=150)
    r = LLMResult(text='{"ok": true}', usage=u)
    assert r.text == '{"ok": true}'
    assert r.usage.prompt_tokens == 100


# ── request_id binding ────────────────────────────────────────────────────────


def test_bind_request_id_generates_id():
    from tacit.logging import bind_request_id, unbind_request_id

    rid = bind_request_id()
    assert len(rid) == 12
    unbind_request_id()


def test_bind_request_id_uses_provided():
    from tacit.logging import bind_request_id, unbind_request_id

    rid = bind_request_id("custom-id-123")
    assert rid == "custom-id-123"
    unbind_request_id()


def test_bind_unbind_cycle():
    from structlog.contextvars import get_contextvars

    from tacit.logging import bind_request_id, unbind_request_id

    bind_request_id("test-rid")
    ctx = get_contextvars()
    assert ctx.get("request_id") == "test-rid"
    unbind_request_id()
    ctx = get_contextvars()
    assert "request_id" not in ctx


# ── stage_log ─────────────────────────────────────────────────────────────────


def test_stage_log_emits_event():
    from tacit.logging import bind_request_id, stage_log, unbind_request_id

    captured = {}

    def capture_processor(logger, method, event_dict):
        captured.update(event_dict)
        raise structlog.DropEvent

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            capture_processor,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )

    try:
        bind_request_id("test-stage-rid")
        usage = TokenUsage(prompt_tokens=500, completion_tokens=200, total_tokens=700)
        stage_log(
            "intent",
            142.5,
            token_usage=usage,
            metrics_considered=284,
            metrics_selected=18,
        )

        assert captured["event"] == "stage_complete"
        assert captured["stage"] == "intent"
        assert captured["latency_ms"] == 142.5
        assert captured["token_count"] == 700
        assert captured["prompt_tokens"] == 500
        assert captured["completion_tokens"] == 200
        assert captured["metrics_considered"] == 284
        assert captured["metrics_selected"] == 18
        assert captured["request_id"] == "test-stage-rid"
    finally:
        unbind_request_id()
        # Reset structlog
        structlog.reset_defaults()


def test_stage_log_without_token_usage():
    from tacit.logging import stage_log

    captured = {}

    def capture_processor(logger, method, event_dict):
        captured.update(event_dict)
        raise structlog.DropEvent

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            capture_processor,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )

    try:
        stage_log("metric_ranking", 5.3, metrics_considered=288, metrics_selected=60)

        assert captured["stage"] == "metric_ranking"
        assert captured["latency_ms"] == 5.3
        assert "token_count" not in captured
        assert captured["metrics_considered"] == 288
        assert captured["metrics_selected"] == 60
    finally:
        structlog.reset_defaults()


# ── call_llm returns (model, usage) ──────────────────────────────────────────


def test_call_llm_returns_tuple():
    from pydantic import BaseModel

    from tacit.agents.llm import call_llm

    class Simple(BaseModel):
        v: int

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        return_value=LLMResult(
            text='{"v": 42}',
            usage=TokenUsage(prompt_tokens=100, completion_tokens=20, total_tokens=120),
        )
    )

    with patch("tacit.agents.llm.get_provider", return_value=mock_provider):
        model, usage = asyncio.run(call_llm("sys", "user", Simple))

    assert model.v == 42
    assert usage.total_tokens == 120
    assert usage.prompt_tokens == 100


def test_call_llm_accumulates_repair_tokens():
    """When JSON repair is needed, tokens from both calls are accumulated."""
    from pydantic import BaseModel

    from tacit.agents.llm import call_llm

    class Simple(BaseModel):
        v: int

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        side_effect=[
            LLMResult(
                text="{broken json",
                usage=TokenUsage(prompt_tokens=100, completion_tokens=50, total_tokens=150),
            ),
            LLMResult(
                text='{"v": 7}',
                usage=TokenUsage(prompt_tokens=80, completion_tokens=30, total_tokens=110),
            ),
        ]
    )

    with patch("tacit.agents.llm.get_provider", return_value=mock_provider):
        model, usage = asyncio.run(call_llm("sys", "user", Simple))

    assert model.v == 7
    assert usage.total_tokens == 260  # 150 + 110
    assert usage.prompt_tokens == 180  # 100 + 80


# ── Agent functions return (result, usage) ────────────────────────────────────


def test_classify_intent_returns_usage():
    from tacit.agents.intent import classify_intent

    intent_json = json.dumps(
        {
            "summary": "test",
            "domain": "general",
            "services": [],
            "signals": ["metrics"],
            "keywords": ["cpu"],
            "timerange": "1h",
            "problem_type": "general",
            "archetypes": [{"type": "general", "confidence": 0.9}],
        }
    )

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        return_value=LLMResult(
            text=intent_json,
            usage=TokenUsage(prompt_tokens=500, completion_tokens=100, total_tokens=600),
        )
    )

    with patch("tacit.agents.llm.get_provider", return_value=mock_provider):
        intent, usage = asyncio.run(classify_intent("high cpu"))

    assert intent.domain == "general"
    assert usage.total_tokens == 600
