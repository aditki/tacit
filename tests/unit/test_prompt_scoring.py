"""Unit tests for the expectation-aware prompt scoring (LLM-free)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from tests.eval.prompt_scoring import evaluate, is_negative, signals

_FIX = Path(__file__).resolve().parent.parent / "eval" / "fixtures"


@dataclass
class _Match:
    type: str
    confidence: float


@dataclass
class _Intent:
    keywords: list[str] = field(default_factory=list)
    summary: str = ""
    archetypes: list[_Match] = field(default_factory=list)
    keyword_evidence: list[dict[str, object]] = field(default_factory=list)


def test_positive_requires_expected_signals():
    cache_and_latency = _Intent(keywords=["redis", "latency"])
    item = {"class": "reworded"}  # defaults expects_cache/latency True
    passed, _ = evaluate(cache_and_latency, item)
    assert passed
    # Missing latency fails a default-positive prompt.
    cache_only = _Intent(keywords=["redis"])
    assert not evaluate(cache_only, item)[0]


def test_positive_can_waive_latency_via_label():
    cache_only = _Intent(keywords=["cache"])
    item = {"class": "paraphrase", "expects_cache": True, "expects_latency": False}
    assert evaluate(cache_only, item)[0]


def test_positive_can_waive_cache_via_label():
    latency_only = _Intent(keywords=["latency"])
    item = {"class": "vague", "expects_cache": False, "expects_latency": True}
    assert not is_negative(item)
    assert evaluate(latency_only, item)[0]


def test_colloquial_evidence_preserves_positive_cache_hypothesis():
    preserved = _Intent(
        keywords=["latency"],
        keyword_evidence=[
            {"keyword": "cache", "score": 0.4, "tier": "colloquial", "source": "key churn"},
            {"keyword": "eviction", "score": 0.4, "tier": "colloquial", "source": "key churn"},
        ],
    )

    result = signals(preserved)

    assert result["preserved_cache"]
    assert not result["asserted_cache"]
    assert evaluate(preserved, {"class": "reworded"})[0]


def test_negative_passes_when_cache_not_asserted():
    benign = _Intent(keywords=["auth", "security"], summary="rotate signing keys")
    item = {"class": "negative", "expects_cache": False, "forbidden_keywords": ["cache", "eviction"]}
    assert is_negative(item)
    assert evaluate(benign, item)[0]


def test_negative_fails_on_cache_false_positive():
    leaked = _Intent(keywords=["cache", "eviction"], summary="rotate signing keys")
    item = {"class": "negative", "expects_cache": False, "forbidden_keywords": ["cache", "eviction"]}
    passed, detail = evaluate(leaked, item)
    assert not passed
    assert detail["injected_forbidden"] == ["cache", "eviction"]


def test_negative_allows_unconfirmed_colloquial_evidence():
    preserved_only = _Intent(
        keywords=["auth", "security"],
        summary="rotate signing keys",
        keyword_evidence=[
            {"keyword": "cache", "score": 0.4, "tier": "colloquial", "source": "key churn"},
            {"keyword": "eviction", "score": 0.4, "tier": "colloquial", "source": "key churn"},
        ],
    )
    item = {"class": "negative", "expects_cache": False, "forbidden_keywords": ["cache", "eviction"]}
    passed, detail = evaluate(preserved_only, item)

    assert passed
    assert detail["preserved_cache"]
    assert not detail["asserted_cache"]


def test_archetype_evidence_counts_as_cache():
    via_archetype = _Intent(keywords=[], summary="checkout incident", archetypes=[_Match("redis_saturation", 0.8)])
    assert signals(via_archetype)["has_cache"]


def test_generic_memory_and_request_words_do_not_satisfy_signal_expectations():
    generic = _Intent(keywords=["memory", "requests"], summary="memory used by incoming requests")

    result = signals(generic)

    assert not result["has_cache"]
    assert not result["has_latency"]
    assert not evaluate(generic, {"class": "reworded"})[0]


def test_contextual_response_time_counts_as_latency():
    contextual = _Intent(keywords=["redis"], summary="Redis response times increased")

    assert signals(contextual)["has_latency"]


def test_holdout_fixture_is_label_complete():
    """Every holdout prompt must carry the labels the scorer relies on."""
    corpus = json.loads((_FIX / "clickstack_prompts_holdout.json").read_text())
    for p in corpus["prompts"]:
        assert "class" in p and "text" in p
        if p["class"] == "negative":
            assert p.get("expects_cache") is False
            assert p.get("forbidden_keywords"), p["text"]
        else:
            assert "expects_cache" in p and "expects_latency" in p
