"""Pure, LLM-free scoring for the prompt-variation harness.

Kept separate from ``prompt_variation_harness`` (which imports the LLM intent
agent) so the expectation-aware evaluation can be unit-tested directly against
duck-typed intents.
"""

from __future__ import annotations

import re
from typing import Any

_CACHE_TERMS = {"redis", "cache", "eviction", "evictions", "keyspace", "hit_ratio", "cache_hit_ratio"}
_CACHE_PHRASES = {"cache hit ratio", "cache miss", "cache misses"}
_LATENCY_TERMS = {"latency", "duration", "slow", "slowness", "timeout", "timeouts"}
_LATENCY_PHRASES = {"response time", "response times", "request time", "request times"}
_CACHE_ARCHETYPES = {"redis_saturation"}
_LATENCY_ARCHETYPES = {"latency_investigation", "api_response_time_spike", "golden_signals"}
_PRESERVED_CACHE_TERMS = {"redis", "cache", "eviction", "evictions", "hit_ratio"}
_PRESERVED_LATENCY_TERMS = {"latency"}


def signals(intent: Any) -> dict[str, Any]:
    archetypes = {match.type for match in intent.archetypes if match.confidence >= 0.3}
    words = {str(word).lower() for word in intent.keywords}
    keyword_evidence = getattr(intent, "keyword_evidence", []) or []
    preserved = {
        str(item.get("keyword", "")).lower()
        for item in keyword_evidence
        if float(item.get("score", 0.0) or 0.0) < 1.0
    }
    summary = " ".join(re.findall(r"[a-z0-9_]+", str(intent.summary).lower()))
    padded_summary = f" {summary} "
    summary_words = set(summary.split())
    evidence = words | summary_words
    asserted_cache = (
        bool(archetypes & _CACHE_ARCHETYPES)
        or bool(evidence & _CACHE_TERMS)
        or any(f" {phrase} " in padded_summary for phrase in _CACHE_PHRASES)
    )
    asserted_latency = (
        bool(archetypes & _LATENCY_ARCHETYPES)
        or bool(evidence & _LATENCY_TERMS)
        or any(f" {phrase} " in padded_summary for phrase in _LATENCY_PHRASES)
    )
    preserved_cache = bool(preserved & _PRESERVED_CACHE_TERMS)
    preserved_latency = bool(preserved & _PRESERVED_LATENCY_TERMS)
    return {
        "archetypes": sorted(archetypes),
        "keywords": sorted(words),
        "preserved_keywords": sorted(preserved),
        "evidence": evidence,
        "asserted_cache": asserted_cache,
        "asserted_latency": asserted_latency,
        "preserved_cache": preserved_cache,
        "preserved_latency": preserved_latency,
        "has_cache": asserted_cache or preserved_cache,
        "has_latency": asserted_latency or preserved_latency,
    }


def is_negative(item: dict[str, Any]) -> bool:
    return item.get("class") == "negative"


def evaluate(intent: Any, item: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    """Expectation-aware scoring honoring per-prompt labels.

    Positive prompts pass when they meet their cache/latency expectations
    (defaulting to both, for the original dev corpus that has no labels).
    Negative prompts pass when they do NOT assert cache and inject none of their
    forbidden keywords — the layer must not false-positive.
    """
    s = signals(intent)
    if is_negative(item):
        forbidden = {k.lower() for k in item.get("forbidden_keywords", [])}
        injected_forbidden = sorted(forbidden & {k.lower() for k in s["keywords"]})
        passed = (not s["asserted_cache"]) and not injected_forbidden
        detail = {
            "archetypes": s["archetypes"],
            "keywords": s["keywords"],
            "preserved_keywords": s["preserved_keywords"],
            "asserted_cache": s["asserted_cache"],
            "preserved_cache": s["preserved_cache"],
            "has_cache": s["has_cache"],
            "injected_forbidden": injected_forbidden,
        }
        return passed, detail

    expects_cache = item.get("expects_cache", True)
    expects_latency = item.get("expects_latency", True)
    passed = (s["has_cache"] or not expects_cache) and (s["has_latency"] or not expects_latency)
    detail = {k: s[k] for k in ("archetypes", "keywords", "has_cache", "has_latency")}
    return passed, detail
