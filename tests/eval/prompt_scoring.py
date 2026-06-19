"""Pure, LLM-free scoring for the prompt-variation harness.

Kept separate from ``prompt_variation_harness`` (which imports the LLM intent
agent) so the expectation-aware evaluation can be unit-tested directly against
duck-typed intents.
"""

from __future__ import annotations

from typing import Any

_CACHE_TERMS = {"redis", "cache", "eviction", "evictions", "memory", "keyspace"}
_LATENCY_TERMS = {"latency", "duration", "response", "slow", "requests", "request"}
_CACHE_ARCHETYPES = {"redis_saturation"}
_LATENCY_ARCHETYPES = {"latency_investigation", "api_response_time_spike", "golden_signals"}


def signals(intent: Any) -> dict[str, Any]:
    archetypes = {match.type for match in intent.archetypes if match.confidence >= 0.3}
    words = {str(word).lower() for word in intent.keywords}
    summary_words = set(str(intent.summary).lower().replace("/", " ").split())
    evidence = words | summary_words
    has_cache = bool(archetypes & _CACHE_ARCHETYPES) or bool(evidence & _CACHE_TERMS)
    has_latency = bool(archetypes & _LATENCY_ARCHETYPES) or bool(evidence & _LATENCY_TERMS)
    return {
        "archetypes": sorted(archetypes),
        "keywords": sorted(words),
        "evidence": evidence,
        "has_cache": has_cache,
        "has_latency": has_latency,
    }


def is_negative(item: dict[str, Any]) -> bool:
    return item.get("class") == "negative" or item.get("expects_cache") is False


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
        passed = (not s["has_cache"]) and not injected_forbidden
        detail = {
            "archetypes": s["archetypes"],
            "keywords": s["keywords"],
            "has_cache": s["has_cache"],
            "injected_forbidden": injected_forbidden,
        }
        return passed, detail

    expects_cache = item.get("expects_cache", True)
    expects_latency = item.get("expects_latency", True)
    passed = (s["has_cache"] or not expects_cache) and (s["has_latency"] or not expects_latency)
    detail = {k: s[k] for k in ("archetypes", "keywords", "has_cache", "has_latency")}
    return passed, detail
