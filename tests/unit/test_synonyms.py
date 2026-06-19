"""Tests for the two-tier operational-synonym layer.

Asserts the architecture that replaced the overfit flat table:
  * CONVENTIONAL terms are injected as keywords (high precision).
  * COLLOQUIAL metaphors are NOT injected — only emitted as scored evidence
    with provenance, and confirmed against live coverage.
  * De-leak check: the deterministic floor on the FROZEN DEV corpus drops once
    metaphors stop auto-passing the scorer (the 6/6 reworded floor was leakage).
  * Precision check: on the UNTOUCHED holdout, negative prompts using trigger
    phrases in non-cache senses do not false-positive into cache keywords.
"""

from __future__ import annotations

import json
from pathlib import Path

from dashforge.agents.synonyms import (
    confirm_colloquial,
    expand_operational_terms,
    high_confidence_keywords,
    operational_evidence,
)

# Mirror of the robustness scorer's evidence sets.
_CACHE_TERMS = {"redis", "cache", "eviction", "evictions", "memory", "keyspace"}
_LATENCY_TERMS = {"latency", "duration", "response", "slow", "requests", "request"}

_FIX = Path(__file__).resolve().parent.parent / "eval" / "fixtures"
_DEV = _FIX / "clickstack_prompts.json"
_HOLDOUT = _FIX / "clickstack_prompts_holdout.json"


# ── Tier behaviour ────────────────────────────────────────────────────────────


def test_conventional_terms_inject():
    out = expand_operational_terms("redis evictions with high latency and oom")
    assert {"redis", "cache", "eviction", "latency", "memory"} <= set(out)


def test_memcached_does_not_inject_redis():
    out = expand_operational_terms("Memcached evictions increased")

    assert "cache" in out
    assert "redis" not in out


def test_colloquial_metaphors_do_not_inject():
    # Pure-metaphor prompt: nothing should be auto-injected as a keyword.
    assert expand_operational_terms("the fast-data layer was squeezed") == []
    assert "cache" not in expand_operational_terms("did key churn hurt the request path")


def test_colloquial_tier_phrase_masks_embedded_memory_keyword():
    evidence = operational_evidence("the in-memory tier looks unhealthy")

    assert "memory" not in high_confidence_keywords("the in-memory tier looks unhealthy")
    assert any(item.keyword == "cache" and item.tier == "colloquial" for item in evidence)


def test_standalone_memory_outside_colloquial_phrase_still_injects():
    keywords = high_confidence_keywords("the memory tier has high process memory usage")

    assert "memory" in keywords


def test_colloquial_surfaces_as_scored_evidence_with_provenance():
    ev = operational_evidence("the fast-data layer was squeezed")
    by_kw = {e.keyword: e for e in ev}
    assert by_kw["cache"].tier == "colloquial"
    assert by_kw["cache"].score < 1.0
    assert by_kw["cache"].source == "fast-data layer"  # provenance retained


def test_confirmation_gate_uses_scoped_signal_coverage():
    ev = operational_evidence("the in-memory tier looks unhealthy")
    # Nothing resolves -> metaphor not promoted.
    assert confirm_colloquial(ev, lambda sig: False) == []
    # Only a cache signal resolving promotes the "cache" keyword; an unrelated
    # signal resolving does NOT (scoped, not global).
    assert confirm_colloquial(ev, lambda sig: sig == "error_rate") == []
    assert "cache" in confirm_colloquial(ev, lambda sig: sig in {"cache_hits", "cache_size"})


def test_no_false_positive_on_word_boundaries():
    assert "memory" not in high_confidence_keywords("the program drew a diagram")


# ── De-leak: floor on the frozen DEV corpus must drop ─────────────────────────


def _has(terms: set[str], kws: list[str]) -> bool:
    return bool(set(kws) & terms)


def test_dev_floor_dropped_after_deleak():
    """With metaphors no longer auto-injected, the deterministic cache+latency
    floor on the reworded class must fall well below the old 6/6 leakage."""
    corpus = json.loads(_DEV.read_text())
    by_class: dict[str, list[bool]] = {}
    for item in corpus["prompts"]:
        kws = high_confidence_keywords(item["text"])
        ok = _has(_CACHE_TERMS, kws) and _has(_LATENCY_TERMS, kws)
        by_class.setdefault(item["class"], []).append(ok)
    reworded = sum(by_class["reworded"]) / len(by_class["reworded"])
    print("\nDev reworded floor after de-leak:", round(reworded, 3))
    # The metaphor-driven prompts no longer pass deterministically; the LLM must.
    assert reworded < 0.7, f"reworded floor still high ({reworded}); metaphors may still be leaking"
    # Literal/precise prompts still carry deterministically.
    precise = sum(by_class["precise"]) / len(by_class["precise"])
    assert precise >= 0.8


# ── Precision on the UNTOUCHED holdout negatives ──────────────────────────────


def test_holdout_negatives_have_no_cache_false_positives():
    corpus = json.loads(_HOLDOUT.read_text())
    failures = []
    for item in corpus["prompts"]:
        if item["class"] != "negative":
            continue
        injected = set(expand_operational_terms(item["text"]))
        for forbidden in item.get("forbidden_keywords", []):
            if forbidden in injected:
                failures.append((item["text"], forbidden))
    assert not failures, f"metaphor false-positives injected on negatives: {failures}"


def test_holdout_novel_metaphors_are_not_deterministically_caught():
    """Honesty check: prompts that use ONLY novel metaphors are NOT expected to
    pass on the deterministic layer — they depend on the LLM. If they started
    passing deterministically, the table has grown to chase the holdout."""
    corpus = json.loads(_HOLDOUT.read_text())
    novel = [p for p in corpus["prompts"] if p["class"] == "novel_metaphor"]
    caught = sum(1 for p in novel if _has(_CACHE_TERMS, high_confidence_keywords(p["text"])))
    print(f"\nNovel-metaphor deterministic catches: {caught}/{len(novel)} (expected low)")
    assert caught == 0
