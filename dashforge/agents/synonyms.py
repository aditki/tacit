"""Operational vocabulary normalization for intent keywords — two tiers.

On-call engineers describe incidents with a mix of standard terminology and
loose metaphor. We normalize this, but with a deliberate, auditable split so the
layer cannot quietly memorize an evaluation corpus:

* CONVENTIONAL — high-precision, dataset-independent normalization: standard SRE
  terms, vendor aliases, and common abbreviations (e.g. "oom" → memory, "rps" →
  throughput, "5xx" → errors, "redis"/"memcached" → cache). These are confident
  enough to inject directly as intent keywords.

* COLLOQUIAL — metaphors and ambiguous paraphrases ("fast-data layer",
  "key churn", "reuse efficiency", "ran out of headroom"). These are emitted as
  *scored evidence with provenance*, NOT injected as ground-truth keywords. They
  carry low confidence and a source phrase so a downstream consumer can decide
  whether live metric coverage or a learned archetype actually backs them. This
  keeps a single metaphor from confidently steering an investigation, and keeps
  the deterministic floor honest (metaphors do not auto-satisfy a scorer).

The colloquial list is explicitly NOT meant to grow toward whatever phrasing a
test corpus happens to use; new metaphors should be earned via learned mappings,
not appended here. Pure stdlib so it can be unit-tested without the LLM.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass

_CONVENTIONAL_SCORE = 1.0
_COLLOQUIAL_SCORE = 0.4

# Evidence at or above this score is trusted enough to inject as a keyword
# directly; anything below requires live confirmation. The split is driven by
# the score, not the tier label, so the score is load-bearing.
_AUTOINJECT_SCORE = 1.0

# Canonical keyword -> the signal types (as defined in signals.yaml) it implies.
# Used to confirm colloquial evidence against *scoped* signal coverage: a
# metaphor is only trusted when the specific signal it implies actually resolves
# to a metric in the live catalog — not when some unrelated metric merely shares
# a substring.
KEYWORD_SIGNALS: dict[str, tuple[str, ...]] = {
    "cache": ("cache_hits", "cache_hit_ratio", "cache_size"),
    "eviction": ("cache_evictions",),
    "hit_ratio": ("cache_hit_ratio", "cache_hits", "cache_misses"),
    "redis": ("cache_hits", "cache_evictions", "cache_memory_pressure"),
    "memory": ("memory_usage", "cache_memory_pressure"),
    "connections": ("client_pressure", "db_connection_pool"),
    "latency": ("request_latency", "api_latency"),
    "queue": ("queue_depth", "consumer_lag"),
    "disk": ("disk_usage", "io_wait"),
    "cpu": ("cpu_usage",),
    "throughput": ("request_rate",),
    "errors": ("error_rate",),
    "saturation": ("in_flight_requests", "cpu_usage"),
}

# ── Conventional: standard terminology / aliases / abbreviations ─────────────
# High precision; safe to inject as keywords. (phrase, canonical keywords)
_CONVENTIONAL: list[tuple[str, frozenset[str]]] = [
    # Caching (literal)
    ("cache", frozenset({"cache"})),
    ("caching", frozenset({"cache"})),
    ("cache hit", frozenset({"cache", "hit_ratio"})),
    ("cache miss", frozenset({"cache", "hit_ratio"})),
    ("hit ratio", frozenset({"cache", "hit_ratio"})),
    ("hit rate", frozenset({"cache", "hit_ratio"})),
    ("miss rate", frozenset({"cache", "hit_ratio"})),
    ("eviction", frozenset({"cache", "eviction"})),
    ("evictions", frozenset({"cache", "eviction"})),
    ("evicted", frozenset({"cache", "eviction"})),
    ("keyspace", frozenset({"cache"})),
    # Datastore / cache engines (vendor aliases)
    ("redis", frozenset({"redis", "cache"})),
    ("memcached", frozenset({"cache"})),
    ("elasticache", frozenset({"redis", "cache"})),
    # Memory / resources
    ("memory", frozenset({"memory"})),
    ("heap", frozenset({"memory"})),
    ("working set", frozenset({"memory"})),
    ("out of memory", frozenset({"memory", "saturation"})),
    ("oom", frozenset({"memory", "saturation"})),
    ("cpu", frozenset({"cpu"})),
    ("disk", frozenset({"disk"})),
    ("io wait", frozenset({"disk"})),
    ("iowait", frozenset({"disk"})),
    ("iops", frozenset({"disk"})),
    # Latency (standard)
    ("latency", frozenset({"latency"})),
    ("duration", frozenset({"latency"})),
    ("response time", frozenset({"latency"})),
    ("response times", frozenset({"latency"})),
    ("response-time", frozenset({"latency"})),
    ("tail latency", frozenset({"latency"})),
    ("timeout", frozenset({"latency"})),
    ("timeouts", frozenset({"latency"})),
    ("slow", frozenset({"latency"})),
    ("slower", frozenset({"latency"})),
    ("slowdown", frozenset({"latency"})),
    ("p95", frozenset({"latency"})),
    ("p99", frozenset({"latency"})),
    ("p999", frozenset({"latency"})),
    # Errors
    ("error rate", frozenset({"errors"})),
    ("errors", frozenset({"errors"})),
    ("5xx", frozenset({"errors"})),
    ("4xx", frozenset({"errors"})),
    ("failed requests", frozenset({"errors"})),
    ("exceptions", frozenset({"errors"})),
    # Throughput
    ("throughput", frozenset({"throughput"})),
    ("qps", frozenset({"throughput"})),
    ("rps", frozenset({"throughput"})),
    ("request rate", frozenset({"throughput"})),
    ("requests per second", frozenset({"throughput"})),
    # Saturation (standard)
    ("saturation", frozenset({"saturation"})),
    ("saturated", frozenset({"saturation"})),
    ("contention", frozenset({"saturation"})),
    ("throttling", frozenset({"saturation"})),
    ("throttled", frozenset({"saturation"})),
    ("starvation", frozenset({"saturation"})),
    # Queue / backlog
    ("backlog", frozenset({"queue"})),
    ("queue", frozenset({"queue"})),
    ("consumer lag", frozenset({"queue"})),
    # Connections (standard)
    ("connection pool", frozenset({"connections"})),
    ("connected clients", frozenset({"connections"})),
    ("blocked clients", frozenset({"connections", "saturation"})),
    ("open connections", frozenset({"connections"})),
]

# ── Colloquial: metaphor / ambiguous — EVIDENCE ONLY, never auto-injected ─────
# Low precision. Emitted with provenance so downstream can confirm against live
# metric coverage or a learned archetype before trusting it.
_COLLOQUIAL: list[tuple[str, frozenset[str]]] = [
    ("fast-data layer", frozenset({"cache"})),
    ("fast data layer", frozenset({"cache"})),
    ("in-memory tier", frozenset({"cache"})),
    ("in memory tier", frozenset({"cache"})),
    ("in-memory store", frozenset({"cache"})),
    ("memory tier", frozenset({"cache"})),
    ("hot tier", frozenset({"cache"})),
    ("reuse efficiency", frozenset({"cache", "hit_ratio"})),
    ("cache effectiveness", frozenset({"cache", "hit_ratio"})),
    ("cache turnover", frozenset({"cache", "eviction"})),
    ("key churn", frozenset({"cache", "eviction"})),
    ("key removal", frozenset({"cache", "eviction"})),
    ("key removals", frozenset({"cache", "eviction"})),
    ("discarded entries", frozenset({"cache", "eviction"})),
    ("discarded keys", frozenset({"cache", "eviction"})),
    ("purged keys", frozenset({"cache", "eviction"})),
    ("object retention", frozenset({"cache", "eviction"})),
    ("headroom", frozenset({"memory", "saturation"})),
    ("ran out of headroom", frozenset({"memory", "saturation"})),
    ("squeezed", frozenset({"saturation"})),
    ("under pressure", frozenset({"saturation"})),
    ("maxed out", frozenset({"saturation"})),
    ("took longer", frozenset({"latency"})),
    ("taking longer", frozenset({"latency"})),
    ("waiting longer", frozenset({"latency"})),
    ("response delay", frozenset({"latency"})),
    ("request path", frozenset({"latency"})),
    ("connection demand", frozenset({"connections"})),
    ("consumer load", frozenset({"connections"})),
    ("client load", frozenset({"connections"})),
]


def _compile(rules: list[tuple[str, frozenset[str]]]) -> list[tuple[re.Pattern[str], frozenset[str]]]:
    return [(re.compile(r"\b" + re.escape(p) + r"\b", re.IGNORECASE), kws) for p, kws in rules]


_CONVENTIONAL_C = _compile(_CONVENTIONAL)
_COLLOQUIAL_C = _compile(_COLLOQUIAL)


@dataclass(frozen=True)
class SynonymEvidence:
    """A single piece of normalization evidence with provenance."""

    keyword: str  # canonical observability keyword implied
    score: float  # 1.0 conventional, < 1.0 colloquial/ambiguous
    tier: str  # "conventional" | "colloquial"
    source: str  # the matched phrase that produced this evidence

    def as_dict(self) -> dict[str, object]:
        return {"keyword": self.keyword, "score": self.score, "tier": self.tier, "source": self.source}


def operational_evidence(text: str) -> list[SynonymEvidence]:
    """All normalization evidence implied by *text*, conventional and colloquial.

    De-duplicated per (keyword, source); a keyword reachable from both tiers keeps
    only its highest-scoring occurrence.
    """
    best: dict[str, SynonymEvidence] = {}
    for patterns, tier, score in (
        (_CONVENTIONAL_C, "conventional", _CONVENTIONAL_SCORE),
        (_COLLOQUIAL_C, "colloquial", _COLLOQUIAL_SCORE),
    ):
        for pattern, kws in patterns:
            m = pattern.search(text)
            if not m:
                continue
            phrase = m.group(0).lower()
            for kw in sorted(kws):
                prior = best.get(kw)
                if prior is None or score > prior.score:
                    best[kw] = SynonymEvidence(keyword=kw, score=score, tier=tier, source=phrase)
    # Stable order: conventional first (score desc), then keyword name.
    return sorted(best.values(), key=lambda e: (-e.score, e.keyword))


def high_confidence_keywords(text: str) -> list[str]:
    """Canonical keywords trusted enough to inject directly (score ≥ threshold)."""
    return [e.keyword for e in operational_evidence(text) if e.score >= _AUTOINJECT_SCORE]


def confirm_colloquial(
    evidence: Iterable[SynonymEvidence],
    signal_resolves: Callable[[str], bool],
    *,
    min_score: float = 0.0,
) -> list[str]:
    """Promote below-threshold (colloquial) evidence via SCOPED signal coverage.

    For each piece of evidence whose score is below the auto-inject threshold,
    look up the specific signal types its keyword implies (``KEYWORD_SIGNALS``)
    and confirm it only if ``signal_resolves`` reports that one of those signals
    actually resolves to a metric in the live catalog. This replaces loose global
    substring matching: a metaphor implying "cache" is trusted only when a cache
    signal genuinely resolves, not when any metric happens to contain "cache".

    ``min_score`` lets a caller additionally require a minimum evidence score.
    """
    out: list[str] = []
    seen: set[str] = set()
    for e in evidence:
        if e.score >= _AUTOINJECT_SCORE:
            continue  # already injected as high-confidence
        if e.score < min_score:
            continue
        if e.keyword in seen:
            continue
        signals = KEYWORD_SIGNALS.get(e.keyword, ())
        if any(signal_resolves(sig) for sig in signals):
            seen.add(e.keyword)
            out.append(e.keyword)
    return out


def expand_operational_terms(text: str, keywords: Iterable[str] = ()) -> list[str]:
    """Return *keywords* augmented with CONVENTIONAL canonical terms only.

    Additive and order-preserving. Colloquial metaphors are intentionally NOT
    injected here — retrieve them via ``operational_evidence`` and confirm them
    with ``confirm_colloquial`` against live coverage instead.
    """
    out: list[str] = []
    seen: set[str] = set()
    for kw in keywords:
        low = str(kw).lower()
        if low not in seen:
            seen.add(low)
            out.append(str(kw))
    for kw in high_confidence_keywords(text):
        if kw.lower() not in seen:
            seen.add(kw.lower())
            out.append(kw)
    return out
