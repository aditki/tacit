"""Deterministic keyword-based intent fallback — zero-key mode.

When no LLM API key is configured (e.g. first-run demos), Tacit can still
classify well-known incident shapes deterministically and route them through
the archetype engine, which needs no LLM for query generation. This keeps the
full prompt → dashboard demo runnable with zero credentials.

The fallback is intentionally conservative: it only matches explicit
operational vocabulary, and when nothing matches it falls back to a
low-confidence ``golden_signals`` overview instead of guessing.
"""

from __future__ import annotations

import re
from typing import Any, cast

import structlog

from tacit.agents.synonyms import expand_operational_terms, operational_evidence
from tacit.config import Settings
from tacit.models.schemas import ArchetypeMatch, Intent, SignalType

logger = structlog.get_logger()

# Providers that cannot work without an API key. Ollama is local and Bedrock
# uses IAM, so neither belongs here.
_KEY_REQUIRED_PROVIDERS = {"anthropic", "openai", "azure"}

# Archetype → trigger patterns. Matched case-insensitively against the prompt.
# Order matters only for tie-breaking (first listed wins on equal hits).
_ARCHETYPE_TRIGGERS: dict[str, list[str]] = {
    "latency_investigation": [
        r"\blatenc",
        r"\bslow(?:ness|er)?\b",
        r"\bp9[59]\b",
        r"\bp50\b",
        r"\btimeouts?\b",
        r"response time",
    ],
    "error_spike": [
        r"\b5\d\ds?\b",
        r"\b5xxs?\b",
        r"\berrors?\b",
        r"error rate",
        r"\bfail(?:ed|ing|ures?)\b",
        r"\bretr(?:y|ies|ying)\b",
    ],
    "resource_saturation": [
        r"\bcpu\b",
        r"\bmemory\b",
        r"\boom\b",
        r"\bthrottl",
        r"\bsaturat",
    ],
    "kubernetes_investigation": [
        r"\bpods?\b",
        r"\bkubernetes\b",
        r"\bk8s\b",
        r"crashloop",
        r"\bnode pressure\b",
        r"\bevict",
    ],
    "memory_leak_investigation": [
        r"memory leak",
        r"memory growth",
        r"heap growth",
        r"oom kill",
    ],
    "message_queue_backlog": [
        r"\bkafka lag\b",
        r"\bconsumer lag\b",
        r"\bqueue (?:depth|backlog|growth)\b",
        r"\bbacklog\b",
        r"\bsqs\b",
        r"\brabbitmq\b",
    ],
    "deployment_regression": [
        r"\bafter (?:a )?deploy",
        r"\bdeployment\b",
        r"\bcanary\b",
        r"\brollback\b",
        r"\bregression\b",
        r"\bnew release\b",
    ],
    "db_connection_pool_exhaustion": [
        r"connection pool",
        r"pool exhaust",
        r"connection timeout",
        r"\bdb connections?\b",
    ],
    "redis_saturation": [
        r"\bredis\b",
        r"\bcache (?:miss|stampede|evict)",
    ],
    "storage_io_bottleneck": [
        r"\bdisk\b",
        r"\bio wait\b",
        r"\biops\b",
        r"filesystem full",
        r"storage latency",
    ],
    "third_party_dependency_degradation": [
        r"third[- ]party",
        r"\bupstream\b",
        r"\bdownstream\b",
        r"external api",
        r"circuit breaker",
    ],
    "rate_limiting_investigation": [
        r"rate limit",
        r"\bthrottling\b",
        r"\b429s?\b",
    ],
    "dns_certificate_failures": [
        r"\bdns\b",
        r"\btls\b",
        r"\bcertificates?\b",
        r"\bhandshake\b",
    ],
    "autoscaling_instability": [
        r"\bhpa\b",
        r"\bautoscal",
        r"\breplicas? flapping\b",
        r"\bscaling\b",
    ],
    "golden_signals": [
        r"golden signals?",
        r"\boverview\b",
        r"service health",
        r"\bslo\b",
    ],
}

_DOMAIN_BY_ARCHETYPE = {
    "latency_investigation": "application",
    "error_spike": "application",
    "resource_saturation": "infrastructure",
    "kubernetes_investigation": "infrastructure",
    "memory_leak_investigation": "infrastructure",
    "message_queue_backlog": "messaging",
    "deployment_regression": "application",
    "db_connection_pool_exhaustion": "database",
    "redis_saturation": "database",
    "storage_io_bottleneck": "infrastructure",
    "third_party_dependency_degradation": "application",
    "rate_limiting_investigation": "network",
    "dns_certificate_failures": "network",
    "autoscaling_instability": "infrastructure",
    "golden_signals": "general",
}

# Compound tokens that look like service names but are operational vocabulary.
_SERVICE_STOPLIST = {
    "api",
    "app",
    "cpu",
    "db",
    "disk",
    "error_rate",
    "error-rate",
    "errors",
    "cache-hit",
    "cache-miss",
    "cache-stampede",
    "cache_hit",
    "cache_miss",
    "cache_stampede",
    "grafana",
    "heap",
    "latency",
    "last",
    "memory",
    "minutes",
    "prometheus",
    "queue_depth",
    "queue-depth",
    "connection_pool",
    "connection-pool",
    "requests",
    "service",
    "throttling",
    "third-party",
    "rate-limit",
    "rate_limit",
    "on-call",
    "e2e",
    "p95",
    "p99",
}

_SERVICE_TOKEN = re.compile(r"\b[a-z][a-z0-9]*(?:[-_][a-z0-9]+)+\b")
_SERVICE_PHRASE = re.compile(r"\b([a-z][a-z0-9]{2,})\s+service\b", re.IGNORECASE)
_SERVICE_PREPOSITION = re.compile(
    r"\b(?:on|for|in|with|from|to|of)\s+(?:the\s+)?([a-z][a-z0-9]{2,})(?:\s+service)?"
    r"(?=\s*(?:$|[,.?!;:]|\b(?:and|during|for|in|last|over|past|since|with)\b))",
    re.IGNORECASE,
)

_TIMERANGE = re.compile(
    r"(?:last|past)\s+(\d+)\s*(m(?:in(?:ute)?s?)?|h(?:(?:ou)?rs?)?|d(?:ays?)?)\b",
    re.IGNORECASE,
)
_ENVIRONMENT = re.compile(
    r"\b(production|prod|staging|stage|development|dev|qa|test|sandbox)\b",
    re.IGNORECASE,
)
_QUALIFIED_ENVIRONMENT = re.compile(
    r"\b(?:environment|env)\s*[:=]\s*([a-z0-9][a-z0-9_.:-]*)",
    re.IGNORECASE,
)


def zero_key_mode(settings: Settings) -> bool:
    """True when the configured LLM provider cannot work without an API key."""
    provider = settings.llm_provider.lower()
    api_base = getattr(cast(Any, settings), "llm_api_base", "")
    if provider == "openai" and api_base:
        return False
    return provider in _KEY_REQUIRED_PROVIDERS and not settings.llm_api_key


def _match_archetypes(text: str) -> list[ArchetypeMatch]:
    scored: list[tuple[str, int]] = []
    for archetype, patterns in _ARCHETYPE_TRIGGERS.items():
        hits = sum(1 for pattern in patterns if re.search(pattern, text, re.IGNORECASE))
        if hits:
            scored.append((archetype, hits))
    specificity = {
        "resource_saturation": 0,
        "memory_leak_investigation": 2,
        "rate_limiting_investigation": 2,
    }
    scored.sort(key=lambda item: (item[1], specificity.get(item[0], 1)), reverse=True)
    return [ArchetypeMatch(type=archetype, confidence=min(0.95, 0.55 + 0.15 * hits)) for archetype, hits in scored[:4]]


def _extract_services(text: str) -> list[str]:
    services: list[str] = []
    environments = set(_extract_environments(text))
    candidates = [
        *[match.lower() for match in _SERVICE_PHRASE.findall(text)],
        *[match.lower() for match in _SERVICE_PREPOSITION.findall(text)],
        *_SERVICE_TOKEN.findall(text.lower()),
    ]
    for token in candidates:
        if token in _SERVICE_STOPLIST or token in environments or _ENVIRONMENT.fullmatch(token) or token in services:
            continue
        services.append(token)
    return services[:5]


def _extract_timerange(text: str) -> str:
    match = _TIMERANGE.search(text)
    if not match:
        return "1h"
    value, unit = match.group(1), match.group(2).lower()
    return f"{value}{unit[0]}"


def _extract_environments(text: str) -> list[str]:
    """Capture only environment names explicitly present in the prompt."""
    qualified = _QUALIFIED_ENVIRONMENT.findall(text)
    unqualified_text = _QUALIFIED_ENVIRONMENT.sub("", text)
    standalone = [
        match.group(1)
        for match in _ENVIRONMENT.finditer(unqualified_text)
        if (match.start() == 0 or unqualified_text[match.start() - 1] not in "-_.")
        and (match.end() == len(unqualified_text) or unqualified_text[match.end()] not in "-_.")
    ]
    candidates = [*qualified, *standalone]
    return list(dict.fromkeys(match.casefold() for match in candidates))


def _extract_signals(text: str) -> list[SignalType]:
    signals = [SignalType.METRICS]
    lowered = text.lower()
    if "log" in lowered:
        signals.append(SignalType.LOGS)
    if "trace" in lowered or "span" in lowered:
        signals.append(SignalType.TRACES)
    return signals


def heuristic_intent(prompt: str) -> Intent:
    """Build a deterministic Intent from operational vocabulary in *prompt*.

    Guarantees at least one archetype so the pipeline always routes through
    the deterministic archetype engine (never the LLM freeform path).
    """
    archetypes = _match_archetypes(prompt)
    if not archetypes:
        archetypes = [ArchetypeMatch(type="golden_signals", confidence=0.35)]

    top = archetypes[0].type
    intent = Intent(
        summary=prompt.strip()[:200],
        domain=_DOMAIN_BY_ARCHETYPE.get(top, "general"),
        services=_extract_services(prompt),
        environments=_extract_environments(prompt),
        signals=_extract_signals(prompt),
        keywords=expand_operational_terms(prompt, []),
        timerange=_extract_timerange(prompt),
        problem_type=top,
        archetypes=archetypes,
    )
    intent.keyword_evidence = [e.as_dict() for e in operational_evidence(prompt)]
    logger.info(
        "intent_fallback_used",
        reason="no_llm_api_key",
        archetypes=[(a.type, a.confidence) for a in intent.archetypes],
        services=intent.services,
        environments=intent.environments,
        timerange=intent.timerange,
    )
    return intent
