"""Deterministic *candidate* signal inference from dashboard context.

The dashboard carries far more semantic signal than the metric name alone, so
ingestion can propose signal mappings for *custom* metrics without anyone
hand-teaching each one. Classification is weighted and explainable:

    metric name morphology   40%
    panel + row title        30%
    unit                     15%
    query shape (PromQL fn)  10%
    dashboard grouping        5%

This is a *candidate* layer: results carry ``score``, calibrated ``confidence``,
``margin`` over the runner-up, and the ``evidence``/``evidence_sources`` behind
them, so approval can be conservative (see ``auto_teach_eligible``) and never
poison the mapping store. LLM-assisted inference for the low-coverage long tail
is a separate opt-in.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

SIGNAL_FAMILIES = (
    "latency",
    "errors",
    "traffic",
    "saturation",
    "availability",
    "backlog",
    "resource_usage",
    "security",
    "capacity",
    "caching",
)

# Bump when rules change so learned mappings can be invalidated/replayed.
INFERENCE_VERSION = "1.1"

MIN_SCORE = 0.25  # below this we emit nothing rather than guess

# Legacy auto-teach eligibility thresholds. This is candidate-quality evidence;
# governed promotion remains the authority for runtime activation.
_AUTOTEACH_SCORE = 0.70
_AUTOTEACH_MARGIN = 0.25
_EXPLICIT_SCORE = 0.45  # explicit names may teach with a single source

# Source weights (sum ~1.0).
_W_NAME = 0.40
_W_TITLE = 0.30
_W_UNIT = 0.15
_W_QUERY = 0.10
_W_GROUP = 0.05

# Metadata/info metrics that are not operational signals → never classify.
_IGNORE_RE = re.compile(r"(_info$|build_info|_build_info|_version|version_info|_created$|created_|^scrape_|^logback_)")

# Time-point / lifecycle overrides: these end in _seconds but are NOT latency.
# (family, regex, evidence)
_OVERRIDE_RULES: list[tuple[str, re.Pattern[str], str]] = [
    (
        "security",
        re.compile(r"(expiry|expiration|valid_until|not_after|cert.*seconds|ssl.*seconds)"),
        "certificate/expiry → security",
    ),
    (
        "availability",
        re.compile(r"(uptime|_start_time|boot_time|last_success|last_run|heartbeat|timestamp|_age_)"),
        "time-point/uptime → availability, not latency",
    ),
]

# Explicit, high-strength name rules (>=0.9) may qualify with a single source.
# (regex, family, strength, evidence)
_NAME_RULES: list[tuple[re.Pattern[str], str, float, str]] = [
    (re.compile(r"([45]xx|http_?5\d\d|status_?5\d\d)"), "errors", 1.0, "name indicates HTTP errors"),
    (re.compile(r"(errors?|_err)(_total|_count)?(\b|_|$)"), "errors", 1.0, "name contains 'error'"),
    (re.compile(r"(_failed|_failures?|_failing)"), "errors", 1.0, "name contains 'fail'"),
    (re.compile(r"(_dropped|_drops?|_discard|_rejected)"), "errors", 0.9, "name indicates drops/rejects"),
    (re.compile(r"(_timeouts?)"), "errors", 0.7, "name indicates timeouts"),
    (re.compile(r"(latency|duration|response_time|_rtt)"), "latency", 1.0, "name indicates latency/duration"),
    (
        re.compile(r"((db|database|sql|connection|pool).*wait|wait.*(db|database|sql|connection|pool))"),
        "latency",
        0.95,
        "name indicates database or connection-pool wait time",
    ),
    # Bare time unit → latency, but NOT counters of seconds (e.g.
    # process_cpu_seconds_total is CPU time, a resource — handled by the
    # resource rule). True latency counters carry duration/latency keywords.
    (re.compile(r"(_seconds|_time)(_sum|_count)?$"), "latency", 0.8, "name ends with a time unit"),
    (re.compile(r"(_depth|_backlog|_pending|_lag|queue)"), "backlog", 0.9, "name indicates a queue/backlog"),
    (re.compile(r"(inflight|in_flight|concurrent)"), "saturation", 0.95, "name indicates concurrency"),
    (
        # Match both Redis-INFO order (connected_clients) and OTLP semconv
        # order (clients_connected); same for blocked/rejected.
        re.compile(
            r"(connected_clients|clients_connected|blocked_clients|clients_blocked"
            r"|rejected_connections|connections_rejected|client_recent_max)"
        ),
        "saturation",
        1.0,
        "name indicates client pressure",
    ),
    (
        re.compile(
            r"(active_requests|requests_active|open_requests|requests_in_progress"
            r"|active_connections|connections_active)"
        ),
        "saturation",
        0.9,
        "name indicates active request pressure",
    ),
    # Cache rules sit ABOVE the generic traffic/_total rules so cache counters
    # are not misread as request-rate traffic.
    (
        re.compile(r"(keyspace_hits|keyspace_misses|cache_hits?|cache_miss|_cache_hit|_cache_miss)"),
        "caching",
        0.95,
        "name indicates cache hits/misses",
    ),
    (
        # Both word orders: evicted_keys / keys_evicted, expired_keys / keys_expired.
        re.compile(r"(evicted_keys|keys_evicted|expired_keys|keys_expired|_evicted\b|_evictions?|cache_evict)"),
        "caching",
        0.9,
        "name indicates cache evictions",
    ),
    (
        re.compile(r"(cache_size|cache_entries|redis_db_keys|redis_keys_count)"),
        "caching",
        0.7,
        "name indicates cache size",
    ),
    (
        re.compile(r"(cpu|memory|_mem_|disk|_bytes|utilization|usage|file_descriptors|_fd_)"),
        "resource_usage",
        0.8,
        "name indicates a resource",
    ),
    (re.compile(r"(requests?|_req_|_rpc_|_calls?|_ops)"), "traffic", 0.7, "name indicates request/call traffic"),
    (re.compile(r"_total$"), "traffic", 0.65, "counter (_total) → traffic"),
    (
        re.compile(r"(^up$|_up\b|healthy|readiness|_ready\b|reachable|session_health|availab)"),
        "availability",
        0.8,
        "name indicates availability/health",
    ),
    (
        re.compile(
            r"(restarts?|(?:health|http|tcp|dns)check_status|"
            r"(?:pod|node|container|deployment|replica|connector|task|job|process|service)"
            r".*_status(?!_code)(?:_|$)|(?:lifecycle|health|readiness)_status)"
        ),
        "availability",
        0.85,
        "name indicates lifecycle status",
    ),
    (
        re.compile(r"(unauthorized|denied|forbidden|_tls_|_cert|auth_fail|security)"),
        "security",
        0.85,
        "name indicates security",
    ),
    (
        re.compile(r"(num_|_nodes?\b|_hosts?\b|policies|endpoints)"),
        "capacity",
        0.5,
        "name indicates an inventory gauge",
    ),
]

# Keyword → family for free text (panel/row titles, descriptions). ALL matches
# are returned (a title like "5xx error rate" implies both errors and traffic).
_TEXT_RULES: list[tuple[re.Pattern[str], str, str]] = [
    (re.compile(r"\b(error|fail|drop|discard|reject)"), "errors", "title mentions errors"),
    (
        re.compile(r"\b(latency|duration|response time|p9\d|percentile|apply time|rtt)"),
        "latency",
        "title mentions latency",
    ),
    (re.compile(r"\b(throughput|traffic|requests?|qps|rps|ops|rate)\b"), "traffic", "title mentions traffic"),
    (re.compile(r"\b(saturation|utiliz|in[- ]?flight|concurrency)"), "saturation", "title mentions saturation"),
    (re.compile(r"\b(backlog|queue|lag|pending)"), "backlog", "title mentions a backlog/queue"),
    (re.compile(r"\b(cpu|memory|disk|bytes|resource)"), "resource_usage", "title mentions a resource"),
    (re.compile(r"\b(up|health|availab|ready|uptime)"), "availability", "title mentions availability"),
    (re.compile(r"\b(denied|unauthorized|tls|cert|security|auth)"), "security", "title mentions security"),
    (re.compile(r"\b(capacity|number of|total hosts|total nodes|policies)"), "capacity", "title mentions capacity"),
    (re.compile(r"\b(cache|eviction|evicted|hit ratio|hit rate|keyspace)"), "caching", "title mentions caching"),
]

_UNIT_FAMILY: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"^(s|ms|ns|us|µs|seconds)$"), "latency"),
    (re.compile(r"(percent|percentunit)"), "saturation"),
    (re.compile(r"(bytes|decbytes|bits|kb|mb|gb)"), "resource_usage"),
    (re.compile(r"(ops|reqps|rps|cps|wps|/s|persec)"), "traffic"),
]

# Suffixes that are accumulator artifacts (always safe to drop from the name).
_ACCUMULATOR_SUFFIXES = ("_bucket", "_sum", "_count", "_total")
# Unit suffixes only dropped for latency signals (keeps "cpu_seconds" meaningful).
_LATENCY_UNIT_SUFFIXES = ("_seconds", "_milliseconds", "_ms")
_TIME_WORDS = ("time", "seconds", "duration", "latency", "_ms", "millis", "rtt")


@dataclass
class InferredSignal:
    metric: str
    signal_family: str
    signal_name: str
    score: float
    confidence: float  # calibrated 0..1 (score / total source weight)
    margin: float  # winner score minus runner-up
    evidence: list[str] = field(default_factory=list)
    evidence_sources: list[str] = field(default_factory=list)
    explicit_name: bool = False

    @property
    def confidence_label(self) -> str:
        if self.score >= _AUTOTEACH_SCORE and self.margin >= _AUTOTEACH_MARGIN:
            return "high"
        if self.score >= _EXPLICIT_SCORE:
            return "medium"
        return "low"

    @property
    def auto_teach_eligible(self) -> bool:
        """Conservative candidate-quality gate; this does not grant runtime authority."""
        if self.explicit_name and self.score >= _EXPLICIT_SCORE:
            return True
        return self.score >= _AUTOTEACH_SCORE and self.margin >= _AUTOTEACH_MARGIN and len(self.evidence_sources) >= 2

    @property
    def why_not_auto_taught(self) -> str | None:
        """Reason a candidate was held back, or None if it is eligible."""
        if self.auto_teach_eligible:
            return None
        if self.score < _AUTOTEACH_SCORE:  # includes < _EXPLICIT_SCORE
            return "low_score"
        if self.margin < _AUTOTEACH_MARGIN:
            return "low_margin"
        if len(self.evidence_sources) < 2:
            return "single_source_only"
        return "not_eligible"

    def as_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "signal_family": self.signal_family,
            "signal_name": self.signal_name,
            "score": round(self.score, 4),
            "confidence": round(self.confidence, 4),
            "confidence_label": self.confidence_label,
            "margin": round(self.margin, 4),
            "evidence": self.evidence,
            "evidence_sources": self.evidence_sources,
            "auto_teach_eligible": self.auto_teach_eligible,
        }


def _text_families(text: str) -> list[tuple[str, str]]:
    """All distinct families implied by free text (not just the first)."""
    text = text.lower()
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for pattern, family, evidence in _TEXT_RULES:
        if pattern.search(text) and family not in seen:
            seen.add(family)
            out.append((family, evidence))
    return out


def _derive_signal_name(metric: str, family: str) -> str:
    """Collapse a metric to a stable signal name (family-aware).

    Accumulator suffixes (_bucket/_sum/_count/_total) always drop so a
    histogram's tri-metrics fold into one signal. Unit suffixes (_seconds) drop
    only for latency, so e.g. ``process_cpu_seconds_total`` keeps "cpu_seconds".
    """
    name = metric.strip().lower()
    changed = True
    while changed:
        changed = False
        for suffix in _ACCUMULATOR_SUFFIXES:
            if name.endswith(suffix) and len(name) > len(suffix) + 1:
                name = name[: -len(suffix)]
                changed = True
    if family == "latency":
        for suffix in _LATENCY_UNIT_SUFFIXES:
            if name.endswith(suffix) and len(name) > len(suffix) + 1:
                name = name[: -len(suffix)]
                break
    return name or metric.lower()


def _owning_panels(metric: str, panels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Panels that reference this metric, by extraction OR query substring.

    Falls back to a query-text match (and base-name variants) so context isn't
    lost when upstream metric extraction is imperfect.
    """
    base = metric
    for suffix in _ACCUMULATOR_SUFFIXES:
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    owning = []
    for p in panels:
        if metric in (p.get("metrics") or []):
            owning.append(p)
            continue
        queries = " ".join(p.get("queries") or [])
        if metric in queries or (base and base in queries):
            owning.append(p)
    return owning


def infer_signal(
    metric: str,
    panels: list[dict[str, Any]] | None = None,
    *,
    unit: str = "",
    metric_type: str = "",
    dimensions: list[str] | None = None,
    namespace: str = "",
) -> InferredSignal | None:
    """Infer a single metric's candidate signal, or None if weak/metadata."""
    panels = panels or []
    name = metric.lower()

    # Metadata/info metrics are not signals.
    if _IGNORE_RE.search(name):
        return None

    scores: dict[str, float] = {fam: 0.0 for fam in SIGNAL_FAMILIES}
    evidence: dict[str, list[str]] = {fam: [] for fam in SIGNAL_FAMILIES}
    sources: dict[str, set[str]] = {fam: set() for fam in SIGNAL_FAMILIES}
    explicit: dict[str, bool] = {fam: False for fam in SIGNAL_FAMILIES}
    available_weight = _W_NAME  # name is always available

    def vote(family: str, weight: float, why: str, source: str, *, is_explicit: bool = False) -> None:
        scores[family] += weight
        evidence[family].append(why)
        sources[family].add(source)
        if is_explicit:
            explicit[family] = True

    # 1. Time-point / lifecycle overrides (suppress the generic _seconds→latency).
    suppress_latency_time = False
    for family, pattern, why in _OVERRIDE_RULES:
        if pattern.search(name):
            vote(family, _W_NAME, why, "name")
            suppress_latency_time = True

    # 2. Name morphology (first rule per family wins).
    seen_families: set[str] = set()
    for pattern, family, strength, why in _NAME_RULES:
        if family in seen_families:
            continue
        # Don't let the weak _seconds/_time rule fire for time-point metrics.
        if suppress_latency_time and family == "latency" and "ends with a time unit" in why:
            continue
        if pattern.search(name):
            vote(family, _W_NAME * strength, why, "name", is_explicit=strength >= 0.9)
            seen_families.add(family)

    owning = _owning_panels(metric, panels)

    # 3. Panel + description text (0.30, split across matched families) and
    #    5. row grouping (0.05). Aggregate across owning panels first so
    #    repeated/duplicate panels cannot exceed each source's advertised cap.
    title_votes: dict[str, set[str]] = {}
    group_votes: dict[str, set[str]] = {}
    for p in owning:
        text = " ".join(str(p.get(k, "")) for k in ("title", "description"))
        for fam, why in _text_families(text):
            title_votes.setdefault(fam, set()).add(why)
        row_hits = _text_families(str(p.get("row", "")))
        for fam, why in row_hits:
            group_votes.setdefault(fam, set()).add(f"row grouping: {why}")
    if title_votes:
        per = _W_TITLE / len(title_votes)
        for fam, whys in title_votes.items():
            vote(fam, per, "; ".join(sorted(whys)), "title")
    if group_votes:
        per = _W_GROUP / len(group_votes)
        for fam, whys in group_votes.items():
            vote(fam, per, "; ".join(sorted(whys)), "group")
    if owning:
        available_weight += _W_TITLE + _W_GROUP

    # 4. Unit (0.15). Cold discovery supplies datasource metadata directly;
    # dashboard ingestion can still supply the unit through owning panels.
    unit_voted = False
    candidate_units = [unit, *(str(p.get("unit", "")) for p in owning)]
    for raw_unit in candidate_units:
        normalized_unit = str(raw_unit).lower()
        for pattern, family in _UNIT_FAMILY:
            if normalized_unit and pattern.search(normalized_unit):
                vote(family, _W_UNIT, f"unit '{normalized_unit}' → {family}", "unit")
                unit_voted = True
                break
        if unit_voted:
            break
    if owning or unit:
        available_weight += _W_UNIT

    # Metric type and label/scope metadata are tie breakers, not standalone
    # classifiers. This keeps sparse catalogs useful without turning generic
    # counters or labels into confident semantic guesses.
    normalized_type = metric_type.lower()
    if normalized_type in {"histogram", "summary", "gaugehistogram"} and any(word in name for word in _TIME_WORDS):
        vote("latency", 0.10, f"{normalized_type} time metric → latency", "metric_type")
        available_weight += 0.10
    elif normalized_type in {"counter", "sum"}:
        leader = max(scores, key=lambda family: scores[family])
        if scores[leader] > 0:
            vote(leader, 0.05, f"{normalized_type} confirms {leader}", "metric_type")
            available_weight += 0.05

    scope_text = " ".join([namespace, *(dimensions or [])]).lower()
    if scope_text:
        scope_rules = (
            (r"\b(http|rpc|route|method|status_code)\b", "traffic"),
            (r"\b(cache|redis|memcached|keyspace)\b", "caching"),
            (r"\b(queue|messaging|kafka|consumer)\b", "backlog"),
            (r"\b(cpu|memory|disk|filesystem|container|process)\b", "resource_usage"),
        )
        for scope_pattern, family in scope_rules:
            if re.search(scope_pattern, scope_text) and scores[family] > 0:
                vote(family, 0.10, f"labels/scope confirm {family}", "scope")
                available_weight += 0.10
                break

    # 6. Query shape (0.10): histogram → latency; rate/increase confirms
    #    counter-ness and BOOSTS the leading family rather than forcing traffic.
    queries = " ".join(q for p in owning for q in (p.get("queries") or []))
    if queries:
        available_weight += _W_QUERY
        if "histogram_quantile" in queries:
            vote("latency", _W_QUERY, "query uses histogram_quantile", "query")
        elif "rate(" in queries or "increase(" in queries:
            leader = max(scores, key=lambda f: scores[f])
            if scores[leader] > 0:
                vote(leader, _W_QUERY, f"rate()/increase() confirms counter → {leader}", "query")
            else:
                vote("traffic", _W_QUERY, "rate()/increase() with no other evidence → traffic", "query")

    # Pick winner + runner-up.
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    family, top = ranked[0]
    second = ranked[1][1] if len(ranked) > 1 else 0.0
    if top < MIN_SCORE:
        return None

    confidence = min(top / max(available_weight, 1.0), 1.0)
    return InferredSignal(
        metric=metric,
        signal_family=family,
        signal_name=_derive_signal_name(metric, family),
        score=min(top, 1.0),
        confidence=confidence,
        margin=round(top - second, 4),
        evidence=evidence[family],
        evidence_sources=sorted(sources[family]),
        explicit_name=explicit[family],
    )


def infer_signals(
    metrics: list[str],
    panels: list[dict[str, Any]] | None = None,
    *,
    min_score: float = MIN_SCORE,
) -> list[InferredSignal]:
    """Infer candidate signals for metrics, sorted by score (desc)."""
    out: list[InferredSignal] = []
    for metric in dict.fromkeys(metrics):  # dedupe, preserve order
        sig = infer_signal(metric, panels)
        if sig and sig.score >= min_score:
            out.append(sig)
    out.sort(key=lambda s: s.score, reverse=True)
    return out


def coverage(metrics: list[str], inferred: list[InferredSignal]) -> float:
    """Fraction of unique metrics that got a candidate (for the LLM fallback gate)."""
    total = len(set(metrics))
    if not total:
        return 1.0
    return len({s.metric for s in inferred}) / total
