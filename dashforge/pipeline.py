"""Orchestration pipeline: Prompt → Intent → Discover → Build → Publish."""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any, cast

import structlog

from dashforge.agents.intent import classify_intent
from dashforge.agents.metrics_discovery import discover_metrics
from dashforge.agents.providers.base import TokenUsage
from dashforge.agents.query_builder import build_dashboard
from dashforge.archetypes.engine import blend_archetypes, compile_archetype, rank_archetypes_by_coverage
from dashforge.archetypes.templates import (
    get_archetype,
    get_archetypes_by_confidence,
    get_archetypes_by_learning_context,
)
from dashforge.backends import get_active_backends
from dashforge.backends.base import PublishResult
from dashforge.cache import llm_cache, make_cache_key
from dashforge.catalog import catalog_for_services
from dashforge.config import settings
from dashforge.context.enrichment import enrich_context
from dashforge.evidence import (
    contributing_archetypes,
    observe_evidence,
    resolve_requirements_for_archetypes,
    summarize_evidence,
)
from dashforge.history import get_investigation_store
from dashforge.logging import bind_request_id, stage_log, unbind_request_id
from dashforge.models.schemas import (
    DashboardSpec,
    DashRequest,
    DashResponse,
    EvidenceRequirement,
    EvidenceResolution,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
)
from dashforge.ranking import prerank_metrics

logger = structlog.get_logger()

_SYMPTOM_SIGNAL_PANELS = {
    "request_latency": ("Observed Request Latency", "Application request timing evidence", "s"),
    "api_latency": ("Observed API Latency", "Application API timing evidence", "s"),
    "request_rate": ("Observed Request Rate", "Application request traffic evidence", "reqps"),
    "error_rate": ("Observed Error Rate", "Application error evidence", "percentunit"),
}


def _record_stage(history, inv_id: str, stage: str, status: str, reason_code: str, **details) -> None:
    """Best-effort persistence for diagnostic stage outcomes."""
    try:
        history.record_stage(
            inv_id,
            stage,
            status=status,
            reason_code=reason_code,
            details=details,
        )
    except Exception:
        logger.warning("history_record_stage_failed", stage=stage, exc_info=True)


def _semantic_mapping_diagnostics(metric_catalog) -> tuple[str, str, dict]:
    """Measure deterministic name-level semantic mapping independently of binding."""
    from dashforge.signal_inference import infer_signals

    names = list(dict.fromkeys(entry.name for entry in metric_catalog if entry.name))
    inferred = infer_signals(names)
    mapped = {item.metric: item.signal_family for item in inferred}
    unmapped = [name for name in names if name not in mapped]
    if not names:
        return "skipped", "no_named_metrics", {"metrics_total": 0}
    if not mapped:
        status, reason = "failed", "no_metrics_semantically_mapped"
    elif unmapped:
        status, reason = "partial", "some_metrics_unmapped"
    else:
        status, reason = "passed", "all_metrics_semantically_mapped"
    return (
        status,
        reason,
        {
            "metrics_total": len(names),
            "metrics_mapped": len(mapped),
            "coverage": round(len(mapped) / len(names), 4),
            "mapped": mapped,
            "unmapped": unmapped,
        },
    )


def _compiled_query_diagnostics(dashboard_spec, catalog) -> tuple[str, str, dict]:
    """Compare compiled PromQL references with the live catalog before probing."""
    from dashforge.dashboard_ingest import extract_metrics_from_promql

    catalog_names = {entry.name for entry in catalog if entry.name}
    references: set[str] = set()
    query_count = 0
    for panel in dashboard_spec.panels:
        for query in panel.queries:
            if not query.expr:
                continue
            query_count += 1
            if (query.query_language or "promql").lower() in {"", "promql"}:
                references.update(extract_metrics_from_promql(query.expr))
    present = sorted(references & catalog_names)
    missing = sorted(references - catalog_names)
    if not references:
        status, reason = "skipped", "no_promql_metric_references"
    elif missing and present:
        status, reason = "partial", "some_compiled_metrics_absent_from_catalog"
    elif missing:
        status, reason = "failed", "compiled_metrics_absent_from_catalog"
    else:
        status, reason = "passed", "all_compiled_metrics_present"
    return (
        status,
        reason,
        {
            "query_count": query_count,
            "referenced_metrics": sorted(references),
            "present_metrics": present,
            "missing_metrics": missing,
        },
    )


def _promql_service_selector(services: list[str]) -> str:
    if not services:
        return ""
    escaped = "|".join(re.escape(service) for service in services if service)
    return f'{{service=~"{escaped}"}}' if escaped else ""


def _build_symptom_evidence_dashboard(
    requirements: list[EvidenceRequirement],
    resolutions: list[EvidenceResolution],
    intent: Intent,
    *,
    catalog: list[MetricEntry],
    target_language: str,
    timerange: str,
) -> tuple[DashboardSpec, list[EvidenceResolution]]:
    """Build direct, validation-gated panels for observed application symptoms.

    This is intentionally not a root-cause fallback. It only surfaces resolved
    symptom evidence that already came from selected archetype requirements.
    """
    requirements_by_id = {requirement.id: requirement for requirement in requirements}
    resolutions_by_id = {resolution.requirement_id: resolution for resolution in resolutions}
    service_selector = _promql_service_selector(intent.services)
    panels: list[PanelSpec] = []
    rescue_resolutions: list[EvidenceResolution] = []
    seen: set[tuple[str, str, str]] = set()

    for requirement in requirements:
        resolution = resolutions_by_id.get(requirement.id)
        if resolution is None or resolution.status != "resolved" or not resolution.metric:
            resolution = _resolve_direct_symptom_evidence(
                requirement,
                intent,
                catalog,
                target_language=target_language,
            )
        if resolution is None or resolution.status != "resolved" or not resolution.metric:
            continue
        signal_type = requirement.signal_type
        if not signal_type and "latency" in resolution.metric:
            signal_type = "request_latency"
        elif not signal_type and ("request_rate" in resolution.metric or "requests_total" in resolution.metric):
            signal_type = "request_rate"
        if signal_type not in _SYMPTOM_SIGNAL_PANELS:
            continue
        query_language = (resolution.query_language or "promql").lower()
        datasource_type = (resolution.datasource_type or "prometheus").lower()
        if query_language not in {"", "promql"} or datasource_type not in {"", "prometheus"}:
            continue
        key = (signal_type, resolution.metric, resolution.datasource_uid)
        if key in seen:
            continue
        seen.add(key)
        rescue_resolutions.append(resolution)
        title, description, unit = _SYMPTOM_SIGNAL_PANELS[signal_type]
        panels.append(
            PanelSpec(
                title=title,
                description=description,
                row="Observed Symptoms",
                unit=unit,
                queries=[
                    PanelQuery(
                        expr=f"{resolution.metric}{service_selector}",
                        legend_format="{{service}}",
                        datasource_uid=resolution.datasource_uid,
                        datasource_type=resolution.datasource_type or "prometheus",
                        query_language=resolution.query_language or "promql",
                    )
                ],
            )
        )

    return (
        DashboardSpec(
            title=f"{intent.services[0].title() if intent.services else 'Service'} — Observed Symptoms",
            tags=["dashforge", "evidence", "symptom"],
            timerange=timerange,
            panels=panels,
        ),
        rescue_resolutions,
    )


def _resolve_direct_symptom_evidence(
    requirement: EvidenceRequirement,
    intent: Intent,
    catalog: list[MetricEntry],
    *,
    target_language: str,
) -> EvidenceResolution | None:
    """Resolve symptom evidence for direct observation panels.

    This deliberately skips archetype-template shape compatibility because the
    rescue panel queries the resolved symptom metric directly.
    """
    from dashforge.archetypes.engine import _datasource_type_for_language, _legacy_metric_signal
    from dashforge.signals import get_signal_store

    try:
        store = get_signal_store()
    except Exception:
        return None

    target_catalog = [
        entry for entry in catalog if (entry.query_language or "").lower() in {"", target_language.lower()}
    ]
    scoped_catalog = catalog_for_services(target_catalog, intent.services, include_unscoped=True)
    signal_type = requirement.signal_type or _legacy_metric_signal(
        store,
        requirement.default_metric,
        scoped_catalog,
        target_language,
    )
    if signal_type not in _SYMPTOM_SIGNAL_PANELS:
        return None
    resolved = store.resolve_signal(
        signal_type,
        scoped_catalog,
        context_service=intent.services[0] if intent.services else "",
        context_datasource_type=_datasource_type_for_language(target_language),
        target_query_language=target_language,
    )
    if not resolved:
        return None
    best_score = resolved[0][1]
    best = [item for item in resolved if item[1] == best_score]
    best_names = {entry.name for entry, _ in best}
    if len(best_names) > 1:
        return None
    entry, score = best[0]
    return EvidenceResolution(
        requirement_id=requirement.id,
        status="resolved",
        reason_code="direct_symptom_signal_resolved",
        metric=entry.name,
        datasource_uid=entry.datasource_uid,
        datasource_type=entry.datasource_type,
        query_language=entry.query_language,
        semantic_score=score,
        ownership_score=1.0,
    )


# Concurrency gate — prevents thundering-herd on LLM + Grafana APIs
_pipeline_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _pipeline_semaphore
    if _pipeline_semaphore is None:
        _pipeline_semaphore = asyncio.Semaphore(settings.pipeline_max_concurrent)
    return _pipeline_semaphore


def _history_archetypes(
    classifier_archetypes: list,
    selected_archetypes: list[tuple[Any, float]],
    learned_archetypes: list[tuple[Any, float]],
) -> list[dict[str, object]]:
    """Return history archetype records with selected learned matches included."""
    learned_ids = {arch.id for arch, _ in learned_archetypes}
    selected_ids = {arch.id for arch, _ in selected_archetypes}
    records: list[dict[str, object]] = []
    seen: set[str] = set()

    for arch, confidence in selected_archetypes:
        if arch.id in seen:
            continue
        seen.add(arch.id)
        records.append(
            {
                "type": arch.id,
                "name": arch.name,
                "confidence": confidence,
                "source": "learned" if arch.id in learned_ids else "classifier",
                "selected": True,
                "signals": sorted(set(arch.required_signals) | set(arch.signal_bindings.keys())),
            }
        )

    for match in classifier_archetypes:
        if match.type in seen or match.type in selected_ids:
            continue
        seen.add(match.type)
        records.append(
            {
                "type": match.type,
                "confidence": match.confidence,
                "source": "classifier",
                "selected": False,
                "signals": [],
            }
        )

    return records


def _history_signals(intent_signals: list, selected_archetypes: list[tuple[Any, float]]) -> list[str]:
    """Return intent signal types plus semantic signals from selected archetypes."""
    values: list[str] = []
    seen: set[str] = set()
    for signal in intent_signals:
        value = getattr(signal, "value", str(signal))
        if value and value not in seen:
            seen.add(value)
            values.append(value)
    for arch, _ in selected_archetypes:
        for signal in [*arch.required_signals, *arch.signal_bindings.keys()]:
            if signal and signal not in seen:
                seen.add(signal)
                values.append(signal)
    return values


def _discovery_keywords(intent: Any) -> list[str]:
    """Include advisory evidence when searching provider-scoped catalogs.

    Colloquial evidence may broaden discovery so providers such as CloudWatch
    can inspect the relevant namespace. It does not become trusted intent until
    post-discovery semantic-signal confirmation succeeds.
    """
    keywords = list(intent.keywords)
    seen = {str(keyword).lower() for keyword in keywords}
    for item in intent.keyword_evidence:
        keyword = str(item.get("keyword", ""))
        if keyword and keyword.lower() not in seen:
            seen.add(keyword.lower())
            keywords.append(keyword)
    return keywords


async def run_pipeline(request: DashRequest) -> DashResponse:
    """End-to-end: natural language → Grafana dashboard URL."""
    bind_request_id()
    sem = _get_semaphore()
    try:
        async with sem:
            try:
                return await asyncio.wait_for(
                    _run_pipeline_inner(request),
                    timeout=settings.pipeline_timeout_seconds,
                )
            except TimeoutError:
                logger.error("pipeline_timeout", user=request.user_id, timeout=settings.pipeline_timeout_seconds)
                try:
                    store = get_investigation_store()
                    inv_id = store.start(request.prompt, request.user_id, request.channel_id)
                    store.finish(
                        inv_id,
                        status="timeout",
                        error=f"Timed out after {settings.pipeline_timeout_seconds}s",
                    )
                except Exception:
                    pass
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary=f"Pipeline timed out after {settings.pipeline_timeout_seconds}s. "
                    "Try a more specific query or check datasource connectivity.",
                )
    finally:
        unbind_request_id()


async def _run_pipeline_inner(request: DashRequest) -> DashResponse:
    """Inner pipeline logic (wrapped with timeout + semaphore above).

    Uses the backend adapter pattern: each enabled vendor (Grafana, SignalFx,
    etc.) is a DashboardBackend instance.  The pipeline iterates over backends
    for discovery, validation, and publishing — zero vendor-specific if/else.
    """
    backends = get_active_backends()
    if not backends:
        return DashResponse(
            dashboard_url="",
            dashboard_uid="",
            panel_count=0,
            summary="No dashboard backends are enabled. " "Enable at least one of: grafana, signalfx.",
        )

    primary = backends[0]  # determines query language for compilation

    t_start = time.monotonic()
    timings: dict[str, float] = {}
    history = get_investigation_store()
    inv_id = history.start(request.prompt, request.user_id or "", request.channel_id or "")

    cumulative_tokens = TokenUsage()

    try:
        # ── 1. Intent Agent ──────────────────────────────────────────
        t0 = time.monotonic()
        intent, intent_usage = await classify_intent(request.prompt)
        timings["intent"] = time.monotonic() - t0
        cumulative_tokens = cumulative_tokens + intent_usage
        stage_log(
            "intent",
            (time.monotonic() - t0) * 1000,
            token_usage=intent_usage,
            prompt=request.prompt[:100],
            user_id=request.user_id,
            archetypes_detected=len(intent.archetypes),
            domain=intent.domain,
        )

        try:
            history.record_intent(
                inv_id,
                summary=intent.summary,
                domain=intent.domain,
                services=intent.services,
                keywords=intent.keywords,
                signals=[s.value for s in intent.signals],
                problem_type=intent.problem_type,
                archetypes=[{"type": a.type, "confidence": a.confidence} for a in intent.archetypes],
                timerange=intent.timerange,
            )
        except Exception:
            logger.warning("history_record_intent_failed", exc_info=True)

        # ── 2. Context enrichment (optional) ───────────────────
        t0 = time.monotonic()
        context_chunks = await enrich_context(intent)
        timings["context"] = time.monotonic() - t0
        stage_log(
            "context_enrichment",
            (time.monotonic() - t0) * 1000,
            chunks_returned=len(context_chunks),
        )

        # ── 3. Metric discovery — each backend contributes ───────────
        t0 = time.monotonic()
        discovery_keywords = _discovery_keywords(intent)
        metric_catalog = []
        datasource_catalog = []
        ds_types: list[str] = []
        for backend in backends:
            entries = await backend.discover_metrics(discovery_keywords, intent)
            metric_catalog.extend(entries)
            if entries:
                ds_types.append(backend.name)
            else:
                if not getattr(getattr(backend, "last_discovery_status", None), "available", True):
                    continue
                target_discovery = getattr(backend, "discover_datasource_targets", None)
                if target_discovery is not None:
                    targets = await target_discovery(discovery_keywords, intent)
                    datasource_catalog.extend(targets)
                    if targets and backend.name not in ds_types:
                        ds_types.append(backend.name)
        timings["metrics_fetch"] = time.monotonic() - t0
        stage_log(
            "metrics_fetch",
            (time.monotonic() - t0) * 1000,
            backends_queried=len(backends),
            datasource_types=ds_types,
            metrics_found=len(metric_catalog),
            datasource_targets_found=len(datasource_catalog),
        )

        try:
            history.record_discovery(
                inv_id,
                datasources_found=len(ds_types),
                datasource_types=ds_types,
                metrics_catalog_size=len(metric_catalog),
            )
        except Exception:
            logger.warning("history_record_discovery_failed", exc_info=True)

        catalog_for_compile = metric_catalog or datasource_catalog
        if metric_catalog:
            _record_stage(
                history,
                inv_id,
                "discovery",
                "passed",
                "named_metrics_discovered",
                metric_count=len(metric_catalog),
                datasource_count=len(ds_types),
                datasource_uids=sorted({entry.datasource_uid for entry in metric_catalog}),
            )
        elif datasource_catalog:
            _record_stage(
                history,
                inv_id,
                "discovery",
                "partial",
                "datasource_targets_without_metric_names",
                target_count=len(datasource_catalog),
                datasource_count=len(ds_types),
            )
        else:
            _record_stage(
                history,
                inv_id,
                "discovery",
                "failed",
                "no_metrics_or_datasource_targets",
                datasource_count=len(ds_types),
            )

        mapping_status, mapping_reason, mapping_details = _semantic_mapping_diagnostics(metric_catalog)
        _record_stage(
            history,
            inv_id,
            "semantic_mapping",
            mapping_status,
            mapping_reason,
            **mapping_details,
        )

        # Confirm advisory (colloquial) synonym evidence via SCOPED signal
        # coverage: a metaphor implying "cache" becomes a real keyword only if a
        # cache signal actually resolves against the discovered metrics, using
        # the signal store — not a global substring match. Keeps ambiguous
        # evidence from steering the investigation on its own.
        try:
            if intent.keyword_evidence and metric_catalog:
                from dashforge.agents.synonyms import SynonymEvidence, confirm_colloquial
                from dashforge.signals import get_signal_store

                signal_store = get_signal_store()
                _resolve_cache: dict[str, bool] = {}
                confirmation_catalog = catalog_for_services(metric_catalog, intent.services)
                context_service = intent.services[0] if intent.services else ""

                def _signal_resolves(sig: str) -> bool:
                    if sig not in _resolve_cache:
                        try:
                            hits = signal_store.resolve_signal(
                                sig,
                                confirmation_catalog,
                                context_service=context_service,
                                target_query_language=primary.query_language,
                            )
                            _resolve_cache[sig] = bool(hits)
                        except Exception:
                            _resolve_cache[sig] = False
                    return _resolve_cache[sig]

                evidence = [
                    SynonymEvidence(
                        keyword=str(e.get("keyword", "")),
                        score=float(e.get("score", 0.0)),
                        tier=str(e.get("tier", "")),
                        source=str(e.get("source", "")),
                    )
                    for e in intent.keyword_evidence
                ]
                confirmed = confirm_colloquial(evidence, _signal_resolves)
                for kw in confirmed:
                    if kw not in intent.keywords:
                        intent.keywords.append(kw)
                if confirmed:
                    logger.info("colloquial_evidence_confirmed", keywords=confirmed)
        except Exception:
            logger.warning("colloquial_confirmation_failed", exc_info=True)

        if not catalog_for_compile:
            ranked_archetypes = get_archetypes_by_confidence(intent.archetypes, min_confidence=0.3)
            ranked_ids = {arch.id for arch, _ in ranked_archetypes}
            learned_archetypes = get_archetypes_by_learning_context(
                intent,
                metric_catalog,
                min_confidence=0.35,
                exclude_ids=ranked_ids,
            )
            if learned_archetypes:
                ranked_archetypes.extend(learned_archetypes)
                ranked_archetypes.sort(key=lambda item: item[1], reverse=True)
            try:
                history.record_intent(
                    inv_id,
                    summary=intent.summary,
                    domain=intent.domain,
                    services=intent.services,
                    keywords=intent.keywords,
                    signals=_history_signals(intent.signals, ranked_archetypes),
                    problem_type=intent.problem_type,
                    archetypes=_history_archetypes(intent.archetypes, ranked_archetypes, learned_archetypes),
                    timerange=intent.timerange,
                )
            except Exception:
                logger.warning("history_record_selected_archetypes_failed", exc_info=True)

            unavailable = [
                backend.name
                for backend in backends
                if not getattr(getattr(backend, "last_discovery_status", None), "available", True)
            ]
            if unavailable:
                names = ", ".join(unavailable)
                error = f"Datasource discovery failed for: {names}"
                summary = (
                    f"Could not connect to {names} during datasource discovery. "
                    "Verify the backend is running and reachable, then retry."
                )
            else:
                error = "No metrics or datasource targets found"
                summary = (
                    "No metrics found across any datasource. " "Verify your datasources are configured and have data."
                )
            history.finish(
                inv_id,
                status="failed",
                error=error,
                timings=timings,
                total_time=time.monotonic() - t_start,
            )
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary=summary,
            )

        # ── 4. Multi-label archetype matching ────────────────────
        t0 = time.monotonic()
        ranked_archetypes = get_archetypes_by_confidence(intent.archetypes, min_confidence=0.3)
        ranked_ids = {arch.id for arch, _ in ranked_archetypes}
        learned_archetypes = get_archetypes_by_learning_context(
            intent,
            metric_catalog,
            min_confidence=0.35,
            exclude_ids=ranked_ids,
        )
        if learned_archetypes:
            ranked_archetypes.extend(learned_archetypes)
            ranked_archetypes.sort(key=lambda item: item[1], reverse=True)

        # Fallback: try legacy single-label lookup
        if not ranked_archetypes:
            legacy = get_archetype(intent.problem_type)
            if legacy is not None:
                ranked_archetypes = [(legacy, 0.9)]

        try:
            history.record_intent(
                inv_id,
                summary=intent.summary,
                domain=intent.domain,
                services=intent.services,
                keywords=intent.keywords,
                signals=_history_signals(intent.signals, ranked_archetypes),
                problem_type=intent.problem_type,
                archetypes=_history_archetypes(intent.archetypes, ranked_archetypes, learned_archetypes),
                timerange=intent.timerange,
            )
        except Exception:
            logger.warning("history_record_selected_archetypes_failed", exc_info=True)

        # Target query language comes from the primary backend
        target_language = primary.query_language

        # Coverage-rank + cap before deciding compile vs blend, so a strongly
        # matching archetype wins over many generic templates and the panel
        # explosion is bounded.
        if ranked_archetypes:
            ranked_archetypes = rank_archetypes_by_coverage(
                ranked_archetypes,
                catalog_for_compile,
                target_language=target_language,
                services=intent.services,
                max_archetypes=settings.max_blended_archetypes,
                min_secondary_coverage=settings.min_secondary_coverage,
            )

        if ranked_archetypes:
            primary_arch, primary_conf = ranked_archetypes[0]
            # ── ARCHETYPE PATH: deterministic, no LLM needed ──────────

            # Signal resolution happens inside compile_archetype/blend_archetypes
            # via _resolve_archetype_signals — substitutes missing metrics
            # with signal-resolved alternatives from the live catalog.

            if len(ranked_archetypes) > 1:
                dashboard_spec = blend_archetypes(
                    ranked_archetypes,
                    intent,
                    catalog_for_compile,
                    target_language=target_language,
                )
            else:
                dashboard_spec = compile_archetype(
                    primary_arch,
                    intent,
                    catalog_for_compile,
                    target_language=target_language,
                )
            timings["archetype_compile"] = time.monotonic() - t0
            stage_log(
                "archetype_compile",
                (time.monotonic() - t0) * 1000,
                primary_archetype=primary_arch.id,
                primary_confidence=primary_conf,
                archetypes_matched=len(ranked_archetypes),
                learned_archetypes_matched=len(learned_archetypes),
                panels_generated=len(dashboard_spec.panels),
                target_language=target_language,
                signal_bindings_count=len(primary_arch.signal_bindings),
            )
        else:
            # ── FREEFORM PATH: LLM-driven discovery + query generation ─
            if not metric_catalog:
                history.finish(
                    inv_id,
                    status="failed",
                    error="No metrics found for freeform generation",
                    timings=timings,
                    total_time=time.monotonic() - t_start,
                )
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary=(
                        "Datasource metadata was available, but no metrics matched your query. "
                        "Approve or teach a dashboard pattern for this service, or connect a "
                        "datasource with matching series."
                    ),
                )

            # Pre-rank to reduce LLM token cost
            t_prerank = time.monotonic()
            ranked_catalog = prerank_metrics(intent, metric_catalog)
            stage_log(
                "metric_ranking",
                (time.monotonic() - t_prerank) * 1000,
                metrics_considered=len(metric_catalog),
                metrics_selected=len(ranked_catalog),
            )

            # Metrics Discovery LLM (cached)
            discovery_cache_key = make_cache_key(
                "discovery",
                intent.summary,
                ",".join(intent.keywords),
                ",".join(e.name for e in ranked_catalog[:20]),
            )
            cached_discovery = llm_cache.get(discovery_cache_key)
            discovery_usage = TokenUsage()
            t_disc = time.monotonic()
            if cached_discovery is not None:
                discovery = cached_discovery
                discovery_cached = True
            else:
                discovery, discovery_usage = await discover_metrics(intent, ranked_catalog, context_chunks)
                cumulative_tokens = cumulative_tokens + discovery_usage
                if discovery.metrics:
                    llm_cache.set(discovery_cache_key, discovery)
                discovery_cached = False

            stage_log(
                "metrics_discovery",
                (time.monotonic() - t_disc) * 1000,
                token_usage=discovery_usage if not discovery_cached else None,
                catalog_size=len(ranked_catalog),
                metrics_selected=len(discovery.metrics),
                cached=discovery_cached,
            )

            if not discovery.metrics:
                history.finish(
                    inv_id,
                    status="failed",
                    error="No relevant metrics found by LLM",
                    timings=timings,
                    total_time=time.monotonic() - t_start,
                )
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary="Could not find relevant metrics for your query. "
                    "Try rephrasing with more specific service or metric names.",
                )

            # Post-validate LLM output
            valid_uids = {e.datasource_uid for e in metric_catalog}
            original_count = len(discovery.metrics)
            discovery.metrics = [m for m in discovery.metrics if m.datasource_uid in valid_uids]
            dropped = original_count - len(discovery.metrics)
            if dropped:
                logger.warning("llm_hallucinated_uids_dropped", dropped=dropped)

            if not discovery.metrics:
                history.finish(
                    inv_id,
                    status="failed",
                    error="All LLM-selected metrics had invalid datasource UIDs",
                    timings=timings,
                    total_time=time.monotonic() - t_start,
                )
                return DashResponse(
                    dashboard_url="",
                    dashboard_uid="",
                    panel_count=0,
                    summary="LLM selected metrics with invalid datasource references. " "Try rephrasing your query.",
                )

            # Query Builder Agent
            t0 = time.monotonic()
            dashboard_spec, qb_usage = await build_dashboard(intent, discovery, ranked_catalog)
            timings["query_builder"] = time.monotonic() - t0
            cumulative_tokens = cumulative_tokens + qb_usage
            stage_log(
                "query_builder",
                (time.monotonic() - t0) * 1000,
                token_usage=qb_usage,
                metrics_input=len(discovery.metrics),
                panels_generated=len(dashboard_spec.panels),
            )

        evidence_requirements: list[EvidenceRequirement] = []
        evidence_resolutions: list[EvidenceResolution] = []
        if ranked_archetypes:
            try:
                evidence_archetypes = contributing_archetypes(ranked_archetypes, dashboard_spec)
                evidence_requirements, evidence_resolutions = resolve_requirements_for_archetypes(
                    evidence_archetypes,
                    intent,
                    catalog_for_compile,
                    target_language=target_language,
                )
            except Exception:
                logger.warning("evidence_resolution_failed", exc_info=True)

        binding_status, binding_reason, binding_details = _compiled_query_diagnostics(
            dashboard_spec,
            catalog_for_compile,
        )
        _record_stage(
            history,
            inv_id,
            "binding",
            binding_status,
            binding_reason,
            **binding_details,
        )
        compiled_query_count = sum(len(panel.queries) for panel in dashboard_spec.panels)
        if compiled_query_count:
            _record_stage(
                history,
                inv_id,
                "compilation",
                "passed",
                "queries_compiled",
                panel_count=len(dashboard_spec.panels),
                query_count=compiled_query_count,
                path="archetype" if ranked_archetypes else "freeform",
            )
        else:
            _record_stage(
                history,
                inv_id,
                "compilation",
                "failed",
                "no_queries_compiled",
                panel_count=len(dashboard_spec.panels),
                path="archetype" if ranked_archetypes else "freeform",
            )

        # ── 5. Validate queries — primary backend validates ──────────
        t0 = time.monotonic()
        panels_before = len(dashboard_spec.panels)
        pre_validation_spec = dashboard_spec.model_copy(deep=True)
        dashboard_spec, validation_warnings = await primary.validate_queries(dashboard_spec, catalog_for_compile)
        if not dashboard_spec.panels and evidence_requirements:
            symptom_pre_validation_spec, symptom_resolutions = _build_symptom_evidence_dashboard(
                evidence_requirements,
                evidence_resolutions,
                intent,
                catalog=catalog_for_compile,
                target_language=target_language,
                timerange=pre_validation_spec.timerange,
            )
            if symptom_pre_validation_spec.panels:
                symptom_spec, symptom_warnings = await primary.validate_queries(
                    symptom_pre_validation_spec,
                    catalog_for_compile,
                )
                validation_warnings.extend(symptom_warnings)
                _record_stage(
                    history,
                    inv_id,
                    "symptom_evidence_rescue",
                    "passed" if symptom_spec.panels else "failed",
                    "symptom_panels_validated" if symptom_spec.panels else "symptom_panels_rejected",
                    panels_before=len(symptom_pre_validation_spec.panels),
                    panels_after=len(symptom_spec.panels),
                )
                if symptom_spec.panels:
                    rescue_requirement_ids = {resolution.requirement_id for resolution in symptom_resolutions}
                    evidence_resolutions = [
                        resolution
                        for resolution in evidence_resolutions
                        if resolution.requirement_id not in rescue_requirement_ids
                    ]
                    evidence_resolutions.extend(symptom_resolutions)
                    pre_validation_spec = symptom_pre_validation_spec
                    dashboard_spec = symptom_spec
            else:
                _record_stage(
                    history,
                    inv_id,
                    "symptom_evidence_rescue",
                    "skipped",
                    "no_resolved_symptom_evidence",
                )
        timings["query_validation"] = time.monotonic() - t0
        stage_log(
            "query_validation",
            (time.monotonic() - t0) * 1000,
            backend=primary.name,
            panels_before=panels_before,
            panels_after=len(dashboard_spec.panels),
            warnings=len(validation_warnings),
        )
        panels_after = len(dashboard_spec.panels)
        if panels_after == 0:
            validation_status, validation_reason = "failed", "all_panels_rejected"
        elif panels_after < panels_before:
            validation_status, validation_reason = "partial", "some_panels_rejected"
        else:
            validation_status, validation_reason = "passed", "all_panels_survived"
        _record_stage(
            history,
            inv_id,
            "validation",
            validation_status,
            validation_reason,
            panels_before=panels_before,
            panels_after=panels_after,
            warnings=validation_warnings,
        )
        try:
            if evidence_requirements:
                evidence_observations = observe_evidence(
                    evidence_requirements,
                    evidence_resolutions,
                    pre_validation_spec,
                    dashboard_spec,
                )
                evidence_summary = summarize_evidence(
                    evidence_requirements,
                    evidence_resolutions,
                    evidence_observations,
                )
                critical_total = cast(int, evidence_summary["critical_total"])
                critical_observed = cast(int, evidence_summary["critical_observed"])
                if critical_total and critical_observed == critical_total:
                    evidence_status, evidence_reason = "passed", "all_critical_evidence_observed"
                elif critical_observed:
                    evidence_status, evidence_reason = "partial", "some_critical_evidence_observed"
                else:
                    evidence_status, evidence_reason = "failed", "no_critical_evidence_observed"
                _record_stage(
                    history,
                    inv_id,
                    "evidence",
                    evidence_status,
                    evidence_reason,
                    **evidence_summary,
                )
            else:
                _record_stage(
                    history,
                    inv_id,
                    "evidence",
                    "skipped",
                    "no_declared_evidence_requirements",
                    path="archetype" if ranked_archetypes else "freeform",
                )
        except Exception:
            logger.warning("history_record_evidence_failed", exc_info=True)

        # Record queries after validation
        try:
            queries_for_history = [
                {"expr": q.expr, "panel_title": p.title} for p in dashboard_spec.panels for q in p.queries if q.expr
            ]
            metrics_for_history = list(
                {
                    q.expr.split("{")[0].split("(")[-1].strip()
                    for p in dashboard_spec.panels
                    for q in p.queries
                    if q.expr
                }
            )
            history.record_queries(
                inv_id,
                metrics_selected=metrics_for_history,
                generated_queries=queries_for_history,
                panel_count=len(dashboard_spec.panels),
                path_used="archetype" if ranked_archetypes else "freeform",
            )
        except Exception:
            logger.warning("history_record_queries_failed", exc_info=True)

        if not dashboard_spec.panels:
            history.finish(
                inv_id,
                status="failed",
                error="All panels empty after validation",
                timings=timings,
                total_time=time.monotonic() - t_start,
            )
            return DashResponse(
                dashboard_url="",
                dashboard_uid="",
                panel_count=0,
                summary="No panels returned data for your query. "
                "The service or metrics you asked about may not exist "
                "in the connected datasources.\n" + "\n".join(validation_warnings),
            )

        # ── 6. Publish — each backend publishes independently ────────
        publish_results: dict[str, PublishResult] = {}
        for backend in backends:
            t0 = time.monotonic()
            try:
                result = await backend.publish(dashboard_spec)
                publish_results[backend.name] = result
            except Exception:
                logger.warning("publish_failed", backend=backend.name, exc_info=True)
            timings[f"{backend.name}_publish"] = time.monotonic() - t0
            stage_log(
                "publish",
                (time.monotonic() - t0) * 1000,
                backend=backend.name,
                success=backend.name in publish_results,
            )

        # Effective identifiers — first successful backend wins
        grafana_result = publish_results.get("grafana", PublishResult())
        sfx_result = publish_results.get("signalfx", PublishResult())
        effective_uid = grafana_result.uid or sfx_result.uid or ""
        effective_url = grafana_result.url or sfx_result.url or ""

        path_used = "archetype" if ranked_archetypes else "freeform"
        # Report only the datasources that actually back a *surviving* panel,
        # never the full discovery catalog. Map each surviving query's UID back
        # to a human name via the discovered catalogs, falling back to type.
        uid_to_name: dict[str, str] = {}
        for entry in [*metric_catalog, *datasource_catalog]:
            if entry.datasource_uid and entry.datasource_name:
                uid_to_name.setdefault(entry.datasource_uid, entry.datasource_name)
        surviving_ds: list[str] = []
        seen_ds: set[str] = set()
        for panel in dashboard_spec.panels:
            for q in panel.queries:
                name = uid_to_name.get(q.datasource_uid) or q.datasource_type or q.datasource_uid
                if name and name not in seen_ds:
                    seen_ds.add(name)
                    surviving_ds.append(name)
        ds_info = ", ".join(surviving_ds) if surviving_ds else "none"
        summary_parts = [
            f"Created dashboard **{dashboard_spec.title}** with " f"{len(dashboard_spec.panels)} panels.",
            f"Timerange: last {dashboard_spec.timerange}",
            f"Datasources used: {ds_info}",
            f"Path: {path_used}",
        ]
        for name, result in publish_results.items():
            if result.url:
                summary_parts.append(f"{name.title()}: {result.url}")
        summary = "\n".join(summary_parts)

        total_s = time.monotonic() - t_start
        timings["total"] = total_s
        timings_rounded = {k: round(v, 2) for k, v in timings.items()}

        # Record validation results
        try:
            history.record_validation(
                inv_id,
                warnings=validation_warnings,
                panels_dropped=max(panels_before - len(dashboard_spec.panels), 0),
                final_panel_count=len(dashboard_spec.panels),
            )
        except Exception:
            logger.warning("history_record_validation_failed", exc_info=True)

        stage_log(
            "pipeline_complete",
            total_s * 1000,
            token_usage=cumulative_tokens,
            user_id=request.user_id,
            channel_id=request.channel_id,
            dashboard_uid=effective_uid,
            panel_count=len(dashboard_spec.panels),
            path=path_used,
            timings=timings_rounded,
        )

        # Record final result
        try:
            history.finish(
                inv_id,
                status="success",
                dashboard_uid=effective_uid,
                dashboard_url=effective_url,
                timings=timings_rounded,
                total_time=total_s,
            )
        except Exception:
            logger.warning("history_finish_failed", exc_info=True)

        # ── 7. Record provenance for feedback system ──────────────────
        try:
            from dashforge.feedback import get_feedback_store

            feedback_store = get_feedback_store()
            metrics_used = list(
                {
                    q.expr.split("{")[0].split("(")[-1].strip()
                    for p in dashboard_spec.panels
                    for q in p.queries
                    if q.expr
                }
            )
            feedback_store.record_provenance(
                dashboard_uid=effective_uid,
                prompt=request.prompt,
                problem_type=intent.problem_type,
                archetypes=[{"type": a.type, "confidence": a.confidence} for a in intent.archetypes],
                metrics_used=metrics_used,
                panel_count=len(dashboard_spec.panels),
                path_used=path_used,
                dashboard_url=effective_url,
                user_id=request.user_id,
                channel_id=request.channel_id,
            )
        except Exception:
            logger.warning("provenance_record_failed", exc_info=True)

        return DashResponse(
            dashboard_url=grafana_result.url,
            dashboard_uid=effective_uid,
            panel_count=len(dashboard_spec.panels),
            summary=summary,
            signalfx_url=sfx_result.url,
            signalfx_dashboard_id=sfx_result.uid,
        )

    finally:
        for backend in backends:
            try:
                await backend.close()
            except Exception:
                logger.warning("backend_close_failed", backend=backend.name, exc_info=True)
