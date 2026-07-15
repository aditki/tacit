"""History and diagnostic helpers for pipeline orchestration."""

from __future__ import annotations

from typing import Any

import structlog

from tacit.backends.base import PublishResult
from tacit.errors import HistoryWriteFailed
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry, SignalType
from tacit.pipeline.progress import emit_progress

logger = structlog.get_logger()


class PipelineRecorder:
    """Centralize best-effort history writes and diagnostic stage recording."""

    def __init__(self, history: Any, investigation_id: str, run_id: str | None = None):
        self.history = history
        self.investigation_id = investigation_id
        self.run_id = run_id

    def stage(self, stage: str, status: str, reason_code: str, **details: Any) -> None:
        emit_progress(stage, status, reason_code, **details)
        record_stage(self.history, self.investigation_id, stage, status, reason_code, **details)
        if self.run_id and hasattr(self.history, "append_event"):
            try:
                self.history.append_event(
                    self.investigation_id,
                    self.run_id,
                    "stage_completed",
                    {"stage": stage, "status": status, "reason_code": reason_code, "details": details},
                )
            except Exception:
                logger.warning("history_append_event_failed", stage=stage, exc_info=True)

    def event(self, event_type: str, payload: dict[str, Any]) -> None:
        """Best-effort append of a typed reconstruction event."""
        if not self.run_id or not hasattr(self.history, "append_event"):
            return
        try:
            self.history.append_event(self.investigation_id, self.run_id, event_type, payload)
        except Exception:
            logger.warning("history_append_event_failed", event_type=event_type, exc_info=True)

    def intent(self, intent: Intent) -> None:
        emit_progress(
            "intent",
            "passed",
            "intent_classified",
            summary=intent.summary,
            domain=intent.domain,
            services=intent.services,
            archetypes=[{"type": a.type, "confidence": a.confidence} for a in intent.archetypes],
            timerange=intent.timerange,
        )
        try:
            self.history.record_intent(
                self.investigation_id,
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
            logger.warning(
                "history_record_intent_failed",
                error_type=HistoryWriteFailed.__name__,
                exc_info=True,
            )

    def selected_intent(
        self,
        intent: Intent,
        ranked_archetypes: list[tuple[Any, float]],
        learned_archetypes: list[tuple[Any, float]],
    ) -> None:
        record_selected_intent(self.history, self.investigation_id, intent, ranked_archetypes, learned_archetypes)

    def discovery(self, catalog_discovery: Any) -> None:
        emit_progress(
            "discovery",
            "passed",
            "metrics_discovered",
            datasources_found=len(catalog_discovery.datasource_types),
            datasource_types=catalog_discovery.datasource_types,
            metrics_catalog_size=len(catalog_discovery.metric_catalog),
        )
        try:
            self.history.record_discovery(
                self.investigation_id,
                datasources_found=len(catalog_discovery.datasource_types),
                datasource_types=catalog_discovery.datasource_types,
                metrics_catalog_size=len(catalog_discovery.metric_catalog),
            )
        except Exception:
            logger.warning(
                "history_record_discovery_failed",
                error_type=HistoryWriteFailed.__name__,
                exc_info=True,
            )

    def queries(self, dashboard_spec: DashboardSpec, *, path_used: str) -> None:
        emit_progress(
            "queries",
            "passed",
            "queries_recorded",
            panel_count=len(dashboard_spec.panels),
            path_used=path_used,
        )
        try:
            queries_for_history, metrics_for_history = query_history_payload(dashboard_spec)
            self.history.record_queries(
                self.investigation_id,
                metrics_selected=metrics_for_history,
                generated_queries=queries_for_history,
                panel_count=len(dashboard_spec.panels),
                path_used=path_used,
            )
        except Exception:
            logger.warning(
                "history_record_queries_failed",
                error_type=HistoryWriteFailed.__name__,
                exc_info=True,
            )

    def validation(self, warnings: list[str], *, panels_before: int, final_panel_count: int) -> None:
        panels_dropped = max(panels_before - final_panel_count, 0)
        if final_panel_count and panels_dropped:
            status = "partial"
            reason = "some_panels_rejected"
        elif final_panel_count:
            status = "passed"
            reason = "queries_validated"
        else:
            status = "failed"
            reason = "queries_validated"
        emit_progress(
            "validation",
            status,
            reason,
            panels_before=panels_before,
            final_panel_count=final_panel_count,
            panels_dropped=panels_dropped,
            warnings=warnings,
        )
        try:
            self.history.record_validation(
                self.investigation_id,
                warnings=warnings,
                panels_dropped=panels_dropped,
                final_panel_count=final_panel_count,
            )
        except Exception:
            logger.warning(
                "history_record_validation_failed",
                error_type=HistoryWriteFailed.__name__,
                exc_info=True,
            )

    def finish(self, **kwargs: Any) -> None:
        try:
            self.history.finish(self.investigation_id, **kwargs)
        except Exception:
            logger.warning(
                "history_finish_failed",
                error_type=HistoryWriteFailed.__name__,
                exc_info=True,
            )
        if self.run_id and hasattr(self.history, "complete_run"):
            try:
                pipeline_status = str(kwargs.get("status", "success"))
                succeeded = pipeline_status == "success"
                self.history.complete_run(
                    self.run_id,
                    status="completed" if succeeded else "failed",
                    error_code=(
                        "" if succeeded else ("pipeline_timeout" if pipeline_status == "timeout" else "pipeline_failed")
                    ),
                    error_detail=str(kwargs.get("error", "")),
                )
            except Exception:
                logger.warning("history_complete_run_failed", exc_info=True)


def record_stage(history: Any, inv_id: str, stage: str, status: str, reason_code: str, **details: Any) -> None:
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
        logger.warning(
            "history_record_stage_failed",
            stage=stage,
            error_type=HistoryWriteFailed.__name__,
            exc_info=True,
        )


def history_archetypes(
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


def history_signals(intent_signals: list[SignalType], selected_archetypes: list[tuple[Any, float]]) -> list[str]:
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


def compiled_query_diagnostics(dashboard_spec: DashboardSpec, catalog: list[MetricEntry]) -> tuple[str, str, dict]:
    """Compare compiled PromQL references with the live catalog before probing."""
    from tacit.query_parsing.promql import extract_metrics_from_promql

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


def query_history_payload(dashboard_spec: DashboardSpec) -> tuple[list[dict[str, str]], list[str]]:
    """Return generated-query and metric lists for history/provenance stores."""
    queries = [{"expr": q.expr, "panel_title": p.title} for p in dashboard_spec.panels for q in p.queries if q.expr]
    metrics = list(
        {q.expr.split("{")[0].split("(")[-1].strip() for p in dashboard_spec.panels for q in p.queries if q.expr}
    )
    return queries, metrics


def surviving_datasource_names(
    dashboard_spec: DashboardSpec,
    metric_catalog: list[MetricEntry],
    datasource_catalog: list[MetricEntry],
) -> list[str]:
    """Return datasource names backing surviving panels, preserving query order."""
    uid_to_name: dict[str, str] = {}
    for entry in [*metric_catalog, *datasource_catalog]:
        if entry.datasource_uid and entry.datasource_name:
            uid_to_name.setdefault(entry.datasource_uid, entry.datasource_name)
    surviving: list[str] = []
    seen: set[str] = set()
    for panel in dashboard_spec.panels:
        for query in panel.queries:
            name = uid_to_name.get(query.datasource_uid) or query.datasource_type or query.datasource_uid
            if name and name not in seen:
                seen.add(name)
                surviving.append(name)
    return surviving


def dashboard_summary(
    dashboard_spec: DashboardSpec,
    path_used: str,
    datasource_names: list[str],
    publish_results: dict[str, PublishResult],
) -> str:
    """Build the user-facing dashboard summary from surviving artifacts."""
    ds_info = ", ".join(datasource_names) if datasource_names else "none"
    summary_parts = [
        f"Created dashboard **{dashboard_spec.title}** with {len(dashboard_spec.panels)} panels.",
        f"Timerange: last {dashboard_spec.timerange}",
        f"Datasources used: {ds_info}",
        f"Path: {path_used}",
    ]
    for name, result in publish_results.items():
        if result.url:
            summary_parts.append(f"{name.title()}: {result.url}")
    return "\n".join(summary_parts)


def record_selected_intent(
    history: Any,
    inv_id: str,
    intent: Intent,
    ranked_archetypes: list[tuple[Any, float]],
    learned_archetypes: list[tuple[Any, float]],
) -> None:
    """Persist selected archetype context without leaking persistence policy into the runner."""
    try:
        history.record_intent(
            inv_id,
            summary=intent.summary,
            domain=intent.domain,
            services=intent.services,
            keywords=intent.keywords,
            signals=history_signals(intent.signals, ranked_archetypes),
            problem_type=intent.problem_type,
            archetypes=history_archetypes(intent.archetypes, ranked_archetypes, learned_archetypes),
            timerange=intent.timerange,
        )
    except Exception:
        logger.warning("history_record_selected_archetypes_failed", exc_info=True)
