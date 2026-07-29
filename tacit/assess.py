"""Deterministic operational-knowledge assessment — `tacit assess`.

Answers, with zero LLM calls: what did Tacit ingest, extract, resolve, and
fail to resolve? Every number comes from local SQLite stores (signals,
learning artifacts, investigation history), so the command runs offline and
with zero API keys.

Optional LLM enrichment (`tacit assess --llm`) turns the deterministic report
into a short narrative: what this probably means and what to look at first.
"""

from __future__ import annotations

import json
import time
from typing import Any

import structlog

from tacit.signals.schema import GLOBAL_BOOTSTRAP_TENANT_ID

logger = structlog.get_logger()

_DAY_S = 86_400.0
_STALE_REVIEW_AGE_DAYS = 30

READINESS_LOW = "Low"
READINESS_MEDIUM = "Medium"
READINESS_HIGH = "High"


def _rows(conn, sql: str, params: tuple = ()) -> list[Any]:
    return conn.execute(sql, params).fetchall()


def _one(conn, sql: str, params: tuple = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else 0


def _json_list(raw: str | None) -> list[Any]:
    try:
        value = json.loads(raw or "[]")
        return value if isinstance(value, list) else []
    except (TypeError, ValueError):
        return []


def _inventory(conn, tenant_id: str) -> dict[str, Any]:
    dashboards_by_status = {
        row[0]: row[1]
        for row in _rows(
            conn,
            "SELECT status, COUNT(*) FROM ingested_dashboards WHERE tenant_id=? GROUP BY status",
            (tenant_id,),
        )
    }
    alerts_by_status = {
        row[0]: row[1]
        for row in _rows(
            conn,
            "SELECT status, COUNT(*) FROM ingested_alerts WHERE tenant_id=? GROUP BY status",
            (tenant_id,),
        )
    }
    artifacts_by_type = {
        row[0]: row[1]
        for row in _rows(
            conn,
            "SELECT artifact_type, COUNT(*) FROM learned_artifacts WHERE tenant_id=? GROUP BY artifact_type",
            (tenant_id,),
        )
    }
    mappings_by_state = {
        row[0]: row[1]
        for row in _rows(
            conn,
            """SELECT review_state, COUNT(*) FROM signal_metric_mappings
               WHERE tenant_id IN (?, ?) GROUP BY review_state""",
            (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
        )
    }
    return {
        "dashboards_ingested": sum(dashboards_by_status.values()),
        "dashboards_by_status": dashboards_by_status,
        "alerts_ingested": sum(alerts_by_status.values()),
        "alerts_by_status": alerts_by_status,
        "alerts_stale": _one(
            conn,
            "SELECT COUNT(*) FROM ingested_alerts WHERE tenant_id=? AND stale=1",
            (tenant_id,),
        ),
        "runbooks": artifacts_by_type.get("runbook", 0),
        "incidents": sum(n for t, n in artifacts_by_type.items() if t in ("incident", "pagerduty_incident")),
        "artifacts_by_type": artifacts_by_type,
        "artifacts_stale": _one(
            conn,
            "SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id=? AND stale=1",
            (tenant_id,),
        ),
        "signal_types": _one(
            conn,
            """SELECT COUNT(*) FROM (
                   SELECT signal_type FROM signal_types
                   UNION SELECT signal_type FROM tenant_signal_types WHERE tenant_id=?
               )""",
            (tenant_id,),
        ),
        "metric_mappings": sum(mappings_by_state.values()),
        "mappings_by_review_state": mappings_by_state,
        "rejected_signal_candidates": _one(
            conn,
            "SELECT COUNT(*) FROM rejected_signal_candidates WHERE tenant_id=?",
            (tenant_id,),
        ),
    }


def _services(conn, tenant_id: str) -> dict[str, Any]:
    names: set[str] = set()
    owned: set[str] = set()

    for row in _rows(
        conn,
        "SELECT DISTINCT entity FROM ownership_hints WHERE tenant_id=? AND entity!=''",
        (tenant_id,),
    ):
        names.add(row[0])
        owned.add(row[0])
    for row in _rows(conn, "SELECT service_hints FROM ingested_alerts WHERE tenant_id=?", (tenant_id,)):
        names.update(str(v) for v in _json_list(row[0]))
    for row in _rows(
        conn,
        "SELECT context_services FROM signal_metric_mappings WHERE tenant_id IN (?, ?)",
        (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
    ):
        names.update(str(v) for v in _json_list(row[0]))
    for row in _rows(
        conn,
        "SELECT DISTINCT source_entity FROM dependency_hints WHERE tenant_id=? AND source_entity != '' "
        "UNION SELECT DISTINCT target_entity FROM dependency_hints WHERE tenant_id=? AND target_entity != ''",
        (tenant_id, tenant_id),
    ):
        names.add(row[0])

    unowned = sorted(names - owned)
    return {
        "known": len(names),
        "sample": sorted(names)[:10],
        "with_ownership": len(owned & names),
        "missing_ownership": len(unowned),
        "missing_ownership_sample": unowned[:10],
    }


def _coverage(conn, tenant_id: str) -> dict[str, Any]:
    total = _one(
        conn,
        """SELECT COUNT(*) FROM (
               SELECT signal_type FROM signal_types
               UNION SELECT signal_type FROM tenant_signal_types WHERE tenant_id=?
           )""",
        (tenant_id,),
    )
    mapped = _one(
        conn,
        "SELECT COUNT(DISTINCT signal_type) FROM signal_metric_mappings "
        "WHERE tenant_id IN (?, ?) AND review_state IN ('approved', 'trusted')",
        (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
    )
    candidate_only = _one(
        conn,
        """SELECT COUNT(DISTINCT signal_type) FROM signal_metric_mappings
           WHERE tenant_id IN (?, ?) AND signal_type NOT IN (
               SELECT DISTINCT signal_type FROM signal_metric_mappings
               WHERE tenant_id IN (?, ?) AND review_state IN ('approved', 'trusted')
           )""",
        (tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID, tenant_id, GLOBAL_BOOTSTRAP_TENANT_ID),
    )
    return {
        "signal_types_total": total,
        "signal_types_with_trusted_mapping": mapped,
        "signal_types_candidate_only": candidate_only,
        "knowledge_coverage_pct": round(100.0 * mapped / total, 1) if total else 0.0,
    }


def _duplicate_dashboards(conn, tenant_id: str) -> dict[str, Any]:
    groups: dict[str, list[str]] = {}
    for row in _rows(
        conn,
        "SELECT dashboard_uid, dashboard_title, metrics_found FROM ingested_dashboards WHERE tenant_id=?",
        (tenant_id,),
    ):
        metrics = sorted(str(m) for m in _json_list(row[2]))
        if not metrics:
            continue
        key = "|".join(metrics)
        groups.setdefault(key, []).append(row[1] or row[0])
    duplicates = [titles for titles in groups.values() if len(titles) > 1]
    return {
        "duplicate_groups": len(duplicates),
        "duplicate_dashboards": sum(len(g) for g in duplicates),
        "sample": duplicates[:5],
    }


def _quality(conn, now: float, tenant_id: str) -> dict[str, Any]:
    # Alerts whose service hints have no ownership attribution at all.
    owned_entities = {
        row[0]
        for row in _rows(
            conn,
            "SELECT DISTINCT entity FROM ownership_hints WHERE tenant_id=? AND entity!=''",
            (tenant_id,),
        )
    }
    alerts_without_owner = 0
    for row in _rows(
        conn,
        "SELECT service_hints FROM ingested_alerts WHERE tenant_id=? AND enabled=1",
        (tenant_id,),
    ):
        hints = {str(v) for v in _json_list(row[0])}
        if not hints or not (hints & owned_entities):
            alerts_without_owner += 1

    # Runbooks that produced no signal candidates and no signal-hinted evidence.
    runbooks_without_signals = _one(
        conn,
        """
        SELECT COUNT(*) FROM learned_artifacts a
        WHERE a.tenant_id=? AND a.artifact_type = 'runbook'
          AND NOT EXISTS (SELECT 1 FROM signal_mapping_candidates c
                          WHERE c.tenant_id=a.tenant_id AND c.artifact_id = a.artifact_id)
          AND NOT EXISTS (
              SELECT 1 FROM evidence_requirements e
              WHERE e.tenant_id=a.tenant_id AND e.artifact_id = a.artifact_id
                AND e.signal_hint IS NOT NULL AND e.signal_hint != ''
          )
        """,
        (tenant_id,),
    )

    # RCA claims from incidents: rejected/ignored vs still-unreviewed candidates.
    incident_filter = (
        "IN (SELECT artifact_id FROM learned_artifacts WHERE tenant_id=? "
        "AND artifact_type IN ('incident', 'pagerduty_incident'))"
    )
    rca_rejected = _one(
        conn,
        f"SELECT COUNT(*) FROM evidence_requirements WHERE tenant_id=? AND artifact_id {incident_filter} "
        "AND review_state IN ('rejected', 'ignored')",
        (tenant_id, tenant_id),
    )
    stale_cutoff = now - _STALE_REVIEW_AGE_DAYS * _DAY_S
    rca_unreviewed = _one(
        conn,
        f"SELECT COUNT(*) FROM evidence_requirements WHERE tenant_id=? AND artifact_id {incident_filter} "
        "AND review_state = 'candidate' AND created_at < ?",
        (tenant_id, tenant_id, stale_cutoff),
    )

    # Unresolved evidence: claims that never matched live telemetry.
    unresolved_evidence = _one(
        conn,
        "SELECT COUNT(*) FROM evidence_requirements WHERE tenant_id=? AND observation_state='indeterminate'",
        (tenant_id,),
    )

    # Artifact yield: extraction rows per learned artifact.
    artifact_count = _one(conn, "SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id=?", (tenant_id,))
    extraction_count = (
        _one(conn, "SELECT COUNT(*) FROM evidence_requirements WHERE tenant_id=?", (tenant_id,))
        + _one(conn, "SELECT COUNT(*) FROM ownership_hints WHERE tenant_id=?", (tenant_id,))
        + _one(conn, "SELECT COUNT(*) FROM dependency_hints WHERE tenant_id=?", (tenant_id,))
        + _one(conn, "SELECT COUNT(*) FROM signal_mapping_candidates WHERE tenant_id=?", (tenant_id,))
    )
    zero_yield = _one(
        conn,
        """
        SELECT COUNT(*) FROM learned_artifacts a
        WHERE a.tenant_id=?
          AND NOT EXISTS (SELECT 1 FROM evidence_requirements e
                          WHERE e.tenant_id=a.tenant_id AND e.artifact_id = a.artifact_id)
          AND NOT EXISTS (SELECT 1 FROM ownership_hints o
                          WHERE o.tenant_id=a.tenant_id AND o.artifact_id = a.artifact_id)
          AND NOT EXISTS (SELECT 1 FROM dependency_hints d
                          WHERE d.tenant_id=a.tenant_id AND d.artifact_id = a.artifact_id)
          AND NOT EXISTS (SELECT 1 FROM signal_mapping_candidates c
                          WHERE c.tenant_id=a.tenant_id AND c.artifact_id = a.artifact_id)
        """,
        (tenant_id,),
    )

    return {
        "alerts_without_owner_attribution": alerts_without_owner,
        "runbooks_without_matching_signals": runbooks_without_signals,
        "incident_rca_claims_rejected_or_ignored": rca_rejected,
        "incident_rca_claims_unreviewed_over_30d": rca_unreviewed,
        "unresolved_evidence_claims": unresolved_evidence,
        "artifacts_with_zero_extractions": zero_yield,
        "avg_extractions_per_artifact": round(extraction_count / artifact_count, 2) if artifact_count else 0.0,
        **_duplicate_dashboards(conn, tenant_id),
    }


def _activity(history_stats: dict[str, Any]) -> dict[str, Any]:
    total = history_stats.get("total") or 0
    succeeded = history_stats.get("succeeded") or 0
    return {
        "investigations_total": total,
        "investigations_succeeded": succeeded,
        "success_rate_pct": round(100.0 * succeeded / total, 1) if total else 0.0,
        "avg_panels": round(history_stats.get("avg_panels") or 0.0, 1),
        "avg_time_s": round(history_stats.get("avg_time") or 0.0, 1),
        "archetype_path": history_stats.get("archetype_path") or 0,
        "freeform_path": history_stats.get("freeform_path") or 0,
    }


def _readiness(report: dict[str, Any]) -> dict[str, Any]:
    inventory = report["inventory"]
    coverage = report["coverage"]
    services = report["services"]
    activity = report["activity"]

    score = 0
    reasons: list[str] = []

    def add(points: int, ok: bool, yes: str, no: str) -> None:
        nonlocal score
        if ok:
            score += points
            reasons.append(f"+{points} {yes}")
        else:
            reasons.append(f"+0 {no}")

    trusted = inventory["mappings_by_review_state"].get("trusted", 0) + inventory["mappings_by_review_state"].get(
        "approved", 0
    )
    add(25, trusted > 0, f"{trusted} trusted/approved signal mappings", "no trusted signal mappings yet")
    add(
        20,
        coverage["knowledge_coverage_pct"] >= 50,
        f"knowledge coverage {coverage['knowledge_coverage_pct']}%",
        f"knowledge coverage below 50% ({coverage['knowledge_coverage_pct']}%)",
    )
    add(
        15,
        inventory["dashboards_ingested"] > 0,
        f"{inventory['dashboards_ingested']} dashboards ingested",
        "no dashboards ingested (run `tacit learn grafana`)",
    )
    add(
        10,
        inventory["alerts_ingested"] > 0,
        f"{inventory['alerts_ingested']} alerts ingested",
        "no alerts ingested (run `tacit learn alerts`)",
    )
    add(
        10,
        (inventory["runbooks"] + inventory["incidents"]) > 0,
        f"{inventory['runbooks']} runbooks / {inventory['incidents']} incidents learned",
        "no runbooks or incidents learned",
    )
    add(
        10,
        services["with_ownership"] > 0,
        f"ownership known for {services['with_ownership']} services",
        "no service ownership hints",
    )
    add(
        10,
        activity["investigations_succeeded"] > 0,
        f"{activity['investigations_succeeded']} successful investigations",
        "no successful investigations yet (run `tacit test` or `tacit demo`)",
    )

    if score >= 70:
        level = READINESS_HIGH
    elif score >= 40:
        level = READINESS_MEDIUM
    else:
        level = READINESS_LOW
    return {"score": score, "max_score": 100, "level": level, "reasons": reasons}


def build_assessment(
    signal_store: Any | None = None,
    history_store: Any | None = None,
    *,
    stores: Any | None = None,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Compute the deterministic operational-knowledge assessment."""
    from pathlib import Path

    from tacit.config import Settings
    from tacit.config import settings as global_settings
    from tacit.runtime_stores import RuntimeStores
    from tacit.tenancy import resolve_tenant_boundary

    def store_settings(store: Any) -> Settings | None:
        candidate = getattr(store, "_settings", None)
        return candidate if isinstance(candidate, Settings) else None

    def composition_identity(runtime_settings: Settings) -> tuple[str, str, str]:
        def normalized_path(value: str) -> str:
            return str(Path(value).resolve()) if value else ""

        return (
            str(runtime_settings.knowledge_tenant_id or "default"),
            normalized_path(str(runtime_settings.signals_db_path or "")),
            normalized_path(str(runtime_settings.history_db_path or "")),
        )

    def partial_runtime(store: Any, path_setting: str, store_label: str) -> RuntimeStores:
        runtime_settings = store_settings(store)
        configured_path = str(getattr(runtime_settings, path_setting, "") or "") if runtime_settings else ""
        actual_path = getattr(store, "_db_path", None)
        if runtime_settings is None or not configured_path or actual_path is None:
            raise ValueError(
                f"partial assessment {store_label} injection requires a Settings-backed configured database path"
            )
        if Path(configured_path).resolve() != Path(actual_path).resolve():
            raise ValueError(f"partial assessment {store_label} injection does not match its runtime settings")
        return RuntimeStores(runtime_settings)

    if stores is not None:
        signal_store = signal_store or stores.signals()
        history_store = history_store or stores.history()
    elif signal_store is None and history_store is None:
        stores = RuntimeStores(global_settings)
        signal_store = stores.signals()
        history_store = stores.history()
    elif signal_store is None:
        assert history_store is not None
        stores = partial_runtime(history_store, "history_db_path", "history-store")
        signal_store = stores.signals()
    elif history_store is None:
        stores = partial_runtime(signal_store, "signals_db_path", "signal-store")
        history_store = stores.history()

    assert signal_store is not None
    assert history_store is not None
    runtime_settings = getattr(stores, "settings", None)
    scoped_settings = [
        candidate
        for candidate in (store_settings(signal_store), store_settings(history_store))
        if candidate is not None
    ]
    configured_tenants = {str(candidate.knowledge_tenant_id or "default") for candidate in scoped_settings}
    if isinstance(runtime_settings, Settings):
        configured_tenants.add(str(runtime_settings.knowledge_tenant_id or "default"))
    if len(configured_tenants) > 1:
        raise ValueError("injected assessment stores use different tenant boundaries")
    composition_settings = list(scoped_settings)
    if isinstance(runtime_settings, Settings):
        composition_settings.append(runtime_settings)
    if len({composition_identity(candidate) for candidate in composition_settings}) > 1:
        raise ValueError("injected assessment stores use different runtime compositions")
    if not isinstance(runtime_settings, Settings):
        if not scoped_settings:
            raise ValueError("assessment stores must expose a Settings-backed runtime composition")
        runtime_settings = scoped_settings[0]
    selected_tenant = resolve_tenant_boundary(
        str(runtime_settings.knowledge_tenant_id or "default"),
        tenant_id,
    )
    now = time.time()
    with signal_store._conn() as conn:  # noqa: SLF001 — read-only sibling-module access
        report: dict[str, Any] = {
            "generated_at": now,
            "tenant_id": selected_tenant,
            "inventory": _inventory(conn, selected_tenant),
            "services": _services(conn, selected_tenant),
            "coverage": _coverage(conn, selected_tenant),
            "quality": _quality(conn, now, selected_tenant),
        }

    try:
        history_stats = history_store.stats(tenant_id=selected_tenant)
    except Exception:
        logger.warning("assess_history_stats_failed", exc_info=True)
        history_stats = {}
    report["activity"] = _activity(history_stats)
    report["readiness"] = _readiness(report)
    return report


async def narrate_assessment(report: dict[str, Any]) -> str:
    """Optional LLM enrichment: what does this mean, what to look at first."""
    from tacit.agents.llm import call_llm_text

    system_prompt = (
        "You are an SRE advisor reviewing an operational-knowledge assessment "
        "produced by Tacit. Given the JSON report, write a short plain-text "
        "narrative (max 250 words): 1) overall state in one sentence, "
        "2) the three most important gaps ordered by impact, 3) the single "
        "next action the team should take. Be concrete and reference the "
        "numbers. No markdown headers, no bullet symbols other than dashes."
    )
    narrative, _usage = await call_llm_text(system_prompt, json.dumps(report, default=str))
    return narrative
