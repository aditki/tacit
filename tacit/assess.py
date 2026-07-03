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


def _inventory(conn) -> dict[str, Any]:
    dashboards_by_status = {
        row[0]: row[1] for row in _rows(conn, "SELECT status, COUNT(*) FROM ingested_dashboards GROUP BY status")
    }
    alerts_by_status = {
        row[0]: row[1] for row in _rows(conn, "SELECT status, COUNT(*) FROM ingested_alerts GROUP BY status")
    }
    artifacts_by_type = {
        row[0]: row[1]
        for row in _rows(conn, "SELECT artifact_type, COUNT(*) FROM learned_artifacts GROUP BY artifact_type")
    }
    mappings_by_state = {
        row[0]: row[1]
        for row in _rows(conn, "SELECT review_state, COUNT(*) FROM signal_metric_mappings GROUP BY review_state")
    }
    return {
        "dashboards_ingested": sum(dashboards_by_status.values()),
        "dashboards_by_status": dashboards_by_status,
        "alerts_ingested": sum(alerts_by_status.values()),
        "alerts_by_status": alerts_by_status,
        "alerts_stale": _one(conn, "SELECT COUNT(*) FROM ingested_alerts WHERE stale = 1"),
        "runbooks": artifacts_by_type.get("runbook", 0),
        "incidents": sum(n for t, n in artifacts_by_type.items() if t in ("incident", "pagerduty_incident")),
        "artifacts_by_type": artifacts_by_type,
        "artifacts_stale": _one(conn, "SELECT COUNT(*) FROM learned_artifacts WHERE stale = 1"),
        "signal_types": _one(conn, "SELECT COUNT(*) FROM signal_types"),
        "metric_mappings": sum(mappings_by_state.values()),
        "mappings_by_review_state": mappings_by_state,
        "rejected_signal_candidates": _one(conn, "SELECT COUNT(*) FROM rejected_signal_candidates"),
    }


def _services(conn) -> dict[str, Any]:
    names: set[str] = set()
    owned: set[str] = set()

    for row in _rows(conn, "SELECT DISTINCT entity FROM ownership_hints WHERE entity != ''"):
        names.add(row[0])
        owned.add(row[0])
    for row in _rows(conn, "SELECT service_hints FROM ingested_alerts"):
        names.update(str(v) for v in _json_list(row[0]))
    for row in _rows(conn, "SELECT context_services FROM signal_metric_mappings"):
        names.update(str(v) for v in _json_list(row[0]))
    for row in _rows(
        conn,
        "SELECT DISTINCT source_entity FROM dependency_hints WHERE source_entity != '' "
        "UNION SELECT DISTINCT target_entity FROM dependency_hints WHERE target_entity != ''",
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


def _coverage(conn) -> dict[str, Any]:
    total = _one(conn, "SELECT COUNT(*) FROM signal_types")
    mapped = _one(
        conn,
        "SELECT COUNT(DISTINCT signal_type) FROM signal_metric_mappings "
        "WHERE review_state IN ('approved', 'trusted')",
    )
    candidate_only = _one(
        conn,
        "SELECT COUNT(DISTINCT signal_type) FROM signal_metric_mappings WHERE signal_type NOT IN "
        "(SELECT DISTINCT signal_type FROM signal_metric_mappings WHERE review_state IN ('approved', 'trusted'))",
    )
    return {
        "signal_types_total": total,
        "signal_types_with_trusted_mapping": mapped,
        "signal_types_candidate_only": candidate_only,
        "knowledge_coverage_pct": round(100.0 * mapped / total, 1) if total else 0.0,
    }


def _duplicate_dashboards(conn) -> dict[str, Any]:
    groups: dict[str, list[str]] = {}
    for row in _rows(conn, "SELECT dashboard_uid, dashboard_title, metrics_found FROM ingested_dashboards"):
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


def _quality(conn, now: float) -> dict[str, Any]:
    # Alerts whose service hints have no ownership attribution at all.
    owned_entities = {row[0] for row in _rows(conn, "SELECT DISTINCT entity FROM ownership_hints WHERE entity != ''")}
    alerts_without_owner = 0
    for row in _rows(conn, "SELECT service_hints FROM ingested_alerts WHERE enabled = 1"):
        hints = {str(v) for v in _json_list(row[0])}
        if not hints or not (hints & owned_entities):
            alerts_without_owner += 1

    # Runbooks that produced no signal candidates and no signal-hinted evidence.
    runbooks_without_signals = _one(
        conn,
        """
        SELECT COUNT(*) FROM learned_artifacts a
        WHERE a.artifact_type = 'runbook'
          AND NOT EXISTS (SELECT 1 FROM signal_mapping_candidates c WHERE c.artifact_id = a.artifact_id)
          AND NOT EXISTS (
              SELECT 1 FROM evidence_requirements e
              WHERE e.artifact_id = a.artifact_id AND e.signal_hint IS NOT NULL AND e.signal_hint != ''
          )
        """,
    )

    # RCA claims from incidents: rejected/ignored vs still-unreviewed candidates.
    incident_filter = (
        "IN (SELECT artifact_id FROM learned_artifacts " "WHERE artifact_type IN ('incident', 'pagerduty_incident'))"
    )
    rca_rejected = _one(
        conn,
        f"SELECT COUNT(*) FROM evidence_requirements WHERE artifact_id {incident_filter} "
        "AND review_state IN ('rejected', 'ignored')",
    )
    stale_cutoff = now - _STALE_REVIEW_AGE_DAYS * _DAY_S
    rca_unreviewed = _one(
        conn,
        f"SELECT COUNT(*) FROM evidence_requirements WHERE artifact_id {incident_filter} "
        "AND review_state = 'candidate' AND created_at < ?",
        (stale_cutoff,),
    )

    # Unresolved evidence: claims that never matched live telemetry.
    unresolved_evidence = _one(
        conn,
        "SELECT COUNT(*) FROM evidence_requirements WHERE observation_state = 'indeterminate'",
    )

    # Artifact yield: extraction rows per learned artifact.
    artifact_count = _one(conn, "SELECT COUNT(*) FROM learned_artifacts")
    extraction_count = (
        _one(conn, "SELECT COUNT(*) FROM evidence_requirements")
        + _one(conn, "SELECT COUNT(*) FROM ownership_hints")
        + _one(conn, "SELECT COUNT(*) FROM dependency_hints")
        + _one(conn, "SELECT COUNT(*) FROM signal_mapping_candidates")
    )
    zero_yield = _one(
        conn,
        """
        SELECT COUNT(*) FROM learned_artifacts a
        WHERE NOT EXISTS (SELECT 1 FROM evidence_requirements e WHERE e.artifact_id = a.artifact_id)
          AND NOT EXISTS (SELECT 1 FROM ownership_hints o WHERE o.artifact_id = a.artifact_id)
          AND NOT EXISTS (SELECT 1 FROM dependency_hints d WHERE d.artifact_id = a.artifact_id)
          AND NOT EXISTS (SELECT 1 FROM signal_mapping_candidates c WHERE c.artifact_id = a.artifact_id)
        """,
    )

    return {
        "alerts_without_owner_attribution": alerts_without_owner,
        "runbooks_without_matching_signals": runbooks_without_signals,
        "incident_rca_claims_rejected_or_ignored": rca_rejected,
        "incident_rca_claims_unreviewed_over_30d": rca_unreviewed,
        "unresolved_evidence_claims": unresolved_evidence,
        "artifacts_with_zero_extractions": zero_yield,
        "avg_extractions_per_artifact": round(extraction_count / artifact_count, 2) if artifact_count else 0.0,
        **_duplicate_dashboards(conn),
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


def build_assessment(signal_store: Any | None = None, history_store: Any | None = None) -> dict[str, Any]:
    """Compute the deterministic operational-knowledge assessment."""
    if signal_store is None:
        from tacit.signals.store import get_signal_store

        signal_store = get_signal_store()
    if history_store is None:
        from tacit.history import get_investigation_store

        history_store = get_investigation_store()

    now = time.time()
    with signal_store._conn() as conn:  # noqa: SLF001 — read-only sibling-module access
        report: dict[str, Any] = {
            "generated_at": now,
            "inventory": _inventory(conn),
            "services": _services(conn),
            "coverage": _coverage(conn),
            "quality": _quality(conn, now),
        }

    try:
        history_stats = history_store.stats()
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
    return await call_llm_text(system_prompt, json.dumps(report, default=str))
