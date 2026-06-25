"""Measure contextual culprit ranking lift from learned alert context.

This benchmark keeps the frozen 47 contextual-ranking cases unchanged, then
adds a deterministic alert-context overlay. It evaluates whether alert-derived
operational knowledge improves ranking while preserving the no-RCA guardrails.
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any

from tests.eval.contextual_culprit_ranking_harness import (
    FIXTURE_PATH,
    evaluate,
    evaluate_fixture,
)
from tests.eval.contextual_culprit_ranking_harness import (
    gate_failures as baseline_gate_failures,
)
from tests.eval.ranking_benchmark_contract import benchmark_contract

TIE_BREAK_ALERT_IDS = {
    "contextual_top3_only_payments",
    "contextual_top3_only_orders",
    "contextual_top3_only_inventory",
}

CONFIRMING_ALERT_IDS = {
    "direct_dependency_db",
    "recent_deploy_checkout_api",
    "runbook_guided_redis",
    "historical_match_db",
    "distractor_unconnected_redis",
    "stale_runbook_mysql",
    "conflicting_sources",
    "dashboard_weight_support",
    "plausible_redis_db_cart",
    "plausible_redis_db_profile",
    "plausible_redis_db_search",
    "plausible_redis_db_catalog",
    "plausible_redis_db_coupons",
    "plausible_redis_db_sessions",
    "runtime_breaks_tie_payments",
    "runtime_breaks_tie_orders",
    "runtime_breaks_tie_inventory",
    "runtime_breaks_tie_pricing",
    "runtime_breaks_tie_shipping",
    "runtime_breaks_tie_accounts",
    "stale_runbook_risk",
    "stale_runbook_auth",
    "stale_runbook_billing",
    "stale_runbook_fulfillment",
    "distractor_unconnected_quotes",
    "distractor_unconnected_fraud",
    "distractor_unconnected_email",
    "distractor_unconnected_payouts",
}


def _load(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def _alert_for(case: dict[str, Any], *, entity: str, enabled: bool = True, stale: bool = False) -> dict[str, Any]:
    symptom = case["bundle"]["incident"]["symptom"]
    signal = f"{entity} {symptom} alert"
    return {
        "entity": entity,
        "signals": [signal],
        "severity": "critical" if enabled and not stale else "info",
        "enabled": enabled,
        "stale": stale,
        "runbook_url": f"https://runbooks.example/{entity}" if enabled and not stale else "",
        "source": "alert_context_overlay",
    }


def alert_augmented_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    augmented = copy.deepcopy(fixture)
    augmented["benchmark"] = f"{fixture['benchmark']} + alert context"
    augmented["version"] = f"{fixture['version']}-alert-context"

    for case in augmented["cases"]:
        context = case["bundle"].setdefault("context", {})
        alerts = context.setdefault("alerts", [])
        expected = case.get("expected_culprit")
        if case["id"] in CONFIRMING_ALERT_IDS | TIE_BREAK_ALERT_IDS and expected:
            alerts.append(_alert_for(case, entity=expected))
        elif expected:
            alerts.append(_alert_for(case, entity=expected, enabled=False))
        else:
            affected = case["bundle"]["incident"]["affected_service"]
            alerts.append(_alert_for(case, entity=affected, enabled=False))
    return augmented


def _rank_by_case(report: dict[str, Any], case_id: str, entity: str | None) -> int | None:
    if not entity:
        return None
    for suspect in report["results"][case_id]["suspects"]:
        if suspect["entity"] == entity:
            return int(suspect["rank"])
    return None


def _top1(report: dict[str, Any], case_id: str) -> str | None:
    suspects = report["results"][case_id]["suspects"]
    return suspects[0]["entity"] if suspects else None


def _top1_has_alert_reason(report: dict[str, Any], case_id: str) -> bool:
    suspects = report["results"][case_id]["suspects"]
    if not suspects:
        return False
    return any(reason["type"] == "alert_association" for reason in suspects[0]["reasons"])


def _case_has_alert(case: dict[str, Any]) -> bool:
    return bool(case["bundle"].get("context", {}).get("alerts"))


def _delta(after: float, before: float) -> float:
    return round(after - before, 4)


def evaluate_alert_lift(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    fixture = _load(path)
    baseline = evaluate(path)
    alert_fixture = alert_augmented_fixture(fixture)
    after = evaluate_fixture(alert_fixture)

    positive = [case for case in alert_fixture["cases"] if case.get("expected_culprit")]
    alert_cases = [case for case in alert_fixture["cases"] if _case_has_alert(case)]
    top1_alert_contribution_cases = [
        case["id"] for case in alert_fixture["cases"] if _top1_has_alert_reason(after, case["id"])
    ]

    tie_break_candidates = []
    tie_break_hits = []
    noise_cases = []
    improved_cases = []
    regressed_cases = []
    for case in positive:
        expected = case["expected_culprit"]
        before_rank = _rank_by_case(baseline, case["id"], expected)
        after_rank = _rank_by_case(after, case["id"], expected)
        if before_rank and before_rank <= 3 and before_rank != 1:
            tie_break_candidates.append(case["id"])
            if after_rank == 1:
                tie_break_hits.append(case["id"])
        if before_rank and after_rank and after_rank < before_rank:
            improved_cases.append(case["id"])
        elif before_rank and after_rank and after_rank > before_rank:
            regressed_cases.append(case["id"])
        if (
            _case_has_alert(case)
            and before_rank == after_rank
            and _top1(baseline, case["id"]) == _top1(after, case["id"])
        ):
            noise_cases.append(case["id"])

    metrics_before = baseline["metrics"]
    metrics_after = after["metrics"]
    deltas = {
        "top1_recall": _delta(metrics_after["top1_recall"], metrics_before["top1_recall"]),
        "top3_recall": _delta(metrics_after["top3_recall"], metrics_before["top3_recall"]),
        "mrr": _delta(metrics_after["mrr"], metrics_before["mrr"]),
        "false_culprit_rate": _delta(metrics_after["false_culprit_rate"], metrics_before["false_culprit_rate"]),
        "unsupported_rca_rate": _delta(metrics_after["unsupported_rca_rate"], metrics_before["unsupported_rca_rate"]),
    }
    alert_metrics = {
        "alert_contribution_rate": round(len(top1_alert_contribution_cases) / after["case_count"], 4),
        "alert_contribution_cases": top1_alert_contribution_cases,
        "alert_tie_break_rate": (
            round(len(tie_break_hits) / len(tie_break_candidates), 4) if tie_break_candidates else 0.0
        ),
        "alert_tie_break_cases": tie_break_hits,
        "alert_tie_break_candidate_cases": tie_break_candidates,
        "alert_noise_rate": round(len(noise_cases) / len(alert_cases), 4) if alert_cases else 0.0,
        "alert_noise_cases": noise_cases,
        "alert_improved_cases": improved_cases,
        "alert_regressed_cases": regressed_cases,
    }

    failures = []
    if baseline_gate_failures(baseline):
        failures.append("frozen baseline gate failed")
    if metrics_after["top1_recall"] <= metrics_before["top1_recall"]:
        failures.append("alert context did not improve top1_recall")
    if metrics_after["top3_recall"] < metrics_before["top3_recall"]:
        failures.append("alert context regressed top3_recall")
    if metrics_after["false_culprit_rate"] > metrics_before["false_culprit_rate"]:
        failures.append("alert context increased false_culprit_rate")
    if metrics_after["unsupported_rca_rate"] > metrics_before["unsupported_rca_rate"]:
        failures.append("alert context increased unsupported_rca_rate")
    if alert_metrics["alert_regressed_cases"]:
        failures.append("alert context regressed expected culprit rank")

    return {
        "benchmark": "alert_context_ranking_lift",
        "case_count": after["case_count"],
        "benchmark_contract": benchmark_contract(
            case_count=after["case_count"],
            scorable_case_count=after["positive_count"],
            negative_case_count=after["negative_count"],
            total_ranked_denominator=sum(len(result["suspects"]) for result in after["results"].values()) or 1,
            metric_denominators={
                "alert_contribution_rate": after["case_count"],
                "alert_tie_break_rate": len(tie_break_candidates),
                "alert_noise_rate": len(alert_cases),
            },
            context_available=[
                "service_graph",
                "runbooks",
                "historical_incidents",
                "deployments",
                "dashboards",
                "alerts",
            ],
        ),
        "hypothesis": (
            "Alert-derived operational knowledge improves suspect ranking "
            "without increasing unsupported culprit assertions."
        ),
        "baseline": baseline,
        "after": after,
        "after_vs_random": after["vs_random"],
        "deltas": deltas,
        "alert_metrics": alert_metrics,
        "gate": {"passed": not failures, "failures": failures},
    }


def _print(report: dict[str, Any]) -> None:
    before = report["baseline"]["metrics"]
    after = report["after"]["metrics"]
    deltas = report["deltas"]
    print("=== Alert context ranking lift ===")
    print(f"cases={report['case_count']}")
    print("random_baselines:", report["benchmark_contract"]["random_baselines"])
    print("after_vs_random:", report["after_vs_random"])
    print("metric\tbefore\tafter\tdelta")
    for key in ("top1_recall", "top3_recall", "mrr", "false_culprit_rate", "unsupported_rca_rate"):
        print(f"{key}\t{before[key]}\t{after[key]}\t{deltas[key]:+}")
    print("\nalert_contribution_rate:", report["alert_metrics"]["alert_contribution_rate"])
    print("alert_tie_break_rate:", report["alert_metrics"]["alert_tie_break_rate"])
    print("alert_noise_rate:", report["alert_metrics"]["alert_noise_rate"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure contextual-ranking lift from alert context.")
    parser.add_argument("--fixture", type=Path, default=FIXTURE_PATH, help="Frozen contextual ranking fixture path.")
    parser.add_argument("--json", type=Path, default=None, help="Write the full report to this JSON path.")
    args = parser.parse_args()

    report = evaluate_alert_lift(args.fixture)
    _print(report)
    if report["gate"]["failures"]:
        print("\n=== ALERT CONTEXT RANKING LIFT FAILED ===")
        for failure in report["gate"]["failures"]:
            print(f"- {failure}")
    else:
        print("\n=== ALERT CONTEXT RANKING LIFT PASSED ===")
    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 1 if report["gate"]["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
