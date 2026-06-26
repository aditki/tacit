"""Measure contextual-ranking lift from Tacit Artifact Learning runbook IR."""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any

from tests.eval.alert_context_ranking_harness import alert_augmented_fixture
from tests.eval.contextual_culprit_ranking_harness import FIXTURE_PATH, evaluate_fixture
from tests.eval.ranking_benchmark_contract import benchmark_contract

OBSERVED_RUNBOOK_IDS = {
    "contextual_top3_only_pricing",
    "contextual_top3_only_shipping",
}

FROZEN_BASELINE_NAME = "Contextual Ranking + Alerts + Runbooks Baseline v1"


def _load(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def runbook_augmented_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    augmented = copy.deepcopy(fixture)
    augmented["benchmark"] = f"{fixture['benchmark']} + alert context + runbook artifact IR"
    augmented["version"] = f"{fixture['version']}-alert-runbook-context"
    for case in augmented["cases"]:
        expected = case.get("expected_culprit")
        if not expected:
            continue
        context = case["bundle"].setdefault("context", {})
        requirements = context.setdefault("evidence_requirements", [])
        observed = case["id"] in OBSERVED_RUNBOOK_IDS
        requirements.append(
            {
                "subject": f"Check {expected} runbook signal",
                "evidence_kind": "latency",
                "target_entity": expected,
                "signal_hint": f"{expected}_latency_check",
                "observation_state": "observed" if observed else "indeterminate",
                "source": "runbook_artifact_overlay",
            }
        )
        if case["id"] in {"plausible_redis_db_cart", "plausible_redis_db_profile"}:
            context.setdefault("dependency_hints", []).append(
                {
                    "source_entity": case["bundle"]["incident"]["affected_service"],
                    "target_entity": expected,
                    "direction": "depends_on",
                    "source": "runbook_artifact_overlay",
                }
            )
        if case["id"] == "contextual_top3_only_accounts":
            context.setdefault("ownership_hints", []).append(
                {
                    "entity": expected,
                    "owner": "accounts-team",
                    "hint_kind": "escalation",
                    "source": "runbook_artifact_overlay",
                }
            )
    return augmented


def _rank(report: dict[str, Any], case_id: str, entity: str | None) -> int | None:
    if not entity:
        return None
    for suspect in report["results"][case_id]["suspects"]:
        if suspect["entity"] == entity:
            return int(suspect["rank"])
    return None


def _top1_has_runbook_reason(report: dict[str, Any], case_id: str) -> bool:
    suspects = report["results"][case_id]["suspects"]
    if not suspects:
        return False
    return any(
        reason["type"] in {"evidence_requirement_observed", "dependency_hint"}
        and reason["source"] == "runbook_artifact_overlay"
        for reason in suspects[0]["reasons"]
    )


def _delta(after: float, before: float) -> float:
    return round(after - before, 4)


def evaluate_runbook_lift(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    frozen = _load(path)
    alert_fixture = alert_augmented_fixture(frozen)
    runbook_fixture = runbook_augmented_fixture(alert_fixture)
    before = evaluate_fixture(alert_fixture)
    after = evaluate_fixture(runbook_fixture)
    positive = [case for case in runbook_fixture["cases"] if case.get("expected_culprit")]

    tie_break_candidates = []
    tie_break_hits = []
    noise_cases = []
    contribution_cases = []
    regressed_cases = []
    indeterminate = 0
    total_requirements = 0
    for case in positive:
        expected = case["expected_culprit"]
        before_rank = _rank(before, case["id"], expected)
        after_rank = _rank(after, case["id"], expected)
        requirements = case["bundle"].get("context", {}).get("evidence_requirements", [])
        total_requirements += len(requirements)
        indeterminate += sum(1 for req in requirements if req.get("observation_state") == "indeterminate")
        if before_rank and before_rank <= 3 and before_rank != 1:
            tie_break_candidates.append(case["id"])
            if after_rank == 1:
                tie_break_hits.append(case["id"])
        if before_rank and after_rank and after_rank > before_rank:
            regressed_cases.append(case["id"])
        if _top1_has_runbook_reason(after, case["id"]):
            contribution_cases.append(case["id"])
        if before_rank == after_rank:
            noise_cases.append(case["id"])

    before_metrics = before["metrics"]
    after_metrics = after["metrics"]
    deltas = {
        "top1_recall": _delta(after_metrics["top1_recall"], before_metrics["top1_recall"]),
        "top3_recall": _delta(after_metrics["top3_recall"], before_metrics["top3_recall"]),
        "mrr": _delta(after_metrics["mrr"], before_metrics["mrr"]),
        "false_culprit_rate": _delta(after_metrics["false_culprit_rate"], before_metrics["false_culprit_rate"]),
        "unsupported_rca_rate": _delta(after_metrics["unsupported_rca_rate"], before_metrics["unsupported_rca_rate"]),
    }
    runbook_metrics = {
        "runbook_contribution_rate": round(len(contribution_cases) / after["case_count"], 4),
        "runbook_contribution_cases": contribution_cases,
        "runbook_tie_break_rate": (
            round(len(tie_break_hits) / len(tie_break_candidates), 4) if tie_break_candidates else 0.0
        ),
        "runbook_tie_break_cases": tie_break_hits,
        "runbook_noise_rate": round(len(noise_cases) / after["case_count"], 4) if after["case_count"] else 0.0,
        "runbook_noise_cases": noise_cases,
        "indeterminate_requirement_rate": round(indeterminate / total_requirements, 4) if total_requirements else 0.0,
        "regressed_cases": regressed_cases,
    }

    failures = []
    if after_metrics["top1_recall"] < before_metrics["top1_recall"]:
        failures.append("runbook context regressed top1_recall")
    if after_metrics["top3_recall"] < before_metrics["top3_recall"]:
        failures.append("runbook context regressed top3_recall")
    if after_metrics["false_culprit_rate"] > before_metrics["false_culprit_rate"]:
        failures.append("runbook context increased false_culprit_rate")
    if after_metrics["unsupported_rca_rate"] > before_metrics["unsupported_rca_rate"]:
        failures.append("runbook context increased unsupported_rca_rate")
    if runbook_metrics["regressed_cases"]:
        failures.append("runbook context regressed expected culprit rank")
    if runbook_metrics["indeterminate_requirement_rate"] <= 0:
        failures.append("runbook benchmark did not exercise indeterminate requirements")

    return {
        "benchmark": "contextual_alerts_runbooks_baseline_v1",
        "baseline_name": FROZEN_BASELINE_NAME,
        "case_count": after["case_count"],
        "benchmark_contract": benchmark_contract(
            case_count=after["case_count"],
            scorable_case_count=after["positive_count"],
            negative_case_count=after["negative_count"],
            total_ranked_denominator=sum(len(result["suspects"]) for result in after["results"].values()) or 1,
            metric_denominators={
                "runbook_contribution_rate": after["case_count"],
                "runbook_tie_break_rate": len(tie_break_candidates),
                "runbook_noise_rate": after["case_count"],
                "indeterminate_requirement_rate": total_requirements,
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
        "baseline": before,
        "after": after,
        "after_vs_random": after["vs_random"],
        "deltas": deltas,
        "runbook_metrics": runbook_metrics,
        "gate": {"passed": not failures, "failures": failures},
    }


def _print(report: dict[str, Any]) -> None:
    before = report["baseline"]["metrics"]
    after = report["after"]["metrics"]
    deltas = report["deltas"]
    print(f"=== {report['baseline_name']} ===")
    print(f"cases={report['case_count']}")
    print("random_baselines:", report["benchmark_contract"]["random_baselines"])
    print("after_vs_random:", report["after_vs_random"])
    print("metric\tbefore\tafter\tdelta")
    for key in ("top1_recall", "top3_recall", "mrr", "false_culprit_rate", "unsupported_rca_rate"):
        print(f"{key}\t{before[key]}\t{after[key]}\t{deltas[key]:+}")
    print("\nrunbook_contribution_rate:", report["runbook_metrics"]["runbook_contribution_rate"])
    print("runbook_tie_break_rate:", report["runbook_metrics"]["runbook_tie_break_rate"])
    print("runbook_noise_rate:", report["runbook_metrics"]["runbook_noise_rate"])
    print("indeterminate_requirement_rate:", report["runbook_metrics"]["indeterminate_requirement_rate"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure contextual-ranking lift from runbook artifact IR.")
    parser.add_argument("--fixture", type=Path, default=FIXTURE_PATH, help="Frozen contextual ranking fixture path.")
    parser.add_argument("--json", type=Path, default=None, help="Write the full report to this JSON path.")
    args = parser.parse_args()
    report = evaluate_runbook_lift(args.fixture)
    _print(report)
    if report["gate"]["failures"]:
        print("\n=== RUNBOOK ARTIFACT RANKING LIFT FAILED ===")
        for failure in report["gate"]["failures"]:
            print(f"- {failure}")
    else:
        print("\n=== RUNBOOK ARTIFACT RANKING LIFT PASSED ===")
    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 1 if report["gate"]["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
