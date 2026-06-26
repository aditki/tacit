"""Measure contextual-ranking lift from incident history Operational IR."""

from __future__ import annotations

import argparse
import copy
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from tacit.artifact_learning import IncidentExtractor, artifact_from_text
from tests.eval.alert_context_ranking_harness import alert_augmented_fixture
from tests.eval.contextual_culprit_ranking_harness import FIXTURE_PATH, evaluate_fixture
from tests.eval.ranking_benchmark_contract import benchmark_contract
from tests.eval.runbook_context_ranking_harness import FROZEN_BASELINE_NAME, runbook_augmented_fixture

INCIDENT_TIE_BREAK_IDS = {
    "contextual_top3_only_accounts",
    "contextual_top3_only_refunds",
}

IGNORED_CAUSAL_CLAIM_IDS = {
    "contextual_top3_only_ledger",
}


def _load(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def _incident_result(case: dict[str, Any], body: str):
    artifact = artifact_from_text(
        artifact_type="incident",
        title=f"Historical investigation for {case['id']}",
        body_text=body,
        external_id=f"incident-history:{case['id']}",
        source_vendor="fixture",
        source_instance="incident_context_ranking",
    )
    return IncidentExtractor().extract(artifact)


def _append_incident_ir(case: dict[str, Any], result) -> int:
    context = case["bundle"].setdefault("context", {})
    ignored_causal_claims = sum(1 for warning in result.warnings if warning.startswith("ignored_causal_claim:"))

    for requirement in result.evidence_requirements:
        row = asdict(requirement)
        row["source"] = "incident_history_overlay"
        row["target_entity"] = row.get("target_entity") or ""
        row["signal_hint"] = row.get("signal_hint") or ""
        row["query_hint"] = row.get("query_hint") or ""
        context.setdefault("evidence_requirements", []).append(row)
    for hint in result.dependency_hints:
        row = asdict(hint)
        row["source"] = "incident_history_overlay"
        context.setdefault("dependency_hints", []).append(row)
    for hint in result.ownership_hints:
        row = asdict(hint)
        row["source"] = "incident_history_overlay"
        context.setdefault("ownership_hints", []).append(row)

    return ignored_causal_claims


def incident_augmented_fixture(fixture: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    augmented = copy.deepcopy(fixture)
    augmented["benchmark"] = f"{fixture['benchmark']} + incident history artifact IR"
    augmented["version"] = f"{fixture['version']}-incident-context"
    ignored_by_case: dict[str, int] = {}

    for case in augmented["cases"]:
        expected = case.get("expected_culprit")
        if case["id"] in INCIDENT_TIE_BREAK_IDS and expected:
            result = _incident_result(
                case,
                "\n".join(
                    [
                        "## Observed Evidence",
                        f"- observed `{expected}` latency pattern during a similar investigation",
                        "## Investigation References",
                        "- See prior dashboard and runbook notes",
                    ]
                ),
            )
            ignored_by_case[case["id"]] = _append_incident_ir(case, result)
        elif case["id"] in IGNORED_CAUSAL_CLAIM_IDS:
            result = _incident_result(
                case,
                "\n".join(
                    [
                        "## Investigation References",
                        "- Prior note claimed: Root cause was Redis",
                    ]
                ),
            )
            ignored_by_case[case["id"]] = _append_incident_ir(case, result)
    return augmented, ignored_by_case


def _rank(report: dict[str, Any], case_id: str, entity: str | None) -> int | None:
    if not entity:
        return None
    for suspect in report["results"][case_id]["suspects"]:
        if suspect["entity"] == entity:
            return int(suspect["rank"])
    return None


def _top1(report: dict[str, Any], case_id: str) -> str | None:
    suspects = report["results"][case_id]["suspects"]
    return suspects[0]["entity"] if suspects else None


def _top1_has_incident_reason(report: dict[str, Any], case_id: str) -> bool:
    suspects = report["results"][case_id]["suspects"]
    if not suspects:
        return False
    return any(
        reason["type"] in {"incident_observed_evidence", "dependency_hint", "ownership_context"}
        and reason["source"] == "incident_history_overlay"
        for reason in suspects[0]["reasons"]
    )


def _delta(after: float, before: float) -> float:
    return round(after - before, 4)


def evaluate_incident_lift(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    frozen = _load(path)
    baseline_fixture = runbook_augmented_fixture(alert_augmented_fixture(frozen))
    incident_fixture, ignored_by_case = incident_augmented_fixture(baseline_fixture)
    before = evaluate_fixture(baseline_fixture)
    after = evaluate_fixture(incident_fixture)
    positive = [case for case in incident_fixture["cases"] if case.get("expected_culprit")]

    tie_break_candidates = []
    tie_break_hits = []
    contribution_cases = []
    noise_cases = []
    regressed_cases = []
    improved_cases = []
    for case in positive:
        expected = case["expected_culprit"]
        before_rank = _rank(before, case["id"], expected)
        after_rank = _rank(after, case["id"], expected)
        if before_rank and before_rank <= 3 and before_rank != 1:
            tie_break_candidates.append(case["id"])
            if after_rank == 1:
                tie_break_hits.append(case["id"])
        if before_rank and after_rank and after_rank < before_rank:
            improved_cases.append(case["id"])
        elif before_rank and after_rank and after_rank > before_rank:
            regressed_cases.append(case["id"])
        if _top1_has_incident_reason(after, case["id"]):
            contribution_cases.append(case["id"])
        if before_rank == after_rank and _top1(before, case["id"]) == _top1(after, case["id"]):
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
    incident_metrics = {
        "incident_contribution_rate": round(len(contribution_cases) / after["case_count"], 4),
        "incident_contribution_cases": contribution_cases,
        "incident_tie_break_rate": (
            round(len(tie_break_hits) / len(tie_break_candidates), 4) if tie_break_candidates else 0.0
        ),
        "incident_tie_break_cases": tie_break_hits,
        "incident_tie_break_candidate_cases": tie_break_candidates,
        "incident_noise_rate": round(len(noise_cases) / after["case_count"], 4) if after["case_count"] else 0.0,
        "incident_noise_cases": noise_cases,
        "incident_improved_cases": improved_cases,
        "incident_regressed_cases": regressed_cases,
        "ignored_causal_claim_count": sum(ignored_by_case.values()),
        "ignored_causal_claim_cases": [case_id for case_id, count in ignored_by_case.items() if count],
    }

    ignored_case_top1 = _top1(after, "contextual_top3_only_ledger")
    failures = []
    if after_metrics["top1_recall"] <= before_metrics["top1_recall"]:
        failures.append("incident history did not improve top1_recall")
    if after_metrics["mrr"] <= before_metrics["mrr"]:
        failures.append("incident history did not improve mrr")
    if after_metrics["top3_recall"] < before_metrics["top3_recall"]:
        failures.append("incident history regressed top3_recall")
    if after_metrics["false_culprit_rate"] > before_metrics["false_culprit_rate"]:
        failures.append("incident history increased false_culprit_rate")
    if after_metrics["unsupported_rca_rate"] > before_metrics["unsupported_rca_rate"]:
        failures.append("incident history increased unsupported_rca_rate")
    if incident_metrics["incident_regressed_cases"]:
        failures.append("incident history regressed expected culprit rank")
    if incident_metrics["ignored_causal_claim_count"] <= 0:
        failures.append("incident benchmark did not exercise ignored causal claims")
    if ignored_case_top1 == "Redis":
        failures.append("ignored causal claim promoted Redis")

    return {
        "benchmark": "incident_context_ranking_lift",
        "baseline_name": FROZEN_BASELINE_NAME,
        "case_count": after["case_count"],
        "benchmark_contract": benchmark_contract(
            case_count=after["case_count"],
            scorable_case_count=after["positive_count"],
            negative_case_count=after["negative_count"],
            total_ranked_denominator=sum(len(result["suspects"]) for result in after["results"].values()) or 1,
            metric_denominators={
                "incident_contribution_rate": after["case_count"],
                "incident_tie_break_rate": len(tie_break_candidates),
                "incident_noise_rate": after["case_count"],
                "ignored_causal_claim_count": sum(1 for count in ignored_by_case.values() if count),
            },
            context_available=[
                "service_graph",
                "runbooks",
                "historical_incidents",
                "deployments",
                "dashboards",
                "alerts",
                "incidents",
            ],
        ),
        "baseline": before,
        "after": after,
        "after_vs_random": after["vs_random"],
        "deltas": deltas,
        "incident_metrics": incident_metrics,
        "critical_regression": {
            "case": "contextual_top3_only_ledger",
            "ignored_claim": "Root cause was Redis",
            "top1_after": ignored_case_top1,
            "passed": ignored_case_top1 != "Redis",
        },
        "gate": {"passed": not failures, "failures": failures},
    }


def _print(report: dict[str, Any]) -> None:
    before = report["baseline"]["metrics"]
    after = report["after"]["metrics"]
    deltas = report["deltas"]
    print("=== Incident history ranking lift ===")
    print(f"baseline={report['baseline_name']}")
    print(f"cases={report['case_count']}")
    print("random_baselines:", report["benchmark_contract"]["random_baselines"])
    print("after_vs_random:", report["after_vs_random"])
    print("metric\tbefore\tafter\tdelta")
    for key in ("top1_recall", "top3_recall", "mrr", "false_culprit_rate", "unsupported_rca_rate"):
        print(f"{key}\t{before[key]}\t{after[key]}\t{deltas[key]:+}")
    print("\nincident_contribution_rate:", report["incident_metrics"]["incident_contribution_rate"])
    print("incident_tie_break_rate:", report["incident_metrics"]["incident_tie_break_rate"])
    print("incident_noise_rate:", report["incident_metrics"]["incident_noise_rate"])
    print("ignored_causal_claim_count:", report["incident_metrics"]["ignored_causal_claim_count"])
    print("critical_regression_passed:", report["critical_regression"]["passed"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure contextual-ranking lift from incident history IR.")
    parser.add_argument("--fixture", type=Path, default=FIXTURE_PATH, help="Frozen contextual ranking fixture path.")
    parser.add_argument("--json", type=Path, default=None, help="Write the full report to this JSON path.")
    args = parser.parse_args()
    report = evaluate_incident_lift(args.fixture)
    _print(report)
    if report["gate"]["failures"]:
        print("\n=== INCIDENT HISTORY RANKING LIFT FAILED ===")
        for failure in report["gate"]["failures"]:
            print(f"- {failure}")
    else:
        print("\n=== INCIDENT HISTORY RANKING LIFT PASSED ===")
    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 1 if report["gate"]["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
