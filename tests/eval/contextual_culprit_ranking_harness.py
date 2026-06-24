"""Offline gate harness for synthetic contextual culprit ranking.

This benchmark deliberately starts after integrations have produced normalized
context. It evaluates only:

    ContextBundle + EvidenceObservations -> RankedSuspects

The gate is not an RCA benchmark. Any output that claims proof instead of
``suspect_not_proven`` fails the unsupported-RCA gate.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from tacit.contextual_culprit_ranking import CAUSAL_STATUS, ContextBundle, RankedSuspectsResult, rank_context_bundle

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "contextual_culprit_ranking.json"

MIN_TOP1_RECALL = 0.50
MIN_TOP3_RECALL = 0.80
MIN_MRR = 0.65
MAX_FALSE_CULPRIT_RATE = 0.0
MAX_UNSUPPORTED_RCA_RATE = 0.0
MIN_EVIDENCE_ATTRIBUTION = 1.0
MIN_NEGATIVE_CORRECTNESS = 0.90
MIN_ABSTENTION_ON_INSUFFICIENT = 0.80
MIN_CONTEXTUAL_TOP3_ONLY_RECALL = 1.0
MIN_CONTEXTUAL_TOP3_ONLY_NOT_TOP1 = 1.0


def _load(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def _result(case: dict[str, Any]) -> RankedSuspectsResult:
    bundle = ContextBundle.model_validate(case["bundle"])
    return rank_context_bundle(bundle)


def _top1(result: RankedSuspectsResult) -> str | None:
    return result.suspects[0].entity if result.suspects else None


def _rank_of(result: RankedSuspectsResult, expected: str) -> int | None:
    for suspect in result.suspects:
        if suspect.entity == expected:
            return suspect.rank
    return None


def _has_high_confidence_false_culprit(result: RankedSuspectsResult) -> bool:
    return any(suspect.score >= 0.5 for suspect in result.suspects)


def _has_attribution(result: RankedSuspectsResult) -> tuple[int, int]:
    total = len(result.suspects)
    attributed = 0
    for suspect in result.suspects:
        has_valid_reasons = all(reason.type and reason.source and reason.confidence >= 0 for reason in suspect.reasons)
        if suspect.reasons and has_valid_reasons:
            attributed += 1
    return attributed, total


def evaluate(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    fixture = _load(path)
    cases = fixture["cases"]
    results = {case["id"]: _result(case) for case in cases}

    positive = [case for case in cases if case.get("expected_culprit")]
    negative = [case for case in cases if case.get("negative_control") or case.get("expected_culprit") is None]
    insufficient = [case for case in cases if case.get("insufficient_evidence")]
    contextual_top3_only = [case for case in cases if case.get("contextual_top3_only")]

    top1_hits = 0
    top3_hits = 0
    reciprocal_rank_total = 0.0
    positive_rows: list[dict[str, Any]] = []
    for case in positive:
        expected = case["expected_culprit"]
        result = results[case["id"]]
        rank = _rank_of(result, expected)
        top1_hit = rank == 1
        top3_hit = rank is not None and rank <= 3
        top1_hits += int(top1_hit)
        top3_hits += int(top3_hit)
        reciprocal_rank_total += 1 / rank if rank else 0.0
        positive_rows.append(
            {
                "id": case["id"],
                "expected": expected,
                "top1": _top1(result),
                "rank": rank,
                "top1_hit": top1_hit,
                "top3_hit": top3_hit,
            }
        )

    false_culprits = [
        case["id"]
        for case in negative
        if _has_high_confidence_false_culprit(results[case["id"]]) or not results[case["id"]].abstained
    ]
    unsupported_rca = [
        {"case": case["id"], "entity": suspect.entity, "causal_status": suspect.causal_status}
        for case in cases
        for suspect in results[case["id"]].suspects
        if suspect.causal_status != CAUSAL_STATUS
    ]
    attributed = total_suspects = 0
    for result in results.values():
        result_attributed, result_total = _has_attribution(result)
        attributed += result_attributed
        total_suspects += result_total

    negative_correct = [
        case["id"]
        for case in negative
        if results[case["id"]].abstained and not _has_high_confidence_false_culprit(results[case["id"]])
    ]
    insufficient_abstentions = [case["id"] for case in insufficient if results[case["id"]].abstained]
    contextual_top3_hits = []
    contextual_not_top1_hits = []
    for case in contextual_top3_only:
        expected = case["expected_culprit"]
        rank = _rank_of(results[case["id"]], expected)
        if rank is not None and rank <= 3:
            contextual_top3_hits.append(case["id"])
        if rank is not None and rank != 1:
            contextual_not_top1_hits.append(case["id"])

    stability_failures = []
    for case in cases:
        repeated = _result(case)
        if repeated.model_dump(mode="json") != results[case["id"]].model_dump(mode="json"):
            stability_failures.append(case["id"])

    counterfactual_failures = []
    for case in cases:
        if not case.get("counterfactual_of") or not case.get("expect_top1_changes"):
            continue
        base = results[case["counterfactual_of"]]
        changed = results[case["id"]]
        if _top1(base) == _top1(changed):
            counterfactual_failures.append({"base": case["counterfactual_of"], "counterfactual": case["id"]})

    positive_count = len(positive)
    negative_count = len(negative)
    insufficient_count = len(insufficient)
    total_ranked = total_suspects or 1
    metrics = {
        "top1_recall": round(top1_hits / positive_count, 4) if positive_count else 1.0,
        "top3_recall": round(top3_hits / positive_count, 4) if positive_count else 1.0,
        "mrr": round(reciprocal_rank_total / positive_count, 4) if positive_count else 1.0,
        "false_culprit_rate": round(len(false_culprits) / negative_count, 4) if negative_count else 0.0,
        "unsupported_rca_rate": round(len(unsupported_rca) / total_ranked, 4),
        "evidence_attribution": round(attributed / total_ranked, 4),
        "negative_correctness": round(len(negative_correct) / negative_count, 4) if negative_count else 1.0,
        "abstention_on_insufficient": (
            round(len(insufficient_abstentions) / insufficient_count, 4) if insufficient_count else 1.0
        ),
        "contextual_top3_only_recall": (
            round(len(contextual_top3_hits) / len(contextual_top3_only), 4) if contextual_top3_only else 1.0
        ),
        "contextual_top3_only_not_top1": (
            round(len(contextual_not_top1_hits) / len(contextual_top3_only), 4) if contextual_top3_only else 1.0
        ),
        "stability": not stability_failures,
        "counterfactual_sensitivity": not counterfactual_failures,
    }

    return {
        "benchmark": fixture["benchmark"],
        "version": fixture["version"],
        "target_matrix_size": fixture["target_matrix_size"],
        "case_count": len(cases),
        "positive_count": positive_count,
        "negative_count": negative_count,
        "metrics": metrics,
        "positive_cases": positive_rows,
        "contextual_top3_only_cases": [case["id"] for case in contextual_top3_only],
        "false_culprits": false_culprits,
        "unsupported_rca": unsupported_rca,
        "stability_failures": stability_failures,
        "counterfactual_failures": counterfactual_failures,
        "results": {
            case_id: result.model_dump(mode="json")
            for case_id, result in sorted(results.items(), key=lambda item: item[0])
        },
    }


def gate_failures(report: dict[str, Any]) -> list[str]:
    metrics = report["metrics"]
    failures: list[str] = []
    for name, threshold in (
        ("top1_recall", MIN_TOP1_RECALL),
        ("top3_recall", MIN_TOP3_RECALL),
        ("mrr", MIN_MRR),
        ("evidence_attribution", MIN_EVIDENCE_ATTRIBUTION),
        ("negative_correctness", MIN_NEGATIVE_CORRECTNESS),
        ("abstention_on_insufficient", MIN_ABSTENTION_ON_INSUFFICIENT),
        ("contextual_top3_only_recall", MIN_CONTEXTUAL_TOP3_ONLY_RECALL),
        ("contextual_top3_only_not_top1", MIN_CONTEXTUAL_TOP3_ONLY_NOT_TOP1),
    ):
        if metrics[name] < threshold:
            failures.append(f"{name} {metrics[name]:.4f} < {threshold:.2f}")
    for name, threshold in (
        ("false_culprit_rate", MAX_FALSE_CULPRIT_RATE),
        ("unsupported_rca_rate", MAX_UNSUPPORTED_RCA_RATE),
    ):
        if metrics[name] > threshold:
            failures.append(f"{name} {metrics[name]:.4f} > {threshold:.2f}")
    if not metrics["stability"]:
        failures.append("stability failed")
    if not metrics["counterfactual_sensitivity"]:
        failures.append("counterfactual_sensitivity failed")
    return failures


def _print(report: dict[str, Any]) -> None:
    metrics = report["metrics"]
    print("=== Contextual culprit ranking gate ===")
    print(f"cases={report['case_count']} target_matrix_size={report['target_matrix_size']}")
    for key, value in metrics.items():
        print(f"{key}: {value}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic contextual culprit ranking benchmark.")
    parser.add_argument("--fixture", type=Path, default=FIXTURE_PATH, help="Fixture JSON path.")
    parser.add_argument("--json", type=Path, default=None, help="Write the full report to this JSON path.")
    args = parser.parse_args()

    report = evaluate(args.fixture)
    failures = gate_failures(report)
    report["gate"] = {"passed": not failures, "failures": failures}
    _print(report)
    if failures:
        print("\n=== CONTEXTUAL CULPRIT RANKING GATE FAILED ===")
        for failure in failures:
            print(f"- {failure}")
    else:
        print("\n=== CONTEXTUAL CULPRIT RANKING GATE PASSED ===")
    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
