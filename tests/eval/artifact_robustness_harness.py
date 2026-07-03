"""Robustness gates for Operational IR artifact learning."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from tacit.artifact_learning import IncidentExtractor, artifact_from_text
from tacit.contextual_culprit_ranking import CAUSAL_STATUS, ContextBundle, rank_context_bundle
from tests.eval.alert_context_ranking_harness import alert_augmented_fixture
from tests.eval.contextual_culprit_ranking_harness import FIXTURE_PATH, evaluate_fixture
from tests.eval.incident_context_ranking_harness import incident_augmented_fixture
from tests.eval.runbook_context_ranking_harness import runbook_augmented_fixture

RCA_PHRASES = [
    "Root cause: Redis saturation",
    "Culprit: redis-cart",
    "Caused by redis cache pressure",
    "Caused when Redis memory filled up",
    "Primary issue: Redis CPU throttling",
    "Underlying issue: stale Redis schema",
    "Resolution: restarted Redis",
    "Fix: rollback checkout deploy",
    "Fix was to increase Redis connections",
    "Recovered after restarting Redis",
    "Rollback fixed the issue",
    "Schema drift introduced by PR 4275",
    "Postmortem conclusion: Redis was at fault",
    "Contributing factors: Redis timeout settings",
    "Lessons learned: Redis ownership was unclear",
    "Resolved by clearing Redis keys",
    "Remediated by scaling Redis",
    "Triggered by checkout deploy",
    "Regression from PR 4275",
    "Fault was in Redis",
    "Latency due to Redis saturation",
    "The issue was caused by Redis",
    "Primary issue was the cache tier",
    "Underlying issue was a bad index",
    "Postmortem conclusion was database lock contention",
    "Contributing factor was missing Redis alerting",
    "Lesson learned: runbook pointed at Redis",
    "Recovered after DB failover",
    "Rollback fixed customer impact",
    "Resolved by reverting checkout-api",
    "Fix: increase connection pool",
    "Resolution: disable the new writer path",
    "Root cause analysis: payment DB hot shard",
    "Culprit service: checkout-api",
    "Caused by schema drift",
    "Triggered by feature flag rollout",
    "Regression from deployment 2026.06.24",
    "Fault was traced to Redis",
    "Due to cache eviction storm",
    "Primary issue: lock contention",
    "Underlying issue: connection leak",
    "Postmortem conclusion: missing index",
    "Contributing factors: alert fatigue",
    "Lessons learned: owners were paged late",
    "Recovered after queue drain",
    "Rollback fixed checkout errors",
    "Schema drift introduced by migration 184",
    "Remediated by failover",
    "Resolved by restarting workers",
    "Fix was to disable writes",
]

LEGITIMATE_EVIDENCE_PHRASES = [
    "check Redis latency",
    "verify DB saturation",
    "look at checkout_latency_seconds",
    "inspect checkout-db connection pool",
    "observe redis_cache_misses_total",
    "confirmed checkout_errors_total spike",
    "detected payment_errors_total increase",
    "saw api_latency_seconds high",
    "evidence: db p95 latency high",
    "signal: checkout_request_duration_seconds elevated",
    "symptom: checkout latency high",
    "impact: checkout users saw errors",
]

NOISE_LEVELS = [0, 25, 50, 75, 90, 95]
ARTIFACT_COUNTS = [10, 50, 100, 500]


def _load(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def _combined_fixture(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    frozen = _load(path)
    baseline_fixture = runbook_augmented_fixture(alert_augmented_fixture(frozen))
    incident_fixture, _ignored = incident_augmented_fixture(baseline_fixture)
    return incident_fixture


def evaluate_rca_phrase_robustness() -> dict[str, Any]:
    failures = []
    warning_count = 0
    for idx, phrase in enumerate(RCA_PHRASES):
        artifact = artifact_from_text(
            artifact_type="incident",
            title=f"RCA phrase {idx}",
            body_text=f"## Investigation Notes\n- {phrase}",
            external_id=f"rca-phrase:{idx}",
            source_vendor="fixture",
        )
        result = IncidentExtractor().extract(artifact)
        emitted = (
            len(result.evidence_requirements)
            + len(result.ownership_hints)
            + len(result.dependency_hints)
            + len(result.signal_mapping_candidates)
        )
        warnings = [warning for warning in result.warnings if warning.startswith("ignored_causal_claim:")]
        warning_count += len(warnings)
        if emitted:
            failures.append({"phrase": phrase, "reason": "emitted_operational_ir", "emitted": emitted})
        if not warnings:
            failures.append({"phrase": phrase, "reason": "missing_ignored_causal_claim_warning"})

    return {
        "phrases": len(RCA_PHRASES),
        "phrase_construction_note": (
            "Handwritten adversarial phrase corpus; suppression recall is paired with a separate "
            "legitimate-evidence precision corpus to avoid overfitting to causal templates."
        ),
        "ignored_causal_claim_count": warning_count,
        "failures": failures,
        "passed": not failures,
    }


def evaluate_rca_precision() -> dict[str, Any]:
    failures = []
    evidence_count = 0
    false_positive_suppressions = 0
    for idx, phrase in enumerate(LEGITIMATE_EVIDENCE_PHRASES):
        artifact = artifact_from_text(
            artifact_type="incident",
            title=f"Legitimate evidence {idx}",
            body_text=f"## Observed Evidence\n- {phrase}",
            external_id=f"legitimate-evidence:{idx}",
            source_vendor="fixture",
        )
        result = IncidentExtractor().extract(artifact)
        warnings = [warning for warning in result.warnings if warning.startswith("ignored_causal_claim:")]
        false_positive_suppressions += len(warnings)
        evidence_count += len(result.evidence_requirements)
        if warnings:
            failures.append({"phrase": phrase, "reason": "false_positive_causal_suppression"})
        if not result.evidence_requirements:
            failures.append({"phrase": phrase, "reason": "missing_evidence_requirement"})

    return {
        "phrases": len(LEGITIMATE_EVIDENCE_PHRASES),
        "evidence_requirement_count": evidence_count,
        "false_positive_suppression_count": false_positive_suppressions,
        "failures": failures,
        "passed": not failures,
    }


def _inject_noise(fixture: dict[str, Any], *, noise_count: int) -> dict[str, Any]:
    noised = copy.deepcopy(fixture)
    for case in noised["cases"]:
        context = case["bundle"].setdefault("context", {})
        affected = case["bundle"]["incident"]["affected_service"]
        for idx in range(noise_count):
            noise_entity = f"{affected}-noise-{idx}"
            context.setdefault("alerts", []).append(
                {
                    "entity": noise_entity,
                    "signals": [f"noise_metric_{idx}_total"],
                    "severity": "info",
                    "enabled": True,
                    "stale": True,
                    "source": "noise_injection",
                }
            )
            context.setdefault("dependency_hints", []).append(
                {
                    "source_entity": affected,
                    "target_entity": noise_entity,
                    "direction": "depends_on",
                    "source": "noise_injection",
                }
            )
            context.setdefault("evidence_requirements", []).append(
                {
                    "subject": f"observed noise_metric_{idx}_total",
                    "evidence_kind": "errors",
                    "target_entity": noise_entity,
                    "signal_hint": f"noise_metric_{idx}_total",
                    "observation_state": "indeterminate",
                    "source": "noise_injection",
                }
            )
    return noised


def evaluate_noise_injection(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    base_fixture = _combined_fixture(path)
    baseline = evaluate_fixture(base_fixture)
    rows = []
    failures = []
    for artifact_count in ARTIFACT_COUNTS:
        for noise_level in NOISE_LEVELS:
            noise_count = int(artifact_count * noise_level / 100)
            report = evaluate_fixture(_inject_noise(base_fixture, noise_count=noise_count))
            metrics = report["metrics"]
            noise_suspect_count = sum(
                1
                for result in report["results"].values()
                for suspect in result["suspects"]
                if any(reason["source"] == "noise_injection" for reason in suspect["reasons"])
            )
            row = {
                "artifact_count": artifact_count,
                "noise_level": noise_level,
                "noise_rows_per_case": noise_count,
                "noise_suspect_count": noise_suspect_count,
                "top1_recall": metrics["top1_recall"],
                "mrr": metrics["mrr"],
                "false_culprit_rate": metrics["false_culprit_rate"],
                "unsupported_rca_rate": metrics["unsupported_rca_rate"],
                "top1_delta": round(metrics["top1_recall"] - baseline["metrics"]["top1_recall"], 4),
                "mrr_delta": round(metrics["mrr"] - baseline["metrics"]["mrr"], 4),
            }
            rows.append(row)
            if metrics["top1_recall"] < baseline["metrics"]["top1_recall"]:
                failures.append({**row, "reason": "top1_regressed"})
            if metrics["mrr"] < baseline["metrics"]["mrr"]:
                failures.append({**row, "reason": "mrr_regressed"})
            if metrics["false_culprit_rate"] > baseline["metrics"]["false_culprit_rate"]:
                failures.append({**row, "reason": "false_culprit_regressed"})
            if metrics["unsupported_rca_rate"] > baseline["metrics"]["unsupported_rca_rate"]:
                failures.append({**row, "reason": "unsupported_rca_regressed"})
            if noise_count and noise_suspect_count == 0:
                failures.append({**row, "reason": "noise_did_not_reach_ranker"})

    return {
        "baseline_metrics": baseline["metrics"],
        "rows": rows,
        "failures": failures,
        "passed": not failures,
    }


def evaluate_contradictory_artifacts() -> dict[str, Any]:
    bundle = ContextBundle.model_validate(
        {
            "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
            "context": {
                "services": [{"name": "checkout-api", "depends_on": ["checkout-db", "redis-cart"]}],
                "runbook_hints": [{"symptom": "checkout latency", "suspects": ["redis-cart"]}],
                "alerts": [
                    {
                        "entity": "checkout-api",
                        "signals": ["checkout api latency alert"],
                        "severity": "critical",
                        "source": "contradictory_alert",
                    }
                ],
                "evidence_requirements": [
                    {
                        "subject": "observed checkout-db latency in prior investigation",
                        "evidence_kind": "latency",
                        "target_entity": "checkout-db",
                        "signal_hint": "checkout_db_latency",
                        "observation_state": "observed",
                        "source": "incident_history_overlay",
                    }
                ],
            },
            "evidence": {"observations": []},
        }
    )
    result = rank_context_bundle(bundle)
    entities = [suspect.entity for suspect in result.suspects]
    failures = []
    if not {"checkout-api", "checkout-db", "redis-cart"}.issubset(set(entities)):
        failures.append("missing_plausible_contradictory_suspects")
    if any(suspect.causal_status != CAUSAL_STATUS for suspect in result.suspects):
        failures.append("asserted_rca_from_contradictory_artifacts")
    if not result.abstained:
        failures.append("did_not_abstain_without_runtime_proof")

    return {
        "suspects": [suspect.model_dump(mode="json") for suspect in result.suspects],
        "failures": failures,
        "passed": not failures,
    }


def evaluate_artifact_robustness(path: Path = FIXTURE_PATH) -> dict[str, Any]:
    rca = evaluate_rca_phrase_robustness()
    rca_precision = evaluate_rca_precision()
    noise = evaluate_noise_injection(path)
    contradictory = evaluate_contradictory_artifacts()
    failures = []
    if not rca["passed"]:
        failures.append("rca_phrase_robustness_failed")
    if not rca_precision["passed"]:
        failures.append("rca_precision_failed")
    if not noise["passed"]:
        failures.append("noise_injection_failed")
    if not contradictory["passed"]:
        failures.append("contradictory_artifacts_failed")
    return {
        "benchmark": "artifact_learning_robustness",
        "fixture_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "rca_phrase_robustness": rca,
        "rca_precision": rca_precision,
        "noise_injection": noise,
        "contradictory_artifacts": contradictory,
        "gate": {"passed": not failures, "failures": failures},
    }


def _print(report: dict[str, Any]) -> None:
    print("=== Artifact learning robustness gate ===")
    print("rca_phrases:", report["rca_phrase_robustness"]["phrases"])
    print("ignored_causal_claim_count:", report["rca_phrase_robustness"]["ignored_causal_claim_count"])
    print("rca_precision_phrases:", report["rca_precision"]["phrases"])
    print("false_positive_suppression_count:", report["rca_precision"]["false_positive_suppression_count"])
    print("noise_rows:", len(report["noise_injection"]["rows"]))
    worst = min(row["mrr_delta"] for row in report["noise_injection"]["rows"])
    print("worst_noise_mrr_delta:", worst)
    print("contradictory_artifacts_passed:", report["contradictory_artifacts"]["passed"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Run artifact-learning robustness gates.")
    parser.add_argument("--fixture", type=Path, default=FIXTURE_PATH, help="Frozen contextual ranking fixture path.")
    parser.add_argument("--json", type=Path, default=None, help="Write the full report to this JSON path.")
    args = parser.parse_args()
    report = evaluate_artifact_robustness(args.fixture)
    _print(report)
    if report["gate"]["failures"]:
        print("\n=== ARTIFACT LEARNING ROBUSTNESS FAILED ===")
        for failure in report["gate"]["failures"]:
            print(f"- {failure}")
    else:
        print("\n=== ARTIFACT LEARNING ROBUSTNESS PASSED ===")
    if args.json:
        args.json.write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 1 if report["gate"]["failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
