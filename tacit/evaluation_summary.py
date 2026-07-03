"""Benchmark evaluation summaries for anonymous assessment bundles.

Assessment summaries explain what Tacit saw. Evaluation summaries explain why
a benchmark claim is auditable: what benchmark ran, against what contract,
with what denominators, and with what anonymized per-case outcomes.

The summary never contains raw prompts, raw artifact text, raw operational
names, or the anonymization mapping. Every string it emits must satisfy the
allowlist patterns enforced by :func:`validate_evaluation_summary`.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tacit import __version__

EVALUATION_VERSION = "1"
UNAVAILABLE_REASON = "No benchmark evaluation result found for this assessment."
DEFAULT_EVALUATION_RESULTS_DIR = Path("data/evaluations")
EVALUATION_RESULTS_DIR_ENV = "TACIT_EVALUATION_RESULTS_DIR"

MRR_UNTRUNCATED = "untruncated"
MRR_TOP_K_TRUNCATED = "top_k_truncated"
MRR_TRUNCATIONS = {MRR_UNTRUNCATED, MRR_TOP_K_TRUNCATED}

EVALUATION_MODES = {
    "archetype",
    "pipeline",
    "gate",
    "e2e",
    "gamma",
    "prompt_variation",
    "artifact_robustness",
}

_BENCHMARK_MODES = {
    "alert_context_ranking_lift": "gate",
    "artifact_learning_robustness": "artifact_robustness",
    "contextual_artifact_ranking": "gate",
    "contextual_alerts_runbooks_baseline_v1": "gate",
    "contextual_culprit_ranking": "gate",
    "incident_context_ranking_lift": "gate",
    "offline_gate": "gate",
    "prompt_variation": "prompt_variation",
    "gamma": "gamma",
    "artifact_robustness": "artifact_robustness",
}

_BENCHMARK_NAMES = set(_BENCHMARK_MODES)
_PUBLIC_VERSION_LABELS = {
    "artifact_robustness_v1",
    "gamma_diagnostic_v1",
    "offline_gate_v1",
    "prompt_variation_v1",
}
_CONTEXT_VALUES = {
    "alerts",
    "context",
    "dashboards",
    "deployments",
    "historical_incidents",
    "incidents",
    "runbooks",
    "service_graph",
}
_CASE_CLASSES = {"negative", "negative_noise", "positive", "scorable", "unknown"}
_STAGE_NAMES = {
    "intent",
    "ranking",
    "passed",
    "failed",
    "dropped",
    "indeterminate",
    "intent_parsed",
    "evidence_requirements_created",
    "evidence_resolved",
    "queries_built",
    "queries_validated",
    "panels_created",
}
_METRIC_NAMES = {
    "top1",
    "top3",
    "mrr",
    "false_culprit_rate",
    "unsupported_rca_rate",
    "evidence_attribution",
    "negative_correctness",
    "abstention_on_insufficient",
    "contextual_top3_only_recall",
    "contextual_top3_only_not_top1",
    "positive_useful_rate",
    "negative_correct_rate",
    "worst_prompt_rate",
    "top1_delta",
    "top3_delta",
    "mrr_delta",
    "false_culprit_delta",
    "unsupported_rca_delta",
    "alert_contribution_rate",
    "alert_tie_break_rate",
    "alert_noise_rate",
    "runbook_contribution_rate",
    "runbook_tie_break_rate",
    "runbook_noise_rate",
    "indeterminate_requirement_rate",
    "incident_contribution_rate",
    "incident_tie_break_rate",
    "incident_noise_rate",
    "ignored_causal_claim_count",
    "rca_suppression_recall",
    "rca_precision",
    "noise_scenarios",
    "noise_worst_mrr_delta",
    "contradictory_artifacts_passed",
    "canonical_evidence_recall",
    "prefixed_evidence_recall",
    "canonical_dashboard_rate",
    "prefixed_dashboard_rate",
    "raw_dashboard_rate",
    "false_culprit_rate_controls",
    "control_abstention_rate",
    "healthy_symptom_panel_recall",
    "semantic_precision",
    "semantic_recall",
    "semantic_coverage",
    "cold_resolution_recall",
    "learned_resolution_recall",
    "learned_selection_rate",
}
_RANDOM_BASELINE_KEYS = {
    "assumption",
    "mrr",
    "mrr_truncation",
    "per_case_computed",
    "top1",
    "top3",
}
_CONTRACT_KEYS = {
    "candidate_set_fixed",
    "candidate_set_size",
    "mrr_denominator",
    "mrr_truncation",
    "negative_cases",
    "negative_denominator",
    "negative_noise_cases",
    "positive_cases",
    "positive_denominator",
    "recall_denominator",
    "control_cases",
    "evidence_signal_denominator",
    "noise_scenarios",
    "rca_phrases",
    "rca_precision_phrases",
    "scorable_culprit_cases",
    "source_contribution_denominator",
    "top_k",
    "total_cases",
    "trials_per_prompt",
    "classification_datasets",
    "cold_resolution_datasets",
    "learned_resolution_datasets",
    "learned_selection_datasets",
    "labeled_signal_metrics",
    "critical_signals",
}
_REASON_CODES = {
    "false_culprit",
    "prompt_intent_mismatch",
    "top1_missed",
    "top3_missed",
    "unsupported_rca",
    "alert_context_regressed_expected_culprit_rank",
    "alert_context_did_not_improve_top1_recall",
    "alert_context_increased_false_culprit_rate",
    "alert_context_increased_unsupported_rca_rate",
    "alert_context_regressed_top3_recall",
    "all_arm_cache_hits_are_zero",
    "canonical_all_prompts_create_dashboard",
    "canonical_evidence_recall_meets_gate",
    "contradictory_artifacts_failed",
    "control_cache_hits_are_zero",
    "control_classes_are_balanced",
    "control_families_are_diverse",
    "control_sample_size_meets_gate",
    "control_scenarios_are_distinct",
    "evidence_absent_discovers_symptom",
    "evidence_absent_does_not_assert_resource_culprit",
    "false_culprit_regressed",
    "healthy_does_not_assert_culprit",
    "incident_benchmark_did_not_exercise_ignored_causal_claims",
    "incident_history_did_not_improve_mrr",
    "incident_history_did_not_improve_top1_recall",
    "incident_history_increased_false_culprit_rate",
    "incident_history_increased_unsupported_rca_rate",
    "incident_history_regressed_expected_culprit_rank",
    "incident_history_regressed_top3_recall",
    "ignored_causal_claim_promoted_redis",
    "mrr_regressed",
    "noise_did_not_reach_ranker",
    "noise_injection_failed",
    "prefix_only_preserves_mapping_coverage",
    "prefixed_all_prompts_bind_post_fix",
    "prefixed_evidence_recall_meets_gate",
    "prefixed_fails_binding_pre_fix",
    "raw_ambiguous_binding_abstains",
    "raw_fails_binding_pre_fix",
    "rca_phrase_robustness_failed",
    "rca_precision_failed",
    "runbook_benchmark_did_not_exercise_indeterminate_requirements",
    "runbook_context_increased_false_culprit_rate",
    "runbook_context_increased_unsupported_rca_rate",
    "runbook_context_regressed_expected_culprit_rank",
    "runbook_context_regressed_top1_recall",
    "runbook_context_regressed_top3_recall",
    "top1_regressed",
    "other",
    "semantic_precision_below_threshold",
    "semantic_recall_below_threshold",
    "semantic_coverage_below_threshold",
    "cold_resolution_below_threshold",
    "learned_resolution_below_threshold",
    "learned_selection_mismatch",
}
_SCHEMA_KEYS = {
    "actual_rank",
    "anonymous",
    "available",
    "benchmark_name",
    "benchmark_version",
    "case_class",
    "case_id",
    "checks",
    "context_available",
    "contract",
    "dataset_hash",
    "denominator",
    "evaluation_version",
    "evaluations",
    "expected_rank",
    "failure_reasons",
    "failure_stage",
    "failures",
    "false_culprit",
    "findings",
    "findings_count",
    "generated_at",
    "kind",
    "metrics",
    "mode",
    "mrr_contribution",
    "numerator",
    "passed_trials",
    "path",
    "per_case",
    "random_baselines",
    "rate",
    "raw_inputs_included",
    "reason",
    "reason_codes",
    "runner_version",
    "sample",
    "stage_counts",
    "top1_hit",
    "top3_hit",
    "total_trials",
    "truncation",
    "unsupported_rca",
    "value",
}
_DYNAMIC_KEYS_BY_PATH = {
    "$.evaluations[].contract": _CONTRACT_KEYS,
    "$.evaluations[].failure_reasons": _REASON_CODES,
    "$.evaluations[].metrics": _METRIC_NAMES,
    "$.evaluations[].random_baselines": _RANDOM_BASELINE_KEYS,
    "$.evaluations[].stage_counts": _STAGE_NAMES,
}

_SNAKE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ANON_ID_RE = re.compile(r"^[a-z][a-z0-9_]*_[0-9]{3,6}$")
_HASH_RE = re.compile(r"^sha256:[0-9a-f]{16,64}$")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")
_VERSION_RE = re.compile(
    r"^(?:" r"v?\d+(?:\.\d+){0,3}(?:[-+][0-9A-Za-z.]+)?" r"|version_hash_[0-9a-f]{16}" r"|[0-9a-f]{7,40}" r")$"
)
_FREE_TEXT_RE = re.compile(r"^[A-Za-z0-9 .,()\-]{1,200}$")

_FREE_TEXT_KEYS = {"reason", "assumption"}
_VERSION_KEYS = {"evaluation_version", "benchmark_version", "runner_version"}
_TIMESTAMP_KEYS = {"generated_at"}
_HASH_KEYS = {"dataset_hash"}
_MODE_KEYS = {"mode"}
_TRUNCATION_KEYS = {"mrr_truncation", "truncation"}
_ANON_ID_KEYS = {"case_id", "service_id", "alert_id", "runbook_id", "incident_id", "dashboard_id"}
_BENCHMARK_KEYS = {"benchmark_name"}
_CONTEXT_KEYS = {"context_available"}
_CASE_CLASS_KEYS = {"case_class"}
_STAGE_KEYS = {"failure_stage"}
_REASON_CODE_KEYS = {"reason_codes"}
_FAILURE_KEYS = {"failures"}
_TRUE_BOOL_KEYS = {"anonymous"}
_FALSE_BOOL_KEYS = {"raw_inputs_included"}
_STRING_ONLY_KEYS = (
    _ANON_ID_KEYS
    | _VERSION_KEYS
    | _TIMESTAMP_KEYS
    | _HASH_KEYS
    | _MODE_KEYS
    | _TRUNCATION_KEYS
    | _BENCHMARK_KEYS
    | _CONTEXT_KEYS
    | _CASE_CLASS_KEYS
    | _STAGE_KEYS
    | _REASON_CODE_KEYS
    | _FAILURE_KEYS
    | {"reason", "assumption"}
)


def evaluation_results_dir() -> Path:
    """Resolve the local directory where benchmark evaluation results are stored."""
    override = os.environ.get(EVALUATION_RESULTS_DIR_ENV, "")
    if override:
        return Path(override)
    try:
        from tacit.config import settings

        custom = getattr(settings, "evaluation_results_dir", "")
        if custom:
            return Path(str(custom))
    except Exception:  # pragma: no cover - settings are optional here
        pass
    return DEFAULT_EVALUATION_RESULTS_DIR


def save_evaluation_result(report: dict[str, Any], *, directory: Path | None = None) -> Path:
    """Persist a raw benchmark harness report locally for later export summarization.

    The stored file may contain raw fixture content; it is never exported.
    Only the anonymized summary built from it enters assessment bundles.
    """
    directory = directory or evaluation_results_dir()
    directory.mkdir(parents=True, exist_ok=True)
    report = dict(report)
    report.setdefault("generated_at", _now_iso())
    name = str(report.get("benchmark") or report.get("benchmark_name") or "evaluation")
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"{_safe_filename(name)}-{stamp}-{uuid.uuid4().hex[:8]}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_evaluation_results(directory: Path | None = None) -> list[dict[str, Any]]:
    """Load stored benchmark results, oldest first. Invalid JSON files are skipped."""
    directory = directory or evaluation_results_dir()
    if not directory.is_dir():
        return []
    results: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        data.setdefault("generated_at", _mtime_iso(path))
        results.append(data)
    return results


def build_evaluation_summary(
    results: list[dict[str, Any]] | None = None,
    *,
    directory: Path | None = None,
) -> dict[str, Any]:
    """Build the exportable evaluation summary section.

    Keeps the latest result per benchmark and dataset identity. Entries that
    fail privacy validation are dropped (fail closed) rather than exported.
    """
    if results is None:
        results = load_evaluation_results(directory)

    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for result in results:
        try:
            entries = _entries_from_result(result)
        except Exception:
            continue
        for entry in entries:
            name = str(entry.get("benchmark_name", ""))
            if not name:
                continue
            validation = validate_evaluation_summary({"evaluation_version": EVALUATION_VERSION, "evaluations": [entry]})
            if not validation["passed"]:
                continue
            identity = _evaluation_identity(entry)
            previous = latest.get(identity)
            if previous is None or str(entry.get("generated_at", "")) >= str(previous.get("generated_at", "")):
                latest[identity] = entry

    evaluations = []
    for identity in sorted(latest):
        evaluations.append(latest[identity])

    if not evaluations:
        return {
            "evaluation_version": EVALUATION_VERSION,
            "available": False,
            "reason": UNAVAILABLE_REASON,
        }
    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "evaluations": evaluations,
    }


def validate_evaluation_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Allowlist-based privacy validation for evaluation summaries.

    Rejects raw prompts, raw operational names (service/alert/runbook/incident/
    dashboard titles), URLs, emails, paths, hostnames, and secrets. Permits
    reason codes, anonymous IDs, counts, ratios, hashes, and timestamps.
    """
    findings: list[dict[str, str]] = []
    _walk_validate(summary, "$", findings)
    return {
        "passed": not findings,
        "findings_count": len(findings),
        "findings": findings[:100],
    }


# ---------------------------------------------------------------------------
# Adapters from raw harness reports to anonymized summary entries
# ---------------------------------------------------------------------------


def _entries_from_result(result: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(result.get("evaluations"), list):
        return [dict(entry) for entry in result["evaluations"] if isinstance(entry, dict)]
    if "benchmark_name" in result:
        return [dict(result)]
    if "benchmark_contract" in result and "metrics" in result:
        entry = summarize_ranking_report(result)
        return [entry] if entry else []
    if "benchmark_contract" in result and isinstance(result.get("after"), dict):
        entry = summarize_lift_report(result)
        return [entry] if entry else []
    if result.get("benchmark") == "artifact_learning_robustness":
        entry = summarize_artifact_robustness_report(result)
        return [entry] if entry else []
    if result.get("dataset") == "gamma" and "prediction_evaluation" in result and "control_evaluation" in result:
        entry = summarize_gamma_report(result)
        return [entry] if entry else []
    if {"classification", "cold_resolution", "learned_resolution", "learned_selection"} <= result.keys():
        entry = summarize_offline_gate_report(result)
        return [entry] if entry else []
    if {"positive_useful_rate", "negative_correct_rate", "worst_prompt_rate"} <= result.keys():
        entry = summarize_prompt_variation_report(result)
        return [entry] if entry else []
    return []


def summarize_lift_report(report: dict[str, Any]) -> dict[str, Any] | None:
    """Summarize contextual ranking lift harness reports from their after result."""
    after = report.get("after") or {}
    contract = report.get("benchmark_contract") or {}
    if not isinstance(after, dict) or not contract or not after.get("metrics"):
        return None

    merged = dict(after)
    merged["benchmark"] = str(report.get("benchmark", after.get("benchmark", "")))
    merged["version"] = str(report.get("version", after.get("version", "")) or "lift_v1")
    merged["generated_at"] = str(report.get("generated_at", after.get("generated_at", "")))
    merged["benchmark_contract"] = contract
    entry = summarize_ranking_report(merged)
    if not entry:
        return None

    deltas = report.get("deltas") or {}
    for source, target in {
        "top1_recall": "top1_delta",
        "top3_recall": "top3_delta",
        "mrr": "mrr_delta",
        "false_culprit_rate": "false_culprit_delta",
        "unsupported_rca_rate": "unsupported_rca_delta",
    }.items():
        if source in deltas:
            entry["metrics"][target] = {"value": _float(deltas.get(source))}

    metric_sections = [
        report.get("alert_metrics") or {},
        report.get("runbook_metrics") or {},
        report.get("incident_metrics") or {},
    ]
    denominators = contract.get("metric_denominators") or {}
    for section in metric_sections:
        for name, value in section.items():
            if name not in _METRIC_NAMES or not isinstance(value, int | float):
                continue
            metric: dict[str, Any] = {"value": _float(value)}
            denominator = denominators.get(name)
            if isinstance(denominator, int | float) and denominator:
                metric["denominator"] = int(denominator)
                metric["numerator"] = round(metric["value"] * int(denominator))
            entry["metrics"][name] = metric

    failures = _safe_failure_codes((report.get("gate") or {}).get("failures", []))
    entry["failures"] = failures
    for failure in entry["failures"]:
        entry["failure_reasons"][failure] = entry["failure_reasons"].get(failure, 0) + 1
    return entry


def summarize_artifact_robustness_report(report: dict[str, Any]) -> dict[str, Any] | None:
    """Summarize artifact-learning robustness gates without artifact text."""
    rca = report.get("rca_phrase_robustness") or {}
    precision = report.get("rca_precision") or {}
    noise = report.get("noise_injection") or {}
    contradictory = report.get("contradictory_artifacts") or {}
    if not rca or not precision or not noise or not contradictory:
        return None

    rca_phrases = int(rca.get("phrases", 0) or 0)
    rca_ignored = int(rca.get("ignored_causal_claim_count", 0) or 0)
    precision_phrases = int(precision.get("phrases", 0) or 0)
    false_positive_suppressions = int(precision.get("false_positive_suppression_count", 0) or 0)
    noise_rows = [row for row in noise.get("rows", []) if isinstance(row, dict)]
    noise_mrr_deltas = [_float(row.get("mrr_delta")) for row in noise_rows]
    failures = _safe_failure_codes((report.get("gate") or {}).get("failures", []))
    failure_reasons = _counts(failures)
    contract = {
        "total_cases": rca_phrases + precision_phrases + len(noise_rows) + 1,
        "rca_phrases": rca_phrases,
        "rca_precision_phrases": precision_phrases,
        "noise_scenarios": len(noise_rows),
    }

    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "benchmark_name": "artifact_learning_robustness",
        "benchmark_version": _safe_version(report.get("version"), default="artifact_robustness_v1"),
        "dataset_hash": _dataset_hash(contract),
        "runner_version": _safe_version(report.get("runner_version"), default=__version__),
        "generated_at": str(report.get("generated_at", "")) or _now_iso(),
        "mode": "artifact_robustness",
        "context_available": ["context", "alerts", "runbooks", "incidents"],
        "anonymous": True,
        "raw_inputs_included": False,
        "contract": contract,
        "metrics": {
            "rca_suppression_recall": _ratio(rca_ignored, rca_phrases),
            "rca_precision": _ratio(precision_phrases - false_positive_suppressions, precision_phrases),
            "noise_scenarios": {"value": float(len(noise_rows))},
            "noise_worst_mrr_delta": {"value": min(noise_mrr_deltas) if noise_mrr_deltas else 0.0},
            "contradictory_artifacts_passed": {"value": 1.0 if contradictory.get("passed") else 0.0},
        },
        "random_baselines": {},
        "stage_counts": {
            "passed": 1 if (report.get("gate") or {}).get("passed") else 0,
            "failed": 0 if (report.get("gate") or {}).get("passed") else 1,
            "dropped": 0,
            "indeterminate": 0,
        },
        "failure_reasons": failure_reasons,
        "failures": failures,
        "per_case": [],
    }


def summarize_gamma_report(report: dict[str, Any]) -> dict[str, Any] | None:
    """Summarize GAMMA naming diagnostic reports without prompts or manifests."""
    prediction = report.get("prediction_evaluation") or {}
    control = report.get("control_evaluation") or {}
    if not prediction or not control:
        return None

    prediction_counts = prediction.get("counts") or {}
    control_counts = control.get("counts") or {}
    dashboards = prediction_counts.get("dashboards") or {}
    known_gaps = control.get("known_gaps") or {}
    symptom_panel = known_gaps.get("evidence_absent_preserves_symptom_panel") or {}
    failures = _safe_failure_codes(
        name
        for checks in (prediction.get("checks") or {}, control.get("checks") or {})
        for name, passed in checks.items()
        if not passed
    )
    failure_reasons = _counts(failures)
    control_cases = int(control_counts.get("abstention", {}).get("denominator", 0) or 0)
    evidence_denominator = int(prediction_counts.get("canonical_evidence_recall", {}).get("denominator", 0) or 0)
    contract = {
        "total_cases": control_cases + sum(int(item.get("denominator", 0) or 0) for item in dashboards.values()),
        "control_cases": control_cases,
        "evidence_signal_denominator": evidence_denominator,
    }
    fingerprint = report.get("protocol_fingerprint") or {}
    dataset_contract = {
        "dataset": "gamma",
        "scenario_id": report.get("scenario_id", ""),
        "protocol_sha256": fingerprint.get("protocol_sha256", ""),
        "control_matrix_sha256": fingerprint.get("control_matrix_sha256", ""),
    }

    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "benchmark_name": "gamma",
        "benchmark_version": "gamma_diagnostic_v1",
        "dataset_hash": _dataset_hash(dataset_contract),
        "runner_version": _safe_version(report.get("runner_version"), default=__version__),
        "generated_at": str(report.get("generated_at", "")) or _now_iso(),
        "mode": "gamma",
        "context_available": ["context"],
        "anonymous": True,
        "raw_inputs_included": False,
        "contract": contract,
        "metrics": {
            "canonical_evidence_recall": _metric_from_count(prediction_counts.get("canonical_evidence_recall")),
            "prefixed_evidence_recall": _metric_from_count(prediction_counts.get("prefixed_evidence_recall")),
            "canonical_dashboard_rate": _metric_from_count(dashboards.get("canonical")),
            "prefixed_dashboard_rate": _metric_from_count(dashboards.get("prefixed")),
            "raw_dashboard_rate": _metric_from_count(dashboards.get("raw")),
            "false_culprit_rate_controls": _metric_from_count(control_counts.get("false_culprit")),
            "control_abstention_rate": _metric_from_count(control_counts.get("abstention")),
            "healthy_symptom_panel_recall": _metric_from_count(symptom_panel),
        },
        "random_baselines": {},
        "stage_counts": {
            "passed": int(bool(prediction.get("passed"))) + int(bool(control.get("passed"))),
            "failed": int(not bool(prediction.get("passed"))) + int(not bool(control.get("passed"))),
            "dropped": 0,
            "indeterminate": 0,
        },
        "failure_reasons": failure_reasons,
        "failures": failures,
        "per_case": [],
    }


def summarize_offline_gate_report(report: dict[str, Any]) -> dict[str, Any] | None:
    """Summarize offline accuracy-gate reports without metric names or fixture rows."""
    classification = [row for row in report.get("classification", []) if isinstance(row, dict)]
    cold = [row for row in report.get("cold_resolution", []) if isinstance(row, dict)]
    learned = [row for row in report.get("learned_resolution", []) if isinstance(row, dict)]
    selection = [row for row in report.get("learned_selection", []) if isinstance(row, dict)]
    if not classification and not cold and not learned and not selection:
        return None

    tp = sum(int(row.get("tp", 0) or 0) for row in classification)
    fp = sum(int(row.get("fp", 0) or 0) for row in classification)
    fn = sum(int(row.get("fn", 0) or 0) for row in classification)
    labeled = sum(int(row.get("labeled_signal_metrics", 0) or 0) for row in classification)
    uncovered = sum(len(row.get("uncovered", []) or []) for row in classification)
    cold_resolved = sum(int(row.get("resolved", 0) or 0) for row in cold)
    cold_total = sum(int(row.get("total", 0) or 0) for row in cold)
    learned_resolved = sum(int(row.get("resolved", 0) or 0) for row in learned)
    learned_total = sum(int(row.get("total", 0) or 0) for row in learned)
    selection_passed = sum(1 for row in selection if row.get("passed"))

    failures = [_offline_gate_failure_code(str(item)) for item in (report.get("gate") or {}).get("failures", [])]
    failure_reasons = _counts(failures)
    contract = {
        "total_cases": len(classification) + len(cold) + len(learned) + len(selection),
        "classification_datasets": len(classification),
        "cold_resolution_datasets": len(cold),
        "learned_resolution_datasets": len(learned),
        "learned_selection_datasets": len(selection),
        "labeled_signal_metrics": labeled,
        "critical_signals": cold_total,
    }

    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "benchmark_name": "offline_gate",
        "benchmark_version": _safe_version(report.get("version"), default="offline_gate_v1"),
        "dataset_hash": _dataset_hash(contract),
        "runner_version": _safe_version(report.get("runner_version"), default=__version__),
        "generated_at": str(report.get("generated_at", "")) or _now_iso(),
        "mode": "gate",
        "context_available": ["context"],
        "anonymous": True,
        "raw_inputs_included": False,
        "contract": contract,
        "metrics": {
            "semantic_precision": _ratio(tp, tp + fp),
            "semantic_recall": _ratio(tp, tp + fn),
            "semantic_coverage": _ratio(labeled - uncovered, labeled),
            "cold_resolution_recall": _ratio(cold_resolved, cold_total),
            "learned_resolution_recall": _ratio(learned_resolved, learned_total),
            "learned_selection_rate": _ratio(selection_passed, len(selection)),
        },
        "random_baselines": {},
        "stage_counts": {
            "passed": 1 if (report.get("gate") or {}).get("passed") else 0,
            "failed": 0 if (report.get("gate") or {}).get("passed") else 1,
            "dropped": 0,
            "indeterminate": 0,
        },
        "failure_reasons": failure_reasons,
        "failures": failures,
        "per_case": [],
    }


def summarize_ranking_report(report: dict[str, Any]) -> dict[str, Any] | None:
    """Summarize a contextual culprit-ranking harness report (see tests/eval)."""
    contract_in = report.get("benchmark_contract") or {}
    metrics_in = report.get("metrics") or {}
    if not contract_in or not metrics_in:
        return None

    benchmark_name = str(report.get("benchmark", "contextual_culprit_ranking"))
    denominators = contract_in.get("metric_denominators") or {}
    total_cases = int(contract_in.get("total_cases", report.get("case_count", 0)))
    scorable = int(contract_in.get("scorable_cases", 0))
    negative = int(contract_in.get("negative_cases", 0))
    candidate_set_size = int(contract_in.get("candidate_set_size", 0))
    top_k = int(contract_in.get("top_k", 3))

    positive_rows = report.get("positive_cases") or []
    false_culprits = set(report.get("false_culprits") or [])
    unsupported_entries = [item for item in report.get("unsupported_rca") or [] if isinstance(item, dict)]
    unsupported_cases = {str(item.get("case", "")) for item in unsupported_entries}

    case_ids = sorted(str(case_id) for case_id in (report.get("results") or {}).keys())
    if not case_ids:
        case_ids = sorted(str(row.get("id", "")) for row in positive_rows)
    case_alias = {case_id: f"case_{index:03d}" for index, case_id in enumerate(case_ids, start=1)}
    positive_ids = {str(row.get("id", "")) for row in positive_rows}

    top1_hits = sum(1 for row in positive_rows if row.get("top1_hit"))
    top3_hits = sum(1 for row in positive_rows if row.get("top3_hit"))

    per_case: list[dict[str, Any]] = []
    for case_id in case_ids:
        alias = case_alias[case_id]
        if case_id in positive_ids:
            row = next(row for row in positive_rows if str(row.get("id", "")) == case_id)
            rank = _finite_int(row.get("rank"))
            per_case.append(
                {
                    "case_id": alias,
                    "case_class": "scorable",
                    "expected_rank": 1,
                    "actual_rank": rank,
                    "top1_hit": bool(row.get("top1_hit")),
                    "top3_hit": bool(row.get("top3_hit")),
                    "mrr_contribution": round(1 / rank, 4) if rank else 0.0,
                    "false_culprit": False,
                    "unsupported_rca": case_id in unsupported_cases,
                    "failure_stage": None if row.get("top3_hit") else "ranking",
                }
            )
        else:
            false_culprit = case_id in false_culprits
            per_case.append(
                {
                    "case_id": alias,
                    "case_class": "negative_noise",
                    "expected_rank": None,
                    "actual_rank": None,
                    "top1_hit": None,
                    "top3_hit": None,
                    "mrr_contribution": None,
                    "false_culprit": false_culprit,
                    "unsupported_rca": case_id in unsupported_cases,
                    "failure_stage": "ranking" if false_culprit else None,
                }
            )

    failure_reasons: dict[str, int] = {}
    top1_missed = scorable - top1_hits
    top3_missed = scorable - top3_hits
    if top1_missed:
        failure_reasons["top1_missed"] = top1_missed
    if top3_missed:
        failure_reasons["top3_missed"] = top3_missed
    if false_culprits:
        failure_reasons["false_culprit"] = len(false_culprits)
    if unsupported_entries:
        failure_reasons["unsupported_rca"] = len(unsupported_entries)
    failures = _safe_failure_codes((report.get("gate") or {}).get("failures", []))
    for failure in failures:
        failure_reasons[failure] = failure_reasons.get(failure, 0) + 1

    negative_passed = negative - len(false_culprits)
    stage_counts = {
        "passed": top3_hits + negative_passed,
        "failed": (scorable - top3_hits) + len(false_culprits),
        "dropped": 0,
        "indeterminate": 0,
    }

    baselines_in = contract_in.get("random_baselines") or {}
    context_available = [str(item) for item in contract_in.get("context_available") or []]

    metrics = {
        "top1": {
            "numerator": top1_hits,
            "denominator": int(denominators.get("top1_recall", scorable)),
            "value": _float(metrics_in.get("top1_recall")),
        },
        "top3": {
            "numerator": top3_hits,
            "denominator": int(denominators.get("top3_recall", scorable)),
            "value": _float(metrics_in.get("top3_recall")),
        },
        "mrr": {
            "denominator": int(denominators.get("mrr", scorable)),
            "value": _float(metrics_in.get("mrr")),
            "truncation": MRR_UNTRUNCATED,
        },
        "false_culprit_rate": {
            "numerator": len(false_culprits),
            "denominator": int(denominators.get("false_culprit_rate", negative)),
            "value": _float(metrics_in.get("false_culprit_rate")),
        },
        "unsupported_rca_rate": {
            "numerator": len(unsupported_entries),
            "denominator": int(denominators.get("unsupported_rca_rate", total_cases)),
            "value": _float(metrics_in.get("unsupported_rca_rate")),
        },
    }
    extra_denominators = {
        "evidence_attribution": int(denominators.get("unsupported_rca_rate", 0) or 0),
        "negative_correctness": int(negative),
        "contextual_top3_only_recall": len(report.get("contextual_top3_only_cases") or []),
        "contextual_top3_only_not_top1": len(report.get("contextual_top3_only_cases") or []),
    }
    for source_name, export_name in {
        "evidence_attribution": "evidence_attribution",
        "negative_correctness": "negative_correctness",
        "abstention_on_insufficient": "abstention_on_insufficient",
        "contextual_top3_only_recall": "contextual_top3_only_recall",
        "contextual_top3_only_not_top1": "contextual_top3_only_not_top1",
    }.items():
        if source_name not in metrics_in:
            continue
        value = _float(metrics_in.get(source_name))
        metric: dict[str, Any] = {"value": value}
        denominator = extra_denominators.get(source_name, 0)
        if denominator:
            metric["denominator"] = denominator
            metric["numerator"] = round(value * denominator)
        metrics[export_name] = metric

    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "benchmark_name": benchmark_name,
        "benchmark_version": _safe_version(report.get("version"), default="version_hash_0000000000000000"),
        "dataset_hash": _dataset_hash(contract_in),
        "runner_version": _safe_version(report.get("runner_version"), default=__version__),
        "generated_at": str(report.get("generated_at", "")) or _now_iso(),
        "mode": _BENCHMARK_MODES.get(benchmark_name, "gate"),
        "context_available": context_available,
        "anonymous": True,
        "raw_inputs_included": False,
        "contract": {
            "total_cases": total_cases,
            "scorable_culprit_cases": scorable,
            "negative_noise_cases": negative,
            "recall_denominator": int(denominators.get("top1_recall", scorable)),
            "mrr_denominator": int(denominators.get("mrr", scorable)),
            "source_contribution_denominator": total_cases,
            "candidate_set_size": candidate_set_size,
            "top_k": top_k,
            "mrr_truncation": MRR_UNTRUNCATED,
            "candidate_set_fixed": True,
        },
        "metrics": metrics,
        "random_baselines": {
            "top1": _float(baselines_in.get("top1_recall")),
            "top3": _float(baselines_in.get("top3_recall")),
            "mrr": _float(baselines_in.get("mrr")),
            "assumption": f"uniform random permutation over {candidate_set_size} candidates",
            "mrr_truncation": MRR_UNTRUNCATED,
        },
        "stage_counts": stage_counts,
        "failure_reasons": failure_reasons,
        "failures": failures,
        "per_case": per_case,
    }


def summarize_prompt_variation_report(report: dict[str, Any]) -> dict[str, Any] | None:
    """Summarize a prompt-variation harness report without exporting prompts."""
    rows = [row for row in (report.get("results") or []) if isinstance(row, dict)]
    trials_per_prompt = int(report.get("trials_per_prompt", 0) or 0)
    if not rows or trials_per_prompt < 1:
        return None

    positive = [row for row in rows if row.get("polarity") == "positive"]
    negative = [row for row in rows if row.get("polarity") == "negative"]
    positive_trials = sum(int(row.get("trials", trials_per_prompt) or 0) for row in positive)
    negative_trials = sum(int(row.get("trials", trials_per_prompt) or 0) for row in negative)
    positive_passed = sum(int(row.get("passed", 0) or 0) for row in positive)
    negative_passed = sum(int(row.get("passed", 0) or 0) for row in negative)

    per_case = []
    failure_reasons: dict[str, int] = {}
    prompt_rates: list[float] = []
    for index, row in enumerate(rows, start=1):
        trials = int(row.get("trials", trials_per_prompt) or 0)
        passed = int(row.get("passed", 0) or 0)
        rate = round(passed / trials, 4) if trials else 0.0
        prompt_rates.append(rate)
        case_failed = passed < trials
        if case_failed:
            failure_reasons["prompt_intent_mismatch"] = failure_reasons.get("prompt_intent_mismatch", 0) + 1
        per_case.append(
            {
                "case_id": f"case_{index:03d}",
                "case_class": str(row.get("polarity") or "unknown"),
                "passed_trials": passed,
                "total_trials": trials,
                "rate": rate,
                "failure_stage": "intent" if case_failed else None,
                "reason_codes": ["prompt_intent_mismatch"] if case_failed else [],
            }
        )

    contract = {
        "total_cases": len(rows),
        "positive_cases": len(positive),
        "negative_cases": len(negative),
        "trials_per_prompt": trials_per_prompt,
        "positive_denominator": positive_trials,
        "negative_denominator": negative_trials,
    }

    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "benchmark_name": "prompt_variation",
        "benchmark_version": _safe_version(report.get("version"), default="prompt_variation_v1"),
        "dataset_hash": _dataset_hash(
            {
                "corpus": report.get("corpus", ""),
                "role": report.get("role", ""),
                "prompts": int(report.get("prompts", len(rows)) or 0),
                "trials_per_prompt": trials_per_prompt,
                "corpus_content_sha256": _prompt_corpus_fingerprint(rows),
            }
        ),
        "runner_version": _safe_version(report.get("runner_version"), default=__version__),
        "generated_at": str(report.get("generated_at", "")) or _now_iso(),
        "mode": "prompt_variation",
        "context_available": [],
        "anonymous": True,
        "raw_inputs_included": False,
        "contract": contract,
        "metrics": {
            "positive_useful_rate": {
                "numerator": positive_passed,
                "denominator": positive_trials,
                "value": _ratio_value(positive_passed, positive_trials),
            },
            "negative_correct_rate": {
                "numerator": negative_passed,
                "denominator": negative_trials,
                "value": _ratio_value(negative_passed, negative_trials),
            },
            "worst_prompt_rate": {"value": min(prompt_rates) if prompt_rates else 0.0},
        },
        "random_baselines": {},
        "stage_counts": {
            "passed": sum(1 for row in per_case if row["failure_stage"] is None),
            "failed": sum(1 for row in per_case if row["failure_stage"] is not None),
            "dropped": 0,
            "indeterminate": 0,
        },
        "failure_reasons": failure_reasons,
        "per_case": per_case,
    }


# ---------------------------------------------------------------------------
# Validation internals
# ---------------------------------------------------------------------------


def _walk_validate(value: Any, path: str, findings: list[dict[str, str]], key: str = "") -> None:
    if isinstance(value, dict):
        for child_key, item in value.items():
            child_key = str(child_key)
            if not _key_is_allowed(path, child_key):
                findings.append({"path": f"{path}.<key>", "kind": "forbidden_key", "sample": "<redacted_key>"})
                continue
            _walk_validate(item, f"{path}.{child_key}", findings, key=child_key)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _walk_validate(item, f"{path}[{index}]", findings, key=key)
    elif isinstance(value, str):
        if not _string_is_allowed(key, value, path):
            findings.append({"path": path, "kind": "forbidden_value", "sample": "<redacted_value>"})
    elif isinstance(value, int | float) and not isinstance(value, bool):
        if key in _STRING_ONLY_KEYS:
            findings.append({"path": path, "kind": "invalid_type", "sample": "<redacted_value>"})
        if not math.isfinite(float(value)):
            findings.append({"path": path, "kind": "non_finite_number", "sample": "<redacted_value>"})
    elif isinstance(value, bool):
        if key in _TRUE_BOOL_KEYS and value is not True:
            findings.append({"path": path, "kind": "invalid_boolean", "sample": "<redacted_value>"})
        if key in _FALSE_BOOL_KEYS and value is not False:
            findings.append({"path": path, "kind": "invalid_boolean", "sample": "<redacted_value>"})


def _string_is_allowed(key: str, value: str, path: str) -> bool:
    if _contains_leakage(value):
        return False
    if value == "":
        return True
    if key in _ANON_ID_KEYS:
        return bool(_ANON_ID_RE.match(value))
    if key == "reason":
        return path == "$.reason" and value == UNAVAILABLE_REASON
    if key == "assumption":
        return bool(re.fullmatch(r"uniform random permutation over [1-9][0-9]* candidates", value))
    if key in _TIMESTAMP_KEYS:
        return bool(_TIMESTAMP_RE.match(value))
    if key in _HASH_KEYS:
        return bool(_HASH_RE.match(value))
    if key in _VERSION_KEYS:
        return bool(_VERSION_RE.match(value)) or value in _PUBLIC_VERSION_LABELS
    if key in _MODE_KEYS:
        return value in EVALUATION_MODES
    if key in _TRUNCATION_KEYS:
        return value in MRR_TRUNCATIONS
    if key in _BENCHMARK_KEYS:
        return value in _BENCHMARK_NAMES
    if key in _CONTEXT_KEYS:
        return value in _CONTEXT_VALUES
    if key in _CASE_CLASS_KEYS:
        return value in _CASE_CLASSES
    if key in _STAGE_KEYS:
        return value in _STAGE_NAMES
    if key in _REASON_CODE_KEYS:
        return value in _REASON_CODES
    if key in _FAILURE_KEYS:
        return value in _REASON_CODES
    return False


def _key_is_allowed(path: str, key: str) -> bool:
    if not _SNAKE_RE.match(key):
        return False
    normalized_path = re.sub(r"\[\d+\]", "[]", path)
    dynamic_keys = _DYNAMIC_KEYS_BY_PATH.get(normalized_path)
    if dynamic_keys is not None:
        return key in dynamic_keys
    return key in _SCHEMA_KEYS


def _contains_leakage(value: str) -> bool:
    from tacit.export_report import _LEAKAGE_PATTERNS

    return any(pattern.search(value) for _kind, pattern in _LEAKAGE_PATTERNS)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dataset_hash(contract: dict[str, Any]) -> str:
    """Hash the benchmark dataset contract, never raw private artifact contents."""
    canonical = json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _evaluation_identity(entry: dict[str, Any]) -> tuple[str, str]:
    name = str(entry.get("benchmark_name", ""))
    dataset_hash = str(entry.get("dataset_hash", ""))
    if not dataset_hash and isinstance(entry.get("contract"), dict):
        dataset_hash = _dataset_hash(entry["contract"])
    return name, dataset_hash


def _prompt_corpus_fingerprint(rows: list[dict[str, Any]]) -> str:
    payload = [
        {
            "class": row.get("class", ""),
            "polarity": row.get("polarity", ""),
            "prompt": row.get("prompt", ""),
        }
        for row in rows
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _ratio(numerator: int | float, denominator: int | float) -> dict[str, Any]:
    numerator = int(numerator or 0)
    denominator = int(denominator or 0)
    return {
        "numerator": numerator,
        "denominator": denominator,
        "value": _ratio_value(numerator, denominator),
    }


def _ratio_value(numerator: int | float, denominator: int | float) -> float:
    denominator = float(denominator or 0)
    if not denominator:
        return 0.0
    return round(float(numerator or 0) / denominator, 4)


def _metric_from_count(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"value": 0.0}
    numerator = value.get("numerator", 0)
    denominator = value.get("denominator", 0)
    metric = _ratio(numerator, denominator)
    if "recall" in value:
        metric["value"] = _float(value.get("recall"))
    return metric


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[value] = counts.get(value, 0) + 1
    return counts


def _safe_failure_codes(values: Any) -> list[str]:
    codes = []
    for value in values:
        code = _reason_code(str(value))
        codes.append(code if code in _REASON_CODES else "other")
    return codes


def _offline_gate_failure_code(value: str) -> str:
    normalized = _reason_code(value)
    if "semantic_precision" in normalized:
        return "semantic_precision_below_threshold"
    if "semantic_recall" in normalized:
        return "semantic_recall_below_threshold"
    if "semantic_coverage" in normalized:
        return "semantic_coverage_below_threshold"
    if "cold_resolution" in normalized:
        return "cold_resolution_below_threshold"
    if "learned_resolution" in normalized:
        return "learned_resolution_below_threshold"
    if "learned_selection" in normalized:
        return "learned_selection_mismatch"
    return "other"


def _safe_version(value: Any, *, default: str) -> str:
    candidate = str(value or default)
    if _VERSION_RE.match(candidate) or candidate in _PUBLIC_VERSION_LABELS:
        return candidate
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()[:16]
    return f"version_hash_{digest}"


def _reason_code(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return cleaned or "unknown"


def _float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if math.isfinite(number) else 0.0


def _finite_int(value: Any) -> int | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return int(number)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _mtime_iso(path: Path) -> str:
    try:
        stamp = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return _now_iso()
    return stamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_filename(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", name.lower()).strip("_")
    return cleaned or "evaluation"
