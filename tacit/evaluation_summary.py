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
import os
import re
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
    "contextual_artifact_ranking": "gate",
    "contextual_alerts_runbooks_baseline_v1": "gate",
    "contextual_culprit_ranking": "gate",
    "incident_context_ranking_lift": "gate",
    "prompt_variation": "prompt_variation",
    "gamma": "gamma",
    "artifact_robustness": "artifact_robustness",
}

_BENCHMARK_NAMES = set(_BENCHMARK_MODES)
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
    "positive_useful_rate",
    "negative_correct_rate",
    "worst_prompt_rate",
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
    "scorable_culprit_cases",
    "source_contribution_denominator",
    "top_k",
    "total_cases",
    "trials_per_prompt",
}
_REASON_CODES = {
    "false_culprit",
    "prompt_intent_mismatch",
    "top1_missed",
    "top3_missed",
    "unsupported_rca",
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
_VERSION_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+\-]{0,63}$")
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
    path = directory / f"{_safe_filename(name)}-{stamp}.json"
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
        for entry in _entries_from_result(result):
            name = str(entry.get("benchmark_name", ""))
            if not name:
                continue
            identity = _evaluation_identity(entry)
            previous = latest.get(identity)
            if previous is None or str(entry.get("generated_at", "")) >= str(previous.get("generated_at", "")):
                latest[identity] = entry

    evaluations = []
    for identity in sorted(latest):
        entry = latest[identity]
        validation = validate_evaluation_summary({"evaluation_version": EVALUATION_VERSION, "evaluations": [entry]})
        if validation["passed"]:
            evaluations.append(entry)

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
    merged["benchmark_contract"] = contract
    return summarize_ranking_report(merged)


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
            rank = row.get("rank")
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

    return {
        "evaluation_version": EVALUATION_VERSION,
        "available": True,
        "benchmark_name": benchmark_name,
        "benchmark_version": str(report.get("version", "")),
        "dataset_hash": _dataset_hash(contract_in),
        "runner_version": str(report.get("runner_version", "")) or __version__,
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
    for index, row in enumerate(rows, start=1):
        trials = int(row.get("trials", trials_per_prompt) or 0)
        passed = int(row.get("passed", 0) or 0)
        case_failed = passed < trials
        if case_failed:
            failure_reasons["prompt_intent_mismatch"] = failure_reasons.get("prompt_intent_mismatch", 0) + 1
        per_case.append(
            {
                "case_id": f"case_{index:03d}",
                "case_class": str(row.get("polarity") or "unknown"),
                "passed_trials": passed,
                "total_trials": trials,
                "rate": _float(row.get("rate")),
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
        "benchmark_version": str(report.get("version", "prompt_variation_v1")),
        "dataset_hash": _dataset_hash(
            {
                "corpus": report.get("corpus", ""),
                "role": report.get("role", ""),
                "prompts": int(report.get("prompts", len(rows)) or 0),
                "trials_per_prompt": trials_per_prompt,
            }
        ),
        "runner_version": str(report.get("runner_version", "")) or __version__,
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
                "value": _float(report.get("positive_useful_rate")),
            },
            "negative_correct_rate": {
                "numerator": negative_passed,
                "denominator": negative_trials,
                "value": _float(report.get("negative_correct_rate")),
            },
            "worst_prompt_rate": {"value": _float(report.get("worst_prompt_rate"))},
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
        if not _string_is_allowed(key, value):
            findings.append({"path": path, "kind": "forbidden_value", "sample": "<redacted_value>"})


def _string_is_allowed(key: str, value: str) -> bool:
    if _contains_leakage(value):
        return False
    if value == "":
        return True
    if key in _ANON_ID_KEYS:
        return bool(_ANON_ID_RE.match(value))
    if key in _FREE_TEXT_KEYS:
        return bool(_FREE_TEXT_RE.match(value))
    if key in _TIMESTAMP_KEYS:
        return bool(_TIMESTAMP_RE.match(value))
    if key in _HASH_KEYS:
        return bool(_HASH_RE.match(value))
    if key in _VERSION_KEYS:
        return bool(_VERSION_RE.match(value))
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


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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
