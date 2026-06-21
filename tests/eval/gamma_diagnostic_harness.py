"""Live, cache-isolated GAMMA naming diagnostic.

This is deliberately a diagnostic harness, not an accuracy benchmark. It keeps
the underlying infrastructure samples fixed while changing only their metric
name representation, then records the pipeline's reason-coded stage outcomes.

Run with the isolated GAMMA Grafana and VictoriaMetrics stack available:

    python -m tests.eval.gamma_diagnostic_harness --json data/gamma/prefix-baseline.json
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx

from dashforge.agents.providers import registry as provider_registry
from dashforge.cache import llm_cache, metric_cache
from dashforge.config import settings
from dashforge.models.schemas import DashRequest
from dashforge.pipeline import run_pipeline
from demo.gamma_pilot import DEFAULT_ARCHIVE, DEFAULT_SCENARIO, build
from tests.eval.cold_isolation import cold_isolation

PROTOCOL_PATH = Path(__file__).parent / "fixtures" / "gamma_diagnostic_protocol.json"
PROTOCOL = json.loads(PROTOCOL_PATH.read_text())
PROMPTS = PROTOCOL["arm_prompts"]

ARMS = {
    "canonical": "Exact packaged canonical metric names",
    "prefixed": "Canonical stems with a gamma_ vendor prefix",
    "raw": "Raw GAMMA service-prefixed metric filenames",
}

CONTROL_CASES = PROTOCOL["control_cases"]
EXPECTED_EVIDENCE_SIGNALS = set(PROTOCOL["expected_evidence_signals"])
MIN_EVIDENCE_RECALL = float(PROTOCOL["minimum_evidence_recall"])
MIN_CONTROL_SCENARIOS = int(PROTOCOL["minimum_control_scenarios"])
DEFAULT_EXPECTATION = "post-fix"
CAUSE_ASSERTION_PATTERNS = (
    re.compile(r"\broot cause\b", re.IGNORECASE),
    re.compile(r"\bculprit\b", re.IGNORECASE),
    re.compile(r"\bcaused by\b", re.IGNORECASE),
    re.compile(r"\bdue to\b", re.IGNORECASE),
    re.compile(r"\bbottleneck(?:ed)?\b", re.IGNORECASE),
    re.compile(r"\bresponsible for\b", re.IGNORECASE),
)
ISOLATED_VM_SERIES_SELECTOR = '{__name__=~".+"}'

PREDICTED_OUTCOMES = PROTOCOL["expected_outcomes"]


@contextmanager
def _evaluation_settings(grafana_url: str, ollama_url: str, model: str):
    names = ("grafana_url", "grafana_api_key", "llm_provider", "llm_api_base", "llm_model")
    previous = {name: getattr(settings, name) for name in names}
    settings.grafana_url = grafana_url
    settings.grafana_api_key = ""
    settings.llm_provider = "ollama"
    settings.llm_api_base = ollama_url
    settings.llm_model = model
    provider_registry._provider = None
    try:
        yield
    finally:
        for name, value in previous.items():
            setattr(settings, name, value)
        provider_registry._provider = None


def _first_metric(metrics_path: Path) -> str:
    first_line = metrics_path.read_text().splitlines()[0]
    return first_line.split("{", 1)[0]


def _replace_gamma_metrics(client: httpx.Client, vm_url: str, metrics_path: Path) -> None:
    response = client.post(
        f"{vm_url}/api/v1/admin/tsdb/delete_series",
        params={"match[]": ISOLATED_VM_SERIES_SELECTOR},
    )
    response.raise_for_status()
    with metrics_path.open("rb") as payload:
        response = client.post(f"{vm_url}/api/v1/import/prometheus", content=payload)
    response.raise_for_status()

    expected = _first_metric(metrics_path)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        result = client.get(
            f"{vm_url}/api/v1/query",
            params={"query": f"count_over_time({expected}[1h])"},
        )
        result.raise_for_status()
        if result.json().get("data", {}).get("result"):
            return
        time.sleep(0.5)
    raise RuntimeError(f"imported metric did not become queryable: {expected}")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _protocol_fingerprint() -> dict[str, str]:
    arm_prompt_payload = json.dumps(PROMPTS, sort_keys=True)
    control_payload = json.dumps(CONTROL_CASES, sort_keys=True)
    return {
        "arm_prompt_set_sha256": _sha256(arm_prompt_payload),
        "control_matrix_sha256": _sha256(control_payload),
        "protocol_sha256": hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest(),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }


def _evidence_signals(generated_queries: list[dict[str, Any]]) -> list[str]:
    text = " ".join(
        f"{query.get('panel_title', '')} {query.get('expr', '')}" for query in generated_queries
    ).lower()
    present = set()
    if "cpu" in text or "processor" in text:
        present.add("cpu")
    if "memory" in text or "working_set" in text:
        present.add("memory")
    return sorted(present)


def _cause_match_is_negated(text: str, start: int, end: int) -> bool:
    before = text[max(0, start - 48) : start]
    after = text[end : end + 48]
    negated_before = re.search(
        r"\b(?:no|not|without|cannot|can't|isn't|wasn't)\b(?:\s+\w+){0,3}\s*$",
        before,
        re.IGNORECASE,
    )
    negated_after = re.match(
        r"\s+(?:(?:is|was|appears|seems)\s+)?(?:not|unlikely|unsupported|unconfirmed|absent|ruled out)\b",
        after,
        re.IGNORECASE,
    )
    return bool(negated_before or negated_after)


def _detect_cause_assertion(summary: str, generated_queries: list[dict[str, Any]]) -> dict[str, Any]:
    text = " ".join([summary, *(query.get("panel_title", "") for query in generated_queries)])
    matches = sorted(
        {
            match.group(0).lower()
            for pattern in CAUSE_ASSERTION_PATTERNS
            for match in pattern.finditer(text)
            if not _cause_match_is_negated(text, match.start(), match.end())
        }
    )
    return {"asserted": bool(matches), "matches": matches}


def _reset_and_snapshot_caches() -> None:
    metric_cache.invalidate()
    llm_cache.invalidate()
    metric_cache.reset_stats()
    llm_cache.reset_stats()


def _cache_stats() -> dict[str, dict[str, int]]:
    return {"metric": metric_cache.stats, "llm": llm_cache.stats}


async def _run_arm(arm: str) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    with cold_isolation() as state:
        for index, prompt in enumerate(PROMPTS, start=1):
            _reset_and_snapshot_caches()
            provider_registry._provider = None
            user_id = f"gamma-diagnostic-{arm}-{index}"
            response = await run_pipeline(DashRequest(prompt=prompt, user_id=user_id))
            record = state.history_store.list_recent(limit=1, user_id=user_id)[0]
            generated_queries = record["generated_queries"]
            rows.append(
                {
                    "prompt": prompt,
                    "dashboard_created": bool(response.dashboard_uid),
                    "panel_count": response.panel_count,
                    "status": record["status"],
                    "problem_type": record["problem_type"],
                    "archetypes": record["archetypes"],
                    "stages": record["stage_outcomes"],
                    "evidence_signals": _evidence_signals(generated_queries),
                    "cause_assertion": _detect_cause_assertion(response.summary, generated_queries),
                    "cache_stats": _cache_stats(),
                }
            )
    return {
        "arm": arm,
        "description": ARMS[arm],
        "independent_prompts": len(rows),
        "dashboards_created": sum(row["dashboard_created"] for row in rows),
        "results": rows,
    }


def _healthy_slice(metrics_path: Path, manifest_path: Path, output_dir: Path) -> Path:
    manifest = json.loads(manifest_path.read_text())
    first_fault_ms = int(
        min(window["replay_start"] for window in manifest["ground_truth"]["interference_windows"]) * 1000
    )
    lines = [
        line
        for line in metrics_path.read_text().splitlines()
        if line and int(line.rsplit(" ", 1)[1]) < first_fault_ms
    ]
    if not lines:
        raise ValueError("healthy control produced no pre-interference samples")
    output = output_dir / "control-healthy.prom"
    output.write_text("\n".join(lines) + "\n")
    return output


async def _run_control(case: dict[str, str]) -> dict[str, Any]:
    _reset_and_snapshot_caches()
    provider_registry._provider = None
    user_id = f"gamma-control-{case['id']}"
    with cold_isolation() as state:
        response = await run_pipeline(DashRequest(prompt=case["prompt"], user_id=user_id))
        record = state.history_store.list_recent(limit=1, user_id=user_id)[0]
    generated_queries = record["generated_queries"]
    cause_assertion = _detect_cause_assertion(response.summary, generated_queries)
    return {
        "id": case["id"],
        "kind": case["kind"],
        "family": case["family"],
        "scenario": case["scenario"],
        "prompt": case["prompt"],
        "dashboard_created": bool(response.dashboard_uid),
        "panel_count": response.panel_count,
        "stages": record["stage_outcomes"],
        "evidence_signals": _evidence_signals(generated_queries),
        "cause_assertion": cause_assertion,
        "unsupported_cause_asserted": cause_assertion["asserted"],
        "cache_stats": _cache_stats(),
    }


def _evaluate_controls(controls: dict[str, dict[str, Any]]) -> dict[str, Any]:
    healthy = [control for control in controls.values() if control["kind"] == "healthy"]
    evidence_absent = [control for control in controls.values() if control["kind"] == "evidence_absent"]
    families = {control["family"] for control in controls.values()}
    scenarios = {control["scenario"] for control in controls.values()}
    checks = {
        "healthy_does_not_assert_culprit": all(not control["unsupported_cause_asserted"] for control in healthy),
        "evidence_absent_does_not_assert_resource_culprit": all(
            not control["unsupported_cause_asserted"] for control in evidence_absent
        ),
        "evidence_absent_discovers_symptom": all(
            control["stages"].get("semantic_mapping", {}).get("details", {}).get("coverage", 0.0) > 0
            for control in evidence_absent
        ),
        "control_cache_hits_are_zero": all(
            cache["hits"] == 0
            for control in controls.values()
            for cache in control["cache_stats"].values()
        ),
        "control_sample_size_meets_gate": len(controls) >= MIN_CONTROL_SCENARIOS,
        "control_classes_are_balanced": len(healthy) >= 10 and len(evidence_absent) >= 10,
        "control_scenarios_are_distinct": len(scenarios) == len(controls),
        "control_families_are_diverse": len(families) >= 4,
    }
    symptom_panels = sum(control["panel_count"] > 0 for control in evidence_absent)
    known_gaps = {
        "evidence_absent_preserves_symptom_panel": {
            "numerator": symptom_panels,
            "denominator": len(evidence_absent),
            "recall": symptom_panels / len(evidence_absent) if evidence_absent else 0.0,
        },
        "culprit_ranking_available": all(
            control["stages"].get("ranking", {}).get("status") != "skipped" for control in controls.values()
        ),
    }
    counts = {
        "false_culprit": {
            "numerator": sum(control["unsupported_cause_asserted"] for control in controls.values()),
            "denominator": len(controls),
        },
        "abstention": {
            "numerator": sum(not control["unsupported_cause_asserted"] for control in controls.values()),
            "denominator": len(controls),
        },
    }
    return {
        "checks": checks,
        "counts": counts,
        "required_control_scenarios": MIN_CONTROL_SCENARIOS,
        "known_gaps": known_gaps,
        "passed": all(checks.values()),
    }


def _mapping_coverages(arm: dict[str, Any]) -> list[float]:
    return [
        row["stages"].get("semantic_mapping", {}).get("details", {}).get("coverage", 0.0)
        for row in arm["results"]
    ]


def _evidence_recall(arm: dict[str, Any]) -> dict[str, float | int]:
    denominator = len(arm["results"]) * len(EXPECTED_EVIDENCE_SIGNALS)
    numerator = sum(
        len(EXPECTED_EVIDENCE_SIGNALS.intersection(row["evidence_signals"])) for row in arm["results"]
    )
    return {"numerator": numerator, "denominator": denominator, "recall": numerator / denominator}


def _evaluate_predictions(arms: dict[str, dict[str, Any]], expectation: str = "pre-fix") -> dict[str, Any]:
    canonical = arms["canonical"]
    prefixed = arms["prefixed"]
    raw = arms["raw"]
    canonical_mapping = _mapping_coverages(canonical)
    prefixed_mapping = _mapping_coverages(prefixed)
    canonical_recall = _evidence_recall(canonical)
    prefixed_recall = _evidence_recall(prefixed)
    checks = {
        "canonical_all_prompts_create_dashboard": canonical["dashboards_created"] == canonical["independent_prompts"],
        "prefix_only_preserves_mapping_coverage": canonical_mapping == prefixed_mapping,
        "canonical_evidence_recall_meets_gate": canonical_recall["recall"] >= MIN_EVIDENCE_RECALL,
        "all_arm_cache_hits_are_zero": all(
            cache["hits"] == 0
            for arm in arms.values()
            for row in arm["results"]
            for cache in row["cache_stats"].values()
        ),
    }
    if expectation == "pre-fix":
        checks.update(
            {
                "prefixed_fails_binding_pre_fix": prefixed["dashboards_created"] == 0,
                "raw_fails_binding_pre_fix": raw["dashboards_created"] == 0,
            }
        )
    else:
        checks.update(
            {
                "prefixed_all_prompts_bind_post_fix": (
                    prefixed["dashboards_created"] == prefixed["independent_prompts"]
                ),
                "prefixed_evidence_recall_meets_gate": prefixed_recall["recall"] >= MIN_EVIDENCE_RECALL,
                "raw_ambiguous_binding_abstains": raw["dashboards_created"] == 0,
            }
        )
    return {
        "expectation": expectation,
        "sealed_expected_outcomes": PREDICTED_OUTCOMES[expectation],
        "checks": checks,
        "counts": {
            "canonical_evidence_recall": canonical_recall,
            "prefixed_evidence_recall": prefixed_recall,
            "dashboards": {
                name: {"numerator": arm["dashboards_created"], "denominator": arm["independent_prompts"]}
                for name, arm in arms.items()
            },
        },
        "passed": all(checks.values()),
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = args.workdir
    output_dir.mkdir(parents=True, exist_ok=True)
    arms: dict[str, dict[str, Any]] = {}
    with httpx.Client(timeout=120, trust_env=False) as client:
        for arm in ARMS:
            metrics_path, manifest_path = build(
                args.archive,
                args.scenario,
                "infrastructure",
                output_dir,
                naming=arm,
            )
            _replace_gamma_metrics(client, args.vm_url, metrics_path)
            arm_result = await _run_arm(arm)
            arm_result["manifest"] = str(manifest_path)
            arms[arm] = arm_result

        controls: dict[str, dict[str, Any]] = {}
        for case in CONTROL_CASES:
            case_dir = output_dir / "controls" / case["id"]
            if case["kind"] == "healthy":
                metrics_path, manifest_path = build(
                    args.archive,
                    case["scenario"],
                    "infrastructure",
                    case_dir,
                    naming="canonical",
                )
                metrics_path = _healthy_slice(metrics_path, manifest_path, case_dir)
            else:
                metrics_path, _ = build(
                    args.archive,
                    case["scenario"],
                    "application",
                    case_dir,
                )
            _replace_gamma_metrics(client, args.vm_url, metrics_path)
            controls[case["id"]] = await _run_control(case)

    evaluation = _evaluate_predictions(arms, args.expect)
    control_evaluation = _evaluate_controls(controls)
    return {
        "dataset": "gamma",
        "scenario_id": "gamma-0001",
        "model": args.model,
        "cache_policy": "metric and LLM caches invalidated before every prompt",
        "protocol_fingerprint": _protocol_fingerprint(),
        "arms": arms,
        "controls": controls,
        "prediction_evaluation": evaluation,
        "control_evaluation": control_evaluation,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO)
    parser.add_argument("--workdir", type=Path, default=Path("data/gamma/diagnostic"))
    parser.add_argument("--grafana-url", default="http://127.0.0.1:3001")
    parser.add_argument("--vm-url", default="http://127.0.0.1:8428")
    parser.add_argument("--ollama-url", default="http://127.0.0.1:11434")
    parser.add_argument("--model", default="qwen3-coder:30b-a3b-q4_K_M")
    parser.add_argument("--json", type=Path)
    parser.add_argument("--expect", choices=("pre-fix", "post-fix"), default=DEFAULT_EXPECTATION)
    args = parser.parse_args()
    with _evaluation_settings(args.grafana_url, args.ollama_url, args.model):
        report = asyncio.run(run(args))
    summary = {
        "model": report["model"],
        "cache_policy": report["cache_policy"],
        "dashboards_created": {
            name: f"{arm['dashboards_created']}/{arm['independent_prompts']}" for name, arm in report["arms"].items()
        },
        "prediction_evaluation": report["prediction_evaluation"],
        "control_evaluation": report["control_evaluation"],
    }
    print(json.dumps(summary, indent=2))
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2) + "\n")
        print(f"wrote {args.json}")
    gates_passed = report["prediction_evaluation"]["passed"] and report["control_evaluation"]["passed"]
    return 0 if gates_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
