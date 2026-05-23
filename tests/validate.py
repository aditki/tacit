#!/usr/bin/env python
"""DashForge Validation Suite

Validates archetype classification accuracy and metric selection against a test
dataset. Produces per-category and overall accuracy scores.

Modes:
  archetype  — Tests intent agent problem_type classification (needs LLM, no stack)
  pipeline   — Tests full pipeline metric selection (requires running stack)
  all        — Runs both

Usage:
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode archetype
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode pipeline
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode all
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode archetype --limit 10
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode all --output results.json
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# ── Project bootstrap ───────────────────────────────────────────────────────
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)


# ── Data classes ────────────────────────────────────────────────────────────

@dataclass
class TestCase:
    prompt_id: str
    prompt: str
    expected_archetype: str
    expected_metrics: list[str]
    expected_datasources: list[str]
    difficulty: str
    validation_goal: str
    critical_metrics: list[str] = field(default_factory=list)


@dataclass
class ArchetypeResult:
    prompt_id: str
    expected: str
    actual: str
    passed: bool
    latency_ms: float
    # Multi-label fields
    all_archetypes: list[dict] = field(default_factory=list)  # [{type, confidence}]
    any_match: bool = False  # True if expected matches ANY returned archetype
    top_confidence: float = 0.0


@dataclass
class PipelineResult:
    prompt_id: str
    expected_metrics: list[str]
    found_metrics: list[str]
    missing_metrics: list[str]
    extra_metrics: list[str]
    metric_recall: float
    dashboard_url: str
    panel_count: int
    latency_ms: float
    archetype_expected: str = ""
    archetype_actual: str = ""
    archetype_passed: bool = False
    error: str = ""
    # Weighted recall fields
    critical_metrics_expected: list[str] = field(default_factory=list)
    critical_metrics_found: list[str] = field(default_factory=list)
    critical_metrics_missing: list[str] = field(default_factory=list)
    critical_recall: float = 0.0
    weighted_recall: float = 0.0
    signal_to_noise: float = 0.0  # relevant / (relevant + irrelevant)


# ── Archetype alias resolution ──────────────────────────────────────────────
# All problem_type values that map to the same canonical archetype are grouped.

ARCHETYPE_ALIASES: dict[str, str] = {
    "latency_investigation": "latency_investigation",
    "slow_requests": "latency_investigation",
    "high_latency": "latency_investigation",
    "p99_spike": "latency_investigation",
    "error_spike": "error_spike",
    "5xx_errors": "error_spike",
    "error_rate": "error_spike",
    "failed_requests": "error_spike",
    "golden_signals": "golden_signals",
    "sre_overview": "golden_signals",
    "service_health": "golden_signals",
    "service_overview": "golden_signals",
    "resource_saturation": "resource_saturation",
    "high_cpu": "resource_saturation",
    "high_memory": "resource_saturation",
    "oom": "resource_saturation",
    "memory_leak": "resource_saturation",
    "cpu_throttling": "resource_saturation",
    "general": "general",
}


def normalize_archetype(problem_type: str) -> str:
    """Normalize a problem_type to its canonical archetype id."""
    return ARCHETYPE_ALIASES.get(problem_type, "general")


# ── CSV loader ──────────────────────────────────────────────────────────────

def load_test_cases(csv_path: str) -> list[TestCase]:
    """Load test cases from a CSV file. Supports any CSV with the required columns."""
    cases: list[TestCase] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {"prompt_id", "prompt", "expected_archetype", "expected_metrics",
                     "expected_datasources", "difficulty", "validation_goal"}
        if not required.issubset(set(reader.fieldnames or [])):
            missing = required - set(reader.fieldnames or [])
            raise ValueError(f"CSV missing columns: {missing}")

        has_critical = "critical_metrics" in (reader.fieldnames or [])

        for row in reader:
            metrics = [m.strip() for m in row["expected_metrics"].split(",") if m.strip()]
            datasources = [d.strip() for d in row["expected_datasources"].split(",") if d.strip()]
            critical = []
            if has_critical and row.get("critical_metrics"):
                critical = [m.strip() for m in row["critical_metrics"].split(";") if m.strip()]
            cases.append(TestCase(
                prompt_id=row["prompt_id"].strip(),
                prompt=row["prompt"].strip(),
                expected_archetype=row["expected_archetype"].strip(),
                expected_metrics=metrics,
                expected_datasources=datasources,
                difficulty=row["difficulty"].strip(),
                validation_goal=row["validation_goal"].strip(),
                critical_metrics=critical,
            ))
    return cases


# ── PromQL metric extraction ───────────────────────────────────────────────

_PROMQL_FUNCTIONS = frozenset({
    "abs", "absent", "absent_over_time", "avg", "avg_over_time", "bottomk",
    "ceil", "changes", "clamp", "clamp_max", "clamp_min", "count",
    "count_over_time", "count_values", "day_of_month", "day_of_week",
    "days_in_month", "delta", "deriv", "exp", "floor", "group",
    "histogram_quantile", "holt_winters", "hour", "idelta", "increase",
    "irate", "label_join", "label_replace", "last_over_time", "ln", "log2",
    "log10", "max", "max_over_time", "min", "min_over_time", "minute",
    "month", "predict_linear", "quantile", "quantile_over_time", "rate",
    "resets", "round", "scalar", "sgn", "sort", "sort_desc", "sqrt",
    "stddev", "stddev_over_time", "stdvar", "stdvar_over_time", "sum",
    "sum_over_time", "time", "timestamp", "topk", "vector", "year",
    "by", "without", "on", "ignoring", "group_left", "group_right", "bool",
    "offset", "le", "inf",
})


def extract_metrics_from_expr(expr: str) -> set[str]:
    """Extract metric names from a PromQL expression.

    Identifies metric names that appear before ``{`` or ``[`` (standard PromQL
    positions) and filters out known PromQL functions.
    """
    metrics: set[str] = set()
    # Primary: identifiers immediately before { or [
    for match in re.finditer(r'([a-zA-Z_:][a-zA-Z0-9_:]*)\s*[{\[]', expr):
        name = match.group(1)
        if name.lower() not in _PROMQL_FUNCTIONS:
            metrics.add(name)
    # Secondary: identifiers used as function arguments — strip brace content first
    stripped = re.sub(r'\{[^}]*\}', '{}', expr)
    for match in re.finditer(r'(?<=[(,])\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?=[{\[(])', stripped):
        name = match.group(1)
        if name.lower() not in _PROMQL_FUNCTIONS:
            metrics.add(name)
    return metrics


def fuzzy_metric_match(expected: set[str], found: set[str]) -> set[str]:
    """Match expected metrics against found using prefix/substring matching.

    Handles histogram suffixes: expected 'http_request_duration_seconds' matches
    found 'http_request_duration_seconds_bucket'.
    """
    matched: set[str] = set()
    for exp in expected:
        for fnd in found:
            if exp == fnd or exp in fnd or fnd in exp:
                matched.add(exp)
                break
    return matched


# ── Archetype validation ───────────────────────────────────────────────────

async def run_archetype_validation(cases: list[TestCase]) -> list[ArchetypeResult]:
    """Test intent agent problem_type classification accuracy.

    Evaluates both strict (top-1) and soft (any-match) accuracy using
    the multi-label archetypes returned by the intent agent.
    """
    from dashforge.agents.intent import classify_intent

    results: list[ArchetypeResult] = []
    total = len(cases)

    for i, case in enumerate(cases, 1):
        t0 = time.monotonic()
        all_archetypes: list[dict] = []
        try:
            intent = await classify_intent(case.prompt)
            actual = intent.problem_type
            all_archetypes = [
                {"type": a.type, "confidence": a.confidence}
                for a in intent.archetypes
            ]
        except Exception as e:
            actual = f"ERROR:{e}"
        elapsed = (time.monotonic() - t0) * 1000

        expected_norm = normalize_archetype(case.expected_archetype)
        actual_norm = normalize_archetype(actual)
        passed = expected_norm == actual_norm

        # Soft match: does expected match ANY returned archetype?
        any_match = passed
        top_confidence = 0.0
        if all_archetypes:
            top_confidence = all_archetypes[0].get("confidence", 0.0)
            for a in all_archetypes:
                if normalize_archetype(a["type"]) == expected_norm:
                    any_match = True
                    break

        results.append(ArchetypeResult(
            prompt_id=case.prompt_id,
            expected=case.expected_archetype,
            actual=actual,
            passed=passed,
            latency_ms=elapsed,
            all_archetypes=all_archetypes,
            any_match=any_match,
            top_confidence=top_confidence,
        ))

        # Show multi-label info in output
        arch_str = " ".join(
            f"{a['type']}({a['confidence']:.2f})" for a in all_archetypes[:3]
        ) if all_archetypes else actual
        icon = "\u2713" if passed else ("\u25b3" if any_match else "\u2717")
        print(f"  [{i:3d}/{total}] {icon} {case.prompt_id}: "
              f"expected={case.expected_archetype:25s} top={actual:25s} [{arch_str}] ({elapsed:.0f}ms)")

    return results


# ── Pipeline validation ────────────────────────────────────────────────────

async def run_pipeline_validation(
    cases: list[TestCase],
    api_url: str,
    grafana_url: str,
) -> list[PipelineResult]:
    """Test full pipeline: metric selection + archetype via the running API."""
    import httpx
    from dashforge.config import settings

    results: list[PipelineResult] = []
    total = len(cases)

    async with httpx.AsyncClient(timeout=180) as client:
        grafana_headers = {
            "Authorization": f"Bearer {settings.grafana_api_key}",
            "X-Grafana-Org-Id": str(settings.grafana_org_id),
        }

        for i, case in enumerate(cases, 1):
            t0 = time.monotonic()
            try:
                resp = await client.post(
                    f"{api_url}/api/v1/chart",
                    json={"prompt": case.prompt, "user_id": "validation", "channel_id": "test"},
                )
                elapsed = (time.monotonic() - t0) * 1000

                if resp.status_code != 200:
                    results.append(PipelineResult(
                        prompt_id=case.prompt_id,
                        expected_metrics=case.expected_metrics,
                        found_metrics=[],
                        missing_metrics=case.expected_metrics,
                        extra_metrics=[],
                        metric_recall=0.0,
                        dashboard_url="",
                        panel_count=0,
                        latency_ms=elapsed,
                        error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                    ))
                    print(f"  [{i:3d}/{total}] \u2717 {case.prompt_id}: "
                          f"API error {resp.status_code} ({elapsed:.0f}ms)")
                    continue

                data = resp.json()
                dashboard_uid = data.get("dashboard_uid", "")
                dashboard_url = data.get("dashboard_url", "")
                panel_count = data.get("panel_count", 0)

                if not dashboard_uid:
                    results.append(PipelineResult(
                        prompt_id=case.prompt_id,
                        expected_metrics=case.expected_metrics,
                        found_metrics=[],
                        missing_metrics=case.expected_metrics,
                        extra_metrics=[],
                        metric_recall=0.0,
                        dashboard_url="",
                        panel_count=0,
                        latency_ms=elapsed,
                        error="No dashboard created",
                    ))
                    print(f"  [{i:3d}/{total}] \u25cb {case.prompt_id}: "
                          f"No dashboard ({elapsed:.0f}ms)")
                    continue

                # Fetch dashboard JSON from Grafana to inspect panel queries
                found_metrics: set[str] = set()
                try:
                    dash_resp = await client.get(
                        f"{grafana_url}/api/dashboards/uid/{dashboard_uid}",
                        headers=grafana_headers,
                    )
                    if dash_resp.status_code == 200:
                        dash_json = dash_resp.json()
                        panels = dash_json.get("dashboard", {}).get("panels", [])
                        for panel in panels:
                            for target in panel.get("targets", []):
                                found_metrics.update(
                                    extract_metrics_from_expr(target.get("expr", ""))
                                )
                            # Panels nested inside row panels
                            for nested in panel.get("panels", []):
                                for target in nested.get("targets", []):
                                    found_metrics.update(
                                        extract_metrics_from_expr(target.get("expr", ""))
                                    )
                except Exception:
                    pass  # grafana fetch failure — metrics stay empty

                expected_set = set(case.expected_metrics)
                matched = fuzzy_metric_match(expected_set, found_metrics)
                missing = sorted(expected_set - matched)
                extra = sorted(found_metrics - expected_set)
                recall = len(matched) / len(expected_set) if expected_set else 1.0

                # Critical metric recall
                critical_set = set(case.critical_metrics) if case.critical_metrics else set()
                critical_matched = fuzzy_metric_match(critical_set, found_metrics) if critical_set else set()
                critical_missing = sorted(critical_set - critical_matched) if critical_set else []
                critical_recall = (
                    len(critical_matched) / len(critical_set)
                    if critical_set else recall  # fall back to overall recall
                )

                # Weighted recall: critical=1.0, supporting=0.4
                if critical_set:
                    supporting_set = expected_set - critical_set
                    supporting_matched = matched - critical_matched
                    w_crit = len(critical_matched) * 1.0
                    w_supp = len(supporting_matched) * 0.4
                    w_max = len(critical_set) * 1.0 + len(supporting_set) * 0.4
                    weighted_recall = (w_crit + w_supp) / w_max if w_max > 0 else recall
                else:
                    weighted_recall = recall

                # Signal-to-noise ratio: relevant / (relevant + irrelevant)
                relevant_count = len(matched)
                total_found = len(found_metrics)
                snr = relevant_count / total_found if total_found > 0 else 0.0

                results.append(PipelineResult(
                    prompt_id=case.prompt_id,
                    expected_metrics=case.expected_metrics,
                    found_metrics=sorted(found_metrics),
                    missing_metrics=missing,
                    extra_metrics=extra,
                    metric_recall=recall,
                    dashboard_url=dashboard_url,
                    panel_count=panel_count,
                    latency_ms=elapsed,
                    critical_metrics_expected=case.critical_metrics,
                    critical_metrics_found=sorted(critical_matched),
                    critical_metrics_missing=critical_missing,
                    critical_recall=critical_recall,
                    weighted_recall=weighted_recall,
                    signal_to_noise=snr,
                ))

                if recall >= 0.5:
                    icon = "\u2713"
                elif recall > 0:
                    icon = "\u25b3"
                else:
                    icon = "\u2717"
                print(f"  [{i:3d}/{total}] {icon} {case.prompt_id}: "
                      f"recall={recall:.0%} found={len(matched)}/{len(expected_set)} "
                      f"panels={panel_count} ({elapsed:.0f}ms)")

            except Exception as e:
                elapsed = (time.monotonic() - t0) * 1000
                results.append(PipelineResult(
                    prompt_id=case.prompt_id,
                    expected_metrics=case.expected_metrics,
                    found_metrics=[],
                    missing_metrics=case.expected_metrics,
                    extra_metrics=[],
                    metric_recall=0.0,
                    dashboard_url="",
                    panel_count=0,
                    latency_ms=elapsed,
                    error=str(e),
                ))
                print(f"  [{i:3d}/{total}] \u2717 {case.prompt_id}: {e} ({elapsed:.0f}ms)")

    return results


# ── Reporting ──────────────────────────────────────────────────────────────

def print_archetype_report(
    results: list[ArchetypeResult],
    cases: list[TestCase],
) -> float:
    """Print archetype classification report. Returns overall accuracy."""
    passed = sum(1 for r in results if r.passed)
    soft_passed = sum(1 for r in results if r.any_match)
    total = len(results)
    accuracy = passed / total if total else 0.0
    soft_accuracy = soft_passed / total if total else 0.0
    avg_latency = sum(r.latency_ms for r in results) / total if total else 0.0
    avg_confidence = (
        sum(r.top_confidence for r in results if r.top_confidence > 0)
        / max(1, sum(1 for r in results if r.top_confidence > 0))
    )

    print(f"\n{'=' * 72}")
    print("  ARCHETYPE CLASSIFICATION REPORT")
    print(f"{'=' * 72}")
    print(f"  Strict accuracy (top-1) : {passed}/{total} ({accuracy:.1%})")
    print(f"  Soft accuracy (any-match): {soft_passed}/{total} ({soft_accuracy:.1%})")
    print(f"  Avg top confidence       : {avg_confidence:.2f}")
    print(f"  Avg latency              : {avg_latency:.0f}ms")

    # Per-archetype breakdown
    by_archetype: dict[str, list[ArchetypeResult]] = {}
    for r in results:
        by_archetype.setdefault(r.expected, []).append(r)

    print(f"\n  Per-archetype breakdown (strict / soft):")
    for arch in sorted(by_archetype):
        arch_results = by_archetype[arch]
        arch_passed = sum(1 for r in arch_results if r.passed)
        arch_soft = sum(1 for r in arch_results if r.any_match)
        arch_total = len(arch_results)
        bar = _bar(arch_soft, arch_total, width=20)
        print(f"    {arch:28s} {arch_passed:2d}/{arch_total:2d} strict  "
              f"{arch_soft:2d}/{arch_total:2d} soft  {bar}")

    # Per-difficulty breakdown
    case_map = {c.prompt_id: c for c in cases}
    by_diff: dict[str, list[ArchetypeResult]] = {}
    for r in results:
        diff = case_map.get(r.prompt_id, cases[0]).difficulty
        by_diff.setdefault(diff, []).append(r)

    print(f"\n  Per-difficulty breakdown:")
    for diff in ["easy", "medium", "hard"]:
        if diff in by_diff:
            d_results = by_diff[diff]
            d_passed = sum(1 for r in d_results if r.passed)
            d_soft = sum(1 for r in d_results if r.any_match)
            d_total = len(d_results)
            print(f"    {diff:10s} {d_passed:2d}/{d_total:2d} strict  "
                  f"{d_soft:2d}/{d_total:2d} soft")

    # Strict failures
    failures = [r for r in results if not r.passed]
    if failures:
        print(f"\n  Strict failures ({len(failures)}):")
        for r in failures:
            archs = " ".join(f"{a['type']}({a['confidence']:.2f})" for a in r.all_archetypes[:4])
            soft_tag = " [soft-match]" if r.any_match else ""
            print(f"    {r.prompt_id}: expected={r.expected:25s} actual={r.actual}{soft_tag}")
            if archs:
                print(f"      archetypes: {archs}")

    print()
    return accuracy


def print_pipeline_report(
    results: list[PipelineResult],
    cases: list[TestCase],
) -> float:
    """Print tiered pipeline evaluation report. Returns avg recall."""
    valid = [r for r in results if not r.error]
    errored = [r for r in results if r.error]
    total = len(results)

    avg_recall = sum(r.metric_recall for r in valid) / len(valid) if valid else 0.0
    avg_critical = sum(r.critical_recall for r in valid) / len(valid) if valid else 0.0
    avg_weighted = sum(r.weighted_recall for r in valid) / len(valid) if valid else 0.0
    avg_snr = sum(r.signal_to_noise for r in valid) / len(valid) if valid else 0.0
    full_match = sum(1 for r in valid if r.metric_recall == 1.0)
    partial = sum(1 for r in valid if 0 < r.metric_recall < 1.0)
    no_match = sum(1 for r in valid if r.metric_recall == 0)
    avg_latency = sum(r.latency_ms for r in results) / total if total else 0.0
    has_critical = any(r.critical_metrics_expected for r in valid)

    print(f"\n{'=' * 72}")
    print("  TIERED PIPELINE EVALUATION REPORT")
    print(f"{'=' * 72}")

    # ── Tier 1: Retrieval Accuracy ─────────────────────────────────────
    print(f"\n  ── Tier 1: Retrieval Accuracy ──")
    print(f"  Avg metric recall    : {avg_recall:.1%}")
    if has_critical:
        print(f"  Avg critical recall  : {avg_critical:.1%}")
        print(f"  Avg weighted recall  : {avg_weighted:.1%}")
    print(f"  Avg signal-to-noise  : {avg_snr:.1%}")
    print(f"  Full match (100%)    : {full_match}")
    print(f"  Partial match        : {partial}")
    print(f"  No match (0%)        : {no_match}")

    # ── Tier 2: Operational Utility ────────────────────────────────────
    avg_panels = sum(r.panel_count for r in valid) / len(valid) if valid else 0.0
    print(f"\n  ── Tier 2: Operational Utility ──")
    print(f"  Total prompts        : {total}")
    print(f"  Succeeded            : {len(valid)}")
    print(f"  Errors               : {len(errored)}")
    print(f"  Avg panels/dashboard : {avg_panels:.1f}")
    print(f"  Avg latency          : {avg_latency:.0f}ms")

    # Per-archetype breakdown
    case_map = {c.prompt_id: c for c in cases}
    by_archetype: dict[str, list[PipelineResult]] = {}
    for r in results:
        arch = case_map.get(r.prompt_id, cases[0]).expected_archetype
        by_archetype.setdefault(arch, []).append(r)

    print(f"\n  Per-archetype breakdown:")
    header = f"    {'archetype':28s} {'recall':>7s}"
    if has_critical:
        header += f"  {'critical':>8s}  {'weighted':>8s}"
    header += f"  {'SNR':>5s}  {'n':>3s}"
    print(header)
    print(f"    {'─' * 68}")
    for arch in sorted(by_archetype):
        arch_valid = [r for r in by_archetype[arch] if not r.error]
        if arch_valid:
            ar = sum(r.metric_recall for r in arch_valid) / len(arch_valid)
            ac = sum(r.critical_recall for r in arch_valid) / len(arch_valid)
            aw = sum(r.weighted_recall for r in arch_valid) / len(arch_valid)
            asnr = sum(r.signal_to_noise for r in arch_valid) / len(arch_valid)
            line = f"    {arch:28s} {ar:6.0%}"
            if has_critical:
                line += f"  {ac:7.0%}  {aw:7.0%}"
            line += f"  {asnr:4.0%}  {len(arch_valid):3d}"
            print(line)

    # Critical metric misses (most important failures)
    if has_critical:
        critical_misses = [r for r in valid if r.critical_metrics_missing]
        if critical_misses:
            print(f"\n  Critical metric misses ({len(critical_misses)}):")
            for r in critical_misses[:15]:
                print(f"    {r.prompt_id}: missing {', '.join(r.critical_metrics_missing)}")

    if errored:
        print(f"\n  Errors ({len(errored)}):")
        for r in errored:
            print(f"    {r.prompt_id}: {r.error[:100]}")

    print()
    return avg_recall


def _bar(filled: int, total: int, width: int = 20) -> str:
    """Render a simple text progress bar."""
    if total == 0:
        return "[" + " " * width + "]"
    n = int(filled / total * width)
    return "[" + "\u2588" * n + "\u2591" * (width - n) + "]"


# ── Human review ──────────────────────────────────────────────────────────

def _prompt_rating(prompt_text: str, min_val: int = 1, max_val: int = 5) -> int | None:
    """Prompt reviewer for a rating. Returns None on skip (empty input)."""
    while True:
        raw = input(f"    {prompt_text} ({min_val}-{max_val}, Enter to skip): ").strip()
        if not raw:
            return None
        try:
            val = int(raw)
            if min_val <= val <= max_val:
                return val
            print(f"      Please enter {min_val}-{max_val}")
        except ValueError:
            print(f"      Please enter a number or press Enter to skip")


def _prompt_bool(prompt_text: str) -> bool | None:
    """Prompt reviewer for yes/no. Returns None on skip."""
    raw = input(f"    {prompt_text} (y/n, Enter to skip): ").strip().lower()
    if not raw:
        return None
    return raw in ("y", "yes", "1", "true")


def collect_human_reviews(
    results: list[PipelineResult],
    cases: list[TestCase],
) -> list[dict]:
    """Interactive review loop — reviewer rates each pipeline result.

    Dimensions:
    - Symptom visibility (1-5): Did the dashboard surface the symptom?
    - Root cause support (1-5): Did it help identify root cause?
    - Noise level (1-5): How much irrelevant info? (1=noisy, 5=all signal)
    - Investigation speed (1-5): Did it accelerate the investigation?
    - Overall useful (y/n): Would you use this in a real incident?
    - Comment (free text)
    """
    case_map = {c.prompt_id: c for c in cases}
    reviews: list[dict] = []
    valid_results = [r for r in results if not r.error and r.dashboard_url]

    print(f"\n{'=' * 72}")
    print("  HUMAN REVIEW MODE")
    print(f"  {len(valid_results)} dashboards to review. Enter 'q' at any prompt to stop.")
    print(f"{'=' * 72}")

    for i, r in enumerate(valid_results, 1):
        case = case_map.get(r.prompt_id)
        prompt_text = case.prompt if case else "(unknown)"

        print(f"\n  ── [{i}/{len(valid_results)}] {r.prompt_id} ──")
        print(f"  Prompt : {prompt_text[:100]}")
        print(f"  URL    : {r.dashboard_url}")
        print(f"  Panels : {r.panel_count}  |  Recall: {r.metric_recall:.0%}  |  SNR: {r.signal_to_noise:.0%}")
        if r.missing_metrics:
            print(f"  Missing: {', '.join(r.missing_metrics[:5])}")
        print(f"  → Open the dashboard in Grafana, then rate it:\n")

        try:
            symptom = _prompt_rating("Symptom visibility")
            root_cause = _prompt_rating("Root cause support")
            noise = _prompt_rating("Noise level (1=noisy, 5=clean)")
            speed = _prompt_rating("Investigation speed")
            useful = _prompt_bool("Overall useful?")
            comment = input("    Comment (Enter to skip): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Review stopped.")
            break

        # Check for quit
        if comment.lower() == "q":
            print("  Review stopped.")
            break

        review = {
            "prompt_id": r.prompt_id,
            "dashboard_uid": r.dashboard_url.split("/d/")[-1].split("/")[0] if "/d/" in r.dashboard_url else "",
            "dashboard_url": r.dashboard_url,
            "symptom_visibility": symptom,
            "root_cause_support": root_cause,
            "noise_level": noise,
            "investigation_speed": speed,
            "overall_useful": useful,
            "comment": comment,
            "metric_recall": round(r.metric_recall, 4),
            "signal_to_noise": round(r.signal_to_noise, 4),
        }
        reviews.append(review)

    return reviews


def print_review_report(reviews: list[dict]) -> None:
    """Print aggregate human review statistics."""
    if not reviews:
        return

    print(f"\n{'=' * 72}")
    print("  HUMAN REVIEW SUMMARY")
    print(f"{'=' * 72}")
    print(f"  Dashboards reviewed: {len(reviews)}")

    for dim, label in [
        ("symptom_visibility", "Symptom visibility"),
        ("root_cause_support", "Root cause support"),
        ("noise_level", "Noise level"),
        ("investigation_speed", "Investigation speed"),
    ]:
        vals = [r[dim] for r in reviews if r[dim] is not None]
        if vals:
            avg = sum(vals) / len(vals)
            print(f"  Avg {label:22s}: {avg:.1f}/5  (n={len(vals)})")

    useful_vals = [r["overall_useful"] for r in reviews if r["overall_useful"] is not None]
    if useful_vals:
        rate = sum(useful_vals) / len(useful_vals)
        print(f"  Overall useful rate      : {rate:.0%}  ({sum(useful_vals)}/{len(useful_vals)})")

    comments = [r for r in reviews if r.get("comment")]
    if comments:
        print(f"\n  Comments ({len(comments)}):")
        for r in comments:
            print(f"    {r['prompt_id']}: {r['comment'][:100]}")

    print()


# ── Main ───────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="DashForge Validation Suite — test archetype and metric accuracy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode archetype
  python tests/validate.py tests/dashforge_validation_prompts.csv --mode pipeline --limit 5
  python tests/validate.py my_custom_dataset.csv --mode all --output results.json
""",
    )
    parser.add_argument("csv", help="Path to validation prompts CSV file")
    parser.add_argument(
        "--mode",
        choices=["archetype", "pipeline", "all"],
        default="all",
        help="Validation mode (default: all)",
    )
    parser.add_argument(
        "--api-url",
        default="http://localhost:8000",
        help="DashForge API base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--grafana-url",
        default="http://localhost:3000",
        help="Grafana base URL (default: http://localhost:3000)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of prompts to validate (0 = all)",
    )
    parser.add_argument(
        "--output",
        help="Save detailed results to a JSON file",
    )
    parser.add_argument(
        "--review",
        action="store_true",
        help="Interactive human review mode — after each pipeline result, "
        "prompt the reviewer to rate the dashboard on multiple dimensions",
    )
    args = parser.parse_args()

    # Load dataset
    cases = load_test_cases(args.csv)
    if args.limit > 0:
        cases = cases[: args.limit]

    print(f"\nDashForge Validation Suite")
    print(f"Dataset  : {args.csv}")
    print(f"Prompts  : {len(cases)}")
    print(f"Mode     : {args.mode}")

    output: dict = {"dataset": args.csv, "prompt_count": len(cases), "mode": args.mode}

    # ── Archetype validation ────────────────────────────────────────────
    if args.mode in ("archetype", "all"):
        print(f"\n{'─' * 72}")
        print(f"  Running archetype classification validation ...")
        print(f"{'─' * 72}")
        arch_results = await run_archetype_validation(cases)
        arch_accuracy = print_archetype_report(arch_results, cases)
        soft_passed = sum(1 for r in arch_results if r.any_match)
        output["archetype"] = {
            "strict_accuracy": round(arch_accuracy, 4),
            "soft_accuracy": round(soft_passed / len(arch_results), 4) if arch_results else 0,
            "total": len(arch_results),
            "strict_passed": sum(1 for r in arch_results if r.passed),
            "soft_passed": soft_passed,
            "failed": sum(1 for r in arch_results if not r.passed),
            "avg_latency_ms": round(
                sum(r.latency_ms for r in arch_results) / len(arch_results), 1
            ),
            "avg_top_confidence": round(
                sum(r.top_confidence for r in arch_results) / max(1, len(arch_results)), 3
            ),
            "details": [
                {
                    "prompt_id": r.prompt_id,
                    "expected": r.expected,
                    "actual": r.actual,
                    "passed": r.passed,
                    "any_match": r.any_match,
                    "top_confidence": round(r.top_confidence, 3),
                    "archetypes": r.all_archetypes,
                    "latency_ms": round(r.latency_ms, 1),
                }
                for r in arch_results
            ],
        }

    # ── Pipeline validation ─────────────────────────────────────────────
    if args.mode in ("pipeline", "all"):
        print(f"\n{'─' * 72}")
        print(f"  Running pipeline metric selection validation ...")
        print(f"{'─' * 72}")
        pipe_results = await run_pipeline_validation(cases, args.api_url, args.grafana_url)
        pipe_recall = print_pipeline_report(pipe_results, cases)
        pipe_valid = [r for r in pipe_results if not r.error]
        output["pipeline"] = {
            "avg_metric_recall": round(pipe_recall, 4),
            "avg_critical_recall": round(
                sum(r.critical_recall for r in pipe_valid) / len(pipe_valid), 4
            ) if pipe_valid else 0,
            "avg_weighted_recall": round(
                sum(r.weighted_recall for r in pipe_valid) / len(pipe_valid), 4
            ) if pipe_valid else 0,
            "avg_signal_to_noise": round(
                sum(r.signal_to_noise for r in pipe_valid) / len(pipe_valid), 4
            ) if pipe_valid else 0,
            "total": len(pipe_results),
            "succeeded": len(pipe_valid),
            "errors": sum(1 for r in pipe_results if r.error),
            "avg_latency_ms": round(
                sum(r.latency_ms for r in pipe_results) / len(pipe_results), 1
            )
            if pipe_results
            else 0,
            "details": [
                {
                    "prompt_id": r.prompt_id,
                    "metric_recall": round(r.metric_recall, 4),
                    "critical_recall": round(r.critical_recall, 4),
                    "weighted_recall": round(r.weighted_recall, 4),
                    "signal_to_noise": round(r.signal_to_noise, 4),
                    "found_metrics": r.found_metrics,
                    "missing_metrics": r.missing_metrics,
                    "extra_metrics": r.extra_metrics,
                    "critical_metrics_expected": r.critical_metrics_expected,
                    "critical_metrics_found": r.critical_metrics_found,
                    "critical_metrics_missing": r.critical_metrics_missing,
                    "panel_count": r.panel_count,
                    "dashboard_url": r.dashboard_url,
                    "latency_ms": round(r.latency_ms, 1),
                    "error": r.error,
                }
                for r in pipe_results
            ],
        }

    # ── Human review (interactive) ────────────────────────────────────
    if args.review and args.mode in ("pipeline", "all") and "pipeline" in output:
        reviews = collect_human_reviews(pipe_results, cases)
        if reviews:
            print_review_report(reviews)
            output["human_reviews"] = reviews

    # ── Save results ────────────────────────────────────────────────────
    if args.output:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"  Results saved to {args.output}")

    # ── Final summary ───────────────────────────────────────────────────
    print(f"\n{'=' * 72}")
    print("  VALIDATION SUMMARY")
    print(f"{'=' * 72}")
    if "archetype" in output:
        a = output["archetype"]
        print(f"  Archetype strict   : {a['strict_passed']}/{a['total']} ({a['strict_accuracy']:.1%})")
        print(f"  Archetype soft     : {a['soft_passed']}/{a['total']} ({a['soft_accuracy']:.1%})")
        print(f"  Avg confidence     : {a['avg_top_confidence']:.2f}")
    if "pipeline" in output:
        p = output["pipeline"]
        print(f"  Metric recall      : {p['avg_metric_recall']:.1%}  "
              f"({p['succeeded']} succeeded, {p['errors']} errors)")
        if p.get("avg_critical_recall"):
            print(f"  Critical recall    : {p['avg_critical_recall']:.1%}")
            print(f"  Weighted recall    : {p['avg_weighted_recall']:.1%}")
        print(f"  Signal-to-noise    : {p['avg_signal_to_noise']:.1%}")
    if "human_reviews" in output:
        reviews = output["human_reviews"]
        useful = [r["overall_useful"] for r in reviews if r["overall_useful"] is not None]
        if useful:
            print(f"  Human useful rate  : {sum(useful)}/{len(useful)} ({sum(useful)/len(useful):.0%})")
    print(f"{'=' * 72}\n")


if __name__ == "__main__":
    asyncio.run(main())
