#!/usr/bin/env python
"""Tacit Validation Suite

Validates archetype classification accuracy and metric selection against a test
dataset. Produces per-category and overall accuracy scores.

Modes:
  archetype  — Tests intent agent problem_type classification (needs LLM, no stack)
  pipeline   — Tests full pipeline metric selection (requires running stack)
  all        — Runs both

Usage:
  python tests/validate.py tests/tacit_validation_prompts.csv --mode archetype
  python tests/validate.py tests/tacit_validation_prompts.csv --mode pipeline
  python tests/validate.py tests/tacit_validation_prompts.csv --mode all
  python tests/validate.py tests/tacit_validation_prompts.csv --mode archetype --limit 10
  python tests/validate.py tests/tacit_validation_prompts.csv --mode all --output results.json
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import hashlib
import importlib
import json
import math
import os
import re
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# ── Project bootstrap ───────────────────────────────────────────────────────
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _PROJECT_ROOT)
os.chdir(_PROJECT_ROOT)


# ── Data classes ────────────────────────────────────────────────────────────


def validate_rate_threshold(value: float | str) -> float:
    """Return one finite release-gate rate in the inclusive [0, 1] range."""
    rate = float(value)
    if not math.isfinite(rate) or not 0 <= rate <= 1:
        raise ValueError("Validation rate threshold must be a finite number between 0 and 1")
    return rate


def _rate_threshold_argument(value: str) -> float:
    try:
        return validate_rate_threshold(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _nonnegative_integer_argument(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


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
    error: str = ""


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


@dataclass(frozen=True)
class ValidationThresholds:
    """Release-quality floors for the public validation suite."""

    min_archetype_accuracy: float = 0.85
    min_archetype_soft_accuracy: float = 0.90
    min_metric_recall: float = 0.75
    min_critical_recall: float = 0.80
    min_weighted_recall: float = 0.78
    min_signal_to_noise: float = 0.65
    max_errors: int = 0
    max_error_rate: float = 0.0

    def __post_init__(self) -> None:
        rates = (
            ("min_archetype_accuracy", self.min_archetype_accuracy),
            ("min_archetype_soft_accuracy", self.min_archetype_soft_accuracy),
            ("min_metric_recall", self.min_metric_recall),
            ("min_critical_recall", self.min_critical_recall),
            ("min_weighted_recall", self.min_weighted_recall),
            ("min_signal_to_noise", self.min_signal_to_noise),
            ("max_error_rate", self.max_error_rate),
        )
        for field_name, rate in rates:
            object.__setattr__(self, field_name, validate_rate_threshold(rate))
        if self.max_errors < 0:
            raise ValueError("Validation max_errors must be non-negative")


@dataclass(frozen=True)
class ValidationGateResult:
    passed: bool
    failures: tuple[str, ...]
    thresholds: ValidationThresholds

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "failures": list(self.failures),
            "thresholds": asdict(self.thresholds),
        }


@dataclass(frozen=True)
class SelectedEvaluationState:
    """One named benchmark state and its disposable runtime, when applicable."""

    mode: str
    fingerprint: str
    isolated_state: Any | None = None
    tenant_id: str = "default"


_CLEAN_STATE_FINGERPRINT_INPUT = b"tacit-validation-state-v1:clean"
_MAX_ADMITTED_STATE_MANIFEST_BYTES = 64 * 1024


def _release_quality_contract() -> Any:
    """Load the CI-owned release evidence contract only for long-lived modes."""
    scripts_directory = Path(_PROJECT_ROOT) / ".github" / "scripts"
    scripts_path = str(scripts_directory)
    if scripts_path not in sys.path:
        sys.path.insert(0, scripts_path)
    return importlib.import_module("release_quality_contract")


LONG_LIVED_STATE_DATABASES = tuple(_release_quality_contract().LONG_LIVED_STATE_DATABASES)
_ADMITTED_STATE_MANIFEST_NAME = str(_release_quality_contract().ADMITTED_STATE_MANIFEST)


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


def normalize_archetype(problem_type: str) -> str | None:
    """Normalize a problem_type to its canonical archetype id."""
    return ARCHETYPE_ALIASES.get(problem_type)


def grafana_request_headers(api_key: str, org_id: int) -> dict[str, str]:
    """Build valid Grafana headers for anonymous and authenticated benchmarks."""
    headers = {"X-Grafana-Org-Id": str(org_id)}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def tacit_request_headers(api_key: str, tenant_id: str) -> tuple[dict[str, str], str]:
    """Build authenticated headers around one concrete tenant boundary."""
    from tacit.config import canonical_knowledge_tenant_id

    concrete_tenant = canonical_knowledge_tenant_id(tenant_id)
    if concrete_tenant == "*":
        raise ValueError("Pipeline validation requires a concrete tenant")
    headers = {"X-Tacit-Tenant": concrete_tenant}
    if api_key:
        headers["X-API-Key"] = api_key
    return headers, concrete_tenant


def _long_lived_state_sources(source_dir: Path) -> tuple[Path, ...]:
    if not source_dir.is_dir() or source_dir.is_symlink():
        raise ValueError("Long-lived state directory must be a real directory")
    sources = tuple(source_dir / name for name in LONG_LIVED_STATE_DATABASES)
    for source in sources:
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"Long-lived state is missing regular database {source.name}")
    return sources


def _state_fingerprint(sources: tuple[Path, ...]) -> str:
    return str(_release_quality_contract().logical_snapshot_fingerprint(sources))


def _admitted_state_fingerprint(
    source_dir: Path,
    manifest_path: Path,
    *,
    tenant_id: str,
) -> str:
    if (
        manifest_path.name != _ADMITTED_STATE_MANIFEST_NAME
        or manifest_path.is_symlink()
        or not manifest_path.is_file()
        or manifest_path.parent.resolve() != source_dir.resolve()
    ):
        raise ValueError("Admitted long-lived state manifest is invalid")
    manifest_size = manifest_path.stat().st_size
    if not 1 <= manifest_size <= _MAX_ADMITTED_STATE_MANIFEST_BYTES:
        raise ValueError("Admitted long-lived state manifest exceeds the size limit")
    try:
        manifest = json.loads(manifest_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Admitted long-lived state manifest is invalid") from exc
    contract = _release_quality_contract()
    try:
        _manifest_tenant, fingerprint, limits = contract.validate_admitted_state_manifest(
            manifest,
            expected_tenant=tenant_id,
            require_release_limits=True,
        )
    except ValueError as exc:
        raise ValueError(str(exc)) from exc
    sources = _long_lived_state_sources(source_dir)
    try:
        contract.validate_snapshot_database_sizes(sources, limits=limits)
    except ValueError as exc:
        raise ValueError(str(exc)) from exc
    if _state_fingerprint(sources) != fingerprint:
        raise ValueError("Admitted long-lived state fingerprint does not match")
    return fingerprint


@contextlib.contextmanager
def evaluation_state(
    mode: str,
    source_dir: str | os.PathLike[str] | None = None,
    *,
    endpoints: Any | None = None,
    tenant_id: str = "default",
    admitted_state_manifest: str | os.PathLike[str] | None = None,
) -> Iterator[SelectedEvaluationState]:
    """Yield an external marker or a disposable clean/long-lived runtime."""
    from tacit.sqlite_identity import snapshot_sqlite_database_set
    from tests.eval.cold_isolation import (
        cold_isolation,
        validate_evaluation_tenant,
        validate_local_evaluation_endpoints,
    )

    if mode not in {"external", "clean", "long-lived"}:
        raise ValueError(f"Unsupported evaluation state: {mode}")
    if mode == "external":
        if source_dir is not None or admitted_state_manifest is not None:
            raise ValueError("External evaluation state does not accept a state directory or admitted manifest")
        yield SelectedEvaluationState(
            mode="external",
            fingerprint=hashlib.sha256(b"tacit-validation-state-v1:external-unpinned").hexdigest(),
            tenant_id=tenant_id,
        )
        return

    selected_endpoints = validate_local_evaluation_endpoints(endpoints)
    concrete_tenant = validate_evaluation_tenant(tenant_id)
    contract = _release_quality_contract()
    sources: tuple[Path, ...] = ()
    if mode == "long-lived":
        if source_dir is None:
            raise ValueError("Long-lived evaluation requires --state-dir")
        sources = _long_lived_state_sources(Path(source_dir))
    else:
        if source_dir is not None or admitted_state_manifest is not None:
            raise ValueError("Clean evaluation does not accept a state directory or admitted manifest")
        fingerprint = hashlib.sha256(_CLEAN_STATE_FINGERPRINT_INPUT).hexdigest()

    if admitted_state_manifest is not None:
        source_path = Path(source_dir) if source_dir is not None else Path()
        fingerprint = _admitted_state_fingerprint(
            source_path,
            Path(admitted_state_manifest),
            tenant_id=concrete_tenant,
        )
        with cold_isolation(source_path, endpoints=selected_endpoints, tenant_id=concrete_tenant) as isolated:
            yield SelectedEvaluationState(
                mode=mode,
                fingerprint=fingerprint,
                isolated_state=isolated,
                tenant_id=concrete_tenant,
            )
        return

    with tempfile.TemporaryDirectory(prefix=f"tacit-validation-{mode}-") as temporary:
        workdir = Path(temporary)
        if sources:
            contract.validate_source_database_sizes(sources, limits=contract.STATE_SNAPSHOT_LIMITS)
            snapshot_sqlite_database_set(
                sources,
                workdir,
                snapshot_max_bytes=contract.STATE_SNAPSHOT_LIMITS.max_database_bytes,
                snapshot_total_max_bytes=contract.STATE_SNAPSHOT_LIMITS.max_total_bytes,
            )
            contract.validate_snapshot_database_sizes(
                tuple(workdir / source.name for source in sources),
                limits=contract.STATE_SNAPSHOT_LIMITS,
            )
        if mode == "long-lived":
            fingerprint = _state_fingerprint(tuple(workdir / source.name for source in sources))
        with cold_isolation(workdir, endpoints=selected_endpoints, tenant_id=concrete_tenant) as isolated:
            yield SelectedEvaluationState(
                mode=mode,
                fingerprint=fingerprint,
                isolated_state=isolated,
                tenant_id=concrete_tenant,
            )


# ── CSV loader ──────────────────────────────────────────────────────────────


def load_test_cases(csv_path: str) -> list[TestCase]:
    """Load test cases from a CSV file. Supports any CSV with the required columns."""
    cases: list[TestCase] = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        required = {
            "prompt_id",
            "prompt",
            "expected_archetype",
            "expected_metrics",
            "expected_datasources",
            "difficulty",
            "validation_goal",
        }
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
            cases.append(
                TestCase(
                    prompt_id=row["prompt_id"].strip(),
                    prompt=row["prompt"].strip(),
                    expected_archetype=row["expected_archetype"].strip(),
                    expected_metrics=metrics,
                    expected_datasources=datasources,
                    difficulty=row["difficulty"].strip(),
                    validation_goal=row["validation_goal"].strip(),
                    critical_metrics=critical,
                )
            )
    return cases


# ── PromQL metric extraction ───────────────────────────────────────────────

_PROMQL_FUNCTIONS = frozenset(
    {
        "abs",
        "absent",
        "absent_over_time",
        "avg",
        "avg_over_time",
        "bottomk",
        "ceil",
        "changes",
        "clamp",
        "clamp_max",
        "clamp_min",
        "count",
        "count_over_time",
        "count_values",
        "day_of_month",
        "day_of_week",
        "days_in_month",
        "delta",
        "deriv",
        "exp",
        "floor",
        "group",
        "histogram_quantile",
        "holt_winters",
        "hour",
        "idelta",
        "increase",
        "irate",
        "label_join",
        "label_replace",
        "last_over_time",
        "ln",
        "log2",
        "log10",
        "max",
        "max_over_time",
        "min",
        "min_over_time",
        "minute",
        "month",
        "predict_linear",
        "quantile",
        "quantile_over_time",
        "rate",
        "resets",
        "round",
        "scalar",
        "sgn",
        "sort",
        "sort_desc",
        "sqrt",
        "stddev",
        "stddev_over_time",
        "stdvar",
        "stdvar_over_time",
        "sum",
        "sum_over_time",
        "time",
        "timestamp",
        "topk",
        "vector",
        "year",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        "bool",
        "offset",
        "le",
        "inf",
    }
)


def extract_metrics_from_expr(expr: str) -> set[str]:
    """Extract metric names from a PromQL expression.

    Identifies metric names that appear before ``{`` or ``[`` (standard PromQL
    positions) and filters out known PromQL functions.
    """
    metrics: set[str] = set()
    # Primary: identifiers immediately before { or [
    for match in re.finditer(r"([a-zA-Z_:][a-zA-Z0-9_:]*)\s*[{\[]", expr):
        name = match.group(1)
        if name.lower() not in _PROMQL_FUNCTIONS:
            metrics.add(name)
    # Secondary: identifiers used as function arguments — strip brace content first
    stripped = re.sub(r"\{[^}]*\}", "{}", expr)
    for match in re.finditer(r"(?<=[(,])\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?=[{\[(])", stripped):
        name = match.group(1)
        if name.lower() not in _PROMQL_FUNCTIONS:
            metrics.add(name)
    return metrics


def fuzzy_metric_match(expected: set[str], found: set[str]) -> set[str]:
    """Return expected metrics matched one-to-one against found metrics."""
    from tests.eval.metric_matching import match_metric_sets

    return set(match_metric_sets(expected, found))


# ── Archetype validation ───────────────────────────────────────────────────


async def run_archetype_validation(
    cases: list[TestCase],
    *,
    provider: Any | None = None,
    runtime_settings: Any | None = None,
) -> list[ArchetypeResult]:
    """Test intent agent problem_type classification accuracy.

    Evaluates both strict (top-1) and soft (any-match) accuracy using
    the multi-label archetypes returned by the intent agent.
    """
    from tacit.agents.intent import classify_intent

    if provider is None:
        raise RuntimeError("Archetype validation requires an owner-managed LLM provider")

    results: list[ArchetypeResult] = []
    total = len(cases)

    for i, case in enumerate(cases, 1):
        t0 = time.monotonic()
        all_archetypes: list[dict] = []
        error = ""
        try:
            intent, _usage = await classify_intent(
                case.prompt,
                provider=provider,
                runtime_settings=runtime_settings,
            )
            actual = intent.problem_type
            all_archetypes = [{"type": a.type, "confidence": a.confidence} for a in intent.archetypes]
        except Exception as e:
            actual = f"ERROR:{e}"
            error = str(e)
        elapsed = (time.monotonic() - t0) * 1000

        expected_norm = normalize_archetype(case.expected_archetype)
        actual_norm = normalize_archetype(actual)
        passed = not error and expected_norm is not None and expected_norm == actual_norm

        # Soft match: does expected match ANY returned archetype?
        any_match = passed
        top_confidence = 0.0
        if all_archetypes:
            top_confidence = all_archetypes[0].get("confidence", 0.0)
            for a in all_archetypes:
                if expected_norm is not None and normalize_archetype(a["type"]) == expected_norm:
                    any_match = True
                    break

        results.append(
            ArchetypeResult(
                prompt_id=case.prompt_id,
                expected=case.expected_archetype,
                actual=actual,
                passed=passed,
                latency_ms=elapsed,
                all_archetypes=all_archetypes,
                any_match=any_match,
                top_confidence=top_confidence,
                error=error,
            )
        )

        # Show multi-label info in output
        arch_str = (
            " ".join(f"{a['type']}({a['confidence']:.2f})" for a in all_archetypes[:3]) if all_archetypes else actual
        )
        icon = "\u2713" if passed else ("\u25b3" if any_match else "\u2717")
        print(
            f"  [{i:3d}/{total}] {icon} {case.prompt_id}: "
            f"expected={case.expected_archetype:25s} top={actual:25s} [{arch_str}] ({elapsed:.0f}ms)"
        )

    return results


# ── Pipeline validation ────────────────────────────────────────────────────


async def run_pipeline_validation(
    cases: list[TestCase],
    api_url: str,
    grafana_url: str,
    *,
    api_key: str = "",
    tenant_id: str = "default",
    grafana_api_key: str | None = None,
    grafana_org_id: int | None = None,
    pipeline_runner: Callable[[TestCase], Awaitable[Any]] | None = None,
) -> list[PipelineResult]:
    """Test full pipeline: metric selection + archetype via the running API."""
    import httpx

    api_headers, concrete_tenant = tacit_request_headers(api_key, tenant_id)
    if grafana_api_key is None or grafana_org_id is None:
        from tacit.config import settings

        if grafana_api_key is None:
            grafana_api_key = settings.grafana_api_key
        if grafana_org_id is None:
            grafana_org_id = settings.grafana_org_id
    results: list[PipelineResult] = []
    total = len(cases)

    async with httpx.AsyncClient(timeout=180, trust_env=False) as client:
        grafana_headers = grafana_request_headers(grafana_api_key, grafana_org_id)

        for i, case in enumerate(cases, 1):
            t0 = time.monotonic()
            try:
                if pipeline_runner is None:
                    resp = await client.post(
                        f"{api_url}/api/v1/chart",
                        json={
                            "prompt": case.prompt,
                            "user_id": "validation",
                            "channel_id": "test",
                            "tenant_id": concrete_tenant,
                        },
                        headers=api_headers,
                    )
                    elapsed = (time.monotonic() - t0) * 1000

                    if resp.status_code != 200:
                        results.append(
                            PipelineResult(
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
                                critical_metrics_expected=list(case.critical_metrics),
                                critical_metrics_missing=list(case.critical_metrics),
                            )
                        )
                        print(
                            f"  [{i:3d}/{total}] \u2717 {case.prompt_id}: "
                            f"API error {resp.status_code} ({elapsed:.0f}ms)"
                        )
                        continue
                    data = resp.json()
                else:
                    response = await pipeline_runner(case)
                    elapsed = (time.monotonic() - t0) * 1000
                    if hasattr(response, "model_dump"):
                        data = response.model_dump(mode="json")
                    elif isinstance(response, dict):
                        data = response
                    else:
                        raise TypeError("Pipeline runner returned an unsupported response")
                dashboard_uid = data.get("dashboard_uid", "")
                dashboard_url = data.get("dashboard_url", "")
                panel_count = data.get("panel_count", 0)

                if not dashboard_uid:
                    results.append(
                        PipelineResult(
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
                            critical_metrics_expected=list(case.critical_metrics),
                            critical_metrics_missing=list(case.critical_metrics),
                        )
                    )
                    print(f"  [{i:3d}/{total}] \u25cb {case.prompt_id}: No dashboard ({elapsed:.0f}ms)")
                    continue

                # Fetch dashboard JSON from Grafana to inspect panel queries
                found_metrics: set[str] = set()
                dashboard_fetch_error = ""
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
                                found_metrics.update(extract_metrics_from_expr(target.get("expr", "")))
                            # Panels nested inside row panels
                            for nested in panel.get("panels", []):
                                for target in nested.get("targets", []):
                                    found_metrics.update(extract_metrics_from_expr(target.get("expr", "")))
                    else:
                        dashboard_fetch_error = (
                            f"Grafana dashboard fetch HTTP {dash_resp.status_code}: {dash_resp.text[:200]}"
                        )
                except Exception as exc:
                    dashboard_fetch_error = f"Grafana dashboard fetch failed: {exc}"

                if dashboard_fetch_error:
                    results.append(
                        PipelineResult(
                            prompt_id=case.prompt_id,
                            expected_metrics=case.expected_metrics,
                            found_metrics=[],
                            missing_metrics=case.expected_metrics,
                            extra_metrics=[],
                            metric_recall=0.0,
                            dashboard_url=dashboard_url,
                            panel_count=panel_count,
                            latency_ms=elapsed,
                            error=dashboard_fetch_error,
                            critical_metrics_expected=case.critical_metrics,
                            critical_metrics_missing=case.critical_metrics,
                        )
                    )
                    print(f"  [{i:3d}/{total}] ✗ {case.prompt_id}: {dashboard_fetch_error} ({elapsed:.0f}ms)")
                    continue

                from tests.eval.metric_matching import match_metric_sets

                expected_set = set(case.expected_metrics)
                metric_matches = match_metric_sets(expected_set, found_metrics)
                matched = set(metric_matches)
                matched_found = set(metric_matches.values())
                missing = sorted(expected_set - matched)
                extra = sorted(found_metrics - matched_found)
                recall = len(matched) / len(expected_set) if expected_set else 1.0

                # Critical metric recall
                critical_set = set(case.critical_metrics) if case.critical_metrics else set()
                critical_matched = fuzzy_metric_match(critical_set, found_metrics) if critical_set else set()
                critical_missing = sorted(critical_set - critical_matched) if critical_set else []
                critical_recall = (
                    len(critical_matched) / len(critical_set) if critical_set else recall  # fall back to overall recall
                )

                # Weighted recall: critical=1.0, supporting=0.4
                if critical_set:
                    supporting_set = expected_set - critical_set
                    supporting_matched = matched - critical_set
                    w_crit = len(critical_matched) * 1.0
                    w_supp = len(supporting_matched) * 0.4
                    w_max = len(critical_set) * 1.0 + len(supporting_set) * 0.4
                    weighted_recall = (w_crit + w_supp) / w_max if w_max > 0 else recall
                else:
                    weighted_recall = recall

                # Signal-to-noise ratio: relevant / (relevant + irrelevant)
                relevant_count = len(matched_found)
                total_found = len(found_metrics)
                snr = relevant_count / total_found if total_found > 0 else 0.0

                results.append(
                    PipelineResult(
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
                    )
                )

                if recall >= 0.5:
                    icon = "\u2713"
                elif recall > 0:
                    icon = "\u25b3"
                else:
                    icon = "\u2717"
                print(
                    f"  [{i:3d}/{total}] {icon} {case.prompt_id}: "
                    f"recall={recall:.0%} found={len(matched)}/{len(expected_set)} "
                    f"panels={panel_count} ({elapsed:.0f}ms)"
                )

            except Exception as e:
                elapsed = (time.monotonic() - t0) * 1000
                results.append(
                    PipelineResult(
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
                        critical_metrics_expected=list(case.critical_metrics),
                        critical_metrics_missing=list(case.critical_metrics),
                    )
                )
                print(f"  [{i:3d}/{total}] \u2717 {case.prompt_id}: {e} ({elapsed:.0f}ms)")

    return results


# ── Reporting ──────────────────────────────────────────────────────────────


def _critical_recall_summary(results: list[PipelineResult]) -> tuple[int, float]:
    """Score frozen critical cases, counting errored cases as zero recall."""
    critical_results = [result for result in results if result.critical_metrics_expected]
    if not critical_results:
        return 0, 0.0
    earned_recall = sum(result.critical_recall for result in critical_results if not result.error)
    return len(critical_results), earned_recall / len(critical_results)


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
    avg_confidence = sum(r.top_confidence for r in results if r.top_confidence > 0) / max(
        1, sum(1 for r in results if r.top_confidence > 0)
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

    print("\n  Per-archetype breakdown (strict / soft):")
    for arch in sorted(by_archetype):
        arch_results = by_archetype[arch]
        arch_passed = sum(1 for r in arch_results if r.passed)
        arch_soft = sum(1 for r in arch_results if r.any_match)
        arch_total = len(arch_results)
        bar = _bar(arch_soft, arch_total, width=20)
        print(f"    {arch:28s} {arch_passed:2d}/{arch_total:2d} strict  {arch_soft:2d}/{arch_total:2d} soft  {bar}")

    # Per-difficulty breakdown
    case_map = {c.prompt_id: c for c in cases}
    by_diff: dict[str, list[ArchetypeResult]] = {}
    for r in results:
        diff = case_map.get(r.prompt_id, cases[0]).difficulty
        by_diff.setdefault(diff, []).append(r)

    print("\n  Per-difficulty breakdown:")
    for diff in ["easy", "medium", "hard"]:
        if diff in by_diff:
            d_results = by_diff[diff]
            d_passed = sum(1 for r in d_results if r.passed)
            d_soft = sum(1 for r in d_results if r.any_match)
            d_total = len(d_results)
            print(f"    {diff:10s} {d_passed:2d}/{d_total:2d} strict  {d_soft:2d}/{d_total:2d} soft")

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
    critical_cases, avg_critical = _critical_recall_summary(results)
    avg_weighted = sum(r.weighted_recall for r in valid) / len(valid) if valid else 0.0
    avg_snr = sum(r.signal_to_noise for r in valid) / len(valid) if valid else 0.0
    full_match = sum(1 for r in valid if r.metric_recall == 1.0)
    partial = sum(1 for r in valid if 0 < r.metric_recall < 1.0)
    no_match = sum(1 for r in valid if r.metric_recall == 0)
    avg_latency = sum(r.latency_ms for r in results) / total if total else 0.0
    has_critical = critical_cases > 0

    print(f"\n{'=' * 72}")
    print("  TIERED PIPELINE EVALUATION REPORT")
    print(f"{'=' * 72}")

    # ── Tier 1: Retrieval Accuracy ─────────────────────────────────────
    print("\n  ── Tier 1: Retrieval Accuracy ──")
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
    print("\n  ── Tier 2: Operational Utility ──")
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

    print("\n  Per-archetype breakdown:")
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
            _, ac = _critical_recall_summary(by_archetype[arch])
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


def evaluate_validation_gate(
    report: dict[str, Any],
    thresholds: ValidationThresholds,
) -> ValidationGateResult:
    """Evaluate every reported mode against explicit quality and error bounds."""
    failures: list[str] = []

    def minimum(section: dict[str, Any], key: str, threshold: float, label: str) -> None:
        actual = float(section.get(key, 0.0))
        if actual < threshold:
            failures.append(f"{label} {actual:.1%} is below {threshold:.1%}")

    def errors(section_name: str, section: dict[str, Any]) -> None:
        total = int(section.get("total", 0))
        count = int(section.get("errors", 0))
        if total <= 0:
            failures.append(f"{section_name} produced no results")
            return
        rate = count / total
        if count > thresholds.max_errors:
            failures.append(f"{section_name} errors {count} exceed {thresholds.max_errors}")
        if rate > thresholds.max_error_rate:
            failures.append(f"{section_name} error rate {rate:.1%} exceeds {thresholds.max_error_rate:.1%}")

    archetype = report.get("archetype")
    if isinstance(archetype, dict):
        minimum(
            archetype,
            "strict_accuracy",
            thresholds.min_archetype_accuracy,
            "Archetype strict accuracy",
        )
        minimum(
            archetype,
            "soft_accuracy",
            thresholds.min_archetype_soft_accuracy,
            "Archetype soft accuracy",
        )
        errors("Archetype", archetype)

    pipeline = report.get("pipeline")
    if isinstance(pipeline, dict):
        minimum(pipeline, "avg_metric_recall", thresholds.min_metric_recall, "Metric recall")
        if int(pipeline.get("critical_cases", 1)) > 0:
            minimum(
                pipeline,
                "avg_critical_recall",
                thresholds.min_critical_recall,
                "Critical recall",
            )
        minimum(
            pipeline,
            "avg_weighted_recall",
            thresholds.min_weighted_recall,
            "Weighted recall",
        )
        minimum(
            pipeline,
            "avg_signal_to_noise",
            thresholds.min_signal_to_noise,
            "Signal-to-noise",
        )
        errors("Pipeline", pipeline)

    if not isinstance(archetype, dict) and not isinstance(pipeline, dict):
        failures.append("Validation produced no scored mode")
    return ValidationGateResult(
        passed=not failures,
        failures=tuple(failures),
        thresholds=thresholds,
    )


def print_gate_report(result: ValidationGateResult) -> None:
    print(f"\n{'=' * 72}")
    print(f"  VALIDATION GATE: {'PASS' if result.passed else 'FAIL'}")
    print(f"{'=' * 72}")
    if result.failures:
        for failure in result.failures:
            print(f"  - {failure}")
    else:
        print("  All configured quality and error thresholds passed.")


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
            print("      Please enter a number or press Enter to skip")


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
        print("  → Open the dashboard in Grafana, then rate it:\n")

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


async def _execute_validation(
    args: argparse.Namespace,
    cases: list[TestCase],
    selected_state: SelectedEvaluationState,
    *,
    provider: Any | None = None,
) -> tuple[dict[str, Any], ValidationGateResult]:
    output: dict[str, Any] = {
        "dataset": args.csv,
        "prompt_count": len(cases),
        "mode": args.mode,
        "state": {
            "mode": selected_state.mode,
            "fingerprint": selected_state.fingerprint,
            "tenant": selected_state.tenant_id,
        },
    }
    isolated = selected_state.isolated_state
    dependencies = isolated.dependencies if isolated is not None else None
    if args.mode in ("archetype", "all") and provider is None:
        raise RuntimeError("Archetype validation requires an owner-managed LLM provider")

    pipe_results: list[PipelineResult] = []
    try:
        if args.mode in ("archetype", "all"):
            print(f"\n{'─' * 72}")
            print("  Running archetype classification validation ...")
            print(f"{'─' * 72}")
            arch_results = await run_archetype_validation(
                cases,
                provider=provider,
                runtime_settings=(
                    isolated.settings if isolated is not None else getattr(provider, "runtime_settings", None)
                ),
            )
            arch_accuracy = print_archetype_report(arch_results, cases)
            soft_passed = sum(1 for result in arch_results if result.any_match)
            output["archetype"] = {
                "strict_accuracy": round(arch_accuracy, 4),
                "soft_accuracy": round(soft_passed / len(arch_results), 4) if arch_results else 0,
                "total": len(arch_results),
                "strict_passed": sum(1 for result in arch_results if result.passed),
                "soft_passed": soft_passed,
                "failed": sum(1 for result in arch_results if not result.passed),
                "errors": sum(1 for result in arch_results if result.error),
                "avg_latency_ms": (
                    round(sum(result.latency_ms for result in arch_results) / len(arch_results), 1)
                    if arch_results
                    else 0
                ),
                "avg_top_confidence": round(
                    sum(result.top_confidence for result in arch_results) / max(1, len(arch_results)),
                    3,
                ),
                "details": [
                    {
                        "prompt_id": result.prompt_id,
                        "expected": result.expected,
                        "actual": result.actual,
                        "passed": result.passed,
                        "any_match": result.any_match,
                        "top_confidence": round(result.top_confidence, 3),
                        "archetypes": result.all_archetypes,
                        "latency_ms": round(result.latency_ms, 1),
                        "error": result.error,
                    }
                    for result in arch_results
                ],
            }

        if args.mode in ("pipeline", "all"):
            print(f"\n{'─' * 72}")
            print("  Running pipeline metric selection validation ...")
            print(f"{'─' * 72}")
            pipeline_runner: Callable[[TestCase], Awaitable[Any]] | None = None
            if dependencies is not None:
                from tacit.models.schemas import DashRequest
                from tacit.pipeline import run_pipeline

                async def run_isolated_pipeline(case: TestCase) -> Any:
                    request = DashRequest(
                        prompt=case.prompt,
                        user_id="validation",
                        channel_id="test",
                        tenant_id=args.tenant,
                    )
                    return await run_pipeline(request, dependencies)

                pipeline_runner = run_isolated_pipeline
            pipe_results = await run_pipeline_validation(
                cases,
                args.api_url,
                args.grafana_url,
                api_key=args.api_key,
                tenant_id=args.tenant,
                grafana_api_key=(isolated.settings.grafana_api_key if isolated is not None else None),
                grafana_org_id=(isolated.settings.grafana_org_id if isolated is not None else None),
                pipeline_runner=pipeline_runner,
            )
            pipe_recall = print_pipeline_report(pipe_results, cases)
            pipe_valid = [result for result in pipe_results if not result.error]
            critical_cases, avg_critical_recall = _critical_recall_summary(pipe_results)
            output["pipeline"] = {
                "avg_metric_recall": round(pipe_recall, 4),
                "avg_critical_recall": round(avg_critical_recall, 4),
                "avg_weighted_recall": (
                    round(sum(result.weighted_recall for result in pipe_valid) / len(pipe_valid), 4)
                    if pipe_valid
                    else 0
                ),
                "avg_signal_to_noise": (
                    round(sum(result.signal_to_noise for result in pipe_valid) / len(pipe_valid), 4)
                    if pipe_valid
                    else 0
                ),
                "total": len(pipe_results),
                "critical_cases": critical_cases,
                "succeeded": len(pipe_valid),
                "errors": sum(1 for result in pipe_results if result.error),
                "avg_latency_ms": (
                    round(sum(result.latency_ms for result in pipe_results) / len(pipe_results), 1)
                    if pipe_results
                    else 0
                ),
                "details": [
                    {
                        "prompt_id": result.prompt_id,
                        "metric_recall": round(result.metric_recall, 4),
                        "critical_recall": round(result.critical_recall, 4),
                        "weighted_recall": round(result.weighted_recall, 4),
                        "signal_to_noise": round(result.signal_to_noise, 4),
                        "found_metrics": result.found_metrics,
                        "missing_metrics": result.missing_metrics,
                        "extra_metrics": result.extra_metrics,
                        "critical_metrics_expected": result.critical_metrics_expected,
                        "critical_metrics_found": result.critical_metrics_found,
                        "critical_metrics_missing": result.critical_metrics_missing,
                        "panel_count": result.panel_count,
                        "dashboard_url": result.dashboard_url,
                        "latency_ms": round(result.latency_ms, 1),
                        "error": result.error,
                    }
                    for result in pipe_results
                ],
            }

        if args.review and args.mode in ("pipeline", "all") and "pipeline" in output:
            reviews = collect_human_reviews(pipe_results, cases)
            if reviews:
                print_review_report(reviews)
                output["human_reviews"] = reviews
    finally:
        # The synchronous composition boundary owns provider retirement.
        provider = None

    thresholds = ValidationThresholds(
        min_archetype_accuracy=args.min_archetype_accuracy,
        min_archetype_soft_accuracy=args.min_archetype_soft_accuracy,
        min_metric_recall=args.min_metric_recall,
        min_critical_recall=args.min_critical_recall,
        min_weighted_recall=args.min_weighted_recall,
        min_signal_to_noise=args.min_signal_to_noise,
        max_errors=args.max_errors,
        max_error_rate=args.max_error_rate,
    )
    gate = evaluate_validation_gate(output, thresholds)
    output["gate"] = gate.as_dict()
    return output, gate


def main(
    argv: list[str] | None = None,
    *,
    runtime_stores: Any | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description="Tacit Validation Suite — test archetype and metric accuracy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python tests/validate.py tests/tacit_validation_prompts.csv --mode archetype
  python tests/validate.py tests/tacit_validation_prompts.csv --mode pipeline --limit 5
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
        help="Tacit API base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--grafana-url",
        default="http://localhost:3000",
        help="Grafana base URL (default: http://localhost:3000)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("TACIT_API_KEY", ""),
        help="Tacit API key for pipeline mode (defaults to TACIT_API_KEY)",
    )
    parser.add_argument(
        "--tenant",
        default="default",
        help="Concrete Tacit tenant sent by pipeline validation (default: default)",
    )
    parser.add_argument(
        "--state",
        choices=["external", "clean", "long-lived"],
        default="external",
        help="Evaluation state: existing API, disposable clean state, or copied long-lived state",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        help="Read-only source directory containing signals.db, history.db, and feedback.db for long-lived state",
    )
    parser.add_argument(
        "--admitted-state-manifest",
        type=Path,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--llm-url",
        default="",
        help="Explicit local Ollama URL required for clean and long-lived states",
    )
    parser.add_argument(
        "--llm-model",
        default="",
        help="Explicit local Ollama model required for clean and long-lived states",
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
    defaults = ValidationThresholds()
    parser.add_argument(
        "--min-archetype-accuracy",
        type=_rate_threshold_argument,
        default=defaults.min_archetype_accuracy,
    )
    parser.add_argument(
        "--min-archetype-soft-accuracy",
        type=_rate_threshold_argument,
        default=defaults.min_archetype_soft_accuracy,
    )
    parser.add_argument("--min-metric-recall", type=_rate_threshold_argument, default=defaults.min_metric_recall)
    parser.add_argument("--min-critical-recall", type=_rate_threshold_argument, default=defaults.min_critical_recall)
    parser.add_argument("--min-weighted-recall", type=_rate_threshold_argument, default=defaults.min_weighted_recall)
    parser.add_argument("--min-signal-to-noise", type=_rate_threshold_argument, default=defaults.min_signal_to_noise)
    parser.add_argument("--max-errors", type=_nonnegative_integer_argument, default=defaults.max_errors)
    parser.add_argument("--max-error-rate", type=_rate_threshold_argument, default=defaults.max_error_rate)
    args = parser.parse_args(argv)

    # Load dataset
    cases = load_test_cases(args.csv)
    if args.limit > 0:
        cases = cases[: args.limit]

    print("\nTacit Validation Suite")
    print(f"Dataset  : {args.csv}")
    print(f"Prompts  : {len(cases)}")
    print(f"Mode     : {args.mode}")
    if args.state != "external" and (not args.llm_url or not args.llm_model):
        raise ValueError("Clean and long-lived validation require --llm-url and --llm-model")
    if args.mode in ("pipeline", "all"):
        _, args.tenant = tacit_request_headers(args.api_key, args.tenant)

    from tests.eval.cold_isolation import LocalEvaluationEndpoints

    endpoints = None
    if args.state != "external":
        endpoints = LocalEvaluationEndpoints(
            grafana_url=args.grafana_url,
            llm_api_base=args.llm_url,
            llm_model=args.llm_model,
        )
    with evaluation_state(
        args.state,
        args.state_dir,
        endpoints=endpoints,
        tenant_id=args.tenant,
        admitted_state_manifest=args.admitted_state_manifest,
    ) as selected_state:
        print(f"State    : {selected_state.mode} ({selected_state.fingerprint[:12]})")
        isolated = selected_state.isolated_state
        dependencies = isolated.dependencies if isolated is not None else None
        if args.mode in ("archetype", "all"):
            from tacit.config import create_settings
            from tacit.dependencies import managed_nonpipeline_llm_provider

            active_settings = (
                dependencies.settings
                if dependencies is not None
                else runtime_stores.settings if runtime_stores is not None else create_settings()
            )
            owner_kwargs = (
                {"dependencies": dependencies} if dependencies is not None else {"runtime_stores": runtime_stores}
            )
            with managed_nonpipeline_llm_provider(
                active_settings,
                **owner_kwargs,
            ) as provider:
                output, gate = asyncio.run(
                    _execute_validation(
                        args,
                        cases,
                        selected_state,
                        provider=provider,
                    )
                )
        else:
            output, gate = asyncio.run(_execute_validation(args, cases, selected_state))

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
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
        print(
            f"  Metric recall      : {p['avg_metric_recall']:.1%}  ({p['succeeded']} succeeded, {p['errors']} errors)"
        )
        if p.get("avg_critical_recall"):
            print(f"  Critical recall    : {p['avg_critical_recall']:.1%}")
            print(f"  Weighted recall    : {p['avg_weighted_recall']:.1%}")
        print(f"  Signal-to-noise    : {p['avg_signal_to_noise']:.1%}")
    if "human_reviews" in output:
        reviews = output["human_reviews"]
        useful = [r["overall_useful"] for r in reviews if r["overall_useful"] is not None]
        if useful:
            print(f"  Human useful rate  : {sum(useful)}/{len(useful)} ({sum(useful) / len(useful):.0%})")
    print(f"{'=' * 72}\n")
    print_gate_report(gate)
    print()
    return 0 if gate.passed else 1


def cli(argv: list[str] | None = None) -> int:
    """Run the validation gate with stable nonzero outcomes."""
    try:
        return main(argv)
    except KeyboardInterrupt:
        print("VALIDATION ERROR: interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"VALIDATION ERROR: {type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(cli())
