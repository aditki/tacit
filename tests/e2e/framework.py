"""Reusable helpers for upload-to-incident E2E tests.

The framework is intentionally hermetic: it drives the real ingestion and
pipeline code, but swaps Grafana/LLM calls for deterministic fakes so the score
means "did Tacit preserve the incident investigation path?" rather than
"was a dev stack available today?"
"""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from tacit.backends.base import DashboardFeatures, DiscoveryStatus, PublishResult
from tacit.dashboard_ingest import extract_metrics_from_promql
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry


@dataclass(frozen=True)
class IncidentPromptCase:
    case_id: str
    prompt: str
    service: str
    failure_mode: str
    expected_metrics: list[str]
    critical_metrics: list[str]


@dataclass
class IncidentEvaluation:
    found_metrics: set[str]
    matched_metrics: set[str]
    missing_metrics: set[str]
    critical_found: set[str]
    critical_missing: set[str]
    metric_recall: float
    critical_recall: float
    signal_to_noise: float
    usefulness_score: float

    def assert_passes(self, thresholds: dict[str, float], *, case_id: str) -> None:
        failures = []
        if self.metric_recall < thresholds["min_metric_recall"]:
            failures.append(f"metric_recall={self.metric_recall:.0%}")
        if self.critical_recall < thresholds["min_critical_recall"]:
            failures.append(f"critical_recall={self.critical_recall:.0%}")
        if self.signal_to_noise < thresholds["min_signal_to_noise"]:
            failures.append(f"signal_to_noise={self.signal_to_noise:.0%}")
        if self.usefulness_score < thresholds["min_usefulness_score"]:
            failures.append(f"usefulness_score={self.usefulness_score:.2f}")
        assert not failures, (
            f"{case_id} failed incident utility gate: {', '.join(failures)}; "
            f"missing={sorted(self.missing_metrics)}; critical_missing={sorted(self.critical_missing)}; "
            f"found={sorted(self.found_metrics)}"
        )


@dataclass
class CapturingBackend:
    catalog: list[MetricEntry]
    published_specs: list[DashboardSpec] = field(default_factory=list)
    last_discovery_status: DiscoveryStatus = field(default_factory=DiscoveryStatus)

    @property
    def name(self) -> str:
        return "grafana"

    @property
    def query_language(self) -> str:
        return "promql"

    async def discover_metrics(self, keywords: list[str], intent: Intent) -> list[MetricEntry]:
        del keywords, intent
        self.last_discovery_status = DiscoveryStatus(
            available=True,
            datasource_count=1,
            searchable_datasource_count=1,
        )
        return self.catalog

    async def discover_datasource_targets(self, keywords: list[str], intent: Intent) -> list[MetricEntry]:
        del keywords, intent
        return []

    async def validate_queries(
        self,
        spec: DashboardSpec,
        catalog: list[MetricEntry] | None = None,
    ) -> tuple[DashboardSpec, list[str]]:
        del catalog
        return spec, []

    async def publish(self, spec: DashboardSpec) -> PublishResult:
        self.published_specs.append(spec)
        uid = f"e2e-{len(self.published_specs)}"
        return PublishResult(url=f"http://grafana.example/d/{uid}", uid=uid, backend_name=self.name)

    async def ingest_dashboard(self, uid: str) -> DashboardFeatures:
        raise NotImplementedError(f"{uid} should be uploaded through /learn/dashboard/json in this suite")

    async def close(self) -> None:
        return None


def load_scenario(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_grafana_dashboard(scenario: dict[str, Any]) -> dict[str, Any]:
    dashboard = scenario["dashboard"]
    panels = []
    for idx, panel in enumerate(dashboard["panels"], start=1):
        panels.append(
            {
                "id": idx,
                "type": "timeseries",
                "title": panel["title"],
                "datasource": {"type": "prometheus", "uid": "prom-e2e"},
                "fieldConfig": {"defaults": {"unit": panel.get("unit", "")}},
                "targets": [
                    {
                        "expr": panel["expr"],
                        "legendFormat": "{{service}}",
                        "datasource": {"type": "prometheus", "uid": "prom-e2e"},
                    }
                ],
                "gridPos": {"x": 0, "y": (idx - 1) * 8, "w": 12, "h": 8},
            }
        )
    return {
        "dashboard": {
            "uid": dashboard["uid"],
            "title": dashboard["title"],
            "tags": dashboard.get("tags", []),
            "panels": panels,
        }
    }


def scenario_catalog(scenario: dict[str, Any]) -> list[MetricEntry]:
    metrics: list[str] = []
    for panel in scenario["dashboard"]["panels"]:
        metrics.extend(extract_metrics_from_promql(panel["expr"]))
    metrics.extend(scenario.get("noise_metrics", []))

    return [
        MetricEntry(
            name=metric,
            datasource_uid="prom-e2e",
            datasource_name="Prometheus E2E",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=scenario.get("catalog_dimensions", []),
        )
        for metric in dict.fromkeys(metrics)
    ]


def incident_cases(scenario: dict[str, Any]) -> list[IncidentPromptCase]:
    matrix = scenario["prompt_matrix"]
    cases: list[IncidentPromptCase] = []
    for service, failure, style, perturbation in itertools.product(
        matrix["services"],
        matrix["failure_modes"],
        matrix["prompt_styles"],
        matrix["perturbations"],
    ):
        prompt = style.format(
            service=service,
            symptom=failure["symptom"],
            failure_mode=failure["id"],
            perturbation=perturbation,
        ).strip()
        cases.append(
            IncidentPromptCase(
                case_id=f"{service}-{failure['id']}-{len(cases) + 1:02d}",
                prompt=prompt,
                service=service,
                failure_mode=failure["id"],
                expected_metrics=failure["expected_metrics"],
                critical_metrics=failure["critical_metrics"],
            )
        )
    return cases


def intent_from_prompt(prompt: str, *, service: str) -> Intent:
    tokens = [t for t in re.split(r"[^a-zA-Z0-9_]+", prompt.lower()) if len(t) >= 3]
    keywords = list(dict.fromkeys([*tokens, "checkout", "edge", "incident", "latency", "errors", "saturation"]))
    return Intent(
        summary=prompt,
        domain="application",
        services=[service],
        keywords=keywords,
        timerange="1h",
        problem_type="general",
        archetypes=[],
    )


def spec_metrics(spec: DashboardSpec) -> set[str]:
    metrics: set[str] = set()
    for panel in spec.panels:
        for query in panel.queries:
            metrics.update(extract_metrics_from_promql(query.expr))
    return metrics


def fuzzy_match(expected: set[str], found: set[str]) -> set[str]:
    matched: set[str] = set()
    for exp in expected:
        for fnd in found:
            if exp == fnd or exp in fnd or fnd in exp:
                matched.add(exp)
                break
    return matched


def evaluate_incident(spec: DashboardSpec, case: IncidentPromptCase) -> IncidentEvaluation:
    found = spec_metrics(spec)
    expected = set(case.expected_metrics)
    critical = set(case.critical_metrics)
    matched = fuzzy_match(expected, found)
    critical_found = fuzzy_match(critical, found)
    metric_recall = len(matched) / len(expected) if expected else 1.0
    critical_recall = len(critical_found) / len(critical) if critical else metric_recall
    signal_to_noise = len(matched) / len(found) if found else 0.0
    panel_score = min(len(spec.panels) / 6, 1.0)
    usefulness_score = (
        (0.45 * critical_recall) + (0.30 * metric_recall) + (0.15 * signal_to_noise) + (0.10 * panel_score)
    )
    return IncidentEvaluation(
        found_metrics=found,
        matched_metrics=matched,
        missing_metrics=expected - matched,
        critical_found=critical_found,
        critical_missing=critical - critical_found,
        metric_recall=metric_recall,
        critical_recall=critical_recall,
        signal_to_noise=signal_to_noise,
        usefulness_score=usefulness_score,
    )
