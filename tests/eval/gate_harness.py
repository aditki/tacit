"""Offline gate-metric harness for the DashForge accuracy gates.

Measures the *datasource-free* gates against labeled gold-set fixtures, under a
cold-isolated runtime, with explicit numerators/denominators:

  1. Semantic mapping (metric -> signal family) precision / recall / coverage,
     using the deterministic morphology layer (``signal_inference.infer_signal``)
     — the "metric semantic understanding" the M1 review scored 5/10.
  2. Critical-signal cold resolvability recall, using the bootstrap signal store
     (``SignalStore.resolve_signal``) seeded only from packaged ``signals.yaml``.

It deliberately does NOT measure the "returns data in-window" axis, datasource
routing against a live stack, or LLM intent variance — those need the running
Grafana/VictoriaMetrics stack and the real ingested datasets (roadmap M2/M3).

ClickStack is a curated 34-metric primary morphology slice; LO2 and GAMMA are
synthetic *holdouts* whose metric names follow each dataset's conventions. The
harness detects obvious naming-convention regressions, but does not claim
catalog-level or incident-level accuracy until the complete real datasets are
labeled and ingested.

Run:
    python -m tests.eval.gate_harness                # all fixtures
    python -m tests.eval.gate_harness --json out.json
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dashforge.archetypes.engine import rank_archetypes_by_coverage
from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.models.schemas import MetricEntry
from dashforge.signal_inference import infer_signal
from tests.eval.cold_isolation import cold_isolation

FIXTURES_DIR = Path(__file__).parent / "fixtures"
MIN_SEMANTIC_PRECISION = 0.90
MIN_SEMANTIC_RECALL = 0.80
MIN_SEMANTIC_COVERAGE = 0.80
MIN_COLD_RESOLUTION = 0.75
MIN_LEARNED_RESOLUTION = 0.90


@dataclass
class ClassificationResult:
    dataset: str
    role: str
    tp: int
    fp: int
    fn: int
    tn: int
    labeled_signal_metrics: int  # metrics whose gold family is a real signal
    precision: float
    recall: float
    coverage: float
    misclassified: list[dict[str, str]]
    uncovered: list[str]


@dataclass
class ResolutionResult:
    dataset: str
    role: str
    resolved: int
    total: int
    recall: float
    misses: list[dict[str, str]]


@dataclass
class LearnedSelectionResult:
    dataset: str
    selected: str
    expected: str
    passed: bool


def _load_fixture(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _catalog(fixture: dict[str, Any]) -> list[MetricEntry]:
    out: list[MetricEntry] = []
    for m in fixture["metrics"]:
        out.append(
            MetricEntry(
                name=m["name"],
                datasource_uid=fixture["datasource_uid"],
                datasource_name=fixture["dataset"],
                datasource_type=fixture["datasource_type"],
                query_language=fixture["query_language"],
                unit=m.get("unit", ""),
                metric_type=m.get("type", ""),
            )
        )
    return out


def classify(fixture: dict[str, Any]) -> ClassificationResult:
    """Metric -> signal-family classification vs gold labels (name-only morphology)."""
    tp = fp = fn = tn = 0
    labeled = 0
    misclassified: list[dict[str, str]] = []
    uncovered: list[str] = []

    for m in fixture["metrics"]:
        gold = m["family"]  # 'none' means metadata/info, should NOT be classified
        got = infer_signal(
            m["name"],
            unit=m.get("unit", ""),
            metric_type=m.get("type", ""),
            dimensions=m.get("dimensions", []),
            namespace=m.get("namespace", ""),
        )
        got_family = got.signal_family if got else None

        if gold == "none":
            if got_family is None:
                tn += 1
            else:
                fp += 1
                misclassified.append({"metric": m["name"], "gold": "none", "got": got_family})
            continue

        labeled += 1
        if got_family is None:
            fn += 1
            uncovered.append(m["name"])
        elif got_family == gold:
            tp += 1
        else:
            fp += 1
            fn += 1
            misclassified.append({"metric": m["name"], "gold": gold, "got": got_family})

    precision = tp / (tp + fp) if (tp + fp) else 1.0
    recall = tp / (tp + fn) if (tp + fn) else 1.0
    coverage = (labeled - len(uncovered)) / labeled if labeled else 1.0
    return ClassificationResult(
        dataset=fixture["dataset"],
        role=fixture["role"],
        tp=tp,
        fp=fp,
        fn=fn,
        tn=tn,
        labeled_signal_metrics=labeled,
        precision=round(precision, 4),
        recall=round(recall, 4),
        coverage=round(coverage, 4),
        misclassified=misclassified,
        uncovered=uncovered,
    )


def resolve_critical(fixture: dict[str, Any], signal_store: Any) -> ResolutionResult:
    """Cold critical-signal resolvability: does the bootstrap taxonomy map each
    critical signal to its gold metric (top-1) against the live catalog?"""
    catalog = _catalog(fixture)
    resolved = 0
    misses: list[dict[str, str]] = []
    crit = fixture["critical_signals"]
    for c in crit:
        sig, expected = c["signal_type"], c["expected_metric"]
        hits = signal_store.resolve_signal(sig, catalog, target_query_language=fixture["query_language"])
        names = [m.name for m, _ in hits]
        if expected in names[:1]:
            resolved += 1
        else:
            misses.append({"signal": sig, "expected": expected, "got": ", ".join(names[:3]) or "(none)"})
    total = len(crit)
    return ResolutionResult(
        dataset=fixture["dataset"],
        role=fixture["role"],
        resolved=resolved,
        total=total,
        recall=round(resolved / total, 4) if total else 1.0,
        misses=misses,
    )


def _teach_fixture(fixture: dict[str, Any], signal_store: Any) -> None:
    """Apply a reproducible teaching step in the isolated learned run."""
    for critical in fixture["critical_signals"]:
        signal_store.add_mapping(
            critical["signal_type"],
            critical["expected_metric"],
            confidence=0.99,
            context_datasource_types=[fixture["datasource_type"]],
            source_type="dashboard_ingest",
            source_refs=[f"eval:{fixture['dataset']}"],
            review_state="approved",
        )


def evaluate_learned_selection(fixture: dict[str, Any]) -> LearnedSelectionResult:
    """Verify strong learned context beats a higher-confidence generic match."""
    critical = fixture["critical_signals"]
    learned_id = f"learned_{fixture['dataset']}_incident"
    learned = InvestigationArchetype(
        id=learned_id,
        name=f"Learned {fixture['dataset']} incident",
        problem_types=[learned_id],
        required_signals=[item["signal_type"] for item in critical],
        signal_bindings={item["signal_type"]: item["expected_metric"] for item in critical},
        panels=[
            PanelTemplate(
                title=item["signal_type"],
                queries=[QueryTemplate(expr=item["expected_metric"])],
            )
            for item in critical
        ],
        tags=["learned", fixture["dataset"]],
    )
    generic = InvestigationArchetype(
        id="generic_incident",
        name="Generic incident",
        problem_types=["general"],
        required_signals=[critical[0]["signal_type"], "definitely_absent_signal"],
        signal_bindings={
            critical[0]["signal_type"]: critical[0]["expected_metric"],
            "definitely_absent_signal": "definitely_absent_metric",
        },
        panels=[PanelTemplate(title="Generic", queries=[QueryTemplate(expr=critical[0]["expected_metric"])])],
        tags=["general"],
    )
    ranked = rank_archetypes_by_coverage(
        [(generic, 0.95), (learned, 0.70)],
        _catalog(fixture),
        target_language=fixture["query_language"],
        max_archetypes=2,
    )
    selected = ranked[0][0].id
    return LearnedSelectionResult(
        dataset=fixture["dataset"],
        selected=selected,
        expected=learned_id,
        passed=selected == learned_id,
    )


def run() -> dict[str, Any]:
    fixtures = [
        fixture
        for fixture in (_load_fixture(p) for p in sorted(FIXTURES_DIR.glob("*.json")))
        if "metrics" in fixture and "critical_signals" in fixture
    ]
    classifications = [classify(f) for f in fixtures]

    # Resolution runs inside a cold-isolated runtime (packaged taxonomy only).
    cold_resolutions: list[ResolutionResult] = []
    with cold_isolation() as state:
        for f in fixtures:
            cold_resolutions.append(resolve_critical(f, state.signal_store))

    learned_resolutions: list[ResolutionResult] = []
    learned_selections: list[LearnedSelectionResult] = []
    for f in fixtures:
        with cold_isolation() as state:
            _teach_fixture(f, state.signal_store)
            learned_resolutions.append(resolve_critical(f, state.signal_store))
            learned_selections.append(evaluate_learned_selection(f))

    return {
        "classification": [asdict(c) for c in classifications],
        "cold_resolution": [asdict(r) for r in cold_resolutions],
        "learned_resolution": [asdict(r) for r in learned_resolutions],
        "learned_selection": [asdict(r) for r in learned_selections],
    }


def _aggregate(rows: list[dict[str, Any]], keys: list[str]) -> dict[str, int]:
    return {k: sum(r[k] for r in rows) for k in keys}


def gate_failures(report: dict[str, Any]) -> list[str]:
    """Return explicit gate failures; an empty list means the offline gate passes."""
    failures: list[str] = []
    for row in report["classification"]:
        dataset = row["dataset"]
        for metric, threshold in (
            ("precision", MIN_SEMANTIC_PRECISION),
            ("recall", MIN_SEMANTIC_RECALL),
            ("coverage", MIN_SEMANTIC_COVERAGE),
        ):
            if row[metric] < threshold:
                failures.append(f"{dataset} semantic {metric} {row[metric]:.4f} < {threshold:.2f}")
    for row in report["cold_resolution"]:
        if row["recall"] < MIN_COLD_RESOLUTION:
            failures.append(f"{row['dataset']} cold resolution {row['recall']:.4f} < {MIN_COLD_RESOLUTION:.2f}")
    for row in report["learned_resolution"]:
        if row["recall"] < MIN_LEARNED_RESOLUTION:
            failures.append(f"{row['dataset']} learned resolution {row['recall']:.4f} < {MIN_LEARNED_RESOLUTION:.2f}")
    for row in report["learned_selection"]:
        if not row["passed"]:
            failures.append(f"{row['dataset']} learned selection chose {row['selected']} instead of {row['expected']}")
    return failures


def _print(report: dict[str, Any]) -> None:
    print("\n=== Semantic mapping: metric -> signal family (morphology + catalog metadata) ===")
    print(f"{'dataset':12s} {'role':8s} {'TP':>3} {'FP':>3} {'FN':>3} {'prec':>6} {'recall':>7} {'cover':>6}")
    for c in report["classification"]:
        print(
            f"{c['dataset']:12s} {c['role']:8s} {c['tp']:3d} {c['fp']:3d} {c['fn']:3d} "
            f"{c['precision']:6.2f} {c['recall']:7.2f} {c['coverage']:6.2f}"
        )
    hold = [c for c in report["classification"] if c["role"] == "holdout"]
    agg = _aggregate(hold, ["tp", "fp", "fn"])
    if hold:
        p = agg["tp"] / (agg["tp"] + agg["fp"]) if (agg["tp"] + agg["fp"]) else 1.0
        r = agg["tp"] / (agg["tp"] + agg["fn"]) if (agg["tp"] + agg["fn"]) else 1.0
        print(f"{'HOLDOUT agg':12s} {'':8s} {agg['tp']:3d} {agg['fp']:3d} {agg['fn']:3d} {p:6.2f} {r:7.2f}")

    for c in report["classification"]:
        if c["misclassified"] or c["uncovered"]:
            print(f"  [{c['dataset']}] misclassified={c['misclassified']} uncovered={c['uncovered']}")

    print("\n=== Critical-signal cold resolvability (bootstrap taxonomy only) ===")
    print(f"{'dataset':12s} {'role':8s} {'resolved':>9} {'total':>6} {'recall':>7}")
    for r in report["cold_resolution"]:
        print(f"{r['dataset']:12s} {r['role']:8s} {r['resolved']:9d} {r['total']:6d} {r['recall']:7.2f}")
    for r in report["cold_resolution"]:
        if r["misses"]:
            print(f"  [{r['dataset']}] misses={r['misses']}")

    print("\n=== Critical-signal learned resolvability (fresh taught runtime) ===")
    for r in report["learned_resolution"]:
        print(f"{r['dataset']:12s} {r['role']:8s} {r['resolved']:9d} {r['total']:6d} {r['recall']:7.2f}")

    print("\n=== Learned archetype preference (strong live coverage required) ===")
    for r in report["learned_selection"]:
        print(f"{r['dataset']:12s} selected={r['selected']} passed={r['passed']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Offline gate-metric harness.")
    parser.add_argument("--json", type=str, default="", help="Write the full report to this JSON path.")
    args = parser.parse_args()
    report = run()
    failures = gate_failures(report)
    report["gate"] = {"passed": not failures, "failures": failures}
    _print(report)
    if failures:
        print("\n=== OFFLINE GATE FAILED ===")
        for failure in failures:
            print(f"- {failure}")
    else:
        print("\n=== OFFLINE GATE PASSED ===")
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2))
        print(f"\nwrote {args.json}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
