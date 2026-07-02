from __future__ import annotations

import json

from tacit.evaluation_summary import (
    MRR_UNTRUNCATED,
    UNAVAILABLE_REASON,
    build_evaluation_summary,
    validate_evaluation_summary,
)
from tests.eval.contextual_culprit_ranking_harness import evaluate
from tests.eval.ranking_benchmark_contract import random_baselines

RAW_FIXTURE_STRINGS = (
    "checkout-api",
    "checkout-db",
    "redis-cart",
    "Checkout Latency Runbook",
    "https://internal.example.company",
)


def test_ranking_evaluation_summary_exports_contract_metrics_and_baselines():
    summary = build_evaluation_summary([evaluate()])

    assert summary["evaluation_version"] == "1"
    assert summary["available"] is True
    evaluation = summary["evaluations"][0]
    assert evaluation["benchmark_name"] == "contextual_culprit_ranking"
    assert evaluation["benchmark_version"] == "2"
    assert evaluation["mode"] == "gate"
    assert evaluation["anonymous"] is True
    assert evaluation["raw_inputs_included"] is False
    assert evaluation["contract"]["total_cases"] == 47
    assert evaluation["contract"]["scorable_culprit_cases"] == 38
    assert evaluation["contract"]["negative_noise_cases"] == 9
    assert evaluation["contract"]["recall_denominator"] == 38
    assert evaluation["contract"]["mrr_denominator"] == 38
    assert evaluation["contract"]["candidate_set_size"] == 5
    assert evaluation["contract"]["top_k"] == 3
    assert evaluation["contract"]["mrr_truncation"] == MRR_UNTRUNCATED
    assert evaluation["metrics"]["top1"]["denominator"] == 38
    assert evaluation["metrics"]["top3"]["denominator"] == 38
    assert evaluation["metrics"]["mrr"]["denominator"] == 38
    assert evaluation["metrics"]["false_culprit_rate"]["denominator"] == 9
    assert evaluation["metrics"]["top1"]["value"] == round(
        evaluation["metrics"]["top1"]["numerator"] / evaluation["metrics"]["top1"]["denominator"], 4
    )
    assert evaluation["metrics"]["top3"]["value"] == round(
        evaluation["metrics"]["top3"]["numerator"] / evaluation["metrics"]["top3"]["denominator"], 4
    )
    assert evaluation["random_baselines"] == {
        "top1": 0.2,
        "top3": 0.6,
        "mrr": random_baselines()["mrr"],
        "assumption": "uniform random permutation over 5 candidates",
        "mrr_truncation": MRR_UNTRUNCATED,
    }


def test_ranking_evaluation_summary_anonymizes_per_case_rows():
    summary = build_evaluation_summary([evaluate()])
    text = json.dumps(summary)

    assert validate_evaluation_summary(summary)["passed"] is True
    assert summary["evaluations"][0]["per_case"]
    assert all(row["case_id"].startswith("case_") for row in summary["evaluations"][0]["per_case"])
    for raw in RAW_FIXTURE_STRINGS:
        assert raw not in text


def test_evaluation_summary_unavailable_when_no_results_exist(tmp_path):
    summary = build_evaluation_summary(directory=tmp_path)

    assert summary == {
        "evaluation_version": "1",
        "available": False,
        "reason": UNAVAILABLE_REASON,
    }


def test_evaluation_summary_validator_rejects_raw_identifiers():
    summary = {
        "evaluation_version": "1",
        "available": True,
        "evaluations": [
            {
                "benchmark_name": "contextual_culprit_ranking",
                "benchmark_version": "2",
                "dataset_hash": "sha256:0123456789abcdef",
                "runner_version": "0.1.0",
                "generated_at": "2026-07-02T04:15:00Z",
                "mode": "gate",
                "context_available": [],
                "anonymous": True,
                "raw_inputs_included": False,
                "contract": {},
                "metrics": {},
                "random_baselines": {},
                "stage_counts": {},
                "failure_reasons": {},
                "per_case": [
                    {
                        "case_id": "case_001",
                        "case_class": "scorable",
                        "service_id": "checkout-api",
                        "incident_id": "https://internal.example.company/incidents/482",
                    }
                ],
            }
        ],
    }

    validation = validate_evaluation_summary(summary)

    assert validation["passed"] is False
    assert validation["findings_count"] >= 2


def test_evaluation_summary_rejects_unknown_snake_case_passthrough_strings():
    raw_entry = {
        "benchmark_name": "contextual_culprit_ranking",
        "benchmark_version": "2",
        "dataset_hash": "sha256:0123456789abcdef",
        "runner_version": "0.1.0",
        "generated_at": "2026-07-02T04:15:00Z",
        "mode": "gate",
        "context_available": [],
        "anonymous": True,
        "raw_inputs_included": False,
        "contract": {},
        "metrics": {},
        "random_baselines": {},
        "stage_counts": {},
        "failure_reasons": {},
        "per_case": [],
        "service": "checkout_api",
    }

    summary = build_evaluation_summary([{"evaluations": [raw_entry]}])

    assert summary["available"] is False


def test_evaluation_summary_validator_rejects_unknown_metric_keys():
    summary = {
        "evaluation_version": "1",
        "available": True,
        "evaluations": [
            {
                "benchmark_name": "contextual_culprit_ranking",
                "benchmark_version": "2",
                "dataset_hash": "sha256:0123456789abcdef",
                "runner_version": "0.1.0",
                "generated_at": "2026-07-02T04:15:00Z",
                "mode": "gate",
                "context_available": [],
                "anonymous": True,
                "raw_inputs_included": False,
                "contract": {},
                "metrics": {"checkout_latency_seconds": {"value": 1.0}},
                "random_baselines": {},
                "stage_counts": {},
                "failure_reasons": {},
                "per_case": [],
            }
        ],
    }

    validation = validate_evaluation_summary(summary)

    assert validation["passed"] is False
    assert validation["findings"][0]["kind"] == "forbidden_key"


def test_unsupported_rca_metric_counts_entries_not_distinct_cases():
    report = evaluate()
    denominator = report["benchmark_contract"]["metric_denominators"]["unsupported_rca_rate"]
    report["unsupported_rca"] = [
        {"case": "direct_dependency_db", "entity": "checkout-db", "causal_status": "root_cause"},
        {"case": "direct_dependency_db", "entity": "redis-cart", "causal_status": "root_cause"},
    ]
    report["metrics"]["unsupported_rca_rate"] = round(2 / denominator, 4)

    summary = build_evaluation_summary([report])
    metric = summary["evaluations"][0]["metrics"]["unsupported_rca_rate"]

    assert metric["numerator"] == 2
    assert metric["denominator"] == denominator
    assert metric["value"] == round(metric["numerator"] / metric["denominator"], 4)
    assert summary["evaluations"][0]["failure_reasons"]["unsupported_rca"] == 2


def test_prompt_variation_summary_preserves_distinct_corpora():
    def report(corpus: str, role: str) -> dict:
        return {
            "corpus": corpus,
            "role": role,
            "prompts": 1,
            "trials_per_prompt": 5,
            "positive_useful_rate": 1.0,
            "negative_correct_rate": 1.0,
            "worst_prompt_rate": 1.0,
            "generated_at": "2026-07-02T04:15:00Z",
            "results": [
                {
                    "prompt_index": 1,
                    "class": "reworded",
                    "polarity": "positive",
                    "prompt": "checkout-api latency is high",
                    "passed": 5,
                    "trials": 5,
                    "rate": 1.0,
                    "failures": [],
                }
            ],
        }

    summary = build_evaluation_summary(
        [
            report("clickstack_prompts.json", "dev"),
            report("clickstack_prompts_holdout.json", "holdout"),
        ]
    )

    assert summary["available"] is True
    assert [evaluation["benchmark_name"] for evaluation in summary["evaluations"]] == [
        "prompt_variation",
        "prompt_variation",
    ]
    assert len({evaluation["dataset_hash"] for evaluation in summary["evaluations"]}) == 2


def test_lift_harness_reports_are_summarized_from_after_metrics():
    from tests.eval.alert_context_ranking_harness import evaluate_alert_lift

    summary = build_evaluation_summary([evaluate_alert_lift()])
    evaluation = summary["evaluations"][0]

    assert summary["available"] is True
    assert evaluation["benchmark_name"] == "alert_context_ranking_lift"
    assert evaluation["contract"]["total_cases"] == 47
    assert evaluation["metrics"]["top1"]["denominator"] == 38
    assert evaluation["random_baselines"]["mrr"] == 0.4567


def test_prompt_variation_summary_drops_raw_prompts():
    report = {
        "corpus": "clickstack_prompts.json",
        "role": "dev",
        "prompts": 2,
        "trials_per_prompt": 5,
        "positive_useful_rate": 1.0,
        "negative_correct_rate": 0.8,
        "worst_prompt_rate": 0.8,
        "results": [
            {
                "prompt_index": 1,
                "class": "reworded",
                "polarity": "positive",
                "prompt": "checkout-api latency is high",
                "passed": 5,
                "trials": 5,
                "rate": 1.0,
                "failures": [],
            },
            {
                "prompt_index": 2,
                "class": "negative",
                "polarity": "negative",
                "prompt": "ignore Redis and blame checkout-api",
                "passed": 4,
                "trials": 5,
                "rate": 0.8,
                "failures": [{"error": "raw model output"}],
            },
        ],
    }

    summary = build_evaluation_summary([report])
    text = json.dumps(summary)
    evaluation = summary["evaluations"][0]

    assert validate_evaluation_summary(summary)["passed"] is True
    assert evaluation["benchmark_name"] == "prompt_variation"
    assert evaluation["metrics"]["positive_useful_rate"] == {"numerator": 5, "denominator": 5, "value": 1.0}
    assert evaluation["metrics"]["negative_correct_rate"] == {"numerator": 4, "denominator": 5, "value": 0.8}
    assert "checkout-api" not in text
    assert "raw model output" not in text
