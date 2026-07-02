from __future__ import annotations

import json
import math
from datetime import UTC, datetime

from tacit.evaluation_summary import (
    MRR_UNTRUNCATED,
    UNAVAILABLE_REASON,
    build_evaluation_summary,
    save_evaluation_result,
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


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 7, 2, 4, 15, 0, tzinfo=tz or UTC)


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
    assert "evidence_attribution" in evaluation["metrics"]
    assert "negative_correctness" in evaluation["metrics"]
    assert "abstention_on_insufficient" in evaluation["metrics"]
    assert "contextual_top3_only_recall" in evaluation["metrics"]
    assert "contextual_top3_only_not_top1" in evaluation["metrics"]
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


def test_evaluation_summary_rejects_free_form_reason_in_preshaped_entry():
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
        "reason": "checkout-api latency is high",
    }

    summary = build_evaluation_summary([{"evaluations": [raw_entry]}])

    assert summary["available"] is False


def test_evaluation_summary_rejects_private_slug_versions_in_preshaped_entry():
    raw_entry = {
        "benchmark_name": "contextual_culprit_ranking",
        "benchmark_version": "checkout-api",
        "dataset_hash": "sha256:0123456789abcdef",
        "runner_version": "checkout-api",
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
    }

    summary = build_evaluation_summary([{"evaluations": [raw_entry]}])

    assert summary["available"] is False


def test_adapters_hash_private_slug_versions_before_export():
    report = evaluate()
    report["version"] = "checkout-api"
    report["runner_version"] = "checkout-api"

    summary = build_evaluation_summary([report])
    evaluation = summary["evaluations"][0]

    assert evaluation["benchmark_version"].startswith("version_hash_")
    assert evaluation["runner_version"].startswith("version_hash_")
    assert "checkout-api" not in json.dumps(summary)


def test_adapters_hash_private_snake_case_version_slugs_before_export():
    report = evaluate()
    report["version"] = "checkout_api_v1"
    report["runner_version"] = "prod_payments_v2"

    summary = build_evaluation_summary([report])
    evaluation = summary["evaluations"][0]

    assert evaluation["benchmark_version"].startswith("version_hash_")
    assert evaluation["runner_version"].startswith("version_hash_")
    assert "checkout_api_v1" not in json.dumps(summary)
    assert "prod_payments_v2" not in json.dumps(summary)


def test_preshaped_summary_rejects_private_snake_case_version_slugs():
    raw_entry = {
        "benchmark_name": "contextual_culprit_ranking",
        "benchmark_version": "checkout_api_v1",
        "dataset_hash": "sha256:0123456789abcdef",
        "runner_version": "prod_payments_v2",
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
    }

    summary = build_evaluation_summary([{"evaluations": [raw_entry]}])

    assert summary["available"] is False


def test_non_finite_metric_values_are_coerced_or_rejected():
    report = evaluate()
    report["metrics"]["mrr"] = "nan"
    report["positive_cases"][0]["rank"] = math.inf

    summary = build_evaluation_summary([report])
    evaluation = summary["evaluations"][0]

    assert evaluation["metrics"]["mrr"]["value"] == 0.0
    assert validate_evaluation_summary(summary)["passed"] is True

    preshaped = {
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
                "metrics": {"mrr": {"value": math.inf}},
                "random_baselines": {},
                "stage_counts": {},
                "failure_reasons": {},
                "per_case": [],
            }
        ],
    }
    assert validate_evaluation_summary(preshaped)["passed"] is False


def test_preshaped_summary_rejects_invalid_privacy_booleans():
    raw_entry = {
        "benchmark_name": "contextual_culprit_ranking",
        "benchmark_version": "2",
        "dataset_hash": "sha256:0123456789abcdef",
        "runner_version": "0.1.0",
        "generated_at": "2026-07-02T04:15:00Z",
        "mode": "gate",
        "context_available": [],
        "anonymous": False,
        "raw_inputs_included": True,
        "contract": {},
        "metrics": {},
        "random_baselines": {},
        "stage_counts": {},
        "failure_reasons": {},
        "per_case": [],
    }

    summary = build_evaluation_summary([{"evaluations": [raw_entry]}])

    assert summary["available"] is False


def test_malformed_cached_result_is_skipped_without_blocking_valid_result():
    malformed = {"benchmark_contract": {"scorable_cases": "not-a-number"}, "metrics": {"top1_recall": 1.0}}
    valid = evaluate()

    summary = build_evaluation_summary([malformed, valid])

    assert summary["available"] is True
    assert len(summary["evaluations"]) == 1
    assert summary["evaluations"][0]["benchmark_name"] == "contextual_culprit_ranking"


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


def test_ranking_gate_failures_are_preserved():
    report = evaluate()
    report["gate"] = {"passed": False, "failures": ["stability failed", "counterfactual_sensitivity failed"]}

    summary = build_evaluation_summary([report])
    evaluation = summary["evaluations"][0]

    assert evaluation["failures"] == ["other", "other"]
    assert evaluation["failure_reasons"]["other"] == 2


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

    report = evaluate_alert_lift()
    report["generated_at"] = "2026-07-02T04:15:00Z"
    report["gate"]["failures"] = ["alert context did not improve top1_recall", "frozen baseline gate failed"]

    summary = build_evaluation_summary([report])
    evaluation = summary["evaluations"][0]

    assert summary["available"] is True
    assert evaluation["benchmark_name"] == "alert_context_ranking_lift"
    assert evaluation["generated_at"] == "2026-07-02T04:15:00Z"
    assert evaluation["contract"]["total_cases"] == 47
    assert evaluation["metrics"]["top1"]["denominator"] == 38
    assert "top1_delta" in evaluation["metrics"]
    assert "alert_contribution_rate" in evaluation["metrics"]
    assert evaluation["failures"] == ["alert_context_did_not_improve_top1_recall", "other"]
    assert evaluation["failure_reasons"]["other"] == 1
    assert evaluation["random_baselines"]["mrr"] == 0.4567


def test_lift_harness_latest_selection_uses_top_level_timestamp():
    from tests.eval.alert_context_ranking_harness import evaluate_alert_lift

    older = evaluate_alert_lift()
    newer = evaluate_alert_lift()
    older["generated_at"] = "2026-07-02T04:15:00Z"
    newer["generated_at"] = "2026-07-02T04:16:00Z"
    older["deltas"]["top1_recall"] = 0.1
    newer["deltas"]["top1_recall"] = 0.2

    summary = build_evaluation_summary([newer, older])
    evaluation = summary["evaluations"][0]

    assert evaluation["generated_at"] == "2026-07-02T04:16:00Z"
    assert evaluation["metrics"]["top1_delta"]["value"] == 0.2


def test_artifact_robustness_report_is_summarized_without_raw_artifacts():
    from tests.eval.artifact_robustness_harness import evaluate_artifact_robustness

    summary = build_evaluation_summary([evaluate_artifact_robustness()])
    text = json.dumps(summary)
    evaluation = summary["evaluations"][0]

    assert summary["available"] is True
    assert evaluation["benchmark_name"] == "artifact_learning_robustness"
    assert evaluation["mode"] == "artifact_robustness"
    assert "rca_suppression_recall" in evaluation["metrics"]
    assert "noise_worst_mrr_delta" in evaluation["metrics"]
    assert "checkout-api" not in text
    assert "checkout-db" not in text


def test_gamma_report_is_summarized_without_prompts_or_model():
    report = {
        "dataset": "gamma",
        "scenario_id": "gamma-0001",
        "model": "qwen3-coder:30b-a3b-q4_K_M",
        "protocol_fingerprint": {
            "protocol_sha256": "a" * 64,
            "control_matrix_sha256": "b" * 64,
        },
        "prediction_evaluation": {
            "checks": {
                "canonical_all_prompts_create_dashboard": True,
                "prefix_only_preserves_mapping_coverage": True,
                "canonical_evidence_recall_meets_gate": True,
                "all_arm_cache_hits_are_zero": True,
                "prefixed_all_prompts_bind_post_fix": False,
                "prefixed_evidence_recall_meets_gate": True,
                "raw_ambiguous_binding_abstains": True,
            },
            "counts": {
                "canonical_evidence_recall": {"numerator": 20, "denominator": 20, "recall": 1.0},
                "prefixed_evidence_recall": {"numerator": 18, "denominator": 20, "recall": 0.9},
                "dashboards": {
                    "canonical": {"numerator": 10, "denominator": 10},
                    "prefixed": {"numerator": 9, "denominator": 10},
                    "raw": {"numerator": 0, "denominator": 10},
                },
            },
            "passed": False,
        },
        "control_evaluation": {
            "checks": {
                "healthy_does_not_assert_culprit": True,
                "evidence_absent_does_not_assert_resource_culprit": True,
                "evidence_absent_discovers_symptom": True,
                "control_cache_hits_are_zero": True,
                "control_sample_size_meets_gate": True,
                "control_classes_are_balanced": True,
                "control_scenarios_are_distinct": True,
                "control_families_are_diverse": True,
            },
            "counts": {
                "false_culprit": {"numerator": 0, "denominator": 20},
                "abstention": {"numerator": 20, "denominator": 20},
            },
            "known_gaps": {
                "evidence_absent_preserves_symptom_panel": {"numerator": 10, "denominator": 10, "recall": 1.0}
            },
            "passed": True,
        },
        "arms": {"canonical": {"results": [{"prompt": "raw prompt"}]}},
    }

    summary = build_evaluation_summary([report])
    text = json.dumps(summary)
    evaluation = summary["evaluations"][0]

    assert summary["available"] is True
    assert evaluation["benchmark_name"] == "gamma"
    assert evaluation["metrics"]["prefixed_dashboard_rate"] == {"numerator": 9, "denominator": 10, "value": 0.9}
    assert evaluation["failures"] == ["prefixed_all_prompts_bind_post_fix"]
    assert "qwen3-coder" not in text
    assert "raw prompt" not in text


def test_offline_gate_report_is_summarized_without_fixture_details():
    report = {
        "classification": [
            {
                "dataset": "clickstack",
                "role": "development",
                "tp": 8,
                "fp": 1,
                "fn": 2,
                "tn": 3,
                "labeled_signal_metrics": 10,
                "precision": 0.8889,
                "recall": 0.8,
                "coverage": 0.8,
                "misclassified": [{"metric": "checkout_latency_seconds", "gold": "latency", "got": "errors"}],
                "uncovered": ["checkout_errors_total"],
            }
        ],
        "cold_resolution": [
            {"dataset": "clickstack", "role": "development", "resolved": 3, "total": 4, "recall": 0.75, "misses": []}
        ],
        "learned_resolution": [
            {"dataset": "clickstack", "role": "development", "resolved": 4, "total": 4, "recall": 1.0, "misses": []}
        ],
        "learned_selection": [
            {
                "dataset": "clickstack",
                "selected": "learned_clickstack",
                "expected": "learned_clickstack",
                "passed": True,
            }
        ],
        "gate": {"passed": False, "failures": ["clickstack semantic precision 0.8889 < 0.90"]},
    }

    summary = build_evaluation_summary([report])
    text = json.dumps(summary)
    evaluation = summary["evaluations"][0]

    assert summary["available"] is True
    assert evaluation["benchmark_name"] == "offline_gate"
    assert evaluation["metrics"]["semantic_precision"] == {"numerator": 8, "denominator": 9, "value": 0.8889}
    assert evaluation["failures"] == ["semantic_precision_below_threshold"]
    assert "checkout_latency_seconds" not in text


def test_save_evaluation_result_uses_unique_paths_for_same_second(tmp_path, monkeypatch):
    monkeypatch.setattr("tacit.evaluation_summary.datetime", FrozenDateTime)

    first = save_evaluation_result({"benchmark": "prompt_variation"}, directory=tmp_path)
    second = save_evaluation_result({"benchmark": "prompt_variation"}, directory=tmp_path)

    assert first != second
    assert first.exists()
    assert second.exists()


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


def test_prompt_variation_rates_are_derived_from_exported_counts():
    report = {
        "corpus": "clickstack_prompts.json",
        "role": "dev",
        "prompts": 1,
        "trials_per_prompt": 5,
        "positive_useful_rate": 1.0,
        "negative_correct_rate": 1.0,
        "worst_prompt_rate": 1.0,
        "results": [
            {
                "prompt_index": 1,
                "class": "reworded",
                "polarity": "positive",
                "prompt": "checkout-api latency is high",
                "passed": 0,
                "trials": 5,
                "rate": "nan",
                "failures": [],
            }
        ],
    }

    summary = build_evaluation_summary([report])
    evaluation = summary["evaluations"][0]

    assert evaluation["metrics"]["positive_useful_rate"] == {"numerator": 0, "denominator": 5, "value": 0.0}
    assert evaluation["metrics"]["worst_prompt_rate"] == {"value": 0.0}
    assert evaluation["per_case"][0]["rate"] == 0.0
