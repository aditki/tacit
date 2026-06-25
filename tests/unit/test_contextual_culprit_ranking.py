from tacit.contextual_culprit_ranking import CAUSAL_STATUS, WEIGHTS, ContextBundle, rank_context_bundle
from tests.eval.contextual_culprit_ranking_harness import evaluate, gate_failures


def _bundle(payload: dict) -> ContextBundle:
    return ContextBundle.model_validate(payload)


def test_context_bundle_ranks_with_weighted_attribution():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [
                        {
                            "name": "checkout-api",
                            "depends_on": ["checkout-db", "redis-cart"],
                            "owner": "payments",
                        }
                    ],
                    "historical_incidents": [
                        {
                            "symptom": "checkout latency",
                            "culprit": "checkout-db",
                            "evidence": ["db p95 latency"],
                        }
                    ],
                },
                "evidence": {
                    "observations": [
                        {
                            "signal": "database latency",
                            "status": "abnormal",
                            "related_entity": "checkout-db",
                        }
                    ]
                },
            }
        )
    )

    assert result.suspects[0].entity == "checkout-db"
    assert result.suspects[0].score == 1.0
    assert result.suspects[0].causal_status == CAUSAL_STATUS
    assert {reason.type for reason in result.suspects[0].reasons} == {
        "dependency_match",
        "historical_incident_match",
        "runtime_observation_match",
    }
    assert result.abstained is True
    assert result.abstention_reason == "suspect_not_proven"


def test_ownership_only_never_creates_a_causal_candidate():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {"services": [{"name": "checkout-api", "owner": "payments"}]},
                "evidence": {"observations": []},
            }
        )
    )

    assert result.suspects == []
    assert result.abstained is True
    assert result.abstention_reason == "no_context_points_to_culprit"


def test_artifact_ownership_hint_alone_never_creates_suspect():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "ownership_hints": [
                        {
                            "entity": "checkout-api",
                            "owner": "payments-team",
                            "hint_kind": "escalation",
                        }
                    ]
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert result.suspects == []
    assert result.abstention_reason == "no_context_points_to_culprit"


def test_dependency_hint_creates_candidate_but_not_cause():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "dependency_hints": [
                        {
                            "source_entity": "checkout-api",
                            "target_entity": "redis-cart",
                            "direction": "depends_on",
                        }
                    ]
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert result.suspects[0].entity == "redis-cart"
    assert result.suspects[0].causal_status == CAUSAL_STATUS
    assert result.abstention_reason == "suspects_ranked_without_runtime_proof"


def test_stale_runbook_dependency_lowers_ranking_contribution():
    fresh = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "dependency_hints": [
                        {
                            "source_entity": "checkout-api",
                            "target_entity": "redis-cart",
                            "direction": "depends_on",
                        }
                    ]
                },
                "evidence": {"observations": []},
            }
        )
    )
    stale = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "dependency_hints": [
                        {
                            "source_entity": "checkout-api",
                            "target_entity": "redis-cart",
                            "direction": "depends_on",
                            "stale": True,
                        }
                    ]
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert fresh.suspects[0].score > stale.suspects[0].score


def test_distractor_runbook_service_does_not_outrank_connected_suspect():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [{"name": "checkout-api", "depends_on": ["checkout-db"]}],
                    "runbook_hints": [{"symptom": "checkout latency", "suspects": ["redis-cart"]}],
                },
                "evidence": {
                    "observations": [
                        {
                            "signal": "database latency",
                            "status": "abnormal",
                            "related_entity": "checkout-db",
                        }
                    ]
                },
            }
        )
    )

    assert [suspect.entity for suspect in result.suspects] == ["checkout-db"]
    assert all(suspect.entity != "redis-cart" for suspect in result.suspects)


def test_stale_runbook_is_penalized_against_current_service_graph():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [{"name": "checkout-api", "depends_on": ["checkout-postgres"]}],
                    "runbook_hints": [
                        {
                            "symptom": "checkout latency",
                            "suspects": ["checkout-mysql"],
                            "stale": True,
                        }
                    ],
                },
                "evidence": {"observations": [{"signal": "database latency", "status": "abnormal"}]},
            }
        )
    )

    assert result.suspects[0].entity == "checkout-postgres"
    assert all(suspect.entity != "checkout-mysql" for suspect in result.suspects)


def test_conflicting_sources_are_ranked_with_attribution_but_no_rca_claim():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [{"name": "checkout-api", "depends_on": ["checkout-db", "redis-cart"]}],
                    "runbook_hints": [{"symptom": "checkout latency", "suspects": ["redis-cart"]}],
                    "historical_incidents": [
                        {
                            "symptom": "checkout latency",
                            "culprit": "checkout-db",
                            "evidence": ["connection saturation"],
                        }
                    ],
                    "recent_changes": [
                        {
                            "service": "checkout-api",
                            "time_delta_minutes": 8,
                            "summary": "checkout rollout",
                        }
                    ],
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert [suspect.entity for suspect in result.suspects] == ["checkout-db", "redis-cart", "checkout-api"]
    assert all(suspect.reasons for suspect in result.suspects)
    assert all(suspect.causal_status == CAUSAL_STATUS for suspect in result.suspects)
    assert result.abstained is True
    assert result.abstention_reason == "suspects_ranked_without_runtime_proof"


def test_alert_context_can_break_contextual_tie_without_rca_claim():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [{"name": "checkout-api", "depends_on": ["checkout-db"]}],
                    "historical_incidents": [
                        {
                            "symptom": "checkout latency",
                            "culprit": "checkout-db",
                            "evidence": ["database p95 latency"],
                        }
                    ],
                    "recent_changes": [
                        {
                            "service": "checkout-api",
                            "time_delta_minutes": 8,
                            "summary": "checkout rollout",
                        }
                    ],
                    "alerts": [
                        {
                            "entity": "checkout-api",
                            "signals": ["checkout api latency alert"],
                            "severity": "critical",
                            "runbook_url": "https://runbooks.example/checkout-api",
                        }
                    ],
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert result.suspects[0].entity == "checkout-api"
    assert any(reason.type == "alert_association" for reason in result.suspects[0].reasons)
    assert result.suspects[0].causal_status == CAUSAL_STATUS
    assert result.abstained is True
    assert result.abstention_reason == "suspects_ranked_without_runtime_proof"


def test_incident_artifact_observed_evidence_does_not_count_as_runtime_support():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [{"name": "checkout-api", "depends_on": ["checkout-db"]}],
                    "evidence_requirements": [
                        {
                            "subject": "observed checkout-db latency in prior incident",
                            "evidence_kind": "latency",
                            "target_entity": "checkout-db",
                            "signal_hint": "checkout_db_latency",
                            "observation_state": "observed",
                            "source_type": "incident",
                            "source": "artifact_learning",
                        }
                    ],
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert result.suspects[0].entity == "checkout-db"
    assert any(reason.type == "incident_observed_evidence" for reason in result.suspects[0].reasons)
    assert result.abstention_reason == "suspects_ranked_without_runtime_proof"


def test_disabled_duplicate_alert_does_not_hide_later_active_alert():
    result = rank_context_bundle(
        _bundle(
            {
                "incident": {"symptom": "checkout latency increased", "affected_service": "checkout-api"},
                "context": {
                    "services": [{"name": "checkout-api", "depends_on": ["checkout-db"]}],
                    "alerts": [
                        {
                            "entity": "checkout-db",
                            "signals": ["checkout db latency alert"],
                            "enabled": False,
                            "source": "disabled_alert",
                        },
                        {
                            "entity": "checkout-db",
                            "signals": ["checkout db latency alert"],
                            "enabled": True,
                            "source": "active_alert",
                        },
                    ],
                },
                "evidence": {"observations": []},
            }
        )
    )

    assert result.suspects[0].entity == "checkout-db"
    assert any(
        reason.type == "alert_association" and reason.source == "active_alert" for reason in result.suspects[0].reasons
    )


def test_feature_weights_are_explicit_and_stable():
    assert WEIGHTS == {
        "runtime_observation_match": 40,
        "direct_dependency": 25,
        "recent_deploy": 20,
        "runbook_match": 15,
        "historical_incident_match": 15,
        "alert_association": 25,
        "evidence_requirement_observed": 25,
        "incident_observed_evidence": 18,
        "dependency_hint": 12,
        "stale_alert": 2,
        "stale_runbook_hint": 2,
        "ownership_context": 5,
        "dashboard_association": 5,
        "stale_artifact": -20,
        "contradictory_evidence": -30,
    }


def test_contextual_culprit_ranking_fixture_gate_passes():
    report = evaluate()

    assert gate_failures(report) == []
    assert report["case_count"] == 47
    assert report["benchmark_contract"] == {
        "total_cases": 47,
        "scorable_cases": 38,
        "negative_cases": 9,
        "candidate_set_size": 5,
        "context_available": [
            "service_graph",
            "runbooks",
            "historical_incidents",
            "deployments",
            "dashboards",
        ],
        "top_k": 3,
        "metric_conventions": {
            "mrr": "Reciprocal rank over the full candidate set, not truncated at top_k.",
            "top3_recall": "Recall of scorable cases whose expected culprit ranks at or above top_k.",
            "source_contribution_rates": "Artifact-source contribution/noise rates use total_cases unless overridden.",
        },
        "metric_denominators": {
            "top1_recall": 38,
            "top3_recall": 38,
            "mrr": 38,
            "false_culprit_rate": 9,
            "unsupported_rca_rate": 83,
        },
        "random_baselines": {
            "top1_recall": 0.2,
            "top3_recall": 0.6,
            "mrr": 0.4567,
        },
    }
    assert report["target_matrix_size"] == 110
    assert report["metrics"]["top1_recall"] < 1
    assert report["metrics"]["top3_recall"] >= 0.8
    assert report["metrics"]["false_culprit_rate"] == 0
    assert report["metrics"]["unsupported_rca_rate"] == 0
    assert report["metrics"]["evidence_attribution"] == 1
    assert report["metrics"]["negative_correctness"] >= 0.9
    assert report["metrics"]["abstention_on_insufficient"] >= 0.8
    assert report["metrics"]["contextual_top3_only_recall"] == 1
    assert report["metrics"]["contextual_top3_only_not_top1"] == 1
    assert report["metrics"]["stability"] is True
    assert report["metrics"]["counterfactual_sensitivity"] is True


def test_alert_context_ranking_lift_gate_passes():
    from tests.eval.alert_context_ranking_harness import evaluate_alert_lift

    report = evaluate_alert_lift()

    assert report["gate"]["failures"] == []
    assert report["case_count"] == 47
    assert report["benchmark_contract"]["total_cases"] == 47
    assert report["benchmark_contract"]["scorable_cases"] == 38
    assert report["benchmark_contract"]["candidate_set_size"] == 5
    assert report["benchmark_contract"]["top_k"] == 3
    assert report["benchmark_contract"]["random_baselines"]["top1_recall"] == 0.2
    assert report["benchmark_contract"]["random_baselines"]["top3_recall"] == 0.6
    assert "alerts" in report["benchmark_contract"]["context_available"]
    assert report["deltas"]["top1_recall"] > 0
    assert report["deltas"]["top3_recall"] == 0
    assert report["deltas"]["false_culprit_rate"] == 0
    assert report["deltas"]["unsupported_rca_rate"] == 0
    assert report["alert_metrics"]["alert_tie_break_cases"]
    assert report["alert_metrics"]["alert_regressed_cases"] == []


def test_runbook_context_ranking_lift_gate_passes():
    from tests.eval.runbook_context_ranking_harness import evaluate_runbook_lift

    report = evaluate_runbook_lift()

    assert report["gate"]["failures"] == []
    assert report["benchmark"] == "contextual_alerts_runbooks_baseline_v1"
    assert report["baseline_name"] == "Contextual Ranking + Alerts + Runbooks Baseline v1"
    assert report["case_count"] == 47
    assert report["benchmark_contract"]["total_cases"] == 47
    assert report["benchmark_contract"]["scorable_cases"] == 38
    assert report["benchmark_contract"]["candidate_set_size"] == 5
    assert report["benchmark_contract"]["top_k"] == 3
    assert report["benchmark_contract"]["metric_denominators"]["runbook_noise_rate"] == 47
    assert "alerts" in report["benchmark_contract"]["context_available"]
    assert "runbooks" in report["benchmark_contract"]["context_available"]
    assert report["deltas"]["top1_recall"] >= 0
    assert report["deltas"]["top3_recall"] == 0
    assert report["deltas"]["false_culprit_rate"] == 0
    assert report["deltas"]["unsupported_rca_rate"] == 0
    assert report["runbook_metrics"]["indeterminate_requirement_rate"] > 0


def test_incident_context_ranking_lift_gate_passes():
    from tests.eval.incident_context_ranking_harness import evaluate_incident_lift

    report = evaluate_incident_lift()

    assert report["gate"]["failures"] == []
    assert report["baseline_name"] == "Contextual Ranking + Alerts + Runbooks Baseline v1"
    assert report["case_count"] == 47
    assert report["benchmark_contract"]["total_cases"] == 47
    assert report["benchmark_contract"]["scorable_cases"] == 38
    assert report["benchmark_contract"]["candidate_set_size"] == 5
    assert report["benchmark_contract"]["top_k"] == 3
    assert report["benchmark_contract"]["metric_denominators"]["incident_contribution_rate"] == 47
    assert "incidents" in report["benchmark_contract"]["context_available"]
    assert report["deltas"]["top1_recall"] > 0
    assert report["deltas"]["mrr"] > 0
    assert report["deltas"]["top3_recall"] == 0
    assert report["deltas"]["false_culprit_rate"] == 0
    assert report["deltas"]["unsupported_rca_rate"] == 0
    assert report["incident_metrics"]["ignored_causal_claim_count"] > 0
    assert report["critical_regression"]["passed"] is True


def test_artifact_learning_robustness_gate_passes():
    from tests.eval.artifact_robustness_harness import evaluate_artifact_robustness

    report = evaluate_artifact_robustness()

    assert report["gate"]["failures"] == []
    assert report["rca_phrase_robustness"]["phrases"] >= 50
    assert report["rca_phrase_robustness"]["ignored_causal_claim_count"] == report["rca_phrase_robustness"]["phrases"]
    assert report["rca_precision"]["false_positive_suppression_count"] == 0
    assert report["rca_precision"]["evidence_requirement_count"] == report["rca_precision"]["phrases"]
    assert report["noise_injection"]["failures"] == []
    assert report["contradictory_artifacts"]["passed"] is True
