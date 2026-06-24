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


def test_feature_weights_are_explicit_and_stable():
    assert WEIGHTS == {
        "runtime_observation_match": 40,
        "direct_dependency": 25,
        "recent_deploy": 20,
        "runbook_match": 15,
        "historical_incident_match": 15,
        "ownership_context": 5,
        "dashboard_association": 5,
        "stale_artifact": -20,
        "contradictory_evidence": -30,
    }


def test_contextual_culprit_ranking_fixture_gate_passes():
    report = evaluate()

    assert gate_failures(report) == []
    assert report["case_count"] == 47
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
