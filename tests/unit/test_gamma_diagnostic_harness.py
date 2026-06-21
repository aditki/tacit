from tests.eval.gamma_diagnostic_harness import (
    _detect_cause_assertion,
    _evaluate_controls,
    _evaluate_predictions,
)


def _arm(dashboards: int, coverage: float):
    return {
        "dashboards_created": dashboards,
        "independent_prompts": 1,
        "results": [
            {
                "evidence_signals": ["cpu", "memory"] if dashboards else [],
                "cache_stats": {
                    "metric": {"hits": 0, "misses": 1, "size": 1},
                    "llm": {"hits": 0, "misses": 0, "size": 0},
                },
                "stages": {
                    "semantic_mapping": {
                        "details": {"coverage": coverage},
                    }
                }
            }
        ],
    }


def test_pre_fix_predictions_require_mapping_stability_and_binding_cliff():
    result = _evaluate_predictions(
        {
            "canonical": _arm(1, 1.0),
            "prefixed": _arm(0, 1.0),
            "raw": _arm(0, 0.8),
        }
    )

    assert result["passed"] is True
    assert all(result["checks"].values())


def test_pre_fix_predictions_fail_when_prefix_changes_semantic_mapping():
    result = _evaluate_predictions(
        {
            "canonical": _arm(1, 1.0),
            "prefixed": _arm(0, 0.5),
            "raw": _arm(0, 0.8),
        }
    )

    assert result["passed"] is False
    assert result["checks"]["prefix_only_preserves_mapping_coverage"] is False


def test_post_fix_predictions_require_prefixed_dashboards():
    result = _evaluate_predictions(
        {
            "canonical": _arm(1, 1.0),
            "prefixed": _arm(1, 1.0),
            "raw": _arm(0, 0.8),
        },
        expectation="post-fix",
    )

    assert result["passed"] is True
    assert result["checks"]["prefixed_all_prompts_bind_post_fix"] is True
    assert result["checks"]["raw_ambiguous_binding_abstains"] is True


def test_post_fix_predictions_reject_arbitrary_raw_binding():
    result = _evaluate_predictions(
        {
            "canonical": _arm(1, 1.0),
            "prefixed": _arm(1, 1.0),
            "raw": _arm(1, 1.0),
        },
        expectation="post-fix",
    )

    assert result["passed"] is False
    assert result["checks"]["raw_ambiguous_binding_abstains"] is False


def test_post_fix_predictions_fail_when_positive_arm_goes_silent():
    prefixed = _arm(1, 1.0)
    prefixed["results"][0]["evidence_signals"] = []
    result = _evaluate_predictions(
        {
            "canonical": _arm(1, 1.0),
            "prefixed": prefixed,
            "raw": _arm(0, 1.0),
        },
        expectation="post-fix",
    )

    assert result["passed"] is False
    assert result["checks"]["prefixed_evidence_recall_meets_gate"] is False
    assert result["counts"]["prefixed_evidence_recall"] == {
        "numerator": 0,
        "denominator": 2,
        "recall": 0.0,
    }


def test_cause_assertion_detector_is_independent_of_ranking():
    assertion = _detect_cause_assertion(
        "Created an investigation dashboard.",
        [{"panel_title": "CPU identified as root cause", "expr": "cpu_usage"}],
    )

    assert assertion == {"asserted": True, "matches": ["root cause"]}


def test_two_controls_are_provisional_not_an_acceptance_gate():
    skipped_ranking = {"status": "skipped", "reason_code": "culprit_ranking_not_implemented"}
    result = _evaluate_controls(
        {
            "healthy": {
                "kind": "healthy",
                "family": "cpu",
                "scenario": "healthy-1",
                "unsupported_cause_asserted": False,
                "panel_count": 2,
                "cache_stats": {
                    "metric": {"hits": 0},
                    "llm": {"hits": 0},
                },
                "stages": {"ranking": skipped_ranking},
            },
            "evidence_absent": {
                "kind": "evidence_absent",
                "family": "memory",
                "scenario": "symptom-1",
                "unsupported_cause_asserted": False,
                "panel_count": 0,
                "cache_stats": {
                    "metric": {"hits": 0},
                    "llm": {"hits": 0},
                },
                "stages": {
                    "semantic_mapping": {"details": {"coverage": 1.0}},
                    "ranking": skipped_ranking,
                },
            },
        }
    )

    assert result["passed"] is False
    assert result["checks"]["evidence_absent_discovers_symptom"] is True
    assert result["checks"]["control_sample_size_meets_gate"] is False
    assert result["counts"]["false_culprit"] == {"numerator": 0, "denominator": 2}
    assert result["known_gaps"] == {
        "evidence_absent_preserves_symptom_panel": {
            "numerator": 0,
            "denominator": 1,
            "recall": 0.0,
        },
        "culprit_ranking_available": False,
    }


def test_controls_fail_when_a_culprit_is_asserted_without_evidence():
    result = _evaluate_controls(
        {
            "healthy": {
                "kind": "healthy",
                "family": "cpu",
                "scenario": "healthy-1",
                "unsupported_cause_asserted": True,
                "panel_count": 1,
                "cache_stats": {
                    "metric": {"hits": 0},
                    "llm": {"hits": 0},
                },
                "stages": {"ranking": {"status": "passed"}},
            },
            "evidence_absent": {
                "kind": "evidence_absent",
                "family": "network",
                "scenario": "symptom-1",
                "unsupported_cause_asserted": False,
                "panel_count": 1,
                "cache_stats": {
                    "metric": {"hits": 0},
                    "llm": {"hits": 0},
                },
                "stages": {
                    "semantic_mapping": {"details": {"coverage": 1.0}},
                    "ranking": {"status": "skipped"},
                },
            },
        }
    )

    assert result["passed"] is False
    assert result["checks"]["healthy_does_not_assert_culprit"] is False


def test_twenty_diverse_controls_make_the_safety_gate_evaluable():
    controls = {}
    for index in range(20):
        kind = "healthy" if index < 10 else "evidence_absent"
        controls[str(index)] = {
            "kind": kind,
            "family": ("cpu", "memory", "network", "cpu_memory")[index % 4],
            "scenario": f"scenario-{index}",
            "unsupported_cause_asserted": False,
            "panel_count": 1,
            "cache_stats": {"metric": {"hits": 0}, "llm": {"hits": 0}},
            "stages": {
                "semantic_mapping": {"details": {"coverage": 1.0}},
                "ranking": {"status": "skipped"},
            },
        }

    result = _evaluate_controls(controls)

    assert result["passed"] is True
    assert result["counts"]["false_culprit"] == {"numerator": 0, "denominator": 20}
    assert result["checks"]["control_families_are_diverse"] is True
