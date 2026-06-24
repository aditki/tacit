from tacit.feedback import FeedbackStore


def test_empty_feedback_stats_match_api_response_model(tmp_path):
    store = FeedbackStore(tmp_path / "feedback.db")

    assert store.get_aggregate_stats() == {
        "total_feedback": 0,
        "total_dashboards": 0,
        "useful_rate": None,
        "avg_symptom_visibility": None,
        "avg_root_cause_support": None,
        "avg_noise_level": None,
        "avg_investigation_speed": None,
    }
