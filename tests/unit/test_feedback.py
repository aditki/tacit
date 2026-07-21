from pathlib import Path

from tacit.feedback import FeedbackStore
from tacit.models.schemas import Intent, MetricEntry
from tacit.ranking import invalidate_metric_quality_cache, prerank_metrics


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


class _MetricQualityStore:
    def __init__(self, db_path: Path, scores: dict[str, float]):
        self._db_path = db_path
        self.scores = scores
        self.analyze_calls = 0

    def analyze(self):
        self.analyze_calls += 1
        return {"metric_quality": [{"metric": metric, "quality_score": score} for metric, score in self.scores.items()]}


def _metric(name: str) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid="prom",
        datasource_name="Prometheus",
        datasource_type="prometheus",
        query_language="promql",
    )


def test_preranking_uses_feedback_and_cache_from_the_supplied_store(tmp_path, monkeypatch):
    invalidate_metric_quality_cache()
    first_store = _MetricQualityStore(
        tmp_path / "first.db",
        {"alpha_latency": 0.9, "beta_latency": 0.1},
    )
    second_store = _MetricQualityStore(
        tmp_path / "second.db",
        {"alpha_latency": 0.1, "beta_latency": 0.9},
    )
    monkeypatch.setattr(
        "tacit.feedback.get_feedback_store",
        lambda: (_ for _ in ()).throw(AssertionError("global feedback store was consulted")),
    )
    intent = Intent(
        summary="latency",
        domain="application",
        keywords=["latency"],
    )
    catalog = [_metric("alpha_latency"), _metric("beta_latency")]

    first = prerank_metrics(intent, catalog, max_candidates=1, feedback_store=first_store)
    second = prerank_metrics(intent, catalog, max_candidates=1, feedback_store=second_store)
    cached_first = prerank_metrics(intent, catalog, max_candidates=1, feedback_store=first_store)

    assert [metric.name for metric in first] == ["alpha_latency"]
    assert [metric.name for metric in second] == ["beta_latency"]
    assert [metric.name for metric in cached_first] == ["alpha_latency"]
    assert first_store.analyze_calls == 1
    assert second_store.analyze_calls == 1


def test_preranking_does_not_open_feedback_store_for_small_catalogs():
    def unavailable_store():
        raise AssertionError("feedback store should not be initialized")

    catalog = [_metric("alpha_latency"), _metric("beta_latency")]
    ranked = prerank_metrics(
        Intent(summary="latency", domain="application", keywords=["latency"]),
        catalog,
        max_candidates=2,
        feedback_store_factory=unavailable_store,
    )

    assert ranked == catalog


def test_preranking_tolerates_feedback_store_initialization_failure():
    calls = 0

    def unavailable_store():
        nonlocal calls
        calls += 1
        raise OSError("feedback database unavailable")

    ranked = prerank_metrics(
        Intent(summary="latency", domain="application", keywords=["latency"]),
        [_metric("alpha_latency"), _metric("beta_latency")],
        max_candidates=1,
        feedback_store_factory=unavailable_store,
    )

    assert len(ranked) == 1
    assert calls == 1
