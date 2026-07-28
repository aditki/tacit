import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from tacit.api.app import create_app
from tacit.config import Settings
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

    def analyze(self, *, tenant_id: str = "default"):
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


def test_feedback_store_isolates_duplicate_dashboard_uids_by_tenant(tmp_path):
    store = FeedbackStore(tmp_path / "feedback.db")
    store.record_provenance(
        "shared-dashboard",
        "Tenant A prompt",
        metrics_used=["tenant_a_latency"],
        tenant_id="tenant-a",
    )
    store.record_provenance(
        "shared-dashboard",
        "Tenant B prompt",
        metrics_used=["tenant_b_latency"],
        tenant_id="tenant-b",
    )
    store.submit_feedback(
        "shared-dashboard",
        overall_useful=True,
        reviewer="reviewer-a",
        tenant_id="tenant-a",
    )
    store.submit_feedback(
        "shared-dashboard",
        overall_useful=False,
        reviewer="reviewer-b",
        tenant_id="tenant-b",
    )

    assert store.get_provenance("shared-dashboard", tenant_id="tenant-a")["prompt"] == "Tenant A prompt"
    assert store.get_provenance("shared-dashboard", tenant_id="tenant-b")["prompt"] == "Tenant B prompt"
    assert [row["reviewer"] for row in store.get_feedback("shared-dashboard", tenant_id="tenant-a")] == ["reviewer-a"]
    assert store.get_aggregate_stats(tenant_id="tenant-a")["useful_rate"] == 1.0
    assert store.get_aggregate_stats(tenant_id="tenant-b")["useful_rate"] == 0


def test_feedback_migration_uses_the_configured_pinned_tenant(tmp_path):
    db_path = tmp_path / "legacy-feedback.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""CREATE TABLE dashboard_provenance (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   dashboard_uid TEXT NOT NULL UNIQUE,
                   prompt TEXT NOT NULL,
                   problem_type TEXT NOT NULL DEFAULT '',
                   archetypes TEXT NOT NULL DEFAULT '[]',
                   metrics_used TEXT NOT NULL DEFAULT '[]',
                   panel_count INTEGER NOT NULL DEFAULT 0,
                   path_used TEXT NOT NULL DEFAULT '',
                   dashboard_url TEXT NOT NULL DEFAULT '',
                   user_id TEXT NOT NULL DEFAULT '',
                   channel_id TEXT NOT NULL DEFAULT '',
                   created_at REAL NOT NULL
               );
               CREATE TABLE feedback (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   dashboard_uid TEXT NOT NULL,
                   reviewer TEXT NOT NULL DEFAULT '',
                   symptom_visibility INTEGER,
                   root_cause_support INTEGER,
                   noise_level INTEGER,
                   investigation_speed INTEGER,
                   overall_useful INTEGER,
                   comment TEXT NOT NULL DEFAULT '',
                   created_at REAL NOT NULL
               );
               INSERT INTO dashboard_provenance
                   (dashboard_uid, prompt, created_at)
               VALUES ('legacy-dashboard', 'Legacy prompt', 1);
               INSERT INTO feedback
                   (dashboard_uid, reviewer, overall_useful, created_at)
               VALUES ('legacy-dashboard', 'legacy-reviewer', 1, 1);""")

    store = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert store.get_provenance("legacy-dashboard", tenant_id="tenant-a") is not None
    assert store.get_provenance("legacy-dashboard", tenant_id="default") is None
    assert store.get_feedback("legacy-dashboard", tenant_id="tenant-a")[0]["reviewer"] == "legacy-reviewer"


def test_feedback_api_requires_and_applies_the_selected_wildcard_tenant(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="*",
        feedback_db_path=str(tmp_path / "feedback.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    store = app.state.runtime_stores.feedback()
    store.record_provenance("shared-dashboard", "Tenant A prompt", tenant_id="tenant-a")
    store.record_provenance("shared-dashboard", "Tenant B prompt", tenant_id="tenant-b")
    client = TestClient(app)

    assert client.get("/api/v1/feedback/shared-dashboard").status_code == 400
    tenant_a = client.get(
        "/api/v1/feedback/shared-dashboard",
        headers={"X-Tacit-Tenant": "tenant-a"},
    )
    submitted = client.post(
        "/api/v1/feedback",
        headers={"X-Tacit-Tenant": "tenant-a"},
        json={"dashboard_uid": "shared-dashboard", "overall_useful": True},
    )
    tenant_b_stats = client.get(
        "/api/v1/feedback/stats",
        headers={"X-Tacit-Tenant": "tenant-b"},
    )

    assert tenant_a.status_code == 200
    assert tenant_a.json()["provenance"]["prompt"] == "Tenant A prompt"
    assert submitted.status_code == 200
    assert tenant_b_stats.status_code == 200
    assert tenant_b_stats.json()["total_feedback"] == 0


def test_feedback_metric_quality_cache_is_tenant_scoped(tmp_path):
    invalidate_metric_quality_cache()
    store = FeedbackStore(tmp_path / "feedback.db")
    for tenant_id, useful_metric, poor_metric in (
        ("tenant-a", "alpha_latency", "beta_latency"),
        ("tenant-b", "beta_latency", "alpha_latency"),
    ):
        store.record_provenance(
            f"{tenant_id}-useful",
            "Useful",
            metrics_used=[useful_metric],
            tenant_id=tenant_id,
        )
        store.submit_feedback(f"{tenant_id}-useful", overall_useful=True, tenant_id=tenant_id)
        store.record_provenance(
            f"{tenant_id}-poor",
            "Poor",
            metrics_used=[poor_metric],
            tenant_id=tenant_id,
        )
        store.submit_feedback(f"{tenant_id}-poor", overall_useful=False, tenant_id=tenant_id)

    intent = Intent(summary="latency", domain="application", keywords=["latency"])
    catalog = [_metric("alpha_latency"), _metric("beta_latency")]

    tenant_a = prerank_metrics(
        intent,
        catalog,
        max_candidates=1,
        feedback_store=store,
        tenant_id="tenant-a",
    )
    tenant_b = prerank_metrics(
        intent,
        catalog,
        max_candidates=1,
        feedback_store=store,
        tenant_id="tenant-b",
    )

    assert [metric.name for metric in tenant_a] == ["alpha_latency"]
    assert [metric.name for metric in tenant_b] == ["beta_latency"]
