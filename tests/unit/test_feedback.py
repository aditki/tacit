import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tacit.api.app import create_app
from tacit.config import Settings
from tacit.feedback import FeedbackStore
from tacit.models.schemas import Intent, MetricEntry
from tacit.ranking import prerank_metrics


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


def test_preranking_keeps_raw_feedback_assessment_only(tmp_path, monkeypatch):
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
    assert [metric.name for metric in first] == ["alpha_latency"]
    assert [metric.name for metric in second] == ["alpha_latency"]
    assert first_store.analyze_calls == 0
    assert second_store.analyze_calls == 0


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


def test_preranking_does_not_initialize_unavailable_feedback_store():
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
    assert calls == 0


def test_feedback_store_isolates_duplicate_dashboard_uids_by_tenant(tmp_path):
    store = FeedbackStore(
        tmp_path / "feedback.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
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


def test_feedback_requires_dashboard_provenance_in_the_same_tenant(tmp_path):
    store = FeedbackStore(
        tmp_path / "feedback.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
    store.record_provenance("shared-dashboard", "Tenant A prompt", tenant_id="tenant-a")

    with pytest.raises(ValueError, match="same tenant"):
        store.submit_feedback("shared-dashboard", overall_useful=True, tenant_id="tenant-b")

    assert store.get_feedback("shared-dashboard", tenant_id="tenant-a") == []
    assert store.get_feedback("shared-dashboard", tenant_id="tenant-b") == []


def test_wildcard_feedback_store_requires_an_explicit_tenant(tmp_path):
    store = FeedbackStore(
        tmp_path / "feedback.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )

    operations = [
        lambda: store.record_provenance("dashboard", "Prompt"),
        lambda: store.get_provenance("dashboard"),
        lambda: store.submit_feedback("dashboard", overall_useful=True),
        lambda: store.get_feedback("dashboard"),
        lambda: store.get_aggregate_stats(),
        lambda: store.analyze(),
    ]
    for operation in operations:
        with pytest.raises(ValueError, match="tenant"):
            operation()


def test_feedback_store_rejects_cross_tenant_calls_in_a_pinned_runtime(tmp_path):
    store = FeedbackStore(
        tmp_path / "feedback.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    with pytest.raises(ValueError, match="Tenant access denied"):
        store.get_aggregate_stats(tenant_id="tenant-b")


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
    with pytest.raises(ValueError, match="Tenant access denied"):
        store.get_provenance("legacy-dashboard", tenant_id="default")
    assert store.get_feedback("legacy-dashboard", tenant_id="tenant-a")[0]["reviewer"] == "legacy-reviewer"


def test_wildcard_feedback_migration_refuses_ownerless_rows_before_schema_mutation(tmp_path):
    db_path = tmp_path / "ownerless-feedback.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""CREATE TABLE dashboard_provenance (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   dashboard_uid TEXT NOT NULL UNIQUE
               );
               INSERT INTO dashboard_provenance (dashboard_uid) VALUES ('private-dashboard');""")

    with pytest.raises(RuntimeError, match="Legacy feedback data has no tenant owner"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
        )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")}
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "tenant_id" not in columns
    assert "dashboard_provenance_legacy_tenant" not in tables
    assert "feedback" not in tables


def test_wildcard_feedback_migration_rebuilds_empty_legacy_tables(tmp_path):
    db_path = tmp_path / "empty-legacy-feedback.db"
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
               );""")

    store = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
    store.record_provenance("tenant-a-dashboard", "Tenant A prompt", tenant_id="tenant-a")

    assert store.get_provenance("tenant-a-dashboard", tenant_id="tenant-a") is not None
    assert store.get_provenance("tenant-a-dashboard", tenant_id="default") is None
    with sqlite3.connect(db_path) as conn:
        provenance_columns = {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")}
        feedback_columns = {row[1] for row in conn.execute("PRAGMA table_info(feedback)")}
    assert "tenant_id" in provenance_columns
    assert "tenant_id" in feedback_columns


def test_complete_schema_default_feedback_requires_and_records_a_migration_owner(tmp_path):
    db_path = tmp_path / "previously-migrated-feedback.db"
    original = FeedbackStore(db_path, runtime_settings=Settings(_env_file=None))
    original.record_provenance("legacy-dashboard", "Legacy prompt")
    original.submit_feedback("legacy-dashboard", reviewer="legacy", overall_useful=True)
    with original._conn() as conn:
        conn.execute("DROP TABLE feedback_tenant_migration_metadata")

    with pytest.raises(RuntimeError, match="unconfirmed default-tenant ownership"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
        )

    pinned = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    assert pinned.get_provenance("legacy-dashboard", tenant_id="tenant-a") is not None
    assert pinned.get_feedback("legacy-dashboard", tenant_id="tenant-a")[0]["reviewer"] == "legacy"

    wildcard = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
    assert wildcard.get_provenance("legacy-dashboard", tenant_id="tenant-a") is not None
    assert wildcard.get_provenance("legacy-dashboard", tenant_id="default") is None


def test_feedback_owner_migration_resumes_bounded_batches_for_the_same_owner(tmp_path, monkeypatch):
    db_path = tmp_path / "resumable-feedback-owner.db"
    original = FeedbackStore(db_path, runtime_settings=Settings(_env_file=None))
    for index in range(5):
        dashboard_uid = f"dashboard-{index}"
        original.record_provenance(dashboard_uid, f"Prompt {index}")
        original.submit_feedback(dashboard_uid, reviewer="reviewer", overall_useful=True)
    with original._conn() as conn:
        conn.execute(
            "DELETE FROM feedback_tenant_migration_metadata WHERE key=?",
            ("default_owner_v1",),
        )

    original_reconcile = FeedbackStore._reconcile_default_tenant_owner_batched

    def migrate_one_batch_then_stop(store: FeedbackStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, _operation, row_count = store._reconcile_default_tenant_owner_batch(
                conn,
                batch_size=2,
            )
        assert complete is False
        assert row_count == 2
        raise RuntimeError("simulated process interruption")

    monkeypatch.setattr(FeedbackStore, "_reconcile_default_tenant_owner_batched", migrate_one_batch_then_stop)
    with pytest.raises(RuntimeError, match="simulated process interruption"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        progress = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key='default_owner_in_progress_v1'"
        ).fetchone()
        cursor = conn.execute("""SELECT value FROM feedback_tenant_migration_metadata
               WHERE key='default_owner_cursor_v1:dashboard_provenance'""").fetchone()
        assert conn.execute("SELECT COUNT(*) FROM dashboard_provenance WHERE tenant_id='tenant-a'").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM feedback WHERE tenant_id='tenant-a'").fetchone()[0] == 0
    assert progress == ("tenant-a",)
    assert cursor is not None
    assert int(cursor[0]) > 0

    monkeypatch.setattr(FeedbackStore, "_reconcile_default_tenant_owner_batched", original_reconcile)
    with pytest.raises(RuntimeError, match="already in progress for another tenant"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
        )

    resumed = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with resumed._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM dashboard_provenance WHERE tenant_id='tenant-a'").fetchone()[0] == 5
        assert conn.execute("SELECT COUNT(*) FROM feedback WHERE tenant_id='tenant-a'").fetchone()[0] == 5
        assert (
            conn.execute(
                "SELECT 1 FROM feedback_tenant_migration_metadata WHERE key='default_owner_in_progress_v1'"
            ).fetchone()
            is None
        )
        assert conn.execute("""SELECT 1 FROM feedback_tenant_migration_metadata
               WHERE key LIKE 'default_owner_cursor_v1:%'""").fetchone() is None


def test_feedback_store_rejects_a_configured_owner_change(tmp_path):
    db_path = tmp_path / "feedback-owner-change.db"
    FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    with pytest.raises(RuntimeError, match="tenant owner does not match"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
        )


def test_feedback_owner_migration_pages_use_tenant_id_indexes(tmp_path):
    store = FeedbackStore(tmp_path / "feedback-owner-query-plan.db")

    with store._conn() as conn:
        provenance_plan = " ".join(
            str(row["detail"])
            for row in conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT id FROM dashboard_provenance
                   WHERE tenant_id='default' AND id>? ORDER BY id LIMIT ?""",
                (0, 500),
            )
        )
        feedback_plan = " ".join(
            str(row["detail"])
            for row in conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT id FROM feedback
                   WHERE tenant_id='default' AND id>? ORDER BY id LIMIT ?""",
                (0, 500),
            )
        )

    assert "idx_provenance_tenant_id" in provenance_plan
    assert "idx_feedback_tenant_id" in feedback_plan
    assert "TEMP B-TREE" not in provenance_plan
    assert "TEMP B-TREE" not in feedback_plan


def test_feedback_api_requires_and_applies_the_selected_wildcard_tenant(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="*",
        api_auth_enabled=True,
        knowledge_tenant_api_keys={
            "tenant-a": "tenant-a-secret",
            "tenant-b": "tenant-b-secret",
        },
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
        headers={"X-Tacit-Tenant": "tenant-a", "X-API-Key": "tenant-a-secret"},
    )
    submitted = client.post(
        "/api/v1/feedback",
        headers={"X-Tacit-Tenant": "tenant-a", "X-API-Key": "tenant-a-secret"},
        json={"dashboard_uid": "shared-dashboard", "overall_useful": True},
    )
    tenant_b_stats = client.get(
        "/api/v1/feedback/stats",
        headers={"X-Tacit-Tenant": "tenant-b", "X-API-Key": "tenant-b-secret"},
    )

    assert tenant_a.status_code == 200
    assert tenant_a.json()["provenance"]["prompt"] == "Tenant A prompt"
    assert submitted.status_code == 200
    assert tenant_b_stats.status_code == 200
    assert tenant_b_stats.json()["total_feedback"] == 0


def test_feedback_api_rejects_unknown_dashboard_in_selected_tenant(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="*",
        api_auth_enabled=True,
        knowledge_tenant_api_keys={
            "tenant-a": "tenant-a-secret",
            "tenant-b": "tenant-b-secret",
        },
        feedback_db_path=str(tmp_path / "feedback.db"),
    )
    app = create_app(runtime_settings=runtime_settings)
    store = app.state.runtime_stores.feedback()
    store.record_provenance("shared-dashboard", "Tenant A prompt", tenant_id="tenant-a")
    client = TestClient(app)

    response = client.post(
        "/api/v1/feedback",
        headers={"X-Tacit-Tenant": "tenant-b", "X-API-Key": "tenant-b-secret"},
        json={"dashboard_uid": "shared-dashboard", "overall_useful": True},
    )

    assert response.status_code == 404
    assert "same tenant" in response.json()["detail"]


def test_tenant_feedback_does_not_bypass_governed_runtime_ranking(tmp_path):
    store = FeedbackStore(
        tmp_path / "feedback.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*"),
    )
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
    assert [metric.name for metric in tenant_b] == ["alpha_latency"]
