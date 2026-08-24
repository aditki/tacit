import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Lock

import pytest
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

import tacit.feedback as feedback_module
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
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
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
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )
    store.record_provenance("shared-dashboard", "Tenant A prompt", tenant_id="tenant-a")

    with pytest.raises(ValueError, match="same tenant"):
        store.submit_feedback("shared-dashboard", overall_useful=True, tenant_id="tenant-b")

    assert store.get_feedback("shared-dashboard", tenant_id="tenant-a") == []
    assert store.get_feedback("shared-dashboard", tenant_id="tenant-b") == []


def test_wildcard_feedback_store_requires_an_explicit_tenant(tmp_path):
    store = FeedbackStore(
        tmp_path / "feedback.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
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


def _create_pre_tenant_feedback_database(db_path: Path, *, row_count: int) -> None:
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
        conn.executemany(
            """INSERT INTO dashboard_provenance
               (dashboard_uid, prompt, created_at) VALUES (?, ?, ?)""",
            ((f"legacy-{index}", f"Prompt {index}", float(index)) for index in range(1, row_count + 1)),
        )
        conn.executemany(
            """INSERT INTO feedback
               (dashboard_uid, reviewer, overall_useful, created_at)
               VALUES (?, ?, 1, ?)""",
            ((f"legacy-{index}", f"reviewer-{index}", float(index)) for index in range(1, row_count + 1)),
        )


def _create_pre_tenant_feedback_database_with_ids(db_path: Path, ids: tuple[int, ...]) -> None:
    _create_pre_tenant_feedback_database(db_path, row_count=0)
    with sqlite3.connect(db_path) as conn:
        for index, row_id in enumerate(ids):
            dashboard_uid = f"boundary-{index}"
            conn.execute(
                """INSERT INTO dashboard_provenance
                   (id, dashboard_uid, prompt, created_at) VALUES (?, ?, ?, ?)""",
                (row_id, dashboard_uid, f"Prompt {index}", float(index)),
            )
            conn.execute(
                """INSERT INTO feedback
                   (id, dashboard_uid, reviewer, overall_useful, created_at)
                   VALUES (?, ?, ?, 1, ?)""",
                (row_id, dashboard_uid, f"reviewer-{index}", float(index)),
            )


def _feedback_database_state(db_path: Path) -> dict[str, object]:
    uri = f"{db_path.resolve().as_uri()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    try:
        journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0])
        schema_version = int(conn.execute("PRAGMA schema_version").fetchone()[0])
        schema = tuple(conn.execute("""SELECT type, name, tbl_name, COALESCE(sql, '')
                   FROM sqlite_master ORDER BY type, name""").fetchall())
        metadata_exists = conn.execute("""SELECT 1 FROM sqlite_master
                   WHERE type='table' AND name='feedback_tenant_migration_metadata'""").fetchone() is not None
        metadata = tuple(conn.execute("""SELECT key, value, updated_at
                       FROM feedback_tenant_migration_metadata ORDER BY key""").fetchall()) if metadata_exists else ()
    finally:
        conn.close()
    sidecars = tuple(
        (suffix, sidecar.read_bytes() if sidecar.exists() else None)
        for suffix in ("-wal", "-shm", "-journal")
        for sidecar in (Path(f"{db_path}{suffix}"),)
    )
    return {
        "main": db_path.read_bytes(),
        "sidecars": sidecars,
        "journal_mode": journal_mode,
        "schema_version": schema_version,
        "schema": schema,
        "metadata": metadata,
    }


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


def test_feedback_first_open_schema_failure_rolls_back_identity_and_partial_schema(tmp_path, monkeypatch):
    db_path = tmp_path / "failed-feedback-first-open.db"
    original_schema = feedback_module._SCHEMA_SQL
    monkeypatch.setattr(
        feedback_module,
        "_SCHEMA_SQL",
        original_schema + """
        CREATE TABLE partial_schema_probe (id INTEGER PRIMARY KEY);
        CREATE TABLE invalid feedback schema;
        """,
    )

    with pytest.raises(sqlite3.OperationalError):
        FeedbackStore(db_path)

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "partial_schema_probe" not in tables
    assert "dashboard_provenance" not in tables
    assert "feedback" not in tables
    assert "feedback_tenant_migration_metadata" not in tables
    assert "tacit_runtime_database_identity" not in tables

    monkeypatch.setattr(feedback_module, "_SCHEMA_SQL", original_schema)
    retried = FeedbackStore(db_path)
    retried.record_provenance("retry-dashboard", "Retry prompt")
    assert retried.get_provenance("retry-dashboard") is not None


def test_feedback_legacy_schema_failure_rolls_back_migration_and_retries(tmp_path, monkeypatch):
    db_path = tmp_path / "failed-legacy-feedback.db"
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

    original_schema = feedback_module._SCHEMA_SQL
    monkeypatch.setattr(
        feedback_module,
        "_SCHEMA_SQL",
        original_schema + """
        CREATE TABLE partial_schema_probe (id INTEGER PRIMARY KEY);
        CREATE TABLE invalid feedback schema;
        """,
    )
    tenant_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")

    with pytest.raises(sqlite3.OperationalError):
        FeedbackStore(db_path, runtime_settings=tenant_settings)

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        provenance_columns = {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")}
        legacy_provenance = conn.execute("SELECT dashboard_uid, prompt FROM dashboard_provenance").fetchall()
        legacy_feedback = conn.execute("SELECT dashboard_uid, reviewer FROM feedback").fetchall()
    assert "partial_schema_probe" not in tables
    assert "dashboard_provenance_legacy_tenant" not in tables
    assert "feedback_legacy_tenant" not in tables
    assert "feedback_tenant_migration_metadata" not in tables
    assert "tacit_runtime_database_identity" not in tables
    assert "tenant_id" not in provenance_columns
    assert legacy_provenance == [("legacy-dashboard", "Legacy prompt")]
    assert legacy_feedback == [("legacy-dashboard", "legacy-reviewer")]

    monkeypatch.setattr(feedback_module, "_SCHEMA_SQL", original_schema)
    retried = FeedbackStore(db_path, runtime_settings=tenant_settings)
    assert retried.get_provenance("legacy-dashboard", tenant_id="tenant-a") is not None
    assert retried.get_feedback("legacy-dashboard", tenant_id="tenant-a")[0]["reviewer"] == "legacy-reviewer"


def test_feedback_legacy_migration_structural_setup_is_rollback_atomic(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy-feedback-structural-failure.db"
    _create_pre_tenant_feedback_database(db_path, row_count=1)
    original_prepare = FeedbackStore._migrate_tenant_scope

    def fail_after_structural_setup(store: FeedbackStore, conn: sqlite3.Connection) -> None:
        original_prepare(store, conn)
        conn.execute("CREATE TABLE feedback_migration_failure_probe (id INTEGER PRIMARY KEY)")
        raise RuntimeError("simulated structural migration failure")

    monkeypatch.setattr(FeedbackStore, "_migrate_tenant_scope", fail_after_structural_setup)
    with pytest.raises(RuntimeError, match="simulated structural migration failure"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        provenance_columns = {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")}
        assert conn.execute("SELECT COUNT(*) FROM dashboard_provenance").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == 1
    assert "tenant_id" not in provenance_columns
    assert "dashboard_provenance_tenant_migration_v2" not in tables
    assert "feedback_tenant_migration_v2" not in tables
    assert "feedback_tenant_migration_metadata" not in tables
    assert "feedback_migration_failure_probe" not in tables
    assert "tacit_runtime_database_identity" not in tables


def test_feedback_pre_tenant_migration_resumes_after_a_committed_bounded_batch(tmp_path, monkeypatch):
    db_path = tmp_path / "resumable-pre-tenant-feedback.db"
    row_count = feedback_module._OWNER_MIGRATION_BATCH_SIZE + 1
    _create_pre_tenant_feedback_database(db_path, row_count=row_count)
    original_migrate = FeedbackStore._migrate_tenant_scope_batched

    def migrate_one_batch_then_stop(store: FeedbackStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, operation, copied = store._migrate_tenant_scope_batch(
                conn,
                batch_size=feedback_module._OWNER_MIGRATION_BATCH_SIZE,
            )
        assert complete is False
        assert operation == "dashboard_provenance:copy"
        assert copied == feedback_module._OWNER_MIGRATION_BATCH_SIZE
        raise RuntimeError("simulated interruption after committed legacy batch")

    monkeypatch.setattr(FeedbackStore, "_migrate_tenant_scope_batched", migrate_one_batch_then_stop)
    with pytest.raises(RuntimeError, match="simulated interruption after committed legacy batch"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        progress = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key='legacy_tenant_scope_owner_v2'"
        ).fetchone()
        cursor = conn.execute("""SELECT value FROM feedback_tenant_migration_metadata
               WHERE key='legacy_tenant_scope_cursor_v2:dashboard_provenance'""").fetchone()
        role = conn.execute("SELECT role FROM tacit_runtime_database_identity WHERE singleton=1").fetchone()
        assert conn.execute("SELECT COUNT(*) FROM dashboard_provenance").fetchone()[0] == row_count
        assert conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == row_count
        assert (
            conn.execute("SELECT COUNT(*) FROM dashboard_provenance_tenant_migration_v2").fetchone()[0]
            == feedback_module._OWNER_MIGRATION_BATCH_SIZE
        )
        assert conn.execute("SELECT COUNT(*) FROM feedback_tenant_migration_v2").fetchone()[0] == 0
    assert progress == ("tenant-a",)
    assert cursor == (f"id:{feedback_module._OWNER_MIGRATION_BATCH_SIZE}",)
    assert role == ("feedback",)
    assert "dashboard_provenance_tenant_migration_v2" in tables
    assert "feedback_tenant_migration_v2" in tables

    denied_before = _feedback_database_state(db_path)
    with capture_logs() as logs, pytest.raises(RuntimeError, match="migration_owner_mismatch") as exc_info:
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
        )
    assert _feedback_database_state(db_path) == denied_before
    assert "tenant-a" not in str(exc_info.value) + repr(logs)
    assert "tenant-b" not in str(exc_info.value) + repr(logs)

    monkeypatch.setattr(FeedbackStore, "_migrate_tenant_scope_batched", original_migrate)
    resumed = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    with resumed._conn() as conn:
        tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        provenance = conn.execute(
            "SELECT id, tenant_id, dashboard_uid FROM dashboard_provenance ORDER BY id"
        ).fetchall()
        feedback = conn.execute("SELECT id, tenant_id, dashboard_uid FROM feedback ORDER BY id").fetchall()
        owner = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (feedback_module._DEFAULT_OWNER_MARKER,),
        ).fetchone()
        in_progress = conn.execute("""SELECT 1 FROM feedback_tenant_migration_metadata
               WHERE key LIKE 'legacy_tenant_scope_%'""").fetchall()
    assert [(row["id"], row["tenant_id"], row["dashboard_uid"]) for row in provenance] == [
        (index, "tenant-a", f"legacy-{index}") for index in range(1, row_count + 1)
    ]
    assert [(row["id"], row["tenant_id"], row["dashboard_uid"]) for row in feedback] == [
        (index, "tenant-a", f"legacy-{index}") for index in range(1, row_count + 1)
    ]
    assert owner["value"] == "tenant-a"
    assert in_progress == []
    assert "dashboard_provenance_tenant_migration_v2" not in tables
    assert "feedback_tenant_migration_v2" not in tables


def test_feedback_pre_tenant_migration_includes_every_legal_integer_boundary_on_restart(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "boundary-pre-tenant-feedback.db"
    boundary_ids = (-(2**63), -1, 0, 17, 2**63 - 1)
    _create_pre_tenant_feedback_database_with_ids(db_path, boundary_ids)
    original_migrate = FeedbackStore._migrate_tenant_scope_batched

    def migrate_one_boundary_batch_then_stop(store: FeedbackStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, operation, copied = store._migrate_tenant_scope_batch(conn, batch_size=3)
        assert complete is False
        assert operation == "dashboard_provenance:copy"
        assert copied == 3
        raise RuntimeError("simulated boundary migration interruption")

    monkeypatch.setattr(
        FeedbackStore,
        "_migrate_tenant_scope_batched",
        migrate_one_boundary_batch_then_stop,
    )
    with pytest.raises(RuntimeError, match="simulated boundary migration interruption"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        copied_ids = tuple(
            row[0] for row in conn.execute("SELECT id FROM dashboard_provenance_tenant_migration_v2 ORDER BY id")
        )
        cursor = conn.execute("""SELECT value FROM feedback_tenant_migration_metadata
               WHERE key='legacy_tenant_scope_cursor_v2:dashboard_provenance'""").fetchone()
        pending_cursor = conn.execute("""SELECT value FROM feedback_tenant_migration_metadata
               WHERE key='legacy_tenant_scope_cursor_v2:feedback'""").fetchone()
    assert copied_ids == boundary_ids[:3]
    assert cursor == (f"id:{boundary_ids[2]}",)
    assert pending_cursor == ("not_started",)

    monkeypatch.setattr(FeedbackStore, "_migrate_tenant_scope_batched", original_migrate)
    resumed = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with resumed._conn() as conn:
        provenance_ids = tuple(row[0] for row in conn.execute("SELECT id FROM dashboard_provenance ORDER BY id"))
        feedback_ids = tuple(row[0] for row in conn.execute("SELECT id FROM feedback ORDER BY id"))
    assert provenance_ids == boundary_ids
    assert feedback_ids == boundary_ids


def test_feedback_pre_tenant_migration_final_swap_is_atomic(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy-feedback-final-swap.db"
    _create_pre_tenant_feedback_database(db_path, row_count=3)
    original_finalize = FeedbackStore._finalize_tenant_scope_migration

    def fail_after_final_swap(store: FeedbackStore, conn: sqlite3.Connection) -> None:
        original_finalize(store, conn)
        raise RuntimeError("simulated final swap failure")

    monkeypatch.setattr(FeedbackStore, "_finalize_tenant_scope_migration", fail_after_final_swap)
    with pytest.raises(RuntimeError, match="simulated final swap failure"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        provenance_columns = {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")}
        assert conn.execute("SELECT COUNT(*) FROM dashboard_provenance").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0] == 3
    assert "tenant_id" not in provenance_columns
    assert "dashboard_provenance_tenant_migration_v2" in tables
    assert "feedback_tenant_migration_v2" in tables
    assert "dashboard_provenance_legacy_tenant" not in tables
    assert "feedback_legacy_tenant" not in tables

    monkeypatch.setattr(FeedbackStore, "_finalize_tenant_scope_migration", original_finalize)
    resumed = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    assert resumed.get_provenance("legacy-1", tenant_id="tenant-a") is not None
    assert len(resumed.get_feedback("legacy-1", tenant_id="tenant-a")) == 1


def test_feedback_migration_rechecks_schema_after_acquiring_writer_lock(tmp_path, monkeypatch):
    db_path = tmp_path / "concurrent-legacy-feedback.db"
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
               VALUES ('legacy-dashboard', 'Legacy prompt', 1);""")

    first_migration_started = Event()
    second_migration_started = Event()
    call_lock = Lock()
    call_count = 0
    original_migrate = FeedbackStore._migrate_tenant_scope

    def coordinated_migrate(store: FeedbackStore, conn: sqlite3.Connection) -> None:
        nonlocal call_count
        with call_lock:
            call_count += 1
            call_number = call_count
        if call_number == 1:
            first_migration_started.set()
            second_migration_started.wait(timeout=0.25)
        else:
            second_migration_started.set()
        original_migrate(store, conn)

    monkeypatch.setattr(FeedbackStore, "_migrate_tenant_scope", coordinated_migrate)
    owner_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    conflicting_settings = Settings(_env_file=None, knowledge_tenant_id="default")
    with ThreadPoolExecutor(max_workers=2) as pool:
        owner = pool.submit(FeedbackStore, db_path, runtime_settings=owner_settings)
        assert first_migration_started.wait(timeout=2)
        conflicting = pool.submit(FeedbackStore, db_path, runtime_settings=conflicting_settings)
        owner.result(timeout=5)
        with pytest.raises(RuntimeError, match="pinned_owner_mismatch"):
            conflicting.result(timeout=5)

    with sqlite3.connect(db_path) as conn:
        tenant_id = conn.execute(
            "SELECT tenant_id FROM dashboard_provenance WHERE dashboard_uid='legacy-dashboard'"
        ).fetchone()[0]
    assert call_count == 1
    assert tenant_id == "tenant-a"


def test_wildcard_feedback_migration_refuses_ownerless_rows_before_schema_mutation(tmp_path):
    db_path = tmp_path / "ownerless-feedback.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""CREATE TABLE dashboard_provenance (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   dashboard_uid TEXT NOT NULL UNIQUE
               );
               INSERT INTO dashboard_provenance (dashboard_uid) VALUES ('private-dashboard');""")

    before = _feedback_database_state(db_path)
    with capture_logs() as logs, pytest.raises(RuntimeError, match="ownerless_wildcard") as exc_info:
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
        )
    after = _feedback_database_state(db_path)

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")}
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "tenant_id" not in columns
    assert "dashboard_provenance_legacy_tenant" not in tables
    assert "dashboard_provenance_tenant_migration_v2" not in tables
    assert "feedback_tenant_migration_v2" not in tables
    assert "feedback_tenant_migration_metadata" not in tables
    assert "feedback" not in tables
    assert "tacit_runtime_database_identity" not in tables
    assert after == before
    assert "private-dashboard" not in str(exc_info.value)
    assert "private-dashboard" not in repr(logs)
    rejected = [log for log in logs if log.get("event") == "feedback_owner_preflight_rejected"]
    assert rejected == [
        {
            "configured_owner_class": "wildcard",
            "event": "feedback_owner_preflight_rejected",
            "log_level": "error",
            "ownerless_table_count": 1,
            "reason_code": "ownerless_wildcard",
        }
    ]


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
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
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

    with pytest.raises(RuntimeError, match="unconfirmed_default_owner"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
        )

    pinned = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    assert pinned.get_provenance("legacy-dashboard", tenant_id="tenant-a") is not None
    assert pinned.get_feedback("legacy-dashboard", tenant_id="tenant-a")[0]["reviewer"] == "legacy"

    wildcard = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
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
    assert cursor == ("id:2",)

    monkeypatch.setattr(FeedbackStore, "_reconcile_default_tenant_owner_batched", original_reconcile)
    with pytest.raises(RuntimeError, match="migration_owner_mismatch"):
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


def test_feedback_owner_reconciliation_includes_boundary_ids_and_resumes(tmp_path, monkeypatch):
    db_path = tmp_path / "boundary-owner-reconciliation.db"
    boundary_ids = (-(2**63), -1, 0, 17, 2**63 - 1)
    original = FeedbackStore(db_path, runtime_settings=Settings(_env_file=None))
    with original._conn() as conn:
        conn.execute(
            "DELETE FROM feedback_tenant_migration_metadata WHERE key=?",
            (feedback_module._DEFAULT_OWNER_MARKER,),
        )
        for index, row_id in enumerate(boundary_ids):
            dashboard_uid = f"owner-boundary-{index}"
            conn.execute(
                """INSERT INTO dashboard_provenance
                   (id, tenant_id, dashboard_uid, prompt, created_at)
                   VALUES (?, 'default', ?, ?, ?)""",
                (row_id, dashboard_uid, f"Prompt {index}", float(index)),
            )
            conn.execute(
                """INSERT INTO feedback
                   (id, tenant_id, dashboard_uid, reviewer, overall_useful, created_at)
                   VALUES (?, 'default', ?, ?, 1, ?)""",
                (row_id, dashboard_uid, f"reviewer-{index}", float(index)),
            )

    original_reconcile = FeedbackStore._reconcile_default_tenant_owner_batched

    def reconcile_one_boundary_batch_then_stop(store: FeedbackStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, operation, copied = store._reconcile_default_tenant_owner_batch(
                conn,
                batch_size=3,
            )
        assert complete is False
        assert operation == "dashboard_provenance:retarget"
        assert copied == 3
        raise RuntimeError("simulated owner-boundary interruption")

    monkeypatch.setattr(
        FeedbackStore,
        "_reconcile_default_tenant_owner_batched",
        reconcile_one_boundary_batch_then_stop,
    )
    with pytest.raises(RuntimeError, match="simulated owner-boundary interruption"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        migrated_ids = tuple(row[0] for row in conn.execute("""SELECT id FROM dashboard_provenance
                   WHERE tenant_id='tenant-a' ORDER BY id"""))
        cursor = conn.execute("""SELECT value FROM feedback_tenant_migration_metadata
               WHERE key='default_owner_cursor_v1:dashboard_provenance'""").fetchone()
        pending_cursor = conn.execute("""SELECT value FROM feedback_tenant_migration_metadata
               WHERE key='default_owner_cursor_v1:feedback'""").fetchone()
    assert migrated_ids == boundary_ids[:3]
    assert cursor == (f"id:{boundary_ids[2]}",)
    assert pending_cursor == ("not_started",)

    monkeypatch.setattr(
        FeedbackStore,
        "_reconcile_default_tenant_owner_batched",
        original_reconcile,
    )
    resumed = FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with resumed._conn() as conn:
        provenance_ids = tuple(
            row[0] for row in conn.execute("SELECT id FROM dashboard_provenance WHERE tenant_id='tenant-a' ORDER BY id")
        )
        feedback_ids = tuple(
            row[0] for row in conn.execute("SELECT id FROM feedback WHERE tenant_id='tenant-a' ORDER BY id")
        )
    assert provenance_ids == boundary_ids
    assert feedback_ids == boundary_ids


def test_feedback_store_rejects_a_configured_owner_change(tmp_path):
    db_path = tmp_path / "feedback-owner-change.db"
    FeedbackStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    with pytest.raises(RuntimeError, match="pinned_owner_mismatch"):
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
        )


def test_feedback_owner_mismatch_is_read_only_and_redacts_diagnostics(tmp_path):
    record_identifier = "record-id-canary"
    db_path = tmp_path / f"{record_identifier}-feedback-owner-preflight.db"
    recorded_owner = "tenant-recorded-canary"
    configured_owner = "tenant-configured-canary"
    with capture_logs() as startup_logs:
        FeedbackStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=recorded_owner),
        )
    startup_text = repr(startup_logs)
    assert recorded_owner not in startup_text
    assert record_identifier not in startup_text
    initialized = [log for log in startup_logs if log.get("event") == "feedback_store_init"]
    assert len(initialized) == 1
    assert len(str(initialized[0]["database_path_fingerprint"])) == 16
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP INDEX idx_feedback_tenant_uid")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        assert str(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).casefold() == "delete"

    before = _feedback_database_state(db_path)
    attempts: list[tuple[str, list[dict[str, object]]]] = []
    for _ in range(2):
        with capture_logs() as logs, pytest.raises(RuntimeError, match="pinned_owner_mismatch") as exc_info:
            FeedbackStore(
                db_path,
                runtime_settings=Settings(_env_file=None, knowledge_tenant_id=configured_owner),
            )
        attempts.append((str(exc_info.value), list(logs)))
        assert _feedback_database_state(db_path) == before

    for message, logs in attempts:
        diagnostic_text = message + repr(logs)
        assert recorded_owner not in diagnostic_text
        assert configured_owner not in diagnostic_text
        rejected = [log for log in logs if log.get("event") == "feedback_owner_preflight_rejected"]
        assert len(rejected) == 1
        assert rejected[0]["reason_code"] == "pinned_owner_mismatch"
        assert rejected[0]["recorded_owner_class"] == "pinned"
        assert rejected[0]["configured_owner_class"] == "pinned"
        assert len(str(rejected[0]["recorded_owner_fingerprint"])) == 16
        assert len(str(rejected[0]["configured_owner_fingerprint"])) == 16
    assert attempts[0][1][0]["recorded_owner_fingerprint"] == attempts[1][1][0]["recorded_owner_fingerprint"]
    assert attempts[0][1][0]["configured_owner_fingerprint"] == attempts[1][1][0]["configured_owner_fingerprint"]


def test_feedback_owner_migration_pages_use_tenant_id_indexes(tmp_path):
    store = FeedbackStore(tmp_path / "feedback-owner-query-plan.db")

    with store._conn() as conn:
        provenance_first_page_plan = " ".join(
            str(row["detail"])
            for row in conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT id FROM dashboard_provenance
                   WHERE tenant_id='default' ORDER BY id LIMIT ?""",
                (500,),
            )
        )
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
        feedback_first_page_plan = " ".join(
            str(row["detail"])
            for row in conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT id FROM feedback
                   WHERE tenant_id='default' ORDER BY id LIMIT ?""",
                (500,),
            )
        )

    assert "idx_provenance_tenant_id" in provenance_first_page_plan
    assert "idx_provenance_tenant_id" in provenance_plan
    assert "idx_feedback_tenant_id" in feedback_first_page_plan
    assert "idx_feedback_tenant_id" in feedback_plan
    assert "TEMP B-TREE" not in provenance_first_page_plan
    assert "TEMP B-TREE" not in provenance_plan
    assert "TEMP B-TREE" not in feedback_first_page_plan
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
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
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
