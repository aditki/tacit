from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any

import pytest

import tacit.config as config_mod
import tacit.feedback as feedback_mod
import tacit.history as history_mod
import tacit.signals.store as signals_store_mod
from tacit.alert_ingest import ingest_alert_features
from tacit.backends.base import AlertFeatures
from tacit.config import Settings, create_settings, validate_distinct_sqlite_role_paths
from tacit.errors import RuntimeOwnershipError
from tacit.feedback import FeedbackStore
from tacit.history import InvestigationStore
from tacit.knowledge.enums import SourceFamily
from tacit.knowledge.repository import KnowledgeRepository
from tacit.signals import SignalStore


def _isolated_settings(**updates: object) -> Settings:
    values: dict[str, Any] = {"_env_file": None, **updates}
    return Settings(**values)


def test_settings_reject_unauthenticated_wildcard_tenancy_before_storage_creation(
    tmp_path,
):
    database_path = tmp_path / "must-not-exist" / "history.db"

    with pytest.raises(ValueError, match="requires API authentication"):
        _isolated_settings(
            knowledge_tenant_id="*",
            api_auth_enabled=False,
            history_db_path=str(database_path),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        )

    assert not database_path.parent.exists()


async def test_alert_dry_run_enforces_runtime_tenant_boundary(tmp_path):
    runtime_settings = Settings(knowledge_tenant_id="tenant-a")
    store = SignalStore(db_path=tmp_path / "signals.db", runtime_settings=runtime_settings)
    features = AlertFeatures(
        alert_uid="cross-tenant-alert",
        alert_title="Cross tenant alert",
        backend_name="grafana",
        query_language="promql",
        metrics_found=["checkout_requests_total"],
    )

    with pytest.raises(ValueError, match="Tenant access denied"):
        await ingest_alert_features(
            features,
            dry_run=True,
            runtime_settings=runtime_settings,
            store=store,
            tenant_id="tenant-b",
        )


def _clear_store_path_environment(monkeypatch) -> None:
    for name in ("HISTORY_DB_PATH", "FEEDBACK_DB_PATH", "SIGNALS_DB_PATH"):
        monkeypatch.delenv(name, raising=False)


def test_sqlite_store_paths_load_from_environment_and_drive_stores(tmp_path, monkeypatch):
    paths = {
        "HISTORY_DB_PATH": tmp_path / "state" / "history.db",
        "FEEDBACK_DB_PATH": tmp_path / "state" / "feedback.db",
        "SIGNALS_DB_PATH": tmp_path / "state" / "signals.db",
    }
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TACIT_CONFIG", raising=False)
    for name, path in paths.items():
        monkeypatch.setenv(name, str(path))

    runtime_settings = create_settings()

    assert runtime_settings.history_db_path == str(paths["HISTORY_DB_PATH"])
    assert runtime_settings.feedback_db_path == str(paths["FEEDBACK_DB_PATH"])
    assert runtime_settings.signals_db_path == str(paths["SIGNALS_DB_PATH"])

    monkeypatch.setattr(history_mod, "settings", runtime_settings)
    monkeypatch.setattr(feedback_mod, "settings", runtime_settings)
    monkeypatch.setattr(signals_store_mod, "settings", runtime_settings)

    assert InvestigationStore()._db_path == paths["HISTORY_DB_PATH"]
    assert FeedbackStore()._db_path == paths["FEEDBACK_DB_PATH"]
    assert SignalStore()._db_path == paths["SIGNALS_DB_PATH"]


def test_sqlite_store_paths_load_from_dotenv(tmp_path, monkeypatch):
    expected = {
        "history_db_path": tmp_path / "dotenv-history.db",
        "feedback_db_path": tmp_path / "dotenv-feedback.db",
        "signals_db_path": tmp_path / "dotenv-signals.db",
    }
    (tmp_path / ".env").write_text(
        "\n".join(f"{name.upper()}={path}" for name, path in expected.items()),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TACIT_CONFIG", raising=False)
    _clear_store_path_environment(monkeypatch)

    runtime_settings = create_settings()

    for field, path in expected.items():
        assert Path(getattr(runtime_settings, field)) == path


def test_sqlite_store_paths_load_from_yaml(tmp_path, monkeypatch):
    config_path = tmp_path / "tacit.yaml"
    config_path.write_text(
        """
history:
  db_path: state/yaml-history.db
feedback:
  db_path: state/yaml-feedback.db
signals:
  db_path: state/yaml-signals.db
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("TACIT_CONFIG", str(config_path))
    _clear_store_path_environment(monkeypatch)

    runtime_settings = create_settings()

    assert runtime_settings.history_db_path == "state/yaml-history.db"
    assert runtime_settings.feedback_db_path == "state/yaml-feedback.db"
    assert runtime_settings.signals_db_path == "state/yaml-signals.db"


@pytest.mark.parametrize(
    ("relative_role", "absolute_role"),
    [
        ("history_db_path", "feedback_db_path"),
        ("feedback_db_path", "history_db_path"),
        ("history_db_path", "signals_db_path"),
        ("signals_db_path", "history_db_path"),
        ("feedback_db_path", "signals_db_path"),
        ("signals_db_path", "feedback_db_path"),
    ],
)
def test_sqlite_store_roles_reject_canonical_path_collisions_without_creation(
    tmp_path,
    monkeypatch,
    relative_role,
    absolute_role,
):
    shared_path = tmp_path / "must-not-exist" / "shared.db"
    monkeypatch.chdir(tmp_path)

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        _isolated_settings(
            **{
                relative_role: str(Path("must-not-exist") / "shared.db"),
                absolute_role: str(shared_path),
            },
        )

    assert not shared_path.parent.exists()


@pytest.mark.parametrize(
    ("history_path", "signals_path"),
    [
        ("", "data/tacit_history.db"),
        ("data/tacit_signals.db", ""),
    ],
)
def test_sqlite_store_paths_include_defaults_when_detecting_collisions(history_path, signals_path):
    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        _isolated_settings(
            history_db_path=history_path,
            signals_db_path=signals_path,
        )


def test_sqlite_store_roles_reject_hard_linked_files(tmp_path):
    history_path = tmp_path / "history.db"
    feedback_path = tmp_path / "feedback.db"
    signals_path = tmp_path / "signals.db"
    history_path.touch()
    feedback_path.hardlink_to(history_path)

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        _isolated_settings(
            history_db_path=str(history_path),
            feedback_db_path=str(feedback_path),
            signals_db_path=str(signals_path),
        )

    assert history_path.stat().st_ino == feedback_path.stat().st_ino
    assert signals_path.exists() is False


def test_sqlite_store_roles_reject_special_files_without_blocking(tmp_path):
    history_path = tmp_path / "history.pipe"
    os.mkfifo(history_path)

    with pytest.raises(ValueError, match="regular file"):
        _isolated_settings(
            history_db_path=str(history_path),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        )


def _track_absolute_target_opens(monkeypatch, target: Path) -> list[Path]:
    original_open = os.open
    opened_targets: list[Path] = []

    def tracked_open(path, *args, **kwargs):
        if kwargs.get("dir_fd") is None:
            candidate = Path(os.fsdecode(path))
            if candidate == target:
                opened_targets.append(candidate)
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(config_mod.os, "open", tracked_open)
    return opened_targets


def test_sqlite_role_preflight_rejects_final_symlink_without_opening_target(
    tmp_path,
    monkeypatch,
):
    target = tmp_path / "target.db"
    target.write_bytes(b"do-not-open")
    configured = tmp_path / "history.db"
    configured.symlink_to(target)
    opened_targets = _track_absolute_target_opens(monkeypatch, target)

    with pytest.raises(ValueError, match="symbolic link"):
        _isolated_settings(
            history_db_path=str(configured),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        )

    assert opened_targets == []
    assert target.read_bytes() == b"do-not-open"


def test_sqlite_role_preflight_rejects_component_symlink_without_opening_target(
    tmp_path,
    monkeypatch,
):
    target_directory = tmp_path / "target"
    target_directory.mkdir()
    target = target_directory / "history.db"
    target.write_bytes(b"do-not-open")
    configured_directory = tmp_path / "configured"
    configured_directory.symlink_to(target_directory, target_is_directory=True)
    opened_targets = _track_absolute_target_opens(monkeypatch, target)

    with pytest.raises(ValueError, match="symbolic link"):
        _isolated_settings(
            history_db_path=str(configured_directory / "history.db"),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        )

    assert opened_targets == []
    assert target.read_bytes() == b"do-not-open"


def test_sqlite_role_preflight_rejects_fifo_without_opening_target(
    tmp_path,
    monkeypatch,
):
    configured = tmp_path / "history.pipe"
    os.mkfifo(configured)
    opened_targets = _track_absolute_target_opens(monkeypatch, configured)

    with pytest.raises(ValueError, match="regular file"):
        _isolated_settings(
            history_db_path=str(configured),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        )

    assert opened_targets == []


def test_sqlite_role_preflight_accepts_ordinary_distinct_paths_without_creation(
    tmp_path,
):
    configured = {
        "history": tmp_path / "missing" / "history.db",
        "feedback": tmp_path / "missing" / "feedback.db",
        "signals": tmp_path / "missing" / "signals.db",
    }

    canonical = validate_distinct_sqlite_role_paths(configured)

    assert canonical == configured
    assert not (tmp_path / "missing").exists()


def test_sqlite_role_preflight_rejects_normalized_lexical_collision_without_creation(
    tmp_path,
):
    database_path = tmp_path / "missing" / "shared.db"

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        validate_distinct_sqlite_role_paths(
            {
                "history": database_path,
                "feedback": tmp_path / "missing" / "nested" / ".." / "shared.db",
                "signals": tmp_path / "missing" / "signals.db",
            }
        )

    assert not database_path.parent.exists()


@pytest.mark.parametrize(
    "store_factory",
    (InvestigationStore, FeedbackStore, SignalStore, KnowledgeRepository),
)
def test_direct_sqlite_stores_reject_fifo_without_blocking(tmp_path, store_factory):
    database_path = tmp_path / f"{store_factory.__name__}.pipe"
    os.mkfifo(database_path)

    with pytest.raises(RuntimeOwnershipError, match="regular file"):
        store_factory(database_path)


@pytest.mark.parametrize(
    "store_factory",
    (InvestigationStore, FeedbackStore, SignalStore, KnowledgeRepository),
)
def test_direct_sqlite_stores_reject_directory_as_database(tmp_path, store_factory):
    database_path = tmp_path / store_factory.__name__
    database_path.mkdir()

    with pytest.raises(RuntimeOwnershipError, match="regular file"):
        store_factory(database_path)


@pytest.mark.parametrize(
    ("store_factory", "schema_table"),
    [
        (InvestigationStore, "investigations"),
        (FeedbackStore, "dashboard_provenance"),
        (SignalStore, "signal_types"),
        (KnowledgeRepository, "knowledge_candidates"),
    ],
)
def test_secure_sqlite_first_creation_and_existing_reopen(
    tmp_path,
    store_factory,
    schema_table,
):
    database_path = tmp_path / "new" / f"{schema_table}.db"

    first = store_factory(database_path)
    second = store_factory(database_path)

    assert first.database_path == database_path
    assert second.database_path == database_path
    with sqlite3.connect(database_path) as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (schema_table,),
            ).fetchone()
            is not None
        )


def test_sqlite_connection_treats_uri_metacharacters_as_literal_path(tmp_path):
    database_path = tmp_path / "history?tenant=acme#current.db"

    InvestigationStore(database_path)

    assert database_path.is_file()
    assert not (tmp_path / "history").exists()


def test_sqlite_store_roles_reject_case_aliases_when_filesystem_exposes_them(tmp_path):
    history_path = tmp_path / "History.db"
    case_alias = tmp_path / "history.db"
    history_path.touch()
    if not case_alias.exists():
        pytest.skip("filesystem is case-sensitive")

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        _isolated_settings(
            history_db_path=str(history_path),
            feedback_db_path=str(case_alias),
            signals_db_path=str(tmp_path / "signals.db"),
        )


def test_generated_archetype_aggregate_retrieval_limits_have_bounded_defaults():
    runtime_settings = _isolated_settings()

    assert runtime_settings.learned_archetypes_retrieval_max_total_artifacts == 256
    assert runtime_settings.learned_archetypes_retrieval_max_total_panels == 1_024
    assert runtime_settings.learned_archetypes_retrieval_max_total_queries == 4_096
    assert runtime_settings.learned_archetypes_retrieval_max_results == 256


@pytest.mark.parametrize(
    ("field_name", "too_large"),
    [
        ("learned_archetypes_retrieval_max_total_artifacts", 4_097),
        ("learned_archetypes_retrieval_max_total_panels", 16_385),
        ("learned_archetypes_retrieval_max_total_queries", 65_537),
        ("learned_archetypes_retrieval_max_results", 4_097),
    ],
)
def test_generated_archetype_aggregate_retrieval_limits_are_positive_and_bounded(
    field_name,
    too_large,
):
    with pytest.raises(ValueError):
        _isolated_settings(**{field_name: 0})
    with pytest.raises(ValueError):
        _isolated_settings(**{field_name: too_large})


def test_signal_store_sets_busy_timeout(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")

    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000


def test_feedback_store_sets_busy_timeout(tmp_path):
    store = FeedbackStore(db_path=tmp_path / "feedback.db")

    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000


def test_history_store_sets_busy_timeout(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")

    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000


def test_history_store_persists_reason_coded_stage_outcomes(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")
    investigation_id = store.start("Investigate latency")

    store.record_stage(
        investigation_id,
        "binding",
        status="failed",
        reason_code="compiled_metrics_absent_from_catalog",
        details={"missing_metrics": ["http_requests_total"]},
    )
    store.finish(investigation_id, status="failed")

    record = store.get(investigation_id)
    assert record is not None
    assert record["stage_outcomes"]["binding"]["reason_code"] == "compiled_metrics_absent_from_catalog"
    assert record["stage_outcomes"]["ranking"]["reason_code"] == "culprit_ranking_not_implemented"


def test_signal_store_persists_ingested_alert_context(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")

    store.record_ingested_alert(
        "checkout-latency",
        backend_name="grafana",
        source_vendor="grafana",
        source_instance="prod",
        external_id="checkout-latency",
        fingerprint="abc123",
        alert_title="Checkout latency high",
        alert_tags=["service:checkout"],
        condition="A > 1",
        severity="critical",
        labels={"service": "checkout"},
        metrics_found=["checkout_request_duration_seconds"],
        query_transformations=['histogram_quantile(0.95, checkout_request_duration_seconds{service="checkout"})'],
        service_hints=["checkout"],
        source_url="http://grafana.example/alerting/grafana/checkout-latency/view",
        provenance_url="http://grafana.example/alerting/grafana/checkout-latency/view",
        confidence=0.9,
        signals_inferred=[
            {
                "signal_type": "request_latency",
                "metric": "checkout_request_duration_seconds",
                "source": "heuristic",
                "confidence": 0.9,
            }
        ],
    )

    alerts = store.list_ingested_alerts()

    assert len(alerts) == 1
    assert alerts[0]["alert_uid"] == "checkout-latency"
    assert alerts[0]["backend_name"] == "grafana"
    assert alerts[0]["source_vendor"] == "grafana"
    assert alerts[0]["source_instance"] == "prod"
    assert alerts[0]["external_id"] == "checkout-latency"
    assert alerts[0]["fingerprint"] == "abc123"
    assert alerts[0]["provenance_url"].endswith("/checkout-latency/view")
    assert alerts[0]["confidence"] == 0.9
    assert alerts[0]["first_seen_at"] > 0
    assert alerts[0]["last_seen_at"] > 0
    assert alerts[0]["updated_at"] > 0
    assert alerts[0]["enabled"] is True
    assert alerts[0]["labels"] == {"service": "checkout"}
    assert alerts[0]["metrics_found"] == ["checkout_request_duration_seconds"]


async def test_alert_ingestion_is_idempotent_and_tracks_content_changes(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)

    first = await ingest_alert_features(
        AlertFeatures(
            alert_uid="checkout-latency",
            alert_title="Checkout latency high",
            alert_tags=["service:checkout"],
            backend_name="grafana",
            query_language="promql",
            condition="A > 1",
            labels={"service": "checkout"},
            metrics_found=["checkout_request_duration_seconds"],
            query_transformations=['histogram_quantile(0.95, checkout_request_duration_seconds{service="checkout"})'],
        )
    )
    first_row = store.list_ingested_alerts()[0]

    time.sleep(0.001)
    second = await ingest_alert_features(
        AlertFeatures(
            alert_uid="checkout-latency",
            alert_title="Checkout latency high",
            alert_tags=["service:checkout"],
            backend_name="grafana",
            query_language="promql",
            condition="A > 1",
            labels={"service": "checkout"},
            metrics_found=["checkout_request_duration_seconds"],
            query_transformations=['histogram_quantile(0.95, checkout_request_duration_seconds{service="checkout"})'],
        )
    )
    second_row = store.list_ingested_alerts()[0]

    time.sleep(0.001)
    changed = await ingest_alert_features(
        AlertFeatures(
            alert_uid="checkout-latency",
            alert_title="Checkout latency high",
            alert_tags=["service:checkout"],
            backend_name="grafana",
            query_language="promql",
            condition="A > 2",
            labels={"service": "checkout"},
            metrics_found=["checkout_request_duration_seconds"],
            query_transformations=['histogram_quantile(0.99, checkout_request_duration_seconds{service="checkout"})'],
        )
    )
    changed_row = store.list_ingested_alerts()[0]

    assert len(store.list_ingested_alerts()) == 1
    assert first["fingerprint"] == second["fingerprint"]
    assert first["fingerprint"] != changed["fingerprint"]
    assert first_row["first_seen_at"] == second_row["first_seen_at"] == changed_row["first_seen_at"]
    assert second_row["last_seen_at"] >= first_row["last_seen_at"]
    assert second_row["updated_at"] == first_row["updated_at"]
    assert changed_row["last_seen_at"] >= second_row["last_seen_at"]
    assert changed_row["updated_at"] > second_row["updated_at"]
    assert second["change_state"] == "skipped"
    assert changed["change_state"] == "updated"


async def test_alert_fingerprint_ignores_unordered_tag_metadata(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)

    first = await ingest_alert_features(
        AlertFeatures(
            alert_uid="checkout-latency",
            alert_title="Checkout latency high",
            alert_tags=["severity:critical", "service:checkout"],
            backend_name="grafana",
            query_language="promql",
            condition="A > 1",
            labels={"service": "checkout", "severity": "critical"},
            metrics_found=["checkout_request_duration_seconds", "checkout_request_errors_total"],
            query_transformations=[
                'checkout_request_duration_seconds{service="checkout"}',
                'checkout_request_errors_total{service="checkout"}',
            ],
            service_hints=["checkout", "payments"],
        )
    )
    first_row = store.get_ingested_alert("checkout-latency", "grafana")
    assert first_row is not None

    second = await ingest_alert_features(
        AlertFeatures(
            alert_uid="checkout-latency",
            alert_title="Checkout latency high",
            alert_tags=["service:checkout", "severity:critical"],
            backend_name="grafana",
            query_language="promql",
            condition="A > 1",
            labels={"severity": "critical", "service": "checkout"},
            metrics_found=["checkout_request_errors_total", "checkout_request_duration_seconds"],
            query_transformations=[
                'checkout_request_errors_total{service="checkout"}',
                'checkout_request_duration_seconds{service="checkout"}',
            ],
            service_hints=["payments", "checkout"],
        )
    )
    second_row = store.get_ingested_alert("checkout-latency", "grafana")
    assert second_row is not None

    assert first["fingerprint"] == second["fingerprint"]
    assert second["change_state"] == "skipped"
    assert second_row["updated_at"] == first_row["updated_at"]


async def test_unchanged_alert_recrawl_preserves_approved_status(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    features = AlertFeatures(
        alert_uid="checkout-latency",
        alert_title="Checkout latency high",
        alert_tags=["service:checkout"],
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        labels={"service": "checkout"},
        metrics_found=["checkout_request_duration_seconds"],
        query_transformations=['histogram_quantile(0.95, checkout_request_duration_seconds{service="checkout"})'],
    )

    await ingest_alert_features(features, auto_approve=True)
    result = await ingest_alert_features(features, auto_approve=False)
    row = store.get_ingested_alert("checkout-latency", "grafana")

    assert result["change_state"] == "skipped"
    assert result["status"] == "approved"
    assert row is not None
    assert row["status"] == "approved"
    governed = KnowledgeRepository(store._db_path).list_candidates("default", kind="signal_mapping")
    assert len(governed) == 1
    assert governed[0].payload_ref.startswith("signal_mapping:grafana:alert:checkout-latency")
    assert governed[0].evidence.items[0].source_family == SourceFamily.ALERT
    if store._learning_index_available():
        rows = store.search_learning_context("checkout latency", service="checkout")
        assert rows
        # Source approval is preserved, but a single alert does not bypass the
        # governed mapping promotion threshold.
        assert rows[0]["review_state"] == "candidate"


async def test_unchanged_pending_alert_can_upgrade_to_approved(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    features = AlertFeatures(
        alert_uid="checkout-latency",
        alert_title="Checkout latency high",
        alert_tags=["service:checkout"],
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        labels={"service": "checkout"},
        metrics_found=["checkout_request_duration_seconds"],
        query_transformations=['histogram_quantile(0.95, checkout_request_duration_seconds{service="checkout"})'],
    )

    await ingest_alert_features(features, auto_approve=False)
    result = await ingest_alert_features(features, auto_approve=True)
    row = store.get_ingested_alert("checkout-latency", "grafana")

    assert result["change_state"] == "skipped"
    assert result["status"] == "approved"
    assert row is not None
    assert row["status"] == "approved"
    if store._learning_index_available():
        rows = store.search_learning_context("checkout latency", service="checkout")
        assert rows
        assert rows[0]["review_state"] == "candidate"


async def test_alert_refresh_retires_removed_signal_knowledge(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    batches = iter(
        [
            [
                {
                    "signal_type": "old_alert_signal",
                    "metric": "old_alert_metric",
                    "confidence": 0.9,
                    "source": "heuristic",
                    "signal_family": "latency",
                    "auto_teach_eligible": True,
                }
            ],
            [
                {
                    "signal_type": "new_alert_signal",
                    "metric": "new_alert_metric",
                    "confidence": 0.9,
                    "source": "heuristic",
                    "signal_family": "latency",
                    "auto_teach_eligible": True,
                }
            ],
        ]
    )
    monkeypatch.setattr("tacit.alert_ingest.infer_signals_from_metrics", lambda *args, **kwargs: next(batches))

    def features(metric: str) -> AlertFeatures:
        return AlertFeatures(
            alert_uid="refresh-alert",
            alert_title="Refresh alert",
            backend_name="grafana",
            query_language="promql",
            condition="A > 1",
            metrics_found=[metric],
            query_transformations=[metric],
        )

    await ingest_alert_features(features("old_alert_metric"), auto_approve=True)
    await ingest_alert_features(features("new_alert_metric"), auto_approve=True)

    assert store.get_mappings_for_signal("old_alert_signal", include_decayed=True) == []
    candidates = KnowledgeRepository(store._db_path).list_candidates("default", kind="signal_mapping")
    old = next(candidate for candidate in candidates if "old_alert_metric" in candidate.payload_ref)
    new = next(candidate for candidate in candidates if "new_alert_metric" in candidate.payload_ref)
    assert old.state.lifecycle_status.value == "stale"
    assert new.state.lifecycle_status.value == "active"


def test_missing_alerts_are_marked_stale_not_deleted(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_alert(
        "checkout-latency",
        backend_name="grafana",
        alert_title="Checkout latency high",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )

    stale_count = store.mark_missing_alerts_stale(
        backend_name="grafana",
        seen_alert_uids=set(),
        authority_reconciler=lambda _conn, _source: None,
    )
    alerts = store.list_ingested_alerts()

    assert stale_count == 1
    assert len(alerts) == 1
    assert alerts[0]["alert_uid"] == "checkout-latency"
    assert alerts[0]["stale"] is True
    assert alerts[0]["status"] == "stale"
    assert alerts[0]["missing_since"] is not None

    store.record_ingested_alert(
        "checkout-latency",
        backend_name="grafana",
        alert_title="Checkout latency high",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )
    refreshed = store.list_ingested_alerts()[0]

    assert refreshed["stale"] is False
    assert refreshed["missing_since"] is None
    assert refreshed["status"] == "pending"


def test_missing_alerts_are_marked_stale_when_fts_unavailable(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_alert(
        "checkout-latency",
        backend_name="grafana",
        alert_title="Checkout latency high",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )
    monkeypatch.setattr(store, "_learning_index_available", lambda: False)

    stale_count = store.mark_missing_alerts_stale(
        backend_name="grafana",
        seen_alert_uids=set(),
        authority_reconciler=lambda _conn, _source: None,
    )
    row = store.get_ingested_alert("checkout-latency", "grafana")

    assert stale_count == 1
    assert row is not None
    assert row["stale"] is True


def test_stale_alert_context_is_removed_from_active_search(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        return
    store.record_ingested_alert(
        "checkout-latency",
        backend_name="grafana",
        alert_title="Checkout latency high",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )
    store.index_alert_context(
        alert_uid="checkout-latency",
        backend_name="grafana",
        alert_title="Checkout latency high",
        alert_tags=["service:checkout"],
        condition="A > 1",
        metrics_found=["checkout_request_duration_seconds"],
        query_transformations=['checkout_request_duration_seconds{service="checkout"}'],
        service_hints=["checkout"],
        signals_inferred=[
            {"metric": "checkout_request_duration_seconds", "signal_type": "request_latency", "confidence": 0.8}
        ],
    )

    assert store.search_learning_context("checkout latency", service="checkout")

    store.mark_missing_alerts_stale(
        backend_name="grafana",
        seen_alert_uids=set(),
        authority_reconciler=lambda _conn, _source: None,
    )

    assert store.search_learning_context("checkout latency", service="checkout") == []


def test_alert_context_namespace_does_not_collide_with_dashboard_uid(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    if not store._learning_index_available():
        return
    store.index_alert_context(
        alert_uid="shared-id",
        backend_name="grafana",
        alert_title="Checkout latency alert",
        alert_tags=["service:checkout"],
        condition="A > 1",
        metrics_found=["checkout_request_duration_seconds"],
        query_transformations=['checkout_request_duration_seconds{service="checkout"}'],
        service_hints=["checkout"],
        signals_inferred=[
            {"metric": "checkout_request_duration_seconds", "signal_type": "request_latency", "confidence": 0.8}
        ],
    )
    store.index_dashboard_context(
        dashboard_uid="shared-id",
        backend_name="grafana",
        dashboard_title="Checkout dashboard",
        dashboard_tags=["service:checkout"],
        panels=[
            {
                "title": "Checkout traffic",
                "queries": ['rate(checkout_requests_total{service="checkout"}[5m])'],
                "metrics": ["checkout_requests_total"],
            }
        ],
        metrics_found=["checkout_requests_total"],
        signals_inferred=[
            {"metric": "checkout_requests_total", "signal_type": "request_throughput", "confidence": 0.8}
        ],
    )

    rows = store.search_learning_context("checkout", service="checkout")
    source_kinds = {row["source_kind"] for row in rows}

    assert "alert_rule" in source_kinds
    assert "dashboard_panel" in source_kinds
