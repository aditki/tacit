from __future__ import annotations

import time

from tacit.alert_ingest import ingest_alert_features
from tacit.backends.base import AlertFeatures
from tacit.feedback import FeedbackStore
from tacit.history import InvestigationStore
from tacit.signals import SignalStore


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
    if store._learning_index_available():
        rows = store.search_learning_context("checkout latency", service="checkout")
        assert rows
        assert rows[0]["review_state"] != "candidate"


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
        assert rows[0]["review_state"] != "candidate"


def test_missing_alerts_are_marked_stale_not_deleted(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_alert(
        "checkout-latency",
        backend_name="grafana",
        alert_title="Checkout latency high",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )

    stale_count = store.mark_missing_alerts_stale(backend_name="grafana", seen_alert_uids=set())
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

    stale_count = store.mark_missing_alerts_stale(backend_name="grafana", seen_alert_uids=set())
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

    store.mark_missing_alerts_stale(backend_name="grafana", seen_alert_uids=set())

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
