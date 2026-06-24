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
