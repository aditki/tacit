"""Deterministic operational-knowledge assessment tests."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from tacit.assess import build_assessment
from tacit.config import Settings
from tacit.signals.store import SignalStore


@pytest.fixture
def store(tmp_path):
    return SignalStore(db_path=tmp_path / "signals.db")


def _history(stats: dict | None = None) -> MagicMock:
    history = MagicMock()
    history.stats.return_value = stats or {}
    return history


class TestEmptyStore:
    def test_runs_on_empty_store_without_llm(self, store):
        report = build_assessment(signal_store=store, history_store=_history())
        assert report["inventory"]["dashboards_ingested"] == 0
        assert report["readiness"]["level"] == "Low"
        assert report["readiness"]["score"] == 0

    def test_history_failure_is_tolerated(self, store):
        history = MagicMock()
        history.stats.side_effect = RuntimeError("no db")
        report = build_assessment(signal_store=store, history_store=history)
        assert report["activity"]["investigations_total"] == 0

    def test_wildcard_assessment_requires_and_propagates_a_tenant(self, tmp_path):
        runtime_settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
        store = SignalStore(
            db_path=tmp_path / "wildcard-signals.db",
            runtime_settings=runtime_settings,
        )
        history = _history()

        class Stores:
            settings = store.runtime_settings

            @staticmethod
            def signals():
                return store

            @staticmethod
            def history():
                return history

        with pytest.raises(ValueError, match="tenant"):
            build_assessment(stores=Stores())

        report = build_assessment(stores=Stores(), tenant_id="tenant-a")

        assert report["tenant_id"] == "tenant-a"
        history.stats.assert_called_once_with(tenant_id="tenant-a")

    def test_injected_store_boundaries_must_match(self, tmp_path):
        signal_store = SignalStore(
            db_path=tmp_path / "signals.db",
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )
        history = MagicMock()
        history._settings = Settings(_env_file=None, knowledge_tenant_id="tenant-b")

        with pytest.raises(ValueError, match="different tenant boundaries"):
            build_assessment(signal_store=signal_store, history_store=history)

    def test_injected_store_database_graphs_must_match(self, tmp_path):
        signal_store = SignalStore(
            db_path=tmp_path / "signals-a.db",
            runtime_settings=Settings(
                _env_file=None,
                knowledge_tenant_id="tenant-a",
                signals_db_path=str(tmp_path / "signals-a.db"),
                history_db_path=str(tmp_path / "history-a.db"),
            ),
        )
        history = MagicMock()
        history._settings = Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            signals_db_path=str(tmp_path / "signals-b.db"),
            history_db_path=str(tmp_path / "history-b.db"),
        )

        with pytest.raises(ValueError, match="different runtime compositions"):
            build_assessment(signal_store=signal_store, history_store=history)

    def test_partial_signal_store_injection_derives_history_from_same_runtime(self, monkeypatch, tmp_path):
        runtime_settings = Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            signals_db_path=str(tmp_path / "signals.db"),
            history_db_path=str(tmp_path / "history.db"),
        )
        signal_store = SignalStore(
            db_path=tmp_path / "signals.db",
            runtime_settings=runtime_settings,
        )

        def unexpected_global_history():
            raise AssertionError("assessment composed with the process-global history store")

        monkeypatch.setattr("tacit.history.get_investigation_store", unexpected_global_history)

        report = build_assessment(signal_store=signal_store)

        assert report["tenant_id"] == "tenant-a"
        assert report["activity"]["investigations_total"] == 0

    def test_partial_injection_adopts_the_store_owned_database_path(self, tmp_path):
        signal_store = SignalStore(
            db_path=tmp_path / "ad-hoc-signals.db",
            runtime_settings=Settings(
                _env_file=None,
                history_db_path=str(tmp_path / "history.db"),
            ),
        )

        report = build_assessment(signal_store=signal_store)

        assert report["tenant_id"] == "default"
        assert signal_store.runtime_settings.signals_db_path == str((tmp_path / "ad-hoc-signals.db").resolve())


class TestPopulatedStore:
    def test_counts_and_coverage(self, store):
        store.register_signal_type("request_latency", category="latency")
        store.register_signal_type("error_rate", category="errors")
        store.add_mapping("request_latency", "http_request_duration_seconds", 0.9, review_state="trusted")
        store.add_mapping("error_rate", "http_errors_total", 0.6, review_state="candidate")
        store.record_ingested_dashboard("dash-1", dashboard_title="Checkout", metrics_found=["a", "b"], panel_count=4)
        store.record_ingested_dashboard(
            "dash-2", dashboard_title="Checkout Copy", metrics_found=["a", "b"], panel_count=4
        )
        store.record_ingested_alert("alert-1", alert_title="High p99", service_hints=["checkout-service"], enabled=True)
        store.record_learned_artifact(artifact_id="rb-1", artifact_type="runbook", title="Checkout runbook")
        store.record_learned_artifact(artifact_id="inc-1", artifact_type="incident", title="Sev2")

        report = build_assessment(
            signal_store=store,
            history_store=_history({"total": 10, "succeeded": 9, "avg_panels": 6.2, "archetype_path": 8}),
        )

        inventory = report["inventory"]
        assert inventory["dashboards_ingested"] == 2
        assert inventory["alerts_ingested"] == 1
        assert inventory["runbooks"] == 1
        assert inventory["incidents"] == 1
        assert inventory["signal_types"] == 2

        coverage = report["coverage"]
        assert coverage["signal_types_with_trusted_mapping"] == 1
        assert coverage["knowledge_coverage_pct"] == 50.0

        quality = report["quality"]
        assert quality["duplicate_groups"] == 1  # dash-1 and dash-2 share a metric set
        assert quality["alerts_without_owner_attribution"] == 1  # no ownership hints exist
        assert quality["runbooks_without_matching_signals"] == 1

        activity = report["activity"]
        assert activity["investigations_total"] == 10
        assert activity["success_rate_pct"] == 90.0

    def test_readiness_increases_with_knowledge(self, store):
        empty = build_assessment(signal_store=store, history_store=_history())

        store.register_signal_type("request_latency")
        store.add_mapping("request_latency", "latency_seconds", 0.9, review_state="trusted")
        store.record_ingested_dashboard("dash-1", metrics_found=["latency_seconds"])
        populated = build_assessment(signal_store=store, history_store=_history({"total": 3, "succeeded": 3}))

        assert populated["readiness"]["score"] > empty["readiness"]["score"]

    def test_services_from_hints_and_mappings(self, store):
        store.register_signal_type("request_latency")
        store.add_mapping(
            "request_latency",
            "latency_seconds",
            0.9,
            context_services=["payment-api"],
            review_state="trusted",
        )
        store.record_ingested_alert("alert-1", service_hints=["checkout-service"])

        report = build_assessment(signal_store=store, history_store=_history())
        services = report["services"]
        assert services["known"] == 2
        assert services["missing_ownership"] == 2  # no ownership hints recorded
