from __future__ import annotations

import json
import tarfile
from pathlib import Path

import pytest

from tacit.export_report import (
    ANONYMOUS_BUNDLE_FILES,
    ReportAnonymizer,
    export_assessment_report,
    validate_report_for_leakage,
)

SENSITIVE_STRINGS = (
    "checkout-api",
    "user@example.com",
    "prod.internal.example.com",
    "prod-us-east-1-payments-vip",
    "acme_checkout_private",
    "https://internal.example.company",
)


class FakeHistoryStore:
    def stats(self):
        return {
            "total": 1,
            "succeeded": 1,
            "failed": 0,
            "timed_out": 0,
            "avg_time": 2.5,
            "avg_panels": 3,
            "avg_catalog_size": 20,
            "archetype_path": 1,
            "freeform_path": 0,
        }

    def list_recent(self, limit=50, offset=0, status=None, user_id=None):
        return [
            {
                "id": "inv-1",
                "prompt": "checkout-api failed for user@example.com",
                "status": "success",
                "path_used": "archetype",
                "problem_type": "latency_investigation",
                "archetypes": [{"type": "latency_investigation", "confidence": 0.91}],
                "datasource_types": ["prometheus"],
                "metrics_catalog_size": 20,
                "metrics_ranked_size": 8,
                "metrics_selected": ["checkout_latency_seconds"],
                "generated_queries": [{"expr": 'rate(checkout_latency_seconds{service="checkout-api"}[5m])'}],
                "panel_count": 3,
                "panels_dropped": 1,
                "validation_warnings": ['Panel "Latency" dropped - no series in window'],
                "stage_outcomes": {"ranking": {"status": "skipped", "reason_code": "not_implemented"}},
                "error": "",
            }
        ]


class FakeFeedbackStore:
    def get_aggregate_stats(self):
        return {
            "total_feedback": 1,
            "total_dashboards": 1,
            "useful_rate": 1.0,
            "avg_symptom_visibility": 5,
            "avg_root_cause_support": 4,
            "avg_noise_level": 4,
            "avg_investigation_speed": 5,
        }

    def analyze(self):
        return {
            "total_feedback": 1,
            "recommendations": ["raw recommendation mentioning https://internal.example.company"],
            "metric_quality": [{"metric": "checkout_latency_seconds", "good": 1, "bad": 0}],
        }


class FakeSignalStore:
    def stats(self):
        return {
            "signal_types": 2,
            "metric_mappings": 3,
            "ingested_dashboards": 10001,
            "ingested_alerts": 1,
            "learned_artifacts": 1,
            "mappings_by_source": {
                "dashboard_ingest": 2,
                "https://internal.example.company/source": 1,
            },
            "signals_by_category": {"latency": 1, "prod-us-east-1-payments-vip": 1},
        }

    def list_ingested_dashboards(self, status=None, limit=50):
        return [
            {
                "dashboard_uid": "checkout-dashboard",
                "backend_name": "prod.internal.example.com",
                "dashboard_title": "Checkout Latency",
                "status": "approved",
                "panel_count": 3,
                "metrics_found": ["checkout_latency_seconds"],
                "signals_inferred": [{"signal_type": "request_latency"}],
            }
        ]

    def list_ingested_alerts(self, status=None, limit=50):
        return [
            {
                "alert_uid": "checkout-alert",
                "backend_name": "prod.internal.example.com",
                "alert_title": "Checkout Alert",
                "status": "pending",
                "metrics_found": ["checkout_errors_total"],
                "signals_inferred": [{"signal_type": "error_rate"}],
            }
        ]

    def list_learned_artifacts(self, *, artifact_type=None, limit=50):
        return [
            {
                "artifact_id": "runbook-1",
                "artifact_type": "acme_checkout_private",
                "title": "Checkout Runbook",
                "stale": False,
            }
        ]


@pytest.fixture
def fake_stores(monkeypatch):
    monkeypatch.setattr("tacit.history.get_investigation_store", lambda: FakeHistoryStore())
    monkeypatch.setattr("tacit.feedback.get_feedback_store", lambda: FakeFeedbackStore())
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: FakeSignalStore())


def test_anonymous_export_writes_safe_bundle_files(tmp_path: Path, fake_stores):
    output = tmp_path / "bundle.tar.gz"

    result = export_assessment_report(output=output, anonymous=True, validate=True)

    assert result.output_path == output.resolve()
    assert result.validation_report["passed"] is True
    with tarfile.open(output, "r:gz") as tar:
        names = sorted(tar.getnames())
        assert names == sorted(ANONYMOUS_BUNDLE_FILES)
        assert "raw_local_details.json" not in names
        metadata = json.loads(tar.extractfile("metadata.json").read().decode())
        assert metadata["anonymous"] is True
        assert metadata["mapping_included"] is False
        assert metadata["raw_artifacts_included"] is False
        assert metadata["collection"]["ingested_dashboards"]["truncated"] is True
        assert metadata["collection"]["ingested_dashboards"]["source_total"] == 10001
        members = _tar_member_texts(tar)
        for text in members.values():
            for sensitive in SENSITIVE_STRINGS:
                assert sensitive not in text


def test_raw_export_includes_local_details(tmp_path: Path, fake_stores):
    output = tmp_path / "raw.tar.gz"

    result = export_assessment_report(output=output, anonymous=False)

    with tarfile.open(output, "r:gz") as tar:
        names = tar.getnames()
        assert "raw_local_details.json" in names
    assert result.validation_report["skipped"] is True


def test_leakage_validator_detects_obvious_identifiers():
    report = {
        "metadata": {"generated_at": "2026-07-02T04:15:00Z"},
        "assessment_summary": {"owner": "user@example.com", "url": "https://internal.example.company/a"},
    }

    validation = validate_report_for_leakage(report)

    assert validation["passed"] is False
    assert validation["findings_count"] >= 2


def test_leakage_validator_detects_identifiers_in_object_keys():
    report = {
        "metadata": {"generated_at": "2026-07-02T04:15:00Z"},
        "knowledge_coverage": {"backend_distribution": {"prod.internal.example.com": 1}},
    }

    validation = validate_report_for_leakage(report)

    assert validation["passed"] is False
    assert validation["findings_count"] == 1
    assert validation["findings"][0]["location"] == "key"
    assert "prod.internal.example.com" not in json.dumps(validation)


def test_anonymizer_is_deterministic_per_kind():
    anonymizer = ReportAnonymizer()

    assert anonymizer.anonymize_value("checkout-api", "service") == "service_001"
    assert anonymizer.anonymize_value("cart-api", "service") == "service_002"
    assert anonymizer.anonymize_value("checkout-api", "service") == "service_001"
    assert anonymizer.anonymize_value("checkout-api", "dashboard") == "dashboard_001"


def _tar_member_texts(tar: tarfile.TarFile) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in tar.getnames():
        member = tar.extractfile(name)
        if member is not None:
            out[name] = member.read().decode()
    return out
