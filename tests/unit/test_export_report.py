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
        assert metadata["collection"]["investigations"]["row_limit"] == 10000
        assert metadata["collection"]["ingested_dashboards"]["truncated"] is True
        assert metadata["collection"]["ingested_dashboards"]["source_total"] == 10001
        for member in tar.getmembers():
            assert member.uid == 0
            assert member.gid == 0
            assert member.uname == ""
            assert member.gname == ""
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
        metadata = json.loads(tar.extractfile("metadata.json").read().decode())
        assert metadata["hostnames_included"] is True
        assert metadata["emails_included"] is True
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


def test_anonymizer_sanitizes_known_and_unknown_dict_keys():
    report = {
        "assessment_summary": {
            "investigations": {"total": 1, "succeeded": 1, "failed": 0, "timed_out": 0},
            "feedback": {"total_feedback": 1, "useful_rate": 1.0},
        },
        "knowledge_coverage": {
            "backend_distribution": {"prod.internal.example.com": 1},
        },
        "custom_sensitive_section": {
            "prod-us-east-1-payments-vip": {
                "owner": "user@example.com",
            }
        },
    }

    anonymized = ReportAnonymizer().anonymize_report(report)
    text = json.dumps(anonymized)

    assert "prod.internal.example.com" not in text
    assert "prod-us-east-1-payments-vip" not in text
    assert "custom_sensitive_section" not in text
    assert "user@example.com" not in text
    assert anonymized["assessment_summary"]["investigations"] == {
        "failed": 0,
        "succeeded": 1,
        "timed_out": 0,
        "total": 1,
    }
    assert anonymized["assessment_summary"]["feedback"] == {"total_feedback": 1, "useful_rate": 1.0}
    assert anonymized["knowledge_coverage"]["backend_distribution"] == {"backend_001": 1}


def test_anonymizer_preserves_packaged_taxonomy_and_diagnostics():
    report = {
        "artifact_stats": {
            "dashboards": {
                "metrics_found": {"count": 2, "min": 1, "max": 3, "avg": 2},
                "signals_inferred": {"count": 2, "min": 1, "max": 3, "avg": 2},
            }
        },
        "knowledge_coverage": {
            "signals_by_category": {
                "auth": 1,
                "caching": 1,
                "network": 1,
                "storage": 1,
                "serverless": 1,
                "traffic_management": 1,
                "prod-us-east-1-payments-vip": 1,
            }
        },
        "ranking_summary": {
            "all_archetype_counts": {
                "kubernetes_investigation": 1,
                "rate_limiting_investigation": 1,
                "redis_saturation": 1,
                "kafka_broker_health": 1,
            },
            "datasource_type_counts": {"prometheus": 1},
            "metrics_catalog_size": {"count": 1, "min": 1, "max": 1, "avg": 1},
            "metrics_selected_count": {"count": 1, "min": 1, "max": 1, "avg": 1},
            "panels_dropped": {"count": 1, "min": 0, "max": 0, "avg": 0},
        },
        "robustness_summary": {
            "reason_code_counts": {
                "named_metrics_discovered": 1,
                "all_compiled_metrics_present": 1,
                "all_panels_survived": 1,
                "culprit_ranking_not_implemented": 1,
            },
            "stage_status_counts": {
                "semantic_mapping:partial": 1,
                "binding:passed": 1,
                "compilation:passed": 1,
                "symptom_evidence_rescue:skipped": 1,
                "evidence_gap_resolution:passed": 1,
            },
        },
        "assessment_summary": {
            "status_counts": {"running": 1, "partial": 1},
            "path_counts": {"failed": 1},
        },
    }

    anonymized = ReportAnonymizer().anonymize_report(report)
    text = json.dumps(anonymized)

    assert "key_" not in text
    assert "signal_category_" in text
    assert "prod-us-east-1-payments-vip" not in text
    assert "auth" in anonymized["knowledge_coverage"]["signals_by_category"]
    assert "traffic_management" in anonymized["knowledge_coverage"]["signals_by_category"]
    assert "kubernetes_investigation" in anonymized["ranking_summary"]["all_archetype_counts"]
    assert "rate_limiting_investigation" in anonymized["ranking_summary"]["all_archetype_counts"]
    assert "kafka_broker_health" in anonymized["ranking_summary"]["all_archetype_counts"]
    assert "metrics_found" in anonymized["artifact_stats"]["dashboards"]
    assert "datasource_type_counts" in anonymized["ranking_summary"]
    assert "all_panels_survived" in anonymized["robustness_summary"]["reason_code_counts"]
    assert "semantic_mapping:partial" in anonymized["robustness_summary"]["stage_status_counts"]
    assert "running" in anonymized["assessment_summary"]["status_counts"]
    assert "failed" in anonymized["assessment_summary"]["path_counts"]


def _tar_member_texts(tar: tarfile.TarFile) -> dict[str, str]:
    out: dict[str, str] = {}
    for name in tar.getnames():
        member = tar.extractfile(name)
        if member is not None:
            out[name] = member.read().decode()
    return out
