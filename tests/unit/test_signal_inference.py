"""Tests for deterministic signal inference + the ingestion/approve compounding.

This is the "learns your telemetry language" path: custom metrics (e.g. Calico
``felix_*``) get mapped to signal families without anyone hand-teaching them.
"""

from __future__ import annotations

import pytest
import yaml as _yaml

from tacit.archetypes.templates import (
    append_archetype_to_yaml,
    get_archetype,
    get_archetypes_by_learning_context,
    reload_archetypes,
)
from tacit.dashboard_ingest import generate_archetype_yaml, infer_signals_from_metrics
from tacit.models.schemas import ArchetypeMatch, Intent, MetricEntry
from tacit.signal_inference import INFERENCE_VERSION, coverage, infer_signal, infer_signals
from tacit.signals import SignalStore


@pytest.fixture
def signal_store(tmp_path):
    return SignalStore(db_path=tmp_path / "inf_signals.db")


# ── L1 engine ────────────────────────────────────────────────────────────────


class TestInferenceEngine:
    def test_errors_family_without_total_suffix(self):
        sig = infer_signal("felix_iptables_save_errors")
        assert sig is not None
        assert sig.signal_family == "errors"
        assert sig.evidence  # carries explanation

    def test_latency_family_without_duration_keyword(self):
        sig = infer_signal("felix_int_dataplane_apply_time_seconds")
        assert sig is not None
        assert sig.signal_family == "latency"
        # volatile unit suffix collapsed in the emergent name
        assert sig.signal_name == "felix_int_dataplane_apply_time"

    def test_panel_context_boosts_confidence(self):
        panels = [
            {
                "title": "Dataplane apply time",
                "row": "Dataplane",
                "unit": "s",
                "metrics": ["felix_int_dataplane_apply_time_seconds"],
                "queries": ["histogram_quantile(0.99, felix_int_dataplane_apply_time_seconds)"],
            }
        ]
        bare = infer_signal("felix_int_dataplane_apply_time_seconds")
        rich = infer_signal("felix_int_dataplane_apply_time_seconds", panels)
        assert rich.confidence > bare.confidence

    def test_duplicate_panel_context_does_not_exceed_source_caps(self):
        panels = [
            {
                "title": "Memory",
                "row": "Resources",
                "unit": "bytes",
                "metrics": ["opaque_value"],
                "queries": ["opaque_value"],
            }
            for _ in range(3)
        ]

        sig = infer_signal("opaque_value", panels)

        assert sig is not None
        assert sig.signal_family == "resource_usage"
        assert sig.score < 0.70
        assert sig.auto_teach_eligible is False
        assert sig.evidence_sources == ["group", "title", "unit"]

    def test_backlog_and_saturation(self):
        assert infer_signal("workqueue_depth").signal_family == "backlog"
        assert infer_signal("controller_runtime_webhook_requests_in_flight").signal_family == "saturation"

    def test_histogram_name_collapses(self):
        # _bucket/_seconds fold so the histogram's tri-metrics map to one signal.
        assert infer_signal("http_request_duration_seconds_bucket").signal_name == "http_request_duration"

    def test_weak_metric_returns_none(self):
        assert infer_signal("felix_resyncs_started") is None

    def test_coverage(self):
        metrics = ["felix_iptables_save_errors", "felix_int_dataplane_apply_time_seconds", "felix_resyncs_started"]
        inf = infer_signals(metrics)
        assert 0.0 < coverage(metrics, inf) < 1.0


class TestInferenceHardening:
    def test_metadata_metrics_ignored(self):
        for m in ("kube_build_info", "go_info", "node_version_info", "thing_created"):
            assert infer_signal(m) is None, m

    def test_uptime_is_availability_not_latency(self):
        sig = infer_signal("node_uptime_seconds")
        assert sig is not None and sig.signal_family == "availability"

    def test_probe_status_is_availability(self):
        sig = infer_signal("httpcheck_status")
        assert sig is not None and sig.signal_family == "availability"

    def test_cert_expiry_is_security(self):
        sig = infer_signal("ssl_cert_expiry_seconds")
        assert sig is not None and sig.signal_family == "security"

    def test_cpu_seconds_counter_is_resource_not_latency(self):
        sig = infer_signal("process_cpu_seconds_total")
        assert sig is not None and sig.signal_family == "resource_usage"
        # _total drops, _seconds kept (not a latency) → meaning preserved
        assert sig.signal_name == "process_cpu_seconds"

    def test_rate_boosts_existing_family_not_traffic(self):
        panels = [{"title": "Errors", "metrics": ["app_errors_total"], "queries": ["rate(app_errors_total[5m])"]}]
        sig = infer_signal("app_errors_total", panels)
        assert sig.signal_family == "errors"
        assert any("confirms counter" in e for e in sig.evidence)

    @pytest.mark.parametrize(
        "metric",
        ["http_response_status_code_total", "http_service_response_status_code_total"],
    )
    def test_http_status_code_counter_is_traffic_not_availability(self, metric):
        sig = infer_signal(metric)

        assert sig is not None
        assert sig.signal_family == "traffic"

    def test_bucket_without_time_is_not_latency(self):
        panels = [{"title": "Response size", "unit": "bytes", "metrics": ["response_size_bytes_bucket"], "queries": []}]
        sig = infer_signal("response_size_bytes_bucket", panels)
        assert sig is not None and sig.signal_family == "resource_usage"

    def test_explicit_name_with_context_is_auto_teachable(self):
        panels = [{"title": "Iptables save errors", "metrics": ["felix_iptables_save_errors"], "queries": []}]
        rich = infer_signal("felix_iptables_save_errors", panels)
        assert rich.auto_teach_eligible is True
        assert set(rich.evidence_sources) >= {"name", "title"}

    @pytest.mark.parametrize(
        ("metric", "panel"),
        [
            (
                "checkout_edge_inflight_requests",
                {
                    "title": "In-flight requests",
                    "row": "Saturation",
                    "unit": "short",
                    "metrics": ["checkout_edge_inflight_requests"],
                    "queries": ["checkout_edge_inflight_requests"],
                },
            ),
            (
                "checkout_edge_db_pool_wait_seconds_bucket",
                {
                    "title": "DB pool wait time",
                    "row": "Downstream",
                    "unit": "s",
                    "metrics": ["checkout_edge_db_pool_wait_seconds_bucket"],
                    "queries": ["histogram_quantile(0.95, checkout_edge_db_pool_wait_seconds_bucket)"],
                },
            ),
        ],
    )
    def test_explicit_incident_signals_are_auto_teachable(self, metric, panel):
        signal = infer_signal(metric, [panel])

        assert signal is not None
        assert signal.auto_teach_eligible is True

    def test_pool_wait_metrics_do_not_teach_frontend_request_latency(self, signal_store):
        metrics = ["connection_pool_wait_seconds", "worker_pool_wait_seconds"]
        panels = [
            {
                "title": "Connection pool wait",
                "unit": "s",
                "metrics": ["connection_pool_wait_seconds"],
                "queries": ["connection_pool_wait_seconds"],
            },
            {
                "title": "Worker pool",
                "unit": "s",
                "metrics": ["worker_pool_wait_seconds"],
                "queries": ["worker_pool_wait_seconds"],
            },
        ]

        inferred = {
            signal["metric"]: signal for signal in infer_signals_from_metrics(metrics, panels, store=signal_store)
        }

        assert inferred["connection_pool_wait_seconds"]["signal_type"] == "db_connection_pool"
        assert inferred["connection_pool_wait_seconds"]["signal_family"] == "saturation"
        assert inferred["connection_pool_wait_seconds"]["auto_teach_eligible"] is True
        assert inferred["worker_pool_wait_seconds"]["signal_type"] == "worker_pool_wait"
        assert inferred["worker_pool_wait_seconds"]["auto_teach_eligible"] is False
        assert all(signal["signal_type"] != "request_latency" for signal in inferred.values())

    def test_weak_single_source_not_auto_teachable(self):
        bare = infer_signal("felix_iptables_save_errors")  # name only, score 0.40
        assert bare.signal_family == "errors"
        assert bare.auto_teach_eligible is False  # < 0.45, single source
        assert bare.margin >= 0.0

    def test_query_substring_ownership(self):
        # metric only appears inside the query (not in extracted metrics list)
        panels = [
            {
                "title": "Apply time",
                "unit": "s",
                "metrics": [],
                "queries": ["histogram_quantile(0.9, felix_int_dataplane_apply_time_seconds_bucket)"],
            }
        ]
        sig = infer_signal("felix_int_dataplane_apply_time_seconds", panels)
        assert "unit 's' → latency" in sig.evidence or "title mentions latency" in sig.evidence


# ── Ingestion integration ────────────────────────────────────────────────────


class TestIngestionInference:
    def _felix_extracted(self):
        panels = [
            {
                "title": "Iptables save errors",
                "row": "Dataplane",
                "unit": "short",
                "metrics": ["felix_iptables_save_errors"],
                "queries": ["rate(felix_iptables_save_errors[5m])"],
            },
            {
                "title": "Dataplane apply time",
                "row": "Dataplane",
                "unit": "s",
                "metrics": ["felix_int_dataplane_apply_time_seconds"],
                "queries": ["histogram_quantile(0.99, felix_int_dataplane_apply_time_seconds)"],
            },
        ]
        metrics = ["felix_iptables_save_errors", "felix_int_dataplane_apply_time_seconds"]
        return (
            {
                "dashboard_uid": "felix",
                "dashboard_title": "Felix",
                "dashboard_tags": ["calico"],
                "query_language": "promql",
                "metrics_found": metrics,
                "panel_count": len(panels),
                "panels": panels,
            },
            metrics,
            panels,
        )

    def test_custom_metrics_inferred_via_heuristic(self, signal_store):
        _, metrics, panels = self._felix_extracted()
        signals = infer_signals_from_metrics(metrics, panels, store=signal_store)
        assert signals, "felix metrics should infer signals without teaching"
        assert all(s.get("signal_family") for s in signals)
        assert any(s["source"] == "heuristic" for s in signals)

    def test_generated_archetype_has_signal_bindings(self, signal_store):
        extracted, metrics, panels = self._felix_extracted()
        signals = infer_signals_from_metrics(metrics, panels, store=signal_store)
        arch = _yaml.safe_load(generate_archetype_yaml(extracted, signals, archetype_id="felix"))["archetypes"][0]
        assert arch["signal_bindings"], "felix archetype must now carry signal bindings"
        assert arch["required_signals"]

    def test_generated_archetype_skips_weak_heuristic_signal_bindings(self):
        extracted, _, _ = self._felix_extracted()
        signals = [
            {
                "signal_type": "weak_guess",
                "metric": "felix_resyncs_started",
                "confidence": 0.25,
                "source": "heuristic",
                "auto_teach_eligible": False,
            }
        ]

        arch = _yaml.safe_load(generate_archetype_yaml(extracted, signals, archetype_id="felix"))["archetypes"][0]

        assert arch["signal_bindings"] == {}
        assert arch["required_signals"] == []


# ── Cross-vendor regression corpus (messy real-world metric names) ────────────


class TestRegressionCorpus:
    # metric → expected family, drawn from Calico, Envoy, JVM, Redis, Kafka,
    # CoreDNS, NGINX, and the AWS CloudWatch exporter.
    CORPUS = {
        # errors
        "felix_iptables_save_errors": "errors",
        "envoy_cluster_upstream_rq_timeout": "errors",
        "aws_lambda_errors_sum": "errors",
        "kafka_network_request_errors_total": "errors",
        # latency
        "felix_int_dataplane_apply_time_seconds": "latency",
        "redis_commands_duration_seconds_total": "latency",
        "coredns_dns_request_duration_seconds_bucket": "latency",
        "nginx_upstream_response_time_seconds": "latency",
        "jvm_gc_pause_seconds": "latency",
        # traffic
        "envoy_cluster_upstream_rq_total": "traffic",
        "nginx_http_requests_total": "traffic",
        "kafka_server_brokertopicmetrics_messagesin_total": "traffic",
        # resource_usage
        "jvm_memory_used_bytes": "resource_usage",
        "redis_memory_used_bytes": "resource_usage",
        "aws_rds_cpuutilization_average": "resource_usage",
        # backlog / availability
        "kafka_consumer_lag": "backlog",
        "node_uptime_seconds": "availability",
    }

    @pytest.mark.parametrize("metric,family", list(CORPUS.items()))
    def test_corpus_family(self, metric, family):
        sig = infer_signal(metric)
        assert sig is not None, f"{metric} should classify"
        assert sig.signal_family == family, f"{metric}: got {sig.signal_family}, want {family}"

    def test_corpus_coverage_is_high(self):
        metrics = list(self.CORPUS)
        assert coverage(metrics, infer_signals(metrics)) >= 0.9


# ── Provenance, review state, and rejected candidates ─────────────────────────


class TestProvenanceAndReview:
    def test_mapping_carries_version_and_state(self, signal_store):
        signal_store.add_mapping(
            "dataplane_errors",
            "felix_*_errors",
            confidence=0.8,
            source_type="dashboard_ingest",
            inference_version=INFERENCE_VERSION,
            review_state="approved",
        )
        m = signal_store.get_mappings_for_signal("dataplane_errors", include_decayed=True)[0]
        assert m["inference_version"] == INFERENCE_VERSION
        assert m["review_state"] == "approved"

    def test_reteach_does_not_downgrade_trust(self, signal_store):
        signal_store.add_mapping("s", "m", confidence=0.9, review_state="trusted")
        signal_store.add_mapping("s", "m", confidence=0.9, review_state="candidate")  # would downgrade
        m = signal_store.get_mappings_for_signal("s", include_decayed=True)[0]
        assert m["review_state"] == "trusted"

    def test_human_teach_upgrades_approved_mapping_to_trusted(self, signal_store):
        signal_store.add_mapping(
            "s",
            "m",
            confidence=0.7,
            source_type="dashboard_ingest",
            source_refs=["grafana:d1"],
            inference_version=INFERENCE_VERSION,
            review_state="approved",
        )
        signal_store.add_mapping(
            "s",
            "m",
            confidence=0.9,
            source_type="api",
            source_refs=["api:teach"],
            review_state="trusted",
        )

        m = signal_store.get_mappings_for_signal("s", include_decayed=True)[0]
        assert m["review_state"] == "trusted"
        assert m["inference_version"] == INFERENCE_VERSION
        assert m["source_refs"] == ["grafana:d1", "api:teach"]

    def test_rejected_candidate_roundtrip(self, signal_store):
        signal_store.record_rejected_candidate(
            metric="felix_resyncs_started",
            signal_family="traffic",
            signal_name="felix_resyncs",
            score=0.2,
            margin=0.0,
            why_not="low_score",
            evidence=["weak"],
            inference_version=INFERENCE_VERSION,
            dashboard_uid="felix",
            backend_name="grafana",
        )
        rows = signal_store.list_rejected_candidates()
        assert len(rows) == 1
        assert rows[0]["metric"] == "felix_resyncs_started"
        assert rows[0]["why_not"] == "low_score"
        assert rows[0]["evidence"] == ["weak"]


# ── L3 archetype auto-registration ───────────────────────────────────────────


class TestArchetypeAutoRegister:
    def test_no_path_returns_none(self, monkeypatch):
        monkeypatch.delenv("TACIT_ARCHETYPES_PATH", raising=False)
        assert append_archetype_to_yaml("archetypes:\n- id: x\n  name: X\n") is None

    def test_merge_dedupes_by_id(self, tmp_path):
        target = tmp_path / "archetypes.yaml"
        append_archetype_to_yaml("archetypes:\n- id: felix\n  name: Felix\n  problem_types: [felix]\n", path=target)
        append_archetype_to_yaml("archetypes:\n- id: felix\n  name: Felix v2\n  problem_types: [felix]\n", path=target)
        doc = _yaml.safe_load(target.read_text())
        felix = [a for a in doc["archetypes"] if a["id"] == "felix"]
        assert len(felix) == 1 and felix[0]["name"] == "Felix v2"

    def test_new_override_file_is_seeded_with_existing_archetypes(self, tmp_path, monkeypatch):
        target = tmp_path / "archetypes.yaml"
        monkeypatch.setenv("TACIT_ARCHETYPES_PATH", str(target))
        try:
            append_archetype_to_yaml("archetypes:\n- id: felix\n  name: Felix\n  problem_types: [felix]\n")
            doc = _yaml.safe_load(target.read_text())
            ids = {a["id"] for a in doc["archetypes"]}

            assert "felix" in ids
            assert "resource_saturation" in ids
            reload_archetypes()
            assert get_archetype("resource_saturation") is not None
            assert get_archetype("felix") is not None
        finally:
            monkeypatch.delenv("TACIT_ARCHETYPES_PATH", raising=False)
            reload_archetypes()

    def test_register_and_reload_makes_it_routable(self, tmp_path, monkeypatch):
        target = tmp_path / "archetypes.yaml"
        monkeypatch.setenv("TACIT_ARCHETYPES_PATH", str(target))
        try:
            append_archetype_to_yaml(
                "archetypes:\n- id: felix\n  name: Felix\n  problem_types: [felix]\n  panels: []\n"
            )
            assert get_archetype("felix") is not None
        finally:
            monkeypatch.delenv("TACIT_ARCHETYPES_PATH", raising=False)
            reload_archetypes()  # restore default registry for other tests

    def test_learning_context_retrieves_generated_archetype_by_catalog_overlap(self, tmp_path, monkeypatch):
        target = tmp_path / "archetypes.yaml"
        monkeypatch.setenv("TACIT_ARCHETYPES_PATH", str(target))
        try:
            append_archetype_to_yaml("""
archetypes:
  - id: felix_dataplane
    name: Felix Dataplane
    problem_types: [felix_dataplane]
    required_signals: [felix_iptables_save_errors]
    signal_bindings:
      felix_iptables_save_errors: felix_iptables_save_errors
    panels:
      - title: Iptables save errors
        queries:
          - expr: "rate(felix_iptables_save_errors[5m])"
""")
            intent = Intent(
                summary="show calico dataplane health",
                domain="networking",
                services=[],
                signals=[],
                keywords=["calico", "dataplane"],
                timerange="1h",
                problem_type="networking",
                archetypes=[ArchetypeMatch(type="unknown", confidence=0.2)],
            )
            catalog = [
                MetricEntry(
                    name="felix_iptables_save_errors",
                    datasource_uid="prom",
                    datasource_name="Prometheus",
                    datasource_type="prometheus",
                    query_language="promql",
                )
            ]

            ranked = get_archetypes_by_learning_context(intent, catalog)

            assert ranked
            assert ranked[0][0].id == "felix_dataplane"
        finally:
            monkeypatch.delenv("TACIT_ARCHETYPES_PATH", raising=False)
            reload_archetypes()
