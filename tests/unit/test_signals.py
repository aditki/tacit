"""Tests for the semantic signal mapping store, resolution engine, and dashboard ingestion.

Covers both PromQL (Grafana) and SignalFlow (SignalFx) extraction.
"""

from __future__ import annotations

import sqlite3
import tempfile
import time
from importlib.resources import files
from pathlib import Path

import pytest

from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.backends.base import DashboardFeatures
from dashforge.dashboard_ingest import (
    extract_aggregation_patterns,
    extract_metrics_from_promql,
    generate_archetype_yaml,
    infer_signals_from_metrics,
    parse_dashboard_json,
)
from dashforge.models.schemas import MetricEntry
from dashforge.signals import (
    SignalStore,
    _context_matches,
    _effective_confidence,
    _metric_matches_pattern,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def signal_store(tmp_path):
    """Create a fresh SignalStore with an isolated temp DB."""
    db_path = tmp_path / "test_signals.db"
    store = SignalStore(db_path=db_path)
    return store


@pytest.fixture
def signal_store_with_bootstrap(tmp_path):
    """SignalStore loaded with bootstrap signals.yaml."""
    db_path = tmp_path / "test_signals.db"
    store = SignalStore(db_path=db_path)
    store.load_from_yaml()
    return store


@pytest.fixture
def sample_catalog():
    """A sample metric catalog with custom SSO metrics."""
    return [
        MetricEntry(
            name="sso_auth_requests_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={sso-gateway}"],
        ),
        MetricEntry(
            name="sso_auth_failures_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={sso-gateway}", "reason={expired_token,invalid_cert}"],
        ),
        MetricEntry(
            name="sso_auth_latency_seconds_bucket",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={sso-gateway}", "le={0.1,0.5,1,5}"],
        ),
        MetricEntry(
            name="http_requests_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="container_cpu_usage_seconds_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]


# ── Signal Store basics ──────────────────────────────────────────────────────


class TestSignalStoreBasics:

    def test_register_and_list_signal_types(self, signal_store):
        signal_store.register_signal_type(
            "request_latency", description="Request latency", category="latency", unit="s"
        )
        signal_store.register_signal_type("error_rate", description="Error rate", category="errors", unit="percentunit")

        types = signal_store.list_signal_types()
        assert len(types) == 2
        names = {t["signal_type"] for t in types}
        assert names == {"request_latency", "error_rate"}

    def test_register_signal_type_upsert(self, signal_store):
        signal_store.register_signal_type("test_signal", description="v1")
        signal_store.register_signal_type("test_signal", description="v2")

        types = signal_store.list_signal_types()
        assert len(types) == 1
        assert types[0]["description"] == "v2"

    def test_get_signal_type_not_found(self, signal_store):
        assert signal_store.get_signal_type("nonexistent") is None

    def test_stats_empty(self, signal_store):
        stats = signal_store.stats()
        assert stats["signal_types"] == 0
        assert stats["metric_mappings"] == 0


# ── Signal ↔ metric mappings ────────────────────────────────────────────────


class TestSignalMappings:

    def test_add_and_retrieve_mapping(self, signal_store):
        signal_store.register_signal_type("request_latency")
        signal_store.add_mapping(
            "request_latency",
            "http_request_duration_seconds",
            confidence=0.95,
            source_type="bootstrap",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["metric_pattern"] == "http_request_duration_seconds"
        assert mappings[0]["confidence"] == 0.95

    def test_many_to_many_signal_metric(self, signal_store):
        """One metric can map to multiple signals (many-to-many)."""
        signal_store.add_mapping("saturation", "queue_depth_total", confidence=0.8)
        signal_store.add_mapping("throughput_mismatch", "queue_depth_total", confidence=0.6)
        signal_store.add_mapping("downstream_outage", "queue_depth_total", confidence=0.4)

        # Same metric under 3 different signals
        sat = signal_store.get_mappings_for_signal("saturation")
        thr = signal_store.get_mappings_for_signal("throughput_mismatch")
        out = signal_store.get_mappings_for_signal("downstream_outage")

        assert len(sat) == 1
        assert len(thr) == 1
        assert len(out) == 1
        assert sat[0]["metric_pattern"] == "queue_depth_total"
        assert thr[0]["metric_pattern"] == "queue_depth_total"

    def test_multiple_metrics_per_signal(self, signal_store):
        """One signal can map to many metrics."""
        signal_store.add_mapping("request_latency", "http_request_duration_seconds", 0.95)
        signal_store.add_mapping("request_latency", "payments_api_latency_ms", 0.8)
        signal_store.add_mapping("request_latency", "gateway_request_duration", 0.7)

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 3
        # Sorted by confidence desc
        assert mappings[0]["confidence"] == 0.95
        assert mappings[2]["confidence"] == 0.7

    def test_add_mapping_upsert_keeps_max_confidence(self, signal_store):
        signal_store.add_mapping("test_signal", "test_metric", confidence=0.5)
        signal_store.add_mapping("test_signal", "test_metric", confidence=0.9)

        mappings = signal_store.get_mappings_for_signal("test_signal")
        assert len(mappings) == 1
        assert mappings[0]["confidence"] == 0.9

    def test_provenance_tracking(self, signal_store):
        signal_store.add_mapping(
            "auth_latency",
            "sso_auth_latency_seconds",
            confidence=0.8,
            source_type="dashboard_ingest",
            source_refs=["dashboard-uid-123"],
        )

        mappings = signal_store.get_mappings_for_signal("auth_latency")
        assert mappings[0]["source_type"] == "dashboard_ingest"
        assert "dashboard-uid-123" in mappings[0]["source_refs"]

    def test_feedback_recording(self, signal_store):
        signal_store.add_mapping("test", "test_metric", 0.7)
        signal_store.record_feedback("test", "test_metric", positive=True)
        signal_store.record_feedback("test", "test_metric", positive=True)
        signal_store.record_feedback("test", "test_metric", positive=False)

        mappings = signal_store.get_mappings_for_signal("test")
        assert mappings[0]["positive_feedback"] == 2
        assert mappings[0]["negative_feedback"] == 1


# ── Context filtering ────────────────────────────────────────────────────────


class TestContextFiltering:

    def test_empty_context_matches_all(self):
        mapping = {
            "context_services": [],
            "context_datasource_types": [],
            "context_archetypes": [],
            "context_environments": [],
        }
        assert _context_matches(mapping, "any-svc", "prometheus", "latency", "prod")

    def test_service_context_filter(self):
        mapping = {
            "context_services": ["sso-gateway"],
            "context_datasource_types": [],
            "context_archetypes": [],
            "context_environments": [],
        }
        assert _context_matches(mapping, "sso-gateway", "", "", "")
        assert not _context_matches(mapping, "payment-service", "", "", "")

    def test_datasource_type_context_filter(self):
        mapping = {
            "context_services": [],
            "context_datasource_types": ["prometheus"],
            "context_archetypes": [],
            "context_environments": [],
        }
        assert _context_matches(mapping, "", "prometheus", "", "")
        assert not _context_matches(mapping, "", "cloudwatch", "", "")

    def test_context_filter_with_signal_store(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "sso_specific_latency",
            confidence=0.9,
            context_services=["sso-gateway"],
        )
        signal_store.add_mapping(
            "request_latency",
            "generic_latency",
            confidence=0.8,
        )

        # With SSO context — both match
        mappings = signal_store.get_mappings_for_signal("request_latency", context_service="sso-gateway")
        assert len(mappings) == 2

        # With different service — only generic matches
        mappings = signal_store.get_mappings_for_signal("request_latency", context_service="payment-service")
        assert len(mappings) == 1
        assert mappings[0]["metric_pattern"] == "generic_latency"

    def test_context_specific_mapping_penalized_without_context(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_specific_latency",
            confidence=0.9,
            context_services=["checkout"],
        )
        signal_store.add_mapping(
            "request_latency",
            "generic_latency",
            confidence=0.8,
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")

        assert [m["metric_pattern"] for m in mappings] == [
            "generic_latency",
            "checkout_specific_latency",
        ]
        assert mappings[1]["effective_confidence"] == pytest.approx(0.63, abs=0.001)

    def test_context_specific_mapping_not_penalized_with_matching_context(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_specific_latency",
            confidence=0.9,
            context_services=["checkout"],
        )
        signal_store.add_mapping(
            "request_latency",
            "generic_latency",
            confidence=0.8,
        )

        mappings = signal_store.get_mappings_for_signal("request_latency", context_service="checkout")

        assert mappings[0]["metric_pattern"] == "checkout_specific_latency"
        assert mappings[0]["effective_confidence"] == pytest.approx(0.9, abs=0.001)

    def test_context_penalty_does_not_make_trusted_mapping_disappear(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "low_confidence_checkout_latency",
            confidence=0.2,
            context_services=["checkout"],
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")

        assert len(mappings) == 1
        assert mappings[0]["metric_pattern"] == "low_confidence_checkout_latency"
        assert mappings[0]["effective_confidence"] == pytest.approx(0.14, abs=0.001)

    def test_conflict_preserves_global_mapping_context(self, signal_store):
        signal_store.add_mapping("request_latency", "latency_seconds", confidence=0.5)
        signal_store.add_mapping(
            "request_latency",
            "latency_seconds",
            confidence=0.6,
            context_services=["checkout"],
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency", include_decayed=True)

        assert mappings[0]["context_services"] == []
        assert mappings[0]["source_type"] == "teach"


# ── Confidence decay ─────────────────────────────────────────────────────────


class TestConfidenceDecay:

    def test_bootstrap_no_decay(self):
        mapping = {
            "confidence": 0.9,
            "source_type": "bootstrap",
            "last_seen": time.time() - 365 * 86400,  # 1 year ago
            "positive_feedback": 0,
            "negative_feedback": 0,
        }
        eff = _effective_confidence(mapping, time.time())
        assert eff == 0.9  # no decay for bootstrap

    def test_learned_mapping_decays(self):
        now = time.time()
        mapping = {
            "confidence": 0.9,
            "source_type": "dashboard_ingest",
            "last_seen": now - 90 * 86400,  # 90 days ago = 1 half-life
            "positive_feedback": 0,
            "negative_feedback": 0,
        }
        eff = _effective_confidence(mapping, now)
        assert 0.4 < eff < 0.5  # ~0.45 after one half-life

    def test_positive_feedback_boosts(self):
        now = time.time()
        mapping = {
            "confidence": 0.5,
            "source_type": "teach",
            "last_seen": now,  # fresh
            "positive_feedback": 10,
            "negative_feedback": 0,
        }
        eff = _effective_confidence(mapping, now)
        assert eff > 0.5  # boosted by all-positive feedback
        assert eff == pytest.approx(0.5 * 1.3, abs=0.01)

    def test_negative_feedback_penalizes(self):
        now = time.time()
        mapping = {
            "confidence": 0.5,
            "source_type": "teach",
            "last_seen": now,
            "positive_feedback": 0,
            "negative_feedback": 10,
        }
        eff = _effective_confidence(mapping, now)
        assert eff < 0.5  # penalized
        assert eff == pytest.approx(0.5 * 0.7, abs=0.01)

    def test_min_confidence_floor(self):
        now = time.time()
        mapping = {
            "confidence": 0.01,
            "source_type": "dashboard_ingest",
            "last_seen": now - 365 * 86400,
            "positive_feedback": 0,
            "negative_feedback": 100,
        }
        eff = _effective_confidence(mapping, now)
        assert eff >= 0.05  # never drops below MIN_CONFIDENCE


# ── Metric pattern matching ──────────────────────────────────────────────────


class TestMetricPatternMatching:

    def test_exact_match(self):
        assert _metric_matches_pattern("http_requests_total", "http_requests_total")

    def test_glob_wildcard_prefix(self):
        assert _metric_matches_pattern("sso_auth_failures_total", "*auth*fail*")

    def test_glob_wildcard_suffix(self):
        assert _metric_matches_pattern("http_request_duration_seconds_bucket", "*_duration_seconds*")

    def test_glob_no_match(self):
        assert not _metric_matches_pattern("cpu_usage_total", "*auth*")

    def test_substring_match(self):
        assert _metric_matches_pattern("my_custom_latency_metric", "latency")

    def test_no_match(self):
        assert not _metric_matches_pattern("cpu_usage", "memory")


# ── Signal resolution ────────────────────────────────────────────────────────


class TestSignalResolution:

    def test_resolve_signal_exact_match(self, signal_store, sample_catalog):
        signal_store.add_mapping("request_rate", "http_requests_total", 0.95, source_type="bootstrap")
        resolved = signal_store.resolve_signal("request_rate", sample_catalog)
        assert len(resolved) == 1
        assert resolved[0][0].name == "http_requests_total"
        assert resolved[0][1] == 0.95

    def test_resolve_signal_pattern_match(self, signal_store, sample_catalog):
        signal_store.add_mapping("auth_failure_count", "*auth*fail*", 0.85, source_type="bootstrap")
        resolved = signal_store.resolve_signal("auth_failure_count", sample_catalog)
        assert len(resolved) == 1
        assert resolved[0][0].name == "sso_auth_failures_total"

    def test_resolve_signal_multiple_matches(self, signal_store, sample_catalog):
        signal_store.add_mapping("auth_request_rate", "*auth*requests*", 0.8, source_type="bootstrap")
        resolved = signal_store.resolve_signal("auth_request_rate", sample_catalog)
        assert len(resolved) >= 1
        names = {r[0].name for r in resolved}
        assert "sso_auth_requests_total" in names

    def test_resolve_signal_no_match(self, signal_store, sample_catalog):
        signal_store.add_mapping("kafka_lag", "kafka_consumer_lag", 0.9, source_type="bootstrap")
        resolved = signal_store.resolve_signal("kafka_lag", sample_catalog)
        assert len(resolved) == 0

    def test_resolve_signals_for_archetype(self, signal_store, sample_catalog):
        """Core SSO use case: archetype says auth_requests_total but env has sso_auth_requests_total."""
        signal_store.add_mapping("auth_request_rate", "*auth*requests*total", 0.85, source_type="bootstrap")
        signal_store.add_mapping("auth_failure_count", "*auth*fail*total", 0.85, source_type="bootstrap")
        signal_store.add_mapping("auth_latency", "*auth*latency*", 0.8, source_type="bootstrap")

        signal_bindings = {
            "auth_request_rate": "auth_requests_total",
            "auth_failure_count": "failed_login_attempts_total",
            "auth_latency": "auth_latency_seconds",
        }

        subs = signal_store.resolve_signals_for_archetype(
            signal_bindings=signal_bindings,
            catalog=sample_catalog,
        )

        # auth_requests_total is NOT in catalog → should be resolved
        assert "auth_requests_total" in subs
        assert subs["auth_requests_total"] == "sso_auth_requests_total"

        # failed_login_attempts_total is NOT in catalog → should resolve to sso_auth_failures_total
        assert "failed_login_attempts_total" in subs
        assert subs["failed_login_attempts_total"] == "sso_auth_failures_total"

    def test_resolve_skips_existing_metrics(self, signal_store, sample_catalog):
        """If the default metric exists in catalog, no substitution needed."""
        signal_store.add_mapping("request_rate", "*requests*total", 0.9, source_type="bootstrap")

        subs = signal_store.resolve_signals_for_archetype(
            signal_bindings={"request_rate": "http_requests_total"},
            catalog=sample_catalog,
        )

        # http_requests_total IS in catalog → no substitution
        assert "http_requests_total" not in subs

    def test_default_presence_is_scoped_to_target_language_and_datasource(self, signal_store):
        signal_store.add_mapping(
            "request_rate",
            "prom_http_requests_total",
            confidence=0.9,
            context_datasource_types=["prometheus"],
            source_type="teach",
        )
        catalog = [
            MetricEntry(
                name="http_requests_total",
                datasource_uid="sfx-1",
                datasource_name="SignalFx",
                datasource_type="signalfx",
                query_language="signalflow",
            ),
            MetricEntry(
                name="prom_http_requests_total",
                datasource_uid="prom-1",
                datasource_name="Prometheus",
                datasource_type="prometheus",
                query_language="promql",
            ),
        ]

        subs = signal_store.resolve_signals_for_archetype(
            signal_bindings={"request_rate": "http_requests_total"},
            catalog=catalog,
            context_datasource_type="prometheus",
            target_query_language="promql",
        )

        assert subs == {"http_requests_total": "prom_http_requests_total"}


# ── Metric substitution in archetypes ────────────────────────────────────────


class TestArchetypeMetricSubstitution:

    def test_apply_metric_substitutions(self):
        from dashforge.archetypes.engine import _apply_metric_substitutions

        archetype = InvestigationArchetype(
            id="test_auth",
            name="Test Auth",
            problem_types=["auth_failures"],
            panels=[
                PanelTemplate(
                    title="Auth Rate",
                    queries=[
                        QueryTemplate(
                            expr="sum(rate(auth_requests_total{{{service_filter}}}[{rate_interval}]))",
                        )
                    ],
                ),
                PanelTemplate(
                    title="Auth Failures",
                    queries=[
                        QueryTemplate(
                            expr="sum(increase(failed_login_attempts_total{{{service_filter}}}[{rate_interval}]))",
                        )
                    ],
                ),
            ],
        )

        substitutions = {
            "auth_requests_total": "sso_auth_requests_total",
            "failed_login_attempts_total": "sso_auth_failures_total",
        }

        result = _apply_metric_substitutions(archetype, substitutions)

        assert "sso_auth_requests_total" in result.panels[0].queries[0].expr
        # The old metric name should be replaced — check the expr starts with the new one
        assert result.panels[0].queries[0].expr.startswith("sum(rate(sso_auth_requests_total")
        assert "sso_auth_failures_total" in result.panels[1].queries[0].expr

    def test_no_substitution_returns_same(self):
        from dashforge.archetypes.engine import _apply_metric_substitutions

        archetype = InvestigationArchetype(
            id="test",
            name="Test",
            problem_types=["test"],
            panels=[
                PanelTemplate(
                    title="T",
                    queries=[QueryTemplate(expr="metric{filter}")],
                )
            ],
        )

        result = _apply_metric_substitutions(archetype, {})
        assert result is archetype  # identity — no copy needed


# ── PromQL metric extraction ────────────────────────────────────────────────


class TestPromQLExtraction:

    def test_simple_metric(self):
        metrics = extract_metrics_from_promql('http_requests_total{job="api"}')
        assert "http_requests_total" in metrics

    def test_rate_wrapped(self):
        metrics = extract_metrics_from_promql('sum(rate(http_requests_total{service="checkout"}[5m])) by (status)')
        assert "http_requests_total" in metrics
        assert "sum" not in metrics
        assert "rate" not in metrics
        assert "status" not in metrics

    def test_histogram_quantile(self):
        metrics = extract_metrics_from_promql(
            'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{job="api"}[5m])) by (le))'
        )
        assert "http_request_duration_seconds_bucket" in metrics
        assert "histogram_quantile" not in metrics

    def test_multiple_metrics(self):
        expr = 'sum(rate(http_requests_total{status=~"5.."}[5m])) / ' "sum(rate(http_requests_total[5m]))"
        metrics = extract_metrics_from_promql(expr)
        assert "http_requests_total" in metrics

    def test_excludes_promql_keywords(self):
        metrics = extract_metrics_from_promql("topk(5, sum(rate(my_metric[5m])) by (instance))")
        assert "my_metric" in metrics
        assert "topk" not in metrics
        assert "sum" not in metrics
        assert "by" not in metrics
        assert "instance" not in metrics

    def test_custom_sso_metrics(self):
        metrics = extract_metrics_from_promql(
            'sum(rate(sso_auth_failures_total{service="sso-gateway"}[5m])) by (reason)'
        )
        assert "sso_auth_failures_total" in metrics
        assert "reason" not in metrics

    def test_without_grouping_labels_are_not_metrics(self):
        metrics = extract_metrics_from_promql("sum without(instance, pod) (http_requests_total)")
        assert "http_requests_total" in metrics
        assert "instance" not in metrics
        assert "pod" not in metrics

    def test_vector_matching_labels_are_not_metrics(self):
        metrics = extract_metrics_from_promql("http_requests_total / ignoring(instance) group_left(job) target_info")
        assert "http_requests_total" in metrics
        assert "target_info" in metrics
        assert "instance" not in metrics
        assert "job" not in metrics

    def test_falls_back_to_regex_for_templated_queries(self):
        metrics = extract_metrics_from_promql("sum(rate(http_requests_total[$__rate_interval])) by (status)")
        assert "http_requests_total" in metrics
        assert "status" not in metrics

    def test_range_selector_walks_matrix_vs(self):
        metrics = extract_metrics_from_promql("rate(http_requests_total[5m])")
        assert metrics == ["http_requests_total"]


class TestAggregationExtraction:

    def test_sum_rate(self):
        patterns = extract_aggregation_patterns("sum(rate(http_requests_total[5m])) by (status)")
        assert any(p["aggregation"] == "sum" and p.get("inner_function") == "rate" for p in patterns)

    def test_histogram_quantile(self):
        patterns = extract_aggregation_patterns("histogram_quantile(0.99, sum(rate(metric_bucket[5m])) by (le))")
        assert any(p["aggregation"] == "histogram_quantile" for p in patterns)

    def test_bare_rate(self):
        patterns = extract_aggregation_patterns("rate(container_cpu_usage_seconds_total[5m])")
        assert any(p["aggregation"] == "rate" for p in patterns)


# ── Dashboard JSON parsing ───────────────────────────────────────────────────


class TestDashboardParsing:

    def test_parse_simple_dashboard(self):
        dashboard_json = {
            "dashboard": {
                "uid": "test-dash-1",
                "title": "SSO Service Health",
                "tags": ["sso", "auth"],
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Auth Request Rate",
                        "targets": [{"expr": "sum(rate(sso_auth_requests_total[5m])) by (result)"}],
                    },
                    {
                        "type": "timeseries",
                        "title": "Auth Failures",
                        "targets": [{"expr": "sum(increase(sso_auth_failures_total[5m])) by (reason)"}],
                    },
                    {
                        "type": "stat",
                        "title": "Auth Latency p95",
                        "targets": [
                            {"expr": "histogram_quantile(0.95, sum(rate(sso_auth_latency_seconds_bucket[5m])) by (le))"}
                        ],
                        "fieldConfig": {"defaults": {"unit": "s"}},
                    },
                ],
                "links": [],
                "annotations": {"list": []},
            }
        }

        result = parse_dashboard_json(dashboard_json)

        assert result["dashboard_uid"] == "test-dash-1"
        assert result["dashboard_title"] == "SSO Service Health"
        assert result["panel_count"] == 3
        assert "sso_auth_requests_total" in result["metrics_found"]
        assert "sso_auth_failures_total" in result["metrics_found"]
        assert "sso_auth_latency_seconds_bucket" in result["metrics_found"]
        assert len(result["panel_titles"]) == 3
        assert len(result["metric_cooccurrence"]) > 0

    def test_parse_dashboard_with_rows(self):
        dashboard_json = {
            "dashboard": {
                "uid": "row-dash",
                "title": "Row Test",
                "tags": [],
                "panels": [
                    {
                        "type": "row",
                        "title": "Traffic",
                        "panels": [
                            {
                                "type": "timeseries",
                                "title": "Request Rate",
                                "targets": [{"expr": "rate(requests_total[5m])"}],
                            },
                        ],
                    },
                    {
                        "type": "row",
                        "title": "Errors",
                        "panels": [
                            {
                                "type": "timeseries",
                                "title": "Error Rate",
                                "targets": [{"expr": "rate(errors_total[5m])"}],
                            },
                        ],
                    },
                ],
                "links": [],
                "annotations": {"list": []},
            }
        }

        result = parse_dashboard_json(dashboard_json)
        assert result["panel_count"] == 2
        assert len(result["row_groups"]) == 2
        row_names = {rg["row"] for rg in result["row_groups"]}
        assert "Traffic" in row_names
        assert "Errors" in row_names

    def test_parse_skips_text_panels(self):
        dashboard_json = {
            "dashboard": {
                "uid": "skip-test",
                "title": "Skip Test",
                "tags": [],
                "panels": [
                    {"type": "text", "title": "Instructions", "targets": []},
                    {"type": "timeseries", "title": "Real Panel", "targets": [{"expr": "up"}]},
                ],
                "links": [],
                "annotations": {"list": []},
            }
        }

        result = parse_dashboard_json(dashboard_json)
        assert result["panel_count"] == 1
        assert result["panel_titles"] == ["Real Panel"]


# ── Signal inference from metrics ────────────────────────────────────────────


class TestSignalInference:

    def test_infer_signals_from_sso_metrics(self, signal_store_with_bootstrap):
        metrics = [
            "sso_auth_requests_total",
            "sso_auth_failures_total",
            "sso_auth_latency_seconds_bucket",
        ]
        signals = infer_signals_from_metrics(metrics)

        # Should infer auth-related signals
        assert len(signals) > 0
        # At least one auth signal should be inferred
        auth_signals = [s for s in signals if "auth" in s["signal_type"]]
        assert len(auth_signals) > 0

    def test_infer_signals_from_standard_metrics(self, signal_store_with_bootstrap):
        metrics = [
            "http_requests_total",
            "http_request_duration_seconds_bucket",
            "container_cpu_usage_seconds_total",
        ]
        signals = infer_signals_from_metrics(metrics)

        signal_types = {s["signal_type"] for s in signals}
        assert "request_rate" in signal_types or "request_latency" in signal_types


# ── Archetype YAML generation ───────────────────────────────────────────────


class TestArchetypeGeneration:

    def test_generate_archetype_yaml(self, signal_store_with_bootstrap):
        extracted = {
            "dashboard_uid": "sso-health",
            "dashboard_title": "SSO Service Health",
            "dashboard_tags": ["sso", "auth"],
            "query_language": "promql",
            "metrics_found": ["sso_auth_requests_total", "sso_auth_failures_total"],
            "panel_count": 2,
            "panels": [
                {
                    "title": "Auth Rate",
                    "queries": ["sum(rate(sso_auth_requests_total[5m]))"],
                    "row": "Auth",
                    "unit": "reqps",
                },
                {
                    "title": "Auth Failures",
                    "queries": ["sum(increase(sso_auth_failures_total[5m]))"],
                    "row": "Auth",
                    "unit": "short",
                },
            ],
        }
        signals = infer_signals_from_metrics(extracted["metrics_found"])
        yaml_str = generate_archetype_yaml(extracted, signals, archetype_id="sso_health")

        assert "sso_health" in yaml_str
        assert "SSO Service Health" in yaml_str
        assert "sso_auth_requests_total" in yaml_str
        assert "auto-generated" in yaml_str
        import yaml

        parsed = yaml.safe_load(yaml_str)
        query = parsed["archetypes"][0]["panels"][0]["queries"][0]
        assert query["query_language"] == "promql"


# ── Ingested dashboard records ───────────────────────────────────────────────


class TestIngestedDashboards:

    def test_record_and_retrieve(self, signal_store):
        signal_store.record_ingested_dashboard(
            "dash-1",
            backend_name="grafana",
            dashboard_title="Test Dashboard",
            metrics_found=["metric_a", "metric_b"],
            panel_count=3,
            signals_inferred=["request_latency"],
        )

        result = signal_store.get_ingested_dashboard("dash-1")
        assert result is not None
        assert result["backend_name"] == "grafana"
        assert result["dashboard_title"] == "Test Dashboard"
        assert result["metrics_found"] == ["metric_a", "metric_b"]
        assert result["panel_count"] == 3
        assert result["status"] == "pending"

    @pytest.mark.asyncio
    async def test_ingest_dashboard_persists_generated_archetype(self, signal_store, monkeypatch):
        from dashforge import dashboard_ingest as di

        class FakeBackend:
            async def ingest_dashboard(self, uid):
                return DashboardFeatures(
                    dashboard_uid=uid,
                    dashboard_title="CPU Dashboard",
                    dashboard_tags=[],
                    backend_name="signalfx",
                    query_language="signalflow",
                    metrics_found=["cpu.utilization"],
                    panel_count=1,
                    panel_titles=["CPU"],
                    panels=[
                        {
                            "title": "CPU",
                            "queries": ["data('cpu.utilization').publish()"],
                            "row": "",
                            "unit": "",
                            "description": "",
                        }
                    ],
                )

            async def close(self):
                return None

        monkeypatch.setattr(di, "get_signal_store", lambda: signal_store)

        result = await di.ingest_dashboard("cpu-dash", backend=FakeBackend(), auto_approve=False)

        stored = signal_store.get_ingested_dashboard("cpu-dash")
        assert stored is not None
        assert stored["backend_name"] == "signalfx"
        assert stored["archetype_generated"] == result["archetype_yaml"]
        assert "archetypes:" in stored["archetype_generated"]

    def test_dashboard_uid_is_scoped_by_backend(self, signal_store):
        signal_store.record_ingested_dashboard(
            "shared-dash",
            backend_name="grafana",
            dashboard_title="Grafana Dashboard",
            status="pending",
        )
        signal_store.record_ingested_dashboard(
            "shared-dash",
            backend_name="signalfx",
            dashboard_title="SignalFx Dashboard",
            status="pending",
        )

        grafana = signal_store.get_ingested_dashboard("shared-dash", backend_name="grafana")
        signalfx = signal_store.get_ingested_dashboard("shared-dash", backend_name="signalfx")
        ambiguous = signal_store.get_ingested_dashboard("shared-dash")

        assert grafana is not None
        assert signalfx is not None
        assert grafana["dashboard_title"] == "Grafana Dashboard"
        assert signalfx["dashboard_title"] == "SignalFx Dashboard"
        assert ambiguous is None

        assert signal_store.approve_ingested_dashboard("shared-dash", backend_name="grafana")
        assert signal_store.get_ingested_dashboard("shared-dash", backend_name="grafana")["status"] == "approved"
        assert signal_store.get_ingested_dashboard("shared-dash", backend_name="signalfx")["status"] == "pending"

    def test_existing_uid_unique_table_migrates_to_backend_scope(self, tmp_path):
        db_path = tmp_path / "legacy_signals.db"
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE ingested_dashboards (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    dashboard_uid       TEXT NOT NULL UNIQUE,
                    dashboard_title     TEXT NOT NULL DEFAULT '',
                    dashboard_tags      TEXT NOT NULL DEFAULT '[]',
                    metrics_found       TEXT NOT NULL DEFAULT '[]',
                    panel_count         INTEGER NOT NULL DEFAULT 0,
                    row_groups          TEXT NOT NULL DEFAULT '[]',
                    metric_cooccurrence TEXT NOT NULL DEFAULT '{}',
                    aggregation_patterns TEXT NOT NULL DEFAULT '[]',
                    query_transformations TEXT NOT NULL DEFAULT '[]',
                    panel_titles        TEXT NOT NULL DEFAULT '[]',
                    alert_links         TEXT NOT NULL DEFAULT '[]',
                    drilldown_links     TEXT NOT NULL DEFAULT '[]',
                    status              TEXT NOT NULL DEFAULT 'pending',
                    signals_inferred    TEXT NOT NULL DEFAULT '[]',
                    archetype_generated TEXT NOT NULL DEFAULT '',
                    created_at          REAL NOT NULL,
                    reviewed_at         REAL
                );
                INSERT INTO ingested_dashboards (dashboard_uid, dashboard_title, created_at)
                VALUES ('shared-dash', 'Legacy Grafana', 1.0);
            """)

        store = SignalStore(db_path=db_path)
        store.record_ingested_dashboard(
            "shared-dash",
            backend_name="signalfx",
            dashboard_title="SignalFx Dashboard",
        )

        legacy = store.get_ingested_dashboard("shared-dash", backend_name="")
        signalfx = store.get_ingested_dashboard("shared-dash", backend_name="signalfx")

        assert legacy is not None
        assert signalfx is not None
        assert legacy["dashboard_title"] == "Legacy Grafana"
        assert signalfx["dashboard_title"] == "SignalFx Dashboard"

    def test_list_by_status(self, signal_store):
        signal_store.record_ingested_dashboard("d1", status="pending")
        signal_store.record_ingested_dashboard("d2", status="approved")
        signal_store.record_ingested_dashboard("d3", status="pending")

        pending = signal_store.list_ingested_dashboards(status="pending")
        assert len(pending) == 2

        approved = signal_store.list_ingested_dashboards(status="approved")
        assert len(approved) == 1

    def test_approve_dashboard(self, signal_store):
        signal_store.record_ingested_dashboard("d1", status="pending")
        assert signal_store.approve_ingested_dashboard("d1")

        result = signal_store.get_ingested_dashboard("d1")
        assert result["status"] == "approved"
        assert result["reviewed_at"] is not None

    def test_approve_nonexistent(self, signal_store):
        assert not signal_store.approve_ingested_dashboard("nonexistent")


# ── YAML loading ─────────────────────────────────────────────────────────────


class TestYAMLLoading:

    def test_load_bootstrap_signals(self, tmp_path):
        yaml_content = """
signals:
  test_latency:
    description: Test latency
    category: latency
    unit: s
    metric_patterns:
      - pattern: "test_duration_seconds"
        confidence: 0.9
      - pattern: "*_latency_*"
        confidence: 0.6
  test_errors:
    description: Test errors
    category: errors
    unit: short
    metric_patterns:
      - pattern: "*_errors_total"
        confidence: 0.85
"""
        yaml_path = tmp_path / "signals.yaml"
        yaml_path.write_text(yaml_content)

        db_path = tmp_path / "test.db"
        store = SignalStore(db_path=db_path)
        count = store.load_from_yaml(yaml_path)

        assert count == 3  # 2 + 1 patterns
        types = store.list_signal_types()
        assert len(types) == 2

        mappings = store.get_mappings_for_signal("test_latency")
        assert len(mappings) == 2

    def test_load_project_signals_yaml(self):
        """Verify the actual project signals.yaml loads without errors."""
        db_path = Path(tempfile.mktemp(suffix=".db"))
        try:
            store = SignalStore(db_path=db_path)
            count = store.load_from_yaml()
            assert count > 20  # should have many bootstrap mappings
            types = store.list_signal_types()
            assert len(types) > 10
            stats = store.stats()
            assert stats["signal_types"] > 10
            assert stats["metric_mappings"] > 20
        finally:
            db_path.unlink(missing_ok=True)


# ── End-to-end: SSO custom metrics scenario ──────────────────────────────────


class TestEndToEndSSO:
    """The original motivating use case: SSO service with custom metrics."""

    def test_sso_signal_resolution_e2e(self, signal_store_with_bootstrap, sample_catalog):
        """Full flow: archetype with generic metrics → signal resolution →
        substituted to SSO-specific metrics."""
        store = signal_store_with_bootstrap

        # The archetype expects these generic metrics
        signal_bindings = {
            "auth_request_rate": "auth_requests_total",
            "auth_failure_count": "failed_login_attempts_total",
            "auth_latency": "auth_latency_seconds",
        }

        # But the catalog has SSO-specific ones
        subs = store.resolve_signals_for_archetype(
            signal_bindings=signal_bindings,
            catalog=sample_catalog,
        )

        # Verify all three were resolved
        assert "auth_requests_total" in subs
        assert "failed_login_attempts_total" in subs
        # auth_latency resolves to the bucket metric
        assert "auth_latency_seconds" in subs

        # Verify they resolved to the correct SSO metrics
        assert "sso" in subs["auth_requests_total"]
        assert "sso" in subs["failed_login_attempts_total"]


# ── SignalFlow metric extraction ───────────────────────────────────────────


class TestSignalFlowExtraction:
    """Test SignalFlow metric name and pattern extraction."""

    def test_simple_data_call(self):
        from dashforge.backends.signalfx import _extract_metrics_from_signalflow

        metrics = _extract_metrics_from_signalflow("data('cpu.utilization').publish()")
        assert metrics == ["cpu.utilization"]

    def test_multiple_data_calls(self):
        from dashforge.backends.signalfx import _extract_metrics_from_signalflow

        program = """
        A = data('requests.count', filter=filter('service', 'api')).publish()
        B = data('errors.count', filter=filter('service', 'api')).publish()
        """
        metrics = _extract_metrics_from_signalflow(program)
        assert "requests.count" in metrics
        assert "errors.count" in metrics
        assert len(metrics) == 2

    def test_data_with_double_quotes(self):
        from dashforge.backends.signalfx import _extract_metrics_from_signalflow

        metrics = _extract_metrics_from_signalflow('data("memory.usage").publish()')
        assert metrics == ["memory.usage"]

    def test_analytics_patterns(self):
        from dashforge.backends.signalfx import _extract_signalflow_patterns

        program = "data('cpu.utilization').mean().percentile(pct=95).publish()"
        patterns = _extract_signalflow_patterns(program)
        agg_names = [p["aggregation"] for p in patterns]
        assert "mean" in agg_names
        assert "percentile" in agg_names

    def test_rate_and_sum(self):
        from dashforge.backends.signalfx import _extract_signalflow_patterns

        program = "data('requests.count').sum().rate().publish()"
        patterns = _extract_signalflow_patterns(program)
        agg_names = [p["aggregation"] for p in patterns]
        assert "sum" in agg_names
        assert "rate" in agg_names

    def test_no_metrics(self):
        from dashforge.backends.signalfx import _extract_metrics_from_signalflow

        assert _extract_metrics_from_signalflow("") == []
        assert _extract_metrics_from_signalflow("publish()") == []


# ── DashboardFeatures dataclass ──────────────────────────────────────────


class TestDashboardFeatures:
    """Verify the vendor-agnostic DashboardFeatures dataclass."""

    def test_defaults(self):
        f = DashboardFeatures()
        assert f.dashboard_uid == ""
        assert f.metrics_found == []
        assert f.panel_count == 0
        assert f.backend_name == ""

    def test_grafana_features(self):
        f = DashboardFeatures(
            dashboard_uid="graf-123",
            dashboard_title="API Health",
            backend_name="grafana",
            query_language="promql",
            metrics_found=["http_requests_total", "http_request_duration_seconds"],
            panel_count=3,
        )
        assert f.backend_name == "grafana"
        assert f.query_language == "promql"
        assert len(f.metrics_found) == 2

    def test_signalfx_features(self):
        f = DashboardFeatures(
            dashboard_uid="sfx-456",
            dashboard_title="API Health",
            backend_name="signalfx",
            query_language="signalflow",
            metrics_found=["requests.count", "latency.p99"],
            panel_count=2,
        )
        assert f.backend_name == "signalfx"
        assert f.query_language == "signalflow"

    def test_features_to_dict(self):
        from dashforge.dashboard_ingest import _features_to_dict

        f = DashboardFeatures(
            dashboard_uid="test",
            dashboard_title="Test",
            metrics_found=["m1", "m2"],
            panel_count=2,
            backend_name="grafana",
        )
        d = _features_to_dict(f)
        assert isinstance(d, dict)
        assert d["dashboard_uid"] == "test"
        assert d["metrics_found"] == ["m1", "m2"]
        assert d["backend_name"] == "grafana"

    def test_signal_inference_works_with_features(self, signal_store_with_bootstrap):
        """Signal inference is vendor-agnostic — works the same for both backends."""
        # Simulate SignalFx metrics (dot-separated naming)
        sfx_metrics = ["cpu.utilization", "memory.usage"]
        grafana_metrics = ["container_cpu_usage_seconds_total", "process_resident_memory_bytes"]

        infer_signals_from_metrics(sfx_metrics)
        grafana_signals = infer_signals_from_metrics(grafana_metrics)

        # Grafana standard metrics should match known signals
        grafana_types = {s["signal_type"] for s in grafana_signals}
        assert "cpu_usage" in grafana_types or "memory_usage" in grafana_types


# ── Signal coverage dashboard ingestion tests ───────────────────────────


class TestSignalCoverageDashboard:
    """Tests for the provisioned Grafana dashboard that exercises every signal category.

    The dashboard JSON fixture lives at dev/grafana/provisioning/dashboards/signal_coverage.json.
    """

    @pytest.fixture
    def dashboard_json(self):
        """Load the signal coverage dashboard JSON fixture."""
        import json

        fixture_path = Path(__file__).parent.parent.parent / "dev/grafana/provisioning/dashboards/signal_coverage.json"
        with open(fixture_path) as f:
            return json.load(f)

    @pytest.fixture
    def extracted(self, dashboard_json):
        """Parse the dashboard fixture and return extracted features."""
        return parse_dashboard_json(dashboard_json)

    @pytest.fixture
    def inferred_signals(self, extracted, signal_store_with_bootstrap):
        """Infer signals from the extracted metrics."""
        return infer_signals_from_metrics(
            extracted["metrics_found"],
            extracted.get("panels"),
        )

    # ── Metric extraction ──────────────────────────────────────────────

    def test_extracts_metrics(self, extracted):
        """Dashboard should yield a rich metric catalog."""
        metrics = extracted["metrics_found"]
        assert len(metrics) >= 15, f"Expected >=15 metrics, got {len(metrics)}"

    def test_contains_latency_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "http_request_duration_seconds" in metrics or "http_request_duration_seconds_bucket" in metrics

    def test_contains_throughput_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "http_requests_total" in metrics

    def test_contains_saturation_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        saturation = {
            "container_cpu_usage_seconds_total",
            "container_memory_working_set_bytes",
            "http_requests_in_flight",
            "db_connections_active",
        }
        assert saturation & metrics, f"No saturation metrics found in {metrics}"

    def test_contains_stability_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "kube_pod_container_restarts_total" in metrics

    def test_contains_error_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "http_requests_total" in metrics  # used with status=~"5.."

    def test_contains_db_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        db = {"db_query_duration_seconds", "db_connections_active"}
        assert db & metrics, f"No DB metrics found in {metrics}"

    def test_contains_cache_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        cache = {"cache_hit_total", "cache_miss_total"}
        assert cache & metrics, f"No cache metrics found in {metrics}"

    def test_contains_network_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        net = {
            "network_bytes_received_total",
            "network_bytes_transmitted_total",
            "dns_failures_total",
            "tls_handshake_failures_total",
        }
        assert net & metrics, f"No network metrics found in {metrics}"

    def test_contains_queue_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        q = {"kafka_consumer_lag", "message_queue_depth"}
        assert q & metrics, f"No queue metrics found in {metrics}"

    # ── Panel & row extraction ─────────────────────────────────────────

    def test_panel_count(self, extracted):
        """Should have panels from all signal categories."""
        assert extracted["panel_count"] >= 12

    def test_panel_titles_not_empty(self, extracted):
        assert len(extracted["panel_titles"]) >= 12
        for t in extracted["panel_titles"]:
            assert len(t) > 0, "Panel title should not be empty"

    def test_row_groups(self, extracted):
        """Dashboard uses row panels to group by signal category."""
        row_names = [r["row"] for r in extracted["row_groups"]]
        assert len(row_names) >= 4, f"Expected >=4 rows, got {row_names}"

    # ── Co-occurrence & aggregation ────────────────────────────────────

    def test_metric_cooccurrence(self, extracted):
        cooc = extracted["metric_cooccurrence"]
        assert len(cooc) > 0, "Should have metric co-occurrence data"

    def test_aggregation_patterns(self, extracted):
        aggs = extracted["aggregation_patterns"]
        agg_types = {a["aggregation"] for a in aggs}
        assert "rate" in agg_types, f"rate() not found in {agg_types}"

    def test_has_histogram_quantile(self, extracted):
        aggs = extracted["aggregation_patterns"]
        agg_types = {a["aggregation"] for a in aggs}
        assert "histogram_quantile" in agg_types

    # ── Links ──────────────────────────────────────────────────────────

    def test_has_drilldown_links(self, extracted):
        assert len(extracted["drilldown_links"]) >= 1

    # ── Dashboard metadata ─────────────────────────────────────────────

    def test_dashboard_title(self, extracted):
        assert "signal" in extracted["dashboard_title"].lower()

    def test_dashboard_tags(self, extracted):
        tags = extracted["dashboard_tags"]
        assert "dashforge" in tags or "signals" in tags

    # ── Signal inference ───────────────────────────────────────────────

    def test_infers_signals(self, inferred_signals):
        assert len(inferred_signals) >= 10

    def test_covers_latency_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "request_latency" in types

    def test_covers_throughput_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "request_rate" in types

    def test_covers_error_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "error_rate" in types

    def test_covers_saturation_signals(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        saturation = {"cpu_usage", "memory_usage", "in_flight_requests", "queue_depth", "db_connection_pool"}
        assert types & saturation, f"No saturation signals in {types}"

    def test_covers_cache_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "cache_hit_ratio" in types

    def test_covers_stability_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "pod_restarts" in types

    def test_covers_network_signals(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        net = {"network_bytes", "dns_failures", "tls_handshake_failures"}
        assert types & net, f"No network signals in {types}"

    def test_covers_messaging_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "consumer_lag" in types

    def test_covers_db_latency_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "db_query_latency" in types

    def test_signal_categories_coverage(self, inferred_signals):
        """Verify we hit at least 8 of the 12 signal categories."""
        # Collect categories from inferred signals by looking up
        # the signal_type in the bootstrap yaml
        import yaml

        resource = files("dashforge.data").joinpath("signals.yaml")
        with resource.open() as f:
            data = yaml.safe_load(f)
        sig_defs = data.get("signals", {})

        categories = set()
        for s in inferred_signals:
            sig_def = sig_defs.get(s["signal_type"], {})
            cat = sig_def.get("category", "")
            if cat:
                categories.add(cat)
        assert len(categories) >= 8, f"Expected >=8 categories, got {len(categories)}: {categories}"

    # ── Archetype generation ───────────────────────────────────────────

    def test_generates_archetype_yaml(self, extracted, inferred_signals):
        yaml_str = generate_archetype_yaml(extracted, inferred_signals)
        assert "archetypes:" in yaml_str
        assert "required_signals:" in yaml_str
        assert "signal_bindings:" in yaml_str
        assert "panels:" in yaml_str


# ── Bug 3: Literal braces in generated archetype YAML ────────────────────


class TestArchetypeYamlBraceEscaping:
    """Queries with label selectors like {service=\"api\"} must not break
    str.format() when the generated archetype is later compiled."""

    def test_braces_are_escaped_in_generated_yaml(self):
        """Concrete query braces must be escaped as {{ / }} so
        compile_archetype()'s str.format(**params) does not interpret them
        as Python format placeholders."""
        import yaml

        extracted = {
            "dashboard_title": "Test Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["http_requests_total"],
            "panels": [
                {
                    "title": "RPS",
                    "queries": ['rate(http_requests_total{service="api"}[5m])'],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        signals = [
            {"signal_type": "request_rate", "metric": "http_requests_total", "confidence": 0.8},
        ]
        yaml_str = generate_archetype_yaml(extracted, signals)
        parsed = yaml.safe_load(yaml_str)
        expr = parsed["archetypes"][0]["panels"][0]["queries"][0]["expr"]

        # The expression must survive str.format() with no matching keys
        # If braces are NOT escaped, this raises KeyError('service="api"')
        result = expr.format(service_filter="", container_filter="", rate_interval="5m")
        # Verify the original brace content is preserved after formatting
        assert '{service="api"}' in result

    def test_template_placeholders_preserved(self):
        """Legitimate {service_filter} placeholders must NOT be double-escaped."""
        extracted = {
            "dashboard_title": "Template Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["http_requests_total"],
            "panels": [
                {
                    "title": "RPS",
                    "queries": ["rate(http_requests_total{{{service_filter}}}[5m])"],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        signals = []
        yaml_str = generate_archetype_yaml(extracted, signals)
        import yaml

        parsed = yaml.safe_load(yaml_str)
        expr = parsed["archetypes"][0]["panels"][0]["queries"][0]["expr"]
        # Template placeholder must still resolve as a PromQL label selector.
        result = expr.format(service_filter='job="api"', container_filter="", rate_interval="5m")
        assert '{job="api"}' in result
        assert '{{job="api"}}' not in result

    def test_rate_interval_placeholder_preserved(self):
        extracted = {
            "dashboard_title": "Interval Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["http_requests_total"],
            "panels": [
                {
                    "title": "RPS",
                    "queries": ["rate(http_requests_total[ {rate_interval} ])"],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        yaml_str = generate_archetype_yaml(extracted, [])
        import yaml

        parsed = yaml.safe_load(yaml_str)
        expr = parsed["archetypes"][0]["panels"][0]["queries"][0]["expr"]

        assert "[ 1m ]" in expr.format(service_filter="", container_filter="", rate_interval="1m")


class TestSignalFlowCompileCompatibility:

    def test_raw_signalfx_query_is_not_recompiled_as_promql(self):
        from dashforge.archetypes.engine import compile_archetype
        from dashforge.models.schemas import ArchetypeMatch, Intent

        archetype = InvestigationArchetype(
            id="sfx_cpu",
            name="SFX CPU",
            problem_types=["cpu"],
            panels=[
                PanelTemplate(
                    title="CPU",
                    queries=[
                        QueryTemplate(
                            expr="data('cpu.utilization').publish()",
                            datasource_type="signalfx",
                        )
                    ],
                )
            ],
        )
        intent = Intent(
            summary="cpu",
            domain="infra",
            services=["api"],
            signals=[],
            keywords=[],
            timerange="1h",
            problem_type="cpu",
            archetypes=[ArchetypeMatch(type="cpu", confidence=1.0)],
        )
        spec = compile_archetype(
            archetype,
            intent,
            [
                MetricEntry(
                    name="cpu.utilization",
                    datasource_uid="x",
                    datasource_name="SignalFx",
                    datasource_type="signalfx",
                    query_language="signalflow",
                )
            ],
            target_language="signalflow",
        )

        assert spec.panels[0].queries[0].expr == "data('cpu.utilization').publish()"

    def test_explicit_query_language_marks_raw_signalflow(self):
        from dashforge.archetypes.engine import compile_archetype
        from dashforge.models.schemas import ArchetypeMatch, Intent

        archetype = InvestigationArchetype(
            id="sfx_cpu_language",
            name="SFX CPU Language",
            problem_types=["cpu"],
            panels=[
                PanelTemplate(
                    title="CPU",
                    queries=[
                        QueryTemplate(
                            expr="data('cpu.utilization').publish()",
                            query_language="signalflow",
                        )
                    ],
                )
            ],
        )
        intent = Intent(
            summary="cpu",
            domain="infra",
            services=["api"],
            signals=[],
            keywords=[],
            timerange="1h",
            problem_type="cpu",
            archetypes=[ArchetypeMatch(type="cpu", confidence=1.0)],
        )
        spec = compile_archetype(
            archetype,
            intent,
            [
                MetricEntry(
                    name="cpu.utilization",
                    datasource_uid="x",
                    datasource_name="SignalFx",
                    datasource_type="signalfx",
                    query_language="signalflow",
                )
            ],
            target_language="signalflow",
        )

        assert spec.panels[0].queries[0].expr == "data('cpu.utilization').publish()"
        assert spec.panels[0].queries[0].datasource_type == "signalfx"


# ── Bug 5: Suffix-aware metric substitution ──────────────────────────────


class TestSuffixAwareMetricSubstitution:
    """_apply_metric_substitutions must not double-suffix when the base
    binding name is a prefix of a suffixed variant in the query template."""

    def _make_archetype(self, expr: str, binding_default: str) -> InvestigationArchetype:
        return InvestigationArchetype(
            id="test",
            name="Test",
            problem_types=["test"],
            signal_bindings={"request_latency": binding_default},
            panels=[
                PanelTemplate(
                    title="P1",
                    queries=[QueryTemplate(expr=expr)],
                ),
            ],
        )

    def test_base_metric_replaced(self):
        """Simple base metric replacement still works."""
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(http_request_duration_seconds[5m])",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds",
            },
        )
        assert result.panels[0].queries[0].expr == "rate(custom_request_duration_seconds[5m])"

    def test_suffixed_variant_no_double_suffix(self):
        """Replacing base metric when the template uses _bucket suffix.
        The resolved metric is the base form, so the suffix should survive."""
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds",
            },
        )
        assert "custom_request_duration_seconds_bucket" in result.panels[0].queries[0].expr
        assert "_bucket_bucket" not in result.panels[0].queries[0].expr

    def test_resolved_metric_already_suffixed(self):
        """When the catalog match is already a suffixed form (e.g. _bucket),
        replacing the base binding should NOT produce double suffix."""
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
            binding_default="http_request_duration_seconds",
        )
        # The substitution map says base → already-suffixed resolved metric
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds_bucket",
            },
        )
        # Must NOT become custom_request_duration_seconds_bucket_bucket
        assert "_bucket_bucket" not in result.panels[0].queries[0].expr
        # The _bucket variant should appear exactly once
        assert "custom_request_duration_seconds_bucket" in result.panels[0].queries[0].expr

    def test_multiple_suffixes_in_one_expression(self):
        """An expression referencing both _bucket and _count of the same base."""
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr=(
                "histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) "
                "/ rate(http_request_duration_seconds_count[5m])"
            ),
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_latency",
            },
        )
        expr = result.panels[0].queries[0].expr
        assert "custom_latency_bucket" in expr
        assert "custom_latency_count" in expr
        assert "_bucket_bucket" not in expr
        assert "_count_count" not in expr

    def test_replacement_not_reprocessed_when_new_metric_contains_old_metric(self):
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(request_duration_seconds_bucket[5m])",
            binding_default="request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "request_duration_seconds": "custom_request_duration_seconds",
            },
        )

        assert result.panels[0].queries[0].expr == "rate(custom_request_duration_seconds_bucket[5m])"

    def test_replacement_obeys_metric_token_boundaries(self):
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(foo_request_duration_seconds[5m]) + rate(request_duration_seconds[5m])",
            binding_default="request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "request_duration_seconds": "custom_request_duration_seconds",
            },
        )
        expr = result.panels[0].queries[0].expr

        assert "foo_request_duration_seconds" in expr
        assert "rate(custom_request_duration_seconds[5m])" in expr
        assert "foo_custom_request_duration_seconds" not in expr

    def test_already_suffixed_metric_rebases_other_suffixes(self):
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(http_request_duration_seconds_count[5m])",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds_bucket",
            },
        )

        assert result.panels[0].queries[0].expr == "rate(custom_request_duration_seconds_count[5m])"

    def test_same_base_resolved_to_suffixed_form(self):
        """Bug 6: When the resolved metric shares the same base as old_metric
        and already ends with a suffix, the bare fallback must not re-replace
        inside the already-substituted suffixed name.

        Binding: http_request_duration_seconds -> http_request_duration_seconds_bucket
        Template: ...http_request_duration_seconds_bucket...
        Expected: no change (already correct), NOT _bucket_bucket.
        """
        from dashforge.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "http_request_duration_seconds_bucket",
            },
        )
        expr = result.panels[0].queries[0].expr
        assert "_bucket_bucket" not in expr
        assert "http_request_duration_seconds_bucket" in expr


# ── Bug 7: PromQL metric extraction regex coverage ──────────────────────


class TestPromQLExtractionBug7:
    """The regex must capture metrics in positions not followed by { or [,
    e.g. inside avg(metric), metric == 0, metric / metric."""

    def test_metric_inside_function_no_braces(self):
        metrics = extract_metrics_from_promql("avg(go_goroutines)")
        assert "go_goroutines" in metrics

    def test_bare_metric_with_comparison(self):
        metrics = extract_metrics_from_promql("up == 0")
        assert "up" in metrics

    def test_metric_in_binary_expression(self):
        metrics = extract_metrics_from_promql("node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes")
        assert "node_memory_MemAvailable_bytes" in metrics
        assert "node_memory_MemTotal_bytes" in metrics

    def test_metric_followed_by_closing_paren(self):
        metrics = extract_metrics_from_promql("count(some_metric)")
        assert "some_metric" in metrics

    def test_metric_at_end_of_line(self):
        metrics = extract_metrics_from_promql("process_resident_memory_bytes")
        assert "process_resident_memory_bytes" in metrics


# ── Bug 9: teach upsert must preserve global context fields ─────────────


class TestTeachUpsertContext:
    """Mappings keep global scope unless an existing scoped mapping is updated."""

    def test_upsert_preserves_global_context_services(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            source_type="bootstrap",
        )
        # Re-teach with a service scope
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            context_services=["checkout"],
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["context_services"] == []

    def test_upsert_unions_existing_scoped_context_services(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            context_services=["checkout"],
            source_type="teach",
        )
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            context_services=["payments"],
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert set(mappings[0]["context_services"]) == {"checkout", "payments"}

    def test_upsert_updates_source_type(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "latency_metric",
            confidence=0.8,
            source_type="bootstrap",
        )
        signal_store.add_mapping(
            "request_latency",
            "latency_metric",
            confidence=0.8,
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["source_type"] == "teach"

    def test_bootstrap_reload_preserves_learned_provenance(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "http_requests_total",
            confidence=0.8,
            context_services=["checkout"],
            source_type="dashboard_ingest",
            source_refs=["grafana:checkout-dash"],
        )
        signal_store.add_mapping(
            "request_latency",
            "http_requests_total",
            confidence=0.9,
            source_type="bootstrap",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["source_type"] == "dashboard_ingest"
        assert mappings[0]["source_refs"] == ["grafana:checkout-dash"]
        assert mappings[0]["context_services"] == ["checkout"]


# ── Bug 10: pending ingestion must store full signal records ─────────────


class TestPendingIngestionSignalRecords:
    """signals_inferred stored in ingested_dashboards should include the
    metric and confidence from infer_signals_from_metrics(), not just
    the signal type name."""

    def test_signals_inferred_includes_metric_and_confidence(self, signal_store):
        signal_store.record_ingested_dashboard(
            dashboard_uid="test-dash",
            dashboard_title="Test",
            signals_inferred=[
                {"signal_type": "request_latency", "metric": "http_request_duration_seconds", "confidence": 0.95},
                {"signal_type": "error_rate", "metric": "http_requests_total", "confidence": 0.8},
            ],
            status="pending",
        )

        ingested = signal_store.get_ingested_dashboard("test-dash")
        assert ingested is not None
        sigs = ingested["signals_inferred"]
        assert len(sigs) == 2
        assert sigs[0]["metric"] == "http_request_duration_seconds"
        assert sigs[0]["confidence"] == 0.95


# ── Bug 11: SignalFlow queries should not be written as PromQL templates ─


class TestSignalFlowArchetypeGeneration:
    """When the ingested dashboard is from SignalFx, generate_archetype_yaml
    should tag query templates with a datasource_type so compile_archetype
    knows not to convert them through _promql_template_to_signalflow."""

    def test_signalflow_query_preserved_in_archetype(self):
        extracted = {
            "dashboard_title": "SignalFx Dash",
            "dashboard_tags": [],
            "metrics_found": ["cpu.utilization"],
            "query_language": "signalflow",
            "panels": [
                {
                    "title": "CPU",
                    "queries": ["data('cpu.utilization').publish()"],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        signals = []
        import yaml

        yaml_str = generate_archetype_yaml(extracted, signals)
        parsed = yaml.safe_load(yaml_str)
        query = parsed["archetypes"][0]["panels"][0]["queries"][0]
        # Must indicate this is already SignalFlow, not PromQL
        assert query.get("datasource_type") == "signalfx"
        assert query.get("query_language") == "signalflow"
        # Expression must be preserved as-is (no brace escaping
        # that would break SignalFlow syntax)
        assert "data('cpu.utilization').publish()" in query["expr"]


class TestLearningTabRendering:

    def _learning_load_section(self) -> str:
        html = (Path(__file__).parent.parent.parent / "dashforge" / "static" / "index.html").read_text()
        return html.split("async function loadIngestedDashboards()", 1)[1].split("async function approveDashboard", 1)[
            0
        ]

    def test_ingested_dashboard_signal_chips_render_fields_not_object_repr(self):
        load_section = self._learning_load_section()
        assert "d.signals_inferred" in load_section
        assert "s.signal_type" in load_section
        assert "s.metric" in load_section
        assert "s.confidence" in load_section

    def test_ingested_dashboard_list_renders_persisted_archetype_yaml(self):
        load_section = self._learning_load_section()
        assert "d.archetype_generated" in load_section
        assert "Generated archetype YAML" in load_section

    def test_ingested_dashboard_approval_uses_data_attributes_not_inline_js(self):
        html = (Path(__file__).parent.parent.parent / "dashforge" / "static" / "index.html").read_text()
        load_section = self._learning_load_section()
        assert 'onclick="approveDashboard' not in load_section
        assert "data-dashboard-uid" in load_section
        assert "data-dashboard-backend" in load_section
        assert "encodeURIComponent(uid)" in html
