"""Regression tests for the second batch of dashboard-ingestion review fixes.

Covers:
  #1 SignalFlow conversion of dotted/colon metric names
  #2/#3 per-language panel capture (CloudWatch structured, Loki, SignalFx)
  #4 signals.yaml bootstrap discovery
  #5 union/clear semantics for context scopes on re-teach
  #6 register_signal_type preserving metadata
  #7 restricting signal substitution to the target backend's query language

Run with the project's 3.12 toolchain: `pytest tests/test_round2_fixes.py`.
"""

from __future__ import annotations

import pytest
import yaml as _yaml

from dashforge.archetypes.engine import _promql_template_to_signalflow, compile_archetype
from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.archetypes.templates import _load_archetypes_from_yaml
from dashforge.dashboard_ingest import generate_archetype_yaml, parse_dashboard_json
from dashforge.models.schemas import Intent, MetricEntry
from dashforge.signals import SignalStore


@pytest.fixture
def signal_store(tmp_path):
    return SignalStore(db_path=tmp_path / "test_signals.db")


def _metric(name: str, language: str, ds_type: str) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid=f"uid-{name}",
        datasource_name=f"ds-{name}",
        datasource_type=ds_type,
        query_language=language,
    )


# ── #1 SignalFlow conversion with dotted metric names ────────────────────────


class TestDottedSignalFlow:
    def test_rate_with_dotted_metric(self):
        out = _promql_template_to_signalflow("rate(http.server.duration{}[5m])", "", "", "lat")
        assert "data('http.server.duration'" in out
        # Must NOT fall through to the literal fallback wrapping the whole expr.
        assert "data('rate(" not in out

    def test_simple_dotted_metric(self):
        out = _promql_template_to_signalflow("http.server.duration{}", "", "", "lat")
        assert out.startswith("data('http.server.duration'")

    def test_histogram_quantile_dotted_bucket(self):
        out = _promql_template_to_signalflow(
            "histogram_quantile(0.95, sum(rate(http.server.duration_bucket{}[5m])) by (le))",
            "",
            "",
            "p95",
        )
        assert "data('http.server.duration'" in out
        assert ".percentile(pct=95" in out


# ── #2/#3 per-language panel capture ─────────────────────────────────────────


class TestPerLanguageCapture:
    def _archetype(self, dashboard):
        parsed = parse_dashboard_json(dashboard)
        return _yaml.safe_load(generate_archetype_yaml(parsed, []))["archetypes"][0]

    def test_cloudwatch_panel_preserved_as_loadable_template(self, tmp_path):
        dash = {
            "dashboard": {
                "uid": "cw",
                "title": "CW",
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "ELB 5xx",
                        "datasource": {"type": "cloudwatch"},
                        "targets": [
                            {
                                "metricName": "HTTPCode_ELB_5XX",
                                "namespace": "AWS/ELB",
                                "statistic": "Sum",
                                "region": "us-east-1",
                                "dimensions": {"LoadBalancer": "app/x"},
                            }
                        ],
                    }
                ],
            }
        }
        parsed = parse_dashboard_json(dash)
        yaml_str = generate_archetype_yaml(parsed, [])
        arch = _yaml.safe_load(yaml_str)["archetypes"][0]
        assert len(arch["panels"]) == 1  # not dropped
        q = arch["panels"][0]["queries"][0]
        assert q["query_language"] == "cloudwatch"
        assert q["expr"] == "HTTPCode_ELB_5XX"
        assert q["cloudwatch_namespace"] == "AWS/ELB"
        assert q["cloudwatch_stat"] == "Sum"
        assert q["cloudwatch_region"] == "us-east-1"
        assert q["cloudwatch_dimensions"] == {"LoadBalancer": "app/x"}

        path = tmp_path / "archetypes.yaml"
        path.write_text(yaml_str)
        loaded = _load_archetypes_from_yaml(path)
        query = loaded[0].panels[0].queries[0]
        assert query.expr == "HTTPCode_ELB_5XX"
        assert query.cloudwatch_namespace == "AWS/ELB"

    def test_loki_panel_preserved_raw(self):
        dash = {
            "dashboard": {
                "uid": "lk",
                "title": "Logs",
                "panels": [
                    {
                        "type": "logs",
                        "title": "Errors",
                        "datasource": {"type": "loki"},
                        "targets": [{"expr": '{app="api"} |= "error"'}],
                    }
                ],
            }
        }
        q = self._archetype(dash)["panels"][0]["queries"][0]
        assert q["query_language"] == "logql"
        assert q["datasource_type"] == "loki"
        # Preserved verbatim — no PromQL brace-escaping.
        assert q["expr"] == '{app="api"} |= "error"'

    def test_grafana_signalfx_panel_preserved(self):
        dash = {
            "dashboard": {
                "uid": "sx",
                "title": "CPU",
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "CPU",
                        "datasource": {"type": "grafana-signalfx-datasource"},
                        "targets": [{"query": "data('cpu.utilization').publish()"}],
                    }
                ],
            }
        }
        q = self._archetype(dash)["panels"][0]["queries"][0]
        assert q["query_language"] == "signalflow"
        assert q["datasource_type"] == "signalfx"
        assert q["expr"] == "data('cpu.utilization').publish()"

    def test_promql_panel_still_escaped(self):
        dash = {
            "dashboard": {
                "uid": "pm",
                "title": "Prom",
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Rate",
                        "datasource": {"type": "prometheus"},
                        "targets": [{"expr": 'rate(http_requests_total{job="api"}[5m])'}],
                    }
                ],
            }
        }
        q = self._archetype(dash)["panels"][0]["queries"][0]
        assert q["query_language"] == "promql"
        # PromQL label selectors are brace-escaped for safe str.format at compile.
        assert "{{job=" in q["expr"]

    def test_generated_non_promql_tags_survive_activation_and_compile(self):
        dash = {
            "dashboard": {
                "uid": "lk",
                "title": "Logs",
                "panels": [
                    {
                        "type": "logs",
                        "title": "Errors",
                        "datasource": {"type": "loki"},
                        "targets": [{"expr": '{app="api"} |= "error"'}],
                    }
                ],
            }
        }
        arch_data = self._archetype(dash)
        arch = InvestigationArchetype(
            **{
                **arch_data,
                "panels": [
                    PanelTemplate(
                        **{
                            **p,
                            "queries": [QueryTemplate(**q) for q in p["queries"]],
                        }
                    )
                    for p in arch_data["panels"]
                ],
            }
        )
        spec = compile_archetype(arch, Intent(summary="logs", domain="app"), [], target_language="promql")
        query = spec.panels[0].queries[0]
        assert query.expr == '{app="api"} |= "error"'
        assert query.datasource_type == "loki"

    def test_compile_uses_datasource_uid_matching_native_query_type(self):
        arch = InvestigationArchetype(
            id="mixed",
            name="Mixed",
            problem_types=["mixed"],
            panels=[
                PanelTemplate(
                    title="Logs",
                    queries=[QueryTemplate(expr='{app="api"}', query_language="logql", datasource_type="loki")],
                ),
                PanelTemplate(
                    title="CW",
                    queries=[
                        QueryTemplate(
                            expr="HTTPCode_ELB_5XX",
                            query_language="cloudwatch",
                            datasource_type="cloudwatch",
                            cloudwatch_namespace="AWS/ELB",
                            cloudwatch_stat="Sum",
                        )
                    ],
                ),
            ],
        )
        catalog = [
            _metric("http_requests_total", "promql", "prometheus"),
            _metric("loki:available_labels", "logql", "loki"),
            _metric("AWS/ELB/HTTPCode_ELB_5XX", "cloudwatch", "cloudwatch"),
        ]
        spec = compile_archetype(arch, Intent(summary="mixed", domain="app"), catalog)
        assert spec.panels[0].queries[0].datasource_uid == "uid-loki:available_labels"
        assert spec.panels[1].queries[0].datasource_uid == "uid-AWS/ELB/HTTPCode_ELB_5XX"
        assert spec.panels[1].queries[0].cloudwatch_namespace == "AWS/ELB"
        assert spec.panels[0].queries[0].query_language == "logql"
        assert spec.panels[1].queries[0].query_language == "cloudwatch"

    def test_missing_native_datasource_does_not_inherit_first_catalog_target(self):
        arch = InvestigationArchetype(
            id="missing_loki",
            name="Missing Loki",
            problem_types=["logs"],
            panels=[
                PanelTemplate(
                    title="Logs",
                    queries=[QueryTemplate(expr='{app="api"}', query_language="logql", datasource_type="loki")],
                )
            ],
        )
        catalog = [_metric("http_requests_total", "promql", "prometheus")]
        spec = compile_archetype(arch, Intent(summary="logs", domain="app"), catalog)
        query = spec.panels[0].queries[0]
        assert query.datasource_type == "loki"
        assert query.query_language == "logql"
        assert query.datasource_uid == ""


# ── #4 signals.yaml discovery ────────────────────────────────────────────────


class TestBootstrapDiscovery:
    def test_default_load_finds_taxonomy(self, signal_store):
        # The source-checkout candidate (project root) must resolve.
        assert signal_store.load_from_yaml() > 0

    def test_omitted_datasource_types_do_not_widen_existing_mapping(self, signal_store, tmp_path):
        signal_store.add_mapping(
            "request_rate",
            "http_requests_total",
            confidence=0.8,
            context_datasource_types=["prometheus"],
            source_type="teach",
        )
        path = tmp_path / "signals.yaml"
        path.write_text("""
signals:
  request_rate:
    description: Request rate
    metric_patterns:
      - pattern: http_requests_total
        confidence: 0.9
""")
        signal_store.load_from_yaml(path)
        mappings = signal_store.get_mappings_for_signal("request_rate", include_decayed=True)
        assert mappings[0]["context_datasource_types"] == ["prometheus"]


# ── #5 union / clear context scopes ──────────────────────────────────────────


class TestContextScopeMerge:
    def _scopes(self, store, sig="s", metric="m"):
        m = store.get_mappings_for_signal(sig, include_decayed=True)
        return m[0]["context_services"]

    def test_second_service_unions(self, signal_store):
        signal_store.add_mapping("s", "m", confidence=0.8, context_services=["checkout"], source_type="teach")
        signal_store.add_mapping("s", "m", confidence=0.8, context_services=["payments"], source_type="teach")
        assert set(self._scopes(signal_store)) == {"checkout", "payments"}

    def test_omitted_services_left_unchanged(self, signal_store):
        signal_store.add_mapping("s", "m", confidence=0.8, context_services=["checkout"], source_type="teach")
        signal_store.add_mapping("s", "m", confidence=0.8, context_services=None, source_type="teach")
        assert self._scopes(signal_store) == ["checkout"]

    def test_empty_list_clears_scope(self, signal_store):
        signal_store.add_mapping("s", "m", confidence=0.8, context_services=["checkout"], source_type="teach")
        signal_store.add_mapping("s", "m", confidence=0.8, context_services=[], source_type="teach")
        assert self._scopes(signal_store) == []


# ── #6 metadata preservation ─────────────────────────────────────────────────


class TestRegisterMetadata:
    def test_blank_teach_does_not_wipe_metadata(self, signal_store):
        signal_store.register_signal_type("request_latency", description="Latency", category="latency", unit="ms")
        signal_store.register_signal_type("request_latency")  # teach with empties
        st = signal_store.get_signal_type("request_latency")
        assert st["description"] == "Latency"
        assert st["category"] == "latency"
        assert st["unit"] == "ms"

    def test_nonempty_values_still_update(self, signal_store):
        signal_store.register_signal_type("s", description="a", category="x")
        signal_store.register_signal_type("s", description="b")
        st = signal_store.get_signal_type("s")
        assert st["description"] == "b"
        assert st["category"] == "x"  # untouched


# ── #7 restrict substitution to target backend ───────────────────────────────


class TestBackendScopedResolution:
    def test_resolve_signal_filters_by_language(self, signal_store):
        signal_store.add_mapping("lat", "latency", confidence=0.9, source_type="teach")
        catalog = [
            _metric("service.latency", "signalflow", "signalfx"),
            _metric("service_latency_seconds", "promql", "prometheus"),
        ]
        names = [e.name for e, _ in signal_store.resolve_signal("lat", catalog, target_query_language="promql")]
        assert "service_latency_seconds" in names
        assert "service.latency" not in names

    def test_archetype_resolution_skips_wrong_backend(self, signal_store):
        signal_store.add_mapping("lat", "latency", confidence=0.9, source_type="teach")
        catalog = [_metric("service.latency", "signalflow", "signalfx")]
        subs = signal_store.resolve_signals_for_archetype(
            {"lat": "missing_default_metric"}, catalog, target_query_language="promql"
        )
        assert subs == {}

    def test_signalflow_compile_passes_datasource_context(self, signal_store, monkeypatch):
        monkeypatch.setattr("dashforge.signals._store", signal_store)
        signal_store.add_mapping(
            "lat",
            "service.latency",
            confidence=0.9,
            context_datasource_types=["prometheus"],
            source_type="teach",
        )
        arch = InvestigationArchetype(
            id="latency",
            name="Latency",
            problem_types=["latency"],
            signal_bindings={"lat": "missing_default_metric"},
            panels=[
                PanelTemplate(
                    title="Latency",
                    queries=[QueryTemplate(expr="missing_default_metric")],
                )
            ],
        )
        catalog = [_metric("service.latency", "signalflow", "signalfx")]
        spec = compile_archetype(
            arch,
            Intent(summary="latency", domain="app", services=["api"]),
            catalog,
            target_language="signalflow",
        )
        assert "service.latency" not in spec.panels[0].queries[0].expr
        assert "missing_default_metric" in spec.panels[0].queries[0].expr
