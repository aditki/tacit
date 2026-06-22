"""Unit tests for SignalFx integration: engine SignalFlow compilation,
publisher PromQL detection/translation, chart JSON builder, config routing."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dashforge.models.schemas import (
    ArchetypeMatch,
    DashboardSpec,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _make_intent(**overrides) -> Intent:
    defaults = dict(
        summary="5xx errors on checkout-service",
        domain="web",
        services=["checkout-service"],
        signals=[SignalType.METRICS],
        keywords=["error", "5xx", "http"],
        timerange="1h",
        problem_type="error_spike",
        archetypes=[ArchetypeMatch(type="error_spike", confidence=0.95)],
    )
    defaults.update(overrides)
    return Intent(**defaults)


def _make_catalog(
    names: list[str] | None = None,
    dims: list[str] | None = None,
    ds_type: str = "prometheus",
    query_lang: str = "promql",
) -> list[MetricEntry]:
    names = names or ["http_requests_total"]
    dims = dims or ["service={checkout-service}"]
    return [
        MetricEntry(
            name=n,
            datasource_uid="ds1",
            datasource_name="prom",
            datasource_type=ds_type,
            query_language=query_lang,
            dimensions=dims,
        )
        for n in names
    ]


# ═══════════════════════════════════════════════════════════════════════════
# 1. Engine — SignalFlow filter resolvers
# ═══════════════════════════════════════════════════════════════════════════


def test_find_best_label_service():
    from dashforge.archetypes.engine import _find_best_label

    intent = _make_intent()
    catalog = _make_catalog(dims=["service={checkout-service,api-gateway}"])
    result = _find_best_label(intent, catalog)
    assert result is not None
    assert result == ("service", "checkout-service")
    print("[PASS] test_find_best_label_service")


def test_find_best_label_container_restrict():
    from dashforge.archetypes.engine import _find_best_label

    intent = _make_intent()
    catalog = _make_catalog(dims=["container={checkout-service}", "service={checkout-service}"])
    result = _find_best_label(intent, catalog, restrict_to={"container", "pod"})
    assert result is not None
    assert result[0] == "container"
    print("[PASS] test_find_best_label_container_restrict")


def test_find_best_label_no_services():
    from dashforge.archetypes.engine import _find_best_label

    intent = _make_intent(services=[])
    catalog = _make_catalog()
    result = _find_best_label(intent, catalog)
    assert result is None
    print("[PASS] test_find_best_label_no_services")


def test_resolve_sfx_service_filter():
    from dashforge.archetypes.engine import _resolve_sfx_service_filter

    intent = _make_intent()
    catalog = _make_catalog(dims=["service={checkout-service}"])
    filt = _resolve_sfx_service_filter(intent, catalog)
    assert filt == "filter('service', 'checkout-service')"
    print("[PASS] test_resolve_sfx_service_filter")


def test_resolve_sfx_service_filter_fallback():
    from dashforge.archetypes.engine import _resolve_sfx_service_filter

    intent = _make_intent()
    catalog = _make_catalog(dims=[])  # no dimensions to match
    filt = _resolve_sfx_service_filter(intent, catalog)
    assert "checkout-service" in filt
    assert filt.startswith("filter('service',")
    print("[PASS] test_resolve_sfx_service_filter_fallback")


def test_resolve_sfx_container_filter():
    from dashforge.archetypes.engine import _resolve_sfx_container_filter

    intent = _make_intent()
    catalog = _make_catalog(dims=["container={checkout-service}"])
    filt = _resolve_sfx_container_filter(intent, catalog)
    assert filt == "filter('container', 'checkout-service')"
    print("[PASS] test_resolve_sfx_container_filter")


# ═══════════════════════════════════════════════════════════════════════════
# 2. Engine — PromQL template → SignalFlow compilation
# ═══════════════════════════════════════════════════════════════════════════


def test_signalflow_simple_metric():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    result = _promql_template_to_signalflow(f"http_requests_total{{{sf}}}", sf, "", "rate")
    assert "data('http_requests_total'" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_simple_metric")


def test_signalflow_rate():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    result = _promql_template_to_signalflow(f"rate(http_requests_total{{{sf}}}[5m])", sf, "", "rps")
    assert "data('http_requests_total'" in result
    assert "rollup='rate'" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_rate")


def test_signalflow_sum_rate():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    result = _promql_template_to_signalflow(f"sum(rate(http_requests_total{{{sf}}}[5m]))", sf, "", "total")
    assert "data('http_requests_total'" in result
    assert "rollup='rate'" in result
    assert ".sum()" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_sum_rate")


def test_signalflow_sum_rate_by():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    expr = f'sum(rate(http_requests_total{{{sf}, status=~"5.."}}[5m])) by (status)'
    result = _promql_template_to_signalflow(expr, sf, "", "by_status")
    assert ".sum(by=" in result
    assert "'status'" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_sum_rate_by")


def test_signalflow_histogram_quantile():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    expr = f"histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{{sf}}}[5m])) by (le))"
    result = _promql_template_to_signalflow(expr, sf, "", "p95")
    assert "data('http_request_duration_seconds'" in result
    assert ".percentile(pct=95)" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_histogram_quantile")


def test_signalflow_ratio():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    expr = (
        f'sum(rate(http_requests_total{{{sf}, status=~"5.."}}[5m]))'
        f" / "
        f"sum(rate(http_requests_total{{{sf}}}[5m]))"
    )
    result = _promql_template_to_signalflow(expr, sf, "", "ratio")
    assert " / " in result
    assert ".publish(label='ratio')" in result
    print("[PASS] test_signalflow_ratio")


def test_signalflow_topk():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    expr = f"topk(5, sum(rate(http_requests_total{{{sf}}}[5m])) by (path))"
    result = _promql_template_to_signalflow(expr, sf, "", "top5")
    assert ".top(count=5)" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_topk")


def test_signalflow_increase():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    sf = "filter('service', 'checkout-service')"
    expr = f"increase(http_requests_total{{{sf}}}[5m])"
    result = _promql_template_to_signalflow(expr, sf, "", "inc")
    assert "rollup='delta'" in result
    assert ".publish(" in result
    print("[PASS] test_signalflow_increase")


def test_signalflow_bare_metric():
    from dashforge.archetypes.engine import _promql_template_to_signalflow

    result = _promql_template_to_signalflow("up", "", "", "heartbeat")
    assert result == "data('up').publish(label='heartbeat')"
    print("[PASS] test_signalflow_bare_metric")


# ═══════════════════════════════════════════════════════════════════════════
# 3. Engine — compile_archetype with target_language
# ═══════════════════════════════════════════════════════════════════════════


def test_compile_archetype_signalflow():
    from dashforge.archetypes.engine import compile_archetype
    from dashforge.archetypes.templates import get_archetype

    intent = _make_intent()
    catalog = _make_catalog(dims=["service={checkout-service}"])
    arch = get_archetype("error_spike")
    assert arch is not None, "error_spike archetype not found"

    spec = compile_archetype(arch, intent, catalog, target_language="signalflow")
    assert len(spec.panels) > 0

    for panel in spec.panels:
        for q in panel.queries:
            assert q.datasource_type == "signalfx", f"Expected signalfx, got {q.datasource_type}"
            assert "data(" in q.expr, f"Expected SignalFlow data() call, got: {q.expr[:80]}"
            assert ".publish(" in q.expr, f"Missing .publish() in: {q.expr[:80]}"
            # Should NOT have PromQL patterns
            assert "rate(" not in q.expr, f"PromQL rate() found in SignalFlow: {q.expr[:80]}"
            assert "sum(" not in q.expr or ".sum(" in q.expr, f"PromQL sum() found: {q.expr[:80]}"
    print("[PASS] test_compile_archetype_signalflow")


def test_compile_archetype_promql():
    from dashforge.archetypes.engine import compile_archetype
    from dashforge.archetypes.templates import get_archetype

    intent = _make_intent()
    catalog = _make_catalog(dims=["service={checkout-service}"])
    arch = get_archetype("error_spike")
    assert arch is not None

    spec = compile_archetype(arch, intent, catalog, target_language="promql")
    assert len(spec.panels) > 0

    for panel in spec.panels:
        for q in panel.queries:
            assert q.datasource_type == "prometheus"
            # Should have PromQL patterns, not SignalFlow
            assert "data(" not in q.expr, f"SignalFlow data() found in PromQL: {q.expr[:80]}"
    print("[PASS] test_compile_archetype_promql")


def test_blend_archetypes_signalflow():
    from dashforge.archetypes.engine import blend_archetypes
    from dashforge.archetypes.templates import get_archetype

    intent = _make_intent(
        archetypes=[
            ArchetypeMatch(type="error_spike", confidence=0.9),
            ArchetypeMatch(type="latency_investigation", confidence=0.6),
        ]
    )
    catalog = _make_catalog(dims=["service={checkout-service}"])
    arch1 = get_archetype("error_spike")
    arch2 = get_archetype("latency_investigation")
    if not arch1 or not arch2:
        print("[SKIP] test_blend_archetypes_signalflow — archetypes not found")
        return

    spec = blend_archetypes(
        [(arch1, 0.9), (arch2, 0.6)],
        intent,
        catalog,
        target_language="signalflow",
    )
    assert len(spec.panels) > 0
    for panel in spec.panels:
        for q in panel.queries:
            assert q.datasource_type == "signalfx"
            assert "data(" in q.expr
    print("[PASS] test_blend_archetypes_signalflow")


# ═══════════════════════════════════════════════════════════════════════════
# 4. Publisher — PromQL detection
# ═══════════════════════════════════════════════════════════════════════════


def test_is_promql_true():
    from dashforge.signalfx.publisher import _is_promql

    assert _is_promql('rate(http_requests_total{service="web"}[5m])') is True
    assert _is_promql('sum(rate(x{a="b"}[5m]))') is True
    assert _is_promql('http_requests_total{service="web"}') is True
    assert _is_promql('histogram_quantile(0.99, sum(rate(x_bucket{a="b"}[5m])) by (le))') is True
    assert _is_promql('increase(errors_total{app="x"}[1h])') is True
    print("[PASS] test_is_promql_true")


def test_is_promql_false():
    from dashforge.signalfx.publisher import _is_promql

    assert _is_promql("data('cpu.utilization').mean().publish()") is False
    assert _is_promql("data('http_requests', filter=filter('service', 'web')).sum().publish(label='A')") is False
    print("[PASS] test_is_promql_false")


# ═══════════════════════════════════════════════════════════════════════════
# 5. Publisher — PromQL → SignalFlow fallback translator
# ═══════════════════════════════════════════════════════════════════════════


def test_publisher_promql_to_signalflow_simple():
    from dashforge.signalfx.publisher import _promql_to_signalflow

    result = _promql_to_signalflow('http_requests_total{service="web"}', "A")
    assert "data('http_requests_total'" in result
    assert "filter('service', 'web')" in result
    assert ".publish(" in result
    print("[PASS] test_publisher_promql_to_signalflow_simple")


def test_publisher_promql_to_signalflow_rate():
    from dashforge.signalfx.publisher import _promql_to_signalflow

    result = _promql_to_signalflow('rate(http_requests_total{service="web"}[5m])', "B")
    assert "data('http_requests_total'" in result
    assert "rollup='rate'" in result
    assert ".publish(" in result
    print("[PASS] test_publisher_promql_to_signalflow_rate")


def test_publisher_promql_to_signalflow_histogram():
    from dashforge.signalfx.publisher import _promql_to_signalflow

    expr = 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{service="web"}[5m])) by (le))'
    result = _promql_to_signalflow(expr, "p95")
    assert ".percentile(pct=95)" in result
    assert "data('http_request_duration_seconds'" in result
    print("[PASS] test_publisher_promql_to_signalflow_histogram")


def test_publisher_promql_to_signalflow_ratio():
    from dashforge.signalfx.publisher import _promql_to_signalflow

    # No-space label format (matches the ratio regex)
    expr = 'sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total{service="web"}[5m]))'
    result = _promql_to_signalflow(expr, "ratio")
    assert ".publish(" in result
    # The publisher translator should produce something valid
    assert "data(" in result
    print("[PASS] test_publisher_promql_to_signalflow_ratio")


def test_publisher_promql_to_signalflow_bare():
    from dashforge.signalfx.publisher import _promql_to_signalflow

    result = _promql_to_signalflow("up", "heartbeat")
    assert result == "data('up').publish(label='heartbeat')"
    print("[PASS] test_publisher_promql_to_signalflow_bare")


# ═══════════════════════════════════════════════════════════════════════════
# 6. Publisher — _build_chart_json
# ═══════════════════════════════════════════════════════════════════════════


def test_build_chart_json_signalflow():
    from dashforge.signalfx.publisher import _build_chart_json

    panel = PanelSpec(
        title="Request Rate",
        panel_type="timeseries",
        queries=[
            PanelQuery(
                expr="data('http_requests_total', filter=filter('service', 'web'), "
                "rollup='rate').sum().publish(label='rps')",
                legend_format="rps",
                datasource_uid="signalfx-direct",
                datasource_type="signalfx",
            )
        ],
        unit="reqps",
    )
    chart = _build_chart_json(panel)
    assert chart["name"] == "Request Rate"
    assert chart["options"]["type"] == "TimeSeriesChart"
    assert "data('http_requests_total'" in chart["programText"]
    assert ".publish(" in chart["programText"]
    print("[PASS] test_build_chart_json_signalflow")


def test_build_chart_json_auto_publish():
    from dashforge.signalfx.publisher import _build_chart_json

    panel = PanelSpec(
        title="CPU",
        panel_type="timeseries",
        queries=[
            PanelQuery(
                expr="data('cpu.utilization').mean()",
                legend_format="cpu",
                datasource_uid="sfx",
                datasource_type="signalfx",
            )
        ],
    )
    chart = _build_chart_json(panel)
    assert ".publish(label='cpu')" in chart["programText"]
    print("[PASS] test_build_chart_json_auto_publish")


def test_build_chart_json_panel_types():
    from dashforge.signalfx.publisher import _build_chart_json

    for ptype, expected in [
        ("timeseries", "TimeSeriesChart"),
        ("stat", "SingleValue"),
        ("gauge", "SingleValue"),
        ("table", "List"),
        ("heatmap", "Heatmap"),
    ]:
        panel = PanelSpec(
            title="Test",
            panel_type=ptype,
            queries=[
                PanelQuery(
                    expr="data('x').publish(label='A')",
                    datasource_uid="sfx",
                    datasource_type="signalfx",
                )
            ],
        )
        chart = _build_chart_json(panel)
        assert chart["options"]["type"] == expected, f"{ptype} → {chart['options']['type']}, expected {expected}"
    print("[PASS] test_build_chart_json_panel_types")


def test_build_chart_json_multi_query():
    from dashforge.signalfx.publisher import _build_chart_json

    panel = PanelSpec(
        title="Multi",
        panel_type="timeseries",
        queries=[
            PanelQuery(expr="data('a').publish(label='A')", datasource_uid="sfx", datasource_type="signalfx"),
            PanelQuery(expr="data('b').publish(label='B')", datasource_uid="sfx", datasource_type="signalfx"),
        ],
    )
    chart = _build_chart_json(panel)
    lines = chart["programText"].split("\n")
    assert len(lines) == 2
    assert "data('a')" in lines[0]
    assert "data('b')" in lines[1]
    print("[PASS] test_build_chart_json_multi_query")


# ═══════════════════════════════════════════════════════════════════════════
# 7. Publisher — _build_dashboard_json layout
# ═══════════════════════════════════════════════════════════════════════════


def test_build_dashboard_json_layout():
    from dashforge.signalfx.publisher import _build_dashboard_json

    spec = DashboardSpec(
        title="Test Dash",
        tags=["test"],
        timerange="1h",
        panels=[
            PanelSpec(
                title=f"P{i}",
                panel_type="timeseries",
                queries=[
                    PanelQuery(expr="data('x').publish(label='A')", datasource_uid="sfx", datasource_type="signalfx")
                ],
            )
            for i in range(4)
        ],
    )
    dash = _build_dashboard_json(spec, ["c1", "c2", "c3", "c4"], "grp1")
    assert dash["name"] == "Test Dash"
    assert dash["groupId"] == "grp1"
    assert len(dash["charts"]) == 4
    # 2-column layout: positions should be (0,0), (6,0), (0,2), (6,2)
    positions = [(c["column"], c["row"]) for c in dash["charts"]]
    assert positions[0] == (0, 0)
    assert positions[1] == (6, 0)
    assert positions[2] == (0, 2)
    assert positions[3] == (6, 2)
    print("[PASS] test_build_dashboard_json_layout")


# ═══════════════════════════════════════════════════════════════════════════
# 8. Config — grafana_enabled field
# ═══════════════════════════════════════════════════════════════════════════


def test_config_grafana_enabled():
    from dashforge.config import Settings

    s = Settings(grafana_enabled=True, signalfx_enabled=False)
    assert s.grafana_enabled is True
    assert s.signalfx_enabled is False
    print("[PASS] test_config_grafana_enabled")


def test_config_grafana_disabled():
    from dashforge.config import Settings

    s = Settings(grafana_enabled=False, signalfx_enabled=True, signalfx_api_token="test")
    assert s.grafana_enabled is False
    assert s.signalfx_enabled is True
    print("[PASS] test_config_grafana_disabled")


def test_config_sfx_backend_routing():
    """When grafana disabled + signalfx enabled, pipeline should route to signalflow."""
    from dashforge.config import Settings

    s = Settings(grafana_enabled=False, signalfx_enabled=True, signalfx_api_token="tok")
    sfx_backend = s.signalfx_enabled and s.signalfx_api_token and not s.grafana_enabled
    assert sfx_backend is True
    target_language = "signalflow" if sfx_backend else "promql"
    assert target_language == "signalflow"

    s2 = Settings(grafana_enabled=True, signalfx_enabled=True, signalfx_api_token="tok")
    sfx_backend2 = s2.signalfx_enabled and s2.signalfx_api_token and not s2.grafana_enabled
    assert not sfx_backend2
    assert ("signalflow" if sfx_backend2 else "promql") == "promql"
    print("[PASS] test_config_sfx_backend_routing")


# ═══════════════════════════════════════════════════════════════════════════
# 9. Publisher — label parsing helpers
# ═══════════════════════════════════════════════════════════════════════════


def test_parse_labels():
    from dashforge.signalfx.publisher import _parse_labels

    labels = _parse_labels('service="web", status=~"5..", method!="OPTIONS"')
    assert len(labels) == 3
    assert labels[0] == ("service", "=", "web")
    assert labels[1] == ("status", "=~", "5..")
    assert labels[2] == ("method", "!=", "OPTIONS")
    print("[PASS] test_parse_labels")


def test_labels_to_filter():
    from dashforge.signalfx.publisher import _labels_to_filter

    # Simple regex (no [ or |) keeps the value
    labels = [("service", "=", "web"), ("status", "=~", "5..")]
    result = _labels_to_filter(labels)
    assert "filter('service', 'web')" in result
    assert "filter('status', '5..')" in result
    assert " and " in result

    # Regex with alternation gets wildcarded
    labels2 = [("status", "=~", "5..|4..")]
    result2 = _labels_to_filter(labels2)
    assert "filter('status', '*')" in result2

    # Regex with character class gets wildcarded
    labels3 = [("code", "=~", "[45]..")]
    result3 = _labels_to_filter(labels3)
    assert "filter('code', '*')" in result3
    print("[PASS] test_labels_to_filter")


def test_labels_to_filter_not_equal():
    from dashforge.signalfx.publisher import _labels_to_filter

    labels = [("method", "!=", "OPTIONS")]
    result = _labels_to_filter(labels)
    assert "not filter('method', 'OPTIONS')" in result
    print("[PASS] test_labels_to_filter_not_equal")


# ═══════════════════════════════════════════════════════════════════════════
# 10. Engine — _find_top_level_slash
# ═══════════════════════════════════════════════════════════════════════════


def test_find_top_level_slash():
    from dashforge.archetypes.engine import _find_top_level_slash

    # Simple ratio
    assert _find_top_level_slash("a / b") == 2
    # Nested parens should be skipped
    assert _find_top_level_slash("sum(a/b) / sum(c)") is not None
    pos = _find_top_level_slash("sum(a/b) / sum(c)")
    assert "sum(a/b)" == "sum(a/b) / sum(c)"[:pos].strip() or pos == 9
    # No slash
    assert _find_top_level_slash("sum(rate(x[5m]))") is None
    print("[PASS] test_find_top_level_slash")


# ═══════════════════════════════════════════════════════════════════════════
# 11. DashResponse model — signalfx fields
# ═══════════════════════════════════════════════════════════════════════════


def test_dash_response_signalfx_fields():
    from dashforge.models.schemas import DashResponse

    resp = DashResponse(
        dashboard_url="",
        dashboard_uid="sfx-dash-123",
        panel_count=5,
        summary="Created dashboard",
        signalfx_url="https://app.us1.signalfx.com/#/dashboard/sfx-dash-123",
        signalfx_dashboard_id="sfx-dash-123",
    )
    assert resp.signalfx_url is not None
    assert resp.signalfx_dashboard_id == "sfx-dash-123"
    assert resp.dashboard_uid == "sfx-dash-123"

    # Default: signalfx fields are empty/None
    resp2 = DashResponse(dashboard_url="http://g/d/abc", dashboard_uid="abc", panel_count=3, summary="ok")
    assert not resp2.signalfx_url  # empty or None
    assert not resp2.signalfx_dashboard_id  # empty or None
    print("[PASS] test_dash_response_signalfx_fields")


# ═══════════════════════════════════════════════════════════════════════════
# 12. SignalFx discovery — KEYWORD_METRIC_MAP sanity
# ═══════════════════════════════════════════════════════════════════════════


def test_keyword_metric_map():
    from dashforge.grafana.adapters.signalfx import KEYWORD_METRIC_MAP

    assert isinstance(KEYWORD_METRIC_MAP, dict)
    assert len(KEYWORD_METRIC_MAP) > 0
    # Every value should be a list of metric prefixes
    for kw, prefixes in KEYWORD_METRIC_MAP.items():
        assert isinstance(prefixes, list), f"Expected list for key '{kw}'"
        for p in prefixes:
            assert isinstance(p, str), f"Expected str prefix, got {type(p)}"
    # Spot-check common keywords
    assert "error" in KEYWORD_METRIC_MAP or "5xx" in KEYWORD_METRIC_MAP
    print("[PASS] test_keyword_metric_map")


# ═══════════════════════════════════════════════════════════════════════════
# 13. Validation — SignalFlow metric extraction & validation
# ═══════════════════════════════════════════════════════════════════════════


def test_extract_signalflow_metrics():
    from dashforge.validation import _extract_signalflow_metrics

    # Single data() call
    assert _extract_signalflow_metrics("data('cpu.utilization').mean().publish()") == ["cpu.utilization"]
    # Multiple data() calls
    result = _extract_signalflow_metrics(
        "data('http_requests', filter=filter('service', 'web'), rollup='rate').sum().publish() / "
        "data('http_requests', filter=filter('service', 'web')).sum().publish()"
    )
    assert result == ["http_requests", "http_requests"]
    # Double-quoted
    assert _extract_signalflow_metrics('data("memory.usage").publish()') == ["memory.usage"]
    # No data() call
    assert _extract_signalflow_metrics("some_random_expression") == []
    print("[PASS] test_extract_signalflow_metrics")


def test_validate_signalflow_drops_missing_panels():
    import asyncio
    from unittest.mock import AsyncMock

    from dashforge.validation import validate_signalflow_queries

    mock_client = AsyncMock()

    # cpu.utilization exists, nonexistent_metric does not
    async def fake_get_metric(name):
        if name == "cpu.utilization":
            return {"name": "cpu.utilization"}
        raise Exception("404 Not Found")

    mock_client.get_metric = AsyncMock(side_effect=fake_get_metric)

    spec = DashboardSpec(
        title="Test",
        timerange="1h",
        panels=[
            PanelSpec(
                title="CPU (exists)",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr="data('cpu.utilization').mean().publish(label='cpu')",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                    )
                ],
            ),
            PanelSpec(
                title="Fake (missing)",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr="data('nonexistent_metric').sum().publish(label='fake')",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                    )
                ],
            ),
        ],
    )
    result_spec, warnings = asyncio.run(validate_signalflow_queries(mock_client, spec))
    assert len(result_spec.panels) == 1
    assert result_spec.panels[0].title == "CPU (exists)"
    assert any("Fake (missing)" in w for w in warnings)
    print("[PASS] test_validate_signalflow_drops_missing_panels")


def test_validate_signalflow_keeps_all_when_valid():
    import asyncio
    from unittest.mock import AsyncMock

    from dashforge.validation import validate_signalflow_queries

    mock_client = AsyncMock()
    mock_client.get_metric = AsyncMock(return_value={"name": "ok"})

    spec = DashboardSpec(
        title="Test",
        timerange="1h",
        panels=[
            PanelSpec(
                title=f"P{i}",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr=f"data('metric_{i}').publish(label='A')",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                    )
                ],
            )
            for i in range(3)
        ],
    )
    result_spec, warnings = asyncio.run(validate_signalflow_queries(mock_client, spec))
    assert len(result_spec.panels) == 3
    assert warnings == []
    print("[PASS] test_validate_signalflow_keeps_all_when_valid")


def test_validate_signalflow_drops_missing_sibling_query():
    import asyncio
    from unittest.mock import AsyncMock

    from dashforge.validation import validate_signalflow_queries

    mock_client = AsyncMock()

    async def fake_get_metric(name):
        if name == "cpu.utilization":
            return {"name": "cpu.utilization"}
        raise Exception("404 Not Found")

    mock_client.get_metric = AsyncMock(side_effect=fake_get_metric)

    spec = DashboardSpec(
        title="Mixed SignalFx",
        timerange="1h",
        panels=[
            PanelSpec(
                title="Mixed",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr="data('cpu.utilization').mean().publish(label='cpu')",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                    ),
                    PanelQuery(
                        expr="data('missing.metric').mean().publish(label='missing')",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                    ),
                ],
            )
        ],
    )

    result_spec, warnings = asyncio.run(validate_signalflow_queries(mock_client, spec))

    assert len(result_spec.panels) == 1
    assert len(result_spec.panels[0].queries) == 1
    assert result_spec.panels[0].queries[0].expr == "data('cpu.utilization').mean().publish(label='cpu')"
    assert result_spec.panels[0].queries[0].validation_status == "exists"
    assert any("missing.metric" in warning for warning in warnings)
    print("[PASS] test_validate_signalflow_drops_missing_sibling_query")


def test_validate_signalflow_all_missing():
    import asyncio
    from unittest.mock import AsyncMock

    from dashforge.validation import validate_signalflow_queries

    mock_client = AsyncMock()
    mock_client.get_metric = AsyncMock(side_effect=Exception("404"))

    spec = DashboardSpec(
        title="Test",
        timerange="1h",
        panels=[
            PanelSpec(
                title="Bad Panel",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr="data('ghost_metric').publish(label='A')",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                    )
                ],
            ),
        ],
    )
    result_spec, warnings = asyncio.run(validate_signalflow_queries(mock_client, spec))
    assert len(result_spec.panels) == 0
    assert any("ALL panels" in w for w in warnings)
    print("[PASS] test_validate_signalflow_all_missing")


# ═══════════════════════════════════════════════════════════════════════════
# Runner
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # 1. Engine — filters
    test_find_best_label_service()
    test_find_best_label_container_restrict()
    test_find_best_label_no_services()
    test_resolve_sfx_service_filter()
    test_resolve_sfx_service_filter_fallback()
    test_resolve_sfx_container_filter()

    # 2. Engine — SignalFlow compilation
    test_signalflow_simple_metric()
    test_signalflow_rate()
    test_signalflow_sum_rate()
    test_signalflow_sum_rate_by()
    test_signalflow_histogram_quantile()
    test_signalflow_ratio()
    test_signalflow_topk()
    test_signalflow_increase()
    test_signalflow_bare_metric()

    # 3. Engine — compile_archetype
    test_compile_archetype_signalflow()
    test_compile_archetype_promql()
    test_blend_archetypes_signalflow()

    # 4. Publisher — PromQL detection
    test_is_promql_true()
    test_is_promql_false()

    # 5. Publisher — fallback translator
    test_publisher_promql_to_signalflow_simple()
    test_publisher_promql_to_signalflow_rate()
    test_publisher_promql_to_signalflow_histogram()
    test_publisher_promql_to_signalflow_ratio()
    test_publisher_promql_to_signalflow_bare()

    # 6. Publisher — chart JSON
    test_build_chart_json_signalflow()
    test_build_chart_json_auto_publish()
    test_build_chart_json_panel_types()
    test_build_chart_json_multi_query()

    # 7. Publisher — dashboard layout
    test_build_dashboard_json_layout()

    # 8. Config
    test_config_grafana_enabled()
    test_config_grafana_disabled()
    test_config_sfx_backend_routing()

    # 9. Publisher — label helpers
    test_parse_labels()
    test_labels_to_filter()
    test_labels_to_filter_not_equal()

    # 10. Engine — top-level slash
    test_find_top_level_slash()

    # 11. DashResponse
    test_dash_response_signalfx_fields()

    # 12. Keyword map
    test_keyword_metric_map()

    # 13. Validation
    test_extract_signalflow_metrics()
    test_validate_signalflow_drops_missing_panels()
    test_validate_signalflow_keeps_all_when_valid()
    test_validate_signalflow_drops_missing_sibling_query()
    test_validate_signalflow_all_missing()

    print("\n=== All SignalFx unit tests passed ===")
