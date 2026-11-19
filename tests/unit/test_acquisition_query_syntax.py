"""Native operands and ratio syntax retain the shared acquisition boundaries."""

from unittest.mock import AsyncMock

import pytest
import yaml

from tacit.archetypes.engine import (
    ArchetypeCoverageWorkLimitError,
    ArchetypeCoverageWorkLimits,
    UnsupportedSignalFlowTranslation,
    _find_top_level_slash,
    _promql_template_to_signalflow,
    compile_archetype,
)
from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.catalog import begin_selection_audit, end_selection_audit, recorded_selections
from tacit.config import Settings
from tacit.dashboard_ingest.archetype_generation import generate_archetype_yaml
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry, PanelQuery, PanelSpec
from tacit.signalfx.client import SignalFxClient
from tacit.signalfx.publisher import _promql_to_signalflow, publish_dashboard, validate_signalflow_publication


def _native_archetype(expr: str, language: str = "graphite") -> InvestigationArchetype:
    return InvestigationArchetype(
        id="native-operands",
        name="Native operands",
        problem_types=["resource_saturation"],
        panels=[
            PanelTemplate(
                title="CPU",
                queries=[QueryTemplate(expr=expr, query_language=language, datasource_type=language)],
            )
        ],
    )


def _intent() -> Intent:
    return Intent(summary="inspect CPU", domain="infrastructure", problem_type="resource_saturation")


def _metric(name: str, uid: str = "graphite-owner", language: str = "graphite") -> MetricEntry:
    return MetricEntry(
        name=name, datasource_uid=uid, datasource_name=uid, datasource_type=language, query_language=language
    )


@pytest.mark.parametrize(
    "expr,names",
    [
        ("servers.checkout.cpu", {"servers.checkout.cpu"}),
        ('alias(servers.checkout.cpu, "CPU")', {"servers.checkout.cpu"}),
        (
            'alias(scale(divideSeries(servers.checkout.errors, servers.checkout.requests), 100), "Rate / CPU")',
            {"servers.checkout.errors", "servers.checkout.requests"},
        ),
        (
            "sumSeries(servers.checkout.cpu, servers.checkout.system)",
            {"servers.checkout.cpu", "servers.checkout.system"},
        ),
    ],
)
def test_native_graphite_query_binds_complete_operands_without_rewriting(expr, names):
    catalog = [_metric(name) for name in names] + [_metric("CPU", "label-is-not-an-operand")]

    dashboard = compile_archetype(_native_archetype(expr), _intent(), catalog)

    assert len(dashboard.panels) == 1
    query = dashboard.panels[0].queries[0]
    assert query.expr == expr
    assert query.datasource_uid == "graphite-owner"


def test_learned_graphite_query_preserves_expression_and_binds_real_metric():
    expr = 'alias(servers.checkout.cpu, "CPU {unresolved}")'
    serialized = generate_archetype_yaml(
        {
            "dashboard_title": "Graphite CPU",
            "metrics_found": ["servers.checkout.cpu"],
            "query_language": "graphite",
            "panels": [{"title": "CPU", "queries": [expr], "datasource_type": "graphite"}],
        },
        [],
    )
    archetype = InvestigationArchetype.model_validate(yaml.safe_load(serialized)["archetypes"][0])

    dashboard = compile_archetype(archetype, _intent(), [_metric("servers.checkout.cpu")])

    assert dashboard.panels[0].queries[0].expr == expr
    assert dashboard.panels[0].queries[0].datasource_uid == "graphite-owner"


@pytest.mark.parametrize(
    "catalog,reason",
    [
        ([_metric("servers.checkout.errors")], "missing_operand"),
        ([_metric("servers.checkout.errors", "a"), _metric("servers.checkout.requests", "b")], "no_owner"),
        (
            [
                _metric("servers.checkout.errors", "a"),
                _metric("servers.checkout.requests", "a"),
                _metric("servers.checkout.errors", "b"),
                _metric("servers.checkout.requests", "b"),
            ],
            "multiple_owners",
        ),
    ],
)
def test_graphite_binding_retains_full_operand_and_unique_owner_requirements(catalog, reason):
    token = begin_selection_audit()
    try:
        dashboard = compile_archetype(
            _native_archetype("divideSeries(servers.checkout.errors, servers.checkout.requests)"),
            _intent(),
            catalog,
        )
        decisions = recorded_selections()
    finally:
        end_selection_audit(token)

    assert dashboard.panels == []
    assert len(decisions) == 1
    assert decisions[0]["required_metrics"] == ["servers.checkout.errors", "servers.checkout.requests"]
    assert decisions[0]["reason"] == reason


@pytest.mark.parametrize(
    "expr",
    [
        'unknownFunction(servers.checkout.cpu, "servers.other.cpu")',
        'alias("servers.checkout.cpu", "CPU")',
        'alias(servers.checkout.cpu, "unterminated)',
        'alias(servers.checkout.cpu, "CPU") trailing',
        "sumSeries(servers.*.cpu)",
        "sumSeries(servers.{checkout,payment}.cpu)",
        "scale(servers.checkout.cpu, servers.other.cpu)",
        "divideSeries(servers.checkout.cpu)",
        "alias(" * 33 + "servers.checkout.cpu" + ', "CPU")' * 33,
    ],
)
def test_unprovable_graphite_syntax_cannot_bind_via_catalog_expression_fallback(expr):
    dashboard = compile_archetype(
        _native_archetype(expr),
        _intent(),
        [_metric(expr), _metric("servers.checkout.cpu")],
    )

    assert dashboard.panels == []


def test_native_parser_does_not_run_before_existing_text_admission(monkeypatch):
    monkeypatch.setattr(
        "tacit.archetypes.engine._resolve_native_query_target",
        lambda *args, **kwargs: pytest.fail("owner selection ran before query text admission"),
    )
    with pytest.raises(ArchetypeCoverageWorkLimitError):
        compile_archetype(
            _native_archetype('alias(servers.checkout.cpu, "CPU")'),
            _intent(),
            [_metric("servers.checkout.cpu")],
            work_limits=ArchetypeCoverageWorkLimits(max_scalar_characters=16),
        )


@pytest.mark.parametrize(
    "expr",
    [
        'http_requests_total{path="/checkout"}',
        'http_requests_total{path="/checkout/(confirm)"}',
        'http_requests_total{path="/checkout/{confirm}"}',
    ],
)
def test_quoted_selector_slashes_translate_in_compiler_and_publisher(expr):
    expected = expr.split('path="', 1)[1].rsplit('"', 1)[0]
    for translated in (
        _promql_template_to_signalflow(expr, "", "", "requests"),
        _promql_to_signalflow(expr, "requests"),
    ):
        assert "data('http_requests_total'" in translated
        assert f"filter('path', '{expected}')" in translated
        assert " / " not in translated


@pytest.mark.parametrize(
    "expr",
    [
        'http_requests_total{path="/checkout"} / http_requests_total{path="/all"}',
        'rate(http_requests_total{path="/checkout"}[5m]) / rate(http_requests_total{path="/all"}[5m])',
    ],
)
def test_real_division_after_quoted_selector_slashes_still_translates(expr):
    slash = expr.index(" / ") + 1
    assert _find_top_level_slash(expr) == slash
    for translated in (
        _promql_template_to_signalflow(expr, "", "", "ratio"),
        _promql_to_signalflow(expr, "ratio"),
    ):
        assert translated.count(" / ") == 1
        assert "filter('path', '/checkout')" in translated
        assert "filter('path', '/all')" in translated


@pytest.mark.parametrize(
    "literal",
    [r"/checkout/\"quote", r"/checkout/\\", "/checkout/{(]"],
)
def test_ratio_scanner_ignores_escaped_quotes_and_delimiters_in_selector_literal(literal):
    left = f'http_requests_total{{path="{literal}"}}'
    assert _find_top_level_slash(left) is None
    assert _find_top_level_slash(f"{left} / requests_total") == len(left) + 1


def test_quoted_selector_slash_passes_publication_preflight():
    spec = DashboardSpec(
        title="Requests",
        panels=[
            PanelSpec(
                title="Requests",
                queries=[PanelQuery(expr='http_requests_total{path="/checkout"}', datasource_uid="signalfx")],
            )
        ],
    )
    validate_signalflow_publication(spec)


@pytest.mark.asyncio
async def test_escaped_selector_literal_remains_unsupported_before_publication_side_effects():
    spec = DashboardSpec(
        title="Requests",
        panels=[
            PanelSpec(
                title="Requests",
                queries=[PanelQuery(expr=r'http_requests_total{path="/checkout/\"quoted"}', datasource_uid="signalfx")],
            )
        ],
    )
    client = SignalFxClient(runtime_settings=Settings(_env_file=None))
    client.get_or_create_dashboard_group = AsyncMock()
    client.create_chart = AsyncMock()
    client.create_dashboard = AsyncMock()

    try:
        with pytest.raises(UnsupportedSignalFlowTranslation):
            await publish_dashboard(client, spec)
    finally:
        await client.close()

    client.get_or_create_dashboard_group.assert_not_called()
    client.create_chart.assert_not_called()
    client.create_dashboard.assert_not_called()
