"""SignalFlow guards distinguish literal data from actual query operators."""

from unittest.mock import AsyncMock

import pytest

import tacit.archetypes.engine as engine
from tacit.archetypes.engine import (
    UnsupportedSignalFlowTranslation,
    _promql_template_to_signalflow,
    compile_archetype,
)
from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.config import Settings
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry, PanelQuery, PanelSpec
from tacit.signalfx.client import SignalFxClient
from tacit.signalfx.publisher import (
    _build_chart_json,
    _promql_to_signalflow,
    publish_dashboard,
    validate_signalflow_publication,
)


def _spec(expressions):
    return DashboardSpec(
        title="Requests",
        panels=[
            PanelSpec(
                title=f"Requests {index}",
                queries=[PanelQuery(expr=expr, datasource_uid="sfx", datasource_type="signalfx")],
            )
            for index, expr in enumerate(expressions)
        ],
    )


@pytest.mark.parametrize(
    "value",
    [
        "/a!=b",
        "/a!~b",
        "/a=~b",
        "histogram_quantile(0.9)",
        "a!=b!~c=~d",
        "literal ' != =~ !~ / )}[",
    ],
)
@pytest.mark.parametrize("shape", ["bare", "rate", "aggregate", "ratio"])
def test_exact_literal_syntax_survives_compiler_and_standalone_publication(value, shape):
    selector = f'requests_total{{route="{value}"}}'
    expr = {
        "bare": selector,
        "rate": f"rate({selector}[5m])",
        "aggregate": f"sum(rate({selector}[5m])) by (job)",
        "ratio": f'{selector} / requests_total{{route="all"}}',
    }[shape]
    expected = f"filter('route', {value!r})"

    # The compiler, evidence/rescue compilation and the standalone publisher
    # must consume the same guarded translator rather than separate policies.
    assert expected in _promql_template_to_signalflow(expr, "", "", "Requests")
    assert expected in _promql_to_signalflow(expr, "Requests")
    spec = _spec([expr])
    validate_signalflow_publication(spec)
    assert expected in _build_chart_json(spec.panels[0])["programText"]

    archetype = InvestigationArchetype(
        id="literal-syntax",
        name="Literal syntax",
        problem_types=["error_spike"],
        panels=[
            PanelTemplate(
                title="Requests",
                queries=[QueryTemplate(expr=expr.replace("{", "{{").replace("}", "}}"))],
            )
        ],
    )
    catalog = [
        MetricEntry(
            name="requests_total",
            datasource_uid="sfx",
            datasource_name="SignalFx",
            datasource_type="signalfx",
            query_language="signalflow",
        )
    ]
    compiled = compile_archetype(
        archetype,
        Intent(summary="Inspect request errors", domain="application", problem_type="error_spike"),
        catalog,
        target_language="signalflow",
    )
    assert len(compiled.panels) == 1
    assert compiled.panels[0].queries[0].datasource_uid == "sfx"
    assert expected in compiled.panels[0].queries[0].expr


@pytest.mark.parametrize(
    "operator,reason",
    [("!=", "synthetic_fallback"), ("!~", "synthetic_fallback"), ("=~", "regex_widening")],
)
def test_real_matcher_operators_outside_quoted_literals_remain_rejected(operator, reason):
    expr = f'requests_total{{route="literal=~!=!~",job{operator}"checkout"}}'

    for translate in (
        lambda: _promql_template_to_signalflow(expr, "", "", "Requests"),
        lambda: _promql_to_signalflow(expr, "Requests"),
    ):
        with pytest.raises(UnsupportedSignalFlowTranslation) as error:
            translate()
        assert error.value.reason == reason


@pytest.mark.asyncio
@pytest.mark.parametrize("operator", ["!=", "!~", "=~"])
async def test_real_unsupported_matcher_after_literal_chart_has_zero_publication_effects(operator):
    spec = _spec(
        [
            'requests_total{route="literal=~!=!~"}',
            f'requests_total{{route="literal=~!=!~",job{operator}"checkout"}}',
        ]
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


@pytest.mark.asyncio
@pytest.mark.parametrize("literal", [r"a\"!=b", r"a\\=~b", "a*=~b", "a!~\nb"])
async def test_unsupported_literal_grammar_stays_rejected_before_publication(literal):
    spec = _spec([f'rate(requests_total{{route="{literal}"}}[5m])'])
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


@pytest.mark.parametrize("shape", ["ratio", "topk"])
@pytest.mark.parametrize("entrypoint", ["compiler", "publisher"])
def test_one_syntax_admission_covers_all_recursive_translation_work(monkeypatch, shape, entrypoint):
    selector = 'requests_total{route="/a!=b"}'
    expr = " / ".join([selector] * 100) if shape == "ratio" else "topk(1, " * 100 + selector + ")" * 100
    visits = []
    validate = engine._validate_signalflow_translation_syntax

    def counted(value):
        visits.append(len(value))
        return validate(value)

    monkeypatch.setattr(engine, "_validate_signalflow_translation_syntax", counted)
    translated = (
        _promql_template_to_signalflow(expr, "", "", "Requests")
        if entrypoint == "compiler"
        else _promql_to_signalflow(expr, "Requests")
    )

    assert translated.count("data('requests_total'") == (100 if shape == "ratio" else 1)
    assert translated.count(".publish(") == 1
    assert visits == [len(expr)]


@pytest.mark.parametrize("shape", ["ratio", "topk"])
def test_single_syntax_admission_still_rejects_nested_real_operators(shape):
    selector = 'requests_total{route="/a!=b",job=~"checkout"}'
    expr = (
        " / ".join(["requests_total"] * 99 + [selector])
        if shape == "ratio"
        else "topk(1, " * 100 + selector + ")" * 100
    )

    with pytest.raises(UnsupportedSignalFlowTranslation) as error:
        _promql_to_signalflow(expr, "Requests")
    assert error.value.reason == "regex_widening"
