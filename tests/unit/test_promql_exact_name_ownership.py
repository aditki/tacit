"""Exact metric-name selectors retain complete, catalog-grounded ownership."""

import pytest
import yaml

import tacit.query_parsing.promql as parsing
from tacit.archetypes.engine import compile_archetype
from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.catalog import begin_selection_audit, end_selection_audit, recorded_selections
from tacit.dashboard_ingest.archetype_generation import escape_literal_braces, generate_archetype_yaml
from tacit.models.schemas import Intent, MetricEntry


def metric(name="http_requests_total", uid="owner"):
    return MetricEntry(
        name=name,
        datasource_uid=uid,
        datasource_name=uid,
        datasource_type="prometheus",
        query_language="promql",
    )


def archetype(expression):
    return InvestigationArchetype(
        id="exact-name-owner",
        name="Exact metric name",
        problem_types=["resource_saturation"],
        panels=[PanelTemplate(title="Requests", queries=[QueryTemplate(expr=escape_literal_braces(expression))])],
    )


def intent():
    return Intent(summary="inspect checkout requests", domain="application", problem_type="resource_saturation")


@pytest.mark.parametrize(
    "expression,expected",
    [
        ('{__name__="http_requests_total",job="checkout"}', ["http_requests_total"]),
        ("{__name__='http_requests_total'}", ["http_requests_total"]),
        ("{__name__=`http_requests_total`}", ["http_requests_total"]),
        ('{"__name__"="http_requests_total"}', ["http_requests_total"]),
        ('{__name__="http_requests_total",__name__="http_requests_total"}', ["http_requests_total"]),
        ('{__name__="rate"}', ["rate"]),
        ('{__name__="a",__name__=~"a.*"}', ["a"]),
        ('{__name__="a",__name__!="b"}', ["a"]),
        ('{__name__="a",__name__!~"b.*"}', ["a"]),
        ("rate", ["rate"]),
        ("__internal_total", ["__internal_total"]),
        ('sum(rate({__name__="http_requests_total",job="checkout"}[5m]))', ["http_requests_total"]),
        (
            'sum({__name__="http_requests_total"}) / sum({__name__="http_errors_total"})',
            ["http_requests_total", "http_errors_total"],
        ),
    ],
)
def test_ast_extracts_exact_name_literals_without_treating_them_as_labels_or_functions(expression, expected):
    assert parsing.extract_metrics_from_promql(expression) == expected


def test_exact_name_selector_compiles_at_its_unique_owner_and_keeps_expression():
    expression = '{__name__="http_requests_total",job="checkout"}'
    dashboard = compile_archetype(archetype(expression), intent(), [metric()])
    assert len(dashboard.panels) == 1
    query = dashboard.panels[0].queries[0]
    assert query.expr == expression and query.datasource_uid == "owner"


@pytest.mark.parametrize("name", ["sum", "rate", "__internal_total"])
def test_ast_named_metric_literals_compile_without_regex_keyword_heuristics(name):
    dashboard = compile_archetype(archetype(name), intent(), [metric(name)])
    assert len(dashboard.panels) == 1
    assert dashboard.panels[0].queries[0].expr == name
    assert dashboard.panels[0].queries[0].datasource_uid == "owner"


def test_learned_exact_name_selector_uses_shared_parser_and_preserves_query():
    expression = 'rate({__name__="http_requests_total",job="checkout"}[5m])'
    serialized = generate_archetype_yaml(
        {
            "dashboard_title": "Exact metric name",
            "metrics_found": ["http_requests_total"],
            "query_language": "promql",
            "panels": [{"title": "Requests", "queries": [expression]}],
        },
        [],
    )
    learned = InvestigationArchetype.model_validate(yaml.safe_load(serialized)["archetypes"][0])
    dashboard = compile_archetype(learned, intent(), [metric()])
    assert len(dashboard.panels) == 1
    assert dashboard.panels[0].queries[0].expr == expression
    assert dashboard.panels[0].queries[0].datasource_uid == "owner"


@pytest.mark.parametrize(
    "entries,reason",
    [
        ([metric(uid="a"), metric(uid="b")], "multiple_owners"),
        ([metric("unrelated")], "no_owner"),
        ([metric(), metric("http_errors_total", uid="other")], "no_owner"),
    ],
)
def test_exact_matchers_keep_full_operand_and_unique_owner_requirements(entries, reason):
    expression = '{__name__="http_requests_total"}'
    if len(entries) == 2 and entries[1].name == "http_errors_total":
        expression += ' / {__name__="http_errors_total"}'
    token = begin_selection_audit()
    try:
        dashboard = compile_archetype(archetype(expression), intent(), entries)
        selections = recorded_selections()
    finally:
        end_selection_audit(token)
    assert dashboard.panels == []
    assert len(selections) == 1
    assert selections[0]["reason"] == reason
    assert "http_requests_total" in selections[0]["required_metrics"]


@pytest.mark.parametrize(
    "expression",
    [
        '{__name__=~"http_.*"}',
        '{job="checkout"}',
        '{__name__="a",__name__="b"}',
        '{__name__="a",__name__!="a"}',
        '{__name__="a" or __name__="b"}',
        'known_total + {__name__=~"http_.*"}',
        'known_total + {job="checkout"}',
        'known_total + {__name__="a",__name__="b"}',
        'known_total + a{__name__="b"}',
    ],
)
def test_unproved_selectors_invalidate_the_complete_operand_set(expression):
    assert parsing.extract_metrics_from_promql(expression) == []
    entries = [metric(name) for name in ("known_total", "a", "b", "http_requests_total")]
    assert compile_archetype(archetype(expression), intent(), entries).panels == []


@pytest.mark.parametrize(
    "expression",
    [
        '{__name__="http_requests_total"}',
        'known_total + {__name__=~"http_.*"}',
        'a{__name__="b"}',
        'a{"__name__"="b"}',
    ],
)
def test_parser_failure_does_not_turn_name_matchers_into_partial_regex_owner_proof(monkeypatch, expression):
    def unavailable(_expression):
        raise ValueError("parser could not prove this expression")

    monkeypatch.setattr(parsing.promql_parser, "parse", unavailable)
    assert parsing.extract_metrics_from_promql(expression) == []
    assert parsing.extract_metrics_from_promql_regex(expression) == []


def test_keyword_like_named_operand_cannot_be_ignored_beside_an_exact_selector():
    expression = 'rate + {__name__="http_requests_total"}'
    assert parsing.extract_metrics_from_promql(expression) == ["rate", "http_requests_total"]
    assert compile_archetype(archetype(expression), intent(), [metric("rate", uid="other"), metric()]).panels == []


def test_ordinary_label_values_and_function_arguments_are_not_metric_operands():
    expression = 'label_replace(known_total{job="__name__=ignored"}, "__name__", "replacement", "job", ".*")'
    assert parsing.extract_metrics_from_promql(expression) == ["known_total"]


def test_ordinary_legacy_named_metric_fallback_is_unchanged(monkeypatch):
    monkeypatch.setattr(parsing.promql_parser, "parse", lambda _expression: (_ for _ in ()).throw(ValueError()))
    assert parsing.extract_metrics_from_promql("rate(known_total{{{service_filter}}}[{rate_interval}])") == [
        "known_total"
    ]
