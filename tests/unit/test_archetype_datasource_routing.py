from dashforge.archetypes.engine import compile_archetype
from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.models.schemas import ArchetypeMatch, Intent, MetricEntry, SignalType


def test_promql_query_routes_to_datasource_that_owns_metric():
    archetype = InvestigationArchetype(
        id="real-data-test",
        name="Real data test",
        description="",
        problem_types=["real_data_test"],
        required_metrics=["real_metric"],
        panels=[
            PanelTemplate(
                title="Real metric",
                queries=[QueryTemplate(expr="rate(real_metric[5m])")],
            )
        ],
    )
    intent = Intent(
        summary="inspect real data",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["real"],
        timerange="1h",
        problem_type="real_data_test",
        archetypes=[ArchetypeMatch(type="real_data_test", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="synthetic_metric",
            datasource_uid="synthetic",
            datasource_name="Synthetic",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="real_metric",
            datasource_uid="real-telemetry",
            datasource_name="Real Telemetry",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert dashboard.panels[0].queries[0].datasource_uid == "real-telemetry"


def test_shared_promql_metric_routes_to_datasource_with_requested_service():
    archetype = InvestigationArchetype(
        id="shared-metric-test",
        name="Shared metric test",
        problem_types=["latency"],
        required_metrics=["http_requests_total"],
        panels=[
            PanelTemplate(
                title="Requests",
                queries=[QueryTemplate(expr="rate(http_requests_total{{{service_filter}}}[5m])")],
            )
        ],
    )
    intent = Intent(
        summary="checkout is slow",
        domain="application",
        services=["checkout-service"],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        timerange="1h",
        problem_type="latency",
        archetypes=[ArchetypeMatch(type="latency", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="http_requests_total",
            datasource_uid="inventory-prom",
            datasource_name="Inventory",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={inventory}"],
        ),
        MetricEntry(
            name="http_requests_total",
            datasource_uid="checkout-prom",
            datasource_name="Checkout",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={checkout}"],
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert dashboard.panels[0].queries[0].datasource_uid == "checkout-prom"
