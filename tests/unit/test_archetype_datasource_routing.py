from dashforge.archetypes.engine import compile_archetype
from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.models.schemas import ArchetypeMatch, Intent, MetricEntry, SignalType
from dashforge.signals import SignalStore


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


def test_multi_metric_query_routes_when_one_datasource_owns_all_metrics():
    archetype = InvestigationArchetype(
        id="ratio-test",
        name="Ratio test",
        problem_types=["errors"],
        required_metrics=["request_errors_total", "requests_total"],
        panels=[
            PanelTemplate(
                title="Error ratio",
                queries=[QueryTemplate(expr="request_errors_total / requests_total")],
            )
        ],
    )
    intent = Intent(
        summary="inspect errors",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["errors"],
        timerange="1h",
        problem_type="errors",
        archetypes=[ArchetypeMatch(type="errors", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="unrelated_metric",
            datasource_uid="default-prom",
            datasource_name="Default",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="request_errors_total",
            datasource_uid="service-prom",
            datasource_name="Service",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="requests_total",
            datasource_uid="service-prom",
            datasource_name="Service",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert dashboard.panels[0].queries[0].datasource_uid == "service-prom"


def test_service_owner_must_cover_every_metric_in_query():
    archetype = InvestigationArchetype(
        id="cache-ratio",
        name="Cache ratio",
        problem_types=["cache"],
        required_metrics=["redis_keyspace_hits_total", "redis_keyspace_misses_total"],
        panels=[
            PanelTemplate(
                title="Hit ratio",
                queries=[QueryTemplate(expr="redis_keyspace_hits_total / redis_keyspace_misses_total")],
            )
        ],
    )
    intent = Intent(
        summary="checkout cache ratio",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cache"],
        timerange="1h",
        problem_type="cache",
        archetypes=[ArchetypeMatch(type="cache", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="redis_keyspace_misses_total",
            datasource_uid="default-prom",
            datasource_name="Default",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={payment}"],
        ),
        MetricEntry(
            name="redis_keyspace_hits_total",
            datasource_uid="checkout-prom",
            datasource_name="Checkout",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={checkout}"],
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert dashboard.panels[0].queries[0].datasource_uid == "default-prom"


def test_single_discovered_operand_does_not_reroute_multi_metric_query():
    archetype = InvestigationArchetype(
        id="partial-ratio",
        name="Partial ratio",
        problem_types=["errors"],
        required_metrics=["errors_total", "requests_total"],
        panels=[
            PanelTemplate(
                title="Error ratio",
                queries=[QueryTemplate(expr="errors_total / requests_total")],
            )
        ],
    )
    intent = Intent(
        summary="error ratio",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["errors"],
        timerange="1h",
        problem_type="errors",
        archetypes=[ArchetypeMatch(type="errors", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="default_only_metric",
            datasource_uid="default-prom",
            datasource_name="Default",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="errors_total",
            datasource_uid="partial-prom",
            datasource_name="Partial",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert dashboard.panels[0].queries[0].datasource_uid == "default-prom"


def test_legacy_required_metrics_bind_through_live_semantic_signals(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="resource-saturation",
        name="Resource saturation",
        problem_types=["resource_saturation"],
        required_metrics=["container_cpu_usage_seconds_total", "container_memory_working_set_bytes"],
        panels=[
            PanelTemplate(
                title="CPU",
                queries=[QueryTemplate(expr="rate(container_cpu_usage_seconds_total[5m])")],
            ),
            PanelTemplate(
                title="Memory",
                queries=[QueryTemplate(expr="container_memory_working_set_bytes")],
            ),
        ],
    )
    intent = Intent(
        summary="resource pressure",
        domain="infrastructure",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["cpu", "memory"],
        timerange="1h",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="gamma_container_cpu_usage_seconds_total",
            datasource_uid="gamma",
            datasource_name="GAMMA",
            datasource_type="prometheus",
            query_language="promql",
            metric_type="counter",
        ),
        MetricEntry(
            name="gamma_container_memory_working_set_bytes",
            datasource_uid="gamma",
            datasource_name="GAMMA",
            datasource_type="prometheus",
            query_language="promql",
            metric_type="gauge",
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    expressions = [query.expr for panel in dashboard.panels for query in panel.queries]
    assert "rate(gamma_container_cpu_usage_seconds_total[5m])" in expressions
    assert "gamma_container_memory_working_set_bytes" in expressions


def test_legacy_metric_existence_is_scoped_to_target_backend(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="resource-saturation",
        name="Resource saturation",
        problem_types=["resource_saturation"],
        required_metrics=["container_cpu_usage_seconds_total"],
        panels=[
            PanelTemplate(
                title="CPU",
                queries=[QueryTemplate(expr="rate(container_cpu_usage_seconds_total[5m])")],
            )
        ],
    )
    intent = Intent(
        summary="resource pressure",
        domain="infrastructure",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        timerange="1h",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="container_cpu_usage_seconds_total",
            datasource_uid="signalfx",
            datasource_name="SignalFx",
            datasource_type="signalfx",
            query_language="signalflow",
            metric_type="counter",
        ),
        MetricEntry(
            name="gamma_container_cpu_usage_seconds_total",
            datasource_uid="prometheus",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            metric_type="counter",
        ),
    ]

    dashboard = compile_archetype(archetype, intent, catalog, target_language="promql")

    query = dashboard.panels[0].queries[0]
    assert query.expr == "rate(gamma_container_cpu_usage_seconds_total[5m])"
    assert query.datasource_uid == "prometheus"


def test_legacy_binding_uses_requested_service_before_tie_abstention(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="resource-saturation",
        name="Resource saturation",
        problem_types=["resource_saturation"],
        required_metrics=["container_cpu_usage_seconds_total"],
        panels=[
            PanelTemplate(
                title="CPU",
                queries=[QueryTemplate(expr="rate(container_cpu_usage_seconds_total[5m])")],
            )
        ],
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        timerange="1h",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name=f"{service}_container_cpu_usage_seconds_total",
            datasource_uid="gamma",
            datasource_name="GAMMA",
            datasource_type="prometheus",
            query_language="promql",
            metric_type="counter",
            dimensions=[f"service={{{service}}}"],
        )
        for service in ("checkout", "payments")
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert "checkout_container_cpu_usage_seconds_total" in dashboard.panels[0].queries[0].expr


def test_legacy_binding_abstains_when_multiple_services_are_equally_plausible(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="resource-saturation",
        name="Resource saturation",
        problem_types=["resource_saturation"],
        required_metrics=["container_cpu_usage_seconds_total"],
        panels=[
            PanelTemplate(
                title="CPU",
                queries=[QueryTemplate(expr="rate(container_cpu_usage_seconds_total[5m])")],
            )
        ],
    )
    intent = Intent(
        summary="resource pressure",
        domain="infrastructure",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        timerange="1h",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name=f"{service}_container_cpu_usage_seconds_total",
            datasource_uid="gamma",
            datasource_name="GAMMA",
            datasource_type="prometheus",
            query_language="promql",
            metric_type="counter",
        )
        for service in ("checkout", "payments")
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert dashboard.panels[0].queries[0].expr == "rate(container_cpu_usage_seconds_total[5m])"


def test_legacy_binding_rejects_gauge_for_histogram_template(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="latency",
        name="Latency",
        problem_types=["latency"],
        required_metrics=["http_request_duration_seconds"],
        panels=[
            PanelTemplate(
                title="p95",
                queries=[
                    QueryTemplate(
                        expr=(
                            "histogram_quantile(0.95, "
                            "sum(rate(http_request_duration_seconds_bucket[5m])) by (le))"
                        )
                    )
                ],
            )
        ],
    )
    intent = Intent(
        summary="latency",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        timerange="1h",
        problem_type="latency",
        archetypes=[ArchetypeMatch(type="latency", confidence=1.0)],
    )
    catalog = [
        MetricEntry(
            name="gamma_request_latency_seconds",
            datasource_uid="gamma",
            datasource_name="GAMMA",
            datasource_type="prometheus",
            query_language="promql",
            metric_type="gauge",
        )
    ]

    dashboard = compile_archetype(archetype, intent, catalog)

    assert "http_request_duration_seconds_bucket" in dashboard.panels[0].queries[0].expr
