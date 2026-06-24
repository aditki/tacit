from promql_parser import parse

from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.evidence import SUPPORTED_OBSERVATION, requirements_for_archetype
from tacit.evidence_artifacts import (
    build_evidence_gap_dashboard,
    build_symptom_evidence_dashboard,
    missing_critical_evidence_gap_requirements,
    missing_critical_symptom_requirements,
)
from tacit.models.schemas import (
    ArchetypeMatch,
    DashboardSpec,
    EvidenceObservation,
    EvidenceResolution,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)
from tacit.pipeline import (
    _compiled_query_diagnostics,
    _history_archetypes,
    _history_signals,
    _semantic_mapping_diagnostics,
)
from tacit.signals import SignalStore


def _arch(
    arch_id: str,
    *,
    required_signals: list[str] | None = None,
    signal_bindings: dict[str, str] | None = None,
) -> InvestigationArchetype:
    return InvestigationArchetype(
        id=arch_id,
        name=arch_id.replace("_", " ").title(),
        problem_types=[arch_id],
        required_signals=required_signals or [],
        signal_bindings=signal_bindings or {},
        panels=[
            PanelTemplate(
                title="Panel",
                queries=[QueryTemplate(expr="up", legend_format="{{instance}}")],
            )
        ],
    )


def _metric(
    name: str,
    *,
    dimensions: list[str] | None = None,
    metric_type: str = "",
    datasource_uid: str = "gamma-telemetry",
    datasource_type: str = "prometheus",
    query_language: str = "promql",
) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid=datasource_uid,
        datasource_name="GAMMA Telemetry",
        datasource_type=datasource_type,
        query_language=query_language,
        dimensions=dimensions or [],
        metric_type=metric_type,
    )


def test_history_archetypes_include_selected_learned_matches():
    classifier = [ArchetypeMatch(type="resource_saturation", confidence=0.9)]
    base = _arch("resource_saturation")
    learned = _arch(
        "learned_falco_memory",
        required_signals=["container_memory_usage"],
        signal_bindings={"pod_memory_pressure": "falco_memory_bytes"},
    )

    records = _history_archetypes(
        classifier,
        selected_archetypes=[(learned, 0.82), (base, 0.7)],
        learned_archetypes=[(learned, 0.82)],
    )

    assert records[0]["type"] == "learned_falco_memory"
    assert records[0]["source"] == "learned"
    assert records[0]["selected"] is True
    assert records[0]["signals"] == ["container_memory_usage", "pod_memory_pressure"]
    assert records[1]["type"] == "resource_saturation"


def test_history_signals_include_semantic_archetype_signals():
    learned = _arch(
        "learned_falco_memory",
        required_signals=["container_memory_usage"],
        signal_bindings={"pod_memory_pressure": "falco_memory_bytes"},
    )

    signals = _history_signals([SignalType.METRICS], [(learned, 0.82)])

    assert signals == ["metrics", "container_memory_usage", "pod_memory_pressure"]


def test_semantic_mapping_diagnostics_is_independent_of_exact_binding():
    catalog = [
        _metric("gamma_container_cpu_usage_seconds_total"),
        _metric("opaque_metric"),
    ]

    status, reason, details = _semantic_mapping_diagnostics(catalog)

    assert status == "partial"
    assert reason == "some_metrics_unmapped"
    assert details["mapped"] == {"gamma_container_cpu_usage_seconds_total": "resource_usage"}
    assert details["unmapped"] == ["opaque_metric"]


def test_compiled_query_diagnostics_reports_missing_live_bindings():
    spec = DashboardSpec(
        title="CPU",
        panels=[
            PanelSpec(
                title="CPU",
                queries=[
                    PanelQuery(
                        expr="rate(container_cpu_usage_seconds_total[5m])",
                        datasource_uid="gamma-telemetry",
                    )
                ],
            )
        ],
    )
    catalog = [_metric("gamma_container_cpu_usage_seconds_total")]

    status, reason, details = _compiled_query_diagnostics(spec, catalog)

    assert status == "failed"
    assert reason == "compiled_metrics_absent_from_catalog"
    assert details["missing_metrics"] == ["container_cpu_usage_seconds_total"]


def test_symptom_evidence_dashboard_preserves_resolved_application_symptoms():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_metrics=["http_request_duration_seconds", "http_requests_total"],
        panels=[
            PanelTemplate(
                title="Latency",
                queries=[
                    QueryTemplate(expr="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))")
                ],
            )
        ],
    )
    intent = Intent(
        summary="checkout latency changed",
        domain="application",
        services=["checkout-service"],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        timerange="30m",
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_request_latency_seconds",
            datasource_uid="gamma",
            datasource_type="prometheus",
            query_language="promql",
        ),
        EvidenceResolution(
            requirement_id=requirements[1].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_request_rate",
            datasource_uid="gamma",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[
            _metric("gamma_request_latency_seconds", dimensions=["service={checkout-service}"], datasource_uid="gamma"),
            _metric("gamma_request_rate", dimensions=["service={checkout-service}"], datasource_uid="gamma"),
        ],
        target_language="promql",
        timerange="30m",
    )

    assert dashboard.tags == ["tacit", "evidence", "symptom"]
    assert [resolution.reason_code for resolution in rescue_resolutions] == [
        "live_signal_resolved",
        "live_signal_resolved",
    ]
    assert [panel.title for panel in dashboard.panels] == ["Observed Request Latency", "Observed Request Rate"]
    assert [panel.queries[0].expr for panel in dashboard.panels] == [
        'gamma_request_latency_seconds{service=~"checkout-service"}',
        'gamma_request_rate{service=~"checkout-service"}',
    ]
    for panel in dashboard.panels:
        parse(panel.queries[0].expr)


def test_symptom_evidence_dashboard_does_not_promote_resource_evidence():
    archetype = _arch(
        "resource_saturation",
        required_signals=["cpu_usage"],
        signal_bindings={"cpu_usage": "container_cpu_usage_seconds_total"},
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_container_cpu_usage_seconds_total",
            datasource_uid="gamma",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[],
        target_language="promql",
        timerange="1h",
    )

    assert dashboard.panels == []
    assert rescue_resolutions == []


def test_symptom_evidence_dashboard_resolves_direct_latency_when_template_shape_fails(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_metrics=["http_request_duration_seconds"],
        panels=[
            PanelTemplate(
                title="Latency",
                queries=[
                    QueryTemplate(expr="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))")
                ],
            )
        ],
    )
    intent = Intent(
        summary="requests slowed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    unresolved = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="unresolved",
            reason_code="no_compatible_live_signal",
        )
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        unresolved,
        intent,
        catalog=[_metric("gamma_request_latency_seconds")],
        target_language="promql",
        timerange="15m",
    )

    assert [panel.title for panel in dashboard.panels] == ["Observed Request Latency"]
    assert dashboard.panels[0].queries[0].expr == "gamma_request_latency_seconds"
    assert rescue_resolutions[0].reason_code == "direct_symptom_signal_resolved"


def test_symptom_evidence_dashboard_allows_prometheus_compatible_datasources():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_metrics=["http_request_duration_seconds"],
        panels=[
            PanelTemplate(
                title="Latency",
                queries=[
                    QueryTemplate(expr="histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))")
                ],
            )
        ],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="default_metric_present",
            metric="http_request_duration_seconds",
            datasource_uid="gamma-telemetry",
            datasource_type="mimir",
            query_language="promql",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_request_duration_seconds", datasource_type="mimir")],
        target_language="promql",
        timerange="15m",
    )

    assert [panel.title for panel in dashboard.panels] == ["Observed Request Latency"]
    assert dashboard.panels[0].queries[0].datasource_type == "mimir"


def test_symptom_evidence_dashboard_uses_catalog_label_selector_and_rates_counter():
    archetype = InvestigationArchetype(
        id="traffic_investigation",
        name="Traffic Investigation",
        problem_types=["traffic_investigation"],
        required_metrics=["http_requests_total"],
        panels=[PanelTemplate(title="Traffic", queries=[QueryTemplate(expr="rate(http_requests_total[5m])")])],
    )
    intent = Intent(
        summary="checkout traffic changed",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["traffic"],
        problem_type="traffic_investigation",
        archetypes=[ArchetypeMatch(type="traffic_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_requests_total",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_requests_total", dimensions=['app="checkout-service"'], metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == 'sum(rate(http_requests_total{app=~"checkout-service"}[5m]))'


def test_symptom_evidence_dashboard_scopes_promql_when_service_labels_unsampled():
    archetype = InvestigationArchetype(
        id="traffic_investigation",
        name="Traffic Investigation",
        problem_types=["traffic_investigation"],
        required_metrics=["http_requests_total"],
        panels=[PanelTemplate(title="Traffic", queries=[QueryTemplate(expr="rate(http_requests_total[5m])")])],
    )
    intent = Intent(
        summary="checkout traffic changed",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["traffic"],
        problem_type="traffic_investigation",
        archetypes=[ArchetypeMatch(type="traffic_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_requests_total",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_requests_total", metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == 'sum(rate(http_requests_total{service=~".*checkout.*"}[5m]))'


def test_symptom_evidence_dashboard_keeps_service_variant_selector():
    archetype = InvestigationArchetype(
        id="traffic_investigation",
        name="Traffic Investigation",
        problem_types=["traffic_investigation"],
        required_metrics=["gamma_request_rate"],
        panels=[PanelTemplate(title="Traffic", queries=[QueryTemplate(expr="gamma_request_rate")])],
    )
    intent = Intent(
        summary="checkout traffic changed",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["traffic"],
        problem_type="traffic_investigation",
        archetypes=[ArchetypeMatch(type="traffic_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_request_rate",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("gamma_request_rate", dimensions=["service={prod-checkout,checkout-v2}"])],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == 'gamma_request_rate{service=~"checkout-v2|prod-checkout"}'


def test_symptom_evidence_dashboard_records_duplicate_resolutions_while_deduping_panel():
    primary = InvestigationArchetype(
        id="primary_latency",
        name="Primary Latency",
        problem_types=["latency"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "gamma_request_latency_seconds"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="gamma_request_latency_seconds")])],
    )
    secondary = InvestigationArchetype(
        id="secondary_latency",
        name="Secondary Latency",
        problem_types=["latency"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "gamma_request_latency_seconds"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="gamma_request_latency_seconds")])],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency",
        archetypes=[ArchetypeMatch(type="latency", confidence=0.9)],
    )
    requirements = [
        *requirements_for_archetype(primary, intent),
        *requirements_for_archetype(secondary, intent),
    ]
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_request_latency_seconds",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        ),
        EvidenceResolution(
            requirement_id=requirements[1].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_request_latency_seconds",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("gamma_request_latency_seconds")],
        target_language="promql",
        timerange="15m",
    )

    assert len(dashboard.panels) == 1
    assert [resolution.requirement_id for resolution in rescue_resolutions] == [
        requirements[0].id,
        requirements[1].id,
    ]


def test_symptom_evidence_dashboard_renders_histogram_bucket_latency():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "http_request_duration_seconds_bucket"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="http_request_duration_seconds_bucket")])],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_request_duration_seconds_bucket",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_request_duration_seconds_bucket", metric_type="histogram")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == (
        "histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))"
    )


def test_symptom_evidence_dashboard_preserves_error_spike_as_error_rate():
    archetype = InvestigationArchetype(
        id="error_spike",
        name="Error Spike",
        problem_types=["error_spike"],
        required_metrics=["http_requests_total"],
        panels=[PanelTemplate(title="Errors", queries=[QueryTemplate(expr="http_requests_total")])],
    )
    intent = Intent(
        summary="checkout errors increased",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["errors"],
        problem_type="error_spike",
        archetypes=[ArchetypeMatch(type="error_spike", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_requests_total",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_requests_total", metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert rescue_resolutions == []


def test_symptom_evidence_dashboard_rates_clear_error_counters():
    archetype = InvestigationArchetype(
        id="error_spike",
        name="Error Spike",
        problem_types=["error_spike"],
        required_signals=["error_rate"],
        signal_bindings={"error_rate": "http_errors_total"},
        panels=[PanelTemplate(title="Errors", queries=[QueryTemplate(expr="http_errors_total")])],
    )
    intent = Intent(
        summary="checkout errors increased",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["errors"],
        problem_type="error_spike",
        archetypes=[ArchetypeMatch(type="error_spike", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_errors_total",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_errors_total", metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].title == "Observed Error Rate"
    assert dashboard.panels[0].queries[0].expr == "sum(rate(http_errors_total[5m]))"
    assert dashboard.panels[0].unit == "ops"


def test_symptom_evidence_dashboard_abstains_on_latency_sum_count_helpers():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "http_request_duration_seconds_sum"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="http_request_duration_seconds_sum")])],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_request_duration_seconds_sum",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_request_duration_seconds_sum", metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert rescue_resolutions == []


def test_symptom_evidence_dashboard_builds_signalflow_panels():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "request.duration"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="data('request.duration')")])],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="request.duration",
            datasource_uid="sfx",
            datasource_type="signalfx",
            query_language="signalflow",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[
            _metric(
                "request.duration",
                dimensions=["service={checkout}"],
                datasource_uid="sfx",
                datasource_type="signalfx",
                query_language="signalflow",
            )
        ],
        target_language="signalflow",
        timerange="15m",
    )

    query = dashboard.panels[0].queries[0]
    assert query.datasource_type == "signalfx"
    assert query.query_language == "signalflow"
    assert query.expr == "data('request.duration', filter=filter('service', 'checkout')).mean().publish(label='value')"


def test_symptom_evidence_dashboard_scopes_signalflow_when_dimension_values_unsampled():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "request.duration"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="data('request.duration')")])],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="request.duration",
            datasource_uid="sfx",
            datasource_type="signalfx",
            query_language="signalflow",
        )
    ]

    dashboard, _ = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[
            _metric(
                "request.duration",
                dimensions=["service"],
                datasource_uid="sfx",
                datasource_type="signalfx",
                query_language="signalflow",
            )
        ],
        target_language="signalflow",
        timerange="15m",
    )

    query = dashboard.panels[0].queries[0]
    assert (
        query.expr == "data('request.duration', filter=filter('service', '*checkout*')).mean().publish(label='value')"
    )


def test_symptom_evidence_dashboard_abstains_on_tied_metric_owners(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency_investigation"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "http_request_duration_seconds"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="http_request_duration_seconds")])],
    )
    intent = Intent(
        summary="checkout requests slowed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    unresolved = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="unresolved",
            reason_code="no_compatible_live_signal",
        )
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        unresolved,
        intent,
        catalog=[
            _metric("gamma_request_latency_seconds", datasource_uid="prom-a"),
            _metric("gamma_request_latency_seconds", datasource_uid="prom-b"),
        ],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert rescue_resolutions == []


def test_symptom_evidence_dashboard_preserves_error_context_for_request_counter():
    archetype = InvestigationArchetype(
        id="error_spike",
        name="Error Spike",
        problem_types=["error_spike"],
        required_metrics=["http_requests_total"],
        panels=[PanelTemplate(title="Errors", queries=[QueryTemplate(expr='http_requests_total{status=~"5.."}')])],
    )
    intent = Intent(
        summary="checkout errors increased",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["errors"],
        problem_type="error_spike",
        archetypes=[ArchetypeMatch(type="error_spike", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_requests_total",
            datasource_uid="gamma-telemetry",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, rescue_resolutions = build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_requests_total", metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert rescue_resolutions == []


def testmissing_critical_symptom_requirements_detects_partial_dashboard_gap():
    archetype = InvestigationArchetype(
        id="latency_and_cpu",
        name="Latency and CPU",
        problem_types=["latency"],
        required_signals=["request_latency", "cpu_usage"],
        signal_bindings={
            "request_latency": "http_request_duration_seconds",
            "cpu_usage": "container_cpu_usage_seconds_total",
        },
        panels=[PanelTemplate(title="Signals", queries=[QueryTemplate(expr="up")])],
    )
    intent = Intent(
        summary="checkout is slow",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency", "cpu"],
        problem_type="latency",
        archetypes=[ArchetypeMatch(type="latency", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_request_duration_seconds",
        ),
        EvidenceResolution(
            requirement_id=requirements[1].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="container_cpu_usage_seconds_total",
        ),
    ]
    observations = [
        EvidenceObservation(
            requirement_id=requirements[0].id,
            resolution_metric="http_request_duration_seconds",
            survived=False,
            non_empty=False,
        ),
        EvidenceObservation(
            requirement_id=requirements[1].id,
            outcome=SUPPORTED_OBSERVATION,
            resolution_metric="container_cpu_usage_seconds_total",
            survived=True,
            non_empty=True,
        ),
    ]

    missing = missing_critical_symptom_requirements(requirements, resolutions, observations)

    assert [requirement.signal_type for requirement in missing] == ["request_latency"]


def test_non_symptom_evidence_gaps_are_detected_without_symptom_rescue():
    archetype = InvestigationArchetype(
        id="resource_saturation",
        name="Resource Saturation",
        problem_types=["resource_saturation"],
        required_signals=["cpu_usage", "memory_usage"],
        signal_bindings={
            "cpu_usage": "container_cpu_usage_seconds_total",
            "memory_usage": "container_memory_working_set_bytes",
        },
        panels=[PanelTemplate(title="Resources", queries=[QueryTemplate(expr="up")])],
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu", "memory"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirement.id,
            status="unresolved",
            reason_code="no_compatible_live_signal",
        )
        for requirement in requirements
    ]
    observations = [
        EvidenceObservation(
            requirement_id=requirement.id,
            resolution_metric=requirement.default_metric,
            survived=False,
            non_empty=False,
        )
        for requirement in requirements
    ]

    symptom_missing = missing_critical_symptom_requirements(requirements, resolutions, observations)
    gap_missing = missing_critical_evidence_gap_requirements(requirements, resolutions, observations)

    assert symptom_missing == []
    assert [requirement.signal_type for requirement in gap_missing] == ["cpu_usage", "memory_usage"]


def testmissing_critical_symptom_requirements_treats_signalfx_exists_as_surfaced():
    archetype = InvestigationArchetype(
        id="latency_investigation",
        name="Latency Investigation",
        problem_types=["latency"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "request.duration"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="data('request.duration')")])],
    )
    intent = Intent(
        summary="checkout is slow",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="latency",
        archetypes=[ArchetypeMatch(type="latency", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="request.duration",
            datasource_uid="sfx",
            datasource_type="signalfx",
            query_language="signalflow",
        )
    ]
    observations = [
        EvidenceObservation(
            requirement_id=requirements[0].id,
            resolution_metric="request.duration",
            survived=True,
            non_empty=False,
            rejection_reason="exists",
        )
    ]

    missing = missing_critical_symptom_requirements(requirements, resolutions, observations)

    assert missing == []


def test_evidence_gap_dashboard_resolves_supported_resource_observation(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    archetype = _arch(
        "resource_saturation",
        required_signals=["cpu_usage"],
        signal_bindings={"cpu_usage": "container_cpu_usage_seconds_total"},
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    unresolved = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="unresolved",
            reason_code="no_compatible_live_signal",
        )
    ]

    dashboard, gap_resolutions = build_evidence_gap_dashboard(
        requirements,
        unresolved,
        intent,
        catalog=[
            _metric(
                "gamma_container_cpu_usage_seconds_total",
                dimensions=["service={checkout}"],
                metric_type="counter",
            )
        ],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.tags == ["tacit", "evidence", "gap-observation"]
    assert dashboard.panels[0].title == "Supported CPU Observation"
    assert dashboard.panels[0].row == "Supported Observations"
    assert dashboard.panels[0].queries[0].expr == (
        'sum(rate(gamma_container_cpu_usage_seconds_total{service=~"checkout"}[5m]))'
    )
    assert gap_resolutions[0].reason_code == "evidence_gap_supported_observation"
    panel_text = " ".join(
        [
            dashboard.title,
            dashboard.panels[0].title,
            dashboard.panels[0].description,
            dashboard.panels[0].row,
        ]
    ).lower()
    assert "culprit" not in panel_text
    assert "root cause" not in panel_text
    assert "caused by" not in panel_text


def test_evidence_gap_dashboard_marks_reused_primary_resolution_as_gap():
    archetype = _arch(
        "resource_saturation",
        required_signals=["cpu_usage"],
        signal_bindings={"cpu_usage": "container_cpu_usage_seconds_total"},
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    primary = EvidenceResolution(
        requirement_id=requirements[0].id,
        status="resolved",
        reason_code="live_signal_resolved",
        metric="gamma_container_cpu_usage_seconds_total",
        datasource_uid="gamma-telemetry",
        datasource_type="prometheus",
        query_language="promql",
        semantic_score=0.91,
        ownership_score=1.0,
    )

    dashboard, gap_resolutions = build_evidence_gap_dashboard(
        requirements,
        [primary],
        intent,
        catalog=[
            _metric(
                "gamma_container_cpu_usage_seconds_total",
                dimensions=["service={checkout}"],
                metric_type="counter",
            )
        ],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels
    assert primary.reason_code == "live_signal_resolved"
    assert gap_resolutions[0] is not primary
    assert gap_resolutions[0].metric == primary.metric
    assert gap_resolutions[0].datasource_uid == primary.datasource_uid
    assert gap_resolutions[0].reason_code == "evidence_gap_supported_observation"


def test_evidence_gap_dashboard_requires_requested_service_scope(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    archetype = _arch(
        "resource_saturation",
        required_signals=["cpu_usage"],
        signal_bindings={"cpu_usage": "container_cpu_usage_seconds_total"},
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)

    dashboard, gap_resolutions = build_evidence_gap_dashboard(
        requirements,
        [
            EvidenceResolution(
                requirement_id=requirements[0].id,
                status="unresolved",
                reason_code="no_compatible_live_signal",
            )
        ],
        intent,
        catalog=[_metric("gamma_container_cpu_usage_seconds_total", dimensions=["service={payments}"])],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert gap_resolutions == []


def test_evidence_gap_dashboard_requires_catalog_owner_for_service_scope():
    archetype = _arch(
        "resource_saturation",
        required_signals=["cpu_usage"],
        signal_bindings={"cpu_usage": "container_cpu_usage_seconds_total"},
    )
    intent = Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)

    dashboard, gap_resolutions = build_evidence_gap_dashboard(
        requirements,
        [
            EvidenceResolution(
                requirement_id=requirements[0].id,
                status="resolved",
                reason_code="live_signal_resolved",
                metric="gamma_container_cpu_usage_seconds_total",
                datasource_uid="gamma",
                datasource_type="prometheus",
                query_language="promql",
            )
        ],
        intent,
        catalog=[],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert gap_resolutions == []


def test_evidence_gap_dashboard_abstains_on_ambiguous_owners(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)
    archetype = _arch(
        "resource_saturation",
        required_signals=["cpu_usage"],
        signal_bindings={"cpu_usage": "container_cpu_usage_seconds_total"},
    )
    intent = Intent(
        summary="resource pressure",
        domain="infrastructure",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["cpu"],
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)

    dashboard, gap_resolutions = build_evidence_gap_dashboard(
        requirements,
        [
            EvidenceResolution(
                requirement_id=requirements[0].id,
                status="unresolved",
                reason_code="ambiguous_live_signal",
            )
        ],
        intent,
        catalog=[
            _metric("gamma_container_cpu_usage_seconds_total", datasource_uid="prom-a"),
            _metric("gamma_container_cpu_usage_seconds_total", datasource_uid="prom-b"),
        ],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels == []
    assert gap_resolutions == []


def testmissing_critical_evidence_gap_requirements_excludes_symptoms_and_surfaced_evidence():
    archetype = InvestigationArchetype(
        id="mixed",
        name="Mixed",
        problem_types=["mixed"],
        required_signals=["request_latency", "cpu_usage", "memory_usage"],
        signal_bindings={
            "request_latency": "http_request_duration_seconds",
            "cpu_usage": "container_cpu_usage_seconds_total",
            "memory_usage": "container_memory_working_set_bytes",
        },
        panels=[PanelTemplate(title="Mixed", queries=[QueryTemplate(expr="up")])],
    )
    intent = Intent(
        summary="checkout slow and resource pressure",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency", "cpu"],
        problem_type="mixed",
        archetypes=[ArchetypeMatch(type="mixed", confidence=0.9)],
    )
    requirements = requirements_for_archetype(archetype, intent)
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="http_request_duration_seconds",
        ),
        EvidenceResolution(
            requirement_id=requirements[1].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="container_cpu_usage_seconds_total",
        ),
        EvidenceResolution(
            requirement_id=requirements[2].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="container_memory_working_set_bytes",
        ),
    ]
    observations = [
        EvidenceObservation(
            requirement_id=requirements[0].id,
            resolution_metric="http_request_duration_seconds",
            survived=False,
            non_empty=False,
        ),
        EvidenceObservation(
            requirement_id=requirements[1].id,
            outcome=SUPPORTED_OBSERVATION,
            resolution_metric="container_cpu_usage_seconds_total",
            survived=True,
            non_empty=True,
        ),
        EvidenceObservation(
            requirement_id=requirements[2].id,
            resolution_metric="container_memory_working_set_bytes",
            survived=False,
            non_empty=False,
        ),
    ]

    missing = missing_critical_evidence_gap_requirements(requirements, resolutions, observations)

    assert [requirement.signal_type for requirement in missing] == ["memory_usage"]
