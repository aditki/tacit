from promql_parser import parse

from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.evidence import requirements_for_archetype
from dashforge.models.schemas import (
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
from dashforge.pipeline import (
    _build_symptom_evidence_dashboard,
    _compiled_query_diagnostics,
    _history_archetypes,
    _history_signals,
    _missing_critical_symptom_requirements,
    _semantic_mapping_diagnostics,
)
from dashforge.signals import SignalStore


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

    dashboard, rescue_resolutions = _build_symptom_evidence_dashboard(
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

    assert dashboard.tags == ["dashforge", "evidence", "symptom"]
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

    dashboard, rescue_resolutions = _build_symptom_evidence_dashboard(
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
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
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

    dashboard, rescue_resolutions = _build_symptom_evidence_dashboard(
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

    dashboard, _ = _build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_requests_total", dimensions=['app="checkout-service"'], metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == 'sum(rate(http_requests_total{app=~"checkout-service"}[5m]))'


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

    dashboard, _ = _build_symptom_evidence_dashboard(
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

    dashboard, rescue_resolutions = _build_symptom_evidence_dashboard(
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

    dashboard, _ = _build_symptom_evidence_dashboard(
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

    dashboard, rescue_resolutions = _build_symptom_evidence_dashboard(
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

    dashboard, _ = _build_symptom_evidence_dashboard(
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

    dashboard, _ = _build_symptom_evidence_dashboard(
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


def test_symptom_evidence_dashboard_abstains_on_tied_metric_owners(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
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

    dashboard, rescue_resolutions = _build_symptom_evidence_dashboard(
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


def test_missing_critical_symptom_requirements_detects_partial_dashboard_gap():
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
            resolution_metric="container_cpu_usage_seconds_total",
            survived=True,
            non_empty=True,
        ),
    ]

    missing = _missing_critical_symptom_requirements(requirements, resolutions, observations)

    assert [requirement.signal_type for requirement in missing] == ["request_latency"]
