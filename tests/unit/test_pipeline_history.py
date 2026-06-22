from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.evidence import requirements_for_archetype
from dashforge.models.schemas import (
    ArchetypeMatch,
    DashboardSpec,
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


def _metric(name: str, *, dimensions: list[str] | None = None, metric_type: str = "") -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid="gamma-telemetry",
        datasource_name="GAMMA Telemetry",
        datasource_type="prometheus",
        query_language="promql",
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
    assert dashboard.panels[0].source_archetype == "latency_investigation"
    assert dashboard.panels[0].queries[0].expr == "gamma_request_latency_seconds"
    assert rescue_resolutions[0].reason_code == "direct_symptom_signal_resolved"


def test_symptom_evidence_dashboard_treats_duration_default_as_latency():
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
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    dashboard, _ = _build_symptom_evidence_dashboard(
        requirements,
        resolutions,
        intent,
        catalog=[_metric("http_request_duration_seconds", metric_type="histogram")],
        target_language="promql",
        timerange="15m",
    )

    assert [panel.title for panel in dashboard.panels] == ["Observed Request Latency"]
    assert dashboard.panels[0].queries[0].expr == "http_request_duration_seconds"


def test_symptom_evidence_dashboard_wraps_counter_request_rate():
    archetype = InvestigationArchetype(
        id="traffic_investigation",
        name="Traffic Investigation",
        problem_types=["traffic_investigation"],
        required_metrics=["http_requests_total"],
        panels=[
            PanelTemplate(
                title="Traffic",
                queries=[QueryTemplate(expr="rate(http_requests_total[5m])")],
            )
        ],
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
        catalog=[_metric("http_requests_total", dimensions=["service_name={checkout}"], metric_type="counter")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == 'sum(rate(http_requests_total{service_name=~"checkout"}[5m]))'


def test_symptom_evidence_dashboard_omits_selector_for_unscoped_metric():
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
        catalog=[_metric("gamma_request_rate")],
        target_language="promql",
        timerange="15m",
    )

    assert dashboard.panels[0].queries[0].expr == "gamma_request_rate"
