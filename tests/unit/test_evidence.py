from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.evidence import (
    contributing_archetypes,
    observe_evidence,
    requirements_for_archetype,
    resolve_requirements_for_archetype,
    summarize_evidence,
)
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
from dashforge.signals import SignalStore


def _intent() -> Intent:
    return Intent(
        summary="checkout resource pressure",
        domain="infrastructure",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["cpu", "memory"],
        timerange="1h",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=1.0)],
    )


def _metric(name: str) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid="gamma",
        datasource_name="GAMMA",
        datasource_type="prometheus",
        query_language="promql",
        metric_type="counter" if "cpu" in name else "gauge",
        dimensions=["service={checkout}"],
    )


def _resource_archetype() -> InvestigationArchetype:
    return InvestigationArchetype(
        id="resource-saturation",
        name="Resource saturation",
        problem_types=["resource_saturation"],
        required_signals=["cpu_usage", "memory_usage"],
        signal_bindings={
            "cpu_usage": "container_cpu_usage_seconds_total",
            "memory_usage": "container_memory_working_set_bytes",
        },
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


def test_evidence_requirements_are_declared_once_per_archetype_signal():
    requirements = requirements_for_archetype(_resource_archetype(), _intent())

    assert [(req.signal_type, req.default_metric) for req in requirements] == [
        ("cpu_usage", "container_cpu_usage_seconds_total"),
        ("memory_usage", "container_memory_working_set_bytes"),
    ]
    assert all(req.priority == "critical" for req in requirements)
    assert all(req.service_scope == ["checkout"] for req in requirements)


def test_evidence_resolves_prefixed_live_metrics(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.load_from_yaml()
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    requirements, resolutions = resolve_requirements_for_archetype(
        _resource_archetype(),
        _intent(),
        [
            _metric("gamma_container_cpu_usage_seconds_total"),
            _metric("gamma_container_memory_working_set_bytes"),
        ],
    )

    summary = summarize_evidence(requirements, resolutions, [])

    assert summary["critical_resolution_recall"] == 1.0
    assert {resolution.metric for resolution in resolutions} == {
        "gamma_container_cpu_usage_seconds_total",
        "gamma_container_memory_working_set_bytes",
    }
    assert all(resolution.reason_code == "live_signal_resolved" for resolution in resolutions)


def test_evidence_resolution_includes_native_query_languages():
    archetype = InvestigationArchetype(
        id="learned-cloudwatch",
        name="Learned CloudWatch",
        problem_types=["elb"],
        required_metrics=["HTTPCode_ELB_5XX"],
        panels=[
            PanelTemplate(
                title="ELB 5xx",
                queries=[
                    QueryTemplate(
                        expr="HTTPCode_ELB_5XX",
                        query_language="cloudwatch",
                        datasource_type="cloudwatch",
                    )
                ],
            )
        ],
        tags=["learned"],
    )
    catalog = [
        MetricEntry(
            name="HTTPCode_ELB_5XX",
            datasource_uid="cloudwatch",
            datasource_name="CloudWatch",
            datasource_type="cloudwatch",
            query_language="cloudwatch",
        )
    ]

    _, resolutions = resolve_requirements_for_archetype(archetype, _intent(), catalog, target_language="promql")

    assert resolutions[0].status == "resolved"
    assert resolutions[0].datasource_uid == "cloudwatch"


def test_signal_bound_evidence_mirrors_binder_top_match_on_tie(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.register_signal_type("custom_latency", category="latency")
    store.add_mapping("custom_latency", "*latency*", confidence=0.9)
    monkeypatch.setattr("dashforge.signals.get_signal_store", lambda: store)
    archetype = InvestigationArchetype(
        id="learned-latency",
        name="Learned Latency",
        problem_types=["latency"],
        required_signals=["custom_latency"],
        signal_bindings={"custom_latency": "default_latency_seconds"},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="default_latency_seconds")])],
        tags=["learned"],
    )
    catalog = [
        _metric("service_a_latency_seconds"),
        _metric("service_b_latency_seconds"),
    ]

    _, resolutions = resolve_requirements_for_archetype(archetype, _intent(), catalog)

    assert resolutions[0].status == "resolved"
    assert resolutions[0].metric == "service_a_latency_seconds"


def test_default_metric_with_multiple_owners_abstains_from_evidence_owner():
    archetype = InvestigationArchetype(
        id="shared",
        name="Shared",
        problem_types=["shared"],
        required_metrics=["shared_metric_total"],
        panels=[PanelTemplate(title="Shared", queries=[QueryTemplate(expr="shared_metric_total")])],
    )
    catalog = [
        MetricEntry(
            name="shared_metric_total",
            datasource_uid="prom-a",
            datasource_name="Prom A",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="shared_metric_total",
            datasource_uid="prom-b",
            datasource_name="Prom B",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]

    _, resolutions = resolve_requirements_for_archetype(archetype, _intent(), catalog)

    assert resolutions[0].status == "unresolved"
    assert resolutions[0].reason_code == "ambiguous_default_metric_owner"


def test_evidence_only_tracks_archetypes_that_contributed_panels():
    primary = _resource_archetype()
    omitted = InvestigationArchetype(
        id="omitted",
        name="Omitted Secondary",
        problem_types=["omitted"],
        required_metrics=["omitted_metric"],
        panels=[PanelTemplate(title="Omitted Panel", queries=[QueryTemplate(expr="omitted_metric")])],
    )
    dashboard = DashboardSpec(
        title="Compiled",
        panels=[
            PanelSpec(
                title="CPU",
                queries=[PanelQuery(expr="rate(container_cpu_usage_seconds_total[5m])", datasource_uid="gamma")],
            )
        ],
    )

    contributed = contributing_archetypes([(primary, 0.9), (omitted, 0.8)], dashboard)

    assert [archetype.id for archetype, _ in contributed] == ["resource-saturation"]


def test_evidence_includes_secondary_panels_with_existing_rows():
    primary = _resource_archetype()
    secondary = InvestigationArchetype(
        id="secondary-latency",
        name="Secondary Latency",
        problem_types=["latency"],
        required_metrics=["latency_metric"],
        panels=[
            PanelTemplate(
                title="Latency",
                row="Application",
                queries=[QueryTemplate(expr="latency_metric")],
            )
        ],
    )
    dashboard = DashboardSpec(
        title="Compiled",
        panels=[
            PanelSpec(
                title="CPU",
                queries=[PanelQuery(expr="rate(container_cpu_usage_seconds_total[5m])", datasource_uid="gamma")],
            ),
            PanelSpec(
                title="Latency",
                row="Application",
                queries=[PanelQuery(expr="latency_metric", datasource_uid="gamma")],
            ),
        ],
    )

    contributed = contributing_archetypes([(primary, 0.9), (secondary, 0.8)], dashboard)

    assert [archetype.id for archetype, _ in contributed] == ["resource-saturation", "secondary-latency"]


def test_evidence_observations_measure_survival_after_validation():
    requirements = requirements_for_archetype(_resource_archetype(), _intent())
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_container_cpu_usage_seconds_total",
        ),
        EvidenceResolution(
            requirement_id=requirements[1].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="gamma_container_memory_working_set_bytes",
        ),
    ]
    pre_validation = DashboardSpec(
        title="Resource",
        panels=[
            PanelSpec(
                title="CPU",
                queries=[PanelQuery(expr="rate(gamma_container_cpu_usage_seconds_total[5m])", datasource_uid="gamma")],
            ),
            PanelSpec(
                title="Memory",
                queries=[PanelQuery(expr="gamma_container_memory_working_set_bytes", datasource_uid="gamma")],
            ),
        ],
    )
    post_validation = DashboardSpec(title="Resource", panels=[pre_validation.panels[0]])

    observations = observe_evidence(requirements, resolutions, pre_validation, post_validation)
    summary = summarize_evidence(requirements, resolutions, observations)

    assert summary["critical_resolution_recall"] == 1.0
    assert summary["critical_survival_recall"] == 0.5
    assert {obs.rejection_reason for obs in observations} == {"", "query_rejected_by_validation"}


def test_skipped_validation_survives_but_does_not_count_as_observed():
    archetype = InvestigationArchetype(
        id="logs",
        name="Logs",
        problem_types=["logs"],
        required_metrics=["log_metric"],
        panels=[PanelTemplate(title="Logs", queries=[QueryTemplate(expr="log_metric")])],
    )
    requirements = requirements_for_archetype(archetype, _intent())
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="log_metric",
        )
    ]
    pre_validation = DashboardSpec(
        title="Logs",
        panels=[
            PanelSpec(
                title="Logs",
                queries=[
                    PanelQuery(
                        expr="log_metric",
                        datasource_uid="loki",
                        datasource_type="loki",
                        query_language="logql",
                    )
                ],
            )
        ],
    )
    post_validation = DashboardSpec(
        title="Logs",
        panels=[
            PanelSpec(
                title="Logs",
                queries=[
                    PanelQuery(
                        expr="log_metric",
                        datasource_uid="loki",
                        datasource_type="loki",
                        query_language="logql",
                        validation_status="skipped",
                        validation_has_data=False,
                    )
                ],
            )
        ],
    )

    observations = observe_evidence(requirements, resolutions, pre_validation, post_validation)
    summary = summarize_evidence(requirements, resolutions, observations)

    assert observations[0].survived is True
    assert observations[0].non_empty is False
    assert observations[0].rejection_reason == "skipped"
    assert summary["critical_survival_recall"] == 0.0


def test_signalfx_exists_validation_survives_but_does_not_count_as_observed():
    archetype = InvestigationArchetype(
        id="signalfx-latency",
        name="SignalFx Latency",
        problem_types=["latency"],
        required_metrics=["request.duration"],
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="data('request.duration').mean()")])],
    )
    requirements = requirements_for_archetype(archetype, _intent())
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
    pre_validation = DashboardSpec(
        title="SignalFx",
        panels=[
            PanelSpec(
                title="Latency",
                queries=[
                    PanelQuery(
                        expr="data('request.duration').mean()",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                        query_language="signalflow",
                    )
                ],
            )
        ],
    )
    post_validation = DashboardSpec(
        title="SignalFx",
        panels=[
            PanelSpec(
                title="Latency",
                queries=[
                    PanelQuery(
                        expr="data('request.duration').mean()",
                        datasource_uid="sfx",
                        datasource_type="signalfx",
                        query_language="signalflow",
                        validation_status="exists",
                        validation_has_data=False,
                    )
                ],
            )
        ],
    )

    observations = observe_evidence(requirements, resolutions, pre_validation, post_validation)
    summary = summarize_evidence(requirements, resolutions, observations)

    assert observations[0].survived is True
    assert observations[0].non_empty is False
    assert observations[0].rejection_reason == "exists"
    assert summary["critical_survival_recall"] == 0.0


def test_evidence_observation_matches_metric_tokens_not_substrings():
    requirements = requirements_for_archetype(_resource_archetype(), _intent())
    resolutions = [
        EvidenceResolution(
            requirement_id=requirements[0].id,
            status="resolved",
            reason_code="live_signal_resolved",
            metric="cpu.utilization",
        )
    ]
    pre_validation = DashboardSpec(
        title="CPU",
        panels=[
            PanelSpec(
                title="CPU",
                queries=[PanelQuery(expr="cpuXutilization", datasource_uid="gamma")],
            )
        ],
    )

    observations = observe_evidence(requirements, resolutions, pre_validation, pre_validation)

    assert observations[0].rejection_reason == "resolved_metric_not_observed_in_queries"
