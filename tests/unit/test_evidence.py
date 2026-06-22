from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.evidence import (
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
