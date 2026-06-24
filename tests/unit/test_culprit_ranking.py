from tacit.archetypes.schema import InvestigationArchetype
from tacit.culprit_ranking import rank_culprits
from tacit.models.schemas import (
    ArchetypeMatch,
    CulpritRankingMode,
    DashboardSpec,
    EvidenceObservation,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    PanelQuery,
    PanelSpec,
    SignalType,
)


def _intent() -> Intent:
    return Intent(
        summary="checkout is slow",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency", "database"],
        timerange="1h",
        problem_type="latency_investigation",
        archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
    )


def _archetype() -> InvestigationArchetype:
    return InvestigationArchetype(
        id="checkout-db-latency",
        name="Checkout DB Latency",
        problem_types=["latency_investigation"],
        required_signals=["db_query_latency"],
        signal_bindings={"db_query_latency": "db_query_duration_seconds"},
        panels=[],
    )


def _dashboard() -> DashboardSpec:
    return DashboardSpec(
        title="Checkout Investigation",
        panels=[
            PanelSpec(
                title="DB latency",
                queries=[
                    PanelQuery(
                        expr="rate(db_query_duration_seconds_sum[5m])",
                        datasource_uid="prom",
                        datasource_type="prometheus",
                        query_language="promql",
                        validation_status="ok",
                        validation_has_data=True,
                    )
                ],
            )
        ],
    )


def _requirement() -> EvidenceRequirement:
    return EvidenceRequirement(
        id="checkout-db-latency:1",
        evidence_type="semantic_signal",
        signal_type="db_query_latency",
        default_metric="db_query_duration_seconds",
        service_scope=["checkout"],
        source="checkout-db-latency",
    )


def test_ranking_becomes_telemetry_evidenced_when_observation_survives():
    requirement = _requirement()
    resolution = EvidenceResolution(
        requirement_id=requirement.id,
        status=EvidenceResolutionStatus.RESOLVED,
        reason_code="live_signal_resolved",
        metric="db_query_duration_seconds",
        datasource_uid="prom",
        datasource_type="prometheus",
        query_language="promql",
    )
    observation = EvidenceObservation(
        requirement_id=requirement.id,
        resolution_metric="db_query_duration_seconds",
        panel_title="DB latency",
        query="rate(db_query_duration_seconds_sum[5m])",
        datasource_uid="prom",
        valid_query=True,
        non_empty=True,
        survived=True,
    )

    ranking = rank_culprits(
        intent=_intent(),
        dashboard_spec=_dashboard(),
        ranked_archetypes=[(_archetype(), 0.9)],
        evidence_requirements=[requirement],
        evidence_resolutions=[resolution],
        evidence_observations=[observation],
    )

    assert ranking.mode == CulpritRankingMode.TELEMETRY_EVIDENCED
    assert ranking.abstained is False
    assert ranking.telemetry_status == "supported"
    assert ranking.candidates[0].suspect == "Checkout Database"
    assert ranking.candidates[0].suspect_type == "datastore"
    assert ranking.candidates[0].runtime_evidence == [
        "Observed db_query_latency via db_query_duration_seconds in 'DB latency'"
    ]


def test_ranking_abstains_when_only_contextual_or_missing_evidence_exists():
    requirement = _requirement()
    resolution = EvidenceResolution(
        requirement_id=requirement.id,
        status=EvidenceResolutionStatus.UNRESOLVED,
        reason_code="no_compatible_live_signal",
    )
    observation = EvidenceObservation(
        requirement_id=requirement.id,
        rejection_reason="no_compatible_live_signal",
    )

    ranking = rank_culprits(
        intent=_intent(),
        dashboard_spec=DashboardSpec(title="No Data", panels=[]),
        ranked_archetypes=[(_archetype(), 0.9)],
        evidence_requirements=[requirement],
        evidence_resolutions=[resolution],
        evidence_observations=[observation],
    )

    assert ranking.mode == CulpritRankingMode.CONTEXTUAL
    assert ranking.abstained is True
    assert ranking.abstention_reason == "no_supported_runtime_evidence"
    assert ranking.candidates[0].suspect == "checkout"
    assert ranking.candidates[0].runtime_evidence == []
    assert any("no_compatible_live_signal" in item for item in ranking.candidates[1].missing_evidence)


def test_ranking_skips_when_there_are_no_candidates():
    ranking = rank_culprits(
        intent=Intent(summary="", domain="", services=[], signals=[SignalType.METRICS]),
        dashboard_spec=DashboardSpec(title="Empty", panels=[]),
        ranked_archetypes=[],
        evidence_requirements=[],
        evidence_resolutions=[],
        evidence_observations=[],
    )

    assert ranking.abstained is True
    assert ranking.abstention_reason == "no_contextual_or_runtime_candidates"
    assert ranking.candidates == []
