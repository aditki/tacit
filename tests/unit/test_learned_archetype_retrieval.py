from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.archetypes.templates import get_archetypes_by_learning_context
from dashforge.models.schemas import ArchetypeMatch, Intent, SignalType


def test_learned_archetype_can_match_service_without_live_metric_catalog(monkeypatch):
    falco_arch = InvestigationArchetype(
        id="learned_falco_memory",
        name="Falco Pod Memory",
        description="Falco pods running high memory",
        problem_types=["falco_memory"],
        required_metrics=["falco_container_memory_bytes"],
        required_signals=["container_memory_usage"],
        signal_bindings={"container_memory_usage": "falco_container_memory_bytes"},
        tags=["falco", "kubernetes", "memory"],
        panels=[
            PanelTemplate(
                title="Falco memory",
                queries=[
                    QueryTemplate(
                        expr="falco_container_memory_bytes{service_filter}",
                        legend_format="{{pod}}",
                    )
                ],
            )
        ],
    )
    monkeypatch.setattr("dashforge.archetypes.templates.ALL_ARCHETYPES", [falco_arch])

    intent = Intent(
        summary="falco pods running high on memory",
        domain="infrastructure",
        services=["falco"],
        signals=[SignalType.METRICS],
        keywords=["falco", "pods", "memory"],
        timerange="1h",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.9)],
    )

    matches = get_archetypes_by_learning_context(intent, [], min_confidence=0.35)

    assert matches
    assert matches[0][0] == falco_arch
    assert matches[0][1] >= 0.35
