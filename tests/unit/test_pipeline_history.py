from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.models.schemas import ArchetypeMatch, SignalType
from dashforge.pipeline import _history_archetypes, _history_signals


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
