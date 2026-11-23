"""Bind the rejection gate to every original YAML and Python query slot."""

import hashlib
import json
from pathlib import Path

import pytest
import yaml

from tacit.archetypes import templates
from tacit.archetypes.engine import UnsupportedSignalFlowTranslation, compile_archetype
from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.models.schemas import Intent, MetricEntry

ROOT = Path(__file__).resolve().parents[2]
INVENTORY = json.loads((ROOT / "tests/eval/fixtures/acquisition_signalflow_slots_v1.json").read_text())
UNSUPPORTED = [slot for slot in INVENTORY["slots"] if slot["state"] == "unsupported_translation"]


def _original_archetypes():
    packaged = yaml.safe_load((ROOT / "tacit/data/archetypes.yaml").read_text())
    return {
        "yaml": [InvestigationArchetype.model_validate(raw) for raw in packaged["archetypes"]],
        "python": [
            templates.LATENCY_INVESTIGATION,
            templates.ERROR_SPIKE,
            templates.GOLDEN_SIGNALS,
            templates.RESOURCE_SATURATION,
        ],
    }


def _original_slots():
    return {
        f"{kind}/{archetype.id}/p{panel_index}/q{query_index}": (archetype, panel, query)
        for kind, archetypes in _original_archetypes().items()
        for archetype in archetypes
        for panel_index, panel in enumerate(archetype.panels, 1)
        for query_index, query in enumerate(panel.queries, 1)
    }


def _catalog():
    return [
        MetricEntry(
            name=name,
            datasource_uid="signalflow-fixture",
            datasource_name="SignalFlow fixture",
            datasource_type="signalfx",
            query_language="signalflow",
            dimensions=["service={svc}", "container={svc}"],
        )
        for name in ("inventory_padding_metric", "http_requests_total")
    ]


def _isolate_original_slot(slot):
    """Preserve original positions while preventing earlier slots masking this one."""
    archetype, panel, query = _original_slots()[slot["slot_id"]]
    preceding_panels = [
        PanelTemplate(title=f"Empty prior panel {index}", queries=[]) for index in range(1, slot["panel_index"])
    ]
    preceding_queries = [QueryTemplate(expr="inventory_padding_metric") for _ in range(1, slot["query_index"])]
    selected_panel = panel.model_copy(update={"queries": [*preceding_queries, query.model_copy(deep=True)]})
    return archetype.model_copy(
        deep=True,
        update={
            "panels": [*preceding_panels, selected_panel],
            # Resolve no different names before checking the frozen template.
            "required_signals": [],
            "signal_bindings": {},
        },
    )


def test_frozen_inventory_covers_all_228_current_template_slots():
    assert INVENTORY["schema"] == "tacit.signalflow-slot-inventory/1"
    assert INVENTORY["source_head"] == "49816476f5d11422922b002ec3762fa1620bf616"
    assert INVENTORY["slot_count"] == 228
    assert INVENTORY["affected_slot_count"] == len(UNSUPPORTED) == 103
    assert INVENTORY["native_alternatives_approved"] == 0
    # The compiler is intentionally changed by this work. Read-only recipe
    # sources, unlike the old compiler digest recorded for provenance, must
    # remain exactly bound to the approved inventory.
    for source in ("tacit/data/archetypes.yaml", "tacit/archetypes/templates.py"):
        assert hashlib.sha256((ROOT / source).read_bytes()).hexdigest() == INVENTORY["source_sha256"][source]
    slots = _original_slots()
    assert len(slots) == len(INVENTORY["slots"]) == 228
    assert set(slots) == {slot["slot_id"] for slot in INVENTORY["slots"]}
    assert sum(identity.startswith("yaml/") for identity in slots) == 198
    assert sum(identity.startswith("python/") for identity in slots) == 30
    for slot in INVENTORY["slots"]:
        archetype, panel, query = slots[slot["slot_id"]]
        assert archetype.id == slot["archetype_id"]
        assert panel.title == slot["panel"]
        assert query.query_language == slot["query_language"]
        assert query.expr == slot["original_expression"]
        assert hashlib.sha256(query.expr.encode()).hexdigest() == slot["expression_sha256"]
        assert slot["required_survival"] is True
        assert slot["native_alternative"] is slot["alternative_fixture"] is None
        assert slot["required_fixture_id"] == "signalflow-slot/" + slot["slot_id"]


@pytest.mark.parametrize("slot", UNSUPPORTED, ids=lambda slot: slot["slot_id"])
def test_every_affected_original_slot_rejects_at_actual_compiler_boundary(slot):
    assert slot["selected_run_policy"] == "fail_before_publish"
    with pytest.raises(UnsupportedSignalFlowTranslation) as failure:
        compile_archetype(
            _isolate_original_slot(slot),
            Intent(summary="Inspect svc", domain="application", services=["svc"]),
            _catalog(),
            target_language="signalflow",
            signal_store=None,
        )
    assert failure.value.reason in slot["rejected_branches"]
    assert failure.value.reason_code == "signalflow_translation_unsupported"
    assert failure.value.slot == f"{slot['archetype_id']}/{slot['panel_index']}/{slot['query_index']}"


@pytest.mark.parametrize("kind", ["yaml", "python"])
def test_supported_original_request_rate_slot_preserves_operand_filter_and_grouping(kind):
    slot = next(slot for slot in INVENTORY["slots"] if slot["slot_id"] == f"{kind}/latency_investigation/p1/q1")
    assert slot["state"] == "unchanged_existing_path"
    compiled = compile_archetype(
        _isolate_original_slot(slot),
        Intent(summary="Inspect svc", domain="application", services=["svc"]),
        _catalog(),
        target_language="signalflow",
        signal_store=None,
    )
    assert len(compiled.panels) == len(compiled.panels[0].queries) == 1
    query = compiled.panels[0].queries[0]
    assert query.datasource_uid == "signalflow-fixture"
    assert query.query_language == "signalflow"
    assert query.expr == (
        "data('http_requests_total', filter=filter('service', 'svc'), rollup='rate')"
        ".sum(by=['status']).publish(label='{{status}}')"
    )
