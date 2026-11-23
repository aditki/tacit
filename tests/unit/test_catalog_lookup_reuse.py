"""Operation-owned catalog lookup admission is bounded and preserves provenance."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from threading import Barrier
from types import SimpleNamespace

import pytest

import tacit.catalog as catalog_module
from tacit.agents.providers.base import TokenUsage
from tacit.archetypes.engine import blend_archetypes, compile_archetype
from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.catalog import (
    CatalogNormalization,
    MetricCatalogResult,
    build_acquisition_capture,
    capture_selection,
    select_query_target,
)
from tacit.config import Settings
from tacit.evidence import resolve_requirements_for_archetype, resolve_requirements_for_archetypes
from tacit.evidence_artifacts import build_evidence_gap_dashboard, build_symptom_evidence_dashboard
from tacit.models.schemas import DashboardSpec, EvidenceRequirement, Intent, MetricEntry, MetricsDiscoveryResult
from tacit.pipeline.stages.freeform import build_freeform_dashboard


def metric(name="requests", uid="owner", **updates):
    return MetricEntry(
        name=name,
        datasource_uid=uid,
        datasource_name=uid,
        datasource_type="prometheus",
        query_language="promql",
    ).model_copy(update=updates)


def archetype(identifier="lookup", start=0, count=10):
    names = [f"metric_{i}" for i in range(start, start + count)]
    return InvestigationArchetype(
        id=identifier,
        name=identifier,
        problem_types=["resource_saturation"],
        required_metrics=names,
        panels=[PanelTemplate(title=name, queries=[QueryTemplate(expr=name)]) for name in names],
    )


def intent():
    return Intent(summary="inspect metrics", domain="infrastructure", problem_type="resource_saturation")


def count_normalizations(monkeypatch):
    calls = []
    original = catalog_module.normalize_catalog

    def counted(entries):
        calls.append(1)
        return original(entries)

    monkeypatch.setattr(catalog_module, "normalize_catalog", counted)
    return calls


def test_ten_query_compilation_admits_5000_fact_catalog_once(monkeypatch):
    entries = [metric(f"metric_{i}") for i in range(5000)]
    calls = count_normalizations(monkeypatch)

    dashboard = compile_archetype(archetype(), intent(), entries)

    assert len(dashboard.panels) == 10
    assert {query.datasource_uid for panel in dashboard.panels for query in panel.queries} == {"owner"}
    assert len(calls) == 1


def test_blended_compilation_shares_admission_across_archetypes(monkeypatch):
    entries = [metric(f"metric_{i}") for i in range(12)]
    ranked = [(archetype("primary", count=3), 0.95), (archetype("secondary", start=3, count=3), 0.9)]
    calls = count_normalizations(monkeypatch)

    dashboard = blend_archetypes(ranked, intent(), entries)

    assert len(dashboard.panels) == 6
    assert len(calls) == 1


@pytest.mark.parametrize("multiple", [False, True])
def test_declared_evidence_reuses_admitted_catalog_for_all_requirements(monkeypatch, multiple):
    entries = [metric(f"metric_{i}") for i in range(20)]
    calls = count_normalizations(monkeypatch)
    if multiple:
        requirements, resolutions = resolve_requirements_for_archetypes(
            [(archetype("a", count=5), 0.9), (archetype("b", start=5, count=5), 0.8)],
            intent(),
            entries,
        )
    else:
        requirements, resolutions = resolve_requirements_for_archetype(archetype(), intent(), entries)

    assert len(requirements) == len(resolutions) == 10
    assert {resolution.datasource_uid for resolution in resolutions} == {"owner"}
    assert len(calls) == 1


def test_admitted_lookup_does_no_whole_catalog_validation_copy_or_hash_per_query(monkeypatch):
    entries = [metric(f"metric_{i}") for i in range(5000)]
    admitted = catalog_module.QueryTargetCatalog(entries)
    calls = {"copy": 0, "validate": 0, "hash": 0}
    original_copy = MetricEntry.model_copy
    original_validate = catalog_module._validate_metric
    original_hash = catalog_module.acquisition_entry_id

    def copied(self, *args, **kwargs):
        calls["copy"] += 1
        return original_copy(self, *args, **kwargs)

    def validated(raw):
        calls["validate"] += 1
        return original_validate(raw)

    def hashed(entry):
        calls["hash"] += 1
        return original_hash(entry)

    monkeypatch.setattr(MetricEntry, "model_copy", copied)
    monkeypatch.setattr(catalog_module, "_validate_metric", validated)
    monkeypatch.setattr(catalog_module, "acquisition_entry_id", hashed)
    for i in range(10):
        selected = select_query_target(admitted, [f"metric_{i}"], "promql")
        assert selected.target.datasource_uid == "owner"
    assert calls["copy"] == 0
    assert calls["validate"] == 0
    assert calls["hash"] <= 20


def test_lookup_is_an_admitted_snapshot_and_outputs_remain_defensive():
    original = metric(unit="seconds", dimensions=["service={checkout}"])
    entries = [original]
    admitted = catalog_module.QueryTargetCatalog(entries)
    original.datasource_uid = "changed"
    original.unit = "bytes"
    original.dimensions.clear()
    entries.clear()

    selected = select_query_target(admitted, ["requests"], "promql")
    assert selected.target.datasource_uid == "owner"
    selected.target.datasource_uid = "forged"
    assert select_query_target(admitted, ["requests"], "promql").target.datasource_uid == "owner"
    with pytest.raises((FrozenInstanceError, AttributeError, TypeError)):
        admitted.conflicts = ()


def test_constructible_unadmitted_normalization_cannot_hide_conflicts():
    entries = [metric(unit="seconds"), metric(unit="bytes")]
    forged = CatalogNormalization(MetricCatalogResult(entries), ())
    with pytest.raises((TypeError, ValueError)):
        catalog_module.QueryTargetCatalog(forged)
    with pytest.raises((TypeError, ValueError)):
        select_query_target(forged, ["requests"], "promql")
    with pytest.raises(TypeError):
        catalog_module.QueryTargetCatalog(entries, conflicts=())
    admitted = catalog_module.QueryTargetCatalog(entries)
    assert select_query_target(admitted, ["requests"], "promql").reason == "metadata_conflict"


def test_index_preserves_full_catalog_conflicts_order_and_capture_bytes():
    entries = [
        metric(unit="seconds"),
        metric(unit="bytes"),
        metric("other", datasource_name="different-source-name"),
        metric("requests", uid="second"),
        metric("requests", query_language="signalflow", datasource_type="signalfx", datasource_name="owner"),
    ]
    admitted = catalog_module.QueryTargetCatalog(entries)
    reversed_view = catalog_module.QueryTargetCatalog(reversed(entries))
    for names, language, uid in [
        (["requests"], "promql", None),
        (["requests"], "signalflow", None),
        (["absent"], "promql", None),
        (["requests"], "promql", "second"),
    ]:
        baseline = select_query_target(entries, names, language, uid)
        selected = select_query_target(admitted, names, language, uid)
        assert selected == baseline == select_query_target(reversed_view, names, language, uid)
        baseline_decision = capture_selection(
            baseline,
            consumer="archetype",
            subject_ref="a/1/1",
            required_metrics=names,
            query_language=language,
            requested_uid=uid,
        )
        decision = capture_selection(
            selected,
            consumer="archetype",
            subject_ref="a/1/1",
            required_metrics=names,
            query_language=language,
            requested_uid=uid,
        )
        assert build_acquisition_capture(entries, selections=[baseline_decision]) == build_acquisition_capture(
            entries, selections=[decision]
        )


def test_no_shared_cache_between_distinct_catalogs():
    first = catalog_module.QueryTargetCatalog([metric(uid="tenant-a")])
    second = catalog_module.QueryTargetCatalog([metric(uid="tenant-b")])
    assert select_query_target(first, ["requests"], "promql").target.datasource_uid == "tenant-a"
    assert select_query_target(second, ["requests"], "promql").target.datasource_uid == "tenant-b"
    assert select_query_target([metric(uid="fresh")], ["requests"], "promql").target.datasource_uid == "fresh"


def test_nested_operation_rejects_view_for_a_different_raw_catalog():
    entries = [metric("metric_0")]
    foreign = catalog_module.QueryTargetCatalog([metric("metric_0", uid="foreign")])
    with pytest.raises(catalog_module.AcquisitionCaptureInvalidError, match="ownership_catalog_source"):
        compile_archetype(archetype(count=1), intent(), entries, _ownership_catalog=foreign)


def test_prepared_factory_rejects_unadmitted_duck_typed_view():
    entries = [metric("metric_0")]
    forged = SimpleNamespace(_source=entries)
    with pytest.raises(catalog_module.AcquisitionCaptureInvalidError, match="ownership_catalog_source"):
        catalog_module.query_target_catalog(entries, _prepared=forged)


def test_independent_concurrent_operations_never_share_ownership_state():
    ready = Barrier(2)

    def resolve(uid):
        admitted = catalog_module.QueryTargetCatalog([metric(f"metric_{i}", uid=uid) for i in range(10)])
        ready.wait(timeout=10)
        return [select_query_target(admitted, [f"metric_{i}"], "promql").target.datasource_uid for i in range(10)]

    with ThreadPoolExecutor(max_workers=2) as workers:
        first = workers.submit(resolve, "tenant-a")
        second = workers.submit(resolve, "tenant-b")
        assert first.result(timeout=10) == ["tenant-a"] * 10
        assert second.result(timeout=10) == ["tenant-b"] * 10


@pytest.mark.parametrize("gap", [False, True])
def test_rescue_builders_share_admission_and_use_proved_owner_after_mutable_callback(monkeypatch, gap):
    signals = ("cpu_usage", "memory_usage") if gap else ("request_latency", "request_rate")
    entries = [metric(f"metric_{i}", dimensions=["service={checkout}"]) for i in range(2)]
    requirements = [
        EvidenceRequirement(id=f"r{i}", evidence_type="semantic_signal", signal_type=signal)
        for i, signal in enumerate(signals)
    ]

    def resolve(signal, *_args, **_kwargs):
        entry = entries[signals.index(signal)].model_copy(update={"datasource_uid": "unproved-owner"})
        return [SimpleNamespace(entry=entry, confidence=1.0, governance_ref=None, knowledge_revision_ref=None)]

    calls = count_normalizations(monkeypatch)
    builder = build_evidence_gap_dashboard if gap else build_symptom_evidence_dashboard
    dashboard, resolutions = builder(
        requirements,
        [],
        intent().model_copy(update={"services": ["checkout"]}),
        catalog=entries,
        target_language="promql",
        timerange="1h",
        signal_store=SimpleNamespace(resolve_signal_details=resolve),
    )
    assert len(resolutions) == len(dashboard.panels) == 2
    assert {resolution.datasource_uid for resolution in resolutions} == {"owner"}
    assert {query.datasource_uid for panel in dashboard.panels for query in panel.queries} == {"owner"}
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_freeform_reuses_admitted_owner_catalog_across_selected_metrics(monkeypatch):
    entries = [metric(f"metric_{i}") for i in range(10)]
    discovery = MetricsDiscoveryResult.model_validate(
        {
            "metrics": [
                {
                    "metric_name": entry.name,
                    "datasource_uid": entry.datasource_uid,
                    "datasource_name": entry.datasource_name,
                }
                for entry in entries
            ]
        }
    )

    async def discover(*_args, **_kwargs):
        return discovery, TokenUsage()

    async def build(*_args, **_kwargs):
        return DashboardSpec(title="selected", panels=[]), TokenUsage()

    monkeypatch.setattr("tacit.pipeline.stages.freeform.discover_metrics", discover)
    monkeypatch.setattr("tacit.pipeline.stages.freeform.build_dashboard", build)
    monkeypatch.setattr("tacit.pipeline.stages.freeform.prerank_metrics", lambda _intent, catalog, **_kwargs: catalog)
    deps = SimpleNamespace(
        settings=Settings(_env_file=None),
        feedback_store_factory=lambda: None,
        llm_provider_factory=lambda: object(),
        llm_cache=SimpleNamespace(get=lambda _key: None, set=lambda *_args: None),
        cache_key_factory=lambda *parts: ":".join(str(part) for part in parts),
    )
    calls = count_normalizations(monkeypatch)
    result = await build_freeform_dashboard(
        intent=intent(),
        metric_catalog=entries,
        context_chunks=[],
        deps=deps,
        recorder=SimpleNamespace(),
        timings={},
        started_at=0,
    )
    assert result.dashboard_spec is not None
    assert len(result.selection_subjects) == 10
    assert len(calls) == 1
