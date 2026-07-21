from __future__ import annotations

from datetime import UTC, datetime

import pytest
import yaml

from tacit.archetypes.generated import (
    ArchetypeRetrievalMode,
    GeneratedArchetype,
    GeneratedArchetypeQuery,
    GeneratedArchetypeStatus,
    load_experimental_archetypes,
    quarantine_generated_archetype_yaml,
    write_generated_archetype,
)
from tacit.archetypes.schema import InvestigationArchetype
from tacit.archetypes.templates import (
    _is_generated_archetype,
    _load_archetypes_from_yaml,
    append_archetype_to_yaml,
)
from tacit.config import Settings
from tacit.dashboard_ingest import generate_archetype_yaml, register_generated_archetype_if_enabled
from tacit.models.schemas import ArchetypeMatch, Intent, MetricEntry, SignalType
from tacit.pipeline.stages.archetypes import select_archetypes


def _generated(
    *,
    tenant_id: str = "tenant-a",
    service: str = "checkout",
    status: GeneratedArchetypeStatus = GeneratedArchetypeStatus.EXPERIMENTAL,
    environment_refs: frozenset[str] = frozenset(),
    archetype_kind: str = "investigation_dashboard",
    generation_version: str = "generated-archetype-v1",
) -> GeneratedArchetype:
    return GeneratedArchetype(
        id="checkout_generated",
        name="Checkout Generated",
        description="Experimental checkout dashboard",
        problem_types=["resource_saturation"],
        required_metrics=["shared_cpu_metric"],
        panels=[],
        tags=["auto-generated", "learned"],
        retrieval_status=status,
        tenant_id=tenant_id,
        service_refs=frozenset({service}) if service else frozenset(),
        environment_refs=environment_refs,
        archetype_kind=archetype_kind,
        generation_version=generation_version,
        generation_run_id="run-123",
        source_refs=["dashboard:checkout"],
        created_at=datetime.now(UTC),
    )


def _intent(service: str) -> Intent:
    return Intent(
        summary=f"high CPU on {service}",
        domain="application",
        services=[service],
        signals=[SignalType.METRICS],
        keywords=["high", "cpu"],
        timerange="30m",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.95)],
    )


def _catalog() -> list[MetricEntry]:
    return [
        MetricEntry(
            name="shared_cpu_metric",
            datasource_uid="prom",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]


def _settings(tmp_path, *, mode: ArchetypeRetrievalMode) -> Settings:
    return Settings(
        _env_file=None,
        learned_archetypes_retrieval_mode=mode,
        learned_archetypes_quarantine_path=str(tmp_path),
        learned_archetypes_generation_version="generated-archetype-v1",
        learned_archetypes_tenant_id="tenant-a",
    )


def test_generated_archetype_controls_are_disabled_by_default():
    runtime_settings = Settings(_env_file=None)

    assert runtime_settings.learned_archetypes_generation_enabled is False
    assert runtime_settings.learned_archetypes_automatic_registration_enabled is False
    assert runtime_settings.learned_archetypes_normal_retrieval_enabled is False
    assert runtime_settings.learned_archetypes_retrieval_mode == ArchetypeRetrievalMode.CURATED_ONLY


def test_legacy_registration_flag_cannot_mutate_curated_registry(monkeypatch):
    monkeypatch.setattr("tacit.dashboard_ingest.service.settings.learning_auto_register_archetype", True)

    assert register_generated_archetype_if_enabled("archetypes: [{id: generated}]") is False


def test_generated_archetype_is_rejected_from_curated_append(tmp_path):
    artifact = _generated(status=GeneratedArchetypeStatus.QUARANTINED)
    generated_yaml = yaml.safe_dump({"archetypes": [artifact.model_dump(mode="json")]}, sort_keys=False)

    with pytest.raises(ValueError, match="cannot enter the curated registry"):
        append_archetype_to_yaml(generated_yaml, path=tmp_path / "archetypes.yaml")


def test_legacy_generated_entries_are_filtered_when_curated_yaml_loads(tmp_path):
    artifact = _generated(status=GeneratedArchetypeStatus.QUARANTINED)
    curated = InvestigationArchetype(
        id="curated",
        name="Curated",
        problem_types=["curated"],
        panels=[],
    )
    path = tmp_path / "archetypes.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "archetypes": [
                    curated.model_dump(mode="json"),
                    artifact.model_dump(mode="json"),
                ]
            },
            sort_keys=False,
        )
    )

    loaded = _load_archetypes_from_yaml(path)

    assert [item.id for item in loaded] == ["curated"]


@pytest.mark.parametrize("tag", ["learned", "auto-generated"])
def test_curated_archetype_with_an_ordinary_learning_tag_is_preserved(tmp_path, tag):
    curated = InvestigationArchetype(
        id=f"curated_{tag}",
        name="Operator Curated",
        problem_types=["resource_saturation"],
        panels=[],
        tags=[tag, "operator-authored"],
    )
    path = tmp_path / "archetypes.yaml"
    path.write_text(
        yaml.safe_dump({"archetypes": [curated.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )

    loaded = _load_archetypes_from_yaml(path)

    assert [item.id for item in loaded] == [curated.id]
    assert _is_generated_archetype(loaded[0]) is False


def test_quarantine_rejects_generated_artifact_without_service_scope(tmp_path):
    generated_yaml = generate_archetype_yaml(
        {"dashboard_title": "Unscoped", "dashboard_tags": [], "metrics_found": [], "panels": []},
        [],
        tenant_id="tenant-a",
        generation_run_id="run-123",
        source_refs=["dashboard:unscoped"],
    )

    with pytest.raises(ValueError, match="service_ref"):
        quarantine_generated_archetype_yaml(generated_yaml, tmp_path)


def test_quarantine_rejects_generated_artifact_without_tenant_scope(tmp_path):
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Checkout",
            "dashboard_tags": ["service:checkout"],
            "metrics_found": [],
            "panels": [],
        },
        [],
        generation_run_id="run-123",
        source_refs=["dashboard:checkout"],
    )

    with pytest.raises(ValueError, match="tenant_id"):
        quarantine_generated_archetype_yaml(generated_yaml, tmp_path)


def test_quarantine_rejects_generated_artifact_without_id(tmp_path):
    artifact = _generated(status=GeneratedArchetypeStatus.QUARANTINED).model_copy(update={"id": ""})
    generated_yaml = yaml.safe_dump({"generated_archetypes": [artifact.model_dump(mode="json")]}, sort_keys=False)

    with pytest.raises(ValueError, match="id is required"):
        quarantine_generated_archetype_yaml(generated_yaml, tmp_path)


@pytest.mark.parametrize(
    ("field", "message"),
    [("archetype_kind", "archetype_kind"), ("generation_version", "generation_version")],
)
def test_quarantine_rejects_missing_identity_metadata(tmp_path, field, message):
    artifact = _generated(status=GeneratedArchetypeStatus.QUARANTINED).model_copy(update={field: ""})
    generated_yaml = yaml.safe_dump({"generated_archetypes": [artifact.model_dump(mode="json")]}, sort_keys=False)

    with pytest.raises(ValueError, match=message):
        quarantine_generated_archetype_yaml(generated_yaml, tmp_path)


def test_generation_captures_only_explicit_query_service_scope():
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Checkout Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["shared_cpu_metric"],
            "panels": [
                {
                    "title": "CPU",
                    "queries": ['shared_cpu_metric{service="checkout"}'],
                }
            ],
        },
        [],
        tenant_id="tenant-a",
        generation_run_id="run-123",
        source_refs=["dashboard:checkout"],
    )

    generated = yaml.safe_load(generated_yaml)["archetypes"][0]

    assert generated["service_refs"] == ["entity:service:checkout"]


@pytest.mark.parametrize("variable", ["$service", "${service}", "[[service]]"])
def test_generation_excludes_unresolved_grafana_service_variables(variable):
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Checkout Dashboard",
            "dashboard_tags": ["service:checkout"],
            "metrics_found": ["shared_cpu_metric"],
            "panels": [{"title": "CPU", "queries": [f'shared_cpu_metric{{service="{variable}"}}']}],
        },
        [],
        tenant_id="tenant-a",
        generation_run_id="run-123",
        source_refs=["dashboard:checkout"],
    )

    generated = yaml.safe_load(generated_yaml)["archetypes"][0]
    assert generated["service_refs"] == ["entity:service:checkout"]


@pytest.mark.parametrize(
    ("tenant_id", "service"),
    [
        ("tenant-a", "payment"),
        ("tenant-b", "checkout"),
        ("tenant-a", "checkout-api"),
    ],
)
def test_experimental_retrieval_rejects_cross_scope_matches(tmp_path, tenant_id, service):
    write_generated_archetype(_generated(), tmp_path)

    result = load_experimental_archetypes(
        tmp_path,
        GeneratedArchetypeQuery.exact(tenant_id=tenant_id, service_refs=[service]),
    )

    assert result.archetypes == []


@pytest.mark.parametrize(
    ("artifact", "query"),
    [
        (
            _generated(environment_refs=frozenset({"production"})),
            GeneratedArchetypeQuery.exact(
                tenant_id="tenant-a",
                service_refs=["checkout"],
                environment_refs=["staging"],
            ),
        ),
        (
            _generated(archetype_kind="capacity_dashboard"),
            GeneratedArchetypeQuery.exact(tenant_id="tenant-a", service_refs=["checkout"]),
        ),
        (
            _generated(generation_version="generated-archetype-v2"),
            GeneratedArchetypeQuery.exact(tenant_id="tenant-a", service_refs=["checkout"]),
        ),
    ],
    ids=["environment", "kind", "generation-version"],
)
def test_experimental_retrieval_requires_every_scope_dimension(tmp_path, artifact, query):
    write_generated_archetype(artifact, tmp_path)

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.rejected_by_scope == 1


def test_quarantined_artifact_is_not_retrievable_even_with_exact_scope(tmp_path):
    write_generated_archetype(_generated(status=GeneratedArchetypeStatus.QUARANTINED), tmp_path)

    result = load_experimental_archetypes(
        tmp_path,
        GeneratedArchetypeQuery.exact(tenant_id="tenant-a", service_refs=["checkout"]),
    )

    assert result.archetypes == []
    assert result.quarantined == 1


def test_checkout_generated_archetype_is_absent_from_normal_payment_selection(tmp_path):
    write_generated_archetype(_generated(), tmp_path)
    settings = _settings(tmp_path, mode=ArchetypeRetrievalMode.CURATED_ONLY)

    selection = select_archetypes(
        intent=_intent("payment"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=settings,
    )

    assert "checkout_generated" not in {archetype.id for archetype, _ in selection.ranked_archetypes}
    assert selection.context_sources["generated_archetypes"] == 0
    assert selection.experimental_retrieval.files_scanned == 0
    assert selection.unexpected_cross_service_matches == 0


def test_exact_scope_experimental_mode_keeps_generated_archetype_shadow_only(tmp_path):
    write_generated_archetype(_generated(), tmp_path)
    settings = _settings(
        tmp_path,
        mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
    )

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=settings,
    )

    assert "checkout_generated" not in {archetype.id for archetype, _ in selection.ranked_archetypes}
    assert [archetype.id for archetype, _ in selection.shadow_archetypes] == ["checkout_generated"]
    assert selection.context_sources["generated_archetypes"] == 0
    assert selection.context_sources["shadow_generated_archetypes"] == 1
    assert selection.experimental_retrieval.files_scanned == 1
    assert selection.unexpected_cross_service_matches == 0


def test_shadow_candidate_never_enters_authoritative_coverage_ranking(tmp_path, monkeypatch):
    write_generated_archetype(_generated(), tmp_path)
    settings = _settings(tmp_path, mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE)
    ranked_candidate_ids: list[list[str]] = []

    def capture_authoritative_candidates(candidates, *_args, **_kwargs):
        ranked_candidate_ids.append([item[0].id for item in candidates])
        return candidates

    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.rank_archetypes_by_coverage",
        capture_authoritative_candidates,
    )
    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=settings,
    )

    assert selection.experimental_retrieval.archetypes
    assert [archetype.id for archetype, _ in selection.shadow_archetypes] == ["checkout_generated"]
    assert all("checkout_generated" not in candidate_ids for candidate_ids in ranked_candidate_ids)
    assert selection.context_sources["generated_archetypes"] == 0
