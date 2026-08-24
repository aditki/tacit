from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml
from structlog.testing import capture_logs

from tacit.archetypes.generated import (
    ArchetypeRetrievalMode,
    GeneratedArchetype,
    GeneratedArchetypeQuery,
    GeneratedArchetypeStatus,
    load_experimental_archetypes,
    quarantine_generated_archetype_yaml,
    write_generated_archetype,
)
from tacit.archetypes.generated.store import (
    DEFAULT_GENERATED_RETRIEVAL_MAX_FILE_BYTES,
    DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_DEPTH,
    DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_NODES,
    DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALAR_BYTES,
)
from tacit.archetypes.schema import InvestigationArchetype
from tacit.archetypes.templates import (
    _is_generated_archetype,
    _load_archetypes_from_yaml,
    append_archetype_to_yaml,
)
from tacit.config import Settings
from tacit.dashboard_ingest import generate_archetype_yaml, register_generated_archetype_if_enabled
from tacit.errors import AUTHORITY_BOUNDARY_ERRORS, SemanticAuthorizationError
from tacit.knowledge.authorization import KnowledgeAction, enforce_knowledge_action
from tacit.models.schemas import ArchetypeMatch, Intent, MetricEntry, SignalType
from tacit.pipeline import _history_archetypes
from tacit.pipeline.stages.archetypes import select_archetypes
from tacit.runtime_ownership import RuntimeOwnershipError
from tacit.signals.availability import SIGNAL_STORE_UNAVAILABLE
from tacit.tenancy import TenantBoundaryError


def _generated(
    *,
    archetype_id: str = "checkout_generated",
    tenant_id: str = "tenant-a",
    service: str = "checkout",
    status: GeneratedArchetypeStatus = GeneratedArchetypeStatus.EXPERIMENTAL,
    environment_refs: frozenset[str] = frozenset({"production"}),
    archetype_kind: str = "investigation_dashboard",
    generation_version: str = "generated-archetype-v1",
) -> GeneratedArchetype:
    return GeneratedArchetype(
        id=archetype_id,
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


def _intent(service: str, *, environments: list[str] | None = None) -> Intent:
    return Intent(
        summary=f"high CPU on {service}",
        domain="application",
        services=[service],
        environments=["production"] if environments is None else environments,
        signals=[SignalType.METRICS],
        keywords=["high", "cpu"],
        timerange="30m",
        problem_type="resource_saturation",
        archetypes=[ArchetypeMatch(type="resource_saturation", confidence=0.95)],
    )


def _exact_generated_query(
    *,
    tenant_id: str = "tenant-a",
    service_refs: list[str] | None = None,
    environment_refs: list[str] | None = None,
) -> GeneratedArchetypeQuery:
    return GeneratedArchetypeQuery.exact(
        tenant_id=tenant_id,
        service_refs=service_refs or ["checkout"],
        environment_refs=["production"] if environment_refs is None else environment_refs,
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


def _settings(tmp_path, *, mode: ArchetypeRetrievalMode, **updates: object) -> Settings:
    values: dict[str, object] = {
        "learned_archetypes_retrieval_mode": mode,
        "learned_archetypes_quarantine_path": str(tmp_path),
        "learned_archetypes_generation_version": "generated-archetype-v1",
        "learned_archetypes_tenant_id": "tenant-a",
    }
    values.update(updates)
    return Settings.model_validate(values)


def _replace_generated_document(path: Path, artifacts: list[dict[str, Any]]) -> bytes:
    payload = yaml.safe_dump({"generated_archetypes": artifacts}, sort_keys=False).encode()
    path.write_bytes(payload)
    return payload


def test_experimental_retrieval_requires_environment_before_opening_quarantine(
    tmp_path,
    monkeypatch,
):
    opened = False

    def fail_if_opened(_root_path):
        nonlocal opened
        opened = True
        raise AssertionError("quarantine must not be opened without an exact environment scope")

    monkeypatch.setattr(
        "tacit.archetypes.generated.store._open_root_without_symlinks",
        fail_if_opened,
    )

    result = load_experimental_archetypes(
        tmp_path,
        GeneratedArchetypeQuery.exact(
            tenant_id="tenant-a",
            service_refs=["checkout"],
        ),
    )

    assert opened is False
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_environment_scope_required"
    assert result.rejected_by_scope == 1
    assert result.files_scanned == 0


def test_experimental_retrieval_requires_service_before_opening_quarantine(
    tmp_path,
    monkeypatch,
):
    opened = False

    def fail_if_opened(_root_path):
        nonlocal opened
        opened = True
        raise AssertionError("quarantine must not be opened without an exact service scope")

    monkeypatch.setattr(
        "tacit.archetypes.generated.store._open_root_without_symlinks",
        fail_if_opened,
    )

    result = load_experimental_archetypes(
        tmp_path,
        GeneratedArchetypeQuery.exact(
            tenant_id="tenant-a",
            service_refs=[],
            environment_refs=["production"],
        ),
    )

    assert opened is False
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_service_scope_required"
    assert result.rejected_by_scope == 1
    assert result.files_scanned == 0


def test_archetype_selection_reports_missing_service_scope_as_skipped(tmp_path):
    intent = _intent("checkout").model_copy(update={"services": []})

    selection = select_archetypes(
        intent=intent,
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
        ),
        tenant_id="tenant-a",
        signal_store=SIGNAL_STORE_UNAVAILABLE,
    )

    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.status.value == "skipped"
    assert selection.experimental_retrieval.reason_code == "generated_archetype_service_scope_required"
    assert selection.retrieval_reason_code == "generated_archetype_service_scope_required"


def test_archetype_selection_requires_environment_before_generated_retrieval(
    tmp_path,
    monkeypatch,
):
    def fail_if_retrieved(*_args, **_kwargs):
        raise AssertionError("generated retrieval must not run without an exact environment scope")

    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.load_experimental_archetypes",
        fail_if_retrieved,
    )

    selection = select_archetypes(
        intent=_intent("checkout", environments=[]),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
        ),
        tenant_id="tenant-a",
    )

    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.status.value == "skipped"
    assert selection.experimental_retrieval.reason_code == "generated_archetype_environment_scope_required"
    assert selection.experimental_retrieval.rejected_by_scope == 1


def test_generated_archetype_controls_are_disabled_by_default():
    runtime_settings = Settings.model_validate({})

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


def test_quarantine_rejects_generated_artifact_without_environment_scope(tmp_path):
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Checkout",
            "dashboard_tags": ["service:checkout"],
            "metrics_found": [],
            "panels": [],
        },
        [],
        tenant_id="tenant-a",
        generation_run_id="run-123",
        source_refs=["dashboard:checkout"],
    )

    with pytest.raises(ValueError, match="environment_ref"):
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


def test_generated_archetype_tenant_identity_is_case_sensitive_and_isolated(tmp_path):
    upper = _generated(archetype_id="upper-tenant", tenant_id="Acme")
    lower = _generated(archetype_id="lower-tenant", tenant_id="acme")

    assert upper.tenant_id == "Acme"
    assert lower.tenant_id == "acme"

    write_generated_archetype(upper, tmp_path)
    write_generated_archetype(lower, tmp_path)

    upper_result = load_experimental_archetypes(
        tmp_path,
        _exact_generated_query(tenant_id="Acme"),
    )
    lower_result = load_experimental_archetypes(
        tmp_path,
        _exact_generated_query(tenant_id="acme"),
    )

    assert [item.id for item in upper_result.archetypes] == ["upper-tenant"]
    assert [item.id for item in lower_result.archetypes] == ["lower-tenant"]


def test_generated_archetype_runtime_scope_rejects_wildcard_tenant():
    artifact = _generated(tenant_id="*")

    assert "tenant_id must be concrete" in artifact.registration_errors()
    with pytest.raises(ValueError, match="Invalid knowledge tenant"):
        GeneratedArchetypeQuery.exact(tenant_id="*", service_refs=["checkout"])


@pytest.mark.parametrize("tenant_id", [None, "", "   ", "*"])
def test_generated_archetype_query_direct_constructor_requires_concrete_tenant(tenant_id):
    with pytest.raises(ValueError, match="Invalid knowledge tenant"):
        GeneratedArchetypeQuery(
            tenant_id=tenant_id,
            service_refs=frozenset({"entity:service:checkout"}),
        )


def test_generated_archetype_query_rejects_oversized_scope_scalar_before_normalization():
    with pytest.raises(ValueError, match="scope_scalar"):
        GeneratedArchetypeQuery(
            tenant_id="tenant-a",
            service_refs=frozenset({"x" * 1_000_000}),
        )


def test_generated_archetype_query_rejects_oversized_utf8_scope_scalar():
    with pytest.raises(ValueError, match="scope_scalar_bytes"):
        GeneratedArchetypeQuery(
            tenant_id="tenant-a",
            service_refs=frozenset({"\U0001f642" * 600}),
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"tenant_id": "x" * 1_000_000},
        {"service_refs": {"x" * 1_000_000}},
        {"environment_refs": {"x" * 1_000_000}},
    ],
    ids=["tenant", "service", "environment"],
)
def test_generated_archetype_model_rejects_oversized_scope_before_normalization(updates):
    values = _generated().model_dump(mode="python")
    values.update(updates)

    with pytest.raises(ValueError, match="scope_scalar"):
        GeneratedArchetype.model_validate(values)


def test_experimental_selection_does_not_substitute_configured_tenant(tmp_path):
    write_generated_archetype(_generated(), tmp_path)

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
            learned_archetypes_tenant_id="tenant-a",
        ),
        tenant_id=None,
    )

    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.status.value == "skipped"
    assert selection.experimental_retrieval.reason_code == "generated_archetype_concrete_tenant_required"


def test_experimental_selection_rejects_wildcard_request_tenant(tmp_path):
    write_generated_archetype(_generated(), tmp_path)

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
        ),
        tenant_id="*",
    )

    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.status.value == "skipped"
    assert selection.experimental_retrieval.reason_code == "generated_archetype_concrete_tenant_required"


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


def test_quarantine_prevalidates_entire_batch_before_writing(tmp_path):
    valid = _generated(archetype_id="valid-first").model_dump(mode="json")
    invalid = _generated(archetype_id="invalid-second").model_dump(mode="json")
    invalid.pop("name")
    payload = yaml.safe_dump({"generated_archetypes": [valid, invalid]}, sort_keys=False)

    with pytest.raises(ValueError):
        quarantine_generated_archetype_yaml(payload, tmp_path)

    assert not tmp_path.exists() or list(tmp_path.rglob("*.yaml")) == []


def test_quarantine_prevalidates_later_scope_cardinality_before_writing(tmp_path):
    valid = _generated(archetype_id="valid-first").model_dump(mode="json")
    invalid = _generated(archetype_id="invalid-second").model_dump(mode="json")
    invalid["service_refs"] = [f"service-{index}" for index in range(65)]
    payload = yaml.safe_dump({"generated_archetypes": [valid, invalid]}, sort_keys=False)

    with pytest.raises(ValueError, match="service_refs"):
        quarantine_generated_archetype_yaml(payload, tmp_path)

    assert not tmp_path.exists() or list(tmp_path.rglob("*.yaml")) == []


@pytest.mark.parametrize(
    "artifact",
    [
        _generated().model_copy(update={"description": "x" * (DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALAR_BYTES + 1)}),
        _generated().model_copy(
            update={"description": "\U0001f642" * ((DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_SCALAR_BYTES // 4) + 1)}
        ),
        _generated().model_copy(
            update={
                "source_refs": [
                    f"source-{index}:" + ("x" * 60_000)
                    for index in range((DEFAULT_GENERATED_RETRIEVAL_MAX_FILE_BYTES // 60_000) + 2)
                ]
            }
        ),
    ],
    ids=["oversized-scalar", "oversized-utf8-scalar", "oversized-document"],
)
def test_direct_generated_write_rejects_oversized_payload_before_persistence(tmp_path, artifact):
    with pytest.raises(ValueError):
        write_generated_archetype(artifact, tmp_path)

    assert not tmp_path.exists() or list(tmp_path.rglob("*.yaml")) == []


@pytest.mark.parametrize(
    ("shape", "reason_code"),
    [
        ("deep", "generated_archetype_yaml_depth_limit_exceeded"),
        ("wide", "generated_archetype_yaml_node_limit_exceeded"),
    ],
)
def test_direct_generated_write_bounds_bypassed_model_shape_before_persistence(tmp_path, shape, reason_code):
    if shape == "deep":
        nested: object = "source"
        for _ in range(DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_DEPTH + 1):
            nested = [nested]
        source_refs = [nested]
    else:
        source_refs = [[] for _ in range(DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_NODES + 1)]
    artifact = _generated().model_copy(update={"source_refs": source_refs})

    with pytest.raises(ValueError, match=reason_code):
        write_generated_archetype(artifact, tmp_path)

    assert not tmp_path.exists() or list(tmp_path.rglob("*.yaml")) == []


@pytest.mark.parametrize(
    ("shape", "reason_code"),
    [
        ("deep", "generated_archetype_yaml_depth_limit_exceeded"),
        ("wide", "generated_archetype_yaml_node_limit_exceeded"),
    ],
)
def test_quarantine_bounds_yaml_shape_before_persistence(tmp_path, shape, reason_code):
    if shape == "deep":
        nested: object = "source"
        for _ in range(DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_DEPTH + 1):
            nested = [nested]
        document: object = {"generated_archetypes": [nested]}
    else:
        document = {
            "generated_archetypes": [_generated().model_dump(mode="json")],
            "padding": [[] for _ in range(DEFAULT_GENERATED_RETRIEVAL_MAX_YAML_NODES + 1)],
        }
    payload = yaml.safe_dump(document, sort_keys=False)

    with pytest.raises(ValueError, match=reason_code):
        quarantine_generated_archetype_yaml(payload, tmp_path)

    assert not tmp_path.exists() or list(tmp_path.rglob("*.yaml")) == []


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


def test_generation_captures_positive_regex_query_service_scope():
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Checkout Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["shared_cpu_metric"],
            "panels": [
                {
                    "title": "CPU",
                    "queries": ['shared_cpu_metric{service=~"checkout"}'],
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


def test_generation_does_not_treat_multi_service_regex_as_exact_scope():
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Shared Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["shared_cpu_metric"],
            "panels": [
                {
                    "title": "CPU",
                    "queries": ['shared_cpu_metric{service=~"checkout|payments"}'],
                }
            ],
        },
        [],
        tenant_id="tenant-a",
        generation_run_id="run-123",
        source_refs=["dashboard:shared"],
    )

    generated = yaml.safe_load(generated_yaml)["archetypes"][0]

    assert generated["service_refs"] == []


@pytest.mark.parametrize("operator", ["=", "=~"])
@pytest.mark.parametrize("variable", ["$service", "${service}", "[[service]]"])
def test_generation_excludes_unresolved_grafana_service_variables(variable, operator):
    generated_yaml = generate_archetype_yaml(
        {
            "dashboard_title": "Checkout Dashboard",
            "dashboard_tags": ["service:checkout"],
            "metrics_found": ["shared_cpu_metric"],
            "panels": [{"title": "CPU", "queries": [f'shared_cpu_metric{{service{operator}"{variable}"}}']}],
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
        _exact_generated_query(tenant_id=tenant_id, service_refs=[service]),
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
            _exact_generated_query(),
        ),
        (
            _generated(generation_version="generated-archetype-v2"),
            _exact_generated_query(),
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
        _exact_generated_query(),
    )

    assert result.archetypes == []
    assert result.quarantined == 1


def test_experimental_retrieval_enforces_applicable_file_count_after_scope_filtering(tmp_path):
    query = _exact_generated_query()
    write_generated_archetype(_generated(archetype_id="generated-a"), tmp_path)
    write_generated_archetype(_generated(archetype_id="generated-b"), tmp_path)

    at_limit = load_experimental_archetypes(
        tmp_path,
        query,
        max_files=2,
        max_file_bytes=1024 * 1024,
        max_total_bytes=2 * 1024 * 1024,
    )
    assert {item.id for item in at_limit.archetypes} == {"generated-a", "generated-b"}
    assert at_limit.files_discovered == 2
    assert at_limit.files_scanned == 2

    write_generated_archetype(_generated(archetype_id="generated-c"), tmp_path)
    over_limit = load_experimental_archetypes(
        tmp_path,
        query,
        max_files=2,
        max_file_bytes=1024 * 1024,
        max_total_bytes=2 * 1024 * 1024,
    )

    assert over_limit.archetypes == []
    assert over_limit.files_discovered == 3
    assert over_limit.files_scanned == 3
    assert over_limit.rejected_by_limit == 1
    assert over_limit.limit_reason_codes == ("generated_archetype_file_count_limit_exceeded",)


def test_exact_scope_variants_do_not_consume_matching_file_limit(tmp_path):
    query = GeneratedArchetypeQuery.exact(
        tenant_id="tenant-a",
        service_refs=["checkout"],
        environment_refs=["production"],
    )
    variants = [
        _generated(archetype_id="a-staging", environment_refs=frozenset({"staging"})),
        _generated(
            archetype_id="b-wrong-kind",
            environment_refs=frozenset({"production"}),
            archetype_kind="capacity_dashboard",
        ),
        _generated(
            archetype_id="c-wrong-version",
            environment_refs=frozenset({"production"}),
            generation_version="generated-archetype-v2",
        ),
        _generated(archetype_id="z-production", environment_refs=frozenset({"production"})),
    ]
    for artifact in variants:
        write_generated_archetype(artifact, tmp_path)

    result = load_experimental_archetypes(
        tmp_path,
        query,
        max_files=1,
        max_directory_entries=4,
    )

    assert [artifact.id for artifact in result.archetypes] == ["z-production"]
    assert result.files_discovered == 4
    assert result.files_scanned == 4
    assert result.rejected_by_scope == 3


def test_experimental_retrieval_enforces_file_and_aggregate_byte_limits(tmp_path):
    query = _exact_generated_query()
    first = write_generated_archetype(_generated(archetype_id="generated-a"), tmp_path)
    second = write_generated_archetype(_generated(archetype_id="generated-b"), tmp_path)
    first_size = first.stat().st_size
    total_size = first_size + second.stat().st_size

    oversized = load_experimental_archetypes(
        tmp_path,
        query,
        max_files=2,
        max_file_bytes=first_size - 1,
        max_total_bytes=total_size,
    )
    assert oversized.archetypes == []
    assert oversized.files_scanned == 0
    assert oversized.oversized_files == 2
    assert oversized.limit_reason_codes == ("generated_archetype_file_size_limit_exceeded",)

    aggregate_at_limit = load_experimental_archetypes(
        tmp_path,
        query,
        max_files=2,
        max_file_bytes=total_size,
        max_total_bytes=total_size,
    )
    assert len(aggregate_at_limit.archetypes) == 2
    assert aggregate_at_limit.bytes_scanned == total_size

    aggregate_over_limit = load_experimental_archetypes(
        tmp_path,
        query,
        max_files=2,
        max_file_bytes=total_size,
        max_total_bytes=total_size - 1,
    )
    assert aggregate_over_limit.archetypes == []
    assert aggregate_over_limit.files_scanned == 1
    assert aggregate_over_limit.bytes_scanned == first_size
    assert aggregate_over_limit.limit_reason_codes == ("generated_archetype_total_bytes_limit_exceeded",)
    assert aggregate_over_limit.status.value == "skipped"
    assert aggregate_over_limit.reason_code == "generated_archetype_total_bytes_limit_exceeded"


@pytest.mark.parametrize(
    ("limit_name", "settings_name", "reason_code", "with_panels"),
    [
        (
            "max_total_artifacts",
            "learned_archetypes_retrieval_max_total_artifacts",
            "generated_archetype_total_artifact_limit_exceeded",
            False,
        ),
        (
            "max_total_panels",
            "learned_archetypes_retrieval_max_total_panels",
            "generated_archetype_total_panel_limit_exceeded",
            True,
        ),
        (
            "max_total_queries",
            "learned_archetypes_retrieval_max_total_queries",
            "generated_archetype_total_query_limit_exceeded",
            True,
        ),
        (
            "max_results",
            "learned_archetypes_retrieval_max_results",
            "generated_archetype_result_limit_exceeded",
            False,
        ),
    ],
)
def test_experimental_retrieval_enforces_distributed_aggregate_limits(
    tmp_path,
    limit_name,
    settings_name,
    reason_code,
    with_panels,
):
    query = _exact_generated_query()
    paths = [
        write_generated_archetype(_generated(archetype_id="aggregate-a"), tmp_path),
        write_generated_archetype(_generated(archetype_id="aggregate-b"), tmp_path),
    ]
    if with_panels:
        for index, path in enumerate(paths):
            artifact = _generated(archetype_id=f"aggregate-{index}").model_dump(mode="json")
            artifact["panels"] = [
                {
                    "title": f"Panel {index}",
                    "queries": [{"expr": f"metric_{index}"}],
                }
            ]
            _replace_generated_document(path, [artifact])

    at_limit = load_experimental_archetypes(
        tmp_path,
        query,
        **{limit_name: 2},
    )
    over_limit = load_experimental_archetypes(
        tmp_path,
        query,
        **{limit_name: 1},
    )

    assert len(at_limit.archetypes) == 2
    assert at_limit.status.value == "passed"
    assert over_limit.archetypes == []
    assert over_limit.rejected_by_limit == 1
    assert over_limit.limit_reason_codes == (reason_code,)
    assert over_limit.status.value == "skipped"
    assert over_limit.reason_code == reason_code

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
            **{settings_name: 1},
        ),
    )
    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.reason_code == reason_code
    assert selection.retrieval_stage_status == "skipped"


@pytest.mark.parametrize(
    ("limit_name", "limit_value", "reason_code"),
    [
        (
            "max_total_artifacts",
            1,
            "generated_archetype_total_artifact_limit_exceeded",
        ),
        (
            "max_total_panels",
            1,
            "generated_archetype_total_panel_limit_exceeded",
        ),
        (
            "max_total_queries",
            1,
            "generated_archetype_total_query_limit_exceeded",
        ),
    ],
)
def test_schema_invalid_files_still_consume_aggregate_structural_budget(
    tmp_path,
    limit_name,
    limit_value,
    reason_code,
):
    query = _exact_generated_query()
    paths = [
        write_generated_archetype(_generated(archetype_id="a-invalid"), tmp_path),
        write_generated_archetype(_generated(archetype_id="b-invalid"), tmp_path),
    ]
    for index, path in enumerate(paths):
        invalid = _generated(archetype_id=f"invalid-{index}").model_dump(mode="json")
        invalid.pop("created_at")
        invalid["panels"] = [
            {
                "title": f"Invalid panel {index}",
                "queries": [{"expr": f"invalid_metric_{index}"}],
            }
        ]
        _replace_generated_document(path, [invalid])

    limits = {
        "max_total_artifacts": 2,
        "max_total_panels": 2,
        "max_total_queries": 2,
        limit_name: limit_value,
    }
    result = load_experimental_archetypes(tmp_path, query, **limits)

    assert result.archetypes == []
    assert result.status.value == "skipped"
    assert result.reason_code == reason_code
    assert result.limit_reason_codes == (reason_code,)
    assert result.invalid == 1
    assert result.total_artifacts == 1
    assert result.total_panels == 1
    assert result.total_queries == 1


def test_schema_invalid_file_cannot_evade_budget_after_valid_file(tmp_path):
    query = _exact_generated_query()
    write_generated_archetype(_generated(archetype_id="a-valid"), tmp_path)
    invalid_path = write_generated_archetype(_generated(archetype_id="b-invalid"), tmp_path)
    invalid = _generated(archetype_id="b-invalid").model_dump(mode="json")
    invalid.pop("created_at")
    _replace_generated_document(invalid_path, [invalid])

    result = load_experimental_archetypes(
        tmp_path,
        query,
        max_total_artifacts=1,
    )

    assert result.archetypes == []
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_total_artifact_limit_exceeded"
    assert result.total_artifacts == 1


def test_experimental_retrieval_limit_never_changes_curated_selection(tmp_path):
    write_generated_archetype(_generated(archetype_id="generated-a"), tmp_path)
    write_generated_archetype(_generated(archetype_id="generated-b"), tmp_path)

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
            learned_archetypes_retrieval_max_files=1,
        ),
    )

    assert "resource_saturation" in {archetype.id for archetype, _ in selection.ranked_archetypes}
    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.limit_reason_codes == ("generated_archetype_file_count_limit_exceeded",)
    assert selection.context_sources["generated_archetypes"] == 0


def test_experimental_retrieval_bounds_directory_entries_and_rejects_symlinks(tmp_path):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    scoped_directory = generated_path.parent
    (scoped_directory / "ignored.txt").write_text("ignored")
    (scoped_directory / "linked.yaml").symlink_to(generated_path)

    bounded = load_experimental_archetypes(
        tmp_path,
        query,
        max_directory_entries=2,
        max_files=2,
    )

    assert bounded.archetypes == []
    assert bounded.directory_entries_discovered == 3
    assert bounded.files_scanned == 0
    assert bounded.limit_reason_codes == ("generated_archetype_directory_entry_limit_exceeded",)

    accepted = load_experimental_archetypes(
        tmp_path,
        query,
        max_directory_entries=3,
        max_files=2,
    )
    assert [item.id for item in accepted.archetypes] == ["checkout_generated"]
    assert accepted.symlinks_rejected == 1
    assert accepted.files_scanned == 1
    assert accepted.status.value == "partial"
    assert accepted.reason_code == "generated_archetype_scope_path_symlink_rejected"


def test_experimental_retrieval_rejects_intermediate_scope_symlink_escape(tmp_path):
    query = _exact_generated_query()
    outside_root = tmp_path.parent / f"{tmp_path.name}-outside"
    outside_path = write_generated_archetype(_generated(archetype_id="outside-canary"), outside_root)
    outside_tenant_directory = outside_path.parents[1]
    (tmp_path / outside_tenant_directory.name).symlink_to(
        outside_tenant_directory,
        target_is_directory=True,
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.symlinks_rejected == 1
    assert dict(result.reason_counts) == {
        "generated_archetype_scope_path_symlink_rejected": 1,
    }
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_scope_path_symlink_rejected"


def test_generated_write_rejects_symlinked_tenant_directory(tmp_path):
    outside_root = tmp_path / "outside"
    outside_path = write_generated_archetype(_generated(archetype_id="outside"), outside_root)
    outside_tenant = outside_path.parents[1]
    before = {path.name: path.read_bytes() for path in outside_path.parent.glob("*.yaml")}
    quarantine_root = tmp_path / "quarantine"
    quarantine_root.mkdir()
    (quarantine_root / outside_tenant.name).symlink_to(outside_tenant, target_is_directory=True)

    with pytest.raises(OSError):
        write_generated_archetype(_generated(archetype_id="escape-attempt"), quarantine_root)

    assert {path.name: path.read_bytes() for path in outside_path.parent.glob("*.yaml")} == before


def test_generated_write_rejects_symlinked_scope_directory(tmp_path):
    outside_root = tmp_path / "outside"
    outside_path = write_generated_archetype(_generated(archetype_id="outside"), outside_root)
    outside_scope = outside_path.parent
    outside_tenant = outside_scope.parent
    before = {path.name: path.read_bytes() for path in outside_scope.glob("*.yaml")}
    quarantine_root = tmp_path / "quarantine"
    tenant_directory = quarantine_root / outside_tenant.name
    tenant_directory.mkdir(parents=True)
    (tenant_directory / outside_scope.name).symlink_to(outside_scope, target_is_directory=True)

    with pytest.raises(OSError):
        write_generated_archetype(_generated(archetype_id="escape-attempt"), quarantine_root)

    assert {path.name: path.read_bytes() for path in outside_scope.glob("*.yaml")} == before


def test_generated_write_atomically_replaces_target_symlink_without_following_it(tmp_path):
    artifact = _generated(archetype_id="atomic-replacement")
    target = write_generated_archetype(artifact, tmp_path)
    outside = tmp_path / "outside-canary.yaml"
    outside.write_text("outside-canary")
    target.unlink()
    target.symlink_to(outside)

    rewritten = write_generated_archetype(artifact, tmp_path)

    assert rewritten == target
    assert rewritten.is_symlink() is False
    assert outside.read_text() == "outside-canary"
    assert yaml.safe_load(rewritten.read_text())["generated_archetypes"][0]["id"] == "atomic-replacement"


def test_generated_write_rename_failure_preserves_target_and_removes_temporary_file(tmp_path, monkeypatch):
    artifact = _generated(archetype_id="atomic-failure")
    target = write_generated_archetype(artifact, tmp_path)
    original_payload = target.read_bytes()
    monkeypatch.setattr("tacit.archetypes.generated.store._descriptor_write_supported", lambda: True)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store.os.rename",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("rename-failure")),
    )

    with pytest.raises(OSError, match="rename-failure"):
        write_generated_archetype(artifact, tmp_path)

    assert target.read_bytes() == original_payload
    assert list(target.parent.glob("*.tmp")) == []


def test_experimental_retrieval_treats_missing_root_as_clean_no_match(tmp_path):
    query = _exact_generated_query()

    result = load_experimental_archetypes(tmp_path / "missing", query)

    assert result.archetypes == []
    assert result.status.value == "passed"
    assert result.reason_counts == ()


def test_experimental_retrieval_rejects_root_symlink_escape(tmp_path):
    query = _exact_generated_query()
    outside_root = tmp_path / "outside"
    write_generated_archetype(_generated(archetype_id="outside-canary"), outside_root)
    linked_root = tmp_path / "linked-root"
    linked_root.symlink_to(outside_root, target_is_directory=True)

    result = load_experimental_archetypes(linked_root, query)

    assert result.archetypes == []
    assert result.symlinks_rejected == 1
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_scope_path_symlink_rejected"


def test_experimental_retrieval_rejects_symlinked_root_ancestor(tmp_path):
    query = _exact_generated_query()
    outside_parent = tmp_path / "outside-parent"
    outside_root = outside_parent / "quarantine"
    write_generated_archetype(_generated(archetype_id="outside-canary"), outside_root)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(outside_parent, target_is_directory=True)

    result = load_experimental_archetypes(linked_parent / "quarantine", query)

    assert result.archetypes == []
    assert result.symlinks_rejected == 1
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_scope_path_symlink_rejected"


def test_experimental_retrieval_does_not_reopen_candidate_by_path(tmp_path, monkeypatch):
    query = _exact_generated_query()
    candidate = write_generated_archetype(_generated(archetype_id="inside"), tmp_path)
    outside_root = tmp_path.parent / f"{tmp_path.name}-race-outside"
    outside = write_generated_archetype(_generated(archetype_id="outside-canary"), outside_root)
    original_open = os.open
    replaced = False

    def replace_before_descriptor_open(path, flags, *args, **kwargs):
        nonlocal replaced
        if path == candidate.name and kwargs.get("dir_fd") is not None and not replaced:
            replaced = True
            candidate.unlink()
            candidate.symlink_to(outside)
        return original_open(path, flags, *args, **kwargs)

    monkeypatch.setattr("tacit.archetypes.generated.store.os.open", replace_before_descriptor_open)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._descriptor_access_supported",
        lambda: True,
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert replaced is True
    assert result.archetypes == []
    assert "outside-canary" not in {artifact.id for artifact in result.archetypes}
    assert result.files_discovered == 1
    assert result.files_scanned == 0
    assert result.symlinks_rejected == 1
    assert result.reason_code == "generated_archetype_scope_path_symlink_rejected"


def test_experimental_retrieval_keeps_candidate_descriptor_fanout_constant(tmp_path, monkeypatch):
    query = _exact_generated_query()
    for index in range(8):
        write_generated_archetype(_generated(archetype_id=f"candidate-{index}"), tmp_path)

    real_open = os.open
    real_close = os.close
    all_descriptors: set[int] = set()
    candidate_descriptors: set[int] = set()
    peak_candidate_descriptors = 0

    def tracking_open(path, flags, *args, **kwargs):
        nonlocal peak_candidate_descriptors
        descriptor = real_open(path, flags, *args, **kwargs)
        all_descriptors.add(descriptor)
        if isinstance(path, str) and path.casefold().endswith(".yaml"):
            candidate_descriptors.add(descriptor)
            peak_candidate_descriptors = max(
                peak_candidate_descriptors,
                len(candidate_descriptors),
            )
        return descriptor

    def tracking_close(descriptor):
        all_descriptors.discard(descriptor)
        candidate_descriptors.discard(descriptor)
        return real_close(descriptor)

    monkeypatch.setattr("tacit.archetypes.generated.store.os.open", tracking_open)
    monkeypatch.setattr("tacit.archetypes.generated.store.os.close", tracking_close)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._descriptor_access_supported",
        lambda: True,
    )

    result = load_experimental_archetypes(tmp_path, query, max_files=8)
    missing = load_experimental_archetypes(tmp_path / "missing", query, max_files=8)

    assert len(result.archetypes) == 8
    assert missing.archetypes == []
    assert peak_candidate_descriptors == 1
    assert candidate_descriptors == set()
    assert all_descriptors == set()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO creation is unavailable")
def test_experimental_retrieval_rejects_non_regular_candidate(tmp_path):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    generated_path.unlink()
    fifo_path = generated_path.parent / "candidate.yaml"
    os.mkfifo(fifo_path)

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.files_discovered == 1
    assert result.invalid == 1
    assert dict(result.reason_counts) == {
        "generated_archetype_non_regular_file_rejected": 1,
    }


def test_experimental_retrieval_directory_list_failure_is_shadow_only(tmp_path, monkeypatch):
    query = _exact_generated_query()
    write_generated_archetype(_generated(), tmp_path)
    original_listdir = os.listdir

    def fail_descriptor_list(path: Any):
        if isinstance(path, int):
            raise OSError("directory-list-payload-canary")
        return original_listdir(path)

    def fail_path_list(_path: Path):
        raise OSError("directory-list-payload-canary")

    monkeypatch.setattr(os, "listdir", fail_descriptor_list)
    monkeypatch.setattr(os, "scandir", fail_descriptor_list)
    monkeypatch.setattr(Path, "iterdir", fail_path_list)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._descriptor_access_supported",
        lambda: True,
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.invalid == 1
    assert dict(result.reason_counts) == {
        "generated_archetype_directory_list_failed": 1,
    }
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_directory_list_failed"


def test_experimental_retrieval_file_read_failure_is_partial(tmp_path, monkeypatch):
    query = _exact_generated_query()
    write_generated_archetype(_generated(), tmp_path)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._read_descriptor",
        lambda *_args: (_ for _ in ()).throw(OSError("read-payload-canary")),
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.status.value == "partial"
    assert result.reason_code == "generated_archetype_file_read_failed"
    assert dict(result.reason_counts) == {
        "generated_archetype_file_read_failed": 1,
    }


def test_experimental_retrieval_file_os_permission_error_is_partial(tmp_path, monkeypatch):
    query = _exact_generated_query()
    write_generated_archetype(_generated(), tmp_path)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._read_descriptor",
        lambda *_args: (_ for _ in ()).throw(PermissionError("file-path-canary")),
    )

    with capture_logs() as logs:
        result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.status.value == "partial"
    assert result.reason_code == "generated_archetype_file_read_failed"
    assert dict(result.reason_counts) == {
        "generated_archetype_file_read_failed": 1,
    }
    assert "file-path-canary" not in json.dumps(logs)


def test_experimental_retrieval_root_os_permission_error_is_shadow_only(tmp_path, monkeypatch):
    query = _exact_generated_query()
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._descriptor_access_supported",
        lambda: True,
    )
    monkeypatch.setattr(
        "tacit.archetypes.generated.store.os.open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError("root-path-canary")),
    )

    with capture_logs() as logs:
        result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_root_open_failed"
    assert "root-path-canary" not in json.dumps(logs)


def test_experimental_retrieval_semantic_authorization_error_propagates(tmp_path, monkeypatch):
    query = _exact_generated_query()
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._descriptor_access_supported",
        lambda: True,
    )
    monkeypatch.setattr(
        "tacit.archetypes.generated.store.os.open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(SemanticAuthorizationError("semantic-authority-canary")),
    )

    with pytest.raises(SemanticAuthorizationError, match="semantic-authority-canary"):
        load_experimental_archetypes(tmp_path, query)


def test_semantic_authorization_error_preserves_permission_error_compatibility():
    runtime_settings = Settings(_env_file=None, knowledge_permissions="")

    with pytest.raises(SemanticAuthorizationError) as exc_info:
        enforce_knowledge_action(runtime_settings, KnowledgeAction.READ)

    assert isinstance(exc_info.value, PermissionError)
    assert SemanticAuthorizationError in AUTHORITY_BOUNDARY_ERRORS
    assert PermissionError not in AUTHORITY_BOUNDARY_ERRORS


def test_experimental_retrieval_root_os_error_remains_degraded(tmp_path, monkeypatch):
    query = _exact_generated_query()
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._descriptor_access_supported",
        lambda: True,
    )
    monkeypatch.setattr(
        "tacit.archetypes.generated.store.os.open",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("root-io-canary")),
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.status.value == "skipped"
    assert result.reason_code == "generated_archetype_root_open_failed"
    assert dict(result.reason_counts) == {
        "generated_archetype_root_open_failed": 1,
    }


@pytest.mark.parametrize(
    "error",
    [
        RuntimeOwnershipError("schema-owner-canary"),
        TenantBoundaryError("schema-tenant-canary", status_code=403),
    ],
    ids=["runtime-ownership", "tenant-boundary"],
)
def test_experimental_retrieval_schema_authority_error_propagates(
    tmp_path,
    monkeypatch,
    error,
):
    query = _exact_generated_query()
    write_generated_archetype(_generated(), tmp_path)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._load_file_atomically",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(error),
    )

    with pytest.raises(type(error), match=str(error)):
        load_experimental_archetypes(tmp_path, query)


def test_experimental_retrieval_schema_value_error_remains_degraded(tmp_path, monkeypatch):
    query = _exact_generated_query()
    write_generated_archetype(_generated(), tmp_path)
    monkeypatch.setattr(
        "tacit.archetypes.generated.store._load_file_atomically",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("schema-value-canary")),
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.status.value == "partial"
    assert result.reason_code == "generated_archetype_schema_validation_failed"
    assert dict(result.reason_counts) == {
        "generated_archetype_schema_validation_failed": 1,
    }


def test_quarantined_and_scope_mismatched_artifacts_are_clean_no_match(tmp_path):
    query = _exact_generated_query()
    write_generated_archetype(
        _generated(
            archetype_id="quarantined",
            status=GeneratedArchetypeStatus.QUARANTINED,
        ),
        tmp_path,
    )
    write_generated_archetype(
        _generated(
            archetype_id="wrong-environment",
            environment_refs=frozenset({"staging"}),
        ),
        tmp_path,
    )

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.quarantined == 1
    assert result.rejected_by_scope == 1
    assert result.status.value == "passed"
    assert result.reason_code == "generated_archetype_retrieval_complete"
    assert result.reason_counts == ()


def test_experimental_retrieval_failure_logs_redact_path_payload_and_traceback(tmp_path):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    generated_path.unlink()
    canary_path = generated_path.parent / "secret-path-canary.yaml"
    canary_path.write_text("generated_archetypes: [payload-canary: [")

    with capture_logs() as logs:
        result = load_experimental_archetypes(tmp_path, query)

    rendered_logs = json.dumps(logs, default=str)
    assert result.invalid == 1
    assert "secret-path-canary" not in rendered_logs
    assert "payload-canary" not in rendered_logs
    assert "Traceback" not in rendered_logs
    assert all("exc_info" not in record for record in logs)
    failure = next(record for record in logs if record.get("reason_code") == "generated_archetype_yaml_parse_failed")
    assert failure["exception_class"] == "ParserError"
    assert len(failure["artifact_fingerprint"]) == 16
    assert result.status.value == "partial"
    assert result.reason_code == "generated_archetype_yaml_parse_failed"


def _yaml_work_counts(payload: bytes) -> tuple[int, int, int]:
    node_count = 0
    scalar_count = 0
    depth = 0
    max_depth = 0
    for event in yaml.parse(payload):
        if isinstance(event, (yaml.MappingStartEvent, yaml.SequenceStartEvent)):
            node_count += 1
            depth += 1
            max_depth = max(max_depth, depth)
        elif isinstance(event, (yaml.MappingEndEvent, yaml.SequenceEndEvent)):
            depth -= 1
        elif isinstance(event, yaml.ScalarEvent):
            node_count += 1
            scalar_count += 1
    return node_count, max_depth, scalar_count


@pytest.mark.parametrize(
    ("limit_name", "reason_code", "count_index"),
    [
        ("max_yaml_nodes", "generated_archetype_yaml_node_limit_exceeded", 0),
        ("max_yaml_depth", "generated_archetype_yaml_depth_limit_exceeded", 1),
        ("max_yaml_scalars", "generated_archetype_yaml_scalar_limit_exceeded", 2),
    ],
)
def test_experimental_retrieval_enforces_yaml_work_limit_and_limit_plus_one(
    tmp_path,
    limit_name,
    reason_code,
    count_index,
):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    payload = generated_path.read_bytes()
    work_counts = _yaml_work_counts(payload)

    at_limit = load_experimental_archetypes(
        tmp_path,
        query,
        **{limit_name: work_counts[count_index]},
    )
    over_limit = load_experimental_archetypes(
        tmp_path,
        query,
        **{limit_name: work_counts[count_index] - 1},
    )

    assert [artifact.id for artifact in at_limit.archetypes] == ["checkout_generated"]
    assert over_limit.archetypes == []
    assert over_limit.rejected_by_limit == 1
    assert reason_code in over_limit.limit_reason_codes


def test_experimental_retrieval_rejects_yaml_aliases(tmp_path):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    shared: list[str] = ["alias-canary"]
    payload = yaml.safe_dump(
        {
            "generated_archetypes": [_generated().model_dump(mode="json")],
            "padding": [shared, shared],
        },
        sort_keys=False,
    )
    assert "*id" in payload
    generated_path.write_text(payload)

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.invalid == 1
    assert dict(result.reason_counts) == {
        "generated_archetype_yaml_alias_rejected": 1,
    }


def test_experimental_retrieval_enforces_yaml_scalar_byte_limit(tmp_path):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    payload = generated_path.read_bytes()
    largest_scalar = max(
        len(event.value.encode("utf-8")) for event in yaml.parse(payload) if isinstance(event, yaml.ScalarEvent)
    )

    at_limit = load_experimental_archetypes(
        tmp_path,
        query,
        max_yaml_scalar_bytes=largest_scalar,
    )
    over_limit = load_experimental_archetypes(
        tmp_path,
        query,
        max_yaml_scalar_bytes=largest_scalar - 1,
    )

    assert [artifact.id for artifact in at_limit.archetypes] == ["checkout_generated"]
    assert over_limit.archetypes == []
    assert over_limit.rejected_by_limit == 1
    assert over_limit.limit_reason_codes == ("generated_archetype_yaml_scalar_size_limit_exceeded",)


def test_experimental_retrieval_uses_typed_semantic_limits_from_settings(tmp_path):
    write_generated_archetype(_generated(), tmp_path)

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
            learned_archetypes_retrieval_max_yaml_scalar_bytes=1,
        ),
    )

    assert selection.shadow_archetypes == []
    assert selection.experimental_retrieval.limit_reason_codes == (
        "generated_archetype_yaml_scalar_size_limit_exceeded",
    )


@pytest.mark.parametrize(
    ("limit_name", "reason_code", "artifacts"),
    [
        (
            "max_artifacts_per_file",
            "generated_archetype_artifact_limit_exceeded",
            [
                _generated(archetype_id="first").model_dump(mode="json"),
                _generated(archetype_id="second").model_dump(mode="json"),
            ],
        ),
        (
            "max_panels_per_file",
            "generated_archetype_panel_limit_exceeded",
            [
                {
                    **_generated().model_dump(mode="json"),
                    "panels": [
                        {"title": "First", "queries": [{"expr": "first_metric"}]},
                        {"title": "Second", "queries": [{"expr": "second_metric"}]},
                    ],
                }
            ],
        ),
        (
            "max_queries_per_file",
            "generated_archetype_query_limit_exceeded",
            [
                {
                    **_generated().model_dump(mode="json"),
                    "panels": [
                        {
                            "title": "Queries",
                            "queries": [{"expr": "first_metric"}, {"expr": "second_metric"}],
                        }
                    ],
                }
            ],
        ),
    ],
)
def test_experimental_retrieval_enforces_schema_collection_limit_plus_one(
    tmp_path,
    limit_name,
    reason_code,
    artifacts,
):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    _replace_generated_document(generated_path, artifacts)

    over_limit = load_experimental_archetypes(tmp_path, query, **{limit_name: 1})

    assert over_limit.archetypes == []
    assert over_limit.rejected_by_limit == 1
    assert reason_code in over_limit.limit_reason_codes

    _replace_generated_document(generated_path, artifacts[:1])
    if limit_name == "max_panels_per_file":
        artifacts[0]["panels"] = artifacts[0]["panels"][:1]
        _replace_generated_document(generated_path, artifacts)
    elif limit_name == "max_queries_per_file":
        artifacts[0]["panels"][0]["queries"] = artifacts[0]["panels"][0]["queries"][:1]
        _replace_generated_document(generated_path, artifacts)

    at_limit = load_experimental_archetypes(tmp_path, query, **{limit_name: 1})

    assert len(at_limit.archetypes) == 1


def test_experimental_retrieval_rolls_back_valid_artifact_when_file_contains_invalid_artifact(tmp_path):
    query = _exact_generated_query()
    generated_path = write_generated_archetype(_generated(), tmp_path)
    valid = _generated(archetype_id="valid-canary").model_dump(mode="json")
    invalid = _generated(archetype_id="invalid-canary").model_dump(mode="json")
    invalid.pop("name")
    _replace_generated_document(generated_path, [valid, invalid])

    result = load_experimental_archetypes(tmp_path, query)

    assert result.archetypes == []
    assert result.invalid == 1
    assert dict(result.reason_counts) == {
        "generated_archetype_schema_validation_failed": 1,
    }


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
        tenant_id="tenant-a",
        settings=settings,
    )

    assert "checkout_generated" not in {archetype.id for archetype, _ in selection.ranked_archetypes}
    assert [archetype.id for archetype, _ in selection.shadow_archetypes] == ["checkout_generated"]
    assert selection.context_sources["generated_archetypes"] == 0
    assert selection.context_sources["shadow_generated_archetypes"] == 1
    assert selection.experimental_retrieval.files_scanned == 1
    assert selection.unexpected_cross_service_matches == 0


def test_environment_scoped_artifact_uses_the_frozen_intent_scope(tmp_path):
    write_generated_archetype(
        _generated(environment_refs=frozenset({"production"})),
        tmp_path,
    )

    selection = select_archetypes(
        intent=_intent("checkout", environments=["production"]),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
        ),
    )

    assert [archetype.id for archetype, _ in selection.shadow_archetypes] == ["checkout_generated"]
    assert selection.experimental_retrieval.rejected_by_scope == 0


def test_same_id_generated_archetype_remains_in_shadow_evaluation(tmp_path):
    generated = _generated(archetype_id="resource_saturation")
    write_generated_archetype(generated, tmp_path)
    settings = _settings(tmp_path, mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE)

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=settings,
    )

    assert "resource_saturation" in {archetype.id for archetype, _ in selection.ranked_archetypes}
    assert [archetype for archetype, _ in selection.shadow_archetypes] == [generated]

    records = _history_archetypes(
        [],
        selection.ranked_archetypes,
        selection.learned_archetypes,
        selection.shadow_archetypes,
    )
    same_id_records = [record for record in records if record["type"] == "resource_saturation"]
    assert len(same_id_records) == 2
    assert {record["template_origin"] for record in same_id_records} == {"curated", "generated"}
    generated_record = next(record for record in same_id_records if record["template_origin"] == "generated")
    assert generated_record["generation_version"] == generated.generation_version
    assert generated_record["generation_run_id"] == generated.generation_run_id
    assert str(generated_record["artifact_ref"]).startswith("generated:resource_saturation:")


def test_experimental_mode_records_no_match_separately_from_curated_control(tmp_path):
    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
        ),
    )

    assert selection.shadow_archetypes == []
    assert selection.retrieval_mode == ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE
    assert selection.retrieval_reason_code == "experimental_exact_scope_no_match"
    assert selection.retrieval_stage_status == "passed"


@pytest.mark.parametrize(
    "scenario",
    ["no-curated", "normal", "limit", "parse-failure", "unexpected-failure"],
)
def test_experimental_retrieval_emits_exactly_one_bounded_completion_event(
    tmp_path,
    monkeypatch,
    scenario,
):
    settings_updates: dict[str, object] = {}
    if scenario == "normal":
        write_generated_archetype(_generated(), tmp_path)
    elif scenario == "limit":
        write_generated_archetype(_generated(archetype_id="first"), tmp_path)
        write_generated_archetype(_generated(archetype_id="second"), tmp_path)
        settings_updates["learned_archetypes_retrieval_max_files"] = 1
    elif scenario == "parse-failure":
        generated_path = write_generated_archetype(_generated(), tmp_path)
        generated_path.write_text("generated_archetypes: [event-payload-canary: [")
    elif scenario == "unexpected-failure":
        monkeypatch.setattr(
            "tacit.pipeline.stages.archetypes.load_experimental_archetypes",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("event-exception-message-canary")),
        )
    else:
        monkeypatch.setattr(
            "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
            lambda *_args, **_kwargs: [],
        )
        monkeypatch.setattr(
            "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
            lambda *_args, **_kwargs: [],
        )
        monkeypatch.setattr("tacit.pipeline.stages.archetypes.get_archetype", lambda *_args: None)

    events: list[tuple[str, float, dict[str, Any]]] = []
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.stage_log",
        lambda stage, duration, **details: events.append((stage, duration, details)),
    )

    selection = select_archetypes(
        intent=_intent("checkout"),
        metric_catalog=_catalog(),
        catalog_for_compile=_catalog(),
        target_language="promql",
        tenant_id="tenant-a",
        settings=_settings(
            tmp_path,
            mode=ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
            **settings_updates,
        ),
    )

    retrieval_events = [event for event in events if event[0] == "archetype_retrieval"]
    assert len(retrieval_events) == 1
    _, duration, details = retrieval_events[0]
    assert duration >= 0
    assert set(details) == {
        "bytes_scanned",
        "directory_entries_discovered",
        "exception_class",
        "files_discovered",
        "files_scanned",
        "invalid",
        "matches",
        "oversized_files",
        "quarantined",
        "reason_code",
        "reason_counts",
        "rejected_by_limit",
        "rejected_by_scope",
        "status",
        "symlinks_rejected",
        "total_artifacts",
        "total_panels",
        "total_queries",
    }
    assert all(
        isinstance(value, int)
        for key, value in details.items()
        if key not in {"exception_class", "reason_code", "reason_counts", "status"}
    )
    assert isinstance(details["reason_counts"], tuple)
    assert "event-payload-canary" not in json.dumps(details)
    assert "event-exception-message-canary" not in json.dumps(details)
    assert details["exception_class"] == ("RuntimeError" if scenario == "unexpected-failure" else "")
    expected_status = {
        "no-curated": "passed",
        "normal": "passed",
        "limit": "skipped",
        "parse-failure": "partial",
        "unexpected-failure": "skipped",
    }[scenario]
    assert details["status"] == expected_status
    assert selection.retrieval_stage_status == expected_status
    assert details["reason_code"] == selection.retrieval_reason_code
    assert selection.context_sources["generated_archetypes"] == 0


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
        tenant_id="tenant-a",
        settings=settings,
    )

    assert selection.experimental_retrieval.archetypes
    assert [archetype.id for archetype, _ in selection.shadow_archetypes] == ["checkout_generated"]
    assert all("checkout_generated" not in candidate_ids for candidate_ids in ranked_candidate_ids)
    assert selection.context_sources["generated_archetypes"] == 0
