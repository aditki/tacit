from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast

import pytest
from pydantic import ValidationError

from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.backends.base import DashboardBackend
from tacit.evidence import (
    EvidenceObservationWorkBudget,
    EvidenceObservationWorkLimitError,
    EvidenceObservationWorkLimits,
    contributing_archetypes,
    observe_evidence,
    requirements_for_archetypes,
    resolve_declared_requirements_for_archetypes,
)
from tacit.evidence_artifacts import build_symptom_evidence_dashboard
from tacit.models.schemas import (
    MAX_CLOUDWATCH_DIMENSION_VALUES,
    MAX_CLOUDWATCH_DIMENSIONS,
    MAX_DASHBOARD_NESTED_DEPTH,
    MAX_DASHBOARD_NESTED_NODES,
    MAX_DASHBOARD_PANELS,
    MAX_DASHBOARD_QUERIES,
    MAX_DASHBOARD_SCALAR_BYTES,
    MAX_DASHBOARD_SCALAR_CHARACTERS,
    MAX_DASHBOARD_TAGS,
    MAX_DASHBOARD_TOTAL_SCALAR_CHARACTERS,
    MAX_PANEL_QUERIES,
    MAX_PANEL_THRESHOLDS,
    DashboardSpec,
    DashboardWorkLimitError,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    validate_dashboard_composition_work_limits,
)
from tacit.pipeline.stages.evidence import run_evidence_stage
from tacit.pipeline.validation import validate_dashboard_and_evidence


def _query(expr: str = "request_latency_seconds") -> PanelQuery:
    return PanelQuery(
        expr=expr,
        datasource_uid="prometheus",
        datasource_type="prometheus",
        query_language="promql",
    )


def _panel(*, queries: list[PanelQuery] | None = None, title: str = "Latency") -> PanelSpec:
    return PanelSpec(title=title, queries=queries if queries is not None else [_query()])


def _dashboard(*, panels: list[PanelSpec] | None = None) -> DashboardSpec:
    return DashboardSpec(title="Checkout", panels=panels if panels is not None else [_panel()])


def _intent() -> Intent:
    return Intent(
        summary="Checkout latency",
        domain="application",
        services=["checkout"],
        keywords=["latency"],
    )


def _requirement(
    requirement_id: str = "req-latency",
    *,
    priority: str = "critical",
    signal_type: str = "request_latency",
) -> EvidenceRequirement:
    return EvidenceRequirement(
        id=requirement_id,
        evidence_type="semantic_signal",
        signal_type=signal_type,
        default_metric="request_latency_seconds",
        priority=priority,
        source="latency-investigation",
    )


def _resolution(
    requirement_id: str = "req-latency",
    *,
    status: EvidenceResolutionStatus = EvidenceResolutionStatus.RESOLVED,
) -> EvidenceResolution:
    return EvidenceResolution(
        requirement_id=requirement_id,
        status=status,
        reason_code="resolved" if status == EvidenceResolutionStatus.RESOLVED else "missing",
        metric="request_latency_seconds" if status == EvidenceResolutionStatus.RESOLVED else "",
        datasource_uid="prometheus" if status == EvidenceResolutionStatus.RESOLVED else "",
        datasource_type="prometheus" if status == EvidenceResolutionStatus.RESOLVED else "",
        query_language="promql" if status == EvidenceResolutionStatus.RESOLVED else "",
    )


class _CountingBackend:
    def __init__(self, *, returned_spec: DashboardSpec | None = None) -> None:
        self.calls = 0
        self.returned_spec = returned_spec

    async def validate_queries(
        self,
        spec: DashboardSpec,
        _catalog: list[Any],
    ) -> tuple[DashboardSpec, list[str]]:
        self.calls += 1
        return self.returned_spec or spec, []


class _NeverIteratedQueries(list[PanelQuery]):
    def __iter__(self) -> Iterator[PanelQuery]:
        raise AssertionError("query traversal occurred before the aggregate work check")


class _NeverIteratedPanels(list[PanelTemplate]):
    def __iter__(self) -> Iterator[PanelTemplate]:
        raise AssertionError("archetype panel traversal occurred before the aggregate work check")


class _NeverIteratedCatalog(list[Any]):
    def __iter__(self) -> Iterator[Any]:
        raise AssertionError("catalog traversal occurred before the aggregate work check")


class _CountingSignalStore:
    def __init__(self, results: list[Any] | None = None) -> None:
        self.calls = 0
        self.results = results or []

    def resolve_signal_details(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        self.calls += 1
        return self.results


def _archetype(
    archetype_id: str = "latency",
    *,
    required_signals: list[str] | None = None,
    panels: list[PanelTemplate] | None = None,
) -> InvestigationArchetype:
    return InvestigationArchetype.model_construct(
        id=archetype_id,
        name=archetype_id,
        description="",
        problem_types=["latency"],
        required_signals=required_signals if required_signals is not None else ["request_latency"],
        signal_bindings={},
        required_metrics=[],
        panels=(
            panels
            if panels is not None
            else [PanelTemplate(title="Latency", queries=[QueryTemplate(expr="request_latency_seconds")])]
        ),
        tags=[],
        default_timerange="1h",
    )


def test_dashboard_schema_rejects_duplicate_heavy_panel_input() -> None:
    raw_panel = {"title": "Latency", "queries": []}

    with pytest.raises(ValidationError, match="panels"):
        DashboardSpec.model_validate(
            {
                "title": "Oversized",
                "panels": [raw_panel] * (MAX_DASHBOARD_PANELS + 1),
            }
        )


def test_panel_schema_rejects_duplicate_heavy_query_input() -> None:
    raw_query = {
        "expr": "request_latency_seconds",
        "datasource_uid": "prometheus",
    }

    with pytest.raises(ValidationError, match="queries_per_panel"):
        PanelSpec.model_validate(
            {
                "title": "Oversized",
                "queries": [raw_query] * (MAX_PANEL_QUERIES + 1),
            }
        )


@pytest.mark.parametrize(
    ("payload", "dimension"),
    [
        ({"title": "Oversized", "tags": ["tag"] * (MAX_DASHBOARD_TAGS + 1), "panels": []}, "tags"),
        (
            {
                "title": "Oversized",
                "queries": [],
                "thresholds": [{}] * (MAX_PANEL_THRESHOLDS + 1),
            },
            "thresholds_per_panel",
        ),
        (
            {
                "expr": "metric",
                "datasource_uid": "cloudwatch",
                "cloudwatch_dimensions": {
                    f"Dimension{index}": "value" for index in range(MAX_CLOUDWATCH_DIMENSIONS + 1)
                },
            },
            "cloudwatch_dimensions",
        ),
        (
            {
                "expr": "metric",
                "datasource_uid": "cloudwatch",
                "cloudwatch_dimensions": {
                    "Service": ["value"] * (MAX_CLOUDWATCH_DIMENSION_VALUES + 1),
                },
            },
            "cloudwatch_dimension_values",
        ),
    ],
)
def test_dashboard_models_reject_duplicate_heavy_metadata(payload: dict[str, Any], dimension: str) -> None:
    model = DashboardSpec if "panels" in payload else PanelSpec if "thresholds" in payload else PanelQuery

    with pytest.raises((DashboardWorkLimitError, ValidationError), match=dimension):
        model.model_validate(payload)


def test_panel_schema_rejects_deep_threshold_metadata_before_model_construction() -> None:
    nested: dict[str, Any] = {"value": "ok"}
    for _ in range(MAX_DASHBOARD_NESTED_DEPTH + 1):
        nested = {"nested": nested}

    with pytest.raises(ValidationError, match="nested_depth"):
        PanelSpec.model_validate(
            {
                "title": "Oversized",
                "queries": [],
                "thresholds": [nested],
            }
        )


def test_panel_schema_rejects_wide_threshold_metadata_before_model_construction() -> None:
    thresholds = [{f"key-{index}": "value" for index in range(MAX_DASHBOARD_NESTED_NODES + 1)}]

    with pytest.raises(ValidationError, match="nested_nodes"):
        PanelSpec.model_validate(
            {
                "title": "Oversized",
                "queries": [],
                "thresholds": thresholds,
            }
        )


def test_dashboard_schema_rejects_aggregate_queries_across_panels() -> None:
    first_count = MAX_DASHBOARD_QUERIES // 2
    second_count = MAX_DASHBOARD_QUERIES - first_count + 1

    with pytest.raises(ValidationError, match="total_queries"):
        DashboardSpec(
            title="Oversized",
            panels=[
                _panel(queries=[_query()] * first_count, title="First"),
                _panel(queries=[_query()] * second_count, title="Second"),
            ],
        )


def test_dashboard_schema_accepts_every_dimension_at_its_limit() -> None:
    dashboard = DashboardSpec(
        title="At limit",
        panels=[
            _panel(queries=[_query()] * MAX_DASHBOARD_QUERIES, title="Queries"),
            *[_panel(queries=[], title=f"Empty {index}") for index in range(MAX_DASHBOARD_PANELS - 1)],
        ],
    )

    assert len(dashboard.panels) == MAX_DASHBOARD_PANELS
    assert len(dashboard.panels[0].queries) == MAX_PANEL_QUERIES


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("oversized_dashboard", "dimension"),
    [
        (
            DashboardSpec.model_construct(
                title="Too many panels",
                panels=[_panel()] * (MAX_DASHBOARD_PANELS + 1),
            ),
            "panels",
        ),
        (
            DashboardSpec.model_construct(
                title="Too many panel queries",
                panels=[
                    PanelSpec.model_construct(
                        title="Oversized",
                        queries=[_query()] * (MAX_PANEL_QUERIES + 1),
                    )
                ],
            ),
            "queries_per_panel",
        ),
        (
            DashboardSpec.model_construct(
                title="Too many total queries",
                panels=[
                    PanelSpec.model_construct(
                        title="First",
                        queries=[_query()] * (MAX_DASHBOARD_QUERIES // 2),
                    ),
                    PanelSpec.model_construct(
                        title="Second",
                        queries=[_query()] * (MAX_DASHBOARD_QUERIES // 2 + 1),
                    ),
                ],
            ),
            "total_queries",
        ),
    ],
)
async def test_validation_rejects_model_construct_overflow_before_backend_work(
    oversized_dashboard: DashboardSpec,
    dimension: str,
) -> None:
    backend = _CountingBackend()

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=oversized_dashboard,
            catalog=[],
            evidence_requirements=[],
            evidence_resolutions=[],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=False,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == dimension
    assert backend.calls == 0


@pytest.mark.asyncio
async def test_validation_rejects_oversized_query_payload_before_copy_or_backend_work() -> None:
    backend = _CountingBackend()
    oversized = DashboardSpec.model_construct(
        title="Oversized payload",
        panels=[
            PanelSpec.model_construct(
                title="Latency",
                queries=[PanelQuery.model_construct(expr="x" * 1_000_000)],
            )
        ],
    )

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=oversized,
            catalog=[],
            evidence_requirements=[],
            evidence_resolutions=[],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=False,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == "scalar_characters"
    assert backend.calls == 0


@pytest.mark.asyncio
async def test_validation_rejects_multibyte_query_payload_before_copy_or_backend_work() -> None:
    backend = _CountingBackend()
    oversized = DashboardSpec.model_construct(
        title="Oversized bytes",
        panels=[
            PanelSpec.model_construct(
                title="Latency",
                queries=[
                    PanelQuery.model_construct(
                        expr="\U0001f642" * ((MAX_DASHBOARD_SCALAR_BYTES // 4) + 1),
                        datasource_uid="prometheus",
                    )
                ],
            )
        ],
    )

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=oversized,
            catalog=[],
            evidence_requirements=[],
            evidence_resolutions=[],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=False,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == "scalar_bytes"
    assert backend.calls == 0


@pytest.mark.asyncio
async def test_validation_rejects_oversized_nested_metadata_before_copy_or_backend_work() -> None:
    backend = _CountingBackend()
    oversized = DashboardSpec.model_construct(
        title="Oversized metadata",
        tags=[],
        timerange="1h",
        panels=[
            PanelSpec.model_construct(
                title="Latency",
                description="",
                panel_type="timeseries",
                queries=[_query()],
                unit="",
                thresholds=[{"label": "x" * 1_000_000}],
                source_archetype="",
                row="",
            )
        ],
    )

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=oversized,
            catalog=[],
            evidence_requirements=[],
            evidence_resolutions=[],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=False,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == "scalar_characters"
    assert backend.calls == 0


@pytest.mark.asyncio
async def test_validation_rejects_deep_bypass_metadata_before_copy_or_backend_work() -> None:
    backend = _CountingBackend()
    nested: dict[str, Any] = {"value": "ok"}
    for _ in range(MAX_DASHBOARD_NESTED_DEPTH + 1):
        nested = {"nested": nested}
    oversized = DashboardSpec.model_construct(
        title="Oversized metadata",
        tags=[],
        timerange="1h",
        panels=[
            PanelSpec.model_construct(
                title="Latency",
                description="",
                panel_type="timeseries",
                queries=[_query()],
                unit="",
                thresholds=[nested],
                source_archetype="",
                row="",
            )
        ],
    )

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=oversized,
            catalog=[],
            evidence_requirements=[],
            evidence_resolutions=[],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=False,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == "nested_depth"
    assert backend.calls == 0


def test_observation_rejects_duplicate_requirements_before_query_traversal() -> None:
    query_list = _NeverIteratedQueries([_query()])
    panel = PanelSpec.model_construct(title="Latency", queries=query_list)
    dashboard = DashboardSpec.model_construct(title="Checkout", panels=[panel])
    requirement = _requirement()
    budget = EvidenceObservationWorkBudget(EvidenceObservationWorkLimits(max_requirements=1))

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        observe_evidence(
            [requirement, requirement],
            [],
            dashboard,
            dashboard,
            work_budget=budget,
        )

    assert exc_info.value.dimension == "requirements"


def test_observation_rejects_query_comparison_fanout_before_query_traversal() -> None:
    query_list = _NeverIteratedQueries([_query(), _query("request_latency_seconds_sum")])
    panel = PanelSpec.model_construct(title="Latency", queries=query_list)
    dashboard = DashboardSpec.model_construct(title="Checkout", panels=[panel])
    requirements = [_requirement("req-1"), _requirement("req-2")]
    resolutions = [_resolution("req-1"), _resolution("req-2")]
    budget = EvidenceObservationWorkBudget(EvidenceObservationWorkLimits(max_total_query_checks=3))

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        observe_evidence(
            requirements,
            resolutions,
            dashboard,
            dashboard,
            work_budget=budget,
        )

    assert exc_info.value.dimension == "total_query_checks"


def test_observation_rejects_output_fanout_before_query_traversal() -> None:
    query_list = _NeverIteratedQueries([_query(), _query("request_latency_seconds_sum")])
    panel = PanelSpec.model_construct(title="Latency", queries=query_list)
    dashboard = DashboardSpec.model_construct(title="Checkout", panels=[panel])
    requirements = [_requirement("req-1"), _requirement("req-2")]
    resolutions = [_resolution("req-1"), _resolution("req-2")]
    budget = EvidenceObservationWorkBudget(
        EvidenceObservationWorkLimits(
            max_total_query_checks=100,
            max_total_observation_slots=3,
        )
    )

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        observe_evidence(
            requirements,
            resolutions,
            dashboard,
            dashboard,
            work_budget=budget,
        )

    assert exc_info.value.dimension == "total_observation_slots"


def test_contribution_selection_rejects_ranked_fanout_before_archetype_panel_traversal() -> None:
    archetype = _archetype(panels=cast(list[PanelTemplate], _NeverIteratedPanels([])))
    ranked = [(archetype, 0.9)] * 3
    limits = EvidenceObservationWorkLimits(max_ranked_archetypes=2)

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        contributing_archetypes(ranked, _dashboard(), work_limits=limits)

    assert exc_info.value.dimension == "ranked_archetypes"


def test_requirement_declaration_rejects_duplicate_heavy_raw_inputs_before_allocation() -> None:
    archetype = _archetype(required_signals=["request_latency"] * 3)
    limits = EvidenceObservationWorkLimits(max_requirements=2)

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        requirements_for_archetypes([(archetype, 0.9)], _intent(), work_limits=limits)

    assert exc_info.value.dimension == "projected_requirements"


def test_resolution_rejects_duplicate_archetype_ids_before_resolver_work() -> None:
    archetype = _archetype()
    requirements = requirements_for_archetypes([(archetype, 0.9)], _intent())
    store = _CountingSignalStore()

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        resolve_declared_requirements_for_archetypes(
            [(archetype, 0.9), (archetype, 0.8)],
            _intent(),
            [],
            requirements,
            signal_store=store,
        )

    assert exc_info.value.dimension == "duplicate_archetype_ids"
    assert store.calls == 0


def test_resolution_rejects_per_call_signal_result_fanout_before_sorting() -> None:
    archetype = _archetype()
    requirements = requirements_for_archetypes([(archetype, 0.9)], _intent())

    class Match:
        confidence = 1.0

    store = _CountingSignalStore([Match(), Match(), Match()])

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        resolve_declared_requirements_for_archetypes(
            [(archetype, 0.9)],
            _intent(),
            [],
            requirements,
            signal_store=store,
            work_limits=EvidenceObservationWorkLimits(max_signal_resolution_results_per_call=2),
        )

    assert exc_info.value.dimension == "signal_resolution_results_per_call"
    assert store.calls == 1


def test_evidence_stage_rejects_catalog_before_contribution_or_resolution_work() -> None:
    archetype = _archetype(panels=cast(list[PanelTemplate], _NeverIteratedPanels([])))
    catalog = cast(list[Any], _NeverIteratedCatalog([object(), object(), object()]))

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        run_evidence_stage(
            ranked_archetypes=[(archetype, 0.9)],
            dashboard_spec=_dashboard(),
            intent=_intent(),
            catalog=catalog,
            target_language="promql",
            signal_store=object(),
            evidence_work_limits=EvidenceObservationWorkLimits(max_catalog_entries=2),
        )

    assert exc_info.value.dimension == "catalog_entries"


def test_evidence_stage_returns_the_initial_resolution_budget_for_validation() -> None:
    catalog = [
        MetricEntry(
            name="other_metric",
            datasource_uid="prometheus",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]
    result = run_evidence_stage(
        ranked_archetypes=[(_archetype(), 0.9)],
        dashboard_spec=_dashboard(),
        intent=_intent(),
        catalog=catalog,
        target_language="promql",
        signal_store=_CountingSignalStore(),
    )

    assert result.work_budget is not None
    assert result.work_budget.total_resolution_catalog_checks > 0


@pytest.mark.asyncio
async def test_validation_preserves_initial_resolution_work_in_final_telemetry() -> None:
    catalog = [
        MetricEntry(
            name="other_metric",
            datasource_uid="prometheus",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]
    evidence_stage = run_evidence_stage(
        ranked_archetypes=[(_archetype(), 0.9)],
        dashboard_spec=_dashboard(),
        intent=_intent(),
        catalog=catalog,
        target_language="promql",
        signal_store=_CountingSignalStore(),
    )
    stages: list[tuple[str, dict[str, Any]]] = []

    await validate_dashboard_and_evidence(
        primary=cast(DashboardBackend, _CountingBackend()),
        dashboard_spec=_dashboard(),
        catalog=catalog,
        evidence_requirements=evidence_stage.requirements,
        evidence_resolutions=evidence_stage.resolutions,
        intent=_intent(),
        target_language="promql",
        ranked_archetypes_present=True,
        record_stage=lambda name, *_args, **details: stages.append((name, details)),
        signal_store=_CountingSignalStore(),
        evidence_work_budget=evidence_stage.work_budget,
    )

    evidence_details = next(details for name, details in stages if name == "evidence")
    assert evidence_details["evidence_resolution_catalog_checks"] > 0


@pytest.mark.asyncio
async def test_initial_and_rescue_resolution_share_one_catalog_budget() -> None:
    catalog = [
        MetricEntry(
            name="other_metric",
            datasource_uid="prometheus",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]
    limits = EvidenceObservationWorkLimits(max_total_resolution_catalog_checks=12)
    evidence_stage = run_evidence_stage(
        ranked_archetypes=[(_archetype(), 0.9)],
        dashboard_spec=_dashboard(),
        intent=_intent(),
        catalog=catalog,
        target_language="promql",
        signal_store=_CountingSignalStore(),
        evidence_work_limits=limits,
    )
    backend = _CountingBackend()

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=_dashboard(),
            catalog=catalog,
            evidence_requirements=evidence_stage.requirements,
            evidence_resolutions=evidence_stage.resolutions,
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
            signal_store=_CountingSignalStore(),
            evidence_work_budget=evidence_stage.work_budget,
        )

    assert exc_info.value.dimension == "total_resolution_catalog_checks"
    assert backend.calls == 1


def test_rescue_builder_rejects_catalog_before_resolution_or_output_allocation() -> None:
    catalog = cast(list[Any], _NeverIteratedCatalog([object(), object(), object()]))
    budget = EvidenceObservationWorkBudget(EvidenceObservationWorkLimits(max_catalog_entries=2))

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        build_symptom_evidence_dashboard(
            [_requirement()],
            [_resolution(status=EvidenceResolutionStatus.UNRESOLVED)],
            _intent(),
            catalog=catalog,
            target_language="promql",
            timerange="1h",
            signal_store=object(),
            work_budget=budget,
        )

    assert exc_info.value.dimension == "catalog_entries"


@pytest.mark.asyncio
async def test_validation_shares_one_observation_budget_across_all_passes() -> None:
    dashboard = _dashboard()
    backend = _CountingBackend()
    requirement = _requirement(priority="supporting", signal_type="custom_signal")
    resolution = _resolution(status=EvidenceResolutionStatus.UNRESOLVED)

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=dashboard,
            catalog=[],
            evidence_requirements=[requirement],
            evidence_resolutions=[resolution],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
            evidence_work_limits=EvidenceObservationWorkLimits(max_observation_passes=2),
        )

    assert exc_info.value.dimension == "observation_passes"
    assert backend.calls == 1


@pytest.mark.asyncio
async def test_rescue_composition_limit_fails_before_second_backend_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base_dashboard = _dashboard(panels=[_panel(title=f"Panel {index}") for index in range(MAX_DASHBOARD_PANELS)])
    rescue_dashboard = _dashboard(panels=[_panel(title="Rescue")])
    backend = _CountingBackend()
    requirement = _requirement()
    resolution = _resolution(status=EvidenceResolutionStatus.UNRESOLVED)

    monkeypatch.setattr(
        "tacit.pipeline.validation.build_symptom_evidence_dashboard",
        lambda *_args, **_kwargs: (rescue_dashboard, [_resolution()]),
    )

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=base_dashboard,
            catalog=[],
            evidence_requirements=[requirement],
            evidence_resolutions=[resolution],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == "panels"
    assert backend.calls == 1


def _scalar_composition_dashboard(*, extra_character: bool = False) -> DashboardSpec:
    tags = ["x" * MAX_DASHBOARD_SCALAR_CHARACTERS] * 64
    if extra_character:
        tags.append("x")
    return DashboardSpec(
        title="",
        timerange="",
        tags=tags,
        panels=[
            PanelSpec(
                title="",
                description="",
                panel_type="",
                queries=[],
                unit="",
                thresholds=[],
                source_archetype="",
                row="",
            )
        ],
    )


def _nested_composition_dashboard(*, second_half: bool, extra_node: bool = False) -> DashboardSpec:
    threshold_counts = [MAX_PANEL_THRESHOLDS] * 8
    if second_half:
        threshold_counts[-1] = 240 + int(extra_node)
    return DashboardSpec(
        title="",
        timerange="",
        panels=[
            PanelSpec(title="", queries=[], thresholds=[{}] * threshold_count) for threshold_count in threshold_counts
        ],
    )


def test_dashboard_composition_accepts_aggregate_scalar_and_nested_limits_exactly() -> None:
    scalar_half = _scalar_composition_dashboard()
    nested_first = _nested_composition_dashboard(second_half=False)
    nested_second = _nested_composition_dashboard(second_half=True)

    validate_dashboard_composition_work_limits(scalar_half, scalar_half)
    validate_dashboard_composition_work_limits(nested_first, nested_second)

    assert 2 * 64 * MAX_DASHBOARD_SCALAR_CHARACTERS == MAX_DASHBOARD_TOTAL_SCALAR_CHARACTERS


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("base_dashboard", "rescue_dashboard", "dimension"),
    [
        (
            _scalar_composition_dashboard(),
            _scalar_composition_dashboard(extra_character=True),
            "total_scalar_characters",
        ),
        (
            _nested_composition_dashboard(second_half=False),
            _nested_composition_dashboard(second_half=True, extra_node=True),
            "nested_nodes",
        ),
    ],
)
async def test_rescue_composition_rejects_aggregate_payload_before_second_backend_validation(
    monkeypatch: pytest.MonkeyPatch,
    base_dashboard: DashboardSpec,
    rescue_dashboard: DashboardSpec,
    dimension: str,
) -> None:
    backend = _CountingBackend()
    requirement = _requirement()
    resolution = _resolution(status=EvidenceResolutionStatus.UNRESOLVED)

    monkeypatch.setattr(
        "tacit.pipeline.validation.build_symptom_evidence_dashboard",
        lambda *_args, **_kwargs: (rescue_dashboard, [_resolution()]),
    )

    with pytest.raises(DashboardWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=base_dashboard,
            catalog=[],
            evidence_requirements=[requirement],
            evidence_resolutions=[resolution],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
        )

    assert exc_info.value.dimension == dimension
    assert backend.calls == 1


@pytest.mark.asyncio
async def test_rescue_resolution_fanout_fails_before_second_backend_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dashboard = _dashboard()
    backend = _CountingBackend()
    requirement = _requirement()
    resolution = _resolution(status=EvidenceResolutionStatus.UNRESOLVED)

    monkeypatch.setattr(
        "tacit.pipeline.validation.build_symptom_evidence_dashboard",
        lambda *_args, **_kwargs: (dashboard, [_resolution()]),
    )

    with pytest.raises(EvidenceObservationWorkLimitError) as exc_info:
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, backend),
            dashboard_spec=dashboard,
            catalog=[],
            evidence_requirements=[requirement],
            evidence_resolutions=[resolution],
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
            evidence_work_limits=EvidenceObservationWorkLimits(max_resolutions=1),
        )

    assert exc_info.value.dimension == "resolutions"
    assert backend.calls == 1


@pytest.mark.asyncio
async def test_validation_records_bounded_observation_work_counters() -> None:
    dashboard = _dashboard()
    backend = _CountingBackend()
    requirement = _requirement(priority="supporting", signal_type="custom_signal")
    resolution = _resolution(status=EvidenceResolutionStatus.UNRESOLVED)
    stages: list[tuple[str, dict[str, Any]]] = []

    await validate_dashboard_and_evidence(
        primary=cast(DashboardBackend, backend),
        dashboard_spec=dashboard,
        catalog=[],
        evidence_requirements=[requirement],
        evidence_resolutions=[resolution],
        intent=_intent(),
        target_language="promql",
        ranked_archetypes_present=True,
        record_stage=lambda name, *_args, **details: stages.append((name, details)),
    )

    evidence_details = next(details for name, details in stages if name == "evidence")
    assert evidence_details["observation_passes"] == 3
    assert evidence_details["evidence_resolution_catalog_checks"] == 0
    assert evidence_details["observation_pass_limit"] == 3
    assert evidence_details["evidence_query_checks"] == 3
    assert evidence_details["evidence_query_check_limit"] == 2_000_000
    assert evidence_details["evidence_observation_slots"] == 3
    assert evidence_details["evidence_observation_slot_limit"] == 32_768


def test_normal_observation_behavior_is_preserved() -> None:
    dashboard = _dashboard()
    dashboard.panels[0].queries[0].validation_status = "ok"
    dashboard.panels[0].queries[0].validation_has_data = True

    observations = observe_evidence(
        [_requirement()],
        [_resolution()],
        dashboard,
        dashboard,
    )

    assert len(observations) == 1
    assert observations[0].survived is True
    assert observations[0].non_empty is True
