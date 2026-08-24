from __future__ import annotations

import ast
import asyncio
import json
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from tacit.archetypes.engine import (
    ArchetypeCoverageWorkLimitError,
    ArchetypeCoverageWorkLimits,
    compile_archetype,
    rank_archetypes_by_coverage,
)
from tacit.archetypes.generated import ArchetypeRetrievalMode
from tacit.archetypes.generated.schema import (
    MAX_GENERATED_QUERY_ENVIRONMENT_REFS,
    MAX_GENERATED_QUERY_SERVICE_REFS,
    GeneratedArchetypeQuery,
    GeneratedArchetypeQueryWorkLimitError,
)
from tacit.archetypes.schema import (
    MAX_ARCHETYPE_PANELS,
    MAX_ARCHETYPE_QUERIES_PER_PANEL,
    MAX_ARCHETYPE_REQUIRED_METRICS,
    MAX_ARCHETYPE_REQUIRED_SIGNALS,
    MAX_ARCHETYPE_SIGNAL_BINDINGS,
    MAX_ARCHETYPE_SIGNAL_REQUIREMENTS,
    MAX_ARCHETYPE_TOTAL_QUERIES,
    InvestigationArchetype,
    PanelTemplate,
    QueryTemplate,
)
from tacit.backends.base import DashboardBackend
from tacit.config import Settings
from tacit.culprit_ranking import rank_culprits
from tacit.errors import SemanticAuthorizationError
from tacit.evidence import (
    MISSING_EVIDENCE,
    observe_evidence,
    requirements_for_archetype,
    unresolved_resolutions_for_requirements,
)
from tacit.evidence_artifacts import evidence_failure_diagnostics
from tacit.investigation_contract import GroundingStatus, InvestigationContractAssembler
from tacit.knowledge.service import KnowledgeService
from tacit.knowledge.usage import KnowledgeRevisionRef
from tacit.models.schemas import (
    DashboardSpec,
    DashRequest,
    EvidenceRequirement,
    EvidenceResolution,
    EvidenceResolutionStatus,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
)
from tacit.pipeline.stages.archetypes import compile_selected_archetypes, select_archetypes
from tacit.pipeline.stages.discovery import run_discovery_stage
from tacit.pipeline.stages.evidence import run_evidence_stage
from tacit.pipeline.validation import validate_dashboard_and_evidence
from tacit.runtime_ownership import RuntimeOwnershipError
from tacit.signals import SignalStore
from tacit.signals.availability import SIGNAL_STORE_UNAVAILABLE, resolve_signal_store
from tacit.signals.resolution import (
    SignalResolutionWorkBudget,
    SignalResolutionWorkLimitError,
)
from tacit.tenancy import TenantBoundaryError


def test_tracked_package_has_no_raw_permission_error_raises() -> None:
    package_root = Path(__file__).resolve().parents[2] / "tacit"
    violations: list[str] = []

    for source_path in sorted(package_root.rglob("*.py")):
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Raise) or node.exc is None:
                continue
            raised = node.exc.func if isinstance(node.exc, ast.Call) else node.exc
            is_raw_permission_error = (
                isinstance(raised, ast.Name)
                and raised.id == "PermissionError"
                or isinstance(raised, ast.Attribute)
                and isinstance(raised.value, ast.Name)
                and raised.value.id == "builtins"
                and raised.attr == "PermissionError"
            )
            if is_raw_permission_error:
                relative_path = source_path.relative_to(package_root.parent)
                violations.append(f"{relative_path}:{node.lineno}")

    assert violations == []


def test_api_and_cli_do_not_catch_raw_permission_error_as_authorization() -> None:
    package_root = Path(__file__).resolve().parents[2] / "tacit"
    source_paths = [*sorted((package_root / "api").rglob("*.py")), package_root / "cli.py"]
    violations: list[str] = []

    for source_path in source_paths:
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler) or node.type is None:
                continue
            is_raw_permission_error = any(
                isinstance(caught, ast.Name)
                and caught.id == "PermissionError"
                or isinstance(caught, ast.Attribute)
                and isinstance(caught.value, ast.Name)
                and caught.value.id == "builtins"
                and caught.attr == "PermissionError"
                for caught in ast.walk(node.type)
            )
            if is_raw_permission_error:
                relative_path = source_path.relative_to(package_root.parent)
                violations.append(f"{relative_path}:{node.lineno}")

    assert violations == []


def test_authority_error_taxonomy_imports_from_a_fresh_interpreter():
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import tacit.config; "
                "import tacit.archetypes.generated.store; "
                "import tacit.errors; "
                "import tacit.runtime_ownership"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def _intent() -> Intent:
    return Intent(
        summary="checkout latency",
        domain="application",
        services=["checkout"],
        keywords=["latency"],
        problem_type="latency",
    )


def _metric(name: str) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid="prom",
        datasource_name="Prometheus",
        datasource_type="prometheus",
        query_language="promql",
        dimensions=["service={checkout}"],
    )


def test_evidence_failure_diagnostics_are_bounded_and_message_free() -> None:
    long_error_type = type("Sensitive" * 100, (RuntimeError,), {})
    diagnostics = evidence_failure_diagnostics(
        long_error_type("payload-secret"),
        reason_code="evidence_resolution_failed",
        requirement_count=10**9,
    )

    assert len(str(diagnostics["error_type"])) == 128
    assert diagnostics["requirement_count"] == 1_000_000
    assert diagnostics["failure_fingerprint"]
    assert "payload-secret" not in json.dumps(diagnostics)
    assert "reason_code" not in diagnostics


@pytest.mark.parametrize(
    ("error_factory", "error_type"),
    [
        (lambda: SemanticAuthorizationError("denied"), SemanticAuthorizationError),
        (lambda: RuntimeOwnershipError("owner mismatch"), RuntimeOwnershipError),
        (lambda: TenantBoundaryError("tenant denied", status_code=403), TenantBoundaryError),
    ],
)
def test_optional_signal_store_acquisition_propagates_authority_failures(
    error_factory: Any,
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        resolve_signal_store(None, lambda: (_ for _ in ()).throw(error_factory()))


def test_optional_signal_store_acquisition_degrades_ordinary_failure() -> None:
    assert (
        resolve_signal_store(
            None,
            lambda: (_ for _ in ()).throw(RuntimeError("optional store unavailable")),
        )
        is None
    )


def test_trust_guard_propagates_semantic_denial_through_optional_store_boundary() -> None:
    boundary = SimpleNamespace(
        enforce_candidate_review_action=lambda **_kwargs: None,
        _resolve_tenant=lambda _tenant_id: "tenant-a",
        _require_candidate=lambda *_args: SimpleNamespace(),
        _require_candidate_workflow=lambda *_args: None,
    )

    with pytest.raises(SemanticAuthorizationError, match="knowledge.trust") as captured:
        resolve_signal_store(
            None,
            lambda: KnowledgeService.review_candidate(
                boundary,
                "candidate-a",
                approved=True,
                reviewer="operator",
                tenant_id="tenant-a",
                trust=True,
                can_trust=False,
            ),
        )

    assert isinstance(captured.value, PermissionError)


def test_correction_workflow_guard_propagates_semantic_denial_through_optional_store_boundary() -> None:
    repository = SimpleNamespace(
        get_correction_for_candidate=lambda *_args: SimpleNamespace(id="correction-a"),
    )
    boundary = SimpleNamespace(repository=repository)

    with pytest.raises(SemanticAuthorizationError, match="correction workflow"):
        resolve_signal_store(
            None,
            lambda: KnowledgeService._require_candidate_workflow(
                boundary,
                "candidate-a",
                "tenant-a",
                None,
            ),
        )


def test_bootstrap_write_guard_propagates_semantic_denial_through_optional_store_boundary(
    tmp_path: Path,
) -> None:
    store = SignalStore(
        db_path=tmp_path / "semantic-bootstrap-guard.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )

    with pytest.raises(SemanticAuthorizationError, match="packaged catalog loader"):
        resolve_signal_store(
            None,
            lambda: store.add_mapping(
                "request_latency",
                "tenant_supplied_latency_seconds",
                source_type="bootstrap",
                tenant_id="tenant-a",
            ),
        )


def test_initial_evidence_resolution_propagates_store_acquisition_authority(
    monkeypatch: Any,
) -> None:
    archetype = _archetype(
        "authority-latency",
        "missing_default_metric",
        signal_type="request_latency",
    )
    monkeypatch.setattr(
        "tacit.signals.get_signal_store",
        lambda: (_ for _ in ()).throw(RuntimeOwnershipError("owner mismatch")),
    )

    with pytest.raises(RuntimeOwnershipError):
        run_evidence_stage(
            ranked_archetypes=[(archetype, 0.9)],
            dashboard_spec=_dashboard(archetype, "existing_metric"),
            intent=_intent(),
            catalog=[_metric("unrelated_metric")],
            target_language="promql",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_type", ["request_latency", "cpu_usage"])
async def test_evidence_rescue_propagates_store_acquisition_authority(
    monkeypatch: Any,
    signal_type: str,
) -> None:
    archetype = _archetype(
        f"authority-{signal_type}",
        f"canonical_{signal_type}",
        signal_type=signal_type,
    )
    requirements, resolutions = _unresolved_evidence(archetype)

    class PassThroughBackend:
        async def validate_queries(
            self,
            spec: DashboardSpec,
            _catalog: list[MetricEntry],
        ) -> tuple[DashboardSpec, list[str]]:
            return spec, []

    monkeypatch.setattr(
        "tacit.signals.get_signal_store",
        lambda: (_ for _ in ()).throw(RuntimeOwnershipError("owner mismatch")),
    )

    with pytest.raises(RuntimeOwnershipError):
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, PassThroughBackend()),
            dashboard_spec=_dashboard(archetype, "existing_metric"),
            catalog=[_metric("unrelated_metric")],
            evidence_requirements=requirements,
            evidence_resolutions=resolutions,
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
        )


@pytest.mark.parametrize(
    ("error_factory", "error_type"),
    [
        (lambda: SemanticAuthorizationError("denied"), SemanticAuthorizationError),
        (lambda: RuntimeOwnershipError("owner mismatch"), RuntimeOwnershipError),
        (lambda: TenantBoundaryError("tenant denied", status_code=403), TenantBoundaryError),
    ],
)
def test_generated_retrieval_propagates_authority_failures(
    tmp_path: Any,
    monkeypatch: Any,
    error_factory: Any,
    error_type: type[Exception],
) -> None:
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.load_experimental_archetypes",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(error_factory()),
    )

    with pytest.raises(error_type):
        select_archetypes(
            intent=_intent().model_copy(update={"environments": ["production"]}),
            metric_catalog=[_metric("checkout_latency_seconds")],
            catalog_for_compile=[_metric("checkout_latency_seconds")],
            target_language="promql",
            settings=Settings(
                _env_file=None,
                learned_archetypes_retrieval_mode=(ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE),
                learned_archetypes_quarantine_path=str(tmp_path),
            ),
            tenant_id="tenant-a",
        )


def _pinned_governed_signal_store(tmp_path: Any) -> tuple[SignalStore, Any]:
    store = SignalStore(
        db_path=tmp_path / "pinned-archetype-signals.db",
        runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
    )
    token = store.activate_pinned_governed_mappings(
        tenant_id="tenant-a",
        mappings=[
            {
                "tenant_id": "tenant-a",
                "signal_type": "request_latency",
                "metric_pattern": "checkout_latency_seconds",
                "governance_ref": "knowledge-request-latency",
                "governance_revision": 3,
            }
        ],
    )
    return store, token


def test_archetype_compilation_propagates_pinned_tenant_denial(tmp_path) -> None:
    store, token = _pinned_governed_signal_store(tmp_path)
    archetype = _archetype(
        "governed-latency",
        "missing_default_metric",
        signal_type="request_latency",
    )
    try:
        with pytest.raises(TenantBoundaryError) as exc_info:
            compile_archetype(
                archetype,
                _intent(),
                [_metric("checkout_latency_seconds")],
                signal_store=store,
                tenant_id="tenant-b",
            )
        assert exc_info.value.status_code == 403
    finally:
        store.reset_pinned_governed_mappings(token)


def test_archetype_ranking_propagates_pinned_tenant_denial(tmp_path) -> None:
    store, token = _pinned_governed_signal_store(tmp_path)
    archetype = _archetype(
        "governed-latency",
        "missing_default_metric",
        signal_type="request_latency",
    )
    try:
        with pytest.raises(TenantBoundaryError) as exc_info:
            rank_archetypes_by_coverage(
                [(archetype, 0.9)],
                [_metric("checkout_latency_seconds")],
                signal_store=store,
                tenant_id="tenant-b",
            )
        assert exc_info.value.status_code == 403
    finally:
        store.reset_pinned_governed_mappings(token)


def test_archetype_signal_fallback_logs_only_safe_bounded_diagnostics() -> None:
    class FailingStore:
        def resolve_signals_for_archetype(self, **_kwargs):
            raise RuntimeError("payload-secret /tmp/private tenant-secret query-secret")

    archetype = _archetype(
        "sensitive-archetype-id",
        "sensitive-default-query",
        signal_type="request_latency",
    )

    with capture_logs() as logs:
        dashboard = compile_archetype(
            archetype,
            _intent(),
            [_metric("checkout_latency_seconds")],
            signal_store=FailingStore(),
            tenant_id="tenant-secret",
        )

    assert dashboard.panels
    failure = next(item for item in logs if item["event"] == "signal_resolution_failed")
    serialized_failure = json.dumps(failure)
    assert "payload-secret" not in serialized_failure
    assert "tenant-secret" not in serialized_failure
    assert "sensitive-archetype-id" not in serialized_failure
    assert "sensitive-default-query" not in serialized_failure
    assert "traceback" not in serialized_failure.casefold()
    assert failure["reason_code"] == "archetype_signal_resolution_failed"
    assert failure["error_type"] == "RuntimeError"
    assert len(failure["failure_fingerprint"]) == 12
    assert failure["signal_count"] == 1
    assert failure["required_metric_count"] == 0
    assert set(failure) <= {
        "error_type",
        "event",
        "failure_fingerprint",
        "log_level",
        "reason_code",
        "required_metric_count",
        "signal_count",
    }


def test_archetype_coverage_fallback_logs_once_without_sensitive_state() -> None:
    class FailingStore:
        def resolve_signal_details(self, *_args, **_kwargs):
            raise RuntimeError("payload-secret /tmp/private tenant-secret query-secret")

    first = _archetype("first-sensitive-id", "missing-one", signal_type="first_signal")
    second = _archetype("second-sensitive-id", "missing-two", signal_type="second_signal")

    with capture_logs() as logs:
        ranked = rank_archetypes_by_coverage(
            [(first, 0.9), (second, 0.8)],
            [_metric("live_metric")],
            signal_store=FailingStore(),
            tenant_id="tenant-secret",
        )

    serialized = json.dumps(logs)
    assert len(ranked) == 2
    assert "payload-secret" not in serialized
    assert "tenant-secret" not in serialized
    assert "first-sensitive-id" not in serialized
    assert "second-sensitive-id" not in serialized
    assert "traceback" not in serialized.casefold()
    failures = [item for item in logs if item["event"] == "archetype_coverage_signal_resolution_failed"]
    assert len(failures) == 2
    assert all(item["error_type"] == "RuntimeError" for item in failures)
    assert all(item["reason_code"] == "archetype_coverage_signal_resolution_failed" for item in failures)
    assert all(len(item["failure_fingerprint"]) == 12 for item in failures)
    assert all(item["resolution_failure_count"] == 1 for item in failures)
    assert all(
        set(item)
        <= {
            "error_type",
            "event",
            "failure_fingerprint",
            "log_level",
            "reason_code",
            "resolution_failure_count",
        }
        for item in failures
    )


@pytest.mark.parametrize("engine_path", ["compilation", "ranking"])
@pytest.mark.parametrize(
    ("error_factory", "error_type"),
    [
        (lambda: SemanticAuthorizationError("denied"), SemanticAuthorizationError),
        (lambda: RuntimeOwnershipError("owner mismatch"), RuntimeOwnershipError),
        (lambda: TenantBoundaryError("tenant denied", status_code=403), TenantBoundaryError),
        (lambda: asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
def test_archetype_engine_propagates_authority_and_cancellation(
    engine_path: str,
    error_factory: Any,
    error_type: type[BaseException],
) -> None:
    class AuthorityFailingStore:
        def resolve_signals_for_archetype(self, **_kwargs: Any) -> Any:
            raise error_factory()

        def resolve_signal_details(self, *_args: Any, **_kwargs: Any) -> Any:
            raise error_factory()

    archetype = _archetype(
        "authority-latency",
        "missing_default_metric",
        signal_type="request_latency",
    )
    with capture_logs() as logs, pytest.raises(error_type):
        if engine_path == "compilation":
            compile_archetype(
                archetype,
                _intent(),
                [_metric("checkout_latency_seconds")],
                signal_store=AuthorityFailingStore(),
            )
        else:
            rank_archetypes_by_coverage(
                [(archetype, 0.9)],
                [_metric("checkout_latency_seconds")],
                signal_store=AuthorityFailingStore(),
            )

    assert not {
        "signal_resolution_failed",
        "archetype_coverage_signal_resolution_failed",
    } & {item.get("event") for item in logs}


@pytest.mark.parametrize("engine_path", ["compilation", "ranking"])
def test_archetype_engine_propagates_authority_from_store_acquisition(
    monkeypatch: Any,
    engine_path: str,
) -> None:
    def deny_store_acquisition() -> Any:
        raise RuntimeOwnershipError("owner mismatch")

    monkeypatch.setattr("tacit.signals.get_signal_store", deny_store_acquisition)
    archetype = _archetype(
        "authority-latency",
        "missing_default_metric",
        signal_type="request_latency",
    )

    with pytest.raises(RuntimeOwnershipError):
        if engine_path == "compilation":
            compile_archetype(
                archetype,
                _intent(),
                [_metric("checkout_latency_seconds")],
            )
        else:
            rank_archetypes_by_coverage(
                [(archetype, 0.9)],
                [_metric("checkout_latency_seconds")],
            )


@pytest.mark.parametrize(
    ("engine_path", "failure_event", "reason_code"),
    [
        ("compilation", "signal_resolution_failed", "archetype_signal_resolution_failed"),
        (
            "ranking",
            "archetype_coverage_signal_resolution_failed",
            "archetype_coverage_signal_resolution_failed",
        ),
    ],
)
def test_archetype_engine_degrades_ordinary_store_acquisition_failure(
    monkeypatch: Any,
    engine_path: str,
    failure_event: str,
    reason_code: str,
) -> None:
    canaries = "payload-secret /tmp/private tenant-secret query-secret"

    def fail_store_acquisition() -> Any:
        raise RuntimeError(canaries)

    monkeypatch.setattr("tacit.signals.get_signal_store", fail_store_acquisition)
    archetype = _archetype(
        "sensitive-archetype-id",
        "sensitive-default-query",
        signal_type="request_latency",
    )

    with capture_logs() as logs:
        if engine_path == "compilation":
            dashboard = compile_archetype(
                archetype,
                _intent(),
                [_metric("checkout_latency_seconds")],
                tenant_id="tenant-secret",
            )
            assert dashboard.panels
        else:
            ranked = rank_archetypes_by_coverage(
                [(archetype, 0.9)],
                [_metric("checkout_latency_seconds")],
                tenant_id="tenant-secret",
            )
            assert ranked

    [failure] = [item for item in logs if item.get("event") == failure_event]
    assert failure["reason_code"] == reason_code
    assert failure["error_type"] == "RuntimeError"
    assert len(failure["failure_fingerprint"]) == 12
    serialized_failure = json.dumps(failure)
    for canary in canaries.split():
        assert canary not in serialized_failure


def _archetype(
    archetype_id: str,
    metric: str,
    *,
    tags: list[str] | None = None,
    signal_type: str = "",
) -> InvestigationArchetype:
    return InvestigationArchetype(
        id=archetype_id,
        name=archetype_id.replace("-", " ").title(),
        problem_types=["latency"],
        required_metrics=[] if signal_type else [metric],
        required_signals=[signal_type] if signal_type else [],
        signal_bindings={signal_type: metric} if signal_type else {},
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr=metric)])],
        tags=tags or [],
    )


def _coverage_archetype(
    archetype_id: str,
    signal_types: list[str],
    *,
    required_metrics: list[str] | None = None,
) -> InvestigationArchetype:
    return InvestigationArchetype(
        id=archetype_id,
        name=archetype_id,
        problem_types=["latency"],
        required_signals=signal_types,
        signal_bindings={signal: f"missing_{signal}" for signal in signal_types},
        required_metrics=required_metrics or [],
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])],
    )


def _unvalidated_query_shape_archetype(
    query_counts: list[int],
) -> InvestigationArchetype:
    query = QueryTemplate(expr="live_metric")
    panels = [
        PanelTemplate.model_construct(
            title=f"Panel {index}",
            description="",
            panel_type="timeseries",
            row="",
            queries=[query] * query_count,
            unit="",
        )
        for index, query_count in enumerate(query_counts)
    ]
    return InvestigationArchetype.model_construct(
        id="unvalidated-query-shape",
        name="Unvalidated query shape",
        description="",
        problem_types=["latency"],
        required_metrics=[],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "missing_request_latency"},
        panels=panels,
        tags=[],
        default_timerange="1h",
    )


class _CountingCoverageStore:
    def __init__(self, revisions_by_archetype: dict[str, KnowledgeRevisionRef]) -> None:
        self.revisions_by_archetype = revisions_by_archetype
        self.calls: list[tuple[str, frozenset[KnowledgeRevisionRef]]] = []

    def resolve_signal_details(
        self,
        _signal_type: str,
        _catalog: list[MetricEntry],
        **kwargs: Any,
    ) -> list[Any]:
        archetype_id = str(kwargs["context_archetype"])
        excluded = frozenset(kwargs.get("excluded_knowledge_refs", set()))
        self.calls.append((archetype_id, excluded))
        revision_ref = self.revisions_by_archetype[archetype_id]
        if revision_ref in excluded:
            return []
        return [SimpleNamespace(knowledge_revision_ref=revision_ref)]


class _StrictExternalCoverageStore:
    """External compatibility double that intentionally lacks work_budget."""

    def resolve_signal_details(
        self,
        _signal_type: str,
        _catalog: list[MetricEntry],
        *,
        context_service: str = "",
        context_datasource_type: str = "",
        context_archetype: str = "",
        tenant_id: str = "default",
        knowledge_scope: Any | None = None,
        excluded_knowledge_refs: set[KnowledgeRevisionRef] | None = None,
    ) -> list[Any]:
        del (
            context_service,
            context_datasource_type,
            context_archetype,
            tenant_id,
            knowledge_scope,
            excluded_knowledge_refs,
        )
        return [SimpleNamespace(knowledge_revision_ref=None)]


class _NeverTraversedText:
    def __len__(self) -> int:
        raise AssertionError("scalar traversal happened before cardinality admission")

    def __str__(self) -> str:
        raise AssertionError("scalar conversion happened before cardinality admission")


@pytest.mark.parametrize(
    ("field_name", "limit"),
    [
        ("required_metrics", MAX_ARCHETYPE_REQUIRED_METRICS),
        ("required_signals", MAX_ARCHETYPE_REQUIRED_SIGNALS),
        ("signal_bindings", MAX_ARCHETYPE_SIGNAL_BINDINGS),
    ],
)
def test_archetype_schema_bounds_coverage_collections(
    field_name: str,
    limit: int,
) -> None:
    at_limit: Any = [f"item_{index}" for index in range(limit)]
    oversized: Any = [f"item_{index}" for index in range(limit + 1)]
    if field_name == "signal_bindings":
        at_limit = {f"signal_{index}": f"metric_{index}" for index in range(limit)}
        oversized = {f"signal_{index}": f"metric_{index}" for index in range(limit + 1)}

    valid = InvestigationArchetype(
        id="at-limit",
        name="At limit",
        problem_types=["latency"],
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])],
        **{field_name: at_limit},
    )
    assert len(getattr(valid, field_name)) == limit

    with pytest.raises(ValidationError):
        InvestigationArchetype(
            id="oversized",
            name="Oversized",
            problem_types=["latency"],
            panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])],
            **{field_name: oversized},
        )


def test_archetype_schema_bounds_combined_signal_requirements() -> None:
    required_count = MAX_ARCHETYPE_SIGNAL_REQUIREMENTS // 2
    binding_count = MAX_ARCHETYPE_SIGNAL_REQUIREMENTS - required_count + 1

    with pytest.raises(ValidationError):
        InvestigationArchetype(
            id="oversized-combined-signals",
            name="Oversized combined signals",
            problem_types=["latency"],
            required_signals=[f"required_{index}" for index in range(required_count)],
            signal_bindings={f"binding_{index}": f"metric_{index}" for index in range(binding_count)},
            panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])],
        )


def test_archetype_schema_bounds_panels() -> None:
    panel = PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])

    at_limit = InvestigationArchetype(
        id="panels-at-limit",
        name="Panels at limit",
        problem_types=["latency"],
        panels=[panel] * MAX_ARCHETYPE_PANELS,
    )
    assert len(at_limit.panels) == MAX_ARCHETYPE_PANELS

    with pytest.raises(ValidationError):
        InvestigationArchetype(
            id="panels-over-limit",
            name="Panels over limit",
            problem_types=["latency"],
            panels=[panel] * (MAX_ARCHETYPE_PANELS + 1),
        )


def test_panel_schema_bounds_queries() -> None:
    query = QueryTemplate(expr="live_metric")

    at_limit = PanelTemplate(
        title="Queries at limit",
        queries=[query] * MAX_ARCHETYPE_QUERIES_PER_PANEL,
    )
    assert len(at_limit.queries) == MAX_ARCHETYPE_QUERIES_PER_PANEL

    with pytest.raises(ValidationError):
        PanelTemplate(
            title="Queries over limit",
            queries=[query] * (MAX_ARCHETYPE_QUERIES_PER_PANEL + 1),
        )


def test_archetype_schema_bounds_total_queries() -> None:
    query = QueryTemplate(expr="live_metric")
    first_count = MAX_ARCHETYPE_TOTAL_QUERIES // 2
    second_count = MAX_ARCHETYPE_TOTAL_QUERIES - first_count

    at_limit = InvestigationArchetype(
        id="total-queries-at-limit",
        name="Total queries at limit",
        problem_types=["latency"],
        panels=[
            PanelTemplate(title="First", queries=[query] * first_count),
            PanelTemplate(title="Second", queries=[query] * second_count),
        ],
    )
    assert sum(len(panel.queries) for panel in at_limit.panels) == MAX_ARCHETYPE_TOTAL_QUERIES

    with pytest.raises(ValidationError):
        InvestigationArchetype(
            id="total-queries-over-limit",
            name="Total queries over limit",
            problem_types=["latency"],
            panels=[
                PanelTemplate(title="First", queries=[query] * first_count),
                PanelTemplate(title="Second", queries=[query] * (second_count + 1)),
            ],
        )


@pytest.mark.parametrize(
    ("query_counts", "limits", "dimension"),
    [
        (
            [1, 1, 1],
            ArchetypeCoverageWorkLimits(max_panels_per_archetype=2),
            "panels_per_archetype",
        ),
        (
            [3],
            ArchetypeCoverageWorkLimits(max_queries_per_panel=2),
            "queries_per_panel",
        ),
        (
            [2, 2],
            ArchetypeCoverageWorkLimits(
                max_panels_per_archetype=2,
                max_queries_per_panel=2,
                max_total_queries_per_archetype=3,
            ),
            "total_queries_per_archetype",
        ),
    ],
)
def test_archetype_coverage_query_shape_limits_handle_unvalidated_models(
    query_counts: list[int],
    limits: ArchetypeCoverageWorkLimits,
    dimension: str,
) -> None:
    archetype = _unvalidated_query_shape_archetype(query_counts)
    store = _CountingCoverageStore({archetype.id: KnowledgeRevisionRef("knowledge-latency", 1)})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")],
            signal_store=store,
            work_limits=limits,
        )

    assert exc_info.value.dimension == dimension
    assert store.calls == []


def test_archetype_coverage_query_shape_limits_allow_valid_inputs() -> None:
    archetype = _unvalidated_query_shape_archetype([2, 2])
    store = _CountingCoverageStore({archetype.id: KnowledgeRevisionRef("knowledge-latency", 1)})

    ranked = rank_archetypes_by_coverage(
        [(archetype, 0.9)],
        [_metric("live_metric")],
        signal_store=store,
        work_limits=ArchetypeCoverageWorkLimits(
            max_panels_per_archetype=2,
            max_queries_per_panel=2,
            max_total_queries_per_archetype=4,
        ),
    )

    assert ranked == [(archetype, 0.9)]
    assert store.calls == [(archetype.id, frozenset())]


def test_archetype_coverage_preserves_external_store_without_budget_keyword() -> None:
    archetype = _coverage_archetype("latency", ["request_latency"])

    ranked = rank_archetypes_by_coverage(
        [(archetype, 0.9)],
        [_metric("live_metric")],
        signal_store=_StrictExternalCoverageStore(),
    )

    assert ranked == [(archetype, 0.9)]


def test_archetype_base_and_counterfactual_resolution_share_one_aggregate_budget(
    tmp_path,
) -> None:
    store = SignalStore(db_path=tmp_path / "aggregate-archetype-resolution.db")
    store.register_signal_type("request_latency")
    store._add_bootstrap_mapping("request_latency", "live_metric", 0.8)
    governed_mapping = {
        "tenant_id": "default",
        "signal_type": "request_latency",
        "metric_pattern": "live_metric",
        "confidence": 0.99,
        "source_type": "operational_knowledge",
        "source_refs": ["source-a"],
        "review_state": "approved",
        "governance_ref": "knowledge-request-latency",
        "governance_revision": 1,
        "context_services": [],
        "context_datasource_types": [],
        "context_environments": [],
        "context_archetypes": [],
        "context_regions": [],
        "context_clusters": [],
        "context_namespaces": [],
        "context_versions": [],
        "last_seen": time.time(),
        "positive_feedback": 0,
        "negative_feedback": 0,
    }
    token = store.activate_pinned_governed_mappings(
        tenant_id="default",
        mappings=[governed_mapping],
    )
    budget = SignalResolutionWorkBudget(
        max_calls=4,
        max_mapping_catalog_comparisons=2,
        max_results=4,
    )
    try:
        with pytest.raises(SignalResolutionWorkLimitError) as exc_info:
            rank_archetypes_by_coverage(
                [(_coverage_archetype("latency", ["request_latency"]), 0.9)],
                [_metric("live_metric")],
                signal_store=store,
                knowledge_stage_uses=[],
                resolution_work_budget=budget,
            )
    finally:
        store.reset_pinned_governed_mappings(token)

    assert exc_info.value.dimension == "mapping_catalog_comparisons"
    assert budget.calls == 2
    assert budget.mapping_catalog_comparisons == 2
    assert budget.results_constructed == 1


class _PipelineBudgetStore:
    """First-party resolver double that records one shared pipeline budget."""

    supports_signal_resolution_work_budget = True

    def __init__(self, *, cancel_on: str = "") -> None:
        self.cancel_on = cancel_on
        self.events: list[str] = []
        self.budgets: list[SignalResolutionWorkBudget] = []

    def _consume(self, event: str, work_budget: SignalResolutionWorkBudget) -> None:
        if event == self.cancel_on:
            raise asyncio.CancelledError
        assert work_budget is not None
        work_budget.begin_call()
        self.events.append(event)
        self.budgets.append(work_budget)

    def resolve_signal_details(
        self,
        signal_type: str,
        _catalog: list[MetricEntry],
        *,
        work_budget: SignalResolutionWorkBudget,
        context_archetype: str = "",
        **_kwargs: Any,
    ) -> list[Any]:
        if not context_archetype:
            event = "discovery"
        elif context_archetype == "selection-budget":
            event = "selection"
        elif context_archetype == "initial-budget":
            event = "initial_evidence"
        elif signal_type == "request_latency":
            event = "symptom_rescue"
        else:
            event = "gap_rescue"
        self._consume(event, work_budget)
        return [
            SimpleNamespace(
                entry=_metric(f"checkout_{signal_type}"),
                confidence=0.95,
                governance_ref="",
                knowledge_revision_ref=None,
            )
        ]

    def resolve_signals_for_archetype(
        self,
        *,
        work_budget: SignalResolutionWorkBudget,
        **_kwargs: Any,
    ) -> dict[str, str]:
        self._consume("compilation", work_budget)
        return {}


class _BudgetPassThroughBackend:
    name = "prometheus"
    query_language = "promql"

    def __init__(self) -> None:
        self.validation_calls = 0
        self.publish_calls = 0

    async def validate_queries(
        self,
        spec: DashboardSpec,
        _catalog: list[MetricEntry],
    ) -> tuple[DashboardSpec, list[str]]:
        self.validation_calls += 1
        return spec, []

    async def discover_metrics(self, _keywords: list[str], _intent: Intent) -> list[MetricEntry]:
        return [_metric("live_request_latency")]

    async def publish(self, _spec: DashboardSpec) -> None:
        self.publish_calls += 1


def _pipeline_budget_requirements() -> tuple[list[EvidenceRequirement], list[EvidenceResolution]]:
    requirements = [
        EvidenceRequirement(
            id="symptom-budget:1",
            evidence_type="semantic_signal",
            signal_type="request_latency",
            default_metric="missing_request_latency",
            priority="critical",
            service_scope=["checkout"],
            source="symptom-budget",
        ),
        EvidenceRequirement(
            id="gap-budget:1",
            evidence_type="semantic_signal",
            signal_type="cpu_usage",
            default_metric="missing_cpu_usage",
            priority="critical",
            service_scope=["checkout"],
            source="gap-budget",
        ),
    ]
    return requirements, unresolved_resolutions_for_requirements(
        requirements,
        reason_code="no_compatible_live_signal",
    )


async def _exercise_pipeline_signal_resolution_stages(
    monkeypatch: Any,
    *,
    budget: SignalResolutionWorkBudget,
    store: _PipelineBudgetStore,
    backend: _BudgetPassThroughBackend,
) -> Any:
    discovery_intent = _intent().model_copy(
        update={
            "keyword_evidence": [
                {
                    "keyword": "latency",
                    "score": 0.8,
                    "tier": "colloquial",
                    "source": "prompt",
                }
            ]
        }
    )
    recorder = SimpleNamespace(
        discovery=lambda *_args, **_kwargs: None,
        stage=lambda *_args, **_kwargs: None,
    )
    await run_discovery_stage(
        backends=[cast(DashboardBackend, backend)],
        primary=cast(DashboardBackend, backend),
        intent=discovery_intent,
        timings={},
        recorder=recorder,
        signal_store=store,
        signal_resolution_work_budget=budget,
    )

    selection_archetype = _archetype(
        "selection-budget",
        "missing_request_latency",
        signal_type="request_latency",
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        lambda *_args, **_kwargs: [(selection_archetype, 0.9)],
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        lambda *_args, **_kwargs: [],
    )
    selection = select_archetypes(
        intent=_intent(),
        metric_catalog=[_metric("live_request_latency")],
        catalog_for_compile=[_metric("live_request_latency")],
        target_language="promql",
        settings=Settings(_env_file=None),
        signal_store=store,
        resolution_work_budget=budget,
    )
    compilation = compile_selected_archetypes(
        selection=selection,
        intent=_intent(),
        catalog_for_compile=[_metric("missing_request_latency")],
        timings={},
        signal_store=store,
        resolution_work_budget=budget,
    )
    assert compilation is not None

    initial_archetype = _archetype(
        "initial-budget",
        "missing_initial_latency",
        signal_type="initial_latency",
    )
    run_evidence_stage(
        ranked_archetypes=[(initial_archetype, 0.9)],
        dashboard_spec=_dashboard(initial_archetype, "missing_initial_latency"),
        intent=_intent(),
        catalog=[_metric("checkout_initial_latency")],
        target_language="promql",
        signal_store=store,
        signal_resolution_work_budget=budget,
    )

    requirements, resolutions = _pipeline_budget_requirements()
    return await validate_dashboard_and_evidence(
        primary=cast(DashboardBackend, backend),
        dashboard_spec=_dashboard(selection_archetype, "existing_dashboard_metric"),
        catalog=[
            _metric("checkout_request_latency"),
            _metric("checkout_cpu_usage"),
        ],
        evidence_requirements=requirements,
        evidence_resolutions=resolutions,
        intent=_intent(),
        target_language="promql",
        ranked_archetypes_present=True,
        record_stage=lambda *_args, **_kwargs: None,
        signal_store=store,
        signal_resolution_work_budget=budget,
    )


@pytest.mark.asyncio
async def test_pipeline_resolution_stages_share_one_budget_at_the_exact_limit(monkeypatch: Any) -> None:
    budget = SignalResolutionWorkBudget(
        max_calls=6,
        max_mapping_catalog_comparisons=100,
        max_results=100,
    )
    store = _PipelineBudgetStore()
    backend = _BudgetPassThroughBackend()

    result = await _exercise_pipeline_signal_resolution_stages(
        monkeypatch,
        budget=budget,
        store=store,
        backend=backend,
    )

    assert store.events == [
        "discovery",
        "selection",
        "compilation",
        "initial_evidence",
        "symptom_rescue",
        "gap_rescue",
    ]
    assert store.budgets == [budget] * 6
    assert budget.calls == 6
    assert result.evidence_summary["resolution_call_count"] == 6
    assert result.evidence_summary["resolution_call_limit"] == 6
    assert result.evidence_summary["signal_resolution_work_limit_exhausted"] == 0
    assert backend.publish_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("max_calls", "preconsumed_calls", "expected_events"),
    [
        (1, 1, []),
        (1, 0, ["discovery"]),
        (2, 0, ["discovery", "selection"]),
        (3, 0, ["discovery", "selection", "compilation"]),
        (4, 0, ["discovery", "selection", "compilation", "initial_evidence"]),
        (5, 0, ["discovery", "selection", "compilation", "initial_evidence", "symptom_rescue"]),
    ],
)
async def test_pipeline_resolution_stages_fail_closed_at_limit_plus_one(
    monkeypatch: Any,
    max_calls: int,
    preconsumed_calls: int,
    expected_events: list[str],
) -> None:
    budget = SignalResolutionWorkBudget(
        max_calls=max_calls,
        max_mapping_catalog_comparisons=100,
        max_results=100,
    )
    for _ in range(preconsumed_calls):
        budget.begin_call()
    store = _PipelineBudgetStore()
    backend = _BudgetPassThroughBackend()

    with pytest.raises(SignalResolutionWorkLimitError) as exc_info:
        await _exercise_pipeline_signal_resolution_stages(
            monkeypatch,
            budget=budget,
            store=store,
            backend=backend,
        )

    assert exc_info.value.dimension == "calls"
    assert store.events == expected_events
    assert budget.counters()["signal_resolution_work_limit_exhausted"] == 1
    assert backend.publish_calls == 0


@pytest.mark.asyncio
async def test_pipeline_resolution_cancellation_propagates_without_publication(monkeypatch: Any) -> None:
    budget = SignalResolutionWorkBudget(
        max_calls=6,
        max_mapping_catalog_comparisons=100,
        max_results=100,
    )
    store = _PipelineBudgetStore(cancel_on="gap_rescue")
    backend = _BudgetPassThroughBackend()

    with pytest.raises(asyncio.CancelledError):
        await _exercise_pipeline_signal_resolution_stages(
            monkeypatch,
            budget=budget,
            store=store,
            backend=backend,
        )

    assert backend.publish_calls == 0


def test_archetype_coverage_rejects_twenty_thousand_queries_before_resolution() -> None:
    archetype = _unvalidated_query_shape_archetype([20_000])
    store = _CountingCoverageStore({archetype.id: KnowledgeRevisionRef("knowledge-latency", 1)})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")],
            signal_store=store,
        )

    assert exc_info.value.dimension == "queries_per_panel"
    assert exc_info.value.observed == 20_000
    assert store.calls == []


def test_archetype_coverage_candidate_limit_fails_before_resolution() -> None:
    revision_ref = KnowledgeRevisionRef("knowledge-latency", 1)
    store = _CountingCoverageStore(
        {
            "first": revision_ref,
            "second": revision_ref,
            "third": revision_ref,
        }
    )
    candidates = [
        (_coverage_archetype("first", ["first_signal"]), 0.9),
        (_coverage_archetype("second", ["second_signal"]), 0.8),
        (_coverage_archetype("third", ["third_signal"]), 0.7),
    ]
    limits = ArchetypeCoverageWorkLimits(max_candidates=2)

    at_limit = rank_archetypes_by_coverage(
        candidates[:2],
        [_metric("live_metric")],
        signal_store=store,
        work_limits=limits,
    )
    assert len(at_limit) == 2
    store.calls.clear()

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            candidates,
            [_metric("live_metric")],
            signal_store=store,
            work_limits=limits,
        )

    assert exc_info.value.dimension == "candidate_count"
    assert store.calls == []


def test_archetype_coverage_requirement_limit_handles_unvalidated_models() -> None:
    archetype = InvestigationArchetype.model_construct(
        id="unvalidated",
        name="Unvalidated",
        problem_types=["latency"],
        required_signals=["one", "two", "three"],
        signal_bindings={},
        required_metrics=[],
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])],
        tags=[],
        description="",
        default_timerange="1h",
    )
    store = _CountingCoverageStore({"unvalidated": KnowledgeRevisionRef("knowledge-latency", 1)})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")],
            signal_store=store,
            work_limits=ArchetypeCoverageWorkLimits(max_signal_requirements_per_archetype=2),
        )

    assert exc_info.value.dimension == "signal_requirements_per_archetype"
    assert store.calls == []


@pytest.mark.parametrize(
    ("field_name", "values", "limits", "dimension"),
    [
        (
            "required_signals",
            ["same"] * 3,
            ArchetypeCoverageWorkLimits(max_required_signals_per_archetype=2),
            "required_signals_per_archetype",
        ),
        (
            "required_metrics",
            ["same"] * 3,
            ArchetypeCoverageWorkLimits(max_required_metrics_per_archetype=2),
            "required_metrics_per_archetype",
        ),
        (
            "tags",
            ["same"] * 3,
            ArchetypeCoverageWorkLimits(max_tags_per_archetype=2),
            "tags_per_archetype",
        ),
    ],
)
def test_archetype_coverage_bounds_raw_duplicate_heavy_collections_before_deduplication(
    field_name: str,
    values: list[str],
    limits: ArchetypeCoverageWorkLimits,
    dimension: str,
) -> None:
    updates = {
        "required_signals": [],
        "required_metrics": [],
        "signal_bindings": {},
        "tags": [],
    }
    updates[field_name] = values
    archetype = InvestigationArchetype.model_construct(
        id="raw-duplicate-heavy",
        name="Raw duplicate heavy",
        problem_types=["latency"],
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="live_metric")])],
        description="",
        default_timerange="1h",
        **updates,
    )
    store = _CountingCoverageStore({})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")],
            signal_store=store,
            work_limits=limits,
        )

    assert exc_info.value.dimension == dimension
    assert store.calls == []


def test_archetype_coverage_bounds_catalog_before_service_matching_or_resolution(monkeypatch) -> None:
    archetype = _coverage_archetype("latency", ["request_latency"])
    store = _CountingCoverageStore({})
    service_match_calls = 0

    def service_match(*_args, **_kwargs):
        nonlocal service_match_calls
        service_match_calls += 1
        return True

    monkeypatch.setattr("tacit.catalog.metric_matches_services", service_match)

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")] * 3,
            services=["checkout"],
            signal_store=store,
            work_limits=ArchetypeCoverageWorkLimits(max_catalog_entries=2),
        )

    assert exc_info.value.dimension == "catalog_entries"
    assert service_match_calls == 0
    assert store.calls == []


def test_archetype_coverage_catalog_count_precedes_scalar_traversal() -> None:
    archetype = _coverage_archetype("latency", ["request_latency"])
    trap = _NeverTraversedText()
    catalog = [
        MetricEntry.model_construct(
            name=trap,
            datasource_uid="prometheus",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            datasource_is_default=False,
            query_language="promql",
            namespace="",
            dimensions=[],
            unit="",
            metric_type="",
        )
        for _ in range(3)
    ]

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            catalog,
            signal_store=_StrictExternalCoverageStore(),
            work_limits=ArchetypeCoverageWorkLimits(max_catalog_entries=2),
        )

    assert exc_info.value.dimension == "catalog_entries"


def test_archetype_coverage_query_fanout_precedes_scalar_traversal() -> None:
    query = QueryTemplate.model_construct(
        expr=_NeverTraversedText(),
        legend_format="",
        query_language="promql",
        datasource_type="prometheus",
        cloudwatch_namespace="",
        cloudwatch_stat="",
        cloudwatch_dimensions={},
        cloudwatch_region="",
    )
    panel = PanelTemplate.model_construct(
        title="Latency",
        description="",
        panel_type="timeseries",
        row="",
        queries=[query, query, query],
        unit="",
    )
    archetype = InvestigationArchetype.model_construct(
        id="unvalidated",
        name="Unvalidated",
        description="",
        problem_types=["latency"],
        required_metrics=[],
        required_signals=["request_latency"],
        signal_bindings={},
        panels=[panel],
        tags=[],
        default_timerange="1h",
    )

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")],
            signal_store=_StrictExternalCoverageStore(),
            work_limits=ArchetypeCoverageWorkLimits(max_queries_per_panel=2),
        )

    assert exc_info.value.dimension == "queries_per_panel"


def test_archetype_coverage_rejects_model_construct_utf8_scalar_before_matching(
    monkeypatch,
) -> None:
    archetype = _coverage_archetype("latency", ["request_latency"])
    metric = MetricEntry.model_construct(
        name="é" * 20,
        datasource_uid="prometheus",
        datasource_name="Prometheus",
        datasource_type="prometheus",
        datasource_is_default=False,
        query_language="promql",
        namespace="",
        dimensions=[],
        unit="",
        metric_type="",
    )
    match_calls = 0

    def match(*_args: Any, **_kwargs: Any) -> bool:
        nonlocal match_calls
        match_calls += 1
        return True

    monkeypatch.setattr("tacit.signals.store._metric_matches_pattern", match)

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [metric],
            signal_store=_StrictExternalCoverageStore(),
            work_limits=ArchetypeCoverageWorkLimits(
                max_scalar_characters=32,
                max_scalar_utf8_bytes=32,
            ),
        )

    assert exc_info.value.dimension == "scalar_utf8_bytes"
    assert match_calls == 0


def test_compile_rejects_model_construct_archetype_scalar_before_resolution() -> None:
    class Store:
        calls = 0

        def resolve_signals_for_archetype(self, **_kwargs: Any) -> dict[str, str]:
            self.calls += 1
            return {}

    store = Store()
    query = QueryTemplate.model_construct(
        expr="x" * 65,
        legend_format="",
        query_language="promql",
        datasource_type="prometheus",
        cloudwatch_namespace="",
        cloudwatch_stat="",
        cloudwatch_dimensions={},
        cloudwatch_region="",
    )
    archetype = InvestigationArchetype.model_construct(
        id="unvalidated",
        name="Unvalidated",
        description="",
        problem_types=["latency"],
        required_metrics=[],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "missing_metric"},
        panels=[
            PanelTemplate.model_construct(
                title="Latency",
                description="",
                panel_type="timeseries",
                row="",
                queries=[query],
                unit="",
            )
        ],
        tags=[],
        default_timerange="1h",
    )

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        compile_archetype(
            archetype,
            _intent(),
            [_metric("live_metric")],
            signal_store=store,
            work_limits=ArchetypeCoverageWorkLimits(
                max_scalar_characters=64,
            ),
        )

    assert exc_info.value.dimension == "scalar_characters"
    assert store.calls == 0


def test_archetype_coverage_aggregate_catalog_comparison_budget_fails_before_resolution() -> None:
    archetype = _coverage_archetype(
        "latency",
        ["request_latency"],
        required_metrics=["live_metric"],
    )
    store = _CountingCoverageStore({})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [(archetype, 0.9)],
            [_metric("live_metric")] * 2,
            services=["checkout"],
            signal_store=store,
            work_limits=ArchetypeCoverageWorkLimits(max_total_catalog_comparisons=1),
        )

    assert exc_info.value.dimension == "total_catalog_comparisons"
    assert store.calls == []


@pytest.mark.parametrize(
    ("field_name", "limit"),
    [
        ("service_refs", MAX_GENERATED_QUERY_SERVICE_REFS),
        ("environment_refs", MAX_GENERATED_QUERY_ENVIRONMENT_REFS),
    ],
)
def test_generated_exact_scope_bounds_raw_duplicate_heavy_refs_before_normalization(
    field_name: str,
    limit: int,
) -> None:
    values = ["same"] * (limit + 1)
    kwargs = {
        "tenant_id": "tenant-a",
        "service_refs": ["checkout"],
        "environment_refs": [],
    }
    kwargs[field_name] = values

    with pytest.raises(GeneratedArchetypeQueryWorkLimitError) as exc_info:
        GeneratedArchetypeQuery.exact(**kwargs)

    assert exc_info.value.dimension == field_name
    assert exc_info.value.observed == limit + 1


def test_archetype_coverage_rescores_only_revision_affected_candidates() -> None:
    first_ref = KnowledgeRevisionRef("knowledge-first", 1)
    second_ref = KnowledgeRevisionRef("knowledge-second", 2)
    store = _CountingCoverageStore({"first": first_ref, "second": second_ref})
    stage_uses: list[Any] = []

    ranked = rank_archetypes_by_coverage(
        [
            (_coverage_archetype("first", ["first_signal"]), 0.9),
            (_coverage_archetype("second", ["second_signal"]), 0.8),
            (_coverage_archetype("fallback", [], required_metrics=["live_metric"]), 0.7),
        ],
        [_metric("live_metric")],
        max_archetypes=3,
        signal_store=store,
        knowledge_stage_uses=stage_uses,
        work_limits=ArchetypeCoverageWorkLimits(
            max_unique_revisions=2,
            max_counterfactual_candidate_scores=2,
            max_total_resolver_calls=4,
        ),
    )

    assert [archetype.id for archetype, _ in ranked] == ["first", "second", "fallback"]
    assert store.calls == [
        ("first", frozenset()),
        ("second", frozenset()),
        ("first", frozenset({first_ref})),
        ("second", frozenset({second_ref})),
    ]
    assert [(use.revision_ref, use.target_ref) for use in stage_uses] == [
        (first_ref, "archetype:first"),
        (second_ref, "archetype:second"),
    ]


def test_archetype_coverage_total_work_limit_stops_before_counterfactual_calls() -> None:
    first_ref = KnowledgeRevisionRef("knowledge-first", 1)
    second_ref = KnowledgeRevisionRef("knowledge-second", 2)
    store = _CountingCoverageStore({"first": first_ref, "second": second_ref})

    with capture_logs() as logs, pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [
                (_coverage_archetype("first", ["first_signal"]), 0.9),
                (_coverage_archetype("second", ["second_signal"]), 0.8),
            ],
            [_metric("live_metric")],
            signal_store=store,
            knowledge_stage_uses=[],
            work_limits=ArchetypeCoverageWorkLimits(max_total_resolver_calls=3),
        )

    assert exc_info.value.dimension == "total_resolver_calls"
    assert store.calls == [("first", frozenset()), ("second", frozenset())]
    [failure] = [item for item in logs if item["event"] == "archetype_coverage_work_limit_exceeded"]
    assert failure == {
        "event": "archetype_coverage_work_limit_exceeded",
        "reason_code": "archetype_coverage_work_limit_exceeded",
        "dimension": "total_resolver_calls",
        "observed": 4,
        "limit": 3,
        "log_level": "warning",
    }


def test_archetype_coverage_unique_revision_limit_stops_before_counterfactual_calls() -> None:
    first_ref = KnowledgeRevisionRef("knowledge-first", 1)
    second_ref = KnowledgeRevisionRef("knowledge-second", 2)
    store = _CountingCoverageStore({"first": first_ref, "second": second_ref})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [
                (_coverage_archetype("first", ["first_signal"]), 0.9),
                (_coverage_archetype("second", ["second_signal"]), 0.8),
            ],
            [_metric("live_metric")],
            signal_store=store,
            knowledge_stage_uses=[],
            work_limits=ArchetypeCoverageWorkLimits(max_unique_revisions=1),
        )

    assert exc_info.value.dimension == "unique_knowledge_revisions"
    assert store.calls == [("first", frozenset()), ("second", frozenset())]


def test_archetype_coverage_counterfactual_fanout_is_bounded() -> None:
    shared_ref = KnowledgeRevisionRef("knowledge-shared", 1)
    store = _CountingCoverageStore({"first": shared_ref, "second": shared_ref})

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        rank_archetypes_by_coverage(
            [
                (_coverage_archetype("first", ["first_signal"]), 0.9),
                (_coverage_archetype("second", ["second_signal"]), 0.8),
            ],
            [_metric("live_metric")],
            signal_store=store,
            knowledge_stage_uses=[],
            work_limits=ArchetypeCoverageWorkLimits(max_counterfactual_candidate_scores=1),
        )

    assert exc_info.value.dimension == "counterfactual_candidate_scores"
    assert store.calls == [("first", frozenset()), ("second", frozenset())]


def _dashboard(archetype: InvestigationArchetype, metric: str) -> DashboardSpec:
    return DashboardSpec(
        title="Checkout latency",
        panels=[
            PanelSpec(
                title="Latency",
                source_archetype=archetype.id,
                queries=[
                    PanelQuery(
                        expr=metric,
                        datasource_uid="prom",
                        datasource_type="prometheus",
                        query_language="promql",
                    )
                ],
            )
        ],
    )


def _pinned_governed_mapping(signal_type: str, metric: str) -> dict[str, Any]:
    return {
        "tenant_id": "tenant-a",
        "signal_type": signal_type,
        "metric_pattern": metric,
        "governance_ref": f"knowledge-{signal_type}",
        "governance_revision": 1,
    }


def _unresolved_evidence(
    archetype: InvestigationArchetype,
) -> tuple[list[Any], list[Any]]:
    requirements = requirements_for_archetype(archetype, _intent())
    resolutions = unresolved_resolutions_for_requirements(
        requirements,
        reason_code="no_compatible_live_signal",
    )
    return requirements, resolutions


def test_archetype_coverage_policy_is_request_scoped(monkeypatch: Any) -> None:
    generic = _archetype("generic", "request_latency_seconds")
    learned = _archetype("learned", "request_latency_seconds", tags=["learned"])
    candidates = [(generic, 0.87), (learned, 0.7)]
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        lambda *_args, **_kwargs: list(candidates),
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        lambda *_args, **_kwargs: [],
    )

    boosted = select_archetypes(
        intent=_intent(),
        metric_catalog=[_metric("request_latency_seconds")],
        catalog_for_compile=[_metric("request_latency_seconds")],
        target_language="promql",
        settings=Settings(
            max_blended_archetypes=2,
            min_secondary_coverage=0.0,
            learned_archetype_min_coverage=0.5,
            learned_archetype_boost=0.2,
        ),
        signal_store=SIGNAL_STORE_UNAVAILABLE,
    )
    unboosted = select_archetypes(
        intent=_intent(),
        metric_catalog=[_metric("request_latency_seconds")],
        catalog_for_compile=[_metric("request_latency_seconds")],
        target_language="promql",
        settings=Settings(
            max_blended_archetypes=2,
            min_secondary_coverage=0.0,
            learned_archetype_min_coverage=1.1,
            learned_archetype_boost=0.9,
        ),
        signal_store=SIGNAL_STORE_UNAVAILABLE,
    )

    assert [archetype.id for archetype, _ in boosted.ranked_archetypes] == ["learned", "generic"]
    assert [archetype.id for archetype, _ in unboosted.ranked_archetypes] == ["generic", "learned"]

    compilation = compile_selected_archetypes(
        selection=boosted,
        intent=_intent(),
        catalog_for_compile=[_metric("request_latency_seconds")],
        timings={},
        signal_store=SIGNAL_STORE_UNAVAILABLE,
    )

    assert compilation is not None
    assert compilation.dashboard_spec.panels[0].source_archetype == "learned"


def test_resolution_failure_preserves_obligations_and_abstains(monkeypatch: Any) -> None:
    archetype = _archetype("latency", "request_latency_seconds")
    dashboard = _dashboard(archetype, "request_latency_seconds")

    def fail_after_declaration(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("resolver unavailable")

    monkeypatch.setattr(
        "tacit.pipeline.stages.evidence.resolve_declared_requirements_for_archetypes",
        fail_after_declaration,
    )

    result = run_evidence_stage(
        ranked_archetypes=[(archetype, 0.9)],
        dashboard_spec=dashboard,
        intent=_intent(),
        catalog=[_metric("request_latency_seconds")],
        target_language="promql",
        signal_store=SIGNAL_STORE_UNAVAILABLE,
    )

    assert result.requirements
    assert len(result.resolutions) == len(result.requirements)
    assert {resolution.status for resolution in result.resolutions} == {EvidenceResolutionStatus.UNRESOLVED}
    assert {resolution.reason_code for resolution in result.resolutions} == {"evidence_resolution_failed"}

    observations = observe_evidence(result.requirements, result.resolutions, dashboard, dashboard)
    assert len(observations) == len(result.requirements)
    assert {observation.outcome for observation in observations} == {MISSING_EVIDENCE}
    assert {observation.rejection_reason for observation in observations} == {"evidence_resolution_failed"}

    ranking = rank_culprits(
        intent=_intent(),
        dashboard_spec=dashboard,
        ranked_archetypes=[(archetype, 0.9)],
        evidence_requirements=result.requirements,
        evidence_resolutions=result.resolutions,
        evidence_observations=observations,
    )
    contract = InvestigationContractAssembler().from_pipeline(
        investigation_id="inv-s4-evidence-failure",
        revision=1,
        parent_revision=None,
        request=DashRequest(prompt="Why is checkout slow?", tenant_id="default"),
        intent=_intent(),
        dashboard_spec=dashboard,
        evidence_requirements=result.requirements,
        evidence_resolutions=result.resolutions,
        evidence_observations=observations,
        culprit_ranking=ranking,
        dashboard_url="",
        dashboard_uid="dashboard-s4",
    )

    observation_ids = {observation.id for observation in contract.observations}
    assert ranking.abstained is True
    assert contract.grounding.abstained is True
    assert contract.grounding.status == GroundingStatus.INSUFFICIENT_EVIDENCE
    assert set(contract.grounding.missing_observation_refs) == observation_ids


def test_evidence_resolution_degraded_log_is_bounded_and_redacted(monkeypatch: Any) -> None:
    archetype = _archetype("latency", "request_latency_seconds")
    dashboard = _dashboard(archetype, "request_latency_seconds")
    canaries = [
        "tenant-secret",
        "query_secret_total{service='checkout'}",
        "/private/catalog.json",
        "intent-secret",
        "dashboard-secret",
        "payload-secret",
    ]

    def fail_after_declaration(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError(" ".join(canaries))

    monkeypatch.setattr(
        "tacit.pipeline.stages.evidence.resolve_declared_requirements_for_archetypes",
        fail_after_declaration,
    )

    with capture_logs() as logs:
        run_evidence_stage(
            ranked_archetypes=[(archetype, 0.9)],
            dashboard_spec=dashboard,
            intent=_intent(),
            catalog=[_metric("request_latency_seconds")],
            target_language="promql",
            signal_store=SIGNAL_STORE_UNAVAILABLE,
            tenant_id="tenant-secret",
        )

    serialized = json.dumps(logs, default=str)
    for canary in canaries:
        assert canary not in serialized
    [failure] = [entry for entry in logs if entry.get("event") == "evidence_resolution_failed"]
    assert failure["reason_code"] == "evidence_resolution_failed"
    assert failure["error_type"] == "RuntimeError"
    assert failure["requirement_count"] == 1
    assert len(failure["failure_fingerprint"]) == 12
    assert "exc_info" not in failure


def test_successful_evidence_resolution_preserves_governed_attribution(tmp_path: Any) -> None:
    archetype = _archetype(
        "latency",
        "canonical_request_latency_seconds",
        signal_type="request_latency",
    )
    live_metric = "checkout_custom_latency_seconds"
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.add_mapping(
        "request_latency",
        live_metric,
        confidence=0.95,
        context_services=["checkout"],
        context_datasource_types=["prometheus"],
        context_archetypes=[archetype.id],
        source_type="operational_knowledge",
        governance_ref="knowledge-latency-mapping",
        governance_revision=4,
        review_state="approved",
    )

    result = run_evidence_stage(
        ranked_archetypes=[(archetype, 0.9)],
        dashboard_spec=_dashboard(archetype, live_metric),
        intent=_intent(),
        catalog=[_metric(live_metric)],
        target_language="promql",
        signal_store=store,
    )

    requirement = result.requirements[0]
    assert result.resolutions[0].status == EvidenceResolutionStatus.RESOLVED
    assert result.resolutions[0].metric == live_metric
    assert result.applied_knowledge_refs == frozenset({"knowledge-latency-mapping"})
    assert result.knowledge_refs_by_requirement == {requirement.id: frozenset({"knowledge-latency-mapping"})}
    assert result.applied_knowledge_revision_refs == frozenset({KnowledgeRevisionRef("knowledge-latency-mapping", 4)})
    assert result.knowledge_revision_refs_by_requirement == {
        requirement.id: frozenset({KnowledgeRevisionRef("knowledge-latency-mapping", 4)})
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("signal_type", "rescue_stage", "reason_code"),
    [
        ("request_latency", "symptom_evidence_rescue", "symptom_evidence_resolution_failed"),
        ("cpu_usage", "evidence_gap_resolution", "evidence_gap_resolution_failed"),
    ],
)
async def test_validation_preserves_frozen_evidence_when_optional_rescue_resolution_fails(
    signal_type: str,
    rescue_stage: str,
    reason_code: str,
) -> None:
    archetype = _archetype(
        f"{signal_type}-investigation",
        f"canonical_{signal_type}",
        signal_type=signal_type,
    )
    dashboard = _dashboard(archetype, f"canonical_{signal_type}")

    class ExplodingSignalStore:
        def resolve_signal_details(self, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("resolver unavailable")

    class PassThroughBackend:
        async def validate_queries(
            self,
            spec: DashboardSpec,
            _catalog: list[MetricEntry],
        ) -> tuple[DashboardSpec, list[str]]:
            return spec, []

    signal_store = ExplodingSignalStore()
    evidence_stage = run_evidence_stage(
        ranked_archetypes=[(archetype, 0.9)],
        dashboard_spec=dashboard,
        intent=_intent(),
        catalog=[_metric("unrelated_metric")],
        target_language="promql",
        signal_store=signal_store,
    )
    frozen_resolutions = [resolution.model_dump(mode="json") for resolution in evidence_stage.resolutions]
    recorded_stages: list[tuple[str, str, str, dict[str, Any]]] = []

    result = await validate_dashboard_and_evidence(
        primary=cast(DashboardBackend, PassThroughBackend()),
        dashboard_spec=dashboard,
        catalog=[_metric("unrelated_metric")],
        evidence_requirements=evidence_stage.requirements,
        evidence_resolutions=evidence_stage.resolutions,
        intent=_intent(),
        target_language="promql",
        ranked_archetypes_present=True,
        record_stage=lambda name, status, reason_code, **details: recorded_stages.append(
            (name, status, reason_code, details)
        ),
        signal_store=signal_store,
    )

    assert [resolution.model_dump(mode="json") for resolution in evidence_stage.resolutions] == frozen_resolutions
    assert result.dashboard_spec == dashboard
    assert result.applied_knowledge_refs == frozenset()
    assert result.evidence_observations
    assert {observation.outcome for observation in result.evidence_observations} == {MISSING_EVIDENCE}
    assert {observation.rejection_reason for observation in result.evidence_observations} == {
        "evidence_resolution_failed"
    }
    assert any(
        name == rescue_stage and status == "skipped" and reason == reason_code
        for name, status, reason, _details in recorded_stages
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error_factory", "error_type"),
    [
        (lambda: SemanticAuthorizationError("denied"), SemanticAuthorizationError),
        (lambda: RuntimeOwnershipError("owner mismatch"), RuntimeOwnershipError),
        (lambda: TenantBoundaryError("tenant denied", status_code=403), TenantBoundaryError),
    ],
)
async def test_evidence_resolution_never_degrades_authority_failures(
    error_factory: Any,
    error_type: type[Exception],
) -> None:
    archetype = _archetype(
        "latency-investigation",
        "canonical_request_latency",
        signal_type="request_latency",
    )
    dashboard = _dashboard(archetype, "canonical_request_latency")

    class AuthorityFailingStore:
        def resolve_signal_details(self, *_args: Any, **_kwargs: Any) -> Any:
            raise error_factory()

    with pytest.raises(error_type):
        run_evidence_stage(
            ranked_archetypes=[(archetype, 0.9)],
            dashboard_spec=dashboard,
            intent=_intent(),
            catalog=[_metric("unrelated_metric")],
            target_language="promql",
            signal_store=AuthorityFailingStore(),
        )

    unresolved_stage = run_evidence_stage(
        ranked_archetypes=[(archetype, 0.9)],
        dashboard_spec=dashboard,
        intent=_intent(),
        catalog=[_metric("unrelated_metric")],
        target_language="promql",
        signal_store=SIGNAL_STORE_UNAVAILABLE,
    )

    class PassThroughBackend:
        async def validate_queries(
            self,
            spec: DashboardSpec,
            _catalog: list[MetricEntry],
        ) -> tuple[DashboardSpec, list[str]]:
            return spec, []

    with pytest.raises(error_type):
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, PassThroughBackend()),
            dashboard_spec=dashboard,
            catalog=[_metric("unrelated_metric")],
            evidence_requirements=unresolved_stage.requirements,
            evidence_resolutions=unresolved_stage.resolutions,
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
            signal_store=AuthorityFailingStore(),
        )


@pytest.mark.parametrize("signal_type", ["request_latency", "cpu_usage"])
def test_real_pinned_signal_store_fails_closed_during_initial_evidence_resolution(
    tmp_path: Any,
    signal_type: str,
) -> None:
    metric = f"checkout_{signal_type}_metric"
    archetype = _archetype(
        f"{signal_type}-investigation",
        f"canonical_{signal_type}",
        signal_type=signal_type,
    )
    store = SignalStore(
        db_path=tmp_path / f"initial-{signal_type}.db",
        runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
    )
    token = store.activate_pinned_governed_mappings(
        tenant_id="tenant-a",
        mappings=[_pinned_governed_mapping(signal_type, metric)],
    )
    try:
        with pytest.raises(TenantBoundaryError) as exc_info:
            run_evidence_stage(
                ranked_archetypes=[(archetype, 0.9)],
                dashboard_spec=_dashboard(archetype, metric),
                intent=_intent(),
                catalog=[_metric(metric)],
                target_language="promql",
                signal_store=store,
                tenant_id="tenant-b",
            )
        assert exc_info.value.status_code == 403
    finally:
        store.reset_pinned_governed_mappings(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_type", ["request_latency", "cpu_usage"])
async def test_real_pinned_signal_store_fails_closed_during_evidence_rescue(
    tmp_path: Any,
    signal_type: str,
) -> None:
    metric = f"checkout_{signal_type}_metric"
    archetype = _archetype(
        f"{signal_type}-investigation",
        f"canonical_{signal_type}",
        signal_type=signal_type,
    )
    dashboard = _dashboard(archetype, "existing_dashboard_metric")
    requirements, resolutions = _unresolved_evidence(archetype)
    store = SignalStore(
        db_path=tmp_path / f"rescue-{signal_type}.db",
        runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
    )
    token = store.activate_pinned_governed_mappings(
        tenant_id="tenant-a",
        mappings=[_pinned_governed_mapping(signal_type, metric)],
    )

    class PassThroughBackend:
        async def validate_queries(
            self,
            spec: DashboardSpec,
            _catalog: list[MetricEntry],
        ) -> tuple[DashboardSpec, list[str]]:
            return spec, []

    try:
        with pytest.raises(TenantBoundaryError) as exc_info:
            await validate_dashboard_and_evidence(
                primary=cast(DashboardBackend, PassThroughBackend()),
                dashboard_spec=dashboard,
                catalog=[_metric(metric)],
                evidence_requirements=requirements,
                evidence_resolutions=resolutions,
                intent=_intent(),
                target_language="promql",
                ranked_archetypes_present=True,
                record_stage=lambda *_args, **_kwargs: None,
                signal_store=store,
                tenant_id="tenant-b",
            )
        assert exc_info.value.status_code == 403
    finally:
        store.reset_pinned_governed_mappings(token)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("signal_type", "rescue_stage", "reason_code"),
    [
        ("request_latency", "symptom_evidence_rescue", "symptom_evidence_validation_failed"),
        ("cpu_usage", "evidence_gap_resolution", "evidence_gap_validation_failed"),
    ],
)
async def test_second_rescue_validation_failure_preserves_validated_state(
    tmp_path: Any,
    signal_type: str,
    rescue_stage: str,
    reason_code: str,
) -> None:
    metric = f"checkout_{signal_type}_metric"
    archetype = _archetype(
        f"{signal_type}-investigation",
        f"canonical_{signal_type}",
        signal_type=signal_type,
    )
    dashboard = _dashboard(archetype, "existing_dashboard_metric")
    requirements, resolutions = _unresolved_evidence(archetype)
    store = SignalStore(db_path=tmp_path / f"validation-{signal_type}.db")
    store.add_mapping(
        signal_type,
        metric,
        confidence=0.95,
        context_services=["checkout"],
        context_datasource_types=["prometheus"],
        context_archetypes=[archetype.id],
        source_type="operational_knowledge",
        governance_ref=f"knowledge-{signal_type}",
        governance_revision=7,
        review_state="approved",
    )
    canaries = [
        "tenant-secret",
        "query_secret_total{service='checkout'}",
        "/private/catalog.json",
        "intent-secret",
        "dashboard-secret",
        "payload-secret",
    ]

    class FailSecondValidationBackend:
        calls = 0

        async def validate_queries(
            self,
            spec: DashboardSpec,
            _catalog: list[MetricEntry],
        ) -> tuple[DashboardSpec, list[str]]:
            self.calls += 1
            if self.calls == 1:
                return spec, ["initial-warning"]
            raise RuntimeError(" ".join(canaries))

    frozen_requirements = [requirement.model_dump(mode="json") for requirement in requirements]
    frozen_resolutions = [resolution.model_dump(mode="json") for resolution in resolutions]
    recorded_stages: list[tuple[str, str, str, dict[str, Any]]] = []
    with capture_logs() as logs:
        result = await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, FailSecondValidationBackend()),
            dashboard_spec=dashboard,
            catalog=[_metric(metric)],
            evidence_requirements=requirements,
            evidence_resolutions=resolutions,
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda name, status, reason_code, **details: recorded_stages.append(
                (name, status, reason_code, details)
            ),
            signal_store=store,
        )

    assert [requirement.model_dump(mode="json") for requirement in requirements] == frozen_requirements
    assert [resolution.model_dump(mode="json") for resolution in resolutions] == frozen_resolutions
    assert result.dashboard_spec == dashboard
    assert result.validation_warnings == ["initial-warning"]
    assert result.panels_before == len(dashboard.panels)
    assert result.applied_knowledge_refs == frozenset()
    assert result.applied_knowledge_revision_refs == frozenset()
    assert any(
        name == rescue_stage and status == "skipped" and reason == reason_code
        for name, status, reason, _details in recorded_stages
    )
    serialized = json.dumps({"logs": logs, "stages": recorded_stages}, default=str)
    for canary in canaries:
        assert canary not in serialized
    [failure] = [entry for entry in logs if entry.get("reason_code") == reason_code]
    assert failure["error_type"] == "RuntimeError"
    assert len(failure["failure_fingerprint"]) == 12
    assert "exc_info" not in failure


@pytest.mark.asyncio
@pytest.mark.parametrize("signal_type", ["request_latency", "cpu_usage"])
@pytest.mark.parametrize(
    ("error_factory", "error_type"),
    [
        (lambda: SemanticAuthorizationError("denied"), SemanticAuthorizationError),
        (lambda: RuntimeOwnershipError("owner mismatch"), RuntimeOwnershipError),
        (lambda: TenantBoundaryError("tenant denied", status_code=403), TenantBoundaryError),
        (lambda: asyncio.CancelledError(), asyncio.CancelledError),
    ],
)
async def test_second_rescue_validation_propagates_authority_and_cancellation(
    tmp_path: Any,
    signal_type: str,
    error_factory: Any,
    error_type: type[BaseException],
) -> None:
    metric = f"checkout_{signal_type}_metric"
    archetype = _archetype(
        f"{signal_type}-investigation",
        f"canonical_{signal_type}",
        signal_type=signal_type,
    )
    requirements, resolutions = _unresolved_evidence(archetype)
    store = SignalStore(db_path=tmp_path / f"validation-authority-{signal_type}.db")
    store.add_mapping(
        signal_type,
        metric,
        confidence=0.95,
        context_services=["checkout"],
        context_datasource_types=["prometheus"],
        context_archetypes=[archetype.id],
        review_state="approved",
    )

    class FailSecondValidationBackend:
        calls = 0

        async def validate_queries(
            self,
            spec: DashboardSpec,
            _catalog: list[MetricEntry],
        ) -> tuple[DashboardSpec, list[str]]:
            self.calls += 1
            if self.calls == 1:
                return spec, []
            raise error_factory()

    with pytest.raises(error_type):
        await validate_dashboard_and_evidence(
            primary=cast(DashboardBackend, FailSecondValidationBackend()),
            dashboard_spec=_dashboard(archetype, "existing_dashboard_metric"),
            catalog=[_metric(metric)],
            evidence_requirements=requirements,
            evidence_resolutions=resolutions,
            intent=_intent(),
            target_language="promql",
            ranked_archetypes_present=True,
            record_stage=lambda *_args, **_kwargs: None,
            signal_store=store,
        )
