from __future__ import annotations

import asyncio

import pytest
from structlog.testing import capture_logs

from tacit.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from tacit.archetypes.engine import (
    KnowledgeQueryUse,
    _query_changes_under_metric_substitution,
    _query_references_metric,
)
from tacit.backends.base import PublishResult
from tacit.config import Settings
from tacit.context.base import ContextProvider
from tacit.context.enrichment import enrich_context
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies
from tacit.errors import (
    PipelineAdmissionRejected,
    PipelineExecutionError,
    RuntimeOwnershipError,
    SemanticAuthorizationError,
)
from tacit.history import InvestigationStore
from tacit.models.schemas import (
    ArchetypeMatch,
    ContextChunk,
    DashboardSpec,
    DashRequest,
    DashResponse,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)
from tacit.pipeline.completion import _rounded_timings
from tacit.pipeline.failures import PipelineFailureFactory
from tacit.pipeline.runner import (
    _initial_knowledge_archetype_ids,
    _initialize_signal_store,
    run_pipeline,
)
from tacit.pipeline.side_effects import safe_close_backends, safe_finish_timeout_history, safe_record_provenance
from tacit.pipeline.stages.archetypes import ArchetypeCompilation
from tacit.pipeline.stages.freeform import build_freeform_dashboard, discovery_cache_parts
from tacit.pipeline.stages.intent import IntentStageResult, run_intent_stage
from tacit.pipeline.stages.publish import PublicationState, publish_dashboard
from tacit.runtime_ownership import (
    declare_runtime_factory,
    runtime_descriptor_for_backends,
    runtime_descriptor_for_provider,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStores
from tacit.signals.availability import SIGNAL_STORE_UNAVAILABLE, resolve_signal_store
from tacit.tenancy import TenantBoundaryError


def _owned_test_factory(factory, *, runtime_settings: Settings, factory_kind: str):
    if getattr(factory, "factory_kind", None) == factory_kind:
        return factory
    category, capability = factory_kind.split(":", 1)
    if category in {"store", "knowledge"}:
        settings_owner = runtime_descriptor_from_settings(
            runtime_settings,
            component="pipeline_helper_test_settings",
        )
        database_path = next(item.path for item in settings_owner.databases if item.role == capability)
        ownership = runtime_descriptor_for_store(
            component=f"pipeline_helper_{factory_kind}_factory",
            runtime_settings=runtime_settings,
            database_role=capability,
            database_path=database_path,
        )
    elif category == "provider":
        ownership = runtime_descriptor_for_provider(
            component=f"pipeline_helper_{factory_kind}_factory",
            runtime_settings=runtime_settings,
            capability=capability,
        )
    else:
        ownership = runtime_descriptor_for_backends(
            component=f"pipeline_helper_{factory_kind}_factory",
            runtime_settings=runtime_settings,
        )
    return declare_runtime_factory(factory, ownership=ownership, factory_kind=factory_kind)


def _isolated_dependencies(**values) -> PipelineDependencies:
    runtime_settings = values["settings"]
    for field, kind in (
        ("backend_factory", "backend:dashboard"),
        ("history_store_factory", "store:history"),
        ("feedback_store_factory", "store:feedback"),
        ("signal_store_factory", "store:signals"),
        ("knowledge_service_factory", "knowledge:signals"),
        ("llm_provider_factory", "provider:llm"),
        ("context_provider_factory", "provider:context"),
    ):
        factory = values.get(field)
        if factory is not None:
            values[field] = _owned_test_factory(
                factory,
                runtime_settings=runtime_settings,
                factory_kind=kind,
            )
    return PipelineDependencies.isolated(**values)


class FakeRecorder:
    def __init__(self):
        self.investigation_id = "inv-1"
        self.run_id = "run-1"
        self.finished: list[dict] = []
        self.stages: list[tuple[str, str, str, dict]] = []

    def finish(self, **kwargs):
        self.finished.append(kwargs)

    def stage(self, stage, status, reason_code, **details):
        self.stages.append((stage, status, reason_code, details))


class FakeHistoryStore:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.started: list[tuple[str, str, str, str | None]] = []
        self.finished: list[tuple[str, dict]] = []

    def start(self, prompt, user_id, channel_id, tenant_id=None):
        if self.fail:
            raise RuntimeError("history unavailable")
        self.started.append((prompt, user_id, channel_id, tenant_id))
        return "inv-1"

    def finish(self, inv_id, **kwargs):
        self.finished.append((inv_id, kwargs))


class FakeFeedbackStore:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.provenance: list[dict] = []

    def record_provenance(self, **kwargs):
        if self.fail:
            raise RuntimeError("feedback unavailable")
        self.provenance.append(kwargs)


class FakeBackend:
    name = "fake"
    query_language = "promql"

    def __init__(self, fail_close: bool = False):
        self.fail_close = fail_close
        self.closed = False

    async def close(self):
        self.closed = True
        if self.fail_close:
            raise RuntimeError("close failed")


class BlockingCloseBackend(FakeBackend):
    def __init__(self, release: asyncio.Event):
        super().__init__()
        self.release = release
        self.close_started = asyncio.Event()

    async def close(self):
        self.close_started.set()
        await self.release.wait()
        self.closed = True


class EmptyDiscoveryBackend(FakeBackend):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__()
        self.runtime_ownership = runtime_descriptor_for_backends(
            component="empty_discovery_backend",
            runtime_settings=runtime_settings,
        )
        self.discovery_calls = 0

    async def discover_metrics(self, _keywords, _intent):
        self.discovery_calls += 1
        return []

    async def discover_datasource_targets(self, _keywords, _intent):
        return []


class _OwnedPublishingBackend:
    query_language = "promql"

    def __init__(
        self,
        name: str,
        runtime_settings: Settings,
        *,
        error: Exception | None = None,
    ) -> None:
        self.name = name
        self.runtime_ownership = runtime_descriptor_from_settings(
            runtime_settings,
            component=f"{name}_test_backend",
        )
        self.error = error
        self.publish_calls = 0

    async def publish(self, _dashboard_spec: DashboardSpec) -> PublishResult:
        self.publish_calls += 1
        if self.error is not None:
            raise self.error
        return PublishResult(
            url=f"https://dashboards.example/{self.name}",
            uid=f"{self.name}-uid",
            backend_name=self.name,
        )


class FakeProvider(LLMProvider):
    def __init__(self, runtime_settings: Settings | None = None):
        super().__init__(
            runtime_settings,
            component="fake_llm_provider",
        )
        self.closed = False

    async def chat_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> LLMResult:
        return LLMResult("")

    async def close(self) -> None:
        self.closed = True


class FakeContextProvider(ContextProvider):
    def __init__(self, runtime_settings: Settings | None = None):
        super().__init__(
            runtime_settings,
            component="fake_context_provider",
        )
        self.closed = False

    @property
    def name(self) -> str:
        return "fake"

    async def query(self, intent: Intent, max_chunks: int = 10) -> list[ContextChunk]:
        return []

    async def close(self) -> None:
        self.closed = True


class FailingCloseProvider(FakeProvider):
    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("close failed")


class FailingCloseContextProvider(FakeContextProvider):
    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("context close failed")


def _intent() -> Intent:
    return Intent(
        summary="checkout latency",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency"],
        problem_type="general",
        archetypes=[ArchetypeMatch(type="general", confidence=0.7)],
    )


def _own_store(store, runtime_settings: Settings, role: str):
    settings_owner = runtime_descriptor_from_settings(
        runtime_settings,
        component=f"{role}_test_settings",
    )
    database = next(item for item in settings_owner.databases if item.role == role)
    store.runtime_ownership = runtime_descriptor_for_store(
        component=f"{role}_test_store",
        runtime_settings=runtime_settings,
        database_role=role,
        database_path=database.path,
    )
    return store


def _request() -> DashRequest:
    return DashRequest(prompt="checkout latency", user_id="u1", channel_id="c1")


def test_initial_knowledge_scope_includes_concrete_curated_archetype_ids():
    intent = _intent().model_copy(
        update={
            "problem_type": "api_latency_spike",
            "archetypes": [ArchetypeMatch(type="api_latency_spike", confidence=0.9)],
        }
    )

    archetype_ids = _initial_knowledge_archetype_ids(intent)

    assert "api_latency_spike" in archetype_ids
    assert "api_response_time_spike" in archetype_ids
    assert "error_spike" not in archetype_ids
    assert len(archetype_ids) == 2


def _dashboard() -> DashboardSpec:
    return DashboardSpec(
        title="Test",
        panels=[
            PanelSpec(
                title="Latency",
                queries=[PanelQuery(expr="up", datasource_uid="prom")],
            )
        ],
    )


async def test_publish_preflights_every_backend_owner_before_remote_writes():
    runtime_settings = Settings(_env_file=None, grafana_url="https://grafana.runtime.example")
    mismatched_settings = runtime_settings.model_copy(update={"grafana_url": "https://grafana.other.example"})
    first = _OwnedPublishingBackend("grafana", runtime_settings)
    mismatched = _OwnedPublishingBackend("signalfx", mismatched_settings)

    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        await publish_dashboard(
            backends=[first, mismatched],
            dashboard_spec=_dashboard(),
            timings={},
            runtime_settings=runtime_settings,
        )

    assert first.publish_calls == 0
    assert mismatched.publish_calls == 0


@pytest.mark.asyncio
async def test_pipeline_preflights_backend_owner_before_discovery(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        grafana_url="https://grafana.runtime.example",
    )
    mismatched_settings = runtime_settings.model_copy(update={"grafana_url": "https://grafana.other.example"})

    class MismatchedDiscoveryBackend(EmptyDiscoveryBackend):
        name = "grafana"

        def __init__(self) -> None:
            super().__init__(mismatched_settings)

    backend = MismatchedDiscoveryBackend()
    history = _own_store(FakeHistoryStore(), runtime_settings, "history")
    deps = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [backend],
        history_store_factory=lambda: history,
        feedback_store_factory=FakeFeedbackStore,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(PipelineExecutionError, match="Dashboard backend initialization failed") as exc_info:
        await run_pipeline(DashRequest(prompt="checkout latency"), deps)

    assert isinstance(exc_info.value.__cause__, RuntimeOwnershipError)
    assert backend.discovery_calls == 0
    assert backend.closed is True


async def test_publish_propagates_authority_failures_without_logging_exception_text():
    runtime_settings = Settings(_env_file=None)
    sensitive_detail = "owner mismatch api_key=publish-secret path=/private/publish.json"
    backend = _OwnedPublishingBackend(
        "grafana",
        runtime_settings,
        error=RuntimeOwnershipError(sensitive_detail),
    )

    with capture_logs() as logs:
        with pytest.raises(RuntimeOwnershipError, match="owner mismatch"):
            await publish_dashboard(
                backends=[backend],
                dashboard_spec=_dashboard(),
                timings={},
                runtime_settings=runtime_settings,
            )

    assert sensitive_detail not in str(logs)
    assert "exc_info" not in str(logs)


async def test_committed_publish_finishes_after_caller_cancellation():
    runtime_settings = Settings(_env_file=None)
    first = _OwnedPublishingBackend("grafana", runtime_settings)
    second_started = asyncio.Event()
    release_second = asyncio.Event()

    class WaitingBackend(_OwnedPublishingBackend):
        async def publish(self, dashboard_spec: DashboardSpec) -> PublishResult:
            self.publish_calls += 1
            second_started.set()
            await release_second.wait()
            return PublishResult(
                url="https://dashboards.example/signalfx",
                uid="signalfx-uid",
                backend_name=self.name,
            )

    second = WaitingBackend("signalfx", runtime_settings)
    state = PublicationState()
    task = asyncio.create_task(
        publish_dashboard(
            backends=[first, second],
            dashboard_spec=_dashboard(),
            timings={},
            runtime_settings=runtime_settings,
            preserve_commit_on_cancellation=True,
            state=state,
        )
    )
    await second_started.wait()

    task.cancel()
    await asyncio.sleep(0)
    release_second.set()
    results = await task

    assert set(results) == {"grafana", "signalfx"}
    assert state.cancellation_requested is True
    assert first.publish_calls == 1
    assert second.publish_calls == 1


def test_compilation_usage_keeps_only_governed_queries_that_survive_validation():
    kept_query = PanelQuery(
        expr="kept_metric",
        datasource_uid="prom",
        datasource_type="prometheus",
        query_language="promql",
    )
    dropped_query = PanelQuery(
        expr="dropped_metric",
        datasource_uid="prom",
        datasource_type="prometheus",
        query_language="promql",
    )
    selected = DashboardSpec(
        title="Selected",
        panels=[
            PanelSpec(
                title="Kept",
                source_archetype="latency",
                queries=[kept_query],
            ),
            PanelSpec(
                title="Dropped",
                source_archetype="errors",
                queries=[dropped_query],
            ),
        ],
    )
    compilation = ArchetypeCompilation(
        dashboard_spec=selected,
        primary_archetype=object(),
        primary_confidence=0.9,
        knowledge_query_uses=(
            KnowledgeQueryUse.from_query("knowledge-kept", selected.panels[0], kept_query),
            KnowledgeQueryUse.from_query("knowledge-dropped", selected.panels[1], dropped_query),
        ),
    )
    validated = selected.model_copy(update={"panels": [selected.panels[0]]})

    assert compilation.applied_knowledge_refs == frozenset({"knowledge-kept", "knowledge-dropped"})
    assert compilation.surviving_knowledge_refs(validated) == frozenset({"knowledge-kept"})


def test_compilation_provenance_matches_complete_metric_tokens():
    assert _query_references_metric("sum(rate(checkout_latency_seconds[5m]))", "checkout_latency_seconds")
    assert not _query_references_metric(
        "sum(rate(checkout_latency_seconds_total[5m]))",
        "checkout_latency_seconds",
    )


def test_compilation_provenance_uses_suffix_aware_substitution_semantics():
    query = "histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))"

    assert _query_changes_under_metric_substitution(
        query,
        "http_request_duration_seconds",
        "acme_checkout_latency_seconds_bucket",
    )
    assert not _query_changes_under_metric_substitution(
        query,
        "unrelated_request_duration_seconds",
        "acme_checkout_latency_seconds_bucket",
    )


def test_pipeline_failure_factory_records_finish():
    recorder = FakeRecorder()

    response = PipelineFailureFactory.finish_failure(
        recorder=recorder,
        error="no data",
        summary="No data",
        timings={"intent": 0.1},
        started_at=0.0,
    )

    assert response.panel_count == 0
    assert response.summary == "No data"
    assert response.investigation_id == "inv-1"
    assert response.investigation_run_id == "run-1"
    assert response.investigation_status == "failed"
    assert recorder.finished[0]["status"] == "failed"
    assert recorder.finished[0]["error"] == "no data"


def test_signal_store_initialization_failure_is_recorded_and_nonfatal():
    recorder = FakeRecorder()
    timings: dict[str, float] = {}
    deps = _isolated_dependencies(
        settings=Settings(),
        backend_factory=lambda: [],
        history_store_factory=FakeHistoryStore,
        feedback_store_factory=FakeFeedbackStore,
        signal_store_factory=lambda: (_ for _ in ()).throw(OSError("signals database unavailable")),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with capture_logs() as logs:
        store = _initialize_signal_store(deps, recorder, timings)

    assert store is SIGNAL_STORE_UNAVAILABLE
    assert timings["signal_store_init"] >= 0
    assert recorder.stages == [
        (
            "signal_store",
            "skipped",
            "signal_store_unavailable",
            {
                "error_type": "OSError",
                "failure_fingerprint": recorder.stages[0][3]["failure_fingerprint"],
            },
        )
    ]
    assert len(recorder.stages[0][3]["failure_fingerprint"]) == 12
    assert "signals database unavailable" not in str(logs)
    assert "exc_info" not in str(logs)


@pytest.mark.parametrize(
    "error",
    [
        SemanticAuthorizationError("permission denied"),
        RuntimeOwnershipError("owner mismatch"),
        TenantBoundaryError("tenant denied", status_code=403),
    ],
    ids=["authorization", "runtime-owner", "tenant"],
)
def test_signal_store_initialization_propagates_authority_errors(error):
    recorder = FakeRecorder()
    deps = _isolated_dependencies(
        settings=Settings(),
        backend_factory=lambda: [],
        history_store_factory=FakeHistoryStore,
        feedback_store_factory=FakeFeedbackStore,
        signal_store_factory=lambda: (_ for _ in ()).throw(error),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(type(error), match=str(error)):
        _initialize_signal_store(deps, recorder, {})

    assert recorder.stages == []


def test_signal_store_initialization_preserves_cancellation():
    recorder = FakeRecorder()
    deps = _isolated_dependencies(
        settings=Settings(),
        backend_factory=lambda: [],
        history_store_factory=FakeHistoryStore,
        feedback_store_factory=FakeFeedbackStore,
        signal_store_factory=lambda: (_ for _ in ()).throw(asyncio.CancelledError()),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with pytest.raises(asyncio.CancelledError):
        _initialize_signal_store(deps, recorder, {})

    assert recorder.stages == []


def test_unavailable_signal_store_forbids_global_fallback():
    fallback_calls = 0

    def global_store():
        nonlocal fallback_calls
        fallback_calls += 1
        return object()

    assert resolve_signal_store(SIGNAL_STORE_UNAVAILABLE, global_store) is None
    assert fallback_calls == 0


def test_none_returned_by_signal_store_factory_becomes_explicitly_unavailable():
    recorder = FakeRecorder()
    deps = _isolated_dependencies(
        settings=Settings(),
        backend_factory=lambda: [],
        history_store_factory=FakeHistoryStore,
        feedback_store_factory=FakeFeedbackStore,
        signal_store_factory=lambda: None,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    store = _initialize_signal_store(deps, recorder, {})

    assert store is SIGNAL_STORE_UNAVAILABLE
    assert recorder.stages[0][2:] == ("signal_store_unavailable", {"error_type": "NoneReturned"})


def test_omitted_signal_store_preserves_legacy_global_fallback():
    fallback_store = object()

    assert resolve_signal_store(None, lambda: fallback_store) is fallback_store


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("runtime_settings", "tenant_id", "error_type"),
    [
        (
            Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
            "tenant-b",
            TenantBoundaryError,
        ),
        (
            Settings(
                _env_file=None,
                api_auth_enabled=True,
                knowledge_tenant_id="*",
                knowledge_tenant_api_keys={"tenant-a": "tenant-a-key"},
            ),
            "",
            TenantBoundaryError,
        ),
        (
            Settings(
                _env_file=None,
                knowledge_permissions="knowledge.read",
            ),
            "default",
            SemanticAuthorizationError,
        ),
        (
            Settings(
                _env_file=None,
                knowledge_permissions="knowledge.apply",
            ),
            "default",
            SemanticAuthorizationError,
        ),
        (
            Settings(
                _env_file=None,
                api_auth_enabled=True,
                knowledge_tenant_id="*",
                knowledge_tenant_api_keys={"tenant-a": "tenant-a-key"},
            ),
            "*bootstrap*",
            TenantBoundaryError,
        ),
    ],
)
async def test_direct_pipeline_denial_precedes_default_dependency_construction(
    monkeypatch,
    runtime_settings: Settings,
    tenant_id: str,
    error_type: type[Exception],
) -> None:
    import tacit.pipeline.runner as runner_module

    dependency_construction = 0

    def unexpected_dependencies() -> PipelineDependencies:
        nonlocal dependency_construction
        dependency_construction += 1
        raise AssertionError("dependencies constructed before pipeline authorization")

    monkeypatch.setattr(runner_module, "settings", runtime_settings)
    monkeypatch.setattr(runner_module, "_default_dependencies", unexpected_dependencies)

    with pytest.raises(error_type):
        await run_pipeline(
            DashRequest(prompt="checkout latency", tenant_id=tenant_id),
        )

    assert dependency_construction == 0


@pytest.mark.asyncio
async def test_knowledge_pin_runtime_owner_mismatch_fails_closed_without_sensitive_diagnostics(
    tmp_path,
    monkeypatch,
):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        knowledge_permissions="knowledge.read,knowledge.apply",
    )
    history = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=runtime_settings)
    backend = EmptyDiscoveryBackend(runtime_settings)
    sensitive_detail = "owner mismatch token=secret path=/private/runtime/signals.db"

    async def classify_without_external_work(**_kwargs):
        return IntentStageResult(intent=_intent(), context_chunks=[], token_usage=TokenUsage())

    def fail_knowledge_owner(*_args, **_kwargs):
        raise RuntimeOwnershipError(sensitive_detail)

    monkeypatch.setattr("tacit.pipeline.runner.run_intent_stage", classify_without_external_work)
    monkeypatch.setattr("tacit.pipeline.runner.resolve_knowledge_service", fail_knowledge_owner)
    deps = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [backend],
        history_store_factory=lambda: history,
        feedback_store_factory=FakeFeedbackStore,
        signal_store_factory=object,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with capture_logs() as logs:
        with pytest.raises(PipelineExecutionError, match="Dashboard pipeline failed") as exc_info:
            await run_pipeline(DashRequest(prompt="checkout latency"), deps)

    assert isinstance(exc_info.value.__cause__, RuntimeOwnershipError)
    assert sensitive_detail not in str(exc_info.value)
    assert sensitive_detail not in str(logs)
    assert "exc_info" not in str(logs)
    assert backend.discovery_calls == 0
    run = history.list_runs(history.list_recent()[0]["id"])[0]
    assert "pipeline_execution_failed" in run["error_detail"]
    assert "error_type=RuntimeOwnershipError" in run["error_detail"]
    assert sensitive_detail not in run["error_detail"]


@pytest.mark.asyncio
async def test_ordinary_knowledge_pin_failure_degrades_without_sensitive_diagnostics(
    tmp_path,
    monkeypatch,
):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "history.db"),
        knowledge_permissions="knowledge.read,knowledge.apply",
    )
    history = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=runtime_settings)
    backend = EmptyDiscoveryBackend(runtime_settings)
    sensitive_detail = "api_key=knowledge-secret path=/private/runtime/knowledge.db"

    async def classify_without_external_work(**_kwargs):
        return IntentStageResult(intent=_intent(), context_chunks=[], token_usage=TokenUsage())

    def fail_knowledge_store(*_args, **_kwargs):
        raise OSError(sensitive_detail)

    signal_database_path = next(
        database.path
        for database in runtime_descriptor_from_settings(
            runtime_settings,
            component="ordinary_failure_signal_settings",
        ).databases
        if database.role == "signals"
    )

    class OwnedSignalStore:
        pin_calls: list[tuple[str, tuple]] = []
        reset_calls: list[object] = []
        runtime_ownership = runtime_descriptor_for_store(
            component="ordinary_failure_signal_store",
            runtime_settings=runtime_settings,
            database_role="signals",
            database_path=signal_database_path,
        )

        def activate_pinned_governed_mappings(self, *, tenant_id, mappings):
            token = object()
            self.pin_calls.append((tenant_id, tuple(mappings)))
            return token

        def reset_pinned_governed_mappings(self, token):
            self.reset_calls.append(token)

    monkeypatch.setattr("tacit.pipeline.runner.run_intent_stage", classify_without_external_work)
    monkeypatch.setattr("tacit.pipeline.runner.resolve_knowledge_service", fail_knowledge_store)
    deps = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [backend],
        history_store_factory=lambda: history,
        feedback_store_factory=FakeFeedbackStore,
        signal_store_factory=OwnedSignalStore,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    with capture_logs() as logs:
        response = await run_pipeline(DashRequest(prompt="checkout latency"), deps)

    assert response.investigation_status == "failed"
    assert backend.discovery_calls == 1
    assert OwnedSignalStore.pin_calls == [("default", ())]
    assert len(OwnedSignalStore.reset_calls) == 1
    assert sensitive_detail not in str(logs)
    assert "exc_info" not in str(logs)
    events = history.list_events(response.investigation_id, response.investigation_run_id)
    knowledge_event = next(
        event
        for event in events
        if event["event_type"] == "stage_completed" and event["payload"]["stage"] == "knowledge_snapshot"
    )
    assert knowledge_event["payload"]["reason_code"] == "knowledge_snapshot_unavailable"
    assert knowledge_event["payload"]["details"]["error_type"] == "OSError"


async def test_build_freeform_dashboard_no_metrics_returns_failure():
    recorder = FakeRecorder()
    deps = _isolated_dependencies(
        settings=Settings(_env_file=None),
        backend_factory=lambda: [],
        history_store_factory=lambda: FakeHistoryStore(),
        feedback_store_factory=lambda: FakeFeedbackStore(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )

    result = await build_freeform_dashboard(
        intent=_intent(),
        metric_catalog=[],
        context_chunks=[],
        deps=deps,
        recorder=recorder,
        timings={},
        started_at=0.0,
    )

    assert result.dashboard_spec is None
    assert result.failure is not None
    assert result.token_usage == TokenUsage()
    assert recorder.finished[0]["error"] == "No metrics found for freeform generation"


def test_discovery_cache_identity_includes_tenant_and_complete_ranked_catalog():
    def metric(name: str) -> MetricEntry:
        return MetricEntry(
            name=name,
            datasource_uid="prom",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        )

    shared = [metric(f"metric_{index}") for index in range(20)]
    tenant_a = discovery_cache_parts(
        tenant_id="tenant-a",
        intent=_intent(),
        ranked_catalog=[*shared, metric("tenant_a_tail")],
        context_chunks=[],
    )
    tenant_b = discovery_cache_parts(
        tenant_id="tenant-b",
        intent=_intent(),
        ranked_catalog=[*shared, metric("tenant_a_tail")],
        context_chunks=[],
    )
    changed_tail = discovery_cache_parts(
        tenant_id="tenant-a",
        intent=_intent(),
        ranked_catalog=[*shared, metric("tenant_b_tail")],
        context_chunks=[],
    )

    assert tenant_a != tenant_b
    assert tenant_a != changed_tail


def test_discovery_cache_identity_includes_runtime_provider_and_model():
    common = {
        "tenant_id": "tenant-a",
        "intent": _intent(),
        "ranked_catalog": [],
        "context_chunks": [],
    }

    assert discovery_cache_parts(**common, runtime_identity="openai:model-a") != discovery_cache_parts(
        **common,
        runtime_identity="openai:model-b",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("domain", "database"),
        ("services", ["payments"]),
        ("timerange", "6h"),
    ],
)
def test_discovery_cache_identity_includes_every_rendered_intent_field(field, value):
    intent = _intent()
    changed = intent.model_copy(update={field: value})
    common = {
        "tenant_id": "tenant-a",
        "ranked_catalog": [],
        "context_chunks": [],
    }

    assert discovery_cache_parts(intent=intent, **common) != discovery_cache_parts(intent=changed, **common)


async def test_intent_stage_defers_provider_construction_for_legacy_hooks():
    calls = 0

    async def classify(prompt: str):
        return _intent(), TokenUsage()

    async def enrich(intent: Intent):
        return []

    def provider_factory():
        nonlocal calls
        calls += 1
        raise AssertionError("provider should not be constructed")

    result = await run_intent_stage(
        prompt="checkout latency",
        user_id="u1",
        deps=_isolated_dependencies(
            settings=Settings(),
            backend_factory=lambda: [],
            history_store_factory=lambda: FakeHistoryStore(),
            feedback_store_factory=lambda: FakeFeedbackStore(),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
        ),
        classify=classify,
        enrich=enrich,
        classify_provider_factory=provider_factory,
        context_provider_factory=provider_factory,
        timings={},
    )

    assert result.intent.summary == "checkout latency"
    assert calls == 0


async def test_intent_stage_zero_key_skips_provider_construction_for_key_based_provider():
    calls = 0
    runtime_settings = Settings(
        llm_provider="openai",
        llm_api_key="",
        llm_api_base="",
        intent_fallback_enabled=True,
    )

    async def classify(prompt: str, *, provider=None):
        assert provider is not None
        assert provider.is_configured is False
        expected_owner = runtime_descriptor_for_provider(
            component="expected_zero_key_provider",
            runtime_settings=runtime_settings,
        )
        assert provider.runtime_ownership.settings_identity == expected_owner.settings_identity
        assert provider.runtime_ownership.tenant_policy == expected_owner.tenant_policy
        return _intent(), TokenUsage()

    async def enrich(intent: Intent):
        return []

    def provider_factory():
        nonlocal calls
        calls += 1
        raise AssertionError("provider should not be constructed in zero-key mode")

    result = await run_intent_stage(
        prompt="checkout latency",
        user_id="u1",
        deps=_isolated_dependencies(
            settings=runtime_settings,
            backend_factory=lambda: [],
            history_store_factory=lambda: FakeHistoryStore(),
            feedback_store_factory=lambda: FakeFeedbackStore(),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
        ),
        classify=classify,
        enrich=enrich,
        classify_provider_factory=provider_factory,
        context_provider_factory=None,
        timings={},
    )

    assert result.intent.summary == "checkout latency"
    assert calls == 0


async def test_intent_stage_does_not_skip_provider_construction_for_ollama_without_key():
    calls = 0

    async def classify(prompt: str, *, provider=None):
        assert provider is not None
        return _intent(), TokenUsage()

    async def enrich(intent: Intent):
        return []

    def provider_factory():
        nonlocal calls
        calls += 1
        return FakeProvider()

    await run_intent_stage(
        prompt="checkout latency",
        user_id="u1",
        deps=_isolated_dependencies(
            settings=Settings(llm_provider="ollama", llm_api_key="", intent_fallback_enabled=True),
            backend_factory=lambda: [],
            history_store_factory=lambda: FakeHistoryStore(),
            feedback_store_factory=lambda: FakeFeedbackStore(),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
        ),
        classify=classify,
        enrich=enrich,
        classify_provider_factory=provider_factory,
        context_provider_factory=None,
        timings={},
    )

    assert calls == 1


async def test_pipeline_dependencies_cache_and_close_runtime_providers(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        context_provider="mcp",
        context_mcp_server_url="http://127.0.0.1:8765",
    )
    providers = [FakeProvider(runtime_settings), FakeProvider(runtime_settings)]
    context_providers = [FakeContextProvider(runtime_settings), FakeContextProvider(runtime_settings)]

    monkeypatch.setattr("tacit.agents.providers.registry.create_provider", lambda settings: providers.pop(0))
    monkeypatch.setattr(
        "tacit.context.registry.create_context_provider",
        lambda settings: context_providers.pop(0),
    )

    deps = build_pipeline_dependencies(runtime_settings, stores=RuntimeStores(runtime_settings))

    assert deps.llm_provider_factory is not None
    assert deps.context_provider_factory is not None
    await deps.acquire_resources()
    first_provider = deps.llm_provider_factory()
    first_context_provider = deps.context_provider_factory()
    assert deps.llm_provider_factory() is first_provider
    assert deps.context_provider_factory() is first_context_provider

    await deps.close_resources()

    assert first_provider.closed is True
    assert first_context_provider is not None
    assert first_context_provider.closed is True

    await deps.acquire_resources()
    second_provider = deps.llm_provider_factory()
    second_context_provider = deps.context_provider_factory()
    assert second_provider is not first_provider
    assert second_context_provider is not first_context_provider
    assert second_provider.closed is False
    assert second_context_provider is not None
    assert second_context_provider.closed is False
    await deps.close_resources()


async def test_pipeline_dependencies_cleanup_is_best_effort_and_resets_cache(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        context_provider="mcp",
        context_mcp_server_url="http://127.0.0.1:8765",
    )
    providers = [FailingCloseProvider(runtime_settings), FakeProvider(runtime_settings)]
    context_providers = [FailingCloseContextProvider(runtime_settings), FakeContextProvider(runtime_settings)]

    monkeypatch.setattr("tacit.agents.providers.registry.create_provider", lambda settings: providers.pop(0))
    monkeypatch.setattr(
        "tacit.context.registry.create_context_provider",
        lambda settings: context_providers.pop(0),
    )

    deps = build_pipeline_dependencies(runtime_settings, stores=RuntimeStores(runtime_settings))

    assert deps.llm_provider_factory is not None
    assert deps.context_provider_factory is not None
    await deps.acquire_resources()
    first_provider = deps.llm_provider_factory()
    first_context_provider = deps.context_provider_factory()

    await deps.close_resources()

    assert first_provider.closed is True
    assert first_context_provider is not None
    assert first_context_provider.closed is True
    await deps.acquire_resources()
    assert deps.llm_provider_factory() is not first_provider
    assert deps.context_provider_factory() is not first_context_provider
    await deps.close_resources()


@pytest.mark.asyncio
async def test_backend_cleanup_is_bounded_by_runtime_grace():
    release = asyncio.Event()
    backend = BlockingCloseBackend(release)

    await asyncio.wait_for(
        safe_close_backends([backend], grace_seconds=0.01),
        timeout=0.2,
    )

    assert backend.close_started.is_set()
    assert backend.closed is False
    release.set()
    await asyncio.sleep(0)


async def test_intent_stage_honors_explicit_disabled_context_provider(monkeypatch):
    async def classify(prompt: str):
        return _intent(), TokenUsage()

    def global_context_provider():
        raise AssertionError("global context provider should not be used")

    monkeypatch.setattr("tacit.context.enrichment.get_context_provider", global_context_provider)

    result = await run_intent_stage(
        prompt="checkout latency",
        user_id="u1",
        deps=_isolated_dependencies(
            settings=Settings(context_provider="none"),
            backend_factory=lambda: [],
            history_store_factory=lambda: FakeHistoryStore(),
            feedback_store_factory=lambda: FakeFeedbackStore(),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
        ),
        classify=classify,
        enrich=enrich_context,
        classify_provider_factory=None,
        context_provider_factory=lambda: None,
        timings={},
    )

    assert result.context_chunks == []


async def test_isolated_intent_stage_never_consults_process_global_providers(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="none",
    )
    owned_provider = FakeProvider(runtime_settings)
    dependencies = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=lambda: [],
        history_store_factory=lambda: FakeHistoryStore(),
        feedback_store_factory=lambda: FakeFeedbackStore(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=lambda: owned_provider,
        context_provider_factory=lambda: None,
    )

    def forbidden_global_provider():
        raise AssertionError("isolated stage consulted a process-global provider")

    monkeypatch.setattr("tacit.agents.llm.get_provider", forbidden_global_provider)
    monkeypatch.setattr("tacit.context.enrichment.get_context_provider", forbidden_global_provider)

    async def classify(_prompt: str, *, provider=None, runtime_settings=None):
        assert provider is owned_provider
        assert runtime_settings == dependencies.settings
        return _intent(), TokenUsage()

    assert dependencies.llm_provider_factory is not None
    assert dependencies.context_provider_factory is not None
    await dependencies.acquire_resources()
    try:
        result = await run_intent_stage(
            prompt="checkout latency",
            user_id="u1",
            deps=dependencies,
            classify=classify,
            enrich=enrich_context,
            classify_provider_factory=dependencies.llm_provider_factory,
            context_provider_factory=dependencies.context_provider_factory,
            timings={},
        )
    finally:
        await dependencies.close_resources()

    assert result.intent.summary == "checkout latency"
    assert result.context_chunks == []


async def test_pipeline_admission_is_isolated_per_runtime(monkeypatch):
    active = {"runtime-a": 0, "runtime-b": 0}
    maximum = {"runtime-a": 0, "runtime-b": 0}
    admitted = {"runtime-a": asyncio.Event(), "runtime-b": asyncio.Event()}
    release = asyncio.Event()

    async def fake_inner(request, deps, **_kwargs):
        runtime = request.user_id
        active[runtime] += 1
        maximum[runtime] = max(maximum[runtime], active[runtime])
        expected = deps.settings.pipeline_max_concurrent
        if active[runtime] == expected:
            admitted[runtime].set()
        await release.wait()
        active[runtime] -= 1
        return DashResponse(
            dashboard_url="https://dashboards.example/test",
            dashboard_uid=f"{runtime}-dashboard",
            panel_count=1,
            summary="test",
        )

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", fake_inner)

    def dependencies(runtime: str, limit: int) -> PipelineDependencies:
        return _isolated_dependencies(
            settings=Settings(_env_file=None, pipeline_max_concurrent=limit),
            backend_factory=lambda: [],
            history_store_factory=lambda: FakeHistoryStore(),
            feedback_store_factory=lambda: FakeFeedbackStore(),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
        )

    runtime_a = dependencies("runtime-a", 1)
    runtime_b = dependencies("runtime-b", 2)
    requests = [
        run_pipeline(_request().model_copy(update={"user_id": runtime}), deps)
        for runtime, deps in (
            ("runtime-a", runtime_a),
            ("runtime-b", runtime_b),
            ("runtime-a", runtime_a),
            ("runtime-b", runtime_b),
            ("runtime-a", runtime_a),
            ("runtime-b", runtime_b),
        )
    ]
    tasks = [asyncio.create_task(request) for request in requests]
    await asyncio.wait_for(
        asyncio.gather(admitted["runtime-a"].wait(), admitted["runtime-b"].wait()),
        timeout=1,
    )

    assert active == {"runtime-a": 1, "runtime-b": 2}
    release.set()
    await asyncio.gather(*tasks)

    assert maximum == {"runtime-a": 1, "runtime-b": 2}


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", ["client_cancel", "timeout"])
async def test_pipeline_cancellation_grace_returns_request_but_retains_effective_work(monkeypatch, trigger):
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    started = asyncio.Event()
    timeout_seconds = 0.02 if trigger == "timeout" else 30.0
    dependencies = _isolated_dependencies(
        settings=Settings(
            _env_file=None,
            pipeline_max_concurrent=1,
            pipeline_timeout_seconds=timeout_seconds,
        ),
        backend_factory=lambda: [],
        history_store_factory=lambda: FakeHistoryStore(),
        feedback_store_factory=lambda: FakeFeedbackStore(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        cleanup_grace_seconds=0.01,
    )

    async def stubborn_inner(*_args, **_kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cleanup_started.set()
            await release_cleanup.wait()
            raise

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", stubborn_inner)
    task = asyncio.create_task(run_pipeline(_request(), dependencies))
    await started.wait()
    if trigger == "client_cancel":
        task.cancel()

    done, _pending = await asyncio.wait({task}, timeout=0.2)
    try:
        assert task in done
        assert cleanup_started.is_set()
        assert dependencies.pipeline_admission is not None
        assert dependencies.pipeline_admission.in_flight == 1
        assert dependencies.pipeline_admission.retained == 1
        if trigger == "client_cancel":
            with pytest.raises(asyncio.CancelledError):
                task.result()
        else:
            assert task.result().investigation_status == "failed"
    finally:
        release_cleanup.set()
        for _ in range(100):
            if dependencies.pipeline_admission.in_flight == 0:
                break
            await asyncio.sleep(0)
    assert dependencies.pipeline_admission.in_flight == 0
    assert dependencies.pipeline_admission.retained == 0


@pytest.mark.asyncio
async def test_repeated_timeouts_are_fenced_and_cannot_exceed_effective_work_budget(monkeypatch):
    release_cleanup = asyncio.Event()
    started = 0
    fenced = 0
    late_side_effects: list[str] = []
    dependencies = _isolated_dependencies(
        settings=Settings(
            _env_file=None,
            pipeline_max_concurrent=2,
            pipeline_max_queued=0,
            pipeline_timeout_seconds=0.01,
        ),
        backend_factory=lambda: [],
        history_store_factory=lambda: FakeHistoryStore(),
        feedback_store_factory=lambda: FakeFeedbackStore(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        cleanup_grace_seconds=0.01,
    )

    async def resistant_inner(*_args, lifecycle, **_kwargs):
        nonlocal fenced, started
        started += 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release_cleanup.wait()
            with pytest.raises(RuntimeError, match="fenced"):
                lifecycle.ensure_side_effects_allowed()
            fenced += 1
            lifecycle.ensure_side_effects_allowed()
            late_side_effects.append("published")

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", resistant_inner)

    first = await asyncio.wait_for(run_pipeline(_request(), dependencies), timeout=0.2)
    second = await asyncio.wait_for(run_pipeline(_request(), dependencies), timeout=0.2)

    assert first.investigation_status == "failed"
    assert second.investigation_status == "failed"
    assert started == 2
    assert dependencies.pipeline_admission is not None
    assert dependencies.pipeline_admission.in_flight == 2
    assert dependencies.pipeline_admission.retained == 2
    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await run_pipeline(_request(), dependencies)
    assert exc_info.value.reason_code == "pipeline_admission_queue_full"
    assert started == 2

    release_cleanup.set()
    for _ in range(100):
        if dependencies.pipeline_admission.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert fenced == 2
    assert late_side_effects == []
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    [
        "history_factory",
        "contract_lookup",
        "history_start",
        "run_start",
        "run_start_audit_failure",
        "backend_factory",
        "backend_factory_audit_failure",
    ],
)
async def test_early_pipeline_cancellation_releases_provider_lease_and_admission(phase):
    runtime_settings = Settings(
        _env_file=None,
        pipeline_max_concurrent=1,
        pipeline_timeout_seconds=30.0,
    )
    provider = FakeProvider(runtime_settings)
    dependencies: PipelineDependencies

    def activate_provider_then_cancel() -> None:
        assert dependencies.llm_provider_factory is not None
        assert dependencies.llm_provider_factory() is provider
        raise asyncio.CancelledError

    class CancellingHistory(FakeHistoryStore):
        def get_contract(self, *_args, **_kwargs):
            if phase == "contract_lookup":
                activate_provider_then_cancel()
            raise AssertionError("contract lookup should cancel")

        def start(self, prompt, user_id, channel_id, tenant_id=None):
            if phase == "history_start":
                activate_provider_then_cancel()
            return super().start(prompt, user_id, channel_id, tenant_id=tenant_id)

        def start_run(self, *_args, **_kwargs):
            if phase in {"run_start", "run_start_audit_failure"}:
                activate_provider_then_cancel()
            return "run-1"

        def finish(self, *args, **kwargs):
            if phase in {"run_start_audit_failure", "backend_factory_audit_failure"}:
                raise RuntimeError("audit unavailable during cancellation")
            return super().finish(*args, **kwargs)

    history = _own_store(CancellingHistory(), runtime_settings, "history")

    def history_factory():
        if phase == "history_factory":
            activate_provider_then_cancel()
        return history

    def backend_factory():
        if phase in {"backend_factory", "backend_factory_audit_failure"}:
            activate_provider_then_cancel()
        raise AssertionError("backend factory should cancel")

    dependencies = _isolated_dependencies(
        settings=runtime_settings,
        backend_factory=backend_factory,
        history_store_factory=history_factory,
        feedback_store_factory=lambda: FakeFeedbackStore(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=lambda: provider,
    )

    with pytest.raises(asyncio.CancelledError):
        await run_pipeline(
            _request(),
            dependencies,
            investigation_id="inv-existing" if phase == "contract_lookup" else None,
        )

    assert provider.closed is True
    assert dependencies.pipeline_admission is not None
    assert dependencies.pipeline_admission.in_flight == 0


async def test_pipeline_deadline_includes_admission_wait(monkeypatch):
    deps = _isolated_dependencies(
        settings=Settings(
            _env_file=None,
            pipeline_max_concurrent=1,
            pipeline_max_queued=1,
            pipeline_timeout_seconds=0.01,
        ),
        backend_factory=lambda: [],
        history_store_factory=lambda: FakeHistoryStore(),
        feedback_store_factory=lambda: FakeFeedbackStore(),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )
    assert deps.pipeline_admission is not None
    active_lease = await deps.pipeline_admission.acquire()

    async def unexpected_inner(*_args, **_kwargs):
        raise AssertionError("pipeline started after its admission deadline")

    monkeypatch.setattr("tacit.pipeline.runner._run_pipeline_inner", unexpected_inner)
    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await run_pipeline(_request(), deps)

    assert exc_info.value.reason_code == "pipeline_admission_wait_timeout"
    deps.pipeline_admission.release(active_lease)


def test_pipeline_timing_diagnostics_preserve_sub_ten_millisecond_stages():
    assert _rounded_timings({"knowledge_snapshot_repin": 0.00346}) == {"knowledge_snapshot_repin": 0.0035}


def test_safe_finish_timeout_history_records_when_available():
    store = FakeHistoryStore()
    request = _request().model_copy(update={"tenant_id": "tenant-a"})

    safe_finish_timeout_history(
        history_store_factory=lambda: store,
        request=request,
        timeout_seconds=9,
    )

    assert store.started == [("checkout latency", "u1", "c1", "tenant-a")]
    assert store.finished == [
        (
            "inv-1",
            {
                "status": "timeout",
                "error": "Timed out after 9s",
                "tenant_id": "tenant-a",
            },
        )
    ]


def test_safe_finish_timeout_history_swallows_noncritical_errors():
    safe_finish_timeout_history(
        history_store_factory=lambda: FakeHistoryStore(fail=True),
        request=_request(),
        timeout_seconds=9,
    )


async def test_pipeline_threads_resolved_tenant_through_run_and_recorder_history_writes():
    class TrackingHistory:
        def __init__(self):
            self.calls: list[tuple[str, str | None]] = []

        def start(self, _prompt, _user_id, _channel_id, *, tenant_id=None):
            self.calls.append(("start", tenant_id))
            return "inv-tenant-a"

        def start_run(self, _investigation_id, *, run_type, base_revision=None, tenant_id=None):
            assert run_type is not None
            assert base_revision is None
            self.calls.append(("start_run", tenant_id))
            return "run-tenant-a"

        def finish(self, _investigation_id, **kwargs):
            self.calls.append(("finish", kwargs.get("tenant_id")))

        def complete_run(self, _run_id, **kwargs):
            self.calls.append(("complete_run", kwargs.get("tenant_id")))

    history = TrackingHistory()
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="*",
        api_auth_enabled=True,
        grafana_enabled=False,
    )
    _own_store(history, runtime_settings, "history")
    response = await run_pipeline(
        DashRequest(prompt="checkout latency", tenant_id="tenant-a"),
        _isolated_dependencies(
            settings=runtime_settings,
            backend_factory=lambda: [],
            history_store_factory=lambda: history,
            feedback_store_factory=FakeFeedbackStore,
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
        ),
    )

    assert response.investigation_id == "inv-tenant-a"
    assert history.calls == [
        ("start", "tenant-a"),
        ("start_run", "tenant-a"),
        ("finish", "tenant-a"),
        ("complete_run", "tenant-a"),
    ]


def test_safe_record_provenance_records_when_available():
    store = FakeFeedbackStore()

    safe_record_provenance(
        feedback_store_factory=lambda: store,
        dashboard_uid="dash-1",
        dashboard_url="http://dash",
        request=_request(),
        intent=_intent(),
        dashboard_spec=_dashboard(),
        path_used="archetype",
    )

    assert store.provenance[0]["dashboard_uid"] == "dash-1"
    assert store.provenance[0]["metrics_used"] == ["up"]
    assert store.provenance[0]["tenant_id"] == "default"


def test_safe_record_provenance_swallows_noncritical_errors():
    safe_record_provenance(
        feedback_store_factory=lambda: FakeFeedbackStore(fail=True),
        dashboard_uid="dash-1",
        dashboard_url="http://dash",
        request=_request(),
        intent=_intent(),
        dashboard_spec=_dashboard(),
        path_used="archetype",
    )


async def test_safe_close_backends_closes_all_and_swallows_errors():
    good = FakeBackend()
    bad = FakeBackend(fail_close=True)

    await safe_close_backends([bad, good])

    assert bad.closed is True
    assert good.closed is True
