from __future__ import annotations

from tacit.agents.providers.base import LLMProvider, LLMResult, TokenUsage
from tacit.config import Settings
from tacit.context.base import ContextProvider
from tacit.context.enrichment import enrich_context
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies
from tacit.models.schemas import (
    ArchetypeMatch,
    ContextChunk,
    DashboardSpec,
    DashRequest,
    Intent,
    PanelQuery,
    PanelSpec,
    SignalType,
)
from tacit.pipeline.failures import PipelineFailureFactory
from tacit.pipeline.runner import _get_semaphore
from tacit.pipeline.side_effects import safe_close_backends, safe_finish_timeout_history, safe_record_provenance
from tacit.pipeline.stages.freeform import build_freeform_dashboard
from tacit.pipeline.stages.intent import run_intent_stage


class FakeRecorder:
    def __init__(self):
        self.finished: list[dict] = []

    def finish(self, **kwargs):
        self.finished.append(kwargs)


class FakeHistoryStore:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.finished: list[tuple[str, dict]] = []

    def start(self, prompt, user_id, channel_id):
        if self.fail:
            raise RuntimeError("history unavailable")
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


class FakeProvider(LLMProvider):
    def __init__(self):
        self.closed = False

    async def chat_json(self, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> LLMResult:
        return LLMResult("")

    async def close(self) -> None:
        self.closed = True


class FakeContextProvider(ContextProvider):
    def __init__(self):
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


def _request() -> DashRequest:
    return DashRequest(prompt="checkout latency", user_id="u1", channel_id="c1")


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
    assert recorder.finished[0]["status"] == "failed"
    assert recorder.finished[0]["error"] == "no data"


async def test_build_freeform_dashboard_no_metrics_returns_failure():
    recorder = FakeRecorder()
    deps = PipelineDependencies(
        settings=object(),
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
        deps=PipelineDependencies(
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


async def test_pipeline_dependencies_cache_and_close_runtime_providers(monkeypatch):
    providers = [FakeProvider(), FakeProvider()]
    context_providers = [FakeContextProvider(), FakeContextProvider()]

    monkeypatch.setattr("tacit.agents.providers.registry.create_provider", lambda settings: providers.pop(0))
    monkeypatch.setattr(
        "tacit.context.registry.create_context_provider",
        lambda settings: context_providers.pop(0),
    )

    deps = build_pipeline_dependencies(Settings())

    assert deps.llm_provider_factory is not None
    assert deps.context_provider_factory is not None
    first_provider = deps.llm_provider_factory()
    first_context_provider = deps.context_provider_factory()
    assert deps.llm_provider_factory() is first_provider
    assert deps.context_provider_factory() is first_context_provider

    await deps.close_resources()

    assert first_provider.closed is True
    assert first_context_provider is not None
    assert first_context_provider.closed is True

    second_provider = deps.llm_provider_factory()
    second_context_provider = deps.context_provider_factory()
    assert second_provider is not first_provider
    assert second_context_provider is not first_context_provider
    assert second_provider.closed is False
    assert second_context_provider is not None
    assert second_context_provider.closed is False


async def test_pipeline_dependencies_cleanup_is_best_effort_and_resets_cache(monkeypatch):
    providers = [FailingCloseProvider(), FakeProvider()]
    context_providers = [FailingCloseContextProvider(), FakeContextProvider()]

    monkeypatch.setattr("tacit.agents.providers.registry.create_provider", lambda settings: providers.pop(0))
    monkeypatch.setattr(
        "tacit.context.registry.create_context_provider",
        lambda settings: context_providers.pop(0),
    )

    deps = build_pipeline_dependencies(Settings())

    assert deps.llm_provider_factory is not None
    assert deps.context_provider_factory is not None
    first_provider = deps.llm_provider_factory()
    first_context_provider = deps.context_provider_factory()

    await deps.close_resources()

    assert first_provider.closed is True
    assert first_context_provider is not None
    assert first_context_provider.closed is True
    assert deps.llm_provider_factory() is not first_provider
    assert deps.context_provider_factory() is not first_context_provider


async def test_intent_stage_honors_explicit_disabled_context_provider(monkeypatch):
    async def classify(prompt: str):
        return _intent(), TokenUsage()

    def global_context_provider():
        raise AssertionError("global context provider should not be used")

    monkeypatch.setattr("tacit.context.enrichment.get_context_provider", global_context_provider)

    result = await run_intent_stage(
        prompt="checkout latency",
        user_id="u1",
        deps=PipelineDependencies(
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


def test_get_semaphore_recreates_when_limit_changes():
    first = _get_semaphore(1)
    second = _get_semaphore(2)
    third = _get_semaphore(2)

    assert first is not second
    assert second is third


def test_safe_finish_timeout_history_records_when_available():
    store = FakeHistoryStore()

    safe_finish_timeout_history(
        history_store_factory=lambda: store,
        request=_request(),
        timeout_seconds=9,
    )

    assert store.finished == [("inv-1", {"status": "timeout", "error": "Timed out after 9s"})]


def test_safe_finish_timeout_history_swallows_noncritical_errors():
    safe_finish_timeout_history(
        history_store_factory=lambda: FakeHistoryStore(fail=True),
        request=_request(),
        timeout_seconds=9,
    )


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
