from __future__ import annotations

import asyncio
import gc
import io
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import structlog
from structlog.testing import capture_logs

import tacit.dependencies as dependencies_module
from tacit.agents.providers.anthropic import AnthropicProvider
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.agents.providers.bedrock import BedrockProvider
from tacit.agents.providers.http_transport import LLMSDKHTTPClientCloseGuard
from tacit.agents.providers.openai_provider import OpenAIProvider
from tacit.backends.base import DashboardBackend
from tacit.config import Settings
from tacit.context.base import ContextProvider
from tacit.dependencies import (
    PipelineDependencies,
    ProviderLifecycleState,
    _cleanup_rejected_products,
    _RuntimeProviderResources,
    build_pipeline_dependencies,
    declare_backend_factory,
)
from tacit.errors import PipelineAdmissionRejected, PipelineExecutionError, RuntimeOwnershipError
from tacit.history import InvestigationStore
from tacit.logging import configure_logging
from tacit.models.schemas import DashRequest
from tacit.pipeline.runner import run_pipeline
from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork
from tacit.pipeline_admission import PipelineAdmissionController
from tacit.runtime_ownership import (
    BedrockCredentialIdentity,
    BedrockCredentialPlan,
    credential_fingerprint,
    declare_runtime_factory,
    runtime_descriptor_for_backends,
    runtime_descriptor_for_provider,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStoreReadinessError, RuntimeStores


@pytest.fixture(autouse=True)
def _bound_provider_owner_thread_lifetime() -> Iterator[None]:
    """Fail the creating test instead of hanging Python during thread shutdown."""
    existing_threads = {id(thread) for thread in threading.enumerate()}
    yield

    leaked_threads: list[str] = []
    for thread in threading.enumerate():
        if (
            id(thread) in existing_threads
            or thread is threading.current_thread()
            or thread.name != "tacit-lifecycle-provider-owner"
        ):
            continue
        thread.join(timeout=1.0)
        if thread.is_alive():
            leaked_threads.append(thread.name)
    assert leaked_threads == [], f"provider lifecycle owner did not exit: {leaked_threads}"


class _ProviderProbe(LLMProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="factory_test_llm_provider")
        self.closed = False

    async def chat_json(self, *_args, **_kwargs) -> LLMResult:
        return LLMResult("{}")

    async def chat_text(self, *_args, **_kwargs) -> LLMResult:
        return LLMResult("")

    async def close(self) -> None:
        self.closed = True


class _BackendProbe:
    name = "factory-ownership-backend"
    query_language = "promql"

    def __init__(self, runtime_settings: Settings) -> None:
        self.close_calls = 0
        self.runtime_ownership = runtime_descriptor_from_settings(
            runtime_settings,
            component="factory_ownership_backend",
        )

    async def close(self) -> None:
        self.close_calls += 1


class _ContextProbe(ContextProvider):
    def __init__(self, runtime_settings: Settings) -> None:
        super().__init__(runtime_settings, component="factory_test_context_provider")
        self.closed = False

    @property
    def name(self) -> str:
        return "context-probe"

    async def query(self, *_args, **_kwargs) -> list[Any]:
        return []

    async def close(self) -> None:
        self.closed = True


class _CrossLoopAsyncGate:
    """Release one async waiter from a deterministic cross-thread barrier."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self._lock = threading.Lock()
        self._released = False
        self._release_waiter: Callable[[], None] | None = None

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        event = asyncio.Event()
        with self._lock:
            if self._released:
                event.set()
            else:
                self._release_waiter = lambda: loop.call_soon_threadsafe(event.set)
        self.started.set()
        await event.wait()

    def release(self) -> None:
        with self._lock:
            self._released = True
            release_waiter = self._release_waiter
        if release_waiter is not None:
            release_waiter()


class _KeepAliveHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class _KeepAliveServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False


@contextmanager
def _keep_alive_http_server() -> Iterator[str]:
    server = _KeepAliveServer(("127.0.0.1", 0), _KeepAliveHandler)
    serving = threading.Event()

    def serve() -> None:
        serving.set()
        server.serve_forever(poll_interval=0.01)

    thread = threading.Thread(target=serve, name="provider-loop-affinity-http", daemon=True)
    thread.start()
    assert serving.wait(timeout=1.0)
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1.0)


def _settings(tmp_path, *, suffix: str = "active", **updates) -> Settings:
    values: dict[str, Any] = {
        "_env_file": None,
        "history_db_path": str(tmp_path / f"{suffix}-history.db"),
        "feedback_db_path": str(tmp_path / f"{suffix}-feedback.db"),
        "signals_db_path": str(tmp_path / f"{suffix}-signals.db"),
    }
    values.update(updates)
    return Settings(**values)


def _backend_factory(factory, runtime_settings: Settings):
    return declare_backend_factory(
        factory,
        runtime_settings=runtime_settings,
        component="factory_test_backend_factory",
    )


def _static_bedrock_identity(access_key: str, secret_key: str) -> BedrockCredentialIdentity:
    return BedrockCredentialIdentity(
        account=f"access-key:{credential_fingerprint(access_key)}",
        credential_fingerprint=credential_fingerprint("\0".join((access_key, secret_key, ""))),
        uses_sts=False,
    )


def _provider_resource_matrix(
    tmp_path,
    *,
    llm_factory,
    context_factory=None,
    max_concurrent: int = 2,
    cleanup_grace_seconds: float = 0.05,
) -> tuple[_RuntimeProviderResources, PipelineAdmissionController, Settings]:
    runtime_settings = _settings(
        tmp_path,
        suffix="provider-generation-matrix",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="mcp" if context_factory is not None else "none",
        context_mcp_server_url="http://127.0.0.1:8765",
        pipeline_max_concurrent=max_concurrent,
        pipeline_max_queued=max_concurrent,
    )
    lifecycle = PipelineAdmissionController(
        max_concurrent,
        max_queued=max_concurrent,
    )
    lifecycle.bind_runtime_identity(
        runtime_descriptor_from_settings(
            runtime_settings,
            component="provider_generation_matrix",
        ).admission_namespace
        or "provider-generation-matrix"
    )
    declared_llm = declare_runtime_factory(
        lambda: llm_factory(runtime_settings),
        ownership=runtime_descriptor_for_provider(
            component="provider_generation_llm_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    declared_context = None
    if context_factory is not None:
        declared_context = declare_runtime_factory(
            lambda: context_factory(runtime_settings),
            ownership=runtime_descriptor_for_provider(
                component="provider_generation_context_factory",
                runtime_settings=runtime_settings,
                capability="context",
            ),
            factory_kind="provider:context",
        )
    return (
        _RuntimeProviderResources(
            runtime_settings,
            lifecycle=lifecycle,
            llm_factory=declared_llm,
            context_factory=declared_context,
            cleanup_grace_seconds=cleanup_grace_seconds,
        ),
        lifecycle,
        runtime_settings,
    )


async def _wait_for_admission_queue(
    lifecycle: PipelineAdmissionController,
    expected: int,
) -> None:
    for _ in range(1_000):
        if lifecycle.queued == expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {expected} queued pipeline runs, found {lifecycle.queued}")


class _DependencyRealizationTrace:
    def __init__(self) -> None:
        self.factory_started = threading.Event()
        self.product_closed = threading.Event()
        self.factory_threads: list[int] = []
        self.factory_blocking_counts: list[int] = []
        self.close_threads: list[int] = []
        self.close_blocking_counts: list[int] = []
        self.close_service_owner_counts: list[int] = []
        self.products: list[Any] = []
        self.supporting_factory_calls = 0

    def record_factory(self, lifecycle: PipelineAdmissionController) -> None:
        self.factory_threads.append(threading.get_ident())
        self.factory_blocking_counts.append(lifecycle.blocking_in_flight)

    def record_close(self, lifecycle: PipelineAdmissionController) -> None:
        self.close_threads.append(threading.get_ident())
        self.close_blocking_counts.append(lifecycle.blocking_in_flight)
        self.close_service_owner_counts.append(lifecycle.service_owner_in_flight)
        self.product_closed.set()


def _dependency_realization_case(
    tmp_path,
    capability: str,
    *,
    rejected: bool = False,
    factory_release: threading.Event | None = None,
) -> tuple[PipelineDependencies, _DependencyRealizationTrace]:
    runtime_settings = _settings(
        tmp_path,
        suffix=f"realize-{capability}",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="mcp" if capability == "context" else "none",
        context_mcp_server_url="http://127.0.0.1:8765",
        grafana_enabled=capability == "backend",
        grafana_url="http://127.0.0.1:3000",
        signalfx_enabled=False,
        pipeline_max_concurrent=1,
        pipeline_max_queued=2,
    )
    if rejected:
        if capability == "llm":
            product_settings = runtime_settings.model_copy(update={"llm_api_base": "http://127.0.0.1:11435"})
        elif capability == "context":
            product_settings = runtime_settings.model_copy(update={"context_mcp_server_url": "http://127.0.0.1:8766"})
        else:
            product_settings = runtime_settings.model_copy(update={"grafana_url": "http://127.0.0.1:3001"})
    else:
        product_settings = runtime_settings

    trace = _DependencyRealizationTrace()
    lifecycle_box: list[PipelineAdmissionController] = []

    class TracedProvider(_ProviderProbe):
        async def close(self) -> None:
            trace.record_close(lifecycle_box[0])
            await super().close()

    class TracedContext(_ContextProbe):
        async def close(self) -> None:
            trace.record_close(lifecycle_box[0])
            await super().close()

    class TracedBackend(_BackendProbe):
        async def close(self) -> None:
            trace.record_close(lifecycle_box[0])
            await super().close()

    def target_factory() -> Any:
        lifecycle = lifecycle_box[0]
        trace.record_factory(lifecycle)
        if capability == "llm":
            product: Any = TracedProvider(product_settings)
        elif capability == "context":
            product = TracedContext(product_settings)
        else:
            product = TracedBackend(product_settings)
            product.runtime_ownership = runtime_descriptor_for_backends(
                component="traced_realized_backend",
                runtime_settings=product_settings,
            )
        trace.products.append(product)
        trace.factory_started.set()
        if factory_release is not None:
            assert factory_release.wait(timeout=2.0)
        return [cast(DashboardBackend, product)] if capability == "backend" else product

    def supporting_llm_factory() -> LLMProvider:
        trace.supporting_factory_calls += 1
        return _ProviderProbe(runtime_settings)

    selected_llm_factory = target_factory if capability == "llm" else supporting_llm_factory
    declared_llm_factory = declare_runtime_factory(
        cast(Any, selected_llm_factory),
        ownership=runtime_descriptor_for_provider(
            component=f"{capability}_matrix_llm_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    declared_context_factory = (
        declare_runtime_factory(
            cast(Any, target_factory),
            ownership=runtime_descriptor_for_provider(
                component="context_matrix_factory",
                runtime_settings=runtime_settings,
                capability="context",
            ),
            factory_kind="provider:context",
        )
        if capability == "context"
        else None
    )
    selected_backend_factory = target_factory if capability == "backend" else (lambda: [])
    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=_backend_factory(cast(Any, selected_backend_factory), runtime_settings),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component=f"{capability}_matrix_history_factory",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component=f"{capability}_matrix_feedback_factory",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=cast(Any, declared_llm_factory),
        context_provider_factory=cast(Any, declared_context_factory),
        cleanup_grace_seconds=0.05,
    )
    assert dependencies.pipeline_admission is not None
    lifecycle_box.append(dependencies.pipeline_admission)
    return dependencies, trace


async def _realize_dependency_product(
    dependencies: PipelineDependencies,
    capability: str,
) -> Any:
    if capability == "backend":
        realize_backends = getattr(dependencies, "realize_backends")
        products = await realize_backends()
        assert len(products) == 1
        return products[0]
    await dependencies.acquire_resources()
    factory = dependencies.llm_provider_factory if capability == "llm" else dependencies.context_provider_factory
    assert factory is not None
    return factory()


async def _close_dependency_products(
    dependencies: PipelineDependencies,
    capability: str,
    products: list[Any],
) -> None:
    if capability == "backend":
        await asyncio.gather(*(product.close() for product in products))
    else:
        await dependencies.close_resources()


async def _assert_dependency_lifecycle_idle(lifecycle: PipelineAdmissionController) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 1.0
    while (
        lifecycle.in_flight or lifecycle.blocking_in_flight or lifecycle.service_owner_in_flight or lifecycle.retained
    ) and loop.time() < deadline:
        await asyncio.sleep(0.001)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.retained == 0


async def _assert_provider_factory_terminal_idle(dependencies: PipelineDependencies) -> None:
    lifecycle = dependencies.pipeline_admission
    resources = dependencies.provider_lifecycle_owner
    assert lifecycle is not None
    assert isinstance(resources, _RuntimeProviderResources)
    await _assert_dependency_lifecycle_idle(lifecycle)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 1.0
    while True:
        with resources._lock:
            state = resources._generation_owner
            active_operations = 0 if state is None else len(state.active_operations)
            active_handoffs = 0 if state is None else len(state.active_handoffs)
            committed_submissions = 0 if state is None else len(state.committed_submissions)
        if (
            resources._provider_factory_work.active == 0
            and active_operations == 0
            and active_handoffs == 0
            and committed_submissions == 0
        ):
            break
        if loop.time() >= deadline:
            break
        await asyncio.sleep(0.001)
    assert resources._provider_factory_work.active == 0
    assert active_operations == 0
    assert active_handoffs == 0
    assert committed_submissions == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.retained == 0


def _observe_blocking_permit_release(
    monkeypatch: pytest.MonkeyPatch,
    lifecycle: PipelineAdmissionController,
) -> threading.Event:
    released = threading.Event()
    original_release = lifecycle.release_blocking_permit

    def observe_release(permit: Any) -> None:
        original_release(permit)
        released.set()

    monkeypatch.setattr(lifecycle, "release_blocking_permit", observe_release)
    return released


def _assert_terminal_provider_cleanup(
    resources: _RuntimeProviderResources,
    lifecycle: PipelineAdmissionController,
) -> None:
    assert resources.lifecycle_state is ProviderLifecycleState.REVOKED
    assert resources._llm_provider is None
    assert resources._context_provider is None
    assert resources._generation_owner is None
    retired = resources._retired_generation
    if retired is not None:
        assert retired.retained_products == ()
        assert retired.cleanup_future is None
        assert retired.loop is None
    fatal = lifecycle.runtime_fatal_circuit
    assert fatal is not None
    assert 0 < len(fatal.reason_code) <= 128
    assert 0 < len(fatal.error_type) <= 128
    assert resources.quarantined_generation_count == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.queued == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.execution_graph.root_owner_count == 0


def _assert_collected(reference: weakref.ReferenceType[Any]) -> None:
    gc.collect()
    assert reference() is None


def test_backend_factory_declaration_is_lazy_and_public(tmp_path) -> None:
    runtime_settings = _settings(tmp_path)
    calls = 0

    def local_backends() -> list[DashboardBackend]:
        nonlocal calls
        calls += 1
        return []

    declared = declare_backend_factory(
        local_backends,
        runtime_settings=runtime_settings,
        component="local_eval_backends",
    )

    assert calls == 0
    assert declared.factory_kind == "backend:dashboard"
    assert declared.runtime_ownership == runtime_descriptor_for_backends(
        component="local_eval_backends",
        runtime_settings=runtime_settings,
    )
    assert declared() == []
    assert calls == 1


@pytest.mark.parametrize("grace", [0.0, -1.0, 300.1, float("inf"), float("nan")])
def test_pipeline_cleanup_grace_is_finite_and_bounded(tmp_path, grace: float) -> None:
    runtime_settings = _settings(tmp_path)

    with pytest.raises(ValueError, match="cleanup grace"):
        build_pipeline_dependencies(
            runtime_settings,
            stores=RuntimeStores(runtime_settings),
            cleanup_grace_seconds=grace,
        )


def test_ownerless_backend_factory_fails_before_invocation(tmp_path) -> None:
    active = _settings(tmp_path)
    calls = 0

    def ownerless_factory() -> list[DashboardBackend]:
        nonlocal calls
        calls += 1
        raise AssertionError("ownerless backend factory was invoked")

    with pytest.raises(RuntimeOwnershipError, match="declared runtime owner"):
        build_pipeline_dependencies(
            active,
            stores=RuntimeStores(active),
            backend_factory=ownerless_factory,
        )

    assert calls == 0


def test_foreign_backend_factory_fails_before_invocation(tmp_path) -> None:
    active = _settings(tmp_path)
    foreign = _settings(
        tmp_path,
        suffix="foreign",
        grafana_url="https://foreign-grafana.example",
    )
    calls = 0

    def foreign_factory() -> list[DashboardBackend]:
        nonlocal calls
        calls += 1
        raise AssertionError("foreign backend factory was invoked")

    declared = declare_runtime_factory(
        foreign_factory,
        ownership=runtime_descriptor_for_backends(
            component="foreign_backend_factory",
            runtime_settings=foreign,
        ),
        factory_kind="backend:dashboard",
    )

    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        build_pipeline_dependencies(
            active,
            stores=RuntimeStores(active),
            backend_factory=declared,
        )

    assert calls == 0


def test_realized_backend_is_rejected_when_no_backend_remote_is_declared(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        grafana_enabled=False,
        signalfx_enabled=False,
    )
    backend = _BackendProbe(runtime_settings)
    backend.runtime_ownership = runtime_descriptor_for_backends(
        component="disabled_realized_backend",
        runtime_settings=runtime_settings,
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        backend_factory=_backend_factory(lambda: [cast(DashboardBackend, backend)], runtime_settings),
    )

    with pytest.raises(RuntimeOwnershipError, match="backend realization failed"):
        dependencies.backend_factory()


@pytest.mark.asyncio
@pytest.mark.parametrize("realized_remote_indexes", [(), (0,), (0, 0), (0, 1, 1)])
async def test_realized_backend_set_must_match_declared_remotes_one_to_one(
    tmp_path,
    realized_remote_indexes: tuple[int, ...],
) -> None:
    runtime_settings = _settings(
        tmp_path,
        grafana_enabled=True,
        signalfx_enabled=True,
        signalfx_api_token="test-token",
    )
    expected = runtime_descriptor_for_backends(
        component="backend_set_test",
        runtime_settings=runtime_settings,
    )
    realized: list[_BackendProbe] = []

    def backend_factory() -> list[DashboardBackend]:
        for index in realized_remote_indexes:
            backend = _BackendProbe(runtime_settings)
            backend.runtime_ownership = replace(
                backend.runtime_ownership,
                remotes=(expected.remotes[index],),
            )
            realized.append(backend)
        return cast(list[DashboardBackend], realized)

    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        backend_factory=_backend_factory(backend_factory, runtime_settings),
        cleanup_grace_seconds=0.01,
    )
    assert dependencies.pipeline_admission is not None

    async with dependencies.pipeline_admission.slot():
        with pytest.raises(RuntimeOwnershipError, match="backend realization failed"):
            dependencies.backend_factory()

    for _ in range(100):
        if all(backend.close_calls == 1 for backend in realized) and dependencies.pipeline_admission.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert [backend.close_calls for backend in realized] == [1] * len(realized)
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
async def test_rejected_backend_retirement_continues_after_one_close_fails(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        grafana_enabled=True,
        signalfx_enabled=True,
        signalfx_api_token="test-token",
    )
    expected = runtime_descriptor_for_backends(
        component="backend_close_failure_test",
        runtime_settings=runtime_settings,
    )

    class FailingCloseBackend(_BackendProbe):
        async def close(self) -> None:
            await super().close()
            raise RuntimeError("synthetic backend close failure")

    first = FailingCloseBackend(runtime_settings)
    second = _BackendProbe(runtime_settings)
    first.runtime_ownership = replace(first.runtime_ownership, remotes=(expected.remotes[0],))
    second.runtime_ownership = replace(second.runtime_ownership, remotes=(expected.remotes[0],))
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        backend_factory=_backend_factory(
            lambda: cast(list[DashboardBackend], [first, second]),
            runtime_settings,
        ),
    )

    with pytest.raises(RuntimeOwnershipError, match="backend realization failed"):
        await dependencies.realize_backends()

    assert first.close_calls == 1
    assert second.close_calls == 1
    assert dependencies.pipeline_admission is not None
    assert dependencies.pipeline_admission.in_flight == 0
    assert dependencies.pipeline_admission.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_realized_backend_set_accepts_each_declared_remote_exactly_once(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        grafana_enabled=True,
        signalfx_enabled=True,
        signalfx_api_token="test-token",
    )
    expected = runtime_descriptor_for_backends(
        component="backend_set_test",
        runtime_settings=runtime_settings,
    )
    realized = [_BackendProbe(runtime_settings) for _remote in expected.remotes]
    for backend, remote in zip(realized, expected.remotes, strict=True):
        backend.runtime_ownership = replace(backend.runtime_ownership, remotes=(remote,))
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        backend_factory=_backend_factory(lambda: cast(list[DashboardBackend], realized), runtime_settings),
    )

    assert dependencies.backend_factory() == realized
    await asyncio.gather(*(backend.close() for backend in realized))


@pytest.mark.asyncio
async def test_realized_backend_owner_is_rejected_before_discovery_and_audited(tmp_path) -> None:
    active = _settings(tmp_path, grafana_url="https://active-grafana.example")
    foreign = _settings(
        tmp_path,
        suffix="foreign",
        grafana_url="https://foreign-grafana.example",
    )
    backend_calls = 0
    history_starts = 0
    history_finishes = 0

    admitted_history = InvestigationStore(active.history_db_path, runtime_settings=active)

    class HistoryProbe:
        runtime_ownership = admitted_history.runtime_ownership
        sqlite_readiness_admission = admitted_history.sqlite_readiness_admission

        def start(self, *_args, **_kwargs):
            nonlocal history_starts
            history_starts += 1
            return "inv-backend-owner"

        def finish(self, *_args, **_kwargs):
            nonlocal history_finishes
            history_finishes += 1

    history_probe = HistoryProbe()

    def backend_factory() -> list[DashboardBackend]:
        nonlocal backend_calls
        backend_calls += 1
        return [cast(DashboardBackend, _BackendProbe(foreign))]

    dependencies = build_pipeline_dependencies(
        active,
        stores=RuntimeStores(active),
        backend_factory=_backend_factory(backend_factory, active),
        history_store_factory=declare_runtime_factory(
            lambda: history_probe,
            ownership=history_probe.runtime_ownership,
            factory_kind="store:history",
        ),
    )

    with pytest.raises(PipelineExecutionError) as exc_info:
        await run_pipeline(DashRequest(prompt="checkout latency"), dependencies)

    assert isinstance(exc_info.value.__cause__, RuntimeOwnershipError)
    assert backend_calls == 1
    assert history_starts == 1
    assert history_finishes == 1


def test_signal_factory_declaration_is_checked_before_invocation(tmp_path) -> None:
    active = _settings(tmp_path)
    foreign = _settings(tmp_path, suffix="foreign")
    calls = 0

    def foreign_factory():
        nonlocal calls
        calls += 1
        foreign_path = tmp_path / "foreign-side-effect.db"
        foreign_path.touch()
        raise AssertionError("mismatched signal factory was invoked")

    declared = declare_runtime_factory(
        foreign_factory,
        ownership=runtime_descriptor_for_store(
            component="foreign_signal_factory",
            runtime_settings=foreign,
            database_role="signals",
            database_path=foreign.signals_db_path,
        ),
        factory_kind="store:signals",
    )

    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        build_pipeline_dependencies(
            active,
            stores=RuntimeStores(active),
            signal_store_factory=declared,
        )

    assert calls == 0
    assert not (tmp_path / "foreign-side-effect.db").exists()


def test_ownerless_signal_factory_fails_before_invocation(tmp_path) -> None:
    active = _settings(tmp_path)
    calls = 0

    def ownerless_factory():
        nonlocal calls
        calls += 1
        raise AssertionError("ownerless signal factory was invoked")

    with pytest.raises(RuntimeOwnershipError, match="declared runtime owner"):
        build_pipeline_dependencies(
            active,
            stores=RuntimeStores(active),
            signal_store_factory=ownerless_factory,
        )

    assert calls == 0


def test_factory_preflight_does_not_execute_dynamic_ownership_properties(tmp_path) -> None:
    active = _settings(tmp_path)
    descriptor_reads = 0
    calls = 0

    class HostileFactory:
        @property
        def runtime_ownership(self):
            nonlocal descriptor_reads
            descriptor_reads += 1
            raise AssertionError("dynamic ownership property was evaluated")

        @property
        def factory_kind(self):
            raise AssertionError("dynamic factory kind was evaluated")

        def __call__(self):
            nonlocal calls
            calls += 1
            raise AssertionError("hostile factory was invoked")

    with pytest.raises(RuntimeOwnershipError, match="declared runtime owner"):
        build_pipeline_dependencies(
            active,
            stores=RuntimeStores(active),
            signal_store_factory=HostileFactory(),
        )

    assert descriptor_reads == 0
    assert calls == 0


def test_ownerless_provider_factory_fails_before_invocation(tmp_path) -> None:
    active = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    calls = 0

    def ownerless_factory() -> LLMProvider:
        nonlocal calls
        calls += 1
        raise AssertionError("ownerless provider factory was invoked")

    with pytest.raises(RuntimeOwnershipError, match="declared runtime owner"):
        PipelineDependencies.isolated(
            settings=active,
            backend_factory=_backend_factory(lambda: [], active),
            history_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component="history_factory",
                    runtime_settings=active,
                    database_role="history",
                    database_path=active.history_db_path,
                ),
                factory_kind="store:history",
            ),
            feedback_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component="feedback_factory",
                    runtime_settings=active,
                    database_role="feedback",
                    database_path=active.feedback_db_path,
                ),
                factory_kind="store:feedback",
            ),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
            llm_provider_factory=ownerless_factory,
        )

    assert calls == 0


def test_provider_factory_declaration_is_checked_before_invocation(tmp_path) -> None:
    active = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    foreign = active.model_copy(update={"llm_api_base": "http://127.0.0.1:11435"})
    calls = 0

    def foreign_factory() -> LLMProvider:
        nonlocal calls
        calls += 1
        raise AssertionError("mismatched provider factory was invoked")

    declared = declare_runtime_factory(
        foreign_factory,
        ownership=runtime_descriptor_for_provider(
            component="foreign_llm_factory",
            runtime_settings=foreign,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )

    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        PipelineDependencies.isolated(
            settings=active,
            backend_factory=_backend_factory(lambda: [], active),
            history_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component="history_factory",
                    runtime_settings=active,
                    database_role="history",
                    database_path=active.history_db_path,
                ),
                factory_kind="store:history",
            ),
            feedback_store_factory=declare_runtime_factory(
                lambda: object(),
                ownership=runtime_descriptor_for_store(
                    component="feedback_factory",
                    runtime_settings=active,
                    database_role="feedback",
                    database_path=active.feedback_db_path,
                ),
                factory_kind="store:feedback",
            ),
            llm_cache={},
            cache_key_factory=lambda *parts: ":".join(parts),
            llm_provider_factory=declared,
        )

    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_rejected_provider_products_are_closed_by_the_runtime_lifecycle(tmp_path, capability: str) -> None:
    active = _settings(
        tmp_path,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="mcp",
        context_mcp_server_url="http://127.0.0.1:8765",
    )
    foreign = active.model_copy(
        update={
            "llm_api_base": "http://127.0.0.1:11435",
            "context_mcp_server_url": "http://127.0.0.1:8766",
        }
    )
    product: LLMProvider | ContextProvider
    provider_kwargs: dict[str, Any]
    if capability == "llm":
        product = _ProviderProbe(foreign)
        provider_kwargs = {
            "llm_provider_factory": declare_runtime_factory(
                lambda: cast(LLMProvider, product),
                ownership=runtime_descriptor_for_provider(
                    component="rejected_llm_factory",
                    runtime_settings=active,
                    capability="llm",
                ),
                factory_kind="provider:llm",
            )
        }
    else:
        product = _ContextProbe(foreign)
        provider_kwargs = {
            "context_provider_factory": declare_runtime_factory(
                lambda: cast(ContextProvider, product),
                ownership=runtime_descriptor_for_provider(
                    component="rejected_context_factory",
                    runtime_settings=active,
                    capability="context",
                ),
                factory_kind="provider:context",
            )
        }

    dependencies = PipelineDependencies.isolated(
        settings=active,
        backend_factory=_backend_factory(lambda: [], active),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="history_factory",
                runtime_settings=active,
                database_role="history",
                database_path=active.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="feedback_factory",
                runtime_settings=active,
                database_role="feedback",
                database_path=active.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        cleanup_grace_seconds=0.01,
        **provider_kwargs,
    )
    assert dependencies.pipeline_admission is not None
    async with dependencies.pipeline_admission.slot():
        with pytest.raises(RuntimeOwnershipError):
            await dependencies.acquire_resources()

    for _ in range(100):
        if product.closed and dependencies.pipeline_admission.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert product.closed is True
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context", "backend"])
async def test_dependency_realization_commits_admitted_worker_before_factory_and_releases_permit(
    monkeypatch,
    tmp_path,
    capability: str,
) -> None:
    dependencies, trace = _dependency_realization_case(tmp_path, capability)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    caller_thread = threading.get_ident()
    lifecycle_starts = 0
    precommit_factory_runs: list[str] = []
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    original_start = threading.Thread.start

    def observe_worker_start(thread: threading.Thread) -> None:
        nonlocal lifecycle_starts
        if not thread.name.startswith("tacit-lifecycle-"):
            original_start(thread)
            return
        lifecycle_starts += 1
        factory_count = len(trace.factory_threads)
        target = thread._target
        owner_waiting = threading.Event()
        assert target is not None
        closure = (
            dict(zip(target.__code__.co_freevars, target.__closure__, strict=True))
            if target.__closure__ is not None
            else {}
        )
        if "owner_start" in closure:
            owner_start = closure["owner_start"].cell_contents
            assert isinstance(owner_start, threading.Event)
            original_wait = owner_start.wait

            def observe_owner_wait(timeout: float | None = None) -> bool:
                owner_waiting.set()
                return original_wait(timeout)

            owner_start.wait = observe_owner_wait
        elif thread.name == "tacit-lifecycle-provider-owner" and "state" in closure:
            state = closure["state"].cell_contents

            def observe_provider_readiness() -> None:
                assert state.startup_ready.wait(timeout=1.0)
                owner_waiting.set()

            original_start(thread)
            observe_provider_readiness()
            if len(trace.factory_threads) != factory_count:
                precommit_factory_runs.append(thread.name)
            return
        else:
            call = thread._args[0]
            original_wait_for_commit = call.wait_for_start_commit

            def observe_start_commit() -> bool:
                owner_waiting.set()
                return original_wait_for_commit()

            call.wait_for_start_commit = observe_start_commit
        original_start(thread)
        assert owner_waiting.wait(timeout=1.0)
        if len(trace.factory_threads) != factory_count:
            precommit_factory_runs.append(thread.name)

    monkeypatch.setattr(threading.Thread, "start", observe_worker_start)
    product = await _realize_dependency_product(dependencies, capability)

    assert product is trace.products[0]
    assert lifecycle_starts >= 1
    assert precommit_factory_runs == []
    assert trace.factory_threads != [caller_thread]
    assert trace.factory_blocking_counts == [1]
    if capability == "backend":
        assert await asyncio.to_thread(permit_released.wait, 1.0)
        await _assert_dependency_lifecycle_idle(lifecycle)
    else:
        assert lifecycle.in_flight == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.retained == 0
        assert lifecycle.service_owner_in_flight == 1

    await _close_dependency_products(dependencies, capability, trace.products)
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_provider_factory_uses_dedicated_worker_while_owner_loop_remains_responsive(
    tmp_path,
    capability: str,
) -> None:
    release_factory = threading.Event()
    dependencies, trace = _dependency_realization_case(
        tmp_path,
        capability,
        factory_release=release_factory,
    )
    lifecycle = dependencies.pipeline_admission
    resources = dependencies.provider_lifecycle_owner
    assert lifecycle is not None
    assert isinstance(resources, _RuntimeProviderResources)
    requester_thread = threading.get_ident()
    owner_callback = threading.Event()
    realization = asyncio.create_task(_realize_dependency_product(dependencies, capability))
    product: Any = None

    try:
        assert await asyncio.to_thread(trace.factory_started.wait, 1.0)
        with resources._lock:
            state = resources._generation_owner
            assert state is not None
            owner_loop = state.loop
            owner_thread = state.owner_thread
        assert owner_loop is not None
        assert owner_thread is not None
        owner_loop.call_soon_threadsafe(owner_callback.set)
        owner_responsive = await asyncio.to_thread(owner_callback.wait, 0.2)
        factory_thread = trace.factory_threads[0]
        factory_worker_count = resources._provider_factory_work.active
        blocking_in_flight = lifecycle.blocking_in_flight
        service_owner_in_flight = lifecycle.service_owner_in_flight
    finally:
        release_factory.set()
        product = await realization
        await _close_dependency_products(dependencies, capability, trace.products)

    assert factory_thread != requester_thread
    assert factory_thread != owner_thread.ident
    assert owner_responsive is True
    assert factory_worker_count == 1
    assert blocking_in_flight == 1
    assert service_owner_in_flight == 1
    assert product is trace.products[0]
    assert trace.close_threads == [owner_thread.ident]
    await _assert_provider_factory_terminal_idle(dependencies)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
@pytest.mark.parametrize("accessor", ["async", "sync"])
async def test_provider_adoption_occurs_once_on_service_owner_thread(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capability: str,
    accessor: str,
) -> None:
    release_factory = threading.Event()
    dependencies, trace = _dependency_realization_case(
        tmp_path,
        capability,
        factory_release=release_factory,
    )
    resources = dependencies.provider_lifecycle_owner
    assert isinstance(resources, _RuntimeProviderResources)
    adoption_threads: list[int] = []
    original_adopt = getattr(resources, f"_adopt_{capability}_provider")

    async def observe_adoption(product: Any, **kwargs: Any) -> None:
        adoption_threads.append(threading.get_ident())
        await original_adopt(product, **kwargs)

    monkeypatch.setattr(resources, f"_adopt_{capability}_provider", observe_adoption)

    async def realize() -> Any:
        if accessor == "async":
            return await _realize_dependency_product(dependencies, capability)
        factory = dependencies.llm_provider_factory if capability == "llm" else dependencies.context_provider_factory
        assert factory is not None
        return await asyncio.to_thread(factory)

    realization = asyncio.create_task(realize())
    assert await asyncio.to_thread(trace.factory_started.wait, 1.0)
    with resources._lock:
        state = resources._generation_owner
        assert state is not None
        owner_thread = state.owner_thread
    assert owner_thread is not None
    release_factory.set()
    product = await realization

    assert product is trace.products[0]
    assert adoption_threads == [owner_thread.ident]
    assert trace.factory_threads != adoption_threads

    await dependencies.close_resources()
    assert trace.close_threads == [owner_thread.ident]
    await _assert_provider_factory_terminal_idle(dependencies)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_owner_adoption_rejection_retires_product_once_on_admitted_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capability: str,
) -> None:
    dependencies, trace = _dependency_realization_case(tmp_path, capability)
    resources = dependencies.provider_lifecycle_owner
    assert isinstance(resources, _RuntimeProviderResources)
    adoption_threads: list[int] = []

    async def reject_adoption(_product: Any, **_kwargs: Any) -> None:
        adoption_threads.append(threading.get_ident())
        raise RuntimeOwnershipError("synthetic owner adoption rejection")

    monkeypatch.setattr(resources, f"_adopt_{capability}_provider", reject_adoption)
    with pytest.raises(RuntimeOwnershipError, match="synthetic owner adoption rejection"):
        await _realize_dependency_product(dependencies, capability)

    assert await asyncio.to_thread(trace.product_closed.wait, 1.0)
    assert len(adoption_threads) == 1
    assert adoption_threads != trace.factory_threads
    assert trace.close_threads == trace.factory_threads
    assert len(trace.close_threads) == 1

    await dependencies.close_resources()
    await _assert_provider_factory_terminal_idle(dependencies)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context", "backend"])
async def test_rejected_dependency_product_is_closed_once_by_its_admitted_realizing_worker(
    tmp_path,
    capability: str,
) -> None:
    dependencies, trace = _dependency_realization_case(
        tmp_path,
        capability,
        rejected=True,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    caller_thread = threading.get_ident()

    try:
        with pytest.raises(RuntimeOwnershipError):
            await _realize_dependency_product(dependencies, capability)
        assert await asyncio.to_thread(trace.product_closed.wait, 1.0)
    finally:
        await _close_dependency_products(dependencies, capability, [])

    assert len(trace.products) == 1
    assert trace.factory_threads == trace.close_threads
    assert trace.factory_threads != [caller_thread]
    assert trace.factory_blocking_counts == [1]
    if capability in {"llm", "context"}:
        assert trace.close_blocking_counts == [1]
        assert trace.close_service_owner_counts == [1]
    else:
        assert trace.close_blocking_counts == [1]
        assert trace.close_service_owner_counts == [0]
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context", "backend"])
async def test_dependency_realization_cancelled_before_admission_runs_no_factory(
    tmp_path,
    capability: str,
) -> None:
    dependencies, trace = _dependency_realization_case(tmp_path, capability)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    holder_entered = asyncio.Event()
    release_holder = asyncio.Event()

    async def hold_runtime_capacity() -> None:
        async with lifecycle.slot():
            holder_entered.set()
            await release_holder.wait()

    holder = asyncio.create_task(hold_runtime_capacity())
    await holder_entered.wait()
    realization = asyncio.create_task(_realize_dependency_product(dependencies, capability))
    try:
        await _wait_for_admission_queue(lifecycle, 1)
        realization.cancel()
        with pytest.raises(asyncio.CancelledError):
            await realization
    finally:
        if not realization.done():
            realization.cancel()
        await asyncio.gather(realization, return_exceptions=True)
        release_holder.set()
        await holder
        await _close_dependency_products(dependencies, capability, trace.products)

    assert trace.products == []
    assert trace.factory_threads == []
    assert trace.supporting_factory_calls == 0
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context", "backend"])
async def test_dependency_realization_worker_start_failure_runs_no_factory(
    monkeypatch,
    tmp_path,
    capability: str,
) -> None:
    dependencies, trace = _dependency_realization_case(tmp_path, capability)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    original_start = threading.Thread.start

    def fail_lifecycle_worker_start(thread: threading.Thread) -> None:
        if thread.name.startswith("tacit-lifecycle-"):
            raise RuntimeError("synthetic lifecycle worker start failure")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_lifecycle_worker_start)
    try:
        with pytest.raises(RuntimeError, match="worker.*start"):
            await _realize_dependency_product(dependencies, capability)
    finally:
        await _close_dependency_products(dependencies, capability, trace.products)

    assert trace.products == []
    assert trace.factory_threads == []
    assert trace.supporting_factory_calls == 0
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context", "backend"])
async def test_cancelled_dependency_realization_retires_unadopted_product_on_realizing_worker(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capability: str,
) -> None:
    release_factory = threading.Event()
    dependencies, trace = _dependency_realization_case(
        tmp_path,
        capability,
        factory_release=release_factory,
    )
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    caller_thread = threading.get_ident()
    release_timer = threading.Timer(0.3, release_factory.set)
    release_timer.start()
    realization = asyncio.create_task(_realize_dependency_product(dependencies, capability))
    try:
        assert await asyncio.to_thread(trace.factory_started.wait, 1.0)
        if capability in {"llm", "context"}:
            resources = dependencies.provider_lifecycle_owner
            assert isinstance(resources, _RuntimeProviderResources)
            with resources._lock:
                state = resources._generation_owner
                assert state is not None
                owner_loop = state.loop
                owner_thread = state.owner_thread
            assert owner_loop is not None
            assert owner_thread is not None
            owner_callback = threading.Event()
            owner_loop.call_soon_threadsafe(owner_callback.set)
            owner_responsive = await asyncio.to_thread(owner_callback.wait, 0.1)
            factory_worker_count = resources._provider_factory_work.active
            with resources._lock:
                if capability == "llm":
                    assert resources._llm_provider is None
                else:
                    assert resources._context_initialized is False
                    assert resources._context_provider is None
        assert lifecycle.blocking_in_flight == 1
        realization.cancel()
        with pytest.raises(asyncio.CancelledError):
            await realization
    finally:
        release_factory.set()
        release_timer.cancel()
        release_timer.join(timeout=1.0)
        if not realization.done():
            realization.cancel()
        await asyncio.gather(realization, return_exceptions=True)

    assert await asyncio.to_thread(trace.product_closed.wait, 1.0)
    await _close_dependency_products(dependencies, capability, [])
    assert len(trace.products) == 1
    assert trace.factory_threads == trace.close_threads
    assert trace.factory_threads != [caller_thread]
    if capability in {"llm", "context"}:
        assert trace.factory_threads != [owner_thread.ident]
        assert owner_responsive is True
        assert factory_worker_count == 1
    assert trace.factory_blocking_counts == [1]
    if capability in {"llm", "context"}:
        assert trace.close_blocking_counts == [1]
        assert trace.close_service_owner_counts == [0]
    else:
        assert trace.close_blocking_counts == [1]
        assert trace.close_service_owner_counts == [0]
    assert await asyncio.to_thread(permit_released.wait, 1.0)
    await _assert_dependency_lifecycle_idle(lifecycle)
    if capability in {"llm", "context"}:
        await _assert_provider_factory_terminal_idle(dependencies)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_cancellation_during_owner_adoption_keeps_owner_responsive_and_settles_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capability: str,
) -> None:
    dependencies, trace = _dependency_realization_case(tmp_path, capability)
    lifecycle = dependencies.pipeline_admission
    resources = dependencies.provider_lifecycle_owner
    assert lifecycle is not None
    assert isinstance(resources, _RuntimeProviderResources)
    adoption_gate = _CrossLoopAsyncGate()
    adoption_threads: list[int] = []
    original_adopt = getattr(resources, f"_adopt_{capability}_provider")

    async def delayed_adoption(product: Any, **kwargs: Any) -> None:
        adoption_threads.append(threading.get_ident())
        await adoption_gate.wait()
        await original_adopt(product, **kwargs)

    monkeypatch.setattr(resources, f"_adopt_{capability}_provider", delayed_adoption)
    realization = asyncio.create_task(_realize_dependency_product(dependencies, capability))
    assert await asyncio.to_thread(adoption_gate.started.wait, 1.0)

    with resources._lock:
        state = resources._generation_owner
        assert state is not None
        owner_loop = state.loop
        owner_thread = state.owner_thread
    assert owner_loop is not None
    assert owner_thread is not None

    realization.cancel()
    await asyncio.sleep(0.01)
    owner_heartbeat = threading.Event()
    owner_loop.call_soon_threadsafe(owner_heartbeat.set)
    assert await asyncio.to_thread(owner_heartbeat.wait, 0.2)
    assert realization.done() is False
    assert resources._provider_factory_work.active == 1
    assert lifecycle.blocking_in_flight == 1
    assert lifecycle.service_owner_in_flight == 1

    adoption_gate.release()
    with pytest.raises(asyncio.CancelledError):
        await realization
    await dependencies.close_resources()

    assert adoption_threads == [owner_thread.ident]
    assert len(trace.products) == 1
    assert trace.close_threads == [owner_thread.ident]
    await _assert_provider_factory_terminal_idle(dependencies)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_provider_construction_cancellation_race_reaches_one_terminal_outcome_repeatedly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capability: str,
) -> None:
    for attempt in range(12):
        release_factory = threading.Event()
        dependencies, trace = _dependency_realization_case(
            tmp_path,
            capability,
            factory_release=release_factory,
        )
        resources = dependencies.provider_lifecycle_owner
        assert isinstance(resources, _RuntimeProviderResources)
        adoption_completed = threading.Event()
        original_adopt = getattr(resources, f"_adopt_{capability}_provider")

        async def observe_adoption(product: Any, **kwargs: Any) -> None:
            await original_adopt(product, **kwargs)
            adoption_completed.set()

        monkeypatch.setattr(resources, f"_adopt_{capability}_provider", observe_adoption)
        realization = asyncio.create_task(_realize_dependency_product(dependencies, capability))
        assert await asyncio.to_thread(trace.factory_started.wait, 1.0)
        with resources._lock:
            state = resources._generation_owner
            assert state is not None
            owner_thread = state.owner_thread
        assert owner_thread is not None

        if attempt % 2 == 0:
            realization.cancel()
            release_factory.set()
        else:
            release_factory.set()
            await asyncio.sleep(0)
            realization.cancel()

        outcome = (await asyncio.gather(realization, return_exceptions=True))[0]
        assert outcome is trace.products[0] or isinstance(
            outcome,
            (asyncio.CancelledError, RuntimeOwnershipError),
        )
        await dependencies.close_resources()

        assert len(trace.products) == 1
        assert len(trace.close_threads) == 1
        expected_close_thread = owner_thread.ident if adoption_completed.is_set() else trace.factory_threads[0]
        assert trace.close_threads == [expected_close_thread]
        await _assert_provider_factory_terminal_idle(dependencies)


@pytest.mark.parametrize("start_failure", ["definite", "ambiguous"])
@pytest.mark.asyncio
async def test_provider_factory_worker_start_failure_runs_no_factory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    start_failure: str,
) -> None:
    dependencies, trace = _dependency_realization_case(tmp_path, "llm")
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None
    original_start = threading.Thread.start
    started_workers: list[threading.Thread] = []

    def fail_provider_factory_worker_start(thread: threading.Thread) -> None:
        if thread.name != "tacit-lifecycle-blocking-work":
            original_start(thread)
            return
        if start_failure == "ambiguous":
            started_workers.append(thread)
            original_start(thread)
        raise RuntimeError("synthetic provider factory worker start failure")

    monkeypatch.setattr(threading.Thread, "start", fail_provider_factory_worker_start)
    try:
        with pytest.raises(RuntimeError, match="blocking worker could not start"):
            await dependencies.acquire_resources()
    finally:
        for thread in started_workers:
            thread.join(timeout=1.0)
        await dependencies.close_resources()

    assert all(not thread.is_alive() for thread in started_workers)
    assert trace.factory_threads == []
    assert trace.products == []
    await _assert_provider_factory_terminal_idle(dependencies)


def test_requester_loop_loss_keeps_provider_factory_worker_owned_and_owner_responsive(tmp_path) -> None:
    release_factory = threading.Event()
    dependencies, trace = _dependency_realization_case(
        tmp_path,
        "llm",
        factory_release=release_factory,
    )
    lifecycle = dependencies.pipeline_admission
    resources = dependencies.provider_lifecycle_owner
    assert lifecycle is not None
    assert isinstance(resources, _RuntimeProviderResources)
    requester_ready = threading.Event()
    requester_stopped = threading.Event()
    requester_loop_box: list[asyncio.AbstractEventLoop] = []
    requester_task_box: list[asyncio.Task[Any]] = []
    requester_thread_id: list[int] = []

    def run_requester() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        requester_loop_box.append(loop)
        requester_thread_id.append(threading.get_ident())
        requester_task_box.append(loop.create_task(_realize_dependency_product(dependencies, "llm")))
        requester_ready.set()
        loop.run_forever()
        requester_stopped.set()

    requester = threading.Thread(target=run_requester, name="provider-factory-requester")
    requester.start()
    assert requester_ready.wait(timeout=1.0)
    assert trace.factory_started.wait(timeout=1.0)
    with resources._lock:
        state = resources._generation_owner
        assert state is not None
        owner_loop = state.loop
        owner_thread = state.owner_thread
    assert owner_loop is not None
    assert owner_thread is not None

    requester_loop = requester_loop_box[0]
    requester_loop.call_soon_threadsafe(requester_loop.stop)
    assert requester_stopped.wait(timeout=1.0)
    requester.join(timeout=1.0)
    assert requester.is_alive() is False

    owner_callback = threading.Event()
    owner_loop.call_soon_threadsafe(owner_callback.set)
    owner_responsive = owner_callback.wait(timeout=0.2)
    factory_thread = trace.factory_threads[0]
    factory_worker_count = resources._provider_factory_work.active
    blocking_in_flight = lifecycle.blocking_in_flight
    service_owner_in_flight = lifecycle.service_owner_in_flight

    release_factory.set()
    assert trace.product_closed.wait(timeout=0.05) is False
    asyncio.run(resources.shutdown())

    requester_task = requester_task_box[0]

    def settle_requester() -> None:
        asyncio.set_event_loop(requester_loop)
        try:
            requester_loop.run_until_complete(asyncio.gather(requester_task, return_exceptions=True))
        finally:
            requester_loop.close()

    requester_cleanup = threading.Thread(target=settle_requester, name="provider-factory-requester-cleanup")
    requester_cleanup.start()
    requester_cleanup.join(timeout=1.0)

    assert requester_cleanup.is_alive() is False
    assert factory_thread != requester_thread_id[0]
    assert factory_thread != owner_thread.ident
    assert owner_responsive is True
    assert factory_worker_count == 1
    assert blocking_in_flight == 1
    assert service_owner_in_flight == 1
    assert len(trace.products) == 1
    assert trace.close_threads == [owner_thread.ident]
    asyncio.run(_assert_provider_factory_terminal_idle(dependencies))


def test_factory_failure_observability_contains_only_stable_fields(tmp_path) -> None:
    active = _settings(tmp_path, knowledge_tenant_id="tenant-secret")
    foreign = _settings(
        tmp_path,
        suffix="foreign",
        knowledge_tenant_id="other-secret",
        llm_api_base="https://sensitive.example.invalid/v1",
    )
    declared = declare_runtime_factory(
        lambda: object(),
        ownership=runtime_descriptor_for_store(
            component="foreign_signal_factory",
            runtime_settings=foreign,
            database_role="signals",
            database_path=foreign.signals_db_path,
        ),
        factory_kind="store:signals",
    )

    with capture_logs() as logs, pytest.raises(RuntimeOwnershipError):
        build_pipeline_dependencies(active, stores=RuntimeStores(active), signal_store_factory=declared)

    failures = [entry for entry in logs if entry.get("event") == "runtime_factory_ownership_failed"]
    assert failures
    serialized = repr(logs)
    assert failures[-1]["phase"] == "preflight"
    assert failures[-1]["factory_kind"] == "store:signals"
    assert failures[-1]["reason_code"] == "runtime_factory_owner_mismatch"
    assert "tenant-secret" not in serialized
    assert "other-secret" not in serialized
    assert "sensitive.example.invalid" not in serialized
    assert str(tmp_path) not in serialized


def test_realized_signal_owner_failure_is_observed_without_identity_values(tmp_path) -> None:
    active = _settings(tmp_path, knowledge_tenant_id="tenant-secret")
    foreign = _settings(tmp_path, suffix="foreign", knowledge_tenant_id="other-secret")

    class ForeignStore:
        runtime_ownership = runtime_descriptor_for_store(
            component="foreign_realized_signal_store",
            runtime_settings=foreign,
            database_role="signals",
            database_path=foreign.signals_db_path,
        )

    declared = declare_runtime_factory(
        ForeignStore,
        ownership=runtime_descriptor_for_store(
            component="declared_signal_factory",
            runtime_settings=active,
            database_role="signals",
            database_path=active.signals_db_path,
        ),
        factory_kind="store:signals",
    )
    dependencies = build_pipeline_dependencies(
        active,
        stores=RuntimeStores(active),
        signal_store_factory=declared,
    )

    with capture_logs() as logs, pytest.raises(RuntimeOwnershipError):
        assert dependencies.signal_store_factory is not None
        dependencies.signal_store_factory()

    failures = [entry for entry in logs if entry.get("event") == "runtime_factory_ownership_failed"]
    assert failures[-1]["phase"] == "realization"
    assert failures[-1]["factory_kind"] == "store:signals"
    assert failures[-1]["reason_code"] == "runtime_factory_realization_mismatch"
    serialized = repr(logs)
    assert "tenant-secret" not in serialized
    assert "other-secret" not in serialized
    assert str(tmp_path) not in serialized


@pytest.mark.parametrize("error_type", [OSError, RuntimeOwnershipError])
def test_factory_invocation_failure_is_observed_without_exception_details(tmp_path, error_type) -> None:
    active = _settings(tmp_path, knowledge_tenant_id="tenant-secret")
    sensitive_detail = f"tenant-secret endpoint=https://secret.invalid path={tmp_path}"

    def failing_factory():
        raise error_type(sensitive_detail)

    declared = declare_runtime_factory(
        failing_factory,
        ownership=runtime_descriptor_for_store(
            component="failing_signal_factory",
            runtime_settings=active,
            database_role="signals",
            database_path=active.signals_db_path,
        ),
        factory_kind="store:signals",
    )
    dependencies = build_pipeline_dependencies(
        active,
        stores=RuntimeStores(active),
        signal_store_factory=declared,
    )

    with capture_logs() as logs, pytest.raises(error_type, match="tenant-secret"):
        assert dependencies.signal_store_factory is not None
        dependencies.signal_store_factory()

    failures = [entry for entry in logs if entry.get("event") == "runtime_factory_ownership_failed"]
    assert failures == [
        {
            "phase": "realization",
            "factory_kind": "store:signals",
            "reason_code": "runtime_factory_realization_failed",
            "dimensions": [],
            "event": "runtime_factory_ownership_failed",
            "log_level": "warning",
        }
    ]
    assert sensitive_detail not in repr(logs)
    assert str(tmp_path) not in repr(logs)


def test_bedrock_constructor_and_preflight_do_not_construct_sdk_resources(monkeypatch, tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_model_id="",
        llm_model="claude-sonnet-4-20250514",
        llm_aws_access_key_id="AKIATESTFIXTURE",
        llm_aws_secret_access_key="test-fixture-secret",
    )
    build_session = MagicMock()
    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build_session)

    provider = BedrockProvider(runtime_settings)

    build_session.assert_not_called()
    provider.realize_blocking()
    build_session.assert_not_called()
    assert provider.bedrock_credential_identity is None
    assert provider.runtime_ownership.remotes


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["profile", "default"])
async def test_dependency_acquire_constructs_bedrock_provider_without_sdk_resources(
    monkeypatch,
    tmp_path,
    selector: str,
) -> None:
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    if selector == "profile":
        monkeypatch.setenv("AWS_PROFILE", "integration-profile")
    else:
        monkeypatch.delenv("AWS_PROFILE", raising=False)
    selected_profile = "integration-profile" if selector == "profile" else "default"
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        f"[{selected_profile}]\naws_access_key_id = AKIAPREPARED\naws_secret_access_key = prepared-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    build_session = MagicMock()
    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build_session)
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
    )
    assert dependencies.pipeline_admission is not None
    assert dependencies.llm_provider_factory is not None
    with pytest.raises(RuntimeOwnershipError, match="were not acquired"):
        dependencies.llm_provider_factory()

    async with dependencies.pipeline_admission.slot():
        await dependencies.acquire_resources()
        provider = dependencies.llm_provider_factory()
        assert isinstance(provider, BedrockProvider)
        assert provider.bedrock_credential_identity is None
        build_session.assert_not_called()
        await dependencies.close_resources()

    build_session.assert_not_called()
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
async def test_bedrock_runtime_rejects_custom_non_bedrock_provider_inside_admitted_worker(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_aws_access_key_id="AKIACUSTOMFACTORY",
        llm_aws_secret_access_key="custom-factory-secret",
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    factory_observations: list[tuple[int, int]] = []

    class NonBedrockProvider(_ProviderProbe):
        def __init__(self) -> None:
            super().__init__(runtime_settings)
            self.chat_calls = 0

        async def chat_json(self, *_args, **_kwargs) -> LLMResult:
            self.chat_calls += 1
            return LLMResult("{}")

        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            self.chat_calls += 1
            return LLMResult("")

    provider = NonBedrockProvider()
    plan = BedrockCredentialPlan.capture(runtime_settings)

    def provider_factory() -> LLMProvider:
        factory_observations.append((threading.get_ident(), lifecycle.blocking_in_flight))
        return provider

    resources = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=lifecycle,
        llm_factory=declare_runtime_factory(
            provider_factory,
            ownership=plan.ownership(component="custom_bedrock_provider_factory"),
            factory_kind="provider:llm",
        ),
    )
    caller_thread = threading.get_ident()

    async with lifecycle.slot():
        with pytest.raises(RuntimeOwnershipError, match="operation-scoped Bedrock provider"):
            await resources.acquire()

    assert len(factory_observations) == 1
    worker_thread, blocking_in_flight = factory_observations[0]
    assert worker_thread != caller_thread
    assert blocking_in_flight == 1
    assert provider.closed is True
    assert provider.chat_calls == 0
    assert lifecycle.in_flight == 0


@pytest.mark.asyncio
async def test_bedrock_operation_accepts_current_credential_identity_and_cleans_up(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    identity = BedrockCredentialIdentity(
        account="arn:aws:iam::123456789012:role/tacitruntime",
        credential_fingerprint=credential_fingerprint("temporary-generation"),
        uses_sts=True,
    )
    credential_client = MagicMock()
    runtime_client = MagicMock()
    runtime_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "accepted"}]}},
    }
    session = MagicMock()
    session.client.return_value = runtime_client
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _ResolvedBedrockRuntime(
            session=session,
            credential_identity=identity,
            credential_clients=(credential_client,),
        ),
    )
    provider = BedrockProvider(credential_plan=plan)
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    provider.bind_pipeline_lifecycle(lifecycle)

    async with lifecycle.slot():
        result = await provider.chat_text("system", "user")

    assert result.text == "accepted"
    runtime_client.converse.assert_called_once()
    runtime_client.close.assert_called_once_with()
    credential_client.close.assert_called_once_with()
    session.close.assert_called_once_with()
    assert lifecycle.in_flight == 0


@pytest.mark.asyncio
async def test_bedrock_operation_rejects_current_identity_mismatch_and_cleans_up(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    identity = BedrockCredentialIdentity(
        account="arn:aws:iam::999999999999:role/foreign",
        credential_fingerprint=credential_fingerprint("temporary-generation"),
        uses_sts=True,
    )
    credential_client = MagicMock()
    session = MagicMock()
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _ResolvedBedrockRuntime(
            session=session,
            credential_identity=identity,
            credential_clients=(credential_client,),
        ),
    )
    provider = BedrockProvider(credential_plan=plan)
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    provider.bind_pipeline_lifecycle(lifecycle)

    async with lifecycle.slot():
        with pytest.raises(
            RuntimeOwnershipError,
            match="credential (?:realization mismatch|identity does not match)",
        ) as exc_info:
            await provider.chat_text("system", "user")

    assert "account" in str(exc_info.value)
    credential_client.close.assert_called_once_with()
    session.close.assert_called_once_with()
    session.client.assert_not_called()
    assert lifecycle.in_flight == 0


def test_cancelled_blocking_work_releases_admission_after_originating_loop_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    worker_started = threading.Event()
    allow_worker = threading.Event()
    worker_finished = threading.Event()

    def blocking_call() -> None:
        worker_started.set()
        assert allow_worker.wait(timeout=2.0)
        worker_finished.set()

    loop = asyncio.new_event_loop()

    async def start_and_cancel() -> None:
        async with lifecycle.slot():
            call = asyncio.create_task(
                blocking_work.run(
                    blocking_call,
                    reason_code="closed_loop_blocking_worker_retained",
                )
            )
            while not worker_started.is_set():
                await asyncio.sleep(0)
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call

    try:
        loop.run_until_complete(start_and_cancel())
    finally:
        loop.close()

    assert lifecycle.in_flight == 1
    assert lifecycle.retained == 1
    allow_worker.set()
    assert worker_finished.wait(timeout=0.5)
    assert permit_released.wait(timeout=1.0)
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0


@pytest.mark.asyncio
async def test_saturated_background_cleanup_is_rejected_before_submission() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    cleanup_ran = threading.Event()
    occupied = asyncio.Event()
    release = asyncio.Event()

    async def hold_capacity() -> None:
        async with lifecycle.slot():
            occupied.set()
            await release.wait()

    holder = asyncio.create_task(hold_capacity())
    try:
        await occupied.wait()
        assert (
            blocking_work.run_background(
                cleanup_ran.set,
                reason_code="saturated_background_cleanup",
            )
            is False
        )
        assert cleanup_ran.is_set() is False
        assert blocking_work.active == 0
    finally:
        release.set()
        await holder

    assert cleanup_ran.is_set() is False
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_cancelled_off_lease_cleanup_wait_invokes_discard_rollback() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=1)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    active_lease = await lifecycle.acquire()
    cleanup_ran = threading.Event()
    rollback_calls = 0

    def rollback() -> None:
        nonlocal rollback_calls
        rollback_calls += 1

    cleanup = asyncio.create_task(
        blocking_work.run(
            cleanup_ran.set,
            reason_code="cancelled_off_lease_cleanup_wait",
            on_discarded=rollback,
            cleanup=True,
        )
    )
    try:
        await _wait_for_admission_queue(lifecycle, 1)
        assert blocking_work.active == 0
        assert cleanup_ran.is_set() is False

        cleanup.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cleanup
        await _wait_for_admission_queue(lifecycle, 0)
    finally:
        if not cleanup.done():
            cleanup.cancel()
            await asyncio.gather(cleanup, return_exceptions=True)
        lifecycle.release(active_lease)

    assert cleanup_ran.is_set() is False
    assert blocking_work.active == 0
    assert lifecycle.queued == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert rollback_calls == 1


@pytest.mark.asyncio
async def test_blocking_work_reserves_capacity_before_off_lease_submission() -> None:
    lifecycle = PipelineAdmissionController(2, max_queued=0)
    workers = [LifecycleOwnedBlockingWork(lifecycle) for _ in range(3)]
    started = [threading.Event(), threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]

    def blocking_call(index: int) -> None:
        started[index].set()
        assert release[index].wait(timeout=2.0)

    calls = [
        asyncio.create_task(
            workers[index].run(
                lambda index=index: blocking_call(index),
                reason_code="off_lease_blocking_worker_retained",
            )
        )
        for index in range(2)
    ]
    try:
        for event in started[:2]:
            assert await asyncio.to_thread(event.wait, 0.5)
        assert lifecycle.in_flight == 2
        assert lifecycle.retained == 2
        assert all(worker.active == 1 for worker in workers[:2])

        with pytest.raises(PipelineAdmissionRejected):
            await workers[2].run(
                lambda: started[2].set(),
                reason_code="off_lease_blocking_worker_rejected",
            )
        assert started[2].is_set() is False
        assert await asyncio.wait_for(asyncio.to_thread(lambda: "available"), timeout=0.5) == "available"
    finally:
        for event in release:
            event.set()
        await asyncio.gather(*calls)

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert all(worker.active == 0 for worker in workers)


@pytest.mark.asyncio
async def test_stale_inherited_lease_cannot_submit_blocking_work() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    child_go = asyncio.Event()
    function_ran = threading.Event()

    async def stale_child() -> None:
        await child_go.wait()
        await blocking_work.run(function_ran.set, reason_code="stale_inherited_lease")

    async with lifecycle.slot():
        child = asyncio.create_task(stale_child())

    occupied = asyncio.Event()
    release = asyncio.Event()

    async def hold_capacity() -> None:
        async with lifecycle.slot():
            occupied.set()
            await release.wait()

    holder = asyncio.create_task(hold_capacity())
    await occupied.wait()
    child_go.set()
    try:
        with pytest.raises(PipelineAdmissionRejected):
            await asyncio.wait_for(child, timeout=0.5)
        assert function_ran.is_set() is False
        assert blocking_work.active == 0
    finally:
        release.set()
        await holder
    assert lifecycle.in_flight == 0


def test_originating_loop_death_releases_blocking_work_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    worker_started = threading.Event()
    allow_worker = threading.Event()
    cleaned = threading.Event()

    def blocking_call() -> object:
        worker_started.set()
        assert allow_worker.wait(timeout=2.0)
        return object()

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda *_args: None)
    asyncio.set_event_loop(loop)
    task = loop.create_task(
        blocking_work.run(
            blocking_call,
            reason_code="originating_loop_died",
            on_abandoned_result=lambda _result: cleaned.set(),
        )
    )
    loop.run_until_complete(asyncio.to_thread(worker_started.wait, 0.5))
    task._log_destroy_pending = False
    loop.close()
    asyncio.set_event_loop(None)

    allow_worker.set()
    assert cleaned.wait(timeout=0.5)
    assert permit_released.wait(timeout=1.0)
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0


def test_direct_bedrock_uses_runtime_stores_admission_controller(monkeypatch, tmp_path) -> None:
    from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_aws_access_key_id="test-shared-controller-access-id",
        llm_aws_secret_access_key="test-shared-controller-secret",
    )
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _ResolvedBedrockRuntime(
            session=MagicMock(),
            credential_identity=_static_bedrock_identity(
                "test-shared-controller-access-id",
                "test-shared-controller-secret",
            ),
        ),
    )
    runtime_controller = RuntimeStores(runtime_settings).pipeline_admission()
    provider = BedrockProvider(runtime_settings)

    direct_controller, manages_slot = provider._execution_lifecycle()

    assert manages_slot is True
    assert direct_controller is runtime_controller


def test_completed_blocking_worker_releases_off_lease_capacity_when_origin_loop_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    worker_started = threading.Event()
    allow_worker = threading.Event()
    worker_finished = threading.Event()

    def blocking_call() -> str:
        worker_started.set()
        assert allow_worker.wait(timeout=2.0)
        worker_finished.set()
        return "completed"

    async def wait_until_started() -> None:
        while not worker_started.is_set():
            await asyncio.sleep(0)

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda *_args: None)
    task = loop.create_task(
        blocking_work.run(
            blocking_call,
            reason_code="stopped_origin_loop_worker_permit",
        )
    )
    task._log_destroy_pending = False
    try:
        loop.run_until_complete(wait_until_started())
        allow_worker.set()
        assert worker_finished.wait(timeout=0.5)
        assert permit_released.wait(timeout=1.0)

        assert lifecycle.in_flight == 0
        assert lifecycle.retained == 0
        assert blocking_work.active == 0
    finally:
        loop.close()


def test_stopped_loop_factory_result_is_cleaned_before_worker_releases_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    worker_started = threading.Event()
    allow_result = threading.Event()
    cleanup_finished = threading.Event()
    cleanup_threads: list[int] = []
    product = object()

    def realize() -> object:
        worker_started.set()
        assert allow_result.wait(timeout=2.0)
        return product

    def cleanup(result: object) -> None:
        assert result is product
        cleanup_threads.append(threading.get_ident())
        cleanup_finished.set()

    async def submit() -> asyncio.Task[object]:
        task = asyncio.create_task(
            blocking_work.run(
                realize,
                reason_code="stopped_loop_factory_result",
                on_abandoned_result=cleanup,
                result_handoff_seconds=0.05,
            )
        )
        while not worker_started.is_set():
            await asyncio.sleep(0)
        return task

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda *_args: None)
    task = loop.run_until_complete(submit())
    task._log_destroy_pending = False
    allow_result.set()
    try:
        assert cleanup_finished.wait(timeout=0.5)
        assert permit_released.wait(timeout=1.0)
        assert lifecycle.in_flight == 0
        assert cleanup_threads and cleanup_threads != [threading.get_ident()]
    finally:
        loop.close()


@pytest.mark.asyncio
async def test_factory_result_cancelled_after_delivery_is_cleaned_by_worker(monkeypatch) -> None:
    from tacit.pipeline import side_effects as side_effects_module

    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    cleanup_finished = threading.Event()
    cleanup_threads: list[int] = []
    task_holder: list[asyncio.Task[object]] = []
    product = object()
    original_deliver = side_effects_module._LifecycleBlockingCall._deliver

    def deliver_then_cancel(call) -> None:
        original_deliver(call)
        task_holder[0].cancel()

    def cleanup(result: object) -> None:
        assert result is product
        cleanup_threads.append(threading.get_ident())
        cleanup_finished.set()

    monkeypatch.setattr(side_effects_module._LifecycleBlockingCall, "_deliver", deliver_then_cancel)
    async with lifecycle.slot():
        task = asyncio.create_task(
            blocking_work.run(
                lambda: product,
                reason_code="factory_result_delivery_cancelled",
                on_abandoned_result=cleanup,
                result_handoff_seconds=0.1,
            )
        )
        task_holder.append(task)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.to_thread(cleanup_finished.wait, 0.5)

    assert cleanup_threads and cleanup_threads != [threading.get_ident()]
    assert lifecycle.in_flight == 0
    assert blocking_work.active == 0


@pytest.mark.asyncio
async def test_blocking_workers_are_bounded_inside_one_admission_lease() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    started = 0
    started_event = threading.Event()
    started_lock = threading.Lock()
    release = threading.Event()

    def blocking_call() -> None:
        nonlocal started
        with started_lock:
            started += 1
        started_event.set()
        assert release.wait(timeout=2.0)

    entered = [asyncio.Event() for _ in range(8)]

    async def run_call(index: int) -> None:
        entered[index].set()
        await blocking_work.run(
            blocking_call,
            reason_code="same_lease_worker_bound",
        )

    async with lifecycle.slot():
        calls = [asyncio.create_task(run_call(index)) for index in range(8)]
        try:
            await asyncio.gather(*(event.wait() for event in entered))
            assert await asyncio.to_thread(started_event.wait, 1.0)
            assert started == 1
            assert blocking_work.active <= 2
        finally:
            for call in calls[1:]:
                call.cancel()
            release.set()
            await asyncio.gather(*calls, return_exceptions=True)

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert blocking_work.active == 0


@pytest.mark.asyncio
async def test_saturated_blocking_work_is_rejected_before_downstream_execution() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    first_started = threading.Event()
    release_first = threading.Event()
    queued_ran = threading.Event()

    def first_call() -> None:
        first_started.set()
        assert release_first.wait(timeout=2.0)

    async with lifecycle.slot():
        first = asyncio.create_task(blocking_work.run(first_call, reason_code="queued_cancel_first"))
        assert await asyncio.to_thread(first_started.wait, 0.5)
        with pytest.raises(PipelineAdmissionRejected):
            await blocking_work.run(queued_ran.set, reason_code="saturated_second")
        assert blocking_work.active == 1
        release_first.set()
        await first

    assert queued_ran.is_set() is False
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0


@pytest.mark.asyncio
async def test_blocking_thread_start_failure_releases_admission(monkeypatch) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    function_ran = threading.Event()
    original_start = threading.Thread.start

    def fail_blocking_worker_start(thread: threading.Thread) -> None:
        if thread.name == "tacit-lifecycle-blocking-work":
            raise RuntimeError("synthetic thread start failure")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_blocking_worker_start)
    with pytest.raises(RuntimeError, match="blocking worker could not start"):
        await blocking_work.run(function_ran.set, reason_code="thread_start_failed")

    assert function_ran.is_set() is False
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_ambiguous_blocking_thread_start_aborts_before_execution(monkeypatch) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    function_ran = threading.Event()
    started_threads: list[threading.Thread] = []
    original_start = threading.Thread.start

    def start_then_raise(thread: threading.Thread) -> None:
        if thread.name == "tacit-lifecycle-blocking-work":
            started_threads.append(thread)
            original_start(thread)
            raise RuntimeError("synthetic ambiguous thread start failure")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", start_then_raise)
    with pytest.raises(RuntimeError, match="blocking worker could not start"):
        await blocking_work.run(function_ran.set, reason_code="ambiguous_thread_start_failed")

    for thread in started_threads:
        thread.join(timeout=1.0)
    assert function_ran.is_set() is False
    assert all(not thread.is_alive() for thread in started_threads)
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_cleanup_thread_start_failure_returns_ownership_to_caller(monkeypatch) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    cleanup_ran = threading.Event()
    original_start = threading.Thread.start

    def fail_blocking_worker_start(thread: threading.Thread) -> None:
        if thread.name == "tacit-lifecycle-blocking-work":
            raise RuntimeError("synthetic thread start failure")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_blocking_worker_start)
    assert (
        blocking_work.run_background(
            cleanup_ran.set,
            reason_code="cleanup_worker_start_failure",
        )
        is False
    )
    assert cleanup_ran.is_set() is False
    assert blocking_work.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0


def test_paused_origin_loop_preserves_blocking_result_until_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    blocking_work = LifecycleOwnedBlockingWork(lifecycle)
    permit_released = _observe_blocking_permit_release(monkeypatch, lifecycle)
    started = threading.Event()
    release = threading.Event()

    def blocking_call() -> str:
        started.set()
        assert release.wait(timeout=2.0)
        return "completed"

    async def wait_until_started() -> None:
        while not started.is_set():
            await asyncio.sleep(0)

    loop = asyncio.new_event_loop()
    try:
        task = loop.create_task(blocking_work.run(blocking_call, reason_code="paused_origin_loop"))
        loop.run_until_complete(wait_until_started())
        release.set()
        assert permit_released.wait(timeout=1.0)
        assert lifecycle.in_flight == 0
        assert loop.run_until_complete(task) == "completed"
    finally:
        loop.close()
    assert blocking_work.active == 0


@pytest.mark.asyncio
async def test_first_bedrock_chat_rejects_profile_mutation_before_sdk_use(monkeypatch, tmp_path) -> None:
    from types import SimpleNamespace
    from unittest.mock import patch

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_PROFILE", "owner-a")
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[owner-a]\naws_access_key_id = AKIAOWNERA\naws_secret_access_key = owner-a-secret\n")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    credentials = MagicMock(method="shared-credentials-file")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="AKIAOWNERA",
        secret_key="owner-a-secret",
        token="owner-a-token",
    )
    discovery_session = MagicMock()
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [discovery_session, pinned_session]

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        dependencies = build_pipeline_dependencies(
            runtime_settings,
            stores=RuntimeStores(runtime_settings),
        )
        assert dependencies.pipeline_admission is not None
        assert dependencies.llm_provider_factory is not None

        async with dependencies.pipeline_admission.slot():
            await dependencies.acquire_resources()
            provider = cast(BedrockProvider, dependencies.llm_provider_factory())
            monkeypatch.setenv("AWS_PROFILE", "owner-b")
            monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAOWNERB")
            monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "owner-b-secret")
            with pytest.raises(RuntimeOwnershipError, match="credential selector changed"):
                await provider.chat_text("system", "user")
            await dependencies.close_resources()

    mock_boto3.Session.assert_not_called()
    credentials.get_frozen_credentials.assert_not_called()


@pytest.mark.asyncio
async def test_first_bedrock_chat_rejects_environment_credentials_added_after_plan_capture(
    monkeypatch,
    tmp_path,
) -> None:
    from unittest.mock import patch

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[default]\naws_access_key_id = AKIACAPTURED\naws_secret_access_key = captured-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    mock_boto3 = MagicMock()

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        dependencies = build_pipeline_dependencies(
            runtime_settings,
            stores=RuntimeStores(runtime_settings),
        )
        assert dependencies.pipeline_admission is not None

        async with dependencies.pipeline_admission.slot():
            await dependencies.acquire_resources()
            assert dependencies.llm_provider_factory is not None
            provider = cast(BedrockProvider, dependencies.llm_provider_factory())
            monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIALATE")
            monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "late-secret")
            with pytest.raises(RuntimeOwnershipError, match="credential selector changed"):
                await provider.chat_text("system", "user")
            await dependencies.close_resources()

    mock_boto3.Session.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("path_variable", ["AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE"])
async def test_first_bedrock_chat_rejects_credential_source_path_changes_before_sdk_use(
    monkeypatch,
    tmp_path,
    path_variable: str,
) -> None:
    from unittest.mock import patch

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    first_credentials = tmp_path / "credentials-a"
    second_credentials = tmp_path / "credentials-b"
    first_config = tmp_path / "config-a"
    second_config = tmp_path / "config-b"
    first_credentials.write_text("[default]\naws_access_key_id = AKIAFIRST\naws_secret_access_key = first\n")
    second_credentials.write_text("[default]\naws_access_key_id = AKIASECOND\naws_secret_access_key = second\n")
    first_config.write_text("[default]\nregion = us-east-1\n")
    second_config.write_text("[default]\nregion = us-west-2\n")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(first_credentials))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(first_config))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    mock_boto3 = MagicMock()

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        dependencies = build_pipeline_dependencies(
            runtime_settings,
            stores=RuntimeStores(runtime_settings),
        )
        assert dependencies.pipeline_admission is not None

        async with dependencies.pipeline_admission.slot():
            await dependencies.acquire_resources()
            assert dependencies.llm_provider_factory is not None
            provider = cast(BedrockProvider, dependencies.llm_provider_factory())
            replacement = second_credentials if path_variable == "AWS_SHARED_CREDENTIALS_FILE" else second_config
            monkeypatch.setenv(path_variable, str(replacement))
            with pytest.raises(RuntimeOwnershipError, match="credential selector changed"):
                await provider.chat_text("system", "user")
            await dependencies.close_resources()

    mock_boto3.Session.assert_not_called()


def test_bedrock_resolution_cannot_adopt_environment_keys_after_final_plan_check(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.agents.providers.bedrock import _build_boto3_session
    from tacit.runtime_ownership import BedrockCredentialPlan

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[default]\naws_access_key_id = AKIACAPTURED\naws_secret_access_key = captured-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    original_verify = BedrockCredentialPlan.verify_unchanged
    mutated = False

    def verify_then_mutate_environment(self: BedrockCredentialPlan) -> None:
        nonlocal mutated
        original_verify(self)
        if not mutated:
            monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIALATE")
            monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "late-secret")
            mutated = True

    monkeypatch.setattr(BedrockCredentialPlan, "verify_unchanged", verify_then_mutate_environment)

    resolved = _build_boto3_session(credential_plan=plan)

    assert resolved.credential_identity.credential_fingerprint == credential_fingerprint(
        "AKIACAPTURED\0captured-secret\0"
    )


def test_bedrock_resolution_uses_captured_file_contents_after_final_plan_check(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.agents.providers.bedrock import _build_boto3_session
    from tacit.runtime_ownership import BedrockCredentialPlan

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    credentials_path.write_text(
        "[default]\naws_access_key_id = AKIACAPTURED\naws_secret_access_key = captured-secret\n"
    )
    config_path.write_text("[default]\nregion = us-east-1\n")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    original_verify = BedrockCredentialPlan.verify_unchanged
    mutated = False

    def verify_then_replace_file(self: BedrockCredentialPlan) -> None:
        nonlocal mutated
        original_verify(self)
        if not mutated:
            credentials_path.write_text(
                "[default]\naws_access_key_id = AKIAMUTATED\naws_secret_access_key = mutated-secret\n"
            )
            mutated = True

    monkeypatch.setattr(BedrockCredentialPlan, "verify_unchanged", verify_then_replace_file)

    resolved = _build_boto3_session(credential_plan=plan)

    assert resolved.credential_identity.credential_fingerprint == credential_fingerprint(
        "AKIACAPTURED\0captured-secret\0"
    )


def test_bedrock_web_identity_rejects_token_change_before_sdk_use(
    monkeypatch,
    tmp_path,
) -> None:
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session
    from tacit.runtime_ownership import BedrockCredentialPlan

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
    ):
        monkeypatch.delenv(name, raising=False)
    token_path = tmp_path / "web-identity-token"
    token_path.write_text("captured-token")
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/TacitRuntime")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    token_path.write_text("changed-token")
    mock_boto3 = MagicMock()

    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        pytest.raises(RuntimeOwnershipError, match="credential source changed"),
    ):
        _build_boto3_session(credential_plan=plan)

    mock_boto3.Session.assert_not_called()


def test_bedrock_web_identity_operation_uses_captured_selector_after_final_check(
    monkeypatch,
    tmp_path,
) -> None:
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
    ):
        monkeypatch.delenv(name, raising=False)
    token_path = tmp_path / "web-identity-token"
    token_path.write_text("captured-token")
    ambient_token_path = tmp_path / "ambient-token"
    ambient_token_path.write_text("ambient-token")
    role_arn = "arn:aws:iam::123456789012:role/TacitRuntime"
    monkeypatch.setenv("AWS_ROLE_ARN", role_arn)
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    original_verify = BedrockCredentialPlan.verify_unchanged
    sts_client = MagicMock()
    sts_client.assume_role_with_web_identity.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAWEBIDENTITY",
            "SecretAccessKey": "web-identity-secret",
            "SessionToken": "web-identity-session-token",
        }
    }
    unsigned_session = MagicMock()
    unsigned_session.client.return_value = sts_client
    pinned_session = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [unsigned_session, pinned_session]

    def verify_then_mutate_ambient_selector(self: BedrockCredentialPlan) -> None:
        original_verify(self)
        token_path.write_text("changed-token")
        monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::999999999999:role/Ambient")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(ambient_token_path))

    monkeypatch.setattr(BedrockCredentialPlan, "verify_unchanged", verify_then_mutate_ambient_selector)

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(credential_plan=plan)

    sts_client.assume_role_with_web_identity.assert_called_once_with(
        RoleArn=role_arn,
        RoleSessionName="tacit-bedrock",
        WebIdentityToken="captured-token",
        DurationSeconds=3600,
    )
    assert unsigned_session.client.call_args.kwargs["verify"] is True
    assert resolved.session is pinned_session
    assert resolved.credential_identity.account == role_arn.casefold()


def test_bedrock_role_operation_uses_captured_source_and_role_precedence(
    monkeypatch,
    tmp_path,
) -> None:
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[selected]\n"
        "role_arn = arn:aws:iam::111111111111:role/CredentialsRole\n"
        "source_profile = base\n"
        "[base]\n"
        "aws_access_key_id = AKIABASE\n"
        "aws_secret_access_key = base-secret\n"
    )
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile selected]\nrole_arn = arn:aws:iam::222222222222:role/ConfigRole\nsource_profile = base\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "selected")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    sts_client = MagicMock()
    sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAROLE",
            "SecretAccessKey": "role-secret",
            "SessionToken": "role-token",
        }
    }
    source_session = MagicMock()
    source_session.client.return_value = sts_client
    pinned_session = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [source_session, pinned_session]

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(credential_plan=plan)

    first_session_kwargs = mock_boto3.Session.call_args_list[0].kwargs
    assert first_session_kwargs["aws_access_key_id"] == "AKIABASE"
    assert first_session_kwargs["aws_secret_access_key"] == "base-secret"
    sts_client.assume_role.assert_called_once_with(
        RoleArn="arn:aws:iam::111111111111:role/CredentialsRole",
        RoleSessionName="tacit-bedrock",
        DurationSeconds=3600,
    )
    assert source_session.client.call_args.kwargs["verify"] is True
    assert resolved.session is pinned_session
    assert resolved.credential_identity.account == plan.account


def test_bedrock_named_static_profile_yields_to_ambient_web_identity_provider(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    token_path = tmp_path / "ambient-token"
    token_path.write_text("ambient-token")
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[named]\naws_access_key_id = AKIAPROFILE\naws_secret_access_key = profile-secret\n")
    monkeypatch.setenv("AWS_PROFILE", "named")
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::222222222222:role/AmbientRole")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    plan = BedrockCredentialPlan.capture(
        _settings(
            tmp_path,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
        )
    )

    assert plan.profile == "named"
    assert plan.discovery_methods == ("assume-role-with-web-identity",)
    assert plan.account == "arn:aws:iam::222222222222:role/ambientrole"


def test_bedrock_default_assume_role_profile_precedes_ambient_web_identity(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    token_path = tmp_path / "ambient-token"
    token_path.write_text("ambient-token")
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n")
    config_path = tmp_path / "config"
    config_path.write_text("[default]\nrole_arn = arn:aws:iam::111111111111:role/ProfileRole\nsource_profile = base\n")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::222222222222:role/AmbientRole")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    plan = BedrockCredentialPlan.capture(
        _settings(
            tmp_path,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
        )
    )

    assert plan.discovery_methods == ("assume-role",)
    assert plan.account == "arn:aws:iam::111111111111:role/profilerole"


def test_bedrock_environment_profile_precedence_matches_botocore(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[from-default]\n"
        "aws_access_key_id = AKIADEFAULT\n"
        "aws_secret_access_key = default-secret\n"
        "[from-profile]\n"
        "aws_access_key_id = AKIAPROFILE\n"
        "aws_secret_access_key = profile-secret\n"
    )
    monkeypatch.setenv("AWS_DEFAULT_PROFILE", "from-default")
    monkeypatch.setenv("AWS_PROFILE", "from-profile")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    plan = BedrockCredentialPlan.capture(
        _settings(
            tmp_path,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
        )
    )

    assert plan.profile == "from-default"


def test_bedrock_environment_session_token_precedence_matches_botocore(
    monkeypatch,
    tmp_path,
) -> None:
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENVIRONMENT")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "environment-secret")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "security-token")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "session-token")
    mock_boto3 = MagicMock()
    pinned_session = MagicMock()
    mock_boto3.Session.return_value = pinned_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )

    mock_boto3.Session.assert_called_once_with(
        region_name="us-east-1",
        aws_access_key_id="AKIAENVIRONMENT",
        aws_secret_access_key="environment-secret",
        aws_session_token="security-token",
    )
    assert resolved.session is pinned_session


def test_bedrock_ignores_non_botocore_environment_key_aliases(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[default]\naws_access_key_id = AKIAFILE\naws_secret_access_key = file-secret\n")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY", "AKIAALIAS")
    monkeypatch.setenv("AWS_SECRET_KEY", "alias-secret")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    plan = BedrockCredentialPlan.capture(
        _settings(
            tmp_path,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
        )
    )

    assert plan.discovery_methods == ("shared-credentials-file",)


def test_bedrock_rejects_blank_ambient_web_identity_session_name(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    token_path = tmp_path / "ambient-token"
    token_path.write_text("ambient-token")
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::222222222222:role/AmbientRole")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_ROLE_SESSION_NAME", "")

    with pytest.raises(RuntimeOwnershipError, match="AWS credential environment value is invalid"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_explicit_profile_files_do_not_require_home(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[default]\naws_access_key_id = AKIAFILE\naws_secret_access_key = file-secret\n")
    monkeypatch.delenv("HOME", raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    plan = BedrockCredentialPlan.capture(
        _settings(
            tmp_path,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
        )
    )

    assert plan.discovery_methods == ("shared-credentials-file",)


def test_bedrock_rejects_blank_default_profile_before_profile_fallback(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[fallback]\naws_access_key_id = AKIAFALLBACK\naws_secret_access_key = fallback-secret\n"
    )
    monkeypatch.setenv("AWS_DEFAULT_PROFILE", "")
    monkeypatch.setenv("AWS_PROFILE", "fallback")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    with pytest.raises(RuntimeOwnershipError, match="AWS credential environment value is invalid"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_rejects_blank_credentials_path_before_home_fallback(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    aws_home = tmp_path / ".aws"
    aws_home.mkdir()
    (aws_home / "credentials").write_text(
        "[default]\naws_access_key_id = AKIAFALLBACK\naws_secret_access_key = fallback-secret\n"
    )
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    with pytest.raises(RuntimeOwnershipError, match="AWS credential environment value is invalid"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


@pytest.mark.parametrize(
    ("name", "value", "extra"),
    [
        ("AWS_ACCESS_KEY_ID", " AKIAENVIRONMENT", {"AWS_SECRET_ACCESS_KEY": "environment-secret"}),
        (
            "AWS_SECURITY_TOKEN",
            " ",
            {
                "AWS_ACCESS_KEY_ID": "AKIAENVIRONMENT",
                "AWS_SECRET_ACCESS_KEY": "environment-secret",
                "AWS_SESSION_TOKEN": "session-token",
            },
        ),
        (
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            " /var/run/secrets/aws/token",
            {"AWS_ROLE_ARN": "arn:aws:iam::222222222222:role/AmbientRole"},
        ),
    ],
    ids=("access-key", "security-token", "web-token-path"),
)
def test_bedrock_rejects_padded_credential_environment_values(
    monkeypatch,
    tmp_path,
    name: str,
    value: str,
    extra: dict[str, str],
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    monkeypatch.setenv(name, value)
    for extra_name, extra_value in extra.items():
        monkeypatch.setenv(extra_name, extra_value)

    with pytest.raises(RuntimeOwnershipError, match="AWS credential environment value is invalid"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_web_identity_rejects_blank_unmodeled_provider_selector(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    token_path = tmp_path / "profile-token"
    token_path.write_text("profile-token")
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[workload]\n"
        "role_arn = arn:aws:iam::111111111111:role/WorkloadRole\n"
        f"web_identity_token_file = {token_path}\n"
        "credential_process =\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "workload")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    with pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


@pytest.mark.parametrize(
    "profile_lines",
    [
        "role_arn =\naws_access_key_id = AKIASTATIC\naws_secret_access_key = static-secret\n",
        (
            "role_arn = arn:aws:iam::111111111111:role/WorkloadRole\n"
            "web_identity_token_file =\n"
            "aws_access_key_id = AKIASTATIC\n"
            "aws_secret_access_key = static-secret\n"
        ),
    ],
    ids=("blank-role", "blank-web-token"),
)
def test_bedrock_rejects_blank_modeled_provider_selector(
    monkeypatch,
    tmp_path,
    profile_lines: str,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(f"[default]\n{profile_lines}")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)

    with pytest.raises(RuntimeOwnershipError, match="AWS Bedrock|AWS web identity"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_rejects_ambient_web_identity_token_without_role(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    token_path = tmp_path / "ambient-token"
    token_path.write_text("ambient-token")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.delenv("AWS_ROLE_ARN", raising=False)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)

    with pytest.raises(RuntimeOwnershipError, match="requires a role ARN"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


@pytest.mark.parametrize("as_role_source", [False, True], ids=("default", "role-source"))
def test_bedrock_rejects_static_credentials_split_across_provider_files(
    monkeypatch,
    tmp_path,
    as_role_source: bool,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    if as_role_source:
        credentials_path.write_text(
            "[selected]\n"
            "role_arn = arn:aws:iam::111111111111:role/SelectedRole\n"
            "source_profile = source\n"
            "[source]\n"
            "aws_access_key_id = AKIAPARTIAL\n"
        )
        config_path.write_text(
            "[profile source]\naws_access_key_id = AKIACONFIG\naws_secret_access_key = config-secret\n"
        )
        monkeypatch.setenv("AWS_PROFILE", "selected")
    else:
        credentials_path.write_text("[default]\naws_access_key_id = AKIAPARTIAL\n")
        config_path.write_text("[default]\naws_access_key_id = AKIACONFIG\naws_secret_access_key = config-secret\n")
        monkeypatch.delenv("AWS_PROFILE", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    with pytest.raises(RuntimeOwnershipError, match="credentials must include both"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_rejects_credential_process_before_execution(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    sentinel = tmp_path / "credential-process-ran"
    process = tmp_path / "credential-process"
    process.write_text(f"#!/bin/sh\ntouch {sentinel}\n")
    process.chmod(0o700)
    config_path = tmp_path / "config"
    config_path.write_text(f"[default]\ncredential_process = {process}\n")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "missing-credentials"))
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )

    assert sentinel.exists() is False


def test_bedrock_rejects_chained_roles_before_sdk_use(monkeypatch, tmp_path) -> None:
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n")
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile first-role]\nrole_arn = arn:aws:iam::111111111111:role/FirstRole\nsource_profile = base\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "first-role")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    mock_boto3 = MagicMock()

    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        pytest.raises(RuntimeOwnershipError, match="chained role assumption is unsupported"),
    ):
        _build_boto3_session(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
                llm_bedrock_role_arn="arn:aws:iam::222222222222:role/FinalRole",
            )
        )

    mock_boto3.Session.assert_not_called()


def test_bedrock_rejects_mfa_role_profile_before_prompting(monkeypatch, tmp_path) -> None:
    from unittest.mock import patch

    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text("[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n")
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile mfa-role]\n"
        "role_arn = arn:aws:iam::111111111111:role/MfaRole\n"
        "source_profile = base\n"
        "mfa_serial = arn:aws:iam::111111111111:mfa/operator\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "mfa-role")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    with (
        patch("getpass.getpass") as prompt,
        pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"),
    ):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )

    prompt.assert_not_called()


def test_bedrock_rejects_blank_mfa_field_that_overrides_config_profile(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[selected]\nmfa_serial =\n[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n"
    )
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile selected]\n"
        "role_arn = arn:aws:iam::111111111111:role/MfaRole\n"
        "source_profile = base\n"
        "mfa_serial = arn:aws:iam::111111111111:mfa/operator\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "selected")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    with pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_rejects_blank_process_field_in_role_source_profile(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[selected]\n"
        "role_arn = arn:aws:iam::111111111111:role/SelectedRole\n"
        "source_profile = source\n"
        "[source]\n"
        "credential_process =\n"
    )
    config_path = tmp_path / "config"
    config_path.write_text("[profile source]\naws_access_key_id = AKIASOURCE\naws_secret_access_key = source-secret\n")
    monkeypatch.setenv("AWS_PROFILE", "selected")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    with pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )


def test_bedrock_rejects_credentials_file_source_profile_override_before_process_execution(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    sentinel = tmp_path / "unsafe-source-ran"
    process = tmp_path / "unsafe-source"
    process.write_text(f"#!/bin/sh\ntouch {sentinel}\n")
    process.chmod(0o700)
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[selected]\n"
        "role_arn = arn:aws:iam::111111111111:role/SelectedRole\n"
        "source_profile = unsafe\n"
        "[safe]\n"
        "aws_access_key_id = AKIASAFE\n"
        "aws_secret_access_key = safe-secret\n"
        "[unsafe]\n"
        f"credential_process = {process}\n"
    )
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile selected]\nrole_arn = arn:aws:iam::222222222222:role/ConfigRole\nsource_profile = safe\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "selected")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    with pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"):
        BedrockCredentialPlan.capture(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )

    assert sentinel.exists() is False


@pytest.mark.parametrize(
    "ambient_values",
    [
        {"AWS_EC2_METADATA_SERVICE_ENDPOINT": "http://127.0.0.1:45678"},
        {"AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://127.0.0.1:45678/credentials"},
    ],
    ids=("instance-metadata", "container-metadata"),
)
def test_bedrock_rejects_unmodeled_remote_credential_provider_before_sdk_use(
    monkeypatch,
    tmp_path,
    ambient_values: dict[str, str],
) -> None:
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    for name, value in ambient_values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "missing-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    mock_boto3 = MagicMock()

    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        pytest.raises(RuntimeOwnershipError, match="credential provider is unsupported"),
    ):
        _build_boto3_session(
            _settings(
                tmp_path,
                llm_provider="bedrock",
                llm_bedrock_region="us-east-1",
            )
        )

    mock_boto3.Session.assert_not_called()


@pytest.mark.parametrize("profile_name", ["role-owner", ""])
def test_role_profile_sts_remote_is_declared_from_captured_metadata_before_resolution(
    monkeypatch,
    tmp_path,
    profile_name: str,
) -> None:
    from unittest.mock import patch

    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    credentials_path.write_text("[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n")
    section = f"profile {profile_name}" if profile_name else "default"
    config_path.write_text(
        f"[{section}]\nrole_arn = arn:aws:iam::123456789012:role/TacitRuntime\nsource_profile = base\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    if profile_name:
        monkeypatch.setenv("AWS_PROFILE", profile_name)
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    mock_boto3 = MagicMock()

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        dependencies = build_pipeline_dependencies(
            runtime_settings,
            stores=RuntimeStores(runtime_settings),
        )

    assert dependencies.llm_provider_factory is not None
    remotes = {remote.provider: remote for remote in dependencies.llm_provider_factory.runtime_ownership.remotes}
    assert set(remotes) == {"llm:bedrock", "llm:bedrock:sts"}
    assert remotes["llm:bedrock"].account == "arn:aws:iam::123456789012:role/tacitruntime"
    assert remotes["llm:bedrock:sts"].account == remotes["llm:bedrock"].account
    mock_boto3.Session.assert_not_called()


def test_declared_non_bedrock_provider_preserves_direct_compatibility(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
    )
    provider = _ProviderProbe(runtime_settings)
    calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal calls
        calls += 1
        return provider

    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="direct_local_provider_factory",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
    )
    assert dependencies.llm_provider_factory is not None

    assert dependencies.llm_provider_factory() is provider
    assert dependencies.llm_provider_factory() is provider
    assert calls == 1
    asyncio.run(dependencies.close_resources())
    assert provider.closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_direct_provider_operations_share_aggregate_admission_limit(
    tmp_path,
    capability: str,
) -> None:
    first_release = _CrossLoopAsyncGate()
    first_started = threading.Event()
    second_started = threading.Event()
    active = 0
    max_active = 0
    calls = 0
    active_lock = threading.Lock()

    async def admitted_operation() -> None:
        nonlocal active, calls, max_active
        with active_lock:
            calls += 1
            call = calls
            active += 1
            max_active = max(max_active, active)
        (first_started if call == 1 else second_started).set()
        try:
            if call == 1:
                await first_release.wait()
        finally:
            with active_lock:
                active -= 1

    class DirectLLM(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            await admitted_operation()
            return LLMResult("ok")

    class DirectContext(_ContextProbe):
        async def query(self, *_args, **_kwargs) -> list[Any]:
            await admitted_operation()
            return []

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: DirectLLM(settings),
        context_factory=(lambda settings: DirectContext(settings)) if capability == "context" else None,
        max_concurrent=1,
    )
    product = resources.llm() if capability == "llm" else resources.context()
    assert product is not None

    async def invoke() -> Any:
        if capability == "llm":
            return await product.chat_text("system", "user")
        return await product.query(SimpleNamespace(), max_chunks=1)

    first = asyncio.create_task(invoke())
    assert await asyncio.to_thread(first_started.wait, 1.0)
    second = asyncio.create_task(invoke())
    queued = False
    second_started_while_first_active = False
    try:
        for _ in range(1_000):
            if lifecycle.queued == 1:
                queued = True
                break
            if second_started.is_set():
                second_started_while_first_active = True
                break
            await asyncio.sleep(0)
    finally:
        first_release.release()
        await asyncio.gather(first, second, return_exceptions=True)
        await resources.close()

    assert queued is True
    assert second_started_while_first_active is False
    assert max_active == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_cancelled_queued_direct_call_never_invokes_provider(tmp_path) -> None:
    first_release = _CrossLoopAsyncGate()
    first_started = threading.Event()
    second_started = threading.Event()
    calls = 0

    class QueuedProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                await first_release.wait()
            else:
                second_started.set()
            return LLMResult("ok")

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: QueuedProvider(settings),
        max_concurrent=1,
    )
    provider = resources.llm()
    first = asyncio.create_task(provider.chat_text("system", "first"))
    assert await asyncio.to_thread(first_started.wait, 1.0)
    queued = asyncio.create_task(provider.chat_text("system", "queued"))
    await _wait_for_admission_queue(lifecycle, 1)

    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert lifecycle.queued == 0

    first_release.release()
    await first
    await resources.close()

    assert calls == 1
    assert second_started.is_set() is False
    assert lifecycle.queued == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_cancelled_direct_call_before_owner_task_start_releases_operation_on_owner_settlement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    owner_blocked = threading.Event()
    release_owner = threading.Event()
    provider_invoked = threading.Event()

    class UnstartedProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            provider_invoked.set()
            return LLMResult("unexpected")

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: UnstartedProvider(settings),
        max_concurrent=1,
    )
    provider = resources.llm()
    state = resources._generation_owner
    assert state is not None and state.loop is not None

    def block_owner_loop() -> None:
        owner_blocked.set()
        assert release_owner.wait(timeout=2.0)

    state.loop.call_soon_threadsafe(block_owner_loop)
    assert await asyncio.to_thread(owner_blocked.wait, 1.0)

    operation_settled = threading.Event()
    original_finish_operation = resources._finish_generation_operation

    def observe_finish_operation(*args: Any, **kwargs: Any) -> None:
        original_finish_operation(*args, **kwargs)
        operation_settled.set()

    monkeypatch.setattr(resources, "_finish_generation_operation", observe_finish_operation)

    operation = asyncio.create_task(provider.chat_text("system", "queued-on-owner"))
    for _ in range(1_000):
        with resources._lock:
            if state.active_operations:
                break
        await asyncio.sleep(0)
    with resources._lock:
        assert state.active_operations

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    release_owner.set()
    assert await asyncio.to_thread(operation_settled.wait, 1.0)

    leaked_operation_ids: tuple[int, ...] = ()
    try:
        with resources._lock:
            leaked_operation_ids = tuple(state.active_operations)
        assert leaked_operation_ids == ()
        assert provider_invoked.is_set() is False
    finally:
        for operation_id in leaked_operation_ids:
            resources._finish_generation_operation(state, operation_id)
        await resources.close()

    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
async def test_cancelled_direct_provider_call_releases_admission_only_after_owner_settles(tmp_path) -> None:
    operation_gate = _CrossLoopAsyncGate()
    operation_finished = threading.Event()

    class DirectProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            try:
                await operation_gate.wait()
                return LLMResult("ok")
            finally:
                operation_finished.set()

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: DirectProvider(settings),
        max_concurrent=1,
    )
    provider = resources.llm()
    operation = asyncio.create_task(provider.chat_text("system", "user"))
    assert await asyncio.to_thread(operation_gate.started.wait, 1.0)

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    operation_was_still_running = operation_finished.is_set() is False
    admission_was_retained = lifecycle.in_flight == 1
    operation_gate.release()
    try:
        assert await asyncio.to_thread(operation_finished.wait, 1.0)
    finally:
        await resources.close()
    assert operation_was_still_running is True
    assert admission_was_retained is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
async def test_failed_direct_provider_call_releases_aggregate_admission(tmp_path) -> None:
    class FailingProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            raise ValueError("synthetic direct provider failure")

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: FailingProvider(settings),
        max_concurrent=1,
    )

    with pytest.raises(ValueError, match="synthetic direct provider failure"):
        await resources.llm().chat_text("system", "user")

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    await resources.close()
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_cancelled_provider_adoption_caches_or_retires_completed_product_once(
    monkeypatch,
    tmp_path,
    capability: str,
) -> None:
    closed = threading.Event()
    products: list[Any] = []
    selected_llm_factory: Callable[[Settings], LLMProvider]
    selected_context_factory: Callable[[Settings], ContextProvider] | None

    if capability == "llm":

        class CountingLLMProduct(_ProviderProbe):
            def __init__(self, runtime_settings: Settings) -> None:
                super().__init__(runtime_settings)
                self.close_calls = 0

            async def close(self) -> None:
                self.close_calls += 1
                await super().close()
                closed.set()

        def counting_llm_factory(runtime_settings: Settings) -> LLMProvider:
            product = CountingLLMProduct(runtime_settings)
            products.append(product)
            return product

        selected_llm_factory = counting_llm_factory
        selected_context_factory = None
    else:

        class CountingContextProduct(_ContextProbe):
            def __init__(self, runtime_settings: Settings) -> None:
                super().__init__(runtime_settings)
                self.close_calls = 0

            async def close(self) -> None:
                self.close_calls += 1
                await super().close()
                closed.set()

        def supporting_llm_factory(runtime_settings: Settings) -> LLMProvider:
            return _ProviderProbe(runtime_settings)

        def counting_context_factory(runtime_settings: Settings) -> ContextProvider:
            product = CountingContextProduct(runtime_settings)
            products.append(product)
            return product

        selected_llm_factory = supporting_llm_factory
        selected_context_factory = counting_context_factory

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=selected_llm_factory,
        context_factory=selected_context_factory,
    )
    acquire_task: asyncio.Task[Any] | None = None
    requester_loop = asyncio.get_running_loop()
    original_adopt = getattr(resources, f"_adopt_{capability}_provider")

    async def cancel_after_adoption(product: Any, **kwargs: Any) -> None:
        assert acquire_task is not None
        await original_adopt(product, **kwargs)
        requester_loop.call_soon_threadsafe(acquire_task.cancel)

    monkeypatch.setattr(resources, f"_adopt_{capability}_provider", cancel_after_adoption)
    acquire_task = asyncio.create_task(resources.acquire())

    with pytest.raises(asyncio.CancelledError):
        await acquire_task

    assert await asyncio.to_thread(closed.wait, 1.0)
    assert len(products) == 1
    assert products[0].close_calls == 1
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
async def test_cancelled_provider_close_trips_runtime_fatal_circuit_after_grace(tmp_path) -> None:
    close_gate = _CrossLoopAsyncGate()
    providers: list[_ProviderProbe] = []

    class BlockingCloseProvider(_ProviderProbe):
        def __init__(self, runtime_settings: Settings, *, block: bool) -> None:
            super().__init__(runtime_settings)
            self.block = block
            self.close_calls = 0
            self.closed_event = asyncio.Event()

        async def close(self) -> None:
            self.close_calls += 1
            if self.block:
                await close_gate.wait()
            await super().close()
            self.closed_event.set()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = BlockingCloseProvider(runtime_settings, block=not providers)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        cleanup_grace_seconds=0.05,
    )

    provider_acquired = asyncio.Event()
    acquired_providers: list[LLMProvider] = []

    async def acquire_and_close() -> None:
        await resources.acquire()
        acquired_providers.append(resources.llm())
        assert acquired_providers[0] is providers[0]
        provider_acquired.set()
        await resources.close()

    close_task = asyncio.create_task(acquire_and_close())
    await provider_acquired.wait()
    first_provider = acquired_providers[0]
    first_reference = weakref.ref(first_provider)
    assert await asyncio.to_thread(close_gate.started.wait, 1.0)
    charged_before_cancel = (
        lifecycle.in_flight,
        lifecycle.retained,
        lifecycle.blocking_in_flight,
        lifecycle.service_owner_in_flight,
    )
    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task
    charged_after_cancel = (
        lifecycle.in_flight,
        lifecycle.retained,
        lifecycle.blocking_in_flight,
        lifecycle.service_owner_in_flight,
    )
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.acquire()
    assert len(providers) == 1

    assert charged_before_cancel == (0, 0, 0, 1)
    assert charged_after_cancel == (0, 0, 0, 1)
    assert providers[0].closed is False
    assert cast(BlockingCloseProvider, providers[0]).close_calls == 1
    with pytest.raises(RuntimeOwnershipError, match="generation is no longer active"):
        await first_provider.chat_text("system", "user")
    _assert_terminal_provider_cleanup(resources, lifecycle)
    providers.clear()
    acquired_providers.clear()
    del close_task, first_provider
    _assert_collected(first_reference)


@pytest.mark.asyncio
async def test_child_self_cancellation_does_not_release_provider_generation_early(tmp_path) -> None:
    llm_close_gate = _CrossLoopAsyncGate()
    llm_closed = threading.Event()
    context_close_attempted = threading.Event()

    class BlockingLLM(_ProviderProbe):
        async def close(self) -> None:
            await llm_close_gate.wait()
            await super().close()
            llm_closed.set()

    class SelfCancellingContext(_ContextProbe):
        def __init__(self, runtime_settings: Settings) -> None:
            super().__init__(runtime_settings)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            context_close_attempted.set()
            if self.close_calls == 1:
                raise asyncio.CancelledError
            await super().close()

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: BlockingLLM(settings),
        context_factory=lambda settings: SelfCancellingContext(settings),
        cleanup_grace_seconds=0.01,
    )
    acquired = asyncio.Event()
    context_providers: list[SelfCancellingContext] = []

    async def acquire_and_close() -> None:
        await resources.acquire()
        context_providers.append(cast(SelfCancellingContext, resources.context()))
        acquired.set()
        await resources.close()

    close_task = asyncio.create_task(acquire_and_close())
    await acquired.wait()
    context_provider = context_providers[0]
    assert await asyncio.to_thread(llm_close_gate.started.wait, 1.0)
    assert await asyncio.to_thread(context_close_attempted.wait, 1.0)
    llm_close_gate.release()
    await close_task
    assert llm_closed.is_set() is True
    assert await asyncio.to_thread(llm_closed.wait, 1.0)
    assert context_provider.close_calls == 2
    assert context_provider.closed is True
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
async def test_outer_cleanup_cancellation_is_not_retried_as_child_cancellation(tmp_path) -> None:
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_calls = 0

    async def cleanup() -> None:
        nonlocal cleanup_calls
        cleanup_calls += 1
        cleanup_started.set()
        await release_cleanup.wait()

    resources, _lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: _ProviderProbe(settings),
    )
    cleanup_task = asyncio.create_task(resources._settle_generation_cleanup("llm", cleanup))
    await cleanup_started.wait()

    cleanup_task.cancel()
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await cleanup_task

    assert cleanup_calls == 1


@pytest.mark.asyncio
async def test_self_cancelled_provider_cleanup_retries_once_then_trips_runtime_fatal(tmp_path) -> None:
    close_attempted = threading.Event()
    factory_calls = 0
    close_calls = 0

    class SelfCancellingProvider(_ProviderProbe):
        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            close_attempted.set()
            raise asyncio.CancelledError

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        return SelfCancellingProvider(runtime_settings)

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        cleanup_grace_seconds=1.0,
    )

    await resources.acquire()
    provider = resources.llm()
    provider_reference = weakref.ref(provider)
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.close()

    assert close_attempted.is_set()
    assert close_calls == 2
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.acquire()
    assert factory_calls == 1
    with pytest.raises(RuntimeOwnershipError, match="generation is no longer active"):
        await provider.chat_text("system", "user")
    _assert_terminal_provider_cleanup(resources, lifecycle)
    del provider
    _assert_collected(provider_reference)


@pytest.mark.asyncio
async def test_lifecycle_owner_start_failure_rolls_back_before_factory_realization(
    monkeypatch,
    tmp_path,
) -> None:
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )
    original_start = threading.Thread.start

    def fail_owner_start(thread: threading.Thread) -> None:
        if thread.name == "tacit-lifecycle-provider-owner":
            raise RuntimeError("synthetic accepted-provider lifecycle start failure")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_owner_start)
    with pytest.raises(RuntimeError, match="lifecycle worker could not start"):
        await resources.acquire()

    assert providers == []
    await _assert_dependency_lifecycle_idle(lifecycle)

    monkeypatch.setattr(threading.Thread, "start", original_start)
    await resources.acquire()
    provider = providers[0]
    await resources.close()
    assert provider.closed is True
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
async def test_ambiguous_lifecycle_owner_start_rolls_back_and_exits_thread(
    monkeypatch,
    tmp_path,
) -> None:
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )
    original_start = threading.Thread.start
    started_threads: list[threading.Thread] = []

    def start_then_fail(thread: threading.Thread) -> None:
        if thread.name != "tacit-lifecycle-provider-owner":
            original_start(thread)
            return
        started_threads.append(thread)
        original_start(thread)
        raise RuntimeError("synthetic ambiguous lifecycle start failure")

    monkeypatch.setattr(threading.Thread, "start", start_then_fail)
    with pytest.raises(RuntimeError, match="lifecycle worker could not start"):
        await resources.acquire()

    assert providers == []
    assert len(started_threads) == 1
    started_threads[0].join(timeout=1.0)
    assert started_threads[0].is_alive() is False
    await _assert_dependency_lifecycle_idle(lifecycle)


def test_stopped_request_loop_does_not_own_accepted_provider_cleanup(tmp_path) -> None:
    cleanup_gate = _CrossLoopAsyncGate()
    cleanup_finished = threading.Event()
    caller_finished = threading.Event()
    caller_errors: list[BaseException] = []
    close_threads: list[int] = []

    class WorkerClosedProvider(_ProviderProbe):
        async def close(self) -> None:
            close_threads.append(threading.get_ident())
            await cleanup_gate.wait()
            await super().close()
            cleanup_finished.set()

    providers: list[WorkerClosedProvider] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = WorkerClosedProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _unused_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        cleanup_grace_seconds=0.01,
    )
    caller_thread_id: list[int] = []

    def use_and_stop_loop() -> None:
        caller_thread_id.append(threading.get_ident())

        async def run() -> None:
            await resources.acquire()
            await resources.close()

        try:
            asyncio.run(run())
        except BaseException as exc:
            caller_errors.append(exc)
        finally:
            caller_finished.set()

    caller = threading.Thread(target=use_and_stop_loop, name="stopped-provider-request-loop")
    caller.start()
    assert cleanup_gate.started.wait(timeout=1.0)
    assert caller_finished.wait(timeout=1.0)
    caller.join(timeout=1.0)

    assert caller.is_alive() is False
    assert len(caller_errors) == 1
    assert isinstance(caller_errors[0], RuntimeOwnershipError)
    assert str(caller_errors[0]) == "Pipeline provider generation cleanup failed"
    assert len(providers) == 1
    provider = providers[0]
    assert provider.closed is False
    assert cleanup_finished.is_set() is False
    assert close_threads != caller_thread_id
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0
    assert resources.quarantined_generation_count == 1


def test_closed_request_loop_generation_can_only_transfer_to_cleanup_owner(tmp_path) -> None:
    owner_finished = threading.Event()
    owner_errors: list[BaseException] = []
    close_thread_names: list[str] = []
    providers: list[_ProviderProbe] = []

    class RecoverableProvider(_ProviderProbe):
        async def close(self) -> None:
            close_thread_names.append(threading.current_thread().name)
            await super().close()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = RecoverableProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    def acquire_without_close() -> None:
        try:
            asyncio.run(resources.acquire())
        except BaseException as exc:
            owner_errors.append(exc)
        finally:
            owner_finished.set()

    owner = threading.Thread(target=acquire_without_close, name="abandoned-provider-request-loop")
    owner.start()
    assert owner_finished.wait(timeout=1.0)
    owner.join(timeout=1.0)

    assert owner.is_alive() is False
    assert owner_errors == []
    assert len(providers) == 1
    assert providers[0].closed is False

    asyncio.run(resources.close())

    assert providers[0].closed is True
    assert close_thread_names == ["tacit-lifecycle-provider-owner"]
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


def test_explicit_current_release_also_reclaims_closed_abandoned_lease(tmp_path) -> None:
    providers: list[_ProviderProbe] = []
    first_loop = asyncio.new_event_loop()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    async def acquire_abandoned() -> None:
        await resources.acquire()

    first_loop.run_until_complete(acquire_abandoned())
    first_loop.close()

    async def release_current() -> None:
        current = await resources.acquire()
        await resources.close(current)

    asyncio.run(release_current())

    assert len(providers) == 1
    assert providers[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


def test_completed_owner_on_stopped_open_loop_is_reclaimed_but_pending_owner_is_preserved(tmp_path) -> None:
    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: _ProviderProbe(settings),
    )
    requester_loop = asyncio.new_event_loop()
    completed_owner = requester_loop.create_task(resources.acquire())
    requester_loop.run_until_complete(completed_owner)
    assert completed_owner.done()
    assert requester_loop.is_closed() is False

    asyncio.run(resources.close())
    assert lifecycle.service_owner_in_flight == 0
    requester_loop.close()

    paused_resources, paused_lifecycle, _paused_settings = _provider_resource_matrix(
        tmp_path / "paused-runtime",
        llm_factory=lambda settings: _ProviderProbe(settings),
    )
    paused_loop = asyncio.new_event_loop()
    ready = asyncio.Event()
    resume = asyncio.Event()

    async def pending_owner() -> None:
        handle = await paused_resources.acquire()
        ready.set()
        await resume.wait()
        await paused_resources.close(handle)

    pending_task = paused_loop.create_task(pending_owner())
    paused_loop.run_until_complete(ready.wait())
    assert pending_task.done() is False

    asyncio.run(paused_resources.close())
    assert paused_lifecycle.service_owner_in_flight == 1

    paused_loop.call_soon(resume.set)
    paused_loop.run_until_complete(pending_task)
    paused_loop.close()
    assert paused_lifecycle.service_owner_in_flight == 0


def test_keep_alive_provider_uses_worker_for_create_and_one_owner_loop_for_use_and_close(tmp_path) -> None:
    providers: list[LoopBoundHttpProvider] = []

    class LoopBoundHttpProvider(_ProviderProbe):
        def __init__(self, runtime_settings: Settings, endpoint: str) -> None:
            super().__init__(runtime_settings)
            try:
                self.factory_loop_id: int | None = id(asyncio.get_running_loop())
            except RuntimeError:
                self.factory_loop_id = None
            self.request_loop_id: int | None = None
            self.close_loop_id: int | None = None
            self.client = httpx.AsyncClient(base_url=endpoint)

        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            self.request_loop_id = id(asyncio.get_running_loop())
            response = await self.client.get("/")
            response.raise_for_status()
            return LLMResult(response.text)

        async def close(self) -> None:
            self.close_loop_id = id(asyncio.get_running_loop())
            await self.client.aclose()
            await super().close()

    with _keep_alive_http_server() as endpoint:

        def provider_factory(runtime_settings: Settings) -> LLMProvider:
            provider = LoopBoundHttpProvider(runtime_settings, endpoint)
            providers.append(provider)
            return provider

        resources, lifecycle, _runtime_settings = _provider_resource_matrix(
            tmp_path,
            llm_factory=provider_factory,
        )

        async def use_provider_without_closing() -> None:
            await resources.acquire()
            result = await resources.llm().chat_text("system", "user")
            assert result.text == "ok"

        asyncio.run(use_provider_without_closing())
        assert len(providers) == 1
        provider = providers[0]

        asyncio.run(resources.close())

        assert provider.closed is True
        assert provider.factory_loop_id is None
        assert provider.request_loop_id is not None
        assert provider.request_loop_id == provider.close_loop_id
        assert lifecycle.in_flight == 0
        assert lifecycle.retained == 0
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_bedrock_chat_runs_under_normal_lifecycle_owner_capacity(monkeypatch, tmp_path) -> None:
    from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime

    access_key = "AKIALIFECYCLEOWNER"
    secret_key = "lifecycle-owner-secret"
    runtime_settings = _settings(
        tmp_path,
        suffix="bedrock-lifecycle-owner",
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        llm_aws_access_key_id=access_key,
        llm_aws_secret_access_key=secret_key,
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    runtime_client = MagicMock()
    session = MagicMock()
    session.client.return_value = runtime_client
    observations: list[tuple[bool, int, str]] = []

    def converse(**_kwargs: Any) -> dict[str, Any]:
        observations.append(
            (
                lifecycle.current_thread_can_reuse_blocking_capacity(cleanup=False),
                lifecycle.blocking_in_flight,
                threading.current_thread().name,
            )
        )
        return {"output": {"message": {"content": [{"text": "ok"}]}}}

    runtime_client.converse.side_effect = converse
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _ResolvedBedrockRuntime(
            session=session,
            credential_identity=_static_bedrock_identity(access_key, secret_key),
        ),
    )
    resources = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=lifecycle,
        cleanup_grace_seconds=0.1,
    )

    async with lifecycle.slot():
        await resources.acquire()
        result = await resources.llm().chat_text("system", "user")
        assert result.text == "ok"
        assert observations == [(True, 1, "tacit-lifecycle-blocking-work")]
        assert lifecycle.service_owner_in_flight == 1
        await resources.close()

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_limit_one_owner_reuses_one_normal_permit_and_releases_it(tmp_path) -> None:
    observations: list[tuple[bool, int, int]] = []
    resources: _RuntimeProviderResources

    class NestedBlockingProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            blocking_work = LifecycleOwnedBlockingWork(lifecycle)

            def sdk_call() -> str:
                observations.append(
                    (
                        lifecycle.current_thread_can_reuse_blocking_capacity(cleanup=False),
                        lifecycle.blocking_in_flight,
                        threading.get_ident(),
                    )
                )
                return "ok"

            return LLMResult(
                await blocking_work.run(
                    sdk_call,
                    reason_code="test_nested_provider_sdk_call",
                )
            )

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: NestedBlockingProvider(settings),
        max_concurrent=1,
        cleanup_grace_seconds=0.1,
    )

    await resources.acquire()
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 1
    result = await resources.llm().chat_text("system", "user")
    assert result.text == "ok"
    assert observations == [(True, 1, observations[0][2])]
    await resources.close()

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_limit_one_provider_owner_does_not_starve_backend_realization(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix="limit-one-provider-and-backend",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        grafana_enabled=True,
        grafana_url="http://127.0.0.1:3000",
        signalfx_enabled=False,
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )
    providers: list[_ProviderProbe] = []
    backend_observations: list[tuple[int, str]] = []

    def provider_factory() -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    def backend_factory() -> list[DashboardBackend]:
        dependencies = dependency_holder[0]
        lifecycle = dependencies.pipeline_admission
        assert lifecycle is not None
        backend_observations.append((lifecycle.blocking_in_flight, threading.current_thread().name))
        backend = _BackendProbe(runtime_settings)
        backend.runtime_ownership = runtime_descriptor_for_backends(
            component="limit_one_backend",
            runtime_settings=runtime_settings,
        )
        return [cast(DashboardBackend, backend)]

    dependency_holder: list[PipelineDependencies] = []
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        backend_factory=_backend_factory(backend_factory, runtime_settings),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="limit_one_provider_factory",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
    )
    dependency_holder.append(dependencies)
    lifecycle = dependencies.pipeline_admission
    assert lifecycle is not None

    async with lifecycle.slot():
        await dependencies.acquire_resources()
        provider_resource = dependencies.llm_provider_factory
        assert provider_resource is not None
        assert await provider_resource().chat_text("system", "user") == LLMResult("")
        assert lifecycle.blocking_in_flight == 0
        backends = await dependencies.realize_backends()
        assert len(backends) == 1
        assert backend_observations == [(1, "tacit-lifecycle-blocking-work")]
        for _ in range(100):
            if lifecycle.blocking_in_flight == 0:
                break
            await asyncio.sleep(0)
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.service_owner_in_flight == 1
        await dependencies.close_resources()

    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.asyncio
async def test_sync_provider_accessor_transports_limit_one_lease_to_owner(tmp_path) -> None:
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        max_concurrent=1,
    )

    async with lifecycle.slot():
        provider = resources.llm()
        assert provider is providers[0]
        assert lifecycle.in_flight == 1
        assert lifecycle.blocking_in_flight == 0
        assert lifecycle.service_owner_in_flight == 1
        await resources.close()

    assert provider.closed is True
    await _assert_dependency_lifecycle_idle(lifecycle)


def test_foreign_loop_bound_factory_product_is_rejected_without_wrong_loop_close(tmp_path) -> None:
    owner_ready = threading.Event()
    release_owner = threading.Event()
    owner_errors: list[BaseException] = []
    products: list[_ProviderProbe] = []
    close_calls: list[tuple[int, int]] = []
    owner_thread_id: list[int] = []
    owner_loop_id: list[int] = []

    class ForeignLoopProvider(_ProviderProbe):
        async def close(self) -> None:
            close_calls.append((threading.get_ident(), id(asyncio.get_running_loop())))
            await super().close()

    resources, lifecycle, runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda _settings: products[0],
    )

    def own_product() -> None:
        owner_thread_id.append(threading.get_ident())

        async def run() -> None:
            owner_loop_id.append(id(asyncio.get_running_loop()))
            products.append(ForeignLoopProvider(runtime_settings))
            owner_ready.set()
            assert await asyncio.to_thread(release_owner.wait, 2.0)

        try:
            asyncio.run(run())
        except BaseException as exc:
            owner_errors.append(exc)

    owner = threading.Thread(target=own_product, name="foreign-provider-loop-owner")
    owner.start()
    assert owner_ready.wait(timeout=1.0)
    try:
        with pytest.raises(RuntimeOwnershipError, match="event loop affinity"):
            asyncio.run(resources.acquire())
        assert close_calls == [(owner_thread_id[0], owner_loop_id[0])]
        assert products[0].closed is True
        assert lifecycle.in_flight == 0
    finally:
        release_owner.set()
        owner.join(timeout=2.0)

    assert owner.is_alive() is False
    assert owner_errors == []
    assert close_calls == [(owner_thread_id[0], owner_loop_id[0])]


def test_foreign_loop_rejection_failure_releases_capacity_and_fences_runtime(tmp_path) -> None:
    close_calls: list[tuple[int, int]] = []
    factory_calls = 0

    class ClosedLoopProvider(_ProviderProbe):
        async def close(self) -> None:
            close_calls.append((threading.get_ident(), id(asyncio.get_running_loop())))
            await super().close()

    owner_loop = asyncio.new_event_loop()

    async def create_product(runtime_settings: Settings) -> ClosedLoopProvider:
        return ClosedLoopProvider(runtime_settings)

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            return product
        return _ProviderProbe(runtime_settings)

    resources, lifecycle, runtime_settings = _provider_resource_matrix(tmp_path, llm_factory=provider_factory)
    product = owner_loop.run_until_complete(create_product(runtime_settings))
    owner_loop.close()

    with pytest.raises(RuntimeOwnershipError, match="event loop owner is unavailable"):
        asyncio.run(resources.acquire())

    assert product.closed is False
    assert close_calls == []
    assert resources.quarantined_generation_count == 1
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.service_owner_in_flight == 0

    async def clean_generation() -> None:
        with pytest.raises(RuntimeOwnershipError, match="cleanup failed"):
            await resources.acquire()

    asyncio.run(clean_generation())
    assert lifecycle.runtime_fatal_circuit is not None
    assert factory_calls == 1
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.parametrize("capability", ("llm", "context"))
def test_stopped_foreign_loop_rejection_is_bounded_and_fences_generation(
    tmp_path,
    capability: str,
) -> None:
    owner_loop = asyncio.new_event_loop()
    product: LLMProvider | ContextProvider

    async def create_product(runtime_settings: Settings) -> LLMProvider | ContextProvider:
        if capability == "llm":
            return _ProviderProbe(runtime_settings)
        return _ContextProbe(runtime_settings)

    resources, lifecycle, runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=(
            (lambda _settings: cast(LLMProvider, product))
            if capability == "llm"
            else (lambda settings: _ProviderProbe(settings))
        ),
        context_factory=(lambda _settings: cast(ContextProvider, product)) if capability == "context" else None,
        cleanup_grace_seconds=0.02,
    )
    product = owner_loop.run_until_complete(create_product(runtime_settings))

    started = time.monotonic()
    try:
        with pytest.raises(RuntimeOwnershipError):
            asyncio.run(resources.acquire())
    finally:
        owner_loop.close()

    assert time.monotonic() - started < 0.5
    assert lifecycle.runtime_fatal_circuit is not None
    assert resources.lifecycle_state is ProviderLifecycleState.REVOKED
    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.parametrize("capability", ("llm", "context"))
def test_hung_foreign_loop_rejection_does_not_block_generation_owner(
    tmp_path,
    capability: str,
) -> None:
    owner_ready = threading.Event()
    close_started = threading.Event()
    close_finished = threading.Event()
    release_close = _CrossLoopAsyncGate()
    owner_loop_box: list[asyncio.AbstractEventLoop] = []
    products: list[LLMProvider | ContextProvider] = []
    owner_errors: list[BaseException] = []

    class HungProvider(_ProviderProbe):
        async def close(self) -> None:
            close_started.set()
            while not close_finished.is_set():
                try:
                    await release_close.wait()
                except asyncio.CancelledError:
                    continue
                close_finished.set()
            await super().close()

    class HungContext(_ContextProbe):
        async def close(self) -> None:
            close_started.set()
            while not close_finished.is_set():
                try:
                    await release_close.wait()
                except asyncio.CancelledError:
                    continue
                close_finished.set()
            await super().close()

    runtime_settings_box: list[Settings] = []

    def own_product() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        owner_loop_box.append(loop)

        async def create() -> None:
            runtime_settings = runtime_settings_box[0]
            if capability == "llm":
                products.append(HungProvider(runtime_settings))
            else:
                products.append(HungContext(runtime_settings))
            owner_ready.set()

        try:
            loop.run_until_complete(create())
            loop.run_forever()
        except BaseException as exc:
            owner_errors.append(exc)
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    resources, lifecycle, runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=(
            (lambda _settings: cast(LLMProvider, products[0]))
            if capability == "llm"
            else (lambda settings: _ProviderProbe(settings))
        ),
        context_factory=(lambda _settings: cast(ContextProvider, products[0])) if capability == "context" else None,
        cleanup_grace_seconds=0.02,
    )
    runtime_settings_box.append(runtime_settings)
    owner = threading.Thread(target=own_product, name=f"hung-foreign-{capability}-owner")
    acquire_errors: list[BaseException] = []
    acquire_finished = threading.Event()

    def acquire() -> None:
        try:
            asyncio.run(resources.acquire())
        except BaseException as exc:
            acquire_errors.append(exc)
        finally:
            acquire_finished.set()

    acquirer = threading.Thread(target=acquire, name=f"hung-foreign-{capability}-acquirer")
    owner.start()
    assert owner_ready.wait(timeout=1.0)
    acquirer.start()
    try:
        assert close_started.wait(timeout=1.0)
        completed_with_close_still_blocked = acquire_finished.wait(timeout=0.3)
    finally:
        release_close.release()
        assert close_finished.wait(timeout=1.0)
        if owner_loop_box and not owner_loop_box[0].is_closed():
            owner_loop_box[0].call_soon_threadsafe(owner_loop_box[0].stop)
        acquirer.join(timeout=1.0)
        owner.join(timeout=1.0)

    assert completed_with_close_still_blocked is True
    assert acquirer.is_alive() is False
    assert owner.is_alive() is False
    assert owner_errors == []
    assert len(acquire_errors) == 1
    assert isinstance(acquire_errors[0], RuntimeOwnershipError)
    assert lifecycle.runtime_fatal_circuit is not None
    assert resources.lifecycle_state is ProviderLifecycleState.REVOKED
    assert lifecycle.service_owner_in_flight == 0
    assert lifecycle.blocking_in_flight == 0


def test_provider_generation_supports_cross_event_loop_callers_through_owner(tmp_path) -> None:
    products: list[Any] = []

    def llm_factory(runtime_settings: Settings) -> LLMProvider:
        product = _ProviderProbe(runtime_settings)
        products.append(product)
        return product

    def context_factory(runtime_settings: Settings) -> ContextProvider:
        product = _ContextProbe(runtime_settings)
        products.append(product)
        return product

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=llm_factory,
        context_factory=context_factory,
    )
    first_loop_ready = threading.Event()
    release_first_loop = threading.Event()
    first_loop_errors: list[BaseException] = []

    def own_first_generation() -> None:
        async def run() -> None:
            await resources.acquire()
            first_loop_ready.set()
            await asyncio.to_thread(release_first_loop.wait)
            await resources.close()

        try:
            asyncio.run(run())
        except BaseException as exc:
            first_loop_errors.append(exc)

    owner = threading.Thread(target=own_first_generation, name="provider-generation-loop-owner")
    owner.start()
    try:
        assert first_loop_ready.wait(timeout=1.0)

        async def cross_loop_acquire() -> None:
            await resources.acquire()
            assert resources.llm() is products[0]
            assert resources.context() is products[1]
            await resources.close()
            assert all(product.closed is False for product in products)

        asyncio.run(cross_loop_acquire())
    finally:
        release_first_loop.set()
        owner.join(timeout=2.0)

    assert owner.is_alive() is False
    assert first_loop_errors == []
    assert len(products) == 2
    assert all(product.closed for product in products)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


def test_concurrent_cross_loop_initialization_notifies_every_caller(tmp_path) -> None:
    factory_started = threading.Event()
    release_factory = threading.Event()
    second_waiting = threading.Event()
    release_callers = threading.Event()
    acquired = [threading.Event(), threading.Event()]
    caller_loops: list[asyncio.AbstractEventLoop | None] = [None, None]
    caller_errors: list[BaseException] = []
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        factory_started.set()
        assert release_factory.wait(timeout=2.0)
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    def caller(index: int) -> None:
        async def run() -> None:
            loop = asyncio.get_running_loop()
            loop.set_debug(True)
            caller_loops[index] = loop

            async def acquire_and_close() -> None:
                await resources.acquire()
                acquired[index].set()
                assert await asyncio.to_thread(release_callers.wait, 2.0)
                await resources.close()

            if index == 1:
                acquire_task = asyncio.create_task(acquire_and_close())
                await asyncio.sleep(0)
                second_waiting.set()
                await acquire_task
            else:
                await acquire_and_close()

        try:
            asyncio.run(run())
        except BaseException as exc:
            caller_errors.append(exc)

    callers = [
        threading.Thread(target=caller, args=(index,), name=f"provider-caller-{index}", daemon=True)
        for index in range(2)
    ]
    callers[0].start()
    assert factory_started.wait(timeout=1.0)
    callers[1].start()
    assert second_waiting.wait(timeout=1.0)
    try:
        release_factory.set()
        assert acquired[0].wait(timeout=1.0)
        assert acquired[1].wait(timeout=1.0)
    finally:
        release_factory.set()
        release_callers.set()
        with resources._lock:
            initializing = resources._llm_initializing
        if isinstance(initializing, asyncio.Event) and caller_loops[1] is not None:
            caller_loops[1].call_soon_threadsafe(initializing.set)
        for thread in callers:
            thread.join(timeout=2.0)

    assert all(thread.is_alive() is False for thread in callers)
    assert caller_errors == []
    assert len(providers) == 1
    assert providers[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["llm", "context"])
async def test_provider_initialization_waiters_use_one_generation_notification(
    monkeypatch,
    tmp_path,
    capability: str,
) -> None:
    factory_started = threading.Event()
    release_factory = threading.Event()
    products: list[Any] = []
    original_sleep = asyncio.sleep
    zero_delay_sleeps = 0

    async def counted_sleep(delay: float, *args, **kwargs):
        nonlocal zero_delay_sleeps
        if delay == 0:
            zero_delay_sleeps += 1
        return await original_sleep(delay, *args, **kwargs)

    def blocking_llm_factory(runtime_settings: Settings) -> LLMProvider:
        if capability == "llm":
            factory_started.set()
            assert release_factory.wait(timeout=2.0)
        product = _ProviderProbe(runtime_settings)
        products.append(product)
        return product

    def blocking_context_factory(runtime_settings: Settings) -> ContextProvider:
        factory_started.set()
        assert release_factory.wait(timeout=2.0)
        product = _ContextProbe(runtime_settings)
        products.append(product)
        return product

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=blocking_llm_factory,
        context_factory=blocking_context_factory if capability == "context" else None,
        max_concurrent=8,
    )
    monkeypatch.setattr("tacit.dependencies.asyncio.sleep", counted_sleep)

    async def use_generation() -> None:
        await resources.acquire()
        await resources.close()

    clients = [asyncio.create_task(use_generation()) for _ in range(8)]
    assert await asyncio.to_thread(factory_started.wait, 1.0)
    await original_sleep(0.02)
    observed_zero_delay_sleeps = zero_delay_sleeps
    release_factory.set()
    await asyncio.gather(*clients)

    assert observed_zero_delay_sleeps == 0
    assert len(products) == (2 if capability == "context" else 1)
    assert all(product.closed for product in products)
    await _assert_dependency_lifecycle_idle(lifecycle)


@pytest.mark.asyncio
async def test_rejected_cleanup_budget_exhaustion_fails_closed_without_inline_close() -> None:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    lifecycle.bind_runtime_identity("rejected-cleanup-fail-closed")
    cleanup_work = LifecycleOwnedBlockingWork(lifecycle)

    class BlockingCloseProduct:
        def __init__(self) -> None:
            self.close_threads: list[int] = []

        def close_blocking(self) -> None:
            self.close_threads.append(threading.get_ident())

    product = BlockingCloseProduct()
    async with lifecycle.slot():
        permits = cleanup_work.reserve_cleanup_permits(3)
        assert permits is not None
        try:
            with pytest.raises(RuntimeOwnershipError, match="cleanup owner"):
                _cleanup_rejected_products(
                    lifecycle,
                    (product,),
                    cleanup_grace_seconds=0.01,
                    reason_code="rejected_cleanup_matrix",
                )
        finally:
            for permit in permits:
                lifecycle.release_blocking_permit(permit)

    assert product.close_threads == []
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_concurrent_dependency_runs_do_not_close_a_provider_still_in_use(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    providers: list[_ProviderProbe] = []
    calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal calls
        calls += 1
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    declared_provider = declare_runtime_factory(
        provider_factory,
        ownership=runtime_descriptor_for_provider(
            component="shared_provider_factory",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=_backend_factory(lambda: [], runtime_settings),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="history_factory",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="feedback_factory",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=declared_provider,
    )
    first_ready = asyncio.Event()
    second_ready = asyncio.Event()
    release_second = asyncio.Event()
    provider_resource = dependencies.llm_provider_factory
    assert provider_resource is not None

    async def first_run() -> None:
        await dependencies.acquire_resources()
        provider = providers[0]
        assert provider_resource() is provider
        first_ready.set()
        await second_ready.wait()
        await dependencies.close_resources()
        assert provider.closed is False
        release_second.set()

    async def second_run() -> None:
        await dependencies.acquire_resources()
        provider = providers[0]
        assert provider_resource() is provider
        second_ready.set()
        await first_ready.wait()
        await release_second.wait()
        assert provider.closed is False
        await dependencies.close_resources()

    await asyncio.gather(first_run(), second_run())

    assert calls == 1
    assert providers[0].closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("distinct_store_owners", [False, True])
async def test_independent_same_runtime_bundles_share_one_provider_generation(
    tmp_path,
    distinct_store_owners: bool,
) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix="shared-runtime-bundles",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        pipeline_max_concurrent=2,
        pipeline_max_queued=0,
    )
    first_stores = RuntimeStores(runtime_settings)
    second_stores = RuntimeStores(runtime_settings) if distinct_store_owners else first_stores
    providers: list[_ProviderProbe] = []
    factory_calls = 0
    close_calls = 0
    use_calls = 0

    class SharedProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            nonlocal use_calls
            use_calls += 1
            return LLMResult("shared")

        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            await super().close()

    def provider_factory() -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        provider = SharedProvider(runtime_settings)
        providers.append(provider)
        return provider

    declared_provider = declare_runtime_factory(
        provider_factory,
        ownership=runtime_descriptor_for_provider(
            component="shared_runtime_bundle_provider",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    first = build_pipeline_dependencies(
        runtime_settings,
        stores=first_stores,
        llm_provider_factory=declared_provider,
    )
    second = build_pipeline_dependencies(
        runtime_settings,
        stores=second_stores,
        llm_provider_factory=declared_provider,
    )
    assert first.pipeline_admission is second.pipeline_admission
    lifecycle = first.pipeline_admission
    assert lifecycle is not None
    first_ready = asyncio.Event()
    second_ready = asyncio.Event()
    first_released = asyncio.Event()

    async def first_request() -> None:
        await first.acquire_resources()
        assert first.llm_provider_factory is not None
        assert first.llm_provider_factory() is providers[0]
        first_ready.set()
        await second_ready.wait()
        await first.close_resources()
        assert providers[0].closed is False
        first_released.set()

    async def second_request() -> None:
        await first_ready.wait()
        await second.acquire_resources()
        assert second.llm_provider_factory is not None
        assert second.llm_provider_factory() is providers[0]
        second_ready.set()
        await first_released.wait()
        result = await second.llm_provider_factory().chat_text("system", "user")
        assert result.text == "shared"
        await second.close_resources()

    await asyncio.gather(first_request(), second_request())

    assert factory_calls == 1
    assert use_calls == 1
    assert close_calls == 1
    assert providers[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


def test_same_runtime_rejects_incompatible_provider_specs_before_factory_execution(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix="incompatible-provider-specs",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
    )
    first_stores = RuntimeStores(runtime_settings)
    second_stores = RuntimeStores(runtime_settings)
    factory_calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        return _ProviderProbe(runtime_settings)

    declared_provider = declare_runtime_factory(
        provider_factory,
        ownership=runtime_descriptor_for_provider(
            component="incompatible_spec_provider",
            runtime_settings=runtime_settings,
            capability="llm",
        ),
        factory_kind="provider:llm",
    )
    first = build_pipeline_dependencies(
        runtime_settings,
        stores=first_stores,
        llm_provider_factory=declared_provider,
        cleanup_grace_seconds=0.1,
    )

    with pytest.raises(RuntimeOwnershipError, match="provider specification"):
        build_pipeline_dependencies(
            runtime_settings,
            stores=second_stores,
            llm_provider_factory=declared_provider,
            cleanup_grace_seconds=0.2,
        )

    assert factory_calls == 0
    assert first.pipeline_admission is second_stores.pipeline_admission()
    assert first.pipeline_admission.service_owner_in_flight == 0


def test_default_provider_factories_share_one_same_runtime_manager(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix="default-provider-spec-identity",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
    )

    first = build_pipeline_dependencies(runtime_settings, stores=RuntimeStores(runtime_settings))
    second = build_pipeline_dependencies(runtime_settings, stores=RuntimeStores(runtime_settings))

    assert first.pipeline_admission is second.pipeline_admission
    assert first.resource_acquire is not None
    assert second.resource_acquire is not None
    assert first.resource_acquire.__self__ is second.resource_acquire.__self__


@pytest.mark.parametrize("capability", ["llm", "context"])
def test_same_runtime_shares_the_same_retained_explicit_provider_declaration(tmp_path, capability: str) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix=f"same-explicit-{capability}-identity",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="mcp" if capability == "context" else "none",
        context_mcp_server_url="http://127.0.0.1:8765",
    )
    factory_calls = 0

    def provider_factory() -> LLMProvider | ContextProvider:
        nonlocal factory_calls
        factory_calls += 1
        if capability == "llm":
            return _ProviderProbe(runtime_settings)
        return _ContextProbe(runtime_settings)

    declared_provider = declare_runtime_factory(
        cast(Any, provider_factory),
        ownership=runtime_descriptor_for_provider(
            component=f"same_explicit_{capability}_factory",
            runtime_settings=runtime_settings,
            capability=capability,
        ),
        factory_kind=f"provider:{capability}",
    )
    factory_arguments = {f"{capability}_provider_factory": declared_provider}

    first = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        **factory_arguments,
    )
    second = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        **factory_arguments,
    )

    assert first.resource_acquire is not None
    assert second.resource_acquire is not None
    assert first.resource_acquire.__self__ is second.resource_acquire.__self__
    assert factory_calls == 0


@pytest.mark.parametrize("capability", ["llm", "context"])
@pytest.mark.parametrize("reuse_callable", [False, True])
def test_same_runtime_rejects_distinct_explicit_provider_declarations_before_execution(
    tmp_path,
    capability: str,
    reuse_callable: bool,
) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix=f"distinct-explicit-{capability}-{reuse_callable}",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        context_provider="mcp" if capability == "context" else "none",
        context_mcp_server_url="http://127.0.0.1:8765",
    )
    factory_calls = 0

    def first_factory() -> LLMProvider | ContextProvider:
        nonlocal factory_calls
        factory_calls += 1
        if capability == "llm":
            return _ProviderProbe(runtime_settings)
        return _ContextProbe(runtime_settings)

    def second_factory() -> LLMProvider | ContextProvider:
        nonlocal factory_calls
        factory_calls += 1
        if capability == "llm":
            return _ProviderProbe(runtime_settings)
        return _ContextProbe(runtime_settings)

    ownership = runtime_descriptor_for_provider(
        component=f"distinct_explicit_{capability}_factory",
        runtime_settings=runtime_settings,
        capability=capability,
    )
    first_declaration = declare_runtime_factory(
        cast(Any, first_factory),
        ownership=ownership,
        factory_kind=f"provider:{capability}",
    )
    second_declaration = declare_runtime_factory(
        cast(Any, first_factory if reuse_callable else second_factory),
        ownership=ownership,
        factory_kind=f"provider:{capability}",
    )
    first_arguments = {f"{capability}_provider_factory": first_declaration}
    second_arguments = {f"{capability}_provider_factory": second_declaration}

    first = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        **first_arguments,
    )
    with pytest.raises(RuntimeOwnershipError, match="provider specification") as exc_info:
        build_pipeline_dependencies(
            runtime_settings,
            stores=RuntimeStores(runtime_settings),
            **second_arguments,
        )

    assert "0x" not in str(exc_info.value)
    assert factory_calls == 0
    assert first.pipeline_admission is not None
    assert first.pipeline_admission.service_owner_in_flight == 0


def test_same_runtime_chained_cleanup_requires_retained_callback_identity(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        suffix="chained-cleanup-spec-identity",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
    )
    lifecycle = RuntimeStores(runtime_settings).pipeline_admission()
    cleanup_calls: list[str] = []

    async def first_cleanup() -> None:
        cleanup_calls.append("first")

    async def second_cleanup() -> None:
        cleanup_calls.append("second")

    first = _RuntimeProviderResources.resolve(
        runtime_settings,
        lifecycle=lifecycle,
        chained_cleanup=first_cleanup,
    )
    shared = _RuntimeProviderResources.resolve(
        runtime_settings,
        lifecycle=lifecycle,
        chained_cleanup=first_cleanup,
    )
    assert shared is first

    with pytest.raises(RuntimeOwnershipError, match="provider specification") as exc_info:
        _RuntimeProviderResources.resolve(
            runtime_settings,
            lifecycle=lifecycle,
            chained_cleanup=second_cleanup,
        )

    assert "0x" not in str(exc_info.value)
    assert cleanup_calls == []
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_starting_cancellation_retires_unleased_generation(tmp_path) -> None:
    factory_started = threading.Event()
    release_factory = threading.Event()
    provider_closed = threading.Event()
    providers: list[_ProviderProbe] = []

    class StartingProvider(_ProviderProbe):
        async def close(self) -> None:
            await super().close()
            provider_closed.set()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        factory_started.set()
        assert release_factory.wait(timeout=1.0)
        provider = StartingProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )
    acquire_task = asyncio.create_task(resources.acquire())
    assert await asyncio.to_thread(factory_started.wait, 1.0)

    acquire_task.cancel()
    release_factory.set()
    with pytest.raises(asyncio.CancelledError):
        await acquire_task

    assert await asyncio.to_thread(provider_closed.wait, 1.0)
    assert len(providers) == 1
    assert providers[0].closed is True
    await _assert_dependency_lifecycle_idle(lifecycle)
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_shutdown_waits_for_starting_generation_before_factory_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    before_factory = asyncio.Event()
    release_factory = asyncio.Event()
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )
    realize = resources._realize_llm_provider

    async def delayed_realization() -> LLMProvider:
        before_factory.set()
        await release_factory.wait()
        return await realize()

    monkeypatch.setattr(resources, "_realize_llm_provider", delayed_realization)
    acquire_task = asyncio.create_task(resources.acquire())
    await before_factory.wait()

    shutdown_task = asyncio.create_task(resources.shutdown())
    await asyncio.sleep(0)
    assert shutdown_task.done() is False
    assert resources.lifecycle_state is not ProviderLifecycleState.EMPTY

    release_factory.set()
    acquisition = await asyncio.gather(acquire_task, return_exceptions=True)
    await shutdown_task

    assert isinstance(acquisition[0], RuntimeOwnershipError)
    assert all(provider.closed for provider in providers)
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_shutdown_during_provider_construction_retires_product_once(tmp_path) -> None:
    factory_started = threading.Event()
    release_factory = threading.Event()
    providers: list[_ProviderProbe] = []

    class CountingProvider(_ProviderProbe):
        def __init__(self, runtime_settings: Settings) -> None:
            super().__init__(runtime_settings)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            await super().close()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        factory_started.set()
        assert release_factory.wait(timeout=2.0)
        provider = CountingProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )
    acquire_task = asyncio.create_task(resources.acquire())
    assert await asyncio.to_thread(factory_started.wait, 1.0)

    shutdown_task = asyncio.create_task(resources.shutdown())
    await asyncio.sleep(0)
    assert shutdown_task.done() is False
    assert resources.lifecycle_state is not ProviderLifecycleState.EMPTY

    release_factory.set()
    acquisition = await asyncio.gather(acquire_task, return_exceptions=True)
    await shutdown_task

    assert isinstance(acquisition[0], RuntimeOwnershipError)
    assert len(providers) == 1
    assert providers[0].close_calls == 1
    assert providers[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_shutdown_during_context_construction_retires_product_once(tmp_path) -> None:
    factory_started = threading.Event()
    release_factory = threading.Event()
    contexts: list[_ContextProbe] = []

    class CountingContext(_ContextProbe):
        def __init__(self, runtime_settings: Settings) -> None:
            super().__init__(runtime_settings)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            await super().close()

    def context_factory(runtime_settings: Settings) -> ContextProvider:
        factory_started.set()
        assert release_factory.wait(timeout=2.0)
        context = CountingContext(runtime_settings)
        contexts.append(context)
        return context

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: _ProviderProbe(settings),
        context_factory=context_factory,
    )
    acquire_task = asyncio.create_task(resources.acquire())
    assert await asyncio.to_thread(factory_started.wait, 1.0)

    shutdown_task = asyncio.create_task(resources.shutdown())
    await asyncio.sleep(0)
    assert shutdown_task.done() is False
    assert resources.lifecycle_state is not ProviderLifecycleState.EMPTY

    release_factory.set()
    acquisition = await asyncio.gather(acquire_task, return_exceptions=True)
    await shutdown_task

    assert isinstance(acquisition[0], RuntimeOwnershipError)
    assert len(contexts) == 1
    assert contexts[0].close_calls == 1
    assert contexts[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


def test_shutdown_between_construction_and_adoption_retires_product_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    adoption_gate = _CrossLoopAsyncGate()
    adoption_threads: list[int] = []
    close_threads: list[int] = []
    caller_done = threading.Event()
    providers: list[_ProviderProbe] = []
    caller_errors: list[BaseException] = []

    class CountingProvider(_ProviderProbe):
        def __init__(self, runtime_settings: Settings) -> None:
            super().__init__(runtime_settings)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            close_threads.append(threading.get_ident())
            await super().close()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = CountingProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        cleanup_grace_seconds=1.0,
    )
    adopt = resources._adopt_llm_provider

    async def blocked_adoption(provider: LLMProvider, **kwargs: Any) -> None:
        adoption_threads.append(threading.get_ident())
        await adoption_gate.wait()
        await adopt(provider, **kwargs)

    monkeypatch.setattr(resources, "_adopt_llm_provider", blocked_adoption)
    retirement_started = threading.Event()
    begin_retirement = resources._begin_generation_retirement

    def observed_retirement(state, *, revoked_by=None) -> None:
        begin_retirement(state, revoked_by=revoked_by)
        retirement_started.set()

    monkeypatch.setattr(resources, "_begin_generation_retirement", observed_retirement)

    def acquire() -> None:
        try:
            asyncio.run(resources.acquire())
        except BaseException as exc:
            caller_errors.append(exc)
        finally:
            caller_done.set()

    caller = threading.Thread(target=acquire, name="provider-adoption-requester")
    caller.start()
    assert adoption_gate.started.wait(timeout=1.0)
    with resources._lock:
        state = resources._generation_owner
        assert state is not None
        owner_loop = state.loop
        owner_thread = state.owner_thread
    assert owner_loop is not None
    assert owner_thread is not None

    shutdown_done = threading.Event()
    shutdown_errors: list[BaseException] = []

    def shutdown() -> None:
        try:
            asyncio.run(resources.shutdown())
        except BaseException as exc:
            shutdown_errors.append(exc)
        finally:
            shutdown_done.set()

    shutdown_thread = threading.Thread(target=shutdown, name="provider-adoption-shutdown")
    shutdown_thread.start()
    assert retirement_started.wait(timeout=1.0)
    assert resources.lifecycle_state is ProviderLifecycleState.DRAINING
    assert shutdown_done.is_set() is False
    owner_heartbeat = threading.Event()
    owner_loop.call_soon_threadsafe(owner_heartbeat.set)
    assert owner_heartbeat.wait(timeout=0.2)
    assert resources._provider_factory_work.active == 1
    assert lifecycle.blocking_in_flight == 1

    adoption_gate.release()
    assert caller_done.wait(timeout=2.0)
    assert shutdown_done.wait(timeout=2.0)
    caller.join(timeout=1.0)
    shutdown_thread.join(timeout=1.0)

    assert caller_errors and isinstance(caller_errors[0], RuntimeOwnershipError)
    assert shutdown_errors == []
    assert len(providers) == 1
    assert adoption_threads == [owner_thread.ident]
    assert close_threads == [owner_thread.ident]
    assert providers[0].close_calls == 1
    assert providers[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_provider_adoption_lock_contention_never_blocks_generation_owner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    """A foreign handoff mutex cannot stall the persistent service event loop."""
    products: list[_ProviderProbe] = []

    class CountingProvider(_ProviderProbe):
        def __init__(self, runtime_settings: Settings) -> None:
            super().__init__(runtime_settings)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            await super().close()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        product = CountingProvider(runtime_settings)
        products.append(product)
        return product

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        cleanup_grace_seconds=1.0,
    )
    lock_attempted = threading.Event()

    class ObservedLock:
        def __init__(self) -> None:
            self._lock = threading.Lock()

        def acquire(self, blocking: bool = True) -> bool:
            lock_attempted.set()
            return self._lock.acquire(blocking=blocking)

        def release(self) -> None:
            self._lock.release()

        def __enter__(self) -> ObservedLock:
            self.acquire()
            return self

        def __exit__(self, *_args: object) -> None:
            self.release()

    handoff = dependencies_module._ProviderAdoptionHandoff()
    handoff.lock = ObservedLock()
    handoff.lock.acquire()
    lock_attempted.clear()
    monkeypatch.setattr(dependencies_module, "_ProviderAdoptionHandoff", lambda: handoff)

    acquisition = asyncio.create_task(resources.acquire())
    owner_loop = None
    for _ in range(1_000):
        with resources._lock:
            state = resources._generation_owner
            owner_loop = None if state is None else state.loop
        if owner_loop is not None and products:
            break
        await asyncio.sleep(0)
    assert owner_loop is not None
    assert await asyncio.to_thread(lock_attempted.wait, 1.0)

    heartbeat = threading.Event()
    owner_loop.call_soon_threadsafe(heartbeat.set)
    owner_was_responsive = await asyncio.to_thread(heartbeat.wait, 0.2)
    try:
        result = (
            await asyncio.wait_for(
                asyncio.gather(acquisition, return_exceptions=True),
                timeout=1.0,
            )
        )[0]
    finally:
        handoff.lock.release()
    if not isinstance(result, BaseException):
        await resources.close(result)
    else:
        await resources.shutdown()

    assert owner_was_responsive
    assert isinstance(result, RuntimeOwnershipError)
    assert len(products) == 1
    assert products[0].close_calls == 1
    assert products[0].closed is True
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_cancelled_provider_operation_drains_before_final_lease_close(tmp_path) -> None:
    operation_started = threading.Event()
    operation_finished = threading.Event()
    cancellation_observed = threading.Event()
    close_started = threading.Event()
    settle_operation = _CrossLoopAsyncGate()

    class DrainingProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            operation_started.set()
            try:
                await settle_operation.wait()
                return LLMResult("settled")
            except asyncio.CancelledError:
                cancellation_observed.set()
                raise
            finally:
                operation_finished.set()

        async def close(self) -> None:
            close_started.set()
            await super().close()

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: DrainingProvider(settings),
    )
    handle = await resources.acquire()
    provider = resources.llm()
    operation = asyncio.create_task(provider.chat_text("system", "user"))
    assert await asyncio.to_thread(operation_started.wait, 1.0)

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    owner_barrier = threading.Event()
    state = resources._generation_owner
    assert state is not None and state.loop is not None
    state.loop.call_soon_threadsafe(owner_barrier.set)
    assert await asyncio.to_thread(owner_barrier.wait, 1.0)
    assert cancellation_observed.is_set() is False
    assert operation_finished.is_set() is False

    release_started = asyncio.Event()

    async def release_last_lease() -> None:
        release_started.set()
        await resources.close(handle)

    release_task = asyncio.create_task(release_last_lease())
    await release_started.wait()
    assert close_started.is_set() is False
    assert lifecycle.service_owner_in_flight == 1

    settle_operation.release()
    await release_task

    assert operation_finished.is_set()
    assert close_started.is_set()
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_cancelled_provider_operation_keeps_blocking_worker_admitted_until_exit(tmp_path) -> None:
    worker_started = threading.Event()
    release_worker = threading.Event()
    lifecycle_box: list[PipelineAdmissionController] = []

    class BlockingProvider(_ProviderProbe):
        async def chat_text(self, *_args, **_kwargs) -> LLMResult:
            def blocking_call() -> LLMResult:
                worker_started.set()
                assert release_worker.wait(timeout=2.0)
                return LLMResult("settled")

            return cast(
                LLMResult,
                await LifecycleOwnedBlockingWork(lifecycle_box[0]).run(
                    blocking_call,
                    reason_code="provider_operation_blocking_worker",
                ),
            )

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: BlockingProvider(settings),
        max_concurrent=1,
    )
    lifecycle_box.append(lifecycle)
    handle = await resources.acquire()
    operation = asyncio.create_task(resources.llm().chat_text("system", "user"))
    assert await asyncio.to_thread(worker_started.wait, 1.0)

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert lifecycle.in_flight == 1
    assert lifecycle.blocking_in_flight == 1

    release_worker.set()
    await resources.close(handle)

    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_last_release_and_new_acquire_cross_only_after_epoch_retirement(tmp_path) -> None:
    first_close_gate = _CrossLoopAsyncGate()
    first_close_started = threading.Event()
    providers: list[_ProviderProbe] = []

    class FirstProvider(_ProviderProbe):
        async def close(self) -> None:
            first_close_started.set()
            await first_close_gate.wait()
            await super().close()

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider: _ProviderProbe = _ProviderProbe(runtime_settings) if providers else FirstProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
        cleanup_grace_seconds=1.0,
    )
    first_handle = await resources.acquire()
    first_release = asyncio.create_task(resources.close(first_handle))
    assert await asyncio.to_thread(first_close_started.wait, 1.0)

    acquire_attempted = asyncio.Event()
    second_acquired = asyncio.Event()
    second_handle_box: list[Any] = []

    async def acquire_next_epoch() -> None:
        acquire_attempted.set()
        second_handle_box.append(await resources.acquire())
        second_acquired.set()

    second_acquire = asyncio.create_task(acquire_next_epoch())
    await acquire_attempted.wait()
    assert second_acquired.is_set() is False
    assert len(providers) == 1
    assert lifecycle.service_owner_in_flight == 1

    first_close_gate.release()
    await first_release
    await second_acquire

    assert len(providers) == 2
    assert providers[0].closed is True
    assert resources.generation_epoch == 2
    await resources.close(second_handle_box[0])
    assert providers[1].closed is True
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_duplicate_and_stale_provider_lease_handles_cannot_mutate_new_epoch(tmp_path) -> None:
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )
    first_handle = await resources.acquire()
    await resources.close(first_handle)

    with pytest.raises(RuntimeOwnershipError, match="stale or duplicate"):
        await resources.close(first_handle)

    second_handle = await resources.acquire()
    assert resources.llm() is providers[1]
    with pytest.raises(RuntimeOwnershipError, match="prior provider generation"):
        await resources.close(first_handle)
    foreign_handle = replace(second_handle, graph_nonce="foreign-runtime-graph")
    with pytest.raises(RuntimeOwnershipError, match="another runtime graph"):
        await resources.close(foreign_handle)

    assert providers[1].closed is False
    assert lifecycle.service_owner_in_flight == 1
    await resources.close(second_handle)
    assert providers[1].closed is True
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_runtime_shutdown_revokes_leases_and_releases_provider_owner(tmp_path) -> None:
    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: _ProviderProbe(settings),
    )
    handle = await resources.acquire()
    provider = resources.llm()

    await resources.shutdown()

    assert provider.closed is True
    with pytest.raises(RuntimeOwnershipError, match="generation is no longer active"):
        await provider.chat_text("system", "user")
    with pytest.raises(RuntimeOwnershipError, match="stale or duplicate"):
        await resources.close(handle)
    assert lifecycle.in_flight == 0
    assert lifecycle.retained == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_next_epoch_waits_until_prior_owner_thread_has_exited(monkeypatch, tmp_path) -> None:
    original_thread = threading.Thread
    first_target_returned = threading.Event()
    release_first_thread = threading.Event()
    second_owner_started = threading.Event()
    counter_lock = threading.Lock()
    owner_count = 0
    active_owners = 0
    maximum_active_owners = 0

    def recording_thread(*args, **kwargs):
        nonlocal owner_count
        nonlocal active_owners
        nonlocal maximum_active_owners
        if kwargs.get("name") != "tacit-lifecycle-provider-owner":
            return original_thread(*args, **kwargs)
        target = kwargs["target"]
        owner_count += 1
        owner_index = owner_count

        def recorded_target() -> None:
            nonlocal active_owners
            nonlocal maximum_active_owners
            with counter_lock:
                active_owners += 1
                maximum_active_owners = max(maximum_active_owners, active_owners)
            if owner_index == 2:
                second_owner_started.set()
            try:
                target()
            finally:
                if owner_index == 1:
                    first_target_returned.set()
                    assert release_first_thread.wait(timeout=1.0)
                with counter_lock:
                    active_owners -= 1

        return original_thread(*args, **{**kwargs, "target": recorded_target})

    monkeypatch.setattr(threading, "Thread", recording_thread)
    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: _ProviderProbe(settings),
    )
    first_handle = await resources.acquire()
    first_close = asyncio.create_task(resources.close(first_handle))
    assert await asyncio.to_thread(first_target_returned.wait, 1.0)

    second_acquire = asyncio.create_task(resources.acquire())
    overlapped = await asyncio.to_thread(second_owner_started.wait, 0.1)
    release_first_thread.set()
    await first_close
    second_handle = await second_acquire
    await resources.close(second_handle)

    assert overlapped is False
    assert maximum_active_owners == 1
    assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_distinct_runtime_graphs_keep_provider_owners_isolated(tmp_path) -> None:
    first_settings = _settings(
        tmp_path,
        suffix="isolated-runtime-a",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
    )
    second_settings = _settings(
        tmp_path,
        suffix="isolated-runtime-b",
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11435",
    )
    first = build_pipeline_dependencies(first_settings, stores=RuntimeStores(first_settings))
    second = build_pipeline_dependencies(second_settings, stores=RuntimeStores(second_settings))
    assert first.pipeline_admission is not second.pipeline_admission

    first_handle, second_handle = await asyncio.gather(
        first.acquire_resources(),
        second.acquire_resources(),
    )
    assert first.pipeline_admission is not None
    assert second.pipeline_admission is not None
    assert first.pipeline_admission.service_owner_in_flight == 1
    assert second.pipeline_admission.service_owner_in_flight == 1

    await asyncio.gather(
        first.close_resources(first_handle),
        second.close_resources(second_handle),
    )
    assert first.pipeline_admission.service_owner_in_flight == 0
    assert second.pipeline_admission.service_owner_in_flight == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["transient", "terminal"])
async def test_sdk_close_guard_retries_only_failed_generation_cleanup(
    tmp_path,
    failure_mode: str,
) -> None:
    providers: list[GuardedProvider] = []

    class GuardedProvider(_ProviderProbe):
        def __init__(self, runtime_settings: Settings) -> None:
            super().__init__(runtime_settings)
            self.http_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _request: httpx.Response(200)))
            self.close_guard = LLMSDKHTTPClientCloseGuard(self.http_client)
            self.sdk_close_calls = 0

        async def close(self) -> None:
            async def close_sdk() -> None:
                self.sdk_close_calls += 1
                if failure_mode == "terminal" or self.sdk_close_calls == 1:
                    raise RuntimeError("synthetic SDK close failure")

            await self.close_guard.close(close_sdk)
            self.closed = True

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = GuardedProvider(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    await resources.acquire()
    provider = providers[0]
    if failure_mode == "terminal":
        with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
            await resources.close()
    else:
        await resources.close()

    assert provider.sdk_close_calls == 2
    assert provider.http_client.is_closed is True
    if failure_mode == "terminal":
        _assert_terminal_provider_cleanup(resources, lifecycle)
    else:
        assert provider.closed is True
        assert resources.lifecycle_state is ProviderLifecycleState.EMPTY
        assert resources.quarantined_generation_count == 0
        assert lifecycle.runtime_fatal_circuit is None
        assert lifecycle.service_owner_in_flight == 0


@pytest.mark.asyncio
async def test_child_close_failure_retries_once_then_trips_runtime_fatal_circuit(tmp_path) -> None:
    providers: list[_ProviderProbe] = []
    close_calls = 0

    class FirstCloseFails(_ProviderProbe):
        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            raise RuntimeError("synthetic child close failure")

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider: _ProviderProbe
        if providers:
            provider = _ProviderProbe(runtime_settings)
        else:
            provider = FirstCloseFails(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    await resources.acquire()
    first = resources.llm()
    first_reference = weakref.ref(first)
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.close()

    assert close_calls == 2
    with pytest.raises(RuntimeOwnershipError, match="generation is no longer active"):
        await first.chat_text("system", "user")
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.acquire()
    assert providers == [first]
    _assert_terminal_provider_cleanup(resources, lifecycle)
    providers.clear()
    del first
    _assert_collected(first_reference)


@pytest.mark.asyncio
async def test_provider_cleanup_failures_render_only_stable_secret_free_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    credential_sentinel = "SENTINEL_CREDENTIAL=top-secret"
    tenant_sentinel = "tenant-private"
    path_sentinel = str(tmp_path / "private-provider.sock")
    output = io.StringIO()
    original_config = structlog.get_config()
    original_logger = dependencies_module.logger

    class SensitiveContext(_ContextProbe):
        async def close(self) -> None:
            raise RuntimeError(f"{credential_sentinel} {tenant_sentinel} {path_sentinel}")

    async def sensitive_chained_cleanup() -> None:
        raise OSError(f"{path_sentinel} {tenant_sentinel} {credential_sentinel}")

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=lambda settings: _ProviderProbe(settings),
        context_factory=lambda settings: SensitiveContext(settings),
    )
    resources._chained_cleanup = sensitive_chained_cleanup

    try:
        configure_logging("INFO")
        production_config = structlog.get_config()
        structlog.configure(
            processors=production_config["processors"],
            context_class=production_config["context_class"],
            wrapper_class=production_config["wrapper_class"],
            logger_factory=structlog.PrintLoggerFactory(file=output),
            cache_logger_on_first_use=False,
        )
        monkeypatch.setattr(dependencies_module, "logger", structlog.get_logger())

        await resources.acquire()
        with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
            await resources.close()
    finally:
        structlog.configure(**original_config)
        monkeypatch.setattr(dependencies_module, "logger", original_logger)

    rendered = output.getvalue()
    assert credential_sentinel not in rendered
    assert tenant_sentinel not in rendered
    assert path_sentinel not in rendered
    assert "Traceback" not in rendered
    assert '"resource": "context"' in rendered
    assert '"resource": "chained"' in rendered
    assert '"reason_code": "provider_child_cleanup_failed"' in rendered
    assert '"error_type": "RuntimeError"' in rendered
    assert '"error_type": "OSError"' in rendered
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.acquire()
    _assert_terminal_provider_cleanup(resources, lifecycle)


@pytest.mark.asyncio
async def test_stopped_generation_owner_trips_runtime_fatal_and_releases_all_authority(tmp_path) -> None:
    providers: list[_ProviderProbe] = []

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    await resources.acquire()
    first = resources.llm()
    first_reference = weakref.ref(first)
    state = resources._generation_owner
    assert state is not None
    assert state.loop is not None
    state.loop.call_soon_threadsafe(state.loop.stop)
    assert await asyncio.to_thread(state.done.wait, 1.0)

    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.close()

    with pytest.raises(RuntimeOwnershipError, match="generation is no longer active"):
        await first.chat_text("system", "user")

    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.acquire()
    assert providers == [first]
    _assert_terminal_provider_cleanup(resources, lifecycle)
    providers.clear()
    del first, state
    _assert_collected(first_reference)


@pytest.mark.asyncio
async def test_runtime_fatal_cleanup_rejections_keep_only_bounded_metadata(tmp_path) -> None:
    rejected_acquires = 20
    factory_calls = 0
    close_calls = 0
    provider_references: list[weakref.ReferenceType[LLMProvider]] = []

    class AlwaysFailsClose(_ProviderProbe):
        async def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            raise RuntimeError("synthetic repeated close failure")

    def provider_factory(runtime_settings: Settings) -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        provider = AlwaysFailsClose(runtime_settings)
        provider_references.append(weakref.ref(provider))
        return provider

    resources, lifecycle, _runtime_settings = _provider_resource_matrix(
        tmp_path,
        llm_factory=provider_factory,
    )

    await resources.acquire()
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await resources.close()

    failures: set[tuple[str, str, str]] = set()
    for _ in range(rejected_acquires):
        with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed") as exc_info:
            await resources.acquire()
        error = exc_info.value
        assert getattr(error, "runtime_provider_fatal", False) is True
        reason_code = str(getattr(error, "cleanup_reason_code", ""))
        error_type = str(getattr(error, "cleanup_error_type", ""))
        assert 0 < len(reason_code) <= 128
        assert 0 < len(error_type) <= 128
        failures.add((str(error), reason_code, error_type))

    assert failures == {
        (
            "Pipeline provider generation cleanup failed",
            "provider_child_cleanup_failed",
            "RuntimeError",
        )
    }
    assert close_calls == 2
    assert factory_calls == 1
    _assert_terminal_provider_cleanup(resources, lifecycle)
    _assert_collected(provider_references[0])


@pytest.mark.asyncio
async def test_hung_provider_cleanup_trips_runtime_fatal_without_a_next_epoch(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        pipeline_max_concurrent=2,
        pipeline_max_queued=0,
    )
    close_gate = _CrossLoopAsyncGate()
    first_closed = threading.Event()

    class BlockingProvider(_ProviderProbe):
        async def close(self) -> None:
            await close_gate.wait()
            await super().close()
            first_closed.set()

    providers: list[_ProviderProbe] = []

    def provider_factory() -> LLMProvider:
        provider: _ProviderProbe
        if providers:
            provider = _ProviderProbe(runtime_settings)
        else:
            provider = BlockingProvider(runtime_settings)
        providers.append(provider)
        return provider

    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=_backend_factory(lambda: [], runtime_settings),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="bounded_history_factory",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="bounded_feedback_factory",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="bounded_provider_factory",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
        cleanup_grace_seconds=0.01,
    )
    assert dependencies.llm_provider_factory is not None
    assert dependencies.pipeline_admission is not None

    async with dependencies.pipeline_admission.slot():
        await dependencies.acquire_resources()
        first = providers[0]
        first_reference = weakref.ref(first)
        assert dependencies.llm_provider_factory() is first
        with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
            await asyncio.wait_for(dependencies.close_resources(), timeout=0.2)

    assert close_gate.started.is_set()
    assert first.closed is False
    assert first_closed.is_set() is False
    manager = dependencies.pipeline_admission.execution_graph.provider_manager()
    assert isinstance(manager, _RuntimeProviderResources)

    with pytest.raises(RuntimeOwnershipError, match="runtime cleanup failed"):
        await dependencies.pipeline_admission.acquire()
    with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
        await dependencies.acquire_resources()

    assert providers == [first]
    with pytest.raises(RuntimeOwnershipError, match="generation is no longer active"):
        await first.chat_text("system", "user")
    _assert_terminal_provider_cleanup(manager, dependencies.pipeline_admission)
    providers.clear()
    del first
    _assert_collected(first_reference)


@pytest.mark.asyncio
async def test_hung_provider_cleanup_rejects_repeated_acquires_without_new_authority(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        pipeline_max_concurrent=2,
        pipeline_max_queued=0,
    )
    close_gate = _CrossLoopAsyncGate()
    close_finished = threading.Event()

    class BlockingProvider(_ProviderProbe):
        def __init__(self) -> None:
            super().__init__(runtime_settings)
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            await close_gate.wait()
            await super().close()
            close_finished.set()

    providers: list[BlockingProvider] = []
    factory_calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal factory_calls
        provider = BlockingProvider()
        providers.append(provider)
        factory_calls += 1
        return provider

    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=_backend_factory(lambda: [], runtime_settings),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="hung_history_factory",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="hung_feedback_factory",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="hung_provider_factory",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
        cleanup_grace_seconds=0.01,
    )
    assert dependencies.pipeline_admission is not None
    assert dependencies.llm_provider_factory is not None

    async with dependencies.pipeline_admission.slot():
        await dependencies.acquire_resources()
        provider = providers[0]
        provider_reference = weakref.ref(provider)
        assert dependencies.llm_provider_factory() is provider
        with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
            await dependencies.close_resources()

    assert close_gate.started.is_set()
    assert close_finished.is_set() is False
    assert provider.close_calls == 1
    manager = dependencies.pipeline_admission.execution_graph.provider_manager()
    assert isinstance(manager, _RuntimeProviderResources)
    _assert_terminal_provider_cleanup(manager, dependencies.pipeline_admission)

    for _ in range(20):
        with pytest.raises(RuntimeOwnershipError, match="runtime cleanup failed"):
            await dependencies.pipeline_admission.acquire()
        with pytest.raises(RuntimeOwnershipError, match="generation cleanup failed"):
            await dependencies.acquire_resources()

    assert factory_calls == 1
    assert providers == [provider]
    _assert_terminal_provider_cleanup(manager, dependencies.pipeline_admission)
    providers.clear()
    del provider
    _assert_collected(provider_reference)


@pytest.mark.asyncio
async def test_new_run_waits_for_previous_provider_generation_to_close(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    close_gate = _CrossLoopAsyncGate()

    class BlockingProvider(_ProviderProbe):
        async def close(self) -> None:
            await close_gate.wait()
            await super().close()

    providers: list[_ProviderProbe] = []

    def provider_factory() -> LLMProvider:
        provider: _ProviderProbe
        if providers:
            provider = _ProviderProbe(runtime_settings)
        else:
            provider = BlockingProvider(runtime_settings)
        providers.append(provider)
        return provider

    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=_backend_factory(lambda: [], runtime_settings),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="history_factory",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="feedback_factory",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="generation_provider_factory",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
    )
    provider_resource = dependencies.llm_provider_factory
    assert provider_resource is not None
    first_ready = asyncio.Event()
    begin_first_close = asyncio.Event()
    second_acquired = asyncio.Event()
    release_second = asyncio.Event()

    async def first_run() -> None:
        await dependencies.acquire_resources()
        assert provider_resource() is providers[0]
        first_ready.set()
        await begin_first_close.wait()
        await dependencies.close_resources()

    async def second_run() -> None:
        assert await asyncio.to_thread(close_gate.started.wait, 1.0)
        await dependencies.acquire_resources()
        assert provider_resource() is providers[1]
        second_acquired.set()
        await release_second.wait()
        await dependencies.close_resources()

    first_task = asyncio.create_task(first_run())
    await first_ready.wait()
    begin_first_close.set()
    assert await asyncio.to_thread(close_gate.started.wait, 1.0)
    second_task = asyncio.create_task(second_run())
    await asyncio.sleep(0)
    assert second_acquired.is_set() is False

    close_gate.release()
    await second_acquired.wait()
    assert providers[0].closed is True
    release_second.set()
    await asyncio.gather(first_task, second_task)
    assert providers[1].closed is True


@pytest.mark.asyncio
async def test_inherited_child_context_cannot_release_parent_provider_lease(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    providers: list[_ProviderProbe] = []

    def provider_factory() -> LLMProvider:
        provider = _ProviderProbe(runtime_settings)
        providers.append(provider)
        return provider

    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=_backend_factory(lambda: [], runtime_settings),
        history_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="history_factory",
                runtime_settings=runtime_settings,
                database_role="history",
                database_path=runtime_settings.history_db_path,
            ),
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: object(),
            ownership=runtime_descriptor_for_store(
                component="feedback_factory",
                runtime_settings=runtime_settings,
                database_role="feedback",
                database_path=runtime_settings.feedback_db_path,
            ),
            factory_kind="store:feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
        llm_provider_factory=declare_runtime_factory(
            provider_factory,
            ownership=runtime_descriptor_for_provider(
                component="parent_owned_provider_factory",
                runtime_settings=runtime_settings,
                capability="llm",
            ),
            factory_kind="provider:llm",
        ),
    )
    provider_resource = dependencies.llm_provider_factory
    assert provider_resource is not None

    await dependencies.acquire_resources()
    provider = providers[0]
    assert provider_resource() is provider
    await asyncio.create_task(dependencies.close_resources())

    assert provider.closed is False
    await dependencies.close_resources()
    assert provider.closed is True


@pytest.mark.asyncio
async def test_foreign_knowledge_owner_fails_before_provider_construction(
    monkeypatch,
    tmp_path,
) -> None:
    active = _settings(tmp_path)
    foreign = _settings(tmp_path, suffix="foreign")
    provider_calls = 0

    class ForeignKnowledgeService:
        runtime_ownership = runtime_descriptor_for_store(
            component="foreign_knowledge_service",
            runtime_settings=foreign,
            database_role="signals",
            database_path=foreign.signals_db_path,
        )

    def create_provider_probe(_runtime_settings: Settings) -> LLMProvider:
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("provider constructed before knowledge ownership validation")

    monkeypatch.setattr("tacit.agents.providers.registry.create_provider", create_provider_probe)
    knowledge_factory = declare_runtime_factory(
        ForeignKnowledgeService,
        ownership=runtime_descriptor_for_store(
            component="declared_knowledge_factory",
            runtime_settings=active,
            database_role="signals",
            database_path=active.signals_db_path,
        ),
        factory_kind="knowledge:signals",
    )
    dependencies = build_pipeline_dependencies(
        active,
        stores=RuntimeStores(active),
        backend_factory=_backend_factory(
            lambda: [cast(DashboardBackend, _BackendProbe(active))],
            active,
        ),
        knowledge_service_factory=knowledge_factory,
    )

    with pytest.raises(RuntimeStoreReadinessError) as exc_info:
        await run_pipeline(DashRequest(prompt="checkout latency"), dependencies)

    assert isinstance(exc_info.value.__cause__, RuntimeOwnershipError)
    assert provider_calls == 0


def test_openai_and_anthropic_ignore_ambient_remote_overrides(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENAI_BASE_URL", "http://127.0.0.1:43210/v1")
    monkeypatch.setenv("OPENAI_ORG_ID", "ambient-org")
    monkeypatch.setenv("OPENAI_PROJECT_ID", "ambient-project")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "http://127.0.0.1:43211")
    openai_client = MagicMock()
    anthropic_client = MagicMock()
    openai_sdk = openai_client.return_value
    openai_sdk.api_key = "openai-secret"
    openai_sdk.admin_api_key = ""
    openai_sdk.workload_identity = None
    openai_sdk._api_key_provider = None
    openai_sdk.organization = ""
    openai_sdk.project = ""
    openai_sdk.webhook_secret = ""
    openai_sdk._custom_headers = {}
    openai_sdk.close = AsyncMock()
    anthropic_sdk = anthropic_client.return_value
    anthropic_sdk.api_key = "anthropic-secret"
    anthropic_sdk.auth_token = None
    anthropic_sdk.credentials = None
    anthropic_sdk.webhook_key = ""
    anthropic_sdk._custom_headers = {}
    anthropic_sdk.close = AsyncMock()
    openai_http_client = MagicMock()
    openai_http_client.is_closed = False
    openai_http_client.aclose = AsyncMock()
    anthropic_http_client = MagicMock()
    anthropic_http_client.is_closed = False
    anthropic_http_client.aclose = AsyncMock()
    monkeypatch.setattr("tacit.agents.providers.openai_provider.openai.AsyncOpenAI", openai_client)
    monkeypatch.setattr("tacit.agents.providers.anthropic.anthropic.AsyncAnthropic", anthropic_client)
    monkeypatch.setattr(
        "tacit.agents.providers.openai_provider.create_llm_sdk_http_client",
        MagicMock(return_value=openai_http_client),
    )
    monkeypatch.setattr(
        "tacit.agents.providers.anthropic.create_llm_sdk_http_client",
        MagicMock(return_value=anthropic_http_client),
    )
    openai_settings = _settings(tmp_path, llm_provider="openai", llm_api_key="openai-secret")
    anthropic_settings = _settings(
        tmp_path,
        suffix="anthropic",
        llm_provider="anthropic",
        llm_api_key="anthropic-secret",
    )

    openai_provider = OpenAIProvider(openai_settings)
    anthropic_provider = AnthropicProvider(anthropic_settings)

    openai_client.assert_called_once_with(
        api_key="openai-secret",
        admin_api_key="",
        workload_identity=None,
        base_url="https://api.openai.com/v1",
        organization="",
        project="",
        webhook_secret="",
        default_headers={},
        http_client=openai_http_client,
    )
    anthropic_client.assert_called_once_with(
        api_key="anthropic-secret",
        auth_token=None,
        credentials=None,
        config=None,
        profile=None,
        webhook_key="",
        base_url="https://api.anthropic.com",
        default_headers={},
        http_client=anthropic_http_client,
    )
    assert openai_provider.runtime_ownership.remotes[0].endpoint == "https://api.openai.com/v1"
    assert openai_provider.runtime_ownership.remotes[0].account == "organization:none;project:none"
    assert anthropic_provider.runtime_ownership.remotes[0].endpoint == "https://api.anthropic.com"

    async def close_providers() -> None:
        await asyncio.gather(openai_provider.close(), anthropic_provider.close())

    asyncio.run(close_providers())
    openai_sdk.close.assert_awaited_once()
    anthropic_sdk.close.assert_awaited_once()
    openai_http_client.aclose.assert_awaited_once()
    anthropic_http_client.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_bedrock_chat_rejects_missing_current_identity_before_sdk_use(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_aws_access_key_id="AKIAMISSINGIDENTITY",
        llm_aws_secret_access_key="missing-identity-secret",
    )
    session = MagicMock()
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: session,
    )
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    resources = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=lifecycle,
        cleanup_grace_seconds=0.01,
    )

    async with lifecycle.slot():
        await resources.acquire()
        provider = cast(BedrockProvider, resources.llm())
        with pytest.raises(RuntimeOwnershipError, match="realized credential identity"):
            await provider.chat_text("system", "user")
        await resources.close()

    session.client.assert_not_called()
    session.close.assert_called_once_with()
    assert lifecycle.in_flight == 0


@pytest.mark.parametrize("credential_source", ("assume-role", "web-identity"))
def test_injected_bedrock_temporary_credential_plan_passes_preflight(
    monkeypatch,
    tmp_path,
    credential_source: str,
) -> None:
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
    ):
        monkeypatch.delenv(name, raising=False)

    if credential_source == "assume-role":
        credentials_path = tmp_path / "credentials"
        config_path = tmp_path / "config"
        credentials_path.write_text("[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n")
        config_path.write_text(
            "[profile role-owner]\nrole_arn = arn:aws:iam::123456789012:role/TacitRuntime\nsource_profile = base\n"
        )
        monkeypatch.setenv("AWS_PROFILE", "role-owner")
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    else:
        token_path = tmp_path / "web-identity-token"
        token_path.write_text("captured-token")
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
        monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/TacitRuntime")

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    factory_calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("preflight invoked the provider factory")

    declared = declare_runtime_factory(
        provider_factory,
        ownership=plan.ownership(component="injected_bedrock_plan"),
        factory_kind="provider:llm",
    )
    resources = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=PipelineAdmissionController(1, max_queued=0),
        llm_factory=declared,
    )

    assert resources.llm_ownership(component="test_plan").remotes == plan.ownership(component="test_plan").remotes
    assert factory_calls == 0
