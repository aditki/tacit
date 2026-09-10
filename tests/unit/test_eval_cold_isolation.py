from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import subprocess
import sys
import textwrap
import threading
import weakref
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from tacit import config as config_module
from tacit.archetypes.generated.schema import ArchetypeRetrievalMode
from tacit.cache import llm_cache, metric_cache
from tacit.config import settings
from tacit.pipeline_admission import RuntimeRootDrainStartupError, RuntimeRootOwnerHandle
from tacit.runtime_ownership import RuntimeOwnedFactory, runtime_descriptor_for_backends
from tests.eval import cold_isolation as cold_isolation_module
from tests.eval.cold_isolation import (
    COLD_ENV_CREDENTIAL_NAMES,
    COLD_ENV_PROXY_NAMES,
    LocalEvaluationEndpoints,
    cold_isolation,
)
from tests.eval.gamma_diagnostic_harness import (
    _cache_stats,
    _evaluation_settings,
    _reset_and_snapshot_caches,
)


@contextlib.contextmanager
def _local_http_server(handler: type[BaseHTTPRequestHandler]) -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host = cast(str, server.server_address[0])
        port = cast(int, server.server_address[1])
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_cold_isolation_bypasses_and_restores_ambient_proxies(tmp_path, monkeypatch) -> None:
    target_requests: list[tuple[str, str]] = []
    proxy_requests: list[tuple[str, str]] = []

    class TargetHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            target_requests.append(("GET", self.path))
            body = json.dumps([]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            target_requests.append(("POST", self.path))
            body = json.dumps(
                {
                    "message": {"content": "local response"},
                    "prompt_eval_count": 1,
                    "eval_count": 1,
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    class ProxyHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            proxy_requests.append(("GET", self.path))
            self.send_error(502)

        def do_POST(self) -> None:  # noqa: N802
            proxy_requests.append(("POST", self.path))
            self.send_error(502)

        def log_message(self, format: str, *args: object) -> None:
            return

    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    with _local_http_server(TargetHandler) as target_url, _local_http_server(ProxyHandler) as proxy_url:
        expected_proxies = {name: f"{proxy_url}/{index}" for index, name in enumerate(COLD_ENV_PROXY_NAMES, start=1)}
        for name, value in expected_proxies.items():
            monkeypatch.setenv(name, value)

        endpoints = LocalEvaluationEndpoints(
            grafana_url=target_url,
            llm_api_base=target_url,
            llm_model="local-model",
        )
        with cold_isolation(tmp_path, endpoints=endpoints) as state:
            assert all(name not in os.environ for name in COLD_ENV_PROXY_NAMES)

            # Reintroducing ambient proxies after composition must not redirect
            # clients that belong to the isolated evaluation runtime.
            os.environ.update(expected_proxies)

            async def exercise_local_clients() -> None:
                assert state.dependencies.llm_provider_factory is not None
                provider = state.dependencies.llm_provider_factory()
                result = await provider.chat_text("system", "sensitive local prompt")
                assert result.text == "local response"

                backends = state.dependencies.backend_factory()
                assert len(backends) == 1
                try:
                    assert await backends[0]._client.list_datasources() == []
                finally:
                    await backends[0].close()
                    await state.dependencies.close_resources()

            asyncio.run(exercise_local_clients())

        assert {name: os.environ.get(name) for name in COLD_ENV_PROXY_NAMES} == expected_proxies

    assert target_requests == [("POST", "/api/chat"), ("GET", "/api/datasources")]
    assert proxy_requests == []


def test_cold_isolation_exposes_one_explicit_runtime_graph(tmp_path) -> None:
    admission = None
    with cold_isolation(tmp_path) as state:
        dependencies = state.dependencies
        admission = dependencies.pipeline_admission

        assert dependencies.settings == state.settings
        assert dependencies.pipeline_admission is state.runtime_stores.pipeline_admission()
        assert admission.execution_graph.root_owner_count == 1
        assert admission.execution_graph.root_state == "active"
        assert isinstance(dependencies.backend_factory, RuntimeOwnedFactory)
        assert dependencies.backend_factory.factory_kind == "backend:dashboard"
        assert dependencies.backend_factory.runtime_ownership == runtime_descriptor_for_backends(
            component="evaluation_backend_factory",
            runtime_settings=state.settings,
        )
        assert dependencies.history_store_factory() is state.history_store
        assert dependencies.feedback_store_factory() is state.feedback_store
        assert dependencies.signal_store_factory is not None
        assert dependencies.signal_store_factory() is state.signal_store

    assert admission is not None
    assert admission.execution_graph.root_owner_count == 0
    assert admission.execution_graph.root_state == "closed"


class _FaultingRuntimeGraph:
    def __init__(self) -> None:
        self.root_owner_count = 1
        self.root_state = "active"
        self.provider_manager_count = 0

    def fence_root_owner_without_transport(
        self,
        handle: RuntimeRootOwnerHandle,
        _error: BaseException,
    ) -> None:
        assert handle.generation == 7
        self.root_owner_count = 0
        self.root_state = "fenced"


class _FaultingRuntimeStores:
    def __init__(self, failures: list[BaseException]) -> None:
        self._failures = failures
        self.calls = 0
        self.drained = False
        self._graph = _FaultingRuntimeGraph()
        self.root_handle = RuntimeRootOwnerHandle(
            graph_nonce="cold-test-runtime",
            generation=7,
            owner_id=1,
            _graph=weakref.ref(self._graph),
        )
        self._admission = SimpleNamespace(
            in_flight=0,
            retained=0,
            blocking_in_flight=0,
            service_owner_in_flight=0,
            execution_graph=self._graph,
        )

    def pipeline_admission(self) -> Any:
        return self._admission

    async def shutdown_runtime_services(self, _root_handle: Any) -> None:
        self.calls += 1
        if self._failures:
            raise self._failures.pop(0)
        self.drained = True
        self._graph.root_owner_count = 0
        self._graph.root_state = "closed"


def _invoke_cold_shutdown(runtime_stores: Any, *, inside_running_loop: bool) -> None:
    root_handle = runtime_stores.root_handle
    if not inside_running_loop:
        cold_isolation_module._shutdown_cold_runtime(runtime_stores, root_handle)
        return

    async def invoke() -> None:
        cold_isolation_module._shutdown_cold_runtime(runtime_stores, root_handle)

    asyncio.run(invoke())


@pytest.mark.parametrize("inside_running_loop", [False, True])
def test_cold_isolation_retries_one_runtime_drain_startup_failure(
    inside_running_loop: bool,
) -> None:
    runtime_stores = _FaultingRuntimeStores([RuntimeRootDrainStartupError("startup failed")])

    _invoke_cold_shutdown(runtime_stores, inside_running_loop=inside_running_loop)

    assert runtime_stores.calls == 2
    assert runtime_stores.drained is True
    assert runtime_stores._graph.root_owner_count == 0
    assert runtime_stores._graph.root_state == "closed"


@pytest.mark.parametrize("inside_running_loop", [False, True])
def test_cold_isolation_caps_runtime_drain_startup_retry_at_one(
    inside_running_loop: bool,
) -> None:
    runtime_stores = _FaultingRuntimeStores(
        [
            RuntimeRootDrainStartupError("first startup failed"),
            RuntimeRootDrainStartupError("second startup failed"),
        ]
    )

    with pytest.raises(RuntimeRootDrainStartupError, match="second startup failed"):
        _invoke_cold_shutdown(runtime_stores, inside_running_loop=inside_running_loop)

    assert runtime_stores.calls == 2
    assert runtime_stores.drained is False


@pytest.mark.parametrize("inside_running_loop", [False, True])
def test_cold_isolation_does_not_retry_non_startup_drain_failures(
    inside_running_loop: bool,
) -> None:
    runtime_stores = _FaultingRuntimeStores([ValueError("terminal drain failed")])

    with pytest.raises(ValueError, match="terminal drain failed"):
        _invoke_cold_shutdown(runtime_stores, inside_running_loop=inside_running_loop)

    assert runtime_stores.calls == 1
    assert runtime_stores.drained is False


def test_cold_isolation_retries_transport_start_once_then_reaches_terminal_zero(
    monkeypatch,
) -> None:
    runtime_stores = _FaultingRuntimeStores([])
    real_thread = threading.Thread
    start_attempts = 0

    class StartControlledThread:
        def __init__(self, *, target, args=(), name: str, daemon: bool = False) -> None:
            self._thread = real_thread(target=target, args=args, name=name, daemon=daemon)

        @property
        def ident(self) -> int | None:
            return self._thread.ident

        def is_alive(self) -> bool:
            return self._thread.is_alive()

        def start(self) -> None:
            nonlocal start_attempts
            start_attempts += 1
            if start_attempts == 1:
                raise RuntimeError("transport start unavailable")
            self._thread.start()

        def join(self, timeout: float | None = None) -> None:
            self._thread.join(timeout=timeout)

    monkeypatch.setattr(cold_isolation_module.threading, "Thread", StartControlledThread)

    _invoke_cold_shutdown(runtime_stores, inside_running_loop=True)

    assert start_attempts == 2
    assert runtime_stores.calls == 1
    assert runtime_stores._graph.root_owner_count == 0
    assert runtime_stores._graph.root_state == "closed"


def test_cold_isolation_transport_start_exhaustion_never_orphans_root(monkeypatch) -> None:
    runtime_stores = _FaultingRuntimeStores([])
    start_attempts = 0

    class StartFailingThread:
        def __init__(self, *, target, args=(), name: str, daemon: bool = False) -> None:
            del target, args, name, daemon

        ident = None

        @staticmethod
        def is_alive() -> bool:
            return False

        def start(self) -> None:
            nonlocal start_attempts
            start_attempts += 1
            raise RuntimeError(f"transport start unavailable {start_attempts}")

        def join(self, timeout: float | None = None) -> None:
            del timeout
            raise AssertionError("an unstarted transport must not be joined")

    monkeypatch.setattr(cold_isolation_module.threading, "Thread", StartFailingThread)

    with pytest.raises(RuntimeError, match="transport start unavailable 2"):
        _invoke_cold_shutdown(runtime_stores, inside_running_loop=True)

    assert start_attempts == 2
    assert runtime_stores._graph.root_owner_count == 0
    assert runtime_stores._graph.root_state == "fenced"
    assert cold_isolation_module._cold_runtime_execution_is_impossible(runtime_stores) is False
    assert cold_isolation_module._cold_runtime_at_terminal_zero(runtime_stores) is False


@pytest.mark.parametrize(
    ("root_state", "root_owners", "normal", "retained", "blocking", "service_owner", "providers"),
    [
        ("fenced", 0, 0, 0, 0, 0, 0),
        ("closed", 1, 0, 0, 0, 0, 0),
        ("closed", 0, 1, 0, 0, 0, 0),
        ("closed", 0, 0, 1, 0, 0, 0),
        ("closed", 0, 0, 0, 1, 0, 0),
        ("closed", 0, 0, 0, 0, 1, 0),
        ("closed", 0, 0, 0, 0, 0, 1),
    ],
)
def test_cold_isolation_requires_complete_terminal_zero_before_restoration(
    root_state: str,
    root_owners: int,
    normal: int,
    retained: int,
    blocking: int,
    service_owner: int,
    providers: int,
) -> None:
    graph = SimpleNamespace(
        root_state=root_state,
        root_owner_count=root_owners,
        provider_manager_count=providers,
    )
    admission = SimpleNamespace(
        in_flight=normal,
        retained=retained,
        blocking_in_flight=blocking,
        service_owner_in_flight=service_owner,
        execution_graph=graph,
    )
    runtime_stores = SimpleNamespace(pipeline_admission=lambda: admission)

    assert cold_isolation_module._cold_runtime_execution_is_impossible(runtime_stores) is False
    assert cold_isolation_module._cold_runtime_at_terminal_zero(runtime_stores) is False


def test_cold_isolation_accepts_only_closed_runtime_at_terminal_zero() -> None:
    graph = SimpleNamespace(root_state="closed", root_owner_count=0, provider_manager_count=0)
    admission = SimpleNamespace(
        in_flight=0,
        retained=0,
        blocking_in_flight=0,
        service_owner_in_flight=0,
        execution_graph=graph,
    )
    runtime_stores = SimpleNamespace(pipeline_admission=lambda: admission)

    assert cold_isolation_module._cold_runtime_execution_is_impossible(runtime_stores) is True
    assert cold_isolation_module._cold_runtime_at_terminal_zero(runtime_stores) is True


def test_cold_isolation_no_loop_path_does_not_create_transport_thread(monkeypatch) -> None:
    runtime_stores = _FaultingRuntimeStores([])

    def forbidden_thread(*_args, **_kwargs):
        raise AssertionError("the no-loop path must not create a transport thread")

    monkeypatch.setattr(cold_isolation_module.threading, "Thread", forbidden_thread)

    _invoke_cold_shutdown(runtime_stores, inside_running_loop=False)

    assert runtime_stores.calls == 1
    assert runtime_stores._graph.root_owner_count == 0
    assert runtime_stores._graph.root_state == "closed"


def test_cold_isolation_transport_exhaustion_preserves_process_state_until_restart(
    tmp_path,
    monkeypatch,
) -> None:
    import tacit.archetypes.templates as templates_module
    from tacit import runtime_stores as runtime_stores_module

    disposable = tmp_path / "disposable"
    start_attempts = 0
    created_store: _FaultingRuntimeStores | None = None
    cache_snapshot = object()
    restored_cache_snapshots: list[object] = []

    class FakeSignalStore:
        @staticmethod
        def stats(*, tenant_id: str) -> dict[str, int]:
            assert tenant_id == "default"
            return {"metric_mappings": 0}

    class FakeRuntimeStores(_FaultingRuntimeStores):
        def __init__(self, runtime_settings) -> None:
            nonlocal created_store
            super().__init__([])
            self.settings = runtime_settings
            created_store = self

        def start_runtime_services(self) -> object:
            return self.root_handle

        def signals(self) -> FakeSignalStore:
            return FakeSignalStore()

        def history(self) -> object:
            return object()

        def feedback(self) -> object:
            return object()

    class StartFailingThread:
        def __init__(self, *, target, args=(), name: str, daemon: bool = False) -> None:
            del target, args, name, daemon

        ident = None

        @staticmethod
        def is_alive() -> bool:
            return False

        def start(self) -> None:
            nonlocal start_attempts
            start_attempts += 1
            raise RuntimeError("transport start unavailable")

        def join(self, timeout: float | None = None) -> None:
            del timeout
            raise AssertionError("an unstarted transport must not be joined")

    fake_cache = SimpleNamespace(invalidate=lambda: None, reset_stats=lambda: None)
    fake_dependencies = SimpleNamespace(llm_cache=fake_cache)
    monkeypatch.setattr(cold_isolation_module, "_capture_global_caches", lambda: cache_snapshot)
    monkeypatch.setattr(
        cold_isolation_module,
        "_restore_global_caches",
        restored_cache_snapshots.append,
    )
    monkeypatch.setattr(cold_isolation_module, "_reset_caches", lambda *_args: None)
    monkeypatch.setattr(templates_module, "reload_archetypes", lambda: None)
    monkeypatch.setattr(runtime_stores_module, "RuntimeStores", FakeRuntimeStores)
    monkeypatch.setattr(
        cold_isolation_module,
        "_evaluation_dependencies",
        lambda *_args, **_kwargs: fake_dependencies,
    )
    monkeypatch.setattr(cold_isolation_module.tempfile, "mkdtemp", lambda **_kwargs: str(disposable))
    monkeypatch.setattr(cold_isolation_module.threading, "Thread", StartFailingThread)
    monkeypatch.setenv("OPENAI_API_KEY", "caller-owned-secret")
    monkeypatch.setenv("TACIT_ARCHETYPES_PATH", "caller-owned-archetypes.yaml")

    async def invoke() -> None:
        with cold_isolation() as state:
            state.workdir.mkdir(parents=True, exist_ok=True)
            (state.workdir / "retained.db").write_text("owned until drain\n")

    cleanup_error: BaseException | None = None
    try:
        asyncio.run(invoke())
    except BaseException as exc:
        cleanup_error = exc

    assert isinstance(cleanup_error, RuntimeError)
    assert "transport start unavailable" in str(cleanup_error)
    assert start_attempts == 2
    assert created_store is not None
    assert created_store._graph.root_owner_count == 0
    assert created_store._graph.root_state == "fenced"
    assert "OPENAI_API_KEY" not in os.environ
    assert os.environ["TACIT_ARCHETYPES_PATH"] == str(disposable / "learned_archetypes.yaml")
    assert restored_cache_snapshots == []
    assert disposable.exists()
    assert (disposable / "retained.db").exists()


def test_running_loop_transport_exhaustion_with_active_provider_requires_process_restart(
    tmp_path,
) -> None:
    workdir = tmp_path / "active-provider-runtime"
    script = textwrap.dedent("""
        import asyncio
        import json
        import os
        import pathlib
        import sys

        from tests.eval import cold_isolation as cold

        workdir = pathlib.Path(sys.argv[1])
        cache_snapshot = object()
        restored_caches = []
        start_attempts = 0
        state_ref = None

        class StartFailingThread:
            def __init__(self, *, target, args=(), name: str, daemon: bool = False) -> None:
                del target, args, name, daemon

            ident = None

            @staticmethod
            def is_alive() -> bool:
                return False

            def start(self) -> None:
                global start_attempts
                start_attempts += 1
                raise RuntimeError("transport start unavailable")

            def join(self, timeout=None) -> None:
                del timeout
                raise AssertionError("an unstarted transport must not be joined")

        cold._capture_global_caches = lambda: cache_snapshot
        cold._restore_global_caches = restored_caches.append
        cold._reset_caches = lambda *_args: None
        cold.tempfile.mkdtemp = lambda **_kwargs: str(workdir)
        os.environ["OPENAI_API_KEY"] = "caller-owned-secret"
        os.environ["HTTPS_PROXY"] = "http://caller-owned-proxy.invalid"
        os.environ["TACIT_ARCHETYPES_PATH"] = "caller-owned-archetypes.yaml"

        async def exercise() -> None:
            global state_ref
            with cold.cold_isolation() as state:
                state_ref = state
                (state.workdir / "retained.db").write_text("owned until restart\\n")
                await state.dependencies.acquire_resources()
                admission = state.runtime_stores.pipeline_admission()
                assert admission.service_owner_in_flight == 1
                assert admission.execution_graph.provider_manager_count == 1
                cold.threading.Thread = StartFailingThread

        failure = None
        try:
            asyncio.run(exercise())
        except BaseException as exc:
            failure = exc

        assert state_ref is not None
        admission = state_ref.runtime_stores.pipeline_admission()
        print(json.dumps({
            "error_type": type(failure).__name__ if failure is not None else "",
            "error": str(failure) if failure is not None else "",
            "start_attempts": start_attempts,
            "root_state": admission.execution_graph.root_state,
            "root_owners": admission.execution_graph.root_owner_count,
            "service_owners": admission.service_owner_in_flight,
            "provider_managers": admission.execution_graph.provider_manager_count,
            "credential_present": "OPENAI_API_KEY" in os.environ,
            "proxy_present": "HTTPS_PROXY" in os.environ,
            "archetypes_path": os.environ.get("TACIT_ARCHETYPES_PATH"),
            "caches_restored": len(restored_caches),
            "workdir_exists": workdir.exists(),
            "retained_storage_exists": (workdir / "retained.db").exists(),
        }))
        """)

    result = subprocess.run(
        [sys.executable, "-c", script, str(workdir)],
        cwd=Path(__file__).parents[2],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    observed = json.loads(result.stdout.strip().splitlines()[-1])

    assert observed == {
        "error_type": "RuntimeError",
        "error": "transport start unavailable",
        "start_attempts": 2,
        "root_state": "fenced",
        "root_owners": 0,
        "service_owners": 1,
        "provider_managers": 1,
        "credential_present": False,
        "proxy_present": False,
        "archetypes_path": str(workdir / "learned_archetypes.yaml"),
        "caches_restored": 0,
        "workdir_exists": True,
        "retained_storage_exists": True,
    }


def test_cold_isolation_drains_before_restoring_ambient_state_or_deleting_storage(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "caller-owned-secret")
    original_shutdown = cold_isolation_module._shutdown_cold_runtime
    observed: list[tuple[bool, bool]] = []

    def observe_shutdown(runtime_stores, root_handle) -> None:
        workdir = Path(runtime_stores.settings.history_db_path).parent
        observed.append(("OPENAI_API_KEY" not in os.environ, workdir.exists()))
        original_shutdown(runtime_stores, root_handle)

    monkeypatch.setattr(cold_isolation_module, "_shutdown_cold_runtime", observe_shutdown)

    with cold_isolation() as state:
        workdir = state.workdir
        assert workdir.exists()

    assert observed == [(True, True)]
    assert os.environ["OPENAI_API_KEY"] == "caller-owned-secret"
    assert not workdir.exists()


def test_cold_isolation_denies_unselected_remote_integrations_before_path_access(
    tmp_path,
    monkeypatch,
) -> None:
    hostile_quarantine = tmp_path / "production-generated-archetypes"
    hostile_quarantine.mkdir()
    (hostile_quarantine / "artifact.yaml").write_text("must-not-be-read: true\n")
    original_read_text = Path.read_text
    hostile_reads: list[Path] = []

    def guarded_read_text(path: Path, *args, **kwargs):
        if hostile_quarantine in path.parents or path == hostile_quarantine:
            hostile_reads.append(path)
            raise AssertionError("cold isolation read production generated-archetype state")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    selected_grafana = "http://127.0.0.1:3001"
    selected_ollama = "http://127.0.0.1:11434"
    selected_model = "gamma-local-model"
    hostile_settings = {
        "grafana_enabled": True,
        "grafana_url": selected_grafana,
        "grafana_public_url": "https://production-grafana.example",
        "grafana_api_key": "production-grafana-secret",
        "llm_provider": "ollama",
        "llm_api_base": selected_ollama,
        "llm_model": selected_model,
        "llm_api_key": "production-llm-secret",
        "llm_azure_deployment": "production-azure-deployment",
        "llm_bedrock_model_id": "production-bedrock-model",
        "llm_bedrock_role_arn": "arn:aws:iam::123456789012:role/production",
        "llm_aws_access_key_id": "production-access-key",
        "llm_aws_secret_access_key": "production-secret-key",
        "llm_aws_session_token": "production-session-token",
        "signalfx_enabled": True,
        "signalfx_api_token": "production-signalfx-secret",
        "pagerduty_api_token": "production-pagerduty-secret",
        "pagerduty_base_url": "https://production-pagerduty.example",
        "slack_bot_token": "production-slack-bot-secret",
        "slack_app_token": "production-slack-app-secret",
        "slack_signing_secret": "production-slack-signing-secret",
        "context_provider": "rag_api",
        "context_api_key": "production-context-secret",
        "context_mcp_server_url": "https://production-mcp.example",
        "context_a2a_agent_url": "https://production-a2a.example",
        "context_rag_api_url": "https://production-rag.example",
        "api_auth_enabled": True,
        "api_auth_key": "production-api-secret",
        "learned_archetypes_generation_enabled": True,
        "learned_archetypes_automatic_registration_enabled": True,
        "learned_archetypes_normal_retrieval_enabled": True,
        "learned_archetypes_retrieval_mode": ArchetypeRetrievalMode.CURATED_WITH_EXPERIMENTAL_EXACT_SCOPE,
        "learned_archetypes_quarantine_path": str(hostile_quarantine),
        "learning_auto_register_archetype": True,
    }
    for name, value in hostile_settings.items():
        if name in {"llm_aws_access_key_id", "llm_aws_secret_access_key", "llm_aws_session_token"}:
            # This fixture deliberately installs one hostile credential tuple
            # atomically; assignment validation correctly rejects its partial
            # intermediate states.
            monkeypatch.setitem(settings.__dict__, name, value)
        else:
            monkeypatch.setattr(settings, name, value)
    for name in COLD_ENV_CREDENTIAL_NAMES:
        monkeypatch.setenv(name, f"hostile-{name.casefold()}")

    isolated_workdir = tmp_path / "isolated"
    endpoints = LocalEvaluationEndpoints(
        grafana_url=selected_grafana,
        llm_api_base=selected_ollama,
        llm_model=selected_model,
    )
    with cold_isolation(isolated_workdir, endpoints=endpoints) as state:
        isolated = state.settings
        assert isolated.grafana_enabled is True
        assert isolated.grafana_url == selected_grafana
        assert isolated.llm_provider == "ollama"
        assert isolated.llm_api_base == selected_ollama
        assert isolated.llm_model == selected_model

        assert isolated.grafana_public_url == ""
        assert isolated.grafana_api_key == ""
        assert isolated.llm_api_key == ""
        assert isolated.llm_azure_deployment == ""
        assert isolated.llm_bedrock_model_id == ""
        assert isolated.llm_bedrock_role_arn == ""
        assert isolated.llm_aws_access_key_id == ""
        assert isolated.llm_aws_secret_access_key == ""
        assert isolated.signalfx_enabled is False
        assert isolated.signalfx_api_token == ""
        assert isolated.pagerduty_api_token == ""
        assert isolated.pagerduty_base_url == "http://127.0.0.1:9"
        assert isolated.slack_bot_token == ""
        assert isolated.slack_app_token == ""
        assert isolated.slack_signing_secret == ""
        assert isolated.context_provider == "none"
        assert isolated.context_api_key == ""
        assert isolated.context_mcp_server_url == ""
        assert isolated.context_a2a_agent_url == ""
        assert isolated.context_rag_api_url == ""
        assert isolated.api_auth_enabled is False
        assert isolated.api_auth_key == ""
        assert isolated.learned_archetypes_generation_enabled is False
        assert isolated.learned_archetypes_automatic_registration_enabled is False
        assert isolated.learned_archetypes_normal_retrieval_enabled is False
        assert isolated.learned_archetypes_retrieval_mode is ArchetypeRetrievalMode.CURATED_ONLY
        assert Path(isolated.learned_archetypes_quarantine_path).is_relative_to(isolated_workdir)
        assert isolated.learning_auto_register_archetype is False
        assert all(name not in os.environ for name in COLD_ENV_CREDENTIAL_NAMES)

        backends = state.dependencies.backend_factory()
        assert [backend.name for backend in backends] == ["grafana"]
        assert backends[0].runtime_settings is not None
        assert backends[0].runtime_settings.grafana_url == selected_grafana
        assert state.dependencies.context_provider_factory is not None
        assert state.dependencies.context_provider_factory() is None
        assert state.dependencies.llm_provider_factory is not None
        provider = state.dependencies.llm_provider_factory()
        assert provider._base_url == selected_ollama
        asyncio.run(state.dependencies.close_resources())

    assert hostile_reads == []
    assert all(os.environ[name].startswith("hostile-") for name in COLD_ENV_CREDENTIAL_NAMES)


def test_gamma_cache_helpers_reset_and_report_dependency_owned_llm_cache(tmp_path) -> None:
    metric_cache.invalidate()
    metric_cache.reset_stats()
    llm_cache.invalidate()
    llm_cache.reset_stats()
    metric_cache.set("metric", ["value"])

    with cold_isolation(tmp_path) as state:
        scoped_llm_cache = state.dependencies.llm_cache
        llm_cache.set("global", "must-survive")
        scoped_llm_cache.set("scoped", "must-clear")
        assert scoped_llm_cache.get("missing") is None

        _reset_and_snapshot_caches(state)

        assert metric_cache.size == 0
        assert metric_cache.stats == {"hits": 0, "misses": 0, "size": 0}
        assert scoped_llm_cache.stats == {"hits": 0, "misses": 0, "size": 0}
        assert llm_cache.get("global") == "must-survive"
        assert _cache_stats(state) == {
            "metric": {"hits": 0, "misses": 0, "size": 0},
            "llm": {"hits": 0, "misses": 0, "size": 0},
        }


@pytest.mark.parametrize(
    ("grafana_url", "ollama_url"),
    [
        ("https://production-grafana.example", "http://127.0.0.1:11434"),
        ("http://127.0.0.1:3001", "https://production-llm.example"),
    ],
)
def test_gamma_settings_reject_remote_endpoints_before_mutating_process_settings(
    grafana_url,
    ollama_url,
) -> None:
    previous = (
        settings.grafana_url,
        settings.llm_provider,
        settings.llm_api_base,
        settings.llm_model,
    )

    with pytest.raises(ValueError, match="local loopback endpoint"):
        with _evaluation_settings(grafana_url, ollama_url, "gamma-model"):
            raise AssertionError("remote endpoint must fail before entering the context")

    assert (
        settings.grafana_url,
        settings.llm_provider,
        settings.llm_api_base,
        settings.llm_model,
    ) == previous


@pytest.mark.parametrize(
    ("setting_name", "remote_url"),
    [
        ("grafana_url", "https://production-grafana.example"),
        ("llm_api_base", "https://production-llm.example"),
    ],
)
def test_cold_isolation_rejects_remote_selected_endpoints_before_storage(
    tmp_path,
    monkeypatch,
    setting_name,
    remote_url,
) -> None:
    workdir = tmp_path / "rejected"
    values = {
        "grafana_url": "http://127.0.0.1:3001",
        "llm_api_base": "http://127.0.0.1:11434",
        "llm_model": "gamma-model",
    }
    values[setting_name] = remote_url
    endpoints = LocalEvaluationEndpoints(**values)

    with pytest.raises(ValueError, match="local loopback endpoint"):
        with cold_isolation(workdir, endpoints=endpoints):
            raise AssertionError("remote endpoint must fail before entering the context")

    assert not workdir.exists()


def test_cold_isolation_rejects_endpoints_before_any_side_effect(tmp_path, monkeypatch) -> None:
    workdir = tmp_path / "must-not-exist"
    calls: list[str] = []

    def forbidden(label: str):
        def fail(*_args, **_kwargs):
            calls.append(label)
            raise AssertionError(f"{label} happened before endpoint validation")

        return fail

    monkeypatch.setattr(cold_isolation_module, "_remove_ambient_credentials", forbidden("environment"))
    monkeypatch.setattr(cold_isolation_module, "_remove_ambient_proxies", forbidden("proxy environment"))
    monkeypatch.setattr(cold_isolation_module, "_evaluation_dependencies", forbidden("client construction"))
    monkeypatch.setattr(Path, "mkdir", forbidden("workdir creation"))
    monkeypatch.setattr(Path, "rglob", forbidden("file traversal"))
    monkeypatch.setattr(subprocess, "run", forbidden("subprocess"))
    monkeypatch.setattr(socket, "create_connection", forbidden("network"))

    endpoints = LocalEvaluationEndpoints(
        grafana_url="https://production-grafana.example",
        llm_api_base="http://127.0.0.1:11434",
        llm_model="gamma-model",
    )
    with pytest.raises(ValueError, match="local loopback endpoint"):
        with cold_isolation(workdir, endpoints=endpoints):
            raise AssertionError("invalid endpoint entered cold isolation")

    assert calls == []
    assert not workdir.exists()


def test_cold_isolation_rejects_endpoints_before_temporary_directory_creation(monkeypatch) -> None:
    calls: list[str] = []

    def forbidden_mkdtemp(*_args, **_kwargs):
        calls.append("temporary directory")
        raise AssertionError("temporary directory created before endpoint validation")

    monkeypatch.setattr(cold_isolation_module.tempfile, "mkdtemp", forbidden_mkdtemp)
    endpoints = LocalEvaluationEndpoints(grafana_url="https://production-grafana.example")

    with pytest.raises(ValueError, match="local loopback endpoint"):
        with cold_isolation(endpoints=endpoints):
            raise AssertionError("invalid endpoint entered cold isolation")

    assert calls == []


def test_cold_isolation_rejects_wildcard_tenant_before_workdir_creation(tmp_path) -> None:
    workdir = tmp_path / "wildcard-must-not-exist"

    with pytest.raises(ValueError, match="concrete tenant"):
        with cold_isolation(workdir, tenant_id="*"):
            raise AssertionError("wildcard tenant entered cold isolation")

    assert not workdir.exists()


def test_cold_isolation_restores_caller_owned_global_caches(tmp_path) -> None:
    metric_cache.invalidate()
    metric_cache.reset_stats()
    llm_cache.invalidate()
    llm_cache.reset_stats()
    metric_cache.set("caller-metric", ["value"])
    llm_cache.set("caller-llm", "value")
    with metric_cache._lock, llm_cache._lock:
        metric_before = (
            metric_cache._store.copy(),
            metric_cache._total_weight,
            metric_cache._hits,
            metric_cache._misses,
        )
        llm_before = (llm_cache._store.copy(), llm_cache._total_weight, llm_cache._hits, llm_cache._misses)

    with cold_isolation(tmp_path):
        assert metric_cache.size == 0
        assert llm_cache.size == 0
        metric_cache.set("evaluation-only", ["temporary"])
        llm_cache.set("evaluation-only", "temporary")

    with metric_cache._lock, llm_cache._lock:
        metric_after = (
            metric_cache._store.copy(),
            metric_cache._total_weight,
            metric_cache._hits,
            metric_cache._misses,
        )
        llm_after = (llm_cache._store.copy(), llm_cache._total_weight, llm_cache._hits, llm_cache._misses)
    assert metric_after == metric_before
    assert llm_after == llm_before


def test_cold_isolation_does_not_reload_ambient_configuration(tmp_path, monkeypatch) -> None:
    def fail_if_loaded():
        raise AssertionError("cold isolation must not read ambient YAML configuration")

    monkeypatch.setattr(config_module, "_load_yaml_config", fail_if_loaded)

    with cold_isolation(tmp_path) as state:
        assert state.settings.history_db_path == str(tmp_path / "history.db")


def test_cold_isolation_offline_mode_does_not_require_ambient_endpoints(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(settings, "grafana_url", "")
    monkeypatch.setattr(settings, "llm_api_base", "")
    monkeypatch.setattr(settings, "llm_api_key", "production-secret")
    monkeypatch.setenv("OPENAI_API_KEY", "production-openai-secret")

    with cold_isolation(tmp_path) as state:
        assert state.settings.grafana_enabled is False
        assert state.settings.llm_provider == "ollama"
        assert state.settings.llm_api_base == "http://127.0.0.1:9"
        assert state.settings.llm_api_key == ""
        assert state.dependencies.backend_factory() == []
        assert "OPENAI_API_KEY" not in os.environ


def test_cold_isolation_serializes_overlapping_process_contexts(tmp_path) -> None:
    first_entered = threading.Event()
    release_first = threading.Event()
    second_attempted = threading.Event()
    second_entered = threading.Event()
    failures: list[BaseException] = []

    def first() -> None:
        try:
            with cold_isolation(tmp_path / "first"):
                first_entered.set()
                assert release_first.wait(timeout=5)
        except BaseException as exc:
            failures.append(exc)

    def second() -> None:
        try:
            assert first_entered.wait(timeout=5)
            second_attempted.set()
            with cold_isolation(tmp_path / "second"):
                second_entered.set()
        except BaseException as exc:
            failures.append(exc)

    first_thread = threading.Thread(target=first)
    second_thread = threading.Thread(target=second)
    first_thread.start()
    second_thread.start()
    assert second_attempted.wait(timeout=5)
    assert not second_entered.wait(timeout=0.1)
    release_first.set()
    first_thread.join(timeout=5)
    second_thread.join(timeout=5)

    assert not first_thread.is_alive()
    assert not second_thread.is_alive()
    assert failures == []
    assert second_entered.is_set()


def test_cold_isolation_rejects_cross_task_overlap_before_global_mutation(tmp_path) -> None:
    async def exercise() -> None:
        first_entered = asyncio.Event()
        release_first = asyncio.Event()

        async def first() -> None:
            with cold_isolation(tmp_path / "first-task"):
                first_entered.set()
                await release_first.wait()

        async def second() -> None:
            await first_entered.wait()
            with pytest.raises(RuntimeError, match="cannot overlap"):
                with cold_isolation(tmp_path / "second-task"):
                    raise AssertionError("overlapping task entered cold isolation")
            release_first.set()

        await asyncio.gather(first(), second())

    asyncio.run(exercise())


def test_cold_isolation_fails_closed_when_cache_reset_fails_before_yield(tmp_path, monkeypatch) -> None:
    entered: list[object] = []
    original_store = metric_cache._store.copy()
    original_weight = metric_cache._total_weight
    original_hits = metric_cache._hits
    original_misses = metric_cache._misses

    def fail_reset() -> None:
        raise RuntimeError("synthetic metric cache reset failure")

    monkeypatch.setattr(metric_cache, "invalidate", fail_reset)

    with pytest.raises(RuntimeError, match="metric cache reset failure"):
        with cold_isolation(tmp_path / "cache-reset-failure") as state:
            entered.append(state)

    assert entered == []
    assert metric_cache._store == original_store
    assert metric_cache._total_weight == original_weight
    assert metric_cache._hits == original_hits
    assert metric_cache._misses == original_misses
