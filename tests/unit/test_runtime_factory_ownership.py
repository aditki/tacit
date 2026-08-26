from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

from tacit.agents.providers.anthropic import AnthropicProvider
from tacit.agents.providers.base import LLMProvider, LLMResult
from tacit.agents.providers.bedrock import BedrockProvider
from tacit.agents.providers.openai_provider import OpenAIProvider
from tacit.backends.base import DashboardBackend
from tacit.config import Settings
from tacit.context.base import ContextProvider
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies, declare_backend_factory
from tacit.errors import PipelineAdmissionRejected, PipelineExecutionError, RuntimeOwnershipError
from tacit.models.schemas import DashRequest
from tacit.pipeline.runner import run_pipeline
from tacit.runtime_ownership import (
    BedrockCredentialIdentity,
    credential_fingerprint,
    declare_runtime_factory,
    runtime_descriptor_for_backends,
    runtime_descriptor_for_provider,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStores


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

    class HistoryProbe:
        runtime_ownership = runtime_descriptor_for_store(
            component="backend_preflight_history",
            runtime_settings=active,
            database_role="history",
            database_path=active.history_db_path,
        )

        def start(self, *_args, **_kwargs):
            nonlocal history_starts
            history_starts += 1
            return "inv-backend-owner"

        def finish(self, *_args, **_kwargs):
            nonlocal history_finishes
            history_finishes += 1

    def backend_factory() -> list[DashboardBackend]:
        nonlocal backend_calls
        backend_calls += 1
        return [cast(DashboardBackend, _BackendProbe(foreign))]

    dependencies = build_pipeline_dependencies(
        active,
        stores=RuntimeStores(active),
        backend_factory=_backend_factory(backend_factory, active),
        history_store_factory=declare_runtime_factory(
            HistoryProbe,
            ownership=HistoryProbe.runtime_ownership,
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
        if capability == "llm":
            with pytest.raises(RuntimeOwnershipError):
                await dependencies.acquire_resources()
        else:
            await dependencies.acquire_resources()
            assert dependencies.context_provider_factory is not None
            try:
                with pytest.raises(RuntimeOwnershipError):
                    dependencies.context_provider_factory()
            finally:
                await dependencies.close_resources()

    for _ in range(100):
        if product.closed and dependencies.pipeline_admission.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert product.closed is True
    assert dependencies.pipeline_admission.in_flight == 0


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


def test_bedrock_constructor_pins_session_without_creating_clients(monkeypatch, tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_model_id="",
        llm_model="claude-sonnet-4-20250514",
        llm_aws_access_key_id="AKIATESTFIXTURE",
        llm_aws_secret_access_key="test-fixture-secret",
    )
    session = MagicMock()
    build_session = MagicMock(return_value=session)
    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build_session)

    provider = BedrockProvider(runtime_settings)

    build_session.assert_called_once()
    assert build_session.call_args.args == ()
    credential_plan = build_session.call_args.kwargs["credential_plan"]
    assert credential_plan.runtime_settings == provider.runtime_settings
    session.client.assert_not_called()
    assert provider.runtime_ownership.remotes


def test_bedrock_creates_only_declared_runtime_client_on_first_use(
    monkeypatch,
    tmp_path,
) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_model_id="",
        llm_model="runtime-discovered-model",
        llm_aws_access_key_id="AKIATESTFIXTURE",
        llm_aws_secret_access_key="test-fixture-secret",
    )
    runtime_client = MagicMock()
    runtime_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "ok"}]}},
    }
    session = MagicMock()
    session.client.return_value = runtime_client
    build_session = MagicMock(return_value=session)
    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build_session)

    provider = BedrockProvider(runtime_settings)
    build_session.assert_called_once()
    assert build_session.call_args.args == ()
    credential_plan = build_session.call_args.kwargs["credential_plan"]
    assert credential_plan.runtime_settings == provider.runtime_settings
    session.client.assert_not_called()

    result = provider._converse("system", "user", 0.2)

    assert result.text == "ok"
    session.client.assert_called_once_with(
        "bedrock-runtime",
        endpoint_url="https://bedrock-runtime.us-east-1.amazonaws.com",
    )


@pytest.mark.asyncio
async def test_sync_factory_realization_runs_without_blocking_event_loop() -> None:
    from tacit.runtime_ownership import realize_runtime_factory_async

    started = threading.Event()
    release = threading.Event()
    product = object()

    def blocking_factory() -> object:
        started.set()
        assert release.wait(timeout=1.0)
        return product

    realization = asyncio.create_task(realize_runtime_factory_async(blocking_factory))
    assert await asyncio.to_thread(started.wait, 1.0)

    heartbeat = asyncio.Event()
    asyncio.get_running_loop().call_later(0.01, heartbeat.set)
    await asyncio.wait_for(heartbeat.wait(), timeout=0.2)

    release.set()
    assert await asyncio.wait_for(realization, timeout=0.2) is product


@pytest.mark.asyncio
async def test_bedrock_credential_resolution_preserves_event_loop_heartbeat(monkeypatch, tmp_path) -> None:
    from types import SimpleNamespace
    from unittest.mock import patch

    from tacit.runtime_ownership import realize_runtime_factory_async

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    started = threading.Event()
    release = threading.Event()
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[default]\n" "aws_access_key_id = AKIAHEARTBEAT\n" "aws_secret_access_key = heartbeat-secret\n"
    )
    credentials = MagicMock(method="shared-credentials-file")

    def freeze_credentials():
        started.set()
        assert release.wait(timeout=1.0)
        return SimpleNamespace(
            access_key="AKIAHEARTBEAT",
            secret_key="heartbeat-secret",
            token="heartbeat-token",
        )

    credentials.get_frozen_credentials.side_effect = freeze_credentials
    discovery_session = MagicMock()
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [discovery_session, pinned_session]

    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        patch(
            "tacit.runtime_ownership.capture_bedrock_environment",
            return_value={
                "HOME": str(tmp_path),
                "AWS_SHARED_CREDENTIALS_FILE": str(credentials_path),
                "AWS_CONFIG_FILE": str(tmp_path / "missing-config"),
                "AWS_EC2_METADATA_DISABLED": "true",
            },
        ),
    ):
        realization = asyncio.create_task(realize_runtime_factory_async(lambda: BedrockProvider(runtime_settings)))
        assert await asyncio.to_thread(started.wait, 1.0)

        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_later(0.01, heartbeat.set)
        await asyncio.wait_for(heartbeat.wait(), timeout=0.2)

        release.set()
        provider = await asyncio.wait_for(realization, timeout=0.2)

    assert provider.runtime_ownership.remotes
    credentials.get_frozen_credentials.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("selector", ["profile", "default"])
async def test_dependency_acquire_admits_prepared_bedrock_snapshot_without_blocking(
    monkeypatch,
    tmp_path,
    selector: str,
) -> None:
    from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime

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
        f"[{selected_profile}]\n" "aws_access_key_id = AKIAPREPARED\n" "aws_secret_access_key = prepared-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))

    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    session = MagicMock()
    realization_threads: list[int] = []
    prepared_identity = BedrockCredentialIdentity(
        account=f"access-key:{credential_fingerprint('AKIAPREPARED')}",
        credential_fingerprint=credential_fingerprint("prepared-secret-snapshot"),
        uses_sts=False,
    )

    def build_session(*_args, **_kwargs):
        realization_threads.append(threading.get_ident())
        return _ResolvedBedrockRuntime(
            session=session,
            credential_identity=prepared_identity,
        )

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
        realized_remote = provider.runtime_ownership.remotes[0]
        assert realized_remote.account == prepared_identity.account
        assert realized_remote.credential_fingerprint == prepared_identity.credential_fingerprint
        await dependencies.close_resources()

    assert realization_threads
    assert realization_threads[0] != threading.get_ident()
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
async def test_dependency_acquire_admits_role_profile_from_one_frozen_plan(monkeypatch, tmp_path) -> None:
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
    monkeypatch.setenv("AWS_PROFILE", "role-owner")
    credentials_path = tmp_path / "role-profile-credentials"
    config_path = tmp_path / "role-profile-config"
    credentials_path.write_text("[base]\naws_access_key_id = AKIABASE\naws_secret_access_key = base-secret\n")
    config_path.write_text(
        "[profile role-owner]\n" "role_arn = arn:aws:iam::123456789012:role/TacitRuntime\n" "source_profile = base\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    credentials = MagicMock(method="assume-role")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="ASIAROLEPROFILE",
        secret_key="role-profile-secret",
        token="role-profile-token",
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
            provider = dependencies.llm_provider_factory()
            remotes = {remote.provider: remote for remote in provider.runtime_ownership.remotes}
            assert set(remotes) == {"llm:bedrock", "llm:bedrock:sts"}
            assert remotes["llm:bedrock"].account == "arn:aws:iam::123456789012:role/tacitruntime"
            assert remotes["llm:bedrock:sts"].account == remotes["llm:bedrock"].account
            await dependencies.close_resources()

    discovery_kwargs = mock_boto3.Session.call_args_list[0].kwargs
    assert set(discovery_kwargs) == {"botocore_session"}
    core_session = discovery_kwargs["botocore_session"]
    assert core_session.get_config_variable("profile") == "role-owner"
    assert core_session.get_config_variable("region") == "us-east-1"
    assert "env" not in {
        str(getattr(provider, "METHOD", "")) for provider in core_session.get_component("credential_provider").providers
    }
    credentials.get_frozen_credentials.assert_called_once_with()


@pytest.mark.asyncio
async def test_dependency_acquire_rejects_profile_mutation_after_dependency_construction(monkeypatch, tmp_path) -> None:
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
    credentials_path.write_text(
        "[owner-a]\n" "aws_access_key_id = AKIAOWNERA\n" "aws_secret_access_key = owner-a-secret\n"
    )
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

        monkeypatch.setenv("AWS_PROFILE", "owner-b")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAOWNERB")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "owner-b-secret")

        async with dependencies.pipeline_admission.slot():
            with pytest.raises(RuntimeOwnershipError, match="environment changed"):
                await dependencies.acquire_resources()

    mock_boto3.Session.assert_not_called()
    credentials.get_frozen_credentials.assert_not_called()


@pytest.mark.asyncio
async def test_dependency_acquire_rejects_environment_credentials_added_after_plan_capture(
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
        "[default]\n" "aws_access_key_id = AKIACAPTURED\n" "aws_secret_access_key = captured-secret\n"
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

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIALATE")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "late-secret")

        async with dependencies.pipeline_admission.slot():
            with pytest.raises(RuntimeOwnershipError, match="environment changed"):
                await dependencies.acquire_resources()

    mock_boto3.Session.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("path_variable", ["AWS_SHARED_CREDENTIALS_FILE", "AWS_CONFIG_FILE"])
async def test_dependency_acquire_rejects_credential_source_path_changes_before_sdk_use(
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

        replacement = second_credentials if path_variable == "AWS_SHARED_CREDENTIALS_FILE" else second_config
        monkeypatch.setenv(path_variable, str(replacement))

        async with dependencies.pipeline_admission.slot():
            with pytest.raises(RuntimeOwnershipError, match="environment changed"):
                await dependencies.acquire_resources()

    mock_boto3.Session.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("source_name", ["credentials", "config"])
async def test_dependency_acquire_rejects_credential_source_content_changes_before_sdk_use(
    monkeypatch,
    tmp_path,
    source_name: str,
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
    credentials_path.write_text("[default]\naws_access_key_id = AKIAFIRST\naws_secret_access_key = first\n")
    config_path.write_text("[default]\nregion = us-east-1\n")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
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

        changed_path = credentials_path if source_name == "credentials" else config_path
        changed_path.write_text(changed_path.read_text() + "# changed\n")

        async with dependencies.pipeline_admission.slot():
            with pytest.raises(RuntimeOwnershipError, match="credential source changed"):
                await dependencies.acquire_resources()

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
        "[default]\n" "aws_access_key_id = AKIACAPTURED\n" "aws_secret_access_key = captured-secret\n"
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


def test_bedrock_web_identity_uses_captured_token_after_final_plan_check(
    monkeypatch,
    tmp_path,
) -> None:
    from types import SimpleNamespace
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
    original_verify = BedrockCredentialPlan.verify_unchanged
    discovery_session = MagicMock()
    credentials = MagicMock(method="assume-role-with-web-identity")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="ASIAWEBIDENTITY",
        secret_key="web-identity-secret",
        token="web-identity-session-token",
    )
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    observed_token = ""

    def verify_then_replace_token(self: BedrockCredentialPlan) -> None:
        original_verify(self)
        token_path.write_text("changed-token")

    def build_session(**kwargs):
        nonlocal observed_token
        if "botocore_session" not in kwargs:
            return pinned_session
        core_session = kwargs["botocore_session"]
        frozen_token_path = Path(core_session.full_config["profiles"]["default"]["web_identity_token_file"])
        observed_token = frozen_token_path.read_text()
        return discovery_session

    monkeypatch.setattr(BedrockCredentialPlan, "verify_unchanged", verify_then_replace_token)
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = build_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(credential_plan=plan)

    assert observed_token == "captured-token"
    assert resolved.session is pinned_session
    credentials.get_frozen_credentials.assert_called_once_with()


def test_bedrock_profile_web_identity_uses_winning_captured_token_after_final_check(
    monkeypatch,
    tmp_path,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session
    from tacit.runtime_ownership import BedrockCredentialPlan

    config_token_path = tmp_path / "config-web-identity-token"
    config_token_path.write_text("config-token")
    credentials_token_path = tmp_path / "credentials-web-identity-token"
    credentials_token_path.write_text("captured-credentials-token")
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[workload]\n"
        "role_arn = arn:aws:iam::111111111111:role/CredentialsRole\n"
        f"web_identity_token_file = {credentials_token_path}\n"
    )
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile workload]\n"
        "role_arn = arn:aws:iam::222222222222:role/ConfigRole\n"
        f"web_identity_token_file = {config_token_path}\n"
    )
    monkeypatch.setenv("AWS_PROFILE", "workload")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    original_verify = BedrockCredentialPlan.verify_unchanged
    discovery_session = MagicMock()
    credentials = MagicMock(method="assume-role-with-web-identity")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="ASIAWEBIDENTITY",
        secret_key="web-identity-secret",
        token="web-identity-session-token",
    )
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    observed_token = ""

    def verify_then_replace_token(self: BedrockCredentialPlan) -> None:
        original_verify(self)
        credentials_token_path.write_text("changed-credentials-token")

    def build_session(**kwargs):
        nonlocal observed_token
        if "botocore_session" not in kwargs:
            return pinned_session
        core_session = kwargs["botocore_session"]
        frozen_token_path = Path(core_session.full_config["profiles"]["workload"]["web_identity_token_file"])
        observed_token = frozen_token_path.read_text()
        return discovery_session

    monkeypatch.setattr(BedrockCredentialPlan, "verify_unchanged", verify_then_replace_token)
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = build_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(credential_plan=plan)

    assert plan.account == "arn:aws:iam::111111111111:role/credentialsrole"
    assert observed_token == "captured-credentials-token"
    assert resolved.session is pinned_session
    credentials.get_frozen_credentials.assert_called_once_with()


def test_bedrock_implicit_default_profile_disables_post_admission_ambient_web_identity(
    monkeypatch,
    tmp_path,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session
    from tacit.runtime_ownership import BedrockCredentialPlan

    captured_token_path = tmp_path / "captured-default-token"
    captured_token_path.write_text("captured-default-token")
    ambient_token_path = tmp_path / "ambient-token"
    ambient_token_path.write_text("ambient-token")
    config_path = tmp_path / "config"
    config_path.write_text(
        "[default]\n"
        "role_arn = arn:aws:iam::111111111111:role/DefaultRole\n"
        f"web_identity_token_file = {captured_token_path}\n"
    )
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
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    runtime_settings = _settings(
        tmp_path,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )
    plan = BedrockCredentialPlan.capture(runtime_settings)
    original_verify = BedrockCredentialPlan.verify_unchanged
    discovery_session = MagicMock()
    credentials = MagicMock(method="assume-role-with-web-identity")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="ASIADEFAULT",
        secret_key="default-secret",
        token="default-session-token",
    )
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    observed_profile = None
    observed_disable_env = False
    observed_token = ""

    def verify_then_add_ambient_identity(self: BedrockCredentialPlan) -> None:
        original_verify(self)
        monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::222222222222:role/AmbientRole")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(ambient_token_path))

    def build_session(**kwargs):
        nonlocal observed_profile, observed_disable_env, observed_token
        if "botocore_session" not in kwargs:
            return pinned_session
        core_session = kwargs["botocore_session"]
        observed_profile = core_session.get_config_variable("profile")
        provider = next(
            item
            for item in core_session.get_component("credential_provider").providers
            if item.METHOD == "assume-role-with-web-identity"
        )
        observed_disable_env = bool(getattr(provider, "_disable_env_vars", False))
        frozen_token_path = Path(core_session.full_config["profiles"]["default"]["web_identity_token_file"])
        observed_token = frozen_token_path.read_text()
        return discovery_session

    monkeypatch.setattr(BedrockCredentialPlan, "verify_unchanged", verify_then_add_ambient_identity)
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = build_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(credential_plan=plan)

    assert observed_profile == "default"
    assert observed_disable_env is True
    assert observed_token == "captured-default-token"
    assert resolved.session is pinned_session


def test_bedrock_named_static_profile_yields_to_ambient_web_identity_provider(
    monkeypatch,
    tmp_path,
) -> None:
    from tacit.runtime_ownership import BedrockCredentialPlan

    token_path = tmp_path / "ambient-token"
    token_path.write_text("ambient-token")
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[named]\n" "aws_access_key_id = AKIAPROFILE\n" "aws_secret_access_key = profile-secret\n"
    )
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
    credentials_path.write_text("[base]\n" "aws_access_key_id = AKIABASE\n" "aws_secret_access_key = base-secret\n")
    config_path = tmp_path / "config"
    config_path.write_text(
        "[default]\n" "role_arn = arn:aws:iam::111111111111:role/ProfileRole\n" "source_profile = base\n"
    )
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
    credentials_path.write_text("[default]\n" "aws_access_key_id = AKIAFILE\n" "aws_secret_access_key = file-secret\n")
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
    credentials_path.write_text("[default]\n" "aws_access_key_id = AKIAFILE\n" "aws_secret_access_key = file-secret\n")
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
        "[fallback]\n" "aws_access_key_id = AKIAFALLBACK\n" "aws_secret_access_key = fallback-secret\n"
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
        "[default]\n" "aws_access_key_id = AKIAFALLBACK\n" "aws_secret_access_key = fallback-secret\n"
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
            "[profile source]\n" "aws_access_key_id = AKIACONFIG\n" "aws_secret_access_key = config-secret\n"
        )
        monkeypatch.setenv("AWS_PROFILE", "selected")
    else:
        credentials_path.write_text("[default]\naws_access_key_id = AKIAPARTIAL\n")
        config_path.write_text(
            "[default]\n" "aws_access_key_id = AKIACONFIG\n" "aws_secret_access_key = config-secret\n"
        )
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
    credentials_path.write_text("[base]\n" "aws_access_key_id = AKIABASE\n" "aws_secret_access_key = base-secret\n")
    config_path = tmp_path / "config"
    config_path.write_text(
        "[profile first-role]\n" "role_arn = arn:aws:iam::111111111111:role/FirstRole\n" "source_profile = base\n"
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
    credentials_path.write_text("[base]\n" "aws_access_key_id = AKIABASE\n" "aws_secret_access_key = base-secret\n")
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
        "[selected]\n"
        "mfa_serial =\n"
        "[base]\n"
        "aws_access_key_id = AKIABASE\n"
        "aws_secret_access_key = base-secret\n"
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
    config_path.write_text(
        "[profile source]\n" "aws_access_key_id = AKIASOURCE\n" "aws_secret_access_key = source-secret\n"
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


def test_bedrock_profile_metadata_matches_botocore_file_precedence(
    monkeypatch,
    tmp_path,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import patch

    from tacit.agents.providers.bedrock import _build_boto3_session
    from tacit.runtime_ownership import BedrockCredentialPlan

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
        "[profile selected]\n" "role_arn = arn:aws:iam::222222222222:role/ConfigRole\n" "source_profile = base\n"
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
    observed_role = ""
    credentials = MagicMock(method="assume-role")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="ASIAROLE",
        secret_key="role-secret",
        token="role-token",
    )
    discovery_session = MagicMock()
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()

    def build_session(**kwargs):
        nonlocal observed_role
        if "botocore_session" in kwargs:
            observed_role = kwargs["botocore_session"].full_config["profiles"]["selected"]["role_arn"]
            return discovery_session
        return pinned_session

    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = build_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        resolved = _build_boto3_session(credential_plan=plan)

    assert plan.account == "arn:aws:iam::111111111111:role/credentialsrole"
    assert observed_role == "arn:aws:iam::111111111111:role/CredentialsRole"
    assert resolved.credential_identity.account == plan.account


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
        "[profile selected]\n" "role_arn = arn:aws:iam::222222222222:role/ConfigRole\n" "source_profile = safe\n"
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
        f"[{section}]\n" "role_arn = arn:aws:iam::123456789012:role/TacitRuntime\n" "source_profile = base\n"
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
async def test_concurrent_dependency_runs_do_not_close_a_provider_still_in_use(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    provider = _ProviderProbe(runtime_settings)
    calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal calls
        calls += 1
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
        assert provider_resource() is provider
        first_ready.set()
        await second_ready.wait()
        await dependencies.close_resources()
        assert provider.closed is False
        release_second.set()

    async def second_run() -> None:
        await dependencies.acquire_resources()
        assert provider_resource() is provider
        second_ready.set()
        await first_ready.wait()
        await release_second.wait()
        assert provider.closed is False
        await dependencies.close_resources()

    await asyncio.gather(first_run(), second_run())

    assert calls == 1
    assert provider.closed is True


@pytest.mark.asyncio
async def test_hung_provider_cleanup_does_not_wedge_the_next_generation(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        pipeline_max_concurrent=2,
        pipeline_max_queued=0,
    )
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    class BlockingProvider(_ProviderProbe):
        async def close(self) -> None:
            close_started.set()
            try:
                await release_close.wait()
            except asyncio.CancelledError:
                await release_close.wait()
            await super().close()

    first = BlockingProvider(runtime_settings)
    second = _ProviderProbe(runtime_settings)
    providers = [first, second]
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
            lambda: providers.pop(0),
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
        assert dependencies.llm_provider_factory() is first
        await asyncio.wait_for(dependencies.close_resources(), timeout=0.2)

    assert close_started.is_set()
    assert first.closed is False
    assert dependencies.pipeline_admission.in_flight == 1
    assert dependencies.pipeline_admission.retained == 1

    async with dependencies.pipeline_admission.slot():
        await dependencies.acquire_resources()
        assert dependencies.llm_provider_factory() is second
        await dependencies.close_resources()

    assert second.closed is True
    assert dependencies.pipeline_admission.in_flight == 1
    release_close.set()
    for _ in range(100):
        if dependencies.pipeline_admission.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert first.closed is True
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
async def test_repeated_hung_provider_cleanup_is_bounded_by_effective_work_budget(tmp_path) -> None:
    runtime_settings = _settings(
        tmp_path,
        llm_provider="ollama",
        llm_api_base="http://127.0.0.1:11434",
        pipeline_max_concurrent=2,
        pipeline_max_queued=0,
    )
    releases = [asyncio.Event(), asyncio.Event()]
    close_started = [asyncio.Event(), asyncio.Event()]

    class BlockingProvider(_ProviderProbe):
        def __init__(self, index: int) -> None:
            super().__init__(runtime_settings)
            self.index = index

        async def close(self) -> None:
            close_started[self.index].set()
            try:
                await releases[self.index].wait()
            except asyncio.CancelledError:
                await releases[self.index].wait()
            await super().close()

    providers = [BlockingProvider(0), BlockingProvider(1)]
    factory_calls = 0

    def provider_factory() -> LLMProvider:
        nonlocal factory_calls
        provider = providers[factory_calls]
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

    for provider in providers:
        async with dependencies.pipeline_admission.slot():
            await dependencies.acquire_resources()
            assert dependencies.llm_provider_factory() is provider
            await dependencies.close_resources()

    assert all(event.is_set() for event in close_started)
    assert dependencies.pipeline_admission.in_flight == 2
    assert dependencies.pipeline_admission.retained == 2
    with pytest.raises(PipelineAdmissionRejected) as exc_info:
        await dependencies.pipeline_admission.acquire()
    assert exc_info.value.reason_code == "pipeline_admission_queue_full"
    assert factory_calls == 2

    for event in releases:
        event.set()
    for _ in range(100):
        if dependencies.pipeline_admission.in_flight == 0:
            break
        await asyncio.sleep(0)
    assert all(provider.closed for provider in providers)
    assert dependencies.pipeline_admission.in_flight == 0


@pytest.mark.asyncio
async def test_new_run_waits_for_previous_provider_generation_to_close(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    close_started = asyncio.Event()
    release_close = asyncio.Event()

    class BlockingProvider(_ProviderProbe):
        async def close(self) -> None:
            close_started.set()
            await release_close.wait()
            await super().close()

    first = BlockingProvider(runtime_settings)
    second = _ProviderProbe(runtime_settings)
    providers = [first, second]

    def provider_factory() -> LLMProvider:
        return providers.pop(0)

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
        assert provider_resource() is first
        first_ready.set()
        await begin_first_close.wait()
        await dependencies.close_resources()

    async def second_run() -> None:
        await close_started.wait()
        await dependencies.acquire_resources()
        assert provider_resource() is second
        second_acquired.set()
        await release_second.wait()
        await dependencies.close_resources()

    first_task = asyncio.create_task(first_run())
    await first_ready.wait()
    begin_first_close.set()
    await close_started.wait()
    second_task = asyncio.create_task(second_run())
    await asyncio.sleep(0)
    assert second_acquired.is_set() is False

    release_close.set()
    await second_acquired.wait()
    assert first.closed is True
    release_second.set()
    await asyncio.gather(first_task, second_task)
    assert second.closed is True


@pytest.mark.asyncio
async def test_inherited_child_context_cannot_release_parent_provider_lease(tmp_path) -> None:
    runtime_settings = _settings(tmp_path, llm_provider="ollama", llm_api_base="http://127.0.0.1:11434")
    provider = _ProviderProbe(runtime_settings)
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
            lambda: provider,
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

    with pytest.raises(PipelineExecutionError) as exc_info:
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
    monkeypatch.setattr("tacit.agents.providers.openai_provider.openai.AsyncOpenAI", openai_client)
    monkeypatch.setattr("tacit.agents.providers.anthropic.anthropic.AsyncAnthropic", anthropic_client)
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
        base_url="https://api.openai.com/v1",
        organization="",
        project="",
    )
    anthropic_client.assert_called_once_with(
        api_key="anthropic-secret",
        base_url="https://api.anthropic.com",
    )
    assert openai_provider.runtime_ownership.remotes[0].endpoint == "https://api.openai.com/v1"
    assert openai_provider.runtime_ownership.remotes[0].account == "organization:none;project:none"
    assert anthropic_provider.runtime_ownership.remotes[0].endpoint == "https://api.anthropic.com"
