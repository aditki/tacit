"""Matrix tests for the temporary synchronous Bedrock compatibility bridge.

The bridge deliberately gives up SDK resource reuse so one admitted worker owns
credential realization, client construction, the request, and cleanup. These
tests describe that containment contract without coupling to the worker bridge's
private bookkeeping.
"""

from __future__ import annotations

import asyncio
import errno
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

import tacit.pipeline_admission as pipeline_admission_module
from tacit.agents.providers.bedrock import (
    BedrockProvider,
    _ResolvedBedrockRuntime,
)
from tacit.config import Settings
from tacit.errors import PipelineAdmissionRejected, RuntimeOwnershipError
from tacit.pipeline_admission import PipelineAdmissionController, pipeline_execution_deadline
from tacit.runtime_ownership import BedrockCredentialIdentity, credential_fingerprint

_ROLE_ACCOUNT = "arn:aws:iam::123456789012:role/tacitruntime"


@dataclass(slots=True)
class _Event:
    name: str
    thread_id: int
    blocking_in_flight: int


@dataclass(slots=True)
class _Closeable:
    name: str
    lifecycle: PipelineAdmissionController
    events: list[_Event]
    close_calls: int = 0
    close_error: BaseException | None = None

    def close(self) -> None:
        self.close_calls += 1
        self.events.append(
            _Event(
                f"close:{self.name}",
                threading.get_ident(),
                self.lifecycle.blocking_in_flight,
            )
        )
        if self.close_error is not None:
            raise self.close_error


@dataclass(slots=True)
class _RuntimeClient(_Closeable):
    response_text: str = "ok"
    delay_seconds: float = 0.0
    started: threading.Event | None = None
    release: threading.Event | None = None
    request_error: BaseException | None = None

    def converse(self, **_kwargs: object) -> dict[str, Any]:
        self.events.append(
            _Event(
                "converse",
                threading.get_ident(),
                self.lifecycle.blocking_in_flight,
            )
        )
        if self.started is not None:
            self.started.set()
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        if self.release is not None:
            assert self.release.wait(timeout=2), "test did not release the Bedrock worker"
        if self.request_error is not None:
            raise self.request_error
        return {
            "output": {
                "message": {
                    "content": [{"text": self.response_text}],
                }
            }
        }


@dataclass(slots=True)
class _Session:
    lifecycle: PipelineAdmissionController
    events: list[_Event]
    runtime_client: _RuntimeClient | None
    construction_error: BaseException | None = None

    def client(self, service_name: str, **_kwargs: object) -> _RuntimeClient:
        assert service_name == "bedrock-runtime"
        self.events.append(
            _Event(
                "construct:runtime-client",
                threading.get_ident(),
                self.lifecycle.blocking_in_flight,
            )
        )
        if self.construction_error is not None:
            raise self.construction_error
        assert self.runtime_client is not None
        return self.runtime_client


@dataclass(slots=True)
class _Generation:
    lifecycle: PipelineAdmissionController
    number: int
    events: list[_Event]
    started: threading.Event | None = None
    release: threading.Event | None = None
    construction_error: BaseException | None = None
    request_error: BaseException | None = None
    credential_client: _Closeable = field(init=False)
    runtime_client: _RuntimeClient = field(init=False)
    session: _Session = field(init=False)

    def __post_init__(self) -> None:
        self.credential_client = _Closeable(
            f"credentials-{self.number}",
            self.lifecycle,
            self.events,
        )
        self.runtime_client = _RuntimeClient(
            f"runtime-{self.number}",
            self.lifecycle,
            self.events,
            response_text=f"generation-{self.number}",
            started=self.started,
            release=self.release,
            request_error=self.request_error,
        )
        self.session = _Session(
            self.lifecycle,
            self.events,
            self.runtime_client,
            construction_error=self.construction_error,
        )

    def resolved_runtime(self) -> _ResolvedBedrockRuntime:
        return _ResolvedBedrockRuntime(
            session=self.session,
            credential_identity=BedrockCredentialIdentity(
                account=_ROLE_ACCOUNT,
                credential_fingerprint=credential_fingerprint(f"temporary-credential-generation-{self.number}"),
                uses_sts=True,
            ),
            credential_clients=(self.credential_client,),
        )


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        pipeline_max_concurrent=1,
        pipeline_max_queued=0,
    )


def _provider_with_lifecycle() -> tuple[BedrockProvider, PipelineAdmissionController]:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    provider = BedrockProvider(_settings())
    provider.bind_pipeline_lifecycle(lifecycle)
    return provider, lifecycle


def _builder(
    lifecycle: PipelineAdmissionController,
    generations: list[_Generation],
    events: list[_Event],
) -> Callable[..., _ResolvedBedrockRuntime]:
    def build(**_kwargs: object) -> _ResolvedBedrockRuntime:
        events.append(
            _Event(
                "realize:credentials",
                threading.get_ident(),
                lifecycle.blocking_in_flight,
            )
        )
        assert generations, "Bedrock reused a generation instead of realizing a new one"
        return generations.pop(0).resolved_runtime()

    return build


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            pytest.fail("timed out waiting for Bedrock worker lifecycle state")
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_invocation_owns_sdk_generation_inside_one_admitted_worker(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    caller_thread = threading.get_ident()
    events: list[_Event] = []
    generation = _Generation(lifecycle, 1, events)
    pending = [generation]
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, pending, events),
    )

    result = await provider.chat_text("system", "user")

    assert result.text == "generation-1"
    assert pending == []
    assert [event.name for event in events[:3]] == [
        "realize:credentials",
        "construct:runtime-client",
        "converse",
    ]
    assert {event.thread_id for event in events} == {events[0].thread_id}
    assert events[0].thread_id != caller_thread
    assert all(event.blocking_in_flight == 1 for event in events)
    assert generation.runtime_client.close_calls == 1
    assert generation.credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_cancelled_caller_retains_capacity_until_worker_cleanup(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    started = threading.Event()
    release = threading.Event()
    generation = _Generation(lifecycle, 1, events, started=started, release=release)
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, [generation], events),
    )
    task = asyncio.create_task(provider.chat_text("system", "user"))

    try:
        assert await asyncio.to_thread(started.wait, 1), "Bedrock worker did not start"
        assert lifecycle.in_flight == 1
        assert lifecycle.blocking_in_flight == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert lifecycle.in_flight == 1
        assert lifecycle.blocking_in_flight == 1
        assert generation.runtime_client.close_calls == 0
        assert generation.credential_client.close_calls == 0
    finally:
        release.set()

    await _wait_until(lambda: lifecycle.in_flight == 0)
    assert lifecycle.blocking_in_flight == 0
    assert generation.runtime_client.close_calls == 1
    assert generation.credential_client.close_calls == 1
    worker_threads = {event.thread_id for event in events}
    assert len(worker_threads) == 1


@pytest.mark.asyncio
async def test_sequential_invocations_use_fresh_rotating_generations(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    first = _Generation(lifecycle, 1, events)
    second = _Generation(lifecycle, 2, events)
    pending = [first, second]
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, pending, events),
    )

    first_result = await provider.chat_text("system", "first")
    second_result = await provider.chat_text("system", "second")

    assert (first_result.text, second_result.text) == ("generation-1", "generation-2")
    assert pending == []
    assert first.runtime_client.close_calls == first.credential_client.close_calls == 1
    assert second.runtime_client.close_calls == second.credential_client.close_calls == 1
    assert [event.name for event in events].count("realize:credentials") == 2
    assert [event.name for event in events].count("construct:runtime-client") == 2
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ("construction", "request"))
async def test_failed_invocation_closes_every_created_client(
    monkeypatch,
    failure_phase: str,
) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    generation = _Generation(
        lifecycle,
        1,
        events,
        construction_error=(RuntimeError("client construction failed") if failure_phase == "construction" else None),
        request_error=(RuntimeError("request failed") if failure_phase == "request" else None),
    )
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, [generation], events),
    )

    with pytest.raises(RuntimeError, match="failed"):
        await provider.chat_text("system", "user")

    assert generation.credential_client.close_calls == 1
    assert generation.runtime_client.close_calls == (0 if failure_phase == "construction" else 1)
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert all(event.blocking_in_flight == 1 for event in events)


@pytest.mark.asyncio
async def test_cleanup_failure_attempts_every_close_and_fails_the_operation(monkeypatch) -> None:
    monkeypatch.setattr(
        pipeline_admission_module,
        "_PROCESS_RUNTIME_FATAL_REGISTRY",
        pipeline_admission_module._ProcessRuntimeFatalRegistry(limit=1024),
    )
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    generation = _Generation(lifecycle, 1, events)
    generation.runtime_client.close_error = RuntimeError("close failed")
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, [generation], events),
    )

    with pytest.raises(RuntimeOwnershipError, match="operation cleanup failed"):
        await provider.chat_text("system", "user")

    assert generation.runtime_client.close_calls == 1
    assert generation.credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


def test_close_is_sync_idempotent_and_has_no_sdk_cleanup_work(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    generation = _Generation(lifecycle, 1, events)
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, [generation], events),
    )
    asyncio.run(provider.chat_text("system", "user"))
    event_count = len(events)

    provider.close_blocking()
    provider.close_blocking()

    assert len(events) == event_count
    assert generation.runtime_client.close_calls == 1
    assert generation.credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


def test_client_config_consumes_one_absolute_operation_budget() -> None:
    from tacit.agents.providers.bedrock import _bedrock_client_config

    runtime_settings = _settings()
    config = _bedrock_client_config(runtime_settings, deadline=time.monotonic() + 0.5)

    assert 0 < config.connect_timeout <= 0.5
    assert 0 < config.read_timeout <= 0.5
    assert config.retries["total_max_attempts"] == 1


def test_profile_fallback_stops_when_the_shared_deadline_is_exhausted(monkeypatch) -> None:
    provider = BedrockProvider(_settings())
    client = MagicMock()
    retry_factory = MagicMock()

    class ValidationException(Exception):
        pass

    client.converse.side_effect = ValidationException("On-demand throughput is not supported; use an inference profile")
    remaining_checks = iter((1.0, TimeoutError("deadline expired")))

    def remaining(*_args: object, **_kwargs: object) -> float:
        result = next(remaining_checks)
        if isinstance(result, BaseException):
            raise result
        return result

    monkeypatch.setattr("tacit.agents.providers.bedrock._remaining_operation_timeout", remaining)

    with pytest.raises(TimeoutError, match="deadline expired"):
        provider._converse_with_client(
            client,
            "system",
            "user",
            0.2,
            deadline=time.monotonic() + 1.0,
            retry_client_factory=retry_factory,
        )

    assert client.converse.call_count == 1
    retry_factory.assert_not_called()


@pytest.mark.asyncio
async def test_profile_fallback_rebuilds_the_client_with_remaining_budget(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()

    class ValidationException(Exception):
        pass

    first_client = MagicMock()

    def reject_bare_model(**_kwargs: object) -> dict[str, Any]:
        time.sleep(0.01)
        raise ValidationException("On-demand throughput is not supported; use an inference profile")

    first_client.converse.side_effect = reject_bare_model
    second_client = MagicMock()
    second_client.converse.return_value = {"output": {"message": {"content": [{"text": "profile-result"}]}}}
    session = MagicMock()
    session.client.side_effect = [first_client, second_client]
    resolved = _ResolvedBedrockRuntime(
        session=session,
        credential_identity=BedrockCredentialIdentity(
            account=_ROLE_ACCOUNT,
            credential_fingerprint=credential_fingerprint("profile-retry-generation"),
            uses_sts=True,
        ),
    )
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: resolved,
    )

    result = await provider.chat_text("system", "user")

    assert result.text == "profile-result"
    assert session.client.call_count == 2
    first_config = session.client.call_args_list[0].kwargs["config"]
    second_config = session.client.call_args_list[1].kwargs["config"]
    assert 0 < second_config.read_timeout < first_config.read_timeout
    assert first_client.close.call_count == 1
    assert second_client.close.call_count == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_pipeline_deadline_is_forwarded_to_the_owned_worker(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    generation = _Generation(lifecycle, 1, events)
    observed_deadlines: list[float | None] = []

    def build(**kwargs: object) -> _ResolvedBedrockRuntime:
        observed_deadlines.append(kwargs.get("deadline"))
        return generation.resolved_runtime()

    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build)
    deadline = time.monotonic() + 5.0

    with pipeline_execution_deadline(deadline):
        result = await provider.chat_text("system", "user")

    assert result.text == "generation-1"
    assert observed_deadlines == [deadline]
    assert generation.runtime_client.close_calls == 1
    assert generation.credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


def test_web_identity_token_rotation_is_operation_scoped_and_closes_sts(
    monkeypatch,
    tmp_path,
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
        "AWS_ROLE_SESSION_NAME",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_STS",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    token_path = tmp_path / "token"
    credentials_path.write_text("")
    config_path.write_text("")
    token_path.write_text("token-generation-one")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/TacitRuntime")

    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    sts_clients = [MagicMock(), MagicMock()]
    runtime_clients = [MagicMock(), MagicMock()]
    for index, (sts_client, runtime_client) in enumerate(
        zip(sts_clients, runtime_clients, strict=True),
        start=1,
    ):
        sts_client.assume_role_with_web_identity.return_value = {
            "Credentials": {
                "AccessKeyId": f"ASIA{index}",
                "SecretAccessKey": f"secret-{index}",
                "SessionToken": f"session-{index}",
            }
        }
        runtime_client.converse.return_value = {"output": {"message": {"content": [{"text": f"result-{index}"}]}}}
    unsigned_sessions = [MagicMock(), MagicMock()]
    assumed_sessions = [MagicMock(), MagicMock()]
    for unsigned_session, sts_client in zip(unsigned_sessions, sts_clients, strict=True):
        unsigned_session.client.return_value = sts_client
    for assumed_session, runtime_client in zip(assumed_sessions, runtime_clients, strict=True):
        assumed_session.client.return_value = runtime_client
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [
        unsigned_sessions[0],
        assumed_sessions[0],
        unsigned_sessions[1],
        assumed_sessions[1],
    ]

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        provider = BedrockProvider(runtime_settings)
        assert asyncio.run(provider.chat_text("system", "first")).text == "result-1"
        token_path.write_text("token-generation-two")
        assert asyncio.run(provider.chat_text("system", "second")).text == "result-2"

    assert sts_clients[0].assume_role_with_web_identity.call_args.kwargs["WebIdentityToken"] == ("token-generation-one")
    assert sts_clients[1].assume_role_with_web_identity.call_args.kwargs["WebIdentityToken"] == ("token-generation-two")
    assert all(session.client.call_args.kwargs["verify"] is True for session in unsigned_sessions)
    assert all(session.client.call_args.kwargs["verify"] is True for session in assumed_sessions)
    assert all(client.close.call_count == 1 for client in sts_clients)
    assert all(client.close.call_count == 1 for client in runtime_clients)


def test_environment_credentials_are_redacted_and_identity_changes_fail_closed(
    monkeypatch,
) -> None:
    for name in (
        "AWS_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_SESSION_NAME",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAGENERATIONONE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-generation-one")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token-generation-one")

    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    runtime_client = MagicMock()
    runtime_client.converse.return_value = {"output": {"message": {"content": [{"text": "result-1"}]}}}
    session = MagicMock()
    session.client.return_value = runtime_client
    mock_boto3 = MagicMock()
    mock_boto3.Session.return_value = session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        provider = BedrockProvider(runtime_settings)
        assert "AWS_ACCESS_KEY_ID" not in provider._credential_plan.environment
        assert "AWS_SECRET_ACCESS_KEY" not in provider._credential_plan.environment
        assert "AWS_SESSION_TOKEN" not in provider._credential_plan.environment
        assert asyncio.run(provider.chat_text("system", "first")).text == "result-1"
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAGENERATIONONE")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-generation-two")
        monkeypatch.setenv("AWS_SESSION_TOKEN", "token-generation-two")
        with pytest.raises(RuntimeOwnershipError, match="credential selector changed"):
            asyncio.run(provider.chat_text("system", "second"))

    assert mock_boto3.Session.call_count == 1
    first_session = mock_boto3.Session.call_args.kwargs
    assert first_session["aws_access_key_id"] == "AKIAGENERATIONONE"
    assert first_session["aws_session_token"] == "token-generation-one"
    assert runtime_client.close.call_count == 1


@pytest.mark.asyncio
async def test_static_profile_content_mutation_fails_before_sdk_construction(
    monkeypatch,
    tmp_path,
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
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    credentials_path.write_text("[workload]\naws_access_key_id = AKIASTATIC\naws_secret_access_key = first-secret\n")
    config_path.write_text("")
    monkeypatch.setenv("AWS_PROFILE", "workload")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    provider = BedrockProvider(
        Settings(
            _env_file=None,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
            llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        )
    )
    credentials_path.write_text("[workload]\naws_access_key_id = AKIASTATIC\naws_secret_access_key = second-secret\n")
    mock_boto3 = MagicMock()

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        with pytest.raises(RuntimeOwnershipError, match="credential selector changed"):
            await provider.chat_text("system", "user")

    mock_boto3.Session.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutated_field", "mutated_value"),
    (
        ("aws_access_key_id", "AKIASOURCETWO"),
        ("aws_secret_access_key", "source-secret-two"),
        ("aws_session_token", "source-token-two"),
    ),
)
async def test_assume_role_static_source_credentials_are_pinned_before_sts(
    monkeypatch,
    tmp_path,
    mutated_field: str,
    mutated_value: str,
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
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ):
        monkeypatch.delenv(name, raising=False)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    source_credentials = {
        "aws_access_key_id": "AKIASOURCEONE",
        "aws_secret_access_key": "source-secret-one",
        "aws_session_token": "source-token-one",
    }

    def write_source_credentials() -> None:
        credentials_path.write_text(
            "[source]\n" + "".join(f"{name} = {value}\n" for name, value in source_credentials.items())
        )

    write_source_credentials()
    config_path.write_text("[profile workload]\n" f"role_arn = {_ROLE_ACCOUNT}\n" "source_profile = source\n")
    monkeypatch.setenv("AWS_PROFILE", "workload")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    provider = BedrockProvider(
        Settings(
            _env_file=None,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
            llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        )
    )

    source_credentials[mutated_field] = mutated_value
    write_source_credentials()
    mock_boto3 = MagicMock()

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        with pytest.raises(RuntimeOwnershipError, match="credential selector changed"):
            await provider.chat_text("system", "user")

    mock_boto3.Session.assert_not_called()


@pytest.mark.asyncio
async def test_direct_admission_wait_consumes_the_operation_deadline(monkeypatch) -> None:
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_aws_access_key_id="AKIADIRECTDEADLINE",
        llm_aws_secret_access_key="direct-deadline-secret",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        pipeline_max_concurrent=1,
        pipeline_max_queued=1,
        pipeline_timeout_seconds=0.5,
    )
    first = BedrockProvider(runtime_settings)
    second = BedrockProvider(runtime_settings)
    lifecycle = first._execution_lifecycle()[0]
    started = threading.Event()
    release = threading.Event()
    events: list[_Event] = []
    generation = _Generation(lifecycle, 1, events, started=started, release=release)
    builds = 0

    def build(**_kwargs: object) -> _ResolvedBedrockRuntime:
        nonlocal builds
        builds += 1
        return generation.resolved_runtime()

    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build)
    active = asyncio.create_task(first.chat_text("system", "first"))
    try:
        assert await asyncio.to_thread(started.wait, 1.0)
        with pipeline_execution_deadline(time.monotonic() + 0.05):
            with pytest.raises(PipelineAdmissionRejected) as exc_info:
                await second.chat_text("system", "second")
        assert exc_info.value.reason_code == "pipeline_admission_wait_timeout"
        assert builds == 1
        assert lifecycle.in_flight == 1
    finally:
        release.set()
    assert (await active).text == "generation-1"
    await _wait_until(lambda: lifecycle.in_flight == 0)
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_bound_provider_wait_consumes_the_operation_deadline(monkeypatch) -> None:
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_aws_access_key_id="AKIABOUNDDEADLINE",
        llm_aws_secret_access_key="bound-deadline-secret",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        pipeline_timeout_seconds=0.03,
    )
    lifecycle = PipelineAdmissionController(1, max_queued=1)
    provider = BedrockProvider(runtime_settings)
    provider.bind_pipeline_lifecycle(lifecycle)
    lease = await lifecycle.acquire()
    build = MagicMock()
    monkeypatch.setattr("tacit.agents.providers.bedrock._build_boto3_session", build)

    started = time.monotonic()
    try:
        with pytest.raises(PipelineAdmissionRejected) as exc_info:
            await asyncio.wait_for(provider.chat_text("system", "user"), timeout=0.2)
    finally:
        lifecycle.release(lease)

    assert exc_info.value.reason_code == "pipeline_admission_wait_timeout"
    assert time.monotonic() - started < 0.2
    build.assert_not_called()
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_late_converse_result_is_rejected_after_cleanup(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    generation = _Generation(lifecycle, 1, events)

    generation.runtime_client.delay_seconds = 0.15
    generation.runtime_client.response_text = "too-late"
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        _builder(lifecycle, [generation], events),
    )

    with pipeline_execution_deadline(time.monotonic() + 0.1):
        with pytest.raises(TimeoutError, match="deadline expired"):
            await provider.chat_text("system", "user")

    assert generation.runtime_client.close_calls == 1
    assert generation.credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


@pytest.mark.asyncio
async def test_invalid_runtime_wrapper_closes_nested_resources(monkeypatch) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    events: list[_Event] = []
    session = _Closeable("session", lifecycle, events)
    credential_client = _Closeable("credentials", lifecycle, events)
    invalid = _ResolvedBedrockRuntime(
        session=cast(Any, session),
        credential_identity=cast(Any, object()),
        credential_clients=(credential_client,),
    )
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: invalid,
    )

    with pytest.raises(RuntimeOwnershipError, match="no realized credential identity"):
        await provider.chat_text("system", "user")

    assert session.close_calls == 1
    assert credential_client.close_calls == 1
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0


def test_ambient_ca_bundle_cannot_change_sts_or_runtime_tls_trust(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.delenv("AWS_CA_BUNDLE", raising=False)
    runtime_settings = _settings()
    sts_client = MagicMock()
    sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAREALIZED",
            "SecretAccessKey": "realized-secret",
            "SessionToken": "realized-token",
        }
    }
    runtime_client = MagicMock()
    runtime_client.converse.return_value = {"output": {"message": {"content": [{"text": "result"}]}}}
    source_session = MagicMock()
    source_session.client.return_value = sts_client
    runtime_session = MagicMock()
    runtime_session.client.return_value = runtime_client
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [source_session, runtime_session]

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        provider = BedrockProvider(runtime_settings)
        ca_bundle = tmp_path / "untrusted.pem"
        ca_bundle.write_text("not a certificate")
        monkeypatch.setenv("AWS_CA_BUNDLE", str(ca_bundle))
        assert asyncio.run(provider.chat_text("system", "user")).text == "result"

    assert source_session.client.call_args.kwargs["verify"] is True
    assert runtime_session.client.call_args.kwargs["verify"] is True
    assert sts_client.close.call_count == 1
    assert runtime_client.close.call_count == 1


def _release_fifo_reader_later(path: os.PathLike[str]) -> threading.Thread:
    def release() -> None:
        time.sleep(0.2)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
        except OSError as exc:
            assert exc.errno == errno.ENXIO
            return
        os.close(descriptor)

    thread = threading.Thread(target=release)
    thread.start()
    return thread


def test_fifo_web_identity_source_fails_promptly_during_provider_construction(
    monkeypatch,
    tmp_path,
) -> None:
    token_path = tmp_path / "web-identity-token"
    os.mkfifo(token_path)
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/TacitRuntime")
    release = _release_fifo_reader_later(token_path)

    started = time.monotonic()
    try:
        with pytest.raises(RuntimeOwnershipError, match="regular files"):
            BedrockProvider(
                Settings(
                    _env_file=None,
                    llm_provider="bedrock",
                    llm_bedrock_region="us-east-1",
                    llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
                )
            )
        elapsed = time.monotonic() - started
    finally:
        release.join(timeout=1)

    assert elapsed < 0.15
    assert not release.is_alive()


@pytest.mark.asyncio
async def test_fifo_replacement_fails_promptly_during_operation_recapture(
    monkeypatch,
    tmp_path,
) -> None:
    token_path = tmp_path / "web-identity-token"
    token_path.write_text("initial-token")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/TacitRuntime")
    provider = BedrockProvider(
        Settings(
            _env_file=None,
            llm_provider="bedrock",
            llm_bedrock_region="us-east-1",
            llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        )
    )
    token_path.unlink()
    os.mkfifo(token_path)
    release = _release_fifo_reader_later(token_path)

    started = time.monotonic()
    try:
        with pytest.raises(RuntimeOwnershipError, match="regular files"):
            await provider.chat_text("system", "user")
        elapsed = time.monotonic() - started
    finally:
        release.join(timeout=1)

    assert elapsed < 0.15
    assert not release.is_alive()
