"""Cancellation and event-loop-loss matrix for the blocking Bedrock bridge."""

from __future__ import annotations

import asyncio
import sys
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from types import SimpleNamespace
from typing import Any

import pytest

from tacit.agents.providers.bedrock import BedrockProvider, _ResolvedBedrockRuntime
from tacit.config import Settings
from tacit.pipeline_admission import PipelineAdmissionController
from tacit.runtime_ownership import BedrockCredentialIdentity, credential_fingerprint

_ROLE_ARN = "arn:aws:iam::123456789012:role/TacitRuntime"
_ROLE_ACCOUNT = "arn:aws:iam::123456789012:role/tacitruntime"


def _run_on_owned_loop(
    test: Callable[..., Coroutine[Any, Any, None]],
) -> Callable[..., None]:
    """Give a synchronous pytest case one fresh, deterministically closed loop."""

    @wraps(test)
    def run(*args: Any, **kwargs: Any) -> None:
        asyncio.run(test(*args, **kwargs))

    return run


@dataclass(slots=True)
class _Closeable:
    close_calls: int = 0

    def close(self) -> None:
        self.close_calls += 1


class _RuntimeClient(_Closeable):
    def __init__(
        self,
        *,
        started: threading.Event | None = None,
        release: threading.Event | None = None,
    ) -> None:
        super().__init__()
        self.started = started
        self.release = release

    def converse(self, **_kwargs: object) -> dict[str, Any]:
        if self.started is not None:
            self.started.set()
        if self.release is not None:
            assert self.release.wait(timeout=2), "test did not release the Bedrock request"
        return {"output": {"message": {"content": [{"text": "ok"}]}}}


class _RuntimeSession(_Closeable):
    def __init__(self, client_factory: Callable[[], _RuntimeClient]) -> None:
        super().__init__()
        self._client_factory = client_factory

    def client(self, service_name: str, **_kwargs: object) -> _RuntimeClient:
        assert service_name == "bedrock-runtime"
        return self._client_factory()


class _BlockingStsClient(_Closeable):
    def __init__(self, started: threading.Event, release: threading.Event) -> None:
        super().__init__()
        self._started = started
        self._release = release

    def assume_role_with_web_identity(self, **_kwargs: object) -> dict[str, Any]:
        self._started.set()
        assert self._release.wait(timeout=2), "test did not release STS realization"
        return {
            "Credentials": {
                "AccessKeyId": "ASIATEMPORARY",
                "SecretAccessKey": "temporary-secret",
                "SessionToken": "temporary-token",
            }
        }


class _UnsignedSession:
    def __init__(self, sts_client: _BlockingStsClient) -> None:
        self._sts_client = sts_client

    def client(self, service_name: str, **_kwargs: object) -> _BlockingStsClient:
        assert service_name == "sts"
        return self._sts_client


def _settings() -> Settings:
    return Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_role_arn=_ROLE_ARN,
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )


def _provider_with_lifecycle(
    runtime_settings: Settings | None = None,
) -> tuple[BedrockProvider, PipelineAdmissionController]:
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    provider = BedrockProvider(runtime_settings or _settings())
    provider.bind_pipeline_lifecycle(lifecycle)
    return provider, lifecycle


def _resolved_runtime(
    session: _RuntimeSession,
    credential_client: _Closeable,
) -> _ResolvedBedrockRuntime:
    return _ResolvedBedrockRuntime(
        session=session,
        credential_identity=BedrockCredentialIdentity(
            account=_ROLE_ACCOUNT,
            credential_fingerprint=credential_fingerprint("temporary-generation"),
            uses_sts=True,
        ),
        credential_clients=(credential_client,),
    )


async def _wait_for_controller_zero(
    lifecycle: PipelineAdmissionController,
    provider: BedrockProvider,
    *,
    timeout: float = 1.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while lifecycle.in_flight or lifecycle.blocking_in_flight or provider._blocking_work.active:
        if loop.time() >= deadline:
            pytest.fail("Bedrock worker did not release shared admission capacity")
        await asyncio.sleep(0.001)
    assert lifecycle.retained == 0


def _wait_for_controller_zero_blocking(
    lifecycle: PipelineAdmissionController,
    provider: BedrockProvider,
    *,
    timeout: float = 1.0,
) -> None:
    deadline = time.monotonic() + timeout
    while lifecycle.in_flight or lifecycle.blocking_in_flight or provider._blocking_work.active:
        if time.monotonic() >= deadline:
            pytest.fail("Bedrock worker did not release shared admission capacity")
        threading.Event().wait(0.001)
    assert lifecycle.retained == 0


def _clear_ambient_aws(monkeypatch: pytest.MonkeyPatch) -> None:
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
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_SESSION_NAME",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_STS",
        "AWS_STS_REGIONAL_ENDPOINTS",
        "AWS_USE_FIPS_ENDPOINT",
        "AWS_USE_DUALSTACK_ENDPOINT",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "AWS_EC2_METADATA_DISABLED",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE",
    ):
        monkeypatch.delenv(name, raising=False)


@_run_on_owned_loop
async def test_cancellation_during_sts_realization_closes_clients_before_releasing_capacity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    _clear_ambient_aws(monkeypatch)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    token_path = tmp_path / "token"
    credentials_path.write_text("")
    config_path.write_text("")
    token_path.write_text("web-identity-token")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    monkeypatch.setenv("AWS_ROLE_ARN", _ROLE_ARN)

    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
        signals_db_path=str(tmp_path / "signals.db"),
    )
    sts_started = threading.Event()
    release_sts = threading.Event()
    sts_client = _BlockingStsClient(sts_started, release_sts)
    runtime_client = _RuntimeClient()
    runtime_session = _RuntimeSession(lambda: runtime_client)
    unsigned_session = _UnsignedSession(sts_client)

    def session_factory(**kwargs: object) -> object:
        if "aws_access_key_id" in kwargs:
            return runtime_session
        return unsigned_session

    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(Session=session_factory))
    provider, lifecycle = _provider_with_lifecycle(runtime_settings)
    task = asyncio.create_task(provider.chat_text("system", "user"))

    try:
        assert await asyncio.to_thread(sts_started.wait, 1), "STS realization did not start"
        assert lifecycle.in_flight == lifecycle.blocking_in_flight == lifecycle.retained == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert lifecycle.in_flight == lifecycle.blocking_in_flight == lifecycle.retained == 1
        assert sts_client.close_calls == 0
        assert runtime_client.close_calls == 0
    finally:
        release_sts.set()

    await _wait_for_controller_zero(lifecycle, provider)
    assert sts_client.close_calls == 1
    assert runtime_client.close_calls == 1
    assert runtime_session.close_calls == 1


@_run_on_owned_loop
async def test_cancellation_during_runtime_client_construction_closes_all_created_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    construction_started = threading.Event()
    release_construction = threading.Event()
    credential_client = _Closeable()
    created_runtime_clients: list[_RuntimeClient] = []

    def construct_runtime_client() -> _RuntimeClient:
        construction_started.set()
        assert release_construction.wait(timeout=2), "test did not release client construction"
        client = _RuntimeClient()
        created_runtime_clients.append(client)
        return client

    runtime_session = _RuntimeSession(construct_runtime_client)
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _resolved_runtime(runtime_session, credential_client),
    )
    task = asyncio.create_task(provider.chat_text("system", "user"))

    try:
        assert await asyncio.to_thread(construction_started.wait, 1), "runtime-client construction did not start"
        assert lifecycle.in_flight == lifecycle.blocking_in_flight == lifecycle.retained == 1

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert lifecycle.in_flight == lifecycle.blocking_in_flight == lifecycle.retained == 1
        assert credential_client.close_calls == 0
        assert runtime_session.close_calls == 0
        assert created_runtime_clients == []
    finally:
        release_construction.set()

    await _wait_for_controller_zero(lifecycle, provider)
    assert len(created_runtime_clients) == 1
    assert created_runtime_clients[0].close_calls == 1
    assert credential_client.close_calls == 1
    assert runtime_session.close_calls == 1


def test_closed_originating_loop_does_not_own_bedrock_cleanup_or_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider, lifecycle = _provider_with_lifecycle()
    request_started = threading.Event()
    release_request = threading.Event()
    credential_client = _Closeable()
    runtime_client = _RuntimeClient(started=request_started, release=release_request)
    runtime_session = _RuntimeSession(lambda: runtime_client)
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _resolved_runtime(runtime_session, credential_client),
    )

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda *_args: None)
    task = loop.create_task(provider.chat_text("system", "user"))
    task._log_destroy_pending = False

    async def wait_for_request() -> None:
        while not request_started.is_set():
            await asyncio.sleep(0.001)

    try:
        loop.run_until_complete(asyncio.wait_for(wait_for_request(), timeout=1))
        assert lifecycle.in_flight == lifecycle.blocking_in_flight == lifecycle.retained == 1
    finally:
        loop.close()

    assert runtime_client.close_calls == 0
    assert credential_client.close_calls == 0
    release_request.set()

    _wait_for_controller_zero_blocking(lifecycle, provider)
    assert runtime_client.close_calls == 1
    assert credential_client.close_calls == 1
    assert runtime_session.close_calls == 1
