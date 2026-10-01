"""Security and lifecycle matrix for credential-bearing LLM SDK transports."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import Executor, Future
from contextvars import Context
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import anthropic
import httpx
import openai
import pytest

import tacit.agents.providers.http_transport as http_transport
from tacit.agents.providers.anthropic import AnthropicProvider
from tacit.agents.providers.http_transport import (
    create_llm_sdk_http_client,
    isolate_llm_sdk_ambient_credentials,
    isolate_llm_sdk_custom_headers,
    llm_sdk_http_policy,
)
from tacit.agents.providers.openai_provider import AzureOpenAIProvider, OpenAIProvider
from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork
from tacit.pipeline_admission import PipelineAdmissionController

_CONFIGURED_ORIGIN = "https://llm-owner.example"
_REDIRECT_ORIGIN = "https://redirect-target.example"
_API_KEY = "provider-credential-for-test"
_PROMPT = "private incident prompt"


@dataclass(frozen=True, slots=True)
class _ProviderCase:
    name: str
    provider_type: type[AnthropicProvider | OpenAIProvider | AzureOpenAIProvider]
    credential_header: str
    error_type: type[Exception]


_PROVIDER_CASES = (
    _ProviderCase("anthropic", AnthropicProvider, "x-api-key", anthropic.APIStatusError),
    _ProviderCase("openai", OpenAIProvider, "authorization", openai.APIStatusError),
    _ProviderCase("azure", AzureOpenAIProvider, "api-key", openai.APIStatusError),
)


def _settings(case: _ProviderCase) -> Settings:
    values: dict[str, Any] = {
        "llm_provider": case.name,
        "llm_api_key": _API_KEY,
        "llm_api_base": _CONFIGURED_ORIGIN,
        "llm_model": "test-model",
        "pipeline_max_concurrent": 7,
        "pipeline_timeout_seconds": 45,
    }
    if case.name == "azure":
        values.update(
            {
                "llm_azure_api_version": "2024-06-01",
                "llm_azure_deployment": "test-deployment",
            }
        )
    return Settings.model_validate(values)


def _success_payload(case: _ProviderCase) -> dict[str, Any]:
    if case.name == "anthropic":
        return {
            "id": "msg_test",
            "type": "message",
            "role": "assistant",
            "model": "test-model",
            "content": [{"type": "text", "text": "ok"}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
    return {
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 1,
        "model": "test-model",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "ok"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _install_transport(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
    transport: httpx.AsyncBaseTransport,
) -> None:
    def factory(runtime_settings: Settings, *, endpoint: str) -> httpx.AsyncClient:
        return create_llm_sdk_http_client(
            runtime_settings,
            endpoint=endpoint,
            transport=transport,
        )

    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )
    monkeypatch.setattr(f"{module}.create_llm_sdk_http_client", factory)


def _credential_value(case: _ProviderCase, request: httpx.Request) -> str:
    value = request.headers.get(case.credential_header, "")
    return value.removeprefix("Bearer ") if case.name == "openai" else value


async def _construct_owned_provider(
    case: _ProviderCase,
    runtime_settings: Settings,
) -> AnthropicProvider | OpenAIProvider | AzureOpenAIProvider:
    """Exercise the same admitted constructor boundary used by runtime factories."""
    lifecycle = PipelineAdmissionController(
        runtime_settings.pipeline_max_concurrent,
        max_queued=runtime_settings.pipeline_max_queued,
    )
    construction = LifecycleOwnedBlockingWork(lifecycle)
    adopted: list[AnthropicProvider | OpenAIProvider | AzureOpenAIProvider] = []
    provider = await construction.realize_owned(
        lambda: case.provider_type(runtime_settings),
        validate=lambda _provider: None,
        adopt=adopted.append,
        retire=lambda rejected: rejected.close(),
        reason_code=f"test_{case.name}_provider_construction",
    )
    assert adopted == [provider]
    for _ in range(100):
        if construction.active == 0:
            break
        await asyncio.sleep(0.001)
    assert construction.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    return provider


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [307, 308])
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_cross_origin_redirect_never_receives_prompt_or_credentials(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
    status_code: int,
) -> None:
    configured_requests: list[httpx.Request] = []
    redirect_requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "redirect-target.example":
            redirect_requests.append(request)
            return httpx.Response(200, json=_success_payload(case))
        configured_requests.append(request)
        return httpx.Response(
            status_code,
            headers={"location": f"{_REDIRECT_ORIGIN}/capture"},
        )

    _install_transport(monkeypatch, case, httpx.MockTransport(handler))
    provider = await _construct_owned_provider(case, _settings(case))
    http_client = provider._http_client
    assert isinstance(http_client, httpx.AsyncClient)
    try:
        with pytest.raises(case.error_type):
            await provider.chat_text("system", _PROMPT)
    finally:
        await provider.close()

    assert len(configured_requests) == 1
    assert _PROMPT.encode() in configured_requests[0].content
    assert _credential_value(case, configured_requests[0]) == _API_KEY
    assert redirect_requests == []
    assert http_client.is_closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_same_configured_endpoint_succeeds_and_closes_owned_transport(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://redirect-target.example:8080")
    monkeypatch.setenv("https_proxy", "http://redirect-target.example:8080")
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_success_payload(case))

    _install_transport(monkeypatch, case, httpx.MockTransport(handler))
    provider = await _construct_owned_provider(case, _settings(case))
    http_client = provider._http_client
    assert isinstance(http_client, httpx.AsyncClient)
    try:
        result = await provider.chat_text("system", _PROMPT)
        assert result.text == "ok"
        assert len(requests) == 1
        assert requests[0].url.scheme == "https"
        assert requests[0].url.host == "llm-owner.example"
        assert _PROMPT.encode() in requests[0].content
        assert _credential_value(case, requests[0]) == _API_KEY
        assert provider.runtime_ownership.remotes[0].endpoint == _CONFIGURED_ORIGIN
        assert str(http_client.base_url).rstrip("/") == _CONFIGURED_ORIGIN
        assert http_client.follow_redirects is False
        assert http_client.trust_env is False
    finally:
        await provider.close()

    assert http_client.is_closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_ambient_sdk_custom_headers_never_reach_the_wire(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    ambient_variable = "ANTHROPIC_CUSTOM_HEADERS" if case.name == "anthropic" else "OPENAI_CUSTOM_HEADERS"
    ambient_credential = "Bearer ambient-token" if case.name == "openai" else "ambient-token"
    monkeypatch.setenv(
        ambient_variable,
        "\n".join(
            (
                "Host: ambient-owner.example",
                f"{case.credential_header}: {ambient_credential}",
                "X-Tacit-Ambient: leaked",
            )
        ),
    )
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_success_payload(case))

    _install_transport(monkeypatch, case, httpx.MockTransport(handler))
    provider = await _construct_owned_provider(case, _settings(case))
    try:
        result = await provider.chat_text("system", _PROMPT)
    finally:
        await provider.close()

    assert result.text == "ok"
    assert len(requests) == 1
    request = requests[0]
    assert request.url.host == "llm-owner.example"
    assert request.headers["host"] == "llm-owner.example"
    assert _credential_value(case, request) == _API_KEY
    assert "x-tacit-ambient" not in request.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_adopted_sdk_credential_state_is_owned_only_by_settings(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    ambient_credentials = {
        "OPENAI_API_KEY": "ambient-openai-api-key",
        "OPENAI_ADMIN_KEY": "ambient-openai-admin-key",
        "OPENAI_WEBHOOK_SECRET": "ambient-openai-webhook-secret",
        "OPENAI_ORG_ID": "ambient-openai-organization",
        "OPENAI_PROJECT_ID": "ambient-openai-project",
        "AZURE_OPENAI_API_KEY": "ambient-azure-api-key",
        "AZURE_OPENAI_AD_TOKEN": "ambient-azure-ad-token",
        "ANTHROPIC_API_KEY": "ambient-anthropic-api-key",
        "ANTHROPIC_AUTH_TOKEN": "ambient-anthropic-auth-token",
        "ANTHROPIC_CONFIG_DIR": "/ambient-anthropic-config",
        "ANTHROPIC_PROFILE": "ambient-anthropic-profile",
        "ANTHROPIC_IDENTITY_TOKEN": "ambient-anthropic-identity-token",
        "ANTHROPIC_IDENTITY_TOKEN_FILE": "/ambient-anthropic-token",
        "ANTHROPIC_FEDERATION_RULE_ID": "ambient-anthropic-rule",
        "ANTHROPIC_ORGANIZATION_ID": "ambient-anthropic-organization",
        "ANTHROPIC_SERVICE_ACCOUNT_ID": "ambient-anthropic-service-account",
        "ANTHROPIC_WORKSPACE_ID": "ambient-anthropic-workspace",
        "ANTHROPIC_SCOPE": "ambient-anthropic-scope",
        "ANTHROPIC_WEBHOOK_SIGNING_KEY": "ambient-anthropic-webhook-key",
    }
    for name, value in ambient_credentials.items():
        monkeypatch.setenv(name, value)

    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_success_payload(case))

    _install_transport(monkeypatch, case, httpx.MockTransport(handler))
    provider = await _construct_owned_provider(case, _settings(case))
    client = provider._client
    assert client is not None
    try:
        if case.name == "anthropic":
            assert isinstance(client, anthropic.AsyncAnthropic)
            assert client.api_key == _API_KEY
            assert client.auth_token is None
            assert client.credentials is None
            assert client.webhook_key == ""
        else:
            assert isinstance(client, openai.AsyncOpenAI)
            assert client.api_key == _API_KEY
            assert client.admin_api_key == ""
            assert client.webhook_secret == ""
            assert client.organization == ""
            assert client.project == ""
            if case.name == "azure":
                assert getattr(client, "_azure_ad_token") is None
                assert getattr(client, "_azure_ad_token_provider") is None

        result = await provider.chat_text("system", _PROMPT)
    finally:
        await provider.close()

    assert result.text == "ok"
    assert len(requests) == 1
    assert _credential_value(case, requests[0]) == _API_KEY
    if case.name == "azure":
        assert "authorization" not in requests[0].headers
    assert {name: os.environ.get(name) for name in ambient_credentials} == ambient_credentials


@pytest.mark.asyncio
async def test_simultaneous_app_scoped_providers_isolate_conflicting_ambient_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OPENAI_CUSTOM_HEADERS",
        "\n".join(
            (
                "Host: ambient-openai.example",
                "Authorization: Bearer ambient-openai-token",
                "api-key: ambient-azure-token",
                "X-Tacit-Ambient: openai-leak",
            )
        ),
    )
    monkeypatch.setenv(
        "ANTHROPIC_CUSTOM_HEADERS",
        "\n".join(
            (
                "Host: ambient-anthropic.example",
                "x-api-key: ambient-anthropic-token",
                "X-Tacit-Ambient: anthropic-leak",
            )
        ),
    )
    cases = {case.name: case for case in _PROVIDER_CASES}
    endpoints = {name: f"https://{name}-owner.example" for name in cases}
    credentials = {name: f"{name}-credential" for name in cases}
    requests: dict[str, list[httpx.Request]] = {name: [] for name in cases}
    transports: dict[str, httpx.MockTransport] = {}

    for name, case in cases.items():

        async def handler(request: httpx.Request, *, case: _ProviderCase = case) -> httpx.Response:
            requests[case.name].append(request)
            return httpx.Response(200, json=_success_payload(case))

        transports[name] = httpx.MockTransport(handler)

    def factory(runtime_settings: Settings, *, endpoint: str) -> httpx.AsyncClient:
        name = httpx.URL(endpoint).host.removesuffix("-owner.example")
        return create_llm_sdk_http_client(
            runtime_settings,
            endpoint=endpoint,
            transport=transports[name],
        )

    monkeypatch.setattr("tacit.agents.providers.openai_provider.create_llm_sdk_http_client", factory)
    monkeypatch.setattr("tacit.agents.providers.anthropic.create_llm_sdk_http_client", factory)

    providers: dict[str, AnthropicProvider | OpenAIProvider | AzureOpenAIProvider] = {}
    for name, case in cases.items():
        runtime_settings = _settings(case).model_copy(
            update={
                "llm_api_base": endpoints[name],
                "llm_api_key": credentials[name],
            }
        )
        providers[name] = await _construct_owned_provider(case, runtime_settings)

    try:
        results = await asyncio.gather(
            *(provider.chat_text("system", f"{_PROMPT} for {name}") for name, provider in providers.items())
        )
    finally:
        await asyncio.gather(*(provider.close() for provider in providers.values()))

    assert [result.text for result in results] == ["ok", "ok", "ok"]
    for name, case in cases.items():
        assert len(requests[name]) == 1
        request = requests[name][0]
        assert request.url.host == f"{name}-owner.example"
        assert request.headers["host"] == f"{name}-owner.example"
        assert _credential_value(case, request) == credentials[name]
        assert "x-tacit-ambient" not in request.headers


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_generation_owner_retains_exclusive_transport_close_authority(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    _install_transport(monkeypatch, case, httpx.MockTransport(lambda _request: httpx.Response(500)))
    provider = await _construct_owned_provider(case, _settings(case))
    http_client = provider._http_client
    assert isinstance(http_client, httpx.AsyncClient)
    lifecycle_owner = object()
    lifecycle_calls = 0

    async def invoke(operation: Callable[[], Awaitable[Any]]) -> Any:
        nonlocal lifecycle_calls
        lifecycle_calls += 1
        return await operation()

    provider.bind_lifecycle_invoker(owner=lifecycle_owner, invoke=invoke)

    with pytest.raises(RuntimeOwnershipError, match="runtime owner"):
        await provider.close()
    assert http_client.is_closed is False

    await provider.close_from_lifecycle(owner=lifecycle_owner)

    assert lifecycle_calls == 1
    assert http_client.is_closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_provider_closes_owned_transport_even_if_sdk_close_does_not(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    _install_transport(monkeypatch, case, httpx.MockTransport(lambda _request: httpx.Response(500)))
    provider = await _construct_owned_provider(case, _settings(case))
    http_client = provider._http_client
    assert isinstance(http_client, httpx.AsyncClient)

    class NonClosingSDKClient:
        close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    sdk_client = NonClosingSDKClient()
    setattr(provider, "_client", sdk_client)

    await provider.close()

    assert sdk_client.close_calls == 1
    assert http_client.is_closed is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_sdk_close_failure_remains_retryable_until_success(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    _install_transport(monkeypatch, case, httpx.MockTransport(lambda _request: httpx.Response(500)))
    provider = await _construct_owned_provider(case, _settings(case))
    http_client = provider._http_client
    assert isinstance(http_client, httpx.AsyncClient)

    first_failure = RuntimeError("sdk close failed once")

    class FailOnceSDKClient:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise first_failure

    sdk_client = FailOnceSDKClient()
    setattr(provider, "_client", sdk_client)

    with pytest.raises(RuntimeError, match="sdk close failed once") as first_close:
        await provider.close()
    await provider.close()
    await provider.close()

    assert first_close.value is first_failure
    assert sdk_client.close_calls == 2
    assert http_client.is_closed is True


def test_shared_policy_derives_transport_bounds_without_new_admission_owner() -> None:
    runtime_settings = _settings(_PROVIDER_CASES[1])
    policy = llm_sdk_http_policy(runtime_settings)

    assert policy.follow_redirects is False
    assert policy.trust_env is False
    assert policy.connect_timeout_seconds == 10
    assert policy.read_timeout_seconds == runtime_settings.pipeline_timeout_seconds
    assert policy.write_timeout_seconds == 30
    assert policy.pool_timeout_seconds == 10
    assert policy.max_connections == runtime_settings.pipeline_max_concurrent
    assert policy.max_keepalive_connections == runtime_settings.pipeline_max_concurrent
    assert policy.keepalive_expiry_seconds == 30
    assert not hasattr(policy, "semaphore")
    assert not hasattr(policy, "admission")

    high_concurrency = runtime_settings.model_copy(update={"pipeline_max_concurrent": 1_000})
    high_concurrency_policy = llm_sdk_http_policy(high_concurrency)
    assert high_concurrency_policy.max_connections == 1_000
    assert high_concurrency_policy.max_keepalive_connections == 20


def test_custom_header_isolation_fails_closed_for_unsupported_sdk_storage() -> None:
    with pytest.raises(RuntimeOwnershipError, match="custom-header isolation is unavailable"):
        isolate_llm_sdk_custom_headers(object())


def test_ambient_credential_isolation_fails_closed_for_unsupported_sdk_state() -> None:
    with pytest.raises(RuntimeOwnershipError, match="ambient credential isolation is unavailable"):
        isolate_llm_sdk_ambient_credentials(
            object(),
            expected_fields=(("admin_api_key", ""),),
        )


@pytest.mark.parametrize("failure_phase", ["sdk_construction", "header_isolation"])
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
def test_provider_construction_failure_closes_unadopted_transport_once(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
    failure_phase: str,
) -> None:
    """A constructor failure cannot strand a transport outside lifecycle ownership."""

    class TransportProbe:
        def __init__(self) -> None:
            self.is_closed = False
            self.close_calls = 0

        async def aclose(self) -> None:
            self.close_calls += 1
            self.is_closed = True

    class SDKProbe:
        def __init__(self) -> None:
            self.api_key = _API_KEY
            self.admin_api_key = ""
            self.workload_identity = None
            self._api_key_provider = None
            self.organization = ""
            self.project = ""
            self.webhook_secret = ""
            self.auth_token = None
            self.credentials = None
            self.webhook_key = ""
            self._azure_ad_token = ""
            self._azure_ad_token_provider = None
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    transport = TransportProbe()
    sdk = SDKProbe()
    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )
    sdk_constructor = {
        "anthropic": "anthropic.AsyncAnthropic",
        "openai": "openai.AsyncOpenAI",
        "azure": "openai.AsyncAzureOpenAI",
    }[case.name]

    monkeypatch.setattr(
        f"{module}.create_llm_sdk_http_client",
        lambda *_args, **_kwargs: transport,
    )

    if failure_phase == "sdk_construction":

        def fail_sdk_construction(**_kwargs: Any) -> SDKProbe:
            raise RuntimeError("sdk construction failed")

        monkeypatch.setattr(f"{module}.{sdk_constructor}", fail_sdk_construction)
    else:
        monkeypatch.setattr(f"{module}.{sdk_constructor}", lambda **_kwargs: sdk)

        def fail_header_isolation(_client: object) -> None:
            raise RuntimeError("header isolation failed")

        monkeypatch.setattr(f"{module}.isolate_llm_sdk_custom_headers", fail_header_isolation)

    with pytest.raises(RuntimeError, match="failed"):
        case.provider_type(_settings(case))

    assert transport.close_calls == 1
    assert transport.is_closed is True
    assert sdk.close_calls == (1 if failure_phase == "header_isolation" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_direct_running_loop_construction_fails_before_sdk_allocation(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    allocations = 0
    started_threads: list[str] = []
    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )
    real_start = threading.Thread.start

    def create_transport(*_args: Any, **_kwargs: Any) -> object:
        nonlocal allocations
        allocations += 1
        raise AssertionError("transport allocation must remain unreachable")

    def track_thread_start(thread: threading.Thread) -> None:
        started_threads.append(thread.name)
        real_start(thread)

    monkeypatch.setattr(f"{module}.create_llm_sdk_http_client", create_transport)
    monkeypatch.setattr(threading.Thread, "start", track_thread_start)

    with pytest.raises(RuntimeOwnershipError, match="lifecycle-owned construction boundary"):
        case.provider_type(_settings(case))

    assert allocations == 0
    assert started_threads == []


@pytest.mark.asyncio
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
async def test_owned_worker_start_failure_precedes_sdk_allocation(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    runtime_settings = _settings(case)
    lifecycle = PipelineAdmissionController(
        runtime_settings.pipeline_max_concurrent,
        max_queued=runtime_settings.pipeline_max_queued,
    )
    construction = LifecycleOwnedBlockingWork(lifecycle)
    allocations = 0
    real_start = threading.Thread.start
    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )

    def create_transport(*_args: Any, **_kwargs: Any) -> object:
        nonlocal allocations
        allocations += 1
        raise AssertionError("transport allocation must remain unreachable")

    def fail_construction_worker_start(thread: threading.Thread) -> None:
        if thread.name == "tacit-lifecycle-blocking-work":
            raise RuntimeError("synthetic constructor worker start failure")
        real_start(thread)

    monkeypatch.setattr(f"{module}.create_llm_sdk_http_client", create_transport)
    monkeypatch.setattr(threading.Thread, "start", fail_construction_worker_start)

    with pytest.raises(RuntimeError, match="blocking worker could not start"):
        await construction.realize_owned(
            lambda: case.provider_type(runtime_settings),
            validate=lambda _provider: None,
            adopt=lambda _provider: None,
            retire=lambda rejected: rejected.close(),
            reason_code=f"test_{case.name}_provider_start_failure",
        )

    assert allocations == 0
    assert construction.active == 0
    assert lifecycle.in_flight == 0
    assert lifecycle.blocking_in_flight == 0
    assert lifecycle.retained == 0


@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
def test_rollback_quarantine_owner_start_failure_precedes_sdk_allocation(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    allocations = 0
    real_start = threading.Thread.start
    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )

    def create_transport(*_args: Any, **_kwargs: Any) -> object:
        nonlocal allocations
        allocations += 1
        raise AssertionError("transport allocation must remain unreachable")

    def fail_quarantine_owner_start(thread: threading.Thread) -> None:
        if thread.name == "tacit-llm-constructor-rollback-quarantine":
            raise RuntimeError("synthetic quarantine owner start failure")
        real_start(thread)

    monkeypatch.setattr(http_transport, "_CONSTRUCTION_ROLLBACK_QUARANTINE", quarantine)
    monkeypatch.setattr(f"{module}.create_llm_sdk_http_client", create_transport)
    monkeypatch.setattr(threading.Thread, "start", fail_quarantine_owner_start)

    with pytest.raises(RuntimeOwnershipError, match="quarantine owner could not start"):
        case.provider_type(_settings(case))

    snapshot = quarantine.snapshot()
    assert allocations == 0
    assert snapshot.slots == 0
    assert snapshot.owner_threads == 0


def test_rollback_quarantine_owner_start_transition_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    owner_start_entered = threading.Event()
    release_owner_start = threading.Event()

    def stall_owner_start(**_kwargs: object) -> threading.Thread:
        owner_start_entered.set()
        release_owner_start.wait(timeout=1.0)
        raise RuntimeError("synthetic delayed owner start")

    monkeypatch.setattr(
        http_transport,
        "_CONSTRUCTION_ROLLBACK_OWNER_STARTUP_TIMEOUT_SECONDS",
        0.05,
    )
    monkeypatch.setattr(
        http_transport,
        "_start_lifecycle_owner_thread",
        stall_owner_start,
        raising=False,
    )

    started_at = time.monotonic()
    try:
        with pytest.raises(RuntimeOwnershipError, match="readiness timed out"):
            quarantine.reserve()
    finally:
        release_owner_start.set()

    assert owner_start_entered.wait(timeout=0.25)
    assert time.monotonic() - started_at < 0.25
    assert quarantine.snapshot().slots == 0


def test_rollback_quarantine_ambiguous_owner_start_remains_fenced(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)

    def fail_owner_start(**_kwargs: object) -> threading.Thread:
        raise http_transport._LifecycleOwnerStartupError(
            phase="start",
            cause=RuntimeError("synthetic ambiguous owner start"),
            thread_alive=True,
        )

    monkeypatch.setattr(
        http_transport,
        "_start_lifecycle_owner_thread",
        fail_owner_start,
        raising=False,
    )

    with pytest.raises(RuntimeOwnershipError, match="owner is unavailable"):
        quarantine.reserve()

    snapshot = quarantine.snapshot()
    assert snapshot.slots == 0
    assert snapshot.owner_threads == 0


def test_rollback_quarantine_completion_never_waits_on_requester_registry_lock() -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    operation_started = threading.Event()
    release_operation = threading.Event()
    heartbeat = threading.Event()
    callback_registered = threading.Event()
    rollback_finished = threading.Event()
    rollback_errors: list[BaseException | None] = []

    async def rollback_operation() -> None:
        operation_started.set()
        while not release_operation.is_set():
            await asyncio.sleep(0.001)

    def submit_rollback() -> None:
        try:
            rollback_errors.append(
                quarantine.rollback(
                    reservation,
                    rollback_operation,
                    deadline=time.monotonic() + 1.0,
                )
            )
        finally:
            rollback_finished.set()

    submitter = threading.Thread(target=submit_rollback)
    submitter.start()
    assert operation_started.wait(timeout=1.0)

    task = reservation._task
    assert task is not None
    assert quarantine._loop is not None

    def register_heartbeat() -> None:
        task.add_done_callback(lambda _completed: heartbeat.set())
        callback_registered.set()

    quarantine._loop.call_soon_threadsafe(register_heartbeat)
    assert callback_registered.wait(timeout=1.0)

    quarantine._lock.acquire()
    try:
        release_operation.set()
        assert heartbeat.wait(timeout=0.25), "durable cleanup owner blocked on the requester registry lock"
        assert rollback_finished.wait(timeout=0.25)
    finally:
        quarantine._lock.release()
        submitter.join(timeout=1.0)

    assert submitter.is_alive() is False
    assert rollback_errors == [None]
    for _ in range(100):
        if quarantine.snapshot().slots == 0:
            break
        time.sleep(0.01)
    assert quarantine.snapshot().slots == 0


def test_rollback_quarantine_startup_never_waits_on_requester_reservation_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=2)
    blocked = quarantine.reserve()
    unrelated = quarantine.reserve()
    assert quarantine._loop is not None
    real_call_soon_threadsafe = quarantine._loop.call_soon_threadsafe
    blocked_dispatch_ready = threading.Event()
    allow_blocked_dispatch = threading.Event()
    blocked_transition_entered = threading.Event()
    blocked_operation_started = threading.Event()
    release_blocked_operation = threading.Event()
    unrelated_operation_started = threading.Event()
    heartbeat = threading.Event()
    blocked_errors: list[BaseException | None] = []
    unrelated_errors: list[BaseException | None] = []
    original_start_submission = quarantine._start_submission

    def observed_start_submission(
        reservation: http_transport._ConstructionRollbackReservation,
    ) -> None:
        blocked_transition_entered.set()
        original_start_submission(reservation)

    monkeypatch.setattr(quarantine, "_start_submission", observed_start_submission)

    def intercept_call_soon_threadsafe(
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.Handle:
        if callback is observed_start_submission and args and args[0] is blocked:
            blocked_dispatch_ready.set()
            assert allow_blocked_dispatch.wait(timeout=1.0)
        return real_call_soon_threadsafe(callback, *args, context=context)

    monkeypatch.setattr(quarantine._loop, "call_soon_threadsafe", intercept_call_soon_threadsafe)

    async def blocked_operation() -> None:
        blocked_operation_started.set()
        while not release_blocked_operation.is_set():
            await asyncio.sleep(0.001)

    async def unrelated_operation() -> None:
        unrelated_operation_started.set()

    blocked_submitter = threading.Thread(
        target=lambda: blocked_errors.append(
            quarantine.rollback(
                blocked,
                blocked_operation,
                deadline=time.monotonic() + 2.0,
            )
        )
    )
    unrelated_submitter = threading.Thread(
        target=lambda: unrelated_errors.append(
            quarantine.rollback(
                unrelated,
                unrelated_operation,
                deadline=time.monotonic() + 2.0,
            )
        )
    )
    blocked_submitter.start()
    assert blocked_dispatch_ready.wait(timeout=1.0)

    blocked._lock.acquire()
    try:
        allow_blocked_dispatch.set()
        assert blocked_transition_entered.wait(timeout=1.0)
        unrelated_submitter.start()
        real_call_soon_threadsafe(heartbeat.set)

        assert heartbeat.wait(timeout=0.25), "rollback owner blocked during startup publication"
        assert unrelated_operation_started.wait(timeout=0.25), "unrelated rollback slot did not progress"
        assert blocked_operation_started.wait(timeout=0.25)
    finally:
        blocked._lock.release()
        release_blocked_operation.set()
        allow_blocked_dispatch.set()
        blocked_submitter.join(timeout=2.0)
        unrelated_submitter.join(timeout=2.0)

    assert blocked_submitter.is_alive() is False
    assert unrelated_submitter.is_alive() is False
    assert blocked_errors == [None]
    assert unrelated_errors == [None]


def test_rollback_quarantine_cancellation_never_waits_on_requester_reservation_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=2)
    blocked = quarantine.reserve()
    unrelated = quarantine.reserve()
    assert quarantine._loop is not None
    real_call_soon_threadsafe = quarantine._loop.call_soon_threadsafe
    cancellation_dispatch_ready = threading.Event()
    allow_cancellation_dispatch = threading.Event()
    cancellation_transition_entered = threading.Event()
    blocked_operation_started = threading.Event()
    cancellation_observed = threading.Event()
    unrelated_operation_started = threading.Event()
    heartbeat = threading.Event()
    blocked_errors: list[BaseException | None] = []
    unrelated_errors: list[BaseException | None] = []
    original_cancel_submission = quarantine._cancel_submission

    def observed_cancel_submission(
        reservation: http_transport._ConstructionRollbackReservation,
    ) -> None:
        cancellation_transition_entered.set()
        original_cancel_submission(reservation)

    monkeypatch.setattr(quarantine, "_cancel_submission", observed_cancel_submission)

    def intercept_call_soon_threadsafe(
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.Handle:
        if callback is observed_cancel_submission and args and args[0] is blocked:
            cancellation_dispatch_ready.set()
            assert allow_cancellation_dispatch.wait(timeout=1.0)
        return real_call_soon_threadsafe(callback, *args, context=context)

    monkeypatch.setattr(quarantine._loop, "call_soon_threadsafe", intercept_call_soon_threadsafe)

    async def blocked_operation() -> None:
        blocked_operation_started.set()
        try:
            while True:
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            cancellation_observed.set()
            raise

    async def unrelated_operation() -> None:
        unrelated_operation_started.set()

    blocked_submitter = threading.Thread(
        target=lambda: blocked_errors.append(
            quarantine.rollback(
                blocked,
                blocked_operation,
                deadline=time.monotonic() + 0.05,
            )
        )
    )
    unrelated_submitter = threading.Thread(
        target=lambda: unrelated_errors.append(
            quarantine.rollback(
                unrelated,
                unrelated_operation,
                deadline=time.monotonic() + 2.0,
            )
        )
    )
    blocked_submitter.start()
    assert blocked_operation_started.wait(timeout=1.0)
    assert cancellation_dispatch_ready.wait(timeout=1.0)

    blocked._lock.acquire()
    try:
        allow_cancellation_dispatch.set()
        assert cancellation_transition_entered.wait(timeout=1.0)
        unrelated_submitter.start()
        real_call_soon_threadsafe(heartbeat.set)

        assert heartbeat.wait(timeout=0.25), "rollback owner blocked while claiming cancellation"
        assert unrelated_operation_started.wait(timeout=0.25), "unrelated rollback slot did not progress"
        assert cancellation_observed.wait(timeout=0.25)
    finally:
        blocked._lock.release()
        allow_cancellation_dispatch.set()
        blocked_submitter.join(timeout=2.0)
        unrelated_submitter.join(timeout=2.0)

    assert blocked_submitter.is_alive() is False
    assert unrelated_submitter.is_alive() is False
    assert len(blocked_errors) == 1
    assert isinstance(blocked_errors[0], TimeoutError)
    assert unrelated_errors == [None]


def test_rollback_quarantine_completion_keeps_capacity_while_requester_lock_is_held() -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=2)
    blocked = quarantine.reserve()
    unrelated = quarantine.reserve()
    assert quarantine._loop is not None
    blocked_operation_started = threading.Event()
    release_blocked_operation = threading.Event()
    unrelated_operation_started = threading.Event()
    heartbeat = threading.Event()
    blocked_errors: list[BaseException | None] = []
    unrelated_errors: list[BaseException | None] = []

    async def blocked_operation() -> None:
        blocked_operation_started.set()
        while not release_blocked_operation.is_set():
            await asyncio.sleep(0.001)

    async def unrelated_operation() -> None:
        unrelated_operation_started.set()

    blocked_submitter = threading.Thread(
        target=lambda: blocked_errors.append(
            quarantine.rollback(
                blocked,
                blocked_operation,
                deadline=time.monotonic() + 2.0,
            )
        )
    )
    unrelated_submitter = threading.Thread(
        target=lambda: unrelated_errors.append(
            quarantine.rollback(
                unrelated,
                unrelated_operation,
                deadline=time.monotonic() + 2.0,
            )
        )
    )
    blocked_submitter.start()
    assert blocked_operation_started.wait(timeout=1.0)

    blocked._lock.acquire()
    try:
        release_blocked_operation.set()
        unrelated_submitter.start()
        quarantine._loop.call_soon_threadsafe(heartbeat.set)

        assert heartbeat.wait(timeout=0.25), "rollback owner blocked during terminal publication"
        assert unrelated_operation_started.wait(timeout=0.25), "unrelated rollback slot did not progress"
        with quarantine._lock:
            assert quarantine._slots.get(blocked.token) is blocked
    finally:
        blocked._lock.release()
        blocked_submitter.join(timeout=2.0)
        unrelated_submitter.join(timeout=2.0)

    assert blocked_submitter.is_alive() is False
    assert unrelated_submitter.is_alive() is False
    assert blocked_errors == [None]
    assert unrelated_errors == [None]
    for _ in range(100):
        with quarantine._lock:
            if blocked.token not in quarantine._slots:
                break
        time.sleep(0.01)
    with quarantine._lock:
        assert blocked.token not in quarantine._slots


def test_rollback_quarantine_retries_transient_owner_dispatch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    assert quarantine._loop is not None
    real_call_soon_threadsafe = quarantine._loop.call_soon_threadsafe
    dispatch_attempts = 0
    operation_started = threading.Event()

    def flaky_call_soon_threadsafe(
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.Handle:
        nonlocal dispatch_attempts
        if getattr(callback, "__name__", "") == "_start_submission":
            dispatch_attempts += 1
            if dispatch_attempts == 1:
                raise RuntimeError("synthetic transient owner dispatch failure")
        return real_call_soon_threadsafe(callback, *args, context=context)

    monkeypatch.setattr(quarantine._loop, "call_soon_threadsafe", flaky_call_soon_threadsafe)

    async def rollback_operation() -> None:
        operation_started.set()

    assert (
        quarantine.rollback(
            reservation,
            rollback_operation,
            deadline=time.monotonic() + 1.0,
        )
        is None
    )
    assert operation_started.is_set()
    assert dispatch_attempts == 2
    for _ in range(100):
        if quarantine.snapshot().slots == 0:
            break
        time.sleep(0.01)
    assert quarantine.snapshot().slots == 0


def test_rollback_quarantine_persistent_owner_dispatch_failure_is_fenced_and_charged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    assert quarantine._loop is not None
    real_call_soon_threadsafe = quarantine._loop.call_soon_threadsafe
    dispatch_attempts = 0
    operation_started = threading.Event()

    def fail_start_dispatch(
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> asyncio.Handle:
        nonlocal dispatch_attempts
        if getattr(callback, "__name__", "") == "_start_submission":
            dispatch_attempts += 1
            raise RuntimeError("synthetic persistent owner dispatch failure")
        return real_call_soon_threadsafe(callback, *args, context=context)

    monkeypatch.setattr(quarantine._loop, "call_soon_threadsafe", fail_start_dispatch)

    async def rollback_operation() -> None:
        operation_started.set()

    started_at = time.monotonic()
    error = quarantine.rollback(
        reservation,
        rollback_operation,
        deadline=started_at + 0.05,
    )

    assert time.monotonic() - started_at < 0.25
    assert isinstance(error, TimeoutError)
    assert isinstance(error.__cause__, RuntimeError)
    assert operation_started.is_set() is False
    assert dispatch_attempts >= 2
    snapshot = quarantine.snapshot()
    assert snapshot.slots == 1
    assert snapshot.owner_threads == 1


def test_rollback_quarantine_retries_transient_task_creation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    real_create_task = asyncio.create_task
    create_attempts = 0
    operation_started = threading.Event()

    def flaky_create_task(
        coroutine: Coroutine[object, object, None],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[None]:
        nonlocal create_attempts
        if threading.current_thread().name == http_transport._CONSTRUCTION_ROLLBACK_THREAD_NAME:
            create_attempts += 1
            if create_attempts == 1:
                raise RuntimeError("synthetic transient task creation failure")
        return real_create_task(coroutine, name=name, context=context)

    monkeypatch.setattr(http_transport.asyncio, "create_task", flaky_create_task)

    async def rollback_operation() -> None:
        operation_started.set()

    assert (
        quarantine.rollback(
            reservation,
            rollback_operation,
            deadline=time.monotonic() + 1.0,
        )
        is None
    )
    assert operation_started.is_set()
    assert create_attempts == 2
    for _ in range(100):
        if quarantine.snapshot().slots == 0:
            break
        time.sleep(0.01)
    assert quarantine.snapshot().slots == 0


def test_rollback_quarantine_persistent_task_creation_failure_is_fenced_and_charged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    real_create_task = asyncio.create_task
    create_attempts = 0
    operation_started = threading.Event()

    def fail_owner_create_task(
        coroutine: Coroutine[object, object, None],
        *,
        name: str | None = None,
        context: Context | None = None,
    ) -> asyncio.Task[None]:
        nonlocal create_attempts
        if threading.current_thread().name == http_transport._CONSTRUCTION_ROLLBACK_THREAD_NAME:
            create_attempts += 1
            raise RuntimeError("synthetic persistent task creation failure")
        return real_create_task(coroutine, name=name, context=context)

    monkeypatch.setattr(http_transport.asyncio, "create_task", fail_owner_create_task)

    async def rollback_operation() -> None:
        operation_started.set()

    error = quarantine.rollback(
        reservation,
        rollback_operation,
        deadline=time.monotonic() + 0.05,
    )

    assert isinstance(error, RuntimeOwnershipError)
    assert "task could not be created" in str(error)
    assert isinstance(error.__cause__, RuntimeError)
    assert operation_started.is_set() is False
    assert create_attempts >= 2
    snapshot = quarantine.snapshot()
    assert snapshot.slots == 1
    assert snapshot.owner_threads == 1


def test_rollback_quarantine_requester_lock_cannot_defeat_deadline() -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    operation_started = threading.Event()
    release_operation = threading.Event()
    rollback_finished = threading.Event()
    heartbeat = threading.Event()
    rollback_errors: list[BaseException | None] = []

    async def rollback_operation() -> None:
        operation_started.set()
        while not release_operation.is_set():
            await asyncio.sleep(0.001)

    def submit_rollback() -> None:
        try:
            rollback_errors.append(
                quarantine.rollback(
                    reservation,
                    rollback_operation,
                    deadline=time.monotonic() + 0.05,
                )
            )
        finally:
            rollback_finished.set()

    reservation._lock.acquire()
    submitter = threading.Thread(target=submit_rollback)
    submitter.start()
    try:
        assert operation_started.wait(timeout=0.25)
        assert rollback_finished.wait(timeout=0.25), "requester lock defeated rollback deadline"
        assert len(rollback_errors) == 1
        assert isinstance(rollback_errors[0], TimeoutError)
        assert quarantine._loop is not None
        quarantine._loop.call_soon_threadsafe(heartbeat.set)
        assert heartbeat.wait(timeout=0.25)
        with quarantine._lock:
            assert quarantine._slots.get(reservation.token) is reservation
        release_operation.set()
        time.sleep(0.05)
        with quarantine._lock:
            assert quarantine._slots.get(reservation.token) is reservation
    finally:
        reservation._lock.release()
        release_operation.set()
        submitter.join(timeout=1.0)

    assert submitter.is_alive() is False
    for _ in range(100):
        if quarantine.snapshot().slots == 0:
            break
        time.sleep(0.01)
    assert quarantine.snapshot().slots == 0


def test_rollback_quarantine_tracks_descendants_until_actual_retirement() -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    descendant_started = threading.Event()
    descendant_cancelled = threading.Event()
    release_descendant = threading.Event()
    rollback_finished = threading.Event()
    rollback_errors: list[BaseException | None] = []

    async def descendant() -> None:
        descendant_started.set()
        try:
            while not release_descendant.is_set():
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            descendant_cancelled.set()
            while not release_descendant.is_set():
                await asyncio.sleep(0.001)

    async def rollback_operation() -> None:
        asyncio.create_task(descendant())

    def submit_rollback() -> None:
        try:
            rollback_errors.append(
                quarantine.rollback(
                    reservation,
                    rollback_operation,
                    deadline=time.monotonic() + 0.05,
                )
            )
        finally:
            rollback_finished.set()

    submitter = threading.Thread(target=submit_rollback)
    submitter.start()
    try:
        assert descendant_started.wait(timeout=1.0)
        assert rollback_finished.wait(timeout=0.5)
        assert len(rollback_errors) == 1
        assert isinstance(rollback_errors[0], TimeoutError)
        assert descendant_cancelled.wait(timeout=0.5)
        snapshot = quarantine.snapshot()
        assert snapshot.slots == 1
        assert snapshot.owner_threads == 1
        with pytest.raises(RuntimeOwnershipError, match="capacity is exhausted"):
            quarantine.reserve()
    finally:
        release_descendant.set()
        submitter.join(timeout=1.0)

    assert submitter.is_alive() is False
    for _ in range(100):
        if quarantine.snapshot().slots == 0:
            break
        time.sleep(0.01)
    assert quarantine.snapshot().slots == 0


@pytest.mark.parametrize("executor_kind", ["default", "explicit"])
def test_rollback_quarantine_rejects_executor_offload_and_keeps_slot_fenced(
    executor_kind: str,
) -> None:
    quarantine = http_transport._ConstructionRollbackQuarantine(capacity=1)
    reservation = quarantine.reserve()
    offloaded_work_ran = threading.Event()
    offload_rejected = threading.Event()
    submitted = threading.Event()

    class ExecutorProbe:
        def submit(self, fn: Callable[..., object], /, *args: object, **kwargs: object) -> Future[object]:
            submitted.set()
            future: Future[object] = Future()
            try:
                future.set_result(fn(*args, **kwargs))
            except BaseException as error:
                future.set_exception(error)
            return future

    executor = cast(Executor, ExecutorProbe())

    async def rollback_operation() -> None:
        try:
            if executor_kind == "default":
                await asyncio.to_thread(offloaded_work_ran.set)
            else:
                await asyncio.get_running_loop().run_in_executor(executor, offloaded_work_ran.set)
        except RuntimeOwnershipError:
            offload_rejected.set()

    error = quarantine.rollback(
        reservation,
        rollback_operation,
        deadline=time.monotonic() + 1.0,
    )

    assert isinstance(error, RuntimeOwnershipError)
    assert "executor offload is unsupported" in str(error)
    assert offload_rejected.wait(timeout=0.25)
    assert offloaded_work_ran.is_set() is False
    assert submitted.is_set() is False
    snapshot = quarantine.snapshot()
    assert snapshot.slots == 1
    assert snapshot.owner_threads == 1
    with pytest.raises(RuntimeOwnershipError, match="capacity is exhausted"):
        quarantine.reserve()


@pytest.mark.parametrize("cleanup_fault", ["failure", "timeout"])
@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
def test_constructor_rollback_failure_preserves_primary_cause_and_attempts_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
    cleanup_fault: str,
) -> None:
    class TransportProbe:
        def __init__(self) -> None:
            self.is_closed = False
            self.close_calls = 0
            self.closed = threading.Event()

        async def aclose(self) -> None:
            self.close_calls += 1
            self.is_closed = True
            self.closed.set()

    class SDKProbe:
        def __init__(self) -> None:
            self._custom_headers: dict[str, str] = {}
            self.api_key = _API_KEY
            self.admin_api_key = ""
            self.workload_identity = None
            self._api_key_provider = None
            self.organization = ""
            self.project = ""
            self.webhook_secret = ""
            self.auth_token = None
            self.credentials = None
            self.webhook_key = ""
            self._azure_ad_token = ""
            self._azure_ad_token_provider = None
            self.close_calls = 0
            self.cancelled = False
            self.cancellation_observed = threading.Event()

        async def close(self) -> None:
            self.close_calls += 1
            if cleanup_fault == "failure":
                raise RuntimeError("synthetic SDK rollback failure")
            try:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                self.cancelled = True
                self.cancellation_observed.set()
                raise

    transport = TransportProbe()
    sdk = SDKProbe()
    primary = RuntimeError("synthetic header isolation failure")
    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )
    sdk_constructor = {
        "anthropic": "anthropic.AsyncAnthropic",
        "openai": "openai.AsyncOpenAI",
        "azure": "openai.AsyncAzureOpenAI",
    }[case.name]

    monkeypatch.setattr(f"{module}.create_llm_sdk_http_client", lambda *_args, **_kwargs: transport)
    monkeypatch.setattr(f"{module}.{sdk_constructor}", lambda **_kwargs: sdk)
    monkeypatch.setattr(
        f"{module}.isolate_llm_sdk_custom_headers",
        lambda _client: (_ for _ in ()).throw(primary),
    )
    monkeypatch.setattr(
        "tacit.agents.providers.http_transport._CONSTRUCTION_ROLLBACK_TIMEOUT_SECONDS",
        0.01,
    )

    with pytest.raises(RuntimeOwnershipError, match="rollback did not complete") as exc_info:
        case.provider_type(_settings(case))

    assert exc_info.value.__cause__ is primary
    expected_reason = {
        "failure": "llm_sdk_construction_rollback_failed",
        "timeout": "llm_sdk_construction_rollback_timeout",
    }[cleanup_fault]
    assert getattr(exc_info.value, "cleanup_reason_code") == expected_reason
    assert getattr(exc_info.value, "cleanup_error_type") == (
        "TimeoutError" if cleanup_fault == "timeout" else "RuntimeError"
    )
    assert getattr(exc_info.value, "cleanup_retains_capacity") is True
    assert sdk.close_calls == 1
    if cleanup_fault == "timeout":
        assert sdk.cancellation_observed.wait(timeout=0.5)
        assert transport.closed.wait(timeout=0.5)
    assert transport.close_calls == 1
    assert transport.is_closed is True
    if cleanup_fault == "timeout":
        assert sdk.cancelled is True


def test_cancellation_resistant_constructor_rollback_is_bounded_and_fences_runtime() -> None:
    """A hostile SDK close cannot retain its admitted constructor worker."""
    source = textwrap.dedent("""
        import asyncio
        import json
        import threading
        import time

        import tacit.agents.providers.http_transport as http_transport
        from tacit.agents.providers.http_transport import LLMSDKHTTPClientConstruction
        from tacit.errors import RuntimeOwnershipError
        from tacit.pipeline.side_effects import LifecycleOwnedBlockingWork
        from tacit.pipeline_admission import PipelineAdmissionController

        http_transport._CONSTRUCTION_ROLLBACK_TIMEOUT_SECONDS = 0.05


        class TransportProbe:
            def __init__(self):
                self.is_closed = False
                self.close_calls = 0

            async def aclose(self):
                self.close_calls += 1
                self.is_closed = True


        class CancellationResistantSDK:
            def __init__(self):
                self.close_calls = 0
                self.cancelled = 0

            async def close(self):
                self.close_calls += 1
                while True:
                    try:
                        await asyncio.sleep(3600)
                    except asyncio.CancelledError:
                        self.cancelled += 1


        async def main():
            lifecycle = PipelineAdmissionController(1, max_queued=0)
            graph = lifecycle.execution_graph
            root = graph.register_root_owner()
            construction_work = LifecycleOwnedBlockingWork(lifecycle)
            transport = TransportProbe()
            sdk = CancellationResistantSDK()
            primary = ValueError("primary constructor failure")

            def construct():
                with LLMSDKHTTPClientConstruction.begin() as construction:
                    construction.create_http_client(lambda: transport)
                    construction.create_sdk_client(lambda: sdk)
                    failure_started[0] = time.monotonic()
                    raise primary

            failure_started = [0.0]
            try:
                await construction_work.realize_owned(
                    construct,
                    validate=lambda _provider: None,
                    adopt=lambda _provider: None,
                    retire=lambda _provider: None,
                    reason_code="provider:llm_realization",
                )
            except RuntimeOwnershipError as error:
                constructor_error = error
            else:
                raise AssertionError("constructor rollback unexpectedly completed")
            elapsed = time.monotonic() - failure_started[0]

            before_drain = lifecycle.health_snapshot()
            owner = http_transport._construction_rollback_quarantine_snapshot()
            try:
                await construction_work.run(
                    lambda: None,
                    reason_code="post_constructor_fatal_probe",
                )
            except RuntimeOwnershipError as error:
                post_fence_rejected = bool(getattr(error, "runtime_provider_fatal", False))
            else:
                post_fence_rejected = False

            await asyncio.wait_for(graph.release_root_owner(root), timeout=1.0)
            after_drain = lifecycle.health_snapshot()

            quarantine = http_transport._CONSTRUCTION_ROLLBACK_QUARANTINE
            additional_reservations = [
                quarantine.reserve()
                for _ in range(owner.capacity - owner.slots)
            ]
            saturated_owner = quarantine.snapshot()
            saturation_rejected = [False]
            allocation_attempts = [0]

            def attempt_construction_at_capacity():
                try:
                    with LLMSDKHTTPClientConstruction.begin():
                        allocation_attempts[0] += 1
                except RuntimeOwnershipError:
                    saturation_rejected[0] = True

            saturation_probe = threading.Thread(target=attempt_construction_at_capacity)
            saturation_probe.start()
            saturation_probe.join(timeout=1.0)
            for reservation in additional_reservations:
                quarantine.release(reservation)
            released_owner = quarantine.snapshot()
            print(
                "CONSTRUCTOR_ROLLBACK_RESULT="
                + json.dumps(
                    {
                        "elapsed": elapsed,
                        "error_cause_is_primary": constructor_error.__cause__ is primary,
                        "error_reason": getattr(constructor_error, "cleanup_reason_code", None),
                        "error_retains_capacity": getattr(
                            constructor_error,
                            "cleanup_retains_capacity",
                            False,
                        ),
                        "fatal_before_drain": before_drain.fatal,
                        "active_before_drain": before_drain.active,
                        "queued_before_drain": before_drain.queued,
                        "blocking_before_drain": before_drain.blocking_in_flight,
                        "cleanup_before_drain": before_drain.cleanup_in_flight,
                        "service_before_drain": before_drain.service_owner_in_flight,
                        "retained_before_drain": before_drain.retained,
                        "active_workers": construction_work.active,
                        "post_fence_rejected": post_fence_rejected,
                        "root_state": lifecycle.runtime_root_state,
                        "active_after_drain": after_drain.active,
                        "queued_after_drain": after_drain.queued,
                        "blocking_after_drain": after_drain.blocking_in_flight,
                        "cleanup_after_drain": after_drain.cleanup_in_flight,
                        "service_after_drain": after_drain.service_owner_in_flight,
                        "retained_after_drain": after_drain.retained,
                        "owner_threads": owner.owner_threads,
                        "owner_capacity": owner.capacity,
                        "owner_slots": owner.slots,
                        "timed_out_slots": owner.timed_out_slots,
                        "saturated_slots": saturated_owner.slots,
                        "saturation_rejected": saturation_rejected[0],
                        "allocation_attempts": allocation_attempts[0],
                        "released_slots": released_owner.slots,
                        "named_owner_threads": sum(
                            thread.name == "tacit-llm-constructor-rollback-quarantine"
                            for thread in threading.enumerate()
                        ),
                        "sdk_close_calls": sdk.close_calls,
                        "sdk_cancellations": sdk.cancelled,
                        "transport_close_calls": transport.close_calls,
                    }
                )
            )


        asyncio.run(main())
        """)

    completed = subprocess.run(
        [sys.executable, "-c", source],
        cwd=Path(__file__).resolve().parents[2],
        check=True,
        capture_output=True,
        text=True,
        timeout=5.0,
    )
    result_line = next(
        line for line in completed.stdout.splitlines() if line.startswith("CONSTRUCTOR_ROLLBACK_RESULT=")
    )
    result = json.loads(result_line.partition("=")[2])

    assert result["elapsed"] < 0.5
    assert result["error_cause_is_primary"] is True
    assert result["error_reason"] == "llm_sdk_construction_rollback_timeout"
    assert result["error_retains_capacity"] is True
    assert result["fatal_before_drain"] is True
    assert result["active_before_drain"] == 0
    assert result["queued_before_drain"] == 0
    assert result["blocking_before_drain"] == 0
    assert result["cleanup_before_drain"] == 0
    assert result["service_before_drain"] == 0
    assert result["retained_before_drain"] == 0
    assert result["active_workers"] == 0
    assert result["post_fence_rejected"] is True
    assert result["root_state"] == "closed"
    assert result["active_after_drain"] == 0
    assert result["queued_after_drain"] == 0
    assert result["blocking_after_drain"] == 0
    assert result["cleanup_after_drain"] == 0
    assert result["service_after_drain"] == 0
    assert result["retained_after_drain"] == 0
    assert result["owner_threads"] == 1
    assert result["owner_capacity"] >= result["owner_slots"] == 1
    assert result["timed_out_slots"] == 1
    assert result["saturated_slots"] == result["owner_capacity"]
    assert result["saturation_rejected"] is True
    assert result["allocation_attempts"] == 0
    assert result["released_slots"] == 1
    assert result["named_owner_threads"] == 1
    assert result["sdk_close_calls"] == 1
    assert result["sdk_cancellations"] == 1
    assert result["transport_close_calls"] == 0


def test_construction_context_retains_rollback_authority_through_commit() -> None:
    class TransportProbe:
        def __init__(self) -> None:
            self.is_closed = False
            self.close_calls = 0

        async def aclose(self) -> None:
            self.close_calls += 1
            self.is_closed = True

    class SDKProbe:
        def __init__(self) -> None:
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1

    transport = TransportProbe()
    sdk = SDKProbe()
    primary = RuntimeError("failure after commit before context exit")

    with pytest.raises(RuntimeError, match="failure after commit") as exc_info:
        with http_transport.LLMSDKHTTPClientConstruction.begin() as construction:
            construction.create_http_client(lambda: cast(httpx.AsyncClient, transport))
            construction.create_sdk_client(lambda: sdk)
            construction.commit()
            raise primary

    assert exc_info.value is primary
    assert sdk.close_calls == 1
    assert transport.close_calls == 1
    assert transport.is_closed is True


@pytest.mark.parametrize("case", _PROVIDER_CASES, ids=lambda case: case.name)
def test_normal_provider_cleanup_closes_sdk_and_transport_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    case: _ProviderCase,
) -> None:
    class TransportProbe:
        def __init__(self) -> None:
            self.is_closed = False
            self.close_calls = 0

        async def aclose(self) -> None:
            self.close_calls += 1
            await asyncio.sleep(0)
            self.is_closed = True

    class SDKProbe:
        def __init__(self) -> None:
            self._custom_headers: dict[str, str] = {}
            self.api_key = _API_KEY
            self.admin_api_key = ""
            self.workload_identity = None
            self._api_key_provider = None
            self.organization = ""
            self.project = ""
            self.webhook_secret = ""
            self.auth_token = None
            self.credentials = None
            self.webhook_key = ""
            self._azure_ad_token = ""
            self._azure_ad_token_provider = None
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            await asyncio.sleep(0)

    transport = TransportProbe()
    sdk = SDKProbe()
    module = (
        "tacit.agents.providers.anthropic" if case.name == "anthropic" else "tacit.agents.providers.openai_provider"
    )
    sdk_constructor = {
        "anthropic": "anthropic.AsyncAnthropic",
        "openai": "openai.AsyncOpenAI",
        "azure": "openai.AsyncAzureOpenAI",
    }[case.name]

    monkeypatch.setattr(f"{module}.create_llm_sdk_http_client", lambda *_args, **_kwargs: transport)
    monkeypatch.setattr(f"{module}.{sdk_constructor}", lambda **_kwargs: sdk)
    provider = case.provider_type(_settings(case))

    async def close_repeatedly() -> None:
        await asyncio.gather(provider.close(), provider.close())
        await provider.close()

    asyncio.run(close_repeatedly())

    assert sdk.close_calls == 1
    assert transport.close_calls == 1
    assert transport.is_closed is True
