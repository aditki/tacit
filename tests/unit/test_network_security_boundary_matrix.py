from __future__ import annotations

import asyncio
import inspect
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from fastapi import FastAPI

import tacit.cli as cli_module
import tacit.integrations.slack as slack_module
import tacit.runtime_ownership as runtime_ownership_module
from tacit.agents.providers.bedrock import _bedrock_client_config
from tacit.agents.providers.registry import create_provider
from tacit.api.app import create_app
from tacit.config import (
    DEFAULT_API_ALLOWED_HOSTS,
    Settings,
    canonical_api_allowed_hosts,
    canonical_aws_region,
    validate_api_server_bind,
)
from tacit.errors import RuntimeOwnershipError
from tacit.runtime_ownership import (
    canonical_aws_sts_endpoint,
    canonical_bedrock_runtime_endpoint,
    runtime_descriptor_for_provider,
)
from tests.http_client import TestClient


def _settings(**values: Any) -> Settings:
    return Settings(**{"_env_file": None, **values})


@pytest.mark.asyncio
async def test_registry_ollama_provider_ignores_ambient_proxy_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:8080")
    provider = create_provider(
        _settings(
            llm_provider="ollama",
            llm_api_base="http://127.0.0.1:11434",
            llm_model="local-model",
        )
    )
    try:
        assert provider._client.trust_env is False
        assert provider._client.follow_redirects is False
    finally:
        await provider.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_name", ["rag_api", "a2a", "mcp"])
async def test_context_clients_ignore_ambient_proxy_credentials(
    monkeypatch: pytest.MonkeyPatch,
    provider_name: str,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:8080")
    runtime_settings = _settings(
        context_provider=provider_name,
        context_api_key="private-context-token",
        context_rag_api_url="https://rag.example.test",
        context_a2a_agent_url="https://a2a.example.test",
        context_mcp_server_url="https://mcp.example.test",
    )
    if provider_name == "rag_api":
        from tacit.context.rag_api_provider import RAGAPIProvider

        provider = RAGAPIProvider(runtime_settings)
    elif provider_name == "a2a":
        from tacit.context.a2a_provider import A2AProvider

        provider = A2AProvider(runtime_settings)
    else:
        from tacit.context.mcp_provider import MCPProvider

        provider = MCPProvider(runtime_settings)
    try:
        assert provider._client.trust_env is False
    finally:
        await provider.close()


@pytest.mark.asyncio
async def test_grafana_client_ignores_ambient_proxy_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    from tacit.grafana.client import GrafanaClient

    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:8080")
    client = GrafanaClient(base_url="https://grafana.example.test", api_key="private-grafana-token")
    try:
        assert client._client.trust_env is False
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("client_name", ["signalfx", "pagerduty"])
async def test_backend_clients_ignore_ambient_proxy_credentials(
    monkeypatch: pytest.MonkeyPatch,
    client_name: str,
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.invalid:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.invalid:8080")
    if client_name == "signalfx":
        from tacit.signalfx.client import SignalFxClient

        client = SignalFxClient(api_token="private-signalfx-token", realm="us0")
    else:
        from tacit.integrations.pagerduty import PagerDutyClient

        client = PagerDutyClient(
            api_token="private-pagerduty-token",
            base_url="https://api.pagerduty.example.test",
        )
    try:
        assert client._client.trust_env is False
    finally:
        await client.close()


def test_bedrock_client_config_disables_ambient_proxies() -> None:
    config = _bedrock_client_config(_settings(llm_provider="bedrock"))

    assert config.proxies == {}


def test_slack_clients_clear_sdk_loaded_ambient_proxies(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class FakeWebClient:
        def __init__(self, **kwargs: Any) -> None:
            captured["web_kwargs"] = kwargs
            self.proxy = "http://ambient-proxy.invalid:8080"

    class FakeSlackApp:
        def __init__(self, **kwargs: Any) -> None:
            captured["app_kwargs"] = kwargs
            self.client = kwargs["client"]

        def event(self, _name: str):
            return lambda handler: handler

        def command(self, _name: str):
            return lambda handler: handler

    class FakeSocketHandler:
        def __init__(self, app: Any, app_token: str) -> None:
            captured["socket_args"] = (app, app_token)
            self.client = type("SocketClient", (), {"proxy": "http://ambient-proxy.invalid:8080"})()

    monkeypatch.setattr(slack_module, "AsyncWebClient", FakeWebClient, raising=False)
    monkeypatch.setattr(slack_module, "AsyncApp", FakeSlackApp)
    monkeypatch.setattr(slack_module, "AsyncSocketModeHandler", FakeSocketHandler)
    runtime_settings = _settings(
        slack_bot_token="xoxb-private-token",
        slack_app_token="xapp-private-token",
        slack_signing_secret="private-signing-secret",
    )

    app = slack_module.create_slack_app(runtime_settings)
    handler = slack_module._new_slack_socket_mode_handler(app, runtime_settings.slack_app_token)

    assert captured["web_kwargs"] == {
        "token": "xoxb-private-token",
        "trust_env_in_session": False,
    }
    assert captured["app_kwargs"]["client"].proxy is None
    assert handler.client.proxy is None


def test_cli_credential_probe_pins_origin_transport_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {"closed": False}

    class FakeResponse:
        status_code = 200

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            observed["client_options"] = kwargs

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_args: Any) -> None:
            observed["closed"] = True

        def get(self, url: str, **kwargs: Any) -> FakeResponse:
            observed["request"] = (url, kwargs)
            return FakeResponse()

    monkeypatch.setattr("httpx.Client", FakeClient)

    response = cli_module._credentialed_remote_get(
        base_url="HTTPS://Grafana.Example.test:443/root/",
        path="/api/org",
        headers={"Authorization": "Bearer private-token"},
        timeout=10.0,
    )

    assert response.status_code == 200
    assert observed["client_options"] == {
        "timeout": 10.0,
        "trust_env": False,
        "follow_redirects": False,
    }
    assert observed["request"] == (
        "https://grafana.example.test/root/api/org",
        {"headers": {"Authorization": "Bearer private-token"}, "params": None},
    )
    assert observed["closed"] is True


def test_cli_signalfx_realm_is_validated_before_credential_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client_constructions = 0

    class ForbiddenClient:
        def __init__(self, **_kwargs: Any) -> None:
            nonlocal client_constructions
            client_constructions += 1
            raise AssertionError("transport must not be constructed")

    monkeypatch.setattr("httpx.Client", ForbiddenClient)

    with pytest.raises(RuntimeOwnershipError, match="SignalFx realm"):
        cli_module._signalfx_api_base("us1.signalfx.com@capture.invalid/")

    assert client_constructions == 0


def test_cli_credentialed_checks_do_not_use_ambient_httpx_convenience_calls() -> None:
    for handler in (
        cli_module._check_grafana,
        cli_module._check_datasources,
        cli_module._check_signalfx,
        cli_module.connect_grafana.callback,
        cli_module.connect_signalfx.callback,
    ):
        assert "httpx.get(" not in inspect.getsource(handler)


def test_ollama_doctor_probe_ignores_ambient_proxies_and_does_not_follow_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx

    observed: dict[str, Any] = {"requests": [], "closed": False}
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.setenv(name, "http://proxy.invalid:8080")

    class RedirectResponse:
        status_code = 302
        headers = {"location": "https://capture.invalid/api/tags"}

    class FakeClient:
        def __init__(self, **kwargs: Any) -> None:
            observed["client_options"] = kwargs

        def __enter__(self) -> FakeClient:
            return self

        def __exit__(self, *_args: Any) -> None:
            observed["closed"] = True

        def get(self, url: str, **kwargs: Any) -> RedirectResponse:
            observed["requests"].append((url, kwargs))
            return RedirectResponse()

    def forbidden_convenience_get(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("Ollama diagnostics must use the isolated one-shot client")

    monkeypatch.setattr(httpx, "Client", FakeClient)
    monkeypatch.setattr(httpx, "get", forbidden_convenience_get)

    assert (
        cli_module._check_llm(
            _settings(
                llm_provider="ollama",
                llm_api_base="HTTP://127.0.0.1:11434/",
                llm_model="qwen3",
            )
        )
        is False
    )
    assert observed["client_options"] == {
        "timeout": 5.0,
        "trust_env": False,
        "follow_redirects": False,
    }
    assert observed["requests"] == [
        (
            "http://127.0.0.1:11434/api/tags",
            {"headers": {}, "params": None},
        )
    ]
    assert observed["closed"] is True


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        ("us-east-1", "us-east-1"),
        (" US-GOV-WEST-1 ", "us-gov-west-1"),
        ("cn-north-1", "cn-north-1"),
        ("eusc-de-east-1", "eusc-de-east-1"),
    ],
)
def test_aws_region_is_canonical_at_direct_and_settings_boundaries(configured: str, expected: str) -> None:
    assert canonical_aws_region(configured) == expected
    assert _settings(llm_bedrock_region=configured).llm_bedrock_region == expected


@pytest.mark.parametrize(
    "configured",
    [
        "",
        "aws-global",
        "us--east-1",
        "us-east-0",
        "us_east_1",
        "us-east-1:443",
        "us-east-1.attacker.example",
        "us-east-1@attacker.example/x",
        "https://attacker.example/us-east-1",
        "evil-attacker-1",
    ],
)
def test_invalid_aws_regions_fail_before_endpoint_construction(configured: str) -> None:
    with pytest.raises(ValueError, match="AWS region"):
        canonical_aws_region(configured)
    with pytest.raises(ValueError, match="AWS region"):
        _settings(llm_bedrock_region=configured)
    with pytest.raises(RuntimeOwnershipError, match="AWS region"):
        canonical_bedrock_runtime_endpoint(configured)
    with pytest.raises(RuntimeOwnershipError, match="AWS region"):
        canonical_aws_sts_endpoint(configured)


def test_valid_aws_regions_cannot_escape_the_vendor_endpoint_suffix() -> None:
    assert canonical_bedrock_runtime_endpoint("US-EAST-1") == ("https://bedrock-runtime.us-east-1.amazonaws.com")
    assert canonical_aws_sts_endpoint("cn-north-1") == "https://sts.cn-north-1.amazonaws.com.cn"


def test_invalid_copied_region_fails_before_credential_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime_settings = _settings(llm_provider="bedrock").model_copy(
        update={"llm_bedrock_region": "us-east-1@attacker.example/x"}
    )
    credential_selections = 0

    def select_credentials(*args: Any, **kwargs: Any) -> tuple[str, str, str]:
        nonlocal credential_selections
        credential_selections += 1
        raise AssertionError("credential selection must not run")

    monkeypatch.setattr(runtime_ownership_module, "bedrock_credential_selector", select_credentials)

    with pytest.raises(RuntimeOwnershipError, match="AWS region"):
        runtime_descriptor_for_provider(component="test", runtime_settings=runtime_settings)

    assert credential_selections == 0


def test_default_allowed_hosts_are_explicit_loopback_and_test_transport_names() -> None:
    assert canonical_api_allowed_hosts(DEFAULT_API_ALLOWED_HOSTS).split(",") == [
        "localhost",
        "127.0.0.1",
        "[::1]",
        "testserver",
    ]
    assert _settings().api_allowed_hosts == DEFAULT_API_ALLOWED_HOSTS


@pytest.mark.parametrize(
    "configured",
    [
        "",
        "*",
        "http://tacit.example.com",
        "tacit.example.com:8000",
        "bad_host.example",
        "example.com/path",
        "example.com@attacker.example",
        "*.127.0.0.1",
    ],
)
def test_allowed_host_policy_rejects_ambiguous_or_unbounded_patterns(configured: str) -> None:
    with pytest.raises(ValueError, match="allowed hosts"):
        canonical_api_allowed_hosts(configured)
    with pytest.raises(ValueError, match="allowed hosts"):
        _settings(api_allowed_hosts=configured)


def test_app_revalidates_allowed_hosts_from_unvalidated_settings_copy() -> None:
    copied = _settings().model_copy(update={"api_allowed_hosts": "*"})

    with pytest.raises(ValueError, match="allowed hosts"):
        create_app(runtime_settings=copied, lifespan=None, include_default_routes=False)


def _probe_app(settings: Settings) -> FastAPI:
    app = create_app(runtime_settings=settings, lifespan=None, include_default_routes=False)

    @app.get("/probe")
    async def probe() -> dict[str, bool]:
        return {"ok": True}

    return app


def test_default_host_policy_accepts_testclient_and_rejects_dns_rebinding_host() -> None:
    client = TestClient(_probe_app(_settings()))

    assert client.get("/probe").json() == {"ok": True}
    rejected = client.get("/probe", headers={"Host": "attacker.example"})

    assert rejected.status_code == 400
    assert rejected.text == "Invalid host header"


@pytest.mark.parametrize(
    "host",
    [
        "tacit.example.com:0",
        "tacit.example.com:65536",
        "tacit.example.com@attacker.example",
        "[::1",
        "[::1]attacker",
    ],
)
def test_malformed_host_headers_are_rejected_before_route_execution(host: str) -> None:
    effects: list[str] = []
    app = create_app(
        runtime_settings=_settings(api_allowed_hosts="testserver,tacit.example.com,[::1]"),
        lifespan=None,
        include_default_routes=False,
    )

    @app.get("/probe")
    async def probe() -> dict[str, bool]:
        effects.append("executed")
        return {"ok": True}

    response = TestClient(app).get("/probe", headers={"Host": host})

    assert response.status_code == 400
    assert effects == []


@pytest.mark.parametrize(
    "host",
    [
        "Tacit.Example.com:8443",
        "worker.internal.example",
        "[2001:0DB8::1]:8000",
    ],
)
def test_configured_exact_wildcard_and_ipv6_hosts_are_accepted(host: str) -> None:
    app = _probe_app(_settings(api_allowed_hosts="tacit.example.com,*.internal.example,[2001:db8::1]"))

    response = TestClient(app).get("/probe", headers={"Host": host})

    assert response.status_code == 200


def test_wildcard_host_policy_does_not_match_the_bare_or_lookalike_domain() -> None:
    client = TestClient(_probe_app(_settings(api_allowed_hosts="testserver,*.internal.example")))

    assert client.get("/probe", headers={"Host": "internal.example"}).status_code == 400
    assert client.get("/probe", headers={"Host": "evilinternal.example"}).status_code == 400


def _invoke_serve(
    monkeypatch: pytest.MonkeyPatch,
    *,
    runtime_settings: Settings,
    arguments: list[str],
) -> tuple[Any, list[dict[str, Any]]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(cli_module, "_load_env", lambda: None)
    monkeypatch.setattr("tacit.config.create_settings", lambda: runtime_settings)
    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: calls.append({"args": args, **kwargs}))
    result = CliRunner().invoke(cli_module.cli, ["serve", "--no-slack", *arguments])
    return result, calls


def test_serve_defaults_to_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    result, calls = _invoke_serve(monkeypatch, runtime_settings=_settings(), arguments=[])

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert isinstance(calls[0]["args"][0], FastAPI)
    assert calls[0] == {
        "args": (calls[0]["args"][0],),
        "host": "127.0.0.1",
        "port": 8000,
        "reload": False,
        "log_level": "info",
    }


def test_serve_no_slack_builds_uvicorn_app_from_one_disabled_settings_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configured = _settings(
        slack_bot_token="xoxb-configured-secret",
        slack_app_token="xapp-configured-secret",
    )
    app_settings: list[Settings] = []
    calls: list[dict[str, Any]] = []
    slack_starts: list[Settings] = []
    real_create_app = create_app

    def observed_create_app(*, runtime_settings: Settings, **kwargs: Any) -> FastAPI:
        app_settings.append(runtime_settings)
        return real_create_app(runtime_settings=runtime_settings, **kwargs)

    async def forbidden_slack_start(runtime_settings: Settings, **_kwargs: Any) -> None:
        slack_starts.append(runtime_settings)

    monkeypatch.setattr(cli_module, "_load_env", lambda: None)
    monkeypatch.setattr("tacit.config.create_settings", lambda: configured)
    monkeypatch.setattr("tacit.api.app.create_app", observed_create_app)
    monkeypatch.setattr("tacit.integrations.slack.start_slack_bot", forbidden_slack_start)
    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: calls.append({"args": args, **kwargs}))

    result = CliRunner().invoke(cli_module.cli, ["serve", "--no-slack"])

    assert result.exit_code == 0, result.output
    assert len(app_settings) == 1
    resolved = app_settings[0]
    assert resolved.slack_bot_token == ""
    assert resolved.slack_app_token == ""
    assert len(calls) == 1
    server_app = calls[0]["args"][0]
    assert isinstance(server_app, FastAPI)
    assert server_app.state.settings is resolved

    async def exercise_lifespan() -> None:
        async with server_app.router.lifespan_context(server_app):
            await asyncio.sleep(0)

    asyncio.run(exercise_lifespan())
    assert slack_starts == []


def test_serve_reload_canonicalizes_no_slack_before_spawning_imported_app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    alias_names = {
        "SLACK_BOT_TOKEN": "xoxb-uppercase-secret",
        "slack_bot_token": "xoxb-lowercase-secret",
        "SlAcK_BoT_ToKeN": "xoxb-mixed-case-secret",
        "SLACK_APP_TOKEN": "xapp-uppercase-secret",
        "slack_app_token": "xapp-lowercase-secret",
        "SlAcK_ApP_ToKeN": "xapp-mixed-case-secret",
    }
    for name in tuple(os.environ):
        if name.casefold() in {"slack_bot_token", "slack_app_token"}:
            monkeypatch.delenv(name, raising=False)
    for name, value in alias_names.items():
        monkeypatch.setenv(name, value)

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(cli_module, "_load_env", lambda: None)
    monkeypatch.setattr("uvicorn.run", lambda *args, **kwargs: calls.append({"args": args, **kwargs}))

    result = CliRunner().invoke(cli_module.cli, ["serve", "--no-slack", "--reload"])

    assert result.exit_code == 0, result.output
    assert calls == [
        {
            "args": ("tacit.main:app",),
            "host": "127.0.0.1",
            "port": 8000,
            "reload": True,
            "log_level": "info",
        }
    ]
    assert os.environ["SLACK_BOT_TOKEN"] == ""
    assert os.environ["SLACK_APP_TOKEN"] == ""
    assert {name for name in os.environ if name.casefold() in {"slack_bot_token", "slack_app_token"}} == {
        "SLACK_BOT_TOKEN",
        "SLACK_APP_TOKEN",
    }

    repository_root = Path(__file__).resolve().parents[2]
    child_environment = dict(os.environ)
    child_environment["HOME"] = str(tmp_path)
    child_environment["PYTHONPATH"] = str(repository_root)
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json; "
                "from tacit.main import app; "
                "print(json.dumps({"
                "'bot': app.state.settings.slack_bot_token, "
                "'app': app.state.settings.slack_app_token"
                "}))"
            ),
        ],
        cwd=tmp_path,
        env=child_environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert child.returncode == 0, child.stderr
    assert json.loads(child.stdout.splitlines()[-1]) == {"bot": "", "app": ""}


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.0.2.10", "tacit.example.com"])
def test_serve_rejects_unauthenticated_non_loopback_bind_before_start(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
) -> None:
    result, calls = _invoke_serve(
        monkeypatch,
        runtime_settings=_settings(api_auth_enabled=False),
        arguments=["--host", host],
    )

    assert result.exit_code != 0
    assert "API authentication" in result.output
    assert calls == []


def test_serve_allows_authenticated_non_loopback_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    result, calls = _invoke_serve(
        monkeypatch,
        runtime_settings=_settings(
            api_auth_enabled=True,
            api_auth_key="secret",
            api_allowed_hosts="tacit.example.com",
        ),
        arguments=["--host", "0.0.0.0", "--port", "8443"],
    )

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["host"] == "0.0.0.0"
    assert calls[0]["port"] == 8443


@pytest.mark.parametrize(
    ("host", "allowed_hosts"),
    [
        ("0.0.0.0", "tacit.example.com"),
        ("::", "tacit.example.com"),
        ("192.0.2.10", "192.0.2.10"),
        ("tacit.example.com", "tacit.example.com"),
    ],
)
def test_serve_rejects_non_loopback_reload_before_imported_child_launch(
    monkeypatch: pytest.MonkeyPatch,
    host: str,
    allowed_hosts: str,
) -> None:
    result, calls = _invoke_serve(
        monkeypatch,
        runtime_settings=_settings(
            api_auth_enabled=True,
            api_auth_key="parent-authorized-secret",
            api_allowed_hosts=allowed_hosts,
        ),
        arguments=["--host", host, "--reload"],
    )

    assert result.exit_code != 0
    assert "Reload is limited to loopback" in result.output
    assert calls == []


def test_serve_rejects_authenticated_non_loopback_bind_without_explicit_host_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, calls = _invoke_serve(
        monkeypatch,
        runtime_settings=_settings(api_auth_enabled=True, api_auth_key="secret"),
        arguments=["--host", "0.0.0.0"],
    )

    assert result.exit_code != 0
    assert "explicit API allowed-host policy" in result.output
    assert calls == []


def test_non_loopback_bind_rejects_an_implicit_default_host_policy() -> None:
    runtime_settings = _settings(api_auth_enabled=True, api_auth_key="secret")

    with pytest.raises(ValueError, match="explicit API allowed-host policy"):
        validate_api_server_bind(runtime_settings, "0.0.0.0")


def test_non_loopback_bind_rejects_an_incompatible_explicit_host_policy() -> None:
    runtime_settings = _settings(
        api_auth_enabled=True,
        api_auth_key="secret",
        api_allowed_hosts="other.example.com",
    )

    with pytest.raises(ValueError, match="compatible API allowed host"):
        validate_api_server_bind(runtime_settings, "tacit.example.com")


@pytest.mark.parametrize(
    ("bind_host", "allowed_hosts"),
    [
        ("0.0.0.0", "localhost,127.0.0.1"),
        ("::", "localhost,[::1]"),
        ("tacit.example.com", "tacit.example.com"),
        ("worker.internal.example", "*.internal.example"),
    ],
)
def test_non_loopback_bind_accepts_authenticated_explicit_compatible_host_policy(
    bind_host: str,
    allowed_hosts: str,
) -> None:
    runtime_settings = _settings(
        api_auth_enabled=True,
        api_auth_key="secret",
        api_allowed_hosts=allowed_hosts,
    )

    assert validate_api_server_bind(runtime_settings, bind_host) == bind_host


def test_direct_module_server_defaults_to_the_shared_loopback_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    import tacit.main as main_module

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(main_module.uvicorn, "run", lambda *args, **kwargs: calls.append({"args": args, **kwargs}))

    main_module.main()

    assert calls == [
        {
            "args": ("tacit.main:app",),
            "host": "127.0.0.1",
            "port": 8000,
            "reload": False,
            "log_level": main_module.settings.log_level.lower(),
        }
    ]
