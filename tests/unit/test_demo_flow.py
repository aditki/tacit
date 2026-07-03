from __future__ import annotations

import httpx
import pytest

from tacit import demo_flow


def test_load_demo_env_supplies_api_auth_header(tmp_path, monkeypatch):
    monkeypatch.delenv("API_AUTH_KEY", raising=False)
    (tmp_path / ".env").write_text("API_AUTH_KEY=demo-secret\n")

    demo_flow.load_demo_env(tmp_path)

    assert demo_flow._auth_headers() == {"X-API-Key": "demo-secret"}


def test_load_demo_env_does_not_override_process_env(tmp_path, monkeypatch):
    monkeypatch.setenv("API_AUTH_KEY", "shell-secret")
    (tmp_path / ".env").write_text("API_AUTH_KEY=demo-secret\n")

    demo_flow.load_demo_env(tmp_path)

    assert demo_flow._auth_headers() == {"X-API-Key": "shell-secret"}


def test_compose_up_wraps_missing_docker(tmp_path, monkeypatch):
    def missing_docker(*args, **kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(demo_flow.subprocess, "run", missing_docker)

    with pytest.raises(demo_flow.DemoError, match="Docker installed and on PATH"):
        demo_flow.compose_up(tmp_path, echo=lambda _msg: None)


def test_request_wraps_http_status_error_with_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, request=request, text="missing api key")

    transport = httpx.MockTransport(handler)
    with httpx.Client(base_url="http://demo.test", transport=transport) as client:
        with pytest.raises(demo_flow.DemoError, match="POST http://demo.test/api/v1/chart returned HTTP 401"):
            demo_flow._request(client, "POST", "/api/v1/chart", {"prompt": "checkout"})
