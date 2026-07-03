from __future__ import annotations

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
