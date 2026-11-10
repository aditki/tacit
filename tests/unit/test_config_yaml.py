from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from tacit.config import Settings, _load_yaml_config

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_shipped_yaml_example_uses_canonical_bedrock_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    example = REPOSITORY_ROOT / "tacit.yaml.example"
    monkeypatch.setenv("TACIT_CONFIG", str(example))

    values = _load_yaml_config()
    parsed = Settings(_env_file=None)

    assert values["llm_bedrock_region"] == "us-east-1"
    assert values["llm_bedrock_model_id"] == ""
    assert values["llm_bedrock_role_arn"] == ""
    assert parsed.llm_bedrock_region == "us-east-1"
    assert parsed.llm_bedrock_model_id == ""
    assert parsed.llm_bedrock_role_arn == ""
    assert values["api_allowed_hosts"] == "localhost,127.0.0.1,[::1]"
    assert parsed.api_allowed_hosts == "localhost,127.0.0.1,[::1]"
    assert values["sqlite_snapshot_max_bytes"] == 1_073_741_824
    assert parsed.sqlite_snapshot_max_bytes == 1_073_741_824
    assert "api_allowed_hosts" in parsed.model_fields_set


@pytest.mark.parametrize("invalid_value", [0, -1])
def test_settings_reject_non_positive_sqlite_snapshot_capacity(invalid_value: int) -> None:
    with pytest.raises(ValueError, match="sqlite_snapshot_max_bytes"):
        Settings(_env_file=None, sqlite_snapshot_max_bytes=invalid_value)


def test_settings_validation_errors_hide_secret_and_nested_input_values() -> None:
    secrets = {
        "llm_api_key": "llm-secret-exact-value",
        "llm_aws_access_key_id": "aws-access-exact-value",
        "llm_aws_secret_access_key": "aws-secret-exact-value",
        "llm_aws_session_token": "aws-session-exact-value",
        "grafana_api_key": "grafana-secret-exact-value",
        "signalfx_api_token": "signalfx-secret-exact-value",
        "pagerduty_api_token": "pagerduty-secret-exact-value",
        "slack_bot_token": "slack-bot-secret-exact-value",
        "slack_app_token": "slack-app-secret-exact-value",
        "slack_signing_secret": "slack-signing-secret-exact-value",
        "context_api_key": "context-secret-exact-value",
        "api_auth_key": "api-secret-exact-value",
        "tenant_key": "nested-tenant-exact-value",
    }

    with pytest.raises(ValidationError) as error:
        Settings(
            _env_file=None,
            api_auth_enabled=True,
            knowledge_tenant_id="*",
            knowledge_tenant_api_keys={
                "tenant-a": secrets["tenant_key"],
                "tenant-b": secrets["tenant_key"],
            },
            llm_api_key=secrets["llm_api_key"],
            llm_aws_access_key_id=secrets["llm_aws_access_key_id"],
            llm_aws_secret_access_key=secrets["llm_aws_secret_access_key"],
            llm_aws_session_token=secrets["llm_aws_session_token"],
            grafana_api_key=secrets["grafana_api_key"],
            signalfx_api_token=secrets["signalfx_api_token"],
            pagerduty_api_token=secrets["pagerduty_api_token"],
            slack_bot_token=secrets["slack_bot_token"],
            slack_app_token=secrets["slack_app_token"],
            slack_signing_secret=secrets["slack_signing_secret"],
            context_api_key=secrets["context_api_key"],
            api_auth_key=secrets["api_auth_key"],
        )

    rendered = f"{error.value!s}\n{error.value!r}"
    assert "input_value" not in rendered
    for secret in secrets.values():
        assert secret not in rendered


@pytest.mark.parametrize("unknown_key", ["model_id", "role_arn"])
def test_yaml_rejects_noncanonical_bedrock_keys(
    unknown_key: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "tacit.yaml"
    config_path.write_text(f"llm:\n  {unknown_key}: unsafe-default\n", encoding="utf-8")
    monkeypatch.setenv("TACIT_CONFIG", str(config_path))

    with pytest.raises(ValueError, match=rf"llm\.{unknown_key}"):
        Settings(_env_file=None)
