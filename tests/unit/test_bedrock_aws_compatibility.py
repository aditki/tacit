"""AWS credential and lifecycle compatibility matrix for the Bedrock bridge."""

from __future__ import annotations

import time
from importlib.metadata import version as distribution_version
from pathlib import Path
from unittest.mock import MagicMock, patch

import botocore.session
import pytest
from botocore.credentials import CredentialResolver
from pydantic import ValidationError

from tacit.agents.providers.bedrock import BedrockProvider, _bedrock_client_config, _build_boto3_session
from tacit.config import Settings
from tacit.runtime_ownership import BedrockCredentialPlan


def test_bedrock_sdk_generation_matches_credential_compatibility_contract() -> None:
    assert distribution_version("boto3") == "1.43.16"
    assert distribution_version("botocore") == "1.43.16"

    core_session = botocore.session.Session()
    assert callable(core_session.get_component)
    resolver = CredentialResolver([])
    assert isinstance(resolver.providers, list)

    config = _bedrock_client_config(Settings(llm_provider="bedrock", llm_bedrock_region="us-east-1"))
    assert config.connect_timeout > 0
    assert config.read_timeout > 0


def _role_profile_environment(tmp_path: Path, *, credentials: str, config: str) -> dict[str, str]:
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    credentials_path.write_text(credentials, encoding="utf-8")
    config_path.write_text(config, encoding="utf-8")
    return {
        "HOME": str(tmp_path),
        "AWS_PROFILE": "runtime",
        "AWS_SHARED_CREDENTIALS_FILE": str(credentials_path),
        "AWS_CONFIG_FILE": str(config_path),
    }


def _role_profile_settings() -> Settings:
    return Settings.model_validate(
        {
            "llm_provider": "bedrock",
            "llm_bedrock_region": "us-east-1",
        }
    )


def test_role_source_credentials_use_one_provider_and_botocore_token_precedence(
    tmp_path: Path,
) -> None:
    environment = _role_profile_environment(
        tmp_path,
        credentials=(
            "[source]\n"
            "aws_access_key_id = AKIASHARED\n"
            "aws_secret_access_key = shared-secret\n"
            "aws_security_token = legacy-shared-token\n"
            "aws_session_token = modern-shared-token\n"
        ),
        config=(
            "[profile runtime]\n"
            "role_arn = arn:aws:iam::123456789012:role/Runtime\n"
            "source_profile = source\n"
            "[profile source]\n"
            "aws_access_key_id = AKIACONFIG\n"
            "aws_secret_access_key = config-secret\n"
            "aws_session_token = config-token\n"
        ),
    )

    plan = BedrockCredentialPlan.capture(_role_profile_settings(), environment=environment)

    assert plan.role_source_credentials() == (
        "AKIASHARED",
        "shared-secret",
        "legacy-shared-token",
    )


def test_role_source_credentials_do_not_merge_config_token_into_shared_provider(
    tmp_path: Path,
) -> None:
    environment = _role_profile_environment(
        tmp_path,
        credentials=("[source]\n" "aws_access_key_id = AKIASHARED\n" "aws_secret_access_key = shared-secret\n"),
        config=(
            "[profile runtime]\n"
            "role_arn = arn:aws:iam::123456789012:role/Runtime\n"
            "source_profile = source\n"
            "[profile source]\n"
            "aws_access_key_id = AKIACONFIG\n"
            "aws_secret_access_key = config-secret\n"
            "aws_session_token = config-only-token\n"
        ),
    )

    plan = BedrockCredentialPlan.capture(_role_profile_settings(), environment=environment)

    assert plan.role_source_credentials() == ("AKIASHARED", "shared-secret", "")


def test_explicit_session_token_is_secret_and_pinned_in_plan_identity() -> None:
    token_a = "explicit-session-token-a"
    token_b = "explicit-session-token-b"
    common: dict[str, object] = {
        "llm_provider": "bedrock",
        "llm_aws_access_key_id": "AKIAEXPLICIT",
        "llm_aws_secret_access_key": "explicit-secret",
    }
    settings_a = Settings.model_validate(common | {"llm_aws_session_token": token_a})
    settings_b = Settings.model_validate(common | {"llm_aws_session_token": token_b})

    plan_a = BedrockCredentialPlan.capture(settings_a, environment={})
    plan_b = BedrockCredentialPlan.capture(settings_b, environment={})
    remote_a = plan_a.ownership(component="test").remotes[0]
    remote_b = plan_b.ownership(component="test").remotes[0]

    assert remote_a.credential_fingerprint != remote_b.credential_fingerprint
    assert token_a not in repr(settings_a)
    assert token_a not in repr(plan_a)
    assert token_a not in repr(remote_a)


@pytest.mark.parametrize(
    "values",
    [
        {"llm_aws_session_token": "orphan-token"},
        {
            "llm_aws_access_key_id": "AKIAINCOMPLETE",
            "llm_aws_session_token": "orphan-token",
        },
        {
            "llm_aws_secret_access_key": "incomplete-secret",
            "llm_aws_session_token": "orphan-token",
        },
    ],
)
def test_explicit_session_token_requires_complete_key_pair(values: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="access key and secret key"):
        Settings.model_validate({"llm_provider": "bedrock"} | values)


def test_boto3_session_receives_explicit_session_token() -> None:
    runtime_settings = Settings.model_validate(
        {
            "llm_provider": "bedrock",
            "llm_bedrock_region": "us-west-2",
            "llm_aws_access_key_id": "AKIAEXPLICIT",
            "llm_aws_secret_access_key": "explicit-secret",
            "llm_aws_session_token": "explicit-session-token",
        }
    )
    mock_boto3 = MagicMock()

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        _build_boto3_session(runtime_settings)

    mock_boto3.Session.assert_called_once_with(
        region_name="us-west-2",
        aws_access_key_id="AKIAEXPLICIT",
        aws_secret_access_key="explicit-secret",
        aws_session_token="explicit-session-token",
    )


def test_cli_keeps_session_token_out_of_yaml_and_hides_interactive_input() -> None:
    from tacit import cli as cli_module

    token = "explicit-session-token"
    yaml_config, secrets = cli_module._split_config(
        {
            "llm_provider": "bedrock",
            "llm_aws_access_key_id": "AKIAEXPLICIT",
            "llm_aws_secret_access_key": "explicit-secret",
            "llm_aws_session_token": token,
        }
    )
    assert "llm_aws_session_token" not in yaml_config
    assert secrets["LLM_AWS_SESSION_TOKEN"] == token

    prompt = MagicMock()
    prompt.ask.return_value = token
    with patch.object(cli_module, "Prompt", prompt):
        assert cli_module._prompt_secret("AWS Session Token") == token
    prompt.ask.assert_called_once_with("  AWS Session Token", password=True)


@pytest.mark.asyncio
async def test_pipeline_adoption_clears_direct_admission_ownership() -> None:
    runtime_settings = Settings.model_validate(
        {
            "llm_provider": "bedrock",
            "llm_bedrock_model_id": "anthropic.claude-sonnet-4-20250514-v1:0",
            "llm_aws_access_key_id": "AKIAEXPLICIT",
            "llm_aws_secret_access_key": "explicit-secret",
            "pipeline_max_concurrent": 1,
            "pipeline_max_queued": 0,
        }
    )
    provider = BedrockProvider(runtime_settings)
    lifecycle, manages_slot = provider._execution_lifecycle()
    assert manages_slot is True

    assert provider.bind_pipeline_lifecycle(lifecycle) is False
    async with lifecycle.slot(timeout_seconds=0.5):
        result = await provider._run_owned(
            lambda: "ok",
            reason_code="bedrock_direct_to_pipeline_adoption_test",
            deadline=time.monotonic() + 1,
        )

    assert result == "ok"
