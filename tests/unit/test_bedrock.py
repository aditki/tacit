"""Tests for AWS Bedrock LLM provider.

Covers:
- _build_boto3_session: explicit keys, default chain, assume-role with RefreshableCredentials
- BedrockProvider._converse: API call structure, multi-block concat, empty response
- Model ID configuration: explicit IDs, static map, provider-prefixed passthrough, fail-closed unknowns
- Inference profile retry: classified throughput rejection → geography-preserving retry + caching
- Mistral system prompt folding
- _inference_profile_id: us/eu/APAC geo prefixes, no implicit global fallback
- Transient error retry: ThrottlingException, service-specific exceptions
- pyproject.toml: bedrock optional extra, boto3 minimum version
"""

import asyncio
import os
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tacit.config import Settings
from tacit.errors import RuntimeOwnershipError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Helper ─────────────────────────────────────────────────────────────────


def _make_bedrock_provider(mock_client, mock_settings_overrides=None):
    """Construct a Bedrock provider for request-shape unit tests."""

    settings_values: dict[str, object] = {
        "llm_provider": "bedrock",
        "llm_bedrock_region": "us-east-1",
        "llm_aws_access_key_id": "AKIATESTFIXTURE",
        "llm_aws_secret_access_key": "test-fixture-secret",
        "llm_bedrock_role_arn": "",
        "llm_bedrock_model_id": "",
        "llm_model": "claude-sonnet-4-20250514",
    }
    settings_values.update(mock_settings_overrides or {})
    runtime_settings = Settings.model_validate(settings_values)
    from tacit.agents.providers.bedrock import BedrockProvider

    provider = BedrockProvider(runtime_settings)
    return provider, runtime_settings


def _resolved_runtime_for_client(mock_client):
    from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime
    from tacit.runtime_ownership import BedrockCredentialIdentity, credential_fingerprint

    session = MagicMock()
    session.client.return_value = mock_client
    return _ResolvedBedrockRuntime(
        session=session,
        credential_identity=BedrockCredentialIdentity(
            account=f"access-key:{credential_fingerprint('AKIATESTFIXTURE')}",
            credential_fingerprint=credential_fingerprint("AKIATESTFIXTURE\0test-fixture-secret\0"),
            uses_sts=False,
        ),
    )


def _assert_frozen_discovery_session(
    mock_boto3: MagicMock,
    *,
    profile: str | None,
    region: str,
) -> None:
    discovery_kwargs = mock_boto3.Session.call_args_list[0].kwargs
    assert set(discovery_kwargs) == {"botocore_session"}
    core_session = discovery_kwargs["botocore_session"]
    assert core_session.get_config_variable("profile") == profile
    assert core_session.get_config_variable("region") == region
    credentials_path = Path(core_session.get_config_variable("credentials_file"))
    config_path = Path(core_session.get_config_variable("config_file"))
    assert credentials_path.name == "credentials"
    assert config_path.name == "config"
    assert credentials_path.parent == config_path.parent
    assert "env" not in {
        str(getattr(provider, "METHOD", "")) for provider in core_session.get_component("credential_provider").providers
    }


# ── _build_boto3_session tests ─────────────────────────────────────────────


def test_bedrock_session_explicit_keys():
    """Strategy 1: explicit access key + secret should be passed to Session."""
    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_boto3.Session.return_value = mock_session

    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-west-2",
        llm_aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        llm_aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        from tacit.agents.providers.bedrock import _build_boto3_session

        resolved = _build_boto3_session(runtime_settings)

        mock_boto3.Session.assert_called_once_with(
            region_name="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        assert resolved.session == mock_session
        assert "AKIAIOSFODNN7EXAMPLE" not in repr(resolved)
        assert "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" not in repr(resolved)

    print("[PASS] test_bedrock_session_explicit_keys")


def test_bedrock_session_default_chain(monkeypatch, tmp_path):
    """Strategy 3: default boto3 credentials are copied into a pinned session."""
    mock_boto3 = MagicMock()
    discovery_session = MagicMock()
    credentials = MagicMock(method="shared-credentials-file")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="AKIADEFAULT",
        secret_key="default-secret",
        token="default-token",
    )
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    mock_boto3.Session.side_effect = [discovery_session, pinned_session]

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
        "[default]\n" "aws_access_key_id = AKIADEFAULT\n" "aws_secret_access_key = default-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="eu-west-1",
    )

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        from tacit.agents.providers.bedrock import _build_boto3_session

        resolved = _build_boto3_session(runtime_settings)

        _assert_frozen_discovery_session(
            mock_boto3,
            profile="default",
            region="eu-west-1",
        )
        assert mock_boto3.Session.call_args_list[1].kwargs == {
            "region_name": "eu-west-1",
            "aws_access_key_id": "AKIADEFAULT",
            "aws_secret_access_key": "default-secret",
            "aws_session_token": "default-token",
        }
        credentials.get_frozen_credentials.assert_called_once_with()
        assert resolved.session == pinned_session

    print("[PASS] test_bedrock_session_default_chain")


@pytest.mark.parametrize(
    ("credential_method", "profile_name"),
    [
        ("shared-credentials-file", "owner-a"),
        ("shared-credentials-file", ""),
    ],
    ids=("profile", "default"),
)
def test_bedrock_freezes_ambient_credentials_before_client_creation(
    monkeypatch,
    tmp_path,
    credential_method,
    profile_name,
):
    """Ambient inputs cannot change between plan capture and an operation."""
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
    ):
        monkeypatch.delenv(name, raising=False)
    if profile_name:
        monkeypatch.setenv("AWS_PROFILE", profile_name)
    credentials_path = tmp_path / "credentials"
    config_path = tmp_path / "config"
    selected_profile = profile_name or "default"
    credentials_path.write_text(
        f"[{selected_profile}]\n" "aws_access_key_id = AKIAFROZEN\n" "aws_secret_access_key = frozen-secret-material\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_path))

    mock_boto3 = MagicMock()
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        from tacit.agents.providers.bedrock import BedrockProvider

        provider = BedrockProvider(runtime_settings)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAAMBIENTMUTATION")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ambient-mutated-secret")
        with pytest.raises(RuntimeOwnershipError, match="selector changed"):
            asyncio.run(provider.chat_text("system", "user"))

    mock_boto3.Session.assert_not_called()


@pytest.mark.parametrize("source", ["settings", "environment"])
def test_bedrock_rejects_partial_credential_pairs(monkeypatch, source):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    values = {
        "_env_file": None,
        "llm_provider": "bedrock",
        "llm_bedrock_region": "us-east-1",
    }
    if source == "settings":
        values["llm_aws_access_key_id"] = "AKIAPARTIAL"
        with pytest.raises(ValueError, match="both access key and secret key"):
            Settings(**values)
        return
    else:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAPARTIAL")
    runtime_settings = Settings(**values)

    from tacit.runtime_ownership import runtime_descriptor_for_provider

    with pytest.raises(RuntimeOwnershipError, match="both access key and secret key"):
        runtime_descriptor_for_provider(
            component="partial_bedrock_credentials",
            runtime_settings=runtime_settings,
            capability="llm",
        )


def test_bedrock_provider_pins_profile_and_ignores_ambient_endpoint(monkeypatch, tmp_path):
    """Provider ownership and clients must retain the accepted AWS profile."""
    mock_boto3 = MagicMock()
    discovery_session = MagicMock()
    credentials = MagicMock(method="shared-credentials-file")
    credentials.get_frozen_credentials.return_value = SimpleNamespace(
        access_key="AKIAPROFILEA",
        secret_key="profile-a-secret",
        token="profile-a-token",
    )
    discovery_session.get_credentials.return_value = credentials
    pinned_session = MagicMock()
    runtime_client = MagicMock()
    runtime_client.converse.return_value = {"output": {"message": {"content": [{"text": "ok"}]}}}
    pinned_session.client.return_value = runtime_client
    mock_boto3.Session.side_effect = [discovery_session, pinned_session]
    monkeypatch.setenv("AWS_PROFILE", "owner-a")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://attacker.invalid")
    monkeypatch.setenv("AWS_ENDPOINT_URL_BEDROCK_RUNTIME", "https://runtime-attacker.invalid")
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[owner-a]\n" "aws_access_key_id = AKIAPROFILEA\n" "aws_secret_access_key = profile-a-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        from tacit.agents.providers.bedrock import BedrockProvider

        provider = BedrockProvider(runtime_settings)
        pinned_session.client.assert_not_called()
        assert asyncio.run(provider.chat_text("system", "user")).text == "ok"

    _assert_frozen_discovery_session(
        mock_boto3,
        profile="owner-a",
        region="us-east-1",
    )
    assert mock_boto3.Session.call_args_list[1].kwargs == {
        "region_name": "us-east-1",
        "aws_access_key_id": "AKIAPROFILEA",
        "aws_secret_access_key": "profile-a-secret",
        "aws_session_token": "profile-a-token",
    }
    assert pinned_session.client.call_args.args == ("bedrock-runtime",)
    assert pinned_session.client.call_args.kwargs["endpoint_url"] == ("https://bedrock-runtime.us-east-1.amazonaws.com")
    assert pinned_session.client.call_args.kwargs["config"].connect_timeout == 10.0
    remotes = {remote.provider: remote for remote in provider.runtime_ownership.remotes}
    assert remotes["llm:bedrock"].account == "profile:owner-a"
    assert remotes["llm:bedrock"].endpoint == "https://bedrock-runtime.us-east-1.amazonaws.com"
    assert "llm:bedrock:control" not in remotes
    runtime_client.close.assert_called_once_with()


def test_bedrock_provider_uses_one_ambient_identity_snapshot(monkeypatch, tmp_path):
    session = MagicMock()
    session.client.return_value.converse.return_value = {"output": {"message": {"content": [{"text": "ok"}]}}}
    observed_environment: dict[str, str] = {}
    monkeypatch.setenv("AWS_PROFILE", "owner-a")
    monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    credentials_path = tmp_path / "credentials"
    credentials_path.write_text(
        "[owner-a]\n" "aws_access_key_id = AKIAOWNERA\n" "aws_secret_access_key = owner-a-secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials_path))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )

    def build_session(*, credential_plan, deadline=None):
        del deadline
        from tacit.agents.providers.bedrock import _ResolvedBedrockRuntime
        from tacit.runtime_ownership import BedrockCredentialIdentity, credential_fingerprint

        observed_environment.update(credential_plan.environment)
        monkeypatch.setenv("AWS_PROFILE", "owner-b")
        return _ResolvedBedrockRuntime(
            session=session,
            credential_identity=BedrockCredentialIdentity(
                account=f"access-key:{credential_fingerprint('AKIAOWNERA')}",
                credential_fingerprint=credential_fingerprint("AKIAOWNERA\0owner-a-secret\0"),
                uses_sts=False,
            ),
        )

    with patch(
        "tacit.agents.providers.bedrock._build_boto3_session",
        side_effect=build_session,
    ):
        from tacit.agents.providers.bedrock import BedrockProvider

        provider = BedrockProvider(runtime_settings)
        assert asyncio.run(provider.chat_text("system", "user")).text == "ok"

    remotes = {remote.provider: remote for remote in provider.runtime_ownership.remotes}
    assert observed_environment["AWS_PROFILE"] == "owner-a"
    assert remotes["llm:bedrock"].account == "profile:owner-a"


@pytest.mark.asyncio
async def test_custom_bedrock_factory_runs_inside_admitted_blocking_worker(
    monkeypatch,
) -> None:
    from tacit.agents.providers.bedrock import (
        BedrockCredentialPlan,
        BedrockProvider,
        _ResolvedBedrockRuntime,
    )
    from tacit.dependencies import _RuntimeProviderResources
    from tacit.pipeline_admission import PipelineAdmissionController
    from tacit.runtime_ownership import (
        BedrockCredentialIdentity,
        credential_fingerprint,
        declare_runtime_factory,
    )

    access_key = "AKIAFACTORYWORKER"
    secret_key = "factory-worker-secret"
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_aws_access_key_id=access_key,
        llm_aws_secret_access_key=secret_key,
    )
    credential_plan = BedrockCredentialPlan.capture(runtime_settings)
    lifecycle = PipelineAdmissionController(1, max_queued=0)
    loop_thread = threading.get_ident()
    factory_observations: list[tuple[int, int, int, int]] = []

    def provider_factory():
        entry_thread = threading.get_ident()
        entry_permits = lifecycle.blocking_in_flight
        provider = BedrockProvider(credential_plan=credential_plan)
        factory_observations.append(
            (
                entry_thread,
                threading.get_ident(),
                entry_permits,
                lifecycle.blocking_in_flight,
            )
        )
        return provider

    declared_factory = declare_runtime_factory(
        provider_factory,
        ownership=credential_plan.ownership(component="custom_bedrock_factory"),
        factory_kind="provider:llm",
    )
    session = MagicMock()
    monkeypatch.setattr(
        "tacit.agents.providers.bedrock._build_boto3_session",
        lambda **_kwargs: _ResolvedBedrockRuntime(
            session=session,
            credential_identity=BedrockCredentialIdentity(
                account=f"access-key:{credential_fingerprint(access_key)}",
                credential_fingerprint=credential_fingerprint("\0".join((access_key, secret_key, ""))),
                uses_sts=False,
            ),
        ),
    )
    resources = _RuntimeProviderResources(
        runtime_settings,
        lifecycle=lifecycle,
        llm_factory=declared_factory,
        cleanup_grace_seconds=0.1,
    )

    try:
        await resources.acquire()
        assert len(factory_observations) == 1
        entry_thread, exit_thread, entry_permits, exit_permits = factory_observations[0]
        assert (
            entry_thread != loop_thread,
            exit_thread == entry_thread,
            entry_permits,
            exit_permits,
        ) == (True, True, 1, 1)
    finally:
        await resources.close()


def test_bedrock_role_declares_sts_and_uses_canonical_endpoint(monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-west-2",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    monkeypatch.setenv("AWS_ENDPOINT_URL_STS", "https://attacker.invalid")

    from tacit.runtime_ownership import runtime_descriptor_for_provider

    descriptor = runtime_descriptor_for_provider(
        component="bedrock_role_test",
        runtime_settings=runtime_settings,
        capability="llm",
    )
    remotes = {remote.provider: remote for remote in descriptor.remotes}
    assert remotes["llm:bedrock"].account == "arn:aws:iam::123456789012:role/tacitruntime"
    assert remotes["llm:bedrock:sts"].endpoint == "https://sts.us-west-2.amazonaws.com"


def test_bedrock_ambient_web_identity_declares_sts(monkeypatch, tmp_path):
    token_path = tmp_path / "web-identity-token"
    token_path.write_text("test-token")
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/AmbientRuntime")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_path))
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-2",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )

    with patch(
        "tacit.agents.providers.bedrock._build_boto3_session",
        return_value=MagicMock(),
    ):
        from tacit.agents.providers.bedrock import BedrockProvider

        provider = BedrockProvider(runtime_settings)

    remotes = {remote.provider: remote for remote in provider.runtime_ownership.remotes}
    assert remotes["llm:bedrock:sts"].endpoint == "https://sts.us-east-2.amazonaws.com"
    assert remotes["llm:bedrock:sts"].account == "arn:aws:iam::123456789012:role/ambientruntime"


def test_bedrock_explicit_keys_ignore_unselected_ambient_role(monkeypatch):
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/Unselected")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/aws/token")
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_aws_access_key_id="AKIAEXPLICIT",
        llm_aws_secret_access_key="explicit-secret",
    )
    mock_boto3 = MagicMock()
    pinned_session = MagicMock()
    mock_boto3.Session.return_value = pinned_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        from tacit.agents.providers.bedrock import BedrockProvider

        provider = BedrockProvider(runtime_settings)

    remotes = {remote.provider: remote for remote in provider.runtime_ownership.remotes}
    assert remotes["llm:bedrock"].account.startswith("access-key:sha256:")
    assert "llm:bedrock:sts" not in remotes


def test_bedrock_ambient_web_identity_rejects_sts_endpoint_override(monkeypatch):
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/AmbientRuntime")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", "/var/run/secrets/aws/token")
    monkeypatch.setenv("AWS_ENDPOINT_URL_STS", "https://sts-attacker.invalid")
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
    )

    build_session = MagicMock()
    with (
        patch(
            "tacit.agents.providers.bedrock._build_boto3_session",
            build_session,
        ),
        pytest.raises(RuntimeOwnershipError, match="AWS STS endpoint overrides"),
    ):
        from tacit.agents.providers.bedrock import BedrockProvider

        BedrockProvider(runtime_settings)

    build_session.assert_not_called()


def test_bedrock_role_closes_runtime_and_sts_clients():
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TacitRuntime",
        llm_bedrock_model_id="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    base_session = MagicMock()
    role_session = MagicMock()
    runtime_client = MagicMock()
    runtime_client.converse.return_value = {"output": {"message": {"content": [{"text": "ok"}]}}}
    sts_client = MagicMock()
    sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAASSUMED",
            "SecretAccessKey": "assumed-secret",
            "SessionToken": "assumed-token",
        }
    }
    base_session.client.return_value = sts_client
    role_session.client.return_value = runtime_client
    mock_boto3 = MagicMock()
    mock_boto3.Session.side_effect = [base_session, role_session]

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        from tacit.agents.providers.bedrock import BedrockProvider

        provider = BedrockProvider(runtime_settings)
        assert asyncio.run(provider.chat_text("system", "user")).text == "ok"

    runtime_client.close.assert_called_once_with()
    sts_client.close.assert_called_once_with()


def test_bedrock_session_assume_role(monkeypatch):
    """Strategy 2: assume-role is frozen into one explicit session."""
    monkeypatch.setenv("AWS_ENDPOINT_URL_STS", "https://sts-attacker.invalid")
    mock_boto3 = MagicMock()
    base_session = MagicMock()
    assumed_session = MagicMock()

    mock_sts_client = MagicMock()
    mock_sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLE",
            "SecretAccessKey": "secretexample",
            "SessionToken": "tokenexample",
        }
    }
    base_session.client.return_value = mock_sts_client
    mock_boto3.Session.side_effect = [base_session, assumed_session]

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
        runtime_settings = Settings(
            _env_file=None,
            llm_bedrock_region="us-east-1",
            llm_aws_access_key_id="AKIABASE",
            llm_aws_secret_access_key="base-secret",
            llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TestRole",
        )
        from tacit.agents.providers.bedrock import _build_boto3_session

        resolved = _build_boto3_session(runtime_settings)

        assert base_session.client.call_args.args == ("sts",)
        assert base_session.client.call_args.kwargs["endpoint_url"] == "https://sts.us-east-1.amazonaws.com"
        assert base_session.client.call_args.kwargs["config"].read_timeout == 120.0
        mock_sts_client.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="tacit-bedrock",
            DurationSeconds=3600,
        )
        assert mock_boto3.Session.call_args_list[-1].kwargs == {
            "region_name": "us-east-1",
            "aws_access_key_id": "ASIAEXAMPLE",
            "aws_secret_access_key": "secretexample",
            "aws_session_token": "tokenexample",
        }
        assert resolved.session is assumed_session
        assert resolved.credential_clients == (mock_sts_client,)

    print("[PASS] test_bedrock_session_assume_role")


def test_bedrock_failed_assume_role_closes_credential_client() -> None:
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TestRole",
    )
    base_session = MagicMock()
    sts_client = MagicMock()
    sts_client.assume_role.side_effect = RuntimeError("sensitive STS failure")
    base_session.client.return_value = sts_client
    mock_boto3 = MagicMock()
    mock_boto3.Session.return_value = base_session

    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        pytest.raises(RuntimeOwnershipError) as exc_info,
    ):
        from tacit.agents.providers.bedrock import _build_boto3_session

        _build_boto3_session(runtime_settings)

    sts_client.close.assert_called_once_with()
    assert "sensitive STS failure" not in str(exc_info.value)


def test_bedrock_malformed_assume_role_response_closes_credential_client() -> None:
    runtime_settings = Settings(
        _env_file=None,
        llm_provider="bedrock",
        llm_bedrock_region="us-east-1",
        llm_aws_access_key_id="AKIABASE",
        llm_aws_secret_access_key="base-secret",
        llm_bedrock_role_arn="arn:aws:iam::123456789012:role/TestRole",
    )
    base_session = MagicMock()
    sts_client = MagicMock()
    sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAINCOMPLETE",
        }
    }
    base_session.client.return_value = sts_client
    mock_boto3 = MagicMock()
    mock_boto3.Session.return_value = base_session

    with (
        patch.dict("sys.modules", {"boto3": mock_boto3}),
        pytest.raises(RuntimeOwnershipError) as exc_info,
    ):
        from tacit.agents.providers.bedrock import _build_boto3_session

        _build_boto3_session(runtime_settings)

    sts_client.close.assert_called_once_with()
    assert "ASIAINCOMPLETE" not in str(exc_info.value)


def test_bedrock_session_no_boto3_raises():
    """Missing boto3 should raise a helpful ImportError."""
    from tacit.agents.providers.bedrock import _build_boto3_session

    original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def mock_import(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        try:
            _build_boto3_session(
                Settings(
                    _env_file=None,
                    llm_provider="bedrock",
                    llm_aws_access_key_id="AKIATESTFIXTURE",
                    llm_aws_secret_access_key="test-fixture-secret",
                )
            )
            assert False, "Should have raised ImportError"
        except ImportError as exc:
            assert "tacit-ai[bedrock]" in str(exc)

    print("[PASS] test_bedrock_session_no_boto3_raises")


# ── BedrockProvider._converse tests ────────────────────────────────────────


def test_bedrock_converse_call_structure():
    """_converse should call client.converse with correct Bedrock API shape."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": '{"result": "ok"}'}]}}}

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "anthropic.claude-sonnet-4-20250514-v1:0"},
    )

    result = provider._converse_with_client(mock_client, "system text", "user text", 0.2)

    mock_client.converse.assert_called_once()
    call_kwargs = mock_client.converse.call_args[1]
    assert call_kwargs["modelId"] == "anthropic.claude-sonnet-4-20250514-v1:0"
    assert call_kwargs["system"] == [{"text": "system text"}]
    assert call_kwargs["messages"] == [{"role": "user", "content": [{"text": "user text"}]}]
    assert call_kwargs["inferenceConfig"]["temperature"] == 0.2
    assert call_kwargs["inferenceConfig"]["maxTokens"] == 4096
    assert result.text == '{"result": "ok"}'

    print("[PASS] test_bedrock_converse_call_structure")


def test_bedrock_converse_multiple_content_blocks():
    """_converse should concatenate multiple text blocks."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {
            "message": {
                "content": [
                    {"text": "part1"},
                    {"text": "part2"},
                ]
            }
        }
    }

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "test-model"},
    )
    result = provider._converse_with_client(mock_client, "sys", "user", 0.5)
    assert result.text == "part1part2"

    print("[PASS] test_bedrock_converse_multiple_content_blocks")


def test_bedrock_converse_empty_response():
    """_converse should handle empty content blocks gracefully."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": []}}}

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "test-model"},
    )
    result = provider._converse_with_client(mock_client, "sys", "user", 0.5)
    assert result.text == ""

    print("[PASS] test_bedrock_converse_empty_response")


# ── Model ID resolution ───────────────────────────────────────────────────


def test_bedrock_unknown_model_requires_explicit_model_id():
    """Unknown model names must not silently route to a different model."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": "{}"}]}}}

    with pytest.raises(RuntimeOwnershipError, match="configure LLM_BEDROCK_MODEL_ID"):
        _make_bedrock_provider(
            mock_client,
            {"llm_model": "unknown-model-id"},
        )

    print("[PASS] test_bedrock_unknown_model_requires_explicit_model_id")


def test_bedrock_model_id_fallback_uses_bedrock_default():
    """When llm_model is the Anthropic API default, should resolve to a
    valid Bedrock model ID, not the bare Anthropic API name."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": "{}"}]}}}

    provider, _ = _make_bedrock_provider(mock_client)

    assert (
        provider._model_id != "claude-sonnet-4-20250514"
    ), f"Model ID should be a Bedrock ID, not Anthropic API name: {provider._model_id}"
    assert "anthropic." in provider._model_id, f"Expected Bedrock model ID format, got: {provider._model_id}"

    print("[PASS] test_bedrock_model_id_fallback_uses_bedrock_default")


def test_bedrock_provider_prefixed_model_id_preserved():
    """Provider-prefixed IDs should pass through production configuration unchanged."""
    mock_bedrock_client = MagicMock()

    for model_id in [
        "meta.llama3-70b-instruct-v1:0",
        "amazon.titan-text-express-v1",
        "cohere.command-r-plus-v1:0",
        "mistral.mixtral-8x7b-instruct-v0:1",
        "anthropic.claude-sonnet-4-20250514-v1:0",
    ]:
        provider, _ = _make_bedrock_provider(mock_bedrock_client, {"llm_model": model_id})
        assert provider._model_id == model_id

    print("[PASS] test_bedrock_provider_prefixed_model_id_preserved")


# ── Async methods ──────────────────────────────────────────────────────────


def test_bedrock_chat_json_appends_json_preamble():
    """chat_json should append JSON preamble to system prompt."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": '{"ok": true}'}]}}}

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "test-model"},
    )

    with patch(
        "tacit.agents.providers.bedrock._build_boto3_session",
        return_value=_resolved_runtime_for_client(mock_client),
    ):
        result = asyncio.run(provider.chat_json("system prompt", "user prompt", 0.2))

    assert result.text == '{"ok": true}'
    call_kwargs = mock_client.converse.call_args[1]
    system_text = call_kwargs["system"][0]["text"]
    assert "system prompt" in system_text
    assert "valid JSON" in system_text

    print("[PASS] test_bedrock_chat_json_appends_json_preamble")


def test_bedrock_chat_text_no_preamble():
    """chat_text should pass system prompt without JSON preamble."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": "plain response"}]}}}

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "test-model"},
    )

    with patch(
        "tacit.agents.providers.bedrock._build_boto3_session",
        return_value=_resolved_runtime_for_client(mock_client),
    ):
        result = asyncio.run(provider.chat_text("system only", "user msg", 0.3))

    assert result.text == "plain response"
    call_kwargs = mock_client.converse.call_args[1]
    system_text = call_kwargs["system"][0]["text"]
    assert system_text == "system only"
    assert "JSON" not in system_text

    print("[PASS] test_bedrock_chat_text_no_preamble")


# ── Converse inference-profile retry ───────────────────────────────────────


def test_converse_retries_with_declared_geography_profile_for_on_demand_error():
    """A documented throughput rejection may retry within the configured geography."""

    class ValidationException(Exception):
        def __init__(self) -> None:
            super().__init__(
                "Invocation of model ID with on-demand throughput isn't supported. "
                "Retry your request with the ID or ARN of an inference profile that contains this model."
            )
            self.response = {
                "Error": {
                    "Code": "ValidationException",
                    "Message": str(self),
                }
            }

    mock_client = MagicMock()
    mock_client.converse.side_effect = ValidationException()
    profile_client = MagicMock()
    profile_client.converse.return_value = {"output": {"message": {"content": [{"text": '{"ok": true}'}]}}}

    provider, _ = _make_bedrock_provider(mock_client)
    bare_id = provider._model_id
    assert not bare_id.startswith("us.")

    result = provider._converse_with_client(
        mock_client,
        "sys",
        "user",
        0.2,
        retry_client_factory=lambda: profile_client,
    )

    assert result.text == '{"ok": true}'
    assert mock_client.converse.call_count == 1
    assert profile_client.converse.call_count == 1
    assert provider._model_id.startswith("us.")
    assert provider._model_id == f"us.{bare_id}"

    print("[PASS] test_converse_retries_with_declared_geography_profile_for_on_demand_error")


def test_converse_apac_retry_never_widens_to_global():
    """APAC requests stay inside the APAC geographic inference profile."""

    class ValidationException(Exception):
        def __init__(self) -> None:
            super().__init__(
                "Invocation of model ID with on-demand throughput isn't supported. "
                "Retry your request with the ID or ARN of an inference profile that contains this model."
            )
            self.response = {"Error": {"Code": "ValidationException", "Message": str(self)}}

    mock_client = MagicMock()
    mock_client.converse.side_effect = ValidationException()
    profile_client = MagicMock()
    profile_client.converse.return_value = {"output": {"message": {"content": [{"text": "ok"}]}}}
    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_region": "ap-northeast-1"},
    )
    bare_id = provider._model_id

    result = provider._converse_with_client(
        mock_client,
        "sys",
        "user",
        0.2,
        retry_client_factory=lambda: profile_client,
    )

    assert result.text == "ok"
    assert profile_client.converse.call_args.kwargs["modelId"] == f"apac.{bare_id}"
    assert provider._model_id == f"apac.{bare_id}"
    assert not provider._model_id.startswith("global.")


def test_converse_unrelated_validation_error_does_not_retry():
    """Validation errors unrelated to on-demand throughput preserve the original failure."""

    class ValidationException(Exception):
        def __init__(self) -> None:
            super().__init__("The request contains an invalid inference configuration")
            self.response = {"Error": {"Code": "ValidationException", "Message": str(self)}}

    mock_client = MagicMock()
    original_error = ValidationException()
    mock_client.converse.side_effect = original_error
    retry_client_factory = MagicMock()
    provider, _ = _make_bedrock_provider(mock_client)

    with pytest.raises(ValidationException) as raised:
        provider._converse_with_client(
            mock_client,
            "sys",
            "user",
            0.2,
            retry_client_factory=retry_client_factory,
        )

    assert raised.value is original_error
    retry_client_factory.assert_not_called()


def test_converse_other_geography_does_not_invent_global_fallback():
    """A region without a declared geography profile cannot silently use global."""

    class ValidationException(Exception):
        def __init__(self) -> None:
            super().__init__(
                "Invocation of model ID with on-demand throughput isn't supported. "
                "Retry your request with the ID or ARN of an inference profile that contains this model."
            )
            self.response = {"Error": {"Code": "ValidationException", "Message": str(self)}}

    mock_client = MagicMock()
    original_error = ValidationException()
    mock_client.converse.side_effect = original_error
    retry_client_factory = MagicMock()
    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_region": "sa-east-1"},
    )

    with pytest.raises(ValidationException) as raised:
        provider._converse_with_client(
            mock_client,
            "sys",
            "user",
            0.2,
            retry_client_factory=retry_client_factory,
        )

    assert raised.value is original_error
    retry_client_factory.assert_not_called()


def test_converse_uses_explicit_global_profile_without_fallback():
    """Global routing is allowed only when the configured model ID declares it."""
    model_id = "global.anthropic.claude-sonnet-4-20250514-v1:0"
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": "ok"}]}}}
    retry_client_factory = MagicMock()
    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": model_id, "llm_bedrock_region": "ap-northeast-1"},
    )

    result = provider._converse_with_client(
        mock_client,
        "sys",
        "user",
        0.2,
        retry_client_factory=retry_client_factory,
    )

    assert result.text == "ok"
    assert mock_client.converse.call_args.kwargs["modelId"] == model_id
    retry_client_factory.assert_not_called()


def test_converse_no_retry_if_already_prefixed():
    """Already-prefixed model ID should not retry on ValidationException."""

    class ValidationException(Exception):
        pass

    mock_client = MagicMock()
    mock_client.converse.side_effect = ValidationException("some other validation error")

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "us.anthropic.claude-sonnet-4-20250514-v1:0"},
    )

    try:
        provider._converse_with_client(mock_client, "sys", "user", 0.2)
        assert False, "Should have raised ValidationException"
    except Exception as exc:
        assert type(exc).__name__ == "ValidationException"
        assert mock_client.converse.call_count == 1

    print("[PASS] test_converse_no_retry_if_already_prefixed")


def test_converse_no_retry_on_non_validation_error():
    """Non-ValidationException should not trigger profile retry."""

    class ThrottlingException(Exception):
        pass

    mock_client = MagicMock()
    mock_client.converse.side_effect = ThrottlingException("Rate exceeded")

    provider, _ = _make_bedrock_provider(mock_client)

    try:
        provider._converse_with_client(mock_client, "sys", "user", 0.2)
        assert False, "Should have raised ThrottlingException"
    except Exception as exc:
        assert type(exc).__name__ == "ThrottlingException"
        assert mock_client.converse.call_count == 1

    print("[PASS] test_converse_no_retry_on_non_validation_error")


def test_converse_cached_profile_id_skips_retry():
    """After successful retry, subsequent calls go direct (no retry)."""

    class ValidationException(Exception):
        def __init__(self) -> None:
            super().__init__(
                "Invocation of model ID with on-demand throughput isn't supported. "
                "Retry your request with the ID or ARN of an inference profile that contains this model."
            )
            self.response = {"Error": {"Code": "ValidationException", "Message": str(self)}}

    mock_client = MagicMock()
    mock_client.converse.side_effect = ValidationException()
    profile_client = MagicMock()
    profile_client.converse.side_effect = [
        {"output": {"message": {"content": [{"text": "first"}]}}},
        {"output": {"message": {"content": [{"text": "second"}]}}},
    ]

    provider, _ = _make_bedrock_provider(mock_client)

    result1 = provider._converse_with_client(
        mock_client,
        "sys",
        "user",
        0.2,
        retry_client_factory=lambda: profile_client,
    )
    assert result1.text == "first"
    assert mock_client.converse.call_count == 1
    assert profile_client.converse.call_count == 1

    retry_factory = MagicMock()
    result2 = provider._converse_with_client(
        profile_client,
        "sys",
        "user",
        0.2,
        retry_client_factory=retry_factory,
    )
    assert result2.text == "second"
    assert profile_client.converse.call_count == 2
    retry_factory.assert_not_called()

    print("[PASS] test_converse_cached_profile_id_skips_retry")


# ── Mistral system prompt folding ──────────────────────────────────────────


def test_mistral_model_folds_system_into_user_message():
    """Mistral models: system prompt folded into user message, no system field."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": '{"v": 1}'}]}}}

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "mistral.mixtral-8x7b-instruct-v0:1"},
    )

    result = provider._converse_with_client(mock_client, "system instructions", "user question", 0.3)

    assert result.text == '{"v": 1}'
    call_kwargs = mock_client.converse.call_args[1]
    assert "system" not in call_kwargs
    user_text = call_kwargs["messages"][0]["content"][0]["text"]
    assert "system instructions" in user_text
    assert "user question" in user_text

    print("[PASS] test_mistral_model_folds_system_into_user_message")


def test_non_mistral_model_uses_system_field():
    """Non-Mistral models should use the standard system field."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": [{"text": "ok"}]}}}

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "anthropic.claude-sonnet-4-20250514-v1:0"},
    )

    provider._converse_with_client(mock_client, "system text", "user text", 0.2)

    call_kwargs = mock_client.converse.call_args[1]
    assert "system" in call_kwargs
    assert call_kwargs["system"] == [{"text": "system text"}]
    assert call_kwargs["messages"][0]["content"][0]["text"] == "user text"

    print("[PASS] test_non_mistral_model_uses_system_field")


# ── _inference_profile_id unit tests ───────────────────────────────────────


def test_inference_profile_id_us_region():
    from tacit.agents.providers.bedrock import _inference_profile_id

    result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", "us-east-1")
    assert result == "us.anthropic.claude-sonnet-4-20250514-v1:0"
    print("[PASS] test_inference_profile_id_us_region")


def test_inference_profile_id_eu_region():
    from tacit.agents.providers.bedrock import _inference_profile_id

    result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", "eu-west-1")
    assert result == "eu.anthropic.claude-sonnet-4-20250514-v1:0"
    print("[PASS] test_inference_profile_id_eu_region")


def test_inference_profile_id_apac_stays_in_geography():
    """APAC regions use AWS's APAC geography profile, never global."""
    from tacit.agents.providers.bedrock import _inference_profile_id

    result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", "ap-northeast-1")
    assert result == "apac.anthropic.claude-sonnet-4-20250514-v1:0"
    print("[PASS] test_inference_profile_id_apac_stays_in_geography")


def test_inference_profile_id_other_regions_do_not_invent_global_profile():
    """Regions without a declared geography profile do not fall back globally."""
    from tacit.agents.providers.bedrock import _inference_profile_id

    for region in ["sa-east-1", "me-south-1", "ca-central-1", "af-south-1"]:
        result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", region)
        assert result is None, f"Region {region}: expected no implicit profile, got {result!r}"
    print("[PASS] test_inference_profile_id_other_regions_do_not_invent_global_profile")


# ── Transient error retry ─────────────────────────────────────────────────


def test_bedrock_converse_wraps_throttling_for_retry():
    """Bedrock ThrottlingException → LLMTransientError so tenacity retries."""
    from pydantic import BaseModel

    from tacit.agents.llm import LLMTransientError, call_llm

    class SimpleModel(BaseModel):
        value: int

    class ClientError(Exception):
        def __init__(self, msg, response):
            super().__init__(msg)
            self.response = response

    throttle_exc = ClientError(
        "An error occurred (ThrottlingException)",
        {"Error": {"Code": "ThrottlingException"}},
    )

    from tacit.agents.providers.base import LLMResult

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(side_effect=[throttle_exc, LLMResult(text='{"value": 99}')])

    with patch("tacit.agents.llm.get_provider", return_value=mock_provider):
        try:
            model, usage = asyncio.run(call_llm("sys", "user", SimpleModel))
            assert model.value == 99
            assert mock_provider.chat_json.call_count == 2
        except Exception as exc:
            assert isinstance(exc, LLMTransientError), f"Expected LLMTransientError, got {type(exc).__name__}: {exc}"

    print("[PASS] test_bedrock_converse_wraps_throttling_for_retry")


def test_bedrock_service_specific_exception_retried():
    """Bedrock service-specific ThrottlingException (not ClientError) → retried."""
    from pydantic import BaseModel
    from tenacity import wait_none

    from tacit.agents.llm import call_llm

    class Simple(BaseModel):
        v: int

    class ThrottlingException(Exception):
        def __init__(self, msg):
            super().__init__(msg)
            self.response = {"Error": {"Code": "ThrottlingException"}}

    from tacit.agents.providers.base import LLMResult

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(side_effect=[ThrottlingException("Rate exceeded"), LLMResult(text='{"v": 42}')])

    original_wait = call_llm.retry.wait
    call_llm.retry.wait = wait_none()

    try:
        with patch("tacit.agents.llm.get_provider", return_value=mock_provider):
            model, usage = asyncio.run(call_llm("sys", "user", Simple))
            assert model.v == 42
            assert mock_provider.chat_json.call_count == 2
    finally:
        call_llm.retry.wait = original_wait

    print("[PASS] test_bedrock_service_specific_exception_retried")


# ── pyproject.toml ─────────────────────────────────────────────────────────


def test_pyproject_has_bedrock_optional_extra():
    """pyproject.toml must define bedrock extra with boto3."""
    import tomllib
    from pathlib import Path

    toml_path = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    opt_deps = data.get("project", {}).get("optional-dependencies", {})
    assert "bedrock" in opt_deps
    assert any("boto3" in dep for dep in opt_deps["bedrock"])
    print("[PASS] test_pyproject_has_bedrock_optional_extra")


def test_pyproject_pins_the_bridge_to_its_tested_boto3_build():
    """The temporary bridge must not advertise untested Botocore internals."""
    import tomllib
    from pathlib import Path

    toml_path = Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    bedrock_deps = data["project"]["optional-dependencies"]["bedrock"]
    boto3_dep = next(d for d in bedrock_deps if "boto3" in d)
    assert boto3_dep == "boto3==1.43.16"

    print("[PASS] test_pyproject_pins_the_bridge_to_its_tested_boto3_build")


# ── Runner ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"[FAIL] {test_fn.__name__}: {e}")
            import traceback

            traceback.print_exc()
            failed += 1

    print(f"\n=== {passed} passed, {failed} failed out of {passed + failed} ===")
    if failed:
        sys.exit(1)
    print("=== All Bedrock tests passed ===")
