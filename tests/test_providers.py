"""Tests for LLM providers: Bedrock, Azure, registry, CloudWatch schema/rendering, CLI doctor.

Covers:
- BedrockProvider: auth strategies, Converse API, transient error retry, model ID fallback
- AzureOpenAIProvider: init validation, deployment resolution
- Registry: provider routing, unknown provider error
- PanelQuery: CloudWatch fields (namespace, stat, dimensions, region)
- Dashboard rendering: CW target JSON with region
- CLI _check_llm: Bedrock assume-role mirroring
- pyproject.toml: bedrock optional extra
"""
import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Bedrock _build_boto3_session tests ─────────────────────────────────────

def test_bedrock_session_explicit_keys():
    """Strategy 1: explicit access key + secret should be passed to Session."""
    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-west-2"
        mock_settings.llm_aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
        mock_settings.llm_aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        mock_settings.llm_bedrock_role_arn = ""

        from dashforge.agents.providers.bedrock import _build_boto3_session
        session = _build_boto3_session()

        mock_boto3.Session.assert_called_once_with(
            region_name="us-west-2",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        assert session == mock_session

    print("[PASS] test_bedrock_session_explicit_keys")


def test_bedrock_session_default_chain():
    """Strategy 3: no explicit keys → default boto3 chain."""
    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "eu-west-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""

        from dashforge.agents.providers.bedrock import _build_boto3_session
        session = _build_boto3_session()

        mock_boto3.Session.assert_called_once_with(region_name="eu-west-1")
        assert session == mock_session

    print("[PASS] test_bedrock_session_default_chain")


def test_bedrock_session_assume_role():
    """Strategy 2: assume-role uses RefreshableCredentials via botocore."""
    mock_boto3 = MagicMock()
    base_session = MagicMock()
    refreshed_session = MagicMock()

    mock_sts_client = MagicMock()
    from datetime import datetime, timezone, timedelta
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    mock_sts_client.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLE",
            "SecretAccessKey": "secretexample",
            "SessionToken": "tokenexample",
            "Expiration": future,
        }
    }
    base_session.client.return_value = mock_sts_client

    mock_boto3.Session.side_effect = [base_session, refreshed_session]

    mock_botocore_session = MagicMock()
    mock_refreshable = MagicMock()
    mock_botocore_creds_mod = MagicMock()
    mock_botocore_creds_mod.RefreshableCredentials.create_from_metadata.return_value = mock_refreshable
    mock_botocore_sess_mod = MagicMock()
    mock_botocore_sess_mod.get_session.return_value = mock_botocore_session

    with patch.dict("sys.modules", {
             "boto3": mock_boto3,
             "botocore": MagicMock(),
             "botocore.credentials": mock_botocore_creds_mod,
             "botocore.session": mock_botocore_sess_mod,
         }), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = "arn:aws:iam::123456789012:role/TestRole"

        from dashforge.agents.providers.bedrock import _build_boto3_session
        session = _build_boto3_session()

        # STS assume_role should have been called
        mock_sts_client.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="dashforge-bedrock",
            DurationSeconds=3600,
        )
        # RefreshableCredentials should have been created with a refresh callback
        rc_cls = mock_botocore_creds_mod.RefreshableCredentials
        rc_cls.create_from_metadata.assert_called_once()
        call_kwargs = rc_cls.create_from_metadata.call_args[1]
        assert call_kwargs["method"] == "sts-assume-role"
        assert callable(call_kwargs["refresh_using"])
        # Final session should be built via botocore_session (not static creds)
        last_session_kwargs = mock_boto3.Session.call_args_list[-1][1]
        assert "botocore_session" in last_session_kwargs, (
            "Session should be built from a botocore_session with refreshable creds"
        )

    print("[PASS] test_bedrock_session_assume_role")


def test_bedrock_session_no_boto3_raises():
    """Missing boto3 should raise a helpful ImportError."""
    from dashforge.agents.providers.bedrock import _build_boto3_session

    # Patch the import inside _build_boto3_session to simulate missing boto3
    original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__

    def mock_import(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return original_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=mock_import):
        try:
            _build_boto3_session()
            assert False, "Should have raised ImportError"
        except ImportError as exc:
            assert "boto3" in str(exc)

    print("[PASS] test_bedrock_session_no_boto3_raises")


# ── BedrockProvider._converse tests ────────────────────────────────────────

def test_bedrock_converse_call_structure():
    """_converse should call client.converse with correct Bedrock API shape."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {
            "message": {
                "content": [{"text": '{"result": "ok"}'}]
            }
        }
    }

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = "anthropic.claude-sonnet-4-20250514-v1:0"
        mock_settings.llm_model = "claude-sonnet-4-20250514"

        from dashforge.agents.providers.bedrock import BedrockProvider
        provider = BedrockProvider()

        result = provider._converse("system text", "user text", 0.2)

        mock_client.converse.assert_called_once()
        call_kwargs = mock_client.converse.call_args[1]
        assert call_kwargs["modelId"] == "anthropic.claude-sonnet-4-20250514-v1:0"
        assert call_kwargs["system"] == [{"text": "system text"}]
        assert call_kwargs["messages"] == [
            {"role": "user", "content": [{"text": "user text"}]}
        ]
        assert call_kwargs["inferenceConfig"]["temperature"] == 0.2
        assert call_kwargs["inferenceConfig"]["maxTokens"] == 4096
        assert result == '{"result": "ok"}'

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

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = ""
        mock_settings.llm_model = "test-model"

        from dashforge.agents.providers.bedrock import BedrockProvider
        provider = BedrockProvider()
        result = provider._converse("sys", "user", 0.5)
        assert result == "part1part2"

    print("[PASS] test_bedrock_converse_multiple_content_blocks")


def test_bedrock_converse_empty_response():
    """_converse should handle empty content blocks gracefully."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": []}}}

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = ""
        mock_settings.llm_model = "test-model"

        from dashforge.agents.providers.bedrock import BedrockProvider
        provider = BedrockProvider()
        result = provider._converse("sys", "user", 0.5)
        assert result == ""

    print("[PASS] test_bedrock_converse_empty_response")


def test_bedrock_model_id_fallback():
    """When llm_bedrock_model_id is empty and llm_model is not a known Anthropic
    API name, should fall back to the Bedrock default model."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "{}"}]}}
    }

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = ""
        mock_settings.llm_model = "unknown-model-id"

        from dashforge.agents.providers.bedrock import BedrockProvider, _BEDROCK_DEFAULT_MODEL
        provider = BedrockProvider()
        assert provider._model_id == _BEDROCK_DEFAULT_MODEL

    print("[PASS] test_bedrock_model_id_fallback")


# ── BedrockProvider async methods ──────────────────────────────────────────

def test_bedrock_chat_json_appends_json_preamble():
    """chat_json should append JSON preamble to system prompt."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": '{"ok": true}'}]}}
    }

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = "test-model"
        mock_settings.llm_model = "test-model"

        from dashforge.agents.providers.bedrock import BedrockProvider
        provider = BedrockProvider()

        result = asyncio.run(provider.chat_json("system prompt", "user prompt", 0.2))

        assert result == '{"ok": true}'
        # Verify the system prompt was augmented with JSON preamble
        call_kwargs = mock_client.converse.call_args[1]
        system_text = call_kwargs["system"][0]["text"]
        assert "system prompt" in system_text
        assert "valid JSON" in system_text

    print("[PASS] test_bedrock_chat_json_appends_json_preamble")


def test_bedrock_chat_text_no_preamble():
    """chat_text should pass system prompt without JSON preamble."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "plain response"}]}}
    }

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = "test-model"
        mock_settings.llm_model = "test-model"

        from dashforge.agents.providers.bedrock import BedrockProvider
        provider = BedrockProvider()

        result = asyncio.run(provider.chat_text("system only", "user msg", 0.3))

        assert result == "plain response"
        call_kwargs = mock_client.converse.call_args[1]
        system_text = call_kwargs["system"][0]["text"]
        assert system_text == "system only"
        assert "JSON" not in system_text

    print("[PASS] test_bedrock_chat_text_no_preamble")


# ── AzureOpenAIProvider tests ──────────────────────────────────────────────

def test_azure_provider_requires_api_base():
    """AzureOpenAIProvider should raise ValueError if llm_api_base is empty."""
    with patch("dashforge.agents.providers.openai_provider.settings") as mock_settings:
        mock_settings.llm_api_base = ""
        mock_settings.llm_api_key = "test-key"
        mock_settings.llm_azure_deployment = ""
        mock_settings.llm_model = "gpt-4o"

        try:
            from dashforge.agents.providers.openai_provider import AzureOpenAIProvider
            AzureOpenAIProvider()
            assert False, "Should have raised ValueError"
        except ValueError as exc:
            assert "azure_endpoint" in str(exc).lower() or "llm_api_base" in str(exc)

    print("[PASS] test_azure_provider_requires_api_base")


def test_azure_deployment_fallback_to_model():
    """When llm_azure_deployment is empty, should use llm_model."""
    with patch("dashforge.agents.providers.openai_provider.settings") as mock_settings, \
         patch("dashforge.agents.providers.openai_provider.openai") as mock_openai:
        mock_settings.llm_api_base = "https://test.openai.azure.com"
        mock_settings.llm_api_key = "test-key"
        mock_settings.llm_azure_deployment = ""
        mock_settings.llm_model = "gpt-4o"
        mock_settings.llm_azure_api_version = "2024-06-01"

        from dashforge.agents.providers.openai_provider import AzureOpenAIProvider
        provider = AzureOpenAIProvider()
        assert provider._deployment == "gpt-4o"

    print("[PASS] test_azure_deployment_fallback_to_model")


def test_azure_deployment_explicit():
    """When llm_azure_deployment is set, should use it over llm_model."""
    with patch("dashforge.agents.providers.openai_provider.settings") as mock_settings, \
         patch("dashforge.agents.providers.openai_provider.openai") as mock_openai:
        mock_settings.llm_api_base = "https://test.openai.azure.com"
        mock_settings.llm_api_key = "test-key"
        mock_settings.llm_azure_deployment = "my-custom-deployment"
        mock_settings.llm_model = "gpt-4o"
        mock_settings.llm_azure_api_version = "2024-06-01"

        from dashforge.agents.providers.openai_provider import AzureOpenAIProvider
        provider = AzureOpenAIProvider()
        assert provider._deployment == "my-custom-deployment"

    print("[PASS] test_azure_deployment_explicit")


# ── Registry tests ─────────────────────────────────────────────────────────

def test_registry_routes_bedrock():
    """Registry should route 'bedrock' to BedrockProvider."""
    import dashforge.agents.providers.registry as reg
    reg._provider = None  # reset singleton

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch.object(reg, "settings") as mock_settings:
        mock_settings.llm_provider = "bedrock"
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = "anthropic.claude-sonnet-4-20250514-v1:0"
        mock_settings.llm_model = "claude-sonnet-4-20250514"

        provider = reg.get_provider()

        from dashforge.agents.providers.bedrock import BedrockProvider
        assert isinstance(provider, BedrockProvider)

    reg._provider = None  # cleanup

    print("[PASS] test_registry_routes_bedrock")


def test_registry_unknown_provider_includes_bedrock_in_error():
    """Unknown provider error message should list 'bedrock' as an option."""
    import dashforge.agents.providers.registry as reg
    reg._provider = None

    with patch.object(reg, "settings") as mock_settings:
        mock_settings.llm_provider = "nonexistent"
        try:
            reg.get_provider()
            assert False, "Should have raised ValueError"
        except ValueError as exc:
            assert "bedrock" in str(exc)
            assert "nonexistent" in str(exc)

    reg._provider = None

    print("[PASS] test_registry_unknown_provider_includes_bedrock_in_error")


# ── CloudWatch PanelQuery schema tests ─────────────────────────────────────

def test_panel_query_cloudwatch_fields():
    """PanelQuery should accept CloudWatch-specific fields including region."""
    from dashforge.models.schemas import PanelQuery
    q = PanelQuery(
        expr="HTTPCode_ELB_5XX",
        datasource_uid="cw1",
        datasource_type="cloudwatch",
        cloudwatch_namespace="AWS/ApplicationELB",
        cloudwatch_stat="Sum",
        cloudwatch_dimensions={"LoadBalancer": ["*"]},
        cloudwatch_region="eu-west-1",
    )
    assert q.cloudwatch_namespace == "AWS/ApplicationELB"
    assert q.cloudwatch_stat == "Sum"
    assert q.cloudwatch_dimensions == {"LoadBalancer": ["*"]}
    assert q.cloudwatch_region == "eu-west-1"
    print("[PASS] test_panel_query_cloudwatch_fields")


def test_panel_query_cloudwatch_fields_default_empty():
    """CloudWatch fields should default to empty when not provided."""
    from dashforge.models.schemas import PanelQuery
    q = PanelQuery(expr="up", datasource_uid="prom1")
    assert q.cloudwatch_namespace == ""
    assert q.cloudwatch_stat == ""
    assert q.cloudwatch_dimensions == {}
    assert q.cloudwatch_region == ""
    print("[PASS] test_panel_query_cloudwatch_fields_default_empty")


# ── Grafana dashboard CloudWatch target rendering ──────────────────────────

def test_dashboard_cloudwatch_target_rendering():
    """CloudWatch panels should include namespace, metricName, statistics, dimensions, region."""
    from dashforge.models.schemas import PanelSpec, PanelQuery
    from dashforge.grafana.dashboard import _build_panel_json

    panel = PanelSpec(
        title="5xx Errors",
        queries=[PanelQuery(
            expr="HTTPCode_ELB_5XX",
            datasource_uid="cw1",
            datasource_type="cloudwatch",
            cloudwatch_namespace="AWS/ApplicationELB",
            cloudwatch_stat="Sum",
            cloudwatch_dimensions={"LoadBalancer": ["app/my-lb/123"]},
            cloudwatch_region="eu-west-1",
        )],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert target["namespace"] == "AWS/ApplicationELB"
    assert target["metricName"] == "HTTPCode_ELB_5XX"
    assert target["statistics"] == ["Sum"]
    assert target["dimensions"] == {"LoadBalancer": ["app/my-lb/123"]}
    assert target["region"] == "eu-west-1"
    print("[PASS] test_dashboard_cloudwatch_target_rendering")


def test_dashboard_prometheus_target_no_cloudwatch_fields():
    """Prometheus panels should NOT include CloudWatch-specific fields."""
    from dashforge.models.schemas import PanelSpec, PanelQuery
    from dashforge.grafana.dashboard import _build_panel_json

    panel = PanelSpec(
        title="Request Rate",
        queries=[PanelQuery(
            expr='rate(http_requests_total[5m])',
            datasource_uid="prom1",
            datasource_type="prometheus",
        )],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert "namespace" not in target
    assert "metricName" not in target
    assert "statistics" not in target
    assert "region" not in target
    print("[PASS] test_dashboard_prometheus_target_no_cloudwatch_fields")


# ── CLI _check_llm bedrock assume-role tests ───────────────────────────────

def test_check_llm_bedrock_with_role_arn_calls_assume_role():
    """When llm_bedrock_role_arn is set, _check_llm must call sts.assume_role
    before declaring success — not just get_caller_identity on the base session."""
    mock_boto3 = MagicMock()
    base_session = MagicMock()
    assumed_session = MagicMock()

    mock_sts_base = MagicMock()
    mock_sts_base.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLE",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
        }
    }
    base_session.client.return_value = mock_sts_base

    mock_sts_assumed = MagicMock()
    mock_sts_assumed.get_caller_identity.return_value = {"Account": "123456789012"}
    assumed_session.client.return_value = mock_sts_assumed

    mock_boto3.Session.side_effect = [base_session, assumed_session]

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.config.settings") as mock_settings:
        mock_settings.llm_provider = "bedrock"
        mock_settings.llm_api_key = ""
        mock_settings.llm_model = "claude-sonnet-4-20250514"
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = "arn:aws:iam::123456789012:role/TestRole"
        mock_settings.llm_bedrock_model_id = ""

        from dashforge.cli import _check_llm
        result = _check_llm()

        # Must have called assume_role on the base session's STS client
        mock_sts_base.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="dashforge-bedrock",
            DurationSeconds=3600,
        )
        # get_caller_identity should be called on the ASSUMED session, not base
        mock_sts_assumed.get_caller_identity.assert_called_once()
        assert result is True

    print("[PASS] test_check_llm_bedrock_with_role_arn_calls_assume_role")


def test_check_llm_bedrock_bad_role_arn_returns_false():
    """A failing assume_role should make _check_llm return False."""
    mock_boto3 = MagicMock()
    base_session = MagicMock()

    mock_sts = MagicMock()
    mock_sts.assume_role.side_effect = Exception(
        "An error occurred (AccessDenied) when calling the AssumeRole operation"
    )
    base_session.client.return_value = mock_sts

    mock_boto3.Session.return_value = base_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.config.settings") as mock_settings:
        mock_settings.llm_provider = "bedrock"
        mock_settings.llm_api_key = ""
        mock_settings.llm_model = "claude-sonnet-4-20250514"
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = "arn:aws:iam::999999999999:role/BadRole"
        mock_settings.llm_bedrock_model_id = ""

        from dashforge.cli import _check_llm
        result = _check_llm()

        assert result is False

    print("[PASS] test_check_llm_bedrock_bad_role_arn_returns_false")


# ── pyproject.toml bedrock optional extra ──────────────────────────────────

def test_pyproject_has_bedrock_optional_extra():
    """pyproject.toml must define a [project.optional-dependencies] bedrock extra
    that installs boto3, so 'pip install dashforge[bedrock]' actually works."""
    from pathlib import Path
    import tomllib

    toml_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    opt_deps = data.get("project", {}).get("optional-dependencies", {})
    assert "bedrock" in opt_deps, "Missing [project.optional-dependencies] bedrock extra"
    bedrock_deps = opt_deps["bedrock"]
    assert any("boto3" in dep for dep in bedrock_deps), "bedrock extra must include boto3"
    print("[PASS] test_pyproject_has_bedrock_optional_extra")


# ── Bedrock botocore transient error retry ─────────────────────────────────

def test_bedrock_converse_wraps_throttling_for_retry():
    """Bedrock ThrottlingException should be re-raised as LLMTransientError
    so tenacity retries it, not bubble out as a raw botocore error."""
    from dashforge.agents.llm import call_llm, LLMTransientError
    from pydantic import BaseModel

    class SimpleModel(BaseModel):
        value: int

    # Create a proper ClientError-like class (simulates botocore.exceptions.ClientError)
    class ClientError(Exception):
        def __init__(self, msg, response):
            super().__init__(msg)
            self.response = response

    throttle_exc = ClientError(
        "An error occurred (ThrottlingException)",
        {"Error": {"Code": "ThrottlingException"}},
    )

    mock_provider = MagicMock()
    # First call throttles, second succeeds
    mock_provider.chat_json = AsyncMock(
        side_effect=[throttle_exc, '{"value": 99}']
    )

    with patch("dashforge.agents.llm.get_provider", return_value=mock_provider):
        try:
            result = asyncio.run(call_llm("sys", "user", SimpleModel))
            # If retry worked, we get the second response
            assert result.value == 99
            assert mock_provider.chat_json.call_count == 2
        except Exception as exc:
            # If retry didn't work, this will be a raw exception — that's the bug
            assert isinstance(exc, LLMTransientError), \
                f"Expected LLMTransientError for retry, got {type(exc).__name__}: {exc}"

    print("[PASS] test_bedrock_converse_wraps_throttling_for_retry")


# ── Bedrock model ID fallback ──────────────────────────────────────────────

def test_bedrock_model_id_fallback_uses_bedrock_default():
    """When llm_bedrock_model_id is empty and llm_model is the Anthropic API default,
    BedrockProvider should use a valid Bedrock model ID, not 'claude-sonnet-4-20250514'."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "{}"}]}}
    }

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = ""
        mock_settings.llm_model = "claude-sonnet-4-20250514"  # Anthropic API default
        mock_settings.llm_provider = "bedrock"

        from dashforge.agents.providers.bedrock import BedrockProvider
        provider = BedrockProvider()

        # Must NOT be the bare Anthropic model name
        assert provider._model_id != "claude-sonnet-4-20250514", \
            f"Model ID should be a Bedrock ARN/ID, not Anthropic API name: {provider._model_id}"
        # Should contain 'anthropic.' prefix (Bedrock format)
        assert "anthropic." in provider._model_id or "bedrock" in provider._model_id.lower(), \
            f"Expected Bedrock model ID format, got: {provider._model_id}"

    print("[PASS] test_bedrock_model_id_fallback_uses_bedrock_default")


def test_bedrock_resolve_model_id_uses_list_foundation_models():
    """_resolve_bedrock_model_id should call ListFoundationModels API to find
    the Bedrock model ID matching an Anthropic API model name."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.return_value = {
        "modelSummaries": [
            {"modelId": "anthropic.claude-3-haiku-20240307-v1:0", "providerName": "Anthropic"},
            {"modelId": "anthropic.claude-sonnet-4-20250514-v1:0", "providerName": "Anthropic"},
            {"modelId": "meta.llama3-70b-instruct-v1:0", "providerName": "Meta"},
        ]
    }

    result = _resolve_bedrock_model_id("claude-sonnet-4-20250514", mock_bedrock_client)
    assert result == "anthropic.claude-sonnet-4-20250514-v1:0"
    mock_bedrock_client.list_foundation_models.assert_called_once()
    print("[PASS] test_bedrock_resolve_model_id_uses_list_foundation_models")


def test_bedrock_resolve_model_id_api_failure_falls_back_to_static_map():
    """When ListFoundationModels fails, _resolve_bedrock_model_id should fall
    back to the static _ANTHROPIC_TO_BEDROCK map."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id, _ANTHROPIC_TO_BEDROCK

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.side_effect = Exception("AccessDenied")

    result = _resolve_bedrock_model_id("claude-sonnet-4-20250514", mock_bedrock_client)
    assert result == _ANTHROPIC_TO_BEDROCK["claude-sonnet-4-20250514"]
    print("[PASS] test_bedrock_resolve_model_id_api_failure_falls_back_to_static_map")


def test_bedrock_resolve_model_id_caches_result():
    """Repeated calls to _resolve_bedrock_model_id should not repeat the API call."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id, _resolve_cache

    # Clear any cached state
    _resolve_cache.clear()

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.return_value = {
        "modelSummaries": [
            {"modelId": "anthropic.claude-sonnet-4-20250514-v1:0", "providerName": "Anthropic"},
        ]
    }

    result1 = _resolve_bedrock_model_id("claude-sonnet-4-20250514", mock_bedrock_client)
    result2 = _resolve_bedrock_model_id("claude-sonnet-4-20250514", mock_bedrock_client)
    assert result1 == result2 == "anthropic.claude-sonnet-4-20250514-v1:0"
    # API should only have been called once despite two resolve calls
    assert mock_bedrock_client.list_foundation_models.call_count == 1

    _resolve_cache.clear()
    print("[PASS] test_bedrock_resolve_model_id_caches_result")


def test_bedrock_resolve_model_id_unknown_model_returns_default():
    """When the model is not found via API or static map, return the Bedrock default."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id, _BEDROCK_DEFAULT_MODEL, _resolve_cache

    _resolve_cache.clear()

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.return_value = {
        "modelSummaries": [
            {"modelId": "meta.llama3-70b-instruct-v1:0", "providerName": "Meta"},
        ]
    }

    result = _resolve_bedrock_model_id("totally-unknown-model", mock_bedrock_client)
    assert result == _BEDROCK_DEFAULT_MODEL

    _resolve_cache.clear()
    print("[PASS] test_bedrock_resolve_model_id_unknown_model_returns_default")


# ── Bug: STS credential refresh for long-running processes ─────────────────

def test_bedrock_assume_role_uses_refreshable_credentials():
    """When llm_bedrock_role_arn is set, _build_boto3_session must return a
    session with auto-refreshable credentials, not static one-shot STS creds.
    The refresh callback must be callable to re-assume before expiry."""
    mock_boto3 = MagicMock()
    base_session = MagicMock()
    refreshed_session = MagicMock()

    mock_sts = MagicMock()
    from datetime import datetime, timezone, timedelta
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    mock_sts.assume_role.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLE",
            "SecretAccessKey": "secret",
            "SessionToken": "token",
            "Expiration": future,
        }
    }
    base_session.client.return_value = mock_sts
    mock_boto3.Session.side_effect = [base_session, refreshed_session]

    mock_botocore_session = MagicMock()
    mock_refreshable = MagicMock()
    mock_botocore_creds_mod = MagicMock()
    mock_botocore_creds_mod.RefreshableCredentials.create_from_metadata.return_value = mock_refreshable
    mock_botocore_sess_mod = MagicMock()
    mock_botocore_sess_mod.get_session.return_value = mock_botocore_session

    with patch.dict("sys.modules", {
             "boto3": mock_boto3,
             "botocore": MagicMock(),
             "botocore.credentials": mock_botocore_creds_mod,
             "botocore.session": mock_botocore_sess_mod,
         }), \
         patch("dashforge.agents.providers.bedrock.settings") as mock_settings:
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = "arn:aws:iam::123456789012:role/TestRole"

        from dashforge.agents.providers.bedrock import _build_boto3_session
        session = _build_boto3_session()

        # The refresh_using callback must be set so creds auto-renew
        rc_cls = mock_botocore_creds_mod.RefreshableCredentials
        rc_cls.create_from_metadata.assert_called_once()
        call_kwargs = rc_cls.create_from_metadata.call_args[1]
        refresh_fn = call_kwargs["refresh_using"]
        assert callable(refresh_fn), "refresh_using must be a callable"

        # Calling it again should re-assume the role
        result = refresh_fn()
        assert result["access_key"] == "ASIAEXAMPLE"
        assert result["token"] == "token"
        assert mock_sts.assume_role.call_count == 2  # initial + refresh

    print("[PASS] test_bedrock_assume_role_uses_refreshable_credentials")


def test_bedrock_provider_prefixed_model_id_preserved():
    """When llm_model is already a provider-prefixed Bedrock ID like
    'meta.llama3-70b-instruct-v1:0', it should be used as-is even if
    ListFoundationModels is unavailable — never silently replaced."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id, _resolve_cache
    _resolve_cache.clear()

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.side_effect = Exception("AccessDenied")

    # Provider-prefixed IDs must pass through without resolution
    for model_id in [
        "meta.llama3-70b-instruct-v1:0",
        "amazon.titan-text-express-v1",
        "cohere.command-r-plus-v1:0",
        "mistral.mixtral-8x7b-instruct-v0:1",
        "anthropic.claude-sonnet-4-20250514-v1:0",
    ]:
        _resolve_cache.clear()
        result = _resolve_bedrock_model_id(model_id, mock_bedrock_client)
        assert result == model_id, (
            f"Provider-prefixed model ID {model_id!r} should be preserved as-is, "
            f"got: {result!r}"
        )
        # Should NOT have called ListFoundationModels — passthrough is immediate
        mock_bedrock_client.list_foundation_models.assert_not_called()

    _resolve_cache.clear()
    print("[PASS] test_bedrock_provider_prefixed_model_id_preserved")


def test_bedrock_service_specific_exception_retried():
    """Bedrock Runtime raises service-specific exceptions like ThrottlingException
    whose type name is NOT 'ClientError'. These must still be caught as transient."""
    from dashforge.agents.llm import call_llm, LLMTransientError
    from pydantic import BaseModel
    from tenacity import RetryError, wait_none

    class Simple(BaseModel):
        v: int

    # Simulate Bedrock's service-specific ThrottlingException
    # (NOT a ClientError subclass — different type hierarchy)
    class ThrottlingException(Exception):
        """Bedrock-specific throttling; __name__ is 'ThrottlingException'."""
        def __init__(self, msg):
            super().__init__(msg)
            self.response = {"Error": {"Code": "ThrottlingException"}}

    throttle = ThrottlingException("Rate exceeded")

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        side_effect=[throttle, '{"v": 42}']
    )

    original_wait = call_llm.retry.wait
    call_llm.retry.wait = wait_none()

    try:
        with patch("dashforge.agents.llm.get_provider", return_value=mock_provider):
            result = asyncio.run(call_llm("sys", "user", Simple))
            assert result.v == 42, f"Expected retry to succeed, got: {result}"
            assert mock_provider.chat_json.call_count == 2, (
                f"Expected 2 calls (1 throttle + 1 success), "
                f"got {mock_provider.chat_json.call_count}"
            )
    finally:
        call_llm.retry.wait = original_wait

    print("[PASS] test_bedrock_service_specific_exception_retried")


def test_pyproject_boto3_minimum_version_supports_converse():
    """The bedrock optional extra must require boto3>=1.34.116 (when Converse
    API was added), not an earlier version that would fail at runtime."""
    from pathlib import Path
    import tomllib
    import re

    toml_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    bedrock_deps = data["project"]["optional-dependencies"]["bedrock"]
    boto3_dep = next(d for d in bedrock_deps if "boto3" in d)

    # Extract version from spec like "boto3>=1.34.0"
    match = re.search(r"(\d+\.\d+\.\d+)", boto3_dep)
    assert match, f"Could not parse version from: {boto3_dep}"
    parts = match.group(1).split(".")
    major, minor, patch_v = int(parts[0]), int(parts[1]), int(parts[2])

    # Must be >= 1.34.116
    version_tuple = (major, minor, patch_v)
    assert version_tuple >= (1, 34, 116), (
        f"boto3 lower bound {match.group(1)} is too low — Converse API "
        f"requires >=1.34.116. Older versions will AttributeError at runtime."
    )

    print("[PASS] test_pyproject_boto3_minimum_version_supports_converse")


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
    print("=== All provider tests passed ===")
