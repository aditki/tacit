"""Tests for AWS Bedrock LLM provider.

Covers:
- _build_boto3_session: explicit keys, default chain, assume-role with RefreshableCredentials
- BedrockProvider._converse: API call structure, multi-block concat, empty response
- Model ID resolution: ListFoundationModels, static map fallback, caching, passthrough
- Inference profile retry: ValidationException → regional/global prefix retry + caching
- Mistral system prompt folding
- _inference_profile_id: us/eu geo prefix, global fallback for APAC/other regions
- Transient error retry: ThrottlingException, service-specific exceptions
- pyproject.toml: bedrock optional extra, boto3 minimum version
"""
import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Helper ─────────────────────────────────────────────────────────────────

def _make_bedrock_provider(mock_client, mock_settings_overrides=None):
    """Helper to construct a BedrockProvider with a mocked client."""
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
        mock_settings.llm_model = "claude-sonnet-4-20250514"
        if mock_settings_overrides:
            for k, v in mock_settings_overrides.items():
                setattr(mock_settings, k, v)
        from dashforge.agents.providers.bedrock import BedrockProvider
        return BedrockProvider(), mock_settings


# ── _build_boto3_session tests ─────────────────────────────────────────────

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

        mock_sts_client.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/TestRole",
            RoleSessionName="dashforge-bedrock",
            DurationSeconds=3600,
        )
        rc_cls = mock_botocore_creds_mod.RefreshableCredentials
        rc_cls.create_from_metadata.assert_called_once()
        call_kwargs = rc_cls.create_from_metadata.call_args[1]
        assert call_kwargs["method"] == "sts-assume-role"
        assert callable(call_kwargs["refresh_using"])
        last_session_kwargs = mock_boto3.Session.call_args_list[-1][1]
        assert "botocore_session" in last_session_kwargs

    print("[PASS] test_bedrock_session_assume_role")


def test_bedrock_session_no_boto3_raises():
    """Missing boto3 should raise a helpful ImportError."""
    from dashforge.agents.providers.bedrock import _build_boto3_session

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


def test_bedrock_assume_role_uses_refreshable_credentials():
    """The refresh callback must be callable to re-assume before expiry."""
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

        rc_cls = mock_botocore_creds_mod.RefreshableCredentials
        rc_cls.create_from_metadata.assert_called_once()
        call_kwargs = rc_cls.create_from_metadata.call_args[1]
        refresh_fn = call_kwargs["refresh_using"]
        assert callable(refresh_fn)

        result = refresh_fn()
        assert result["access_key"] == "ASIAEXAMPLE"
        assert result["token"] == "token"
        assert mock_sts.assume_role.call_count == 2  # initial + refresh

    print("[PASS] test_bedrock_assume_role_uses_refreshable_credentials")


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

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "anthropic.claude-sonnet-4-20250514-v1:0"},
    )

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

    provider, _ = _make_bedrock_provider(
        mock_client, {"llm_bedrock_model_id": "test-model"},
    )
    result = provider._converse("sys", "user", 0.5)
    assert result == "part1part2"

    print("[PASS] test_bedrock_converse_multiple_content_blocks")


def test_bedrock_converse_empty_response():
    """_converse should handle empty content blocks gracefully."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {"output": {"message": {"content": []}}}

    provider, _ = _make_bedrock_provider(
        mock_client, {"llm_bedrock_model_id": "test-model"},
    )
    result = provider._converse("sys", "user", 0.5)
    assert result == ""

    print("[PASS] test_bedrock_converse_empty_response")


# ── Model ID resolution ───────────────────────────────────────────────────

def test_bedrock_model_id_fallback():
    """When llm_bedrock_model_id is empty and llm_model is not a known Anthropic
    API name, should fall back to the bare Bedrock default model.
    _converse() handles inference-profile retry at invocation time."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "{}"}]}}
    }

    provider, _ = _make_bedrock_provider(
        mock_client, {"llm_model": "unknown-model-id"},
    )

    from dashforge.agents.providers.bedrock import _BEDROCK_DEFAULT_MODEL
    assert provider._model_id == _BEDROCK_DEFAULT_MODEL

    print("[PASS] test_bedrock_model_id_fallback")


def test_bedrock_model_id_fallback_uses_bedrock_default():
    """When llm_model is the Anthropic API default, should resolve to a
    valid Bedrock model ID, not the bare Anthropic API name."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "{}"}]}}
    }

    provider, _ = _make_bedrock_provider(mock_client)

    assert provider._model_id != "claude-sonnet-4-20250514", \
        f"Model ID should be a Bedrock ID, not Anthropic API name: {provider._model_id}"
    assert "anthropic." in provider._model_id, \
        f"Expected Bedrock model ID format, got: {provider._model_id}"

    print("[PASS] test_bedrock_model_id_fallback_uses_bedrock_default")


def test_bedrock_resolve_model_id_uses_list_foundation_models():
    """_resolve_bedrock_model_id should call ListFoundationModels API."""
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
    """When ListFoundationModels fails, should fall back to bare static map entry."""
    from dashforge.agents.providers.bedrock import (
        _resolve_bedrock_model_id, _ANTHROPIC_TO_BEDROCK, _resolve_cache,
    )
    _resolve_cache.clear()

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.side_effect = Exception("AccessDenied")

    result = _resolve_bedrock_model_id("claude-sonnet-4-20250514", mock_bedrock_client)
    expected = _ANTHROPIC_TO_BEDROCK["claude-sonnet-4-20250514"]
    assert result == expected, f"Expected bare {expected!r}, got {result!r}"

    _resolve_cache.clear()
    print("[PASS] test_bedrock_resolve_model_id_api_failure_falls_back_to_static_map")


def test_bedrock_resolve_model_id_caches_result():
    """Repeated calls should not repeat the API call."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id, _resolve_cache
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
    assert mock_bedrock_client.list_foundation_models.call_count == 1

    _resolve_cache.clear()
    print("[PASS] test_bedrock_resolve_model_id_caches_result")


def test_bedrock_resolve_model_id_unknown_model_returns_default():
    """Unknown model falls back to bare Bedrock default."""
    from dashforge.agents.providers.bedrock import (
        _resolve_bedrock_model_id, _BEDROCK_DEFAULT_MODEL, _resolve_cache,
    )
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


def test_bedrock_provider_prefixed_model_id_preserved():
    """Provider-prefixed IDs should pass through without resolution."""
    from dashforge.agents.providers.bedrock import _resolve_bedrock_model_id, _resolve_cache
    _resolve_cache.clear()

    mock_bedrock_client = MagicMock()
    mock_bedrock_client.list_foundation_models.side_effect = Exception("AccessDenied")

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
            f"Provider-prefixed {model_id!r} should be preserved, got: {result!r}"
        )
        mock_bedrock_client.list_foundation_models.assert_not_called()

    _resolve_cache.clear()
    print("[PASS] test_bedrock_provider_prefixed_model_id_preserved")


# ── Async methods ──────────────────────────────────────────────────────────

def test_bedrock_chat_json_appends_json_preamble():
    """chat_json should append JSON preamble to system prompt."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": '{"ok": true}'}]}}
    }

    provider, _ = _make_bedrock_provider(
        mock_client, {"llm_bedrock_model_id": "test-model"},
    )

    result = asyncio.run(provider.chat_json("system prompt", "user prompt", 0.2))

    assert result == '{"ok": true}'
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

    provider, _ = _make_bedrock_provider(
        mock_client, {"llm_bedrock_model_id": "test-model"},
    )

    result = asyncio.run(provider.chat_text("system only", "user msg", 0.3))

    assert result == "plain response"
    call_kwargs = mock_client.converse.call_args[1]
    system_text = call_kwargs["system"][0]["text"]
    assert system_text == "system only"
    assert "JSON" not in system_text

    print("[PASS] test_bedrock_chat_text_no_preamble")


# ── Converse inference-profile retry ───────────────────────────────────────

def test_converse_retries_with_inference_profile_on_validation_error():
    """Bare model ID + ValidationException → retry with regional profile → cache."""
    class ValidationException(Exception):
        pass

    mock_client = MagicMock()
    mock_client.converse.side_effect = [
        ValidationException("model not available for on-demand"),
        {"output": {"message": {"content": [{"text": '{"ok": true}'}]}}},
    ]

    provider, _ = _make_bedrock_provider(mock_client)
    bare_id = provider._model_id
    assert not bare_id.startswith("us.")

    result = provider._converse("sys", "user", 0.2)

    assert result == '{"ok": true}'
    assert mock_client.converse.call_count == 2
    assert provider._model_id.startswith("us.")
    assert provider._model_id == f"us.{bare_id}"

    print("[PASS] test_converse_retries_with_inference_profile_on_validation_error")


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
        provider._converse("sys", "user", 0.2)
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
        provider._converse("sys", "user", 0.2)
        assert False, "Should have raised ThrottlingException"
    except Exception as exc:
        assert type(exc).__name__ == "ThrottlingException"
        assert mock_client.converse.call_count == 1

    print("[PASS] test_converse_no_retry_on_non_validation_error")


def test_converse_cached_profile_id_skips_retry():
    """After successful retry, subsequent calls go direct (no retry)."""
    class ValidationException(Exception):
        pass

    mock_client = MagicMock()
    mock_client.converse.side_effect = [
        ValidationException("model not available"),
        {"output": {"message": {"content": [{"text": "first"}]}}},
        {"output": {"message": {"content": [{"text": "second"}]}}},
    ]

    provider, _ = _make_bedrock_provider(mock_client)

    result1 = provider._converse("sys", "user", 0.2)
    assert result1 == "first"
    assert mock_client.converse.call_count == 2  # 1 fail + 1 retry

    result2 = provider._converse("sys", "user", 0.2)
    assert result2 == "second"
    assert mock_client.converse.call_count == 3  # +1 direct

    print("[PASS] test_converse_cached_profile_id_skips_retry")


# ── Mistral system prompt folding ──────────────────────────────────────────

def test_mistral_model_folds_system_into_user_message():
    """Mistral models: system prompt folded into user message, no system field."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": '{"v": 1}'}]}}
    }

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "mistral.mixtral-8x7b-instruct-v0:1"},
    )

    result = provider._converse("system instructions", "user question", 0.3)

    assert result == '{"v": 1}'
    call_kwargs = mock_client.converse.call_args[1]
    assert "system" not in call_kwargs
    user_text = call_kwargs["messages"][0]["content"][0]["text"]
    assert "system instructions" in user_text
    assert "user question" in user_text

    print("[PASS] test_mistral_model_folds_system_into_user_message")


def test_non_mistral_model_uses_system_field():
    """Non-Mistral models should use the standard system field."""
    mock_client = MagicMock()
    mock_client.converse.return_value = {
        "output": {"message": {"content": [{"text": "ok"}]}}
    }

    provider, _ = _make_bedrock_provider(
        mock_client,
        {"llm_bedrock_model_id": "anthropic.claude-sonnet-4-20250514-v1:0"},
    )

    provider._converse("system text", "user text", 0.2)

    call_kwargs = mock_client.converse.call_args[1]
    assert "system" in call_kwargs
    assert call_kwargs["system"] == [{"text": "system text"}]
    assert call_kwargs["messages"][0]["content"][0]["text"] == "user text"

    print("[PASS] test_non_mistral_model_uses_system_field")


# ── _inference_profile_id unit tests ───────────────────────────────────────

def test_inference_profile_id_us_region():
    from dashforge.agents.providers.bedrock import _inference_profile_id
    result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", "us-east-1")
    assert result == "us.anthropic.claude-sonnet-4-20250514-v1:0"
    print("[PASS] test_inference_profile_id_us_region")


def test_inference_profile_id_eu_region():
    from dashforge.agents.providers.bedrock import _inference_profile_id
    result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", "eu-west-1")
    assert result == "eu.anthropic.claude-sonnet-4-20250514-v1:0"
    print("[PASS] test_inference_profile_id_eu_region")


def test_inference_profile_id_apac_uses_global():
    """APAC regions should use global. prefix, not ap."""
    from dashforge.agents.providers.bedrock import _inference_profile_id
    result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", "ap-northeast-1")
    assert result == "global.anthropic.claude-sonnet-4-20250514-v1:0"
    print("[PASS] test_inference_profile_id_apac_uses_global")


def test_inference_profile_id_other_regions_use_global():
    """sa-*, me-*, ca-*, af-* regions should all use global. prefix."""
    from dashforge.agents.providers.bedrock import _inference_profile_id
    for region in ["sa-east-1", "me-south-1", "ca-central-1", "af-south-1"]:
        result = _inference_profile_id("anthropic.claude-sonnet-4-20250514-v1:0", region)
        assert result.startswith("global."), f"Region {region}: expected global., got {result!r}"
    print("[PASS] test_inference_profile_id_other_regions_use_global")


# ── Transient error retry ─────────────────────────────────────────────────

def test_bedrock_converse_wraps_throttling_for_retry():
    """Bedrock ThrottlingException → LLMTransientError so tenacity retries."""
    from dashforge.agents.llm import call_llm, LLMTransientError
    from pydantic import BaseModel

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

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        side_effect=[throttle_exc, '{"value": 99}']
    )

    with patch("dashforge.agents.llm.get_provider", return_value=mock_provider):
        try:
            result = asyncio.run(call_llm("sys", "user", SimpleModel))
            assert result.value == 99
            assert mock_provider.chat_json.call_count == 2
        except Exception as exc:
            assert isinstance(exc, LLMTransientError), \
                f"Expected LLMTransientError, got {type(exc).__name__}: {exc}"

    print("[PASS] test_bedrock_converse_wraps_throttling_for_retry")


def test_bedrock_service_specific_exception_retried():
    """Bedrock service-specific ThrottlingException (not ClientError) → retried."""
    from dashforge.agents.llm import call_llm, LLMTransientError
    from pydantic import BaseModel
    from tenacity import wait_none

    class Simple(BaseModel):
        v: int

    class ThrottlingException(Exception):
        def __init__(self, msg):
            super().__init__(msg)
            self.response = {"Error": {"Code": "ThrottlingException"}}

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        side_effect=[ThrottlingException("Rate exceeded"), '{"v": 42}']
    )

    original_wait = call_llm.retry.wait
    call_llm.retry.wait = wait_none()

    try:
        with patch("dashforge.agents.llm.get_provider", return_value=mock_provider):
            result = asyncio.run(call_llm("sys", "user", Simple))
            assert result.v == 42
            assert mock_provider.chat_json.call_count == 2
    finally:
        call_llm.retry.wait = original_wait

    print("[PASS] test_bedrock_service_specific_exception_retried")


# ── pyproject.toml ─────────────────────────────────────────────────────────

def test_pyproject_has_bedrock_optional_extra():
    """pyproject.toml must define bedrock extra with boto3."""
    from pathlib import Path
    import tomllib

    toml_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    opt_deps = data.get("project", {}).get("optional-dependencies", {})
    assert "bedrock" in opt_deps
    assert any("boto3" in dep for dep in opt_deps["bedrock"])
    print("[PASS] test_pyproject_has_bedrock_optional_extra")


def test_pyproject_boto3_minimum_version_supports_converse():
    """bedrock extra must require boto3>=1.34.116 (Converse API)."""
    from pathlib import Path
    import tomllib
    import re

    toml_path = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)

    bedrock_deps = data["project"]["optional-dependencies"]["bedrock"]
    boto3_dep = next(d for d in bedrock_deps if "boto3" in d)

    match = re.search(r"(\d+\.\d+\.\d+)", boto3_dep)
    assert match, f"Could not parse version from: {boto3_dep}"
    parts = [int(x) for x in match.group(1).split(".")]
    assert tuple(parts) >= (1, 34, 116), (
        f"boto3 lower bound {match.group(1)} too low — Converse API requires >=1.34.116"
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
    print("=== All Bedrock tests passed ===")
