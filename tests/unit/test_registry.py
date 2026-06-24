"""Tests for LLM provider registry.

Covers:
- Provider routing for 'bedrock'
- Unknown provider error message
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_registry_routes_bedrock():
    """Registry should route 'bedrock' to BedrockProvider."""
    import tacit.agents.providers.registry as reg

    reg._provider = None  # reset singleton

    mock_boto3 = MagicMock()
    mock_session = MagicMock()
    mock_client = MagicMock()
    mock_session.client.return_value = mock_client
    mock_boto3.Session.return_value = mock_session

    with patch.dict("sys.modules", {"boto3": mock_boto3}), patch.object(reg, "settings") as mock_settings:
        mock_settings.llm_provider = "bedrock"
        mock_settings.llm_bedrock_region = "us-east-1"
        mock_settings.llm_aws_access_key_id = ""
        mock_settings.llm_aws_secret_access_key = ""
        mock_settings.llm_bedrock_role_arn = ""
        mock_settings.llm_bedrock_model_id = "anthropic.claude-sonnet-4-20250514-v1:0"
        mock_settings.llm_model = "claude-sonnet-4-20250514"

        provider = reg.get_provider()

        from tacit.agents.providers.bedrock import BedrockProvider

        assert isinstance(provider, BedrockProvider)

    reg._provider = None  # cleanup

    print("[PASS] test_registry_routes_bedrock")


def test_registry_unknown_provider_includes_bedrock_in_error():
    """Unknown provider error message should list 'bedrock' as an option."""
    import tacit.agents.providers.registry as reg

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
    print("=== All registry tests passed ===")
