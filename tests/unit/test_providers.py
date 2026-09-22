"""Tests for CloudWatch schema/rendering and CLI doctor checks.

Covers:
- PanelQuery: CloudWatch fields (namespace, stat, dimensions, region)
- Dashboard rendering: CW target JSON with region, namespace stripping, dimension normalization
- CLI non-Bedrock provider checks
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── CloudWatch PanelQuery schema tests ─────────────────────────────────────


def test_panel_query_cloudwatch_fields():
    """PanelQuery should accept CloudWatch-specific fields including region."""
    from tacit.models.schemas import PanelQuery

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
    from tacit.models.schemas import PanelQuery

    q = PanelQuery(expr="up", datasource_uid="prom1")
    assert q.cloudwatch_namespace == ""
    assert q.cloudwatch_stat == ""
    assert q.cloudwatch_dimensions == {}
    assert q.cloudwatch_region == ""
    print("[PASS] test_panel_query_cloudwatch_fields_default_empty")


# ── Grafana dashboard CloudWatch target rendering ──────────────────────────


def test_dashboard_cloudwatch_target_rendering():
    """CloudWatch panels should include namespace, metricName, statistics, dimensions, region."""
    from tacit.grafana.dashboard import _build_panel_json
    from tacit.models.schemas import PanelQuery, PanelSpec

    panel = PanelSpec(
        title="5xx Errors",
        queries=[
            PanelQuery(
                expr="HTTPCode_ELB_5XX",
                datasource_uid="cw1",
                datasource_type="cloudwatch",
                cloudwatch_namespace="AWS/ApplicationELB",
                cloudwatch_stat="Sum",
                cloudwatch_dimensions={"LoadBalancer": ["app/my-lb/123"]},
                cloudwatch_region="eu-west-1",
            )
        ],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert target["namespace"] == "AWS/ApplicationELB"
    assert target["metricName"] == "HTTPCode_ELB_5XX"
    assert target["statistics"] == ["Sum"]
    assert target["dimensions"] == {"LoadBalancer": "app/my-lb/123"}  # single-element list normalized to str
    assert target["region"] == "eu-west-1"
    print("[PASS] test_dashboard_cloudwatch_target_rendering")


def test_dashboard_prometheus_target_no_cloudwatch_fields():
    """Prometheus panels should NOT include CloudWatch-specific fields."""
    from tacit.grafana.dashboard import _build_panel_json
    from tacit.models.schemas import PanelQuery, PanelSpec

    panel = PanelSpec(
        title="Request Rate",
        queries=[
            PanelQuery(
                expr="rate(http_requests_total[5m])",
                datasource_uid="prom1",
                datasource_type="prometheus",
            )
        ],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert "namespace" not in target
    assert "metricName" not in target
    assert "statistics" not in target
    assert "region" not in target
    print("[PASS] test_dashboard_prometheus_target_no_cloudwatch_fields")


# ── CLI _check_llm tests ───────────────────────────────────────────────────


def test_llm_zero_key_mode_only_downgrades_key_based_providers():
    from tacit.cli import _llm_zero_key_mode

    with patch("tacit.config.settings") as mock_settings:
        mock_settings.intent_fallback_enabled = True
        mock_settings.llm_provider = "openai"
        mock_settings.llm_api_key = ""
        mock_settings.llm_api_base = ""
        assert _llm_zero_key_mode(mock_settings) is True

        mock_settings.llm_api_base = "http://localhost:8001/v1"
        assert _llm_zero_key_mode(mock_settings) is False

        mock_settings.llm_api_base = ""
        mock_settings.llm_provider = "ollama"
        assert _llm_zero_key_mode(mock_settings) is False

        mock_settings.llm_provider = "bedrock"
        assert _llm_zero_key_mode(mock_settings) is False


def test_cli_version_uses_renamed_distribution_metadata():
    from tacit.cli import _get_version

    def fake_version(distribution: str) -> str:
        if distribution == "tacit-ai":
            return "1.2.3"
        raise RuntimeError(distribution)

    with patch("importlib.metadata.version", side_effect=fake_version):
        assert _get_version() == "1.2.3"


def test_cli_version_falls_back_to_packaged_tacit_version_without_metadata():
    from tacit import __version__
    from tacit.cli import _get_version

    with (
        patch("importlib.metadata.version", side_effect=LookupError("metadata unavailable")),
        patch("tacit.cli.Path.exists", return_value=False),
    ):
        assert _get_version() == __version__


def test_check_llm_openai_compatible_base_without_key_is_configured():
    from tacit.cli import _check_llm

    with patch("tacit.config.settings") as mock_settings:
        mock_settings.llm_provider = "openai"
        mock_settings.llm_api_key = ""
        mock_settings.llm_api_base = "http://localhost:8001/v1"
        mock_settings.llm_model = "gpt-4o-mini"
        mock_settings.intent_fallback_enabled = True

        assert _check_llm(mock_settings) is True


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
    print("=== All CloudWatch/CLI tests passed ===")
