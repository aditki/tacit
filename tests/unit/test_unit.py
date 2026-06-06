"""Unit tests for DashForge core modules."""

import os
import sys

import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dashforge.agents.metrics_discovery import _keyword_filter as md_keyword_filter
from dashforge.context.enrichment import enrich_context, format_context_for_prompt
from dashforge.context.registry import get_context_provider
from dashforge.grafana.adapters.cloudwatch import _select_namespaces
from dashforge.grafana.adapters.registry import get_adapter, get_adapter_for_type, supported_datasource_types
from dashforge.grafana.dashboard import TIMERANGE_MAP, _build_panel_json, build_dashboard_json
from dashforge.grafana.datasource import (
    filter_datasources_by_signal,
    filter_searchable_datasources,
)
from dashforge.models.schemas import (
    ContextChunk,
    DashboardSpec,
    DashRequest,
    DashResponse,
    DatasourceInfo,
    DiscoveredMetric,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)


def test_intent_model():
    intent = Intent(
        summary="High latency on checkout service",
        domain="application",
        services=["checkout"],
        signals=[SignalType.METRICS],
        keywords=["latency", "p99", "error_rate"],
        timerange="1h",
    )
    assert intent.domain == "application"
    assert len(intent.keywords) == 3
    assert intent.signals == [SignalType.METRICS]
    print("[PASS] test_intent_model")


def test_dashboard_spec_model():
    spec = DashboardSpec(
        title="Test Dashboard",
        tags=["test"],
        timerange="1h",
        panels=[
            PanelSpec(
                title="Request Rate",
                description="HTTP request rate",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr="rate(http_requests_total[5m])",
                        legend_format="{{method}}",
                        datasource_uid="prom-1",
                        datasource_type="prometheus",
                    )
                ],
                unit="reqps",
            ),
        ],
    )
    assert len(spec.panels) == 1
    assert spec.panels[0].queries[0].datasource_uid == "prom-1"
    print("[PASS] test_dashboard_spec_model")


def test_build_dashboard_json():
    spec = DashboardSpec(
        title="Test Dashboard",
        tags=["test"],
        timerange="1h",
        panels=[
            PanelSpec(
                title="Request Rate",
                panel_type="timeseries",
                queries=[
                    PanelQuery(
                        expr="rate(http_requests_total[5m])",
                        legend_format="{{method}}",
                        datasource_uid="prom-1",
                        datasource_type="prometheus",
                    )
                ],
                unit="reqps",
            ),
            PanelSpec(
                title="Error Rate",
                panel_type="stat",
                queries=[
                    PanelQuery(
                        expr='sum(rate(http_requests_total{status=~"5.."}[5m]))',
                        legend_format="errors",
                        datasource_uid="prom-1",
                        datasource_type="prometheus",
                    )
                ],
                unit="percentunit",
            ),
        ],
    )

    result = build_dashboard_json(spec)

    assert result["title"] == "Test Dashboard"
    assert "dashforge" in result["tags"]
    assert result["time"]["from"] == "now-1h"
    assert len(result["panels"]) == 2

    # Check grid layout (2 panels side by side, 12 cols each)
    assert result["panels"][0]["gridPos"] == {"x": 0, "y": 0, "w": 12, "h": 8}
    assert result["panels"][1]["gridPos"] == {"x": 12, "y": 0, "w": 12, "h": 8}

    # Check targets
    assert result["panels"][0]["targets"][0]["refId"] == "A"
    assert result["panels"][0]["targets"][0]["datasource"]["uid"] == "prom-1"

    # Check units
    assert result["panels"][0]["fieldConfig"]["defaults"]["unit"] == "reqps"
    assert result["panels"][1]["fieldConfig"]["defaults"]["unit"] == "percentunit"

    print("[PASS] test_build_dashboard_json")


def test_build_dashboard_json_wraps_rows():
    """Three panels should wrap: two on first row, one on second."""
    panels = [
        PanelSpec(
            title=f"Panel {i}",
            panel_type="timeseries",
            queries=[
                PanelQuery(
                    expr="up",
                    datasource_uid="p1",
                    datasource_type="prometheus",
                )
            ],
        )
        for i in range(3)
    ]
    spec = DashboardSpec(title="Wrap Test", panels=panels, timerange="30m")
    result = build_dashboard_json(spec)

    assert result["panels"][0]["gridPos"]["x"] == 0
    assert result["panels"][0]["gridPos"]["y"] == 0
    assert result["panels"][1]["gridPos"]["x"] == 12
    assert result["panels"][1]["gridPos"]["y"] == 0
    assert result["panels"][2]["gridPos"]["x"] == 0
    assert result["panels"][2]["gridPos"]["y"] == 8

    assert result["time"]["from"] == "now-30m"
    print("[PASS] test_build_dashboard_json_wraps_rows")


def test_filter_datasources_by_signal():
    datasources = [
        DatasourceInfo(uid="1", name="Prom", type="prometheus"),
        DatasourceInfo(uid="2", name="Loki", type="loki"),
        DatasourceInfo(uid="3", name="Tempo", type="tempo"),
        DatasourceInfo(uid="4", name="Mimir", type="mimir"),
        DatasourceInfo(uid="5", name="CW", type="cloudwatch"),
        DatasourceInfo(uid="6", name="ES", type="elasticsearch"),
    ]

    # Metrics now includes prometheus, mimir, cloudwatch, elasticsearch
    metrics_ds = filter_datasources_by_signal(datasources, ["metrics"])
    assert {d.type for d in metrics_ds} == {"prometheus", "mimir", "cloudwatch", "elasticsearch"}

    # Logs includes loki AND elasticsearch
    logs_ds = filter_datasources_by_signal(datasources, ["logs"])
    assert {d.type for d in logs_ds} == {"loki", "elasticsearch"}

    traces_ds = filter_datasources_by_signal(datasources, ["traces"])
    assert len(traces_ds) == 1
    assert traces_ds[0].type == "tempo"

    all_ds = filter_datasources_by_signal(datasources, ["metrics", "logs", "traces"])
    assert len(all_ds) == 6

    print("[PASS] test_filter_datasources_by_signal")


def test_keyword_filter():
    metrics = [
        "http_requests_total",
        "http_request_duration_seconds",
        "process_cpu_seconds_total",
        "node_memory_MemAvailable_bytes",
        "go_goroutines",
        "up",
    ]

    result = md_keyword_filter(metrics, ["cpu", "memory"])
    assert "process_cpu_seconds_total" in result
    assert "node_memory_MemAvailable_bytes" in result
    assert "up" not in result
    assert "go_goroutines" not in result

    result2 = md_keyword_filter(metrics, ["http", "request"])
    assert "http_requests_total" in result2
    assert "http_request_duration_seconds" in result2

    print("[PASS] test_keyword_filter")


def test_dash_request_response_models():
    req = DashRequest(prompt="high CPU on web servers", channel_id="C123", user_id="U456")
    assert req.prompt == "high CPU on web servers"
    assert req.thread_ts == ""

    resp = DashResponse(
        dashboard_url="http://grafana:3000/d/abc",
        dashboard_uid="abc",
        panel_count=5,
        summary="Created dashboard",
    )
    assert resp.panel_count == 5
    print("[PASS] test_dash_request_response_models")


def test_timerange_map():
    assert TIMERANGE_MAP["1h"] == "now-1h"
    assert TIMERANGE_MAP["5m"] == "now-5m"
    assert TIMERANGE_MAP["24h"] == "now-24h"
    assert TIMERANGE_MAP["7d"] == "now-7d"
    print("[PASS] test_timerange_map")


def test_metric_entry_model():
    entry = MetricEntry(
        name="AWS/ApplicationELB/HTTPCode_ELB_5XX",
        datasource_uid="cw-1",
        datasource_name="CloudWatch",
        datasource_type="cloudwatch",
        query_language="cloudwatch",
        namespace="AWS/ApplicationELB",
        dimensions=["LoadBalancer", "TargetGroup"],
    )
    assert entry.query_language == "cloudwatch"
    assert entry.namespace == "AWS/ApplicationELB"
    assert len(entry.dimensions) == 2
    print("[PASS] test_metric_entry_model")


def test_discovered_metric_with_type():
    m = DiscoveredMetric(
        metric_name="HTTPCode_ELB_5XX",
        datasource_uid="cw-1",
        datasource_name="CloudWatch",
        datasource_type="cloudwatch",
        query_language="cloudwatch",
        namespace="AWS/ApplicationELB",
        relevance_reason="Tracks ALB 5xx errors",
    )
    assert m.datasource_type == "cloudwatch"
    assert m.query_language == "cloudwatch"
    print("[PASS] test_discovered_metric_with_type")


def test_adapter_registry():
    supported = supported_datasource_types()
    assert "prometheus" in supported
    assert "cloudwatch" in supported
    assert "loki" in supported
    assert "elasticsearch" in supported
    assert "graphite" in supported
    assert "influxdb" in supported
    assert "mimir" in supported
    assert "cortex" in supported
    assert "thanos" in supported
    assert "opensearch" in supported

    # get_adapter_for_type
    prom = get_adapter_for_type("prometheus")
    assert prom is not None
    assert prom.query_language == "promql"

    cw = get_adapter_for_type("cloudwatch")
    assert cw is not None
    assert cw.query_language == "cloudwatch"

    loki = get_adapter_for_type("loki")
    assert loki is not None
    assert loki.query_language == "logql"

    es = get_adapter_for_type("elasticsearch")
    assert es is not None
    assert es.query_language == "elasticsearch"

    assert get_adapter_for_type("unknown_type") is None

    # get_adapter with DatasourceInfo
    ds = DatasourceInfo(uid="1", name="MyProm", type="prometheus")
    adapter = get_adapter(ds)
    assert adapter is not None
    assert adapter.query_language == "promql"

    print("[PASS] test_adapter_registry")


def test_filter_searchable_datasources():
    datasources = [
        DatasourceInfo(uid="1", name="Prom", type="prometheus"),
        DatasourceInfo(uid="2", name="CW", type="cloudwatch"),
        DatasourceInfo(uid="3", name="Tempo", type="tempo"),  # no adapter
        DatasourceInfo(uid="4", name="Jaeger", type="jaeger"),  # no adapter
        DatasourceInfo(uid="5", name="Loki", type="loki"),
        DatasourceInfo(uid="6", name="Custom", type="my-custom-plugin"),  # no adapter
    ]
    searchable = filter_searchable_datasources(datasources)
    assert {d.type for d in searchable} == {"prometheus", "cloudwatch", "loki"}
    print("[PASS] test_filter_searchable_datasources")


def test_cloudwatch_namespace_selection():
    available = [
        "AWS/EC2",
        "AWS/ApplicationELB",
        "AWS/RDS",
        "AWS/Lambda",
        "AWS/SQS",
        "AWS/DynamoDB",
        "AWS/S3",
    ]

    # 5xx keyword should match ALB namespaces
    result = _select_namespaces(["5xx", "error"], available)
    assert "AWS/ApplicationELB" in result

    # database keyword
    result2 = _select_namespaces(["database"], available)
    assert "AWS/RDS" in result2 or "AWS/DynamoDB" in result2

    # cpu keyword
    result3 = _select_namespaces(["cpu"], available)
    assert "AWS/EC2" in result3

    # empty keywords should still return results (priority namespaces)
    result4 = _select_namespaces([], available)
    assert len(result4) > 0

    print("[PASS] test_cloudwatch_namespace_selection")


def test_datasource_info_with_json_data():
    ds = DatasourceInfo(
        uid="cw-1",
        name="CloudWatch",
        type="cloudwatch",
        json_data={"defaultRegion": "us-west-2"},
    )
    assert ds.json_data["defaultRegion"] == "us-west-2"
    print("[PASS] test_datasource_info_with_json_data")


def test_context_chunk_model():
    chunk = ContextChunk(
        content="When checkout errors spike, check ELB drain count and target health.",
        source="runbook:checkout-service",
        relevance_score=0.92,
        metadata={"page": 3, "section": "troubleshooting"},
    )
    assert chunk.relevance_score == 0.92
    assert chunk.source == "runbook:checkout-service"
    assert chunk.metadata["page"] == 3
    print("[PASS] test_context_chunk_model")


def test_format_context_for_prompt():
    chunks = [
        ContextChunk(
            content="Checkout service depends on payment-api and inventory-db.",
            source="wiki:checkout-architecture",
            relevance_score=0.9,
        ),
        ContextChunk(
            content="Known issue: ELB 5xx spikes during deploys. Check drain count.",
            source="runbook:checkout-deploy",
            relevance_score=0.85,
        ),
    ]
    result = format_context_for_prompt(chunks)
    assert "## Knowledge Base Context" in result
    assert "checkout-architecture" in result
    assert "ELB 5xx spikes" in result
    assert "### Context 1" in result
    assert "### Context 2" in result

    # Empty chunks should return empty string
    assert format_context_for_prompt([]) == ""
    print("[PASS] test_format_context_for_prompt")


def test_context_provider_disabled_by_default():
    # Default config has context_provider="none"
    provider = get_context_provider()
    assert provider is None
    print("[PASS] test_context_provider_disabled_by_default")


def test_enrich_context_noop_when_disabled():
    import asyncio

    intent = Intent(
        summary="High CPU",
        domain="infrastructure",
        keywords=["cpu"],
    )
    result = asyncio.run(enrich_context(intent))
    assert result == []
    print("[PASS] test_enrich_context_noop_when_disabled")


def test_prompt_sanitization():
    from dashforge.main import MAX_PROMPT_LENGTH, _sanitize_prompt

    # Normal prompt passes through
    assert _sanitize_prompt("high latency on checkout") == "high latency on checkout"

    # Truncation at MAX_PROMPT_LENGTH
    long = "a" * (MAX_PROMPT_LENGTH + 500)
    assert len(_sanitize_prompt(long)) == MAX_PROMPT_LENGTH

    # Control characters stripped
    assert _sanitize_prompt("hello\x00world\x01\x02") == "helloworld"

    # Newlines preserved
    assert _sanitize_prompt("line1\nline2") == "line1\nline2"

    # Empty / whitespace
    assert _sanitize_prompt("   ") == ""
    assert _sanitize_prompt("") == ""

    print("[PASS] test_prompt_sanitization")


def test_secrets_not_in_repr():
    from dashforge.config import Settings

    s = Settings(
        llm_api_key="sentinel-llm-secret",
        grafana_api_key="sentinel-grafana-secret",
        slack_bot_token="sentinel-slack-secret",
        context_api_key="ctx-secret",
        api_auth_key="auth-secret",
    )
    r = repr(s)
    assert "sentinel-llm-secret" not in r
    assert "sentinel-grafana-secret" not in r
    assert "sentinel-slack-secret" not in r
    assert "ctx-secret" not in r
    assert "auth-secret" not in r
    print("[PASS] test_secrets_not_in_repr")


def test_llm_error_classes():
    from dashforge.agents.llm import LLMParseError, LLMTransientError

    # LLMTransientError is retryable
    try:
        raise LLMTransientError("rate limited")
    except LLMTransientError as e:
        assert "rate limited" in str(e)

    # LLMParseError is NOT retryable
    try:
        raise LLMParseError("invalid json")
    except LLMParseError as e:
        assert "invalid json" in str(e)

    # They should NOT be caught by each other
    try:
        raise LLMParseError("bad output")
    except LLMTransientError:
        assert False, "LLMParseError should not be caught as LLMTransientError"
    except LLMParseError:
        pass

    print("[PASS] test_llm_error_classes")


def test_intent_prompt_has_security_rules():
    from dashforge.agents.intent import SYSTEM_PROMPT

    assert "SECURITY RULES" in SYSTEM_PROMPT
    assert "UNTRUSTED DATA" in SYSTEM_PROMPT
    assert "NEVER" in SYSTEM_PROMPT
    print("[PASS] test_intent_prompt_has_security_rules")


def test_metrics_discovery_prompt_has_security():
    from dashforge.agents.metrics_discovery import SYSTEM_PROMPT

    assert "SECURITY" in SYSTEM_PROMPT
    assert "never invent metric names" in SYSTEM_PROMPT.lower() or "never invent" in SYSTEM_PROMPT.lower()
    print("[PASS] test_metrics_discovery_prompt_has_security")


def test_query_builder_prompt_has_security():
    from dashforge.agents.query_builder import SYSTEM_PROMPT

    assert "SECURITY" in SYSTEM_PROMPT
    assert "Never invent UIDs" in SYSTEM_PROMPT
    print("[PASS] test_query_builder_prompt_has_security")


def test_config_concurrency_defaults():
    from dashforge.config import settings

    assert settings.pipeline_max_concurrent >= 1
    assert settings.pipeline_timeout_seconds >= 30
    assert settings.adapter_max_concurrent >= 1
    assert settings.adapter_timeout_seconds >= 10
    assert settings.max_metric_catalog_size >= 50
    assert settings.api_auth_enabled is False
    print("[PASS] test_config_concurrency_defaults")


def test_cloudwatch_panel_query_region_field():
    """PanelQuery must accept a cloudwatch_region field for CW targets."""
    q = PanelQuery(
        expr="HTTPCode_ELB_5XX",
        datasource_uid="cw-1",
        datasource_type="cloudwatch",
        cloudwatch_namespace="AWS/ApplicationELB",
        cloudwatch_stat="Sum",
        cloudwatch_region="us-west-2",
    )
    assert q.cloudwatch_region == "us-west-2"
    # Default should be empty when not supplied
    q2 = PanelQuery(expr="up", datasource_uid="p1")
    assert q2.cloudwatch_region == ""
    print("[PASS] test_cloudwatch_panel_query_region_field")


def test_cloudwatch_target_includes_region():
    """Grafana CW target JSON must contain region when cloudwatch_namespace is set."""
    panel = PanelSpec(
        title="5xx Errors",
        queries=[
            PanelQuery(
                expr="HTTPCode_ELB_5XX",
                datasource_uid="cw-1",
                datasource_type="cloudwatch",
                cloudwatch_namespace="AWS/ApplicationELB",
                cloudwatch_stat="Sum",
                cloudwatch_region="eu-west-1",
                cloudwatch_dimensions={"LoadBalancer": "*"},
            )
        ],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert target["region"] == "eu-west-1", f"Expected region 'eu-west-1', got {target.get('region')}"
    assert target["namespace"] == "AWS/ApplicationELB"
    assert target["metricName"] == "HTTPCode_ELB_5XX"
    assert target["statistics"] == ["Sum"]
    assert target["dimensions"] == {"LoadBalancer": "*"}
    print("[PASS] test_cloudwatch_target_includes_region")


def test_json_repair_path_reraises_transient_errors():
    """When the LLM repair call hits a transient error (timeout, 429),
    it must raise LLMTransientError so tenacity retries, not LLMParseError."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from pydantic import BaseModel

    from dashforge.agents.llm import LLMTransientError, call_llm

    class Simple(BaseModel):
        v: int

    from dashforge.agents.providers.base import LLMResult

    mock_provider = MagicMock()
    # Each tenacity attempt: primary returns bad JSON, repair hits timeout.
    # tenacity retries 3 times, so we need 3 × (primary + repair) = 6 calls.
    mock_provider.chat_json = AsyncMock(
        side_effect=[
            LLMResult(text="{broken json"),  # attempt 1: primary
            httpx.TimeoutException("read timed out"),  # attempt 1: repair
            LLMResult(text="{broken json"),  # attempt 2: primary
            httpx.TimeoutException("read timed out"),  # attempt 2: repair
            LLMResult(text="{broken json"),  # attempt 3: primary
            httpx.TimeoutException("read timed out"),  # attempt 3: repair
        ]
    )

    from tenacity import RetryError, wait_none

    # Patch wait to zero so test doesn't sleep
    original_wait = call_llm.retry.wait
    call_llm.retry.wait = wait_none()

    try:
        with patch("dashforge.agents.llm.get_provider", return_value=mock_provider):
            try:
                asyncio.run(call_llm("sys", "user", Simple))
                assert False, "Should have raised"
            except RetryError as re:
                # tenacity wraps the last exception — it must be LLMTransientError
                last = re.last_attempt.exception()
                assert isinstance(last, LLMTransientError), (
                    f"Expected LLMTransientError inside RetryError, " f"got {type(last).__name__}: {last}"
                )
            except LLMTransientError:
                pass  # also acceptable
            except Exception as exc:
                assert False, (
                    f"Expected LLMTransientError for transient repair failure, " f"got {type(exc).__name__}: {exc}"
                )
        # All 3 attempts should have been made (6 chat_json calls = 3 primary + 3 repair)
        assert (
            mock_provider.chat_json.call_count == 6
        ), f"Expected 6 calls (3 attempts × 2), got {mock_provider.chat_json.call_count}"
    finally:
        call_llm.retry.wait = original_wait
    print("[PASS] test_json_repair_path_reraises_transient_errors")


def test_cloudwatch_region_defaults_to_datasource_default():
    """When cloudwatch_region is empty, _build_panel_json should emit
    region='default' so Grafana uses the datasource's defaultRegion,
    rather than a potentially wrong LLM-guessed value."""
    panel = PanelSpec(
        title="5xx Errors",
        queries=[
            PanelQuery(
                expr="HTTPCode_ELB_5XX",
                datasource_uid="cw-1",
                datasource_type="cloudwatch",
                cloudwatch_namespace="AWS/ApplicationELB",
                cloudwatch_stat="Sum",
                # cloudwatch_region intentionally NOT set — should use "default"
            )
        ],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert (
        target["region"] == "default"
    ), f"Empty cloudwatch_region should map to 'default', got {target.get('region')!r}"
    print("[PASS] test_cloudwatch_region_defaults_to_datasource_default")


def test_query_builder_prompt_does_not_instruct_region_guessing():
    """The query builder prompt must NOT tell the LLM to set cloudwatch_region,
    because the metric context doesn't include region info and the LLM would guess."""
    from dashforge.agents.query_builder import SYSTEM_PROMPT

    assert "cloudwatch_region" not in SYSTEM_PROMPT, (
        "SYSTEM_PROMPT should not instruct LLM to set cloudwatch_region — "
        "region should come from the datasource defaultRegion, not LLM guessing"
    )
    print("[PASS] test_query_builder_prompt_does_not_instruct_region_guessing")


def test_prometheus_target_excludes_cloudwatch_fields():
    """Non-CloudWatch targets must NOT include region, namespace, etc."""
    panel = PanelSpec(
        title="Request Rate",
        queries=[
            PanelQuery(
                expr="rate(http_requests_total[5m])",
                datasource_uid="prom-1",
                datasource_type="prometheus",
            )
        ],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert "region" not in target
    assert "namespace" not in target
    assert "metricName" not in target
    assert "statistics" not in target
    print("[PASS] test_prometheus_target_excludes_cloudwatch_fields")


def test_cloudwatch_validation_is_skipped_for_prometheus_probe():
    """CloudWatch panels should not be probed through Prometheus api/v1/query."""
    import asyncio
    from unittest.mock import AsyncMock

    from dashforge.validation import validate_dashboard_queries

    client = type("Client", (), {})()
    client.datasource_proxy_get = AsyncMock(return_value={"data": {"result": []}})

    spec = DashboardSpec(
        title="cw",
        panels=[
            PanelSpec(
                title="ELB 5xx",
                queries=[
                    PanelQuery(
                        expr="HTTPCode_ELB_5XX",
                        datasource_uid="cw-1",
                        datasource_type="cloudwatch",
                        cloudwatch_namespace="AWS/ApplicationELB",
                    )
                ],
            )
        ],
    )

    filtered, warnings = asyncio.run(validate_dashboard_queries(client, spec))

    assert len(filtered.panels) == 1
    assert warnings == []
    client.datasource_proxy_get.assert_not_called()
    print("[PASS] test_cloudwatch_validation_is_skipped_for_prometheus_probe")


def test_prometheus_validation_still_uses_proxy_query():
    """Prometheus panels should continue using api/v1/query validation."""
    import asyncio
    from unittest.mock import AsyncMock

    from dashforge.validation import validate_dashboard_queries

    client = type("Client", (), {})()
    client.datasource_proxy_get = AsyncMock(return_value={"data": {"result": [{"metric": {}}]}})

    spec = DashboardSpec(
        title="prom",
        panels=[
            PanelSpec(
                title="Request Rate",
                queries=[
                    PanelQuery(
                        expr="rate(http_requests_total[5m])",
                        datasource_uid="prom-1",
                        datasource_type="prometheus",
                    )
                ],
            )
        ],
    )

    filtered, warnings = asyncio.run(validate_dashboard_queries(client, spec))

    assert len(filtered.panels) == 1
    assert warnings == []
    client.datasource_proxy_get.assert_called_once()
    print("[PASS] test_prometheus_validation_still_uses_proxy_query")


def test_provider_sdk_transient_error_in_repair_retried():
    """When the LLM repair call raises a provider SDK exception (e.g.
    OpenAI RateLimitError), it must be classified as LLMTransientError
    so tenacity retries, not swallowed as LLMParseError."""
    import asyncio
    from unittest.mock import AsyncMock, MagicMock, patch

    from pydantic import BaseModel
    from tenacity import RetryError, wait_none

    from dashforge.agents.llm import LLMTransientError, call_llm

    class Simple(BaseModel):
        v: int

    # Simulate a provider SDK rate-limit exception (not httpx, not ClientError)
    class RateLimitError(Exception):
        """Simulates openai.RateLimitError."""

        def __init__(self):
            super().__init__("Rate limit exceeded")
            self.status_code = 429

    from dashforge.agents.providers.base import LLMResult

    mock_provider = MagicMock()
    mock_provider.chat_json = AsyncMock(
        side_effect=[
            LLMResult(text="{broken json"),  # attempt 1: primary
            RateLimitError(),  # attempt 1: repair → transient
            LLMResult(text="{broken json"),  # attempt 2: primary
            RateLimitError(),  # attempt 2: repair → transient
            LLMResult(text="{broken json"),  # attempt 3: primary
            RateLimitError(),  # attempt 3: repair → transient
        ]
    )

    original_wait = call_llm.retry.wait
    call_llm.retry.wait = wait_none()

    try:
        with patch("dashforge.agents.llm.get_provider", return_value=mock_provider):
            try:
                asyncio.run(call_llm("sys", "user", Simple))
                assert False, "Should have raised"
            except RetryError as re:
                last = re.last_attempt.exception()
                assert isinstance(last, LLMTransientError), (
                    f"Expected LLMTransientError (retryable), " f"got {type(last).__name__}: {last}"
                )
            except LLMTransientError:
                pass  # also acceptable
            except Exception as exc:
                assert False, (
                    f"Provider SDK rate-limit error should be LLMTransientError, " f"got {type(exc).__name__}: {exc}"
                )
        assert (
            mock_provider.chat_json.call_count == 6
        ), f"Expected 6 calls (3 attempts × 2), got {mock_provider.chat_json.call_count}"
    finally:
        call_llm.retry.wait = original_wait
    print("[PASS] test_provider_sdk_transient_error_in_repair_retried")


def test_cloudwatch_metric_name_strips_namespace_prefix():
    """When expr contains the catalog-style 'Namespace/MetricName', _build_panel_json
    must strip the namespace prefix so metricName is just 'MetricName'."""
    panel = PanelSpec(
        title="5xx Errors",
        queries=[
            PanelQuery(
                expr="AWS/ApplicationELB/HTTPCode_ELB_5XX",
                datasource_uid="cw-1",
                datasource_type="cloudwatch",
                cloudwatch_namespace="AWS/ApplicationELB",
                cloudwatch_stat="Sum",
            )
        ],
    )
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    target = result["targets"][0]
    assert (
        target["metricName"] == "HTTPCode_ELB_5XX"
    ), f"Expected stripped metricName 'HTTPCode_ELB_5XX', got {target['metricName']!r}"
    # When expr is already bare, should pass through unchanged
    panel2 = PanelSpec(
        title="5xx Errors",
        queries=[
            PanelQuery(
                expr="HTTPCode_ELB_5XX",
                datasource_uid="cw-1",
                datasource_type="cloudwatch",
                cloudwatch_namespace="AWS/ApplicationELB",
                cloudwatch_stat="Sum",
            )
        ],
    )
    result2 = _build_panel_json(panel2, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    assert result2["targets"][0]["metricName"] == "HTTPCode_ELB_5XX"
    print("[PASS] test_cloudwatch_metric_name_strips_namespace_prefix")


def test_cloudwatch_dimensions_accept_str_and_list():
    """CloudWatch dimensions accept both str and list[str] values.
    Single-element lists should be normalized to strings by _build_panel_json.
    Multi-value lists should pass through."""
    q = PanelQuery(
        expr="HTTPCode_ELB_5XX",
        datasource_uid="cw-1",
        datasource_type="cloudwatch",
        cloudwatch_namespace="AWS/ApplicationELB",
        cloudwatch_stat="Sum",
        cloudwatch_dimensions={
            "LoadBalancer": "*",
            "AvailabilityZone": ["us-east-1a", "us-east-1b"],
        },
    )
    assert q.cloudwatch_dimensions["LoadBalancer"] == "*"
    assert q.cloudwatch_dimensions["AvailabilityZone"] == ["us-east-1a", "us-east-1b"]

    # Verify _build_panel_json passes through correctly
    panel = PanelSpec(title="Test", queries=[q])
    result = _build_panel_json(panel, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    dims = result["targets"][0]["dimensions"]
    assert dims["LoadBalancer"] == "*", "String values should pass through"
    assert dims["AvailabilityZone"] == ["us-east-1a", "us-east-1b"], "Multi-value lists should pass through"

    # Single-element lists should be normalized to strings
    q2 = PanelQuery(
        expr="HTTPCode_ELB_5XX",
        datasource_uid="cw-1",
        datasource_type="cloudwatch",
        cloudwatch_namespace="AWS/ApplicationELB",
        cloudwatch_stat="Sum",
        cloudwatch_dimensions={"LoadBalancer": ["*"]},
    )
    panel2 = PanelSpec(title="Test", queries=[q2])
    result2 = _build_panel_json(panel2, 1, {"x": 0, "y": 0, "w": 12, "h": 8})
    assert (
        result2["targets"][0]["dimensions"]["LoadBalancer"] == "*"
    ), "Single-element list ['*'] should be normalized to '*'"
    print("[PASS] test_cloudwatch_dimensions_accept_str_and_list")


def test_signalfx_discovery_normalized_keywords_match_cache():
    """SignalFx discovery must use normalized keywords for both the cache key
    and the API search, so whitespace-containing keywords don't poison the cache."""
    from dashforge.signalfx.discovery import _normalize_keywords

    raw = [" CPU ", "High", "cpu"]
    norm = _normalize_keywords(raw)
    assert norm == ["cpu", "high"], f"Expected ['cpu', 'high'], got {norm}"

    # Verify the same keywords in different order/case produce same result
    raw2 = ["high", "CPU"]
    norm2 = _normalize_keywords(raw2)
    assert norm == norm2, f"Expected same normalization, got {norm} vs {norm2}"
    print("[PASS] test_signalfx_discovery_normalized_keywords_match_cache")


def test_strip_trailing_commas_preserves_string_content():
    """Trailing-comma repair must not mutate commas inside JSON string values."""
    from dashforge.agents.llm import _strip_trailing_commas

    # Comma inside a string value followed by } — must NOT be stripped
    raw = '{"desc": "Check errors,}", "v": 1,}'
    fixed = _strip_trailing_commas(raw)
    assert fixed == '{"desc": "Check errors,}", "v": 1}', f"Got: {fixed!r}"

    # Comma inside string followed by ] — must NOT be stripped
    raw2 = '{"arr": ["a,]", "b",]}'
    fixed2 = _strip_trailing_commas(raw2)
    assert fixed2 == '{"arr": ["a,]", "b"]}', f"Got: {fixed2!r}"

    # Escaped quotes inside strings must not confuse the parser
    raw3 = '{"q": "rate(x{\\"svc\\":\\"a\\"},})[5m]", "n": 1,}'
    fixed3 = _strip_trailing_commas(raw3)
    assert '"n": 1}' in fixed3, f"Trailing comma not removed: {fixed3!r}"
    assert '\\"a\\"},})' in fixed3, f"String content mutated: {fixed3!r}"

    # Plain trailing comma (no string involvement) — should still be removed
    raw4 = '{"a": 1, "b": 2,}'
    assert _strip_trailing_commas(raw4) == '{"a": 1, "b": 2}'

    # No trailing comma — no change
    raw5 = '{"a": 1}'
    assert _strip_trailing_commas(raw5) == raw5

    print("[PASS] test_strip_trailing_commas_preserves_string_content")


if __name__ == "__main__":
    test_intent_model()
    test_dashboard_spec_model()
    test_build_dashboard_json()
    test_build_dashboard_json_wraps_rows()
    test_filter_datasources_by_signal()
    test_keyword_filter()
    test_dash_request_response_models()
    test_timerange_map()
    test_metric_entry_model()
    test_discovered_metric_with_type()
    test_adapter_registry()
    test_filter_searchable_datasources()
    test_cloudwatch_namespace_selection()
    test_datasource_info_with_json_data()
    test_context_chunk_model()
    test_format_context_for_prompt()
    test_context_provider_disabled_by_default()
    test_enrich_context_noop_when_disabled()
    test_prompt_sanitization()
    test_secrets_not_in_repr()
    test_llm_error_classes()
    test_intent_prompt_has_security_rules()
    test_metrics_discovery_prompt_has_security()
    test_query_builder_prompt_has_security()
    test_config_concurrency_defaults()
    test_cloudwatch_panel_query_region_field()
    test_cloudwatch_target_includes_region()
    test_prometheus_target_excludes_cloudwatch_fields()
    test_json_repair_path_reraises_transient_errors()
    test_cloudwatch_region_defaults_to_datasource_default()
    test_query_builder_prompt_does_not_instruct_region_guessing()
    test_cloudwatch_validation_is_skipped_for_prometheus_probe()
    test_prometheus_validation_still_uses_proxy_query()
    test_provider_sdk_transient_error_in_repair_retried()
    test_cloudwatch_metric_name_strips_namespace_prefix()
    test_cloudwatch_dimensions_accept_str_and_list()
    test_signalfx_discovery_normalized_keywords_match_cache()
    test_strip_trailing_commas_preserves_string_content()
    print("\n=== All tests passed ===")
