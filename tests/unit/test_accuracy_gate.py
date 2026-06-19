import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

from dashforge.archetypes.engine import blend_archetypes, rank_archetypes_by_coverage
from dashforge.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from dashforge.cache import metric_cache
from dashforge.catalog import catalog_for_services
from dashforge.config import settings
from dashforge.grafana.adapters.prometheus import PrometheusAdapter
from dashforge.models.schemas import (
    ArchetypeMatch,
    DashboardSpec,
    DatasourceInfo,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)
from dashforge.pipeline import _discovery_keywords
from dashforge.signals import SignalStore, _unit_compatibility
from dashforge.validation import validate_dashboard_queries
from tests.eval.gate_harness import gate_failures


def _metric(name: str, uid: str = "real") -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid=uid,
        datasource_name=uid,
        datasource_type="prometheus",
        query_language="promql",
    )


def _dashboard(*queries: PanelQuery) -> DashboardSpec:
    return DashboardSpec(title="gate", panels=[PanelSpec(title="Evidence", queries=list(queries))])


def _query(expr: str, uid: str = "real") -> PanelQuery:
    return PanelQuery(expr=expr, datasource_uid=uid, datasource_type="prometheus", query_language="promql")


def test_clickstack_prompt_corpus_has_required_size_and_classes():
    path = Path(__file__).parents[1] / "eval" / "fixtures" / "clickstack_prompts.json"
    fixture = json.loads(path.read_text())

    assert 25 <= len(fixture["prompts"]) <= 40
    assert {item["class"] for item in fixture["prompts"]} == {
        "precise",
        "vague",
        "noisy",
        "misleading",
        "reworded",
    }


@pytest.mark.asyncio
async def test_validation_requires_metrics_to_exist_in_routed_datasource():
    client = AsyncMock()
    catalog = [_metric("only_in_a", "a"), _metric("shared_name", "b")]

    filtered, warnings = await validate_dashboard_queries(
        client,
        _dashboard(_query("shared_name", "a")),
        catalog,
        catalog_authoritative=True,
    )

    assert filtered.panels == []
    assert any("metric not in catalog" in warning for warning in warnings)
    client.datasource_proxy_get.assert_not_awaited()


@pytest.mark.asyncio
async def test_validation_rejects_query_when_any_referenced_metric_is_absent():
    client = AsyncMock()
    catalog = [_metric("real_metric")]

    filtered, warnings = await validate_dashboard_queries(
        client,
        _dashboard(_query("real_metric + invented_metric")),
        catalog,
        catalog_authoritative=True,
    )

    assert filtered.panels == []
    assert any("metric not in catalog" in warning for warning in warnings)
    client.datasource_proxy_get.assert_not_awaited()


@pytest.mark.asyncio
async def test_validation_drops_only_bad_query_from_mixed_panel():
    client = AsyncMock()
    client.datasource_proxy_get.return_value = {"status": "success", "data": {"result": [{"metric": {}}]}}
    catalog = [_metric("real_metric")]

    filtered, warnings = await validate_dashboard_queries(
        client,
        _dashboard(_query("real_metric"), _query("invented_metric")),
        catalog,
        catalog_authoritative=True,
    )

    assert [query.expr for query in filtered.panels[0].queries] == ["real_metric"]
    assert any("metric not in catalog" in warning for warning in warnings)
    client.datasource_proxy_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_validation_probes_target_only_catalog_instead_of_marking_absent():
    client = AsyncMock()
    client.datasource_proxy_get.return_value = {"status": "success", "data": {"result": [{"metric": {}}]}}
    target = _metric("", "target-only")

    filtered, warnings = await validate_dashboard_queries(
        client,
        _dashboard(_query("metric_not_enumerated", "target-only")),
        [target],
    )

    assert len(filtered.panels) == 1
    assert not any("metric not in catalog" in warning for warning in warnings)
    client.datasource_proxy_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_validation_probes_metric_missing_from_partial_catalog():
    client = AsyncMock()
    client.datasource_proxy_get.return_value = {"status": "success", "data": {"result": [{"metric": {}}]}}
    partial_catalog = [_metric("catalog_was_capped_before_this_metric")]

    filtered, warnings = await validate_dashboard_queries(
        client,
        _dashboard(_query("real_metric_omitted_by_cap")),
        partial_catalog,
    )

    assert len(filtered.panels) == 1
    assert not any("metric not in catalog" in warning for warning in warnings)
    client.datasource_proxy_get.assert_awaited_once()


@pytest.mark.asyncio
async def test_validation_does_not_parse_cloudwatch_metric_as_promql():
    client = AsyncMock()
    catalog = [
        MetricEntry(
            name="AWS/ApplicationELB/HTTPCode_ELB_5XX",
            datasource_uid="cloudwatch",
            datasource_name="CloudWatch",
            datasource_type="cloudwatch",
            query_language="cloudwatch",
        )
    ]
    query = PanelQuery(
        expr="HTTPCode_ELB_5XX",
        datasource_uid="cloudwatch",
        datasource_type="cloudwatch",
        query_language="",
        cloudwatch_namespace="AWS/ApplicationELB",
    )

    filtered, warnings = await validate_dashboard_queries(client, _dashboard(query), catalog)

    assert len(filtered.panels) == 1
    assert not any("metric not in catalog" in warning for warning in warnings)
    client.datasource_proxy_get.assert_not_awaited()


@pytest.mark.asyncio
async def test_validation_skips_non_prometheus_grafana_queries():
    client = AsyncMock()
    query = PanelQuery(
        expr='{service_name="checkout"} |= "error"',
        datasource_uid="loki",
        datasource_type="",
        query_language="logql",
    )
    catalog = [
        MetricEntry(
            name="logs",
            datasource_uid="loki",
            datasource_name="Loki",
            datasource_type="loki",
            query_language="logql",
        )
    ]

    filtered, warnings = await validate_dashboard_queries(client, _dashboard(query), catalog)

    assert len(filtered.panels) == 1
    assert not warnings
    client.datasource_proxy_get.assert_not_awaited()


@pytest.mark.asyncio
async def test_prometheus_http_422_is_reported_as_syntax_error():
    client = AsyncMock()
    request = httpx.Request("GET", "https://grafana.test/query")
    response = httpx.Response(
        422,
        request=request,
        json={"status": "error", "errorType": "bad_data", "error": "parse error"},
    )
    client.datasource_proxy_get.side_effect = httpx.HTTPStatusError(
        "unprocessable entity",
        request=request,
        response=response,
    )

    filtered, warnings = await validate_dashboard_queries(
        client,
        _dashboard(_query("real_metric")),
        [_metric("real_metric")],
    )

    assert filtered.panels == []
    assert any("invalid syntax" in warning for warning in warnings)


def test_prometheus_metadata_lookup_handles_exported_suffixes():
    metadata = {
        "request_duration_seconds": ("seconds", "histogram"),
        "transactions_total": ("", "counter"),
    }

    assert PrometheusAdapter._metadata_for("request_duration_seconds_bucket", metadata) == (
        "seconds",
        "histogram",
    )
    assert PrometheusAdapter._metadata_for("transactions_total", metadata) == ("", "counter")


@pytest.mark.asyncio
async def test_prometheus_metadata_is_loaded_from_datasource_api():
    metric_cache.invalidate()
    client = AsyncMock()
    client.datasource_proxy_get.return_value = {
        "status": "success",
        "data": {"request_duration_seconds": [{"unit": "seconds", "type": "histogram"}]},
    }
    datasource = DatasourceInfo(uid="metadata-test", name="Metadata", type="prometheus")

    try:
        metadata = await PrometheusAdapter()._get_metric_metadata(client, datasource)
    finally:
        metric_cache.invalidate()

    assert metadata == {"request_duration_seconds": ("seconds", "histogram")}


def test_unit_compatibility_rewards_matches_and_penalizes_conflicts():
    assert _unit_compatibility("seconds", "ms") > 1.0
    assert _unit_compatibility("seconds", "bytes") < 1.0
    assert _unit_compatibility("seconds", "") == 1.0


def _archetype(archetype_id: str, metric_name: str, panel_count: int = 1) -> InvestigationArchetype:
    return InvestigationArchetype(
        id=archetype_id,
        name=archetype_id,
        description="",
        problem_types=[archetype_id],
        required_signals=[f"{archetype_id}_signal"],
        signal_bindings={f"{archetype_id}_signal": metric_name},
        panels=[
            PanelTemplate(title=f"{archetype_id}-{index}", queries=[QueryTemplate(expr=metric_name)])
            for index in range(panel_count)
        ],
    )


def test_archetype_ranking_prefers_live_coverage_over_raw_confidence():
    uncovered = _archetype("uncovered", "missing_metric")
    covered = _archetype("covered", "real_metric")

    ranked = rank_archetypes_by_coverage(
        [(uncovered, 0.99), (covered, 0.7)],
        [_metric("real_metric")],
        max_archetypes=1,
    )

    assert [archetype.id for archetype, _ in ranked] == ["covered"]


def test_archetype_ranking_includes_required_metrics_without_signals():
    uncovered = InvestigationArchetype(
        id="uncovered-required-metrics",
        name="uncovered",
        problem_types=["uncovered"],
        required_metrics=["missing_metric"],
        panels=[PanelTemplate(title="Missing", queries=[QueryTemplate(expr="missing_metric")])],
    )
    covered = InvestigationArchetype(
        id="covered-required-metrics",
        name="covered",
        problem_types=["covered"],
        required_metrics=["real_metric"],
        panels=[PanelTemplate(title="Real", queries=[QueryTemplate(expr="real_metric")])],
    )

    ranked = rank_archetypes_by_coverage(
        [(uncovered, 0.99), (covered, 0.60)],
        [_metric("real_metric")],
        max_archetypes=1,
    )

    assert ranked[0][0].id == "covered-required-metrics"


@pytest.mark.parametrize("suffix", ["_bucket", "_sum", "_count"])
def test_archetype_coverage_treats_histogram_series_as_base_metric(suffix):
    latency = InvestigationArchetype(
        id="latency",
        name="latency",
        problem_types=["latency"],
        required_metrics=["http_request_duration_seconds"],
        panels=[PanelTemplate(title="Latency", queries=[QueryTemplate(expr="latency")])],
    )
    unrelated = InvestigationArchetype(
        id="unrelated",
        name="unrelated",
        problem_types=["general"],
        required_metrics=["other_metric"],
        panels=[PanelTemplate(title="Other", queries=[QueryTemplate(expr="other_metric")])],
    )

    ranked = rank_archetypes_by_coverage(
        [(unrelated, 0.99), (latency, 0.8)],
        [_metric(f"http_request_duration_seconds{suffix}")],
        max_archetypes=1,
    )

    assert ranked[0][0].id == "latency"


def test_colloquial_evidence_broadens_discovery_without_mutating_intent():
    intent = Intent(
        summary="the in-memory tier is squeezed",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=["saturation"],
        timerange="1h",
        problem_type="general",
        archetypes=[],
        keyword_evidence=[{"keyword": "cache", "score": 0.4, "tier": "colloquial", "source": "in-memory tier"}],
    )

    assert _discovery_keywords(intent) == ["saturation", "cache"]
    assert intent.keywords == ["saturation"]


def test_colloquial_confirmation_catalog_is_scoped_to_requested_service(tmp_path):
    checkout_metric = _metric("http_requests_total")
    checkout_metric.dimensions = ["service={checkout}"]
    payment_cache = _metric("redis_keys_evicted")
    payment_cache.dimensions = ["service={payment}"]

    scoped = catalog_for_services([checkout_metric, payment_cache], ["checkout-service"])

    assert scoped == [checkout_metric]
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.register_signal_type("cache_evictions", category="caching")
    store.add_mapping("cache_evictions", "*keys_evicted*", confidence=0.9)
    assert not store.resolve_signal(
        "cache_evictions",
        scoped,
        context_service="checkout-service",
        target_query_language="promql",
    )


def test_archetype_ranking_prefers_strong_learned_match(monkeypatch):
    monkeypatch.setattr(settings, "learned_archetype_min_coverage", 0.75)
    monkeypatch.setattr(settings, "learned_archetype_boost", 0.15)
    learned = _archetype("learned_specific", "real_metric")
    learned.tags = ["learned"]
    generic = _archetype("generic", "real_metric")

    ranked = rank_archetypes_by_coverage(
        [(generic, 0.80), (learned, 0.70)],
        [_metric("real_metric")],
        max_archetypes=2,
    )

    assert ranked[0][0].id == "learned_specific"


def test_archetype_ranking_boosts_ingestion_generated_match(monkeypatch):
    monkeypatch.setattr(settings, "learned_archetype_min_coverage", 0.75)
    monkeypatch.setattr(settings, "learned_archetype_boost", 0.15)
    generated = _archetype("generated_specific", "real_metric")
    generated.tags = ["auto-generated"]
    generic = _archetype("generic", "real_metric")

    ranked = rank_archetypes_by_coverage(
        [(generic, 0.80), (generated, 0.70)],
        [_metric("real_metric")],
        max_archetypes=2,
    )

    assert ranked[0][0].id == "generated_specific"


def test_archetype_ranking_does_not_boost_weak_learned_match(monkeypatch):
    monkeypatch.setattr(settings, "learned_archetype_min_coverage", 0.75)
    monkeypatch.setattr(settings, "learned_archetype_boost", 0.15)
    learned = _archetype("learned_weak", "missing_metric")
    learned.tags = ["learned"]
    generic = _archetype("generic", "real_metric")

    ranked = rank_archetypes_by_coverage(
        [(learned, 0.99), (generic, 0.60)],
        [_metric("real_metric")],
        max_archetypes=2,
    )

    assert ranked[0][0].id == "generic"


def test_signal_resolution_uses_type_labels_and_otel_scope_to_rank(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.register_signal_type("request_latency", category="latency", unit="s")
    store.add_mapping("request_latency", "*request_duration*", confidence=0.8)
    weak = MetricEntry(
        name="worker_request_duration_seconds",
        datasource_uid="prom",
        datasource_name="prom",
        datasource_type="prometheus",
        query_language="promql",
        unit="s",
        metric_type="gauge",
    )
    otel = MetricEntry(
        name="http_server_request_duration_seconds",
        datasource_uid="prom",
        datasource_name="prom",
        datasource_type="prometheus",
        query_language="promql",
        namespace="otel.instrumentation.scope=http.server",
        dimensions=["http.request.method", "http.response.status_code", "service.name"],
        unit="s",
        metric_type="histogram",
    )

    hits = store.resolve_signal("request_latency", [weak, otel], target_query_language="promql")

    assert [entry.name for entry, _ in hits] == [otel.name, weak.name]
    assert hits[0][1] > hits[1][1]


def test_blending_enforces_archetype_and_panel_caps(monkeypatch):
    monkeypatch.setattr(settings, "max_blended_archetypes", 2)
    monkeypatch.setattr(settings, "max_dashboard_panels", 3)
    monkeypatch.setattr(settings, "min_secondary_coverage", 0.0)
    first = _archetype("first", "first_metric", panel_count=2)
    second = _archetype("second", "second_metric", panel_count=2)
    third = _archetype("third", "third_metric", panel_count=2)
    intent = Intent(
        summary="bounded dashboard",
        domain="application",
        services=[],
        signals=[SignalType.METRICS],
        keywords=[],
        timerange="1h",
        problem_type="first",
        archetypes=[ArchetypeMatch(type="first", confidence=1.0)],
    )

    dashboard = blend_archetypes(
        [(first, 0.9), (second, 0.8), (third, 0.7)],
        intent,
        [_metric("first_metric"), _metric("second_metric"), _metric("third_metric")],
    )

    assert len(dashboard.panels) == 3
    assert all("third" not in panel.title for panel in dashboard.panels)


def test_offline_gate_reports_semantic_and_selection_regressions():
    report = {
        "classification": [{"dataset": "regressed", "precision": 0.89, "recall": 0.79, "coverage": 0.79}],
        "cold_resolution": [{"dataset": "regressed", "recall": 0.74}],
        "learned_resolution": [{"dataset": "regressed", "recall": 0.89}],
        "learned_selection": [{"dataset": "regressed", "selected": "generic", "expected": "learned", "passed": False}],
    }

    failures = gate_failures(report)

    assert len(failures) == 6
