import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from structlog.testing import capture_logs

from tacit.archetypes.engine import (
    ArchetypeCoverageWorkLimitError,
    _panel_signature,
    blend_archetypes,
    compile_archetype,
    rank_archetypes_by_coverage,
)
from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.cache import metric_cache
from tacit.catalog import catalog_for_services
from tacit.config import Settings, settings
from tacit.errors import RuntimeOwnershipError
from tacit.grafana.adapters.prometheus import PrometheusAdapter
from tacit.knowledge.usage import KnowledgeRevisionRef, KnowledgeUsageEffect, KnowledgeUsageStage
from tacit.models.schemas import (
    ArchetypeMatch,
    DashboardSpec,
    DatasourceInfo,
    Intent,
    MetricEntry,
    PanelQuery,
    PanelSpec,
    SignalType,
)
from tacit.pipeline import _discovery_keywords
from tacit.pipeline.discovery import (
    MAX_COLLOQUIAL_CATALOG_ENTRIES,
    ColloquialConfirmationWorkLimitError,
    ConfirmedKeywords,
    confirm_colloquial_keywords,
)
from tacit.pipeline.stages.archetypes import (
    MAX_DISCOVERY_ATTRIBUTION_REVISIONS,
    ArchetypeCompilation,
    ArchetypeSelectionWorkLimits,
    DiscoveryAttributionWorkLimitError,
    select_archetypes,
)
from tacit.signals import SignalStore, _unit_compatibility
from tacit.signals.resolution import (
    ResolutionInputTextLimits,
    SignalResolutionWorkBudget,
    SignalResolutionWorkLimitError,
)
from tacit.validation import validate_dashboard_queries
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
async def test_validation_probes_prometheus_datasource_despite_language_mismatch():
    client = AsyncMock()
    client.datasource_proxy_get.return_value = {
        "status": "error",
        "errorType": "bad_data",
        "error": "parse error",
    }
    query = PanelQuery(
        expr='{service_name="checkout"} |= "error"',
        datasource_uid="prom",
        datasource_type="prometheus",
        query_language="logql",
    )
    catalog = [_metric("http_requests_total", "prom")]

    filtered, warnings = await validate_dashboard_queries(client, _dashboard(query), catalog)

    assert filtered.panels == []
    assert any("invalid syntax" in warning for warning in warnings)
    client.datasource_proxy_get.assert_awaited_once()


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


@pytest.mark.asyncio
async def test_prometheus_metric_cache_isolated_by_backend_identity():
    metric_cache.invalidate()
    datasource = DatasourceInfo(uid="shared-uid", name="Shared", type="prometheus")
    first = AsyncMock()
    first.cache_namespace = "runtime-a"
    first.datasource_proxy_get.return_value = {
        "status": "success",
        "data": {"tenant_a_metric": [{"unit": "seconds", "type": "gauge"}]},
    }
    second = AsyncMock()
    second.cache_namespace = "runtime-b"
    second.datasource_proxy_get.return_value = {
        "status": "success",
        "data": {"tenant_b_metric": [{"unit": "bytes", "type": "gauge"}]},
    }

    try:
        first_metadata = await PrometheusAdapter()._get_metric_metadata(first, datasource)
        second_metadata = await PrometheusAdapter()._get_metric_metadata(second, datasource)
    finally:
        metric_cache.invalidate()

    assert first_metadata == {"tenant_a_metric": ("seconds", "gauge")}
    assert second_metadata == {"tenant_b_metric": ("bytes", "gauge")}
    first.datasource_proxy_get.assert_awaited_once()
    second.datasource_proxy_get.assert_awaited_once()


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


def test_archetype_selection_attributes_the_exact_governed_mapping_revision(tmp_path):
    uncovered = _archetype("uncovered", "missing_uncovered_metric")
    archetype = _archetype("governed", "missing_default_metric")
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.add_mapping(
        "governed_signal",
        "live_metric",
        confidence=0.9,
        source_type="operational_knowledge",
        source_refs=["knowledge-governed@4"],
        governance_ref="knowledge-governed",
        governance_revision=4,
        review_state="approved",
    )
    stage_uses = []

    ranked = rank_archetypes_by_coverage(
        [(uncovered, 0.99), (archetype, 0.8)],
        [_metric("live_metric")],
        max_archetypes=1,
        signal_store=store,
        knowledge_stage_uses=stage_uses,
    )

    assert [item.id for item, _ in ranked] == ["governed"]
    assert len(stage_uses) == 1
    assert stage_uses[0].revision_ref.knowledge_ref == "knowledge-governed"
    assert stage_uses[0].revision_ref.knowledge_revision == 4
    assert stage_uses[0].stage == KnowledgeUsageStage.ARCHETYPE_SELECTION
    assert stage_uses[0].effect == KnowledgeUsageEffect.ARCHETYPE_SELECTED_BY_LIVE_COVERAGE


def test_archetype_selection_does_not_attribute_a_mapping_that_cannot_change_selection(tmp_path):
    archetype = _archetype("governed", "missing_default_metric")
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.add_mapping(
        "governed_signal",
        "live_metric",
        confidence=0.9,
        source_type="operational_knowledge",
        source_refs=["knowledge-governed@4"],
        governance_ref="knowledge-governed",
        governance_revision=4,
        review_state="approved",
    )
    stage_uses = []

    ranked = rank_archetypes_by_coverage(
        [(archetype, 0.8)],
        [_metric("live_metric")],
        max_archetypes=1,
        signal_store=store,
        knowledge_stage_uses=stage_uses,
    )

    assert [item.id for item, _ in ranked] == ["governed"]
    assert stage_uses == []


def test_archetype_selection_attributes_only_targets_changed_by_each_revision(tmp_path):
    stable = InvestigationArchetype(
        id="stable",
        name="stable",
        problem_types=["stable"],
        required_signals=["shared_signal"],
        required_metrics=["stable_base_metric"],
        panels=[PanelTemplate(title="stable", queries=[QueryTemplate(expr="stable_base_metric")])],
    )
    dependent = InvestigationArchetype(
        id="dependent",
        name="dependent",
        problem_types=["dependent"],
        required_signals=["shared_signal"],
        panels=[PanelTemplate(title="dependent", queries=[QueryTemplate(expr="missing_default")])],
    )
    fallback = InvestigationArchetype(
        id="fallback",
        name="fallback",
        problem_types=["fallback"],
        required_metrics=["fallback_metric"],
        panels=[PanelTemplate(title="fallback", queries=[QueryTemplate(expr="fallback_metric")])],
    )
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.add_mapping(
        "shared_signal",
        "live_shared_metric",
        confidence=0.9,
        source_type="operational_knowledge",
        governance_ref="knowledge-shared",
        governance_revision=3,
        review_state="approved",
    )
    stage_uses = []

    ranked = rank_archetypes_by_coverage(
        [(stable, 0.99), (dependent, 0.8), (fallback, 0.45)],
        [_metric("live_shared_metric"), _metric("stable_base_metric"), _metric("fallback_metric")],
        max_archetypes=2,
        signal_store=store,
        knowledge_stage_uses=stage_uses,
    )

    assert [item.id for item, _ in ranked] == ["stable", "dependent"]
    assert [use.target_ref for use in stage_uses] == ["archetype:dependent"]


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


def test_archetype_coverage_treats_required_metric_as_literal():
    dotted = InvestigationArchetype(
        id="dotted",
        name="dotted",
        problem_types=["dotted"],
        required_metrics=["cpu.utilization"],
        panels=[PanelTemplate(title="CPU", queries=[QueryTemplate(expr="cpu.utilization")])],
        tags=["auto-generated"],
    )
    covered = InvestigationArchetype(
        id="covered",
        name="covered",
        problem_types=["covered"],
        required_metrics=["real_metric"],
        panels=[PanelTemplate(title="Real", queries=[QueryTemplate(expr="real_metric")])],
    )

    ranked = rank_archetypes_by_coverage(
        [(dotted, 0.99), (covered, 0.60)],
        [_metric("cpuXutilization"), _metric("real_metric")],
        max_archetypes=1,
    )

    assert ranked[0][0].id == "covered"


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


def test_governed_colloquial_confirmation_attributes_archetype_routing(monkeypatch, tmp_path):
    routed = _archetype("cache-context", "live_cache_metric")
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
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.add_mapping(
        "cache_hits",
        "live_cache_metric",
        confidence=0.9,
        source_type="operational_knowledge",
        governance_ref="knowledge-cache",
        governance_revision=7,
        review_state="approved",
    )
    confirmed = confirm_colloquial_keywords(
        intent,
        [_metric("live_cache_metric")],
        "promql",
        store,
    )

    monkeypatch.setattr("tacit.pipeline.stages.archetypes.get_archetypes_by_confidence", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        lambda candidate_intent, *_args, **_kwargs: [(routed, 0.8)] if "cache" in candidate_intent.keywords else [],
    )
    monkeypatch.setattr("tacit.pipeline.stages.archetypes.get_archetype", lambda *_args, **_kwargs: None)

    selection = select_archetypes(
        intent=intent,
        metric_catalog=[_metric("live_cache_metric")],
        catalog_for_compile=[_metric("live_cache_metric")],
        target_language="promql",
        settings=Settings(_env_file=None),
        signal_store=store,
        confirmed_keywords=confirmed,
    )

    assert confirmed == ["cache"]
    assert [archetype.id for archetype, _ in selection.ranked_archetypes] == ["cache-context"]
    assert [(use.knowledge_ref, use.knowledge_revision, use.target_ref) for use in selection.knowledge_stage_uses] == [
        ("knowledge-cache", 7, "archetype:cache-context")
    ]


def test_colloquial_confirmation_propagates_authority_failures(monkeypatch):
    intent = Intent(
        summary="the in-memory tier is squeezed",
        domain="infrastructure",
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )

    class DeniedStore:
        def resolve_signal_details(self, *_args, **_kwargs):
            raise RuntimeOwnershipError("owner mismatch")

    with pytest.raises(RuntimeOwnershipError, match="owner mismatch"):
        confirm_colloquial_keywords(
            intent,
            [_metric("live_cache_metric")],
            "promql",
            DeniedStore(),
        )


def test_colloquial_confirmation_degrades_ordinary_resolution_failure_without_payload():
    intent = Intent(
        summary="the in-memory tier is squeezed",
        domain="infrastructure",
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )

    class UnavailableStore:
        def resolve_signal_details(self, *_args, **_kwargs):
            raise OSError("private-path-canary")

    with capture_logs() as logs:
        confirmed = confirm_colloquial_keywords(
            intent,
            [_metric("live_cache_metric")],
            "promql",
            UnavailableStore(),
        )

    assert confirmed == []
    assert confirmed.degraded is True
    assert "private-path-canary" not in json.dumps(logs)


def test_colloquial_confirmation_bounds_catalog_before_service_matching_or_resolution(monkeypatch):
    intent = Intent(
        summary="the in-memory tier is squeezed",
        domain="infrastructure",
        services=["checkout"],
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )
    service_match_calls = 0
    resolution_calls = 0

    def service_match(*_args, **_kwargs):
        nonlocal service_match_calls
        service_match_calls += 1
        return True

    class Store:
        def resolve_signal_details(self, *_args, **_kwargs):
            nonlocal resolution_calls
            resolution_calls += 1
            return []

    monkeypatch.setattr("tacit.catalog.metric_matches_services", service_match)

    with pytest.raises(ColloquialConfirmationWorkLimitError) as exc_info:
        confirm_colloquial_keywords(
            intent,
            [_metric("live_cache_metric")] * (MAX_COLLOQUIAL_CATALOG_ENTRIES + 1),
            "promql",
            Store(),
        )

    assert exc_info.value.dimension == "catalog_entries"
    assert service_match_calls == 0
    assert resolution_calls == 0


def test_colloquial_confirmation_rejects_bypass_scalar_before_resolution() -> None:
    intent = Intent.model_construct(
        summary="x" * 65,
        domain="infrastructure",
        services=[],
        environments=[],
        signals=[],
        keywords=[],
        timerange="1h",
        problem_type="general",
        archetypes=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )

    class Store:
        calls = 0

        def resolve_signal_details(self, *_args, **_kwargs):
            self.calls += 1
            return []

    store = Store()
    with pytest.raises(ColloquialConfirmationWorkLimitError) as exc_info:
        confirm_colloquial_keywords(
            intent,
            [_metric("live_cache_metric")],
            "promql",
            store,
            input_text_limits=ResolutionInputTextLimits(
                max_scalar_characters=64,
            ),
        )

    assert exc_info.value.dimension == "scalar_characters"
    assert store.calls == 0


def test_colloquial_confirmation_rejects_aggregate_characters_before_resolution() -> None:
    intent = Intent(
        summary="cache cache cache",
        domain="infrastructure",
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )

    class Store:
        calls = 0

        def resolve_signal_details(self, *_args, **_kwargs):
            self.calls += 1
            return []

    store = Store()
    with pytest.raises(ColloquialConfirmationWorkLimitError) as exc_info:
        confirm_colloquial_keywords(
            intent,
            [_metric("live_cache_metric")],
            "promql",
            store,
            input_text_limits=ResolutionInputTextLimits(
                max_total_characters=16,
            ),
        )

    assert exc_info.value.dimension == "total_input_characters"
    assert store.calls == 0


def test_colloquial_confirmation_rejects_aggregate_utf8_bytes_before_resolution() -> None:
    intent = Intent(
        summary="cache",
        domain="infrastructure",
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )

    class Store:
        calls = 0

        def resolve_signal_details(self, *_args, **_kwargs):
            self.calls += 1
            return []

    store = Store()
    with pytest.raises(ColloquialConfirmationWorkLimitError) as exc_info:
        confirm_colloquial_keywords(
            intent,
            [_metric("live_cache_metric")],
            "promql",
            store,
            input_text_limits=ResolutionInputTextLimits(
                max_total_utf8_bytes=1,
            ),
        )

    assert exc_info.value.dimension == "total_input_utf8_bytes"
    assert store.calls == 0


def test_colloquial_attribution_rechecks_exact_revision_with_fallback(monkeypatch, tmp_path):
    routed = _archetype("cache-context", "live_cache_metric")
    intent = Intent(
        summary="the in-memory tier is squeezed",
        domain="infrastructure",
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )
    store = SignalStore(db_path=tmp_path / "signals.db")
    for confidence, knowledge_ref in ((0.9, "primary-cache"), (0.8, "fallback-cache")):
        store.add_mapping(
            "cache_hits",
            "live_cache_metric",
            confidence=confidence,
            source_type="operational_knowledge",
            governance_ref=knowledge_ref,
            governance_revision=1,
            review_state="approved",
        )
    confirmed = confirm_colloquial_keywords(
        intent,
        [_metric("live_cache_metric")],
        "promql",
        store,
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        lambda candidate_intent, *_args, **_kwargs: [(routed, 0.8)] if "cache" in candidate_intent.keywords else [],
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetype",
        lambda *_args, **_kwargs: None,
    )

    selection = select_archetypes(
        intent=intent,
        metric_catalog=[_metric("live_cache_metric")],
        catalog_for_compile=[_metric("live_cache_metric")],
        target_language="promql",
        settings=Settings(_env_file=None),
        signal_store=store,
        confirmed_keywords=confirmed,
    )

    assert [item.id for item, _ in selection.ranked_archetypes] == ["cache-context"]
    assert selection.knowledge_stage_uses == ()


def test_colloquial_reconfirmation_shares_selection_resolution_budget(monkeypatch, tmp_path):
    routed = _archetype("cache-context", "live_cache_metric")
    intent = Intent(
        summary="the in-memory tier is squeezed",
        domain="infrastructure",
        keywords=[],
        keyword_evidence=[
            {
                "keyword": "cache",
                "score": 0.4,
                "tier": "colloquial",
                "source": "in-memory tier",
            }
        ],
    )
    store = SignalStore(db_path=tmp_path / "signals.db")
    for confidence, knowledge_ref in ((0.9, "primary-cache"), (0.8, "fallback-cache")):
        store.add_mapping(
            "cache_hits",
            "live_cache_metric",
            confidence=confidence,
            source_type="operational_knowledge",
            governance_ref=knowledge_ref,
            governance_revision=1,
            review_state="approved",
        )
    budget = SignalResolutionWorkBudget(
        max_calls=8,
        max_mapping_catalog_comparisons=2,
        max_results=8,
    )
    confirmed = confirm_colloquial_keywords(
        intent,
        [_metric("live_cache_metric")],
        "promql",
        store,
        resolution_work_budget=budget,
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        lambda candidate_intent, *_args, **_kwargs: [(routed, 0.8)] if "cache" in candidate_intent.keywords else [],
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetype",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(SignalResolutionWorkLimitError) as exc_info:
        select_archetypes(
            intent=intent,
            metric_catalog=[_metric("live_cache_metric")],
            catalog_for_compile=[_metric("live_cache_metric")],
            target_language="promql",
            settings=Settings(_env_file=None),
            signal_store=store,
            confirmed_keywords=confirmed,
        )

    assert exc_info.value.dimension == "mapping_catalog_comparisons"
    assert budget.calls >= 2
    assert budget.mapping_catalog_comparisons == 2


def test_discovery_attribution_revision_fanout_is_bounded_before_reranking(monkeypatch):
    intent = Intent(summary="cache", domain="infrastructure", keywords=["cache"])
    refs = {KnowledgeRevisionRef(f"knowledge-{index}", 1) for index in range(MAX_DISCOVERY_ATTRIBUTION_REVISIONS + 1)}
    confirmed = ConfirmedKeywords(
        ["cache"],
        revision_refs_by_keyword={"cache": refs},
        added_keywords=["cache"],
    )
    rank_calls = 0

    def rank(*_args, **_kwargs):
        nonlocal rank_calls
        rank_calls += 1
        return []

    monkeypatch.setattr("tacit.pipeline.stages.archetypes.rank_archetypes_by_coverage", rank)
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr("tacit.pipeline.stages.archetypes.get_archetype", lambda *_args: None)

    with pytest.raises(DiscoveryAttributionWorkLimitError, match="revision_count"):
        select_archetypes(
            intent=intent,
            metric_catalog=[_metric("live_cache_metric")],
            catalog_for_compile=[_metric("live_cache_metric")],
            target_language="promql",
            settings=Settings(_env_file=None),
            signal_store=object(),
            confirmed_keywords=confirmed,
        )

    assert rank_calls == 0


def test_archetype_selection_rejects_oversized_intent_before_retrieval(monkeypatch):
    intent = Intent.model_construct(
        summary="x" * 65_537,
        domain="infrastructure",
        services=[],
        environments=[],
        signals=[],
        keywords=[],
        timerange="1h",
        problem_type="general",
        archetypes=[],
        keyword_evidence=[],
    )
    retrieval_calls = 0

    def retrieve(*_args, **_kwargs):
        nonlocal retrieval_calls
        retrieval_calls += 1
        return []

    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        retrieve,
    )

    with pytest.raises(ArchetypeCoverageWorkLimitError) as exc_info:
        select_archetypes(
            intent=intent,
            metric_catalog=[],
            catalog_for_compile=[],
            target_language="promql",
            settings=Settings(_env_file=None),
            signal_store=object(),
        )

    assert exc_info.value.dimension == "scalar_characters"
    assert retrieval_calls == 0


def test_discovery_attribution_bounds_registry_multiplied_retrieval_before_first_lookup(monkeypatch):
    intent = Intent(
        summary="cache",
        domain="infrastructure",
        keywords=["cache"],
    )
    refs = {KnowledgeRevisionRef("knowledge-cache", 1)}
    confirmed = ConfirmedKeywords(
        ["cache"],
        revision_refs_by_keyword={"cache": refs},
        added_keywords=["cache"],
    )
    retrieval_calls = 0

    def retrieve(*_args, **_kwargs):
        nonlocal retrieval_calls
        retrieval_calls += 1
        return []

    monkeypatch.setattr("tacit.pipeline.stages.archetypes.curated_archetype_count", lambda: 3)
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_confidence",
        retrieve,
    )
    monkeypatch.setattr(
        "tacit.pipeline.stages.archetypes.get_archetypes_by_learning_context",
        retrieve,
    )

    with pytest.raises(DiscoveryAttributionWorkLimitError) as exc_info:
        select_archetypes(
            intent=intent,
            metric_catalog=[_metric("live_cache_metric")],
            catalog_for_compile=[_metric("live_cache_metric")],
            target_language="promql",
            settings=Settings(_env_file=None),
            signal_store=object(),
            confirmed_keywords=confirmed,
            work_limits=ArchetypeSelectionWorkLimits(max_attribution_retrieval_work=1),
        )

    assert exc_info.value.dimension == "attribution_retrieval_work"
    assert retrieval_calls == 0


def test_compilation_attribution_follows_the_substituted_template_query(tmp_path):
    archetype = InvestigationArchetype(
        id="latency",
        name="Latency",
        problem_types=["latency"],
        required_signals=["request_latency"],
        signal_bindings={"request_latency": "missing_default_metric"},
        panels=[
            PanelTemplate(title="Substituted", queries=[QueryTemplate(expr="missing_default_metric")]),
            PanelTemplate(title="Unrelated", queries=[QueryTemplate(expr="live_latency_metric")]),
        ],
    )
    intent = Intent(summary="latency", domain="application", problem_type="latency")
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.add_mapping(
        "request_latency",
        "live_latency_metric",
        confidence=0.9,
        source_type="operational_knowledge",
        governance_ref="knowledge-latency",
        governance_revision=5,
        review_state="approved",
    )
    query_uses = []

    dashboard = compile_archetype(
        archetype,
        intent,
        [_metric("live_latency_metric")],
        signal_store=store,
        knowledge_query_uses=query_uses,
    )

    assert [(use.panel_title, use.knowledge_ref, use.knowledge_revision) for use in query_uses] == [
        ("Substituted", "knowledge-latency", 5)
    ]
    compilation = ArchetypeCompilation(
        dashboard_spec=dashboard,
        primary_archetype=archetype,
        primary_confidence=1.0,
        knowledge_query_uses=tuple(query_uses),
    )
    dashboard_without_substitution = dashboard.model_copy(update={"panels": [dashboard.panels[1]]})
    assert compilation.surviving_knowledge_revision_refs(dashboard_without_substitution) == frozenset()


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


def test_archetype_coverage_resolves_signals_only_for_requested_service(monkeypatch, tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")
    store.register_signal_type("cache_evictions", category="caching")
    store.add_mapping("cache_evictions", "*cache_evictions*", confidence=0.9)
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)

    learned_cache = InvestigationArchetype(
        id="learned-cache",
        name="learned cache",
        problem_types=["cache"],
        required_signals=["cache_evictions"],
        signal_bindings={"cache_evictions": "missing_default"},
        panels=[PanelTemplate(title="Cache", queries=[QueryTemplate(expr="missing_default")])],
        tags=["learned"],
    )
    checkout = InvestigationArchetype(
        id="checkout",
        name="checkout",
        problem_types=["latency"],
        required_metrics=["checkout_requests_total"],
        panels=[PanelTemplate(title="Requests", queries=[QueryTemplate(expr="checkout_requests_total")])],
    )
    checkout_metric = _metric("checkout_requests_total")
    checkout_metric.dimensions = ["service={checkout}"]
    payment_cache = _metric("payment_cache_evictions_total")
    payment_cache.dimensions = ["service={payment}"]

    ranked = rank_archetypes_by_coverage(
        [(learned_cache, 0.99), (checkout, 0.60)],
        [checkout_metric, payment_cache],
        services=["checkout"],
        max_archetypes=1,
    )

    assert ranked[0][0].id == "checkout"


def test_archetype_coverage_uses_native_query_language(monkeypatch):
    monkeypatch.setattr(settings, "learned_archetype_min_coverage", 0.75)
    monkeypatch.setattr(settings, "learned_archetype_boost", 0.15)
    cloudwatch = InvestigationArchetype(
        id="learned-cloudwatch",
        name="learned cloudwatch",
        problem_types=["elb-errors"],
        required_metrics=["HTTPCode_ELB_5XX"],
        panels=[
            PanelTemplate(
                title="ELB errors",
                queries=[
                    QueryTemplate(
                        expr="HTTPCode_ELB_5XX",
                        query_language="cloudwatch",
                        datasource_type="cloudwatch",
                    )
                ],
            )
        ],
        tags=["learned"],
    )
    generic = _archetype("generic", "prometheus_metric")
    catalog = [
        _metric("prometheus_metric"),
        MetricEntry(
            name="HTTPCode_ELB_5XX",
            datasource_uid="cloudwatch",
            datasource_name="CloudWatch",
            datasource_type="cloudwatch",
            query_language="cloudwatch",
        ),
    ]

    ranked = rank_archetypes_by_coverage(
        [(generic, 0.80), (cloudwatch, 0.70)],
        catalog,
        target_language="promql",
        max_archetypes=2,
    )

    assert ranked[0][0].id == "learned-cloudwatch"


def test_archetype_coverage_keeps_metrics_without_service_metadata(monkeypatch):
    monkeypatch.setattr(settings, "learned_archetype_min_coverage", 0.75)
    monkeypatch.setattr(settings, "learned_archetype_boost", 0.15)
    learned = _archetype("learned-unscoped", "unscoped_metric")
    learned.tags = ["learned"]
    generic = _archetype("generic", "generic_metric")

    ranked = rank_archetypes_by_coverage(
        [(generic, 0.80), (learned, 0.70)],
        [_metric("generic_metric"), _metric("unscoped_metric")],
        services=["checkout"],
        max_archetypes=2,
    )

    assert ranked[0][0].id == "learned-unscoped"


def test_panel_signature_preserves_datasource_identity():
    first = PanelSpec(title="Requests A", queries=[_query("rate(requests_total[5m])", "prom-a")])
    second = PanelSpec(title="Requests B", queries=[_query("rate(requests_total[5m])", "prom-b")])

    assert _panel_signature(first) != _panel_signature(second)


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


def test_blending_enforces_archetype_and_panel_caps():
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
        max_archetypes=2,
        max_dashboard_panels=3,
        min_secondary_coverage=0.0,
    )

    assert len(dashboard.panels) == 3
    assert all("third" not in panel.title for panel in dashboard.panels)


def test_offline_gate_reports_semantic_and_selection_regressions():
    report = {
        "classification": [
            {
                "dataset": "regressed",
                "precision": 0.89,
                "recall": 0.79,
                "coverage": 0.79,
                "labeled_signal_metrics": 1,
                "tn": 1,
                "fp": 0,
            }
        ],
        "cold_resolution": [{"dataset": "regressed", "recall": 0.74, "total": 1}],
        "learned_resolution": [{"dataset": "regressed", "recall": 0.89, "total": 1}],
        "learned_selection": [{"dataset": "regressed", "selected": "generic", "expected": "learned", "passed": False}],
    }

    failures = gate_failures(report)

    assert len(failures) == 6
