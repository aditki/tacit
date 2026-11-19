"""Cached LLM candidates stay separate from run-specific source authority."""

from types import SimpleNamespace

import pytest

import tacit.pipeline.stages.freeform as freeform
from tacit.agents.providers.base import TokenUsage
from tacit.cache import TTLCache, make_cache_key
from tacit.config import Settings
from tacit.models.schemas import DashboardSpec, Intent, MetricEntry, MetricsDiscoveryResult


def metric(name="requests", uid="owner", **updates):
    return MetricEntry(
        name=name,
        datasource_uid=uid,
        datasource_name=uid,
        datasource_type="prometheus",
        query_language="promql",
    ).model_copy(update=updates)


class DiscoveryCache(TTLCache):
    def __init__(self):
        super().__init__()
        self.writes = []

    def set(self, key, value, ttl=None):
        self.writes.append((key, value))
        super().set(key, value, ttl=ttl)


class FreeformHarness:
    def __init__(self, monkeypatch):
        self.cache = DiscoveryCache()
        self.discovery = MetricsDiscoveryResult.model_validate(
            {
                "metrics": [
                    {
                        "metric_name": "requests",
                        "datasource_uid": "owner",
                        "datasource_name": "owner",
                        "relevance_reason": "request signal",
                    }
                ]
            }
        )
        self.original = self.discovery.model_dump()
        self.discovery_calls = []
        self.build_calls = []
        self.failures = []
        self.mutate_builder = False

        async def discover(intent, catalog, context_chunks, **kwargs):
            self.discovery_calls.append([entry.model_dump() for entry in catalog])
            return self.discovery, TokenUsage(total_tokens=11)

        async def build(intent, discovery, catalog, **kwargs):
            self.build_calls.append(discovery.model_dump())
            if self.mutate_builder:
                discovery.metrics[0].datasource_uid = "builder-mutated-owner"
                discovery.metrics[0].relevance_reason = "builder-mutated-reason"
                discovery.metrics.append(discovery.metrics[0].model_copy(update={"metric_name": "unselected"}))
            return DashboardSpec(title="requests", panels=[]), TokenUsage(total_tokens=7)

        monkeypatch.setattr(freeform, "discover_metrics", discover)
        monkeypatch.setattr(freeform, "build_dashboard", build)
        # The prompt projection is deliberately stable while the actual catalog
        # can gain or lose an additional owner outside its one visible entry.
        monkeypatch.setattr(freeform, "prerank_metrics", lambda intent, catalog, **kwargs: catalog[:1])
        self.deps = SimpleNamespace(
            settings=Settings(_env_file=None, max_metric_catalog_size=1),
            feedback_store_factory=lambda: None,
            llm_provider_factory=lambda: object(),
            llm_cache=self.cache,
            cache_key_factory=make_cache_key,
        )

    async def run(self, catalog, tenant="tenant-a"):
        return await freeform.build_freeform_dashboard(
            intent=Intent(summary="Inspect request activity", domain="application"),
            metric_catalog=catalog,
            context_chunks=[],
            deps=self.deps,
            recorder=SimpleNamespace(
                finish=lambda **kwargs: self.failures.append(kwargs), investigation_id="test", run_id="run"
            ),
            timings={},
            started_at=0,
            tenant_id=tenant,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("already_cached", [False, True])
async def test_ambiguous_run_cannot_poison_later_unique_owner_with_same_prompt_projection(monkeypatch, already_cached):
    harness = FreeformHarness(monkeypatch)
    unique = [metric()]
    if already_cached:
        prime = await harness.run(unique)
        assert prime.dashboard_spec is not None and prime.token_usage.total_tokens == 18

    rejected = await harness.run([*unique, metric(uid="additional-owner")])
    assert rejected.dashboard_spec is None
    assert rejected.token_usage.total_tokens == (0 if already_cached else 11)
    assert len(harness.build_calls) == int(already_cached)

    recovered = await harness.run(unique)

    assert recovered.dashboard_spec is not None
    assert recovered.token_usage.total_tokens == 7
    assert len(harness.discovery_calls) == 1
    assert len(harness.build_calls) == 1 + int(already_cached)
    assert len(harness.cache.writes) == 1
    assert harness.cache.stats == {"hits": 1 + int(already_cached), "misses": 1, "size": 1}
    assert harness.cache.get(harness.cache.writes[0][0]).model_dump() == harness.original
    assert harness.discovery.model_dump() == harness.original


@pytest.mark.asyncio
@pytest.mark.parametrize("already_cached", [False, True])
async def test_query_builder_mutations_do_not_change_cached_or_provider_candidates(monkeypatch, already_cached):
    harness = FreeformHarness(monkeypatch)
    if already_cached:
        await harness.run([metric()])
    harness.mutate_builder = True

    result = await harness.run([metric()])

    assert result.dashboard_spec is not None
    assert result.token_usage.total_tokens == (7 if already_cached else 18)
    assert len(harness.discovery_calls) == 1
    assert harness.discovery.model_dump() == harness.original
    cached = harness.cache.get(harness.cache.writes[0][0])
    assert cached.model_dump() == harness.original
    harness.mutate_builder = False
    assert (await harness.run([metric()])).dashboard_spec is not None
    assert harness.build_calls[-1] == harness.original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "catalog",
    [
        [metric("other")],
        [metric(uid="different-owner")],
        [metric(unit="seconds"), metric(unit="bytes")],
    ],
    ids=["missing-name", "wrong-proposed-owner", "metadata-conflict"],
)
async def test_rejected_owner_has_no_builder_effect_and_does_not_mutate_cached_candidate(monkeypatch, catalog):
    harness = FreeformHarness(monkeypatch)

    result = await harness.run(catalog)

    assert result.dashboard_spec is None
    assert not harness.build_calls
    assert len(harness.failures) == 1
    assert harness.failures[0]["status"] == "failed"
    assert harness.discovery.model_dump() == harness.original
    assert harness.cache.get(harness.cache.writes[0][0]).model_dump() == harness.original


@pytest.mark.asyncio
async def test_run_copies_keep_nested_candidates_independent_for_separate_tenant_keys(monkeypatch):
    harness = FreeformHarness(monkeypatch)
    harness.mutate_builder = True

    first = await harness.run([metric()], tenant="tenant-a")
    second = await harness.run([metric()], tenant="tenant-b")

    assert first.dashboard_spec is not None and second.dashboard_spec is not None
    assert first.token_usage.total_tokens == second.token_usage.total_tokens == 18
    assert len(harness.discovery_calls) == len(harness.build_calls) == harness.cache.size == 2
    assert len({key for key, _ in harness.cache.writes}) == 2
    assert all(value.model_dump() == harness.original for _, value in harness.cache.writes)


@pytest.mark.asyncio
async def test_empty_discovery_is_not_cached_or_sent_to_builder(monkeypatch):
    harness = FreeformHarness(monkeypatch)
    harness.discovery.metrics.clear()

    result = await harness.run([metric()])

    assert result.dashboard_spec is None
    assert len(harness.discovery_calls) == 1 and not harness.build_calls
    assert not harness.cache.writes and harness.cache.size == 0
