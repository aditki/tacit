"""The frozen 588-metric regression through the owned public pipeline."""

from __future__ import annotations

import json

import pytest
import yaml

import tacit.archetypes.templates as templates
import tacit.pipeline as pipeline_mod
from tacit.agents.providers import registry as provider_registry
from tacit.agents.providers.base import LLMProvider, TokenUsage
from tacit.backends.base import MeasuredValidationResult
from tacit.catalog import CatalogSourceStatus, MetricCatalogResult, acquisition_entry_id
from tacit.config import settings
from tacit.dashboard_ingest import extract_metrics_from_promql
from tacit.measurements import normalize_prometheus_measurement
from tacit.models.schemas import ArchetypeMatch, DashRequest, Intent, MetricEntry
from tacit.ranking import MAX_LLM_CANDIDATES, prerank_metrics
from tests.e2e.framework import CapturingBackend
from tests.eval import acquisition_parity


class NoModelProvider(LLMProvider):
    async def chat_json(self, *args, **kwargs):
        raise AssertionError("The deterministic fixture must not call a model")

    async def chat_text(self, *args, **kwargs):
        raise AssertionError("The deterministic fixture must not call a model")


class TypedParityBackend(CapturingBackend):
    """Transport substitute with explicit typed facts and query-result oracle."""

    def __init__(self, catalog, expected_queries):
        super().__init__(catalog=catalog)
        self.expected_queries = expected_queries
        self.typed_calls = 0
        self.validation_calls = 0
        self.work_budget = None

    async def discover_metrics(self, keywords, intent):
        raise AssertionError("The owned proxy must select the typed capability once")

    async def discover_metrics_with_metadata(self, keywords, intent, work_budget):
        del intent
        self.typed_calls += 1
        self.work_budget = work_budget
        # This hermetic transport substitutes one finite response. Production
        # raw-read/closure behavior has its separate adapter/transport gate.
        work_budget.reserve_acquisition_call()
        work_budget.reserve_acquisition_bytes(len(json.dumps([entry.model_dump() for entry in self.catalog])))
        groups = {}
        for entry in self.catalog:
            work_budget.reserve_acquisition_entry(acquisition_entry_id(entry))
            groups.setdefault(entry.datasource_uid, []).append(entry)
        return MetricCatalogResult(
            entries=self.catalog,
            sources=[
                CatalogSourceStatus(
                    backend=self.name,
                    datasource_uid=uid,
                    datasource_type="prometheus",
                    query_language="promql",
                    terms=tuple(sorted(set(keywords))),
                    returned_count=len(entries),
                    retained_count=len(entries),
                    total_before_cap=len(entries),
                    disposition="complete",
                    reasons=("authoritative_terminal",),
                    retained_entry_ids=tuple(acquisition_entry_id(entry) for entry in entries),
                )
                for uid, entries in sorted(groups.items())
            ],
        )

    async def validate_queries(self, spec, catalog=None):
        self.validation_calls += 1
        assert catalog is not None and len(catalog) == 588
        validated = spec.model_copy(deep=True)
        for panel in validated.panels:
            for query in panel.queries:
                # Only the exact known fixture expressions receive the finite,
                # nonempty query response. EvidenceObservation is then produced
                # by the real evidence lifecycle rather than built by this test.
                assert (query.datasource_uid, query.expr) in self.expected_queries
                assert set(extract_metrics_from_promql(query.expr)) <= {
                    entry.name for entry in catalog if entry.datasource_uid == query.datasource_uid
                }
                query.validation_status = "ok"
                query.validation_has_data = True
        return validated, []


class MeasuredParityBackend(TypedParityBackend):
    async def validate_queries_with_measurements(self, spec, catalog, work_budget):
        assert work_budget is self.work_budget
        validated, warnings = await self.validate_queries(spec, catalog)
        measurements = tuple(
            normalize_prometheus_measurement(
                query,
                {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [
                            {
                                "metric": {"service": "payments", "secret_id": "excluded-value"},
                                "value": [1700000000, "0.25"],
                            }
                        ],
                    },
                },
                work_budget,
            )
            for panel in validated.panels
            for query in panel.queries
        )
        return MeasuredValidationResult(validated, warnings, measurements)


@pytest.mark.e2e
@pytest.mark.parametrize("measured", [False, True])
async def test_typed_588_catalog_preserves_six_requests_and_seven_fixture_metrics(
    isolated_learning_runtime, monkeypatch, measured
):
    _signals, history, _feedback, archetypes_path, _quarantine = isolated_learning_runtime
    fixture = acquisition_parity.load_fixture()
    catalog = acquisition_parity.catalog(fixture)
    requested = sorted(
        {name for names in fixture["categories"].values() for name in names} | set(fixture["fixture_critical"])
    )
    assert len(catalog) == 588
    assert all(entry.name not in requested for entry in catalog[:300])
    for index, entry in enumerate(catalog):
        # Irrelevant default datasource hints cannot route the actual requested
        # operands. Preserve the frozen total and beyond-300 metric placement.
        if index < 120:
            entry.datasource_uid = "irrelevant-default"
            entry.datasource_name = "Irrelevant default source"
            entry.datasource_is_default = True
        else:
            entry.dimensions = ["service={payments}"]

    query_templates = {
        name: (
            f"sum(rate({name}" + "{{{service_filter}}}[{rate_interval}]))"
            if name.endswith("_total")
            else name + "{{{service_filter}}}"
        )
        for name in requested
    }
    # The request's hit ratio retains its complete same-owner operand set.
    hit_ratio = (
        "sum(rate(redis_keyspace_hits_total{{{service_filter}}}[{rate_interval}])) / "
        "(sum(rate(redis_keyspace_hits_total{{{service_filter}}}[{rate_interval}])) + "
        "sum(rate(redis_keyspace_misses_total{{{service_filter}}}[{rate_interval}])))"
    )
    query_templates["Redis hit ratio"] = hit_ratio
    archetype_id = "frozen_acquisition_parity"
    archetypes_path.write_text(
        yaml.safe_dump(
            {
                "archetypes": [
                    {
                        "id": archetype_id,
                        "name": "Frozen acquisition parity",
                        "problem_types": [archetype_id],
                        "required_metrics": requested,
                        "panels": [
                            {"title": name, "queries": [{"expr": expression}]}
                            for name, expression in query_templates.items()
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    templates.reload_archetypes()
    intent = Intent(
        summary="Inspect Visa cache, payment transaction rate, Redis memory, hit ratio, evictions and clients",
        domain="application",
        services=["payments"],
        keywords=["visa", "payment", "redis", "cache", "evictions", "clients", "duration"],
        problem_type=archetype_id,
        archetypes=[ArchetypeMatch(type=archetype_id, confidence=1.0)],
    )
    expected_queries = {
        (
            fixture["datasource_uid"],
            expression.format(service_filter='service="payments"', container_filter="", rate_interval="5m"),
        )
        for expression in query_templates.values()
    }
    backend = (MeasuredParityBackend if measured else TypedParityBackend)(catalog, expected_queries)

    async def classify(_prompt):
        return intent.model_copy(deep=True), TokenUsage()

    async def no_context(_intent):
        return []

    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "classify_intent", classify)
    monkeypatch.setattr(pipeline_mod, "enrich_context", no_context)
    monkeypatch.setattr(
        provider_registry, "create_provider", lambda runtime_settings: NoModelProvider(runtime_settings)
    )
    monkeypatch.setattr(settings, "max_metric_catalog_size", fixture["legacy_projection_count"])

    response = await pipeline_mod.run_pipeline(DashRequest(prompt=intent.summary, user_id="e2e-acquisition"))
    assert response.dashboard_uid
    assert backend.typed_calls == backend.validation_calls == len(backend.published_specs) == 1
    assert backend.work_budget.acquisition_entries == 588
    published_queries = {
        (query.datasource_uid, query.expr) for panel in backend.published_specs[0].panels for query in panel.queries
    }
    assert published_queries == expected_queries
    contract = history.get_contract(response.investigation_id)
    snapshot = history.get_snapshot(response.investigation_id)
    assert contract is not None and snapshot is not None
    if measured:
        assert len(snapshot.query_measurements) == len(published_queries)
        assert all(query.execution["measurement"]["samples"][0]["value"] == 0.25 for query in contract.queries)
        assert "excluded-value" not in snapshot.model_dump_json()
        assert "excluded-value" not in contract.model_dump_json()
        query_events = [
            event
            for event in history.list_events(response.investigation_id)
            if event["event_type"] in {"query_validated", "query_executed"}
        ]
        assert query_events
        assert all('"samples"' not in json.dumps(event["payload"]) for event in query_events)
    capture = snapshot.model_inputs["catalog_acquisition_v1"]
    retained = [MetricEntry.model_validate(item["metric"]) for item in capture["entries"]]
    assert len(retained) == 588
    assert capture["counters"]["entries_retained"] == 588
    assert len(capture["sources"]) == 2
    assert all(source["disposition"] == "complete" for source in capture["sources"])
    assert all(
        selection["target"]["datasource_uid"] == fixture["datasource_uid"] for selection in capture["selections"]
    )

    # Retention and the existing LLM projection remain different boundaries.
    prompt_catalog = prerank_metrics(intent, retained)
    assert settings.max_metric_catalog_size == 300
    assert len(prompt_catalog) == MAX_LLM_CANDIDATES == 60
    assert set(requested) <= {entry.name for entry in prompt_catalog}

    unsafe = [candidate for candidate in contract.candidate_rankings if candidate.causal_status == "proven"]
    assert contract.grounding.maximum_trustworthy_conclusion["causal_status"] != "proven"
    report = acquisition_parity.score(fixture, retained, snapshot.evidence_observations, unsafe)
    assert report["passed"], report
    assert (report["available_categories"], report["observed_categories"], report["category_denominator"]) == (6, 6, 6)
    assert (report["fixture_critical_observed"], report["fixture_critical_denominator"]) == (7, 7)
    assert report["routing_errors"] == report["unsafe_assertions"] == 0
    replayed = history.replay_contract(response.investigation_id)
    assert replayed is not None
    assert replayed.runtime.output_fingerprint == contract.runtime.output_fingerprint
