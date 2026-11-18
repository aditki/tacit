from __future__ import annotations

from pathlib import Path

import pytest

import tacit.archetypes.templates as templates
import tacit.pipeline as pipeline_mod
from tacit.agents.providers import registry as provider_registry
from tacit.agents.providers.base import TokenUsage
from tacit.config import settings
from tacit.dependencies import build_pipeline_dependencies, declare_backend_factory
from tacit.errors import FatalPipelineError
from tacit.knowledge.enums import KnowledgeUsageDisposition
from tacit.knowledge.models import KnowledgeSnapshot, KnowledgeUsage
from tacit.knowledge.service import KnowledgeService
from tacit.main import app
from tacit.models.schemas import ArchetypeMatch, CulpritCandidate, DashRequest, Intent, MetricEntry, SignalType
from tacit.runtime_ownership import declare_runtime_factory
from tacit.runtime_stores import RuntimeStores
from tests.e2e.framework import (
    CapturingBackend,
    IncidentFixtureProvider,
    build_grafana_dashboard,
    evaluate_incident,
    incident_cases,
    intent_from_prompt,
    load_scenario,
    scenario_catalog,
)
from tests.http_client import TestClient

SCENARIO_PATH = Path(__file__).parent / "scenarios" / "checkout_upload_incident.yaml"
TAUGHT_LATENCY_ARCHETYPE = """
archetypes:
  - id: taught_latency
    name: Taught Latency
    description: Uses manually taught latency mappings
    problem_types: [latency_investigation]
    required_metrics:
      - http_request_duration_seconds
    required_signals:
      - request_latency
    signal_bindings:
      request_latency: http_request_duration_seconds
    tags: [manual-teach, latency]
    default_timerange: 1h
    panels:
      - title: Taught p95 latency
        row: Latency
        unit: s
        queries:
          - expr: >
              histogram_quantile(
                0.95,
                sum(rate(http_request_duration_seconds_bucket{{{service_filter}}}[{rate_interval}])) by (le)
              )
            legend_format: p95
"""

CATALOG_SCOPE_ARCHETYPE = """
archetypes:
  - id: catalog_scope_latency
    name: Catalog Scope Latency
    description: Curated latency path discovered only from live catalog overlap
    problem_types: [catalog_scope_only]
    required_metrics: [catalog_scope_latency_seconds]
    tags: [catalog-scope, latency]
    default_timerange: 1h
    panels:
      - title: Catalog latency
        row: Latency
        unit: s
        queries:
          - expr: 'catalog_scope_latency_seconds{{{service_filter}}}'
            legend_format: latency
"""


async def _no_context(_intent):
    return []


def _install_taught_latency_archetype(archetypes_path: Path) -> None:
    archetypes_path.write_text(TAUGHT_LATENCY_ARCHETYPE, encoding="utf-8")
    templates.reload_archetypes()


def _teach_taught_latency_mapping() -> None:
    teach = TestClient(app).post(
        "/api/v1/signals/teach",
        json={
            "signal_type": "request_latency",
            "metric_patterns": [{"pattern": "acme_checkout_latency_seconds", "confidence": 0.94}],
            "category": "latency",
            "datasource_types": ["prometheus"],
            "taught_by": "e2e",
        },
    )
    assert teach.status_code == 200, teach.text
    assert teach.json()["mappings_created"] == 1


def _configure_taught_latency_pipeline(monkeypatch) -> CapturingBackend:
    backend = CapturingBackend(
        catalog=[
            MetricEntry(
                name="acme_checkout_latency_seconds_bucket",
                datasource_uid="prom-e2e",
                datasource_name="Prometheus E2E",
                datasource_type="prometheus",
                query_language="promql",
                dimensions=['service="checkout-api"', "le={0.1,0.5,1,5}"],
            )
        ]
    )
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["checkout-api"],
                signals=[SignalType.METRICS],
                keywords=["checkout", "latency", "p95"],
                timerange="1h",
                problem_type="latency_investigation",
                archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.97)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)
    return backend


@pytest.mark.e2e
async def test_uploaded_dashboard_teaches_signals_and_prompt_matrix_scores_useful(
    isolated_learning_runtime,
    monkeypatch,
):
    signal_store, history_store, _feedback_store, _archetypes_path, quarantine_path = isolated_learning_runtime
    scenario = load_scenario(SCENARIO_PATH)
    dashboard_json = build_grafana_dashboard(scenario)
    client = TestClient(app)

    upload = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "grafana",
            "source_name": "checkout-edge-incident.json",
            "auto_approve": False,
            "dashboard": dashboard_json,
        },
    )
    assert upload.status_code == 200, upload.text
    uploaded = upload.json()
    assert uploaded["status"] == "pending"
    assert uploaded["dashboard_uid"] == scenario["dashboard"]["uid"]
    assert set(uploaded["metrics_found"]) >= {
        "checkout_edge_requests_total",
        "checkout_edge_errors_total",
        "checkout_edge_request_duration_seconds_bucket",
    }

    inferred_signals = uploaded["signals_inferred"]
    inferred_metrics = {sig["metric"] for sig in inferred_signals}
    inferred_families = {sig["signal_family"] for sig in inferred_signals}
    assert {
        "checkout_edge_requests_total",
        "checkout_edge_errors_total",
        "checkout_edge_request_duration_seconds_bucket",
        "checkout_edge_inflight_requests",
        "checkout_edge_cpu_usage_seconds_total",
        "checkout_edge_memory_working_set_bytes",
    } <= inferred_metrics
    assert {"errors", "latency", "saturation"} <= inferred_families
    assert inferred_families & {"traffic", "throughput"}

    approve = client.post(f"/api/v1/learn/dashboards/{scenario['dashboard']['uid']}/approve?backend=grafana_json")
    assert approve.status_code == 200, approve.text
    approved = approve.json()
    assert approved["status"] == "approved"
    assert approved["mappings_created"] == 0
    assert approved["archetype_registered"] is False
    assert approved["archetype_quarantined"] is True
    assert len(list(quarantine_path.rglob("*.yaml"))) == 1

    candidates = client.get(
        "/api/v1/knowledge/candidates",
        params={"kind": "signal_mapping", "limit": 100},
    )
    assert candidates.status_code == 200, candidates.text
    signal_candidates = candidates.json()["candidates"]
    assert len(signal_candidates) >= 5
    for candidate in signal_candidates:
        promoted = client.post(
            f"/api/v1/knowledge/candidates/{candidate['id']}/review",
            json={
                "decision": "trust",
                "reviewer": "e2e-live-verifier",
                "live_verified": True,
            },
        )
        assert promoted.status_code == 200, promoted.text
        assert promoted.json()["knowledge_revision"] is not None

    learned_sources = signal_store.stats()["mappings_by_source"]
    assert learned_sources.get("dashboard_ingest", 0) >= 5

    catalog = scenario_catalog(scenario)
    backend = CapturingBackend(catalog=catalog)
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    current_service = scenario["prompt_matrix"]["services"][0]
    resolved_metrics = {
        entry.name
        for signal_type in {sig["signal_type"] for sig in inferred_signals}
        for entry, _confidence in signal_store.resolve_signal(
            signal_type,
            catalog,
            context_service=current_service,
            target_query_language="promql",
        )
    }
    assert resolved_metrics
    assert resolved_metrics <= inferred_metrics

    def create_fixture_provider(runtime_settings):
        return IncidentFixtureProvider(
            catalog,
            resolved_metrics,
            service=current_service,
            runtime_settings=runtime_settings,
        )

    monkeypatch.setattr(provider_registry, "create_provider", create_fixture_provider)

    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return intent_from_prompt(prompt, service=current_service), TokenUsage()

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    thresholds = scenario["utility_thresholds"]
    cases = incident_cases(scenario)
    evaluations = []
    for case in cases:
        response = await pipeline_mod.run_pipeline(
            DashRequest(prompt=case.prompt, user_id="e2e", channel_id="dashboard-upload-learning")
        )
        assert response.dashboard_uid, case.case_id
        contract = history_store.get_contract(response.investigation_id)
        assert contract is not None, case.case_id
        assert "checkout_edge_incident_response" not in contract.model_dump_json(), case.case_id
        assert backend.published_specs, case.case_id
        spec = backend.published_specs[-1]
        assert "Checkout Edge Incident Response" not in spec.title
        evaluation = evaluate_incident(spec, case)
        evaluation.assert_passes(thresholds, case_id=case.case_id)
        evaluations.append(evaluation)

    assert len(evaluations) == 27
    assert sum(e.usefulness_score for e in evaluations) / len(evaluations) >= thresholds["min_usefulness_score"]


@pytest.mark.e2e
async def test_manual_teach_signal_mapping_is_used_before_dashboard_creation(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, history_store, _feedback_store, archetypes_path, _quarantine_path = isolated_learning_runtime
    _install_taught_latency_archetype(archetypes_path)
    _teach_taught_latency_mapping()
    backend = _configure_taught_latency_pipeline(monkeypatch)

    response = await pipeline_mod.run_pipeline(
        DashRequest(prompt="checkout-api p95 latency is high", user_id="e2e", channel_id="manual-teach")
    )

    assert response.dashboard_uid
    assert backend.published_specs
    found = {query.expr for panel in backend.published_specs[-1].panels for query in panel.queries}
    assert any("acme_checkout_latency_seconds_bucket" in expr for expr in found)
    assert all("http_request_duration_seconds" not in expr for expr in found)
    contract = history_store.get_contract(response.investigation_id)
    assert contract is not None
    compilation_usage = [
        usage
        for usage in contract.knowledge_usage
        if usage.disposition == "applied" and "query_compilation" in usage.used_for
    ]
    assert len(compilation_usage) == 1
    assert compilation_usage[0].score_delta == 0

    snapshot = history_store.get_snapshot(response.investigation_id)
    assert snapshot is not None
    acquisition = snapshot.model_inputs["catalog_acquisition_v1"]
    assert acquisition["version"] == "catalog-acquisition/1"
    assert acquisition["entries"]
    assert acquisition["selections"]
    assert any("legacy_unknown" in source["reasons"] for source in acquisition["sources"])
    assert snapshot.additional_decisions and snapshot.additional_provenance
    assert {item.id for item in snapshot.additional_decisions} <= {item.id for item in contract.decision_log}
    assert {item.id for item in snapshot.additional_provenance} <= {item.id for item in contract.provenance}

    def forbid_refetch(*_args, **_kwargs):
        pytest.fail("exact replay must use the persisted acquisition inputs")

    monkeypatch.setattr(pipeline_mod, "get_active_backends", forbid_refetch)
    monkeypatch.setattr(provider_registry, "create_provider", forbid_refetch)
    replayed = history_store.replay_contract(response.investigation_id)
    assert replayed is not None
    assert replayed.runtime.input_fingerprint == contract.runtime.input_fingerprint
    assert replayed.runtime.output_fingerprint == contract.runtime.output_fingerprint
    assert replayed.decision_log == contract.decision_log
    assert replayed.provenance == contract.provenance
    assert replayed.model_dump(mode="json") == contract.model_dump(mode="json")


@pytest.mark.e2e
@pytest.mark.parametrize("dimension", ["tree", "text", "key", "duplicate_id"])
async def test_acquisition_admission_failure_finishes_run_without_publication(
    isolated_learning_runtime, monkeypatch, dimension
):
    import tacit.pipeline.runner as runner
    from tacit.models.schemas import ContextChunk

    _signals, history, _feedback, archetypes_path, _quarantine = isolated_learning_runtime
    _install_taught_latency_archetype(archetypes_path)
    _teach_taught_latency_mapping()
    backend = _configure_taught_latency_pipeline(monkeypatch)
    prepare = runner.prepare_acquisition_completion

    def inadmissible_inputs(**kwargs):
        metadata = {}
        if dimension == "tree":
            nested = []
            for _ in range(33):
                nested = [nested]
            metadata["nested"] = nested
        elif dimension == "text":
            metadata["text"] = "x" * 65_537
        elif dimension == "key":
            metadata[0] = "invalid"
        else:
            metadata["provenance_id"] = kwargs["assembly_inputs"]["investigation_id"]
        kwargs["assembly_inputs"]["context_chunks"] = [
            ContextChunk.model_construct(source="runbook", content="test", metadata=metadata)
        ]
        return prepare(**kwargs)

    monkeypatch.setattr(runner, "prepare_acquisition_completion", inadmissible_inputs)
    response = await pipeline_mod.run_pipeline(DashRequest(prompt="checkout-api p95 latency is high"))
    assert response.investigation_status == "failed"
    assert not response.dashboard_uid
    assert backend.published_specs == []
    assert history.get_contract(response.investigation_id) is None
    assert history.list_runs(response.investigation_id)[-1]["status"] == "failed"


@pytest.mark.e2e
async def test_acquisition_completion_replays_pinned_knowledge_reference(isolated_learning_runtime, monkeypatch):
    import tacit.pipeline.runner as runner

    _signals, history, _feedback, archetypes_path, _quarantine = isolated_learning_runtime
    _install_taught_latency_archetype(archetypes_path)
    _teach_taught_latency_mapping()
    _configure_taught_latency_pipeline(monkeypatch)
    complete = runner.complete_pipeline

    async def mutate_original(**kwargs):
        assert kwargs["knowledge_snapshot"] is not None
        kwargs["knowledge_snapshot"].id = "knowledge_snapshot_mutated_after_preparation"
        return await complete(**kwargs)

    monkeypatch.setattr(runner, "complete_pipeline", mutate_original)
    response = await pipeline_mod.run_pipeline(DashRequest(prompt="checkout-api p95 latency is high"))
    contract = history.get_contract(response.investigation_id)
    snapshot = history.get_snapshot(response.investigation_id)
    assert contract is not None and snapshot is not None
    assert snapshot.knowledge_snapshot_ref == contract.knowledge_snapshot_ref
    replayed = history.replay_contract(response.investigation_id)
    assert replayed is not None
    assert replayed.runtime.output_fingerprint == contract.runtime.output_fingerprint


@pytest.mark.e2e
@pytest.mark.parametrize(
    "expression",
    [
        "histogram_quantile(0.95, sum(rate(requests_bucket{{}}[5m])) by (le))",
        'sum(rate(requests{{status=~"5.."}}[5m]))',
        "sum(rate(requests{{}}[5m])) * 100",
    ],
)
async def test_unsupported_signalflow_slot_fails_entire_mixed_investigation(
    isolated_learning_runtime, monkeypatch, expression
):
    from dataclasses import replace

    import yaml

    _signals, history, _feedback, archetypes_path, _quarantine = isolated_learning_runtime
    definition = yaml.safe_load(TAUGHT_LATENCY_ARCHETYPE)
    archetype = definition["archetypes"][0]
    archetype["required_metrics"] = ["requests"]
    archetype["required_signals"] = []
    archetype["signal_bindings"] = {}
    archetype["panels"] = [
        {"title": "Supported", "queries": [{"expr": "requests"}]},
        {"title": "Unsupported", "queries": [{"expr": expression}]},
    ]
    archetypes_path.write_text(yaml.safe_dump(definition), encoding="utf-8")
    templates.reload_archetypes()
    _configure_taught_latency_pipeline(monkeypatch)
    monkeypatch.setattr(settings, "signalfx_enabled", True)
    monkeypatch.setattr(settings, "signalfx_api_token", "hermetic-fixture-token")

    class SingleRemoteBackend(CapturingBackend):
        @property
        def runtime_ownership(self):
            descriptor = super().runtime_ownership
            return replace(
                descriptor, remotes=tuple(remote for remote in descriptor.remotes if remote.provider == self.name)
            )

    class SignalFlowBackend(SingleRemoteBackend):
        @property
        def name(self):
            return "signalfx"

        @property
        def query_language(self):
            return "signalflow"

    signalfx = SignalFlowBackend(
        catalog=[
            MetricEntry(
                name="requests",
                datasource_uid="sfx-e2e",
                datasource_name="SignalFx E2E",
                datasource_type="signalfx",
                query_language="signalflow",
            )
        ]
    )
    grafana = SingleRemoteBackend(catalog=[])
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [signalfx, grafana])
    response = await pipeline_mod.run_pipeline(DashRequest(prompt="checkout-api p95 latency is high"))
    assert response.investigation_status == "failed"
    assert not response.dashboard_uid
    assert signalfx.published_specs == grafana.published_specs == []
    assert history.get_contract(response.investigation_id) is None
    run = history.list_runs(response.investigation_id)[-1]
    assert run["status"] == "failed"
    assert run["error_code"] == "pipeline_failed"
    assert run["error_detail"] == "signalflow_translation_unsupported"
    events = history.list_events(response.investigation_id, response.investigation_run_id)
    assert any(
        event["event_type"] == "stage_completed"
        and event["payload"].get("stage") == "acquisition"
        and event["payload"].get("reason_code") == "signalflow_translation_unsupported"
        for event in events
    )


@pytest.mark.e2e
async def test_read_only_pipeline_is_rejected_before_history_or_publish(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, history_store, _feedback_store, archetypes_path, _quarantine_path = isolated_learning_runtime
    _install_taught_latency_archetype(archetypes_path)
    _teach_taught_latency_mapping()
    backend = _configure_taught_latency_pipeline(monkeypatch)
    monkeypatch.setattr(settings, "knowledge_permissions", "knowledge.read")

    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        await pipeline_mod.run_pipeline(
            DashRequest(prompt="checkout-api p95 latency is high", user_id="e2e", channel_id="read-only")
        )

    assert history_store.list_recent() == []
    assert backend.published_specs == []


@pytest.mark.e2e
async def test_pipeline_repins_knowledge_when_catalog_retrieval_widens_archetype_scope(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, _history_store, _feedback_store, archetypes_path, _quarantine_path = isolated_learning_runtime
    archetypes_path.write_text(CATALOG_SCOPE_ARCHETYPE, encoding="utf-8")
    templates.reload_archetypes()
    backend = CapturingBackend(
        catalog=[
            MetricEntry(
                name="catalog_scope_latency_seconds",
                datasource_uid="prom-e2e",
                datasource_name="Prometheus E2E",
                datasource_type="prometheus",
                query_language="promql",
                dimensions=['service="checkout-api"'],
            )
        ]
    )
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["checkout-api"],
                signals=[SignalType.METRICS],
                keywords=["checkout", "health"],
                timerange="1h",
                problem_type="general",
                archetypes=[ArchetypeMatch(type="general", confidence=0.9)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)
    repinned_scopes: list[list[str]] = []
    original_repin = KnowledgeService.repin_snapshot

    def record_repin(self, scope, usage, *, previous_scope):
        repinned_scopes.append(list(scope.archetype_refs))
        return original_repin(self, scope, usage, previous_scope=previous_scope)

    monkeypatch.setattr(KnowledgeService, "repin_snapshot", record_repin)

    response = await pipeline_mod.run_pipeline(
        DashRequest(prompt="checkout-api general health", user_id="e2e", channel_id="scope-repin")
    )

    assert response.dashboard_uid
    assert repinned_scopes
    assert "archetype:catalog_scope_latency" in repinned_scopes[-1]
    assert any(panel.source_archetype == "catalog_scope_latency" for panel in backend.published_specs[-1].panels)


@pytest.mark.e2e
async def test_governed_compilation_fails_closed_when_usage_audit_is_unavailable(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, history_store, _feedback_store, archetypes_path, _quarantine_path = isolated_learning_runtime
    _install_taught_latency_archetype(archetypes_path)
    _teach_taught_latency_mapping()
    backend = _configure_taught_latency_pipeline(monkeypatch)

    def fail_snapshot(_self, _scope, _usage):
        raise OSError("knowledge audit database unavailable")

    monkeypatch.setattr("tacit.knowledge.service.KnowledgeService.reconcile_pinned_usage", fail_snapshot)

    with pytest.raises(FatalPipelineError, match="usage audit could not be persisted"):
        await pipeline_mod.run_pipeline(
            DashRequest(prompt="checkout-api p95 latency is high", user_id="e2e", channel_id="audit-failure")
        )

    assert backend.published_specs == []
    investigation = history_store.list_recent(limit=1)[0]
    assert investigation["status"] == "failed"
    assert "usage audit could not be persisted" in investigation["error"]


@pytest.mark.e2e
async def test_governed_ranking_fails_closed_when_final_snapshot_is_unavailable(
    isolated_learning_runtime,
    monkeypatch,
):
    signal_store, history_store, feedback_store, archetypes_path, _quarantine_path = isolated_learning_runtime
    runtime_settings = signal_store.runtime_settings
    _install_taught_latency_archetype(archetypes_path)
    backend = CapturingBackend(
        catalog=[
            MetricEntry(
                name="http_request_duration_seconds_bucket",
                datasource_uid="prom-e2e",
                datasource_name="Prometheus E2E",
                datasource_type="prometheus",
                query_language="promql",
                dimensions=['service="checkout-api"', "le={0.1,0.5,1,5}"],
            )
        ],
        runtime_settings=runtime_settings,
    )
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["checkout-api"],
                signals=[SignalType.METRICS],
                keywords=["checkout", "latency", "p95"],
                timerange="1h",
                problem_type="latency_investigation",
                archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.97)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    class RankingKnowledgeService:
        runtime_ownership = signal_store.runtime_ownership
        sqlite_readiness_admission = signal_store.sqlite_readiness_admission

        def create_snapshot(self, scope):
            return (
                KnowledgeSnapshot(
                    id="knowledge_snapshot_before_ranking",
                    tenant_id=scope.tenant_id,
                    fingerprint="sha256:before-ranking",
                ),
                [
                    KnowledgeUsage(
                        tenant_id=scope.tenant_id,
                        knowledge_ref="knowledge_ranked_dependency",
                        knowledge_revision=1,
                        disposition=KnowledgeUsageDisposition.CONSIDERED_NOT_APPLIED,
                    )
                ],
            )

        def apply_stage_usage(self, usage, _stage_uses):
            return usage

        def apply_compilation_usage(self, usage, _revision_refs):
            return usage

        def apply_evidence_usage(self, usage, _revision_refs, _refs_by_requirement):
            return usage

        def reconcile_live_observations(self, usage, _observations):
            return usage

        def apply_to_ranking(self, ranking, usage):
            candidate = CulpritCandidate(
                rank=1,
                suspect="redis-session",
                suspect_type="datastore",
                score=0.08,
                contextual_reasons=["Governed dependency context"],
            )
            return (
                ranking.model_copy(update={"candidates": [candidate, *ranking.candidates]}),
                [
                    usage[0].model_copy(
                        update={
                            "disposition": KnowledgeUsageDisposition.APPLIED,
                            "used_for": ["candidate_generation", "ranking"],
                            "score_delta": 0.08,
                        }
                    )
                ],
            )

        def snapshot_from_usage(self, _tenant_id, _usage):
            raise OSError("final knowledge snapshot unavailable")

    knowledge_service = RankingKnowledgeService()
    deps = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        backend_factory=declare_backend_factory(
            lambda: [backend],
            runtime_settings=runtime_settings,
            component="e2e_ranking_backend_factory",
        ),
        history_store_factory=declare_runtime_factory(
            lambda: history_store,
            ownership=history_store.runtime_ownership,
            factory_kind="store:history",
        ),
        feedback_store_factory=declare_runtime_factory(
            lambda: feedback_store,
            ownership=feedback_store.runtime_ownership,
            factory_kind="store:feedback",
        ),
        signal_store_factory=declare_runtime_factory(
            lambda: signal_store,
            ownership=signal_store.runtime_ownership,
            factory_kind="store:signals",
        ),
        knowledge_service_factory=declare_runtime_factory(
            lambda: knowledge_service,
            ownership=knowledge_service.runtime_ownership,
            factory_kind="knowledge:signals",
        ),
    )

    with pytest.raises(FatalPipelineError, match="usage audit could not be persisted"):
        await pipeline_mod.run_pipeline(
            DashRequest(prompt="checkout-api p95 latency is high", user_id="e2e", channel_id="ranking-audit"),
            deps,
        )

    assert backend.published_specs == []
    investigation = history_store.list_recent(limit=1)[0]
    assert investigation["status"] == "failed"
    assert "usage audit could not be persisted" in investigation["error"]


@pytest.mark.e2e
def test_reject_uploaded_dashboard_records_negative_candidates_without_teaching(
    isolated_learning_runtime,
):
    signal_store, _history_store, _feedback_store, _archetypes_path, _quarantine_path = isolated_learning_runtime
    scenario = load_scenario(SCENARIO_PATH)
    client = TestClient(app)

    upload = client.post(
        "/api/v1/learn/dashboard/json",
        json={
            "vendor": "grafana",
            "source_name": "checkout-edge-incident.json",
            "auto_approve": False,
            "dashboard": build_grafana_dashboard(scenario),
        },
    )
    assert upload.status_code == 200, upload.text

    reject = client.post(f"/api/v1/learn/dashboards/{scenario['dashboard']['uid']}/reject?backend=grafana_json")
    assert reject.status_code == 200, reject.text
    rejected = reject.json()
    assert rejected["status"] == "rejected"
    assert rejected["rejected_candidates"] >= 1

    stored = signal_store.get_ingested_dashboard(scenario["dashboard"]["uid"], backend_name="grafana_json")
    assert stored["status"] == "rejected"
    assert signal_store.stats()["mappings_by_source"].get("dashboard_ingest", 0) == 0
    rejected_metrics = {candidate["metric"] for candidate in signal_store.list_rejected_candidates()}
    assert (
        "checkout_edge_inflight_requests" in rejected_metrics
        or "checkout_edge_db_pool_wait_seconds_bucket" in rejected_metrics
    )


@pytest.mark.e2e
async def test_pipeline_returns_helpful_no_metrics_response_without_publishing(
    isolated_learning_runtime,
    monkeypatch,
):
    _signal_store, history_store, _feedback_store, _archetypes_path, _quarantine_path = isolated_learning_runtime
    backend = CapturingBackend(catalog=[])
    monkeypatch.setattr(pipeline_mod, "get_active_backends", lambda: [backend])
    monkeypatch.setattr(pipeline_mod, "enrich_context", _no_context)

    async def fake_classify_intent(prompt: str):
        return (
            Intent(
                summary=prompt,
                domain="application",
                services=["missing-service"],
                signals=[SignalType.METRICS],
                keywords=["missing", "latency"],
                timerange="1h",
                problem_type="latency_investigation",
                archetypes=[ArchetypeMatch(type="latency_investigation", confidence=0.9)],
            ),
            TokenUsage(),
        )

    monkeypatch.setattr(pipeline_mod, "classify_intent", fake_classify_intent)

    response = await pipeline_mod.run_pipeline(
        DashRequest(prompt="missing-service is slow", user_id="e2e", channel_id="empty-telemetry")
    )

    assert response.dashboard_uid == ""
    assert response.dashboard_url == ""
    assert response.panel_count == 0
    assert "No metrics found" in response.summary
    assert response.investigation_id
    assert response.investigation_run_id
    assert response.investigation_status == "failed"
    assert history_store.get(response.investigation_id) is not None
    assert history_store.list_runs(response.investigation_id)[-1]["run_id"] == response.investigation_run_id
    assert history_store.list_runs(response.investigation_id)[-1]["status"] == "failed"
    assert backend.published_specs == []
