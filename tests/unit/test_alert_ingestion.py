import pytest

from tacit.alert_ingest import ingest_alert, ingest_alert_features, learn_backend_alerts
from tacit.backends.base import AlertFeatures
from tacit.backends.grafana import GrafanaBackend, _parse_grafana_alert_rule
from tacit.backends.signalfx import SignalFxBackend, _parse_signalfx_detector
from tacit.config import Settings
from tacit.knowledge.migration import migrate_signal_mapping
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.signals import SignalStore


def test_grafana_alert_rule_parses_to_common_alert_features():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "isPaused": False,
            "labels": {"service": "checkout", "severity": "critical"},
            "annotations": {"__dashboardUid__": "checkout-dashboard", "__panelTitle__": "p95 latency"},
            "data": [
                {
                    "refId": "A",
                    "model": {
                        "datasource": {"type": "prometheus", "uid": "prom"},
                        "expr": (
                            'histogram_quantile(0.95, rate(checkout_latency_seconds_bucket{service="checkout"}[5m]))'
                        ),
                    },
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.alert_uid == "checkout-latency"
    assert features.backend_name == "grafana"
    assert features.query_language == "promql"
    assert features.metrics_found == ["checkout_latency_seconds_bucket"]
    assert features.service_hints == ["checkout"]
    assert features.dashboard_uid == "checkout-dashboard"


def test_grafana_alert_rule_skips_expression_ref_ids_and_non_prometheus_queries():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "B",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": "prom",
                    "model": {
                        "datasource": {"type": "prometheus", "uid": "prom"},
                        "expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])',
                    },
                },
                {
                    "refId": "B",
                    "datasourceUid": "__expr__",
                    "model": {"type": "math", "expression": "$A > 0"},
                },
                {
                    "refId": "C",
                    "datasourceUid": "loki",
                    "model": {"datasource": {"type": "loki", "uid": "loki"}, "expr": '{app="checkout"} |= "error"'},
                },
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.metrics_found == ["checkout_latency_seconds_count"]
    assert features.query_transformations == ['rate(checkout_latency_seconds_count{service="checkout"}[5m])']
    assert "$A > 0" in features.condition


def test_grafana_alert_rule_skips_unknown_datasource_uid_queries():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-logs",
            "title": "Checkout logs high",
            "condition": "A",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": "loki-prod",
                    "model": {"expr": '{app="checkout"} |= "error"'},
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
    )

    assert features.metrics_found == []
    assert features.query_transformations == []


def test_grafana_alert_rule_resolves_datasource_uid_only_prometheus_queries():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "datasourceUid": "prom-prod",
                    "model": {"expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])'},
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
        datasource_types_by_uid={"prom-prod": "prometheus"},
    )

    assert features.metrics_found == ["checkout_latency_seconds_count"]
    assert features.query_transformations == ['rate(checkout_latency_seconds_count{service="checkout"}[5m])']


def test_grafana_alert_rule_uses_uid_resolution_when_datasource_object_has_name_only():
    features = _parse_grafana_alert_rule(
        {
            "uid": "checkout-latency",
            "title": "Checkout latency high",
            "condition": "A",
            "labels": {"service": "checkout"},
            "data": [
                {
                    "refId": "A",
                    "model": {
                        "datasource": {"uid": "prom-prod", "name": "Prod Prometheus"},
                        "expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])',
                    },
                }
            ],
        },
        backend_name="grafana",
        base_url="http://grafana.example",
        datasource_types_by_uid={"prom-prod": "prometheus"},
    )

    assert features.metrics_found == ["checkout_latency_seconds_count"]


def test_grafana_alert_threshold_details_change_condition_for_fingerprint():
    base_rule = {
        "uid": "checkout-latency",
        "title": "Checkout latency high",
        "condition": "C",
        "labels": {"service": "checkout"},
        "data": [
            {
                "refId": "A",
                "model": {
                    "datasource": {"type": "prometheus", "uid": "prom"},
                    "expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])',
                },
            },
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "model": {"type": "math", "expression": "$A > 10"},
            },
        ],
    }
    changed_rule = {
        **base_rule,
        "data": [
            base_rule["data"][0],
            {
                "refId": "C",
                "datasourceUid": "__expr__",
                "model": {"type": "math", "expression": "$A > 20"},
            },
        ],
    }

    first = _parse_grafana_alert_rule(base_rule, backend_name="grafana", base_url="http://grafana.example")
    changed = _parse_grafana_alert_rule(changed_rule, backend_name="grafana", base_url="http://grafana.example")

    assert first.metrics_found == changed.metrics_found == ["checkout_latency_seconds_count"]
    assert "$A > 10" in first.condition
    assert "$A > 20" in changed.condition
    assert first.condition != changed.condition


@pytest.mark.asyncio
async def test_direct_alert_auto_approval_requires_teach_permissions():
    from tacit.config import Settings

    features = AlertFeatures(
        alert_uid="protected-alert",
        alert_title="Protected alert",
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        metrics_found=["protected_metric"],
    )

    with pytest.raises(PermissionError, match="knowledge.review"):
        await ingest_alert_features(
            features,
            auto_approve=True,
            store=object(),
            runtime_settings=Settings(
                _env_file=None,
                knowledge_permissions="knowledge.read",
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("dry_run", [False, True])
async def test_alert_ingestion_uses_explicit_runtime_signal_store(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    dry_run: bool,
):
    db_path = tmp_path / f"scoped-alert-{dry_run}.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_tenant_id="tenant-a",
    )

    def unexpected_global_store():
        raise AssertionError("explicit alert runtime consulted the process-global signal store")

    monkeypatch.setattr("tacit.alert_ingest.get_signal_store", unexpected_global_store)
    features = AlertFeatures(
        alert_uid="scoped-alert",
        alert_title="Scoped alert",
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        metrics_found=["checkout_errors_total"],
    )

    result = await ingest_alert_features(
        features,
        dry_run=dry_run,
        runtime_settings=runtime_settings,
    )

    assert result["dry_run"] is dry_run
    scoped_store = SignalStore(db_path=db_path, runtime_settings=runtime_settings)
    stored = scoped_store.get_ingested_alert("scoped-alert", "grafana", tenant_id="tenant-a")
    assert (stored is None) is dry_run


@pytest.mark.asyncio
async def test_alert_ingestion_requires_apply_before_persistence(tmp_path):
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review",
        ),
    )
    features = AlertFeatures(
        alert_uid="protected-pending-alert",
        alert_title="Protected pending alert",
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        metrics_found=["protected_metric"],
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        await ingest_alert_features(
            features,
            auto_approve=False,
            store=store,
        )

    assert store.list_ingested_alerts(tenant_id="default") == []


@pytest.mark.asyncio
async def test_alert_auto_approval_claims_source_before_promotion(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    features = AlertFeatures(
        alert_uid="promotion-crash-alert",
        alert_title="Promotion crash alert",
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        metrics_found=["checkout_latency_seconds"],
    )
    inferred = [
        {
            "signal_type": "request_latency",
            "metric": "checkout_latency_seconds",
            "source": "heuristic",
            "confidence": 0.9,
            "auto_teach_eligible": True,
        }
    ]
    monkeypatch.setattr("tacit.alert_ingest.infer_signals_from_metrics", lambda *_args, **_kwargs: inferred)

    def crash_during_promotion(**_kwargs):
        source = store.get_ingested_alert("promotion-crash-alert", "grafana")
        assert source is not None and source["status"] == "approving"
        raise RuntimeError("simulated promotion crash")

    monkeypatch.setattr("tacit.alert_ingest.persist_inferred_signal_review", crash_during_promotion)

    with pytest.raises(RuntimeError, match="simulated promotion crash"):
        await ingest_alert_features(features, auto_approve=True, store=store)

    source = store.get_ingested_alert("promotion-crash-alert", "grafana")
    assert source is not None
    assert source["status"] == "approving"


@pytest.mark.asyncio
async def test_alert_auto_approval_retry_finalizes_claimed_generation(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    features = AlertFeatures(
        alert_uid="promotion-retry-alert",
        alert_title="Promotion retry alert",
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        metrics_found=["checkout_latency_seconds"],
    )
    inferred = [
        {
            "signal_type": "request_latency",
            "metric": "checkout_latency_seconds",
            "source": "heuristic",
            "confidence": 0.9,
            "auto_teach_eligible": True,
        }
    ]
    monkeypatch.setattr("tacit.alert_ingest.infer_signals_from_metrics", lambda *_args, **_kwargs: inferred)
    promotion_calls = 0

    def idempotent_promotion(**kwargs):
        nonlocal promotion_calls
        promotion_calls += 1
        source = store.get_ingested_alert("promotion-retry-alert", "grafana")
        assert source is not None and source["status"] == "approving"
        kwargs["governed_pairs"].add(("checkout_latency_seconds", "request_latency"))
        return True

    monkeypatch.setattr("tacit.alert_ingest.persist_inferred_signal_review", idempotent_promotion)

    knowledge_service = KnowledgeService(
        KnowledgeRepository(store._db_path),
        signal_store=store,
    )

    original_finalize = store.finalize_ingested_alert_approval
    finalize_calls = 0

    def crash_once(*args, **kwargs):
        nonlocal finalize_calls
        finalize_calls += 1
        if finalize_calls == 1:
            raise RuntimeError("simulated finalization crash")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(store, "finalize_ingested_alert_approval", crash_once)

    with pytest.raises(RuntimeError, match="simulated finalization crash"):
        await ingest_alert_features(
            features,
            auto_approve=True,
            store=store,
            knowledge_service=knowledge_service,
        )
    assert store.get_ingested_alert("promotion-retry-alert", "grafana")["status"] == "approving"

    recovered = await ingest_alert_features(
        features,
        auto_approve=True,
        store=store,
        knowledge_service=knowledge_service,
    )

    assert recovered["status"] == "approved"
    assert promotion_calls == finalize_calls == 2
    assert store.get_ingested_alert("promotion-retry-alert", "grafana")["status"] == "approved"


@pytest.mark.asyncio
async def test_alert_approval_rolls_back_governed_authority_before_final_status(tmp_path, monkeypatch):
    store = SignalStore(db_path=tmp_path / "signals.db")
    knowledge_service = KnowledgeService(
        KnowledgeRepository(store._db_path),
        signal_store=store,
    )
    features = AlertFeatures(
        alert_uid="governed-alert-rollback",
        alert_title="Governed alert rollback",
        backend_name="grafana",
        query_language="promql",
        condition="A > 1",
        metrics_found=["checkout_latency_seconds"],
    )
    inferred = [
        {
            "signal_type": "request_latency",
            "metric": "checkout_latency_seconds",
            "source": "heuristic",
            "confidence": 0.9,
            "auto_teach_eligible": True,
        }
    ]
    monkeypatch.setattr("tacit.alert_ingest.infer_signals_from_metrics", lambda *_args, **_kwargs: inferred)

    def promote_governed_mapping(**kwargs):
        candidate_id = migrate_signal_mapping(
            {
                "id": "governed-alert-rollback",
                "signal_type": "request_latency",
                "metric_pattern": "checkout_latency_seconds",
                "source_type": "alert_ingest",
                "source_refs": [kwargs["source_ref"]],
            },
            service=knowledge_service,
        )
        knowledge_service.review_candidate(
            candidate_id,
            approved=True,
            reviewer="test",
        )
        _decision, revision = knowledge_service.evaluate_candidate(
            candidate_id,
            live_verified=True,
        )
        assert revision is not None
        kwargs["governed_candidate_ids"].add(candidate_id)
        kwargs["governed_pairs"].add(("checkout_latency_seconds", "request_latency"))
        return True

    monkeypatch.setattr("tacit.alert_ingest.persist_inferred_signal_review", promote_governed_mapping)
    original_finalize = store.finalize_ingested_alert_approval
    finalize_attempts = 0

    def crash_once(*args, **kwargs):
        nonlocal finalize_attempts
        finalize_attempts += 1
        if finalize_attempts == 1:
            raise RuntimeError("simulated finalization crash")
        return original_finalize(*args, **kwargs)

    monkeypatch.setattr(store, "finalize_ingested_alert_approval", crash_once)

    with pytest.raises(RuntimeError, match="simulated finalization crash"):
        await ingest_alert_features(
            features,
            auto_approve=True,
            store=store,
            knowledge_service=knowledge_service,
        )

    assert store.get_ingested_alert("governed-alert-rollback", "grafana")["status"] == "approving"
    assert knowledge_service.repository.list_candidates() == []
    assert knowledge_service.repository.list_current_revisions() == []
    assert store.get_mappings_for_signal("request_latency", include_decayed=True) == []

    recovered = await ingest_alert_features(
        features,
        auto_approve=True,
        store=store,
        knowledge_service=knowledge_service,
    )

    assert recovered["status"] == "approved"
    revisions = knowledge_service.repository.list_current_revisions()
    assert len(revisions) == 1
    mappings = store.get_mappings_for_signal("request_latency", include_decayed=True)
    assert len(mappings) == 1
    assert mappings[0]["governance_ref"] == revisions[0].knowledge_id


@pytest.mark.asyncio
async def test_direct_alert_ingestion_authorizes_before_backend_access(tmp_path):
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review",
        ),
    )

    class Backend:
        accessed = False

        async def ingest_alert(self, _alert_uid):
            self.accessed = True
            raise AssertionError("unauthorized ingestion accessed the backend")

    backend = Backend()
    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        await ingest_alert("protected-alert", backend=backend, store=store)

    assert backend.accessed is False


@pytest.mark.asyncio
async def test_bulk_alert_learning_authorizes_before_backend_access(tmp_path, monkeypatch):
    store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review",
        ),
    )
    backend_accessed = False

    def get_backends(*_args):
        nonlocal backend_accessed
        backend_accessed = True
        return []

    monkeypatch.setattr("tacit.backends.get_active_backends", get_backends)

    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        await learn_backend_alerts("grafana", store=store)

    assert backend_accessed is False


@pytest.mark.asyncio
async def test_grafana_backend_resolves_datasource_uid_for_managed_alerts():
    class FakeGrafanaClient:
        base_url = "http://grafana.example"

        async def _get(self, path: str, **_kwargs):
            assert path == "/api/v1/provisioning/alert-rules/checkout-latency"
            return {
                "uid": "checkout-latency",
                "title": "Checkout latency high",
                "condition": "A",
                "data": [
                    {
                        "refId": "A",
                        "datasourceUid": "prom-prod",
                        "model": {"expr": 'rate(checkout_latency_seconds_count{service="checkout"}[5m])'},
                    }
                ],
            }

        async def list_datasources(self):
            return [{"uid": "prom-prod", "name": "Prometheus", "type": "prometheus"}]

        async def close(self):
            return None

    backend = GrafanaBackend(client=FakeGrafanaClient())

    features = await backend.ingest_alert("checkout-latency")

    assert features.metrics_found == ["checkout_latency_seconds_count"]


def test_signalfx_detector_parses_to_common_alert_features():
    features = _parse_signalfx_detector(
        {
            "id": "detector-1",
            "name": "Checkout errors high",
            "tags": ["service:checkout"],
            "teams": ["payments"],
            "programText": "A = data('checkout.errors').sum().publish(label='A')",
            "rules": [{"detectLabel": "A above threshold", "severity": "Critical"}],
        },
        backend_name="signalfx",
        realm="us1",
    )

    assert features.alert_uid == "detector-1"
    assert features.backend_name == "signalfx"
    assert features.query_language == "signalflow"
    assert features.metrics_found == ["checkout.errors"]
    assert features.condition == "A above threshold"
    assert features.severity == "Critical"
    assert features.labels == {"team": "payments"}


def test_signalfx_detector_preserves_rule_runbook_annotations():
    features = _parse_signalfx_detector(
        {
            "id": "detector-1",
            "name": "Checkout errors high",
            "programText": "A = data('checkout.errors').sum().publish(label='A')",
            "rules": [
                {
                    "detectLabel": "A above threshold",
                    "severity": "Critical",
                    "runbookUrl": "https://runbooks.example/checkout-errors",
                    "tip": "Check checkout downstream dependencies",
                }
            ],
        },
        backend_name="signalfx",
        realm="us1",
    )

    assert features.annotations["rule_0_runbookUrl"] == "https://runbooks.example/checkout-errors"
    assert features.annotations["rule_0_tip"] == "Check checkout downstream dependencies"


@pytest.mark.asyncio
async def test_invalid_alert_backend_closes_instantiated_clients(monkeypatch):
    closed = []

    class FakeBackend:
        name = "grafana"

        async def close(self):
            closed.append(self.name)

    monkeypatch.setattr("tacit.backends.get_active_backends", lambda *_args, **_kwargs: [FakeBackend()])

    with pytest.raises(ValueError):
        await ingest_alert("checkout-latency", backend_name="grafna")

    assert closed == ["grafana"]


@pytest.mark.asyncio
async def test_pending_alert_refresh_retires_removed_support_without_promoting_replacement(tmp_path, monkeypatch):
    from tacit.signals import SignalStore

    store = SignalStore(db_path=tmp_path / "signals.db")
    batches = iter(
        [
            [
                {
                    "signal_type": "old_latency",
                    "metric": "old_latency_seconds",
                    "confidence": 0.9,
                    "source": "heuristic",
                    "signal_family": "latency",
                    "auto_teach_eligible": True,
                }
            ],
            [
                {
                    "signal_type": "new_latency",
                    "metric": "new_latency_seconds",
                    "confidence": 0.9,
                    "source": "heuristic",
                    "signal_family": "latency",
                    "auto_teach_eligible": True,
                }
            ],
        ]
    )
    monkeypatch.setattr(
        "tacit.alert_ingest.infer_signals_from_metrics",
        lambda *args, **kwargs: next(batches),
    )

    def features(metric: str) -> AlertFeatures:
        return AlertFeatures(
            alert_uid="pending-refresh-alert",
            alert_title="Pending refresh alert",
            backend_name="grafana",
            query_language="promql",
            condition="A > 1",
            metrics_found=[metric],
            query_transformations=[metric],
            service_hints=["checkout"],
        )

    await ingest_alert_features(features("old_latency_seconds"), auto_approve=True, store=store)
    pending = await ingest_alert_features(features("new_latency_seconds"), auto_approve=False, store=store)

    candidates = KnowledgeRepository(store._db_path).list_candidates(
        "default",
        kind="signal_mapping",
        limit=None,
    )
    old = next(candidate for candidate in candidates if "old_latency_seconds" in candidate.payload_ref)
    assert pending["status"] == "pending"
    assert old.state.lifecycle_status.value == "stale"
    assert all("new_latency_seconds" not in candidate.payload_ref for candidate in candidates)
    assert store.get_mappings_for_signal("old_latency", include_decayed=True) == []


@pytest.mark.asyncio
async def test_limited_alert_crawl_does_not_mark_unseen_alerts_stale(tmp_path, monkeypatch):
    import tacit.alert_ingest as alert_ingest_module
    from tacit.signals import SignalStore

    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_alert(
        "outside-current-page",
        backend_name="grafana",
        alert_title="Still exists on a later page",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )
    monkeypatch.setattr("tacit.signals.get_signal_store", lambda: store)

    class FakeBackend:
        name = "grafana"
        last_alert_list_complete = False

        async def list_alerts(self, limit: int = 500):
            assert limit == 1
            return [{"uid": "current-page", "title": "Current page"}]

        async def ingest_alert(self, uid: str):
            return AlertFeatures(
                alert_uid=uid,
                alert_title="Current page",
                backend_name="grafana",
                query_language="promql",
                condition="A > 1",
                metrics_found=["checkout_request_duration_seconds"],
                query_transformations=['checkout_request_duration_seconds{service="checkout"}'],
            )

        async def close(self):
            return None

    monkeypatch.setattr("tacit.backends.get_active_backends", lambda *_args, **_kwargs: [FakeBackend()])
    service_creations = 0
    original_factory = alert_ingest_module._knowledge_service_for_store

    def counted_factory(*args, **kwargs):
        nonlocal service_creations
        service_creations += 1
        return original_factory(*args, **kwargs)

    monkeypatch.setattr(alert_ingest_module, "_knowledge_service_for_store", counted_factory)

    result = await learn_backend_alerts("grafana", limit=1)
    stale_row = store.get_ingested_alert("outside-current-page", "grafana")

    assert result["stale_marked"] == 0
    assert result["stale_reconciliation_skipped"] is True
    assert result["summary"]["warnings"] == ["stale_reconciliation_skipped_partial_crawl"]
    assert stale_row is not None
    assert stale_row["stale"] is False
    assert service_creations == 1


@pytest.mark.asyncio
async def test_complete_alert_crawl_paginates_all_stale_sources_for_its_backend(tmp_path, monkeypatch):
    from tacit.signals import SignalStore

    store = SignalStore(db_path=tmp_path / "signals.db")
    store.record_ingested_alert(
        "removed-alert",
        backend_name="grafana",
        alert_title="Removed alert",
        fingerprint="abc",
        metrics_found=["checkout_request_duration_seconds"],
    )
    cursors: list[int] = []
    reconciled: list[str] = []

    def list_stale_alerts(*, limit, tenant_id, backend_name, after_id):
        assert limit == 500
        assert tenant_id == "default"
        assert backend_name == "grafana"
        cursors.append(after_id)
        if after_id == 0:
            return [
                {"id": index + 1, "alert_uid": f"stale-{index}", "missing_since": float(index + 1)}
                for index in range(500)
            ]
        if after_id == 500:
            return [{"id": 501, "alert_uid": "stale-final", "missing_since": 501.0}]
        return []

    def reconcile_source(_self, *, provenance_ref, tenant_id, source_stale, source_generation_guard):
        assert tenant_id == "default"
        assert source_stale is True
        reconciled.append(provenance_ref)

    def mark_reconciled(*, tenant_id, backend_name, alert_uid, missing_since):
        assert tenant_id == "default"
        assert backend_name == "grafana"
        assert alert_uid
        assert missing_since > 0
        return True

    monkeypatch.setattr(store, "list_unreconciled_stale_alerts", list_stale_alerts)
    monkeypatch.setattr(store, "mark_alert_knowledge_reconciled", mark_reconciled)
    monkeypatch.setattr("tacit.knowledge.service.KnowledgeService.reconcile_source_lifecycle", reconcile_source)

    class CompleteBackend:
        name = "grafana"
        last_alert_list_complete = True

        async def list_alerts(self, limit=500):
            return []

        async def close(self):
            return None

    monkeypatch.setattr("tacit.backends.get_active_backends", lambda *_args, **_kwargs: [CompleteBackend()])

    result = await learn_backend_alerts("grafana", store=store)

    assert result["stale_marked"] == 1
    assert cursors == [0, 500]
    assert len(reconciled) == 501
    assert reconciled[0] == "grafana:alert:stale-0"
    assert reconciled[-1] == "grafana:alert:stale-final"


@pytest.mark.asyncio
async def test_signalfx_detector_crawl_marks_short_page_complete():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            assert params == {"limit": 10}
            return {"results": [{"id": "detector-1", "name": "Checkout"}]}

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    alerts = await backend.list_alerts(limit=10)

    assert alerts == [{"uid": "detector-1", "title": "Checkout", "backend": "signalfx"}]
    assert backend.last_alert_list_complete is True


@pytest.mark.asyncio
async def test_signalfx_detector_crawl_keeps_paged_response_incomplete():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            assert params == {"limit": 10}
            return {
                "results": [{"id": f"detector-{idx}", "name": f"Detector {idx}"} for idx in range(10)],
                "nextPageLink": "/v2/detector?offset=10",
            }

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    await backend.list_alerts(limit=10)

    assert backend.last_alert_list_complete is False


@pytest.mark.asyncio
async def test_signalfx_detector_crawl_pages_until_limit_or_complete():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            if params == {"limit": 100}:
                return {
                    "results": [{"id": f"detector-{idx}", "name": f"Detector {idx}"} for idx in range(100)],
                    "nextPageLink": "/v2/detector?offset=100",
                }
            assert params == {"limit": 100, "offset": 100}
            return {"results": [{"id": f"detector-{idx}", "name": f"Detector {idx}"} for idx in range(100, 125)]}

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    alerts = await backend.list_alerts(limit=200)

    assert len(alerts) == 125
    assert alerts[0]["uid"] == "detector-0"
    assert alerts[-1]["uid"] == "detector-124"
    assert backend.last_alert_list_complete is True


@pytest.mark.asyncio
async def test_signalfx_detector_exact_limit_complete_snapshot_can_reconcile():
    class FakeSignalFxClient:
        realm = "us1"

        async def _get(self, path: str, params=None):
            assert path == "/v2/detector"
            assert params == {"limit": 2}
            return {
                "count": 2,
                "results": [
                    {"id": "detector-1", "name": "Detector 1"},
                    {"id": "detector-2", "name": "Detector 2"},
                ],
            }

        async def close(self):
            return None

    backend = SignalFxBackend(client=FakeSignalFxClient())

    alerts = await backend.list_alerts(limit=2)

    assert len(alerts) == 2
    assert backend.last_alert_list_complete is True
