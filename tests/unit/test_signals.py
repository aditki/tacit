"""Tests for the semantic signal mapping store, resolution engine, and dashboard ingestion.

Covers both PromQL (Grafana) and SignalFlow (SignalFx) extraction.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from importlib.resources import files
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
import yaml
from structlog.testing import capture_logs

from tacit.archetypes.schema import InvestigationArchetype, PanelTemplate, QueryTemplate
from tacit.backends.base import DashboardFeatures
from tacit.config import Settings
from tacit.dashboard_ingest import (
    approve_ingested_dashboard_record,
    build_learning_impact_report,
    build_signal_quality_report,
    extract_aggregation_patterns,
    extract_metrics_from_promql,
    generate_archetype_yaml,
    ignore_ingested_dashboard_record,
    infer_signals_from_metrics,
    parse_dashboard_json,
    reject_ingested_dashboard_record,
)
from tacit.dashboard_ingest.service import _dashboard_content_fingerprint, persist_inferred_signal_review
from tacit.dashboard_uploads import parse_uploaded_dashboard
from tacit.errors import RuntimeOwnershipError
from tacit.knowledge.migration import migrate_signal_mapping
from tacit.knowledge.models import KnowledgeScope
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.models.schemas import MetricEntry
from tacit.runtime_ownership import RuntimeOwnershipMismatchError, runtime_descriptor_for_store
from tacit.signals import (
    SignalStore,
    _context_matches,
    _effective_confidence,
    _metric_matches_pattern,
)
from tacit.signals.migrations import (
    _DEFAULT_OWNER_MARKER,
    CURRENT_SIGNAL_SCHEMA_MARKER,
    MAPPING_SOURCE_REF_INDEX_MARKER,
    ensure_mapping_columns,
    ensure_mapping_tenant_scope,
    projection_matches_authority,
    reconcile_legacy_signal_schema_batch,
    reconcile_mapping_source_ref_index_batch,
    require_confirmed_default_tenant_owner,
)
from tacit.signals.resolution import (
    ResolutionInputTextLimits,
    ResolutionInputWorkLimitError,
    SignalResolutionWorkBudget,
    SignalResolutionWorkLimitError,
    admit_resolution_input_text,
)
from tacit.signals.schema import GLOBAL_BOOTSTRAP_TENANT_ID
from tacit.sqlite_identity import SQLiteIdentityError
from tacit.tenancy import TenantBoundaryError

_SQLITE_MIN_ID = -(2**63)
_SQLITE_MAX_ID = 2**63 - 1

# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def signal_store(tmp_path):
    """Create a fresh SignalStore with an isolated temp DB."""
    db_path = tmp_path / "test_signals.db"
    store = SignalStore(db_path=db_path)
    return store


def _metric_entry(name: str) -> MetricEntry:
    return MetricEntry(
        name=name,
        datasource_uid="prometheus",
        datasource_name="Prometheus",
        datasource_type="prometheus",
        query_language="promql",
    )


def _replace_signal_mapping_id(store: SignalStore, current_id: int, replacement_id: int) -> None:
    """Re-key one mapping exactly as a legacy migration may preserve it."""
    with store._conn() as conn:
        row = conn.execute(
            "SELECT * FROM signal_metric_mappings WHERE id=?",
            (current_id,),
        ).fetchone()
        assert row is not None
        values = dict(row)
        values["id"] = replacement_id
        columns = tuple(values)
        conn.execute("DELETE FROM signal_metric_mappings WHERE id=?", (current_id,))
        conn.execute(
            f"INSERT INTO signal_metric_mappings ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            tuple(values[column] for column in columns),
        )


def _replace_table_ids(store: SignalStore, table: str, replacement_ids: tuple[int, ...]) -> None:
    assert table in {"ingested_dashboards", "ingested_alerts", "learned_artifacts"}
    with store._conn() as conn:
        current_ids = [int(row["id"]) for row in conn.execute(f"SELECT id FROM {table} ORDER BY id")]
        assert len(current_ids) == len(replacement_ids)
        conn.executemany(
            f"UPDATE {table} SET id=? WHERE id=?",
            zip(replacement_ids, current_ids, strict=True),
        )


def _wildcard_store(store: SignalStore) -> SignalStore:
    return SignalStore(
        db_path=store._db_path,
        runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
    )


def _pinned_store(store: SignalStore, tenant_id: str) -> SignalStore:
    db_path = store._db_path.with_name(f"{store._db_path.stem}-{tenant_id}.db")
    return SignalStore(
        db_path=db_path,
        runtime_settings=Settings(knowledge_tenant_id=tenant_id),
    )


class _DescriptorOnlyStoreAdapter:
    """Expose an injected store through its descriptor and public operations only."""

    def __init__(self, delegate: SignalStore):
        self._delegate = delegate
        self.runtime_settings = delegate.runtime_settings
        self.runtime_ownership = delegate.runtime_ownership
        self.private_accesses: list[str] = []

    def __getattr__(self, name: str):
        if name == "database_path":
            raise AttributeError(name)
        if name.startswith("_"):
            self.private_accesses.append(name)
            raise AssertionError(f"private ownership probe: {name}")
        return getattr(self._delegate, name)


class _DescriptorOnlyKnowledgeServiceAdapter:
    """Expose a knowledge service without its private signal-store accessor."""

    def __init__(self, delegate: KnowledgeService):
        self._delegate = delegate
        self.runtime_settings = delegate.runtime_settings
        self.runtime_ownership = delegate.runtime_ownership
        self.private_accesses: list[str] = []

    def __getattr__(self, name: str):
        if name == "database_path":
            raise AttributeError(name)
        if name.startswith("_"):
            self.private_accesses.append(name)
            raise AssertionError(f"private ownership probe: {name}")
        return getattr(self._delegate, name)


def _descriptor_learning_features(uid: str) -> DashboardFeatures:
    return DashboardFeatures(
        dashboard_uid=uid,
        dashboard_title="Descriptor-owned checkout",
        backend_name="grafana_json",
        query_language="promql",
        metrics_found=["checkout_latency_seconds"],
        panel_count=1,
        panel_titles=["Checkout latency"],
        panels=[
            {
                "title": "Checkout latency",
                "queries": ["checkout_latency_seconds"],
                "metrics": ["checkout_latency_seconds"],
            }
        ],
    )


@pytest.mark.asyncio
async def test_dashboard_learning_uses_descriptor_only_store_without_global_fallback(
    tmp_path,
    monkeypatch,
):
    from tacit.dashboard_ingest.service import ingest_dashboard_features

    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(tmp_path / "signals.db"),
        knowledge_tenant_id="tenant-a",
    )
    store = _DescriptorOnlyStoreAdapter(
        SignalStore(runtime_settings.signals_db_path, runtime_settings=runtime_settings)
    )

    def forbidden_global_store():
        raise AssertionError("descriptor-owned dashboard learning consulted a process-global store")

    monkeypatch.setattr("tacit.dashboard_ingest.service.get_signal_store", forbidden_global_store)
    result = await ingest_dashboard_features(
        _descriptor_learning_features("descriptor-store-dashboard"),
        auto_approve=False,
        runtime_settings=runtime_settings,
        store=store,
        tenant_id="tenant-a",
    )

    assert result["status"] == "pending"
    assert store.private_accesses == []


@pytest.mark.asyncio
async def test_dashboard_learning_resolves_service_owner_without_private_store_or_global_fallback(
    tmp_path,
    monkeypatch,
):
    from tacit.dashboard_ingest.service import ingest_dashboard_features

    database_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(database_path),
        knowledge_tenant_id="tenant-a",
    )
    real_store = SignalStore(database_path, runtime_settings=runtime_settings)
    service = _DescriptorOnlyKnowledgeServiceAdapter(
        KnowledgeService(
            KnowledgeRepository(database_path),
            signal_store=real_store,
            runtime_settings=runtime_settings,
        )
    )

    def forbidden_global_store():
        raise AssertionError("descriptor-owned dashboard learning consulted a process-global store")

    monkeypatch.setattr("tacit.dashboard_ingest.service.get_signal_store", forbidden_global_store)
    result = await ingest_dashboard_features(
        _descriptor_learning_features("descriptor-service-dashboard"),
        auto_approve=False,
        runtime_settings=runtime_settings,
        knowledge_service=service,
        tenant_id="tenant-a",
    )

    assert result["status"] == "pending"
    assert service.private_accesses == []


def test_dashboard_governance_helpers_use_descriptor_database_identity(tmp_path):
    from tacit.dashboard_ingest.service import (
        _active_governed_signal_mapping_ref,
        _existing_governed_candidate_ids,
        reconcile_signal_source,
    )

    database_path = tmp_path / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(database_path),
        knowledge_tenant_id="tenant-a",
    )
    store = _DescriptorOnlyStoreAdapter(SignalStore(database_path, runtime_settings=runtime_settings))

    assert (
        _existing_governed_candidate_ids(
            store=store,
            tenant_id="tenant-a",
            source_ref="grafana:missing",
            active_pairs=set(),
        )
        == set()
    )
    assert (
        _active_governed_signal_mapping_ref(
            store=store,
            candidate_id="missing-candidate",
            tenant_id="tenant-a",
        )
        == ""
    )
    reconcile_signal_source(
        store=store,
        tenant_id="tenant-a",
        source_type="dashboard_ingest",
        source_ref="grafana:missing",
        active_pairs=set(),
        active_candidate_ids=set(),
        runtime_settings=runtime_settings,
    )

    assert store.private_accesses == []


def test_dashboard_governance_scan_preserves_an_empty_candidate_id() -> None:
    from tacit.dashboard_ingest.service import _existing_governed_candidate_ids

    empty_candidate = SimpleNamespace(
        id="",
        evidence=SimpleNamespace(items=[]),
        typed_payload={"metric_pattern": "metric_empty", "signal_type": "latency"},
    )
    later_candidate = SimpleNamespace(
        id="candidate-later",
        evidence=SimpleNamespace(items=[]),
        typed_payload={"metric_pattern": "metric_later", "signal_type": "errors"},
    )

    class Repository:
        def __init__(self) -> None:
            self.boundaries: list[str | None] = []

        def list_candidates_for_provenance(
            self,
            _tenant_id: str,
            _source_ref: str,
            *,
            after_candidate_id: str | None,
            kind: str,
        ) -> list[SimpleNamespace]:
            del kind
            self.boundaries.append(after_candidate_id)
            if after_candidate_id is None:
                return [empty_candidate]
            if after_candidate_id == "":
                return [later_candidate]
            return []

    repository = Repository()
    candidate_ids = _existing_governed_candidate_ids(
        store=object(),
        repository=repository,
        tenant_id="tenant-a",
        source_ref="grafana:legal-empty-key",
        active_pairs={("metric_empty", "latency"), ("metric_later", "errors")},
    )

    assert candidate_ids == {"", "candidate-later"}
    assert repository.boundaries == [None, "", "candidate-later"]


def test_wildcard_signal_store_requires_an_explicit_tenant(signal_store):
    store = _wildcard_store(signal_store)

    operations = [
        lambda: store.list_signal_types(),
        lambda: store.get_signal_type("request_latency"),
        lambda: store.add_mapping("request_latency", "latency_seconds"),
        lambda: store.resolve_signal("request_latency", [_metric_entry("latency_seconds")]),
        lambda: store.search_learning_context("checkout"),
        lambda: store.list_ingested_dashboards(),
        lambda: store.list_learned_artifacts(),
        lambda: store.stats(),
    ]
    for operation in operations:
        with pytest.raises(ValueError, match="tenant"):
            operation()


def test_pinned_signal_store_rejects_cross_tenant_calls(signal_store):
    store = _pinned_store(signal_store, "tenant-a")

    with pytest.raises(ValueError, match="Tenant access denied"):
        store.list_signal_types(tenant_id="tenant-b")
    with pytest.raises(ValueError, match="Tenant access denied"):
        store.add_mapping("request_latency", "latency_seconds", tenant_id="tenant-b")


@pytest.mark.parametrize(
    "operation",
    [
        lambda store, conn: store.register_signal_type("external_signal", connection=conn),
        lambda store, conn: store.add_mapping("request_latency", "external_metric", connection=conn),
        lambda store, conn: store.set_mapping_review_state(
            "request_latency",
            "external_metric",
            "approved",
            connection=conn,
        ),
        lambda store, conn: store.deactivate_governed_mappings(
            tenant_id="default",
            governance_ref="knowledge-external",
            connection=conn,
        ),
        lambda store, conn: store.mark_governed_projection_audit_current(conn),
        lambda store, conn: store.governed_projection_audit_is_current(conn),
    ],
)
def test_signal_store_rejects_external_connections_for_another_database(signal_store, operation):
    with sqlite3.connect(":memory:") as connection:
        with pytest.raises(SQLiteIdentityError, match="same SQLite database"):
            operation(signal_store, connection)


def test_signal_store_external_mutations_require_an_active_transaction(signal_store):
    with signal_store._conn() as connection:
        assert not connection.in_transaction
        with pytest.raises(RuntimeOwnershipError, match="active transaction"):
            signal_store.register_signal_type("external_signal", connection=connection)


def test_signal_store_never_yields_a_connection_when_wal_activation_is_locked(
    signal_store,
    monkeypatch,
):
    def locked(_connection, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr("tacit.signals.store.activate_sqlite_wal", locked)

    yielded = False
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        with signal_store._conn():
            yielded = True
    assert not yielded


def test_pinned_governed_mapping_checks_share_typed_tenant_boundary(tmp_path):
    store = SignalStore(
        db_path=tmp_path / "pinned-governed-mappings.db",
        runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
    )
    mapping = {
        "tenant_id": "tenant-a",
        "signal_type": "request_latency",
        "metric_pattern": "checkout_latency_seconds",
        "governance_ref": "knowledge-request-latency",
        "governance_revision": 3,
    }

    with pytest.raises(TenantBoundaryError) as validation_error:
        store.activate_pinned_governed_mappings(
            tenant_id="tenant-a",
            mappings=[{**mapping, "tenant_id": "tenant-b"}],
        )
    assert validation_error.value.status_code == 403

    token = store.activate_pinned_governed_mappings(tenant_id="tenant-a", mappings=[mapping])
    try:
        with pytest.raises(TenantBoundaryError) as forward_error:
            store.resolve_signal_details(
                "request_latency",
                [_metric_entry("checkout_latency_seconds")],
                tenant_id="tenant-b",
            )
        assert forward_error.value.status_code == 403

        with pytest.raises(TenantBoundaryError) as reverse_error:
            store.resolve_metric_signal_details(
                [_metric_entry("checkout_latency_seconds")],
                tenant_id="tenant-b",
            )
        assert reverse_error.value.status_code == 403
    finally:
        store.reset_pinned_governed_mappings(token)


def test_signal_approval_validates_wildcard_tenant_before_activation(signal_store):
    runtime_settings = signal_store.runtime_settings.model_copy(
        deep=True,
        update={"knowledge_tenant_id": "*", "api_auth_enabled": True},
    )
    wildcard_store = SignalStore(
        signal_store.database_path,
        runtime_settings=runtime_settings,
    )

    with pytest.raises(ValueError, match="tenant_id is required"):
        persist_inferred_signal_review(
            store=wildcard_store,
            runtime_settings=runtime_settings,
            sig={
                "signal_type": "wildcard_tenant_signal",
                "metric": "wildcard_tenant_metric",
                "source": "heuristic",
                "auto_teach_eligible": True,
                "confidence": 0.9,
            },
            source_ref="dashboard:wildcard-tenant",
            dashboard_uid="wildcard-tenant",
        )
    assert wildcard_store.get_signal_type("wildcard_tenant_signal", tenant_id="default") is None


def test_direct_signal_approval_requires_teach_permissions(signal_store):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read",
    )
    restricted_store = SignalStore(
        db_path=signal_store._db_path,
        runtime_settings=runtime_settings,
    )

    with pytest.raises(PermissionError, match="knowledge.review"):
        persist_inferred_signal_review(
            store=restricted_store,
            sig={
                "signal_type": "protected_signal",
                "metric": "protected_metric",
                "source": "heuristic",
                "auto_teach_eligible": True,
                "confidence": 0.9,
            },
            source_ref="dashboard:protected",
            dashboard_uid="protected",
        )

    assert restricted_store.get_signal_type("protected_signal") is None


@pytest.mark.asyncio
async def test_direct_dashboard_auto_approval_requires_teach_permissions(tmp_path):
    from tacit.dashboard_ingest.service import ingest_dashboard_features

    runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.read",
    )
    restricted_store = SignalStore(
        db_path=tmp_path / "restricted-dashboard.db",
        runtime_settings=runtime_settings,
    )

    with pytest.raises(PermissionError, match="knowledge.review"):
        await ingest_dashboard_features(
            object(),
            auto_approve=True,
            store=restricted_store,
            runtime_settings=runtime_settings,
        )


@pytest.mark.asyncio
async def test_dashboard_ingestion_rejects_split_runtime_before_processing_features(tmp_path):
    from tacit.dashboard_ingest.service import ingest_dashboard_features

    store_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    store = SignalStore(tmp_path / "split-dashboard.db", runtime_settings=store_settings)
    explicit_settings = store_settings.model_copy(update={"knowledge_tenant_id": "tenant-b"})

    with pytest.raises(ValueError, match="runtime settings must match"):
        await ingest_dashboard_features(
            object(),
            store=store,
            runtime_settings=explicit_settings,
            tenant_id="tenant-b",
        )

    assert store.list_ingested_dashboards(tenant_id="tenant-a") == []


@pytest.mark.asyncio
async def test_dashboard_ingestion_requires_apply_before_persistence(tmp_path):
    from tacit.dashboard_ingest.service import ingest_dashboard_features

    restricted_store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review",
        ),
    )
    features = DashboardFeatures(
        dashboard_uid="protected-pending-dashboard",
        dashboard_title="Protected pending dashboard",
        backend_name="grafana",
        query_language="promql",
        metrics_found=["protected_metric"],
        panel_count=1,
        panels=[],
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        await ingest_dashboard_features(
            features,
            auto_approve=False,
            store=restricted_store,
        )

    assert restricted_store.list_ingested_dashboards(tenant_id="default") == []


@pytest.mark.asyncio
async def test_bulk_dashboard_learning_authorizes_before_backend_access(tmp_path, monkeypatch):
    from tacit.dashboard_ingest.service import learn_backend_dashboards

    restricted_store = SignalStore(
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
        await learn_backend_dashboards("grafana", store=restricted_store)

    assert backend_accessed is False


@pytest.mark.asyncio
async def test_direct_dashboard_ingestion_authorizes_before_backend_access(tmp_path):
    from tacit.dashboard_ingest.service import ingest_dashboard

    restricted_store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.review",
        ),
    )

    class Backend:
        accessed = False

        async def ingest_dashboard(self, _dashboard_uid):
            self.accessed = True
            raise AssertionError("unauthorized ingestion accessed the backend")

    backend = Backend()
    with pytest.raises(PermissionError, match="Missing permission: knowledge.apply"):
        await ingest_dashboard("protected-dashboard", backend=backend, store=restricted_store)

    assert backend.accessed is False


@pytest.mark.asyncio
async def test_direct_dashboard_ingestion_requires_read_before_backend_or_store(tmp_path):
    from tacit.dashboard_ingest.service import ingest_dashboard

    db_path = tmp_path / "read-protected-signals.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.apply",
        signals_db_path=str(db_path),
    )

    class Backend:
        accessed = False

        async def ingest_dashboard(self, _dashboard_uid):
            self.accessed = True
            raise AssertionError("read-denied ingestion accessed the backend")

    backend = Backend()
    with pytest.raises(PermissionError, match="Missing permission: knowledge.read"):
        await ingest_dashboard(
            "read-protected-dashboard",
            backend=backend,
            runtime_settings=runtime_settings,
        )

    assert backend.accessed is False
    assert not db_path.exists()


def test_dashboard_review_transitions_use_injected_store_permissions(tmp_path):
    restricted_store = SignalStore(
        db_path=tmp_path / "signals.db",
        runtime_settings=Settings(
            _env_file=None,
            knowledge_permissions="knowledge.read,knowledge.apply",
        ),
    )

    with pytest.raises(PermissionError, match="Missing permission: knowledge.review"):
        approve_ingested_dashboard_record(
            dashboard_uid="protected-dashboard",
            store=restricted_store,
        )
    with pytest.raises(PermissionError, match="Missing permission: knowledge.reject"):
        reject_ingested_dashboard_record(
            dashboard_uid="protected-dashboard",
            store=restricted_store,
        )


@pytest.mark.parametrize(
    "transition",
    [
        approve_ingested_dashboard_record,
        reject_ingested_dashboard_record,
        ignore_ingested_dashboard_record,
    ],
)
def test_direct_dashboard_review_rejects_tenant_before_store_initialization(tmp_path, transition):
    db_path = tmp_path / "tenant-denied-signals.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        signals_db_path=str(db_path),
    )

    with pytest.raises(ValueError, match="Tenant access denied"):
        transition(
            dashboard_uid="cross-tenant-dashboard",
            runtime_settings=runtime_settings,
            tenant_id="tenant-b",
        )

    assert not db_path.exists()


def test_signal_approval_enforces_supplied_runtime_tenant(tmp_path):
    runtime_settings = Settings(knowledge_tenant_id="tenant-a")
    scoped_store = SignalStore(tmp_path / "tenant-a-signals.db", runtime_settings=runtime_settings)

    with pytest.raises(ValueError, match="Tenant access denied"):
        persist_inferred_signal_review(
            store=scoped_store,
            sig={
                "signal_type": "cross_tenant_signal",
                "metric": "cross_tenant_metric",
                "source": "heuristic",
                "auto_teach_eligible": True,
                "confidence": 0.9,
            },
            source_ref="dashboard:cross-tenant",
            dashboard_uid="cross-tenant",
            tenant_id="tenant-b",
            runtime_settings=runtime_settings,
        )

    with scoped_store._conn() as conn:
        assert conn.execute("""SELECT 1 FROM tenant_signal_types
               WHERE tenant_id='tenant-b' AND signal_type='cross_tenant_signal'""").fetchone() is None


def test_signal_mapping_resolution_is_tenant_scoped(signal_store, monkeypatch):
    signal_store = _wildcard_store(signal_store)
    signal_store.add_mapping(
        "request_latency",
        "tenant_a_latency_seconds",
        confidence=0.9,
        tenant_id="tenant-a",
    )
    signal_store.add_mapping(
        "request_latency",
        "tenant_b_latency_seconds",
        confidence=0.9,
        tenant_id="tenant-b",
    )
    catalog = [
        _metric_entry("tenant_a_latency_seconds"),
        _metric_entry("tenant_b_latency_seconds"),
    ]

    tenant_a = signal_store.resolve_signal("request_latency", catalog, tenant_id="tenant-a")
    tenant_b = signal_store.resolve_signal("request_latency", catalog, tenant_id="tenant-b")

    assert [entry.name for entry, _ in tenant_a] == ["tenant_a_latency_seconds"]
    assert [entry.name for entry, _ in tenant_b] == ["tenant_b_latency_seconds"]

    import tacit.dashboard_ingest as dashboard_ingest

    monkeypatch.setattr(dashboard_ingest, "get_signal_store", lambda: signal_store)
    inferred = infer_signals_from_metrics(
        ["tenant_a_latency_seconds", "tenant_b_latency_seconds"],
        tenant_id="tenant-a",
    )
    assert {(row["signal_type"], row["metric"]) for row in inferred if row["source"] == "taxonomy"} == {
        ("request_latency", "tenant_a_latency_seconds")
    }


def test_signal_inference_uses_one_bounded_reverse_resolution(signal_store, monkeypatch):
    signal_store.add_mapping(
        "request_latency",
        "checkout_latency_seconds",
        confidence=0.9,
    )
    calls: list[list[str]] = []
    resolve_metric_signals = signal_store.resolve_metric_signal_details

    def tracked_resolve_metric_signals(catalog, **kwargs):
        calls.append([entry.name for entry in catalog])
        return resolve_metric_signals(catalog, **kwargs)

    monkeypatch.setattr(signal_store, "resolve_metric_signal_details", tracked_resolve_metric_signals)
    metrics = ["checkout_latency_seconds", *[f"unmatched_metric_{index}" for index in range(20)]]
    with capture_logs() as logs:
        inferred = infer_signals_from_metrics(metrics, store=signal_store)

    assert any(row["metric"] == "checkout_latency_seconds" for row in inferred)
    assert calls == [metrics]
    scan = next(entry for entry in logs if entry["event"] == "signal_inference_taxonomy_scan")
    assert scan["mapping_lookup_count"] == 1
    assert scan["metric_count"] == len(metrics)


def test_signal_inference_preserves_and_enforces_panel_datasource_scope(signal_store):
    signal_store.register_signal_type(
        "cloudwatch_target_latency",
        description="CloudWatch target latency",
        category="latency",
    )
    signal_store.add_mapping(
        "cloudwatch_target_latency",
        "SharedLatencyMetric",
        confidence=0.9,
        context_datasource_types=["cloudwatch"],
        governance_ref="knowledge-cloudwatch-latency",
        governance_revision=1,
        review_state="trusted",
    )
    prometheus_panel = {
        "metrics": ["SharedLatencyMetric"],
        "datasource_type": "prometheus",
        "query_language": "promql",
    }
    cloudwatch_panel = {
        "metrics": ["SharedLatencyMetric"],
        "datasource_type": "cloudwatch",
        "query_language": "cloudwatch",
    }

    prometheus = infer_signals_from_metrics(
        ["SharedLatencyMetric"],
        [prometheus_panel],
        store=signal_store,
    )
    cloudwatch = infer_signals_from_metrics(
        ["SharedLatencyMetric"],
        [cloudwatch_panel],
        store=signal_store,
    )

    assert not any(
        row["source"] == "taxonomy" and row["signal_type"] == "cloudwatch_target_latency" for row in prometheus
    )
    taxonomy_match = next(
        row for row in cloudwatch if row["source"] == "taxonomy" and row["signal_type"] == "cloudwatch_target_latency"
    )
    assert taxonomy_match["datasource_types"] == ["cloudwatch"]
    assert taxonomy_match["query_languages"] == ["cloudwatch"]

    knowledge_service = KnowledgeService(
        KnowledgeRepository(signal_store._db_path),
        signal_store=signal_store,
        runtime_settings=signal_store._settings,
    )
    persist_inferred_signal_review(
        store=signal_store,
        sig=taxonomy_match,
        source_ref="grafana:cloudwatch-dashboard",
        dashboard_uid="cloudwatch-dashboard",
        source_fingerprint="cloudwatch-dashboard-fingerprint",
        knowledge_service=knowledge_service,
    )
    governed = next(
        candidate
        for candidate in knowledge_service.repository.list_candidates("default", kind="signal_mapping")
        if candidate.typed_payload.get("metric_pattern") == "SharedLatencyMetric"
    )
    assert governed.typed_payload["context_datasource_types"] == ["cloudwatch"]


def test_dashboard_content_fingerprint_includes_inferred_datasource_scope():
    base = {
        "metrics_found": ["shared_metric"],
        "signals_inferred": [
            {
                "signal_type": "request_latency",
                "metric": "shared_metric",
                "datasource_types": ["prometheus"],
                "query_languages": ["promql"],
            }
        ],
    }
    changed = {
        **base,
        "signals_inferred": [
            {
                **base["signals_inferred"][0],
                "datasource_types": ["mimir"],
            }
        ],
    }

    assert _dashboard_content_fingerprint(base) != _dashboard_content_fingerprint(changed)


def test_reverse_resolution_preserves_same_pattern_datasource_variants(signal_store):
    signal_store.register_signal_type(
        "shared_latency",
        description="Shared latency",
        category="latency",
    )
    signal_store.add_mapping(
        "shared_latency",
        "SharedLatencyMetric",
        confidence=0.9,
        context_datasource_types=["prometheus"],
        governance_ref="knowledge-prometheus-latency",
        governance_revision=1,
        review_state="trusted",
    )
    signal_store.add_mapping(
        "shared_latency",
        "SharedLatencyMetric",
        confidence=0.9,
        context_datasource_types=["cloudwatch"],
        governance_ref="knowledge-cloudwatch-latency",
        governance_revision=1,
        review_state="trusted",
    )
    catalog = [
        MetricEntry(
            name="SharedLatencyMetric",
            datasource_uid="prom",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="SharedLatencyMetric",
            datasource_uid="cloudwatch",
            datasource_name="CloudWatch",
            datasource_type="cloudwatch",
            query_language="cloudwatch",
        ),
    ]

    resolved = signal_store.resolve_metric_signal_details(catalog)

    assert {(item.entry.datasource_type, item.governance_ref) for item in resolved} == {
        ("prometheus", "knowledge-prometheus-latency"),
        ("cloudwatch", "knowledge-cloudwatch-latency"),
    }


def test_forward_resolution_preserves_same_pattern_datasource_variants(signal_store):
    signal_store.register_signal_type("shared_latency", description="Shared latency", category="latency")
    for datasource_type in ("prometheus", "cloudwatch"):
        signal_store.add_mapping(
            "shared_latency",
            "SharedLatencyMetric",
            confidence=0.9,
            context_datasource_types=[datasource_type],
            governance_ref=f"knowledge-{datasource_type}-latency",
            governance_revision=1,
            review_state="trusted",
        )
    catalog = [
        MetricEntry(
            name="SharedLatencyMetric",
            datasource_uid=datasource_type,
            datasource_name=datasource_type,
            datasource_type=datasource_type,
            query_language=query_language,
        )
        for datasource_type, query_language in (("prometheus", "promql"), ("cloudwatch", "cloudwatch"))
    ]

    prometheus = signal_store.resolve_signal_details(
        "shared_latency",
        catalog,
        target_query_language="promql",
    )
    cloudwatch = signal_store.resolve_signal_details(
        "shared_latency",
        catalog,
        target_query_language="cloudwatch",
    )

    assert [(item.entry.datasource_type, item.governance_ref) for item in prometheus] == [
        ("prometheus", "knowledge-prometheus-latency")
    ]
    assert [(item.entry.datasource_type, item.governance_ref) for item in cloudwatch] == [
        ("cloudwatch", "knowledge-cloudwatch-latency")
    ]


@pytest.mark.parametrize(
    ("query_language", "datasource_type"),
    [("promql", "mimir"), ("promql", "cortex"), ("promql", "thanos"), ("lucene", "opensearch")],
)
def test_forward_resolution_uses_catalog_datasource_within_query_language_family(
    signal_store,
    query_language,
    datasource_type,
):
    signal_store.register_signal_type("scoped_signal", description="Scoped signal", category="latency")
    signal_store.add_mapping(
        "scoped_signal",
        "scoped_metric",
        confidence=0.9,
        context_datasource_types=[datasource_type],
        governance_ref=f"knowledge-{datasource_type}",
        governance_revision=1,
        review_state="trusted",
    )
    catalog = [
        MetricEntry(
            name="scoped_metric",
            datasource_uid=datasource_type,
            datasource_name=datasource_type,
            datasource_type=datasource_type,
            query_language=query_language,
        )
    ]

    resolved = signal_store.resolve_signal_details(
        "scoped_signal",
        catalog,
        target_query_language=query_language,
    )

    assert [(item.entry.datasource_type, item.governance_ref) for item in resolved] == [
        (datasource_type, f"knowledge-{datasource_type}")
    ]


def test_bootstrap_signal_mappings_are_available_to_every_tenant(signal_store):
    signal_store = _wildcard_store(signal_store)
    signal_store._add_bootstrap_mapping(
        "request_latency",
        "http_request_duration_seconds",
        confidence=0.9,
    )
    catalog = [_metric_entry("http_request_duration_seconds")]

    assert signal_store.resolve_signal("request_latency", catalog, tenant_id="tenant-a")
    assert signal_store.resolve_signal("request_latency", catalog, tenant_id="tenant-b")


def test_public_mapping_write_cannot_create_global_bootstrap_authority(signal_store):
    signal_store = _wildcard_store(signal_store)

    with pytest.raises(PermissionError, match="packaged catalog loader"):
        signal_store.add_mapping(
            "request_latency",
            "tenant_supplied_latency_seconds",
            confidence=0.9,
            source_type="bootstrap",
            tenant_id="tenant-a",
        )

    assert signal_store.get_mappings_for_signal("request_latency", tenant_id="tenant-b") == []


def test_default_tenant_mapping_cannot_mutate_global_bootstrap(signal_store):
    signal_store = _wildcard_store(signal_store)
    signal_store._add_bootstrap_mapping(
        "request_latency",
        "http_request_duration_seconds",
        confidence=0.9,
    )
    signal_store.add_mapping(
        "request_latency",
        "http_request_duration_seconds",
        confidence=0.95,
        source_type="teach",
        review_state="approved",
        tenant_id="default",
    )

    with signal_store._conn() as conn:
        rows = conn.execute("""SELECT tenant_id, source_type FROM signal_metric_mappings
               WHERE signal_type='request_latency' AND metric_pattern='http_request_duration_seconds'
               ORDER BY tenant_id""").fetchall()

    assert {(row["tenant_id"], row["source_type"]) for row in rows} == {
        (GLOBAL_BOOTSTRAP_TENANT_ID, "bootstrap"),
        ("default", "teach"),
    }
    assert signal_store.get_mappings_for_signal("request_latency", tenant_id="default")[0]["source_type"] == "teach"

    signal_store.set_mapping_review_state(
        "request_latency",
        "http_request_duration_seconds",
        "candidate",
        tenant_id="default",
    )

    tenant_b = signal_store.get_mappings_for_signal("request_latency", tenant_id="tenant-b")
    assert len(tenant_b) == 1
    assert tenant_b[0]["source_type"] == "bootstrap"


def test_context_rejected_tenant_override_does_not_hide_bootstrap_mapping(signal_store):
    signal_store = _wildcard_store(signal_store)
    signal_store._add_bootstrap_mapping(
        "request_latency",
        "http_request_duration_seconds",
        confidence=0.9,
    )
    signal_store.add_mapping(
        "request_latency",
        "http_request_duration_seconds",
        confidence=0.95,
        source_type="teach",
        context_services=["checkout"],
        tenant_id="tenant-a",
    )

    mappings = signal_store.get_mappings_for_signal(
        "request_latency",
        context_service="payments",
        tenant_id="tenant-a",
    )

    assert len(mappings) == 1
    assert mappings[0]["source_type"] == "bootstrap"


def test_stale_alert_removes_only_its_signal_mapping_provenance(signal_store):
    signal_store = _wildcard_store(signal_store)
    for alert_uid in ("checkout-latency", "payments-latency"):
        signal_store.record_ingested_alert(
            alert_uid,
            tenant_id="tenant-a",
            backend_name="grafana",
            status="approved",
        )
    signal_store.add_mapping(
        "request_latency",
        "service_latency_seconds",
        confidence=0.9,
        source_type="alert_ingest",
        source_refs=["grafana:alert:checkout-latency", "grafana:alert:payments-latency"],
        tenant_id="tenant-a",
    )
    signal_store.add_mapping(
        "checkout_latency",
        "checkout_latency_seconds",
        confidence=0.9,
        source_type="alert_ingest",
        source_refs=["grafana:alert:checkout-latency"],
        tenant_id="tenant-a",
    )

    signal_store.mark_missing_alerts_stale(
        tenant_id="tenant-a",
        backend_name="grafana",
        seen_alert_uids={"payments-latency"},
        authority_reconciler=lambda _conn, _source: None,
    )

    mappings = signal_store.get_mappings_for_signal(
        "request_latency",
        include_decayed=True,
        tenant_id="tenant-a",
    )
    assert mappings[0]["source_refs"] == ["grafana:alert:payments-latency"]
    assert (
        signal_store.get_mappings_for_signal(
            "checkout_latency",
            include_decayed=True,
            tenant_id="tenant-a",
        )
        == []
    )


def test_mapping_reconciliation_checks_all_provenance_refs(signal_store):
    signal_store = _wildcard_store(signal_store)
    signal_store.add_mapping(
        "request_latency",
        "service_latency_seconds",
        confidence=0.9,
        source_type="dashboard_ingest",
        source_refs=["grafana:dashboard:checkout"],
        tenant_id="tenant-a",
    )
    signal_store.add_mapping(
        "request_latency",
        "service_latency_seconds",
        confidence=0.9,
        source_type="alert_ingest",
        source_refs=["grafana:alert:checkout"],
        tenant_id="tenant-a",
    )

    signal_store.reconcile_mapping_source(
        tenant_id="tenant-a",
        source_type="dashboard_ingest",
        source_ref="grafana:dashboard:checkout",
        active_pairs=set(),
    )

    mappings = signal_store.get_mappings_for_signal(
        "request_latency",
        include_decayed=True,
        tenant_id="tenant-a",
    )
    assert mappings[0]["source_refs"] == ["grafana:alert:checkout"]

    with signal_store._conn() as conn:
        plan = conn.execute(
            """EXPLAIN QUERY PLAN
               SELECT mapping.id
               FROM signal_mapping_source_refs source
               JOIN signal_metric_mappings mapping ON mapping.id=source.mapping_id
               WHERE source.tenant_id=? AND source.source_ref=?
                 AND mapping.governance_ref=''""",
            ("tenant-a", "grafana:alert:checkout"),
        ).fetchall()
    assert any("idx_signal_mapping_source_ref" in str(row[3]) for row in plan)


def test_rejected_signal_candidates_are_tenant_scoped(signal_store):
    signal_store = _wildcard_store(signal_store)
    signal_store.record_rejected_candidate("tenant_a_metric", tenant_id="tenant-a")
    signal_store.record_rejected_candidate("tenant_b_metric", tenant_id="tenant-b")

    assert [item["metric"] for item in signal_store.list_rejected_candidates(tenant_id="tenant-a")] == [
        "tenant_a_metric"
    ]
    assert [item["metric"] for item in signal_store.list_rejected_candidates(tenant_id="tenant-b")] == [
        "tenant_b_metric"
    ]


def test_ingested_source_lists_filter_by_backend_before_pagination(signal_store):
    for uid, backend in (
        ("grafana-old", "grafana"),
        ("signalfx-new", "signalfx"),
        ("grafana-new", "grafana"),
    ):
        signal_store.record_ingested_dashboard(uid, backend_name=backend, status="stale")
        signal_store.record_ingested_alert(uid, backend_name=backend, status="stale")

    dashboard_page = signal_store.list_ingested_dashboards(
        status="stale",
        limit=1,
        tenant_id="default",
        backend_name="grafana",
        offset=1,
    )
    alert_page = signal_store.list_ingested_alerts(
        status="stale",
        limit=1,
        tenant_id="default",
        backend_name="grafana",
        offset=1,
    )

    assert [row["dashboard_uid"] for row in dashboard_page] == ["grafana-old"]
    assert [row["alert_uid"] for row in alert_page] == ["grafana-old"]


def test_ingested_source_lists_use_stable_cursors_and_bounded_limits(signal_store):
    for index in range(5):
        uid = f"source-{index}"
        signal_store.record_ingested_dashboard(uid, backend_name="grafana", status="pending")
        signal_store.record_ingested_alert(uid, backend_name="grafana", status="pending")
    with signal_store._conn() as conn:
        conn.execute("UPDATE ingested_dashboards SET created_at=100.0")
        conn.execute("UPDATE ingested_alerts SET created_at=100.0")

    dashboard_first = signal_store.list_ingested_dashboards(limit=2)
    dashboard_second = signal_store.list_ingested_dashboards(
        limit=3,
        before_created_at=dashboard_first[-1]["created_at"],
        before_id=dashboard_first[-1]["id"],
    )
    alert_first = signal_store.list_ingested_alerts(limit=2)
    alert_second = signal_store.list_ingested_alerts(
        limit=3,
        before_created_at=alert_first[-1]["created_at"],
        before_id=alert_first[-1]["id"],
    )

    dashboard_ids = [row["id"] for row in [*dashboard_first, *dashboard_second]]
    alert_ids = [row["id"] for row in [*alert_first, *alert_second]]
    assert dashboard_ids == sorted(dashboard_ids, reverse=True)
    assert alert_ids == sorted(alert_ids, reverse=True)
    assert len(set(dashboard_ids)) == len(set(alert_ids)) == 5
    for invalid_limit in (-1, 0, 10_001):
        with pytest.raises(ValueError, match="limit"):
            signal_store.list_ingested_dashboards(limit=invalid_limit)
        with pytest.raises(ValueError, match="limit"):
            signal_store.list_ingested_alerts(limit=invalid_limit)
    with pytest.raises(ValueError, match="supplied together"):
        signal_store.list_ingested_dashboards(before_created_at=100.0)


def test_source_lifecycle_stale_scans_cover_every_sqlite_integer_id(
    signal_store,
    monkeypatch,
):
    replacement_ids = (_SQLITE_MIN_ID, -41, 0, 100_003, _SQLITE_MAX_ID)
    for index in range(len(replacement_ids)):
        signal_store.record_ingested_dashboard(f"dashboard-{index}", backend_name="grafana")
        signal_store.record_ingested_alert(
            f"alert-{index}",
            backend_name="grafana",
            fingerprint=f"alert-fingerprint-{index}",
        )
        signal_store.record_learned_artifact(
            artifact_id=f"artifact-{index}",
            artifact_type="runbook",
            fingerprint=f"artifact-fingerprint-{index}",
        )
    for table in ("ingested_dashboards", "ingested_alerts", "learned_artifacts"):
        _replace_table_ids(signal_store, table, replacement_ids)

    monkeypatch.setattr("tacit.signals.store._STALE_SOURCE_PAGE_SIZE", 1)
    dashboard_generations: list[int] = []
    alert_generations: list[int] = []
    artifact_generations: list[int] = []

    assert signal_store.mark_missing_dashboards_stale(
        backend_name="grafana",
        seen_dashboard_uids=set(),
        crawl_started_at=time.time() + 1,
        authority_reconciler=lambda _conn, source: dashboard_generations.append(int(source["id"])),
    ) == len(replacement_ids)
    assert signal_store.mark_missing_alerts_stale(
        backend_name="grafana",
        seen_alert_uids=set(),
        crawl_started_at=time.time() + 1,
        authority_reconciler=lambda _conn, source: alert_generations.append(int(source["id"])),
    ) == len(replacement_ids)
    assert signal_store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        crawl_started_at=time.time() + 1,
        authority_reconciler=lambda _conn, source: artifact_generations.append(int(source["id"])),
    ) == len(replacement_ids)
    assert dashboard_generations == alert_generations == artifact_generations == list(replacement_ids)


def test_unreconciled_source_pages_cover_every_sqlite_integer_id(signal_store):
    replacement_ids = (_SQLITE_MIN_ID, -19, 0, 901_003, _SQLITE_MAX_ID)
    for index in range(len(replacement_ids)):
        signal_store.record_ingested_dashboard(f"dashboard-{index}", backend_name="grafana")
        signal_store.record_ingested_alert(
            f"alert-{index}",
            backend_name="grafana",
            fingerprint=f"alert-fingerprint-{index}",
        )
        signal_store.record_learned_artifact(
            artifact_id=f"artifact-{index}",
            artifact_type="runbook",
            fingerprint=f"artifact-fingerprint-{index}",
        )
    for table in ("ingested_dashboards", "ingested_alerts", "learned_artifacts"):
        _replace_table_ids(signal_store, table, replacement_ids)
    with signal_store._conn() as conn:
        for table in ("ingested_dashboards", "ingested_alerts", "learned_artifacts"):
            conn.execute(f"UPDATE {table} SET stale=1, missing_since=123.0, knowledge_reconciled_at=NULL")

    page_specs = (
        (signal_store.list_unreconciled_stale_dashboards, {"backend_name": "grafana"}),
        (signal_store.list_unreconciled_stale_alerts, {"backend_name": "grafana"}),
        (signal_store.list_unreconciled_stale_artifacts, {"artifact_type": "runbook"}),
    )
    for list_page, kwargs in page_specs:
        after_id: int | None = None
        observed: list[int] = []
        while True:
            rows = list_page(limit=1, after_id=after_id, **kwargs)
            if not rows:
                break
            observed.append(int(rows[0]["id"]))
            after_id = int(rows[-1]["id"])
        assert observed == list(replacement_ids)


def test_artifact_and_extraction_cursors_preserve_boundary_and_empty_keys(signal_store):
    replacement_ids = (_SQLITE_MIN_ID, -7, 0, 700_003, _SQLITE_MAX_ID)
    artifact_ids = [f"artifact-{index}" for index in range(len(replacement_ids))]
    for artifact_id in artifact_ids:
        signal_store.record_learned_artifact(
            artifact_id=artifact_id,
            artifact_type="runbook",
            fingerprint=f"fingerprint:{artifact_id}",
        )
    _replace_table_ids(signal_store, "learned_artifacts", replacement_ids)
    with signal_store._conn() as conn:
        conn.execute("UPDATE learned_artifacts SET updated_at=100.0")

    cursor = None
    observed_ids: list[int] = []
    while True:
        page = signal_store.list_learned_artifacts_page(
            artifact_type="runbook",
            limit=1,
            cursor=cursor,
        )
        observed_ids.extend(int(item["id"]) for item in page.items)
        if not page.has_more:
            break
        cursor = page.next_cursor
    assert observed_ids == list(reversed(replacement_ids))

    extraction_artifact = artifact_ids[0]
    with signal_store._conn() as conn:
        conn.executemany(
            """INSERT INTO evidence_requirements
               (tenant_id, id, artifact_id, subject, evidence_kind, created_at)
               VALUES ('default', ?, ?, 'subject', 'metric', 1.0)""",
            [("", extraction_artifact), ("later-extraction", extraction_artifact)],
        )

    first = signal_store.list_artifact_extraction_page(
        extraction_artifact,
        extraction_kind="evidence_requirements",
        limit=1,
    )
    assert [item["id"] for item in first.items] == [""]
    assert first.next_cursor
    second = signal_store.list_artifact_extraction_page(
        extraction_artifact,
        extraction_kind="evidence_requirements",
        limit=1,
        cursor=first.next_cursor,
    )
    assert [item["id"] for item in second.items] == ["later-extraction"]


def test_source_lifecycle_and_public_pages_use_bounded_indexes(signal_store):
    with signal_store._conn() as conn:
        plans = {
            "dashboard_page": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM ingested_dashboards
                   INDEXED BY idx_ingested_dashboard_status_page
                   WHERE tenant_id=? AND status=?
                     AND (created_at < ? OR (created_at = ? AND id < ?))
                   ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
                ("default", "pending", 100.0, 100.0, 0, 51, 0),
            ).fetchall(),
            "alert_page": conn.execute(
                """EXPLAIN QUERY PLAN SELECT * FROM ingested_alerts
                   INDEXED BY idx_ingested_alert_status_page
                   WHERE tenant_id=? AND status=?
                     AND (created_at < ? OR (created_at = ? AND id < ?))
                   ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?""",
                ("default", "pending", 100.0, 100.0, 0, 51, 0),
            ).fetchall(),
            "artifact_page": conn.execute(
                """EXPLAIN QUERY PLAN SELECT id FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_type=?
                     AND (updated_at < ? OR (updated_at = ? AND id < ?))
                   ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?""",
                ("default", "runbook", 100.0, 100.0, 0, 51, 0),
            ).fetchall(),
            "artifact_stale": conn.execute(
                """EXPLAIN QUERY PLAN SELECT id FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_type=? AND stale=0
                     AND id>? AND last_seen_at<=? ORDER BY id LIMIT ?""",
                ("default", "runbook", 0, 100.0, 500),
            ).fetchall(),
            "artifact_reconciliation": conn.execute(
                """EXPLAIN QUERY PLAN SELECT id FROM learned_artifacts
                   WHERE tenant_id=? AND artifact_type=? AND stale=1
                     AND knowledge_reconciled_at IS NULL AND id>?
                   ORDER BY id LIMIT ?""",
                ("default", "runbook", 0, 500),
            ).fetchall(),
            "extraction_page": conn.execute(
                """EXPLAIN QUERY PLAN SELECT id FROM evidence_requirements
                   WHERE tenant_id=? AND artifact_id=? AND id>?
                   ORDER BY id LIMIT ?""",
                ("default", "artifact", "", 201),
            ).fetchall(),
        }

    expected_indexes = {
        "dashboard_page": "idx_ingested_dashboard_status_page",
        "alert_page": "idx_ingested_alert_status_page",
        "artifact_page": "idx_learned_artifacts_page",
        "artifact_stale": "idx_learned_artifact_stale_scan",
        "artifact_reconciliation": "idx_learned_artifact_reconciliation",
        "extraction_page": "idx_evidence_requirements_artifact_page",
    }
    for name, plan in plans.items():
        details = [str(row["detail"]) for row in plan]
        assert any(expected_indexes[name] in detail for detail in details), (name, details)
        assert not any("TEMP B-TREE" in detail for detail in details), (name, details)


def test_mapping_tenant_rebuild_rolls_back_on_copy_failure(tmp_path):
    db_path = tmp_path / "failed-mapping-migration.db"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY, signal_type TEXT, metric_pattern TEXT,
                confidence REAL, context_services TEXT, context_datasource_types TEXT,
                context_environments TEXT, context_archetypes TEXT, source_type TEXT,
                source_refs TEXT, inference_version TEXT, review_state TEXT, use_count INTEGER,
                positive_feedback INTEGER, negative_feedback INTEGER, created_at REAL, last_seen REAL,
                UNIQUE(signal_type, metric_pattern)
            );
            INSERT INTO signal_metric_mappings VALUES
                (1, 'latency', 'broken_metric', NULL, '[]', '[]', '[]', '[]',
                 'teach', '[]', '', 'trusted', 0, 0, 0, 1, 1);
        """)

        conn.execute("""CREATE TABLE signal_tenant_migration_metadata (
                          key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
                      )""")
        conn.execute("""INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
                      VALUES ('legacy_schema_owner_v1', 'default', 1)""")
        ensure_mapping_columns(conn)
        ensure_mapping_tenant_scope(conn)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        with pytest.raises(sqlite3.IntegrityError):
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                reconcile_legacy_signal_schema_batch(conn, legacy_tenant="default", batch_size=10)

        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)")}
        shadow_rows = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[
            0
        ]
        cursor = conn.execute("""SELECT 1 FROM signal_tenant_migration_metadata
               WHERE key='legacy_schema_copy_cursor_v1:signal_metric_mappings'""").fetchone()
        row = conn.execute("SELECT confidence FROM signal_metric_mappings WHERE id=1").fetchone()
        tables = {entry[0] for entry in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    assert "tenant_id" not in columns
    assert shadow_rows == 0
    assert cursor is None
    assert "tacit_runtime_database_identity" not in tables
    assert row["confidence"] is None


def test_mapping_schema_migrates_governed_projection_identity(tmp_path):
    db_path = tmp_path / "projection-identity-migration.db"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY, tenant_id TEXT NOT NULL, signal_type TEXT NOT NULL,
                metric_pattern TEXT NOT NULL, confidence REAL NOT NULL,
                context_services TEXT NOT NULL, context_datasource_types TEXT NOT NULL,
                context_environments TEXT NOT NULL, context_archetypes TEXT NOT NULL,
                source_type TEXT NOT NULL, source_refs TEXT NOT NULL,
                governance_ref TEXT NOT NULL, governance_revision INTEGER NOT NULL,
                inference_version TEXT NOT NULL, review_state TEXT NOT NULL, use_count INTEGER NOT NULL,
                positive_feedback INTEGER NOT NULL, negative_feedback INTEGER NOT NULL,
                created_at REAL NOT NULL, last_seen REAL NOT NULL,
                UNIQUE(tenant_id, signal_type, metric_pattern, governance_ref)
            );
            INSERT INTO signal_metric_mappings VALUES
                (1, 'default', 'request_latency', 'shared_metric', 0.9, '[]', '["prometheus"]',
                 '[]', '[]', 'operational_knowledge', '["knowledge_latency@1"]',
                 'knowledge_latency', 1, 'policy:1', 'approved', 0, 0, 0, 1, 1);
        """)

        ensure_mapping_columns(conn)
        ensure_mapping_tenant_scope(conn)
        conn.execute("""CREATE TABLE signal_tenant_migration_metadata (
                          key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
                      )""")
        conn.execute("""INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
                      VALUES ('legacy_schema_owner_v1', 'default', 1)""")
        complete = False
        while not complete:
            complete, _, _ = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="default",
                batch_size=10,
            )

        columns = {row["name"] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)")}
        unique_indexes = {
            tuple(column["name"] for column in conn.execute(f"PRAGMA index_info({index['name']})"))
            for index in conn.execute("PRAGMA index_list(signal_metric_mappings)")
            if index["unique"]
        }
        migrated = conn.execute(
            "SELECT governance_ref, projection_key FROM signal_metric_mappings WHERE id=1"
        ).fetchone()

    assert "projection_key" in columns
    assert (
        "tenant_id",
        "signal_type",
        "metric_pattern",
        "governance_ref",
        "projection_key",
    ) in unique_indexes
    assert migrated["governance_ref"] == "knowledge_latency"
    assert migrated["projection_key"] == ""


def test_signal_mapping_activates_after_governed_corroboration(signal_store, monkeypatch):
    monkeypatch.setattr("tacit.signals.store.settings.knowledge_tenant_id", "tenant-a")
    signal_store = _pinned_store(signal_store, "tenant-a")
    runtime_settings = Settings(knowledge_tenant_id="tenant-a")
    first_persisted = persist_inferred_signal_review(
        store=signal_store,
        sig={
            "signal_type": "checkout_latency",
            "metric": "checkout_latency_seconds",
            "source": "heuristic",
            "auto_teach_eligible": True,
            "confidence": 0.9,
        },
        source_ref="grafana:checkout",
        dashboard_uid="checkout",
        tenant_id="tenant-a",
        runtime_settings=runtime_settings,
    )
    catalog = [_metric_entry("checkout_latency_seconds")]

    assert first_persisted is False
    assert signal_store.resolve_signal("checkout_latency", catalog, tenant_id="tenant-a") == []

    second_persisted = persist_inferred_signal_review(
        store=signal_store,
        sig={
            "signal_type": "checkout_latency",
            "metric": "checkout_latency_seconds",
            "source": "heuristic",
            "auto_teach_eligible": True,
            "confidence": 0.9,
        },
        source_ref="grafana:alert:checkout",
        source_type="alert_ingest",
        dashboard_uid="checkout-alert",
        tenant_id="tenant-a",
        runtime_settings=runtime_settings,
    )

    assert second_persisted is True
    assert signal_store.resolve_signal("checkout_latency", catalog, tenant_id="tenant-a")
    assert (
        _wildcard_store(signal_store).resolve_signal(
            "checkout_latency",
            catalog,
            tenant_id="tenant-b",
        )
        == []
    )
    with signal_store._conn() as conn:
        rows = conn.execute(
            """SELECT governance_ref, review_state
                 FROM signal_metric_mappings
                WHERE tenant_id = ? AND signal_type = ? AND metric_pattern = ?
                ORDER BY governance_ref""",
            ("tenant-a", "checkout_latency", "checkout_latency_seconds"),
        ).fetchall()
        audit = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='governed_projection_audit_v2'""").fetchone()
    ungoverned = next(row for row in rows if not row["governance_ref"])
    governed = next(row for row in rows if row["governance_ref"])
    assert ungoverned["review_state"] == "candidate"
    assert governed["review_state"] == "approved"
    assert audit["value"] == "clean"


def test_copied_dashboard_and_alert_sources_do_not_manufacture_corroboration(signal_store):
    signal_store = _pinned_store(signal_store, "tenant-a")
    runtime_settings = Settings(knowledge_tenant_id="tenant-a")
    signal = {
        "signal_type": "checkout_latency",
        "metric": "checkout_latency_seconds",
        "source": "heuristic",
        "auto_teach_eligible": True,
        "confidence": 0.9,
    }

    dashboard = persist_inferred_signal_review(
        store=signal_store,
        sig=signal,
        source_ref="grafana:checkout-copy",
        dashboard_uid="checkout-copy",
        tenant_id="tenant-a",
        runtime_settings=runtime_settings,
        source_fingerprint="copied-operational-content",
    )
    alert = persist_inferred_signal_review(
        store=signal_store,
        sig=signal,
        source_ref="grafana:alert:checkout-copy",
        source_type="alert_ingest",
        dashboard_uid="checkout-alert-copy",
        tenant_id="tenant-a",
        runtime_settings=runtime_settings,
        source_fingerprint="copied-operational-content",
    )

    repository = KnowledgeRepository(signal_store._db_path)
    candidates = repository.list_candidates("tenant-a", kind="signal_mapping", limit=None)
    assert len(candidates) == 2
    from tacit.knowledge.service import KnowledgeService

    summary, _ = KnowledgeService(repository).corroboration.analyze(
        "tenant-a",
        candidates[0].proposition.proposition_key,
    )
    assert dashboard is False
    assert alert is False
    assert summary.raw_source_count == 2
    assert summary.independent_source_count == 1
    assert summary.independent_source_family_count == 1
    assert repository.find_knowledge_by_proposition("tenant-a", candidates[0].proposition.proposition_key) is None


def test_governed_mapping_scopes_and_lifecycles_are_independent(signal_store):
    signal_store = _pinned_store(signal_store, "tenant-a")
    for knowledge_id, service in (("knowledge-checkout", "checkout"), ("knowledge-payments", "payments")):
        signal_store.add_mapping(
            "request_latency",
            "shared_latency_seconds",
            confidence=0.9,
            context_services=[service],
            source_type="operational_knowledge",
            source_refs=[f"{knowledge_id}@1"],
            governance_ref=knowledge_id,
            governance_revision=1,
            review_state="approved",
            tenant_id="tenant-a",
        )

    with signal_store._conn() as conn:
        rows = conn.execute("""SELECT governance_ref, context_services FROM signal_metric_mappings
               WHERE tenant_id='tenant-a' AND signal_type='request_latency'
                 AND metric_pattern='shared_latency_seconds'
               ORDER BY governance_ref""").fetchall()

    assert [(row["governance_ref"], row["context_services"]) for row in rows] == [
        ("knowledge-checkout", '["checkout"]'),
        ("knowledge-payments", '["payments"]'),
    ]

    applied_refs: set[str] = set()
    substitutions = signal_store.resolve_signals_for_archetype(
        {"request_latency": "default_latency_seconds"},
        [_metric_entry("shared_latency_seconds")],
        context_service="checkout",
        tenant_id="tenant-a",
        applied_governance_refs=applied_refs,
    )
    assert substitutions == {"default_latency_seconds": "shared_latency_seconds"}
    assert applied_refs == {"knowledge-checkout"}

    assert signal_store.set_mapping_review_state(
        "request_latency",
        "shared_latency_seconds",
        "candidate",
        tenant_id="tenant-a",
        governance_ref="knowledge-checkout",
    )
    assert (
        signal_store.get_mappings_for_signal(
            "request_latency",
            context_service="checkout",
            tenant_id="tenant-a",
        )
        == []
    )
    payments = signal_store.get_mappings_for_signal(
        "request_latency",
        context_service="payments",
        tenant_id="tenant-a",
    )
    assert [mapping["governance_ref"] for mapping in payments] == ["knowledge-payments"]


def test_learning_context_index_is_tenant_scoped(signal_store):
    signal_store = _wildcard_store(signal_store)
    for tenant_id, title, metric in (
        ("tenant-a", "Tenant A Checkout", "tenant_a_checkout_latency"),
        ("tenant-b", "Tenant B Checkout", "tenant_b_checkout_latency"),
    ):
        signal_store.index_dashboard_context(
            tenant_id=tenant_id,
            dashboard_uid="shared-dashboard",
            backend_name="grafana",
            dashboard_title=title,
            metrics_found=[metric],
            status="approved",
        )

    tenant_a = signal_store.search_learning_context("checkout", tenant_id="tenant-a")
    tenant_b = signal_store.search_learning_context("checkout", tenant_id="tenant-b")

    assert {row["dashboard_title"] for row in tenant_a} == {"Tenant A Checkout"}
    assert {row["dashboard_title"] for row in tenant_b} == {"Tenant B Checkout"}


def test_taught_signal_definitions_are_tenant_scoped(signal_store):
    signal_store = _wildcard_store(signal_store)
    signal_store.register_signal_type(
        "request_latency",
        description="Built-in latency",
        category="latency",
    )
    signal_store.register_signal_type(
        "request_latency",
        description="Acme latency semantics",
        tenant_id="tenant-a",
    )
    signal_store.register_signal_type(
        "acme_queue_pressure",
        description="Acme custom queue signal",
        category="saturation",
        tenant_id="tenant-a",
    )

    tenant_a = {row["signal_type"]: row for row in signal_store.list_signal_types(tenant_id="tenant-a")}
    tenant_b = {row["signal_type"]: row for row in signal_store.list_signal_types(tenant_id="tenant-b")}

    assert tenant_a["request_latency"]["description"] == "Acme latency semantics"
    assert tenant_a["request_latency"]["category"] == "latency"
    assert tenant_b["request_latency"]["description"] == "Built-in latency"
    assert "acme_queue_pressure" in tenant_a
    assert "acme_queue_pressure" not in tenant_b
    assert signal_store.get_signal_type("acme_queue_pressure", tenant_id="tenant-b") is None


def test_legacy_mappings_and_learning_index_migrate_to_default_tenant(tmp_path):
    db_path = tmp_path / "legacy-tenantless-signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '', unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT, signal_type TEXT NOT NULL,
                metric_pattern TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 0.5,
                context_services TEXT NOT NULL DEFAULT '[]',
                context_datasource_types TEXT NOT NULL DEFAULT '[]',
                context_environments TEXT NOT NULL DEFAULT '[]', context_archetypes TEXT NOT NULL DEFAULT '[]',
                source_type TEXT NOT NULL DEFAULT 'bootstrap', source_refs TEXT NOT NULL DEFAULT '[]',
                inference_version TEXT NOT NULL DEFAULT '', review_state TEXT NOT NULL DEFAULT 'trusted',
                use_count INTEGER NOT NULL DEFAULT 0, positive_feedback INTEGER NOT NULL DEFAULT 0,
                negative_feedback INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, last_seen REAL NOT NULL,
                UNIQUE(signal_type, metric_pattern)
            );
            CREATE VIRTUAL TABLE learning_context_fts USING fts5(
                source_kind, source_id UNINDEXED, backend_name UNINDEXED, dashboard_uid UNINDEXED,
                dashboard_title, dashboard_tags, panel_title, metric_name, query_text, service,
                signal_type, review_state UNINDEXED, reason, provenance, indexed_at UNINDEXED
            );
            INSERT INTO signal_types VALUES ('latency', '', '', '', 1, 1);
            INSERT INTO signal_metric_mappings
                (signal_type, metric_pattern, confidence, created_at, last_seen)
            VALUES ('latency', 'legacy_latency_seconds', 0.9, 1, 1);
            INSERT INTO learning_context_fts
                (source_kind, source_id, dashboard_title, metric_name, review_state, indexed_at)
            VALUES ('dashboard_panel', 'legacy', 'Legacy Checkout', 'legacy_latency_seconds', 'approved', 1);
        """)

    store = SignalStore(db_path=db_path)

    with store._conn() as conn:
        migrated_mapping = conn.execute("""SELECT tenant_id, governance_ref FROM signal_metric_mappings
               WHERE signal_type='latency' AND metric_pattern='legacy_latency_seconds'""").fetchone()

    assert store.resolve_signal(
        "latency",
        [_metric_entry("legacy_latency_seconds")],
        tenant_id="default",
    )
    assert migrated_mapping is not None
    assert migrated_mapping["tenant_id"] == GLOBAL_BOOTSTRAP_TENANT_ID
    assert migrated_mapping["governance_ref"] == ""
    assert store.search_learning_context("legacy", tenant_id="default")
    _wildcard_store(store).add_mapping(
        "latency",
        "legacy_latency_seconds",
        confidence=0.8,
        tenant_id="tenant-b",
    )


def test_wildcard_migration_requires_an_explicit_owner_for_legacy_tenant_data(tmp_path):
    db_path = tmp_path / "legacy-owner-unknown.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type TEXT NOT NULL,
                metric_pattern TEXT NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'teach'
            );
            INSERT INTO signal_metric_mappings (signal_type, metric_pattern, source_type)
            VALUES ('request_latency', 'private_latency_seconds', 'teach');
        """)

    with pytest.raises(RuntimeError, match="reason=ownerless_wildcard"):
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
        )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)")}
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "tenant_id" not in columns
    assert "signal_metric_mappings_old" not in tables


def test_legacy_signal_definition_migration_fails_closed_without_bootstrap_taxonomy(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy-definitions-without-taxonomy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '', unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            INSERT INTO signal_types VALUES
                ('private_tenant_signal', 'Private definition', 'custom', 'count', 1, 1);
        """)

    canary = "PRIVATE-BOOTSTRAP-CANARY"

    def unavailable_taxonomy(_package: str):
        raise OSError(canary)

    monkeypatch.setattr("tacit.signals.store.files", unavailable_taxonomy)

    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match="bootstrap taxonomy is unavailable"):
            SignalStore(
                db_path=db_path,
                runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
            )

    assert canary not in str(logs)
    bootstrap_log = next(entry for entry in logs if entry.get("event") == "signals_bootstrap_taxonomy_unavailable")
    assert bootstrap_log["reason_code"] == "signals_bootstrap_taxonomy_unavailable"
    assert bootstrap_log["exception_class"] == "OSError"
    assert len(bootstrap_log["error_fingerprint"]) == 16

    with sqlite3.connect(db_path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        original = conn.execute(
            "SELECT description FROM signal_types WHERE signal_type='private_tenant_signal'"
        ).fetchone()
    assert "tenant_signal_types" not in tables
    assert "signal_tenant_migration_metadata" not in tables
    assert original == ("Private definition",)


def test_concurrent_signal_store_startup_serializes_schema_and_markers(tmp_path):
    db_path = tmp_path / "concurrent-startup.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '', unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            INSERT INTO signal_types VALUES
                ('tenant_private_signal', 'Tenant private', 'custom', 'count', 1, 1);
        """)

    def initialize() -> None:
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _: initialize(), range(8)))

    with sqlite3.connect(db_path) as conn:
        markers = dict(conn.execute("SELECT key, value FROM signal_tenant_migration_metadata"))
        tenant_definition = conn.execute("""SELECT tenant_id FROM tenant_signal_types
               WHERE signal_type='tenant_private_signal'""").fetchone()
        global_definition = conn.execute(
            "SELECT 1 FROM signal_types WHERE signal_type='tenant_private_signal'"
        ).fetchone()
        old_tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_old'").fetchall()

    assert markers["default_owner_v1"] == "tenant-a"
    assert markers["signal_definition_scope_v1"] == "tenant-a"
    assert tenant_definition == ("tenant-a",)
    assert global_definition is None
    assert old_tables == []


def test_concurrent_schema_recheck_claims_database_identity(tmp_path, monkeypatch):
    schema_checks = iter((False, True))
    claims: list[tuple[str, str | None]] = []

    monkeypatch.setattr(
        "tacit.signals.store.signal_schema_is_current",
        lambda _conn: next(schema_checks),
    )
    monkeypatch.setattr(
        "tacit.signals.store.governed_projection_audit_is_current",
        lambda _conn: True,
    )
    monkeypatch.setattr(
        "tacit.signals.store.signal_tenant_owner_is_current",
        lambda _conn, *, legacy_tenant: True,
    )
    monkeypatch.setattr(
        "tacit.signals.store.require_confirmed_default_tenant_owner",
        lambda _conn, *, legacy_tenant: None,
    )

    def claim(_conn, *, role, expected_database_id):
        claims.append((role, expected_database_id))
        return "signals-database-id"

    monkeypatch.setattr("tacit.signals.store.claim_sqlite_database_identity", claim)

    store = SignalStore(
        db_path=tmp_path / "concurrent-recheck.db",
        runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
    )

    assert claims == [("signals", None)]
    assert store._database_id == "signals-database-id"


def test_current_signal_schema_reopens_without_running_migrations(tmp_path, monkeypatch):
    db_path = tmp_path / "current-schema.db"
    SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    def fail_migration(*_args, **_kwargs):
        raise AssertionError("current signal schema should not run migrations or authority audit")

    monkeypatch.setattr("tacit.signals.store.ensure_schema", fail_migration)

    reopened = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    assert reopened._db_path == db_path


def test_current_signal_schema_preflights_owner_then_rechecks_after_writer_lock(tmp_path, monkeypatch):
    db_path = tmp_path / "current-schema-owner-lock.db"
    SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))
    owner_checks: list[bool] = []

    def require_locked_owner(conn, *, legacy_tenant):
        owner_checks.append(conn.in_transaction)
        return require_confirmed_default_tenant_owner(conn, legacy_tenant=legacy_tenant)

    monkeypatch.setattr(
        "tacit.signals.store.require_confirmed_default_tenant_owner",
        require_locked_owner,
    )

    SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    assert owner_checks == [False, True]


def test_mapping_source_ref_migration_resumes_after_committed_batch(tmp_path, monkeypatch):
    db_path = tmp_path / "resumable-source-refs.db"
    original = SignalStore(db_path=db_path)
    for index in range(5):
        original.add_mapping(
            f"source_signal_{index}",
            f"source_metric_{index}",
            source_refs=[f"dashboard:{index}"],
        )
    with original._conn() as conn:
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (MAPPING_SOURCE_REF_INDEX_MARKER,),
        )
        conn.execute("DELETE FROM signal_mapping_source_refs")

    original_reconcile = SignalStore._reconcile_mapping_source_ref_index_batched

    def migrate_one_batch_then_stop(store: SignalStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, source_ref_count = reconcile_mapping_source_ref_index_batch(conn, batch_size=2)
        assert complete is False
        assert source_ref_count == 2
        raise RuntimeError("simulated source-ref migration interruption")

    monkeypatch.setattr(
        SignalStore,
        "_reconcile_mapping_source_ref_index_batched",
        migrate_one_batch_then_stop,
    )
    with pytest.raises(RuntimeError, match="simulated source-ref migration interruption"):
        SignalStore(db_path=db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_mapping_source_refs").fetchone()[0] == 2
        cursor = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='mapping_source_ref_cursor_v2'""").fetchone()
        complete = conn.execute(
            "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
            (MAPPING_SOURCE_REF_INDEX_MARKER,),
        ).fetchone()
    assert cursor is not None
    assert complete is None

    monkeypatch.setattr(
        SignalStore,
        "_reconcile_mapping_source_ref_index_batched",
        original_reconcile,
    )
    resumed = SignalStore(db_path=db_path)
    with resumed._conn() as conn:
        refs = conn.execute(
            "SELECT mapping_id, source_ref FROM signal_mapping_source_refs ORDER BY mapping_id"
        ).fetchall()
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (MAPPING_SOURCE_REF_INDEX_MARKER,),
        ).fetchone()
        cursor = conn.execute("""SELECT 1 FROM signal_tenant_migration_metadata
               WHERE key='mapping_source_ref_cursor_v2'""").fetchone()
    assert [row["source_ref"] for row in refs] == [f"dashboard:{index}" for index in range(5)]
    assert marker["value"] == "complete"
    assert cursor is None


def test_mapping_source_ref_migration_rejects_malformed_legacy_provenance(tmp_path):
    path_canary = "PRIVATE-MIGRATION-PATH-CANARY"
    payload_canary = "PRIVATE-SOURCE-REF-PAYLOAD-CANARY"
    db_path = tmp_path / path_canary / "malformed-source-refs.db"
    store = SignalStore(db_path=db_path)
    original_id = store.add_mapping("source_signal", "source_metric", source_refs=["dashboard:valid"])
    mapping_id = -9_223_372_036_854_775_001
    with store._conn() as conn:
        conn.execute("DROP TRIGGER trg_signal_mapping_source_refs_validate_update")
        conn.execute("DROP TRIGGER trg_signal_mapping_source_ref_update")
        conn.execute(
            "UPDATE signal_metric_mappings SET id=?, source_refs=? WHERE id=?",
            (mapping_id, payload_canary, original_id),
        )
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (MAPPING_SOURCE_REF_INDEX_MARKER,),
        )
        conn.execute("DELETE FROM signal_tenant_migration_metadata WHERE key='mapping_source_ref_cursor_v2'")

    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match="source refs are malformed") as exc_info:
            SignalStore(db_path=db_path)

    rendered = f"{logs!r} {exc_info.value!s}"
    assert str(mapping_id) not in rendered
    assert payload_canary not in rendered
    assert path_canary not in rendered
    diagnostic = next(entry for entry in logs if entry.get("event") == "signal_mapping_source_ref_migration_failed")
    assert diagnostic["reason_code"] == "signal_mapping_source_refs_malformed"
    assert diagnostic["exception_class"] == "JSONDecodeError"
    assert len(str(diagnostic["mapping_ref_fingerprint"])) == 16
    assert len(str(diagnostic["error_fingerprint"])) == 16

    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
                (MAPPING_SOURCE_REF_INDEX_MARKER,),
            ).fetchone()
            is None
        )


@pytest.mark.parametrize(
    "source_refs",
    ["not-json", "[123]", '["valid", null]', '[" padded "]', '[""]'],
)
def test_mapping_source_ref_writes_reject_malformed_json_arrays(signal_store, source_refs):
    mapping_id = signal_store.add_mapping("source_signal", "source_metric")

    with pytest.raises(sqlite3.IntegrityError, match="source_refs must be a JSON string array"):
        with signal_store._conn() as conn:
            conn.execute(
                "UPDATE signal_metric_mappings SET source_refs=? WHERE id=?",
                (source_refs, mapping_id),
            )


@pytest.mark.parametrize("source_refs", [[123], [""], [" padded "]])
def test_add_mapping_rejects_invalid_source_refs_before_storage(signal_store, source_refs):
    with pytest.raises(ValueError, match="source_refs must contain non-blank, trimmed strings"):
        signal_store.add_mapping("source_signal", "source_metric", source_refs=source_refs)

    with signal_store._conn() as conn:
        assert conn.execute("SELECT 1 FROM signal_metric_mappings WHERE signal_type='source_signal'").fetchone() is None


def test_mapping_source_ref_migration_canonicalizes_legacy_payload_and_lifecycle_lookup(tmp_path):
    db_path = tmp_path / "legacy-source-ref-whitespace.db"
    store = SignalStore(db_path=db_path)
    mapping_id = store.add_mapping(
        "source_signal",
        "source_metric",
        source_type="dashboard_ingest",
        source_refs=["dashboard:42"],
    )
    with store._conn() as conn:
        conn.execute("DROP TRIGGER trg_signal_mapping_source_refs_validate_update")
        conn.execute("DROP TRIGGER trg_signal_mapping_source_ref_update")
        conn.execute(
            "UPDATE signal_metric_mappings SET source_refs=? WHERE id=?",
            ('[" dashboard:42 "]', mapping_id),
        )
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (MAPPING_SOURCE_REF_INDEX_MARKER,),
        )
        conn.execute("DELETE FROM signal_tenant_migration_metadata WHERE key='mapping_source_ref_cursor_v2'")
        conn.execute("DELETE FROM signal_mapping_source_refs")

    migrated = SignalStore(db_path=db_path)
    with migrated._conn() as conn:
        payload = conn.execute(
            "SELECT source_refs FROM signal_metric_mappings WHERE id=?",
            (mapping_id,),
        ).fetchone()
        indexed = conn.execute(
            "SELECT source_ref FROM signal_mapping_source_refs WHERE mapping_id=?",
            (mapping_id,),
        ).fetchall()
        assert json.loads(payload["source_refs"]) == ["dashboard:42"]
        assert [row["source_ref"] for row in indexed] == ["dashboard:42"]
        SignalStore._remove_mapping_source_refs(
            conn,
            tenant_id="default",
            source_type="dashboard_ingest",
            stale_refs={"dashboard:42"},
        )
        assert (
            conn.execute(
                "SELECT 1 FROM signal_metric_mappings WHERE id=?",
                (mapping_id,),
            ).fetchone()
            is None
        )


def test_default_owner_migration_resumes_after_a_committed_batch(tmp_path, monkeypatch):
    from tacit.signals.migrations import reconcile_default_tenant_owner_batch

    db_path = tmp_path / "resumable-default-owner.db"
    original = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="default"))
    for index in range(5):
        original.record_learned_artifact(
            tenant_id="default",
            artifact_id=f"runbook:{index}",
            artifact_type="runbook",
            title=f"Runbook {index}",
        )
    with original._conn() as conn:
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        )

    original_reconcile = SignalStore._reconcile_default_tenant_owner_batched

    def migrate_one_batch_then_stop(store: SignalStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, _operation, row_count = reconcile_default_tenant_owner_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=2,
            )
        assert complete is False
        assert row_count == 2
        raise RuntimeError("simulated process interruption")

    monkeypatch.setattr(SignalStore, "_reconcile_default_tenant_owner_batched", migrate_one_batch_then_stop)
    with pytest.raises(RuntimeError, match="simulated process interruption"):
        SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
                (_DEFAULT_OWNER_MARKER,),
            ).fetchone()
            is None
        )
        progress = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='default_owner_in_progress_v1'"
        ).fetchone()
        cursor = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='default_owner_cursor_v1:learned_artifacts'""").fetchone()
        assert conn.execute("SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id='tenant-a'").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id='default'").fetchone()[0] == 3
    assert progress == ("tenant-a",)
    assert cursor is not None
    assert int(cursor[0]) > 0

    monkeypatch.setattr(SignalStore, "_reconcile_default_tenant_owner_batched", original_reconcile)
    with pytest.raises(RuntimeError, match="reason=migration_owner_mismatch"):
        SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-b"))

    resumed = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    with resumed._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id='tenant-a'").fetchone()[0] == 5
        assert conn.execute("SELECT COUNT(*) FROM learned_artifacts WHERE tenant_id='default'").fetchone()[0] == 0
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        ).fetchone()
        progress = conn.execute(
            "SELECT 1 FROM signal_tenant_migration_metadata WHERE key='default_owner_in_progress_v1'"
        ).fetchone()
        cursor = conn.execute("""SELECT 1 FROM signal_tenant_migration_metadata
               WHERE key LIKE 'default_owner_cursor_v1:%'""").fetchone()
    assert marker is not None
    assert marker["value"] == "tenant-a"
    assert progress is None
    assert cursor is None


def test_signal_store_rejects_a_configured_owner_change(tmp_path):
    db_path = tmp_path / "owner-change.db"
    SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    with pytest.raises(RuntimeError, match="reason=pinned_owner_mismatch"):
        SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-b"))


def test_default_owner_migration_pages_unindexed_learning_context_by_rowid(tmp_path, monkeypatch):
    db_path = tmp_path / "learning-context-owner.db"
    original = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="default"))
    with original._conn() as conn:
        conn.executemany(
            """INSERT INTO learning_context_fts
               (tenant_id, source_kind, source_id, dashboard_title, indexed_at)
               VALUES ('default', 'dashboard', ?, ?, 1)""",
            [(f"dashboard:{index}", f"Dashboard {index}") for index in range(5)],
        )
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        )

    monkeypatch.setattr("tacit.signals.store._DEFAULT_OWNER_MIGRATION_BATCH_SIZE", 2)
    migrated = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))

    with migrated._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM learning_context_fts WHERE tenant_id='tenant-a'").fetchone()[0] == 5
        assert conn.execute("SELECT 1 FROM learning_context_fts WHERE tenant_id='default'").fetchone() is None


def test_knowledge_service_rejects_a_resolver_from_another_database(tmp_path):
    from tacit.knowledge.service import KnowledgeService

    authority = KnowledgeRepository(tmp_path / "authority.db")
    resolver = SignalStore(tmp_path / "resolver.db")

    with pytest.raises(ValueError, match="must use the same database"):
        KnowledgeService(authority, signal_store=resolver)


def test_knowledge_service_rejects_a_resolver_with_another_tenant_boundary(tmp_path):
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "knowledge.db"
    tenant_a_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    authority = KnowledgeRepository(db_path, runtime_settings=tenant_a_settings)
    resolver = SignalStore(
        db_path,
        runtime_settings=tenant_a_settings,
    )

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        KnowledgeService(
            authority,
            signal_store=resolver,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
        )
    assert "tenant" in exc_info.value.dimensions


def test_wildcard_rejects_unconfirmed_default_owner_and_pinned_reopen_retargets_current_schema(tmp_path):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "previously-default-owned.db"
    default_settings = Settings(knowledge_tenant_id="default")
    store = SignalStore(db_path=db_path, runtime_settings=default_settings)
    store.record_learned_artifact(
        tenant_id="default",
        artifact_id="runbook:checkout",
        artifact_type="runbook",
        title="Checkout recovery",
    )
    service = KnowledgeService(KnowledgeRepository(db_path))
    candidate = service.create_candidate(
        kind=KnowledgeKind.INVESTIGATION_PATTERN,
        payload_ref="runbook:checkout",
        typed_payload={"pattern": "check saturation"},
        proposition={
            "subject_ref": "concept:checkout-investigation",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:saturation",
        },
        scope=KnowledgeScope(
            tenant_id="default",
            service_refs=["entity:service:checkout"],
        ),
        provenance_refs=["runbook:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="migration-test")
    _decision, promoted = service.evaluate_candidate(candidate.id, authoritative_source=True)
    assert promoted is not None
    with store._conn() as conn:
        conn.execute(
            """INSERT INTO knowledge_candidate_entity_refs
               (tenant_id, candidate_id, entity_ref, role)
               VALUES ('default', ?, 'entity:service:checkout', 'subject')""",
            (candidate.id,),
        )
        conn.execute(
            "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        )

    with pytest.raises(RuntimeError, match="reason=unconfirmed_default_owner"):
        SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True))

    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute("SELECT tenant_id FROM learned_artifacts WHERE artifact_id='runbook:checkout'").fetchone()[0]
            == "default"
        )
        assert (
            conn.execute(
                "SELECT tenant_id FROM knowledge_candidates WHERE id=?",
                (candidate.id,),
            ).fetchone()[0]
            == "default"
        )

    pinned = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="tenant-a"))
    repository = KnowledgeRepository(db_path)

    assert [row["title"] for row in pinned.list_learned_artifacts(tenant_id="tenant-a")] == ["Checkout recovery"]
    with pinned._conn() as conn:
        assert conn.execute("SELECT 1 FROM learned_artifacts WHERE tenant_id='default'").fetchone() is None
    assert repository.get_candidate(candidate.id, "default") is None
    assert repository.get_candidate(candidate.id, "tenant-a") is None
    with pinned._conn() as conn:
        quarantined_tables = {
            row["source_table"] for row in conn.execute("""SELECT source_table FROM signal_migration_quarantine
                   WHERE reason='tenant_identity_requires_remigration'""").fetchall()
        }
    assert {
        "knowledge_candidates",
        "knowledge_candidate_provenance",
        "knowledge_candidate_entity_refs",
        "knowledge_propositions",
        "knowledge_current_scope_refs",
        "knowledge_current_contributors",
    } <= quarantined_tables
    with pinned._conn() as conn:
        for table in (
            "knowledge_candidate_provenance",
            "knowledge_candidate_entity_refs",
            "knowledge_current_scope_refs",
            "knowledge_current_contributors",
        ):
            assert conn.execute(f"SELECT 1 FROM {table} WHERE tenant_id='default' LIMIT 1").fetchone() is None

    tenant_a_service = KnowledgeService(
        repository,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    tenant_a_candidate = tenant_a_service.create_candidate(
        kind=KnowledgeKind.INVESTIGATION_PATTERN,
        payload_ref="runbook:checkout",
        typed_payload={"pattern": "check saturation"},
        proposition={
            "subject_ref": "concept:checkout-investigation",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:saturation",
        },
        scope=KnowledgeScope(tenant_id="tenant-a"),
        provenance_refs=["runbook:checkout"],
        tenant_id="tenant-a",
    )
    duplicate = tenant_a_service.create_candidate(
        kind=KnowledgeKind.INVESTIGATION_PATTERN,
        payload_ref="runbook:checkout",
        typed_payload={"pattern": "check saturation"},
        proposition={
            "subject_ref": "concept:checkout-investigation",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:saturation",
        },
        scope=KnowledgeScope(tenant_id="tenant-a"),
        provenance_refs=["runbook:checkout"],
        tenant_id="tenant-a",
    )
    assert duplicate.id == tenant_a_candidate.id
    assert len(repository.list_candidates("tenant-a", limit=None)) == 1
    with pinned._conn() as conn:
        proposition_count = conn.execute(
            "SELECT COUNT(*) FROM knowledge_propositions WHERE tenant_id='tenant-a'"
        ).fetchone()[0]
    assert proposition_count == 1

    wildcard = SignalStore(db_path=db_path, runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True))
    assert wildcard.list_learned_artifacts(tenant_id="tenant-a")


def test_startup_rejects_unprojectable_authority_without_certifying_it_clean(tmp_path, monkeypatch):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeRevision, KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "governed-projection-backfill.db"
    service = KnowledgeService(KnowledgeRepository(db_path))
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:checkout",
        typed_payload={"metric_pattern": "checkout_latency_seconds", "confidence": 0.91},
        proposition={
            "subject_ref": "concept:request-latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """SELECT content_json, semantic_fingerprint FROM operational_knowledge_revisions
               WHERE tenant_id='default' AND knowledge_id=? AND revision=?""",
            (revision.knowledge_id, revision.revision),
        ).fetchone()
        content = json.loads(row["content_json"])
        content.pop("resolver_payload", None)
        legacy_content = json.dumps(content, sort_keys=True)
        legacy_fingerprint = row["semantic_fingerprint"]
        conn.execute(
            """UPDATE operational_knowledge_revisions SET content_json=?
               WHERE tenant_id='default' AND knowledge_id=? AND revision=?""",
            (legacy_content, revision.knowledge_id, revision.revision),
        )
        conn.execute(
            """UPDATE signal_metric_mappings SET governance_revision=0
               WHERE tenant_id='default' AND governance_ref=?""",
            (revision.knowledge_id,),
        )

    canary = "PRIVATE-PROJECTION-CANARY"

    def reject_revision(_payload: str):
        raise ValueError(canary)

    monkeypatch.setattr(KnowledgeRevision, "model_validate_json", reject_revision)
    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match="active governed signal authority cannot be projected exactly"):
            SignalStore(db_path=db_path)

    assert canary not in str(logs)
    failure_log = next(
        entry for entry in logs if entry.get("event") == "governed_signal_projection_authority_unrepairable"
    )
    assert failure_log["reason_code"] == "governed_signal_projection_validation_failed"
    assert failure_log["exception_class"] == "ValueError"
    assert len(failure_log["error_fingerprint"]) == 16
    assert len(failure_log["authority_ref_fingerprint"]) == 16

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        authority = conn.execute(
            """SELECT content_json, semantic_fingerprint FROM operational_knowledge_revisions
               WHERE tenant_id='default' AND knowledge_id=? AND revision=?""",
            (revision.knowledge_id, revision.revision),
        ).fetchone()
        projection = conn.execute(
            """SELECT governance_revision, review_state FROM signal_metric_mappings
               WHERE tenant_id='default' AND governance_ref=?""",
            (revision.knowledge_id,),
        ).fetchone()
        audit = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='governed_projection_audit_v2'""").fetchone()

    assert authority["content_json"] == legacy_content
    assert authority["semantic_fingerprint"] == legacy_fingerprint
    assert dict(projection) == {"governance_revision": 0, "review_state": "approved"}
    assert audit["value"].startswith("dirty")


def _insert_empty_projection_authority(
    store: SignalStore,
    *,
    content_json: str | None = None,
):
    from tacit.knowledge.models import KnowledgeRevision

    revision = KnowledgeRevision(
        knowledge_id="nonempty-authority",
        revision=1,
        proposition={
            "kind": "signal_mapping",
            "subject_ref": "concept:empty-authority",
            "predicate": "represented_by",
            "object_ref": "concept:empty-authority-metric",
            "concept_ref": "signal:empty_authority_signal",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        state={
            "review_state": "approved",
            "lifecycle_status": "active",
            "eligibility": "live_verified",
        },
        corroboration_snapshot_ref="snapshot:test",
        policy_id="test-policy",
        policy_version="1",
        decision_ref="decision:test",
        provenance_refs=["dashboard:empty-authority"],
        resolver_payload={"mappings": [{"metric_pattern": "empty_authority_metric", "confidence": 0.9}]},
        semantic_fingerprint="semantic:test",
    )
    empty_revision = revision.model_copy(
        update={
            "knowledge_id": "",
            "tenant_id": "",
            "scope": revision.scope.model_copy(update={"tenant_id": ""}),
        }
    )
    serialized = content_json if content_json is not None else empty_revision.model_dump_json()
    with store._conn() as conn:
        conn.executescript("""
            CREATE TABLE operational_knowledge (
                knowledge_id TEXT NOT NULL, tenant_id TEXT NOT NULL, kind TEXT NOT NULL,
                proposition_key TEXT NOT NULL, current_revision INTEGER NOT NULL,
                status TEXT NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL,
                PRIMARY KEY(tenant_id, knowledge_id)
            );
            CREATE TABLE operational_knowledge_revisions (
                knowledge_id TEXT NOT NULL, tenant_id TEXT NOT NULL, revision INTEGER NOT NULL,
                parent_revision INTEGER, schema_version TEXT NOT NULL, proposition_key TEXT NOT NULL,
                scope_json TEXT NOT NULL, review_state TEXT NOT NULL,
                lifecycle_status TEXT NOT NULL, eligibility TEXT NOT NULL,
                corroboration_snapshot_ref TEXT NOT NULL, policy_id TEXT NOT NULL,
                policy_version TEXT NOT NULL, revision_reason TEXT NOT NULL,
                content_json TEXT NOT NULL, semantic_fingerprint TEXT NOT NULL,
                created_at REAL NOT NULL, PRIMARY KEY(tenant_id, knowledge_id, revision)
            );
        """)
        now = time.time()
        for active_revision, active_content in (
            (empty_revision, serialized),
            (revision, revision.model_dump_json()),
        ):
            proposition_key = f"authority:{active_revision.tenant_id or 'empty'}"
            conn.execute(
                """INSERT INTO operational_knowledge
                   (knowledge_id, tenant_id, kind, proposition_key, current_revision,
                    status, created_at, updated_at)
                   VALUES (?, ?, 'signal_mapping', ?, 1, 'active', ?, ?)""",
                (
                    active_revision.knowledge_id,
                    active_revision.tenant_id,
                    proposition_key,
                    now,
                    now,
                ),
            )
            conn.execute(
                """INSERT INTO operational_knowledge_revisions
                   (knowledge_id, tenant_id, revision, parent_revision, schema_version,
                    proposition_key, scope_json, review_state, lifecycle_status,
                    eligibility, corroboration_snapshot_ref, policy_id, policy_version,
                    revision_reason, content_json, semantic_fingerprint, created_at)
                   VALUES (?, ?, 1, NULL, '1.0', ?, ?, 'approved', 'active',
                           'live_verified', 'snapshot:test', 'test-policy', '1',
                           'promoted', ?, 'semantic:test', ?)""",
                (
                    active_revision.knowledge_id,
                    active_revision.tenant_id,
                    proposition_key,
                    active_revision.scope.model_dump_json(),
                    active_content,
                    now,
                ),
            )
        conn.execute("""UPDATE signal_tenant_migration_metadata SET value='dirty:empty-key-test'
               WHERE key='governed_projection_audit_v2'""")
    return revision


def test_empty_projection_authority_key_failure_cannot_be_certified_clean(tmp_path):
    db_path = tmp_path / "empty-projection-authority.db"
    store = SignalStore(db_path=db_path)
    payload_canary = "PRIVATE-EMPTY-AUTHORITY-PAYLOAD-CANARY"
    _insert_empty_projection_authority(store, content_json=payload_canary)

    with capture_logs() as logs:
        with pytest.raises(
            RuntimeError,
            match="active governed signal authority cannot be projected exactly",
        ) as exc_info:
            SignalStore(db_path=db_path)

    assert payload_canary not in f"{logs!r} {exc_info.value!s}"
    with sqlite3.connect(db_path) as conn:
        dirty = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='governed_projection_audit_v2'"
        ).fetchone()
        conn.execute("DELETE FROM operational_knowledge_revisions WHERE tenant_id='' AND knowledge_id=''")
        conn.execute("DELETE FROM operational_knowledge WHERE tenant_id='' AND knowledge_id=''")
    assert dirty is not None and dirty[0].startswith("dirty:")

    repaired = SignalStore(db_path=db_path)
    with repaired._conn() as conn:
        clean = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='governed_projection_audit_v2'"
        ).fetchone()
    assert clean is not None and clean["value"] == "clean"


def test_projection_repair_interruption_after_empty_composite_key_stays_dirty(
    tmp_path,
    monkeypatch,
):
    from tacit.knowledge.models import KnowledgeRevision

    store = SignalStore(db_path=tmp_path / "empty-projection-cursor.db")
    _insert_empty_projection_authority(store)
    original_validate = KnowledgeRevision.model_validate_json
    validated_ids: list[str] = []
    interruption_canary = "PRIVATE-PROJECTION-INTERRUPTION-CANARY"

    def validate_then_interrupt(payload: str):
        revision = original_validate(payload)
        validated_ids.append(revision.knowledge_id)
        if len(validated_ids) == 2:
            raise ValueError(interruption_canary)
        return revision

    def skip_projection(_self, _revision, *, connection, allow_dirty=False):
        assert connection.in_transaction
        assert allow_dirty is True
        return {"active": True, "deactivated": 0, "projected": 0}

    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 1)
    monkeypatch.setattr(KnowledgeRevision, "model_validate_json", validate_then_interrupt)
    monkeypatch.setattr(SignalStore, "sync_governed_revision", skip_projection)
    with capture_logs() as logs:
        with pytest.raises(
            RuntimeError,
            match="active governed signal authority cannot be projected exactly",
        ):
            store._repair_projection_authority_batches()

    assert validated_ids[0] == ""
    assert len(validated_ids) == 2
    assert interruption_canary not in repr(logs)
    batches = [entry for entry in logs if entry.get("event") == "governed_signal_projection_repair_batch"]
    assert batches[0]["cursor_fingerprint"] == ""
    assert len(str(batches[1]["cursor_fingerprint"])) == 16
    with store._conn() as conn:
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='governed_projection_audit_v2'"
        ).fetchone()
    assert marker["value"].startswith("dirty:")


def test_later_projection_repair_batch_redacts_tenant_and_authority_ids(tmp_path, monkeypatch):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeRevision, KnowledgeScope

    db_path = tmp_path / "later-batch-projection-diagnostic.db"
    tenant_id = "PRIVATE-PROJECTION-TENANT-CANARY"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id=tenant_id)
    store = SignalStore(db_path=db_path, runtime_settings=runtime_settings)
    service = KnowledgeService(
        KnowledgeRepository(db_path),
        signal_store=store,
        runtime_settings=runtime_settings,
    )
    revisions = []
    for index in range(2):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=f"dashboard:private-{index}",
            typed_payload={"metric_pattern": f"private_metric_{index}", "confidence": 0.9},
            proposition={
                "subject_ref": f"concept:private-signal-{index}",
                "predicate": "represented_by",
                "object_ref": f"concept:private-metric-{index}",
                "concept_ref": f"signal:private_signal_{index}",
            },
            scope=KnowledgeScope(tenant_id=tenant_id, service_refs=["entity:service:checkout"]),
            provenance_refs=[f"dashboard:private-{index}"],
            tenant_id=tenant_id,
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
        assert revision is not None
        revisions.append(revision)

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE signal_metric_mappings SET review_state='candidate' WHERE governance_ref!=''")

    original_validate = KnowledgeRevision.model_validate_json
    validation_calls = 0
    error_canary = "PRIVATE-LATER-BATCH-ERROR-CANARY"

    def fail_second_authority(payload: str):
        nonlocal validation_calls
        validation_calls += 1
        if validation_calls == 2:
            raise ValueError(error_canary)
        return original_validate(payload)

    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 1)
    monkeypatch.setattr(KnowledgeRevision, "model_validate_json", fail_second_authority)
    with capture_logs() as logs:
        with pytest.raises(
            RuntimeError,
            match="active governed signal authority cannot be projected exactly",
        ) as exc_info:
            SignalStore(db_path=db_path, runtime_settings=runtime_settings)

    rendered = f"{logs!s} {exc_info.value!s}"
    assert validation_calls == 2
    assert tenant_id not in rendered
    assert error_canary not in rendered
    assert all(revision.knowledge_id not in rendered for revision in revisions)
    batch_logs = [entry for entry in logs if entry.get("event") == "governed_signal_projection_repair_batch"]
    assert [entry["batch"] for entry in batch_logs] == [1, 2]
    assert batch_logs[0]["cursor_fingerprint"] == ""
    assert len(batch_logs[1]["cursor_fingerprint"]) == 16
    assert all("after_tenant" not in entry and "after_knowledge_id" not in entry for entry in batch_logs)


@pytest.mark.parametrize("force_schema_migration", [False, True])
def test_dirty_projection_audit_repairs_authority_in_bounded_batches(
    tmp_path,
    monkeypatch,
    force_schema_migration,
):
    from structlog.testing import capture_logs

    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "bounded-projection-repair.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(
        KnowledgeRepository(db_path),
        signal_store=store,
    )
    revisions = []
    for index in range(2):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=f"dashboard:bounded-{index}",
            typed_payload={"metric_pattern": f"bounded_metric_{index}", "confidence": 0.9},
            proposition={
                "subject_ref": f"concept:bounded-signal-{index}",
                "predicate": "represented_by",
                "object_ref": f"concept:bounded_metric_{index}",
                "concept_ref": f"signal:bounded_signal_{index}",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            provenance_refs=[f"dashboard:bounded-{index}"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
        assert revision is not None
        revisions.append(revision)

    with sqlite3.connect(db_path) as conn:
        conn.execute("""UPDATE signal_metric_mappings SET review_state='candidate'
               WHERE governance_ref!=''""")
        if force_schema_migration:
            conn.execute(
                "DELETE FROM signal_tenant_migration_metadata WHERE key=?",
                (CURRENT_SIGNAL_SCHEMA_MARKER,),
            )

    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 1)
    with capture_logs() as logs:
        repaired = SignalStore(db_path=db_path)

    batches = [entry for entry in logs if entry["event"] == "governed_signal_projection_repair_batch"]
    assert len(batches) == 2
    assert all(entry["batch_size"] == 1 for entry in batches)
    with repaired._conn() as conn:
        rows = conn.execute("""SELECT governance_ref, governance_revision, review_state
               FROM signal_metric_mappings WHERE governance_ref!=''
               ORDER BY governance_ref""").fetchall()
        audit = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='governed_projection_audit_v2'""").fetchone()
    assert [(row["governance_ref"], row["governance_revision"]) for row in rows] == sorted(
        (revision.knowledge_id, revision.revision) for revision in revisions
    )
    assert {row["review_state"] for row in rows} == {"approved"}
    assert audit["value"] == "clean"


def test_projection_audit_validation_pages_without_per_mapping_authority_queries(tmp_path, monkeypatch):
    from contextlib import contextmanager

    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "paged-projection-validation.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(KnowledgeRepository(db_path), signal_store=store)
    for index in range(2):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=f"dashboard:validation-{index}",
            typed_payload={"metric_pattern": f"validation_metric_{index}", "confidence": 0.9},
            proposition={
                "subject_ref": f"concept:validation-{index}",
                "predicate": "represented_by",
                "object_ref": f"concept:validation-metric-{index}",
                "concept_ref": f"signal:validation_signal_{index}",
            },
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            provenance_refs=[f"dashboard:validation-{index}"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
        assert revision is not None

    with store._conn() as conn:
        conn.execute("""UPDATE signal_tenant_migration_metadata SET value='dirty:test'
               WHERE key='governed_projection_audit_v2'""")

    statements: list[str] = []
    original_conn = store._conn

    @contextmanager
    def traced_conn():
        with original_conn() as conn:
            conn.set_trace_callback(statements.append)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    monkeypatch.setattr(store, "_conn", traced_conn)
    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUTHORITY_VALIDATION_BATCH_SIZE", 1)
    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 1)

    assert store._validated_projection_audit_token() == "dirty:test"
    assert not [statement for statement in statements if "FROM operational_knowledge item" in statement]
    authority_pages = [
        statement
        for statement in statements
        if "FROM operational_knowledge current" in statement and "LIMIT 1" in statement
    ]
    assert len(authority_pages) >= 2


def test_projection_audit_and_resolution_include_nonpositive_governed_mapping_ids(
    tmp_path,
    monkeypatch,
):
    from tacit.knowledge.enums import KnowledgeKind

    db_path = tmp_path / "nonpositive-governed-mapping-ids.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(KnowledgeRepository(db_path), signal_store=store)
    revisions = []
    with monkeypatch.context() as promotion_patch:
        promotion_patch.setattr(store, "ensure_governed_projection_audit_current", lambda: None)
        promotion_patch.setattr(store, "governed_projection_audit_is_current", lambda _connection: True)
        for index, replacement_id in enumerate((-17, 0)):
            signal_type = f"legal_id_signal_{index}"
            metric_pattern = f"legal_id_metric_{index}"
            candidate = service.create_candidate(
                kind=KnowledgeKind.SIGNAL_MAPPING,
                payload_ref=f"dashboard:legal-id-{index}",
                typed_payload={"metric_pattern": metric_pattern, "confidence": 0.9},
                proposition={
                    "subject_ref": f"concept:legal-id-signal-{index}",
                    "predicate": "represented_by",
                    "object_ref": f"concept:legal-id-metric-{index}",
                    "concept_ref": f"signal:{signal_type}",
                },
                scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
                provenance_refs=[f"dashboard:legal-id-{index}"],
            )
            service.review_candidate(candidate.id, approved=True, reviewer="operator")
            _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
            assert revision is not None
            revisions.append((revision, signal_type, metric_pattern))
            with store._conn() as conn:
                mapping_id = int(
                    conn.execute(
                        "SELECT id FROM signal_metric_mappings WHERE governance_ref=?",
                        (revision.knowledge_id,),
                    ).fetchone()["id"]
                )
            _replace_signal_mapping_id(store, mapping_id, replacement_id)

    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 1)
    store.ensure_governed_projection_audit_current()

    with store._conn() as conn:
        persisted_ids = {
            int(row["id"])
            for row in conn.execute("SELECT id FROM signal_metric_mappings WHERE governance_ref!=''").fetchall()
        }
    assert {-17, 0} <= persisted_ids
    assert store._validated_projection_audit_token() is None
    for revision, signal_type, metric_pattern in revisions:
        forward = store.resolve_signal_details(
            signal_type,
            [_metric_entry(metric_pattern)],
            context_service="checkout",
        )
        reverse = store.resolve_metric_signal_details(
            [_metric_entry(metric_pattern)],
            context_service="checkout",
        )
        assert [(match.entry.name, match.governance_ref) for match in forward] == [
            (metric_pattern, revision.knowledge_id)
        ]
        assert [(match.signal_type, match.governance_ref) for match in reverse] == [
            (signal_type, revision.knowledge_id)
        ]


def test_projection_quarantine_resumes_across_negative_zero_and_sparse_ids(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "projection-quarantine-legal-id-domain.db"
    store = SignalStore(db_path=db_path)
    for index, replacement_id in enumerate((-23, 0, 101_003)):
        mapping_id = store.add_mapping(
            f"quarantine_signal_{index}",
            f"quarantine_metric_{index}",
            confidence=0.9,
            source_type="teach",
            review_state="trusted",
        )
        _replace_signal_mapping_id(store, mapping_id, replacement_id)

    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 1)
    original_validate = SignalStore._validated_projection_audit_token

    def interrupt_before_certification(_store):
        raise RuntimeError("injected audit certification interruption")

    monkeypatch.setattr(SignalStore, "_validated_projection_audit_token", interrupt_before_certification)
    with pytest.raises(RuntimeError, match="injected audit certification interruption"):
        store.ensure_governed_projection_audit_current()

    monkeypatch.setattr(SignalStore, "_validated_projection_audit_token", original_validate)
    store.ensure_governed_projection_audit_current()
    with store._conn() as conn:
        rows = conn.execute("""SELECT id, review_state FROM signal_metric_mappings
               WHERE id IN (-23, 0, 101003) ORDER BY id""").fetchall()
    assert [(int(row["id"]), row["review_state"]) for row in rows] == [
        (-23, "candidate"),
        (0, "candidate"),
        (101_003, "candidate"),
    ]
    assert store._validated_projection_audit_token() is None


def test_projection_audit_rejects_a_partial_multi_pattern_projection(tmp_path):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "partial-projection.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(KnowledgeRepository(db_path), signal_store=store)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:multi-pattern",
        typed_payload={"metric_pattern": "checkout_latency_seconds", "confidence": 0.9},
        proposition={
            "subject_ref": "concept:checkout-latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout-latency-seconds",
            "concept_ref": "signal:checkout_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:multi-pattern"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    expanded = revision.model_copy(
        update={
            "resolver_payload": {
                "mappings": [
                    {"metric_pattern": "checkout_latency_seconds", "confidence": 0.9},
                    {"metric_pattern": "checkout_latency_milliseconds", "confidence": 0.8},
                ]
            }
        }
    )
    with store.transaction() as conn:
        conn.execute(
            """UPDATE operational_knowledge_revisions SET content_json=?
               WHERE tenant_id=? AND knowledge_id=? AND revision=?""",
            (
                expanded.model_dump_json(),
                expanded.tenant_id,
                expanded.knowledge_id,
                expanded.revision,
            ),
        )
        store.sync_governed_revision(expanded, connection=conn)
        store.mark_governed_projection_audit_current(conn)
    with store._conn() as conn:
        conn.execute(
            """DELETE FROM signal_metric_mappings
               WHERE tenant_id=? AND governance_ref=? AND metric_pattern=?""",
            (
                expanded.tenant_id,
                expanded.knowledge_id,
                "checkout_latency_milliseconds",
            ),
        )

    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match="incomplete resolver projection") as exc_info:
            store._validated_projection_audit_token()

    rendered = f"{logs!s} {exc_info.value!s}"
    assert expanded.tenant_id not in rendered
    assert expanded.knowledge_id not in rendered
    failure_log = next(entry for entry in logs if entry.get("event") == "governed_signal_projection_validation_failed")
    assert failure_log["reason_code"] == "governed_signal_projection_incomplete"
    assert len(failure_log["authority_ref_fingerprint"]) == 16
    assert failure_log["expected_count"] == 2
    assert failure_log["projected_count"] == 1


def test_governed_projection_preserves_datasource_scope_per_metric_pattern(tmp_path):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "pattern-datasource-projection.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(KnowledgeRepository(db_path), signal_store=store)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:multi-datasource",
        typed_payload={"metric_pattern": "prom_latency_seconds", "confidence": 0.9},
        proposition={
            "subject_ref": "concept:checkout-latency",
            "predicate": "represented_by",
            "object_ref": "concept:prom-latency-seconds",
            "concept_ref": "signal:checkout_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:multi-datasource"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    expanded = revision.model_copy(
        update={
            "resolver_payload": {
                "mappings": [
                    {
                        "metric_pattern": "prom_latency_seconds",
                        "confidence": 0.9,
                        "context_datasource_types": ["prometheus"],
                    },
                    {
                        "metric_pattern": "AWS/ApplicationELB/TargetResponseTime",
                        "confidence": 0.8,
                        "context_datasource_types": ["cloudwatch"],
                    },
                ]
            }
        }
    )
    with store.transaction() as conn:
        conn.execute(
            """UPDATE operational_knowledge_revisions SET content_json=?
               WHERE tenant_id=? AND knowledge_id=? AND revision=?""",
            (
                expanded.model_dump_json(),
                expanded.tenant_id,
                expanded.knowledge_id,
                expanded.revision,
            ),
        )
        store.sync_governed_revision(expanded, connection=conn)
        rows = conn.execute(
            """SELECT * FROM signal_metric_mappings
               WHERE tenant_id=? AND governance_ref=? ORDER BY metric_pattern""",
            (expanded.tenant_id, expanded.knowledge_id),
        ).fetchall()
        authorities = {
            row["metric_pattern"]: projection_matches_authority(
                row,
                conn.execute(
                    """SELECT revision.content_json, revision.review_state,
                              revision.lifecycle_status, revision.eligibility
                       FROM operational_knowledge_revisions revision
                       WHERE revision.tenant_id=? AND revision.knowledge_id=? AND revision.revision=?""",
                    (expanded.tenant_id, expanded.knowledge_id, expanded.revision),
                ).fetchone(),
            )
            for row in rows
        }

    scopes = {row["metric_pattern"]: json.loads(row["context_datasource_types"]) for row in rows}
    assert scopes == {
        "AWS/ApplicationELB/TargetResponseTime": ["cloudwatch"],
        "prom_latency_seconds": ["prometheus"],
    }
    assert {pattern: result for pattern, (result, _reason) in authorities.items()} == {
        "AWS/ApplicationELB/TargetResponseTime": True,
        "prom_latency_seconds": True,
    }


def test_governed_projection_preserves_same_pattern_datasource_variants(tmp_path):
    from tacit.knowledge.enums import EvidenceRole, KnowledgeKind, LineageKind, SourceFamily
    from tacit.knowledge.models import KnowledgeEvidenceReference, KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "same-pattern-datasource-projection.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(KnowledgeRepository(db_path), signal_store=store)
    store.register_signal_type("shared_latency", description="Shared latency", category="latency")
    proposition = {
        "subject_ref": "concept:shared-latency",
        "predicate": "represented_by",
        "object_ref": "concept:SharedLatencyMetric",
        "concept_ref": "signal:shared_latency",
    }
    candidates = []
    for datasource_type, confidence, source_family in (
        ("prometheus", 0.91, SourceFamily.DASHBOARD),
        ("cloudwatch", 0.73, SourceFamily.ALERT),
    ):
        candidate = service.create_candidate(
            kind=KnowledgeKind.SIGNAL_MAPPING,
            payload_ref=f"dashboard:{datasource_type}:shared-latency",
            typed_payload={
                "metric_pattern": "SharedLatencyMetric",
                "confidence": confidence,
                "context_datasource_types": [datasource_type],
            },
            proposition=proposition,
            scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
            evidence=[
                KnowledgeEvidenceReference(
                    evidence_ref=f"evidence:{datasource_type}:shared-latency",
                    evidence_role=EvidenceRole.SUPPORTING,
                    source_family=source_family,
                    lineage_group=f"source:{datasource_type}:shared-latency",
                    lineage_kind=LineageKind.INDEPENDENT,
                    provenance_refs=[f"dashboard:{datasource_type}:shared-latency"],
                )
            ],
            provenance_refs=[f"dashboard:{datasource_type}:shared-latency"],
        )
        service.review_candidate(candidate.id, approved=True, reviewer="operator")
        candidates.append(candidate)

    _, revision = service.evaluate_candidate(candidates[0].id, live_verified=True)

    assert revision is not None
    assert revision.resolver_payload["mappings"] == [
        {
            "metric_pattern": "SharedLatencyMetric",
            "confidence": 0.73,
            "context_datasource_types": ["cloudwatch"],
        },
        {
            "metric_pattern": "SharedLatencyMetric",
            "confidence": 0.91,
            "context_datasource_types": ["prometheus"],
        },
    ]
    with store._conn() as conn:
        rows = conn.execute(
            """SELECT projection_key, confidence, context_datasource_types
               FROM signal_metric_mappings
               WHERE tenant_id=? AND governance_ref=? AND review_state IN ('approved', 'trusted')
               ORDER BY context_datasource_types""",
            (revision.tenant_id, revision.knowledge_id),
        ).fetchall()
    assert len(rows) == 2
    assert len({row["projection_key"] for row in rows}) == 2
    assert [(row["confidence"], json.loads(row["context_datasource_types"])) for row in rows] == [
        (0.73, ["cloudwatch"]),
        (0.91, ["prometheus"]),
    ]

    resolved = store.resolve_metric_signal_details(
        [
            MetricEntry(
                name="SharedLatencyMetric",
                datasource_uid=datasource_type,
                datasource_name=datasource_type,
                datasource_type=datasource_type,
                query_language=query_language,
            )
            for datasource_type, query_language in (("cloudwatch", "cloudwatch"), ("prometheus", "promql"))
        ],
        context_service="checkout",
    )
    assert [(item.entry.datasource_type, item.confidence) for item in resolved] == [
        ("prometheus", 0.91),
        ("cloudwatch", 0.73),
    ]

    with store._conn() as conn:
        conn.execute(
            """DELETE FROM signal_metric_mappings
               WHERE tenant_id=? AND governance_ref=? AND context_datasource_types='["cloudwatch"]'""",
            (revision.tenant_id, revision.knowledge_id),
        )
    with pytest.raises(RuntimeError, match="incomplete resolver projection"):
        store._validated_projection_audit_token()


def test_projection_audit_converges_duplicate_legacy_variant_confidences(tmp_path):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "duplicate-legacy-variant.db"
    store = SignalStore(db_path=db_path)
    service = KnowledgeService(KnowledgeRepository(db_path), signal_store=store)
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:duplicate-legacy-variant",
        typed_payload={
            "metric_pattern": "shared_latency_seconds",
            "confidence": 0.4,
            "context_datasource_types": ["prometheus"],
        },
        proposition={
            "subject_ref": "concept:shared-latency",
            "predicate": "represented_by",
            "object_ref": "concept:shared-latency-seconds",
            "concept_ref": "signal:shared_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:duplicate-legacy-variant"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    duplicated = revision.model_copy(
        update={
            "resolver_payload": {
                "mappings": [
                    {
                        "metric_pattern": "shared_latency_seconds",
                        "confidence": 0.4,
                        "context_datasource_types": ["prometheus"],
                    },
                    {
                        "metric_pattern": "shared_latency_seconds",
                        "confidence": 0.9,
                        "context_datasource_types": ["prometheus"],
                    },
                ]
            }
        }
    )

    with store.transaction() as conn:
        conn.execute(
            """UPDATE operational_knowledge_revisions SET content_json=?
               WHERE tenant_id=? AND knowledge_id=? AND revision=?""",
            (
                duplicated.model_dump_json(),
                duplicated.tenant_id,
                duplicated.knowledge_id,
                duplicated.revision,
            ),
        )
        store.sync_governed_revision(duplicated, connection=conn)
        projection = conn.execute(
            """SELECT * FROM signal_metric_mappings
               WHERE tenant_id=? AND governance_ref=? AND review_state IN ('approved', 'trusted')""",
            (duplicated.tenant_id, duplicated.knowledge_id),
        ).fetchone()
        authority = conn.execute(
            """SELECT content_json, review_state, lifecycle_status, eligibility
               FROM operational_knowledge_revisions
               WHERE tenant_id=? AND knowledge_id=? AND revision=?""",
            (duplicated.tenant_id, duplicated.knowledge_id, duplicated.revision),
        ).fetchone()

    assert projection["confidence"] == 0.9
    assert projection_matches_authority(projection, authority) == (True, "validated")
    assert store._validated_projection_audit_token() is not None


def test_learning_index_reraises_rebuild_failures(tmp_path, monkeypatch):
    from tacit.signals.migrations import ensure_learning_index

    db_path = tmp_path / "learning-index.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    def fail_rebuild(*_args, **_kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    try:
        conn.execute("""CREATE VIRTUAL TABLE learning_context_fts USING fts5(
                source_kind, source_id, backend_name, dashboard_uid,
                dashboard_title, dashboard_tags, panel_title, metric_name,
                query_text, service, signal_type, review_state, reason,
                provenance, indexed_at
            )""")
        monkeypatch.setattr("tacit.signals.migrations.prepare_learning_index_rebuild", fail_rebuild)

        with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
            ensure_learning_index(conn)
    finally:
        conn.close()


def test_startup_preserves_exact_projection_validated_by_immutable_revision(tmp_path):
    from tacit.knowledge.enums import KnowledgeKind
    from tacit.knowledge.models import KnowledgeScope
    from tacit.knowledge.service import KnowledgeService

    db_path = tmp_path / "governed-projection-valid.db"
    service = KnowledgeService(KnowledgeRepository(db_path))
    candidate = service.create_candidate(
        kind=KnowledgeKind.SIGNAL_MAPPING,
        payload_ref="dashboard:checkout",
        typed_payload={"metric_pattern": "checkout_latency_seconds", "confidence": 0.91},
        proposition={
            "subject_ref": "concept:request-latency",
            "predicate": "represented_by",
            "object_ref": "concept:checkout_latency_seconds",
            "concept_ref": "signal:request_latency",
        },
        scope=KnowledgeScope(service_refs=["entity:service:checkout"]),
        provenance_refs=["dashboard:checkout"],
    )
    service.review_candidate(candidate.id, approved=True, reviewer="operator")
    _, revision = service.evaluate_candidate(candidate.id, live_verified=True)
    assert revision is not None
    with sqlite3.connect(db_path) as conn:
        before = conn.execute(
            """SELECT content_json, semantic_fingerprint FROM operational_knowledge_revisions
               WHERE tenant_id='default' AND knowledge_id=? AND revision=?""",
            (revision.knowledge_id, revision.revision),
        ).fetchone()

    reopened = SignalStore(db_path=db_path)
    resolved = reopened.resolve_signal_details(
        "request_latency",
        [_metric_entry("checkout_latency_seconds")],
        context_service="checkout",
    )
    with sqlite3.connect(db_path) as conn:
        after = conn.execute(
            """SELECT content_json, semantic_fingerprint FROM operational_knowledge_revisions
               WHERE tenant_id='default' AND knowledge_id=? AND revision=?""",
            (revision.knowledge_id, revision.revision),
        ).fetchone()

    assert after == before
    assert len(resolved) == 1
    assert resolved[0].knowledge_revision_ref is not None
    assert resolved[0].knowledge_revision_ref.knowledge_revision == revision.revision


def test_source_reconciliation_never_mutates_governed_projection(signal_store):
    for governance_ref in ("", "knowledge:checkout-latency"):
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            source_type="dashboard_ingest",
            source_refs=["grafana:checkout"],
            governance_ref=governance_ref,
            governance_revision=1 if governance_ref else 0,
            review_state="approved",
        )

    signal_store.reconcile_mapping_source(
        tenant_id="default",
        source_type="dashboard_ingest",
        source_ref="grafana:checkout",
        active_pairs=set(),
    )

    with signal_store._conn() as conn:
        rows = conn.execute("""SELECT governance_ref, source_refs, review_state FROM signal_metric_mappings
               WHERE metric_pattern='checkout_latency_seconds' ORDER BY governance_ref""").fetchall()
    assert [dict(row) for row in rows] == [
        {
            "governance_ref": "knowledge:checkout-latency",
            "source_refs": '["grafana:checkout"]',
            "review_state": "approved",
        }
    ]


def test_excluding_preferred_revision_reveals_same_pattern_fallback(signal_store):
    from tacit.knowledge.usage import KnowledgeRevisionRef

    signal_store.add_mapping(
        "request_latency",
        "shared_latency_seconds",
        confidence=0.95,
        source_type="operational_knowledge",
        governance_ref="knowledge:tenant-override",
        governance_revision=2,
        review_state="approved",
        tenant_id="default",
    )
    signal_store._add_bootstrap_mapping(
        "request_latency",
        "shared_latency_seconds",
        confidence=0.8,
        review_state="trusted",
        tenant_id=GLOBAL_BOOTSTRAP_TENANT_ID,
    )

    matches = signal_store.resolve_signal_details(
        "request_latency",
        [_metric_entry("shared_latency_seconds")],
        tenant_id="default",
        excluded_knowledge_refs={KnowledgeRevisionRef("knowledge:tenant-override", 2)},
    )

    assert len(matches) == 1
    assert matches[0].entry.name == "shared_latency_seconds"
    assert matches[0].governance_ref == ""


def test_legacy_ungoverned_learned_mapping_is_quarantined_on_startup(tmp_path):
    db_path = tmp_path / "legacy-ungoverned.db"
    store = SignalStore(db_path=db_path)
    store.add_mapping(
        "request_latency",
        "private_latency_seconds",
        confidence=0.9,
        source_type="teach",
        review_state="trusted",
    )
    assert store.resolve_signal("request_latency", [_metric_entry("private_latency_seconds")])

    reopened = SignalStore(db_path=db_path)

    assert reopened.resolve_signal("request_latency", [_metric_entry("private_latency_seconds")]) == []
    with reopened._conn() as conn:
        row = conn.execute("""SELECT review_state FROM signal_metric_mappings
               WHERE signal_type='request_latency' AND metric_pattern='private_latency_seconds'""").fetchone()
    assert row["review_state"] == "candidate"


def test_legacy_governed_mapping_without_authoritative_revision_is_quarantined(tmp_path, monkeypatch):
    monkeypatch.setattr("tacit.signals.store._PROJECTION_AUDIT_BATCH_SIZE", 2)
    db_path = tmp_path / "legacy-governed-revision.db"
    store = SignalStore(db_path=db_path)
    now = time.time()
    with store._conn() as conn:
        for metric, knowledge_ref, governance_revision, source_refs in (
            ("mutable_source_ref_seconds", "knowledge-mutable", 0, '["knowledge-mutable@3"]'),
            ("zero_source_ref_seconds", "knowledge-zero", 0, '["knowledge-zero@0"]'),
            ("negative_revision_seconds", "knowledge-negative", -1, '["knowledge-negative@8"]'),
            ("authoritative_revision_seconds", "knowledge-valid", 2, '["knowledge-valid@99"]'),
        ):
            conn.execute(
                """INSERT INTO signal_metric_mappings
                       (tenant_id, signal_type, metric_pattern, confidence, source_type,
                        source_refs, governance_ref, governance_revision, review_state,
                        created_at, last_seen)
                   VALUES ('default', 'request_latency', ?, 0.9, 'operational_knowledge',
                           ?, ?, ?, 'approved', ?, ?)""",
                (metric, source_refs, knowledge_ref, governance_revision, now, now),
            )

    with capture_logs() as logs:
        reopened = SignalStore(db_path=db_path)

    for metric in (
        "authoritative_revision_seconds",
        "mutable_source_ref_seconds",
        "zero_source_ref_seconds",
        "negative_revision_seconds",
    ):
        assert reopened.resolve_signal("request_latency", [_metric_entry(metric)]) == []
    with reopened._conn() as conn:
        rows = conn.execute("""SELECT governance_ref, review_state, governance_revision
               FROM signal_metric_mappings
               WHERE governance_ref!=''
               ORDER BY governance_ref""").fetchall()
    assert [dict(row) for row in rows] == [
        {
            "governance_ref": "knowledge-mutable",
            "review_state": "candidate",
            "governance_revision": 0,
        },
        {
            "governance_ref": "knowledge-negative",
            "review_state": "candidate",
            "governance_revision": -1,
        },
        {
            "governance_ref": "knowledge-valid",
            "review_state": "candidate",
            "governance_revision": 2,
        },
        {
            "governance_ref": "knowledge-zero",
            "review_state": "candidate",
            "governance_revision": 0,
        },
    ]
    diagnostic = next(log for log in logs if log["event"] == "governed_signal_mapping_revision_unknown")
    assert diagnostic["mappings"] == 4
    assert diagnostic["quarantined"] == 4
    patterns = (
        "authoritative_revision_seconds",
        "mutable_source_ref_seconds",
        "negative_revision_seconds",
        "zero_source_ref_seconds",
    )
    assert diagnostic["sample_pattern_fingerprints"] == sorted(
        hashlib.sha256(pattern.encode()).hexdigest()[:16] for pattern in patterns
    )
    assert all(pattern not in repr(diagnostic) for pattern in patterns)


def test_reverse_resolution_includes_empty_signal_name_and_negative_mapping_id(signal_store):
    now = time.time()
    with signal_store._conn() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO signal_types
               (signal_type, description, category, unit, created_at, updated_at)
               VALUES ('', 'Legacy unnamed signal', '', '', ?, ?)""",
            (now, now),
        )
        conn.execute(
            """INSERT INTO signal_metric_mappings
               (id, tenant_id, signal_type, metric_pattern, confidence, source_type,
                review_state, created_at, last_seen)
               VALUES (-31, ?, '', 'legacy_unnamed_metric', 0.9, 'bootstrap',
                       'trusted', ?, ?)""",
            (GLOBAL_BOOTSTRAP_TENANT_ID, now, now),
        )

    matches = signal_store.resolve_metric_signal_details(
        [_metric_entry("legacy_unnamed_metric")],
    )

    assert [(match.signal_type, match.entry.name) for match in matches] == [("", "legacy_unnamed_metric")]
    with signal_store._conn() as conn:
        initial_plan = conn.execute(
            """EXPLAIN QUERY PLAN
               SELECT * FROM signal_metric_mappings INDEXED BY idx_smm_active_reverse
               WHERE tenant_id=? AND review_state IN ('approved', 'trusted')
               ORDER BY id LIMIT ?""",
            (GLOBAL_BOOTSTRAP_TENANT_ID, 500),
        ).fetchall()
        continuation_plan = conn.execute(
            """EXPLAIN QUERY PLAN
               SELECT * FROM signal_metric_mappings INDEXED BY idx_smm_active_reverse
               WHERE tenant_id=? AND review_state IN ('approved', 'trusted') AND id>?
               ORDER BY id LIMIT ?""",
            (GLOBAL_BOOTSTRAP_TENANT_ID, -31, 500),
        ).fetchall()
    for plan in (initial_plan, continuation_plan):
        details = [str(row["detail"]) for row in plan]
        assert any("idx_smm_active_reverse" in detail for detail in details)
        assert not any("TEMP B-TREE" in detail for detail in details)


def test_legacy_signal_data_and_custom_definitions_migrate_to_pinned_tenant(tmp_path):
    db_path = tmp_path / "legacy-pinned-signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '', unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT, signal_type TEXT NOT NULL,
                metric_pattern TEXT NOT NULL, confidence REAL NOT NULL DEFAULT 0.5,
                context_services TEXT NOT NULL DEFAULT '[]',
                context_datasource_types TEXT NOT NULL DEFAULT '[]',
                context_environments TEXT NOT NULL DEFAULT '[]', context_archetypes TEXT NOT NULL DEFAULT '[]',
                source_type TEXT NOT NULL DEFAULT 'bootstrap', source_refs TEXT NOT NULL DEFAULT '[]',
                inference_version TEXT NOT NULL DEFAULT '', review_state TEXT NOT NULL DEFAULT 'trusted',
                use_count INTEGER NOT NULL DEFAULT 0, positive_feedback INTEGER NOT NULL DEFAULT 0,
                negative_feedback INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL, last_seen REAL NOT NULL,
                UNIQUE(signal_type, metric_pattern)
            );
            CREATE VIRTUAL TABLE learning_context_fts USING fts5(
                source_kind, source_id UNINDEXED, backend_name UNINDEXED, dashboard_uid UNINDEXED,
                dashboard_title, dashboard_tags, panel_title, metric_name, query_text, service,
                signal_type, review_state UNINDEXED, reason, provenance, indexed_at UNINDEXED
            );
            CREATE TABLE rejected_signal_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT, dashboard_uid TEXT NOT NULL DEFAULT '',
                backend_name TEXT NOT NULL DEFAULT '', metric TEXT NOT NULL,
                signal_family TEXT NOT NULL DEFAULT '', signal_name TEXT NOT NULL DEFAULT '',
                score REAL NOT NULL DEFAULT 0.0, margin REAL NOT NULL DEFAULT 0.0,
                why_not TEXT NOT NULL DEFAULT '', evidence TEXT NOT NULL DEFAULT '[]',
                inference_version TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL
            );
            INSERT INTO signal_types VALUES
                ('request_latency', 'Built in', 'latency', 'seconds', 1, 1),
                ('acme_queue_pressure', 'Acme only', 'saturation', 'count', 1, 1);
            INSERT INTO signal_metric_mappings
                (signal_type, metric_pattern, confidence, source_type, created_at, last_seen)
            VALUES
                ('request_latency', 'http_request_duration_seconds', 0.9, 'bootstrap', 1, 1),
                ('acme_queue_pressure', 'acme_queue_waiting', 0.9, 'teach', 1, 1);
            INSERT INTO learning_context_fts
                (source_kind, source_id, dashboard_title, metric_name, review_state, indexed_at)
            VALUES ('dashboard_panel', 'legacy-acme', 'Legacy Acme', 'acme_queue_waiting', 'approved', 1);
            INSERT INTO rejected_signal_candidates (metric, why_not, created_at)
            VALUES ('legacy_rejected_metric', 'low_score', 1);
        """)

    store = SignalStore(
        db_path=db_path,
        runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
    )

    tenant_a = {row["signal_type"]: row for row in store.list_signal_types(tenant_id="tenant-a")}
    wildcard = SignalStore(
        db_path=db_path,
        runtime_settings=Settings(knowledge_tenant_id="*", api_auth_enabled=True),
    )
    tenant_b = {row["signal_type"]: row for row in wildcard.list_signal_types(tenant_id="tenant-b")}
    with store._conn() as conn:
        mapping_tenants = {
            row["metric_pattern"]: row["tenant_id"]
            for row in conn.execute("SELECT metric_pattern, tenant_id FROM signal_metric_mappings").fetchall()
        }
        rejected_tenant = conn.execute(
            "SELECT tenant_id FROM rejected_signal_candidates WHERE metric='legacy_rejected_metric'"
        ).fetchone()["tenant_id"]

    assert "acme_queue_pressure" in tenant_a
    assert "acme_queue_pressure" not in tenant_b
    assert "request_latency" in tenant_a and "request_latency" in tenant_b
    assert tenant_a["request_latency"]["description"] == "Built in"
    assert tenant_b["request_latency"]["description"] != "Built in"
    assert mapping_tenants["acme_queue_waiting"] == "tenant-a"
    assert mapping_tenants["http_request_duration_seconds"] == GLOBAL_BOOTSTRAP_TENANT_ID
    assert rejected_tenant == "tenant-a"
    assert store.search_learning_context("legacy", tenant_id="tenant-a")
    assert wildcard.search_learning_context("legacy", tenant_id="tenant-b") == []


def test_legacy_alerts_migrate_to_pinned_tenant(tmp_path):
    db_path = tmp_path / "legacy-alerts.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE ingested_alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT, alert_uid TEXT NOT NULL,
                backend_name TEXT NOT NULL DEFAULT '', source_vendor TEXT NOT NULL DEFAULT '',
                source_instance TEXT NOT NULL DEFAULT '', external_id TEXT NOT NULL DEFAULT '',
                fingerprint TEXT NOT NULL DEFAULT '', alert_title TEXT NOT NULL DEFAULT '',
                alert_tags TEXT NOT NULL DEFAULT '[]', condition TEXT NOT NULL DEFAULT '',
                severity TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1,
                labels TEXT NOT NULL DEFAULT '{}', annotations TEXT NOT NULL DEFAULT '{}',
                metrics_found TEXT NOT NULL DEFAULT '[]', query_transformations TEXT NOT NULL DEFAULT '[]',
                service_hints TEXT NOT NULL DEFAULT '[]', dashboard_uid TEXT NOT NULL DEFAULT '',
                panel_title TEXT NOT NULL DEFAULT '', source_url TEXT NOT NULL DEFAULT '',
                provenance_url TEXT NOT NULL DEFAULT '', confidence REAL NOT NULL DEFAULT 0.0,
                stale INTEGER NOT NULL DEFAULT 0, missing_since REAL,
                status TEXT NOT NULL DEFAULT 'pending', signals_inferred TEXT NOT NULL DEFAULT '[]',
                first_seen_at REAL NOT NULL, last_seen_at REAL NOT NULL,
                updated_at REAL NOT NULL, created_at REAL NOT NULL, reviewed_at REAL,
                UNIQUE(alert_uid, backend_name)
            );
            INSERT INTO ingested_alerts
                (alert_uid, backend_name, alert_title, first_seen_at, last_seen_at, updated_at, created_at)
            VALUES ('legacy-alert', 'grafana', 'Legacy Alert', 1, 1, 1, 1);
        """)

    store = SignalStore(
        db_path=db_path,
        runtime_settings=Settings(knowledge_tenant_id="tenant-a"),
    )

    alert = store.get_ingested_alert("legacy-alert", "grafana", tenant_id="tenant-a")
    assert alert is not None
    assert alert["tenant_id"] == "tenant-a"
    assert "generation_fingerprint" in alert
    with store._conn() as conn:
        columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingested_alerts)")}
        assert "generation_fingerprint" in columns
        assert conn.execute("""SELECT 1 FROM ingested_alerts
               WHERE tenant_id='default' AND alert_uid='legacy-alert'""").fetchone() is None


@pytest.fixture
def signal_store_with_bootstrap(tmp_path, monkeypatch):
    """SignalStore loaded with bootstrap signals.yaml.

    Also redirects the global ``get_signal_store`` accessor to this fresh store so
    helpers like ``infer_signals_from_metrics`` (which resolve the store globally)
    don't read the developer's persisted ``data/tacit_signals.db``. Without
    this, a stale local DB can make tests pass locally while failing on a hermetic
    CI runner.
    """
    db_path = tmp_path / "test_signals.db"
    store = SignalStore(db_path=db_path)
    store.load_from_yaml()
    import tacit.dashboard_ingest as _ingest
    import tacit.signals as _signals

    monkeypatch.setattr(_signals, "get_signal_store", lambda: store)
    monkeypatch.setattr(_ingest, "get_signal_store", lambda: store)
    return store


@pytest.fixture
def sample_catalog():
    """A sample metric catalog with custom SSO metrics."""
    return [
        MetricEntry(
            name="sso_auth_requests_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={sso-gateway}"],
        ),
        MetricEntry(
            name="sso_auth_failures_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={sso-gateway}", "reason={expired_token,invalid_cert}"],
        ),
        MetricEntry(
            name="sso_auth_latency_seconds_bucket",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
            dimensions=["service={sso-gateway}", "le={0.1,0.5,1,5}"],
        ),
        MetricEntry(
            name="http_requests_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        ),
        MetricEntry(
            name="container_cpu_usage_seconds_total",
            datasource_uid="prom-1",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        ),
    ]


# ── Signal Store basics ──────────────────────────────────────────────────────


class TestSignalStoreBasics:
    def test_register_and_list_signal_types(self, signal_store):
        signal_store.register_signal_type(
            "request_latency", description="Request latency", category="latency", unit="s"
        )
        signal_store.register_signal_type("error_rate", description="Error rate", category="errors", unit="percentunit")

        types = signal_store.list_signal_types()
        assert len(types) == 2
        names = {t["signal_type"] for t in types}
        assert names == {"request_latency", "error_rate"}

    def test_register_signal_type_upsert(self, signal_store):
        signal_store.register_signal_type("test_signal", description="v1")
        signal_store.register_signal_type("test_signal", description="v2")

        types = signal_store.list_signal_types()
        assert len(types) == 1
        assert types[0]["description"] == "v2"

    def test_signal_type_listing_is_keyset_paged_and_compatibility_bounded(self, signal_store):
        now = time.time()
        with signal_store._conn() as conn:
            conn.executemany(
                """INSERT INTO signal_types
                   (signal_type, description, category, unit, created_at, updated_at)
                   VALUES (?, '', ?, '', ?, ?)""",
                [(f"signal_{index:04d}", f"category_{index % 3}", now, now) for index in range(501)],
            )
        signal_store.add_mapping("signal_0000", "metric_0000")

        first = signal_store.list_signal_types_page(limit=200)
        second = signal_store.list_signal_types_page(limit=200, cursor=first.next_cursor)
        third = signal_store.list_signal_types_page(limit=200, cursor=second.next_cursor)

        combined = [*first.items, *second.items, *third.items]
        assert len(combined) == 501
        assert len({item["signal_type"] for item in combined}) == 501
        assert first.has_more is True
        assert second.has_more is True
        assert third.has_more is False
        assert third.next_cursor is None
        assert next(item for item in combined if item["signal_type"] == "signal_0000")["mapping_count"] == 1
        assert len(signal_store.list_signal_types()) == 500

        with signal_store._conn() as conn:
            indexes = {
                str(row["name"])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE '%signal_types_page'"
                ).fetchall()
            }
        assert indexes == {"idx_signal_types_page", "idx_tenant_signal_types_page"}

    def test_signal_mapping_pages_cross_tenant_and_bootstrap_without_temp_sorts(self, signal_store):
        signal_type = "paged_signal"
        signal_store.register_signal_type(signal_type)
        for index, confidence in enumerate((0.7, 0.9, 0.8), 1):
            signal_store.add_mapping(
                signal_type,
                f"tenant_metric_{index}",
                confidence=confidence,
                source_type="teach",
            )
        signal_store._add_bootstrap_mapping(signal_type, "bootstrap_metric_1", confidence=0.99)
        signal_store._add_bootstrap_mapping(signal_type, "bootstrap_metric_2", confidence=0.6)

        cursor = None
        patterns: list[str] = []
        while True:
            page = signal_store.get_signal_type_page(signal_type, limit=2, cursor=cursor)
            assert page is not None
            patterns.extend(mapping["metric_pattern"] for mapping in page["mappings"])
            if not page["has_more"]:
                break
            cursor = page["next_cursor"]

        assert patterns == [
            "tenant_metric_2",
            "tenant_metric_3",
            "tenant_metric_1",
            "bootstrap_metric_1",
            "bootstrap_metric_2",
        ]
        with signal_store._conn() as conn:
            for storage_tenant in ("default", GLOBAL_BOOTSTRAP_TENANT_ID):
                plan = conn.execute(
                    """EXPLAIN QUERY PLAN
                       SELECT * FROM signal_metric_mappings INDEXED BY idx_smm_tenant_signal_page
                       WHERE tenant_id=? AND signal_type=?
                       ORDER BY confidence DESC, id DESC LIMIT ?""",
                    (storage_tenant, signal_type, 3),
                ).fetchall()
                details = [str(row["detail"]) for row in plan]
                assert any("idx_smm_tenant_signal_page" in detail for detail in details)
                assert not any("TEMP B-TREE" in detail for detail in details)

    def test_reverse_resolution_finds_mapping_beyond_compatibility_taxonomy_page(self, signal_store):
        now = time.time()
        target_signal = "zz_reverse_signal_0500"
        with signal_store._conn() as conn:
            conn.executemany(
                """INSERT INTO signal_types
                   (signal_type, description, category, unit, created_at, updated_at)
                   VALUES (?, '', 'zz-reverse', '', ?, ?)""",
                [(f"zz_reverse_signal_{index:04d}", now, now) for index in range(501)],
            )
        signal_store.add_mapping(target_signal, "late_page_metric", confidence=0.9)

        first_page_names = {row["signal_type"] for row in signal_store.list_signal_types()}
        resolved = signal_store.resolve_metric_signal_details([_metric_entry("late_page_metric")])
        inferred = infer_signals_from_metrics(["late_page_metric"], store=signal_store)

        assert target_signal not in first_page_names
        assert [(match.signal_type, match.entry.name) for match in resolved] == [(target_signal, "late_page_metric")]
        assert any(
            row["source"] == "taxonomy" and row["signal_type"] == target_signal and row["metric"] == "late_page_metric"
            for row in inferred
        )
        with signal_store._conn() as conn:
            plan = conn.execute(
                """EXPLAIN QUERY PLAN
                   SELECT * FROM signal_metric_mappings INDEXED BY idx_smm_active_reverse
                   WHERE tenant_id=? AND review_state IN ('approved', 'trusted') AND id>?
                   ORDER BY id LIMIT ?""",
                ("default", 0, 500),
            ).fetchall()
        assert any("idx_smm_active_reverse" in str(row["detail"]) for row in plan)

    def test_reverse_resolution_fails_closed_when_candidate_budget_is_exceeded(self, tmp_path):
        store = SignalStore(
            db_path=tmp_path / "bounded-reverse.db",
            runtime_settings=Settings(signal_resolution_mapping_limit=10),
        )
        for index in range(11):
            signal_type = f"bounded_signal_{index:02d}"
            store.register_signal_type(signal_type)
            store.add_mapping(signal_type, "shared_metric", confidence=0.9)

        with pytest.raises(RuntimeError, match="more than 10 active mapping candidates"):
            store.resolve_metric_signal_details([_metric_entry("shared_metric")])

    def test_reverse_resolution_indexes_literal_fragments_before_pattern_checks(
        self,
        tmp_path,
        monkeypatch,
    ):
        store = SignalStore(db_path=tmp_path / "indexed-reverse.db")
        metric_count = 80
        for index in range(metric_count):
            signal_type = f"indexed_signal_{index:03d}"
            store.register_signal_type(signal_type)
            store.add_mapping(signal_type, f"service_{index:03d}_request_total", confidence=0.9)
        catalog = [_metric_entry(f"service_{index:03d}_request_total") for index in range(metric_count)]

        pattern_checks = 0
        original_matcher = _metric_matches_pattern

        def count_pattern_checks(metric_name: str, pattern: str) -> bool:
            nonlocal pattern_checks
            pattern_checks += 1
            return original_matcher(metric_name, pattern)

        monkeypatch.setattr("tacit.signals.store._metric_matches_pattern", count_pattern_checks)

        resolved = store.resolve_metric_signal_details(catalog)

        assert len(resolved) == metric_count
        assert pattern_checks < metric_count * 4

    def test_reverse_resolution_fails_closed_at_aggregate_pattern_budget(self, tmp_path):
        store = SignalStore(
            db_path=tmp_path / "pattern-budget.db",
            runtime_settings=Settings(
                _env_file=None,
                signal_resolution_pattern_check_limit=100,
            ),
        )
        for index in range(11):
            signal_type = f"broad_signal_{index:02d}"
            store.register_signal_type(signal_type)
            store.add_mapping(signal_type, "*", confidence=0.9)
        catalog = [_metric_entry(f"metric_{index:02d}") for index in range(10)]

        with pytest.raises(RuntimeError, match="100-check pattern safety limit"):
            store.resolve_metric_signal_details(catalog)

    def test_reverse_resolution_prefilter_preserves_glob_character_classes(self, signal_store):
        signal_store.register_signal_type("regional_latency")
        signal_store.add_mapping(
            "regional_latency",
            "service_[ab]*_latency_seconds",
            confidence=0.9,
        )

        resolved = signal_store.resolve_metric_signal_details([_metric_entry("service_a_checkout_latency_seconds")])

        assert [(match.signal_type, match.entry.name) for match in resolved] == [
            ("regional_latency", "service_a_checkout_latency_seconds")
        ]

    @pytest.mark.parametrize(
        ("pattern", "metric"),
        [
            ("service_[!z]*_latency_seconds", "service_a_latency_seconds"),
            ("service_[]abc]*_latency_seconds", "service_a_latency_seconds"),
        ],
    )
    def test_reverse_resolution_prefilter_handles_character_class_edges(
        self,
        signal_store,
        pattern,
        metric,
    ):
        signal_store.register_signal_type("character_class_latency")
        signal_store.add_mapping(
            "character_class_latency",
            pattern,
            confidence=0.9,
        )

        resolved = signal_store.resolve_metric_signal_details([_metric_entry(metric)])

        assert [(match.signal_type, match.entry.name) for match in resolved] == [("character_class_latency", metric)]

    def test_get_signal_type_not_found(self, signal_store):
        assert signal_store.get_signal_type("nonexistent") is None

    def test_stats_empty(self, signal_store):
        stats = signal_store.stats()
        assert stats["signal_types"] == 0
        assert stats["metric_mappings"] == 0


# ── Signal ↔ metric mappings ────────────────────────────────────────────────


class TestSignalMappings:
    def test_add_and_retrieve_mapping(self, signal_store):
        signal_store.register_signal_type("request_latency")
        signal_store._add_bootstrap_mapping(
            "request_latency",
            "http_request_duration_seconds",
            confidence=0.95,
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["metric_pattern"] == "http_request_duration_seconds"
        assert mappings[0]["confidence"] == 0.95

    def test_many_to_many_signal_metric(self, signal_store):
        """One metric can map to multiple signals (many-to-many)."""
        signal_store.add_mapping("saturation", "queue_depth_total", confidence=0.8)
        signal_store.add_mapping("throughput_mismatch", "queue_depth_total", confidence=0.6)
        signal_store.add_mapping("downstream_outage", "queue_depth_total", confidence=0.4)

        # Same metric under 3 different signals
        sat = signal_store.get_mappings_for_signal("saturation")
        thr = signal_store.get_mappings_for_signal("throughput_mismatch")
        out = signal_store.get_mappings_for_signal("downstream_outage")

        assert len(sat) == 1
        assert len(thr) == 1
        assert len(out) == 1
        assert sat[0]["metric_pattern"] == "queue_depth_total"
        assert thr[0]["metric_pattern"] == "queue_depth_total"

    def test_multiple_metrics_per_signal(self, signal_store):
        """One signal can map to many metrics."""
        signal_store.add_mapping("request_latency", "http_request_duration_seconds", 0.95)
        signal_store.add_mapping("request_latency", "payments_api_latency_ms", 0.8)
        signal_store.add_mapping("request_latency", "gateway_request_duration", 0.7)

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 3
        # Sorted by confidence desc
        assert mappings[0]["confidence"] == 0.95
        assert mappings[2]["confidence"] == 0.7

    def test_add_mapping_upsert_keeps_max_confidence(self, signal_store):
        signal_store.add_mapping("test_signal", "test_metric", confidence=0.5)
        signal_store.add_mapping("test_signal", "test_metric", confidence=0.9)

        mappings = signal_store.get_mappings_for_signal("test_signal")
        assert len(mappings) == 1
        assert mappings[0]["confidence"] == 0.9

    def test_provenance_tracking(self, signal_store):
        signal_store.add_mapping(
            "auth_latency",
            "sso_auth_latency_seconds",
            confidence=0.8,
            source_type="dashboard_ingest",
            source_refs=["dashboard-uid-123"],
        )

        mappings = signal_store.get_mappings_for_signal("auth_latency")
        assert mappings[0]["source_type"] == "dashboard_ingest"
        assert "dashboard-uid-123" in mappings[0]["source_refs"]

    def test_feedback_recording(self, signal_store):
        signal_store.add_mapping("test", "test_metric", 0.7)
        signal_store.record_feedback("test", "test_metric", positive=True)
        signal_store.record_feedback("test", "test_metric", positive=True)
        signal_store.record_feedback("test", "test_metric", positive=False)

        mappings = signal_store.get_mappings_for_signal("test")
        assert mappings[0]["positive_feedback"] == 2
        assert mappings[0]["negative_feedback"] == 1


# ── Context filtering ────────────────────────────────────────────────────────


class TestContextFiltering:
    def test_empty_context_matches_all(self):
        mapping = {
            "context_services": [],
            "context_datasource_types": [],
            "context_archetypes": [],
            "context_environments": [],
        }
        assert _context_matches(mapping, "any-svc", "prometheus", "latency", "prod")

    def test_service_context_filter(self):
        mapping = {
            "context_services": ["sso-gateway"],
            "context_datasource_types": [],
            "context_archetypes": [],
            "context_environments": [],
        }
        assert _context_matches(mapping, "sso-gateway", "", "", "")
        assert not _context_matches(mapping, "payment-service", "", "", "")

    def test_datasource_type_context_filter(self):
        mapping = {
            "context_services": [],
            "context_datasource_types": ["prometheus"],
            "context_archetypes": [],
            "context_environments": [],
        }
        assert _context_matches(mapping, "", "prometheus", "", "")
        assert not _context_matches(mapping, "", "cloudwatch", "", "")

    def test_context_filter_with_signal_store(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "sso_specific_latency",
            confidence=0.9,
            context_services=["sso-gateway"],
        )
        signal_store.add_mapping(
            "request_latency",
            "generic_latency",
            confidence=0.8,
        )

        # With SSO context — both match
        mappings = signal_store.get_mappings_for_signal("request_latency", context_service="sso-gateway")
        assert len(mappings) == 2

        # With different service — only generic matches
        mappings = signal_store.get_mappings_for_signal("request_latency", context_service="payment-service")
        assert len(mappings) == 1
        assert mappings[0]["metric_pattern"] == "generic_latency"

    def test_context_specific_mapping_penalized_without_context(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_specific_latency",
            confidence=0.9,
            context_services=["checkout"],
        )
        signal_store.add_mapping(
            "request_latency",
            "generic_latency",
            confidence=0.8,
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")

        assert [m["metric_pattern"] for m in mappings] == [
            "generic_latency",
            "checkout_specific_latency",
        ]
        assert mappings[1]["effective_confidence"] == pytest.approx(0.63, abs=0.001)

    def test_context_specific_mapping_not_penalized_with_matching_context(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_specific_latency",
            confidence=0.9,
            context_services=["checkout"],
        )
        signal_store.add_mapping(
            "request_latency",
            "generic_latency",
            confidence=0.8,
        )

        mappings = signal_store.get_mappings_for_signal("request_latency", context_service="checkout")

        assert mappings[0]["metric_pattern"] == "checkout_specific_latency"
        assert mappings[0]["effective_confidence"] == pytest.approx(0.9, abs=0.001)

    def test_context_penalty_does_not_make_trusted_mapping_disappear(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "low_confidence_checkout_latency",
            confidence=0.2,
            context_services=["checkout"],
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")

        assert len(mappings) == 1
        assert mappings[0]["metric_pattern"] == "low_confidence_checkout_latency"
        assert mappings[0]["effective_confidence"] == pytest.approx(0.14, abs=0.001)

    def test_conflict_preserves_global_mapping_context(self, signal_store):
        signal_store.add_mapping("request_latency", "latency_seconds", confidence=0.5)
        signal_store.add_mapping(
            "request_latency",
            "latency_seconds",
            confidence=0.6,
            context_services=["checkout"],
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency", include_decayed=True)

        assert mappings[0]["context_services"] == []
        assert mappings[0]["source_type"] == "teach"


# ── Confidence decay ─────────────────────────────────────────────────────────


class TestConfidenceDecay:
    def test_bootstrap_no_decay(self):
        mapping = {
            "confidence": 0.9,
            "source_type": "bootstrap",
            "last_seen": time.time() - 365 * 86400,  # 1 year ago
            "positive_feedback": 0,
            "negative_feedback": 0,
        }
        eff = _effective_confidence(mapping, time.time())
        assert eff == 0.9  # no decay for bootstrap

    def test_learned_mapping_decays(self):
        now = time.time()
        mapping = {
            "confidence": 0.9,
            "source_type": "dashboard_ingest",
            "last_seen": now - 90 * 86400,  # 90 days ago = 1 half-life
            "positive_feedback": 0,
            "negative_feedback": 0,
        }
        eff = _effective_confidence(mapping, now)
        assert 0.4 < eff < 0.5  # ~0.45 after one half-life

    def test_positive_feedback_boosts(self):
        now = time.time()
        mapping = {
            "confidence": 0.5,
            "source_type": "teach",
            "last_seen": now,  # fresh
            "positive_feedback": 10,
            "negative_feedback": 0,
        }
        eff = _effective_confidence(mapping, now)
        assert eff > 0.5  # boosted by all-positive feedback
        assert eff == pytest.approx(0.5 * 1.3, abs=0.01)

    def test_negative_feedback_penalizes(self):
        now = time.time()
        mapping = {
            "confidence": 0.5,
            "source_type": "teach",
            "last_seen": now,
            "positive_feedback": 0,
            "negative_feedback": 10,
        }
        eff = _effective_confidence(mapping, now)
        assert eff < 0.5  # penalized
        assert eff == pytest.approx(0.5 * 0.7, abs=0.01)

    def test_min_confidence_floor(self):
        now = time.time()
        mapping = {
            "confidence": 0.01,
            "source_type": "dashboard_ingest",
            "last_seen": now - 365 * 86400,
            "positive_feedback": 0,
            "negative_feedback": 100,
        }
        eff = _effective_confidence(mapping, now)
        assert eff >= 0.05  # never drops below MIN_CONFIDENCE


# ── Metric pattern matching ──────────────────────────────────────────────────


class TestMetricPatternMatching:
    def test_exact_match(self):
        assert _metric_matches_pattern("http_requests_total", "http_requests_total")

    def test_glob_wildcard_prefix(self):
        assert _metric_matches_pattern("sso_auth_failures_total", "*auth*fail*")

    def test_glob_wildcard_suffix(self):
        assert _metric_matches_pattern("http_request_duration_seconds_bucket", "*_duration_seconds*")

    def test_glob_no_match(self):
        assert not _metric_matches_pattern("cpu_usage_total", "*auth*")

    def test_substring_match(self):
        assert _metric_matches_pattern("my_custom_latency_metric", "latency")

    def test_no_match(self):
        assert not _metric_matches_pattern("cpu_usage", "memory")


# ── Signal resolution ────────────────────────────────────────────────────────


class TestSignalResolution:
    def test_input_character_admission_precedes_utf8_traversal(self):
        class EncodeMustNotRun(str):
            def encode(self, *_args, **_kwargs):
                raise AssertionError("UTF-8 traversal ran before aggregate characters")

        with pytest.raises(ResolutionInputWorkLimitError) as exc_info:
            admit_resolution_input_text(
                [EncodeMustNotRun("too-long")],
                limits=ResolutionInputTextLimits(max_total_characters=2),
            )

        assert exc_info.value.dimension == "total_input_characters"

    def test_aggregate_budget_reserves_mapping_catalog_product_before_matching(
        self,
        signal_store,
        monkeypatch,
    ):
        signal_store._add_bootstrap_mapping("bounded_signal", "metric_a", 0.9)
        signal_store._add_bootstrap_mapping("bounded_signal", "metric_b", 0.8)
        catalog = [_metric_entry(f"metric_{suffix}") for suffix in ("a", "b", "c")]
        budget = SignalResolutionWorkBudget(
            max_calls=1,
            max_mapping_catalog_comparisons=5,
            max_results=10,
        )
        pattern_checks = 0

        def unexpected_match(_metric_name: str, _pattern: str) -> bool:
            nonlocal pattern_checks
            pattern_checks += 1
            return False

        monkeypatch.setattr("tacit.signals.store._metric_matches_pattern", unexpected_match)

        with capture_logs() as logs, pytest.raises(SignalResolutionWorkLimitError) as exc_info:
            signal_store.resolve_signal_details(
                "bounded_signal",
                catalog,
                work_budget=budget,
            )

        assert exc_info.value.dimension == "mapping_catalog_comparisons"
        assert exc_info.value.observed == 6
        assert pattern_checks == 0
        assert budget.mapping_catalog_comparisons == 0
        [diagnostic] = [item for item in logs if item["event"] == SignalResolutionWorkLimitError.reason_code]
        assert diagnostic["dimension"] == "mapping_catalog_comparisons"
        assert diagnostic["mapping_catalog_comparison_count"] == 0
        assert diagnostic["mapping_catalog_comparison_limit"] == 5
        serialized = json.dumps(diagnostic)
        assert "bounded_signal" not in serialized
        assert "metric_a" not in serialized

    def test_reverse_resolution_reserves_aggregate_product_before_matching(
        self,
        signal_store,
        monkeypatch,
    ):
        for index in range(2):
            signal_type = f"reverse_signal_{index}"
            signal_store.register_signal_type(signal_type)
            signal_store.add_mapping(signal_type, f"reverse_metric_{index}", 0.9)
        budget = SignalResolutionWorkBudget(
            max_calls=1,
            max_mapping_catalog_comparisons=5,
            max_results=10,
        )
        pattern_checks = 0

        def unexpected_match(_metric_name: str, _pattern: str) -> bool:
            nonlocal pattern_checks
            pattern_checks += 1
            return False

        monkeypatch.setattr("tacit.signals.store._metric_matches_pattern", unexpected_match)

        with pytest.raises(SignalResolutionWorkLimitError) as exc_info:
            signal_store.resolve_metric_signal_details(
                [_metric_entry(f"reverse_metric_{index}") for index in range(3)],
                work_budget=budget,
            )

        assert exc_info.value.dimension == "mapping_catalog_comparisons"
        assert exc_info.value.observed == 6
        assert pattern_checks == 0

    def test_reverse_resolution_result_budget_allows_catalog_limit(self, tmp_path):
        store = SignalStore(
            db_path=tmp_path / "reverse-result-limit.db",
            runtime_settings=Settings(
                _env_file=None,
                signal_resolution_catalog_limit=100,
                signal_resolution_mapping_limit=10,
            ),
        )
        store.register_signal_type("broad_signal")
        store.add_mapping("broad_signal", "*", 0.9)

        resolved = store.resolve_metric_signal_details([_metric_entry(f"metric_{index}") for index in range(100)])

        assert len(resolved) == 100

    def test_aggregate_budget_bounds_result_growth_during_construction(self, signal_store):
        signal_store._add_bootstrap_mapping("broad_signal", "*", 0.9)
        budget = SignalResolutionWorkBudget(
            max_calls=1,
            max_mapping_catalog_comparisons=3,
            max_results=2,
        )

        with pytest.raises(SignalResolutionWorkLimitError) as exc_info:
            signal_store.resolve_signal_details(
                "broad_signal",
                [_metric_entry(f"metric_{index}") for index in range(3)],
                work_budget=budget,
            )

        assert exc_info.value.dimension == "results_constructed"
        assert exc_info.value.observed == 3
        assert budget.results_constructed == 2

    def test_explicit_aggregate_budget_preserves_normal_resolution(self, signal_store, sample_catalog):
        signal_store._add_bootstrap_mapping("request_rate", "http_requests_total", 0.95)
        budget = SignalResolutionWorkBudget(
            max_calls=2,
            max_mapping_catalog_comparisons=100,
            max_results=10,
        )

        resolved = signal_store.resolve_signal_details(
            "request_rate",
            sample_catalog,
            work_budget=budget,
        )

        assert [(match.entry.name, match.confidence) for match in resolved] == [("http_requests_total", 0.95)]
        assert budget.calls == 1
        assert budget.mapping_catalog_comparisons == len(sample_catalog)
        assert budget.results_constructed == 1

    def test_resolution_fails_closed_when_mapping_budget_is_exceeded(self, tmp_path):
        store = SignalStore(
            db_path=tmp_path / "mapping-budget.db",
            runtime_settings=Settings(_env_file=None, signal_resolution_mapping_limit=10),
        )
        for index in range(11):
            store._add_bootstrap_mapping(
                "bounded_signal",
                f"bounded_metric_{index}",
                0.9,
            )

        with pytest.raises(RuntimeError, match="more than 10 active mapping candidates"):
            store.resolve_signal("bounded_signal", [_metric_entry("bounded_metric_0")])

    def test_resolution_applies_mapping_cap_after_service_and_environment_scope(self, tmp_path):
        store = SignalStore(db_path=tmp_path / "mapping-scope-cap.db")
        for index in range(501):
            store.add_mapping(
                "scoped_signal",
                f"disjoint_metric_{index}",
                0.99,
                context_services=[f"service-{index}"],
                context_environments=[f"environment-{index}"],
                source_type="teach",
            )
        store.add_mapping(
            "scoped_signal",
            "checkout_metric",
            0.8,
            context_services=["checkout"],
            context_environments=["production"],
            source_type="teach",
        )

        resolved = store.resolve_signal(
            "scoped_signal",
            [_metric_entry("checkout_metric")],
            context_service="checkout",
            context_environment="production",
        )

        assert [entry.name for entry, _confidence in resolved] == ["checkout_metric"]

    def test_resolution_applies_mapping_cap_after_full_governed_scope(self, tmp_path):
        store = SignalStore(
            db_path=tmp_path / "mapping-full-scope-cap.db",
            runtime_settings=Settings(_env_file=None, signal_resolution_mapping_limit=10),
        )
        now = time.time()

        def governed_mapping(index: int, region: str) -> dict:
            return {
                "signal_type": "regional_signal",
                "metric_pattern": f"regional_metric_{index}",
                "confidence": 0.9,
                "source_type": "operational_knowledge",
                "source_refs": [f"source-{index}"],
                "review_state": "approved",
                "tenant_id": "default",
                "governance_ref": f"knowledge-{index}",
                "governance_revision": 1,
                "context_services": [],
                "context_datasource_types": [],
                "context_environments": [],
                "context_archetypes": [],
                "context_regions": [region],
                "context_clusters": [],
                "context_namespaces": [],
                "context_versions": [],
                "last_seen": now,
                "positive_feedback": 0,
                "negative_feedback": 0,
            }

        mappings = [governed_mapping(index, f"region-{index}") for index in range(11)]
        mappings.append(governed_mapping(99, "us-central1"))
        token = store.activate_pinned_governed_mappings(tenant_id="default", mappings=mappings)
        try:
            resolved = store.resolve_signal(
                "regional_signal",
                [_metric_entry("regional_metric_99")],
                knowledge_scope=KnowledgeScope(tenant_id="default", region_refs=["us-central1"]),
            )
        finally:
            store.reset_pinned_governed_mappings(token)

        assert [entry.name for entry, _confidence in resolved] == ["regional_metric_99"]

    def test_resolution_fails_closed_when_catalog_budget_is_exceeded(self, tmp_path):
        store = SignalStore(
            db_path=tmp_path / "catalog-budget.db",
            runtime_settings=Settings(_env_file=None, signal_resolution_catalog_limit=100),
        )
        store._add_bootstrap_mapping("bounded_catalog", "bounded_metric", 0.9)
        catalog = [_metric_entry(f"metric_{index}") for index in range(101)]

        with pytest.raises(RuntimeError, match="more than 100 eligible metrics"):
            store.resolve_signal("bounded_catalog", catalog)

    def test_resolve_signal_exact_match(self, signal_store, sample_catalog):
        signal_store._add_bootstrap_mapping("request_rate", "http_requests_total", 0.95)
        resolved = signal_store.resolve_signal("request_rate", sample_catalog)
        assert len(resolved) == 1
        assert resolved[0][0].name == "http_requests_total"
        assert resolved[0][1] == 0.95

    def test_resolve_signal_pattern_match(self, signal_store, sample_catalog):
        signal_store._add_bootstrap_mapping("auth_failure_count", "*auth*fail*", 0.85)
        resolved = signal_store.resolve_signal("auth_failure_count", sample_catalog)
        assert len(resolved) == 1
        assert resolved[0][0].name == "sso_auth_failures_total"

    def test_resolve_signal_multiple_matches(self, signal_store, sample_catalog):
        signal_store._add_bootstrap_mapping("auth_request_rate", "*auth*requests*", 0.8)
        resolved = signal_store.resolve_signal("auth_request_rate", sample_catalog)
        assert len(resolved) >= 1
        names = {r[0].name for r in resolved}
        assert "sso_auth_requests_total" in names

    def test_resolve_signal_no_match(self, signal_store, sample_catalog):
        signal_store._add_bootstrap_mapping("kafka_lag", "kafka_consumer_lag", 0.9)
        resolved = signal_store.resolve_signal("kafka_lag", sample_catalog)
        assert len(resolved) == 0

    def test_resolve_signals_for_archetype(self, signal_store, sample_catalog):
        """Core SSO use case: archetype says auth_requests_total but env has sso_auth_requests_total."""
        signal_store._add_bootstrap_mapping("auth_request_rate", "*auth*requests*total", 0.85)
        signal_store._add_bootstrap_mapping("auth_failure_count", "*auth*fail*total", 0.85)
        signal_store._add_bootstrap_mapping("auth_latency", "*auth*latency*", 0.8)

        signal_bindings = {
            "auth_request_rate": "auth_requests_total",
            "auth_failure_count": "failed_login_attempts_total",
            "auth_latency": "auth_latency_seconds",
        }

        subs = signal_store.resolve_signals_for_archetype(
            signal_bindings=signal_bindings,
            catalog=sample_catalog,
        )

        # auth_requests_total is NOT in catalog → should be resolved
        assert "auth_requests_total" in subs
        assert subs["auth_requests_total"] == "sso_auth_requests_total"

        # failed_login_attempts_total is NOT in catalog → should resolve to sso_auth_failures_total
        assert "failed_login_attempts_total" in subs
        assert subs["failed_login_attempts_total"] == "sso_auth_failures_total"

    def test_resolve_skips_existing_metrics(self, signal_store, sample_catalog):
        """If the default metric exists in catalog, no substitution needed."""
        signal_store._add_bootstrap_mapping("request_rate", "*requests*total", 0.9)

        subs = signal_store.resolve_signals_for_archetype(
            signal_bindings={"request_rate": "http_requests_total"},
            catalog=sample_catalog,
        )

        # http_requests_total IS in catalog → no substitution
        assert "http_requests_total" not in subs

    def test_default_presence_is_scoped_to_target_language_and_datasource(self, signal_store):
        signal_store.add_mapping(
            "request_rate",
            "prom_http_requests_total",
            confidence=0.9,
            context_datasource_types=["prometheus"],
            source_type="teach",
        )
        catalog = [
            MetricEntry(
                name="http_requests_total",
                datasource_uid="sfx-1",
                datasource_name="SignalFx",
                datasource_type="signalfx",
                query_language="signalflow",
            ),
            MetricEntry(
                name="prom_http_requests_total",
                datasource_uid="prom-1",
                datasource_name="Prometheus",
                datasource_type="prometheus",
                query_language="promql",
            ),
        ]

        subs = signal_store.resolve_signals_for_archetype(
            signal_bindings={"request_rate": "http_requests_total"},
            catalog=catalog,
            context_datasource_type="prometheus",
            target_query_language="promql",
        )

        assert subs == {"http_requests_total": "prom_http_requests_total"}

    def test_target_query_language_supplies_datasource_scope(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_custom_latency_ms",
            confidence=0.9,
            context_datasource_types=["prometheus"],
            source_type="teach",
        )
        catalog = [
            MetricEntry(
                name="checkout_custom_latency_ms",
                datasource_uid="prom-1",
                datasource_name="Prometheus",
                datasource_type="prometheus",
                query_language="promql",
            )
        ]

        resolved = signal_store.resolve_signal(
            "request_latency",
            catalog,
            target_query_language="promql",
        )

        assert [entry.name for entry, _confidence in resolved] == ["checkout_custom_latency_ms"]


# ── Metric substitution in archetypes ────────────────────────────────────────


class TestArchetypeMetricSubstitution:
    def test_apply_metric_substitutions(self):
        from tacit.archetypes.engine import _apply_metric_substitutions

        archetype = InvestigationArchetype(
            id="test_auth",
            name="Test Auth",
            problem_types=["auth_failures"],
            panels=[
                PanelTemplate(
                    title="Auth Rate",
                    queries=[
                        QueryTemplate(
                            expr="sum(rate(auth_requests_total{{{service_filter}}}[{rate_interval}]))",
                        )
                    ],
                ),
                PanelTemplate(
                    title="Auth Failures",
                    queries=[
                        QueryTemplate(
                            expr="sum(increase(failed_login_attempts_total{{{service_filter}}}[{rate_interval}]))",
                        )
                    ],
                ),
            ],
        )

        substitutions = {
            "auth_requests_total": "sso_auth_requests_total",
            "failed_login_attempts_total": "sso_auth_failures_total",
        }

        result = _apply_metric_substitutions(archetype, substitutions)

        assert "sso_auth_requests_total" in result.panels[0].queries[0].expr
        # The old metric name should be replaced — check the expr starts with the new one
        assert result.panels[0].queries[0].expr.startswith("sum(rate(sso_auth_requests_total")
        assert "sso_auth_failures_total" in result.panels[1].queries[0].expr

    def test_no_substitution_returns_same(self):
        from tacit.archetypes.engine import _apply_metric_substitutions

        archetype = InvestigationArchetype(
            id="test",
            name="Test",
            problem_types=["test"],
            panels=[
                PanelTemplate(
                    title="T",
                    queries=[QueryTemplate(expr="metric{filter}")],
                )
            ],
        )

        result = _apply_metric_substitutions(archetype, {})
        assert result is archetype  # identity — no copy needed


# ── PromQL metric extraction ────────────────────────────────────────────────


class TestPromQLExtraction:
    def test_simple_metric(self):
        metrics = extract_metrics_from_promql('http_requests_total{job="api"}')
        assert "http_requests_total" in metrics

    def test_rate_wrapped(self):
        metrics = extract_metrics_from_promql('sum(rate(http_requests_total{service="checkout"}[5m])) by (status)')
        assert "http_requests_total" in metrics
        assert "sum" not in metrics
        assert "rate" not in metrics
        assert "status" not in metrics

    def test_histogram_quantile(self):
        metrics = extract_metrics_from_promql(
            'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{job="api"}[5m])) by (le))'
        )
        assert "http_request_duration_seconds_bucket" in metrics
        assert "histogram_quantile" not in metrics

    def test_multiple_metrics(self):
        expr = 'sum(rate(http_requests_total{status=~"5.."}[5m])) / sum(rate(http_requests_total[5m]))'
        metrics = extract_metrics_from_promql(expr)
        assert "http_requests_total" in metrics

    def test_excludes_promql_keywords(self):
        metrics = extract_metrics_from_promql("topk(5, sum(rate(my_metric[5m])) by (instance))")
        assert "my_metric" in metrics
        assert "topk" not in metrics
        assert "sum" not in metrics
        assert "by" not in metrics
        assert "instance" not in metrics

    def test_custom_sso_metrics(self):
        metrics = extract_metrics_from_promql(
            'sum(rate(sso_auth_failures_total{service="sso-gateway"}[5m])) by (reason)'
        )
        assert "sso_auth_failures_total" in metrics
        assert "reason" not in metrics

    def test_without_grouping_labels_are_not_metrics(self):
        metrics = extract_metrics_from_promql("sum without(instance, pod) (http_requests_total)")
        assert "http_requests_total" in metrics
        assert "instance" not in metrics
        assert "pod" not in metrics

    def test_vector_matching_labels_are_not_metrics(self):
        metrics = extract_metrics_from_promql("http_requests_total / ignoring(instance) group_left(job) target_info")
        assert "http_requests_total" in metrics
        assert "target_info" in metrics
        assert "instance" not in metrics
        assert "job" not in metrics

    def test_falls_back_to_regex_for_templated_queries(self):
        metrics = extract_metrics_from_promql("sum(rate(http_requests_total[$__rate_interval])) by (status)")
        assert "http_requests_total" in metrics
        assert "status" not in metrics

    def test_range_selector_walks_matrix_vs(self):
        metrics = extract_metrics_from_promql("rate(http_requests_total[5m])")
        assert metrics == ["http_requests_total"]


class TestAggregationExtraction:
    def test_sum_rate(self):
        patterns = extract_aggregation_patterns("sum(rate(http_requests_total[5m])) by (status)")
        assert any(p["aggregation"] == "sum" and p.get("inner_function") == "rate" for p in patterns)

    def test_histogram_quantile(self):
        patterns = extract_aggregation_patterns("histogram_quantile(0.99, sum(rate(metric_bucket[5m])) by (le))")
        assert any(p["aggregation"] == "histogram_quantile" for p in patterns)

    def test_bare_rate(self):
        patterns = extract_aggregation_patterns("rate(container_cpu_usage_seconds_total[5m])")
        assert any(p["aggregation"] == "rate" for p in patterns)


# ── Dashboard JSON parsing ───────────────────────────────────────────────────


class TestDashboardParsing:
    def test_parse_simple_dashboard(self):
        dashboard_json = {
            "dashboard": {
                "uid": "test-dash-1",
                "title": "SSO Service Health",
                "tags": ["sso", "auth"],
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Auth Request Rate",
                        "targets": [{"expr": "sum(rate(sso_auth_requests_total[5m])) by (result)"}],
                    },
                    {
                        "type": "timeseries",
                        "title": "Auth Failures",
                        "targets": [{"expr": "sum(increase(sso_auth_failures_total[5m])) by (reason)"}],
                    },
                    {
                        "type": "stat",
                        "title": "Auth Latency p95",
                        "targets": [
                            {"expr": "histogram_quantile(0.95, sum(rate(sso_auth_latency_seconds_bucket[5m])) by (le))"}
                        ],
                        "fieldConfig": {"defaults": {"unit": "s"}},
                    },
                ],
                "links": [],
                "annotations": {"list": []},
            }
        }

        result = parse_dashboard_json(dashboard_json)

        assert result["dashboard_uid"] == "test-dash-1"
        assert result["dashboard_title"] == "SSO Service Health"
        assert result["panel_count"] == 3
        assert "sso_auth_requests_total" in result["metrics_found"]
        assert "sso_auth_failures_total" in result["metrics_found"]
        assert "sso_auth_latency_seconds_bucket" in result["metrics_found"]
        assert len(result["panel_titles"]) == 3
        assert len(result["metric_cooccurrence"]) > 0

    def test_mixed_datasource_panel_preserves_metric_level_identity(self, signal_store):
        dashboard_json = {
            "dashboard": {
                "uid": "mixed-datasource",
                "title": "Mixed datasource",
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Mixed",
                        "targets": [
                            {
                                "expr": "rate(checkout_requests_total[5m])",
                                "datasource": {"type": "prometheus", "uid": "prom"},
                            },
                            {
                                "metricName": "checkout_request_latency_seconds",
                                "datasource": {"type": "cloudwatch", "uid": "cw"},
                            },
                        ],
                    }
                ],
            }
        }

        parsed = parse_dashboard_json(dashboard_json)
        panel = parsed["panels"][0]
        assert panel["datasource_type"] == ""
        assert panel["metric_sources"] == [
            {
                "metric": "checkout_requests_total",
                "datasource_type": "prometheus",
                "query_language": "promql",
            },
            {
                "metric": "checkout_request_latency_seconds",
                "datasource_type": "cloudwatch",
                "query_language": "cloudwatch",
            },
        ]

        inferred = infer_signals_from_metrics(parsed["metrics_found"], parsed["panels"], store=signal_store)
        scopes = {
            row["metric"]: (row["datasource_types"], row["query_languages"])
            for row in inferred
            if row["source"] == "heuristic"
        }
        assert scopes["checkout_requests_total"] == (["prometheus"], ["promql"])
        assert scopes["checkout_request_latency_seconds"] == (["cloudwatch"], ["cloudwatch"])

    def test_parse_uploaded_grafana_dashboard_features(self):
        dashboard_json = {
            "dashboard": {
                "uid": "upload-dash",
                "title": "Uploaded Grafana",
                "tags": ["upload"],
                "panels": [
                    {
                        "type": "timeseries",
                        "title": "Request Rate",
                        "targets": [
                            {
                                "expr": "sum(rate(http_requests_total[5m]))",
                                "datasource": {"type": "prometheus", "uid": "prom"},
                            }
                        ],
                    }
                ],
            }
        }

        features = parse_uploaded_dashboard(dashboard_json, vendor="grafana", source_name="upload.json")

        assert features.dashboard_uid == "upload-dash"
        assert features.backend_name == "grafana_json"
        assert features.query_language == "promql"
        assert features.metrics_found == ["http_requests_total"]
        assert features.panel_count == 1

    def test_parse_uploaded_signalfx_dashboard_features(self):
        document = {
            "dashboard": {"id": "sfx-dash", "name": "Uploaded SignalFx", "tags": ["upload"]},
            "charts": [
                {
                    "name": "CPU",
                    "options": {"programOptions": {"programText": "data('cpu.utilization').mean().publish()"}},
                }
            ],
        }

        features = parse_uploaded_dashboard(document, vendor="signalfx", source_name="sfx.json")

        assert features.dashboard_uid == "sfx-dash"
        assert features.backend_name == "signalfx_json"
        assert features.query_language == "signalflow"
        assert features.metrics_found == ["cpu.utilization"]
        assert features.panel_count == 1

    def test_parse_uploaded_signalfx_dashboard_with_nested_charts(self):
        document = {
            "dashboard": {
                "id": "sfx-nested",
                "name": "Nested SignalFx",
                "tags": ["upload"],
                "charts": [
                    {
                        "name": "Memory",
                        "options": {
                            "programOptions": {"programText": "data('container.memory.usage').mean().publish()"}
                        },
                    }
                ],
            }
        }

        features = parse_uploaded_dashboard(document, vendor="signalfx", source_name="sfx-nested.json")

        assert features.dashboard_uid == "sfx-nested"
        assert features.metrics_found == ["container.memory.usage"]
        assert features.panel_count == 1
        assert features.panel_titles == ["Memory"]

    def test_parse_uploaded_signalfx_program_options_null_uses_program_text(self):
        document = {
            "dashboard": {"id": "sfx-null-options", "name": "Nullable SignalFx"},
            "charts": [
                {
                    "name": "CPU",
                    "options": {"programOptions": None},
                    "programText": "data('cpu.utilization').mean().publish()",
                }
            ],
        }

        features = parse_uploaded_dashboard(document, vendor="signalfx", source_name="sfx-null.json")

        assert features.dashboard_uid == "sfx-null-options"
        assert features.metrics_found == ["cpu.utilization"]
        assert features.panel_count == 1

    def test_parse_dashboard_with_rows(self):
        dashboard_json = {
            "dashboard": {
                "uid": "row-dash",
                "title": "Row Test",
                "tags": [],
                "panels": [
                    {
                        "type": "row",
                        "title": "Traffic",
                        "panels": [
                            {
                                "type": "timeseries",
                                "title": "Request Rate",
                                "targets": [{"expr": "rate(requests_total[5m])"}],
                            },
                        ],
                    },
                    {
                        "type": "row",
                        "title": "Errors",
                        "panels": [
                            {
                                "type": "timeseries",
                                "title": "Error Rate",
                                "targets": [{"expr": "rate(errors_total[5m])"}],
                            },
                        ],
                    },
                ],
                "links": [],
                "annotations": {"list": []},
            }
        }

        result = parse_dashboard_json(dashboard_json)
        assert result["panel_count"] == 2
        assert len(result["row_groups"]) == 2
        row_names = {rg["row"] for rg in result["row_groups"]}
        assert "Traffic" in row_names
        assert "Errors" in row_names

    def test_parse_skips_text_panels(self):
        dashboard_json = {
            "dashboard": {
                "uid": "skip-test",
                "title": "Skip Test",
                "tags": [],
                "panels": [
                    {"type": "text", "title": "Instructions", "targets": []},
                    {"type": "timeseries", "title": "Real Panel", "targets": [{"expr": "up"}]},
                ],
                "links": [],
                "annotations": {"list": []},
            }
        }

        result = parse_dashboard_json(dashboard_json)
        assert result["panel_count"] == 1
        assert result["panel_titles"] == ["Real Panel"]


# ── Signal inference from metrics ────────────────────────────────────────────


class TestSignalInference:
    def test_infer_signals_from_sso_metrics(self, signal_store_with_bootstrap):
        metrics = [
            "sso_auth_requests_total",
            "sso_auth_failures_total",
            "sso_auth_latency_seconds_bucket",
        ]
        signals = infer_signals_from_metrics(metrics)

        # Should infer auth-related signals
        assert len(signals) > 0
        # At least one auth signal should be inferred
        auth_signals = [s for s in signals if "auth" in s["signal_type"]]
        assert len(auth_signals) > 0

    def test_infer_signals_from_standard_metrics(self, signal_store_with_bootstrap):
        metrics = [
            "http_requests_total",
            "http_request_duration_seconds_bucket",
            "container_cpu_usage_seconds_total",
        ]
        signals = infer_signals_from_metrics(metrics)

        signal_types = {s["signal_type"] for s in signals}
        assert "request_rate" in signal_types or "request_latency" in signal_types


# ── Archetype YAML generation ───────────────────────────────────────────────


class TestArchetypeGeneration:
    def test_generate_archetype_yaml(self, signal_store_with_bootstrap):
        extracted = {
            "dashboard_uid": "sso-health",
            "dashboard_title": "SSO Service Health",
            "dashboard_tags": ["sso", "auth"],
            "query_language": "promql",
            "metrics_found": ["sso_auth_requests_total", "sso_auth_failures_total"],
            "panel_count": 2,
            "panels": [
                {
                    "title": "Auth Rate",
                    "queries": ["sum(rate(sso_auth_requests_total[5m]))"],
                    "row": "Auth",
                    "unit": "reqps",
                },
                {
                    "title": "Auth Failures",
                    "queries": ["sum(increase(sso_auth_failures_total[5m]))"],
                    "row": "Auth",
                    "unit": "short",
                },
            ],
        }
        signals = infer_signals_from_metrics(extracted["metrics_found"])
        yaml_str = generate_archetype_yaml(extracted, signals, archetype_id="sso_health")

        assert "sso_health" in yaml_str
        assert "SSO Service Health" in yaml_str
        assert "sso_auth_requests_total" in yaml_str
        assert "auto-generated" in yaml_str
        assert "learned" in yaml_str
        import yaml

        parsed = yaml.safe_load(yaml_str)
        query = parsed["archetypes"][0]["panels"][0]["queries"][0]
        assert query["query_language"] == "promql"


# ── Ingested dashboard records ───────────────────────────────────────────────


class TestIngestedDashboards:
    def test_record_and_retrieve(self, signal_store):
        signal_store.record_ingested_dashboard(
            "dash-1",
            backend_name="grafana",
            dashboard_title="Test Dashboard",
            metrics_found=["metric_a", "metric_b"],
            panel_count=3,
            signals_inferred=["request_latency"],
        )

        result = signal_store.get_ingested_dashboard("dash-1")
        assert result is not None
        assert result["backend_name"] == "grafana"
        assert result["dashboard_title"] == "Test Dashboard"
        assert result["metrics_found"] == ["metric_a", "metric_b"]
        assert result["panel_count"] == 3
        assert result["status"] == "pending"

    def test_learning_context_index_respects_review_state(self, signal_store):
        if not signal_store._learning_index_available():
            pytest.skip("SQLite FTS5 is not available")

        signal_store.record_ingested_dashboard(
            "checkout-dash",
            backend_name="grafana_json",
            dashboard_title="Checkout Service Health",
            dashboard_tags=["service:checkout"],
            metrics_found=["checkout_custom_latency_ms"],
            panel_count=1,
            signals_inferred=[
                {
                    "signal_type": "request_latency",
                    "metric": "checkout_custom_latency_ms",
                    "source": "heuristic",
                    "confidence": 0.88,
                    "auto_teach_eligible": True,
                    "reason": "Panel title and metric name indicate checkout latency",
                }
            ],
            status="pending",
        )
        indexed = signal_store.index_dashboard_context(
            dashboard_uid="checkout-dash",
            backend_name="grafana_json",
            dashboard_title="Checkout Service Health",
            dashboard_tags=["service:checkout"],
            panels=[
                {
                    "title": "Checkout p95 latency",
                    "queries": ['histogram_quantile(0.95, checkout_custom_latency_ms{service="checkout"})'],
                    "metrics": ["checkout_custom_latency_ms"],
                }
            ],
            metrics_found=["checkout_custom_latency_ms"],
            signals_inferred=[
                {
                    "signal_type": "request_latency",
                    "metric": "checkout_custom_latency_ms",
                    "source": "heuristic",
                    "confidence": 0.88,
                    "auto_teach_eligible": True,
                    "reason": "Panel title and metric name indicate checkout latency",
                }
            ],
            status="pending",
        )

        assert indexed == 1
        pending = signal_store.search_learning_context("checkout latency", service="checkout")
        assert pending[0]["metric_name"] == "checkout_custom_latency_ms"
        assert pending[0]["review_state"] == "candidate"
        assert (
            signal_store.search_learning_context(
                "checkout latency",
                service="checkout",
                include_candidates=False,
            )
            == []
        )

        approve_ingested_dashboard_record(
            dashboard_uid="checkout-dash",
            backend_name="grafana_json",
            store=signal_store,
        )
        approved = signal_store.search_learning_context(
            "checkout latency",
            service="checkout",
            include_candidates=False,
        )
        assert approved == []

    def test_describe_service_summarizes_learned_context(self, signal_store):
        if not signal_store._learning_index_available():
            pytest.skip("SQLite FTS5 is not available")

        signal_store.index_dashboard_context(
            dashboard_uid="checkout-dash",
            backend_name="grafana_json",
            dashboard_title="Checkout Service Health",
            dashboard_tags=["service:checkout"],
            panels=[
                {
                    "title": "Checkout errors",
                    "queries": ['sum(rate(checkout_5xx_count{service="checkout"}[5m]))'],
                    "metrics": ["checkout_5xx_count"],
                }
            ],
            metrics_found=["checkout_5xx_count"],
            signals_inferred=[
                {
                    "signal_type": "error_rate",
                    "metric": "checkout_5xx_count",
                    "source": "heuristic",
                    "confidence": 0.91,
                    "auto_teach_eligible": True,
                    "reason": "5xx metric indicates errors",
                }
            ],
            status="approved",
        )

        summary = signal_store.describe_service("checkout", include_candidates=False)

        assert summary["service"] == "checkout"
        assert summary["trusted_context_rows"] == 1
        assert summary["candidate_context_rows"] == 0
        assert summary["dashboards"][0]["dashboard_title"] == "Checkout Service Health"
        assert summary["top_metrics"][0]["metric"] == "checkout_5xx_count"
        assert summary["signals"] == {"error_rate": 1}

    def test_rejected_and_ignored_context_are_excluded_from_default_search(self, signal_store):
        if not signal_store._learning_index_available():
            pytest.skip("SQLite FTS5 is not available")

        for uid, status in (("rejected-dash", "rejected"), ("ignored-dash", "ignored")):
            signal_store.index_dashboard_context(
                dashboard_uid=uid,
                backend_name="grafana_json",
                dashboard_title="Checkout Service Health",
                dashboard_tags=["service:checkout"],
                panels=[
                    {
                        "title": "Checkout latency",
                        "queries": ['checkout_custom_latency_ms{service="checkout"}'],
                        "metrics": ["checkout_custom_latency_ms"],
                    }
                ],
                metrics_found=["checkout_custom_latency_ms"],
                signals_inferred=[
                    {
                        "signal_type": "request_latency",
                        "metric": "checkout_custom_latency_ms",
                        "source": "heuristic",
                        "confidence": 0.88,
                    }
                ],
                status="pending",
            )
            signal_store.update_learning_context_review_state(uid, status, backend_name="grafana_json")

        assert signal_store.search_learning_context("checkout latency", service="checkout") == []

    def test_describe_service_does_not_fallback_to_other_services(self, signal_store):
        if not signal_store._learning_index_available():
            pytest.skip("SQLite FTS5 is not available")

        signal_store.index_dashboard_context(
            dashboard_uid="payments-dash",
            backend_name="grafana_json",
            dashboard_title="Payments Service Health",
            dashboard_tags=["service:payments"],
            panels=[
                {
                    "title": "Checkout-adjacent payment latency",
                    "queries": ['payment_latency_ms{service="payments"}'],
                    "metrics": ["payment_latency_ms"],
                }
            ],
            metrics_found=["payment_latency_ms"],
            signals_inferred=[
                {
                    "signal_type": "request_latency",
                    "metric": "payment_latency_ms",
                    "source": "heuristic",
                    "confidence": 0.88,
                }
            ],
            status="approved",
        )

        summary = signal_store.describe_service("checkout", include_candidates=False)

        assert summary["matched_context_rows"] == 0
        assert summary["top_metrics"] == []

    @pytest.mark.asyncio
    async def test_ingest_dashboard_does_not_generate_archetype_by_default(self, signal_store, monkeypatch):
        from tacit import dashboard_ingest as di

        class FakeBackend:
            async def ingest_dashboard(self, uid):
                return DashboardFeatures(
                    dashboard_uid=uid,
                    dashboard_title="CPU Dashboard",
                    dashboard_tags=[],
                    backend_name="signalfx",
                    query_language="signalflow",
                    metrics_found=["cpu.utilization"],
                    panel_count=1,
                    panel_titles=["CPU"],
                    panels=[
                        {
                            "title": "CPU",
                            "queries": ["data('cpu.utilization').publish()"],
                            "row": "",
                            "unit": "",
                            "description": "",
                        }
                    ],
                )

            async def close(self):
                return None

        monkeypatch.setattr(di, "get_signal_store", lambda: signal_store)

        result = await di.ingest_dashboard("cpu-dash", backend=FakeBackend(), auto_approve=False)

        stored = signal_store.get_ingested_dashboard("cpu-dash")
        assert stored is not None
        assert stored["backend_name"] == "signalfx"
        assert stored["archetype_generated"] == result["archetype_yaml"]
        assert stored["archetype_generated"] == ""
        assert result["archetype_generation_enabled"] is False

    @pytest.mark.asyncio
    async def test_auto_approve_quarantines_generated_archetype(self, signal_store, monkeypatch, tmp_path):
        from tacit import dashboard_ingest as di

        runtime_settings = signal_store.runtime_settings.model_copy(
            deep=True,
            update={
                "learned_archetypes_generation_enabled": True,
                "learned_archetypes_automatic_registration_enabled": True,
                "learned_archetypes_quarantine_path": str(tmp_path / "quarantine"),
            },
        )
        scoped_store = SignalStore(signal_store.database_path, runtime_settings=runtime_settings)

        features = DashboardFeatures(
            dashboard_uid="checkout-autoreg",
            dashboard_title="Checkout Autoreg",
            dashboard_tags=["service:checkout", "environment:production"],
            backend_name="grafana_json",
            query_language="promql",
            metrics_found=["checkout_custom_latency_ms"],
            panel_count=1,
            panel_titles=["Checkout Latency"],
            panels=[
                {
                    "title": "Checkout Latency",
                    "queries": ["checkout_custom_latency_ms"],
                    "metrics": ["checkout_custom_latency_ms"],
                }
            ],
        )

        result = await di.ingest_dashboard_features(
            features,
            auto_approve=True,
            runtime_settings=runtime_settings,
            store=scoped_store,
        )

        assert result["archetype_registered"] is False
        assert result["archetype_quarantined"] is True
        quarantine_files = list((tmp_path / "quarantine").rglob("*.yaml"))
        assert len(quarantine_files) == 1
        assert "checkout_autoreg" in quarantine_files[0].read_text()

    @pytest.mark.asyncio
    async def test_generated_archetype_uses_resolved_wildcard_tenant(self, tmp_path):
        from tacit import dashboard_ingest as di

        runtime_settings = Settings(
            _env_file=None,
            knowledge_tenant_id="*",
            api_auth_enabled=True,
            learned_archetypes_tenant_id="default",
            learned_archetypes_generation_enabled=True,
            learned_archetypes_automatic_registration_enabled=True,
            learned_archetypes_quarantine_path=str(tmp_path / "quarantine"),
        )
        signal_store = SignalStore(
            db_path=tmp_path / "tenant-signals.db",
            runtime_settings=runtime_settings,
        )
        features = DashboardFeatures(
            dashboard_uid="tenant-a-checkout",
            dashboard_title="Tenant A Checkout",
            dashboard_tags=["service:checkout", "environment:production"],
            backend_name="grafana_json",
            query_language="promql",
            metrics_found=["checkout_custom_latency_ms"],
            panel_count=1,
            panel_titles=["Checkout Latency"],
            panels=[
                {
                    "title": "Checkout Latency",
                    "queries": ["checkout_custom_latency_ms"],
                    "metrics": ["checkout_custom_latency_ms"],
                }
            ],
        )

        result = await di.ingest_dashboard_features(
            features,
            auto_approve=False,
            runtime_settings=runtime_settings,
            store=signal_store,
            tenant_id="tenant-a",
        )

        document = yaml.safe_load(result["archetype_yaml"])
        assert document["archetypes"][0]["tenant_id"] == "tenant-a"
        quarantine_files = list((tmp_path / "quarantine").rglob("*.yaml"))
        assert len(quarantine_files) == 1
        assert "tenant-a" in quarantine_files[0].relative_to(tmp_path / "quarantine").parts[0]

    @pytest.mark.asyncio
    async def test_pending_ingest_quarantines_without_activating_mappings(self, signal_store, monkeypatch, tmp_path):
        from tacit import dashboard_ingest as di

        runtime_settings = signal_store.runtime_settings.model_copy(
            deep=True,
            update={
                "learned_archetypes_generation_enabled": True,
                "learned_archetypes_automatic_registration_enabled": True,
                "learned_archetypes_quarantine_path": str(tmp_path / "quarantine"),
            },
        )
        scoped_store = SignalStore(signal_store.database_path, runtime_settings=runtime_settings)
        features = DashboardFeatures(
            dashboard_uid="checkout-pending",
            dashboard_title="Checkout Pending",
            dashboard_tags=["service:checkout", "environment:production"],
            backend_name="grafana_json",
            query_language="promql",
            metrics_found=["checkout_custom_latency_ms"],
            panel_count=1,
            panel_titles=["Checkout Latency"],
            panels=[
                {
                    "title": "Checkout Latency",
                    "queries": ["checkout_custom_latency_ms"],
                    "metrics": ["checkout_custom_latency_ms"],
                }
            ],
        )

        result = await di.ingest_dashboard_features(
            features,
            auto_approve=False,
            runtime_settings=runtime_settings,
            store=scoped_store,
        )

        assert result["status"] == "pending"
        assert result["archetype_quarantined"] is True
        assert len(list((tmp_path / "quarantine").rglob("*.yaml"))) == 1
        active_metrics = {
            mapping["metric_pattern"] for mapping in scoped_store.get_mappings_for_signal("request_latency")
        }
        assert "checkout_custom_latency_ms" not in active_metrics

    @pytest.mark.asyncio
    async def test_auto_approve_keeps_held_candidates_out_of_approved_context(self, signal_store, monkeypatch):
        from tacit import dashboard_ingest as di

        monkeypatch.setattr(di, "get_signal_store", lambda: signal_store)
        features = DashboardFeatures(
            dashboard_uid="held-autoapprove",
            dashboard_title="Held Autoapprove",
            backend_name="grafana_json",
            query_language="promql",
            metrics_found=["opaque_value"],
            panel_count=1,
            panel_titles=["Opaque"],
            panels=[
                {
                    "title": "Opaque",
                    "queries": ['opaque_value{service="checkout"}'],
                    "metrics": ["opaque_value"],
                }
            ],
        )

        monkeypatch.setattr(
            di,
            "infer_signals_from_metrics",
            lambda *_args, **_kwargs: [
                {
                    "signal_type": "supporting_evidence",
                    "metric": "opaque_value",
                    "source": "heuristic",
                    "signal_family": "unknown",
                    "confidence": 0.2,
                    "auto_teach_eligible": False,
                    "why_not_auto_taught": "low_score",
                }
            ],
        )

        result = await di.ingest_dashboard_features(features, auto_approve=True)

        assert result["mappings_created"] == 0
        assert signal_store.search_learning_context("opaque", service="checkout", include_candidates=False) == []
        candidate = signal_store.search_learning_context("opaque", service="checkout")
        assert candidate[0]["review_state"] == "candidate"

    def test_manual_approval_quarantines_generated_archetype(self, signal_store, tmp_path):
        runtime_settings = signal_store.runtime_settings.model_copy(
            deep=True,
            update={
                "learned_archetypes_automatic_registration_enabled": True,
                "learned_archetypes_quarantine_path": str(tmp_path / "quarantine"),
            },
        )
        scoped_store = SignalStore(signal_store.database_path, runtime_settings=runtime_settings)

        archetype_yaml = generate_archetype_yaml(
            {
                "dashboard_title": "Checkout Manual",
                "dashboard_tags": ["service:checkout", "environment:production"],
                "metrics_found": ["checkout_5xx_count"],
                "panels": [],
            },
            [],
            tenant_id="default",
            generation_run_id="dashboard_ingest:grafana_json:checkout-manual",
            source_refs=["grafana_json:checkout-manual"],
        )

        scoped_store.record_ingested_dashboard(
            "checkout-manual",
            backend_name="grafana_json",
            metrics_found=["checkout_5xx_count"],
            signals_inferred=[
                {
                    "signal_type": "error_rate",
                    "metric": "checkout_5xx_count",
                    "source": "heuristic",
                    "confidence": 0.9,
                    "auto_teach_eligible": True,
                }
            ],
            archetype_generated=archetype_yaml,
            status="pending",
        )

        result = approve_ingested_dashboard_record(
            dashboard_uid="checkout-manual",
            backend_name="grafana_json",
            store=scoped_store,
            runtime_settings=runtime_settings,
        )

        assert result["status"] == "approved"
        assert result["archetype_registered"] is False
        assert result["archetype_quarantined"] is True
        quarantine_files = list((tmp_path / "quarantine").rglob("*.yaml"))
        assert len(quarantine_files) == 1
        assert "checkout_manual" in quarantine_files[0].read_text()

    @pytest.mark.asyncio
    async def test_unchanged_approved_dashboard_reingest_preserves_review_state(self, signal_store, monkeypatch):
        from tacit.dashboard_ingest.service import ingest_dashboard_features

        inferred = [
            {
                "signal_type": "request_latency",
                "metric": "checkout_latency_seconds",
                "source": "heuristic",
                "signal_family": "latency",
                "confidence": 0.9,
                "auto_teach_eligible": True,
            }
        ]
        monkeypatch.setattr(
            "tacit.dashboard_ingest.service.infer_signals_from_metrics",
            lambda *_args, **_kwargs: inferred,
        )
        features = DashboardFeatures(
            dashboard_uid="unchanged-approved",
            dashboard_title="Unchanged approved",
            backend_name="grafana",
            query_language="promql",
            metrics_found=["checkout_latency_seconds"],
            panel_count=1,
            panel_titles=["Checkout latency"],
            panels=[
                {
                    "title": "Checkout latency",
                    "metrics": ["checkout_latency_seconds"],
                    "queries": ["checkout_latency_seconds"],
                }
            ],
        )

        approved = await ingest_dashboard_features(features, auto_approve=True, store=signal_store)
        before = signal_store.get_ingested_dashboard("unchanged-approved", backend_name="grafana")
        refreshed = await ingest_dashboard_features(features, auto_approve=False, store=signal_store)
        after = signal_store.get_ingested_dashboard("unchanged-approved", backend_name="grafana")

        assert approved["status"] == refreshed["status"] == "approved"
        assert before is not None and after is not None
        assert after["status"] == "approved"
        assert after["created_at"] == before["created_at"]

    def test_changed_dashboard_inference_creates_a_pending_generation(self, signal_store):
        source = {
            "dashboard_title": "Checkout latency",
            "metrics_found": ["checkout_latency_seconds"],
            "query_transformations": ["checkout_latency_seconds"],
        }
        signal_store.record_ingested_dashboard(
            "changed-inference",
            backend_name="grafana",
            signals_inferred=[{"signal_type": "request_latency", "metric": "checkout_latency_seconds"}],
            status="approved",
            **source,
        )
        before = signal_store.get_ingested_dashboard("changed-inference", backend_name="grafana")

        result = signal_store.record_ingested_dashboard(
            "changed-inference",
            backend_name="grafana",
            signals_inferred=[{"signal_type": "database_latency", "metric": "checkout_latency_seconds"}],
            status="pending",
            **source,
        )
        after = signal_store.get_ingested_dashboard("changed-inference", backend_name="grafana")

        assert result == "updated"
        assert before is not None and after is not None
        assert after["status"] == "pending"
        assert after["created_at"] > before["created_at"]
        assert after["last_seen_at"] >= before["last_seen_at"]

    def test_manual_approval_keeps_held_candidates_out_of_approved_context(self, signal_store):
        if not signal_store._learning_index_available():
            pytest.skip("SQLite FTS5 is not available")

        signals = [
            {
                "signal_type": "request_latency",
                "metric": "checkout_custom_latency_ms",
                "source": "heuristic",
                "signal_family": "latency",
                "confidence": 0.9,
                "auto_teach_eligible": True,
            },
            {
                "signal_type": "supporting_evidence",
                "metric": "opaque_value",
                "source": "heuristic",
                "signal_family": "unknown",
                "confidence": 0.2,
                "auto_teach_eligible": False,
                "why_not_auto_taught": "low_score",
            },
        ]
        signal_store.record_ingested_dashboard(
            "held-manual",
            backend_name="grafana_json",
            metrics_found=["checkout_custom_latency_ms", "opaque_value"],
            signals_inferred=signals,
            status="pending",
        )
        signal_store.index_dashboard_context(
            dashboard_uid="held-manual",
            backend_name="grafana_json",
            dashboard_title="Held Manual",
            dashboard_tags=["service:checkout"],
            panels=[
                {
                    "title": "Checkout latency",
                    "queries": ['checkout_custom_latency_ms{service="checkout"}'],
                    "metrics": ["checkout_custom_latency_ms"],
                },
                {
                    "title": "Opaque",
                    "queries": ['opaque_value{service="checkout"}'],
                    "metrics": ["opaque_value"],
                },
            ],
            metrics_found=["checkout_custom_latency_ms", "opaque_value"],
            signals_inferred=signals,
            status="pending",
        )

        result = approve_ingested_dashboard_record(
            dashboard_uid="held-manual",
            backend_name="grafana_json",
            store=signal_store,
        )

        assert result["mappings_created"] == 0
        approved = signal_store.search_learning_context(
            "checkout latency",
            service="checkout",
            include_candidates=False,
        )
        assert approved == []
        assert signal_store.search_learning_context("opaque", service="checkout", include_candidates=False) == []
        candidate = signal_store.search_learning_context("opaque", service="checkout")
        assert candidate[0]["review_state"] == "candidate"

    def test_store_direct_approval_syncs_fts_for_eligible_rows(self, signal_store):
        if not signal_store._learning_index_available():
            pytest.skip("SQLite FTS5 is not available")

        signals = [
            {
                "signal_type": "request_latency",
                "metric": "checkout_custom_latency_ms",
                "source": "heuristic",
                "signal_family": "latency",
                "confidence": 0.9,
                "auto_teach_eligible": True,
            },
            {
                "signal_type": "supporting_evidence",
                "metric": "opaque_value",
                "source": "heuristic",
                "signal_family": "unknown",
                "confidence": 0.2,
                "auto_teach_eligible": False,
            },
        ]
        signal_store.record_ingested_dashboard(
            "direct-approve",
            backend_name="grafana_json",
            metrics_found=["checkout_custom_latency_ms", "opaque_value"],
            signals_inferred=signals,
            status="pending",
        )
        signal_store.index_dashboard_context(
            dashboard_uid="direct-approve",
            backend_name="grafana_json",
            dashboard_title="Direct Approve",
            dashboard_tags=["service:checkout"],
            panels=[
                {
                    "title": "Checkout latency",
                    "queries": ['checkout_custom_latency_ms{service="checkout"}'],
                    "metrics": ["checkout_custom_latency_ms"],
                },
                {
                    "title": "Opaque",
                    "queries": ['opaque_value{service="checkout"}'],
                    "metrics": ["opaque_value"],
                },
            ],
            metrics_found=["checkout_custom_latency_ms", "opaque_value"],
            signals_inferred=signals,
            status="pending",
        )

        assert signal_store.approve_ingested_dashboard("direct-approve", backend_name="grafana_json")

        approved = signal_store.search_learning_context(
            "checkout latency",
            service="checkout",
            include_candidates=False,
        )
        assert approved[0]["metric_name"] == "checkout_custom_latency_ms"
        assert signal_store.search_learning_context("opaque", service="checkout", include_candidates=False) == []
        assert signal_store.search_learning_context("opaque", service="checkout")[0]["review_state"] == "candidate"

    def test_reject_record_persists_negative_training_data(self, signal_store):
        signal_store.record_ingested_dashboard(
            "checkout-reject",
            backend_name="grafana_json",
            metrics_found=["checkout_5xx_count"],
            signals_inferred=[
                {
                    "signal_type": "error_rate",
                    "metric": "checkout_5xx_count",
                    "source": "heuristic",
                    "signal_family": "errors",
                    "score": 0.91,
                    "margin": 0.5,
                    "evidence": ["name contains 5xx"],
                    "inference_version": "test",
                }
            ],
            status="pending",
        )

        result = reject_ingested_dashboard_record(
            dashboard_uid="checkout-reject",
            backend_name="grafana_json",
            store=signal_store,
        )

        assert result["status"] == "rejected"
        assert result["rejected_candidates"] == 1
        rejected = signal_store.list_rejected_candidates()
        assert rejected[0]["metric"] == "checkout_5xx_count"
        assert rejected[0]["why_not"] == "dashboard_rejected"
        assert rejected[0]["dashboard_uid"] == "checkout-reject"

    def test_reject_record_enforces_permission_before_mutating(self, signal_store):
        runtime_settings = Settings(_env_file=None, knowledge_permissions="knowledge.read")
        scoped_store = SignalStore(signal_store._db_path, runtime_settings=runtime_settings)
        scoped_store.record_ingested_dashboard(
            "checkout-reject-denied",
            backend_name="grafana_json",
            metrics_found=["checkout_5xx_count"],
            signals_inferred=[
                {
                    "signal_type": "error_rate",
                    "metric": "checkout_5xx_count",
                    "source": "heuristic",
                    "signal_family": "errors",
                }
            ],
            status="pending",
        )

        with pytest.raises(PermissionError, match="knowledge.reject"):
            reject_ingested_dashboard_record(
                dashboard_uid="checkout-reject-denied",
                backend_name="grafana_json",
                store=scoped_store,
                runtime_settings=runtime_settings,
            )

        persisted = scoped_store.get_ingested_dashboard(
            "checkout-reject-denied",
            backend_name="grafana_json",
        )
        assert persisted is not None
        assert persisted["status"] == "pending"
        assert signal_store.list_rejected_candidates() == []

    def test_reject_record_rolls_back_status_and_negatives_together(self, signal_store, monkeypatch):
        signal_store.record_ingested_dashboard(
            "checkout-reject-rollback",
            backend_name="grafana_json",
            metrics_found=["checkout_5xx_count"],
            signals_inferred=[
                {
                    "signal_type": "error_rate",
                    "metric": "checkout_5xx_count",
                    "source": "heuristic",
                    "signal_family": "errors",
                }
            ],
            status="pending",
        )
        persist_negative = signal_store.record_rejected_candidate

        def fail_after_insert(*args, **kwargs):
            persist_negative(*args, **kwargs)
            raise RuntimeError("simulated negative-training write failure")

        monkeypatch.setattr(signal_store, "record_rejected_candidate", fail_after_insert)

        with pytest.raises(RuntimeError, match="simulated negative-training write failure"):
            reject_ingested_dashboard_record(
                dashboard_uid="checkout-reject-rollback",
                backend_name="grafana_json",
                store=signal_store,
            )

        persisted = signal_store.get_ingested_dashboard(
            "checkout-reject-rollback",
            backend_name="grafana_json",
        )
        assert persisted is not None
        assert persisted["status"] == "pending"
        assert signal_store.list_rejected_candidates() == []

    @pytest.mark.parametrize(
        "decision",
        [reject_ingested_dashboard_record, ignore_ingested_dashboard_record],
    )
    def test_terminal_dashboard_review_rolls_back_when_authority_reconciliation_fails(
        self,
        signal_store,
        monkeypatch,
        decision,
    ):
        signal_store.record_ingested_dashboard(
            "authority-rollback",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
            signals_inferred=[],
            status="approved",
        )

        def fail_reconciliation(**_kwargs):
            raise RuntimeError("simulated authority reconciliation failure")

        monkeypatch.setattr(
            "tacit.dashboard_ingest.service._reconcile_dashboard_authority_for_state",
            fail_reconciliation,
        )

        with pytest.raises(RuntimeError, match="simulated authority reconciliation failure"):
            decision(
                dashboard_uid="authority-rollback",
                backend_name="grafana",
                store=signal_store,
            )

        persisted = signal_store.get_ingested_dashboard("authority-rollback", backend_name="grafana")
        assert persisted is not None
        assert persisted["status"] == "approved"

    def test_approval_loss_repair_rolls_back_resolver_cleanup_when_lifecycle_fails(
        self,
        signal_store,
        monkeypatch,
    ):
        source_ref = "grafana:approval-loss-rollback"
        signal_store.record_ingested_dashboard(
            "approval-loss-rollback",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
            signals_inferred=[],
            status="rejected",
        )
        mapping_id = signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            0.9,
            source_type="dashboard_ingest",
            source_refs=[source_ref],
            review_state="trusted",
        )
        signal_store.ensure_governed_projection_audit_current()
        with signal_store._conn() as conn:
            before_mapping = dict(
                conn.execute(
                    "SELECT review_state, source_refs FROM signal_metric_mappings WHERE id=?",
                    (mapping_id,),
                ).fetchone()
            )
        service = KnowledgeService(
            KnowledgeRepository(signal_store._db_path),
            signal_store=signal_store,
        )

        def fail_lifecycle_after_resolver_cleanup(**_kwargs):
            raise RuntimeError("simulated approval-loss lifecycle failure")

        monkeypatch.setattr(service, "reconcile_source_lifecycle", fail_lifecycle_after_resolver_cleanup)

        with pytest.raises(RuntimeError, match="simulated approval-loss lifecycle failure"):
            approve_ingested_dashboard_record(
                dashboard_uid="approval-loss-rollback",
                backend_name="grafana",
                store=signal_store,
                knowledge_service=service,
            )

        with signal_store._conn() as conn:
            mapping = conn.execute(
                "SELECT review_state, source_refs FROM signal_metric_mappings WHERE id=?",
                (mapping_id,),
            ).fetchone()
        assert mapping is not None
        assert dict(mapping) == before_mapping
        assert json.loads(mapping["source_refs"]) == [source_ref]

    def test_dashboard_rejection_rolls_back_mapping_and_revision_retirement_together(
        self,
        signal_store,
        monkeypatch,
    ):
        source_ref = "grafana:governed-rollback"
        signal_store.record_ingested_dashboard(
            "governed-rollback",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
            signals_inferred=[],
            status="approved",
        )
        service = KnowledgeService(
            KnowledgeRepository(signal_store._db_path),
            signal_store=signal_store,
        )
        candidate_id = migrate_signal_mapping(
            {
                "id": "governed-rollback-a",
                "signal_type": "request_latency",
                "metric_pattern": "checkout_latency_seconds",
                "source_type": "dashboard_ingest",
                "source_refs": [source_ref],
                "source_fingerprint": "independent-a",
                "review_state": "approved",
            },
            service=service,
        )
        migrate_signal_mapping(
            {
                "id": "governed-rollback-b",
                "signal_type": "request_latency",
                "metric_pattern": "checkout_latency_seconds",
                "source_type": "alert_ingest",
                "source_refs": ["grafana:alert:governed-rollback"],
                "source_fingerprint": "independent-b",
                "review_state": "approved",
            },
            service=service,
        )
        candidate = service.repository.get_candidate(candidate_id, "default")
        assert candidate is not None
        item = service.repository.find_knowledge_by_proposition(
            "default",
            candidate.proposition.proposition_key,
        )
        assert item is not None
        before_revision = service.repository.get_revision(item.id, tenant_id="default")
        assert before_revision is not None
        legacy_mapping_id = signal_store.add_mapping(
            "request_latency",
            "legacy_checkout_latency_seconds",
            0.9,
            source_type="dashboard_ingest",
            source_refs=[source_ref],
            review_state="candidate",
        )

        reconcile_lifecycle = service.reconcile_source_lifecycle

        def fail_after_lifecycle_reconciliation(**kwargs):
            reconcile_lifecycle(**kwargs)
            raise RuntimeError("simulated lifecycle checkpoint failure")

        monkeypatch.setattr(service, "reconcile_source_lifecycle", fail_after_lifecycle_reconciliation)

        with pytest.raises(RuntimeError, match="simulated lifecycle checkpoint failure"):
            reject_ingested_dashboard_record(
                dashboard_uid="governed-rollback",
                backend_name="grafana",
                store=signal_store,
                knowledge_service=service,
            )

        persisted = signal_store.get_ingested_dashboard("governed-rollback", backend_name="grafana")
        assert persisted is not None
        assert persisted["status"] == "approved"
        with signal_store._conn() as conn:
            legacy_mapping = conn.execute(
                "SELECT metric_pattern FROM signal_metric_mappings WHERE id=?",
                (legacy_mapping_id,),
            ).fetchone()
        assert legacy_mapping is not None
        assert legacy_mapping["metric_pattern"] == "legacy_checkout_latency_seconds"
        after_revision = service.repository.get_revision(item.id, tenant_id="default")
        assert after_revision == before_revision
        assert service.repository.get_candidate(candidate_id, "default") == candidate

    @pytest.mark.parametrize(
        ("decision", "terminal_status"),
        [
            (reject_ingested_dashboard_record, "rejected"),
            (ignore_ingested_dashboard_record, "ignored"),
        ],
    )
    def test_terminal_dashboard_review_wins_pending_cas_before_approval_promotion(
        self,
        signal_store,
        monkeypatch,
        decision,
        terminal_status,
    ):
        signal_store.record_ingested_dashboard(
            "approval-race",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
            signals_inferred=[
                {
                    "signal_type": "request_latency",
                    "metric": "checkout_latency_seconds",
                    "source": "heuristic",
                    "signal_family": "latency",
                    "confidence": 0.9,
                    "auto_teach_eligible": True,
                }
            ],
        )
        claim_started = Event()
        release_claim = Event()

        class FakeKnowledgeService:
            repository = KnowledgeRepository(signal_store._db_path)
            runtime_settings = signal_store.runtime_settings
            database_path = signal_store.database_path
            runtime_ownership = runtime_descriptor_for_store(
                component="approval-race-knowledge-service",
                runtime_settings=runtime_settings,
                database_role="signals",
                database_path=database_path,
            )

            def reconcile_source_lifecycle(self, **_kwargs):
                return []

        knowledge_service = FakeKnowledgeService()

        original_claim = signal_store.claim_ingested_dashboard_approval

        def delayed_claim(*args, **kwargs):
            claim_started.set()
            assert release_claim.wait(timeout=5)
            return original_claim(*args, **kwargs)

        monkeypatch.setattr(signal_store, "claim_ingested_dashboard_approval", delayed_claim)
        monkeypatch.setattr(
            "tacit.dashboard_ingest.service.persist_inferred_signal_review",
            lambda **_kwargs: pytest.fail("promotion ran after the approval claim lost"),
        )

        with ThreadPoolExecutor(max_workers=1) as executor:
            approval = executor.submit(
                approve_ingested_dashboard_record,
                dashboard_uid="approval-race",
                backend_name="grafana",
                store=signal_store,
                knowledge_service=knowledge_service,
            )
            assert claim_started.wait(timeout=5)
            try:
                reviewed = decision(
                    dashboard_uid="approval-race",
                    backend_name="grafana",
                    store=signal_store,
                    knowledge_service=knowledge_service,
                )
            finally:
                release_claim.set()
            with pytest.raises(RuntimeError, match="approval claim was lost"):
                approval.result(timeout=5)

        assert reviewed["status"] == terminal_status
        assert signal_store.get_ingested_dashboard("approval-race", backend_name="grafana")["status"] == terminal_status
        assert signal_store.get_mappings_for_signal("request_latency", include_decayed=True) == []

    def test_dashboard_approval_retry_recovers_an_approving_generation(self, signal_store, monkeypatch):
        signal_store.record_ingested_dashboard(
            "approval-retry",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
            signals_inferred=[
                {
                    "signal_type": "request_latency",
                    "metric": "checkout_latency_seconds",
                    "source": "heuristic",
                    "confidence": 0.9,
                    "auto_teach_eligible": True,
                }
            ],
        )

        knowledge_service = KnowledgeService(
            KnowledgeRepository(signal_store._db_path),
            signal_store=signal_store,
        )

        def idempotent_promotion(**kwargs):
            kwargs["store"].add_mapping(
                "request_latency",
                "checkout_latency_seconds",
                0.9,
                source_type="dashboard_ingest",
                source_refs=[kwargs["source_ref"]],
                review_state="approved",
            )
            kwargs["governed_pairs"].add(("checkout_latency_seconds", "request_latency"))
            return True

        monkeypatch.setattr(
            "tacit.dashboard_ingest.service.persist_inferred_signal_review",
            idempotent_promotion,
        )
        original_finalize = signal_store.finalize_ingested_dashboard_approval
        finalize_attempts = 0

        def crash_once(*args, **kwargs):
            nonlocal finalize_attempts
            finalize_attempts += 1
            if finalize_attempts == 1:
                raise RuntimeError("simulated crash before finalization")
            return original_finalize(*args, **kwargs)

        monkeypatch.setattr(signal_store, "finalize_ingested_dashboard_approval", crash_once)

        with pytest.raises(RuntimeError, match="simulated crash"):
            approve_ingested_dashboard_record(
                dashboard_uid="approval-retry",
                backend_name="grafana",
                store=signal_store,
                knowledge_service=knowledge_service,
            )
        assert signal_store.get_ingested_dashboard("approval-retry", backend_name="grafana")["status"] == "approving"
        assert signal_store.get_mappings_for_signal("request_latency", include_decayed=True) == []

        recovered = approve_ingested_dashboard_record(
            dashboard_uid="approval-retry",
            backend_name="grafana",
            store=signal_store,
            knowledge_service=knowledge_service,
        )

        assert recovered["status"] == "approved"
        assert finalize_attempts == 2
        mappings = signal_store.get_mappings_for_signal("request_latency", include_decayed=True)
        assert [mapping["metric_pattern"] for mapping in mappings] == ["checkout_latency_seconds"]

    def test_dashboard_approval_rolls_back_governed_authority_before_final_status(
        self,
        signal_store,
        monkeypatch,
    ):
        signal_store.record_ingested_dashboard(
            "governed-approval-rollback",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
            signals_inferred=[
                {
                    "signal_type": "request_latency",
                    "metric": "checkout_latency_seconds",
                    "source": "heuristic",
                    "confidence": 0.9,
                    "auto_teach_eligible": True,
                }
            ],
        )
        knowledge_service = KnowledgeService(
            KnowledgeRepository(signal_store._db_path),
            signal_store=signal_store,
        )

        def promote_governed_mapping(**kwargs):
            candidate_id = migrate_signal_mapping(
                {
                    "id": "governed-approval-rollback",
                    "signal_type": "request_latency",
                    "metric_pattern": "checkout_latency_seconds",
                    "source_type": "dashboard_ingest",
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

        monkeypatch.setattr(
            "tacit.dashboard_ingest.service.persist_inferred_signal_review",
            promote_governed_mapping,
        )
        original_finalize = signal_store.finalize_ingested_dashboard_approval
        finalize_attempts = 0

        def crash_once(*args, **kwargs):
            nonlocal finalize_attempts
            finalize_attempts += 1
            if finalize_attempts == 1:
                raise RuntimeError("simulated finalization crash")
            return original_finalize(*args, **kwargs)

        monkeypatch.setattr(signal_store, "finalize_ingested_dashboard_approval", crash_once)

        with pytest.raises(RuntimeError, match="simulated finalization crash"):
            approve_ingested_dashboard_record(
                dashboard_uid="governed-approval-rollback",
                backend_name="grafana",
                store=signal_store,
                knowledge_service=knowledge_service,
            )

        assert (
            signal_store.get_ingested_dashboard(
                "governed-approval-rollback",
                backend_name="grafana",
            )["status"]
            == "approving"
        )
        assert knowledge_service.repository.list_candidates() == []
        assert knowledge_service.repository.list_current_revisions() == []
        assert signal_store.get_mappings_for_signal("request_latency", include_decayed=True) == []

        recovered = approve_ingested_dashboard_record(
            dashboard_uid="governed-approval-rollback",
            backend_name="grafana",
            store=signal_store,
            knowledge_service=knowledge_service,
        )

        assert recovered["status"] == "approved"
        revisions = knowledge_service.repository.list_current_revisions()
        assert len(revisions) == 1
        mappings = signal_store.get_mappings_for_signal("request_latency", include_decayed=True)
        assert len(mappings) == 1
        assert mappings[0]["governance_ref"] == revisions[0].knowledge_id

    def test_approval_claim_blocks_changed_dashboard_reingestion(self, signal_store, monkeypatch):
        signal_store.record_ingested_dashboard(
            "approval-reingest-race",
            backend_name="grafana",
            metrics_found=["old_latency_seconds"],
            signals_inferred=[
                {
                    "signal_type": "old_latency",
                    "metric": "old_latency_seconds",
                    "source": "heuristic",
                    "confidence": 0.9,
                    "auto_teach_eligible": True,
                }
            ],
        )

        knowledge_service = KnowledgeService(
            KnowledgeRepository(signal_store._db_path),
            signal_store=signal_store,
        )

        def reingest_then_promote_old_generation(**kwargs):
            kwargs["store"].record_ingested_dashboard(
                "approval-reingest-race",
                backend_name="grafana",
                metrics_found=["new_latency_seconds"],
                signals_inferred=[
                    {
                        "signal_type": "new_latency",
                        "metric": "new_latency_seconds",
                        "source": "heuristic",
                        "confidence": 0.9,
                        "auto_teach_eligible": True,
                    }
                ],
                status="pending",
            )
            kwargs["store"].add_mapping(
                "old_latency",
                "old_latency_seconds",
                0.9,
                source_type="dashboard_ingest",
                source_refs=[kwargs["source_ref"]],
                review_state="approved",
            )
            kwargs["governed_pairs"].add(("old_latency_seconds", "old_latency"))
            return True

        monkeypatch.setattr(
            "tacit.dashboard_ingest.service.persist_inferred_signal_review",
            reingest_then_promote_old_generation,
        )

        with pytest.raises(RuntimeError, match="approval is in progress"):
            approve_ingested_dashboard_record(
                dashboard_uid="approval-reingest-race",
                backend_name="grafana",
                store=signal_store,
                knowledge_service=knowledge_service,
            )

        current = signal_store.get_ingested_dashboard("approval-reingest-race", backend_name="grafana")
        assert current is not None
        assert current["status"] == "approving"
        assert current["metrics_found"] == ["old_latency_seconds"]
        assert signal_store.get_mappings_for_signal("old_latency", include_decayed=True) == []

    def test_complete_crawl_does_not_stale_claimed_sources(self, signal_store):
        signal_store.record_ingested_dashboard(
            "claimed-dashboard",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
        )
        dashboard = signal_store.get_ingested_dashboard("claimed-dashboard", backend_name="grafana")
        assert dashboard is not None
        assert signal_store.claim_ingested_dashboard_approval(
            "claimed-dashboard",
            backend_name="grafana",
            expected_generation=dashboard["created_at"],
        )
        signal_store.record_ingested_alert(
            "claimed-alert",
            backend_name="grafana",
            fingerprint="fingerprint:claimed-alert",
        )
        alert = signal_store.get_ingested_alert("claimed-alert", "grafana")
        assert alert is not None
        assert signal_store.claim_ingested_alert_approval(
            "claimed-alert",
            "grafana",
            generation_fingerprint=alert["generation_fingerprint"],
        )

        assert (
            signal_store.mark_missing_dashboards_stale(
                backend_name="grafana",
                seen_dashboard_uids=set(),
                authority_reconciler=lambda _conn, _source: None,
            )
            == 0
        )
        assert (
            signal_store.mark_missing_alerts_stale(
                backend_name="grafana",
                seen_alert_uids=set(),
                authority_reconciler=lambda _conn, _source: None,
            )
            == 0
        )
        assert (
            signal_store.get_ingested_dashboard(
                "claimed-dashboard",
                backend_name="grafana",
            )["status"]
            == "approving"
        )
        assert signal_store.get_ingested_alert("claimed-alert", "grafana")["status"] == "approving"

    def test_complete_crawl_recovers_expired_approval_claims(self, signal_store):
        signal_store.record_ingested_dashboard(
            "abandoned-dashboard",
            backend_name="grafana",
            metrics_found=["checkout_latency_seconds"],
        )
        dashboard = signal_store.get_ingested_dashboard("abandoned-dashboard", backend_name="grafana")
        assert dashboard is not None
        assert signal_store.claim_ingested_dashboard_approval(
            "abandoned-dashboard",
            backend_name="grafana",
            expected_generation=dashboard["created_at"],
        )
        signal_store.record_ingested_alert(
            "abandoned-alert",
            backend_name="grafana",
            fingerprint="fingerprint:abandoned-alert",
        )
        alert = signal_store.get_ingested_alert("abandoned-alert", "grafana")
        assert alert is not None
        assert signal_store.claim_ingested_alert_approval(
            "abandoned-alert",
            "grafana",
            generation_fingerprint=alert["generation_fingerprint"],
        )
        expired_at = time.time() - signal_store._settings.learning_approval_claim_ttl_seconds - 1
        with signal_store._conn() as conn:
            conn.execute(
                "UPDATE ingested_dashboards SET reviewed_at=? WHERE dashboard_uid='abandoned-dashboard'",
                (expired_at,),
            )
            conn.execute(
                "UPDATE ingested_alerts SET reviewed_at=? WHERE alert_uid='abandoned-alert'",
                (expired_at,),
            )

        crawl_started_at = time.time() + 1
        assert (
            signal_store.mark_missing_dashboards_stale(
                backend_name="grafana",
                seen_dashboard_uids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _source: None,
            )
            == 1
        )
        assert (
            signal_store.mark_missing_alerts_stale(
                backend_name="grafana",
                seen_alert_uids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _source: None,
            )
            == 1
        )
        assert (
            signal_store.get_ingested_dashboard(
                "abandoned-dashboard",
                backend_name="grafana",
            )["status"]
            == "stale"
        )
        assert signal_store.get_ingested_alert("abandoned-alert", "grafana")["status"] == "stale"

    def test_alert_reingestion_serializes_with_approval_claim(self, signal_store, monkeypatch):
        signal_store.record_ingested_alert(
            "alert-generation-race",
            backend_name="grafana",
            fingerprint="generation:1",
        )
        original = signal_store.get_ingested_alert("alert-generation-race", "grafana")
        assert original is not None
        claimant = SignalStore(db_path=signal_store._db_path)
        source_read = Event()
        release_source = Event()
        claim_started = Event()
        original_source_conn = signal_store._conn
        original_claim_conn = claimant._conn

        class ConnectionProxy:
            def __init__(self, connection, *, pause_source=False, observe_claim=False):
                self._connection = connection
                self._pause_source = pause_source
                self._observe_claim = observe_claim

            def execute(self, sql, *args, **kwargs):
                if self._observe_claim and "SET status='approving'" in sql:
                    claim_started.set()
                result = self._connection.execute(sql, *args, **kwargs)
                if self._pause_source and "SELECT id, fingerprint" in sql:
                    source_read.set()
                    assert release_source.wait(timeout=5)
                return result

            def __getattr__(self, name):
                return getattr(self._connection, name)

        @contextmanager
        def delayed_source_conn():
            with original_source_conn() as connection:
                yield ConnectionProxy(connection, pause_source=True)

        @contextmanager
        def observed_claim_conn():
            with original_claim_conn() as connection:
                yield ConnectionProxy(connection, observe_claim=True)

        monkeypatch.setattr(signal_store, "_conn", delayed_source_conn)
        monkeypatch.setattr(claimant, "_conn", observed_claim_conn)

        with ThreadPoolExecutor(max_workers=2) as executor:
            reingestion = executor.submit(
                signal_store.record_ingested_alert,
                "alert-generation-race",
                backend_name="grafana",
                fingerprint="generation:2",
            )
            assert source_read.wait(timeout=5)
            claim = executor.submit(
                claimant.claim_ingested_alert_approval,
                "alert-generation-race",
                "grafana",
                generation_fingerprint=original["generation_fingerprint"],
            )
            assert claim_started.wait(timeout=5)
            assert not claim.done()
            release_source.set()

            assert reingestion.result(timeout=5) == "updated"
            assert claim.result(timeout=5) is False

        current = signal_store.get_ingested_alert("alert-generation-race", "grafana")
        assert current is not None
        assert current["fingerprint"] == "generation:2"
        assert current["status"] == "pending"

    def test_changed_alert_inference_invalidates_the_approved_generation(self, signal_store):
        signal_store.record_ingested_alert(
            "changed-alert-inference",
            backend_name="grafana",
            fingerprint="source-v1",
            signals_inferred=[{"signal_type": "request_latency", "metric": "latency_seconds"}],
            status="approved",
        )
        before = signal_store.get_ingested_alert("changed-alert-inference", "grafana")
        assert before is not None

        result = signal_store.record_ingested_alert(
            "changed-alert-inference",
            backend_name="grafana",
            fingerprint="source-v1",
            signals_inferred=[{"signal_type": "database_latency", "metric": "latency_seconds"}],
            status="pending",
        )
        after = signal_store.get_ingested_alert("changed-alert-inference", "grafana")

        assert result == "updated"
        assert after is not None
        assert after["fingerprint"] == before["fingerprint"]
        assert after["generation_fingerprint"] != before["generation_fingerprint"]
        assert after["status"] == "pending"
        assert not signal_store.claim_ingested_alert_approval(
            "changed-alert-inference",
            "grafana",
            generation_fingerprint=before["generation_fingerprint"],
        )
        assert signal_store.claim_ingested_alert_approval(
            "changed-alert-inference",
            "grafana",
            generation_fingerprint=after["generation_fingerprint"],
        )

    @pytest.mark.asyncio
    async def test_auto_approve_honors_heuristic_auto_teach_gate(self, signal_store, monkeypatch):
        from tacit import dashboard_ingest as di

        features = DashboardFeatures(
            dashboard_uid="memory-context",
            dashboard_title="Memory Context",
            backend_name="grafana_json",
            query_language="promql",
            metrics_found=["opaque_value"],
            panel_count=3,
            panel_titles=["Memory", "Memory", "Memory"],
            panels=[
                {
                    "title": "Memory",
                    "queries": ["opaque_value"],
                    "metrics": ["opaque_value"],
                    "unit": "bytes",
                    "row": "Resources",
                }
                for _ in range(3)
            ],
        )
        monkeypatch.setattr(di, "get_signal_store", lambda: signal_store)

        result = await di.ingest_dashboard_features(features, auto_approve=True)

        assert result["mappings_created"] == 0
        rejected = signal_store.list_rejected_candidates()
        assert len(rejected) == 1
        assert rejected[0]["metric"] == "opaque_value"
        assert rejected[0]["why_not"] == "low_score"
        assert signal_store.get_mappings_for_signal(rejected[0]["signal_name"]) == []

    def test_signal_quality_report_explains_inference_decisions(self):
        signals = [
            {
                "signal_type": "request_latency",
                "metric": "http_request_duration_seconds",
                "confidence": 0.95,
                "source": "taxonomy",
                "reason": "matches taught pattern",
                "evidence": ["matches taught pattern"],
            },
            {
                "signal_type": "cache_saturation",
                "metric": "redis_evictions_total",
                "confidence": 0.74,
                "source": "heuristic",
                "reason": "metric contains eviction",
                "evidence": ["name suggests cache pressure"],
                "auto_teach_eligible": True,
            },
            {
                "signal_type": "supporting_evidence",
                "metric": "opaque_value",
                "confidence": 0.2,
                "source": "heuristic",
                "reason": "weak single source",
                "evidence": ["panel title only"],
                "auto_teach_eligible": False,
                "why_not_auto_taught": "low_score",
            },
        ]

        report = build_signal_quality_report(
            metrics=["http_request_duration_seconds", "redis_evictions_total", "opaque_value", "unmapped_metric"],
            signals=signals,
        )

        assert report["metrics_total"] == 4
        assert report["metrics_mapped"] == 3
        assert report["metrics_unmapped"] == ["unmapped_metric"]
        assert report["taxonomy_matches"] == 1
        assert report["auto_teach_eligible"] == 1
        assert report["held_for_review"] == 1
        assert report["explanations"][0]["review_state"] == "trusted"
        assert report["explanations"][1]["review_state"] == "eligible"
        assert report["explanations"][2]["why_not_auto_taught"] == "low_score"

    def test_signal_reports_tolerate_legacy_string_signals(self):
        quality = build_signal_quality_report(
            metrics=["legacy_metric_total"],
            signals=["request_rate", "error_rate"],
        )
        impact = build_learning_impact_report(
            metrics=["legacy_metric_total"],
            signals=["request_rate", "error_rate"],
        )

        assert quality["legacy_signals"] == 2
        assert quality["metrics_mapped"] == 0
        assert quality["explanations"][0]["signal_type"] == "request_rate"
        assert quality["explanations"][0]["source"] == "legacy"
        assert impact["unresolved_metrics"] == ["legacy_metric_total"]

    def test_learning_impact_report_shows_before_after_mapping_gain(self):
        signals = [
            {
                "signal_type": "request_latency",
                "metric": "http_request_duration_seconds",
                "confidence": 0.95,
                "source": "taxonomy",
                "reason": "matches taught pattern",
            },
            {
                "signal_type": "cache_saturation",
                "metric": "redis_evictions_total",
                "confidence": 0.74,
                "source": "heuristic",
                "reason": "metric contains eviction",
                "auto_teach_eligible": True,
            },
            {
                "signal_type": "supporting_evidence",
                "metric": "opaque_value",
                "confidence": 0.2,
                "source": "heuristic",
                "reason": "weak single source",
                "auto_teach_eligible": False,
            },
        ]

        report = build_learning_impact_report(
            metrics=["http_request_duration_seconds", "redis_evictions_total", "opaque_value"],
            signals=signals,
        )

        assert report["recognized_metrics_before_learning"] == 1
        assert report["recognized_metrics_after_approval"] == 2
        assert report["active_mappings_before_learning"] == 1
        assert report["candidate_mappings_pending_approval"] == 1
        assert report["new_active_mappings_after_approval"] == 0
        assert report["new_mappings_available"] == 1
        assert report["newly_understood_metrics"][0]["mapping_state"] == "candidate"
        assert report["newly_active_metrics_after_approval"] == []
        assert report["newly_understood_metrics"][0]["metric"] == "redis_evictions_total"
        assert report["unresolved_metrics"] == ["opaque_value"]

    def test_learning_impact_report_marks_approved_candidates_active(self):
        signals = [
            {
                "signal_type": "cache_saturation",
                "metric": "redis_evictions_total",
                "confidence": 0.74,
                "source": "heuristic",
                "reason": "metric contains eviction",
                "auto_teach_eligible": True,
            },
        ]

        report = build_learning_impact_report(
            metrics=["redis_evictions_total"],
            signals=signals,
            approved=True,
        )

        assert report["candidate_mappings_pending_approval"] == 0
        assert report["new_active_mappings_after_approval"] == 1
        assert report["newly_understood_metrics"][0]["mapping_state"] == "approved"
        assert report["newly_active_metrics_after_approval"][0]["metric"] == "redis_evictions_total"

    @pytest.mark.asyncio
    async def test_before_after_learning_fixture_resolves_custom_checkout_metrics(self, signal_store, monkeypatch):
        from tacit import dashboard_ingest as di

        monkeypatch.setattr(di, "get_signal_store", lambda: signal_store)
        catalog = [
            MetricEntry(
                name="checkout_custom_latency_ms",
                datasource_uid="prom",
                datasource_name="Prometheus",
                datasource_type="prometheus",
                query_language="promql",
            ),
            MetricEntry(
                name="checkout_5xx_count",
                datasource_uid="prom",
                datasource_name="Prometheus",
                datasource_type="prometheus",
                query_language="promql",
            ),
        ]
        signal_bindings = {
            "request_latency": "http_request_duration_seconds",
            "error_rate": "http_requests_total",
        }

        before = signal_store.resolve_signals_for_archetype(
            signal_bindings=signal_bindings,
            catalog=catalog,
            context_datasource_type="prometheus",
            target_query_language="promql",
        )
        assert before == {}

        features = DashboardFeatures(
            dashboard_uid="checkout-custom-ops",
            dashboard_title="Checkout Custom Ops",
            backend_name="grafana_json",
            query_language="promql",
            metrics_found=["checkout_custom_latency_ms", "checkout_5xx_count"],
            panel_count=2,
            panel_titles=["Checkout Latency", "Checkout 5xx Errors"],
            panels=[
                {
                    "title": "Checkout Latency",
                    "queries": ["checkout_custom_latency_ms"],
                    "metrics": ["checkout_custom_latency_ms"],
                    "unit": "ms",
                    "row": "Latency",
                },
                {
                    "title": "Checkout 5xx Errors",
                    "queries": ["checkout_5xx_count"],
                    "metrics": ["checkout_5xx_count"],
                    "unit": "short",
                    "row": "Errors",
                },
            ],
        )

        pending = await di.ingest_dashboard_features(features, auto_approve=False)

        assert pending["learning_impact"]["candidate_mappings_pending_approval"] == 2
        assert pending["learning_impact"]["new_active_mappings_after_approval"] == 0
        assert {m["metric"] for m in pending["learning_impact"]["newly_understood_metrics"]} == {
            "checkout_custom_latency_ms",
            "checkout_5xx_count",
        }
        assert (
            signal_store.resolve_signals_for_archetype(
                signal_bindings=signal_bindings,
                catalog=catalog,
                context_datasource_type="prometheus",
                target_query_language="promql",
            )
            == {}
        )

        approved = await di.ingest_dashboard_features(features, auto_approve=True)

        assert approved["learning_impact"]["candidate_mappings_pending_approval"] == 2
        assert approved["learning_impact"]["new_active_mappings_after_approval"] == 0
        after = signal_store.resolve_signals_for_archetype(
            signal_bindings=signal_bindings,
            catalog=catalog,
            context_datasource_type="prometheus",
            target_query_language="promql",
        )
        assert after == {}

    @pytest.mark.asyncio
    async def test_bulk_auto_approve_quarantines_archetypes(self, signal_store, monkeypatch, tmp_path):
        import tacit.backends as backends_mod
        from tacit import dashboard_ingest as di
        from tacit.dashboard_ingest import service as dashboard_service

        class FakeBackend:
            name = "grafana"

            async def list_dashboards(self, limit=500):
                return [
                    {"uid": "checkout-a", "title": "Checkout A"},
                    {"uid": "checkout-b", "title": "Checkout B"},
                ]

            async def ingest_dashboard(self, uid):
                return DashboardFeatures(
                    dashboard_uid=uid,
                    dashboard_title=uid.replace("-", " ").title(),
                    dashboard_tags=[f"service:{uid}", "environment:production"],
                    backend_name="grafana",
                    query_language="promql",
                    metrics_found=[f"{uid.replace('-', '_')}_latency_ms"],
                    panel_count=1,
                    panel_titles=["Latency"],
                    panels=[
                        {
                            "title": "Latency",
                            "queries": [f"{uid.replace('-', '_')}_latency_ms"],
                            "metrics": [f"{uid.replace('-', '_')}_latency_ms"],
                        }
                    ],
                )

            async def close(self):
                return None

        runtime_settings = signal_store.runtime_settings.model_copy(
            deep=True,
            update={
                "learned_archetypes_generation_enabled": True,
                "learned_archetypes_automatic_registration_enabled": True,
                "learned_archetypes_quarantine_path": str(tmp_path / "quarantine"),
            },
        )
        scoped_store = SignalStore(signal_store.database_path, runtime_settings=runtime_settings)
        monkeypatch.setattr(backends_mod, "get_active_backends", lambda: [FakeBackend()])
        service_creations = 0
        original_factory = dashboard_service._knowledge_service_for_store

        def counted_factory(*args, **kwargs):
            nonlocal service_creations
            service_creations += 1
            return original_factory(*args, **kwargs)

        monkeypatch.setattr(dashboard_service, "_knowledge_service_for_store", counted_factory)

        result = await di.learn_backend_dashboards(
            "grafana",
            auto_approve=True,
            runtime_settings=runtime_settings,
            store=scoped_store,
        )

        assert result["dashboards_learned"] == 2
        assert result["archetypes_registered"] == 0
        assert result["archetypes_quarantined"] == 2
        assert len(list((tmp_path / "quarantine").rglob("*.yaml"))) == 2
        assert all(item["archetype_registered"] is False for item in result["learned"])
        assert all(item["archetype_quarantined"] is True for item in result["learned"])
        assert service_creations == 1

    @pytest.mark.asyncio
    async def test_complete_dashboard_crawl_retires_missing_sources(self, signal_store, monkeypatch):
        import tacit.backends as backends_mod
        from tacit import dashboard_ingest as di

        signal_store.record_ingested_dashboard(
            "removed-dashboard",
            backend_name="grafana",
            dashboard_title="Removed dashboard",
            status="approved",
        )
        signal_store.add_mapping(
            "request_latency",
            "removed_latency_seconds",
            source_type="dashboard_ingest",
            source_refs=["grafana:removed-dashboard"],
        )

        class CompleteBackend:
            name = "grafana"
            last_dashboard_list_complete = True

            async def list_dashboards(self, limit=500):
                return []

            async def close(self):
                return None

        def unexpected_global_store():
            raise AssertionError("injected dashboard crawl consulted the process-global signal store")

        monkeypatch.setattr("tacit.dashboard_ingest.service.get_signal_store", unexpected_global_store)
        monkeypatch.setattr(backends_mod, "get_active_backends", lambda: [CompleteBackend()])

        result = await di.learn_backend_dashboards("grafana", store=signal_store)

        dashboard = signal_store.get_ingested_dashboard("removed-dashboard", backend_name="grafana")
        assert result["stale_marked"] == 1
        assert dashboard is not None and dashboard["stale"] is True
        assert dashboard["status"] == "stale"
        assert signal_store.get_mappings_for_signal("request_latency", include_decayed=True) == []

    @pytest.mark.asyncio
    async def test_complete_dashboard_crawl_paginates_all_stale_sources_for_its_backend(
        self,
        signal_store,
        monkeypatch,
    ):
        import tacit.backends as backends_mod
        from tacit import dashboard_ingest as di

        for index in range(3):
            signal_store.record_ingested_dashboard(
                f"removed-dashboard-{index}",
                backend_name="grafana",
                dashboard_title=f"Removed dashboard {index}",
                status="approved",
            )
        reconciled: list[str] = []

        def reconcile_source(_self, *, provenance_ref, tenant_id, source_stale, source_generation_guard):
            assert tenant_id == "default"
            assert source_stale is True
            reconciled.append(provenance_ref)

        monkeypatch.setattr("tacit.signals.store._STALE_SOURCE_PAGE_SIZE", 1)
        monkeypatch.setattr(
            "tacit.knowledge.service.KnowledgeService.reconcile_source_lifecycle",
            reconcile_source,
        )

        class CompleteBackend:
            name = "grafana"
            last_dashboard_list_complete = True

            async def list_dashboards(self, limit=500):
                return []

            async def close(self):
                return None

        monkeypatch.setattr(backends_mod, "get_active_backends", lambda: [CompleteBackend()])

        result = await di.learn_backend_dashboards("grafana", store=signal_store)

        assert result["stale_marked"] == 3
        assert reconciled == [f"grafana:removed-dashboard-{index}" for index in range(3)]

    @pytest.mark.asyncio
    async def test_failed_stale_dashboard_reconciliation_retries_without_a_new_stale_mark(
        self,
        signal_store,
        monkeypatch,
    ):
        import tacit.backends as backends_mod
        from tacit import dashboard_ingest as di

        signal_store.record_ingested_dashboard(
            "removed-dashboard",
            backend_name="grafana",
            dashboard_title="Removed dashboard",
            status="approved",
        )
        attempts: list[str] = []

        def reconcile_source(_self, *, provenance_ref, tenant_id, source_stale, source_generation_guard):
            assert tenant_id == "default"
            assert source_stale is True
            attempts.append(provenance_ref)
            if len(attempts) == 1:
                raise OSError("transient knowledge database failure")

        monkeypatch.setattr(
            "tacit.knowledge.service.KnowledgeService.reconcile_source_lifecycle",
            reconcile_source,
        )

        class CompleteBackend:
            name = "grafana"
            last_dashboard_list_complete = True

            async def list_dashboards(self, limit=500):
                return []

            async def close(self):
                return None

        monkeypatch.setattr(backends_mod, "get_active_backends", lambda: [CompleteBackend()])

        with pytest.raises(OSError, match="transient knowledge database failure"):
            await di.learn_backend_dashboards("grafana", store=signal_store)
        current = signal_store.get_ingested_dashboard("removed-dashboard", "grafana")
        assert current is not None and current["stale"] is False
        assert current["status"] == "approved"

        second = await di.learn_backend_dashboards("grafana", store=signal_store)
        assert second["stale_marked"] == 1
        assert second["stale_reconciliation_failures"] == 0
        assert (
            signal_store.list_unreconciled_stale_dashboards(
                tenant_id="default",
                backend_name="grafana",
            )
            == []
        )

        third = await di.learn_backend_dashboards("grafana", store=signal_store)
        assert third["stale_marked"] == 0
        assert attempts == ["grafana:removed-dashboard", "grafana:removed-dashboard"]

    def test_artifact_stale_checkpoint_is_bound_to_the_missing_generation(
        self,
        signal_store,
        monkeypatch,
    ):
        import tacit.signals.store as signal_store_module

        clock = {"now": 100.0}
        monkeypatch.setattr(signal_store_module.time, "time", lambda: clock["now"])
        signal_store.record_learned_artifact(
            artifact_id="runbook",
            artifact_type="runbook",
            fingerprint="runbook-v1",
        )

        clock["now"] = 200.0
        with signal_store.transaction() as connection:
            connection.execute(
                """UPDATE learned_artifacts
                   SET stale=1, missing_since=?, knowledge_reconciled_at=NULL
                   WHERE tenant_id='default' AND artifact_id='runbook'""",
                (clock["now"],),
            )
        old_artifact = signal_store.list_unreconciled_stale_artifacts(artifact_type="runbook")[0]

        clock["now"] = 300.0
        signal_store.record_learned_artifact(
            artifact_id="runbook",
            artifact_type="runbook",
            fingerprint="runbook-v1",
        )
        clock["now"] = 400.0
        with signal_store.transaction() as connection:
            connection.execute(
                """UPDATE learned_artifacts
                   SET stale=1, missing_since=?, knowledge_reconciled_at=NULL
                   WHERE tenant_id='default' AND artifact_id='runbook'""",
                (clock["now"],),
            )
        current_artifact = signal_store.list_unreconciled_stale_artifacts(artifact_type="runbook")[0]

        assert old_artifact["missing_since"] == 200.0
        assert current_artifact["missing_since"] == 400.0
        assert not signal_store.mark_artifact_knowledge_reconciled(
            artifact_id="runbook",
            missing_since=float(old_artifact["missing_since"]),
        )
        assert signal_store.mark_artifact_knowledge_reconciled(
            artifact_id="runbook",
            missing_since=float(current_artifact["missing_since"]),
        )

    def test_complete_crawl_does_not_stale_sources_seen_after_it_started(
        self,
        signal_store,
        monkeypatch,
    ):
        import tacit.signals.store as signal_store_module

        clock = {"now": 100.0}
        monkeypatch.setattr(signal_store_module.time, "time", lambda: clock["now"])
        signal_store.record_ingested_dashboard("dash", backend_name="grafana")
        signal_store.record_ingested_alert("alert", backend_name="grafana", fingerprint="alert-v1")
        signal_store.record_learned_artifact(
            artifact_id="runbook",
            artifact_type="runbook",
            fingerprint="runbook-v1",
        )
        crawl_started_at = 150.0

        clock["now"] = 200.0
        signal_store.record_ingested_dashboard("dash", backend_name="grafana")
        signal_store.record_ingested_alert("alert", backend_name="grafana", fingerprint="alert-v1")
        signal_store.record_learned_artifact(
            artifact_id="runbook",
            artifact_type="runbook",
            fingerprint="runbook-v1",
        )

        assert (
            signal_store.mark_missing_dashboards_stale(
                backend_name="grafana",
                seen_dashboard_uids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _source: None,
            )
            == 0
        )
        assert (
            signal_store.mark_missing_alerts_stale(
                backend_name="grafana",
                seen_alert_uids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _source: None,
            )
            == 0
        )
        assert (
            signal_store.mark_missing_artifacts_stale(
                artifact_type="runbook",
                seen_artifact_ids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _artifact: None,
            )
            == 0
        )
        assert not signal_store.get_ingested_dashboard("dash", backend_name="grafana")["stale"]
        assert not signal_store.get_ingested_alert("alert", "grafana")["stale"]
        assert not signal_store.get_learned_artifact("runbook")["stale"]

    def test_complete_crawl_stale_scan_pages_every_source(
        self,
        signal_store,
        monkeypatch,
    ):
        monkeypatch.setattr("tacit.signals.store._STALE_SOURCE_PAGE_SIZE", 1)
        for index in range(3):
            signal_store.record_ingested_dashboard(f"dash-{index}", backend_name="grafana")
            signal_store.record_ingested_alert(
                f"alert-{index}",
                backend_name="grafana",
                fingerprint=f"alert-{index}",
            )
            signal_store.record_learned_artifact(
                artifact_id=f"runbook-{index}",
                artifact_type="runbook",
                fingerprint=f"runbook-{index}",
            )
        crawl_started_at = time.time() + 1

        assert (
            signal_store.mark_missing_dashboards_stale(
                backend_name="grafana",
                seen_dashboard_uids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _source: None,
            )
            == 3
        )
        assert (
            signal_store.mark_missing_alerts_stale(
                backend_name="grafana",
                seen_alert_uids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _source: None,
            )
            == 3
        )
        assert (
            signal_store.mark_missing_artifacts_stale(
                artifact_type="runbook",
                seen_artifact_ids=set(),
                crawl_started_at=crawl_started_at,
                authority_reconciler=lambda _conn, _artifact: None,
            )
            == 3
        )

    def test_dashboard_uid_is_scoped_by_backend(self, signal_store):
        signal_store.record_ingested_dashboard(
            "shared-dash",
            backend_name="grafana",
            dashboard_title="Grafana Dashboard",
            status="pending",
        )
        signal_store.record_ingested_dashboard(
            "shared-dash",
            backend_name="signalfx",
            dashboard_title="SignalFx Dashboard",
            status="pending",
        )

        grafana = signal_store.get_ingested_dashboard("shared-dash", backend_name="grafana")
        signalfx = signal_store.get_ingested_dashboard("shared-dash", backend_name="signalfx")
        ambiguous = signal_store.get_ingested_dashboard("shared-dash")

        assert grafana is not None
        assert signalfx is not None
        assert grafana["dashboard_title"] == "Grafana Dashboard"
        assert signalfx["dashboard_title"] == "SignalFx Dashboard"
        assert ambiguous is None

        assert signal_store.approve_ingested_dashboard("shared-dash", backend_name="grafana")
        assert signal_store.get_ingested_dashboard("shared-dash", backend_name="grafana")["status"] == "approved"
        assert signal_store.get_ingested_dashboard("shared-dash", backend_name="signalfx")["status"] == "pending"

    @pytest.mark.parametrize(
        ("configured_tenant", "expected_tenant"),
        [("default", "default"), ("tenant-a", "tenant-a")],
    )
    def test_existing_uid_unique_table_migrates_to_backend_scope(
        self,
        tmp_path,
        configured_tenant: str,
        expected_tenant: str,
    ):
        db_path = tmp_path / f"legacy_signals_{configured_tenant}.db"
        with sqlite3.connect(db_path) as conn:
            conn.executescript("""
                CREATE TABLE ingested_dashboards (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    dashboard_uid       TEXT NOT NULL UNIQUE,
                    dashboard_title     TEXT NOT NULL DEFAULT '',
                    dashboard_tags      TEXT NOT NULL DEFAULT '[]',
                    metrics_found       TEXT NOT NULL DEFAULT '[]',
                    panel_count         INTEGER NOT NULL DEFAULT 0,
                    row_groups          TEXT NOT NULL DEFAULT '[]',
                    metric_cooccurrence TEXT NOT NULL DEFAULT '{}',
                    aggregation_patterns TEXT NOT NULL DEFAULT '[]',
                    query_transformations TEXT NOT NULL DEFAULT '[]',
                    panel_titles        TEXT NOT NULL DEFAULT '[]',
                    alert_links         TEXT NOT NULL DEFAULT '[]',
                    drilldown_links     TEXT NOT NULL DEFAULT '[]',
                    status              TEXT NOT NULL DEFAULT 'pending',
                    signals_inferred    TEXT NOT NULL DEFAULT '[]',
                    archetype_generated TEXT NOT NULL DEFAULT '',
                    created_at          REAL NOT NULL,
                    reviewed_at         REAL
                );
                INSERT INTO ingested_dashboards (dashboard_uid, dashboard_title, created_at)
                VALUES ('shared-dash', 'Legacy Grafana', 1.0);
            """)

        store = SignalStore(
            db_path=db_path,
            runtime_settings=Settings(knowledge_tenant_id=configured_tenant),
        )
        store.record_ingested_dashboard(
            "shared-dash",
            tenant_id=expected_tenant,
            backend_name="signalfx",
            dashboard_title="SignalFx Dashboard",
        )

        legacy = store.get_ingested_dashboard("shared-dash", backend_name="", tenant_id=expected_tenant)
        signalfx = store.get_ingested_dashboard(
            "shared-dash",
            backend_name="signalfx",
            tenant_id=expected_tenant,
        )

        assert legacy is not None
        assert legacy["tenant_id"] == expected_tenant
        assert signalfx is not None
        assert legacy["dashboard_title"] == "Legacy Grafana"
        assert signalfx["dashboard_title"] == "SignalFx Dashboard"

    def test_pending_dashboards_are_isolated_by_tenant(self, signal_store):
        signal_store = _wildcard_store(signal_store)
        for tenant_id, title in (("tenant-a", "Tenant A"), ("tenant-b", "Tenant B")):
            signal_store.record_ingested_dashboard(
                "shared-dashboard",
                tenant_id=tenant_id,
                backend_name="grafana",
                dashboard_title=title,
            )

        assert (
            signal_store.get_ingested_dashboard("shared-dashboard", "grafana", tenant_id="tenant-a")["dashboard_title"]
            == "Tenant A"
        )
        assert (
            signal_store.get_ingested_dashboard("shared-dashboard", "grafana", tenant_id="tenant-b")["dashboard_title"]
            == "Tenant B"
        )
        assert signal_store.approve_ingested_dashboard("shared-dashboard", "grafana", tenant_id="tenant-b")
        assert (
            signal_store.get_ingested_dashboard("shared-dashboard", "grafana", tenant_id="tenant-a")["status"]
            == "pending"
        )
        assert (
            signal_store.get_ingested_dashboard("shared-dashboard", "grafana", tenant_id="tenant-b")["status"]
            == "approved"
        )

    def test_list_by_status(self, signal_store):
        signal_store.record_ingested_dashboard("d1", status="pending")
        signal_store.record_ingested_dashboard("d2", status="approved")
        signal_store.record_ingested_dashboard("d3", status="pending")

        pending = signal_store.list_ingested_dashboards(status="pending")
        assert len(pending) == 2

        approved = signal_store.list_ingested_dashboards(status="approved")
        assert len(approved) == 1

    def test_approve_dashboard(self, signal_store):
        signal_store.record_ingested_dashboard("d1", status="pending")
        assert signal_store.approve_ingested_dashboard("d1")

        result = signal_store.get_ingested_dashboard("d1")
        assert result["status"] == "approved"
        assert result["reviewed_at"] is not None

    def test_approve_nonexistent(self, signal_store):
        assert not signal_store.approve_ingested_dashboard("nonexistent")


# ── YAML loading ─────────────────────────────────────────────────────────────


class TestYAMLLoading:
    def test_load_bootstrap_signals(self, tmp_path):
        yaml_content = """
signals:
  test_latency:
    description: Test latency
    category: latency
    unit: s
    metric_patterns:
      - pattern: "test_duration_seconds"
        confidence: 0.9
      - pattern: "*_latency_*"
        confidence: 0.6
  test_errors:
    description: Test errors
    category: errors
    unit: short
    metric_patterns:
      - pattern: "*_errors_total"
        confidence: 0.85
"""
        yaml_path = tmp_path / "signals.yaml"
        yaml_path.write_text(yaml_content)

        db_path = tmp_path / "test.db"
        store = SignalStore(db_path=db_path)
        count = store.load_from_yaml(yaml_path)

        assert count == 3  # 2 + 1 patterns
        types = store.list_signal_types()
        assert len(types) == 2

        mappings = store.get_mappings_for_signal("test_latency")
        assert len(mappings) == 2

    def test_load_project_signals_yaml(self):
        """Verify the actual project signals.yaml loads without errors."""
        db_path = Path(tempfile.mktemp(suffix=".db"))
        try:
            store = SignalStore(db_path=db_path)
            count = store.load_from_yaml()
            assert count > 20  # should have many bootstrap mappings
            types = store.list_signal_types()
            assert len(types) > 10
            stats = store.stats()
            assert stats["signal_types"] > 10
            assert stats["metric_mappings"] > 20
        finally:
            db_path.unlink(missing_ok=True)

    def test_unchanged_bootstrap_catalog_skips_repeated_upserts(self, tmp_path, monkeypatch):
        yaml_path = tmp_path / "signals.yaml"
        yaml_path.write_text("""signals:
  request_latency:
    metric_patterns:
      - pattern: request_seconds
        confidence: 0.9
""")
        store = SignalStore(db_path=tmp_path / "signals.db")

        assert store.load_from_yaml(yaml_path, only_if_changed=True) == 1

        def unexpected_write(*_args, **_kwargs):
            raise AssertionError("unchanged bootstrap catalogs must not be rewritten")

        monkeypatch.setattr(store, "register_signal_type", unexpected_write)
        monkeypatch.setattr(store, "add_mapping", unexpected_write)
        assert store.load_from_yaml(yaml_path, only_if_changed=True) == 0


# ── End-to-end: SSO custom metrics scenario ──────────────────────────────────


class TestEndToEndSSO:
    """The original motivating use case: SSO service with custom metrics."""

    def test_sso_signal_resolution_e2e(self, signal_store_with_bootstrap, sample_catalog):
        """Full flow: archetype with generic metrics → signal resolution →
        substituted to SSO-specific metrics."""
        store = signal_store_with_bootstrap

        # The archetype expects these generic metrics
        signal_bindings = {
            "auth_request_rate": "auth_requests_total",
            "auth_failure_count": "failed_login_attempts_total",
            "auth_latency": "auth_latency_seconds",
        }

        # But the catalog has SSO-specific ones
        subs = store.resolve_signals_for_archetype(
            signal_bindings=signal_bindings,
            catalog=sample_catalog,
        )

        # Verify all three were resolved
        assert "auth_requests_total" in subs
        assert "failed_login_attempts_total" in subs
        # auth_latency resolves to the bucket metric
        assert "auth_latency_seconds" in subs

        # Verify they resolved to the correct SSO metrics
        assert "sso" in subs["auth_requests_total"]
        assert "sso" in subs["failed_login_attempts_total"]


# ── SignalFlow metric extraction ───────────────────────────────────────────


class TestSignalFlowExtraction:
    """Test SignalFlow metric name and pattern extraction."""

    def test_simple_data_call(self):
        from tacit.backends.signalfx import _extract_metrics_from_signalflow

        metrics = _extract_metrics_from_signalflow("data('cpu.utilization').publish()")
        assert metrics == ["cpu.utilization"]

    def test_multiple_data_calls(self):
        from tacit.backends.signalfx import _extract_metrics_from_signalflow

        program = """
        A = data('requests.count', filter=filter('service', 'api')).publish()
        B = data('errors.count', filter=filter('service', 'api')).publish()
        """
        metrics = _extract_metrics_from_signalflow(program)
        assert "requests.count" in metrics
        assert "errors.count" in metrics
        assert len(metrics) == 2

    def test_data_with_double_quotes(self):
        from tacit.backends.signalfx import _extract_metrics_from_signalflow

        metrics = _extract_metrics_from_signalflow('data("memory.usage").publish()')
        assert metrics == ["memory.usage"]

    def test_analytics_patterns(self):
        from tacit.backends.signalfx import _extract_signalflow_patterns

        program = "data('cpu.utilization').mean().percentile(pct=95).publish()"
        patterns = _extract_signalflow_patterns(program)
        agg_names = [p["aggregation"] for p in patterns]
        assert "mean" in agg_names
        assert "percentile" in agg_names

    def test_rate_and_sum(self):
        from tacit.backends.signalfx import _extract_signalflow_patterns

        program = "data('requests.count').sum().rate().publish()"
        patterns = _extract_signalflow_patterns(program)
        agg_names = [p["aggregation"] for p in patterns]
        assert "sum" in agg_names
        assert "rate" in agg_names

    def test_no_metrics(self):
        from tacit.backends.signalfx import _extract_metrics_from_signalflow

        assert _extract_metrics_from_signalflow("") == []
        assert _extract_metrics_from_signalflow("publish()") == []


# ── DashboardFeatures dataclass ──────────────────────────────────────────


class TestDashboardFeatures:
    """Verify the vendor-agnostic DashboardFeatures dataclass."""

    def test_defaults(self):
        f = DashboardFeatures()
        assert f.dashboard_uid == ""
        assert f.metrics_found == []
        assert f.panel_count == 0
        assert f.backend_name == ""

    def test_grafana_features(self):
        f = DashboardFeatures(
            dashboard_uid="graf-123",
            dashboard_title="API Health",
            backend_name="grafana",
            query_language="promql",
            metrics_found=["http_requests_total", "http_request_duration_seconds"],
            panel_count=3,
        )
        assert f.backend_name == "grafana"
        assert f.query_language == "promql"
        assert len(f.metrics_found) == 2

    def test_signalfx_features(self):
        f = DashboardFeatures(
            dashboard_uid="sfx-456",
            dashboard_title="API Health",
            backend_name="signalfx",
            query_language="signalflow",
            metrics_found=["requests.count", "latency.p99"],
            panel_count=2,
        )
        assert f.backend_name == "signalfx"
        assert f.query_language == "signalflow"

    def test_features_to_dict(self):
        from tacit.dashboard_ingest import _features_to_dict

        f = DashboardFeatures(
            dashboard_uid="test",
            dashboard_title="Test",
            metrics_found=["m1", "m2"],
            panel_count=2,
            backend_name="grafana",
        )
        d = _features_to_dict(f)
        assert isinstance(d, dict)
        assert d["dashboard_uid"] == "test"
        assert d["metrics_found"] == ["m1", "m2"]
        assert d["backend_name"] == "grafana"

    def test_signal_inference_works_with_features(self, signal_store_with_bootstrap):
        """Signal inference is vendor-agnostic — works the same for both backends."""
        # Simulate SignalFx metrics (dot-separated naming)
        sfx_metrics = ["cpu.utilization", "memory.usage"]
        grafana_metrics = ["container_cpu_usage_seconds_total", "process_resident_memory_bytes"]

        infer_signals_from_metrics(sfx_metrics)
        grafana_signals = infer_signals_from_metrics(grafana_metrics)

        # Grafana standard metrics should match known signals
        grafana_types = {s["signal_type"] for s in grafana_signals}
        assert "cpu_usage" in grafana_types or "memory_usage" in grafana_types


# ── Signal coverage dashboard ingestion tests ───────────────────────────


class TestSignalCoverageDashboard:
    """Tests for the provisioned Grafana dashboard that exercises every signal category.

    The dashboard JSON fixture lives at dev/grafana/provisioning/dashboards/signal_coverage.json.
    """

    @pytest.fixture
    def dashboard_json(self):
        """Load the signal coverage dashboard JSON fixture."""
        import json

        fixture_path = Path(__file__).parent.parent.parent / "dev/grafana/provisioning/dashboards/signal_coverage.json"
        with open(fixture_path) as f:
            return json.load(f)

    @pytest.fixture
    def extracted(self, dashboard_json):
        """Parse the dashboard fixture and return extracted features."""
        return parse_dashboard_json(dashboard_json)

    @pytest.fixture
    def inferred_signals(self, extracted, signal_store_with_bootstrap):
        """Infer signals from the extracted metrics."""
        return infer_signals_from_metrics(
            extracted["metrics_found"],
            extracted.get("panels"),
        )

    # ── Metric extraction ──────────────────────────────────────────────

    def test_extracts_metrics(self, extracted):
        """Dashboard should yield a rich metric catalog."""
        metrics = extracted["metrics_found"]
        assert len(metrics) >= 15, f"Expected >=15 metrics, got {len(metrics)}"

    def test_contains_latency_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "http_request_duration_seconds" in metrics or "http_request_duration_seconds_bucket" in metrics

    def test_contains_throughput_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "http_requests_total" in metrics

    def test_contains_saturation_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        saturation = {
            "container_cpu_usage_seconds_total",
            "container_memory_working_set_bytes",
            "http_requests_in_flight",
            "db_connections_active",
        }
        assert saturation & metrics, f"No saturation metrics found in {metrics}"

    def test_contains_stability_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "kube_pod_container_restarts_total" in metrics

    def test_contains_error_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        assert "http_requests_total" in metrics  # used with status=~"5.."

    def test_contains_db_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        db = {"db_query_duration_seconds", "db_connections_active"}
        assert db & metrics, f"No DB metrics found in {metrics}"

    def test_contains_cache_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        cache = {"cache_hit_total", "cache_miss_total"}
        assert cache & metrics, f"No cache metrics found in {metrics}"

    def test_contains_network_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        net = {
            "network_bytes_received_total",
            "network_bytes_transmitted_total",
            "dns_failures_total",
            "tls_handshake_failures_total",
        }
        assert net & metrics, f"No network metrics found in {metrics}"

    def test_contains_queue_metrics(self, extracted):
        metrics = set(extracted["metrics_found"])
        q = {"kafka_consumer_lag", "message_queue_depth"}
        assert q & metrics, f"No queue metrics found in {metrics}"

    # ── Panel & row extraction ─────────────────────────────────────────

    def test_panel_count(self, extracted):
        """Should have panels from all signal categories."""
        assert extracted["panel_count"] >= 12

    def test_panel_titles_not_empty(self, extracted):
        assert len(extracted["panel_titles"]) >= 12
        for t in extracted["panel_titles"]:
            assert len(t) > 0, "Panel title should not be empty"

    def test_row_groups(self, extracted):
        """Dashboard uses row panels to group by signal category."""
        row_names = [r["row"] for r in extracted["row_groups"]]
        assert len(row_names) >= 4, f"Expected >=4 rows, got {row_names}"

    # ── Co-occurrence & aggregation ────────────────────────────────────

    def test_metric_cooccurrence(self, extracted):
        cooc = extracted["metric_cooccurrence"]
        assert len(cooc) > 0, "Should have metric co-occurrence data"

    def test_aggregation_patterns(self, extracted):
        aggs = extracted["aggregation_patterns"]
        agg_types = {a["aggregation"] for a in aggs}
        assert "rate" in agg_types, f"rate() not found in {agg_types}"

    def test_has_histogram_quantile(self, extracted):
        aggs = extracted["aggregation_patterns"]
        agg_types = {a["aggregation"] for a in aggs}
        assert "histogram_quantile" in agg_types

    # ── Links ──────────────────────────────────────────────────────────

    def test_has_drilldown_links(self, extracted):
        assert len(extracted["drilldown_links"]) >= 1

    # ── Dashboard metadata ─────────────────────────────────────────────

    def test_dashboard_title(self, extracted):
        assert "signal" in extracted["dashboard_title"].lower()

    def test_dashboard_tags(self, extracted):
        tags = extracted["dashboard_tags"]
        assert "tacit" in tags or "signals" in tags

    # ── Signal inference ───────────────────────────────────────────────

    def test_infers_signals(self, inferred_signals):
        assert len(inferred_signals) >= 10

    def test_covers_latency_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "request_latency" in types

    def test_covers_throughput_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "request_rate" in types

    def test_covers_error_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "error_rate" in types

    def test_covers_saturation_signals(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        saturation = {"cpu_usage", "memory_usage", "in_flight_requests", "queue_depth", "db_connection_pool"}
        assert types & saturation, f"No saturation signals in {types}"

    def test_covers_cache_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        # The caching taxonomy was split into precise signals (hits/misses/ratio/
        # evictions/size); a hit/miss counter resolves to one of these. The test's
        # intent is that *a cache signal is covered*, not one specific name.
        cache_signals = {"cache_hit_ratio", "cache_hits", "cache_misses", "cache_evictions", "cache_size"}
        assert types & cache_signals, f"No cache signal covered in {sorted(types)}"

    def test_covers_stability_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "pod_restarts" in types

    def test_covers_network_signals(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        net = {"network_bytes", "dns_failures", "tls_handshake_failures"}
        assert types & net, f"No network signals in {types}"

    def test_covers_messaging_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "consumer_lag" in types

    def test_covers_db_latency_signal(self, inferred_signals):
        types = {s["signal_type"] for s in inferred_signals}
        assert "db_query_latency" in types

    def test_signal_categories_coverage(self, inferred_signals):
        """Verify we hit at least 8 of the 12 signal categories."""
        # Collect categories from inferred signals by looking up
        # the signal_type in the bootstrap yaml
        import yaml

        resource = files("tacit.data").joinpath("signals.yaml")
        with resource.open() as f:
            data = yaml.safe_load(f)
        sig_defs = data.get("signals", {})

        categories = set()
        for s in inferred_signals:
            sig_def = sig_defs.get(s["signal_type"], {})
            cat = sig_def.get("category", "")
            if cat:
                categories.add(cat)
        assert len(categories) >= 8, f"Expected >=8 categories, got {len(categories)}: {categories}"

    # ── Archetype generation ───────────────────────────────────────────

    def test_generates_archetype_yaml(self, extracted, inferred_signals):
        yaml_str = generate_archetype_yaml(extracted, inferred_signals)
        assert "archetypes:" in yaml_str
        assert "required_signals:" in yaml_str
        assert "signal_bindings:" in yaml_str
        assert "panels:" in yaml_str


# ── Bug 3: Literal braces in generated archetype YAML ────────────────────


class TestArchetypeYamlBraceEscaping:
    """Queries with label selectors like {service=\"api\"} must not break
    str.format() when the generated archetype is later compiled."""

    def test_braces_are_escaped_in_generated_yaml(self):
        """Concrete query braces must be escaped as {{ / }} so
        compile_archetype()'s str.format(**params) does not interpret them
        as Python format placeholders."""
        import yaml

        extracted = {
            "dashboard_title": "Test Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["http_requests_total"],
            "panels": [
                {
                    "title": "RPS",
                    "queries": ['rate(http_requests_total{service="api"}[5m])'],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        signals = [
            {"signal_type": "request_rate", "metric": "http_requests_total", "confidence": 0.8},
        ]
        yaml_str = generate_archetype_yaml(extracted, signals)
        parsed = yaml.safe_load(yaml_str)
        expr = parsed["archetypes"][0]["panels"][0]["queries"][0]["expr"]

        # The expression must survive str.format() with no matching keys
        # If braces are NOT escaped, this raises KeyError('service="api"')
        result = expr.format(service_filter="", container_filter="", rate_interval="5m")
        # Verify the original brace content is preserved after formatting
        assert '{service="api"}' in result

    def test_template_placeholders_preserved(self):
        """Legitimate {service_filter} placeholders must NOT be double-escaped."""
        extracted = {
            "dashboard_title": "Template Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["http_requests_total"],
            "panels": [
                {
                    "title": "RPS",
                    "queries": ["rate(http_requests_total{{{service_filter}}}[5m])"],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        signals = []
        yaml_str = generate_archetype_yaml(extracted, signals)
        import yaml

        parsed = yaml.safe_load(yaml_str)
        expr = parsed["archetypes"][0]["panels"][0]["queries"][0]["expr"]
        # Template placeholder must still resolve as a PromQL label selector.
        result = expr.format(service_filter='job="api"', container_filter="", rate_interval="5m")
        assert '{job="api"}' in result
        assert '{{job="api"}}' not in result

    def test_rate_interval_placeholder_preserved(self):
        extracted = {
            "dashboard_title": "Interval Dashboard",
            "dashboard_tags": [],
            "metrics_found": ["http_requests_total"],
            "panels": [
                {
                    "title": "RPS",
                    "queries": ["rate(http_requests_total[ {rate_interval} ])"],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        yaml_str = generate_archetype_yaml(extracted, [])
        import yaml

        parsed = yaml.safe_load(yaml_str)
        expr = parsed["archetypes"][0]["panels"][0]["queries"][0]["expr"]

        assert "[ 1m ]" in expr.format(service_filter="", container_filter="", rate_interval="1m")

    def test_kafka_topic_selector_literal_braces_compile(self):
        from tacit.archetypes.engine import compile_archetype
        from tacit.archetypes.templates import _load_archetypes_from_yaml
        from tacit.models.schemas import ArchetypeMatch, Intent

        archetypes = _load_archetypes_from_yaml(Path(__file__).resolve().parents[2] / "tacit/data/archetypes.yaml")
        archetype = next(a for a in archetypes if a.id == "kafka_topic_throughput")
        intent = Intent(
            summary="kafka topic imbalance",
            domain="messaging",
            services=[],
            signals=[],
            keywords=["kafka", "topic"],
            timerange="4h",
            problem_type="kafka_topic_imbalance",
            archetypes=[ArchetypeMatch(type="kafka_topic_imbalance", confidence=1.0)],
        )

        spec = compile_archetype(
            archetype,
            intent,
            [
                MetricEntry(
                    name="kafka_log_log_logendoffset",
                    datasource_uid="prom",
                    datasource_name="Prometheus",
                    datasource_type="prometheus",
                    query_language="promql",
                )
            ],
        )

        exprs = [query.expr for panel in spec.panels for query in panel.queries]
        assert 'kafka_log_log_logendoffset{topic!=""}' in exprs


class TestSignalFlowCompileCompatibility:
    def test_raw_signalfx_query_is_not_recompiled_as_promql(self):
        from tacit.archetypes.engine import compile_archetype
        from tacit.models.schemas import ArchetypeMatch, Intent

        archetype = InvestigationArchetype(
            id="sfx_cpu",
            name="SFX CPU",
            problem_types=["cpu"],
            panels=[
                PanelTemplate(
                    title="CPU",
                    queries=[
                        QueryTemplate(
                            expr="data('cpu.utilization').publish()",
                            datasource_type="signalfx",
                        )
                    ],
                )
            ],
        )
        intent = Intent(
            summary="cpu",
            domain="infra",
            services=["api"],
            signals=[],
            keywords=[],
            timerange="1h",
            problem_type="cpu",
            archetypes=[ArchetypeMatch(type="cpu", confidence=1.0)],
        )
        spec = compile_archetype(
            archetype,
            intent,
            [
                MetricEntry(
                    name="cpu.utilization",
                    datasource_uid="x",
                    datasource_name="SignalFx",
                    datasource_type="signalfx",
                    query_language="signalflow",
                )
            ],
            target_language="signalflow",
        )

        assert spec.panels[0].queries[0].expr == "data('cpu.utilization').publish()"

    def test_explicit_query_language_marks_raw_signalflow(self):
        from tacit.archetypes.engine import compile_archetype
        from tacit.models.schemas import ArchetypeMatch, Intent

        archetype = InvestigationArchetype(
            id="sfx_cpu_language",
            name="SFX CPU Language",
            problem_types=["cpu"],
            panels=[
                PanelTemplate(
                    title="CPU",
                    queries=[
                        QueryTemplate(
                            expr="data('cpu.utilization').publish()",
                            query_language="signalflow",
                        )
                    ],
                )
            ],
        )
        intent = Intent(
            summary="cpu",
            domain="infra",
            services=["api"],
            signals=[],
            keywords=[],
            timerange="1h",
            problem_type="cpu",
            archetypes=[ArchetypeMatch(type="cpu", confidence=1.0)],
        )
        spec = compile_archetype(
            archetype,
            intent,
            [
                MetricEntry(
                    name="cpu.utilization",
                    datasource_uid="x",
                    datasource_name="SignalFx",
                    datasource_type="signalfx",
                    query_language="signalflow",
                )
            ],
            target_language="signalflow",
        )

        assert spec.panels[0].queries[0].expr == "data('cpu.utilization').publish()"
        assert spec.panels[0].queries[0].datasource_type == "signalfx"


# ── Bug 5: Suffix-aware metric substitution ──────────────────────────────


class TestSuffixAwareMetricSubstitution:
    """_apply_metric_substitutions must not double-suffix when the base
    binding name is a prefix of a suffixed variant in the query template."""

    def _make_archetype(self, expr: str, binding_default: str) -> InvestigationArchetype:
        return InvestigationArchetype(
            id="test",
            name="Test",
            problem_types=["test"],
            signal_bindings={"request_latency": binding_default},
            panels=[
                PanelTemplate(
                    title="P1",
                    queries=[QueryTemplate(expr=expr)],
                ),
            ],
        )

    def test_base_metric_replaced(self):
        """Simple base metric replacement still works."""
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(http_request_duration_seconds[5m])",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds",
            },
        )
        assert result.panels[0].queries[0].expr == "rate(custom_request_duration_seconds[5m])"

    def test_suffixed_variant_no_double_suffix(self):
        """Replacing base metric when the template uses _bucket suffix.
        The resolved metric is the base form, so the suffix should survive."""
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds",
            },
        )
        assert "custom_request_duration_seconds_bucket" in result.panels[0].queries[0].expr
        assert "_bucket_bucket" not in result.panels[0].queries[0].expr

    def test_resolved_metric_already_suffixed(self):
        """When the catalog match is already a suffixed form (e.g. _bucket),
        replacing the base binding should NOT produce double suffix."""
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
            binding_default="http_request_duration_seconds",
        )
        # The substitution map says base → already-suffixed resolved metric
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds_bucket",
            },
        )
        # Must NOT become custom_request_duration_seconds_bucket_bucket
        assert "_bucket_bucket" not in result.panels[0].queries[0].expr
        # The _bucket variant should appear exactly once
        assert "custom_request_duration_seconds_bucket" in result.panels[0].queries[0].expr

    def test_multiple_suffixes_in_one_expression(self):
        """An expression referencing both _bucket and _count of the same base."""
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr=(
                "histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m])) "
                "/ rate(http_request_duration_seconds_count[5m])"
            ),
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_latency",
            },
        )
        expr = result.panels[0].queries[0].expr
        assert "custom_latency_bucket" in expr
        assert "custom_latency_count" in expr
        assert "_bucket_bucket" not in expr
        assert "_count_count" not in expr

    def test_replacement_not_reprocessed_when_new_metric_contains_old_metric(self):
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(request_duration_seconds_bucket[5m])",
            binding_default="request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "request_duration_seconds": "custom_request_duration_seconds",
            },
        )

        assert result.panels[0].queries[0].expr == "rate(custom_request_duration_seconds_bucket[5m])"

    def test_replacement_obeys_metric_token_boundaries(self):
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(foo_request_duration_seconds[5m]) + rate(request_duration_seconds[5m])",
            binding_default="request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "request_duration_seconds": "custom_request_duration_seconds",
            },
        )
        expr = result.panels[0].queries[0].expr

        assert "foo_request_duration_seconds" in expr
        assert "rate(custom_request_duration_seconds[5m])" in expr
        assert "foo_custom_request_duration_seconds" not in expr

    def test_already_suffixed_metric_rebases_other_suffixes(self):
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="rate(http_request_duration_seconds_count[5m])",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "custom_request_duration_seconds_bucket",
            },
        )

        assert result.panels[0].queries[0].expr == "rate(custom_request_duration_seconds_count[5m])"

    def test_same_base_resolved_to_suffixed_form(self):
        """Bug 6: When the resolved metric shares the same base as old_metric
        and already ends with a suffix, the bare fallback must not re-replace
        inside the already-substituted suffixed name.

        Binding: http_request_duration_seconds -> http_request_duration_seconds_bucket
        Template: ...http_request_duration_seconds_bucket...
        Expected: no change (already correct), NOT _bucket_bucket.
        """
        from tacit.archetypes.engine import _apply_metric_substitutions

        arch = self._make_archetype(
            expr="histogram_quantile(0.99, rate(http_request_duration_seconds_bucket[5m]))",
            binding_default="http_request_duration_seconds",
        )
        result = _apply_metric_substitutions(
            arch,
            {
                "http_request_duration_seconds": "http_request_duration_seconds_bucket",
            },
        )
        expr = result.panels[0].queries[0].expr
        assert "_bucket_bucket" not in expr
        assert "http_request_duration_seconds_bucket" in expr


# ── Bug 7: PromQL metric extraction regex coverage ──────────────────────


class TestPromQLExtractionBug7:
    """The regex must capture metrics in positions not followed by { or [,
    e.g. inside avg(metric), metric == 0, metric / metric."""

    def test_metric_inside_function_no_braces(self):
        metrics = extract_metrics_from_promql("avg(go_goroutines)")
        assert "go_goroutines" in metrics

    def test_bare_metric_with_comparison(self):
        metrics = extract_metrics_from_promql("up == 0")
        assert "up" in metrics

    def test_metric_in_binary_expression(self):
        metrics = extract_metrics_from_promql("node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes")
        assert "node_memory_MemAvailable_bytes" in metrics
        assert "node_memory_MemTotal_bytes" in metrics

    def test_metric_followed_by_closing_paren(self):
        metrics = extract_metrics_from_promql("count(some_metric)")
        assert "some_metric" in metrics

    def test_metric_at_end_of_line(self):
        metrics = extract_metrics_from_promql("process_resident_memory_bytes")
        assert "process_resident_memory_bytes" in metrics


# ── Bug 9: tenant teaching must preserve global bootstrap fallback ─────


class TestTeachUpsertContext:
    """Tenant-scoped mappings override, but never mutate, global defaults."""

    def test_tenant_mapping_preserves_global_context_fallback(self, signal_store):
        signal_store._add_bootstrap_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
        )
        # Re-teach with a service scope
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            context_services=["checkout"],
            source_type="teach",
        )

        checkout_mappings = signal_store.get_mappings_for_signal("request_latency", context_service="checkout")
        payments_mappings = signal_store.get_mappings_for_signal("request_latency", context_service="payments")

        assert len(checkout_mappings) == 1
        assert checkout_mappings[0]["context_services"] == ["checkout"]
        assert len(payments_mappings) == 1
        assert payments_mappings[0]["context_services"] == []

    def test_upsert_unions_existing_scoped_context_services(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            context_services=["checkout"],
            source_type="teach",
        )
        signal_store.add_mapping(
            "request_latency",
            "checkout_latency_seconds",
            confidence=0.9,
            context_services=["payments"],
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert set(mappings[0]["context_services"]) == {"checkout", "payments"}

    def test_upsert_updates_source_type(self, signal_store):
        signal_store._add_bootstrap_mapping(
            "request_latency",
            "latency_metric",
            confidence=0.8,
        )
        signal_store.add_mapping(
            "request_latency",
            "latency_metric",
            confidence=0.8,
            source_type="teach",
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["source_type"] == "teach"

    def test_bootstrap_reload_preserves_learned_provenance(self, signal_store):
        signal_store.add_mapping(
            "request_latency",
            "http_requests_total",
            confidence=0.8,
            context_services=["checkout"],
            source_type="dashboard_ingest",
            source_refs=["grafana:checkout-dash"],
        )
        signal_store._add_bootstrap_mapping(
            "request_latency",
            "http_requests_total",
            confidence=0.9,
        )

        mappings = signal_store.get_mappings_for_signal("request_latency")
        assert len(mappings) == 1
        assert mappings[0]["source_type"] == "dashboard_ingest"
        assert mappings[0]["source_refs"] == ["grafana:checkout-dash"]
        assert mappings[0]["context_services"] == ["checkout"]


# ── Bug 10: pending ingestion must store full signal records ─────────────


class TestPendingIngestionSignalRecords:
    """signals_inferred stored in ingested_dashboards should include the
    metric and confidence from infer_signals_from_metrics(), not just
    the signal type name."""

    def test_signals_inferred_includes_metric_and_confidence(self, signal_store):
        signal_store.record_ingested_dashboard(
            dashboard_uid="test-dash",
            dashboard_title="Test",
            signals_inferred=[
                {"signal_type": "request_latency", "metric": "http_request_duration_seconds", "confidence": 0.95},
                {"signal_type": "error_rate", "metric": "http_requests_total", "confidence": 0.8},
            ],
            status="pending",
        )

        ingested = signal_store.get_ingested_dashboard("test-dash")
        assert ingested is not None
        sigs = ingested["signals_inferred"]
        assert len(sigs) == 2
        assert sigs[0]["metric"] == "http_request_duration_seconds"
        assert sigs[0]["confidence"] == 0.95


# ── Bug 11: SignalFlow queries should not be written as PromQL templates ─


class TestSignalFlowArchetypeGeneration:
    """When the ingested dashboard is from SignalFx, generate_archetype_yaml
    should tag query templates with a datasource_type so compile_archetype
    knows not to convert them through _promql_template_to_signalflow."""

    def test_signalflow_query_preserved_in_archetype(self):
        extracted = {
            "dashboard_title": "SignalFx Dash",
            "dashboard_tags": [],
            "metrics_found": ["cpu.utilization"],
            "query_language": "signalflow",
            "panels": [
                {
                    "title": "CPU",
                    "queries": ["data('cpu.utilization').publish()"],
                    "row": "",
                    "unit": "",
                    "description": "",
                },
            ],
        }
        signals = []
        import yaml

        yaml_str = generate_archetype_yaml(extracted, signals)
        parsed = yaml.safe_load(yaml_str)
        query = parsed["archetypes"][0]["panels"][0]["queries"][0]
        # Must indicate this is already SignalFlow, not PromQL
        assert query.get("datasource_type") == "signalfx"
        assert query.get("query_language") == "signalflow"
        # Expression must be preserved as-is (no brace escaping
        # that would break SignalFlow syntax)
        assert "data('cpu.utilization').publish()" in query["expr"]


async def test_dashboard_refresh_retires_removed_signal_knowledge(signal_store, monkeypatch):
    from tacit import dashboard_ingest as di

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
    monkeypatch.setattr(di, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(
        "tacit.dashboard_ingest.service.infer_signals_from_metrics",
        lambda *args, **kwargs: next(batches),
    )

    def features(metric: str) -> DashboardFeatures:
        return DashboardFeatures(
            dashboard_uid="refresh-dashboard",
            dashboard_title="Refresh dashboard",
            backend_name="grafana",
            query_language="promql",
            metrics_found=[metric],
            panel_count=1,
            panels=[{"title": "Latency", "metrics": [metric], "queries": [metric]}],
        )

    await di.ingest_dashboard_features(features("old_latency_seconds"), auto_approve=True)
    await di.ingest_dashboard_features(features("new_latency_seconds"), auto_approve=True)

    assert signal_store.get_mappings_for_signal("old_latency", include_decayed=True) == []
    candidates = KnowledgeRepository(signal_store._db_path).list_candidates("default", kind="signal_mapping")
    old = next(candidate for candidate in candidates if "old_latency_seconds" in candidate.payload_ref)
    new = next(candidate for candidate in candidates if "new_latency_seconds" in candidate.payload_ref)
    assert old.state.lifecycle_status.value == "stale"
    assert new.state.lifecycle_status.value == "active"


async def test_pending_dashboard_refresh_retires_removed_support_without_promoting_replacement(
    signal_store,
    monkeypatch,
):
    from tacit import dashboard_ingest as di

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
        "tacit.dashboard_ingest.service.infer_signals_from_metrics",
        lambda *args, **kwargs: next(batches),
    )

    def features(metric: str) -> DashboardFeatures:
        return DashboardFeatures(
            dashboard_uid="pending-refresh-dashboard",
            dashboard_title="Pending refresh dashboard",
            backend_name="grafana",
            query_language="promql",
            metrics_found=[metric],
            panel_count=1,
            panels=[{"title": "Latency", "metrics": [metric], "queries": [metric]}],
        )

    await di.ingest_dashboard_features(
        features("old_latency_seconds"),
        auto_approve=True,
        store=signal_store,
    )
    pending = await di.ingest_dashboard_features(
        features("new_latency_seconds"),
        auto_approve=False,
        store=signal_store,
    )

    candidates = KnowledgeRepository(signal_store._db_path).list_candidates(
        "default",
        kind="signal_mapping",
        limit=None,
    )
    old = next(candidate for candidate in candidates if "old_latency_seconds" in candidate.payload_ref)
    assert pending["status"] == "pending"
    assert old.state.lifecycle_status.value == "stale"
    assert all("new_latency_seconds" not in candidate.payload_ref for candidate in candidates)
    assert signal_store.get_mappings_for_signal("old_latency", include_decayed=True) == []


async def test_pending_dashboard_refresh_rolls_back_source_authority_and_index_together(
    signal_store,
    monkeypatch,
):
    from tacit import dashboard_ingest as di

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
        "tacit.dashboard_ingest.service.infer_signals_from_metrics",
        lambda *args, **kwargs: next(batches),
    )

    def features(metric: str) -> DashboardFeatures:
        return DashboardFeatures(
            dashboard_uid="atomic-pending-refresh",
            dashboard_title="Atomic pending refresh",
            backend_name="grafana",
            query_language="promql",
            metrics_found=[metric],
            panel_count=1,
            panels=[{"title": "Latency", "metrics": [metric], "queries": [metric]}],
        )

    await di.ingest_dashboard_features(
        features("old_latency_seconds"),
        auto_approve=True,
        store=signal_store,
    )
    repository = KnowledgeRepository(signal_store._db_path)
    before_source = signal_store.get_ingested_dashboard("atomic-pending-refresh", "grafana")
    before_candidate = next(
        candidate
        for candidate in repository.list_candidates("default", kind="signal_mapping", limit=None)
        if "old_latency_seconds" in candidate.payload_ref
    )
    assert before_source is not None and before_source["status"] == "approved"
    assert before_candidate.state.lifecycle_status.value == "active"
    with signal_store._conn() as connection:
        before_mapping = connection.execute(
            """SELECT id FROM signal_metric_mappings
               WHERE tenant_id=? AND source_refs LIKE ?""",
            ("default", '%"grafana:atomic-pending-refresh"%'),
        ).fetchone()
    assert before_mapping is not None

    index_generation = signal_store.index_dashboard_context

    def fail_after_index_write(**kwargs):
        index_generation(**kwargs)
        raise RuntimeError("simulated pending dashboard index failure")

    monkeypatch.setattr(signal_store, "index_dashboard_context", fail_after_index_write)

    with pytest.raises(RuntimeError, match="simulated pending dashboard index failure"):
        await di.ingest_dashboard_features(
            features("new_latency_seconds"),
            auto_approve=False,
            store=signal_store,
        )

    after_source = signal_store.get_ingested_dashboard("atomic-pending-refresh", "grafana")
    after_candidate = repository.get_candidate(before_candidate.id, "default")
    assert after_source is not None
    assert after_source["status"] == "approved"
    assert after_source["metrics_found"] == ["old_latency_seconds"]
    assert after_candidate == before_candidate
    with signal_store._conn() as connection:
        after_mapping = connection.execute(
            "SELECT id FROM signal_metric_mappings WHERE id=?",
            (before_mapping["id"],),
        ).fetchone()
    assert after_mapping is not None
    if signal_store._learning_index_available():
        assert signal_store.search_learning_context("old_latency_seconds")
        assert signal_store.search_learning_context("new_latency_seconds") == []


async def test_auto_approved_dashboard_rolls_back_authority_when_indexing_fails(
    signal_store,
    monkeypatch,
):
    from tacit import dashboard_ingest as di

    inferred = [
        {
            "signal_type": "request_latency",
            "metric": "atomic_approval_latency_seconds",
            "confidence": 0.9,
            "source": "heuristic",
            "signal_family": "latency",
            "auto_teach_eligible": True,
        }
    ]
    monkeypatch.setattr(
        "tacit.dashboard_ingest.service.infer_signals_from_metrics",
        lambda *args, **kwargs: inferred,
    )
    features = DashboardFeatures(
        dashboard_uid="atomic-approved-dashboard",
        dashboard_title="Atomic approved dashboard",
        backend_name="grafana",
        query_language="promql",
        metrics_found=["atomic_approval_latency_seconds"],
        panel_count=1,
        panels=[
            {
                "title": "Latency",
                "metrics": ["atomic_approval_latency_seconds"],
                "queries": ["atomic_approval_latency_seconds"],
            }
        ],
    )
    knowledge_service = KnowledgeService(
        KnowledgeRepository(signal_store._db_path),
        signal_store=signal_store,
    )
    repository = knowledge_service.repository

    def promote_governed_mapping(**kwargs):
        candidate_id = migrate_signal_mapping(
            {
                "id": "atomic-approved-dashboard",
                "signal_type": "request_latency",
                "metric_pattern": "atomic_approval_latency_seconds",
                "source_type": "dashboard_ingest",
                "source_refs": [kwargs["source_ref"]],
            },
            service=knowledge_service,
        )
        knowledge_service.review_candidate(candidate_id, approved=True, reviewer="test")
        _decision, revision = knowledge_service.evaluate_candidate(candidate_id, live_verified=True)
        assert revision is not None
        kwargs["governed_candidate_ids"].add(candidate_id)
        kwargs["governed_pairs"].add(("atomic_approval_latency_seconds", "request_latency"))
        return True

    monkeypatch.setattr(
        "tacit.dashboard_ingest.service.persist_inferred_signal_review",
        promote_governed_mapping,
    )
    index_generation = signal_store.index_dashboard_context

    def fail_after_index_write(**kwargs):
        index_generation(**kwargs)
        raise RuntimeError("simulated approved dashboard index failure")

    monkeypatch.setattr(signal_store, "index_dashboard_context", fail_after_index_write)

    with pytest.raises(RuntimeError, match="simulated approved dashboard index failure"):
        await di.ingest_dashboard_features(
            features,
            auto_approve=True,
            store=signal_store,
            knowledge_service=knowledge_service,
        )

    source = signal_store.get_ingested_dashboard("atomic-approved-dashboard", "grafana")
    assert source is None
    assert repository.list_candidates() == []
    assert repository.list_current_revisions() == []
    assert signal_store.get_mappings_for_signal("request_latency", include_decayed=True) == []
    with signal_store._conn() as connection:
        indexed = connection.execute(
            "SELECT COUNT(*) FROM learning_context_fts WHERE tenant_id=? AND dashboard_uid=?",
            ("default", "atomic-approved-dashboard"),
        ).fetchone()[0]
    assert indexed == 0

    monkeypatch.setattr(signal_store, "index_dashboard_context", index_generation)
    recovered = await di.ingest_dashboard_features(
        features,
        auto_approve=True,
        store=signal_store,
        knowledge_service=knowledge_service,
    )

    assert recovered["status"] == "approved"
    assert repository.list_current_revisions()
    assert signal_store.get_mappings_for_signal("request_latency", include_decayed=True)
    with signal_store._conn() as connection:
        indexed = connection.execute(
            "SELECT COUNT(*) FROM learning_context_fts WHERE tenant_id=? AND dashboard_uid=?",
            ("default", "atomic-approved-dashboard"),
        ).fetchone()[0]
    assert indexed > 0


async def test_changed_auto_approved_dashboard_preserves_prior_authority_when_preparation_fails(
    signal_store,
    monkeypatch,
):
    from tacit import dashboard_ingest as di

    def inferred(metrics, *_args, **_kwargs):
        metric = metrics[0]
        return [
            {
                "signal_type": "request_latency",
                "metric": metric,
                "confidence": 0.9,
                "source": "heuristic",
                "signal_family": "latency",
                "auto_teach_eligible": True,
            }
        ]

    monkeypatch.setattr("tacit.dashboard_ingest.service.infer_signals_from_metrics", inferred)
    knowledge_service = KnowledgeService(
        KnowledgeRepository(signal_store._db_path),
        signal_store=signal_store,
    )

    def promote_governed_mapping(**kwargs):
        metric = next(iter(kwargs["governed_pairs"]))[0] if kwargs["governed_pairs"] else None
        signal = kwargs["sig"]
        metric = metric or signal["metric"]
        candidate_id = migrate_signal_mapping(
            {
                "id": f"atomic-refresh:{metric}",
                "signal_type": signal["signal_type"],
                "metric_pattern": metric,
                "source_type": "dashboard_ingest",
                "source_refs": [kwargs["source_ref"]],
            },
            service=knowledge_service,
        )
        knowledge_service.review_candidate(candidate_id, approved=True, reviewer="test")
        _decision, revision = knowledge_service.evaluate_candidate(candidate_id, live_verified=True)
        assert revision is not None
        kwargs["governed_candidate_ids"].add(candidate_id)
        kwargs["governed_pairs"].add((metric, signal["signal_type"]))
        return True

    monkeypatch.setattr(
        "tacit.dashboard_ingest.service.persist_inferred_signal_review",
        promote_governed_mapping,
    )

    def features(metric: str) -> DashboardFeatures:
        return DashboardFeatures(
            dashboard_uid="atomic-refresh-dashboard",
            dashboard_title="Atomic refresh dashboard",
            backend_name="grafana",
            query_language="promql",
            metrics_found=[metric],
            panel_count=1,
            panels=[{"title": "Latency", "metrics": [metric], "queries": [metric]}],
        )

    await di.ingest_dashboard_features(
        features("old_latency_seconds"),
        auto_approve=True,
        store=signal_store,
        knowledge_service=knowledge_service,
    )
    revisions_before = knowledge_service.repository.list_current_revisions()
    index_generation = signal_store.index_dashboard_context

    def fail_after_index_write(**kwargs):
        index_generation(**kwargs)
        raise RuntimeError("simulated refresh preparation failure")

    monkeypatch.setattr(signal_store, "index_dashboard_context", fail_after_index_write)
    with pytest.raises(RuntimeError, match="simulated refresh preparation failure"):
        await di.ingest_dashboard_features(
            features("new_latency_seconds"),
            auto_approve=True,
            store=signal_store,
            knowledge_service=knowledge_service,
        )

    persisted = signal_store.get_ingested_dashboard("atomic-refresh-dashboard", "grafana")
    assert persisted is not None
    assert persisted["status"] == "approved"
    assert persisted["metrics_found"] == ["old_latency_seconds"]
    assert knowledge_service.repository.list_current_revisions() == revisions_before
    mappings = signal_store.get_mappings_for_signal("request_latency", include_decayed=True)
    assert {mapping["metric_pattern"] for mapping in mappings} == {"old_latency_seconds"}


async def test_pending_dashboard_refresh_revalidates_changed_source_lineage(signal_store, monkeypatch):
    from tacit import dashboard_ingest as di

    inferred = [
        {
            "signal_type": "checkout_latency",
            "metric": "checkout_latency_seconds",
            "confidence": 0.9,
            "source": "heuristic",
            "signal_family": "latency",
            "auto_teach_eligible": True,
        }
    ]
    monkeypatch.setattr(
        "tacit.dashboard_ingest.service.infer_signals_from_metrics",
        lambda *args, **kwargs: inferred,
    )

    def features(query: str) -> DashboardFeatures:
        return DashboardFeatures(
            dashboard_uid="lineage-refresh-dashboard",
            dashboard_title="Lineage refresh dashboard",
            backend_name="grafana",
            query_language="promql",
            metrics_found=["checkout_latency_seconds"],
            panel_count=1,
            query_transformations=[query],
            panels=[
                {
                    "title": "Latency",
                    "metrics": ["checkout_latency_seconds"],
                    "queries": [query],
                }
            ],
        )

    await di.ingest_dashboard_features(
        features("checkout_latency_seconds"),
        auto_approve=True,
        store=signal_store,
    )
    pending = await di.ingest_dashboard_features(
        features("rate(checkout_latency_seconds[5m])"),
        auto_approve=False,
        store=signal_store,
    )

    candidates = KnowledgeRepository(signal_store._db_path).list_candidates(
        "default",
        kind="signal_mapping",
        limit=None,
    )
    original = next(candidate for candidate in candidates if "checkout_latency_seconds" in candidate.payload_ref)
    assert pending["status"] == "pending"
    assert original.state.lifecycle_status.value == "stale"
    assert signal_store.get_mappings_for_signal("checkout_latency", include_decayed=True) == []


@pytest.mark.asyncio
async def test_dashboard_ingestion_uses_explicit_runtime_signal_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from tacit import dashboard_ingest as dashboard_ingest_module

    db_path = tmp_path / "scoped-dashboard-signals.db"
    runtime_settings = Settings(
        _env_file=None,
        signals_db_path=str(db_path),
        knowledge_tenant_id="tenant-a",
    )

    def unexpected_global_store():
        raise AssertionError("explicit dashboard runtime consulted the process-global signal store")

    monkeypatch.setattr("tacit.dashboard_ingest.service.get_signal_store", unexpected_global_store)
    features = DashboardFeatures(
        dashboard_uid="scoped-dashboard",
        dashboard_title="Scoped dashboard",
        backend_name="grafana",
        query_language="promql",
        metrics_found=["checkout_requests_total"],
        panel_count=1,
        panels=[{"title": "Requests", "metrics": ["checkout_requests_total"]}],
    )

    result = await dashboard_ingest_module.ingest_dashboard_features(
        features,
        runtime_settings=runtime_settings,
    )

    assert result["status"] == "pending"
    scoped_store = SignalStore(db_path=db_path, runtime_settings=runtime_settings)
    assert scoped_store.get_ingested_dashboard("scoped-dashboard", "grafana", tenant_id="tenant-a") is not None


def test_manual_signal_teaching_rolls_back_the_complete_pattern_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    from fastapi.testclient import TestClient

    from tacit.api.app import create_app
    from tacit.knowledge import migration as migration_module

    db_path = tmp_path / "atomic-teach.db"
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(db_path),
            knowledge_tenant_id="tenant-a",
        )
    )
    original = migration_module.migrate_signal_mapping
    call_count = 0

    def fail_second_mapping(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("injected second-pattern failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(migration_module, "migrate_signal_mapping", fail_second_mapping)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post(
        "/api/v1/signals/teach",
        json={
            "signal_type": "atomic_custom_signal",
            "metric_patterns": [
                {"pattern": "atomic_metric_a", "confidence": 0.9},
                {"pattern": "atomic_metric_b", "confidence": 0.8},
            ],
            "taught_by": "failure-injection",
        },
    )

    assert response.status_code == 500
    repository = app.state.runtime_stores.knowledge_repository()
    signal_store = app.state.runtime_stores.signals()
    assert repository.list_candidates("tenant-a", kind="signal_mapping") == []
    assert repository.list_current_revisions("tenant-a") == []
    assert signal_store.get_signal_type("atomic_custom_signal", tenant_id="tenant-a") is None
    with signal_store._conn() as connection:
        rows = connection.execute(
            """SELECT metric_pattern FROM signal_metric_mappings
               WHERE tenant_id=? AND metric_pattern IN (?, ?)""",
            ("tenant-a", "atomic_metric_a", "atomic_metric_b"),
        ).fetchall()
    assert rows == []


def test_signal_taxonomy_api_uses_keyset_pages(tmp_path: Path):
    from fastapi.testclient import TestClient

    from tacit.api.app import create_app

    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(tmp_path / "paged-taxonomy.db"),
            knowledge_tenant_id="tenant-a",
        )
    )
    store = app.state.runtime_stores.signals()
    for signal_type in ("custom_a", "custom_b", "custom_c"):
        store.register_signal_type(signal_type, tenant_id="tenant-a")
    client = TestClient(app)

    first = client.get("/api/v1/signals", params={"limit": 2})
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["has_more"] is True
    assert first_body["next_cursor"]

    second = client.get(
        "/api/v1/signals",
        params={"limit": 2, "cursor": first_body["next_cursor"]},
    )
    assert second.status_code == 200
    second_body = second.json()
    names = {row["signal_type"] for row in [*first_body["signal_types"], *second_body["signal_types"]]}
    assert {"custom_a", "custom_b", "custom_c"}.issubset(names)
    assert not {row["signal_type"] for row in first_body["signal_types"]}.intersection(
        row["signal_type"] for row in second_body["signal_types"]
    )


def test_signal_taxonomy_api_continues_after_empty_signal_name(tmp_path: Path):
    from fastapi.testclient import TestClient

    from tacit.api.app import create_app

    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(tmp_path / "empty-signal-name-page.db"),
            knowledge_tenant_id="tenant-a",
        )
    )
    store = app.state.runtime_stores.signals()
    now = time.time()
    with store._conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO tenant_signal_types
               (tenant_id, signal_type, description, category, unit, created_at, updated_at)
               VALUES ('tenant-a', '', 'Migrated unnamed signal', '', '', ?, ?)""",
            (now, now),
        )
    client = TestClient(app)

    first = client.get("/api/v1/signals", params={"limit": 1})
    assert first.status_code == 200
    first_body = first.json()
    assert [row["signal_type"] for row in first_body["signal_types"]] == [""]
    assert first_body["next_cursor"]

    second = client.get(
        "/api/v1/signals",
        params={"limit": 1, "cursor": first_body["next_cursor"]},
    )
    assert second.status_code == 200
    assert all(row["signal_type"] != "" for row in second.json()["signal_types"])


def test_signal_mapping_api_cursor_accepts_zero_negative_and_sparse_ids(tmp_path: Path):
    from fastapi.testclient import TestClient

    from tacit.api.app import create_app

    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(tmp_path / "legal-mapping-id-page.db"),
            knowledge_tenant_id="tenant-a",
        )
    )
    store = app.state.runtime_stores.signals()
    signal_type = "legacy_boundary_signal"
    store.register_signal_type(signal_type, tenant_id="tenant-a")
    now = time.time()
    with store._conn() as conn:
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (id, tenant_id, signal_type, metric_pattern, confidence, source_type,
                review_state, created_at, last_seen)
               VALUES (?, 'tenant-a', ?, ?, 0.9, 'teach', 'candidate', ?, ?)""",
            [
                (-41, signal_type, "negative_id_metric", now, now),
                (0, signal_type, "zero_id_metric", now, now),
                (100_003, signal_type, "sparse_id_metric", now, now),
            ],
        )
    client = TestClient(app)

    cursor = None
    observed_ids = []
    for _ in range(3):
        response = client.get(
            f"/api/v1/signals/{signal_type}",
            params={"limit": 1, **({"cursor": cursor} if cursor else {})},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        observed_ids.append(body["mappings"][0]["id"])
        cursor = body["next_cursor"]

    assert observed_ids == [100_003, 0, -41]
    assert cursor is None


def test_dashboard_and_alert_api_cursors_round_trip_full_sqlite_integer_domain(tmp_path: Path):
    from fastapi.testclient import TestClient

    from tacit.api.app import create_app

    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(tmp_path / "learning-api-boundary-ids.db"),
            knowledge_tenant_id="tenant-a",
        )
    )
    store = app.state.runtime_stores.signals()
    replacement_ids = (_SQLITE_MIN_ID, -31, 0, 100_003, _SQLITE_MAX_ID)
    for index in range(len(replacement_ids)):
        store.record_ingested_dashboard(
            f"dashboard-{index}",
            backend_name="grafana",
            tenant_id="tenant-a",
        )
        store.record_ingested_alert(
            f"alert-{index}",
            backend_name="grafana",
            fingerprint=f"alert-fingerprint-{index}",
            tenant_id="tenant-a",
        )
    for table in ("ingested_dashboards", "ingested_alerts"):
        _replace_table_ids(store, table, replacement_ids)
    with store._conn() as conn:
        conn.execute("UPDATE ingested_dashboards SET created_at=100.0")
        conn.execute("UPDATE ingested_alerts SET created_at=100.0")

    client = TestClient(app)
    for endpoint, item_key in (
        ("/api/v1/learn/dashboards", "dashboards"),
        ("/api/v1/learn/alerts", "alerts"),
    ):
        params: dict[str, int | float] = {"limit": 1}
        observed: list[int] = []
        while True:
            response = client.get(endpoint, params=params)
            assert response.status_code == 200, response.text
            body = response.json()
            observed.extend(int(item["id"]) for item in body[item_key])
            cursor = body["next_cursor"]
            if cursor is None:
                break
            params = {"limit": 1, **cursor}
        assert observed == list(reversed(replacement_ids))


def test_source_lifecycle_scans_include_every_legal_integer_key(signal_store: SignalStore):
    legal_ids = (-(2**63), -17, 0, 100_003, 2**63 - 1)
    artifact_ids = [f"artifact-{index}" for index in range(len(legal_ids))]
    dashboard_ids = [f"dashboard-{index}" for index in range(len(legal_ids))]
    alert_ids = [f"alert-{index}" for index in range(len(legal_ids))]

    for artifact_id in artifact_ids:
        signal_store.record_learned_artifact(
            artifact_id=artifact_id,
            artifact_type="runbook",
            fingerprint=f"fingerprint-{artifact_id}",
        )
    for dashboard_id in dashboard_ids:
        signal_store.record_ingested_dashboard(dashboard_id, backend_name="grafana")
    for alert_id in alert_ids:
        signal_store.record_ingested_alert(
            alert_id,
            backend_name="grafana",
            fingerprint=f"fingerprint-{alert_id}",
        )

    with signal_store._conn() as conn:
        for table, key_column, keys in (
            ("learned_artifacts", "artifact_id", artifact_ids),
            ("ingested_dashboards", "dashboard_uid", dashboard_ids),
            ("ingested_alerts", "alert_uid", alert_ids),
        ):
            for key, replacement_id in zip(keys, legal_ids, strict=True):
                conn.execute(
                    f"UPDATE {table} SET id=? WHERE tenant_id='default' AND {key_column}=?",
                    (replacement_id, key),
                )

    reconciled_artifacts: list[str] = []
    reconciled_dashboards: list[str] = []
    reconciled_alerts: list[str] = []
    crawl_started_at = time.time() + 1
    assert signal_store.mark_missing_artifacts_stale(
        artifact_type="runbook",
        seen_artifact_ids=set(),
        crawl_started_at=crawl_started_at,
        authority_reconciler=lambda _conn, row: reconciled_artifacts.append(str(row["artifact_id"])),
    ) == len(legal_ids)
    assert signal_store.mark_missing_dashboards_stale(
        backend_name="grafana",
        seen_dashboard_uids=set(),
        crawl_started_at=crawl_started_at,
        authority_reconciler=lambda _conn, row: reconciled_dashboards.append(str(row["dashboard_uid"])),
    ) == len(legal_ids)
    assert signal_store.mark_missing_alerts_stale(
        backend_name="grafana",
        seen_alert_uids=set(),
        crawl_started_at=crawl_started_at,
        authority_reconciler=lambda _conn, row: reconciled_alerts.append(str(row["alert_uid"])),
    ) == len(legal_ids)

    assert set(reconciled_artifacts) == set(artifact_ids)
    assert set(reconciled_dashboards) == set(dashboard_ids)
    assert set(reconciled_alerts) == set(alert_ids)
    with signal_store._conn() as conn:
        for table in ("learned_artifacts", "ingested_dashboards", "ingested_alerts"):
            conn.execute(f"UPDATE {table} SET knowledge_reconciled_at=NULL WHERE tenant_id='default'")
    assert {
        int(row["id"])
        for row in signal_store.list_unreconciled_stale_artifacts(
            artifact_type="runbook",
            limit=len(legal_ids),
        )
    } == set(legal_ids)
    assert {
        int(row["id"])
        for row in signal_store.list_unreconciled_stale_dashboards(
            backend_name="grafana",
            limit=len(legal_ids),
        )
    } == set(legal_ids)
    assert {
        int(row["id"])
        for row in signal_store.list_unreconciled_stale_alerts(
            backend_name="grafana",
            limit=len(legal_ids),
        )
    } == set(legal_ids)


def test_artifact_and_extraction_cursors_round_trip_zero_negative_and_empty_keys(
    signal_store: SignalStore,
):
    for artifact_id in ("negative", "zero", "positive"):
        signal_store.record_learned_artifact(
            artifact_id=artifact_id,
            artifact_type="runbook",
            fingerprint=f"fingerprint-{artifact_id}",
        )
    with signal_store._conn() as conn:
        for artifact_id, replacement_id in (("negative", -7), ("zero", 0), ("positive", 9)):
            conn.execute(
                "UPDATE learned_artifacts SET id=?, updated_at=123 WHERE artifact_id=?",
                (replacement_id, artifact_id),
            )

    cursor = None
    observed_artifact_ids = []
    for _ in range(3):
        page = signal_store.list_learned_artifacts_page(
            artifact_type="runbook",
            limit=1,
            cursor=cursor,
        )
        observed_artifact_ids.append(int(page.items[0]["id"]))
        cursor = page.next_cursor
    assert observed_artifact_ids == [9, 0, -7]

    signal_store.replace_artifact_extractions(
        artifact_id="positive",
        evidence_requirements=[
            {"id": "", "subject": "empty-key"},
            {"id": "next", "subject": "next-key"},
        ],
    )
    first = signal_store.list_artifact_extraction_page(
        "positive",
        extraction_kind="evidence_requirements",
        limit=1,
    )
    assert [row["id"] for row in first.items] == [""]
    assert first.next_cursor
    second = signal_store.list_artifact_extraction_page(
        "positive",
        extraction_kind="evidence_requirements",
        limit=1,
        cursor=first.next_cursor,
    )
    assert [row["id"] for row in second.items] == ["next"]


@pytest.mark.parametrize(
    ("endpoint", "response_key", "table", "key_column", "record_kind"),
    (
        ("/api/v1/learn/dashboards", "dashboards", "ingested_dashboards", "dashboard_uid", "dashboard"),
        ("/api/v1/learn/alerts", "alerts", "ingested_alerts", "alert_uid", "alert"),
    ),
)
def test_learning_api_cursors_accept_zero_and_negative_ids(
    tmp_path: Path,
    endpoint: str,
    response_key: str,
    table: str,
    key_column: str,
    record_kind: str,
):
    from fastapi.testclient import TestClient

    from tacit.api.app import create_app

    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(tmp_path / f"{record_kind}-cursor.db"),
            knowledge_tenant_id="tenant-a",
        )
    )
    store = app.state.runtime_stores.signals()
    keys = [f"{record_kind}-{index}" for index in range(3)]
    for key in keys:
        if record_kind == "dashboard":
            store.record_ingested_dashboard(key, tenant_id="tenant-a", backend_name="grafana")
        else:
            store.record_ingested_alert(
                key,
                tenant_id="tenant-a",
                backend_name="grafana",
                fingerprint=f"fingerprint-{key}",
            )
    with store._conn() as conn:
        for key, replacement_id in zip(keys, (-9, 0, 11), strict=True):
            conn.execute(
                f"UPDATE {table} SET id=?, created_at=123 WHERE tenant_id='tenant-a' AND {key_column}=?",
                (replacement_id, key),
            )

    client = TestClient(app)
    cursor_params: dict[str, int | float] = {}
    observed_ids: list[int] = []
    for _ in range(3):
        response = client.get(endpoint, params={"limit": 1, **cursor_params})
        assert response.status_code == 200, response.text
        body = response.json()
        observed_ids.append(int(body[response_key][0]["id"]))
        cursor_params = body["next_cursor"]

    terminal = client.get(endpoint, params={"limit": 1, **cursor_params})
    assert terminal.status_code == 200, terminal.text
    assert terminal.json()[response_key] == []
    assert observed_ids == [11, 0, -9]


class TestLearningTabRendering:
    def _learning_render_section(self) -> str:
        html = (Path(__file__).parent.parent.parent / "tacit" / "static" / "index.html").read_text()
        return html.split("function renderIngestedDashboards(request)", 1)[1].split(
            "async function approveDashboard", 1
        )[0]

    def test_ingested_dashboard_signal_chips_render_fields_not_object_repr(self):
        load_section = self._learning_render_section()
        assert "d.signals_inferred" in load_section
        assert "s.signal_type" in load_section
        assert "s.metric" in load_section
        assert "s.confidence" in load_section

    def test_ingested_dashboard_list_renders_persisted_archetype_yaml(self):
        load_section = self._learning_render_section()
        assert "d.archetype_generated" in load_section
        assert "Quarantined experimental archetype YAML" in load_section

    def test_ingested_dashboard_approval_uses_data_attributes_not_inline_js(self):
        html = (Path(__file__).parent.parent.parent / "tacit" / "static" / "index.html").read_text()
        load_section = self._learning_render_section()
        assert 'onclick="approveDashboard' not in load_section
        assert "data-dashboard-uid" in load_section
        assert "data-dashboard-backend" in load_section
        assert "data-dashboard-tenant" in load_section
        assert "const request = captureTenantRequest(tenant)" in load_section
        assert "fetchTenantJson(`${BASE}/api/v1/learn/dashboards?limit=50`, {}, request)" in load_section
        assert "btn.dataset.dashboardTenant" in html
        assert "encodeURIComponent(uid)" in html

    def test_ingested_dashboard_approving_state_can_resume_for_loaded_tenant(self):
        load_section = self._learning_render_section()
        assert "d.status === 'approving'" in load_section
        assert "Resume approval" in load_section
        assert 'data-dashboard-tenant="${escAttr(request.tenant)}"' in load_section
