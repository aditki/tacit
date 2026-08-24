from __future__ import annotations

import json
import math
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

import tacit.signals.store as signal_store_module
from tacit.api.app import create_app
from tacit.config import Settings
from tacit.knowledge.models import KnowledgeRevision, KnowledgeScope
from tacit.knowledge.repository import KnowledgeRepository
from tacit.models.schemas import MetricEntry
from tacit.pagination import encode_cursor
from tacit.signals import SignalStore
from tacit.signals import migrations as signal_migrations

_SQLITE_MIN_ID = -(2**63)
_SQLITE_MAX_ID = 2**63 - 1


def _learning_client(tmp_path: Path, name: str) -> tuple[TestClient, SignalStore]:
    app = create_app(
        runtime_settings=Settings(
            _env_file=None,
            signals_db_path=str(tmp_path / f"{name}.db"),
            knowledge_tenant_id="tenant-a",
        )
    )
    return TestClient(app), app.state.runtime_stores.signals()


def _record_dashboard(store: SignalStore, key: str) -> None:
    store.record_ingested_dashboard(key, backend_name="grafana", tenant_id="tenant-a")


def _record_alert(store: SignalStore, key: str) -> None:
    store.record_ingested_alert(
        key,
        backend_name="grafana",
        fingerprint=f"fingerprint-{key}",
        tenant_id="tenant-a",
    )


def _signal_mapping_revision(
    knowledge_id: str,
    mappings: list[object],
) -> KnowledgeRevision:
    return KnowledgeRevision(
        knowledge_id=knowledge_id,
        revision=1,
        proposition={
            "kind": "signal_mapping",
            "subject_ref": "concept:s1-audit",
            "predicate": "represented_by",
            "object_ref": "concept:s1-audit-metric",
            "concept_ref": "signal:s1_audit_signal",
        },
        scope=KnowledgeScope(),
        state={
            "review_state": "approved",
            "lifecycle_status": "active",
            "eligibility": "live_verified",
        },
        corroboration_snapshot_ref="snapshot:s1",
        policy_id="s1-policy",
        policy_version="1",
        decision_ref="decision:s1",
        provenance_refs=["dashboard:s1"],
        resolver_payload={"mappings": mappings},
        semantic_fingerprint=f"semantic:{knowledge_id}",
    )


def _insert_active_signal_authority(
    store: SignalStore,
    revision: KnowledgeRevision,
    *,
    content_json: str | None = None,
) -> None:
    KnowledgeRepository(store.database_path, runtime_settings=store.runtime_settings)
    now = time.time()
    with store._conn() as conn:
        conn.execute(
            """INSERT INTO operational_knowledge
               (knowledge_id, tenant_id, kind, proposition_key, current_revision,
                status, created_at, updated_at)
               VALUES (?, ?, 'signal_mapping', ?, ?, 'active', ?, ?)""",
            (
                revision.knowledge_id,
                revision.tenant_id,
                f"proposition:{revision.knowledge_id}",
                revision.revision,
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
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                revision.knowledge_id,
                revision.tenant_id,
                revision.revision,
                revision.parent_revision,
                revision.schema_version,
                f"proposition:{revision.knowledge_id}",
                revision.scope.model_dump_json(),
                revision.state.review_state.value,
                revision.state.lifecycle_status.value,
                revision.state.eligibility.value,
                revision.corroboration_snapshot_ref,
                revision.policy_id,
                revision.policy_version,
                revision.revision_reason,
                content_json or revision.model_dump_json(),
                revision.semantic_fingerprint,
                now,
            ),
        )
        conn.execute("""UPDATE signal_tenant_migration_metadata SET value='dirty:s1-authority-test'
               WHERE key='governed_projection_audit_v2'""")


@pytest.mark.parametrize(
    ("endpoint", "response_key", "table", "key_column", "recorder"),
    (
        (
            "/api/v1/learn/dashboards",
            "dashboards",
            "ingested_dashboards",
            "dashboard_uid",
            _record_dashboard,
        ),
        (
            "/api/v1/learn/alerts",
            "alerts",
            "ingested_alerts",
            "alert_uid",
            _record_alert,
        ),
    ),
)
def test_learning_timestamp_cursors_round_trip_every_finite_sign(
    tmp_path: Path,
    endpoint: str,
    response_key: str,
    table: str,
    key_column: str,
    recorder: Callable[[SignalStore, str], None],
) -> None:
    client, store = _learning_client(tmp_path, table)
    rows = (
        ("positive", _SQLITE_MAX_ID, 1.0),
        ("zero", 0, 0.0),
        ("negative", -1, -1.0),
        ("minimum", _SQLITE_MIN_ID, -2.0),
    )
    for key, _row_id, _created_at in rows:
        recorder(store, key)
    with store._conn() as conn:
        for key, row_id, created_at in rows:
            conn.execute(
                f"UPDATE {table} SET id=?, created_at=? WHERE tenant_id='tenant-a' AND {key_column}=?",
                (row_id, created_at, key),
            )

    params: dict[str, int | float] = {"limit": 1}
    observed: list[tuple[int, float]] = []
    while True:
        response = client.get(endpoint, params=params)
        assert response.status_code == 200, response.text
        body = response.json()
        if not body[response_key]:
            break
        observed.append((int(body[response_key][0]["id"]), float(body[response_key][0]["created_at"])))
        params = {"limit": 1, **body["next_cursor"]}

    assert observed == [(row_id, created_at) for _key, row_id, created_at in rows]


@pytest.mark.parametrize("timestamp", (math.nan, math.inf, -math.inf, 10**400))
@pytest.mark.parametrize("method_name", ("list_ingested_dashboards", "list_ingested_alerts"))
def test_learning_store_rejects_nonfinite_timestamp_cursors(
    tmp_path: Path,
    method_name: str,
    timestamp: float | int,
) -> None:
    store = SignalStore(
        tmp_path / f"{method_name}.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    with pytest.raises(ValueError, match="finite"):
        getattr(store, method_name)(
            tenant_id="tenant-a",
            before_created_at=timestamp,
            before_id=0,
        )


@pytest.mark.parametrize("endpoint", ("/api/v1/learn/dashboards", "/api/v1/learn/alerts"))
@pytest.mark.parametrize("timestamp", ("nan", "inf", "-inf", "1" + ("0" * 400)))
def test_learning_api_rejects_nonfinite_timestamp_cursors(
    tmp_path: Path,
    endpoint: str,
    timestamp: str,
) -> None:
    cursor_case = "overflow" if len(timestamp) > 32 else timestamp.replace("-", "negative-")
    client, _store = _learning_client(
        tmp_path,
        f"nonfinite-{endpoint.rsplit('/', 1)[-1]}-{cursor_case}",
    )

    response = client.get(
        endpoint,
        params={"before_created_at": timestamp, "before_id": 0},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "before_created_at must be finite"


@pytest.mark.parametrize(
    "cursor",
    (
        encode_cursor(10**400, 0),
        encode_cursor(1.0, _SQLITE_MAX_ID + 1),
        encode_cursor(1.0, _SQLITE_MIN_ID - 1),
    ),
)
@pytest.mark.parametrize("endpoint", ("/api/v1/learn/runbooks", "/api/v1/learn/incidents"))
def test_artifact_learning_apis_reject_overflowing_opaque_cursors(
    tmp_path: Path,
    endpoint: str,
    cursor: str,
) -> None:
    client, _store = _learning_client(tmp_path, f"artifact-cursor-{endpoint.rsplit('/', 1)[-1]}")

    response = client.get(endpoint, params={"cursor": cursor})

    assert response.status_code == 400
    assert response.json()["detail"] == "invalid artifact cursor"


@pytest.mark.parametrize(
    ("source", "table", "index_name"),
    (
        ("dashboard", "ingested_dashboards", "idx_ingested_dashboard_page"),
        ("alert", "ingested_alerts", "idx_ingested_alert_page"),
    ),
)
def test_learning_continuation_uses_an_indexed_row_value_seek(
    tmp_path: Path,
    source: str,
    table: str,
    index_name: str,
) -> None:
    store = SignalStore(tmp_path / f"{source}-continuation-plan.db")
    sql, params = signal_store_module._ingested_source_page_statement(
        source,
        tenant_id="default",
        status=None,
        backend_name=None,
        cursor=(0.0, 0),
        limit=50,
        offset=0,
    )

    with store._conn() as conn:
        plan = conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()

    details = [str(row["detail"]) for row in plan]
    assert any(f"SEARCH {table} USING INDEX {index_name}" in detail for detail in details)
    assert not any(detail == f"SCAN {table}" for detail in details)
    assert not any("TEMP B-TREE" in detail for detail in details)
    assert "(created_at, id) < (?, ?)" in sql
    assert " OR " not in sql


def test_empty_artifact_id_is_preserved_in_direct_and_batch_summaries(tmp_path: Path) -> None:
    store = SignalStore(tmp_path / "empty-artifact-summary.db")
    store.record_learned_artifact(
        artifact_id="",
        artifact_type="runbook",
        fingerprint="empty-artifact",
    )
    store.replace_artifact_extractions(
        artifact_id="",
        evidence_requirements=[{"id": "empty-artifact-requirement", "subject": "checkout"}],
    )

    batch = store.artifact_extraction_counts_batch(["", "missing", ""])

    assert set(batch) == {"", "missing"}
    assert batch[""]["evidence_requirements"] == 1
    assert batch["missing"] == {
        "evidence_requirements": 0,
        "ownership_hints": 0,
        "dependency_hints": 0,
        "signal_mapping_candidates": 0,
    }
    assert store.artifact_extraction_counts("") == batch[""]


@pytest.mark.parametrize(
    ("artifact_type", "endpoint", "response_key"),
    (
        ("runbook", "/api/v1/learn/runbooks", "runbooks"),
        ("incident", "/api/v1/learn/incidents", "incidents"),
    ),
)
def test_empty_artifact_id_is_preserved_in_learning_summary_apis(
    tmp_path: Path,
    artifact_type: str,
    endpoint: str,
    response_key: str,
) -> None:
    client, store = _learning_client(tmp_path, f"empty-{artifact_type}")
    store.record_learned_artifact(
        tenant_id="tenant-a",
        artifact_id="",
        artifact_type=artifact_type,
        fingerprint=f"empty-{artifact_type}",
    )
    store.replace_artifact_extractions(
        tenant_id="tenant-a",
        artifact_id="",
        dependency_hints=[
            {
                "id": f"empty-{artifact_type}-dependency",
                "source_entity": "checkout",
                "target_entity": "redis",
            }
        ],
    )

    response = client.get(endpoint)

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["count"] == 1
    assert body[response_key][0]["artifact_id"] == ""
    assert body[response_key][0]["extraction_counts"]["dependency_hints"] == 1


@pytest.mark.parametrize(
    ("artifact_type", "endpoint", "response_key"),
    (
        ("runbook", "/api/v1/learn/runbooks", "runbooks"),
        ("incident", "/api/v1/learn/incidents", "incidents"),
    ),
)
def test_artifact_summary_page_and_counts_share_one_read_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_type: str,
    endpoint: str,
    response_key: str,
) -> None:
    client, store = _learning_client(tmp_path, f"snapshot-{artifact_type}")
    writer = SignalStore(store.database_path, runtime_settings=store.runtime_settings)
    store.record_learned_artifact(
        tenant_id="tenant-a",
        artifact_id="",
        artifact_type=artifact_type,
        fingerprint=f"snapshot-{artifact_type}",
    )
    store.replace_artifact_extractions(
        tenant_id="tenant-a",
        artifact_id="",
        dependency_hints=[{"id": "before", "source_entity": "checkout", "target_entity": "redis"}],
    )
    original_counts = store.artifact_extraction_counts_batch
    replaced = False

    def replace_between_page_and_counts(artifact_ids: list[str], *, tenant_id: str | None = None):
        nonlocal replaced
        if not replaced:
            replaced = True
            writer.replace_artifact_extractions(
                tenant_id="tenant-a",
                artifact_id="",
                dependency_hints=[
                    {"id": "after-a", "source_entity": "checkout", "target_entity": "redis"},
                    {"id": "after-b", "source_entity": "checkout", "target_entity": "postgres"},
                ],
            )
        return original_counts(artifact_ids, tenant_id=tenant_id)

    monkeypatch.setattr(store, "artifact_extraction_counts_batch", replace_between_page_and_counts)

    response = client.get(endpoint)

    assert response.status_code == 200, response.text
    assert response.json()[response_key][0]["extraction_counts"]["dependency_hints"] == 1
    assert writer.artifact_extraction_counts("", tenant_id="tenant-a")["dependency_hints"] == 2


@pytest.mark.parametrize(
    ("endpoint", "module_name", "callable_name", "reason_code"),
    (
        (
            "/api/v1/learn/grafana?limit=1",
            "tacit.dashboard_ingest",
            "learn_backend_dashboards",
            "dashboard_learning_item_failed",
        ),
        (
            "/api/v1/learn/backends/grafana/alerts?limit=1",
            "tacit.alert_ingest",
            "learn_backend_alerts",
            "alert_learning_item_failed",
        ),
    ),
)
def test_bulk_learning_api_redacts_per_item_failure_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
    module_name: str,
    callable_name: str,
    reason_code: str,
) -> None:
    tenant_canary = "PRIVATE-BULK-TENANT-CANARY"
    path_canary = str(tmp_path / "PRIVATE-BULK-PATH-CANARY" / "signals.db")
    client, _store = _learning_client(tmp_path, f"bulk-redaction-{callable_name}")

    async def return_failure(**_kwargs):
        return {
            "failures": [
                {
                    "title": "Visible source title",
                    "error": f"tenant={tenant_canary} database={path_canary}",
                    "traceback": f"traceback tenant={tenant_canary}",
                    "database_path": path_canary,
                    "tenant_id": tenant_canary,
                },
                f"raw failure tenant={tenant_canary} database={path_canary}",
            ]
        }

    module = __import__(module_name, fromlist=[callable_name])
    monkeypatch.setattr(module, callable_name, return_failure)

    response = client.post(endpoint)

    assert response.status_code == 200, response.text
    rendered = response.text
    assert tenant_canary not in rendered
    assert path_canary not in rendered
    failure = response.json()["failures"][0]
    assert failure["error"] == "Learning item failed."
    assert failure["reason_code"] == reason_code
    assert len(failure["error_fingerprint"]) == 16
    assert len(failure["item_fingerprint"]) == 16
    assert set(failure) == {"error", "reason_code", "error_fingerprint", "item_fingerprint"}
    raw_failure = response.json()["failures"][1]
    assert raw_failure["error"] == "Learning item failed."
    assert raw_failure["reason_code"] == reason_code
    assert len(raw_failure["error_fingerprint"]) == 16
    assert set(raw_failure) == {"error", "reason_code", "error_fingerprint"}


def test_governed_projection_write_rejects_oversized_authority_before_mutation(tmp_path: Path) -> None:
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        signal_resolution_mapping_limit=10,
    )
    store = SignalStore(tmp_path / "projection-write-limit.db", runtime_settings=runtime_settings)
    knowledge_id = "knowledge:bounded-projection"
    store.add_mapping(
        "s1_audit_signal",
        "existing_metric",
        source_type="operational_knowledge",
        source_refs=[f"{knowledge_id}@1"],
        governance_ref=knowledge_id,
        governance_revision=1,
        review_state="approved",
    )
    oversized = _signal_mapping_revision(
        knowledge_id,
        [{"metric_pattern": f"metric_{index}"} for index in range(11)],
    )

    with store.transaction() as conn:
        with pytest.raises(RuntimeError, match="resolver mapping limit"):
            store.sync_governed_revision(oversized, connection=conn, allow_dirty=True)

    with store._conn() as conn:
        rows = conn.execute(
            """SELECT metric_pattern, review_state FROM signal_metric_mappings
               WHERE governance_ref=? ORDER BY id""",
            (knowledge_id,),
        ).fetchall()
    assert [(row["metric_pattern"], row["review_state"]) for row in rows] == [("existing_metric", "approved")]


def test_governed_projection_write_rejects_aggregate_source_ref_fanout_before_mutation(tmp_path: Path) -> None:
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        signal_resolution_mapping_limit=20,
    )
    store = SignalStore(tmp_path / "projection-source-ref-work-limit.db", runtime_settings=runtime_settings)
    knowledge_id = "knowledge:bounded-source-ref-projection"
    store.add_mapping(
        "s1_audit_signal",
        "existing_metric",
        source_type="operational_knowledge",
        source_refs=[f"{knowledge_id}@1"],
        governance_ref=knowledge_id,
        governance_revision=1,
        review_state="approved",
    )
    provenance_count = signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN // 10 + 1
    oversized = _signal_mapping_revision(
        knowledge_id,
        [{"metric_pattern": f"metric_{index}"} for index in range(10)],
    ).model_copy(update={"provenance_refs": [f"source:{index}" for index in range(provenance_count)]})

    with store.transaction() as conn:
        with pytest.raises(RuntimeError, match="source-reference child work limit"):
            store.sync_governed_revision(oversized, connection=conn, allow_dirty=True)

    with store._conn() as conn:
        rows = conn.execute(
            """SELECT metric_pattern, review_state FROM signal_metric_mappings
               WHERE governance_ref=? ORDER BY id""",
            (knowledge_id,),
        ).fetchall()
    assert [(row["metric_pattern"], row["review_state"]) for row in rows] == [("existing_metric", "approved")]


@pytest.mark.parametrize(
    "malformed_mapping",
    (
        "not-an-object",
        {"metric_pattern": ""},
        {"metric_pattern": "metric", "context_datasource_types": "prometheus"},
        {"metric_pattern": "metric", "confidence": float("nan")},
    ),
)
def test_governed_projection_write_rejects_malformed_mapping_before_mutation(
    tmp_path: Path,
    malformed_mapping: object,
) -> None:
    store = SignalStore(tmp_path / "projection-write-shape.db")
    revision = _signal_mapping_revision("knowledge:malformed-write", [malformed_mapping])

    with store.transaction() as conn:
        with pytest.raises(RuntimeError, match="invalid resolver mapping"):
            store.sync_governed_revision(revision, connection=conn, allow_dirty=True)

    with store._conn() as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM signal_metric_mappings WHERE governance_ref=?",
                (revision.knowledge_id,),
            ).fetchone()
            is None
        )


def test_add_mapping_rejects_source_ref_fanout_before_opening_write_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SignalStore(tmp_path / "source-ref-write-limit.db")
    write_context_opened = False

    def forbidden_write_context(_connection):
        nonlocal write_context_opened
        write_context_opened = True
        raise AssertionError("write context must not be opened for oversized source refs")

    monkeypatch.setattr(store, "_write_connection", forbidden_write_context)
    refs = [f"source:{index}" for index in range(signal_migrations.SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT + 1)]

    with pytest.raises(ValueError, match="source_refs exceeds the count limit"):
        store.add_mapping("bounded_source_signal", "bounded_source_metric", source_refs=refs)

    assert write_context_opened is False


def test_add_mapping_rejects_source_ref_bytes_before_opening_write_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SignalStore(tmp_path / "source-ref-byte-limit.db")
    write_context_opened = False

    def forbidden_write_context(_connection):
        nonlocal write_context_opened
        write_context_opened = True
        raise AssertionError("write context must not be opened for oversized source refs")

    monkeypatch.setattr(store, "_write_connection", forbidden_write_context)
    refs = ["x" * signal_migrations.SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES]

    with pytest.raises(ValueError, match="source_refs exceeds the byte limit"):
        store.add_mapping("bounded_source_signal", "bounded_source_metric", source_refs=refs)

    assert write_context_opened is False


@pytest.mark.parametrize(
    "source_refs",
    (
        lambda: [f"source:{index}" for index in range(signal_migrations.SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT + 1)],
        lambda: ["x" * signal_migrations.SIGNAL_MAPPING_SOURCE_REF_MAX_BYTES],
    ),
)
def test_source_ref_sqlite_trigger_rejects_unbounded_child_fanout(
    tmp_path: Path,
    source_refs: Callable[[], list[str]],
) -> None:
    store = SignalStore(tmp_path / "source-ref-trigger-limit.db")
    mapping_id = store.add_mapping("bounded_source_signal", "bounded_source_metric")

    with pytest.raises(sqlite3.IntegrityError, match="source_refs exceeds the storage limit"):
        with store._conn() as conn:
            conn.execute(
                "UPDATE signal_metric_mappings SET source_refs=? WHERE id=?",
                (json.dumps(source_refs()), mapping_id),
            )

    with store._conn() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM signal_mapping_source_refs WHERE mapping_id=?",
                (mapping_id,),
            ).fetchone()[0]
            == 0
        )


def test_projection_validation_uses_bounded_covering_key_seek_at_50k_rows(tmp_path: Path) -> None:
    store = SignalStore(tmp_path / "projection-audit-plan.db")
    now = time.time()
    noise_count = 50_000
    with store._conn() as conn:
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, created_at, last_seen)
               VALUES ('default', 'noise', ?, 0.9, 'operational_knowledge',
                       '[]', ?, 1, ?, 'approved', ?, ?)""",
            (
                (f"noise_metric_{index}", f"knowledge:noise:{index}", f"noise:{index}", now, now)
                for index in range(noise_count)
            ),
        )
        target = conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, context_datasource_types, created_at, last_seen)
               VALUES ('default', 'request_latency', 'target_metric', 0.9,
                       'operational_knowledge', '[]', 'knowledge:target', 7,
                       'target', 'approved', '["prometheus"]', ?, ?)
               RETURNING id""",
            (now, now),
        ).fetchone()
        assert target is not None
        target_id = int(target["id"])
        conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, context_datasource_types, created_at, last_seen)
               VALUES ('tenant-b', 'request_latency', 'cross_tenant_metric', 0.9,
                       'operational_knowledge', '[]', 'knowledge:target', 7,
                       'cross-tenant', 'approved', '["prometheus"]', ?, ?)""",
            (now, now),
        )

        initial_sql, initial_params = signal_store_module._projection_mapping_page_statement(
            tenant_id="default",
            governance_ref="knowledge:target",
            governance_revision=7,
            after_id=None,
            limit=500,
        )
        continuation_sql, continuation_params = signal_store_module._projection_mapping_page_statement(
            tenant_id="default",
            governance_ref="knowledge:target",
            governance_revision=7,
            after_id=target_id,
            limit=500,
        )
        initial_plan = conn.execute(f"EXPLAIN QUERY PLAN {initial_sql}", initial_params).fetchall()
        continuation_plan = conn.execute(f"EXPLAIN QUERY PLAN {continuation_sql}", continuation_params).fetchall()
        rows = conn.execute(initial_sql, initial_params).fetchall()

    assert [int(row["mapping_id"]) for row in rows] == [target_id]
    for plan in (initial_plan, continuation_plan):
        details = [str(row["detail"]) for row in plan]
        assert any("idx_smm_governed_revision_audit" in detail for detail in details)
        assert any("COVERING INDEX" in detail for detail in details)
        assert not any("SCAN signal_metric_mappings" in detail for detail in details)
        assert not any("TEMP B-TREE" in detail for detail in details)
    assert " OR " not in initial_sql
    assert "LIMIT ?" in initial_sql


def test_complete_projection_audit_uses_only_bounded_partial_index_pages_at_50k_rows(tmp_path: Path) -> None:
    store = SignalStore(tmp_path / "complete-projection-audit-plan.db")
    now = time.time()
    with store._conn() as conn:
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, review_state, created_at, last_seen)
               VALUES ('*bootstrap*', 'noise', ?, 0.9, 'bootstrap', '[]',
                       'trusted', ?, ?)""",
            ((f"bootstrap_noise_{index}", now, now) for index in range(50_000)),
        )
        governed = conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, created_at, last_seen)
               VALUES ('default', 'request_latency', 'governed_target', 0.9,
                       'operational_knowledge', '[]', 'knowledge:governed-target', 3,
                       'governed-target', 'approved', ?, ?)
               RETURNING id""",
            (now, now),
        ).fetchone()
        ungoverned = conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, review_state, created_at, last_seen)
               VALUES ('default', 'request_latency', 'ungoverned_target', 0.9,
                       'teach', '[]', '', 'trusted', ?, ?)
               RETURNING id""",
            (now, now),
        ).fetchone()
        assert governed is not None and ungoverned is not None
        governed_id = int(governed["id"])
        ungoverned_id = int(ungoverned["id"])

        governed_sql, governed_params = signal_store_module._projection_audit_key_page_statement(
            "governed",
            after=None,
            limit=500,
        )
        governed_continuation_sql, governed_continuation_params = (
            signal_store_module._projection_audit_key_page_statement(
                "governed",
                after=("default", "knowledge:governed-target", 3, governed_id),
                limit=500,
            )
        )
        ungoverned_sql, ungoverned_params = signal_store_module._projection_audit_key_page_statement(
            "ungoverned",
            after=None,
            limit=500,
        )
        ungoverned_continuation_sql, ungoverned_continuation_params = (
            signal_store_module._projection_audit_key_page_statement(
                "ungoverned",
                after=("default", ungoverned_id),
                limit=500,
            )
        )
        statements = (
            (governed_sql, governed_params, "idx_smm_governed_revision_audit"),
            (governed_continuation_sql, governed_continuation_params, "idx_smm_governed_revision_audit"),
            (ungoverned_sql, ungoverned_params, "idx_smm_ungoverned_audit"),
            (ungoverned_continuation_sql, ungoverned_continuation_params, "idx_smm_ungoverned_audit"),
        )
        plans = [
            (conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall(), index_name)
            for sql, params, index_name in statements
        ]
        governed_rows = conn.execute(governed_sql, governed_params).fetchall()
        ungoverned_rows = conn.execute(ungoverned_sql, ungoverned_params).fetchall()
        hydration_sql, hydration_params = signal_store_module._projection_audit_mapping_rows_statement(
            [governed_id, ungoverned_id]
        )
        hydration_plan = conn.execute(f"EXPLAIN QUERY PLAN {hydration_sql}", hydration_params).fetchall()

    assert [int(row["mapping_id"]) for row in governed_rows] == [governed_id]
    assert [int(row["mapping_id"]) for row in ungoverned_rows] == [ungoverned_id]
    for plan, index_name in plans:
        details = [str(row["detail"]) for row in plan]
        assert any(index_name in detail and "COVERING INDEX" in detail for detail in details)
        assert not any(detail in {"SCAN signal_metric_mappings", "SCAN mapping"} for detail in details)
        assert not any("TEMP B-TREE" in detail for detail in details)
    hydration_details = [str(row["detail"]) for row in hydration_plan]
    assert any("USING INTEGER PRIMARY KEY" in detail for detail in hydration_details)
    assert not any(detail in {"SCAN signal_metric_mappings", "SCAN mapping"} for detail in hydration_details)
    assert not any("TEMP B-TREE" in detail for detail in hydration_details)
    assert all(" OR " not in sql for sql, _params, _index in statements)


def test_projection_authority_validation_uses_one_indexed_join_page_at_50k_rows(tmp_path: Path) -> None:
    store = SignalStore(tmp_path / "projection-authority-join-plan.db")
    revision = _signal_mapping_revision(
        "knowledge:authority-target",
        [{"metric_pattern": "authority_target_metric", "context_datasource_types": ["prometheus"]}],
    )
    _insert_active_signal_authority(store, revision)
    now = time.time()
    with store._conn() as conn:
        target_id = conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, context_datasource_types, created_at, last_seen)
               VALUES ('default', 's1_audit_signal', 'authority_target_metric', 0.9,
                       'operational_knowledge', '[]', ?, 1, 'target', 'approved',
                       '["prometheus"]', ?, ?)
               RETURNING id""",
            (revision.knowledge_id, now, now),
        ).fetchone()["id"]
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, created_at, last_seen)
               VALUES ('tenant-z', 'noise', ?, 0.9, 'operational_knowledge',
                       '[]', ?, 1, ?, 'approved', ?, ?)""",
            (
                (f"noise_metric_{index}", f"knowledge:noise:{index}", f"noise:{index}", now, now)
                for index in range(50_000)
            ),
        )
        sql, params = signal_store_module._projection_authority_audit_page_statement(
            after=None,
            limit=500,
        )
        plan = conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()
        rows = conn.execute(sql, params).fetchall()

    target = next(row for row in rows if int(row["mapping_id"]) == int(target_id))
    assert int(target["authority_active"]) == 1
    details = [str(row["detail"]) for row in plan]
    assert any(
        "idx_smm_governed_revision_audit" in detail and "COVERING INDEX" in detail for detail in details
    ), details
    assert any("sqlite_autoindex_operational_knowledge_" in detail for detail in details), details
    assert any("sqlite_autoindex_operational_knowledge_revisions_" in detail for detail in details), details
    assert not any("SCAN signal_metric_mappings" in detail for detail in details), details
    assert not any("TEMP B-TREE" in detail for detail in details), details


def test_active_projection_authority_pages_use_the_exact_bounded_plan_at_50k_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-authority-page-plan.db"
    store = SignalStore(db_path)
    first_revision = _signal_mapping_revision(
        "knowledge:authority-page-a",
        [{"metric_pattern": "authority_page_a"}],
    )
    second_revision = _signal_mapping_revision(
        "knowledge:authority-page-b",
        [{"metric_pattern": "authority_page_b"}],
    )
    _insert_active_signal_authority(store, first_revision)
    _insert_active_signal_authority(store, second_revision)
    store = SignalStore(db_path)
    with store._conn() as conn:
        conn.executemany(
            """INSERT INTO operational_knowledge
               (knowledge_id, tenant_id, kind, proposition_key, current_revision,
                status, created_at, updated_at)
               VALUES (?, ?, 'ownership', ?, 1, 'active', 1, 1)""",
            ((f"knowledge:noise:{index}", f"tenant:{index}", f"proposition:noise:{index}") for index in range(50_000)),
        )
        initial_sql, initial_params = signal_store_module._active_projection_authority_page_statement(
            after=None,
            limit=1,
        )
        initial_plan = conn.execute(f"EXPLAIN QUERY PLAN {initial_sql}", initial_params).fetchall()
        initial_rows = conn.execute(initial_sql, initial_params).fetchall()
        first_key = (str(initial_rows[0]["tenant_id"]), str(initial_rows[0]["knowledge_id"]))
        continuation_sql, continuation_params = signal_store_module._active_projection_authority_page_statement(
            after=first_key,
            limit=1,
        )
        continuation_plan = conn.execute(f"EXPLAIN QUERY PLAN {continuation_sql}", continuation_params).fetchall()
        continuation_rows = conn.execute(continuation_sql, continuation_params).fetchall()

    assert len(initial_rows) == 1
    assert len(continuation_rows) == 1
    for plan in (initial_plan, continuation_plan):
        details = [str(row["detail"]) for row in plan]
        assert any(
            "SEARCH current USING COVERING INDEX idx_operational_knowledge_signal_projection_page" in detail
            for detail in details
        ), details
        assert not any(detail == "SCAN current" for detail in details), details
        assert not any("TEMP B-TREE" in detail for detail in details), details


def test_projection_quarantine_does_not_issue_per_mapping_authority_queries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = SignalStore(tmp_path / "projection-authority-no-n-plus-one.db")
    revision = _signal_mapping_revision(
        "knowledge:no-n-plus-one",
        [{"metric_pattern": "no_n_plus_one_metric"}],
    )
    _insert_active_signal_authority(store, revision)
    with store._conn() as conn:
        conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, created_at, last_seen)
               VALUES ('default', 's1_audit_signal', 'no_n_plus_one_metric', 0.9,
                       'operational_knowledge', '[]', ?, 1, '', 'approved', ?, ?)""",
            (revision.knowledge_id, time.time(), time.time()),
        )

    def forbidden_point_query(*_args, **_kwargs):
        raise AssertionError("projection audit must batch authority validation")

    monkeypatch.setattr(store, "_projection_authority_is_active", forbidden_point_query)
    assert store._quarantine_projection_batches() == (0, 0)


def test_signal_store_replaces_an_existing_noncovering_projection_audit_index(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-audit-index-upgrade.db"
    store = SignalStore(db_path)
    with store._conn() as conn:
        conn.execute("DROP INDEX idx_smm_governed_revision_audit")
        conn.execute("""CREATE INDEX idx_smm_governed_revision_audit
               ON signal_metric_mappings(
                   tenant_id, governance_ref, governance_revision, id,
                   signal_type, metric_pattern, context_datasource_types
               )
               WHERE governance_ref != '' AND review_state IN ('approved', 'trusted')""")

    reopened = SignalStore(db_path)
    with reopened._conn() as conn:
        columns = tuple(
            str(row["name"]) for row in conn.execute("PRAGMA index_info(idx_smm_governed_revision_audit)").fetchall()
        )
        sql, params = signal_store_module._projection_mapping_page_statement(
            tenant_id="default",
            governance_ref="knowledge:target",
            governance_revision=1,
            after_id=None,
            limit=500,
        )
        plan = conn.execute(f"EXPLAIN QUERY PLAN {sql}", params).fetchall()

    assert columns == (
        "tenant_id",
        "governance_ref",
        "governance_revision",
        "id",
        "signal_type",
        "metric_pattern",
        "context_datasource_types",
        "review_state",
    )
    assert any("USING COVERING INDEX idx_smm_governed_revision_audit" in str(row["detail"]) for row in plan)


def test_projection_validation_rejects_authority_over_the_mapping_work_budget(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-audit-work-budget.db"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="default")
    store = SignalStore(db_path, runtime_settings=runtime_settings)
    KnowledgeRepository(db_path, runtime_settings=runtime_settings)
    mapping_limit = int(runtime_settings.signal_resolution_mapping_limit)
    mappings = [
        {
            "metric_pattern": f"bounded_metric_{index}",
            "context_datasource_types": ["prometheus"],
        }
        for index in range(mapping_limit + 1)
    ]
    now = time.time()
    content = json.dumps(
        {
            "proposition": {"concept_ref": "signal:bounded_signal"},
            "resolver_payload": {"mappings": mappings},
        }
    )
    with store._conn() as conn:
        conn.execute(
            """INSERT INTO operational_knowledge
               (knowledge_id, tenant_id, kind, proposition_key, current_revision,
                status, created_at, updated_at)
               VALUES ('knowledge:oversized', 'default', 'signal_mapping',
                       'proposition:oversized', 1, 'active', ?, ?)""",
            (now, now),
        )
        conn.execute(
            """INSERT INTO operational_knowledge_revisions
               (knowledge_id, tenant_id, revision, parent_revision, schema_version,
                proposition_key, scope_json, review_state, lifecycle_status,
                eligibility, corroboration_snapshot_ref, policy_id, policy_version,
                revision_reason, content_json, semantic_fingerprint, created_at)
               VALUES ('knowledge:oversized', 'default', 1, NULL, '1.0',
                       'proposition:oversized', '{}', 'approved', 'active',
                       'live_verified', 'snapshot:test', 'test-policy', '1',
                       'promoted', ?, 'semantic:oversized', ?)""",
            (content, now),
        )
        conn.execute("""UPDATE signal_tenant_migration_metadata SET value='dirty:work-budget-test'
               WHERE key='governed_projection_audit_v2'""")

    with pytest.raises(RuntimeError, match="resolver mapping limit"):
        store._validated_projection_audit_token()


@pytest.mark.parametrize(
    "malformed_mapping",
    (
        "not-an-object",
        {"metric_pattern": ""},
        {"metric_pattern": "valid_metric", "context_datasource_types": "prometheus"},
    ),
)
def test_projection_validation_rejects_every_malformed_authority_mapping(
    tmp_path: Path,
    malformed_mapping: object,
) -> None:
    store = SignalStore(tmp_path / f"malformed-authority-{type(malformed_mapping).__name__}.db")
    revision = _signal_mapping_revision(
        "knowledge:malformed-authority",
        [
            {"metric_pattern": "valid_metric", "context_datasource_types": ["prometheus"]},
            malformed_mapping,
        ],
    )
    _insert_active_signal_authority(store, revision)

    with pytest.raises(RuntimeError, match="invalid resolver mapping"):
        store._validated_projection_audit_token()


def test_projection_repair_rejects_oversized_authority_before_any_mapping_write(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-repair-work-budget.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        signal_resolution_mapping_limit=10,
    )
    store = SignalStore(db_path, runtime_settings=runtime_settings)
    revision = _signal_mapping_revision(
        "knowledge:oversized-repair",
        [{"metric_pattern": f"oversized_metric_{index}"} for index in range(11)],
    )
    _insert_active_signal_authority(store, revision)

    with pytest.raises(RuntimeError, match="resolver mapping limit"):
        SignalStore(db_path, runtime_settings=runtime_settings)

    with sqlite3.connect(db_path) as conn:
        projection_count = conn.execute(
            "SELECT COUNT(*) FROM signal_metric_mappings WHERE governance_ref=?",
            (revision.knowledge_id,),
        ).fetchone()[0]
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='governed_projection_audit_v2'"
        ).fetchone()[0]
    assert projection_count == 0
    assert str(marker).startswith("dirty:")


def test_projection_repair_rejects_oversized_serialized_authority_before_mapping_write(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-repair-payload-budget.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        signal_resolution_mapping_limit=10,
    )
    store = SignalStore(db_path, runtime_settings=runtime_settings)
    revision = _signal_mapping_revision(
        "knowledge:oversized-payload",
        [{"metric_pattern": "bounded_metric"}],
    )
    content = revision.model_dump(mode="json")
    payload_limit = signal_store_module._projection_authority_payload_byte_limit(10)
    content["padding"] = "x" * payload_limit
    _insert_active_signal_authority(store, revision, content_json=json.dumps(content))

    with pytest.raises(RuntimeError, match="payload byte limit"):
        SignalStore(db_path, runtime_settings=runtime_settings)

    with sqlite3.connect(db_path) as conn:
        projection_count = conn.execute(
            "SELECT COUNT(*) FROM signal_metric_mappings WHERE governance_ref=?",
            (revision.knowledge_id,),
        ).fetchone()[0]
    assert projection_count == 0


def test_projection_authority_rejects_oversized_json_before_selecting_content(tmp_path: Path) -> None:
    db_path = tmp_path / "projection-authority-payload-preflight.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        signal_resolution_mapping_limit=10,
    )
    store = SignalStore(db_path, runtime_settings=runtime_settings)
    revision = _signal_mapping_revision(
        "knowledge:oversized-preflight",
        [{"metric_pattern": "bounded_metric"}],
    )
    content = revision.model_dump(mode="json")
    payload_limit = signal_store_module._projection_authority_payload_byte_limit(10)
    content["padding"] = "x" * payload_limit
    _insert_active_signal_authority(store, revision, content_json=json.dumps(content))

    traced_statements: list[str] = []
    with store._conn() as conn:
        conn.set_trace_callback(traced_statements.append)
        with pytest.raises(RuntimeError, match="payload byte limit"):
            store._prepare_projection_authority(
                conn,
                tenant_id=revision.tenant_id,
                knowledge_id=revision.knowledge_id,
                revision_number=revision.revision,
            )

    content_selects = [
        statement
        for statement in traced_statements
        if "select revision.content_json" in " ".join(statement.casefold().split())
    ]
    assert content_selects == []


def test_signal_diagnostics_redact_tenant_and_path_on_success_and_failure(tmp_path: Path) -> None:
    tenant_canary = "PRIVATE-TENANT-DIAGNOSTIC-CANARY"
    path_canary = "PRIVATE-PATH-DIAGNOSTIC-CANARY"
    db_path = tmp_path / path_canary / "signals.db"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id=tenant_canary,
        signal_resolution_mapping_limit=10,
    )

    with capture_logs() as init_logs:
        store = SignalStore(db_path, runtime_settings=runtime_settings)
    for index in range(11):
        signal_type = f"diagnostic_signal_{index}"
        store.register_signal_type(signal_type, tenant_id=tenant_canary)
        store.add_mapping(
            signal_type,
            "shared_diagnostic_metric",
            tenant_id=tenant_canary,
            confidence=0.9,
        )
    catalog = [
        MetricEntry(
            name="shared_diagnostic_metric",
            datasource_uid="prometheus",
            datasource_name="Prometheus",
            datasource_type="prometheus",
            query_language="promql",
        )
    ]

    with capture_logs() as failure_logs, pytest.raises(RuntimeError, match="active mapping candidates"):
        store.resolve_metric_signal_details(catalog, tenant_id=tenant_canary)

    rendered = repr([*init_logs, *failure_logs])
    assert tenant_canary not in rendered
    assert path_canary not in rendered
    assert str(db_path) not in rendered
    initialized = [entry for entry in init_logs if entry.get("event") == "signal_store_init"]
    assert initialized
    assert all(entry.get("reason_code") == "signal_store_initialized" for entry in initialized)
    failures = [entry for entry in failure_logs if entry.get("event") == "signal_reverse_resolution_limit_exceeded"]
    assert len(failures) == 1
    assert failures[0]["reason_code"] == "signal_reverse_resolution_limit_exceeded"
    assert len(str(failures[0]["tenant_fingerprint"])) == 16
    assert "tenant_id" not in failures[0]
    assert failures[0]["matching_mapping_count"] == 11
    assert failures[0]["resolution_limit"] == 10


def test_signal_migration_diagnostics_use_safe_codes_fingerprints_and_counters(tmp_path: Path) -> None:
    tenant_canary = "PRIVATE-MIGRATION-TENANT-CANARY"
    path_canary = "PRIVATE-MIGRATION-PATH-CANARY"
    db_path = tmp_path / path_canary / "signals.db"
    db_path.parent.mkdir(parents=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            INSERT INTO signal_types VALUES
                ('private_migration_signal', 'private', 'custom', 'count', 1, 1);
            """)

    with capture_logs() as logs:
        SignalStore(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_canary),
        )

    rendered = repr(logs)
    assert tenant_canary not in rendered
    assert path_canary not in rendered
    assert str(db_path) not in rendered
    batches = [entry for entry in logs if entry.get("event") == "signal_legacy_schema_migration_batch"]
    assert batches
    assert all(entry.get("reason_code") == "signal_legacy_schema_migration_batch" for entry in batches)
    assert all(0 < int(entry["rows"]) <= 500 for entry in batches)
    completed = [entry for entry in logs if entry.get("event") == "signal_legacy_schema_migration_complete"]
    assert completed
    assert all(entry.get("reason_code") == "signal_legacy_schema_migration_complete" for entry in completed)
    assert all(len(str(entry["database_fingerprint"])) == 16 for entry in completed)
