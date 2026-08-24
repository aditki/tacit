from __future__ import annotations

import json
import sqlite3

import pytest

from tacit.config import Settings
from tacit.signals import SignalStore
from tacit.signals import migrations as signal_migrations
from tacit.signals.migrations import (
    CURRENT_SIGNAL_SCHEMA_MARKER,
    ensure_schema,
    mark_governed_projection_audit_current,
    reconcile_default_tenant_owner_batch,
    reconcile_legacy_signal_schema_batch,
    reconcile_mapping_source_ref_index_batch,
    signal_schema_is_current,
)
from tacit.signals.schema import GLOBAL_BOOTSTRAP_TENANT_ID, SCHEMA_SQL


def _seed_legacy_mapping_schema(path, *, rows: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_type TEXT NOT NULL,
                metric_pattern TEXT NOT NULL,
                confidence REAL NOT NULL DEFAULT 0.5,
                context_services TEXT NOT NULL DEFAULT '[]',
                context_datasource_types TEXT NOT NULL DEFAULT '[]',
                context_environments TEXT NOT NULL DEFAULT '[]',
                context_archetypes TEXT NOT NULL DEFAULT '[]',
                source_type TEXT NOT NULL DEFAULT 'teach',
                source_refs TEXT NOT NULL DEFAULT '[]',
                inference_version TEXT NOT NULL DEFAULT '',
                review_state TEXT NOT NULL DEFAULT 'trusted',
                use_count INTEGER NOT NULL DEFAULT 0,
                positive_feedback INTEGER NOT NULL DEFAULT 0,
                negative_feedback INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                last_seen REAL NOT NULL,
                UNIQUE(signal_type, metric_pattern)
            );
        """)
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (signal_type, metric_pattern, created_at, last_seen)
               VALUES ('request_latency', ?, 1, 1)""",
            [(f"legacy_metric_{index}",) for index in range(rows)],
        )


def test_startup_reinstalls_same_named_projection_indexes_and_triggers_with_drifted_sql(tmp_path) -> None:
    db_path = tmp_path / "drifted-projection-objects.db"
    store = SignalStore(db_path)
    with store._conn() as conn:
        conn.execute("DROP INDEX idx_smm_governed_revision_audit")
        conn.execute("""CREATE INDEX idx_smm_governed_revision_audit
               ON signal_metric_mappings(
                   tenant_id, governance_ref, governance_revision, id,
                   signal_type, metric_pattern, context_datasource_types, review_state
               )
               WHERE governance_ref != '' AND governance_revision > 7
                 AND review_state IN ('approved', 'trusted')""")
        conn.execute("DROP TRIGGER trg_governed_mapping_insert_audit_dirty")
        conn.execute("""CREATE TRIGGER trg_governed_mapping_insert_audit_dirty
               AFTER INSERT ON signal_metric_mappings BEGIN SELECT 1; END""")
        conn.execute("DROP TRIGGER trg_signal_mapping_source_ref_insert")
        conn.execute("""CREATE TRIGGER trg_signal_mapping_source_ref_insert
               AFTER INSERT ON signal_metric_mappings BEGIN SELECT 1; END""")
        mark_governed_projection_audit_current(conn)
        assert not signal_schema_is_current(conn)

    reopened = SignalStore(db_path)
    with reopened._conn() as conn:
        assert signal_schema_is_current(conn)
        index_sql = str(
            conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_smm_governed_revision_audit'"
            ).fetchone()["sql"]
        )
        assert "governance_revision > 7" not in index_sql
        mark_governed_projection_audit_current(conn)
        mapping_id = conn.execute("""INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, governance_ref, governance_revision, projection_key,
                review_state, created_at, last_seen)
               VALUES ('default', 'request_latency', 'trigger_canary', 0.9,
                       'operational_knowledge', '["source:canary"]',
                       'knowledge:trigger-canary', 1, 'trigger-canary',
                       'approved', 1, 1)
               RETURNING id""").fetchone()["id"]
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='governed_projection_audit_v2'"
        ).fetchone()["value"]
        source_refs = conn.execute(
            "SELECT source_ref FROM signal_mapping_source_refs WHERE mapping_id=?",
            (mapping_id,),
        ).fetchall()

    assert str(marker).startswith("dirty:")
    assert [row["source_ref"] for row in source_refs] == ["source:canary"]


def test_startup_trigger_repair_rebuilds_existing_source_ref_divergence(tmp_path) -> None:
    db_path = tmp_path / "drifted-source-ref-projection.db"
    store = SignalStore(db_path)
    mapping_id = store.add_mapping(
        "request_latency",
        "drifted_source_ref_metric",
        source_refs=["source:old"],
    )
    with store._conn() as conn:
        conn.execute("DROP TRIGGER trg_signal_mapping_source_ref_update")
        conn.execute("""CREATE TRIGGER trg_signal_mapping_source_ref_update
               AFTER UPDATE OF tenant_id, source_refs ON signal_metric_mappings
               BEGIN SELECT 1; END""")
        conn.execute(
            "UPDATE signal_metric_mappings SET source_refs=? WHERE id=?",
            (json.dumps(["source:new"]), mapping_id),
        )
        stale_refs = conn.execute(
            "SELECT source_ref FROM signal_mapping_source_refs WHERE mapping_id=?",
            (mapping_id,),
        ).fetchall()
        assert [row["source_ref"] for row in stale_refs] == ["source:old"]

    reopened = SignalStore(db_path)
    with reopened._conn() as conn:
        refs = conn.execute(
            "SELECT source_ref FROM signal_mapping_source_refs WHERE mapping_id=?",
            (mapping_id,),
        ).fetchall()
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (signal_migrations.MAPPING_SOURCE_REF_INDEX_MARKER,),
        ).fetchone()

    assert [row["source_ref"] for row in refs] == ["source:new"]
    assert marker is not None and marker["value"] == "complete"


def _seed_legacy_signal_definitions(path, *, names: list[str]) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )""")
        conn.executemany(
            """INSERT INTO signal_types
               (signal_type, description, category, unit, created_at, updated_at)
               VALUES (?, ?, 'custom', 'count', 1, 1)""",
            [(name, f"Description for {name}") for name in names],
        )


def _seed_legacy_dashboards(path, *, rows: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE ingested_dashboards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dashboard_uid TEXT NOT NULL UNIQUE,
                dashboard_title TEXT NOT NULL DEFAULT '',
                dashboard_tags TEXT NOT NULL DEFAULT '[]',
                metrics_found TEXT NOT NULL DEFAULT '[]',
                panel_count INTEGER NOT NULL DEFAULT 0,
                row_groups TEXT NOT NULL DEFAULT '[]',
                metric_cooccurrence TEXT NOT NULL DEFAULT '{}',
                aggregation_patterns TEXT NOT NULL DEFAULT '[]',
                query_transformations TEXT NOT NULL DEFAULT '[]',
                panel_titles TEXT NOT NULL DEFAULT '[]',
                alert_links TEXT NOT NULL DEFAULT '[]',
                drilldown_links TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'pending',
                signals_inferred TEXT NOT NULL DEFAULT '[]',
                archetype_generated TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                reviewed_at REAL
            );
        """)
        conn.executemany(
            """INSERT INTO ingested_dashboards
               (dashboard_uid, dashboard_title, created_at)
               VALUES (?, ?, ?)""",
            [(f"dashboard-{index}", f"Dashboard {index}", float(index + 1)) for index in range(rows)],
        )


def _seed_current_bootstrap_mappings(path, *, rows: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        signal_migrations.execute_script_statements(conn, SCHEMA_SQL)
        conn.execute(
            """INSERT INTO signal_tenant_migration_metadata (key, value, updated_at)
               VALUES (?, 'previously-current', 1)""",
            (CURRENT_SIGNAL_SCHEMA_MARKER,),
        )
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, created_at, last_seen)
               VALUES ('default', 'request_latency', ?, 0.5, 'bootstrap', '[]', 1, 1)""",
            [(f"bootstrap_metric_{index}",) for index in range(rows)],
        )


def _finish_legacy_schema_migration(path, *, tenant: str, batch_size: int) -> list[tuple[str, int]]:
    complete = False
    operations: list[tuple[str, int]] = []
    while not complete:
        with sqlite3.connect(path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, operation, copied = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant=tenant,
                batch_size=batch_size,
            )
            operations.append((operation, copied))
    return operations


def _fail_cursor_update_once(monkeypatch, *, key: str):
    original = signal_migrations._record_migration_marker

    def fail(conn: sqlite3.Connection, marker_key: str, value: str) -> None:
        if marker_key == key:
            raise RuntimeError("simulated bounded migration failure")
        original(conn, marker_key, value)

    monkeypatch.setattr(signal_migrations, "_record_migration_marker", fail)


def test_legacy_signal_definitions_use_bounded_restartable_text_keyset_copy(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "legacy-signal-definitions.db"
    names = ["", "-boundary", "0", *[f"custom_signal_{index:04d}" for index in range(1_198)]]
    assert len(names) == 1_201
    _seed_legacy_signal_definitions(db_path, names=names)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert ensure_schema(conn, legacy_tenant="tenant-a", bootstrap_signal_definitions={}) is False

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_types").fetchone()[0] == 1_201
        assert conn.execute("SELECT COUNT(*) FROM tenant_signal_types").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM signal_types_tacit_tenant_migration_v1").fetchone()[0] == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="tenant-a",
            batch_size=500,
        ) == (False, "signal_types:copy", 500)

    with monkeypatch.context() as failure_patch:
        _fail_cursor_update_once(
            failure_patch,
            key="legacy_schema_copy_cursor_v1:signal_types",
        )
        with pytest.raises(RuntimeError, match="simulated bounded migration failure"):
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("BEGIN IMMEDIATE")
                reconcile_legacy_signal_schema_batch(
                    conn,
                    legacy_tenant="tenant-a",
                    batch_size=500,
                )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_types").fetchone()[0] == 1_201
        assert conn.execute("SELECT COUNT(*) FROM tenant_signal_types").fetchone()[0] == 500

    operations: list[tuple[str, int]] = []
    for expected_count in (500, 201):
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            _, operation, copied = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=500,
            )
        operations.append((operation, copied))
        assert (operation, copied) == ("signal_types:copy", expected_count)

    with monkeypatch.context() as final_swap_patch:
        _fail_cursor_update_once(final_swap_patch, key="signal_definition_scope_v1")
        with pytest.raises(RuntimeError, match="simulated bounded migration failure"):
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("BEGIN IMMEDIATE")
                reconcile_legacy_signal_schema_batch(
                    conn,
                    legacy_tenant="tenant-a",
                    batch_size=500,
                )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_types").fetchone()[0] == 1_201
        assert conn.execute("SELECT COUNT(*) FROM tenant_signal_types").fetchone()[0] == 1_201
        assert conn.execute("SELECT COUNT(*) FROM signal_types_tacit_tenant_migration_v1").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT 1 FROM signal_tenant_migration_metadata WHERE key='signal_definition_scope_v1'"
            ).fetchone()
            is None
        )

    operations.extend(_finish_legacy_schema_migration(db_path, tenant="tenant-a", batch_size=500))
    with sqlite3.connect(db_path) as conn:
        tenant_rows = conn.execute(
            "SELECT signal_type FROM tenant_signal_types WHERE tenant_id='tenant-a' ORDER BY signal_type"
        ).fetchall()
        global_rows = conn.execute("SELECT signal_type FROM signal_types").fetchall()
        shadow = conn.execute("""SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='signal_types_tacit_tenant_migration_v1'""").fetchone()
    assert [row[0] for row in tenant_rows] == sorted(names)
    assert global_rows == []
    assert shadow is None
    assert max(copied for _, copied in operations) <= 500


def test_dashboard_lifecycle_backfill_is_shadowed_bounded_and_restartable(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "legacy-dashboard-backfill.db"
    _seed_legacy_dashboards(db_path, rows=1_201)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert ensure_schema(conn, legacy_tenant="tenant-a", bootstrap_signal_definitions={}) is False

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ingested_dashboards WHERE last_seen_at=0").fetchone()[0] == 1_201
        assert conn.execute("SELECT COUNT(*) FROM ingested_dashboards_tacit_tenant_migration_v1").fetchone()[0] == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="tenant-a",
            batch_size=500,
        ) == (False, "ingested_dashboards:copy", 500)

    with monkeypatch.context() as failure_patch:
        _fail_cursor_update_once(
            failure_patch,
            key="legacy_schema_copy_cursor_v1:ingested_dashboards",
        )
        with pytest.raises(RuntimeError, match="simulated bounded migration failure"):
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("BEGIN IMMEDIATE")
                reconcile_legacy_signal_schema_batch(
                    conn,
                    legacy_tenant="tenant-a",
                    batch_size=500,
                )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ingested_dashboards WHERE last_seen_at=0").fetchone()[0] == 1_201
        assert conn.execute("SELECT COUNT(*) FROM ingested_dashboards_tacit_tenant_migration_v1").fetchone()[0] == 500

    operations = _finish_legacy_schema_migration(db_path, tenant="tenant-a", batch_size=500)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT tenant_id, created_at, last_seen_at FROM ingested_dashboards ORDER BY id"
        ).fetchall()
    assert len(rows) == 1_201
    assert {row[0] for row in rows} == {"tenant-a"}
    assert all(row[1] == row[2] for row in rows)
    assert max(copied for _, copied in operations) <= 500


def test_bootstrap_scope_normalization_is_shadowed_bounded_and_restartable(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "legacy-bootstrap-scope.db"
    _seed_current_bootstrap_mappings(db_path, rows=1_200)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """INSERT INTO signal_metric_mappings
               (tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, created_at, last_seen)
               VALUES (?, 'request_latency', 'bootstrap_metric_0', 0.9,
                       'bootstrap', '[]', 2, 2)""",
            (GLOBAL_BOOTSTRAP_TENANT_ID,),
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert ensure_schema(conn, legacy_tenant="default", bootstrap_signal_definitions={}) is False

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("""SELECT COUNT(*) FROM signal_metric_mappings
               WHERE tenant_id='default' AND source_type='bootstrap'""").fetchone()[0] == 1_200
        assert conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
                (CURRENT_SIGNAL_SCHEMA_MARKER,),
            ).fetchone()
            is None
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="default",
            batch_size=500,
        ) == (False, "signal_metric_mappings:copy", 500)

    with monkeypatch.context() as failure_patch:
        _fail_cursor_update_once(
            failure_patch,
            key="legacy_schema_copy_cursor_v1:signal_metric_mappings",
        )
        with pytest.raises(RuntimeError, match="simulated bounded migration failure"):
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute("BEGIN IMMEDIATE")
                reconcile_legacy_signal_schema_batch(
                    conn,
                    legacy_tenant="default",
                    batch_size=500,
                )

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_metric_mappings").fetchone()[0] == 1_201
        assert (
            conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[0] == 500
        )

    operations = _finish_legacy_schema_migration(db_path, tenant="default", batch_size=500)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT tenant_id, metric_pattern, confidence FROM signal_metric_mappings ORDER BY metric_pattern"
        ).fetchall()
    assert len(rows) == 1_200
    assert {row[0] for row in rows} == {GLOBAL_BOOTSTRAP_TENANT_ID}
    assert next(row[2] for row in rows if row[1] == "bootstrap_metric_0") == 0.9
    assert max(copied for _, copied in operations) <= 500


def test_legacy_schema_rowid_keyset_includes_negative_zero_and_sparse_ids(tmp_path) -> None:
    db_path = tmp_path / "legacy-rowid-boundaries.db"
    _seed_legacy_mapping_schema(db_path, rows=0)
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """INSERT INTO signal_metric_mappings
               (id, signal_type, metric_pattern, created_at, last_seen)
               VALUES (?, 'request_latency', ?, 1, 1)""",
            [(-7, "negative"), (0, "zero"), (97, "sparse-positive")],
        )
        conn.commit()
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert ensure_schema(conn, legacy_tenant="tenant-a", bootstrap_signal_definitions={}) is False

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="tenant-a",
            batch_size=2,
        ) == (False, "signal_metric_mappings:copy", 2)

    with sqlite3.connect(db_path) as conn:
        first_page = conn.execute(
            "SELECT id FROM signal_metric_mappings_tacit_tenant_migration_v1 ORDER BY id"
        ).fetchall()
    assert first_page == [(-7,), (0,)]

    _finish_legacy_schema_migration(db_path, tenant="tenant-a", batch_size=2)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT id, metric_pattern FROM signal_metric_mappings ORDER BY id").fetchall()
    assert rows == [(-7, "negative"), (0, "zero"), (97, "sparse-positive")]


def test_owner_reconciliation_rowid_keyset_includes_negative_zero_and_sparse_ids(tmp_path) -> None:
    db_path = tmp_path / "owner-rowid-boundaries.db"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_tenant_migration_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE tenant_signal_types (
                tenant_id TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '',
                unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (tenant_id, signal_type)
            );
        """)
        conn.executemany(
            """INSERT INTO tenant_signal_types
               (rowid, tenant_id, signal_type, created_at, updated_at)
               VALUES (?, 'default', ?, 1, 1)""",
            [(-9, "negative"), (0, "zero"), (113, "sparse-positive")],
        )

    batch_counts: list[int] = []
    for _ in range(20):
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, _, count = reconcile_default_tenant_owner_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=2,
            )
            batch_counts.append(count)
        if complete:
            break
    else:
        pytest.fail("owner reconciliation did not converge")

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT rowid, tenant_id FROM tenant_signal_types ORDER BY rowid").fetchall()
    assert rows == [(-9, "tenant-a"), (0, "tenant-a"), (113, "tenant-a")]
    assert max(batch_counts) <= 2


def test_source_ref_keyset_includes_negative_zero_and_sparse_mapping_ids(tmp_path) -> None:
    db_path = tmp_path / "source-ref-id-boundaries.db"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_tenant_migration_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                source_refs TEXT NOT NULL
            );
            CREATE TABLE signal_mapping_source_refs (
                mapping_id INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                PRIMARY KEY (mapping_id, source_ref)
            );
        """)
        conn.executemany(
            "INSERT INTO signal_metric_mappings (id, tenant_id, source_refs) VALUES (?, 'tenant-a', ?)",
            [(-11, '["negative"]'), (0, '["zero"]'), (211, '["sparse"]')],
        )

    complete = False
    while not complete:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, _ = reconcile_mapping_source_ref_index_batch(conn, batch_size=2)

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT mapping_id, source_ref FROM signal_mapping_source_refs ORDER BY mapping_id"
        ).fetchall()
    assert rows == [(-11, "negative"), (0, "zero"), (211, "sparse")]


def test_source_ref_migration_rejects_oversized_legacy_fanout_without_advancing_cursor(tmp_path) -> None:
    db_path = tmp_path / "source-ref-oversized-legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_tenant_migration_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                source_refs TEXT NOT NULL
            );
            CREATE TABLE signal_mapping_source_refs (
                mapping_id INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                PRIMARY KEY (mapping_id, source_ref)
            );
        """)
        oversized = [f"source:{index}" for index in range(signal_migrations.SIGNAL_MAPPING_SOURCE_REF_MAX_COUNT + 1)]
        conn.execute(
            "INSERT INTO signal_metric_mappings (id, tenant_id, source_refs) VALUES (7, 'tenant-a', ?)",
            (json.dumps(oversized),),
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RuntimeError, match="source refs exceed the storage limit"):
            reconcile_mapping_source_ref_index_batch(conn, batch_size=2)
        conn.rollback()

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM signal_mapping_source_refs").fetchone()[0] == 0
        assert (
            conn.execute(
                "SELECT 1 FROM signal_tenant_migration_metadata WHERE key='mapping_source_ref_cursor_v2'"
            ).fetchone()
            is None
        )


@pytest.mark.parametrize("budget_kind", ["bytes", "children"])
def test_source_ref_migration_bounds_aggregate_transaction_work(tmp_path, budget_kind: str) -> None:
    db_path = tmp_path / f"source-ref-aggregate-{budget_kind}.db"
    if budget_kind == "bytes":
        payload_size = signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES // 2 + 1_024
        payloads = [json.dumps([character * payload_size]) for character in ("a", "b")]
    else:
        refs_per_row = signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN // 2 + 1
        payloads = [
            json.dumps([f"source:{row_index}:{index}" for index in range(refs_per_row)]) for row_index in range(2)
        ]

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_tenant_migration_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                source_refs TEXT NOT NULL
            );
            CREATE TABLE signal_mapping_source_refs (
                mapping_id INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                PRIMARY KEY (mapping_id, source_ref)
            );
        """)
        conn.executemany(
            "INSERT INTO signal_metric_mappings (id, tenant_id, source_refs) VALUES (?, 'tenant-a', ?)",
            [(1, payloads[0]), (2, payloads[1])],
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        complete, first_count = reconcile_mapping_source_ref_index_batch(conn, batch_size=500)
    assert complete is False

    with sqlite3.connect(db_path) as conn:
        first_mapping_ids = {
            int(row[0]) for row in conn.execute("SELECT DISTINCT mapping_id FROM signal_mapping_source_refs")
        }
        cursor = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='mapping_source_ref_cursor_v2'"
        ).fetchone()
    assert first_mapping_ids == {1}
    assert cursor is not None and cursor[0] == "1"
    assert first_count <= signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        complete, second_count = reconcile_mapping_source_ref_index_batch(conn, batch_size=500)
    assert complete is True
    assert second_count <= signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN

    with sqlite3.connect(db_path) as conn:
        assert {int(row[0]) for row in conn.execute("SELECT DISTINCT mapping_id FROM signal_mapping_source_refs")} == {
            1,
            2,
        }


def test_source_ref_migration_does_not_decode_the_child_budget_overflow_row(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "source-ref-decode-budget.db"
    refs_per_row = signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN // 2 + 1
    payloads = [json.dumps([f"source:{row_index}:{index}" for index in range(refs_per_row)]) for row_index in range(2)]
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_tenant_migration_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                source_refs TEXT NOT NULL
            );
            CREATE TABLE signal_mapping_source_refs (
                mapping_id INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                PRIMARY KEY (mapping_id, source_ref)
            );
        """)
        conn.executemany(
            "INSERT INTO signal_metric_mappings (id, tenant_id, source_refs) VALUES (?, 'tenant-a', ?)",
            [(1, payloads[0]), (2, payloads[1])],
        )

    decoded_child_counts: list[int] = []
    original_loads = signal_migrations.json.loads

    def tracking_loads(payload: str):
        decoded = original_loads(payload)
        if isinstance(decoded, list):
            decoded_child_counts.append(len(decoded))
        return decoded

    monkeypatch.setattr(signal_migrations.json, "loads", tracking_loads)
    complete = False
    for _ in range(3):
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, _ = reconcile_mapping_source_ref_index_batch(conn, batch_size=500)
        if decoded_child_counts:
            break

    assert complete is False
    assert decoded_child_counts == [refs_per_row]
    assert sum(decoded_child_counts) <= signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_CHILDREN


def test_source_ref_migration_checks_aggregate_bytes_before_sqlite_json_parsing(tmp_path) -> None:
    db_path = tmp_path / "source-ref-sql-json-budget.db"
    payload_size = signal_migrations.SIGNAL_MAPPING_SOURCE_REF_WORK_MAX_BYTES // 2 + 1_024
    first_payload = json.dumps(["a" * payload_size])
    overflow_payload = json.dumps(["b" * payload_size])
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE signal_tenant_migration_metadata (
                key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                source_refs TEXT NOT NULL
            );
            CREATE TABLE signal_mapping_source_refs (
                mapping_id INTEGER NOT NULL,
                tenant_id TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                PRIMARY KEY (mapping_id, source_ref)
            );
        """)
        conn.executemany(
            "INSERT INTO signal_metric_mappings (id, tenant_id, source_refs) VALUES (?, 'tenant-a', ?)",
            [(1, first_payload), (2, overflow_payload)],
        )

    parsed_payloads: list[str] = []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        def tracked_json_valid(payload: str) -> int:
            parsed_payloads.append(payload)
            if payload == overflow_payload:
                raise AssertionError("aggregate overflow row reached SQLite JSON parsing")
            try:
                json.loads(payload)
            except json.JSONDecodeError:
                return 0
            return 1

        conn.create_function("json_valid", 1, tracked_json_valid)
        conn.execute("BEGIN IMMEDIATE")
        complete, _ = reconcile_mapping_source_ref_index_batch(conn, batch_size=500)

    assert complete is False
    assert parsed_payloads == [first_payload]


def test_source_ref_repair_removes_orphans_before_certifying_the_projection(tmp_path) -> None:
    db_path = tmp_path / "source-ref-orphan-repair.db"
    store = SignalStore(db_path)
    orphan_id = store.add_mapping(
        "source_ref_orphan",
        "orphan_metric",
        tenant_id="default",
        source_refs=["dashboard:tenant-private:orphan"],
    )
    reused_id = store.add_mapping(
        "source_ref_reuse",
        "old_reused_metric",
        tenant_id="default",
        source_refs=["dashboard:tenant-private:old"],
    )
    with store._conn() as conn:
        conn.execute("DROP TRIGGER trg_signal_mapping_source_ref_delete")
        conn.execute("""CREATE TRIGGER trg_signal_mapping_source_ref_delete
               AFTER DELETE ON signal_metric_mappings BEGIN SELECT 1; END""")
        conn.execute("DELETE FROM signal_metric_mappings WHERE id IN (?, ?)", (orphan_id, reused_id))
        conn.execute(
            """INSERT INTO signal_metric_mappings
               (id, tenant_id, signal_type, metric_pattern, confidence, source_type,
                source_refs, review_state, created_at, last_seen)
               VALUES (?, 'tenant-b', 'source_ref_reuse', 'new_reused_metric', 0.9,
                       'teach', '["dashboard:tenant-b:new"]', 'trusted', 1, 1)""",
            (reused_id,),
        )
        signal_migrations.ensure_mapping_source_ref_index(conn)
        assert (
            conn.execute(
                "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
                (signal_migrations.MAPPING_SOURCE_REF_INDEX_MARKER,),
            ).fetchone()
            is None
        )

    complete = False
    while not complete:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, _ = reconcile_mapping_source_ref_index_batch(conn, batch_size=1)
            orphan_exists = conn.execute("""SELECT 1 FROM signal_mapping_source_refs child
                   LEFT JOIN signal_metric_mappings parent ON parent.id=child.mapping_id
                   WHERE parent.id IS NULL LIMIT 1""").fetchone() is not None
            marker_exists = (
                conn.execute(
                    "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
                    (signal_migrations.MAPPING_SOURCE_REF_INDEX_MARKER,),
                ).fetchone()
                is not None
            )
            if orphan_exists:
                assert marker_exists is False

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("""SELECT mapping_id, tenant_id, source_ref
               FROM signal_mapping_source_refs ORDER BY mapping_id, source_ref""").fetchall()
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (signal_migrations.MAPPING_SOURCE_REF_INDEX_MARKER,),
        ).fetchone()
    assert rows == [(reused_id, "tenant-b", "dashboard:tenant-b:new")]
    assert marker == ("complete",)


def test_fts_unavailable_capability_marker_converges_across_reopens(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "fts-unavailable-converges.db"
    settings = Settings.model_validate({"knowledge_tenant_id": "default"})
    monkeypatch.setattr(
        signal_migrations,
        "FTS_SCHEMA_SQL",
        "CREATE VIRTUAL TABLE IF NOT EXISTS learning_context_fts USING unavailable_fts(body);",
    )

    SignalStore(db_path=db_path, runtime_settings=settings)
    with sqlite3.connect(db_path) as conn:
        capability = conn.execute("""SELECT value, updated_at FROM signal_tenant_migration_metadata
               WHERE key='learning_context_fts_capability_v1'""").fetchone()
        current = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (CURRENT_SIGNAL_SCHEMA_MARKER,),
        ).fetchone()
    assert capability is not None
    assert capability[0] == "unavailable"
    assert current is not None

    monkeypatch.setattr(signal_migrations, "FTS_SCHEMA_SQL", "THIS SECOND OPEN MUST NOT EXECUTE;")
    SignalStore(db_path=db_path, runtime_settings=settings)
    with sqlite3.connect(db_path) as conn:
        reopened = conn.execute("""SELECT value, updated_at FROM signal_tenant_migration_metadata
               WHERE key='learning_context_fts_capability_v1'""").fetchone()
    assert reopened == capability


def test_legacy_signal_schema_copy_resumes_from_a_committed_bounded_batch(tmp_path) -> None:
    db_path = tmp_path / "legacy-schema-resume.db"
    row_count = 1_201
    batch_size = 500
    _seed_legacy_mapping_schema(db_path, rows=row_count)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert (
            ensure_schema(
                conn,
                legacy_tenant="tenant-a",
                bootstrap_signal_definitions={},
            )
            is False
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        complete, operation, copied = reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="tenant-a",
            batch_size=batch_size,
        )
        assert complete is False
        assert operation == "signal_metric_mappings:copy"
        assert copied == batch_size

    with sqlite3.connect(db_path) as conn:
        source_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings").fetchone()[0]
        target_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[
            0
        ]
        cursor = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='legacy_schema_copy_cursor_v1:signal_metric_mappings'""").fetchone()
        current = conn.execute(
            "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
            (CURRENT_SIGNAL_SCHEMA_MARKER,),
        ).fetchone()
    assert source_count == row_count
    assert target_count == batch_size
    assert cursor == (str(batch_size),)
    assert current is None

    complete = False
    operations: list[tuple[str, int]] = []
    while not complete:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, operation, copied = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=batch_size,
            )
            operations.append((operation, copied))

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert (
            ensure_schema(
                conn,
                legacy_tenant="tenant-a",
                bootstrap_signal_definitions={},
            )
            is True
        )

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT tenant_id, metric_pattern FROM signal_metric_mappings ORDER BY id").fetchall()
        old_table = conn.execute("""SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='signal_metric_mappings_tacit_tenant_migration_v1'""").fetchone()
        cursor = conn.execute("""SELECT 1 FROM signal_tenant_migration_metadata
               WHERE key LIKE 'legacy_schema_copy_cursor_v1:%'""").fetchone()
        current = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (CURRENT_SIGNAL_SCHEMA_MARKER,),
        ).fetchone()

    assert [row[0] for row in rows] == ["tenant-a"] * row_count
    assert [row[1] for row in rows] == [f"legacy_metric_{index}" for index in range(row_count)]
    assert old_table is None
    assert cursor is None
    assert current is not None
    assert max(copied for _, copied in operations) <= batch_size


def test_legacy_learning_index_remains_authoritative_until_atomic_swap(tmp_path) -> None:
    db_path = tmp_path / "legacy-learning-index.db"
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
            CREATE VIRTUAL TABLE learning_context_fts USING fts5(
                source_kind, source_id UNINDEXED, backend_name UNINDEXED,
                dashboard_uid UNINDEXED, dashboard_title, dashboard_tags,
                panel_title, metric_name, query_text, service, signal_type,
                review_state UNINDEXED, reason, provenance, indexed_at UNINDEXED
            );
            INSERT INTO learning_context_fts
                (rowid, source_kind, source_id, dashboard_title, metric_name, indexed_at)
            VALUES
                (-13, 'dashboard', 'negative', 'Negative', 'metric_negative', 1),
                (0, 'dashboard', 'zero', 'Zero', 'metric_zero', 2),
                (89, 'dashboard', 'positive', 'Positive', 'metric_positive', 3);
        """)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert (
            ensure_schema(
                conn,
                legacy_tenant="tenant-a",
                bootstrap_signal_definitions={},
            )
            is False
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        complete, operation, copied = reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="tenant-a",
            batch_size=2,
        )
        assert (complete, operation, copied) == (False, "learning_context_fts:copy", 2)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM learning_context_fts").fetchone()[0] == 3
        assert conn.execute("SELECT COUNT(*) FROM learning_context_fts_tacit_tenant_migration_v1").fetchone()[0] == 2
        assert conn.execute(
            "SELECT source_id FROM learning_context_fts_tacit_tenant_migration_v1 ORDER BY rowid"
        ).fetchall() == [("negative",), ("zero",)]

    complete = False
    while not complete:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            complete, _, _ = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=2,
            )

    with sqlite3.connect(db_path) as conn:
        columns = [row[1] for row in conn.execute("PRAGMA table_info(learning_context_fts)")]
        tenants = conn.execute("SELECT tenant_id FROM learning_context_fts ORDER BY rowid").fetchall()
        shadow = conn.execute("""SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='learning_context_fts_tacit_tenant_migration_v1'""").fetchone()
    assert "tenant_id" in columns
    assert tenants == [("tenant-a",), ("tenant-a",), ("tenant-a",)]
    assert shadow is None


def test_legacy_signal_schema_final_swap_rolls_back_as_one_unit(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "legacy-schema-final-swap.db"
    _seed_legacy_mapping_schema(db_path, rows=3)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        assert (
            ensure_schema(
                conn,
                legacy_tenant="tenant-a",
                bootstrap_signal_definitions={},
            )
            is False
        )
    for _ in range(2):
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            _, operation, _ = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=2,
            )
        assert operation == "signal_metric_mappings:copy"

    original_finalize = signal_migrations._ensure_signal_mapping_indexes

    def fail_after_swap(_conn: sqlite3.Connection) -> None:
        raise RuntimeError("simulated final swap failure")

    monkeypatch.setattr(signal_migrations, "_ensure_signal_mapping_indexes", fail_after_swap)
    with pytest.raises(RuntimeError, match="simulated final swap failure"):
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=2,
            )

    with sqlite3.connect(db_path) as conn:
        source_columns = [row[1] for row in conn.execute("PRAGMA table_info(signal_metric_mappings)")]
        source_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings").fetchone()[0]
        shadow_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[
            0
        ]
    assert "tenant_id" not in source_columns
    assert source_count == shadow_count == 3

    monkeypatch.setattr(signal_migrations, "_ensure_signal_mapping_indexes", original_finalize)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        _, operation, _ = reconcile_legacy_signal_schema_batch(
            conn,
            legacy_tenant="tenant-a",
            batch_size=2,
        )
    assert operation == "signal_metric_mappings:finalize"


def test_signal_store_resumes_legacy_schema_copy_after_constructor_interruption(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "legacy-store-constructor-resume.db"
    row_count = 501
    _seed_legacy_mapping_schema(db_path, rows=row_count)
    runtime_settings = Settings.model_validate({"knowledge_tenant_id": "tenant-a"})
    original_reconcile = SignalStore._reconcile_legacy_signal_schema_batched

    def copy_one_batch_then_stop(store: SignalStore) -> None:
        with store._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            complete, operation, copied = reconcile_legacy_signal_schema_batch(
                conn,
                legacy_tenant="tenant-a",
                batch_size=500,
            )
        assert complete is False
        assert operation == "signal_metric_mappings:copy"
        assert copied == 500
        raise RuntimeError("simulated constructor interruption")

    monkeypatch.setattr(SignalStore, "_reconcile_legacy_signal_schema_batched", copy_one_batch_then_stop)
    with pytest.raises(RuntimeError, match="simulated constructor interruption"):
        SignalStore(db_path=db_path, runtime_settings=runtime_settings)

    with sqlite3.connect(db_path) as conn:
        role = conn.execute("SELECT role FROM tacit_runtime_database_identity WHERE singleton=1").fetchone()
        source_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings").fetchone()[0]
        shadow_count = conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[
            0
        ]
        cursor = conn.execute("""SELECT value FROM signal_tenant_migration_metadata
               WHERE key='legacy_schema_copy_cursor_v1:signal_metric_mappings'""").fetchone()
        current = conn.execute(
            "SELECT 1 FROM signal_tenant_migration_metadata WHERE key=?",
            (CURRENT_SIGNAL_SCHEMA_MARKER,),
        ).fetchone()
    assert role == ("signals",)
    assert source_count == row_count
    assert shadow_count == 500
    assert cursor == ("500",)
    assert current is None

    with pytest.raises(RuntimeError, match="reason=migration_owner_mismatch"):
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings.model_validate({"knowledge_tenant_id": "tenant-b"}),
        )
    with sqlite3.connect(db_path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM signal_metric_mappings_tacit_tenant_migration_v1").fetchone()[0] == 500
        )
        assert conn.execute("SELECT COUNT(*) FROM signal_metric_mappings").fetchone()[0] == row_count

    monkeypatch.setattr(SignalStore, "_reconcile_legacy_signal_schema_batched", original_reconcile)
    resumed = SignalStore(db_path=db_path, runtime_settings=runtime_settings)
    with resumed._conn() as conn:
        rows = conn.execute("SELECT tenant_id, metric_pattern FROM signal_metric_mappings ORDER BY id").fetchall()
        shadow = conn.execute("""SELECT 1 FROM sqlite_master
               WHERE type='table' AND name='signal_metric_mappings_tacit_tenant_migration_v1'""").fetchone()
        cursor = conn.execute("""SELECT 1 FROM signal_tenant_migration_metadata
               WHERE key LIKE 'legacy_schema_copy_cursor_v1:%'""").fetchone()
    assert len(rows) == row_count
    assert {row["tenant_id"] for row in rows} == {"tenant-a"}
    assert shadow is None
    assert cursor is None
