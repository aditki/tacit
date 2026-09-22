from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from structlog.testing import capture_logs

from tacit.api.app import create_app
from tacit.cli import cli
from tacit.config import Settings
from tacit.grounding_benchmark import _contract_for_case, load_grounding_corpus
from tacit.history import InvestigationStore, StaleRevisionError
from tacit.investigation_bundle import build_investigation_bundle, export_investigation_bundle
from tacit.investigation_contract import InvestigationRunType
from tests.http_client import TestClient


def _contract(investigation_id: str = "inv-a", *, tenant_id: str = "tenant-a"):
    contract = _contract_for_case(load_grounding_corpus()[0])
    return contract.model_copy(
        update={
            "investigation": contract.investigation.model_copy(update={"id": investigation_id}),
            "request": contract.request.model_copy(
                update={
                    "scope": contract.request.scope.model_copy(update={"tenant_id": tenant_id}),
                }
            ),
        }
    )


def _seed_legacy_history(
    db_path: Path,
    contract_tenants: dict[str, str | None],
    *,
    retain_tenant_column: bool = False,
) -> dict[str, str]:
    store = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    investigation_ids: dict[str, str] = {}
    for label, contract_tenant in contract_tenants.items():
        investigation_id = store.start(f"Legacy {label}")
        investigation_ids[label] = investigation_id
        scope = {} if contract_tenant is None else {"tenant_id": contract_tenant}
        with store._conn() as conn:
            conn.execute(
                """INSERT INTO investigation_revisions (
                       investigation_id, revision, parent_revision, schema_version, contract_json,
                       input_fingerprint, output_fingerprint, engine_version, created_at, reason
                   ) VALUES (?, 1, NULL, '1.0', ?, 'input', 'output', 'legacy', ?, 'fixture')""",
                (investigation_id, json.dumps({"request": {"scope": scope}}), time.time()),
            )
            conn.execute(
                "UPDATE investigations SET current_revision=1 WHERE id=?",
                (investigation_id,),
            )
    with store._conn() as conn:
        for index_name in (
            "idx_inv_tenant_started",
            "idx_inv_tenant_status_started",
            "idx_inv_tenant_user_started",
            "idx_inv_tenant_dashboard",
        ):
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        conn.execute("DROP TABLE investigation_tenant_assignments")
        if not retain_tenant_column:
            conn.execute("ALTER TABLE investigations DROP COLUMN tenant_id")
    return investigation_ids


def test_pinned_migration_assigns_ownerless_contracts_and_preserves_explicit_owners(tmp_path):
    db_path = tmp_path / "legacy-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {
            "absent": None,
            "empty": "",
            "default": "default",
            "explicit": "tenant-b",
        },
    )

    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert store.get(investigation_ids["absent"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["empty"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["default"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["explicit"]) is None
    with store._conn() as conn:
        explicit_owner = conn.execute(
            "SELECT tenant_id FROM investigations WHERE id=?",
            (investigation_ids["explicit"],),
        ).fetchone()["tenant_id"]
    assert explicit_owner == "tenant-b"


def test_pinned_history_migration_processes_multiple_bounded_batches(tmp_path):
    db_path = tmp_path / "large-legacy-history.db"
    store = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    rows = [(f"legacy-{index:04d}", f"Legacy prompt {index}", time.time()) for index in range(1_205)]
    with store._conn() as conn:
        conn.executemany(
            "INSERT INTO investigations (id, prompt, started_at) VALUES (?, ?, ?)",
            rows,
        )
        for index_name in (
            "idx_inv_tenant_started",
            "idx_inv_tenant_status_started",
            "idx_inv_tenant_user_started",
            "idx_inv_tenant_dashboard",
        ):
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        conn.execute("ALTER TABLE investigations DROP COLUMN tenant_id")

    migrated = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    with migrated._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM investigations WHERE tenant_id='tenant-a'").fetchone()[0] == len(rows)
        assert conn.execute("SELECT COUNT(*) FROM investigation_tenant_assignments").fetchone()[0] == len(rows)


def test_wildcard_migration_rejects_ownerless_contracts_before_schema_mutation(tmp_path):
    db_path = tmp_path / "ownerless-history.db"
    _seed_legacy_history(
        db_path,
        {"ownerless": "default", "owned": "tenant-b"},
    )
    with sqlite3.connect(db_path) as conn:
        identity_before = conn.execute("SELECT role, database_id FROM tacit_runtime_database_identity").fetchone()

    with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
        )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
        identity_after = conn.execute("SELECT role, database_id FROM tacit_runtime_database_identity").fetchone()
        tenant_index = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_inv_tenant_started'"
        ).fetchone()
    assert "tenant_id" not in columns
    assert identity_after == identity_before
    assert tenant_index is None


def test_wildcard_legacy_owner_preflight_redacts_investigation_ids(tmp_path):
    db_path = tmp_path / "legacy-owner-diagnostic.db"
    investigation_id = "PRIVATE-LEGACY-INVESTIGATION-CANARY"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE investigations (id TEXT PRIMARY KEY, prompt TEXT, started_at REAL)")
        conn.execute(
            "INSERT INTO investigations (id, prompt, started_at) VALUES (?, 'canary', ?)",
            (investigation_id, time.time()),
        )

    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
            InvestigationStore(
                db_path=db_path,
                runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
            )

    rendered_logs = str(logs)
    assert investigation_id not in rendered_logs
    event = next(entry for entry in logs if entry.get("event") == "legacy_history_owner_required")
    assert event["reason_code"] == "legacy_history_owner_required"
    assert event["ownerless_count"] == 1
    assert len(event["investigation_ids_fingerprint"]) == 16
    assert "sample_investigation_ids" not in event


def test_wildcard_unconfirmed_default_owner_preflight_redacts_investigation_ids(tmp_path):
    db_path = tmp_path / "default-owner-diagnostic.db"
    investigation_id = "PRIVATE-DEFAULT-INVESTIGATION-CANARY"
    original = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    with original._conn() as conn:
        conn.execute(
            "INSERT INTO investigations (id, tenant_id, prompt, started_at) VALUES (?, 'default', 'canary', ?)",
            (investigation_id, time.time()),
        )

    with capture_logs() as logs:
        with pytest.raises(RuntimeError, match="unconfirmed default-tenant ownership"):
            InvestigationStore(
                db_path=db_path,
                runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
            )

    rendered_logs = str(logs)
    assert investigation_id not in rendered_logs
    event = next(entry for entry in logs if entry.get("event") == "legacy_history_default_owner_unconfirmed")
    assert event["reason_code"] == "legacy_history_default_owner_unconfirmed"
    assert event["ownerless_count"] == 1
    assert len(event["investigation_ids_fingerprint"]) == 16
    assert "sample_investigation_ids" not in event


def test_contract_deserialization_diagnostics_redact_path_and_tenant(tmp_path):
    tenant_canary = "PRIVATE-HISTORY-TENANT-CANARY"
    path_canary = "PRIVATE-HISTORY-PATH-CANARY"
    db_path = tmp_path / path_canary / "history.db"
    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(
            _env_file=None,
            knowledge_tenant_id=tenant_canary,
            history_db_path=str(db_path),
        ),
    )
    investigation_id = store.start("Diagnostic canary", tenant_id=tenant_canary)
    with store._conn() as conn:
        conn.execute(
            """INSERT INTO investigation_revisions (
                   investigation_id, revision, parent_revision, schema_version, contract_json,
                   input_fingerprint, output_fingerprint, engine_version, created_at, reason
               ) VALUES (?, 1, NULL, '1.0', ?, 'input', 'output', 'test', ?, 'fixture')""",
            (
                investigation_id,
                json.dumps({"tenant": tenant_canary, "database_path": str(db_path)}),
                time.time(),
            ),
        )

    with capture_logs() as logs:
        assert store.get_contract(investigation_id, 1, tenant_id=tenant_canary) is None

    rendered = repr(logs)
    assert tenant_canary not in rendered
    assert path_canary not in rendered
    assert str(db_path) not in rendered
    warning = next(entry for entry in logs if entry.get("event") == "investigation_contract_deserialize_failed")
    assert warning["reason_code"] == "investigation_contract_deserialize_failed"
    assert len(str(warning["investigation_fingerprint"])) == 16
    assert warning["failure_fingerprint"]
    assert warning["error_type"]


def test_wildcard_migration_rechecks_owner_after_acquiring_writer_lock(tmp_path):
    db_path = tmp_path / "owner-race-history.db"
    _seed_legacy_history(db_path, {"owned": "tenant-a"})

    class RacingHistoryStore(InvestigationStore):
        owner_checks = 0

        def _require_legacy_tenant_owner(self, conn, *, tenant_column_existed):
            type(self).owner_checks += 1
            super()._require_legacy_tenant_owner(conn, tenant_column_existed=tenant_column_existed)

        def _preflight_existing_owner(self):
            super()._preflight_existing_owner()
            with sqlite3.connect(db_path) as writer:
                writer.execute(
                    "INSERT INTO investigations (id, prompt, started_at) VALUES (?, ?, ?)",
                    ("late-ownerless", "Inserted after preflight", time.time()),
                )

    with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
        RacingHistoryStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
        )

    assert RacingHistoryStore.owner_checks == 2
    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
        progress = conn.execute("SELECT 1 FROM history_migration_progress").fetchone()
    assert "tenant_id" not in columns
    assert progress is None


def test_wildcard_first_open_rechecks_legacy_table_existence_after_writer_lock(tmp_path):
    db_path = tmp_path / "first-open-owner-race.db"

    class RacingHistoryStore(InvestigationStore):
        inserted_legacy_table = False

        @classmethod
        def _table_exists(cls, conn, table_name):
            existed = super()._table_exists(conn, table_name)
            if table_name == "investigations" and not existed and not cls.inserted_legacy_table:
                cls.inserted_legacy_table = True
                with sqlite3.connect(db_path) as writer:
                    writer.execute("""CREATE TABLE investigations (
                               id TEXT PRIMARY KEY,
                               prompt TEXT NOT NULL,
                               started_at REAL NOT NULL
                           )""")
                    writer.execute(
                        "INSERT INTO investigations (id, prompt, started_at) VALUES (?, ?, ?)",
                        ("late-ownerless", "Inserted after preflight", time.time()),
                    )
            return existed

    with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
        RacingHistoryStore(
            db_path=db_path,
            runtime_settings=Settings(
                _env_file=None,
                knowledge_tenant_id="*",
                api_auth_enabled=True,
            ),
        )

    assert RacingHistoryStore.inserted_legacy_table is True
    with sqlite3.connect(db_path) as conn:
        assert {row[1] for row in conn.execute("PRAGMA table_info(investigations)")} == {
            "id",
            "prompt",
            "started_at",
        }
        assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='history_migration_progress'").fetchone() is None


def test_wildcard_migration_preserves_explicit_contract_owners(tmp_path):
    db_path = tmp_path / "owned-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {"tenant-a": "tenant-a", "tenant-b": "tenant-b"},
    )

    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )

    assert store.get(investigation_ids["tenant-a"], tenant_id="tenant-a")["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["tenant-b"], tenant_id="tenant-b")["tenant_id"] == "tenant-b"


def test_pinned_migration_repairs_partial_default_placeholders(tmp_path):
    db_path = tmp_path / "partial-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {"placeholder": "default", "owned": "tenant-b"},
        retain_tenant_column=True,
    )

    store = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert store.get(investigation_ids["placeholder"])["tenant_id"] == "tenant-a"
    assert store.get(investigation_ids["owned"]) is None
    with store._conn() as conn:
        explicit_owner = conn.execute(
            "SELECT tenant_id FROM investigations WHERE id=?",
            (investigation_ids["owned"],),
        ).fetchone()["tenant_id"]
    assert explicit_owner == "tenant-b"


def test_wildcard_migration_rejects_partial_default_placeholders(tmp_path):
    db_path = tmp_path / "partial-ownerless-history.db"
    investigation_ids = _seed_legacy_history(
        db_path,
        {"placeholder": "default", "owned": "tenant-b"},
        retain_tenant_column=True,
    )

    with pytest.raises(RuntimeError, match="Legacy investigation history has no tenant owner"):
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
        )

    with sqlite3.connect(db_path) as conn:
        placeholder = conn.execute(
            "SELECT tenant_id FROM investigations WHERE id=?",
            (investigation_ids["placeholder"],),
        ).fetchone()
        tenant_index = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_inv_tenant_started'"
        ).fetchone()
    assert placeholder == ("default",)
    assert tenant_index is None


def test_complete_schema_default_history_requires_and_records_a_migration_owner(tmp_path):
    db_path = tmp_path / "previously-migrated-history.db"
    original = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    investigation_id = original.start("Previously migrated")
    with original._conn() as conn:
        conn.execute("DROP TABLE investigation_tenant_assignments")

    with pytest.raises(RuntimeError, match="tenant owner"):
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
        )

    pinned = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    assert pinned.get(investigation_id, tenant_id="tenant-a")["tenant_id"] == "tenant-a"

    wildcard = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )
    assert wildcard.get(investigation_id, tenant_id="tenant-a") is not None
    assert wildcard.get(investigation_id, tenant_id="default") is None


def test_current_history_schema_preflights_owner_then_rechecks_after_writer_lock(tmp_path, monkeypatch):
    db_path = tmp_path / "current-history-owner-lock.db"
    InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    owner_checks: list[bool] = []
    original = InvestigationStore._require_confirmed_default_tenant_owner

    def require_locked_owner(self, conn):
        owner_checks.append(conn.in_transaction)
        return original(self, conn)

    monkeypatch.setattr(
        InvestigationStore,
        "_require_confirmed_default_tenant_owner",
        require_locked_owner,
    )

    InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert owner_checks == [False, True]


def test_history_schema_migration_resumes_for_the_same_owner_and_rejects_an_owner_change(tmp_path):
    db_path = tmp_path / "rollback-history.db"
    store = InvestigationStore(db_path=db_path, runtime_settings=Settings(_env_file=None))
    rows = [(f"legacy-{index:04d}", f"Legacy prompt {index}", time.time()) for index in range(505)]
    with store._conn() as conn:
        conn.executemany(
            "INSERT INTO investigations (id, prompt, started_at) VALUES (?, ?, ?)",
            rows,
        )
        for index_name in (
            "idx_inv_tenant_started",
            "idx_inv_tenant_status_started",
            "idx_inv_tenant_user_started",
            "idx_inv_tenant_dashboard",
        ):
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        conn.execute("ALTER TABLE investigations DROP COLUMN tenant_id")

    class FailingHistoryStore(InvestigationStore):
        def _migrate_history_tenant_batch(self) -> tuple[int, bool]:
            super()._migrate_history_tenant_batch()
            raise RuntimeError("forced migration failure")

    with pytest.raises(RuntimeError, match="forced migration failure"):
        FailingHistoryStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
        )

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
        assigned = conn.execute("SELECT COUNT(*) FROM investigation_tenant_assignments").fetchone()
        progress = conn.execute("SELECT 1 FROM history_migration_progress").fetchone()
    assert "tenant_id" in columns
    assert assigned == (500,)
    assert progress is not None

    with pytest.raises(RuntimeError, match="already in progress for another tenant") as exc_info:
        InvestigationStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
        )
    assert "tenant-a" not in str(exc_info.value)
    assert "tenant-b" not in str(exc_info.value)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM investigation_tenant_assignments").fetchone() == (500,)
        assert conn.execute("SELECT COUNT(*) FROM investigations WHERE tenant_id='tenant-b'").fetchone() == (0,)
        details = json.loads(
            conn.execute(
                "SELECT details_json FROM history_migration_progress WHERE migration_name=?",
                ("history_tenant_assignment_backfill_v2",),
            ).fetchone()[0]
        )
    assert details["configured_tenant"] == "tenant-a"

    legacy_row_checks = 0

    class CountingHistoryStore(InvestigationStore):
        def _legacy_row_tenant(self, *args, **kwargs):
            nonlocal legacy_row_checks
            legacy_row_checks += 1
            return super()._legacy_row_tenant(*args, **kwargs)

    reopened = CountingHistoryStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with reopened._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM investigation_tenant_assignments").fetchone()[0] == 505
        assert conn.execute("SELECT COUNT(*) FROM history_migration_progress").fetchone()[0] == 0
    assert legacy_row_checks == 5


def test_history_upgrade_adds_investigation_event_lookup_index(tmp_path, monkeypatch):
    db_path = tmp_path / "history-event-index.db"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    store = InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)
    investigation_id = store.start("Index event history", tenant_id="tenant-a")
    with store._conn() as conn:
        conn.execute("DROP INDEX idx_inv_events_investigation_order")

    def unexpected_reconciliation(self):
        raise AssertionError("event-index upgrade must not rescan tenant assignments")

    monkeypatch.setattr(InvestigationStore, "_migrate_history_tenant_batch", unexpected_reconciliation)
    reopened = InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)

    with reopened._conn() as conn:
        plan = conn.execute(
            """EXPLAIN QUERY PLAN
               SELECT e.* FROM investigation_events e
               JOIN investigations i ON i.id=e.investigation_id
               WHERE e.investigation_id=? AND i.tenant_id=?
               ORDER BY e.created_at ASC, e.sequence ASC""",
            (investigation_id, "tenant-a"),
        ).fetchall()
    assert any("idx_inv_events_investigation_order" in str(row["detail"]) for row in plan)


def test_current_history_schema_reopens_without_reconciling_every_tenant_row(tmp_path, monkeypatch):
    db_path = tmp_path / "current-history.db"
    initial = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    initial.start("Already migrated", tenant_id="tenant-a")

    def unexpected_reconciliation(self):
        raise AssertionError("current-schema startup must not rewrite tenant assignments")

    monkeypatch.setattr(InvestigationStore, "_migrate_history_tenant_batch", unexpected_reconciliation)

    reopened = InvestigationStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    assert reopened.stats(tenant_id="tenant-a")["total"] == 1


def test_history_upgrade_adds_run_lease_before_creating_its_index(tmp_path):
    db_path = tmp_path / "pre-lease-history.db"
    runtime_settings = Settings(_env_file=None)
    store = InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)
    investigation_id = store.start("Legacy run lease")
    run_id = store.start_run(
        investigation_id,
        run_type=InvestigationRunType.INITIAL,
        lease_seconds=60,
    )
    with store._conn() as conn:
        conn.execute("DROP INDEX idx_inv_runs_investigation_active_lease")
        conn.execute("ALTER TABLE investigation_runs DROP COLUMN lease_expires_at")

    reopened = InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)

    with reopened._conn() as conn:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(investigation_runs)")}
        indexes = {str(row[1]) for row in conn.execute("PRAGMA index_list(investigation_runs)")}
        run = conn.execute(
            "SELECT started_at, lease_expires_at FROM investigation_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
    assert "lease_expires_at" in columns
    assert "idx_inv_runs_investigation_active_lease" in indexes
    assert run is not None
    assert run["lease_expires_at"] == run["started_at"]


def test_history_run_lease_upgrade_is_bounded_and_restartable(tmp_path, monkeypatch):
    db_path = tmp_path / "pre-lease-history-large.db"
    runtime_settings = Settings(_env_file=None)
    store = InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)
    investigation_id = store.start("Legacy run lease batch")
    with store._conn() as conn:
        conn.executemany(
            """INSERT INTO investigation_runs (
                   run_id, investigation_id, run_type, status, started_at
               ) VALUES (?, ?, 'initial', 'running', ?)""",
            [(f"legacy-run-{index:04d}", investigation_id, float(index + 1)) for index in range(1_201)],
        )
        conn.execute("DROP INDEX idx_inv_runs_investigation_active_lease")
        conn.execute("ALTER TABLE investigation_runs DROP COLUMN lease_expires_at")

    original_runner = InvestigationStore._run_history_run_lease_migration

    def migrate_one_batch_then_stop(self):
        migrated, completed = self._migrate_history_run_lease_batch()
        assert migrated == 500
        assert completed is False
        raise RuntimeError("simulated lease migration interruption")

    monkeypatch.setattr(InvestigationStore, "_run_history_run_lease_migration", migrate_one_batch_then_stop)
    with pytest.raises(RuntimeError, match="simulated lease migration interruption"):
        InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)

    with sqlite3.connect(db_path) as conn:
        migrated = conn.execute(
            "SELECT COUNT(*) FROM investigation_runs WHERE status='running' AND lease_expires_at>0"
        ).fetchone()[0]
        pending = conn.execute(
            "SELECT cursor FROM history_migration_progress WHERE migration_name='history_run_lease_backfill_v1'"
        ).fetchone()
        current = conn.execute(
            "SELECT 1 FROM history_schema_metadata WHERE migration_name='history_tenant_assignments_v2'"
        ).fetchone()
    assert migrated == 500
    assert pending is not None
    assert current is None

    monkeypatch.setattr(InvestigationStore, "_run_history_run_lease_migration", original_runner)
    InvestigationStore(db_path=db_path, runtime_settings=runtime_settings)

    with sqlite3.connect(db_path) as conn:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM investigation_runs WHERE status='running' AND lease_expires_at=0"
        ).fetchone()[0]
        pending = conn.execute(
            "SELECT 1 FROM history_migration_progress WHERE migration_name='history_run_lease_backfill_v1'"
        ).fetchone()
    assert remaining == 0
    assert pending is None


def test_history_cursor_pagination_is_stable_for_equal_timestamps(tmp_path):
    store = InvestigationStore(
        db_path=tmp_path / "cursor-history.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    investigation_ids = [store.start(f"Investigation {index}", tenant_id="tenant-a") for index in range(5)]
    with store._conn() as conn:
        conn.execute("UPDATE investigations SET started_at=100.0 WHERE tenant_id='tenant-a'")

    first = store.list_recent(limit=2, tenant_id="tenant-a")
    second = store.list_recent(
        limit=2,
        tenant_id="tenant-a",
        before_started_at=first[-1]["started_at"],
        before_id=first[-1]["id"],
    )
    third = store.list_recent(
        limit=2,
        tenant_id="tenant-a",
        before_started_at=second[-1]["started_at"],
        before_id=second[-1]["id"],
    )

    observed = [item["id"] for page in (first, second, third) for item in page]
    assert observed == sorted(investigation_ids, reverse=True)
    assert len(observed) == len(set(observed))


def test_history_filtered_pages_use_tenant_composite_indexes(tmp_path):
    store = InvestigationStore(
        db_path=tmp_path / "indexed-history.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with store._conn() as conn:
        status_plan = conn.execute(
            """EXPLAIN QUERY PLAN SELECT * FROM investigations
               WHERE tenant_id=? AND status=?
               ORDER BY started_at DESC, id DESC LIMIT ?""",
            ("tenant-a", "success", 20),
        ).fetchall()
        user_plan = conn.execute(
            """EXPLAIN QUERY PLAN SELECT * FROM investigations
               WHERE tenant_id=? AND user_id=?
               ORDER BY started_at DESC, id DESC LIMIT ?""",
            ("tenant-a", "operator", 20),
        ).fetchall()

    assert any("idx_inv_tenant_status_started" in str(row["detail"]) for row in status_plan)
    assert any("idx_inv_tenant_user_started" in str(row["detail"]) for row in user_plan)


def test_compare_revisions_scopes_both_contract_lookups(monkeypatch):
    contract = _contract()
    calls: list[tuple[int | None, str | None]] = []
    store = object.__new__(InvestigationStore)

    def get_contract(
        investigation_id: str,
        revision: int | None = None,
        *,
        tenant_id: str | None = None,
    ):
        assert investigation_id == "inv-a"
        calls.append((revision, tenant_id))
        return contract

    monkeypatch.setattr(store, "get_contract", get_contract)

    comparison = store.compare_revisions("inv-a", 1, 2, tenant_id="tenant-a")

    assert comparison is not None
    assert calls == [(1, "tenant-a"), (2, "tenant-a")]


class _TrackingBundleStore:
    def __init__(self) -> None:
        self.contract = _contract()
        self.calls: list[tuple[str, str | None]] = []

    def get_contract(self, _investigation_id, _revision=None, *, tenant_id=None):
        self.calls.append(("contract", tenant_id))
        return self.contract

    def get_snapshot(self, _investigation_id, _revision=None, *, tenant_id=None):
        self.calls.append(("snapshot", tenant_id))
        return None

    def list_revisions(self, _investigation_id, *, tenant_id=None):
        self.calls.append(("revisions", tenant_id))
        return [{"revision": self.contract.investigation.revision}]

    def compare_revisions(self, _investigation_id, _left, _right, *, tenant_id=None):
        self.calls.append(("compare", tenant_id))
        return {}


def test_bundle_build_and_export_scope_every_lookup(tmp_path):
    build_store = _TrackingBundleStore()
    build_investigation_bundle(build_store, "inv-a", tenant_id="tenant-a")  # type: ignore[arg-type]
    assert build_store.calls == [
        ("contract", "tenant-a"),
        ("snapshot", "tenant-a"),
        ("revisions", "tenant-a"),
    ]

    export_store = _TrackingBundleStore()
    output = tmp_path / "bundle.tar.gz"
    export_investigation_bundle(
        export_store,  # type: ignore[arg-type]
        "inv-a",
        output,
        tenant_id="tenant-a",
    )
    assert output.exists()
    assert export_store.calls == [
        ("contract", "tenant-a"),
        ("snapshot", "tenant-a"),
        ("revisions", "tenant-a"),
    ]


def test_bundle_requires_a_concrete_tenant_for_wildcard_history(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    store.persist_contract_revision(_contract(investigation_id, tenant_id="tenant-a"))

    with pytest.raises(ValueError, match="tenant"):
        build_investigation_bundle(store, investigation_id)

    assert build_investigation_bundle(store, investigation_id, tenant_id="tenant-a")


def test_history_stats_rejects_cross_tenant_requests_in_pinned_runtime(tmp_path):
    store = InvestigationStore(
        db_path=tmp_path / "history.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    store.start("Tenant A investigation", tenant_id="tenant-a")

    assert store.stats()["total"] == 1
    with pytest.raises(ValueError, match="Tenant access denied"):
        store.stats(tenant_id="tenant-b")


def test_wildcard_history_id_mutations_require_an_explicit_tenant(tmp_path):
    store = InvestigationStore(
        db_path=tmp_path / "history.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    run_id = store.start_run(
        investigation_id,
        run_type=InvestigationRunType.INITIAL,
        tenant_id="tenant-a",
    )
    calls = [
        lambda: store.record_intent(investigation_id, summary="Blocked intent"),
        lambda: store.record_discovery(investigation_id, datasources_found=3),
        lambda: store.record_queries(investigation_id, panel_count=3),
        lambda: store.record_validation(investigation_id, final_panel_count=3),
        lambda: store.record_stage(
            investigation_id,
            "ranking",
            status="passed",
            reason_code="blocked_stage",
        ),
        lambda: store.finish(investigation_id, status="success"),
        lambda: store.start_run(investigation_id, run_type=InvestigationRunType.REPLAY),
        lambda: store.append_event(investigation_id, run_id, "blocked_event"),
        lambda: store.complete_run(run_id, status="completed"),
    ]

    for call in calls:
        with pytest.raises(ValueError, match="tenant"):
            call()

    record = store.get(investigation_id, tenant_id="tenant-a")
    assert record is not None
    assert record["intent_summary"] == ""
    assert record["datasources_found"] == 0
    assert record["panel_count"] == 0
    assert record["status"] == "running"
    assert [event["event_type"] for event in store.list_events(investigation_id, tenant_id="tenant-a")] == [
        "run_started"
    ]
    assert store.list_runs(investigation_id, tenant_id="tenant-a")[0]["status"] == "running"


def test_start_run_rolls_back_row_when_start_event_fails(tmp_path, monkeypatch):
    store = InvestigationStore(
        db_path=tmp_path / "history.db",
        runtime_settings=Settings(_env_file=None),
    )
    investigation_id = store.start("Atomic start event")
    append_event = store._append_event_in_transaction

    def fail_after_event(*args, **kwargs):
        append_event(*args, **kwargs)
        raise RuntimeError("simulated run-start event failure")

    monkeypatch.setattr(store, "_append_event_in_transaction", fail_after_event)

    with pytest.raises(RuntimeError, match="simulated run-start event failure"):
        store.start_run(investigation_id, run_type=InvestigationRunType.INITIAL)

    assert store.list_runs(investigation_id) == []
    assert store.list_events(investigation_id) == []


def test_complete_run_rolls_back_state_when_terminal_event_fails(tmp_path, monkeypatch):
    store = InvestigationStore(
        db_path=tmp_path / "history.db",
        runtime_settings=Settings(_env_file=None),
    )
    investigation_id = store.start("Atomic completion event")
    run_id = store.start_run(investigation_id, run_type=InvestigationRunType.INITIAL)
    append_event = store._append_event_in_transaction

    def fail_after_event(*args, **kwargs):
        append_event(*args, **kwargs)
        raise RuntimeError("simulated run-terminal event failure")

    monkeypatch.setattr(store, "_append_event_in_transaction", fail_after_event)

    with pytest.raises(RuntimeError, match="simulated run-terminal event failure"):
        store.complete_run(run_id, status="completed")

    assert store.list_runs(investigation_id)[0]["status"] == "running"
    assert [event["event_type"] for event in store.list_events(investigation_id)] == ["run_started"]


def test_history_mutations_enforce_tenant_and_run_ownership(tmp_path):
    store = InvestigationStore(
        db_path=tmp_path / "history.db",
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True),
    )
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    other_investigation_id = store.start("Other tenant A investigation", tenant_id="tenant-a")
    tenant_b_investigation_id = store.start("Tenant B investigation", tenant_id="tenant-b")
    run_id = store.start_run(
        investigation_id,
        run_type=InvestigationRunType.INITIAL,
        tenant_id="tenant-a",
    )
    other_run_id = store.start_run(
        other_investigation_id,
        run_type=InvestigationRunType.INITIAL,
        tenant_id="tenant-a",
    )
    tenant_b_run_id = store.start_run(
        tenant_b_investigation_id,
        run_type=InvestigationRunType.INITIAL,
        tenant_id="tenant-b",
    )

    store.record_intent(investigation_id, summary="Blocked intent", tenant_id="tenant-b")
    store.record_discovery(investigation_id, datasources_found=3, tenant_id="tenant-b")
    store.record_queries(investigation_id, panel_count=3, tenant_id="tenant-b")
    store.record_validation(investigation_id, final_panel_count=3, tenant_id="tenant-b")
    store.record_stage(
        investigation_id,
        "ranking",
        status="passed",
        reason_code="blocked_stage",
        tenant_id="tenant-b",
    )
    store.finish(investigation_id, status="success", tenant_id="tenant-b")
    with pytest.raises(ValueError, match="selected tenant"):
        store.start_run(
            investigation_id,
            run_type=InvestigationRunType.REPLAY,
            tenant_id="tenant-b",
        )

    store.append_event(
        investigation_id,
        other_run_id,
        "wrong_investigation",
        tenant_id="tenant-a",
    )
    store.append_event(
        investigation_id,
        tenant_b_run_id,
        "wrong_tenant_and_investigation",
        tenant_id="tenant-a",
    )
    store.append_event(
        investigation_id,
        run_id,
        "wrong_tenant",
        tenant_id="tenant-b",
    )
    store.complete_run(run_id, status="completed", tenant_id="tenant-b")

    record = store.get(investigation_id, tenant_id="tenant-a")
    assert record is not None
    assert record["intent_summary"] == ""
    assert record["datasources_found"] == 0
    assert record["panel_count"] == 0
    assert record["stage_outcomes"] == {}
    assert record["status"] == "running"
    assert [event["event_type"] for event in store.list_events(investigation_id, tenant_id="tenant-a")] == [
        "run_started"
    ]
    assert store.list_runs(investigation_id, tenant_id="tenant-a")[0]["status"] == "running"

    with pytest.raises(StaleRevisionError, match="run does not belong"):
        store.persist_contract_revision(
            _contract(investigation_id, tenant_id="tenant-a"),
            run_id=other_run_id,
        )
    assert store.list_revisions(investigation_id, tenant_id="tenant-a") == []

    store.persist_contract_revision(
        _contract(investigation_id, tenant_id="tenant-a"),
        run_id=run_id,
    )
    store.append_event(investigation_id, run_id, "owned_event", tenant_id="tenant-a")
    store.complete_run(run_id, status="completed", tenant_id="tenant-a")

    assert [event["event_type"] for event in store.list_events(investigation_id, tenant_id="tenant-a")] == [
        "run_started",
        "revision_persisted",
        "owned_event",
        "run_completed",
    ]
    assert store.list_runs(investigation_id, tenant_id="tenant-a")[0]["status"] == "completed"


def test_history_child_records_require_and_enforce_the_selected_tenant(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    store.persist_contract_revision(_contract(investigation_id, tenant_id="tenant-a"))
    run_id = store.start_run(
        investigation_id,
        run_type=InvestigationRunType.REPLAY,
        base_revision=1,
        tenant_id="tenant-a",
    )
    store.append_event(investigation_id, run_id, "tenant_a_event", {}, tenant_id="tenant-a")
    candidate = store.create_knowledge_candidate(
        investigation_id,
        revision=1,
        correction_text="Tenant A correction",
        tenant_id="tenant-a",
    )
    assert candidate is not None

    with pytest.raises(ValueError, match="tenant"):
        store.list_runs(investigation_id)
    assert store.list_runs(investigation_id, tenant_id="tenant-b") == []
    assert store.list_events(investigation_id, tenant_id="tenant-b") == []
    assert store.list_knowledge_candidates(investigation_id, tenant_id="tenant-b") == []
    assert (
        store.review_knowledge_candidate(
            investigation_id,
            candidate.id,
            approved=True,
            reviewed_by="tenant-b-reviewer",
            tenant_id="tenant-b",
        )
        is None
    )
    assert store.apply_knowledge_candidate(investigation_id, candidate.id, tenant_id="tenant-b") is None

    own_candidates = store.list_knowledge_candidates(investigation_id, tenant_id="tenant-a")
    assert own_candidates[0].status.value == "pending_review"
    assert store.list_runs(investigation_id, tenant_id="tenant-a")[-1]["run_id"] == run_id
    assert any(
        event["event_type"] == "tenant_a_event" for event in store.list_events(investigation_id, tenant_id="tenant-a")
    )


def test_wildcard_history_store_requires_a_tenant_for_direct_reads(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    with pytest.raises(ValueError, match="tenant"):
        store.start("Ownerless investigation")
    investigation_id = store.start("Tenant A investigation", tenant_id="tenant-a")
    store.persist_contract_revision(_contract(investigation_id, tenant_id="tenant-a"))

    direct_reads = [
        lambda: store.get(investigation_id),
        lambda: store.get_by_dashboard("dashboard-a"),
        lambda: store.list_recent(),
        lambda: store.get_contract(investigation_id),
        lambda: store.get_snapshot(investigation_id),
        lambda: store.list_revisions(investigation_id),
        lambda: store.compare_revisions(investigation_id, 1, 1),
    ]
    for direct_read in direct_reads:
        with pytest.raises(ValueError, match="tenant"):
            direct_read()


def test_legacy_migration_is_scoped_before_reading_or_persisting(tmp_path):
    settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)
    store = InvestigationStore(db_path=tmp_path / "history.db", runtime_settings=settings)
    investigation_id = store.start("Tenant A legacy investigation", tenant_id="tenant-a")
    store.finish(investigation_id, status="success", tenant_id="tenant-a")

    assert store.migrate_legacy_investigation(investigation_id, tenant_id="tenant-b") is None
    assert store.get_contract(investigation_id, tenant_id="tenant-a") is None

    migrated = store.migrate_legacy_investigation(investigation_id, tenant_id="tenant-a")
    assert migrated is not None
    assert migrated.request.scope.tenant_id == "tenant-a"


def test_history_api_migration_requires_apply_permission_before_mutation(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        knowledge_permissions="knowledge.read",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    store = InvestigationStore(
        db_path=Path(runtime_settings.history_db_path),
        runtime_settings=runtime_settings,
    )
    investigation_id = store.start("Legacy migration permission", tenant_id="tenant-a")
    store.finish(investigation_id, status="success", tenant_id="tenant-a")
    client = TestClient(create_app(runtime_settings=runtime_settings))

    response = client.post(f"/api/v1/investigations/{investigation_id}/migrate")

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.apply"
    assert store.get_contract(investigation_id, tenant_id="tenant-a") is None


class _FakeHistory:
    def __init__(self) -> None:
        self.contract = _contract()
        self.calls: list[tuple[str, str | None]] = []

    def list_recent(self, **kwargs):
        self.calls.append(("list", kwargs.get("tenant_id")))
        return []

    def get(self, _investigation_id, *, tenant_id=None):
        self.calls.append(("show", tenant_id))
        return {"id": "inv-a", "tenant_id": tenant_id, "status": "success", "prompt": "Prompt"}

    def get_contract(self, _investigation_id, _revision=None, *, tenant_id=None):
        self.calls.append(("contract", tenant_id))
        return self.contract

    def compare_revisions(self, _investigation_id, _left, _right, *, tenant_id=None):
        self.calls.append(("compare", tenant_id))
        return {"same_output": True}

    def replay_contract(self, _investigation_id, _revision=None, *, tenant_id=None, **_kwargs):
        self.calls.append(("replay", tenant_id))
        return self.contract


class _FakeStores:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.history_store = _FakeHistory()

    def history(self):
        return self.history_store

    def knowledge(self):
        raise AssertionError("exact replay must not resolve current knowledge")


@pytest.mark.parametrize(
    "arguments",
    [
        ["history", "list"],
        ["history", "show", "inv-a"],
        ["history", "contract", "inv-a"],
        ["history", "compare", "inv-a", "1", "2"],
        ["history", "export", "inv-a"],
        ["history", "replay", "inv-a"],
    ],
)
def test_history_cli_requires_tenant_in_wildcard_mode(monkeypatch, arguments):
    stores = _FakeStores(Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True))
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code != 0
    assert "--tenant is required" in result.output
    assert stores.history_store.calls == []


def test_history_cli_scopes_all_commands_to_selected_tenant(tmp_path, monkeypatch):
    stores = _FakeStores(Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True))
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    exported: list[str | None] = []

    def fake_export(_store, _investigation_id, _output, *, revision=None, tenant_id=None):
        assert revision is None
        exported.append(tenant_id)

    monkeypatch.setattr("tacit.investigation_bundle.export_investigation_bundle", fake_export)
    runner = CliRunner()
    commands = [
        ["history", "list", "--tenant", "tenant-a"],
        ["history", "show", "inv-a", "--tenant", "tenant-a"],
        ["history", "contract", "inv-a", "--tenant", "tenant-a"],
        ["history", "compare", "inv-a", "1", "2", "--tenant", "tenant-a"],
        [
            "history",
            "export",
            "inv-a",
            "--tenant",
            "tenant-a",
            "--output",
            str(tmp_path / "bundle.tar.gz"),
        ],
        ["history", "replay", "inv-a", "--tenant", "tenant-a"],
    ]

    for command in commands:
        result = runner.invoke(cli, command)
        assert result.exit_code == 0, result.output

    assert stores.history_store.calls == [
        ("list", "tenant-a"),
        ("show", "tenant-a"),
        ("contract", "tenant-a"),
        ("compare", "tenant-a"),
        ("contract", "tenant-a"),
        ("show", "tenant-a"),
        ("replay", "tenant-a"),
    ]
    assert exported == ["tenant-a"]


def test_history_export_requires_export_permission(tmp_path, monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    called = False

    def fake_export(*_args: Any, **_kwargs: Any):
        nonlocal called
        called = True

    monkeypatch.setattr("tacit.investigation_bundle.export_investigation_bundle", fake_export)

    result = CliRunner().invoke(
        cli,
        ["history", "export", "inv-a", "--output", str(tmp_path / "bundle.tar.gz")],
    )

    assert result.exit_code != 0
    assert "knowledge.export" in result.output
    assert called is False


@pytest.mark.parametrize(
    "arguments",
    [
        ["history", "list"],
        ["history", "show", "inv-a"],
        ["history", "contract", "inv-a"],
        ["history", "compare", "inv-a", "1", "2"],
        ["history", "replay", "inv-a"],
        ["history", "stats"],
        ["doctor"],
    ],
)
def test_history_cli_reads_require_read_permission(monkeypatch, arguments):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.apply",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code != 0
    assert "knowledge.read" in result.output
    assert stores.history_store.calls == []


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/investigations",
        "/api/v1/investigations/stats",
        "/api/v1/investigations/inv-a/revisions",
        "/api/v1/investigations/inv-a/runs",
        "/api/v1/investigations/inv-a/compare?left=1&right=2",
        "/api/v1/investigations/inv-a",
        "/api/v1/feedback/stats",
        "/api/v1/feedback/analysis",
        "/api/v1/feedback/dashboard-a",
    ],
)
def test_history_and_feedback_api_reads_require_knowledge_read(tmp_path, path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_permissions="knowledge.apply",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    client = TestClient(create_app(runtime_settings=runtime_settings))

    response = client.get(path)

    assert response.status_code == 403
    assert response.json()["detail"] == "Missing permission: knowledge.read"


def test_history_current_replay_cli_requires_apply_permission(monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)

    result = CliRunner().invoke(
        cli,
        ["history", "replay", "inv-a", "--mode", "current_engine"],
    )

    assert result.exit_code != 0
    assert "knowledge.apply" in result.output
    assert stores.history_store.calls == []


def test_assessment_export_requires_export_permission(tmp_path, monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.read",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    called = False

    def fake_export(*_args: Any, **_kwargs: Any):
        nonlocal called
        called = True

    monkeypatch.setattr("tacit.export_report.export_assessment_report", fake_export)

    result = CliRunner().invoke(
        cli,
        ["export-report", "--output", str(tmp_path / "assessment.tar.gz")],
    )

    assert result.exit_code != 0
    assert "knowledge.export" in result.output
    assert called is False


def test_assessment_export_requires_read_permission(tmp_path, monkeypatch):
    stores = _FakeStores(
        Settings(
            _env_file=None,
            knowledge_tenant_id="tenant-a",
            knowledge_permissions="knowledge.export",
        )
    )
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    called = False

    def fake_export(*_args: Any, **_kwargs: Any):
        nonlocal called
        called = True

    monkeypatch.setattr("tacit.export_report.export_assessment_report", fake_export)

    result = CliRunner().invoke(
        cli,
        ["export-report", "--output", str(tmp_path / "assessment.tar.gz")],
    )

    assert result.exit_code != 0
    assert "knowledge.read" in result.output
    assert called is False


def test_history_cli_rejects_pinned_tenant_override_before_lookup(monkeypatch):
    stores = _FakeStores(Settings(_env_file=None, knowledge_tenant_id="tenant-a"))
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)

    result = CliRunner().invoke(
        cli,
        ["history", "list", "--tenant", "tenant-b"],
    )

    assert result.exit_code != 0
    assert "Tenant access denied" in result.output
    assert stores.history_store.calls == []
