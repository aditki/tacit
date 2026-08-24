from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from tacit.config import Settings
from tacit.history import InvestigationStore
from tacit.investigation_contract import InvestigationRunType
from tacit.knowledge.models import KnowledgeScope
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.runtime_ownership import RuntimeOwnershipMismatchError
from tacit.signals import SignalStore

_DEFAULT_OWNER_MARKER = "default_owner_v1"


def _database_snapshot(path: Path) -> tuple[str, list[str]]:
    with sqlite3.connect(path) as conn:
        journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).casefold()
        dump = list(conn.iterdump())
    return journal_mode, dump


def _file_snapshot(path: Path) -> dict[str, bytes]:
    return {
        candidate.name: candidate.read_bytes()
        for candidate in path.parent.iterdir()
        if candidate.name == path.name or candidate.name.startswith(f"{path.name}-")
    }


_SUBPROCESS_OPEN_SCRIPT = r"""
import json
import sys
import time
from pathlib import Path

from tacit.config import Settings
from tacit.knowledge.repository import KnowledgeRepository

database_path, tenant_id, ready_path, start_path, result_path = sys.argv[1:]
Path(ready_path).write_text("ready")
deadline = time.monotonic() + 30
while not Path(start_path).exists():
    if time.monotonic() >= deadline:
        Path(result_path).write_text(json.dumps({"status": "timeout"}))
        raise SystemExit(2)
    time.sleep(0.005)
try:
    repository = KnowledgeRepository(
        Path(database_path),
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_id),
    )
    repository.append_event(
        "first_open_canary",
        tenant_id=tenant_id,
        dimensions={"reason_code": "first_open_canary"},
    )
except Exception as exc:
    result = {"status": "error", "exception_class": type(exc).__name__}
else:
    result = {"status": "ok"}
Path(result_path).write_text(json.dumps(result, sort_keys=True))
"""


def _race_knowledge_repository_openers(
    tmp_path: Path,
    owners: tuple[str, str],
) -> tuple[Path, list[dict[str, str]]]:
    db_path = tmp_path / "raced-knowledge.db"
    start_path = tmp_path / "start"
    processes: list[subprocess.Popen[str]] = []
    result_paths: list[Path] = []
    ready_paths: list[Path] = []
    environment = dict(os.environ)
    for index, owner in enumerate(owners):
        ready_path = tmp_path / f"ready-{index}"
        result_path = tmp_path / f"result-{index}.json"
        ready_paths.append(ready_path)
        result_paths.append(result_path)
        processes.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    _SUBPROCESS_OPEN_SCRIPT,
                    str(db_path),
                    owner,
                    str(ready_path),
                    str(start_path),
                    str(result_path),
                ],
                cwd=Path(__file__).resolve().parents[2],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        )

    deadline = time.monotonic() + 20
    while not all(path.exists() for path in ready_paths):
        if time.monotonic() >= deadline:
            for process in processes:
                process.kill()
            pytest.fail("knowledge repository subprocesses did not reach the start barrier")
        time.sleep(0.01)
    start_path.write_text("start")

    diagnostics: list[str] = []
    for process in processes:
        try:
            stdout, stderr = process.communicate(timeout=45)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            pytest.fail(f"knowledge repository subprocess timed out: stdout={stdout!r} stderr={stderr!r}")
        assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
        diagnostics.append(f"{stdout}\n{stderr}")
    for rendered in diagnostics:
        assert str(db_path) not in rendered
        assert all(owner not in rendered for owner in owners)
    return db_path, [json.loads(path.read_text()) for path in result_paths]


def test_knowledge_repository_rejects_pinned_owner_mismatch_without_side_effects(tmp_path: Path) -> None:
    path_canary = "PRIVATE-KNOWLEDGE-PATH-CANARY"
    recorded_owner = "PRIVATE-RECORDED-TENANT-CANARY"
    configured_owner = "PRIVATE-CONFIGURED-TENANT-CANARY"
    db_path = tmp_path / path_canary / "signals.db"
    SignalStore(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=recorded_owner),
    )
    before_database = _database_snapshot(db_path)
    before_files = _file_snapshot(db_path)

    with capture_logs() as logs, pytest.raises(RuntimeError, match="owner") as exc_info:
        KnowledgeRepository(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=configured_owner),
        )

    rendered = f"{logs!r} {exc_info.value!s}"
    assert _database_snapshot(db_path) == before_database
    assert _file_snapshot(db_path) == before_files
    assert path_canary not in rendered
    assert str(db_path) not in rendered
    assert recorded_owner not in rendered
    assert configured_owner not in rendered
    rejected = [entry for entry in logs if entry.get("event") == "knowledge_repository_owner_rejected"]
    assert len(rejected) == 1
    assert rejected[0]["reason_code"] == "pinned_owner_mismatch"
    assert len(str(rejected[0]["database_fingerprint"])) == 16
    assert len(str(rejected[0]["configured_owner_fingerprint"])) == 16
    assert len(str(rejected[0]["recorded_owner_fingerprint"])) == 16


def test_fresh_knowledge_schema_failure_rolls_back_role_owner_and_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tacit.knowledge.repository as repository_module

    db_path = tmp_path / "atomic-first-open.db"
    tenant_id = "tenant-atomic-first-open"

    def fail_after_schema_write(conn: sqlite3.Connection, _script: str) -> None:
        conn.execute("CREATE TABLE losing_knowledge_schema (id INTEGER PRIMARY KEY)")
        raise RuntimeError("injected knowledge schema failure")

    monkeypatch.setattr(repository_module, "_execute_schema_statements", fail_after_schema_write)
    with pytest.raises(RuntimeError, match="injected knowledge schema failure"):
        KnowledgeRepository(
            db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_id),
        )

    with sqlite3.connect(db_path) as conn:
        user_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
    assert user_tables == set()


def test_knowledge_service_rejects_repository_owner_mismatch_without_side_effects(tmp_path: Path) -> None:
    tenant_a = "PRIVATE-SERVICE-OWNER-A"
    tenant_b = "PRIVATE-SERVICE-OWNER-B"
    db_path = tmp_path / "service-owner.db"
    repository = KnowledgeRepository(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_a),
    )
    before_database = _database_snapshot(db_path)
    before_files = _file_snapshot(db_path)

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        KnowledgeService(
            repository,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_b),
        )

    assert _database_snapshot(db_path) == before_database
    assert _file_snapshot(db_path) == before_files
    assert tenant_a not in str(exc_info.value)
    assert tenant_b not in str(exc_info.value)


def test_repository_revalidates_owner_after_acquiring_its_write_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_a = "tenant-write-lock-a"
    tenant_b = "tenant-write-lock-b"
    db_path = tmp_path / "write-lock-owner.db"
    repository = KnowledgeRepository(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_a),
    )
    original_check = repository._require_owner_on_connection
    checks = 0

    def change_owner_after_preflight(conn: sqlite3.Connection) -> None:
        nonlocal checks
        checks += 1
        original_check(conn)
        if checks == 2:
            conn.execute(
                "UPDATE signal_tenant_migration_metadata SET value=? WHERE key=?",
                (tenant_b, _DEFAULT_OWNER_MARKER),
            )
            conn.commit()

    monkeypatch.setattr(repository, "_require_owner_on_connection", change_owner_after_preflight)
    with pytest.raises(RuntimeError, match="owner"):
        repository.append_event("must_not_persist", tenant_id=tenant_a)

    assert checks == 3
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT 1 FROM knowledge_events WHERE event_type='must_not_persist'").fetchone() is None
        conn.execute(
            "UPDATE signal_tenant_migration_metadata SET value=? WHERE key=?",
            (tenant_a, _DEFAULT_OWNER_MARKER),
        )


def test_nested_read_transaction_revalidates_owner_when_upgrading_to_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_a = "tenant-read-upgrade-a"
    tenant_b = "tenant-read-upgrade-b"
    db_path = tmp_path / "read-upgrade-owner.db"
    repository = KnowledgeRepository(
        db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_a),
    )
    original_check = repository._require_owner_on_connection
    checks = 0

    def change_owner_after_connection_preflight(conn: sqlite3.Connection) -> None:
        nonlocal checks
        checks += 1
        original_check(conn)
        if checks == 2:
            with sqlite3.connect(db_path) as writer:
                writer.execute(
                    "UPDATE signal_tenant_migration_metadata SET value=? WHERE key=?",
                    (tenant_b, _DEFAULT_OWNER_MARKER),
                )

    monkeypatch.setattr(repository, "_require_owner_on_connection", change_owner_after_connection_preflight)
    with repository.read_transaction():
        with pytest.raises(RuntimeError, match="owner"):
            repository.append_event("must_not_persist", tenant_id=tenant_a)

    assert checks == 3
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT 1 FROM knowledge_events WHERE event_type='must_not_persist'").fetchone() is None
        conn.execute(
            "UPDATE signal_tenant_migration_metadata SET value=? WHERE key=?",
            (tenant_a, _DEFAULT_OWNER_MARKER),
        )


def test_read_transaction_rejects_direct_sql_write_bypasses(tmp_path: Path) -> None:
    repository = KnowledgeRepository(tmp_path / "query-only-read.db")

    with repository.read_transaction() as conn:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("""INSERT INTO knowledge_events (
                       event_id, tenant_id, event_type, payload_json, created_at
                   ) VALUES ('direct-write', 'default', 'must_not_persist', '{}', 0)""")

    assert repository.list_events() == []


def test_nested_repository_write_restores_query_only_protection(tmp_path: Path) -> None:
    repository = KnowledgeRepository(tmp_path / "query-only-nested-write.db")

    with repository.read_transaction() as conn:
        repository.append_event("nested-write", tenant_id="default")
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("""INSERT INTO knowledge_events (
                       event_id, tenant_id, event_type, payload_json, created_at
                   ) VALUES ('direct-write-after-nested', 'default', 'must_not_persist', '{}', 0)""")

    assert [event["event_type"] for event in repository.list_events()] == ["nested-write"]


def test_bound_knowledge_transaction_revalidates_owner_before_writes(tmp_path: Path) -> None:
    tenant_a = "tenant-bound-a"
    tenant_b = "tenant-bound-b"
    db_path = tmp_path / "bound-owner.db"
    settings = Settings(_env_file=None, knowledge_tenant_id=tenant_a)
    store = SignalStore(db_path, runtime_settings=settings)
    repository = KnowledgeRepository(db_path, runtime_settings=settings)

    with store.transaction() as conn:
        conn.execute(
            "UPDATE signal_tenant_migration_metadata SET value=? WHERE key=?",
            (tenant_b, _DEFAULT_OWNER_MARKER),
        )
        with pytest.raises(RuntimeError, match="owner"):
            with repository.bind_transaction_connection(conn):
                repository.append_event("must_not_persist", tenant_id=tenant_a)
        assert conn.execute("SELECT 1 FROM knowledge_events WHERE event_type='must_not_persist'").fetchone() is None
        conn.execute(
            "UPDATE signal_tenant_migration_metadata SET value=? WHERE key=?",
            (tenant_a, _DEFAULT_OWNER_MARKER),
        )


def test_real_subprocess_conflicting_knowledge_first_openers_leave_only_winner_state(tmp_path: Path) -> None:
    owners = ("tenant-knowledge-race-a", "tenant-knowledge-race-b")
    db_path, results = _race_knowledge_repository_openers(tmp_path, owners)

    assert sorted(result["status"] for result in results) == ["error", "ok"]
    winner_index = next(index for index, result in enumerate(results) if result["status"] == "ok")
    winner = owners[winner_index]
    loser = owners[1 - winner_index]
    with sqlite3.connect(db_path) as conn:
        owner = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        ).fetchone()
        events = conn.execute("SELECT tenant_id FROM knowledge_events WHERE event_type='first_open_canary'").fetchall()
        marker_values = {
            str(row[0]) for row in conn.execute("SELECT value FROM signal_tenant_migration_metadata").fetchall()
        }

    assert owner == (winner,)
    assert events == [(winner,)]
    assert loser not in marker_values


def test_knowledge_and_history_init_diagnostics_redact_paths_and_tenants(tmp_path: Path) -> None:
    path_canary = "PRIVATE-STORE-PATH-CANARY"
    tenant_canary = "PRIVATE-STORE-TENANT-CANARY"
    knowledge_path = tmp_path / path_canary / "signals.db"
    history_path = tmp_path / path_canary / "history.db"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id=tenant_canary)

    with capture_logs() as logs:
        KnowledgeRepository(knowledge_path, runtime_settings=runtime_settings)
        InvestigationStore(history_path, runtime_settings=runtime_settings)

    rendered = repr(logs)
    assert path_canary not in rendered
    assert str(knowledge_path) not in rendered
    assert str(history_path) not in rendered
    assert tenant_canary not in rendered
    knowledge_init = [entry for entry in logs if entry.get("event") == "knowledge_repository_init"]
    history_init = [entry for entry in logs if entry.get("event") == "investigation_store_init"]
    assert knowledge_init and history_init
    assert all(entry["reason_code"] == "knowledge_repository_initialized" for entry in knowledge_init)
    assert all(entry["reason_code"] == "investigation_store_initialized" for entry in history_init)
    assert all(len(str(entry["database_fingerprint"])) == 16 for entry in [*knowledge_init, *history_init])


def test_knowledge_migration_failure_diagnostics_redact_record_identity(tmp_path: Path) -> None:
    path_canary = str(tmp_path / "PRIVATE-MIGRATION-PATH-CANARY" / "signals.db")
    tenant_canary = "PRIVATE-MIGRATION-TENANT-CANARY"
    record_canary = "PRIVATE-MIGRATION-RECORD-CANARY"

    with capture_logs() as logs, pytest.raises(RuntimeError) as exc_info:
        KnowledgeRepository._raise_invalid_migration_record(
            ValueError(f"invalid row at {path_canary} for {tenant_canary}"),
            migration_name="candidate_review_priority_v2",
            record_class="candidate",
            tenant_id=tenant_canary,
            row_id=record_canary,
        )

    rendered = f"{logs!r} {exc_info.value!s}"
    assert path_canary not in rendered
    assert tenant_canary not in rendered
    assert record_canary not in rendered
    failures = [entry for entry in logs if entry.get("event") == "knowledge_repository_migration_failed"]
    assert len(failures) == 1
    assert failures[0]["reason_code"] == "knowledge_migration_invalid_record"
    assert failures[0]["record_class"] == "candidate"
    assert int(failures[0]["record_count"]) == 1
    assert failures[0]["record_fingerprint"]
    assert failures[0]["failure_fingerprint"]


def test_knowledge_scope_and_history_recovery_diagnostics_redact_tenant_ids(tmp_path: Path) -> None:
    tenant_canary = "PRIVATE-RUNTIME-TENANT-CANARY"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id=tenant_canary)
    repository = KnowledgeRepository(
        tmp_path / "signals.db",
        runtime_settings=runtime_settings,
    )
    service = KnowledgeService(repository, runtime_settings=runtime_settings)
    history = InvestigationStore(
        tmp_path / "history.db",
        runtime_settings=runtime_settings,
    )
    investigation_id = history.start("checkout latency", tenant_id=tenant_canary)
    run_id = history.start_run(
        investigation_id,
        run_type=InvestigationRunType.INITIAL,
        tenant_id=tenant_canary,
    )
    with history._conn() as conn:
        conn.execute(
            "UPDATE investigation_runs SET lease_expires_at=? WHERE run_id=?",
            (time.time() - 1, run_id),
        )

    with capture_logs() as logs:
        service.create_snapshot(KnowledgeScope(tenant_id=tenant_canary))
        history.list_runs(investigation_id, tenant_id=tenant_canary)

    rendered = repr(logs)
    assert tenant_canary not in rendered
    scope_selected = [entry for entry in logs if entry.get("event") == "knowledge_snapshot_scope_selected"]
    recovered = [entry for entry in logs if entry.get("event") == "expired_investigation_runs_reconciled"]
    assert scope_selected and recovered
    assert all(len(str(entry["tenant_fingerprint"])) == 16 for entry in [*scope_selected, *recovered])
