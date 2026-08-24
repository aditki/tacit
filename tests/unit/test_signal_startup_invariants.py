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
from tacit.signals import SignalStore


def _database_snapshot(path) -> tuple[str, list[str]]:
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
from tacit.signals import SignalStore

database_path, tenant_id, ready_path, start_path, result_path = sys.argv[1:]
Path(ready_path).write_text("ready")
deadline = time.monotonic() + 30
while not Path(start_path).exists():
    if time.monotonic() >= deadline:
        Path(result_path).write_text(json.dumps({"status": "timeout"}))
        raise SystemExit(2)
    time.sleep(0.005)
try:
    SignalStore(
        db_path=Path(database_path),
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=tenant_id),
    )
except Exception as exc:
    result = {"status": "error", "exception_class": type(exc).__name__}
else:
    result = {"status": "ok"}
Path(result_path).write_text(json.dumps(result, sort_keys=True))
"""


def _race_signal_store_openers(tmp_path: Path, owners: tuple[str, str]) -> tuple[Path, list[dict[str, str]]]:
    db_path = tmp_path / "raced-signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_types (
                signal_type TEXT PRIMARY KEY, description TEXT NOT NULL DEFAULT '',
                category TEXT NOT NULL DEFAULT '', unit TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            INSERT INTO signal_types VALUES
                ('private_first_open_canary', 'private payload', 'custom', 'count', 1, 1);
        """)

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
            pytest.fail("signal opener subprocesses did not reach the start barrier")
        time.sleep(0.01)
    start_path.write_text("start")

    for process in processes:
        try:
            stdout, stderr = process.communicate(timeout=45)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            pytest.fail(f"signal opener subprocess timed out: stdout={stdout!r} stderr={stderr!r}")
        assert process.returncode == 0, f"stdout={stdout!r} stderr={stderr!r}"
    return db_path, [json.loads(path.read_text()) for path in result_paths]


def test_signal_owner_mismatch_is_rejected_before_wal_or_schema_mutation(tmp_path) -> None:
    db_path = tmp_path / "signal-owner-mismatch.db"
    recorded_owner = "tenant-recorded-canary"
    configured_owner = "tenant-configured-canary"
    SignalStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id=recorded_owner),
    )
    with sqlite3.connect(db_path) as conn:
        assert str(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).casefold() == "delete"
    before = _database_snapshot(db_path)
    before_files = _file_snapshot(db_path)

    with capture_logs() as logs, pytest.raises(RuntimeError, match="owner preflight") as exc_info:
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=configured_owner),
        )

    after = _database_snapshot(db_path)
    diagnostic_text = f"{exc_info.value!s} {logs!r}"
    assert after == before
    assert _file_snapshot(db_path) == before_files
    assert recorded_owner not in diagnostic_text
    assert configured_owner not in diagnostic_text
    rejected = [log for log in logs if log.get("event") == "signal_owner_preflight_rejected"]
    assert len(rejected) == 1
    assert rejected[0]["reason_code"] == "pinned_owner_mismatch"
    assert len(str(rejected[0]["recorded_owner_fingerprint"])) == 16
    assert len(str(rejected[0]["configured_owner_fingerprint"])) == 16


def test_same_owner_public_signal_store_reopen_converges(tmp_path) -> None:
    db_path = tmp_path / "same-owner.db"
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-owner")

    SignalStore(db_path=db_path, runtime_settings=runtime_settings)
    reopened = SignalStore(db_path=db_path, runtime_settings=runtime_settings)

    with reopened._conn() as conn:
        owners = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='default_owner_v1'"
        ).fetchall()
    assert [row["value"] for row in owners] == ["tenant-owner"]


def test_real_subprocess_same_owner_first_openers_converge(tmp_path) -> None:
    db_path, results = _race_signal_store_openers(tmp_path, ("tenant-owner", "tenant-owner"))

    assert results == [{"status": "ok"}, {"status": "ok"}]
    with sqlite3.connect(db_path) as conn:
        owner = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='default_owner_v1'"
        ).fetchone()
        custom = conn.execute(
            "SELECT tenant_id FROM tenant_signal_types WHERE signal_type='private_first_open_canary'"
        ).fetchone()
    assert owner == ("tenant-owner",)
    assert custom == ("tenant-owner",)


def test_real_subprocess_conflicting_first_openers_leave_only_winner_state(tmp_path) -> None:
    owners = ("tenant-race-a", "tenant-race-b")
    db_path, results = _race_signal_store_openers(tmp_path, owners)

    assert sorted(result["status"] for result in results) == ["error", "ok"]
    winner_index = next(index for index, result in enumerate(results) if result["status"] == "ok")
    winner = owners[winner_index]
    loser = owners[1 - winner_index]
    assert results[1 - winner_index]["exception_class"] == "RuntimeError"
    with sqlite3.connect(db_path) as conn:
        marker_rows = conn.execute("SELECT key, value FROM signal_tenant_migration_metadata").fetchall()
        tenant_values = [row[0] for row in conn.execute("""SELECT DISTINCT tenant_id FROM tenant_signal_types
                   UNION SELECT DISTINCT tenant_id FROM signal_metric_mappings""").fetchall()]
        custom = conn.execute(
            "SELECT tenant_id FROM tenant_signal_types WHERE signal_type='private_first_open_canary'"
        ).fetchone()

    assert custom == (winner,)
    assert winner in {value for _key, value in marker_rows}
    assert loser not in {value for _key, value in marker_rows}
    assert loser not in tenant_values

    before_retry = _file_snapshot(db_path)
    with pytest.raises(RuntimeError, match="owner preflight"):
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=loser),
        )
    assert _file_snapshot(db_path) == before_retry


def test_signal_startup_diagnostics_fingerprint_paths(tmp_path) -> None:
    path_canary = "PRIVATE-SIGNAL-PATH-CANARY"
    db_path = tmp_path / path_canary / "signals.db"

    with capture_logs() as logs:
        SignalStore(db_path=db_path)

    rendered = repr(logs)
    assert path_canary not in rendered
    assert str(db_path) not in rendered
    initialized = [entry for entry in logs if entry.get("event") == "signal_store_init"]
    assert initialized
    assert all(entry["reason_code"] == "signal_store_initialized" for entry in initialized)
    assert all(len(str(entry["database_fingerprint"])) == 16 for entry in initialized)


def test_losing_signal_first_opener_revalidates_owner_before_any_migration_write(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "signal-first-open-owner-race.db"
    losing_owner = "tenant-a"
    winning_owner = "tenant-b"
    original_preflight = SignalStore._preflight_owner_before_mutation
    winner_snapshot: list[tuple[str, list[str]]] = []
    winner_file_snapshot: list[dict[str, bytes]] = []
    interleaved = False

    def interleave_after_empty_preflight(store: SignalStore) -> None:
        nonlocal interleaved
        original_preflight(store)
        if store._legacy_tenant == losing_owner and not interleaved:
            interleaved = True
            SignalStore(
                db_path=db_path,
                runtime_settings=Settings(_env_file=None, knowledge_tenant_id=winning_owner),
            )
            winner_snapshot.append(_database_snapshot(db_path))
            winner_file_snapshot.append(_file_snapshot(db_path))

    monkeypatch.setattr(SignalStore, "_preflight_owner_before_mutation", interleave_after_empty_preflight)

    with pytest.raises(RuntimeError, match="owner preflight|tenant owner"):
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings(_env_file=None, knowledge_tenant_id=losing_owner),
        )

    assert len(winner_snapshot) == 1
    assert _database_snapshot(db_path) == winner_snapshot[0]
    assert _file_snapshot(db_path) == winner_file_snapshot[0]


def test_external_projection_marker_write_revalidates_owner_under_immediate_lock(tmp_path) -> None:
    db_path = tmp_path / "external-projection-owner-guard.db"
    store = SignalStore(
        db_path=db_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE signal_tenant_migration_metadata SET value='tenant-b' WHERE key='default_owner_v1'")
        conn.execute(
            "UPDATE signal_tenant_migration_metadata SET value='dirty:external-test' "
            "WHERE key='governed_projection_audit_v2'"
        )
    before = _database_snapshot(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        with pytest.raises(RuntimeError, match="owner preflight"):
            store.mark_governed_projection_audit_current(conn)
        marker = conn.execute(
            "SELECT value FROM signal_tenant_migration_metadata WHERE key='governed_projection_audit_v2'"
        ).fetchone()
        conn.rollback()

    assert marker is not None and marker["value"] == "dirty:external-test"
    assert _database_snapshot(db_path) == before


def test_ownerless_wildcard_signal_open_preserves_legacy_database(tmp_path) -> None:
    db_path = tmp_path / "ownerless-signals.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE signal_metric_mappings (
                id INTEGER PRIMARY KEY,
                signal_type TEXT NOT NULL,
                metric_pattern TEXT NOT NULL,
                source_type TEXT NOT NULL
            );
            INSERT INTO signal_metric_mappings
                (id, signal_type, metric_pattern, source_type)
            VALUES (1, 'request_latency', 'private_metric_canary', 'teach');
        """)
    before = _database_snapshot(db_path)

    with capture_logs() as logs, pytest.raises(RuntimeError, match="owner preflight"):
        SignalStore(
            db_path=db_path,
            runtime_settings=Settings(
                _env_file=None,
                knowledge_tenant_id="*",
                api_auth_enabled=True,
            ),
        )

    assert _database_snapshot(db_path) == before
    diagnostic_text = repr(logs)
    assert "private_metric_canary" not in diagnostic_text
    rejected = [log for log in logs if log.get("event") == "signal_owner_preflight_rejected"]
    assert len(rejected) == 1
    assert rejected[0]["reason_code"] == "ownerless_wildcard"
