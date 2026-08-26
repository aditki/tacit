from __future__ import annotations

import multiprocessing
import os
import queue
import re
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from structlog.testing import capture_logs

import tacit.sqlite_identity as sqlite_identity
from tacit.sqlite_identity import (
    SQLiteDatabaseTarget,
    SQLiteIdentityError,
    SQLiteIdentityRejectionReason,
    activate_sqlite_wal,
    claim_sqlite_database_identity,
    connect_sqlite_database,
    inspect_sqlite_database_target,
    require_sqlite_connection_path,
    require_sqlite_database_identity,
    snapshot_sqlite_database_set,
    sqlite_database_path,
)

_DISTINCTIVE_ROLE_TABLES = {
    "history": {
        "history_migration_progress",
        "history_schema_metadata",
        "investigation_events",
        "investigation_revisions",
        "investigation_runs",
        "investigation_snapshots",
        "investigation_tenant_assignments",
        "investigations",
    },
    "feedback": {
        "dashboard_provenance",
        "dashboard_provenance_legacy_tenant",
        "dashboard_provenance_tenant_migration_v2",
        "feedback",
        "feedback_legacy_tenant",
        "feedback_tenant_migration_v2",
        "feedback_tenant_migration_metadata",
    },
    "signals": {
        "candidate_promotions",
        "corroboration_snapshots",
        "dependency_hints",
        "dependency_hints_old",
        "dependency_hints_tacit_tenant_migration_v1",
        "entities",
        "entity_aliases",
        "entity_resolution_attempts",
        "evidence_requirements",
        "evidence_requirements_old",
        "evidence_requirements_tacit_tenant_migration_v1",
        "ingested_alerts",
        "ingested_alerts_old",
        "ingested_alerts_tacit_tenant_migration_v1",
        "ingested_dashboards",
        "ingested_dashboards_old",
        "ingested_dashboards_tacit_tenant_migration_v1",
        "knowledge_candidate_entity_refs",
        "knowledge_candidate_evidence",
        "knowledge_candidate_provenance",
        "knowledge_conflicts",
        "knowledge_corrections",
        "knowledge_current_contributors",
        "knowledge_current_scope_refs",
        "knowledge_events",
        "knowledge_migration_progress",
        "knowledge_migrations",
        "knowledge_propositions",
        "knowledge_snapshots",
        "knowledge_usage_events",
        "learned_artifacts",
        "learned_artifacts_old",
        "learned_artifacts_tacit_tenant_migration_v1",
        "learning_context_fts",
        "learning_context_fts_config",
        "learning_context_fts_content",
        "learning_context_fts_data",
        "learning_context_fts_docsize",
        "learning_context_fts_idx",
        "learning_context_fts_old",
        "learning_context_fts_old_config",
        "learning_context_fts_old_content",
        "learning_context_fts_old_data",
        "learning_context_fts_old_docsize",
        "learning_context_fts_old_idx",
        "learning_context_fts_tacit_tenant_migration_v1",
        "learning_context_fts_tacit_tenant_migration_v1_config",
        "learning_context_fts_tacit_tenant_migration_v1_content",
        "learning_context_fts_tacit_tenant_migration_v1_data",
        "learning_context_fts_tacit_tenant_migration_v1_docsize",
        "learning_context_fts_tacit_tenant_migration_v1_idx",
        "operational_knowledge",
        "operational_knowledge_revisions",
        "ownership_hints",
        "ownership_hints_old",
        "ownership_hints_tacit_tenant_migration_v1",
        "promotion_decisions",
        "proposition_candidates",
        "rejected_signal_candidates",
        "signal_mapping_candidates",
        "signal_mapping_candidates_old",
        "signal_mapping_candidates_tacit_tenant_migration_v1",
        "signal_mapping_source_refs",
        "signal_metric_mappings",
        "signal_metric_mappings_old",
        "signal_metric_mappings_tacit_tenant_migration_v1",
        "signal_migration_quarantine",
        "signal_tenant_migration_metadata",
        "signal_types",
        "tenant_signal_types",
    },
}


def _schema_table_names(*scripts: str) -> set[str]:
    pattern = re.compile(
        r"CREATE\s+(?:VIRTUAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z_][A-Za-z0-9_]*)",
        re.IGNORECASE,
    )
    return {match.group(1) for script in scripts for match in pattern.finditer(script)}


def _write_rows_in_process(database_path: str, worker: int, count: int) -> int:
    target = SQLiteDatabaseTarget(database_path)
    for offset in range(count):
        with target.connect(timeout_ms=30_000) as connection:
            activate_sqlite_wal(connection, timeout_ms=30_000)
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO process_rows (worker, offset) VALUES (?, ?)",
                (worker, offset),
            )
            connection.commit()
    return count


def _open_first_generation_in_process(database_path: str, worker: int) -> int:
    with SQLiteDatabaseTarget(database_path).connect(timeout_ms=30_000) as connection:
        activate_sqlite_wal(connection, timeout_ms=30_000)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS first_open_rows (worker INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT OR REPLACE INTO first_open_rows (worker, value) VALUES (?, ?)",
            (worker, f"worker-{worker}"),
        )
        connection.commit()
    return worker


def _coordinated_wal_worker(
    database_path: str,
    worker: int,
    start_event: Any,
    reopen_event: Any,
    checkpoint_event: Any,
    close_event: Any,
    messages: Any,
) -> None:
    connection: sqlite3.Connection | None = None
    try:
        if not start_event.wait(10):
            raise TimeoutError("start signal not received")
        target = SQLiteDatabaseTarget(database_path)
        connection = target.connect(timeout_ms=10_000)
        activate_sqlite_wal(connection, timeout_ms=10_000)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS coordinated_rows (worker INTEGER PRIMARY KEY, value TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO coordinated_rows (worker, value) VALUES (?, ?)",
            (worker, f"worker-{worker}"),
        )
        connection.commit()
        messages.put(("ready", worker, None))

        if worker == 1:
            if not reopen_event.wait(10):
                raise TimeoutError("reopen signal not received")
            connection.close()
            connection = target.connect(timeout_ms=10_000)
            activate_sqlite_wal(connection, timeout_ms=10_000)
            row_count = connection.execute("SELECT COUNT(*) FROM coordinated_rows").fetchone()[0]
            messages.put(("reopened", worker, row_count))

        if worker == 0:
            if not checkpoint_event.wait(10):
                raise TimeoutError("checkpoint signal not received")
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            messages.put(("checkpointed", worker, checkpoint))

        if not close_event.wait(10):
            raise TimeoutError("close signal not received")
        connection.close()
        connection = None
        messages.put(("closed", worker, None))
    except BaseException as exc:
        messages.put(("error", worker, f"{type(exc).__name__}: {exc}"))
        raise
    finally:
        if connection is not None:
            connection.close()


def _verify_wal_after_last_close(database_path: str, messages: Any) -> None:
    try:
        with SQLiteDatabaseTarget(database_path).connect(timeout_ms=10_000) as connection:
            activate_sqlite_wal(connection, timeout_ms=10_000)
            prior_count = connection.execute("SELECT COUNT(*) FROM coordinated_rows").fetchone()[0]
            connection.execute("INSERT INTO coordinated_rows (worker, value) VALUES (2, 'fresh-process')")
            connection.commit()
            checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            final_count = connection.execute("SELECT COUNT(*) FROM coordinated_rows").fetchone()[0]
        messages.put(("fresh", prior_count, final_count, checkpoint))
    except BaseException as exc:
        messages.put(("error", 2, f"{type(exc).__name__}: {exc}"))
        raise


def _next_process_message(messages: Any, expected: str) -> tuple[Any, ...]:
    try:
        message = messages.get(timeout=15)
    except queue.Empty as exc:
        raise AssertionError(f"timed out waiting for subprocess message {expected!r}") from exc
    assert message[0] != "error", message
    assert message[0] == expected, message
    return message


def test_connect_creates_missing_private_parents_and_literal_path(tmp_path: Path) -> None:
    database_path = tmp_path / "state" / "nested" / "history?tenant=acme#current.db"

    with connect_sqlite_database(database_path, timeout_ms=1_000) as connection:
        assert type(connection) is sqlite3.Connection
        connection.execute("CREATE TABLE values_table (value TEXT NOT NULL)")
        connection.execute("INSERT INTO values_table VALUES ('ok')")
        connection.commit()

    assert database_path.is_file()
    assert database_path.parent.stat().st_mode & (stat.S_IWGRP | stat.S_IWOTH) == 0
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT value FROM values_table").fetchone() == ("ok",)


def test_sqlite_database_path_is_absolute_without_uri_interpretation(tmp_path: Path) -> None:
    database_path = tmp_path / "signals?mode=memory#fragment.db"

    assert sqlite_database_path(database_path) == database_path.absolute()


def test_protected_path_storage_fails_before_creation_on_unsupported_platform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "unsupported" / "signals.db"
    monkeypatch.setattr(sqlite_identity, "_PROTECTED_PATH_PLATFORM_SUPPORTED", False, raising=False)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        connect_sqlite_database(database_path, timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.UNSUPPORTED_PLATFORM.value
    assert not database_path.parent.exists()


def test_inspection_returns_none_without_creating_storage(tmp_path: Path) -> None:
    database_path = tmp_path / "missing" / "signals.db"

    assert inspect_sqlite_database_target(database_path) is None
    assert not database_path.parent.exists()


def test_direct_store_boundary_rejects_component_symlink(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    linked_parent = tmp_path / "linked"
    try:
        linked_parent.symlink_to(real_parent, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symbolic links unavailable: {exc}")

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(linked_parent / "signals.db").connect(timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.SYMLINK.value
    assert not (real_parent / "signals.db").exists()


def test_direct_store_boundary_rejects_final_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.db"
    target.touch()
    link = tmp_path / "signals.db"
    try:
        link.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symbolic links unavailable: {exc}")

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(link).connect(timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.SYMLINK.value
    assert target.stat().st_size == 0


def test_direct_store_boundary_rejects_sidecar_symlink_before_creating_main_file(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    target = tmp_path / "sidecar-canary"
    target.write_text("canary", encoding="utf-8")
    sidecar = Path(f"{database_path}-wal")
    try:
        sidecar.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symbolic links unavailable: {exc}")

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(database_path).connect(timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.SYMLINK.value
    assert not database_path.exists()
    assert target.read_text(encoding="utf-8") == "canary"


@pytest.mark.skipif(os.name != "posix", reason="FIFO metadata is POSIX-specific")
def test_direct_store_boundary_rejects_special_file(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    os.mkfifo(database_path)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(database_path).connect(timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.SPECIAL_FILE.value


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and mode policy")
def test_direct_store_boundary_rejects_writable_parent(tmp_path: Path) -> None:
    parent = tmp_path / "shared"
    parent.mkdir(mode=0o700)
    parent.chmod(0o777)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(parent / "signals.db").connect(timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.PARENT_UNTRUSTED.value
    assert not (parent / "signals.db").exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and mode policy")
def test_root_owned_sticky_temp_ancestor_allows_private_service_directory() -> None:
    platform_tmp = sqlite_database_path("/tmp/tacit-probe.db").parent
    metadata = platform_tmp.stat()
    if metadata.st_uid != 0 or not metadata.st_mode & stat.S_ISVTX:
        pytest.skip("platform temporary directory is not root-owned and sticky")

    with tempfile.TemporaryDirectory(prefix="tacit-sqlite-", dir="/tmp") as directory:
        private_parent = Path(directory)
        private_parent.chmod(0o700)
        database_path = private_parent / "signals.db"
        with connect_sqlite_database(database_path, timeout_ms=1_000) as connection:
            assert connection.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and mode policy")
@pytest.mark.parametrize("mode", (0o660, 0o666))
def test_direct_store_boundary_rejects_writable_database_file(tmp_path: Path, mode: int) -> None:
    database_path = tmp_path / "signals.db"
    database_path.touch(mode=0o600)
    database_path.chmod(mode)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(database_path).connect(timeout_ms=1_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.FILE_UNTRUSTED.value
    assert database_path.stat().st_size == 0


@pytest.mark.skipif(os.name != "posix", reason="POSIX ownership and mode policy")
def test_direct_store_boundary_accepts_readable_nonwritable_database_file(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    database_path.touch(mode=0o600)
    database_path.chmod(0o644)

    with SQLiteDatabaseTarget(database_path).connect(timeout_ms=1_000) as connection:
        connection.execute("SELECT 1")


@pytest.mark.skipif(os.name != "posix", reason="POSIX sidecar mode policy")
def test_existing_writable_sidecar_is_rejected_before_open(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    anchor = SQLiteDatabaseTarget(database_path).connect(timeout_ms=1_000)
    try:
        activate_sqlite_wal(anchor, timeout_ms=1_000)
        anchor.execute("CREATE TABLE rows (value INTEGER)")
        anchor.execute("INSERT INTO rows VALUES (1)")
        anchor.commit()
        wal_path = Path(f"{database_path}-wal")
        assert wal_path.exists()
        wal_path.chmod(0o666)

        with pytest.raises(SQLiteIdentityError) as exc_info:
            SQLiteDatabaseTarget(database_path).connect(timeout_ms=1_000)

        assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.FILE_UNTRUSTED.value
    finally:
        wal_path = Path(f"{database_path}-wal")
        if wal_path.exists():
            wal_path.chmod(0o600)
        anchor.close()


def test_activate_sqlite_wal_requires_exact_wal_result() -> None:
    class NonWalConnection:
        def execute(self, _sql: str):
            return self

        def fetchone(self):
            return ("delete",)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        activate_sqlite_wal(NonWalConnection())  # type: ignore[arg-type]

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.WAL_UNAVAILABLE.value


def test_connection_supports_standard_sqlite_mutation_apis(tmp_path: Path) -> None:
    database_path = tmp_path / "standard-apis.db"
    with connect_sqlite_database(database_path, timeout_ms=1_000) as connection:
        activate_sqlite_wal(connection, timeout_ms=1_000)
        connection.execute("CREATE TABLE blobs (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        connection.execute("INSERT INTO blobs(payload) VALUES (zeroblob(4))")
        connection.commit()
        with connection.blobopen("blobs", "payload", 1) as blob:
            blob.write(b"test")

        backup = sqlite3.connect(":memory:")
        try:
            connection.backup(backup)
            assert backup.execute("SELECT payload FROM blobs").fetchone() == (b"test",)
        finally:
            backup.close()


def test_same_database_external_connection_can_join_and_other_database_cannot(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    other_path = tmp_path / "other.db"
    target = SQLiteDatabaseTarget(database_path)
    with target.connect(timeout_ms=1_000) as connection:
        target.bind_connection(connection)
        require_sqlite_connection_path(connection, path=database_path)

    with sqlite3.connect(other_path) as connection:
        with pytest.raises(SQLiteIdentityError) as exc_info:
            target.bind_connection(connection)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.CONNECTION_IDENTITY.value


def test_database_role_identity_is_claimed_and_rechecked(tmp_path: Path) -> None:
    database_path = tmp_path / "signals.db"
    with connect_sqlite_database(database_path, timeout_ms=1_000) as connection:
        connection.execute("BEGIN IMMEDIATE")
        database_id = claim_sqlite_database_identity(
            connection,
            role="signals",
            expected_database_id=None,
        )
        connection.commit()

    with connect_sqlite_database(database_path, timeout_ms=1_000) as connection:
        assert (
            require_sqlite_database_identity(
                connection,
                role="signals",
                expected_database_id=database_id,
            )
            == database_id
        )
        with pytest.raises(SQLiteIdentityError) as exc_info:
            require_sqlite_database_identity(
                connection,
                role="history",
                expected_database_id=None,
            )

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ROLE_COLLISION.value


@pytest.mark.parametrize(
    ("owning_role", "table_name"),
    [
        (role, table_name)
        for role, table_names in _DISTINCTIVE_ROLE_TABLES.items()
        for table_name in sorted(table_names)
    ],
)
def test_partial_role_schema_cannot_be_claimed_by_another_role(
    owning_role: str,
    table_name: str,
) -> None:
    with sqlite3.connect(":memory:") as connection:
        connection.execute(f'CREATE TABLE "{table_name}" (placeholder INTEGER)')

        for requested_role in {"history", "feedback", "signals"} - {owning_role}:
            with pytest.raises(SQLiteIdentityError) as exc_info:
                claim_sqlite_database_identity(
                    connection,
                    role=requested_role,
                    expected_database_id=None,
                )

            assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ROLE_COLLISION.value
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tacit_runtime_database_identity'"
            ).fetchone()
            is None
        )


@pytest.mark.parametrize(
    ("owning_role", "columns"),
    [
        (
            "history",
            "id TEXT, investigation_id TEXT, revision INTEGER, correction_text TEXT, provenance_json TEXT",
        ),
        (
            "signals",
            "id TEXT, tenant_id TEXT, kind TEXT, proposition_key TEXT, candidate_json TEXT",
        ),
    ],
)
def test_shared_table_name_uses_schema_shape_for_role_detection(
    owning_role: str,
    columns: str,
) -> None:
    with sqlite3.connect(":memory:") as connection:
        connection.execute(f"CREATE TABLE knowledge_candidates ({columns})")

        for requested_role in {"history", "feedback", "signals"} - {owning_role}:
            with pytest.raises(SQLiteIdentityError) as exc_info:
                claim_sqlite_database_identity(
                    connection,
                    role=requested_role,
                    expected_database_id=None,
                )

            assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ROLE_COLLISION.value


@pytest.mark.parametrize(
    "columns",
    [
        "id TEXT, tenant_id TEXT",
        "id TEXT, investigation_id TEXT, revision INTEGER, correction_text TEXT",
        "id TEXT, tenant_id TEXT, kind TEXT, proposition_key TEXT",
    ],
)
def test_malformed_shared_table_shape_cannot_be_claimed_by_any_role(columns: str) -> None:
    with sqlite3.connect(":memory:") as connection:
        connection.execute(f"CREATE TABLE knowledge_candidates ({columns})")

        for requested_role in ("history", "feedback", "signals"):
            with pytest.raises(SQLiteIdentityError) as exc_info:
                claim_sqlite_database_identity(
                    connection,
                    role=requested_role,
                    expected_database_id=None,
                )

            assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ROLE_COLLISION.value
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tacit_runtime_database_identity'"
            ).fetchone()
            is None
        )


def test_ambiguous_shared_table_shape_cannot_be_claimed_by_any_role() -> None:
    columns = """id TEXT, investigation_id TEXT, revision INTEGER, correction_text TEXT,
                 provenance_json TEXT, tenant_id TEXT, kind TEXT, proposition_key TEXT,
                 candidate_json TEXT"""
    with sqlite3.connect(":memory:") as connection:
        connection.execute(f"CREATE TABLE knowledge_candidates ({columns})")

        for requested_role in ("history", "feedback", "signals"):
            with pytest.raises(SQLiteIdentityError) as exc_info:
                claim_sqlite_database_identity(
                    connection,
                    role=requested_role,
                    expected_database_id=None,
                )

            assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ROLE_COLLISION.value
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tacit_runtime_database_identity'"
            ).fetchone()
            is None
        )


def test_readonly_preflight_does_not_create_a_missing_database(tmp_path: Path) -> None:
    database_path = tmp_path / "missing" / "history.db"
    target = SQLiteDatabaseTarget(database_path)

    with target.connect_existing_readonly(timeout_ms=1_000) as connection:
        assert connection is None
    assert not database_path.exists()
    assert not database_path.parent.exists()


def _sqlite_directory_snapshot(directory: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in sorted(directory.iterdir()) if path.is_file()}


def test_readonly_preflight_preserves_existing_journal_mode(tmp_path: Path) -> None:
    database_path = tmp_path / "history.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES ('unchanged')")
        connection.commit()
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)

    before = _sqlite_directory_snapshot(tmp_path)
    with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=1_000) as connection:
        assert connection is not None
        assert connection.execute("SELECT value FROM canary").fetchone() == ("unchanged",)
        with pytest.raises(sqlite3.OperationalError):
            connection.execute("CREATE TABLE forbidden (id INTEGER)")
    assert _sqlite_directory_snapshot(tmp_path) == before

    with sqlite3.connect(database_path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("delete",)
        assert connection.execute("SELECT value FROM canary").fetchone() == ("unchanged",)


def test_readonly_preflight_fails_closed_without_touching_live_rollback_journal(tmp_path: Path) -> None:
    database_path = tmp_path / "live-rollback.db"
    writer = sqlite3.connect(database_path)
    try:
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('committed')")
        writer.commit()
        assert writer.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE canary SET value='uncommitted'")
        journal_path = Path(f"{database_path}-journal")
        assert journal_path.exists() and journal_path.stat().st_size > 0
        before = _sqlite_directory_snapshot(tmp_path)

        with pytest.raises(SQLiteIdentityError) as exc_info:
            with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=1_000):
                pass

        assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED.value
        assert _sqlite_directory_snapshot(tmp_path) == before
    finally:
        writer.rollback()
        writer.close()


def test_readonly_admission_retries_a_transient_rollback_journal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "transient-rollback.db"
    writer = sqlite3.connect(database_path)
    original_sleep = sqlite_identity.time.sleep
    retry_delays: list[float] = []
    try:
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('committed')")
        writer.commit()
        assert writer.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE canary SET value='uncommitted'")

        def complete_recovery(delay: float) -> None:
            retry_delays.append(delay)
            writer.rollback()
            original_sleep(0)

        monkeypatch.setattr(sqlite_identity.time, "sleep", complete_recovery)

        with capture_logs() as logs:
            value = SQLiteDatabaseTarget(database_path).read_existing_readonly(
                lambda connection: str(connection.execute("SELECT value FROM canary").fetchone()[0]),
                timeout_ms=1_000,
            )

        assert value == "committed"
        assert len(retry_delays) == 1
        retry = next(log for log in logs if log.get("event") == "sqlite_readonly_admission_retry")
        assert retry["reason_code"] == SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED.value
        assert retry["attempt"] == 1
    finally:
        writer.rollback()
        writer.close()


def test_readonly_preflight_does_not_create_closed_wal_sidecars(tmp_path: Path) -> None:
    database_path = tmp_path / "closed-wal.db"
    connection = sqlite3.connect(database_path)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES ('closed-wal')")
        connection.commit()
    finally:
        connection.close()

    before = _sqlite_directory_snapshot(tmp_path)
    assert set(before) == {database_path.name}

    with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=1_000) as connection:
        assert connection is not None
        assert connection.execute("SELECT value FROM canary").fetchone() == ("closed-wal",)

    assert _sqlite_directory_snapshot(tmp_path) == before


def test_readonly_preflight_preserves_live_wal_sidecars(tmp_path: Path) -> None:
    database_path = tmp_path / "live-wal.db"
    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('live-wal')")
        writer.commit()
        before = _sqlite_directory_snapshot(tmp_path)
        assert {database_path.name, f"{database_path.name}-shm", f"{database_path.name}-wal"} <= set(before)

        with capture_logs() as logs:
            with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=1_000) as connection:
                assert connection is not None
                assert connection.execute("SELECT value FROM canary").fetchone() == ("live-wal",)

        assert _sqlite_directory_snapshot(tmp_path) == before
        event = next(log for log in logs if log.get("event") == "sqlite_readonly_admission_snapshot")
        assert event["snapshot_bytes"] == len(before[database_path.name]) + len(before[f"{database_path.name}-wal"])
        assert float(event["copy_duration_ms"]) >= 0
        assert "path" not in event
    finally:
        writer.close()


def _source_directory_state(directory: Path) -> dict[str, tuple[bytes, int, int, int]]:
    state: dict[str, tuple[bytes, int, int, int]] = {}
    for path in sorted(directory.iterdir()):
        metadata = path.lstat()
        state[path.name] = (
            path.read_bytes(),
            stat.S_IMODE(metadata.st_mode),
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        )
    return state


def test_public_snapshot_preserves_live_wal_and_shm_in_readonly_source_directory(tmp_path: Path) -> None:
    source_dir = tmp_path / "authority"
    source_dir.mkdir(mode=0o700)
    database_path = source_dir / "history.db"
    destination_dir = tmp_path / "snapshots"
    destination_dir.mkdir(mode=0o700)
    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE generation (value TEXT NOT NULL)")
        writer.execute("INSERT INTO generation VALUES ('stable-live-wal')")
        writer.commit()
        assert Path(f"{database_path}-wal").exists()
        assert Path(f"{database_path}-shm").exists()
        source_dir.chmod(0o500)
        before = _source_directory_state(source_dir)

        snapshots = snapshot_sqlite_database_set([database_path], destination_dir, timeout_ms=2_000)

        after = _source_directory_state(source_dir)
        assert after == before
        assert set(snapshots) == {database_path}
        with sqlite3.connect(snapshots[database_path]) as snapshot:
            assert snapshot.execute("SELECT value FROM generation").fetchone() == ("stable-live-wal",)
    finally:
        source_dir.chmod(0o700)
        writer.close()


def test_public_snapshot_retries_complete_source_set_after_writer_interleaves(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_dir = tmp_path / "authority"
    source_dir.mkdir(mode=0o700)
    destination_dir = tmp_path / "snapshots"
    destination_dir.mkdir(mode=0o700)
    sources = [source_dir / name for name in ("history.db", "feedback.db", "signals.db")]
    for source in sources:
        with sqlite3.connect(source) as connection:
            connection.execute("CREATE TABLE generation (value INTEGER NOT NULL)")
            connection.execute("INSERT INTO generation VALUES (1)")

    original_snapshot = sqlite_identity._snapshot_sqlite_database
    first_copy_finished = threading.Event()
    writer_finished = threading.Event()
    copy_count = 0

    def writer() -> None:
        assert first_copy_finished.wait(timeout=5)
        for source in sources:
            with sqlite3.connect(source) as connection:
                connection.execute("UPDATE generation SET value=2")
        writer_finished.set()

    writer_thread = threading.Thread(target=writer)
    writer_thread.start()

    def interleaved_snapshot(*args, **kwargs):
        nonlocal copy_count
        original_snapshot(*args, **kwargs)
        copy_count += 1
        if copy_count == 1:
            first_copy_finished.set()
            assert writer_finished.wait(timeout=5)

    monkeypatch.setattr(sqlite_identity, "_snapshot_sqlite_database", interleaved_snapshot)
    try:
        snapshots = snapshot_sqlite_database_set(sources, destination_dir, timeout_ms=2_000)
    finally:
        first_copy_finished.set()
        writer_thread.join(timeout=5)

    assert not writer_thread.is_alive()
    assert copy_count >= len(sources) + 1
    observed_generations = set()
    for snapshot_path in snapshots.values():
        with sqlite3.connect(snapshot_path) as connection:
            observed_generations.add(connection.execute("SELECT value FROM generation").fetchone()[0])
    assert observed_generations == {2}


def test_readonly_preflight_fails_closed_above_live_wal_snapshot_bound(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "bounded-live-wal.db"
    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('bounded')")
        writer.commit()
        before = _sqlite_directory_snapshot(tmp_path)
        monkeypatch.setattr(sqlite_identity, "_READONLY_ADMISSION_SNAPSHOT_MAX_BYTES", 1)

        with pytest.raises(SQLiteIdentityError) as exc_info:
            with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=1_000):
                pass

        assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT.value
        assert _sqlite_directory_snapshot(tmp_path) == before
    finally:
        writer.close()


def test_readonly_snapshot_copy_enforces_budget_against_source_growth(tmp_path: Path) -> None:
    source = tmp_path / "source.db"
    destination = tmp_path / "snapshot.db"
    source.write_bytes(b"1234")
    observed_size = source.stat().st_size
    source.write_bytes(b"1234567890123456")
    budget = sqlite_identity._ReadOnlyAdmissionBudget(
        deadline=time.monotonic() + 1,
        max_bytes=8,
    )

    with pytest.raises(SQLiteIdentityError) as exc_info:
        sqlite_identity._copy_snapshot_component(source, destination, budget=budget)

    assert observed_size <= budget.max_bytes
    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT.value
    assert destination.stat().st_size <= budget.max_bytes


def test_readonly_preflight_rejects_copy_that_exhausts_shared_deadline(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "slow-copy-live-wal.db"
    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('slow-copy')")
        writer.commit()
        before = _sqlite_directory_snapshot(tmp_path)
        original_copy = sqlite_identity._copy_snapshot_component

        def slow_copy(source, destination, *, budget):
            time.sleep(0.02)
            return original_copy(source, destination, budget=budget)

        monkeypatch.setattr(sqlite_identity, "_copy_snapshot_component", slow_copy)
        with pytest.raises(SQLiteIdentityError) as exc_info:
            SQLiteDatabaseTarget(database_path).read_existing_readonly(
                lambda connection: connection.execute("SELECT value FROM canary").fetchone(),
                timeout_ms=5,
            )

        assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_TIMEOUT.value
        assert _sqlite_directory_snapshot(tmp_path) == before
    finally:
        writer.close()


def test_readonly_admission_interrupts_query_after_shared_deadline(tmp_path: Path) -> None:
    database_path = tmp_path / "slow-query.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES ('slow-query')")
        connection.commit()

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(database_path).read_existing_readonly(
            lambda connection: connection.execute("""WITH RECURSIVE values_cte(value) AS (
                       SELECT 1 UNION ALL SELECT value + 1 FROM values_cte WHERE value < 10000000
                   ) SELECT sum(value) FROM values_cte""").fetchone(),
            timeout_ms=1,
        )

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_TIMEOUT.value


def test_readonly_admission_never_accepts_callback_completion_after_deadline(tmp_path: Path) -> None:
    database_path = tmp_path / "slow-callback.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES ('slow-callback')")
        connection.commit()

    def slow_reader(connection: sqlite3.Connection) -> str:
        value = str(connection.execute("SELECT value FROM canary").fetchone()[0])
        time.sleep(0.02)
        return value

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(database_path).read_existing_readonly(
            slow_reader,
            timeout_ms=5,
        )

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_TIMEOUT.value


def test_readonly_preflight_rejects_source_change_before_admission_completes(tmp_path: Path) -> None:
    database_path = tmp_path / "changing-live-wal.db"
    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('first')")
        writer.commit()

        with pytest.raises(SQLiteIdentityError) as exc_info:
            with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=1_000) as connection:
                assert connection is not None
                assert connection.execute("SELECT COUNT(*) FROM canary").fetchone() == (1,)
                writer.execute("INSERT INTO canary VALUES ('second')")
                writer.commit()

        assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.FILE_REPLACED.value
    finally:
        writer.close()


def test_readonly_admission_retries_complete_callback_after_source_change(tmp_path: Path) -> None:
    database_path = tmp_path / "retry-live-wal.db"
    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO canary VALUES ('first')")
        writer.commit()
        calls = 0

        def read_count(connection: sqlite3.Connection) -> int:
            nonlocal calls
            calls += 1
            count = int(connection.execute("SELECT COUNT(*) FROM canary").fetchone()[0])
            if calls == 1:
                writer.execute("INSERT INTO canary VALUES ('second')")
                writer.commit()
            return count

        with capture_logs() as logs:
            count = SQLiteDatabaseTarget(database_path).read_existing_readonly(
                read_count,
                timeout_ms=1_000,
            )

        assert count == 2
        assert calls == 2
        retry = next(log for log in logs if log.get("event") == "sqlite_readonly_admission_retry")
        assert retry["reason_code"] == SQLiteIdentityRejectionReason.FILE_REPLACED.value
        assert retry["attempt"] == 1
        assert "path" not in retry
    finally:
        writer.close()


def test_readonly_admission_retries_database_error_only_after_source_change(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "database-error-source-change.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES ('stable')")
        connection.commit()

    original_readonly_connection = sqlite_identity._readonly_connection
    attempts = 0

    @contextmanager
    def fail_during_first_snapshot(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            with sqlite3.connect(database_path) as writer:
                writer.execute("CREATE TABLE concurrent_change (id INTEGER)")
                writer.commit()
            raise sqlite3.DatabaseError("transient snapshot failure")
        with original_readonly_connection(*args, **kwargs) as connection:
            yield connection

    monkeypatch.setattr(sqlite_identity, "_readonly_connection", fail_during_first_snapshot)

    with capture_logs() as logs:
        value = SQLiteDatabaseTarget(database_path).read_existing_readonly(
            lambda connection: str(connection.execute("SELECT value FROM canary").fetchone()[0]),
            timeout_ms=1_000,
        )

    assert value == "stable"
    assert attempts == 2
    retry = next(log for log in logs if log.get("event") == "sqlite_readonly_admission_retry")
    assert retry["reason_code"] == SQLiteIdentityRejectionReason.FILE_REPLACED.value


@pytest.mark.skipif(os.name != "posix", reason="hard-link behavior is POSIX-specific")
def test_readonly_preflight_preserves_hard_link_and_wal_directory(tmp_path: Path) -> None:
    database_path = tmp_path / "canonical.db"
    alias_path = tmp_path / "alias.db"
    connection = sqlite3.connect(database_path)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES ('hard-link')")
        connection.commit()
    finally:
        connection.close()
    os.link(database_path, alias_path)
    before = _sqlite_directory_snapshot(tmp_path)

    with SQLiteDatabaseTarget(alias_path).connect_existing_readonly(timeout_ms=1_000) as connection:
        assert connection is not None
        assert connection.execute("SELECT value FROM canary").fetchone() == ("hard-link",)

    assert _sqlite_directory_snapshot(tmp_path) == before


@pytest.mark.parametrize("store_role", ("history", "feedback", "signals"))
def test_owner_denial_preserves_live_wal_files_for_every_store(tmp_path: Path, store_role: str) -> None:
    from tacit.config import Settings
    from tacit.feedback import FeedbackStore
    from tacit.history import InvestigationStore
    from tacit.signals import SignalStore

    stores = {
        "history": InvestigationStore,
        "feedback": FeedbackStore,
        "signals": SignalStore,
    }
    store_type = stores[store_role]
    database_path = tmp_path / f"{store_role}.db"
    store_type(
        db_path=database_path,
        runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-a"),
    )

    writer = sqlite3.connect(database_path)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("CREATE TABLE live_wal_admission_canary (value TEXT NOT NULL)")
        writer.execute("INSERT INTO live_wal_admission_canary VALUES ('preserve-me')")
        if store_role == "history":
            writer.execute(
                """INSERT OR REPLACE INTO history_migration_progress
                   (migration_name, cursor, details_json, updated_at)
                   VALUES ('history_tenant_assignment_backfill_v2', '', ?, 0)""",
                ('{"configured_tenant":"tenant-a","tenant_column_existed":true}',),
            )
        writer.commit()
        before = _sqlite_directory_snapshot(tmp_path)
        assert {database_path.name, f"{database_path.name}-shm", f"{database_path.name}-wal"} <= set(before)

        with pytest.raises(RuntimeError):
            store_type(
                db_path=database_path,
                runtime_settings=Settings(_env_file=None, knowledge_tenant_id="tenant-b"),
            )

        assert _sqlite_directory_snapshot(tmp_path) == before
    finally:
        writer.close()


def test_role_detection_inventory_covers_every_canonical_schema_table() -> None:
    from tacit.feedback import _SCHEMA_SQL as feedback_schema
    from tacit.history import _SCHEMA_SQL as history_schema
    from tacit.knowledge.repository import SCHEMA_SQL as knowledge_schema
    from tacit.signals.schema import FTS_SCHEMA_SQL as signal_fts_schema
    from tacit.signals.schema import SCHEMA_SQL as signal_schema

    shared_tables = {"knowledge_candidates"}
    canonical_tables = {
        "history": _schema_table_names(history_schema) - shared_tables,
        "feedback": _schema_table_names(feedback_schema),
        "signals": _schema_table_names(signal_schema, signal_fts_schema, knowledge_schema) - shared_tables,
    }

    for role, table_names in canonical_tables.items():
        assert table_names <= _DISTINCTIVE_ROLE_TABLES[role]


def test_normal_two_connection_wal_checkpoint_and_reopen(tmp_path: Path) -> None:
    database_path = tmp_path / "wal-lifecycle.db"
    target = SQLiteDatabaseTarget(database_path)
    writer = target.connect(timeout_ms=5_000)
    reader = target.connect(timeout_ms=5_000)
    try:
        assert activate_sqlite_wal(writer, timeout_ms=5_000) == "wal"
        assert activate_sqlite_wal(reader, timeout_ms=5_000) == "wal"
        writer.execute("CREATE TABLE rows (value INTEGER)")
        writer.execute("INSERT INTO rows VALUES (1)")
        writer.commit()
        assert reader.execute("SELECT value FROM rows").fetchone() == (1,)
        assert writer.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone() is not None
    finally:
        reader.close()
        writer.close()

    with target.connect(timeout_ms=5_000) as reopened:
        assert activate_sqlite_wal(reopened, timeout_ms=5_000) == "wal"
        assert reopened.execute("SELECT value FROM rows").fetchone() == (1,)


def test_normal_multiprocess_wal_writers_all_complete(tmp_path: Path) -> None:
    database_path = tmp_path / "multiprocess.db"
    with SQLiteDatabaseTarget(database_path).connect(timeout_ms=30_000) as connection:
        activate_sqlite_wal(connection, timeout_ms=30_000)
        connection.execute(
            "CREATE TABLE process_rows (worker INTEGER NOT NULL, offset INTEGER NOT NULL, PRIMARY KEY(worker, offset))"
        )
        connection.commit()

    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=4, mp_context=context) as executor:
        results = list(
            executor.map(
                _write_rows_in_process,
                [str(database_path)] * 4,
                range(4),
                [8] * 4,
            )
        )

    assert results == [8, 8, 8, 8]
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM process_rows").fetchone() == (32,)


def test_synchronized_multiprocess_first_open_is_idempotent(tmp_path: Path) -> None:
    database_path = tmp_path / "first-open.db"
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=4, mp_context=context) as executor:
        results = sorted(
            executor.map(
                _open_first_generation_in_process,
                [str(database_path)] * 4,
                range(4),
            )
        )

    assert results == [0, 1, 2, 3]
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM first_open_rows").fetchone() == (4,)


@pytest.mark.parametrize("last_closer", (0, 1))
def test_coordinated_multiprocess_wal_checkpoint_last_close_and_reopen(
    tmp_path: Path,
    last_closer: int,
) -> None:
    database_path = tmp_path / f"coordinated-{last_closer}.db"
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    reopen_event = context.Event()
    checkpoint_event = context.Event()
    close_events = [context.Event(), context.Event()]
    messages = context.Queue()
    processes = [
        context.Process(
            target=_coordinated_wal_worker,
            args=(
                str(database_path),
                worker,
                start_event,
                reopen_event,
                checkpoint_event,
                close_events[worker],
                messages,
            ),
        )
        for worker in range(2)
    ]

    try:
        for process in processes:
            process.start()
        start_event.set()
        ready = {_next_process_message(messages, "ready")[1] for _ in processes}
        assert ready == {0, 1}

        reopen_event.set()
        assert _next_process_message(messages, "reopened")[2] == 2
        checkpoint_event.set()
        assert _next_process_message(messages, "checkpointed")[2] is not None

        for worker in (1 - last_closer, last_closer):
            close_events[worker].set()
            assert _next_process_message(messages, "closed")[1] == worker
            processes[worker].join(timeout=15)
            assert processes[worker].exitcode == 0

        fresh = context.Process(
            target=_verify_wal_after_last_close,
            args=(str(database_path), messages),
        )
        fresh.start()
        assert _next_process_message(messages, "fresh")[1:3] == (2, 3)
        fresh.join(timeout=15)
        assert fresh.exitcode == 0
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
        messages.close()


def test_rejection_telemetry_contains_only_bounded_reason_codes(tmp_path: Path) -> None:
    target = tmp_path / "do-not-log-this-name.db"
    target.mkdir()
    observed: list[SQLiteIdentityRejectionReason] = []

    with capture_logs() as logs, pytest.raises(SQLiteIdentityError):
        connect_sqlite_database(
            target,
            timeout_ms=1_000,
            rejection_hook=observed.append,
        )

    assert observed == [SQLiteIdentityRejectionReason.SPECIAL_FILE]
    rejection_logs = [entry for entry in logs if entry.get("event") == "sqlite_identity_rejected"]
    assert rejection_logs == [
        {
            "log_level": "warning",
            "event": "sqlite_identity_rejected",
            "reason_code": SQLiteIdentityRejectionReason.SPECIAL_FILE.value,
        }
    ]
    assert "do-not-log-this-name" not in str(rejection_logs)


def test_sqlite_storage_benchmark_emits_reproducible_machine_readable_results(tmp_path: Path) -> None:
    from tests.eval.sqlite_storage_benchmark import run_benchmark

    result = run_benchmark(
        root=tmp_path / "benchmark",
        samples=2,
        warmups=1,
        batch_size=3,
        subprocess_workers=2,
        subprocess_writes=2,
    )

    assert result["schema_version"] == 1
    assert result["failures"] == []
    assert result["parameters"] == {
        "samples": 2,
        "warmups": 1,
        "batch_size": 3,
        "subprocess_workers": 2,
        "subprocess_writes": 2,
        "coordinated_last_close_orders": [0, 1],
    }
    assert result["runtime"]["python"]
    assert result["runtime"]["sqlite"]
    assert result["runtime"]["platform"]
    assert result["runtime"]["revision"]
    assert result["runtime"]["filesystem_root"] == str((tmp_path / "benchmark").resolve())
    assert result["runtime"]["filesystem_device"] >= 0
    assert result["descriptor_delta"] is None or result["descriptor_delta"] <= 2
    assert set(result["workloads"]) == {
        "connect_wal_close",
        "single_row_commit",
        "batched_statements",
        "checkpoint_reopen",
        "subprocess_wal",
        "coordinated_wal_lifecycle",
    }
    for workload in result["workloads"].values():
        assert workload["control"]["sample_count"] > 0
        assert workload["tacit"]["sample_count"] > 0
        assert workload["control"]["p50_ms"] >= 0
        assert workload["tacit"]["p95_ms"] >= 0


def test_sqlite_storage_benchmark_marks_untracked_inputs_dirty(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.eval import sqlite_storage_benchmark as benchmark

    def fake_run(args, **_kwargs):
        if args[1:3] == ["rev-parse", "--short=12"]:
            return subprocess.CompletedProcess(args, 0, stdout="abc123\n", stderr="")
        if args[1:3] == ["status", "--porcelain=v1"]:
            return subprocess.CompletedProcess(
                args,
                0,
                stdout="?? tests/eval/sqlite_storage_benchmark.py\n",
                stderr="",
            )
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(benchmark.subprocess, "run", fake_run)

    assert benchmark._revision() == "abc123-dirty"
