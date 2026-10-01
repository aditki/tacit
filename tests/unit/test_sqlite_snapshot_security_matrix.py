from __future__ import annotations

import json
import os
import sqlite3
import stat
import threading
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

import pytest

import tacit.sqlite_identity as sqlite_identity
from tacit.sqlite_identity import (
    SQLiteDatabaseTarget,
    SQLiteIdentityError,
    SQLiteIdentityRejectionReason,
    snapshot_sqlite_database_set,
)

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX owner-only snapshot policy")


@contextmanager
def _temporary_umask(mask: int) -> Iterator[None]:
    previous = os.umask(mask)
    try:
        yield
    finally:
        os.umask(previous)


def _mode(path: Path) -> int:
    return stat.S_IMODE(path.lstat().st_mode)


def _source_state(directory: Path) -> dict[str, tuple[bytes, int, int, int, int]]:
    state: dict[str, tuple[bytes, int, int, int, int]] = {}
    for path in sorted(directory.iterdir()):
        metadata = path.lstat()
        if stat.S_ISREG(metadata.st_mode):
            state[path.name] = (
                path.read_bytes(),
                stat.S_IMODE(metadata.st_mode),
                metadata.st_uid,
                metadata.st_mtime_ns,
                metadata.st_ctime_ns,
            )
    return state


def _create_source(database_path: Path, *, live_wal: bool) -> sqlite3.Connection | None:
    with _temporary_umask(0o077):
        connection = sqlite3.connect(database_path)
        mode = "WAL" if live_wal else "DELETE"
        assert connection.execute(f"PRAGMA journal_mode={mode}").fetchone() == (mode.casefold(),)
        if live_wal:
            connection.execute("PRAGMA wal_autocheckpoint=0")
        connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
        connection.execute("INSERT INTO canary VALUES (?)", (mode.casefold(),))
        connection.commit()
    database_path.chmod(0o600)
    if live_wal:
        assert Path(f"{database_path}-wal").exists()
        assert Path(f"{database_path}-shm").exists()
        return connection
    connection.close()
    return None


def _create_exact_8k_database(database_path: Path) -> None:
    with _temporary_umask(0o077):
        with closing(sqlite3.connect(database_path)) as connection:
            connection.execute("PRAGMA page_size=4096")
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
            connection.execute("INSERT INTO canary VALUES ('stable')")
            connection.commit()
            connection.execute("VACUUM")
    database_path.chmod(0o600)
    assert database_path.stat().st_size == 8 * 1024


def _create_database_with_value(database_path: Path, value: str) -> None:
    with _temporary_umask(0o077):
        with closing(sqlite3.connect(database_path)) as connection:
            connection.execute("CREATE TABLE canary (value TEXT NOT NULL)")
            connection.execute("INSERT INTO canary VALUES (?)", (value,))
            connection.commit()
    database_path.chmod(0o600)


@pytest.mark.parametrize("destination_mode", (0o770, 0o707), ids=("group-writable", "world-writable"))
def test_snapshot_publication_rejects_destination_writable_by_another_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination_mode: int,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    database_path = source_root / "history.db"
    _create_source(database_path, live_wal=False)
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    destination_root.chmod(destination_mode)

    monkeypatch.setattr(
        sqlite_identity,
        "_snapshot_sqlite_database",
        lambda *_args, **_kwargs: pytest.fail("snapshot work started before destination admission"),
    )

    with pytest.raises(SQLiteIdentityError) as exc_info:
        snapshot_sqlite_database_set([database_path], destination_root, timeout_ms=2_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE.value
    assert _mode(destination_root) == destination_mode
    assert not any(destination_root.iterdir())


def test_snapshot_publication_rejects_role_path_replacement_during_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    database_path = source_root / "history.db"
    _create_database_with_value(database_path, "trusted")
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    attacker_database = tmp_path / "attacker-during.db"
    _create_database_with_value(attacker_database, "attacker")
    original_replace = sqlite_identity.os.replace
    replaced = False

    def replace_role_with_attacker(source, destination) -> None:
        nonlocal replaced
        original_replace(source, destination)
        destination_path = Path(destination)
        if not replaced and destination_path.parent == destination_root and destination_path.suffix == ".db":
            replaced = True
            original_replace(attacker_database, destination_path)

    monkeypatch.setattr(sqlite_identity.os, "replace", replace_role_with_attacker)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        snapshot_sqlite_database_set([database_path], destination_root, timeout_ms=2_000)

    assert replaced
    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE.value
    assert not (destination_root / ".tacit-sqlite-snapshot-complete.json").exists()


@pytest.mark.parametrize("replacement_target", ("role", "manifest"))
def test_snapshot_publication_rejects_artifact_path_replacement_after_manifest_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_target: str,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    database_path = source_root / "history.db"
    _create_database_with_value(database_path, "trusted")
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    published_database = destination_root / database_path.name
    completion_manifest = destination_root / ".tacit-sqlite-snapshot-complete.json"
    attacker_artifact = tmp_path / "attacker-after.db"
    _create_database_with_value(attacker_artifact, "attacker")
    original_replace = sqlite_identity.os.replace
    replaced = False

    def replace_role_after_manifest(source, destination) -> None:
        nonlocal replaced
        original_replace(source, destination)
        if Path(destination) == completion_manifest:
            replaced = True
            replaced_path = published_database if replacement_target == "role" else completion_manifest
            original_replace(attacker_artifact, replaced_path)

    monkeypatch.setattr(sqlite_identity.os, "replace", replace_role_after_manifest)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        snapshot_sqlite_database_set([database_path], destination_root, timeout_ms=2_000)

    assert replaced
    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE.value
    assert not completion_manifest.exists()


@pytest.mark.parametrize("entry_point", ("readonly", "published"))
@pytest.mark.parametrize("live_wal", (False, True), ids=("clean", "live-wal"))
def test_disposable_snapshot_artifacts_are_owner_only_and_sources_are_immutable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_point: str,
    live_wal: bool,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    source_root.chmod(0o700)
    database_path = source_root / "history.db"
    writer = _create_source(database_path, live_wal=live_wal)
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    destination_mode = 0o777 if entry_point == "readonly" else 0o700
    destination_root.chmod(destination_mode)
    source_before = _source_state(source_root)

    observed_files: list[tuple[str, str, int, int]] = []
    observed_directories: list[tuple[str, int, int]] = []
    original_copy = sqlite_identity._copy_snapshot_component
    original_readonly_connection = sqlite_identity._readonly_connection
    original_replace = sqlite_identity.os.replace

    def record_file(phase: str, path: Path) -> None:
        metadata = path.lstat()
        observed_files.append((phase, path.name, stat.S_IMODE(metadata.st_mode), metadata.st_uid))

    def record_directory(phase: str, path: Path) -> None:
        metadata = path.lstat()
        observed_directories.append((phase, stat.S_IMODE(metadata.st_mode), metadata.st_uid))

    def record_directory_files(phase: str, directory: Path) -> None:
        record_directory(phase, directory)
        for path in sorted(directory.iterdir()):
            if path.is_file():
                record_file(phase, path)

    def observed_copy(source: Path, destination: Path, *, budget, **kwargs) -> None:
        original_copy(source, destination, budget=budget, **kwargs)
        record_file("copied", destination)
        record_directory("copy-directory", destination.parent)

    @contextmanager
    def observed_readonly_connection(path: Path, **kwargs):
        with original_readonly_connection(path, **kwargs) as connection:
            record_directory_files("sqlite-open", path.parent)
            try:
                yield connection
            finally:
                record_directory_files("sqlite-close", path.parent)

    def observed_replace(source, destination) -> None:
        source_path = Path(source)
        destination_path = Path(destination)
        record_directory_files("publication-directory", source_path.parent)
        original_replace(source, destination)
        record_file("published", destination_path)

    monkeypatch.setattr(sqlite_identity, "_copy_snapshot_component", observed_copy)
    monkeypatch.setattr(sqlite_identity, "_readonly_connection", observed_readonly_connection)
    monkeypatch.setattr(sqlite_identity.os, "replace", observed_replace)

    try:
        with _temporary_umask(0o000):
            if entry_point == "readonly":
                with SQLiteDatabaseTarget(database_path).connect_existing_readonly(timeout_ms=2_000) as connection:
                    assert connection is not None
                    assert connection.execute("SELECT value FROM canary").fetchone() == (
                        "wal" if live_wal else "delete",
                    )
            else:
                snapshots = snapshot_sqlite_database_set([database_path], destination_root, timeout_ms=2_000)
                published = snapshots[database_path]
                with closing(sqlite3.connect(published)) as connection:
                    assert connection.execute("SELECT value FROM canary").fetchone() == (
                        "wal" if live_wal else "delete",
                    )

        assert _source_state(source_root) == source_before
        assert observed_files
        assert observed_directories
        assert any(phase == "copied" and name == "snapshot.db" for phase, name, _mode, _uid in observed_files)
        if live_wal:
            assert any(name.endswith("-wal") for _phase, name, _mode, _uid in observed_files)
        if entry_point == "published":
            assert any(phase == "published" for phase, _name, _mode, _uid in observed_files)
        assert {(mode, uid) for _phase, _name, mode, uid in observed_files} == {(0o600, os.geteuid())}
        assert {(mode, uid) for _phase, mode, uid in observed_directories} == {(0o700, os.geteuid())}
        assert _mode(destination_root) == destination_mode
    finally:
        if writer is not None:
            writer.close()


def test_readonly_retries_consume_one_cumulative_byte_and_deadline_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    database_path = source_root / "history.db"
    _create_exact_8k_database(database_path)
    source_before = _source_state(source_root)
    original_readonly_connection = sqlite_identity._readonly_connection
    attempts = 0
    deadlines: list[float] = []

    @contextmanager
    def retry_after_complete_copy(path: Path, **kwargs):
        nonlocal attempts
        attempts += 1
        deadlines.append(kwargs["deadline"])
        with original_readonly_connection(path, **kwargs) as connection:
            yield connection
        if attempts == 1:
            raise SQLiteIdentityError(
                "injected source generation movement",
                SQLiteIdentityRejectionReason.FILE_REPLACED,
            )

    monkeypatch.setattr(sqlite_identity, "_READONLY_ADMISSION_SNAPSHOT_MAX_BYTES", 8 * 1024)
    monkeypatch.setattr(sqlite_identity, "_readonly_connection", retry_after_complete_copy)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        SQLiteDatabaseTarget(database_path).read_existing_readonly(
            lambda connection: connection.execute("SELECT value FROM canary").fetchone(),
            timeout_ms=2_000,
        )

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT.value
    assert attempts == 2
    assert len(set(deadlines)) == 1
    assert _source_state(source_root) == source_before


def test_published_snapshot_retries_consume_one_cumulative_byte_and_deadline_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    database_path = source_root / "history.db"
    _create_exact_8k_database(database_path)
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    source_before = _source_state(source_root)
    original_snapshot = sqlite_identity._snapshot_sqlite_database
    attempts = 0
    deadlines: list[float] = []

    def retry_after_complete_snapshot(*args, **kwargs) -> None:
        nonlocal attempts
        attempts += 1
        deadlines.append(kwargs["deadline"])
        original_snapshot(*args, **kwargs)
        if attempts == 1:
            raise SQLiteIdentityError(
                "injected source generation movement",
                SQLiteIdentityRejectionReason.FILE_REPLACED,
            )

    monkeypatch.setattr(sqlite_identity, "_READONLY_ADMISSION_SNAPSHOT_MAX_BYTES", 8 * 1024)
    monkeypatch.setattr(sqlite_identity, "_snapshot_sqlite_database", retry_after_complete_snapshot)

    with pytest.raises(SQLiteIdentityError) as exc_info:
        snapshot_sqlite_database_set([database_path], destination_root, timeout_ms=2_000)

    assert exc_info.value.reason_code == SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT.value
    assert attempts == 2
    assert len(set(deadlines)) == 1
    assert _source_state(source_root) == source_before
    assert not any(destination_root.iterdir())


def test_snapshot_set_publication_is_committed_only_after_every_role_is_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    sources = [source_root / name for name in ("history.db", "feedback.db", "signals.db")]
    for source in sources:
        _create_source(source, live_wal=False)
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    completion_manifest = destination_root / ".tacit-sqlite-snapshot-complete.json"
    first_role_published = threading.Event()
    allow_publication_to_finish = threading.Event()
    original_replace = sqlite_identity.os.replace
    result: dict[Path, Path] = {}
    failures: list[BaseException] = []
    published_roles = 0

    def pause_after_first_role(source, destination) -> None:
        nonlocal published_roles
        original_replace(source, destination)
        destination_path = Path(destination)
        if destination_path.parent == destination_root and destination_path.suffix == ".db":
            published_roles += 1
            if published_roles == 1:
                first_role_published.set()
                assert allow_publication_to_finish.wait(timeout=5)

    def publish() -> None:
        try:
            result.update(snapshot_sqlite_database_set(sources, destination_root, timeout_ms=5_000))
        except BaseException as exc:
            failures.append(exc)

    monkeypatch.setattr(sqlite_identity.os, "replace", pause_after_first_role)
    publisher = threading.Thread(target=publish)
    publisher.start()
    assert first_role_published.wait(timeout=5)
    assert len([path for path in destination_root.iterdir() if path.suffix == ".db"]) == 1
    assert not completion_manifest.exists()

    allow_publication_to_finish.set()
    publisher.join(timeout=5)
    assert not publisher.is_alive()
    assert not failures
    assert set(result) == set(sources)
    assert completion_manifest.is_file()
    assert _mode(completion_manifest) == 0o600
    manifest = json.loads(completion_manifest.read_text(encoding="utf-8"))
    assert manifest == {
        "files": sorted(source.name for source in sources),
        "format_version": 1,
    }
    assert {path.name for path in result.values()} == set(manifest["files"])
    assert all(path.is_file() for path in result.values())


def test_snapshot_set_interruption_after_first_role_leaves_no_committed_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InjectedPublicationCrash(BaseException):
        pass

    source_root = tmp_path / "authority"
    source_root.mkdir(mode=0o700)
    sources = [source_root / name for name in ("history.db", "signals.db")]
    for source in sources:
        _create_source(source, live_wal=False)
    destination_root = tmp_path / "snapshots"
    destination_root.mkdir(mode=0o700)
    original_replace = sqlite_identity.os.replace
    role_publications = 0

    def crash_after_first_role(source, destination) -> None:
        nonlocal role_publications
        original_replace(source, destination)
        destination_path = Path(destination)
        if destination_path.parent == destination_root and destination_path.suffix == ".db":
            role_publications += 1
            if role_publications == 1:
                raise InjectedPublicationCrash

    monkeypatch.setattr(sqlite_identity.os, "replace", crash_after_first_role)

    with pytest.raises(InjectedPublicationCrash):
        snapshot_sqlite_database_set(sources, destination_root, timeout_ms=5_000)

    assert role_publications == 1
    assert not any(destination_root.iterdir())
