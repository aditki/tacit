"""SQLite storage policy for Tacit's local, protected-path deployment model.

Tacit validates configured paths before opening them, but delegates connection,
WAL, checkpoint, and sidecar lifecycle behavior to SQLite. The containing
directory must not be writable by another identity. Defending against a process
that can replace files as the Tacit service user requires a real SQLite VFS or a
server database and is intentionally outside this module's contract.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from typing import Any, NoReturn, TypeVar

import structlog

from tacit.errors import RuntimeOwnershipError

_IDENTITY_TABLE = "tacit_runtime_database_identity"
_SUPPORTED_ROLES = frozenset({"history", "feedback", "signals"})
_PROTECTED_PATH_PLATFORM_SUPPORTED = os.name == "posix"
_SYSTEM_ROOT_ALIASES = (Path("/var"), Path("/tmp"), Path("/etc"))
_SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_READONLY_ADMISSION_SNAPSHOT_MAX_BYTES = 256 * 1024 * 1024
_READONLY_ADMISSION_COPY_CHUNK_BYTES = 1024 * 1024

logger = structlog.get_logger()

_HISTORY_ROLE_TABLES = frozenset(
    {
        "history_migration_progress",
        "history_schema_metadata",
        "investigation_events",
        "investigation_revisions",
        "investigation_runs",
        "investigation_snapshots",
        "investigation_tenant_assignments",
        "investigations",
    }
)
_FEEDBACK_ROLE_TABLES = frozenset(
    {
        "dashboard_provenance",
        "dashboard_provenance_legacy_tenant",
        "dashboard_provenance_tenant_migration_v2",
        "feedback",
        "feedback_legacy_tenant",
        "feedback_tenant_migration_v2",
        "feedback_tenant_migration_metadata",
    }
)
_SIGNAL_ROLE_TABLES = frozenset(
    {
        "dependency_hints",
        "evidence_requirements",
        "ingested_alerts",
        "ingested_dashboards",
        "learned_artifacts",
        "learning_context_fts",
        "learning_context_fts_config",
        "learning_context_fts_content",
        "learning_context_fts_data",
        "learning_context_fts_docsize",
        "learning_context_fts_idx",
        "ownership_hints",
        "rejected_signal_candidates",
        "signal_mapping_candidates",
        "signal_mapping_source_refs",
        "signal_metric_mappings",
        "signal_migration_quarantine",
        "signal_tenant_migration_metadata",
        "signal_types",
        "tenant_signal_types",
    }
)
_KNOWLEDGE_ROLE_TABLES = frozenset(
    {
        "candidate_promotions",
        "corroboration_snapshots",
        "entities",
        "entity_aliases",
        "entity_resolution_attempts",
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
        "operational_knowledge",
        "operational_knowledge_revisions",
        "promotion_decisions",
        "proposition_candidates",
    }
)
_SIGNAL_MIGRATION_TABLES = (
    frozenset(
        {
            "dependency_hints_old",
            "evidence_requirements_old",
            "ingested_alerts_old",
            "ingested_dashboards_old",
            "learned_artifacts_old",
            "learning_context_fts_old",
            "learning_context_fts_old_config",
            "learning_context_fts_old_content",
            "learning_context_fts_old_data",
            "learning_context_fts_old_docsize",
            "learning_context_fts_old_idx",
            "ownership_hints_old",
            "signal_mapping_candidates_old",
            "signal_metric_mappings_old",
        }
    )
    | frozenset(
        f"{table}_tacit_tenant_migration_v1"
        for table in (
            "dependency_hints",
            "evidence_requirements",
            "ingested_alerts",
            "ingested_dashboards",
            "learned_artifacts",
            "learning_context_fts",
            "ownership_hints",
            "signal_mapping_candidates",
            "signal_metric_mappings",
        )
    )
    | frozenset(
        f"learning_context_fts_tacit_tenant_migration_v1_{suffix}"
        for suffix in ("config", "content", "data", "docsize", "idx")
    )
)

# This registry includes every canonical and known interrupted-migration table
# whose name is distinctive to one Tacit database role. The two stores that use
# knowledge_candidates are distinguished by required columns below.
_ROLE_SIGNATURES = {
    "history": _HISTORY_ROLE_TABLES,
    "feedback": _FEEDBACK_ROLE_TABLES,
    "signals": _SIGNAL_ROLE_TABLES | _KNOWLEDGE_ROLE_TABLES | _SIGNAL_MIGRATION_TABLES,
}
_SHARED_TABLE_ROLE_SIGNATURES = {
    "knowledge_candidates": {
        "history": frozenset({"investigation_id", "revision", "correction_text", "provenance_json"}),
        "signals": frozenset({"tenant_id", "kind", "proposition_key", "candidate_json"}),
    }
}


class SQLiteIdentityRejectionReason(StrEnum):
    """Stable, non-sensitive SQLite storage rejection taxonomy."""

    INVALID_PATH = "sqlite_invalid_path"
    UNSUPPORTED_PLATFORM = "sqlite_unsupported_platform"
    PARENT_MISSING = "sqlite_parent_missing"
    PARENT_INVALID = "sqlite_parent_invalid"
    PARENT_UNTRUSTED = "sqlite_parent_untrusted"
    SYMLINK = "sqlite_symlink"
    SPECIAL_FILE = "sqlite_special_file"
    FILE_UNTRUSTED = "sqlite_file_untrusted"
    HARD_LINK = "sqlite_hard_link"
    SECURE_OPEN_FAILED = "sqlite_secure_open_failed"
    FILE_REPLACED = "sqlite_file_replaced"
    CONNECTION_IDENTITY = "sqlite_connection_identity"
    WAL_UNAVAILABLE = "sqlite_wal_unavailable"
    ADMISSION_SNAPSHOT_LIMIT = "sqlite_admission_snapshot_limit"
    ADMISSION_TIMEOUT = "sqlite_admission_timeout"
    ADMISSION_RECOVERY_REQUIRED = "sqlite_admission_recovery_required"
    ROLE_INVALID = "sqlite_role_invalid"
    ROLE_COLLISION = "sqlite_role_collision"
    ROLE_IDENTITY = "sqlite_role_identity"


SQLiteIdentityRejectionHook = Callable[[SQLiteIdentityRejectionReason], None]
SQLiteIdentityVerificationHook = Callable[[str], None]
_ReadResult = TypeVar("_ReadResult")


class SQLiteIdentityError(RuntimeOwnershipError):
    """Raised when a SQLite target violates the supported storage contract."""

    def __init__(
        self,
        message: str,
        reason: SQLiteIdentityRejectionReason | str = SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
    ) -> None:
        self.reason = SQLiteIdentityRejectionReason(reason)
        self.reason_code = self.reason.value
        super().__init__(message)

    def __reduce__(self):
        return type(self), (str(self), self.reason)


def _reject(
    reason: SQLiteIdentityRejectionReason,
    message: str,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
    cause: BaseException | None = None,
) -> NoReturn:
    logger.warning("sqlite_identity_rejected", reason_code=reason.value)
    if rejection_hook is not None:
        rejection_hook(reason)
    error = SQLiteIdentityError(message, reason)
    if cause is None:
        raise error
    raise error from cause


def sqlite_database_path(
    value: str | Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> Path:
    """Return an absolute SQLite path without resolving application links."""
    if not _PROTECTED_PATH_PLATFORM_SUPPORTED:
        _reject(
            SQLiteIdentityRejectionReason.UNSUPPORTED_PLATFORM,
            "SQLite protected-path storage is unavailable on this platform",
            rejection_hook=rejection_hook,
        )
    try:
        expanded = os.fspath(Path(value).expanduser())
        path = Path(os.path.abspath(expanded))
        if os.name == "posix":
            for alias in _SYSTEM_ROOT_ALIASES:
                try:
                    relative = path.relative_to(alias)
                except ValueError:
                    continue
                if alias.is_symlink():
                    path = alias.resolve(strict=True) / relative
                break
        if not path.name:
            raise ValueError("database filename is empty")
        return path
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        _reject(
            SQLiteIdentityRejectionReason.INVALID_PATH,
            "SQLite database path is invalid",
            rejection_hook=rejection_hook,
            cause=exc,
        )


def _path_components(path: Path) -> tuple[Path, ...]:
    root = Path(path.anchor) if path.anchor else Path(os.curdir).absolute()
    current = root
    components: list[Path] = []
    start = 1 if path.anchor else 0
    for component in path.parts[start:-1]:
        current /= component
        components.append(current)
    return tuple(components)


def _validate_parent_metadata(
    path: Path,
    metadata: os.stat_result,
    *,
    final_parent: bool,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    if stat.S_ISLNK(metadata.st_mode):
        _reject(
            SQLiteIdentityRejectionReason.SYMLINK,
            "SQLite database path must not contain a symbolic link",
            rejection_hook=rejection_hook,
        )
    if not stat.S_ISDIR(metadata.st_mode):
        _reject(
            SQLiteIdentityRejectionReason.PARENT_INVALID,
            "SQLite database parent path must be a directory",
            rejection_hook=rejection_hook,
        )
    if os.name != "posix":
        return
    if metadata.st_uid not in {0, os.geteuid()}:
        _reject(
            SQLiteIdentityRejectionReason.PARENT_UNTRUSTED,
            "SQLite database ancestors must be owned by root or the service identity",
            rejection_hook=rejection_hook,
        )
    writable_by_others = bool(metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH))
    sticky_root = bool(metadata.st_mode & stat.S_ISVTX) and metadata.st_uid == 0
    if writable_by_others and (final_parent or not sticky_root):
        _reject(
            SQLiteIdentityRejectionReason.PARENT_UNTRUSTED,
            "SQLite database parent must not be writable by another identity",
            rejection_hook=rejection_hook,
        )


def _validate_file_metadata(
    metadata: os.stat_result,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
    reject_hard_links: bool = True,
    allow_unlinked: bool = False,
) -> None:
    if stat.S_ISLNK(metadata.st_mode):
        _reject(
            SQLiteIdentityRejectionReason.SYMLINK,
            "SQLite database files must not be symbolic links",
            rejection_hook=rejection_hook,
        )
    if not stat.S_ISREG(metadata.st_mode):
        _reject(
            SQLiteIdentityRejectionReason.SPECIAL_FILE,
            "SQLite database targets must be regular files",
            rejection_hook=rejection_hook,
        )
    invalid_link_count = metadata.st_nlink > 1 or (metadata.st_nlink == 0 and not allow_unlinked)
    if reject_hard_links and invalid_link_count:
        _reject(
            SQLiteIdentityRejectionReason.HARD_LINK,
            "SQLite database targets must not have multiple hard links",
            rejection_hook=rejection_hook,
        )
    if os.name != "posix":
        return
    if metadata.st_uid != os.geteuid() or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        _reject(
            SQLiteIdentityRejectionReason.FILE_UNTRUSTED,
            "SQLite database files must be owned by and writable only by the service identity",
            rejection_hook=rejection_hook,
        )


def inspect_sqlite_database_target(
    value: str | Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> os.stat_result | None:
    """Inspect an existing configured target without creating or opening it."""
    path = sqlite_database_path(value, rejection_hook=rejection_hook)
    for component in _path_components(path):
        try:
            parent_metadata = component.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            _reject(
                SQLiteIdentityRejectionReason.PARENT_INVALID,
                "SQLite database parent path could not be inspected",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        _validate_parent_metadata(
            component,
            parent_metadata,
            final_parent=component == path.parent,
            rejection_hook=rejection_hook,
        )
    target_metadata: os.stat_result | None
    try:
        target_metadata = path.lstat()
    except FileNotFoundError:
        target_metadata = None
    except OSError as exc:
        _reject(
            SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
            "SQLite database path could not be inspected",
            rejection_hook=rejection_hook,
            cause=exc,
        )
    if target_metadata is not None:
        _validate_file_metadata(
            target_metadata,
            rejection_hook=rejection_hook,
            reject_hard_links=False,
        )
    for suffix in _SQLITE_SIDECAR_SUFFIXES:
        sidecar = Path(f"{path}{suffix}")
        try:
            sidecar_metadata = sidecar.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            _reject(
                SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
                "SQLite sidecar could not be inspected",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        _validate_file_metadata(
            sidecar_metadata,
            rejection_hook=rejection_hook,
            allow_unlinked=True,
        )
    return target_metadata


def _ensure_parent_directories(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    for component in _path_components(path):
        try:
            metadata = component.lstat()
        except FileNotFoundError:
            try:
                component.mkdir(mode=0o700)
            except FileExistsError:
                pass
            except OSError as exc:
                _reject(
                    SQLiteIdentityRejectionReason.PARENT_INVALID,
                    "SQLite database parent directory could not be created",
                    rejection_hook=rejection_hook,
                    cause=exc,
                )
            try:
                metadata = component.lstat()
            except OSError as exc:
                _reject(
                    SQLiteIdentityRejectionReason.PARENT_INVALID,
                    "SQLite database parent directory could not be inspected",
                    rejection_hook=rejection_hook,
                    cause=exc,
                )
        except OSError as exc:
            _reject(
                SQLiteIdentityRejectionReason.PARENT_INVALID,
                "SQLite database parent path could not be inspected",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        _validate_parent_metadata(
            component,
            metadata,
            final_parent=component == path.parent,
            rejection_hook=rejection_hook,
        )


def _create_database_file(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        return
    except OSError as exc:
        _reject(
            SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
            "SQLite database file could not be created",
            rejection_hook=rejection_hook,
            cause=exc,
        )
    else:
        os.close(descriptor)


def _prepare_database_path(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    _ensure_parent_directories(path, rejection_hook=rejection_hook)
    inspect_sqlite_database_target(path, rejection_hook=rejection_hook)
    _create_database_file(path, rejection_hook=rejection_hook)
    try:
        metadata = path.lstat()
    except OSError as exc:
        _reject(
            SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
            "SQLite database file could not be inspected",
            rejection_hook=rejection_hook,
            cause=exc,
        )
    _validate_file_metadata(metadata, rejection_hook=rejection_hook)
    for suffix in _SQLITE_SIDECAR_SUFFIXES:
        sidecar = Path(f"{path}{suffix}")
        try:
            sidecar_metadata = sidecar.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            _reject(
                SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
                "SQLite sidecar could not be inspected",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        _validate_file_metadata(
            sidecar_metadata,
            rejection_hook=rejection_hook,
            allow_unlinked=True,
        )


def activate_sqlite_wal(
    connection: sqlite3.Connection,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
    timeout_ms: int = 30_000,
) -> str:
    """Enable WAL and fail unless SQLite confirms the exact mode."""
    deadline = time.monotonic() + max(timeout_ms, 0) / 1_000
    delay = 0.005
    while True:
        try:
            current = connection.execute("PRAGMA journal_mode").fetchone()
            if current and str(current[0]).casefold() == "wal":
                return "wal"
            observed = connection.execute("PRAGMA journal_mode=WAL").fetchone()
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).casefold() or time.monotonic() >= deadline:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 0.1)
    mode = str(observed[0]).casefold() if observed else ""
    if mode != "wal":
        _reject(
            SQLiteIdentityRejectionReason.WAL_UNAVAILABLE,
            "SQLite WAL mode is required",
            rejection_hook=rejection_hook,
        )
    return mode


_ReadOnlyFileState = tuple[int, int, int, int, int]


def _require_admission_deadline(
    deadline: float,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    if time.monotonic() >= deadline:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_TIMEOUT,
            "SQLite read-only admission exceeded its deadline",
            rejection_hook=rejection_hook,
        )


class _ReadOnlyAdmissionBudget:
    """Shared byte and time budget for one admission attempt."""

    def __init__(
        self,
        *,
        deadline: float,
        max_bytes: int,
        rejection_hook: SQLiteIdentityRejectionHook | None = None,
    ) -> None:
        self.deadline = deadline
        self.max_bytes = max_bytes
        self.copied_bytes = 0
        self._rejection_hook = rejection_hook

    def require_time(self) -> None:
        _require_admission_deadline(
            self.deadline,
            rejection_hook=self._rejection_hook,
        )

    def reserve(self, byte_count: int) -> None:
        if byte_count < 0 or self.copied_bytes + byte_count > self.max_bytes:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
                "SQLite live-WAL admission snapshot exceeds the supported bound",
                rejection_hook=self._rejection_hook,
            )
        self.copied_bytes += byte_count


def _copy_snapshot_component(
    source: Path,
    destination: Path,
    *,
    budget: _ReadOnlyAdmissionBudget,
) -> None:
    """Copy one regular SQLite component without exceeding the shared budget."""
    budget.require_time()
    with source.open("rb") as input_file, destination.open("xb") as output_file:
        while True:
            budget.require_time()
            remaining = budget.max_bytes - budget.copied_bytes
            chunk = input_file.read(min(_READONLY_ADMISSION_COPY_CHUNK_BYTES, remaining + 1))
            budget.require_time()
            if not chunk:
                return
            budget.reserve(len(chunk))
            output_file.write(chunk)
            budget.require_time()


def _readonly_file_state(path: Path) -> _ReadOnlyFileState | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
        int(metadata.st_ctime_ns),
    )


def _readonly_source_state(path: Path) -> dict[str, _ReadOnlyFileState]:
    state: dict[str, _ReadOnlyFileState] = {}
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-journal")):
        observed = _readonly_file_state(candidate)
        if observed is not None:
            state[candidate.name] = observed
    return state


@contextmanager
def _readonly_connection(
    path: Path,
    *,
    timeout_ms: int,
    immutable: bool,
    deadline: float,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> Iterator[sqlite3.Connection]:
    immutable_query = "&immutable=1" if immutable else ""
    uri = f"{path.as_uri()}?mode=ro{immutable_query}"
    connection: sqlite3.Connection | None = None
    try:
        _require_admission_deadline(deadline, rejection_hook=rejection_hook)
        connection = sqlite3.connect(
            uri,
            uri=True,
            timeout=timeout_ms / 1_000,
        )
        connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1_000)
        connection.execute(f"PRAGMA busy_timeout={max(int(timeout_ms), 0)}")
        connection.execute("PRAGMA query_only=ON")
        require_sqlite_connection_path(
            connection,
            path=path,
            rejection_hook=rejection_hook,
        )
        _require_admission_deadline(deadline, rejection_hook=rejection_hook)
        yield connection
        _require_admission_deadline(deadline, rejection_hook=rejection_hook)
    except sqlite3.OperationalError as exc:
        if time.monotonic() >= deadline:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_TIMEOUT,
                "SQLite read-only admission exceeded its deadline",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        raise
    finally:
        if connection is not None:
            connection.set_progress_handler(None, 0)
            connection.close()


class SQLiteDatabaseTarget:
    """Open one SQLite role path under Tacit's protected-path policy."""

    def __init__(
        self,
        path: str | Path,
        *,
        rejection_hook: SQLiteIdentityRejectionHook | None = None,
        verification_hook: SQLiteIdentityVerificationHook | None = None,
    ) -> None:
        self.path = sqlite_database_path(path, rejection_hook=rejection_hook)
        self._rejection_hook = rejection_hook
        self._verification_hook = verification_hook

    def connect(
        self,
        *,
        timeout_ms: int,
        **connect_kwargs: Any,
    ) -> sqlite3.Connection:
        """Open a standard SQLite connection after protected-path validation."""
        if "factory" in connect_kwargs:
            raise TypeError("SQLiteDatabaseTarget owns the SQLite connection factory")
        if connect_kwargs.get("uri"):
            raise TypeError("SQLiteDatabaseTarget treats configured paths as literal filenames")
        _prepare_database_path(self.path, rejection_hook=self._rejection_hook)
        try:
            connection = sqlite3.connect(
                str(self.path),
                timeout=timeout_ms / 1_000,
                **connect_kwargs,
            )
            connection.execute(f"PRAGMA busy_timeout={max(int(timeout_ms), 0)}")
            require_sqlite_connection_path(
                connection,
                path=self.path,
                rejection_hook=self._rejection_hook,
            )
            if self._verification_hook is not None:
                self._verification_hook("after_connect")
            return connection
        except Exception:
            if "connection" in locals():
                connection.close()
            raise

    @contextmanager
    def connect_existing_readonly(
        self,
        *,
        timeout_ms: int,
        _deadline: float | None = None,
    ) -> Iterator[sqlite3.Connection | None]:
        """Yield a nonmutating admission snapshot for a trusted bounded reader."""
        deadline = _deadline or time.monotonic() + max(timeout_ms, 1) / 1_000
        if not self.path.exists():
            yield None
            return
        _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
        inspect_sqlite_database_target(self.path, rejection_hook=self._rejection_hook)
        source_state = _readonly_source_state(self.path)
        _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
        wal_path = Path(f"{self.path}-wal")
        journal_path = Path(f"{self.path}-journal")
        journal_state = source_state.get(journal_path.name)
        if journal_state is not None and journal_state[2] > 0:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                "SQLite rollback recovery must complete before read-only admission",
                rejection_hook=self._rejection_hook,
            )
        wal_state = source_state.get(wal_path.name)
        if wal_state is not None and wal_state[2] > 0:
            main_state = source_state.get(self.path.name)
            snapshot_bytes = (main_state[2] if main_state is not None else 0) + wal_state[2]
            if snapshot_bytes > _READONLY_ADMISSION_SNAPSHOT_MAX_BYTES:
                _reject(
                    SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
                    "SQLite live-WAL admission snapshot exceeds the supported bound",
                    rejection_hook=self._rejection_hook,
                )
            with tempfile.TemporaryDirectory(prefix="tacit-sqlite-admission-") as directory:
                snapshot_path = Path(directory) / "snapshot.db"
                copy_started = time.perf_counter()
                budget = _ReadOnlyAdmissionBudget(
                    deadline=deadline,
                    max_bytes=_READONLY_ADMISSION_SNAPSHOT_MAX_BYTES,
                    rejection_hook=self._rejection_hook,
                )
                try:
                    _copy_snapshot_component(self.path, snapshot_path, budget=budget)
                    _copy_snapshot_component(wal_path, Path(f"{snapshot_path}-wal"), budget=budget)
                except SQLiteIdentityError:
                    raise
                except OSError as exc:
                    _reject(
                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                        "SQLite database changed during read-only admission",
                        rejection_hook=self._rejection_hook,
                        cause=exc,
                    )
                logger.info(
                    "sqlite_readonly_admission_snapshot",
                    snapshot_bytes=budget.copied_bytes,
                    copy_duration_ms=round((time.perf_counter() - copy_started) * 1_000, 3),
                )
                budget.require_time()
                if _readonly_source_state(self.path) != source_state:
                    _reject(
                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                        "SQLite database changed during read-only admission",
                        rejection_hook=self._rejection_hook,
                    )
                with _readonly_connection(
                    snapshot_path,
                    timeout_ms=timeout_ms,
                    immutable=False,
                    deadline=deadline,
                    rejection_hook=self._rejection_hook,
                ) as connection:
                    yield connection
                budget.require_time()
                if _readonly_source_state(self.path) != source_state:
                    _reject(
                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                        "SQLite database changed during read-only admission",
                        rejection_hook=self._rejection_hook,
                    )
            return

        try:
            with _readonly_connection(
                self.path,
                timeout_ms=timeout_ms,
                immutable=True,
                deadline=deadline,
                rejection_hook=self._rejection_hook,
            ) as connection:
                yield connection
        except sqlite3.DatabaseError as exc:
            if _readonly_source_state(self.path) != source_state:
                _reject(
                    SQLiteIdentityRejectionReason.FILE_REPLACED,
                    "SQLite database changed during read-only admission",
                    rejection_hook=self._rejection_hook,
                    cause=exc,
                )
            raise
        _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
        if _readonly_source_state(self.path) != source_state:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "SQLite database changed during read-only admission",
                rejection_hook=self._rejection_hook,
            )

    def read_existing_readonly(
        self,
        reader: Callable[[sqlite3.Connection], _ReadResult],
        *,
        timeout_ms: int,
    ) -> _ReadResult | None:
        """Run one admission read, retrying only concurrent source movement."""
        deadline = time.monotonic() + max(timeout_ms, 1) / 1_000
        attempt = 0
        while True:
            _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1_000))
            try:
                with self.connect_existing_readonly(
                    timeout_ms=remaining_ms,
                    _deadline=deadline,
                ) as connection:
                    if connection is None:
                        return None
                    return reader(connection)
            except SQLiteIdentityError as exc:
                retryable_reasons = {
                    SQLiteIdentityRejectionReason.FILE_REPLACED,
                    SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                }
                if exc.reason not in retryable_reasons or time.monotonic() >= deadline:
                    raise
                attempt += 1
                delay = min(0.005 * (2 ** min(attempt - 1, 5)), 0.1, max(deadline - time.monotonic(), 0))
                logger.info(
                    "sqlite_readonly_admission_retry",
                    reason_code=exc.reason_code,
                    attempt=attempt,
                )
                if delay > 0:
                    time.sleep(delay)

    def bind_connection(self, connection: sqlite3.Connection) -> None:
        """Require an external transaction to use this target's main database."""
        require_sqlite_connection_path(
            connection,
            path=self.path,
            rejection_hook=self._rejection_hook,
        )


def _snapshot_sqlite_database(
    target: SQLiteDatabaseTarget,
    destination: Path,
    *,
    deadline: float,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    """Materialize one admitted source into a disposable SQLite database."""
    remaining_ms = max(1, int((deadline - time.monotonic()) * 1_000))
    with target.connect_existing_readonly(timeout_ms=remaining_ms, _deadline=deadline) as source:
        if source is None:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "SQLite snapshot source disappeared during admission",
                rejection_hook=rejection_hook,
            )
        destination_connection: sqlite3.Connection | None = None
        try:
            destination_connection = sqlite3.connect(destination)

            def require_backup_deadline(_status: int, _remaining: int, _total: int) -> None:
                _require_admission_deadline(deadline, rejection_hook=rejection_hook)

            source.backup(
                destination_connection,
                pages=256,
                progress=require_backup_deadline,
                sleep=0.001,
            )
            _require_admission_deadline(deadline, rejection_hook=rejection_hook)
        finally:
            if destination_connection is not None:
                destination_connection.close()


def _sqlite_source_set_state(
    targets: Sequence[SQLiteDatabaseTarget],
) -> tuple[tuple[Path, tuple[tuple[str, _ReadOnlyFileState], ...]], ...]:
    return tuple((target.path, tuple(sorted(_readonly_source_state(target.path).items()))) for target in targets)


def snapshot_sqlite_database_set(
    sources: Sequence[str | Path],
    destination_dir: str | Path,
    *,
    timeout_ms: int = 30_000,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> dict[Path, Path]:
    """Snapshot one stable generation of a protected SQLite source set.

    Source databases are opened only through the nonmutating admission path.
    If any main/WAL generation moves while the set is copied, every disposable
    copy is discarded and the complete set is retried under one deadline.
    """
    if not sources:
        raise ValueError("SQLite snapshot source set must not be empty")
    destination_root = Path(destination_dir)
    if destination_root.is_symlink() or not destination_root.is_dir():
        raise ValueError("SQLite snapshot destination must be a real directory")

    targets = tuple(SQLiteDatabaseTarget(source, rejection_hook=rejection_hook) for source in sources)
    source_paths = tuple(target.path for target in targets)
    if len(set(source_paths)) != len(source_paths):
        raise ValueError("SQLite snapshot source paths must be unique")
    destination_names = tuple(path.name for path in source_paths)
    if len(set(destination_names)) != len(destination_names):
        raise ValueError("SQLite snapshot source filenames must be unique")
    destinations = {source_path: destination_root / source_path.name for source_path in source_paths}
    if any(destination.exists() or destination.is_symlink() for destination in destinations.values()):
        raise ValueError("SQLite snapshot destinations must not already exist")

    deadline = time.monotonic() + max(timeout_ms, 1) / 1_000
    attempt = 0
    while True:
        _require_admission_deadline(deadline, rejection_hook=rejection_hook)
        try:
            for target in targets:
                inspect_sqlite_database_target(target.path, rejection_hook=rejection_hook)
            source_state = _sqlite_source_set_state(targets)
            if any(not state for _path, state in source_state):
                _reject(
                    SQLiteIdentityRejectionReason.FILE_REPLACED,
                    "SQLite snapshot source set is incomplete",
                    rejection_hook=rejection_hook,
                )

            with tempfile.TemporaryDirectory(
                prefix=".tacit-sqlite-snapshot-",
                dir=destination_root,
            ) as attempt_directory:
                attempt_root = Path(attempt_directory)
                for target in targets:
                    _snapshot_sqlite_database(
                        target,
                        attempt_root / target.path.name,
                        deadline=deadline,
                        rejection_hook=rejection_hook,
                    )
                _require_admission_deadline(deadline, rejection_hook=rejection_hook)
                if _sqlite_source_set_state(targets) != source_state:
                    _reject(
                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                        "SQLite source set changed while snapshots were copied",
                        rejection_hook=rejection_hook,
                    )

                published: list[Path] = []
                try:
                    for target in targets:
                        destination = destinations[target.path]
                        os.replace(attempt_root / target.path.name, destination)
                        published.append(destination)
                except Exception:
                    for destination in published:
                        destination.unlink(missing_ok=True)
                    raise
            return destinations
        except SQLiteIdentityError as exc:
            retryable_reasons = {
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
            }
            if exc.reason not in retryable_reasons or time.monotonic() >= deadline:
                raise
            attempt += 1
            delay = min(0.005 * (2 ** min(attempt - 1, 5)), 0.1, max(deadline - time.monotonic(), 0))
            logger.info(
                "sqlite_snapshot_source_set_retry",
                reason_code=exc.reason_code,
                attempt=attempt,
            )
            if delay > 0:
                time.sleep(delay)


def connect_sqlite_database(
    path: str | Path,
    *,
    timeout_ms: int,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
    verification_hook: SQLiteIdentityVerificationHook | None = None,
    **connect_kwargs: Any,
) -> sqlite3.Connection:
    """Open one regular SQLite file under the protected-path policy."""
    return SQLiteDatabaseTarget(
        path,
        rejection_hook=rejection_hook,
        verification_hook=verification_hook,
    ).connect(timeout_ms=timeout_ms, **connect_kwargs)


def _connection_main_path(connection: sqlite3.Connection) -> Path | None:
    rows = connection.execute("PRAGMA database_list").fetchall()
    for row in rows:
        if str(row[1]) == "main" and str(row[2]):
            return sqlite_database_path(str(row[2]))
    return None


def require_sqlite_connection_path(
    connection: sqlite3.Connection,
    *,
    path: str | Path,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> None:
    """Require an opened connection to use the configured main database path."""
    expected = sqlite_database_path(path, rejection_hook=rejection_hook)
    try:
        actual = _connection_main_path(connection)
    except sqlite3.Error as exc:
        _reject(
            SQLiteIdentityRejectionReason.CONNECTION_IDENTITY,
            "External connection could not report its SQLite database",
            rejection_hook=rejection_hook,
            cause=exc,
        )
    if actual != expected:
        _reject(
            SQLiteIdentityRejectionReason.CONNECTION_IDENTITY,
            "External connection must use the same SQLite database",
            rejection_hook=rejection_hook,
        )


def _table_names(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        str(row[0]) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    )


def _existing_role(
    connection: sqlite3.Connection,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> str | None:
    table_names = _table_names(connection)
    matched = {role for role, signatures in _ROLE_SIGNATURES.items() if table_names.intersection(signatures)}
    for table_name, role_signatures in _SHARED_TABLE_ROLE_SIGNATURES.items():
        if table_name not in table_names:
            continue
        columns = frozenset(str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table_name}")').fetchall())
        shape_matches = {role for role, required_columns in role_signatures.items() if required_columns <= columns}
        if len(shape_matches) != 1:
            _reject(
                SQLiteIdentityRejectionReason.ROLE_COLLISION,
                "SQLite database contains a malformed or ambiguous shared role table",
                rejection_hook=rejection_hook,
            )
        matched.update(shape_matches)
    if len(matched) > 1:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_COLLISION,
            "SQLite database contains tables from multiple database roles",
            rejection_hook=rejection_hook,
        )
    return next(iter(matched), None)


def _read_identity(
    connection: sqlite3.Connection,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> tuple[str, str] | None:
    if _IDENTITY_TABLE not in _table_names(connection):
        return None
    rows = connection.execute(f"SELECT role, database_id FROM {_IDENTITY_TABLE} WHERE singleton=1").fetchall()
    if len(rows) != 1:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_IDENTITY,
            "SQLite database role identity is invalid",
            rejection_hook=rejection_hook,
        )
    role, database_id = str(rows[0][0]), str(rows[0][1])
    if role not in _SUPPORTED_ROLES or not database_id:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_IDENTITY,
            "SQLite database role identity is invalid",
            rejection_hook=rejection_hook,
        )
    return role, database_id


def require_sqlite_database_identity(
    connection: sqlite3.Connection,
    *,
    role: str,
    expected_database_id: str | None,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> str | None:
    """Validate an opened database without mutating an unclaimed legacy file."""
    normalized_role = str(role or "").strip().casefold()
    if normalized_role not in _SUPPORTED_ROLES:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_INVALID,
            "SQLite database role is invalid",
            rejection_hook=rejection_hook,
        )

    identity = _read_identity(connection, rejection_hook=rejection_hook)
    if identity is None and expected_database_id is not None:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_IDENTITY,
            "SQLite database identity changed after initialization",
            rejection_hook=rejection_hook,
        )
    if identity is None:
        existing_role = _existing_role(connection, rejection_hook=rejection_hook)
        if existing_role is not None and existing_role != normalized_role:
            _reject(
                SQLiteIdentityRejectionReason.ROLE_COLLISION,
                "SQLite database role conflicts with existing schema",
                rejection_hook=rejection_hook,
            )
        return None

    actual_role, database_id = identity
    if actual_role != normalized_role:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_COLLISION,
            "SQLite database role does not match the requested store role",
            rejection_hook=rejection_hook,
        )
    if expected_database_id is not None and database_id != expected_database_id:
        _reject(
            SQLiteIdentityRejectionReason.ROLE_IDENTITY,
            "SQLite database identity changed after initialization",
            rejection_hook=rejection_hook,
        )
    return database_id


def claim_sqlite_database_identity(
    connection: sqlite3.Connection,
    *,
    role: str,
    expected_database_id: str | None,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> str:
    """Claim an owner-validated database inside the caller's transaction."""
    normalized_role = str(role or "").strip().casefold()
    database_id = require_sqlite_database_identity(
        connection,
        role=normalized_role,
        expected_database_id=expected_database_id,
        rejection_hook=rejection_hook,
    )
    if database_id is not None:
        return database_id

    database_id = uuid.uuid4().hex
    connection.execute(f"""CREATE TABLE {_IDENTITY_TABLE} (
            singleton INTEGER PRIMARY KEY CHECK (singleton=1),
            role TEXT NOT NULL,
            database_id TEXT NOT NULL
        )""")
    connection.execute(
        f"INSERT INTO {_IDENTITY_TABLE} (singleton, role, database_id) VALUES (1, ?, ?)",
        (normalized_role, database_id),
    )
    return database_id
