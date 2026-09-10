"""SQLite storage policy for Tacit's local, protected-path deployment model.

Tacit validates configured paths before opening them, but delegates connection,
WAL, checkpoint, and sidecar lifecycle behavior to SQLite. The containing
directory must not be writable by another identity. Defending against a process
that can replace files as the Tacit service user requires a real SQLite VFS or a
server database and is intentionally outside this module's contract.
"""

from __future__ import annotations

import errno
import json
import os
import sqlite3
import stat
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
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
DEFAULT_SQLITE_SNAPSHOT_MAX_BYTES = 1024 * 1024 * 1024
# Kept as the runtime default indirection for compatibility with focused tests
# that exercise the default policy. New runtime owners pass an explicit value.
_READONLY_ADMISSION_SNAPSHOT_MAX_BYTES = DEFAULT_SQLITE_SNAPSHOT_MAX_BYTES
_READONLY_ADMISSION_COPY_CHUNK_BYTES = 1024 * 1024
_SNAPSHOT_FILE_MODE = 0o600
_SNAPSHOT_DIRECTORY_MODE = 0o700
_SNAPSHOT_COMPLETION_MANIFEST = ".tacit-sqlite-snapshot-complete.json"
_SNAPSHOT_COMPLETION_FORMAT_VERSION = 1
_SQLITE_ADMISSION_ROLES = frozenset(
    {"benchmark", "feedback", "history", "knowledge", "signals", "snapshot", "unspecified"}
)

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
    ADMISSION_STORAGE_UNAVAILABLE = "sqlite_admission_storage_unavailable"
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
_SnapshotFileState = _ReadOnlyFileState
_SnapshotDirectoryIdentity = tuple[int, int, int, int]
_SnapshotArtifactIdentity = tuple[int, int, int, int, int, int]


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


def validate_sqlite_snapshot_max_bytes(value: int, *, field_name: str = "snapshot_max_bytes") -> int:
    """Validate one explicit byte capacity without touching its database path."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return value


def runtime_sqlite_snapshot_max_bytes(
    runtime_settings: object | None,
    explicit_value: int | None = None,
) -> int:
    """Resolve the public runtime setting while retaining an explicit override."""
    selected = explicit_value
    if selected is None and runtime_settings is not None:
        selected = getattr(runtime_settings, "sqlite_snapshot_max_bytes", None)
    if selected is None:
        selected = DEFAULT_SQLITE_SNAPSHOT_MAX_BYTES
    return validate_sqlite_snapshot_max_bytes(
        selected,
        field_name="sqlite_snapshot_max_bytes",
    )


class _ReadOnlyAdmissionBudget:
    """Shared byte and time budget for one complete admission operation."""

    def __init__(
        self,
        *,
        deadline: float,
        max_bytes: int,
        rejection_hook: SQLiteIdentityRejectionHook | None = None,
        aggregate_budget: _ReadOnlyAdmissionBudget | None = None,
    ) -> None:
        if aggregate_budget is self:
            raise ValueError("SQLite admission budget cannot aggregate itself")
        self.deadline = deadline
        self.max_bytes = max_bytes
        self.copied_bytes = 0
        self._rejection_hook = rejection_hook
        self._aggregate_budget = aggregate_budget

    def require_time(self) -> None:
        _require_admission_deadline(
            self.deadline,
            rejection_hook=self._rejection_hook,
        )
        if self._aggregate_budget is not None:
            self._aggregate_budget.require_time()

    def reserve(self, byte_count: int) -> None:
        self.require_capacity(byte_count)
        if self._aggregate_budget is not None:
            self._aggregate_budget.reserve(byte_count)
        self.copied_bytes += byte_count

    def require_capacity(self, byte_count: int) -> None:
        if byte_count < 0 or byte_count > self.max_bytes - self.copied_bytes:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
                "SQLite admission snapshot exceeds the supported bound",
                rejection_hook=self._rejection_hook,
            )
        if self._aggregate_budget is not None:
            self._aggregate_budget.require_capacity(byte_count)


@dataclass(frozen=True, slots=True)
class SQLiteSnapshotCopyTelemetry:
    """Bounded counters for copy-before-open work owned by one target."""

    role: str
    copy_count: int
    copied_bytes: int


@dataclass(frozen=True, slots=True)
class _SQLiteSnapshotCopyObserver:
    role: str
    observer: Callable[[SQLiteSnapshotCopyTelemetry], None]


_sqlite_snapshot_copy_observer: ContextVar[_SQLiteSnapshotCopyObserver | None] = ContextVar(
    "sqlite_snapshot_copy_observer",
    default=None,
)


@contextmanager
def observe_sqlite_snapshot_copies(
    *,
    role: str,
    observer: Callable[[SQLiteSnapshotCopyTelemetry], None],
) -> Iterator[None]:
    """Attribute physical copy work to one bounded runtime readiness role."""
    if role not in _SQLITE_ADMISSION_ROLES or role == "unspecified":
        raise ValueError("role must be one explicit bounded SQLite role")
    token = _sqlite_snapshot_copy_observer.set(_SQLiteSnapshotCopyObserver(role=role, observer=observer))
    try:
        yield
    finally:
        _sqlite_snapshot_copy_observer.reset(token)


@dataclass(frozen=True, slots=True)
class SQLiteAuthorityReadinessAdmission:
    """Capability proving one initialized SQLite authority generation.

    The database ID is the durable generation fence. Consumers must still
    revalidate it after acquiring their first write lock; this capability only
    lets sibling owners avoid repeating the copy-before-open admission.
    """

    role: str
    database_id: str
    tenant_owner: str
    copy_telemetry: SQLiteSnapshotCopyTelemetry
    _target: SQLiteDatabaseTarget

    def require_target(
        self,
        *,
        path: str | Path,
        role: str,
        tenant_owner: str,
    ) -> SQLiteDatabaseTarget:
        """Return the admitted target only for the exact authority identity."""
        expected_path = sqlite_database_path(path)
        if (
            self.role != role
            or self._target.path != expected_path
            or self.tenant_owner != tenant_owner
            or not self.database_id
        ):
            raise RuntimeOwnershipError("SQLite readiness admission owner mismatch")
        return self._target

    def require_current_generation(
        self,
        *,
        snapshot_max_bytes: int,
        timeout_ms: int = 30_000,
    ) -> int:
        """Verify final capacity and database ID for this prepared authority."""
        self._target._require_stable_snapshot_capacity(
            timeout_ms=timeout_ms,
            snapshot_max_bytes=snapshot_max_bytes,
        )
        connection = self._target.connect(timeout_ms=timeout_ms)
        try:
            connection.execute("BEGIN IMMEDIATE")
            require_sqlite_database_identity(
                connection,
                role=self.role,
                expected_database_id=self.database_id,
            )
            admitted_bytes = require_sqlite_authority_snapshot_capacity(
                self._target.path,
                snapshot_max_bytes=snapshot_max_bytes,
                require_exists=True,
            )
            connection.rollback()
        finally:
            connection.close()
        return admitted_bytes


def _bounded_errno_cause(error_number: int | None) -> OSError | None:
    """Retain only a recognized errno, never the original filesystem detail."""
    if not isinstance(error_number, int):
        return None
    error_name = errno.errorcode.get(error_number)
    if error_name is None:
        return None
    return OSError(error_number, f"SQLite storage operation failed ({error_name})")


def _reject_snapshot_storage_failure(
    error_number: int | None,
    message: str,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> NoReturn:
    _reject(
        SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
        message,
        rejection_hook=rejection_hook,
        cause=_bounded_errno_cause(error_number),
    )


def _snapshot_storage_call[StorageResult](
    operation: Callable[[], StorageResult],
    *,
    message: str,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> StorageResult:
    """Run one disposable-storage operation behind the domain-error boundary."""
    try:
        return operation()
    except OSError as exc:
        error_number = exc.errno
    _reject_snapshot_storage_failure(
        error_number,
        message,
        rejection_hook=rejection_hook,
    )


def _snapshot_artifact_metadata(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> os.stat_result | None:
    try:
        return path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        error_number = exc.errno
    _reject_snapshot_storage_failure(
        error_number,
        "SQLite snapshot artifact could not be inspected",
        rejection_hook=rejection_hook,
    )


def _create_owner_only_snapshot_file(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> int:
    """Create one disposable artifact without granting ambient umask access."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    descriptor = _snapshot_storage_call(
        lambda: os.open(path, flags, _SNAPSHOT_FILE_MODE),
        message="SQLite snapshot file could not be created",
        rejection_hook=rejection_hook,
    )
    try:
        _snapshot_storage_call(
            lambda: os.fchmod(descriptor, _SNAPSHOT_FILE_MODE),
            message="SQLite snapshot file could not be secured",
            rejection_hook=rejection_hook,
        )
        metadata = _snapshot_storage_call(
            lambda: os.fstat(descriptor),
            message="SQLite snapshot file could not be inspected",
            rejection_hook=rejection_hook,
        )
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != _SNAPSHOT_FILE_MODE
        ):
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
                "SQLite snapshot file ownership or mode is invalid",
                rejection_hook=rejection_hook,
            )
        return descriptor
    except BaseException:
        with suppress(OSError):
            os.close(descriptor)
        with suppress(OSError):
            path.unlink(missing_ok=True)
        raise


def _require_owner_only_snapshot_directory(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    """Harden only a directory generated by this module; never alter callers' roots."""
    metadata = _snapshot_storage_call(
        path.lstat,
        message="SQLite snapshot directory could not be inspected",
        rejection_hook=rejection_hook,
    )
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot directory ownership is invalid",
            rejection_hook=rejection_hook,
        )
    _snapshot_storage_call(
        lambda: os.chmod(path, _SNAPSHOT_DIRECTORY_MODE),
        message="SQLite snapshot directory could not be secured",
        rejection_hook=rejection_hook,
    )
    secured_metadata = _snapshot_storage_call(
        path.lstat,
        message="SQLite snapshot directory could not be inspected",
        rejection_hook=rejection_hook,
    )
    if (
        not stat.S_ISDIR(secured_metadata.st_mode)
        or secured_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(secured_metadata.st_mode) != _SNAPSHOT_DIRECTORY_MODE
    ):
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot directory ownership or mode is invalid",
            rejection_hook=rejection_hook,
        )


def _snapshot_directory_identity(metadata: os.stat_result) -> _SnapshotDirectoryIdentity:
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_uid),
        stat.S_IMODE(metadata.st_mode),
    )


def _require_pinned_snapshot_directory(
    path: Path,
    descriptor: int,
    expected_identity: _SnapshotDirectoryIdentity,
    *,
    owner_only: bool,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    path_metadata = _snapshot_storage_call(
        path.lstat,
        message="SQLite snapshot directory could not be inspected",
        rejection_hook=rejection_hook,
    )
    descriptor_metadata = _snapshot_storage_call(
        lambda: os.fstat(descriptor),
        message="SQLite snapshot directory could not be inspected",
        rejection_hook=rejection_hook,
    )
    path_identity = _snapshot_directory_identity(path_metadata)
    descriptor_identity = _snapshot_directory_identity(descriptor_metadata)
    invalid_mode = path_identity[3] != _SNAPSHOT_DIRECTORY_MODE if owner_only else bool(path_identity[3] & 0o022)
    if (
        not stat.S_ISDIR(path_metadata.st_mode)
        or not stat.S_ISDIR(descriptor_metadata.st_mode)
        or path_identity != expected_identity
        or descriptor_identity != expected_identity
        or path_identity[2] != os.geteuid()
        or invalid_mode
    ):
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot directory identity or permissions changed",
            rejection_hook=rejection_hook,
        )


@contextmanager
def _pinned_snapshot_directory(
    path: Path,
    *,
    owner_only: bool,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> Iterator[tuple[int, _SnapshotDirectoryIdentity]]:
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        _reject(
            SQLiteIdentityRejectionReason.UNSUPPORTED_PLATFORM,
            "SQLite secure snapshot publication is unavailable on this platform",
            rejection_hook=rejection_hook,
        )
    metadata = _snapshot_storage_call(
        path.lstat,
        message="SQLite snapshot directory could not be inspected",
        rejection_hook=rejection_hook,
    )
    identity = _snapshot_directory_identity(metadata)
    invalid_mode = identity[3] != _SNAPSHOT_DIRECTORY_MODE if owner_only else bool(identity[3] & 0o022)
    if not stat.S_ISDIR(metadata.st_mode) or identity[2] != os.geteuid() or invalid_mode:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot directory ownership or permissions are invalid",
            rejection_hook=rejection_hook,
        )
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
    descriptor = _snapshot_storage_call(
        lambda: os.open(path, flags),
        message="SQLite snapshot directory could not be opened securely",
        rejection_hook=rejection_hook,
    )
    try:
        _require_pinned_snapshot_directory(
            path,
            descriptor,
            identity,
            owner_only=owner_only,
            rejection_hook=rejection_hook,
        )
        yield descriptor, identity
    finally:
        with suppress(OSError):
            os.close(descriptor)


def _snapshot_artifact_identity(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> _SnapshotArtifactIdentity:
    metadata = _snapshot_artifact_metadata(path, rejection_hook=rejection_hook)
    if metadata is None or (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != _SNAPSHOT_FILE_MODE
    ):
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot artifact identity or permissions are invalid",
            rejection_hook=rejection_hook,
        )
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
        int(metadata.st_uid),
        stat.S_IMODE(metadata.st_mode),
    )


def _require_snapshot_artifact_identity(
    path: Path,
    expected_identity: _SnapshotArtifactIdentity,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    if _snapshot_artifact_identity(path, rejection_hook=rejection_hook) != expected_identity:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot artifact identity changed during publication",
            rejection_hook=rejection_hook,
        )


def _require_owner_only_snapshot_artifacts(
    path: Path,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    """Secure a disposable main file and every sidecar SQLite may have created."""
    for candidate in (path, *(Path(f"{path}{suffix}") for suffix in _SQLITE_SIDECAR_SUFFIXES)):
        metadata = _snapshot_artifact_metadata(candidate, rejection_hook=rejection_hook)
        if metadata is None:
            continue
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
                "SQLite snapshot artifact ownership is invalid",
                rejection_hook=rejection_hook,
            )
        _snapshot_storage_call(
            lambda: os.chmod(candidate, _SNAPSHOT_FILE_MODE),
            message="SQLite snapshot artifact could not be secured",
            rejection_hook=rejection_hook,
        )
        secured_metadata = _snapshot_artifact_metadata(candidate, rejection_hook=rejection_hook)
        if secured_metadata is None or (
            not stat.S_ISREG(secured_metadata.st_mode)
            or secured_metadata.st_uid != os.geteuid()
            or stat.S_IMODE(secured_metadata.st_mode) != _SNAPSHOT_FILE_MODE
        ):
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
                "SQLite snapshot artifact ownership or mode is invalid",
                rejection_hook=rejection_hook,
            )


def _readonly_file_state_from_metadata(metadata: os.stat_result) -> _ReadOnlyFileState:
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
        int(metadata.st_ctime_ns),
    )


def _snapshot_file_state_from_metadata(metadata: os.stat_result) -> _SnapshotFileState:
    return _readonly_file_state_from_metadata(metadata)


@contextmanager
def _open_snapshot_source(
    source: Path,
    *,
    expected_state: _SnapshotFileState,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> Iterator[Any]:
    """Open an authority component without following links.

    Linux can suppress access-time accounting with ``O_NOATIME``. Other POSIX
    platforms do not expose an equivalent standard flag, so access time is not
    part of the source-generation identity; content, inode, size, mtime, and
    ctime remain protected and are revalidated after the copy.
    """
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        _reject(
            SQLiteIdentityRejectionReason.UNSUPPORTED_PLATFORM,
            "SQLite secure snapshot reads are unavailable on this platform",
            rejection_hook=rejection_hook,
        )
    noatime_flag = getattr(os, "O_NOATIME", None)
    base_flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    flags = base_flags | (int(noatime_flag) if noatime_flag is not None else 0)
    descriptor: int | None = None
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        noatime_unavailable = noatime_flag is not None and exc.errno in {
            errno.EACCES,
            errno.EINVAL,
            errno.EPERM,
            getattr(errno, "ENOTSUP", errno.EINVAL),
            getattr(errno, "EOPNOTSUPP", errno.EINVAL),
        }
        if not noatime_unavailable:
            _reject(
                SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
                "SQLite snapshot source could not be opened securely",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        try:
            descriptor = os.open(source, base_flags)
        except OSError as fallback_exc:
            _reject(
                SQLiteIdentityRejectionReason.SECURE_OPEN_FAILED,
                "SQLite snapshot source could not be opened securely",
                rejection_hook=rejection_hook,
                cause=fallback_exc,
            )
    try:
        metadata = os.fstat(descriptor)
        _validate_file_metadata(
            metadata,
            rejection_hook=rejection_hook,
            reject_hard_links=False,
        )
        if _snapshot_file_state_from_metadata(metadata) != expected_state:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "SQLite snapshot source changed before its read",
                rejection_hook=rejection_hook,
            )
        with os.fdopen(descriptor, "rb", closefd=True) as input_file:
            descriptor = None
            yield input_file
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _copy_snapshot_component(
    source: Path,
    destination: Path,
    *,
    budget: _ReadOnlyAdmissionBudget,
    expected_state: _SnapshotFileState | None = None,
) -> None:
    """Copy one regular SQLite component without exceeding the shared budget."""
    budget.require_time()
    observed_state = _snapshot_file_state(source)
    if observed_state is None or (expected_state is not None and observed_state != expected_state):
        _reject(
            SQLiteIdentityRejectionReason.FILE_REPLACED,
            "SQLite snapshot source changed before copying",
            rejection_hook=budget._rejection_hook,
        )
    budget.require_capacity(observed_state[2])
    with _open_snapshot_source(
        source,
        expected_state=observed_state,
        rejection_hook=budget._rejection_hook,
    ) as input_file:
        descriptor = _create_owner_only_snapshot_file(
            destination,
            rejection_hook=budget._rejection_hook,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as output_file:
                descriptor = -1
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
        finally:
            if descriptor >= 0:
                os.close(descriptor)


def _readonly_file_state(path: Path) -> _ReadOnlyFileState | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    return _readonly_file_state_from_metadata(metadata)


def _readonly_source_state(path: Path) -> dict[str, _ReadOnlyFileState]:
    state: dict[str, _ReadOnlyFileState] = {}
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-journal")):
        observed = _readonly_file_state(candidate)
        if observed is not None:
            state[candidate.name] = observed
    return state


def _snapshot_file_state(path: Path) -> _SnapshotFileState | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    return _snapshot_file_state_from_metadata(metadata)


def _snapshot_source_state(path: Path) -> dict[str, _SnapshotFileState]:
    state: dict[str, _SnapshotFileState] = {}
    for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-journal")):
        observed = _snapshot_file_state(candidate)
        if observed is not None:
            state[candidate.name] = observed
    return state


def _bounded_snapshot_source_bytes(
    path: Path,
    source_state: dict[str, _SnapshotFileState],
    *,
    max_bytes: int,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> tuple[int, bool]:
    main_state = source_state.get(path.name)
    if main_state is None:
        _reject(
            SQLiteIdentityRejectionReason.FILE_REPLACED,
            "SQLite snapshot source disappeared during admission",
            rejection_hook=rejection_hook,
        )
    wal_state = source_state.get(Path(f"{path}-wal").name)
    has_live_wal = wal_state is not None and wal_state[2] > 0
    admitted_bytes = main_state[2] + (wal_state[2] if has_live_wal and wal_state is not None else 0)
    if admitted_bytes > max_bytes:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
            "SQLite admission snapshot exceeds the supported bound",
            rejection_hook=rejection_hook,
        )
    return admitted_bytes, has_live_wal


def require_sqlite_authority_snapshot_capacity(
    path: str | Path,
    *,
    snapshot_max_bytes: int,
    require_exists: bool = False,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> int:
    """Verify current raw authority bytes without opening SQLite or copying."""
    target_path = sqlite_database_path(path, rejection_hook=rejection_hook)
    if not target_path.exists():
        if require_exists:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "Prepared SQLite authority is no longer available",
                rejection_hook=rejection_hook,
            )
        return 0
    inspect_sqlite_database_target(target_path, rejection_hook=rejection_hook)
    source_state = _snapshot_source_state(target_path)
    journal_state = source_state.get(Path(f"{target_path}-journal").name)
    if journal_state is not None and journal_state[2] > 0:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
            "SQLite rollback recovery must complete before readiness admission",
            rejection_hook=rejection_hook,
        )
    admitted_bytes, _has_live_wal = _bounded_snapshot_source_bytes(
        target_path,
        source_state,
        max_bytes=validate_sqlite_snapshot_max_bytes(snapshot_max_bytes),
        rejection_hook=rejection_hook,
    )
    if _snapshot_source_state(target_path) != source_state:
        _reject(
            SQLiteIdentityRejectionReason.FILE_REPLACED,
            "SQLite database changed during readiness admission",
            rejection_hook=rejection_hook,
        )
    return admitted_bytes


def _reject_snapshot_copy_error(
    exc: OSError,
    *,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> NoReturn:
    destination_errors = {
        errno.EACCES,
        getattr(errno, "EDQUOT", errno.ENOSPC),
        errno.EFBIG,
        errno.ENOSPC,
        errno.EROFS,
    }
    if exc.errno in destination_errors:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot destination became unavailable",
            rejection_hook=rejection_hook,
            cause=exc,
        )
    _reject(
        SQLiteIdentityRejectionReason.FILE_REPLACED,
        "SQLite database changed during read-only admission",
        rejection_hook=rejection_hook,
        cause=exc,
    )


@contextmanager
def _materialized_readonly_source(
    path: Path,
    *,
    source_state: dict[str, _SnapshotFileState],
    deadline: float,
    snapshot_max_bytes: int,
    rejection_hook: SQLiteIdentityRejectionHook | None,
    budget: _ReadOnlyAdmissionBudget | None = None,
    temporary_parent: Path | None = None,
    authority_role: str = "snapshot",
    telemetry_hook: Callable[[int], None] | None = None,
) -> Iterator[tuple[Path, bool]]:
    """Copy one admitted source generation without opening it through SQLite."""
    admitted_bytes, has_live_wal = _bounded_snapshot_source_bytes(
        path,
        source_state,
        max_bytes=snapshot_max_bytes,
        rejection_hook=rejection_hook,
    )
    copy_budget = budget or _ReadOnlyAdmissionBudget(
        deadline=deadline,
        max_bytes=snapshot_max_bytes,
        rejection_hook=rejection_hook,
    )
    copy_budget.require_capacity(admitted_bytes)
    main_state = source_state[path.name]
    wal_path = Path(f"{path}-wal")
    wal_state = source_state.get(wal_path.name)

    temporary_directory = _snapshot_storage_call(
        lambda: tempfile.TemporaryDirectory(
            prefix="tacit-sqlite-admission-",
            dir=temporary_parent,
        ),
        message="SQLite snapshot directory could not be created",
        rejection_hook=rejection_hook,
    )

    with temporary_directory as directory:
        _require_owner_only_snapshot_directory(
            Path(directory),
            rejection_hook=rejection_hook,
        )
        _require_snapshot_destination_space(
            Path(directory),
            required_bytes=admitted_bytes,
            deadline=deadline,
            rejection_hook=rejection_hook,
        )
        snapshot_path = Path(directory) / "snapshot.db"
        copy_started = time.perf_counter()
        copied_before = copy_budget.copied_bytes
        try:
            _copy_snapshot_component(
                path,
                snapshot_path,
                budget=copy_budget,
                expected_state=main_state,
            )
            if has_live_wal and wal_state is not None:
                _copy_snapshot_component(
                    wal_path,
                    Path(f"{snapshot_path}-wal"),
                    budget=copy_budget,
                    expected_state=wal_state,
                )
        except SQLiteIdentityError:
            raise
        except OSError as exc:
            _reject_snapshot_copy_error(exc, rejection_hook=rejection_hook)
        copied_bytes = copy_budget.copied_bytes - copied_before
        if telemetry_hook is not None:
            telemetry_hook(copied_bytes)
        logger.info(
            "sqlite_readonly_admission_snapshot",
            authority_role=authority_role,
            copy_count=1,
            snapshot_bytes=copied_bytes,
            copy_duration_ms=round((time.perf_counter() - copy_started) * 1_000, 3),
        )
        copy_budget.require_time()
        if _snapshot_source_state(path) != source_state:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "SQLite database changed during read-only admission",
                rejection_hook=rejection_hook,
            )
        _require_owner_only_snapshot_artifacts(
            snapshot_path,
            rejection_hook=rejection_hook,
        )
        yield snapshot_path, has_live_wal


@contextmanager
def _readonly_connection(
    path: Path,
    *,
    timeout_ms: int,
    immutable: bool,
    deadline: float,
    rejection_hook: SQLiteIdentityRejectionHook | None,
    retry_reservation: tuple[_ReadOnlyAdmissionBudget, int] | None = None,
) -> Iterator[sqlite3.Connection]:
    immutable_query = "&immutable=1" if immutable else ""
    uri = f"{path.as_uri()}?mode=ro{immutable_query}"
    connection: sqlite3.Connection | None = None
    try:
        if retry_reservation is not None:
            retry_budget, retry_bytes = retry_reservation
            retry_budget.require_time()
            retry_budget.reserve(retry_bytes)
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
        _require_owner_only_snapshot_artifacts(
            path,
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
            _require_owner_only_snapshot_artifacts(
                path,
                rejection_hook=rejection_hook,
            )


class SQLiteDatabaseTarget:
    """Open one SQLite role path under Tacit's protected-path policy."""

    def __init__(
        self,
        path: str | Path,
        *,
        snapshot_max_bytes: int | None = None,
        admission_role: str = "unspecified",
        rejection_hook: SQLiteIdentityRejectionHook | None = None,
        verification_hook: SQLiteIdentityVerificationHook | None = None,
    ) -> None:
        if admission_role not in _SQLITE_ADMISSION_ROLES:
            raise ValueError("admission_role must be one supported bounded SQLite role")
        selected_snapshot_max_bytes = (
            _READONLY_ADMISSION_SNAPSHOT_MAX_BYTES if snapshot_max_bytes is None else snapshot_max_bytes
        )
        self._snapshot_max_bytes = validate_sqlite_snapshot_max_bytes(selected_snapshot_max_bytes)
        self._admission_role = admission_role
        self._snapshot_copy_count = 0
        self._snapshot_copied_bytes = 0
        self._telemetry_lock = threading.Lock()
        self.path = sqlite_database_path(path, rejection_hook=rejection_hook)
        self._rejection_hook = rejection_hook
        self._verification_hook = verification_hook

    @property
    def snapshot_max_bytes(self) -> int:
        """Return the immutable raw-copy capacity owned by this target."""
        return self._snapshot_max_bytes

    @property
    def snapshot_copy_telemetry(self) -> SQLiteSnapshotCopyTelemetry:
        """Return bounded copy counters without exposing the authority path."""
        with self._telemetry_lock:
            return SQLiteSnapshotCopyTelemetry(
                role=self._admission_role,
                copy_count=self._snapshot_copy_count,
                copied_bytes=self._snapshot_copied_bytes,
            )

    def _record_snapshot_copy(self, copied_bytes: int) -> None:
        with self._telemetry_lock:
            self._snapshot_copy_count += 1
            self._snapshot_copied_bytes += copied_bytes
        scoped_observer = _sqlite_snapshot_copy_observer.get()
        if scoped_observer is not None:
            scoped_observer.observer(
                SQLiteSnapshotCopyTelemetry(
                    role=scoped_observer.role,
                    copy_count=1,
                    copied_bytes=copied_bytes,
                )
            )

    def require_snapshot_capacity(self) -> int:
        """Verify that the current raw authority generation fits its copy cap."""
        return require_sqlite_authority_snapshot_capacity(
            self.path,
            snapshot_max_bytes=self._snapshot_max_bytes,
            require_exists=True,
            rejection_hook=self._rejection_hook,
        )

    def _require_stable_snapshot_capacity(
        self,
        *,
        timeout_ms: int,
        snapshot_max_bytes: int | None = None,
    ) -> int:
        """Bound retries while a legitimate writer advances the same authority."""
        deadline = time.monotonic() + max(timeout_ms, 1) / 1_000
        attempt = 0
        while True:
            try:
                if snapshot_max_bytes is None:
                    return self.require_snapshot_capacity()
                return require_sqlite_authority_snapshot_capacity(
                    self.path,
                    snapshot_max_bytes=snapshot_max_bytes,
                    require_exists=True,
                    rejection_hook=self._rejection_hook,
                )
            except SQLiteIdentityError as exc:
                retryable_reasons = {
                    SQLiteIdentityRejectionReason.FILE_REPLACED,
                    SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                }
                if exc.reason not in retryable_reasons or time.monotonic() >= deadline:
                    raise
                attempt += 1
                delay = min(
                    0.005 * (2 ** min(attempt - 1, 5)),
                    0.1,
                    max(deadline - time.monotonic(), 0),
                )
                logger.info(
                    "sqlite_readiness_capacity_retry",
                    reason_code=exc.reason_code,
                    attempt=attempt,
                )
                if delay > 0:
                    time.sleep(delay)

    def issue_readiness_admission(
        self,
        *,
        role: str,
        database_id: str,
        tenant_owner: str,
        timeout_ms: int = 30_000,
    ) -> SQLiteAuthorityReadinessAdmission:
        """Issue one generation-fenced capability after schema readiness."""
        if role != self._admission_role or role not in _SQLITE_ADMISSION_ROLES:
            raise RuntimeOwnershipError("SQLite readiness admission role mismatch")
        if not database_id or not tenant_owner:
            raise RuntimeOwnershipError("SQLite readiness admission requires database and tenant identity")
        self._require_stable_snapshot_capacity(timeout_ms=timeout_ms)
        return SQLiteAuthorityReadinessAdmission(
            role=role,
            database_id=database_id,
            tenant_owner=tenant_owner,
            copy_telemetry=self.snapshot_copy_telemetry,
            _target=self,
        )

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
        _budget: _ReadOnlyAdmissionBudget | None = None,
    ) -> Iterator[sqlite3.Connection | None]:
        """Yield a nonmutating admission snapshot for a trusted bounded reader."""
        deadline = _deadline or time.monotonic() + max(timeout_ms, 1) / 1_000
        if not self.path.exists():
            yield None
            return
        _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
        inspect_sqlite_database_target(self.path, rejection_hook=self._rejection_hook)
        source_state = _snapshot_source_state(self.path)
        _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
        journal_path = Path(f"{self.path}-journal")
        journal_state = source_state.get(journal_path.name)
        if journal_state is not None and journal_state[2] > 0:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                "SQLite rollback recovery must complete before read-only admission",
                rejection_hook=self._rejection_hook,
            )
        scoped_observer = _sqlite_snapshot_copy_observer.get()
        authority_role = scoped_observer.role if scoped_observer is not None else self._admission_role
        with _materialized_readonly_source(
            self.path,
            source_state=source_state,
            deadline=deadline,
            snapshot_max_bytes=self._snapshot_max_bytes,
            rejection_hook=self._rejection_hook,
            budget=_budget,
            authority_role=authority_role,
            telemetry_hook=self._record_snapshot_copy,
        ) as (snapshot_path, has_live_wal):
            try:
                with _readonly_connection(
                    snapshot_path,
                    timeout_ms=timeout_ms,
                    immutable=not has_live_wal,
                    deadline=deadline,
                    rejection_hook=self._rejection_hook,
                ) as connection:
                    yield connection
            except sqlite3.DatabaseError as exc:
                if _snapshot_source_state(self.path) != source_state:
                    _reject(
                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                        "SQLite database changed during read-only admission",
                        rejection_hook=self._rejection_hook,
                        cause=exc,
                    )
                raise
            _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
            if _snapshot_source_state(self.path) != source_state:
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
        budget = _ReadOnlyAdmissionBudget(
            deadline=deadline,
            max_bytes=self._snapshot_max_bytes,
            rejection_hook=self._rejection_hook,
        )
        attempt = 0
        while True:
            try:
                _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
                if not self.path.exists():
                    return None
                inspect_sqlite_database_target(self.path, rejection_hook=self._rejection_hook)
                source_state = _snapshot_source_state(self.path)
                journal_path = Path(f"{self.path}-journal")
                journal_state = source_state.get(journal_path.name)
                if journal_state is not None and journal_state[2] > 0:
                    _reject(
                        SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                        "SQLite rollback recovery must complete before read-only admission",
                        rejection_hook=self._rejection_hook,
                    )
                admitted_bytes, has_live_wal = _bounded_snapshot_source_bytes(
                    self.path,
                    source_state,
                    max_bytes=self._snapshot_max_bytes,
                    rejection_hook=self._rejection_hook,
                )
                scoped_observer = _sqlite_snapshot_copy_observer.get()
                authority_role = scoped_observer.role if scoped_observer is not None else self._admission_role
                with _materialized_readonly_source(
                    self.path,
                    source_state=source_state,
                    deadline=deadline,
                    snapshot_max_bytes=self._snapshot_max_bytes,
                    rejection_hook=self._rejection_hook,
                    budget=budget,
                    authority_role=authority_role,
                    telemetry_hook=self._record_snapshot_copy,
                ) as (snapshot_path, _materialized_has_live_wal):
                    retry_same_snapshot = False
                    while True:
                        remaining_ms = max(1, int((deadline - time.monotonic()) * 1_000))
                        try:
                            try:
                                with _readonly_connection(
                                    snapshot_path,
                                    timeout_ms=remaining_ms,
                                    immutable=not has_live_wal,
                                    deadline=deadline,
                                    rejection_hook=self._rejection_hook,
                                    retry_reservation=(budget, admitted_bytes) if retry_same_snapshot else None,
                                ) as connection:
                                    result = reader(connection)
                            except sqlite3.DatabaseError as exc:
                                if _snapshot_source_state(self.path) != source_state:
                                    _reject(
                                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                                        "SQLite database changed during read-only admission",
                                        rejection_hook=self._rejection_hook,
                                        cause=exc,
                                    )
                                raise
                            _require_admission_deadline(deadline, rejection_hook=self._rejection_hook)
                            if _snapshot_source_state(self.path) != source_state:
                                _reject(
                                    SQLiteIdentityRejectionReason.FILE_REPLACED,
                                    "SQLite database changed during read-only admission",
                                    rejection_hook=self._rejection_hook,
                                )
                            return result
                        except SQLiteIdentityError as exc:
                            retryable_reasons = {
                                SQLiteIdentityRejectionReason.FILE_REPLACED,
                                SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                            }
                            if exc.reason not in retryable_reasons or time.monotonic() >= deadline:
                                raise
                            if _snapshot_source_state(self.path) != source_state:
                                raise
                            attempt += 1
                            delay = min(
                                0.005 * (2 ** min(attempt - 1, 5)),
                                0.1,
                                max(deadline - time.monotonic(), 0),
                            )
                            logger.info(
                                "sqlite_readonly_admission_retry",
                                reason_code=exc.reason_code,
                                attempt=attempt,
                            )
                            if delay > 0:
                                time.sleep(delay)
                            retry_same_snapshot = True
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
    materialization_budget: _ReadOnlyAdmissionBudget | None = None,
    backup_budget: _ReadOnlyAdmissionBudget | None = None,
) -> None:
    """Materialize one admitted source into a disposable SQLite database."""
    destination_created = False
    completed = False
    source_state: dict[str, _SnapshotFileState] | None = None
    try:
        _require_admission_deadline(deadline, rejection_hook=rejection_hook)
        if inspect_sqlite_database_target(target.path, rejection_hook=rejection_hook) is None:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "SQLite snapshot source disappeared during admission",
                rejection_hook=rejection_hook,
            )
        source_state = _snapshot_source_state(target.path)
        journal_state = source_state.get(Path(f"{target.path}-journal").name)
        if journal_state is not None and journal_state[2] > 0:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_RECOVERY_REQUIRED,
                "SQLite rollback recovery must complete before read-only admission",
                rejection_hook=rejection_hook,
            )

        with _materialized_readonly_source(
            target.path,
            source_state=source_state,
            deadline=deadline,
            snapshot_max_bytes=target.snapshot_max_bytes,
            rejection_hook=rejection_hook,
            budget=materialization_budget,
            temporary_parent=destination.parent,
        ) as (snapshot_path, has_live_wal):
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1_000))
            with _readonly_connection(
                snapshot_path,
                timeout_ms=remaining_ms,
                immutable=not has_live_wal,
                deadline=deadline,
                rejection_hook=rejection_hook,
            ) as source:
                output_budget = backup_budget or _ReadOnlyAdmissionBudget(
                    deadline=deadline,
                    max_bytes=target.snapshot_max_bytes,
                    rejection_hook=rejection_hook,
                )
                page_size_row = source.execute("PRAGMA page_size").fetchone()
                page_count_row = source.execute("PRAGMA page_count").fetchone()
                page_size = int(page_size_row[0]) if page_size_row else -1
                page_count = int(page_count_row[0]) if page_count_row else -1
                if page_size < 0 or page_count < 0:
                    _reject(
                        SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
                        "SQLite snapshot output size is invalid",
                        rejection_hook=rejection_hook,
                    )
                expected_output_bytes = page_size * page_count
                output_budget.reserve(expected_output_bytes)
                output_budget.require_time()
                destination_connection: sqlite3.Connection | None = None
                try:
                    descriptor = _create_owner_only_snapshot_file(
                        destination,
                        rejection_hook=rejection_hook,
                    )
                    os.close(descriptor)
                    destination_created = True
                    destination_connection = sqlite3.connect(destination)
                    destination_mode = destination_connection.execute("PRAGMA journal_mode=OFF").fetchone()
                    if not destination_mode or str(destination_mode[0]).casefold() != "off":
                        _reject(
                            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
                            "SQLite snapshot output could not disable disposable journaling",
                            rejection_hook=rejection_hook,
                        )

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
                _require_owner_only_snapshot_artifacts(
                    destination,
                    rejection_hook=rejection_hook,
                )
                try:
                    actual_output_bytes = destination.stat().st_size
                except OSError as exc:
                    _reject(
                        SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
                        "SQLite snapshot output could not be inspected",
                        rejection_hook=rejection_hook,
                        cause=exc,
                    )
                if actual_output_bytes > expected_output_bytes:
                    _reject(
                        SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
                        "SQLite snapshot output exceeds its admitted bound",
                        rejection_hook=rejection_hook,
                    )
            _require_admission_deadline(deadline, rejection_hook=rejection_hook)
            if _snapshot_source_state(target.path) != source_state:
                _reject(
                    SQLiteIdentityRejectionReason.FILE_REPLACED,
                    "SQLite database changed during snapshot admission",
                    rejection_hook=rejection_hook,
                )
        completed = True
    except sqlite3.DatabaseError as exc:
        if source_state is not None and _snapshot_source_state(target.path) != source_state:
            _reject(
                SQLiteIdentityRejectionReason.FILE_REPLACED,
                "SQLite database changed during snapshot admission",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        if getattr(exc, "sqlite_errorcode", None) in {sqlite3.SQLITE_CANTOPEN, sqlite3.SQLITE_FULL}:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
                "SQLite snapshot destination became unavailable",
                rejection_hook=rejection_hook,
                cause=exc,
            )
        raise
    finally:
        if destination_created and not completed:
            destination.unlink(missing_ok=True)


def _sqlite_source_set_state(
    targets: Sequence[SQLiteDatabaseTarget],
) -> tuple[tuple[Path, tuple[tuple[str, _SnapshotFileState], ...]], ...]:
    return tuple((target.path, tuple(sorted(_snapshot_source_state(target.path).items()))) for target in targets)


def _bounded_snapshot_source_set_bytes(
    source_state: tuple[tuple[Path, tuple[tuple[str, _SnapshotFileState], ...]], ...],
    *,
    snapshot_max_bytes: int,
    snapshot_total_max_bytes: int,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> int:
    aggregate_bytes = 0
    for path, state_items in source_state:
        source_bytes, _has_live_wal = _bounded_snapshot_source_bytes(
            path,
            dict(state_items),
            max_bytes=snapshot_max_bytes,
            rejection_hook=rejection_hook,
        )
        if source_bytes > snapshot_total_max_bytes - aggregate_bytes:
            _reject(
                SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT,
                "SQLite snapshot source set exceeds the supported bound",
                rejection_hook=rejection_hook,
            )
        aggregate_bytes += source_bytes
    return aggregate_bytes


def _require_snapshot_destination_space(
    destination: Path,
    *,
    required_bytes: int,
    deadline: float,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> None:
    _require_admission_deadline(deadline, rejection_hook=rejection_hook)
    try:
        filesystem = os.statvfs(destination)
        available_bytes = int(filesystem.f_bavail) * int(filesystem.f_frsize)
    except (AttributeError, OSError, TypeError, ValueError) as exc:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot destination capacity could not be verified",
            rejection_hook=rejection_hook,
            cause=exc,
        )
    if available_bytes < required_bytes:
        _reject(
            SQLiteIdentityRejectionReason.ADMISSION_STORAGE_UNAVAILABLE,
            "SQLite snapshot destination has insufficient capacity",
            rejection_hook=rejection_hook,
        )
    _require_admission_deadline(deadline, rejection_hook=rejection_hook)


def _create_snapshot_completion_manifest(
    directory: Path,
    *,
    destination_names: Sequence[str],
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> Path:
    """Prepare the owner-only commit record for one complete snapshot set."""
    manifest_path = directory / _SNAPSHOT_COMPLETION_MANIFEST
    descriptor = _create_owner_only_snapshot_file(
        manifest_path,
        rejection_hook=rejection_hook,
    )
    payload = json.dumps(
        {
            "files": sorted(destination_names),
            "format_version": _SNAPSHOT_COMPLETION_FORMAT_VERSION,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    try:
        with os.fdopen(descriptor, "wb", closefd=True) as output:
            descriptor = -1
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
    except OSError as exc:
        with suppress(OSError):
            manifest_path.unlink(missing_ok=True)
        _reject_snapshot_storage_failure(
            exc.errno,
            "SQLite snapshot completion manifest could not be written",
            rejection_hook=rejection_hook,
        )
    finally:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
    _require_owner_only_snapshot_artifacts(
        manifest_path,
        rejection_hook=rejection_hook,
    )
    return manifest_path


def snapshot_sqlite_database_set(
    sources: Sequence[str | Path],
    destination_dir: str | Path,
    *,
    timeout_ms: int = 30_000,
    snapshot_max_bytes: int | None = None,
    snapshot_total_max_bytes: int | None = None,
    rejection_hook: SQLiteIdentityRejectionHook | None = None,
) -> dict[Path, Path]:
    """Snapshot one stable generation of a protected SQLite source set.

    Source databases are byte-copied before any SQLite open.
    If any main/WAL generation moves while the set is copied, every disposable
    copy is discarded and the complete set is retried under one deadline.
    ``snapshot_max_bytes`` bounds each source database; when supplied,
    ``snapshot_total_max_bytes`` independently bounds the complete source set.
    Omitting the latter preserves the historical single-cap behavior.
    Returned paths are valid only after the owner-only completion manifest has
    atomically committed the complete role set.
    """
    selected_snapshot_max_bytes = validate_sqlite_snapshot_max_bytes(
        _READONLY_ADMISSION_SNAPSHOT_MAX_BYTES if snapshot_max_bytes is None else snapshot_max_bytes
    )
    if snapshot_total_max_bytes is not None and snapshot_max_bytes is None:
        raise ValueError("snapshot_total_max_bytes requires snapshot_max_bytes")
    selected_snapshot_total_max_bytes = validate_sqlite_snapshot_max_bytes(
        selected_snapshot_max_bytes if snapshot_total_max_bytes is None else snapshot_total_max_bytes,
        field_name="snapshot_total_max_bytes",
    )
    if selected_snapshot_total_max_bytes < selected_snapshot_max_bytes:
        raise ValueError("snapshot_total_max_bytes must not be smaller than snapshot_max_bytes")
    if not sources:
        raise ValueError("SQLite snapshot source set must not be empty")
    destination_root = Path(destination_dir)
    if destination_root.is_symlink() or not destination_root.is_dir():
        raise ValueError("SQLite snapshot destination must be a real directory")

    with _pinned_snapshot_directory(
        destination_root,
        owner_only=False,
        rejection_hook=rejection_hook,
    ) as (destination_descriptor, destination_identity):
        return _snapshot_sqlite_database_set_in_pinned_destination(
            sources,
            destination_root,
            destination_descriptor=destination_descriptor,
            destination_identity=destination_identity,
            timeout_ms=timeout_ms,
            snapshot_max_bytes=selected_snapshot_max_bytes,
            snapshot_total_max_bytes=selected_snapshot_total_max_bytes,
            rejection_hook=rejection_hook,
        )


def _snapshot_sqlite_database_set_in_pinned_destination(
    sources: Sequence[str | Path],
    destination_root: Path,
    *,
    destination_descriptor: int,
    destination_identity: _SnapshotDirectoryIdentity,
    timeout_ms: int,
    snapshot_max_bytes: int,
    snapshot_total_max_bytes: int,
    rejection_hook: SQLiteIdentityRejectionHook | None,
) -> dict[Path, Path]:
    _require_pinned_snapshot_directory(
        destination_root,
        destination_descriptor,
        destination_identity,
        owner_only=False,
        rejection_hook=rejection_hook,
    )

    targets = tuple(
        SQLiteDatabaseTarget(
            source,
            snapshot_max_bytes=snapshot_max_bytes,
            rejection_hook=rejection_hook,
        )
        for source in sources
    )
    source_paths = tuple(target.path for target in targets)
    if len(set(source_paths)) != len(source_paths):
        raise ValueError("SQLite snapshot source paths must be unique")
    destination_names = tuple(path.name for path in source_paths)
    if len(set(destination_names)) != len(destination_names):
        raise ValueError("SQLite snapshot source filenames must be unique")
    destinations = {source_path: destination_root / source_path.name for source_path in source_paths}
    completion_manifest = destination_root / _SNAPSHOT_COMPLETION_MANIFEST
    if completion_manifest.exists() or completion_manifest.is_symlink():
        raise ValueError("SQLite snapshot destination already contains a completed generation")
    if any(destination.exists() or destination.is_symlink() for destination in destinations.values()):
        raise ValueError("SQLite snapshot destinations must not already exist")

    deadline = time.monotonic() + max(timeout_ms, 1) / 1_000
    materialization_budget = _ReadOnlyAdmissionBudget(
        deadline=deadline,
        max_bytes=snapshot_total_max_bytes,
        rejection_hook=rejection_hook,
    )
    backup_budget = _ReadOnlyAdmissionBudget(
        deadline=deadline,
        max_bytes=snapshot_total_max_bytes,
        rejection_hook=rejection_hook,
    )
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
            aggregate_source_bytes = _bounded_snapshot_source_set_bytes(
                source_state,
                snapshot_max_bytes=snapshot_max_bytes,
                snapshot_total_max_bytes=snapshot_total_max_bytes,
                rejection_hook=rejection_hook,
            )
            _require_snapshot_destination_space(
                destination_root,
                required_bytes=aggregate_source_bytes * 2,
                deadline=deadline,
                rejection_hook=rejection_hook,
            )
            _require_pinned_snapshot_directory(
                destination_root,
                destination_descriptor,
                destination_identity,
                owner_only=False,
                rejection_hook=rejection_hook,
            )
            temporary_directory = _snapshot_storage_call(
                lambda: tempfile.TemporaryDirectory(
                    prefix=".tacit-sqlite-snapshot-",
                    dir=destination_root,
                ),
                message="SQLite snapshot directory could not be created",
                rejection_hook=rejection_hook,
            )
            with temporary_directory as attempt_directory:
                attempt_root = Path(attempt_directory)
                _require_owner_only_snapshot_directory(
                    attempt_root,
                    rejection_hook=rejection_hook,
                )
                with _pinned_snapshot_directory(
                    attempt_root,
                    owner_only=True,
                    rejection_hook=rejection_hook,
                ) as (attempt_descriptor, attempt_identity):
                    for target in targets:
                        _snapshot_sqlite_database(
                            target,
                            attempt_root / target.path.name,
                            deadline=deadline,
                            rejection_hook=rejection_hook,
                            materialization_budget=_ReadOnlyAdmissionBudget(
                                deadline=deadline,
                                max_bytes=snapshot_max_bytes,
                                rejection_hook=rejection_hook,
                                aggregate_budget=materialization_budget,
                            ),
                            backup_budget=_ReadOnlyAdmissionBudget(
                                deadline=deadline,
                                max_bytes=snapshot_max_bytes,
                                rejection_hook=rejection_hook,
                                aggregate_budget=backup_budget,
                            ),
                        )
                    _require_admission_deadline(deadline, rejection_hook=rejection_hook)
                    if _sqlite_source_set_state(targets) != source_state:
                        _reject(
                            SQLiteIdentityRejectionReason.FILE_REPLACED,
                            "SQLite source set changed while snapshots were copied",
                            rejection_hook=rejection_hook,
                        )

                    prepared_manifest = _create_snapshot_completion_manifest(
                        attempt_root,
                        destination_names=destination_names,
                        rejection_hook=rejection_hook,
                    )
                    _require_admission_deadline(deadline, rejection_hook=rejection_hook)
                    published_identities: dict[Path, _SnapshotArtifactIdentity] = {}
                    try:
                        for target in targets:
                            source_artifact = attempt_root / target.path.name
                            destination = destinations[target.path]
                            _require_owner_only_snapshot_artifacts(
                                source_artifact,
                                rejection_hook=rejection_hook,
                            )
                            expected_identity = _snapshot_artifact_identity(
                                source_artifact,
                                rejection_hook=rejection_hook,
                            )
                            _require_pinned_snapshot_directory(
                                attempt_root,
                                attempt_descriptor,
                                attempt_identity,
                                owner_only=True,
                                rejection_hook=rejection_hook,
                            )
                            _require_pinned_snapshot_directory(
                                destination_root,
                                destination_descriptor,
                                destination_identity,
                                owner_only=False,
                                rejection_hook=rejection_hook,
                            )
                            os.replace(source_artifact, destination)
                            _require_pinned_snapshot_directory(
                                destination_root,
                                destination_descriptor,
                                destination_identity,
                                owner_only=False,
                                rejection_hook=rejection_hook,
                            )
                            _require_snapshot_artifact_identity(
                                destination,
                                expected_identity,
                                rejection_hook=rejection_hook,
                            )
                            published_identities[destination] = expected_identity

                        manifest_identity = _snapshot_artifact_identity(
                            prepared_manifest,
                            rejection_hook=rejection_hook,
                        )
                        _require_admission_deadline(deadline, rejection_hook=rejection_hook)
                        _require_pinned_snapshot_directory(
                            attempt_root,
                            attempt_descriptor,
                            attempt_identity,
                            owner_only=True,
                            rejection_hook=rejection_hook,
                        )
                        _require_pinned_snapshot_directory(
                            destination_root,
                            destination_descriptor,
                            destination_identity,
                            owner_only=False,
                            rejection_hook=rejection_hook,
                        )
                        os.replace(prepared_manifest, completion_manifest)
                        _require_pinned_snapshot_directory(
                            destination_root,
                            destination_descriptor,
                            destination_identity,
                            owner_only=False,
                            rejection_hook=rejection_hook,
                        )
                        _require_snapshot_artifact_identity(
                            completion_manifest,
                            manifest_identity,
                            rejection_hook=rejection_hook,
                        )
                        for destination, expected_identity in published_identities.items():
                            _require_snapshot_artifact_identity(
                                destination,
                                expected_identity,
                                rejection_hook=rejection_hook,
                            )
                    except BaseException:
                        for name in (*destination_names, _SNAPSHOT_COMPLETION_MANIFEST):
                            with suppress(OSError):
                                os.unlink(name, dir_fd=destination_descriptor)
                        raise
            _require_pinned_snapshot_directory(
                destination_root,
                destination_descriptor,
                destination_identity,
                owner_only=False,
                rejection_hook=rejection_hook,
            )
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
