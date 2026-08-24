"""Feedback & dashboard provenance store.

Lightweight SQLite-backed persistence for:
- Dashboard provenance (prompt, archetypes, metrics used per generation)
- Human feedback ratings (dimensional SRE evaluation per dashboard)

In production, swap for Postgres via SQLAlchemy or similar.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import structlog

from tacit.config import DEFAULT_FEEDBACK_DB_PATH, Settings, settings
from tacit.runtime_ownership import (
    RuntimeOwnershipDescriptor,
    copy_runtime_settings,
    runtime_descriptor_for_store,
    snapshot_runtime_settings,
)
from tacit.sqlite_identity import (
    SQLiteDatabaseTarget,
    activate_sqlite_wal,
    claim_sqlite_database_identity,
    require_sqlite_database_identity,
    sqlite_database_path,
)
from tacit.tenancy import resolve_tenant_boundary

_UID_PATTERN = re.compile(r"^[a-zA-Z0-9_\-]{1,128}$")


def _sanitize_uid(uid: str) -> str:
    """Validate a dashboard UID — alphanumeric, hyphens, underscores only (max 128 chars)."""
    if not _UID_PATTERN.match(uid):
        raise ValueError(f"Invalid dashboard_uid: must be 1-128 alphanumeric/hyphen/underscore chars, got {uid!r:.40}")
    return uid


logger = structlog.get_logger()

_DEFAULT_DB_PATH = DEFAULT_FEEDBACK_DB_PATH
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_DEFAULT_OWNER_MARKER = "default_owner_v1"
_DEFAULT_OWNER_PROGRESS_MARKER = "default_owner_in_progress_v1"
_DEFAULT_OWNER_CURSOR_PREFIX = "default_owner_cursor_v1:"
_OWNER_MIGRATION_BATCH_SIZE = 500
_LEGACY_SCOPE_OWNER_MARKER = "legacy_tenant_scope_owner_v2"
_LEGACY_SCOPE_CURSOR_PREFIX = "legacy_tenant_scope_cursor_v2:"
_LEGACY_SCOPE_COMPLETE_PREFIX = "legacy_tenant_scope_complete_v2:"
_LEGACY_PROVENANCE_SHADOW = "dashboard_provenance_tenant_migration_v2"
_LEGACY_FEEDBACK_SHADOW = "feedback_tenant_migration_v2"
_KEYSET_NOT_STARTED = "not_started"
_KEYSET_CURSOR_PREFIX = "id:"
_DIAGNOSTIC_FINGERPRINT_LENGTH = 16


class _OwnerPreflightStatus(StrEnum):
    ALLOWED = "allowed"
    UNKNOWN = "unknown"
    REJECTED = "rejected"


@dataclass(frozen=True)
class _OwnerPreflightResult:
    status: _OwnerPreflightStatus
    reason_code: str = ""
    recorded_owner: str | None = None
    ownerless_table_count: int = 0


def _db_path(runtime_settings: Settings | None = None) -> Path:
    """Resolve DB path from settings or default."""
    active_settings = runtime_settings or settings
    custom = active_settings.feedback_db_path
    return sqlite_database_path(custom or _DEFAULT_DB_PATH)


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dashboard_provenance (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT NOT NULL DEFAULT 'default',
    dashboard_uid   TEXT NOT NULL,
    prompt          TEXT NOT NULL,
    problem_type    TEXT NOT NULL DEFAULT '',
    archetypes      TEXT NOT NULL DEFAULT '[]',   -- JSON array of {type, confidence}
    metrics_used    TEXT NOT NULL DEFAULT '[]',    -- JSON array of metric names
    panel_count     INTEGER NOT NULL DEFAULT 0,
    path_used       TEXT NOT NULL DEFAULT '',      -- 'archetype' or 'freeform'
    dashboard_url   TEXT NOT NULL DEFAULT '',
    user_id         TEXT NOT NULL DEFAULT '',
    channel_id      TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL,
    UNIQUE (tenant_id, dashboard_uid)
);

CREATE TABLE IF NOT EXISTS feedback (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT NOT NULL DEFAULT 'default',
    dashboard_uid   TEXT NOT NULL,
    reviewer        TEXT NOT NULL DEFAULT '',      -- user id or email
    symptom_visibility  INTEGER CHECK(symptom_visibility BETWEEN 1 AND 5),
    root_cause_support  INTEGER CHECK(root_cause_support BETWEEN 1 AND 5),
    noise_level         INTEGER CHECK(noise_level BETWEEN 1 AND 5),
    investigation_speed INTEGER CHECK(investigation_speed BETWEEN 1 AND 5),
    overall_useful  INTEGER CHECK(overall_useful IN (0, 1)),
    comment         TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL,
    FOREIGN KEY (tenant_id, dashboard_uid)
        REFERENCES dashboard_provenance(tenant_id, dashboard_uid)
);

CREATE INDEX IF NOT EXISTS idx_provenance_tenant_uid
    ON dashboard_provenance(tenant_id, dashboard_uid);
CREATE INDEX IF NOT EXISTS idx_feedback_tenant_uid
    ON feedback(tenant_id, dashboard_uid);
CREATE INDEX IF NOT EXISTS idx_provenance_tenant_id
    ON dashboard_provenance(tenant_id, id);
CREATE INDEX IF NOT EXISTS idx_feedback_tenant_id
    ON feedback(tenant_id, id);

CREATE TABLE IF NOT EXISTS feedback_tenant_migration_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""


def _execute_schema_statements(conn: sqlite3.Connection, script: str) -> None:
    """Execute schema SQL without sqlite3.executescript's implicit commit."""
    pending: list[str] = []
    for line in script.splitlines():
        pending.append(line)
        statement = "\n".join(pending).strip()
        if statement and sqlite3.complete_statement(statement):
            conn.execute(statement)
            pending.clear()
    if "\n".join(pending).strip():
        raise RuntimeError("Feedback schema contains an incomplete SQL statement")


def _validate_schema_sql(script: str) -> None:
    """Reject an invalid target schema before touching a legacy database."""
    conn = sqlite3.connect(":memory:")
    try:
        _execute_schema_statements(conn, script)
    finally:
        conn.close()


def _diagnostic_fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:_DIAGNOSTIC_FINGERPRINT_LENGTH]


def _owner_class(value: str) -> str:
    if value == "*":
        return "wildcard"
    if value == "default":
        return "default"
    return "pinned"


def _decode_keyset_cursor(value: object | None) -> int | None:
    if value is None:
        return None
    encoded = str(value)
    if encoded in {_KEYSET_NOT_STARTED, "0"}:
        # Plain zero was the legacy not-started sentinel. New cursors are
        # prefixed so a processed row whose legal ID is zero is unambiguous.
        return None
    if encoded.startswith(_KEYSET_CURSOR_PREFIX):
        encoded = encoded[len(_KEYSET_CURSOR_PREFIX) :]
    return int(encoded)


def _encode_keyset_cursor(value: int) -> str:
    return f"{_KEYSET_CURSOR_PREFIX}{value}"


class FeedbackStore:
    """SQLite-backed store for dashboard provenance and human feedback."""

    def __init__(self, db_path: Path | None = None, *, runtime_settings: Settings | None = None):
        settings_owner = runtime_settings or settings
        selected_path = db_path or settings_owner.feedback_db_path or _DEFAULT_DB_PATH
        self._settings = snapshot_runtime_settings(
            settings_owner,
            database_role="feedback" if db_path is not None else None,
            database_path=db_path,
        )
        self._db_path = sqlite_database_path(selected_path)
        self._runtime_ownership = runtime_descriptor_for_store(
            component="feedback_store",
            runtime_settings=self._settings,
            database_role="feedback",
            database_path=self._db_path,
        )
        self._database_id: str | None = None
        self._sqlite_target = SQLiteDatabaseTarget(self._db_path)
        self._preflight_owner_before_mutation()
        self._ensure_schema()

    @property
    def runtime_settings(self) -> Settings:
        """Return the settings that own this store."""
        return copy_runtime_settings(self._settings)

    @property
    def database_path(self) -> Path:
        """Return the canonical feedback database identity."""
        return self._db_path

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Return this store's public runtime ownership descriptor."""
        return self._runtime_ownership

    @contextmanager
    def _conn(self):
        conn = self._sqlite_target.connect(timeout_ms=_SQLITE_BUSY_TIMEOUT_MS)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
        observed_database_id = require_sqlite_database_identity(
            conn,
            role="feedback",
            expected_database_id=self._database_id,
        )
        if observed_database_id is not None:
            self._database_id = observed_database_id
        activate_sqlite_wal(conn, timeout_ms=_SQLITE_BUSY_TIMEOUT_MS)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self):
        migration_required = False
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._require_owner_preflight(conn)
            self._require_confirmed_default_tenant_owner(conn)
            migration_required = self._tenant_migration_required(conn)
            if migration_required:
                self._require_legacy_tenant_owner(conn)
                _validate_schema_sql(_SCHEMA_SQL)
                self._migrate_tenant_scope(conn)
            else:
                self._database_id = claim_sqlite_database_identity(
                    conn,
                    role="feedback",
                    expected_database_id=self._database_id,
                )
                _execute_schema_statements(conn, _SCHEMA_SQL)
        if migration_required:
            self._migrate_tenant_scope_batched()
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                self._database_id = claim_sqlite_database_identity(
                    conn,
                    role="feedback",
                    expected_database_id=self._database_id,
                )
                _execute_schema_statements(conn, _SCHEMA_SQL)
        self._reconcile_default_tenant_owner_batched()
        logger.info(
            "feedback_store_init",
            database_path_fingerprint=_diagnostic_fingerprint(str(self._db_path)),
        )

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            is not None
        )

    @classmethod
    def _tenant_migration_required(cls, conn: sqlite3.Connection) -> bool:
        for table_name in ("dashboard_provenance", "feedback"):
            if not cls._table_exists(conn, table_name):
                continue
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}
            if "tenant_id" not in columns:
                return True
        return False

    @staticmethod
    def _metadata_value(conn: sqlite3.Connection, key: str) -> str | None:
        if not FeedbackStore._table_exists(conn, "feedback_tenant_migration_metadata"):
            return None
        row = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (key,),
        ).fetchone()
        return str(row[0]) if row is not None else None

    def _owner_preflight_result(self, conn: sqlite3.Connection) -> _OwnerPreflightResult:
        configured_owner = str(self._settings.knowledge_tenant_id or "default")
        terminal_owner = self._metadata_value(conn, _DEFAULT_OWNER_MARKER)
        if terminal_owner is not None and configured_owner != "*" and terminal_owner != configured_owner:
            return _OwnerPreflightResult(
                _OwnerPreflightStatus.REJECTED,
                reason_code="pinned_owner_mismatch",
                recorded_owner=terminal_owner,
            )

        progress_markers = (
            (_DEFAULT_OWNER_PROGRESS_MARKER, configured_owner),
            (
                _LEGACY_SCOPE_OWNER_MARKER,
                configured_owner if configured_owner != "*" else "default",
            ),
        )
        for marker, expected_owner in progress_markers:
            recorded_owner = self._metadata_value(conn, marker)
            if recorded_owner is not None and recorded_owner != expected_owner:
                return _OwnerPreflightResult(
                    _OwnerPreflightStatus.REJECTED,
                    reason_code="migration_owner_mismatch",
                    recorded_owner=recorded_owner,
                )

        ownerless_table_count = 0
        ambiguous_default_table_count = 0
        for table_name in ("dashboard_provenance", "feedback"):
            if not self._table_exists(conn, table_name):
                continue
            columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})")}
            if "tenant_id" not in columns:
                if conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone() is not None:
                    ownerless_table_count += 1
                continue
            if (
                terminal_owner is None
                and conn.execute(f"SELECT 1 FROM {table_name} WHERE tenant_id='default' LIMIT 1").fetchone() is not None
            ):
                ambiguous_default_table_count += 1

        if configured_owner == "*" and ownerless_table_count:
            return _OwnerPreflightResult(
                _OwnerPreflightStatus.REJECTED,
                reason_code="ownerless_wildcard",
                ownerless_table_count=ownerless_table_count,
            )
        if configured_owner == "*" and ambiguous_default_table_count:
            return _OwnerPreflightResult(
                _OwnerPreflightStatus.REJECTED,
                reason_code="unconfirmed_default_owner",
                ownerless_table_count=ambiguous_default_table_count,
            )
        if terminal_owner is not None or any(
            self._metadata_value(conn, marker) is not None for marker, _expected in progress_markers
        ):
            return _OwnerPreflightResult(_OwnerPreflightStatus.ALLOWED)
        return _OwnerPreflightResult(_OwnerPreflightStatus.UNKNOWN)

    def _reject_owner_preflight(self, result: _OwnerPreflightResult) -> None:
        configured_owner = str(self._settings.knowledge_tenant_id or "default")
        fields: dict[str, object] = {
            "reason_code": result.reason_code,
            "configured_owner_class": _owner_class(configured_owner),
        }
        if result.recorded_owner is not None:
            fields.update(
                {
                    "recorded_owner_class": _owner_class(result.recorded_owner),
                    "recorded_owner_fingerprint": _diagnostic_fingerprint(result.recorded_owner),
                    "configured_owner_fingerprint": _diagnostic_fingerprint(configured_owner),
                }
            )
        if result.ownerless_table_count:
            fields["ownerless_table_count"] = result.ownerless_table_count
        logger.error("feedback_owner_preflight_rejected", **fields)
        raise RuntimeError(f"Feedback store owner preflight rejected (reason={result.reason_code})")

    def _require_owner_preflight(self, conn: sqlite3.Connection) -> _OwnerPreflightStatus:
        result = self._owner_preflight_result(conn)
        if result.status is _OwnerPreflightStatus.REJECTED:
            self._reject_owner_preflight(result)
        return result.status

    def _preflight_owner_before_mutation(self) -> _OwnerPreflightStatus:
        def inspect_owner(conn: sqlite3.Connection) -> _OwnerPreflightStatus:
            conn.row_factory = sqlite3.Row
            return self._require_owner_preflight(conn)

        try:
            result = self._sqlite_target.read_existing_readonly(
                inspect_owner,
                timeout_ms=_SQLITE_BUSY_TIMEOUT_MS,
            )
            return result if result is not None else _OwnerPreflightStatus.UNKNOWN
        except RuntimeError:
            raise
        except sqlite3.Error as exc:
            logger.error(
                "feedback_owner_preflight_rejected",
                reason_code="owner_preflight_unavailable",
                configured_owner_class=_owner_class(str(self._settings.knowledge_tenant_id or "default")),
                database_path_fingerprint=_diagnostic_fingerprint(str(self._db_path)),
            )
            raise RuntimeError("Feedback store owner preflight rejected (reason=owner_preflight_unavailable)") from exc

    def _require_legacy_tenant_owner(self, conn: sqlite3.Connection) -> None:
        """Refuse to guess ownership for populated pre-tenant feedback stores."""
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        if configured_tenant != "*":
            return
        ownerless_tables: list[str] = []
        for table_name in ("dashboard_provenance", "feedback"):
            if not self._table_exists(conn, table_name):
                continue
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}
            if "tenant_id" in columns:
                continue
            if conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone() is not None:
                ownerless_tables.append(table_name)
        if not ownerless_tables:
            return
        self._reject_owner_preflight(
            _OwnerPreflightResult(
                _OwnerPreflightStatus.REJECTED,
                reason_code="ownerless_wildcard",
                ownerless_table_count=len(ownerless_tables),
            )
        )

    def _require_confirmed_default_tenant_owner(self, conn: sqlite3.Connection) -> None:
        """Refuse ambiguous default rows written by the previous migration."""
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        if configured_tenant != "*":
            return
        if self._table_exists(conn, "feedback_tenant_migration_metadata"):
            marker = conn.execute(
                "SELECT 1 FROM feedback_tenant_migration_metadata WHERE key=?",
                (_DEFAULT_OWNER_MARKER,),
            ).fetchone()
            if marker is not None:
                return
        ambiguous_tables = []
        for table_name in ("dashboard_provenance", "feedback"):
            if not self._table_exists(conn, table_name):
                continue
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})")}
            if "tenant_id" not in columns:
                continue
            if conn.execute(f"SELECT 1 FROM {table_name} WHERE tenant_id='default' LIMIT 1").fetchone() is not None:
                ambiguous_tables.append(table_name)
        if ambiguous_tables:
            self._reject_owner_preflight(
                _OwnerPreflightResult(
                    _OwnerPreflightStatus.REJECTED,
                    reason_code="unconfirmed_default_owner",
                    ownerless_table_count=len(ambiguous_tables),
                )
            )

    def _reconcile_default_tenant_owner_batch(
        self,
        conn: sqlite3.Connection,
        *,
        batch_size: int,
    ) -> tuple[bool, str, int]:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        marker = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        ).fetchone()
        if marker is not None:
            configured_tenant = str(self._settings.knowledge_tenant_id or "default")
            recorded_owner = str(marker["value"])
            if configured_tenant != "*" and recorded_owner != configured_tenant:
                self._reject_owner_preflight(
                    _OwnerPreflightResult(
                        _OwnerPreflightStatus.REJECTED,
                        reason_code="pinned_owner_mismatch",
                        recorded_owner=recorded_owner,
                    )
                )
            return True, "already_complete", 0

        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        progress = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_PROGRESS_MARKER,),
        ).fetchone()
        if progress is not None and str(progress["value"]) != configured_tenant:
            self._reject_owner_preflight(
                _OwnerPreflightResult(
                    _OwnerPreflightStatus.REJECTED,
                    reason_code="migration_owner_mismatch",
                    recorded_owner=str(progress["value"]),
                )
            )
        if configured_tenant in {"*", "default"}:
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)""",
                (_DEFAULT_OWNER_MARKER, configured_tenant, time.time()),
            )
            conn.execute(
                "DELETE FROM feedback_tenant_migration_metadata WHERE key=?",
                (_DEFAULT_OWNER_PROGRESS_MARKER,),
            )
            conn.execute(
                "DELETE FROM feedback_tenant_migration_metadata WHERE key LIKE ?",
                (f"{_DEFAULT_OWNER_CURSOR_PREFIX}%",),
            )
            return True, "marker", 0

        if progress is None:
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)""",
                (_DEFAULT_OWNER_PROGRESS_MARKER, configured_tenant, time.time()),
            )
        for table_name in ("dashboard_provenance", "feedback"):
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO NOTHING""",
                (
                    f"{_DEFAULT_OWNER_CURSOR_PREFIX}{table_name}",
                    _KEYSET_NOT_STARTED,
                    time.time(),
                ),
            )

        for table_name in ("dashboard_provenance", "feedback"):
            cursor_key = f"{_DEFAULT_OWNER_CURSOR_PREFIX}{table_name}"
            cursor_row = conn.execute(
                "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
                (cursor_key,),
            ).fetchone()
            if cursor_row is not None and str(cursor_row["value"]) == "complete":
                continue
            after_id = _decode_keyset_cursor(cursor_row["value"] if cursor_row is not None else None)
            if after_id is None:
                ids = conn.execute(
                    f"""SELECT id FROM {table_name}
                        WHERE tenant_id='default'
                        ORDER BY id LIMIT ?""",
                    (batch_size,),
                ).fetchall()
            else:
                ids = conn.execute(
                    f"""SELECT id FROM {table_name}
                        WHERE id>? AND tenant_id='default'
                        ORDER BY id LIMIT ?""",
                    (after_id, batch_size),
                ).fetchall()
            if not ids:
                conn.execute(
                    """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                       VALUES (?, 'complete', ?)
                       ON CONFLICT(key) DO UPDATE SET value='complete', updated_at=excluded.updated_at""",
                    (cursor_key, time.time()),
                )
                continue
            last_id = int(ids[-1]["id"])
            if after_id is None:
                cursor = conn.execute(
                    f"""UPDATE {table_name} SET tenant_id=?
                        WHERE id<=? AND tenant_id='default'""",
                    (configured_tenant, last_id),
                )
            else:
                cursor = conn.execute(
                    f"""UPDATE {table_name} SET tenant_id=?
                        WHERE id>? AND id<=? AND tenant_id='default'""",
                    (configured_tenant, after_id, last_id),
                )
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (cursor_key, _encode_keyset_cursor(last_id), time.time()),
            )
            if cursor.rowcount:
                return False, f"{table_name}:retarget", int(cursor.rowcount)

        for table_name in ("dashboard_provenance", "feedback"):
            remaining = conn.execute(
                f"SELECT MIN(id) AS first_id FROM {table_name} WHERE tenant_id='default'"
            ).fetchone()
            if remaining is not None and remaining["first_id"] is not None:
                conn.execute(
                    """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                    (
                        f"{_DEFAULT_OWNER_CURSOR_PREFIX}{table_name}",
                        _KEYSET_NOT_STARTED,
                        time.time(),
                    ),
                )
                return False, f"{table_name}:rescan", 0

        conn.execute(
            """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
               VALUES (?, ?, ?)""",
            (_DEFAULT_OWNER_MARKER, configured_tenant, time.time()),
        )
        conn.execute(
            "DELETE FROM feedback_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_PROGRESS_MARKER,),
        )
        conn.execute(
            "DELETE FROM feedback_tenant_migration_metadata WHERE key LIKE ?",
            (f"{_DEFAULT_OWNER_CURSOR_PREFIX}%",),
        )
        return True, "marker", 0

    def _reconcile_default_tenant_owner_batched(self) -> None:
        migrated_rows = 0
        batches = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                complete, operation, row_count = self._reconcile_default_tenant_owner_batch(
                    conn,
                    batch_size=_OWNER_MIGRATION_BATCH_SIZE,
                )
            migrated_rows += row_count
            if row_count:
                batches += 1
                logger.info(
                    "feedback_tenant_owner_migration_batch",
                    operation=operation,
                    rows=row_count,
                    batch=batches,
                )
            if complete:
                if migrated_rows:
                    logger.warning(
                        "feedback_tenant_owner_migration_complete",
                        rows=migrated_rows,
                        batches=batches,
                        owner_class=_owner_class(str(self._settings.knowledge_tenant_id or "default")),
                        owner_fingerprint=_diagnostic_fingerprint(str(self._settings.knowledge_tenant_id or "default")),
                    )
                return

    def _migrate_tenant_scope(self, conn: sqlite3.Connection) -> None:
        """Atomically prepare a resumable pre-tenant shadow migration."""
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        legacy_tenant = configured_tenant if configured_tenant != "*" else "default"
        metadata_exists = self._table_exists(conn, "feedback_tenant_migration_metadata")
        shadows_exist = self._table_exists(conn, _LEGACY_PROVENANCE_SHADOW) or self._table_exists(
            conn, _LEGACY_FEEDBACK_SHADOW
        )
        if shadows_exist and not metadata_exists:
            raise RuntimeError("Feedback tenant migration shadow tables have no durable progress metadata")

        conn.execute("""CREATE TABLE IF NOT EXISTS feedback_tenant_migration_metadata (
                           key TEXT PRIMARY KEY,
                           value TEXT NOT NULL,
                           updated_at REAL NOT NULL
                       )""")
        terminal_owner = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        ).fetchone()
        if (
            terminal_owner is not None
            and configured_tenant != "*"
            and str(terminal_owner["value"]) != configured_tenant
        ):
            self._reject_owner_preflight(
                _OwnerPreflightResult(
                    _OwnerPreflightStatus.REJECTED,
                    reason_code="pinned_owner_mismatch",
                    recorded_owner=str(terminal_owner["value"]),
                )
            )
        progress_owner = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (_LEGACY_SCOPE_OWNER_MARKER,),
        ).fetchone()
        if progress_owner is not None and str(progress_owner["value"]) != legacy_tenant:
            self._reject_owner_preflight(
                _OwnerPreflightResult(
                    _OwnerPreflightStatus.REJECTED,
                    reason_code="migration_owner_mismatch",
                    recorded_owner=str(progress_owner["value"]),
                )
            )
        if progress_owner is None:
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)""",
                (_LEGACY_SCOPE_OWNER_MARKER, legacy_tenant, time.time()),
            )
        for table_name in ("dashboard_provenance", "feedback"):
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO NOTHING""",
                (
                    self._migration_cursor_key(table_name),
                    _KEYSET_NOT_STARTED,
                    time.time(),
                ),
            )

        conn.execute(f"""CREATE TABLE IF NOT EXISTS {_LEGACY_PROVENANCE_SHADOW} (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       tenant_id TEXT NOT NULL DEFAULT 'default',
                       dashboard_uid TEXT NOT NULL,
                       prompt TEXT NOT NULL,
                       problem_type TEXT NOT NULL DEFAULT '',
                       archetypes TEXT NOT NULL DEFAULT '[]',
                       metrics_used TEXT NOT NULL DEFAULT '[]',
                       panel_count INTEGER NOT NULL DEFAULT 0,
                       path_used TEXT NOT NULL DEFAULT '',
                       dashboard_url TEXT NOT NULL DEFAULT '',
                       user_id TEXT NOT NULL DEFAULT '',
                       channel_id TEXT NOT NULL DEFAULT '',
                       created_at REAL NOT NULL,
                       UNIQUE (tenant_id, dashboard_uid)
                   )""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {_LEGACY_FEEDBACK_SHADOW} (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       tenant_id TEXT NOT NULL DEFAULT 'default',
                       dashboard_uid TEXT NOT NULL,
                       reviewer TEXT NOT NULL DEFAULT '',
                       symptom_visibility INTEGER CHECK(symptom_visibility BETWEEN 1 AND 5),
                       root_cause_support INTEGER CHECK(root_cause_support BETWEEN 1 AND 5),
                       noise_level INTEGER CHECK(noise_level BETWEEN 1 AND 5),
                       investigation_speed INTEGER CHECK(investigation_speed BETWEEN 1 AND 5),
                       overall_useful INTEGER CHECK(overall_useful IN (0, 1)),
                       comment TEXT NOT NULL DEFAULT '',
                       created_at REAL NOT NULL,
                       FOREIGN KEY (tenant_id, dashboard_uid)
                           REFERENCES {_LEGACY_PROVENANCE_SHADOW}(tenant_id, dashboard_uid)
                   )""")
        self._database_id = claim_sqlite_database_identity(
            conn,
            role="feedback",
            expected_database_id=self._database_id,
        )

    @staticmethod
    def _migration_cursor_key(table_name: str) -> str:
        return f"{_LEGACY_SCOPE_CURSOR_PREFIX}{table_name}"

    @staticmethod
    def _migration_complete_key(table_name: str) -> str:
        return f"{_LEGACY_SCOPE_COMPLETE_PREFIX}{table_name}"

    def _copy_tenant_scope_batch(
        self,
        conn: sqlite3.Connection,
        *,
        source_table: str,
        shadow_table: str,
        owner: str,
        after_id: int | None,
        batch_size: int,
    ) -> tuple[int | None, int]:
        if after_id is None:
            ids = conn.execute(
                f"SELECT id FROM {source_table} ORDER BY id LIMIT ?",
                (batch_size,),
            ).fetchall()
        else:
            ids = conn.execute(
                f"SELECT id FROM {source_table} WHERE id>? ORDER BY id LIMIT ?",
                (after_id, batch_size),
            ).fetchall()
        if not ids:
            return after_id, 0
        last_id = int(ids[-1]["id"])
        keyset_predicate = "id<=?" if after_id is None else "id>? AND id<=?"
        keyset_parameters: tuple[int, ...] = (last_id,) if after_id is None else (after_id, last_id)
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({source_table})")}
        if "tenant_id" in columns:
            if owner == "*":
                tenant_expression = "tenant_id"
                parameters: tuple[Any, ...] = keyset_parameters
            else:
                tenant_expression = "COALESCE(NULLIF(tenant_id, ''), ?)"
                parameters = (owner, *keyset_parameters)
        else:
            if owner == "*":
                raise RuntimeError("Legacy feedback rows require a concrete tenant owner")
            tenant_expression = "?"
            parameters = (owner, *keyset_parameters)

        if source_table == "dashboard_provenance":
            conn.execute(
                f"""INSERT OR IGNORE INTO {shadow_table}
                       (id, tenant_id, dashboard_uid, prompt, problem_type, archetypes,
                        metrics_used, panel_count, path_used, dashboard_url, user_id,
                        channel_id, created_at)
                     SELECT id, {tenant_expression}, dashboard_uid, prompt, problem_type,
                            archetypes, metrics_used, panel_count, path_used, dashboard_url,
                            user_id, channel_id, created_at
                     FROM {source_table}
                     WHERE {keyset_predicate} ORDER BY id""",
                parameters,
            )
        else:
            conn.execute(
                f"""INSERT OR IGNORE INTO {shadow_table}
                       (id, tenant_id, dashboard_uid, reviewer, symptom_visibility,
                        root_cause_support, noise_level, investigation_speed,
                        overall_useful, comment, created_at)
                     SELECT id, {tenant_expression}, dashboard_uid, reviewer,
                            symptom_visibility, root_cause_support, noise_level,
                            investigation_speed, overall_useful, comment, created_at
                     FROM {source_table}
                     WHERE {keyset_predicate} ORDER BY id""",
                parameters,
            )
        return last_id, len(ids)

    def _migrate_tenant_scope_batch(
        self,
        conn: sqlite3.Connection,
        *,
        batch_size: int,
    ) -> tuple[bool, str, int]:
        """Copy at most one bounded legacy page and advance its cursor."""
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if not self._tenant_migration_required(conn):
            return True, "already_complete", 0
        owner_row = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (_LEGACY_SCOPE_OWNER_MARKER,),
        ).fetchone()
        if owner_row is None:
            raise RuntimeError("Feedback tenant schema migration has no pinned owner")
        owner = str(owner_row["value"])
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        expected_owner = configured_tenant if configured_tenant != "*" else "default"
        if owner != expected_owner:
            self._reject_owner_preflight(
                _OwnerPreflightResult(
                    _OwnerPreflightStatus.REJECTED,
                    reason_code="migration_owner_mismatch",
                    recorded_owner=owner,
                )
            )

        tables = (
            ("dashboard_provenance", _LEGACY_PROVENANCE_SHADOW),
            ("feedback", _LEGACY_FEEDBACK_SHADOW),
        )
        for source_table, shadow_table in tables:
            cursor_key = self._migration_cursor_key(source_table)
            complete_key = self._migration_complete_key(source_table)
            cursor_row = conn.execute(
                "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
                (cursor_key,),
            ).fetchone()
            after_id = _decode_keyset_cursor(cursor_row["value"] if cursor_row is not None else None)
            complete_row = conn.execute(
                "SELECT 1 FROM feedback_tenant_migration_metadata WHERE key=?",
                (complete_key,),
            ).fetchone()
            if complete_row is not None and self._table_exists(conn, source_table):
                if after_id is None:
                    has_new_rows = conn.execute(f"SELECT 1 FROM {source_table} LIMIT 1").fetchone() is not None
                else:
                    has_new_rows = (
                        conn.execute(f"SELECT 1 FROM {source_table} WHERE id>? LIMIT 1", (after_id,)).fetchone()
                        is not None
                    )
                if has_new_rows:
                    conn.execute("DELETE FROM feedback_tenant_migration_metadata WHERE key=?", (complete_key,))
                    complete_row = None
            if complete_row is not None:
                continue
            if not self._table_exists(conn, source_table):
                conn.execute(
                    """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                       VALUES (?, 'complete', ?)
                       ON CONFLICT(key) DO UPDATE SET value='complete', updated_at=excluded.updated_at""",
                    (complete_key, time.time()),
                )
                continue
            last_id, copied = self._copy_tenant_scope_batch(
                conn,
                source_table=source_table,
                shadow_table=shadow_table,
                owner=owner,
                after_id=after_id,
                batch_size=batch_size,
            )
            if copied:
                assert last_id is not None
                conn.execute(
                    """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                    (cursor_key, _encode_keyset_cursor(last_id), time.time()),
                )
                return False, f"{source_table}:copy", copied
            conn.execute(
                """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
                   VALUES (?, 'complete', ?)
                   ON CONFLICT(key) DO UPDATE SET value='complete', updated_at=excluded.updated_at""",
                (complete_key, time.time()),
            )

        self._finalize_tenant_scope_migration(conn)
        return True, "final_swap", 0

    def _finalize_tenant_scope_migration(self, conn: sqlite3.Connection) -> None:
        """Atomically swap complete shadow tables into the public schema."""
        for source_table in ("dashboard_provenance", "feedback"):
            complete = conn.execute(
                "SELECT 1 FROM feedback_tenant_migration_metadata WHERE key=?",
                (self._migration_complete_key(source_table),),
            ).fetchone()
            if complete is None:
                raise RuntimeError("Feedback tenant schema migration cannot finalize before every table is complete")

        if self._table_exists(conn, "feedback"):
            conn.execute("ALTER TABLE feedback RENAME TO feedback_legacy_tenant")
        if self._table_exists(conn, "dashboard_provenance"):
            conn.execute("ALTER TABLE dashboard_provenance RENAME TO dashboard_provenance_legacy_tenant")
        conn.execute(f"ALTER TABLE {_LEGACY_PROVENANCE_SHADOW} RENAME TO dashboard_provenance")
        conn.execute(f"ALTER TABLE {_LEGACY_FEEDBACK_SHADOW} RENAME TO feedback")
        if self._table_exists(conn, "feedback_legacy_tenant"):
            conn.execute("DROP TABLE feedback_legacy_tenant")
        if self._table_exists(conn, "dashboard_provenance_legacy_tenant"):
            conn.execute("DROP TABLE dashboard_provenance_legacy_tenant")
        _execute_schema_statements(conn, _SCHEMA_SQL)

        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        conn.execute(
            """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
            (_DEFAULT_OWNER_MARKER, configured_tenant, time.time()),
        )
        conn.execute(
            "DELETE FROM feedback_tenant_migration_metadata WHERE key=? OR key LIKE ? OR key LIKE ?",
            (
                _LEGACY_SCOPE_OWNER_MARKER,
                f"{_LEGACY_SCOPE_CURSOR_PREFIX}%",
                f"{_LEGACY_SCOPE_COMPLETE_PREFIX}%",
            ),
        )

    def _migrate_tenant_scope_batched(self) -> None:
        copied_rows = 0
        batches = 0
        while True:
            with self._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                complete, operation, row_count = self._migrate_tenant_scope_batch(
                    conn,
                    batch_size=_OWNER_MIGRATION_BATCH_SIZE,
                )
            copied_rows += row_count
            if row_count:
                batches += 1
                logger.info(
                    "feedback_tenant_scope_migration_batch",
                    operation=operation,
                    rows=row_count,
                    batch=batches,
                )
            if complete:
                logger.info(
                    "feedback_tenant_scope_migration_complete",
                    rows=copied_rows,
                    batches=batches,
                )
                return

    def _resolve_tenant(self, tenant_id: str | None) -> str:
        return resolve_tenant_boundary(
            str(self._settings.knowledge_tenant_id or "default"),
            tenant_id,
        )

    # ── Provenance ────────────────────────────────────────────────────────

    def record_provenance(
        self,
        dashboard_uid: str,
        prompt: str,
        *,  # force keyword args after this point
        problem_type: str = "",
        archetypes: list[dict] | None = None,
        metrics_used: list[str] | None = None,
        panel_count: int = 0,
        path_used: str = "",
        dashboard_url: str = "",
        user_id: str = "",
        channel_id: str = "",
        tenant_id: str | None = None,
    ) -> None:
        """Store dashboard generation provenance."""
        tenant_id = self._resolve_tenant(tenant_id)
        dashboard_uid = _sanitize_uid(dashboard_uid)
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO dashboard_provenance
                   (tenant_id, dashboard_uid, prompt, problem_type, archetypes,
                    metrics_used, panel_count, path_used, dashboard_url,
                    user_id, channel_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(tenant_id, dashboard_uid) DO UPDATE SET
                       prompt=excluded.prompt,
                       problem_type=excluded.problem_type,
                       archetypes=excluded.archetypes,
                       metrics_used=excluded.metrics_used,
                       panel_count=excluded.panel_count,
                       path_used=excluded.path_used,
                       dashboard_url=excluded.dashboard_url,
                       user_id=excluded.user_id,
                       channel_id=excluded.channel_id,
                       created_at=excluded.created_at""",
                (
                    tenant_id,
                    dashboard_uid,
                    prompt,
                    problem_type,
                    json.dumps(archetypes or []),
                    json.dumps(metrics_used or []),
                    panel_count,
                    path_used,
                    dashboard_url,
                    user_id,
                    channel_id,
                    time.time(),
                ),
            )
        logger.info("provenance_recorded", dashboard_uid=dashboard_uid, tenant_id=tenant_id)

    def get_provenance(
        self,
        dashboard_uid: str,
        *,
        tenant_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve provenance for a dashboard."""
        tenant_id = self._resolve_tenant(tenant_id)
        dashboard_uid = _sanitize_uid(dashboard_uid)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM dashboard_provenance WHERE tenant_id = ? AND dashboard_uid = ?",
                (tenant_id, dashboard_uid),
            ).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["archetypes"] = json.loads(d["archetypes"])
        d["metrics_used"] = json.loads(d["metrics_used"])
        return d

    # ── Feedback ──────────────────────────────────────────────────────────

    def submit_feedback(
        self,
        dashboard_uid: str,
        symptom_visibility: int | None = None,
        root_cause_support: int | None = None,
        noise_level: int | None = None,
        investigation_speed: int | None = None,
        overall_useful: bool | None = None,
        comment: str = "",
        reviewer: str = "",
        tenant_id: str | None = None,
    ) -> int:
        """Store human feedback for a dashboard. Returns feedback id."""
        tenant_id = self._resolve_tenant(tenant_id)
        dashboard_uid = _sanitize_uid(dashboard_uid)
        with self._conn() as conn:
            provenance = conn.execute(
                "SELECT 1 FROM dashboard_provenance WHERE tenant_id=? AND dashboard_uid=?",
                (tenant_id, dashboard_uid),
            ).fetchone()
            if provenance is None:
                raise ValueError("feedback requires dashboard provenance in the same tenant")
            cursor = conn.execute(
                """INSERT INTO feedback
                   (tenant_id, dashboard_uid, reviewer, symptom_visibility, root_cause_support,
                    noise_level, investigation_speed, overall_useful, comment, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    tenant_id,
                    dashboard_uid,
                    reviewer,
                    symptom_visibility,
                    root_cause_support,
                    noise_level,
                    investigation_speed,
                    int(overall_useful) if overall_useful is not None else None,
                    comment,
                    time.time(),
                ),
            )
            feedback_id = cursor.lastrowid
        logger.info(
            "feedback_submitted",
            dashboard_uid=dashboard_uid,
            feedback_id=feedback_id,
            tenant_id=tenant_id,
        )
        return feedback_id

    def get_feedback(
        self,
        dashboard_uid: str,
        *,
        tenant_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve all feedback for a dashboard."""
        tenant_id = self._resolve_tenant(tenant_id)
        dashboard_uid = _sanitize_uid(dashboard_uid)
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT * FROM feedback
                   WHERE tenant_id = ? AND dashboard_uid = ? ORDER BY created_at DESC""",
                (tenant_id, dashboard_uid),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_aggregate_stats(self, *, tenant_id: str | None = None) -> dict[str, Any]:
        """Aggregate feedback statistics across all dashboards."""
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM feedback WHERE tenant_id = ?",
                (tenant_id,),
            ).fetchone()[0]
            if total == 0:
                return {
                    "total_feedback": 0,
                    "total_dashboards": 0,
                    "useful_rate": None,
                    "avg_symptom_visibility": None,
                    "avg_root_cause_support": None,
                    "avg_noise_level": None,
                    "avg_investigation_speed": None,
                }

            row = conn.execute(
                """SELECT
                       COUNT(*) as total,
                       AVG(symptom_visibility) as avg_symptom,
                       AVG(root_cause_support) as avg_root_cause,
                       AVG(noise_level) as avg_noise,
                       AVG(investigation_speed) as avg_speed,
                       AVG(CAST(overall_useful AS FLOAT)) as useful_rate
                   FROM feedback
                   WHERE tenant_id = ?""",
                (tenant_id,),
            ).fetchone()

            return {
                "total_feedback": row["total"],
                "avg_symptom_visibility": round(row["avg_symptom"] or 0, 2),
                "avg_root_cause_support": round(row["avg_root_cause"] or 0, 2),
                "avg_noise_level": round(row["avg_noise"] or 0, 2),
                "avg_investigation_speed": round(row["avg_speed"] or 0, 2),
                "useful_rate": round(row["useful_rate"] or 0, 3),
                "total_dashboards": conn.execute(
                    "SELECT COUNT(DISTINCT dashboard_uid) FROM feedback WHERE tenant_id = ?",
                    (tenant_id,),
                ).fetchone()[0],
            }

    # ── Feedback Analysis (closes the loop) ───────────────────────────

    def analyze(self, *, tenant_id: str | None = None) -> dict[str, Any]:
        """Analyze feedback to produce actionable improvement signals.

        Returns a report with:
        - per_archetype_quality: which archetypes score well/poorly
        - noisy_dashboards: dashboards with low noise_level (candidates for panel pruning)
        - low_symptom_dashboards: dashboards where symptom wasn't visible (missing critical metrics)
        - archetype_gaps: prompts where no archetype matched but dashboard was useful
        - metric_quality: metrics appearing in high vs low rated dashboards
        - confidence_calibration: are high-confidence archetypes actually better?
        - recommendations: ordered list of concrete actions
        """
        tenant_id = self._resolve_tenant(tenant_id)
        with self._conn() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM feedback WHERE tenant_id = ?",
                (tenant_id,),
            ).fetchone()[0]
            if total == 0:
                return {"status": "no_feedback", "recommendations": []}

            report: dict[str, Any] = {"total_feedback": total}

            # ── Per-archetype quality ──────────────────────────────────
            rows = conn.execute(
                """SELECT
                       p.problem_type,
                       COUNT(*) as n,
                       AVG(f.symptom_visibility) as avg_symptom,
                       AVG(f.root_cause_support) as avg_root_cause,
                       AVG(f.noise_level) as avg_noise,
                       AVG(f.investigation_speed) as avg_speed,
                       AVG(CAST(f.overall_useful AS FLOAT)) as useful_rate
                   FROM feedback f
                   JOIN dashboard_provenance p
                     ON f.tenant_id = p.tenant_id AND f.dashboard_uid = p.dashboard_uid
                   WHERE f.tenant_id = ?
                   GROUP BY p.problem_type
                   ORDER BY useful_rate ASC""",
                (tenant_id,),
            ).fetchall()
            report["per_archetype_quality"] = [
                {
                    "archetype": r["problem_type"],
                    "count": r["n"],
                    "avg_symptom": round(r["avg_symptom"] or 0, 2),
                    "avg_root_cause": round(r["avg_root_cause"] or 0, 2),
                    "avg_noise": round(r["avg_noise"] or 0, 2),
                    "avg_speed": round(r["avg_speed"] or 0, 2),
                    "useful_rate": round(r["useful_rate"] or 0, 3),
                }
                for r in rows
            ]

            # ── Noisy dashboards (noise_level <= 2) ───────────────────
            rows = conn.execute(
                """SELECT p.dashboard_uid, p.prompt, p.problem_type,
                          p.metrics_used, f.noise_level, f.comment
                   FROM feedback f
                   JOIN dashboard_provenance p
                     ON f.tenant_id = p.tenant_id AND f.dashboard_uid = p.dashboard_uid
                   WHERE f.tenant_id = ?
                     AND f.noise_level IS NOT NULL AND f.noise_level <= 2
                   ORDER BY f.noise_level ASC
                   LIMIT 20""",
                (tenant_id,),
            ).fetchall()
            report["noisy_dashboards"] = [
                {
                    "dashboard_uid": r["dashboard_uid"],
                    "prompt": r["prompt"][:100],
                    "archetype": r["problem_type"],
                    "metrics_used": json.loads(r["metrics_used"]),
                    "noise_level": r["noise_level"],
                    "comment": r["comment"],
                }
                for r in rows
            ]

            # ── Low symptom visibility (symptom <= 2) ─────────────────
            rows = conn.execute(
                """SELECT p.dashboard_uid, p.prompt, p.problem_type,
                          p.metrics_used, f.symptom_visibility, f.comment
                   FROM feedback f
                   JOIN dashboard_provenance p
                     ON f.tenant_id = p.tenant_id AND f.dashboard_uid = p.dashboard_uid
                   WHERE f.tenant_id = ?
                     AND f.symptom_visibility IS NOT NULL AND f.symptom_visibility <= 2
                   ORDER BY f.symptom_visibility ASC
                   LIMIT 20""",
                (tenant_id,),
            ).fetchall()
            report["low_symptom_dashboards"] = [
                {
                    "dashboard_uid": r["dashboard_uid"],
                    "prompt": r["prompt"][:100],
                    "archetype": r["problem_type"],
                    "metrics_used": json.loads(r["metrics_used"]),
                    "symptom_visibility": r["symptom_visibility"],
                    "comment": r["comment"],
                }
                for r in rows
            ]

            # ── Archetype gaps (freeform path but useful) ─────────────
            rows = conn.execute(
                """SELECT p.dashboard_uid, p.prompt, p.problem_type,
                          f.overall_useful, f.comment
                   FROM feedback f
                   JOIN dashboard_provenance p
                     ON f.tenant_id = p.tenant_id AND f.dashboard_uid = p.dashboard_uid
                   WHERE f.tenant_id = ? AND p.path_used = 'freeform' AND f.overall_useful = 1
                   LIMIT 20""",
                (tenant_id,),
            ).fetchall()
            report["archetype_gaps"] = [
                {
                    "prompt": r["prompt"][:120],
                    "problem_type": r["problem_type"],
                    "comment": r["comment"],
                }
                for r in rows
            ]

            # ── Metric quality signal ─────────────────────────────────
            # Metrics in high-rated (useful=1) vs low-rated (useful=0)
            good_metrics: dict[str, int] = {}
            bad_metrics: dict[str, int] = {}

            rows = conn.execute(
                """SELECT p.metrics_used, f.overall_useful
                   FROM feedback f
                   JOIN dashboard_provenance p
                     ON f.tenant_id = p.tenant_id AND f.dashboard_uid = p.dashboard_uid
                   WHERE f.tenant_id = ? AND f.overall_useful IS NOT NULL""",
                (tenant_id,),
            ).fetchall()
            for r in rows:
                metrics = json.loads(r["metrics_used"])
                bucket = good_metrics if r["overall_useful"] else bad_metrics
                for m in metrics:
                    bucket[m] = bucket.get(m, 0) + 1

            all_metrics = set(good_metrics) | set(bad_metrics)
            metric_scores: list[dict[str, Any]] = []
            for m in all_metrics:
                good = good_metrics.get(m, 0)
                bad_count = bad_metrics.get(m, 0)
                total_m = good + bad_count
                score = good / total_m if total_m > 0 else 0.5
                metric_scores.append(
                    {
                        "metric": m,
                        "good": good,
                        "bad": bad_count,
                        "quality_score": round(score, 3),
                    }
                )
            metric_scores.sort(key=lambda x: float(x["quality_score"]))
            report["metric_quality"] = metric_scores

            # ── Confidence calibration ────────────────────────────────
            # Do high-confidence archetypes actually produce better dashboards?
            rows = conn.execute(
                """SELECT p.archetypes, f.overall_useful,
                          f.symptom_visibility, f.noise_level
                   FROM feedback f
                   JOIN dashboard_provenance p
                     ON f.tenant_id = p.tenant_id AND f.dashboard_uid = p.dashboard_uid
                   WHERE f.tenant_id = ? AND f.overall_useful IS NOT NULL""",
                (tenant_id,),
            ).fetchall()

            high_conf: list[dict[str, Any]] = []
            low_conf: list[dict[str, Any]] = []
            for r in rows:
                archetypes = json.loads(r["archetypes"])
                top_conf = archetypes[0]["confidence"] if archetypes else 0
                confidence_bucket = high_conf if top_conf >= 0.8 else low_conf
                confidence_bucket.append(
                    {
                        "useful": r["overall_useful"],
                        "symptom": r["symptom_visibility"],
                        "noise": r["noise_level"],
                    }
                )

            def _avg(items: list[dict[str, Any]], key: str) -> float | None:
                vals = [i[key] for i in items if i[key] is not None]
                return round(sum(vals) / len(vals), 2) if vals else None

            report["confidence_calibration"] = {
                "high_confidence_ge_0.8": {
                    "count": len(high_conf),
                    "useful_rate": _avg(high_conf, "useful"),
                    "avg_symptom": _avg(high_conf, "symptom"),
                    "avg_noise": _avg(high_conf, "noise"),
                },
                "low_confidence_lt_0.8": {
                    "count": len(low_conf),
                    "useful_rate": _avg(low_conf, "useful"),
                    "avg_symptom": _avg(low_conf, "symptom"),
                    "avg_noise": _avg(low_conf, "noise"),
                },
            }

            # ── Generate recommendations ──────────────────────────────
            recommendations: list[str] = []

            # Noisy archetypes
            for aq in report["per_archetype_quality"]:
                if aq["avg_noise"] and aq["avg_noise"] < 3.0 and aq["count"] >= 3:
                    recommendations.append(
                        f"PRUNE: '{aq['archetype']}' has avg noise={aq['avg_noise']}/5 — "
                        f"review its panel templates for irrelevant metrics"
                    )

            # Low symptom archetypes
            for aq in report["per_archetype_quality"]:
                if aq["avg_symptom"] and aq["avg_symptom"] < 3.0 and aq["count"] >= 3:
                    recommendations.append(
                        f"ADD SIGNAL: '{aq['archetype']}' has avg symptom={aq['avg_symptom']}/5 — "
                        f"critical metrics may be missing from its template"
                    )

            # Archetype gap candidates
            if report["archetype_gaps"]:
                prompts = set(g["problem_type"] for g in report["archetype_gaps"])
                recommendations.append(
                    f"NEW ARCHETYPE: {len(report['archetype_gaps'])} useful dashboards "
                    f"hit freeform path — consider new archetypes for: {', '.join(prompts)}"
                )

            # Low-quality metrics
            bad_metric_scores = [m for m in metric_scores if m["quality_score"] < 0.3 and m["bad"] >= 2]
            if bad_metric_scores:
                names = ", ".join(m["metric"] for m in bad_metric_scores[:5])
                recommendations.append(f"DEPRIORITIZE METRICS: {names} — appear mostly in poorly-rated dashboards")

            # Confidence miscalibration
            cal = report["confidence_calibration"]
            hi = cal["high_confidence_ge_0.8"]
            lo = cal["low_confidence_lt_0.8"]
            if (
                hi["useful_rate"] is not None
                and lo["useful_rate"] is not None
                and lo["useful_rate"] > hi["useful_rate"] + 0.1
                and lo["count"] >= 3
            ):
                recommendations.append(
                    f"RECALIBRATE: low-confidence archetypes ({lo['useful_rate']:.0%} useful) "
                    f"outperform high-confidence ({hi['useful_rate']:.0%}) — "
                    f"confidence scoring may need adjustment"
                )

            report["recommendations"] = recommendations
            return report


# ── Singleton ─────────────────────────────────────────────────────────────

_store: FeedbackStore | None = None


def get_feedback_store() -> FeedbackStore:
    """Get or create the global FeedbackStore singleton."""
    global _store
    if _store is None:
        _store = FeedbackStore()
    return _store
