"""Investigation history store.

Full request persistence for every pipeline run:
- prompts, intent, archetypes, selected metrics, generated queries
- per-step timings, failures, validation warnings
- dashboard URLs and UIDs

SQLite-backed. Complements the feedback store (which tracks post-hoc human ratings)
by capturing the full investigation lifecycle for debugging and audit.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import structlog

from dashforge.config import settings

logger = structlog.get_logger()

_DEFAULT_DB_PATH = Path("data/dashforge_history.db")
_SQLITE_BUSY_TIMEOUT_MS = 30_000


def _db_path() -> Path:
    custom = getattr(settings, "history_db_path", None)
    path = Path(custom) if custom else _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS investigations (
    id              TEXT PRIMARY KEY,          -- UUID
    prompt          TEXT NOT NULL,
    user_id         TEXT NOT NULL DEFAULT '',
    channel_id      TEXT NOT NULL DEFAULT '',

    -- Intent
    intent_summary  TEXT NOT NULL DEFAULT '',
    intent_domain   TEXT NOT NULL DEFAULT '',
    intent_services TEXT NOT NULL DEFAULT '[]', -- JSON array
    intent_keywords TEXT NOT NULL DEFAULT '[]', -- JSON array
    intent_signals  TEXT NOT NULL DEFAULT '[]', -- JSON array
    problem_type    TEXT NOT NULL DEFAULT '',
    archetypes      TEXT NOT NULL DEFAULT '[]', -- JSON: [{type, confidence}]
    timerange       TEXT NOT NULL DEFAULT '',

    -- Metrics
    datasources_found    INTEGER NOT NULL DEFAULT 0,
    datasource_types     TEXT NOT NULL DEFAULT '[]',  -- JSON array
    metrics_catalog_size INTEGER NOT NULL DEFAULT 0,
    metrics_selected     TEXT NOT NULL DEFAULT '[]',  -- JSON array of metric names
    metrics_ranked_size  INTEGER NOT NULL DEFAULT 0,

    -- Queries
    generated_queries TEXT NOT NULL DEFAULT '[]', -- JSON: [{expr, panel_title}]
    panel_count       INTEGER NOT NULL DEFAULT 0,

    -- Routing
    path_used TEXT NOT NULL DEFAULT '',  -- 'archetype', 'freeform', 'failed'

    -- Validation
    validation_warnings TEXT NOT NULL DEFAULT '[]', -- JSON array
    panels_dropped      INTEGER NOT NULL DEFAULT 0,

    -- Diagnostic stage outcomes
    stage_outcomes TEXT NOT NULL DEFAULT '{}', -- JSON: {stage: {status, reason_code, details}}

    -- Result
    dashboard_uid TEXT NOT NULL DEFAULT '',
    dashboard_url TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'running', -- running, success, failed, timeout
    error         TEXT NOT NULL DEFAULT '',

    -- Timings
    timings     TEXT NOT NULL DEFAULT '{}', -- JSON: {step: seconds}
    total_time  REAL NOT NULL DEFAULT 0,

    -- Timestamps
    started_at  REAL NOT NULL,
    finished_at REAL
);

CREATE INDEX IF NOT EXISTS idx_inv_status ON investigations(status);
CREATE INDEX IF NOT EXISTS idx_inv_user ON investigations(user_id);
CREATE INDEX IF NOT EXISTS idx_inv_started ON investigations(started_at);
CREATE INDEX IF NOT EXISTS idx_inv_dashboard ON investigations(dashboard_uid);
"""


class InvestigationStore:
    """SQLite-backed investigation history."""

    def __init__(self, db_path: Path | None = None):
        self._db_path = db_path or _db_path()
        self._ensure_schema()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self._db_path), timeout=_SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self):
        with self._conn() as conn:
            conn.executescript(_SCHEMA_SQL)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(investigations)")}
            if "stage_outcomes" not in columns:
                conn.execute("ALTER TABLE investigations ADD COLUMN stage_outcomes TEXT NOT NULL DEFAULT '{}'")
        logger.info("investigation_store_init", db_path=str(self._db_path))

    # ── Write operations ──────────────────────────────────────────────────

    def start(self, prompt: str, user_id: str = "", channel_id: str = "") -> str:
        """Record the start of a new investigation. Returns investigation ID."""
        inv_id = uuid.uuid4().hex[:16]
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO investigations (id, prompt, user_id, channel_id, started_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (inv_id, prompt, user_id, channel_id, time.time()),
            )
        return inv_id

    def record_intent(
        self,
        inv_id: str,
        *,
        summary: str = "",
        domain: str = "",
        services: list[str] | None = None,
        keywords: list[str] | None = None,
        signals: list[str] | None = None,
        problem_type: str = "",
        archetypes: list[dict] | None = None,
        timerange: str = "",
    ) -> None:
        """Record intent classification results."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   intent_summary=?, intent_domain=?, intent_services=?,
                   intent_keywords=?, intent_signals=?, problem_type=?,
                   archetypes=?, timerange=?
                   WHERE id=?""",
                (
                    summary,
                    domain,
                    json.dumps(services or []),
                    json.dumps(keywords or []),
                    json.dumps(signals or []),
                    problem_type,
                    json.dumps(archetypes or []),
                    timerange,
                    inv_id,
                ),
            )

    def record_discovery(
        self,
        inv_id: str,
        *,
        datasources_found: int = 0,
        datasource_types: list[str] | None = None,
        metrics_catalog_size: int = 0,
        metrics_ranked_size: int = 0,
    ) -> None:
        """Record datasource & metric discovery results."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   datasources_found=?, datasource_types=?,
                   metrics_catalog_size=?, metrics_ranked_size=?
                   WHERE id=?""",
                (
                    datasources_found,
                    json.dumps(datasource_types or []),
                    metrics_catalog_size,
                    metrics_ranked_size,
                    inv_id,
                ),
            )

    def record_queries(
        self,
        inv_id: str,
        *,
        metrics_selected: list[str] | None = None,
        generated_queries: list[dict] | None = None,
        panel_count: int = 0,
        path_used: str = "",
    ) -> None:
        """Record generated queries and panel info."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   metrics_selected=?, generated_queries=?,
                   panel_count=?, path_used=?
                   WHERE id=?""",
                (
                    json.dumps(metrics_selected or []),
                    json.dumps(generated_queries or []),
                    panel_count,
                    path_used,
                    inv_id,
                ),
            )

    def record_validation(
        self,
        inv_id: str,
        *,
        warnings: list[str] | None = None,
        panels_dropped: int = 0,
        final_panel_count: int = 0,
    ) -> None:
        """Record query validation results."""
        with self._conn() as conn:
            conn.execute(
                """UPDATE investigations SET
                   validation_warnings=?, panels_dropped=?, panel_count=?
                   WHERE id=?""",
                (
                    json.dumps(warnings or []),
                    panels_dropped,
                    final_panel_count,
                    inv_id,
                ),
            )

    def record_stage(
        self,
        inv_id: str,
        stage: str,
        *,
        status: str,
        reason_code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Persist one reason-coded diagnostic stage outcome."""
        with self._conn() as conn:
            row = conn.execute("SELECT stage_outcomes FROM investigations WHERE id=?", (inv_id,)).fetchone()
            if row is None:
                return
            try:
                outcomes = json.loads(row[0] or "{}")
            except (json.JSONDecodeError, TypeError):
                outcomes = {}
            outcomes[stage] = {
                "status": status,
                "reason_code": reason_code,
                "details": details or {},
            }
            conn.execute(
                "UPDATE investigations SET stage_outcomes=? WHERE id=?",
                (json.dumps(outcomes), inv_id),
            )

    def finish(
        self,
        inv_id: str,
        *,
        status: str = "success",
        dashboard_uid: str = "",
        dashboard_url: str = "",
        error: str = "",
        timings: dict[str, float] | None = None,
        total_time: float = 0,
    ) -> None:
        """Record the final result of an investigation."""
        with self._conn() as conn:
            row = conn.execute("SELECT stage_outcomes FROM investigations WHERE id=?", (inv_id,)).fetchone()
            try:
                outcomes = json.loads(row[0] or "{}") if row else {}
            except (json.JSONDecodeError, TypeError):
                outcomes = {}
            outcomes.setdefault(
                "ranking",
                {
                    "status": "skipped",
                    "reason_code": "culprit_ranking_not_implemented",
                    "details": {},
                },
            )
            conn.execute(
                """UPDATE investigations SET
                   status=?, dashboard_uid=?, dashboard_url=?,
                   error=?, timings=?, total_time=?, finished_at=?, stage_outcomes=?
                   WHERE id=?""",
                (
                    status,
                    dashboard_uid,
                    dashboard_url,
                    error,
                    json.dumps(timings or {}),
                    total_time,
                    time.time(),
                    json.dumps(outcomes),
                    inv_id,
                ),
            )

    # ── Read operations ──────────────────────────────────────────────────

    def get(self, inv_id: str) -> dict[str, Any] | None:
        """Get a single investigation by ID."""
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM investigations WHERE id=?", (inv_id,)).fetchone()
            return self._row_to_dict(row) if row else None

    def get_by_dashboard(self, dashboard_uid: str) -> dict[str, Any] | None:
        """Get investigation by dashboard UID."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM investigations WHERE dashboard_uid=?",
                (dashboard_uid,),
            ).fetchone()
            return self._row_to_dict(row) if row else None

    def list_recent(
        self,
        limit: int = 50,
        offset: int = 0,
        status: str | None = None,
        user_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List recent investigations, newest first."""
        query = "SELECT * FROM investigations"
        params: list[Any] = []
        conditions: list[str] = []

        if status:
            conditions.append("status=?")
            params.append(status)
        if user_id:
            conditions.append("user_id=?")
            params.append(user_id)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY started_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def stats(self) -> dict[str, Any]:
        """Aggregate stats across all investigations."""
        with self._conn() as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as succeeded,
                    SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status='timeout' THEN 1 ELSE 0 END) as timed_out,
                    AVG(total_time) as avg_time,
                    AVG(panel_count) as avg_panels,
                    AVG(metrics_catalog_size) as avg_catalog_size,
                    SUM(CASE WHEN path_used='archetype' THEN 1 ELSE 0 END) as archetype_path,
                    SUM(CASE WHEN path_used='freeform' THEN 1 ELSE 0 END) as freeform_path
                FROM investigations
            """).fetchone()
            return dict(row) if row else {}

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        """Convert a Row to dict, parsing JSON fields."""
        d = dict(row)
        for key in (
            "intent_services",
            "intent_keywords",
            "intent_signals",
            "archetypes",
            "datasource_types",
            "metrics_selected",
            "generated_queries",
            "validation_warnings",
            "stage_outcomes",
            "timings",
        ):
            if key in d and isinstance(d[key], str):
                try:
                    d[key] = json.loads(d[key])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d


# ── Singleton ─────────────────────────────────────────────────────────────

_store: InvestigationStore | None = None


def get_investigation_store() -> InvestigationStore:
    global _store
    if _store is None:
        _store = InvestigationStore()
    return _store
