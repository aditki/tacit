"""Feedback & dashboard provenance store.

Lightweight SQLite-backed persistence for:
- Dashboard provenance (prompt, archetypes, metrics used per generation)
- Human feedback ratings (dimensional SRE evaluation per dashboard)

In production, swap for Postgres via SQLAlchemy or similar.
"""

from __future__ import annotations

import json
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import structlog

from tacit.config import DEFAULT_FEEDBACK_DB_PATH, Settings, settings
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


def _db_path(runtime_settings: Settings | None = None) -> Path:
    """Resolve DB path from settings or default."""
    active_settings = runtime_settings or settings
    custom = active_settings.feedback_db_path
    path = Path(custom) if custom else _DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


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

CREATE TABLE IF NOT EXISTS feedback_tenant_migration_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""


class FeedbackStore:
    """SQLite-backed store for dashboard provenance and human feedback."""

    def __init__(self, db_path: Path | None = None, *, runtime_settings: Settings | None = None):
        self._settings = runtime_settings or settings
        self._db_path = db_path or _db_path(self._settings)
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
            self._require_confirmed_default_tenant_owner(conn)
            if self._tenant_migration_required(conn):
                self._require_legacy_tenant_owner(conn)
                self._migrate_tenant_scope(conn)
            conn.executescript(_SCHEMA_SQL)
            self._reconcile_default_tenant_owner(conn)
        logger.info("feedback_store_init", db_path=str(self._db_path))

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
        logger.error("legacy_feedback_owner_required", tables=ownerless_tables)
        raise RuntimeError(
            "Legacy feedback data has no tenant owner. Start once with knowledge_tenant_id pinned to its owner "
            "before enabling wildcard tenancy. Affected tables: " + ", ".join(ownerless_tables)
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
            logger.error("legacy_feedback_default_owner_unconfirmed", tables=ambiguous_tables)
            raise RuntimeError(
                "Previously migrated feedback has unconfirmed default-tenant ownership. Start once with "
                "knowledge_tenant_id pinned to its owner before enabling wildcard tenancy. Affected tables: "
                + ", ".join(ambiguous_tables)
            )

    def _reconcile_default_tenant_owner(self, conn: sqlite3.Connection) -> None:
        marker = conn.execute(
            "SELECT value FROM feedback_tenant_migration_metadata WHERE key=?",
            (_DEFAULT_OWNER_MARKER,),
        ).fetchone()
        if marker is not None:
            return
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        if configured_tenant not in {"*", "default"}:
            conn.execute("UPDATE feedback SET tenant_id=? WHERE tenant_id='default'", (configured_tenant,))
            conn.execute(
                "UPDATE dashboard_provenance SET tenant_id=? WHERE tenant_id='default'",
                (configured_tenant,),
            )
        conn.execute(
            """INSERT INTO feedback_tenant_migration_metadata (key, value, updated_at)
               VALUES (?, ?, ?)""",
            (_DEFAULT_OWNER_MARKER, configured_tenant, time.time()),
        )

    def _migrate_tenant_scope(self, conn: sqlite3.Connection) -> None:
        """Atomically rebuild legacy feedback tables around tenant-scoped keys."""
        configured_tenant = str(self._settings.knowledge_tenant_id or "default")
        # Wildcard migration reaches this point only after the owner preflight
        # proves that no pre-tenant rows need assignment.
        legacy_tenant = configured_tenant if configured_tenant != "*" else "default"
        if configured_tenant == "*":
            logger.info(
                "legacy_feedback_wildcard_rebuild_without_ownerless_rows",
                placeholder_tenant=legacy_tenant,
            )
        has_provenance = self._table_exists(conn, "dashboard_provenance")
        has_feedback = self._table_exists(conn, "feedback")
        provenance_columns = (
            {row[1] for row in conn.execute("PRAGMA table_info(dashboard_provenance)")} if has_provenance else set()
        )
        feedback_columns = {row[1] for row in conn.execute("PRAGMA table_info(feedback)")} if has_feedback else set()

        conn.execute("BEGIN IMMEDIATE")
        try:
            if has_feedback:
                conn.execute("ALTER TABLE feedback RENAME TO feedback_legacy_tenant")
            if has_provenance:
                conn.execute("ALTER TABLE dashboard_provenance RENAME TO dashboard_provenance_legacy_tenant")
            conn.execute("""CREATE TABLE dashboard_provenance (
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
            conn.execute("""CREATE TABLE feedback (
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
                           REFERENCES dashboard_provenance(tenant_id, dashboard_uid)
                   )""")
            if has_provenance:
                tenant_expression = "COALESCE(NULLIF(tenant_id, ''), ?)" if "tenant_id" in provenance_columns else "?"
                conn.execute(
                    f"""INSERT INTO dashboard_provenance
                           (id, tenant_id, dashboard_uid, prompt, problem_type, archetypes,
                            metrics_used, panel_count, path_used, dashboard_url, user_id,
                            channel_id, created_at)
                         SELECT id, {tenant_expression}, dashboard_uid, prompt, problem_type,
                                archetypes, metrics_used, panel_count, path_used, dashboard_url,
                                user_id, channel_id, created_at
                         FROM dashboard_provenance_legacy_tenant""",
                    (legacy_tenant,),
                )
            if has_feedback:
                tenant_expression = "COALESCE(NULLIF(tenant_id, ''), ?)" if "tenant_id" in feedback_columns else "?"
                conn.execute(
                    f"""INSERT INTO feedback
                           (id, tenant_id, dashboard_uid, reviewer, symptom_visibility,
                            root_cause_support, noise_level, investigation_speed,
                            overall_useful, comment, created_at)
                         SELECT id, {tenant_expression}, dashboard_uid, reviewer,
                                symptom_visibility, root_cause_support, noise_level,
                                investigation_speed, overall_useful, comment, created_at
                         FROM feedback_legacy_tenant""",
                    (legacy_tenant,),
                )
            if has_feedback:
                conn.execute("DROP TABLE feedback_legacy_tenant")
            if has_provenance:
                conn.execute("DROP TABLE dashboard_provenance_legacy_tenant")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        logger.info("feedback_tenant_scope_migrated", tenant_id=legacy_tenant)

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
