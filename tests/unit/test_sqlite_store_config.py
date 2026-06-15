from __future__ import annotations

from dashforge.feedback import FeedbackStore
from dashforge.history import InvestigationStore
from dashforge.signals import SignalStore


def test_signal_store_sets_busy_timeout(tmp_path):
    store = SignalStore(db_path=tmp_path / "signals.db")

    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000


def test_feedback_store_sets_busy_timeout(tmp_path):
    store = FeedbackStore(db_path=tmp_path / "feedback.db")

    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000


def test_history_store_sets_busy_timeout(tmp_path):
    store = InvestigationStore(db_path=tmp_path / "history.db")

    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000
