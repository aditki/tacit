from __future__ import annotations

import pytest

import dashforge.archetypes.templates as templates
import dashforge.dashboard_ingest as dashboard_ingest
import dashforge.feedback as feedback_mod
import dashforge.history as history_mod
import dashforge.main as main_mod
import dashforge.pipeline as pipeline_mod
import dashforge.signals as signals_mod
from dashforge.config import settings
from dashforge.feedback import FeedbackStore
from dashforge.history import InvestigationStore
from dashforge.signals import SignalStore


@pytest.fixture
def isolated_learning_runtime(tmp_path, monkeypatch):
    signal_store = SignalStore(db_path=tmp_path / "signals.db")
    signal_store.load_from_yaml()
    history_store = InvestigationStore(db_path=tmp_path / "history.db")
    feedback_store = FeedbackStore(db_path=tmp_path / "feedback.db")
    archetypes_path = tmp_path / "learned_archetypes.yaml"

    monkeypatch.setattr(signals_mod, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(dashboard_ingest, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(history_mod, "get_investigation_store", lambda: history_store)
    monkeypatch.setattr(pipeline_mod, "get_investigation_store", lambda: history_store)
    monkeypatch.setattr(feedback_mod, "get_feedback_store", lambda: feedback_store)
    monkeypatch.setattr(main_mod, "get_feedback_store", lambda: feedback_store)
    monkeypatch.setattr(settings, "learning_auto_register_archetype", True)
    monkeypatch.setattr(settings, "api_auth_enabled", False)
    monkeypatch.setenv("DASHFORGE_ARCHETYPES_PATH", str(archetypes_path))
    templates.reload_archetypes()

    try:
        yield signal_store, history_store, feedback_store, archetypes_path
    finally:
        monkeypatch.delenv("DASHFORGE_ARCHETYPES_PATH", raising=False)
        templates.reload_archetypes()
