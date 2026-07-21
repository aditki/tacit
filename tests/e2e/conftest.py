from __future__ import annotations

from pathlib import Path

import pytest

import tacit.archetypes.templates as templates
import tacit.dashboard_ingest as dashboard_ingest
import tacit.feedback as feedback_mod
import tacit.history as history_mod
import tacit.main as main_mod
import tacit.pipeline as pipeline_mod
import tacit.signals as signals_mod
from tacit.config import settings
from tacit.feedback import FeedbackStore
from tacit.history import InvestigationStore
from tacit.signals import SignalStore


def pytest_collection_modifyitems(config, items):
    e2e_root = Path(__file__).parent
    for item in items:
        if Path(str(item.fspath)).is_relative_to(e2e_root):
            item.add_marker(pytest.mark.e2e)


@pytest.fixture
def isolated_learning_runtime(tmp_path, monkeypatch):
    signal_store = SignalStore(db_path=tmp_path / "signals.db")
    signal_store.load_from_yaml()
    history_store = InvestigationStore(db_path=tmp_path / "history.db")
    feedback_store = FeedbackStore(db_path=tmp_path / "feedback.db")
    archetypes_path = tmp_path / "curated_archetypes.yaml"
    quarantine_path = tmp_path / "generated_archetypes" / "quarantine"

    monkeypatch.setattr(signals_mod, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(dashboard_ingest, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(history_mod, "get_investigation_store", lambda: history_store)
    monkeypatch.setattr(pipeline_mod, "get_investigation_store", lambda: history_store)
    monkeypatch.setattr(feedback_mod, "get_feedback_store", lambda: feedback_store)
    monkeypatch.setattr(main_mod, "get_feedback_store", lambda: feedback_store)
    monkeypatch.setattr(settings, "learned_archetypes_generation_enabled", True)
    monkeypatch.setattr(settings, "learned_archetypes_automatic_registration_enabled", True)
    monkeypatch.setattr(settings, "learned_archetypes_normal_retrieval_enabled", False)
    monkeypatch.setattr(settings, "learned_archetypes_retrieval_mode", "curated_only")
    monkeypatch.setattr(settings, "learned_archetypes_quarantine_path", str(quarantine_path))
    monkeypatch.setattr(settings, "api_auth_enabled", False)
    monkeypatch.setenv("TACIT_ARCHETYPES_PATH", str(archetypes_path))
    templates.reload_archetypes()

    try:
        yield signal_store, history_store, feedback_store, archetypes_path, quarantine_path
    finally:
        monkeypatch.delenv("TACIT_ARCHETYPES_PATH", raising=False)
        templates.reload_archetypes()
