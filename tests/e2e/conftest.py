from __future__ import annotations

from pathlib import Path

import pytest

import tacit.archetypes.templates as templates
import tacit.cli as cli_mod
import tacit.dashboard_ingest as dashboard_ingest
import tacit.feedback as feedback_mod
import tacit.history as history_mod
import tacit.main as main_mod
import tacit.pipeline as pipeline_mod
import tacit.signals as signals_mod
from tacit.config import Settings, settings
from tacit.runtime_stores import RuntimeStores


def pytest_collection_modifyitems(config, items):
    e2e_root = Path(__file__).parent
    for item in items:
        if Path(str(item.fspath)).is_relative_to(e2e_root):
            item.add_marker(pytest.mark.e2e)


@pytest.fixture
def isolated_learning_runtime(tmp_path, monkeypatch):
    archetypes_path = tmp_path / "curated_archetypes.yaml"
    quarantine_path = tmp_path / "generated_archetypes" / "quarantine"
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="default",
        knowledge_permissions=(
            "knowledge.read,knowledge.review,knowledge.trust,knowledge.reject,knowledge.correct,"
            "knowledge.apply,knowledge.export,knowledge.override"
        ),
        api_auth_enabled=False,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
        learned_archetypes_generation_enabled=True,
        learned_archetypes_automatic_registration_enabled=True,
        learned_archetypes_normal_retrieval_enabled=False,
        learned_archetypes_retrieval_mode="curated_only",
        learned_archetypes_quarantine_path=str(quarantine_path),
    )
    runtime_stores = RuntimeStores(runtime_settings)
    signal_store = runtime_stores.signals()
    history_store = runtime_stores.history()
    feedback_store = runtime_stores.feedback()

    monkeypatch.setattr(signals_mod, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(dashboard_ingest, "get_signal_store", lambda: signal_store)
    monkeypatch.setattr(history_mod, "get_investigation_store", lambda: history_store)
    monkeypatch.setattr(pipeline_mod, "get_investigation_store", lambda: history_store)
    monkeypatch.setattr(feedback_mod, "get_feedback_store", lambda: feedback_store)
    monkeypatch.setattr(main_mod, "get_feedback_store", lambda: feedback_store)
    for field in (
        "knowledge_tenant_id",
        "knowledge_permissions",
        "api_auth_enabled",
        "history_db_path",
        "feedback_db_path",
        "signals_db_path",
        "learned_archetypes_generation_enabled",
        "learned_archetypes_automatic_registration_enabled",
        "learned_archetypes_normal_retrieval_enabled",
        "learned_archetypes_retrieval_mode",
        "learned_archetypes_quarantine_path",
    ):
        monkeypatch.setattr(settings, field, getattr(runtime_settings, field))
    monkeypatch.setattr(cli_mod, "_cli_runtime_stores", lambda: runtime_stores)
    monkeypatch.setattr(main_mod.app.state, "settings", runtime_settings)
    monkeypatch.setattr(main_mod.app.state, "runtime_stores", runtime_stores)
    monkeypatch.setenv("TACIT_ARCHETYPES_PATH", str(archetypes_path))
    templates.reload_archetypes()

    try:
        yield signal_store, history_store, feedback_store, archetypes_path, quarantine_path
    finally:
        monkeypatch.delenv("TACIT_ARCHETYPES_PATH", raising=False)
        templates.reload_archetypes()
