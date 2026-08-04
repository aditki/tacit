from __future__ import annotations

from types import SimpleNamespace

from click.testing import CliRunner

from tacit.cli import cli
from tacit.config import Settings
from tacit.dependencies import build_pipeline_dependencies
from tacit.history import InvestigationStore
from tacit.knowledge.models import KnowledgeScope
from tacit.runtime_stores import RuntimeStores
from tacit.signals.store import SignalStore


def _unexpected_global_store():
    raise AssertionError("configured runtime consulted a process-global store")


def test_configured_runtime_owns_and_reuses_all_stores(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "state" / "history.db"),
        feedback_db_path=str(tmp_path / "state" / "feedback.db"),
        signals_db_path=str(tmp_path / "state" / "signals.db"),
    )
    stores = RuntimeStores(
        runtime_settings,
        history_fallback=_unexpected_global_store,
        feedback_fallback=_unexpected_global_store,
        signal_fallback=_unexpected_global_store,
    )
    dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)

    assert dependencies.history_store_factory() is stores.history()
    assert dependencies.feedback_store_factory() is stores.feedback()
    assert dependencies.signal_store_factory is not None
    assert dependencies.signal_store_factory() is stores.signals()
    assert dependencies.knowledge_service_factory is not None
    assert dependencies.knowledge_service_factory() is stores.knowledge()
    assert stores.history()._db_path == tmp_path / "state" / "history.db"
    assert stores.feedback()._db_path == tmp_path / "state" / "feedback.db"
    assert stores.signals()._db_path == tmp_path / "state" / "signals.db"
    assert stores.knowledge_repository()._db_path == tmp_path / "state" / "signals.db"
    assert stores.knowledge().repository is stores.knowledge_repository()
    assert stores.knowledge()._signal_store() is stores.signals()


def test_default_paths_still_use_runtime_settings_instead_of_global_fallbacks(tmp_path, monkeypatch):
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", tmp_path / "defaults" / "history.db")
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", tmp_path / "defaults" / "feedback.db")
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", tmp_path / "defaults" / "signals.db")
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    stores = RuntimeStores(runtime_settings)

    history = stores.history()
    feedback = stores.feedback()
    signals = stores.signals()

    assert history._settings is runtime_settings
    assert feedback._settings is runtime_settings
    assert signals._settings is runtime_settings
    assert history._db_path == tmp_path / "defaults" / "history.db"
    assert feedback._db_path == tmp_path / "defaults" / "feedback.db"
    assert signals._db_path == tmp_path / "defaults" / "signals.db"


def test_configured_runtime_passes_settings_into_legacy_history_migration(tmp_path):
    db_path = tmp_path / "state" / "history.db"
    db_path.parent.mkdir(parents=True)
    legacy_store = InvestigationStore(db_path)
    investigation_id = legacy_store.start("Legacy tenant")
    with legacy_store._conn() as conn:
        for index_name in (
            "idx_inv_tenant_started",
            "idx_inv_tenant_status_started",
            "idx_inv_tenant_user_started",
            "idx_inv_tenant_dashboard",
        ):
            conn.execute(f"DROP INDEX IF EXISTS {index_name}")
        conn.execute("ALTER TABLE investigations DROP COLUMN tenant_id")

    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(db_path),
        knowledge_tenant_id="tenant-a",
    )
    store = RuntimeStores(runtime_settings).history()

    assert store._settings is runtime_settings
    assert store.get(investigation_id)["tenant_id"] == "tenant-a"


def test_cli_history_uses_the_same_settings_backed_store_owner(tmp_path, monkeypatch):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "cli" / "history.db"),
    )
    monkeypatch.setattr("tacit.config.create_settings", lambda: runtime_settings)
    monkeypatch.setattr("tacit.history.get_investigation_store", _unexpected_global_store)

    result = CliRunner().invoke(cli, ["history", "stats"])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "cli" / "history.db").exists()


def test_injected_signal_store_also_scopes_operational_knowledge(tmp_path):
    injected = SignalStore(tmp_path / "injected-signals.db")
    dependencies = build_pipeline_dependencies(
        Settings(_env_file=None),
        signal_store_factory=lambda: injected,
    )

    assert dependencies.knowledge_service_factory is not None
    service = dependencies.knowledge_service_factory()

    assert service.repository._db_path == injected._db_path
    assert service._signal_store() is injected
    assert dependencies.knowledge_service_factory() is service


def test_pipeline_llm_cache_is_shared_only_within_one_runtime_graph():
    settings_a = Settings(_env_file=None, llm_provider="openai", llm_model="model-a")
    settings_b = Settings(_env_file=None, llm_provider="openai", llm_model="model-b")
    stores_a = RuntimeStores(settings_a)
    first = build_pipeline_dependencies(settings_a, stores=stores_a)
    second = build_pipeline_dependencies(settings_a, stores=stores_a)
    other = build_pipeline_dependencies(settings_b, stores=RuntimeStores(settings_b))

    assert first.llm_cache is second.llm_cache
    assert first.llm_cache is not other.llm_cache


def test_custom_history_factory_is_lazy_and_used_for_correction_validation(tmp_path):
    history_calls: list[tuple[str, int, str | None]] = []

    class ScopedHistory:
        def get_contract(self, investigation_id, revision, *, tenant_id=None):
            history_calls.append((investigation_id, revision, tenant_id))
            return SimpleNamespace(
                investigation=SimpleNamespace(id=investigation_id, revision=revision),
                request=SimpleNamespace(scope=SimpleNamespace(tenant_id=tenant_id)),
                knowledge_usage=[],
            )

    injected_history = ScopedHistory()
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "unused-runtime-history.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        history_store_factory=lambda: injected_history,
    )
    assert dependencies.knowledge_service_factory is not None

    service = dependencies.knowledge_service_factory()
    assert history_calls == []
    assert not (tmp_path / "unused-runtime-history.db").exists()

    correction, _candidate = service.create_correction(
        investigation_id="inv-scoped-history",
        investigation_revision=3,
        correction_type="artifact_quality",
        proposed={
            "subject_ref": "concept:artifact-quality",
            "predicate": "useful_for_investigation",
            "concept_ref": "concept:scoped-history",
        },
        scope=KnowledgeScope(),
        explanation="Validate against the injected history store.",
        created_by="runtime-test",
    )

    assert correction.investigation_id == "inv-scoped-history"
    assert history_calls == [("inv-scoped-history", 3, "default")]
    assert not (tmp_path / "unused-runtime-history.db").exists()


def test_runtime_knowledge_service_does_not_eagerly_initialize_history(tmp_path):
    history_path = tmp_path / "state" / "history.db"
    stores = RuntimeStores(
        Settings(
            _env_file=None,
            history_db_path=str(history_path),
            signals_db_path=str(tmp_path / "state" / "signals.db"),
        )
    )

    service = stores.knowledge()

    assert service.repository._db_path == tmp_path / "state" / "signals.db"
    assert not history_path.exists()


def test_cli_history_replay_requires_and_authorizes_wildcard_tenant(monkeypatch):
    class FakeContract:
        request = SimpleNamespace(scope=SimpleNamespace(tenant_id="tenant-a"))

        def model_dump(self, **_kwargs):
            return {"investigation": {"id": "inv-a"}}

    class FakeHistory:
        replay_calls = 0

        def get_contract(self, investigation_id, revision=None, *, tenant_id=None):
            assert investigation_id == "inv-a"
            return FakeContract() if tenant_id == "tenant-a" else None

        def get(self, investigation_id, *, tenant_id=None):
            assert investigation_id == "inv-a"
            return {"tenant_id": "tenant-a"} if tenant_id == "tenant-a" else None

        def replay_contract(self, investigation_id, revision=None, *, tenant_id=None, **_kwargs):
            assert investigation_id == "inv-a"
            assert tenant_id == "tenant-a"
            self.replay_calls += 1
            return FakeContract()

    class FakeStores:
        settings = Settings(_env_file=None, knowledge_tenant_id="*")

        def __init__(self):
            self.history_store = FakeHistory()

        def history(self):
            return self.history_store

        def knowledge(self):
            raise AssertionError("exact replay should not resolve current knowledge")

    stores = FakeStores()
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: stores)
    runner = CliRunner()

    missing = runner.invoke(cli, ["history", "replay", "inv-a"])
    denied = runner.invoke(cli, ["history", "replay", "inv-a", "--tenant", "tenant-b"])
    allowed = runner.invoke(cli, ["history", "replay", "inv-a", "--tenant", "tenant-a"])

    assert missing.exit_code != 0
    assert "--tenant is required" in missing.output
    assert denied.exit_code != 0
    assert "not found" in denied.output
    assert allowed.exit_code == 0, allowed.output
    assert stores.history_store.replay_calls == 1


def test_doctor_requires_and_propagates_wildcard_tenant(tmp_path, monkeypatch):
    selected: list[str] = []
    config_file = tmp_path / "config.yaml"
    config_file.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("TACIT_CONFIG", str(config_file))

    class Stores:
        settings = Settings(_env_file=None, knowledge_tenant_id="*")

    monkeypatch.setattr("tacit.cli._cli_runtime_stores", Stores)
    monkeypatch.setattr("tacit.cli.CONFIG_FILE", config_file)
    monkeypatch.setattr("tacit.cli._check_archetypes", lambda: True)
    monkeypatch.setattr("tacit.cli._check_grafana", lambda: True)
    monkeypatch.setattr("tacit.cli._check_llm", lambda: True)
    monkeypatch.setattr("tacit.cli._check_datasources", lambda: True)

    def assessment(*, stores, tenant_id):
        selected.append(tenant_id)
        return {
            "inventory": {"dashboards_ingested": 0, "alerts_ingested": 0, "runbooks": 0, "incidents": 0},
            "readiness": {"level": "Low"},
        }

    monkeypatch.setattr("tacit.assess.build_assessment", assessment)
    runner = CliRunner()

    missing = runner.invoke(cli, ["doctor"])
    allowed = runner.invoke(cli, ["doctor", "--tenant", "tenant-a"])

    assert missing.exit_code != 0
    assert "--tenant is required" in missing.output
    assert allowed.exit_code == 0, allowed.output
    assert selected == ["tenant-a"]
