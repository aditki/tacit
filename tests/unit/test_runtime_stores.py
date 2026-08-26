from __future__ import annotations

import inspect
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tacit.cli import cli
from tacit.config import Settings
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies, declare_backend_factory
from tacit.errors import RuntimeOwnershipError
from tacit.feedback import FeedbackStore
from tacit.history import InvestigationStore
from tacit.knowledge.models import KnowledgeScope
from tacit.knowledge.repository import KnowledgeRepository
from tacit.runtime_ownership import (
    RuntimeDatabaseIdentity,
    RuntimeOwnershipDescriptor,
    RuntimeOwnershipMismatchError,
    RuntimeRemoteIdentity,
    declare_runtime_factory,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
)
from tacit.runtime_stores import RuntimeStores
from tacit.signals.store import SignalStore


def _owned_store_factory(factory, runtime_settings: Settings, role: str):
    settings_owner = runtime_descriptor_from_settings(
        runtime_settings,
        component="runtime_store_test_settings",
    )
    database_path = next(item.path for item in settings_owner.databases if item.role == role)
    return declare_runtime_factory(
        factory,
        ownership=runtime_descriptor_for_store(
            component=f"runtime_store_test_{role}_factory",
            runtime_settings=runtime_settings,
            database_role=role,
            database_path=database_path,
        ),
        factory_kind=f"store:{role}",
    )


def _owned_fallback_factory(factory, *, role: str, paths: dict[str, object]):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(paths["history"]),
        feedback_db_path=str(paths["feedback"]),
        signals_db_path=str(paths["signals"]),
    )
    return declare_runtime_factory(
        factory,
        ownership=runtime_descriptor_for_store(
            component=f"runtime_store_test_{role}_fallback",
            runtime_settings=runtime_settings,
            database_role=role,
            database_path=paths[role],
        ),
        factory_kind=f"store:{role}",
    )


def _unexpected_global_store():
    raise AssertionError("configured runtime consulted a process-global store")


class _DescriptorOnlyStore:
    """Compatibility result that exposes only the public ownership contract."""

    def __init__(self, runtime_ownership):
        self.runtime_ownership = runtime_ownership
        self.unexpected_accesses: list[str] = []
        self.bootstrap_calls = 0

    def load_from_yaml(self, *, only_if_changed: bool):
        assert only_if_changed is True
        self.bootstrap_calls += 1

    def __getattr__(self, name: str):
        self.unexpected_accesses.append(name)
        raise AssertionError(f"store was consumed before ownership validation: {name}")


_STORE_ROLE_CASES = (
    ("history", "history", "history_fallback"),
    ("feedback", "feedback", "feedback_fallback"),
    ("signals", "signals", "signal_fallback"),
)
_STORE_CONSTRUCTOR_TARGETS = {
    "history": "tacit.history.InvestigationStore",
    "feedback": "tacit.feedback.FeedbackStore",
    "signals": "tacit.signals.SignalStore",
}


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


def test_dependency_bundles_share_their_runtime_admission_controller(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        pipeline_max_concurrent=2,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )
    stores = RuntimeStores(runtime_settings)

    first = build_pipeline_dependencies(runtime_settings, stores=stores)
    second = build_pipeline_dependencies(runtime_settings, stores=stores)

    assert first.pipeline_admission is second.pipeline_admission
    assert first.pipeline_admission.limit == 2


def test_wildcard_runtime_admission_reserves_queue_capacity_per_tenant(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        api_auth_enabled=True,
        knowledge_tenant_id="*",
        knowledge_tenant_api_keys={"tenant-a": "secret-a", "tenant-b": "secret-b"},
        pipeline_max_queued=40,
        pipeline_max_queued_per_tenant=7,
        pipeline_max_concurrent=5,
        pipeline_max_concurrent_per_tenant=3,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )

    admission = RuntimeStores(runtime_settings).pipeline_admission()

    assert admission.max_queued == 40
    assert admission.max_queued_per_partition == 7
    assert admission.max_in_flight_per_partition == 3


def test_pinned_runtime_uses_the_global_queue_limit_for_its_single_tenant(tmp_path):
    runtime_settings = Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        pipeline_max_queued=40,
        pipeline_max_queued_per_tenant=7,
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
    )

    admission = RuntimeStores(runtime_settings).pipeline_admission()

    assert admission.max_queued == 40
    assert admission.max_queued_per_partition == 40
    assert admission.max_in_flight_per_partition == runtime_settings.pipeline_max_concurrent


def test_pipeline_builder_does_not_accept_an_ownerless_admission_controller():
    assert "pipeline_admission" not in inspect.signature(build_pipeline_dependencies).parameters


def test_direct_dependency_construction_requires_deliberate_isolated_admission():
    runtime_settings = Settings(_env_file=None)
    values = {
        "settings": runtime_settings,
        "backend_factory": declare_backend_factory(
            lambda: [],
            runtime_settings=runtime_settings,
            component="isolated_runtime_store_test_backends",
        ),
        "history_store_factory": _owned_store_factory(lambda: object(), runtime_settings, "history"),
        "feedback_store_factory": _owned_store_factory(lambda: object(), runtime_settings, "feedback"),
        "llm_cache": {},
        "cache_key_factory": lambda *parts: ":".join(parts),
    }

    with pytest.raises(ValueError, match="runtime-owned pipeline admission"):
        PipelineDependencies(**values)

    isolated = PipelineDependencies.isolated(**values)

    assert isolated.pipeline_admission.limit == runtime_settings.pipeline_max_concurrent


def test_runtime_owner_revalidates_copied_admission_settings_before_construction():
    invalid = Settings(_env_file=None).model_copy(update={"pipeline_max_concurrent": 0})

    with pytest.raises(ValueError, match="pipeline_max_concurrent"):
        RuntimeStores(invalid).pipeline_admission()


def test_public_default_dependencies_share_process_admission_controller():
    first = PipelineDependencies.defaults()
    second = PipelineDependencies.defaults()

    assert first.pipeline_admission is second.pipeline_admission


def test_pipeline_dependency_construction_rejects_split_runtime_before_resource_initialization():
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")

    class SplitRuntimeStores:
        settings = runtime_settings.model_copy(update={"knowledge_tenant_id": "tenant-b"})

        def llm_cache(self):
            raise AssertionError("resources initialized before runtime ownership validation")

    with pytest.raises(ValueError, match="runtime settings must match"):
        build_pipeline_dependencies(runtime_settings, stores=SplitRuntimeStores())  # type: ignore[arg-type]


def test_runtime_stores_rejects_ownerless_used_fallback_before_invocation(tmp_path, monkeypatch):
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", tmp_path / "signals.db")
    calls = 0

    def ownerless_fallback():
        nonlocal calls
        calls += 1
        raise AssertionError("ownerless fallback was invoked")

    with pytest.raises(RuntimeOwnershipError, match="declared runtime owner"):
        RuntimeStores(Settings(_env_file=None), signal_fallback=ownerless_fallback)

    assert calls == 0
    assert not (tmp_path / "signals.db").exists()


def test_runtime_stores_rejects_foreign_fallback_declaration_before_invocation(tmp_path, monkeypatch):
    active_path = tmp_path / "active-signals.db"
    foreign_path = tmp_path / "foreign-signals.db"
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", active_path)
    active = Settings(_env_file=None)
    foreign = Settings(_env_file=None, signals_db_path=str(foreign_path))
    calls = 0

    def foreign_fallback():
        nonlocal calls
        calls += 1
        foreign_path.touch()
        raise AssertionError("foreign fallback was invoked")

    declared = declare_runtime_factory(
        foreign_fallback,
        ownership=runtime_descriptor_for_store(
            component="foreign_runtime_signal_fallback",
            runtime_settings=foreign,
            database_role="signals",
            database_path=foreign_path,
        ),
        factory_kind="store:signals",
    )

    with pytest.raises(RuntimeOwnershipError, match="runtime ownership mismatch"):
        RuntimeStores(active, signal_fallback=declared)

    assert calls == 0
    assert not foreign_path.exists()


def test_default_paths_still_use_runtime_settings_instead_of_global_fallbacks(tmp_path, monkeypatch):
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", tmp_path / "defaults" / "history.db")
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", tmp_path / "defaults" / "feedback.db")
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", tmp_path / "defaults" / "signals.db")
    runtime_settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    stores = RuntimeStores(runtime_settings)

    history = stores.history()
    feedback = stores.feedback()
    signals = stores.signals()

    assert history.runtime_settings.knowledge_tenant_id == runtime_settings.knowledge_tenant_id
    assert feedback.runtime_settings.knowledge_tenant_id == runtime_settings.knowledge_tenant_id
    assert signals.runtime_settings.knowledge_tenant_id == runtime_settings.knowledge_tenant_id
    assert history.runtime_settings is not runtime_settings
    assert feedback.runtime_settings is not runtime_settings
    assert signals.runtime_settings is not runtime_settings
    assert history._db_path == tmp_path / "defaults" / "history.db"
    assert feedback._db_path == tmp_path / "defaults" / "feedback.db"
    assert signals._db_path == tmp_path / "defaults" / "signals.db"


@pytest.mark.parametrize(
    "colliding_roles",
    [
        ("history", "feedback"),
        ("history", "signals"),
        ("feedback", "signals"),
    ],
)
def test_runtime_stores_rejects_effective_fallback_path_collisions_before_creation(
    tmp_path,
    monkeypatch,
    colliding_roles,
):
    paths = {
        "history": tmp_path / "must-not-exist" / "history.db",
        "feedback": tmp_path / "must-not-exist" / "feedback.db",
        "signals": tmp_path / "must-not-exist" / "signals.db",
    }
    first_role, second_role = colliding_roles
    paths[second_role] = paths[first_role].parent / "." / paths[first_role].name
    targets = {
        "history": "tacit.history._DEFAULT_DB_PATH",
        "feedback": "tacit.feedback._DEFAULT_DB_PATH",
        "signals": "tacit.signals.store._DEFAULT_DB_PATH",
    }
    for role, target in targets.items():
        monkeypatch.setattr(target, paths[role])

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        RuntimeStores(Settings(_env_file=None))

    assert not (tmp_path / "must-not-exist").exists()


def test_runtime_stores_revalidates_hard_links_before_store_schema_initialization(tmp_path):
    history_path = tmp_path / "state" / "history.db"
    feedback_path = tmp_path / "state" / "feedback.db"
    signals_path = tmp_path / "state" / "signals.db"
    stores = RuntimeStores(
        Settings(
            _env_file=None,
            history_db_path=str(history_path),
            feedback_db_path=str(feedback_path),
            signals_db_path=str(signals_path),
        )
    )
    history_path.parent.mkdir(parents=True)
    history_path.touch()
    feedback_path.hardlink_to(history_path)

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        stores.signals()

    assert history_path.stat().st_size == 0
    assert feedback_path.stat().st_size == 0
    assert signals_path.exists() is False


def test_runtime_stores_revalidates_file_identity_after_target_open_before_schema(
    tmp_path,
    monkeypatch,
):
    history_path = tmp_path / "state" / "history.db"
    feedback_path = tmp_path / "state" / "feedback.db"
    signals_path = tmp_path / "state" / "signals.db"
    stores = RuntimeStores(
        Settings(
            _env_file=None,
            history_db_path=str(history_path),
            feedback_db_path=str(feedback_path),
            signals_db_path=str(signals_path),
        )
    )
    history_path.parent.mkdir(parents=True)
    history_path.touch()
    constructor_called = False

    def late_collision(path):
        if path == signals_path:
            signals_path.hardlink_to(history_path)
        return path

    class UnexpectedSignalStore:
        def __init__(self, *_args, **_kwargs):
            nonlocal constructor_called
            constructor_called = True

    monkeypatch.setattr(stores, "_configured_path", late_collision)
    monkeypatch.setattr("tacit.signals.SignalStore", UnexpectedSignalStore)

    with pytest.raises(ValueError, match="SQLite database roles must use distinct files"):
        stores.signals()

    assert constructor_called is False
    assert history_path.stat().st_ino == signals_path.stat().st_ino


def test_runtime_store_rejects_parent_rebinding_before_first_open(
    tmp_path,
):
    configured_parent = tmp_path / "configured-state"
    configured_parent.mkdir()
    history_path = configured_parent / "history.db"
    stores = RuntimeStores(
        Settings(
            _env_file=None,
            history_db_path=str(history_path),
            feedback_db_path=str(tmp_path / "feedback.db"),
            signals_db_path=str(tmp_path / "signals.db"),
        )
    )
    original_parent = tmp_path / "original-state"
    configured_parent.rename(original_parent)
    redirected_parent = tmp_path / "redirected-state"
    redirected_parent.mkdir()
    configured_parent.symlink_to(redirected_parent, target_is_directory=True)

    with pytest.raises(RuntimeOwnershipError, match="symbolic link"):
        stores.history()

    assert not (redirected_parent / "history.db").exists()


@pytest.mark.parametrize(
    ("store_factory", "schema_table"),
    [
        (InvestigationStore, "investigations"),
        (FeedbackStore, "dashboard_provenance"),
        (SignalStore, "signal_types"),
        (KnowledgeRepository, "knowledge_candidates"),
    ],
)
def test_direct_stores_reject_final_symlinks_before_schema(
    tmp_path,
    store_factory,
    schema_table,
):
    redirected_path = tmp_path / f"redirected-{schema_table}.db"
    sqlite3.connect(redirected_path).close()
    configured_path = tmp_path / f"configured-{schema_table}.db"
    configured_path.symlink_to(redirected_path)

    with pytest.raises(RuntimeOwnershipError, match="symbolic link"):
        store_factory(configured_path)

    with sqlite3.connect(redirected_path) as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (schema_table,),
            ).fetchone()
            is None
        )


@pytest.mark.parametrize(
    ("store_factory", "schema_table"),
    [
        (InvestigationStore, "investigations"),
        (FeedbackStore, "dashboard_provenance"),
        (SignalStore, "signal_types"),
        (KnowledgeRepository, "knowledge_candidates"),
    ],
)
def test_direct_stores_reject_symlinked_path_components_before_schema(
    tmp_path,
    store_factory,
    schema_table,
):
    redirected_directory = tmp_path / "redirected"
    redirected_directory.mkdir()
    configured_directory = tmp_path / "configured"
    configured_directory.symlink_to(redirected_directory, target_is_directory=True)
    configured_path = configured_directory / f"{schema_table}.db"

    with pytest.raises(RuntimeOwnershipError, match="symbolic link"):
        store_factory(configured_path)

    redirected_path = redirected_directory / configured_path.name
    if redirected_path.exists():
        with sqlite3.connect(redirected_path) as conn:
            assert (
                conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (schema_table,),
                ).fetchone()
                is None
            )


def test_store_connection_rejects_same_role_path_rebinding_after_initialization(tmp_path):
    primary_path = tmp_path / "primary-history.db"
    replacement_path = tmp_path / "replacement-history.db"
    store = InvestigationStore(primary_path)
    InvestigationStore(replacement_path)

    primary_path.unlink()
    primary_path.hardlink_to(replacement_path)

    with pytest.raises(RuntimeOwnershipError, match="multiple hard links"):
        with store._conn():
            pass


def test_knowledge_repository_rejects_rebound_external_transaction(tmp_path):
    signal_path = tmp_path / "signals.db"
    replacement_path = tmp_path / "replacement-signals.db"
    SignalStore(signal_path)
    repository = KnowledgeRepository(signal_path)
    SignalStore(replacement_path)

    signal_path.unlink()
    signal_path.hardlink_to(replacement_path)

    import sqlite3

    with sqlite3.connect(signal_path) as conn:
        with pytest.raises(RuntimeOwnershipError, match="database identity"):
            with repository.bind_transaction_connection(conn):
                pass


def test_knowledge_repository_rolls_back_role_and_schema_together(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "signals.db"
    import tacit.knowledge.repository as repository_module

    def fail_after_schema_write(conn, _script):
        conn.execute("CREATE TABLE partial_knowledge_schema (id INTEGER PRIMARY KEY)")
        raise RuntimeError("injected schema failure")

    monkeypatch.setattr(
        repository_module,
        "_execute_schema_statements",
        fail_after_schema_write,
    )

    with pytest.raises(RuntimeError, match="injected schema failure"):
        KnowledgeRepository(database_path)

    with sqlite3.connect(database_path) as conn:
        assert (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='partial_knowledge_schema'"
            ).fetchone()
            is None
        )
        assert (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='tacit_runtime_database_identity'"
            ).fetchone()
            is None
        )


def test_connection_role_identity_rejects_cross_role_reuse_before_schema(tmp_path):
    database_path = tmp_path / "claimed-signals.db"
    SignalStore(database_path)

    with pytest.raises(RuntimeOwnershipError, match="store role"):
        InvestigationStore(database_path)

    with sqlite3.connect(database_path) as conn:
        assert (
            conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='investigations'").fetchone() is None
        )


@pytest.mark.parametrize(("role", "accessor", "fallback_name"), _STORE_ROLE_CASES)
@pytest.mark.parametrize("mismatch_kind", ("path", "role"))
def test_runtime_stores_rejects_mismatched_realized_compatibility_store(
    tmp_path,
    monkeypatch,
    role,
    accessor,
    fallback_name,
    mismatch_kind,
):
    expected_paths = {
        "history": tmp_path / "advertised" / "history.db",
        "feedback": tmp_path / "advertised" / "feedback.db",
        "signals": tmp_path / "advertised" / "signals.db",
    }
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", expected_paths["history"])
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", expected_paths["feedback"])
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", expected_paths["signals"])
    candidate_holder = {}
    factory_calls = 0

    def compatibility_factory():
        nonlocal factory_calls
        factory_calls += 1
        return candidate_holder["store"]

    stores = RuntimeStores(
        Settings(_env_file=None),
        **{
            fallback_name: _owned_fallback_factory(
                compatibility_factory,
                role=role,
                paths=expected_paths,
            )
        },
    )
    actual_descriptor = replace(
        stores.runtime_ownership,
        component=f"preinitialized_global_{role}",
        databases=(
            RuntimeDatabaseIdentity(
                role=role if mismatch_kind == "path" else "persistence",
                path=(
                    tmp_path / "already-initialized" / f"{role}.db" if mismatch_kind == "path" else expected_paths[role]
                ),
            ),
        ),
    )
    candidate = _DescriptorOnlyStore(actual_descriptor)
    candidate_holder["store"] = candidate

    with pytest.raises(RuntimeOwnershipMismatchError, match="database"):
        getattr(stores, accessor)()

    assert factory_calls == 1
    assert candidate.unexpected_accesses == []
    assert not (tmp_path / "advertised").exists()
    assert not (tmp_path / "already-initialized").exists()


@pytest.mark.parametrize(("role", "accessor", "fallback_name"), _STORE_ROLE_CASES)
def test_runtime_stores_accepts_same_owner_compatibility_store(
    tmp_path,
    monkeypatch,
    role,
    accessor,
    fallback_name,
):
    expected_paths = {
        "history": tmp_path / "advertised" / "history.db",
        "feedback": tmp_path / "advertised" / "feedback.db",
        "signals": tmp_path / "advertised" / "signals.db",
    }
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", expected_paths["history"])
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", expected_paths["feedback"])
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", expected_paths["signals"])
    candidate_holder = {}

    stores = RuntimeStores(
        Settings(_env_file=None),
        **{
            fallback_name: _owned_fallback_factory(
                lambda: candidate_holder["store"],
                role=role,
                paths=expected_paths,
            )
        },
    )
    expected_database = next(database for database in stores.runtime_ownership.databases if database.role == role)
    candidate = _DescriptorOnlyStore(
        replace(
            stores.runtime_ownership,
            component=f"preinitialized_global_{role}",
            databases=(expected_database,),
        )
    )
    candidate_holder["store"] = candidate

    assert getattr(stores, accessor)() is candidate
    assert candidate.unexpected_accesses == []


@pytest.mark.parametrize(("role", "accessor", "fallback_name"), _STORE_ROLE_CASES)
def test_runtime_stores_rejects_database_only_compatibility_store(
    tmp_path,
    monkeypatch,
    role,
    accessor,
    fallback_name,
):
    expected_paths = {
        "history": tmp_path / "advertised" / "history.db",
        "feedback": tmp_path / "advertised" / "feedback.db",
        "signals": tmp_path / "advertised" / "signals.db",
    }
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", expected_paths["history"])
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", expected_paths["feedback"])
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", expected_paths["signals"])
    candidate_holder = {}
    stores = RuntimeStores(
        Settings(_env_file=None),
        **{
            fallback_name: _owned_fallback_factory(
                lambda: candidate_holder["store"],
                role=role,
                paths=expected_paths,
            )
        },
    )
    candidate = _DescriptorOnlyStore(
        RuntimeOwnershipDescriptor(
            component=f"database_only_{role}",
            databases=(RuntimeDatabaseIdentity(role=role, path=expected_paths[role]),),
        )
    )
    candidate_holder["store"] = candidate

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        getattr(stores, accessor)()

    assert {"settings", "tenant"} <= exc_info.value.dimensions
    assert candidate.bootstrap_calls == 0
    assert candidate.unexpected_accesses == []


@pytest.mark.parametrize("mismatch_kind", ["database", "remote"])
def test_signal_store_rejects_conflicting_complete_owner_before_bootstrap(
    tmp_path,
    monkeypatch,
    mismatch_kind,
):
    paths = {
        "history": tmp_path / "advertised" / "history.db",
        "feedback": tmp_path / "advertised" / "feedback.db",
        "signals": tmp_path / "advertised" / "signals.db",
    }
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", paths["history"])
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", paths["feedback"])
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", paths["signals"])
    candidate_holder = {}
    stores = RuntimeStores(
        Settings(_env_file=None),
        signal_fallback=_owned_fallback_factory(
            lambda: candidate_holder["store"],
            role="signals",
            paths=paths,
        ),
    )
    expected_database = next(database for database in stores.runtime_ownership.databases if database.role == "signals")
    updates = {"databases": (RuntimeDatabaseIdentity(role="signals", path=tmp_path / "conflicting" / "signals.db"),)}
    if mismatch_kind == "remote":
        updates = {
            "databases": (expected_database,),
            "remotes": (
                RuntimeRemoteIdentity(
                    provider="grafana",
                    endpoint="https://different.example.test",
                ),
            ),
        }
    candidate = _DescriptorOnlyStore(
        replace(
            stores.runtime_ownership,
            component="conflicting_signal_store",
            **updates,
        )
    )
    candidate_holder["store"] = candidate

    with pytest.raises(RuntimeOwnershipMismatchError) as exc_info:
        stores.signals()

    expected_dimension = "database" if mismatch_kind == "database" else "endpoint"
    assert expected_dimension in exc_info.value.dimensions
    assert candidate.bootstrap_calls == 0
    assert candidate.unexpected_accesses == []


@pytest.mark.parametrize(("role", "accessor", "_fallback_name"), _STORE_ROLE_CASES)
def test_runtime_stores_validates_newly_constructed_store_before_consumption(
    tmp_path,
    monkeypatch,
    role,
    accessor,
    _fallback_name,
):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "advertised" / "history.db"),
        feedback_db_path=str(tmp_path / "advertised" / "feedback.db"),
        signals_db_path=str(tmp_path / "advertised" / "signals.db"),
    )
    stores = RuntimeStores(runtime_settings)
    candidate = _DescriptorOnlyStore(
        replace(
            stores.runtime_ownership,
            component=f"constructed_{role}",
            databases=(
                RuntimeDatabaseIdentity(
                    role=role,
                    path=tmp_path / "wrong-owner" / f"{role}.db",
                ),
            ),
        )
    )
    constructor_calls = 0

    def construct_store(*_args, **_kwargs):
        nonlocal constructor_calls
        constructor_calls += 1
        return candidate

    monkeypatch.setattr(_STORE_CONSTRUCTOR_TARGETS[role], construct_store)

    with pytest.raises(RuntimeOwnershipMismatchError, match="database"):
        getattr(stores, accessor)()

    assert constructor_calls == 1
    assert candidate.bootstrap_calls == 0
    assert candidate.unexpected_accesses == []
    assert not (tmp_path / "wrong-owner").exists()


@pytest.mark.parametrize(("role", "accessor", "_fallback_name"), _STORE_ROLE_CASES)
def test_runtime_stores_revalidates_cached_store_before_every_return(
    tmp_path,
    monkeypatch,
    role,
    accessor,
    _fallback_name,
):
    runtime_settings = Settings(
        _env_file=None,
        history_db_path=str(tmp_path / "advertised" / "history.db"),
        feedback_db_path=str(tmp_path / "advertised" / "feedback.db"),
        signals_db_path=str(tmp_path / "advertised" / "signals.db"),
    )
    stores = RuntimeStores(runtime_settings)
    expected_database = next(database for database in stores.runtime_ownership.databases if database.role == role)
    candidate = _DescriptorOnlyStore(
        replace(
            stores.runtime_ownership,
            component=f"constructed_{role}",
            databases=(expected_database,),
        )
    )
    constructor_calls = 0

    def construct_store(*_args, **_kwargs):
        nonlocal constructor_calls
        constructor_calls += 1
        return candidate

    monkeypatch.setattr(_STORE_CONSTRUCTOR_TARGETS[role], construct_store)

    assert getattr(stores, accessor)() is candidate
    candidate.runtime_ownership = replace(
        candidate.runtime_ownership,
        databases=(
            RuntimeDatabaseIdentity(
                role=role,
                path=tmp_path / "changed-owner" / f"{role}.db",
            ),
        ),
    )

    with pytest.raises(RuntimeOwnershipMismatchError, match="database"):
        getattr(stores, accessor)()

    assert constructor_calls == 1
    assert candidate.bootstrap_calls == (1 if role == "signals" else 0)
    assert candidate.unexpected_accesses == []


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

    assert store.runtime_settings.knowledge_tenant_id == runtime_settings.knowledge_tenant_id
    assert store.runtime_settings.history_db_path == str(db_path)
    assert store.runtime_settings is not runtime_settings
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
    signal_path = tmp_path / "injected-signals.db"
    runtime_settings = Settings(_env_file=None, signals_db_path=str(signal_path))
    injected = SignalStore(
        signal_path,
        runtime_settings=runtime_settings,
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        signal_store_factory=_owned_store_factory(lambda: injected, runtime_settings, "signals"),
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
    injected_history.runtime_ownership = runtime_descriptor_for_store(
        component="scoped_history",
        runtime_settings=runtime_settings,
        database_role="history",
        database_path=runtime_settings.history_db_path,
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=RuntimeStores(runtime_settings),
        history_store_factory=_owned_store_factory(lambda: injected_history, runtime_settings, "history"),
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
        settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)

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
        settings = Settings(_env_file=None, knowledge_tenant_id="*", api_auth_enabled=True)

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


def test_cli_knowledge_review_redacts_os_permission_errors(monkeypatch):
    runtime_settings = Settings(_env_file=None)

    class FailingKnowledgeService:
        def review_candidate(self, *_args, **_kwargs):
            raise PermissionError("/private/authority/path-canary")

    class Stores:
        settings = runtime_settings

        def knowledge(self):
            return FailingKnowledgeService()

    monkeypatch.setattr("tacit.cli._load_env", lambda: None)
    monkeypatch.setattr("tacit.cli._cli_runtime_stores", lambda: Stores())

    result = CliRunner().invoke(
        cli,
        [
            "knowledge",
            "review",
            "candidate-a",
            "--approve",
            "--reviewer",
            "operator",
        ],
    )

    assert result.exit_code != 0
    assert "local I/O error" in result.output
    assert "path-canary" not in result.output
