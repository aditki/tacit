from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import tacit.signals as signals
import tacit.sqlite_identity as sqlite_identity
from tacit.config import Settings
from tacit.dashboard_ingest import service as dashboard_service
from tacit.dependencies import (
    build_pipeline_dependencies,
    create_scoped_knowledge_service,
    declare_backend_factory,
    resolve_knowledge_service,
)
from tacit.errors import RuntimeOwnershipError
from tacit.knowledge import service as knowledge_service_module
from tacit.knowledge.repository import KnowledgeRepository
from tacit.knowledge.service import KnowledgeService
from tacit.runtime_ownership import declare_runtime_factory, runtime_descriptor_for_store
from tacit.runtime_stores import RuntimeStores
from tacit.signals import store as signal_store_module
from tacit.signals.store import SignalStore
from tacit.sqlite_identity import (
    SQLiteIdentityError,
    SQLiteIdentityRejectionReason,
    require_sqlite_authority_snapshot_capacity,
)


def _settings(tmp_path: Path, *, snapshot_max_bytes: int = 64 * 1024 * 1024) -> Settings:
    return Settings(
        _env_file=None,
        knowledge_tenant_id="tenant-a",
        history_db_path=str(tmp_path / "history.db"),
        feedback_db_path=str(tmp_path / "feedback.db"),
        signals_db_path=str(tmp_path / "signals.db"),
        sqlite_snapshot_max_bytes=snapshot_max_bytes,
    )


def _schema(path: Path) -> list[tuple[str, str, str]]:
    with sqlite3.connect(path) as connection:
        return connection.execute("""SELECT type, name, COALESCE(sql, '')
               FROM sqlite_master
               WHERE name NOT LIKE 'sqlite_%'
               ORDER BY type, name""").fetchall()


def test_scoped_dependency_service_reuses_one_exact_signals_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    seed = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)
    KnowledgeRepository(runtime_settings=settings, signal_store=seed)

    copied_sources: list[Path] = []
    original_copy = sqlite_identity._copy_snapshot_component

    def observed_copy(source: Path, destination: Path, *, budget, **kwargs) -> None:
        copied_sources.append(source)
        original_copy(source, destination, budget=budget, **kwargs)

    monkeypatch.setattr(sqlite_identity, "_copy_snapshot_component", observed_copy)
    signal_store = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)

    service = create_scoped_knowledge_service(signal_store, runtime_settings=settings)

    assert service.signal_store is signal_store
    assert service.repository.sqlite_readiness_admission is signal_store.sqlite_readiness_admission
    assert copied_sources.count(Path(settings.signals_db_path)) == 1


def test_direct_service_from_signal_store_reuses_exact_admission(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    signal_store = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)

    service = KnowledgeService(signal_store=signal_store, runtime_settings=settings)

    assert service.signal_store is signal_store
    assert service.repository.sqlite_readiness_admission is signal_store.sqlite_readiness_admission


def test_direct_service_from_settings_constructs_one_shared_owner(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    service = KnowledgeService(runtime_settings=settings)

    assert service.repository.sqlite_readiness_admission is service.signal_store.sqlite_readiness_admission


def test_process_global_service_facade_reuses_the_global_signal_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path)
    signal_path = Path(runtime_settings.signals_db_path)
    monkeypatch.setattr(signals, "_DEFAULT_DB_PATH", signal_path)
    monkeypatch.setattr(signals, "_store", None)
    monkeypatch.setattr(signal_store_module, "_DEFAULT_DB_PATH", signal_path)
    monkeypatch.setattr(signal_store_module, "_store", None)
    monkeypatch.setattr(signal_store_module, "settings", runtime_settings)

    service = knowledge_service_module.get_knowledge_service()
    signal_store = signals.get_signal_store()

    assert service.signal_store is signal_store
    assert service.repository.sqlite_readiness_admission is signal_store.sqlite_readiness_admission


def test_direct_service_rejects_an_independently_admitted_repository(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    repository = KnowledgeRepository(Path(settings.signals_db_path), runtime_settings=settings)
    signal_store = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)

    with pytest.raises(RuntimeOwnershipError, match="shared SQLite readiness admission"):
        KnowledgeService(repository, signal_store=signal_store, runtime_settings=settings)


def test_repository_only_service_fails_closed_before_opening_a_signal_owner(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    repository = KnowledgeRepository(Path(settings.signals_db_path), runtime_settings=settings)
    service = KnowledgeService(repository, runtime_settings=settings)

    with pytest.raises(RuntimeOwnershipError, match="injected shared Signals admission owner"):
        _ = service.signal_store


def test_runtime_stores_app_scope_reuses_exact_admission(tmp_path: Path) -> None:
    stores = RuntimeStores(_settings(tmp_path))

    signal_store = stores.signals()
    service = stores.knowledge()

    assert service.signal_store is signal_store
    assert service.repository.sqlite_readiness_admission is signal_store.sqlite_readiness_admission


def test_runtime_stores_caches_one_fallback_signal_owner_for_knowledge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    history_path = tmp_path / "fallback-history.db"
    feedback_path = tmp_path / "fallback-feedback.db"
    signal_path = tmp_path / "fallback-signals.db"
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", history_path)
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", feedback_path)
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", signal_path)
    settings = Settings(_env_file=None, knowledge_tenant_id="tenant-a")
    owner_settings = settings.model_copy(
        update={
            "history_db_path": str(history_path),
            "feedback_db_path": str(feedback_path),
            "signals_db_path": str(signal_path),
        }
    )
    calls = 0

    def build_signal_store() -> SignalStore:
        nonlocal calls
        calls += 1
        return SignalStore(signal_path, runtime_settings=owner_settings)

    fallback = declare_runtime_factory(
        build_signal_store,
        ownership=runtime_descriptor_for_store(
            component="wave_ib_signal_fallback",
            runtime_settings=owner_settings,
            database_role="signals",
            database_path=signal_path,
        ),
        factory_kind="store:signals",
    )
    stores = RuntimeStores(settings, signal_fallback=fallback)

    service = stores.knowledge()

    assert calls == 1
    assert service.signal_store is stores.signals()
    assert service.repository.sqlite_readiness_admission is service.signal_store.sqlite_readiness_admission


def test_pipeline_custom_signal_factory_is_realized_once_for_signal_and_knowledge(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "llm_provider": "ollama",
            "llm_api_base": "http://127.0.0.1:11434",
        }
    )
    stores = RuntimeStores(settings)
    calls = 0

    def build_signal_store() -> SignalStore:
        nonlocal calls
        calls += 1
        return SignalStore(Path(settings.signals_db_path), runtime_settings=settings)

    signal_factory = declare_runtime_factory(
        build_signal_store,
        ownership=runtime_descriptor_for_store(
            component="wave_ib_pipeline_signal_factory",
            runtime_settings=settings,
            database_role="signals",
            database_path=settings.signals_db_path,
        ),
        factory_kind="store:signals",
    )
    dependencies = build_pipeline_dependencies(
        settings,
        stores=stores,
        signal_store_factory=signal_factory,
        backend_factory=declare_backend_factory(
            lambda: [],
            runtime_settings=settings,
            component="wave_ib_backend_factory",
        ),
    )
    assert dependencies.signal_store_factory is not None
    assert dependencies.knowledge_service_factory is not None

    signal_store = dependencies.signal_store_factory()
    service = dependencies.knowledge_service_factory()
    resolved = resolve_knowledge_service(dependencies, signal_store=signal_store)

    assert calls == 1
    assert resolved is service
    assert service.signal_store is signal_store
    assert service.repository.sqlite_readiness_admission is signal_store.sqlite_readiness_admission


def test_pipeline_rejects_custom_knowledge_factory_with_a_split_signal_admission(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "llm_provider": "ollama",
            "llm_api_base": "http://127.0.0.1:11434",
        }
    )
    stores = RuntimeStores(settings)

    signal_factory = declare_runtime_factory(
        lambda: SignalStore(Path(settings.signals_db_path), runtime_settings=settings),
        ownership=runtime_descriptor_for_store(
            component="wave_ib_custom_signal_factory",
            runtime_settings=settings,
            database_role="signals",
            database_path=settings.signals_db_path,
        ),
        factory_kind="store:signals",
    )
    knowledge_factory = declare_runtime_factory(
        lambda: KnowledgeService(runtime_settings=settings),
        ownership=runtime_descriptor_for_store(
            component="wave_ib_split_knowledge_factory",
            runtime_settings=settings,
            database_role="signals",
            database_path=settings.signals_db_path,
        ),
        factory_kind="knowledge:signals",
    )
    dependencies = build_pipeline_dependencies(
        settings,
        stores=stores,
        signal_store_factory=signal_factory,
        knowledge_service_factory=knowledge_factory,
        backend_factory=declare_backend_factory(
            lambda: [],
            runtime_settings=settings,
            component="wave_ib_split_backend_factory",
        ),
    )
    assert dependencies.knowledge_service_factory is not None

    with pytest.raises(RuntimeOwnershipError, match="exact shared SQLite readiness admission"):
        dependencies.knowledge_service_factory()


def test_pipeline_accepts_custom_factories_that_share_one_signal_admission(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path).model_copy(
        update={
            "llm_provider": "ollama",
            "llm_api_base": "http://127.0.0.1:11434",
        }
    )
    stores = RuntimeStores(settings)
    signal_store = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)
    knowledge_service = KnowledgeService(signal_store=signal_store, runtime_settings=settings)
    ownership = runtime_descriptor_for_store(
        component="wave_ib_shared_custom_factories",
        runtime_settings=settings,
        database_role="signals",
        database_path=settings.signals_db_path,
    )
    signal_factory = declare_runtime_factory(
        lambda: signal_store,
        ownership=ownership,
        factory_kind="store:signals",
    )
    knowledge_factory = declare_runtime_factory(
        lambda: knowledge_service,
        ownership=ownership,
        factory_kind="knowledge:signals",
    )
    dependencies = build_pipeline_dependencies(
        settings,
        stores=stores,
        signal_store_factory=signal_factory,
        knowledge_service_factory=knowledge_factory,
        backend_factory=declare_backend_factory(
            lambda: [],
            runtime_settings=settings,
            component="wave_ib_shared_backend_factory",
        ),
    )
    assert dependencies.signal_store_factory is not None
    assert dependencies.knowledge_service_factory is not None

    assert dependencies.signal_store_factory() is signal_store
    assert dependencies.knowledge_service_factory() is knowledge_service
    assert knowledge_service.repository.sqlite_readiness_admission is signal_store.sqlite_readiness_admission


def test_dashboard_repository_fallbacks_consume_the_store_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    signal_store = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)
    observed_stores: list[object | None] = []
    original_init = KnowledgeRepository.__init__

    def observed_init(self, *args, **kwargs) -> None:
        observed_stores.append(kwargs.get("signal_store"))
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(KnowledgeRepository, "__init__", observed_init)

    assert (
        dashboard_service._existing_governed_candidate_ids(
            store=signal_store,
            tenant_id="tenant-a",
            source_ref="dashboard:test",
            active_pairs=set(),
        )
        == set()
    )
    assert (
        dashboard_service._active_governed_signal_mapping_ref(
            store=signal_store,
            candidate_id="missing",
            tenant_id="tenant-a",
        )
        == ""
    )
    assert observed_stores == [signal_store, signal_store]


def test_dashboard_service_only_fallback_returns_the_owned_signal_store(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    signal_store = SignalStore(Path(settings.signals_db_path), runtime_settings=settings)
    service = create_scoped_knowledge_service(signal_store, runtime_settings=settings)

    resolved = dashboard_service._signal_store_for_runtime(
        None,
        None,
        knowledge_service=service,
        resolved_runtime_settings=settings,
    )

    assert resolved is signal_store
    assert service.repository.sqlite_readiness_admission is resolved.sqlite_readiness_admission


def test_path_replacement_between_signal_and_knowledge_construction_fails_before_mutation(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    signal_path = Path(settings.signals_db_path)
    replacement_path = tmp_path / "replacement.db"
    signal_store = SignalStore(signal_path, runtime_settings=settings)
    replacement_settings = settings.model_copy(update={"signals_db_path": str(replacement_path)})
    SignalStore(replacement_path, runtime_settings=replacement_settings)
    replacement_schema = _schema(replacement_path)

    signal_path.rename(tmp_path / "original.db")
    replacement_path.rename(signal_path)

    with pytest.raises(RuntimeOwnershipError, match="generation|identity"):
        create_scoped_knowledge_service(signal_store, runtime_settings=settings)

    assert _schema(signal_path) == replacement_schema


def test_shared_admission_accepts_exact_cap_and_rejects_cap_plus_one(
    tmp_path: Path,
) -> None:
    seed_settings = _settings(tmp_path, snapshot_max_bytes=2 * 1024 * 1024)
    seed_store = SignalStore(Path(seed_settings.signals_db_path), runtime_settings=seed_settings)
    KnowledgeRepository(runtime_settings=seed_settings, signal_store=seed_store)
    signal_path = Path(seed_settings.signals_db_path)
    admitted_bytes = require_sqlite_authority_snapshot_capacity(
        signal_path,
        snapshot_max_bytes=seed_settings.sqlite_snapshot_max_bytes,
        require_exists=True,
    )
    assert (
        seed_store.sqlite_readiness_admission.require_current_generation(
            snapshot_max_bytes=admitted_bytes,
        )
        == admitted_bytes
    )
    original_size = signal_path.stat().st_size
    with signal_path.open("ab") as database_file:
        database_file.write(b"x")
    try:
        with pytest.raises(SQLiteIdentityError) as exact_cap_error:
            seed_store.sqlite_readiness_admission.require_current_generation(
                snapshot_max_bytes=admitted_bytes,
            )
        assert exact_cap_error.value.reason == SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT
    finally:
        with signal_path.open("r+b") as database_file:
            database_file.truncate(original_size)

    service = create_scoped_knowledge_service(seed_store, runtime_settings=seed_settings)

    assert service.repository.sqlite_readiness_admission is seed_store.sqlite_readiness_admission

    with seed_store.transaction() as connection:
        connection.execute("CREATE TABLE wave_ib_capacity_growth (payload BLOB NOT NULL)")
        connection.execute(
            "INSERT INTO wave_ib_capacity_growth VALUES (zeroblob(?))",
            (2 * 1024 * 1024,),
        )

    with pytest.raises(SQLiteIdentityError) as exc_info:
        create_scoped_knowledge_service(seed_store, runtime_settings=seed_settings)
    assert exc_info.value.reason == SQLiteIdentityRejectionReason.ADMISSION_SNAPSHOT_LIMIT
