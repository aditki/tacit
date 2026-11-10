"""Matrix coverage for composition-root ownership of injected SQLite stores."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from tacit.config import Settings
from tacit.dependencies import PipelineDependencies, build_pipeline_dependencies, declare_backend_factory
from tacit.errors import RuntimeOwnershipError
from tacit.feedback import FeedbackStore
from tacit.history import InvestigationStore
from tacit.knowledge.service import KnowledgeService
from tacit.runtime_ownership import declare_runtime_factory, runtime_descriptor_for_store
from tacit.runtime_stores import RuntimeStoreReadinessError, RuntimeStores
from tacit.signals.store import SignalStore


def _settings(tmp_path: Path, *, prefix: str = "runtime") -> Settings:
    return Settings(
        _env_file=None,
        history_db_path=str(tmp_path / prefix / "history.db"),
        feedback_db_path=str(tmp_path / prefix / "feedback.db"),
        signals_db_path=str(tmp_path / prefix / "signals.db"),
    )


def _declare_store_factory(
    factory: Callable[[], Any],
    *,
    settings: Settings,
    role: str,
    knowledge: bool = False,
) -> Callable[[], Any]:
    path = {
        "history": settings.history_db_path,
        "feedback": settings.feedback_db_path,
        "signals": settings.signals_db_path,
    }[role]
    return declare_runtime_factory(
        factory,
        ownership=runtime_descriptor_for_store(
            component=f"injected_{'knowledge' if knowledge else role}_matrix_factory",
            runtime_settings=settings,
            database_role=role,
            database_path=path,
        ),
        factory_kind="knowledge:signals" if knowledge else f"store:{role}",
    )


def _dependency_accessors(dependencies: PipelineDependencies) -> dict[str, Callable[[], Any]]:
    assert dependencies.signal_store_factory is not None
    assert dependencies.knowledge_service_factory is not None
    return {
        "history": dependencies.history_store_factory,
        "feedback": dependencies.feedback_store_factory,
        "signals": dependencies.signal_store_factory,
        "knowledge": dependencies.knowledge_service_factory,
    }


def _admission(product: Any, role: str) -> Any:
    owner = product.repository if role == "knowledge" else product
    return owner.sqlite_readiness_admission


def _build_injected_matrix(
    tmp_path: Path,
) -> tuple[Settings, RuntimeStores, PipelineDependencies, dict[str, int], dict[str, Any]]:
    runtime_settings = _settings(tmp_path)
    stores = RuntimeStores(runtime_settings)
    graph = stores.pipeline_admission().execution_graph
    calls = {role: 0 for role in ("history", "feedback", "signals", "knowledge")}
    products: dict[str, Any] = {}

    def realize(role: str, constructor: Callable[[], Any]) -> Any:
        calls[role] += 1
        product = constructor()
        products.setdefault(role, product)
        if graph.root_owner_count:
            raise AssertionError(f"{role} initialized after root registration")
        return product

    history_factory = _declare_store_factory(
        lambda: realize(
            "history",
            lambda: InvestigationStore(
                runtime_settings.history_db_path,
                runtime_settings=runtime_settings,
            ),
        ),
        settings=runtime_settings,
        role="history",
    )
    feedback_factory = _declare_store_factory(
        lambda: realize(
            "feedback",
            lambda: FeedbackStore(
                runtime_settings.feedback_db_path,
                runtime_settings=runtime_settings,
            ),
        ),
        settings=runtime_settings,
        role="feedback",
    )

    def create_signals() -> SignalStore:
        signal_store = SignalStore(
            runtime_settings.signals_db_path,
            runtime_settings=runtime_settings,
        )
        signal_store.load_from_yaml(only_if_changed=True)
        return signal_store

    signal_factory = _declare_store_factory(
        lambda: realize("signals", create_signals),
        settings=runtime_settings,
        role="signals",
    )
    knowledge_factory = _declare_store_factory(
        lambda: realize(
            "knowledge",
            lambda: KnowledgeService(
                signal_store=products["signals"],
                history_store_factory=lambda: products["history"],
                runtime_settings=runtime_settings,
            ),
        ),
        settings=runtime_settings,
        role="signals",
        knowledge=True,
    )
    dependencies = build_pipeline_dependencies(
        runtime_settings,
        stores=stores,
        history_store_factory=history_factory,
        feedback_store_factory=feedback_factory,
        signal_store_factory=signal_factory,
        knowledge_service_factory=knowledge_factory,
    )
    return runtime_settings, stores, dependencies, calls, products


def _build_isolated_injected_matrix(
    tmp_path: Path,
) -> tuple[Settings, RuntimeStores, PipelineDependencies, dict[str, int], dict[str, Any]]:
    runtime_settings = _settings(tmp_path, prefix="isolated-runtime")
    calls = {role: 0 for role in ("history", "feedback", "signals", "knowledge")}
    products: dict[str, Any] = {}
    stores_ref: dict[str, RuntimeStores] = {}

    def realize(role: str, constructor: Callable[[], Any]) -> Any:
        calls[role] += 1
        product = constructor()
        products.setdefault(role, product)
        stores = stores_ref.get("owner")
        if stores is not None and stores.pipeline_admission().execution_graph.root_owner_count:
            raise AssertionError(f"{role} initialized after root registration")
        return product

    history_factory = _declare_store_factory(
        lambda: realize(
            "history",
            lambda: InvestigationStore(
                runtime_settings.history_db_path,
                runtime_settings=runtime_settings,
            ),
        ),
        settings=runtime_settings,
        role="history",
    )
    feedback_factory = _declare_store_factory(
        lambda: realize(
            "feedback",
            lambda: FeedbackStore(
                runtime_settings.feedback_db_path,
                runtime_settings=runtime_settings,
            ),
        ),
        settings=runtime_settings,
        role="feedback",
    )

    def create_signals() -> SignalStore:
        signal_store = SignalStore(
            runtime_settings.signals_db_path,
            runtime_settings=runtime_settings,
        )
        signal_store.load_from_yaml(only_if_changed=True)
        return signal_store

    signal_factory = _declare_store_factory(
        lambda: realize("signals", create_signals),
        settings=runtime_settings,
        role="signals",
    )
    knowledge_factory = _declare_store_factory(
        lambda: realize(
            "knowledge",
            lambda: KnowledgeService(
                signal_store=products["signals"],
                history_store_factory=lambda: products["history"],
                runtime_settings=runtime_settings,
            ),
        ),
        settings=runtime_settings,
        role="signals",
        knowledge=True,
    )
    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=declare_backend_factory(
            lambda: [],
            runtime_settings=runtime_settings,
            component="isolated_store_generation_matrix_backends",
        ),
        history_store_factory=history_factory,
        feedback_store_factory=feedback_factory,
        signal_store_factory=signal_factory,
        knowledge_service_factory=knowledge_factory,
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )
    stores = dependencies.runtime_root_owner
    assert isinstance(stores, RuntimeStores)
    stores_ref["owner"] = stores
    return runtime_settings, stores, dependencies, calls, products


def test_injected_store_matrix_is_realized_before_root_and_pinned(tmp_path: Path) -> None:
    _settings_value, stores, dependencies, calls, products = _build_injected_matrix(tmp_path)
    accessors = _dependency_accessors(dependencies)

    assert calls == {"history": 0, "feedback": 0, "signals": 0, "knowledge": 0}
    handle = stores.start_runtime_services()
    try:
        assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
        snapshot_counts = {
            role: _admission(product, role)._target.snapshot_copy_telemetry.copy_count
            for role, product in products.items()
        }
        for role, accessor in accessors.items():
            assert accessor() is products[role]
            assert accessor() is products[role]
        assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
        assert {
            role: _admission(product, role)._target.snapshot_copy_telemetry.copy_count
            for role, product in products.items()
        } == snapshot_counts
        assert _admission(products["signals"], "signals") is _admission(products["knowledge"], "knowledge")
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


def test_isolated_injected_store_matrix_is_realized_before_root_and_pinned(tmp_path: Path) -> None:
    _settings_value, stores, dependencies, calls, products = _build_isolated_injected_matrix(tmp_path)
    accessors = _dependency_accessors(dependencies)

    assert calls == {"history": 0, "feedback": 0, "signals": 0, "knowledge": 0}
    handle = stores.start_runtime_services()
    try:
        assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
        snapshot_counts = {
            role: _admission(product, role)._target.snapshot_copy_telemetry.copy_count
            for role, product in products.items()
        }
        for role, accessor in accessors.items():
            assert accessor() is products[role]
            assert accessor() is products[role]
        assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
        assert {
            role: _admission(product, role)._target.snapshot_copy_telemetry.copy_count
            for role, product in products.items()
        } == snapshot_counts
        assert _admission(products["signals"], "signals") is _admission(products["knowledge"], "knowledge")
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


@pytest.mark.parametrize("role", ("history", "feedback", "signals", "knowledge"))
def test_isolated_pinned_store_matrix_rejects_readiness_capability_rebinding(
    tmp_path: Path,
    role: str,
) -> None:
    _settings_value, stores, dependencies, _calls, products = _build_isolated_injected_matrix(tmp_path)
    handle = stores.start_runtime_services()
    try:
        owner = products[role].repository if role == "knowledge" else products[role]
        owner._sqlite_readiness_admission = object()

        with pytest.raises(RuntimeOwnershipError, match="readiness admission"):
            _dependency_accessors(dependencies)[role]()
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


@pytest.mark.parametrize(
    ("role", "physical_role"),
    (
        ("history", "history"),
        ("feedback", "feedback"),
        ("signals", "signals"),
        ("knowledge", "signals"),
    ),
)
def test_isolated_injected_store_generation_change_blocks_root_reuse(
    tmp_path: Path,
    role: str,
    physical_role: str,
) -> None:
    runtime_settings, stores, dependencies, calls, _products = _build_isolated_injected_matrix(tmp_path)
    stores.prepare_required_stores()
    assert calls[role] == 1
    path = Path(
        {
            "history": runtime_settings.history_db_path,
            "feedback": runtime_settings.feedback_db_path,
            "signals": runtime_settings.signals_db_path,
        }[physical_role]
    )
    path.replace(path.with_suffix(".previous.db"))
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()
    if physical_role == "history":
        InvestigationStore(path, runtime_settings=runtime_settings)
    elif physical_role == "feedback":
        FeedbackStore(path, runtime_settings=runtime_settings)
    else:
        SignalStore(path, runtime_settings=runtime_settings)

    with pytest.raises(RuntimeStoreReadinessError):
        stores.start_runtime_services()

    assert stores.pipeline_admission().execution_graph.root_owner_count == 0
    assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
    with pytest.raises(RuntimeStoreReadinessError):
        _dependency_accessors(dependencies)[role]()


def test_isolated_changing_factories_cannot_escape_the_pinned_product_set(tmp_path: Path) -> None:
    _settings_value, stores, dependencies, calls, products = _build_isolated_injected_matrix(tmp_path)
    handle = stores.start_runtime_services()
    try:
        for role, accessor in _dependency_accessors(dependencies).items():
            first = accessor()
            assert first is products[role]
            assert accessor() is first
        assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


def test_isolated_default_semantic_stores_remain_owned_and_do_no_late_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path, prefix="isolated-defaults")
    calls = {"history": 0, "feedback": 0}

    def history_factory() -> InvestigationStore:
        calls["history"] += 1
        return InvestigationStore(
            runtime_settings.history_db_path,
            runtime_settings=runtime_settings,
        )

    def feedback_factory() -> FeedbackStore:
        calls["feedback"] += 1
        return FeedbackStore(
            runtime_settings.feedback_db_path,
            runtime_settings=runtime_settings,
        )

    dependencies = PipelineDependencies.isolated(
        settings=runtime_settings,
        backend_factory=declare_backend_factory(
            lambda: [],
            runtime_settings=runtime_settings,
            component="isolated_default_store_matrix_backends",
        ),
        history_store_factory=_declare_store_factory(
            history_factory,
            settings=runtime_settings,
            role="history",
        ),
        feedback_store_factory=_declare_store_factory(
            feedback_factory,
            settings=runtime_settings,
            role="feedback",
        ),
        llm_cache={},
        cache_key_factory=lambda *parts: ":".join(parts),
    )
    stores = dependencies.runtime_root_owner
    assert isinstance(stores, RuntimeStores)
    assert dependencies.signal_store_factory is None
    assert dependencies.knowledge_service_factory is None

    handle = stores.start_runtime_services()
    try:
        products = {
            "history": dependencies.history_store_factory(),
            "feedback": dependencies.feedback_store_factory(),
            "signals": stores.signals(),
            "knowledge": stores.knowledge(),
        }
        snapshot_counts = {
            role: _admission(product, role)._target.snapshot_copy_telemetry.copy_count
            for role, product in products.items()
        }
        assert calls == {"history": 1, "feedback": 1}
        assert products["signals"] is stores.signals()
        assert products["knowledge"] is stores.knowledge()
        assert _admission(products["signals"], "signals") is _admission(products["knowledge"], "knowledge")

        def late_work(*_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("isolated dependency access performed late SQLite work")

        monkeypatch.setattr("tacit.history.InvestigationStore", late_work)
        monkeypatch.setattr("tacit.feedback.FeedbackStore", late_work)
        monkeypatch.setattr("tacit.signals.SignalStore", late_work)
        monkeypatch.setattr("tacit.knowledge.repository.KnowledgeRepository", late_work)
        assert dependencies.history_store_factory() is products["history"]
        assert dependencies.feedback_store_factory() is products["feedback"]
        assert stores.signals() is products["signals"]
        assert stores.knowledge() is products["knowledge"]
        assert calls == {"history": 1, "feedback": 1}
        assert {
            role: _admission(product, role)._target.snapshot_copy_telemetry.copy_count
            for role, product in products.items()
        } == snapshot_counts
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


@pytest.mark.parametrize("role", ("history", "feedback", "signals", "knowledge"))
def test_pinned_store_matrix_rejects_readiness_capability_rebinding(
    tmp_path: Path,
    role: str,
) -> None:
    _settings_value, stores, dependencies, _calls, products = _build_injected_matrix(tmp_path)
    handle = stores.start_runtime_services()
    try:
        owner = products[role].repository if role == "knowledge" else products[role]
        owner._sqlite_readiness_admission = object()

        with pytest.raises(RuntimeOwnershipError, match="readiness admission"):
            _dependency_accessors(dependencies)[role]()
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


@pytest.mark.parametrize(
    ("role", "physical_role"),
    (
        ("history", "history"),
        ("feedback", "feedback"),
        ("signals", "signals"),
        ("knowledge", "signals"),
    ),
)
def test_injected_store_generation_change_blocks_root_reuse(
    tmp_path: Path,
    role: str,
    physical_role: str,
) -> None:
    runtime_settings, stores, dependencies, calls, _products = _build_injected_matrix(tmp_path)
    stores.prepare_required_stores()
    assert calls[role] == 1
    path = Path(
        {
            "history": runtime_settings.history_db_path,
            "feedback": runtime_settings.feedback_db_path,
            "signals": runtime_settings.signals_db_path,
        }[physical_role]
    )
    path.replace(path.with_suffix(".previous.db"))
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()
    if physical_role == "history":
        InvestigationStore(path, runtime_settings=runtime_settings)
    elif physical_role == "feedback":
        FeedbackStore(path, runtime_settings=runtime_settings)
    else:
        SignalStore(path, runtime_settings=runtime_settings)

    with pytest.raises(RuntimeStoreReadinessError):
        stores.start_runtime_services()

    assert stores.pipeline_admission().execution_graph.root_owner_count == 0
    assert calls == {"history": 1, "feedback": 1, "signals": 1, "knowledge": 1}
    with pytest.raises(RuntimeStoreReadinessError):
        _dependency_accessors(dependencies)[role]()


def test_compatibility_fallbacks_are_pinned_before_root_registration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = {
        "history": tmp_path / "compatibility" / "history.db",
        "feedback": tmp_path / "compatibility" / "feedback.db",
        "signals": tmp_path / "compatibility" / "signals.db",
    }
    monkeypatch.setattr("tacit.history._DEFAULT_DB_PATH", paths["history"])
    monkeypatch.setattr("tacit.feedback._DEFAULT_DB_PATH", paths["feedback"])
    monkeypatch.setattr("tacit.signals.store._DEFAULT_DB_PATH", paths["signals"])
    requested = Settings(_env_file=None)
    owner_settings = RuntimeStores(requested).runtime_settings
    products = {
        "history": InvestigationStore(paths["history"], runtime_settings=owner_settings),
        "feedback": FeedbackStore(paths["feedback"], runtime_settings=owner_settings),
        "signals": SignalStore(paths["signals"], runtime_settings=owner_settings),
    }
    products["signals"].load_from_yaml(only_if_changed=True)
    calls = {role: 0 for role in products}

    def fallback(role: str) -> Any:
        calls[role] += 1
        if calls[role] != 1:
            raise AssertionError(f"{role} compatibility fallback was realized twice")
        return products[role]

    stores = RuntimeStores(
        requested,
        history_fallback=_declare_store_factory(lambda: fallback("history"), settings=owner_settings, role="history"),
        feedback_fallback=_declare_store_factory(
            lambda: fallback("feedback"), settings=owner_settings, role="feedback"
        ),
        signal_fallback=_declare_store_factory(lambda: fallback("signals"), settings=owner_settings, role="signals"),
    )

    handle = stores.start_runtime_services()
    try:
        assert stores.history() is products["history"]
        assert stores.feedback() is products["feedback"]
        assert stores.signals() is products["signals"]
        assert (
            stores.knowledge().repository.sqlite_readiness_admission is products["signals"].sqlite_readiness_admission
        )
        assert calls == {"history": 1, "feedback": 1, "signals": 1}
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


def test_default_dependency_binding_after_root_reuses_admitted_products_without_cold_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime_settings = _settings(tmp_path)
    stores = RuntimeStores(runtime_settings)
    stores.prepare_required_stores()
    expected = {
        "history": stores.history(),
        "feedback": stores.feedback(),
        "signals": stores.signals(),
        "knowledge": stores.knowledge(),
    }
    handle = stores.start_runtime_services()

    def late_work(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("dependency binding performed late SQLite cold work")

    try:
        monkeypatch.setattr("tacit.history.InvestigationStore", late_work)
        monkeypatch.setattr("tacit.feedback.FeedbackStore", late_work)
        monkeypatch.setattr("tacit.signals.SignalStore", late_work)
        monkeypatch.setattr("tacit.knowledge.repository.KnowledgeRepository", late_work)
        dependencies = build_pipeline_dependencies(runtime_settings, stores=stores)

        for role, accessor in _dependency_accessors(dependencies).items():
            assert accessor() is expected[role]
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))


@pytest.mark.parametrize("role", ("history", "feedback", "signals", "knowledge"))
def test_late_injected_factory_is_rejected_before_invocation(
    tmp_path: Path,
    role: str,
) -> None:
    runtime_settings = _settings(tmp_path)
    stores = RuntimeStores(runtime_settings)
    handle = stores.start_runtime_services()
    calls = 0

    def late_factory() -> Any:
        nonlocal calls
        calls += 1
        raise AssertionError("late injected factory was invoked")

    physical_role = "signals" if role == "knowledge" else role
    declared = _declare_store_factory(
        late_factory,
        settings=runtime_settings,
        role=physical_role,
        knowledge=role == "knowledge",
    )
    keyword = {
        "history": "history_store_factory",
        "feedback": "feedback_store_factory",
        "signals": "signal_store_factory",
        "knowledge": "knowledge_service_factory",
    }[role]
    try:
        with pytest.raises(
            RuntimeOwnershipError,
            match="must be selected before runtime store readiness",
        ):
            build_pipeline_dependencies(
                runtime_settings,
                stores=stores,
                **{keyword: declared},
            )
        assert calls == 0
    finally:
        asyncio.run(stores.shutdown_runtime_services(handle))
