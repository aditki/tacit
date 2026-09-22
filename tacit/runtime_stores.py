"""Settings-backed ownership for Tacit's local persistence stores."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import structlog

import tacit.sqlite_identity as sqlite_identity
from tacit.config import Settings, canonical_sqlite_role_paths
from tacit.errors import RuntimeOwnershipError
from tacit.pipeline_admission import (
    PipelineAdmissionController,
    RuntimeRootOwnerHandle,
    pipeline_admission_limits,
    runtime_admission_controller,
)
from tacit.runtime_ownership import (
    RuntimeDatabaseIdentity,
    RuntimeOwnershipDescriptor,
    copy_runtime_settings,
    declare_runtime_factory,
    get_runtime_ownership,
    observe_runtime_factory_failure,
    observe_runtime_factory_realization,
    require_runtime_factory_ownership,
    require_runtime_store_ownership,
    runtime_descriptor_for_store,
    runtime_descriptor_from_settings,
    snapshot_runtime_settings,
)
from tacit.sqlite_identity import (
    SQLiteIdentityError,
    SQLiteIdentityRejectionReason,
    SQLiteSnapshotCopyTelemetry,
    observe_sqlite_snapshot_copies,
    runtime_sqlite_snapshot_max_bytes,
    sqlite_database_path,
)

StoreFactory = Callable[[], Any]
logger = structlog.get_logger()
_PIPELINE_STORE_UNAVAILABLE = object()


@dataclass(frozen=True)
class RuntimeStoreReadiness:
    """Bounded, non-sensitive result of required store preparation."""

    ready: bool
    prepared_roles: tuple[str, ...]
    snapshot_max_bytes: int
    snapshot_copy_count: int
    snapshot_copy_bytes: int
    snapshot_copy_roles: tuple[str, ...]
    duration_ms: float


class RuntimeStoreReadinessError(RuntimeError):
    """Raised when one required store cannot become ready before traffic."""

    reason_code = "runtime_store_readiness_failed"

    def __init__(self, *, role: str, cause_reason_code: str) -> None:
        self.role = role
        self.cause_reason_code = cause_reason_code
        super().__init__(f"Required SQLite store failed readiness (role={role}, reason={cause_reason_code})")


def require_signal_store_readiness_admission(
    signal_store: Any,
    *,
    runtime_settings: Settings,
    boundary: str,
    sqlite_snapshot_max_bytes: int | None = None,
) -> Any:
    """Return one current Signals capability without reopening its pathname."""
    signal_descriptor = get_runtime_ownership(signal_store, component="signal_store")
    signal_paths = tuple(database.path for database in signal_descriptor.databases if database.role == "signals")
    if len(signal_paths) != 1:
        raise RuntimeOwnershipError(f"{boundary} signal store must expose one Signals database identity")
    database_path = signal_paths[0]
    expected = runtime_descriptor_for_store(
        component=f"{boundary}_settings",
        runtime_settings=snapshot_runtime_settings(runtime_settings),
        database_role="signals",
        database_path=database_path,
    )
    require_runtime_store_ownership(
        boundary=boundary,
        expected=expected,
        store=signal_store,
        database_role="signals",
    )
    admission = getattr(signal_store, "sqlite_readiness_admission", None)
    if admission is None:
        raise RuntimeOwnershipError(f"{boundary} requires a shared SQLite readiness admission")
    target = admission.require_target(
        path=database_path,
        role="signals",
        tenant_owner=str(runtime_settings.knowledge_tenant_id or "default"),
    )
    snapshot_max_bytes = (
        target.snapshot_max_bytes
        if sqlite_snapshot_max_bytes is None
        else runtime_sqlite_snapshot_max_bytes(runtime_settings, sqlite_snapshot_max_bytes)
    )
    if target.snapshot_max_bytes != snapshot_max_bytes:
        raise RuntimeOwnershipError(f"{boundary} SQLite readiness admission capacity mismatch")
    admission.require_current_generation(snapshot_max_bytes=snapshot_max_bytes)
    return admission


def require_shared_signal_knowledge_admission(
    signal_store: Any,
    knowledge_owner: Any,
    *,
    runtime_settings: Settings,
    boundary: str,
    revalidate_generation: bool = True,
    sqlite_snapshot_max_bytes: int | None = None,
) -> Any:
    """Require Signals and Knowledge to share one exact admitted capability."""
    admission = (
        require_signal_store_readiness_admission(
            signal_store,
            runtime_settings=runtime_settings,
            boundary=boundary,
            sqlite_snapshot_max_bytes=sqlite_snapshot_max_bytes,
        )
        if revalidate_generation
        else getattr(signal_store, "sqlite_readiness_admission", None)
    )
    repository = getattr(knowledge_owner, "repository", knowledge_owner)
    repository_admission = getattr(repository, "sqlite_readiness_admission", None)
    if admission is None or repository_admission is not admission:
        raise RuntimeOwnershipError(f"{boundary} requires one exact shared SQLite readiness admission")
    return admission


def _legacy_history_store() -> Any:
    from tacit import history

    return history.get_investigation_store()


def _legacy_feedback_store() -> Any:
    from tacit import feedback

    return feedback.get_feedback_store()


def _legacy_signal_store() -> Any:
    from tacit import signals

    return signals.get_signal_store()


class RuntimeStores:
    """Construct and cache stores for one immutable runtime configuration.

    Every store is owned by this container, including stores using Tacit's
    default paths. Explicit fallback factories remain available for isolated
    compatibility tests, but production composition never consults globals.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        history_fallback: StoreFactory | None = None,
        feedback_fallback: StoreFactory | None = None,
        signal_fallback: StoreFactory | None = None,
        sqlite_snapshot_max_bytes: int | None = None,
        pipeline_admission: PipelineAdmissionController | None = None,
    ) -> None:
        from tacit import feedback as feedback_module
        from tacit import history as history_module
        from tacit.signals import store as signal_store_module

        self._sqlite_snapshot_max_bytes = runtime_sqlite_snapshot_max_bytes(
            settings,
            sqlite_snapshot_max_bytes,
        )
        configured_paths = {
            "history": self._capture_path(
                settings.history_db_path,
                history_module._DEFAULT_DB_PATH,
            ),
            "feedback": self._capture_path(
                settings.feedback_db_path,
                feedback_module._DEFAULT_DB_PATH,
            ),
            "signals": self._capture_path(
                settings.signals_db_path,
                signal_store_module._DEFAULT_DB_PATH,
            ),
        }
        captured_settings = snapshot_runtime_settings(settings)
        self._history_uses_fallback = not captured_settings.history_db_path and history_fallback is not None
        self._feedback_uses_fallback = not captured_settings.feedback_db_path and feedback_fallback is not None
        self._signal_uses_fallback = not captured_settings.signals_db_path and signal_fallback is not None
        try:
            canonical_sqlite_role_paths(configured_paths)
        except ValueError as exc:
            raise RuntimeOwnershipError(str(exc)) from exc
        self._history_path = configured_paths["history"]
        self._feedback_path = configured_paths["feedback"]
        self._signal_path = configured_paths["signals"]
        self._settings = snapshot_runtime_settings(
            captured_settings.model_copy(
                deep=True,
                update={
                    "history_db_path": str(self._history_path),
                    "feedback_db_path": str(self._feedback_path),
                    "signals_db_path": str(self._signal_path),
                },
            )
        )
        base_descriptor = runtime_descriptor_from_settings(self._settings, component="runtime_stores")
        self._runtime_ownership = replace(
            base_descriptor,
            databases=(
                RuntimeDatabaseIdentity(role="history", path=self._history_path),
                RuntimeDatabaseIdentity(role="feedback", path=self._feedback_path),
                RuntimeDatabaseIdentity(role="signals", path=self._signal_path),
            ),
        )
        self._history_fallback = self._validated_fallback_declaration(
            history_fallback,
            role="history",
            enabled=self._history_uses_fallback,
        )
        self._feedback_fallback = self._validated_fallback_declaration(
            feedback_fallback,
            role="feedback",
            enabled=self._feedback_uses_fallback,
        )
        self._signal_fallback = self._validated_fallback_declaration(
            signal_fallback,
            role="signals",
            enabled=self._signal_uses_fallback,
        )
        self._history_store: Any | None = None
        self._feedback_store: Any | None = None
        self._signal_store: Any | None = None
        self._knowledge_repository: Any | None = None
        self._knowledge_service: Any | None = None
        self._pipeline_store_selection_key: tuple[object, ...] | None = None
        self._pipeline_store_factories: dict[str, StoreFactory] | None = None
        self._pipeline_store_products: dict[str, Any] = {}
        self._pipeline_store_admissions: dict[str, Any] = {}
        self._llm_cache: Any | None = None
        if pipeline_admission is not None:
            limits = pipeline_admission_limits(captured_settings)
            configured_limits = (
                pipeline_admission.limit,
                pipeline_admission.max_queued,
                pipeline_admission.max_queued_per_partition,
                pipeline_admission.max_in_flight_per_partition,
            )
            expected_limits = (
                limits.concurrent,
                limits.queued,
                limits.queued_per_partition,
                limits.concurrent_per_partition,
            )
            if configured_limits != expected_limits:
                raise RuntimeOwnershipError("Injected pipeline admission limits must match runtime settings")
        self._pipeline_admission = pipeline_admission
        self._store_readiness: RuntimeStoreReadiness | None = None
        self._store_readiness_admissions: tuple[tuple[str, Any], ...] = ()
        self._pending_snapshot_copy_count = 0
        self._pending_snapshot_copy_bytes = 0
        self._pending_snapshot_copy_roles: set[str] = set()
        self._lock = threading.RLock()

    @property
    def runtime_settings(self) -> Settings:
        """Return the settings owned by this dependency graph."""
        return copy_runtime_settings(self._settings)

    @property
    def settings(self) -> Settings:
        """Return a detached compatibility view of this runtime's settings."""
        return self.runtime_settings

    @property
    def runtime_ownership(self) -> RuntimeOwnershipDescriptor:
        """Describe this dependency graph without constructing any resources."""
        return self._runtime_ownership

    @staticmethod
    def _capture_path(value: str, fallback: Path) -> Path:
        return sqlite_database_path(value or fallback)

    @staticmethod
    def _configured_path(value: Path) -> Path:
        return value

    def _revalidate_database_role_files(self) -> None:
        """Recheck lazy store paths against their current filesystem identities."""
        try:
            canonical_sqlite_role_paths(
                {
                    "history": self._history_path,
                    "feedback": self._feedback_path,
                    "signals": self._signal_path,
                }
            )
        except ValueError as exc:
            raise RuntimeOwnershipError(str(exc)) from exc

    def validate_store(self, store: Any, *, role: str) -> Any:
        """Validate a realized store against this composition owner."""
        require_runtime_store_ownership(
            boundary=f"runtime {role} store realization",
            expected=self._runtime_ownership,
            store=store,
            database_role=role,
        )
        return store

    def _validated_fallback_declaration(
        self,
        factory: StoreFactory | None,
        *,
        role: str,
        enabled: bool,
    ) -> StoreFactory | None:
        """Preflight a used compatibility factory without invoking it."""
        if factory is None or not enabled:
            return factory
        require_runtime_factory_ownership(
            boundary=f"runtime {role} fallback factory preflight",
            factory=factory,
            expected=self._runtime_ownership,
            factory_kind=f"store:{role}",
        )
        return factory

    def _realize_fallback(self, factory: StoreFactory, *, role: str) -> Any:
        """Validate a fallback product before any of its methods are used."""
        with observe_runtime_factory_realization(f"store:{role}"):
            store = factory()
        try:
            return self.validate_store(store, role=role)
        except RuntimeOwnershipError as exc:
            dimensions: frozenset[str] = getattr(exc, "dimensions", frozenset())
            observe_runtime_factory_failure(
                phase="realization",
                factory_kind=f"store:{role}",
                reason_code=(
                    "runtime_factory_realization_mismatch" if dimensions else "runtime_factory_realization_invalid"
                ),
                dimensions=dimensions,
            )
            raise

    def _owned_history(self) -> Any:
        """Return the history store constructed directly by this container."""
        if self._history_uses_fallback and self._history_fallback is not None:
            if self._history_store is None:
                with self._lock:
                    if self._history_store is None:
                        self._history_store = self._realize_fallback(
                            self._history_fallback,
                            role="history",
                        )
            return self.validate_store(self._history_store, role="history")
        if self._history_store is None:
            with self._lock:
                if self._history_store is None:
                    from tacit.history import InvestigationStore

                    self._revalidate_database_role_files()
                    path = self._configured_path(self._history_path)
                    self._revalidate_database_role_files()
                    store = InvestigationStore(
                        path,
                        runtime_settings=self._settings,
                        sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                    )
                    self._history_store = self.validate_store(store, role="history")
        return self.validate_store(self._history_store, role="history")

    def history(self) -> Any:
        """Return the pinned pipeline history store when one is selected."""
        with self._lock:
            if self._pipeline_store_factories is not None:
                return self.pipeline_store("history")
            return self._owned_history()

    def _owned_feedback(self) -> Any:
        """Return the feedback store constructed directly by this container."""
        if self._feedback_uses_fallback and self._feedback_fallback is not None:
            if self._feedback_store is None:
                with self._lock:
                    if self._feedback_store is None:
                        self._feedback_store = self._realize_fallback(
                            self._feedback_fallback,
                            role="feedback",
                        )
            return self.validate_store(self._feedback_store, role="feedback")
        if self._feedback_store is None:
            with self._lock:
                if self._feedback_store is None:
                    from tacit.feedback import FeedbackStore

                    self._revalidate_database_role_files()
                    path = self._configured_path(self._feedback_path)
                    self._revalidate_database_role_files()
                    store = FeedbackStore(
                        path,
                        runtime_settings=self._settings,
                        sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                    )
                    self._feedback_store = self.validate_store(store, role="feedback")
        return self.validate_store(self._feedback_store, role="feedback")

    def feedback(self) -> Any:
        """Return the pinned pipeline feedback store when one is selected."""
        with self._lock:
            if self._pipeline_store_factories is not None:
                return self.pipeline_store("feedback")
            return self._owned_feedback()

    def _owned_signals(self) -> Any:
        """Return the Signals store constructed directly by this container."""
        if self._signal_uses_fallback and self._signal_fallback is not None:
            if self._signal_store is None:
                with self._lock:
                    if self._signal_store is None:
                        self._signal_store = self._realize_fallback(
                            self._signal_fallback,
                            role="signals",
                        )
            return self.validate_store(self._signal_store, role="signals")
        if self._signal_store is None:
            with self._lock:
                if self._signal_store is None:
                    from tacit.signals import SignalStore

                    self._revalidate_database_role_files()
                    path = self._configured_path(self._signal_path)
                    self._revalidate_database_role_files()
                    store = SignalStore(
                        path,
                        runtime_settings=self._settings,
                        sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                    )
                    store = self.validate_store(store, role="signals")
                    store.load_from_yaml(only_if_changed=True)
                    self._signal_store = store
        return self.validate_store(self._signal_store, role="signals")

    def signals(self) -> Any:
        """Return the pinned pipeline Signals store when one is selected."""
        with self._lock:
            if self._pipeline_store_factories is not None:
                return self.pipeline_store("signals")
            return self._owned_signals()

    def _owned_knowledge_repository(self) -> Any:
        """Return the repository constructed beside this container's Signals store."""
        signal_store = self._owned_signals()
        signal_db_path = signal_store.database_path
        if self._knowledge_repository is None or self._knowledge_repository.database_path != signal_db_path:
            with self._lock:
                if self._knowledge_repository is None or self._knowledge_repository.database_path != signal_db_path:
                    from tacit.knowledge.repository import KnowledgeRepository

                    require_signal_store_readiness_admission(
                        signal_store,
                        runtime_settings=self._settings,
                        boundary="runtime Signals and Operational Knowledge admission",
                        sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                    )
                    self._knowledge_repository = KnowledgeRepository(
                        signal_db_path,
                        runtime_settings=self._settings,
                        sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                        signal_store=signal_store,
                    )
                    require_shared_signal_knowledge_admission(
                        signal_store,
                        self._knowledge_repository,
                        runtime_settings=self._settings,
                        boundary="runtime Signals and Operational Knowledge admission",
                        revalidate_generation=False,
                        sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                    )
                    self._knowledge_service = None
        return self._knowledge_repository

    def knowledge_repository(self) -> Any:
        """Return the repository behind the selected Operational Knowledge service."""
        with self._lock:
            if self._pipeline_store_factories is not None:
                return self._readiness_owner(
                    self.pipeline_store("knowledge"),
                    role="knowledge",
                )
            return self._owned_knowledge_repository()

    def _owned_knowledge(self) -> Any:
        """Return the Operational Knowledge service constructed by this container."""
        repository = self._owned_knowledge_repository()
        if self._knowledge_service is None:
            with self._lock:
                if self._knowledge_service is None:
                    from tacit.knowledge.service import KnowledgeService

                    self._knowledge_service = KnowledgeService(
                        repository,
                        signal_store=self._owned_signals(),
                        history_store_factory=self._owned_history,
                        runtime_settings=self._settings,
                    )
        return self._knowledge_service

    def knowledge(self) -> Any:
        """Return the pinned pipeline Operational Knowledge service when selected."""
        with self._lock:
            if self._pipeline_store_factories is not None:
                return self.pipeline_store("knowledge")
            return self._owned_knowledge()

    @staticmethod
    def _readiness_owner(product: Any, *, role: str) -> Any:
        return getattr(product, "repository", product) if role == "knowledge" else product

    @staticmethod
    def _physical_store_role(role: str) -> str:
        return "signals" if role == "knowledge" else role

    def _validate_pipeline_store_product(self, product: Any, *, role: str) -> Any:
        """Validate one selected product and return its exact readiness capability."""
        physical_role = self._physical_store_role(role)
        self.validate_store(product, role=physical_role)
        readiness_owner = self._readiness_owner(product, role=role)
        admission = getattr(readiness_owner, "sqlite_readiness_admission", None)
        require_target = getattr(admission, "require_target", None)
        if admission is None or not callable(require_target):
            raise RuntimeOwnershipError(f"Pinned {role} store must expose a SQLite readiness admission")
        path = {
            "history": self._history_path,
            "feedback": self._feedback_path,
            "signals": self._signal_path,
        }[physical_role]
        target = require_target(
            path=path,
            role=physical_role,
            tenant_owner=str(self._settings.knowledge_tenant_id or "default"),
        )
        if target.snapshot_max_bytes != self._sqlite_snapshot_max_bytes:
            raise RuntimeOwnershipError(f"Pinned {role} store SQLite readiness admission capacity mismatch")
        if role == "knowledge":
            signal_store = self._realize_pipeline_store_locked("signals")
            signal_admission = self._pipeline_store_admissions["signals"]
            if admission is not signal_admission:
                raise RuntimeOwnershipError(
                    "Pinned Signals and Operational Knowledge must share one exact SQLite readiness admission"
                )
            require_shared_signal_knowledge_admission(
                signal_store,
                product,
                runtime_settings=self._settings,
                boundary="Pinned pipeline Signals and Operational Knowledge admission",
                revalidate_generation=False,
                sqlite_snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
            )
        return admission

    def _realize_pipeline_store_locked(self, role: str) -> Any:
        factories = self._pipeline_store_factories
        if factories is None or role not in factories:
            raise RuntimeOwnershipError("Pipeline store selection is unavailable")
        product = self._pipeline_store_products.get(role, _PIPELINE_STORE_UNAVAILABLE)
        if role not in self._pipeline_store_products:
            product = factories[role]()
            if product is None:
                if role != "signals":
                    raise RuntimeOwnershipError(f"Pinned {role} store is unavailable")
                self._pipeline_store_products[role] = _PIPELINE_STORE_UNAVAILABLE
                return None
            admission = self._validate_pipeline_store_product(product, role=role)
            self._pipeline_store_products[role] = product
            self._pipeline_store_admissions[role] = admission
        elif product is _PIPELINE_STORE_UNAVAILABLE:
            return None
        else:
            admission = self._validate_pipeline_store_product(product, role=role)
            if admission is not self._pipeline_store_admissions[role]:
                raise RuntimeOwnershipError(f"Pinned {role} store readiness admission changed")
        return product

    def _require_pipeline_products_pinned_locked(self) -> None:
        """Recheck every realized product against the capability retained for it."""
        for role, product in self._pipeline_store_products.items():
            if product is _PIPELINE_STORE_UNAVAILABLE:
                continue
            admission = self._validate_pipeline_store_product(product, role=role)
            if admission is not self._pipeline_store_admissions.get(role):
                raise RuntimeOwnershipError(f"Pinned {role} store readiness admission changed")

    def bind_pipeline_store_factories(
        self,
        *,
        history: StoreFactory,
        feedback: StoreFactory,
        signals: StoreFactory,
        knowledge: StoreFactory,
        selection_key: tuple[object, ...],
        uses_runtime_owned_products: bool,
    ) -> None:
        """Bind one immutable four-role product set to this runtime root."""
        if not selection_key:
            raise RuntimeOwnershipError("Pipeline store selection identity is required")
        selected = {
            "history": history,
            "feedback": feedback,
            "signals": signals,
            "knowledge": knowledge,
        }
        for role, factory in selected.items():
            physical_role = self._physical_store_role(role)
            require_runtime_factory_ownership(
                boundary=f"runtime pinned {role} store factory preflight",
                factory=factory,
                expected=self._runtime_ownership,
                factory_kind="knowledge:signals" if role == "knowledge" else f"store:{physical_role}",
            )
        with self._lock:
            if self._pipeline_store_selection_key is not None:
                if self._pipeline_store_selection_key != selection_key:
                    raise RuntimeOwnershipError("Runtime store selection cannot change within one composition owner")
                return
            if self._store_readiness is not None and not uses_runtime_owned_products:
                raise RuntimeOwnershipError("Injected pipeline stores must be selected before runtime store readiness")
            self._pipeline_store_selection_key = selection_key
            self._pipeline_store_factories = selected
            try:
                if self._store_readiness is not None:
                    for role in ("history", "feedback", "signals", "knowledge"):
                        self._realize_pipeline_store_locked(role)
                    admitted = tuple(admission for _role, admission in self._store_readiness_admissions)
                    for role, admission in self._pipeline_store_admissions.items():
                        if not any(admission is existing for existing in admitted):
                            raise RuntimeOwnershipError(
                                f"Pinned {role} store was not admitted by the active runtime root"
                            )
            except BaseException:
                self._pipeline_store_selection_key = None
                self._pipeline_store_factories = None
                self._pipeline_store_products.clear()
                self._pipeline_store_admissions.clear()
                raise

    def pipeline_store(self, role: str) -> Any:
        """Return one exact selected product without invoking its factory again."""
        if role not in {"history", "feedback", "signals", "knowledge"}:
            raise RuntimeOwnershipError("Unsupported pipeline store role")
        with self._lock:
            product = self._realize_pipeline_store_locked(role)
            readiness = self._store_readiness
            admissions = self._store_readiness_admissions
            root_state = self.pipeline_admission().execution_graph.root_state
            if readiness is not None and root_state != "active":
                self._require_readiness_admissions_current(
                    admissions,
                    started=time.perf_counter(),
                )
            return product

    def llm_cache(self) -> Any:
        """Return the bounded LLM cache owned by this runtime graph."""
        if self._llm_cache is None:
            with self._lock:
                if self._llm_cache is None:
                    from tacit.cache import TTLCache

                    self._llm_cache = TTLCache(default_ttl=600)
        return self._llm_cache

    def prepare_required_stores(self) -> RuntimeStoreReadiness:
        """Initialize every required SQLite role before the runtime accepts traffic."""
        with self._lock:
            started = time.perf_counter()
            if self._store_readiness is not None:
                self._require_readiness_admissions_current(
                    self._store_readiness_admissions,
                    started=started,
                )
                return self._store_readiness
            prepared: list[str] = []
            prepared_admissions: list[tuple[str, Any]] = []
            accessors = (
                (
                    ("history", lambda: self.pipeline_store("history")),
                    ("feedback", lambda: self.pipeline_store("feedback")),
                    ("signals", lambda: self.pipeline_store("signals")),
                    ("knowledge", lambda: self.pipeline_store("knowledge")),
                )
                if self._pipeline_store_factories is not None
                else (
                    ("history", self.history),
                    ("feedback", self.feedback),
                    ("signals", self.signals),
                    ("knowledge", self.knowledge_repository),
                )
            )
            for role, accessor in accessors:
                try:
                    physical_role = "signals" if role == "knowledge" else role
                    with observe_sqlite_snapshot_copies(
                        role=physical_role,
                        observer=self._record_pending_snapshot_copy,
                    ):
                        prepared_store = accessor()
                    readiness_owner = self._readiness_owner(prepared_store, role=role)
                    readiness_admission = getattr(
                        readiness_owner,
                        "sqlite_readiness_admission",
                        None,
                    )
                    if readiness_admission is None:
                        raise RuntimeOwnershipError("Prepared SQLite store does not expose a readiness admission")
                    if not any(existing is readiness_admission for _existing_role, existing in prepared_admissions):
                        prepared_admissions.append((physical_role, readiness_admission))
                except Exception as exc:
                    cause_reason_code = (
                        exc.reason_code
                        if isinstance(exc, SQLiteIdentityError)
                        else "runtime_store_initialization_failed"
                    )
                    duration_ms = (time.perf_counter() - started) * 1_000
                    logger.error(
                        "runtime_store_readiness_failed",
                        reason_code=RuntimeStoreReadinessError.reason_code,
                        cause_reason_code=cause_reason_code,
                        role=role,
                        duration_ms=round(duration_ms, 3),
                    )
                    raise RuntimeStoreReadinessError(
                        role=role,
                        cause_reason_code=cause_reason_code,
                    ) from exc
                prepared.append(role)
            if self._pipeline_store_factories is not None:
                self._require_pipeline_products_pinned_locked()
            self._require_readiness_admissions_current(
                tuple(prepared_admissions),
                started=started,
            )
            duration_ms = (time.perf_counter() - started) * 1_000
            snapshot_copy_count = self._pending_snapshot_copy_count
            snapshot_copy_bytes = self._pending_snapshot_copy_bytes
            snapshot_copy_roles = tuple(sorted(self._pending_snapshot_copy_roles))
            self._store_readiness = RuntimeStoreReadiness(
                ready=True,
                prepared_roles=tuple(prepared),
                snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                snapshot_copy_count=snapshot_copy_count,
                snapshot_copy_bytes=snapshot_copy_bytes,
                snapshot_copy_roles=snapshot_copy_roles,
                duration_ms=duration_ms,
            )
            self._store_readiness_admissions = tuple(prepared_admissions)
            logger.info(
                "runtime_stores_ready",
                reason_code="runtime_stores_ready",
                prepared_role_count=len(prepared),
                snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                snapshot_copy_count=snapshot_copy_count,
                snapshot_copy_bytes=snapshot_copy_bytes,
                snapshot_copy_roles=snapshot_copy_roles,
                duration_ms=round(duration_ms, 3),
            )
            self._pending_snapshot_copy_count = 0
            self._pending_snapshot_copy_bytes = 0
            self._pending_snapshot_copy_roles.clear()
            return self._store_readiness

    def _require_readiness_admissions_current(
        self,
        admissions: tuple[tuple[str, Any], ...],
        *,
        started: float,
    ) -> None:
        """Fail closed unless every prepared authority is current as one set."""
        with self._hold_readiness_admission_set(admissions, started=started):
            pass

    @contextmanager
    def _hold_readiness_admission_set(
        self,
        admissions: tuple[tuple[str, Any], ...],
        *,
        started: float,
    ) -> Iterator[None]:
        """Hold one deterministic write-fenced boundary across physical stores."""
        role_paths = {
            "history": self._history_path,
            "feedback": self._feedback_path,
            "signals": self._signal_path,
        }
        tenant_owner = str(self._settings.knowledge_tenant_id or "default")
        targets: list[tuple[str, Any, Any]] = []
        active_role = "runtime"
        connections: list[Any] = []
        initial_file_identities: dict[str, tuple[int, int]] = {}
        try:
            for role, admission in admissions:
                active_role = role
                path = role_paths.get(role)
                if path is None:
                    raise RuntimeOwnershipError("Prepared SQLite admission has an unsupported role")
                target = admission.require_target(
                    path=path,
                    role=role,
                    tenant_owner=tenant_owner,
                )
                targets.append((role, admission, target))
            targets.sort(key=lambda item: str(item[2].path))

            for role, _admission, target in targets:
                active_role = role
                metadata = sqlite_identity.inspect_sqlite_database_target(target.path)
                if metadata is None:
                    raise SQLiteIdentityError(
                        "Prepared SQLite authority disappeared before root admission",
                        SQLiteIdentityRejectionReason.FILE_REPLACED,
                    )
                initial_file_identities[role] = (int(metadata.st_dev), int(metadata.st_ino))
                connection = target.connect(timeout_ms=30_000)
                try:
                    connection.execute("BEGIN IMMEDIATE")
                except BaseException:
                    connection.close()
                    raise
                connections.append(connection)

            for (role, admission, target), connection in zip(targets, connections, strict=True):
                active_role = role
                sqlite_identity.require_sqlite_database_identity(
                    connection,
                    role=role,
                    expected_database_id=admission.database_id,
                )
                sqlite_identity.require_sqlite_authority_snapshot_capacity(
                    target.path,
                    snapshot_max_bytes=self._sqlite_snapshot_max_bytes,
                    require_exists=True,
                )

            for role, _admission, target in targets:
                active_role = role
                metadata = sqlite_identity.inspect_sqlite_database_target(target.path)
                current_identity = None if metadata is None else (int(metadata.st_dev), int(metadata.st_ino))
                if current_identity != initial_file_identities[role]:
                    raise SQLiteIdentityError(
                        "Prepared SQLite authority changed during root admission",
                        SQLiteIdentityRejectionReason.ROLE_IDENTITY,
                    )
        except Exception as exc:
            for connection in reversed(connections):
                with suppress(Exception):
                    connection.rollback()
                with suppress(Exception):
                    connection.close()
            if isinstance(exc, sqlite3.OperationalError) and getattr(exc, "sqlite_errorcode", None) in {
                sqlite3.SQLITE_BUSY,
                sqlite3.SQLITE_LOCKED,
            }:
                exc = SQLiteIdentityError(
                    "SQLite store-set admission could not acquire every physical authority",
                    SQLiteIdentityRejectionReason.ADMISSION_TIMEOUT,
                )
            cause_reason_code = (
                exc.reason_code if isinstance(exc, SQLiteIdentityError) else "runtime_store_initialization_failed"
            )
            duration_ms = (time.perf_counter() - started) * 1_000
            logger.error(
                "runtime_store_readiness_failed",
                reason_code=RuntimeStoreReadinessError.reason_code,
                cause_reason_code=cause_reason_code,
                role=active_role,
                duration_ms=round(duration_ms, 3),
            )
            raise RuntimeStoreReadinessError(
                role=active_role,
                cause_reason_code=cause_reason_code,
            ) from exc

        try:
            yield
        finally:
            for connection in reversed(connections):
                with suppress(Exception):
                    connection.rollback()
                with suppress(Exception):
                    connection.close()

    def _record_pending_snapshot_copy(self, telemetry: SQLiteSnapshotCopyTelemetry) -> None:
        """Accumulate retry-safe copy metrics in constant-size bounded state."""
        self._pending_snapshot_copy_count += telemetry.copy_count
        self._pending_snapshot_copy_bytes += telemetry.copied_bytes
        if telemetry.copy_count:
            self._pending_snapshot_copy_roles.add(telemetry.role)

    def pipeline_admission(self) -> PipelineAdmissionController:
        """Return the concurrency gate owned by this runtime graph."""
        if self._pipeline_admission is None:
            with self._lock:
                if self._pipeline_admission is None:
                    runtime_identity = (
                        self._runtime_ownership.admission_namespace or self._runtime_ownership.settings_identity
                    )
                    if runtime_identity is None:
                        raise RuntimeOwnershipError("Runtime stores have no admission identity")
                    self._pipeline_admission = runtime_admission_controller(
                        self._settings,
                        runtime_identity=runtime_identity,
                    )
        return self._pipeline_admission

    def start_runtime_services(self) -> RuntimeRootOwnerHandle:
        """Prepare required stores, then register one composition root."""
        with self._lock:
            if self._store_readiness is None:
                self.prepare_required_stores()
            with self._hold_readiness_admission_set(
                self._store_readiness_admissions,
                started=time.perf_counter(),
            ):
                if self._pipeline_store_factories is not None:
                    self._require_pipeline_products_pinned_locked()
                return self.pipeline_admission().execution_graph.register_root_owner()

    async def shutdown_runtime_services(self, handle: RuntimeRootOwnerHandle) -> None:
        """Release this root and terminally drain only when it is the final owner."""
        await self.pipeline_admission().execution_graph.release_root_owner(handle)


_process_runtime_lock = threading.Lock()
_process_runtime_settings: Settings | None = None
_process_runtime_history_fallback: StoreFactory | None = None
_process_runtime_stores: RuntimeStores | None = None


def get_process_runtime_stores(
    runtime_settings: Settings,
    *,
    history_fallback: StoreFactory,
) -> RuntimeStores:
    """Return the single owner graph for process-default pipeline callers."""
    global _process_runtime_history_fallback
    global _process_runtime_settings
    global _process_runtime_stores

    selected_settings = copy_runtime_settings(runtime_settings)
    with _process_runtime_lock:
        if (
            _process_runtime_stores is None
            or _process_runtime_settings != selected_settings
            or _process_runtime_history_fallback is not history_fallback
        ):
            _process_runtime_settings = selected_settings
            _process_runtime_history_fallback = history_fallback
            owner_probe = RuntimeStores(selected_settings)
            history_database = next(
                database for database in owner_probe.runtime_ownership.databases if database.role == "history"
            )
            declared_history_fallback = declare_runtime_factory(
                history_fallback,
                ownership=runtime_descriptor_for_store(
                    component="process_history_fallback",
                    runtime_settings=owner_probe.runtime_settings,
                    database_role="history",
                    database_path=history_database.path,
                ),
                factory_kind="store:history",
            )
            _process_runtime_stores = RuntimeStores(
                selected_settings,
                history_fallback=declared_history_fallback,
            )
        return _process_runtime_stores
