"""Settings-backed ownership for Tacit's local persistence stores."""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

from tacit.config import Settings, canonical_sqlite_role_paths
from tacit.errors import RuntimeOwnershipError
from tacit.runtime_ownership import (
    RuntimeDatabaseIdentity,
    RuntimeOwnershipDescriptor,
    RuntimeOwnershipMismatchError,
    copy_runtime_settings,
    get_runtime_ownership,
    require_compatible_runtime_ownership,
    runtime_descriptor_from_settings,
    snapshot_runtime_settings,
)
from tacit.sqlite_identity import sqlite_database_path

StoreFactory = Callable[[], Any]


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
    ) -> None:
        from tacit import feedback as feedback_module
        from tacit import history as history_module
        from tacit.signals import store as signal_store_module

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
        self._history_fallback = history_fallback
        self._feedback_fallback = feedback_fallback
        self._signal_fallback = signal_fallback
        self._history_store: Any | None = None
        self._feedback_store: Any | None = None
        self._signal_store: Any | None = None
        self._knowledge_repository: Any | None = None
        self._knowledge_service: Any | None = None
        self._llm_cache: Any | None = None
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

    def _validated_store(self, store: Any, *, role: str) -> Any:
        expected_databases = tuple(database for database in self._runtime_ownership.databases if database.role == role)
        if len(expected_databases) != 1:
            raise RuntimeOwnershipMismatchError(
                "runtime store realization",
                {"database"},
                (self._runtime_ownership.component,),
            )
        expected = replace(
            self._runtime_ownership,
            component=f"runtime_stores_{role}",
            databases=expected_databases,
        )
        actual = get_runtime_ownership(store, component=f"realized_{role}_store")
        missing_dimensions: set[str] = set()
        if actual.settings_identity is None:
            missing_dimensions.add("settings")
        if actual.tenant_policy is None:
            missing_dimensions.update(("tenant", "permission"))
        if missing_dimensions:
            raise RuntimeOwnershipMismatchError(
                f"runtime {role} store realization",
                missing_dimensions,
                (expected.component, actual.component),
            )
        require_compatible_runtime_ownership(
            boundary=f"runtime {role} store realization",
            descriptors=(expected, actual),
        )
        if expected_databases[0] not in actual.databases:
            raise RuntimeOwnershipMismatchError(
                f"runtime {role} store realization",
                {"database"},
                (expected.component, actual.component),
            )
        return store

    def history(self) -> Any:
        """Return the history store for this runtime."""
        if self._history_uses_fallback and self._history_fallback is not None:
            return self._validated_store(self._history_fallback(), role="history")
        if self._history_store is None:
            with self._lock:
                if self._history_store is None:
                    from tacit.history import InvestigationStore

                    self._revalidate_database_role_files()
                    path = self._configured_path(self._history_path)
                    self._revalidate_database_role_files()
                    store = InvestigationStore(path, runtime_settings=self._settings)
                    self._history_store = self._validated_store(store, role="history")
        return self._validated_store(self._history_store, role="history")

    def feedback(self) -> Any:
        """Return the feedback store for this runtime."""
        if self._feedback_uses_fallback and self._feedback_fallback is not None:
            return self._validated_store(self._feedback_fallback(), role="feedback")
        if self._feedback_store is None:
            with self._lock:
                if self._feedback_store is None:
                    from tacit.feedback import FeedbackStore

                    self._revalidate_database_role_files()
                    path = self._configured_path(self._feedback_path)
                    self._revalidate_database_role_files()
                    store = FeedbackStore(path, runtime_settings=self._settings)
                    self._feedback_store = self._validated_store(store, role="feedback")
        return self._validated_store(self._feedback_store, role="feedback")

    def signals(self) -> Any:
        """Return the bootstrapped signal store for this runtime."""
        if self._signal_uses_fallback and self._signal_fallback is not None:
            return self._validated_store(self._signal_fallback(), role="signals")
        if self._signal_store is None:
            with self._lock:
                if self._signal_store is None:
                    from tacit.signals import SignalStore

                    self._revalidate_database_role_files()
                    path = self._configured_path(self._signal_path)
                    self._revalidate_database_role_files()
                    store = SignalStore(path, runtime_settings=self._settings)
                    store = self._validated_store(store, role="signals")
                    store.load_from_yaml(only_if_changed=True)
                    self._signal_store = store
        return self._validated_store(self._signal_store, role="signals")

    def knowledge_repository(self) -> Any:
        """Return the Operational Knowledge repository beside the signal store."""
        signal_store = self.signals()
        signal_db_path = signal_store.database_path
        if self._knowledge_repository is None or self._knowledge_repository.database_path != signal_db_path:
            with self._lock:
                if self._knowledge_repository is None or self._knowledge_repository.database_path != signal_db_path:
                    from tacit.knowledge.repository import KnowledgeRepository

                    self._knowledge_repository = KnowledgeRepository(
                        signal_db_path,
                        runtime_settings=self._settings,
                    )
                    self._knowledge_service = None
        return self._knowledge_repository

    def knowledge(self) -> Any:
        """Return the Operational Knowledge service for this runtime."""
        repository = self.knowledge_repository()
        if self._knowledge_service is None:
            with self._lock:
                if self._knowledge_service is None:
                    from tacit.knowledge.service import KnowledgeService

                    self._knowledge_service = KnowledgeService(
                        repository,
                        signal_store=self.signals(),
                        history_store_factory=self.history,
                        runtime_settings=self._settings,
                    )
        return self._knowledge_service

    def llm_cache(self) -> Any:
        """Return the bounded LLM cache owned by this runtime graph."""
        if self._llm_cache is None:
            with self._lock:
                if self._llm_cache is None:
                    from tacit.cache import TTLCache

                    self._llm_cache = TTLCache(default_ttl=600)
        return self._llm_cache
