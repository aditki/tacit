"""Settings-backed ownership for Tacit's local persistence stores."""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from tacit.config import Settings

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

    Empty path settings delegate to the established global getters so existing
    embedding and test patch points keep working. Explicit paths are always
    owned by this container and never consult process-global store state.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        history_fallback: StoreFactory | None = None,
        feedback_fallback: StoreFactory | None = None,
        signal_fallback: StoreFactory | None = None,
    ) -> None:
        self.settings = settings
        self._history_fallback = history_fallback or _legacy_history_store
        self._feedback_fallback = feedback_fallback or _legacy_feedback_store
        self._signal_fallback = signal_fallback or _legacy_signal_store
        self._history_store: Any | None = None
        self._feedback_store: Any | None = None
        self._signal_store: Any | None = None
        self._lock = threading.RLock()

    @staticmethod
    def _configured_path(value: str) -> Path:
        path = Path(value)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def history(self) -> Any:
        """Return the history store for this runtime."""
        if not self.settings.history_db_path:
            return self._history_fallback()
        if self._history_store is None:
            with self._lock:
                if self._history_store is None:
                    from tacit.history import InvestigationStore

                    self._history_store = InvestigationStore(self._configured_path(self.settings.history_db_path))
        return self._history_store

    def feedback(self) -> Any:
        """Return the feedback store for this runtime."""
        if not self.settings.feedback_db_path:
            return self._feedback_fallback()
        if self._feedback_store is None:
            with self._lock:
                if self._feedback_store is None:
                    from tacit.feedback import FeedbackStore

                    self._feedback_store = FeedbackStore(self._configured_path(self.settings.feedback_db_path))
        return self._feedback_store

    def signals(self) -> Any:
        """Return the bootstrapped signal store for this runtime."""
        if not self.settings.signals_db_path:
            return self._signal_fallback()
        if self._signal_store is None:
            with self._lock:
                if self._signal_store is None:
                    from tacit.signals import SignalStore

                    store = SignalStore(self._configured_path(self.settings.signals_db_path))
                    store.load_from_yaml()
                    self._signal_store = store
        return self._signal_store
