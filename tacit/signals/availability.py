"""Three-state resolution for optional signal-store dependencies."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Final


class UnavailableSignalStore:
    """Marker that forbids fallback to process-global signal storage."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "SIGNAL_STORE_UNAVAILABLE"


SIGNAL_STORE_UNAVAILABLE: Final = UnavailableSignalStore()


def resolve_signal_store(
    signal_store: Any | None,
    fallback_factory: Callable[[], Any],
) -> Any | None:
    """Resolve an injected store while respecting an explicit unavailable state.

    ``None`` preserves the legacy behavior for callers that omit injection.
    ``SIGNAL_STORE_UNAVAILABLE`` means a scoped runtime tried and failed (or
    deliberately disabled the store), so consulting global state is forbidden.
    """
    if signal_store is SIGNAL_STORE_UNAVAILABLE:
        return None
    if signal_store is not None:
        return signal_store
    try:
        return fallback_factory()
    except Exception:
        return None
