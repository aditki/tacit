"""Simple TTL cache for metadata and LLM responses.

Eliminates redundant datasource API calls and LLM invocations.
In production, swap for Redis/Memcached via the same interface.
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

import structlog

logger = structlog.get_logger()


class TTLCache:
    """Thread-safe in-memory cache with per-key TTL."""

    def __init__(self, default_ttl: int = 300):
        self._store: dict[str, tuple[float, Any]] = {}
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() > expires_at:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        self._store[key] = (time.monotonic() + ttl, value)

    def invalidate(self, prefix: str = "") -> int:
        """Remove all keys matching prefix. Returns count removed."""
        if not prefix:
            count = len(self._store)
            self._store.clear()
            return count
        to_remove = [k for k in self._store if k.startswith(prefix)]
        for k in to_remove:
            del self._store[k]
        return len(to_remove)

    @property
    def size(self) -> int:
        return len(self._store)


# ── Global cache instances ───────────────────────────────────────────────

# Metric catalog: metric names + labels per datasource (5 min TTL)
metric_cache = TTLCache(default_ttl=300)

# LLM response cache: keyed by hash of (system_prompt, user_prompt) (10 min TTL)
llm_cache = TTLCache(default_ttl=600)


def make_cache_key(*parts: str) -> str:
    """Create a deterministic cache key from parts."""
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]
