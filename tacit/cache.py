"""Simple TTL cache for metadata and LLM responses.

Eliminates redundant datasource API calls and LLM invocations.
In production, swap for Redis/Memcached via the same interface.
"""

from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from collections.abc import Callable, Sized
from threading import RLock
from typing import Any

import structlog

logger = structlog.get_logger()


class TTLCache:
    """Thread-safe in-memory cache with per-key TTL."""

    def __init__(
        self,
        default_ttl: int = 300,
        max_entries: int = 2_048,
        *,
        max_total_weight: int | None = None,
        max_value_weight: int | None = None,
        weigher: Callable[[Any], int] | None = None,
    ):
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        if max_total_weight is not None and max_total_weight < 1:
            raise ValueError("max_total_weight must be positive")
        if max_value_weight is not None and max_value_weight < 1:
            raise ValueError("max_value_weight must be positive")
        self._store: OrderedDict[str, tuple[float, Any, int]] = OrderedDict()
        self._default_ttl = default_ttl
        self._max_entries = max_entries
        self._max_total_weight = max_total_weight
        self._max_value_weight = max_value_weight
        self._weigher = weigher or self._default_weight
        self._total_weight = 0
        self._hits = 0
        self._misses = 0
        self._lock = RLock()

    @staticmethod
    def _default_weight(value: Any) -> int:
        return max(1, len(value)) if isinstance(value, Sized) else 1

    def _remove(self, key: str) -> None:
        _expires_at, _value, weight = self._store.pop(key)
        self._total_weight -= weight

    def _prune_expired(self, now: float) -> int:
        expired = [key for key, (expires_at, _value, _weight) in self._store.items() if now > expires_at]
        for key in expired:
            self._remove(key)
        return len(expired)

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            expires_at, value, _weight = entry
            if time.monotonic() > expires_at:
                self._remove(key)
                self._misses += 1
                return None
            self._store.move_to_end(key)
            self._hits += 1
            return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        ttl = ttl if ttl is not None else self._default_ttl
        weight = max(1, self._weigher(value))
        with self._lock:
            now = time.monotonic()
            self._prune_expired(now)
            if key in self._store:
                self._remove(key)
            if self._max_value_weight is not None and weight > self._max_value_weight:
                logger.info(
                    "ttl_cache_value_not_retained",
                    value_weight=weight,
                    max_value_weight=self._max_value_weight,
                )
                return
            self._store[key] = (now + ttl, value, weight)
            self._total_weight += weight
            self._store.move_to_end(key)
            evicted = 0
            while len(self._store) > self._max_entries or (
                self._max_total_weight is not None and self._total_weight > self._max_total_weight
            ):
                oldest = next(iter(self._store))
                self._remove(oldest)
                evicted += 1
            if evicted:
                logger.debug(
                    "ttl_cache_capacity_eviction",
                    evicted=evicted,
                    max_entries=self._max_entries,
                    max_total_weight=self._max_total_weight,
                    retained_weight=self._total_weight,
                )

    def invalidate(self, prefix: str = "") -> int:
        """Remove all keys matching prefix. Returns count removed."""
        with self._lock:
            if not prefix:
                count = len(self._store)
                self._store.clear()
                self._total_weight = 0
                return count
            to_remove = [k for k in self._store if k.startswith(prefix)]
            for k in to_remove:
                self._remove(k)
            return len(to_remove)

    @property
    def size(self) -> int:
        with self._lock:
            self._prune_expired(time.monotonic())
            return len(self._store)

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            self._prune_expired(time.monotonic())
            return {"hits": self._hits, "misses": self._misses, "size": len(self._store)}

    @property
    def weight(self) -> int:
        with self._lock:
            self._prune_expired(time.monotonic())
            return self._total_weight

    def reset_stats(self) -> None:
        with self._lock:
            self._hits = 0
            self._misses = 0


# ── Global cache instances ───────────────────────────────────────────────

# Metric catalog: metric names + labels per datasource (5 min TTL)
metric_cache = TTLCache(
    default_ttl=300,
    max_total_weight=100_000,
    max_value_weight=20_000,
)

# LLM response cache: keyed by hash of (system_prompt, user_prompt) (10 min TTL)
llm_cache = TTLCache(default_ttl=600)


def make_cache_key(*parts: str) -> str:
    """Create a deterministic cache key from parts."""
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def cache_owner_namespace(owner: Any) -> str:
    """Return a non-secret cache namespace for one backend client identity."""
    namespace = str(getattr(owner, "cache_namespace", "") or "").strip()
    if namespace:
        return namespace
    owner_type = f"{type(owner).__module__}.{type(owner).__qualname__}"
    return make_cache_key(owner_type, str(id(owner)))
