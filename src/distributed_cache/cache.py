"""Two-level cache with TTL, invalidation, and cache statistics."""

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable


class InMemoryStore:
    """Thread-safe key-value store with optional TTL expiration."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self._clock = clock
        self._data: dict[str, tuple[Any, float | None]] = {}
        self._lock = threading.RLock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return None

            value, expires_at = item

            if expires_at is not None and self._clock() >= expires_at:
                self._data.pop(key, None)
                return None

            return value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float | None = None,
    ) -> None:
        if ttl_seconds is not None and ttl_seconds < 0:
            raise ValueError("TTL cannot be negative")

        expires_at = (
            None if ttl_seconds is None
            else self._clock() + ttl_seconds
        )

        with self._lock:
            self._data[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


@dataclass(frozen=True)
class CacheStats:
    l1_hits: int
    l2_hits: int
    misses: int
    writes: int
    invalidations: int

    @property
    def hit_rate(self) -> float:
        reads = self.l1_hits + self.l2_hits + self.misses
        if reads == 0:
            return 0.0
        return (self.l1_hits + self.l2_hits) / reads


class HierarchicalCache:
    """L1 in-memory cache backed by an independent L2 store."""

    def __init__(
        self,
        l2: InMemoryStore | None = None,
        default_ttl_seconds: float | None = 60.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.l1 = InMemoryStore(clock)
        self.l2 = l2 or InMemoryStore(clock)
        self.default_ttl_seconds = default_ttl_seconds

        self._counts = {
            "l1_hits": 0,
            "l2_hits": 0,
            "misses": 0,
            "writes": 0,
            "invalidations": 0,
        }
        self._lock = threading.RLock()

    def get(self, key: str) -> Any | None:
        value = self.l1.get(key)

        if value is not None:
            with self._lock:
                self._counts["l1_hits"] += 1
            return value

        value = self.l2.get(key)

        if value is None:
            with self._lock:
                self._counts["misses"] += 1
            return None

        self.l1.set(key, value, self.default_ttl_seconds)

        with self._lock:
            self._counts["l2_hits"] += 1

        return value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float | None = None,
    ) -> None:
        ttl = (
            self.default_ttl_seconds
            if ttl_seconds is None
            else ttl_seconds
        )

        if ttl is not None and ttl < 0:
            raise ValueError("TTL cannot be negative")

        self.l2.set(key, value, ttl)
        self.l1.set(key, value, ttl)

        with self._lock:
            self._counts["writes"] += 1

    def invalidate(self, key: str) -> None:
        """Remove a key from both cache levels."""
        self.l1.delete(key)
        self.l2.delete(key)

        with self._lock:
            self._counts["invalidations"] += 1

    def clear(self) -> None:
        self.l1.clear()
        self.l2.clear()

    def stats(self) -> CacheStats:
        with self._lock:
            return CacheStats(**self._counts)