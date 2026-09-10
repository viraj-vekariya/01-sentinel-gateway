"""Pure-Python rate limiter — the control arm of the benchmark.

This is not dead code and it is not a fallback bolted on for portability. It is the
thing the native extension is measured against, and it has to be a *fair* opponent:
if this implementation were sloppy, the speedup in bench/bench_ratelimit.py would be
measuring my bad Python rather than the GIL. So it is written the way a competent
Python engineer would write it if C++ were not an option — lazy refill, striped locks,
monotonic clock, the same eviction policy, no per-request allocation in the hot path.

The one thing it cannot do is release the GIL. That is the entire finding.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, List


@dataclass(slots=True)
class Decision:
    """Mirrors sentinel_native.Decision so callers cannot tell the two apart.

    `slots=True` is not cosmetic: the gateway allocates one of these per request, and
    a __dict__-backed dataclass costs roughly 3x the memory and a measurably slower
    attribute path. Matching the native struct's shape keeps the comparison honest.
    """

    allowed: bool
    tokens_remaining: float
    retry_after: float
    observed: int = 0


class _Bucket:
    """One client's token bucket. Same lazy-refill scheme as the C++ version."""

    __slots__ = ("capacity", "refill_per_sec", "tokens", "last_refill")

    def __init__(self, capacity: float, refill_per_sec: float, now: float) -> None:
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self.tokens = capacity          # fresh clients start full, as in C++
        self.last_refill = now

    def try_consume(self, cost: float, now: float) -> Decision:
        elapsed = now - self.last_refill
        if elapsed > 0.0:
            self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_sec)
            self.last_refill = now

        if self.tokens >= cost:
            self.tokens -= cost
            return Decision(True, self.tokens, 0.0)
        # Denied requests do not consume tokens — same rule as the native path.
        return Decision(False, self.tokens, (cost - self.tokens) / self.refill_per_sec)


class _Window:
    """Sliding-window counter, two-bucket interpolation. Mirrors the C++ class."""

    __slots__ = ("limit", "window_sec", "current", "previous", "window_start")

    def __init__(self, limit: int, window_sec: float, now: float) -> None:
        self.limit = limit
        self.window_sec = window_sec
        self.current = 0
        self.previous = 0
        self.window_start = now

    def _roll(self, now: float) -> None:
        elapsed = now - self.window_start
        if elapsed < self.window_sec:
            return
        if elapsed < 2.0 * self.window_sec:
            self.previous = self.current
            self.current = 0
            self.window_start += self.window_sec
        else:
            self.previous = 0
            self.current = 0
            self.window_start = now

    def estimate(self, now: float) -> float:
        elapsed = now - self.window_start
        if elapsed >= 2.0 * self.window_sec:
            return 0.0
        prev, cur, into = float(self.previous), float(self.current), elapsed
        if elapsed >= self.window_sec:
            prev, cur, into = cur, 0.0, elapsed - self.window_sec
        overlap = 1.0 - (into / self.window_sec)
        return cur + prev * max(0.0, overlap)

    def try_consume(self, now: float) -> Decision:
        self._roll(now)
        est = self.estimate(now)
        if est + 1.0 <= self.limit:
            self.current += 1
            return Decision(True, max(0.0, self.limit - est), 0.0, int(est + 0.5))
        excess = est + 1.0 - self.limit
        retry = ((excess / self.previous) * self.window_sec if self.previous
                 else self.window_sec - (now - self.window_start))
        return Decision(False, max(0.0, self.limit - est), max(0.0, retry), int(est + 0.5))


class PythonRegistry:
    """Striped-lock registry. API-compatible with sentinel_native.Registry.

    Striping is kept even though the GIL already serialises everything, because
    removing it would make the Python arm *artificially* slow and the benchmark
    would then prove nothing. The point is that striping helps the C++ version and
    cannot help this one — not that this one was handicapped.
    """

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"

    def __init__(self, algorithm: str, capacity: float, refill_per_sec: float,
                 stripes: int = 16) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_per_sec <= 0:
            raise ValueError("refill_per_sec must be > 0")
        if stripes <= 0:
            raise ValueError("stripes must be > 0")

        self.algorithm = algorithm
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self._stripes = stripes
        self._locks = [threading.Lock() for _ in range(stripes)]
        self._maps: List[Dict[str, object]] = [{} for _ in range(stripes)]
        self._allowed = 0
        self._denied = 0
        self._stat_lock = threading.Lock()

    def _stripe(self, key: str) -> int:
        return hash(key) % self._stripes

    def check(self, key: str, cost: float = 1.0) -> Decision:
        now = time.monotonic()
        idx = self._stripe(key)
        with self._locks[idx]:
            entry = self._maps[idx].get(key)
            if entry is None:
                if self.algorithm == self.TOKEN_BUCKET:
                    entry = _Bucket(self.capacity, self.refill_per_sec, now)
                else:
                    entry = _Window(int(self.capacity), 1.0, now)
                self._maps[idx][key] = entry
            decision = (entry.try_consume(cost, now) if isinstance(entry, _Bucket)
                        else entry.try_consume(now))

        # int += is not atomic under the GIL (LOAD_FAST/BINARY_OP/STORE_FAST can be
        # interrupted between bytecodes), so the counters genuinely need a lock.
        with self._stat_lock:
            if decision.allowed:
                self._allowed += 1
            else:
                self._denied += 1
        return decision

    def check_many(self, keys: List[str], cost: float = 1.0) -> List[Decision]:
        return [self.check(k, cost) for k in keys]

    def evict_idle(self, max_idle_sec: float) -> int:
        cutoff = time.monotonic() - max_idle_sec
        removed = 0
        for idx in range(self._stripes):
            with self._locks[idx]:
                stale = [
                    k for k, v in self._maps[idx].items()
                    if (v.last_refill if isinstance(v, _Bucket) else v.window_start) < cutoff
                ]
                for k in stale:
                    del self._maps[idx][k]
                removed += len(stale)
        return removed

    def size(self) -> int:
        total = 0
        for idx in range(self._stripes):
            with self._locks[idx]:
                total += len(self._maps[idx])
        return total

    def reset_stats(self) -> None:
        with self._stat_lock:
            self._allowed = 0
            self._denied = 0

    @property
    def total_allowed(self) -> int:
        return self._allowed

    @property
    def total_denied(self) -> int:
        return self._denied
