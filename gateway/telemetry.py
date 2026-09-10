"""In-process metrics: counters, latency histograms, and a rolling event ring.

Why hand-rolled instead of prometheus_client: the dashboard needs percentiles and a
recent-events feed over HTTP as JSON, which is a different shape from a Prometheus
scrape endpoint, and pulling a metrics stack in to then adapt its output would be
more code than this file, not less. The histogram below is the standard fixed-bucket
design a Prometheus histogram uses, so the concept transfers even though the
dependency does not. See DECISIONS.md D-05.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

# Bucket edges in milliseconds. Log-ish spacing because latency distributions are
# long-tailed: linear buckets would put almost everything in bucket 0 and tell us
# nothing about the tail, which is the part that matters for an SLA.
LATENCY_BUCKETS_MS = (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000)


class Histogram:
    """Fixed-bucket cumulative histogram with an exact running sum and count."""

    __slots__ = ("buckets", "counts", "total", "sum_ms", "_lock", "_max", "_min")

    def __init__(self, buckets=LATENCY_BUCKETS_MS) -> None:
        self.buckets = tuple(buckets)
        self.counts = [0] * (len(self.buckets) + 1)  # +1 for the overflow bucket
        self.total = 0
        self.sum_ms = 0.0
        self._max = 0.0
        self._min: Optional[float] = None
        self._lock = threading.Lock()

    def observe(self, value_ms: float) -> None:
        with self._lock:
            idx = len(self.buckets)
            for i, edge in enumerate(self.buckets):
                if value_ms <= edge:
                    idx = i
                    break
            self.counts[idx] += 1
            self.total += 1
            self.sum_ms += value_ms
            if value_ms > self._max:
                self._max = value_ms
            if self._min is None or value_ms < self._min:
                self._min = value_ms

    def percentile(self, p: float) -> float:
        """Bucket-interpolated percentile.

        Returns the bucket's upper edge, so this is an upper bound rather than an
        exact quantile - the standard trade a bucketed histogram makes for O(1)
        memory. Callers that need exactness should use the events ring instead.
        """
        with self._lock:
            if self.total == 0:
                return 0.0
            target = p / 100.0 * self.total
            cumulative = 0
            for i, c in enumerate(self.counts):
                cumulative += c
                if cumulative >= target:
                    return float(self.buckets[i]) if i < len(self.buckets) else self._max
            return self._max

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            total, sum_ms = self.total, self.sum_ms
            mn, mx = self._min or 0.0, self._max
        return {
            "count": total,
            "mean_ms": round(sum_ms / total, 3) if total else 0.0,
            "min_ms": round(mn, 3),
            "max_ms": round(mx, 3),
            "p50_ms": round(self.percentile(50), 3),
            "p95_ms": round(self.percentile(95), 3),
            "p99_ms": round(self.percentile(99), 3),
        }


@dataclass
class RequestEvent:
    """One request as the dashboard sees it."""

    ts: float
    client_id: str
    tier: str
    method: str
    path: str
    status: int
    latency_ms: float
    limited: bool
    anomaly_score: float
    flagged: bool
    upstream: str = ""
    reason: str = ""

    def as_dict(self) -> Dict[str, object]:
        return {
            "ts": round(self.ts, 3),
            "client_id": self.client_id,
            "tier": self.tier,
            "method": self.method,
            "path": self.path,
            "status": self.status,
            "latency_ms": round(self.latency_ms, 2),
            "limited": self.limited,
            "anomaly_score": round(self.anomaly_score, 4),
            "flagged": self.flagged,
            "upstream": self.upstream,
            "reason": self.reason,
        }


class Telemetry:
    """Everything the /metrics and /events endpoints serve."""

    def __init__(self, ring_size: int = 500) -> None:
        self.started = time.time()
        self.counters: Dict[str, int] = {}
        self.latency = Histogram()
        self.upstream_latency: Dict[str, Histogram] = {}
        # Bounded ring: the dashboard only ever shows recent traffic, and an unbounded
        # list here would be a slow memory leak in a long-running process.
        self.events: Deque[RequestEvent] = deque(maxlen=ring_size)
        self._lock = threading.Lock()

    def incr(self, name: str, by: int = 1) -> None:
        with self._lock:
            self.counters[name] = self.counters.get(name, 0) + by

    def observe_latency(self, ms: float, upstream: str = "") -> None:
        self.latency.observe(ms)
        if upstream:
            with self._lock:
                h = self.upstream_latency.get(upstream)
                if h is None:
                    h = self.upstream_latency[upstream] = Histogram()
            h.observe(ms)

    def record(self, event: RequestEvent) -> None:
        with self._lock:
            self.events.append(event)
        self.incr("requests_total")
        self.incr(f"status_{event.status // 100}xx")
        if event.limited:
            self.incr("rate_limited_total")
        if event.flagged:
            self.incr("flagged_total")
        self.observe_latency(event.latency_ms, event.upstream)

    def recent(self, limit: int = 50, only_flagged: bool = False) -> List[Dict[str, object]]:
        with self._lock:
            items = list(self.events)
        if only_flagged:
            items = [e for e in items if e.flagged]
        return [e.as_dict() for e in items[-limit:][::-1]]

    def snapshot(self) -> Dict[str, object]:
        with self._lock:
            counters = dict(self.counters)
            upstreams = {k: h.snapshot() for k, h in self.upstream_latency.items()}
        total = counters.get("requests_total", 0)
        return {
            "uptime_sec": round(time.time() - self.started, 1),
            "counters": counters,
            "latency": self.latency.snapshot(),
            "upstreams": upstreams,
            "rate_limited_pct": round(
                100.0 * counters.get("rate_limited_total", 0) / total, 2) if total else 0.0,
            "flagged_pct": round(
                100.0 * counters.get("flagged_total", 0) / total, 2) if total else 0.0,
        }
