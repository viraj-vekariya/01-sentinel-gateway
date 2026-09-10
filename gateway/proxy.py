"""Upstream routing: forwarding, retries, and a circuit breaker.

The gateway's job once a request survives rate limiting and scoring is to get it to
the right backend and to fail well when that backend is unhealthy.

**Why a circuit breaker at all.** Retrying into a service that is already failing is
how a partial outage becomes a total one: every retry adds load to the thing that is
struggling, and the gateway's own worker pool fills with requests waiting on timeouts
it will not survive. The breaker converts a slow, resource-consuming failure into a
fast, cheap one.

**The three states.** CLOSED forwards normally. After `breaker_threshold` consecutive
failures it moves to OPEN and rejects immediately for `breaker_reset_sec`. It then
moves to HALF_OPEN and allows exactly one probe: success closes it, failure re-opens
it for another interval. HALF_OPEN matters - without it, recovery is decided by a
thundering herd of traffic arriving the instant the timer expires.

See DECISIONS.md D-10.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional, Tuple

import httpx

from .config import UpstreamConfig

log = logging.getLogger("sentinel.proxy")

# Headers that must not be copied verbatim between hops. Hop-by-hop headers are
# defined by RFC 9110 as applying to a single connection; forwarding them corrupts
# framing (Transfer-Encoding) or advertises a connection the next hop does not have.
HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host", "content-length",
})


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class ProxyResult:
    status: int
    body: bytes
    headers: Dict[str, str]
    upstream: str
    latency_ms: float
    attempts: int
    breaker: str
    error: str = ""


class CircuitBreaker:
    def __init__(self, threshold: int, reset_sec: float) -> None:
        self.threshold = threshold
        self.reset_sec = reset_sec
        self.state = BreakerState.CLOSED
        self.consecutive_failures = 0
        self.opened_at = 0.0
        self.trips = 0
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            if self.state is BreakerState.CLOSED:
                return True
            if self.state is BreakerState.OPEN:
                if time.monotonic() - self.opened_at >= self.reset_sec:
                    self.state = BreakerState.HALF_OPEN
                    return True     # exactly one probe gets through
                return False
            # HALF_OPEN: a probe is already in flight, hold everyone else back.
            return False

    async def record(self, ok: bool) -> None:
        async with self._lock:
            if ok:
                self.consecutive_failures = 0
                self.state = BreakerState.CLOSED
                return
            self.consecutive_failures += 1
            if self.state is BreakerState.HALF_OPEN or \
               self.consecutive_failures >= self.threshold:
                if self.state is not BreakerState.OPEN:
                    self.trips += 1
                self.state = BreakerState.OPEN
                self.opened_at = time.monotonic()

    def snapshot(self) -> Dict[str, object]:
        return {
            "state": self.state.value,
            "consecutive_failures": self.consecutive_failures,
            "trips": self.trips,
            "reset_in_sec": (round(max(0.0, self.reset_sec - (time.monotonic() - self.opened_at)), 1)
                             if self.state is BreakerState.OPEN else 0.0),
        }


class Proxy:
    """Owns one HTTP client and one breaker per upstream."""

    def __init__(self, upstreams: list[UpstreamConfig], max_retries: int = 2) -> None:
        self.upstreams = {u.name: u for u in upstreams}
        self.max_retries = max_retries
        self.breakers = {u.name: CircuitBreaker(u.breaker_threshold, u.breaker_reset_sec)
                         for u in upstreams}
        # One shared AsyncClient with a connection pool. Creating a client per request
        # would open a new TCP (and TLS) connection every time and is the single most
        # common way a Python proxy ends up slower than the service behind it.
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=3.0),
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
            follow_redirects=False,   # a gateway forwards redirects, it does not chase them
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @staticmethod
    def _clean_headers(headers: Dict[str, str]) -> Dict[str, str]:
        return {k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP}

    @staticmethod
    def _retryable(method: str, status: Optional[int], exc: Optional[Exception]) -> bool:
        """Retry only what is safe to retry.

        Idempotency is the rule: replaying a POST can create two orders. 5xx and
        connection errors are retryable for idempotent methods only; 4xx never is,
        because the request will be just as wrong the second time.
        """
        if method.upper() not in ("GET", "HEAD", "OPTIONS", "PUT", "DELETE"):
            return False
        if exc is not None:
            return True
        return status is not None and status >= 500

    async def forward(self, upstream: UpstreamConfig, method: str, path: str,
                      headers: Dict[str, str], body: bytes,
                      query: str = "") -> ProxyResult:
        assert self._client is not None, "Proxy.start() was not awaited"
        breaker = self.breakers[upstream.name]
        started = time.perf_counter()

        if not await breaker.allow():
            return ProxyResult(
                503, b'{"error":"upstream_unavailable","detail":"circuit breaker open"}',
                {"content-type": "application/json"}, upstream.name,
                (time.perf_counter() - started) * 1000, 0, breaker.state.value,
                error="circuit_open")

        url = f"{upstream.base_url}{path}" + (f"?{query}" if query else "")
        clean = self._clean_headers(headers)
        attempts = 0
        last_error = ""

        for attempt in range(self.max_retries + 1):
            attempts = attempt + 1
            try:
                resp = await self._client.request(
                    method, url, headers=clean, content=body or None,
                    timeout=upstream.timeout_sec)
                if self._retryable(method, resp.status_code, None) and attempt < self.max_retries:
                    # Exponential backoff with a small base. Without backoff, retries
                    # arrive while the upstream is still in whatever state caused the
                    # first failure.
                    await asyncio.sleep(0.05 * (2 ** attempt))
                    last_error = f"upstream {resp.status_code}"
                    continue
                await breaker.record(resp.status_code < 500)
                return ProxyResult(
                    resp.status_code, resp.content, self._clean_headers(dict(resp.headers)),
                    upstream.name, (time.perf_counter() - started) * 1000,
                    attempts, breaker.state.value)

            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout,
                    httpx.RemoteProtocolError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if self._retryable(method, None, exc) and attempt < self.max_retries:
                    await asyncio.sleep(0.05 * (2 ** attempt))
                    continue
                await breaker.record(False)
                return ProxyResult(
                    502, b'{"error":"bad_gateway","detail":"upstream did not respond"}',
                    {"content-type": "application/json"}, upstream.name,
                    (time.perf_counter() - started) * 1000, attempts,
                    breaker.state.value, error=last_error)

        await breaker.record(False)
        return ProxyResult(
            502, b'{"error":"bad_gateway","detail":"retries exhausted"}',
            {"content-type": "application/json"}, upstream.name,
            (time.perf_counter() - started) * 1000, attempts,
            breaker.state.value, error=last_error)

    def snapshot(self) -> Dict[str, object]:
        return {name: {"base_url": self.upstreams[name].base_url, **b.snapshot()}
                for name, b in self.breakers.items()}
