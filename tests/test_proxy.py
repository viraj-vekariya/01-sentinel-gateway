"""Proxy: header hygiene, retry safety, and the circuit-breaker state machine."""

import asyncio

import pytest

from gateway.proxy import HOP_BY_HOP, BreakerState, CircuitBreaker, Proxy


def test_hop_by_hop_headers_are_stripped():
    """Forwarding these corrupts framing or advertises a connection the next hop
    does not have. RFC 9110 defines them as single-connection only."""
    dirty = {"Host": "a", "Transfer-Encoding": "chunked", "Connection": "keep-alive",
             "Content-Length": "12", "X-Api-Key": "keep", "Accept": "keep"}
    clean = Proxy._clean_headers(dirty)
    assert set(clean) == {"X-Api-Key", "Accept"}
    assert all(h in HOP_BY_HOP for h in ("host", "transfer-encoding", "connection"))


@pytest.mark.parametrize("method,status,expected", [
    ("GET", 500, True), ("GET", 503, True), ("HEAD", 500, True),
    ("PUT", 500, True), ("DELETE", 500, True),
    ("POST", 500, False),        # not idempotent: a replay could create two orders
    ("PATCH", 500, False),
    ("GET", 404, False),         # will be just as wrong the second time
    ("GET", 200, False),
])
def test_only_idempotent_methods_retry(method, status, expected):
    assert Proxy._retryable(method, status, None) is expected


def test_connection_errors_retry_for_idempotent_methods_only():
    exc = ConnectionError("refused")
    assert Proxy._retryable("GET", None, exc) is True
    assert Proxy._retryable("POST", None, exc) is False


def test_breaker_opens_after_consecutive_failures():
    async def run():
        b = CircuitBreaker(threshold=3, reset_sec=10)
        assert await b.allow()
        for _ in range(2):
            await b.record(False)
        assert b.state is BreakerState.CLOSED, "opened too early"
        await b.record(False)
        assert b.state is BreakerState.OPEN
        assert not await b.allow(), "open breaker still forwarding"
    asyncio.run(run())


def test_a_success_resets_the_failure_run():
    """Consecutive means consecutive. Intermittent failures must not accumulate into
    a trip over hours of healthy traffic."""
    async def run():
        b = CircuitBreaker(threshold=3, reset_sec=10)
        await b.record(False)
        await b.record(False)
        await b.record(True)
        await b.record(False)
        await b.record(False)
        assert b.state is BreakerState.CLOSED
    asyncio.run(run())


def test_half_open_admits_exactly_one_probe():
    """Without this, recovery is decided by a thundering herd at the instant the
    timer expires."""
    async def run():
        b = CircuitBreaker(threshold=1, reset_sec=0.05)
        await b.record(False)
        assert b.state is BreakerState.OPEN
        await asyncio.sleep(0.06)
        assert await b.allow(), "never left OPEN"
        assert b.state is BreakerState.HALF_OPEN
        assert not await b.allow(), "let a second probe through"
    asyncio.run(run())


def test_a_failed_probe_reopens_the_breaker():
    async def run():
        b = CircuitBreaker(threshold=1, reset_sec=0.05)
        await b.record(False)
        await asyncio.sleep(0.06)
        await b.allow()                        # -> HALF_OPEN
        await b.record(False)
        assert b.state is BreakerState.OPEN
        assert b.trips == 2
    asyncio.run(run())


def test_a_successful_probe_closes_the_breaker():
    async def run():
        b = CircuitBreaker(threshold=1, reset_sec=0.05)
        await b.record(False)
        await asyncio.sleep(0.06)
        await b.allow()
        await b.record(True)
        assert b.state is BreakerState.CLOSED
        assert await b.allow()
    asyncio.run(run())
