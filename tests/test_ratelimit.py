"""Rate limiter: the native core, the Python control arm, and that they AGREE.

The agreement tests are the important ones. Two implementations of the same
algorithm are only a valid benchmark comparison if they actually compute the same
thing; otherwise the speedup is measuring a difference in behaviour, not in speed.
"""

import threading
import time

import pytest

from gateway.ratelimit.limiter import Limiter, LimitPolicy, NATIVE_AVAILABLE
from gateway.ratelimit.python_bucket import PythonRegistry

try:
    import sentinel_native
except ImportError:
    sentinel_native = None

pytestmark = pytest.mark.filterwarnings("ignore")


def _registries(capacity=5.0, refill=10.0, stripes=4):
    """One of each backend, configured identically."""
    py = PythonRegistry(PythonRegistry.TOKEN_BUCKET, capacity, refill, stripes)
    if sentinel_native is None:
        return [("python", py)]
    nat = sentinel_native.Registry(sentinel_native.Algorithm.TOKEN_BUCKET,
                                   capacity, refill, stripes)
    return [("python", py), ("native", nat)]


@pytest.mark.parametrize("name,reg", _registries())
def test_capacity_is_the_burst_limit(name, reg):
    """A fresh bucket holds exactly `capacity` tokens, no more."""
    allowed = [reg.check("c").allowed for _ in range(8)]
    assert allowed[:5] == [True] * 5, f"{name}: first 5 should pass"
    assert allowed[5:] == [False] * 3, f"{name}: rest should be denied"


@pytest.mark.parametrize("name,reg", _registries())
def test_clients_are_isolated(name, reg):
    """One client exhausting its bucket must not affect another."""
    for _ in range(6):
        reg.check("noisy")
    assert reg.check("quiet").allowed, f"{name}: quiet client was punished for noisy one"


@pytest.mark.parametrize("name,reg", _registries(capacity=2.0, refill=100.0))
def test_tokens_refill_over_time(name, reg):
    assert reg.check("c").allowed and reg.check("c").allowed
    assert not reg.check("c").allowed
    time.sleep(0.05)                      # 100/s * 0.05s = 5 tokens, capped at 2
    assert reg.check("c").allowed, f"{name}: bucket did not refill"


@pytest.mark.parametrize("name,reg", _registries(capacity=1.0, refill=10.0))
def test_denied_requests_do_not_consume_tokens(name, reg):
    """A rejected request must not deepen the client's deficit.

    If denials charged tokens, a client already over its limit would hold itself over
    it indefinitely - a burst would become an outage for that client.
    """
    assert reg.check("c").allowed
    first = reg.check("c")
    second = reg.check("c")
    assert not first.allowed and not second.allowed
    assert second.retry_after <= first.retry_after + 1e-6, \
        f"{name}: retry_after grew while being denied"


@pytest.mark.parametrize("name,reg", _registries(capacity=3.0, refill=1.0))
def test_retry_after_is_honest(name, reg):
    """retry_after must be long enough that waiting it out actually works."""
    for _ in range(3):
        reg.check("c")
    d = reg.check("c")
    assert not d.allowed
    assert 0 < d.retry_after <= 1.01, f"{name}: implausible retry_after {d.retry_after}"


@pytest.mark.skipif(sentinel_native is None, reason="native extension not built")
def test_backends_agree_on_a_long_sequence():
    """Same inputs, same decisions. This is what makes the benchmark meaningful."""
    py = PythonRegistry(PythonRegistry.TOKEN_BUCKET, 10.0, 1e9, 8)
    nat = sentinel_native.Registry(sentinel_native.Algorithm.TOKEN_BUCKET, 10.0, 1e9, 8)
    keys = [f"c{i % 7}" for i in range(400)]
    # Huge refill so wall-clock differences between the two loops cannot change the
    # verdict; we are testing decision logic, not timing.
    assert [py.check(k).allowed for k in keys] == [nat.check(k).allowed for k in keys]


@pytest.mark.skipif(sentinel_native is None, reason="native extension not built")
def test_sliding_window_bounds_a_window():
    w = sentinel_native.SlidingWindowCounter(limit=5, window_sec=10.0)
    assert sum(w.try_consume().allowed for _ in range(20)) == 5


@pytest.mark.skipif(sentinel_native is None, reason="native extension not built")
def test_native_is_threadsafe_and_loses_nothing():
    """Concurrent hammering must not lose or double-count decisions."""
    reg = sentinel_native.Registry(sentinel_native.Algorithm.TOKEN_BUCKET, 1e9, 1e9, 16)
    per_thread, threads = 2000, 8

    def work():
        for i in range(per_thread):
            reg.check(f"c{i % 32}")

    ts = [threading.Thread(target=work) for _ in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert reg.total_allowed + reg.total_denied == per_thread * threads


@pytest.mark.skipif(sentinel_native is None, reason="native extension not built")
def test_eviction_bounds_memory():
    """Bucket count must not grow without bound - the keys are client-controlled."""
    reg = sentinel_native.Registry(sentinel_native.Algorithm.TOKEN_BUCKET, 5.0, 1.0, 4)
    for i in range(500):
        reg.check(f"client-{i}")
    assert reg.size() == 500
    time.sleep(0.05)
    assert reg.evict_idle(0.01) == 500
    assert reg.size() == 0


def test_policy_resolution_prefers_the_most_specific_match():
    lim = Limiter(LimitPolicy("default", 10, 1))
    lim.add_tier_policy("paid", LimitPolicy("tier-paid", 100, 10))
    lim.add_route_policy("/search", LimitPolicy("route-search", 5, 1))
    lim.add_route_tier_policy("/search", "paid", LimitPolicy("both", 50, 5))

    assert lim.resolve("/search", "paid").name == "both"
    assert lim.resolve("/search", "free").name == "route-search"
    assert lim.resolve("/other", "paid").name == "tier-paid"
    assert lim.resolve("/other", "free").name == "default"


def test_tiers_do_not_share_buckets():
    """The same client on two tiers must get two allowances, not one shared one."""
    lim = Limiter(LimitPolicy("default", 100, 10))
    lim.add_route_tier_policy("/x", "free", LimitPolicy("free", 2, 1))
    lim.add_route_tier_policy("/x", "paid", LimitPolicy("paid", 10, 1))

    assert [lim.check("c", "/x", "free").allowed for _ in range(4)] == [True, True, False, False]
    assert all(lim.check("c", "/x", "paid").allowed for _ in range(4)), \
        "paid tier was drained by the free tier"


def test_limiter_rejects_forcing_a_backend_it_does_not_have():
    if NATIVE_AVAILABLE:
        pytest.skip("native is available, cannot test the failure path")
    with pytest.raises(RuntimeError):
        Limiter(LimitPolicy("d", 1, 1), force_backend="native")
