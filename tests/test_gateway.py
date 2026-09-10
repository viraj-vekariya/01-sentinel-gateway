"""Integration: the gateway through its real HTTP surface.

These use FastAPI's TestClient against a real app instance with an isolated in-memory
store. No upstream is running, so proxied calls fail - which is deliberate: it lets
these tests assert the gateway's OWN behaviour (limiting, routing, headers, breaker)
without depending on a second process being alive.
"""

import pytest
from fastapi.testclient import TestClient

from gateway.config import LimitPolicy, Settings, UpstreamConfig
from gateway.main import create_app, identify

# Ports nothing is listening on. Pinned explicitly because an earlier version of this
# fixture relied on "no upstream happens to be running", and the tests duly failed on
# a machine where the demo services WERE running. A test that depends on ambient
# machine state is not a test.
DEAD_PY, DEAD_JAVA = 59_101, 59_102


@pytest.fixture()
def client():
    cfg = Settings(db_path=":memory:", upstreams=[
        UpstreamConfig("catalog-py", f"http://127.0.0.1:{DEAD_PY}", "/api/catalog",
                       timeout_sec=0.25, breaker_threshold=5, breaker_reset_sec=5.0),
        UpstreamConfig("pricing-java", f"http://127.0.0.1:{DEAD_JAVA}", "/api/pricing",
                       timeout_sec=0.25, breaker_threshold=5, breaker_reset_sec=5.0),
    ])
    cfg.tier_policies["test"] = LimitPolicy("test-tier", capacity=5.0, refill_per_sec=0.5)
    with TestClient(create_app(cfg)) as c:
        yield c


def _hdr(key="k1", tier="test"):
    return {"x-api-key": key, "x-tier": tier}


def test_health_reports_the_active_components(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["limiter_backend"] in ("native", "python")
    assert set(body["upstreams"]) == {"catalog-py", "pricing-java"}


def test_rate_limit_returns_429_with_actionable_headers(client):
    codes = [client.get("/api/catalog/items", headers=_hdr()).status_code for _ in range(12)]
    assert 429 in codes, "limiter never engaged"

    r = client.get("/api/catalog/items", headers=_hdr())
    assert r.status_code == 429
    # A 429 without retry-after tells the caller to guess, and callers guess badly.
    assert "retry-after" in r.headers
    assert int(r.headers["retry-after"]) >= 1
    assert r.headers["x-ratelimit-limit"] == "5"
    assert r.json()["error"] == "rate_limited"


def test_limiting_happens_before_any_upstream_io(client):
    """A rejected request must not cost a socket. If limiting ran after routing, a
    throttled client could still exhaust the connection pool."""
    for _ in range(12):
        client.get("/api/catalog/items", headers=_hdr())
    r = client.get("/api/catalog/items", headers=_hdr())
    assert r.status_code == 429
    assert "x-sentinel-upstream" not in r.headers, "upstream was contacted anyway"


def test_separate_clients_get_separate_allowances(client):
    for _ in range(12):
        client.get("/api/catalog/items", headers=_hdr("noisy"))
    assert client.get("/api/catalog/items", headers=_hdr("quiet")).status_code != 429


def test_unknown_prefix_is_404_not_a_proxy_attempt(client):
    r = client.get("/api/nothing/here", headers=_hdr("u", "paid"))
    assert r.status_code == 404
    assert r.json()["error"] == "no_route"


def test_requests_route_to_the_right_upstream(client):
    """No upstream is running, so both fail - but they must fail having chosen the
    CORRECT backend, which the header records."""
    a = client.get("/api/catalog/items", headers=_hdr("r1", "paid"))
    b = client.get("/api/pricing/quote?itemId=1", headers=_hdr("r2", "paid"))
    assert a.headers.get("x-sentinel-upstream") == "catalog-py"
    assert b.headers.get("x-sentinel-upstream") == "pricing-java"
    assert a.status_code in (502, 503) and b.status_code in (502, 503)


def test_a_dead_upstream_trips_its_breaker_and_not_the_other(client):
    for _ in range(10):
        client.get("/api/catalog/items", headers=_hdr("b1", "paid"))
    breakers = client.get("/metrics").json()["breakers"]
    assert breakers["catalog-py"]["state"] == "open"
    assert breakers["pricing-java"]["state"] == "closed", "unrelated upstream was tripped"


def test_metrics_expose_every_subsystem(client):
    client.get("/api/catalog/items", headers=_hdr("m1", "paid"))
    m = client.get("/metrics").json()
    for section in ("telemetry", "limiter", "anomaly", "explainer", "breakers", "store"):
        assert section in m, f"missing {section}"
    assert m["telemetry"]["counters"]["requests_total"] >= 1


def test_events_feed_records_traffic(client):
    for i in range(4):
        client.get("/api/catalog/items", headers=_hdr(f"e{i}", "paid"))
    events = client.get("/events?limit=20").json()["events"]
    assert len(events) >= 4
    assert {"client_id", "path", "status", "anomaly_score", "flagged"} <= set(events[0])


def test_explanation_lookup_404s_for_an_unknown_request(client):
    assert client.get("/explain/999999").status_code == 404


def test_dashboard_is_served_at_root(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "Sentinel" in r.text


def test_identity_falls_back_to_peer_ip_without_a_key():
    """Identity must never be None; an unauthenticated caller still needs a bucket."""
    class _Req:
        headers = {}
        class client:
            host = "10.0.0.9"
    cid, tier = identify(_Req())
    assert cid == "10.0.0.9" and tier == "default"


def test_api_key_beats_peer_ip_for_identity():
    """Behind a load balancer every request shares one IP; limiting by it would
    throttle all customers together."""
    class _Req:
        headers = {"x-api-key": "customer-7", "x-tier": "paid"}
        class client:
            host = "10.0.0.9"
    cid, tier = identify(_Req())
    assert cid == "customer-7" and tier == "paid"
