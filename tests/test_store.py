"""Store: durability, concurrency and the queries the dashboard depends on."""

import threading
import time

from gateway.store import Store


def _event(client="c1", status=200, limited=False, flagged=False, score=0.1):
    return {"ts": time.time(), "client_id": client, "tier": "free", "method": "GET",
            "path": "/api/x", "status": status, "latency_ms": 4.0, "limited": limited,
            "anomaly_score": score, "flagged": flagged, "upstream": "u", "reason": "r"}


def test_in_memory_database_is_shared_across_connections():
    """REGRESSION: a plain ':memory:' path gives every connection its own private
    database, so a thread-local pool handed each thread an empty one and writes
    vanished. The shared-cache URI is what makes this work."""
    s = Store(":memory:")
    s.log_request(_event())
    got = {}

    def reader():
        got["total"] = s.summary()["total"]

    t = threading.Thread(target=reader)
    t.start()
    t.join()
    assert got["total"] == 1, "another thread could not see the write"


def test_concurrent_writers_lose_nothing():
    """REGRESSION: shared-cache SQLite takes a TABLE lock and returns SQLITE_LOCKED
    immediately, ignoring `timeout` - three writer threads produced 'database table
    is locked' and dropped rows. The write mutex fixes it."""
    s = Store(":memory:")
    threads, per = 6, 25

    def work(n):
        for _ in range(per):
            s.log_request(_event(client=f"t{n}"))

    ts = [threading.Thread(target=work, args=(i,)) for i in range(threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert s.summary()["total"] == threads * per


def test_summary_aggregates_correctly():
    s = Store(":memory:")
    for i in range(10):
        s.log_request(_event(client=f"c{i % 3}", status=429 if i % 5 == 0 else 200,
                             limited=i % 5 == 0, flagged=i % 4 == 0))
    summary = s.summary()
    assert summary["total"] == 10
    assert summary["limited"] == 2
    assert summary["flagged"] == 3
    assert summary["distinct_clients"] == 3


def test_client_history_is_newest_first_and_bounded():
    s = Store(":memory:")
    for _ in range(30):
        s.log_request(_event(client="c"))
    rows = s.client_history("c", limit=10)
    assert len(rows) == 10
    assert rows[0]["ts"] >= rows[-1]["ts"]


def test_explanations_join_to_their_request():
    s = Store(":memory:")
    rid = s.log_request(_event(flagged=True, score=0.9))
    s.log_explanation(rid, "offline", "template", False, 1.5, "because reasons",
                      {"request_rate": 12.0})
    row = s.explanation(rid)
    assert row["text"] == "because reasons"
    assert row["provider"] == "offline"
    assert s.explanation(999_999) is None


def test_top_clients_ranks_by_volume():
    s = Store(":memory:")
    for _ in range(5):
        s.log_request(_event(client="busy"))
    s.log_request(_event(client="quiet"))
    top = s.top_clients(10)
    assert top[0]["client_id"] == "busy"
    assert top[0]["requests"] == 5


def test_purge_removes_only_old_rows():
    s = Store(":memory:")
    old = _event(); old["ts"] = time.time() - 10_000
    s.log_request(old)
    s.log_request(_event())
    assert s.purge_before(time.time() - 5_000) == 1
    assert s.summary()["total"] == 1
