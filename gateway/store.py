"""Durable event store, SQLite.

Two consumers with different needs:
  * the dashboard wants the last N events fast -> that is served from the in-memory
    ring in telemetry.py, not from here;
  * anomaly scoring and the ops view want *history* across restarts -> that is this.

SQLite in WAL mode, one connection per thread. WAL matters: in the default rollback
journal a writer blocks all readers, and the gateway writes on every request while
the dashboard polls continuously, so the default mode would make the dashboard stall
the request path. See DECISIONS.md D-06.
"""

from __future__ import annotations

import itertools
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Distinguishes one in-memory database from another. A shared-cache URI is keyed by
# NAME, so a fixed name would make every Store(":memory:") in a process attach to the
# same database - which is right for threads inside one gateway and wrong for two
# independent instances. The test suite found this immediately: separate Stores in
# separate tests saw each other's rows and the counts were nonsense.
_MEM_SEQ = itertools.count()

SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL    NOT NULL,
    client_id     TEXT    NOT NULL,
    tier          TEXT    NOT NULL,
    method        TEXT    NOT NULL,
    path          TEXT    NOT NULL,
    status        INTEGER NOT NULL,
    latency_ms    REAL    NOT NULL,
    limited       INTEGER NOT NULL,
    anomaly_score REAL    NOT NULL,
    flagged       INTEGER NOT NULL,
    upstream      TEXT    NOT NULL DEFAULT '',
    reason        TEXT    NOT NULL DEFAULT ''
);

-- The dashboard's hot query is "recent events, newest first", and the anomaly
-- feature extractor's is "this client's recent events". Both are covered here.
CREATE INDEX IF NOT EXISTS idx_requests_ts     ON requests(ts DESC);
CREATE INDEX IF NOT EXISTS idx_requests_client ON requests(client_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_requests_flag   ON requests(flagged, ts DESC);

CREATE TABLE IF NOT EXISTS explanations (
    request_id  INTEGER PRIMARY KEY REFERENCES requests(id) ON DELETE CASCADE,
    ts          REAL NOT NULL,
    provider    TEXT NOT NULL,
    model       TEXT NOT NULL,
    cached      INTEGER NOT NULL,
    latency_ms  REAL NOT NULL,
    text        TEXT NOT NULL,
    features    TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str) -> None:
        # ":memory:" gives every *connection* its own private database, so a
        # thread-local connection pool over a plain ":memory:" path would hand each
        # thread an empty, separate db - which is exactly the bug the first test run
        # of this file hit. The shared-cache URI makes one in-memory database that
        # all connections in the process attach to, which is what callers mean.
        # `uri=True` is required for the connect() call to parse it as a URI at all.
        self.uri = path.startswith("file:") or path == ":memory:"
        if path == ":memory:":
            path = f"file:sentinel-mem-{next(_MEM_SEQ)}?mode=memory&cache=shared"
        self.path = path
        if not self.uri:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # SQLite serialises writers anyway, but it does so differently per journal
        # mode: a WAL file database makes a second writer wait out `timeout`, while a
        # shared-cache in-memory database takes a TABLE lock and returns
        # SQLITE_LOCKED immediately, ignoring `timeout` entirely. The second test run
        # of this file hit exactly that ("database table is locked") under three
        # concurrent writer threads. Holding our own write mutex makes the two modes
        # behave identically and costs nothing we were not already paying, since no
        # journal mode would have let those writes proceed in parallel.
        self._write_lock = threading.Lock()
        # Keep one connection alive for the process lifetime. For the shared in-memory
        # database this is load-bearing: the db is destroyed when the last connection
        # to it closes, so without this anchor it would vanish between requests.
        self._anchor = self._connect()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0, check_same_thread=False,
                               uri=self.uri)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        # NORMAL rather than FULL: we accept losing the last few request logs if the
        # machine loses power. These are observability records, not transactions -
        # paying an fsync per request to never lose one would be the wrong trade.
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        # Idempotent (every statement is IF NOT EXISTS), so running it per connection
        # costs one cheap catalogue check and removes an ordering dependency between
        # "who created the schema" and "who opened first".
        conn.executescript(SCHEMA)
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        """Per-thread connection. SQLite connections are not safe to share."""
        c = getattr(self._local, "conn", None)
        if c is None:
            c = self._local.conn = self._connect()
        return c

    # -- writes --------------------------------------------------------------

    def log_request(self, event: Dict[str, Any]) -> int:
        with self._write_lock:
            cur = self.conn.execute(
                """INSERT INTO requests
                   (ts, client_id, tier, method, path, status, latency_ms,
                    limited, anomaly_score, flagged, upstream, reason)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (event["ts"], event["client_id"], event["tier"], event["method"],
                 event["path"], event["status"], event["latency_ms"],
                 int(event["limited"]), event["anomaly_score"], int(event["flagged"]),
                 event.get("upstream", ""), event.get("reason", "")),
            )
            self.conn.commit()
            return int(cur.lastrowid)

    def log_explanation(self, request_id: int, provider: str, model: str, cached: bool,
                        latency_ms: float, text: str, features: Dict[str, float]) -> None:
        with self._write_lock:
            self.conn.execute(
                """INSERT OR REPLACE INTO explanations
                   (request_id, ts, provider, model, cached, latency_ms, text, features)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (request_id, time.time(), provider, model, int(cached), latency_ms,
                 text, json.dumps(features)),
            )
            self.conn.commit()

    # -- reads ---------------------------------------------------------------

    def client_history(self, client_id: str, limit: int = 200) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM requests WHERE client_id = ? ORDER BY ts DESC LIMIT ?",
            (client_id, limit),
        ).fetchall()

    def recent(self, limit: int = 100, only_flagged: bool = False) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM requests"
        if only_flagged:
            sql += " WHERE flagged = 1"
        sql += " ORDER BY ts DESC LIMIT ?"
        return [dict(r) for r in self.conn.execute(sql, (limit,)).fetchall()]

    def explanation(self, request_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM explanations WHERE request_id = ?", (request_id,)).fetchone()
        return dict(row) if row else None

    def summary(self) -> Dict[str, Any]:
        row = self.conn.execute(
            """SELECT COUNT(*)              AS total,
                      SUM(limited)          AS limited,
                      SUM(flagged)          AS flagged,
                      AVG(latency_ms)       AS avg_latency,
                      COUNT(DISTINCT client_id) AS clients
               FROM requests"""
        ).fetchone()
        return {
            "total": row["total"] or 0,
            "limited": row["limited"] or 0,
            "flagged": row["flagged"] or 0,
            "avg_latency_ms": round(row["avg_latency"] or 0.0, 2),
            "distinct_clients": row["clients"] or 0,
        }

    def top_clients(self, limit: int = 10) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT client_id, tier, COUNT(*) AS requests,
                      SUM(limited) AS limited, SUM(flagged) AS flagged,
                      AVG(anomaly_score) AS avg_score
               FROM requests GROUP BY client_id, tier
               ORDER BY requests DESC LIMIT ?""", (limit,)).fetchall()
        return [
            {"client_id": r["client_id"], "tier": r["tier"], "requests": r["requests"],
             "limited": r["limited"], "flagged": r["flagged"],
             "avg_score": round(r["avg_score"] or 0.0, 4)}
            for r in rows
        ]

    def purge_before(self, cutoff_ts: float) -> int:
        with self._write_lock:
            cur = self.conn.execute("DELETE FROM requests WHERE ts < ?", (cutoff_ts,))
            self.conn.commit()
            return cur.rowcount
