# Decisions — Sentinel Gateway

Every non-obvious choice, what else was on the table, and why it lost.

This is the file to study. Interviews ask *why this and not that*, and the answer to
"why a token bucket?" is worth more than the ability to retype the token bucket.

---

## D-01 · A C++ extension at all, and setuptools rather than CMake

**Chose:** a pybind11 extension for the limiter core, built by a 30-line
`setup.py`.

**Alternatives:**
- *Pure Python.* The honest baseline, and it is kept as `python_bucket.py` — not as a
  fallback, but as the control arm. Compiled is 2.45× faster at 8 threads.
- *Cython.* Would get most of the compiled win with less ceremony, but generates C
  from annotated Python, so the GIL semantics that this project exists to measure are
  a `nogil` block you write rather than a boundary you can point at.
- *Rust + PyO3.* Genuinely good and roughly equivalent in performance. Rejected
  because C++ is what the target JDs list (C++ 19%, Rust ~0%) and because
  `py::gil_scoped_release` is the most legible way to make the GIL boundary explicit.
- *CMake / scikit-build.* Correct for a library with dependencies. This is two
  translation units and zero dependencies; a build the reader can follow beats one
  that scales to a project we do not have.

**Cost:** the extension must be compiled. Mitigated: the gateway imports it lazily and
degrades to pure Python, and CI runs the whole suite both ways so the fallback is
proven rather than assumed.

## D-02 · Token bucket as the default, sliding window as an option

**Chose:** token bucket by default; both implemented.

**Why:** a token bucket allows a *burst* up to capacity and then a sustained rate. That
matches how APIs are actually used — a page load fires eight requests at once and then
nothing for a minute — and a limiter that rejects the eight is technically correct and
practically useless.

**Alternatives:**
- *Fixed-window counter.* One integer per client, trivially cheap. Rejected: a client
  can send `limit` requests at the end of one window and `limit` at the start of the
  next, sustaining 2× the limit across the boundary.
- *Sliding-window log.* Exact. Rejected: O(limit) timestamps per client, and the
  registry is keyed by client-controlled input, so exact means expensive at exactly the
  moment you are under attack.
- *Sliding-window counter.* Implemented and available. O(1) memory, fixes the boundary
  doubling by interpolating the previous window. Not the default because it forbids
  bursts, which is the wrong default for an API.
- *Leaky bucket.* Equivalent to a token bucket with capacity 1 for our purposes; the
  burst allowance is the whole feature.

## D-03 · The request path holds the GIL; only batches release it

**Chose:** `check_holding_gil` on the hot path.

**Why:** measured. Releasing the GIL per request is 0.08× — a 12× pessimisation —
because the release/reacquire round trip dwarfs a ~200ns critical section. Batching one
release over 256 operations recovers 15.24×.

**This is the reverse of the design it started from,** and it is in the code because
the benchmark said so, not because it was planned. The GIL-releasing entry point
survives for `check_batch` and as the experiment's control arm.

**Alternative considered:** free-threaded CPython (3.13t), where the question dissolves.
Rejected as a dependency — almost nothing deploys on it yet — but it is the honest
answer to "what would you do differently in two years."

## D-04 · Striped locks, 16 stripes

**Chose:** `hash(key) % 16` selecting one of 16 independent mutexes.

**Alternatives:**
- *One global mutex.* Simple, and hands back the exact serialisation the extension
  exists to remove.
- *Lock-free / atomic per bucket.* Faster in principle. Rejected: a token bucket
  refill is a read-modify-write across two fields (tokens and timestamp) and making
  that lock-free correctly is subtle enough that it would be the most likely place for
  a real bug to hide.
- *Per-key lock.* Unbounded lock objects keyed by client-controlled input — the
  memory-exhaustion vector we removed elsewhere, reintroduced.

**Why 16:** more stripes than typical core counts, so collisions are rare, while the
map overhead stays trivial. Configurable; not tuned further because the benchmark shows
the GIL, not lock contention, is the binding constraint.

## D-05 · Hand-rolled metrics instead of prometheus_client

**Chose:** a fixed-bucket histogram and counters in `telemetry.py`.

**Why:** the dashboard needs percentiles and a recent-events feed as JSON, which is a
different shape from a Prometheus scrape. Adapting a metrics stack to that would be
more code than the file it replaces. The histogram is the same fixed-bucket design a
Prometheus histogram uses, so the concept transfers even though the dependency does not.

**Trade accepted:** percentiles are bucket upper bounds, not exact quantiles — the
standard price of O(1) memory. Stated in the code where it is computed.

**Would change if:** this ever needed to federate into a real monitoring system. Then
`prometheus_client` wins immediately and this file should be deleted.

## D-06 · SQLite, WAL, one connection per thread

**Chose:** SQLite in WAL mode with a thread-local connection pool and a write mutex.

**Why WAL:** in the default rollback journal a writer blocks all readers. The gateway
writes on every request and the dashboard polls continuously, so the default would let
the dashboard stall the request path.

**Why `synchronous=NORMAL`:** these are observability records. Paying an fsync per
request so as never to lose a request *log* is the wrong trade; losing the last few on
power failure is acceptable and losing throughput is not.

**Why a write mutex on top:** SQLite serialises writers anyway, but differently per
journal mode — WAL makes a second writer wait out `timeout`, while shared-cache
in-memory takes a *table* lock and returns `SQLITE_LOCKED` immediately, ignoring
`timeout` entirely. The mutex makes both behave the same and costs nothing that was
not already being paid.

**Alternatives:** Postgres (adds a failure mode to the component whose job is to stay
up when things fail); append-only file (would need a query layer, i.e. SQLite);
in-memory only (loses history across restarts, which the anomaly baseline wants).

## D-07 · Features describe a *pattern*, not a request

**Chose:** ten statistics over a bounded per-client window.

**Why:** a single `GET /search` is never anomalous. Sixty of them in four seconds, at
identical spacing, across forty distinct paths, is. Anything computed from one request
cannot express that.

**Two constraints that eliminated most candidates:**
- Computable from what a gateway already sees. No request bodies — a gateway that
  parses payloads to decide whether to forward them is not a gateway.
- Interpretable. An unexplainable "this looked weird" is useless to an on-call
  engineer, and the explanation layer needs something concrete to talk about.

**Notable inclusion:** `regularity`, not just `burstiness`. Humans are irregular;
schedulers are metronomic. Low variance in inter-arrival time is as suspicious as high,
and it is the feature that catches a *slow* scraper staying under the rate limit.

## D-08 · Robust z-score below 50 clients, IsolationForest above

**Chose:** MAD-based robust z-score as the primary detector at small population; the
forest gated behind `min_clients_for_forest=50`.

**Why:** measured, and it inverted the original plan. Fitted on 5 clients the forest
gave two *different* anomalies the identical saturated score (0.8049) and a normal
client 0.7607 — a 0.04 margin with a false positive. The z-score separated the same
data by orders of magnitude. A forest isolates a point from a population; five points
is not a population.

**Why MAD, not mean and standard deviation:** a single aggressive client inflates a
standard deviation enough to hide inside it — the estimator gets corrupted by the
outlier it exists to catch. The median absolute deviation has a 50% breakdown point.

**The trap MAD brings, and the fix:** when most clients cluster tightly, MAD → 0 and
every deviation divided by it explodes. Both shapes of this bit us: a near-zero median
(relative floor, 25% of the median) and an exactly-zero median (absolute floor, 0.02).
Without the second, reason strings read `1000000000.0 MAD`.

**Alternatives:** supervised classification (needs labels nobody has in real time);
LOF (needs a distance metric over mixed-scale features, so it needs scaling the forest
does not); fixed thresholds per feature (no notion of what is normal *for this
gateway*, which is the entire question).

## D-09 · Explanations are optional, deadlined, cached, and off the request path

**Chose:** template by default, LLM opt-in, generated after the response is sent.

**Four rules, each from a failure it prevents:**
- *Never block the response.* Waiting on a language model before returning a 429 turns
  an 8ms rejection into a 900ms one.
- *Deadline everything.* An explanation after the incident is worthless; slow is the
  same as absent.
- *Cache on the anomaly's shape, not the request id.* Deviations are bucketed, so 12.3
  MAD and 12.9 MAD share an explanation — they mean the same thing to a reader, and
  keying on the raw float would make the cache useless.
- *Degrade, never fail.* Any provider error falls back to the template and increments
  a counter. An observability feature must not be able to take down what it observes.

**Why the template is not just a fallback:** it is the control. If a generated sentence
is not more useful than a templated one, the model is not earning its latency, and the
honest thing is to be able to tell.

**Why urllib rather than the SDK:** one HTTP POST does not justify a dependency in an
image that should stay small, and it keeps the provider boundary obvious.

## D-10 · Circuit breaker with a single-probe half-open state

**Chose:** CLOSED → OPEN after N consecutive failures → HALF_OPEN after a timer →
exactly one probe decides.

**Why a breaker:** retrying into a failing service is how a partial outage becomes a
total one. Every retry adds load to the thing that is struggling and fills the
gateway's own worker pool with requests waiting on timeouts they will not survive. The
breaker converts a slow, expensive failure into a fast, cheap one.

**Why HALF_OPEN matters:** without it, recovery is decided by a thundering herd
arriving the instant the timer expires — which re-kills a service that had just come
back. One probe.

**Why *consecutive* failures:** intermittent failures over hours of healthy traffic
must not accumulate into a trip. A success resets the run.

**Retry policy:** only idempotent methods (GET/HEAD/OPTIONS/PUT/DELETE), only on 5xx
and connection errors, with exponential backoff. POST is never retried — replaying it
can create two orders. 4xx is never retried — it will be just as wrong the second time.

## D-11 · React over CDN, no build step

**Chose:** one HTML file, React UMD from cdnjs, Babel in the browser.

**Why:** this is a single operator-facing page served by the gateway itself. A bundler
would mean the gateway's container needs a Node toolchain to produce one file, and the
Dockerfile gains a stage that exists purely to satisfy the frontend.

**Cost:** ~200ms of first-paint compile. Irrelevant for a page an on-call engineer
leaves open for hours.

**Would change if:** the dashboard grew past a few components, or shipped to end users
rather than operators.

## D-12 · Traffic generation lives in the browser, not in a server endpoint

**Chose:** the dashboard's demo buttons drive `fetch()` from the client.

**Why:** the obvious alternative is `POST /admin/replay` that makes the gateway
generate its own load. That is a server-side request-forgery surface on the ops console
of an internet-facing service — an endpoint whose whole purpose is "make this server
issue requests I specify." Driving it from the browser uses the same public path any
real client would, and adds no attack surface at all.

## D-13 · Identity is the API key, with the peer IP only as a fallback

**Why:** behind any load balancer every request shares one source IP. Rate limiting by
it would throttle every customer together the moment one of them misbehaves — one
noisy tenant taking down the rest is the exact outcome a rate limiter exists to prevent.

**Consequence, stated honestly:** an unauthenticated caller is limited by IP, so the
same weakness applies to anonymous traffic. Real deployments should require a key on
anything worth protecting.

## D-14 · Flag on the sustained score, not on this request's score

**Chose:** flag when the median of the last 8 scores crosses the threshold, and only
once a full window exists.

**Why:** measured. Per-request flagging gave a 0.0908 separation margin and one false
positive; the sustained median gave 0.5476 and none, on identical traffic. A cold
gateway's baseline is unstable and a maximum over time always finds that transient. A
client that is genuinely misbehaving stays elevated.

**Why also require a full window:** without it, a client's first score is its own
median, and the guard does nothing for precisely the requests it exists to protect.

**Cost:** detection is delayed by up to 8 requests from that client. For a scanner
issuing 40/second that is 200ms, which is an acceptable price for removing false
positives entirely.
