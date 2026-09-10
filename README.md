# Sentinel Gateway

**[▶ Live demo](https://viraj-vekariya.github.io/01-sentinel-gateway/)** — the token bucket runs in your browser, [verified against the compiled C++ extension](tools/check_js_matches_native.py).

**[⇧ Deploy it yourself](https://render.com/deploy?repo=https://github.com/viraj-vekariya/01-sentinel-gateway)** — one click builds `render.yaml` on Render's free tier: the gateway and its Python upstream in one container. No card, no configuration.

An API gateway in front of two polyglot backends (Python + Java), doing token-bucket
rate limiting in a C++ extension, unsupervised anomaly scoring on request patterns,
and human-readable explanations for anything it flags.

**4,902 lines · 64 tests passing · verified on Apple M-series, CPython 3.13.9, Temurin 21**

Every number below was measured on a real run and written to `outputs/`. Nothing here
is estimated.

---

## The finding

The project started from a specific claim, taken from how this kind of thing is
usually described:

> The gateway takes the limiter lock on every request. In pure Python that path holds
> the GIL, so worker threads serialise through it. Move the token bucket into a C++
> extension that releases the GIL and throughput will scale with threads.

**That claim is false, and the benchmark says so plainly.** Releasing the GIL around
a ~200 nanosecond critical section costs far more than the section it protects.

Four arms, same algorithm, same striping, 40,000 requests per thread, best of 5
(`bench/bench_ratelimit.py`, results in `outputs/bench_ratelimit.json`):

| threads | python | native, GIL held | native, GIL released | native, batched |
|--------:|-------:|-----------------:|---------------------:|----------------:|
| 1 | 2,162,182 | 5,448,324 | 4,939,771 | 7,088,951 |
| 2 | 2,188,189 | 5,349,441 | 2,044,354 | 6,933,911 |
| 4 | 2,206,889 | 5,433,998 | 994,121 | 6,934,349 |
| 8 | 2,198,409 | 5,386,000 | **457,670** | 6,976,453 |

*requests/second*

Each arm isolates exactly one variable, so the effect decomposes:

| what changed | effect at 8 threads |
|---|---|
| compiled instead of interpreted | **2.45×** faster |
| ...then released the GIL per call | **0.08×** — a 12× *pessimisation* |
| ...then amortised one release over a 256-batch | **15.24×** recovery |
| net, best arm vs pure Python | **3.17×** |

Two things worth saying out loud:

1. **Nothing scales with threads.** Every arm sits at ~1.0× going from 1 to 8 threads.
   The Python-side loop holds the GIL regardless of what the extension does, so a
   gateway's concurrency has to come from async I/O, not from threading the limiter.
2. **The measurement changed the code.** `gateway/ratelimit/limiter.py` calls
   `check_holding_gil` on the request path. The GIL-releasing entry point is kept —
   it is correct for `check_batch`, where one release covers 256 operations — and
   keeping both is what made the decomposition above possible at all.

## Why the flag is not the score

Second measured result, from the end-to-end run (`outputs/e2e_results.json`).

The detector scores every request. The obvious thing is to flag when the score crosses
the threshold. On identical traffic — 8 well-behaved clients, one burst client, one
scanner — that gives:

| flagging rule | worst normal | weakest anomaly | margin | false positives |
|---|---:|---:|---:|---:|
| this request's score | 0.7666 | 0.8574 | **0.0908** | 1 |
| median of last 8 scores | 0.2826 | 0.8301 | **0.5476** | 0 |

**6× wider separation, zero false positives.** The reason is that a cold gateway's
baseline is unstable, and taking a maximum over time will always find that transient.
A client that is genuinely misbehaving stays elevated; a normal client does not.

## What the detector actually does

Also worth stating, because it contradicts the design it started with: **the
IsolationForest is not what produces these results.**

Fitted on 5 clients it gave the scanner and the burst client the *identical* saturated
score (0.8049) and put a normal client at 0.7607 — a margin of 0.04 and a false
positive. The robust z-score on the same data separated them by orders of magnitude.
A forest that isolates points needs a population to isolate them from; five points is
not one. It is now gated behind `min_clients_for_forest=50`, and below that the
"fallback" is simply the better detector.

## End-to-end, on the real HTTP surface

`bench/demo_traffic.py` drives four traffic shapes at a running gateway and reads its
own metrics back:

```
normal   80 requests, 8 clients, irregular spacing     -> the control
pricing  30 requests to the JAVA upstream              -> polyglot routing
burst    60 concurrent from one client                 -> should hit the RATE LIMITER
scanner  90 metronomic 404s across 90 distinct paths   -> should hit the DETECTOR
```

Result:

- burst → **45 of 60 rejected with 429**, none with a 5xx. The gateway rate-limited
  the burst rather than being rate-limited by it.
- scanner → flagged, sustained score **0.87**, margin **0.4961** over the worst normal
  client, **zero false positives**.
- the reason it gives:
  > `sustained 0.87 over 8 requests: request rate unusually high (69.2 MAD); path entropy unusually high (50.0 MAD)`

The division of labour is the point: the burst client was handled by the **rate
limiter**, the scanner by the **anomaly detector**. Those are different mechanisms
answering different questions — *too many?* versus *wrong shape?* — and a gateway that
conflated them would catch one and miss the other.

---

## Architecture

```
                 ┌──────────────────────────────────────────┐
   client ──────▶│  Sentinel Gateway  (FastAPI, :8000)      │
                 │                                          │
                 │  1 identify    api key ─▶ client, tier    │
                 │  2 rate limit  ◀── C++ token bucket       │
                 │  3 route       longest prefix match       │
                 │  4 forward     retry + circuit breaker    │
                 └───────┬───────────────────────┬──────────┘
                         │                       │
              /api/catalog│                       │/api/pricing
                         ▼                       ▼
              ┌────────────────────┐   ┌────────────────────┐
              │ catalog  (Python)  │   │ pricing   (Java)   │
              │ FastAPI    :8101   │   │ Spring Boot :8102  │
              └────────────────────┘   └────────────────────┘

        after the response is sent (BackgroundTasks):
          5 observe   features ─▶ score ─▶ sustained median
          6 explain   if flagged: template, or LLM if a key is set
```

Steps 5 and 6 run **after** the response. Observability must never add latency to, or
be able to take down, the traffic it observes. Rate limiting is step 2 for the mirror
reason: a request that will be rejected should be rejected before it costs a socket.

## Layout

| path | lines | what |
|---|---:|---|
| `native/src/` | 487 | C++17 token bucket + sliding window, striped locks, pybind11 |
| `gateway/` | 2,170 | FastAPI app, limiter policy, proxy, anomaly, explanations, store |
| `services/python-service/` | 149 | catalog upstream |
| `services/java-service/` | 287 | pricing upstream, Spring Boot |
| `dashboard/` | 281 | React ops console |
| `bench/` | 452 | the GIL benchmark and the end-to-end traffic driver |
| `tests/` | 687 | 64 tests |
| `infra/` + CI + Makefile | 389 | Dockerfiles, compose, Fly/Render, GitHub Actions |

## Run it

```bash
make setup          # deps + build the C++ extension
make test           # 64 tests
make run            # catalog + pricing + gateway
open http://localhost:8000
```

The dashboard has four buttons that generate the four traffic shapes above, so the
rate limiter tripping and the detector firing are both visible live. Traffic is driven
from the browser, not from a server endpoint — a gateway that ships a route which
makes it attack itself would be shipping an SSRF surface on its own ops console.

```bash
make bench          # the GIL benchmark
make demo           # end-to-end, writes outputs/e2e_results.json
make docker         # full stack in containers
```

Deploy: `fly deploy --config infra/fly.toml --dockerfile infra/Dockerfile.gateway`.
Fly and Render both build the image remotely, so no local Docker daemon is required.

**No API key is needed for anything.** Explanations default to deterministic templates;
set `ANTHROPIC_API_KEY` and `SENTINEL_LLM_PROVIDER=anthropic` to use a model instead.
The template path is also the control that shows whether the model earns its latency.

## Six bugs the tests and the demo found

Listed because they are the honest history of the build, and because each one is a
better interview answer than the feature it sits under.

1. **`:memory:` gives every connection its own database.** The thread-local pool handed
   each thread an empty one; writes vanished. → shared-cache URI.
2. **Shared-cache SQLite takes a *table* lock and ignores `timeout`.** Three concurrent
   writers produced `database table is locked` and dropped rows. → write mutex.
3. **A fixed shared-cache *name* made every `Store(":memory:")` the same database**, so
   independent tests saw each other's rows. → per-instance name.
4. **`min_history` conflated two thresholds** — clients needed to fit, versus
   observations per client. Seven real clients could never satisfy a value of 30, so the
   detector silently scored everything 0.0 forever. → split into `min_clients` and
   `min_observations`.
5. **MAD collapse.** When a feature's median is 0, the robust scale estimate is 0 and
   every deviation divided by it ran to 1e9 — reason strings literally read
   `1000000000.0 MAD`. → relative *and* absolute scale floors.
6. **Integration tests assumed no upstream was running** and failed on a machine where
   the demo services were up. → pinned to dead ports.

## Known limits

- Rate-limit state is **per-process and in-memory**. Two replicas enforce two
  independent limits. Sharing it needs Redis or a consistent-hash router, and that is
  a real design change, not a config flag.
- The anomaly baseline is per-process too, so replicas learn different norms.
- The IsolationForest path is implemented and tested but does not activate below 50
  clients. The measured results above are the robust-z detector.
- Benchmarked on one machine. The GIL result should hold on any CPython with the GIL
  enabled, but it has not been re-measured on x86 or on a free-threaded build.
- The dashboard is an ops console with no authentication. It is fine behind a private
  network and wrong on the public internet.

See `DECISIONS.md` for why each algorithm was chosen and what was rejected.
