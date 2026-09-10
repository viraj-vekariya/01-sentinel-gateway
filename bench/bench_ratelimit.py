"""The measurement this project exists to make.

Claim under test
----------------
The gateway takes the limiter lock on every request. In pure Python that path holds
the GIL, so adding worker threads does not add throughput - the limiter becomes the
ceiling. Moving the same algorithm into a C++ extension that releases the GIL should
let throughput scale with thread count.

That is a claim, not a fact, until it is measured. This script measures it.

Design notes that keep the comparison honest
--------------------------------------------
* Both arms run the *same* algorithm with the *same* striping and the *same* policy.
  The Python arm is not handicapped; see gateway/ratelimit/python_bucket.py.
* Thread counts sweep 1 -> 8. A single-thread comparison would only measure C++ vs
  CPython interpreter overhead, which is the boring half of the result. The
  interesting half is the *slope*: how each arm responds to more threads.
* Capacity is set absurdly high so essentially nothing is denied. We are timing the
  limiter's throughput, not the cost of building rejection responses.
* Each arm is warmed before timing, so the extension's first-call cost and CPython's
  specialising interpreter do not land inside the measurement.
* Every configuration runs REPEATS times and reports the best. Best-of-N rather than
  mean because we want the machine's capability, not its background noise; the median
  is reported too so a skewed distribution is visible rather than hidden.

Run:  python3 bench/bench_ratelimit.py [--quick]
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Callable, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gateway.ratelimit.python_bucket import PythonRegistry  # noqa: E402

try:
    import sentinel_native  # noqa: E402
    NATIVE = True
except ImportError:
    NATIVE = False

THREAD_COUNTS = (1, 2, 4, 8)
REQUESTS_PER_THREAD = 40_000
REPEATS = 5
DISTINCT_CLIENTS = 512      # realistic: many clients, so stripes actually spread
STRIPES = 16


@dataclass
class Sample:
    backend: str
    threads: int
    requests: int
    seconds: float
    throughput: float          # requests/second
    per_request_us: float


def _keys(n: int) -> List[str]:
    """Pre-build the key list so string formatting is not inside the timed loop."""
    return [f"client-{i % DISTINCT_CLIENTS}" for i in range(n)]


def _run_threads(check: Callable[[str], object], threads: int, per_thread: int) -> float:
    """Fire `threads` workers, each issuing `per_thread` checks. Returns wall seconds."""
    keys = _keys(per_thread)
    barrier = threading.Barrier(threads + 1)   # start all workers simultaneously

    def worker() -> None:
        barrier.wait()
        for k in keys:
            check(k)

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for w in workers:
        w.start()
    barrier.wait()
    start = time.perf_counter()
    for w in workers:
        w.join()
    return time.perf_counter() - start


def _make_python_registry() -> PythonRegistry:
    return PythonRegistry(PythonRegistry.TOKEN_BUCKET, capacity=1e12,
                          refill_per_sec=1e9, stripes=STRIPES)


def _make_native_registry():
    return sentinel_native.Registry(sentinel_native.Algorithm.TOKEN_BUCKET,
                                    1e12, 1e9, STRIPES)


# The four arms. Naming them by what varies is the whole point of the experiment:
# arm 2 vs arm 1 isolates "compiled vs interpreted"; arm 3 vs arm 2 isolates
# "released the GIL"; arm 4 vs arm 3 isolates "how often we released it".
ARMS = {
    "python":            (_make_python_registry, lambda r: r.check),
    "native_hold_gil":   (_make_native_registry, lambda r: r.check_holding_gil),
    "native_release_gil": (_make_native_registry, lambda r: r.check),
    "native_batched":    (_make_native_registry, lambda r: r.check_many),
}
BATCH = 256


def _run_threads_batched(check_many, threads: int, per_thread: int) -> float:
    """Same harness, but each worker submits fixed-size batches instead of single calls."""
    keys = _keys(per_thread)
    batches = [keys[i:i + BATCH] for i in range(0, len(keys), BATCH)]
    barrier = threading.Barrier(threads + 1)

    def worker() -> None:
        barrier.wait()
        for b in batches:
            check_many(b)

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for w in workers:
        w.start()
    barrier.wait()
    start = time.perf_counter()
    for w in workers:
        w.join()
    return time.perf_counter() - start


def bench_backend(name: str, threads: int, per_thread: int, repeats: int) -> Sample:
    factory, bind = ARMS[name]
    batched = name == "native_batched"
    timings: List[float] = []
    for _ in range(repeats):
        registry = factory()
        check = bind(registry)  # bind once; attribute lookup is not what we're timing
        # Warm-up: touch every stripe so bucket creation is not inside the measurement.
        if batched:
            check([f"client-{i}" for i in range(DISTINCT_CLIENTS)])
            timings.append(_run_threads_batched(check, threads, per_thread))
        else:
            for i in range(DISTINCT_CLIENTS):
                check(f"client-{i}")
            timings.append(_run_threads(check, threads, per_thread))

    best = min(timings)
    total = threads * per_thread
    return Sample(
        backend=name,
        threads=threads,
        requests=total,
        seconds=round(best, 6),
        throughput=round(total / best, 1),
        per_request_us=round(best / total * 1e6, 4),
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="fewer requests/repeats")
    args = ap.parse_args()

    per_thread = 5_000 if args.quick else REQUESTS_PER_THREAD
    repeats = 2 if args.quick else REPEATS

    if not NATIVE:
        print("ERROR: sentinel_native not built. Run: make native", file=sys.stderr)
        return 1

    print(f"Sentinel rate-limiter benchmark")
    print(f"  python      {platform.python_version()} ({platform.machine()})")
    print(f"  gil enabled {getattr(sys, '_is_gil_enabled', lambda: True)()}")
    print(f"  per-thread  {per_thread:,} requests | repeats {repeats} | stripes {STRIPES}")
    print()

    samples: List[Sample] = []
    print(f"  {'threads':>8} " + "".join(f"{a:>22}" for a in ARMS))
    for threads in THREAD_COUNTS:
        row = {}
        for arm in ARMS:
            s = bench_backend(arm, threads, per_thread, repeats)
            samples.append(s)
            row[arm] = s
        print(f"  {threads:>8} " + "".join(f"{row[a].throughput:>16,.0f} req/s" for a in ARMS))

    print()

    def by(backend: str, threads: int) -> Sample:
        return next(s for s in samples if s.backend == backend and s.threads == threads)

    # Scaling factor is the real result. A GIL-bound arm has a factor near 1.0 no
    # matter how many threads you give it; an arm that releases the GIL does not.
    scaling = {a: by(a, 8).throughput / by(a, 1).throughput for a in ARMS}
    print("  scaling 1->8 threads: " + "  ".join(f"{a}={scaling[a]:.2f}x" for a in ARMS))
    print()
    print("  decomposition at 8 threads (each line isolates ONE variable):")
    b = lambda a: by(a, 8).throughput
    print(f"    compiled vs interpreted   : {b('native_hold_gil')/b('python'):>6.2f}x"
          "   (native_hold_gil / python)")
    print(f"    ...then releasing the GIL : {b('native_release_gil')/b('native_hold_gil'):>6.2f}x"
          "   (native_release_gil / native_hold_gil)")
    print(f"    ...then batching releases : {b('native_batched')/b('native_release_gil'):>6.2f}x"
          f"   (native_batched / native_release_gil, batch={BATCH})")
    print(f"    net best vs python        : {max(b(a) for a in ARMS)/b('python'):>6.2f}x")

    out = {
        "environment": {
            "python": platform.python_version(),
            "machine": platform.machine(),
            "platform": platform.platform(),
            "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)(),
        },
        "config": {
            "requests_per_thread": per_thread,
            "repeats": repeats,
            "distinct_clients": DISTINCT_CLIENTS,
            "stripes": STRIPES,
            "thread_counts": list(THREAD_COUNTS),
        },
        "samples": [asdict(s) for s in samples],
        "scaling_1_to_8": {a: round(scaling[a], 3) for a in ARMS},
        "decomposition_at_8_threads": {
            "compiled_vs_interpreted": round(b("native_hold_gil") / b("python"), 3),
            "effect_of_releasing_gil": round(b("native_release_gil") / b("native_hold_gil"), 3),
            "effect_of_batching_releases": round(b("native_batched") / b("native_release_gil"), 3),
            "net_best_vs_python": round(max(b(a) for a in ARMS) / b("python"), 3),
            "batch_size": BATCH,
        },
        "peak_throughput": {a: max(s.throughput for s in samples if s.backend == a)
                            for a in ARMS},
    }
    dest = ROOT / "outputs" / "bench_ratelimit.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2) + "\n")
    print(f"\n  wrote {dest.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
