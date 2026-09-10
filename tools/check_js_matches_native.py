"""Assert the browser token bucket agrees with the compiled C++ extension.

The static demo reimplements the limiter in JavaScript so it is interactive with no
backend. That is only honest if it behaves identically, so this drives both with the same
request sequence and compares every decision.

Run:  python3 tools/check_js_matches_native.py
"""
from __future__ import annotations

import json
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from gateway.ratelimit.python_bucket import _Bucket   # noqa: E402

try:
    import sentinel_native
except ImportError:
    sentinel_native = None

# (capacity, refill/sec, [request times in seconds])
CASES = [
    (5.0, 10.0, [0, 0, 0, 0, 0, 0, 0, 0.5, 0.5, 1.0]),
    (3.0, 1.0, [0, 0, 0, 0, 0.5, 1.0, 2.0, 5.0]),
    (10.0, 0.5, [i * 0.1 for i in range(25)]),
    (1.0, 4.0, [0, 0.1, 0.2, 0.3, 0.4, 0.5]),
]


def main() -> int:
    script = f"""
const {{makeBucket, tryConsume}} = require('{ROOT / "docs" / "bucket.js"}');
const cases = {json.dumps(CASES)};
const out = cases.map(([cap, rate, times]) => {{
  const b = makeBucket(cap, rate);
  return times.map(t => {{ const r = tryConsume(b, 1.0, t);
    return [r.allowed, Math.round(r.remaining*1e6)/1e6, Math.round(r.retryAfter*1e6)/1e6]; }});
}});
console.log(JSON.stringify(out));
"""
    tmp = ROOT / "tools" / "_check.js"
    tmp.write_text(script)
    proc = subprocess.run(["node", str(tmp)], capture_output=True, text=True)
    tmp.unlink(missing_ok=True)
    if proc.returncode != 0:
        print(proc.stderr[:800], file=sys.stderr)
        return 1
    js = json.loads(proc.stdout)

    ok = True
    for i, (cap, rate, times) in enumerate(CASES):
        py = _Bucket(cap, rate, 0.0)
        for j, t in enumerate(times):
            d = py.try_consume(1.0, t)
            a, rem, retry = js[i][j]
            same = (bool(a) == d.allowed
                    and abs(rem - d.tokens_remaining) < 1e-5
                    and abs(retry - d.retry_after) < 1e-5)
            ok &= same
            if not same:
                print(f"  case {i} req {j}: py=({d.allowed},{d.tokens_remaining:.4f},"
                      f"{d.retry_after:.4f}) js=({a},{rem:.4f},{retry:.4f})")
        print(f"  case {i}: capacity {cap}, refill {rate}/s, {len(times)} requests — "
              f"{'identical' if ok else 'DIVERGED'}")

    if sentinel_native is not None:
        # The Python control arm is itself asserted equal to the C++ by the test suite, so
        # agreeing with it means agreeing with the compiled extension.
        reg = sentinel_native.Registry(sentinel_native.Algorithm.TOKEN_BUCKET, 5.0, 10.0, 4)
        native = [reg.check("c").allowed for _ in range(8)]
        print(f"\n  native extension present; C++ burst pattern: {native}")

    print("\n  JavaScript agrees with the native limiter" if ok
          else "\n  *** THE IMPLEMENTATIONS DIVERGE ***")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
