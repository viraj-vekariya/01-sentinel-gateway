"""Drive realistic traffic at a running gateway and record what it did.

This is the end-to-end verification, not a toy demo. It generates four traffic shapes
against the real HTTP surface, then reads the gateway's own /metrics and /events back
and writes outputs/e2e_results.json. Every number the README quotes about detection
comes from this file's output.

The four shapes exist because each exercises a different part of the gateway:

  normal    - five well-behaved clients, irregular human-ish spacing. The control.
              Everything downstream is judged against how these score.
  pricing   - traffic to the Java upstream. Proves polyglot routing under load.
  burst     - 60 concurrent requests from one client. Trips the RATE LIMITER.
  scanner   - metronomic, high path entropy, mostly 404s. Trips the ANOMALY DETECTOR.

Burst and scanner are separate on purpose: rate limiting and anomaly detection are
different mechanisms answering different questions ("too many?" vs "wrong shape?"),
and a demo that conflated them would not show that the gateway distinguishes them.

Run:  python3 bench/demo_traffic.py [--base http://127.0.0.1:8000]
"""

from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List

import httpx

ROOT = Path(__file__).resolve().parent.parent


def _hdr(client: str, tier: str) -> Dict[str, str]:
    return {"x-api-key": client, "x-tier": tier}


def normal_traffic(c: httpx.Client, base: str, rng: random.Random) -> List[int]:
    """Five clients behaving like applications: irregular gaps, few repeated paths."""
    codes = []
    clients = ["acme", "globex", "initech", "umbrella", "hooli",
               "stark", "wayne", "tyrell"]
    for i in range(80):
        cl = clients[i % len(clients)]
        tier = "paid" if i % 3 == 0 else "free"
        r = c.get(f"{base}/api/catalog/items", params={"limit": 5, "offset": i % 40},
                  headers=_hdr(cl, tier))
        codes.append(r.status_code)
        time.sleep(rng.uniform(0.02, 0.09))     # irregular: the human signature
    return codes


def pricing_traffic(c: httpx.Client, base: str) -> List[int]:
    """Exercises the Java upstream through the same gateway.

    Uses its OWN client id rather than reusing a normal-traffic client. An earlier
    version pointed this at "acme", which gave acme both the normal *and* the pricing
    stream and made it genuinely the fastest client in the population. The detector
    duly flagged it - correctly - and it looked like a false positive because the
    demo's ground truth was wrong, not because the detector was.
    """
    codes = []
    for i in range(30):
        r = c.get(f"{base}/api/pricing/quote",
                  params={"itemId": 40 + i, "quantity": [1, 10, 20, 50, 100][i % 5]},
                  headers=_hdr("pricing-client", "paid"))
        codes.append(r.status_code)
        time.sleep(0.03)
    return codes


def burst_traffic(base: str) -> List[int]:
    """60 concurrent requests from ONE client. Should be met by 429s, not by 500s -
    the distinction matters: a gateway that falls over under a burst has not rate
    limited it, it has been rate limited by it."""
    def one(_: int) -> int:
        with httpx.Client(timeout=10.0) as cc:
            return cc.get(f"{base}/api/catalog/search", params={"q": "Cobalt"},
                          headers=_hdr("burst-client", "free")).status_code
    with ThreadPoolExecutor(max_workers=20) as pool:
        return list(pool.map(one, range(60)))


def scanner_traffic(c: httpx.Client, base: str) -> List[int]:
    """Metronomic enumeration of ids that do not exist."""
    codes = []
    for i in range(90):
        r = c.get(f"{base}/api/catalog/items/{90000 + i}", headers=_hdr("scanner-bot", "free"))
        codes.append(r.status_code)
        time.sleep(0.02)                        # fixed gap: the machine signature
    return codes


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    args = ap.parse_args()
    base = args.base.rstrip("/")
    rng = random.Random(20260910)

    with httpx.Client(timeout=15.0) as c:
        try:
            health = c.get(f"{base}/health").json()
        except Exception as exc:                # noqa: BLE001
            print(f"ERROR: gateway not reachable at {base} ({exc})", file=sys.stderr)
            print("Start it with: make run", file=sys.stderr)
            return 1

        print(f"gateway up  limiter={health['limiter_backend']} detector={health['detector']}")

        phases: Dict[str, List[int]] = {}
        print("  normal traffic  ...", end="", flush=True)
        phases["normal"] = normal_traffic(c, base, rng)
        print(f" {len(phases['normal'])} requests")

        print("  pricing (java)  ...", end="", flush=True)
        phases["pricing"] = pricing_traffic(c, base)
        print(f" {len(phases['pricing'])} requests")

        print("  burst x60       ...", end="", flush=True)
        phases["burst"] = burst_traffic(base)
        print(f" {len(phases['burst'])} requests")

        print("  scanner x90     ...", end="", flush=True)
        phases["scanner"] = scanner_traffic(c, base)
        print(f" {len(phases['scanner'])} requests")

        # The scorer refits on the maintenance loop; give it a moment plus let the
        # background observation tasks drain before reading results back.
        time.sleep(2.0)
        metrics = c.get(f"{base}/metrics").json()
        events = c.get(f"{base}/events", params={"limit": 400}).json()["events"]
        clients = c.get(f"{base}/clients", params={"limit": 20}).json()["clients"]

    def codes(name: str) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for s in phases[name]:
            out[str(s)] = out.get(str(s), 0) + 1
        return out

    by_client: Dict[str, List[float]] = {}
    for e in events:
        by_client.setdefault(e["client_id"], []).append(e["anomaly_score"])
    peak = {k: round(max(v), 4) for k, v in by_client.items()}
    mean = {k: round(statistics.fmean(v), 4) for k, v in by_client.items()}

    normal_clients = ["acme", "globex", "initech", "umbrella", "hooli",
                      "stark", "wayne", "tyrell"]
    normal_peak = max((peak.get(c, 0.0) for c in normal_clients), default=0.0)
    scanner_peak = peak.get("scanner-bot", 0.0)

    # Which clients the gateway ACTUALLY flagged. This is the number that matters, and it
    # is not the same as "whose peak crossed the threshold": the gateway flags on the
    # SUSTAINED median of a client's recent scores, precisely because a single elevated
    # score is noise on a cold baseline. An earlier version of this report derived
    # "flagged" from the peak, so it reported false positives the gateway never raised -
    # and the CI gate built on it failed on a perfectly healthy run.
    flagged_clients = {e["client_id"] for e in events if e["flagged"]}
    normal_flagged = sorted(flagged_clients & set(normal_clients))

    # The SUSTAINED separation - the one that matches how the gateway actually decides.
    # The peak margin above is diagnostic and noisy: a cold baseline produces one high
    # score for almost any client, so peak-vs-peak can look tight even when nothing was
    # flagged. Median-vs-median is the comparison the flagging rule makes.
    sustained = {k: round(statistics.median(v), 4) for k, v in by_client.items()}
    normal_sustained = max((sustained.get(c, 0.0) for c in normal_clients), default=0.0)
    scanner_sustained = sustained.get("scanner-bot", 0.0)

    result = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "gateway": {"limiter_backend": metrics["limiter"]["backend"],
                    "detector": metrics["anomaly"]["detector"],
                    "explainer": metrics["explainer"]["provider"]},
        "phases": {name: {"requests": len(v), "status_codes": codes(name)}
                   for name, v in phases.items()},
        "totals": metrics["telemetry"]["counters"],
        "latency": metrics["telemetry"]["latency"],
        "limiter": metrics["limiter"],
        "anomaly": metrics["anomaly"],
        "breakers": metrics["breakers"],
        "peak_anomaly_score_by_client": peak,
        "mean_anomaly_score_by_client": mean,
        "separation": {
            "worst_normal_client_peak": round(normal_peak, 4),
            "scanner_peak": round(scanner_peak, 4),
            "margin": round(scanner_peak - normal_peak, 4),
            "detector_threshold": metrics["anomaly"]["threshold"],
            "detector_threshold_note": ("the gateway flags on the SUSTAINED median of a "
                                        "client's recent scores, not on any single score; "
                                        "the peaks above are diagnostic"),
            "scanner_flagged": "scanner-bot" in flagged_clients,
            "clients_actually_flagged": sorted(flagged_clients),
            "normal_clients_flagged": normal_flagged,
            "any_normal_client_flagged": bool(normal_flagged),
            "sustained_by_client": sustained,
            "worst_normal_client_sustained": round(normal_sustained, 4),
            "scanner_sustained": round(scanner_sustained, 4),
            "sustained_margin": round(scanner_sustained - normal_sustained, 4),
        },
        "top_clients": clients,
        "flagged_examples": [
            {"client_id": e["client_id"], "score": e["anomaly_score"], "reason": e["reason"]}
            for e in events if e["flagged"]
        ][:5],
    }

    dest = ROOT / "outputs" / "e2e_results.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(result, indent=2) + "\n")

    sep = result["separation"]
    print()
    print(f"  burst        -> {codes('burst')}")
    print(f"  scanner peak -> {sep['scanner_peak']}  (threshold {sep['detector_threshold']})")
    print(f"  worst normal -> {sep['worst_normal_client_peak']}")
    print(f"  peak margin  -> {sep['margin']} (diagnostic; noisy on a cold baseline)")
    print(f"  sustained    -> scanner {sep['scanner_sustained']} vs worst normal "
          f"{sep['worst_normal_client_sustained']}, margin {sep['sustained_margin']}")
    print(f"  clients actually flagged     : {sep['clients_actually_flagged']}")
    print(f"  false flags on normal clients: {sep['any_normal_client_flagged']}"
          f"{' -> ' + str(sep['normal_clients_flagged']) if sep['normal_clients_flagged'] else ''}")
    print(f"\n  wrote {dest.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
