"""Catalog service - upstream #1, Python/FastAPI.

One of the two backends Sentinel sits in front of. It exists to make the gateway's
polyglot claim real rather than asserted: the gateway must not know or care that this
one is Python and the pricing service is Java.

It is deliberately a *real* service, not a stub that returns 200. It has a dataset, a
search that costs measurably more than a lookup, an artificial-latency knob and a
fault-injection knob - because the gateway's circuit breaker, retry logic and latency
histograms cannot be exercised, let alone demonstrated, against a backend that always
succeeds instantly.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

app = FastAPI(title="Catalog Service (python)", version="1.0.0")

# Fault injection, driven by environment so the load/demo scripts can turn it on
# without a redeploy.
FAIL_RATE = float(os.environ.get("CATALOG_FAIL_RATE", "0.0"))
EXTRA_LATENCY_MS = float(os.environ.get("CATALOG_LATENCY_MS", "0"))
SEED = int(os.environ.get("CATALOG_SEED", "1337"))

_rng = random.Random(SEED)

CATEGORIES = ["laptops", "phones", "audio", "cameras", "storage", "displays"]
BRANDS = ["Aster", "Borealis", "Cobalt", "Dune", "Ember", "Fathom"]


class Item(BaseModel):
    id: int
    name: str
    category: str
    brand: str
    price_cents: int
    stock: int


def _build_catalog(n: int = 480) -> List[Item]:
    """Deterministic catalogue. Seeded so two runs of the demo produce identical
    output - a benchmark against a randomly-different dataset is not reproducible."""
    rng = random.Random(SEED)
    items: List[Item] = []
    for i in range(1, n + 1):
        cat = CATEGORIES[i % len(CATEGORIES)]
        brand = BRANDS[(i // 7) % len(BRANDS)]
        items.append(Item(
            id=i,
            name=f"{brand} {cat[:-1].title()} {1000 + i}",
            category=cat,
            brand=brand,
            price_cents=rng.randrange(4_900, 349_900, 100),
            stock=rng.randint(0, 250),
        ))
    return items


CATALOG: List[Item] = _build_catalog()
BY_ID: Dict[int, Item] = {it.id: it for it in CATALOG}

_stats = {"requests": 0, "faults": 0, "searches": 0}


async def _maybe_fault(cost_multiplier: float = 1.0) -> None:
    """Apply configured latency and failure. Central so every route behaves the same."""
    _stats["requests"] += 1
    if EXTRA_LATENCY_MS > 0:
        await asyncio.sleep(EXTRA_LATENCY_MS * cost_multiplier / 1000.0)
    if FAIL_RATE > 0 and _rng.random() < FAIL_RATE:
        _stats["faults"] += 1
        raise HTTPException(status_code=503, detail="injected fault")


@app.get("/health")
async def health() -> Dict[str, object]:
    return {"status": "ok", "service": "catalog-py", "items": len(CATALOG),
            "fail_rate": FAIL_RATE, "extra_latency_ms": EXTRA_LATENCY_MS, "stats": _stats}


@app.get("/api/catalog/items")
async def list_items(limit: int = Query(20, ge=1, le=200), offset: int = Query(0, ge=0)):
    await _maybe_fault()
    window = CATALOG[offset:offset + limit]
    return {"total": len(CATALOG), "offset": offset, "limit": limit,
            "items": [i.model_dump() for i in window]}


@app.get("/api/catalog/items/{item_id}")
async def get_item(item_id: int):
    await _maybe_fault()
    item = BY_ID.get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail=f"no item {item_id}")
    return item.model_dump()


@app.get("/api/catalog/search")
async def search(q: str = Query("", max_length=64),
                 category: Optional[str] = None,
                 max_price: Optional[int] = None,
                 limit: int = Query(20, ge=1, le=100)):
    """Deliberately the expensive route.

    It scans the whole catalogue rather than using an index. That is not laziness: the
    gateway assigns this route a token cost of 2.0, and a route that costs more
    upstream needs to actually cost more or the demonstration is fictional.
    """
    await _maybe_fault(cost_multiplier=2.0)
    _stats["searches"] += 1
    started = time.perf_counter()

    needle = q.lower().strip()
    results = []
    for item in CATALOG:                       # full scan, on purpose
        if needle and needle not in item.name.lower() and needle not in item.brand.lower():
            continue
        if category and item.category != category:
            continue
        if max_price is not None and item.price_cents > max_price:
            continue
        results.append(item)

    took = (time.perf_counter() - started) * 1000
    return {"query": q, "matched": len(results), "scan_ms": round(took, 3),
            "items": [i.model_dump() for i in results[:limit]]}


@app.get("/api/catalog/categories")
async def categories():
    await _maybe_fault()
    counts: Dict[str, int] = {}
    for item in CATALOG:
        counts[item.category] = counts.get(item.category, 0) + 1
    return {"categories": counts}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("CATALOG_HOST", "127.0.0.1"),
                port=int(os.environ.get("CATALOG_PORT", "8101")), log_level="warning")
