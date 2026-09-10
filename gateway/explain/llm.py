"""LLM-generated explanations, with the LLM strictly optional.

Design constraints, all of which come from this being on a request path:

* **Never block the response.** Explanation is generated after the request has been
  answered. A gateway that waits on a language model before returning a 429 has
  turned an 8ms rejection into a 900ms one.
* **Deadline everything.** `llm_timeout_sec` is enforced by the caller; on expiry the
  templated explanation stands. Slow is the same as absent here.
* **Cache aggressively.** The same client tripping the same feature ten times in a
  minute is one explanation, not ten. The cache key is the *shape* of the anomaly,
  not the request id.
* **Degrade, never fail.** Any provider error falls back to the template and is
  counted. An observability feature must not be able to take down the thing it
  observes.

Providers: "offline" (template only, the default), "anthropic" (real API call if
ANTHROPIC_API_KEY is set). Adding one is a subclass and a registry entry.
See DECISIONS.md D-09.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .templates import render_template

log = logging.getLogger("sentinel.explain")

SYSTEM_PROMPT = (
    "You are the explanation layer of an API gateway. You are given the anomaly "
    "features of one client's recent traffic. Write 2-3 sentences for an on-call "
    "engineer: what the pattern looks like, the most likely benign explanation, and "
    "the most likely malicious one. Cite the actual numbers you are given. Do not "
    "speculate beyond the features. Do not recommend blocking; that is not your call."
)


@dataclass
class Explanation:
    text: str
    provider: str
    model: str
    cached: bool
    latency_ms: float
    fell_back: bool = False
    error: str = ""


class _LRUCache:
    """Small ordered-dict LRU. stdlib functools.lru_cache is not usable here because
    the cache must be introspectable (the dashboard shows hit rate) and clearable."""

    def __init__(self, maxsize: int = 256) -> None:
        self.maxsize = maxsize
        self._data: "OrderedDict[str, str]" = OrderedDict()
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            if key in self._data:
                self._data.move_to_end(key)
                self._hits += 1
                return self._data[key]
            self._misses += 1
            return None

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)

    def stats(self) -> Dict[str, object]:
        with self._lock:
            total = self._hits + self._misses
            return {"size": len(self._data), "hits": self._hits, "misses": self._misses,
                    "hit_rate": round(self._hits / total, 3) if total else 0.0}


class Explainer:
    def __init__(self, provider: str = "offline", model: str = "claude-sonnet-5",
                 api_key: str = "", timeout_sec: float = 8.0, cache_size: int = 256) -> None:
        self.provider = provider
        self.model = model
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self.timeout_sec = timeout_sec
        self.cache = _LRUCache(cache_size)
        self.calls = 0
        self.failures = 0

        if self.provider == "anthropic" and not self.api_key:
            log.warning("provider=anthropic but no ANTHROPIC_API_KEY set; using offline mode")
            self.provider = "offline"

    # -- cache key -----------------------------------------------------------

    @staticmethod
    def _cache_key(tier: str, contributors: List[Tuple[str, float]], limited: bool) -> str:
        """Key on the anomaly's *shape*, not its instance.

        Deviations are bucketed to one significant step so that 12.3 MAD and 12.9 MAD
        share an explanation - they mean the same thing to a reader, and keying on the
        raw float would make the cache useless.
        """
        shape = ",".join(f"{n}:{int(d) // 3}" for n, d in contributors[:3])
        return f"{tier}|{shape}|{int(limited)}"

    # -- public --------------------------------------------------------------

    def explain(self, client_id: str, tier: str, score: float,
                contributors: List[Tuple[str, float]], features: Dict[str, float],
                limited: bool) -> Explanation:
        start = time.perf_counter()
        fallback = render_template(client_id, tier, score, contributors, features, limited)

        if self.provider == "offline":
            return Explanation(fallback, "offline", "template", False,
                               (time.perf_counter() - start) * 1000)

        key = self._cache_key(tier, contributors, limited)
        hit = self.cache.get(key)
        if hit is not None:
            return Explanation(hit, self.provider, self.model, True,
                               (time.perf_counter() - start) * 1000)

        try:
            self.calls += 1
            text = self._call_provider(client_id, tier, score, contributors, features, limited)
            self.cache.put(key, text)
            return Explanation(text, self.provider, self.model, False,
                               (time.perf_counter() - start) * 1000)
        except Exception as exc:                      # noqa: BLE001 - degrade on anything
            self.failures += 1
            log.warning("explanation provider failed (%s); using template", exc)
            return Explanation(fallback, "offline", "template", False,
                               (time.perf_counter() - start) * 1000,
                               fell_back=True, error=str(exc)[:200])

    def stats(self) -> Dict[str, object]:
        return {
            "provider": self.provider,
            "model": self.model if self.provider != "offline" else "template",
            "calls": self.calls,
            "failures": self.failures,
            "cache": self.cache.stats(),
        }

    # -- providers -----------------------------------------------------------

    def _call_provider(self, client_id: str, tier: str, score: float,
                       contributors: List[Tuple[str, float]], features: Dict[str, float],
                       limited: bool) -> str:
        if self.provider != "anthropic":
            raise ValueError(f"unknown provider {self.provider!r}")

        user = json.dumps({
            "client_id": client_id,
            "tier": tier,
            "anomaly_score": score,
            "was_rate_limited": limited,
            "top_deviations_in_MAD": {n: round(d, 2) for n, d in contributors[:3]},
            "features": features,
        }, indent=2)

        payload = json.dumps({
            "model": self.model,
            "max_tokens": 300,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user}],
        }).encode()

        req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=payload,
            headers={
                "content-type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        # urllib rather than the SDK: one HTTP POST does not justify a dependency in a
        # container that must stay small, and it keeps the provider boundary obvious.
        with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
            body = json.loads(resp.read().decode())
        blocks = body.get("content", [])
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
        if not text:
            raise ValueError("empty completion")
        return text
