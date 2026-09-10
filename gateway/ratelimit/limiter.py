"""Policy layer over the two limiter backends.

Responsibilities, in order of importance:

1. Pick a backend. Native if importable, pure Python otherwise. The gateway must
   still start on a machine with no compiler, so a missing extension degrades
   throughput rather than breaking the service.
2. Resolve *which* policy applies to a request. A gateway does not have one rate
   limit, it has a table of them: per-route, per-tier, with a default. Resolution
   order is route+tier -> route -> tier -> default, most specific wins.
3. Keep one registry per policy. Buckets for `/api/search` at the free tier must
   not share state with `/api/search` at the paid tier, or the cheaper tier
   would consume the expensive tier's allowance.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

log = logging.getLogger("sentinel.ratelimit")

try:
    import sentinel_native  # type: ignore
    NATIVE_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised on machines without a build
    sentinel_native = None  # type: ignore
    NATIVE_AVAILABLE = False
    log.warning(
        "sentinel_native not importable - falling back to the pure-Python limiter. "
        "Throughput will be GIL-bound; run 'make native' to build the extension."
    )

from .python_bucket import PythonRegistry


@dataclass(frozen=True)
class LimitPolicy:
    """One rate limit. Frozen so a policy cannot be mutated after registries exist."""

    name: str
    capacity: float           # burst size (token bucket) or requests/window (sliding)
    refill_per_sec: float     # sustained rate; ignored by the sliding-window algorithm
    algorithm: str = "token_bucket"
    cost: float = 1.0         # some routes are more expensive than others

    def key(self) -> str:
        return f"{self.name}:{self.algorithm}:{self.capacity}:{self.refill_per_sec}"


@dataclass(slots=True)
class LimitResult:
    allowed: bool
    policy: str
    backend: str
    tokens_remaining: float
    retry_after: float
    limit: float


class Limiter:
    """Owns every registry and resolves policy per request."""

    def __init__(self, default: LimitPolicy, force_backend: Optional[str] = None,
                 stripes: int = 16) -> None:
        self.default = default
        self.stripes = stripes
        self._route_policies: Dict[str, LimitPolicy] = {}
        self._tier_policies: Dict[str, LimitPolicy] = {}
        self._route_tier_policies: Dict[Tuple[str, str], LimitPolicy] = {}
        self._registries: Dict[str, object] = {}
        self._lock = threading.Lock()

        if force_backend not in (None, "native", "python"):
            raise ValueError("force_backend must be 'native', 'python' or None")
        if force_backend == "native" and not NATIVE_AVAILABLE:
            raise RuntimeError("native backend forced but sentinel_native is not built")

        self.backend = force_backend or ("native" if NATIVE_AVAILABLE else "python")
        self._ensure_registry(default)

    # -- policy registration -------------------------------------------------

    def add_route_policy(self, route: str, policy: LimitPolicy) -> None:
        self._route_policies[route] = policy
        self._ensure_registry(policy)

    def add_tier_policy(self, tier: str, policy: LimitPolicy) -> None:
        self._tier_policies[tier] = policy
        self._ensure_registry(policy)

    def add_route_tier_policy(self, route: str, tier: str, policy: LimitPolicy) -> None:
        self._route_tier_policies[(route, tier)] = policy
        self._ensure_registry(policy)

    def resolve(self, route: str, tier: str) -> LimitPolicy:
        """Most specific match wins. Kept as a pure function so it is trivially testable."""
        if (route, tier) in self._route_tier_policies:
            return self._route_tier_policies[(route, tier)]
        if route in self._route_policies:
            return self._route_policies[route]
        if tier in self._tier_policies:
            return self._tier_policies[tier]
        return self.default

    # -- backends ------------------------------------------------------------

    def _ensure_registry(self, policy: LimitPolicy) -> object:
        pkey = policy.key()
        reg = self._registries.get(pkey)
        if reg is not None:
            return reg
        # Double-checked under the lock: two concurrent first-requests for the same
        # new policy must not each build a registry, or one set of buckets is orphaned
        # and that client silently gets double its allowance.
        with self._lock:
            reg = self._registries.get(pkey)
            if reg is None:
                reg = self._build_registry(policy)
                self._registries[pkey] = reg
            return reg

    def _build_registry(self, policy: LimitPolicy) -> object:
        if self.backend == "native":
            algo = (sentinel_native.Algorithm.TOKEN_BUCKET
                    if policy.algorithm == "token_bucket"
                    else sentinel_native.Algorithm.SLIDING_WINDOW)
            return sentinel_native.Registry(algo, policy.capacity, policy.refill_per_sec,
                                            self.stripes)
        algo = (PythonRegistry.TOKEN_BUCKET if policy.algorithm == "token_bucket"
                else PythonRegistry.SLIDING_WINDOW)
        return PythonRegistry(algo, policy.capacity, policy.refill_per_sec, self.stripes)

    # -- hot path ------------------------------------------------------------

    def check(self, client_id: str, route: str, tier: str = "default") -> LimitResult:
        """Rate-limit one request.

        Note which native entry point this calls: `check_holding_gil`, NOT `check`.
        That is a direct consequence of bench/bench_ratelimit.py. Releasing the GIL
        around a ~200ns critical section costs far more than the section itself -
        measured at 0.08x throughput under 8 threads, a 12x pessimisation. The
        GIL-releasing entry point is kept because it is the right choice for a
        *batch*, where one release amortises over 256 operations (15.2x recovery),
        and because keeping both is what makes the benchmark's decomposition
        possible. But the per-request hot path holds the GIL deliberately.
        See DECISIONS.md D-03.
        """
        policy = self.resolve(route, tier)
        registry = self._ensure_registry(policy)
        check = getattr(registry, "check_holding_gil", None) or registry.check
        decision = check(client_id, policy.cost)  # type: ignore[attr-defined]
        return LimitResult(
            allowed=decision.allowed,
            policy=policy.name,
            backend=self.backend,
            tokens_remaining=decision.tokens_remaining,
            retry_after=decision.retry_after,
            limit=policy.capacity,
        )

    # -- maintenance ---------------------------------------------------------

    def check_batch(self, client_ids: list, route: str, tier: str = "default") -> list:
        """Batch entry point. Uses the GIL-releasing path, where it genuinely pays.

        Used by the replay/load tooling rather than the request path, since real HTTP
        requests do not arrive pre-batched.
        """
        policy = self.resolve(route, tier)
        registry = self._ensure_registry(policy)
        many = getattr(registry, "check_many", None)
        if many is None:
            return [self.check(c, route, tier) for c in client_ids]
        decisions = many(client_ids, policy.cost)
        return [
            LimitResult(d.allowed, policy.name, self.backend, d.tokens_remaining,
                        d.retry_after, policy.capacity)
            for d in decisions
        ]

    def evict_idle(self, max_idle_sec: float = 300.0) -> int:
        return sum(r.evict_idle(max_idle_sec) for r in self._registries.values())  # type: ignore

    def stats(self) -> Dict[str, object]:
        allowed = sum(r.total_allowed for r in self._registries.values())  # type: ignore
        denied = sum(r.total_denied for r in self._registries.values())    # type: ignore
        tracked = sum(r.size() for r in self._registries.values())         # type: ignore
        total = allowed + denied
        return {
            "backend": self.backend,
            "native_available": NATIVE_AVAILABLE,
            "policies": len(self._registries),
            "tracked_clients": tracked,
            "allowed": allowed,
            "denied": denied,
            "deny_rate": round(denied / total, 4) if total else 0.0,
        }
