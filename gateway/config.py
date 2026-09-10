"""Configuration for the gateway.

Everything tunable lives here or in the environment; nothing that changes between
deployments is hard-coded in the request path. Defaults are chosen so `make run`
works on a laptop with no environment set at all - a gateway you cannot start
without a config file is a gateway nobody will run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .ratelimit.limiter import LimitPolicy


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


@dataclass
class UpstreamConfig:
    """One backend service the gateway can route to."""

    name: str
    base_url: str
    prefix: str                    # path prefix that routes here, e.g. "/api/catalog"
    timeout_sec: float = 5.0
    # Circuit breaker: after this many consecutive failures the upstream is taken out
    # of rotation for `breaker_reset_sec` rather than being hammered while it is down.
    breaker_threshold: int = 5
    breaker_reset_sec: float = 15.0


@dataclass
class Settings:
    host: str = field(default_factory=lambda: _env("SENTINEL_HOST", "127.0.0.1"))
    port: int = field(default_factory=lambda: _env_int("SENTINEL_PORT", 8000))
    log_level: str = field(default_factory=lambda: _env("SENTINEL_LOG_LEVEL", "info"))

    # Storage. SQLite because the gateway's own state is small, append-mostly and
    # must survive a restart; a network database would add a failure mode to the
    # component whose entire job is to stay up when things fail.
    db_path: str = field(default_factory=lambda: _env("SENTINEL_DB", "outputs/sentinel.db"))

    # Rate limiting
    default_capacity: float = field(default_factory=lambda: _env_float("SENTINEL_CAPACITY", 60.0))
    default_refill: float = field(default_factory=lambda: _env_float("SENTINEL_REFILL", 10.0))
    limiter_backend: Optional[str] = field(
        default_factory=lambda: os.environ.get("SENTINEL_LIMITER_BACKEND") or None)
    stripes: int = field(default_factory=lambda: _env_int("SENTINEL_STRIPES", 16))
    evict_idle_sec: float = field(default_factory=lambda: _env_float("SENTINEL_EVICT_IDLE", 300.0))
    evict_interval_sec: float = field(
        default_factory=lambda: _env_float("SENTINEL_EVICT_INTERVAL", 60.0))

    # Anomaly scoring
    anomaly_enabled: bool = field(
        default_factory=lambda: _env("SENTINEL_ANOMALY", "1") not in ("0", "false", "no"))
    anomaly_threshold: float = field(
        default_factory=lambda: _env_float("SENTINEL_ANOMALY_THRESHOLD", 0.65))
    # See AnomalyScorer.__init__ - these are two different thresholds and were once
    # one, which broke detection entirely.
    anomaly_min_clients: int = field(
        default_factory=lambda: _env_int("SENTINEL_ANOMALY_MIN_CLIENTS", 5))
    anomaly_min_observations: int = field(
        default_factory=lambda: _env_int("SENTINEL_ANOMALY_MIN_OBS", 5))
    anomaly_refit_interval_sec: float = field(
        default_factory=lambda: _env_float("SENTINEL_ANOMALY_REFIT", 10.0))
    anomaly_min_clients_for_forest: int = field(
        default_factory=lambda: _env_int("SENTINEL_ANOMALY_FOREST_MIN", 50))
    anomaly_window: int = field(default_factory=lambda: _env_int("SENTINEL_ANOMALY_WINDOW", 200))

    # LLM explanations
    llm_provider: str = field(default_factory=lambda: _env("SENTINEL_LLM_PROVIDER", "offline"))
    llm_model: str = field(default_factory=lambda: _env("SENTINEL_LLM_MODEL", "claude-sonnet-5"))
    llm_api_key: str = field(default_factory=lambda: _env("ANTHROPIC_API_KEY", ""))
    llm_timeout_sec: float = field(default_factory=lambda: _env_float("SENTINEL_LLM_TIMEOUT", 8.0))
    llm_cache_size: int = field(default_factory=lambda: _env_int("SENTINEL_LLM_CACHE", 256))

    upstreams: List[UpstreamConfig] = field(default_factory=list)
    route_policies: Dict[str, LimitPolicy] = field(default_factory=dict)
    tier_policies: Dict[str, LimitPolicy] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.upstreams:
            self.upstreams = [
                UpstreamConfig(
                    name="catalog-py",
                    base_url=_env("SENTINEL_UPSTREAM_PY", "http://127.0.0.1:8101"),
                    prefix="/api/catalog",
                ),
                UpstreamConfig(
                    name="pricing-java",
                    base_url=_env("SENTINEL_UPSTREAM_JAVA", "http://127.0.0.1:8102"),
                    prefix="/api/pricing",
                ),
            ]

        if not self.route_policies:
            # Search is expensive upstream, so it costs more tokens per call than a
            # plain lookup. This is why LimitPolicy carries a `cost`: a single rate
            # for every route would either throttle cheap calls or under-protect
            # expensive ones.
            self.route_policies = {
                "/api/catalog/search": LimitPolicy(
                    "catalog-search", capacity=30.0, refill_per_sec=3.0, cost=2.0),
                "/api/pricing/quote": LimitPolicy(
                    "pricing-quote", capacity=20.0, refill_per_sec=2.0, cost=1.0),
            }

        if not self.tier_policies:
            self.tier_policies = {
                "free": LimitPolicy("tier-free", capacity=20.0, refill_per_sec=2.0),
                "paid": LimitPolicy("tier-paid", capacity=200.0, refill_per_sec=50.0),
                "internal": LimitPolicy("tier-internal", capacity=5000.0, refill_per_sec=1000.0),
            }

    def default_policy(self) -> LimitPolicy:
        return LimitPolicy("default", self.default_capacity, self.default_refill)

    def upstream_for(self, path: str) -> Optional[UpstreamConfig]:
        """Longest matching prefix wins, so /api/catalog/search beats /api."""
        best: Optional[UpstreamConfig] = None
        for up in self.upstreams:
            if path.startswith(up.prefix) and (best is None or len(up.prefix) > len(best.prefix)):
                best = up
        return best


settings = Settings()
