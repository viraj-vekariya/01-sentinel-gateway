"""Sentinel Gateway - the FastAPI application.

Request path, in order, and the order is the design:

    1. identify   - who is calling, and at what tier
    2. rate limit - cheapest possible rejection, before any I/O
    3. forward    - proxy to the upstream that owns this path prefix
    4. observe    - record the request, extract features, score the pattern
    5. explain    - if flagged, generate an explanation OFF the request path

Steps 4 and 5 happen after the response is produced. That ordering is deliberate:
scoring and explanation are observability, and observability must never be able to
add latency to, or take down, the traffic it observes. FastAPI's BackgroundTasks
gives us that for free.

Rate limiting is step 2 for the same reason - a request that will be rejected should
be rejected before it costs a socket, and certainly before it costs an LLM call.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, Optional

from fastapi import BackgroundTasks, FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

from .anomaly import AnomalyScorer, FeatureExtractor
from .config import Settings, settings as default_settings
from .explain import Explainer
from .proxy import Proxy
from .ratelimit import Limiter
from .store import Store
from .telemetry import RequestEvent, Telemetry

log = logging.getLogger("sentinel")

DASHBOARD = Path(__file__).resolve().parent.parent / "dashboard" / "index.html"


class GatewayState:
    """Every long-lived object the request path touches, built once at startup."""

    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.limiter = Limiter(cfg.default_policy(), cfg.limiter_backend, cfg.stripes)
        for route, policy in cfg.route_policies.items():
            self.limiter.add_route_policy(route, policy)
        for tier, policy in cfg.tier_policies.items():
            self.limiter.add_tier_policy(tier, policy)

        self.store = Store(cfg.db_path)
        self.telemetry = Telemetry()
        self.features = FeatureExtractor(window=cfg.anomaly_window)
        self.scorer = AnomalyScorer(
            cfg.anomaly_threshold, cfg.anomaly_min_clients, cfg.anomaly_min_observations,
            cfg.anomaly_refit_interval_sec,
            min_clients_for_forest=cfg.anomaly_min_clients_for_forest)
        self.explainer = Explainer(cfg.llm_provider, cfg.llm_model, cfg.llm_api_key,
                                   cfg.llm_timeout_sec, cfg.llm_cache_size)
        self.proxy = Proxy(list(cfg.upstreams))
        self._maintenance: Optional[asyncio.Task] = None

    async def maintenance_loop(self) -> None:
        """Periodic eviction and detector refit.

        Both are unbounded-growth defences: the limiter's registry and the feature
        extractor's history are both keyed by client-supplied identifiers, so neither
        may grow forever. The refit lives here too because "normal" drifts - a model
        fitted at 3am does not describe 9am traffic.
        """
        interval = self.cfg.evict_interval_sec
        while True:
            try:
                await asyncio.sleep(interval)
                now = time.time()
                dropped_buckets = self.limiter.evict_idle(self.cfg.evict_idle_sec)
                dropped_clients = self.features.forget_idle(now)
                refit = self.scorer.maybe_fit(
                    self.features.all_vectors(min_history=self.scorer.min_observations), now)
                if dropped_buckets or dropped_clients or refit:
                    log.info("maintenance: buckets=-%d clients=-%d refit=%s",
                             dropped_buckets, dropped_clients, refit)
            except asyncio.CancelledError:
                raise
            except Exception:                        # noqa: BLE001
                # Maintenance must never kill itself. A crash here would silently
                # disable eviction and the process would leak until it was OOM-killed.
                log.exception("maintenance iteration failed; continuing")


def identify(request: Request) -> tuple[str, str]:
    """Resolve (client_id, tier).

    An API key is the identity; the peer IP is only a fallback, because behind any
    load balancer every request shares one IP and rate limiting by it would throttle
    all customers together. The tier normally comes from a key lookup - here it is a
    header so the demo can exercise every tier without a user database.
    """
    api_key = request.headers.get("x-api-key")
    client_id = api_key or (request.client.host if request.client else "anonymous")
    tier = request.headers.get("x-tier", "free" if api_key else "default")
    return client_id, tier


def _observe(state: GatewayState, event: RequestEvent, limited: bool) -> None:
    """Runs as a background task, after the response has been sent."""
    try:
        state.features.observe(event.client_id, event.ts, event.path, event.method,
                               event.status, event.latency_ms, limited)

        # Fit opportunistically here, not only on the maintenance timer. The timer
        # alone meant a gateway that had been up for less than one interval scored
        # everything as cold start - which is exactly the window a short demo, a
        # smoke test, or a freshly-deployed instance lives in. maybe_fit() is cheap
        # when it declines: it checks two integers and a timestamp under a lock.
        state.scorer.maybe_fit(
            state.features.all_vectors(min_history=state.scorer.min_observations))

        feats = state.features.extract(event.client_id)
        result = state.scorer.score(feats, event.client_id)

        event.anomaly_score = result.score
        event.flagged = result.flagged
        event.reason = result.reason()
        state.telemetry.record(event)
        request_id = state.store.log_request(event.as_dict() | {"ts": event.ts})

        if result.flagged:
            expl = state.explainer.explain(
                event.client_id, event.tier, result.score,
                result.top_contributors, result.features, limited)
            state.store.log_explanation(request_id, expl.provider, expl.model,
                                        expl.cached, expl.latency_ms, expl.text,
                                        result.features)
    except Exception:                                # noqa: BLE001
        log.exception("post-response observation failed for %s", event.client_id)


def create_app(cfg: Optional[Settings] = None) -> FastAPI:
    cfg = cfg or default_settings

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await state.proxy.start()
        state._maintenance = asyncio.create_task(state.maintenance_loop())
        log.info("sentinel up: limiter=%s upstreams=%s",
                 state.limiter.backend, [u.name for u in cfg.upstreams])
        yield
        if state._maintenance:
            state._maintenance.cancel()
        await state.proxy.close()

    app = FastAPI(title="Sentinel Gateway", version="0.1.0", lifespan=lifespan)
    state = GatewayState(cfg)
    app.state.sentinel = state

    # -- operations endpoints -------------------------------------------------

    @app.get("/health")
    async def health() -> Dict[str, object]:
        breakers = state.proxy.snapshot()
        # On the free tier only the Python upstream is deployed; a JVM does not fit in
        # 512MB alongside the gateway. Saying so here is better than letting a reader
        # conclude the Java service is broken - and the breaker tripping on /api/pricing
        # is a genuine demonstration rather than a fault.
        undeployed = [n for n, b in breakers.items()
                      if "8102" in str(b.get("base_url", ""))
                      and os.environ.get("SENTINEL_JAVA_DEPLOYED", "0") in ("0", "")]
        return {"status": "ok", "limiter_backend": state.limiter.backend,
                "detector": state.scorer.state()["detector"],
                "upstreams": breakers,
                "note": (f"{undeployed} is not deployed on this instance (a JVM does not "
                         f"fit a 512MB free tier), so /api/pricing has no upstream and "
                         f"the circuit breaker will trip - that is real, not a fault. "
                         f"Run `make docker` locally for the full polyglot stack."
                         if undeployed else None)}

    @app.get("/metrics")
    async def metrics() -> Dict[str, object]:
        return {
            "telemetry": state.telemetry.snapshot(),
            "limiter": state.limiter.stats(),
            "anomaly": state.scorer.state(),
            "explainer": state.explainer.stats(),
            "breakers": state.proxy.snapshot(),
            "store": state.store.summary(),
        }

    @app.get("/events")
    async def events(limit: int = 50, flagged: bool = False):
        return {"events": state.telemetry.recent(limit, flagged)}

    @app.get("/clients")
    async def clients(limit: int = 10):
        return {"clients": state.store.top_clients(limit)}

    @app.get("/explain/{request_id}")
    async def explain(request_id: int):
        row = state.store.explanation(request_id)
        if row is None:
            return JSONResponse({"error": "no explanation for that request"}, 404)
        return row

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        if DASHBOARD.exists():
            return DASHBOARD.read_text()
        return "<h1>Sentinel Gateway</h1><p>Dashboard not built.</p>"

    # -- the gateway path -----------------------------------------------------

    @app.api_route("/api/{full_path:path}",
                   methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"])
    async def gateway(full_path: str, request: Request, background: BackgroundTasks):
        started = time.perf_counter()
        path = f"/api/{full_path}"
        client_id, tier = identify(request)

        # 1. Rate limit, before any I/O.
        verdict = state.limiter.check(client_id, path, tier)
        if not verdict.allowed:
            latency = (time.perf_counter() - started) * 1000
            background.add_task(_observe, state, RequestEvent(
                time.time(), client_id, tier, request.method, path, 429, latency,
                True, 0.0, False, "", ""), True)
            return JSONResponse(
                {"error": "rate_limited", "policy": verdict.policy,
                 "retry_after_sec": round(verdict.retry_after, 3),
                 "limit": verdict.limit},
                status_code=429,
                headers={"retry-after": str(max(1, int(verdict.retry_after + 0.999))),
                         "x-ratelimit-limit": str(int(verdict.limit)),
                         "x-ratelimit-remaining": str(int(max(0, verdict.tokens_remaining))),
                         "x-sentinel-policy": verdict.policy},
            )

        # 2. Route.
        upstream = state.cfg.upstream_for(path)
        if upstream is None:
            latency = (time.perf_counter() - started) * 1000
            background.add_task(_observe, state, RequestEvent(
                time.time(), client_id, tier, request.method, path, 404, latency,
                False, 0.0, False, "", ""), False)
            return JSONResponse({"error": "no_route", "path": path}, 404)

        # 3. Forward.
        body = await request.body()
        result = await state.proxy.forward(
            upstream, request.method, path, dict(request.headers), body,
            request.url.query)

        latency = (time.perf_counter() - started) * 1000
        background.add_task(_observe, state, RequestEvent(
            time.time(), client_id, tier, request.method, path, result.status,
            latency, False, 0.0, False, result.upstream, ""), False)

        headers = dict(result.headers)
        headers["x-sentinel-upstream"] = result.upstream
        headers["x-sentinel-attempts"] = str(result.attempts)
        headers["x-ratelimit-remaining"] = str(int(max(0, verdict.tokens_remaining)))
        return Response(content=result.body, status_code=result.status, headers=headers)

    return app


app = create_app()
