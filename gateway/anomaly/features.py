"""Turn a client's recent request history into a fixed-length feature vector.

The scorer is only as good as this file. Two constraints shaped every feature below:

1. **It must be computable from what a gateway already sees.** No request bodies, no
   user identity beyond the API key, no cross-service joins. A gateway that needs to
   parse payloads to decide whether to forward them is not a gateway any more.

2. **It must describe a *pattern*, not a request.** A single GET /search is never
   anomalous. Sixty of them in four seconds, all with identical spacing, across
   forty distinct paths, is. Every feature here is therefore a statistic over a
   window rather than a property of the current call.

The features are deliberately interpretable. An unexplainable "this looked weird"
score is useless to an on-call engineer, and the LLM explanation layer downstream
needs something concrete to talk about. See DECISIONS.md D-07.
"""

from __future__ import annotations

import math
import statistics
from collections import Counter, deque
from dataclasses import dataclass, asdict
from typing import Deque, Dict, List, Optional, Sequence

FEATURE_NAMES = (
    "request_rate",        # requests per second over the window
    "burstiness",          # coefficient of variation of inter-arrival gaps
    "regularity",          # 1 - normalised stdev of gaps; high = machine-timed
    "path_entropy",        # Shannon entropy over paths, normalised 0..1
    "unique_path_ratio",   # distinct paths / requests
    "error_rate",          # share of 4xx/5xx responses
    "limited_ratio",       # share already rejected by the rate limiter
    "method_diversity",    # distinct HTTP methods / requests
    "latency_variance",    # normalised variance of upstream latency
    "night_ratio",         # share of requests in 00:00-06:00 local
)


@dataclass
class RequestFeatures:
    """A client's pattern. `n_observations` is metadata, not a feature - it is
    excluded from as_vector() so it never becomes something the detector trains on,
    but the scorer needs it to know whether the window is long enough to judge."""

    request_rate: float
    burstiness: float
    regularity: float
    path_entropy: float
    unique_path_ratio: float
    error_rate: float
    limited_ratio: float
    method_diversity: float
    latency_variance: float
    night_ratio: float
    n_observations: int = 0

    def as_vector(self) -> List[float]:
        return [getattr(self, n) for n in FEATURE_NAMES]

    def as_dict(self) -> Dict[str, float]:
        return {k: (round(v, 4) if isinstance(v, float) else v)
                for k, v in asdict(self).items()}

    @classmethod
    def zeros(cls, n: int = 0) -> "RequestFeatures":
        return cls(*[0.0] * len(FEATURE_NAMES), n_observations=n)


@dataclass(slots=True)
class _Obs:
    """One observation retained per client. Kept tiny - there is one deque per client."""

    ts: float
    path: str
    method: str
    status: int
    latency_ms: float
    limited: bool
    hour: int


class FeatureExtractor:
    """Maintains a bounded per-client window and computes features from it.

    Memory is the design constraint: one deque per client, capped at `window`. With
    the default 200 and ~120 bytes per _Obs, ten thousand tracked clients cost about
    240 MB worst case, which is why `forget_idle` exists and is called on the same
    schedule as the rate limiter's bucket eviction.
    """

    def __init__(self, window: int = 200, max_age_sec: float = 900.0) -> None:
        self.window = window
        self.max_age_sec = max_age_sec
        self._hist: Dict[str, Deque[_Obs]] = {}

    def observe(self, client_id: str, ts: float, path: str, method: str, status: int,
                latency_ms: float, limited: bool, hour: Optional[int] = None) -> None:
        dq = self._hist.get(client_id)
        if dq is None:
            dq = self._hist[client_id] = deque(maxlen=self.window)
        if hour is None:
            import time as _t
            hour = _t.localtime(ts).tm_hour
        dq.append(_Obs(ts, path, method, status, latency_ms, limited, hour))

    def history_size(self, client_id: str) -> int:
        dq = self._hist.get(client_id)
        return len(dq) if dq else 0

    def forget_idle(self, now: float) -> int:
        """Drop clients whose newest observation is older than max_age_sec."""
        cutoff = now - self.max_age_sec
        stale = [c for c, dq in self._hist.items() if not dq or dq[-1].ts < cutoff]
        for c in stale:
            del self._hist[c]
        return len(stale)

    # -- the actual feature computation --------------------------------------

    def extract(self, client_id: str) -> RequestFeatures:
        dq = self._hist.get(client_id)
        if not dq or len(dq) < 2:
            # Fewer than two requests is not a window; there is not even one
            # inter-arrival gap to measure.
            # One request is not a pattern. Returning zeros rather than None keeps the
            # vector shape constant, which matters because sklearn will not accept a
            # ragged matrix and the caller should not have to special-case cold start.
            return RequestFeatures.zeros(len(dq) if dq else 0)

        obs: Sequence[_Obs] = list(dq)
        n = len(obs)
        span = max(1e-6, obs[-1].ts - obs[0].ts)

        gaps = [obs[i].ts - obs[i - 1].ts for i in range(1, n)]
        mean_gap = statistics.fmean(gaps) if gaps else 0.0
        stdev_gap = statistics.pstdev(gaps) if len(gaps) > 1 else 0.0

        # Burstiness: coefficient of variation. Poisson traffic sits near 1.0;
        # a tight burst pushes it well above; a fixed-interval script pushes it to 0.
        burstiness = (stdev_gap / mean_gap) if mean_gap > 1e-9 else 0.0

        # Regularity is the mirror image and is the more useful signal for bots:
        # humans are irregular, schedulers are metronomic. Squashed to 0..1.
        regularity = 1.0 / (1.0 + burstiness) if mean_gap > 1e-9 else 0.0

        paths = Counter(o.path for o in obs)
        # Normalised Shannon entropy: 0 = every request to one path, 1 = uniform over
        # all paths seen. High entropy with high rate is the classic scan signature.
        max_entropy = math.log(len(paths)) if len(paths) > 1 else 1.0
        entropy = -sum((c / n) * math.log(c / n) for c in paths.values())
        path_entropy = entropy / max_entropy if max_entropy > 0 else 0.0

        latencies = [o.latency_ms for o in obs if o.latency_ms > 0]
        if len(latencies) > 1:
            mean_lat = statistics.fmean(latencies)
            lat_var = statistics.pvariance(latencies) / (mean_lat ** 2) if mean_lat else 0.0
        else:
            lat_var = 0.0

        return RequestFeatures(
            request_rate=n / span,
            burstiness=min(burstiness, 10.0),
            regularity=regularity,
            path_entropy=path_entropy,
            unique_path_ratio=len(paths) / n,
            error_rate=sum(1 for o in obs if o.status >= 400) / n,
            limited_ratio=sum(1 for o in obs if o.limited) / n,
            method_diversity=len({o.method for o in obs}) / n,
            latency_variance=min(lat_var, 10.0),
            night_ratio=sum(1 for o in obs if 0 <= o.hour < 6) / n,
            n_observations=n,
        )

    def all_vectors(self, min_history: int = 2) -> Dict[str, List[float]]:
        """Every tracked client's current vector. Used to fit the detector."""
        return {c: self.extract(c).as_vector()
                for c, dq in self._hist.items() if len(dq) >= min_history}
