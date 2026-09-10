"""Score a client's request pattern for how unusual it is.

Two detectors, and the choice between them is the interesting part.

**IsolationForest** (primary). Unsupervised, which is forced on us: nobody labels
gateway traffic as abusive in real time, so anything supervised would need a training
set we do not have. It isolates points by random splits and calls a point anomalous
when few splits suffice - which suits us because abuse in this feature space really is
"far from the bulk of clients in at least one dimension", not "inside a complicated
manifold". It also needs no feature scaling, since it splits on raw thresholds.

**Robust z-score / MAD** (fallback and cold start). A forest cannot be fitted until
enough clients exist to describe "normal". Until then, and whenever sklearn is not
installed, each feature is scored against the median and median-absolute-deviation of
the population and the worst dimension wins. MAD rather than mean/stdev because a
single aggressive client would inflate a stdev enough to hide itself - the estimator
would be corrupted by the outlier it is meant to catch.

Both return 0..1 where higher = more unusual, so the caller never needs to know which
one answered. See DECISIONS.md D-08.
"""

from __future__ import annotations

import logging
import math
import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

from collections import deque

from .features import FEATURE_NAMES, RequestFeatures

log = logging.getLogger("sentinel.anomaly")

try:
    import numpy as np
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:  # pragma: no cover
    np = None  # type: ignore
    IsolationForest = None  # type: ignore
    SKLEARN_AVAILABLE = False


@dataclass
class ScoreResult:
    score: float                                 # this request's score, 0..1
    flagged: bool                                # decided on `sustained`, not `score`
    detector: str                                # which detector answered
    top_contributors: List[Tuple[str, float]] = field(default_factory=list)
    features: Dict[str, float] = field(default_factory=dict)
    sustained: float = 0.0                       # median of the recent score window
    sustain_n: int = 0                           # how many scores that median covers

    def reason(self) -> str:
        """A short machine-written reason. The LLM layer elaborates on this; if the
        LLM is unavailable this is still a usable answer on its own."""
        if not self.flagged:
            return "within normal pattern"
        prefix = f"sustained {self.sustained:.2f} over {self.sustain_n} requests: "
        if not self.top_contributors:
            return prefix + f"score {self.score:.2f} above threshold"
        parts = [f"{name.replace('_', ' ')} unusually high ({dev:.1f} MAD)"
                 for name, dev in self.top_contributors[:2]]
        return prefix + "; ".join(parts)


class AnomalyScorer:
    def __init__(self, threshold: float = 0.65, min_clients: int = 5,
                 min_observations: int = 5, refit_interval_sec: float = 10.0,
                 contamination: float = 0.05, random_state: int = 42,
                 min_clients_for_forest: int = 50, mad_halfpoint: float = 10.0,
                 scale_floor_ratio: float = 0.25,
                 scale_floor_abs: float = 0.02,
                 sustain_window: int = 8) -> None:
        """
        `min_clients` and `min_observations` are two genuinely different thresholds
        and an earlier version of this class conflated them into one `min_history`,
        which is why the first end-to-end run scored every client 0.0: eight real
        clients could never satisfy a population threshold of thirty, so the detector
        silently never fitted and returned cold-start zeros forever.

        * min_clients      - how many distinct clients must exist before "normal" is
                             a meaningful population to compare against. Below this,
                             the median and MAD are noise. Five is the smallest
                             population where a median has a defined middle and a MAD
                             is not dominated by one client; the value was 8 and the
                             demo produced exactly 7 clients, so the detector sat one
                             short of ever fitting and reported cold-start zeros.
        * min_observations - how many requests one client must have made before its
                             own feature vector is worth scoring. Below this, its
                             rate and burstiness are dominated by sampling.
        """
        self.threshold = threshold
        self.min_clients = min_clients
        self.min_observations = min_observations
        # An IsolationForest fitted on a handful of points is not a detector. Measured
        # on the first working end-to-end run: fitted on 5 clients, it gave the
        # scanner and the burst client the SAME saturated score (0.8049) and put a
        # well-behaved client at 0.7607 - a margin of 0.04 and a false flag. The
        # robust z-score on the same data separated them by three orders of magnitude
        # (2875 MAD vs single digits). So the forest is gated behind a population
        # large enough for "isolate this point from the others" to mean something,
        # and below that the simpler estimator is not a fallback, it is the better
        # detector. See DECISIONS.md D-08 and README "What the detector actually does".
        self.min_clients_for_forest = min_clients_for_forest
        # MAD deviation that maps to a score of 0.5. Tuned so the flag threshold of
        # 0.65 corresponds to ~19 MAD, which is far outside anything the normal
        # clients produced and far inside what the scanner produced.
        self.mad_halfpoint = mad_halfpoint
        # Minimum scale: a fraction of the median, and an absolute floor for features
        # whose median is zero. Both guard MAD collapse; see _fit_baseline.
        self.scale_floor_ratio = scale_floor_ratio
        self.scale_floor_abs = scale_floor_abs
        # How many recent scores the sustained median is taken over. Measured on the
        # end-to-end run: flagging on a single request's score separated real
        # anomalies from normal clients by only 0.09 and produced a false positive,
        # because a cold gateway's baseline is unstable and peak-over-time always
        # catches that transient. Flagging on the median of the last N scores widened
        # the margin to 0.55 - six times - and removed the false positive, on
        # identical traffic. See README "Why the flag is not the score".
        self.sustain_window = sustain_window
        self.refit_interval_sec = refit_interval_sec
        self.contamination = contamination
        self.random_state = random_state

        self._model: Optional[object] = None
        self._fitted_at = 0.0
        self._fitted_n = 0
        self._baseline: Dict[str, Tuple[float, float]] = {}   # feature -> (median, mad)
        # Rolling score history per client. Flagging is decided on the MEDIAN of this,
        # not on the score of the request in hand - see `score()`.
        self._recent: Dict[str, "deque[float]"] = {}
        self._lock = threading.Lock()

    # -- fitting -------------------------------------------------------------

    def maybe_fit(self, population: Dict[str, List[float]], now: Optional[float] = None) -> bool:
        """Refit if enough clients exist and the interval has elapsed. Returns whether
        a fit actually happened, so callers can log it without guessing."""
        now = now or time.time()
        with self._lock:
            if len(population) < self.min_clients:
                return False
            if now - self._fitted_at < self.refit_interval_sec and self._model is not None:
                return False
            vectors = list(population.values())
            self._fit_baseline(vectors)
            if SKLEARN_AVAILABLE and len(vectors) >= self.min_clients_for_forest:
                model = IsolationForest(
                    n_estimators=100,
                    contamination=self.contamination,
                    random_state=self.random_state,   # seeded: two runs must agree
                    n_jobs=1,                         # the gateway owns its cores
                )
                model.fit(np.asarray(vectors, dtype=float))
                self._model = model
            self._fitted_at = now
            self._fitted_n = len(vectors)
            return True

    def _fit_baseline(self, vectors: Sequence[Sequence[float]]) -> None:
        """Per-feature median and MAD. Used by the fallback and by the explanation of
        *which* dimension made a client unusual - the forest gives a score but not a
        reason, and a score without a reason is not actionable."""
        cols = list(zip(*vectors))
        baseline = {}
        for name, col in zip(FEATURE_NAMES, cols):
            med = statistics.median(col)
            mad = statistics.median([abs(v - med) for v in col])
            # 1.4826 makes MAD a consistent estimator of sigma for normal data, so the
            # deviations below read on the same scale an engineer expects from a z-score.
            scale = mad * 1.4826

            # MAD collapse. When most clients cluster tightly the MAD tends to zero,
            # and every deviation divided by it explodes - a client 2% away from the
            # median reads as hundreds of MAD and gets flagged. This is the classic
            # failure mode of robust scale estimators on near-degenerate data, and it
            # is not hypothetical here: it is what produced a 1192-MAD reading in the
            # first working run. Flooring the scale at a fraction of the median means
            # a client must differ by a meaningful PROPORTION of typical behaviour,
            # not merely by more than an almost-zero spread.
            # Two floors, because the collapse has two shapes:
            #   relative - most clients cluster near a NON-zero median (request_rate);
            #              the scale must be a fraction of typical behaviour.
            #   absolute - most clients sit at exactly ZERO (error_rate, path_entropy
            #              for a client hitting one endpoint). A relative floor is
            #              also zero there, so deviations divided by it ran to 1e9
            #              and every reason string read "1000000000.0 MAD".
            # Every feature is either a ratio in [0,1] or a rate, so an absolute floor
            # of 0.02 is negligible for rates and meaningful for ratios.
            relative_floor = self.scale_floor_ratio * abs(med)
            baseline[name] = (med, max(scale, relative_floor, self.scale_floor_abs))
        self._baseline = baseline

    @property
    def is_fitted(self) -> bool:
        return self._model is not None or bool(self._baseline)

    def state(self) -> Dict[str, object]:
        return {
            "detector": "isolation_forest" if self._model is not None else "robust_z",
            "sklearn_available": SKLEARN_AVAILABLE,
            "fitted": self.is_fitted,
            "fitted_clients": self._fitted_n,
            "min_clients": self.min_clients,
            "min_observations": self.min_observations,
            "min_clients_for_forest": self.min_clients_for_forest,
            "forest_active": self._model is not None,
            "fitted_age_sec": round(time.time() - self._fitted_at, 1) if self._fitted_at else None,
            "threshold": self.threshold,
        }

    # -- scoring -------------------------------------------------------------

    def _contributors(self, features: RequestFeatures) -> List[Tuple[str, float]]:
        """Rank features by robust deviation from the population median."""
        out: List[Tuple[str, float]] = []
        for name, value in zip(FEATURE_NAMES, features.as_vector()):
            med, sigma = self._baseline.get(name, (0.0, 0.0))
            if sigma <= 0:
                continue
            dev = (value - med) / sigma
            if dev > 0:                       # only *elevated* features are suspicious
                out.append((name, dev))
        out.sort(key=lambda kv: kv[1], reverse=True)
        return out

    def _sustained(self, client_id: str, score: float) -> Tuple[float, int]:
        """Record this score and return the median of the recent window.

        A single elevated score is noise: baselines drift, a client's window can be
        briefly unrepresentative, and the very first scores after a refit are the
        least trustworthy ones. A client that is genuinely misbehaving stays elevated,
        so the median over a short window is both more stable and more honest.
        """
        with self._lock:
            dq = self._recent.get(client_id)
            if dq is None:
                dq = self._recent[client_id] = deque(maxlen=self.sustain_window)
            dq.append(score)
            values = sorted(dq)
        n = len(values)
        median = values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2.0
        return median, n

    def forget(self, client_id: str) -> None:
        with self._lock:
            self._recent.pop(client_id, None)

    def score(self, features: RequestFeatures, client_id: str = "") -> ScoreResult:
        # A pattern needs a window. Scoring a client on its first two requests means
        # dividing by a sub-second span and reading a rate of hundreds per second off
        # what is really one burst of two - which is what put a well-behaved client at
        # 0.71 before this guard existed. min_observations already gated FITTING; it
        # must gate SCORING for the same reason.
        if features.n_observations < self.min_observations:
            return ScoreResult(0.0, False, "cold_start", [], features.as_dict())

        vector = features.as_vector()
        contributors = self._contributors(features)

        if self._model is not None and np is not None:
            # decision_function: positive = inlier, negative = outlier, roughly in
            # [-0.5, 0.5]. Map to 0..1 with 0.5 at the fitted decision boundary so the
            # threshold is interpretable rather than an arbitrary cut on a raw score.
            raw = float(self._model.decision_function(np.asarray([vector], dtype=float))[0])
            score = 1.0 / (1.0 + math.exp(raw * 10.0))
            detector = "isolation_forest"
        elif self._baseline:
            worst = contributors[0][1] if contributors else 0.0
            # Saturating rather than linear so one extreme feature cannot be diluted
            # by nine ordinary ones. mad_halfpoint sets where 0.5 lands: at the
            # default of 10, the 0.65 flag threshold works out to ~19 MAD.
            score = worst / (worst + self.mad_halfpoint) if worst > 0 else 0.0
            detector = "robust_z"
        else:
            return ScoreResult(0.0, False, "cold_start", [], features.as_dict())

        score = round(min(max(score, 0.0), 1.0), 4)
        sustained, n_scores = self._sustained(client_id or "?", score)

        # Require BOTH a full window and a sustained median over threshold. Without
        # the window check a client's first score would be its own median and the
        # guard would do nothing for exactly the requests it exists to protect.
        flagged = n_scores >= self.sustain_window and sustained >= self.threshold

        return ScoreResult(
            score=score,
            flagged=flagged,
            detector=detector,
            top_contributors=[(n, round(d, 2)) for n, d in contributors[:3]],
            features=features.as_dict(),
            sustained=round(sustained, 4),
            sustain_n=n_scores,
        )
