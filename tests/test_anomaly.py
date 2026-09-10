"""Anomaly detection: features, the scale floors, and the sustained-score rule.

Several of these tests exist because the behaviour they pin down was WRONG in an
earlier revision and was caught by the end-to-end run. They are regression tests for
real bugs, not hypotheticals - each names what it is protecting against.
"""

import time

import pytest

from gateway.anomaly.features import FEATURE_NAMES, FeatureExtractor, RequestFeatures
from gateway.anomaly.scorer import AnomalyScorer


def _feed(fx, client, n, gap, path=lambda i: "/api/items", status=200, hour=14):
    t0 = time.time()
    for i in range(n):
        fx.observe(client, t0 + i * gap, path(i), "GET", status, 10.0, False, hour)


def _population(fx, n_clients=12, n_each=12):
    """A believable normal population: irregular gaps, few repeated paths."""
    import random
    rng = random.Random(11)
    t0 = time.time()
    for c in range(n_clients):
        t = t0
        for i in range(n_each):
            t += rng.uniform(0.4, 1.2)
            fx.observe(f"u{c}", t, f"/api/items/{i % 3}", "GET", 200, 10.0, False, 14)


def test_vector_shape_is_constant_even_on_cold_start():
    """sklearn will not accept a ragged matrix, so the vector length must never vary."""
    fx = FeatureExtractor()
    assert len(fx.extract("nobody").as_vector()) == len(FEATURE_NAMES)
    _feed(fx, "c", 5, 0.1)
    assert len(fx.extract("c").as_vector()) == len(FEATURE_NAMES)


def test_n_observations_is_metadata_not_a_feature():
    """It must not leak into the vector, or the detector would train on how long it
    has been watching rather than on behaviour."""
    fx = FeatureExtractor()
    _feed(fx, "c", 9, 0.1)
    f = fx.extract("c")
    assert f.n_observations == 9
    assert len(f.as_vector()) == len(FEATURE_NAMES)
    assert "n_observations" not in FEATURE_NAMES


def test_regularity_separates_a_metronome_from_a_human():
    fx = FeatureExtractor()
    _feed(fx, "bot", 30, 0.05)                       # fixed gap
    t0 = time.time()
    for i, gap in enumerate([0.3, 1.1, 0.4, 2.0, 0.6, 1.7, 0.2, 1.4] * 4):
        fx.observe("human", t0 + i * gap, "/api/items", "GET", 200, 10.0, False, 14)
    assert fx.extract("bot").regularity > fx.extract("human").regularity


def test_path_entropy_separates_a_scanner_from_a_normal_client():
    fx = FeatureExtractor()
    _feed(fx, "scan", 40, 0.05, path=lambda i: f"/api/items/{i}")
    _feed(fx, "norm", 40, 0.05, path=lambda i: "/api/items/1")
    assert fx.extract("scan").path_entropy > 0.9
    assert fx.extract("norm").path_entropy == 0.0


def test_forgetting_idle_clients_bounds_memory():
    fx = FeatureExtractor(max_age_sec=0.01)
    _feed(fx, "old", 5, 0.001)
    time.sleep(0.05)
    assert fx.forget_idle(time.time()) == 1
    assert fx.history_size("old") == 0


# -- the regression tests ----------------------------------------------------

def test_forest_is_not_used_on_a_tiny_population():
    """REGRESSION: an IsolationForest fitted on 5 clients gave two different anomalies
    the identical saturated score and put a normal client at 0.76. Below
    min_clients_for_forest the simpler robust estimator must be used instead."""
    fx = FeatureExtractor()
    _population(fx, n_clients=6)
    sc = AnomalyScorer(min_clients=5, min_clients_for_forest=50)
    assert sc.maybe_fit(fx.all_vectors(min_history=5))
    assert sc.state()["forest_active"] is False
    assert sc.state()["detector"] == "robust_z"


def test_scale_floor_survives_a_zero_median():
    """REGRESSION: when a feature's median is 0 (error_rate for healthy clients) the
    relative floor is also 0, and deviations divided by it ran to 1e9 - every reason
    string read '1000000000.0 MAD'. The absolute floor must prevent that."""
    fx = FeatureExtractor()
    _population(fx, n_clients=10)                    # all status 200 -> error_rate 0
    sc = AnomalyScorer(min_clients=5)
    sc.maybe_fit(fx.all_vectors(min_history=5))

    _feed(fx, "broken", 12, 0.5, status=500)         # error_rate 1.0 vs a median of 0
    for _ in range(sc.sustain_window):
        r = sc.score(fx.extract("broken"), "broken")
    assert all(dev < 1e6 for _, dev in r.top_contributors), \
        f"scale collapsed: {r.top_contributors}"


def test_a_short_window_is_never_scored():
    """REGRESSION: scoring a client on its first two requests divides by a sub-second
    span and reads a rate of hundreds/sec off one burst of two, which put a
    well-behaved client at 0.71."""
    fx = FeatureExtractor()
    _population(fx, n_clients=10)
    sc = AnomalyScorer(min_clients=5, min_observations=5)
    sc.maybe_fit(fx.all_vectors(min_history=5))

    fx.observe("new", time.time(), "/api/x", "GET", 200, 5.0, False, 14)
    fx.observe("new", time.time() + 0.001, "/api/x", "GET", 200, 5.0, False, 14)
    r = sc.score(fx.extract("new"), "new")
    assert r.detector == "cold_start" and r.score == 0.0 and not r.flagged


def test_flagging_needs_a_sustained_score_not_one_spike():
    """REGRESSION: flagging on a single request's score gave a separation margin of
    0.09 and one false positive; the median over a window gave 0.55 and none."""
    fx = FeatureExtractor()
    _population(fx, n_clients=10)
    sc = AnomalyScorer(min_clients=5, sustain_window=8)
    sc.maybe_fit(fx.all_vectors(min_history=5))
    _feed(fx, "scanner", 40, 0.02, path=lambda i: f"/api/x/{i}", status=404)
    feats = fx.extract("scanner")

    first = sc.score(feats, "scanner")
    assert not first.flagged, "flagged on the very first score, before any window"

    for _ in range(sc.sustain_window):
        last = sc.score(feats, "scanner")
    assert last.flagged and last.sustain_n == sc.sustain_window
    assert "sustained" in last.reason()


def test_a_scanner_outscores_every_normal_client():
    """The end-to-end property: real separation, not just 'produces a number'."""
    fx = FeatureExtractor()
    _population(fx, n_clients=12)
    sc = AnomalyScorer(min_clients=5, sustain_window=4)
    sc.maybe_fit(fx.all_vectors(min_history=5))
    _feed(fx, "scanner", 60, 0.02, path=lambda i: f"/api/x/{i}", status=404)

    def settled(client):
        for _ in range(6):
            r = sc.score(fx.extract(client), client)
        return r.sustained

    scanner = settled("scanner")
    worst_normal = max(settled(f"u{c}") for c in range(12))
    assert scanner > worst_normal, f"scanner {scanner} !> normal {worst_normal}"
    assert scanner - worst_normal > 0.2, f"margin too thin: {scanner - worst_normal}"


def test_reason_names_the_feature_that_caused_it():
    """An unexplainable score is not actionable, which is the point of the whole layer."""
    fx = FeatureExtractor()
    _population(fx, n_clients=10)
    sc = AnomalyScorer(min_clients=5, sustain_window=3)
    sc.maybe_fit(fx.all_vectors(min_history=5))
    _feed(fx, "fast", 40, 0.01)
    for _ in range(4):
        r = sc.score(fx.extract("fast"), "fast")
    assert r.flagged
    assert "request rate" in r.reason()


def test_scores_are_reproducible_for_identical_input():
    fx = FeatureExtractor()
    _population(fx, n_clients=10)
    a, b = AnomalyScorer(min_clients=5), AnomalyScorer(min_clients=5)
    pop = fx.all_vectors(min_history=5)
    a.maybe_fit(pop)
    b.maybe_fit(pop)
    f = fx.extract("u3")
    assert a.score(f, "u3").score == b.score(f, "u3").score
