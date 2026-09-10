"""Deterministic explanations, written without an LLM.

This exists for three reasons, in ascending order of importance:

1. The gateway must work with no API key. A project that only functions for someone
   holding a paid credential is not deployable.
2. It is the *control* for the LLM. If a templated sentence is as useful as a
   generated one, the LLM is not earning its latency and cost, and the honest thing
   is to know that rather than assume otherwise.
3. It is the fallback when the LLM times out. An explanation that arrives after the
   incident is worthless, so the explainer has a deadline and this is what it returns
   when the deadline passes.

The phrasing is deliberately concrete - a number and a comparison, not an adjective.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

# What each feature means in plain English, and what it usually indicates when high.
FEATURE_PROSE: Dict[str, Tuple[str, str]] = {
    "request_rate":      ("request rate", "sustained automated traffic"),
    "burstiness":        ("burstiness", "traffic arriving in tight clumps"),
    "regularity":        ("timing regularity", "machine-scheduled calls rather than human use"),
    "path_entropy":      ("spread across endpoints", "enumeration or scanning"),
    "unique_path_ratio": ("share of distinct paths", "crawling rather than normal use"),
    "error_rate":        ("error rate", "probing for endpoints that do not exist"),
    "limited_ratio":     ("share of already-throttled calls", "a client ignoring 429s"),
    "method_diversity":  ("variety of HTTP methods", "method probing"),
    "latency_variance":  ("variance in upstream latency", "requests of very uneven cost"),
    "night_ratio":       ("share of overnight traffic", "unattended automation"),
}


def describe_feature(name: str, deviation: float) -> str:
    label, meaning = FEATURE_PROSE.get(name, (name.replace("_", " "), "an unusual pattern"))
    if deviation >= 20:
        strength = "far above"
    elif deviation >= 6:
        strength = "well above"
    else:
        strength = "above"
    return f"{label} is {strength} the norm for this gateway ({deviation:.1f} MAD), which usually means {meaning}"


def render_template(client_id: str, tier: str, score: float,
                    contributors: List[Tuple[str, float]],
                    features: Dict[str, float], limited: bool) -> str:
    """Build the deterministic explanation."""
    if not contributors:
        return (f"Client {client_id} ({tier}) scored {score:.2f}. No single feature stood "
                f"out; the combination of features was unusual relative to other clients.")

    lead = describe_feature(*contributors[0])
    sentences = [f"Client {client_id} ({tier}) was flagged at score {score:.2f}: {lead}."]

    if len(contributors) > 1:
        second = describe_feature(*contributors[1])
        sentences.append(f"It is compounded by the fact that {second}.")

    rate = features.get("request_rate", 0.0)
    errs = features.get("error_rate", 0.0)
    if rate > 0:
        sentences.append(f"Observed rate is {rate:.1f} req/s with a {errs * 100:.0f}% error rate.")

    sentences.append(
        "The request was rejected by the rate limiter." if limited
        else "The request was allowed through; this is a warning, not a block.")
    return " ".join(sentences)
