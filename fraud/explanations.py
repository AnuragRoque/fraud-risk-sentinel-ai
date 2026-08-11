"""Explainability Engine — SentinelStream Fraud.

Assembles human-readable decision reasons for scored transaction events.
"""

from typing import Dict, List, Any


def generate_explainable_reasons(
    rule_reasons: List[str],
    ml_score: float,
    ml_threshold: float = 0.65,
    risk_level: str = "LOW",
) -> List[str]:
    """Combine rule trigger reasons and ML anomaly indications into human-readable list."""
    reasons: List[str] = list(rule_reasons)

    # ML Anomaly Reason
    if ml_score >= ml_threshold:
        reasons.append(f"Unsupervised ML model detected severe statistical anomaly (score: {ml_score:.2f})")
    elif ml_score >= 0.50 and risk_level in ("MEDIUM", "HIGH"):
        reasons.append(f"Elevated ML anomaly signal detected (score: {ml_score:.2f})")

    if not reasons and risk_level != "LOW":
        reasons.append("Elevated risk score driven by combined sub-threshold velocity and behavioral signals")
    elif not reasons:
        reasons.append("Normal transaction pattern within expected historical user baseline")

    return reasons
