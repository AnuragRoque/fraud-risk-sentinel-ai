"""Hybrid Risk Scorer — SentinelStream Fraud.

Combines rule scores, Isolation Forest ML anomaly scores, velocity signals, and behavior scores
into a unified risk score in [0.0, 1.0] and categorizes risk bands (LOW, MEDIUM, HIGH).
"""

from typing import Dict, Any, Tuple
import numpy as np


class HybridRiskScorer:
    """Calculates weighted risk scores and assigns risk severity bands."""

    def __init__(
        self,
        w_rules: float = 0.35,
        w_ml: float = 0.35,
        w_velocity: float = 0.15,
        w_behavior: float = 0.15,
        high_threshold: float = 0.70,
        medium_threshold: float = 0.40,
    ) -> None:
        self.w_rules = w_rules
        self.w_ml = w_ml
        self.w_velocity = w_velocity
        self.w_behavior = w_behavior
        self.high_threshold = high_threshold
        self.medium_threshold = medium_threshold

    def calculate_velocity_score(self, features: Dict[str, float]) -> float:
        """Compute sub-score for transaction velocity (0.0 to 1.0)."""
        tx_5m = features.get("tx_count_5m", 0.0)
        return float(np.clip(tx_5m / 5.0, 0.0, 1.0))

    def calculate_behavior_score(self, features: Dict[str, float]) -> float:
        """Compute sub-score for behavioral deviation (0.0 to 1.0)."""
        amt_ratio = features.get("amount_vs_user_avg", 1.0)
        new_dev = features.get("new_device", 0.0)
        new_loc = features.get("new_location", 0.0)

        score = (min(amt_ratio, 5.0) / 5.0) * 0.5 + new_dev * 0.25 + new_loc * 0.25
        return float(np.clip(score, 0.0, 1.0))

    def calculate_risk_score(
        self,
        rule_score: float,
        ml_score: float,
        features: Dict[str, float],
    ) -> Tuple[float, str, Dict[str, float]]:
        """Calculate aggregated hybrid risk score S_risk and assign risk band.
        
        Returns:
            Tuple of (risk_score in [0.0, 1.0], risk_level ("LOW", "MEDIUM", "HIGH"), breakdown_dict).
        """
        velocity_score = self.calculate_velocity_score(features)
        behavior_score = self.calculate_behavior_score(features)

        raw_risk_score = (
            self.w_rules * rule_score
            + self.w_ml * ml_score
            + self.w_velocity * velocity_score
            + self.w_behavior * behavior_score
        )

        final_risk_score = float(np.clip(raw_risk_score, 0.0, 1.0))

        # Assign risk level band
        if final_risk_score >= self.high_threshold:
            risk_level = "HIGH"
        elif final_risk_score >= self.medium_threshold:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        breakdown = {
            "rule_score": round(rule_score, 4),
            "ml_score": round(ml_score, 4),
            "velocity_score": round(velocity_score, 4),
            "behavior_score": round(behavior_score, 4),
            "final_risk_score": round(final_risk_score, 4),
        }

        return round(final_risk_score, 4), risk_level, breakdown
