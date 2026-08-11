"""Deterministic Rule Engine — SentinelStream Fraud.

Evaluates independent deterministic business risk rules against transaction features.
Returns rule scores and explainable trigger descriptions.
"""

from typing import Dict, List, Tuple

HIGH_RISK_MERCHANTS = {"electronics", "jewelry", "crypto_exchange", "luxury_goods", "wire_transfer"}


class RuleEngine:
    """Evaluates business fraud rules on feature-enriched transaction dictionaries."""

    def __init__(self) -> None:
        pass

    def evaluate_rules(self, features: Dict[str, float], merchant_category: str = "") -> Tuple[float, List[str]]:
        """Evaluate all risk rules.
        
        Returns:
            Tuple of (aggregate_rule_score in [0.0, 1.0], list of human-readable trigger reasons).
        """
        rule_scores: List[float] = []
        reasons: List[str] = []

        # Rule A — Excessive Velocity
        tx_5m = features.get("tx_count_5m", 0.0)
        if tx_5m >= 5.0:
            rule_scores.append(1.0)
            reasons.append(f"High transaction velocity detected ({int(tx_5m)} transactions in 5 minutes)")
        elif tx_5m >= 3.0:
            rule_scores.append(0.5)

        # Rule B — Amount Spike Anomaly
        amt_ratio = features.get("amount_vs_user_avg", 1.0)
        amount = features.get("amount", 0.0)
        if amt_ratio >= 5.0 or amount >= 80000.0:
            rule_scores.append(1.0)
            reasons.append(f"Transaction amount ₹{amount:,.2f} is significantly above historical user baseline ({amt_ratio:.1f}x average)")
        elif amt_ratio >= 3.0 or amount >= 40000.0:
            rule_scores.append(0.6)

        # Rule C — New Device Signal
        if features.get("new_device", 0.0) == 1.0:
            rule_scores.append(0.7)
            reasons.append("Transaction originated from an unrecognized new device")

        # Rule D — Geographic Impossibility / Distance Signal
        dist_km = features.get("distance_from_last_tx", 0.0)
        if dist_km >= 500.0:
            rule_scores.append(1.0)
            reasons.append(f"Impossible travel speed / large location shift detected ({dist_km:.1f} km from prior transaction)")
        elif dist_km >= 150.0:
            rule_scores.append(0.5)

        # Rule E — High Risk Merchant Category
        if merchant_category.lower() in HIGH_RISK_MERCHANTS:
            rule_scores.append(0.6)
            reasons.append(f"Transaction at high-risk merchant category '{merchant_category}'")

        # Composite rule score (maximum or weighted mean of triggered rules)
        if not rule_scores:
            aggregate_rule_score = 0.0
        else:
            aggregate_rule_score = float(max(rule_scores))

        return round(aggregate_rule_score, 4), reasons
