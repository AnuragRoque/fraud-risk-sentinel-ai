"""Risk Engine Orchestrator — SentinelStream Fraud.

Orchestrates deterministic rule evaluation, ML anomaly score inference, hybrid scoring,
explainable reason generation, and high-risk alert routing.
"""

from typing import Dict, List, Any, Optional
import logging

from fraud.explanations import generate_explainable_reasons
from fraud.rules import RuleEngine
from fraud.scorer import HybridRiskScorer
from ml.inference import FraudModelPredictor
from producer.publisher import KafkaTransactionPublisher

logger = logging.getLogger(__name__)


class RiskEngine:
    """End-to-end real-time fraud risk scoring engine."""

    def __init__(
        self,
        predictor: Optional[FraudModelPredictor] = None,
        rule_engine: Optional[RuleEngine] = None,
        scorer: Optional[HybridRiskScorer] = None,
        publisher: Optional[KafkaTransactionPublisher] = None,
    ) -> None:
        self.predictor = predictor
        self.rule_engine = rule_engine or RuleEngine()
        self.scorer = scorer or HybridRiskScorer()
        self.publisher = publisher
        self.scored_events_count: int = 0
        self.alerts_generated_count: int = 0

    def score_event(self, enriched_event_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Score a single feature-enriched transaction dictionary."""
        features = enriched_event_dict.get("features", {})
        merchant_cat = enriched_event_dict.get("merchant_category", "")
        feature_vector = enriched_event_dict.get("feature_vector", [])

        # 1. Deterministic Rule Score
        rule_score, rule_reasons = self.rule_engine.evaluate_rules(features, merchant_category=merchant_cat)

        # 2. ML Anomaly Score
        if self.predictor is not None and feature_vector:
            ml_pred = self.predictor.predict_anomaly_score(feature_vector)
            ml_score = ml_pred["anomaly_score"]
            model_version = ml_pred["model_version"]
        else:
            ml_score = 0.0
            model_version = "rules_only_v1.0"

        # 3. Hybrid Risk Score
        risk_score, risk_level, breakdown = self.scorer.calculate_risk_score(
            rule_score=rule_score,
            ml_score=ml_score,
            features=features,
        )

        # 4. Explainable Reasons
        reasons = generate_explainable_reasons(
            rule_reasons=rule_reasons,
            ml_score=ml_score,
            risk_level=risk_level,
        )

        # 5. Construct Scored Event Payload
        scored_payload = dict(enriched_event_dict)
        scored_payload.update({
            "risk_score": risk_score,
            "risk_level": risk_level,
            "model_version": model_version,
            "rule_score": rule_score,
            "ml_anomaly_score": ml_score,
            "score_breakdown": breakdown,
            "reasons": reasons,
        })

        self.scored_events_count += 1

        # 6. Route High-Risk Alerts to fraud.alerts Topic
        if risk_level == "HIGH":
            self.alerts_generated_count += 1
            logger.info(f"HIGH RISK ALERT [Score: {risk_score}]: Transaction {scored_payload.get('transaction_id')}")
            if self.publisher is not None:
                alert_payload = {
                    "alert_id": f"alt_{scored_payload.get('transaction_id')}",
                    "transaction_id": scored_payload.get("transaction_id"),
                    "user_id": scored_payload.get("user_id"),
                    "amount": scored_payload.get("amount"),
                    "event_time": scored_payload.get("event_time"),
                    "risk_score": risk_score,
                    "risk_level": risk_level,
                    "reasons": reasons,
                }
                self.publisher.publish_event(alert_payload, topic="fraud.alerts")

        return scored_payload

    def score_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Score a micro-batch of enriched events."""
        return [self.score_event(item) for item in batch]
