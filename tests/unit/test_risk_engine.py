"""Unit tests for SentinelStream Milestone 5 — Hybrid Risk Engine & Explainability."""

from datetime import datetime, timezone
import pytest

from fraud.explanations import generate_explainable_reasons
from fraud.risk_engine import RiskEngine
from fraud.rules import RuleEngine
from fraud.scorer import HybridRiskScorer
from ml.inference import FraudModelPredictor
from producer.publisher import KafkaTransactionPublisher
from producer.schemas import TransactionEvent
from streaming.features import StreamingFeatureTransformer


def test_rule_engine_triggers():
    """Test deterministic rule engine trigger evaluation."""
    rule_engine = RuleEngine()

    # Normal features -> 0.0 rule score, empty reasons
    feats_normal = {
        "tx_count_5m": 1.0,
        "amount_vs_user_avg": 1.0,
        "amount": 500.0,
        "new_device": 0.0,
        "distance_from_last_tx": 0.0,
    }
    score, reasons = rule_engine.evaluate_rules(feats_normal, merchant_category="groceries")
    assert score == 0.0
    assert len(reasons) == 0

    # High velocity + High Amount + New Device -> 1.0 rule score
    feats_fraud = {
        "tx_count_5m": 6.0,
        "amount_vs_user_avg": 8.0,
        "amount": 95000.0,
        "new_device": 1.0,
        "distance_from_last_tx": 650.0,
    }
    score_f, reasons_f = rule_engine.evaluate_rules(feats_fraud, merchant_category="electronics")
    assert score_f == 1.0
    assert len(reasons_f) >= 4
    assert any("velocity" in r.lower() for r in reasons_f)
    assert any("device" in r.lower() for r in reasons_f)
    assert any("travel" in r.lower() for r in reasons_f)


def test_hybrid_scorer_risk_bands():
    """Test hybrid risk score calculation and severity band thresholds."""
    scorer = HybridRiskScorer(high_threshold=0.70, medium_threshold=0.40)

    # Low Risk Case
    feats_low = {"tx_count_5m": 1.0, "amount_vs_user_avg": 1.0, "new_device": 0.0, "new_location": 0.0}
    score_low, band_low, _ = scorer.calculate_risk_score(rule_score=0.0, ml_score=0.1, features=feats_low)
    assert score_low < 0.40
    assert band_low == "LOW"

    # Medium Risk Case
    feats_med = {"tx_count_5m": 2.0, "amount_vs_user_avg": 2.5, "new_device": 1.0, "new_location": 0.0}
    score_med, band_med, _ = scorer.calculate_risk_score(rule_score=0.5, ml_score=0.5, features=feats_med)
    assert 0.40 <= score_med < 0.70
    assert band_med == "MEDIUM"

    # High Risk Case
    feats_high = {"tx_count_5m": 6.0, "amount_vs_user_avg": 6.0, "new_device": 1.0, "new_location": 1.0}
    score_high, band_high, _ = scorer.calculate_risk_score(rule_score=1.0, ml_score=0.85, features=feats_high)
    assert score_high >= 0.70
    assert band_high == "HIGH"


def test_explainable_reasons_generation():
    """Test explainable reason string composition."""
    rule_reasons = ["High transaction velocity detected", "New device detected"]
    reasons = generate_explainable_reasons(rule_reasons=rule_reasons, ml_score=0.88, risk_level="HIGH")

    assert len(reasons) == 3
    assert any("High transaction velocity" in r for r in reasons)
    assert any("statistical anomaly" in r for r in reasons)


def test_risk_engine_end_to_end_scoring_and_alert_routing():
    """Test RiskEngine scoring execution and HIGH risk alert routing to fraud.alerts."""
    publisher = KafkaTransactionPublisher(mock_mode=True)
    # Instantiate predictor with mock trained model
    from sklearn.ensemble import IsolationForest
    import numpy as np

    X_dummy = np.random.randn(50, 14)
    model = IsolationForest(n_estimators=10, random_state=42).fit(X_dummy)
    predictor = FraudModelPredictor(model=model, model_version="test_iforest_v1")

    engine = RiskEngine(predictor=predictor, publisher=publisher)
    transformer = StreamingFeatureTransformer()

    now = datetime.now(timezone.utc)
    high_risk_tx = TransactionEvent(
        transaction_id="tx_high_risk_99",
        event_time=now,
        user_id="usr_danger",
        account_id="acc_danger",
        amount=150000.0,
        currency="INR",
        merchant_id="m_high_risk",
        merchant_category="jewelry",
        payment_method="NETBANKING",
        device_id="dev_novel",
        ip_address="185.220.10.5",
        latitude=28.6139,
        longitude=77.2090,
        country="IN",
        city="Delhi",
    )

    enriched_dict = transformer.transform_event(high_risk_tx)
    # Inject high risk features
    enriched_dict["features"]["tx_count_5m"] = 8.0
    enriched_dict["features"]["amount_vs_user_avg"] = 10.0
    enriched_dict["features"]["new_device"] = 1.0
    enriched_dict["features"]["new_location"] = 1.0
    enriched_dict["features"]["distance_from_last_tx"] = 650.0

    scored_event = engine.score_event(enriched_dict)

    assert scored_event["risk_level"] == "HIGH"
    assert scored_event["risk_score"] >= 0.70
    assert len(scored_event["reasons"]) >= 2
    assert engine.alerts_generated_count == 1

    # Verify high-risk alert was published to fraud.alerts topic
    assert len(publisher.published_messages) == 1
    alert_msg = publisher.published_messages[0]
    assert alert_msg["topic"] == "fraud.alerts"
    assert alert_msg["value"]["transaction_id"] == "tx_high_risk_99"
    assert alert_msg["value"]["risk_level"] == "HIGH"
