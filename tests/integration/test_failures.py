"""Integration tests for SentinelStream Milestone 10 — Failure Scenarios & Fault Tolerance."""

from datetime import datetime, timezone
import pytest

from fraud.risk_engine import RiskEngine
from ml.inference import FraudModelPredictor
from producer.generator import SyntheticDataGenerator
from producer.publisher import KafkaTransactionPublisher
from streaming.consumer import KafkaTransactionConsumer
from warehouse.loader import WarehouseLoader


def test_kafka_broker_unreachable_fallback():
    """Test publisher graceful fallback when Kafka broker is unreachable."""
    # Instantiating publisher with invalid host should fall back to mock mode without throwing uncaught exception
    publisher = KafkaTransactionPublisher(bootstrap_servers="invalid_host:9092", mock_mode=False)
    assert publisher.mock_mode is True

    # Ensure publishing still succeeds in fallback mode
    success = publisher.publish_event({"transaction_id": "tx_fb_1", "user_id": "usr_1"})
    assert success is True
    assert len(publisher.published_messages) == 1


def test_consumer_crash_and_offset_resume():
    """Test consumer crash simulation and resuming processing from last offset."""
    publisher = KafkaTransactionPublisher(mock_mode=True)
    gen = SyntheticDataGenerator(seed=888)
    batch = gen.generate_batch(count=12)

    for gt in batch:
        publisher.publish_event(gt.to_streaming_event().to_dict())

    # Consumer 1 processes 6 records and "crashes"
    c1 = KafkaTransactionConsumer(mock_mode=True)
    c1.seed_mock_messages([m["value"] for m in publisher.published_messages])
    c1_processed = c1.consume_batch(max_records=6)
    assert len(c1_processed) == 6

    # Consumer 2 ("restarted worker") picks up remaining 6 records from queue
    c2 = KafkaTransactionConsumer(mock_mode=True)
    c2.seed_mock_messages(c1.mock_queue)
    c2_processed = c2.consume_batch(max_records=10)
    assert len(c2_processed) == 6

    # Verify no events lost across consumer crash restart
    all_ids = [e.transaction_id for e in c1_processed] + [e.transaction_id for e in c2_processed]
    expected_ids = [gt.event.transaction_id for gt in batch]
    assert all_ids == expected_ids


def test_duplicate_transaction_deduplication():
    """Test idempotency deduplication at warehouse loader layer."""
    loader = WarehouseLoader(db_path=":memory:")
    duplicate_sample = [
        {
            "transaction_id": "tx_dup_99",
            "event_time": "2026-08-11T12:00:00Z",
            "user_id": "usr_dup",
            "account_id": "acc_dup",
            "amount": 1000.0,
            "currency": "INR",
            "merchant_id": "m_dup",
            "merchant_category": "groceries",
            "payment_method": "UPI",
            "device_id": "dev_dup",
            "ip_address": "10.0.0.1",
            "latitude": 28.0,
            "longitude": 77.0,
            "country": "IN",
            "city": "Delhi",
            "risk_score": 0.1,
            "risk_level": "LOW",
            "rule_score": 0.0,
            "ml_anomaly_score": 0.0,
            "model_version": "v1",
            "reasons": [],
        },
        # Exact duplicate transaction_id
        {
            "transaction_id": "tx_dup_99",
            "event_time": "2026-08-11T12:00:00Z",
            "user_id": "usr_dup",
            "account_id": "acc_dup",
            "amount": 1000.0,
            "currency": "INR",
            "merchant_id": "m_dup",
            "merchant_category": "groceries",
            "payment_method": "UPI",
            "device_id": "dev_dup",
            "ip_address": "10.0.0.1",
            "latitude": 28.0,
            "longitude": 77.0,
            "country": "IN",
            "city": "Delhi",
            "risk_score": 0.1,
            "risk_level": "LOW",
            "rule_score": 0.0,
            "ml_anomaly_score": 0.0,
            "model_version": "v1",
            "reasons": [],
        },
    ]

    inserted = loader.load_scored_events(duplicate_sample)
    assert inserted == 2

    # Database count should equal 1 due to INSERT OR REPLACE transaction_id deduplication
    with loader.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) as cnt FROM STG_TRANSACTIONS WHERE transaction_id = 'tx_dup_99'").fetchone()["cnt"]
        assert count == 1


def test_malformed_event_dlq_routing():
    """Test that malformed JSON payloads are intercepted and routed to DLQ."""
    publisher = KafkaTransactionPublisher(mock_mode=True)
    consumer = KafkaTransactionConsumer(dlq_publisher=publisher, mock_mode=True)

    malformed_payload = {"invalid_field": "bad_data", "amount": "not_a_number"}
    consumer.seed_mock_messages([malformed_payload])

    events = consumer.consume_batch(max_records=1)
    assert len(events) == 0
    assert consumer.failed_events_count == 1
    assert len(publisher.dlq_messages) == 1
    assert publisher.dlq_messages[0]["topic"] == "deadletter.transactions"


def test_missing_ml_model_graceful_degradation():
    """Test that RiskEngine gracefully degrades to rules-only mode when ML predictor is absent."""
    engine = RiskEngine(predictor=None)
    event_dict = {
        "transaction_id": "tx_nomodel_1",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": "usr_1",
        "account_id": "acc_1",
        "amount": 500.0,
        "currency": "INR",
        "merchant_id": "m_1",
        "merchant_category": "groceries",
        "payment_method": "UPI",
        "device_id": "dev_1",
        "ip_address": "10.0.0.1",
        "latitude": 28.0,
        "longitude": 77.0,
        "country": "IN",
        "city": "Delhi",
        "features": {"tx_count_5m": 1.0, "amount_vs_user_avg": 1.0},
    }

    scored = engine.score_event(event_dict)
    assert scored["model_version"] == "rules_only_v1.0"
    assert scored["ml_anomaly_score"] == 0.0
    assert "risk_score" in scored


def test_invalid_configuration_failfast():
    """Test that loading a non-existent model file raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        FraudModelPredictor.load_from_file("non_existent_model_file.joblib")
