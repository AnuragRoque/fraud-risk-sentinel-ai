"""Unit tests for SentinelStream Milestone 1 — Synthetic Transaction Engine & Schemas."""

from datetime import datetime, timezone
import pytest
from pydantic import ValidationError

from producer.generator import SyntheticDataGenerator
from producer.scenarios import (
    generate_normal_transaction,
    generate_scenario_1_high_velocity,
    generate_scenario_2_large_amount_anomaly,
    generate_scenario_3_geographic_anomaly,
    generate_scenario_4_new_device_high_value,
    generate_scenario_5_merchant_behavior_change,
    generate_scenario_6_burst_pattern,
    haversine_distance_km,
)
from producer.schemas import GroundTruthEvent, TransactionEvent


def test_canonical_schema_validation():
    """Test valid transaction event parsing and serialization."""
    now = datetime.now(timezone.utc)
    event = TransactionEvent(
        transaction_id="tx_1001",
        event_time=now,
        user_id="usr_500",
        account_id="acc_500",
        amount=1250.75,
        currency="INR",
        merchant_id="m_88",
        merchant_category="groceries",
        payment_method="UPI",
        device_id="dev_99",
        ip_address="10.20.30.40",
        latitude=28.6139,
        longitude=77.2090,
        country="IN",
        city="Delhi",
    )

    assert event.transaction_id == "tx_1001"
    assert event.amount == 1250.75
    assert event.currency == "INR"

    # JSON serialization
    json_str = event.to_json()
    assert "tx_1001" in json_str
    assert "1250.75" in json_str

    # Dictionary serialization
    d = event.to_dict()
    assert d["transaction_id"] == "tx_1001"
    assert d["amount"] == 1250.75


def test_negative_amount_validation():
    """Test that negative transaction amounts raise a ValidationError."""
    now = datetime.now(timezone.utc)
    with pytest.raises(ValidationError):
        TransactionEvent(
            transaction_id="tx_invalid",
            event_time=now,
            user_id="usr_1",
            account_id="acc_1",
            amount=-500.0,  # Invalid negative amount
            currency="INR",
            merchant_id="m_1",
            merchant_category="coffee",
            payment_method="UPI",
            device_id="dev_1",
            ip_address="127.0.0.1",
            latitude=28.0,
            longitude=77.0,
            country="IN",
            city="Delhi",
        )


def test_seed_reproducibility():
    """Test that two generators with identical seeds produce identical streams."""
    gen1 = SyntheticDataGenerator(seed=12345, fraud_rate=0.2)
    gen2 = SyntheticDataGenerator(seed=12345, fraud_rate=0.2)

    start_time = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)
    batch1 = gen1.generate_batch(count=50, start_time=start_time)
    batch2 = gen2.generate_batch(count=50, start_time=start_time)

    assert len(batch1) == len(batch2) == 50
    for e1, e2 in zip(batch1, batch2):
        assert e1.event.transaction_id == e2.event.transaction_id
        assert e1.event.amount == e2.event.amount
        assert e1.event.user_id == e2.event.user_id
        assert e1.is_fraud_ground_truth == e2.is_fraud_ground_truth
        assert e1.fraud_scenario_type == e2.fraud_scenario_type


def test_ground_truth_isolation():
    """Test that streaming events are strictly isolated from ground truth metadata."""
    gen = SyntheticDataGenerator(seed=42, fraud_rate=0.5)
    batch = gen.generate_batch(count=10)

    for gt_event in batch:
        streaming_tx = gt_event.to_streaming_event()
        assert isinstance(streaming_tx, TransactionEvent)
        # Verify no ground truth attributes leak into streaming_tx
        assert not hasattr(streaming_tx, "is_fraud_ground_truth")
        assert not hasattr(streaming_tx, "fraud_scenario_type")
        assert not hasattr(streaming_tx, "fraud_reason_ground_truth")

        d = streaming_tx.to_dict()
        assert "is_fraud_ground_truth" not in d
        assert "fraud_scenario_type" not in d
        assert "fraud_reason_ground_truth" not in d


def test_haversine_distance():
    """Test distance calculation between Delhi and London (~6700 km)."""
    dist = haversine_distance_km(28.6139, 77.2090, 51.5074, -0.1278)
    assert 6650.0 <= dist <= 6750.0


def test_all_6_fraud_scenarios():
    """Test that all 6 fraud scenarios generate valid GroundTruthEvents."""
    start_time = datetime.now(timezone.utc)
    base_city = ("Delhi", 28.6139, 77.2090)

    # Scenario 1
    s1 = generate_scenario_1_high_velocity("usr_1", "acc_1", "dev_1", base_city, start_time)
    assert len(s1) == 6
    assert any(e.fraud_scenario_type == "SCENARIO_1_HIGH_VELOCITY" for e in s1)

    # Scenario 2
    s2 = generate_scenario_2_large_amount_anomaly("usr_1", "acc_1", "dev_1", base_city, start_time)
    assert s2.is_fraud_ground_truth is True
    assert s2.fraud_scenario_type == "SCENARIO_2_LARGE_AMOUNT_ANOMALY"

    # Scenario 3
    s3 = generate_scenario_3_geographic_anomaly("usr_1", "acc_1", "dev_1", base_city, start_time)
    assert len(s3) == 2
    assert s3[1].fraud_scenario_type == "SCENARIO_3_GEOGRAPHIC_ANOMALY"
    assert s3[1].is_fraud_ground_truth is True

    # Scenario 4
    s4 = generate_scenario_4_new_device_high_value("usr_1", "acc_1", base_city, start_time)
    assert s4.is_fraud_ground_truth is True
    assert s4.fraud_scenario_type == "SCENARIO_4_NEW_DEVICE_HIGH_VALUE"

    # Scenario 5
    s5 = generate_scenario_5_merchant_behavior_change("usr_1", "acc_1", "dev_1", base_city, start_time)
    assert s5.is_fraud_ground_truth is True
    assert s5.fraud_scenario_type == "SCENARIO_5_MERCHANT_BEHAVIOR_CHANGE"

    # Scenario 6
    s6 = generate_scenario_6_burst_pattern("usr_1", "acc_1", "dev_1", base_city, start_time, count=15)
    assert len(s6) == 15
    assert all(e.fraud_scenario_type == "SCENARIO_6_BURST_PATTERN" for e in s6)
