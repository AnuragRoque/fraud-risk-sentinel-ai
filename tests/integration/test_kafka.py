"""Integration tests for SentinelStream Milestone 3 — Kafka Pipeline & DLQ Handling."""

from datetime import datetime, timezone
import pytest

from producer.generator import SyntheticDataGenerator
from producer.publisher import KafkaTransactionPublisher
from producer.schemas import TransactionEvent
from streaming.consumer import KafkaTransactionConsumer


def test_publisher_keying_and_mock_delivery():
    """Test publisher message keying by user_id and mock delivery list."""
    publisher = KafkaTransactionPublisher(mock_mode=True)
    
    event_dict = {
        "transaction_id": "tx_k1",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": "usr_999",
        "account_id": "acc_999",
        "amount": 1500.0,
        "currency": "INR",
        "merchant_id": "m_10",
        "merchant_category": "coffee",
        "payment_method": "UPI",
        "device_id": "dev_999",
        "ip_address": "10.0.0.1",
        "latitude": 28.6139,
        "longitude": 77.2090,
        "country": "IN",
        "city": "Delhi",
    }

    success = publisher.publish_event(event_dict)
    assert success is True
    assert len(publisher.published_messages) == 1

    msg = publisher.published_messages[0]
    assert msg["topic"] == "transactions.raw"
    assert msg["key"] == "usr_999"  # Partition key MUST be user_id
    assert msg["value"]["transaction_id"] == "tx_k1"


def test_consumer_schema_validation_and_dlq():
    """Test valid message parsing and invalid message DLQ routing."""
    publisher = KafkaTransactionPublisher(mock_mode=True)
    consumer = KafkaTransactionConsumer(dlq_publisher=publisher, mock_mode=True)

    # Seed 1 valid payload and 1 malformed payload (missing user_id, negative amount)
    valid_payload = {
        "transaction_id": "tx_val_1",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "user_id": "usr_77",
        "account_id": "acc_77",
        "amount": 250.0,
        "currency": "INR",
        "merchant_id": "m_7",
        "merchant_category": "food",
        "payment_method": "UPI",
        "device_id": "dev_77",
        "ip_address": "10.0.0.5",
        "latitude": 19.0760,
        "longitude": 72.8777,
        "country": "IN",
        "city": "Mumbai",
    }
    malformed_payload = {
        "transaction_id": "tx_bad_1",
        "amount": -50.0,  # Negative amount invalid
        "city": "Unknown",
    }

    consumer.seed_mock_messages([valid_payload, malformed_payload])
    consumed_events = consumer.consume_batch(max_records=10)

    assert len(consumed_events) == 1
    assert consumed_events[0].transaction_id == "tx_val_1"
    assert consumer.failed_events_count == 1

    # Check DLQ publisher received the malformed payload
    assert len(publisher.dlq_messages) == 1
    assert publisher.dlq_messages[0]["topic"] == "deadletter.transactions"
    assert "ValidationError" in publisher.dlq_messages[0]["value"]["error_message"] or "tx_bad_1" in publisher.dlq_messages[0]["value"]["raw_payload"]


def test_kafka_pipeline_end_to_end_flow():
    """Test end-to-end producer -> Kafka -> consumer streaming loop."""
    gen = SyntheticDataGenerator(seed=42, fraud_rate=0.1)
    batch = gen.generate_batch(count=25)

    publisher = KafkaTransactionPublisher(mock_mode=True)
    consumer = KafkaTransactionConsumer(dlq_publisher=publisher, mock_mode=True)

    # 1. Publish synthetic events
    for gt in batch:
        tx_dict = gt.to_streaming_event().to_dict()
        publisher.publish_event(tx_dict)

    assert len(publisher.published_messages) == 25

    # 2. Feed messages to consumer
    consumer.seed_mock_messages([m["value"] for m in publisher.published_messages])
    received_events = consumer.consume_batch(max_records=50)

    assert len(received_events) == 25
    assert received_events[0].transaction_id == batch[0].event.transaction_id
    assert received_events[-1].transaction_id == batch[-1].event.transaction_id


def test_consumer_restart_offset_simulation():
    """Test state recovery and resume behavior across consumer restarts."""
    publisher = KafkaTransactionPublisher(mock_mode=True)
    gen = SyntheticDataGenerator(seed=100)
    batch = gen.generate_batch(count=10)

    for gt in batch:
        publisher.publish_event(gt.to_streaming_event().to_dict())

    # Consumer 1 reads first 5 messages
    c1 = KafkaTransactionConsumer(mock_mode=True)
    c1.seed_mock_messages([m["value"] for m in publisher.published_messages])
    first_half = c1.consume_batch(max_records=5)
    assert len(first_half) == 5

    # Consumer 2 ("restarted") consumes remaining 5 messages from queue
    remaining_messages = c1.mock_queue
    c2 = KafkaTransactionConsumer(mock_mode=True)
    c2.seed_mock_messages(remaining_messages)
    second_half = c2.consume_batch(max_records=5)
    assert len(second_half) == 5

    # Verify order continuity across restart
    total_ids = [e.transaction_id for e in first_half] + [e.transaction_id for e in second_half]
    expected_ids = [gt.event.transaction_id for gt in batch]
    assert total_ids == expected_ids
