"""Kafka Consumer Engine — SentinelStream Streaming.

Consumes transaction events from Kafka topics (default: transactions.raw),
validates Pydantic schemas, routes malformed events to DLQ, and handles restart offset tracking.
"""

from typing import Any, Callable, Dict, List, Optional
import json
import logging
from pydantic import ValidationError

from producer.schemas import TransactionEvent

logger = logging.getLogger(__name__)

try:
    from kafka import KafkaConsumer  # type: ignore
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class KafkaTransactionConsumer:
    """Consumer for reading, parsing, and validating Kafka transaction event streams."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions.raw",
        group_id: str = "sentinelstream-consumer-group",
        dlq_publisher: Optional[Any] = None,
        mock_mode: bool = False,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.dlq_publisher = dlq_publisher
        self.mock_mode = mock_mode or not KAFKA_AVAILABLE

        self.consumer = None
        self.consumed_events: List[TransactionEvent] = []
        self.failed_events_count: int = 0
        self.mock_queue: List[Dict[str, Any]] = []

        if not self.mock_mode:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    group_id=self.group_id,
                    auto_offset_reset="earliest",
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                )
                logger.info(f"Connected KafkaConsumer to {bootstrap_servers} [Topic: {topic}]")
            except Exception as e:
                logger.warning(f"Failed to connect KafkaConsumer ({e}). Falling back to mock mode.")
                self.mock_mode = True

    def process_message_payload(self, payload: Dict[str, Any]) -> Optional[TransactionEvent]:
        """Validate and parse raw JSON dict payload into a canonical TransactionEvent.
        
        If validation fails, routes malformed payload to DLQ.
        """
        try:
            event = TransactionEvent.model_validate(payload)
            self.consumed_events.append(event)
            return event
        except (ValidationError, Exception) as e:
            self.failed_events_count += 1
            logger.error(f"Schema validation failed for event payload: {e}")
            if self.dlq_publisher is not None:
                self.dlq_publisher.publish_to_dlq(payload, error_message=str(e))
            return None

    def seed_mock_messages(self, messages: List[Dict[str, Any]]) -> None:
        """Seed mock message queue for testing without live Kafka cluster."""
        self.mock_queue.extend(messages)

    def consume_batch(self, max_records: int = 100) -> List[TransactionEvent]:
        """Consume and process up to max_records events."""
        valid_events: List[TransactionEvent] = []

        if self.mock_mode:
            to_process = self.mock_queue[:max_records]
            self.mock_queue = self.mock_queue[max_records:]
            for payload in to_process:
                evt = self.process_message_payload(payload)
                if evt is not None:
                    valid_events.append(evt)
            return valid_events

        if self.consumer is None:
            return valid_events

        records = self.consumer.poll(timeout_ms=1000, max_records=max_records)
        for tp, messages in records.items():
            for msg in messages:
                evt = self.process_message_payload(msg.value)
                if evt is not None:
                    valid_events.append(evt)

        # Commit offset after successful batch processing (at-least-once guarantee)
        self.consumer.commit()
        return valid_events
