"""Kafka Transaction Event Publisher — SentinelStream Producer.

Publishes canonical TransactionEvent payloads to Kafka topics (default: transactions.raw).
Keys events by user_id to enforce per-user partition ordering. Supports mock mode for offline testing.
"""

from typing import Any, Dict, List, Optional
import json
import logging

logger = logging.getLogger(__name__)

# Attempt to import kafka client (kafka-python-ng or confluent_kafka)
try:
    from kafka import KafkaProducer  # type: ignore
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False


class KafkaTransactionPublisher:
    """Publishes transaction events to Kafka with user_id partition keying."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions.raw",
        dlq_topic: str = "deadletter.transactions",
        mock_mode: bool = False,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.mock_mode = mock_mode or not KAFKA_AVAILABLE

        self.producer = None
        self.published_messages: List[Dict[str, Any]] = []
        self.dlq_messages: List[Dict[str, Any]] = []

        if not self.mock_mode:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=3,
                )
                logger.info(f"Connected KafkaProducer to {bootstrap_servers}")
            except Exception as e:
                logger.warning(f"Failed to connect to Kafka broker ({e}). Falling back to mock mode.")
                self.mock_mode = True

    def publish_event(self, transaction_dict: Dict[str, Any], topic: Optional[str] = None) -> bool:
        """Publish a single transaction event dictionary to Kafka.
        
        Uses user_id as key for partition routing.
        """
        target_topic = topic or self.topic
        user_id = transaction_dict.get("user_id", "")

        if self.mock_mode:
            self.published_messages.append({"topic": target_topic, "key": user_id, "value": transaction_dict})
            return True

        try:
            future = self.producer.send(target_topic, key=user_id, value=transaction_dict)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            logger.debug(f"Published event {transaction_dict.get('transaction_id')} to {record_metadata.topic}:{record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Error publishing message to Kafka: {e}")
            self.publish_to_dlq(transaction_dict, error_message=str(e))
            return False

    def publish_to_dlq(self, payload: Any, error_message: str) -> None:
        """Route unparseable or failed message to Dead Letter Queue topic."""
        dlq_event = {
            "raw_payload": str(payload),
            "error_message": error_message,
            "source_topic": self.topic,
        }
        if self.mock_mode:
            self.dlq_messages.append({"topic": self.dlq_topic, "value": dlq_event})
        else:
            try:
                self.producer.send(self.dlq_topic, value=dlq_event)
                self.producer.flush()
            except Exception as ex:
                logger.error(f"Failed to publish to DLQ topic: {ex}")

    def close(self) -> None:
        """Close producer connection."""
        if self.producer is not None:
            self.producer.close()
