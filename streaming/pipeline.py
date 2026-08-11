"""Streaming Pipeline Runner — SentinelStream Streaming.

Defines the stream processing pipeline connecting Kafka ingestion, event parsing,
watermarking, sliding window feature generation, and sink emission.
"""

from typing import Dict, List, Any, Optional
import json
import logging

from producer.schemas import TransactionEvent
from streaming.consumer import KafkaTransactionConsumer
from streaming.features import StreamingFeatureTransformer
from streaming.schemas import get_spark_transaction_schema, PYSPARK_AVAILABLE
from streaming.windows import apply_pyspark_window_aggregations

logger = logging.getLogger(__name__)

if PYSPARK_AVAILABLE:
    from pyspark.sql import SparkSession  # type: ignore
    from pyspark.sql import functions as F  # type: ignore


class SentinelStreamPipeline:
    """Stream processing pipeline execution runner."""

    def __init__(
        self,
        consumer: Optional[KafkaTransactionConsumer] = None,
        transformer: Optional[StreamingFeatureTransformer] = None,
        mock_mode: bool = True,
    ) -> None:
        self.consumer = consumer or KafkaTransactionConsumer(mock_mode=mock_mode)
        self.transformer = transformer or StreamingFeatureTransformer()
        self.processed_records_count: int = 0
        self.output_sink: List[Dict[str, Any]] = []

    def process_micro_batch(self, events: List[TransactionEvent]) -> List[Dict[str, Any]]:
        """Process a micro-batch of transaction events: extract features and emit to sink."""
        if not events:
            return []

        enriched_records = self.transformer.transform_batch(events)
        self.processed_records_count += len(enriched_records)
        self.output_sink.extend(enriched_records)
        return enriched_records

    def run_step(self, max_records: int = 50) -> List[Dict[str, Any]]:
        """Consume next micro-batch from Kafka/Mock queue and process through feature pipeline."""
        events = self.consumer.consume_batch(max_records=max_records)
        return self.process_micro_batch(events)

    def create_pyspark_streaming_query(
        self,
        kafka_bootstrap_servers: str = "localhost:9092",
        subscribe_topic: str = "transactions.raw",
        checkpoint_dir: str = "/tmp/sentinelstream/checkpoints",
    ) -> Any:
        """Create PySpark Structured Streaming query reading from Kafka and parsing schema."""
        if not PYSPARK_AVAILABLE:
            logger.warning("PySpark is not installed in current Python environment.")
            return None

        spark = SparkSession.builder \
            .appName("SentinelStream-SparkStreaming") \
            .getOrCreate()

        schema = get_spark_transaction_schema()

        # Read Kafka Stream
        raw_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", subscribe_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Parse JSON payload
        parsed_df = raw_df.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Apply Watermarks & Sliding Window Aggregations
        windowed_df = apply_pyspark_window_aggregations(parsed_df, watermark_minutes=10)

        return windowed_df
