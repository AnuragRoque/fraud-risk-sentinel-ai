"""Metrics Collector — SentinelStream Monitoring.

Collects operational Kafka, Spark Streaming, ML Inference, and Fraud Risk metrics.
Renders Prometheus text exposition format for scraping.
"""

import time
from typing import Dict, List


class MetricsCollector:
    """Singleton/Instance collector for operational SentinelStream platform metrics."""

    def __init__(self) -> None:
        # Kafka Metrics
        self.events_produced_total: int = 0
        self.events_consumed_total: int = 0
        self.failed_events_total: int = 0

        # Streaming Processing Metrics
        self.records_processed_total: int = 0
        self.processing_latencies_ms: List[float] = []

        # Risk & ML Prediction Metrics
        self.high_risk_predictions_total: int = 0
        self.medium_risk_predictions_total: int = 0
        self.low_risk_predictions_total: int = 0

    def inc_events_produced(self, count: int = 1) -> None:
        self.events_produced_total += count

    def inc_events_consumed(self, count: int = 1) -> None:
        self.events_consumed_total += count

    def inc_failed_events(self, count: int = 1) -> None:
        self.failed_events_total += count

    def inc_records_processed(self, count: int = 1) -> None:
        self.records_processed_total += count

    def observe_processing_latency(self, ms: float) -> None:
        self.processing_latencies_ms.append(ms)
        # Keep last 1000 observations to bound memory
        if len(self.processing_latencies_ms) > 1000:
            self.processing_latencies_ms = self.processing_latencies_ms[-1000:]

    def inc_risk_prediction(self, risk_level: str) -> None:
        if risk_level == "HIGH":
            self.high_risk_predictions_total += 1
        elif risk_level == "MEDIUM":
            self.medium_risk_predictions_total += 1
        elif risk_level == "LOW":
            self.low_risk_predictions_total += 1

    def get_avg_processing_latency_ms(self) -> float:
        if not self.processing_latencies_ms:
            return 0.0
        return float(sum(self.processing_latencies_ms) / len(self.processing_latencies_ms))

    def to_prometheus_text(self) -> str:
        """Render metrics in Prometheus text exposition format."""
        avg_latency = self.get_avg_processing_latency_ms()

        lines = [
            "# HELP sentinelstream_events_produced_total Total events published to Kafka raw topic",
            "# TYPE sentinelstream_events_produced_total counter",
            f"sentinelstream_events_produced_total {self.events_produced_total}",
            "",
            "# HELP sentinelstream_events_consumed_total Total events consumed from Kafka",
            "# TYPE sentinelstream_events_consumed_total counter",
            f"sentinelstream_events_consumed_total {self.events_consumed_total}",
            "",
            "# HELP sentinelstream_failed_events_total Total failed/malformed events routed to DLQ",
            "# TYPE sentinelstream_failed_events_total counter",
            f"sentinelstream_failed_events_total {self.failed_events_total}",
            "",
            "# HELP sentinelstream_records_processed_total Total records processed through streaming pipeline",
            "# TYPE sentinelstream_records_processed_total counter",
            f"sentinelstream_records_processed_total {self.records_processed_total}",
            "",
            "# HELP sentinelstream_processing_latency_ms Average micro-batch processing latency in ms",
            "# TYPE sentinelstream_processing_latency_ms gauge",
            f"sentinelstream_processing_latency_ms {avg_latency:.2f}",
            "",
            "# HELP sentinelstream_risk_predictions_total Risk prediction count by severity level",
            "# TYPE sentinelstream_risk_predictions_total counter",
            f'sentinelstream_risk_predictions_total{{level="HIGH"}} {self.high_risk_predictions_total}',
            f'sentinelstream_risk_predictions_total{{level="MEDIUM"}} {self.medium_risk_predictions_total}',
            f'sentinelstream_risk_predictions_total{{level="LOW"}} {self.low_risk_predictions_total}',
        ]

        return "\n".join(lines) + "\n"
