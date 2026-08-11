"""Windowed Aggregations Engine — SentinelStream Streaming.

Implements event-time sliding window aggregations over user transaction streams
with watermarks to compute rolling velocity, monetary sum, and diversity features.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import numpy as np

try:
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql import DataFrame  # type: ignore
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class SlidingWindowAggregator:
    """Python-native sliding window aggregator for stream micro-batches or memory processing."""

    def __init__(self, watermark_minutes: int = 10) -> None:
        self.watermark_delta = timedelta(minutes=watermark_minutes)
        self.max_event_time: Optional[datetime] = None

    def compute_window_features(
        self,
        current_event: Dict[str, Any],
        history: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        """Compute rolling window features for an event against prior user history."""
        # Convert event_time string to datetime
        evt_time_raw = current_event["event_time"]
        if isinstance(evt_time_raw, str):
            evt_time = datetime.fromisoformat(evt_time_raw.replace("Z", "+00:00"))
        else:
            evt_time = evt_time_raw

        if evt_time.tzinfo is None:
            evt_time = evt_time.replace(tzinfo=timezone.utc)

        # Track watermark
        if self.max_event_time is None or evt_time > self.max_event_time:
            self.max_event_time = evt_time

        watermark_bound = self.max_event_time - self.watermark_delta

        # Drop events before watermark bound
        valid_history = []
        for h in history:
            ht_raw = h["event_time"]
            ht = datetime.fromisoformat(ht_raw.replace("Z", "+00:00")) if isinstance(ht_raw, str) else ht_raw
            if ht.tzinfo is None:
                ht = ht.replace(tzinfo=timezone.utc)
            if ht >= watermark_bound and ht < evt_time:
                valid_history.append((ht, h))

        t_1m = evt_time - timedelta(minutes=1)
        t_5m = evt_time - timedelta(minutes=5)
        t_1h = evt_time - timedelta(hours=1)

        events_1m = [h for ht, h in valid_history if ht >= t_1m]
        events_5m = [h for ht, h in valid_history if ht >= t_5m]
        events_1h = [h for ht, h in valid_history if ht >= t_1h]

        amount = float(current_event["amount"])
        tx_count_1m = float(len(events_1m))
        tx_count_5m = float(len(events_5m))
        tx_count_1h = float(len(events_1h))
        amount_sum_5m = float(sum(float(h["amount"]) for h in events_5m))
        avg_amount_1h = float(np.mean([float(h["amount"]) for h in events_1h])) if events_1h else amount
        unique_merchants_1h = float(len(set(h["merchant_id"] for h in events_1h)))

        return {
            "tx_count_1m": tx_count_1m,
            "tx_count_5m": tx_count_5m,
            "tx_count_1h": tx_count_1h,
            "amount_sum_5m": round(amount_sum_5m, 2),
            "avg_amount_1h": round(avg_amount_1h, 2),
            "unique_merchants_1h": unique_merchants_1h,
        }


def apply_pyspark_window_aggregations(df: Any, watermark_minutes: int = 10) -> Any:
    """Apply PySpark Structured Streaming watermarks and event-time sliding windows."""
    if not PYSPARK_AVAILABLE or df is None:
        return df

    # Enforce 10-minute watermark on event_time
    watermarked_df = df.withWatermark("event_time", f"{watermark_minutes} minutes")

    # 5-minute sliding window aggregations
    aggregated_df = watermarked_df.groupBy(
        F.window(F.col("event_time"), "5 minutes", "1 minute"),
        F.col("user_id")
    ).agg(
        F.count("transaction_id").alias("tx_count_5m"),
        F.sum("amount").alias("amount_sum_5m"),
        F.avg("amount").alias("avg_amount_5m"),
        F.approx_count_distinct("merchant_id").alias("unique_merchants_5m")
    )

    return aggregated_df
