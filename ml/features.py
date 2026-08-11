"""Feature Extraction Engine — SentinelStream ML.

Computes offline and online feature vectors for Isolation Forest model training and inference.
Strictly prevents data leakage by consuming only time-valid prior transaction events.
"""

from datetime import datetime, timedelta, timezone
import math
from typing import Dict, List, Tuple
import numpy as np

from producer.scenarios import haversine_distance_km
from producer.schemas import TransactionEvent

FEATURE_NAMES = [
    "amount",
    "amount_zscore",
    "tx_count_1m",
    "tx_count_5m",
    "tx_count_1h",
    "amount_sum_5m",
    "avg_amount_1h",
    "amount_vs_user_avg",
    "unique_merchants_1h",
    "new_device",
    "new_location",
    "distance_from_last_tx",
    "hour_of_day",
    "day_of_week",
]


class FeatureExtractor:
    """Extracts ML feature vectors from transaction events and historical user buffers."""

    def __init__(self) -> None:
        pass

    def extract_features(
        self,
        event: TransactionEvent,
        user_history: List[TransactionEvent],
    ) -> Dict[str, float]:
        """Compute feature dictionary for a single target transaction given prior user history.
        
        Args:
            event: The target transaction event to featurize.
            user_history: List of prior transactions for this user (MUST be prior to event.event_time).
            
        Returns:
            Dict mapping feature names to numerical values.
        """
        # Filter history to events strictly before target event_time to prevent data leakage
        prior_events = [
            e for e in user_history
            if e.event_time < event.event_time and e.transaction_id != event.transaction_id
        ]

        # 1. Base Amount
        amount = float(event.amount)

        # 2. Historical Amount Z-Score
        if prior_events:
            prior_amounts = [e.amount for e in prior_events]
            mean_amt = float(np.mean(prior_amounts))
            std_amt = float(np.std(prior_amounts))
            amount_zscore = (amount - mean_amt) / std_amt if std_amt > 1e-5 else 0.0
        else:
            amount_zscore = 0.0

        # Time Windows (1m, 5m, 1h prior to target event)
        t_target = event.event_time
        t_1m_ago = t_target - timedelta(minutes=1)
        t_5m_ago = t_target - timedelta(minutes=5)
        t_1h_ago = t_target - timedelta(hours=1)

        events_1m = [e for e in prior_events if e.event_time >= t_1m_ago]
        events_5m = [e for e in prior_events if e.event_time >= t_5m_ago]
        events_1h = [e for e in prior_events if e.event_time >= t_1h_ago]

        # 3-5. Velocity Counts
        tx_count_1m = len(events_1m)
        tx_count_5m = len(events_5m)
        tx_count_1h = len(events_1h)

        # 6-7. Monetary Aggregations
        amount_sum_5m = float(sum(e.amount for e in events_5m))
        avg_amount_1h = float(np.mean([e.amount for e in events_1h])) if events_1h else amount

        # 8. Amount vs 1h Average Ratio
        amount_vs_user_avg = amount / avg_amount_1h if avg_amount_1h > 0 else 1.0

        # 9. Diversity Feature
        unique_merchants_1h = len(set(e.merchant_id for e in events_1h))

        # 10-11. Device & Location Novelty Signals
        prior_devices = set(e.device_id for e in prior_events)
        new_device = 1.0 if (prior_devices and event.device_id not in prior_devices) else 0.0

        prior_cities = set(e.city for e in prior_events)
        new_location = 1.0 if (prior_cities and event.city not in prior_cities) else 0.0

        # 12. Geographic Distance Signal
        if prior_events:
            # Most recent prior event
            last_event = max(prior_events, key=lambda e: e.event_time)
            dist_km = haversine_distance_km(
                last_event.latitude, last_event.longitude,
                event.latitude, event.longitude
            )
        else:
            dist_km = 0.0

        # 13-14. Temporal Features
        hour_of_day = float(event.event_time.hour)
        day_of_week = float(event.event_time.weekday())

        return {
            "amount": amount,
            "amount_zscore": round(amount_zscore, 4),
            "tx_count_1m": float(tx_count_1m),
            "tx_count_5m": float(tx_count_5m),
            "tx_count_1h": float(tx_count_1h),
            "amount_sum_5m": round(amount_sum_5m, 2),
            "avg_amount_1h": round(avg_amount_1h, 2),
            "amount_vs_user_avg": round(amount_vs_user_avg, 4),
            "unique_merchants_1h": float(unique_merchants_1h),
            "new_device": new_device,
            "new_location": new_location,
            "distance_from_last_tx": round(dist_km, 2),
            "hour_of_day": hour_of_day,
            "day_of_week": day_of_week,
        }

    def to_feature_vector(self, feature_dict: Dict[str, float]) -> List[float]:
        """Convert feature dict to an ordered numerical vector matching FEATURE_NAMES."""
        return [feature_dict[name] for name in FEATURE_NAMES]

    def extract_dataset(
        self,
        events: List[TransactionEvent],
    ) -> Tuple[np.ndarray, List[Dict[str, float]]]:
        """Build feature matrix X from an ordered stream of transaction events.
        
        Maintains an in-memory user history buffer as it iterates sequentially through events.
        """
        user_buffers: Dict[str, List[TransactionEvent]] = {}
        feature_dicts: List[Dict[str, float]] = []

        # Sort events by timestamp
        sorted_events = sorted(events, key=lambda e: e.event_time)

        for event in sorted_events:
            history = user_buffers.get(event.user_id, [])
            feats = self.extract_features(event, history)
            feature_dicts.append(feats)

            # Append event to history buffer
            if event.user_id not in user_buffers:
                user_buffers[event.user_id] = []
            user_buffers[event.user_id].append(event)

        X = np.array([[fd[name] for name in FEATURE_NAMES] for fd in feature_dicts], dtype=np.float64)
        return X, feature_dicts
