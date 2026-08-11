"""Streaming Feature Transformer — SentinelStream Streaming.

Enriches raw transaction micro-batches with real-time velocity, monetary, and novelty features.
"""

from typing import Dict, List, Any
from ml.features import FeatureExtractor, FEATURE_NAMES
from producer.schemas import TransactionEvent
from streaming.windows import SlidingWindowAggregator


class StreamingFeatureTransformer:
    """Enriches transaction stream micro-batches with real-time online feature vectors."""

    def __init__(self) -> None:
        self.extractor = FeatureExtractor()
        self.aggregator = SlidingWindowAggregator(watermark_minutes=10)
        self.user_history_buffers: Dict[str, List[TransactionEvent]] = {}

    def transform_event(self, event: TransactionEvent) -> Dict[str, Any]:
        """Enrich a single incoming streaming TransactionEvent with features."""
        user_id = event.user_id
        history = self.user_history_buffers.get(user_id, [])

        # Extract complete feature dict using FeatureExtractor
        features = self.extractor.extract_features(event, history)

        # Update history buffer
        if user_id not in self.user_history_buffers:
            self.user_history_buffers[user_id] = []
        self.user_history_buffers[user_id].append(event)

        # Build feature-enriched output payload
        payload = event.to_dict()
        payload["features"] = features
        payload["feature_vector"] = self.extractor.to_feature_vector(features)
        return payload

    def transform_batch(self, events: List[TransactionEvent]) -> List[Dict[str, Any]]:
        """Transform a batch of streaming events sequentially."""
        enriched_batch = []
        # Sort events by timestamp
        sorted_events = sorted(events, key=lambda e: e.event_time)
        for evt in sorted_events:
            enriched_batch.append(self.transform_event(evt))
        return enriched_batch
