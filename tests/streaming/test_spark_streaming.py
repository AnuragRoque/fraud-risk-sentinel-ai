"""Unit and streaming tests for SentinelStream Milestone 4 — Spark Structured Streaming & Feature Pipeline."""

from datetime import datetime, timedelta, timezone
import pytest

from producer.generator import SyntheticDataGenerator
from producer.schemas import TransactionEvent
from streaming.features import StreamingFeatureTransformer
from streaming.pipeline import SentinelStreamPipeline
from streaming.schemas import get_spark_transaction_schema, PYSPARK_AVAILABLE
from streaming.windows import SlidingWindowAggregator


def test_spark_schema_availability():
    """Test PySpark schema definition function."""
    if PYSPARK_AVAILABLE:
        schema = get_spark_transaction_schema()
        assert schema is not None
        assert len(schema.fields) == 15
        field_names = [f.name for f in schema.fields]
        assert "transaction_id" in field_names
        assert "event_time" in field_names
        assert "amount" in field_names
    else:
        pytest.skip("PySpark is not installed in current test environment")


def test_sliding_window_aggregator_velocity_and_monetary():
    """Test sliding window calculation for velocity and monetary sums."""
    aggregator = SlidingWindowAggregator(watermark_minutes=10)
    user_id = "usr_window_1"
    t_start = datetime(2026, 8, 11, 10, 0, 0, tzinfo=timezone.utc)

    # History: 3 events in past 4 minutes
    history = [
        {
            "transaction_id": "tx_w1",
            "event_time": (t_start - timedelta(minutes=4)).isoformat(),
            "user_id": user_id,
            "amount": 100.0,
            "merchant_id": "m_1",
        },
        {
            "transaction_id": "tx_w2",
            "event_time": (t_start - timedelta(minutes=2)).isoformat(),
            "user_id": user_id,
            "amount": 200.0,
            "merchant_id": "m_2",
        },
        {
            "transaction_id": "tx_w3",
            "event_time": (t_start - timedelta(seconds=30)).isoformat(),
            "user_id": user_id,
            "amount": 300.0,
            "merchant_id": "m_1",
        },
    ]

    current_event = {
        "transaction_id": "tx_w4",
        "event_time": t_start.isoformat(),
        "user_id": user_id,
        "amount": 500.0,
        "merchant_id": "m_3",
    }

    win_feats = aggregator.compute_window_features(current_event, history)

    # In past 1 minute: 1 prior event (tx_w3)
    assert win_feats["tx_count_1m"] == 1.0

    # In past 5 minutes: 3 prior events (tx_w1, tx_w2, tx_w3)
    assert win_feats["tx_count_5m"] == 3.0
    assert win_feats["tx_count_1h"] == 3.0

    # Amount sum in last 5m: 100 + 200 + 300 = 600.0
    assert win_feats["amount_sum_5m"] == 600.0

    # Unique merchants in 1h: m_1, m_2 (2 unique)
    assert win_feats["unique_merchants_1h"] == 2.0


def test_sliding_window_watermark_dropping():
    """Test that events older than watermark threshold are excluded from active window features."""
    aggregator = SlidingWindowAggregator(watermark_minutes=10)
    user_id = "usr_wm_test"
    t_start = datetime(2026, 8, 11, 12, 0, 0, tzinfo=timezone.utc)

    history = [
        {
            "transaction_id": "tx_old_late",
            "event_time": (t_start - timedelta(minutes=25)).isoformat(),  # 25m old > 10m watermark
            "user_id": user_id,
            "amount": 1000.0,
            "merchant_id": "m_old",
        },
        {
            "transaction_id": "tx_recent",
            "event_time": (t_start - timedelta(minutes=3)).isoformat(),  # 3m old < 10m watermark
            "user_id": user_id,
            "amount": 250.0,
            "merchant_id": "m_new",
        },
    ]

    current_event = {
        "transaction_id": "tx_now",
        "event_time": t_start.isoformat(),
        "user_id": user_id,
        "amount": 400.0,
        "merchant_id": "m_now",
    }

    win_feats = aggregator.compute_window_features(current_event, history)

    # tx_old_late (25m old) should be dropped by 10m watermark
    assert win_feats["tx_count_1h"] == 1.0  # Only tx_recent counted
    assert win_feats["unique_merchants_1h"] == 1.0


def test_streaming_feature_transformer():
    """Test batch transformation using StreamingFeatureTransformer."""
    transformer = StreamingFeatureTransformer()
    gen = SyntheticDataGenerator(seed=42)
    gt_batch = gen.generate_batch(count=15)
    events = [gt.to_streaming_event() for gt in gt_batch]

    enriched = transformer.transform_batch(events)

    assert len(enriched) == 15
    for item in enriched:
        assert "features" in item
        assert "feature_vector" in item
        assert len(item["feature_vector"]) == 14
        assert item["transaction_id"] is not None


def test_sentinelstream_pipeline_micro_batch():
    """Test SentinelStreamPipeline micro-batch processing end to end."""
    pipeline = SentinelStreamPipeline(mock_mode=True)
    gen = SyntheticDataGenerator(seed=99)
    gt_batch = gen.generate_batch(count=20)

    # Seed events into consumer queue
    streaming_dicts = [gt.to_streaming_event().to_dict() for gt in gt_batch]
    pipeline.consumer.seed_mock_messages(streaming_dicts)

    processed = pipeline.run_step(max_records=20)

    assert len(processed) == 20
    assert pipeline.processed_records_count == 20
    assert len(pipeline.output_sink) == 20
    assert processed[0]["features"]["amount"] > 0
