"""Unit tests for SentinelStream Milestone 6 — Analytical Warehouse & SQL Reporting."""

from datetime import datetime, timezone
import pytest

from producer.generator import SyntheticDataGenerator
from streaming.features import StreamingFeatureTransformer
from fraud.risk_engine import RiskEngine
from warehouse.loader import WarehouseLoader


def test_warehouse_schema_initialization():
    """Test that WarehouseLoader initializes all required analytical tables."""
    loader = WarehouseLoader(db_path=":memory:")
    with loader.get_connection() as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = [t["name"] for t in tables]

    assert "RAW_TRANSACTIONS" in table_names
    assert "STG_TRANSACTIONS" in table_names
    assert "TRANSACTION_SCORES" in table_names
    assert "FRAUD_ALERTS" in table_names
    assert "DAILY_FRAUD_METRICS" in table_names
    assert "MODEL_RUNS" in table_names


def test_warehouse_event_loading_and_alert_isolation():
    """Test loading scored transaction events into staging, scores, and high-risk alerts tables."""
    loader = WarehouseLoader(db_path=":memory:")
    transformer = StreamingFeatureTransformer()
    risk_engine = RiskEngine()

    gen = SyntheticDataGenerator(seed=42, fraud_rate=0.2)
    gt_batch = gen.generate_batch(count=20)
    streaming_events = [gt.to_streaming_event() for gt in gt_batch]

    enriched = transformer.transform_batch(streaming_events)
    scored = risk_engine.score_batch(enriched)

    inserted = loader.load_scored_events(scored)
    assert inserted == 20

    summary = loader.get_summary_metrics()
    assert summary["total_tx_count"] == 20
    assert summary["total_amount"] > 0.0

    # Verify high-risk count matches alert table row count
    with loader.get_connection() as conn:
        alert_count = conn.execute("SELECT COUNT(*) as cnt FROM FRAUD_ALERTS").fetchone()["cnt"]
        assert alert_count == summary["high_risk_count"]
