"""Historical Data Backfill DAG — SentinelStream Airflow.

Automates batch backfills of historical synthetic transaction events into warehouse marts.
"""

from datetime import datetime, timedelta, timezone
import logging
from typing import Dict, Any, Optional

from producer.generator import SyntheticDataGenerator
from streaming.features import StreamingFeatureTransformer
from fraud.risk_engine import RiskEngine
from warehouse.loader import WarehouseLoader

logger = logging.getLogger(__name__)


def run_historical_backfill(
    num_events: int = 500,
    days_back: int = 7,
    db_path: str = "warehouse/sentinelstream.db",
    loader: Optional[WarehouseLoader] = None,
) -> Dict[str, Any]:
    """Execute backfill of historical events over the past N days."""
    logger.info(f"Starting historical backfill DAG: {num_events} events over past {days_back} days...")

    start_time = datetime.now(timezone.utc) - timedelta(days=days_back)
    gen = SyntheticDataGenerator(seed=777, fraud_rate=0.08)
    gt_batch = gen.generate_batch(count=num_events, start_time=start_time)

    streaming_events = [gt.to_streaming_event() for gt in gt_batch]
    transformer = StreamingFeatureTransformer()
    risk_engine = RiskEngine()
    if loader is None:
        loader = WarehouseLoader(db_path=db_path)

    enriched_batch = transformer.transform_batch(streaming_events)
    scored_batch = risk_engine.score_batch(enriched_batch)
    inserted_count = loader.load_scored_events(scored_batch)

    summary = {
        "num_events_backfilled": inserted_count,
        "days_backfilled": days_back,
        "start_time": start_time.isoformat(),
        "status": "SUCCESS",
    }
    logger.info(f"Backfill finished: {summary}")
    return summary
