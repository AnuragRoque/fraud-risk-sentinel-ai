"""Daily Metrics Rollup DAG — SentinelStream Airflow.

Scheduled batch workflow that aggregates daily fraud metrics and populates the DAILY_FRAUD_METRICS warehouse table.
"""

from datetime import datetime, timezone
import logging
from typing import Dict, Any, Optional

from warehouse.loader import WarehouseLoader

logger = logging.getLogger(__name__)


def run_daily_metrics_task(
    db_path: str = "warehouse/sentinelstream.db",
    loader: Optional[WarehouseLoader] = None,
) -> Dict[str, Any]:
    """Task function aggregating transaction metrics for today and inserting into DAILY_FRAUD_METRICS."""
    if loader is None:
        loader = WarehouseLoader(db_path=db_path)
    today_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    summary = loader.get_summary_metrics()
    total_tx = summary["total_tx_count"]
    total_vol = summary["total_amount"]
    high_cnt = summary["high_risk_count"]
    med_cnt = summary["medium_risk_count"]
    low_cnt = summary["low_risk_count"]
    avg_score = summary["avg_risk_score"]
    alert_rate = round(high_cnt / total_tx, 4) if total_tx > 0 else 0.0

    with loader.get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO DAILY_FRAUD_METRICS
            (metric_date, total_transactions, total_volume, high_risk_count, medium_risk_count, low_risk_count, avg_risk_score, fraud_alert_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (today_date_str, total_tx, total_vol, high_cnt, med_cnt, low_cnt, avg_score, alert_rate)
        )
        conn.commit()

    result = {
        "metric_date": today_date_str,
        "total_transactions": total_tx,
        "total_volume": total_vol,
        "high_risk_count": high_cnt,
        "fraud_alert_rate": alert_rate,
    }
    logger.info(f"Daily metrics rollup completed: {result}")
    return result
