"""Analytical Warehouse Client & Loader — SentinelStream Warehouse.

Loads scored transaction events and alert payloads into analytical database tables (SQLite / Snowflake).
Executes SQL analytical queries for business metric reporting.
"""

from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class WarehouseLoader:
    """Manages analytical database storage and metric query execution."""

    def __init__(self, db_path: str = "warehouse/sentinelstream.db") -> None:
        self.db_path = db_path
        self.use_memory = (db_path == ":memory:")
        self._shared_conn: Optional[sqlite3.Connection] = None

        if not self.use_memory:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        else:
            self._shared_conn = sqlite3.connect(":memory:")
            self._shared_conn.row_factory = sqlite3.Row

        self._init_schema()

    def get_connection(self) -> sqlite3.Connection:
        """Get database connection (reuses persistent connection if in-memory)."""
        if self.use_memory and self._shared_conn is not None:
            return self._shared_conn

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        """Execute DDL schema creation script."""
        schema_path = Path(__file__).parent / "schema.sql"
        if not schema_path.exists():
            logger.warning("schema.sql not found; skipping automatic DDL initialization.")
            return

        sql_script = schema_path.read_text(encoding="utf-8")
        with self.get_connection() as conn:
            conn.executescript(sql_script)
            conn.commit()

    def load_scored_events(self, scored_events: List[Dict[str, Any]]) -> int:
        """Insert a batch of scored transaction events into STG_TRANSACTIONS, TRANSACTION_SCORES, and FRAUD_ALERTS."""
        if not scored_events:
            return 0

        inserted_count = 0
        with self.get_connection() as conn:
            for item in scored_events:
                tx_id = item.get("transaction_id")
                evt_time = item.get("event_time")
                user_id = item.get("user_id")
                account_id = item.get("account_id")
                amount = float(item.get("amount", 0.0))
                currency = item.get("currency", "INR")
                merchant_id = item.get("merchant_id", "")
                merchant_category = item.get("merchant_category", "")
                payment_method = item.get("payment_method", "")
                device_id = item.get("device_id", "")
                ip_address = item.get("ip_address", "")
                lat = float(item.get("latitude", 0.0))
                lon = float(item.get("longitude", 0.0))
                country = item.get("country", "IN")
                city = item.get("city", "")

                # 1. Insert into STG_TRANSACTIONS
                conn.execute(
                    """
                    INSERT OR REPLACE INTO STG_TRANSACTIONS
                    (transaction_id, event_time, user_id, account_id, amount, currency, merchant_id, merchant_category, payment_method, device_id, ip_address, latitude, longitude, country, city)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tx_id, str(evt_time), user_id, account_id, amount, currency, merchant_id, merchant_category, payment_method, device_id, ip_address, lat, lon, country, city)
                )

                # 2. Insert into TRANSACTION_SCORES
                risk_score = float(item.get("risk_score", 0.0))
                risk_level = item.get("risk_level", "LOW")
                rule_score = float(item.get("rule_score", 0.0))
                ml_score = float(item.get("ml_anomaly_score", 0.0))
                model_ver = item.get("model_version", "v1.0")
                reasons_json = json.dumps(item.get("reasons", []))

                conn.execute(
                    """
                    INSERT OR REPLACE INTO TRANSACTION_SCORES
                    (transaction_id, event_time, user_id, amount, risk_score, risk_level, rule_score, ml_anomaly_score, model_version, reasons_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tx_id, str(evt_time), user_id, amount, risk_score, risk_level, rule_score, ml_score, model_ver, reasons_json)
                )

                # 3. Insert into FRAUD_ALERTS if HIGH risk
                if risk_level == "HIGH":
                    alert_id = f"alt_{tx_id}"
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO FRAUD_ALERTS
                        (alert_id, transaction_id, user_id, amount, risk_score, risk_level, reasons_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (alert_id, tx_id, user_id, amount, risk_score, risk_level, reasons_json)
                    )

                inserted_count += 1

            conn.commit()

        return inserted_count

    def get_summary_metrics(self) -> Dict[str, Any]:
        """Query total transaction count, total volume, average risk score, and high risk count."""
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(transaction_id) as total_tx_count,
                    COALESCE(SUM(amount), 0.0) as total_amount,
                    COALESCE(AVG(risk_score), 0.0) as avg_risk_score,
                    COALESCE(SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END), 0) as high_risk_count,
                    COALESCE(SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END), 0) as medium_risk_count,
                    COALESCE(SUM(CASE WHEN risk_level = 'LOW' THEN 1 ELSE 0 END), 0) as low_risk_count
                FROM TRANSACTION_SCORES
                """
            ).fetchone()

            return {
                "total_tx_count": row["total_tx_count"],
                "total_amount": round(row["total_amount"], 2),
                "avg_risk_score": round(row["avg_risk_score"], 4),
                "high_risk_count": row["high_risk_count"],
                "medium_risk_count": row["medium_risk_count"],
                "low_risk_count": row["low_risk_count"],
            }
