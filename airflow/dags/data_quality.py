"""Data Quality Validation DAG — SentinelStream Airflow.

Automates data quality audits matching Section 49 specification:
validates null IDs, negative amounts, coordinate boundaries, and duplicate transaction IDs.
"""

from typing import Dict, Any, Optional
import logging

from warehouse.loader import WarehouseLoader

logger = logging.getLogger(__name__)


def run_data_quality_audit(
    db_path: str = "warehouse/sentinelstream.db",
    loader: Optional[WarehouseLoader] = None,
) -> Dict[str, Any]:
    """Run data quality validation checks against warehouse tables."""
    if loader is None:
        loader = WarehouseLoader(db_path=db_path)
    issues_found = []

    with loader.get_connection() as conn:
        # Check 1: Null or empty IDs
        null_ids = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM STG_TRANSACTIONS
            WHERE transaction_id IS NULL OR user_id IS NULL OR account_id IS NULL
            """
        ).fetchone()["cnt"]
        if null_ids > 0:
            issues_found.append(f"Found {null_ids} transactions with null or empty IDs")

        # Check 2: Negative amounts
        neg_amounts = conn.execute(
            "SELECT COUNT(*) as cnt FROM STG_TRANSACTIONS WHERE amount < 0"
        ).fetchone()["cnt"]
        if neg_amounts > 0:
            issues_found.append(f"Found {neg_amounts} transactions with invalid negative amounts")

        # Check 3: Invalid coordinate ranges
        invalid_coords = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM STG_TRANSACTIONS
            WHERE latitude < -90 OR latitude > 90 OR longitude < -180 OR longitude > 180
            """
        ).fetchone()["cnt"]
        if invalid_coords > 0:
            issues_found.append(f"Found {invalid_coords} transactions with out-of-bounds lat/lon coordinates")

        # Check 4: Duplicate transaction IDs
        duplicates = conn.execute(
            """
            SELECT COUNT(*) as cnt FROM (
                SELECT transaction_id FROM STG_TRANSACTIONS
                GROUP BY transaction_id HAVING COUNT(*) > 1
            )
            """
        ).fetchone()["cnt"]
        if duplicates > 0:
            issues_found.append(f"Found {duplicates} duplicate transaction ID entries")

        total_records = conn.execute("SELECT COUNT(*) as cnt FROM STG_TRANSACTIONS").fetchone()["cnt"]

    status = "PASSED" if not issues_found else "FAILED"
    report = {
        "status": status,
        "total_records_checked": total_records,
        "issues_count": len(issues_found),
        "issues": issues_found,
    }
    logger.info(f"Data quality audit finished [{status}]: {report}")
    return report
