"""Unit tests for SentinelStream Milestone 7 — Airflow Workflow DAGs."""

import tempfile
from pathlib import Path
import pytest

from airflow.dags.backfill import run_historical_backfill
from airflow.dags.daily_metrics import run_daily_metrics_task
from airflow.dags.data_quality import run_data_quality_audit
from airflow.dags.training import run_model_retraining_workflow
from warehouse.loader import WarehouseLoader


def test_daily_metrics_dag_task():
    """Test daily metrics rollup execution and warehouse table insertion."""
    loader = WarehouseLoader(db_path=":memory:")
    # Seed a dummy scored event
    scored_sample = [{
        "transaction_id": "tx_m_1",
        "event_time": "2026-08-11T12:00:00Z",
        "user_id": "usr_1",
        "account_id": "acc_1",
        "amount": 1200.0,
        "currency": "INR",
        "merchant_id": "m_1",
        "merchant_category": "groceries",
        "payment_method": "UPI",
        "device_id": "dev_1",
        "ip_address": "10.0.0.1",
        "latitude": 28.0,
        "longitude": 77.0,
        "country": "IN",
        "city": "Delhi",
        "risk_score": 0.2,
        "risk_level": "LOW",
        "rule_score": 0.0,
        "ml_anomaly_score": 0.1,
        "model_version": "v1",
        "reasons": [],
    }]
    loader.load_scored_events(scored_sample)

    res = run_daily_metrics_task(loader=loader)
    assert res["total_transactions"] == 1
    assert res["total_volume"] == 1200.0

    with loader.get_connection() as conn:
        metrics_row = conn.execute("SELECT * FROM DAILY_FRAUD_METRICS").fetchone()
        assert metrics_row is not None
        assert metrics_row["total_transactions"] == 1


def test_data_quality_audit_task():
    """Test data quality audit task on clean vs faulty data."""
    loader = WarehouseLoader(db_path=":memory:")
    
    # 1. Clean data audit
    clean_sample = [{
        "transaction_id": "tx_dq_clean",
        "event_time": "2026-08-11T12:00:00Z",
        "user_id": "usr_dq",
        "account_id": "acc_dq",
        "amount": 500.0,
        "currency": "INR",
        "merchant_id": "m_dq",
        "merchant_category": "food",
        "payment_method": "UPI",
        "device_id": "dev_dq",
        "ip_address": "10.0.0.1",
        "latitude": 28.0,
        "longitude": 77.0,
        "country": "IN",
        "city": "Delhi",
        "risk_score": 0.1,
        "risk_level": "LOW",
        "rule_score": 0.0,
        "ml_anomaly_score": 0.0,
        "model_version": "v1",
        "reasons": [],
    }]
    loader.load_scored_events(clean_sample)
    report_clean = run_data_quality_audit(loader=loader)
    assert report_clean["status"] == "PASSED"
    assert report_clean["issues_count"] == 0

    # 2. Inject negative amount and check audit failure detection
    with loader.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO STG_TRANSACTIONS
            (transaction_id, event_time, user_id, account_id, amount, currency, merchant_id, merchant_category, payment_method, device_id, ip_address, latitude, longitude, country, city)
            VALUES ('tx_bad_neg', '2026-08-11T12:00:00Z', 'usr_bad', 'acc_bad', -100.0, 'INR', 'm_bad', 'food', 'UPI', 'dev_bad', '10.0.0.1', 28.0, 77.0, 'IN', 'Delhi')
            """
        )
        conn.commit()

    report_faulty = run_data_quality_audit(loader=loader)
    assert report_faulty["status"] == "FAILED"
    assert report_faulty["issues_count"] >= 1
    assert any("negative" in issue.lower() for issue in report_faulty["issues"])


def test_historical_backfill_task():
    """Test historical backfill task populating warehouse database."""
    loader = WarehouseLoader(db_path=":memory:")
    res = run_historical_backfill(num_events=25, days_back=3, loader=loader)

    assert res["status"] == "SUCCESS"
    assert res["num_events_backfilled"] == 25

    summary = loader.get_summary_metrics()
    assert summary["total_tx_count"] == 25


def test_model_retraining_approval_workflow():
    """Test automated model retraining workflow and quality approval gate."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        res = run_model_retraining_workflow(
            model_version="iforest_dag_test",
            min_f1_threshold=0.30,
            output_dir=tmp_dir,
        )

        assert res["model_version"] == "iforest_dag_test"
        assert res["status"] in ("APPROVED", "REJECTED")
        assert "candidate_f1" in res
