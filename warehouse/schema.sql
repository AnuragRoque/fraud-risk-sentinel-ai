-- SentinelStream Analytical Warehouse Schema (Snowflake / PostgreSQL / SQLite Compatible)

-- 1. Raw Transactions Table (Event Ingestion Layer)
CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
    transaction_id VARCHAR(64) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    raw_payload TEXT NOT NULL,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Staging Transactions Table (Cleaned & Normalized Layer)
CREATE TABLE IF NOT EXISTS STG_TRANSACTIONS (
    transaction_id VARCHAR(64) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    merchant_category VARCHAR(64) NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    latitude DECIMAL(9, 6) NOT NULL,
    longitude DECIMAL(9, 6) NOT NULL,
    country VARCHAR(2) NOT NULL,
    city VARCHAR(64) NOT NULL
);

-- 3. Transaction Scores Table (Scored Events Layer)
CREATE TABLE IF NOT EXISTS TRANSACTION_SCORES (
    transaction_id VARCHAR(64) PRIMARY KEY,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    risk_score DECIMAL(5, 4) NOT NULL,
    risk_level VARCHAR(16) NOT NULL,
    rule_score DECIMAL(5, 4) NOT NULL,
    ml_anomaly_score DECIMAL(5, 4) NOT NULL,
    model_version VARCHAR(64) NOT NULL,
    reasons_json TEXT NOT NULL,
    scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Fraud Alerts Table (High Risk Incidents Layer)
CREATE TABLE IF NOT EXISTS FRAUD_ALERTS (
    alert_id VARCHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    risk_score DECIMAL(5, 4) NOT NULL,
    risk_level VARCHAR(16) NOT NULL,
    reasons_json TEXT NOT NULL,
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Daily Fraud Metrics (Data Mart Layer)
CREATE TABLE IF NOT EXISTS DAILY_FRAUD_METRICS (
    metric_date DATE PRIMARY KEY,
    total_transactions INT NOT NULL,
    total_volume DECIMAL(18, 2) NOT NULL,
    high_risk_count INT NOT NULL,
    medium_risk_count INT NOT NULL,
    low_risk_count INT NOT NULL,
    avg_risk_score DECIMAL(5, 4) NOT NULL,
    fraud_alert_rate DECIMAL(5, 4) NOT NULL
);

-- 6. Model Runs Registry (MLOps Tracking Layer)
CREATE TABLE IF NOT EXISTS MODEL_RUNS (
    model_version VARCHAR(64) PRIMARY KEY,
    algorithm VARCHAR(64) NOT NULL,
    hyperparameters_json TEXT NOT NULL,
    f1_score DECIMAL(5, 4) NOT NULL,
    precision_score DECIMAL(5, 4) NOT NULL,
    recall_score DECIMAL(5, 4) NOT NULL,
    status VARCHAR(32) NOT NULL,
    trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
