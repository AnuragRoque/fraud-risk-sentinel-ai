-- SentinelStream Analytical Marts & Reporting Queries (Section 31)

-- Query 1: Daily Transaction Volume and Fraud Alert Rate
SELECT
    DATE(s.event_time) AS tx_date,
    COUNT(s.transaction_id) AS total_transactions,
    SUM(s.amount) AS total_amount,
    AVG(s.risk_score) AS avg_risk_score,
    SUM(CASE WHEN s.risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count,
    SUM(CASE WHEN s.risk_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_count,
    ROUND(CAST(SUM(CASE WHEN s.risk_level = 'HIGH' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(s.transaction_id), 4) AS alert_rate
FROM TRANSACTION_SCORES s
GROUP BY DATE(s.event_time)
ORDER BY tx_date DESC;

-- Query 2: Fraud Risk Summary by Merchant Category
SELECT
    t.merchant_category,
    COUNT(s.transaction_id) AS total_tx_count,
    SUM(s.amount) AS total_spend,
    AVG(s.risk_score) AS avg_merchant_risk_score,
    SUM(CASE WHEN s.risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_tx_count
FROM TRANSACTION_SCORES s
JOIN STG_TRANSACTIONS t ON s.transaction_id = t.transaction_id
GROUP BY t.merchant_category
ORDER BY high_risk_tx_count DESC, avg_merchant_risk_score DESC;

-- Query 3: Geographic Fraud Density (by City)
SELECT
    t.city,
    COUNT(s.transaction_id) AS total_tx_count,
    AVG(s.risk_score) AS avg_city_risk_score,
    SUM(CASE WHEN s.risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_tx_count
FROM TRANSACTION_SCORES s
JOIN STG_TRANSACTIONS t ON s.transaction_id = t.transaction_id
GROUP BY t.city
ORDER BY high_risk_tx_count DESC, avg_city_risk_score DESC;

-- Query 4: Top Suspicious Users (Highest Average Risk & High Risk Counts)
SELECT
    s.user_id,
    COUNT(s.transaction_id) AS user_tx_count,
    SUM(s.amount) AS total_user_spend,
    AVG(s.risk_score) AS avg_user_risk_score,
    SUM(CASE WHEN s.risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_count
FROM TRANSACTION_SCORES s
GROUP BY s.user_id
HAVING SUM(CASE WHEN s.risk_level = 'HIGH' THEN 1 ELSE 0 END) > 0
ORDER BY high_risk_count DESC, avg_user_risk_score DESC
LIMIT 20;

-- Query 5: Risk Score Band Distribution
SELECT
    s.risk_level,
    COUNT(s.transaction_id) AS tx_count,
    ROUND(AVG(s.amount), 2) AS avg_amount,
    ROUND(AVG(s.rule_score), 4) AS avg_rule_score,
    ROUND(AVG(s.ml_anomaly_score), 4) AS avg_ml_score
FROM TRANSACTION_SCORES s
GROUP BY s.risk_level
ORDER BY FIELD(s.risk_level, 'HIGH', 'MEDIUM', 'LOW');
