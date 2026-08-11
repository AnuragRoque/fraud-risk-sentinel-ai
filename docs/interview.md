# SentinelStream — Technical Interview Guide & Architecture Defense

This document provides structured engineering defenses, architectural trade-offs, and failure mode explanations for SentinelStream.

---

## 1. Executive System Walkthrough

> *"Walk me through the SentinelStream platform architecture."*

```text
TRANSACTION SOURCE
       │ (JSON Payloads)
       ▼
KAFKA EVENT BUS (Topic: transactions.raw, Keyed by user_id)
       │
       ▼
SPARK STRUCTURED STREAMING / REAL-TIME STREAM PROCESSOR
       ├─► Schema Validation & DLQ Router (deadletter.transactions)
       ├─► Event-Time Sliding Windows (1m, 5m, 1h) with 10m Watermark
       └─► Real-Time Feature Extractor (Velocity, Monetary, Geo, Novelty)
       │
       ▼
HYBRID RISK ENGINE
       ├─► Deterministic Business Rule Engine (5 Rules)
       ├─► Isolation Forest ML Anomaly Scoring (Normalized S_ml in [0.0, 1.0])
       ├─► Weighted Risk Score Aggregator (S_risk in [0.0, 1.0])
       └─► Explainability Engine (Human-readable decision reason strings)
       │
       ├─────────────────────────────────┐
       ▼                                 ▼
KAFKA ALERT TOPIC (fraud.alerts)    ANALYTICAL WAREHOUSE (Snowflake / SQLite)
 [HIGH Risk Alerts]                  [STG_TRANSACTIONS, TRANSACTION_SCORES]
                                         │
                                         ▼
                                   AIRFLOW BATCH ORCHESTRATION
                                    ├─► Daily Metrics Rollup
                                    ├─► Data Quality Validation
                                    └─► Model Retraining & Approval Gate

PROMETHEUS & GRAFANA OBSERVABILITY STACK
 [Metrics: Throughput, Latency, Consumer Lag, DLQ Errors, Risk Levels]
```

---

## 2. Deep-Dive Interview Questions & Engineering Defenses

### Q1: Why use Kafka for event ingestion?
- **Decoupling**: Decouples high-volume synthetic transaction generation from stream processing, risk scoring, and analytical storage.
- **Partition Keying by `user_id`**: Kafka guarantees strict sequential ordering within a partition. Keying messages by `user_id` ensures all transactions for a specific user land in the same partition and are processed in chronological order.
- **Trade-Off**: Partitioning by `user_id` can create hot-key skew for extremely active users. Mitigation: Monitor per-partition consumer lag and apply compound partition keys for extreme scale.
- **Delivery Guarantees**: Configured with **At-Least-Once** semantics. Idempotency is enforced downstream at the warehouse sink via `transaction_id` deduplication.

---

### Q2: How does Spark Structured Streaming handle event time, windows, and watermarking?
- **Event Time vs. Processing Time**: Features (velocity counts, monetary sums) are computed strictly on `event_time` (ISO-8601 UTC timestamp from the transaction payload) rather than arrival wall-clock time.
- **Sliding Windows**: Computes rolling 1-minute, 5-minute, and 1-hour aggregations sliding every minute.
- **Watermarking**: Enforces a 10-minute watermark (`withWatermark("event_time", "10 minutes")`). Events arriving older than 10 minutes past the maximum observed event time are dropped to prevent unbounded state memory growth.
- **Fault Tolerance**: Spark logs write-ahead logs (WAL) to checkpoint storage, enabling exact state recovery on driver/executor restart.

---

### Q3: Why Isolation Forest for fraud anomaly detection?
- **Unsupervised Nature**: Fraud patterns evolve rapidly and true historical ground-truth labels are frequently delayed or missing in real-world streaming environments.
- **Linear Time Complexity**: Isolation Forest isolates anomalies by randomly building decision trees. Outliers require fewer splits (shorter path length). Complexity is \(O(n \log n)\), making inference fast for real-time CPU micro-batches.
- **Score Normalization**: Raw decision scores range from \([-0.5, 0.5]\). We transform them into normalized anomaly scores \(S_{\text{ml}} \in [0.0, 1.0]\) using \(S_{\text{ml}} = \text{clamp}(0.5 - \text{decision\_function}(X), 0.0, 1.0)\).
- **Data Leakage Protection**: Evaluation labels (`is_fraud_ground_truth`) are strictly isolated from the 14-feature input vector.

---

### Q4: How does the Hybrid Risk Engine work?
- **Relying on ML alone** causes false positives on non-fraudulent high-value purchases. **Relying on static rules alone** fails to detect novel fraud tactics.
- **Hybrid Formula**:
  \[
  S_{\text{risk}} = 0.35 \cdot S_{\text{rules}} + 0.35 \cdot S_{\text{ml}} + 0.15 \cdot S_{\text{velocity}} + 0.15 \cdot S_{\text{behavior}}
  \]
- **Risk Severity Bands**:
  - `0.00 – 0.39`: `LOW`
  - `0.40 – 0.69`: `MEDIUM`
  - `0.70 – 1.00`: `HIGH`
- **Explainability**: Every high-risk transaction includes human-readable decision reasons (e.g. *"High transaction velocity detected (6 transactions in 5 minutes)"*).

---

### Q5: Why is Airflow separated from the real-time path?
- Placing Airflow directly in the real-time scoring loop introduces multi-second latency overhead and tight coupling.
- Airflow is used exclusively in the scheduled batch layer for daily metric rollups, data quality audits, historical backfills, and automated model retraining.

---

### Q6: What are the measured performance benchmarks?
- **Measured Input**: Tested up to 10,000 synthetic transaction events.
- **Throughput**: ~10.48 events/sec (single-thread Python execution baseline).
- **P95 Latency**: 150.65 ms per event scoring micro-batch.
- **RAM Footprint**: ~160 MB – 205 MB.
