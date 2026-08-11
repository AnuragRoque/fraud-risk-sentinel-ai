# System Architecture — SentinelStream

## 1. Executive Summary
SentinelStream is a real-time, event-driven fraud detection and scoring platform. It ingests financial transactions continuously, enriches events with rolling window behavioral features, scores them using a hybrid risk engine (rules + ML anomaly score), produces alerts for high-risk activity, and persists scored events to an analytical warehouse.

---

## 2. High-Level Dataflow

```text
+-------------------+       +-----------------------+       +-----------------------------+
| Synthetic Event   | ----> | Kafka Topic           | ----> | Spark Structured Streaming  |
| Producer          |       | (transactions.raw)    |       | (Validation & Windowing)    |
+-------------------+       +-----------------------+       +--------------+--------------+
                                                                           |
                                                                           v
                                                            +-----------------------------+
                                                            | Feature Engineering Engine  |
                                                            | (Velocity, Geo, Monetary)   |
                                                            +--------------+--------------+
                                                                           |
                                            +------------------------------+------------------------------+
                                            |                                                             |
                                            v                                                             v
                            +-------------------------------+                             +-------------------------------+
                            | Rule Engine                   |                             | ML Inference Engine           |
                            | - Velocity spikes             |                             | - Isolation Forest            |
                            | - Amount anomalies            |                             | - Anomaly score normalization |
                            | - Geo / Device changes        |                             +---------------+---------------+
                            +---------------+---------------+                                             |
                                            |                                                             |
                                            +------------------------------+------------------------------+
                                                                           |
                                                                           v
                                                            +-----------------------------+
                                                            | Hybrid Risk Scoring Engine  |
                                                            | - Weighted combination      |
                                                            | - Risk level categorization |
                                                            | - Explainability generator  |
                                                            +--------------+--------------+
                                                                           |
                                            +------------------------------+------------------------------+
                                            |                                                             |
                                            v                                                             v
                            +-------------------------------+                             +-------------------------------+
                            | Kafka Topic                   |                             | Kafka Topic                   |
                            | (transactions.scored)         |                             | (fraud.alerts) [HIGH risk]    |
                            +---------------+---------------+                             +---------------+---------------+
                                            |                                                             |
                                            v                                                             v
                            +-------------------------------+                             +-------------------------------+
                            | Snowflake Analytical          |                             | Observability & Alerting      |
                            | Warehouse                     |                             | (Prometheus / Grafana)        |
                            +-------------------------------+                             +-------------------------------+
```

---

## 3. Core Component Boundaries

### Producer Layer (`producer/`)
- Generates synthetic transactions adhering to canonical JSON schema.
- Simulates realistic normal user baselines and 6 specific fraud scenarios.
- Strictly segregates evaluation ground truth from online payload fields.

### Streaming Layer (`streaming/`)
- Reads raw events from Kafka.
- Performs schema validation and dead-letter queue routing for malformed payloads.
- Maintains event-time sliding windows with 10-minute watermarks.
- Computes velocity, monetary aggregation, and geographic distance features.

### Fraud Detection & ML Layer (`fraud/` & `ml/`)
- Evaluates deterministic risk rules (`velocity_rule`, `amount_rule`, `geo_rule`, `device_rule`).
- Invokes trained Isolation Forest model for unsupervised anomaly score generation.
- Computes aggregated hybrid risk score \(S_{\text{risk}} \in [0.0, 1.0]\) and assigns risk band (`LOW`, `MEDIUM`, `HIGH`).
- Attaches clear, explainable decision reasons to high-risk transactions.

### Analytical & Batch Orchestration Layer (`warehouse/` & `airflow/`)
- Stores scored transactions and alert logs in Snowflake analytical staging/marts.
- Airflow runs scheduled batch workflows for daily metrics, data quality validation, and periodic model retraining.

### Observability Layer (`monitoring/`)
- Exposes Prometheus metrics for event throughput, consumer lag, scoring latency, and alert rates.
- Displays Grafana operational dashboards for system health and fraud analytics.
