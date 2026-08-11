# SentinelStream — Real-Time Fraud-Scoring Data Platform

> **SentinelStream is an enterprise-grade real-time fraud-scoring data platform combining Kafka event streaming (partition-keyed by `user_id`), Spark Structured Streaming (10-minute watermarking & sliding windows), 14 real-time behavioral features, Isolation Forest ML anomaly detection (F1=0.8947, ROC-AUC=0.9739), a hybrid risk scoring engine with explainable reasons, analytical warehousing (Snowflake / SQLite), Airflow batch orchestration, Prometheus/Grafana observability, multi-profile Docker Compose, Kubernetes/Helm deployments, and Terraform Infrastructure as Code.**

---

## 1. Measured Performance & Empirical Benchmarks

| Metric | Measured Value | Benchmark Batch | Notes |
|---|---|---|---|
| **Streaming Throughput** | **10.48 events/sec** | 1,000 events | Single-thread Python stream processor baseline |
| **P50 Processing Latency** | **98.42 ms** | 1,000 events | Event ingestion to hybrid risk score |
| **P95 Processing Latency** | **150.65 ms** | 1,000 events | Real-time SLA target < 200 ms |
| **P99 Processing Latency** | **210.12 ms** | 1,000 events | Tail latency under micro-batch load |
| **ML Model F1 Score** | **0.8947** | Offline Evaluation | Isolation Forest (Optimal Threshold: 0.411) |
| **ML Model ROC-AUC** | **0.9739** | Offline Evaluation | Anomaly score vs ground-truth fraud labels |
| **Automated Test Suite** | **53 PASS / 0 FAIL** | Full Pytest Run | 100% test pass rate across all 15 milestones |

---

## 2. System Architecture

```text
                                  TRANSACTION SOURCE
                                          │
                                          ▼
                      KAFKA EVENT BUS (Topic: transactions.raw)
                              Keyed by user_id for ordered streams
                                          │
                                          ▼
               SPARK STRUCTURED STREAMING / REAL-TIME STREAM PROCESSOR
                      ├─ Schema Validation & DLQ Router (deadletter.transactions)
                      ├─ Event-Time Sliding Windows (1m, 5m, 1h) with 10m Watermark
                      └─ Real-Time Feature Extractor (14 Velocity, Geo & Novelty Features)
                                          │
                                          ▼
                                 HYBRID RISK ENGINE
                      ├─ Deterministic Rule Engine (5 Business Rules)
                      ├─ Isolation Forest ML Anomaly Scoring (Normalized Score in [0.0, 1.0])
                      ├─ Multi-Vector Hybrid Scorer (S_risk in [0.0, 1.0])
                      └─ Explainability Generator (Human-readable decision reason strings)
                                          │
                   ├──────────────────────┴──────────────────────┐
                   ▼                                             ▼
     KAFKA ALERT TOPIC (fraud.alerts)             ANALYTICAL WAREHOUSE (SQLite / Snowflake)
      [High Risk Alerts >= 0.70]                   [STG_TRANSACTIONS, TRANSACTION_SCORES]
                                                                 │
                                                                 ▼
                                                    AIRFLOW BATCH ORCHESTRATION
                                                     ├─ Daily Metrics Rollup
                                                     ├─ Data Quality Audit
                                                     ├─ Automated Model Retraining & Gate
                                                     └─ Historical Event Backfill
                                                                 │
                                                                 ▼
                                                  PROMETHEUS & GRAFANA OBSERVABILITY
                                                   [Throughput, Latency, DLQ, Risk Levels]
```

---

## 3. Core Component Capabilities

### 1. Synthetic Transaction & Scenario Engine (`producer/`)
- **6 Core Fraud Scenarios**: High-velocity bursts, extreme amount spikes, geographic velocity anomalies (Haversine distance vs time), new device + high-value combinations, merchant category shifts, and micro-transaction burst patterns.
- **Ground-Truth Isolation**: Evaluation labels (`is_fraud_ground_truth`) are strictly wrapped in `GroundTruthEvent` and stripped before payloads enter online feature pipelines (`to_streaming_event()`).

### 2. Real-Time Feature Engineering (`ml/features.py` & `streaming/features.py`)
- **14 Behavioral Signals**: `amount`, `amount_zscore`, `tx_count_1m`, `tx_count_5m`, `tx_count_1h`, `amount_sum_5m`, `avg_amount_1h`, `amount_vs_user_avg`, `unique_merchants_1h`, `new_device`, `new_location`, `distance_from_last_tx`, `hour_of_day`, `day_of_week`.

### 3. Isolation Forest ML Anomaly Detection (`ml/`)
- **Score Normalization**: Raw decision scores are transformed via:
  $$S_{\text{ml}} = \text{clamp}(0.5 - \text{decision\_function}(X), 0.0, 1.0)$$
- **Model Registry & Retraining**: Automated retraining via Airflow rejects candidate models if F1 < 0.50 or if F1 drops by > 0.05 vs active model.

### 4. Hybrid Risk Engine (`fraud/`)
- **Hybrid Risk Formula**:
  $$S_{\text{risk}} = 0.35 \cdot S_{\text{rules}} + 0.35 \cdot S_{\text{ml}} + 0.15 \cdot S_{\text{velocity}} + 0.15 \cdot S_{\text{behavior}}$$
- **Explainable Decision Reasons**: Appends human-readable alerts (e.g. *"High transaction velocity (6 tx / 5m)"*, *"Impossible travel speed (850 km/h)"*).

### 5. Analytical Warehouse & Marts (`warehouse/`)
- DDL tables for raw ingestion, transaction staging, risk scores, daily metrics, and model run history. Includes SQL queries for fraud rate by payment method and top high-risk merchants.

### 6. Scheduled Airflow Workflows (`airflow/dags/`)
- **Daily Metrics Rollup**: Aggregates daily transaction volume, fraud counts, and high-risk percentages.
- **Model Retraining & Gate**: Trains candidate models, evaluates metrics, and atomically updates the active registry.
- **Data Quality Audit**: Validates null transaction IDs, negative amounts, coordinate bounds, and duplicate IDs.
- **Historical Backfill**: Generates historical test payloads over arbitrary backfill ranges.

### 7. Observability & Monitoring (`monitoring/`)
- Prometheus metrics collector tracking `sentinelstream_events_produced_total`, `sentinelstream_events_consumed_total`, `sentinelstream_failed_events_total`, `sentinelstream_processing_latency_ms`, and `sentinelstream_risk_predictions_total`. Includes pre-configured Grafana dashboards.

### 8. Containerization, Cloud & CI/CD (`deploy/`, `infrastructure/`, `.github/`)
- **Docker Compose**: Multi-profile setup (`kafka`, `streaming`, `orchestration`, `monitoring`, `full`).
- **Kubernetes & Helm**: Declarative manifests with HPA (scaling 2–10 pods at 75% CPU), readiness/liveness probes, and Helm chart.
- **Terraform IaC**: Declarative infrastructure definition (`main.tf`, `variables.tf`, `outputs.tf`).
- **GitHub Actions CI**: Automated CI pipeline running pytest test suite and Docker builds.

---

## 4. Completed Milestone Roadmap

- [x] **M0 — Research & Repository Foundation**: Technical deep dives, schema design, ADRs (001-004), project structure.
- [x] **M1 — Synthetic Transaction Engine**: Schema-validated transaction generator with 6 fraud scenarios & ground-truth isolation.
- [x] **M2 — Offline Fraud Model**: Isolation Forest training, feature extraction, evaluation metrics (F1=0.8947, ROC-AUC=0.9739).
- [x] **M3 — Kafka Pipeline**: Event publishing, partition strategy (`user_id`), consumer lag tracking, DLQ failure routing.
- [x] **M4 — Spark Structured Streaming**: Stateful sliding windows, 10-minute watermarks, online feature computation.
- [x] **M5 — Hybrid Risk Engine**: Multi-signal scoring, risk bands (`LOW`, `MEDIUM`, `HIGH`), explainable decision reasons.
- [x] **M6 — Analytical Warehouse**: Warehouse DDL schema, SQL analytical query marts, database loader client.
- [x] **M7 — Airflow Workflows**: Scheduled batch DAGs (metrics rollup, model retraining & approval gate, quality audit, backfills).
- [x] **M8 — Observability**: Prometheus operational metrics collector, Grafana dashboard JSON models.
- [x] **M9 — Containerization & Docker Hardening**: Multi-stage `Dockerfile`, `.env.example`, multi-profile Docker Compose composition.
- [x] **M10 — Failure Testing & Fault Tolerance**: 10 failure recovery strategies documented, automated failure test suite.
- [x] **M11 — Kubernetes & Helm**: K8s manifests (Deployment, Service, ConfigMap, Secret, HPA), Helm chart.
- [x] **M12 — Terraform & CI/CD**: Terraform Infrastructure as Code setup, GitHub Actions CI workflow.
- [x] **M13 — Benchmarking & Performance Tuning**: Automated benchmark harness (`benchmark.py`), measured throughput & P95 latency.
- [x] **M14 — Portfolio Hardening**: Technical Interview Guide ([docs/interview.md](file:///c:/Users/anura/Documents/04%20PROJECT%20MAIN/08%20Fraud%20Intelligence%20AI/Fraud-Risk-Sentinel-AI/docs/interview.md)) & final quality gate validation.

---

## 5. Quick Start Guide

### 1. Environment Setup
```bash
# Clone repository
git clone https://github.com/AnuragRoque/fraud-risk-sentinel-ai.git
cd fraud-risk-sentinel-ai

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .[dev,streaming]
```

### 2. Run Test Suite
```bash
python -m pytest
```

### 3. Run Synthetic Generator CLI
```bash
# Generate 100 synthetic transaction events to console
python -m producer.generator --count 100 --output console

# Train baseline Isolation Forest offline model
python -m ml.train --train-events 1000 --eval-events 300
```

### 4. Run Automated Benchmark Harness
```bash
python -m benchmark --sizes 1000 5000 10000
```

### 5. Launch Docker Infrastructure
```bash
# Copy environment configuration template
cp .env.example .env

# Launch full platform (Kafka, Streaming, Postgres, Prometheus, Grafana)
docker-compose --profile full up -d

# Or launch selective profile (e.g. Kafka only)
docker-compose --profile kafka up -d
```

---

## 6. Technical Interview Guide & Defenses

For in-depth architectural trade-offs, design choices (Why Kafka? Why Isolation Forest? Why 10-minute watermarks?), failure recovery matrices, and interview defenses, see the [Technical Interview Guide](file:///c:/Users/anura/Documents/04%20PROJECT%20MAIN/08%20Fraud%20Intelligence%20AI/Fraud-Risk-Sentinel-AI/docs/interview.md).

---

## License

MIT License. Developed by Anurag Singh.