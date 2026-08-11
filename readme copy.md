# SentinelStream — Real-Time Fraud-Scoring Data Platform

> **SentinelStream is a production-style real-time fraud-scoring platform that combines Kafka event streaming, Spark Structured Streaming, real-time behavioral feature engineering, Isolation Forest anomaly detection, hybrid risk scoring, analytical warehousing, batch orchestration, and end-to-end observability.**

---

## Architecture Overview

```text
                    TRANSACTION SOURCES
                           |
                           v
                 +---------------------+
                 | Kafka Event Bus     |
                 +----------+----------+
                            |
                            v
             +-----------------------------+
             | Spark Structured Streaming  |
             |                             |
             | - Parse                     |
             | - Validate                  |
             | - Deduplicate               |
             | - Enrich                    |
             | - Window aggregation        |
             | - Feature engineering       |
             +-------------+---------------+
                           |
              +------------+------------+
              |                         |
              v                         v
       +-------------+          +--------------+
       | Rule Engine |          | ML Inference |
       |             |          |              |
       | velocity    |          | Isolation    |
       | amount      |          | Forest       |
       | device      |          |              |
       | geo         |          | anomaly      |
       +------+------+\          +------+-------+
              |                       |
              +-----------+-----------+
                          |
                          v
                 +------------------+
                 | Risk Engine      |
                 |                  |
                 | ML score         |
                 | Rule score       |
                 | Velocity score   |
                 | Behavior score   |
                 +--------+---------+
                          |
                  +-------+-------+
                  |               |
                  v               v
           fraud.alerts      Snowflake
             Kafka topic       warehouse
                                |
                                v
                          Analytics layer
```

---

## Primary Design Principles

1. **Real-time path stays real-time**: Kafka → Spark → Feature Engineering → ML → Risk Score → Alert. Airflow is strictly for scheduled/batch workflows.
2. **ML is a component, not the whole system**: Risk Score = Rule Score + Velocity Score + Behavior Score + ML Anomaly Score.
3. **No data leakage**: Evaluation ground truth is strictly separated from online feature generation.
4. **Observable & reproducible**: Continuous metrics, tracing, structured logs, and empirical benchmark reporting.

---

## Directory Structure

```text
sentinelstream/
├── docs/                      # Technical docs, research, ADRs, benchmarks, status
│   ├── research/              # Deep-dive tech research (Kafka, Spark, Isolation Forest)
│   ├── architecture.md        # System architecture specification
│   ├── decisions.md           # Architectural Decision Records (ADRs)
│   └── project_status.md      # Milestone implementation progress tracking
├── producer/                  # Synthetic transaction generator & scenarios
│   ├── schemas.py             # Pydantic schemas (event & ground truth)
│   ├── scenarios.py           # Fraud scenario generators (velocity, geo, burst, etc.)
│   ├── generator.py           # Stream generator engine
│   └── publisher.py           # Kafka event producer
├── streaming/                 # Spark Structured Streaming pipeline
├── fraud/                     # Rule engine, risk engine & explainability
├── ml/                        # Isolation Forest offline/online inference pipeline
├── warehouse/                 # Analytical warehousing schemas & SQL queries
├── airflow/                   # Batch DAGs for metrics, training, and quality checks
├── monitoring/                # Prometheus metrics & Grafana dashboards
├── tests/                     # Unit, integration, streaming, and E2E test suites
└── pyproject.toml             # Python package & project dependencies
```

---

## Milestone Roadmap

- [x] **M0 — Research & Architecture**: Tech deep dives, schema design, ADRs, project structure.
- [ ] **M1 — Synthetic Transaction Engine**: Schema-validated transaction generator with 6 fraud scenarios & ground-truth isolation.
- [ ] **M2 — Offline Fraud Model**: Isolation Forest training, feature extraction, evaluation metrics (Precision/Recall/F1).
- [ ] **M3 — Kafka Pipeline**: Event publishing, partition strategy (`user_id`), consumer lag tracking, DLQ handling.
- [ ] **M4 — Spark Structured Streaming**: Stateful sliding windows, watermarks, online feature computation.
- [ ] **M5 — Hybrid Risk Engine**: Multi-signal scoring, risk bands (Low/Med/High), explainable risk reasons.
- [ ] **M6 — Analytical Warehouse**: Data warehouse models, SQL analytics, metric marts.
- [ ] **M7 — Airflow Workflow Orchestration**: Scheduled batch DAGs, model retraining pipeline.
- [ ] **M8 — Observability**: Prometheus metrics, Grafana operational dashboards, OpenTelemetry tracing.
- [ ] **M9 — Containerization & Docker Hardening**: Docker Compose multi-profile local development stack.
- [ ] **M10 — Failure Testing & Fault Tolerance**: Producer failure, consumer restart, malformed data recovery.
- [ ] **M11 — Kubernetes & Helm**: K8s manifests, resource requests/limits, Helm charts.
- [ ] **M12 — Terraform & CI/CD Pipelines**: Infrastructure as Code, GitHub Actions automated CI/CD.
- [ ] **M13 — Benchmarking & Performance Tuning**: Empirical throughput (events/sec) and P95 latency measurements.
- [ ] **M14 — Portfolio Hardening**: Final documentation, interview guides, and project audit.

---

## Quick Start (Local Setup)

```bash
# Clone and setup environment
git clone https://github.com/AnuragRoque/fraud-risk-sentinel-ai.git
cd fraud-risk-sentinel-ai

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e .[dev,streaming]

# Run unit tests
pytest tests/unit/
```

---

## License

MIT License. Developed by Anurag Singh.