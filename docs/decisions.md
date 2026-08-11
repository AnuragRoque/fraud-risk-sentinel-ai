# Architectural Decision Records (ADRs) — SentinelStream

## ADR-001: Separation of Real-Time Path and Batch Orchestration

### Status
Accepted

### Context
In stream processing architectures, orchestration tools like Apache Airflow are often incorrectly placed directly in the real-time event path, causing latency spikes and tight coupling.

### Decision
Airflow will be restricted strictly to batch/scheduled tasks (daily metric rollups, backfills, model retraining, data quality checks). The real-time path (`Kafka → Spark Streaming → ML/Rules Engine → Alert Topic`) executes continuously and independently of Airflow DAG execution.

---

## ADR-002: Isolation Forest as Baseline Anomaly Detection Model

### Status
Accepted

### Context
Financial transaction fraud data streams suffer from extreme class imbalance, evolving fraud patterns, and delayed ground-truth labels. Complex supervised neural networks or large ensemble models require constant ground-truth retraining and exhibit higher latency.

### Decision
Adopt Isolation Forest as the primary baseline ML model. Isolation Forest isolates anomalies efficiently in linear time \(O(n \log n)\), requires no initial labels, and can be evaluated quickly in real-time scoring micro-batches.

---

## ADR-003: Hybrid Risk Scoring Architecture

### Status
Accepted

### Context
Relying solely on ML anomaly scores often produces false positives for rare but non-fraudulent high-value transactions. Conversely, relying solely on static rules fails to catch novel fraud techniques.

### Decision
Implement a hybrid scoring engine that combines deterministic rule scores (velocity, geographic impossibility, device novelty) with Isolation Forest anomaly scores via a configurable weighting formula:
\[
S_{\text{risk}} = w_{\text{rules}} \cdot S_{\text{rules}} + w_{\text{ml}} \cdot S_{\text{ml}} + w_{\text{velocity}} \cdot S_{\text{velocity}} + w_{\text{behavior}} \cdot S_{\text{behavior}}
\]

---

## ADR-004: Strict Data Leakage Isolation for Ground-Truth Labels

### Status
Accepted

### Context
Including ground-truth fraud labels or post-investigation attributes in online feature vectors inflates offline model accuracy but causes severe failure during real-world streaming deployment.

### Decision
The synthetic transaction generator outputs two distinct schema structures:
1. `TransactionEvent`: The canonical streaming payload containing only fields available at event time.
2. `GroundTruthEvent`: An evaluation wrapper that bundles `TransactionEvent` with `is_fraud_ground_truth` and `fraud_scenario_type`. Ground-truth attributes are strictly stripped before events enter the Kafka ingestion stream.
