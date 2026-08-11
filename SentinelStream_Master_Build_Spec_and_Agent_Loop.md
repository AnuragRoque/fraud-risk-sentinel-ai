# SentinelStream — Deep Technical Build Specification
## Real-Time Fraud-Scoring Data Platform

**Document status:** Master implementation blueprint  
**Project owner:** Anurag Singh  
**Primary portfolio positioning:** Data Engineering + ML Engineering + MLOps  
**Primary development environment:** Windows 11 + WSL2 Ubuntu + Docker  
**Target:** Fully runnable local portfolio project first; production-style deployment layers added afterward

---

# 0. Executive Decision

## Build this project

**SentinelStream** is a production-style, real-time fraud-scoring platform designed to demonstrate:

- event streaming
- distributed stream processing
- real-time feature engineering
- anomaly detection
- fraud scoring
- batch + streaming architecture
- analytical warehousing
- ML lifecycle management
- observability
- containerization
- deployment
- CI/CD

The project must **not** become another LLM/RAG/MCP project.

Those capabilities are already represented elsewhere in the portfolio. SentinelStream should deliberately fill the gap around:

> **Streaming → Distributed Processing → Real-Time ML → MLOps → Production Infrastructure**

---

# 1. Portfolio Context

Anurag's current portfolio already demonstrates:

- Python engineering
- enterprise platforms
- anomaly/fraud analysis
- OCR
- name matching / entity resolution
- LLMs
- RAG
- MCP
- local AI
- spreadsheet intelligence
- reconciliation
- API development
- on-premise deployment

The existing resume specifically positions SentinelStream as:

- Kafka
- Spark Structured Streaming
- Snowflake
- Airflow
- Docker/Kubernetes
- Prometheus/Grafana
- OpenTelemetry
- Terraform

The project should eventually substantiate those claims through working implementation and evidence.

## Portfolio differentiation

### Excellia AI

```text
Spreadsheet/File
      ↓
Data Analysis
      ↓
AI / MCP
      ↓
Insights / Automation
```

Primary identity:

> Local AI + Data Intelligence + MCP

### SentinelStream

```text
Transaction Event
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
Feature Engineering
      ↓
Fraud Rules + ML
      ↓
Risk Score
      ↓
Alerts + Warehouse
      ↓
Monitoring / MLOps
```

Primary identity:

> Real-Time Data Engineering + ML Infrastructure

This distinction is intentional.

---

# 2. Project Goal

Build a platform that can ingest synthetic financial transactions continuously and produce a fraud/risk score in near real time.

The platform should answer:

1. What happened?
2. How quickly did it happen?
3. Is this transaction abnormal for this user?
4. Does it violate deterministic risk rules?
5. What is the overall risk score?
6. Why was the transaction considered risky?
7. Can the event be stored and queried later?
8. Can the system process events continuously?
9. Can the system recover from failures?
10. Can the system be monitored?
11. Can the model be retrained?
12. Can the architecture scale?

---

# 3. Non-Goals

Do **not** add complexity simply because a technology is fashionable.

Do not make SentinelStream depend on:

- LLMs
- RAG
- LangChain
- LangGraph
- MCP
- vector databases
- generative AI
- computer vision
- OCR
- chat interfaces

Those are separate portfolio capabilities.

Do not implement multiple ML algorithms merely to make the repository look larger.

Do not attempt to simulate billions of records on a laptop.

Do not pay for cloud infrastructure during the initial build.

Do not claim production SLAs or enterprise scale without measured evidence.

---

# 4. Core Architecture

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


    Airflow
       |
       +--> batch aggregation
       +--> backfill
       +--> data quality
       +--> model training
       +--> model evaluation
       +--> scheduled reporting

    Prometheus
       |
       +--> service metrics
       +--> pipeline metrics
       +--> ML metrics

    Grafana
       |
       +--> operational dashboards

    OpenTelemetry
       |
       +--> distributed traces

    Docker Compose
       |
       +--> local development

    Kubernetes + Helm
       |
       +--> later deployment

    Terraform
       |
       +--> later infrastructure management

    GitHub Actions
       |
       +--> CI/CD
```

---

# 5. Architecture Principles

## Principle 1 — Real-time path must stay real-time

Do not place Airflow in the critical event-scoring path.

Correct:

```text
Kafka → Spark → Feature Engineering → ML → Risk Score → Alert
```

Incorrect:

```text
Kafka → Airflow → Spark → ML
```

Airflow belongs to scheduled/batch orchestration.

---

## Principle 2 — ML is a component, not the whole system

The fraud platform should combine:

```text
Rules
+
Behavioral Signals
+
Velocity Signals
+
ML Anomaly Score
=
Risk Score
```

This is more realistic than:

```text
Isolation Forest
→ Fraud
```

---

## Principle 3 — Every capability must have an observable purpose

Do not add:

```text
OpenTelemetry
```

because it looks good on a resume.

Use it to trace:

```text
API / producer
   ↓
Kafka
   ↓
Spark
   ↓
ML inference
   ↓
Warehouse write
```

Do not add:

```text
Prometheus
```

without metrics.

Do not add:

```text
Kubernetes
```

until Docker Compose deployment works.

---

## Principle 4 — Measure before claiming

Any final performance claim must come from benchmark results.

Never invent:

- throughput
- latency
- recall
- fraud rate
- accuracy
- memory usage
- processing time
- scalability

---

# 6. Technology Stack

## Tier 1 — Core

These must be deeply understood.

- Python
- SQL
- Kafka
- PySpark
- Spark Structured Streaming
- scikit-learn
- Isolation Forest
- feature engineering
- anomaly detection
- fraud detection
- real-time scoring
- Snowflake
- Docker

## Tier 2 — Production

Add after core pipeline is stable.

- Airflow
- Prometheus
- Grafana
- OpenTelemetry
- GitHub Actions
- Kubernetes
- Helm

## Tier 3 — Infrastructure

Add after deployment is stable.

- Terraform
- Infrastructure as Code
- cloud architecture concepts

## Supporting tools

Use only when necessary.

- FastAPI
- Pydantic
- PostgreSQL
- Git
- Linux / WSL2

---

# 7. What to Discard or Defer

## Permanently discard from core scope

- LLM
- RAG
- MCP
- LangChain
- LangGraph
- vector database
- computer vision
- OCR
- chatbot UI

## Avoid unless there is a concrete engineering reason

- large number of ML algorithms
- unnecessary microservices
- complex service mesh
- Kafka Connect ecosystem before fundamentals are understood
- Flink
- Ray
- Spark ML pipelines for everything
- feature-store product
- model-serving platform such as KServe before basic serving works

## Defer

- Kubernetes
- Helm
- Terraform
- cloud infrastructure
- advanced tracing
- elaborate API layer

---

# 8. Laptop and Cost Strategy

## Hardware assumption

Target development environment:

- Windows 11
- WSL2
- Ubuntu
- Docker
- 16 GB RAM class laptop
- consumer CPU
- optional discrete GPU

GPU is **not required** for the core ML workload.

Isolation Forest is CPU-oriented.

## Recommended runtime model

```text
Windows
   |
 WSL2 Ubuntu
   |
 Docker
   |
+---------------------------------------+
| selectively started services          |
|                                       |
| Kafka                                 |
| Spark                                 |
| Airflow                               |
| PostgreSQL                            |
| Prometheus                            |
| Grafana                               |
| SentinelStream services               |
+---------------------------------------+
```

Use Docker Compose first.

Do not keep all services running continuously.

---

# 9. Resource Discipline

## Development profiles

### Profile A — ML development

```text
Python
PostgreSQL
```

### Profile B — Kafka

```text
Kafka
Python producer
Python consumer
```

### Profile C — Streaming

```text
Kafka
Spark
SentinelStream
```

### Profile D — Orchestration

```text
PostgreSQL
Airflow
```

### Profile E — Monitoring

```text
Prometheus
Grafana
```

### Profile F — Full integration

Use only for integration testing.

```text
Kafka
Spark
Airflow
PostgreSQL
Prometheus
Grafana
SentinelStream
```

Kubernetes should normally be off until specifically testing deployment.

---

# 10. Data Strategy

Do not depend on proprietary or sensitive financial data.

Use synthetic transactions.

The generator should create realistic:

- normal transactions
- suspicious transactions
- user behavior
- device changes
- location changes
- merchant patterns
- time-series behavior
- burst activity

The synthetic generator is part of the engineering system, not disposable test code.

---

# 11. Transaction Event Schema

Suggested canonical event:

```json
{
  "transaction_id": "txn_829173",
  "event_time": "2026-08-11T14:32:18Z",
  "user_id": "usr_19281",
  "account_id": "acc_92831",
  "amount": 84250.00,
  "currency": "INR",
  "merchant_id": "m_8291",
  "merchant_category": "electronics",
  "payment_method": "UPI",
  "device_id": "dev_12981",
  "ip_address": "10.20.30.40",
  "latitude": 28.4595,
  "longitude": 77.0266,
  "country": "IN",
  "city": "Gurugram"
}
```

The implementation may add fields later.

Do not make the initial schema unnecessarily large.

---

# 12. Event Lifecycle

```text
raw event
    ↓
schema validation
    ↓
valid?
 ┌──┴──┐
no    yes
 |      |
 v      v
DLQ   deduplication
         |
         v
      enrichment
         |
         v
    feature generation
         |
         v
      risk scoring
         |
      +--+--+
      |     |
      v     v
 scored   high-risk
 event     alert
```

---

# 13. Kafka Design

Recommended topics:

```text
transactions.raw
transactions.validated
transactions.scored
fraud.alerts
deadletter.transactions
```

## Topic responsibilities

### transactions.raw

Original transaction events.

### transactions.validated

Schema-valid, normalized events.

### transactions.scored

Events with features and risk scores.

### fraud.alerts

High-risk events requiring investigation/action.

### deadletter.transactions

Invalid or repeatedly failed events.

---

# 14. Kafka Concepts You Must Understand

Research and implement:

- producer
- consumer
- topic
- partition
- key
- offset
- consumer group
- replication concept
- retention
- ordering
- delivery semantics
- retry
- dead-letter handling
- consumer lag

You must be able to explain:

> What happens if a consumer crashes after receiving a message but before completing processing?

You must also understand the difference between:

- at-most-once
- at-least-once
- exactly-once semantics

Do not claim exactly-once semantics unless the implementation genuinely supports and verifies the required guarantees.

---

# 15. Partitioning Strategy

Start with:

```text
message key = user_id
```

Reason:

Transactions for the same user can be ordered consistently within the relevant Kafka partition.

But this creates a potential hot-key problem for extremely active users.

Research:

- key distribution
- skew
- hot partitions
- partition count
- ordering guarantees

Document the trade-off.

---

# 16. Spark Structured Streaming Design

Primary streaming flow:

```text
Kafka
 ↓
readStream
 ↓
parse event
 ↓
schema validation
 ↓
deduplicate
 ↓
event-time handling
 ↓
windowed aggregations
 ↓
feature engineering
 ↓
ML/risk scoring
 ↓
write outputs
```

Research deeply:

- streaming DataFrames
- event time
- processing time
- windows
- watermarks
- checkpointing
- stateful processing
- joins
- late-arriving events
- fault tolerance
- micro-batch behavior
- backpressure considerations

---

# 17. Real-Time Feature Engineering

This is one of the most important parts of the project.

Create features based on recent behavior.

## Velocity features

```text
transactions_last_1m
transactions_last_5m
transactions_last_15m
transactions_last_1h
```

## Monetary features

```text
amount_last_5m
average_amount_1h
maximum_amount_1h
amount_vs_user_average
amount_zscore
```

## Diversity features

```text
unique_merchants_1h
unique_devices_1h
unique_locations_1h
```

## Device features

```text
new_device
known_device_count
device_change_frequency
```

## Geographic features

```text
distance_from_last_location
new_city
new_country
impossible_travel_signal
```

## Temporal features

```text
hour_of_day
day_of_week
is_weekend
```

Do not use features that introduce data leakage.

---

# 18. Fraud Scenarios

The generator should explicitly support scenarios.

## Scenario 1 — High-velocity activity

```text
User
 ├── ₹500
 ├── ₹700
 ├── ₹900
 ├── ₹8,000
 ├── ₹15,000
 └── ₹25,000
```

within a short period.

Expected signals:

- transaction velocity
- amount escalation
- behavioral deviation

---

## Scenario 2 — Large amount anomaly

Normal:

```text
₹400
₹800
₹650
₹1,200
₹900
```

Current:

```text
₹85,000
```

Expected signals:

- amount deviation
- ML anomaly score
- rule score

---

## Scenario 3 — Geographic anomaly

```text
10:01 → Delhi
10:04 → Mumbai
```

Potential signal:

- impossible travel / geographic inconsistency

Be careful: location anomalies are signals, not proof of fraud.

---

## Scenario 4 — New device + high value

```text
new_device = true
high_amount = true
```

---

## Scenario 5 — Merchant behavior change

Normal:

```text
food
transport
groceries
```

Then:

```text
electronics
```

The behavior model should identify unusual merchant activity if the generated baseline supports it.

---

## Scenario 6 — Burst pattern

```text
100 transactions
within 60 seconds
```

This scenario is especially useful for demonstrating streaming windows and state.

---

# 19. ML Design

## Primary model

Use:

**Isolation Forest**

Rationale:

- anomaly detection
- useful when trusted fraud labels are limited
- simple enough to understand deeply
- computationally practical for local development
- suitable as one signal in a larger risk engine

Do not present it as a universal fraud solution.

---

# 20. ML Feature Set

Example:

```text
amount
amount_zscore
transactions_1m
transactions_5m
transactions_1h
amount_last_5m
avg_amount_1h
amount_deviation
unique_merchants_1h
unique_devices_1h
new_device
new_location
distance_from_last_transaction
merchant_risk
user_transaction_frequency
hour_of_day
day_of_week
```

Feature availability must match event-time reality.

---

# 21. Isolation Forest Configuration

Starting point:

```python
IsolationForest(
    n_estimators=200,
    contamination="auto",
    random_state=42
)
```

Tune later based on actual experiments.

Do not copy arbitrary parameters into the final system without evaluating them.

---

# 22. Model Output

The ML layer should produce an anomaly signal, not immediately decide the business outcome.

Example:

```json
{
  "model_version": "iforest_v1",
  "anomaly_score": 0.87
}
```

Normalize the model output into the risk-engine scale only after documenting the transformation.

---

# 23. Rule Engine

Example rules:

### Rule A — Excessive velocity

```text
transactions_5m > threshold
```

### Rule B — Large amount deviation

```text
amount_vs_user_average > threshold
```

### Rule C — New device

```text
new_device = true
```

### Rule D — Geographic anomaly

```text
distance_from_last_transaction > threshold
```

### Rule E — Combined high-risk signal

```text
new_device
AND high_amount
AND high_velocity
```

Rules should be independently testable.

---

# 24. Risk Scoring Engine

Conceptual model:

```text
ML anomaly score
        +
rule score
        +
velocity score
        +
behavior score
        ↓
final risk score
```

Example:

```text
ML anomaly       0.82
Velocity         0.91
Geo anomaly      0.70
Device anomaly   0.85
Rule score       1.00
```

Final aggregation should be an explicit, configurable function.

Example:

```text
final_score =
    w_ml * ml_score +
    w_rules * rule_score +
    w_velocity * velocity_score +
    w_behavior * behavior_score
```

Weights are configuration, not magic constants.

---

# 25. Risk Bands

Initial bands:

```text
0.00 – 0.39  LOW
0.40 – 0.69  MEDIUM
0.70 – 1.00  HIGH
```

These thresholds are starting points only.

Later tune them based on:

- business cost
- alert volume
- false-positive rate
- recall
- analyst capacity

---

# 26. Explainability

Every high-risk event should contain reasons.

Example:

```json
{
  "transaction_id": "txn_829173",
  "risk_score": 0.91,
  "risk_level": "HIGH",
  "reasons": [
    "Transaction amount is significantly above recent user behavior",
    "High transaction velocity detected",
    "New device detected",
    "Recent location differs materially from previous activity"
  ],
  "model_score": 0.87,
  "rule_score": 0.95
}
```

Rules and features should be traceable.

Do not invent a "reason" that the underlying system did not actually evaluate.

---

# 27. Evaluation Strategy

Pure accuracy is not enough.

Measure:

- precision
- recall
- F1
- false-positive rate
- alert rate
- anomaly detection coverage
- latency
- throughput

For a fraud system, false positives can matter greatly.

Document the trade-off between:

```text
higher recall
vs
higher false-positive rate
```

---

# 28. Synthetic Ground Truth

Because the data generator creates fraud scenarios, it can also assign a hidden ground-truth label for evaluation.

Example:

```text
is_fraud_ground_truth
```

Important:

This label should be treated as **evaluation-only**.

It must not leak directly into production features.

Use separate fields or separate datasets where necessary.

---

# 29. Data Leakage Protection

Never include:

```text
is_fraud_ground_truth
fraud_reason_ground_truth
future transaction information
post-event investigation result
```

in online model features.

Document every feature's availability time.

---

# 30. Snowflake Design

Use logical warehouse layers:

```text
RAW
 ↓
STAGING
 ↓
CURATED
 ↓
MART
```

Suggested tables:

```text
RAW_TRANSACTIONS
STG_TRANSACTIONS
TRANSACTION_FEATURES
TRANSACTION_SCORES
FRAUD_ALERTS
USER_RISK_PROFILE
DAILY_FRAUD_METRICS
MODEL_RUNS
```

---

# 31. Snowflake Analytical Questions

Build SQL for:

- transaction count by day
- fraud rate by day
- fraud rate by merchant category
- fraud rate by city
- fraud rate by payment method
- average transaction amount
- average risk score
- high-risk transaction count
- alert count
- false-positive rate when ground truth is available
- top suspicious users
- top suspicious merchants
- risk score distribution

---

# 32. Airflow

Airflow should manage scheduled workflows.

Example DAGs:

```text
daily_metrics
historical_backfill
data_quality
model_training
model_evaluation
model_approval
```

Airflow should not sit inside the continuous streaming path.

---

# 33. Model Training DAG

```text
load_training_data
       ↓
validate_data
       ↓
build_features
       ↓
train_model
       ↓
evaluate_model
       ↓
compare_previous_model
       ↓
approve/reject
       ↓
register_model
```

A rejected model must never silently replace a working model.

---

# 34. Model Registry Metadata

At minimum track:

```text
model_version
training_timestamp
dataset_version
feature_version
algorithm
parameters
evaluation_metrics
status
```

Possible statuses:

```text
CANDIDATE
APPROVED
REJECTED
RETIRED
```

---

# 35. Model Drift

Monitor:

- input feature distributions
- transaction amount distribution
- transaction volume
- anomaly score distribution
- fraud alert rate
- population changes

Example:

```text
Training average transaction
₹1,200

Current average
₹8,400
```

A major distribution change should trigger investigation.

Do not automatically retrain every time drift is detected.

---

# 36. Observability

## Prometheus metrics

### Kafka

```text
events_produced_total
events_consumed_total
consumer_lag
failed_events_total
```

### Streaming

```text
records_processed_total
records_failed_total
processing_latency_ms
batch_duration_ms
```

### ML

```text
model_inference_total
model_inference_latency_ms
high_risk_predictions_total
medium_risk_predictions_total
low_risk_predictions_total
```

### API

```text
requests_total
request_latency_ms
request_errors_total
```

---

# 37. Grafana Dashboards

Build at least four dashboards.

## Dashboard 1 — System Health

- event throughput
- processing latency
- consumer lag
- errors
- service health

## Dashboard 2 — Fraud Monitoring

- fraud alert rate
- high-risk count
- average risk score
- fraud distribution
- top risk categories

## Dashboard 3 — Streaming Performance

- Kafka throughput
- Spark batch duration
- input/output rate
- processing delay

## Dashboard 4 — Model Monitoring

- score distribution
- feature drift
- alert distribution
- model version
- inference latency

---

# 38. OpenTelemetry

Use traces to follow a transaction across components.

Conceptually:

```text
transaction_request
    |
    +--> kafka_publish
             |
             +--> spark_process
                     |
                     +--> feature_generation
                             |
                             +--> ml_inference
                                     |
                                     +--> warehouse_write
```

The actual propagation strategy must be implemented correctly; do not fake distributed traces.

---

# 39. FastAPI — Optional Supporting API

Keep the API small.

Potential endpoints:

```text
GET /health
GET /metrics
GET /transactions/{transaction_id}
GET /alerts/{alert_id}
GET /model
GET /risk/{transaction_id}
```

Do not turn this into a giant backend project.

Its job is to expose scoring/inspection results.

---

# 40. Docker

The first deployment target is Docker Compose.

Suggested services:

```text
kafka
spark
sentinelstream
postgres
airflow
prometheus
grafana
```

Not every service must run in every development session.

Use:

- environment variables
- health checks
- persistent volumes where necessary
- explicit networks
- resource limits where practical

---

# 41. Kubernetes

Kubernetes is a later phase.

Learn and implement:

- Deployment
- Service
- ConfigMap
- Secret
- probes
- resource requests/limits
- Horizontal Pod Autoscaler
- persistent storage concepts

Do not run a large local Kubernetes cluster unnecessarily.

---

# 42. Helm

Create:

```text
deploy/helm/sentinelstream/
```

with:

```text
Chart.yaml
values.yaml
templates/
```

Use environment-specific values.

---

# 43. Terraform

Terraform should eventually describe deployment infrastructure.

Possible structure:

```text
infrastructure/terraform/
├── main.tf
├── variables.tf
├── outputs.tf
├── providers.tf
└── modules/
```

Do not provision expensive cloud resources merely to prove Terraform exists.

A documented local or low-cost deployment is acceptable for a portfolio project.

---

# 44. GitHub Actions

Minimum workflow:

```text
push / pull request
        ↓
lint
        ↓
unit tests
        ↓
integration tests
        ↓
Docker build
        ↓
security checks
```

Later:

```text
build image
    ↓
publish image
    ↓
deploy
```

Do not claim automated deployment until the workflow actually deploys.

---

# 45. Testing Strategy

## Unit tests

Test:

- feature functions
- rules
- score normalization
- risk bands
- schema validation
- event generation

## Integration tests

Test:

```text
producer
  ↓
Kafka
  ↓
consumer
```

and:

```text
Kafka
  ↓
Spark
  ↓
scored event
```

## End-to-end test

Test:

```text
synthetic event
 ↓
Kafka
 ↓
Spark
 ↓
features
 ↓
ML
 ↓
risk score
 ↓
alert
 ↓
warehouse
```

---

# 46. Failure Scenarios

The project is incomplete if it only works when everything is healthy.

Test:

1. Kafka unavailable.
2. Consumer crashes.
3. Duplicate event.
4. Malformed event.
5. Late event.
6. Spark restart.
7. ML model missing.
8. Warehouse unavailable.
9. Slow downstream consumer.
10. Invalid configuration.

Document expected recovery behavior.

---

# 47. Idempotency and Duplicate Handling

At-least-once systems can produce duplicates.

Use:

```text
transaction_id
```

as a natural idempotency key where appropriate.

Document:

- how duplicates are detected
- where deduplication happens
- how long duplicate state is retained
- whether deduplication is exact or bounded by a time window

---

# 48. Dead Letter Queue

Invalid or repeatedly failed messages should not block the entire pipeline.

Pattern:

```text
Kafka raw
   |
   +--> valid → normal pipeline
   |
   +--> invalid → deadletter.transactions
```

Include:

```text
original event
error_type
error_message
failure_timestamp
source_topic
partition
offset
```

---

# 49. Data Quality

Validate:

- required IDs
- timestamps
- amount non-negativity
- currency validity
- merchant ID
- device ID
- coordinate ranges
- duplicate transaction IDs
- schema compatibility

Bad data should be observable.

---

# 50. Scaling Experiments

Do not claim large-scale performance.

Measure your own environment.

Suggested dataset sizes:

```text
100K events
500K events
1M events
5M events
```

Streaming throughput experiments:

```text
100 events/sec
500 events/sec
1,000 events/sec
5,000 events/sec
10,000 events/sec
```

Only run higher levels if the laptop remains stable.

---

# 51. Benchmark Metrics

Record:

```text
events/sec
end-to-end latency
P50 latency
P95 latency
P99 latency
Kafka consumer lag
Spark batch duration
CPU utilization
RAM usage
failed events
duplicate events
ML inference latency
warehouse write latency
```

Store benchmark results in:

```text
docs/benchmarks/
```

---

# 52. Resource Benchmark Table

Keep an actual table such as:

| Test | Input | Throughput | P95 | RAM | CPU | Notes |
|---|---:|---:|---:|---:|---:|---|
| Baseline | 100K | TBD | TBD | TBD | TBD | |
| Medium | 500K | TBD | TBD | TBD | TBD | |
| Large | 1M | TBD | TBD | TBD | TBD | |
| Stress | 5M | TBD | TBD | TBD | TBD | |

Do not fill numbers until measured.

---

# 53. Repository Structure

Recommended structure:

```text
sentinelstream/
│
├── README.md
├── LICENSE
├── Makefile
├── docker-compose.yml
├── .env.example
├── pyproject.toml
│
├── docs/
│   ├── architecture.md
│   ├── decisions.md
│   ├── data-model.md
│   ├── streaming.md
│   ├── fraud-detection.md
│   ├── ml.md
│   ├── observability.md
│   ├── deployment.md
│   ├── failure-modes.md
│   ├── interview.md
│   └── benchmarks/
│
├── producer/
│   ├── generator.py
│   ├── scenarios.py
│   ├── schemas.py
│   └── publisher.py
│
├── streaming/
│   ├── pipeline.py
│   ├── schemas.py
│   ├── windows.py
│   ├── features.py
│   └── sinks.py
│
├── fraud/
│   ├── rules.py
│   ├── scorer.py
│   ├── risk_engine.py
│   └── explanations.py
│
├── ml/
│   ├── features.py
│   ├── train.py
│   ├── evaluate.py
│   ├── inference.py
│   └── registry.py
│
├── api/
│   ├── main.py
│   └── routes/
│
├── airflow/
│   └── dags/
│       ├── daily_metrics.py
│       ├── backfill.py
│       ├── training.py
│       └── data_quality.py
│
├── warehouse/
│   ├── schema.sql
│   ├── staging.sql
│   └── marts.sql
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── deploy/
│   ├── docker/
│   ├── helm/
│   └── kubernetes/
│
├── infrastructure/
│   └── terraform/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── streaming/
│   └── e2e/
│
└── .github/
    └── workflows/
        ├── ci.yml
        ├── integration.yml
        └── docker.yml
```

---

# 54. Development Milestones

Do not implement the whole stack at once.

## Milestone 0 — Research

Learn enough to understand:

- Kafka fundamentals
- Spark Structured Streaming
- Isolation Forest
- event-time windows
- Snowflake basics
- Airflow basics
- Docker basics

Output:

```text
docs/research/
```

---

## Milestone 1 — Synthetic transaction engine

Build:

```text
generator
scenarios
schemas
ground truth
```

Success condition:

> Generate reproducible normal and fraud-pattern transaction streams.

---

## Milestone 2 — Offline fraud model

Build:

```text
dataset
 ↓
feature engineering
 ↓
Isolation Forest
 ↓
evaluation
```

Success condition:

> Model produces meaningful anomaly signals on synthetic scenarios.

---

## Milestone 3 — Kafka pipeline

Build:

```text
producer
 ↓
transactions.raw
 ↓
consumer
```

Success condition:

> Events can be streamed continuously and recovered after consumer restart.

---

## Milestone 4 — Spark streaming

Build:

```text
Kafka
 ↓
Spark
 ↓
window features
 ↓
scored events
```

Success condition:

> Online features are computed using event time and streaming state.

---

## Milestone 5 — Risk engine

Build:

```text
ML
+
Rules
+
Behavior
+
Velocity
 ↓
Final risk score
```

Success condition:

> Each scored event contains risk score + explainable reasons.

---

## Milestone 6 — Warehouse

Build:

```text
Spark
 ↓
Snowflake
```

Success condition:

> Historical scored events and alert data are queryable analytically.

---

## Milestone 7 — Airflow

Build:

```text
daily metrics
backfill
training
evaluation
data quality
```

Success condition:

> Batch workflows run independently of the real-time path.

---

## Milestone 8 — Observability

Build:

```text
Prometheus
Grafana
OpenTelemetry
```

Success condition:

> System health and transaction pipeline performance are visible.

---

## Milestone 9 — Docker

Build:

```text
Docker Compose
```

Success condition:

> Another developer can reproduce the environment using documented commands.

---

## Milestone 10 — Failure testing

Test:

- duplicate
- malformed event
- Kafka restart
- consumer restart
- delayed event
- Spark restart
- warehouse unavailable

Success condition:

> Failure behavior is documented and predictable.

---

## Milestone 11 — Kubernetes + Helm

Only after Docker deployment is stable.

Success condition:

> Core services can be deployed using Kubernetes manifests/Helm.

---

## Milestone 12 — Terraform + CI/CD

Add:

```text
Terraform
GitHub Actions
```

Success condition:

> Infrastructure/deployment workflow is reproducible.

---

## Milestone 13 — Benchmarking

Generate measured results.

Success condition:

> Performance claims are backed by reproducible benchmark evidence.

---

## Milestone 14 — Portfolio hardening

Finish:

- README
- architecture diagram
- setup guide
- screenshots
- benchmark table
- design decisions
- failure modes
- interview questions
- resume bullets

---

# 55. Definition of Done

SentinelStream is not "done" because the code runs once.

Core completion requires:

- [ ] synthetic events generated
- [ ] fraud scenarios generated
- [ ] ground truth available for evaluation
- [ ] Kafka pipeline operational
- [ ] Spark Structured Streaming operational
- [ ] streaming features operational
- [ ] Isolation Forest integrated
- [ ] rule engine integrated
- [ ] final risk score generated
- [ ] explanations generated
- [ ] fraud alert topic operational
- [ ] warehouse persistence working
- [ ] Airflow workflows working
- [ ] tests passing
- [ ] failure handling tested
- [ ] Prometheus metrics exposed
- [ ] Grafana dashboard operational
- [ ] tracing tested
- [ ] Docker Compose reproducible
- [ ] benchmarks measured
- [ ] README complete
- [ ] architecture documented
- [ ] interview questions documented

Production layer:

- [ ] Kubernetes deployment
- [ ] Helm chart
- [ ] Terraform
- [ ] GitHub Actions
- [ ] deployment documentation

---

# 56. Resume Truth Policy

Only claim what exists.

## Unsafe

> Processed 100K events/sec.

Unless measured.

## Safe

> Built a Kafka + Spark Structured Streaming pipeline for real-time fraud scoring and benchmarked throughput/latency under synthetic workloads.

Once measured:

> Processed X events/sec at Y ms P95 latency on local hardware.

---

# 57. Interview Depth Checklist

## Kafka

Know:

- partitions
- offsets
- consumer groups
- ordering
- keying
- retention
- replay
- consumer lag
- duplicate processing
- delivery semantics

Questions:

- Why Kafka?
- Why this partition key?
- What happens during consumer failure?
- How do duplicates happen?
- How do you handle slow consumers?

---

## Spark

Know:

- DataFrame
- Structured Streaming
- event time
- watermark
- window
- state
- checkpoint
- fault tolerance
- micro-batch
- late data

Questions:

- Why Spark?
- Why not plain Python?
- What happens when an event arrives late?
- What happens when the process restarts?

---

## ML

Know:

- Isolation Forest intuition
- anomaly score
- contamination
- feature scaling considerations
- class imbalance
- precision/recall trade-off
- false positives
- leakage
- drift

Questions:

- Why Isolation Forest?
- How did you evaluate it?
- Why isn't accuracy enough?
- What would you do with labeled fraud data?
- How would the model degrade?

---

## Data Engineering

Know:

- batch vs streaming
- ETL vs ELT
- partitioning
- data quality
- idempotency
- schema evolution
- replay
- backfill

---

## MLOps

Know:

- model version
- feature version
- dataset version
- evaluation gate
- rollback
- drift
- monitoring
- retraining

---

## Kubernetes

Know:

- pod
- deployment
- service
- config
- secret
- readiness
- liveness
- resources
- scaling

---

# 58. Research Strategy

Do not learn technologies through random tutorials.

Use this sequence:

```text
Official documentation
        ↓
Conceptual understanding
        ↓
Tiny isolated experiment
        ↓
Integrate into SentinelStream
        ↓
Failure test
        ↓
Document decision
```

For every major technology, create:

```text
docs/research/<technology>.md
```

Each note should answer:

1. What is it?
2. Why is it used here?
3. What problem does it solve?
4. What alternatives exist?
5. What trade-offs exist?
6. What did SentinelStream actually use?
7. What failure modes matter?
8. What would change at 10x scale?

---

# 59. Architecture Decision Records

Create:

```text
docs/decisions/
```

Example ADRs:

```text
ADR-001 Kafka for event transport
ADR-002 Spark Structured Streaming for stream processing
ADR-003 Isolation Forest for baseline anomaly detection
ADR-004 Hybrid rule + ML risk engine
ADR-005 Snowflake for analytical warehouse
ADR-006 Airflow for scheduled workflows
ADR-007 Docker Compose for local deployment
ADR-008 Kubernetes as later deployment target
```

Every ADR should contain:

```text
Context
Decision
Alternatives
Trade-offs
Consequences
```

---

# 60. Project Versioning

Use explicit phases.

```text
v0.1
Synthetic data

v0.2
Kafka

v0.3
Spark streaming

v0.4
ML scoring

v0.5
Risk engine

v0.6
Snowflake

v0.7
Airflow

v0.8
Observability

v0.9
Docker hardening

v1.0
Portfolio release

v1.1+
Kubernetes / Helm / Terraform / CI/CD
```

Use Git tags.

---

# 61. Suggested Git Workflow

Branches:

```text
main
develop
feature/<name>
fix/<name>
```

Commit examples:

```text
feat(generator): add synthetic fraud scenarios
feat(kafka): add transaction producer
feat(streaming): add event-time velocity features
feat(ml): integrate isolation forest scorer
feat(risk): add hybrid risk engine
feat(warehouse): add transaction mart
feat(monitoring): add fraud metrics
test(streaming): add duplicate-event test
docs(architecture): document event flow
```

Avoid giant commits.

---

# 62. Demo Scenario

The portfolio demo should show:

## Step 1

Start the platform.

## Step 2

Start synthetic transactions.

## Step 3

Show Kafka events.

## Step 4

Show Spark processing.

## Step 5

Trigger a known fraud scenario.

Example:

```text
new_device
+
₹85,000
+
7 transactions / 2 minutes
```

## Step 6

Show:

```text
Risk = HIGH
Score = measured value
Reasons = actual system signals
```

## Step 7

Show Kafka fraud alert.

## Step 8

Show warehouse record.

## Step 9

Show Grafana throughput/latency.

## Step 10

Show trace.

This is the strongest demo path.

---

# 63. README Structure

Final README:

```text
1. Hero
2. What SentinelStream is
3. Architecture
4. Why these technologies
5. Fraud scenarios
6. ML approach
7. Streaming pipeline
8. Risk scoring
9. Observability
10. Benchmarks
11. Failure handling
12. Project structure
13. Local setup
14. Docker
15. Kubernetes
16. Screenshots
17. Technical decisions
18. Limitations
19. Future work
20. Resume / portfolio links
```

---

# 64. Honest Limitations Section

The README should explicitly say:

- data is synthetic
- fraud labels are simulated
- production scale is not claimed
- model is a baseline anomaly detector
- risk thresholds are configurable
- local benchmarks depend on hardware
- cloud deployment is optional
- additional supervised models could be evaluated when labeled data is available

This increases credibility.

---

# 65. Future Enhancements

Only after the core is strong:

- supervised fraud classification
- graph-based fraud detection
- entity relationships
- feature store
- online feature cache
- real model registry
- canary model deployment
- shadow scoring
- adaptive thresholds
- analyst feedback loop
- Kafka schema registry
- stronger security
- cloud deployment

These are future work, not mandatory scope.

---

# 66. Security

Even with synthetic data, implement basic security discipline:

- no secrets in Git
- `.env.example` only
- secrets via environment/config
- least privilege concepts
- input validation
- safe SQL
- container non-root user where practical
- dependency scanning
- no real financial/PII data

Do not log sensitive fields unnecessarily.

---

# 67. Reproducibility

A fresh environment should be able to run:

```bash
git clone <repo>
cd sentinelstream
cp .env.example .env
docker compose up
```

or equivalent documented commands.

The exact final commands depend on implementation.

---

# 68. Agent Operating Model

The coding agent working on SentinelStream must behave as:

> **Senior Data Engineer + ML Engineer + MLOps Engineer + Reviewer**

The agent is not allowed to blindly generate large amounts of code.

It must:

1. inspect the repository
2. inspect existing implementation
3. identify current milestone
4. inspect tests
5. inspect configuration
6. inspect architecture docs
7. make the smallest coherent change
8. run relevant tests
9. inspect failures
10. fix the root cause
11. update documentation
12. verify no contradictions
13. update project status
14. continue until the current milestone is actually complete

---

# 69. THE LOOPING AGENT PROMPT

Use the following prompt as the persistent instruction for the coding agent.

```text
You are the autonomous engineering agent responsible for building SentinelStream.

ROLE
You are acting simultaneously as:
- Senior Data Engineer
- Senior ML Engineer
- MLOps Engineer
- Backend Engineer
- Platform Engineer
- Code Reviewer
- Technical Writer

MISSION
Build SentinelStream into a technically credible, reproducible, production-style real-time fraud-scoring platform.

PRIMARY OBJECTIVE
Do not optimize for code volume.
Optimize for:
1. correctness
2. reliability
3. explainability
4. reproducibility
5. measurable performance
6. architectural coherence
7. interview defensibility
8. truthful portfolio claims

PROJECT IDENTITY
SentinelStream is:
Real-Time Data Engineering + Streaming + ML Fraud Detection + MLOps.

It is NOT:
- an LLM project
- a RAG project
- an MCP project
- a chatbot project
- a generic dashboard
- a collection of disconnected technologies

TECHNOLOGY PRIORITY
Core:
Python
SQL
Kafka
PySpark
Spark Structured Streaming
scikit-learn
Isolation Forest
feature engineering
fraud/anomaly detection
real-time scoring
Snowflake
Docker

Production:
Airflow
Prometheus
Grafana
OpenTelemetry
GitHub Actions
Kubernetes
Helm

Infrastructure:
Terraform

Supporting:
FastAPI
Pydantic
PostgreSQL
Git
Linux/WSL2

IMPORTANT SCOPE RULE
Do not add technologies merely because they appear impressive.
Every technology must solve a concrete problem.

DO NOT ADD
LLM
RAG
MCP
LangChain
LangGraph
vector DB
OCR
computer vision
unless the user explicitly changes project scope.

DO NOT FABRICATE
Never invent:
- benchmark numbers
- model metrics
- throughput
- latency
- uptime
- production users
- production traffic
- cloud cost
- accuracy
- fraud rate
- deployment status

If a value has not been measured, mark it as TBD.

GENERAL LOOP

Repeat this cycle continuously until the CURRENT MILESTONE is complete:

PHASE A — INSPECT
1. Inspect the repository.
2. Inspect the current branch.
3. Read README and architecture documentation.
4. Read project status.
5. Inspect relevant source files.
6. Inspect tests.
7. Inspect configuration.
8. Identify what is actually implemented versus planned.
9. Never assume planned code exists.

PHASE B — DETERMINE STATE
Classify the project as:
- not started
- partially implemented
- blocked
- test failing
- implementation complete
- milestone complete

Determine the current milestone from:
docs/project_status.md
README
git history
source code
tests

If project_status.md does not exist, create it.

PHASE C — PLAN ONE COHERENT STEP
Choose the smallest meaningful engineering step that advances the current milestone.

Before editing:
- explain the engineering objective internally
- identify affected files
- identify expected behavior
- identify tests required
- identify possible failure modes

Do not implement future milestones early unless required for the current one.

PHASE D — IMPLEMENT
Implement production-quality code.

Requirements:
- clear names
- type hints where useful
- structured logging
- error handling
- configuration through environment/config
- no hard-coded secrets
- deterministic behavior where practical
- tests for meaningful logic
- maintainable module boundaries

Prefer simple architecture over premature abstraction.

PHASE E — TEST
Run the narrowest relevant tests first.

Then run broader tests when appropriate.

At minimum:
- unit tests for logic
- integration tests for component boundaries
- end-to-end tests for milestone completion

When a test fails:
1. inspect the actual error
2. determine root cause
3. fix root cause
4. rerun test
5. do not hide failures
6. do not weaken tests merely to make them pass

PHASE F — VERIFY
Verify:
- imports
- configuration
- schemas
- runtime behavior
- logs
- output
- error paths
- resource usage where relevant

For streaming systems also verify:
- duplicate behavior
- restart behavior
- invalid event behavior
- late-event behavior
- backpressure/lag indicators where relevant

PHASE G — DOCUMENT
Update documentation after implementation.

Keep these synchronized:
- README
- architecture docs
- project status
- ADRs
- benchmark notes
- setup instructions

Never document planned behavior as if it already exists.

PHASE H — REVIEW
Perform a self-review as a senior engineer.

Ask:
- Is this correct?
- Is this simpler than necessary?
- Is this over-engineered?
- Is there hidden coupling?
- Can a failure wedge the pipeline?
- Can duplicate events corrupt state?
- Can bad data enter the model?
- Is there leakage?
- Are metrics actually meaningful?
- Are the docs truthful?
- Could I defend this design in an interview?

PHASE I — UPDATE STATUS
Update:
docs/project_status.md

Record:
- completed
- in progress
- blocked
- next step
- tests executed
- important design decisions
- known limitations

PHASE J — CONTINUE
After completing the current step:
1. re-read project status
2. re-evaluate the next smallest step
3. continue the loop

STOP ONLY WHEN:
- the current milestone is genuinely complete
- tests pass
- documentation is synchronized
- known limitations are documented
- no obvious unfinished work remains for that milestone

THEN MOVE TO THE NEXT MILESTONE.

MILESTONE ORDER

M0 Research
M1 Synthetic transaction engine
M2 Offline ML
M3 Kafka
M4 Spark Structured Streaming
M5 Hybrid risk engine
M6 Snowflake
M7 Airflow
M8 Observability
M9 Docker hardening
M10 Failure testing
M11 Kubernetes + Helm
M12 Terraform + CI/CD
M13 Benchmarking
M14 Portfolio hardening

DO NOT SKIP A MILESTONE WITHOUT A TECHNICAL REASON.

RESEARCH RULES

When a technology is not understood:
1. consult authoritative documentation if available
2. record the concept
3. create a tiny experiment
4. verify the experiment
5. integrate only after understanding the behavior

For each major technology document:
- why chosen
- alternative considered
- trade-off
- actual usage
- failure mode
- scaling implications

STREAMING RULES

For Kafka:
- understand keying
- partitions
- offsets
- consumer groups
- lag
- retries
- duplicates
- delivery semantics

For Spark:
- use event time deliberately
- understand watermarks
- understand windows
- understand checkpointing
- handle late events
- avoid accidental unbounded state

For idempotency:
- use transaction_id or another explicit strategy
- document deduplication behavior
- never claim exactly-once unless verified

ML RULES

Isolation Forest is the primary baseline.

Treat ML as one component of fraud detection.

Always distinguish:
model anomaly score
rule score
risk score
ground-truth fraud label

Never leak:
future information
post-event investigation outcomes
ground-truth labels
future aggregates

Evaluation must report multiple metrics.

Do not call the system accurate merely because examples look correct.

OBSERVABILITY RULES

Every major component should expose useful operational information.

Metrics should answer:
- Is the system alive?
- Is throughput healthy?
- Is processing delayed?
- Are messages failing?
- Is Kafka lag increasing?
- Is ML latency increasing?
- Are high-risk alerts changing unexpectedly?

Tracing must represent real execution.

DOCKER RULES

Docker Compose first.

Do not require Kubernetes for basic local development.

Use profiles or selective startup where practical because local hardware is limited.

KUBERNETES RULES

Only begin Kubernetes after Docker Compose works.

Start with the minimum number of deployments.

Use:
- probes
- resource requests/limits
- ConfigMaps
- Secrets
- Services

Do not deploy every component into Kubernetes just for appearance.

BENCHMARK RULES

Every performance claim must be measured.

Record:
- input size
- event rate
- hardware
- software environment
- throughput
- latency
- memory
- CPU
- errors
- configuration

Benchmark repeatably.

Do not compare results from incompatible environments without saying so.

PORTFOLIO RULE

At the end of every milestone, ask:

"Could Anurag truthfully mention this capability on his resume now?"

If not, keep it as incomplete.

INTERVIEW RULE

For every major feature, maintain one explanation that covers:

1. what it does
2. why it exists
3. alternatives
4. trade-offs
5. failure modes
6. scaling behavior

If an implementation cannot be explained, simplify it or document it until it can.

RESOURCE RULE

Prefer local execution.

Use:
- WSL2
- Docker
- local Spark
- local Kafka
- local monitoring

Use Snowflake/cloud only when it materially improves learning or demonstrates the intended warehouse/cloud behavior.

Do not spend money without explicit approval.

SECURITY RULE

Never commit:
- passwords
- tokens
- API keys
- cloud credentials
- private certificates

Use:
.env.example
environment variables
secret management patterns

FINAL QUALITY GATE

Before declaring v1 complete, verify:

[ ] Synthetic transaction generator
[ ] Fraud scenarios
[ ] Kafka event pipeline
[ ] Spark Structured Streaming
[ ] Real-time features
[ ] Isolation Forest
[ ] Rule engine
[ ] Hybrid risk score
[ ] Explainable risk reasons
[ ] Fraud alerts
[ ] Snowflake warehouse
[ ] Airflow workflows
[ ] Prometheus
[ ] Grafana
[ ] OpenTelemetry
[ ] Docker Compose
[ ] Failure handling
[ ] Unit tests
[ ] Integration tests
[ ] End-to-end test
[ ] Benchmarks
[ ] Architecture documentation
[ ] ADRs
[ ] README
[ ] Setup guide
[ ] Honest limitations
[ ] Interview notes

LOOP COMMAND

At the end of every completed action, return to:

INSPECT → DETERMINE STATE → PLAN ONE STEP → IMPLEMENT → TEST → VERIFY → DOCUMENT → REVIEW → UPDATE STATUS → CONTINUE

Never assume the project is finished simply because one feature works.

Always optimize for the next smallest correct step.

If blocked:
- diagnose the blocker
- identify the smallest unblock action
- perform it
- continue

If uncertain:
- inspect actual code/config/tests
- consult authoritative documentation
- do not invent behavior

If a proposed change would expand scope:
- reject it unless it directly improves the current milestone or the user explicitly requests the scope change.

The success criterion is not "many technologies."

The success criterion is:

A coherent, reproducible, measurable, technically defensible real-time fraud-scoring system that demonstrates real Data Engineering + ML Engineering + MLOps capability.
```

---

# 70. Agent Output Format

When the agent reports progress, prefer:

```text
## Current Milestone
M3 — Kafka Pipeline

## Verified State
- Producer implemented
- Kafka topic created
- Consumer implemented
- Consumer restart test passes

## Changed
- producer/publisher.py
- streaming/consumer.py
- tests/integration/test_kafka.py

## Validation
- unit tests: PASS
- integration tests: PASS
- manual event flow: PASS

## Known Issues
- schema evolution not implemented yet

## Next Step
Implement validated-event topic and DLQ handling.
```

Do not produce vague updates like:

> "Kafka integration is mostly done."

---

# 71. Project Status File

Maintain:

```text
docs/project_status.md
```

Suggested content:

```markdown
# SentinelStream Project Status

## Current Milestone
M0 — Research

## Completed
- [ ]

## In Progress
- [ ]

## Blocked
- [ ]

## Tests
- Unit:
- Integration:
- E2E:

## Current Risks
- 

## Design Decisions
- 

## Next Smallest Step
- 
```

The agent must update this continuously.

---

# 72. Final Portfolio Claims — Desired Shape

Only after measured implementation, the final resume bullet can resemble:

> Engineered a real-time fraud-scoring pipeline using Kafka and Spark Structured Streaming, generating behavioral/velocity features and combining Isolation Forest anomaly detection with rule-based risk scoring; persisted scored events for analytics and monitored pipeline health with production observability tooling.

Once actually implemented:

> Added batch backfills/model workflows with Airflow and containerized the platform for reproducible deployment, with Prometheus/Grafana/OpenTelemetry monitoring and a Kubernetes/Helm deployment path.

Once benchmarks exist:

> Benchmarked the streaming pipeline at **X events/sec** with **Y ms P95 scoring latency** on documented local hardware.

Never use these claims before the evidence exists.

---

# 73. Final Success Definition

SentinelStream succeeds when an interviewer can ask:

> "Walk me through the system."

and Anurag can clearly explain:

```text
Event ingestion
      ↓
Kafka
      ↓
Spark streaming
      ↓
Event-time features
      ↓
Fraud rules
      +
Isolation Forest
      ↓
Risk engine
      ↓
Alert + Warehouse
      ↓
Airflow batch workflows
      ↓
Monitoring + tracing
```

and then answer:

- why Kafka
- why Spark
- why Isolation Forest
- how the model is evaluated
- how duplicates are handled
- how late events are handled
- how failures recover
- how the model is monitored
- how the system scales
- how the platform is deployed
- what has actually been measured
- what the project cannot yet do

That is the standard this project should be built to meet.

---

# 74. One-Sentence Project Definition

> **SentinelStream is a production-style real-time fraud-scoring platform that combines Kafka event streaming, Spark Structured Streaming, real-time behavioral feature engineering, Isolation Forest anomaly detection, hybrid risk scoring, analytical warehousing, batch orchestration, and end-to-end observability.**

---

# 75. Agent Reminder

```text
BUILD THE SYSTEM, NOT THE README.

MEASURE THE SYSTEM, NOT YOUR ASSUMPTIONS.

UNDERSTAND THE TRADE-OFFS, NOT JUST THE APIS.

IMPLEMENT THE SMALLEST CORRECT STEP.

TEST FAILURE, NOT JUST SUCCESS.

DOCUMENT WHAT EXISTS.

NEVER INVENT EVIDENCE.

CONTINUE UNTIL THE CURRENT MILESTONE IS ACTUALLY DONE.
```
