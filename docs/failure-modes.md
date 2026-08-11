# Failure Modes & Fault Tolerance Specification — SentinelStream

## Executive Summary
SentinelStream is engineered to maintain system integrity and data consistency during partial component failures. The system prioritizes zero data loss, predictable recovery, and high availability.

---

## Failure Recovery Matrix (Section 46)

| Scenario # | Failure Mode | Impact | Recovery Strategy | Verification Method |
|---|---|---|---|---|
| **1** | Kafka Broker Unavailable | Event publishing blocked | Producer retries 3 times (`retries=3`), falls back to local buffer or mock mode, and logs operational alerts | `test_kafka_broker_unreachable_fallback` |
| **2** | Consumer Crash | Ingestion stream interrupted | Offsets are committed only post micro-batch processing. Restarted consumer resumes from last committed offset | `test_consumer_crash_and_offset_resume` |
| **3** | Duplicate Transaction Event | Potential state corruption | Idempotency key `transaction_id` deduplicates events at sink / warehouse layer | `test_duplicate_transaction_deduplication` |
| **4** | Malformed JSON Payload | Uncaught parsing exception | Payload intercepted by validation logic, wrapped with error metadata, and routed to `deadletter.transactions` DLQ | `test_malformed_event_dlq_routing` |
| **5** | Late-Arriving Event (>10m) | Out-of-order event stream | 10-minute event-time watermark drops late events from active window memory, preserving bounded state | `test_late_arriving_event_watermark` |
| **6** | Spark Executor/Driver Failure | Stream processing halt | Spark loads write-ahead log (WAL) from `checkpointLocation` and re-processes uncommitted offsets | `test_spark_checkpoint_recovery` |
| **7** | ML Model Artifact Missing | Anomaly score calculation failure | `RiskEngine` gracefully degrades to rules-only mode (`rules_only_v1.0`) without crashing streaming pipeline | `test_missing_ml_model_graceful_degradation` |
| **8** | Warehouse Database Outage | Analytical sink write failure | Micro-batches buffer in memory with retry backoff until warehouse connection is re-established | `test_warehouse_unavailability_retry` |
| **9** | Slow Downstream Consumer | Consumer lag accumulation | Consumer lag monitored via Prometheus metric; partition re-balancing and rate limiting applied | `test_consumer_lag_backpressure` |
| **10** | Invalid Configuration / Env | Application startup failure | Early configuration validation raises explicit `ValueError` before initializing streaming threads | `test_invalid_configuration_failfast` |

---

## Detailed Failure Recovery Patterns

### Dead Letter Queue (DLQ) Architecture
When schema parsing or validation fails:
```text
Kafka raw payload
    │
    ▼
Schema Validator ──(Fails)──► Dead Letter Enricher ──► Kafka topic: deadletter.transactions
    │                                                      (With error_message & timestamp)
    ▼ (Passes)
Streaming Pipeline
```

### Missing ML Model Graceful Degradation
If `models/iforest_v1.0.0.joblib` is missing or corrupted at runtime:
- `RiskEngine` catches `FileNotFoundError`.
- `ml_score` defaults to `0.0`.
- `model_version` is tagged as `"rules_only_v1.0"`.
- Rule engine continues evaluating velocity, amount spikes, device novelty, and location shift.
