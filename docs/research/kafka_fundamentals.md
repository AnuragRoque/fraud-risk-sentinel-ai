# Kafka Engineering Deep Dive — SentinelStream

## 1. Role in SentinelStream
Kafka serves as the high-throughput, fault-tolerant event backbone for ingesting financial transaction events and decoupling transaction generation from real-time stream processing, anomaly detection, scoring, and analytical storage.

---

## 2. Topic Topology

| Topic Name | Purpose | Message Key | Partition Count | Retention |
|---|---|---|---|---|
| `transactions.raw` | Ingested raw synthetic transaction payloads | `user_id` | 4 | 24 Hours |
| `transactions.validated` | Parsed, normalized, and schema-valid events | `user_id` | 4 | 24 Hours |
| `transactions.scored` | Enriched events with features and risk scores | `user_id` | 4 | 7 Days |
| `fraud.alerts` | High-risk events requiring downstream alert handling | `transaction_id` | 2 | 30 Days |
| `deadletter.transactions` | Malformed, unparseable, or schema-invalid events | `transaction_id` | 1 | 30 Days |

---

## 3. Partitioning Strategy & Keying

### Primary Partitioning Key
Messages published to `transactions.raw` use `message_key = user_id`.

### Engineering Rationale
- Kafka guarantees strict sequential ordering within a single partition.
- Keying by `user_id` ensures that all transactions originating from a specific user are consistently routed to the same partition and processed in order.
- Ordered processing is vital for computing accurate user-level time-window state (e.g., velocity count in last 5 minutes, amount escalation, location changes).

### Hot-Key Trade-Off & Mitigation
- **Risk**: Extremely active users or synthetic burst scenarios can cause partition skew, overloading specific partition consumers.
- **Mitigation**: Monitor per-partition consumer lag and processing latency. For extreme scale, compound keys (e.g., `user_id + salt`) can be introduced, though stateful aggregations across partitions would require global state handling in Spark.

---

## 4. Delivery Semantics & Failure Recovery

### Chosen Delivery Guarantee: At-Least-Once
- **Producer**: Configured with `acks=all` (or `acks=1` for local setup) and retries enabled (`retries=3`).
- **Consumer**: Disables auto-commit (`enable.auto.commit=false`). Consumer commits offsets only after successfully processing micro-batches or emitting output events.
- **Deduplication Responsibility**: Downstream engines (Spark / Risk Engine) maintain idempotency using unique `transaction_id` tracking over state watermarks.

### Exact-Once Semantics (EOS) Analysis
- True EOS across Kafka → Spark → Storage requires Kafka transactional producers + Spark idempotent sinks.
- In SentinelStream, we enforce idempotency at the database/sink level via `transaction_id` deduplication rather than imposing double-phase commit overhead across all streaming stages.

---

## 5. Dead Letter Queue (DLQ) Pattern

When a consumer encounters a malformed JSON payload or schema validation failure:
1. The raw payload is caught without throwing an uncaught exception that halts the streaming worker.
2. The payload is wrapped with diagnostic metadata:
   - `original_payload`
   - `error_type` (e.g., `ValidationError`, `JSONDecodeError`)
   - `error_message`
   - `failed_timestamp`
   - `source_topic` / `partition` / `offset`
3. The enriched failure event is published to `deadletter.transactions`.
4. The consumer logs an operational warning metric to Prometheus (`failed_events_total`) and continues processing the next record.

---

## 6. Consumer Lag Monitoring
- **Metric**: `consumer_lag = latest_topic_offset - current_consumer_offset`
- If `consumer_lag` increases monotonically, downstream stream processing is failing to keep pace with ingestion rate (backpressure indicator).
- Prometheus exporter scrapes consumer group offsets for alerting in Grafana.
