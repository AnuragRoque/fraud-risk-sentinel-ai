# Spark Structured Streaming Engineering Deep Dive — SentinelStream

## 1. Role in SentinelStream
Spark Structured Streaming serves as the distributed processing engine responsible for reading transaction streams from Kafka, validating schemas, statefully computing real-time behavioral and velocity features across event-time sliding windows, applying fraud detection models/rules, and writing output streams.

---

## 2. Event Time vs. Processing Time

### Event Time
- Represented by the `event_time` field in the canonical transaction payload (ISO-8601 UTC timestamp).
- All windowed aggregations (velocity, monetary sums, unique merchant counts) operate strictly on **event time**.
- This guarantees deterministic results regardless of network delivery delay or out-of-order Kafka message arrival.

### Processing Time
- The wall-clock time at which Spark micro-batches process event records.
- Used strictly for operational latency measurement (`processing_latency_ms = processing_time - event_time`).

---

## 3. Watermarking & Late Event Handling

### Watermark Strategy
```python
df.withWatermark("event_time", "10 minutes") \
  .groupBy(
      window(col("event_time"), "5 minutes", "1 minute"),
      col("user_id")
  )
```

### How Watermarking Works in SentinelStream
1. Spark tracks the maximum `event_time` seen across all partitions minus the watermark delay threshold (`10 minutes`).
2. Any incoming event with `event_time < (max_event_time - 10 minutes)` is dropped as a "late event" to prevent infinite state memory accumulation.
3. State storage for window aggregations older than the watermark threshold is automatically garbage collected.

---

## 4. Windowed Aggregations & State Management

SentinelStream computes real-time velocity and monetary features using sliding windows:

| Feature Name | Window Duration | Slide Duration | Aggregation Function |
|---|---|---|---|
| `tx_count_1m` | 1 minute | 10 seconds | `count(transaction_id)` |
| `tx_count_5m` | 5 minutes | 1 minute | `count(transaction_id)` |
| `tx_count_1h` | 1 hour | 5 minutes | `count(transaction_id)` |
| `amount_sum_5m` | 5 minutes | 1 minute | `sum(amount)` |
| `avg_amount_1h` | 1 hour | 5 minutes | `avg(amount)` |
| `max_amount_1h` | 1 hour | 5 minutes | `max(amount)` |
| `unique_merchants_1h` | 1 hour | 5 minutes | `approx_count_distinct(merchant_id)` |

---

## 5. Fault Tolerance & Checkpointing

- **Checkpoint Directory**: HDFS/S3/Local directory specified via `option("checkpointLocation", "/tmp/sentinelstream/checkpoints")`.
- **Write-Ahead Logs (WAL)**: Structured Streaming writes micro-batch offsets and state updates to write-ahead logs before committing results to output sinks.
- **Restart Recovery**: If the Spark executor or driver fails, restarting the application loads the checkpoint state, re-reads uncommitted offsets from Kafka, and resumes stream processing without losing window state.

---

## 6. Micro-Batch Trigger Strategy
- SentinelStream uses fixed interval micro-batch execution: `.trigger(processingTime='1 second')`.
- Micro-batching strikes the optimal balance between high throughput and low sub-second/single-digit latency for real-time fraud scoring.
