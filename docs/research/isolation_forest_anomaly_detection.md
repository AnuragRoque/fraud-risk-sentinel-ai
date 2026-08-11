# Isolation Forest & Anomaly Detection Engineering Deep Dive — SentinelStream

## 1. Role in SentinelStream
Isolation Forest (iForest) is chosen as the primary baseline Machine Learning algorithm for unsupervised anomaly detection. It evaluates transaction vectors against expected historical distributions without relying exclusively on labeled fraud data.

---

## 2. Algorithm Principles

### Why Isolation Forest?
1. **Unsupervised Nature**: Fraud patterns evolve quickly and true historical ground-truth labels are frequently delayed or scarce.
2. **Computational Efficiency**: Isolation Forest isolates anomalies by randomly partitioning feature space using decision trees. Anomalies require fewer splits (shorter path lengths) to isolate than normal data points.
3. **Linear Time Complexity**: Time complexity of \(O(n \log n)\), making it computationally practical for real-time inference on standard local CPU resources.

---

## 3. Score Normalization & Output Structure

### Raw Decision Function
In `scikit-learn`, `decision_function(X)` returns negative values for anomalies and positive values for normal instances:
- Raw score range: approximately \([-0.5, 0.5]\)

### SentinelStream Normalization Formula
We transform the raw score into a normalized anomaly score \(S_{\text{ml}} \in [0.0, 1.0]\) where **1.0 indicates maximum anomaly**:

\[
S_{\text{ml}} = \text{clamp}\left(0.5 - \text{decision\_function}(X), 0.0, 1.0\right)
\]

### Anomaly Signal Output Payload
```json
{
  "model_version": "iforest_v1.0.0",
  "raw_decision_score": -0.32,
  "anomaly_score": 0.82
}
```

---

## 4. Feature Vector Specification

The model consumes 12 numeric and ordinal behavioral features:

| Feature Name | Type | Description |
|---|---|---|
| `amount` | Float | Transaction amount |
| `amount_zscore` | Float | Deviation of transaction amount relative to user historical mean/std |
| `tx_count_1m` | Int | Transaction count in last 1 minute |
| `tx_count_5m` | Int | Transaction count in last 5 minutes |
| `tx_count_1h` | Int | Transaction count in last 1 hour |
| `amount_sum_5m` | Float | Total amount spent in last 5 minutes |
| `avg_amount_1h` | Float | Average transaction amount in last 1 hour |
| `amount_vs_user_avg` | Float | Ratio of current amount to 1-hour average amount |
| `unique_merchants_1h` | Int | Count of unique merchants visited in last 1 hour |
| `new_device` | Binary (0/1) | Whether device_id has been seen before for this user |
| `new_location` | Binary (0/1) | Whether location/city is novel for this user |
| `distance_from_last_tx` | Float | Haversine distance (km) from previous transaction location |

---

## 5. Strict Prevention of Data Leakage

To preserve realistic streaming conditions, the following attributes are **strictly prohibited** from the model feature vector:

- `is_fraud_ground_truth` (Evaluation label only)
- `fraud_scenario_type` (Evaluation metadata only)
- Future window aggregations (e.g., `tx_count_next_5m`)
- Post-investigation feedback flags

---

## 6. Hyperparameter Configuration & Calibration

Starting Baseline:
```python
IsolationForest(
    n_estimators=200,
    max_samples=256,
    contamination=0.05,
    random_state=42,
    n_jobs=-1
)
```

- `n_estimators=200`: Provides stable path lengths without excessive tree evaluation latency.
- `contamination=0.05`: Baseline assumption of 5% anomaly rate in synthetic transaction dataset.
- `max_samples=256`: Standard sub-sampling size for effective isolation tree building.
