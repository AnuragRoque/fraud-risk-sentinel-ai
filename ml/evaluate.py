"""Evaluation Engine — SentinelStream ML.

Evaluates Isolation Forest model predictions against ground truth labels.
Computes Precision, Recall, F1, FPR, ROC-AUC, and optimal decision threshold.
"""

from typing import Dict, Any, Tuple
import numpy as np
from sklearn.metrics import (
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
)


def evaluate_anomaly_scores(
    anomaly_scores: np.ndarray,
    y_ground_truth: np.ndarray,
    threshold: float = 0.55,
) -> Dict[str, Any]:
    """Evaluate continuous anomaly scores against binary ground truth labels.
    
    Args:
        anomaly_scores: Array of normalized anomaly scores in [0.0, 1.0].
        y_ground_truth: Array of binary ground truth labels (True for fraud, False for normal).
        threshold: Score cut-off threshold above which an event is classified as positive/anomalous.
        
    Returns:
        Dict containing comprehensive evaluation metrics and confusion matrix.
    """
    y_true = np.array(y_ground_truth, dtype=bool)
    y_pred = (np.array(anomaly_scores) >= threshold)

    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[False, True]).ravel()

    precision = float(precision_score(y_true, y_pred, zero_division=0))
    recall = float(recall_score(y_true, y_pred, zero_division=0))
    f1 = float(f1_score(y_true, y_pred, zero_division=0))
    fpr = float(fp / (fp + tn)) if (fp + tn) > 0 else 0.0

    try:
        roc_auc = float(roc_auc_score(y_true, anomaly_scores))
    except ValueError:
        roc_auc = 0.5

    return {
        "threshold": threshold,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1_score": round(f1, 4),
        "false_positive_rate": round(fpr, 4),
        "roc_auc": round(roc_auc, 4),
        "confusion_matrix": {
            "true_negatives": int(tn),
            "false_positives": int(fp),
            "false_negatives": int(fn),
            "true_positives": int(tp),
        },
        "total_samples": len(y_true),
        "positive_ground_truth_count": int(np.sum(y_true)),
    }


def find_optimal_threshold(
    anomaly_scores: np.ndarray,
    y_ground_truth: np.ndarray,
    threshold_min: float = 0.35,
    threshold_max: float = 0.85,
    steps: int = 50,
) -> Tuple[float, Dict[str, Any]]:
    """Sweep threshold candidates to identify the threshold maximizing F1 score."""
    best_threshold = 0.55
    best_metrics = {}
    best_f1 = -1.0

    for t in np.linspace(threshold_min, threshold_max, steps):
        metrics = evaluate_anomaly_scores(anomaly_scores, y_ground_truth, threshold=float(t))
        if metrics["f1_score"] > best_f1:
            best_f1 = metrics["f1_score"]
            best_threshold = float(t)
            best_metrics = metrics

    return best_threshold, best_metrics
