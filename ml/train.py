"""Model Trainer — SentinelStream ML.

Generates synthetic training and validation transaction datasets, extracts feature matrices,
fits Isolation Forest, evaluates detection metrics, and registers model artifacts.
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple
import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

from ml.evaluate import evaluate_anomaly_scores, find_optimal_threshold
from ml.features import FEATURE_NAMES, FeatureExtractor
from ml.inference import FraudModelPredictor
from ml.registry import ModelMetadata, ModelRegistry
from producer.generator import SyntheticDataGenerator


def train_isolation_forest(
    num_train_events: int = 2000,
    num_eval_events: int = 500,
    n_estimators: int = 200,
    max_samples: int = 256,
    contamination: float = 0.05,
    seed: int = 42,
    model_version: str = "iforest_v1.0.0",
    output_dir: str = "models",
) -> Tuple[FraudModelPredictor, ModelMetadata]:
    """Train, evaluate, and save an Isolation Forest fraud anomaly model."""
    print(f"=== SentinelStream ML: Training Isolation Forest ({model_version}) ===")
    print(f"Dataset sizes: Train={num_train_events}, Eval={num_eval_events} | Seed={seed}")

    # 1. Generate synthetic dataset
    train_gen = SyntheticDataGenerator(seed=seed, fraud_rate=0.03)  # Mostly normal for training
    eval_gen = SyntheticDataGenerator(seed=seed + 1, fraud_rate=0.10)  # 10% fraud scenarios for eval

    start_time = datetime(2026, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
    train_gt_events = train_gen.generate_batch(count=num_train_events, start_time=start_time)
    eval_gt_events = eval_gen.generate_batch(count=num_eval_events, start_time=start_time)

    # Extract pure streaming events for feature building
    train_events = [gt.to_streaming_event() for gt in train_gt_events]
    eval_events = [gt.to_streaming_event() for gt in eval_gt_events]

    # Extract binary ground truth for eval
    y_eval = np.array([gt.is_fraud_ground_truth for gt in eval_gt_events], dtype=bool)

    # 2. Extract feature matrices X_train and X_eval
    extractor = FeatureExtractor()
    X_train, _ = extractor.extract_dataset(train_events)
    X_eval, _ = extractor.extract_dataset(eval_events)

    print(f"Feature Matrix Shape: Train={X_train.shape}, Eval={X_eval.shape}")

    # 3. Fit Isolation Forest
    model = IsolationForest(
        n_estimators=n_estimators,
        max_samples=max_samples,
        contamination=contamination,
        random_state=seed,
        n_jobs=-1,
    )
    model.fit(X_train)

    predictor = FraudModelPredictor(model=model, model_version=model_version)

    # 4. Evaluate Anomaly Scores on Evaluation Set
    anomaly_scores = predictor.predict_batch(X_eval)
    best_thresh, eval_metrics = find_optimal_threshold(anomaly_scores, y_eval)

    print(f"\n--- Evaluation Results ---")
    print(f"Optimal Threshold : {best_thresh:.3f}")
    print(f"Precision         : {eval_metrics['precision']:.4f}")
    print(f"Recall            : {eval_metrics['recall']:.4f}")
    print(f"F1 Score          : {eval_metrics['f1_score']:.4f}")
    print(f"False Pos. Rate   : {eval_metrics['false_positive_rate']:.4f}")
    print(f"ROC-AUC           : {eval_metrics['roc_auc']:.4f}")
    print(f"Confusion Matrix  : {eval_metrics['confusion_matrix']}")

    # 5. Persist Model Artifact and Register Metadata
    out_path = Path(output_dir) / f"{model_version}.joblib"
    predictor.save_to_file(out_path, metadata=eval_metrics)

    # Auto-approve if F1 >= 0.50 for candidate model
    status = "APPROVED" if eval_metrics["f1_score"] >= 0.50 else "CANDIDATE"

    metadata = ModelMetadata(
        model_version=model_version,
        algorithm="IsolationForest",
        hyperparameters={
            "n_estimators": n_estimators,
            "max_samples": max_samples,
            "contamination": contamination,
            "seed": seed,
            "optimal_threshold": best_thresh,
        },
        feature_names=FEATURE_NAMES,
        training_samples_count=num_train_events,
        evaluation_metrics=eval_metrics,
        status=status,
        artifact_path=str(out_path),
    )

    registry = ModelRegistry(registry_file=Path(output_dir) / "registry.json")
    registry.register_model(metadata)
    print(f"Model saved to '{out_path}' and registered with status '{status}'.\n")

    return predictor, metadata


def main() -> None:
    """CLI script for model training."""
    parser = argparse.ArgumentParser(description="SentinelStream Model Training Script")
    parser.add_argument("--train-count", type=int, default=1500, help="Number of training events")
    parser.add_argument("--eval-count", type=int, default=400, help="Number of evaluation events")
    parser.add_argument("--version", type=str, default="iforest_v1.0.0", help="Model version identifier")
    parser.add_argument("--output-dir", type=str, default="models", help="Directory to save model artifacts")
    args = parser.parse_args()

    train_isolation_forest(
        num_train_events=args.train_count,
        num_eval_events=args.eval_count,
        model_version=args.version,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
