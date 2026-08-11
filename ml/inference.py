"""Inference Engine — SentinelStream ML.

Loads trained Isolation Forest models and transforms raw decision function output
into normalized anomaly scores S_ml in [0.0, 1.0].
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

from ml.features import FEATURE_NAMES


class FraudModelPredictor:
    """Predictor wrapping trained Isolation Forest for anomaly score inference."""

    def __init__(
        self,
        model: Optional[IsolationForest] = None,
        model_version: str = "iforest_v1.0.0",
    ) -> None:
        self.model = model
        self.model_version = model_version

    @classmethod
    def load_from_file(cls, model_path: str | Path) -> "FraudModelPredictor":
        """Load a trained model artifact from a .joblib file."""
        path = Path(model_path)
        if not path.exists():
            raise FileNotFoundError(f"Model artifact not found at {path}")

        data = joblib.load(path)
        if isinstance(data, dict) and "model" in data:
            model = data["model"]
            version = data.get("version", "iforest_v1.0.0")
        else:
            model = data
            version = "iforest_v1.0.0"

        return cls(model=model, model_version=version)

    def save_to_file(self, model_path: str | Path, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Save trained model artifact and metadata to a .joblib file."""
        if self.model is None:
            raise ValueError("No model trained to save")

        path = Path(model_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "model": self.model,
            "version": self.model_version,
            "feature_names": FEATURE_NAMES,
            "metadata": metadata or {},
        }
        joblib.dump(payload, path)

    def normalize_score(self, raw_score: float) -> float:
        """Transform raw decision_function score to normalized anomaly score S_ml in [0.0, 1.0].
        
        Raw score interpretation in scikit-learn IsolationForest:
        - Negative values: Anomalies / outlier events
        - Positive values: Normal baseline events
        
        Formula:
        S_ml = clamp(0.5 - raw_score, 0.0, 1.0)
        """
        score = 0.5 - raw_score
        return float(np.clip(score, 0.0, 1.0))

    def predict_anomaly_score(self, feature_vector: List[float] | np.ndarray) -> Dict[str, Any]:
        """Compute raw score and normalized anomaly score S_ml for a single feature vector."""
        if self.model is None:
            raise ValueError("Model is not initialized or trained")

        X = np.array(feature_vector, dtype=np.float64)
        if X.ndim == 1:
            X = X.reshape(1, -1)

        raw_score = float(self.model.decision_function(X)[0])
        anomaly_score = self.normalize_score(raw_score)

        return {
            "model_version": self.model_version,
            "raw_decision_score": round(raw_score, 4),
            "anomaly_score": round(anomaly_score, 4),
        }

    def predict_batch(self, X: np.ndarray) -> np.ndarray:
        """Compute vector of normalized anomaly scores for a 2D feature matrix X."""
        if self.model is None:
            raise ValueError("Model is not initialized or trained")

        raw_scores = self.model.decision_function(X)
        anomaly_scores = np.clip(0.5 - raw_scores, 0.0, 1.0)
        return anomaly_scores
