"""Unit tests for SentinelStream Milestone 2 — Offline ML & Anomaly Detection Pipeline."""

from datetime import datetime, timezone
import tempfile
from pathlib import Path
import numpy as np
import pytest

from ml.evaluate import evaluate_anomaly_scores, find_optimal_threshold
from ml.features import FEATURE_NAMES, FeatureExtractor
from ml.inference import FraudModelPredictor
from ml.registry import ModelMetadata, ModelRegistry
from ml.train import train_isolation_forest
from producer.generator import SyntheticDataGenerator
from producer.schemas import TransactionEvent


def test_feature_extraction_single_event():
    """Test feature dictionary computation for a single transaction event."""
    extractor = FeatureExtractor()
    now = datetime(2026, 8, 11, 14, 30, 0, tzinfo=timezone.utc)
    
    tx = TransactionEvent(
        transaction_id="tx_test_1",
        event_time=now,
        user_id="usr_10",
        account_id="acc_10",
        amount=5000.0,
        currency="INR",
        merchant_id="m_55",
        merchant_category="electronics",
        payment_method="UPI",
        device_id="dev_10",
        ip_address="10.0.0.1",
        latitude=28.6139,
        longitude=77.2090,
        country="IN",
        city="Delhi",
    )

    feats = extractor.extract_features(tx, user_history=[])
    
    assert set(feats.keys()) == set(FEATURE_NAMES)
    assert feats["amount"] == 5000.0
    assert feats["tx_count_1m"] == 0.0
    assert feats["hour_of_day"] == 14.0
    assert feats["day_of_week"] == float(now.weekday())

    vector = extractor.to_feature_vector(feats)
    assert len(vector) == len(FEATURE_NAMES)
    assert not any(np.isnan(vector))


def test_feature_matrix_dataset_extraction():
    """Test extraction of 2D feature matrix from event stream."""
    gen = SyntheticDataGenerator(seed=42)
    gt_events = gen.generate_batch(count=30)
    events = [gt.to_streaming_event() for gt in gt_events]

    extractor = FeatureExtractor()
    X, feat_dicts = extractor.extract_dataset(events)

    assert X.shape == (30, len(FEATURE_NAMES))
    assert len(feat_dicts) == 30
    assert not np.isnan(X).any()
    assert not np.isinf(X).any()


def test_score_normalization():
    """Test raw decision function score normalization into [0.0, 1.0]."""
    predictor = FraudModelPredictor()

    # Raw positive score (normal baseline in scikit-learn) -> low anomaly score
    assert predictor.normalize_score(0.4) == pytest.approx(0.1)
    assert predictor.normalize_score(0.5) == pytest.approx(0.0)

    # Raw zero score -> 0.5 anomaly score
    assert predictor.normalize_score(0.0) == pytest.approx(0.5)

    # Raw negative score (anomaly outlier in scikit-learn) -> high anomaly score
    assert predictor.normalize_score(-0.3) == pytest.approx(0.8)
    assert predictor.normalize_score(-0.6) == pytest.approx(1.0)


def test_model_training_and_registry_flow():
    """Test full training, artifact serialization, prediction, and model registry lifecycle."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        predictor, metadata = train_isolation_forest(
            num_train_events=300,
            num_eval_events=80,
            n_estimators=50,
            max_samples=128,
            model_version="iforest_unit_test",
            output_dir=tmp_dir,
        )

        assert predictor.model is not None
        assert metadata.model_version == "iforest_unit_test"

        # Check artifact files exist
        artifact_path = Path(tmp_dir) / "iforest_unit_test.joblib"
        registry_path = Path(tmp_dir) / "registry.json"
        assert artifact_path.exists()
        assert registry_path.exists()

        # Reload predictor from file
        reloaded_predictor = FraudModelPredictor.load_from_file(artifact_path)
        sample_vector = [1000.0, 0.0, 1.0, 2.0, 5.0, 1000.0, 1000.0, 1.0, 1.0, 0.0, 0.0, 0.0, 12.0, 1.0]
        res = reloaded_predictor.predict_anomaly_score(sample_vector)

        assert res["model_version"] == "iforest_unit_test"
        assert 0.0 <= res["anomaly_score"] <= 1.0

        # Check registry loading
        registry = ModelRegistry(registry_file=registry_path)
        latest = registry.get_latest_approved_model()
        if metadata.status == "APPROVED":
            assert latest is not None
            assert latest.model_version == "iforest_unit_test"


def test_evaluation_metrics_calculation():
    """Test metrics and optimal threshold selection on synthetic score vector."""
    y_true = np.array([False, False, False, False, True, True, True, True])
    scores = np.array([0.1, 0.2, 0.25, 0.3, 0.7, 0.85, 0.9, 0.95])

    metrics = evaluate_anomaly_scores(scores, y_true, threshold=0.5)
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0
    assert metrics["f1_score"] == 1.0
    assert metrics["roc_auc"] == 1.0

    best_t, best_m = find_optimal_threshold(scores, y_true)
    assert 0.35 <= best_t <= 0.7
    assert best_m["f1_score"] == 1.0
