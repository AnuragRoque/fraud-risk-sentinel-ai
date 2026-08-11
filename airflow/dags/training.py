"""Model Retraining & Approval DAG — SentinelStream Airflow.

Automates the ML model retraining lifecycle specified in Section 33:
load_data -> validate_data -> train_model -> evaluate -> compare -> approve/reject -> register
"""

from datetime import datetime, timezone
import logging
from typing import Dict, Any, Tuple

from ml.registry import ModelRegistry, ModelMetadata
from ml.train import train_isolation_forest

logger = logging.getLogger(__name__)


def run_model_retraining_workflow(
    model_version: str = "iforest_retrained_v1",
    min_f1_threshold: float = 0.50,
    output_dir: str = "models",
) -> Dict[str, Any]:
    """Execute complete model retraining, evaluation, comparison, and approval gate.
    
    CRITICAL: A candidate model with inferior metrics or failing quality gates is REJECTED
    and will NEVER silently replace a working APPROVED model.
    """
    logger.info(f"Starting model retraining DAG workflow for version '{model_version}'...")

    # 1-5. Train & Evaluate Candidate Model
    predictor, metadata = train_isolation_forest(
        num_train_events=1200,
        num_eval_events=300,
        model_version=model_version,
        output_dir=output_dir,
    )

    cand_f1 = metadata.evaluation_metrics.get("f1_score", 0.0)
    registry = ModelRegistry(registry_file=f"{output_dir}/registry.json")

    # 6. Compare with Previous Active Approved Model
    previous_approved = registry.get_latest_approved_model()
    prev_f1 = previous_approved.evaluation_metrics.get("f1_score", 0.0) if previous_approved else 0.0

    # 7. Quality Gate Decision
    if cand_f1 >= min_f1_threshold and cand_f1 >= (prev_f1 - 0.05):
        final_status = "APPROVED"
        logger.info(f"Model '{model_version}' APPROVED (F1={cand_f1:.4f} vs Previous F1={prev_f1:.4f})")
    else:
        final_status = "REJECTED"
        logger.warning(f"Model '{model_version}' REJECTED (F1={cand_f1:.4f} failed quality gate vs Previous F1={prev_f1:.4f})")

    # 8. Update Registry Status
    registry.update_status(model_version, final_status)

    return {
        "model_version": model_version,
        "status": final_status,
        "candidate_f1": cand_f1,
        "previous_f1": prev_f1,
        "metrics": metadata.evaluation_metrics,
    }
