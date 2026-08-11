"""Model Registry metadata tracking — SentinelStream ML.

Tracks model versions, parameters, training datasets, metrics, and lifecycle status.
Ensures rejected or unvalidated models are never deployed silently.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class ModelMetadata(BaseModel):
    """Metadata record for a trained ML model artifact."""

    model_version: str = Field(..., description="Unique model version identifier (e.g. iforest_v1.0.0)")
    training_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    algorithm: str = Field(default="IsolationForest", description="Machine learning algorithm name")
    hyperparameters: Dict[str, Any] = Field(default_factory=dict, description="Model hyperparameter values")
    feature_names: List[str] = Field(default_factory=list, description="Ordered list of input features")
    training_samples_count: int = Field(default=0, description="Total training sample count")
    evaluation_metrics: Dict[str, Any] = Field(default_factory=dict, description="Evaluation metrics (Precision, Recall, F1, AUC)")
    status: str = Field(default="CANDIDATE", description="Lifecycle status: CANDIDATE, APPROVED, REJECTED, RETIRED")
    artifact_path: str = Field(..., description="File path to model binary artifact")

    def to_json(self) -> str:
        return self.model_dump_json(indent=2)


class ModelRegistry:
    """Manages model metadata persistence and status updates."""

    def __init__(self, registry_file: str | Path = "models/registry.json") -> None:
        self.registry_file = Path(registry_file)
        self.registry_file.parent.mkdir(parents=True, exist_ok=True)
        self.records: Dict[str, ModelMetadata] = self._load()

    def _load(self) -> Dict[str, ModelMetadata]:
        if not self.registry_file.exists():
            return {}
        try:
            data = json.loads(self.registry_file.read_text(encoding="utf-8"))
            return {ver: ModelMetadata.model_validate(rec) for ver, rec in data.items()}
        except Exception:
            return {}

    def _save(self) -> None:
        data = {ver: json.loads(rec.to_json()) for ver, rec in self.records.items()}
        self.registry_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def register_model(self, metadata: ModelMetadata) -> None:
        """Register a new model artifact in the registry."""
        self.records[metadata.model_version] = metadata
        self._save()

    def update_status(self, model_version: str, status: str) -> None:
        """Update model status (e.g. approve or reject a model candidate)."""
        valid_statuses = {"CANDIDATE", "APPROVED", "REJECTED", "RETIRED"}
        if status not in valid_statuses:
            raise ValueError(f"Invalid status '{status}'. Must be one of {valid_statuses}")

        if model_version not in self.records:
            raise KeyError(f"Model version '{model_version}' not found in registry")

        self.records[model_version].status = status
        self._save()

    def get_latest_approved_model(self) -> Optional[ModelMetadata]:
        """Retrieve the metadata of the most recently trained APPROVED model."""
        approved = [
            m for m in self.records.values()
            if m.status == "APPROVED"
        ]
        if not approved:
            return None
        return max(approved, key=lambda m: m.training_timestamp)
