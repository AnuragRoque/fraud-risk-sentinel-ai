"""Pydantic Event Schemas — SentinelStream.

Defines the canonical TransactionEvent payload used across Kafka/Spark pipelines
and the GroundTruthEvent wrapper used strictly for offline evaluation.
"""

from datetime import datetime, timezone
import json
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class TransactionEvent(BaseModel):
    """Canonical online transaction event payload."""

    transaction_id: str = Field(..., description="Unique ID of the transaction")
    event_time: datetime = Field(..., description="UTC ISO-8601 timestamp of transaction event")
    user_id: str = Field(..., description="Unique ID of the user")
    account_id: str = Field(..., description="Unique ID of the bank/payment account")
    amount: float = Field(..., ge=0.0, description="Transaction monetary amount (must be >= 0)")
    currency: str = Field(default="INR", description="Three-letter ISO currency code")
    merchant_id: str = Field(..., description="Unique ID of the merchant")
    merchant_category: str = Field(..., description="Merchant business category")
    payment_method: str = Field(..., description="Payment instrument used (e.g. UPI, CARD, NETBANKING)")
    device_id: str = Field(..., description="Unique ID of the transacting device")
    ip_address: str = Field(..., description="IPv4 or IPv6 address")
    latitude: float = Field(..., ge=-90.0, le=90.0, description="Geographic latitude")
    longitude: float = Field(..., ge=-180.0, le=180.0, description="Geographic longitude")
    country: str = Field(default="IN", description="Two-letter ISO country code")
    city: str = Field(..., description="City name")

    @field_validator("event_time", mode="before")
    @classmethod
    def ensure_utc(cls, v: datetime | str) -> datetime:
        """Ensure timestamps are formatted as datetime in UTC timezone."""
        if isinstance(v, str):
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        else:
            dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def to_json(self) -> str:
        """Serialize event to JSON string with ISO-8601 timestamp."""
        return self.model_dump_json()

    def to_dict(self) -> dict:
        """Serialize event to Python dictionary."""
        d = self.model_dump()
        d["event_time"] = self.event_time.isoformat()
        return d


class GroundTruthEvent(BaseModel):
    """Evaluation wrapper bundling a TransactionEvent with ground-truth fraud labels.
    
    CRITICAL: This object is for evaluation/testing only and must NOT be published
    to online production model feature pipelines.
    """

    event: TransactionEvent
    is_fraud_ground_truth: bool = Field(..., description="Hidden ground truth label (True if synthetic fraud)")
    fraud_scenario_type: str = Field(default="NORMAL", description="Fraud scenario identifier (e.g. VELOCITY_SPIKE, GEO_ANOMALY)")
    fraud_reason_ground_truth: Optional[str] = Field(default=None, description="Detailed explanation of synthetic fraud trigger")

    def to_streaming_event(self) -> TransactionEvent:
        """Extract pure TransactionEvent stripped of all ground-truth labels."""
        return self.event

    def to_json(self) -> str:
        """Serialize ground truth event to JSON string."""
        return self.model_dump_json()
