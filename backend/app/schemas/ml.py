from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PolicyScoreRequest(BaseModel):
    zone_sismique: str = Field(default="I")
    wilaya_code: str = Field(default="16")
    commune_name: str | None = Field(default=None)
    type_risque: str = Field(default="1 - Bien Immobilier")
    construction_type: str | None = Field(default=None)
    valeur_assuree: float = Field(default=10_000_000.0, ge=0)
    prime_nette: float = Field(default=0.0, ge=0)
    year: int = Field(default=2025, ge=2000, le=2100)
    date_effet: str | None = None
    date_expiration: str | None = None


class PolicyScoreProbabilities(BaseModel):
    LOW: float
    MEDIUM: float
    HIGH: float


class PolicyScoreResponse(BaseModel):
    score: float
    tier: str
    proba: PolicyScoreProbabilities
    dominant_factor: str
    normalized_inputs: dict[str, Any]
    elapsed_ms: float | None = None


class BatchScoreRequest(BaseModel):
    policies: list[dict[str, Any]]


class BatchScoreResult(BaseModel):
    policy_id: str
    score: float
    tier: str


class BatchScoreResponse(BaseModel):
    results: list[BatchScoreResult]


class FeatureImportanceItem(BaseModel):
    name: str
    importance: float


class FeatureImportanceResponse(BaseModel):
    features: list[FeatureImportanceItem]


class MLHealthResponse(BaseModel):
    status: str
    model_loaded: bool
    model_path: str
    metadata_loaded: bool
    training_metrics: dict[str, Any] | None = None
