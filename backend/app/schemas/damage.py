from __future__ import annotations

from pydantic import BaseModel, Field

from app.schemas.recommendation import RecommendationItem, RetrievedDocument


class DamageAssessmentPayload(BaseModel):
    image_type: str = Field(default="satellite")
    area_km2: float = Field(default=1.0, gt=0)
    construction_type: str = Field(default="Beton arme")
    zone_sismique: str | None = Field(default=None)
    wilaya_code: str | None = Field(default=None)
    commune_name: str | None = Field(default=None)
    query: str | None = Field(
        default=None,
        description="Optional analyst question for the RAG layer. If omitted, a default damage recommendation query is used.",
    )
    top_k: int = Field(default=4, ge=1, le=10)


class DamageAssessmentResult(BaseModel):
    damage_class: int
    damage_label: str
    loss_percentage: float
    loss_per_km2_dzd: float
    total_loss_dzd: float
    confidence: float
    is_mock: bool
    heatmap_url: str
    affected_area_km2: float
    breakdown: dict[str, float]
    image_type: str
    construction_type: str
    zone_sismique: str
    wilaya_code: str | None = None
    commune_name: str | None = None


class DamageAssessmentResponse(BaseModel):
    damage_assessment: DamageAssessmentResult
    executive_summary: str
    confidence: float
    recommendations: list[RecommendationItem]
    context_sources: list[str]
    retrieved_documents: list[RetrievedDocument]
    generation_mode: str
    llm_used: bool
    llm_error: str | None = None


class DamageHealthResponse(BaseModel):
    status: str
    model_loaded: bool
    device: str
    model_path: str
    cnn_enabled: bool
    load_error: str | None = None
