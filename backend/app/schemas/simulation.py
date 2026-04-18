from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from app.schemas.recommendation import RecommendationItem, RetrievedDocument

SimulationScope = Literal["wilaya", "commune"]


class SimulationRequest(BaseModel):
    scenario: str = Field(default="boumerdes_2003")
    magnitude: float | None = Field(default=None, ge=4.0, le=9.0)
    epicenter_lat: float | None = Field(default=None, ge=15.0, le=40.0)
    epicenter_lon: float | None = Field(default=None, ge=-10.0, le=12.0)
    depth_km: float = Field(default=10.0, ge=1.0, le=100.0)
    scope: SimulationScope | None = None
    scope_code: str | None = None
    n_simulations: int = Field(default=3_000, ge=100, le=20_000)
    seed: int = Field(default=42, ge=1)
    query: str | None = Field(
        default=None,
        description="Optional analyst question for the RAG layer. If omitted, a default recommendation query is used.",
    )
    top_k: int = Field(default=4, ge=1, le=10)

    @model_validator(mode="after")
    def validate_custom_scenario(self) -> "SimulationRequest":
        if self.scenario == "custom":
            required = [self.magnitude, self.epicenter_lat, self.epicenter_lon]
            if any(value is None for value in required):
                raise ValueError("Custom scenario requires magnitude, epicenter_lat, and epicenter_lon.")
        return self


class ScenarioMeta(BaseModel):
    label: str
    magnitude: float
    epicenter: tuple[float, float]
    depth_km: float
    affected_wilayas: list[str]


class SimulationHealthResponse(BaseModel):
    status: str
    available_scenarios: list[str]


class CommuneLossEntry(BaseModel):
    wilaya_code: str
    wilaya_name: str
    code_commune: str
    commune_name: str
    zone_sismique: str
    lat: float | None
    lon: float | None
    expected_loss: float
    policy_count: int
    total_exposure: float


class ZoneLossEntry(BaseModel):
    zone_sismique: str
    expected_loss: float
    policy_count: int
    total_exposure: float


class WilayaLossEntry(BaseModel):
    wilaya_code: str
    wilaya_name: str
    expected_loss: float
    policy_count: int
    total_exposure: float


class MonteCarloResult(BaseModel):
    scenario_name: str
    affected_policies: int
    source_policies: int
    cleaned_policies: int
    n_simulations: int
    expected_loss: float
    expected_gross_loss: float
    gross_var_95: float
    gross_var_99: float
    expected_net_loss: float
    var_95: float
    var_99: float
    pml_999: float
    worst_case_loss: float
    per_commune_json: list[CommuneLossEntry]
    high_risk_zones: list[ZoneLossEntry]
    overexposed_wilayas: list[WilayaLossEntry]
    data_quality: dict[str, int | float | str] | None = None
    elapsed_seconds: float | None = None


class SimulationRunResponse(BaseModel):
    monte_carlo: MonteCarloResult
    executive_summary: str
    confidence: float
    recommendations: list[RecommendationItem]
    context_sources: list[str]
    retrieved_documents: list[RetrievedDocument]
    generation_mode: str
    llm_used: bool
    llm_error: str | None = None
