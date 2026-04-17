from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class RetrievedDocument(BaseModel):
    source: str
    title: str
    score: float
    excerpt: str
    tags: list[str] = Field(default_factory=list)


class RecommendationItem(BaseModel):
    priority: str
    category: str
    title: str
    description: str
    action: str
    confidence: float
    explanation: str
    rpa_reference: str | None = None
    evidence: list[str] = Field(default_factory=list)


class RAGQueryRequest(BaseModel):
    query: str
    scope: str = "portfolio"
    top_k: int = 4


class RAGQueryResponse(BaseModel):
    answer: str
    executive_summary: str
    confidence: float
    recommendations: list[RecommendationItem]
    context_sources: list[str]
    retrieved_documents: list[RetrievedDocument]


class RiskInsight(BaseModel):
    title: str
    severity: str
    description: str
    metric_name: str
    metric_value: Decimal | float | int | str
    affected_scope: str
    explanation: str


class PortfolioAnalysisResponse(BaseModel):
    generated_at: datetime
    executive_summary: str
    total_exposure: Decimal
    total_policies: int
    net_retention: Decimal
    top_hotspots: list[dict]
    zone_breakdown: list[dict]
    concentration_alerts: list[str]


class RiskInsightsResponse(BaseModel):
    generated_at: datetime
    insights: list[RiskInsight]
    retrieved_documents: list[RetrievedDocument]


class RecommendationsResponse(BaseModel):
    generated_at: datetime
    executive_summary: str
    recommendations: list[RecommendationItem]
    confidence: float
    retrieved_documents: list[RetrievedDocument]


class IngestDocumentRequest(BaseModel):
    title: str
    content: str
    source: str = "user"
    tags: list[str] = Field(default_factory=list)


class RAGIngestRequest(BaseModel):
    documents: list[IngestDocumentRequest]


class RAGIngestResponse(BaseModel):
    ingested_documents: int
    total_documents: int
    status: str


class RAGHealthResponse(BaseModel):
    status: str
    model_loaded: bool
    model_provider: str
    vector_db_status: str
    knowledge_documents: int
    last_initialized_at: datetime | None = None

