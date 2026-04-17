from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_rag_service
from app.db.session import get_db
from app.rag.service import RAGService
from app.schemas.recommendation import (
    PortfolioAnalysisResponse,
    RAGHealthResponse,
    RAGIngestRequest,
    RAGIngestResponse,
    RAGQueryRequest,
    RAGQueryResponse,
    RecommendationsResponse,
    RiskInsightsResponse,
)

router = APIRouter()


@router.get("/health", response_model=RAGHealthResponse)
async def rag_health(rag_service: RAGService = Depends(get_rag_service)) -> RAGHealthResponse:
    return await rag_service.health()


@router.post("/query", response_model=RAGQueryResponse, status_code=status.HTTP_200_OK)
async def query_rag(
    payload: RAGQueryRequest,
    db: AsyncSession = Depends(get_db),
    rag_service: RAGService = Depends(get_rag_service),
) -> RAGQueryResponse:
    return await rag_service.query(db, payload.query, top_k=payload.top_k)


@router.get("/portfolio-analysis", response_model=PortfolioAnalysisResponse)
async def portfolio_analysis(
    db: AsyncSession = Depends(get_db),
    rag_service: RAGService = Depends(get_rag_service),
) -> PortfolioAnalysisResponse:
    return await rag_service.get_portfolio_analysis(db)


@router.get("/risk-insights", response_model=RiskInsightsResponse)
async def risk_insights(
    db: AsyncSession = Depends(get_db),
    rag_service: RAGService = Depends(get_rag_service),
) -> RiskInsightsResponse:
    return await rag_service.get_risk_insights(db)


@router.get("/recommendations", response_model=RecommendationsResponse)
async def recommendations(
    db: AsyncSession = Depends(get_db),
    rag_service: RAGService = Depends(get_rag_service),
) -> RecommendationsResponse:
    return await rag_service.get_recommendations(db)


@router.post("/ingest", response_model=RAGIngestResponse, status_code=status.HTTP_201_CREATED)
async def ingest_knowledge(
    payload: RAGIngestRequest,
    rag_service: RAGService = Depends(get_rag_service),
) -> RAGIngestResponse:
    return await rag_service.ingest(payload.documents)
