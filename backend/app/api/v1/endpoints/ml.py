from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_ml_service, get_rag_service
from app.db.session import get_db
from app.rag.service import RAGService
from app.schemas.ml import (
    BatchScoreRequest,
    BatchScoreResponse,
    FeatureImportanceResponse,
    MLHealthResponse,
    PolicyScoreRequest,
    PolicyScoreResponse,
)
from app.services.ml_service import MLService

router = APIRouter()


@router.get("/health", response_model=MLHealthResponse)
async def ml_health(ml_service: MLService = Depends(get_ml_service)) -> MLHealthResponse:
    return MLHealthResponse(**ml_service.health())


@router.post("/score", response_model=PolicyScoreResponse, status_code=status.HTTP_200_OK)
async def score_policy(
    payload: PolicyScoreRequest,
    ml_service: MLService = Depends(get_ml_service),
    rag_service: RAGService = Depends(get_rag_service),
    db: AsyncSession = Depends(get_db),
) -> PolicyScoreResponse:
    try:
        result = ml_service.score_policy(payload)
        score_analytics = ml_service.get_cached_portfolio_score_analytics()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"CatBoost scoring unavailable: {exc}") from exc

    rag_query = payload.query or "Provide underwriting and pricing recommendations based on this CatBoost policy risk score."
    rag_response = await rag_service.query_with_extra_context(
        db,
        query=rag_query,
        top_k=payload.top_k,
        extra_context={"ml_policy_score": result},
    )
    score_context = {
        "avg_portfolio_score": score_analytics.get("avg_score", 0.0),
        "high_risk_policy_count": score_analytics.get("high_count", 0),
        "high_risk_policy_pct": score_analytics.get("high_pct", 0.0),
        "dominant_factor": score_analytics.get("dominant_factor"),
        "top_high_risk_communes": score_analytics.get("top_high_risk_communes", [])[:5],
    }
    return PolicyScoreResponse(
        **result,
        score_context=score_context,
        executive_summary=rag_response.executive_summary,
        confidence=rag_response.confidence,
        recommendations=rag_response.recommendations,
        context_sources=rag_response.context_sources,
        retrieved_documents=rag_response.retrieved_documents,
        generation_mode=rag_response.generation_mode,
        llm_used=rag_response.llm_used,
        llm_error=rag_response.llm_error,
    )


@router.post("/batch-score", response_model=BatchScoreResponse, status_code=status.HTTP_200_OK)
async def batch_score(
    payload: BatchScoreRequest,
    ml_service: MLService = Depends(get_ml_service),
) -> BatchScoreResponse:
    try:
        results = ml_service.batch_score(payload.policies)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Batch CatBoost scoring unavailable: {exc}") from exc
    return BatchScoreResponse(results=results)


@router.get("/feature-importance", response_model=FeatureImportanceResponse)
async def feature_importance(ml_service: MLService = Depends(get_ml_service)) -> FeatureImportanceResponse:
    try:
        features = ml_service.get_feature_importance()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Feature importance unavailable: {exc}") from exc
    return FeatureImportanceResponse(features=features)
