from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.core.dependencies import get_ml_service
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
) -> PolicyScoreResponse:
    try:
        result = ml_service.score_policy(payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"CatBoost scoring unavailable: {exc}") from exc
    return PolicyScoreResponse(**result)


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
