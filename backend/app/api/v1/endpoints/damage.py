from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_damage_ai_service, get_rag_service
from app.db.session import get_db
from app.rag.service import RAGService
from app.schemas.damage import DamageAssessmentResponse, DamageHealthResponse
from app.services.damage_ai_service import DamageAIService
from app.services.geo_service import geo_service

router = APIRouter()


@router.get("/health", response_model=DamageHealthResponse)
async def damage_health(
    damage_service: DamageAIService = Depends(get_damage_ai_service),
) -> DamageHealthResponse:
    return DamageHealthResponse(**damage_service.health())


@router.post("/estimate", response_model=DamageAssessmentResponse, status_code=status.HTTP_200_OK)
async def estimate_damage(
    image: UploadFile = File(...),
    image_type: str = Form("satellite"),
    area_km2: float = Form(1.0),
    construction_type: str = Form("Beton arme"),
    zone_sismique: str | None = Form(None),
    wilaya_code: str | None = Form(None),
    commune_name: str | None = Form(None),
    query: str | None = Form(None),
    top_k: int = Form(4),
    damage_service: DamageAIService = Depends(get_damage_ai_service),
    rag_service: RAGService = Depends(get_rag_service),
    db: AsyncSession = Depends(get_db),
) -> DamageAssessmentResponse:
    image_bytes = await image.read()
    if not image_bytes:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded image is empty.")

    cleaned_wilaya_code = str(wilaya_code).zfill(2) if wilaya_code else None
    cleaned_commune_name = commune_name.strip() if commune_name else None
    cleaned_zone = zone_sismique

    if cleaned_wilaya_code and cleaned_commune_name:
        commune_detail = await geo_service.get_commune_detail(db, cleaned_wilaya_code, cleaned_commune_name)
        if commune_detail:
            cleaned_commune_name = commune_detail.name
            cleaned_wilaya_code = commune_detail.wilaya_code
            cleaned_zone = cleaned_zone or commune_detail.zone_sismique
    elif cleaned_zone is None:
        cleaned_zone = "IIa"

    if cleaned_zone is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="zone_sismique is required when commune lookup cannot resolve it.",
        )

    try:
        damage_result = damage_service.estimate_damage(
            image_bytes=image_bytes,
            image_type=image_type,
            area_km2=area_km2,
            construction_type=construction_type,
            zone_sismique=cleaned_zone,
            wilaya_code=cleaned_wilaya_code,
            commune_name=cleaned_commune_name,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Damage assessment unavailable: {exc}") from exc

    rag_query = query or "Provide parametric insurance, prevention, and underwriting recommendations based on this damage assessment."
    rag_response = await rag_service.query_with_extra_context(
        db,
        query=rag_query,
        top_k=top_k,
        extra_context={"damage_assessment": damage_result},
    )

    return DamageAssessmentResponse(
        damage_assessment=damage_result,
        executive_summary=rag_response.executive_summary,
        confidence=rag_response.confidence,
        recommendations=rag_response.recommendations,
        context_sources=rag_response.context_sources,
        retrieved_documents=rag_response.retrieved_documents,
        generation_mode=rag_response.generation_mode,
        llm_used=rag_response.llm_used,
        llm_error=rag_response.llm_error,
    )
