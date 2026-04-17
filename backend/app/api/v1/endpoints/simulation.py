from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.dependencies import get_rag_service
from app.db.session import get_db
from app.rag.service import RAGService
from app.schemas.simulation import (
    ScenarioMeta,
    SimulationHealthResponse,
    SimulationRequest,
    SimulationRunResponse,
)
from app.services.simulation_service import simulation_service

router = APIRouter()


@router.get("/health", response_model=SimulationHealthResponse)
async def simulation_health() -> SimulationHealthResponse:
    return SimulationHealthResponse(
        status="ok",
        available_scenarios=list(simulation_service.SCENARIOS.keys()),
    )


@router.get("/scenarios", response_model=dict[str, ScenarioMeta])
async def list_scenarios() -> dict[str, ScenarioMeta]:
    return {
        key: ScenarioMeta(**value)
        for key, value in simulation_service.list_scenarios().items()
    }


@router.post("/run", response_model=SimulationRunResponse, status_code=status.HTTP_200_OK)
async def run_simulation(
    payload: SimulationRequest,
    db: AsyncSession = Depends(get_db),
    rag_service: RAGService = Depends(get_rag_service),
) -> SimulationRunResponse:
    result = await simulation_service.run(db, payload)
    if "error" in result:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=result["error"])

    rag_query = (
        payload.query
        or "Provide portfolio recommendations based on this Monte Carlo earthquake loss simulation."
    )
    rag_response = await rag_service.query_with_extra_context(
        db,
        query=rag_query,
        top_k=payload.top_k,
        extra_context={"monte_carlo": result},
    )

    return SimulationRunResponse(
        monte_carlo=result,
        executive_summary=rag_response.executive_summary,
        confidence=rag_response.confidence,
        recommendations=rag_response.recommendations,
        context_sources=rag_response.context_sources,
        retrieved_documents=rag_response.retrieved_documents,
        generation_mode=rag_response.generation_mode,
        llm_used=rag_response.llm_used,
        llm_error=rag_response.llm_error,
    )
