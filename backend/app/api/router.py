from fastapi import APIRouter

from app.api.v1.endpoints import damage, geo, health, ml, policies, rag, simulation

api_router = APIRouter()
api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(geo.router, prefix="/geo", tags=["Geo"])
api_router.include_router(damage.router, prefix="/damage", tags=["Damage"])
api_router.include_router(ml.router, prefix="/ml", tags=["ML"])
api_router.include_router(policies.router, prefix="/policies", tags=["Policies"])
api_router.include_router(rag.router, prefix="/rag", tags=["RAG"])
api_router.include_router(simulation.router, prefix="/simulation", tags=["Simulation"])
