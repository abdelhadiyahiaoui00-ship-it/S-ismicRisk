from fastapi import APIRouter

from app.api.v1.endpoints import geo, health, policies

api_router = APIRouter()
api_router.include_router(health.router, prefix="/health", tags=["Health"])
api_router.include_router(geo.router, prefix="/geo", tags=["Geo"])
api_router.include_router(policies.router, prefix="/policies", tags=["Policies"])
