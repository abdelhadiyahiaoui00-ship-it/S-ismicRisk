from fastapi import APIRouter, status

from app.core.config import settings
from app.schemas.health import HealthResponse

router = APIRouter()


@router.get("", response_model=HealthResponse, status_code=status.HTTP_200_OK)
async def api_health() -> HealthResponse:
    return HealthResponse(status="ok", service="api", environment=settings.environment)
