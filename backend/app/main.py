from pathlib import Path

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.router import api_router
from app.core.config import settings
from app.core.dependencies import get_ml_service, get_rag_service
from app.schemas.health import HealthResponse, RootResponse


def create_application() -> FastAPI:
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        debug=settings.debug,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=[settings.cors_origins, "https://aecnerd.netlify.app"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    uploads_path = Path(__file__).resolve().parents[1] / settings.uploads_dir
    uploads_path.mkdir(parents=True, exist_ok=True)
    app.mount(f"/{settings.uploads_dir.strip('/')}", StaticFiles(directory=uploads_path), name="uploads")

    @app.get("/", tags=["Root"], response_model=RootResponse, status_code=status.HTTP_200_OK)
    async def root() -> RootResponse:
        return RootResponse(
            message="RASED API is running",
            docs_url="/docs",
            openapi_url="/openapi.json",
        )

    @app.get("/health", tags=["Health"], response_model=HealthResponse, status_code=status.HTTP_200_OK)
    async def healthcheck() -> HealthResponse:
        return HealthResponse(status="ok", service="backend", environment=settings.environment)

    @app.on_event("startup")
    async def initialize_services() -> None:
        get_rag_service()
        get_ml_service()

    app.include_router(api_router, prefix=settings.api_v1_prefix)
    if settings.api_v1_prefix != "/api":
        app.include_router(api_router, prefix="/api")
    return app


app = create_application()
