from functools import lru_cache
from typing import Annotated

from pydantic import Field
from pydantic.functional_validators import BeforeValidator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


def _normalize_db_url(url: str, *, async_driver: bool) -> str:
    if not url:
        return url
    if async_driver:
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql+asyncpg://", 1)
        if url.startswith("postgresql://") and "+asyncpg" not in url:
            return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    else:
        if url.startswith("postgres://"):
            return url.replace("postgres://", "postgresql://", 1)
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return url


def _parse_cors_origins(value: str | list[str]) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str = Field(default="RASED API", alias="APP_NAME")
    environment: str = Field(default="development", alias="ENVIRONMENT")
    debug: bool = Field(default=True, alias="DEBUG")
    api_v1_prefix: str = Field(default="/api/v1", alias="API_V1_PREFIX")
    secret_key: str = Field(default="change-me", alias="SECRET_KEY")

    backend_host: str = Field(default="0.0.0.0", alias="BACKEND_HOST")
    backend_port: int = Field(default=8000, alias="BACKEND_PORT")

    database_url: str = Field(
        default="postgresql+asyncpg://rased:password@localhost:5432/rased_db",
        alias="DATABASE_URL",
    )
    alembic_database_url: str = Field(
        default="postgresql://rased:password@localhost:5432/rased_db",
        alias="ALEMBIC_DATABASE_URL",
    )
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-2.5-flash", alias="GEMINI_MODEL")
    gemini_fallback_models: Annotated[list[str], NoDecode, BeforeValidator(_parse_cors_origins)] = Field(
        default=["gemini-2.5-flash-lite", "gemini-1.5-flash"],
        alias="GEMINI_FALLBACK_MODELS",
    )

    usgs_poll_interval: int = Field(default=60, alias="USGS_POLL_INTERVAL")
    min_alert_magnitude: float = Field(default=4.0, alias="MIN_ALERT_MAGNITUDE")
    auto_simulate_threshold: float = Field(default=5.0, alias="AUTO_SIMULATE_THRESHOLD")

    reinsurance_cession_rate: float = Field(default=0.70, alias="REINSURANCE_CESSION_RATE")
    retention_rate: float = Field(default=0.30, alias="RETENTION_RATE")

    catboost_model_path: str = Field(default="ml_models/catboost_model.cbm", alias="CATBOOST_MODEL_PATH")
    damage_cnn_path: str = Field(default="ml_models/damage_cnn.pt", alias="DAMAGE_CNN_PATH")

    uploads_dir: str = Field(default="uploads", alias="UPLOADS_DIR")
    heatmaps_dir: str = Field(default="uploads/heatmaps", alias="HEATMAPS_DIR")

    cors_origins: Annotated[list[str], NoDecode, BeforeValidator(_parse_cors_origins)] = Field(
        default=["http://localhost:3000", "http://localhost:5173"],
        alias="CORS_ORIGINS",
    )

    def model_post_init(self, __context: object) -> None:
        self.database_url = _normalize_db_url(self.database_url, async_driver=True)
        self.alembic_database_url = _normalize_db_url(self.alembic_database_url, async_driver=False)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
