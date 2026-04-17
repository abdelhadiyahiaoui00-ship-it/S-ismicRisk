from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
    environment: str | None = None


class RootResponse(BaseModel):
    message: str
    docs_url: str
    openapi_url: str
