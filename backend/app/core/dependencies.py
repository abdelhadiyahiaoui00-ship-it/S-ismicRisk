from functools import lru_cache
from pathlib import Path

from app.db.session import get_db
from app.rag.service import RAGService
from app.services.ml_service import MLService, ml_service


@lru_cache
def get_rag_service() -> RAGService:
    storage_path = Path(__file__).resolve().parents[1] / "data" / "rag_knowledge_store.json"
    service = RAGService(storage_path=storage_path)
    service.initialize()
    return service


@lru_cache
def get_ml_service() -> MLService:
    ml_service.load_models()
    return ml_service


__all__ = ["get_db", "get_rag_service", "get_ml_service"]
