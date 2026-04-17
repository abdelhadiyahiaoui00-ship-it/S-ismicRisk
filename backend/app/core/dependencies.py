from functools import lru_cache
from pathlib import Path

from app.db.session import get_db
from app.rag.service import RAGService


@lru_cache
def get_rag_service() -> RAGService:
    storage_path = Path(__file__).resolve().parents[1] / "data" / "rag_knowledge_store.json"
    service = RAGService(storage_path=storage_path)
    service.initialize()
    return service


__all__ = ["get_db", "get_rag_service"]

