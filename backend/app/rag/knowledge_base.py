from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+", re.UNICODE)


@dataclass
class KnowledgeDocument:
    doc_id: str
    title: str
    content: str
    source: str
    tags: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


DEFAULT_KNOWLEDGE_DOCUMENTS: list[KnowledgeDocument] = [
    KnowledgeDocument(
        doc_id="rpa-99-zoning",
        title="RPA 99 - Zoning and design constraints",
        source="RPA 99",
        tags=["rpa99", "seismic", "zoning"],
        content=(
            "Zone III corresponds to the highest seismic severity in the Algerian zoning framework. "
            "Design and underwriting in Zone III should be stricter, especially for masonry and "
            "high-concentration portfolios. For chained masonry, allowable heights reduce as seismic "
            "severity increases: around 5 storeys in lower zones, 4 storeys in intermediate zones, "
            "and 3 storeys in the highest seismic zone."
        ),
    ),
    KnowledgeDocument(
        doc_id="rpa-99-structural-minimums",
        title="RPA 99 - Minimum structural safeguards",
        source="RPA 99",
        tags=["rpa99", "construction", "prevention"],
        content=(
            "RPA 99 emphasizes chained masonry, minimum reinforcement, horizontal and vertical tie beams, "
            "and stricter spacing between structural walls as seismic risk increases. Insurers should "
            "treat pre-RPA and weakly controlled construction as higher vulnerability even if located "
            "outside the most severe zones."
        ),
    ),
    KnowledgeDocument(
        doc_id="pricing-guidelines",
        title="Catastrophe pricing guidance by seismic zone",
        source="Insurance Guidelines",
        tags=["pricing", "catnat", "insurance"],
        content=(
            "Recommended annual catastrophe pricing bands rise with seismic severity. Approximate reference "
            "ranges are 0.03%-0.08% for Zone 0, 0.08%-0.15% for Zone I, 0.20%-0.35% for Zone IIa, "
            "0.35%-0.55% for Zone IIb, and 0.60%-1.00% for Zone III. Portfolios materially below those "
            "levels are likely underpriced."
        ),
    ),
    KnowledgeDocument(
        doc_id="reinsurance-guidelines",
        title="Reinsurance structuring for seismic concentration",
        source="Reinsurance Guidelines",
        tags=["reinsurance", "retention", "portfolio"],
        content=(
            "Higher-zone concentration justifies stronger proportional cession and excess-of-loss support. "
            "Indicative cession ranges are 30%-50% for low-risk zones, 60%-75% for IIa-IIb portfolios, "
            "and 75%-90% plus XL support for Zone III concentrations. Net retention should remain below "
            "the company's tolerance for a single severe event."
        ),
    ),
    KnowledgeDocument(
        doc_id="concentration-thresholds",
        title="Concentration management thresholds",
        source="Insurance Guidelines",
        tags=["concentration", "hotspots", "portfolio"],
        content=(
            "A commune carrying more than 5% of net portfolio exposure in Zone IIb or III should be treated "
            "as a concentration alert. A Zone III commune exceeding 2% of net retained exposure is a critical "
            "hotspot requiring underwriting or reinsurance action."
        ),
    ),
    KnowledgeDocument(
        doc_id="historical-seismic-events",
        title="Historical Algerian seismic events",
        source="Seismic Facts",
        tags=["history", "algeria", "events"],
        content=(
            "Historical Algerian earthquakes such as El Asnam 1980 and Boumerdes 2003 demonstrate that "
            "northern urban corridors can suffer high loss concentration. Hackathon-grade risk platforms "
            "should connect present-day portfolio concentration with historically active seismic zones."
        ),
    ),
]


class HybridKnowledgeBase:
    def __init__(self, storage_path: Path):
        self.storage_path = storage_path
        self.documents: list[KnowledgeDocument] = []

    def initialize(self) -> None:
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        persisted = self._load_persisted_documents()
        if persisted:
            self.documents = persisted
        else:
            self.documents = list(DEFAULT_KNOWLEDGE_DOCUMENTS)
            self._persist()

    def add_documents(self, documents: list[KnowledgeDocument]) -> int:
        existing_ids = {doc.doc_id for doc in self.documents}
        for document in documents:
            if document.doc_id in existing_ids:
                document.doc_id = f"{document.doc_id}-{len(existing_ids) + 1}"
            self.documents.append(document)
            existing_ids.add(document.doc_id)
        self._persist()
        return len(documents)

    def count(self) -> int:
        return len(self.documents)

    def search(self, query: str, top_k: int = 4) -> list[tuple[KnowledgeDocument, float]]:
        query_terms = self._tokenize(query)
        if not query_terms:
            return [(doc, 0.0) for doc in self.documents[:top_k]]

        scored: list[tuple[KnowledgeDocument, float]] = []
        for document in self.documents:
            doc_terms = self._tokenize(" ".join([document.title, document.content, " ".join(document.tags)]))
            score = self._hybrid_score(query_terms, doc_terms, document)
            scored.append((document, score))

        scored.sort(key=lambda item: item[1], reverse=True)
        return scored[:top_k]

    def _hybrid_score(self, query_terms: list[str], doc_terms: list[str], document: KnowledgeDocument) -> float:
        query_set = set(query_terms)
        doc_set = set(doc_terms)
        overlap = len(query_set & doc_set)
        keyword_density = overlap / max(len(query_set), 1)

        tf_score = 0.0
        for term in query_set:
            tf = doc_terms.count(term)
            if tf:
                tf_score += 1.0 + math.log(tf + 1)

        tag_bonus = 0.0
        lower_query = " ".join(query_terms)
        for tag in document.tags:
            if tag.lower().replace("-", "") in lower_query.replace("-", ""):
                tag_bonus += 0.2

        source_bonus = 0.1 if "rpa" in document.source.lower() and "zone" in lower_query else 0.0
        return round(keyword_density * 0.55 + tf_score * 0.25 + tag_bonus + source_bonus, 4)

    def _tokenize(self, value: str) -> list[str]:
        return [token.lower() for token in TOKEN_PATTERN.findall(value)]

    def _load_persisted_documents(self) -> list[KnowledgeDocument]:
        if not self.storage_path.exists():
            return []
        raw = json.loads(self.storage_path.read_text(encoding="utf-8"))
        return [KnowledgeDocument(**item) for item in raw]

    def _persist(self) -> None:
        payload = [asdict(document) for document in self.documents]
        self.storage_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

