from __future__ import annotations

import json
import re
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable
from uuid import uuid4

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.policy import Policy
from app.rag.knowledge_base import HybridKnowledgeBase, KnowledgeDocument
from app.schemas.geo import HotspotData, PortfolioKPIs, PremiumAdequacyRow
from app.schemas.recommendation import (
    IngestDocumentRequest,
    PortfolioAnalysisResponse,
    RAGHealthResponse,
    RAGIngestResponse,
    RAGQueryResponse,
    RecommendationItem,
    RecommendationsResponse,
    RetrievedDocument,
    RiskInsight,
    RiskInsightsResponse,
)
from app.services.geo_service import geo_service


class RAGService:
    def __init__(self, storage_path: Path):
        self.knowledge_base = HybridKnowledgeBase(storage_path=storage_path)
        self.last_initialized_at: datetime | None = None
        self.gemini_api_key = settings.gemini_api_key.strip()
        self.model_name = settings.gemini_model.strip() or "gemini-2.5-flash"
        self.model_fallbacks = [model for model in settings.gemini_fallback_models if model]

    def initialize(self) -> None:
        self.knowledge_base.initialize()
        self.last_initialized_at = datetime.now(timezone.utc)

    async def health(self) -> RAGHealthResponse:
        return RAGHealthResponse(
            status="ok",
            model_loaded=bool(self.gemini_api_key),
            model_provider="gemini-1.5-flash" if self.gemini_api_key else "heuristic-fallback",
            vector_db_status="hybrid-portfolio-knowledge-index",
            knowledge_documents=self.knowledge_base.count(),
            last_initialized_at=self.last_initialized_at,
        )

    async def ingest(self, payload: list[IngestDocumentRequest]) -> RAGIngestResponse:
        docs = [
            KnowledgeDocument(
                doc_id=f"user-{uuid4().hex}",
                title=item.title,
                content=item.content,
                source=item.source,
                tags=item.tags,
            )
            for item in payload
        ]
        ingested = self.knowledge_base.add_documents(docs)
        return RAGIngestResponse(
            ingested_documents=ingested,
            total_documents=self.knowledge_base.count(),
            status="ingested",
        )

    async def query(self, db: AsyncSession, query: str, top_k: int = 4) -> RAGQueryResponse:
        return await self.query_with_extra_context(db, query=query, top_k=top_k, extra_context=None)

    async def query_with_extra_context(
        self,
        db: AsyncSession,
        query: str,
        top_k: int = 4,
        extra_context: dict | None = None,
    ) -> RAGQueryResponse:
        context = await self._build_context(db, extra_context=extra_context)
        retrieved = self._retrieve_documents(query, context, top_k=top_k)
        recommendations = self._build_recommendations(context, retrieved, user_query=query)
        llm_payload = await self._generate_with_gemini(query, context, recommendations, retrieved)
        answer = llm_payload.get("answer") or self._compose_answer(query, context, recommendations)
        executive_summary = llm_payload.get("executive_summary") or context["executive_summary"]
        confidence = float(llm_payload.get("confidence") or self._overall_confidence(recommendations))
        final_recommendations = self._merge_llm_recommendations(llm_payload.get("recommendations"), recommendations)
        return RAGQueryResponse(
            answer=answer,
            executive_summary=executive_summary,
            confidence=confidence,
            recommendations=final_recommendations,
            context_sources=self._context_sources(retrieved, context),
            retrieved_documents=retrieved,
            generation_mode="gemini" if llm_payload.get("_llm_used") else "fallback",
            llm_used=bool(llm_payload.get("_llm_used")),
            llm_error=llm_payload.get("_llm_error"),
        )

    async def get_portfolio_analysis(self, db: AsyncSession) -> PortfolioAnalysisResponse:
        context = await self._build_context(db)
        return PortfolioAnalysisResponse(
            generated_at=datetime.now(timezone.utc),
            executive_summary=context["executive_summary"],
            total_exposure=context["kpis"].total_exposure,
            total_policies=context["kpis"].total_policies,
            net_retention=context["kpis"].net_retention,
            top_hotspots=[item.model_dump() for item in context["hotspots"]],
            zone_breakdown=[item.model_dump() for item in context["kpis"].by_zone],
            concentration_alerts=context["concentration_alerts"],
        )

    async def get_risk_insights(self, db: AsyncSession) -> RiskInsightsResponse:
        context = await self._build_context(db)
        retrieved = self._retrieve_documents("portfolio risk insights overexposure seismic concentration", context)
        insights = self._build_risk_insights(context)
        return RiskInsightsResponse(
            generated_at=datetime.now(timezone.utc),
            insights=insights,
            retrieved_documents=retrieved,
        )

    async def get_recommendations(self, db: AsyncSession) -> RecommendationsResponse:
        context = await self._build_context(db)
        retrieved = self._retrieve_documents("portfolio recommendations pricing reinsurance concentration", context)
        recommendations = self._build_recommendations(context, retrieved)
        llm_payload = await self._generate_with_gemini(
            "Provide the most important portfolio recommendations.",
            context,
            recommendations,
            retrieved,
        )
        recommendations = self._merge_llm_recommendations(llm_payload.get("recommendations"), recommendations)
        return RecommendationsResponse(
            generated_at=datetime.now(timezone.utc),
            executive_summary=llm_payload.get("executive_summary") or context["executive_summary"],
            recommendations=recommendations,
            confidence=float(llm_payload.get("confidence") or self._overall_confidence(recommendations)),
            retrieved_documents=retrieved,
        )

    async def _build_context(self, db: AsyncSession, extra_context: dict | None = None) -> dict:
        kpis = await geo_service.get_portfolio_kpis(db)
        hotspots = await geo_service.get_hotspots(db, top_n=5)
        premium_adequacy = await geo_service.get_premium_adequacy(db)
        portfolio_years = await db.execute(select(func.min(Policy.policy_year), func.max(Policy.policy_year)))
        min_year, max_year = portfolio_years.one()

        executive_summary = (
            f"Portfolio of {kpis.total_policies} policies with gross exposure of {self._fmt_money(kpis.total_exposure)} "
            f"and net retained exposure of {self._fmt_money(kpis.net_retention)}. "
            f"Main concentration sits in {hotspots[0].commune_name if hotspots else 'diversified communes'} "
            f"and the data currently spans underwriting years {min_year} to {max_year}."
        )

        concentration_alerts = self._build_concentration_alerts(kpis, hotspots)
        top_wilayas = await self._top_wilaya_exposure(db)
        type_mix = await self._top_risk_types(db)
        search_query = " ".join(
            filter(
                None,
                [
                    "portfolio risk",
                    " ".join(zone.zone for zone in kpis.by_zone[:3]),
                    hotspots[0].commune_name if hotspots else "",
                    "reinsurance premium adequacy",
                ],
            )
        )

        context = {
            "kpis": kpis,
            "hotspots": hotspots,
            "premium_adequacy": premium_adequacy,
            "executive_summary": executive_summary,
            "concentration_alerts": concentration_alerts,
            "top_wilayas": top_wilayas,
            "top_risk_types": type_mix,
            "search_query": search_query,
        }
        if extra_context:
            monte_carlo = extra_context.get("monte_carlo")
            if monte_carlo:
                context["monte_carlo"] = monte_carlo
                high_risk_zones = monte_carlo.get("high_risk_zones", [])
                overexposed_wilayas = monte_carlo.get("overexposed_wilayas", [])
                summary_bits = [
                    f"Monte Carlo expected net loss {self._fmt_money(Decimal(str(monte_carlo.get('expected_net_loss', 0))))}",
                    f"VaR 95 {self._fmt_money(Decimal(str(monte_carlo.get('var_95', 0))))}",
                    f"Worst case {self._fmt_money(Decimal(str(monte_carlo.get('worst_case_loss', 0))))}",
                ]
                context["executive_summary"] = context["executive_summary"] + " " + ". ".join(summary_bits) + "."
                context["search_query"] = " ".join(
                    filter(
                        None,
                        [
                            context["search_query"],
                            "monte carlo simulation",
                            " ".join(str(item.get("zone_sismique", "")) for item in high_risk_zones[:3]),
                            " ".join(str(item.get("wilaya_name", "")) for item in overexposed_wilayas[:3]),
                        ],
                    )
                )
        return context

    def _build_concentration_alerts(self, kpis: PortfolioKPIs, hotspots: list[HotspotData]) -> list[str]:
        alerts: list[str] = []
        for hotspot in hotspots[:3]:
            net_share = Decimal("0")
            if kpis.net_retention > 0:
                net_share = hotspot.total_exposure * Decimal("0.30") / kpis.net_retention * Decimal("100")
            if hotspot.zone_sismique in {"IIb", "III"} and net_share >= Decimal("2"):
                alerts.append(
                    f"{hotspot.commune_name} ({hotspot.zone_sismique}) concentrates {net_share:.2f}% of retained exposure."
                )

        underpriced = [row for row in []]
        if not alerts:
            alerts.append("No commune currently breaches the configured concentration alert thresholds.")
        return alerts

    def _retrieve_documents(self, user_query: str, context: dict, top_k: int = 4) -> list[RetrievedDocument]:
        combined_query = f"{user_query} {context['search_query']}".strip()
        knowledge_matches = self.knowledge_base.search(combined_query, top_k=top_k)
        portfolio_documents = self._build_portfolio_documents(context)
        portfolio_matches = self._rank_portfolio_documents(combined_query, portfolio_documents, top_k=top_k)

        merged: list[RetrievedDocument] = [
            RetrievedDocument(
                source=document.source,
                title=document.title,
                score=score,
                excerpt=document.content[:260],
                tags=document.tags,
            )
            for document, score in knowledge_matches
        ]
        merged.extend(portfolio_matches)
        merged.sort(key=lambda item: item.score, reverse=True)
        return merged[:top_k]

    def _build_risk_insights(self, context: dict) -> list[RiskInsight]:
        kpis: PortfolioKPIs = context["kpis"]
        hotspots: list[HotspotData] = context["hotspots"]
        premium_rows: list[PremiumAdequacyRow] = context["premium_adequacy"]

        insights: list[RiskInsight] = []
        if hotspots:
            top = hotspots[0]
            insights.append(
                RiskInsight(
                    title="Top exposure hotspot",
                    severity="HIGH" if top.zone_sismique == "III" else "MEDIUM",
                    description=f"{top.commune_name} is the highest exposure commune in the portfolio.",
                    metric_name="hotspot_score",
                    metric_value=top.hotspot_score,
                    affected_scope=f"{top.wilaya_name} / {top.commune_name}",
                    explanation=(
                        f"The commune combines {self._fmt_money(top.total_exposure)} of exposure with "
                        f"a seismic zone of {top.zone_sismique}."
                    ),
                )
            )

        if kpis.by_zone:
            highest_zone = max(kpis.by_zone, key=lambda item: item.exposure)
            insights.append(
                RiskInsight(
                    title="Dominant seismic zone",
                    severity="HIGH" if highest_zone.zone in {"IIb", "III"} else "MEDIUM",
                    description=f"Zone {highest_zone.zone} carries the largest share of exposure.",
                    metric_name="zone_exposure_pct",
                    metric_value=highest_zone.pct,
                    affected_scope=f"Zone {highest_zone.zone}",
                    explanation=(
                        f"Zone {highest_zone.zone} contributes {highest_zone.pct}% of gross exposure "
                        f"with {highest_zone.policy_count} policies."
                    ),
                )
            )

        if premium_rows:
            worst_gap = min(premium_rows, key=lambda item: item.premium_gap_pct)
            insights.append(
                RiskInsight(
                    title="Largest pricing deficiency",
                    severity="HIGH" if worst_gap.premium_gap_pct < Decimal("-25") else "MEDIUM",
                    description=f"{worst_gap.type_risque} in Zone {worst_gap.zone} appears underpriced.",
                    metric_name="premium_gap_pct",
                    metric_value=worst_gap.premium_gap_pct,
                    affected_scope=f"Zone {worst_gap.zone} / {worst_gap.type_risque}",
                    explanation=(
                        f"Observed pricing is {worst_gap.premium_gap_pct}% away from the reference adequate rate "
                        f"for {self._fmt_money(worst_gap.total_exposure)} of exposure."
                    ),
                )
            )

        return insights

    def _build_recommendations(
        self,
        context: dict,
        retrieved: list[RetrievedDocument],
        user_query: str | None = None,
    ) -> list[RecommendationItem]:
        kpis: PortfolioKPIs = context["kpis"]
        hotspots: list[HotspotData] = context["hotspots"]
        premium_rows: list[PremiumAdequacyRow] = context["premium_adequacy"]
        recommendations: list[RecommendationItem] = []

        if hotspots:
            hotspot = hotspots[0]
            confidence = 0.91 if hotspot.zone_sismique == "III" else 0.84
            recommendations.append(
                RecommendationItem(
                    priority="HIGH" if hotspot.zone_sismique == "III" else "MEDIUM",
                    category="Concentration",
                    title="Reduce top seismic hotspot",
                    description=(
                        f"{hotspot.commune_name} in Wilaya {hotspot.wilaya_name} concentrates "
                        f"{self._fmt_money(hotspot.total_exposure)} in Zone {hotspot.zone_sismique}."
                    ),
                    action=(
                        "Cap new writings in the hotspot, review facultative placements, and rebalance growth "
                        "toward lower-severity wilayas."
                    ),
                    confidence=confidence,
                    explanation=(
                        f"Hotspot score {hotspot.hotspot_score} is the highest in the current portfolio and "
                        "creates correlated loss concentration."
                    ),
                    rpa_reference="RPA 99 zoning principles",
                    evidence=self._build_evidence(retrieved, count=2),
                )
            )

        underpriced = [row for row in premium_rows if row.premium_gap_pct < Decimal("-10")]
        if underpriced:
            worst = min(underpriced, key=lambda item: item.premium_gap_pct)
            recommendations.append(
                RecommendationItem(
                    priority="HIGH",
                    category="Tarification",
                    title="Correct underpriced CAT layers",
                    description=(
                        f"{worst.type_risque} in Zone {worst.zone} is priced below the reference catastrophe rate "
                        f"by {worst.premium_gap_pct}%."
                    ),
                    action=(
                        "Review technical rates at renewal, introduce a seismic loading floor, and align pricing "
                        "with zone-driven catastrophe adequacy."
                    ),
                    confidence=0.88,
                    explanation=(
                        f"Observed rate {worst.observed_rate} is below the adequate rate {worst.adequate_rate} "
                        f"for a block of {self._fmt_money(worst.total_exposure)}."
                    ),
                    rpa_reference=None,
                    evidence=self._build_evidence(retrieved, count=2),
                )
            )

        if kpis.net_retention > Decimal("0") and hotspots:
            top_exposure_net = hotspots[0].total_exposure * Decimal("0.30")
            recommendations.append(
                RecommendationItem(
                    priority="MEDIUM",
                    category="Reinsurance",
                    title="Stress retention against hotspot loss",
                    description=(
                        f"Retained exposure is {self._fmt_money(kpis.net_retention)}, while the top hotspot alone "
                        f"contributes roughly {self._fmt_money(top_exposure_net)} of retained value before event severity."
                    ),
                    action=(
                        "Model a higher cession or attach an excess-of-loss layer focused on northern Zone IIb/III communes."
                    ),
                    confidence=0.82,
                    explanation="Retained losses can accumulate rapidly when multiple policies share the same seismic footprint.",
                    rpa_reference=None,
                    evidence=self._build_evidence(retrieved, count=3),
                )
            )

        monte_carlo = context.get("monte_carlo")
        if monte_carlo:
            expected_net_loss = Decimal(str(monte_carlo.get("expected_net_loss", 0)))
            var_95 = Decimal(str(monte_carlo.get("var_95", 0)))
            worst_case_loss = Decimal(str(monte_carlo.get("worst_case_loss", 0)))
            high_risk_zones = monte_carlo.get("high_risk_zones", [])
            overexposed_wilayas = monte_carlo.get("overexposed_wilayas", [])

            if kpis.net_retention > 0 and var_95 > kpis.net_retention * Decimal("0.20"):
                recommendations.append(
                    RecommendationItem(
                        priority="HIGH",
                        category="Reinsurance",
                        title="Rebalance catastrophe retention",
                        description=(
                            f"Monte Carlo stress shows expected net loss of {self._fmt_money(expected_net_loss)} "
                            f"and VaR 95 of {self._fmt_money(var_95)}."
                        ),
                        action=(
                            "Review cession structure, add aggregate or occurrence protection, and test a lower net retention "
                            "for northern seismic corridors."
                        ),
                        confidence=0.9,
                        explanation=(
                            f"Worst simulated retained loss reaches {self._fmt_money(worst_case_loss)}, which indicates "
                            "meaningful tail volatility."
                        ),
                        rpa_reference=None,
                        evidence=self._build_evidence(retrieved, count=3),
                    )
                )

            if high_risk_zones:
                top_zone = high_risk_zones[0]
                recommendations.append(
                    RecommendationItem(
                        priority="HIGH" if top_zone.get("zone_sismique") in {"IIb", "III"} else "MEDIUM",
                        category="Concentration",
                        title="Reduce simulated zone concentration",
                        description=(
                            f"Zone {top_zone.get('zone_sismique')} is the leading simulated loss bucket with "
                            f"{self._fmt_money(Decimal(str(top_zone.get('expected_loss', 0))))} of expected loss."
                        ),
                        action=(
                            "Slow new writings in the stressed zone, review underwriting appetite, and transfer peak exposure "
                            "away from the highest-loss zone cluster."
                        ),
                        confidence=0.86,
                        explanation=(
                            f"The simulation ranks Zone {top_zone.get('zone_sismique')} above other zones for expected loss "
                            f"across {top_zone.get('policy_count', 0)} affected policies."
                        ),
                        rpa_reference="RPA 99 zoning principles",
                        evidence=self._build_evidence(retrieved, count=3),
                    )
                )

            if overexposed_wilayas:
                top_wilaya = overexposed_wilayas[0]
                recommendations.append(
                    RecommendationItem(
                        priority="MEDIUM",
                        category="Prevention",
                        title="Target loss prevention in top wilaya",
                        description=(
                            f"{top_wilaya.get('wilaya_name')} leads simulated wilaya losses with "
                            f"{self._fmt_money(Decimal(str(top_wilaya.get('expected_loss', 0))))} expected loss."
                        ),
                        action=(
                            "Prioritize engineering surveys, stricter construction screening, and portfolio steering for this wilaya."
                        ),
                        confidence=0.8,
                        explanation=(
                            f"The simulation flags {top_wilaya.get('wilaya_name')} as the most overexposed wilaya "
                            f"under the selected earthquake scenario."
                        ),
                        rpa_reference=None,
                        evidence=self._build_evidence(retrieved, count=2),
                    )
                )

        return self._rank_recommendations_for_query(recommendations, context, user_query)

    def _compose_answer(self, query: str, context: dict, recommendations: list[RecommendationItem]) -> str:
        if not recommendations:
            return "The current portfolio does not yet produce a strong recommendation signal."
        top = recommendations[0]
        return (
            f"For the question '{query}', the strongest portfolio signal is {top.title.lower()}. "
            f"{top.description} Recommended action: {top.action}"
        )

    def _build_evidence(self, retrieved: Iterable[RetrievedDocument], count: int = 2) -> list[str]:
        return [f"{doc.source}: {doc.title}" for doc in list(retrieved)[:count]]

    def _context_sources(self, retrieved: list[RetrievedDocument], context: dict | None = None) -> list[str]:
        sources = {"Portfolio KPIs", "Hotspots", "Premium Adequacy"}
        if context and context.get("monte_carlo"):
            sources.add("Monte Carlo Simulation")
        sources.update(f"{doc.source} - {doc.title}" for doc in retrieved)
        return sorted(sources)

    def _overall_confidence(self, recommendations: list[RecommendationItem]) -> float:
        if not recommendations:
            return 0.5
        return round(sum(item.confidence for item in recommendations) / len(recommendations), 2)

    def _fmt_money(self, value: Decimal) -> str:
        return f"{value:,.0f} DZD"

    async def _top_wilaya_exposure(self, db: AsyncSession) -> list[dict]:
        result = await db.execute(
            select(
                Policy.code_wilaya,
                Policy.wilaya,
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
            )
            .group_by(Policy.code_wilaya, Policy.wilaya)
            .order_by(func.sum(Policy.capital_assure).desc())
            .limit(5)
        )
        return [
            {
                "code_wilaya": code,
                "wilaya": wilaya,
                "policy_count": policy_count,
                "total_exposure": Decimal(total_exposure),
            }
            for code, wilaya, policy_count, total_exposure in result.all()
        ]

    async def _top_risk_types(self, db: AsyncSession) -> list[dict]:
        result = await db.execute(
            select(
                Policy.type_risque,
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
            )
            .group_by(Policy.type_risque)
            .order_by(func.sum(Policy.capital_assure).desc())
            .limit(5)
        )
        return [
            {
                "type_risque": type_risque,
                "policy_count": policy_count,
                "total_exposure": Decimal(total_exposure),
            }
            for type_risque, policy_count, total_exposure in result.all()
        ]

    def _build_portfolio_documents(self, context: dict) -> list[KnowledgeDocument]:
        documents: list[KnowledgeDocument] = []
        kpis: PortfolioKPIs = context["kpis"]

        documents.append(
            KnowledgeDocument(
                doc_id="portfolio-summary",
                title="Portfolio summary",
                source="Portfolio Data",
                tags=["portfolio", "summary", "exposure"],
                content=(
                    f"Gross exposure {kpis.total_exposure} DZD, net retention {kpis.net_retention} DZD, "
                    f"total policies {kpis.total_policies}. Concentration alerts: {' | '.join(context['concentration_alerts'])}."
                ),
            )
        )

        for zone in kpis.by_zone:
            documents.append(
                KnowledgeDocument(
                    doc_id=f"zone-{zone.zone}",
                    title=f"Zone {zone.zone} exposure profile",
                    source="Portfolio Data",
                    tags=["zone", zone.zone, "exposure"],
                    content=(
                        f"Zone {zone.zone} has exposure {zone.exposure} DZD, {zone.policy_count} policies, "
                        f"and {zone.pct}% of total gross exposure."
                    ),
                )
            )

        for hotspot in context["hotspots"]:
            documents.append(
                KnowledgeDocument(
                    doc_id=f"hotspot-{hotspot.commune_code}",
                    title=f"Hotspot {hotspot.commune_name}",
                    source="Portfolio Data",
                    tags=["hotspot", hotspot.zone_sismique, hotspot.commune_name.lower()],
                    content=(
                        f"{hotspot.commune_name} in {hotspot.wilaya_name} has {hotspot.total_exposure} DZD exposure, "
                        f"{hotspot.policy_count} policies, hotspot score {hotspot.hotspot_score}, "
                        f"zone {hotspot.zone_sismique}."
                    ),
                )
            )

        for row in context["premium_adequacy"][:10]:
            documents.append(
                KnowledgeDocument(
                    doc_id=f"pricing-{row.zone}-{row.type_risque[:20]}",
                    title=f"Pricing adequacy {row.zone} {row.type_risque}",
                    source="Pricing Data",
                    tags=["pricing", row.zone, row.type_risque.lower()],
                    content=(
                        f"{row.type_risque} in zone {row.zone}: observed rate {row.observed_rate}, "
                        f"adequate rate {row.adequate_rate}, premium gap {row.premium_gap_pct}%, "
                        f"policy count {row.policy_count}, total exposure {row.total_exposure} DZD."
                    ),
                )
            )

        for wilaya in context["top_wilayas"]:
            documents.append(
                KnowledgeDocument(
                    doc_id=f"wilaya-{wilaya['code_wilaya']}",
                    title=f"Wilaya {wilaya['wilaya']} exposure",
                    source="Portfolio Data",
                    tags=["wilaya", wilaya["wilaya"].lower(), "exposure"],
                    content=(
                        f"Wilaya {wilaya['wilaya']} holds {wilaya['total_exposure']} DZD exposure across "
                        f"{wilaya['policy_count']} policies."
                    ),
                )
            )

        for item in context["top_risk_types"]:
            documents.append(
                KnowledgeDocument(
                    doc_id=f"type-{item['type_risque'][:20]}",
                    title=f"Risk type {item['type_risque']}",
                    source="Portfolio Data",
                    tags=["type_risque", item["type_risque"].lower()],
                    content=(
                        f"{item['type_risque']} represents {item['total_exposure']} DZD exposure over "
                        f"{item['policy_count']} policies."
                    ),
                )
            )

        monte_carlo = context.get("monte_carlo")
        if monte_carlo:
            documents.append(
                KnowledgeDocument(
                    doc_id="monte-carlo-summary",
                    title="Monte Carlo portfolio stress summary",
                    source="Monte Carlo Simulation",
                    tags=["simulation", "loss", "var", "stress"],
                    content=(
                        f"Scenario {monte_carlo.get('scenario_name')} affects {monte_carlo.get('affected_policies')} policies. "
                        f"Expected net loss {monte_carlo.get('expected_net_loss')} DZD, VaR 95 {monte_carlo.get('var_95')} DZD, "
                        f"worst case loss {monte_carlo.get('worst_case_loss')} DZD."
                    ),
                )
            )
            for zone in monte_carlo.get("high_risk_zones", [])[:5]:
                documents.append(
                    KnowledgeDocument(
                        doc_id=f"simulation-zone-{zone.get('zone_sismique')}",
                        title=f"Simulated high risk zone {zone.get('zone_sismique')}",
                        source="Monte Carlo Simulation",
                        tags=["simulation", "zone", str(zone.get("zone_sismique", "")).lower()],
                        content=(
                            f"Zone {zone.get('zone_sismique')} produces {zone.get('expected_loss')} DZD of expected simulated loss "
                            f"over {zone.get('policy_count')} policies and {zone.get('total_exposure')} DZD exposure."
                        ),
                    )
                )
            for wilaya in monte_carlo.get("overexposed_wilayas", [])[:5]:
                documents.append(
                    KnowledgeDocument(
                        doc_id=f"simulation-wilaya-{wilaya.get('wilaya_code')}",
                        title=f"Simulated overexposed wilaya {wilaya.get('wilaya_name')}",
                        source="Monte Carlo Simulation",
                        tags=["simulation", "wilaya", str(wilaya.get("wilaya_name", "")).lower()],
                        content=(
                            f"{wilaya.get('wilaya_name')} drives {wilaya.get('expected_loss')} DZD of simulated expected loss "
                            f"across {wilaya.get('policy_count')} affected policies."
                        ),
                    )
                )

        return documents

    def _rank_portfolio_documents(
        self,
        query: str,
        documents: list[KnowledgeDocument],
        top_k: int = 4,
    ) -> list[RetrievedDocument]:
        query_terms = self.knowledge_base._tokenize(query)
        scored: list[RetrievedDocument] = []
        for document in documents:
            doc_terms = self.knowledge_base._tokenize(f"{document.title} {document.content} {' '.join(document.tags)}")
            score = self.knowledge_base._hybrid_score(query_terms, doc_terms, document) + 0.15
            scored.append(
                RetrievedDocument(
                    source=document.source,
                    title=document.title,
                    score=round(score, 4),
                    excerpt=document.content[:260],
                    tags=document.tags,
                )
            )
        scored.sort(key=lambda item: item.score, reverse=True)
        return scored[:top_k]

    async def _generate_with_gemini(
        self,
        query: str,
        context: dict,
        recommendations: list[RecommendationItem],
        retrieved: list[RetrievedDocument],
    ) -> dict:
        if not self.gemini_api_key:
            return {"_llm_used": False, "_llm_error": "GEMINI_API_KEY is not configured."}

        prompt = self._build_gemini_prompt(query, context, recommendations, retrieved)
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self.model_name}:generateContent?key={self.gemini_api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json",
            },
        }
        last_error = None
        models_to_try = [self.model_name, *self.model_fallbacks]
        for model_name in models_to_try:
            url = (
                f"https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model_name}:generateContent?key={self.gemini_api_key}"
            )
            for attempt in range(2):
                try:
                    async with httpx.AsyncClient(timeout=40.0) as client:
                        response = await client.post(
                            url,
                            json=payload,
                            headers={"x-goog-api-key": self.gemini_api_key, "Content-Type": "application/json"},
                        )
                        response.raise_for_status()
                    data = response.json()
                    text = data["candidates"][0]["content"]["parts"][0]["text"]
                    parsed = self._parse_gemini_json(text)
                    parsed["_llm_used"] = True
                    parsed["_llm_error"] = None
                    parsed["_llm_model"] = model_name
                    return parsed
                except Exception as exc:
                    last_error = f"{model_name}: {exc}"
                    if "503" in str(exc) and attempt == 0:
                        await asyncio.sleep(1.2)
                        continue
                    break
        return {"_llm_used": False, "_llm_error": last_error}

    def _build_gemini_prompt(
        self,
        query: str,
        context: dict,
        recommendations: list[RecommendationItem],
        retrieved: list[RetrievedDocument],
    ) -> str:
        kpis: PortfolioKPIs = context["kpis"]
        premium_rows: list[PremiumAdequacyRow] = context["premium_adequacy"][:5]
        hotspots: list[HotspotData] = context["hotspots"][:5]
        monte_carlo = context.get("monte_carlo")
        return f"""
You are an expert insurance catastrophe analyst focused on Algerian seismic portfolio risk.
Answer the user question using the provided portfolio data and retrieved knowledge.
Return ONLY valid JSON with this exact structure:
{{
  "answer": "string",
  "executive_summary": "string",
  "confidence": 0.0,
  "recommendations": [
    {{
      "priority": "HIGH|MEDIUM|LOW",
      "category": "Concentration|Tarification|Reinsurance|Prevention|Growth",
      "title": "string",
      "description": "string",
      "action": "string",
      "confidence": 0.0,
      "explanation": "string",
      "rpa_reference": "string or null",
      "evidence": ["string"]
    }}
  ]
}}

User question:
{query}

Portfolio summary:
- Total exposure: {kpis.total_exposure} DZD
- Net retention: {kpis.net_retention} DZD
- Total policies: {kpis.total_policies}
- Executive summary: {context["executive_summary"]}

Zone breakdown:
{json.dumps([item.model_dump(mode="json") for item in kpis.by_zone], default=str, ensure_ascii=True)}

Hotspots:
{json.dumps([item.model_dump(mode="json") for item in hotspots], default=str, ensure_ascii=True)}

Premium adequacy:
{json.dumps([item.model_dump(mode="json") for item in premium_rows], default=str, ensure_ascii=True)}

Top wilayas:
{json.dumps(context["top_wilayas"], default=str, ensure_ascii=True)}

Top risk types:
{json.dumps(context["top_risk_types"], default=str, ensure_ascii=True)}

Monte Carlo simulation:
{json.dumps(monte_carlo, default=str, ensure_ascii=True) if monte_carlo else "null"}

Retrieved knowledge:
{json.dumps([item.model_dump(mode="json") for item in retrieved], ensure_ascii=True)}

Baseline recommendations:
{json.dumps([item.model_dump(mode="json") for item in recommendations], ensure_ascii=True)}
""".strip()

    def _merge_llm_recommendations(
        self,
        llm_recommendations: list[dict] | None,
        fallback: list[RecommendationItem],
    ) -> list[RecommendationItem]:
        if not llm_recommendations:
            return fallback
        merged: list[RecommendationItem] = []
        for item in llm_recommendations:
            try:
                merged.append(RecommendationItem.model_validate(item))
            except Exception:
                continue
        return merged or fallback

    def _parse_gemini_json(self, text: str) -> dict:
        cleaned = text.strip()

        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if match:
            return json.loads(match.group(0))

        raise ValueError("Gemini response did not contain valid JSON.")

    def _rank_recommendations_for_query(
        self,
        recommendations: list[RecommendationItem],
        context: dict,
        user_query: str | None,
    ) -> list[RecommendationItem]:
        if not user_query:
            return recommendations

        intent = self._detect_query_intent(user_query)
        top_hotspot = context["hotspots"][0] if context["hotspots"] else None

        def score(item: RecommendationItem) -> tuple[float, float]:
            base = item.confidence
            if item.priority == "HIGH":
                base += 0.5
            elif item.priority == "MEDIUM":
                base += 0.25

            if intent == "pricing" and item.category == "Tarification":
                base += 1.0
            if intent == "reinsurance" and item.category == "Reinsurance":
                base += 1.0
            if intent == "cost_saving" and item.category in {"Reinsurance", "Concentration"}:
                base += 0.9
            if intent == "cost_saving" and item.category == "Tarification":
                base += 0.35
            if intent in {"concentration", "location", "biggest_risk"} and item.category == "Concentration":
                base += 1.0
            if intent == "biggest_risk" and top_hotspot and top_hotspot.zone_sismique in {"IIb", "III"} and item.category == "Concentration":
                base += 0.4
            return (base, item.confidence)

        return sorted(recommendations, key=score, reverse=True)

    def _detect_query_intent(self, query: str) -> str:
        lowered = query.lower()
        if any(term in lowered for term in ["reinsurance", "reassurance", "cession", "retention"]):
            return "reinsurance"
        if any(term in lowered for term in ["save money", "reduce cost", "cut cost", "econom", "profit", "money"]):
            return "cost_saving"
        if any(term in lowered for term in ["price", "pricing", "prime", "tarif", "underpriced", "sous"]):
            return "pricing"
        if any(term in lowered for term in ["overexposed", "over exposed", "concentration", "hotspot", "where"]):
            return "location"
        if "biggest risk" in lowered or "main risk" in lowered or "principal risk" in lowered:
            return "biggest_risk"
        return "general"
