from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Select, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.policy import Policy
from app.schemas.policy import PolicyListResponse, PortfolioSummary


class PolicyService:
    async def list_policies(
        self,
        db: AsyncSession,
        *,
        page: int = 1,
        size: int = 50,
        policy_year: int | None = None,
        code_wilaya: str | None = None,
        zone_sismique: str | None = None,
        type_risque: str | None = None,
        commune: str | None = None,
        search: str | None = None,
    ) -> PolicyListResponse:
        query: Select[tuple[Policy]] = select(Policy)
        query = self._apply_filters(
            query,
            policy_year=policy_year,
            code_wilaya=code_wilaya,
            zone_sismique=zone_sismique,
            type_risque=type_risque,
            commune=commune,
            search=search,
        )

        total_query = select(func.count()).select_from(query.subquery())
        total = (await db.execute(total_query)).scalar_one()

        result = await db.execute(
            query.order_by(Policy.policy_year.desc(), Policy.id.asc())
            .offset((page - 1) * size)
            .limit(size)
        )
        items = result.scalars().all()
        return PolicyListResponse(items=items, total=total, page=page, size=size)

    async def get_policy(self, db: AsyncSession, policy_id: int) -> Policy | None:
        result = await db.execute(select(Policy).where(Policy.id == policy_id))
        return result.scalar_one_or_none()

    async def get_summary(self, db: AsyncSession) -> PortfolioSummary:
        totals = await db.execute(
            select(
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
                func.coalesce(func.sum(Policy.prime_nette), 0),
            )
        )
        total_policies, total_capital_assure, total_prime_nette = totals.one()

        zone_result = await db.execute(
            select(
                Policy.zone_sismique,
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
                func.coalesce(func.sum(Policy.prime_nette), 0),
            )
            .group_by(Policy.zone_sismique)
            .order_by(Policy.zone_sismique.asc())
        )
        by_zone = [
            {
                "zone": zone,
                "policy_count": policy_count,
                "capital_assure": capital_assure,
                "prime_nette": prime_nette,
            }
            for zone, policy_count, capital_assure, prime_nette in zone_result.all()
        ]

        year_result = await db.execute(
            select(
                Policy.policy_year,
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
                func.coalesce(func.sum(Policy.prime_nette), 0),
            )
            .group_by(Policy.policy_year)
            .order_by(Policy.policy_year.desc())
        )
        by_year = [
            {
                "policy_year": year,
                "policy_count": policy_count,
                "capital_assure": capital_assure,
                "prime_nette": prime_nette,
            }
            for year, policy_count, capital_assure, prime_nette in year_result.all()
        ]

        return PortfolioSummary(
            total_policies=total_policies,
            total_capital_assure=Decimal(total_capital_assure),
            total_prime_nette=Decimal(total_prime_nette),
            by_zone=by_zone,
            by_year=by_year,
        )

    def _apply_filters(
        self,
        query: Select[tuple[Policy]],
        *,
        policy_year: int | None,
        code_wilaya: str | None,
        zone_sismique: str | None,
        type_risque: str | None,
        commune: str | None,
        search: str | None,
    ) -> Select[tuple[Policy]]:
        if policy_year is not None:
            query = query.where(Policy.policy_year == policy_year)
        if code_wilaya:
            query = query.where(Policy.code_wilaya == code_wilaya)
        if zone_sismique:
            query = query.where(Policy.zone_sismique == zone_sismique)
        if type_risque:
            query = query.where(Policy.type_risque == type_risque)
        if commune:
            query = query.where(func.lower(Policy.commune) == commune.strip().lower())
        if search:
            term = f"%{search.strip()}%"
            query = query.where(
                or_(
                    Policy.numero_police.ilike(term),
                    Policy.wilaya.ilike(term),
                    Policy.commune.ilike(term),
                    Policy.type_risque.ilike(term),
                )
            )
        return query


policy_service = PolicyService()
