from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.policy import Policy
from app.schemas.geo import CommuneBasic, WilayaBasic, ZoneLookupResponse


class GeoService:
    async def get_wilayas(self, db: AsyncSession) -> list[WilayaBasic]:
        result = await db.execute(
            select(Policy.code_wilaya, Policy.wilaya)
            .distinct()
            .order_by(Policy.code_wilaya.asc(), Policy.wilaya.asc())
        )
        return [WilayaBasic(code=code, name=name) for code, name in result.all()]

    async def get_communes(self, db: AsyncSession, wilaya_code: str) -> list[CommuneBasic]:
        result = await db.execute(
            select(Policy.code_commune, Policy.commune, Policy.zone_sismique)
            .distinct()
            .where(Policy.code_wilaya == wilaya_code)
            .order_by(Policy.commune.asc())
        )
        return [CommuneBasic(code=code, name=name, zone_sismique=zone) for code, name, zone in result.all()]

    async def get_zone(self, db: AsyncSession, wilaya_code: str, commune_name: str) -> ZoneLookupResponse | None:
        result = await db.execute(
            select(Policy.commune, Policy.zone_sismique)
            .where(
                Policy.code_wilaya == wilaya_code,
                func.lower(Policy.commune) == commune_name.strip().lower(),
            )
            .limit(1)
        )
        row = result.first()
        if row is None:
            return None
        commune, zone = row
        return ZoneLookupResponse(wilaya_code=wilaya_code, commune=commune, zone=zone)


geo_service = GeoService()
