from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.commune import Commune
from app.models.policy import Policy
from app.schemas.geo import (
    CommuneBasic,
    CommuneMapFeature,
    HotspotData,
    MapDataResponse,
    MapLayer,
    PortfolioKPIs,
    PremiumAdequacyRow,
    WilayaBasic,
    ZoneBreakdown,
    ZoneLookupResponse,
)

ZONE_DESCRIPTIONS = {
    "0": "Zone de sismicite negligeable.",
    "I": "Zone de faible sismicite.",
    "IIa": "Zone de sismicite moderee.",
    "IIb": "Zone de sismicite moyenne a elevee.",
    "III": "Zone de forte sismicite.",
}

ZONE_WEIGHTS = {
    "0": Decimal("0.40"),
    "I": Decimal("0.75"),
    "IIa": Decimal("1.10"),
    "IIb": Decimal("1.45"),
    "III": Decimal("1.90"),
}

ZONE_SCORE_PROXY = {
    "0": Decimal("10"),
    "I": Decimal("25"),
    "IIa": Decimal("45"),
    "IIb": Decimal("65"),
    "III": Decimal("85"),
}

ZONE_BASE_RATES = {
    "0": Decimal("0.00045"),
    "I": Decimal("0.00075"),
    "IIa": Decimal("0.00120"),
    "IIb": Decimal("0.00180"),
    "III": Decimal("0.00250"),
}

TYPE_RATE_MULTIPLIERS = {
    "bien immobilier": Decimal("1.00"),
    "installation commerciale": Decimal("1.10"),
    "installation industrielle": Decimal("1.25"),
}


def _quantize(value: Decimal, precision: str = "0.01") -> Decimal:
    return value.quantize(Decimal(precision), rounding=ROUND_HALF_UP)


class GeoService:
    async def list_communes(
        self,
        db: AsyncSession,
        *,
        wilaya_code: str | None = None,
        zone_sismique: str | None = None,
        has_coordinates: bool | None = None,
    ) -> list[CommuneBasic]:
        query = select(Commune)
        if wilaya_code:
            query = query.where(Commune.wilaya_code == wilaya_code)
        if zone_sismique:
            query = query.where(Commune.zone_sismique == zone_sismique)
        if has_coordinates is not None:
            query = query.where(Commune.has_coordinates.is_(has_coordinates))
        query = query.order_by(Commune.wilaya_code.asc(), Commune.commune_name.asc())
        result = await db.execute(query)
        return [self._to_commune_basic(commune) for commune in result.scalars().all()]

    async def get_wilayas(self, db: AsyncSession) -> list[WilayaBasic]:
        result = await db.execute(
            select(
                Commune.wilaya_code,
                Commune.wilaya_name,
                func.count(Commune.id),
            )
            .group_by(Commune.wilaya_code, Commune.wilaya_name)
            .order_by(Commune.wilaya_code.asc(), Commune.wilaya_name.asc())
        )
        rows = result.all()
        if rows:
            return [WilayaBasic(code=code, name=name, commune_count=commune_count) for code, name, commune_count in rows]

        fallback = await db.execute(
            select(Policy.code_wilaya, Policy.wilaya)
            .distinct()
            .order_by(Policy.code_wilaya.asc(), Policy.wilaya.asc())
        )
        return [WilayaBasic(code=code, name=name, commune_count=None) for code, name in fallback.all()]

    async def get_communes(self, db: AsyncSession, wilaya_code: str) -> list[CommuneBasic]:
        result = await db.execute(
            select(Commune)
            .where(Commune.wilaya_code == wilaya_code)
            .order_by(Commune.commune_name.asc())
        )
        communes = result.scalars().all()
        if communes:
            return [self._to_commune_basic(commune) for commune in communes]

        fallback = await db.execute(
            select(Policy.code_commune, Policy.commune, Policy.zone_sismique, Policy.wilaya, Policy.code_wilaya)
            .distinct()
            .where(Policy.code_wilaya == wilaya_code)
            .order_by(Policy.commune.asc())
        )
        return [
            CommuneBasic(
                code=code,
                name=name,
                wilaya_code=code_w,
                wilaya_name=wilaya_name,
                zone_sismique=zone,
                has_coordinates=False,
            )
            for code, name, zone, wilaya_name, code_w in fallback.all()
        ]

    async def get_commune_detail(self, db: AsyncSession, wilaya_code: str, commune_name: str) -> CommuneBasic | None:
        result = await db.execute(
            select(Commune)
            .where(
                Commune.wilaya_code == wilaya_code,
                func.lower(Commune.commune_name) == commune_name.strip().lower(),
            )
            .limit(1)
        )
        commune = result.scalar_one_or_none()
        if commune:
            return self._to_commune_basic(commune)

        zone = await self.get_zone(db, wilaya_code, commune_name)
        if zone is None:
            return None
        return CommuneBasic(
            code=zone.commune_code,
            name=zone.commune,
            wilaya_code=zone.wilaya_code,
            wilaya_name=zone.wilaya_name or "",
            zone_sismique=zone.zone,
            zone_num=zone.zone_num,
            zone_source=zone.zone_source,
            lat=zone.lat,
            lon=zone.lon,
            coordinate_source=zone.coordinate_source,
            has_coordinates=zone.has_coordinates,
        )

    async def get_zone(self, db: AsyncSession, wilaya_code: str, commune_name: str) -> ZoneLookupResponse | None:
        commune_result = await db.execute(
            select(Commune)
            .where(
                Commune.wilaya_code == wilaya_code,
                func.lower(Commune.commune_name) == commune_name.strip().lower(),
            )
            .limit(1)
        )
        commune = commune_result.scalar_one_or_none()
        if commune is not None:
            return ZoneLookupResponse(
                wilaya_code=commune.wilaya_code,
                wilaya_name=commune.wilaya_name,
                commune_code=commune.code_commune,
                commune=commune.commune_name,
                zone=commune.zone_sismique,
                zone_num=commune.zone_num,
                zone_source=commune.zone_source,
                description=ZONE_DESCRIPTIONS.get(commune.zone_sismique, "Zone sismique non documentee."),
                lat=commune.lat,
                lon=commune.lon,
                coordinate_source=commune.coordinate_source,
                has_coordinates=commune.has_coordinates,
            )

        result = await db.execute(
            select(Policy.commune, Policy.zone_sismique, Policy.code_commune, Policy.wilaya)
            .where(
                Policy.code_wilaya == wilaya_code,
                func.lower(Policy.commune) == commune_name.strip().lower(),
            )
            .limit(1)
        )
        row = result.first()
        if row is None:
            return None
        commune, zone, commune_code, wilaya_name = row
        return ZoneLookupResponse(
            wilaya_code=wilaya_code,
            wilaya_name=wilaya_name,
            commune_code=commune_code,
            commune=commune,
            zone=zone,
            zone_num=None,
            zone_source=None,
            description=ZONE_DESCRIPTIONS.get(zone, "Zone sismique non documentee."),
            lat=None,
            lon=None,
            coordinate_source=None,
            has_coordinates=False,
        )

    async def get_map_data(self, db: AsyncSession, layer: MapLayer) -> MapDataResponse:
        last_updated = (await db.execute(select(func.max(Policy.updated_at)))).scalar_one_or_none()
        total_exposure = Decimal((await db.execute(select(func.coalesce(func.sum(Policy.capital_assure), 0)))).scalar_one())
        features: list[CommuneMapFeature] = []

        commune_count = (await db.execute(select(func.count(Commune.id)))).scalar_one()
        if commune_count:
            policy_agg = (
                select(
                    Policy.code_wilaya.label("wilaya_code"),
                    func.lower(Policy.commune).label("commune_key"),
                    func.coalesce(func.max(Policy.code_commune), "").label("code_commune"),
                    func.coalesce(func.max(Policy.wilaya), "").label("wilaya_name"),
                    func.coalesce(func.count(Policy.id), 0).label("policy_count"),
                    func.coalesce(func.sum(Policy.capital_assure), 0).label("total_exposure"),
                )
                .group_by(Policy.code_wilaya, func.lower(Policy.commune))
                .subquery()
            )

            result = await db.execute(
                select(
                    Commune.code_commune,
                    Commune.commune_name,
                    Commune.wilaya_code,
                    Commune.wilaya_name,
                    Commune.zone_sismique,
                    Commune.zone_source,
                    Commune.lat,
                    Commune.lon,
                    Commune.coordinate_source,
                    Commune.has_coordinates,
                    func.coalesce(policy_agg.c.policy_count, 0),
                    func.coalesce(policy_agg.c.total_exposure, 0),
                )
                .select_from(Commune)
                .outerjoin(
                    policy_agg,
                    (policy_agg.c.wilaya_code == Commune.wilaya_code)
                    & (policy_agg.c.commune_key == func.lower(Commune.commune_name)),
                )
                .order_by(Commune.wilaya_code.asc(), Commune.commune_name.asc())
            )
            rows = result.all()
        else:
            result = await db.execute(
                select(
                    Policy.code_commune,
                    Policy.commune,
                    Policy.code_wilaya,
                    Policy.wilaya,
                    Policy.zone_sismique,
                    Policy.zone_source,
                    Policy.lat,
                    Policy.lon,
                    Policy.coordinate_source,
                    case((Policy.lat.is_not(None) & Policy.lon.is_not(None), True), else_=False),
                    func.count(Policy.id),
                    func.coalesce(func.sum(Policy.capital_assure), 0),
                )
                .group_by(
                    Policy.code_commune,
                    Policy.commune,
                    Policy.code_wilaya,
                    Policy.wilaya,
                    Policy.zone_sismique,
                    Policy.zone_source,
                    Policy.lat,
                    Policy.lon,
                    Policy.coordinate_source,
                )
                .order_by(Policy.code_wilaya.asc(), Policy.commune.asc())
            )
            rows = result.all()

        for (
            commune_code,
            commune_name,
            wilaya_code,
            wilaya_name,
            zone,
            zone_source,
            lat,
            lon,
            coordinate_source,
            has_coordinates,
            policy_count,
            exposure,
        ) in rows:
            exposure_dec = Decimal(exposure)
            zone_weight = ZONE_WEIGHTS.get(zone, Decimal("1.00"))
            score_proxy = ZONE_SCORE_PROXY.get(zone, Decimal("50"))
            hotspot_score = Decimal("0")
            if total_exposure > 0:
                hotspot_score = (exposure_dec / total_exposure) * Decimal("100") * zone_weight

            layer_value = {
                "risk": score_proxy,
                "exposure": exposure_dec,
                "score": score_proxy,
                "simulation": Decimal("0"),
            }[layer]

            features.append(
                CommuneMapFeature(
                    commune_code=commune_code,
                    commune_name=commune_name,
                    wilaya_code=wilaya_code,
                    wilaya_name=wilaya_name,
                    zone_sismique=zone,
                    zone_source=zone_source,
                    lat=lat,
                    lon=lon,
                    coordinate_source=coordinate_source,
                    has_coordinates=bool(has_coordinates),
                    total_exposure=_quantize(exposure_dec),
                    policy_count=policy_count,
                    avg_risk_score=_quantize(score_proxy),
                    net_retention=_quantize(exposure_dec * Decimal(str(settings.retention_rate))),
                    hotspot_score=_quantize(hotspot_score),
                    layer_value=_quantize(layer_value),
                )
            )

        return MapDataResponse(features=features, last_updated=last_updated)

    async def get_hotspots(self, db: AsyncSession, top_n: int = 10) -> list[HotspotData]:
        map_data = await self.get_map_data(db, "exposure")
        ordered = sorted(map_data.features, key=lambda item: item.hotspot_score, reverse=True)[:top_n]
        return [
            HotspotData(
                rank=index,
                wilaya_code=item.wilaya_code,
                wilaya_name=item.wilaya_name,
                commune_code=item.commune_code,
                commune_name=item.commune_name,
                zone_sismique=item.zone_sismique,
                zone_source=item.zone_source,
                lat=item.lat,
                lon=item.lon,
                coordinate_source=item.coordinate_source,
                has_coordinates=item.has_coordinates,
                total_exposure=item.total_exposure,
                policy_count=item.policy_count,
                hotspot_score=item.hotspot_score,
            )
            for index, item in enumerate(ordered, start=1)
        ]

    async def get_portfolio_kpis(self, db: AsyncSession) -> PortfolioKPIs:
        totals = await db.execute(
            select(
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
            )
        )
        total_policies, total_exposure = totals.one()
        total_exposure_dec = Decimal(total_exposure)

        zone_result = await db.execute(
            select(
                Policy.zone_sismique,
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
            )
            .group_by(Policy.zone_sismique)
            .order_by(
                case(
                    (Policy.zone_sismique == "0", 0),
                    (Policy.zone_sismique == "I", 1),
                    (Policy.zone_sismique == "IIa", 2),
                    (Policy.zone_sismique == "IIb", 3),
                    (Policy.zone_sismique == "III", 4),
                    else_=99,
                )
            )
        )

        by_zone: list[ZoneBreakdown] = []
        for zone, policy_count, exposure in zone_result.all():
            exposure_dec = Decimal(exposure)
            pct = Decimal("0")
            if total_exposure_dec > 0:
                pct = (exposure_dec / total_exposure_dec) * Decimal("100")
            by_zone.append(
                ZoneBreakdown(
                    zone=zone,
                    exposure=_quantize(exposure_dec),
                    policy_count=policy_count,
                    pct=_quantize(pct),
                )
            )

        hotspots = await self.get_hotspots(db, top_n=1)
        return PortfolioKPIs(
            total_exposure=_quantize(total_exposure_dec),
            total_policies=total_policies,
            net_retention=_quantize(total_exposure_dec * Decimal(str(settings.retention_rate))),
            by_zone=by_zone,
            top_hotspot=hotspots[0] if hotspots else None,
        )

    async def get_premium_adequacy(self, db: AsyncSession) -> list[PremiumAdequacyRow]:
        observed_rate = func.coalesce(func.sum(Policy.prime_nette) / func.nullif(func.sum(Policy.capital_assure), 0), 0)
        result = await db.execute(
            select(
                Policy.zone_sismique,
                Policy.type_risque,
                func.count(Policy.id),
                func.coalesce(func.sum(Policy.capital_assure), 0),
                observed_rate,
            )
            .group_by(Policy.zone_sismique, Policy.type_risque)
            .order_by(Policy.zone_sismique.asc(), Policy.type_risque.asc())
        )

        rows: list[PremiumAdequacyRow] = []
        for zone, type_risque, policy_count, total_exposure, observed in result.all():
            adequate_rate = self._get_adequate_rate(zone, type_risque)
            observed_dec = Decimal(observed)
            gap_pct = Decimal("0")
            if adequate_rate > 0:
                gap_pct = ((observed_dec - adequate_rate) / adequate_rate) * Decimal("100")
            rows.append(
                PremiumAdequacyRow(
                    zone=zone,
                    type_risque=type_risque,
                    adequate_rate=_quantize(adequate_rate, "0.000001"),
                    observed_rate=_quantize(observed_dec, "0.000001"),
                    premium_gap_pct=_quantize(gap_pct),
                    policy_count=policy_count,
                    total_exposure=_quantize(Decimal(total_exposure)),
                )
            )
        return rows

    def _get_adequate_rate(self, zone: str, type_risque: str) -> Decimal:
        base_rate = ZONE_BASE_RATES.get(zone, Decimal("0.00100"))
        lowered = type_risque.lower()
        multiplier = Decimal("1.15")
        for label, value in TYPE_RATE_MULTIPLIERS.items():
            if label in lowered:
                multiplier = value
                break
        return base_rate * multiplier

    def _to_commune_basic(self, commune: Commune) -> CommuneBasic:
        return CommuneBasic(
            id=commune.id,
            code=commune.code_commune,
            name=commune.commune_name,
            wilaya_code=commune.wilaya_code,
            wilaya_name=commune.wilaya_name,
            zone_sismique=commune.zone_sismique,
            zone_num=commune.zone_num,
            zone_source=commune.zone_source,
            lat=commune.lat,
            lon=commune.lon,
            coordinate_source=commune.coordinate_source,
            has_coordinates=commune.has_coordinates,
        )


geo_service = GeoService()
