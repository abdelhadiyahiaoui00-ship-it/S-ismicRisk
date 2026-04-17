from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.schemas.geo import (
    CommuneBasic,
    HotspotData,
    MapDataResponse,
    MapLayer,
    PortfolioKPIs,
    PremiumAdequacyRow,
    WilayaBasic,
    ZoneLookupResponse,
)
from app.services.geo_service import geo_service

router = APIRouter()


@router.get("/map-data", response_model=MapDataResponse)
async def get_map_data(
    layer: MapLayer = Query(default="risk"),
    db: AsyncSession = Depends(get_db),
) -> MapDataResponse:
    return await geo_service.get_map_data(db, layer)


@router.get("/hotspots", response_model=list[HotspotData])
async def get_hotspots(
    top_n: int = Query(default=10, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
) -> list[HotspotData]:
    return await geo_service.get_hotspots(db, top_n)


@router.get("/kpis", response_model=PortfolioKPIs)
async def get_kpis(db: AsyncSession = Depends(get_db)) -> PortfolioKPIs:
    return await geo_service.get_portfolio_kpis(db)


@router.get("/wilayas", response_model=list[WilayaBasic])
async def get_wilayas(db: AsyncSession = Depends(get_db)) -> list[WilayaBasic]:
    return await geo_service.get_wilayas(db)


@router.get("/wilayas/{wilaya_code}/communes", response_model=list[CommuneBasic])
async def get_communes(wilaya_code: str, db: AsyncSession = Depends(get_db)) -> list[CommuneBasic]:
    return await geo_service.get_communes(db, wilaya_code)


@router.get("/zone/{wilaya_code}/{commune_name}", response_model=ZoneLookupResponse)
async def get_zone(wilaya_code: str, commune_name: str, db: AsyncSession = Depends(get_db)) -> ZoneLookupResponse:
    zone = await geo_service.get_zone(db, wilaya_code, commune_name)
    if zone is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Commune not found for wilaya")
    return zone


@router.get("/premium-adequacy", response_model=list[PremiumAdequacyRow])
async def get_premium_adequacy(db: AsyncSession = Depends(get_db)) -> list[PremiumAdequacyRow]:
    return await geo_service.get_premium_adequacy(db)
