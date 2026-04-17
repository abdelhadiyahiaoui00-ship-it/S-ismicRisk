from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.schemas.geo import CommuneBasic, WilayaBasic, ZoneLookupResponse
from app.services.geo_service import geo_service

router = APIRouter()


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
