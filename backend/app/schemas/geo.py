from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel

MapLayer = Literal["risk", "exposure", "score", "simulation"]


class WilayaBasic(BaseModel):
    code: str
    name: str
    commune_count: int | None = None


class CommuneBasic(BaseModel):
    id: int | None = None
    code: str | None = None
    name: str
    wilaya_code: str
    wilaya_name: str
    zone_sismique: str
    zone_num: int | None = None
    zone_source: str | None = None
    lat: Decimal | None = None
    lon: Decimal | None = None
    coordinate_source: str | None = None
    has_coordinates: bool = False


class ZoneLookupResponse(BaseModel):
    wilaya_code: str
    wilaya_name: str | None = None
    commune_code: str | None = None
    commune: str
    zone: str
    zone_num: int | None = None
    zone_source: str | None = None
    description: str
    lat: Decimal | None = None
    lon: Decimal | None = None
    coordinate_source: str | None = None
    has_coordinates: bool = False


class CommuneMapFeature(BaseModel):
    commune_code: str | None
    commune_name: str
    wilaya_code: str
    wilaya_name: str
    zone_sismique: str
    zone_source: str | None = None
    lat: Decimal | None = None
    lon: Decimal | None = None
    coordinate_source: str | None = None
    has_coordinates: bool = False
    total_exposure: Decimal
    policy_count: int
    avg_risk_score: Decimal
    net_retention: Decimal
    hotspot_score: Decimal
    layer_value: Decimal


class MapDataResponse(BaseModel):
    features: list[CommuneMapFeature]
    last_updated: datetime | None


class HotspotData(BaseModel):
    rank: int
    wilaya_code: str
    wilaya_name: str
    commune_code: str | None
    commune_name: str
    zone_sismique: str
    zone_source: str | None = None
    lat: Decimal | None = None
    lon: Decimal | None = None
    coordinate_source: str | None = None
    has_coordinates: bool = False
    total_exposure: Decimal
    policy_count: int
    hotspot_score: Decimal


class ZoneBreakdown(BaseModel):
    zone: str
    exposure: Decimal
    policy_count: int
    pct: Decimal


class PortfolioKPIs(BaseModel):
    total_exposure: Decimal
    total_policies: int
    net_retention: Decimal
    by_zone: list[ZoneBreakdown]
    top_hotspot: HotspotData | None


class PremiumAdequacyRow(BaseModel):
    zone: str
    type_risque: str
    adequate_rate: Decimal
    observed_rate: Decimal
    premium_gap_pct: Decimal
    policy_count: int
    total_exposure: Decimal

