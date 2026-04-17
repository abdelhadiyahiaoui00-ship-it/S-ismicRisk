from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict


class PolicyRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source_row_number: int
    policy_year: int
    numero_police: str
    date_effet: date
    date_expiration: date
    type_risque: str
    code_wilaya: str
    zone_lookup_code_wilaya: str | None = None
    wilaya: str
    source_code_commune: str | None = None
    code_commune: str
    commune: str
    zone_sismique: str
    capital_assure: Decimal
    prime_nette: Decimal
    prime_rate: Decimal | None = None
    lat: Decimal | None = None
    lon: Decimal | None = None
    zone_source: str | None = None
    coordinate_source: str | None = None
    zone_match_method: str | None = None
    zone_num: int | None = None
    source_sheet: str | None = None
    zone_policy_count_year: int | None = None
    zone_capital_assure_total_year: Decimal | None = None
    wilaya_policy_count_year: int | None = None
    wilaya_capital_assure_total_year: Decimal | None = None
    wilaya_zone_policy_count_year: int | None = None
    wilaya_zone_capital_assure_total_year: Decimal | None = None


class PolicyListResponse(BaseModel):
    items: list[PolicyRead]
    total: int
    page: int
    size: int


class PortfolioSummary(BaseModel):
    total_policies: int
    total_capital_assure: Decimal
    total_prime_nette: Decimal
    by_zone: list[dict[str, Decimal | int | str]]
    by_year: list[dict[str, Decimal | int]]
