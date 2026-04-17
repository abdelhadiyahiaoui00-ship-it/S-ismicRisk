from __future__ import annotations

from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.base import TimestampMixin


class Policy(TimestampMixin, Base):
    __tablename__ = "policies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_row_number: Mapped[int] = mapped_column(Integer, unique=True, index=True, nullable=False)

    policy_year: Mapped[int] = mapped_column(Integer, index=True, nullable=False)
    numero_police: Mapped[str] = mapped_column(String(50), index=True, nullable=False)
    date_effet: Mapped[date] = mapped_column(Date, nullable=False)
    date_expiration: Mapped[date] = mapped_column(Date, nullable=False)

    type_risque: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    code_wilaya: Mapped[str] = mapped_column(String(2), index=True, nullable=False)
    zone_lookup_code_wilaya: Mapped[str] = mapped_column(String(2), nullable=True)
    wilaya: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    source_code_commune: Mapped[str] = mapped_column(String(20), nullable=True)
    code_commune: Mapped[str] = mapped_column(String(20), index=True, nullable=False)
    commune: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    zone_sismique: Mapped[str] = mapped_column(String(4), index=True, nullable=False)

    capital_assure: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    prime_nette: Mapped[Decimal] = mapped_column(Numeric(18, 3), nullable=False)
    prime_rate: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    lat: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    lon: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    zone_source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    coordinate_source: Mapped[str | None] = mapped_column(String(80), nullable=True)
    zone_match_method: Mapped[str | None] = mapped_column(String(50), nullable=True)
    zone_num: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_sheet: Mapped[str | None] = mapped_column(String(20), nullable=True)

    zone_policy_count_year: Mapped[int] = mapped_column(Integer, nullable=True)
    zone_capital_assure_total_year: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=True)
    wilaya_policy_count_year: Mapped[int] = mapped_column(Integer, nullable=True)
    wilaya_capital_assure_total_year: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=True)
    wilaya_zone_policy_count_year: Mapped[int] = mapped_column(Integer, nullable=True)
    wilaya_zone_capital_assure_total_year: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=True)
