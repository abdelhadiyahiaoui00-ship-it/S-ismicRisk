from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Boolean, Integer, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.base import TimestampMixin


class Commune(TimestampMixin, Base):
    __tablename__ = "communes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    wilaya_code: Mapped[str] = mapped_column(String(2), index=True, nullable=False)
    wilaya_name: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    code_commune: Mapped[str | None] = mapped_column(String(20), index=True, nullable=True)
    commune_name: Mapped[str] = mapped_column(String(120), index=True, nullable=False)
    zone_sismique: Mapped[str] = mapped_column(String(8), index=True, nullable=False)
    zone_num: Mapped[int | None] = mapped_column(Integer, nullable=True)
    zone_source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    lat: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    lon: Mapped[Decimal | None] = mapped_column(Numeric(10, 7), nullable=True)
    coordinate_source: Mapped[str | None] = mapped_column(String(80), nullable=True)
    has_coordinates: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")

