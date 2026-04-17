from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from decimal import Decimal
from pathlib import Path

from sqlalchemy import delete, insert

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.db.session import AsyncSessionLocal
from app.models.commune import Commune
from app.services.algeria_location_reference import get_algeria_location_reference


def parse_decimal(value: str) -> Decimal | None:
    value = (value or "").strip()
    if not value:
        return None
    return Decimal(value)


def parse_int(value: str) -> int | None:
    value = (value or "").strip()
    if not value:
        return None
    return int(value)


def build_commune_rows(
    commune_file: Path,
    missing_coordinates_file: Path | None,
    enriched_portfolio_file: Path | None,
) -> list[dict[str, object]]:
    reference = get_algeria_location_reference()
    mapping: dict[str, dict[str, object]] = {
        item.code_commune: {
            "wilaya_code": item.wilaya_code,
            "wilaya_name": item.wilaya_name,
            "code_commune": item.code_commune,
            "commune_name": item.commune_name,
            "zone_sismique": "UNKNOWN",
            "zone_num": None,
            "zone_source": "canonical_reference",
            "lat": item.lat,
            "lon": item.lon,
            "coordinate_source": "canonical_reference" if item.lat is not None and item.lon is not None else None,
            "has_coordinates": bool(item.lat is not None and item.lon is not None),
        }
        for item in reference.communes
    }

    def merge_row(row: dict[str, str]) -> None:
        lat = parse_decimal(row.get("lat", ""))
        lon = parse_decimal(row.get("lon", ""))
        resolved = reference.resolve(
            row.get("commune_name"),
            wilaya_code=row.get("wilaya_code"),
            wilaya_name=row.get("wilaya_name"),
            lat=lat,
            lon=lon,
        )
        if resolved is None:
            return

        key = resolved.commune.code_commune
        existing = mapping[key]
        lat = parse_decimal(row.get("lat", ""))
        lon = parse_decimal(row.get("lon", ""))
        mapping[key] = {
            "wilaya_code": resolved.commune.wilaya_code,
            "wilaya_name": resolved.commune.wilaya_name,
            "code_commune": existing.get("code_commune") or resolved.commune.code_commune,
            "commune_name": resolved.commune.commune_name,
            "zone_sismique": row.get("zone_sismique", existing.get("zone_sismique", "UNKNOWN")).strip(),
            "zone_num": parse_int(row.get("zone_num", "")) if row.get("zone_num") else existing.get("zone_num"),
            "zone_source": row.get("zone_source", existing.get("zone_source")) or existing.get("zone_source"),
            "lat": existing.get("lat") if existing.get("lat") is not None else lat,
            "lon": existing.get("lon") if existing.get("lon") is not None else lon,
            "coordinate_source": existing.get("coordinate_source")
            or row.get("coordinate_source")
            or f"canonical::{resolved.method}",
            "has_coordinates": lat is not None and lon is not None or bool(existing.get("has_coordinates", False)),
        }

    for path in [commune_file, missing_coordinates_file]:
        if path is None:
            continue
        with path.open(newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            for row in reader:
                merge_row(row)

    if enriched_portfolio_file is not None:
        with enriched_portfolio_file.open(newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            for row in reader:
                wilaya_code = row.get("wilaya_code", "").strip().zfill(2)
                commune_name = row.get("commune_name", "").strip().upper()
                if not wilaya_code or not commune_name:
                    continue
                lat = parse_decimal(row.get("lat", ""))
                lon = parse_decimal(row.get("lon", ""))
                resolved = reference.resolve(
                    row.get("commune_name"),
                    wilaya_code=wilaya_code,
                    wilaya_name=row.get("wilaya_name"),
                    lat=lat,
                    lon=lon,
                    raw_label=row.get("commune_du_risque"),
                )
                if resolved is None:
                    continue
                key = resolved.commune.code_commune
                current = mapping[key]
                current["code_commune"] = current.get("code_commune") or resolved.commune.code_commune
                current["zone_num"] = parse_int(row.get("zone_num", "")) if row.get("zone_num") else current.get("zone_num")
                current["lat"] = current.get("lat") or lat
                current["lon"] = current.get("lon") or lon
                current["coordinate_source"] = current.get("coordinate_source") or row.get("coordinate_source", "")
                current["has_coordinates"] = bool(current.get("lat") is not None and current.get("lon") is not None)

    return list(mapping.values())


async def import_communes(
    commune_file: Path,
    missing_coordinates_file: Path | None,
    enriched_portfolio_file: Path | None,
    batch_size: int = 250,
) -> int:
    rows = build_commune_rows(
        commune_file,
        missing_coordinates_file,
        enriched_portfolio_file,
    )
    async with AsyncSessionLocal() as session:
        await session.execute(delete(Commune))
        await session.commit()
        batch: list[dict[str, object]] = []
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                await session.execute(insert(Commune.__table__), batch)
                await session.commit()
                batch.clear()

        if batch:
            await session.execute(insert(Commune.__table__), batch)
            await session.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import commune reference data into Postgres.")
    parser.add_argument("commune_file", type=Path)
    parser.add_argument("--missing-coordinates-file", type=Path, default=None)
    parser.add_argument("--enriched-portfolio-file", type=Path, default=None)
    parser.add_argument("--batch-size", type=int, default=250)
    args = parser.parse_args()

    inserted = asyncio.run(
        import_communes(
            args.commune_file,
            args.missing_coordinates_file,
            args.enriched_portfolio_file,
            args.batch_size,
        )
    )
    print(f"Imported {inserted} commune rows")


if __name__ == "__main__":
    main()
