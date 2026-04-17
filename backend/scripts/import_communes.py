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


async def import_communes(
    commune_file: Path,
    missing_coordinates_file: Path | None,
    enriched_portfolio_file: Path | None,
) -> int:
    mapping: dict[tuple[str, str], dict[str, object]] = {}

    def merge_row(row: dict[str, str]) -> None:
        key = (row["wilaya_code"].strip().zfill(2), row["commune_name"].strip().upper())
        existing = mapping.get(key, {})
        lat = parse_decimal(row.get("lat", ""))
        lon = parse_decimal(row.get("lon", ""))
        mapping[key] = {
            "wilaya_code": key[0],
            "wilaya_name": row.get("wilaya_name", existing.get("wilaya_name", "")).strip(),
            "code_commune": existing.get("code_commune"),
            "commune_name": row["commune_name"].strip(),
            "zone_sismique": row.get("zone_sismique", existing.get("zone_sismique", "UNKNOWN")).strip(),
            "zone_num": existing.get("zone_num"),
            "zone_source": row.get("zone_source", existing.get("zone_source")),
            "lat": lat if lat is not None else existing.get("lat"),
            "lon": lon if lon is not None else existing.get("lon"),
            "coordinate_source": row.get("coordinate_source", existing.get("coordinate_source")),
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
                key = (wilaya_code, commune_name)
                if key not in mapping:
                    continue
                current = mapping[key]
                current["code_commune"] = row.get("commune_du_risque", "").split(" - ")[0].strip() or current.get("code_commune")
                current["zone_num"] = parse_int(row.get("zone_num", "")) if row.get("zone_num") else current.get("zone_num")
                current["lat"] = parse_decimal(row.get("lat", "")) or current.get("lat")
                current["lon"] = parse_decimal(row.get("lon", "")) or current.get("lon")
                current["coordinate_source"] = row.get("coordinate_source", "") or current.get("coordinate_source")
                current["has_coordinates"] = bool(current.get("lat") is not None and current.get("lon") is not None)

    rows = list(mapping.values())
    async with AsyncSessionLocal() as session:
        await session.execute(delete(Commune))
        await session.commit()
        await session.execute(insert(Commune), rows)
        await session.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Import commune reference data into Postgres.")
    parser.add_argument("commune_file", type=Path)
    parser.add_argument("--missing-coordinates-file", type=Path, default=None)
    parser.add_argument("--enriched-portfolio-file", type=Path, default=None)
    args = parser.parse_args()

    inserted = asyncio.run(
        import_communes(
            args.commune_file,
            args.missing_coordinates_file,
            args.enriched_portfolio_file,
        )
    )
    print(f"Imported {inserted} commune rows")


if __name__ == "__main__":
    main()
