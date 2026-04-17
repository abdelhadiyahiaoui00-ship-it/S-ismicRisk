from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import delete, insert

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.db.session import AsyncSessionLocal
from app.models.policy import Policy


def parse_date(value: str) -> datetime.date:
    return datetime.strptime(value.strip(), "%Y-%m-%d").date()


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


def extract_commune_code(value: str) -> str:
    raw = (value or "").strip()
    if " - " in raw:
        return raw.split(" - ", 1)[0].strip()
    return raw or "UNKNOWN"


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


async def import_portfolio(
    enriched_file: Path,
    missing_wilaya_file: Path | None,
    unknown_zone_file: Path | None,
    truncate: bool,
    batch_size: int,
) -> tuple[int, int]:
    rows = load_rows(enriched_file)
    supplemental: list[dict[str, str]] = []
    for path in [missing_wilaya_file, unknown_zone_file]:
        if path is None:
            continue
        supplemental.extend(load_rows(path))

    seen = {
        (
            (row.get("NUMERO_POLICE") or "").strip(),
            (row.get("year") or "").strip(),
            (row.get("TYPE") or "").strip(),
            (row.get("commune_name") or "").strip().upper(),
        )
        for row in rows
    }
    for row in supplemental:
        key = (
            (row.get("NUMERO_POLICE") or "").strip(),
            (row.get("year") or "").strip(),
            (row.get("TYPE") or "").strip(),
            (row.get("commune_name") or "").strip().upper(),
        )
        if key not in seen:
            rows.append(row)
            seen.add(key)

    zone_year_counts: dict[tuple[int, str], int] = defaultdict(int)
    zone_year_exposure: dict[tuple[int, str], Decimal] = defaultdict(lambda: Decimal("0"))
    wilaya_year_counts: dict[tuple[int, str], int] = defaultdict(int)
    wilaya_year_exposure: dict[tuple[int, str], Decimal] = defaultdict(lambda: Decimal("0"))
    wilaya_zone_year_counts: dict[tuple[int, str, str], int] = defaultdict(int)
    wilaya_zone_year_exposure: dict[tuple[int, str, str], Decimal] = defaultdict(lambda: Decimal("0"))

    normalized: list[dict[str, object]] = []
    skipped = 0

    for index, row in enumerate(rows, start=1):
        policy_year = int((row.get("year") or "0").strip() or "0")
        numero_police = (row.get("NUMERO_POLICE") or "").strip()
        type_risque = (row.get("TYPE") or "").strip()
        commune_name = (row.get("commune_name") or "").strip() or "UNKNOWN"
        wilaya_code = (row.get("wilaya_code") or "").strip().zfill(2) or "00"
        wilaya_name = (row.get("wilaya_name") or "").strip() or "UNKNOWN"
        zone_sismique = (row.get("zone_sismique") or "").strip() or "UNKNOWN"
        capital_assure = parse_decimal(row.get("VALEUR_ASSURÉE", "")) or Decimal("0")
        prime_nette = parse_decimal(row.get("PRIME_NETTE", "")) or Decimal("0")

        if not numero_police or not type_risque or not policy_year:
            skipped += 1
            continue

        zone_key = (policy_year, zone_sismique)
        wilaya_key = (policy_year, wilaya_code)
        wilaya_zone_key = (policy_year, wilaya_code, zone_sismique)

        zone_year_counts[zone_key] += 1
        zone_year_exposure[zone_key] += capital_assure
        wilaya_year_counts[wilaya_key] += 1
        wilaya_year_exposure[wilaya_key] += capital_assure
        wilaya_zone_year_counts[wilaya_zone_key] += 1
        wilaya_zone_year_exposure[wilaya_zone_key] += capital_assure

        normalized.append(
            {
                "source_row_number": index,
                "policy_year": policy_year,
                "numero_police": numero_police,
                "date_effet": parse_date(row["DATE_EFFET"]),
                "date_expiration": parse_date(row["DATE_EXPIRATION"]),
                "type_risque": type_risque,
                "code_wilaya": wilaya_code,
                "zone_lookup_code_wilaya": wilaya_code if wilaya_code != "00" else None,
                "wilaya": wilaya_name,
                "source_code_commune": extract_commune_code(row.get("commune_du_risque", "")),
                "code_commune": extract_commune_code(row.get("commune_du_risque", "")),
                "commune": commune_name,
                "zone_sismique": zone_sismique,
                "capital_assure": capital_assure,
                "prime_nette": prime_nette,
                "prime_rate": parse_decimal(row.get("prime_rate", "")),
                "lat": parse_decimal(row.get("lat", "")),
                "lon": parse_decimal(row.get("lon", "")),
                "zone_source": (row.get("zone_source") or "").strip() or None,
                "coordinate_source": (row.get("coordinate_source") or "").strip() or None,
                "zone_match_method": (row.get("zone_match_method") or "").strip() or None,
                "zone_num": parse_int(row.get("zone_num", "")),
                "source_sheet": (row.get("source_sheet") or "").strip() or None,
            }
        )

    for row in normalized:
        zone_key = (row["policy_year"], row["zone_sismique"])
        wilaya_key = (row["policy_year"], row["code_wilaya"])
        wilaya_zone_key = (row["policy_year"], row["code_wilaya"], row["zone_sismique"])
        row["zone_policy_count_year"] = zone_year_counts[zone_key]
        row["zone_capital_assure_total_year"] = zone_year_exposure[zone_key]
        row["wilaya_policy_count_year"] = wilaya_year_counts[wilaya_key]
        row["wilaya_capital_assure_total_year"] = wilaya_year_exposure[wilaya_key]
        row["wilaya_zone_policy_count_year"] = wilaya_zone_year_counts[wilaya_zone_key]
        row["wilaya_zone_capital_assure_total_year"] = wilaya_zone_year_exposure[wilaya_zone_key]

    inserted = 0
    async with AsyncSessionLocal() as session:
        if truncate:
            await session.execute(delete(Policy))
            await session.commit()

        batch: list[dict[str, object]] = []
        for row in normalized:
            batch.append(row)
            if len(batch) >= batch_size:
                await session.execute(insert(Policy), batch)
                await session.commit()
                inserted += len(batch)
                batch.clear()

        if batch:
            await session.execute(insert(Policy), batch)
            await session.commit()
            inserted += len(batch)

    return inserted, skipped


def main() -> None:
    parser = argparse.ArgumentParser(description="Import the enriched portfolio dataset into Postgres.")
    parser.add_argument("enriched_csv", type=Path, help="Path to portfolio_enriched.csv")
    parser.add_argument("--missing-wilaya-file", type=Path, default=None)
    parser.add_argument("--unknown-zone-file", type=Path, default=None)
    parser.add_argument("--no-truncate", action="store_true", help="Append data instead of clearing the table first")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    inserted, skipped = asyncio.run(
        import_portfolio(
            args.enriched_csv,
            args.missing_wilaya_file,
            args.unknown_zone_file,
            truncate=not args.no_truncate,
            batch_size=args.batch_size,
        )
    )
    print(f"Imported {inserted} enriched policy rows from {args.enriched_csv}")
    print(f"Skipped {skipped} malformed rows")


if __name__ == "__main__":
    main()
