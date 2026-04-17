from __future__ import annotations

import argparse
import asyncio
import csv
import sys
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
    return datetime.strptime(value, "%d/%m/%Y").date()


def parse_decimal(value: str) -> Decimal:
    return Decimal(value.strip() or "0")


def parse_int(value: str) -> int:
    return int(value.strip() or "0")


async def import_dataset(path: Path, truncate: bool, batch_size: int) -> int:
    inserted = 0
    async with AsyncSessionLocal() as session:
        if truncate:
            await session.execute(delete(Policy))
            await session.commit()

        batch: list[dict[str, object]] = []
        with path.open(newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            for index, row in enumerate(reader, start=1):
                batch.append(
                    {
                        "source_row_number": index,
                        "policy_year": parse_int(row["policy_year"]),
                        "numero_police": row["numero_police"].strip(),
                        "date_effet": parse_date(row["date_effet"]),
                        "date_expiration": parse_date(row["date_expiration"]),
                        "type_risque": row["type"].strip(),
                        "code_wilaya": row["code_wilaya"].strip().zfill(2),
                        "zone_lookup_code_wilaya": row["zone_lookup_code_wilaya"].strip().zfill(2),
                        "wilaya": row["wilaya"].strip(),
                        "source_code_commune": row["source_code_commune"].strip(),
                        "code_commune": row["code_commune"].strip(),
                        "commune": row["commune"].strip(),
                        "zone_sismique": row["zone_sismique"].strip(),
                        "capital_assure": parse_decimal(row["capital_assure"]),
                        "prime_nette": parse_decimal(row["prime_nette"]),
                        "zone_policy_count_year": parse_int(row["zone_policy_count_year"]),
                        "zone_capital_assure_total_year": parse_decimal(row["zone_capital_assure_total_year"]),
                        "wilaya_policy_count_year": parse_int(row["wilaya_policy_count_year"]),
                        "wilaya_capital_assure_total_year": parse_decimal(row["wilaya_capital_assure_total_year"]),
                        "wilaya_zone_policy_count_year": parse_int(row["wilaya_zone_policy_count_year"]),
                        "wilaya_zone_capital_assure_total_year": parse_decimal(row["wilaya_zone_capital_assure_total_year"]),
                    }
                )
                if len(batch) >= batch_size:
                    await session.execute(insert(Policy), batch)
                    await session.commit()
                    inserted += len(batch)
                    batch.clear()

        if batch:
            await session.execute(insert(Policy), batch)
            await session.commit()
            inserted += len(batch)

    return inserted


def main() -> None:
    parser = argparse.ArgumentParser(description="Import the all-in-one portfolio dataset into Postgres.")
    parser.add_argument("csv_path", type=Path, help="Path to all_in_one_dataset.csv")
    parser.add_argument("--no-truncate", action="store_true", help="Append data instead of clearing the table first")
    parser.add_argument("--batch-size", type=int, default=1000)
    args = parser.parse_args()

    inserted = asyncio.run(import_dataset(args.csv_path, truncate=not args.no_truncate, batch_size=args.batch_size))
    print(f"Imported {inserted} rows from {args.csv_path}")


if __name__ == "__main__":
    main()
