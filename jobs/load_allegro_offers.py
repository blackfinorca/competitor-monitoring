"""Load allegro_offers CSV into the database.

Upserts rows keyed on (ean, seller): updates price/delivery/scraped_at on re-run.

Usage:
    python jobs/load_allegro_offers.py                        # loads data/allegro_offers.csv
    python jobs/load_allegro_offers.py --input data/test.csv  # custom input
    python jobs/load_allegro_offers.py --dry-run              # show counts without writing
"""

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import text

from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _parse_dt(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def main(input_path: str = "data/allegro_offers.csv", dry_run: bool = False) -> int:
    path = Path(input_path)
    if not path.exists():
        print(f"ERROR: {input_path} not found", file=sys.stderr)
        return 1

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("No rows to load.")
        return 0

    print(f"Loading {len(rows)} rows from {input_path} ...")

    if dry_run:
        from collections import Counter
        by_ean = Counter(r["ean"] for r in rows)
        print(f"Dry run: {len(rows)} rows, {len(by_ean)} unique EANs")
        print("Sample:")
        for r in rows[:5]:
            print(f"  EAN={r['ean']} seller={r['seller']} price={r['price_eur']}")
        return 0

    factory = make_session_factory(Settings())
    with factory() as session:
        inserted = 0
        updated = 0
        for r in rows:
            existing = session.execute(
                text("SELECT id FROM allegro_offers WHERE ean=:ean AND seller=:seller"),
                {"ean": r["ean"], "seller": r["seller"]},
            ).fetchone()

            if existing:
                session.execute(
                    text("""
                        UPDATE allegro_offers
                        SET title=:title, seller_url=:seller_url,
                            price_eur=:price_eur, delivery_eur=:delivery_eur,
                            box_price_eur=:box_price_eur, scraped_at=:scraped_at
                        WHERE ean=:ean AND seller=:seller
                    """),
                    {
                        "ean": r["ean"],
                        "seller": r["seller"],
                        "title": r.get("title"),
                        "seller_url": r.get("seller_url"),
                        "price_eur": _parse_float(r.get("price_eur")),
                        "delivery_eur": _parse_float(r.get("delivery_eur")),
                        "box_price_eur": _parse_float(r.get("box_price_eur")),
                        "scraped_at": _parse_dt(r.get("scraped_at")),
                    },
                )
                updated += 1
            else:
                session.execute(
                    text("""
                        INSERT INTO allegro_offers
                            (ean, title, seller, seller_url, price_eur, delivery_eur, box_price_eur, scraped_at)
                        VALUES
                            (:ean, :title, :seller, :seller_url, :price_eur, :delivery_eur, :box_price_eur, :scraped_at)
                    """),
                    {
                        "ean": r["ean"],
                        "title": r.get("title"),
                        "seller": r["seller"],
                        "seller_url": r.get("seller_url"),
                        "price_eur": _parse_float(r.get("price_eur")),
                        "delivery_eur": _parse_float(r.get("delivery_eur")),
                        "box_price_eur": _parse_float(r.get("box_price_eur")),
                        "scraped_at": _parse_dt(r.get("scraped_at")),
                    },
                )
                inserted += 1

        session.commit()

    print(f"Done. Inserted: {inserted}  Updated: {updated}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/allegro_offers.csv")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    sys.exit(main(args.input, args.dry_run))
