"""Normalise brand names across competitor_listings and products.

Groups all brand values by their normalised form (uppercase + fold diacritics + alias map),
picks the most-common raw variant as the canonical value, and updates every row
that uses a minority variant.

Run:
    python jobs/normalise_brands.py          # dry-run (shows what would change)
    python jobs/normalise_brands.py --apply  # write to DB
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT / ".env", override=False)

from agnaradie_pricing.catalogue.normalise import normalise_brand
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings
from sqlalchemy import text


def _load_brand_counts(session) -> list[tuple[str, int]]:
    rows = session.execute(text("""
        SELECT brand, COUNT(*) AS n
        FROM competitor_listings
        WHERE brand IS NOT NULL AND brand != ''
        GROUP BY brand
        ORDER BY brand
    """)).fetchall()
    return [(r.brand, r.n) for r in rows]


def _build_remap(brand_counts: list[tuple[str, int]]) -> dict[str, str]:
    """Return {raw_brand → canonical_brand} for every non-canonical variant."""
    groups: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for raw, n in brand_counts:
        norm = normalise_brand(raw)
        if norm:
            groups[norm].append((raw, n))

    remap: dict[str, str] = {}
    for variants in groups.values():
        if len(variants) < 2:
            continue
        # Canonical = most common raw variant
        canonical = max(variants, key=lambda x: x[1])[0]
        for raw, _ in variants:
            if raw != canonical:
                remap[raw] = canonical
    return remap


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true",
                        help="Write changes to DB (default: dry-run)")
    args = parser.parse_args()

    factory = make_session_factory(Settings())
    with factory() as session:
        brand_counts = _load_brand_counts(session)
        remap = _build_remap(brand_counts)

        if not remap:
            print("No brand duplicates found — nothing to do.")
            return

        # Count affected listings per mapping
        count_map = {raw: n for raw, n in brand_counts}
        total_rows = sum(count_map.get(raw, 0) for raw in remap)

        print(f"{'DRY RUN — ' if not args.apply else ''}Brand normalisations: {len(remap)} variants → canonical")
        print(f"Affected listings: {total_rows:,}\n")

        for raw, canonical in sorted(remap.items(), key=lambda x: -count_map.get(x[0], 0)):
            n = count_map.get(raw, 0)
            print(f"  {n:>6}  {repr(raw)}  →  {repr(canonical)}")

        if not args.apply:
            print("\nRun with --apply to write changes.")
            return

        print("\nApplying...")
        listings_updated = 0
        products_updated = 0

        for raw, canonical in remap.items():
            r = session.execute(
                text("UPDATE competitor_listings SET brand = :c WHERE brand = :r"),
                {"c": canonical, "r": raw},
            )
            listings_updated += r.rowcount

            r2 = session.execute(
                text("UPDATE products SET brand = :c WHERE brand = :r"),
                {"c": canonical, "r": raw},
            )
            products_updated += r2.rowcount

        session.commit()
        print(f"Done — listings updated: {listings_updated:,}  products updated: {products_updated:,}")


if __name__ == "__main__":
    main()
