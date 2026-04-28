"""Backfill simplified product categories from ToolZone/reference titles."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agnaradie_pricing.catalogue.categories import (
    backfill_competitor_listing_categories,
    backfill_product_categories,
)
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.settings import Settings


def main(
    *,
    source: str = "toolzone_sk",
    dry_run: bool = False,
    include_listings: bool = True,
) -> dict[str, object]:
    factory = make_session_factory(Settings())
    with factory() as session:
        product_result = backfill_product_categories(session, source_competitor_id=source)
        listing_result = (
            backfill_competitor_listing_categories(session)
            if include_listings
            else None
        )
        if dry_run:
            session.rollback()
        else:
            session.commit()
    return {
        "products_seen": product_result.products_seen,
        "products_updated": product_result.products_updated,
        "product_category_counts": product_result.category_counts,
        "listings_seen": listing_result.listings_seen if listing_result else 0,
        "listings_updated": listing_result.listings_updated if listing_result else 0,
        "listing_category_counts": listing_result.category_counts if listing_result else {},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default="toolzone_sk",
        help="Competitor listing source to prefer by matching EAN.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify and print counts without committing DB changes.",
    )
    parser.add_argument(
        "--products-only",
        action="store_true",
        help="Only update products.category, leaving competitor listings unchanged.",
    )
    args = parser.parse_args()

    outcome = main(
        source=args.source,
        dry_run=args.dry_run,
        include_listings=not args.products_only,
    )
    print(f"Products seen: {outcome['products_seen']}")
    print(f"Products updated: {outcome['products_updated']}")
    print("Product category counts:")
    for category, count in sorted(
        outcome["product_category_counts"].items(),
        key=lambda item: (-item[1], item[0]),
    ):
        print(f"  {category}: {count}")
    print(f"Listings seen: {outcome['listings_seen']}")
    print(f"Listings updated: {outcome['listings_updated']}")
    print("Listing category counts:")
    for category, count in sorted(
        outcome["listing_category_counts"].items(),
        key=lambda item: (-item[1], item[0]),
    ):
        print(f"  {category}: {count}")
