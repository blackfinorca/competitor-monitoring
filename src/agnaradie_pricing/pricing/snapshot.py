"""Build pricing snapshots from matched competitor listings."""

import statistics
from datetime import date, datetime, UTC
from decimal import Decimal

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import (
    CompetitorListing,
    Product,
    ProductMatch,
    PricingSnapshot,
)
from agnaradie_pricing.settings import own_store_ids


def build_snapshots(session: Session, snapshot_date: date | None = None) -> int:
    """Compute and upsert PricingSnapshot rows for all matched products.

    ToolZone is the baseline store. Its latest scraped listing price is used as
    the "own price" (ag_price). The other competitors are measured against it.

    Returns the number of rows written.
    """
    today = snapshot_date or datetime.now(UTC).date()
    own_stores = own_store_ids()
    written = 0

    # All products that have at least one confirmed match
    matched_product_ids = (
        select(ProductMatch.ag_product_id)
        .where(ProductMatch.ag_product_id.is_not(None))
        .distinct()
    )

    products = session.scalars(
        select(Product).where(Product.id.in_(matched_product_ids))
    ).all()

    for product in products:
        # ToolZone price = our baseline (own store listing, most recent scrape)
        own_prices = _latest_prices_for_product(
            session, product.id, today, include_only=own_stores
        )
        # Fall back to the catalogue price if ToolZone hasn't been scraped yet
        if own_prices:
            toolzone_price = Decimal(str(next(iter(own_prices.values()))))
        else:
            toolzone_price = product.price_eur

        # Competitor prices — own stores excluded from the benchmark
        prices_by_competitor = _latest_prices_for_product(
            session, product.id, today, exclude_competitors=own_stores
        )
        if not prices_by_competitor:
            continue

        all_prices = list(prices_by_competitor.values())
        ag_price = toolzone_price

        min_price = Decimal(str(min(all_prices)))
        max_price = Decimal(str(max(all_prices)))
        median_price = Decimal(str(statistics.median(all_prices)))
        cheapest = min(prices_by_competitor, key=prices_by_competitor.__getitem__)

        # AG rank: 1 = cheapest overall
        if ag_price is not None:
            sorted_prices = sorted(all_prices + [float(ag_price)])
            ag_rank = sorted_prices.index(float(ag_price)) + 1
        else:
            ag_rank = None

        # Upsert
        existing = session.scalars(
            select(PricingSnapshot).where(
                PricingSnapshot.ag_product_id == product.id,
                PricingSnapshot.snapshot_date == today,
            )
        ).first()

        if existing is None:
            existing = PricingSnapshot(
                ag_product_id=product.id,
                snapshot_date=today,
            )
            session.add(existing)

        existing.ag_price = ag_price
        existing.competitor_count = len(prices_by_competitor)
        existing.min_price = min_price
        existing.median_price = median_price
        existing.max_price = max_price
        existing.ag_rank = ag_rank
        existing.cheapest_competitor = cheapest
        written += 1

    return written


def _latest_prices_for_product(
    session: Session,
    product_id: int,
    today: date,
    exclude_competitors: frozenset[str] = frozenset(),
    include_only: frozenset[str] | None = None,
) -> dict[str, float]:
    """Return {competitor_id: price_eur} for the most recent listing per store.

    exclude_competitors — skip these competitor IDs (used to drop own stores from
                          the benchmark calculation).
    include_only        — when set, return only these competitor IDs (used to
                          retrieve the ToolZone baseline price in isolation).
    """
    matches = session.scalars(
        select(ProductMatch).where(
            ProductMatch.ag_product_id == product_id,
            ProductMatch.confidence >= Decimal("0.85"),
        )
    ).all()

    result: dict[str, float] = {}
    for match in matches:
        if include_only is not None and match.competitor_id not in include_only:
            continue
        if match.competitor_id in exclude_competitors:
            continue
        # Find the most recent listing from this competitor with a matching SKU (if known)
        # or any listing with same brand+mpn
        listing_q = (
            select(CompetitorListing)
            .where(CompetitorListing.competitor_id == match.competitor_id)
            .order_by(CompetitorListing.scraped_at.desc())
            .limit(1)
        )
        if match.competitor_sku:
            listing_q = listing_q.where(
                CompetitorListing.competitor_sku == match.competitor_sku
            )

        listing = session.scalars(listing_q).first()
        if listing is not None:
            result[match.competitor_id] = float(listing.price_eur)

    return result
