"""Build pricing snapshots from matched competitor listings."""

import logging
import statistics
from datetime import date, datetime, UTC
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import (
    CompetitorListing,
    Product,
    ProductMatch,
    PricingSnapshot,
)
from agnaradie_pricing.settings import own_store_ids

logger = logging.getLogger(__name__)

# Prices further than this multiple from the median are treated as outliers
# and excluded from market stats (e.g. 3.0 = anything >300% of median or <33%).
_OUTLIER_RATIO = 3.0


def build_snapshots(session: Session, snapshot_date: date | None = None) -> int:
    """Compute and upsert PricingSnapshot rows for all matched products.

    ToolZone is the baseline store. Its latest scraped listing price is used as
    the "own price" (ag_price). The other competitors are measured against it.

    Returns the number of rows written.
    """
    today = snapshot_date or datetime.now(UTC).date()
    own_stores = own_store_ids()
    written = 0

    # All products that have at least one approved match
    matched_product_ids = (
        select(ProductMatch.product_id)
        .where(ProductMatch.product_id.is_not(None))
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

        # Remove outlier prices before computing market stats.
        # Prices >300% above or below the raw median indicate bad matches or
        # scraper errors and would distort min/median/max and recommendations.
        clean_prices, outlier_prices = _filter_price_outliers(prices_by_competitor)
        if outlier_prices:
            logger.warning(
                "product_id=%d: dropping %d outlier price(s) from snapshot: %s",
                product.id,
                len(outlier_prices),
                {k: f"{v:.2f}" for k, v in outlier_prices.items()},
            )
        if not clean_prices:
            continue

        ag_price = toolzone_price
        all_prices = list(clean_prices.values())

        min_price = Decimal(str(min(all_prices)))
        max_price = Decimal(str(max(all_prices)))
        median_price = Decimal(str(statistics.median(all_prices)))
        cheapest = min(clean_prices, key=clean_prices.__getitem__)

        # AG rank: 1 = cheapest overall (own price vs clean competitor prices)
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
        existing.competitor_count = len(clean_prices)
        existing.min_price = min_price
        existing.median_price = median_price
        existing.max_price = max_price
        existing.ag_rank = ag_rank
        existing.cheapest_competitor = cheapest
        written += 1

    return written


def _filter_price_outliers(
    prices: dict[str, float],
    ratio: float = _OUTLIER_RATIO,
) -> tuple[dict[str, float], dict[str, float]]:
    """Split prices into (clean, outliers).

    A price is an outlier when it is more than `ratio` times above the median
    or less than 1/ratio times the median.  With ratio=3 that means any price
    outside the band [median/3, median*3] is excluded — roughly a 300% cap.
    Requires at least 2 prices; single-price dicts are returned as-is.
    """
    if len(prices) < 2:
        return prices, {}

    med = statistics.median(prices.values())
    if med <= 0:
        return prices, {}

    clean: dict[str, float] = {}
    outliers: dict[str, float] = {}
    for cid, price in prices.items():
        if price / med > ratio or price / med < 1.0 / ratio:
            outliers[cid] = price
        else:
            clean[cid] = price

    # If filtering removed everything (e.g. bimodal prices), keep all to avoid
    # silently dropping a product from the snapshot.
    if not clean:
        return prices, {}

    return clean, outliers


def _latest_prices_for_product(
    session: Session,
    product_id: int,
    today: date,
    exclude_competitors: frozenset[str] = frozenset(),
    include_only: frozenset[str] | None = None,
) -> dict[str, float]:
    """Return {competitor_id: price_eur} for the most recent listing per store.

    Uses a single JOIN query (ProductMatch → CompetitorListing) ordered by
    scraped_at DESC so the first row seen per competitor_id is the newest.

    Only approved matches with confidence ≥ 0.85 are considered.
    """
    rows = session.execute(
        select(CompetitorListing)
        .join(ProductMatch, ProductMatch.listing_id == CompetitorListing.id)
        .where(
            ProductMatch.product_id == product_id,
            ProductMatch.confidence >= Decimal("0.85"),
            ProductMatch.status == "approved",
        )
        .order_by(CompetitorListing.competitor_id, CompetitorListing.scraped_at.desc())
    ).scalars().all()

    result: dict[str, float] = {}
    for listing in rows:
        cid = listing.competitor_id
        if include_only is not None and cid not in include_only:
            continue
        if cid in exclude_competitors:
            continue
        if cid not in result:  # keep only the most recent row per competitor
            result[cid] = float(listing.price_eur)

    return result
