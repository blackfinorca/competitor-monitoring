"""Persistence helpers for scraper output."""

from decimal import Decimal

from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import CompetitorListing as CompetitorListingRow
from agnaradie_pricing.scrapers.base import CompetitorListing


def save_competitor_listings(
    session: Session, listings: list[CompetitorListing]
) -> list[CompetitorListingRow]:
    rows = [_to_row(listing) for listing in listings]
    session.add_all(rows)
    return rows


def _to_row(listing: CompetitorListing) -> CompetitorListingRow:
    return CompetitorListingRow(
        competitor_id=listing.competitor_id,
        competitor_sku=listing.competitor_sku,
        brand=listing.brand,
        mpn=listing.mpn,
        ean=listing.ean,
        title=listing.title,
        price_eur=Decimal(str(listing.price_eur)),
        currency=listing.currency,
        in_stock=listing.in_stock,
        url=listing.url,
        scraped_at=listing.scraped_at,
    )

