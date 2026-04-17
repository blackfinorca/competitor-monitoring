"""Persistence helpers for scraper output."""

from decimal import Decimal

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import CompetitorListing as CompetitorListingRow
from agnaradie_pricing.scrapers.base import CompetitorListing


def save_competitor_listings(
    session: Session, listings: list[CompetitorListing]
) -> None:
    """Upsert listings into competitor_listings.

    Uses ON CONFLICT DO UPDATE (SQLite) / ON CONFLICT DO UPDATE (PostgreSQL) on
    the unique constraint (competitor_id, url).  When the same URL is scraped
    again, price, stock, title and scraped_at are refreshed.  Listings with a
    NULL url fall back to a plain INSERT (nothing to conflict on).
    """
    if not listings:
        return

    dialect = session.bind.dialect.name  # type: ignore[union-attr]

    with_url = [l for l in listings if l.url]
    without_url = [l for l in listings if not l.url]

    if with_url:
        rows = [_to_dict(l) for l in with_url]
        if dialect == "sqlite":
            stmt = sqlite_insert(CompetitorListingRow).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["competitor_id", "url"],
                set_={
                    "price_eur":  stmt.excluded.price_eur,
                    "in_stock":   stmt.excluded.in_stock,
                    "title":      stmt.excluded.title,
                    "scraped_at": stmt.excluded.scraped_at,
                },
            )
        else:
            stmt = pg_insert(CompetitorListingRow).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["competitor_id", "url"],
                set_={
                    "price_eur":  stmt.excluded.price_eur,
                    "in_stock":   stmt.excluded.in_stock,
                    "title":      stmt.excluded.title,
                    "scraped_at": stmt.excluded.scraped_at,
                },
            )
        # Use the raw connection to bypass the ORM identity map.  When
        # session.execute() runs a Core INSERT on an ORM-mapped table,
        # SQLAlchemy 2.0 can add the result back into the session as a
        # pending "new" object, causing a duplicate INSERT on the next flush.
        session.connection().execute(stmt)

    if without_url:
        session.add_all([_to_row(l) for l in without_url])


def _to_dict(listing: CompetitorListing) -> dict:
    return {
        "competitor_id": listing.competitor_id,
        "competitor_sku": listing.competitor_sku,
        "brand": listing.brand,
        "mpn": listing.mpn,
        "ean": listing.ean,
        "title": listing.title,
        "price_eur": Decimal(str(listing.price_eur)),
        "currency": listing.currency,
        "in_stock": listing.in_stock,
        "url": listing.url,
        "scraped_at": listing.scraped_at,
    }


def _to_row(listing: CompetitorListing) -> CompetitorListingRow:
    return CompetitorListingRow(**_to_dict(listing))

