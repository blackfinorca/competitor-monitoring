"""Persistence helpers for scraper output."""

from dataclasses import replace
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import CompetitorListing as CompetitorListingRow
from agnaradie_pricing.db.models import Product as ProductRow
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

    listings = _with_backfilled_brands(session, listings)
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
                    "competitor_sku": func.coalesce(stmt.excluded.competitor_sku, CompetitorListingRow.competitor_sku),
                    "brand":      func.coalesce(stmt.excluded.brand, CompetitorListingRow.brand),
                    "mpn":        func.coalesce(stmt.excluded.mpn, CompetitorListingRow.mpn),
                    "ean":        func.coalesce(stmt.excluded.ean, CompetitorListingRow.ean),
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
                    "competitor_sku": func.coalesce(stmt.excluded.competitor_sku, CompetitorListingRow.competitor_sku),
                    "brand":      func.coalesce(stmt.excluded.brand, CompetitorListingRow.brand),
                    "mpn":        func.coalesce(stmt.excluded.mpn, CompetitorListingRow.mpn),
                    "ean":        func.coalesce(stmt.excluded.ean, CompetitorListingRow.ean),
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


def _with_backfilled_brands(
    session: Session, listings: list[CompetitorListing]
) -> list[CompetitorListing]:
    missing_brand_eans = {
        listing.ean
        for listing in listings
        if _is_missing(listing.brand) and _is_real_ean(listing.ean)
    }
    if not missing_brand_eans:
        return listings

    brands_by_ean: dict[str, str] = {}
    product_rows = session.execute(
        select(ProductRow.ean, ProductRow.brand)
        .where(ProductRow.ean.in_(missing_brand_eans))
        .where(ProductRow.brand.is_not(None))
    )
    for ean, brand in product_rows:
        if ean is not None and not _is_missing(brand):
            brands_by_ean.setdefault(ean, brand)

    unresolved_eans = missing_brand_eans - brands_by_ean.keys()
    if unresolved_eans:
        listing_rows = session.execute(
            select(CompetitorListingRow.ean, CompetitorListingRow.brand)
            .where(CompetitorListingRow.ean.in_(unresolved_eans))
            .where(CompetitorListingRow.brand.is_not(None))
        )
        for ean, brand in listing_rows:
            if ean is not None and not _is_missing(brand):
                brands_by_ean.setdefault(ean, brand)

    if not brands_by_ean:
        return listings

    return [
        replace(listing, brand=brands_by_ean[listing.ean])
        if _is_missing(listing.brand)
        and listing.ean is not None
        and listing.ean in brands_by_ean
        else listing
        for listing in listings
    ]


def _is_real_ean(ean: str | None) -> bool:
    return ean is not None and ean.isdigit() and 8 <= len(ean) <= 14


def _is_missing(value: str | None) -> bool:
    return value is None or not value.strip()


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
