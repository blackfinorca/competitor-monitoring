"""Seed the dev database with live-scraped data so the dashboard has something to show.

Steps
-----
1. Scrape 11 Knipex products from ToolZone (our baseline store).
2. Insert them into the `products` table (catalogue) AND `competitor_listings`.
3. Search AhProfi, NaradieShop, DoktorKladivo for each product.
4. Save competitor results to `competitor_listings`.
5. Run daily_match  → product_matches.
6. Run daily_recommend → pricing_snapshot + recommendations.

Run from the project root:
    python scripts/seed_demo_data.py
"""

from __future__ import annotations

import re
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import httpx
from sqlalchemy.orm import Session

from agnaradie_pricing.db.models import Base, Product
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.matching.deterministic import match_deterministic
from agnaradie_pricing.pricing.recommender import build_recommendations
from agnaradie_pricing.pricing.snapshot import build_snapshots
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.base import CompetitorListing
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.persistence import save_competitor_listings
from agnaradie_pricing.scrapers.toolzone import _parse_product_page
from agnaradie_pricing.settings import Settings
from agnaradie_pricing.db.models import CompetitorListing as ListingRow, ProductMatch

# ---------------------------------------------------------------------------
# ToolZone product URLs with manufacturer codes in slug
# ---------------------------------------------------------------------------

TOOLZONE_URLS = [
    "https://www.toolzone.sk/produkt/automaticke-odizolovacie-klieste-precistrip-16-12-52-195-knipex-64049.htm",
    "https://www.toolzone.sk/produkt/klieste-siko-cobra-quickset-8722250-s-2-zlozkovymi-rukovatami-250-mm-knipex-28292.htm",
    "https://www.toolzone.sk/produkt/pakove-klieste-mini-7112200-s-viaczlozkovymi-rukovatami-200-mm-knipex-28242.htm",
    "https://www.toolzone.sk/produkt/pakove-klieste-mini-7102200-s-viaczlozkovymi-rukovatami-200-mm-knipex-28239.htm",
    "https://www.toolzone.sk/produkt/klieste-siko-cobra-8703300-s-plastovymi-rukovatami-300-mm-knipex-28097.htm",
    "https://www.toolzone.sk/produkt/klieste-siko-cobra-8703250-s-plastovymi-rukovatami-250-mm-knipex-28096.htm",
    "https://www.toolzone.sk/produkt/klieste-siko-cobra-8703180-s-plastovymi-rukovatami-180-mm-knipex-28095.htm",
    "https://www.toolzone.sk/produkt/klieste-siko-cobra-8701180-s-plastovymi-rukovatami-180-mm-knipex-28056.htm",
    "https://www.toolzone.sk/produkt/krimpovacie-klieste-9722240-na-izolovane-kablove-koncovky-0-75-6-mm2-knipex-27961.htm",
    "https://www.toolzone.sk/produkt/rezac-kablov-95-32-100-820-mm-knipex-27829.htm",
    "https://www.toolzone.sk/produkt/rezac-kablov-95-32-060-680-mm-knipex-27828.htm",
]

_CODE_RE = re.compile(r'\b(\d{2}-\d{2}-\d{3}|\d{6,8})\b')


def _mpn_from_url(url: str) -> str | None:
    slug = url.split("/produkt/")[1].replace(".htm", "")
    codes = _CODE_RE.findall(slug)
    return codes[0] if codes else None


def _titles_match(t1: str, t2: str, min_common: int = 2) -> bool:
    stop = frozenset("na pre s z a so pri od do mm".split())
    def tok(t: str) -> set[str]:
        return set(re.findall(r'[a-záäčďéíľĺňóôŕšťúýž0-9]{3,}', t.lower())) - stop
    return len(tok(t1) & tok(t2)) >= min_common


# ---------------------------------------------------------------------------
# Step 1 — Scrape ToolZone
# ---------------------------------------------------------------------------

def scrape_toolzone(client: httpx.Client) -> list[CompetitorListing]:
    listings = []
    print("\n[1/4] Scraping ToolZone …")
    for i, url in enumerate(TOOLZONE_URLS, 1):
        time.sleep(1.0)
        try:
            r = client.get(url)
            listing = _parse_product_page(r.text, "toolzone_sk", url)
            if listing:
                # Override mpn with the manufacturer code from the URL slug
                mpn = _mpn_from_url(url)
                if mpn:
                    from dataclasses import asdict
                    listing = listing.__class__(**{**asdict(listing), "mpn": mpn})
                listings.append(listing)
                print(f"  [{i:02d}] ✓ {listing.title[:55]:<55} €{listing.price_eur:.2f}")
            else:
                print(f"  [{i:02d}] ✗ parse failed: {url}")
        except Exception as e:
            print(f"  [{i:02d}] ✗ error: {e}")
    return listings


# ---------------------------------------------------------------------------
# Step 2 — Insert products table from ToolZone listings
# ---------------------------------------------------------------------------

def upsert_products(session: Session, listings: list[CompetitorListing]) -> dict[str, int]:
    """Insert/update one Product row per ToolZone listing. Returns ean→product_id map."""
    from sqlalchemy import select
    ean_to_id: dict[str, int] = {}
    for listing in listings:
        sku = f"TZ-{listing.mpn or listing.ean or listing.title[:20]}"
        existing = session.scalars(select(Product).where(Product.sku == sku)).first()
        if existing is None:
            existing = Product(sku=sku, title=listing.title)
            session.add(existing)
            session.flush()  # get the id

        existing.brand = listing.brand
        existing.mpn = listing.mpn
        existing.ean = listing.ean
        existing.title = listing.title
        existing.category = "Ruční nářadí"
        existing.price_eur = Decimal(str(listing.price_eur))

        if listing.ean:
            ean_to_id[listing.ean] = existing.id

    session.flush()
    return ean_to_id


# ---------------------------------------------------------------------------
# Step 3 — Search competitors
# ---------------------------------------------------------------------------

def search_competitors(
    toolzone_listings: list[CompetitorListing],
    client: httpx.Client,
) -> list[CompetitorListing]:
    scrapers = [
        AhProfiScraper(),
        NaradieShopScraper(),
        DoktorKladivoScraper(),
    ]
    results: list[CompetitorListing] = []
    print("\n[3/4] Searching competitors …")

    for tz in toolzone_listings:
        brand = tz.brand or "Knipex"
        mpn = tz.mpn
        ean = tz.ean

        for scraper in scrapers:
            hit = None
            for term in filter(None, [mpn, ean]):
                try:
                    time.sleep(1.1)
                    r = scraper.search_by_mpn(brand, term)
                    if r and _titles_match(tz.title, r.title):
                        hit = r
                        break
                except Exception:
                    pass

            if hit:
                results.append(hit)
                diff = (hit.price_eur - tz.price_eur) / tz.price_eur * 100
                print(
                    f"  ✓ {scraper.competitor_id:<25} "
                    f"{hit.title[:40]:<40} "
                    f"€{hit.price_eur:.2f} ({diff:+.0f}% vs TZ €{tz.price_eur:.2f})"
                )
            else:
                print(f"  — {scraper.competitor_id:<25} no match for {tz.title[:40]}")

    return results


# ---------------------------------------------------------------------------
# Step 4 — Match competitor listings to products
# ---------------------------------------------------------------------------

def run_matching(session: Session) -> int:
    from sqlalchemy import select as sa_select

    products = session.scalars(sa_select(Product)).all()
    product_list = [{"id": p.id, "brand": p.brand, "mpn": p.mpn, "ean": p.ean} for p in products]

    listings = session.scalars(
        sa_select(ListingRow).where(ListingRow.competitor_id != "toolzone_sk")
    ).all()

    matched = 0
    for listing in listings:
        listing_dict = {"brand": listing.brand, "mpn": listing.mpn, "ean": listing.ean}
        for product in product_list:
            result = match_deterministic(product, listing_dict)
            if result:
                match_type, confidence = result
                existing = session.scalars(
                    sa_select(ProductMatch).where(
                        ProductMatch.ag_product_id == product["id"],
                        ProductMatch.competitor_id == listing.competitor_id,
                        ProductMatch.competitor_sku == listing.competitor_sku,
                    )
                ).first()
                if existing is None:
                    session.add(ProductMatch(
                        ag_product_id=product["id"],
                        competitor_id=listing.competitor_id,
                        competitor_sku=listing.competitor_sku,
                        match_type=match_type,
                        confidence=Decimal(str(confidence)),
                    ))
                    matched += 1
                break

    # Also match ToolZone listings to products (needed for own-store price lookup)
    tz_listings = session.scalars(
        sa_select(ListingRow).where(ListingRow.competitor_id == "toolzone_sk")
    ).all()
    for listing in tz_listings:
        listing_dict = {"brand": listing.brand, "mpn": listing.mpn, "ean": listing.ean}
        for product in product_list:
            result = match_deterministic(product, listing_dict)
            if result:
                match_type, confidence = result
                existing = session.scalars(
                    sa_select(ProductMatch).where(
                        ProductMatch.ag_product_id == product["id"],
                        ProductMatch.competitor_id == "toolzone_sk",
                        ProductMatch.competitor_sku == listing.competitor_sku,
                    )
                ).first()
                if existing is None:
                    session.add(ProductMatch(
                        ag_product_id=product["id"],
                        competitor_id="toolzone_sk",
                        competitor_sku=listing.competitor_sku,
                        match_type=match_type,
                        confidence=Decimal(str(confidence)),
                    ))
                break

    return matched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    settings = Settings()
    factory = make_session_factory(settings)

    # Ensure schema exists
    from agnaradie_pricing.db.session import make_engine
    Base.metadata.create_all(make_engine(settings))

    client = httpx.Client(timeout=15.0, follow_redirects=True)

    # 1. Scrape ToolZone
    tz_listings = scrape_toolzone(client)
    if not tz_listings:
        print("No ToolZone listings — aborting.")
        return
    print(f"  → {len(tz_listings)} ToolZone products scraped")

    with factory() as session:
        # 2. Populate products table + save ToolZone listings
        print("\n[2/4] Writing products + ToolZone listings to DB …")
        upsert_products(session, tz_listings)
        save_competitor_listings(session, tz_listings)
        session.commit()
        print(f"  → {len(tz_listings)} products and listings saved")

    # 3. Search competitors
    competitor_listings = search_competitors(tz_listings, client)
    print(f"  → {len(competitor_listings)} competitor listings found")

    with factory() as session:
        save_competitor_listings(session, competitor_listings)
        session.commit()

        # 4. Match
        print("\n[4/4] Matching + snapshot + recommendations …")
        n_matched = run_matching(session)
        session.commit()
        print(f"  → {n_matched} new product matches")

        # 5. Snapshot + recommendations
        n_snap = build_snapshots(session)
        session.commit()
        print(f"  → {n_snap} pricing snapshots built")

        n_rec = build_recommendations(session)
        session.commit()
        print(f"  → {n_rec} recommendations generated")

    print("\n✓ Done. Refresh the dashboard at http://localhost:8501")


if __name__ == "__main__":
    main()
