"""Daily competitor scraping entrypoint.

Strategy per competitor (from live inspection):
  - madmat_sk         → Heureka XML feed  /heureka.xml
  - centrumnaradia_sk → Heureka XML feed  /heureka.xml
  - doktorkladivo_sk  → search-by-MPN fallback (JSON-LD ItemList)
  - ahprofi_sk        → search-by-MPN fallback (custom HTML parser)
  - naradieshop_sk    → search-by-MPN fallback (ThirtyBees HTML parser)
  - toolzone_sk       → sitemap-based full-catalogue scrape (JSON-LD per page)
  - rebiop_sk         → search-by-MPN fallback (custom HTML parser; /search/products?q=)
  - strend_sk         → discover_feed first; search-by-MPN fallback (WooCommerce HTML)
  - boukal_cz         → discover_feed first (Heureka/Zboží XML); JS-rendered fallback
  - ferant_sk         → skipped (DNS fails as of 2026-04)
"""

import logging
from pathlib import Path

from agnaradie_pricing.catalogue.ingest import load_catalogue_csv
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.boukal import BoukalScraper
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.persistence import save_competitor_listings
from agnaradie_pricing.scrapers.rebiop import RebiopScraper
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper
from agnaradie_pricing.scrapers.strend import StrendScraper
from agnaradie_pricing.scrapers.toolzone import ToolZoneScraper
from agnaradie_pricing.settings import Settings, load_competitors

logger = logging.getLogger(__name__)

# Competitors with a known Heureka feed — use ShoptetGenericScraper as-is
# (discover_feed probes the standard paths; fetch_feed parses the XML).
FEED_COMPETITORS = {
    "madmat_sk",
    "centrumnaradia_sk",
}

# Competitors that need search-by-MPN fallback — custom subclasses registered here.
SEARCH_COMPETITORS = {
    "doktorkladivo_sk": DoktorKladivoScraper,
    "ahprofi_sk": AhProfiScraper,
    "naradieshop_sk": NaradieShopScraper,
    "toolzone_sk": ToolZoneScraper,
    "rebiop_sk": RebiopScraper,
    "strend_sk": StrendScraper,
    "boukal_cz": BoukalScraper,
}


def build_scraper(config: dict):
    cid = config["id"]
    if cid in SEARCH_COMPETITORS:
        return SEARCH_COMPETITORS[cid](config)
    # Generic Shoptet / Heureka-feed scraper for feed competitors
    return ShoptetGenericScraper(config)


def main(
    catalogue_path: Path = Path("data/ag_catalogue.csv"),
    only: list[str] | None = None,
) -> dict[str, int]:
    settings = Settings()
    competitors = load_competitors()
    catalogue = [
        {"brand": r.brand, "mpn": r.mpn, "ean": r.ean, "sku": r.sku}
        for r in load_catalogue_csv(catalogue_path)
    ]

    factory = make_session_factory(settings)
    counts: dict[str, int] = {}

    for comp_config in competitors:
        cid = comp_config["id"]
        if only and cid not in only:
            continue
        if cid not in FEED_COMPETITORS and cid not in SEARCH_COMPETITORS:
            logger.info("Skipping %s — scraper not yet implemented", cid)
            counts[cid] = 0
            continue

        scraper = build_scraper(comp_config)
        logger.info("Scraping %s …", cid)
        try:
            listings = scraper.run_daily(catalogue)
        except Exception:
            logger.exception("Error scraping %s", cid)
            counts[cid] = 0
            continue

        with factory() as session:
            save_competitor_listings(session, listings)
            session.commit()

        counts[cid] = len(listings)
        logger.info("Saved %d listings for %s", len(listings), cid)

    return counts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run daily competitor scraping.")
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="COMPETITOR_ID",
        help="Scrape only these competitor IDs (e.g. --only strend_sk boukal_cz)",
    )
    parser.add_argument(
        "--catalogue",
        type=Path,
        default=Path("data/ag_catalogue.csv"),
        metavar="PATH",
        help="Path to AG catalogue CSV (default: data/ag_catalogue.csv)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = main(catalogue_path=args.catalogue, only=args.only)
    for cid, n in result.items():
        print(f"  {cid}: {n} listings")
