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
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agnaradie_pricing.catalogue.ingest import load_catalogue_csv
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.scrapers.agi import AgiScraper
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.bo_import import BoImportScraper
from agnaradie_pricing.scrapers.boukal import BoukalScraper
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.persistence import save_competitor_listings
from agnaradie_pricing.scrapers.rebiop import RebiopScraper
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper
from agnaradie_pricing.scrapers.toolzone import ToolZoneScraper
from agnaradie_pricing.settings import Settings, load_competitors

logger = logging.getLogger(__name__)
_log_lock = threading.Lock()

# Competitors with a known Heureka feed — use ShoptetGenericScraper as-is
# (discover_feed probes the standard paths; fetch_feed parses the XML).
FEED_COMPETITORS = {
    "madmat_sk",
    "centrumnaradia_sk",
}

# Competitors that need search-by-MPN fallback — custom subclasses registered here.
# strend_sk is excluded: site has no product catalogue (WordPress content site only).
SEARCH_COMPETITORS = {
    "doktorkladivo_sk": DoktorKladivoScraper,
    "ahprofi_sk": AhProfiScraper,
    "naradieshop_sk": NaradieShopScraper,
    "toolzone_sk": ToolZoneScraper,
    "rebiop_sk": RebiopScraper,
    "boukal_cz": BoukalScraper,
    "bo_import_cz": BoImportScraper,
    "agi_sk": AgiScraper,
}


# Flush scraped listings to the DB after this many are buffered.
# Smaller = more frequent commits (more durable); larger = fewer DB round-trips.
_SAVE_BATCH_SIZE = 200


def build_scraper(config: dict):
    cid = config["id"]
    if cid in SEARCH_COMPETITORS:
        return SEARCH_COMPETITORS[cid](config)
    # Generic Shoptet / Heureka-feed scraper for feed competitors
    return ShoptetGenericScraper(config)


def _scrape_one(
    comp_config: dict,
    catalogue: list[dict],
    factory,
) -> tuple[str, int]:
    """Scrape one competitor and save to DB in batches. Thread-safe."""
    cid = comp_config["id"]
    scraper = build_scraper(comp_config)

    def _log(msg, *args):
        with _log_lock:
            logger.info(msg, *args)

    _log("Scraping %s …", cid)
    saved, buffer = 0, []
    try:
        for listing in scraper.run_daily_iter(catalogue):
            buffer.append(listing)
            if len(buffer) >= _SAVE_BATCH_SIZE:
                with factory() as session:
                    save_competitor_listings(session, buffer)
                    session.commit()
                saved += len(buffer)
                _log("  %s: flushed %d listings (%d total so far)", cid, len(buffer), saved)
                buffer.clear()
    except Exception:
        with _log_lock:
            logger.exception("Error scraping %s after %d saved — flushing partial batch", cid, saved)

    # Flush remainder
    if buffer:
        with factory() as session:
            save_competitor_listings(session, buffer)
            session.commit()
        saved += len(buffer)
        buffer.clear()

    _log("Saved %d listings for %s", saved, cid)
    return cid, saved


def main(
    catalogue_path: Path = Path("data/ag_catalogue.csv"),
    only: list[str] | None = None,
    sequential: bool = False,
) -> dict[str, int]:
    settings = Settings()
    competitors = load_competitors()
    catalogue = [
        {"brand": r.brand, "mpn": r.mpn, "ean": r.ean, "sku": r.sku}
        for r in load_catalogue_csv(catalogue_path)
    ]

    factory = make_session_factory(settings)

    active = [
        c for c in competitors
        if (c["id"] in FEED_COMPETITORS or c["id"] in SEARCH_COMPETITORS)
        and (only is None or c["id"] in only)
    ]

    skipped = [
        c["id"] for c in competitors
        if c["id"] not in FEED_COMPETITORS and c["id"] not in SEARCH_COMPETITORS
        and (only is None or c["id"] in only)
    ]
    for cid in skipped:
        logger.info("Skipping %s — scraper not yet implemented", cid)

    counts: dict[str, int] = {}

    if sequential or len(active) <= 1:
        for comp_config in active:
            cid, n = _scrape_one(comp_config, catalogue, factory)
            counts[cid] = n
    else:
        with ThreadPoolExecutor(max_workers=len(active)) as pool:
            futures = {
                pool.submit(_scrape_one, c, catalogue, factory): c["id"]
                for c in active
            }
            for future in as_completed(futures):
                try:
                    cid, n = future.result()
                    counts[cid] = n
                except Exception:
                    cid = futures[future]
                    logger.exception("Competitor %s raised an unhandled exception", cid)
                    counts[cid] = 0

    return counts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run daily competitor scraping.")
    parser.add_argument(
        "--manufacturer",
        metavar="SLUG",
        help=(
            "Scrape all products for this manufacturer brand across ToolZone and "
            "all competitors (e.g. --manufacturer knipex). Delegates to "
            "manufacturer_scrape.py; mutually exclusive with --catalogue."
        ),
    )
    parser.add_argument(
        "--brand-name",
        metavar="NAME",
        help="Brand display name for feed/search filtering (default: derived from --manufacturer slug)",
    )
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
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel scraping (run competitors one by one — useful for debugging)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.manufacturer:
        # Manufacturer mode — delegate to manufacturer_scrape.main()
        import manufacturer_scrape
        result = manufacturer_scrape.main(
            manufacturer_slug=args.manufacturer,
            brand_name=args.brand_name,
            only=set(args.only) if args.only else None,
            sequential=args.sequential,
        )
    else:
        result = main(catalogue_path=args.catalogue, only=args.only, sequential=args.sequential)

    for cid, n in result.items():
        print(f"  {cid}: {n} listings")
