"""Manufacturer-focused scraping entrypoint.

Scrapes all products for a given manufacturer across ToolZone (reference) and
all configured competitors, then saves results to the database.

Strategy per competitor type:
  Catalogue-crawl  → ToolZone, boukal_cz, bo_import_cz, agi_sk,
                      madmat_sk, centrumnaradia_sk (feed filtered by brand)
  Search-by-MPN    → doktorkladivo_sk, ahprofi_sk, naradieshop_sk, rebiop_sk
                      (use ToolZone MPN list as search input)

Execution model:
  Phase 1 (sequential): scrape ToolZone manufacturer page → reference products
  Phase 2 (parallel):   catalogue-crawl competitors run concurrently
  Phase 3 (parallel):   search-by-MPN competitors run concurrently,
                         each searching for every ToolZone MPN

Run:
    python jobs/manufacturer_scrape.py --manufacturer knipex
    python jobs/manufacturer_scrape.py --manufacturer knipex --only boukal_cz agi_sk
    python jobs/manufacturer_scrape.py --manufacturer knipex --sequential
    python jobs/manufacturer_scrape.py --list-manufacturers
"""

import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.scrapers.agi import AgiScraper
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.base import CompetitorListing
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

# ------------------------------------------------------------------
# Competitor registries
# ------------------------------------------------------------------

# Competitors that have a manufacturer-page crawl method
CATALOGUE_COMPETITORS: dict[str, type] = {
    "boukal_cz": BoukalScraper,
    "bo_import_cz": BoImportScraper,
    "agi_sk": AgiScraper,
}

# Heureka feed competitors — fetch full feed and filter by brand
FEED_COMPETITORS: set[str] = {
    "madmat_sk",
    "centrumnaradia_sk",
}

# Search-by-MPN competitors — query each ToolZone MPN in turn
SEARCH_COMPETITORS: dict[str, type] = {
    "doktorkladivo_sk": DoktorKladivoScraper,
    "ahprofi_sk": AhProfiScraper,
    "naradieshop_sk": NaradieShopScraper,
    "rebiop_sk": RebiopScraper,
}

_SAVE_BATCH_SIZE = 200
_log_lock = threading.Lock()


def _log(msg: str, *args) -> None:
    with _log_lock:
        logger.info(msg, *args)


# ------------------------------------------------------------------
# Batch-saving helper
# ------------------------------------------------------------------

def _flush(buffer: list[CompetitorListing], factory, cid: str, saved: int) -> tuple[list, int]:
    """Save buffer to DB, return (empty_buffer, new_saved_count)."""
    if not buffer:
        return buffer, saved
    with factory() as session:
        save_competitor_listings(session, buffer)
        session.commit()
    saved += len(buffer)
    _log("  %s: flushed %d listings (%d total so far)", cid, len(buffer), saved)
    return [], saved


# ------------------------------------------------------------------
# Per-competitor scrape functions
# ------------------------------------------------------------------

def _scrape_catalogue_competitor(
    config: dict,
    scraper_cls: type,
    manufacturer_slug: str,
    factory,
) -> tuple[str, int]:
    """Scrape a catalogue-crawl competitor's manufacturer page."""
    cid = config["id"]
    scraper = scraper_cls(config)
    _log("Scraping %s (catalogue) for manufacturer=%s …", cid, manufacturer_slug)

    saved, buffer = 0, []
    try:
        for listing in scraper.run_manufacturer_iter(manufacturer_slug):
            buffer.append(listing)
            if len(buffer) >= _SAVE_BATCH_SIZE:
                buffer, saved = _flush(buffer, factory, cid, saved)
    except BaseException:
        with _log_lock:
            logger.exception("Error/interrupt scraping %s after %d saved", cid, saved)
        raise
    finally:
        buffer, saved = _flush(buffer, factory, cid, saved)

    _log("Saved %d listings for %s", saved, cid)
    return cid, saved


def _scrape_feed_competitor(
    config: dict,
    brand_name: str,
    factory,
) -> tuple[str, int]:
    """Fetch a Heureka XML feed and keep only items matching brand_name."""
    cid = config["id"]
    scraper = ShoptetGenericScraper(config)
    _log("Scraping %s (feed) filtering brand=%s …", cid, brand_name)

    saved, buffer = 0, []
    try:
        feed_url = scraper.discover_feed()
        if not feed_url:
            _log("%s: no feed found", cid)
            return cid, 0
        all_listings = scraper.fetch_feed(feed_url)
        brand_lower = brand_name.lower()
        for listing in all_listings:
            if listing.brand and listing.brand.lower() == brand_lower:
                buffer.append(listing)
                if len(buffer) >= _SAVE_BATCH_SIZE:
                    buffer, saved = _flush(buffer, factory, cid, saved)
    except BaseException:
        with _log_lock:
            logger.exception("Error/interrupt scraping %s after %d saved", cid, saved)
        raise
    finally:
        buffer, saved = _flush(buffer, factory, cid, saved)

    _log("Saved %d listings for %s", saved, cid)
    return cid, saved


def _scrape_search_competitor(
    config: dict,
    scraper_cls: type,
    reference_products: list[CompetitorListing],
    factory,
) -> tuple[str, int]:
    """Search for each ToolZone reference product MPN on a search-based competitor."""
    cid = config["id"]
    scraper = scraper_cls(config)
    _log("Scraping %s (search-by-MPN) for %d products …", cid, len(reference_products))

    saved, buffer = 0, []
    try:
        for ref in reference_products:
            if not ref.brand or not ref.mpn:
                continue
            try:
                listing = scraper.search_by_mpn(ref.brand, ref.mpn)
                if listing:
                    buffer.append(listing)
                    if len(buffer) >= _SAVE_BATCH_SIZE:
                        buffer, saved = _flush(buffer, factory, cid, saved)
            except Exception:
                pass  # individual search failure — continue
    except BaseException:
        with _log_lock:
            logger.exception("Error/interrupt scraping %s after %d saved", cid, saved)
        raise
    finally:
        buffer, saved = _flush(buffer, factory, cid, saved)

    _log("Saved %d listings for %s", saved, cid)
    return cid, saved


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main(
    manufacturer_slug: str,
    brand_name: str | None = None,
    only: list[str] | None = None,
    sequential: bool = False,
) -> dict[str, int]:
    """Scrape all products for a manufacturer across ToolZone + all competitors.

    Args:
        manufacturer_slug: ToolZone manufacturer URL slug (e.g. "knipex")
        brand_name: Display brand name for filtering feeds (defaults to slug)
        only: If set, restrict to these competitor IDs
        sequential: Disable parallel execution (useful for debugging)

    Returns:
        Dict mapping competitor_id → number of listings saved
    """
    if brand_name is None:
        brand_name = manufacturer_slug.replace("-", " ").title()

    settings = Settings()
    factory = make_session_factory(settings)
    comp_configs = {c["id"]: c for c in load_competitors()}
    counts: dict[str, int] = {}

    def _active(cid: str) -> bool:
        return (only is None or cid in only) and cid in comp_configs

    # ------------------------------------------------------------------
    # Phase 1: scrape ToolZone reference catalogue (sequential — needed first)
    # ------------------------------------------------------------------
    tz_config = comp_configs.get("toolzone_sk", {
        "id": "toolzone_sk", "url": "https://www.toolzone.sk",
        "weight": 1.0, "rate_limit_rps": 4, "workers": 16,
    })
    if only is None or "toolzone_sk" in only:
        tz_scraper = ToolZoneScraper(tz_config)
        _log("Phase 1: scraping ToolZone for manufacturer=%s …", manufacturer_slug)
        tz_buffer: list[CompetitorListing] = []
        tz_saved = 0
        reference_products: list[CompetitorListing] = []
        try:
            for listing in tz_scraper.run_manufacturer_iter(manufacturer_slug):
                tz_buffer.append(listing)
                reference_products.append(listing)
                if len(tz_buffer) >= _SAVE_BATCH_SIZE:
                    tz_buffer, tz_saved = _flush(tz_buffer, factory, "toolzone_sk", tz_saved)
        except BaseException:
            logger.exception("Error/interrupt scraping ToolZone")
            raise
        finally:
            tz_buffer, tz_saved = _flush(tz_buffer, factory, "toolzone_sk", tz_saved)
        counts["toolzone_sk"] = tz_saved
        _log("ToolZone: %d reference products scraped", len(reference_products))
    else:
        reference_products = []

    # ------------------------------------------------------------------
    # Phase 2: catalogue-crawl competitors (parallel)
    # ------------------------------------------------------------------
    catalogue_tasks = [
        (cid, cls) for cid, cls in CATALOGUE_COMPETITORS.items() if _active(cid)
    ]
    feed_tasks = [cid for cid in FEED_COMPETITORS if _active(cid)]

    all_phase2 = catalogue_tasks + [(cid, None) for cid in feed_tasks]

    def _run_phase2(task):
        cid, cls = task
        cfg = comp_configs[cid]
        if cls is not None:
            return _scrape_catalogue_competitor(cfg, cls, manufacturer_slug, factory)
        else:
            return _scrape_feed_competitor(cfg, brand_name, factory)

    if all_phase2:
        _log("Phase 2: running %d catalogue/feed competitors …", len(all_phase2))
        if sequential or len(all_phase2) == 1:
            for task in all_phase2:
                cid, n = _run_phase2(task)
                counts[cid] = n
        else:
            with ThreadPoolExecutor(max_workers=len(all_phase2)) as pool:
                futures = {pool.submit(_run_phase2, t): t[0] for t in all_phase2}
                for future in as_completed(futures):
                    try:
                        cid, n = future.result()
                        counts[cid] = n
                    except Exception:
                        logger.exception("Phase 2 competitor failed")

    # ------------------------------------------------------------------
    # Phase 3: search-by-MPN competitors (parallel, using ToolZone results)
    # ------------------------------------------------------------------
    search_tasks = [
        (cid, cls) for cid, cls in SEARCH_COMPETITORS.items() if _active(cid)
    ]

    if search_tasks and reference_products:
        _log(
            "Phase 3: running %d search competitors against %d reference products …",
            len(search_tasks), len(reference_products),
        )

        def _run_search(task):
            cid, cls = task
            return _scrape_search_competitor(comp_configs[cid], cls, reference_products, factory)

        if sequential or len(search_tasks) == 1:
            for task in search_tasks:
                cid, n = _run_search(task)
                counts[cid] = n
        else:
            with ThreadPoolExecutor(max_workers=len(search_tasks)) as pool:
                futures = {pool.submit(_run_search, t): t[0] for t in search_tasks}
                for future in as_completed(futures):
                    try:
                        cid, n = future.result()
                        counts[cid] = n
                    except Exception:
                        logger.exception("Phase 3 competitor failed")
    elif search_tasks and not reference_products:
        _log("Phase 3 skipped: no ToolZone reference products (run without --only to include toolzone_sk)")

    return counts


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Scrape all products for one manufacturer across ToolZone + competitors."
    )
    parser.add_argument(
        "--manufacturer",
        metavar="SLUG",
        help="Manufacturer slug as used in ToolZone URLs (e.g. knipex, wiha, format)",
    )
    parser.add_argument(
        "--brand-name",
        metavar="NAME",
        default=None,
        help="Brand display name for feed filtering (default: derived from slug)",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        metavar="COMPETITOR_ID",
        help="Scrape only these competitor IDs",
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel execution (run competitors one by one — useful for debugging)",
    )
    parser.add_argument(
        "--list-manufacturers",
        action="store_true",
        help="Print all manufacturer slugs available on ToolZone and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.list_manufacturers:
        tz = ToolZoneScraper()
        slugs = tz.get_manufacturer_slugs()
        print(f"Found {len(slugs)} manufacturers on ToolZone:")
        for name, slug in sorted(slugs, key=lambda x: x[1]):
            print(f"  {slug:30s}  {name}")
        sys.exit(0)

    if not args.manufacturer:
        parser.error("--manufacturer is required (or use --list-manufacturers)")

    result = main(
        manufacturer_slug=args.manufacturer,
        brand_name=args.brand_name,
        only=args.only,
        sequential=args.sequential,
    )
    print("\nResults:")
    for cid, n in sorted(result.items()):
        print(f"  {cid}: {n} listings")
