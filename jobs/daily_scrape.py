"""Daily competitor scraping entrypoint.

Strategy per competitor (from live inspection):
  - madmat_sk         → Heureka XML feed  /heureka.xml
  - centrumnaradia_sk → Heureka XML feed  /heureka.xml
  - doktorkladivo_sk  → search-by-MPN fallback (JSON-LD ItemList)
  - ahprofi_sk        → search-by-MPN fallback (custom HTML parser)
  - naradieshop_sk    → search-by-MPN fallback (ThirtyBees HTML parser)
  - toolzone_sk       → sitemap-based full-catalogue scrape (JSON-LD per page)
  - rebiop_sk         → search-by-MPN fallback (custom HTML parser; /search/products?q=)
  - strendpro_sk      → category + pagination full-catalogue crawl (strendpro.sk)
  - boukal_cz         → discover_feed first (Heureka/Zboží XML); JS-rendered fallback
  - fermatshop_sk     → sitemap full-catalogue crawl (fermatshop.sk)
"""

import logging
import signal
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from types import FrameType

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from agnaradie_pricing.catalogue.ingest import load_catalogue_csv
from agnaradie_pricing.db.session import make_session_factory
from agnaradie_pricing.scrapers.agi import AgiScraper
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.bo_import import BoImportScraper
from agnaradie_pricing.scrapers.boukal import BoukalScraper
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.ferant import FermatshopScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.persistence import save_competitor_listings
from agnaradie_pricing.scrapers.rebiop import RebiopScraper
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper
from agnaradie_pricing.scrapers.strend import StrendproScraper
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

# Competitors with dedicated crawler/fallback implementations.
SEARCH_COMPETITORS = {
    "doktorkladivo_sk": DoktorKladivoScraper,
    "ahprofi_sk": AhProfiScraper,
    "naradieshop_sk": NaradieShopScraper,
    "toolzone_sk": ToolZoneScraper,
    "rebiop_sk": RebiopScraper,
    "strendpro_sk": StrendproScraper,
    "fermatshop_sk": FermatshopScraper,
    "boukal_cz": BoukalScraper,
    "bo_import_cz": BoImportScraper,
    "agi_sk": AgiScraper,
}


# Flush scraped listings to the DB after this many are buffered.
# Smaller batches make interrupted runs more durable and visible in the dashboard sooner.
_SAVE_BATCH_SIZE = 50


def build_scraper(config: dict):
    cid = config["id"]
    if cid in SEARCH_COMPETITORS:
        return SEARCH_COMPETITORS[cid](config)
    # Generic Shoptet / Heureka-feed scraper for feed competitors
    return ShoptetGenericScraper(config)


def _flush_buffer(buffer: list, factory, cid: str, saved: int) -> tuple[list, int]:
    if not buffer:
        return buffer, saved
    with factory() as session:
        save_competitor_listings(session, buffer)
        session.commit()
    saved += len(buffer)
    with _log_lock:
        logger.info("  %s: flushed %d listings (%d total so far)", cid, len(buffer), saved)
    return [], saved


def _install_shutdown_handlers(stop_event: threading.Event):
    previous_handlers = {}

    def _handle_shutdown(signum: int, frame: FrameType | None) -> None:
        if stop_event.is_set():
            raise KeyboardInterrupt
        stop_event.set()
        with _log_lock:
            logger.warning(
                "Shutdown requested (%s) — stopping after current item and flushing buffered listings",
                signal.Signals(signum).name,
            )

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.getsignal(sig)
        signal.signal(sig, _handle_shutdown)

    def _restore() -> None:
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    return _restore


def _scrape_one(
    comp_config: dict,
    catalogue: list[dict],
    factory,
    stop_event: threading.Event | None = None,
    save_batch_size: int = _SAVE_BATCH_SIZE,
) -> tuple[str, int]:
    """Scrape one competitor and save to DB in batches. Thread-safe."""
    cid = comp_config["id"]
    scraper = build_scraper(comp_config)
    stop_event = stop_event or threading.Event()

    def _log(msg, *args):
        with _log_lock:
            logger.info(msg, *args)

    _log("Scraping %s …", cid)
    saved, buffer = 0, []
    try:
        for listing in scraper.run_daily_iter(catalogue):
            if stop_event.is_set():
                _log("  %s: stop requested; flushing %d buffered listings", cid, len(buffer))
                break
            buffer.append(listing)
            if len(buffer) >= save_batch_size:
                buffer, saved = _flush_buffer(buffer, factory, cid, saved)
    except BaseException:
        with _log_lock:
            logger.exception("Error/interrupt scraping %s after %d saved — flushing partial batch", cid, saved)
        raise
    finally:
        try:
            buffer, saved = _flush_buffer(buffer, factory, cid, saved)
        except Exception:
            with _log_lock:
                logger.exception("  %s: failed to flush final batch", cid)

    _log("Saved %d listings for %s", saved, cid)
    return cid, saved


def main(
    catalogue_path: Path = Path("data/ag_catalogue.csv"),
    only: list[str] | None = None,
    sequential: bool = False,
    stop_event: threading.Event | None = None,
    save_batch_size: int = _SAVE_BATCH_SIZE,
) -> dict[str, int]:
    stop_event = stop_event or threading.Event()
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
            if stop_event.is_set():
                logger.warning("Stop requested before starting next scraper")
                break
            cid, n = _scrape_one(
                comp_config,
                catalogue,
                factory,
                stop_event=stop_event,
                save_batch_size=save_batch_size,
            )
            counts[cid] = n
    else:
        with ThreadPoolExecutor(max_workers=len(active)) as pool:
            futures = {
                pool.submit(
                    _scrape_one,
                    c,
                    catalogue,
                    factory,
                    stop_event,
                    save_batch_size,
                ): c["id"]
                for c in active
            }
            try:
                for future in as_completed(futures):
                    try:
                        cid, n = future.result()
                        counts[cid] = n
                    except Exception:
                        cid = futures[future]
                        logger.exception("Competitor %s raised an unhandled exception", cid)
                        counts[cid] = 0
            except KeyboardInterrupt:
                stop_event.set()
                logger.warning("Interrupted — waiting for scraper buffers to flush")
                for future, cid in futures.items():
                    try:
                        result_cid, n = future.result()
                        counts[result_cid] = n
                    except Exception:
                        logger.exception("Competitor %s failed while shutting down", cid)
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
        help="Scrape only these competitor IDs (e.g. --only strendpro_sk boukal_cz)",
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

    stop_event = threading.Event()
    restore_handlers = _install_shutdown_handlers(stop_event)
    try:
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
            result = main(
                catalogue_path=args.catalogue,
                only=args.only,
                sequential=args.sequential,
                stop_event=stop_event,
            )
    finally:
        restore_handlers()

    for cid, n in result.items():
        print(f"  {cid}: {n} listings")
