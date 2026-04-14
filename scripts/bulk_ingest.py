"""Bulk ingest — fastest path to populate the full 41k-product catalogue.

Strategy
--------
1. ToolZone baseline   — no Heureka feed available; scrapes 41k product pages
                          via the sitemap using 16 parallel workers (~50 min).

2. Competitor feeds    — for every competitor site, probe HEUREKA_FEED_PATHS.
                          Sites that return a feed (madmat.sk, centrumnaradia.sk,
                          and any future additions) are downloaded in one request
                          and saved in bulk.  All feed-capable sites run in parallel.

3. Bulk matching       — after all feeds are saved, match competitor listings
                          against the AG product catalogue in-memory (no HTTP).

4. Snapshots + recs    — build pricing snapshots and recommendations.

Sites without a Heureka feed (ahprofi, naradieshop, doktorkladivo, ferant) are
skipped in this script — use `scripts/scrape.py` for incremental per-product
enrichment of those competitors.

Examples
--------
# Full bulk ingest (ToolZone + all competitor feeds + matching):
    python scripts/bulk_ingest.py

# ToolZone baseline only (skip competitor feeds and matching):
    python scripts/bulk_ingest.py --own-store-only

# Use more / fewer ToolZone workers:
    python scripts/bulk_ingest.py --workers 8

# Skip LLM matching:
    python scripts/bulk_ingest.py --no-llm
"""

from __future__ import annotations

import argparse
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Sequence

import httpx

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy.orm import sessionmaker

from agnaradie_pricing.db.models import Base
from agnaradie_pricing.db.session import make_engine
from agnaradie_pricing.pricing.recommender import build_recommendations
from agnaradie_pricing.pricing.snapshot import build_snapshots
from agnaradie_pricing.scrapers.heureka_feed import parse_heureka_feed
from agnaradie_pricing.scrapers.http import make_client
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS
from agnaradie_pricing.scrapers.persistence import save_competitor_listings
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper
from agnaradie_pricing.settings import Settings, load_competitors

# ---------------------------------------------------------------------------
# Import reusable helpers from scrape.py
# ---------------------------------------------------------------------------
# These functions are already implemented and tested there; no need to duplicate.
sys.path.insert(0, str(Path(__file__).parent))
from scrape import (  # noqa: E402
    _fetch_toolzone_urls,
    _process_product_url,
    run_matching,
    upsert_products,
)


# ---------------------------------------------------------------------------
# Phase 1 — ToolZone baseline (sitemap + worker pool)
# ---------------------------------------------------------------------------

def ingest_toolzone(
    factory,
    db_write_lock: threading.Lock,
    settings: Settings,
    workers: int,
    keywords: list[str],
    limit: int | None,
    qwen_api_key: str | None,
    qwen_model: str,
) -> int:
    """Scrape ToolZone product pages via the sitemap, own-store-only mode.

    Returns the number of products successfully scraped.
    """
    urls = _fetch_toolzone_urls(keywords, limit)
    if not urls:
        print("  → no ToolZone URLs matched — skipping.")
        return 0

    total = len(urls)
    print_lock = threading.Lock()
    scraped = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _process_product_url,
                url,
                [],           # competitor_ids=[] → own-store-only
                {},           # config_map (unused without competitors)
                factory,
                db_write_lock,
                qwen_api_key,
                qwen_model,
                print_lock,
                (i % workers) + 1,
                i,
                total,
            ): url
            for i, url in enumerate(urls, 1)
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                scraped += int(result["scraped"])
            except Exception as exc:
                url = futures[future]
                with print_lock:
                    print(f"  [POOL] unhandled: {exc}  {url}")

    return scraped


# ---------------------------------------------------------------------------
# Phase 2 — Competitor feeds
# ---------------------------------------------------------------------------

def _try_heureka_feed(base_url: str, client: httpx.Client) -> bytes | None:
    """Probe HEUREKA_FEED_PATHS; return raw XML bytes if a feed is found."""
    for path in HEUREKA_FEED_PATHS:
        url = base_url.rstrip("/") + path
        try:
            r = client.get(url, follow_redirects=True, timeout=20)
            if r.status_code == 200 and "SHOPITEM" in r.text:
                return r.content
        except Exception:
            pass
    return None


def ingest_competitor_feed(
    cid: str,
    base_url: str,
    factory,
    db_write_lock: threading.Lock,
) -> tuple[str, int]:
    """Download a competitor's Heureka feed and bulk-save all listings.

    Returns (cid, count_saved).  Returns (cid, 0) if no feed found.
    """
    client = make_client(timeout=20.0)
    xml = _try_heureka_feed(base_url, client)

    # Also try scraper discover_feed() as a secondary probe
    if xml is None:
        scraper = ShoptetGenericScraper(
            {"id": cid, "url": base_url, "weight": 1.0, "rate_limit_rps": 1},
            http_client=client,
        )
        feed_url = scraper.discover_feed()
        if feed_url:
            try:
                r = client.get(feed_url, follow_redirects=True, timeout=30)
                r.raise_for_status()
                if "SHOPITEM" in r.text:
                    xml = r.content
            except Exception:
                pass

    if xml is None:
        return cid, 0

    listings = parse_heureka_feed(xml, cid)
    if not listings:
        return cid, 0

    with db_write_lock:
        with factory() as session:
            save_competitor_listings(session, listings)
            session.commit()

    return cid, len(listings)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bulk_ingest",
        description="Bulk-ingest ToolZone (sitemap) + competitor Heureka feeds.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--workers", "-w",
        metavar="N",
        type=int,
        default=16,
        help="Parallel workers for ToolZone sitemap scraping (default: 16).",
    )
    p.add_argument(
        "--own-store-only",
        action="store_true",
        help="Only scrape ToolZone. Skip competitor feeds and matching.",
    )
    p.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM fuzzy matching (even if QWEN_API_KEY is set).",
    )
    p.add_argument(
        "--no-pipeline",
        action="store_true",
        help="Skip snapshot and recommendation steps after matching.",
    )
    p.add_argument(
        "--query", "-q",
        metavar="KEYWORD",
        action="append",
        default=[],
        help="Filter ToolZone URLs by keyword (OR logic). Default: all products.",
    )
    p.add_argument(
        "--limit", "-n",
        metavar="N",
        type=int,
        default=None,
        help="Cap ToolZone product count (for testing).",
    )
    return p


def main(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    settings = Settings()
    all_configs = load_competitors()
    config_map = {c["id"]: c for c in all_configs}

    qwen_api_key: str | None = None
    qwen_model: str = getattr(settings, "qwen_model", "qwen3-235b-a22b") or "qwen3-235b-a22b"
    if not args.no_llm:
        qwen_api_key = getattr(settings, "qwen_api_key", None)

    # --- DB setup ---
    engine = make_engine(settings)
    if settings.database_url.startswith("sqlite"):
        from sqlalchemy import create_engine as _ce, event as _ev
        engine = _ce(
            settings.database_url,
            pool_pre_ping=True,
            connect_args={"check_same_thread": False, "timeout": 30},
        )

        @_ev.listens_for(engine, "connect")
        def _set_wal(conn, _rec):
            conn.execute("PRAGMA journal_mode=WAL")

    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    db_write_lock = threading.Lock()

    # -----------------------------------------------------------------------
    # Phase 1 — ToolZone baseline
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Phase 1 — ToolZone baseline  ({args.workers} workers)")
    print(f"{'='*60}")
    tz_count = ingest_toolzone(
        factory, db_write_lock, settings,
        workers=args.workers,
        keywords=args.query,
        limit=args.limit,
        qwen_api_key=None,   # own-store-only: no LLM needed during scrape
        qwen_model=qwen_model,
    )
    print(f"\n  → {tz_count:,} ToolZone products saved")

    if args.own_store_only:
        print("\nSkipping competitor feeds (--own-store-only).")
        _run_post_processing(args, factory)
        return

    # -----------------------------------------------------------------------
    # Phase 2 — Competitor Heureka feeds (parallel)
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Phase 2 — Competitor feeds")
    print(f"{'='*60}")

    competitor_configs = [c for c in all_configs if not c.get("own_store")]
    feed_totals: dict[str, int] = {}
    no_feed: list[str] = []

    with ThreadPoolExecutor(max_workers=len(competitor_configs)) as pool:
        futures = {
            pool.submit(
                ingest_competitor_feed,
                c["id"],
                c["url"],
                factory,
                db_write_lock,
            ): c["id"]
            for c in competitor_configs
        }
        for future in as_completed(futures):
            cid = futures[future]
            try:
                _, count = future.result()
                if count > 0:
                    feed_totals[cid] = count
                    print(f"  ✓  {cid:<30}  {count:>6,} listings")
                else:
                    no_feed.append(cid)
                    print(f"  —  {cid:<30}  no Heureka feed")
            except Exception as exc:
                no_feed.append(cid)
                print(f"  ✗  {cid:<30}  error: {exc}")

    if no_feed:
        print(
            f"\n  Note: {len(no_feed)} competitor(s) have no Heureka feed:"
            f" {', '.join(no_feed)}\n"
            f"  Run  python scripts/scrape.py --workers 4  for per-product"
            f" enrichment of those sites."
        )

    # -----------------------------------------------------------------------
    # Phase 3 — Bulk matching
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("Phase 3 — Bulk matching")
    print(f"{'='*60}")

    llm_client = None
    if qwen_api_key:
        from agnaradie_pricing.matching.llm_matcher import QwenClient
        llm_client = QwenClient(api_key=qwen_api_key, model=qwen_model)
        print(f"  LLM: enabled  (model: {qwen_model})")
    else:
        print("  LLM: disabled")

    with factory() as session:
        det, llm = run_matching(session, llm_client=llm_client)
        session.commit()
    print(f"  → {det} deterministic matches,  {llm} LLM matches")

    # -----------------------------------------------------------------------
    # Phase 4 — Snapshots + recommendations
    # -----------------------------------------------------------------------
    _run_post_processing(args, factory)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print("\n" + "─" * 60)
    print(f"  ToolZone products   : {tz_count:,}")
    for cid, n in feed_totals.items():
        print(f"  {cid:<20}: {n:,} listings")
    print(f"  Deterministic match : {det}")
    print(f"  LLM matches         : {llm}")
    print("─" * 60)
    print("✓  Done.  Dashboard: http://localhost:8501")


def _run_post_processing(args, factory) -> None:
    if args.no_pipeline:
        print("\nSkipping snapshot/recommend (--no-pipeline).")
        return
    print(f"\n{'='*60}")
    print("Phase 4 — Snapshots + recommendations")
    print(f"{'='*60}")
    with factory() as session:
        n_snap = build_snapshots(session)
        session.commit()
        print(f"  → {n_snap} snapshots")
    with factory() as session:
        n_rec = build_recommendations(session)
        session.commit()
        print(f"  → {n_rec} recommendations")


if __name__ == "__main__":
    main()
