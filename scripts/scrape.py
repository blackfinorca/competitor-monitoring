"""Production scraping CLI.

Fetches ToolZone products (as the baseline own-store), immediately searches
active competitors and runs matching for each product as it is scraped.
Up to --workers parallel workers process the URL queue concurrently.

Examples
--------
# Scrape up to 100 Knipex products, 4 parallel workers (default):
    python scripts/scrape.py --query knipex --limit 100

# Conservative: 2 workers to reduce load on target sites:
    python scripts/scrape.py --query knipex --limit 100 --workers 2

# Scrape all Wera products, only check AH Profi and DoktorKladivo:
    python scripts/scrape.py --query wera --competitors ahprofi_sk,doktorkladivo_sk

# Multiple keyword filters (OR logic — URL must contain at least one):
    python scripts/scrape.py --query knipex --query wera --limit 50

# Dry-run: see which URLs would be fetched without making any requests:
    python scripts/scrape.py --query knipex --limit 20 --dry-run

# Scrape products, skip competitor search (ToolZone baseline only):
    python scripts/scrape.py --query knipex --limit 50 --own-store-only

# Scrape everything on ToolZone (slow — ~41k products):
    python scripts/scrape.py --limit 500
"""

from __future__ import annotations

import argparse
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from sqlalchemy import select as sa_select
from sqlalchemy.orm import sessionmaker

from agnaradie_pricing.db.models import Base, Product
from agnaradie_pricing.db.models import CompetitorListing as ListingRow, ProductMatch
from agnaradie_pricing.db.session import make_engine
from agnaradie_pricing.matching import match_product
from agnaradie_pricing.pricing.recommender import build_recommendations
from agnaradie_pricing.pricing.snapshot import build_snapshots
from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.base import CompetitorListing
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.persistence import save_competitor_listings
from agnaradie_pricing.scrapers.toolzone import ToolZoneScraper, _parse_product_page
from agnaradie_pricing.settings import Settings, load_competitors

# ---------------------------------------------------------------------------
# Competitor registry — add new scrapers here
# ---------------------------------------------------------------------------

_SCRAPER_REGISTRY = {
    "ahprofi_sk": AhProfiScraper,
    "naradieshop_sk": NaradieShopScraper,
    "doktorkladivo_sk": DoktorKladivoScraper,
}

_MPN_RE = re.compile(r"\b(\d{2}-\d{2}-\d{3}|\d{6,8})\b")


# ---------------------------------------------------------------------------
# Step 1 — ToolZone: discover URLs
# ---------------------------------------------------------------------------

def _fetch_toolzone_urls(keywords: list[str], limit: int | None) -> list[str]:
    """Pull the ToolZone sitemap and filter by keyword substrings (OR logic)."""
    from agnaradie_pricing.scrapers.http import polite_get, make_client

    client = make_client(timeout=20.0)
    print("Fetching ToolZone sitemap …")
    resp = polite_get(client, "https://www.toolzone.sk/sitemap.xml", min_rps=0.2, jitter=1.0)
    resp.raise_for_status()

    all_urls = re.findall(
        r"<loc>(https://www\.toolzone\.sk/produkt/[^<]+)</loc>",
        resp.text,
    )
    print(f"  → {len(all_urls):,} product URLs in sitemap")

    if keywords:
        lower_kw = [k.lower() for k in keywords]
        all_urls = [u for u in all_urls if any(kw in u.lower() for kw in lower_kw)]
        print(f"  → {len(all_urls):,} match keyword filter: {keywords}")

    if limit:
        all_urls = all_urls[:limit]
        print(f"  → capped at {limit}")

    return all_urls


def _mpn_from_url(url: str) -> str | None:
    slug = url.split("/produkt/")[1].replace(".htm", "")
    codes = _MPN_RE.findall(slug)
    return codes[0] if codes else None


# ---------------------------------------------------------------------------
# Helpers shared by worker and manual rerun
# ---------------------------------------------------------------------------

def _titles_match(t1: str, t2: str, min_common: int = 2) -> bool:
    stop = frozenset("na pre s z a so pri od do mm".split())

    def tok(t: str) -> set[str]:
        return set(re.findall(r"[a-záäčďéíľĺňóôŕšťúýž0-9]{3,}", t.lower())) - stop

    return len(tok(t1) & tok(t2)) >= min_common


def upsert_products(session, listings: list[CompetitorListing]) -> None:
    for listing in listings:
        sku = f"TZ-{listing.mpn or listing.ean or listing.title[:20]}"
        existing = session.scalars(sa_select(Product).where(Product.sku == sku)).first()
        if existing is None:
            existing = Product(sku=sku, title=listing.title)
            session.add(existing)
            session.flush()

        existing.brand = listing.brand
        existing.mpn = listing.mpn
        existing.ean = listing.ean
        existing.title = listing.title
        existing.category = "Ruční nářadí"
        existing.price_eur = Decimal(str(listing.price_eur))

    session.flush()


def _save_match_row(session, product: dict, listing, result: tuple) -> None:
    match_type, confidence = result
    if session.scalars(
        sa_select(ProductMatch).where(
            ProductMatch.ag_product_id == product["id"],
            ProductMatch.competitor_id == listing.competitor_id,
            ProductMatch.competitor_sku == listing.competitor_sku,
        )
    ).first() is None:
        session.add(ProductMatch(
            ag_product_id=product["id"],
            competitor_id=listing.competitor_id,
            competitor_sku=listing.competitor_sku,
            match_type=match_type,
            confidence=Decimal(str(confidence)),
        ))


def _save_match_raw_row(session, product: dict, listing_dict: dict, result: tuple) -> None:
    match_type, confidence = result
    if session.scalars(
        sa_select(ProductMatch).where(
            ProductMatch.ag_product_id == product["id"],
            ProductMatch.competitor_id == listing_dict["competitor_id"],
            ProductMatch.competitor_sku == listing_dict["competitor_sku"],
        )
    ).first() is None:
        session.add(ProductMatch(
            ag_product_id=product["id"],
            competitor_id=listing_dict["competitor_id"],
            competitor_sku=listing_dict["competitor_sku"],
            match_type=match_type,
            confidence=Decimal(str(confidence)),
        ))


# ---------------------------------------------------------------------------
# Per-product worker — runs in a thread pool
# ---------------------------------------------------------------------------

def _process_product_url(
    url: str,
    competitor_ids: list[str],
    config_map: dict,
    factory,
    db_write_lock: threading.Lock,
    qwen_api_key: str | None,
    qwen_model: str,
    print_lock: threading.Lock,
    worker_id: int,
    index: int,
    total: int,
) -> dict:
    """Full pipeline for one ToolZone product URL.

    Creates its own HTTP clients, scraper instances, DB session, and LLM
    client so nothing is shared across worker threads.

    HTTP work (ToolZone scrape + all competitor searches) runs without any
    lock so all workers proceed in parallel.  DB writes are serialised via
    db_write_lock so SQLite never sees concurrent writers (avoids
    SQLITE_LOCKED which cannot be resolved by a timeout).

    Returns {"scraped": bool, "competitors_found": int, "matched": int}.
    """
    from agnaradie_pricing.scrapers.http import make_client, polite_get

    try:
        # ----------------------------------------------------------------
        # Worker-local resources — never shared across threads
        # ----------------------------------------------------------------
        tz_client = make_client(timeout=15.0)

        llm_client = None
        if qwen_api_key:
            from agnaradie_pricing.matching.llm_matcher import QwenClient
            llm_client = QwenClient(api_key=qwen_api_key, model=qwen_model)

        competitor_scrapers = {}
        for cid in competitor_ids:
            cls = _SCRAPER_REGISTRY.get(cid)
            if cls is None:
                continue
            cfg = config_map.get(cid, {"id": cid, "url": "", "weight": 1.0, "rate_limit_rps": 1})
            competitor_scrapers[cid] = cls(cfg, http_client=make_client())

        # ----------------------------------------------------------------
        # Phase 1 — HTTP: scrape ToolZone product page (no lock)
        # ----------------------------------------------------------------
        try:
            resp = polite_get(
                tz_client, url, min_rps=1.0, referer="https://www.toolzone.sk/"
            )
            listing = _parse_product_page(resp.text, "toolzone_sk", url)
        except Exception as exc:
            with print_lock:
                print(f"[W{worker_id}] [{index:>4}/{total}] ✗  scrape error: {exc}  {url}")
            return {"scraped": False, "competitors_found": 0, "matched": 0}

        if listing is None:
            with print_lock:
                print(f"[W{worker_id}] [{index:>4}/{total}] ✗  parse failed: {url}")
            return {"scraped": False, "competitors_found": 0, "matched": 0}

        # Inject MPN from URL slug if not in JSON-LD
        mpn = _mpn_from_url(url)
        if mpn and not listing.mpn:
            from dataclasses import asdict
            listing = listing.__class__(**{**asdict(listing), "mpn": mpn})

        # ----------------------------------------------------------------
        # Phase 2 — HTTP: search all competitors in parallel (no lock)
        # ----------------------------------------------------------------
        competitor_hits: dict[str, CompetitorListing | None] = {}
        for cid, scraper in competitor_scrapers.items():
            hit: CompetitorListing | None = None

            # Strategy 1: EAN or MPN search (most precise)
            for term in filter(None, [listing.ean, listing.mpn]):
                try:
                    r = scraper.search_by_query(term)
                    if r and _titles_match(listing.title, r.title):
                        hit = r
                        break
                except Exception:
                    pass

            # Strategy 2: brand + MPN (standard)
            if hit is None and listing.brand and listing.mpn:
                try:
                    r = scraper.search_by_mpn(listing.brand, listing.mpn)
                    if r and _titles_match(listing.title, r.title):
                        hit = r
                except Exception:
                    pass

            # Strategy 3: title fragment fallback
            if hit is None:
                try:
                    r = scraper.search_by_query(listing.title[:60])
                    if r and _titles_match(listing.title, r.title):
                        hit = r
                except Exception:
                    pass

            competitor_hits[cid] = hit

        # ----------------------------------------------------------------
        # Phase 3 — DB: all writes in one session, serialised via lock
        # ----------------------------------------------------------------
        competitors_found = 0
        matched = 0
        competitor_statuses: dict[str, str] = {}

        with db_write_lock:
            with factory() as session:
                # Save ToolZone product + listing
                upsert_products(session, [listing])
                save_competitor_listings(session, [listing])
                session.flush()

                # Retrieve the upserted product row to get its DB id
                sku = f"TZ-{listing.mpn or listing.ean or listing.title[:20]}"
                product_row = session.scalars(
                    sa_select(Product).where(Product.sku == sku)
                ).first()
                product_dict = {
                    "id": product_row.id,
                    "brand": product_row.brand,
                    "mpn": product_row.mpn,
                    "ean": product_row.ean,
                    "title": product_row.title,
                }

                # Save competitor hits and run matching
                for cid, hit in competitor_hits.items():
                    if hit is None:
                        competitor_statuses[cid] = "—"
                        continue

                    competitors_found += 1
                    rows = save_competitor_listings(session, [hit])
                    session.flush()
                    comp_row = rows[0]

                    listing_dict = {
                        "brand": hit.brand,
                        "mpn": hit.mpn,
                        "ean": hit.ean,
                        "title": hit.title,
                    }
                    result = match_product(product_dict, listing_dict, llm_client=llm_client)
                    if result:
                        _save_match_row(session, product_dict, comp_row, result)
                        matched += 1
                        competitor_statuses[cid] = f"✓ {result[0]}"
                    else:
                        competitor_statuses[cid] = "~ no match"

                session.commit()

        # ----------------------------------------------------------------
        # Progress line (thread-safe)
        # ----------------------------------------------------------------
        status_parts = "  ".join(
            f"{cid.replace('_sk', '')[:8]} {s}"
            for cid, s in competitor_statuses.items()
        )
        with print_lock:
            print(
                f"[W{worker_id}]  [{index:>4}/{total}]  ✓  "
                f"{listing.title[:48]:<48}  €{listing.price_eur:.2f}"
                + (f"  | {status_parts}" if status_parts else "")
            )

        return {"scraped": True, "competitors_found": competitors_found, "matched": matched}

    except Exception as exc:
        with print_lock:
            print(f"[W{worker_id}] [{index:>4}/{total}] FATAL  {type(exc).__name__}: {exc}  {url}")
        return {"scraped": False, "competitors_found": 0, "matched": 0}


# ---------------------------------------------------------------------------
# Manual catch-all matching (used for reruns / --no-pipeline manual mode)
# ---------------------------------------------------------------------------

def run_matching(session, llm_client=None) -> tuple[int, int]:
    """Match all unmatched listings against the product catalogue.

    Kept for manual reruns.  In the default pipeline, matching now happens
    inline inside _process_product_url() for freshly scraped listings.

    Returns (deterministic_count, llm_count).
    """
    from agnaradie_pricing.matching import match_product_bulk

    products = session.scalars(sa_select(Product)).all()
    product_list = [
        {"id": p.id, "brand": p.brand, "mpn": p.mpn, "ean": p.ean, "title": p.title}
        for p in products
    ]

    already_matched: set[tuple] = set(
        session.execute(
            sa_select(ProductMatch.competitor_id, ProductMatch.competitor_sku)
        ).all()
    )

    det_matched = 0
    unmatched_for_llm: list[dict] = []

    all_listings = session.scalars(sa_select(ListingRow)).all()
    for listing in all_listings:
        if (listing.competitor_id, listing.competitor_sku) in already_matched:
            continue

        listing_dict = {
            "brand": listing.brand,
            "mpn": listing.mpn,
            "ean": listing.ean,
            "title": listing.title,
        }

        best_match = best_result = None
        for product in product_list:
            result = match_product(product, listing_dict)
            if result and (best_result is None or result[1] > best_result[1]):
                best_match, best_result = product, result
            if best_result and best_result[1] == 1.0:
                break

        if best_match:
            _save_match_row(session, best_match, listing, best_result)
            det_matched += 1
            print(
                f"    [{best_result[0]:<22}  {best_result[1]:.0%}]  "
                f"{listing.competitor_id:<25}  {listing.title[:45]}"
            )
        elif llm_client is not None:
            unmatched_for_llm.append({
                "id": listing.id,
                "brand": listing.brand,
                "mpn": listing.mpn,
                "ean": listing.ean,
                "title": listing.title,
                "competitor_id": listing.competitor_id,
                "competitor_sku": listing.competitor_sku,
            })

    llm_matched = 0
    if llm_client is not None and unmatched_for_llm:
        print(f"\n  [LLM] {len(unmatched_for_llm)} unmatched listings → Qwen …")
        llm_results = match_product_bulk(
            unmatched_for_llm, product_list, llm_client=llm_client
        )
        listing_by_id = {l["id"]: l for l in unmatched_for_llm}
        for listing_id, (matched_product, match_result) in llm_results.items():
            raw = listing_by_id[listing_id]
            _save_match_raw_row(session, matched_product, raw, match_result)
            llm_matched += 1
            print(
                f"    [llm_fuzzy  {match_result[1]:.0%}]  "
                f"{raw['competitor_id']:<25}  {raw['title'][:45]}"
            )

    return det_matched, llm_matched


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="scrape",
        description="Scrape ToolZone products and search competitors.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--query", "-q",
        metavar="KEYWORD",
        action="append",
        default=[],
        help=(
            "Filter ToolZone products by keyword in the URL slug (OR logic). "
            "Can be repeated: --query knipex --query wera"
        ),
    )
    p.add_argument(
        "--limit", "-n",
        metavar="N",
        type=int,
        default=None,
        help="Max number of ToolZone products to scrape. Default: no limit.",
    )
    p.add_argument(
        "--workers", "-w",
        metavar="N",
        type=int,
        default=4,
        help=(
            "Number of parallel scraping workers (default: 4). "
            "Each worker has its own HTTP clients, DB session, and LLM client. "
            "Use --workers 1 to run single-threaded."
        ),
    )
    p.add_argument(
        "--competitors", "-c",
        metavar="IDS",
        default=None,
        help=(
            "Comma-separated competitor IDs to search. "
            "Default: all active competitors from config/competitors.yaml. "
            "Example: ahprofi_sk,naradieshop_sk"
        ),
    )
    p.add_argument(
        "--own-store-only",
        action="store_true",
        help="Only scrape ToolZone (baseline). Skip competitor search and matching.",
    )
    p.add_argument(
        "--no-pipeline",
        action="store_true",
        help=(
            "Skip snapshot and recommendation steps after scraping. "
            "Note: inline matching still runs per product. "
            "Use --own-store-only to also skip competitor search."
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover URLs and print what would be scraped. No HTTP requests to product pages.",
    )
    p.add_argument(
        "--no-llm",
        action="store_true",
        help=(
            "Disable the LLM fuzzy matching layer (layers 1–5 only). "
            "By default the LLM runs automatically when QWEN_API_KEY is set."
        ),
    )
    return p


def main(argv: Sequence[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    settings = Settings()
    all_configs = load_competitors()

    # LLM credentials — workers create their own QwenClient instances
    qwen_api_key: str | None = None
    qwen_model: str = getattr(settings, "qwen_model", "qwen3-235b-a22b") or "qwen3-235b-a22b"
    if not args.no_llm:
        qwen_api_key = getattr(settings, "qwen_api_key", None)
        if qwen_api_key:
            print(f"LLM matching: enabled  (model: {qwen_model}, per-worker clients)")
        else:
            print("LLM matching: disabled (QWEN_API_KEY not set — add to .env to enable)")
    else:
        print("LLM matching: disabled (--no-llm)")

    # Resolve competitor IDs
    if args.own_store_only:
        competitor_ids: list[str] = []
    elif args.competitors:
        competitor_ids = [c.strip() for c in args.competitors.split(",") if c.strip()]
    else:
        competitor_ids = [
            c["id"]
            for c in all_configs
            if not c.get("own_store") and c["id"] in _SCRAPER_REGISTRY
        ]

    config_map = {c["id"]: c for c in all_configs}

    # --- Discover ToolZone URLs (always sequential) ---
    urls = _fetch_toolzone_urls(args.query, args.limit)

    if not urls:
        print("No product URLs matched — nothing to scrape.")
        return

    if args.dry_run:
        print(f"\n[DRY RUN] Would scrape {len(urls)} ToolZone products:")
        for u in urls[:20]:
            print(f"  {u}")
        if len(urls) > 20:
            print(f"  … and {len(urls) - 20} more")
        if competitor_ids:
            print(f"\nWould search competitors: {', '.join(competitor_ids)}")
        else:
            print("\nNo competitor search (--own-store-only).")
        return

    # --- DB setup ---
    engine = make_engine(settings)

    # SQLite: enable WAL mode and allow cross-thread connection use
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

    # --- Parallel per-product pipeline ---
    total = len(urls)
    n_workers = args.workers
    print(
        f"\nProcessing {total} product{'s' if total != 1 else ''} "
        f"with {n_workers} worker{'s' if n_workers != 1 else ''} …"
        f"  competitors: {', '.join(competitor_ids) or 'none'}\n"
    )

    print_lock = threading.Lock()
    db_write_lock = threading.Lock()  # serialises SQLite writes; HTTP runs lock-free
    totals = {"scraped": 0, "competitors_found": 0, "matched": 0}

    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(
                _process_product_url,
                url,
                competitor_ids,
                config_map,
                factory,
                db_write_lock,
                qwen_api_key,
                qwen_model,
                print_lock,
                (i % n_workers) + 1,  # worker_id: cycles 1..n_workers
                i,
                total,
            ): url
            for i, url in enumerate(urls, 1)
        }

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                # Should not reach here (worker catches internally), but be safe
                url = futures[future]
                with print_lock:
                    print(f"[POOL] Unhandled: {exc}  {url}")
                result = {"scraped": False, "competitors_found": 0, "matched": 0}

            totals["scraped"]           += int(result["scraped"])
            totals["competitors_found"] += result["competitors_found"]
            totals["matched"]           += result["matched"]

    # --- Post-processing (sequential, after all workers finish) ---
    if args.no_pipeline:
        print("\nSkipping snapshot/recommend (--no-pipeline).")
    else:
        print("\nRunning snapshot → recommend …")
        with factory() as session:
            n_snap = build_snapshots(session)
            session.commit()
            print(f"  → {n_snap} pricing snapshots built")

            n_rec = build_recommendations(session)
            session.commit()
            print(f"  → {n_rec} recommendations generated")

    # --- Summary ---
    print("\n" + "─" * 60)
    print(f"  Products scraped   : {totals['scraped']}/{total}")
    if competitor_ids:
        print(f"  Competitor hits    : {totals['competitors_found']}")
        print(f"  Matches saved      : {totals['matched']}")
    if qwen_api_key:
        print(f"  LLM model          : {qwen_model}")
    print("─" * 60)
    print("✓  Done.  Dashboard: http://localhost:8501")


if __name__ == "__main__":
    main()
