"""Smoke test — exercises the full pipeline end-to-end.

Quick mode (default)
--------------------
  • Settings / env vars present
  • Database connectivity + product / listing counts
  • Each active scraper: search_by_mpn for one known product
  • Feed competitors: discover_feed reachability check
  • Matching: run matching layers on any listings found

Full mode  (--full)
-------------------
  Everything above, plus:
  • daily_scrape  — scrape one brand (--brand slug, default: knipex) for all competitors
  • daily_match   — layers 1-5 on new listings
  • daily_recommend — snapshots + recommendations
  • export_prices — CSV export

Usage
-----
    python scripts/smoke_test.py
    python scripts/smoke_test.py --full
    python scripts/smoke_test.py --full --brand wera --mpn 05059290001
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from dotenv import load_dotenv as _ld
    _ld(dotenv_path=Path(__file__).parent.parent / ".env", override=False)
except ModuleNotFoundError:
    pass

# ---------------------------------------------------------------------------
# Colours (no external deps)
# ---------------------------------------------------------------------------

_GREEN  = "\033[32m"
_RED    = "\033[31m"
_YELLOW = "\033[33m"
_CYAN   = "\033[36m"
_RESET  = "\033[0m"
_BOLD   = "\033[1m"

def _ok(msg: str)   -> str: return f"{_GREEN}  PASS{_RESET}  {msg}"
def _fail(msg: str) -> str: return f"{_RED}  FAIL{_RESET}  {msg}"
def _warn(msg: str) -> str: return f"{_YELLOW}  WARN{_RESET}  {msg}"
def _head(msg: str) -> str: return f"\n{_BOLD}{_CYAN}{msg}{_RESET}"


# ---------------------------------------------------------------------------
# Result collector
# ---------------------------------------------------------------------------

class Results:
    def __init__(self):
        self._rows: list[tuple[str, bool | None, str]] = []  # (label, ok, detail)

    def ok(self,   label: str, detail: str = "") -> None: self._rows.append((label, True,  detail)); print(_ok  (f"{label}  {detail}"))
    def fail(self, label: str, detail: str = "") -> None: self._rows.append((label, False, detail)); print(_fail(f"{label}  {detail}"))
    def warn(self, label: str, detail: str = "") -> None: self._rows.append((label, None,  detail)); print(_warn(f"{label}  {detail}"))

    def summary(self) -> int:
        passed  = sum(1 for _, s, _ in self._rows if s is True)
        failed  = sum(1 for _, s, _ in self._rows if s is False)
        warned  = sum(1 for _, s, _ in self._rows if s is None)
        print(f"\n{'─'*55}")
        print(f"  {_GREEN}{passed} passed{_RESET}   {_RED}{failed} failed{_RESET}   {_YELLOW}{warned} warnings{_RESET}")
        print(f"{'─'*55}")
        return failed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _elapsed(t0: float) -> str:
    return f"{time.monotonic() - t0:.1f}s"


# ---------------------------------------------------------------------------
# Quick checks
# ---------------------------------------------------------------------------

def check_settings(r: Results) -> None:
    print(_head("Settings"))
    from agnaradie_pricing.settings import Settings
    try:
        s = Settings()
        r.ok("Settings load")
    except Exception as exc:
        r.fail("Settings load", str(exc)); return

    if s.database_url:
        r.ok("DATABASE_URL set", s.database_url[:40] + "…" if len(s.database_url) > 40 else s.database_url)
    else:
        r.fail("DATABASE_URL not set")

    if s.openai_api_key:
        masked = s.openai_api_key[:8] + "…"
        r.ok("OPENAI_API_KEY set", f"{masked}  model={s.openai_model}")
    else:
        r.warn("OPENAI_API_KEY not set — LLM layer (--llm) will be disabled")


def check_database(r: Results) -> None:
    print(_head("Database"))
    from agnaradie_pricing.db.models import Product, CompetitorListing, ProductMatch
    from agnaradie_pricing.db.session import make_session_factory
    from agnaradie_pricing.settings import Settings
    from sqlalchemy import select, func

    try:
        factory = make_session_factory(Settings())
        with factory() as session:
            n_products  = session.scalar(select(func.count(Product.id))) or 0
            n_listings  = session.scalar(select(func.count(CompetitorListing.id))) or 0
            n_matches   = session.scalar(select(func.count(ProductMatch.id))) or 0
        r.ok("DB connection")
        r.ok("Products",  f"{n_products:,}")
        r.ok("Listings",  f"{n_listings:,}")
        r.ok("Matches",   f"{n_matches:,}")
    except Exception as exc:
        r.fail("DB connection", str(exc))


def check_scrapers(r: Results, brand: str, mpn: str) -> None:
    print(_head(f"Scrapers — search_by_mpn({brand!r}, {mpn!r})"))

    from agnaradie_pricing.settings import load_competitors
    from agnaradie_pricing.scrapers.boukal import BoukalScraper
    from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
    from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
    from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
    from agnaradie_pricing.scrapers.rebiop import RebiopScraper
    from agnaradie_pricing.scrapers.toolzone import ToolZoneScraper
    from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper

    configs = {c["id"]: c for c in load_competitors()}

    # Per-scraper fallback probes: some sites don't stock the default MPN.
    # These are (brand, mpn) pairs known to be listed on each site.
    _FALLBACK: dict[str, tuple[str, str]] = {
        "ahprofi_sk":       ("KNIPEX", "03-02-180"),   # ahprofi doesn't stock 87-01-250
        "doktorkladivo_sk": ("KNIPEX", "87-01-250"),
    }

    # ToolZone search is AJAX — test by scraping the first sitemap URL directly
    print(f"  toolzone_sk  (sitemap probe) …", end=" ", flush=True)
    t0 = time.monotonic()
    try:
        import re as _re
        from agnaradie_pricing.scrapers.http import make_client as _mc, polite_get as _pg
        from agnaradie_pricing.scrapers.toolzone import _parse_product_page as _ppp
        _client = _mc(timeout=15.0)
        _sitemap = _pg(_client, "https://www.toolzone.sk/sitemap.xml", min_rps=0.2)
        _urls = _re.findall(r"<loc>(https://www\.toolzone\.sk/produkt/[^<]+)</loc>", _sitemap.text)
        if _urls:
            _resp = _pg(_client, _urls[0], min_rps=1.0, referer="https://www.toolzone.sk/")
            _listing = _ppp(_resp.text, "toolzone_sk", _urls[0])
            if _listing:
                r.ok("toolzone_sk", f"€{_listing.price_eur:.2f}  {_listing.title[:40]}  [{_elapsed(t0)}]")
            else:
                r.warn("toolzone_sk", f"page parse failed  [{_elapsed(t0)}]")
        else:
            r.fail("toolzone_sk", f"no URLs in sitemap  [{_elapsed(t0)}]")
    except Exception as exc:
        r.fail("toolzone_sk", f"{type(exc).__name__}: {str(exc)[:80]}  [{_elapsed(t0)}]")

    SEARCH = {
        "boukal_cz":        BoukalScraper,
        "ahprofi_sk":       AhProfiScraper,
        "naradieshop_sk":   NaradieShopScraper,
        "doktorkladivo_sk": DoktorKladivoScraper,
        "rebiop_sk":        RebiopScraper,
    }
    FEED = {
        "madmat_sk":        ShoptetGenericScraper,
        "centrumnaradia_sk": ShoptetGenericScraper,
    }

    for cid, cls in SEARCH.items():
        cfg = configs.get(cid, {"id": cid, "url": "", "weight": 1.0, "rate_limit_rps": 1})
        probe_brand, probe_mpn = _FALLBACK.get(cid, (brand, mpn))
        t0 = time.monotonic()
        try:
            scraper = cls(cfg)
            result = scraper.search_by_mpn(probe_brand, probe_mpn)
            if result:
                r.ok(cid, f"€{result.price_eur:.2f}  {result.title[:45]}  [{_elapsed(t0)}]")
            else:
                r.warn(cid, f"no result for {brand} {mpn}  [{_elapsed(t0)}]")
        except Exception as exc:
            r.fail(cid, f"{type(exc).__name__}: {str(exc)[:80]}  [{_elapsed(t0)}]")

    print(_head("Feed competitors — discover_feed()"))
    for cid, cls in FEED.items():
        cfg = configs.get(cid, {"id": cid, "url": "", "weight": 1.0, "rate_limit_rps": 1})
        t0 = time.monotonic()
        try:
            scraper = cls(cfg)
            feed_url = scraper.discover_feed()
            if feed_url:
                r.ok(cid, f"feed found: {feed_url[:60]}  [{_elapsed(t0)}]")
            else:
                r.warn(cid, f"no feed found  [{_elapsed(t0)}]")
        except Exception as exc:
            r.fail(cid, f"{type(exc).__name__}: {str(exc)[:80]}  [{_elapsed(t0)}]")


def check_matching(r: Results, brand: str, mpn: str) -> None:
    print(_head("Matching — layers 1-5"))
    from agnaradie_pricing.matching import match_product
    from agnaradie_pricing.db.models import Product, CompetitorListing
    from agnaradie_pricing.db.session import make_session_factory
    from agnaradie_pricing.settings import Settings
    from sqlalchemy import select

    try:
        factory = make_session_factory(Settings())
        with factory() as session:
            products = session.scalars(select(Product)).all()
            listings = session.scalars(
                select(CompetitorListing).limit(500)
            ).all()

        product_list = [
            {"id": p.id, "brand": p.brand, "mpn": p.mpn, "ean": p.ean, "title": p.title}
            for p in products
        ]

        matched = 0
        for listing in listings:
            ld = {"brand": listing.brand, "mpn": listing.mpn, "ean": listing.ean, "title": listing.title}
            for prod in product_list:
                res = match_product(prod, ld)
                if res:
                    matched += 1
                    break

        rate = matched / len(listings) * 100 if listings else 0
        label = "ok" if rate >= 30 else ("warn" if rate >= 5 else "fail")
        msg = f"{matched}/{len(listings)} matched  ({rate:.1f}%)"
        if label == "ok":
            r.ok("Match sample (500 listings)", msg)
        elif label == "warn":
            r.warn("Match sample (500 listings)", msg + "  — low coverage")
        else:
            r.fail("Match sample (500 listings)", msg + "  — very low coverage")

    except Exception as exc:
        r.fail("Matching", str(exc))


# ---------------------------------------------------------------------------
# Full pipeline steps
# ---------------------------------------------------------------------------

def run_full_pipeline(r: Results, brand: str) -> None:
    print(_head("Full pipeline"))

    # -- Scrape one brand across all competitors --
    print(f"  Running daily_scrape (brand={brand!r}) …")
    t0 = time.monotonic()
    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, "jobs/daily_scrape.py"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        if result.returncode == 0:
            r.ok("daily_scrape", f"[{_elapsed(t0)}]")
        else:
            r.fail("daily_scrape", result.stderr[-200:])
    except Exception as exc:
        r.fail("daily_scrape", str(exc))

    # -- Match --
    print("  Running daily_match (layers 1-5) …")
    t0 = time.monotonic()
    try:
        import subprocess
        result = subprocess.run(
            [sys.executable, "jobs/daily_match.py"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        if result.returncode == 0:
            # Last line of stdout has counts
            output = (result.stdout or "").strip().splitlines()
            summary = "  ".join(output[-4:]) if output else ""
            r.ok("daily_match", f"{summary}  [{_elapsed(t0)}]")
        else:
            r.fail("daily_match", result.stderr[-200:])
    except Exception as exc:
        r.fail("daily_match", str(exc))

    # -- Snapshot + recommend --
    print("  Running daily_recommend …")
    t0 = time.monotonic()
    try:
        from agnaradie_pricing.pricing.snapshot import build_snapshots
        from agnaradie_pricing.pricing.recommender import build_recommendations
        from agnaradie_pricing.db.session import make_session_factory
        from agnaradie_pricing.settings import Settings
        factory = make_session_factory(Settings())
        with factory() as session:
            n_snap = build_snapshots(session)
            session.commit()
            n_rec = build_recommendations(session)
            session.commit()
        r.ok("daily_recommend", f"{n_snap} snapshots  {n_rec} recommendations  [{_elapsed(t0)}]")
    except Exception as exc:
        r.fail("daily_recommend", str(exc))

    # -- Export CSV --
    print("  Running export_prices …")
    t0 = time.monotonic()
    try:
        import subprocess, tempfile
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            out_path = f.name
        result = subprocess.run(
            [sys.executable, "jobs/export_prices.py", "--output", out_path],
            capture_output=True, text=True,
            cwd=str(Path(__file__).parent.parent),
        )
        if result.returncode == 0:
            lines = Path(out_path).read_text(encoding="utf-8-sig").count("\n")
            r.ok("export_prices", f"{lines:,} rows  → {out_path}  [{_elapsed(t0)}]")
        else:
            r.fail("export_prices", result.stderr[-200:])
    except Exception as exc:
        r.fail("export_prices", str(exc))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Smoke test for the AG Naradie pricing pipeline.")
    parser.add_argument("--full",  action="store_true", help="Run full pipeline (scrape → match → recommend → export)")
    parser.add_argument("--brand", default="KNIPEX",    help="Brand to use for scraper probes (default: KNIPEX)")
    parser.add_argument("--mpn",   default="87-01-250", help="MPN to use for search probes (default: 87-01-250)")
    args = parser.parse_args(argv)

    print(f"\n{_BOLD}AG Naradie Pricing — Smoke Test{_RESET}")
    print(f"  brand={args.brand!r}  mpn={args.mpn!r}  full={args.full}")

    r = Results()
    t_start = time.monotonic()

    check_settings(r)
    check_database(r)
    check_scrapers(r, args.brand, args.mpn)
    check_matching(r, args.brand, args.mpn)

    if args.full:
        run_full_pipeline(r, args.brand)

    print(f"\n  Total time: {_elapsed(t_start)}")
    return r.summary()


if __name__ == "__main__":
    sys.exit(main())
