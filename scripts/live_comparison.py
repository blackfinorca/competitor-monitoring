"""Live price comparison: ToolZone (baseline) vs competitors.

Scrapes 20-30 ToolZone products from the sitemap, then searches each
search-capable competitor for the same product using the EAN or a
manufacturer MPN extracted from the product title.

Usage:
    python scripts/live_comparison.py [--brand SLUG] [--limit N]

Examples:
    python scripts/live_comparison.py --brand knipex --limit 25
    python scripts/live_comparison.py --brand bosch --limit 20
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass

# Ensure src/ is on the path when running from the project root
sys.path.insert(0, "src")

import httpx

from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper
from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.naradieshop import NaradieShopScraper
from agnaradie_pricing.scrapers.toolzone import ToolZoneScraper
from agnaradie_pricing.scrapers.base import CompetitorListing


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_BRAND = "knipex"
DEFAULT_LIMIT = 25
RATE_LIMIT_SLEEP = 1.1  # seconds between requests per competitor

SEARCH_COMPETITORS = {
    "ahprofi_sk": AhProfiScraper,
    "naradieshop_sk": NaradieShopScraper,
    "doktorkladivo_sk": DoktorKladivoScraper,
}


# ---------------------------------------------------------------------------
# MPN extraction
# ---------------------------------------------------------------------------

# Manufacturer MPN patterns found in Knipex/Bosch/etc product names and URLs:
#   "87-01-250"  "8701250"  "0021 37"  "97 91 04"  "98 65 10"  "00 2106"
_MPN_TITLE_RE = re.compile(
    r"\b"
    r"(?:"
    r"\d{2,4}(?:[-\s]\d{2,4}){1,3}|"   # groups with dashes/spaces: 87-01-250, 0021 37
    r"\d{6,8}"                           # long digit-only: 8701250
    r")"
    r"\b"
)

# ToolZone URL slugs look like: klieste-cobra-250mm-knipex-8701250.htm
# The manufacturer code often appears just before the brand name or at the end
# before the internal numeric ID.  We grab the LONGEST digit-group in the slug.
_SLUG_CODE_RE = re.compile(r"\b(\d{4,8})\b")


def extract_mpn_from_url(url: str) -> str | None:
    """Extract manufacturer part number from a ToolZone product URL slug."""
    slug = url.split("/")[-1].replace(".htm", "")
    codes = _SLUG_CODE_RE.findall(slug)
    if not codes:
        return None
    # Prefer codes ≥6 digits (more likely to be a product code than a dimension)
    long_codes = [c for c in codes if len(c) >= 6]
    if long_codes:
        return long_codes[0]
    # Fall back to longest available code
    return max(codes, key=len)


def extract_mpn_from_title(title: str, brand: str) -> str | None:
    """Try to pull a manufacturer part number out of the product title."""
    title_clean = re.sub(re.escape(brand), "", title, flags=re.IGNORECASE).strip()
    match = _MPN_TITLE_RE.search(title_clean)
    return match.group(0).strip() if match else None


# ---------------------------------------------------------------------------
# Title similarity validation
# ---------------------------------------------------------------------------

_STOP = frozenset("na pre v s z a na so pri pre od do".split())


def _title_tokens(text: str) -> set[str]:
    tokens = set(re.findall(r"[a-záäčďéíľĺňóôŕšťúýžA-ZÁÄČĎÉÍĽĹŇÓÔŔŠŤÚÝŽ0-9]{3,}", text.lower()))
    return tokens - _STOP


def titles_match(tz_title: str, result_title: str, min_common: int = 2) -> bool:
    """Return True if the two product titles share at least min_common tokens."""
    tz_tokens = _title_tokens(tz_title)
    res_tokens = _title_tokens(result_title)
    common = tz_tokens & res_tokens
    return len(common) >= min_common


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class ComparisonRow:
    toolzone_title: str
    toolzone_ean: str | None
    toolzone_mpn: str | None      # manufacturer MPN extracted from URL/title
    toolzone_price: float
    toolzone_in_stock: bool | None
    toolzone_url: str
    competitor_results: dict[str, CompetitorListing | None]  # competitor_id → listing


# ---------------------------------------------------------------------------
# Core comparison logic
# ---------------------------------------------------------------------------

def _search_one_competitor(
    scraper_cls,
    brand: str,
    tz_title: str,
    ean: str | None,
    url_mpn: str | None,
    title_mpn: str | None,
    sleep: float = RATE_LIMIT_SLEEP,
) -> CompetitorListing | None:
    """Search a single competitor using multiple strategies; validate each result.

    Strategy order (best signal first):
      1. brand + URL-extracted MPN  (most reliable — MPN in slug is manufacturer code)
      2. brand + title-extracted MPN
      3. brand + EAN                (last resort; many sites don't index by barcode)

    Each result is validated against the ToolZone title via token overlap.
    """
    scraper = scraper_cls()

    def _try(label: str, term: str) -> CompetitorListing | None:
        if not term:
            return None
        try:
            time.sleep(sleep)
            result = scraper.search_by_mpn(brand, term)
            if result is None:
                return None
            if not titles_match(tz_title, result.title):
                print(
                    f"      [{scraper.competitor_id}] {label} hit rejected "
                    f"(title mismatch): '{result.title[:50]}'",
                    flush=True,
                )
                return None
            return result
        except Exception as exc:
            print(f"      [{scraper.competitor_id}] {label} error: {exc}", flush=True)
            return None

    return (
        _try("url-mpn", url_mpn or "")
        or _try("title-mpn", title_mpn or "")
        or _try("ean", ean or "")
    )


def run_comparison(brand_slug: str, limit: int) -> list[ComparisonRow]:
    http_client = httpx.Client(timeout=15.0, follow_redirects=True)

    print(f"\n{'='*65}")
    print(f"  ToolZone → Competitor Price Comparison")
    print(f"  Brand filter: {brand_slug!r}   Limit: {limit} products")
    print(f"{'='*65}\n")

    # -----------------------------------------------------------------------
    # Step 1: Scrape ToolZone products
    # -----------------------------------------------------------------------
    print("▶ Fetching ToolZone sitemap & scraping product pages …", flush=True)

    tz_config = {
        "id": "toolzone_sk",
        "name": "ToolZone",
        "url": "https://www.toolzone.sk",
        "rate_limit_rps": 1,
        "brand_slugs": [brand_slug],
    }
    tz_scraper = ToolZoneScraper(config=tz_config, http_client=http_client)

    product_urls = tz_scraper._get_product_urls()
    product_urls = product_urls[:limit]
    print(f"  Found {len(product_urls)} ToolZone URLs matching '{brand_slug}' (capped at {limit})\n")

    toolzone_listings: list[CompetitorListing] = []
    for i, url in enumerate(product_urls, 1):
        try:
            time.sleep(1.0)
            listing = tz_scraper._scrape_product_page(url)
            if listing is not None:
                toolzone_listings.append(listing)
                print(f"  [{i:02d}/{len(product_urls)}] ✓ {listing.title[:55]:<55}  €{listing.price_eur:.2f}", flush=True)
            else:
                print(f"  [{i:02d}/{len(product_urls)}] ✗ No data from {url}", flush=True)
        except Exception as exc:
            print(f"  [{i:02d}/{len(product_urls)}] ✗ Error: {exc}", flush=True)

    print(f"\n  Scraped {len(toolzone_listings)} ToolZone products.\n")

    # -----------------------------------------------------------------------
    # Step 2: Search each competitor for each ToolZone product
    # -----------------------------------------------------------------------
    rows: list[ComparisonRow] = []

    for idx, tz in enumerate(toolzone_listings, 1):
        brand = tz.brand or brand_slug.capitalize()
        ean = tz.ean
        url_mpn = extract_mpn_from_url(tz.url)
        title_mpn = extract_mpn_from_title(tz.title, brand)

        print(
            f"[{idx:02d}/{len(toolzone_listings)}] {tz.title[:50]:<50}  "
            f"url_mpn={url_mpn or '—'}  title_mpn={title_mpn or '—'}  ean={ean or '—'}",
            flush=True,
        )

        competitor_results: dict[str, CompetitorListing | None] = {}
        for cid, scraper_cls in SEARCH_COMPETITORS.items():
            result = _search_one_competitor(
                scraper_cls, brand, tz.title,
                ean=ean, url_mpn=url_mpn, title_mpn=title_mpn,
            )
            competitor_results[cid] = result
            symbol = f"€{result.price_eur:.2f}" if result else "—"
            print(f"    {cid:<25} {symbol}", flush=True)

        rows.append(ComparisonRow(
            toolzone_title=tz.title,
            toolzone_ean=ean,
            toolzone_mpn=url_mpn or title_mpn,
            toolzone_price=tz.price_eur,
            toolzone_in_stock=tz.in_stock,
            toolzone_url=tz.url,
            competitor_results=competitor_results,
        ))

    return rows


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

def print_summary(rows: list[ComparisonRow]) -> None:
    competitor_ids = list(SEARCH_COMPETITORS.keys())

    print(f"\n{'='*65}")
    print("  SUMMARY")
    print(f"{'='*65}")

    col_w = 18  # competitor column width

    # Header
    header = f"{'Product':<42} {'TZ €':>7}"
    for cid in competitor_ids:
        short = cid.replace("_sk", "")[:col_w]
        header += f"  {short:>{col_w}}"
    print(header)
    print("-" * len(header))

    matched_any = 0
    price_diffs: list[float] = []

    for row in rows:
        title_short = row.toolzone_title[:41]
        line = f"{title_short:<42} {row.toolzone_price:>7.2f}"

        found_at_least_one = False
        for cid in competitor_ids:
            result = row.competitor_results.get(cid)
            if result:
                diff_pct = (result.price_eur - row.toolzone_price) / row.toolzone_price * 100
                price_diffs.append(diff_pct)
                sign = "+" if diff_pct >= 0 else ""
                cell = f"€{result.price_eur:.2f}({sign}{diff_pct:.0f}%)"
                found_at_least_one = True
            else:
                cell = "—"
            line += f"  {cell:>{col_w}}"

        print(line)
        if found_at_least_one:
            matched_any += 1

    print("-" * len(header))

    # Stats
    total = len(rows)
    match_rate = matched_any / total * 100 if total else 0

    print(f"\n  Products scraped:      {total}")
    print(f"  Matched at ≥1 store:   {matched_any}  ({match_rate:.0f}%)")

    if price_diffs:
        avg_diff = sum(price_diffs) / len(price_diffs)
        above = sum(1 for d in price_diffs if d > 5)
        below = sum(1 for d in price_diffs if d < -5)
        at_par = len(price_diffs) - above - below

        print(f"\n  Competitor prices vs ToolZone (where matched):")
        print(f"    Avg competitor premium:  {avg_diff:+.1f}%")
        print(f"    Competitors cheaper >5%: {below}")
        print(f"    At parity (±5%):         {at_par}")
        print(f"    Competitors pricier >5%: {above}")

    per_competitor = {}
    for cid in competitor_ids:
        hits = sum(1 for r in rows if r.competitor_results.get(cid) is not None)
        per_competitor[cid] = hits

    print(f"\n  Matches per competitor:")
    for cid, hits in per_competitor.items():
        bar = "█" * hits + "░" * (total - hits)
        print(f"    {cid:<28} {hits:>2}/{total}  {bar}")

    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Live ToolZone vs competitor comparison")
    parser.add_argument("--brand", default=DEFAULT_BRAND, help="Brand slug to filter (default: knipex)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max products to scrape (default: 25)")
    args = parser.parse_args()

    rows = run_comparison(args.brand, args.limit)
    if rows:
        print_summary(rows)
    else:
        print("No products scraped — check brand slug and network connectivity.")


if __name__ == "__main__":
    main()
