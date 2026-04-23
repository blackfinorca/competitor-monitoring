"""Doktor Kladivo scraper.

DoktorKladivo (doktorkladivo.sk) runs on a custom Shoptet-derived platform.

Strategy
--------
run_daily():
    Full catalogue crawl from the top-level "Náradie" category:
    1. GET /naradie-c1006/?f=0, ?f=24, ?f=48, … (24 products per page)
    2. Collect all product links (href pattern: /slug-pNNNN/?cid=1006)
    3. Open every product page and extract:
         - Title       : <h1>
         - MPN         : "product_code":"…" (inline JS dataLayer)
         - Brand       : "product_brand":"…" (inline JS dataLayer)
         - EAN         : <bs-grid-item class="ean value"><span>…</span>
         - Price (EUR) : "price":N,"priceCurrency":"EUR" (inline JS)
         - Availability: "availability":"https://schema.org/InStock"

search_by_mpn():
    Fallback used by matching jobs — GET /hladat/?q={brand}+{mpn},
    parse first JSON-LD Product result from the search page.

~10,000 products in the catalogue as of 2026-04.
Pagination offset step: 24 items per page.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper


DOKTOR_KLADIVO_CONFIG = {
    "id": "doktorkladivo_sk",
    "name": "Doktor Kladivo",
    "url": "https://www.doktorkladivo.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    "search_path": "hladat/",
    "search_query_param": "q",
    # Top-level category path that contains the full product catalogue
    "catalogue_path": "naradie-c1006/",
    "page_size": 24,
}

_BASE_URL = "https://www.doktorkladivo.sk"


class DoktorKladivoScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        # Merge defaults so search_path/catalogue_path survive YAML-supplied configs
        merged = {**DOKTOR_KLADIVO_CONFIG, **(config or {})}
        super().__init__(merged)
        self._rate_limit_rps: float = merged.get("rate_limit_rps", 1.0)
        self._catalogue_path: str = merged.get("catalogue_path", "naradie-c1006/")
        self._page_size: int = merged.get("page_size", 24)
        self._search_path: str = merged.get("search_path", "hladat/")
        self._search_param: str = merged.get("search_query_param", "q")
        self.http_client = http_client or make_client(timeout=15.0)

    # ------------------------------------------------------------------
    # Full catalogue crawl
    # ------------------------------------------------------------------

    def run_daily_iter(self, ag_catalogue):
        """Yield listings one category page at a time.

        Interleaves pagination and scraping: fetch one listing page (24 URLs),
        scrape those product pages in parallel, yield the results, then move to
        the next listing page. This means up to 24 products are saved to DB
        after every ~24 product-page requests rather than waiting for all ~10k.
        """
        workers: int = int(self.config.get("workers", 1))
        competitor_id = self.competitor_id
        rps = self._rate_limit_rps
        offset = 0

        def _scrape(path: str) -> CompetitorListing | None:
            full_url = urljoin(_BASE_URL + "/", path.lstrip("/"))
            try:
                resp = polite_get(
                    get_thread_client(),
                    full_url,
                    min_rps=rps,
                    referer=_BASE_URL + "/",
                )
                resp.raise_for_status()
            except httpx.HTTPError:
                return None
            return _parse_product_page(resp.text, competitor_id, full_url)

        while True:
            cat_url = urljoin(_BASE_URL + "/", self._catalogue_path)
            try:
                resp = polite_get(
                    self.http_client,
                    cat_url,
                    min_rps=rps,
                    params={"f": offset} if offset > 0 else {},
                )
            except httpx.HTTPError:
                break

            if resp.status_code != 200:
                break

            paths = _extract_product_paths(resp.text)
            if not paths:
                break

            # Scrape this page's products in parallel, yield immediately
            yield from parallel_map(paths, _scrape, workers=workers)
            offset += self._page_size

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        return list(self.run_daily_iter(ag_catalogue))

    def _scrape_product_page(self, url: str) -> CompetitorListing | None:
        try:
            resp = polite_get(
                get_thread_client(),
                url,
                min_rps=self._rate_limit_rps,
                referer=_BASE_URL + "/",
            )
            resp.raise_for_status()
        except httpx.HTTPError:
            return None

        return _parse_product_page(resp.text, self.competitor_id, url)

    # ------------------------------------------------------------------
    # Search fallback (used by live single-product lookups)
    # ------------------------------------------------------------------

    def discover_feed(self) -> str | None:
        return None

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        return []

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        mpn_spaced = re.sub(r"[\-._]+", " ", mpn).strip()
        return self.search_by_query(f"{brand} {mpn_spaced}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        # Delegate to ShoptetGenericScraper logic via a thin wrapper
        _delegate = _SearchDelegate(
            {**DOKTOR_KLADIVO_CONFIG, **{"id": self.competitor_id}},
            http_client=self.http_client,
        )
        return _delegate.search_by_query(query)


# ---------------------------------------------------------------------------
# Thin search delegate — reuses ShoptetGenericScraper without inheritance
# ---------------------------------------------------------------------------

class _SearchDelegate(ShoptetGenericScraper):
    """Minimal subclass used only for search_by_query delegation."""
    def __init__(self, config: dict, http_client=None):
        super().__init__(config, http_client=http_client)


# ---------------------------------------------------------------------------
# Category page helpers
# ---------------------------------------------------------------------------

# Product link pattern: /slug-pNNNN/ or /slug-pNNNN/?cid=NNNN
_PRODUCT_PATH_RE = re.compile(r'href="(/[^"]+\-p\d+/[^"]*)"')


def _extract_product_paths(html: str) -> list[str]:
    """Return deduplicated product URL paths from a category listing page."""
    seen: set[str] = set()
    result: list[str] = []
    for path in _PRODUCT_PATH_RE.findall(html):
        # Normalise: strip query string for deduplication key
        base = path.split("?")[0]
        if base not in seen:
            seen.add(base)
            result.append(path)  # keep original with ?cid= for fetching
    return result


# ---------------------------------------------------------------------------
# Product page parser
# ---------------------------------------------------------------------------

def _parse_product_page(
    html: str,
    competitor_id: str,
    url: str,
) -> CompetitorListing | None:
    # --- Title ---
    title_m = re.search(r"<h1[^>]*>\s*([^<]+)\s*</h1>", html)
    title = title_m.group(1).strip() if title_m else None
    if not title:
        return None

    # --- MPN (product_code from inline JS dataLayer) ---
    mpn_m = re.search(r'"product_code"\s*:\s*"([^"]+)"', html)
    mpn = mpn_m.group(1).strip() if mpn_m else None

    # --- Brand ---
    brand_m = re.search(r'"product_brand"\s*:\s*"([^"]+)"', html)
    brand = brand_m.group(1).strip() if brand_m else None

    # --- EAN (custom web component) ---
    ean_m = re.search(r'class="ean value">\s*<span>\s*(\d{8,13})\s*</span>', html)
    ean = ean_m.group(1) if ean_m else None

    # --- Price (EUR) ---
    price_m = re.search(
        r'"price"\s*:\s*([\d.]+)\s*,\s*"priceCurrency"\s*:\s*"EUR"', html
    )
    if not price_m:
        return None
    try:
        price_eur = float(price_m.group(1))
    except ValueError:
        return None

    # --- Availability ---
    avail_m = re.search(r'"availability"\s*:\s*"https://schema\.org/(\w+)"', html)
    in_stock: bool | None = None
    if avail_m:
        in_stock = avail_m.group(1) == "InStock"

    # --- Competitor SKU (internal product ID from URL or dataLayer) ---
    sku_m = re.search(r'"ecomm_prodid"\s*:\s*"(\d+)"', html)
    if not sku_m:
        sku_m = re.search(r'\-p(\d+)/', url)
    competitor_sku = sku_m.group(1) if sku_m else None

    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=competitor_sku,
        brand=brand,
        mpn=mpn,
        ean=ean,
        title=title,
        price_eur=price_eur,
        currency="EUR",
        in_stock=in_stock,
        url=url,
        scraped_at=datetime.now(UTC),
    )
