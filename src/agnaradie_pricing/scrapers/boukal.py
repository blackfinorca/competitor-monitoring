"""Boukal scraper.

Boukal (boukal.cz) runs on a custom PHP/m1web (K2) e-commerce platform.
Product listings are fully AJAX-rendered — static HTTP scraping returns
no product data. This scraper uses playwright (sync API) to drive a real
browser for each search request.

Strategy
--------
1. discover_feed(): probe Czech Heureka + Zboží XML feed paths first.
   If a feed is found, fetch_feed() handles it (standard Heureka XML).

2. search_by_mpn(brand, mpn): drive a browser to the brand page
   (e.g. /knipex for brand="KNIPEX"), wait for .product_item elements,
   then find the row whose product code matches the MPN.
   Falls back to search_by_query("{brand} {mpn}") if brand page yields
   nothing.

3. search_by_query(query): extract the brand token from the query string,
   build the brand-page URL, then filter results by the remaining tokens.

Product listing selectors (brand category page):
    Container:  .product_item
    Code:       .product_item_code          → "Kód: K 87 01 250"
    Title:      .product_item_title a
    Price:      .product_item_price_wrap    → "740,00 Kč / KS s DPH" (CZK incl. VAT)
    Link:       a[href*="produkt"]

Price currency: CZK. Converted to EUR at a fixed approximate rate.
GTM dataLayer on detail pages has ex-VAT CZK price; we use the
displayed inc-VAT listing price for simplicity.
"""

import re
import time
import unicodedata
from datetime import UTC, datetime
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.heureka_feed import HeurekaFeedMixin
from agnaradie_pricing.scrapers.http import make_client, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


BOUKAL_CONFIG = {
    "id": "boukal_cz",
    "name": "Boukal",
    "url": "https://www.boukal.cz",
    "weight": 1.0,
    "rate_limit_rps": 0.5,   # be polite with Playwright requests
}

# Approximate CZK→EUR rate; good enough for relative price comparison.
_CZK_EUR_RATE = 25.0


class BoukalScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or BOUKAL_CONFIG)
        self._rate_limit_rps: float = (config or BOUKAL_CONFIG).get("rate_limit_rps", 0.5)
        self.http_client = http_client or make_client(timeout=15.0)

    # ------------------------------------------------------------------
    def discover_feed(self) -> str | None:
        """Probe Czech Heureka + Zboží feed paths first."""
        for path in HEUREKA_FEED_PATHS:
            url = urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))
            try:
                response = polite_get(self.http_client, url, min_rps=0.5)
            except httpx.HTTPError:
                continue
            ct = response.headers.get("content-type", "").lower()
            if 200 <= response.status_code < 300 and (
                "xml" in ct or response.text.lstrip().startswith("<SHOP")
            ):
                return str(response.url)
        return None

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        """Navigate to the brand page and find the product matching the MPN."""
        brand_slug = _brand_to_slug(brand)
        brand_url = f"{self.base_url.rstrip('/')}/{brand_slug}?layout=1"
        items = _playwright_scrape_brand_page(brand_url)
        # Normalise MPN for comparison: strip dashes/spaces → "8701250"
        mpn_norm = re.sub(r"[\s\-]", "", mpn).lower()
        for item in items:
            code_norm = re.sub(r"[\s\-]", "", item.get("code", "")).lower()
            if mpn_norm and mpn_norm in code_norm:
                return _to_listing(item, self.competitor_id)
        # Nothing on the brand page — try generic search fallback
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Extract brand token → brand page, filter by remaining tokens."""
        tokens = query.split()
        if not tokens:
            return None
        # Heuristic: first ALL-CAPS token or first token is likely the brand
        brand_token = tokens[0]
        brand_slug = _brand_to_slug(brand_token)
        brand_url = f"{self.base_url.rstrip('/')}/{brand_slug}?layout=1"
        items = _playwright_scrape_brand_page(brand_url)
        if not items:
            return None
        # Score each item by how many query tokens appear in title/code
        rest = " ".join(tokens[1:]).lower()
        if rest:
            scored = [
                (
                    sum(t in (item.get("title") or "").lower() or
                        t in (item.get("code") or "").lower()
                        for t in tokens[1:]),
                    item,
                )
                for item in items
            ]
            scored.sort(key=lambda x: x[0], reverse=True)
            if scored[0][0] > 0:
                return _to_listing(scored[0][1], self.competitor_id)
        return _to_listing(items[0], self.competitor_id)


# ---------------------------------------------------------------------------
# Playwright scraping
# ---------------------------------------------------------------------------

def _playwright_scrape_brand_page(url: str) -> list[dict]:
    """Launch a headless browser, load the brand page, return deduplicated product dicts."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return []

    results = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page()
            # domcontentloaded + explicit sleep is more reliable than networkidle
            # for AJAX-heavy pages — networkidle can resolve before products load.
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(5)

            results = page.evaluate("""() => {
                const seen = new Set();
                const items = [];
                document.querySelectorAll('.product_item').forEach(el => {
                    const link = el.querySelector('a[href*="produkt"]')?.href || '';
                    if (!link || seen.has(link)) return;  // deduplicate dual-layout
                    seen.add(link);
                    const codeRaw = el.querySelector('.product_item_code')?.innerText?.trim() || '';
                    const code = codeRaw.replace(/^K\\u00f3d:\\s*/i, '').trim();
                    const title = el.querySelector('.item_data_wrap a')?.innerText?.trim()
                                || el.querySelector('.product_item_title a')?.innerText?.trim()
                                || '';
                    const priceRaw = el.querySelector('.product_item_price_wrap')?.innerText?.trim() || '';
                    const stockRaw = el.querySelector('[class*="stock"], [class*="sklad"]')?.innerText?.trim() || '';
                    items.push({ code, title, priceRaw, link, stockRaw });
                });
                return items;
            }""")
            browser.close()
    except Exception:
        pass
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _brand_to_slug(brand: str) -> str:
    """Convert brand name to boukal.cz URL slug (lowercase, no accents)."""
    # Remove accents
    nfkd = unicodedata.normalize("NFKD", brand)
    ascii_brand = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", ascii_brand.lower()).strip("-")


def _parse_czk_price(text: str) -> float | None:
    """Parse "2 300,00 Kč / KS s DPH" → float CZK."""
    m = re.search(r"([\d\s]+(?:[.,]\d+)?)\s*Kč", text)
    if not m:
        return None
    cleaned = m.group(1).replace(" ", "").replace(",", ".").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_listing(item: dict, competitor_id: str) -> CompetitorListing | None:
    price_czk = _parse_czk_price(item.get("priceRaw", ""))
    if price_czk is None:
        return None
    price_eur = round(price_czk / _CZK_EUR_RATE, 2)
    stock_text = (item.get("stockRaw") or "").lower()
    in_stock: bool | None = None
    if "sklad" in stock_text:
        in_stock = True
    elif "není" in stock_text or "vyprod" in stock_text:
        in_stock = False

    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=item.get("code") or None,
        brand=None,
        mpn=item.get("code") or None,
        ean=None,
        title=item.get("title", ""),
        price_eur=price_eur,
        currency="EUR",
        in_stock=in_stock,
        url=item.get("link", ""),
        scraped_at=datetime.now(UTC),
    )
