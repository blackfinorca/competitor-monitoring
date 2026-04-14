"""Boukal scraper.

Boukal (boukal.cz) runs on a custom PHP/m1web e-commerce platform.
Search results and product listings are JS-rendered (AJAX) and not
accessible via static HTML scraping.

Strategy
--------
1. discover_feed(): probe Czech-specific feed paths (Heureka.cz and
   Zboží.cz XML feeds share the same SHOPITEM schema). Czech e-shops
   almost universally publish at least one of these. Returns the first
   working URL or None.

2. fetch_feed(feed_url): standard Heureka XML parsing via HeurekaFeedMixin.

3. search_by_query(): best-effort static HTML search — returns None
   gracefully if the platform serves JS-rendered pages (no crash).

Note: Until a feed URL is discovered, boukal_cz will produce no results
during live product searches. The daily batch job will populate data once
discover_feed() finds the feed.
"""

import re
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
    "rate_limit_rps": 1,
}


class BoukalScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or BOUKAL_CONFIG)
        self._rate_limit_rps: float = (config or BOUKAL_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client(timeout=15.0)

    def discover_feed(self) -> str | None:
        """Probe Czech Heureka + Zboží feed paths."""
        for path in HEUREKA_FEED_PATHS:
            url = urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))
            try:
                response = polite_get(self.http_client, url, min_rps=0.5)
            except httpx.HTTPError:
                continue
            ct = response.headers.get("content-type", "").lower()
            if 200 <= response.status_code < 300 and ("xml" in ct or response.text.lstrip().startswith("<")):
                return str(response.url)
        return None

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Best-effort static search — returns None if JS-rendered (no crash)."""
        try:
            resp = polite_get(
                self.http_client,
                self.base_url,
                min_rps=self._rate_limit_rps,
                referer=self.base_url,
                params={"search_term": query},
            )
            resp.raise_for_status()
        except Exception:
            return None
        # Look for product detail links in raw HTML (URLs ending in -produkt)
        urls = re.findall(
            r'href=["\']?(https://www\.boukal\.cz/[^"\'<\s]+-produkt)',
            resp.text,
        )
        if not urls:
            return None
        # If product links found (site switched to static), fetch first one
        return self._scrape_product_page(urls[0])

    def _scrape_product_page(self, url: str) -> CompetitorListing | None:
        """Attempt to extract product data from a detail page (best-effort)."""
        try:
            resp = polite_get(
                self.http_client,
                url,
                min_rps=self._rate_limit_rps,
                referer=self.base_url,
            )
            resp.raise_for_status()
        except Exception:
            return None
        # Try to find price in page (GTM dataLayer or meta tags)
        price = _extract_price(resp.text)
        title = _extract_title(resp.text)
        if price is None or not title:
            return None
        return CompetitorListing(
            competitor_id=self.competitor_id,
            competitor_sku=None,
            brand=None,
            mpn=None,
            ean=None,
            title=title,
            price_eur=price,
            currency="EUR",
            in_stock=None,
            url=url,
            scraped_at=datetime.now(UTC),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_price(html: str) -> float | None:
    """Extract price from GTM dataLayer or og:price meta tag."""
    # Try GTM dataLayer: "price": 123.45 or "price":"123.45"
    m = re.search(r'"price"\s*:\s*"?([\d]+(?:[.,]\d+)?)"?', html)
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    return None


def _extract_title(html: str) -> str | None:
    """Extract product title from <title> or og:title."""
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', html)
    if m:
        return m.group(1).strip()
    m = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return None
