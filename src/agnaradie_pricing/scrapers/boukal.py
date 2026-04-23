"""Boukal scraper.

Boukal (boukal.cz) runs on a custom PHP/m1web (K2/Joomla) e-commerce platform.
Despite appearing JS-heavy, individual product pages are fully static HTML and
category pages are server-rendered with numbered pagination (?p=N).
No browser automation is needed.

Strategy
--------
1. discover_feed(): probe Czech Heureka + Zboží XML feed paths first.
   If a feed is found, fetch_feed() handles it (standard Heureka XML).

2. run_daily(ag_catalogue): HTTP-only, opens every product page:
   a. Group catalogue items by brand slug (e.g. "KNIPEX" → "knipex").
   b. For each brand, paginate /brand?p=1, ?p=2, … to collect ALL product URLs.
   c. Open every product page and extract EAN, Katalog (MPN), E-shop SKU,
      price, availability, brand, title.
   d. Emit a CompetitorListing for every product found — matching against the
      AG catalogue is handled downstream by match_products.py.
   e. Stop pagination once the last page is reached.

3. search_by_mpn / search_by_query: paginate the brand page, open product
   pages, match by Katalog/EAN.

Product page data sources
--------------------------
Spec table (consistent across all products):
    <span><span>E-shop: </span><span>K 87 01 250</span></span>  → competitor_sku
    <span><span>Katalog: </span><span>87 01 250</span></span>   → mpn
    <span><span>EAN: </span><span>4003773022022</span></span>   → ean

Schema.org microdata:
    itemprop="price" content="…"   → inc-VAT CZK price
    itemprop="availability" href   → InStock / OutOfStock

GTM m4detail JSON blob:
    item_id    → internal boukal ID
    item_name  → full title
    item_brand → brand name

Price currency: CZK (inc-VAT). Converted to EUR at a fixed approximate rate.
"""

import json
import re
import unicodedata
from collections import defaultdict
from datetime import UTC, datetime
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.heureka_feed import HeurekaFeedMixin
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


BOUKAL_CONFIG = {
    "id": "boukal_cz",
    "name": "Boukal",
    "url": "https://www.boukal.cz",
    "weight": 1.0,
    "rate_limit_rps": 3.0,
}

_CZK_EUR_RATE = 25.0

# Pattern for spec fields: <span><span>Label: </span><span>Value</span></span>
_SPEC_RE = re.compile(
    r'<span><span>([^<]+):\s*</span><span>([^<]+)</span></span>'
)


class BoukalScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or BOUKAL_CONFIG)
        self._rate_limit_rps: float = (config or BOUKAL_CONFIG).get("rate_limit_rps", 3.0)
        self.http_client = http_client or make_client(timeout=15.0)

    # ------------------------------------------------------------------
    def run_daily_iter(self, ag_catalogue: list[dict]):
        """Yield listings one brand at a time.

        Each brand's products are scraped (and yielded) before moving to the
        next brand, so the orchestrator can flush to DB after each brand's
        batch rather than waiting for the full multi-brand crawl to finish.
        """
        feed_url = self.discover_feed()
        if feed_url:
            yield from self.fetch_feed(feed_url)
            return

        brand_slugs: set[str] = set()
        for item in ag_catalogue:
            brand = item.get("brand") or ""
            if brand:
                brand_slugs.add(_brand_to_slug(brand))

        for brand_slug in brand_slugs:
            yield from self._scrape_all_brand_products(brand_slug)

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        return list(self.run_daily_iter(ag_catalogue))

    def run_manufacturer_iter(self, manufacturer_slug: str):
        """Yield all listings for a single manufacturer brand.

        Uses Boukal's brand-page pagination with the manufacturer slug as the
        brand slug (e.g. 'knipex' → /n/knipex/page/1/).
        """
        yield from self._scrape_all_brand_products(manufacturer_slug)

    def _scrape_all_brand_products(self, brand_slug: str) -> list[CompetitorListing]:
        """Paginate through all pages of a brand, open every product page.

        Phase 1 (sequential): collect all product URL paths via pagination.
        Phase 2 (parallel):   scrape each product page using worker threads.
        """
        # --- Phase 1: collect all product URL paths ---
        all_url_paths: list[str] = []
        page = 1

        while True:
            brand_url = f"{self.base_url.rstrip('/')}/{brand_slug}?p={page}"
            try:
                resp = polite_get(
                    self.http_client, brand_url, min_rps=self._rate_limit_rps,
                )
            except httpx.HTTPError:
                break

            if resp.status_code == 404:
                break

            product_urls = _extract_product_urls(resp.text)
            has_next = _has_next_page(resp.text)
            all_url_paths.extend(product_urls)

            if not has_next or not product_urls:
                break
            page += 1

        # --- Phase 2: scrape product pages in parallel ---
        workers: int = int(self.config.get("workers", 1))
        base_url = self.base_url
        competitor_id = self.competitor_id
        rps = self._rate_limit_rps

        def _scrape(url_path: str) -> CompetitorListing | None:
            return _scrape_product_page(
                get_thread_client(), url_path, base_url, competitor_id, rps=rps,
            )

        return parallel_map(all_url_paths, _scrape, workers=workers)

    # ------------------------------------------------------------------
    def discover_feed(self) -> str | None:
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
        """Scan brand pages opening each product page until MPN or EAN matches."""
        brand_slug = _brand_to_slug(brand)
        mpn_norm = re.sub(r"[\s\-]", "", mpn).lower()
        page = 1
        while True:
            brand_url = f"{self.base_url.rstrip('/')}/{brand_slug}?p={page}"
            try:
                resp = polite_get(self.http_client, brand_url, min_rps=self._rate_limit_rps)
            except httpx.HTTPError:
                break
            if resp.status_code == 404:
                break
            for url_path in _extract_product_urls(resp.text):
                listing = _scrape_product_page(
                    self.http_client, url_path, self.base_url, self.competitor_id,
                    rps=self._rate_limit_rps,
                )
                if listing and listing.mpn:
                    if re.sub(r"[\s\-]", "", listing.mpn).lower() == mpn_norm:
                        return listing
            if not _has_next_page(resp.text):
                break
            page += 1
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Open first-page products, score by token overlap in title/MPN."""
        tokens = query.lower().split()
        if not tokens:
            return None
        brand_slug = _brand_to_slug(tokens[0])
        brand_url = f"{self.base_url.rstrip('/')}/{brand_slug}?p=1"
        try:
            resp = polite_get(self.http_client, brand_url, min_rps=self._rate_limit_rps)
        except httpx.HTTPError:
            return None
        url_paths = _extract_product_urls(resp.text)
        if not url_paths:
            return None

        listings = [
            _scrape_product_page(
                self.http_client, u, self.base_url, self.competitor_id,
                rps=self._rate_limit_rps,
            )
            for u in url_paths
        ]
        rest = tokens[1:]
        if rest:
            def _score(l: CompetitorListing | None) -> int:
                if l is None:
                    return 0
                text = f"{l.title or ''} {l.mpn or ''}".lower()
                return sum(t in text for t in rest)
            listings.sort(key=_score, reverse=True)
        return next((l for l in listings if l is not None), None)


# ---------------------------------------------------------------------------
# Page helpers
# ---------------------------------------------------------------------------

def _extract_product_urls(html: str) -> list[str]:
    """Return deduplicated relative product URLs from a brand category page."""
    seen: set[str] = set()
    result: list[str] = []
    for url in re.findall(r'href="(/[^"]*-produkt)"', html):
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


def _has_next_page(html: str) -> bool:
    """Return True if the next-page button is visible and active."""
    m = re.search(r'k2pagNextAjax[^<]{0,400}', html, re.DOTALL)
    if not m:
        return False
    return "k2hidden" not in m.group(0)


# ---------------------------------------------------------------------------
# Product page scraping
# ---------------------------------------------------------------------------

def _scrape_product_page(
    client: httpx.Client,
    url_path: str,
    base_url: str,
    competitor_id: str,
    *,
    rps: float = 3.0,
) -> CompetitorListing | None:
    full_url = base_url.rstrip("/") + url_path
    try:
        resp = polite_get(client, full_url, min_rps=rps)
        resp.raise_for_status()
    except httpx.HTTPError:
        return None

    html = resp.text

    # --- Spec fields: E-shop (SKU), Katalog (MPN), EAN ---
    specs: dict[str, str] = {}
    for label, value in _SPEC_RE.findall(html):
        specs[label.strip()] = value.strip()

    ean = specs.get("EAN") or None
    mpn = specs.get("Katalog") or None
    sku_raw = specs.get("E-shop") or None

    # Use the E-shop code (e.g. "K 87 01 250") as competitor_sku;
    # fall back to the numeric ID at end of URL
    competitor_sku = sku_raw
    if not competitor_sku:
        id_match = re.search(r"-(\d+)-produkt$", url_path)
        if id_match:
            competitor_sku = id_match.group(1)

    # --- Price (inc-VAT CZK) from Schema.org ---
    price_match = re.search(r'itemprop="price"[^>]*content="([^"]+)"', html)
    if not price_match:
        return None
    try:
        price_czk = float(price_match.group(1))
    except ValueError:
        return None

    # --- Availability ---
    avail_match = re.search(r'itemprop="availability"[^>]*href="([^"]+)"', html)
    in_stock: bool | None = None
    if avail_match:
        in_stock = "InStock" in avail_match.group(1)

    # --- Brand + title from GTM m4detail ---
    brand: str | None = None
    title: str = mpn or url_path
    m4_match = re.search(
        r'"m4detail":\{"currency":"CZK","items":\[(\{[^}]+\})\]', html
    )
    if m4_match:
        try:
            data = json.loads(m4_match.group(1))
            brand = data.get("item_brand") or None
            title = data.get("item_name") or title
        except (json.JSONDecodeError, KeyError):
            pass

    price_eur = round(price_czk / _CZK_EUR_RATE, 2)

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
        url=full_url,
        scraped_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _brand_to_slug(brand: str) -> str:
    """Convert brand name to boukal.cz URL slug (lowercase, no accents)."""
    nfkd = unicodedata.normalize("NFKD", brand)
    ascii_brand = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", ascii_brand.lower()).strip("-")
