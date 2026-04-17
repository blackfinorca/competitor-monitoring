"""AGI scraper.

AGI (agi.sk) is a Slovak tool & hardware distributor running on the rshop
platform (images-agi-cdn.rshop.sk).

Strategy
--------
1. discover_feed(): probe standard Heureka XML feed paths.

2. get_manufacturer_categories(): fetch /vyrobcovia and build a
   brand_name → category_url map.

3. run_daily_iter(ag_catalogue): brand-page crawl.
   For each unique brand in ag_catalogue, look up its category URL
   and paginate that brand's product listing.

4. run_manufacturer_iter(brand_slug): scrape all products for a single
   manufacturer.  Pagination: /{slug}-c{id}?page=N (12 products/page).
   Used directly by manufacturer_scrape.py.

5. search_by_mpn / search_by_query: fall back to /vyhladavanie?search=…

Product page JSON-LD (application/ld+json):
    @type: Product
    name:  {title}
    sku:   93449  (internal agi.sk integer ID)
    mpn:   "97 52 38 SB"  (real manufacturer MPN ✓)
    gtin:  "4003773052630"  → EAN  ← primary match key
    brand.name: "Knipex"   (correct — use directly)
    offers.price: "138.81"  (EUR ✓)
    offers.priceCurrency: "EUR"
    offers.availability: "http://schema.org/OutOfStock"

Note: all product/category hrefs in the HTML use absolute URLs
(https://www.agi.sk/slug-pNNNNN), not relative paths.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from html.parser import HTMLParser
import json

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


AGI_CONFIG = {
    "id": "agi_sk",
    "name": "AGI",
    "url": "https://www.agi.sk",
    "weight": 1.0,
    "rate_limit_rps": 2.0,
    "workers": 4,
}

_MANUFACTURERS_URL = "https://www.agi.sk/vyrobcovia"
_PAGE_SIZE = 12  # products per listing page

# Product link pattern: absolute https://www.agi.sk/slug-pNNNNN
# Captures the path portion (/slug-pNNNNN) from both absolute and relative hrefs.
_PRODUCT_URL_RE = re.compile(r'href="(?:https://www\.agi\.sk)?(/[^"]+\-p\d+)"')

# Manufacturer category link: absolute or relative /slug-cNNNNN
_MFR_LINK_RE = re.compile(r'href="(?:https://www\.agi\.sk)?(/[^"]+\-c\d+)"[^>]*>\s*([^<]+?)\s*<')


class AgiScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or AGI_CONFIG)
        self._rate_limit_rps: float = (config or AGI_CONFIG).get("rate_limit_rps", 2.0)
        self.http_client = http_client or make_client(timeout=15.0)
        self._mfr_cache: dict[str, str] | None = None  # brand_lower → category_url

    # ------------------------------------------------------------------
    # Manufacturer catalogue discovery
    # ------------------------------------------------------------------

    def get_manufacturer_categories(self) -> dict[str, str]:
        """Fetch /vyrobcovia and return {brand_name_lower: category_url} map."""
        if self._mfr_cache is not None:
            return self._mfr_cache
        try:
            resp = polite_get(self.http_client, _MANUFACTURERS_URL, min_rps=0.5)
            resp.raise_for_status()
        except httpx.HTTPError:
            return {}
        result: dict[str, str] = {}
        for path, name in _MFR_LINK_RE.findall(resp.text):
            name_clean = name.strip()
            if name_clean:
                full_url = self.base_url.rstrip("/") + path
                result[name_clean.lower()] = full_url
        self._mfr_cache = result
        return result

    def _resolve_manufacturer_url(self, brand_slug: str) -> str | None:
        """Return the agi.sk category URL for a brand slug/name, or None."""
        cats = self.get_manufacturer_categories()
        # Try exact match first, then prefix/substring match
        brand_lower = brand_slug.lower().replace("-", " ")
        if brand_lower in cats:
            return cats[brand_lower]
        for name, url in cats.items():
            if brand_lower in name or name in brand_lower:
                return url
        return None

    # ------------------------------------------------------------------
    # Daily scrape — brand-page crawl
    # ------------------------------------------------------------------

    def run_daily_iter(self, ag_catalogue: list[dict]):
        """Yield listings one brand at a time, derived from ag_catalogue brands."""
        feed_url = self.discover_feed()
        if feed_url:
            yield from self.fetch_feed(feed_url)
            return

        seen_slugs: set[str] = set()
        for item in ag_catalogue:
            brand = item.get("brand") or ""
            if brand and brand.lower() not in seen_slugs:
                seen_slugs.add(brand.lower())
                yield from self.run_manufacturer_iter(brand)

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        return list(self.run_daily_iter(ag_catalogue))

    # ------------------------------------------------------------------
    # Manufacturer-scoped scrape (used by manufacturer_scrape.py)
    # ------------------------------------------------------------------

    def run_manufacturer_iter(self, brand_slug: str):
        """Paginate the manufacturer's category page and scrape each product."""
        cat_url = self._resolve_manufacturer_url(brand_slug)
        if not cat_url:
            return

        workers: int = int(self.config.get("workers", 1))
        base_url = self.base_url
        competitor_id = self.competitor_id
        rps = self._rate_limit_rps

        # Derive display brand name from slug (e.g. "knipex" → "Knipex")
        fallback_brand = brand_slug.replace("-", " ").title()

        def _scrape(url_path: str) -> CompetitorListing | None:
            return _scrape_product_page(
                get_thread_client(), url_path, base_url, competitor_id, rps=rps,
                fallback_brand=fallback_brand,
            )

        seen_url_keys: set[str] = set()
        page = 1
        while True:
            page_url = cat_url if page == 1 else f"{cat_url}?page={page}"
            try:
                resp = polite_get(self.http_client, page_url, min_rps=rps)
            except httpx.HTTPError:
                break
            if resp.status_code != 200:
                break
            url_paths = _extract_product_urls(resp.text)
            if not url_paths:
                break
            new_paths = [u for u in url_paths if u not in seen_url_keys]
            if not new_paths:
                break
            seen_url_keys.update(new_paths)
            yield from parallel_map(new_paths, _scrape, workers=workers)
            page += 1

    # ------------------------------------------------------------------
    # Feed (Heureka XML)
    # ------------------------------------------------------------------

    def discover_feed(self) -> str | None:
        for path in HEUREKA_FEED_PATHS:
            from urllib.parse import urljoin
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

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        from agnaradie_pricing.scrapers.heureka_feed import parse_heureka_feed
        response = self.http_client.get(feed_url)
        response.raise_for_status()
        return parse_heureka_feed(response.content, self.competitor_id)

    # ------------------------------------------------------------------
    # Search fallbacks
    # ------------------------------------------------------------------

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Search agi.sk and scrape the first matching product detail page."""
        try:
            resp = polite_get(
                self.http_client,
                f"{self.base_url.rstrip('/')}/vyhladavanie",
                min_rps=self._rate_limit_rps,
                params={"search": query},
            )
            resp.raise_for_status()
        except httpx.HTTPError:
            return None
        paths = _extract_product_urls(resp.text)
        if not paths:
            return None
        return _scrape_product_page(
            self.http_client, paths[0], self.base_url, self.competitor_id,
            rps=self._rate_limit_rps,
        )


# ---------------------------------------------------------------------------
# Page helpers
# ---------------------------------------------------------------------------

def _extract_product_urls(html: str) -> list[str]:
    """Return deduplicated relative product URL paths from a listing page."""
    seen: set[str] = set()
    result: list[str] = []
    for url in _PRODUCT_URL_RE.findall(html):
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


# ---------------------------------------------------------------------------
# Product page scraping
# ---------------------------------------------------------------------------

def _scrape_product_page(
    client: httpx.Client,
    url_path: str,
    base_url: str,
    competitor_id: str,
    *,
    rps: float = 2.0,
    fallback_brand: str | None = None,
) -> CompetitorListing | None:
    full_url = base_url.rstrip("/") + url_path if url_path.startswith("/") else url_path
    try:
        resp = polite_get(client, full_url, min_rps=rps)
        resp.raise_for_status()
    except httpx.HTTPError:
        return None
    return _parse_product_page(resp.text, competitor_id, full_url, fallback_brand=fallback_brand)


def _parse_product_page(
    html: str,
    competitor_id: str,
    url: str,
    *,
    fallback_brand: str | None = None,
) -> CompetitorListing | None:
    """Extract product data from an agi.sk product page via JSON-LD."""
    parser = _JsonLdParser()
    parser.feed(html)

    for payload in parser.payloads:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict) or data.get("@type") != "Product":
            continue

        name = data.get("name") or ""
        if not name:
            continue

        # EAN — gtin field (most reliable)
        ean = (
            _as_ean(data.get("gtin13"))
            or _as_ean(data.get("gtin8"))
            or _as_ean(data.get("gtin"))
        )

        # Internal SKU (integer from JSON-LD)
        competitor_sku = str(data.get("sku")) if data.get("sku") is not None else None

        # MPN in JSON-LD is actually the EDE article number — store it as-is
        # but don't use for matching (EAN is the reliable key)
        mpn = data.get("mpn") or None

        # Price — EUR
        offers = data.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        price_raw = offers.get("price")
        if price_raw is None:
            return None
        try:
            price_eur = float(str(price_raw).replace(",", "."))
        except ValueError:
            return None

        # Availability
        availability = offers.get("availability", "")
        in_stock: bool | None = None
        if "InStock" in availability:
            in_stock = True
        elif "OutOfStock" in availability:
            in_stock = False

        # Brand: JSON-LD brand.name is usually correct (e.g. "Knipex"),
        # but some accessory listings have "EDE" (the distributor).
        # Fall back to the manufacturer slug name when that happens.
        brand_data = data.get("brand")
        if isinstance(brand_data, dict):
            brand = brand_data.get("name") or None
        elif isinstance(brand_data, str):
            brand = brand_data or None
        else:
            brand = None
        if (not brand or brand.upper() == "EDE") and fallback_brand:
            brand = fallback_brand

        return CompetitorListing(
            competitor_id=competitor_id,
            competitor_sku=competitor_sku,
            brand=brand,
            mpn=mpn,
            ean=ean,
            title=name,
            price_eur=price_eur,
            currency="EUR",
            in_stock=in_stock,
            url=offers.get("url") or url,
            scraped_at=datetime.now(UTC),
        )

    return None


_BRAND_RE = re.compile(r"Zna[cč]ka\s*\t([^\n\r]+)")


def _extract_real_brand(html: str) -> str | None:
    """Extract real manufacturer brand from agi.sk product page body text."""
    # Strip HTML for text matching
    text = re.sub(r"<[^>]+>", " ", html)
    m = _BRAND_RE.search(text)
    if m:
        brand = m.group(1).strip().rstrip("®™").strip()
        return brand or None
    return None


_EAN_RE = re.compile(r"^\d{8}(?:\d{4,5})?$")


def _as_ean(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if _EAN_RE.match(s) else None


# ---------------------------------------------------------------------------
# JSON-LD parser
# ---------------------------------------------------------------------------

class _JsonLdParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.payloads: list[str] = []
        self._in_jsonld = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        if tag == "script" and dict(attrs).get("type") == "application/ld+json":
            self._in_jsonld = True
            self._chunks = []

    def handle_endtag(self, tag: str):
        if tag == "script" and self._in_jsonld:
            self.payloads.append("".join(self._chunks).strip())
            self._in_jsonld = False
            self._chunks = []

    def handle_data(self, data: str):
        if self._in_jsonld:
            self._chunks.append(data)
