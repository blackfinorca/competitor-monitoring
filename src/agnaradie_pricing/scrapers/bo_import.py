"""BO-Import scraper.

BO-Import (bo-import.cz) is an authorized Czech KNIPEX distributor running
on the BSSHOP e-commerce platform.

Strategy
--------
1. discover_feed(): probe standard Heureka XML feed paths; return URL if found.

2. run_daily_iter(ag_catalogue): brand-page crawl.
   a. Derive unique brand slugs from the AG catalogue.
   b. For each brand, paginate /{brand-slug}/?f=0, ?f=30, ?f=60, …
   c. Collect product URLs and scrape each page for JSON-LD.
   d. Yield CompetitorListing for each product.

3. run_manufacturer_iter(brand_slug): scrape a single manufacturer's catalogue
   page by page. Used by manufacturer_scrape.py.

4. search_by_mpn / search_by_query: fall back to scraping the brand page and
   matching by SKU/title.

Product page JSON-LD (application/ld+json):
    @type: Product
    name:  {title}
    sku:   "KNI-8701300"  → competitor_sku; strip prefix → mpn ("8701300")
    gtin13: "4003773034087"  → EAN
    mpn:   ""  (always empty — use sku instead)
    offers[0].price: "791.62"  (CZK, inc-VAT)
    offers[0].priceCurrency: "czk"
    offers[0].availability: "http://schema.org/InStock"

Price currency: CZK (inc-VAT). Converted to EUR at a fixed approximate rate.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


BO_IMPORT_CONFIG = {
    "id": "bo_import_cz",
    "name": "BO-Import",
    "url": "https://www.bo-import.cz",
    "weight": 1.0,
    "rate_limit_rps": 2.0,
    "workers": 4,
}

_CZK_EUR_RATE = 25.0
_PAGE_SIZE = 30  # products per page (?f=N offset step)

# Product link pattern: /slug-pNNNN/ (with optional ?cid=NNN query string)
_PRODUCT_URL_RE = re.compile(r'href="(/[^"]+\-p\d+/[^"]*)"')


class BoImportScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or BO_IMPORT_CONFIG)
        self._rate_limit_rps: float = (config or BO_IMPORT_CONFIG).get("rate_limit_rps", 2.0)
        self.http_client = http_client or make_client(timeout=15.0)

    # ------------------------------------------------------------------
    # Daily scrape — brand-page crawl
    # ------------------------------------------------------------------

    def run_daily_iter(self, ag_catalogue: list[dict]):
        """Yield listings one brand at a time, derived from ag_catalogue brands."""
        feed_url = self.discover_feed()
        if feed_url:
            yield from self.fetch_feed(feed_url)
            return

        brand_slugs: set[str] = set()
        for item in ag_catalogue:
            brand = item.get("brand") or ""
            if brand:
                brand_slugs.add(_brand_to_slug(brand))

        for slug in brand_slugs:
            yield from self.run_manufacturer_iter(slug)

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        return list(self.run_daily_iter(ag_catalogue))

    # ------------------------------------------------------------------
    # Manufacturer-scoped scrape (used by manufacturer_scrape.py)
    # ------------------------------------------------------------------

    def run_manufacturer_iter(self, brand_slug: str):
        """Paginate /{brand_slug}/?f=N, scrape all product pages, yield listings."""
        workers: int = int(self.config.get("workers", 1))
        base_url = self.base_url
        competitor_id = self.competitor_id
        rps = self._rate_limit_rps

        def _scrape(url_path: str) -> CompetitorListing | None:
            return _scrape_product_page(
                get_thread_client(), url_path, base_url, competitor_id, rps=rps,
            )

        seen_url_keys: set[str] = set()
        offset = 0
        while True:
            page_url = f"{self.base_url.rstrip('/')}/{brand_slug}/"
            params = {"f": offset} if offset > 0 else {}
            try:
                resp = polite_get(
                    self.http_client, page_url,
                    min_rps=self._rate_limit_rps,
                    params=params,
                )
            except httpx.HTTPError:
                break

            if resp.status_code == 404:
                break

            url_paths = _extract_product_urls(resp.text)
            if not url_paths:
                break

            # Detect pagination cycle: stop if all returned URLs were already seen
            # (bo-import.cz returns the same page instead of 404 for out-of-range offsets)
            new_paths = [u for u in url_paths if u.split("?")[0] not in seen_url_keys]
            if not new_paths:
                break
            for u in url_paths:
                seen_url_keys.add(u.split("?")[0])

            yield from parallel_map(new_paths, _scrape, workers=workers)
            offset += _PAGE_SIZE

    # ------------------------------------------------------------------
    # Feed discovery
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

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        from agnaradie_pricing.scrapers.heureka_feed import parse_heureka_feed
        response = self.http_client.get(feed_url)
        response.raise_for_status()
        return parse_heureka_feed(response.content, self.competitor_id)

    # ------------------------------------------------------------------
    # Search fallbacks
    # ------------------------------------------------------------------

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        """Scan the brand page for a product whose SKU or title contains the MPN."""
        brand_slug = _brand_to_slug(brand)
        mpn_norm = re.sub(r"[\s\-]", "", mpn).lower()
        offset = 0
        while True:
            page_url = f"{self.base_url.rstrip('/')}/{brand_slug}/"
            params = {"f": offset} if offset > 0 else {}
            try:
                resp = polite_get(self.http_client, page_url,
                                  min_rps=self._rate_limit_rps, params=params)
            except httpx.HTTPError:
                break
            if resp.status_code == 404:
                break
            url_paths = _extract_product_urls(resp.text)
            if not url_paths:
                break
            for path in url_paths:
                listing = _scrape_product_page(
                    self.http_client, path, self.base_url, self.competitor_id,
                    rps=self._rate_limit_rps,
                )
                if listing and listing.mpn:
                    if re.sub(r"[\s\-]", "", listing.mpn).lower() == mpn_norm:
                        return listing
            offset += _PAGE_SIZE
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Search bo-import.cz and return the first matching product listing."""
        try:
            resp = polite_get(
                self.http_client,
                f"{self.base_url.rstrip('/')}/search/",
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
        # Normalise: use path without query string as deduplication key
        base = url.split("?")[0]
        if base not in seen:
            seen.add(base)
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
) -> CompetitorListing | None:
    full_url = base_url.rstrip("/") + url_path if url_path.startswith("/") else url_path
    try:
        resp = polite_get(client, full_url, min_rps=rps)
        resp.raise_for_status()
    except httpx.HTTPError:
        return None

    return _parse_product_page(resp.text, competitor_id, full_url)


def _parse_product_page(
    html: str,
    competitor_id: str,
    url: str,
) -> CompetitorListing | None:
    """Extract product data from a bo-import.cz product page via JSON-LD."""
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

        # EAN — gtin13 field
        ean = _as_ean(data.get("gtin13")) or _as_ean(data.get("gtin"))

        # SKU: "KNI-8701300" → competitor_sku; strip prefix → mpn
        sku_raw: str | None = data.get("sku")
        competitor_sku = sku_raw or None
        mpn: str | None = None
        if sku_raw:
            # Strip known manufacturer prefixes (e.g. "KNI-", "WIH-")
            mpn_candidate = re.sub(r"^[A-Z]{2,5}\-", "", sku_raw).strip()
            if mpn_candidate:
                mpn = mpn_candidate

        # Price — CZK; convert to EUR
        offers = data.get("offers")
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if not isinstance(offers, dict):
            return None

        price_raw = offers.get("price")
        if price_raw is None:
            return None
        try:
            price_czk = float(str(price_raw).replace(",", "."))
        except ValueError:
            return None

        currency = (offers.get("priceCurrency") or "czk").lower()
        if currency in ("czk", "kč", "kc"):
            price_eur = round(price_czk / _CZK_EUR_RATE, 2)
        elif currency == "eur":
            price_eur = price_czk
        else:
            price_eur = round(price_czk / _CZK_EUR_RATE, 2)

        # Availability
        availability = offers.get("availability", "")
        in_stock: bool | None = None
        if "InStock" in availability:
            in_stock = True
        elif "OutOfStock" in availability:
            in_stock = False

        # Brand
        brand_data = data.get("brand")
        if isinstance(brand_data, dict):
            brand = brand_data.get("name") or None
        elif isinstance(brand_data, str):
            brand = brand_data or None
        else:
            brand = None

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _brand_to_slug(brand: str) -> str:
    """Convert brand name to bo-import.cz URL slug (lowercase, no accents)."""
    nfkd = unicodedata.normalize("NFKD", brand)
    ascii_brand = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "-", ascii_brand.lower()).strip("-")
