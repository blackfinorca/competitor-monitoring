"""ToolZone scraper.

ToolZone (toolzone.sk) runs on iKeloc/Keloc — a custom Czech e-commerce platform.
Product listing pages are fully AJAX-rendered (no static HTML products).
Product *detail* pages expose full JSON-LD with EAN (sku field) and MPN (mpn field).

Strategy
--------
1. discover_feed(): fetch the XML sitemap, collect all /produkt/*.htm URLs,
   then filter to only brands present in the AG catalogue (passed as config
   option 'brand_filter'). Returns a synthetic "sitemap://" URL as a signal
   so fetch_feed() is invoked.

2. fetch_feed(feed_url): the feed_url is ignored; iterate the filtered product
   URLs and scrape JSON-LD from each detail page (rate-limited).

   JSON-LD on product pages:
     @type: Product
     name:  {title}
     sku:   {EAN / gtin13}
     mpn:   {EDE article number, e.g. "E7451656010"}
     brand.name: {brand}
     offers.price: {EUR price}
     offers.availability: schema.org/InStock …

3. search_by_mpn(): not practical (AJAX search). Returns None.

Note: the sitemap is large (~41k URLs). For the PoC, filter by brand slug
using the 'brand_slugs' config list (default: all brands scraped).
"""

import json
import re
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import (
    chunked,
    get_thread_client,
    make_client,
    parallel_map,
    polite_get,
)


TOOLZONE_CONFIG = {
    "id": "toolzone_sk",
    "name": "ToolZone",
    "url": "https://www.toolzone.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    # Slugs of brands to scrape from the sitemap (empty = all brands)
    # Each slug matches a substring in the product URL path.
    # Add brand slugs as the AG catalogue grows.
    "brand_slugs": [],
}

_SITEMAP_URL = "https://www.toolzone.sk/sitemap.xml"
_FEED_SENTINEL = "sitemap://toolzone"

# Number of product URLs to scrape before yielding a batch to the orchestrator.
# Smaller = more frequent DB flushes; 200 is a good balance (≈ 25s at 8 RPS).
_FETCH_CHUNK = 200


class ToolZoneScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or TOOLZONE_CONFIG)
        self._rate_limit_rps: float = (config or TOOLZONE_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client(timeout=15.0)
        self._brand_slugs: list[str] = (config or TOOLZONE_CONFIG).get(
            "brand_slugs", []
        )

    def discover_feed(self) -> str | None:
        """Always return sentinel; actual URL list is built in fetch_feed."""
        return _FEED_SENTINEL

    def run_daily_iter(self, ag_catalogue):
        """Yield listings in chunks of _FETCH_CHUNK so the orchestrator can
        flush to DB after each chunk rather than waiting for all ~41k pages."""
        product_urls = self._get_product_urls()
        workers: int = int(self.config.get("workers", 1))

        def _scrape(url: str) -> CompetitorListing | None:
            try:
                return self._scrape_product_page(url)
            except Exception:
                return None

        for chunk in chunked(product_urls, _FETCH_CHUNK):
            yield from parallel_map(chunk, _scrape, workers=workers)

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        return list(self.run_daily_iter(None))

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Search toolzone.sk and scrape the first matching product detail page.

        ToolZone uses iKeloc/Keloc — the search results page may be partially
        AJAX-rendered.  We fetch the page and look for any /produkt/ hrefs in
        the raw HTML; if the platform returns them server-side we get a result,
        otherwise we fall back gracefully with None.
        """
        try:
            resp = polite_get(
                self.http_client,
                "https://www.toolzone.sk/vyhledavani/",
                min_rps=self._rate_limit_rps,
                referer="https://www.toolzone.sk/",
                params={"search_query": query},
            )
            resp.raise_for_status()
        except Exception:
            return None

        urls = re.findall(
            r'href=["\']?(https://www\.toolzone\.sk/produkt/[^"\'<\s]+)',
            resp.text,
        )
        if not urls:
            return None
        return self._scrape_product_page(urls[0])

    # ------------------------------------------------------------------
    # Manufacturer-page scraping (manufacturer_scrape.py entry point)
    # ------------------------------------------------------------------

    def get_manufacturer_slugs(self) -> list[tuple[str, str]]:
        """Fetch /vyrobci/ and return [(display_name, slug), ...] for all manufacturers.

        Slug is the URL path segment used in /vyrobce/{slug}/.
        """
        url = f"{self.base_url.rstrip('/')}/vyrobci/"
        try:
            resp = polite_get(self.http_client, url, min_rps=0.5)
            resp.raise_for_status()
        except httpx.HTTPError:
            return []
        slugs: list[tuple[str, str]] = []
        seen: set[str] = set()
        # Two link structures in the HTML:
        #   Featured (3): <a href="vyrobce/slug/"><img ...><h2>Name</h2><p>...</p></a>
        #   Others:       <a href="vyrobce/slug/"><img ... alt="Name"></a>
        for m in re.finditer(
            r'href="(?:[^"]*?/)?vyrobce/([^/"]+)/"[^>]*>([\s\S]*?)</a>',
            resp.text,
        ):
            slug = m.group(1).strip()
            if not slug or slug in seen:
                continue
            block = m.group(2)
            h2 = re.search(r'<h2[^>]*>([^<]+)</h2>', block)
            if h2:
                name = h2.group(1).strip()
            else:
                alt = re.search(r'alt="([^"]+)"', block)
                name = alt.group(1).strip() if alt else slug
            seen.add(slug)
            slugs.append((name, slug))
        return slugs

    def run_manufacturer_iter(self, manufacturer_slug: str):
        """Scrape all products for one manufacturer via /vyrobce/{slug}/katalog-stranaX.

        Yields CompetitorListing objects; flushes a parallel batch per page.
        """
        workers: int = int(self.config.get("workers", 1))
        competitor_id = self.competitor_id
        rps = self._rate_limit_rps
        base = self.base_url.rstrip("/")

        def _scrape(url: str) -> CompetitorListing | None:
            try:
                response = polite_get(
                    get_thread_client(), url, min_rps=rps,
                    referer=f"{base}/vyrobce/{manufacturer_slug}/",
                )
                response.raise_for_status()
                return _parse_product_page(response.text, competitor_id, url)
            except Exception:
                return None

        page = 1
        while True:
            if page == 1:
                page_url = f"{base}/vyrobce/{manufacturer_slug}/"
            else:
                page_url = f"{base}/vyrobce/{manufacturer_slug}/katalog-strana{page}"
            try:
                resp = polite_get(self.http_client, page_url, min_rps=rps)
            except httpx.HTTPError:
                break
            if resp.status_code != 200:
                break
            product_urls = _extract_manufacturer_page_product_urls(resp.text)
            if not product_urls:
                break
            yield from parallel_map(product_urls, _scrape, workers=workers)
            page += 1

    # ------------------------------------------------------------------
    def _get_product_urls(self) -> list[str]:
        response = polite_get(self.http_client, _SITEMAP_URL, min_rps=0.2, jitter=1.0)
        response.raise_for_status()
        all_urls = re.findall(
            r"<loc>(https://www\.toolzone\.sk/produkt/[^<]+)</loc>",
            response.text,
        )
        if not self._brand_slugs:
            return all_urls
        lower_slugs = [s.lower() for s in self._brand_slugs]
        return [u for u in all_urls if any(slug in u.lower() for slug in lower_slugs)]

    def _scrape_product_page(self, url: str) -> CompetitorListing | None:
        # get_thread_client() returns a thread-local client, so parallel workers
        # each use their own connection pool without sharing self.http_client.
        response = polite_get(
            get_thread_client(),
            url,
            min_rps=self._rate_limit_rps,
            referer="https://www.toolzone.sk/",
        )
        response.raise_for_status()
        return _parse_product_page(response.text, self.competitor_id, url)


# ---------------------------------------------------------------------------
# Manufacturer listing page helpers
# ---------------------------------------------------------------------------

_MFR_PRODUCT_URL_RE = re.compile(
    r'href="((?:https?://www\.toolzone\.sk)?/?produkt/[^"]+\.htm)"'
)

_TZ_BASE = "https://www.toolzone.sk"


def _extract_manufacturer_page_product_urls(html: str) -> list[str]:
    """Return deduplicated absolute product URLs from a manufacturer listing page.

    ToolZone manufacturer pages use relative paths (produkt/foo.htm), so we
    normalise them to absolute URLs.
    """
    seen: set[str] = set()
    result: list[str] = []
    for raw in _MFR_PRODUCT_URL_RE.findall(html):
        url = raw if raw.startswith("http") else f"{_TZ_BASE}/{raw.lstrip('/')}"
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


# ---------------------------------------------------------------------------
# JSON-LD extraction from product detail page
# ---------------------------------------------------------------------------

def _parse_product_page(
    html: str, competitor_id: str, page_url: str
) -> CompetitorListing | None:
    # Extract GTM EUR price first (more reliable than JSON-LD which uses CZK)
    gtm_price = _extract_gtm_eur_price(html)

    # Extract all JSON-LD blocks
    parser = _JsonLdParser()
    parser.feed(html)
    for payload in parser.payloads:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict) or data.get("@type") != "Product":
            continue
        listing = _listing_from_jsonld(data, competitor_id, page_url)
        if listing is not None and gtm_price is not None:
            # Replace the CZK-converted price with the actual EUR price from GTM
            from dataclasses import asdict
            listing = listing.__class__(**{**asdict(listing), "price_eur": gtm_price})
        return listing
    return None


def _extract_gtm_eur_price(html: str) -> float | None:
    """Extract EUR price from GTM dataLayer ecommerce.detail block.

    The block looks like:
        "ecommerce": {
            "detail": {
                "actionField": {"list": "..."},   ← nested } breaks [^}]* patterns
                "currencyCode": "EUR",
                "products": [{"name": "...", "price": 48, ...}]
            }
        }

    Strategy: find the first occurrence of `"currencyCode": "EUR"` and then
    the first "price" value that follows it in the same push() call.
    """
    match = re.search(
        r'"currencyCode"\s*:\s*"EUR".*?"price"\s*:\s*"?([\d]+(?:\.\d+)?)"?',
        html,
        re.DOTALL,
    )
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None


def _listing_from_jsonld(
    data: dict, competitor_id: str, page_url: str
) -> CompetitorListing | None:
    offers = data.get("offers") if isinstance(data.get("offers"), dict) else {}
    brand = data.get("brand")
    brand_name = brand.get("name") if isinstance(brand, dict) else brand

    price = offers.get("price")
    if price is None:
        return None

    # ToolZone's JSON-LD has prices in CZK — fall back to GTM dataLayer EUR price
    currency = offers.get("priceCurrency", "EUR")
    price_eur = _convert_to_eur(float(price), currency)
    if price_eur is None:
        return None

    availability = offers.get("availability", "")
    in_stock: bool | None = None
    if "InStock" in availability:
        in_stock = True
    elif "OutOfStock" in availability:
        in_stock = False

    # EAN: prefer explicit gtin13/gtin8/gtin fields; fall back to sku if all-numeric
    ean = (
        _as_ean(data.get("gtin13"))
        or _as_ean(data.get("gtin8"))
        or _as_ean(data.get("gtin"))
        or _as_ean(data.get("sku"))
    )

    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=data.get("sku"),   # internal ToolZone SKU
        brand=brand_name,
        mpn=data.get("mpn"),              # manufacturer part number
        ean=ean,
        title=data.get("name", ""),
        price_eur=price_eur,
        currency="EUR",
        in_stock=in_stock,
        url=offers.get("url") or page_url,
        scraped_at=datetime.now(UTC),
    )


_EAN_RE = re.compile(r"^\d{8}(?:\d{4,5})?$")


def _as_ean(value) -> str | None:
    """Return value if it looks like an EAN barcode (8, 12 or 13 digits), else None."""
    if value is None:
        return None
    s = str(value).strip()
    return s if _EAN_RE.match(s) else None


def _convert_to_eur(price: float, currency: str) -> float | None:
    """ToolZone's JSON-LD sometimes reports CZK; try to normalise to EUR.

    The GTM dataLayer on the same page has the EUR price, but parsing it is
    fragile. Use a rough fixed rate (1 EUR ≈ 25 CZK) as a fallback.
    Real implementation should use an exchange-rate API or parse the dataLayer.
    """
    if currency == "EUR":
        return price
    if currency == "CZK":
        # Approximate conversion; this will be replaced by GTM price extraction below
        return round(price / 25.0, 2)
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _JsonLdParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.payloads: list[str] = []
        self._in_jsonld = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        if tag == "script" and attr.get("type") == "application/ld+json":
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
