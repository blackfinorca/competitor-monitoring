"""NaradieShop scraper.

NaradieShop (naradieshop.sk) runs on a ThirtyBees/PrestaShop platform.
No Heureka feed is available.

Strategy
--------
run_daily_iter() — full catalogue crawl via sitemap:
    1. Fetch /sitemap.xml to collect all category URLs (1-segment paths).
    2. For each category, paginate through ?p=1, ?p=2, ... until
       "pagination_next" is disabled (last page reached).
    3. Extract product URLs from <a class="product-name"> in listing cards.
    4. Parallel-fetch each product detail page; parse JSON-LD for full data.

search_by_mpn(brand, mpn):
    GET /vyhladavanie?search_query={brand} {mpn}
    Parse first result from <ul id="catprod-list"> / .ajax_block_product items.
    Enrich via detail page JSON-LD for EAN / brand.

Product detail page (JSON-LD):
    @type "Product"
    name          → title
    gtin13        → ean
    brand.name    → brand
    offers.price  → price_eur
    offers.availability → in_stock
"""

import json
import re
from datetime import UTC, datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.detail import enrich_from_detail_page
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get

_VOID_ELEMENTS = frozenset(
    "area base br col embed hr img input link meta param source track wbr".split()
)

NARADIESHOP_CONFIG = {
    "id": "naradieshop_sk",
    "name": "NaradieShop",
    "url": "https://www.naradieshop.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    "workers": 4,
}

_BASE_URL = "https://naradieshop.sk"
_SITEMAP_URL = "https://naradieshop.sk/sitemap.xml"

_SITEMAP_URL_RE = re.compile(r"<loc>(https://naradieshop\.sk/[^<]+)</loc>")
# Matches one product card block: from <li class="ajax_block_product …"> to the next one
_PRODUCT_CARD_RE = re.compile(
    r'<li[^>]*class="[^"]*ajax_block_product[^"]*"[^>]*>(.*?)(?=<li[^>]*class="[^"]*ajax_block_product|</ul>)',
    re.DOTALL,
)
_PRODUCT_URL_IN_CARD_RE = re.compile(r'class="product-name"[^>]*href="([^"?]+)"')
_NEXT_DISABLED_RE = re.compile(r'pagination_next[^"]*"[^>]*class="[^"]*disabled')


class NaradieShopScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or NARADIESHOP_CONFIG)
        self._rate_limit_rps: float = (config or NARADIESHOP_CONFIG).get("rate_limit_rps", 1.0)
        self._workers: int = int((config or NARADIESHOP_CONFIG).get("workers", 4))
        self.http_client = http_client or make_client()

    def discover_feed(self) -> str | None:
        return None

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        return []

    # ------------------------------------------------------------------
    # Full catalogue crawl via sitemap
    # ------------------------------------------------------------------

    def run_daily_iter(self, ag_catalogue: list[dict]):
        competitor_id = self.competitor_id
        rps = self._rate_limit_rps
        workers = self._workers

        def _scrape(url: str) -> CompetitorListing | None:
            return _scrape_detail_page(get_thread_client(), url, competitor_id, rps=rps)

        try:
            resp = polite_get(self.http_client, _SITEMAP_URL, min_rps=0.5)
            resp.raise_for_status()
        except Exception:
            return

        all_urls = _SITEMAP_URL_RE.findall(resp.text)
        # Category URLs have exactly one path segment (no nested slash)
        cat_urls = [u for u in all_urls if u.rstrip("/").count("/") == 3]
        if not cat_urls:
            return

        seen_product_urls: set[str] = set()

        for cat_url in cat_urls:
            page = 1
            while True:
                page_url = cat_url if page == 1 else f"{cat_url}?p={page}"
                try:
                    resp = polite_get(self.http_client, page_url, min_rps=rps)
                    resp.raise_for_status()
                except Exception:
                    break

                product_urls = [
                    u for u in _extract_listing_urls(resp.text)
                    if u not in seen_product_urls
                ]
                seen_product_urls.update(product_urls)

                if product_urls:
                    yield from parallel_map(product_urls, _scrape, workers=workers)

                if _NEXT_DISABLED_RE.search(resp.text) or not product_urls:
                    break
                page += 1

    # ------------------------------------------------------------------
    # Search fallback (manufacturer mode)
    # ------------------------------------------------------------------

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        search_url = f"{_BASE_URL}/vyhladavanie"
        try:
            response = polite_get(
                self.http_client,
                search_url,
                min_rps=self._rate_limit_rps,
                referer=_BASE_URL,
                params={"search_query": query},
            )
            response.raise_for_status()
        except Exception:
            return None
        listing = _parse_first_product(response.text, self.competitor_id)
        if listing is None:
            return None
        return enrich_from_detail_page(
            listing, self.http_client, min_rps=self._rate_limit_rps, referer=search_url
        )


# ---------------------------------------------------------------------------
# Listing page URL extractor
# ---------------------------------------------------------------------------

def _extract_listing_urls(html: str) -> list[str]:
    """Return product URLs from listing cards, skipping 'Na externom sklade' products.

    ThirtyBees shows externally-warehoused products in category listings but their
    detail pages return 404. Cards with class 'quantity-cat-ext-out' are those products.
    """
    urls: list[str] = []
    for m in _PRODUCT_CARD_RE.finditer(html):
        card = m.group(1)
        if "quantity-cat-ext-out" in card:
            continue
        url_m = _PRODUCT_URL_IN_CARD_RE.search(card)
        if url_m:
            urls.append(url_m.group(1))
    return urls


# ---------------------------------------------------------------------------
# Detail page scraper — JSON-LD
# ---------------------------------------------------------------------------

def _scrape_detail_page(
    client: httpx.Client,
    url: str,
    competitor_id: str,
    *,
    rps: float = 1.0,
) -> CompetitorListing | None:
    try:
        resp = polite_get(client, url, min_rps=rps)
        resp.raise_for_status()
    except Exception:
        return None
    return _parse_detail_page(resp.text, competitor_id, url)


def _parse_detail_page(html: str, competitor_id: str, url: str) -> CompetitorListing | None:
    """Extract product data from JSON-LD on a NaradieShop product detail page."""
    parser = _JsonLdParser()
    parser.feed(html)

    product: dict | None = None
    for payload in parser.payloads:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "Product":
            product = data
            break
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "Product":
                    product = item
                    break
            if product:
                break

    if not product:
        return None

    title = (product.get("name") or "").strip()
    if not title:
        return None

    offers = product.get("offers") or {}
    try:
        price_eur = float(offers.get("price", 0) or 0)
    except (ValueError, TypeError):
        return None
    if not price_eur:
        return None

    ean = _str(product.get("gtin13")) or _str(product.get("gtin8")) or _str(product.get("gtin"))

    brand_raw = product.get("brand")
    if isinstance(brand_raw, dict):
        brand = _str(brand_raw.get("name"))
    else:
        brand = _str(brand_raw)

    avail = _str(offers.get("availability")) or ""
    in_stock: bool | None = None
    if avail:
        in_stock = "InStock" in avail

    sku = _str(product.get("sku"))

    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=sku,
        brand=brand,
        mpn=None,
        ean=ean,
        title=title,
        price_eur=price_eur,
        currency="EUR",
        in_stock=in_stock,
        url=url,
        scraped_at=datetime.now(UTC),
    )


def _str(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


class _JsonLdParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.payloads: list[str] = []
        self._active = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "script" and dict(attrs).get("type") == "application/ld+json":
            self._active = True
            self._chunks = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._active:
            self.payloads.append("".join(self._chunks).strip())
            self._active = False
            self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._active:
            self._chunks.append(data)


# ---------------------------------------------------------------------------
# Search result parser (used by search_by_query)
# ---------------------------------------------------------------------------

class _ProductItem:
    __slots__ = ("title", "href", "price_raw", "in_stock")

    def __init__(self):
        self.title: str | None = None
        self.href: str = ""
        self.price_raw: str | None = None
        self.in_stock: bool | None = None


class _NaradieShopParser(HTMLParser):
    """Parser for NaradieShop ThirtyBees search results.

    Targets the product list after <ul id="catprod-list">.
    The HTML omits </li> closing tags, so item boundaries are detected by
    the start of the next ajax_block_product element.
    """

    def __init__(self):
        super().__init__()
        self.products: list[_ProductItem] = []
        self._in_list = False
        self._item: _ProductItem | None = None
        self._item_depth = 0
        self._tag_depth = 0
        self._collect: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())
        id_val = attr.get("id") or ""
        if tag not in _VOID_ELEMENTS:
            self._tag_depth += 1

        if not self._in_list:
            if id_val == "catprod-list":
                self._in_list = True
            return

        # New product item — also finalises previous one when </li> is omitted
        if "ajax_block_product" in classes:
            if self._item is not None:
                self.products.append(self._item)
            self._item = _ProductItem()
            self._item_depth = self._tag_depth
            return

        if self._item is None:
            return

        if tag == "a" and "product-name" in classes:
            raw_href = attr.get("href") or ""
            self._item.href = _clean_url(raw_href)
            self._collect = "title"

        elif tag == "span" and "price" in classes and "old-price" not in classes:
            self._collect = "price"

        elif tag == "div" and "quantity-cat-spec" in classes:
            self._collect = "stock"

    def handle_endtag(self, tag: str) -> None:
        if tag not in _VOID_ELEMENTS:
            self._tag_depth -= 1
        self._collect = None
        if self._item is not None and self._tag_depth < self._item_depth:
            self.products.append(self._item)
            self._item = None

    def handle_data(self, data: str) -> None:
        if self._item is None or self._collect is None:
            return
        text = unescape(data).strip()
        if not text:
            return
        if self._collect == "title":
            self._item.title = (self._item.title or "") + text
        elif self._collect == "price":
            self._item.price_raw = (self._item.price_raw or "") + text
        elif self._collect == "stock":
            lower = text.lower()
            if "skladom" in lower or "posledn" in lower:
                self._item.in_stock = True
            elif "nie je" in lower or "vypred" in lower:
                self._item.in_stock = False


def _parse_first_product(html: str, competitor_id: str) -> CompetitorListing | None:
    parser = _NaradieShopParser()
    parser.feed(html)
    if parser._item is not None:
        parser.products.append(parser._item)
    if not parser.products:
        return None
    item = parser.products[0]
    if not item.title or not item.price_raw:
        return None
    try:
        price = _parse_price(item.price_raw)
    except ValueError:
        return None
    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=None,
        brand=None,
        mpn=None,
        ean=None,
        title=item.title.strip(),
        price_eur=price,
        currency="EUR",
        in_stock=item.in_stock,
        url=item.href or _BASE_URL,
        scraped_at=datetime.now(UTC),
    )


def _parse_price(text: str) -> float:
    cleaned = (
        text.replace("€", "")
        .replace("\xa0", " ")
        .replace("EUR", "")
        .strip()
        .replace(" ", "")
        .replace(",", ".")
    )
    return float(cleaned)


def _clean_url(href: str) -> str:
    """Remove search_query and results params from a product URL."""
    parsed = urlparse(href)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
