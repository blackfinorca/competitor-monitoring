"""NaradieShop scraper.

NaradieShop (naradieshop.sk) runs on a ThirtyBees/PrestaShop platform.
No Heureka feed is available.

Search URL  : /vyhladavanie?search_query={brand}+{mpn}
Results in  : <ul id="catprod-list">
                <li class="ajax_block_product ...">
                  <div class="product-container" itemscope itemtype="schema.org/Product">
                    <a class="product-name" href="{url}">{title}</a>
                    <span class="price">34,00 €</span>
                    <meta itemprop="priceCurrency" content="..." />
                    ...
                  </div>
                </li>
              </ul>

The product title typically embeds the manufacturer part number
(e.g. "Knipex 8701300 kliešte prestaviteľné Cobra 300mm").
EAN (gtin13) is only available on the product detail page (JSON-LD).
"""

from datetime import UTC, datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.detail import enrich_from_detail_page
from agnaradie_pricing.scrapers.http import make_client, polite_get

# Void elements do not get a matching handle_endtag call.
_VOID_ELEMENTS = frozenset(
    "area base br col embed hr img input link meta param source track wbr".split()
)


NARADIESHOP_CONFIG = {
    "id": "naradieshop_sk",
    "name": "NaradieShop",
    "url": "https://www.naradieshop.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
}

_BASE_URL = "https://naradieshop.sk"


class NaradieShopScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or NARADIESHOP_CONFIG)
        self._rate_limit_rps: float = (config or NARADIESHOP_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client()

    def discover_feed(self) -> str | None:
        return None

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        return []

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        search_url = f"{_BASE_URL}/vyhladavanie"
        response = polite_get(
            self.http_client,
            search_url,
            min_rps=self._rate_limit_rps,
            referer=_BASE_URL,
            params={"search_query": query},
        )
        response.raise_for_status()
        listing = _parse_first_product(response.text, self.competitor_id)
        if listing is None:
            return None
        return enrich_from_detail_page(
            listing, self.http_client, min_rps=self._rate_limit_rps, referer=search_url
        )


# ---------------------------------------------------------------------------
# HTML parsing
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
    Per product item:
      <a class="product-name" href="{url}">{title}</a>
      <span class="price">{price}</span>
      <div class="quantity-cat-spec"> contains "Posledný kus" / stock text
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

        # New product item
        if self._item is None and "ajax_block_product" in classes:
            self._item = _ProductItem()
            self._item_depth = self._tag_depth
            return

        if self._item is None:
            return

        # product-name link → title + URL
        if tag == "a" and "product-name" in classes:
            raw_href = attr.get("href") or ""
            # strip query string (search_query=...) from the URL
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
    # Keep just scheme+netloc+path
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
