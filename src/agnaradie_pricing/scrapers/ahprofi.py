"""AH Profi scraper.

AH Profi (ahprofi.sk) is a custom Slovak platform with no Heureka feed.
Product data is present in static HTML on the search results page.

Search URL : /vysledky-vyhladavania?search_keyword={brand}+{mpn}
Structure  : <div class="item col col-special bg-white relative" itemprop="itemListElement">
               <span class="user_code" title="Kód produktu">{mpn}</span>
               <a href="{relative_url}" itemprop="url">
                 <span itemprop="name">{title}</span>
               </a>
               <span class="final-price row top">
                 <strong class="..."><span class="price">€ {price}</span></strong>
               </span>
               <span class="availability"><span class="green">Skladom</span></span>
             </div>

Note: MPN is stored in user_code and maps to the manufacturer part number.
      No EAN is exposed in search results.
"""

import re
from datetime import UTC, datetime
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.detail import enrich_from_detail_page
from agnaradie_pricing.scrapers.http import make_client, polite_get


AHPROFI_CONFIG = {
    "id": "ahprofi_sk",
    "name": "AH Profi",
    "url": "https://www.ahprofi.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    "search_path": "vysledky-vyhladavania",
    "search_query_param": "search_keyword",
}


class AhProfiScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or AHPROFI_CONFIG)
        self._rate_limit_rps: float = (config or AHPROFI_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client()
        self._search_path: str = (config or AHPROFI_CONFIG).get(
            "search_path", "vysledky-vyhladavania"
        )
        self._search_param: str = (config or AHPROFI_CONFIG).get(
            "search_query_param", "search_keyword"
        )

    # AH Profi has no Heureka feed
    def discover_feed(self) -> str | None:
        return None

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        return []

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        # AH Profi indexes MPN with spaces between digits blocks better
        # e.g. "87-01-250" → "87 01 250"
        mpn_spaced = re.sub(r"[\-._]+", " ", mpn).strip()
        return self.search_by_query(f"{brand} {mpn_spaced}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        url = urljoin(self.base_url.rstrip("/") + "/", self._search_path)
        response = polite_get(
            self.http_client,
            url,
            min_rps=self._rate_limit_rps,
            referer=self.base_url,
            params={self._search_param: query},
        )
        response.raise_for_status()
        listing = _parse_first_product(response.text, self.base_url, self.competitor_id)
        if listing is None:
            return None
        return enrich_from_detail_page(
            listing, self.http_client, min_rps=self._rate_limit_rps, referer=url
        )


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

class _ProductItem:
    __slots__ = ("title", "href", "mpn", "price_raw", "availability")

    def __init__(self):
        self.title: str | None = None
        self.href: str = ""
        self.mpn: str | None = None
        self.price_raw: str | None = None
        self.availability: str | None = None


# Void elements do not get a matching handle_endtag call from HTMLParser,
# so we must not increment depth for them.
_VOID_ELEMENTS = frozenset(
    "area base br col embed hr img input link meta param source track wbr".split()
)


class _AhProfiHTMLParser(HTMLParser):
    """State-machine parser for AH Profi search results.

    Walks the DOM looking for:
      .listing-products → enters product-list context
      .item.col-special  → start of one product item
      span.user_code     → collects MPN text
      a[itemprop=url]    → collects href; next span[itemprop=name] → title
      span.price (inside .final-price) → collects price text
      span.green (inside .availability) → stock status
    """

    def __init__(self):
        super().__init__()
        self.products: list[_ProductItem] = []
        self._in_listing = False
        self._item: _ProductItem | None = None
        # per-item nesting depth tracker
        self._item_depth: int = 0
        self._tag_depth: int = 0

        # field collection flags
        self._collect: str | None = None  # 'mpn' | 'title' | 'price' | 'stock'

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())
        if tag not in _VOID_ELEMENTS:
            self._tag_depth += 1

        if not self._in_listing:
            if "listing-products" in classes:
                self._in_listing = True
            return

        # New product item
        if (
            self._item is None
            and "item" in classes
            and "col-special" in classes
            and "bg-white" in classes
        ):
            self._item = _ProductItem()
            self._item_depth = self._tag_depth
            return

        if self._item is None:
            return

        # --- inside a product item ---
        itemprop = attr.get("itemprop", "")

        if tag == "span" and "user_code" in classes:
            self._collect = "mpn"

        elif tag == "a" and itemprop == "url":
            href = attr.get("href") or ""
            self._item.href = href

        elif tag == "span" and itemprop == "name":
            self._collect = "title"

        elif tag == "span" and "price" in classes and "line-through" not in classes:
            # only collect price if we're inside final-price; check class hierarchy via parent
            # simplification: collect price text, keep last value → final-price is printed last
            self._collect = "price"

        elif tag == "span" and "green" in classes:
            self._collect = "stock"

    def handle_endtag(self, tag: str) -> None:
        self._tag_depth -= 1
        self._collect = None

        if self._item is not None and self._tag_depth < self._item_depth:
            # Exited the product item div
            self.products.append(self._item)
            self._item = None

    def handle_data(self, data: str) -> None:
        if self._item is None or self._collect is None:
            return
        text = unescape(data).strip()
        if not text:
            return
        if self._collect == "mpn":
            self._item.mpn = text
        elif self._collect == "title":
            self._item.title = (self._item.title or "") + text
        elif self._collect == "price":
            # Keep updating — the final-price span comes after the crossed-out old price
            self._item.price_raw = (self._item.price_raw or "") + text
        elif self._collect == "stock":
            self._item.availability = text


def _parse_first_product(
    html: str, base_url: str, competitor_id: str
) -> CompetitorListing | None:
    parser = _AhProfiHTMLParser()
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
        competitor_sku=item.mpn,
        brand=None,  # not exposed in listing
        mpn=item.mpn,
        ean=None,
        title=item.title.strip(),
        price_eur=price,
        currency="EUR",
        in_stock=_parse_stock(item.availability),
        url=urljoin(base_url.rstrip("/") + "/", item.href),
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


def _parse_stock(text: str | None) -> bool | None:
    if text is None:
        return None
    lower = text.lower()
    if "skladom" in lower or "instock" in lower:
        return True
    if "out" in lower or "nedostupn" in lower:
        return False
    return None
