"""Rebiop scraper.

Rebiop (rebiop.sk) runs on a custom DataSun e-commerce platform.
No Heureka/XML feed is published; product data is served as static HTML.

Strategy
--------
1. discover_feed(): probe HEUREKA_FEED_PATHS in case a feed is added later.
   Returns None if none are found.

2. search_by_query(query): GET /search/products?q={query}, parse the first
   div.ctg-product-box from the results page.

   Search result HTML structure:
     <div class="ctg-product-box" data-id="{sku}">
       <a href="detail/{id}/{slug}">
         <div class="name">{title}</div>
         <div class="ctg-info">
           <div class="ctg-prodbox-price">od <strong>{price} €</strong></div>
           <div class="ctg-prodbox-stock">Skladom</div>
         </div>
       </a>
     </div>

3. search_by_mpn(): delegates to search_by_query("{brand} {mpn}").
"""

import re
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.heureka_feed import HeurekaFeedMixin
from agnaradie_pricing.scrapers.http import make_client, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


REBIOP_CONFIG = {
    "id": "rebiop_sk",
    "name": "Rebiop",
    "url": "https://www.rebiop.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
}

_SEARCH_PATH = "search/products"
_SEARCH_PARAM = "q"


class RebiopScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or REBIOP_CONFIG)
        self._rate_limit_rps: float = (config or REBIOP_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client(timeout=15.0)

    def discover_feed(self) -> str | None:
        for path in HEUREKA_FEED_PATHS:
            url = urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))
            try:
                response = polite_get(self.http_client, url, min_rps=0.5)
            except httpx.HTTPError:
                continue
            if 200 <= response.status_code < 300 and "xml" in response.headers.get("content-type", "").lower():
                return str(response.url)
        return None

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        search_url = urljoin(self.base_url.rstrip("/") + "/", _SEARCH_PATH)
        try:
            resp = polite_get(
                self.http_client,
                search_url,
                min_rps=self._rate_limit_rps,
                referer=self.base_url,
                params={_SEARCH_PARAM: query},
            )
            resp.raise_for_status()
        except Exception:
            return None
        return _parse_first_result(resp.text, self.competitor_id, self.base_url)


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def _parse_first_result(html: str, competitor_id: str, base_url: str) -> CompetitorListing | None:
    parser = _RebiopParser()
    parser.feed(html)
    if not parser.products:
        return None
    p = parser.products[0]
    if not p.get("title") or not p.get("price_text"):
        return None
    try:
        price = _parse_price(p["price_text"])
    except (ValueError, TypeError):
        return None
    href = p.get("href", "")
    url = urljoin(base_url.rstrip("/") + "/", href) if href else base_url
    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=p.get("sku"),
        brand=None,
        mpn=None,
        ean=None,
        title=p["title"],
        price_eur=price,
        currency="EUR",
        in_stock=_parse_stock(p.get("stock_text")),
        url=url,
        scraped_at=datetime.now(UTC),
    )


def _parse_price(text: str) -> float:
    # Strip "od " prefix (Slovak for "from"), currency symbol, spaces
    cleaned = (
        text.replace("od ", "")
        .replace("€", "")
        .replace("\xa0", "")
        .replace(" ", "")
        .replace(",", ".")
        .strip()
    )
    return float(cleaned)


def _parse_stock(text: str | None) -> bool | None:
    if text is None:
        return None
    return "skladom" in text.lower()


class _RebiopParser(HTMLParser):
    """State-machine parser for rebiop.sk search results."""

    def __init__(self):
        super().__init__()
        self.products: list[dict] = []
        self._depth = 0           # nesting depth inside current product container
        self._in_product = False
        self._current: dict = {}
        self._field: str | None = None  # "title", "price", "stock"

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())

        if "ctg-product-box" in classes:
            self._in_product = True
            self._depth = 1
            self._current = {"sku": attr.get("data-id")}
            self._field = None
            return

        if not self._in_product:
            return

        self._depth += 1

        if tag == "a" and "href" in attr and not self._current.get("href"):
            href = attr["href"]
            if "detail/" in href:
                self._current["href"] = href

        if "name" in classes:
            self._field = "title"
        elif "ctg-prodbox-price" in classes:
            self._field = "price_outer"  # price is inside <strong>
        elif tag == "strong" and self._field == "price_outer":
            self._field = "price"
        elif "ctg-prodbox-stock" in classes:
            self._field = "stock"

    def handle_endtag(self, tag: str):
        if not self._in_product:
            return
        self._depth -= 1
        if self._depth <= 0:
            self.products.append(self._current)
            self._in_product = False
            self._current = {}
            self._field = None
        else:
            self._field = None

    def handle_data(self, data: str):
        if not self._in_product or self._field is None:
            return
        text = data.strip()
        if not text:
            return
        field_map = {"title": "title", "price": "price_text", "stock": "stock_text"}
        key = field_map.get(self._field)
        if key:
            self._current[key] = self._current.get(key, "") + text
