"""Strend scraper.

NOTE: strend.sk is a WordPress *content/brand site* for the "Strend Pro"
budget tool brand. Despite having WooCommerce installed, its category
pages (/dielna/, /stavba/, /zahradne-naradie/) are static WordPress pages
with no product listings. Search (`?s=`) returns no products. There is no
product catalogue to scrape.

The scraper is kept as a stub so the competitor_id remains registered in
the DB and the daily job does not crash. It will produce 0 listings until
the site adds a proper product shop.

Strategy
--------
1. discover_feed(): probe HEUREKA_FEED_PATHS in case a feed is added later.
   Returns None if none are found.

2. search_by_query(query): GET /?s={query}, parse the first WooCommerce
   product from the search results page.

   WooCommerce listing HTML selectors:
     Container:  li.product  or  .wc-block-grid__product
     URL:        a.woocommerce-LoopProduct-link[href]
     Title:      h2.woocommerce-loop-product__title
     Price:      span.woocommerce-Price-amount bdi
     SKU:        button.add_to_cart_button[data-product_id]
     Stock:      span.stock

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


STREND_CONFIG = {
    "id": "strend_sk",
    "name": "Strend",
    "url": "https://www.strend.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
}

_SEARCH_PARAM = "s"


class StrendScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or STREND_CONFIG)
        self._rate_limit_rps: float = (config or STREND_CONFIG).get("rate_limit_rps", 1.0)
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
        try:
            resp = polite_get(
                self.http_client,
                self.base_url,
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
    parser = _WooCommerceParser()
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
    url = p.get("href") or base_url
    if url and not url.startswith("http"):
        url = urljoin(base_url.rstrip("/") + "/", url)
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


_PRICE_CLEAN_RE = re.compile(r"[^\d,.]")


def _parse_price(text: str) -> float:
    cleaned = (
        text.replace("\xa0", "")
        .replace("&nbsp;", "")
        .replace("€", "")
        .replace(" ", "")
    )
    # Remove any remaining non-numeric except comma and dot
    cleaned = _PRICE_CLEAN_RE.sub("", cleaned)
    # Normalise decimal separator
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(",", "")  # thousands separator
    else:
        cleaned = cleaned.replace(",", ".")
    return float(cleaned)


def _parse_stock(text: str | None) -> bool | None:
    if text is None:
        return None
    lower = text.lower()
    if any(kw in lower for kw in ("in stock", "na sklade", "skladom", "instock")):
        return True
    if any(kw in lower for kw in ("out of stock", "nie je", "outofstock")):
        return False
    return None


class _WooCommerceParser(HTMLParser):
    """State-machine parser for WooCommerce search result pages."""

    def __init__(self):
        super().__init__()
        self.products: list[dict] = []
        self._in_product = False
        self._depth = 0
        self._current: dict = {}
        self._field: str | None = None
        self._in_bdi = False        # <bdi> inside price amount span

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())

        # Product container — classic loop or block grid
        if tag == "li" and ("product" in classes):
            self._in_product = True
            self._depth = 1
            self._current = {}
            self._field = None
            return

        if not self._in_product:
            return

        self._depth += 1

        # Product URL (classic loop)
        if tag == "a" and "woocommerce-LoopProduct-link" in classes:
            if not self._current.get("href"):
                self._current["href"] = attr.get("href", "")

        # Product URL (block grid)
        if tag == "a" and "wc-block-grid__product-link" in classes:
            if not self._current.get("href"):
                self._current["href"] = attr.get("href", "")

        # Title
        if tag == "h2" and "woocommerce-loop-product__title" in classes:
            self._field = "title"
        if tag in ("h2", "div", "span") and "wc-block-grid__product-title" in classes:
            self._field = "title"

        # Price wrapper
        if tag == "span" and "woocommerce-Price-amount" in classes:
            self._field = "price_outer"
        # bdi inside price
        if tag == "bdi" and self._field == "price_outer":
            self._field = "price"
            self._in_bdi = True

        # Add-to-cart button carries product_id as SKU
        if tag == "button" and "add_to_cart_button" in classes and "sku" not in self._current:
            pid = attr.get("data-product_id")
            if pid:
                self._current["sku"] = pid

        # Stock status
        if tag == "span" and "stock" in classes:
            self._field = "stock"

    def handle_endtag(self, tag: str):
        if not self._in_product:
            return
        if tag == "bdi":
            self._in_bdi = False
        self._depth -= 1
        if self._depth <= 0:
            self.products.append(self._current)
            self._in_product = False
            self._current = {}
            self._field = None
        else:
            # Only clear field on block-level closes to avoid losing multi-chunk text
            if tag in ("h2", "span", "div", "p") and self._field not in ("price_outer",):
                if not self._in_bdi:
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
