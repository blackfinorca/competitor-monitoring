"""Rebiop scraper.

Rebiop (rebiop.sk) runs on a custom DataSun e-commerce platform.
No Heureka/XML feed is published; product data is served as static HTML.

Strategy
--------
1. discover_feed(): probe HEUREKA_FEED_PATHS in case a feed is added later.

2. run_daily_iter() — full-catalogue crawl (overrides base):
   a. Fetch homepage to discover top-level category URLs (/catalog/{id}/{slug}).
   b. BFS over categories: pages with products are paginated; pages without
      products yield subcategory links for further crawling.
   c. Pagination: /catalog/{id}/{slug}/p/{n} (24 products/page).
   d. Each discovered product URL is fetched in parallel to extract EAN,
      internal code, title, price (with VAT), and stock status.

3. search_by_mpn(): GET /search/products?q={brand} {mpn} — used as fallback
   when run from match_products.py.

Category listing card structure:
  <div class="ctg-product-box" data-id="{sku}">
    <a href="detail/{id}/{slug}/cat/{cat_id}">   ← relative, no leading /
      <div class="name">{title}</div>
      <div class="ctg-prodbox-price">od <strong>{price} €</strong></div>
      <div class="ctg-prodbox-stock">Skladom</div>
    </a>
  </div>

Detail page fields (parsed from <dt>/<dd> pairs):
  "EAN kód:"   → ean
  "Kód:"       → competitor_sku (internal DataSun article code)
  "Cena s DPH" → price_eur (price including 20% Slovak VAT)
  <h1>         → title
  stock text   → in_stock (contains "Skladom" / "Nie je na sklade")
"""

import re
from dataclasses import replace
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.heureka_feed import HeurekaFeedMixin
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


REBIOP_CONFIG = {
    "id": "rebiop_sk",
    "name": "Rebiop",
    "url": "https://www.rebiop.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    "workers": 4,
}

_SEARCH_PATH = "search/products"
_SEARCH_PARAM = "q"

# Matches /catalog/{id}/{slug} links — both absolute and root-relative
_CATALOG_HREF_RE = re.compile(r'href="((?:https://www\.rebiop\.sk)?/catalog/\d+/[^"?#]+)"')
# Matches only product links that are inside a ctg-product-box card.
# The data-id attribute on the box is used for deduplication; the href is the URL.
_PRODUCT_BOX_RE = re.compile(
    r'class="ctg-product-box"[^>]*data-id="(\d+)".*?href="(detail/\d+/[^"]+)"',
    re.DOTALL,
)


class RebiopScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or REBIOP_CONFIG)
        self._rate_limit_rps: float = (config or REBIOP_CONFIG).get("rate_limit_rps", 1.0)
        self._workers: int = int((config or REBIOP_CONFIG).get("workers", 4))
        self.http_client = http_client or make_client(timeout=15.0)

    # ------------------------------------------------------------------
    # Full-catalogue crawl
    # ------------------------------------------------------------------

    def run_daily_iter(self, ag_catalogue: list[dict]):
        """BFS over all categories; fetch and yield product listings page-by-page.

        Products are scraped immediately as each category page is processed so
        that results stream out rather than waiting for the full URL collection
        phase to finish.
        """
        feed_url = self.discover_feed()
        if feed_url:
            yield from self.fetch_feed(feed_url)
            return

        competitor_id = self.competitor_id
        rps = self._rate_limit_rps
        workers = self._workers

        def _scrape(url: str) -> CompetitorListing | None:
            return _scrape_detail_page(get_thread_client(), url, competitor_id, rps=rps)

        try:
            resp = polite_get(self.http_client, self.base_url, min_rps=0.5)
            resp.raise_for_status()
        except Exception:
            return

        cat_queue = _extract_catalog_urls(resp.text, self.base_url)
        visited_cats: set[str] = set(cat_queue)
        seen_product_ids: set[str] = set()

        while cat_queue:
            cat_url = cat_queue.pop(0)
            page = 1
            while True:
                page_url = cat_url if page == 1 else f"{cat_url}/p/{page}"
                try:
                    resp = polite_get(
                        self.http_client, page_url, min_rps=rps
                    )
                except Exception:
                    break
                if resp.status_code != 200:
                    break

                html = resp.text

                for sub in _extract_catalog_urls(html, self.base_url):
                    if sub not in visited_cats:
                        visited_cats.add(sub)
                        cat_queue.append(sub)

                new_prods = _extract_new_product_urls(html, self.base_url, seen_product_ids)
                if not new_prods:
                    break

                # Fetch this page's products immediately — don't wait for full BFS
                yield from parallel_map(new_prods, _scrape, workers=workers)
                page += 1

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
            if 200 <= response.status_code < 300 and "xml" in response.headers.get(
                "content-type", ""
            ).lower():
                return str(response.url)
        return None

    # ------------------------------------------------------------------
    # Search fallback (used by match_products.py)
    # ------------------------------------------------------------------

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
        direct_hit = _parse_detail_page(resp.text, self.competitor_id, str(resp.url))
        if direct_hit is not None:
            return direct_hit

        listing = _parse_first_search_result(resp.text, self.competitor_id, self.base_url)
        if listing is None:
            return None

        detail_url = re.sub(r"/cat/\d+$", "", listing.url)
        detail_hit = _scrape_detail_page(
            self.http_client,
            detail_url,
            self.competitor_id,
            rps=self._rate_limit_rps,
        )
        if detail_hit is not None:
            return detail_hit
        return replace(listing, url=detail_url)


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

def _extract_catalog_urls(html: str, base_url: str) -> list[str]:
    """Return deduplicated /catalog/ URLs from raw HTML."""
    seen: set[str] = set()
    result: list[str] = []
    for href in _CATALOG_HREF_RE.findall(html):
        url = href if href.startswith("http") else base_url.rstrip("/") + href
        url = url.split("?")[0].rstrip("/")
        if url not in seen:
            seen.add(url)
            result.append(url)
    return result


def _extract_new_product_urls(
    html: str, base_url: str, seen_ids: set[str]
) -> list[str]:
    """Extract product URLs from ctg-product-box cards only; updates seen_ids in-place.

    Scopes extraction to listing cards so sidebar/recommendation links don't
    keep pagination alive past the last real page.
    """
    result: list[str] = []
    for pid, href in _PRODUCT_BOX_RE.findall(html):
        if pid in seen_ids:
            continue
        seen_ids.add(pid)
        clean = re.sub(r"/cat/\d+$", "", href)
        result.append(base_url.rstrip("/") + "/" + clean)
    return result


# ---------------------------------------------------------------------------
# Detail page scraping
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


def _parse_detail_page(
    html: str, competitor_id: str, url: str
) -> CompetitorListing | None:
    parser = _DetailParser()
    parser.feed(html)

    if not parser.title:
        return None

    price_text = parser.fields.get("Cena s DPH") or parser.fields.get("Cena bez DPH")
    if not price_text:
        return None
    try:
        price = _parse_price(price_text)
    except (ValueError, TypeError):
        return None

    ean = parser.fields.get("EAN kód:") or parser.fields.get("EAN kód")
    sku = parser.fields.get("Kód:") or parser.fields.get("Kód")

    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=sku,
        brand=None,
        mpn=None,
        ean=ean,
        title=parser.title,
        price_eur=price,
        currency="EUR",
        in_stock=parser.in_stock,
        url=url,
        scraped_at=datetime.now(UTC),
    )


class _DetailParser(HTMLParser):
    """Parse title, dt/dd fields, and stock status from a rebiop.sk product page."""

    def __init__(self):
        super().__init__()
        self.title: str | None = None
        self.fields: dict[str, str] = {}
        self.in_stock: bool | None = None
        self._in_h1 = False
        self._in_dt = False
        self._in_dd = False
        self._current_label: str | None = None
        self._dd_chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "h1":
            self._in_h1 = True
        elif tag == "dt":
            self._in_dt = True
            self._current_label = None
        elif tag == "dd":
            self._in_dd = True
            self._dd_chunks = []

    def handle_endtag(self, tag):
        if tag == "h1":
            self._in_h1 = False
        elif tag == "dt":
            self._in_dt = False
        elif tag == "dd":
            self._in_dd = False
            if self._current_label and self._dd_chunks:
                self.fields[self._current_label] = " ".join(self._dd_chunks).strip()
            self._dd_chunks = []

    def handle_data(self, data):
        text = data.strip()
        if not text:
            return
        if self._in_h1 and not self.title:
            self.title = text
        elif self._in_dt:
            self._current_label = text
        elif self._in_dd:
            self._dd_chunks.append(text)
            lower = text.lower()
            if "skladom" in lower:
                self.in_stock = "nie" not in lower


# ---------------------------------------------------------------------------
# Search result parsing (used by search_by_query)
# ---------------------------------------------------------------------------

def _parse_first_search_result(
    html: str, competitor_id: str, base_url: str
) -> CompetitorListing | None:
    parser = _SearchParser()
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


class _SearchParser(HTMLParser):
    """State-machine parser for rebiop.sk search results."""

    def __init__(self):
        super().__init__()
        self.products: list[dict] = []
        self._depth = 0
        self._in_product = False
        self._current: dict = {}
        self._field: str | None = None

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
            self._field = "price_outer"
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


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_price(text: str) -> float:
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
