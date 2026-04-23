"""AH Profi scraper.

AH Profi (ahprofi.sk) is a custom Slovak platform with no Heureka feed.
Category listing pages are JS-rendered; product detail pages have full
microdata in static HTML.

Strategy
--------
run_daily_iter() — full catalogue crawl via sitemap:
    1. Fetch /sitemap (XML index) to discover sitemap page count.
    2. Fetch each /sitemap?products=true&page=N to get ~1 000 product URLs.
       (~11 pages × 1 000 = ~11 000 products total)
    3. Fetch each product URL and parse microdata fields.
    Parallel fetching via get_thread_client() + parallel_map().

search_by_mpn(brand, mpn):
    1. Normalise MPN by stripping all separators: "87-01-250" → "8701250"
    2. GET /vysledky-vyhladavania?search_keyword={normalised_mpn}
    3. If the server redirects to a product page, parse it directly.
    4. If still on search results, no exact match → return None.

Product page data (microdata / og tags in static HTML):
    og:title              → title  (strip " | ahprofi.sk" suffix)
    itemprop="productID"  → competitor_sku / mpn
    itemprop="gtin13"     → ean
    itemprop="price" content="N.NN"  → price_eur
    itemprop="availability" href="…" → in_stock
"""

import re
from datetime import UTC, datetime
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import get_thread_client, make_client, parallel_map, polite_get


AHPROFI_CONFIG = {
    "id": "ahprofi_sk",
    "name": "AH Profi",
    "url": "https://www.ahprofi.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    "workers": 4,
    "search_path": "vysledky-vyhladavania",
    "search_query_param": "search_keyword",
}

_SEARCH_PAGE_MARKER = "vysledky-vyhladavania"
_SITEMAP_URL = "https://www.ahprofi.sk/sitemap"
_SITEMAP_PAGE_RE = re.compile(r'sitemap\?products=true&(?:amp;)?page=(\d+)')
_SITEMAP_URL_RE = re.compile(r'<loc>(https://www\.ahprofi\.sk/[^<]+)</loc>')


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
        self._workers: int = int((config or AHPROFI_CONFIG).get("workers", 4))

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
            try:
                resp = polite_get(get_thread_client(), url, min_rps=rps)
                resp.raise_for_status()
            except Exception:
                return None
            return _parse_product_page(resp.text, competitor_id, url)

        try:
            resp = polite_get(self.http_client, _SITEMAP_URL, min_rps=0.5)
            resp.raise_for_status()
        except Exception:
            return

        page_nums = sorted({int(p) for p in _SITEMAP_PAGE_RE.findall(resp.text)})
        if not page_nums:
            return

        for page_num in page_nums:
            sitemap_page_url = f"{_SITEMAP_URL}?products=true&page={page_num}"
            try:
                resp = polite_get(self.http_client, sitemap_page_url, min_rps=rps)
                resp.raise_for_status()
            except Exception:
                continue
            product_urls = _SITEMAP_URL_RE.findall(resp.text)
            if not product_urls:
                continue
            yield from parallel_map(product_urls, _scrape, workers=workers)

    # ------------------------------------------------------------------
    # Search fallback
    # ------------------------------------------------------------------

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        # Strip all separators — ahprofi indexes by condensed code e.g. "8701250"
        normalised = re.sub(r"[\-._\s]+", "", mpn).strip()
        if not normalised:
            return None
        return self._search_and_parse(normalised)

    def search_by_query(self, query: str) -> CompetitorListing | None:
        return self._search_and_parse(query.strip())

    def _search_and_parse(self, query: str) -> CompetitorListing | None:
        search_url = urljoin(self.base_url.rstrip("/") + "/", self._search_path)
        try:
            resp = polite_get(
                self.http_client,
                search_url,
                min_rps=self._rate_limit_rps,
                referer=self.base_url,
                params={self._search_param: query},
            )
            resp.raise_for_status()
        except Exception:
            return None

        final_url = str(resp.url)
        # If still on search results page, no exact match was found
        if _SEARCH_PAGE_MARKER in final_url:
            return None

        return _parse_product_page(resp.text, self.competitor_id, final_url)


# ---------------------------------------------------------------------------
# Product page parser — microdata + og tags
# ---------------------------------------------------------------------------

_OG_TITLE_RE = re.compile(r'og:title"[^>]*content="([^"]+)"')
_EAN_RE = re.compile(r'itemprop="gtin13"\s*>\s*(\d{8,13})\s*<')
_PRODUCT_ID_RE = re.compile(r'itemprop="productID"\s*>\s*([^\s<]+)\s*<')
_PRICE_RE = re.compile(r'itemprop="price"\s+content="([\d.]+)"')
_AVAIL_RE = re.compile(r'itemprop="availability"[^>]*href="([^"]+)"')


def _parse_product_page(
    html: str, competitor_id: str, url: str
) -> CompetitorListing | None:
    title_m = _OG_TITLE_RE.search(html)
    if not title_m:
        return None
    title = title_m.group(1).split(" | ")[0].strip()
    if not title:
        return None

    price_m = _PRICE_RE.search(html)
    if not price_m:
        return None
    try:
        price_eur = float(price_m.group(1))
    except ValueError:
        return None

    ean_m = _EAN_RE.search(html)
    pid_m = _PRODUCT_ID_RE.search(html)
    avail_m = _AVAIL_RE.search(html)

    in_stock: bool | None = None
    if avail_m:
        in_stock = "InStock" in avail_m.group(1)

    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=pid_m.group(1) if pid_m else None,
        brand=None,
        mpn=pid_m.group(1) if pid_m else None,
        ean=ean_m.group(1) if ean_m else None,
        title=title,
        price_eur=price_eur,
        currency="EUR",
        in_stock=in_stock,
        url=url,
        scraped_at=datetime.now(UTC),
    )
