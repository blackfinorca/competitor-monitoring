"""Fermatshop scraper (fermatshop_sk config, backed by fermatshop.sk domain).

Site notes
----------
- Competitor ID in config is ``fermatshop_sk``.
- Live domain with catalogue is ``https://www.fermatshop.sk``.
- No reliable EAN found on product pages, so we emit a deterministic placeholder:
  ``NOEAN-{product_code}`` when product code exists.
"""

from __future__ import annotations

import os
import re
import logging
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urlparse

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import make_client, polite_get
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS

logger = logging.getLogger(__name__)


FERMATSHOP_CONFIG = {
    "id": "fermatshop_sk",
    "name": "Fermatshop",
    "url": "https://www.fermatshop.sk",
    "weight": 1.0,
    "rate_limit_rps": 1.0,
}

_SITEMAP_URL = "https://www.fermatshop.sk/sitemap.xml"
_PRICE_CLEAN_RE = re.compile(r"[^\d,.]")
_PRODUCT_PATH_RE = re.compile(r"^/[^/]+/[^/]+/?$")
_EXCLUDE_PRODUCT_PREFIXES = (
    "/registracia/",
    "/prihlasenie/",
    "/nakupny-kosik/",
    "/product-result/",
)


class FermatshopScraper(CompetitorScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or FERMATSHOP_CONFIG)
        self._rate_limit_rps: float = (config or FERMATSHOP_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client(timeout=15.0)
        self.max_products: int = int(
            (config or FERMATSHOP_CONFIG).get("max_products")
            or os.getenv("FERMATSHOP_MAX_PRODUCTS", os.getenv("FERANT_MAX_PRODUCTS", "0"))
        )

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
        # Full-catalog mode uses sitemap crawl.
        return []

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return None

    def search_by_query(self, query: str) -> CompetitorListing | None:
        return None

    def run_daily_iter(self, ag_catalogue: list[dict]):
        del ag_catalogue  # unused: this scraper crawls full catalogue.
        for idx, url in enumerate(_extract_product_urls_from_sitemap(self.http_client), start=1):
            if self.max_products and idx > self.max_products:
                break
            listing = _scrape_product_page(
                self.http_client, url, self.competitor_id, min_rps=self._rate_limit_rps
            )
            if listing is not None:
                yield listing

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        return list(self.run_daily_iter(ag_catalogue))


def _extract_product_urls_from_sitemap(client: httpx.Client) -> list[str]:
    response = polite_get(client, _SITEMAP_URL, min_rps=0.5)
    response.raise_for_status()
    parser = _SitemapParser()
    parser.feed(response.text)
    urls: list[str] = []
    seen: set[str] = set()
    for url in parser.urls:
        parsed = urlparse(url)
        path = parsed.path
        if not _PRODUCT_PATH_RE.match(path):
            continue
        if path.endswith("//"):
            continue
        if any(path.startswith(prefix) for prefix in _EXCLUDE_PRODUCT_PREFIXES):
            continue
        # Keep only likely product detail links: category/product-slug
        if path in seen:
            continue
        seen.add(path)
        urls.append(url.rstrip("/") + "/")
    return urls


def _scrape_product_page(
    client: httpx.Client, url: str, competitor_id: str, min_rps: float
) -> CompetitorListing | None:
    try:
        response = polite_get(client, url, min_rps=min_rps, referer="https://www.fermatshop.sk/")
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("fermatshop_sk: skipping %s due to HTTP error: %s", url, exc)
        return None
    parsed = _parse_product_detail(response.text)
    if parsed is None:
        return None
    if not parsed.title or parsed.price_eur is None:
        return None
    ean_placeholder = f"NOEAN-{parsed.product_code}" if parsed.product_code else None
    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=parsed.product_code,
        brand=parsed.brand,
        mpn=None,
        ean=ean_placeholder,
        title=parsed.title,
        price_eur=parsed.price_eur,
        currency="EUR",
        in_stock=parsed.in_stock,
        url=url,
        scraped_at=datetime.now(UTC),
    )


class _ParsedDetail:
    def __init__(
        self,
        title: str | None = None,
        brand: str | None = None,
        product_code: str | None = None,
        price_eur: float | None = None,
        in_stock: bool | None = None,
    ) -> None:
        self.title = title
        self.brand = brand
        self.product_code = product_code
        self.price_eur = price_eur
        self.in_stock = in_stock


def _parse_product_detail(html: str) -> _ParsedDetail | None:
    parser = _ProductDetailParser()
    parser.feed(html)

    title = _normalize_space(parser.fields.get("title"))
    brand = _normalize_space(parser.fields.get("brand"))
    code = _normalize_space(parser.fields.get("code"))
    price_text = _normalize_space(parser.fields.get("price"))
    stock_text = _normalize_space(parser.fields.get("stock"))

    if not title or not price_text:
        return None

    try:
        price = _parse_price(price_text)
    except (ValueError, TypeError):
        return None

    return _ParsedDetail(
        title=title,
        brand=brand or None,
        product_code=code or None,
        price_eur=price,
        in_stock=_parse_stock(stock_text),
    )


def _parse_price(text: str) -> float:
    cleaned = (
        text.replace("\xa0", "")
        .replace("&nbsp;", "")
        .replace("€", "")
        .replace(" ", "")
    )
    cleaned = _PRICE_CLEAN_RE.sub("", cleaned).replace(",", ".")
    return float(cleaned)


def _parse_stock(text: str | None) -> bool | None:
    if not text:
        return None
    lower = text.lower()
    if any(token in lower for token in ("na sklade", "skladom")):
        return True
    if any(token in lower for token in ("vypredané", "nie je skladom", "nedostup")):
        return False
    return None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


class _SitemapParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.urls: list[str] = []
        self._in_loc = False
        self._chunk: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        if tag.lower() == "loc":
            self._in_loc = True
            self._chunk = []

    def handle_endtag(self, tag: str):
        if tag.lower() == "loc" and self._in_loc:
            text = "".join(self._chunk).strip()
            if text.startswith("https://www.fermatshop.sk/"):
                self.urls.append(text)
            self._in_loc = False
            self._chunk = []

    def handle_data(self, data: str):
        if self._in_loc:
            self._chunk.append(data)


class _ProductDetailParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.fields: dict[str, str] = {}
        self._current: str | None = None
        self._in_product_sku = False

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())
        tag = tag.lower()

        if tag == "h1" and "flypage-h1" in classes:
            self._current = "title"
            return
        if tag == "span" and "manu_name" in classes:
            self._current = "brand"
            return
        if tag == "div" and "flypage_sku" in classes:
            self._in_product_sku = True
            return
        if tag == "span" and "product_sku_value" in classes and self._in_product_sku:
            self._current = "code"
            return
        if tag == "span" and attr.get("id") == "product-detail-price-value":
            self._current = "price"
            return
        if tag == "span" and "shop_product_availability_value" in classes:
            self._current = "stock"
            return

    def handle_endtag(self, tag: str):
        if tag.lower() == "div" and self._in_product_sku:
            self._in_product_sku = False
        self._current = None

    def handle_data(self, data: str):
        if not self._current:
            return
        value = data.strip()
        if not value:
            return
        self.fields[self._current] = self.fields.get(self._current, "") + value


# Backwards-compatible alias for existing imports.
FerantScraper = FermatshopScraper
