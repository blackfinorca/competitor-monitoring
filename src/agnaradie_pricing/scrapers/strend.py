"""Strendpro scraper (full-catalog crawl for strendpro.sk)."""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from datetime import UTC, datetime
from html.parser import HTMLParser
from urllib.parse import urlparse, urlunparse

import httpx

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.http import make_client, polite_get

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.strendpro.sk"

STRENDPRO_CONFIG = {
    "id": "strendpro_sk",
    "name": "Strendpro",
    "url": _BASE_URL,
    "weight": 1.0,
    "rate_limit_rps": 1.0,
}

_PRICE_CLEAN_RE = re.compile(r"[^\d,.]")
_JSON_LD_RE = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)


class StrendproScraper(CompetitorScraper):
    def __init__(self, config: dict | None = None, http_client: httpx.Client | None = None):
        super().__init__(config or STRENDPRO_CONFIG)
        self._rate_limit_rps: float = (config or STRENDPRO_CONFIG).get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client(timeout=15.0)
        self.max_products: int = int(
            (config or STRENDPRO_CONFIG).get("max_products")
            or os.getenv("STRENDPRO_MAX_PRODUCTS", "0")
        )

    def discover_feed(self) -> str | None:
        return None

    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        del feed_url
        return []

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        del brand, mpn
        return None

    def search_by_query(self, query: str) -> CompetitorListing | None:
        del query
        return None

    def run_daily_iter(self, ag_catalogue: list[dict]):
        del ag_catalogue  # full-catalog mode
        seen_products: set[str] = set()
        yielded = 0
        categories = _fetch_category_urls(self.http_client, self.base_url)
        for category_url in categories:
            for page_html in _iter_category_pages(self.http_client, category_url, self._rate_limit_rps):
                for product_url in _extract_product_urls(page_html):
                    if product_url in seen_products:
                        continue
                    seen_products.add(product_url)
                    listing = _scrape_product_page(
                        self.http_client,
                        product_url,
                        competitor_id=self.competitor_id,
                        min_rps=self._rate_limit_rps,
                    )
                    if listing is None:
                        continue
                    yield listing
                    yielded += 1
                    if self.max_products and yielded >= self.max_products:
                        return

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        return list(self.run_daily_iter(ag_catalogue))


def _fetch_category_urls(client: httpx.Client, base_url: str) -> list[str]:
    response = polite_get(client, base_url, min_rps=0.5, referer=base_url)
    response.raise_for_status()
    return _extract_category_urls(response.text, base_url)


def _extract_category_urls(html: str, base_url: str) -> list[str]:
    parser = _HrefParser()
    parser.feed(html)
    seen: set[str] = set()
    out: list[str] = []
    for href in parser.hrefs:
        absolute = _to_absolute(href, base_url)
        if absolute is None:
            continue
        if "/c/" not in urlparse(absolute).path:
            continue
        if not re.search(r"/c/\d+/", urlparse(absolute).path):
            continue
        normalized = _normalize_url(absolute)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _extract_product_urls(html: str) -> list[str]:
    parser = _HrefParser()
    parser.feed(html)
    seen: set[str] = set()
    out: list[str] = []
    for href in parser.hrefs:
        absolute = _to_absolute(href, _BASE_URL)
        if absolute is None:
            continue
        if not re.search(r"/p/\d+/", urlparse(absolute).path):
            continue
        normalized = _normalize_url(absolute)
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _iter_category_pages(client: httpx.Client, start_url: str, min_rps: float):
    current = start_url
    seen: set[str] = set()
    while current and current not in seen:
        seen.add(current)
        try:
            resp = polite_get(client, current, min_rps=min_rps, referer=_BASE_URL)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("strendpro_sk: skipping category page %s due to HTTP error: %s", current, exc)
            return
        html = resp.text
        yield html
        next_url = _extract_next_page_url(html)
        current = _normalize_url(next_url) if next_url else None


def _extract_next_page_url(html: str) -> str | None:
    parser = _NextPageParser()
    parser.feed(html)
    if not parser.next_href:
        return None
    return _to_absolute(parser.next_href, _BASE_URL)


def _scrape_product_page(
    client: httpx.Client,
    url: str,
    *,
    competitor_id: str,
    min_rps: float,
) -> CompetitorListing | None:
    try:
        response = polite_get(client, url, min_rps=min_rps, referer=_BASE_URL)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("strendpro_sk: skipping %s due to HTTP error: %s", url, exc)
        return None

    parsed = _parse_product_detail(response.text)
    if parsed is None:
        return None
    return CompetitorListing(
        competitor_id=competitor_id,
        competitor_sku=parsed.product_code,
        brand=parsed.brand,
        mpn=None,
        ean=parsed.ean,
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
        *,
        title: str,
        brand: str | None,
        product_code: str | None,
        ean: str | None,
        price_eur: float,
        in_stock: bool | None,
    ) -> None:
        self.title = title
        self.brand = brand
        self.product_code = product_code
        self.ean = ean
        self.price_eur = price_eur
        self.in_stock = in_stock


def _parse_product_detail(html: str) -> _ParsedDetail | None:
    product = _parse_product_jsonld(html)
    if product is None:
        return None
    params = _parse_parameter_values(html)

    title = _normalize_space(product.get("name"))
    if not title:
        return None

    price_raw = product.get("price")
    if price_raw is None:
        return None
    try:
        price = _parse_price(str(price_raw))
    except (ValueError, TypeError):
        return None

    code = params.get("product_code") or _normalize_space(product.get("model")) or None
    ean = params.get("ean") or _normalize_space(product.get("gtin13")) or None
    brand = _normalize_space(product.get("brand")) or None
    availability = _normalize_space(product.get("availability"))

    return _ParsedDetail(
        title=title,
        brand=brand,
        product_code=code,
        ean=ean,
        price_eur=price,
        in_stock=_parse_stock(availability),
    )


def _parse_product_jsonld(html: str) -> dict[str, str] | None:
    for blob in _JSON_LD_RE.findall(html):
        payload = blob.strip()
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict):
            continue
        if str(data.get("@type", "")).lower() != "product":
            continue
        offers = data.get("offers")
        price = None
        availability = None
        if isinstance(offers, dict):
            price = offers.get("price")
            availability = offers.get("availability")
        brand = data.get("brand")
        if isinstance(brand, dict):
            brand = brand.get("name")
        elif brand is not None:
            brand = str(brand)
        return {
            "name": str(data.get("name") or ""),
            "brand": str(brand or ""),
            "model": str(data.get("model") or ""),
            "gtin13": str(data.get("gtin13") or ""),
            "price": str(price or ""),
            "availability": str(availability or ""),
        }
    return None


def _parse_parameter_values(html: str) -> dict[str, str]:
    parser = _ParameterParser()
    parser.feed(html)
    result: dict[str, str] = {}
    for key, value in parser.params:
        label = _normalize_label(key)
        normalized_value = _normalize_space(value)
        if not normalized_value:
            continue
        if label.startswith("kat. cislo") or label.startswith("kat cislo"):
            result["product_code"] = normalized_value
        elif label.startswith("ean kod") or label == "ean":
            result["ean"] = normalized_value
    return result


def _parse_price(text: str) -> float:
    cleaned = (
        text.replace("\xa0", "")
        .replace("&nbsp;", "")
        .replace("€", "")
        .replace(" ", "")
    )
    cleaned = _PRICE_CLEAN_RE.sub("", cleaned)
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(",", "")
    else:
        cleaned = cleaned.replace(",", ".")
    return float(cleaned)


def _parse_stock(text: str | None) -> bool | None:
    if not text:
        return None
    lower = text.lower()
    if "instock" in lower or "na sklade" in lower or "skladom" in lower:
        return True
    if "outofstock" in lower or "nie je skladom" in lower or "vypredan" in lower:
        return False
    return None


def _normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def _normalize_label(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    ascii_only = ascii_only.lower()
    ascii_only = re.sub(r"\s+", " ", ascii_only).strip()
    return ascii_only


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean_path = parsed.path.rstrip("/")
    if not clean_path:
        clean_path = "/"
    return urlunparse((parsed.scheme, parsed.netloc, clean_path, "", "", ""))


def _to_absolute(href: str, base_url: str) -> str | None:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{href}"
    return None


class _HrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        if tag.lower() != "a":
            return
        attr = dict(attrs)
        href = attr.get("href")
        if href:
            self.hrefs.append(href)


class _NextPageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.next_href: str | None = None

    def handle_starttag(self, tag: str, attrs):
        if self.next_href is not None or tag.lower() != "link":
            return
        attr = dict(attrs)
        rel = (attr.get("rel") or "").lower()
        href = attr.get("href")
        if "next" in rel and href:
            self.next_href = href


class _ParameterParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.params: list[tuple[str, str]] = []
        self._in_row = False
        self._field: str | None = None
        self._label_chunks: list[str] = []
        self._value_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs):
        attr = dict(attrs)
        classes = set((attr.get("class") or "").split())
        tag = tag.lower()
        if tag == "div" and "product-info__parameter" in classes:
            self._in_row = True
            self._field = None
            self._label_chunks = []
            self._value_chunks = []
            return
        if not self._in_row:
            return
        if tag == "strong":
            self._field = "label"
        elif tag == "span":
            self._field = "value"

    def handle_endtag(self, tag: str):
        tag = tag.lower()
        if self._in_row and tag == "div":
            label = _normalize_space("".join(self._label_chunks)).rstrip(":")
            value = _normalize_space("".join(self._value_chunks))
            if label and value:
                self.params.append((label, value))
            self._in_row = False
            self._field = None
            self._label_chunks = []
            self._value_chunks = []
            return
        if self._in_row and tag in {"strong", "span"}:
            self._field = None

    def handle_data(self, data: str):
        if not self._in_row or not self._field:
            return
        if self._field == "label":
            self._label_chunks.append(data)
        elif self._field == "value":
            self._value_chunks.append(data)


# Backwards-compatible alias for older imports.
StrendScraper = StrendproScraper
