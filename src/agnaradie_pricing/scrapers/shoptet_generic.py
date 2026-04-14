"""Generic Shoptet scraper fallback."""

import json
from datetime import UTC, datetime
from html.parser import HTMLParser as StdlibHTMLParser
from urllib.parse import urljoin

import httpx

from agnaradie_pricing.scrapers.detail import enrich_from_detail_page
from agnaradie_pricing.scrapers.http import make_client, polite_get

try:
    from selectolax.parser import HTMLParser as SelectolaxHTMLParser
except ModuleNotFoundError:
    SelectolaxHTMLParser = None

from agnaradie_pricing.scrapers.base import CompetitorListing, CompetitorScraper
from agnaradie_pricing.scrapers.heureka_feed import HeurekaFeedMixin
from agnaradie_pricing.scrapers.inspection import HEUREKA_FEED_PATHS


class ShoptetGenericScraper(HeurekaFeedMixin, CompetitorScraper):
    def __init__(self, config: dict, http_client: httpx.Client | None = None):
        super().__init__(config)
        self._rate_limit_rps: float = config.get("rate_limit_rps", 1.0)
        self.http_client = http_client or make_client()
        self.search_path = config.get("search_path", "vyhledavani/")
        self.search_query_param = config.get("search_query_param", "string")

    def discover_feed(self) -> str | None:
        for path in HEUREKA_FEED_PATHS:
            url = urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))
            try:
                response = polite_get(self.http_client, url, min_rps=0.5)
            except httpx.HTTPError:
                continue
            if 200 <= response.status_code < 300:
                return str(response.url)
        return None

    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        return self.search_by_query(f"{brand} {mpn}".strip())

    def search_by_query(self, query: str) -> CompetitorListing | None:
        search_url = urljoin(self.base_url.rstrip("/") + "/", self.search_path)
        response = polite_get(
            self.http_client,
            search_url,
            min_rps=self._rate_limit_rps,
            referer=self.base_url,
            params={self.search_query_param: query},
        )
        response.raise_for_status()
        listing = self._parse_first_search_result(response.text)
        if listing is None:
            return None
        return enrich_from_detail_page(
            listing, self.http_client, min_rps=self._rate_limit_rps, referer=search_url
        )

    def _parse_first_search_result(self, html: str) -> CompetitorListing | None:
        product = _parse_product(html)
        if product is None or not product.title or not product.price:
            return None

        return CompetitorListing(
            competitor_id=self.competitor_id,
            competitor_sku=product.competitor_sku,
            brand=product.brand,
            mpn=product.mpn,
            ean=product.ean,
            title=product.title,
            price_eur=_parse_price(product.price),
            currency=product.currency,
            in_stock=_parse_stock(product.availability),
            url=urljoin(self.base_url.rstrip("/") + "/", product.href),
            scraped_at=datetime.now(UTC),
        )


class ParsedProduct:
    def __init__(
        self,
        title: str | None = None,
        href: str = "",
        brand: str | None = None,
        mpn: str | None = None,
        price: str | None = None,
        availability: str | None = None,
        ean: str | None = None,
        competitor_sku: str | None = None,
        currency: str = "EUR",
    ):
        self.title = title
        self.href = href
        self.brand = brand
        self.mpn = mpn
        self.price = price
        self.availability = availability
        self.ean = ean
        self.competitor_sku = competitor_sku
        self.currency = currency


def _parse_product(html: str) -> ParsedProduct | None:
    jsonld_product = _parse_product_from_jsonld(html)
    if jsonld_product is not None:
        return jsonld_product
    if SelectolaxHTMLParser is not None:
        return _parse_product_with_selectolax(html)
    return _parse_product_with_stdlib(html)


def _parse_product_from_jsonld(html: str) -> ParsedProduct | None:
    parser = JsonLdHTMLParser()
    parser.feed(html)
    for payload in parser.payloads:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            continue
        product = _first_jsonld_product(data)
        if product is not None:
            return product
    return None


def _first_jsonld_product(data) -> ParsedProduct | None:
    if isinstance(data, list):
        for item in data:
            product = _first_jsonld_product(item)
            if product is not None:
                return product
        return None

    if not isinstance(data, dict):
        return None

    if data.get("@type") == "Product":
        return _product_from_jsonld_item(data)

    if data.get("@type") == "ItemList":
        for element in data.get("itemListElement", []):
            if isinstance(element, dict):
                product = _first_jsonld_product(element.get("item"))
                if product is not None:
                    return product

    return None


def _product_from_jsonld_item(item: dict) -> ParsedProduct:
    offers = item.get("offers") if isinstance(item.get("offers"), dict) else {}
    brand = item.get("brand")
    brand_name = brand.get("name") if isinstance(brand, dict) else brand
    identifier = item.get("identifier")
    price = offers.get("price")
    return ParsedProduct(
        title=item.get("name"),
        href=item.get("url") or "",
        brand=brand_name,
        mpn=item.get("sku"),
        price=str(price) if price is not None else None,
        availability=offers.get("availability"),
        ean=item.get("gtin13") or item.get("gtin8") or item.get("gtin"),
        competitor_sku=str(identifier) if identifier is not None else None,
        currency=offers.get("priceCurrency") or "EUR",
    )


def _parse_product_with_selectolax(html: str) -> ParsedProduct | None:
    parser = SelectolaxHTMLParser(html)
    product = parser.css_first(".product")
    if product is None:
        return None
    name = product.css_first(".name")
    price = product.css_first(".price-final")
    return ParsedProduct(
        title=_selectolax_text(name),
        href=name.attributes.get("href", "") if name is not None else "",
        brand=_selectolax_text(product.css_first(".manufacturer")),
        mpn=_selectolax_text(product.css_first(".code")),
        price=_selectolax_text(price),
        availability=_selectolax_text(product.css_first(".availability-amount")),
    )


def _selectolax_text(node) -> str | None:
    if node is None:
        return None
    text = node.text(strip=True)
    return text or None


def _parse_product_with_stdlib(html: str) -> ParsedProduct | None:
    parser = ShoptetSearchHTMLParser()
    parser.feed(html)
    return parser.product


class ShoptetSearchHTMLParser(StdlibHTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.product: ParsedProduct | None = None
        self._current_field: str | None = None
        self._in_product = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        classes = set((attr_map.get("class") or "").split())
        if "product" in classes:
            self._in_product = True
            self.product = ParsedProduct()
        if not self._in_product or self.product is None:
            return
        if "name" in classes:
            self._current_field = "title"
            self.product.href = attr_map.get("href") or ""
        elif "manufacturer" in classes:
            self._current_field = "brand"
        elif "code" in classes:
            self._current_field = "mpn"
        elif "price-final" in classes:
            self._current_field = "price"
        elif "availability-amount" in classes:
            self._current_field = "availability"

    def handle_endtag(self, tag: str) -> None:
        self._current_field = None

    def handle_data(self, data: str) -> None:
        if self.product is None or self._current_field is None:
            return
        text = data.strip()
        if not text:
            return
        current = getattr(self.product, self._current_field)
        setattr(self.product, self._current_field, f"{current or ''}{text}")


class JsonLdHTMLParser(StdlibHTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.payloads: list[str] = []
        self._in_jsonld = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag == "script" and attr_map.get("type") == "application/ld+json":
            self._in_jsonld = True
            self._chunks = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._in_jsonld:
            self.payloads.append("".join(self._chunks).strip())
            self._in_jsonld = False
            self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._in_jsonld:
            self._chunks.append(data)


def _parse_price(text: str) -> float:
    cleaned = (
        text.replace("\xa0", " ")
        .replace("€", "")
        .replace("EUR", "")
        .strip()
        .replace(" ", "")
        .replace(",", ".")
    )
    return float(cleaned)


def _parse_stock(text: str | None) -> bool | None:
    if text is None:
        return None
    lowered = text.lower()
    if "instock" in lowered or "skladom" in lowered:
        return True
    if "outofstock" in lowered:
        return False
    return None
