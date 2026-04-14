"""Heureka XML feed parsing."""

from datetime import UTC, datetime

from lxml import etree

from agnaradie_pricing.scrapers.base import CompetitorListing


def parse_heureka_feed(
    xml_content: bytes,
    competitor_id: str,
    scraped_at: datetime | None = None,
) -> list[CompetitorListing]:
    root = etree.fromstring(xml_content)
    timestamp = scraped_at or datetime.now(UTC)
    listings = []

    for item in root.findall(".//SHOPITEM"):
        title = _text(item, "PRODUCTNAME") or _text(item, "PRODUCT")
        price = _text(item, "PRICE_VAT")
        url = _text(item, "URL")
        if not title or not price or not url:
            continue

        listings.append(
            CompetitorListing(
                competitor_id=competitor_id,
                competitor_sku=_text(item, "ITEM_ID"),
                brand=_text(item, "MANUFACTURER"),
                mpn=_text(item, "PRODUCTNO"),
                ean=_text(item, "EAN"),
                title=title,
                price_eur=float(price.replace(",", ".")),
                currency="EUR",
                in_stock=_delivery_in_stock(_text(item, "DELIVERY_DATE")),
                url=url,
                scraped_at=timestamp,
            )
        )

    return listings


class HeurekaFeedMixin:
    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        response = self.http_client.get(feed_url)
        response.raise_for_status()
        return parse_heureka_feed(response.content, self.competitor_id)


def _text(item, tag: str) -> str | None:
    value = item.findtext(tag)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _delivery_in_stock(value: str | None) -> bool | None:
    if value is None:
        return None
    return value == "0"

