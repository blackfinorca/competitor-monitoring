from pathlib import Path

from agnaradie_pricing.scrapers.heureka_feed import parse_heureka_feed


def test_parse_heureka_feed_maps_core_listing_fields() -> None:
    xml = Path("tests/scrapers/fixtures/heureka_feed.xml").read_bytes()

    listings = parse_heureka_feed(
        xml,
        competitor_id="doktorkladivo_sk",
    )

    assert len(listings) == 1
    listing = listings[0]
    assert listing.competitor_id == "doktorkladivo_sk"
    assert listing.competitor_sku == "DK-123"
    assert listing.brand == "Knipex GmbH"
    assert listing.mpn == "87-01-250"
    assert listing.ean == "4003773012024"
    assert listing.title == "Knipex Cobra 87 01 250"
    assert listing.price_eur == 24.90
    assert listing.currency == "EUR"
    assert listing.in_stock is True
    assert listing.url == "https://www.doktorkladivo.sk/knipex-cobra"

