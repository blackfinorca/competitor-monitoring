from pathlib import Path

import httpx

from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper


def test_shoptet_generic_discovers_common_heureka_feed() -> None:
    config = {"id": "example_sk", "url": "https://example.sk"}
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, text="<SHOP></SHOP>")
    )

    scraper = ShoptetGenericScraper(config, http_client=httpx.Client(transport=transport))

    assert scraper.discover_feed() == "https://example.sk/heureka.xml"


def test_shoptet_generic_search_by_mpn_parses_first_product_hit() -> None:
    html = Path("tests/scrapers/fixtures/shoptet_search.html").read_text()
    config = {"id": "example_sk", "url": "https://example.sk"}
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))

    scraper = ShoptetGenericScraper(config, http_client=httpx.Client(transport=transport))
    listing = scraper.search_by_mpn("Knipex", "87-01-250")

    assert listing is not None
    assert listing.competitor_id == "example_sk"
    assert listing.brand == "Knipex GmbH"
    assert listing.mpn == "87-01-250"
    assert listing.title == "Knipex Cobra 87 01 250"
    assert listing.price_eur == 24.90
    assert listing.in_stock is True
    assert listing.url == "https://example.sk/knipex-cobra/"


def test_shoptet_generic_parses_jsonld_item_list_product() -> None:
    html = Path("tests/scrapers/fixtures/doktorkladivo_search_jsonld.html").read_text()
    config = {"id": "doktorkladivo_sk", "url": "https://www.doktorkladivo.sk"}
    transport = httpx.MockTransport(lambda request: httpx.Response(200, text=html))

    scraper = ShoptetGenericScraper(config, http_client=httpx.Client(transport=transport))
    listing = scraper.search_by_mpn("Knipex", "87-01-250")

    assert listing is not None
    assert listing.competitor_id == "doktorkladivo_sk"
    assert listing.competitor_sku == "2808"
    assert listing.brand == "KNIPEX"
    assert listing.mpn == "8701250"
    assert listing.ean == "4003773022022"
    assert listing.title == "KNIPEX Kliešte inštalatérske Cobra 8701250"
    assert listing.price_eur == 28.61
    assert listing.currency == "EUR"
    assert listing.in_stock is True
