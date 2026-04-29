from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper, _SearchDelegate
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper


def test_doktorkladivo_scraper_uses_expected_identity_and_generic_fallback() -> None:
    scraper = DoktorKladivoScraper()

    # Uses composition rather than inheritance for Shoptet search delegation.
    assert callable(scraper.search_by_query)
    assert scraper.competitor_id == "doktorkladivo_sk"
    assert scraper.base_url == "https://www.doktorkladivo.sk"


def test_doktorkladivo_scraper_uses_real_search_endpoint() -> None:
    scraper = DoktorKladivoScraper()

    assert scraper._search_path == "hladat/"
    assert scraper._search_param == "q"


def test_doktorkladivo_search_delegate_is_shoptet_subclass() -> None:
    delegate = _SearchDelegate({"id": "doktorkladivo_sk", "url": "https://www.doktorkladivo.sk"})

    assert isinstance(delegate, ShoptetGenericScraper)
