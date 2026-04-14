from agnaradie_pricing.scrapers.doktorkladivo import DoktorKladivoScraper
from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper


def test_doktorkladivo_scraper_uses_expected_identity_and_generic_fallback() -> None:
    scraper = DoktorKladivoScraper()

    assert isinstance(scraper, ShoptetGenericScraper)
    assert scraper.competitor_id == "doktorkladivo_sk"
    assert scraper.base_url == "https://www.doktorkladivo.sk"


def test_doktorkladivo_scraper_uses_real_search_endpoint() -> None:
    scraper = DoktorKladivoScraper()

    assert scraper.search_path == "hladat/"
    assert scraper.search_query_param == "q"
