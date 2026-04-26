"""Tests for the AH Profi scraper."""

import re
from pathlib import Path
from unittest.mock import MagicMock

from agnaradie_pricing.scrapers.ahprofi import AhProfiScraper, _parse_product_page

FIXTURE = Path(__file__).parent / "fixtures" / "ahprofi_search.html"


def _fixture_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestAhProfiParser:
    def test_parse_product_page_from_fixture(self):
        html = _fixture_html()
        html = re.sub(
            r'(<span class="col col-7 right"><a href="https://www\.ahprofi\.sk/knipex">)Knipex(</a></span>)',
            r"\1MAGICBRAND\2",
            html,
        )
        result = _parse_product_page(html, "ahprofi_sk", "https://www.ahprofi.sk/produkt")
        assert result is not None
        assert result.competitor_id == "ahprofi_sk"
        assert result.title == "SIKA kliešte KNIPEX Cobra 250 mm - 8711250"
        assert result.brand == "MAGICBRAND"
        assert result.ean == "4003773035473"
        assert result.price_eur == 33.44
        assert result.currency == "EUR"

    def test_parse_empty_html_returns_none(self):
        result = _parse_product_page("<html><body></body></html>", "ahprofi_sk", "https://www.ahprofi.sk/produkt")
        assert result is None


class TestAhProfiScraper:
    def test_discover_feed_returns_none(self):
        scraper = AhProfiScraper()
        assert scraper.discover_feed() is None

    def test_fetch_feed_returns_empty(self):
        scraper = AhProfiScraper()
        assert scraper.fetch_feed("https://example.com/feed") == []

    def test_scraper_identity(self):
        scraper = AhProfiScraper()
        assert scraper.competitor_id == "ahprofi_sk"
        assert "ahprofi.sk" in scraper.base_url

    def test_search_by_mpn_calls_correct_endpoint(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = _fixture_html()
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        scraper = AhProfiScraper(http_client=mock_client)
        scraper.search_by_mpn("KNIPEX", "87-01-250")

        # First call is the search request; second is the detail-page enrichment.
        search_call = mock_client.get.call_args_list[0]
        assert "vysledky-vyhladavania" in search_call[0][0]
        params = search_call[1]["params"]
        assert "search_keyword" in params
        assert params["search_keyword"] == "8701250"
        # Search input should be the condensed MPN only.
        assert "-" not in params["search_keyword"]
