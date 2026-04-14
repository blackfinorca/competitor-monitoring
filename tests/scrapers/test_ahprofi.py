"""Tests for the AH Profi scraper."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from agnaradie_pricing.scrapers.ahprofi import (
    AhProfiScraper,
    _parse_first_product,
    _parse_price,
)

FIXTURE = Path(__file__).parent / "fixtures" / "ahprofi_search.html"


def _fixture_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestAhProfiParser:
    def test_parse_returns_first_product_from_fixture(self):
        html = _fixture_html()
        result = _parse_first_product(html, "https://www.ahprofi.sk", "ahprofi_sk")
        assert result is not None
        assert result.competitor_id == "ahprofi_sk"
        assert result.title
        assert len(result.title) > 5
        assert result.price_eur > 0
        assert result.currency == "EUR"

    def test_parse_product_has_url(self):
        html = _fixture_html()
        result = _parse_first_product(html, "https://www.ahprofi.sk", "ahprofi_sk")
        assert result is not None
        assert result.url.startswith("https://www.ahprofi.sk")

    def test_parse_product_mpn_from_user_code(self):
        html = _fixture_html()
        result = _parse_first_product(html, "https://www.ahprofi.sk", "ahprofi_sk")
        assert result is not None
        # user_code span contains the product code / MPN
        assert result.mpn is not None
        assert len(result.mpn) >= 4

    def test_parse_empty_html_returns_none(self):
        result = _parse_first_product("<html><body></body></html>", "https://www.ahprofi.sk", "ahprofi_sk")
        assert result is None

    def test_parse_price_euro_format(self):
        assert _parse_price("€\xa019,25") == pytest.approx(19.25)
        assert _parse_price("19,25") == pytest.approx(19.25)
        assert _parse_price("€ 29,71") == pytest.approx(29.71)
        assert _parse_price("119.90") == pytest.approx(119.90)


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
        assert "KNIPEX" in params["search_keyword"]
        # Dashes should be replaced with spaces in the MPN
        assert "-" not in params["search_keyword"]
