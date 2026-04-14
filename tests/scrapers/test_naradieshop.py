"""Tests for the NaradieShop scraper."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agnaradie_pricing.scrapers.naradieshop import (
    NaradieShopScraper,
    _clean_url,
    _parse_first_product,
    _parse_price,
)

FIXTURE = Path(__file__).parent / "fixtures" / "naradieshop_search.html"


def _fixture_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestNaradieShopParser:
    def test_parse_returns_first_product_from_fixture(self):
        html = _fixture_html()
        result = _parse_first_product(html, "naradieshop_sk")
        assert result is not None
        assert result.competitor_id == "naradieshop_sk"
        assert result.title
        assert len(result.title) > 5
        assert result.price_eur > 0
        assert result.currency == "EUR"

    def test_parse_product_url_has_no_query_string(self):
        html = _fixture_html()
        result = _parse_first_product(html, "naradieshop_sk")
        assert result is not None
        assert "search_query" not in result.url
        assert "results=" not in result.url

    def test_parse_product_url_is_full(self):
        html = _fixture_html()
        result = _parse_first_product(html, "naradieshop_sk")
        assert result is not None
        assert result.url.startswith("https://naradieshop.sk/")

    def test_parse_empty_html_returns_none(self):
        result = _parse_first_product("<html><body></body></html>", "naradieshop_sk")
        assert result is None

    def test_parse_price_slovak_format(self):
        assert _parse_price("34,00 €") == pytest.approx(34.0)
        assert _parse_price("  51,90€  ") == pytest.approx(51.90)
        assert _parse_price("119.90") == pytest.approx(119.90)

    def test_clean_url_strips_query(self):
        dirty = "https://naradieshop.sk/produkt/knipex?search_query=knipex&results=9"
        clean = _clean_url(dirty)
        assert "search_query" not in clean
        assert clean == "https://naradieshop.sk/produkt/knipex"

    def test_clean_url_preserves_path(self):
        url = "https://naradieshop.sk/klieste-kombinovane/knipex-8701300"
        assert _clean_url(url) == url


class TestNaradieShopScraper:
    def test_discover_feed_returns_none(self):
        assert NaradieShopScraper().discover_feed() is None

    def test_fetch_feed_returns_empty(self):
        assert NaradieShopScraper().fetch_feed("https://x") == []

    def test_scraper_identity(self):
        s = NaradieShopScraper()
        assert s.competitor_id == "naradieshop_sk"

    def test_search_by_mpn_calls_correct_endpoint(self):
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = _fixture_html()
        mock_response.raise_for_status = MagicMock()
        mock_client.get.return_value = mock_response

        scraper = NaradieShopScraper(http_client=mock_client)
        scraper.search_by_mpn("KNIPEX", "87-01-250")

        # First call is the search request; second is the detail-page enrichment.
        search_call = mock_client.get.call_args_list[0]
        assert "vyhladavanie" in search_call[0][0]
        params = search_call[1]["params"]
        assert "search_query" in params
        assert "KNIPEX" in params["search_query"]
