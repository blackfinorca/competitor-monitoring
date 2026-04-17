"""Tests for the AGI scraper."""

from pathlib import Path

import pytest

from agnaradie_pricing.scrapers.agi import (
    AgiScraper,
    _extract_product_urls,
    _parse_product_page,
)

FIXTURE = Path(__file__).parent / "fixtures" / "agi_product.html"


def _fixture_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestAgiParser:
    def test_parse_product_page_returns_listing(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/kombinovane-klieste-180mm-p93449"
        )
        assert result is not None
        assert result.competitor_id == "agi_sk"

    def test_price_is_positive_eur(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/test-p1"
        )
        assert result is not None
        assert result.price_eur == pytest.approx(45.67)
        assert result.currency == "EUR"

    def test_ean_extracted_from_gtin(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/test-p1"
        )
        assert result is not None
        assert result.ean == "4003773009474"

    def test_mpn_field(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/test-p1"
        )
        assert result is not None
        assert result.mpn == "03 01 180"

    def test_brand_extracted(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/test-p1"
        )
        assert result is not None
        assert result.brand == "Knipex"

    def test_availability_in_stock(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/test-p1"
        )
        assert result is not None
        assert result.in_stock is True

    def test_title_present(self):
        result = _parse_product_page(
            _fixture_html(), "agi_sk", "https://www.agi.sk/test-p1"
        )
        assert result is not None
        assert "180" in result.title

    def test_empty_html_returns_none(self):
        result = _parse_product_page("<html><body></body></html>", "agi_sk", "https://x")
        assert result is None

    def test_missing_price_returns_none(self):
        html = """<html><body>
        <script type="application/ld+json">
        {"@type": "Product", "name": "Test", "sku": "1", "offers": {}}
        </script></body></html>"""
        result = _parse_product_page(html, "agi_sk", "https://x")
        assert result is None


class TestAgiExtractUrls:
    def test_extracts_product_paths(self):
        html = '''
        <a href="/knipex-klieste-180mm-p12345">Product</a>
        <a href="/knipex-klieste-250mm-p67890">Product 2</a>
        <a href="/kategoria-c123">Category</a>
        '''
        urls = _extract_product_urls(html)
        assert len(urls) == 2
        assert all(u.startswith("/") for u in urls)
        assert all("-p" in u for u in urls)

    def test_deduplicates_urls(self):
        html = '''
        <a href="/item-p1">A</a>
        <a href="/item-p1">B</a>
        '''
        urls = _extract_product_urls(html)
        assert len(urls) == 1


class TestAgiScraper:
    def test_competitor_id(self):
        s = AgiScraper()
        assert s.competitor_id == "agi_sk"

    def test_base_url(self):
        s = AgiScraper()
        assert "agi.sk" in s.base_url
