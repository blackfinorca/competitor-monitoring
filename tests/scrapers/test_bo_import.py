"""Tests for the BO-Import scraper."""

from pathlib import Path

import pytest

from agnaradie_pricing.scrapers.bo_import import (
    BoImportScraper,
    _brand_to_slug,
    _extract_product_urls,
    _parse_product_page,
)

FIXTURE = Path(__file__).parent / "fixtures" / "bo_import_product.html"


def _fixture_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestBoImportParser:
    def test_parse_product_page_returns_listing(self):
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/kni-klieste-p1234/"
        )
        assert result is not None
        assert result.competitor_id == "bo_import_cz"

    def test_price_converted_from_czk(self):
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/test-p1/"
        )
        assert result is not None
        # 1141.75 CZK / 25.0 = 45.67 EUR
        assert result.price_eur == pytest.approx(45.67)
        assert result.currency == "EUR"

    def test_ean_extracted_from_gtin13(self):
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/test-p1/"
        )
        assert result is not None
        assert result.ean == "4003773009474"

    def test_mpn_derived_from_sku(self):
        """BO-Import encodes MPN inside SKU field (KNI-0301180 → 0301180)."""
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/test-p1/"
        )
        assert result is not None
        assert result.mpn == "0301180"

    def test_competitor_sku_kept_as_raw(self):
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/test-p1/"
        )
        assert result is not None
        assert result.competitor_sku == "KNI-0301180"

    def test_brand_extracted(self):
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/test-p1/"
        )
        assert result is not None
        assert result.brand == "Knipex"

    def test_availability_in_stock(self):
        result = _parse_product_page(
            _fixture_html(), "bo_import_cz", "https://www.bo-import.cz/test-p1/"
        )
        assert result is not None
        assert result.in_stock is True

    def test_empty_html_returns_none(self):
        result = _parse_product_page("<html><body></body></html>", "bo_import_cz", "https://x")
        assert result is None

    def test_out_of_stock(self):
        html = """<html><body>
        <script type="application/ld+json">
        {
          "@type": "Product", "name": "Test", "sku": "KNI-1234",
          "offers": [{"price": "500", "priceCurrency": "czk",
                      "availability": "http://schema.org/OutOfStock"}]
        }
        </script></body></html>"""
        result = _parse_product_page(html, "bo_import_cz", "https://x")
        assert result is not None
        assert result.in_stock is False


class TestBoImportExtractUrls:
    def test_extracts_product_paths(self):
        html = '''
        <a href="/knipex-klieste-180mm-p1234/">Product</a>
        <a href="/knipex-klieste-250mm-p5678/">Product 2</a>
        <a href="/kategoria/">Category</a>
        '''
        urls = _extract_product_urls(html)
        assert len(urls) == 2
        assert all("-p" in u for u in urls)

    def test_deduplicates_by_path(self):
        html = '''
        <a href="/item-p1/?cid=1">A</a>
        <a href="/item-p1/?cid=2">B</a>
        '''
        urls = _extract_product_urls(html)
        assert len(urls) == 1


class TestBrandToSlug:
    def test_simple_ascii(self):
        assert _brand_to_slug("Knipex") == "knipex"

    def test_accented_chars(self):
        assert _brand_to_slug("Stříbrný") == "stribrny"

    def test_spaces_become_hyphens(self):
        assert _brand_to_slug("Black & Decker") == "black-decker"


class TestBoImportScraper:
    def test_competitor_id(self):
        s = BoImportScraper()
        assert s.competitor_id == "bo_import_cz"

    def test_base_url(self):
        s = BoImportScraper()
        assert "bo-import.cz" in s.base_url
