"""Tests for the ToolZone scraper."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from agnaradie_pricing.scrapers.toolzone import (
    ToolZoneScraper,
    _extract_gtm_eur_price,
    _parse_product_page,
)

FIXTURE = Path(__file__).parent / "fixtures" / "toolzone_product.html"


def _fixture_html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestToolZoneParser:
    def test_parse_product_page_from_fixture(self):
        html = _fixture_html()
        result = _parse_product_page(html, "toolzone_sk", "https://www.toolzone.sk/produkt/test.htm")
        assert result is not None
        assert result.competitor_id == "toolzone_sk"
        assert result.title
        assert result.price_eur > 0

    def test_parse_product_ean_in_sku_field(self):
        html = _fixture_html()
        result = _parse_product_page(html, "toolzone_sk", "https://www.toolzone.sk/produkt/test.htm")
        assert result is not None
        # ToolZone JSON-LD puts EAN in `sku` field
        assert result.ean is not None
        assert len(result.ean) >= 8  # EAN-8 or EAN-13

    def test_parse_product_brand(self):
        html = _fixture_html()
        result = _parse_product_page(html, "toolzone_sk", "https://www.toolzone.sk/produkt/test.htm")
        assert result is not None
        assert result.brand is not None
        assert len(result.brand) > 0

    def test_parse_product_uses_gtm_eur_price(self):
        html = _fixture_html()
        gtm_price = _extract_gtm_eur_price(html)
        result = _parse_product_page(html, "toolzone_sk", "https://www.toolzone.sk/produkt/test.htm")
        if gtm_price is not None and result is not None:
            assert result.price_eur == pytest.approx(gtm_price)

    def test_extract_gtm_price_from_fixture(self):
        html = _fixture_html()
        price = _extract_gtm_eur_price(html)
        # GTM price should be a reasonable EUR value if present
        if price is not None:
            assert 0 < price < 10000

    def test_parse_empty_html_returns_none(self):
        result = _parse_product_page("<html><body></body></html>", "toolzone_sk", "https://x")
        assert result is None


class TestToolZoneScraper:
    def test_discover_feed_returns_sentinel(self):
        scraper = ToolZoneScraper()
        result = scraper.discover_feed()
        assert result is not None
        assert "sitemap" in result

    def test_search_by_mpn_returns_none(self):
        scraper = ToolZoneScraper()
        assert scraper.search_by_mpn("Knipex", "87-01-250") is None

    def test_scraper_identity(self):
        s = ToolZoneScraper()
        assert s.competitor_id == "toolzone_sk"
        assert "toolzone.sk" in s.base_url

    def test_brand_slug_filter_limits_product_urls(self):
        """Scraper with brand_slugs only returns URLs matching those slugs."""
        mock_client = MagicMock()
        mock_sitemap = MagicMock()
        mock_sitemap.text = """
        <urlset>
          <url><loc>https://www.toolzone.sk/produkt/knipex-cobra-250mm-12345.htm</loc></url>
          <url><loc>https://www.toolzone.sk/produkt/bosch-drill-gsb-67890.htm</loc></url>
          <url><loc>https://www.toolzone.sk/produkt/knipex-pliers-54321.htm</loc></url>
        </urlset>
        """
        mock_sitemap.raise_for_status = MagicMock()

        # Mock product page response
        mock_page = MagicMock()
        mock_page.text = "<html><body></body></html>"
        mock_page.raise_for_status = MagicMock()

        mock_client.get.side_effect = lambda url, **kw: (
            mock_sitemap if "sitemap" in url else mock_page
        )

        config = {
            "id": "toolzone_sk",
            "url": "https://www.toolzone.sk",
            "brand_slugs": ["knipex"],
        }
        scraper = ToolZoneScraper(config=config, http_client=mock_client)
        urls = scraper._get_product_urls()

        # Only knipex URLs should be returned
        assert all("knipex" in u for u in urls)
        assert len(urls) == 2
