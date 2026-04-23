from agnaradie_pricing.scrapers.ferant import (
    _extract_product_urls_from_sitemap,
    _parse_product_detail,
)


_DETAIL_HTML = """
<html>
  <body>
    <h1 class="flypage-h1">Kompaktny vrtaci skrutkovac M12BDDXKIT-202C milwaukee 4933447836</h1>
    <span class="manu_name"><a href="/vyrobca-milwaukee-8/" title="Milwaukee">Milwaukee</a></span>
    <div class="flypage_sku fp_line"><span class="product_sku_label">Kod produktu:</span><span class="product_sku_value">3805894</span></div>
    <span id="product-detail-price-value" class="akcia-cena-sdph">299,27 EUR</span>
    <span class="shop_product_availability_value attr_avail_12" title="Na sklade">Na sklade</span>
  </body>
</html>
"""

_SITEMAP_XML = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://www.fermatshop.sk/</loc></url>
  <url><loc>https://www.fermatshop.sk/akumulatorove-naradie/</loc></url>
  <url><loc>https://www.fermatshop.sk/akumulatorove-naradie/product-a/</loc></url>
  <url><loc>https://www.fermatshop.sk/akumulatorove-naradie/product-a/</loc></url>
  <url><loc>https://www.fermatshop.sk/prihlasenie/reset/</loc></url>
  <url><loc>https://www.fermatshop.sk/aku-vrtacky/product-b/</loc></url>
</urlset>
"""


class _Resp:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_parse_product_detail_extracts_expected_fields() -> None:
    parsed = _parse_product_detail(_DETAIL_HTML)
    assert parsed is not None
    assert parsed.title.startswith("Kompaktny vrtaci")
    assert parsed.brand == "Milwaukee"
    assert parsed.product_code == "3805894"
    assert parsed.price_eur == 299.27
    assert parsed.in_stock is True


def test_extract_product_urls_from_sitemap_filters_non_products(monkeypatch) -> None:
    from agnaradie_pricing.scrapers import ferant

    monkeypatch.setattr(
        ferant,
        "polite_get",
        lambda client, url, min_rps: _Resp(_SITEMAP_XML),
    )

    urls = _extract_product_urls_from_sitemap(client=object())  # type: ignore[arg-type]
    assert urls == [
        "https://www.fermatshop.sk/akumulatorove-naradie/product-a/",
        "https://www.fermatshop.sk/aku-vrtacky/product-b/",
    ]
