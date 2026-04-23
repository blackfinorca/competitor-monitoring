from agnaradie_pricing.scrapers.strend import (
    _extract_category_urls,
    _extract_next_page_url,
    _extract_product_urls,
    _parse_product_detail,
)


_CATEGORY_HTML = """
<html>
  <head>
    <link rel="next" href="https://www.strendpro.sk/c/1/zamky-kovania?page=2">
  </head>
  <body>
    <a href="https://www.strendpro.sk/c/1/zamky-kovania">Cat A</a>
    <a href="/c/2/rucne-naradie">Cat B</a>
    <a href="https://www.strendpro.sk/c/1/zamky-kovania">Duplicate Cat A</a>
    <a href="https://www.strendpro.sk/p/179/zamok-blossom">Product</a>
  </body>
</html>
"""

_DETAIL_HTML = """
<html>
  <head>
    <script type="application/ld+json">
      {"@context":"http://schema.org","@type":"Product","name":"Zdvihak Strend Pro Premium",
       "brand":"Strend Pro Premium","model":"146880",
       "offers":{"@type":"Offer","priceCurrency":"EUR","price":"42.00","availability":"http://schema.org/InStock"}}
    </script>
  </head>
  <body>
    <div class="product-info__parameters">
      <div class="product-info__parameter "><strong>Kat. cislo:</strong><span>146880</span></div>
      <div class="product-info__parameter "><strong>EAN kod:</strong><span>8584163116362</span></div>
    </div>
  </body>
</html>
"""


def test_extract_category_and_product_urls() -> None:
    categories = _extract_category_urls(_CATEGORY_HTML, "https://www.strendpro.sk")
    products = _extract_product_urls(_CATEGORY_HTML)
    assert categories == [
        "https://www.strendpro.sk/c/1/zamky-kovania",
        "https://www.strendpro.sk/c/2/rucne-naradie",
    ]
    assert products == ["https://www.strendpro.sk/p/179/zamok-blossom"]


def test_extract_next_page_url() -> None:
    assert (
        _extract_next_page_url(_CATEGORY_HTML)
        == "https://www.strendpro.sk/c/1/zamky-kovania?page=2"
    )


def test_parse_product_detail_extracts_expected_fields() -> None:
    parsed = _parse_product_detail(_DETAIL_HTML)
    assert parsed is not None
    assert parsed.title == "Zdvihak Strend Pro Premium"
    assert parsed.brand == "Strend Pro Premium"
    assert parsed.product_code == "146880"
    assert parsed.ean == "8584163116362"
    assert parsed.price_eur == 42.0
    assert parsed.in_stock is True
