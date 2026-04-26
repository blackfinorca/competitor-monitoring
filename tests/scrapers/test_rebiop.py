from agnaradie_pricing.scrapers.rebiop import REBIOP_CONFIG, RebiopScraper, _parse_detail_page


def test_rebiop_parse_detail_page_extracts_brand_field() -> None:
    detail_html = """
    <html>
      <body>
        <nav aria-label="brands">KNIPEX | Milwaukee | Bosch</nav>
        <meta name="keywords" content="KNIPEX, Milwaukee, BOSCH">
        <h1>Blankovací nôž so slzou BK-01</h1>
        <div class="detail-product-info">
          <dl><dt>Značka:</dt><dd>BAUPRO</dd></dl>
          <dl><dt>EAN kód:</dt><dd>8585033303677</dd></dl>
          <dl><dt>Kód:</dt><dd>3.40010</dd></dl>
          <dl class="detail-product-info-price"><dt>Cena s DPH</dt><dd>30,79 €</dd></dl>
          <dl><dt>Dostupnosť:</dt><dd>Skladom</dd></dl>
        </div>
      </body>
    </html>
    """

    listing = _parse_detail_page(
        detail_html,
        "rebiop_sk",
        "https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01",
    )

    assert listing is not None
    assert listing.brand == "BAUPRO"
    assert listing.ean == "8585033303677"


def test_rebiop_search_by_query_fetches_detail_page_for_ean(monkeypatch) -> None:
    search_html = """
    <div class="ctg-product-box" data-id="4853">
      <a href="detail/4853/blankovaci-noz-so-slzou-bk-01/cat/12">
        <div class="name">Blankovací nôž so slzou BK-01</div>
        <div class="ctg-prodbox-price">od <strong>30,79 €</strong></div>
        <div class="ctg-prodbox-stock">Skladom</div>
      </a>
    </div>
    """
    detail_html = """
    <html>
      <body>
        <h1>Blankovací nôž so slzou BK-01</h1>
        <div class="detail-product-info">
          <dl><dt>Značka:</dt><dd>BAUPRO</dd></dl>
          <dl><dt>EAN kód:</dt><dd>8585033303677</dd></dl>
          <dl><dt>Kód:</dt><dd>3.40010</dd></dl>
          <dl class="detail-product-info-price"><dt>Cena s DPH</dt><dd>30,79 €</dd></dl>
          <dl><dt>Dostupnosť:</dt><dd>Skladom</dd></dl>
        </div>
      </body>
    </html>
    """

    class FakeResponse:
        def __init__(self, text: str, *, status_code: int = 200, url: str) -> None:
            self.text = text
            self.status_code = status_code
            self.url = url
            self.headers = {}

        def raise_for_status(self) -> None:
            return None

    seen_urls: list[str] = []
    responses = {
        "https://www.rebiop.sk/search/products": FakeResponse(
            search_html, url="https://www.rebiop.sk/search/products?q=BK-01"
        ),
        "https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01": FakeResponse(
            detail_html, url="https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01"
        ),
    }

    def fake_polite_get(client, url, **kwargs):
        del client, kwargs
        seen_urls.append(url)
        return responses[url]

    monkeypatch.setattr("agnaradie_pricing.scrapers.rebiop.polite_get", fake_polite_get)

    scraper = RebiopScraper(REBIOP_CONFIG)
    listing = scraper.search_by_query("BK-01")

    assert listing is not None
    assert listing.url == "https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01"
    assert listing.brand == "BAUPRO"
    assert listing.ean == "8585033303677"
    assert listing.competitor_sku == "3.40010"
    assert seen_urls == [
        "https://www.rebiop.sk/search/products",
        "https://www.rebiop.sk/detail/4853/blankovaci-noz-so-slzou-bk-01",
    ]
