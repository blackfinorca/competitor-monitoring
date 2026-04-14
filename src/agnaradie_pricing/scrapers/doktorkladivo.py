"""Doktor Kladivo scraper."""

import httpx

from agnaradie_pricing.scrapers.shoptet_generic import ShoptetGenericScraper


DOKTOR_KLADIVO_CONFIG = {
    "id": "doktorkladivo_sk",
    "name": "Doktor Kladivo",
    "url": "https://www.doktorkladivo.sk",
    "weight": 1.0,
    "rate_limit_rps": 1,
    "search_path": "hladat/",
    "search_query_param": "q",
}


class DoktorKladivoScraper(ShoptetGenericScraper):
    def __init__(
        self,
        config: dict | None = None,
        http_client: httpx.Client | None = None,
    ):
        super().__init__(config or DOKTOR_KLADIVO_CONFIG, http_client=http_client)
