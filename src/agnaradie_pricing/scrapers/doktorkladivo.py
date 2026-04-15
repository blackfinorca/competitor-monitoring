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
        # Merge: DOKTOR_KLADIVO_CONFIG supplies search_path/search_query_param;
        # caller-supplied config (e.g. from competitors.yaml) overrides url/rate_limit_rps.
        merged = {**DOKTOR_KLADIVO_CONFIG, **(config or {})}
        super().__init__(merged, http_client=http_client)
