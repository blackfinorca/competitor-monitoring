"""Base scraper contracts."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class CompetitorListing:
    competitor_id: str
    competitor_sku: str | None
    brand: str | None
    mpn: str | None
    ean: str | None
    title: str
    price_eur: float
    currency: str
    in_stock: bool | None
    url: str
    scraped_at: datetime


class CompetitorScraper(ABC):
    def __init__(self, config: dict):
        self.config = config
        self.competitor_id: str = config["id"]
        self.base_url: str = config["url"]

    @abstractmethod
    def discover_feed(self) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def fetch_feed(self, feed_url: str) -> list[CompetitorListing]:
        raise NotImplementedError

    @abstractmethod
    def search_by_mpn(self, brand: str, mpn: str) -> CompetitorListing | None:
        raise NotImplementedError

    def search_by_query(self, query: str) -> CompetitorListing | None:
        """Search by arbitrary query string (EAN, MPN, or title fragment).

        Default delegates to search_by_mpn("", query).
        Subclasses with a flexible search endpoint should override.
        """
        return self.search_by_mpn("", query)

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        feed_url = self.discover_feed()
        if feed_url:
            return self.fetch_feed(feed_url)

        results = []
        for sku in ag_catalogue:
            if sku.get("brand") and sku.get("mpn"):
                hit = self.search_by_mpn(sku["brand"], sku["mpn"])
                if hit:
                    results.append(hit)
        return results

