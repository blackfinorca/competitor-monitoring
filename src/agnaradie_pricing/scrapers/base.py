"""Base scraper contracts."""

import queue as _queue
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator


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

    def run_daily_iter(
        self, ag_catalogue: list[dict]
    ) -> Iterator[CompetitorListing]:
        """Generator version of run_daily — yields listings as they are produced.

        Subclasses with large catalogues (ToolZone, DoktorKladivo, Boukal)
        override this to yield per-page or per-chunk so the orchestrator can
        flush to DB incrementally without waiting for the full scrape to finish.

        Default: search-by-MPN loop over the catalogue, streaming results from
        parallel workers via as_completed so each match is yielded immediately.
        """
        feed_url = self.discover_feed()
        if feed_url:
            # Subclasses with large feeds override run_daily_iter entirely;
            # for small feeds (XML downloads) the full list is fine.
            yield from self.fetch_feed(feed_url)
            return

        items = [
            (s["brand"], s["mpn"])
            for s in ag_catalogue
            if s.get("brand") and s.get("mpn")
        ]
        workers: int = int(self.config.get("workers", 1))

        if workers <= 1:
            for brand, mpn in items:
                hit = self.search_by_mpn(brand, mpn)
                if hit:
                    yield hit
            return

        # Pool of independent scraper instances — each has its own httpx.Client
        # so threads never share connection state.
        pool: _queue.Queue = _queue.Queue()
        for _ in range(workers):
            pool.put(self.__class__(self.config))

        def _search(brand_mpn: tuple) -> CompetitorListing | None:
            brand, mpn = brand_mpn
            scraper = pool.get()
            try:
                return scraper.search_by_mpn(brand, mpn)
            except Exception:
                return None
            finally:
                pool.put(scraper)

        # as_completed streams each result as soon as a worker finishes,
        # rather than waiting for the entire catalogue to be processed.
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_search, item): item for item in items}
            for future in _as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        yield result
                except Exception:
                    pass

    def run_daily(self, ag_catalogue: list[dict]) -> list[CompetitorListing]:
        """Run the full daily scrape and return all listings as a list.

        Delegates to run_daily_iter() — subclasses should override that method
        rather than this one to get automatic batch-saving support.
        """
        return list(self.run_daily_iter(ag_catalogue))
